//! smeltr's own logs as a source, so a bundle can route them to a destination like anything else.
//!
//! The metrics source's twin (design doc, Observability). What a process says about
//! itself already reaches its console and its leader; this is the third consumer, and the only one an operator
//! configures.
//!
//! A record is the event as JSON -- the fields as fields, not a sentence to parse at the other end.

use std::borrow::Cow;
use std::sync::Arc;
use std::time::Duration;

use crate::io::{CommitMap, IoError, Source, SourceRecord};
use crate::logs::{Event, Tap, Value};

/// The `__source.topic` every logs record carries, so a route can select them like any other source's.
pub const LOGS_TOPIC: &str = "__smeltr_logs";

/// How this source is configured. Nothing to reach and nothing to authenticate: what it reads is already here.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct LogsSourceConfig {
    /// The lowest severity this source carries. A ceiling sits above it: the process collects at
    /// `SMELTR_LOG_LEVEL`, and what it never collected is not here to read.
    pub min_level: tracing::Level,
}

/// An ordinary `Source` whose "waiting for data" is the buffer filling.
pub struct LogsSource {
    tap: Tap,
    min_level: tracing::Level,
    next_offset: i64,
    topic: Arc<str>,
}

impl LogsSource {
    /// Open a queue of this process's logs, and of what the worker's other process mirrors here, for this
    /// source alone.
    pub fn new(config: LogsSourceConfig) -> LogsSource {
        let tap = crate::logs::tap();
        // Said here rather than refused at load: the ceiling belongs to this process, and the bundle is read by
        // the leader too, where a different `SMELTR_LOG_LEVEL` would make the same bundle valid in one place and
        // invalid in another. Refusing would also cost every other pipeline in it.
        let ceiling = crate::startup_config::log_level();
        if config.min_level > ceiling {
            tracing::warn!(
                target: "logs_source.below_ceiling",
                min_level = %config.min_level,
                ceiling = %ceiling,
                "a logs source asks for logs this process does not collect, and carries only what it does"
            );
        }
        LogsSource {
            tap,
            min_level: config.min_level,
            next_offset: 0,
            topic: Arc::from(LOGS_TOPIC),
        }
    }
}

impl Source for LogsSource {
    fn poll_batch(
        &mut self,
        max_rows: usize,
        max_bytes: usize,
        timeout: Duration,
    ) -> Result<Vec<SourceRecord>, IoError> {
        // Everything this thread says while reading is console-only. Without it, a poll that reports a problem
        // hands itself that report to carry, and carrying it is what produces the next one.
        crate::logs::while_delivering(|| {
            let mut taken = self.tap.drain(max_rows);
            if taken.is_empty() {
                // No logs is the steady state of a healthy process, so this is the common path, and
                // returning straight away would spin a reader thread against an empty buffer for the life of the
                // deployment. Waiting is this source's poll.
                std::thread::sleep(timeout);
                taken = self.tap.drain(max_rows);
            }

            let mut out = crate::io::RecordChunkBuilder::new();
            let mut bytes = 0usize;
            let mut taken = taken.into_iter();
            for event in taken.by_ref() {
                if event.level > self.min_level {
                    continue;
                }
                let raw = json(&event);
                if !out.is_empty() && bytes + raw.len() > max_bytes {
                    // Past the budget, so this batch ends here. `max_rows` was the only bound the queue could
                    // apply, and a row's size is knowable only once it is encoded, so overshooting is ordinary
                    // rather than exceptional -- which is why it costs a poll rather than the events.
                    self.tap.put_back(std::iter::once(event).chain(taken).collect());
                    break;
                }
                bytes += raw.len();
                out.push(&raw, self.next_offset, 0, self.topic.clone(), None);
                self.next_offset += 1;
            }
            Ok(out.finish())
        })
    }

    /// Nothing upstream to acknowledge.
    fn commit(&mut self, _offsets: &CommitMap) -> Result<(), IoError> {
        Ok(())
    }
}

/// One event as JSON, its fields at their own keys under `fields` so whatever reads them indexes them without
/// being told a schema -- which is the whole reason a log is data rather than a sentence.
///
/// A field this cannot spell is left out rather than spelled wrong: a record the destination refuses is a
/// refusal reported here, and that report is the next record.
fn json(event: &Event) -> Vec<u8> {
    use crate::format::json::write_json_string;
    let mut out = Vec::with_capacity(128);
    out.extend_from_slice(br#"{"timestamp":"#);
    out.extend_from_slice(millis(event).to_string().as_bytes());
    out.extend_from_slice(br#","level":"#);
    write_json_string(event.level.as_str().to_lowercase().as_bytes(), &mut out);
    field(&mut out, b"site", event.site.as_bytes());
    field(&mut out, b"message", event.message.as_bytes());
    if let Some(worker) = event.worker {
        field(&mut out, b"worker", worker.as_bytes());
    }
    if let Some(thread) = &event.thread {
        field(&mut out, b"thread", thread.as_bytes());
    }
    // A field JSON cannot hold is dropped, so the object stays parseable; `writable` is what decides, and an
    // event whose every field it refuses writes no `fields` key at all rather than an empty one.
    if event.fields.iter().any(writable) {
        out.extend_from_slice(br#","fields":{"#);
        let mut first = true;
        for (name, value) in event.fields.iter().filter(|f| writable(f)) {
            if !first {
                out.push(b',');
            }
            first = false;
            write_json_string(name.as_bytes(), &mut out);
            out.push(b':');
            match value {
                Value::Str(s) => write_json_string(s.as_bytes(), &mut out),
                Value::Int(n) => out.extend_from_slice(n.to_string().as_bytes()),
                Value::Uint(n) => out.extend_from_slice(n.to_string().as_bytes()),
                // `{n:?}`, not `{n}`: Display spells `6.0` as `6`, and the first document a destination sees is
                // what types the field there -- after which `6.5` at the same site is refused. It is also the
                // shortest form that reads back as what was recorded (`format::json`).
                Value::Float(n) => out.extend_from_slice(format!("{n:?}").as_bytes()),
                Value::Bool(b) => out.extend_from_slice(b.to_string().as_bytes()),
            }
        }
        out.push(b'}');
    }
    out.push(b'}');
    out
}

/// JSON has no NaN and no infinity, and a dropped field beats a record nothing can parse (`format::json` makes
/// the same call). Every other value has a spelling.
fn writable((_, value): &(Cow<'static, str>, Value)) -> bool {
    match value {
        Value::Float(n) => n.is_finite(),
        _ => true,
    }
}

fn millis(event: &Event) -> i64 {
    event
        .timestamp
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or_default()
}

fn field(out: &mut Vec<u8>, name: &[u8], value: &[u8]) {
    use crate::format::json::write_json_string;
    out.push(b',');
    write_json_string(name, out);
    out.push(b':');
    write_json_string(value, out);
}

#[cfg(test)]
mod fuzz;

#[cfg(test)]
mod tests {
    use super::*;

    fn with(fields: Vec<(&'static str, Value)>) -> String {
        let event = Event {
            timestamp: std::time::SystemTime::UNIX_EPOCH,
            level: tracing::Level::ERROR,
            site: "json.test".into(),
            message: "a site said something".into(),
            fields: fields.into_iter().map(|(name, value)| (name.into(), value)).collect(),
            worker: None,
            thread: None,
            local_only: false,
            seq: 0,
        };
        String::from_utf8(json(&event)).expect("the encoder writes UTF-8")
    }

    /// A float keeps its decimal point, because the first record a destination sees is what types the field
    /// there: `6` would make it an integer and refuse the `6.5` that follows.
    #[test]
    fn a_whole_float_is_not_written_as_an_integer() {
        let out = with(vec![("ratio", Value::Float(6.0))]);
        assert!(out.contains(r#""ratio":6.0"#), "{out}");
    }

    /// JSON has no NaN and no infinity. The field goes rather than the record: a record nothing can parse is a
    /// refusal, and a refusal is the next record.
    #[test]
    fn a_float_json_cannot_spell_is_dropped_and_the_rest_still_parses() {
        let out = with(vec![
            ("rate", Value::Float(f64::NAN)),
            ("rows", Value::Uint(5)),
            ("ceiling", Value::Float(f64::INFINITY)),
        ]);
        let parsed: serde_json::Value = serde_json::from_str(&out).unwrap_or_else(|e| panic!("{e}: {out}"));
        assert_eq!(parsed["fields"]["rows"], 5);
        assert!(parsed["fields"]["rate"].is_null(), "{out}");
        assert!(parsed["fields"]["ceiling"].is_null(), "{out}");
    }

    /// An event whose every field is unspellable writes no `fields` key rather than an empty object, so nothing
    /// downstream has to tell "no fields" from "a field that went".
    #[test]
    fn an_event_left_with_no_writable_field_carries_no_fields_key() {
        let out = with(vec![("rate", Value::Float(f64::NAN))]);
        let parsed: serde_json::Value = serde_json::from_str(&out).unwrap_or_else(|e| panic!("{e}: {out}"));
        assert!(parsed.get("fields").is_none(), "{out}");
        assert_eq!(parsed["message"], "a site said something");
    }

    /// Two logs sources read side by side, and each sees the whole stream.
    #[test]
    fn two_logs_sources_each_read_every_event() {
        let _taps = crate::logs::TAPS_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        // Every ceiling collects ERROR, so opening one raises no warning.
        let cfg = LogsSourceConfig {
            min_level: tracing::Level::ERROR,
        };
        let mut first = LogsSource::new(cfg);
        let mut second = LogsSource::new(cfg);
        crate::logs::mirror([Event {
            timestamp: std::time::SystemTime::now(),
            level: tracing::Level::ERROR,
            site: "two_sources.test".into(),
            message: "said once".into(),
            fields: Vec::new(),
            worker: None,
            thread: None,
            local_only: false,
            seq: 0,
        }]);
        for source in [&mut first, &mut second] {
            let got = source.poll_batch(64, 1 << 20, Duration::ZERO).expect("a poll");
            assert!(
                got.iter()
                    .any(|r| String::from_utf8_lossy(r.bytes().unwrap()).contains("two_sources.test")),
                "a reader missed an event the other read"
            );
        }
    }
}
