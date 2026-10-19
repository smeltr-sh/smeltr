//! The console: one line per event, and the one consumer that is always present.
//!
//! Not a configured destination. It is what still works when configuration is what broke, and no other consumer
//! may replace it.

use std::io::Write;

use super::event::{Event, Value};
use crate::ops::civil::civil_from_days;

/// Render one event for a person reading a terminal.
///
/// The JSON form is what a machine reads; this is deliberately not that. Time, severity and site first because
/// that is what an eye scans for, then the sentence, then what varied.
pub fn line(event: &Event) -> String {
    let mut out = format!("{} {:<5} {}", stamp(event.timestamp), event.level.as_str(), event.site);
    // Who spoke, where a reader is already looking. Eight reader threads holding one source are eight lines that
    // differ only here, so leaving it to a field would put the one distinguishing part last.
    match (event.worker, event.thread.as_deref()) {
        (Some(worker), Some(thread)) => out.push_str(&format!(" ({worker} {thread})")),
        (Some(one), None) | (None, Some(one)) => out.push_str(&format!(" ({one})")),
        (None, None) => {}
    }
    out.push_str(": ");
    out.push_str(&escaped(&event.message));
    out.push_str(&render_fields(
        event
            .fields
            .iter()
            .map(|(name, value)| (name.as_ref(), render_value(value))),
    ));
    out
}

/// What varied, as ` [name=value ...]` after a sentence, or nothing when nothing did. Values as [`render_value`]
/// gives them.
pub(crate) fn render_fields<'a>(fields: impl IntoIterator<Item = (&'a str, String)>) -> String {
    let pairs: Vec<String> = fields
        .into_iter()
        .map(|(name, value)| format!("{name}={value}"))
        .collect();
    if pairs.is_empty() {
        String::new()
    } else {
        format!(" [{}]", pairs.join(" "))
    }
}

/// One event is one line. A value carrying its own line breaks -- an Elasticsearch refusal, a TLS error -- would
/// otherwise emit fragments with no timestamp, level or site, which a shipper indexes as separate malformed
/// records. This makes them visible rather than reversible; the fields are what a machine reads.
fn escaped(text: &str) -> std::borrow::Cow<'_, str> {
    if !text.contains(['\n', '\r', '\t']) {
        return std::borrow::Cow::Borrowed(text);
    }
    std::borrow::Cow::Owned(text.replace('\n', "\\n").replace('\r', "\\r").replace('\t', "\\t"))
}

/// One field's value on one line.
pub(crate) fn render_value(value: &Value) -> String {
    match value {
        Value::Str(s) => escaped(s).into_owned(),
        Value::Int(n) => n.to_string(),
        Value::Uint(n) => n.to_string(),
        Value::Float(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
    }
}

/// RFC 3339 in UTC, to the millisecond, as `2025-10-10T13:00:34.567Z`.
pub(crate) fn stamp(at: std::time::SystemTime) -> String {
    let since = at.duration_since(std::time::UNIX_EPOCH).unwrap_or_default();
    let (secs, millis) = (since.as_secs() as i64, since.subsec_millis());
    let (days, rem) = (secs.div_euclid(86_400), secs.rem_euclid(86_400));
    let (y, m, d) = civil_from_days(days);
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}.{millis:03}Z",
        rem / 3600,
        (rem % 3600) / 60,
        rem % 60
    )
}

/// All of it to stderr, and whether it landed.
///
/// smeltr's stdout carries no data -- rows leave through destinations -- so there is nothing for logs to
/// stay out of the way of, and splitting by severity only means an operator watching one stream sees a failure
/// arrive and never sees it clear. Severity says how bad; it does not pick a stream.
pub fn write(event: &Event) -> std::io::Result<()> {
    write_line(&mut std::io::stderr().lock(), &line(event))
}

/// One line, or nothing.
///
/// Not `println!`, which panics when its write fails. This runs on the drain thread, the one thread every
/// log passes through: a process whose stdout is a pipe with no reader would lose that thread to the
/// first log and then silently drop every log after it, for as long as the process lived. The
/// error goes nowhere because what failed is the way we had to say so.
fn write_line(out: &mut impl Write, text: &str) -> std::io::Result<()> {
    writeln!(out, "{text}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(unix_millis: u64) -> std::time::SystemTime {
        std::time::UNIX_EPOCH + std::time::Duration::from_millis(unix_millis)
    }

    /// Eight reader threads holding one source differ only in who is speaking, so a line that omits it is eight
    /// identical lines. Nothing rendered `worker` or `thread` until a review found it, and no test saw that.
    #[test]
    fn a_line_says_who_spoke() {
        let event = |worker, thread: Option<&str>| Event {
            timestamp: at(1_757_689_445_123),
            level: tracing::Level::WARN,
            site: "runtime.source_blocked".into(),
            message: "a source is blocked".to_string(),
            fields: vec![],
            worker,
            thread: thread.map(str::to_string),
            local_only: false,
            seq: 0,
        };
        assert!(line(&event(Some("w-1"), Some("reader-0"))).contains("runtime.source_blocked (w-1 reader-0): "));
        assert!(line(&event(None, Some("reader-0"))).contains("runtime.source_blocked (reader-0): "));
        assert!(line(&event(Some("w-1"), None)).contains("runtime.source_blocked (w-1): "));
        assert!(line(&event(None, None)).contains("runtime.source_blocked: "));
    }

    /// One event is one line, whatever a destination put in its refusal.
    #[test]
    fn a_value_carrying_line_breaks_does_not_become_three_records() {
        let event = Event {
            timestamp: at(1_757_689_445_123),
            level: tracing::Level::ERROR,
            site: "runtime.rows_rejected".into(),
            message: "a destination refused rows".to_string(),
            fields: vec![(
                "reason".into(),
                Value::Str("parse failed:\n  at field ts\n  in doc 7".into()),
            )],
            worker: None,
            thread: None,
            local_only: false,
            seq: 0,
        };
        let rendered = line(&event);
        assert_eq!(rendered.lines().count(), 1, "still one line: {rendered}");
        assert!(
            rendered.contains(r"parse failed:\n  at field ts"),
            "and the breaks are visible: {rendered}"
        );
    }

    #[test]
    fn a_line_leads_with_what_an_eye_scans_for() {
        let event = Event {
            timestamp: at(1_757_689_445_123),
            level: tracing::Level::ERROR,
            site: "runtime.row_rejected".into(),
            message: "destination refused rows and always will".to_string(),
            fields: vec![
                ("subject".into(), Value::Str("d1".into())),
                ("rows".into(), Value::Uint(4000)),
            ],
            worker: None,
            thread: None,
            local_only: false,
            seq: 0,
        };
        assert_eq!(
            line(&event),
            "2025-09-12T15:04:05.123Z ERROR runtime.row_rejected: \
             destination refused rows and always will [subject=d1 rows=4000]"
        );
    }
}
