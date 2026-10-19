//! The metrics registry (design doc, Observability): one `Arc<Metrics>` per process, written through a
//! per-thread [`MetricsHandle`], read through cursors, one per subscriber. Writes are per batch or per instruction,
//! never per row. [`MetricsSource`] reads it back as an ordinary `Source`.

use std::collections::HashMap;
use std::io::Write as _;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::format::json::write_json_string;
use crate::io::{CommitMap, IoError, Source, SourceRecord};

/// A metric name plus its label set. Labels are structural -- pipeline, function, route, source, destination, thread --
/// never row data, so the key count is bounded by the topology.
#[derive(Clone, PartialEq, Eq, Debug, Hash)]
pub struct MetricKey {
    pub name: &'static str,
    pub labels: Vec<(&'static str, String)>,
}

impl MetricKey {
    fn new(name: &'static str, labels: &[(&'static str, &str)]) -> MetricKey {
        MetricKey {
            name,
            labels: labels.iter().map(|(k, v)| (*k, v.to_string())).collect(),
        }
    }
}

/// A `Counter` sums across writes; a `Gauge` is last-write-wins and always reports its current value. A
/// `FloatCounter` sums as a `Counter` does, for a total that is not a whole number, such as seconds spent.
#[derive(Clone, Copy, Debug)]
pub enum MetricValue {
    Counter(u64),
    FloatCounter(f64),
    Gauge(f64),
}

impl MetricValue {
    pub fn is_counter(&self) -> bool {
        matches!(self, MetricValue::Counter(_) | MetricValue::FloatCounter(_))
    }

    /// A counter's increase of nothing, which a reader may drop before one that moved.
    pub fn is_idle(&self) -> bool {
        matches!(self, MetricValue::Counter(0)) || matches!(self, MetricValue::FloatCounter(v) if *v == 0.0)
    }
}

/// One sample: what [`MetricsSource`] turns into one JSON row.
#[derive(Clone, Debug)]
pub struct MetricSample {
    pub key: MetricKey,
    pub value: MetricValue,
}

impl MetricSample {
    /// Tidy, not wide: `{"metric":"...", <label>:"...", ..., "value":N,"ts":N}`, every label a string field.
    fn write_json(&self, ts_millis: i64, out: &mut Vec<u8>) {
        out.push(b'{');
        out.extend_from_slice(b"\"metric\":");
        write_json_string(self.key.name.as_bytes(), out);
        for (k, v) in &self.key.labels {
            out.push(b',');
            write_json_string(k.as_bytes(), out);
            out.push(b':');
            write_json_string(v.as_bytes(), out);
        }
        out.extend_from_slice(b",\"value\":");
        match self.value {
            MetricValue::Counter(v) => write!(out, "{v}").unwrap(),
            MetricValue::FloatCounter(v) | MetricValue::Gauge(v) => write!(out, "{v}").unwrap(),
        }
        out.extend_from_slice(b",\"ts\":");
        write!(out, "{ts_millis}").unwrap();
        out.push(b'}');
    }
}

/// The heartbeat's cursor, kept in the registry by name because it moves only once the window it read is off the
/// process's hands: in a supervisor once the leader has answered the beat, in a runtime once the report frame is on
/// the supervisor's pipe. A metrics source holds its own ([`Cursor`]).
pub const SUBSCRIBER_HEARTBEAT: &str = "heartbeat";

/// The supervisor's cursor for what it has sent its runtime to mirror, moved once the frame is on the pipe.
pub const SUBSCRIBER_MIRROR: &str = "mirror";

/// Where one subscriber has read the registry to: each counter's total as of its last move. Gauges need none.
pub type Cursor = HashMap<MetricKey, MetricValue>;

/// The process-wide registry. Counters are monotonic and never reset by a read: each subscriber remembers what it
/// last took and gets the difference, so any number of subscribers coexist. A named cursor moves once its delivery
/// has landed, so a failed delivery there loses nothing; a metrics source's own cursor moves on the read.
#[derive(Default)]
pub struct Metrics {
    values: Mutex<HashMap<MetricKey, MetricValue>>,
    /// The named cursors, each moved only once its delivery has landed.
    cursors: Mutex<HashMap<String, Cursor>>,
    /// What another process of this worker recorded and sent here ([`Metrics::mirror`]). A metrics source and the
    /// metrics endpoint read it beside `values`; no named cursor does, so it is never reported onward and the leader
    /// hears each series once, from the process that owns it.
    mirrored: Mutex<HashMap<MetricKey, MetricValue>>,
}

impl Metrics {
    pub fn new() -> Metrics {
        Metrics::default()
    }

    /// Add `delta` to `name{labels}`. A no-op on a name already used as a `Gauge`: mixing kinds is a caller bug, and it
    /// degrades rather than aborts. A `delta` of 0 returns before the lock, since hot paths make it per batch for every
    /// idle route; [`Metrics::start`] is what starts a series at 0.
    pub fn incr(&self, name: &'static str, labels: &[(&'static str, &str)], delta: u64) {
        if delta == 0 {
            return;
        }
        let key = MetricKey::new(name, labels);
        let mut values = self.values.lock().unwrap();
        match values.entry(key).or_insert(MetricValue::Counter(0)) {
            MetricValue::Counter(v) => *v += delta,
            MetricValue::FloatCounter(_) | MetricValue::Gauge(_) => {}
        }
    }

    /// Raise counter `name{labels}` to `total`, a count something else keeps for the life of this process. A total
    /// below what it holds changes nothing, so the counter never falls. A no-op on a name already used as another
    /// kind.
    pub fn count_to(&self, name: &'static str, labels: &[(&'static str, &str)], total: u64) {
        let key = MetricKey::new(name, labels);
        let mut values = self.values.lock().unwrap();
        if let MetricValue::Counter(held) = values.entry(key).or_insert(MetricValue::Counter(0)) {
            *held = (*held).max(total);
        }
    }

    /// Add `seconds` to `name{labels}`, as [`Metrics::incr`] adds a whole number. A no-op on a name already used as
    /// another kind.
    pub fn incr_seconds(&self, name: &'static str, labels: &[(&'static str, &str)], seconds: f64) {
        if seconds == 0.0 {
            return;
        }
        let key = MetricKey::new(name, labels);
        let mut values = self.values.lock().unwrap();
        match values.entry(key).or_insert(MetricValue::FloatCounter(0.0)) {
            MetricValue::FloatCounter(v) => *v += seconds,
            MetricValue::Counter(_) | MetricValue::Gauge(_) => {}
        }
    }

    /// Start counter `name{labels}` at 0 if nothing has written it, so it reports 0 rather than nothing until it first
    /// moves. Once, where the series is known to exist: a thread's start, not its hot path.
    pub fn start(&self, name: &'static str, labels: &[(&'static str, &str)]) {
        let key = MetricKey::new(name, labels);
        self.values
            .lock()
            .unwrap()
            .entry(key)
            .or_insert(MetricValue::Counter(0));
    }

    /// Set `name{labels}` to `value`, overwriting whatever was there.
    pub fn gauge(&self, name: &'static str, labels: &[(&'static str, &str)], value: f64) {
        let key = MetricKey::new(name, labels);
        self.values.lock().unwrap().insert(key, MetricValue::Gauge(value));
    }

    /// Every current sample, cursors untouched.
    pub fn snapshot(&self) -> Vec<MetricSample> {
        self.values
            .lock()
            .unwrap()
            .iter()
            .map(|(key, value)| MetricSample {
                key: key.clone(),
                value: *value,
            })
            .collect()
    }

    /// Every counter's increase since `subscriber` last committed, plus every gauge's current value. The cursor moves
    /// only in [`Metrics::commit_delta`], once the samples have landed somewhere.
    pub fn peek_delta(&self, subscriber: &str) -> Vec<MetricSample> {
        let values = self.values.lock().unwrap();
        let cursors = self.cursors.lock().unwrap();
        delta(&values, cursors.get(subscriber))
    }

    /// Advance `subscriber`'s cursor past `delivered`. Anything not named stays owed.
    pub fn commit_delta(&self, subscriber: &str, delivered: &[MetricSample]) {
        let mut cursors = self.cursors.lock().unwrap();
        advance(cursors.entry(subscriber.to_string()).or_default(), delivered);
    }

    /// Forget every key whose `thread` label names a thread not in `live`, and answer how many went.
    ///
    /// Values and the named cursors together. A thread id is positional -- `reader-0`, `transform-0`, `egress-0` --
    /// so a later deployment reuses it. Dropping a key's value while a cursor still remembered its old total would
    /// make the reused key report nothing until it climbed back past that total, since [`Metrics::peek_delta`]
    /// subtracts the cursor and saturates at zero. A cursor a metrics source holds is out of reach here: after a
    /// retirement, that source reads a reused key low until it climbs back past the total the cursor remembers.
    ///
    /// Orphans only, and never a running thread's. Every sample carries a `thread` label, so retiring a stopping
    /// deployment's own threads would empty the registry on each redeploy -- the reset that having one registry per
    /// process exists to stop. What is left behind is a thread the new deployment has no counterpart for: a reader
    /// for a source that was deleted, a pool thread past a lowered `thread_limit`. Their series would otherwise
    /// report on every heartbeat forever.
    pub fn retire_threads_except(&self, live: &[&str]) -> usize {
        let live: std::collections::HashSet<&str> = live.iter().copied().collect();
        let orphan = |key: &MetricKey| {
            key.labels
                .iter()
                .any(|(k, v)| *k == THREAD_LABEL && !live.contains(v.as_str()))
        };

        // One lock order everywhere: values, then cursors.
        let mut values = self.values.lock().unwrap();
        let mut cursors = self.cursors.lock().unwrap();
        let before = values.len();
        values.retain(|key, _| !orphan(key));
        for seen in cursors.values_mut() {
            seen.retain(|key, _| !orphan(key));
        }
        before - values.len()
    }

    /// Fold in samples another process read out of its own registry: a counter's increase adds, a gauge replaces.
    pub fn mirror(&self, samples: impl IntoIterator<Item = MetricSample>) {
        let mut mirrored = self.mirrored.lock().unwrap();
        for sample in samples {
            let held = mirrored.entry(sample.key).or_insert(MetricValue::Counter(0));
            match (held, sample.value) {
                (MetricValue::Counter(total), MetricValue::Counter(more)) => *total += more,
                (MetricValue::FloatCounter(total), MetricValue::FloatCounter(more)) => *total += more,
                (held, value) => *held = value,
            }
        }
    }

    /// [`Metrics::snapshot`] over what this process mirrors: each counter's total since this process started
    /// mirroring it.
    pub fn snapshot_mirrored(&self) -> Vec<MetricSample> {
        delta(&self.mirrored.lock().unwrap(), None)
    }

    /// Drop the mirrored gauges, once the process that sent them is gone, since nothing now says what they measured.
    /// Its counters stay, and the next process's counts add to them: a total this process reports, alone or summed
    /// with its own share, then never falls, which Prometheus would read as a reset and count again.
    pub fn forget_mirrored(&self) {
        self.mirrored.lock().unwrap().retain(|_, value| value.is_counter());
    }

    /// [`Metrics::take_delta`] over what this process mirrors rather than what it recorded.
    pub fn take_mirrored_delta(&self, cursor: &mut Cursor) -> Vec<MetricSample> {
        let out = delta(&self.mirrored.lock().unwrap(), Some(cursor));
        advance(cursor, &out);
        out
    }

    /// Read and move a cursor the caller holds, for a subscriber that never learns whether delivery succeeded
    /// (`MetricsSource`). The cursor is the caller's alone, so no other subscriber moves it between the read and the
    /// move.
    pub fn take_delta(&self, cursor: &mut Cursor) -> Vec<MetricSample> {
        let out = delta(&self.values.lock().unwrap(), Some(cursor));
        advance(cursor, &out);
        out
    }
}

/// Every counter's increase past `seen`, 0 included, plus every gauge's current value. With no cursor, a counter's
/// increase is its whole total. An unmoved counter reports 0 so a reader can tell idle from silent: only a process
/// that stopped reporting leaves a gap.
fn delta(values: &HashMap<MetricKey, MetricValue>, seen: Option<&Cursor>) -> Vec<MetricSample> {
    let mut out = Vec::with_capacity(values.len());
    for (key, value) in values.iter() {
        let last = seen.and_then(|s| s.get(key));
        let sampled = match *value {
            MetricValue::Gauge(g) => MetricValue::Gauge(g),
            MetricValue::Counter(total) => match last {
                Some(MetricValue::Counter(last)) => MetricValue::Counter(total.saturating_sub(*last)),
                _ => MetricValue::Counter(total),
            },
            MetricValue::FloatCounter(total) => match last {
                Some(MetricValue::FloatCounter(last)) => MetricValue::FloatCounter((total - last).max(0.0)),
                _ => MetricValue::FloatCounter(total),
            },
        };
        out.push(MetricSample {
            key: key.clone(),
            value: sampled,
        });
    }
    out
}

/// Move `seen` past every counter in `delivered`.
fn advance(seen: &mut Cursor, delivered: &[MetricSample]) {
    for sample in delivered {
        let held = seen.entry(sample.key.clone());
        match sample.value {
            MetricValue::Counter(reported) => match held.or_insert(MetricValue::Counter(0)) {
                MetricValue::Counter(total) => *total += reported,
                other => *other = MetricValue::Counter(reported),
            },
            MetricValue::FloatCounter(reported) => match held.or_insert(MetricValue::FloatCounter(0.0)) {
                MetricValue::FloatCounter(total) => *total += reported,
                other => *other = MetricValue::FloatCounter(reported),
            },
            MetricValue::Gauge(_) => {}
        }
    }
}

/// One thread's view of the registry. Every sample written through it carries a `thread` label.
#[derive(Clone)]
pub struct MetricsHandle {
    registry: Arc<Metrics>,
    thread: Arc<str>,
}

impl MetricsHandle {
    pub fn new(registry: Arc<Metrics>, thread: impl AsRef<str>) -> MetricsHandle {
        MetricsHandle {
            registry,
            thread: Arc::from(thread.as_ref()),
        }
    }

    pub fn registry(&self) -> &Arc<Metrics> {
        &self.registry
    }

    pub fn incr(&self, name: &'static str, labels: &[(&'static str, &str)], delta: u64) {
        self.registry.incr(name, &self.with_thread(labels), delta);
    }

    pub fn start(&self, name: &'static str, labels: &[(&'static str, &str)]) {
        self.registry.start(name, &self.with_thread(labels));
    }

    pub fn incr_seconds(&self, name: &'static str, labels: &[(&'static str, &str)], seconds: f64) {
        self.registry.incr_seconds(name, &self.with_thread(labels), seconds);
    }

    pub fn gauge(&self, name: &'static str, labels: &[(&'static str, &str)], value: f64) {
        self.registry.gauge(name, &self.with_thread(labels), value);
    }

    fn with_thread<'a>(&'a self, labels: &[(&'static str, &'a str)]) -> Vec<(&'static str, &'a str)> {
        let mut out = Vec::with_capacity(labels.len() + 1);
        out.extend_from_slice(labels);
        out.push((THREAD_LABEL, &self.thread));
        out
    }
}

/// The label `MetricsHandle` stamps on everything it writes.
pub const THREAD_LABEL: &str = "thread";

/// Config for the smeltr metrics source (`plan::topology::SourceKind::Metrics`).
#[derive(Clone, Copy, Debug)]
pub struct MetricsSourceConfig {
    /// How long to accumulate before draining a window of samples.
    pub bucket: Duration,
}

/// An ordinary `Source` whose "waiting for data" is a wall clock.
pub struct MetricsSource {
    metrics: std::sync::Arc<Metrics>,
    /// Where this source has read to. Its own: any number of metrics sources in one process, a capture's included,
    /// each read every count. Its first poll carries what the process has counted since it started.
    cursor: Cursor,
    /// The same, over what the registry mirrors from the worker's other process.
    mirrored: Cursor,
    bucket: Duration,
    /// `None` until the first poll, which is due immediately.
    last_drain: Option<Instant>,
    next_offset: i64,
    /// A fixed synthetic `__source.topic`, so a route can select metrics rows like any other source's.
    topic: std::sync::Arc<str>,
    /// What a record's labels name its subjects by.
    subjects: Arc<Subjects>,
}

/// The author's own names for what a deployment numbers by position, indexed by that position: `SourceId`, `DestId`,
/// `PipelineConfig::id`, and within a pipeline the `FunctionId` (`PipelineConfig::step_ids`). Built from the same
/// compiled topology that produced every id the deployment labels a metric with.
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct Subjects {
    pub sources: Vec<String>,
    pub destinations: Vec<String>,
    pub pipelines: Vec<String>,
    pub steps: Vec<Vec<String>>,
}

impl Subjects {
    /// Rewrite every `source`/`destination`/`pipeline`/`function` label from its positional id to the author's
    /// name. A `function` resolves through its own pipeline's steps, so `pipeline` is read while still positional
    /// and rewritten last. A capture's thread labels by name already, since its source may be in no deployment.
    pub fn resolve<K: AsRef<str>>(&self, labels: &mut [(K, String)]) {
        if labels
            .iter()
            .any(|(k, v)| k.as_ref() == THREAD_LABEL && v == crate::worker::capture::CAPTURE_LABEL)
        {
            return;
        }
        let steps = labels
            .iter()
            .find(|(k, _)| k.as_ref() == "pipeline")
            .and_then(|(_, v)| v.parse::<usize>().ok())
            .and_then(|i| self.steps.get(i))
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        for (key, value) in labels.iter_mut() {
            let names = match key.as_ref() {
                "source" => &self.sources[..],
                "destination" => &self.destinations[..],
                "function" => steps,
                _ => continue,
            };
            *value = resolve_subject(names, value);
        }
        for (key, value) in labels.iter_mut() {
            if key.as_ref() == "pipeline" {
                *value = resolve_subject(&self.pipelines, value);
            }
        }
    }
}

/// `value` as an index into `names`, or `value` unchanged if it is not one -- a config edit that deletes or reorders
/// an unrelated source or destination must not silently re-attribute another one's durable history to it
/// (`config_file::load::index_by_id` assigns ids positionally, by enumeration order).
///
/// The fallback covers a label already carrying a name rather than an id, so a subject this does not recognize
/// still reaches its reader rather than being dropped.
fn resolve_subject(names: &[String], value: &str) -> String {
    match value.parse::<usize>().ok().and_then(|i| names.get(i)) {
        Some(name) => name.clone(),
        None => value.to_string(),
    }
}

/// The `__source.topic` value every `MetricsSource` record carries.
pub const METRICS_TOPIC: &str = "__smeltr_metrics";

impl MetricsSource {
    pub fn new(metrics: std::sync::Arc<Metrics>, config: MetricsSourceConfig) -> MetricsSource {
        MetricsSource {
            metrics,
            cursor: Cursor::default(),
            mirrored: Cursor::default(),
            bucket: config.bucket,
            last_drain: None,
            next_offset: 0,
            topic: std::sync::Arc::from(METRICS_TOPIC),
            subjects: Arc::default(),
        }
    }

    /// `self`, labeling each record's subjects by the names in `subjects`.
    pub fn with_subjects(mut self, subjects: Arc<Subjects>) -> MetricsSource {
        self.subjects = subjects;
        self
    }
}

impl Source for MetricsSource {
    /// Empty until the bucket has elapsed: an ordinary empty poll.
    ///
    /// Waiting out the bucket is this source's "waiting for data", so it is spent inside the call like any other
    /// source's blocking poll. Returning empty the moment the bucket has not elapsed would hand the reader thread
    /// straight back a turn it has no wait of its own to take, and spin a core for the life of the deployment.
    fn poll_batch(
        &mut self,
        max_rows: usize,
        max_bytes: usize,
        timeout: Duration,
    ) -> Result<Vec<SourceRecord>, IoError> {
        if let Some(last) = self.last_drain {
            let waited = last.elapsed();
            if waited < self.bucket {
                std::thread::sleep((self.bucket - waited).min(timeout));
                // The caller's budget was the shorter of the two: give it back the empty poll it asked for.
                if last.elapsed() < self.bucket {
                    return Ok(Vec::new());
                }
            }
        }
        self.last_drain = Some(Instant::now());

        let ts_millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);

        // Samples past `max_rows`/`max_bytes` are gone: at-least-once does not apply to metrics.
        let mut out = crate::io::RecordChunkBuilder::new();
        let mut bytes = 0usize;
        // What this process recorded, then what the worker's other process did: one worker, one stream.
        let mut samples = self.metrics.take_delta(&mut self.cursor);
        samples.extend(self.metrics.take_mirrored_delta(&mut self.mirrored));
        // Idle counters last, so a cutoff drops a 0 before it drops a count.
        samples.sort_by_key(|s| s.value.is_idle());
        for mut sample in samples {
            if out.len() >= max_rows {
                break;
            }
            self.subjects.resolve(&mut sample.key.labels);
            let mut raw = Vec::new();
            sample.write_json(ts_millis, &mut raw);
            if !out.is_empty() && bytes + raw.len() > max_bytes {
                break;
            }
            bytes += raw.len();
            out.push(&raw, self.next_offset, 0, self.topic.clone(), None);
            self.next_offset += 1;
        }
        Ok(out.finish())
    }

    /// Nothing upstream to acknowledge.
    fn commit(&mut self, _offsets: &CommitMap) -> Result<(), IoError> {
        Ok(())
    }
}

pub mod exposition;

#[cfg(test)]
mod fuzz;

#[cfg(test)]
mod tests {
    use super::*;

    fn value_of(m: &Metrics, name: &'static str, labels: &[(&'static str, &str)]) -> Option<MetricValue> {
        let key = MetricKey::new(name, labels);
        m.snapshot().into_iter().find(|s| s.key == key).map(|s| s.value)
    }

    #[test]
    fn resolve_subject_returns_the_authors_name_for_an_id_in_range_and_the_raw_value_otherwise() {
        let names = vec!["orders-west".to_string(), "orders-east".to_string()];
        assert_eq!(resolve_subject(&names, "0"), "orders-west");
        assert_eq!(resolve_subject(&names, "1"), "orders-east");
        // Past what this deployment knows: kept, not dropped.
        assert_eq!(resolve_subject(&names, "5"), "5");
        // Not a positional id at all: passed through unchanged.
        assert_eq!(resolve_subject(&names, "orders-west"), "orders-west");
    }

    #[test]
    fn incr_creates_and_sums_a_counter() {
        let m = Metrics::new();
        m.incr("rows_in", &[("source", "a")], 3);
        m.incr("rows_in", &[("source", "a")], 4);
        assert!(matches!(
            value_of(&m, "rows_in", &[("source", "a")]),
            Some(MetricValue::Counter(7))
        ));
    }

    #[test]
    fn different_labels_are_different_series() {
        let m = Metrics::new();
        m.incr("rows_in", &[("source", "a")], 1);
        m.incr("rows_in", &[("source", "b")], 5);
        assert!(matches!(
            value_of(&m, "rows_in", &[("source", "a")]),
            Some(MetricValue::Counter(1))
        ));
        assert!(matches!(
            value_of(&m, "rows_in", &[("source", "b")]),
            Some(MetricValue::Counter(5))
        ));
    }

    #[test]
    fn gauge_is_last_write_wins() {
        let m = Metrics::new();
        m.gauge("queue_depth", &[], 3.0);
        m.gauge("queue_depth", &[], 1.0);
        assert!(matches!(
            value_of(&m, "queue_depth", &[]),
            Some(MetricValue::Gauge(v)) if v == 1.0
        ));
    }

    #[test]
    fn a_read_reports_a_delta_without_resetting_the_counter() {
        let m = Metrics::new();
        m.incr("rows_in", &[], 10);
        m.gauge("queue_depth", &[], 2.0);

        let mut s = Cursor::default();
        let read = m.take_delta(&mut s);
        assert_eq!(read.len(), 2);

        // The stored counter still holds the running total, not zero.
        assert!(matches!(value_of(&m, "rows_in", &[]), Some(MetricValue::Counter(10))));
        assert!(matches!(
            value_of(&m, "queue_depth", &[]),
            Some(MetricValue::Gauge(v)) if v == 2.0
        ));
    }

    #[test]
    fn a_read_reflects_only_writes_since_that_subscribers_last_read() {
        let m = Metrics::new();
        m.incr("rows_in", &[], 10);
        let mut s = Cursor::default();
        m.take_delta(&mut s);
        m.incr("rows_in", &[], 3);
        let second = m.take_delta(&mut s);
        let sample = second.iter().find(|s| s.key.name == "rows_in").unwrap();
        assert!(matches!(sample.value, MetricValue::Counter(3)));
    }

    #[test]
    fn subscribers_do_not_consume_each_others_increments() {
        let m = Metrics::new();
        m.incr("rows_in", &[], 10);

        let (mut a, mut b) = (Cursor::default(), Cursor::default());
        let read_a = m.take_delta(&mut a);
        let read_b = m.take_delta(&mut b);
        assert!(matches!(read_a[0].value, MetricValue::Counter(10)));
        assert!(matches!(read_b[0].value, MetricValue::Counter(10)));

        // And each moves on independently from there.
        m.incr("rows_in", &[], 4);
        let a2 = m.take_delta(&mut a);
        assert!(matches!(a2[0].value, MetricValue::Counter(4)));
    }

    #[test]
    fn an_unchanged_counter_reports_zero_beside_the_gauge() {
        let m = Metrics::new();
        m.incr("rows_in", &[], 10);
        m.gauge("queue_depth", &[], 2.0);
        let mut s = Cursor::default();
        m.take_delta(&mut s);

        let second = m.take_delta(&mut s);
        assert_eq!(second.len(), 2);
        let rows = second.iter().find(|s| s.key.name == "rows_in").unwrap();
        assert!(matches!(rows.value, MetricValue::Counter(0)));
    }

    #[test]
    fn a_zero_delta_is_a_no_op_and_start_begins_a_series_at_zero() {
        let m = Metrics::new();
        m.incr("rows_in", &[("source", "a")], 0);
        assert!(value_of(&m, "rows_in", &[("source", "a")]).is_none());

        m.start("rows_in", &[("source", "a")]);
        let read = m.take_delta(&mut Cursor::default());
        assert!(matches!(
            read[..],
            [MetricSample {
                value: MetricValue::Counter(0),
                ..
            }]
        ));

        m.incr("rows_in", &[("source", "a")], 5);
        m.start("rows_in", &[("source", "a")]);
        assert!(matches!(
            value_of(&m, "rows_in", &[("source", "a")]),
            Some(MetricValue::Counter(5))
        ));
    }

    /// Past the row budget a drain drops samples, and an idle counter goes before one that moved.
    #[test]
    fn a_full_drain_drops_idle_counters_before_moving_ones() {
        let m = Arc::new(Metrics::new());
        for source in ["a", "b", "c"] {
            m.start("rows_in", &[("source", source)]);
        }
        m.incr("rows_in", &[("source", "moved")], 5);
        let mut source = MetricsSource::new(m, MetricsSourceConfig { bucket: Duration::ZERO });
        let rows = source.poll_batch(1, 1 << 20, Duration::ZERO).unwrap();
        let row: serde_json::Value = serde_json::from_slice(rows[0].bytes().unwrap()).unwrap();
        assert_eq!(
            (row["source"].as_str(), row["value"].as_u64()),
            (Some("moved"), Some(5))
        );
    }

    #[test]
    fn a_sample_encodes_as_a_tidy_json_row() {
        let sample = MetricSample {
            key: MetricKey::new(
                "smeltr_rows_dropped_total",
                &[("pipeline", "orders"), ("function", "2")],
            ),
            value: MetricValue::Counter(37),
        };
        let mut out = Vec::new();
        sample.write_json(1_699_999_999_000, &mut out);
        let text = std::str::from_utf8(&out).unwrap();
        assert_eq!(
            text,
            r#"{"metric":"smeltr_rows_dropped_total","pipeline":"orders","function":"2","value":37,"ts":1699999999000}"#
        );
    }

    #[test]
    fn a_label_value_needing_escaping_round_trips_through_the_shared_json_escaper() {
        let sample = MetricSample {
            key: MetricKey::new("smeltr_x", &[("pipeline", "quote\"here")]),
            value: MetricValue::Counter(1),
        };
        let mut out = Vec::new();
        sample.write_json(0, &mut out);
        let parsed: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(parsed["pipeline"], "quote\"here");
    }

    /// A metrics source reads what the worker's other process recorded beside this one's: a mirrored counter adds
    /// its increase and is read once, a mirrored gauge reads as its latest value.
    #[test]
    fn metrics_source_reads_the_mirror() {
        let m = std::sync::Arc::new(Metrics::new());
        let sample = |name: &'static str, value| MetricSample {
            key: MetricKey { name, labels: vec![] },
            value,
        };
        m.mirror([
            sample("c", MetricValue::Counter(2)),
            sample("g", MetricValue::Gauge(1.0)),
        ]);
        m.mirror([
            sample("c", MetricValue::Counter(3)),
            sample("g", MetricValue::Gauge(4.0)),
        ]);
        let mut src = MetricsSource::new(m.clone(), MetricsSourceConfig { bucket: Duration::ZERO });
        let read = |src: &mut MetricsSource| -> Vec<serde_json::Value> {
            let mut rows: Vec<serde_json::Value> = src
                .poll_batch(100, 1 << 20, Duration::ZERO)
                .unwrap()
                .iter()
                .map(|r| serde_json::from_slice(r.bytes().unwrap()).unwrap())
                .collect();
            rows.sort_by_key(|r| r["metric"].to_string());
            rows
        };

        let first = read(&mut src);
        assert_eq!((&first[0]["metric"], &first[0]["value"]), (&"c".into(), &5.into()));
        assert_eq!((&first[1]["metric"], &first[1]["value"]), (&"g".into(), &4.into()));

        m.mirror([sample("c", MetricValue::Counter(1))]);
        let second = read(&mut src);
        assert_eq!((&second[0]["metric"], &second[0]["value"]), (&"c".into(), &1.into()));
    }

    #[test]
    fn metrics_source_reports_nothing_before_its_bucket_elapses() {
        let m = std::sync::Arc::new(Metrics::new());
        m.incr("rows_in", &[], 5);
        let mut src = MetricsSource::new(
            m,
            MetricsSourceConfig {
                bucket: Duration::from_secs(3600),
            },
        );
        let first = src.poll_batch(100, 1 << 20, Duration::ZERO).unwrap();
        assert_eq!(first.len(), 1, "first poll always drains immediately");
        let second = src.poll_batch(100, 1 << 20, Duration::ZERO).unwrap();
        assert!(second.is_empty(), "bucket hasn't elapsed yet");
    }

    #[test]
    fn metrics_source_emits_one_record_per_sample_as_json() {
        let m = std::sync::Arc::new(Metrics::new());
        m.incr("smeltr_rows_polled_total", &[("source", "0")], 12);
        let mut src = MetricsSource::new(m, MetricsSourceConfig { bucket: Duration::ZERO });
        let records = src.poll_batch(100, 1 << 20, Duration::ZERO).unwrap();
        assert_eq!(records.len(), 1);
        let parsed: serde_json::Value = serde_json::from_slice(records[0].bytes().unwrap()).unwrap();
        assert_eq!(parsed["metric"], "smeltr_rows_polled_total");
        assert_eq!(parsed["source"], "0");
        assert_eq!(parsed["value"], 12);
    }

    /// A record names its subjects as the author did. A `function` resolves through its own pipeline's steps, and a
    /// capture's labels are names already.
    #[test]
    fn metrics_source_labels_its_subjects_by_name() {
        let m = std::sync::Arc::new(Metrics::new());
        m.incr(
            "smeltr_function_calls_total",
            &[("pipeline", "1"), ("function", "0")],
            2,
        );
        m.incr("smeltr_rows_sent_total", &[("destination", "0")], 3);
        MetricsHandle::new(m.clone(), crate::worker::capture::CAPTURE_LABEL).incr(
            "smeltr_capture_records_total",
            &[("source", "0")],
            1,
        );
        let subjects = Subjects {
            sources: vec!["orders".to_string()],
            destinations: vec!["search".to_string()],
            pipelines: vec!["audit".to_string(), "orders".to_string()],
            steps: vec![vec!["a".to_string()], vec!["parse".to_string()]],
        };
        let mut src = MetricsSource::new(m, MetricsSourceConfig { bucket: Duration::ZERO })
            .with_subjects(std::sync::Arc::new(subjects));
        let records = src.poll_batch(100, 1 << 20, Duration::ZERO).unwrap();
        let rows: Vec<serde_json::Value> = records
            .iter()
            .map(|r| serde_json::from_slice(r.bytes().unwrap()).unwrap())
            .collect();
        let calls = rows
            .iter()
            .find(|r| r["metric"] == "smeltr_function_calls_total")
            .unwrap();
        assert_eq!(
            (&calls["pipeline"], &calls["function"]),
            (&"orders".into(), &"parse".into())
        );
        let sent = rows.iter().find(|r| r["metric"] == "smeltr_rows_sent_total").unwrap();
        assert_eq!(sent["destination"], "search");
        // A capture's label is a name already, even one that reads as a position.
        let captured = rows
            .iter()
            .find(|r| r["metric"] == "smeltr_capture_records_total")
            .unwrap();
        assert_eq!(captured["source"], "0");
    }

    /// Any number of metrics sources read one registry, a capture's beside a deployment's, and none takes another's
    /// counts.
    #[test]
    fn two_metrics_sources_on_one_registry_each_read_every_count() {
        let m = std::sync::Arc::new(Metrics::new());
        m.incr("rows_in", &[], 7);
        let mut sources = [
            MetricsSource::new(m.clone(), MetricsSourceConfig { bucket: Duration::ZERO }),
            MetricsSource::new(m.clone(), MetricsSourceConfig { bucket: Duration::ZERO }),
        ];
        let value_read = |source: &mut MetricsSource| {
            let records = source.poll_batch(100, 1 << 20, Duration::ZERO).unwrap();
            assert_eq!(records.len(), 1, "one counter moved: {records:?}");
            let parsed: serde_json::Value = serde_json::from_slice(records[0].bytes().unwrap()).unwrap();
            parsed["value"].clone()
        };

        for source in &mut sources {
            assert_eq!(value_read(source), 7);
        }
        m.incr("rows_in", &[], 4);
        for source in &mut sources {
            assert_eq!(value_read(source), 4);
        }
    }

    #[test]
    fn metrics_source_offsets_are_distinct_and_increasing() {
        let m = std::sync::Arc::new(Metrics::new());
        m.incr("a", &[], 1);
        m.incr("b", &[], 1);
        let mut src = MetricsSource::new(m, MetricsSourceConfig { bucket: Duration::ZERO });
        let records = src.poll_batch(100, 1 << 20, Duration::ZERO).unwrap();
        assert_eq!(records.len(), 2);
        assert_ne!(records[0].offset, records[1].offset);
    }

    /// A thread the new deployment has no counterpart for goes; one it still runs stays. Without the second half
    /// this would empty the registry on every redeploy, since every sample carries a `thread` label.
    #[test]
    fn retiring_drops_only_the_threads_that_are_gone() {
        let m = Metrics::new();
        m.incr("smeltr_rows_polled_total", &[("thread", "reader-0")], 5);
        m.incr("smeltr_rows_polled_total", &[("thread", "reader-9")], 7);
        m.gauge("smeltr_worker_readers", &[("thread", "deploy")], 2.0);

        let retired = m.retire_threads_except(&["reader-0", "deploy"]);

        assert_eq!(retired, 1, "only reader-9 is an orphan");
        let names: Vec<String> = m
            .snapshot()
            .into_iter()
            .map(|s| {
                s.key
                    .labels
                    .iter()
                    .find(|(k, _)| *k == THREAD_LABEL)
                    .map(|(_, v)| v.clone())
                    .unwrap_or_default()
            })
            .collect();
        assert!(names.contains(&"reader-0".to_string()));
        assert!(names.contains(&"deploy".to_string()));
        assert!(!names.contains(&"reader-9".to_string()));
    }

    /// The reason values and cursors are cleared together. A thread id is positional, so a later deployment reuses
    /// `reader-0`; a cursor left remembering the old total would swallow the new one, because `peek_delta`
    /// subtracts it and saturates.
    #[test]
    fn a_reused_thread_id_reports_its_own_total_after_a_retirement() {
        let m = Metrics::new();
        m.incr("smeltr_rows_polled_total", &[("thread", "reader-0")], 1_000);
        let first = m.peek_delta(SUBSCRIBER_HEARTBEAT);
        m.commit_delta(SUBSCRIBER_HEARTBEAT, &first);

        // The source is deleted, the deployment restarts without it, then a later one brings it back under the
        // same positional id.
        m.retire_threads_except(&[]);
        m.incr("smeltr_rows_polled_total", &[("thread", "reader-0")], 3);

        let again = m.peek_delta(SUBSCRIBER_HEARTBEAT);
        assert_eq!(again.len(), 1, "the reborn key reports: {again:?}");
        assert!(
            matches!(again[0].value, MetricValue::Counter(3)),
            "its own three, not three minus a thousand: {:?}",
            again[0].value
        );
    }

    /// A key carrying no `thread` label at all is nobody's orphan.
    #[test]
    fn retiring_leaves_a_key_with_no_thread_label_alone() {
        let m = Metrics::new();
        m.incr("smeltr_process_wide", &[], 1);
        assert_eq!(m.retire_threads_except(&[]), 0);
        assert_eq!(m.snapshot().len(), 1);
    }

    /// `Source::poll_batch` is blocking, and a bucket that has not elapsed is this source's "no data yet". Returning
    /// at once instead handed the reader thread a turn it had no wait of its own to take, and spun a core.
    #[test]
    fn an_idle_poll_waits_out_the_callers_timeout_rather_than_returning_at_once() {
        let registry = std::sync::Arc::new(Metrics::new());
        let mut source = MetricsSource::new(
            registry,
            MetricsSourceConfig {
                bucket: Duration::from_secs(60),
            },
        );

        // The first poll is due immediately: it drains and starts the bucket.
        source.poll_batch(1024, 1 << 20, Duration::from_millis(50)).unwrap();

        let timeout = Duration::from_millis(120);
        let began = Instant::now();
        let idle = source.poll_batch(1024, 1 << 20, timeout).unwrap();
        let spent = began.elapsed();

        assert!(idle.is_empty(), "the bucket has 60s left to run");
        assert!(
            spent >= timeout.mul_f64(0.8),
            "the wait was spent inside the call, not handed back: {spent:?}"
        );
    }

    /// The wait is the caller's budget or what is left of the bucket, whichever is shorter -- a bucket shorter than
    /// the tick must not hold the reader past the point it has rows to hand over.
    #[test]
    fn a_bucket_shorter_than_the_timeout_drains_as_soon_as_it_elapses() {
        let registry = std::sync::Arc::new(Metrics::new());
        let mut source = MetricsSource::new(
            registry.clone(),
            MetricsSourceConfig {
                bucket: Duration::from_millis(40),
            },
        );
        source.poll_batch(1024, 1 << 20, Duration::from_millis(10)).unwrap();
        registry.incr("smeltr_rows_in_total", &[("thread", "reader-0")], 5);

        let began = Instant::now();
        let out = source.poll_batch(1024, 1 << 20, Duration::from_secs(30)).unwrap();
        let spent = began.elapsed();

        assert_eq!(out.len(), 1, "the bucket elapsed and drained what had accumulated");
        assert!(
            spent < Duration::from_secs(5),
            "it did not wait out the caller's 30s: {spent:?}"
        );
    }
}
