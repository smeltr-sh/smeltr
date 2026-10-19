//! The metrics source's offline fuzzer. Threads count, start and set series in one registry, the worker's other
//! process mirrors its own, the heartbeat reads and commits under its named cursor, orphaned threads retire, and
//! sources open beside each other and poll with random rows, bytes, buckets and waits. However it goes, each poll that
//! reads carries what the source owes: each counter's increase since it last read, every gauge's current value,
//! samples that moved before idle counters, as JSON that reads back as the sample under its author's names, within
//! the batch it asked for. What a cut batch leaves is gone.

use std::collections::HashMap;

use super::*;
use crate::fuzz::{self, Coverage, Rng};
use crate::worker::capture::CAPTURE_LABEL;

/// Metric names, some written as counters and as gauges both.
const NAMES: [&str; 4] = ["rows_in", "bytes_out", "queue_depth", "lag"];

/// Label keys, those `Subjects` renames among them.
const KEYS: [&str; 5] = ["source", "destination", "pipeline", "function", "route"];

/// The keys `Subjects` renames.
const SUBJECT_KEYS: [&str; 4] = ["source", "destination", "pipeline", "function"];

/// Label values: positions `Subjects` names, and ones it does not.
const VALUES: [&str; 7] = ["0", "1", "2", "9", "orders", "été \"q\"", ""];

/// Threads of this process that write.
const THREADS: [&str; 4] = ["reader-0", "reader-1", "egress-0", CAPTURE_LABEL];

/// Threads of the worker's other process, whose series are mirrored here.
const MIRRORED_THREADS: [&str; 2] = ["transform-0", "transform-1"];

/// One source, and where it has read to.
struct Reader {
    source: MetricsSource,
    /// Where it has read what this process recorded, and what it mirrors.
    cursor: Cursor,
    mirrored: Cursor,
    bucket: Duration,
    subjects: Option<Arc<Subjects>>,
    /// When it last read, as late as it could have been.
    last_drain: Option<Instant>,
    next_offset: i64,
}

fn random_labels(rng: &mut Rng) -> Vec<(&'static str, String)> {
    let mut keys = KEYS.to_vec();
    let mut labels = Vec::new();
    for _ in 0..rng.below(4) {
        let key = keys.remove(rng.below(keys.len()));
        labels.push((key, rng.pick(&VALUES).to_string()));
    }
    labels
}

fn gauge(rng: &mut Rng) -> f64 {
    *rng.pick(&[0.0, -0.0, 1.5, -3.25, 6.0, 1e15, 0.125])
}

/// Whether `a` and `b` are the same sample value, a gauge to the bit.
fn same(a: MetricValue, b: MetricValue) -> bool {
    match (a, b) {
        (MetricValue::Counter(a), MetricValue::Counter(b)) => a == b,
        (MetricValue::Gauge(a), MetricValue::Gauge(b))
        | (MetricValue::FloatCounter(a), MetricValue::FloatCounter(b)) => a.to_bits() == b.to_bits(),
        _ => false,
    }
}

fn idle(value: MetricValue) -> bool {
    value.is_idle()
}

/// What `subjects` renames `labels` to, as the design says: a positional id it holds becomes the author's name, a
/// function by its own pipeline's steps, and, where `capture_exempt`, a capture's labels stay as they are.
fn resolved(
    subjects: &Subjects,
    labels: &[(&'static str, String)],
    capture_exempt: bool,
) -> Vec<(&'static str, String)> {
    if capture_exempt && labels.iter().any(|(k, v)| *k == THREAD_LABEL && v == CAPTURE_LABEL) {
        return labels.to_vec();
    }
    let at = |names: &[String], v: &str| v.parse::<usize>().ok().and_then(|i| names.get(i).cloned());
    let pipeline = (labels.iter())
        .find(|(k, _)| *k == "pipeline")
        .and_then(|(_, v)| v.parse::<usize>().ok());
    (labels.iter())
        .map(|(k, v)| {
            let name = match *k {
                "source" => at(&subjects.sources, v),
                "destination" => at(&subjects.destinations, v),
                "pipeline" => at(&subjects.pipelines, v),
                "function" => pipeline
                    .and_then(|p| subjects.steps.get(p))
                    .and_then(|steps| at(steps, v)),
                _ => None,
            };
            (*k, name.unwrap_or_else(|| v.clone()))
        })
        .collect()
}

/// A sample as a record must read: its name, its labels, and its value.
#[derive(Debug)]
struct Owed {
    name: &'static str,
    labels: Vec<(&'static str, String)>,
    value: MetricValue,
}

/// A record, read back.
struct Record {
    name: String,
    labels: Vec<(String, String)>,
    value: serde_json::Number,
}

/// What `record` reads as, or why it is not a metrics record holding `labels` labels stamped between `from` and `to`.
fn read(record: &[u8], labels: usize, from: i64, to: i64) -> Result<Record, String> {
    let text = std::str::from_utf8(record).map_err(|e| format!("a record not UTF-8: {e}"))?;
    let json: serde_json::Map<String, serde_json::Value> =
        serde_json::from_str(text).map_err(|e| format!("a record not a JSON object: {e}: {text}"))?;
    // Its three keys and one per label: a key spelled twice reads as one.
    if json.len() != 3 + labels {
        return Err(format!("{text} holds other keys than its sample's"));
    }
    let ts = json.get("ts").and_then(serde_json::Value::as_i64);
    if ts.is_none_or(|ts| ts < from || ts > to) {
        return Err(format!("{text} is stamped outside {from}..{to}"));
    }
    let name = (json.get("metric").and_then(serde_json::Value::as_str))
        .ok_or_else(|| format!("{text} names no metric"))?
        .to_string();
    let value = match json.get("value") {
        Some(serde_json::Value::Number(n)) => n.clone(),
        _ => return Err(format!("{text} holds no number")),
    };
    let mut read = Vec::new();
    for (k, v) in &json {
        if !["metric", "value", "ts"].contains(&k.as_str()) {
            let v = v
                .as_str()
                .ok_or_else(|| format!("{text} holds a label that is not a string"))?;
            read.push((k.clone(), v.to_string()));
        }
    }
    Ok(Record {
        name,
        labels: read,
        value,
    })
}

/// Whether `number`, as a record spells it, is `value`.
fn reads_as(number: &serde_json::Number, value: MetricValue) -> bool {
    match value {
        MetricValue::Counter(n) => number.as_u64() == Some(n),
        MetricValue::FloatCounter(g) | MetricValue::Gauge(g) => number
            .as_f64()
            .is_some_and(|f| f.to_bits() == g.to_bits() || (f == 0.0 && g == 0.0)),
    }
}

/// Every counter's increase past `cursor`, and every gauge as it is, of `values`; `cursor` moved past them.
fn take(values: &HashMap<MetricKey, MetricValue>, cursor: &mut Cursor) -> Vec<(MetricKey, MetricValue)> {
    (values.iter())
        .map(|(key, value)| match value {
            MetricValue::Gauge(g) => (key.clone(), MetricValue::Gauge(*g)),
            MetricValue::Counter(total) => {
                let seen = match cursor.get(key) {
                    Some(MetricValue::Counter(seen)) => *seen,
                    _ => 0,
                };
                let increase = total.saturating_sub(seen);
                cursor.insert(key.clone(), MetricValue::Counter(seen + increase));
                (key.clone(), MetricValue::Counter(increase))
            }
            MetricValue::FloatCounter(total) => {
                let seen = match cursor.get(key) {
                    Some(MetricValue::FloatCounter(seen)) => *seen,
                    _ => 0.0,
                };
                let increase = (total - seen).max(0.0);
                cursor.insert(key.clone(), MetricValue::FloatCounter(seen + increase));
                (key.clone(), MetricValue::FloatCounter(increase))
            }
        })
        .collect()
}

#[test]
fn every_poll_carries_what_its_source_owes_moved_samples_first_within_its_batch() {
    let mut cov = Coverage::new(
        [
            "a counter",
            "a counter of seconds",
            "a counter started",
            "an increase of 0",
            "a gauge",
            "a counter on a gauge's name",
            "a gauge on a counter's name",
            "a series of no thread",
            "a mirrored counter",
            "a mirrored gauge",
            "a mirrored counter onto a mirrored gauge",
            "a thread retired",
            "a retired series written again under a reader's old total",
            "a heartbeat committing all it read",
            "a heartbeat committing part of what it read",
            "a heartbeat committing nothing",
            "a heartbeat reading a retired series afresh",
            "a label renamed by its subjects",
            "a function renamed through its pipeline",
            "a capture's labels left as they are",
            "a label naming no subject",
            "a source with no subjects",
            "an idle counter",
            "a poll inside its bucket",
            "a poll whose wait ends inside its bucket",
            "a poll waiting out its bucket",
            "a batch cut by its rows",
            "a batch cut by its bytes",
            "a lone record past its bytes",
            "two sources",
        ]
        .map(String::from),
    );
    fuzz::run(1..201, |seed| {
        let mut rng = Rng::seeded(seed);
        let registry = Arc::new(Metrics::new());
        let subjects = Arc::new(Subjects {
            sources: vec!["src-a".into(), "src-b".into()],
            destinations: vec!["dest-a".into()],
            pipelines: vec!["pipe-a".into(), "pipe-b".into(), "pipe-c".into()],
            steps: vec![vec!["parse".into()], vec![], vec!["enrich".into(), "drop".into()]],
        });
        let mut values: HashMap<MetricKey, MetricValue> = HashMap::new();
        let mut mirrored: HashMap<MetricKey, MetricValue> = HashMap::new();
        let mut heartbeat = Cursor::new();
        let mut retired: Vec<MetricKey> = Vec::new();
        let mut readers: Vec<Reader> = Vec::new();
        for round in 0..1 + rng.below(10) {
            if readers.is_empty() || (readers.len() < 3 && rng.chance(4)) {
                let bucket = match rng.chance(4) {
                    true => Duration::from_millis(200),
                    false => Duration::ZERO,
                };
                let named = (!rng.chance(5)).then(|| subjects.clone());
                let mut source = MetricsSource::new(registry.clone(), MetricsSourceConfig { bucket });
                match &named {
                    Some(subjects) => source = source.with_subjects(subjects.clone()),
                    None => cov.reach("a source with no subjects"),
                }
                readers.push(Reader {
                    source,
                    cursor: Cursor::new(),
                    mirrored: Cursor::new(),
                    bucket,
                    subjects: named,
                    last_drain: None,
                    next_offset: 0,
                });
                if readers.len() > 1 {
                    cov.reach("two sources");
                }
            }
            for _ in 0..rng.below(30) {
                let name = *rng.pick(&NAMES);
                let labels = random_labels(&mut rng);
                let borrowed: Vec<(&'static str, &str)> = labels.iter().map(|(k, v)| (*k, v.as_str())).collect();
                let op = rng.below(8);
                // Mirrored series are the other process's threads'; one written straight to the registry has none.
                let thread = match op {
                    7 => Some(*rng.pick(&MIRRORED_THREADS)),
                    _ if rng.chance(10) => None,
                    _ => Some(*rng.pick(&THREADS)),
                };
                let mut key_labels = labels.clone();
                key_labels.extend(thread.map(|t| (THREAD_LABEL, t.to_string())));
                let key = MetricKey {
                    name,
                    labels: key_labels,
                };
                if thread.is_none() {
                    cov.reach("a series of no thread");
                }
                let handle = thread.map(|t| MetricsHandle::new(registry.clone(), t));
                let rewrites = retired.contains(&key) && !values.contains_key(&key);
                match op {
                    3 => {
                        let seconds = match rng.chance(5) {
                            true => 0.0,
                            false => (1 + rng.next_u64() % 1_000_000) as f64 / 1e6,
                        };
                        match &handle {
                            Some(handle) => handle.incr_seconds(name, &borrowed, seconds),
                            None => registry.incr_seconds(name, &borrowed, seconds),
                        }
                        if seconds == 0.0 {
                            cov.reach("an increase of 0");
                            continue;
                        }
                        match values.get_mut(&key) {
                            Some(MetricValue::FloatCounter(total)) => {
                                cov.reach("a counter of seconds");
                                *total += seconds;
                            }
                            Some(_) => cov.reach("a counter on a gauge's name"),
                            None => {
                                cov.reach("a counter of seconds");
                                values.insert(key.clone(), MetricValue::FloatCounter(seconds));
                            }
                        }
                    }
                    0..=2 => {
                        let delta = match rng.chance(5) {
                            true => 0,
                            false => 1 + rng.next_u64() % 1000,
                        };
                        match &handle {
                            Some(handle) => handle.incr(name, &borrowed, delta),
                            None => registry.incr(name, &borrowed, delta),
                        }
                        if delta == 0 {
                            cov.reach("an increase of 0");
                            continue;
                        }
                        match values.get_mut(&key) {
                            Some(MetricValue::Counter(total)) => {
                                cov.reach("a counter");
                                *total += delta;
                            }
                            Some(_) => cov.reach("a counter on a gauge's name"),
                            None => {
                                cov.reach("a counter");
                                values.insert(key.clone(), MetricValue::Counter(delta));
                            }
                        }
                    }
                    4 => {
                        match &handle {
                            Some(handle) => handle.start(name, &borrowed),
                            None => registry.start(name, &borrowed),
                        }
                        cov.reach("a counter started");
                        values.entry(key.clone()).or_insert(MetricValue::Counter(0));
                    }
                    5 | 6 => {
                        let g = gauge(&mut rng);
                        match &handle {
                            Some(handle) => handle.gauge(name, &borrowed, g),
                            None => registry.gauge(name, &borrowed, g),
                        }
                        cov.reach("a gauge");
                        if values
                            .insert(key.clone(), MetricValue::Gauge(g))
                            .is_some_and(|v| v.is_counter())
                        {
                            cov.reach("a gauge on a counter's name");
                        }
                    }
                    _ => {
                        let value = match rng.chance(2) {
                            true => MetricValue::Counter(rng.next_u64() % 1000),
                            false => MetricValue::Gauge(gauge(&mut rng)),
                        };
                        registry.mirror([MetricSample {
                            key: key.clone(),
                            value,
                        }]);
                        let held = mirrored.entry(key.clone()).or_insert(MetricValue::Counter(0));
                        match (held, value) {
                            (MetricValue::Counter(total), MetricValue::Counter(more)) => {
                                cov.reach("a mirrored counter");
                                *total += more;
                            }
                            (held, value) => {
                                if matches!((&held, value), (MetricValue::Gauge(_), MetricValue::Counter(_))) {
                                    cov.reach("a mirrored counter onto a mirrored gauge");
                                }
                                if matches!(value, MetricValue::Gauge(_)) {
                                    cov.reach("a mirrored gauge");
                                }
                                *held = value;
                            }
                        }
                    }
                }
                // A reader that read the series before it retired remembers a total it now reads below.
                let below = |r: &Reader| r.cursor.get(&key).is_some_and(|seen| !seen.is_idle());
                if rewrites && values.contains_key(&key) && readers.iter().any(below) {
                    cov.reach("a retired series written again under a reader's old total");
                }
            }

            // The heartbeat reads what it is owed and commits all, part or none of it.
            if rng.chance(2) {
                let mut cursor = heartbeat.clone();
                let owed = take(&values, &mut cursor);
                let mut read = registry.peek_delta(SUBSCRIBER_HEARTBEAT);
                for (key, value) in &owed {
                    let Some(at) = read.iter().position(|s| s.key == *key && same(s.value, *value)) else {
                        return Err(format!(
                            "round {round}: the heartbeat was not owed {key:?} {value:?}: {read:?}"
                        ));
                    };
                    read.remove(at);
                    if retired.contains(key) && !heartbeat.contains_key(key) && !idle(*value) {
                        cov.reach("a heartbeat reading a retired series afresh");
                    }
                }
                if !read.is_empty() {
                    return Err(format!(
                        "round {round}: the heartbeat read what it was not owed: {read:?}"
                    ));
                }
                let mut owed: Vec<MetricSample> = owed
                    .into_iter()
                    .map(|(key, value)| MetricSample { key, value })
                    .collect();
                // In a fixed order, so a seed draws the same part of it on every run.
                owed.sort_by_cached_key(|s| format!("{:?}", s.key));
                let committed: Vec<MetricSample> = match rng.below(3) {
                    0 => {
                        cov.reach("a heartbeat committing all it read");
                        owed
                    }
                    1 => {
                        cov.reach("a heartbeat committing part of what it read");
                        owed.into_iter().filter(|_| rng.chance(2)).collect()
                    }
                    _ => {
                        cov.reach("a heartbeat committing nothing");
                        Vec::new()
                    }
                };
                registry.commit_delta(SUBSCRIBER_HEARTBEAT, &committed);
                advance(&mut heartbeat, &committed);
            }

            if rng.chance(5) {
                let live: Vec<&str> = THREADS.iter().copied().filter(|_| rng.chance(2)).collect();
                let orphan =
                    |k: &MetricKey| (k.labels.iter()).any(|(l, v)| *l == THREAD_LABEL && !live.contains(&v.as_str()));
                let gone: Vec<MetricKey> = values.keys().filter(|k| orphan(k)).cloned().collect();
                let count = registry.retire_threads_except(&live);
                if count != gone.len() {
                    return Err(format!("round {round}: retired {count} series, not {}", gone.len()));
                }
                if count > 0 {
                    cov.reach("a thread retired");
                }
                // The named cursors forget them too; a source's cursor is its own.
                heartbeat.retain(|k, _| !orphan(k));
                for key in gone {
                    values.remove(&key);
                    retired.push(key);
                }
            }

            for (r, reader) in readers.iter_mut().enumerate() {
                for _ in 0..rng.below(3) {
                    let most_rows = if rng.chance(2) { 4 } else { 500 };
                    let max_rows = rng.below(most_rows);
                    let most_bytes = if rng.chance(2) { 300 } else { 1 << 16 };
                    let max_bytes = rng.below(most_bytes);
                    // No wait, a wait shorter than the bucket, or one that outlasts it.
                    let timeout = match rng.below(3) {
                        0 => Duration::ZERO,
                        1 => Duration::from_millis(1 + rng.below(100) as u64),
                        _ => reader.bucket + Duration::from_millis(50),
                    };
                    let what = format!(
                        "round {round}, source {r}, max_rows {max_rows}, max_bytes {max_bytes}, waiting {timeout:?}"
                    );
                    // Whether the poll ends inside its bucket, by a margin that covers the poll itself; a poll too
                    // near its bucket's end to tell is not made.
                    let since = reader.last_drain.map(|at| at.elapsed());
                    let margin = Duration::from_millis(60);
                    let inside = since.is_some_and(|since| since + timeout + margin < reader.bucket);
                    let near = since.is_some_and(|since| since + timeout < reader.bucket + margin);
                    if !inside && near {
                        continue;
                    }
                    let from = millis(SystemTime::now());
                    let batch =
                        (reader.source.poll_batch(max_rows, max_bytes, timeout)).map_err(|e| format!("{what}: {e}"))?;
                    let to = millis(SystemTime::now());
                    // A poll held up past its bucket's end may have read it, and did if it took anything.
                    let held_up = reader.last_drain.is_some_and(|at| at.elapsed() >= reader.bucket);
                    let inside = inside && !(held_up && !batch.is_empty());
                    if inside {
                        cov.reach(match timeout.is_zero() {
                            true => "a poll inside its bucket",
                            false => "a poll whose wait ends inside its bucket",
                        });
                        if !batch.is_empty() {
                            return Err(format!("{what}: read inside its bucket"));
                        }
                        continue;
                    }
                    if since.is_some_and(|since| since < reader.bucket) {
                        cov.reach("a poll waiting out its bucket");
                    }
                    reader.last_drain = Some(Instant::now());
                    let mut owed = take(&values, &mut reader.cursor);
                    owed.extend(take(&mirrored, &mut reader.mirrored));
                    let mut owed: Vec<Owed> = (owed.into_iter())
                        .map(|(key, value)| {
                            let labels = match &reader.subjects {
                                Some(subjects) => {
                                    let labels = resolved(subjects, &key.labels, true);
                                    let unexempt = resolved(subjects, &key.labels, false);
                                    let value_of = |ls: &[(&'static str, String)], k: &str| {
                                        ls.iter().find(|(l, _)| *l == k).map(|(_, v)| v.clone())
                                    };
                                    if labels != key.labels {
                                        cov.reach("a label renamed by its subjects");
                                    }
                                    if value_of(&labels, "function") != value_of(&key.labels, "function") {
                                        cov.reach("a function renamed through its pipeline");
                                    }
                                    if labels == key.labels && unexempt != key.labels {
                                        cov.reach("a capture's labels left as they are");
                                    }
                                    if (unexempt.iter())
                                        .any(|(k, v)| SUBJECT_KEYS.contains(k) && v.parse::<usize>().is_ok())
                                    {
                                        cov.reach("a label naming no subject");
                                    }
                                    labels
                                }
                                None => key.labels.clone(),
                            };
                            Owed {
                                name: key.name,
                                labels,
                                value,
                            }
                        })
                        .collect();
                    let mut seen_idle = false;
                    let mut size = 0;
                    for record in &batch {
                        let raw = record.bytes().unwrap();
                        let at = owed.iter().position(|o| {
                            read(raw, o.labels.len(), from, to).is_ok_and(|read| {
                                read.name == o.name
                                    && (o.labels.iter())
                                        .all(|(k, v)| read.labels.iter().any(|(k2, v2)| k == k2 && v == v2))
                                    && reads_as(&read.value, o.value)
                            })
                        });
                        let Some(at) = at else {
                            return Err(format!(
                                "{what}: {} is no sample the source owes",
                                String::from_utf8_lossy(raw)
                            ));
                        };
                        let carried = owed.remove(at);
                        // Moved samples first: a cut drops an idle counter before one that moved.
                        if idle(carried.value) {
                            seen_idle = true;
                            cov.reach("an idle counter");
                        } else if seen_idle {
                            return Err(format!("{what}: {carried:?} after an idle counter"));
                        }
                        if record.offset != reader.next_offset
                            || &*record.topic != METRICS_TOPIC
                            || record.partition != 0
                        {
                            return Err(format!(
                                "{what}: a record at {}/{}/{}",
                                record.topic, record.partition, record.offset
                            ));
                        }
                        reader.next_offset += 1;
                        size += raw.len();
                    }
                    if batch.len() > 1 && size > max_bytes {
                        return Err(format!("{what}: a batch of {size} bytes, past its limit"));
                    }
                    if batch.len() == 1 && size > max_bytes {
                        cov.reach("a lone record past its bytes");
                    }
                    // What it left: only past its rows, or a sample that would pass its bytes, and idle ones only
                    // once every moved one is in.
                    if !owed.is_empty() {
                        if seen_idle && owed.iter().any(|o| !idle(o.value)) {
                            return Err(format!("{what}: an idle counter carried before a moved sample"));
                        }
                        if batch.len() == max_rows {
                            if max_rows > 0 {
                                cov.reach("a batch cut by its rows");
                            }
                        } else {
                            let moved_left = owed.iter().any(|o| !idle(o.value));
                            let mut next = owed.iter().filter(|o| moved_left != idle(o.value));
                            let too_large = next.any(|o| {
                                let mut raw = Vec::new();
                                let key = MetricKey {
                                    name: o.name,
                                    labels: o.labels.clone(),
                                };
                                MetricSample { key, value: o.value }.write_json(to, &mut raw);
                                !batch.is_empty() && size + raw.len() > max_bytes
                            });
                            if !too_large {
                                return Err(format!("{what}: left {owed:?} with room for them"));
                            }
                            cov.reach("a batch cut by its bytes");
                        }
                    }
                }
            }
        }
        Ok(())
    });
    cov.assert_complete();
}

fn millis(at: SystemTime) -> i64 {
    at.duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or_default()
}
