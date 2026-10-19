//! The logs source's offline fuzzer. Readers open and close at random moments, and between their polls events of
//! every level and every kind of field arrive, both said here through `tracing` and mirrored from the worker's other
//! process, more than a queue holds at times. Each poll asks for a random number of rows and bytes. However it goes,
//! each reader carries every event at or above its level that its queue held, once and in order, as JSON that reads
//! back as the event, within the batch it asked for.

use std::collections::VecDeque;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tracing_subscriber::layer::SubscriberExt;

use super::*;
use crate::fuzz::{self, Coverage, Rng};
use crate::logs::LOGS_CAPACITY;

/// Each level, and how a record names it.
const LEVELS: [(tracing::Level, &str); 5] = [
    (tracing::Level::ERROR, "error"),
    (tracing::Level::WARN, "warn"),
    (tracing::Level::INFO, "info"),
    (tracing::Level::DEBUG, "debug"),
    (tracing::Level::TRACE, "trace"),
];

/// Text a site, a message, a field's name or a field's value may hold, JSON's escapes among it.
const TEXTS: &[&str] = &[
    "",
    "plain",
    "été 🎉",
    "say \"hi\"",
    "back\\slash",
    "a\nnew\tline\r",
    "\u{0}\u{1f}\u{7f}",
    "</script>",
];

/// Each kind of field value, by what it tries.
const VALUES: &[&str] = &[
    "a string",
    "a negative integer",
    "an integer of no sign",
    "an unsigned integer past i64",
    "a float with a fraction",
    "a whole float",
    "a float JSON cannot spell",
    "a boolean",
];

/// The site every event said here through `tracing` is said at.
const TRACED: &str = "fuzz.traced";

fn value(rng: &mut Rng) -> Value {
    match rng.below(VALUES.len()) {
        0 => Value::Str(rng.pick(TEXTS).to_string()),
        1 => Value::Int(-(rng.next_u64() as i64 >> rng.below(64)).abs() - 1),
        2 => Value::Int((rng.next_u64() >> 1 >> rng.below(64)) as i64),
        3 => Value::Uint(u64::MAX - rng.next_u64() % 1000),
        4 => Value::Float(*rng.pick(&[0.1, -2.5e-300, 1.0 / 3.0, 6.02e23, f64::MIN_POSITIVE])),
        5 => Value::Float(*rng.pick(&[6.0, -0.0, 1e21, 0.0])),
        6 => Value::Float(*rng.pick(&[f64::NAN, f64::INFINITY, f64::NEG_INFINITY])),
        _ => Value::Bool(rng.chance(2)),
    }
}

/// What a field's value tries, as `VALUES` names it.
fn tries(value: &Value) -> &'static str {
    VALUES[match value {
        Value::Str(_) => 0,
        Value::Int(n) if *n < 0 => 1,
        Value::Int(_) => 2,
        Value::Uint(_) => 3,
        Value::Float(f) if !f.is_finite() => 6,
        Value::Float(f) if f.fract() == 0.0 => 5,
        Value::Float(_) => 4,
        Value::Bool(_) => 7,
    }]
}

fn unwritable(value: &Value) -> bool {
    matches!(value, Value::Float(f) if !f.is_finite())
}

/// A random event mirrored from the other process, said at `at`, numbered `n`.
fn mirrored(rng: &mut Rng, n: u64, at: SystemTime) -> Event {
    let fields = (0..rng.below(4))
        // Distinct names, each needing what escapes its text needs.
        .map(|i| (format!("{}{i}", rng.pick(TEXTS)).into(), value(rng)))
        .collect();
    Event {
        timestamp: at,
        level: rng.pick(&LEVELS).0,
        site: format!("fuzz.{n}.{}", rng.pick(TEXTS)).into(),
        message: rng.pick(TEXTS).to_string(),
        fields,
        worker: None,
        thread: rng.chance(2).then(|| rng.pick(TEXTS).to_string()),
        local_only: false,
        seq: n,
    }
}

/// Says a random event here through `tracing`, while delivering logs where `delivering`, and returns it as a record
/// of it must read, said between its two timestamps.
fn traced(rng: &mut Rng, delivering: bool) -> (Event, SystemTime) {
    let (level, _) = *rng.pick(&LEVELS);
    let (text, n, x) = (rng.pick(TEXTS).to_string(), rng.next_u64() as i64, rng.chance(2));
    let message = rng.pick(TEXTS).to_string();
    let before = SystemTime::now();
    let say = || match level {
        tracing::Level::ERROR => tracing::error!(target: TRACED, text = %text, n, x, "{message}"),
        tracing::Level::WARN => tracing::warn!(target: TRACED, text = %text, n, x, "{message}"),
        tracing::Level::INFO => tracing::info!(target: TRACED, text = %text, n, x, "{message}"),
        tracing::Level::DEBUG => tracing::debug!(target: TRACED, text = %text, n, x, "{message}"),
        _ => tracing::trace!(target: TRACED, text = %text, n, x, "{message}"),
    };
    let subscriber = tracing_subscriber::registry().with(crate::logs::LogLayer);
    tracing::subscriber::with_default(subscriber, || match delivering {
        true => crate::logs::while_delivering(say),
        false => say(),
    });
    let event = Event {
        timestamp: before,
        level,
        site: TRACED.into(),
        message,
        fields: vec![
            ("text".into(), Value::Str(text)),
            ("n".into(), Value::Int(n)),
            ("x".into(), Value::Bool(x)),
        ],
        worker: None,
        thread: std::thread::current().name().map(str::to_string),
        local_only: delivering,
        seq: 0,
    };
    (event, SystemTime::now())
}

fn millis(at: SystemTime) -> i64 {
    at.duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or_default()
}

/// Why `record` is not `event`, said between its timestamp and `by`, as JSON, or `Ok`.
fn check_json(record: &[u8], event: &Event, by: SystemTime) -> Result<(), String> {
    let text = std::str::from_utf8(record).map_err(|e| format!("a record not UTF-8: {e}"))?;
    let json: serde_json::Value = serde_json::from_str(text).map_err(|e| format!("a record not JSON: {e}: {text}"))?;
    let object = json
        .as_object()
        .ok_or_else(|| format!("a record not an object: {text}"))?;
    let level = LEVELS.iter().find(|(l, _)| *l == event.level).unwrap().1;
    let text_at = |key: &str| object.get(key).and_then(serde_json::Value::as_str);
    let said = object.get("timestamp").and_then(serde_json::Value::as_i64);
    let writable: Vec<_> = event.fields.iter().filter(|(_, v)| !unwritable(v)).collect();
    // No worker is set in this process, so a record names none.
    let keys = 4 + usize::from(event.thread.is_some()) + usize::from(!writable.is_empty());
    if said.is_none_or(|said| said < millis(event.timestamp) || said > millis(by))
        || text_at("level") != Some(level)
        || text_at("site") != Some(&event.site)
        || text_at("message") != Some(&event.message)
        || text_at("thread") != event.thread.as_deref()
        || object.len() != keys
    {
        return Err(format!("{text} is not {event:?}"));
    }
    // The `fields` key only where a field can be written.
    if writable.is_empty() {
        return Ok(());
    }
    let fields = object
        .get("fields")
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| format!("{text} holds no fields"))?;
    if fields.len() != writable.len() {
        return Err(format!("{text} holds other fields than {event:?}"));
    }
    for (name, value) in writable {
        let got = &fields[name.as_ref()];
        let same = match value {
            Value::Str(s) => got.as_str() == Some(s),
            Value::Int(n) => got.as_i64() == Some(*n),
            Value::Uint(n) => got.as_u64() == Some(*n),
            // Read from its own spelling, so it is the float written, to the bit, and not an integer.
            Value::Float(n) => {
                let key = format!("{}:", serde_json::to_string(name.as_ref()).unwrap());
                let spelled = text
                    .split_once(&key)
                    .map(|(_, rest)| rest.split([',', '}']).next().unwrap());
                got.is_f64()
                    && spelled
                        .and_then(|s| s.parse::<f64>().ok())
                        .is_some_and(|f| f.to_bits() == n.to_bits())
            }
            Value::Bool(b) => got.as_bool() == Some(*b),
        };
        if !same {
            return Err(format!("{text} holds {name:?} as {got}, not {value:?}"));
        }
    }
    Ok(())
}

/// One reader, and what its queue must hold.
struct Reader {
    source: LogsSource,
    min_level: tracing::Level,
    /// The events its queue holds, oldest first, each with when it was said by.
    queue: VecDeque<(Event, SystemTime)>,
    /// When it opened, between these two: nothing mirrored that was said before is handed to it. No event here is said
    /// between them, so which side of its opening each falls on is known.
    opened: (SystemTime, SystemTime),
    next_offset: i64,
}

impl Reader {
    /// Holds `event` where its queue has room.
    fn offer(&mut self, event: &Event, by: SystemTime, cov: &mut Coverage) {
        match self.queue.len() < LOGS_CAPACITY {
            true => self.queue.push_back((event.clone(), by)),
            false => cov.reach("a queue full, refusing an event"),
        }
    }
}

#[test]
fn every_reader_carries_each_event_at_its_level_once_in_order_within_its_batch() {
    let _taps = crate::logs::TAPS_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let ceiling = crate::logs::max_level();
    let mut cov = Coverage::new(
        (LEVELS.iter().map(|(_, l)| format!("a reader at {l}")))
            .chain(VALUES.iter().map(|v| v.to_string()))
            .chain(
                [
                    "an event below a reader's level",
                    "a mirrored event said before a reader opened",
                    "a mirrored event said between two readers opening",
                    "an event said here",
                    "an event said here below the ceiling",
                    "an event said while delivering logs",
                    "an event with fields of which none can be written",
                    "a queue full, refusing an event",
                    "a batch cut by its rows",
                    "a batch cut by its bytes",
                    "a lone record past its bytes",
                    "a poll of an empty queue",
                    "a poll of only events below its level",
                    "a poll of no rows",
                    "a reader closed while others read",
                ]
                .map(String::from),
            ),
    );
    fuzz::run(1..201, |seed| {
        let mut rng = Rng::seeded(seed);
        let mut readers: Vec<Reader> = Vec::new();
        let mut n = 0u64;
        for round in 0..1 + rng.below(12) {
            if readers.len() > 1 && rng.chance(6) {
                readers.remove(rng.below(readers.len()));
                cov.reach("a reader closed while others read");
            }
            if readers.is_empty() || (readers.len() < 3 && rng.chance(5)) {
                let (min_level, name) = *rng.pick(&LEVELS);
                cov.reach(format!("a reader at {name}"));
                let before = SystemTime::now();
                let source = LogsSource::new(LogsSourceConfig { min_level });
                let opened = (before, SystemTime::now());
                readers.push(Reader {
                    source,
                    min_level,
                    queue: VecDeque::new(),
                    opened,
                    next_offset: 0,
                });
            }
            // A burst now and then fills a queue.
            let count = match rng.chance(8) {
                true => LOGS_CAPACITY + rng.below(200),
                false => rng.below(40),
            };
            let mut events = Vec::new();
            for _ in 0..count {
                if rng.chance(8) {
                    // What is mirrored first was said first.
                    crate::logs::mirror(events.drain(..));
                    let delivering = rng.chance(4);
                    let (event, by) = traced(&mut rng, delivering);
                    cov.reach("an event said here");
                    if event.level > ceiling {
                        cov.reach("an event said here below the ceiling");
                    } else if delivering {
                        cov.reach("an event said while delivering logs");
                    } else {
                        // Every open reader, whenever it opened.
                        for reader in &mut readers {
                            reader.offer(&event, by, &mut cov);
                        }
                    }
                    continue;
                }
                let newest = readers.iter().map(|r| r.opened.0).max().unwrap();
                let at = match rng.below(6) {
                    0 => *rng.pick(&[UNIX_EPOCH, readers[0].opened.0 - Duration::from_secs(3600)]),
                    // Before the newest reader opened, and after the others.
                    1 if readers.len() > 1 => newest - Duration::from_nanos(1),
                    _ => SystemTime::now() + Duration::from_millis(rng.below(1000) as u64),
                };
                let event = mirrored(&mut rng, n, at);
                n += 1;
                let handed = readers.iter().filter(|r| r.opened.1 <= at).count();
                if handed < readers.len() {
                    cov.reach(match handed {
                        0 => "a mirrored event said before a reader opened",
                        _ => "a mirrored event said between two readers opening",
                    });
                }
                for reader in readers.iter_mut().filter(|r| r.opened.1 <= at) {
                    reader.offer(&event, at, &mut cov);
                }
                events.push(event);
            }
            crate::logs::mirror(events);

            for (r, reader) in readers.iter_mut().enumerate() {
                for _ in 0..rng.below(3) {
                    let most_rows = if rng.chance(2) { 8 } else { 2000 };
                    let max_rows = rng.below(most_rows);
                    let most_bytes = if rng.chance(2) { 400 } else { 1 << 18 };
                    let max_bytes = rng.below(most_bytes);
                    let what = format!(
                        "round {round}, reader {r} at {}, max_rows {max_rows}, max_bytes {max_bytes}",
                        reader.min_level
                    );
                    let batch = reader
                        .source
                        .poll_batch(max_rows, max_bytes, Duration::ZERO)
                        .map_err(|e| format!("{what}: {e}"))?;
                    // What the reader must carry: of the oldest it holds, up to its rows, the ones at its level, until
                    // the next would pass its bytes, which goes back with all after it.
                    if max_rows == 0 {
                        cov.reach("a poll of no rows");
                    } else if reader.queue.is_empty() {
                        cov.reach("a poll of an empty queue");
                    }
                    let take = max_rows.min(reader.queue.len());
                    let mut taken: VecDeque<_> = reader.queue.drain(..take).collect();
                    let rows_cut = !reader.queue.is_empty();
                    let (mut expected, mut size, mut bytes_cut) = (Vec::new(), 0, false);
                    while let Some((event, by)) = taken.pop_front() {
                        if event.level > reader.min_level {
                            cov.reach("an event below a reader's level");
                            continue;
                        }
                        let record = json(&event).len();
                        if !expected.is_empty() && size + record > max_bytes {
                            cov.reach("a batch cut by its bytes");
                            bytes_cut = true;
                            taken.push_front((event, by));
                            for held in taken.drain(..).rev() {
                                reader.queue.push_front(held);
                            }
                            break;
                        }
                        size += record;
                        expected.push((event, by));
                    }
                    if expected.is_empty() && take > 0 {
                        cov.reach("a poll of only events below its level");
                    }
                    if batch.len() != expected.len() {
                        return Err(format!(
                            "{what}: carried {} records, not {}",
                            batch.len(),
                            expected.len()
                        ));
                    }
                    // The batch's own size, not the model's: a record is its own bytes.
                    let carried: usize = batch.iter().map(|r| r.bytes().unwrap().len()).sum();
                    if batch.len() > 1 && carried > max_bytes {
                        return Err(format!("{what}: a batch of {carried} bytes, past its limit"));
                    }
                    if batch.len() == 1 && carried > max_bytes {
                        cov.reach("a lone record past its bytes");
                    }
                    if rows_cut && !bytes_cut {
                        cov.reach("a batch cut by its rows");
                    }
                    for (record, (event, by)) in batch.iter().zip(&expected) {
                        check_json(record.bytes().unwrap(), event, *by).map_err(|why| format!("{what}: {why}"))?;
                        for (_, value) in &event.fields {
                            cov.reach(tries(value));
                        }
                        if !event.fields.is_empty() && event.fields.iter().all(|(_, v)| unwritable(v)) {
                            cov.reach("an event with fields of which none can be written");
                        }
                        if record.offset != reader.next_offset || &*record.topic != LOGS_TOPIC || record.partition != 0
                        {
                            return Err(format!(
                                "{what}: a record at {}/{}/{}, not at offset {}",
                                record.topic, record.partition, record.offset, reader.next_offset
                            ));
                        }
                        reader.next_offset += 1;
                    }
                }
            }
        }
        drop(readers);
        if crate::logs::taps_open() != 0 {
            return Err("a closed reader left its queue open".into());
        }
        Ok(())
    });
    cov.assert_complete();
}
