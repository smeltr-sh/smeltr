//! What a running deployment sees of its data, kept for the next report and then forgotten (design doc,
//! Observability: Streams).
//!
//! The transform threads write here and the report loop drains it. A Parser's fields are tallied from one batch per
//! route per interval, so the cost follows the configuration and the clock rather than the traffic; the rows no route
//! took are keyed on every batch, since a new stream is what those rows are. Everything held is bounded by the caps
//! the wire sets (`heartbeat::STREAM_FIELDS_PER_PARSER` and its neighbours), and what does not fit is counted.
//!
//! Senders are counted on every batch, since a sender that goes quiet is what they are for: each value of each
//! string field the pipeline delivers, and of the source's stream key, by the rows that held it ([`SenderTally`]).
//! A field past [`SENDER_VALUES`] values names no sender, and is counted no more.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use crate::core::column::{Column, DataType, Scalar};
use crate::core::id::SlotId;
use crate::core::tally::{FieldSeen, FieldTally};
use crate::heartbeat::{
    FieldShape, ParserShape, SENDER_FIELDS, SENDER_VALUES, STREAM_FIELDS_PER_PARSER, STREAM_KEYS_PER_SOURCE,
    STREAM_NAME_LEN, SenderCounts, SenderFields, SenderRows, StreamKey, StreamReport, UnmatchedStreams,
};
use crate::plan::compile::Plan;
use crate::plan::tree::{self, Node};

/// One deployment's account of its streams.
pub struct Streams {
    /// When each route may next tally a batch, in milliseconds since `started`. One atomic per route, so the hot path
    /// decides with a load and no lock; the thread that wins the swap tallies.
    due: Vec<AtomicU64>,
    /// Each pipeline's id as its author wrote it, by position: what a shape names, since the position a plan carries
    /// does not survive a redeploy.
    pipelines: Vec<String>,
    interval_ms: u64,
    started: Instant,
    window: Mutex<StreamReport>,
    edits: SenderEdits,
    senders: Mutex<HashMap<(String, String), Seen>>,
}

/// One sender field of one source counted so far, by (source, field): the values it held, or `None` once it held
/// more than [`SENDER_VALUES`].
type Seen = Option<HashSet<String>>;

/// What an editor changed of each source's sender fields, as the leader last sent it: one per runtime, read by every
/// deployment it starts.
#[derive(Clone, Default)]
pub struct SenderEdits(Arc<RwLock<Vec<SenderFields>>>);

impl SenderEdits {
    pub fn set(&self, fields: Vec<SenderFields>) {
        *self.0.write().unwrap_or_else(|e| e.into_inner()) = fields;
    }

    fn of(&self, source: &str) -> SenderFields {
        let edits = self.0.read().unwrap_or_else(|e| e.into_inner());
        edits.iter().find(|f| f.source == source).cloned().unwrap_or_default()
    }
}

/// One sender field a batch counts: a string field of the record the pipeline delivers, read from its column, or a
/// field an editor added, read by its name as text.
pub struct Counted {
    pub name: String,
    pub slot: Option<SlotId>,
}

/// The rows each value of each sender field held in one batch, each record counted once for each field.
#[derive(Default)]
pub struct SenderTally {
    fields: Vec<Counting>,
}

/// One field of a [`SenderTally`].
struct Counting {
    name: String,
    /// Which records have counted, by their place in the batch.
    counted: Vec<u64>,
    values: HashMap<Vec<u8>, u64>,
    /// It held more values than a sender field does.
    over: bool,
}

impl SenderTally {
    /// The field `name`'s count.
    fn field(&mut self, name: &str) -> &mut Counting {
        let at = match self.fields.iter().position(|f| f.name == name) {
            Some(at) => at,
            None => {
                self.fields.push(Counting {
                    name: name.to_string(),
                    counted: Vec::new(),
                    values: HashMap::new(),
                    over: false,
                });
                self.fields.len() - 1
            }
        };
        &mut self.fields[at]
    }

    /// Count `value` for `record` under `field`, unless the record has counted for it.
    #[cfg(test)]
    fn count(&mut self, field: &str, record: usize, value: &[u8]) {
        self.field(field).count(record, value);
    }

    /// Count each record's value under `field`, where it has one. The field is counted though none has: the leader
    /// judges a sender only where its field was counted (`leader::senders::FIELD_ROWS`).
    pub fn count_rows<V: AsRef<str>>(&mut self, field: &str, values: impl Iterator<Item = (usize, Option<V>)>) {
        let counting = self.field(field);
        for (record, value) in values {
            if let Some(value) = value {
                counting.count(record, value.as_ref().as_bytes());
            }
        }
    }

    /// Count `column`'s value on each of `rows`, for the record `record` gives each, under `field`, as
    /// [`Self::count_rows`] does. A null counts nothing: the record does not hold the field.
    pub fn count_column(&mut self, field: &str, column: &Column, rows: &[usize], record: impl Fn(usize) -> usize) {
        let counting = self.field(field);
        if let Some(Scalar::Str(value)) = column.constant_value() {
            for &row in rows {
                counting.count(record(row), value.as_bytes());
            }
            return;
        }
        let Ok(values) = column.var() else { return };
        for &row in rows {
            if values.validity.is_valid(row) {
                counting.count(record(row), values.get(row));
            }
        }
    }
}

impl Counting {
    fn count(&mut self, record: usize, value: &[u8]) {
        let (word, bit) = (record / 64, 1u64 << (record % 64));
        if self.over || self.counted.get(word).is_some_and(|w| w & bit != 0) {
            return;
        }
        if self.counted.len() <= word {
            self.counted.resize(word + 1, 0);
        }
        self.counted[word] |= bit;
        if let Some(rows) = self.values.get_mut(value) {
            *rows += 1;
        } else if value.len() > STREAM_NAME_LEN {
            // Too long to name on the wire, and one such value says nothing of the field's others.
        } else if self.values.len() < SENDER_VALUES {
            self.values.insert(value.to_vec(), 1);
        } else {
            self.over = true;
            self.values.clear();
        }
    }
}

impl Streams {
    /// An account over `routes` routes, each tallying one batch every `interval`, of a deployment whose pipelines are
    /// `pipelines`, by position.
    pub fn new(routes: usize, pipelines: Vec<String>, interval: Duration, edits: SenderEdits) -> Streams {
        Streams {
            due: (0..routes).map(|_| AtomicU64::new(0)).collect(),
            pipelines,
            interval_ms: interval.as_millis().max(1) as u64,
            started: Instant::now(),
            window: Mutex::new(StreamReport::default()),
            edits,
            senders: Mutex::new(HashMap::new()),
        }
    }

    /// Whether `field` of `source` is counted: an editor did not uncheck it, and it has not held too many values.
    fn counts(&self, source: &str, field: &str, edits: &SenderFields) -> bool {
        let senders = self.senders.lock().unwrap_or_else(|e| e.into_inner());
        !edits.unchecked.iter().any(|f| f == field)
            && !matches!(senders.get(&(source.to_string(), field.to_string())), Some(None))
    }

    /// The sender fields of `source`'s records that `plan` delivers: each string field of the record's own level, and
    /// each field an editor added, less those an editor unchecked and those past [`SENDER_VALUES`] values; the first
    /// [`SENDER_FIELDS`] of them.
    pub fn sender_fields(&self, source: &str, plan: &Plan) -> Vec<Counted> {
        let edits = self.edits.of(source);
        let mut fields: Vec<Counted> = plan
            .tree
            .fields
            .iter()
            .filter_map(|(key, node)| match node {
                Node::Leaf(slot) if !tree::is_internal(key) && plan.slot_layout.get(*slot) == Some(&DataType::Str) => {
                    Some(Counted {
                        name: tree::spell(key).into_owned(),
                        slot: Some(*slot),
                    })
                }
                _ => None,
            })
            .collect();
        for name in &edits.added {
            if !fields.iter().any(|f| f.name == *name) {
                fields.push(Counted {
                    name: name.clone(),
                    slot: None,
                });
            }
        }
        fields.retain(|f| self.counts(source, &f.name, &edits));
        fields.truncate(SENDER_FIELDS);
        fields
    }

    /// Whether `source`'s stream key, filed under `field`, is counted.
    pub fn counts_stream(&self, source: &str, field: &str) -> bool {
        self.counts(source, field, &self.edits.of(source))
    }

    /// What one batch of `source`'s records counted of its senders. A field the batch found past [`SENDER_VALUES`]
    /// values, or that reaches that many with this batch, is reported as having too many values, once, and counted no
    /// more.
    pub fn saw_senders(&self, source: &str, tally: SenderTally) {
        if tally.fields.is_empty() {
            return;
        }
        let mut counts = Vec::new();
        {
            let mut senders = self.senders.lock().unwrap_or_else(|e| e.into_inner());
            for field in tally.fields {
                let seen = senders
                    .entry((source.to_string(), field.name.clone()))
                    .or_insert_with(|| Some(HashSet::new()));
                let Some(held) = seen else { continue };
                let values: Vec<SenderRows> = field
                    .values
                    .into_iter()
                    .map(|(value, rows)| SenderRows {
                        value: String::from_utf8_lossy(&value).into_owned(),
                        rows,
                    })
                    .collect();
                for v in &values {
                    if !held.contains(&v.value) {
                        held.insert(v.value.clone());
                    }
                }
                let too_many = field.over || held.len() > SENDER_VALUES;
                if too_many {
                    *seen = None;
                }
                counts.push(SenderCounts {
                    source: source.to_string(),
                    field: field.name,
                    values: if too_many { Vec::new() } else { values },
                    too_many,
                });
            }
        }
        let mut window = self.window.lock().unwrap_or_else(|e| e.into_inner());
        window.absorb(StreamReport {
            senders: counts,
            ..Default::default()
        });
    }

    /// Whether the batch about to run `route` is the one this interval tallies. Answers yes once per interval per
    /// route, to one thread.
    pub fn due(&self, route: usize) -> bool {
        let Some(due) = self.due.get(route) else { return false };
        let now = self.started.elapsed().as_millis() as u64;
        let at = due.load(Ordering::Relaxed);
        now >= at
            && due
                .compare_exchange(at, now + self.interval_ms, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
    }

    /// Gives back the tally `due` granted for `route`, for a batch that never ran: the next batch on it tallies.
    pub fn not_run(&self, route: usize) {
        if let Some(due) = self.due.get(route) {
            due.store(0, Ordering::Relaxed);
        }
    }

    /// What each Parser of `plan` produced from a tallied batch of `source`'s rows on `route`. A parse whose function
    /// `plan` has no step for -- one reached through a `Call` -- is left out: the shape names steps the pipeline's
    /// author has.
    pub fn saw_parse(&self, source: &str, route: &str, plan: &Plan, tally: &FieldTally) {
        let mut shapes = Vec::new();
        for parse in tally.parses() {
            let Some(step) = plan.step_ids.get(parse.function as usize) else {
                continue;
            };
            let mut shape = ParserShape {
                source: source.to_string(),
                route: route.to_string(),
                pipeline: self
                    .pipelines
                    .get(plan.id)
                    .cloned()
                    .unwrap_or_else(|| plan.id_label.clone()),
                step: step.clone(),
                rows: parse.rows as u64,
                fields: Vec::new(),
                fields_dropped: 0,
            };
            for seen in &parse.fields {
                if seen.name.len() > STREAM_NAME_LEN || shape.fields.len() >= STREAM_FIELDS_PER_PARSER {
                    shape.fields_dropped += seen.counts.present as u64;
                    continue;
                }
                shape.fields.push(field_shape(seen));
            }
            shapes.push(shape);
        }
        if shapes.is_empty() {
            return;
        }
        let mut window = self.window.lock().unwrap_or_else(|e| e.into_inner());
        window.absorb(StreamReport {
            parsers: shapes,
            ..Default::default()
        });
    }

    /// The rows of `source` no route took, each by its stream key, or `None` where its kind files records under
    /// nothing.
    pub fn saw_unmatched<'k>(&self, source: &str, keys: impl IntoIterator<Item = Option<std::borrow::Cow<'k, str>>>) {
        let mut streams = UnmatchedStreams {
            source: source.to_string(),
            rows: 0,
            keys: Vec::new(),
            keys_dropped: 0,
        };
        for key in keys {
            streams.rows += 1;
            let key = key.unwrap_or_default();
            if key.len() > STREAM_NAME_LEN {
                streams.keys_dropped += 1;
                continue;
            }
            match streams.keys.iter().position(|k| k.key == key) {
                Some(at) => streams.keys[at].rows += 1,
                None if streams.keys.len() < STREAM_KEYS_PER_SOURCE => streams.keys.push(StreamKey {
                    key: key.into_owned(),
                    rows: 1,
                }),
                None => streams.keys_dropped += 1,
            }
        }
        if streams.rows == 0 {
            return;
        }
        let mut window = self.window.lock().unwrap_or_else(|e| e.into_inner());
        window.absorb(StreamReport {
            unmatched: vec![streams],
            ..Default::default()
        });
    }

    /// Everything seen since the last drain, for the report that carries it. Every sender field counted no more for
    /// holding too many values is in it too; the leader forgets one it has not heard of for a week. `None` when nothing
    /// was seen.
    pub fn drain(&self) -> Option<StreamReport> {
        let too_many: Vec<SenderCounts> = {
            let senders = self.senders.lock().unwrap_or_else(|e| e.into_inner());
            senders
                .iter()
                .filter(|(_, seen)| seen.is_none())
                .map(|((source, field), _)| SenderCounts {
                    source: source.clone(),
                    field: field.clone(),
                    values: Vec::new(),
                    too_many: true,
                })
                .collect()
        };
        let mut window = self.window.lock().unwrap_or_else(|e| e.into_inner());
        window.absorb(StreamReport {
            senders: too_many,
            ..Default::default()
        });
        if window.is_empty() {
            return None;
        }
        Some(std::mem::take(&mut *window))
    }

    /// `report`, drained earlier and not delivered, put back ahead of what was seen since, so the next drain carries
    /// both.
    pub fn give_back(&self, report: StreamReport) {
        let mut window = self.window.lock().unwrap_or_else(|e| e.into_inner());
        let mut earlier = report;
        earlier.absorb(std::mem::take(&mut *window));
        *window = earlier;
    }
}

fn field_shape(seen: &FieldSeen) -> FieldShape {
    let c = &seen.counts;
    FieldShape {
        name: seen.name.clone(),
        declared: seen.declared,
        suggested: seen.suggested.to_string(),
        present: c.present as u64,
        bools: c.bools as u64,
        ints: c.ints as u64,
        floats: c.floats as u64,
        strings: c.strings as u64,
        arrays: c.arrays as u64,
        objects: c.objects as u64,
        failed: seen.failed as u64,
        times: c.times as u64,
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use super::*;

    #[test]
    fn a_route_is_due_once_per_interval_and_to_one_thread() {
        let streams = Streams::new(2, Vec::new(), Duration::from_secs(3600), SenderEdits::default());
        assert!(streams.due(0), "the first batch of the interval tallies");
        assert!(!streams.due(0), "the second does not");
        assert!(streams.due(1), "each route has its own interval");
        assert!(!streams.due(2), "a route the account was not built for never tallies");
    }

    #[test]
    fn a_tally_given_back_goes_to_the_next_batch() {
        let streams = Streams::new(1, Vec::new(), Duration::from_secs(3600), SenderEdits::default());
        assert!(streams.due(0));
        streams.not_run(0);
        assert!(streams.due(0), "the batch that never ran left the interval's tally");
        assert!(!streams.due(0));
    }

    #[test]
    fn a_zero_interval_tallies_every_batch() {
        let streams = Streams::new(1, Vec::new(), Duration::ZERO, SenderEdits::default());
        assert!(streams.due(0));
        std::thread::sleep(Duration::from_millis(2));
        assert!(streams.due(0));
    }

    #[test]
    fn unmatched_rows_are_keyed_and_capped() {
        let streams = Streams::new(0, Vec::new(), Duration::from_secs(1), SenderEdits::default());
        // Two records filed under nothing, then a cap's worth of senders less the slot those took, five more, and one
        // key too long for the wire.
        let senders = (0..(STREAM_KEYS_PER_SOURCE + 4)).map(|i| Some(Cow::Owned(format!("sender-{i}"))));
        let keys = [None, None]
            .into_iter()
            .chain(senders)
            .chain([Some(Cow::Owned("x".repeat(STREAM_NAME_LEN + 1)))]);
        streams.saw_unmatched("hec", keys);

        let report = streams.drain().expect("something was seen");
        assert!(report.parsers.is_empty());
        let [u] = report.unmatched.as_slice() else {
            panic!("one source")
        };
        assert_eq!(u.source, "hec");
        assert_eq!(u.rows as usize, 2 + STREAM_KEYS_PER_SOURCE + 4 + 1);
        assert_eq!(u.keys.len(), STREAM_KEYS_PER_SOURCE);
        assert_eq!(u.keys_dropped, 5 + 1, "past the cap, or too long");
        assert!(
            u.keys.iter().any(|k| k.key.is_empty() && k.rows == 2),
            "a record filed under nothing"
        );
        assert!(streams.drain().is_none(), "drained");
    }

    /// A record counts once for each field, and a value too long to name counts nothing. A field past
    /// [`SENDER_VALUES`] values, over any number of batches, is said to be too many in that window and every one
    /// after, and is counted no more.
    #[test]
    fn a_record_counts_once_and_a_field_past_its_values_stops() {
        let streams = Streams::new(0, Vec::new(), Duration::from_secs(1), SenderEdits::default());
        let mut tally = SenderTally::default();
        for (record, value) in [(0, "fw-01"), (0, "fw-01"), (1, "fw-07"), (2, "fw-01")] {
            tally.count("host", record, value.as_bytes());
        }
        tally.count("host", 3, &[b'x'; STREAM_NAME_LEN + 1]);
        streams.saw_senders("syslog", tally);
        let report = streams.drain().expect("senders were counted");
        let [host] = report.senders.as_slice() else {
            panic!("one field: {report:?}")
        };
        let mut rows: Vec<(&str, u64)> = host.values.iter().map(|v| (v.value.as_str(), v.rows)).collect();
        rows.sort();
        assert_eq!(
            rows,
            [("fw-01", 2), ("fw-07", 1)],
            "the record's second row counts nothing, nor does a value too long to name"
        );
        assert!(!host.too_many, "{host:?}");

        for batch in 0..2 {
            let mut tally = SenderTally::default();
            for record in 0..SENDER_VALUES / 2 + 1 {
                tally.count("host", record, format!("{batch}-{record}").as_bytes());
            }
            streams.saw_senders("syslog", tally);
        }
        assert!(!streams.counts_stream("syslog", "host"), "counted no more");
        for _ in 0..2 {
            let report = streams.drain().expect("the field is said to be too many");
            let [host] = report.senders.as_slice() else {
                panic!("one field: {report:?}")
            };
            assert!(host.too_many && host.values.is_empty(), "{host:?}");
        }
    }

    #[test]
    fn a_window_given_back_comes_out_in_the_next_drain() {
        let streams = Streams::new(0, Vec::new(), Duration::from_secs(1), SenderEdits::default());
        streams.saw_unmatched("hec", [Some(Cow::Borrowed("a"))]);
        let undelivered = streams.drain().expect("something was seen");
        streams.saw_unmatched("hec", [Some(Cow::Borrowed("a")), Some(Cow::Borrowed("b"))]);
        streams.give_back(undelivered);

        let report = streams.drain().expect("both windows");
        let [u] = report.unmatched.as_slice() else {
            panic!("one source")
        };
        assert_eq!(u.rows, 3, "the given-back window and the one after it");
        assert!(streams.drain().is_none(), "drained");
    }

    #[test]
    fn two_windows_fold_into_one() {
        let mut first = StreamReport {
            senders: Vec::new(),
            parsers: vec![ParserShape {
                source: "s".into(),
                route: "r".into(),
                pipeline: "p".into(),
                step: "parse".into(),
                rows: 10,
                fields: vec![FieldShape {
                    name: "a".into(),
                    declared: false,
                    suggested: "str".into(),
                    present: 10,
                    strings: 10,
                    ..Default::default()
                }],
                fields_dropped: 0,
            }],
            unmatched: vec![UnmatchedStreams {
                source: "s".into(),
                rows: 3,
                keys: vec![StreamKey {
                    key: "k".into(),
                    rows: 3,
                }],
                keys_dropped: 0,
            }],
        };
        first.absorb(StreamReport {
            senders: Vec::new(),
            parsers: vec![ParserShape {
                source: "s".into(),
                route: "r".into(),
                pipeline: "p".into(),
                step: "parse".into(),
                rows: 5,
                fields: vec![
                    FieldShape {
                        name: "a".into(),
                        declared: true,
                        suggested: "i64".into(),
                        present: 5,
                        ints: 5,
                        ..Default::default()
                    },
                    FieldShape {
                        name: "b".into(),
                        present: 1,
                        ..Default::default()
                    },
                ],
                fields_dropped: 2,
            }],
            unmatched: vec![UnmatchedStreams {
                source: "s".into(),
                rows: 2,
                keys: vec![
                    StreamKey {
                        key: "k".into(),
                        rows: 1,
                    },
                    StreamKey {
                        key: "k2".into(),
                        rows: 1,
                    },
                ],
                keys_dropped: 0,
            }],
        });
        let shape = &first.parsers[0];
        assert_eq!((shape.rows, shape.fields_dropped), (15, 2));
        assert_eq!(shape.fields.len(), 2);
        let a = &shape.fields[0];
        assert_eq!((a.present, a.strings, a.ints), (15, 10, 5), "counts sum");
        assert!(a.declared && a.suggested == "i64", "the later reading wins");
        let u = &first.unmatched[0];
        assert_eq!(u.rows, 5);
        assert_eq!(u.keys.iter().map(|k| k.rows).sum::<u64>(), 5);
    }
}
