//! What a running deployment sees of its data, kept for the next report and then forgotten (design doc,
//! Observability: Streams).
//!
//! The transform threads write here and the report loop drains it. A Parser's fields are tallied from one batch per
//! route per interval, so the cost follows the configuration and the clock rather than the traffic; the rows no route
//! took are keyed on every batch, since a new stream is what those rows are. Everything held is bounded by the caps
//! the wire sets (`heartbeat::STREAM_FIELDS_PER_PARSER` and its neighbours), and what does not fit is counted.

use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::core::tally::{FieldSeen, FieldTally};
use crate::heartbeat::{
    FieldShape, ParserShape, STREAM_FIELDS_PER_PARSER, STREAM_KEYS_PER_SOURCE, STREAM_NAME_LEN, StreamKey,
    StreamReport, UnmatchedStreams,
};
use crate::plan::compile::Plan;

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
}

impl Streams {
    /// An account over `routes` routes, each tallying one batch every `interval`, of a deployment whose pipelines are
    /// `pipelines`, by position.
    pub fn new(routes: usize, pipelines: Vec<String>, interval: Duration) -> Streams {
        Streams {
            due: (0..routes).map(|_| AtomicU64::new(0)).collect(),
            pipelines,
            interval_ms: interval.as_millis().max(1) as u64,
            started: Instant::now(),
            window: Mutex::new(StreamReport::default()),
        }
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
            unmatched: Vec::new(),
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
            parsers: Vec::new(),
            unmatched: vec![streams],
        });
    }

    /// Everything seen since the last drain, for the report that carries it. `None` when nothing was.
    pub fn drain(&self) -> Option<StreamReport> {
        let mut window = self.window.lock().unwrap_or_else(|e| e.into_inner());
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
        let streams = Streams::new(2, Vec::new(), Duration::from_secs(3600));
        assert!(streams.due(0), "the first batch of the interval tallies");
        assert!(!streams.due(0), "the second does not");
        assert!(streams.due(1), "each route has its own interval");
        assert!(!streams.due(2), "a route the account was not built for never tallies");
    }

    #[test]
    fn a_tally_given_back_goes_to_the_next_batch() {
        let streams = Streams::new(1, Vec::new(), Duration::from_secs(3600));
        assert!(streams.due(0));
        streams.not_run(0);
        assert!(streams.due(0), "the batch that never ran left the interval's tally");
        assert!(!streams.due(0));
    }

    #[test]
    fn a_zero_interval_tallies_every_batch() {
        let streams = Streams::new(1, Vec::new(), Duration::ZERO);
        assert!(streams.due(0));
        std::thread::sleep(Duration::from_millis(2));
        assert!(streams.due(0));
    }

    #[test]
    fn unmatched_rows_are_keyed_and_capped() {
        let streams = Streams::new(0, Vec::new(), Duration::from_secs(1));
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

    #[test]
    fn a_window_given_back_comes_out_in_the_next_drain() {
        let streams = Streams::new(0, Vec::new(), Duration::from_secs(1));
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
