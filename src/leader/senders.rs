//! Who sends a source its rows, and whether each still sends what it normally does.
//!
//! A sender is one value of one field of a source's records: a firewall's name, a host, a sourcetype. Workers count
//! each value of every string field the pipeline delivers, and of the source's stream key, by the records that held
//! it (`streams::SenderTally`), and the leader keeps each value's rows in [`TIER`]'s buckets. A field that holds more
//! than `heartbeat::SENDER_VALUES` values on a worker is no sender field, and is counted no more.
//!
//! Each day, the pick reads which values are senders: a value is watched once it went no longer than
//! [`LONGEST_EMPTY`] buckets without a row on each of [`PICK_DAYS`] days in a row, and stays watched until a further
//! [`PICK_DAYS`] pass without such a run. A bucket counts only where its field was counted ([`FIELD_ROWS`]), so a
//! field a pipeline change takes out leaves its senders unjudged rather than all below. A field with one value names
//! no sender. An editor may stop watching a field, or add one the pick cannot offer: a number or a nested
//! path (`marks::SourceFields`).
//!
//! A watched sender is judged as a source is (`throughput`), from its own rows, except by the gap, and not while its
//! source warns or is stopped.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use axum::extract::{Path, State};
use serde::{Deserialize, Serialize};

use super::AppState;
use super::envelope::Envelope;
use super::marks::Marks;
use super::series::{RegistryStore, Tier};
use super::store::StoreError;
use super::throughput::{self, ExpectedPoint, Learned, Outside, Reading};
use super::warnings::{self, label};

/// The tier a sender's rows are kept at, and judged at: written there directly, since a source may have hundreds of
/// values nobody watches, too many series to keep finer.
pub const TIER: Tier = Tier::FiveMinutes;

/// The rows each value of a sender field held.
pub const ROWS: &str = "smeltr_sender_rows_total";

/// A sender field a worker counts no more, for holding too many values: one point for each window it says so.
pub const OVER: &str = "smeltr_sender_fields_over_total";

/// The rows that held any value of a sender field, in each window a worker counted the field, though none did. A
/// bucket with no point is one no worker's pipeline delivered the field in.
pub const FIELD_ROWS: &str = "smeltr_sender_field_rows_total";

/// The most [`TIER`] buckets in a row a watched sender may go without a row on each of the days it is picked on.
const LONGEST_EMPTY: usize = 12;

/// How many days in a row a value must pass to be watched, and how long it stays watched after.
const PICK_DAYS: i64 = 7;

const DAY_MS: i64 = 24 * 60 * 60 * 1000;

/// One value of one field.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Sender {
    pub field: String,
    pub value: String,
}

/// A sender, by its group and source.
pub type SenderKey = (String, String, Sender);

/// The series subject of `group`'s `source`'s `field`, or of one of its values: its labels by name, as every series
/// subject is (`registry::subject_key`).
pub fn subject(group: &str, source: &str, field: &str, value: Option<&str>) -> String {
    let mut labels = vec![("field", field), ("group", group), ("source", source)];
    labels.extend(value.map(|v| ("value", v)));
    serde_json::to_string(&labels).unwrap_or_default()
}

/// What a subject names: its group, source and field, and its value, if it names one.
fn named(subject: &str) -> Option<(String, String, String, Option<String>)> {
    let labels: Vec<(String, String)> = serde_json::from_str(subject).ok()?;
    let got = |key| label(&labels, key).map(str::to_string);
    Some((got("group")?, got("source")?, got("field")?, got("value")))
}

/// What one field of one source is, as of the day the pick read.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase", tag = "state")]
pub enum FieldPick {
    /// Its senders, which may be none.
    Watched { values: Vec<String> },
    /// Counted for less than [`PICK_DAYS`] full days, so none of its values can be watched until then.
    Learning { until_millis: i64 },
    /// It held one value: it names no sender.
    OneValue,
    /// It held more values than a sender field does.
    TooMany,
}

/// Every source's sender fields as of the start of one day, by (group, source) and field.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Pick {
    pub day_ms: i64,
    pub fields: BTreeMap<(String, String), BTreeMap<String, FieldPick>>,
}

impl Pick {
    /// Every sender watched, less the fields `unchecked` names for its source.
    pub fn watched(&self, unchecked: impl Fn(&str, &str, &str) -> bool) -> Vec<SenderKey> {
        let mut out = Vec::new();
        for ((group, source), fields) in &self.fields {
            for (field, pick) in fields {
                let FieldPick::Watched { values } = pick else { continue };
                if unchecked(group, source, field) {
                    continue;
                }
                out.extend(values.iter().map(|value| {
                    let sender = Sender {
                        field: field.clone(),
                        value: value.clone(),
                    };
                    (group.clone(), source.clone(), sender)
                }));
            }
        }
        out
    }
}

/// The day `ms` falls in, as the epoch milliseconds it began at.
pub fn day_of(ms: i64) -> i64 {
    ms.div_euclid(DAY_MS) * DAY_MS
}

/// Rows by the start of the bucket they were read in.
type Rows = BTreeMap<i64, f64>;

/// A field, by its group, source and name.
type FieldKey = (String, String, String);

/// Every source's sender fields as of `day_ms`, from the 2 x [`PICK_DAYS`] days before it.
pub fn pick(store: &RegistryStore, day_ms: i64) -> Result<Pick, StoreError> {
    let since = day_ms - 2 * PICK_DAYS * DAY_MS;
    let counted: HashMap<FieldKey, Rows> = store
        .series_since(FIELD_ROWS, TIER, since)?
        .into_iter()
        .filter_map(|(subject, points)| {
            let (group, source, field, _) = named(&subject)?;
            Some(((group, source, field), points.into_iter().collect()))
        })
        .collect();
    let over: BTreeSet<FieldKey> = store
        .series_since(OVER, TIER, day_ms - PICK_DAYS * DAY_MS)?
        .into_iter()
        .filter_map(|(subject, _)| named(&subject))
        .map(|(group, source, field, _)| (group, source, field))
        .collect();
    // Each field's values, each with its rows by bucket.
    let mut values: BTreeMap<FieldKey, Vec<(String, Rows)>> = BTreeMap::new();
    for (subject, points) in store.series_since(ROWS, TIER, since)? {
        let Some((group, source, field, Some(value))) = named(&subject) else {
            continue;
        };
        let rows: BTreeMap<i64, f64> = points.into_iter().filter(|&(at, _)| at < day_ms).collect();
        values.entry((group, source, field)).or_default().push((value, rows));
    }
    for field in &over {
        values.entry(field.clone()).or_default();
    }
    let mut out = Pick {
        day_ms,
        fields: BTreeMap::new(),
    };
    for (key, of) in values {
        let counted = counted.get(&key);
        let (group, source, field) = key;
        let pick = if over.contains(&(group.clone(), source.clone(), field.clone())) {
            FieldPick::TooMany
        } else if of.iter().filter(|(_, rows)| rows.values().any(|&n| n > 0.0)).count() <= 1 {
            FieldPick::OneValue
        } else {
            let first = of.iter().filter_map(|(_, rows)| rows.keys().next()).min().copied();
            let watched: Vec<String> = of
                .iter()
                .filter(|(_, rows)| counted.is_some_and(|counted| steady(rows, counted, day_ms)))
                .map(|(value, _)| value.clone())
                .collect();
            let until = first.map(|first| day_of(first) + (PICK_DAYS + 1) * DAY_MS);
            match until {
                Some(until) if watched.is_empty() && until > day_ms => FieldPick::Learning { until_millis: until },
                _ => FieldPick::Watched { values: watched },
            }
        };
        out.fields.entry((group, source)).or_default().insert(field, pick);
    }
    Ok(out)
}

/// Whether a value with `rows` by bucket is steady: it passed on [`PICK_DAYS`] days in a row, the last of them within
/// the [`PICK_DAYS`] days before `day_ms`. A day passes when the value went no more than [`LONGEST_EMPTY`] buckets
/// without a row, counting only the buckets where its field was `counted`; a day its field was not counted at all
/// does not pass.
fn steady(rows: &BTreeMap<i64, f64>, counted: &BTreeMap<i64, f64>, day_ms: i64) -> bool {
    let step = TIER.bucket_ms();
    let passed = |day: i64| {
        let start = day_ms - (day + 1) * DAY_MS;
        let (mut run, mut longest, mut seen) = (0, 0, false);
        for at in (start..start + DAY_MS).step_by(step as usize) {
            // A bucket the field was not counted in neither ends a run nor adds to one.
            if !counted.contains_key(&at) {
                continue;
            }
            seen = true;
            match rows.get(&at) {
                Some(&n) if n > 0.0 => run = 0,
                _ => {
                    run += 1;
                    longest = usize::max(longest, run);
                }
            }
        }
        seen && longest <= LONGEST_EMPTY
    };
    let days: Vec<bool> = (0..2 * PICK_DAYS).map(passed).collect();
    (0..PICK_DAYS as usize).any(|end| days[end..end + PICK_DAYS as usize].iter().all(|&p| p))
}

/// Each of `watched`'s rows by bucket from `since_ms`, with a bucket its field was counted in and it has no point for
/// read as no rows. A bucket its field was not counted in has no point.
pub fn history(
    store: &RegistryStore,
    watched: &[SenderKey],
    since_ms: i64,
) -> Result<HashMap<SenderKey, BTreeMap<i64, f64>>, StoreError> {
    let read = |metric, subjects: &[String]| -> Result<HashMap<String, Rows>, StoreError> {
        Ok(store
            .subjects_since(metric, subjects, TIER, since_ms)?
            .into_iter()
            .map(|(subject, points)| (subject, points.into_iter().collect()))
            .collect())
    };
    let subjects: Vec<String> = watched
        .iter()
        .map(|(group, source, s)| subject(group, source, &s.field, Some(&s.value)))
        .collect();
    let fields: BTreeSet<String> = watched
        .iter()
        .map(|(group, source, s)| subject(group, source, &s.field, None))
        .collect();
    let mut held = read(ROWS, &subjects)?;
    let counted = read(FIELD_ROWS, &fields.into_iter().collect::<Vec<_>>())?;
    Ok(watched
        .iter()
        .zip(subjects)
        .filter_map(|(key, own)| {
            let counted = counted.get(&subject(&key.0, &key.1, &key.2.field, None))?;
            let own = held.remove(&own).unwrap_or_default();
            let rows = counted
                .range(since_ms..)
                .map(|(&at, _)| (at, own.get(&at).copied().unwrap_or(0.0)))
                .collect();
            Some((key.clone(), rows))
        })
        .collect())
}

/// One source's sender fields, as the Senders tab shows them.
#[derive(Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SourceSenders {
    /// Those with a sender below normal first, then by what the pick read of each.
    pub fields: Vec<FieldNow>,
}

/// One field of a source, and its senders.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FieldNow {
    pub field: String,
    /// An editor did not stop watching it.
    pub checked: bool,
    /// An editor added it.
    pub added: bool,
    /// What the pick read of it today, or `None` before it has read any of it.
    pub pick: Option<FieldPick>,
    /// Its senders watched, those below normal first.
    pub senders: Vec<SenderNow>,
}

/// One sender, and what it reads.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SenderNow {
    pub value: String,
    /// Rows per second in each bucket of the last day, by when it began. A bucket its field was not counted in has
    /// none.
    pub points: Vec<(i64, f64)>,
    pub expected: Vec<ExpectedPoint>,
    /// Rows per second now, and its range, at the width it is shown at (`throughput::Reading::shown`). `None` while
    /// no width judges it.
    pub now: Option<f64>,
    pub low: Option<f64>,
    pub high: Option<f64>,
    pub below: bool,
    /// Who lowered its warning that holds to info.
    pub lowered_by: Option<String>,
}

/// `GET /api/areas/:area/sources/:id/senders`: the source's sender fields and their senders.
pub async fn get(
    State(state): State<AppState>,
    Path((group, source)): Path<(String, String)>,
) -> Envelope<SourceSenders> {
    let now_ms = warnings::now_millis();
    let interval = crate::startup_config::leader_heartbeat_interval();
    let readings = throughput::readings(&state, now_ms, interval, false).await;
    super::blocking_answer(move || {
        let Some(store) = state.registry.series() else {
            return Envelope::ok(SourceSenders::default());
        };
        let marks = Marks::read(&state.store);
        let learned = state.throughput.learned();
        let at = Now {
            store,
            group: &group,
            source: &source,
            now_ms,
        };
        at.senders(&readings, learned.as_deref(), &marks).into()
    })
    .await
}

/// One source, read as of a moment.
struct Now<'a> {
    store: &'a RegistryStore,
    group: &'a str,
    source: &'a str,
    now_ms: i64,
}

impl Now<'_> {
    /// Its sender fields, from `readings`, what was `learned`, and what an editor `marks`ed.
    fn senders(
        &self,
        readings: &[Reading],
        learned: Option<&Learned>,
        marks: &Marks,
    ) -> Result<SourceSenders, StoreError> {
        let (group, source) = (self.group, self.source);
        let edits = marks
            .fields
            .iter()
            .find(|f| f.group == group && f.edits.source == source)
            .map(|f| f.edits.clone())
            .unwrap_or_default();
        let picks = learned
            .and_then(|l| l.pick.fields.get(&(group.to_string(), source.to_string())))
            .cloned()
            .unwrap_or_default();
        let watched = learned.map(|l| l.watched(group, source)).unwrap_or_default();
        let since = TIER.align(self.now_ms) - DAY_MS;
        let rows = history(self.store, &watched, since)?;
        let mut names: BTreeSet<&String> = picks.keys().collect();
        names.extend(&edits.added);
        names.extend(&edits.unchecked);
        let mut fields = Vec::new();
        for field in names {
            let mut senders = Vec::new();
            for key in watched.iter().filter(|k| k.2.field == *field) {
                let of = rows.get(key).cloned().unwrap_or_default();
                let reading = readings
                    .iter()
                    .find(|r| r.group == group && r.source == source && r.sender.as_ref() == Some(&key.2));
                let shown = reading.and_then(Reading::shown).map(throughput::Judged::per_second);
                let below = reading.is_some_and(|r| r.outside == Some(Outside::Below));
                let lowered_by = match below {
                    true => self.lowered_by(&key.2, marks)?,
                    false => None,
                };
                senders.push(SenderNow {
                    value: key.2.value.clone(),
                    points: of
                        .iter()
                        .map(|(&at, &n)| (at, n / TIER.bucket_ms() as f64 * 1000.0))
                        .collect(),
                    expected: learned.map(|l| l.expected(key, of.keys().copied())).unwrap_or_default(),
                    now: shown.map(|(now, _)| now),
                    low: shown.map(|(_, band)| band.low),
                    high: shown.map(|(_, band)| band.high),
                    below,
                    lowered_by,
                });
            }
            senders.sort_by(|a, b| (!a.below, &a.value).cmp(&(!b.below, &b.value)));
            fields.push(FieldNow {
                field: field.clone(),
                checked: !edits.unchecked.contains(field),
                added: edits.added.contains(field),
                pick: picks.get(field).cloned(),
                senders,
            });
        }
        let rank = |f: &FieldNow| {
            let below = f.senders.iter().filter(|s| s.below).count();
            let state = match &f.pick {
                Some(FieldPick::Watched { .. }) => 0,
                Some(FieldPick::Learning { .. }) | None => 1,
                Some(FieldPick::OneValue) => 2,
                Some(FieldPick::TooMany) => 3,
            };
            (std::cmp::Reverse(below), !f.checked, state)
        };
        fields.sort_by(|a, b| rank(a).cmp(&rank(b)).then_with(|| a.field.cmp(&b.field)));
        Ok(SourceSenders { fields })
    }

    /// Who lowered `sender`'s warning that holds, if anyone did.
    fn lowered_by(&self, sender: &Sender, marks: &Marks) -> Result<Option<String>, StoreError> {
        let since = warnings::sender_spans(self.store, self.group, self.source, sender, self.now_ms)?
            .into_iter()
            .find(|s| s.ended_millis.is_none())
            .map(|s| s.started_millis);
        Ok(marks
            .lowered_by(self.group, self.source, Some(sender), since)
            .map(str::to_string))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const STEP: i64 = 5 * 60 * 1000;

    /// A field counted in every bucket of the `days` before `day_ms`.
    fn counted(day_ms: i64, days: i64) -> BTreeMap<i64, f64> {
        (day_ms - days * DAY_MS..day_ms)
            .step_by(STEP as usize)
            .map(|at| (at, 1.0))
            .collect()
    }

    /// Rows in every `every`th bucket of the `days` before `day_ms`.
    fn rows_every(day_ms: i64, days: i64, every: i64) -> BTreeMap<i64, f64> {
        (day_ms - days * DAY_MS..day_ms)
            .step_by((every * STEP) as usize)
            .map(|at| (at, 1.0))
            .collect()
    }

    #[test]
    fn a_value_with_a_row_every_hour_for_a_week_is_steady_and_one_with_a_quiet_afternoon_is_not() {
        let day = 100 * DAY_MS;
        let field = counted(day, 14);
        assert!(
            steady(&rows_every(day, 7, 12), &field, day),
            "never more than 11 buckets empty"
        );
        assert!(!steady(&rows_every(day, 7, 14), &field, day), "13 buckets empty");
        assert!(!steady(&rows_every(day, 6, 12), &field, day), "six days are too few");
    }

    #[test]
    fn a_value_stays_steady_for_a_week_after_its_last_steady_week() {
        let day = 100 * DAY_MS;
        let field = counted(day, 14);
        let week_ago: BTreeMap<i64, f64> = rows_every(day - 6 * DAY_MS, 7, 1);
        assert!(
            steady(&week_ago, &field, day),
            "its seven steady days ended six days ago"
        );
        let longer_ago: BTreeMap<i64, f64> = rows_every(day - 8 * DAY_MS, 6, 1);
        assert!(!steady(&longer_ago, &field, day));
    }

    #[test]
    fn a_bucket_the_field_was_not_counted_in_is_not_an_empty_one() {
        let day = 100 * DAY_MS;
        let mut field = counted(day, 14);
        // Two hours uncounted on every day: the value has no rows then, yet passes.
        let rows: BTreeMap<i64, f64> = rows_every(day, 7, 1)
            .into_iter()
            .filter(|(at, _)| at.rem_euclid(DAY_MS) >= 2 * 60 * 60 * 1000)
            .collect();
        field.retain(|at, _| at.rem_euclid(DAY_MS) >= 2 * 60 * 60 * 1000);
        assert!(steady(&rows, &field, day));
    }

    /// A field counted only once an hour judges its values by the buckets it was counted in: a value absent from
    /// thirteen of them in a row is not steady, though hours pass between them uncounted.
    #[test]
    fn a_run_without_rows_spans_the_buckets_the_field_was_not_counted_in() {
        let day = 100 * DAY_MS;
        let hourly = rows_every(day, 14, 12);
        assert!(
            steady(&rows_every(day, 7, 12), &hourly, day),
            "a row every counted hour"
        );
        assert!(
            !steady(&rows_every(day, 7, 12 * 14), &hourly, day),
            "thirteen counted hours empty"
        );
    }

    /// Each field is picked by what its values sent over the days before: the steady ones watched, too new to
    /// tell, one value, or too many.
    #[test]
    fn a_field_is_picked_by_what_its_values_sent() {
        use crate::heartbeat::{SenderCounts, SenderRows, StreamReport};
        let store = RegistryStore::open(std::path::Path::new(":memory:")).unwrap();
        let day = 100 * DAY_MS;
        let count = |at: i64, field: &str, values: &[&str], too_many| {
            let counts = SenderCounts {
                source: "s".into(),
                field: field.into(),
                values: values
                    .iter()
                    .map(|v| SenderRows {
                        value: v.to_string(),
                        rows: 1,
                    })
                    .collect(),
                too_many,
            };
            let report = StreamReport {
                senders: vec![counts],
                ..Default::default()
            };
            store.record_streams("g", &report, true, None, at).unwrap();
        };
        for at in (day - 8 * DAY_MS..day).step_by((12 * STEP) as usize) {
            count(at, "devname", &["fw-01", "fw-07"], false);
            count(at, "action", &["deny"], false);
        }
        count(day - DAY_MS, "devname", &["fw-12"], false);
        count(day - 2 * DAY_MS, "user", &[], true);
        for at in (day - 2 * DAY_MS..day).step_by((12 * STEP) as usize) {
            count(at, "host", &["a", "b"], false);
        }
        let picked = pick(&store, day).unwrap();
        let fields = &picked.fields[&("g".to_string(), "s".to_string())];
        let FieldPick::Watched { values } = &fields["devname"] else {
            panic!("{fields:?}")
        };
        let mut values = values.clone();
        values.sort();
        assert_eq!(values, ["fw-01", "fw-07"], "fw-12 sent once");
        assert_eq!(fields["action"], FieldPick::OneValue);
        assert_eq!(fields["user"], FieldPick::TooMany);
        assert_eq!(
            fields["host"],
            FieldPick::Learning {
                until_millis: day + 6 * DAY_MS
            }
        );
    }

    /// A field a worker counted though no record held it reads as no rows for its senders; where no worker counted it,
    /// as after a pipeline change took it out, they read nothing.
    #[test]
    fn a_watched_sender_reads_no_rows_where_its_field_was_counted_and_nothing_where_it_was_not() {
        use crate::heartbeat::{SenderCounts, SenderRows, StreamReport};
        let store = RegistryStore::open(std::path::Path::new(":memory:")).unwrap();
        let at = 100 * DAY_MS;
        let count = |at: i64, values: Vec<SenderRows>| {
            let counts = SenderCounts {
                source: "s".into(),
                field: "devname".into(),
                values,
                too_many: false,
            };
            let report = StreamReport {
                senders: vec![counts],
                ..Default::default()
            };
            store.record_streams("g", &report, true, None, at + 1000).unwrap();
        };
        count(at - STEP, Vec::new());
        count(
            at,
            vec![SenderRows {
                value: "fw-01".into(),
                rows: 3,
            }],
        );
        let fw = Sender {
            field: "devname".into(),
            value: "fw-01".into(),
        };
        let key = ("g".to_string(), "s".to_string(), fw);
        let read = history(&store, std::slice::from_ref(&key), at - 2 * STEP).unwrap();
        assert_eq!(read[&key], [(at - STEP, 0.0), (at, 3.0)].into());
    }
}
