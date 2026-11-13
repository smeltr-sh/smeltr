//! Who sends a source its rows, and whether each still sends what it normally does.
//!
//! A sender is one value of one field of a source's records: a firewall's name, a host, a sourcetype. Workers count
//! each value of every string field the pipeline delivers, and of the source's stream key, by the records that held
//! it (`streams::SenderTally`). A field that holds more than `heartbeat::SENDER_VALUES` values on a worker is no
//! sender field, and is counted no more. The leader sums each [`TIER`] bucket's counts as heartbeats bring them
//! ([`Pending`]) and writes the bucket once it ends into each field's and each value's day row: the buckets the field
//! was counted in, the buckets the value held a row in, and for a watched value the rows it held in each ([`Rows`]).
//!
//! Each day, the pick reads which values are senders: a value is watched once it went no longer than
//! [`LONGEST_EMPTY`] buckets without a row on each of [`PICK_DAYS`] days in a row, and stays watched until a further
//! [`PICK_DAYS`] pass without such a run. A bucket counts only where its field was counted, so a field a pipeline
//! change takes out leaves its senders unjudged rather than all below. A field with one value names no sender. An
//! editor may stop watching a field, add one the pick cannot offer (a number or a nested path), or stop watching
//! fields that come later (`marks::SourceFields`).
//!
//! A watched sender is judged as a source is (`throughput`), from its own rows since it was first watched, except by
//! the gap, and not while its source warns or is stopped.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use axum::extract::{Path, State};
use serde::{Deserialize, Serialize};

use super::AppState;
use super::envelope::Envelope;
use super::marks::Marks;
use super::series::{RegistryStore, Tier};
use super::store::StoreError;
use super::throughput::{self, ExpectedPoint, Learned, Outside, Reading};
use super::warnings;
use crate::heartbeat::SenderCounts;

/// The tier a sender's buckets are counted in, and its rows kept and judged at.
pub const TIER: Tier = Tier::FiveMinutes;

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

/// A field, by its group, source and name.
pub type FieldKey = (String, String, String);

/// The [`TIER`] buckets of one day that something held, a bit each, from the day's first; a bucket past the last byte
/// is not held.
pub type Buckets = Vec<u8>;

/// Which of its day's [`TIER`] buckets `bucket_ms` is, from the day's first.
fn index(bucket_ms: i64) -> usize {
    (bucket_ms.rem_euclid(DAY_MS) / TIER.bucket_ms()) as usize
}

/// The byte of a day's [`Buckets`] that holds `bucket_ms`, and its bit there.
fn bit(bucket_ms: i64) -> (usize, u8) {
    let i = index(bucket_ms);
    (i / 8, 1 << (i % 8))
}

/// Each day's [`Buckets`] of one field or value, by the day's start, oldest first.
pub type Days = Vec<(i64, Buckets)>;

/// Adds `bucket_ms` to its day's `buckets`.
pub fn mark(buckets: &mut Buckets, bucket_ms: i64) {
    let (byte, bit) = bit(bucket_ms);
    if buckets.len() <= byte {
        buckets.resize(byte + 1, 0);
    }
    buckets[byte] |= bit;
}

/// Whether its day's `buckets` hold `bucket_ms`.
fn marked(buckets: &[u8], bucket_ms: i64) -> bool {
    let (byte, bit) = bit(bucket_ms);
    buckets.get(byte).is_some_and(|b| b & bit != 0)
}

/// Each bucket the day starting at `day_ms` holds in `buckets`, oldest first.
fn each_marked(day_ms: i64, buckets: &[u8]) -> impl Iterator<Item = i64> + '_ {
    (day_ms..day_ms + DAY_MS)
        .step_by(TIER.bucket_ms() as usize)
        .filter(|&at| marked(buckets, at))
}

/// The rows a watched value held in each [`TIER`] bucket of one day, from the day's first, each count a LEB128
/// varint of a byte or two. A bucket past the last reads as none.
pub type Rows = Vec<u8>;

/// Adds `rows` to what its day's `kept` holds for `bucket_ms`.
pub fn add_rows(kept: &mut Rows, bucket_ms: i64, rows: u64) {
    let mut each: Vec<u64> = each_count(kept).collect();
    let i = index(bucket_ms);
    if each.len() <= i {
        each.resize(i + 1, 0);
    }
    each[i] = each[i].saturating_add(rows);
    kept.clear();
    for mut n in each {
        while n >= 0x80 {
            kept.push(n as u8 | 0x80);
            n >>= 7;
        }
        kept.push(n as u8);
    }
}

/// Each count in `kept`, from the day's first bucket.
fn each_count(kept: &[u8]) -> impl Iterator<Item = u64> + '_ {
    let mut at = 0;
    std::iter::from_fn(move || {
        let (mut n, mut shift) = (0u64, 0u32);
        loop {
            let byte = *kept.get(at)?;
            at += 1;
            n |= u64::from(byte & 0x7f).checked_shl(shift).unwrap_or(0);
            shift += 7;
            if byte & 0x80 == 0 {
                return Some(n);
            }
        }
    })
}

/// The sender counts of one [`TIER`] bucket, summed across the heartbeats that brought them, until it is written.
#[derive(Debug, Default)]
pub struct Pending {
    pub bucket_ms: i64,
    /// Each field, by group, source and name.
    pub fields: HashMap<String, HashMap<String, HashMap<String, FieldCounts>>>,
}

/// What workers said of one field in one bucket.
#[derive(Debug, Default)]
pub struct FieldCounts {
    /// A worker counted it.
    pub counted: bool,
    /// A worker counts it no more, for holding too many values.
    pub over: bool,
    /// Each value's rows.
    pub values: HashMap<String, u64>,
}

impl Pending {
    /// Adds `group`'s `senders`, as one heartbeat reported them.
    pub fn add(&mut self, group: &str, senders: &[SenderCounts]) {
        for counts in senders {
            let of_group = entry(&mut self.fields, group);
            let field = entry(entry(of_group, &counts.source), &counts.field);
            field.over |= counts.too_many;
            field.counted |= !counts.too_many;
            for v in &counts.values {
                *entry(&mut field.values, &v.value) += v.rows;
            }
        }
    }
}

/// `map`'s entry for `key`, added empty if it has none. A heartbeat names the same fields and values again and again,
/// so a key is copied only the first time.
fn entry<'a, T: Default>(map: &'a mut HashMap<String, T>, key: &str) -> &'a mut T {
    if !map.contains_key(key) {
        map.insert(key.to_string(), T::default());
    }
    map.get_mut(key).expect("just added")
}

/// What the leader holds of senders between writes: the bucket not yet written, and the values whose rows it keeps,
/// every value's until a pick has said (`keep`).
#[derive(Debug, Default)]
pub struct Held {
    pub pending: Pending,
    pub watched: Option<HashSet<SenderKey>>,
}

/// One field's days: the buckets it was counted in, by day, and the days a worker said it passed its cap.
#[derive(Debug, Default)]
pub struct FieldDays {
    pub counted: BTreeMap<i64, Buckets>,
    pub over: BTreeSet<i64>,
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

/// The oldest day the pick as of `day_ms` reads: the 2 x [`PICK_DAYS`] days before it.
fn read_from(day_ms: i64) -> i64 {
    day_ms - 2 * PICK_DAYS * DAY_MS
}

/// When a field whose values first held a row at `first_ms` has been counted for [`PICK_DAYS`] full days.
fn learning_until(first_ms: i64) -> i64 {
    day_of(first_ms) + (PICK_DAYS + 1) * DAY_MS
}

/// What the pick reads of one field's values.
#[derive(Default)]
struct Values {
    /// How many values held a row.
    held: usize,
    /// The first day one did.
    first: Option<i64>,
    steady: Vec<String>,
}

/// Every source's sender fields as of `day_ms`, from the days since [`read_from`], read one value at a time.
pub fn pick(store: &RegistryStore, day_ms: i64) -> Result<Pick, StoreError> {
    let since = read_from(day_ms);
    let fields = store.sender_fields_since(since)?;
    let mut read: BTreeMap<FieldKey, Values> = BTreeMap::new();
    store.each_sender_value(since, |key, value, days| {
        let days: BTreeMap<i64, Buckets> = days.into_iter().filter(|&(day, _)| day < day_ms).collect();
        let Some(&first) = days.keys().next() else { return };
        let of = read.entry(key.clone()).or_default();
        of.held += 1;
        of.first = Some(of.first.map_or(first, |f| f.min(first)));
        if fields.get(key).is_some_and(|f| steady(&days, &f.counted, day_ms)) {
            of.steady.push(value.to_string());
        }
    })?;
    let over = |key: &FieldKey| {
        let since = day_ms - PICK_DAYS * DAY_MS;
        fields.get(key).is_some_and(|f| f.over.range(since..).next().is_some())
    };
    let keys: BTreeSet<FieldKey> = read.keys().chain(fields.keys().filter(|k| over(k))).cloned().collect();
    let mut out = Pick {
        day_ms,
        fields: BTreeMap::new(),
    };
    for key in keys {
        let values = read.remove(&key).unwrap_or_default();
        let pick = if over(&key) {
            FieldPick::TooMany
        } else if values.held <= 1 {
            FieldPick::OneValue
        } else {
            match values.first.map(learning_until) {
                Some(until) if values.steady.is_empty() && until > day_ms => {
                    FieldPick::Learning { until_millis: until }
                }
                _ => FieldPick::Watched { values: values.steady },
            }
        };
        let (group, source, field) = key;
        out.fields.entry((group, source)).or_default().insert(field, pick);
    }
    Ok(out)
}

/// Keeps rows only for the values `pick` watches, drops the rows kept for the rest, and forgets the day rows no later
/// pick or judge reads.
pub fn keep(store: &RegistryStore, pick: &Pick) -> Result<(), StoreError> {
    let watched: HashSet<SenderKey> = pick.watched(|_, _, _| false).into_iter().collect();
    store.watch_senders(watched.clone());
    store.forget_senders(
        &watched,
        read_from(pick.day_ms),
        day_of(pick.day_ms - TIER.retention_ms()),
    )
}

/// Whether a value that held a row in its `sent` buckets, by day, is steady: it passed on [`PICK_DAYS`] days in a
/// row, the last of them within the [`PICK_DAYS`] days before `day_ms`. A day passes when the value went no more than
/// [`LONGEST_EMPTY`] buckets without a row, counting only the buckets its field was `counted` in; a day its field was
/// not counted at all does not pass.
fn steady(sent: &BTreeMap<i64, Buckets>, counted: &BTreeMap<i64, Buckets>, day_ms: i64) -> bool {
    let passed = |day: i64| {
        let start = day_ms - (day + 1) * DAY_MS;
        let Some(counted) = counted.get(&start) else {
            return false;
        };
        let sent = sent.get(&start).map(Vec::as_slice).unwrap_or_default();
        let (mut run, mut longest, mut seen) = (0, 0, false);
        // A bucket the field was not counted in neither ends a run nor adds to one.
        for at in each_marked(start, counted) {
            seen = true;
            match marked(sent, at) {
                true => run = 0,
                false => {
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

/// Each of `watched`'s rows by bucket from `since_ms`, from its first row on. A bucket its field was counted in reads
/// as no rows where the sender has none; a bucket its field was not counted in has no point. A sender with no row kept
/// yet has none at all.
pub fn history(
    store: &RegistryStore,
    watched: &[SenderKey],
    since_ms: i64,
) -> Result<HashMap<SenderKey, BTreeMap<i64, f64>>, StoreError> {
    let fields: BTreeSet<FieldKey> = watched
        .iter()
        .map(|(group, source, s)| (group.clone(), source.clone(), s.field.clone()))
        .collect();
    let mut counted: HashMap<FieldKey, Vec<i64>> = HashMap::new();
    for key in fields {
        let days = store.sender_field_days(&key.0, &key.1, &key.2, day_of(since_ms))?;
        let buckets = days.iter().flat_map(|(day, buckets)| each_marked(*day, buckets));
        counted.insert(key, buckets.filter(|&at| at >= since_ms).collect());
    }
    let mut out = HashMap::new();
    for key in watched {
        let (group, source, Sender { field, value }) = key;
        let held: BTreeMap<i64, u64> = store
            .sender_rows(group, source, field, value)?
            .iter()
            .flat_map(|(day, kept)| each_held(*day, kept))
            .collect();
        let (Some(&first), Some(counted)) = (
            held.keys().next(),
            counted.get(&(group.clone(), source.clone(), field.clone())),
        ) else {
            continue;
        };
        let rows = counted
            .iter()
            .filter(|&&at| at >= first)
            .map(|&at| (at, held.get(&at).copied().unwrap_or(0) as f64))
            .collect();
        out.insert(key.clone(), rows);
    }
    Ok(out)
}

/// Each bucket of the day starting at `day_ms` that its `kept` rows say held a row, with how many.
fn each_held(day_ms: i64, kept: &[u8]) -> impl Iterator<Item = (i64, u64)> + '_ {
    (day_ms..)
        .step_by(TIER.bucket_ms() as usize)
        .zip(each_count(kept))
        .filter(|&(_, rows)| rows > 0)
}

/// One source's sender fields, as the Senders tab shows them.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SourceSenders {
    /// Those with a sender below normal first, then by what the pick read of each.
    pub fields: Vec<FieldNow>,
    /// A field that comes later is watched, until an editor says not (`marks::SourceFields::locked`).
    pub watches_new: bool,
}

impl Default for SourceSenders {
    /// No fields, and new ones watched, as for a source nobody edited.
    fn default() -> Self {
        SourceSenders {
            fields: Vec::new(),
            watches_new: true,
        }
    }
}

/// One field of a source, and its senders.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FieldNow {
    pub field: String,
    /// It is watched (`marks::SourceFields::watches`).
    pub checked: bool,
    /// An editor added it.
    pub added: bool,
    /// It came after an editor stopped watching new fields, and is not watched until one checks it.
    pub new: bool,
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
        let edited = marks
            .fields
            .iter()
            .find(|f| f.group == group && f.edits.source == source)
            .cloned()
            .unwrap_or_default();
        let edits = &edited.edits;
        let mut picks = learned
            .and_then(|l| l.pick.fields.get(&(group.to_string(), source.to_string())))
            .cloned()
            .unwrap_or_default();
        let since = TIER.align(self.now_ms) - DAY_MS;
        // A field whose values first held a row after the pick read is learning, as the next pick reads it.
        let mut firsts: BTreeMap<String, i64> = BTreeMap::new();
        for (field, day, buckets) in self.store.sender_value_days(group, source, day_of(since))? {
            if let Some(first) = each_marked(day, &buckets).find(|&at| at >= since) {
                let held = firsts.entry(field).or_insert(first);
                *held = first.min(*held);
            }
        }
        for (field, first) in firsts {
            let until_millis = learning_until(first);
            picks.entry(field).or_insert(FieldPick::Learning { until_millis });
        }
        let watched = learned.map(|l| l.watched(group, source)).unwrap_or_default();
        let rows = history(self.store, &watched, since)?;
        let mut names: BTreeSet<&String> = picks.keys().collect();
        names.extend(&edits.added);
        names.extend(&edits.unchecked);
        names.extend(edited.locked.iter().flatten());
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
                checked: edited.watches(field),
                added: edits.added.contains(field),
                new: edited.is_new(field),
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
        Ok(SourceSenders {
            fields,
            watches_new: edited.locked.is_none(),
        })
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

    /// Every bucket of the `days` before `day_ms`.
    fn counted(day_ms: i64, days: i64) -> BTreeSet<i64> {
        rows_every(day_ms, days, 1)
    }

    /// Every `every`th bucket of the `days` before `day_ms`.
    fn rows_every(day_ms: i64, days: i64, every: i64) -> BTreeSet<i64> {
        (day_ms - days * DAY_MS..day_ms)
            .step_by((every * STEP) as usize)
            .collect()
    }

    /// [`steady`] of a value that held a row in the `sent` buckets, in a field counted in the `counted` buckets.
    fn steady_in(sent: &BTreeSet<i64>, counted: &BTreeSet<i64>, day_ms: i64) -> bool {
        let days = |buckets: &BTreeSet<i64>| {
            let mut days: BTreeMap<i64, Buckets> = BTreeMap::new();
            for &at in buckets {
                mark(days.entry(day_of(at)).or_default(), at);
            }
            days
        };
        steady(&days(sent), &days(counted), day_ms)
    }

    /// Records a worker's report at `at` that it counted `field` of source `s` in group `g`: each of `values` with the
    /// rows that held it, or `too_many` once the field passed its cap.
    fn count(store: &RegistryStore, at: i64, field: &str, values: &[(&str, u64)], too_many: bool) {
        use crate::heartbeat::{SenderRows, StreamReport};
        let values = values
            .iter()
            .map(|&(value, rows)| SenderRows {
                value: value.to_string(),
                rows,
            })
            .collect();
        let counts = SenderCounts {
            source: "s".into(),
            field: field.into(),
            values,
            too_many,
        };
        let report = StreamReport {
            senders: vec![counts],
            ..Default::default()
        };
        store.record_streams("g", &report, true, None, at).unwrap();
    }

    /// A heartbeat at `at` that reports no senders, which writes the bucket held before.
    fn beat(store: &RegistryStore, at: i64) {
        let report = crate::heartbeat::StreamReport::default();
        store.record_streams("g", &report, true, None, at).unwrap();
    }

    fn memory_store() -> RegistryStore {
        RegistryStore::open(std::path::Path::new(":memory:")).unwrap()
    }

    /// Each bucket `value` of field `devname` keeps rows for, with how many.
    fn rows(store: &RegistryStore, value: &str) -> Vec<(i64, u64)> {
        let days = store.sender_rows("g", "s", "devname", value).unwrap();
        days.iter().flat_map(|(day, kept)| each_held(*day, kept)).collect()
    }

    #[test]
    fn a_value_with_a_row_every_hour_for_a_week_is_steady_and_one_with_a_quiet_afternoon_is_not() {
        let day = 100 * DAY_MS;
        let field = counted(day, 14);
        assert!(
            steady_in(&rows_every(day, 7, 12), &field, day),
            "never more than 11 buckets empty"
        );
        assert!(!steady_in(&rows_every(day, 7, 14), &field, day), "13 buckets empty");
        assert!(!steady_in(&rows_every(day, 6, 12), &field, day), "six days are too few");
    }

    #[test]
    fn a_value_stays_steady_for_a_week_after_its_last_steady_week() {
        let day = 100 * DAY_MS;
        let field = counted(day, 14);
        assert!(
            steady_in(&rows_every(day - 6 * DAY_MS, 7, 1), &field, day),
            "its seven steady days ended six days ago"
        );
        assert!(!steady_in(&rows_every(day - 8 * DAY_MS, 6, 1), &field, day));
    }

    #[test]
    fn a_bucket_the_field_was_not_counted_in_is_not_an_empty_one() {
        let day = 100 * DAY_MS;
        // Two hours uncounted on every day: the value has no rows then, yet passes.
        let after_two = |at: &i64| at.rem_euclid(DAY_MS) >= 2 * 60 * 60 * 1000;
        let rows: BTreeSet<i64> = rows_every(day, 7, 1).into_iter().filter(after_two).collect();
        let field: BTreeSet<i64> = counted(day, 14).into_iter().filter(after_two).collect();
        assert!(steady_in(&rows, &field, day));
    }

    /// A field counted only once an hour judges its values by the buckets it was counted in: a value absent from
    /// thirteen of them in a row is not steady, though hours pass between them uncounted.
    #[test]
    fn a_run_without_rows_spans_the_buckets_the_field_was_not_counted_in() {
        let day = 100 * DAY_MS;
        let hourly = rows_every(day, 14, 12);
        assert!(
            steady_in(&rows_every(day, 7, 12), &hourly, day),
            "a row every counted hour"
        );
        assert!(
            !steady_in(&rows_every(day, 7, 12 * 14), &hourly, day),
            "thirteen counted hours empty"
        );
    }

    /// Heartbeats in one bucket are summed, and the bucket is written once a heartbeat lands in a later one.
    #[test]
    fn a_bucket_is_written_once_the_next_begins() {
        let store = memory_store();
        let at = 100 * DAY_MS;
        count(&store, at, "devname", &[("fw-01", 2)], false);
        count(&store, at + 1000, "devname", &[("fw-01", 3)], false);
        assert_eq!(rows(&store, "fw-01"), []);
        beat(&store, at + STEP);
        assert_eq!(rows(&store, "fw-01"), [(at, 5)]);
        let days = store.sender_field_days("g", "s", "devname", at).unwrap();
        assert_eq!(days.len(), 1);
        assert_eq!(each_marked(days[0].0, &days[0].1).collect::<Vec<_>>(), [at]);
    }

    /// Each field is picked by what its values sent over the days before: the steady ones watched, too new to
    /// tell, one value, or too many.
    #[test]
    fn a_field_is_picked_by_what_its_values_sent() {
        let store = memory_store();
        let day = 100 * DAY_MS;
        for at in (day - 8 * DAY_MS..day).step_by((12 * STEP) as usize) {
            count(&store, at, "devname", &[("fw-01", 1), ("fw-07", 1)], false);
            count(&store, at, "action", &[("deny", 1)], false);
        }
        count(&store, day - DAY_MS, "devname", &[("fw-12", 1)], false);
        count(&store, day - 2 * DAY_MS, "user", &[], true);
        for at in (day - 2 * DAY_MS..day).step_by((12 * STEP) as usize) {
            count(&store, at, "host", &[("a", 1), ("b", 1)], false);
        }
        beat(&store, day);
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

    /// A watched sender reads nothing before its first row, no rows where its field was counted, and nothing where
    /// no worker counted the field, as after a pipeline change took it out.
    #[test]
    fn a_watched_sender_reads_no_rows_where_its_field_was_counted_and_nothing_where_it_was_not() {
        let store = memory_store();
        let at = 100 * DAY_MS;
        count(&store, at - 3 * STEP, "devname", &[], false);
        count(&store, at - 2 * STEP, "devname", &[("fw-01", 2)], false);
        count(&store, at, "devname", &[], false);
        count(&store, at + STEP, "devname", &[("fw-01", 3)], false);
        beat(&store, at + 2 * STEP);
        let fw = Sender {
            field: "devname".into(),
            value: "fw-01".into(),
        };
        let key = ("g".to_string(), "s".to_string(), fw);
        let read = history(&store, std::slice::from_ref(&key), at - 4 * STEP).unwrap();
        assert_eq!(read[&key], [(at - 2 * STEP, 2.0), (at, 0.0), (at + STEP, 3.0)].into());
    }

    /// Once a pick has read them, only the values it watches keep their rows, for the four weeks a normal is learned
    /// from, and the days of the rest that no later pick reads are forgotten.
    #[test]
    fn only_a_watched_value_keeps_its_rows() {
        let store = memory_store();
        let day = 100 * DAY_MS;
        let oldest = day - 31 * DAY_MS;
        for at in (oldest..day).step_by((12 * STEP) as usize) {
            count(&store, at, "devname", &[("fw-01", 1), ("fw-07", 1)], false);
        }
        let (old, recent) = (day - 20 * DAY_MS, day - DAY_MS);
        count(&store, old, "devname", &[("fw-12", 1)], false);
        count(&store, recent, "devname", &[("fw-12", 2)], false);
        beat(&store, day);
        assert_eq!(rows(&store, "fw-12"), [(old, 1), (recent, 2)], "kept until a pick says");

        keep(&store, &pick(&store, day).unwrap()).unwrap();
        assert_eq!(rows(&store, "fw-12"), []);
        let learned_from = day_of(day - TIER.retention_ms());
        assert_eq!(rows(&store, "fw-01").first(), Some(&(learned_from, 1)), "watched");
        count(&store, day, "devname", &[("fw-01", 1), ("fw-12", 1)], false);
        beat(&store, day + STEP);
        assert_eq!(rows(&store, "fw-12"), []);
        assert_eq!(rows(&store, "fw-01").last(), Some(&(day, 1)));
        let mut forgotten = true;
        store
            .each_sender_value(0, |_, value, days| {
                let from = if value == "fw-12" { read_from(day) } else { learned_from };
                forgotten &= days.iter().all(|&(d, _)| d >= from)
            })
            .unwrap();
        assert!(
            forgotten,
            "the days before the pick's, or before the learn's for a watched value"
        );
    }

    /// A bucket's count takes one byte up to 127 rows and more past it, and a count past the last bucket written
    /// reads as none.
    #[test]
    fn rows_are_kept_a_varint_a_bucket() {
        let day = 100 * DAY_MS;
        let mut kept = Rows::new();
        add_rows(&mut kept, day + 2 * STEP, 127);
        assert_eq!(kept, [0, 0, 127]);
        add_rows(&mut kept, day + 2 * STEP, 1);
        add_rows(&mut kept, day + 3 * STEP, u64::MAX);
        assert_eq!(kept.len(), 2 + 2 + 10);
        assert_eq!(
            each_held(day, &kept).collect::<Vec<_>>(),
            [(day + 2 * STEP, 128), (day + 3 * STEP, u64::MAX)]
        );
    }

    /// A field whose values first held a row after the day's pick read shows on the tab that day, learning until it
    /// has been counted the days a pick needs.
    #[test]
    fn a_field_counted_after_the_pick_read_is_listed_as_learning() {
        let store = memory_store();
        let day = 100 * DAY_MS;
        let first = day + 3 * 60 * 60 * 1000;
        count(&store, first, "host", &[("a", 1), ("b", 1)], false);
        count(&store, first + STEP, "user", &[], false);
        beat(&store, first + 2 * STEP);
        let now = Now {
            store: &store,
            group: "g",
            source: "s",
            now_ms: first + 2 * STEP,
        };
        let listed = now.senders(&[], None, &Marks::default()).unwrap();
        let fields: Vec<(&str, Option<&FieldPick>)> = listed
            .fields
            .iter()
            .map(|f| (f.field.as_str(), f.pick.as_ref()))
            .collect();
        let learning = FieldPick::Learning {
            until_millis: day + 8 * DAY_MS,
        };
        assert_eq!(fields, [("host", Some(&learning))], "no record held user");
    }
}
