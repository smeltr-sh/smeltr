//! What each source normally reads, and whether it is reading that now.
//!
//! A source's normal is learned from the rows it read, which the registry already keeps (`series.rs`). The last
//! [`WINDOW`] is set against every window of the same length within [`NEIGHBORHOOD`] of the same time of day on each
//! of the last [`HISTORY_DAYS`] days, and the band is the share of those windows the operator asked for, cut evenly
//! from both ends. Empirical rather than a mean and a deviation: rows read are bursty and often zero, which a normal
//! curve does not describe, and a percentile needs no shape at all.
//!
//! Only windows the source was running for throughout count, on either side. A bucket with no point means no worker
//! reported the source then -- not deployed, or a worker gone -- which says nothing about how much it reads.
//!
//! Nothing here is stored. The answer is recomputed from the series once per [`BUCKET`], since nothing it reads
//! changes in between, and cached until the next.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;

use super::envelope::{Envelope, Refusal};
use super::series::{RegistryStore, SubjectPoints, Tier};
use super::store::{Actor, Store};
use super::warnings::label;
use super::{AppState, ROWS_IN_METRIC, run_store};

/// How much reading is judged at once. Long enough that a quiet minute between bursts is not an outage, short enough
/// that a source that stopped is noticed within the hour.
pub const WINDOW: Duration = Duration::from_secs(30 * 60);

/// The resolution history is judged at: the finest every tier the history spans can be read at.
const BUCKET: Tier = Tier::FiveMinutes;

/// How many days back the normal is learned from. The series keeps five-minute buckets a day longer, so the oldest
/// day's windows and their neighborhood are still in them.
const HISTORY_DAYS: i64 = 7;

/// How far either side of the same time of day a past window may end and still count as the same time of day.
const NEIGHBORHOOD: Duration = Duration::from_secs(30 * 60);

/// Days with at least one window the source ran for throughout, before its normal is judged at all. Fewer, and one
/// odd day is most of what it knows.
pub const MIN_DAYS: usize = 3;

const DAY_MS: i64 = 24 * 60 * 60 * 1000;
const HOUR_MS: i64 = 60 * 60 * 1000;

/// Where the band's share is kept, as a percent of past windows.
pub const BAND_KEY: &str = "warnings.source_rows_band_percent";

/// The band where nobody has said: one window in a hundred at this time of day falls outside it, half each side.
pub const DEFAULT_BAND_PERCENT: f64 = 99.0;

/// Its bounds. Narrower than half, and an ordinary window warns. Wider than 99.9, and the band's edges are the
/// widest windows a week holds, however much wider it is asked to be.
pub const MIN_BAND_PERCENT: f64 = 50.0;
pub const MAX_BAND_PERCENT: f64 = 99.9;

/// How many rows one source normally reads in a [`WINDOW`] at this time of day: the share of past windows asked for
/// lies between `low` and `high`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Band {
    pub low: f64,
    pub high: f64,
}

/// One source's last [`WINDOW`] and its normal.
#[derive(Debug, Clone, PartialEq)]
pub struct Reading {
    pub group: String,
    pub source: String,
    /// Rows read over the window that ended at `ended_ms`.
    pub rows: f64,
    pub ended_ms: i64,
    pub band: Band,
}

/// Which side of its band a source's reading is on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Outside {
    Below,
    Above,
}

impl Reading {
    /// `None` while the reading is within its band, edges included.
    pub fn outside(&self) -> Option<Outside> {
        if self.rows < self.band.low {
            Some(Outside::Below)
        } else if self.rows > self.band.high {
            Some(Outside::Above)
        } else {
            None
        }
    }
}

/// The last answer, and what it was computed for: the window's end and the band's share. A new five-minute bucket or a
/// new setting is a new question. One caller computes a new answer at a time; the rest read the last meanwhile.
#[derive(Default)]
pub struct Readings {
    last: Mutex<Option<Answer>>,
    computing: Mutex<()>,
}

type Answer = (i64, u64, Arc<Vec<Reading>>);

impl Readings {
    /// Every source with enough history to judge, as of `now_ms`. `share` is the part of past windows the band holds,
    /// in (0, 1). Waits for every heartbeat sent before `now_ms - heartbeat_interval` to land in the window it judges.
    ///
    /// While another caller computes the new answer, this returns the last one rather than wait for it. Only a
    /// caller with no answer at all to fall back on waits.
    pub fn at(
        &self,
        store: &RegistryStore,
        now_ms: i64,
        heartbeat_interval: Duration,
        share: f64,
    ) -> Arc<Vec<Reading>> {
        let ended_ms = ended(now_ms, heartbeat_interval);
        if let Some(fresh) = self.fresh(ended_ms, share) {
            return fresh;
        }
        let _computing = match self.computing.try_lock() {
            Ok(guard) => guard,
            Err(_) => match self.last() {
                Some(last) => return last,
                None => self.computing.lock().unwrap_or_else(|e| e.into_inner()),
            },
        };
        // Whoever held the lock before may have just computed this very answer.
        if let Some(fresh) = self.fresh(ended_ms, share) {
            return fresh;
        }
        let readings = Arc::new(compute(store, ended_ms, share));
        *self.last.lock().unwrap_or_else(|e| e.into_inner()) = Some((ended_ms, share.to_bits(), readings.clone()));
        readings
    }

    /// The answer for the window ending at `ended_ms` and `share`, if that is the one held.
    fn fresh(&self, ended_ms: i64, share: f64) -> Option<Arc<Vec<Reading>>> {
        match self.last.lock().unwrap_or_else(|e| e.into_inner()).as_ref() {
            Some((at, bits, readings)) if *at == ended_ms && *bits == share.to_bits() => Some(readings.clone()),
            _ => None,
        }
    }

    /// The answer held, whatever it was computed for.
    fn last(&self) -> Option<Arc<Vec<Reading>>> {
        self.last
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .map(|(_, _, readings)| readings.clone())
    }
}

/// The end of the window judged at `now_ms`: the last bucket every heartbeat sent before then has landed in.
fn ended(now_ms: i64, heartbeat_interval: Duration) -> i64 {
    BUCKET.align(now_ms - heartbeat_interval.as_millis() as i64)
}

/// A count of rows, rounded, with its thousands set apart as a reader writes them.
pub fn grouped(rows: f64) -> String {
    let digits = (rows.max(0.0).round() as u64).to_string();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);
    for (i, d) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(d);
    }
    out
}

/// [`Readings::at`] without the cache.
fn compute(store: &RegistryStore, ended_ms: i64, share: f64) -> Vec<Reading> {
    let oldest = ended_ms - HISTORY_DAYS * DAY_MS - NEIGHBORHOOD.as_millis() as i64 - WINDOW.as_millis() as i64;
    let buckets = match history(store, BUCKET.and_finer(), BUCKET, oldest) {
        Ok(buckets) => buckets,
        Err(e) => {
            tracing::warn!(
                target: "throughput.series_unreadable",
                error = %e,
                "could not read rows read, so no source is judged against its normal this pass"
            );
            return Vec::new();
        }
    };
    let mut out: Vec<Reading> = buckets
        .into_iter()
        .filter_map(|((group, source), of)| {
            let rows = window_rows(&of, ended_ms)?;
            let band = band(&of, ended_ms, share)?;
            Some(Reading {
                group,
                source,
                rows,
                ended_ms,
                band,
            })
        })
        .collect();
    out.sort_by(|a, b| (&a.group, &a.source).cmp(&(&b.group, &b.source)));
    out
}

/// Rows read over the [`WINDOW`] ending at `ended_ms`, or `None` if any of its buckets has no point.
fn window_rows(buckets: &BTreeMap<i64, f64>, ended_ms: i64) -> Option<f64> {
    let width = BUCKET.bucket_ms();
    let count = WINDOW.as_millis() as i64 / width;
    (1..=count).map(|i| buckets.get(&(ended_ms - i * width)).copied()).sum()
}

/// The band of the windows at this time of day on past days, or `None` while fewer than [`MIN_DAYS`] days have one.
fn band(buckets: &BTreeMap<i64, f64>, ended_ms: i64, share: f64) -> Option<Band> {
    let width = BUCKET.bucket_ms();
    band_of(
        |ended| window_rows(buckets, ended),
        ended_ms,
        width,
        NEIGHBORHOOD.as_millis() as i64 / width,
        share,
    )
}

/// The band of `rows` at `ended_ms` on each past day, `reach` steps of `step_ms` either side, or `None` while fewer
/// than [`MIN_DAYS`] days have one. `rows` is what a window ending at a time read, if it was read throughout.
fn band_of(rows: impl Fn(i64) -> Option<f64>, ended_ms: i64, step_ms: i64, reach: i64, share: f64) -> Option<Band> {
    let mut samples = Vec::new();
    let mut days = HashSet::new();
    for day in 1..=HISTORY_DAYS {
        for step in -reach..=reach {
            if let Some(rows) = rows(ended_ms - day * DAY_MS + step * step_ms) {
                samples.push(rows);
                days.insert(day);
            }
        }
    }
    if days.len() < MIN_DAYS {
        return None;
    }
    samples.sort_by(f64::total_cmp);
    let tail = (1.0 - share) / 2.0;
    Some(Band {
        low: quantile(&samples, tail),
        high: quantile(&samples, 1.0 - tail),
    })
}

/// Each source's rows by bucket: (group, source) to bucket start to rows.
type History = HashMap<(String, String), BTreeMap<i64, f64>>;

/// Rows read per source at `tiers`, read together and summed into `into`'s buckets.
fn history(
    store: &RegistryStore,
    tiers: &[Tier],
    into: Tier,
    since_ms: i64,
) -> Result<History, super::store::StoreError> {
    // The tiers hold disjoint spans of age, so the history is all of them read together.
    Ok(summed(
        store
            .series_since_at(ROWS_IN_METRIC, tiers, since_ms)?
            .into_iter()
            .flatten(),
        into,
    ))
}

/// Rows read per source, summed into `into`'s buckets.
fn summed(series: impl Iterator<Item = (String, SubjectPoints)>, into: Tier) -> History {
    let mut buckets = History::new();
    for (key, points) in series {
        let Ok(labels) = serde_json::from_str::<Vec<(String, String)>>(&key) else {
            continue;
        };
        let (Some(group), Some(source)) = (label(&labels, "group"), label(&labels, "source")) else {
            continue;
        };
        let of = buckets.entry((group.to_string(), source.to_string())).or_default();
        for (at, rows) in points {
            *of.entry(into.align(at)).or_default() += rows;
        }
    }
    buckets
}

/// What one source was expected to read per second from `from_millis` to `to_millis`.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExpectedPoint {
    pub from_millis: i64,
    pub to_millis: i64,
    pub low: f64,
    pub high: f64,
}

/// `group`'s `source`'s band, as rows per second, for each bucket from `since_ms` to the one `now_ms` is in. Up to a
/// day back the bucket is five minutes and its band the [`WINDOW`] ending with it, as the warning judges; further
/// back, history this old is hourly, so the bucket is an hour and its band that hour on past days. A bucket with too
/// little history before it has none. The band reads only past days, so the bucket still filling has one too.
pub fn expected_over(
    store: &RegistryStore,
    group: &str,
    source: &str,
    since_ms: i64,
    now_ms: i64,
    share: f64,
) -> Result<Vec<ExpectedPoint>, super::store::StoreError> {
    let hourly = now_ms - since_ms > DAY_MS;
    let (into, window_ms) = if hourly {
        (Tier::OneHour, HOUR_MS)
    } else {
        (BUCKET, WINDOW.as_millis() as i64)
    };
    let width = into.bucket_ms();
    let reach = if hourly {
        1
    } else {
        NEIGHBORHOOD.as_millis() as i64 / width
    };
    // The first bucket's oldest sample, and for an hour the hour before it that says it was read.
    let oldest = into.align(since_ms) - HISTORY_DAYS * DAY_MS - reach * width - window_ms;
    let key = (group.to_string(), source.to_string());
    // One read, so `maintain` cannot move a bucket from the five-minute tier to the hourly one between them.
    let mut read = store
        .series_since_at(ROWS_IN_METRIC, into.and_finer(), oldest)?
        .into_iter();
    let fine = summed(read.by_ref().take(BUCKET.and_finer().len()).flatten(), BUCKET)
        .remove(&key)
        .unwrap_or_default();
    let hours = summed(read.flatten(), Tier::OneHour).remove(&key).unwrap_or_default();
    if fine.is_empty() && hours.is_empty() {
        return Ok(Vec::new());
    }
    // An hour still in five-minute buckets counts only if every one of them was read, as the warning's window does:
    // one cut short by a restart would pull the band down at that hour on every day after it. An hour the hourly tier
    // holds no longer says how much of it was read, so it counts only between two hours that were read at all: that
    // leaves out the first hour of a source and the edges of an outage, though not an outage inside one hour.
    let five = BUCKET.bucket_ms();
    let read_at_all =
        |start: i64| hours.contains_key(&start) || (0..HOUR_MS / five).any(|i| fine.contains_key(&(start + i * five)));
    let rows = |ended: i64| -> Option<f64> {
        if !hourly {
            return window_rows(&fine, ended);
        }
        let start = ended - HOUR_MS;
        let mut buckets = (0..HOUR_MS / five).map(|i| fine.get(&(start + i * five)).copied());
        match hours.get(&start) {
            Some(rows) if read_at_all(start - HOUR_MS) && read_at_all(ended) => {
                Some(rows + buckets.flatten().sum::<f64>())
            }
            Some(_) => None,
            None => buckets.try_fold(0.0, |sum, rows| Some(sum + rows?)),
        }
    };
    let mut out = Vec::new();
    let mut ended_ms = into.align(since_ms) + width;
    let last = into.align(now_ms) + width;
    while ended_ms <= last {
        if let Some(band) = band_of(rows, ended_ms, width, reach, share) {
            let seconds = window_ms as f64 / 1000.0;
            out.push(ExpectedPoint {
                from_millis: ended_ms - width,
                to_millis: ended_ms,
                low: band.low / seconds,
                high: band.high / seconds,
            });
        }
        ended_ms += width;
    }
    Ok(out)
}

/// The `q` quantile of `sorted`, linear between the two nearest samples.
fn quantile(sorted: &[f64], q: f64) -> f64 {
    let at = q.clamp(0.0, 1.0) * (sorted.len() - 1) as f64;
    let (below, above) = (at.floor() as usize, at.ceil() as usize);
    sorted[below] + (sorted[above] - sorted[below]) * (at - below as f64)
}

/// The band, in percent of past windows: what the operator set, or the default. A stored value that is no longer
/// allowed is reported rather than replaced.
pub fn band_percent(store: &dyn Store) -> Result<f64, String> {
    match store.setting(BAND_KEY).map_err(|e| e.to_string())? {
        None => Ok(DEFAULT_BAND_PERCENT),
        Some(text) => checked(
            text.parse()
                .map_err(|_| format!("{BAND_KEY} holds {text:?}, which is not a percent"))?,
        ),
    }
}

fn checked(percent: f64) -> Result<f64, String> {
    if !(MIN_BAND_PERCENT..=MAX_BAND_PERCENT).contains(&percent) {
        return Err(format!(
            "a source's normal holds between {MIN_BAND_PERCENT}% and {MAX_BAND_PERCENT}% of past windows, not {percent}%"
        ));
    }
    Ok(percent)
}

/// Every source's last window against its normal, off the async runtime. A band setting that cannot be read is
/// logged and the default used: the warnings it shapes are still worth having.
///
/// Without `wait`, a caller holding an earlier answer gets that one at once and the new one is computed behind it:
/// what a heartbeat asks, which must not wait on a week of history being read.
pub async fn readings(state: &AppState, now_ms: i64, heartbeat_interval: Duration, wait: bool) -> Arc<Vec<Reading>> {
    let read = run_store(state.store.clone(), |s| Ok(band_percent(s))).await;
    let percent = match read.unwrap_or_else(|e| Err(e.to_string())) {
        Ok(percent) => percent,
        Err(e) => {
            tracing::warn!(target: "throughput.band_unreadable", error = %e, "using the default band");
            DEFAULT_BAND_PERCENT
        }
    };
    let share = percent / 100.0;
    let cache = state.throughput.clone();
    if let Some(fresh) = cache.fresh(ended(now_ms, heartbeat_interval), share) {
        return fresh;
    }
    let registry = state.registry.clone();
    let behind = cache.clone();
    let job = tokio::task::spawn_blocking(move || match registry.series() {
        Some(store) => behind.at(store, now_ms, heartbeat_interval, share),
        None => Arc::default(),
    });
    if !wait && let Some(last) = cache.last() {
        return last;
    }
    job.await.unwrap_or_default()
}

/// `GET` and `PUT /api/settings/warnings`: the band a source's normal holds, the bounds the form may offer, and the
/// default it marks a change from.
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WarningSettingsDto {
    source_rows_band_percent: f64,
    default_percent: f64,
    min_percent: f64,
    max_percent: f64,
}

#[derive(Default, serde::Serialize)]
pub struct WarningSettings {
    settings: Option<WarningSettingsDto>,
}

impl WarningSettings {
    fn answer(percent: Result<f64, Refusal>) -> Envelope<WarningSettings> {
        match percent {
            Ok(percent) => Envelope::ok(WarningSettings {
                settings: Some(WarningSettingsDto {
                    source_rows_band_percent: percent,
                    default_percent: DEFAULT_BAND_PERCENT,
                    min_percent: MIN_BAND_PERCENT,
                    max_percent: MAX_BAND_PERCENT,
                }),
            }),
            Err(refusal) => Envelope::refused(refusal),
        }
    }
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PutWarningSettings {
    source_rows_band_percent: f64,
}

/// `GET /api/settings/warnings`. A stored band the bounds no longer allow is answered with the error and the band
/// itself, so the form shows what to correct.
pub async fn get_settings(State(state): State<AppState>) -> Envelope<WarningSettings> {
    let read = run_store(state.store, |s| Ok(band_percent(s).map_err(|e| (e, stored_percent(s))))).await;
    match read {
        Err(e) => Envelope::refused(e),
        Ok(Ok(percent)) => WarningSettings::answer(Ok(percent)),
        Ok(Err((error, stored))) => {
            let answer = WarningSettings::answer(Ok(stored.unwrap_or(DEFAULT_BAND_PERCENT)));
            Envelope::with(answer.body, StatusCode::UNPROCESSABLE_ENTITY, vec![error.into()])
        }
    }
}

/// The band as stored, if it is a number at all.
fn stored_percent(store: &dyn Store) -> Option<f64> {
    store.setting(BAND_KEY).ok().flatten()?.parse().ok()
}

/// `PUT /api/settings/warnings`. Recorded with who set it, in the transaction that sets it.
pub async fn put_settings(
    State(state): State<AppState>,
    actor: Option<axum::Extension<Actor>>,
    Json(request): Json<PutWarningSettings>,
) -> Envelope<WarningSettings> {
    let actor = actor.map(|a| a.0).unwrap_or_else(Actor::system);
    let percent = match checked(request.source_rows_band_percent) {
        Ok(percent) => percent,
        Err(e) => return WarningSettings::answer(Err(Refusal::invalid(e))),
    };
    let event = super::accounts::event_for(&actor, "warning-settings-change", &format!("{percent}%"), "ok");
    let written = run_store(state.store, move |s| {
        s.put_settings(&[(BAND_KEY, percent.to_string())], Some(&event))
    })
    .await;
    WarningSettings::answer(written.map(|()| percent).map_err(Refusal::from))
}

#[cfg(test)]
mod tests {
    use super::*;

    const FIVE: i64 = 5 * 60 * 1000;
    /// A five-minute boundary well past the epoch, so a week of history before it is positive time.
    const END: i64 = 400 * DAY_MS;

    /// `rows(day, bucket)` read in each five-minute bucket of the `days` days before `END` and the hour after the last
    /// of them, keyed as the registry keys rows read.
    fn store_with(days: i64, rows: impl Fn(i64) -> Option<f64>) -> RegistryStore {
        let store = RegistryStore::open(std::path::Path::new(":memory:")).unwrap();
        let key = serde_json::to_string(&[("group", "g"), ("source", "s"), ("worker", "w")]).unwrap();
        let mut at = END - days * DAY_MS - 2 * WINDOW.as_millis() as i64;
        while at < END {
            if let Some(n) = rows(at) {
                store.record_delta(ROWS_IN_METRIC, &key, at, n).unwrap();
            }
            at += FIVE;
        }
        store
    }

    fn reading(store: &RegistryStore, share: f64) -> Option<Reading> {
        compute(store, END, share).into_iter().next()
    }

    #[test]
    fn a_steady_source_is_within_its_band() {
        let store = store_with(7, |_| Some(100.0));
        let r = reading(&store, 0.99).unwrap();
        assert_eq!(r.rows, 600.0);
        assert_eq!(
            r.band,
            Band {
                low: 600.0,
                high: 600.0
            }
        );
        assert_eq!(r.outside(), None);
    }

    #[test]
    fn a_source_that_stops_is_below_its_band() {
        let store = store_with(7, |at| {
            Some(if at >= END - WINDOW.as_millis() as i64 {
                0.0
            } else {
                100.0
            })
        });
        let r = reading(&store, 0.99).unwrap();
        assert_eq!(r.rows, 0.0);
        assert_eq!(r.outside(), Some(Outside::Below));
    }

    #[test]
    fn a_burst_is_above_its_band() {
        let store = store_with(7, |at| Some(if at >= END - FIVE { 10_000.0 } else { 100.0 }));
        assert_eq!(reading(&store, 0.99).unwrap().outside(), Some(Outside::Above));
    }

    #[test]
    fn a_source_quiet_at_night_is_not_below_at_night() {
        // Nothing read in the six hours before END on any day: that is this source's normal at this time.
        let store = store_with(7, |at| {
            Some(if at.rem_euclid(DAY_MS) >= DAY_MS - 6 * 3_600_000 {
                0.0
            } else {
                100.0
            })
        });
        let r = reading(&store, 0.99).unwrap();
        assert_eq!(r.rows, 0.0);
        assert_eq!(r.outside(), None);
    }

    #[test]
    fn too_few_days_are_not_judged() {
        let store = store_with(MIN_DAYS as i64 - 1, |_| Some(100.0));
        assert_eq!(reading(&store, 0.99), None);
        let store = store_with(MIN_DAYS as i64, |_| Some(100.0));
        assert!(reading(&store, 0.99).is_some());
    }

    #[test]
    fn the_oldest_day_still_counts_once_the_series_has_aged() {
        // Only the three oldest days, so the reading needs the seventh.
        let store = store_with(7, |at| {
            (at >= END - 2 * WINDOW.as_millis() as i64 || at < END - 4 * DAY_MS - 2 * 60 * 60_000).then_some(100.0)
        });
        // A bucket after the window ends: the leader judges a window once every heartbeat in it has landed.
        store.maintain(END + FIVE).unwrap();
        assert!(reading(&store, 0.99).is_some());
    }

    #[test]
    fn a_window_with_a_bucket_nobody_reported_is_not_judged() {
        let store = store_with(7, |at| (at != END - 2 * FIVE).then_some(100.0));
        assert_eq!(reading(&store, 0.99), None);
    }

    #[test]
    fn the_band_is_held_to_its_bounds() {
        assert!(checked(MIN_BAND_PERCENT).is_ok());
        assert!(checked(MAX_BAND_PERCENT).is_ok());
        assert!(checked(MIN_BAND_PERCENT - 0.1).is_err());
        assert!(checked(MAX_BAND_PERCENT + 0.01).is_err());
        assert!(checked(f64::NAN).is_err());
        assert!((MIN_BAND_PERCENT..=MAX_BAND_PERCENT).contains(&DEFAULT_BAND_PERCENT));
    }

    #[test]
    fn a_stored_band_out_of_bounds_is_shown_with_its_error() {
        let dir = std::env::temp_dir().join(format!("smeltr-band-{}", std::process::id()));
        let store =
            crate::leader::sqlite_store::SqliteStore::open(std::path::Path::new(":memory:"), &dir.join("k")).unwrap();
        store.put_settings(&[(BAND_KEY, "40".to_string())], None).unwrap();
        assert!(band_percent(&store).is_err());
        assert_eq!(stored_percent(&store), Some(40.0));
    }

    #[test]
    fn rows_are_grouped_by_thousands() {
        assert_eq!(grouped(0.0), "0");
        assert_eq!(grouped(999.4), "999");
        assert_eq!(grouped(1800.0), "1,800");
        assert_eq!(grouped(864_000.0), "864,000");
        assert_eq!(grouped(1_234_567.0), "1,234,567");
    }

    #[test]
    fn the_band_is_the_share_asked_for() {
        let sorted: Vec<f64> = (0..=100).map(f64::from).collect();
        assert_eq!(quantile(&sorted, 0.005), 0.5);
        assert_eq!(quantile(&sorted, 0.995), 99.5);
        assert_eq!(quantile(&sorted, 0.5), 50.0);
    }

    #[test]
    fn a_reading_is_reused_until_the_next_bucket() {
        let store = store_with(7, |_| Some(100.0));
        let readings = Readings::default();
        let first = readings.at(&store, END + 60_000, Duration::from_secs(1), 0.99);
        let again = readings.at(&store, END + 2 * 60_000, Duration::from_secs(1), 0.99);
        assert!(Arc::ptr_eq(&first, &again));
        let other_share = readings.at(&store, END + 2 * 60_000, Duration::from_secs(1), 0.9);
        assert!(!Arc::ptr_eq(&first, &other_share));
    }
    const HOUR: i64 = 60 * 60 * 1000;

    fn expected(store: &RegistryStore, since_ms: i64) -> Vec<ExpectedPoint> {
        expected_over(store, "g", "s", since_ms, END + 1000, 0.99).unwrap()
    }

    #[test]
    fn a_short_chart_has_the_warnings_band_in_each_five_minutes() {
        let store = store_with(7, |_| Some(100.0));
        let points = expected(&store, END - HOUR);
        assert_eq!(points.len(), 13, "the hour's twelve buckets and the one still filling");
        assert_eq!(points[0].from_millis, END - HOUR);
        assert_eq!((points[12].from_millis, points[12].to_millis), (END, END + FIVE));
        for p in &points {
            assert_eq!((p.low, p.high), (600.0 / 1800.0, 600.0 / 1800.0));
        }
    }

    /// The first days of a week's chart have fewer than [`MIN_DAYS`] days of a week's history before them.
    #[test]
    fn a_long_chart_has_an_hourly_band_and_none_where_history_is_short() {
        let store = store_with(7, |_| Some(100.0));
        let points = expected(&store, END - 7 * DAY_MS);
        assert_eq!(points.first().unwrap().from_millis, END - 4 * DAY_MS - 2 * HOUR);
        assert_eq!(points.last().unwrap().from_millis, END);
        assert!(points.windows(2).all(|w| w[0].to_millis == w[1].from_millis));
        for p in &points {
            assert_eq!((p.low, p.high), (1200.0 / 3600.0, 1200.0 / 3600.0));
        }
    }

    /// A restart that cut one hour short leaves that hour out of the band rather than pulling it down every day.
    #[test]
    fn an_hour_not_read_throughout_is_left_out_of_the_band() {
        let cut = END - 6 * DAY_MS - 12 * HOUR;
        let store = store_with(7, |at| (at < cut + 2 * FIVE || at >= cut + HOUR).then_some(100.0));
        let points = expected(&store, END - 7 * DAY_MS);
        assert!(!points.is_empty());
        for p in &points {
            assert_eq!(
                (p.low, p.high),
                (1200.0 / 3600.0, 1200.0 / 3600.0),
                "at {}",
                p.from_millis
            );
        }
    }

    /// An hour old enough to be hourly no longer says how much of it was read, so a source's first one, begun partway
    /// through, is left out by having no hour read before it.
    #[test]
    fn a_first_hour_already_hourly_is_left_out_of_the_band() {
        let began = END - 10 * DAY_MS - HOUR + 10 * FIVE;
        let store = store_with(10, |at| (at >= began).then_some(100.0));
        store.maintain(END).unwrap();
        let points = expected(&store, END - 7 * DAY_MS);
        assert!(!points.is_empty());
        for p in &points {
            assert_eq!(
                (p.low, p.high),
                (1200.0 / 3600.0, 1200.0 / 3600.0),
                "at {}",
                p.from_millis
            );
        }
    }

    #[test]
    fn a_source_never_read_has_no_band() {
        let store = store_with(7, |_| Some(100.0));
        let none = expected_over(&store, "g", "other", END - HOUR, END + 1000, 0.99);
        assert_eq!(none.unwrap(), Vec::new());
    }
}
