//! What each source normally reads, and whether it is reading that now.
//!
//! A source's normal is learned from the rows it read, which the registry already keeps (`series.rs`), and it is
//! judged at each of four [`WIDTHS`]: the rows of the last 5 minutes, 30 minutes, 6 hours and day. At one width, the
//! typical window of a time of day is the median of the windows that ended within [`NEIGHBORHOOD`] of it on each of
//! the last [`PROFILE_DAYS`] days. Every window of the width's history is a ratio to the typical window of its time
//! of day, and the band is the typical window scaled by the lowest and the highest ratios, a [`TAIL`] of them cut
//! from each end, and widened by [`MARGIN`]. Ratios, because a quiet hour and a busy one can be pooled only once
//! each is set against its own time of day; a quantile, because rows read are bursty and often zero, which a mean
//! and a deviation do not describe.
//!
//! A width judges a source only where its typical window holds at least [`GATE`] rows: a window that is usually near
//! empty has a low edge of zero, which no reading is below. So a source that reads every few seconds is judged at
//! every width, and one that reads a few rows an hour only at the long ones.
//!
//! A source is below normal while some width has read below its band [`WAIT`] times in a row, since one reading at
//! the band's edge flickers. Once below, it stays below until a width judges it and none has: a source silent long
//! enough to lose every band is still silent. Above normal is judged at [`WINDOW`] alone, and only informs.
//!
//! Only windows the source was running for throughout count. A bucket with no point means no worker reported the
//! source then -- not deployed, or a worker gone -- which says nothing about how much it reads.
//!
//! Nor does a past window that overlaps a stop or a below-normal warning about the source (`warnings::spans`), nor
//! the readings it waited for. An outage at this time of day would otherwise set the band's low edge there, and a
//! source that dropped would come to call the drop normal. More rows are learned: they are the new normal until
//! shown otherwise.
//!
//! A chart draws the band behind its points, and a point finer than a window swings wider than a window does. So
//! each [`BUCKET`] also records how far each source's rate swung inside it at each of [`SWING_WIDTHS`]
//! (`series::Swing`), while the ten-second tier still holds what it read. A chart's band at that width is learned
//! from those swings.
//!
//! Neither answer is stored. The normal is learned again each hour, since it moves slowly, and each reading once per
//! [`BUCKET`] against it, since nothing it reads changes in between.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::series::{RegistryStore, SubjectPoints, Swing, Tier};
use super::store::Store;
use super::warnings::{self, Condition, SubjectKind, label};
use super::{AppState, ROWS_IN_METRIC, marks, run_store};

/// The width above normal is judged at, and the one a source's rate is shown at where no other is called for.
pub const WINDOW: Duration = Duration::from_secs(30 * 60);

/// The resolution history is judged at: the finest every tier the history spans can be read at.
pub(super) const BUCKET: Tier = Tier::FiveMinutes;

/// One width a source is judged at: its window, and how many days of past windows its band is learned from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Width {
    pub window: Duration,
    history_days: i64,
}

impl Width {
    fn ms(self) -> i64 {
        self.window.as_millis() as i64
    }

    fn buckets(self) -> i64 {
        self.ms() / BUCKET.bucket_ms()
    }
}

/// Narrowest first. The short widths learn from a week, which holds two thousand windows of each. A week holds only
/// seven days that do not overlap, too few to have a lowest one in a thousand, so the long widths learn from four.
pub const WIDTHS: [Width; 4] = [
    Width {
        window: Duration::from_secs(5 * 60),
        history_days: 7,
    },
    Width {
        window: WINDOW,
        history_days: 7,
    },
    Width {
        window: Duration::from_secs(6 * 60 * 60),
        history_days: 28,
    },
    Width {
        window: Duration::from_secs(24 * 60 * 60),
        history_days: 28,
    },
];

/// How many days back a time of day's typical window is learned from.
const PROFILE_DAYS: i64 = 7;

/// How far either side of the same time of day a past window may end and still count as the same time of day.
const NEIGHBORHOOD: Duration = Duration::from_secs(30 * 60);

/// Days with a window at a time of day the source ran for throughout, before it is judged at that time of day.
/// Fewer, and one odd day is most of what it knows.
pub const MIN_DAYS: usize = 3;

/// The share of past ratios each side of the band leaves out: one in two thousand.
const TAIL: f64 = 0.0005;

/// How much wider than the ratios the band is, as a share of each edge.
const MARGIN: f64 = 0.3;

/// Readings in a row outside the band before a source is outside its normal.
const WAIT: i64 = 4;

/// The fewest rows a width's typical window holds for that width to judge a source.
const GATE: f64 = 10.0;

const DAY_MS: i64 = 24 * 60 * 60 * 1000;

/// How many rows one source normally reads in one window at this time of day.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Band {
    pub low: f64,
    pub high: f64,
}

/// What a normal is learned with: the sources an admin said to learn again from a time on ([`marks::Reset`]).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Learning {
    pub reset: Vec<marks::Reset>,
}

impl Learning {
    /// The marks as stored.
    pub fn read(store: &Store) -> Learning {
        Learning {
            reset: marks::Marks::read(store).reset,
        }
    }

    /// When `group`'s `source` was last said to learn again from, if it was.
    fn reset_from(&self, group: &str, source: &str) -> Option<i64> {
        self.reset
            .iter()
            .filter(|r| r.group == group && r.source == source)
            .map(|r| r.from_millis)
            .max()
    }
}

/// One source's reading at the end of one bucket.
#[derive(Debug, Clone, PartialEq)]
pub struct Reading {
    pub group: String,
    pub source: String,
    pub ended_ms: i64,
    /// Each width that judges the source now, narrowest first.
    pub judged: Vec<Judged>,
    pub outside: Option<Outside>,
}

/// One width's reading: the rows of its window that ended with the reading, and its band.
#[derive(Debug, Clone, PartialEq)]
pub struct Judged {
    pub window: Duration,
    pub rows: f64,
    pub band: Band,
    /// This reading and the [`WAIT`] - 1 before it were each below their band.
    pub below: bool,
    /// Each above.
    pub above: bool,
}

impl Judged {
    /// The rows and band as rows per second.
    pub fn per_second(&self) -> (f64, Band) {
        let seconds = self.window.as_secs_f64();
        (
            self.rows / seconds,
            Band {
                low: self.band.low / seconds,
                high: self.band.high / seconds,
            },
        )
    }
}

/// Which side of its normal a source is on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Outside {
    Below,
    Above,
}

impl Reading {
    /// The width a reader is shown: the narrowest that put the source outside its normal, else [`WINDOW`], else the
    /// narrowest that judges it. None while none judges it.
    pub fn shown(&self) -> Option<&Judged> {
        let side = |j: &&Judged| match self.outside {
            Some(Outside::Below) => j.below,
            Some(Outside::Above) => j.above && j.window == WINDOW,
            None => false,
        };
        self.judged
            .iter()
            .find(side)
            .or_else(|| self.judged.iter().find(|j| j.window == WINDOW))
            .or_else(|| self.judged.first())
    }
}

/// Which side of its normal a source is on, from its widths' readings, and whether a below-normal warning about it
/// held until now.
fn outside(judged: &[Judged], below_held: bool) -> Option<Outside> {
    if judged.iter().any(|j| j.below) || (below_held && judged.is_empty()) {
        Some(Outside::Below)
    } else if judged.iter().any(|j| j.above && j.window == WINDOW) {
        Some(Outside::Above)
    } else {
        None
    }
}

/// The last answer, what it was computed for, and the normal it was judged against. A new five-minute bucket or a
/// new mark is a new question, and a new hour or a new mark a new normal. One caller computes a new answer at a
/// time; the rest read the last meanwhile.
#[derive(Default)]
pub struct Readings {
    last: Mutex<Option<Answer>>,
    normals: Mutex<Option<(i64, Learning, Arc<Normals>)>>,
    computing: Mutex<()>,
}

type Answer = (i64, Learning, Arc<Vec<Reading>>);

impl Readings {
    /// Every source with enough history to judge, as of `now_ms`. Waits for every heartbeat sent before
    /// `now_ms - heartbeat_interval` to land in the window it judges.
    ///
    /// While another caller computes the new answer, this returns the last one rather than wait for it. Only a
    /// caller with no answer at all to fall back on waits.
    pub fn at(
        &self,
        store: &RegistryStore,
        now_ms: i64,
        heartbeat_interval: Duration,
        learning: &Learning,
    ) -> Arc<Vec<Reading>> {
        let ended_ms = ended(now_ms, heartbeat_interval);
        if let Some(fresh) = self.fresh(ended_ms, learning) {
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
        if let Some(fresh) = self.fresh(ended_ms, learning) {
            return fresh;
        }
        record_swings(store, ended_ms, heartbeat_interval);
        let normals = self.normals_at(store, ended_ms, learning);
        let readings = Arc::new(compute(store, ended_ms, learning, &normals));
        *self.last.lock().unwrap_or_else(|e| e.into_inner()) = Some((ended_ms, learning.clone(), readings.clone()));
        readings
    }

    /// The answer for the window ending at `ended_ms` learned with `learning`, if that is the one held.
    fn fresh(&self, ended_ms: i64, learning: &Learning) -> Option<Arc<Vec<Reading>>> {
        match self.last.lock().unwrap_or_else(|e| e.into_inner()).as_ref() {
            Some((at, held, readings)) if *at == ended_ms && held == learning => Some(readings.clone()),
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

    /// Every source's normal as of the hour `ended_ms` is in, learned again only when that hour or `learning` is new.
    fn normals_at(&self, store: &RegistryStore, ended_ms: i64, learning: &Learning) -> Arc<Normals> {
        let as_of = Tier::OneHour.align(ended_ms);
        let mut held = self.normals.lock().unwrap_or_else(|e| e.into_inner());
        if let Some((at, with, normals)) = held.as_ref()
            && *at == as_of
            && with == learning
        {
            return normals.clone();
        }
        let normals = Arc::new(normals(store, as_of, learning));
        *held = Some((as_of, learning.clone(), normals.clone()));
        normals
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

/// One width's normal as of one time: the typical window at each time of day, and the lowest and highest ratios to
/// it.
#[derive(Debug, Clone, PartialEq)]
struct Normal {
    width: Width,
    /// By the bucket of the day a window ends at. None where too few days had one.
    typical: Vec<Option<f64>>,
    low: f64,
    high: f64,
}

impl Normal {
    /// The band of the window ending at `ended_ms`, or None where this width does not judge the source.
    fn band(&self, ended_ms: i64) -> Option<Band> {
        let typical = self.typical[time_of_day(ended_ms)].filter(|&t| t >= GATE)?;
        Some(Band {
            low: (1.0 - MARGIN) * ((typical + 1.0) * self.low - 1.0).max(0.0),
            high: (1.0 + MARGIN) * ((typical + 1.0) * self.high - 1.0),
        })
    }
}

/// Which bucket of its day the window ending at `ended_ms` ends at.
fn time_of_day(ended_ms: i64) -> usize {
    (ended_ms.rem_euclid(DAY_MS) / BUCKET.bucket_ms()) as usize
}

/// `width`'s normal as of `as_of`, from the windows learned that ended before it, or None while it has no ratio at all. A
/// window's typical window is that of its time of day as of `as_of`, so every window of the history is set against
/// the same week (+1 each side, so a time of day that is usually empty divides like any other).
fn normal(rows: &Rows, warned: &Warned, as_of: i64, width: Width) -> Option<Normal> {
    let step = BUCKET.bucket_ms();
    let per_day = DAY_MS / step;
    let learned = |ended: i64| rows.window(ended, width).filter(|_| !warned.during(ended, width.ms()));
    // The last readings before `as_of` may yet be waited on by a warning, which would not learn them.
    let mut by_time: Vec<Vec<(i64, f64)>> = vec![Vec::new(); per_day as usize];
    for i in WAIT..=PROFILE_DAYS * per_day {
        let ended = as_of - i * step;
        if let Some(sum) = learned(ended) {
            by_time[time_of_day(ended)].push((i / per_day, sum));
        }
    }
    let reach = NEIGHBORHOOD.as_millis() as i64 / step;
    let typical: Vec<Option<f64>> = (0..per_day)
        .map(|at| {
            let near: Vec<(i64, f64)> = (-reach..=reach)
                .flat_map(|off| by_time[(at + off).rem_euclid(per_day) as usize].iter().copied())
                .collect();
            if near.iter().map(|&(day, _)| day).collect::<HashSet<_>>().len() < MIN_DAYS {
                return None;
            }
            let mut sums: Vec<f64> = near.into_iter().map(|(_, sum)| sum).collect();
            sums.sort_by(f64::total_cmp);
            Some(quantile(&sums, 0.5))
        })
        .collect();
    let mut ratios: Vec<f64> = (WAIT..=width.history_days * per_day)
        .filter_map(|i| {
            let ended = as_of - i * step;
            let typical = typical[time_of_day(ended)]?;
            learned(ended).map(|sum| (sum + 1.0) / (typical + 1.0))
        })
        .collect();
    if ratios.is_empty() {
        return None;
    }
    ratios.sort_by(f64::total_cmp);
    Some(Normal {
        width,
        low: quantile(&ratios, TAIL),
        high: quantile(&ratios, 1.0 - TAIL),
        typical,
    })
}

/// Each source's normal at each width that has one.
type Normals = HashMap<(String, String), Vec<Normal>>;

/// How far back a normal reads: the longest history, and the window that ends where it begins.
fn reach_ms() -> i64 {
    WIDTHS
        .iter()
        .map(|w| w.history_days.max(PROFILE_DAYS) * DAY_MS + w.ms())
        .max()
        .unwrap_or_default()
}

/// Every source's normal as of `as_of`.
fn normals(store: &RegistryStore, as_of: i64, learning: &Learning) -> Normals {
    let oldest = as_of - reach_ms();
    let buckets = match history(store, BUCKET.and_finer(), BUCKET, oldest) {
        Ok(buckets) => buckets,
        Err(e) => {
            tracing::warn!(
                name: "series_unreadable", target: "status",
                error = %e,
                "could not read rows read, so no source's normal is learned this hour"
            );
            return Normals::new();
        }
    };
    buckets
        .into_iter()
        .map(|((group, source), of)| {
            let rows = Rows::of(&of, oldest, as_of);
            let warned = Warned::of(store, &group, &source, oldest, learning);
            let normals = WIDTHS
                .iter()
                .filter_map(|&width| normal(&rows, &warned, as_of, width))
                .collect();
            ((group, source), normals)
        })
        .collect()
}

/// [`Readings::at`] without the cache: each source's reading at `ended_ms` against `normals`.
fn compute(store: &RegistryStore, ended_ms: i64, learning: &Learning, normals: &Normals) -> Vec<Reading> {
    let widest = WIDTHS.iter().map(|w| w.ms()).max().unwrap_or_default();
    let oldest = ended_ms - (WAIT - 1) * BUCKET.bucket_ms() - widest;
    let buckets = match history(store, BUCKET.and_finer(), BUCKET, oldest) {
        Ok(buckets) => buckets,
        Err(e) => {
            tracing::warn!(
                name: "series_unreadable", target: "status",
                error = %e,
                "could not read rows read, so no source is judged against its normal this pass"
            );
            return Vec::new();
        }
    };
    let mut out: Vec<Reading> = buckets
        .into_iter()
        .filter_map(|(key, of)| {
            let rows = Rows::of(&of, oldest, ended_ms);
            let judged: Vec<Judged> = normals
                .get(&key)
                .into_iter()
                .flatten()
                .filter_map(|normal| judge(&rows, normal, ended_ms))
                .collect();
            let (group, source) = key;
            let below_held = Warned::of(store, &group, &source, oldest, learning).below_held;
            let outside = outside(&judged, below_held);
            (!judged.is_empty() || outside.is_some()).then_some(Reading {
                group,
                source,
                ended_ms,
                judged,
                outside,
            })
        })
        .collect();
    out.sort_by(|a, b| (&a.group, &a.source).cmp(&(&b.group, &b.source)));
    out
}

/// The reading at `normal`'s width of the window ending at `ended_ms`, or None where the width does not judge it.
fn judge(rows: &Rows, normal: &Normal, ended_ms: i64) -> Option<Judged> {
    let read = |ended: i64| Some((rows.window(ended, normal.width)?, normal.band(ended)?));
    let (now, band) = read(ended_ms)?;
    let each = |past: fn(f64, Band) -> bool| {
        (0..WAIT).all(|i| read(ended_ms - i * BUCKET.bucket_ms()).is_some_and(|(rows, band)| past(rows, band)))
    };
    Some(Judged {
        window: normal.width.window,
        rows: now,
        band,
        below: each(|rows, band| rows < band.low),
        above: each(|rows, band| rows > band.high),
    })
}

/// One source's rows by bucket, from `start_ms`, summed so that any window is two lookups.
struct Rows {
    start_ms: i64,
    /// The rows of the buckets before each, and how many of them have no point.
    before: Vec<(f64, u32)>,
}

impl Rows {
    /// The buckets of `buckets` from `start_ms` until `end_ms`.
    fn of(buckets: &BTreeMap<i64, f64>, start_ms: i64, end_ms: i64) -> Rows {
        let step = BUCKET.bucket_ms();
        let mut before = vec![(0.0, 0)];
        let (mut rows, mut missing) = (0.0, 0);
        let mut at = start_ms;
        while at < end_ms {
            match buckets.get(&at) {
                Some(n) => rows += n,
                None => missing += 1,
            }
            before.push((rows, missing));
            at += step;
        }
        Rows { start_ms, before }
    }

    /// Rows read over `width` ending at `ended_ms`, or None if any of its buckets has no point or is not held.
    fn window(&self, ended_ms: i64, width: Width) -> Option<f64> {
        let end = usize::try_from((ended_ms - self.start_ms).div_euclid(BUCKET.bucket_ms())).ok()?;
        let start = end.checked_sub(width.buckets() as usize)?;
        let (&(after, missing_after), &(before, missing_before)) = (self.before.get(end)?, self.before.get(start)?);
        (missing_after == missing_before).then_some(after - before)
    }
}

/// When one source's rows teach its normal nothing, as (started, ended), with no end for a span still open: while a
/// stop or a below-normal warning about it held, from the first reading that warning waited for, and all of the time
/// before an admin said to learn it again.
struct Warned {
    spans: Vec<(i64, Option<i64>)>,
    /// A below-normal warning about it still holds.
    below_held: bool,
}

impl Warned {
    /// `group`'s `source`'s spans since `since_ms`. Warnings the store cannot read are logged and left out: the
    /// normal is then learned from those windows too.
    fn of(store: &RegistryStore, group: &str, source: &str, since_ms: i64, learning: &Learning) -> Warned {
        let held = warnings::spans(store, SubjectKind::Source, group, source, since_ms).unwrap_or_else(|e| {
            tracing::warn!(
                name: "warned_unreadable", target: "status",
                error = %e, group, source,
                "could not read when this source warned, so its normal is learned from every window this pass"
            );
            Vec::new()
        });
        let waited = (WAIT - 1) * BUCKET.bucket_ms();
        let mut spans: Vec<(i64, Option<i64>)> = held
            .iter()
            .filter_map(|s| match s.condition {
                Condition::SourceRowsAbove => None,
                Condition::SourceRowsBelow => Some((s.started_millis - waited, s.ended_millis)),
                _ => Some((s.started_millis, s.ended_millis)),
            })
            .collect();
        if let Some(from) = learning.reset_from(group, source) {
            spans.push((i64::MIN, Some(from)));
        }
        Warned {
            spans,
            below_held: held
                .iter()
                .any(|s| s.condition == Condition::SourceRowsBelow && s.ended_millis.is_none()),
        }
    }

    /// Whether the `width_ms` ending at `ended_ms` overlaps a span.
    fn during(&self, ended_ms: i64, width_ms: i64) -> bool {
        self.spans
            .iter()
            .any(|&(started, ended)| ended_ms > started && ended.is_none_or(|ended| ended_ms - width_ms < ended))
    }
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

/// `group`'s `source`'s band, as rows per second, for each bucket from `since_ms` to the one `now_ms` is in, for a
/// chart whose points are `width_ms` wide. Up to a day back the bucket is five minutes, and further back an hour.
/// A bucket's band is the one judged at the width nearest it, as of its hour: for five minutes, five minutes; for an
/// hour, [`WINDOW`]. For a point finer than five minutes, it is the source's swings at that width
/// ([`record_swings`]). A bucket where that width does not judge the source has none. The band reads only past
/// days, so the bucket still filling has one too.
pub fn expected_over(
    store: &RegistryStore,
    group: &str,
    source: &str,
    since_ms: i64,
    now_ms: i64,
    width_ms: i64,
    learning: &Learning,
) -> Result<Vec<ExpectedPoint>, super::store::StoreError> {
    let hourly = now_ms - since_ms > DAY_MS;
    if !hourly && width_ms < BUCKET.bucket_ms() {
        return swung_over(store, group, source, since_ms, now_ms, width_ms, learning);
    }
    let (point, width) = if hourly {
        (Tier::OneHour, WIDTHS[1])
    } else {
        (BUCKET, WIDTHS[0])
    };
    let step = point.bucket_ms();
    let first = point.align(since_ms) + step;
    let last = point.align(now_ms) + step;
    let oldest = Tier::OneHour.align(first) - width.history_days.max(PROFILE_DAYS) * DAY_MS - width.ms();
    let Some(of) = history(store, BUCKET.and_finer(), BUCKET, oldest)?.remove(&(group.to_string(), source.to_string()))
    else {
        return Ok(Vec::new());
    };
    let rows = Rows::of(&of, oldest, last);
    let warned = Warned::of(store, group, source, oldest, learning);
    let mut normals: HashMap<i64, Option<Normal>> = HashMap::new();
    let mut out = Vec::new();
    let mut ended_ms = first;
    while ended_ms <= last {
        let as_of = Tier::OneHour.align(ended_ms);
        let normal = normals
            .entry(as_of)
            .or_insert_with(|| normal(&rows, &warned, as_of, width));
        if let Some(band) = normal.as_ref().and_then(|n| n.band(ended_ms)) {
            let seconds = width.window.as_secs_f64();
            out.push(ExpectedPoint {
                from_millis: ended_ms - step,
                to_millis: ended_ms,
                low: band.low / seconds,
                high: band.high / seconds,
            });
        }
        ended_ms += step;
    }
    Ok(out)
}

/// [`expected_over`] for a chart finer than [`BUCKET`]: each bucket's band is how far the source's rate at
/// `width_ms` swung in the buckets near that time of day on past days, a [`TAIL`] of them cut from each end.
fn swung_over(
    store: &RegistryStore,
    group: &str,
    source: &str,
    since_ms: i64,
    now_ms: i64,
    width_ms: i64,
    learning: &Learning,
) -> Result<Vec<ExpectedPoint>, super::store::StoreError> {
    let width = BUCKET.bucket_ms();
    let reach = NEIGHBORHOOD.as_millis() as i64 / width;
    let first = BUCKET.align(since_ms);
    let oldest = first - PROFILE_DAYS * DAY_MS - reach * width;
    let warned = Warned::of(store, group, source, oldest, learning);
    // By the end of the bucket each swung in, as a window is known by its end.
    let swung: BTreeMap<i64, Band> = store
        .swings_since(group, source, width_ms, oldest)?
        .into_iter()
        .map(|s| {
            (
                s.bucket_ms + width,
                Band {
                    low: s.low,
                    high: s.high,
                },
            )
        })
        .collect();
    let mut out = Vec::new();
    let mut ended_ms = first + width;
    while ended_ms <= BUCKET.align(now_ms) + width {
        let held = |ended| swung.get(&ended).copied().filter(|_| !warned.during(ended, width));
        if let Some(band) = swung_band(held, ended_ms, width, reach) {
            out.push(ExpectedPoint {
                from_millis: ended_ms - width,
                to_millis: ended_ms,
                low: band.low,
                high: band.high,
            });
        }
        ended_ms += width;
    }
    Ok(out)
}

/// The band of `held` at `ended_ms` on each past day, `reach` steps of `step_ms` either side, or `None` while fewer
/// than [`MIN_DAYS`] days have one. `held` is one past bucket's swing, its low and high rate ([`swing`]), if it is
/// known. The band cuts a [`TAIL`] from the bottom of the lows and from the top of the highs.
fn swung_band(held: impl Fn(i64) -> Option<Band>, ended_ms: i64, step_ms: i64, reach: i64) -> Option<Band> {
    let (mut lows, mut highs) = (Vec::new(), Vec::new());
    let mut days = HashSet::new();
    for day in 1..=PROFILE_DAYS {
        for step in -reach..=reach {
            if let Some(band) = held(ended_ms - day * DAY_MS + step * step_ms) {
                lows.push(band.low);
                highs.push(band.high);
                days.insert(day);
            }
        }
    }
    if days.len() < MIN_DAYS {
        return None;
    }
    lows.sort_by(f64::total_cmp);
    highs.sort_by(f64::total_cmp);
    Some(Band {
        low: quantile(&lows, TAIL),
        high: quantile(&highs, 1.0 - TAIL),
    })
}

/// The widths a source's swing is kept at: those a chart finer than [`BUCKET`] draws its points at
/// (`Tier::for_window`).
pub const SWING_WIDTHS: [Tier; 2] = [Tier::TenSeconds, Tier::OneMinute];

/// How far back from the window's end a swing is still recorded. The ten-second tier holds an hour; this leaves
/// `maintain` a margin before it rolls the oldest of it up.
const SWING_REACH_MS: i64 = 50 * 60 * 1000;

/// Records each source's swings for the buckets that ended before `ended_ms` and are not held yet, back as far as
/// the ten-second tier still holds. A bucket is recorded once the bucket after it has ended too: whether its
/// source read throughout it is known only from both sides. A failure is logged, and the next pass tries again.
///
/// Rows land in the slot their heartbeat arrived in, so no swing is kept at a width finer than `heartbeat_interval`:
/// most of its slots would be empty, and its low nothing. A heartbeat that arrives after its bucket was recorded,
/// as one held back by a lost connection can, is left out of that bucket's swing.
fn record_swings(store: &RegistryStore, ended_ms: i64, heartbeat_interval: Duration) {
    let width = BUCKET.bucket_ms();
    let reached = BUCKET.align(ended_ms - SWING_REACH_MS);
    let from = match store.last_swing_ms() {
        Ok(last) => last.map_or(reached, |last| (last + width).max(reached)),
        Err(e) => {
            tracing::warn!(name: "swings_unreadable", target: "status", error = %e, "could not read which swings are held, so none are recorded this pass");
            return;
        }
    };
    let until = ended_ms - width;
    if from >= until {
        return;
    }
    let fine = match history(store, &[Tier::TenSeconds], Tier::TenSeconds, from - width) {
        Ok(fine) => fine,
        Err(e) => {
            tracing::warn!(name: "series_unreadable", target: "status", error = %e, "could not read rows read, so no swing is recorded this pass");
            return;
        }
    };
    let swings: Vec<Swing> = fine
        .into_iter()
        .flat_map(|((group, source), slots)| {
            swings_of(&slots, from, until, heartbeat_interval.as_millis() as i64)
                .into_iter()
                .map(move |(width_ms, bucket_ms, low, high)| Swing {
                    group: group.clone(),
                    source: source.clone(),
                    width_ms,
                    bucket_ms,
                    low,
                    high,
                })
        })
        .collect();
    if let Err(e) = store.record_swings(&swings) {
        tracing::warn!(name: "swings_not_recorded", target: "status", error = %e, "could not record how far sources' rates swung, so their charts' bands miss these buckets");
    }
}

/// One source's swings, as (width, bucket start, low, high), for each bucket from `from` to `until`, from its rows
/// in ten-second slots. A bucket counts only when the source read in it and in the buckets either side, as a window
/// counts only when it was read throughout. A slot in it with no point read nothing.
fn swings_of(slots: &BTreeMap<i64, f64>, from: i64, until: i64, finest_ms: i64) -> Vec<(i64, i64, f64, f64)> {
    let width = BUCKET.bucket_ms();
    let ten = Tier::TenSeconds.bucket_ms();
    let read = |start: i64| slots.range(start..start + width).next().is_some();
    let mut out = Vec::new();
    let mut bucket = from;
    while bucket < until {
        if read(bucket - width) && read(bucket) && read(bucket + width) {
            for tier in SWING_WIDTHS.into_iter().filter(|tier| tier.bucket_ms() >= finest_ms) {
                let step = tier.bucket_ms();
                let mut rates: Vec<f64> = (bucket..bucket + width)
                    .step_by(step as usize)
                    .map(|at| {
                        let rows: f64 = (at..at + step)
                            .step_by(ten as usize)
                            .filter_map(|slot| slots.get(&slot))
                            .sum();
                        rows * 1000.0 / step as f64
                    })
                    .collect();
                let (low, high) = swing(&mut rates);
                out.push((step, bucket, low, high));
            }
        }
        bucket += width;
    }
    out
}

/// The share of a bucket's rates its swing spans, cut evenly from both ends: a slot or two that a slow heartbeat or a
/// restart left nearly empty would otherwise set the bucket's low, and with it the band at that time of day all week.
const SWING_SHARE: f64 = 0.8;

/// The low and high of a bucket's `rates` at one width: the [`SWING_SHARE`] of them in between.
pub fn swing(rates: &mut [f64]) -> (f64, f64) {
    rates.sort_by(f64::total_cmp);
    let tail = (1.0 - SWING_SHARE) / 2.0;
    (quantile(rates, tail), quantile(rates, 1.0 - tail))
}

/// The `q` quantile of `sorted`, linear between the two nearest samples.
fn quantile(sorted: &[f64], q: f64) -> f64 {
    let at = q.clamp(0.0, 1.0) * (sorted.len() - 1) as f64;
    let (below, above) = (at.floor() as usize, at.ceil() as usize);
    sorted[below] + (sorted[above] - sorted[below]) * (at - below as f64)
}

/// Every source's last window against its normal, off the async runtime ([`Learning::read`]).
///
/// Without `wait`, a caller holding an earlier answer gets that one at once and the new one is computed behind it:
/// what a heartbeat asks, which must not wait on a week of history being read.
pub async fn readings(state: &AppState, now_ms: i64, heartbeat_interval: Duration, wait: bool) -> Arc<Vec<Reading>> {
    let learning = run_store(state.store.clone(), |s| Ok(Learning::read(s)))
        .await
        .unwrap_or_default();
    let cache = state.throughput.clone();
    if let Some(fresh) = cache.fresh(ended(now_ms, heartbeat_interval), &learning) {
        return fresh;
    }
    let registry = state.registry.clone();
    let behind = cache.clone();
    let job = tokio::task::spawn_blocking(move || match registry.series() {
        Some(store) => behind.at(store, now_ms, heartbeat_interval, &learning),
        None => Arc::default(),
    });
    if !wait && let Some(last) = cache.last() {
        return last;
    }
    job.await.unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    const FIVE: i64 = 5 * 60 * 1000;
    const HOUR: i64 = 60 * 60 * 1000;
    /// A day's boundary well past the epoch, so four weeks of history before it is positive time.
    const END: i64 = 400 * DAY_MS;

    /// `rows(bucket)` read in each five-minute bucket of the `days` days before `END` and the day before them, keyed
    /// as the registry keys rows read.
    fn store_with(days: i64, rows: impl Fn(i64) -> Option<f64>) -> RegistryStore {
        let store = RegistryStore::open(std::path::Path::new(":memory:")).unwrap();
        let key = serde_json::to_string(&[("group", "g"), ("source", "s"), ("worker", "w")]).unwrap();
        let mut at = END - (days + 1) * DAY_MS;
        while at < END {
            if let Some(n) = rows(at) {
                store.record_delta(ROWS_IN_METRIC, &key, at, n).unwrap();
            }
            at += FIVE;
        }
        store
    }

    /// The reading at `ended`, against the normal as of its hour.
    fn reading_at(store: &RegistryStore, ended: i64, learning: &Learning) -> Option<Reading> {
        let normals = normals(store, Tier::OneHour.align(ended), learning);
        compute(store, ended, learning, &normals).into_iter().next()
    }

    fn reading(store: &RegistryStore) -> Option<Reading> {
        reading_at(store, END, &Learning::default())
    }

    fn windows(r: &Reading) -> Vec<u64> {
        r.judged.iter().map(|j| j.window.as_secs() / 60).collect()
    }

    #[test]
    fn a_steady_source_is_judged_within_its_band_at_every_width() {
        let r = reading(&store_with(7, |_| Some(100.0))).unwrap();
        assert_eq!(windows(&r), [5, 30, 360, 1440]);
        let five = &r.judged[0];
        assert_eq!(five.rows, 100.0);
        assert_eq!(five.band, Band { low: 70.0, high: 130.0 });
        assert_eq!(
            r.judged[1].band,
            Band {
                low: 420.0,
                high: 780.0
            }
        );
        assert_eq!(r.outside, None);
        assert_eq!(r.shown().unwrap().window, WINDOW);
    }

    #[test]
    fn a_source_that_stops_is_below_on_its_fourth_reading() {
        let stopped =
            |buckets: i64| store_with(7, move |at| Some(if at >= END - buckets * FIVE { 0.0 } else { 100.0 }));
        assert_eq!(reading(&stopped(3)).unwrap().outside, None, "three readings wait");
        let r = reading(&stopped(4)).unwrap();
        assert_eq!(r.outside, Some(Outside::Below));
        assert_eq!(r.shown().unwrap().window, WIDTHS[0].window);
    }

    #[test]
    fn a_burst_is_above_at_thirty_minutes() {
        let store = store_with(7, |at| Some(if at >= END - 4 * FIVE { 10_000.0 } else { 100.0 }));
        let r = reading(&store).unwrap();
        assert_eq!(r.outside, Some(Outside::Above));
        assert_eq!(r.shown().unwrap().window, WINDOW);
    }

    #[test]
    fn a_source_quiet_at_night_is_judged_only_by_the_long_widths() {
        // Nothing read in the six hours before END on any day: that is this source's normal at this time.
        let store = store_with(7, |at| {
            Some(if at.rem_euclid(DAY_MS) >= DAY_MS - 6 * HOUR {
                0.0
            } else {
                100.0
            })
        });
        let r = reading(&store).unwrap();
        assert_eq!(windows(&r), [360, 1440]);
        assert_eq!(r.outside, None);
    }

    #[test]
    fn a_sparse_source_that_stops_is_below_at_six_hours() {
        // One row a bucket is too few for the short widths. Stopped for the last five hours, the six-hour window
        // holds a sixth of its rows.
        let ended = END - FIVE;
        let store = store_with(29, |at| Some(if at >= END - 5 * HOUR { 0.0 } else { 1.0 }));
        let r = reading_at(&store, ended, &Learning::default()).unwrap();
        assert_eq!(windows(&r), [360, 1440]);
        assert_eq!(r.outside, Some(Outside::Below));
        assert_eq!(r.shown().unwrap().window, WIDTHS[2].window);
    }

    #[test]
    fn too_few_days_are_not_judged() {
        let store = store_with(MIN_DAYS as i64 - 2, |_| Some(100.0));
        assert_eq!(reading(&store), None);
        let store = store_with(MIN_DAYS as i64, |_| Some(100.0));
        assert!(reading(&store).is_some());
    }

    #[test]
    fn a_window_with_a_bucket_nobody_reported_is_not_judged() {
        let store = store_with(7, |at| (at != END - 2 * FIVE).then_some(100.0));
        assert_eq!(windows(&reading(&store).unwrap()), [5]);
        let store = store_with(7, |at| (at != END - FIVE).then_some(100.0));
        assert_eq!(reading(&store), None);
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
    fn a_quantile_lies_between_the_samples_either_side() {
        let sorted: Vec<f64> = (0..=100).map(f64::from).collect();
        assert_eq!(quantile(&sorted, 0.005), 0.5);
        assert_eq!(quantile(&sorted, 0.995), 99.5);
        assert_eq!(quantile(&sorted, 0.5), 50.0);
    }

    #[test]
    fn a_reading_is_reused_until_the_next_bucket() {
        let store = store_with(7, |_| Some(100.0));
        let readings = Readings::default();
        let none = Learning::default();
        let first = readings.at(&store, END + 60_000, Duration::from_secs(1), &none);
        let again = readings.at(&store, END + 2 * 60_000, Duration::from_secs(1), &none);
        assert!(Arc::ptr_eq(&first, &again));
        let reset = reset_at(END - 30 * DAY_MS);
        let other = readings.at(&store, END + 2 * 60_000, Duration::from_secs(1), &reset);
        assert!(!Arc::ptr_eq(&first, &other));
    }

    fn reset_at(from_millis: i64) -> Learning {
        Learning {
            reset: vec![marks::Reset {
                group: "g".into(),
                source: "s".into(),
                from_millis,
                by: "John".into(),
            }],
        }
    }

    #[test]
    fn a_source_whose_history_was_reset_has_no_normal_until_it_has_days_of_what_came_after() {
        let store = store_with(7, |_| Some(100.0));
        assert_eq!(reading_at(&store, END, &reset_at(END - DAY_MS)), None);
        assert!(reading_at(&store, END, &reset_at(END - 4 * DAY_MS)).is_some());
    }

    /// `entry` held for the source from `started` until `ended`, or still.
    fn warned(store: &RegistryStore, entry: &str, started: i64, ended: Option<i64>) {
        let fact = vec![(entry.to_string(), serde_json::to_string(&("g", "s")).unwrap())];
        store.sync_fact_clock(&[], &fact, started).unwrap();
        if let Some(ended) = ended {
            store.sync_fact_clock(&fact, &[], ended).unwrap();
        }
    }

    /// Nothing read for two hours around this time of day three days ago, and nothing in the last 20 minutes.
    fn outage_and_stop() -> (RegistryStore, i64) {
        let outage = END - 3 * DAY_MS - HOUR;
        let store = store_with(7, |at| {
            Some(if (outage..outage + 2 * HOUR).contains(&at) || at >= END - 4 * FIVE {
                0.0
            } else {
                100.0
            })
        });
        (store, outage)
    }

    #[test]
    fn a_stop_does_not_teach_the_band() {
        let (store, outage) = outage_and_stop();
        let r = reading(&store).unwrap();
        assert_eq!(r.judged[0].band.low, 0.0, "learned, the outage sets the low edge");
        assert_eq!(r.outside, None);
        warned(&store, "source_unreachable", outage, Some(outage + 2 * HOUR));
        let r = reading(&store).unwrap();
        assert_eq!(r.judged[0].band.low, 70.0);
        assert_eq!(r.outside, Some(Outside::Below));
    }

    #[test]
    fn a_below_warning_does_not_teach_the_band_nor_do_the_readings_it_waited_for() {
        let (store, outage) = outage_and_stop();
        // Raised on the fourth bucket of the outage: the three before it waited.
        warned(&store, "source_rows_below", outage + 3 * FIVE, Some(outage + 2 * HOUR));
        assert_eq!(reading(&store).unwrap().judged[0].band.low, 70.0);
    }

    #[test]
    fn an_above_warning_teaches_the_band() {
        let burst = END - 3 * DAY_MS - HOUR;
        let store = store_with(7, |at| {
            Some(if (burst..burst + 2 * HOUR).contains(&at) {
                10_000.0
            } else {
                100.0
            })
        });
        let high = |store: &RegistryStore| reading(store).unwrap().judged[0].band.high;
        let learned = high(&store);
        assert!(learned > 130.0);
        warned(&store, "source_rows_above", burst, Some(burst + 2 * HOUR));
        assert_eq!(high(&store), learned);
    }

    #[test]
    fn a_below_warning_holds_while_no_width_judges_the_source() {
        let store = store_with(7, |at| Some(if at >= END - DAY_MS { 0.0 } else { 100.0 }));
        let reset = reset_at(END - DAY_MS);
        assert_eq!(reading_at(&store, END, &reset), None);
        warned(&store, "source_rows_below", END - DAY_MS + 3 * FIVE, None);
        let r = reading_at(&store, END, &reset).unwrap();
        assert_eq!(r.outside, Some(Outside::Below));
        assert_eq!(r.shown(), None);
    }

    #[test]
    fn a_below_warning_ends_on_a_reading_inside_the_band() {
        let store = store_with(7, |_| Some(100.0));
        warned(&store, "source_rows_below", END - HOUR, None);
        assert_eq!(reading(&store).unwrap().outside, None);
    }

    fn expected(store: &RegistryStore, since_ms: i64) -> Vec<ExpectedPoint> {
        expected_over(
            store,
            "g",
            "s",
            since_ms,
            END + 1000,
            BUCKET.bucket_ms(),
            &Learning::default(),
        )
        .unwrap()
    }

    #[test]
    fn a_short_chart_has_the_five_minute_band_in_each_five_minutes() {
        let store = store_with(7, |_| Some(100.0));
        let points = expected(&store, END - HOUR);
        assert_eq!(points.len(), 13, "the hour's twelve buckets and the one still filling");
        assert_eq!(points[0].from_millis, END - HOUR);
        assert_eq!((points[12].from_millis, points[12].to_millis), (END, END + FIVE));
        for p in &points {
            assert_eq!((p.low, p.high), (70.0 / 300.0, 130.0 / 300.0));
        }
    }

    /// The first days of a week's chart have fewer than [`MIN_DAYS`] days of history before them.
    #[test]
    fn a_long_chart_has_the_thirty_minute_band_each_hour_and_none_where_history_is_short() {
        let store = store_with(7, |_| Some(100.0));
        let points = expected(&store, END - 7 * DAY_MS);
        let first = points.first().unwrap().from_millis;
        assert!(first >= END - 6 * DAY_MS && first < END - 4 * DAY_MS, "{first}");
        assert_eq!(points.last().unwrap().from_millis, END);
        assert!(points.windows(2).all(|w| w[0].to_millis == w[1].from_millis));
        for p in &points {
            assert_eq!(
                (p.low, p.high),
                (420.0 / 1800.0, 780.0 / 1800.0),
                "at {}",
                p.from_millis
            );
        }
    }

    #[test]
    fn a_chart_band_leaves_out_a_stop() {
        let (store, outage) = outage_and_stop();
        warned(&store, "source_unreachable", outage, Some(outage + 2 * HOUR));
        for p in expected(&store, END - HOUR) {
            assert_eq!(p.low, 70.0 / 300.0, "at {}", p.from_millis);
        }
    }

    #[test]
    fn a_source_never_read_has_no_band() {
        let store = store_with(7, |_| Some(100.0));
        let none = expected_over(
            &store,
            "g",
            "other",
            END - HOUR,
            END + 1000,
            BUCKET.bucket_ms(),
            &Learning::default(),
        );
        assert_eq!(none.unwrap(), Vec::new());
    }

    /// Rows read every ten seconds from `from` to `until`, alternating 10 and 30: one and three rows a second, two
    /// over any minute.
    fn alternating(store: &RegistryStore, from: i64, until: i64) {
        let key = serde_json::to_string(&[("group", "g"), ("source", "s"), ("worker", "w")]).unwrap();
        for (i, at) in (from..until).step_by(10_000).enumerate() {
            let rows = if i % 2 == 0 { 10.0 } else { 30.0 };
            store.record_delta(ROWS_IN_METRIC, &key, at, rows).unwrap();
        }
    }

    #[test]
    fn a_swing_is_the_lowest_and_highest_rate_at_each_width() {
        let store = store_with(0, |_| None);
        alternating(&store, END - 30 * 60_000, END);
        record_swings(&store, END, Duration::from_secs(1));
        let swings = |width: Tier| -> Vec<(i64, f64, f64)> {
            store
                .swings_since("g", "s", width.bucket_ms(), 0)
                .unwrap()
                .into_iter()
                .map(|s| (s.bucket_ms, s.low, s.high))
                .collect()
        };
        // The first bucket has no read bucket before it, and the last none ended after it yet.
        let buckets: Vec<i64> = (2..=5).rev().map(|i| END - i * FIVE).collect();
        assert_eq!(
            swings(Tier::TenSeconds),
            buckets.iter().map(|&b| (b, 1.0, 3.0)).collect::<Vec<_>>()
        );
        assert_eq!(
            swings(Tier::OneMinute),
            buckets.iter().map(|&b| (b, 2.0, 2.0)).collect::<Vec<_>>()
        );

        // Held, a bucket is not recorded again; the next bucket's end records the one after.
        alternating(&store, END, END + FIVE);
        record_swings(&store, END + FIVE, Duration::from_secs(1));
        assert_eq!(swings(Tier::TenSeconds).len(), 5);
    }

    /// A slot with no point in a bucket read on both sides read nothing. One such slot among thirty does not set the
    /// bucket's low at ten seconds; among the five minutes, it moves the low part of the way to its minute.
    #[test]
    fn one_quiet_slot_does_not_set_a_buckets_low() {
        let mut slots: BTreeMap<i64, f64> = (0..3 * FIVE).step_by(10_000).map(|at| (END + at, 50.0)).collect();
        slots.remove(&(END + FIVE + 20_000));
        let swings = swings_of(&slots, END + FIVE, END + 2 * FIVE, 1000);
        let quiet_minute = 250.0 * 1000.0 / 60_000.0;
        assert_eq!(
            swings,
            vec![
                (10_000, END + FIVE, 5.0, 5.0),
                (60_000, END + FIVE, quiet_minute + 0.4 * (5.0 - quiet_minute), 5.0)
            ]
        );
    }

    #[test]
    fn a_fine_chart_has_the_band_its_points_swung_in() {
        let store = store_with(0, |_| None);
        let mut swings = Vec::new();
        let mut at = END - 8 * DAY_MS;
        while at < END {
            for (width, low, high) in [(10_000, 4.0, 16.0), (60_000, 8.0, 12.0)] {
                swings.push(Swing {
                    group: "g".into(),
                    source: "s".into(),
                    width_ms: width,
                    bucket_ms: at,
                    low,
                    high,
                });
            }
            at += FIVE;
        }
        store.record_swings(&swings).unwrap();
        for (width, band) in [(10_000, (4.0, 16.0)), (60_000, (8.0, 12.0))] {
            let points = expected_over(&store, "g", "s", END - HOUR, END + 1000, width, &Learning::default()).unwrap();
            assert_eq!(points.len(), 13);
            assert!(points.iter().all(|p| (p.low, p.high) == band), "at {width}: {points:?}");
        }
    }

    /// Heartbeats slower than ten seconds leave most ten-second slots empty, so no swing is kept at that width.
    #[test]
    fn no_swing_is_kept_finer_than_the_heartbeat() {
        let store = store_with(0, |_| None);
        alternating(&store, END - 30 * 60_000, END);
        record_swings(&store, END, Duration::from_secs(15));
        assert_eq!(
            store.swings_since("g", "s", Tier::TenSeconds.bucket_ms(), 0).unwrap(),
            vec![]
        );
        assert_eq!(
            store
                .swings_since("g", "s", Tier::OneMinute.bucket_ms(), 0)
                .unwrap()
                .len(),
            4
        );
    }
}
