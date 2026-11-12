//! What each source normally reads, and whether it is reading that now.
//!
//! A source's normal is learned from the rows it read, which the registry already keeps (`series.rs`), and it is judged
//! at each of four [`WIDTHS`]: the rows of the last 5 minutes, 30 minutes, 6 hours and 24 hours. At one width, the
//! usual window of a time of day is the median of the windows that ended within [`NEIGHBORHOOD`] of it on each of the
//! newest [`USUAL_DAYS`] days learned there. Each window of the width's history, its newest learned windows, is taken
//! as a ratio to the usual window of its time of day. The range is the usual window scaled by the lowest and the
//! highest ratios, a [`TAIL`] of them cut from each end, and widened by [`MARGIN`]. Ratios, because a quiet hour and a
//! busy one can be pooled only once each is set against its own time of day; a quantile, because rows read are bursty
//! and often zero, which a mean and a deviation do not describe.
//!
//! A width judges a source only where its usual window holds at least [`GATE`] rows: a window that is usually near
//! empty has a low edge of zero, which no reading is below. So a source that reads every few seconds is judged at every
//! width, and one that reads a few rows an hour only at the long ones.
//!
//! A source too sparse for any of them still has a normal in time per row, and a fifth width, the gap, judges it there:
//! how long it has gone without a row, against the longest quiet spell it made in the past year ([`Gap`]). A quiet
//! spell is a run of buckets with no rows, each reported by a worker, between two with rows (`series::Silence`). The
//! spells are recorded each hour and kept a year, since the buckets are kept only four weeks. The gap judges a source
//! once it has been watched [`GAP_WATCH_MS`]. Until it has been watched a year, its longest spell is taken to be
//! shorter than the year's longest, and is lengthened by what the rest of the year would likely add.
//!
//! A source is below normal while some width has read below its range [`WAIT`] times in a row, since one reading at the
//! range's edge flickers, or once its quiet spell is longer than [`GAP_MARGIN`] times the longest it learned. Either
//! raises the same warning, and it stays until some width judges the source and none reads it below its range. Days not
//! learned are passed over, not counted, so a held warning is judged against the days before it until those are no
//! longer kept, after which no width judges the source and it still warns. Where no count width judges the source, a
//! warning ends at the first row after it began that ends a quiet spell the gap read below. Above normal is judged at
//! [`WINDOW`] alone, and only informs.
//!
//! Only windows the source was running for throughout count. A bucket with no point means no worker reported the source
//! then -- not deployed, or a worker gone -- which says nothing about how much it reads.
//!
//! A past window does not count when it shares a bucket with a stop about the source (`warnings::spans`), or with a
//! window of its width that ended while a below-normal warning held or waited to be raised. A quiet spell does not
//! count when it shares a bucket with a stop or with such a warning. An outage at this time of day would otherwise set
//! the range's low edge there, and a source that dropped would come to call the drop normal. An editor may say a past
//! period was normal, and it is learned though it warned, or an outage, and it is not learned though it did not warn
//! (`marks::Period`).
//!
//! Rows above normal are learned: more rows are the new normal until shown otherwise.
//!
//! A window that holds a stop is not judged either. Its reading is the stop, which a warning naming the cause has
//! already raised. The short widths judge again within half an hour of the stop's end; the long ones wait up to 24
//! hours. The gap counts the quiet spell from the stop's end. This rule is the leader's own: the detector was measured
//! on sources with no stops.
//!
//! A chart draws the range behind its points, and a point finer than a window swings wider than a window does. So each
//! [`BUCKET`] also records how far each source's rate swung inside it at each of [`SWING_WIDTHS`] (`series::Swing`),
//! while the ten-second tier still holds what it read. A chart's range at that width is learned from those swings.
//!
//! Neither the normal nor a reading is stored. Only each source's quiet spells are, since they outlive its buckets. The
//! normal is learned again each hour, since it moves slowly, and each reading once per [`BUCKET`] against it, since
//! nothing it reads changes in between.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::senders::{self, Pick, Sender, SenderKey};
use super::series::{RegistryStore, Silence, SubjectPoints, Swing, Tier, Watch, YEAR_MS};
use super::store::Store;
use super::warnings::{self, Condition, SubjectKind, label};
use super::{AppState, ROWS_IN_METRIC, marks, run_store};

/// The width above normal is judged at, and the one a source's rate is shown at where no other is called for.
pub const WINDOW: Duration = Duration::from_secs(30 * 60);

/// The resolution history is judged at: the finest every tier the history spans can be read at.
pub(super) const BUCKET: Tier = Tier::FiveMinutes;

/// One width a source is judged at: its window, and how many days of past windows its range is learned from.
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
/// seven days that do not overlap, too few to cut one in two thousand, so the long widths learn from four.
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

/// How many days a time of day's usual window is learned from: the newest with a learned window near it.
const USUAL_DAYS: i64 = 7;

/// How far either side of the same time of day a past window may end and still count as the same time of day.
const NEIGHBORHOOD: Duration = Duration::from_secs(30 * 60);

/// Days with a window at a time of day the source ran for throughout, before it is judged at that time of day.
/// Fewer, and one odd day is most of what it knows.
pub const MIN_DAYS: usize = 3;

/// The share of past ratios each side of the range leaves out: one in two thousand.
const TAIL: f64 = 0.0005;

/// How much wider than the ratios the range is, as a share of each edge.
const MARGIN: f64 = 0.3;

/// Readings in a row outside the range before a source is outside its normal.
const WAIT: i64 = 4;

/// The fewest rows a width's usual window holds for that width to judge a source.
const GATE: f64 = 10.0;

const DAY_MS: i64 = 24 * 60 * 60 * 1000;

/// The factor of its longest learned quiet spell that a source may go without a row before it is below normal. It is
/// more than one because a spell that warned is never learned: without room above the longest, a longer spell could
/// never be learned.
const GAP_MARGIN: f64 = 1.5;

/// How long a source is watched before its quiet spells are judged.
const GAP_WATCH_MS: i64 = 28 * DAY_MS;

/// The range: how many rows one source normally reads in one window at this time of day.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Band {
    pub low: f64,
    pub high: f64,
}

/// What a normal is learned with: the sources an editor said to learn again from a time on ([`marks::Reset`]), the
/// past periods an editor said were normal or an outage ([`marks::Period`]), and the sender fields an editor changed
/// ([`marks::SourceFields`]).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Learning {
    pub reset: Vec<marks::Reset>,
    pub periods: Vec<marks::Period>,
    pub fields: Vec<marks::SourceFields>,
}

impl Learning {
    /// The marks as stored.
    pub fn read(store: &Store) -> Learning {
        let marks = marks::Marks::read(store);
        Learning {
            reset: marks.reset,
            periods: marks.periods,
            fields: marks.fields,
        }
    }

    /// Whether an editor stopped watching `field` of `group`'s `source`.
    pub fn unchecked(&self, group: &str, source: &str, field: &str) -> bool {
        self.fields
            .iter()
            .any(|f| f.group == group && f.edits.source == source && f.edits.unchecked.iter().any(|u| u == field))
    }

    /// The periods of `group`'s `source` an editor said `was`, as (from, to).
    fn marked(&self, group: &str, source: &str, was: marks::Was) -> Vec<(i64, i64)> {
        self.periods
            .iter()
            .filter(|p| p.group == group && p.source == source && p.was == was)
            .map(|p| (p.from_millis, p.to_millis))
            .collect()
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

/// One source's reading at the end of one bucket, or one of its senders'.
#[derive(Debug, Clone, PartialEq)]
pub struct Reading {
    pub group: String,
    pub source: String,
    /// The sender read, or `None` for the source's rows as a whole.
    pub sender: Option<Sender>,
    pub ended_ms: i64,
    /// Each width that judges the source now, narrowest first.
    pub judged: Vec<Judged>,
    pub outside: Option<Outside>,
    /// The gap's reading, where the gap judges the source.
    pub gap: Option<Gap>,
}

/// The gap's reading: how long the source has gone without a row, against the longest quiet spell it learned.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Gap {
    /// Since its last row, or since the last bucket no worker reported it in, or since it was last stopped, whichever
    /// is latest.
    pub silence_ms: i64,
    /// The longest quiet spell it learned.
    pub longest_ms: i64,
    /// The longest quiet spell it may make before it is below normal: [`GAP_MARGIN`] times the longest it learned,
    /// lengthened while it has been watched less than a year.
    pub edge_ms: i64,
}

impl Gap {
    /// The quiet spell under way is longer than the source may make.
    pub fn below(&self) -> bool {
        self.silence_ms > self.edge_ms
    }
}

/// One width's reading: the rows of its window that ended with the reading, and its range.
#[derive(Debug, Clone, PartialEq)]
pub struct Judged {
    pub window: Duration,
    pub rows: f64,
    pub band: Band,
    /// This reading and the [`WAIT`] - 1 before it were each below their range.
    pub below: bool,
    /// This reading and the [`WAIT`] - 1 before it were each above their range, at [`WINDOW`], the one width above
    /// normal is judged at.
    pub above: bool,
}

impl Judged {
    /// This reading alone is below its range.
    pub fn under(&self) -> bool {
        self.rows < self.band.low
    }

    /// The rows and range as rows per second.
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
    /// The width a reader is shown: the narrowest that reads the source outside its range now, else [`WINDOW`], else
    /// the narrowest that judges it. None while none judges it.
    pub fn shown(&self) -> Option<&Judged> {
        let side = |j: &&Judged| match self.outside {
            Some(Outside::Below) => j.under(),
            Some(Outside::Above) => j.above,
            None => false,
        };
        self.judged
            .iter()
            .find(side)
            .or_else(|| self.judged.iter().find(|j| j.window == WINDOW))
            .or_else(|| self.judged.first())
    }
}

/// Which side of its normal a source is on, from its widths' readings and its gap's, and whether a below-normal
/// warning about it held until now. A held warning ends only on a reading inside the range. A reading no width can
/// judge neither ends it nor raises a new warning.
fn outside(judged: &[Judged], gap: Option<&Gap>, below_held: bool) -> Option<Outside> {
    let inside = !judged.is_empty() && !judged.iter().any(Judged::under);
    if judged.iter().any(|j| j.below) || gap.is_some_and(Gap::below) || (below_held && !inside) {
        Some(Outside::Below)
    } else if judged.iter().any(|j| j.above) {
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
    learned: Mutex<Option<(i64, Learning, Arc<Learned>)>>,
    /// The senders picked as of the day it names, read again each day.
    pick: Mutex<Option<Arc<Pick>>>,
    computing: Mutex<()>,
}

/// A source, by its group and id.
type SourceKey = (String, String);

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
        let learned = self.learned_at(store, ended_ms, learning);
        let readings = Arc::new(compute(store, ended_ms, learning, &learned));
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

    /// Every source's normal and gap as of the hour `ended_ms` is in, learned again when that hour or `learning` is
    /// new.
    fn learned_at(&self, store: &RegistryStore, ended_ms: i64, learning: &Learning) -> Arc<Learned> {
        let as_of = Tier::OneHour.align(ended_ms);
        let mut held = self.learned.lock().unwrap_or_else(|e| e.into_inner());
        if let Some((at, with, learned)) = held.as_ref()
            && *at == as_of
            && with == learning
        {
            return learned.clone();
        }
        let mut pick = self.pick.lock().unwrap_or_else(|e| e.into_inner());
        let learned = Arc::new(learn(store, as_of, learning, pick.clone()));
        // What was learned is kept for the hour, and the senders picked for the day, only once some source has a normal
        // or a gap that judges it. Until then a fleet whose history was just written is learned again, and judged, at
        // its next reading.
        if learned.widths.values().any(|n| !n.is_empty()) || learned.gaps.values().any(|g| g.judged.is_some()) {
            *held = Some((as_of, learning.clone(), learned.clone()));
            *pick = Some(learned.pick.clone());
        }
        learned
    }
}

impl Readings {
    /// What was last learned, if anything was: the senders picked, and each one's normal.
    pub fn learned(&self) -> Option<Arc<Learned>> {
        let held = self.learned.lock().unwrap_or_else(|e| e.into_inner());
        held.as_ref().map(|(_, _, learned)| learned.clone())
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

/// One width's normal as of one time: the usual window at each time of day, and the lowest and highest ratios to
/// it.
#[derive(Debug, Clone, PartialEq)]
struct Normal {
    width: Width,
    /// By the bucket of the day a window ends at. None where too few days had one.
    usual: Vec<Option<f64>>,
    low: f64,
    high: f64,
}

impl Normal {
    /// The range of the window ending at `ended_ms`, or None where this width does not judge the source.
    fn band(&self, ended_ms: i64) -> Option<Band> {
        let usual = self.usual[time_of_day(ended_ms)].filter(|&t| t >= GATE)?;
        Some(Band {
            low: (1.0 - MARGIN) * ((usual + 1.0) * self.low - 1.0).max(0.0),
            high: (1.0 + MARGIN) * ((usual + 1.0) * self.high - 1.0),
        })
    }
}

/// Which bucket of its day the window ending at `ended_ms` ends at.
fn time_of_day(ended_ms: i64) -> usize {
    (ended_ms.rem_euclid(DAY_MS) / BUCKET.bucket_ms()) as usize
}

/// `width`'s normal as of `as_of`, from the windows learned that ended before it, or None while it has no ratio at all.
/// Its history is its newest learned windows, as many as fit in [`Width::history_days`] days, and a time of day's usual
/// window is learned from the newest [`USUAL_DAYS`] days with a learned window near it. Both reach back past what is
/// not learned, as far as [`lookback_ms`], so a warning that holds for days is still judged against the days before it.
/// Each window's usual window is that of its time of day as of `as_of`, so every window of the history is set against
/// the same days. Each window and its usual window get one more row, so a time of day that is usually empty divides
/// like any other.
fn normal(rows: &Rows, warned: &Warned, as_of: i64, width: Width) -> Option<Normal> {
    let step = BUCKET.bucket_ms();
    let per_day = DAY_MS / step;
    // Each learned window, newest first, with how many buckets before `as_of` it ended. The last readings' windows are
    // left out: a warning may yet wait for them, and a source's normal learns nothing from the readings a warning
    // waited for.
    let learned: Vec<(i64, f64)> = (WAIT..=lookback_ms() / step)
        .filter_map(|i| {
            let ended = as_of - i * step;
            let sum = rows
                .window(ended, width)
                .filter(|_| !warned.during(ended, width.ms()))?;
            Some((i, sum))
        })
        .collect();
    let mut by_time: Vec<Vec<(i64, f64)>> = vec![Vec::new(); per_day as usize];
    for &(i, sum) in &learned {
        by_time[time_of_day(as_of - i * step)].push((i, sum));
    }
    let across = NEIGHBORHOOD.as_millis() as i64 / step;
    let usual: Vec<Option<f64>> = (0..per_day)
        .map(|at| {
            // Each window near `at`, grouped by the day it is near: a window in the half hour after a midnight counts
            // for that midnight's day. A time of day has one window a day, newest first, so the newest days near `at`
            // are among the first of each.
            let near: Vec<(i64, f64)> = (-across..=across)
                .flat_map(|off| {
                    let of = &by_time[(at + off).rem_euclid(per_day) as usize];
                    of.iter()
                        .take(USUAL_DAYS as usize)
                        .map(move |&(i, sum)| ((i + off).div_euclid(per_day), sum))
                })
                .collect();
            let mut days: Vec<i64> = near.iter().map(|&(day, _)| day).collect();
            days.sort_unstable();
            days.dedup();
            if days.len() < MIN_DAYS {
                return None;
            }
            let newest = days[days.len().min(USUAL_DAYS as usize) - 1];
            let mut sums: Vec<f64> = near
                .into_iter()
                .filter(|&(day, _)| day <= newest)
                .map(|(_, sum)| sum)
                .collect();
            sums.sort_by(f64::total_cmp);
            Some(quantile(&sums, 0.5))
        })
        .collect();
    let mut ratios: Vec<f64> = learned
        .iter()
        .take((width.history_days * per_day) as usize)
        .filter_map(|&(i, sum)| Some((sum + 1.0) / (usual[time_of_day(as_of - i * step)]? + 1.0)))
        .collect();
    if ratios.is_empty() {
        return None;
    }
    ratios.sort_by(f64::total_cmp);
    Some(Normal {
        width,
        low: quantile(&ratios, TAIL),
        high: quantile(&ratios, 1.0 - TAIL),
        usual,
    })
}

/// Each source's normal at each width that has one.
type Normals = HashMap<(String, String), Vec<Normal>>;

/// What every source is judged against as of one hour: its normal at each count width, and its gap; and each
/// sender watched, with its normal at each width.
#[derive(Default)]
pub struct Learned {
    widths: Normals,
    gaps: HashMap<SourceKey, GapNormal>,
    pub pick: Arc<Pick>,
    senders: HashMap<SenderKey, Vec<Normal>>,
}

impl Learned {
    /// Each sender watched, by its source.
    pub fn watched(&self, group: &str, source: &str) -> Vec<SenderKey> {
        let mut out: Vec<SenderKey> = self
            .senders
            .keys()
            .filter(|(g, s, _)| g == group && s == source)
            .cloned()
            .collect();
        out.sort();
        out
    }

    /// `key`'s range, as rows per second, at each [`BUCKET`] of `rows`, by the narrowest width that judges it there.
    pub fn expected(&self, key: &SenderKey, rows: impl Iterator<Item = i64>) -> Vec<ExpectedPoint> {
        let Some(normals) = self.senders.get(key) else {
            return Vec::new();
        };
        let step = BUCKET.bucket_ms();
        rows.filter_map(|at| {
            let ended_ms = at + step;
            normals.iter().find_map(|normal| {
                let band = normal.band(ended_ms)?;
                let seconds = normal.width.window.as_secs_f64();
                Some(ExpectedPoint {
                    from_millis: at,
                    to_millis: ended_ms,
                    low: band.low / seconds,
                    high: band.high / seconds,
                })
            })
        })
        .collect()
    }
}

/// How far back a normal reads: the longest history, and the window that ends where it begins.
fn lookback_ms() -> i64 {
    WIDTHS
        .iter()
        .map(|w| w.history_days.max(USUAL_DAYS) * DAY_MS + w.ms())
        .max()
        .unwrap_or_default()
}

/// Every source's normals and gap as of `as_of`, once the quiet spells its rows hold up to then are recorded, and
/// each watched sender's normals. The senders are picked again when `as_of` is in another day than `pick`'s.
fn learn(store: &RegistryStore, as_of: i64, learning: &Learning, pick: Option<Arc<Pick>>) -> Learned {
    let oldest = as_of - lookback_ms();
    let buckets = match history(store, BUCKET.and_finer(), BUCKET, oldest) {
        Ok(buckets) => buckets,
        Err(e) => {
            tracing::warn!(
                name: "series_unreadable", target: "status",
                error = %e,
                "could not read rows read, so no source's normal is learned this hour"
            );
            return Learned::default();
        }
    };
    let day = senders::day_of(as_of);
    let pick = match pick {
        Some(pick) if pick.day_ms == day => pick,
        _ => Arc::new(senders::pick(store, &buckets, day).unwrap_or_else(|e| {
            tracing::warn!(name: "senders_pick_unreadable", target: "status", error = %e, "could not read the senders' rows, so no sender is watched today");
            Pick { day_ms: day, ..Pick::default() }
        })),
    };
    let watched = pick.watched(|group, source, field| learning.unchecked(group, source, field));
    let sender_rows = senders::history(store, &watched, &buckets, oldest).unwrap_or_else(|e| {
        tracing::warn!(name: "senders_learn_unreadable", target: "status", error = %e, "could not read the senders' rows, so no sender's normal is learned this hour");
        HashMap::new()
    });
    Learned {
        gaps: gaps(store, &buckets, oldest, as_of, learning),
        senders: sender_rows
            .into_iter()
            .map(|(key, of)| {
                let rows = Rows::of(&of, oldest, as_of);
                let warned = Warned::of_sender(store, &key, oldest, learning);
                let normals = WIDTHS
                    .iter()
                    .filter_map(|&width| normal(&rows, &warned, as_of, width))
                    .collect();
                (key, normals)
            })
            .collect(),
        widths: buckets
            .into_iter()
            .map(|(key, of)| {
                let normals = learned(store, &key, &of, oldest, as_of, learning);
                (key, normals)
            })
            .collect(),
        pick,
    }
}

/// The normals of `key`'s rows `of`, read since `oldest`, as of `as_of`.
fn learned(
    store: &RegistryStore,
    key: &SourceKey,
    of: &BTreeMap<i64, f64>,
    oldest: i64,
    as_of: i64,
    learning: &Learning,
) -> Vec<Normal> {
    let rows = Rows::of(of, oldest, as_of);
    let warned = Warned::of(store, &key.0, &key.1, oldest, learning);
    WIDTHS
        .iter()
        .filter_map(|&width| normal(&rows, &warned, as_of, width))
        .collect()
}

/// [`Readings::at`] without the cache: each source's reading at `ended_ms` against what was `learned`.
fn compute(store: &RegistryStore, ended_ms: i64, learning: &Learning, learned: &Learned) -> Vec<Reading> {
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
        .iter()
        .filter_map(|(key, of)| {
            let rows = Rows::of(of, oldest, ended_ms);
            let warned = Warned::of(store, &key.0, &key.1, oldest, learning);
            let judged: Vec<Judged> = learned
                .widths
                .get(key)
                .into_iter()
                .flatten()
                .filter_map(|normal| judge(&rows, &warned, normal, ended_ms))
                .collect();
            let normal = learned.gaps.get(key);
            let gap = normal.and_then(|normal| normal.at(of, &warned, ended_ms));
            // Where no count width judges it, the gap does: a warning ends at the first row after it began that ends a
            // quiet spell the gap read below.
            let quiet_back = judged.is_empty()
                && gap.is_some()
                && normal.zip(warned.held_since).is_some_and(|(normal, since)| {
                    let ended = normal.ended_below(normal.watch, of, &warned, ended_ms);
                    ended.max(normal.ended_below_ms).is_some_and(|ended| ended > since)
                });
            let outside = outside(&judged, gap.as_ref(), warned.held_since.is_some() && !quiet_back);
            let (group, source) = key.clone();
            (!judged.is_empty() || gap.is_some() || outside.is_some()).then_some(Reading {
                group,
                source,
                sender: None,
                ended_ms,
                judged,
                outside,
                gap,
            })
        })
        .collect();
    // A sender is not judged while its source warns (`Warned::of_sender`), nor on the reading its source crosses.
    let below: HashSet<(String, String)> = out
        .iter()
        .filter(|r| r.outside == Some(Outside::Below))
        .map(|r| (r.group.clone(), r.source.clone()))
        .collect();
    let senders = sender_readings(store, ended_ms, oldest, learning, learned, &buckets);
    out.extend(
        senders
            .into_iter()
            .filter(|r| !below.contains(&(r.group.clone(), r.source.clone()))),
    );
    out.sort_by(|a, b| (&a.group, &a.source, &a.sender).cmp(&(&b.group, &b.source, &b.sender)));
    out
}

/// Each watched sender's reading at `ended_ms`, from its rows since `oldest` where its source was reported in
/// `sources`. Only below normal is judged: a sender above it reads more of its source's rows, which its source is
/// judged by.
fn sender_readings(
    store: &RegistryStore,
    ended_ms: i64,
    oldest: i64,
    learning: &Learning,
    learned: &Learned,
    sources: &History,
) -> Vec<Reading> {
    let watched: Vec<SenderKey> = learned.senders.keys().cloned().collect();
    let buckets = senders::history(store, &watched, sources, oldest).unwrap_or_else(|e| {
        tracing::warn!(name: "senders_judge_unreadable", target: "status", error = %e, "could not read the senders' rows, so no sender is judged this pass");
        HashMap::new()
    });
    buckets
        .into_iter()
        .filter_map(|(key, of)| {
            let rows = Rows::of(&of, oldest, ended_ms);
            let warned = Warned::of_sender(store, &key, oldest, learning);
            let judged: Vec<Judged> = learned.senders[&key]
                .iter()
                .filter_map(|normal| judge(&rows, &warned, normal, ended_ms))
                .collect();
            let outside = outside(&judged, None, warned.held_since.is_some()).filter(|o| *o == Outside::Below);
            let (group, source, sender) = key;
            (!judged.is_empty() || outside.is_some()).then_some(Reading {
                group,
                source,
                sender: Some(sender),
                ended_ms,
                judged,
                outside,
                gap: None,
            })
        })
        .collect()
}

/// The reading of the window ending at `ended_ms`, at `normal`'s width, or None where that width does not judge it, as
/// where the window holds a stop.
fn judge(rows: &Rows, warned: &Warned, normal: &Normal, ended_ms: i64) -> Option<Judged> {
    let width = normal.width;
    let read = |ended: i64| {
        if Warned::overlaps(&warned.stops, ended, width.ms()) {
            return None;
        }
        Some((rows.window(ended, width)?, normal.band(ended)?))
    };
    let (now, band) = read(ended_ms)?;
    let each = |past: fn(f64, Band) -> bool| {
        (0..WAIT).all(|i| read(ended_ms - i * BUCKET.bucket_ms()).is_some_and(|(rows, band)| past(rows, band)))
    };
    Some(Judged {
        window: normal.width.window,
        rows: now,
        band,
        below: each(|rows, band| rows < band.low),
        above: width.window == WINDOW && each(|rows, band| rows > band.high),
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

/// What one source's warnings say about its rows, as spans (started, ended), with no end for a span still open.
struct Warned {
    /// Each span a stop held, as (started, ended).
    stops: Vec<(i64, Option<i64>)>,
    /// When its rows teach its normal nothing: while a stop about it held, unless an editor said that period was
    /// normal; in any period an editor said was an outage; and before an editor reset its history.
    unlearned: Vec<(i64, Option<i64>)>,
    /// Each below-normal warning about it, from the first reading it waited for ([`waited_from`]), less the periods an
    /// editor said were normal. One that began in such a period is learned whole, so marking one that still holds
    /// teaches the level it reads.
    below: Vec<(i64, Option<i64>)>,
    /// When a below-normal warning about it that still holds began.
    held_since: Option<i64>,
}

impl Warned {
    /// `group`'s `source`'s spans since `since_ms`. Warnings the store cannot read are logged and left out: the
    /// normal is then learned from those windows too.
    fn of(store: &RegistryStore, group: &str, source: &str, since_ms: i64, learning: &Learning) -> Warned {
        Warned::read(store, group, source, None, since_ms, learning)
    }

    /// [`Warned::of`] for a sender. Its source's below-normal warnings are stops to it, as its source's stops are:
    /// what a sender reads while its source reads too little says nothing of the sender.
    fn of_sender(store: &RegistryStore, key: &SenderKey, since_ms: i64, learning: &Learning) -> Warned {
        Warned::read(store, &key.0, &key.1, Some(&key.2), since_ms, learning)
    }

    fn read(
        store: &RegistryStore,
        group: &str,
        source: &str,
        sender: Option<&Sender>,
        since_ms: i64,
        learning: &Learning,
    ) -> Warned {
        let unreadable = |e: super::store::StoreError| {
            tracing::warn!(
                name: "warned_unreadable", target: "status",
                error = %e, group, source,
                "could not read when this source warned, so its normal is learned from every window this pass"
            );
            Vec::new()
        };
        let held = warnings::spans(store, SubjectKind::Source, group, source, since_ms).unwrap_or_else(unreadable);
        let (stops, below): (Vec<(i64, Option<i64>)>, Vec<warnings::WarningSpan>) = match sender {
            None => (
                held.iter()
                    .filter(|s| !matches!(s.condition, Condition::SourceRowsBelow | Condition::SourceRowsAbove))
                    .map(|s| (s.started_millis, s.ended_millis))
                    .collect(),
                held.into_iter()
                    .filter(|s| s.condition == Condition::SourceRowsBelow)
                    .collect(),
            ),
            Some(sender) => (
                held.iter()
                    .filter(|s| s.condition != Condition::SourceRowsAbove)
                    .map(|s| (s.started_millis, s.ended_millis))
                    .collect(),
                warnings::sender_spans(store, group, source, sender, since_ms).unwrap_or_else(unreadable),
            ),
        };
        let below = below.iter();
        let reset_from = learning.reset_from(group, source);
        let reset = reset_from.map(|from| (i64::MIN, Some(from)));
        let normal = learning.marked(group, source, marks::Was::Normal);
        let outage = learning.marked(group, source, marks::Was::Outage);
        Warned {
            // A reset ends the warning: the normal it was judged against is forgotten.
            held_since: below
                .clone()
                .filter(|s| s.ended_millis.is_none() && reset_from.is_none_or(|from| s.started_millis >= from))
                .map(|s| s.started_millis)
                .min(),
            below: less(
                below
                    .map(|s| (waited_from(s), s.ended_millis))
                    .filter(|&(from, _)| !normal.iter().any(|&(began, ended)| (began..=ended).contains(&from)))
                    .collect(),
                &normal,
            ),
            unlearned: less(stops.clone(), &normal)
                .into_iter()
                .chain(outage.into_iter().map(|(from, to)| (from, Some(to))))
                .chain(reset)
                .collect(),
            stops,
        }
    }

    /// Whether the `width_ms` ending at `ended_ms` teaches the normal nothing: it overlaps a span not learned, or it
    /// shares a bucket with a window of its width that ended while a below-normal warning waited or held.
    fn during(&self, ended_ms: i64, width_ms: i64) -> bool {
        Warned::overlaps(&self.unlearned, ended_ms, width_ms)
            || Warned::overlaps(&self.below, ended_ms + width_ms, 2 * width_ms)
    }

    /// Whether the period of `length_ms` ending at `ended_ms`, a quiet spell or a warning, overlaps a span not
    /// learned or a below-normal warning.
    fn span_during(&self, ended_ms: i64, length_ms: i64) -> bool {
        Warned::overlaps(&self.unlearned, ended_ms, length_ms) || Warned::overlaps(&self.below, ended_ms, length_ms)
    }

    /// Whether the `width_ms` ending at `ended_ms` overlaps one of `spans`.
    fn overlaps(spans: &[(i64, Option<i64>)], ended_ms: i64, width_ms: i64) -> bool {
        spans
            .iter()
            .any(|&(started, ended)| ended_ms > started && ended.is_none_or(|ended| ended_ms - width_ms < ended))
    }
}

/// The earliest the first reading a below-normal warning waited for can have ended: [`WAIT`] buckets before the
/// warning was seen. That reading ended [`WAIT`] - 1 buckets before the one that raised the warning, and the warning
/// was seen up to a heartbeat, taken as one bucket, after that.
fn waited_from(warning: &warnings::WarningSpan) -> i64 {
    warning.started_millis - WAIT * BUCKET.bucket_ms()
}

/// How many of a source's longest quiet spells [`past`] lists.
const LONGEST_LISTED: usize = 10;

/// What a source did over the past year that an editor may say was normal or an outage ([`marks::Period`]).
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Past {
    /// Its below-normal warnings, newest first: each from the first reading it waited for until it ended, or until now
    /// while it holds.
    pub warnings: Vec<PastSpan>,
    /// Its longest quiet spells, longest first.
    pub silences: Vec<PastSpan>,
}

/// One span of [`Past`], and whether its source's normal learns from it.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PastSpan {
    pub from_millis: i64,
    pub to_millis: i64,
    pub learned: bool,
}

/// `group`'s `source`'s [`Past`] as of `now_ms`.
pub fn past(
    store: &RegistryStore,
    group: &str,
    source: &str,
    now_ms: i64,
    learning: &Learning,
) -> Result<Past, super::store::StoreError> {
    let year = now_ms - YEAR_MS;
    let warned = Warned::of(store, group, source, year, learning);
    let span = |from_millis: i64, to_millis: i64| PastSpan {
        from_millis,
        to_millis,
        learned: !warned.span_during(to_millis, to_millis - from_millis),
    };
    let mut silences = store.source_silences(group, source, year)?;
    silences.sort_by_key(|&(ended, length)| (std::cmp::Reverse(length), std::cmp::Reverse(ended)));
    Ok(Past {
        warnings: warnings::spans(store, SubjectKind::Source, group, source, year)?
            .iter()
            .rev()
            .filter(|s| s.condition == Condition::SourceRowsBelow)
            .map(|s| span(waited_from(s), s.ended_millis.unwrap_or(now_ms)))
            .collect(),
        silences: silences
            .into_iter()
            .take(LONGEST_LISTED)
            .map(|(ended, length)| span(ended - length, ended))
            .collect(),
    })
}

/// `spans` less the periods `cut`, each (from, to).
fn less(spans: Vec<(i64, Option<i64>)>, cut: &[(i64, i64)]) -> Vec<(i64, Option<i64>)> {
    cut.iter().fold(spans, |spans, &(from, to)| {
        spans
            .into_iter()
            .flat_map(|(started, ended)| {
                let before = (started < from).then(|| (started, Some(ended.map_or(from, |ended| ended.min(from)))));
                let after = ended.is_none_or(|ended| ended > to).then(|| (started.max(to), ended));
                before.into_iter().chain(after)
            })
            .collect()
    })
}

/// A source's gap as of one hour: how far its rows were read for quiet spells, and how long a spell it may make.
#[derive(Debug, Clone, PartialEq)]
struct GapNormal {
    watch: Watch,
    /// The end of the last stop about it, as of when this was learned.
    stopped_ms: Option<i64>,
    /// The longest quiet spell it learned, and the longest it may make ([`Gap::edge_ms`]). None while the gap does not
    /// judge it: it has been watched less than [`GAP_WATCH_MS`], or it learned no quiet spell.
    judged: Option<(i64, i64)>,
    /// What [`Self::ended_below`] gave for the rows up to `watch`, when this was learned. A later reading reads only the
    /// rows after `watch`, so this keeps the newest row before it that ended such a spell.
    ended_below_ms: Option<i64>,
}

impl GapNormal {
    /// `watch`'s source's gap as of `as_of`, from its quiet spells of the past year, `silences`, as (ended, length).
    fn of(watch: Watch, silences: &[(i64, i64)], warned: &Warned, as_of: i64, reset_from: Option<i64>) -> GapNormal {
        let learned: Vec<f64> = silences
            .iter()
            .filter(|&&(ended, length)| !warned.span_during(ended, length))
            .map(|&(_, length)| length as f64)
            .collect();
        let watched = as_of - reset_from.map_or(watch.watched_ms, |from| from.max(watch.watched_ms));
        let longest = learned
            .iter()
            .copied()
            .reduce(f64::max)
            .filter(|_| watched >= GAP_WATCH_MS);
        GapNormal {
            watch,
            stopped_ms: warned.stops.iter().filter_map(|&(_, ended)| ended).max(),
            judged: longest.map(|longest| {
                // A year's longest quiet spell is likely longer than the longest of less than a year. For quiet spells
                // that end at a steady chance, the longest of n grows by about their mean each time n is multiplied by
                // e. `m` estimates that mean as Σs²/2Σs, which a burst's short spells barely move.
                let m = learned.iter().map(|s| s * s).sum::<f64>() / (2.0 * learned.iter().sum::<f64>());
                let more = m * (YEAR_MS as f64 / watched as f64).ln().max(0.0);
                (longest as i64, (GAP_MARGIN * (longest + more)) as i64)
            }),
            ended_below_ms: None,
        }
    }

    /// The gap's reading at `ended_ms`, from `of`, the source's buckets since the hour. None while the gap does not
    /// judge it, and while a stop holds: its warning already says why there are no rows.
    fn at(&self, of: &BTreeMap<i64, f64>, warned: &Warned, ended_ms: i64) -> Option<Gap> {
        let watch = watched(self.watch, of, ended_ms, &mut Vec::new());
        let stopped = warned.stops.iter().map(|&(_, ended)| ended);
        self.judged
            .filter(|_| stopped.clone().all(|ended| ended.is_some()))
            .map(|(longest_ms, edge_ms)| {
                let from = stopped.flatten().chain(self.stopped_ms).fold(watch.quiet_ms, i64::max);
                Gap {
                    silence_ms: ended_ms - from,
                    longest_ms,
                    edge_ms,
                }
            })
    }

    /// The end of the bucket of the newest row that ended a quiet spell longer than [`Gap::edge_ms`], which the gap read
    /// as below normal. It reads `of`'s rows from `watch` until `until`, and counts each spell as [`Gap::silence_ms`]
    /// counts it. None while the gap does not judge the source.
    fn ended_below(&self, watch: Watch, of: &BTreeMap<i64, f64>, warned: &Warned, until: i64) -> Option<i64> {
        let (_, edge_ms) = self.judged?;
        let step = BUCKET.bucket_ms();
        let stopped = warned
            .stops
            .iter()
            .filter_map(|&(_, ended)| ended)
            .chain(self.stopped_ms);
        let mut quiet = watch.quiet_ms;
        (watch.read_ms..until)
            .step_by(step as usize)
            .filter_map(|at| match of.get(&at) {
                Some(&rows) if rows > 0.0 => {
                    let from = stopped.clone().filter(|&ended| ended <= at).fold(quiet, i64::max);
                    quiet = at + step;
                    (at - from > edge_ms).then_some(at + step)
                }
                Some(_) => None,
                None => {
                    quiet = at + step;
                    None
                }
            })
            .last()
    }
}

/// `watch` read on through `of`'s buckets until `until`, with each quiet spell that ended on the way pushed to `ended`
/// as (ended, length). A bucket `of` has no point for is one no worker reported the source in.
fn watched(mut watch: Watch, of: &BTreeMap<i64, f64>, until: i64, ended: &mut Vec<(i64, i64)>) -> Watch {
    let step = BUCKET.bucket_ms();
    while watch.read_ms < until {
        let at = watch.read_ms;
        match of.get(&at) {
            Some(&rows) if rows > 0.0 => {
                if watch.row_ms == Some(watch.quiet_ms) && at > watch.quiet_ms {
                    ended.push((at, at - watch.quiet_ms));
                }
                watch.row_ms = Some(at + step);
                watch.quiet_ms = at + step;
            }
            Some(_) => {}
            None => watch.quiet_ms = at + step,
        }
        watch.read_ms += step;
    }
    watch
}

/// Each source's gap as of `as_of`, once the quiet spells its `buckets` from `oldest` hold up to then are recorded. A
/// source is watched from its first bucket.
fn gaps(
    store: &RegistryStore,
    buckets: &History,
    oldest: i64,
    as_of: i64,
    learning: &Learning,
) -> HashMap<SourceKey, GapNormal> {
    let held = match store.watches() {
        Ok(held) => held,
        Err(e) => {
            tracing::warn!(name: "watches_unreadable", target: "status", error = %e, "could not read how far each source's rows were checked for quiet spells, so no source's gap is judged this hour");
            return HashMap::new();
        }
    };
    let mut watches = Vec::new();
    let mut began = Vec::new();
    let mut silences = Vec::new();
    for (key, of) in buckets {
        let Some(&first) = of.keys().next() else { continue };
        let watch = match held.get(key) {
            Some(&watch) if watch.read_ms >= oldest => watch,
            // A source first seen, or back after being gone longer than the buckets are held, is watched afresh from
            // its first bucket: what came before is no longer held, so no quiet spell runs through it.
            _ => Watch {
                watched_ms: first,
                read_ms: first,
                row_ms: None,
                quiet_ms: first,
            },
        };
        let mut ended = Vec::new();
        began.push(watch);
        let watch = watched(watch, of, as_of, &mut ended);
        silences.extend(ended.into_iter().map(|(ended_ms, length_ms)| Silence {
            group: key.0.clone(),
            source: key.1.clone(),
            ended_ms,
            length_ms,
        }));
        watches.push((key.clone(), watch));
    }
    if let Err(e) = store.record_silences(&watches, &silences) {
        tracing::warn!(name: "silences_not_recorded", target: "status", error = %e, "could not record sources' quiet spells, so they are read again next hour");
    }
    let year = as_of - YEAR_MS;
    watches
        .into_iter()
        .zip(began)
        .map(|((key, watch), began)| {
            let silences = store.source_silences(&key.0, &key.1, year)?;
            let warned = Warned::of(store, &key.0, &key.1, year, learning);
            let gap = GapNormal::of(watch, &silences, &warned, as_of, learning.reset_from(&key.0, &key.1));
            let ended_below_ms = gap.ended_below(began, &buckets[&key], &warned, as_of);
            Ok((key, GapNormal { ended_below_ms, ..gap }))
        })
        .collect::<Result<_, super::store::StoreError>>()
        .unwrap_or_else(|e| {
            tracing::warn!(name: "silences_unreadable", target: "status", error = %e, "could not read sources' quiet spells, so no source's gap is judged this hour");
            HashMap::new()
        })
}

/// Each source's rows by bucket: (group, source) to bucket start to rows.
type History = senders::Sources;

/// Each source's rows in each [`BUCKET`] from `since_ms`.
pub fn rows_by_bucket(store: &RegistryStore, since_ms: i64) -> Result<History, super::store::StoreError> {
    history(store, BUCKET.and_finer(), BUCKET, since_ms)
}

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

/// `group`'s `source`'s range, as rows per second, for each point of a chart drawn in points `point_ms` wide
/// ([`super::series::chart_point_ms`]), from the point `since_ms` is in to the point `now_ms` is in. A point's range
/// is the one judged at the widest width no wider than it, as of its hour. For a point finer than five minutes, it is
/// the source's swings at the point's own width ([`record_swings`]). A point where that width does not judge the
/// source has none. The range reads only past days, so the point still filling has one too.
pub fn expected_over(
    store: &RegistryStore,
    group: &str,
    source: &str,
    since_ms: i64,
    now_ms: i64,
    point_ms: i64,
    learning: &Learning,
) -> Result<Vec<ExpectedPoint>, super::store::StoreError> {
    if point_ms < BUCKET.bucket_ms() {
        return swung_over(store, group, source, since_ms, now_ms, point_ms, learning);
    }
    let width = WIDTHS
        .into_iter()
        .rev()
        .find(|w| w.ms() <= point_ms)
        .unwrap_or(WIDTHS[0]);
    let step = point_ms;
    let first = since_ms.div_euclid(step) * step + step;
    let last = now_ms.div_euclid(step) * step + step;
    let oldest = Tier::OneHour.align(first) - lookback_ms();
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

/// [`expected_over`] for a chart finer than [`BUCKET`]: each bucket's range is how far the source's rate at
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
    let across = NEIGHBORHOOD.as_millis() as i64 / width;
    let first = BUCKET.align(since_ms);
    let oldest = first - USUAL_DAYS * DAY_MS - across * width;
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
        if let Some(band) = swung_band(held, ended_ms, width, across) {
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

/// The range of `held` at `ended_ms` on each past day, `across` steps of `step_ms` either side, or `None` while fewer
/// than [`MIN_DAYS`] days have one. `held` is one past bucket's swing, its low and high rate ([`swing`]), if it is
/// known. The range cuts a [`TAIL`] from the bottom of the lows and from the top of the highs.
fn swung_band(held: impl Fn(i64) -> Option<Band>, ended_ms: i64, step_ms: i64, across: i64) -> Option<Band> {
    let (mut lows, mut highs) = (Vec::new(), Vec::new());
    let mut days = HashSet::new();
    for day in 1..=USUAL_DAYS {
        for step in -across..=across {
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
/// ([`super::series::chart_point_ms`]).
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
        tracing::warn!(name: "swings_not_recorded", target: "status", error = %e, "could not record how far sources' rates swung, so their charts' ranges miss these buckets");
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
/// restart left nearly empty would otherwise set the bucket's low, and with it the range at that time of day all week.
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

/// Every source's last window against its normal, off the async runtime. The marks are read first
/// ([`Learning::read`]).
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

    /// The reading at `ended`, against what was learned as of its hour.
    fn reading_at(store: &RegistryStore, ended: i64, learning: &Learning) -> Option<Reading> {
        let learned = learn(store, Tier::OneHour.align(ended), learning, None);
        compute(store, ended, learning, &learned).into_iter().next()
    }

    fn reading(store: &RegistryStore) -> Option<Reading> {
        reading_at(store, END, &Learning::default())
    }

    fn windows(r: &Reading) -> Vec<u64> {
        r.judged.iter().map(|j| j.window.as_secs() / 60).collect()
    }

    #[test]
    fn a_steady_source_is_judged_within_its_range_at_every_width() {
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

    /// `rows(bucket)` sent by the `devname` `value` of `g`'s `s`, in each bucket [`store_with`] reads its rows in.
    fn send(store: &RegistryStore, days: i64, value: &str, rows: impl Fn(i64) -> Option<u64>) {
        use crate::heartbeat::{SenderCounts, SenderRows, StreamReport};
        let mut at = END - (days + 1) * DAY_MS;
        while at < END {
            if let Some(rows) = rows(at) {
                let counts = SenderCounts {
                    source: "s".into(),
                    field: "devname".into(),
                    values: vec![SenderRows {
                        value: value.into(),
                        rows,
                    }],
                    too_many: false,
                };
                let report = StreamReport {
                    senders: vec![counts],
                    ..Default::default()
                };
                store.record_streams("g", &report, true, None, at).unwrap();
            }
            at += FIVE;
        }
    }

    /// Each sender's reading at `END`, by its value.
    fn sender_readings_at_end(store: &RegistryStore) -> BTreeMap<String, Option<Outside>> {
        let learning = Learning::default();
        let learned = learn(store, END, &learning, None);
        compute(store, END, &learning, &learned)
            .into_iter()
            .filter_map(|r| Some((r.sender?.value, r.outside)))
            .collect()
    }

    /// A sender that stops sending reads nothing where its source was read, and is below on its fourth reading,
    /// though its source is not; one that keeps sending is within its range.
    #[test]
    fn a_sender_that_stops_is_below_though_its_source_is_not() {
        let stopped = |at: i64| at >= END - 4 * FIVE;
        let store = store_with(7, |at| Some(if stopped(at) { 90.0 } else { 100.0 }));
        send(&store, 7, "fw-01", |at| (!stopped(at)).then_some(10));
        send(&store, 7, "fw-07", |_| Some(90));
        let read = sender_readings_at_end(&store);
        assert_eq!(read["fw-01"], Some(Outside::Below));
        assert_eq!(read["fw-07"], None);
    }

    /// When the whole source stops, its own warning says so: none of its senders is judged on that reading.
    #[test]
    fn a_sender_is_not_judged_on_the_reading_its_source_is_below() {
        let stopped = |at: i64| at >= END - 4 * FIVE;
        let store = store_with(7, |at| Some(if stopped(at) { 0.0 } else { 100.0 }));
        send(&store, 7, "fw-01", |at| (!stopped(at)).then_some(10));
        send(&store, 7, "fw-07", |at| (!stopped(at)).then_some(90));
        assert_eq!(sender_readings_at_end(&store), BTreeMap::new());
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

    /// Days are counted per time of day: the windows in the half hour after a midnight are that midnight's day.
    #[test]
    fn too_few_days_are_not_judged() {
        let days = MIN_DAYS as i64 - 1;
        let since = |start: i64| move |at: i64| (at >= start).then_some(100.0);
        assert_eq!(reading(&store_with(days, since(END - days * DAY_MS + 6 * FIVE))), None);
        assert!(reading(&store_with(days, since(END - days * DAY_MS + 5 * FIVE))).is_some());
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
            ..Learning::default()
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
    fn a_stop_does_not_teach_the_range() {
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
    fn a_window_that_holds_a_stop_is_not_judged() {
        let (store, _) = outage_and_stop();
        warned(&store, "source_unreachable", END - 4 * FIVE, Some(END - FIVE));
        let r = reading(&store).unwrap();
        assert_eq!(windows(&r), [5], "only the five minutes since the stop ended");
        assert_eq!(r.outside, None, "and the three before it held the stop");
    }

    /// Whether the normal as of END learns the `width` window that ended at `ended`.
    fn learns(store: &RegistryStore, ended: i64, width: Width) -> bool {
        let warned = Warned::of(store, "g", "s", END - lookback_ms(), &Learning::default());
        !warned.during(ended, width.ms())
    }

    #[test]
    fn a_width_learns_no_window_that_shares_a_bucket_with_one_a_below_warning_read() {
        let (store, outage) = outage_and_stop();
        // Raised on the outage's fourth reading, and seen a heartbeat later.
        let ended = outage + 2 * HOUR;
        warned(&store, "source_rows_below", outage + 5 * FIVE, Some(ended));
        for width in WIDTHS {
            let w = width.ms();
            assert!(learns(&store, outage + FIVE - w, width), "{width:?}: the window before");
            assert!(
                !learns(&store, outage + 2 * FIVE - w, width),
                "{width:?}: the first that shares a bucket with the first reading it waited for"
            );
            assert!(
                !learns(&store, ended + w - FIVE, width),
                "{width:?}: the last that shares a bucket with the warning"
            );
            assert!(learns(&store, ended + w, width), "{width:?}: the window after");
        }
        assert_eq!(reading(&store).unwrap().judged[0].band.low, 70.0);
    }

    #[test]
    fn an_above_warning_teaches_the_range() {
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
    fn a_held_below_warning_is_judged_against_the_days_learned_before_it() {
        let dropped = END - 6 * DAY_MS;
        let store = store_with(29, |at| Some(if at >= dropped { 30.0 } else { 100.0 }));
        warned(&store, "source_rows_below", dropped + 4 * FIVE, None);
        let r = reading(&store).unwrap();
        assert_eq!(r.outside, Some(Outside::Below));
        assert_eq!(
            (r.judged[0].rows, r.judged[0].band),
            (30.0, Band { low: 70.0, high: 130.0 }),
            "six days not learned are passed over, so the week before them is still the normal"
        );
    }

    #[test]
    fn a_held_below_warning_ends_back_at_the_normal_from_before_it() {
        let dropped = END - 6 * DAY_MS;
        // Back for a day and an hour, so even the 24-hour window reads the old level.
        let store = store_with(29, |at| {
            Some(if (dropped..END - 25 * HOUR).contains(&at) {
                30.0
            } else {
                100.0
            })
        });
        warned(&store, "source_rows_below", dropped + 4 * FIVE, None);
        assert_eq!(reading(&store).unwrap().outside, None);
    }

    #[test]
    fn a_held_below_warning_marked_normal_teaches_its_level_and_ends() {
        let dropped = END - 6 * DAY_MS;
        let store = store_with(29, |at| Some(if at >= dropped { 30.0 } else { 100.0 }));
        warned(&store, "source_rows_below", dropped + 4 * FIVE, None);
        let held = past(&store, "g", "s", END, &Learning::default()).unwrap().warnings[0].clone();
        assert_eq!((held.from_millis, held.to_millis, held.learned), (dropped, END, false));
        let normal = Learning {
            periods: vec![marks::Period {
                group: "g".into(),
                source: "s".into(),
                from_millis: held.from_millis,
                to_millis: held.to_millis,
                was: marks::Was::Normal,
                by: "John".into(),
            }],
            ..Learning::default()
        };
        assert_eq!(reading_at(&store, END, &normal).unwrap().outside, None);
        assert!(
            past(&store, "g", "s", END + HOUR, &normal).unwrap().warnings[0].learned,
            "the warning is learned whole, including the windows after it was marked"
        );
    }

    #[test]
    fn a_reset_ends_a_held_below_warning() {
        let store = store_with(7, |at| Some(if at >= END - DAY_MS { 0.0 } else { 100.0 }));
        warned(&store, "source_rows_below", END - DAY_MS + 3 * FIVE, None);
        assert_eq!(reading_at(&store, END, &reset_at(END - HOUR)), None);
    }

    #[test]
    fn a_below_warning_ends_on_a_reading_inside_the_range() {
        let store = store_with(7, |_| Some(100.0));
        warned(&store, "source_rows_below", END - HOUR, None);
        assert_eq!(reading(&store).unwrap().outside, None);
    }

    #[test]
    fn a_below_warning_holds_through_a_reading_no_width_could_judge() {
        // Stopped, with nothing reported for the third bucket back: no width has four readings in a row.
        let store = store_with(7, |at| match at {
            _ if at == END - 3 * FIVE => None,
            _ if at >= END - 4 * FIVE => Some(0.0),
            _ => Some(100.0),
        });
        assert_eq!(reading(&store).unwrap().outside, None, "never raised");
        warned(&store, "source_rows_below", END - 6 * FIVE, None);
        let r = reading(&store).unwrap();
        assert_eq!(windows(&r), [5]);
        assert_eq!(r.outside, Some(Outside::Below));
        assert_eq!(r.shown().unwrap().window, WIDTHS[0].window);
    }

    #[test]
    fn two_days_either_side_of_the_hour_are_two_days() {
        // Read only around this time of day on the days before, and in the bucket judged now.
        let near = |at: i64, days: i64| (1..=days).any(|d| (at - (END - d * DAY_MS)).abs() <= HOUR / 2);
        let store = store_with(7, move |at| (near(at, 2) || at == END - FIVE).then_some(100.0));
        assert_eq!(reading(&store), None);
        let store = store_with(7, move |at| (near(at, 3) || at == END - FIVE).then_some(100.0));
        assert!(reading(&store).is_some());
    }

    #[test]
    fn a_fleet_whose_history_just_arrived_is_judged_at_its_next_reading() {
        let key = serde_json::to_string(&[("group", "g"), ("source", "s"), ("worker", "w")]).unwrap();
        let store = store_with(0, |at| (at >= END - HOUR).then_some(100.0));
        let readings = Readings::default();
        let none = Learning::default();
        assert!(
            readings
                .at(&store, END + 60_000, Duration::from_secs(1), &none)
                .is_empty()
        );
        let mut at = END - 8 * DAY_MS;
        while at <= END {
            if at < END - HOUR || at == END {
                store.record_delta(ROWS_IN_METRIC, &key, at, 100.0).unwrap();
            }
            at += FIVE;
        }
        assert_eq!(
            readings
                .at(&store, END + FIVE + 60_000, Duration::from_secs(1), &none)
                .len(),
            1
        );
    }

    fn expected(store: &RegistryStore, since_ms: i64, point_ms: i64) -> Vec<ExpectedPoint> {
        expected_over(store, "g", "s", since_ms, END + 1000, point_ms, &Learning::default()).unwrap()
    }

    #[test]
    fn a_short_chart_has_the_five_minute_range_in_each_five_minutes() {
        let store = store_with(7, |_| Some(100.0));
        let points = expected(&store, END - HOUR, FIVE);
        assert_eq!(points.len(), 13, "the hour's twelve buckets and the one still filling");
        assert_eq!(points[0].from_millis, END - HOUR);
        assert_eq!((points[12].from_millis, points[12].to_millis), (END, END + FIVE));
        for p in &points {
            assert_eq!((p.low, p.high), (70.0 / 300.0, 130.0 / 300.0));
        }
    }

    /// The first days of a week's chart have fewer than [`MIN_DAYS`] days of history before them.
    #[test]
    fn a_long_chart_has_its_widest_widths_range_each_point_and_none_where_history_is_short() {
        let store = store_with(7, |_| Some(100.0));
        for (point, width) in [(HOUR, 30 * 60), (6 * HOUR, 6 * 60 * 60)] {
            let points = expected(&store, END - 7 * DAY_MS, point);
            let first = points.first().unwrap().from_millis;
            assert!((END - 6 * DAY_MS - HOUR..END - 4 * DAY_MS).contains(&first), "{first}");
            assert_eq!(points.last().unwrap().from_millis, END);
            assert!(points.windows(2).all(|w| w[0].to_millis == w[1].from_millis));
            // A hundred rows in each five minutes.
            let usual = width as f64 / 300.0 * 100.0;
            for p in &points {
                assert_eq!(
                    (p.low, p.high),
                    (0.7 * usual / width as f64, 1.3 * usual / width as f64),
                    "at {}",
                    p.from_millis
                );
            }
        }
    }

    #[test]
    fn a_chart_range_leaves_out_a_stop() {
        let (store, outage) = outage_and_stop();
        warned(&store, "source_unreachable", outage, Some(outage + 2 * HOUR));
        for p in expected(&store, END - HOUR, FIVE) {
            assert_eq!(p.low, 70.0 / 300.0, "at {}", p.from_millis);
        }
    }

    #[test]
    fn a_source_never_read_has_no_range() {
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
    fn a_fine_chart_has_the_range_its_points_swung_in() {
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

    #[test]
    fn a_quiet_spell_is_a_run_of_reported_buckets_with_no_rows_between_two_with_rows() {
        // Rows, three empty, rows, one nobody reported, one empty, rows.
        let of: BTreeMap<i64, f64> = [(0, 1.0), (1, 0.0), (2, 0.0), (3, 0.0), (4, 2.0), (6, 0.0), (7, 1.0)]
            .into_iter()
            .map(|(bucket, rows)| (bucket * FIVE, rows))
            .collect();
        let first = Watch {
            watched_ms: 0,
            read_ms: 0,
            row_ms: None,
            quiet_ms: 0,
        };
        let mut ended = Vec::new();
        let partway = watched(first, &of, 6 * FIVE, &mut ended);
        assert_eq!(ended, [(4 * FIVE, 3 * FIVE)]);
        assert_eq!(
            (partway.row_ms, partway.quiet_ms),
            (Some(5 * FIVE), 6 * FIVE),
            "the quiet spell under way began after the bucket nobody reported"
        );
        let watch = watched(partway, &of, 8 * FIVE, &mut ended);
        assert_eq!(ended.len(), 1, "a run a bucket nobody reported broke is no quiet spell");
        assert_eq!(
            watch,
            Watch {
                watched_ms: 0,
                read_ms: 8 * FIVE,
                row_ms: Some(8 * FIVE),
                quiet_ms: 8 * FIVE,
            }
        );
    }

    /// A row every three hours, too few for any count width, from `days` days before END until `stop`.
    fn sparse(days: i64, stop: i64) -> RegistryStore {
        store_with(days, |at| {
            Some(if at % (3 * HOUR) == 0 && at < stop { 1.0 } else { 0.0 })
        })
    }

    #[test]
    fn a_source_quiet_longer_than_its_longest_learned_spell_is_below_normal() {
        let r = reading(&sparse(35, END)).unwrap();
        assert_eq!(r.judged, [], "no count width judges a source this sparse");
        let gap = r.gap.unwrap();
        assert_eq!(
            gap.longest_ms,
            35 * FIVE,
            "the longest quiet spell is the three hours between rows, less the bucket with one"
        );
        assert_eq!(
            gap.silence_ms, gap.longest_ms,
            "the quiet spell under way began with the last row, three hours ago"
        );
        // Watched 29 days, the longest is lengthened by m * ln(365 / 29), and m is half the longest, every spell being
        // equal.
        assert_eq!(
            gap.edge_ms,
            (1.5 * 35.0 * FIVE as f64 * (1.0 + 0.5 * (365.0f64 / 29.0).ln())) as i64
        );
        assert_eq!(r.outside, None);

        let r = reading(&sparse(35, END - 12 * HOUR)).unwrap();
        assert_eq!(r.gap.unwrap().silence_ms, 15 * HOUR - FIVE);
        assert_eq!(r.outside, Some(Outside::Below));
    }

    #[test]
    fn the_gap_judges_a_source_watched_four_weeks() {
        let silent = END - 12 * HOUR;
        assert_eq!(reading(&sparse(26, silent)), None, "watched 27 days, it is not judged");
        assert!(
            reading(&sparse(28, silent)).unwrap().gap.is_some(),
            "watched 29 days, it is"
        );
        assert_eq!(
            reading_at(&sparse(35, silent), END, &reset_at(END - 27 * DAY_MS)),
            None,
            "the gap does not judge a source reset less than four weeks ago"
        );
        let back = sparse(26, silent);
        let watch = Watch {
            watched_ms: 0,
            read_ms: HOUR,
            row_ms: Some(0),
            quiet_ms: 0,
        };
        back.record_silences(&[(("g".into(), "s".into()), watch)], &[]).unwrap();
        assert_eq!(
            reading(&back),
            None,
            "a source back after being gone longer than the buckets are held is watched afresh"
        );
    }

    #[test]
    fn the_gap_counts_a_quiet_spell_from_a_stops_end() {
        let store = sparse(35, END - 12 * HOUR);
        warned(&store, "source_unreachable", END - 3 * HOUR, Some(END - HOUR));
        assert_eq!(reading(&store).unwrap().gap.unwrap().silence_ms, HOUR);
        warned(&store, "source_blocked", END - HOUR, None);
        assert_eq!(
            reading(&store).and_then(|r| r.gap),
            None,
            "the gap does not read while a stop holds"
        );
    }

    #[test]
    fn a_quiet_spell_that_warned_is_not_learned_unless_an_editor_says_it_was_normal() {
        // Every quiet spell lasts three hours but one two weeks ago, which lasts nine.
        let long = END - 14 * DAY_MS;
        let store = store_with(35, |at| {
            Some(if at % (3 * HOUR) == 0 && !(long..long + 6 * HOUR).contains(&at) {
                1.0
            } else {
                0.0
            })
        });
        let longest = |store: &RegistryStore, learning: &Learning| {
            reading_at(store, END, learning).unwrap().gap.unwrap().longest_ms
        };
        assert_eq!(longest(&store, &Learning::default()), 107 * FIVE);
        let (began, ended) = (long - 3 * HOUR + FIVE, long + 6 * HOUR + FIVE);
        warned(&store, "source_rows_below", long + 3 * HOUR, Some(ended));
        assert_eq!(longest(&store, &Learning::default()), 35 * FIVE);
        let normal = Learning {
            periods: vec![marks::Period {
                group: "g".into(),
                source: "s".into(),
                from_millis: began,
                to_millis: ended,
                was: marks::Was::Normal,
                by: "John".into(),
            }],
            ..Learning::default()
        };
        assert_eq!(longest(&store, &normal), 107 * FIVE);
    }

    #[test]
    fn a_warning_the_gap_raised_ends_at_the_first_row() {
        let silent = END - 12 * HOUR;
        let store = sparse(35, silent);
        warned(&store, "source_rows_below", END - HOUR, None);
        let key = serde_json::to_string(&[("group", "g"), ("source", "s"), ("worker", "w")]).unwrap();
        store.record_delta(ROWS_IN_METRIC, &key, END, 1.0).unwrap();
        let r = reading_at(&store, END + FIVE, &Learning::default()).unwrap();
        assert_eq!(r.gap.unwrap().silence_ms, 0);
        assert_eq!(r.outside, None);
        store.record_delta(ROWS_IN_METRIC, &key, END + FIVE, 1.0).unwrap();
        assert_eq!(
            reading_at(&store, END + 2 * FIVE, &Learning::default())
                .unwrap()
                .outside,
            None,
            "the warning ends at a later reading, though the source's last row ended no quiet spell"
        );

        // Neither a bucket nobody reported nor a stop's end is a row: the warning holds through each.
        let held = |store: &RegistryStore| {
            warned(store, "source_rows_below", END - HOUR, None);
            reading_at(store, END, &Learning::default()).unwrap().outside
        };
        let skipped = store_with(35, |at| {
            (at != END - FIVE).then_some(if at % (3 * HOUR) == 0 && at < silent { 1.0 } else { 0.0 })
        });
        assert_eq!(held(&skipped), Some(Outside::Below));
        let stopped = sparse(35, silent);
        warned(&stopped, "source_unreachable", END - 2 * HOUR, Some(END - HOUR));
        assert_eq!(held(&stopped), Some(Outside::Below));
    }

    #[test]
    fn a_warning_the_gap_raised_ends_at_the_first_row_after_days_quiet() {
        // A row every three days until twelve days ago, and a warning the gap holds.
        let quiet = || {
            let store = store_with(35, |at| {
                Some(if at % (3 * DAY_MS) == 0 && at < END - 12 * DAY_MS {
                    1.0
                } else {
                    0.0
                })
            });
            warned(&store, "source_rows_below", END - HOUR, None);
            let gap = reading_at(&store, END - FIVE, &Learning::default())
                .unwrap()
                .gap
                .unwrap();
            assert!(
                gap.below() && gap.edge_ms > DAY_MS,
                "the source may be quiet longer than a day"
            );
            store
        };
        let key = serde_json::to_string(&[("group", "g"), ("source", "s"), ("worker", "w")]).unwrap();
        let read = quiet();
        read.record_delta(ROWS_IN_METRIC, &key, END, 1.0).unwrap();
        assert_eq!(
            reading_at(&read, END + FIVE, &Learning::default()).unwrap().outside,
            None,
            "the reading after the row ends it"
        );
        let missed = quiet();
        missed.record_delta(ROWS_IN_METRIC, &key, END - FIVE, 1.0).unwrap();
        missed.record_delta(ROWS_IN_METRIC, &key, END, 1.0).unwrap();
        assert_eq!(
            reading_at(&missed, END + FIVE, &Learning::default()).unwrap().outside,
            None,
            "the warning ends at a reading in the next hour, though no reading in the row's hour came after the row"
        );
    }

    #[test]
    fn a_warning_the_gap_did_not_read_below_holds_through_a_row() {
        let store = sparse(35, END);
        warned(&store, "source_rows_below", END - 5 * HOUR, None);
        let r = reading(&store).unwrap();
        assert!(!r.gap.unwrap().below());
        assert_eq!(
            r.outside,
            Some(Outside::Below),
            "the row three hours ago ended a quiet spell the source may make"
        );
    }

    #[test]
    fn a_period_marked_an_outage_is_not_learned_and_one_marked_normal_is() {
        let (store, outage) = outage_and_stop();
        let marked = |was| Learning {
            periods: vec![marks::Period {
                group: "g".into(),
                source: "s".into(),
                from_millis: outage,
                to_millis: outage + 2 * HOUR,
                was,
                by: "John".into(),
            }],
            ..Learning::default()
        };
        let low = |learning: &Learning| reading_at(&store, END, learning).unwrap().judged[0].band.low;
        assert_eq!(low(&marked(marks::Was::Outage)), 70.0);
        warned(&store, "source_unreachable", outage, Some(outage + 2 * HOUR));
        assert_eq!(low(&marked(marks::Was::Normal)), 0.0);
    }

    #[test]
    fn a_span_less_periods_keeps_what_lies_outside_them() {
        let spans = vec![(10, Some(20)), (30, None)];
        assert_eq!(less(spans.clone(), &[]), spans);
        assert_eq!(less(spans.clone(), &[(15, 35)]), [(10, Some(15)), (35, None)]);
        assert_eq!(less(spans.clone(), &[(0, 20), (40, 50)]), [(30, Some(40)), (50, None)]);
        assert_eq!(less(spans, &[(12, 14)])[..2], [(10, Some(12)), (14, Some(20))]);
    }

    #[test]
    fn past_lists_the_warnings_and_the_longest_quiet_spells_with_what_is_learned() {
        let store = sparse(35, END - 12 * HOUR);
        reading(&store);
        warned(&store, "source_rows_below", END - 9 * DAY_MS, Some(END - 8 * DAY_MS));
        warned(&store, "source_rows_below", END - HOUR, None);
        let past = past(&store, "g", "s", END, &Learning::default()).unwrap();
        let span = |from_millis, to_millis| PastSpan {
            from_millis,
            to_millis,
            learned: false,
        };
        assert_eq!(
            past.warnings,
            [
                span(END - HOUR - 4 * FIVE, END),
                span(END - 9 * DAY_MS - 4 * FIVE, END - 8 * DAY_MS)
            ],
            "the warning that holds is listed first, and runs until now"
        );
        assert_eq!(past.silences.len(), LONGEST_LISTED);
        assert!(past.silences.iter().all(|s| s.to_millis - s.from_millis == 35 * FIVE));
        let newest = &past.silences[0];
        assert_eq!(
            newest.to_millis,
            END - 15 * HOUR,
            "of quiet spells of equal length, the newest is listed first"
        );
        assert!(newest.learned);
        assert!(
            !past
                .silences
                .iter()
                .any(|s| (END - 9 * DAY_MS..END - 8 * DAY_MS).contains(&s.to_millis) && s.learned),
            "a quiet spell under the warning is not learned"
        );
    }
}
