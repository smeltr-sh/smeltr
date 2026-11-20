//! What each source normally reads, and whether it is reading that now.
//!
//! A source's normal is learned from the rows it read, which the registry already keeps (`series.rs`), and it is judged
//! at each of four [`WIDTHS`]: the rows of the last 5 minutes, 30 minutes, 6 hours and 24 hours. At one width, the
//! usual window of a time of day is the median of the windows that ended within [`NEIGHBORHOOD`] of it on each of the
//! newest [`USUAL_DAYS`] days of its kind learned there. Each window of the width's history, its newest learned windows, is taken
//! as a ratio to the usual window of its time of day. The range is the usual window scaled by the lowest and the
//! highest ratios, a [`TAIL`] of them cut from each end, and widened by its [`Preset`]'s margin. Ratios, because a
//! quiet hour and a busy one can be pooled only once each is set against its own time of day; a quantile, because rows
//! read are bursty and often zero, which a mean and a deviation do not describe.
//!
//! A source's days start at its quietest hour, and a source whose rows differ by weekday tells its days apart by kind
//! ([`week`]). A window that reaches into the day before is set against days of today's kind that followed a day of
//! that day's kind.
//!
//! A width judges a source only where its usual window holds at least [`GATE`] rows: a window that is usually near
//! empty has a low edge of zero, which no reading is below. So a source that reads every few seconds is judged at every
//! width, and one that reads a few rows an hour only at the long ones.
//!
//! Two waits keep a source from being judged on too little. The source as a whole waits until its rows reach back
//! [`JUDGED_AFTER_MS`], a week, so every weekday has been learned once before any can warn. Each time of day then waits
//! until its usual window has [`USUAL_MIN_DAYS`] days of its kind learned there, so one odd day is not all it knows.
//!
//! A source too sparse for any of them still has a normal in time per row, and a fifth width, the gap, judges it there:
//! how long it has gone without a row, against the longest quiet spell it made in the past year ([`Gap`]). A quiet
//! spell is a run of buckets with no rows, each reported by a worker, between two with rows (`series::Silence`). The
//! spells are recorded each hour and kept a year, since the buckets are kept only four weeks. The gap judges a source
//! once it has been watched [`GAP_WATCH_MS`]. Until it has been watched a year, its longest spell is taken to be
//! shorter than the year's longest, and is lengthened by what the rest of the year would likely add.
//!
//! A source is below normal while some width has read below its range for as many readings in a row as its [`Preset`]
//! requires, since one reading at the range's edge flickers, or once its quiet spell is longer than [`GAP_MARGIN`]
//! times the longest it learned. Either raises the same warning, and it stays until some width judges the source and
//! none reads it below its range. Days not learned are passed over, not counted, so a held warning is judged against
//! the days before it until those are no longer kept, after which no width judges the source and it still warns. Where
//! no count width judges the source, a warning ends at the first row after it began that ends a quiet spell the gap
//! read below. Above normal is judged at [`WINDOW`] alone, and only informs.
//!
//! Only windows the source was running for throughout count. A bucket with no point means no worker reported the source
//! then -- not deployed, or a worker gone -- which says nothing about how much it reads.
//!
//! A past window does not count when it shares a bucket with a stop about the source (`warnings::spans`), or with a
//! window of its width that ended while a below-normal warning held or waited to be raised. A quiet spell does not
//! count when it shares a bucket with a stop or with such a warning. An outage at this time of day would otherwise set
//! the range's low edge there, and a source that dropped would come to call the drop normal. An editor may say a
//! below-normal warning was normal, and it is learned whole (`marks::Marks::answers`).
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

use super::marks::Marks;
use super::senders::{self, Pick, Sender, SenderKey};
use super::series::{RegistryStore, Silence, SubjectPoints, Swing, Tier, Watch, YEAR_MS};
use super::warnings::{self, Condition, SubjectKind, label};
use super::{AppState, ROWS_IN_METRIC, run_store};

mod week;

use week::{Week, Weekly};

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

/// The fewest days a time of day's usual window is learned from. With fewer, that time of day is not judged.
const USUAL_MIN_DAYS: usize = 3;

/// How far back a source's rows must reach, from its first or from a reset, before it is judged at all: a week, so each
/// weekday is learned once before one can warn. Judged sooner, a new source's first quiet weekend is set against its
/// weekdays, warns, and so is never learned.
pub const JUDGED_AFTER_MS: i64 = 7 * DAY_MS;

/// How far either side of the same time of day a past window may end and still count as the same time of day.
const NEIGHBORHOOD: Duration = Duration::from_secs(30 * 60);

/// The share of past ratios each side of the range leaves out: one in two thousand.
const TAIL: f64 = 0.0005;

/// How a source's normal trades how soon it sees a drop against how often a dip that is not a drop warns: how much wider
/// than its learned spread the range is, and how many readings in a row below it warn. Each is a point the detector was
/// measured at (`docs/research/normal-detector/paper.md`). A stop reads below any range, so only the wait sets how soon
/// one is seen. An editor picks one for a source and one for its senders (`marks::Picked`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Preset {
    Fast,
    Sensitive,
    Balanced,
    Quiet,
    /// Judged as at Balanced, so its range is drawn and it is ranked, but it never warns or informs.
    Off,
}

/// The preset a source is judged at, and the one its senders are.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Presets {
    pub rows: Preset,
    pub senders: Preset,
}

impl Presets {
    /// The preset a source is judged at, or one of its senders where there is a `sender`.
    pub fn of(self, sender: Option<&Sender>) -> Preset {
        match sender {
            Some(_) => self.senders,
            None => self.rows,
        }
    }
}

impl Default for Presets {
    /// Until an editor picks others. Each sender warns on its own, so a source with many senders has that many chances
    /// for a dip to warn.
    fn default() -> Self {
        Presets {
            rows: Preset::Balanced,
            senders: Preset::Quiet,
        }
    }
}

impl Preset {
    /// The presets that warn.
    const ALL: [Preset; 4] = [Preset::Fast, Preset::Sensitive, Preset::Balanced, Preset::Quiet];

    /// How much wider than the ratios the range is, as a share of each edge.
    fn margin(self) -> f64 {
        match self {
            Preset::Fast | Preset::Quiet => 0.5,
            Preset::Sensitive => 0.2,
            Preset::Balanced | Preset::Off => 0.3,
        }
    }

    /// Readings in a row outside the range before a source is outside its normal.
    fn wait(self) -> i64 {
        match self {
            Preset::Fast => 1,
            Preset::Sensitive => 3,
            Preset::Balanced | Preset::Off => 4,
            Preset::Quiet => 6,
        }
    }

    /// Whether a reading outside the range at this preset warns, or informs.
    pub(crate) fn warns(self) -> bool {
        self != Preset::Off
    }

    /// The most readings any preset waits.
    fn longest_wait() -> i64 {
        Preset::ALL.into_iter().map(Preset::wait).max().unwrap_or(1)
    }
}

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
    /// This reading and those before it its [`Preset`] waits for were each below their range.
    pub below: bool,
    /// This reading and those before it its [`Preset`] waits for were each above their range, at [`WINDOW`], the one
    /// width above normal is judged at.
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

/// Why a below-normal warning began, as the warning keeps it (`RegistryStore::keep_reasons`).
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", tag = "kind")]
pub enum Reason {
    /// Its rows per second over the width `window_millis` long that warned, against that width's range then.
    #[serde(rename_all = "camelCase")]
    Width {
        window_millis: i64,
        per_second: f64,
        low: f64,
        high: f64,
    },
    /// It went `silence_millis` without a row, longer than the `edge_millis` it may.
    #[serde(rename_all = "camelCase")]
    Gap { silence_millis: i64, edge_millis: i64 },
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

    /// Why it is below normal: the narrowest width that waited out its readings below its range, else the narrowest
    /// below it now, else the gap. None while it is not below, or is held below while nothing judges it.
    pub fn reason(&self) -> Option<Reason> {
        if self.outside != Some(Outside::Below) {
            return None;
        }
        let width = self
            .judged
            .iter()
            .find(|j| j.below)
            .or_else(|| self.judged.iter().find(|j| j.under()));
        match (width, self.gap.filter(Gap::below)) {
            (Some(j), _) => {
                let (per_second, band) = j.per_second();
                Some(Reason::Width {
                    window_millis: j.window.as_millis() as i64,
                    per_second,
                    low: band.low,
                    high: band.high,
                })
            }
            (None, Some(gap)) => Some(Reason::Gap {
                silence_millis: gap.silence_ms,
                edge_millis: gap.edge_ms,
            }),
            (None, None) => None,
        }
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
/// time; the rest wait for it.
#[derive(Default)]
pub struct Readings {
    last: Mutex<Option<Answer>>,
    learned: Mutex<Option<(i64, Marks, Arc<Learned>)>>,
    /// The senders picked as of the day it names, read again each day.
    pick: Mutex<Option<Arc<Pick>>>,
    computing: Mutex<()>,
}

/// A source, by its group and id.
type SourceKey = (String, String);

type Answer = (i64, Marks, Arc<Vec<Reading>>);

impl Readings {
    /// Every source with enough history to judge, as of `now_ms`. Waits for every heartbeat sent before
    /// `now_ms - heartbeat_interval` to land in the window it judges.
    ///
    /// While another caller computes the new answer, this waits for it: a catalog given the last answer would be
    /// newer than the one given the new answer, and would end every issue the new answer had just started.
    pub fn at(
        &self,
        store: &RegistryStore,
        now_ms: i64,
        heartbeat_interval: Duration,
        learning: &Marks,
    ) -> Arc<Vec<Reading>> {
        let ended_ms = ended(now_ms, heartbeat_interval);
        if let Some(fresh) = self.fresh(ended_ms, learning) {
            return fresh;
        }
        let _computing = self.computing.lock().unwrap_or_else(|e| e.into_inner());
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
    fn fresh(&self, ended_ms: i64, learning: &Marks) -> Option<Arc<Vec<Reading>>> {
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
    fn learned_at(&self, store: &RegistryStore, ended_ms: i64, learning: &Marks) -> Arc<Learned> {
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

/// One width's normal as of one time: the usual window at each time of each kind of day, and the lowest and highest
/// ratios to it, judged at `preset`.
#[derive(Debug, Clone, PartialEq)]
struct Normal {
    width: Width,
    preset: Preset,
    usual: Weekly,
    low: f64,
    high: f64,
}

impl Normal {
    /// The range of the window ending at `ended_ms`, or None where this width does not judge the source.
    fn band(&self, ended_ms: i64) -> Option<Band> {
        let usual = self.usual.at(ended_ms).filter(|&t| t >= GATE)?;
        let margin = self.preset.margin();
        Some(Band {
            low: (1.0 - margin) * ((usual + 1.0) * self.low - 1.0).max(0.0),
            high: (1.0 + margin) * ((usual + 1.0) * self.high - 1.0),
        })
    }
}

/// `width`'s normal at `preset` as of `as_of`, from the windows learned that ended before it, or None while it has no
/// ratio at all.
/// Its history is its newest learned windows, as many as fit in [`Width::history_days`] days, and a time of day's usual
/// window is learned from the newest [`USUAL_DAYS`] days of its kind in `week` with a learned window near it
/// ([`Weekly`]). Both reach back past what is not learned, as far as [`lookback_ms`], so a warning that holds for days
/// is still judged against the days before it. Each window's usual window is that of its time of day as of `as_of`, so
/// every window of the history is set against the same days. Each window and its usual window get one more row, so a
/// time of day that is usually empty divides like any other.
fn normal(rows: &Rows, warned: &Warned, as_of: i64, width: Width, preset: Preset, week: &Week) -> Option<Normal> {
    let step = BUCKET.bucket_ms();
    let per_day = DAY_MS / step;
    // Each learned window, newest first, with how many buckets before `as_of` it ended. A window is learned once a
    // whole width, and the readings in a row a warning waits for at `preset`, have passed since it ended: until then a
    // window it shares a bucket with may yet warn and leave it unlearned (`Warned::during`). Learned sooner, a drop that
    // takes its width to show would teach the normal its own first hours and never fall under it. A window that
    // contains rows an editor said were normal waits only for those readings, so marking the warning that is up
    // teaches its level within the hour.
    let wait = preset.wait();
    let learned: Vec<(i64, f64)> = (wait..=lookback_ms() / step)
        .filter_map(|i| {
            let ended = as_of - i * step;
            let waited = i >= wait + width.buckets() || warned.said_normal(ended, width.ms());
            let sum = rows
                .window(ended, width)
                .filter(|_| waited && !warned.during(ended, width.ms()))?;
            Some((i, sum))
        })
        .collect();
    if learned
        .last()
        .is_none_or(|&(i, _)| i * step + width.ms() < JUDGED_AFTER_MS)
    {
        return None;
    }
    let usual = Weekly::learn(as_of, width, &learned, *week);
    let mut ratios: Vec<f64> = learned
        .iter()
        .take((width.history_days * per_day) as usize)
        .filter_map(|&(i, sum)| Some((sum + 1.0) / (usual.at(as_of - i * step)? + 1.0)))
        .collect();
    if ratios.is_empty() {
        return None;
    }
    ratios.sort_by(f64::total_cmp);
    Some(Normal {
        width,
        preset,
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
    /// How long `group`'s `source` may go without a row before the gap warns ([`Gap::edge_ms`]), while the gap judges
    /// it.
    pub fn quiet_limit_ms(&self, group: &str, source: &str) -> Option<i64> {
        let (_, edge_ms) = self.gaps.get(&(group.to_string(), source.to_string()))?.judged?;
        Some(edge_ms)
    }

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
fn learn(store: &RegistryStore, as_of: i64, learning: &Marks, pick: Option<Arc<Pick>>) -> Learned {
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
        _ => Arc::new(match senders::pick(store, day) {
            Ok(pick) => {
                if let Err(e) = senders::keep(store, &pick) {
                    tracing::warn!(name: "senders_not_dropped", target: "status", error = %e, "could not drop what the values not watched sent, so it is kept until a later pick drops it");
                }
                pick
            }
            Err(e) => {
                tracing::warn!(name: "senders_pick_unreadable", target: "status", error = %e, "could not read the senders' rows, so no sender is watched today");
                Pick {
                    day_ms: day,
                    ..Pick::default()
                }
            }
        }),
    };
    let watched = pick.watched(|group, source, field| learning.unchecked(group, source, field));
    let senders = sender_normals(store, &watched, oldest, as_of, learning).unwrap_or_else(|e| {
        tracing::warn!(name: "senders_learn_unreadable", target: "status", error = %e, "could not read the senders' rows, so no sender's normal is learned this hour");
        HashMap::new()
    });
    Learned {
        gaps: gaps(store, &buckets, oldest, as_of, learning),
        senders,
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

/// The normals of each of `watched`, from its rows since `oldest`, as of `as_of`. `watched` holds each source's
/// senders together, as [`Pick::watched`] gives them: they are read one source at a time, so only that source's rows
/// are held at once.
fn sender_normals(
    store: &RegistryStore,
    watched: &[SenderKey],
    oldest: i64,
    as_of: i64,
    learning: &Marks,
) -> Result<HashMap<SenderKey, Vec<Normal>>, super::store::StoreError> {
    let mut out = HashMap::new();
    for of_source in watched.chunk_by(|a, b| (&a.0, &a.1) == (&b.0, &b.1)) {
        let preset = learning.presets_of(&of_source[0].0, &of_source[0].1).senders;
        let history = senders::history(store, of_source, oldest)?;
        let warned = Warned::of_senders(store, history.keys(), oldest, learning);
        out.extend(history.into_iter().map(|(key, of)| {
            let rows = Rows::of(&of, oldest, as_of);
            let warned = &warned[&key];
            let week = Week::learn(&rows, warned, as_of);
            let normals = WIDTHS
                .iter()
                .filter_map(|&width| normal(&rows, warned, as_of, width, preset, &week))
                .collect();
            (key, normals)
        }));
    }
    Ok(out)
}

/// The normals of `key`'s rows `of`, read since `oldest`, as of `as_of`.
fn learned(
    store: &RegistryStore,
    key: &SourceKey,
    of: &BTreeMap<i64, f64>,
    oldest: i64,
    as_of: i64,
    learning: &Marks,
) -> Vec<Normal> {
    let rows = Rows::of(of, oldest, as_of);
    let warned = Warned::of(store, &key.0, &key.1, oldest, learning);
    let preset = learning.presets_of(&key.0, &key.1).rows;
    let week = Week::learn(&rows, &warned, as_of);
    WIDTHS
        .iter()
        .filter_map(|&width| normal(&rows, &warned, as_of, width, preset, &week))
        .collect()
}

/// [`Readings::at`] without the cache: each source's reading at `ended_ms` against what was `learned`.
fn compute(store: &RegistryStore, ended_ms: i64, learning: &Marks, learned: &Learned) -> Vec<Reading> {
    let widest = WIDTHS.iter().map(|w| w.ms()).max().unwrap_or_default();
    let oldest = ended_ms - (Preset::longest_wait() - 1) * BUCKET.bucket_ms() - widest;
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
            let outside = outside(&judged, gap.as_ref(), warned.held_since.is_some() && !quiet_back)
                .filter(|_| learning.presets_of(&key.0, &key.1).rows.warns());
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
    // A sender is not judged while its source warns (`Warned::of_senders`), nor on the reading its source crosses.
    let below: HashSet<(String, String)> = out
        .iter()
        .filter(|r| r.outside == Some(Outside::Below))
        .map(|r| (r.group.clone(), r.source.clone()))
        .collect();
    let senders = sender_readings(store, ended_ms, oldest, learning, learned);
    out.extend(
        senders
            .into_iter()
            .filter(|r| !below.contains(&(r.group.clone(), r.source.clone()))),
    );
    out.sort_by(|a, b| (&a.group, &a.source, &a.sender).cmp(&(&b.group, &b.source, &b.sender)));
    out
}

/// Each watched sender's reading at `ended_ms`, from its rows since `oldest` where its field was counted. Only below
/// normal is judged: a sender above it reads more of its source's rows, which its source is judged by.
fn sender_readings(
    store: &RegistryStore,
    ended_ms: i64,
    oldest: i64,
    learning: &Marks,
    learned: &Learned,
) -> Vec<Reading> {
    let watched: Vec<SenderKey> = learned.senders.keys().cloned().collect();
    let buckets = senders::history(store, &watched, oldest).unwrap_or_else(|e| {
        tracing::warn!(name: "senders_judge_unreadable", target: "status", error = %e, "could not read the senders' rows, so no sender is judged this pass");
        HashMap::new()
    });
    let warned = Warned::of_senders(store, buckets.keys(), oldest, learning);
    buckets
        .into_iter()
        .filter_map(|(key, of)| {
            let rows = Rows::of(&of, oldest, ended_ms);
            let warned = &warned[&key];
            let judged: Vec<Judged> = learned.senders[&key]
                .iter()
                .filter_map(|normal| judge(&rows, warned, normal, ended_ms))
                .collect();
            let outside = outside(&judged, None, warned.held_since.is_some())
                .filter(|o| *o == Outside::Below && learning.presets_of(&key.0, &key.1).senders.warns());
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
        (0..normal.preset.wait())
            .all(|i| read(ended_ms - i * BUCKET.bucket_ms()).is_some_and(|(rows, band)| past(rows, band)))
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
        let (rows, missing) = self.read(ended_ms - width.ms(), ended_ms)?;
        (missing == 0).then_some(rows)
    }

    /// Rows read from `from_ms` until `to_ms`, and how many of its buckets have no point, or None if any is not held.
    fn read(&self, from_ms: i64, to_ms: i64) -> Option<(f64, u32)> {
        let at = |ms: i64| usize::try_from((ms - self.start_ms).div_euclid(BUCKET.bucket_ms())).ok();
        let (&(after, missing_after), &(before, missing_before)) =
            (self.before.get(at(to_ms)?)?, self.before.get(at(from_ms)?)?);
        Some((after - before, missing_after - missing_before))
    }
}

/// Logs that `group`'s `source`'s warnings could not be read, and reads none.
fn unreadable(e: super::store::StoreError, group: &str, source: &str) -> Vec<warnings::WarningSpan> {
    tracing::warn!(
        name: "warned_unreadable", target: "status",
        error = %e, group, source,
        "could not read when this source warned, so its normal is learned from every window this pass"
    );
    Vec::new()
}

/// Spans as (started, ended), with no end for a span still open.
type Spans = Vec<(i64, Option<i64>)>;

/// What one source's warnings say about its rows, as [`Spans`].
struct Warned {
    /// Each span its source was blocked, refused, waiting or unreachable.
    stops: Spans,
    /// When its rows teach its normal nothing: while a stop about it held, and before an editor reset its history.
    unlearned: Spans,
    /// Each below-normal warning about it an editor did not say was normal, from the first reading it may have waited
    /// for at any preset ([`waited_from`]): its preset may have been another when it was raised.
    below: Spans,
    /// When a below-normal warning about it that still holds began.
    held_since: Option<i64>,
    /// Each below-normal warning about it an editor said was normal, as [`Self::below`] would hold it. It is learned
    /// whole, so marking the one that holds teaches the level it reads.
    normal: Spans,
}

impl Warned {
    /// `group`'s `source`'s spans since `since_ms`. Warnings the store cannot read are logged and left out: the
    /// normal is then learned from those windows too.
    fn of(store: &RegistryStore, group: &str, source: &str, since_ms: i64, learning: &Marks) -> Warned {
        let held = Warned::held(store, group, source, since_ms);
        let stops = held
            .iter()
            .filter(|s| !matches!(s.condition, Condition::SourceRowsBelow | Condition::SourceRowsAbove))
            .map(|s| (s.started_millis, s.ended_millis))
            .collect();
        let below = held
            .into_iter()
            .filter(|s| s.condition == Condition::SourceRowsBelow)
            .collect();
        Warned::new(group, source, None, stops, below, learning)
    }

    /// [`Warned::of`] for each of `senders`, each source's warnings read once. A source's below-normal warnings count
    /// as stops for its senders, as the source's own stops do, but for those an editor said were normal. What a sender
    /// reads while its source reads too little says nothing of the sender.
    fn of_senders<'a>(
        store: &RegistryStore,
        senders: impl IntoIterator<Item = &'a SenderKey>,
        since_ms: i64,
        learning: &Marks,
    ) -> HashMap<SenderKey, Warned> {
        let mut stops: HashMap<(&str, &str), Spans> = HashMap::new();
        senders
            .into_iter()
            .map(|key| {
                let (group, source, sender) = key;
                let stops = stops.entry((group, source)).or_insert_with(|| {
                    Warned::held(store, group, source, since_ms)
                        .iter()
                        .filter(|s| match s.condition {
                            Condition::SourceRowsAbove => false,
                            Condition::SourceRowsBelow => !learning.said_normal(group, source, None, s.started_millis),
                            _ => true,
                        })
                        .map(|s| (s.started_millis, s.ended_millis))
                        .collect()
                });
                let below = warnings::sender_spans(store, group, source, sender, since_ms)
                    .unwrap_or_else(|e| unreadable(e, group, source));
                (
                    key.clone(),
                    Warned::new(group, source, Some(sender), stops.clone(), below, learning),
                )
            })
            .collect()
    }

    /// [`Warned::of`] for `group`'s `source`, or for its `sender`.
    fn of_subject(
        store: &RegistryStore,
        group: &str,
        source: &str,
        sender: Option<&Sender>,
        since_ms: i64,
        learning: &Marks,
    ) -> Warned {
        let Some(sender) = sender else {
            return Warned::of(store, group, source, since_ms, learning);
        };
        let key = (group.to_string(), source.to_string(), sender.clone());
        Warned::of_senders(store, [&key], since_ms, learning)
            .remove(&key)
            .expect("each sender asked for is answered")
    }

    /// `group`'s `source`'s warnings since `since_ms`.
    fn held(store: &RegistryStore, group: &str, source: &str, since_ms: i64) -> Vec<warnings::WarningSpan> {
        warnings::spans(store, SubjectKind::Source, group, source, since_ms)
            .unwrap_or_else(|e| unreadable(e, group, source))
    }

    /// `group`'s `source`'s spans, or its `sender`'s, from its `stops` and its `below`-normal warnings.
    fn new(
        group: &str,
        source: &str,
        sender: Option<&Sender>,
        stops: Spans,
        below: Vec<warnings::WarningSpan>,
        learning: &Marks,
    ) -> Warned {
        let reset_from = learning.reset_from(group, source);
        let reset = reset_from.map(|from| (i64::MIN, Some(from)));
        // A reset ends the warning: the normal it was judged against is forgotten.
        let held_since = below
            .iter()
            .filter(|s| s.ended_millis.is_none() && reset_from.is_none_or(|from| s.started_millis >= from))
            .map(|s| s.started_millis)
            .min();
        let (normal, below): (Vec<_>, Vec<_>) = below
            .iter()
            .partition(|s| learning.said_normal(group, source, sender, s.started_millis));
        let spans = |warnings: Vec<&warnings::WarningSpan>| -> Spans {
            warnings
                .into_iter()
                .map(|s| (waited_from(s, Preset::longest_wait()), s.ended_millis))
                .collect()
        };
        Warned {
            held_since,
            below: spans(below),
            unlearned: stops.iter().copied().chain(reset).collect(),
            stops,
            normal: spans(normal),
        }
    }

    /// Whether the `width_ms` ending at `ended_ms` teaches the normal nothing: it overlaps a span not learned, or it
    /// shares a bucket with a window of its width that ended while a below-normal warning waited or held.
    fn during(&self, ended_ms: i64, width_ms: i64) -> bool {
        Warned::overlaps(&self.unlearned, ended_ms, width_ms)
            || Warned::overlaps(&self.below, ended_ms + width_ms, 2 * width_ms)
    }

    /// Whether the window of `width_ms` that ends at `ended_ms` overlaps a warning an editor said was normal.
    fn said_normal(&self, ended_ms: i64, width_ms: i64) -> bool {
        Warned::overlaps(&self.normal, ended_ms, width_ms)
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

/// The earliest the first reading a below-normal warning raised after `wait` readings waited for can have ended:
/// `wait` buckets before the warning was seen. That reading ended `wait` - 1 buckets before the one that raised the
/// warning, and the warning was seen up to a heartbeat, taken as one bucket, after that.
fn waited_from(warning: &warnings::WarningSpan, wait: i64) -> i64 {
    warning.started_millis - wait * BUCKET.bucket_ms()
}

/// How many of a source's longest quiet spells [`past`] lists.
const LONGEST_LISTED: usize = 10;

/// A source's, or one of its senders', below-normal warnings over the past year, which an editor may say were normal
/// ([`marks::Marks::answers`]), its longest quiet spells, and the preset it is judged at.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Past {
    /// Its below-normal warnings, newest first: each from when it began, which names it, until it ended, or until now
    /// while it holds.
    pub warnings: Vec<PastSpan>,
    /// Its longest quiet spells, longest first. None for a sender, which the gap does not judge.
    pub silences: Vec<PastSpan>,
    pub preset: Preset,
    /// When its rows will reach back [`JUDGED_AFTER_MS`] from its first or from its history's reset, and so it will be
    /// judged, while that is still to come.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub judged_from_millis: Option<i64>,
    /// When it will have the weeks of rows its weekdays are told apart from ([`Week`]), while that is still to come:
    /// until then every day of its week is judged alike.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub weekdays_from_millis: Option<i64>,
    /// How long it may go without a row before it warns, while the gap judges it ([`Learned::quiet_limit_ms`]).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quiet_limit_millis: Option<i64>,
}

impl Default for Past {
    /// Nothing, as for a source with no history, at the preset it has until an editor picks another.
    fn default() -> Self {
        Past {
            warnings: Vec::new(),
            silences: Vec::new(),
            preset: Presets::default().rows,
            judged_from_millis: None,
            weekdays_from_millis: None,
            quiet_limit_millis: None,
        }
    }
}

/// One span of [`Past`], and whether its source's normal learns from it: a warning only once an editor said it was
/// normal.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PastSpan {
    pub from_millis: i64,
    pub to_millis: i64,
    pub learned: bool,
    /// It is the warning that holds, and runs until now.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub holds: bool,
    /// Who answered what it was, if anyone did: Normal where it is `learned`, else Outage.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub marked_by: Option<String>,
    /// Why it began, where it kept why.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<Reason>,
}

/// `group`'s `source`'s [`Past`], or its `sender`'s, as of `now_ms`.
pub fn past(
    store: &RegistryStore,
    group: &str,
    source: &str,
    sender: Option<&Sender>,
    now_ms: i64,
    learning: &Marks,
) -> Result<Past, super::store::StoreError> {
    let year = now_ms - YEAR_MS;
    let warned = Warned::of_subject(store, group, source, sender, year, learning);
    let preset = learning.presets_of(group, source).of(sender);
    let span = |from_millis: i64, to_millis: i64| PastSpan {
        from_millis,
        to_millis,
        learned: !warned.span_during(to_millis, to_millis - from_millis),
        holds: false,
        marked_by: None,
        reason: None,
    };
    let (below, mut silences) = match sender {
        Some(sender) => (warnings::sender_spans(store, group, source, sender, year)?, Vec::new()),
        None => (
            warnings::spans(store, SubjectKind::Source, group, source, year)?
                .into_iter()
                .filter(|s| s.condition == Condition::SourceRowsBelow)
                .collect(),
            store.source_silences(group, source, year)?,
        ),
    };
    silences.sort_by_key(|&(ended, length)| (std::cmp::Reverse(length), std::cmp::Reverse(ended)));
    // The weeks the weekdays are told apart from reach past every time a source is judged from. A day more is read, so
    // a source with rows all through them has its first before them.
    let weeks = week::WEEKS * 7 * DAY_MS;
    let first = rows_of(store, group, source, sender, now_ms - weeks - DAY_MS)?
        .and_then(|rows| rows.into_keys().next())
        .map(|first| first.max(learning.reset_from(group, source).unwrap_or(first)));
    let ahead = |after: i64| first.map(|first| first + after).filter(|&at| at > now_ms);
    Ok(Past {
        warnings: below
            .iter()
            .rev()
            .map(|s| PastSpan {
                from_millis: s.started_millis,
                to_millis: s.ended_millis.unwrap_or(now_ms),
                learned: learning.said_normal(group, source, sender, s.started_millis),
                holds: s.ended_millis.is_none(),
                marked_by: learning
                    .answer(group, source, sender, Some(s.started_millis))
                    .map(|m| m.by.clone()),
                reason: s.reason,
            })
            .collect(),
        silences: silences
            .into_iter()
            .take(LONGEST_LISTED)
            .map(|(ended, length)| span(ended - length, ended))
            .collect(),
        preset,
        judged_from_millis: ahead(JUDGED_AFTER_MS),
        weekdays_from_millis: ahead(weeks),
        quiet_limit_millis: None,
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
    learning: &Marks,
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

/// A source's rows, or one of its sender's, against its range, as [`chart`] draws them.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Chart {
    /// How wide each point is.
    pub point_millis: i64,
    /// Rows per second in each point, by when it began. A point nothing was kept for has none.
    pub points: Vec<(i64, f64)>,
    pub expected: Vec<ExpectedPoint>,
}

/// `group`'s `source`'s rows, or its `sender`'s, from the point `from_ms` is in until `to_ms`, against its range
/// ([`expected_over`]), as of `now_ms`. Its points are as wide as a chart of that span draws them
/// ([`super::series::chart_point_ms`]), and no finer than the rows kept from then: five minutes for the past four
/// weeks, an hour before.
///
/// Given the `window_ms` of one of [`WIDTHS`], each point is instead the rate over that width ending where the point
/// ends, against that width's range, as that width judged it. A point wider than the width samples it, so a chart of
/// any span has as many points. A point whose width reaches a bucket nothing was kept for in five minutes has none.
#[allow(clippy::too_many_arguments)]
pub fn chart(
    store: &RegistryStore,
    group: &str,
    source: &str,
    sender: Option<&Sender>,
    from_ms: i64,
    to_ms: i64,
    now_ms: i64,
    window_ms: Option<i64>,
    learning: &Marks,
) -> Result<Chart, super::store::StoreError> {
    let kept = match now_ms - from_ms <= Tier::FiveMinutes.retention_ms() {
        true => BUCKET.bucket_ms(),
        false => Tier::OneHour.bucket_ms(),
    };
    let width = window_ms.and_then(|ms| WIDTHS.into_iter().find(|w| w.ms() == ms));
    let span = Duration::from_millis(to_ms.saturating_sub(from_ms).max(0) as u64);
    let point_ms = super::series::chart_point_ms(span).max(kept);
    let since = from_ms.div_euclid(point_ms) * point_ms;
    let read_from = since - width.map_or(0, Width::ms);
    let rows = match sender {
        Some(_) => rows_of(store, group, source, sender, read_from)?,
        None => history(store, Tier::OneHour.and_finer(), BUCKET, read_from)?
            .remove(&(group.to_string(), source.to_string())),
    }
    .unwrap_or_default();
    let points = match width {
        None => {
            let mut points: BTreeMap<i64, f64> = BTreeMap::new();
            for (&at, &n) in rows.range(since..to_ms) {
                *points.entry(at.div_euclid(point_ms) * point_ms).or_default() += n;
            }
            let seconds = point_ms as f64 / 1000.0;
            points.into_iter().map(|(at, n)| (at, n / seconds)).collect()
        }
        Some(width) => {
            let read = Rows::of(&rows, read_from, to_ms.div_euclid(point_ms) * point_ms + point_ms);
            let seconds = width.window.as_secs_f64();
            (since..to_ms)
                .step_by(point_ms as usize)
                .filter_map(|at| Some((at, read.window(at + point_ms, width)? / seconds)))
                .collect()
        }
    };
    let expected = match width {
        Some(width) => expected_at(
            store,
            group,
            source,
            sender,
            since,
            to_ms.min(now_ms),
            point_ms,
            width,
            learning,
        )?,
        None => expected_over(
            store,
            group,
            source,
            sender,
            since,
            to_ms.min(now_ms),
            point_ms,
            learning,
        )?,
    };
    Ok(Chart {
        point_millis: point_ms,
        points,
        expected,
    })
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

/// `group`'s `source`'s range, or its `sender`'s, as rows per second, for each point of a chart drawn in points
/// `point_ms` wide ([`super::series::chart_point_ms`]), from the point `since_ms` is in to the point `now_ms` is in. A
/// point's range is the one judged at the widest width no wider than it, as of its hour. For a source's point finer
/// than five minutes, it is the source's swings at the point's own width ([`record_swings`]); a sender's rows are kept
/// no finer than five minutes, so its chart is drawn no finer. A point where that width does not judge it has none.
/// The range reads only past days, so the point still filling has one too.
#[allow(clippy::too_many_arguments)]
pub fn expected_over(
    store: &RegistryStore,
    group: &str,
    source: &str,
    sender: Option<&Sender>,
    since_ms: i64,
    now_ms: i64,
    point_ms: i64,
    learning: &Marks,
) -> Result<Vec<ExpectedPoint>, super::store::StoreError> {
    if point_ms < BUCKET.bucket_ms() {
        return match sender {
            Some(_) => Ok(Vec::new()),
            None => swung_over(store, group, source, since_ms, now_ms, point_ms, learning),
        };
    }
    let width = WIDTHS
        .into_iter()
        .rev()
        .find(|w| w.ms() <= point_ms)
        .unwrap_or(WIDTHS[0]);
    expected_at(
        store, group, source, sender, since_ms, now_ms, point_ms, width, learning,
    )
}

/// [`expected_over`] at `width`, whatever its points' width.
#[allow(clippy::too_many_arguments)]
fn expected_at(
    store: &RegistryStore,
    group: &str,
    source: &str,
    sender: Option<&Sender>,
    since_ms: i64,
    now_ms: i64,
    point_ms: i64,
    width: Width,
    learning: &Marks,
) -> Result<Vec<ExpectedPoint>, super::store::StoreError> {
    let step = point_ms;
    let first = since_ms.div_euclid(step) * step + step;
    let last = now_ms.div_euclid(step) * step + step;
    let oldest = Tier::OneHour.align(first) - lookback_ms();
    let Some(of) = rows_of(store, group, source, sender, oldest)? else {
        return Ok(Vec::new());
    };
    let rows = Rows::of(&of, oldest, last);
    let warned = Warned::of_subject(store, group, source, sender, oldest, learning);
    let preset = learning.presets_of(group, source).of(sender);
    let mut normals: HashMap<i64, Option<Normal>> = HashMap::new();
    // A week holds all day.
    let mut weeks: HashMap<i64, Week> = HashMap::new();
    let mut out = Vec::new();
    let mut ended_ms = first;
    while ended_ms <= last {
        let as_of = Tier::OneHour.align(ended_ms);
        let normal = normals.entry(as_of).or_insert_with(|| {
            let week = weeks
                .entry(as_of.div_euclid(DAY_MS))
                .or_insert_with(|| Week::learn(&rows, &warned, as_of));
            normal(&rows, &warned, as_of, width, preset, week)
        });
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

/// `group`'s `source`'s rows, or its `sender`'s, in each [`BUCKET`] since `since_ms`; None while it has none.
pub fn rows_of(
    store: &RegistryStore,
    group: &str,
    source: &str,
    sender: Option<&Sender>,
    since_ms: i64,
) -> Result<Option<BTreeMap<i64, f64>>, super::store::StoreError> {
    Ok(match sender {
        Some(sender) => {
            let key = (group.to_string(), source.to_string(), sender.clone());
            senders::history(store, std::slice::from_ref(&key), since_ms)?.remove(&key)
        }
        None => history(store, BUCKET.and_finer(), BUCKET, since_ms)?.remove(&(group.to_string(), source.to_string())),
    })
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
    learning: &Marks,
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
    // No range before the swings learned from reach back [`JUDGED_AFTER_MS`], as `normal` draws none at the wider widths.
    let reach = swung
        .keys()
        .find(|&&ended| !warned.during(ended, width))
        .map(|&ended| ended - width);
    let judged_from = reach.map_or(i64::MAX, |reach| reach + JUDGED_AFTER_MS);
    let mut out = Vec::new();
    let mut ended_ms = first + width;
    while ended_ms <= BUCKET.align(now_ms) + width {
        let held = |ended| swung.get(&ended).copied().filter(|_| !warned.during(ended, width));
        if let Some(band) = swung_band(held, ended_ms, width, across).filter(|_| ended_ms - width >= judged_from) {
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
/// than [`USUAL_MIN_DAYS`] days have one. `held` is one past bucket's swing, its low and high rate ([`swing`]), if it
/// is known. The range cuts a [`TAIL`] from the bottom of the lows and from the top of the highs.
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
    if days.len() < USUAL_MIN_DAYS {
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
/// ([`Marks::read`]).
///
/// Without `wait`, a caller holding an earlier answer gets that one at once and the new one is computed behind it:
/// what a heartbeat asks, which must not wait on a week of history being read.
pub async fn readings(state: &AppState, now_ms: i64, heartbeat_interval: Duration, wait: bool) -> Arc<Vec<Reading>> {
    let learning = run_store(state.store.clone(), |s| Ok(Marks::read(s)))
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
    use crate::leader::marks;

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
    fn reading_at(store: &RegistryStore, ended: i64, learning: &Marks) -> Option<Reading> {
        let learned = learn(store, Tier::OneHour.align(ended), learning, None);
        compute(store, ended, learning, &learned).into_iter().next()
    }

    fn reading(store: &RegistryStore) -> Option<Reading> {
        reading_at(store, END, &Marks::default())
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

    /// What is learned with once an editor picked `preset` for `g`'s `s`, or for its `senders`.
    fn at_preset(preset: Preset, senders: bool) -> Marks {
        let mut presets = Presets::default();
        *if senders {
            &mut presets.senders
        } else {
            &mut presets.rows
        } = preset;
        Marks {
            presets: vec![marks::Picked {
                group: "g".into(),
                source: "s".into(),
                presets,
                by: "John".into(),
            }],
            ..Marks::default()
        }
    }

    /// A preset's margin widens the range by its share of each edge.
    #[test]
    fn a_presets_margin_widens_its_range() {
        let store = store_with(7, |_| Some(100.0));
        for (preset, low, high) in [
            (Preset::Fast, 50.0, 150.0),
            (Preset::Sensitive, 80.0, 120.0),
            (Preset::Balanced, 70.0, 130.0),
            (Preset::Quiet, 50.0, 150.0),
        ] {
            let r = reading_at(&store, END, &at_preset(preset, false)).unwrap();
            assert_eq!(r.judged[0].band, Band { low, high }, "{preset:?}");
        }
    }

    /// A stop reads below every preset's range, so each sees it on the last reading it waits for, and not before.
    #[test]
    fn a_source_that_stops_is_below_on_the_last_reading_its_preset_waits_for() {
        let stopped =
            |buckets: i64| store_with(7, move |at| Some(if at >= END - buckets * FIVE { 0.0 } else { 100.0 }));
        assert_eq!(Marks::default().presets_of("g", "s").rows, Preset::Balanced);
        for preset in Preset::ALL {
            let learning = at_preset(preset, false);
            let wait = preset.wait();
            let before = reading_at(&stopped(wait - 1), END, &learning).unwrap();
            assert_eq!(before.outside, None, "{preset:?} waits {wait} readings");
            let r = reading_at(&stopped(wait), END, &learning).unwrap();
            assert_eq!(r.outside, Some(Outside::Below), "{preset:?}");
            assert_eq!(r.shown().unwrap().window, WIDTHS[0].window);
        }
    }

    /// A stop's warning says why it began: its rate over a width that waited out its readings, against that width's
    /// range. The warning keeps it, and its source's past lists it.
    #[test]
    fn a_warning_keeps_why_it_began() {
        let stopped = store_with(7, |at| Some(if at >= END - 12 * FIVE { 0.0 } else { 100.0 }));
        let reason = reading_at(&stopped, END, &Marks::default()).unwrap().reason().unwrap();
        assert!(
            matches!(reason, Reason::Width { per_second, low, .. } if per_second == 0.0 && low > 0.0),
            "{reason:?}"
        );
        let subject = serde_json::to_string(&("g", "s")).unwrap();
        let fact = vec![("source_rows_below".to_string(), subject.clone())];
        stopped.sync_fact_clock(&[], &fact, END - FIVE).unwrap();
        let kept = serde_json::to_string(&reason).unwrap();
        stopped
            .keep_reasons(&[("source_rows_below".to_string(), subject, kept)])
            .unwrap();
        let past = past(&stopped, "g", "s", None, END, &Marks::default()).unwrap();
        assert_eq!(past.warnings[0].reason, Some(reason));
    }

    /// Drawn at a width, a chart's points are the rate over that width, against that width's range.
    #[test]
    fn a_chart_at_a_width_draws_its_rate_against_its_range() {
        let store = store_with(7, |at| Some(if at >= END - 6 * FIVE { 80.0 } else { 100.0 }));
        let half_hour = WINDOW.as_millis() as i64;
        let at = chart(
            &store,
            "g",
            "s",
            None,
            END - 2 * HOUR,
            END,
            END,
            Some(half_hour),
            &Marks::default(),
        )
        .unwrap();
        assert_eq!(at.point_millis, FIVE);
        assert_eq!(at.points.first(), Some(&(END - 2 * HOUR, 600.0 / 1800.0)));
        assert_eq!(
            at.points.last(),
            Some(&(END - FIVE, 480.0 / 1800.0)),
            "the last half hour, all of it low"
        );
        let seconds = 1800.0;
        assert!(
            at.expected
                .iter()
                .all(|e| e.low * seconds == 420.0 && e.high * seconds == 780.0),
            "{:?}",
            at.expected
        );
        assert!(!at.expected.is_empty());
        let week = chart(
            &store,
            "g",
            "s",
            None,
            END - 7 * DAY_MS,
            END,
            END,
            Some(FIVE),
            &Marks::default(),
        )
        .unwrap();
        assert!(
            week.point_millis > FIVE && week.points.len() <= 360,
            "a long chart samples a narrow width"
        );
    }

    /// At Off a stop is judged as at Balanced, so it reads below its range, but it never warns.
    #[test]
    fn a_source_at_off_is_judged_but_never_warns() {
        let stopped = store_with(7, |at| Some(if at >= END - 12 * FIVE { 0.0 } else { 100.0 }));
        let r = reading_at(&stopped, END, &at_preset(Preset::Off, false)).unwrap();
        let balanced = reading_at(&stopped, END, &at_preset(Preset::Balanced, false)).unwrap();
        assert_eq!(balanced.outside, Some(Outside::Below));
        assert_eq!(r.judged, balanced.judged);
        assert_eq!(r.outside, None);
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
        // A heartbeat in the next bucket writes the last.
        store
            .record_streams("g", &StreamReport::default(), true, None, END)
            .unwrap();
    }

    /// Each sender's reading at `END`, learned with `learning`, by its value.
    fn sender_readings_at_end(store: &RegistryStore, learning: &Marks) -> BTreeMap<String, Option<Outside>> {
        let learned = learn(store, END, learning, None);
        compute(store, END, learning, &learned)
            .into_iter()
            .filter_map(|r| Some((r.sender?.value, r.outside)))
            .collect()
    }

    /// A sender that stops sending reads nothing where its field was counted, and is below on the last reading its
    /// preset waits for, though its source is not; one that keeps sending is within its range.
    #[test]
    fn a_sender_that_stops_is_below_though_its_source_is_not() {
        let stopped = |at: i64| at >= END - Presets::default().senders.wait() * FIVE;
        let store = store_with(7, |at| Some(if stopped(at) { 90.0 } else { 100.0 }));
        send(&store, 7, "fw-01", |at| (!stopped(at)).then_some(10));
        send(&store, 7, "fw-07", |_| Some(90));
        let read = sender_readings_at_end(&store, &Marks::default());
        assert_eq!(read["fw-01"], Some(Outside::Below));
        assert_eq!(read["fw-07"], None);
    }

    /// Two senders that send less from three days ago, each with a below-normal warning since, beside one that sends
    /// steadily.
    fn two_senders_dropped() -> (RegistryStore, i64) {
        let dropped = END - 3 * DAY_MS;
        let store = store_with(14, |at| Some(if at >= dropped { 88.0 } else { 100.0 }));
        for value in ["fw-01", "fw-02"] {
            send(&store, 14, value, |at| Some(if at >= dropped { 4 } else { 10 }));
        }
        send(&store, 14, "fw-07", |_| Some(80));
        let facts: Vec<(String, String)> = ["fw-01", "fw-02"]
            .iter()
            .map(|value| {
                let object = serde_json::to_string(&("s", "devname", value)).unwrap();
                (
                    "sender_rows_below".to_string(),
                    serde_json::to_string(&("g", object)).unwrap(),
                )
            })
            .collect();
        store.sync_fact_clock(&[], &facts, dropped + 6 * FIVE).unwrap();
        (store, dropped)
    }

    fn fw(value: &str) -> Sender {
        Sender {
            field: "devname".into(),
            value: value.into(),
        }
    }

    /// An editor said the below-normal warning of the source, or of one `sender`, that began at `since_ms` was normal.
    fn said(sender: Option<Sender>, since_ms: i64) -> Marks {
        Marks {
            answers: vec![marks::Mark {
                group: "g".into(),
                source: "s".into(),
                sender,
                since_millis: since_ms,
                was: marks::Was::Normal,
                by: "John".into(),
            }],
            ..Marks::default()
        }
    }

    /// A sender's below-normal warning is not learned until an editor says it was normal, and a mark names one
    /// warning of one subject, by when it began.
    #[test]
    fn a_senders_warning_said_normal_is_learned_and_no_other_is() {
        let (store, dropped) = two_senders_dropped();
        let began = dropped + 6 * FIVE;
        let read = |learning: &Marks| sender_readings_at_end(&store, learning);
        let below = Some(Outside::Below);
        let both = BTreeMap::from([("fw-01".into(), below), ("fw-02".into(), below), ("fw-07".into(), None)]);
        assert_eq!(read(&Marks::default()), both);
        assert_eq!(read(&said(None, began)), both, "a source's mark is its own");
        assert_eq!(
            read(&said(Some(fw("fw-01")), began + FIVE)),
            both,
            "no warning began then"
        );
        assert_eq!(
            read(&said(Some(fw("fw-01")), began)),
            BTreeMap::from([("fw-01".into(), None), ("fw-02".into(), below), ("fw-07".into(), None)])
        );
    }

    /// Senders at Off are judged but never warn, and their source's preset is its own.
    #[test]
    fn senders_at_off_never_warn() {
        let (store, _) = two_senders_dropped();
        let read = sender_readings_at_end(&store, &at_preset(Preset::Off, true));
        assert!(read.values().all(Option::is_none), "{read:?}");
        assert_eq!(read.len(), 3, "each is still judged");
        assert_eq!(
            reading_at(&store, END, &at_preset(Preset::Off, true)).map(|r| r.outside),
            reading_at(&store, END, &Marks::default()).map(|r| r.outside)
        );
    }

    /// A source's below-normal warning is a stop for its senders, unless an editor said it was normal.
    #[test]
    fn a_sources_warning_said_normal_is_learned_by_its_senders() {
        let (store, dropped) = two_senders_dropped();
        warned(&store, "source_rows_below", dropped, Some(dropped + HOUR));
        let key = ("g".to_string(), "s".to_string(), fw("fw-07"));
        let stops = |learning: &Marks| {
            Warned::of_senders(&store, [&key], dropped - DAY_MS, learning)
                .remove(&key)
                .unwrap()
                .stops
        };
        assert_eq!(stops(&Marks::default()), [(dropped, Some(dropped + HOUR))]);
        assert_eq!(stops(&said(None, dropped)), []);
    }

    /// A sender's past is its own warnings, at its source's senders' preset, with no quiet spells: the gap does not
    /// judge a sender.
    #[test]
    fn a_senders_past_lists_its_own_warnings() {
        let (store, dropped) = two_senders_dropped();
        let past_of =
            |sender: &str, learning: &Marks| past(&store, "g", "s", Some(&fw(sender)), END, learning).unwrap();
        let unmarked = past_of("fw-01", &Marks::default());
        assert_eq!(
            (unmarked.warnings, unmarked.silences, unmarked.preset),
            (
                vec![PastSpan {
                    from_millis: dropped + 6 * FIVE,
                    to_millis: END,
                    learned: false,
                    holds: true,
                    marked_by: None,
                    reason: None,
                }],
                Vec::new(),
                Presets::default().senders,
            )
        );
        assert!(past_of("fw-07", &Marks::default()).warnings.is_empty());
        let marked = said(Some(fw("fw-01")), dropped + 6 * FIVE);
        let said = &past_of("fw-01", &marked).warnings[0];
        assert!(said.learned);
        assert_eq!(said.marked_by.as_deref(), Some("John"));
        assert!(
            !past_of("fw-02", &marked).warnings[0].learned,
            "a sender's mark is its own"
        );
    }

    /// A source is judged once its rows reach back a week, and its weekdays told apart once it has four weeks: a past
    /// answer says when, while either is to come, from its first row or from when its history was reset.
    #[test]
    fn a_past_says_when_a_new_source_is_judged_and_its_weekdays_told_apart() {
        // Its first row two days before `END`.
        let new = store_with(1, |_| Some(100.0));
        let learning = |store: &RegistryStore, learning: &Marks| {
            let past = past(store, "g", "s", None, END, learning).unwrap();
            (past.judged_from_millis, past.weekdays_from_millis)
        };
        assert_eq!(
            learning(&new, &Marks::default()),
            (Some(END + 5 * DAY_MS), Some(END + 26 * DAY_MS))
        );
        let old = store_with(30, |_| Some(100.0));
        assert_eq!(learning(&old, &Marks::default()), (None, None));
        let reset = reset_at(END - DAY_MS);
        assert_eq!(
            learning(&old, &reset),
            (Some(END + 6 * DAY_MS), Some(END + 27 * DAY_MS))
        );
    }

    /// A sender's chart reads its own rows, in points no finer than the five minutes they are kept in.
    #[test]
    fn a_senders_chart_reads_its_own_rows_per_second() {
        let (store, dropped) = two_senders_dropped();
        let hour = chart(
            &store,
            "g",
            "s",
            Some(&fw("fw-01")),
            END - HOUR,
            END,
            END,
            None,
            &Marks::default(),
        )
        .unwrap();
        assert_eq!(hour.point_millis, FIVE);
        assert_eq!(hour.points.len(), 12);
        assert!(
            hour.points.iter().all(|&(_, rows)| rows == 4.0 / 300.0),
            "{:?}",
            hour.points
        );
        let before = chart(
            &store,
            "g",
            "s",
            Some(&fw("fw-01")),
            dropped - DAY_MS,
            dropped,
            END,
            None,
            &Marks::default(),
        )
        .unwrap();
        assert!(before.points.iter().all(|&(_, rows)| rows == 10.0 / 300.0));
        assert!(!before.expected.is_empty(), "eleven days before have a range");
    }

    /// A source's senders are judged at their own preset, whatever the source's is.
    #[test]
    fn a_sender_is_judged_at_its_sources_senders_preset() {
        let stopped = |at: i64| at >= END - FIVE;
        let store = store_with(7, |at| Some(if stopped(at) { 90.0 } else { 100.0 }));
        send(&store, 7, "fw-01", |at| (!stopped(at)).then_some(10));
        send(&store, 7, "fw-07", |_| Some(90));
        let below = |learning: &Marks| sender_readings_at_end(&store, learning)["fw-01"] == Some(Outside::Below);
        assert!(!below(&at_preset(Preset::Fast, false)));
        assert!(below(&at_preset(Preset::Fast, true)));
    }

    /// When the whole source stops, its own warning says so: none of its senders is judged on that reading.
    #[test]
    fn a_sender_is_not_judged_on_the_reading_its_source_is_below() {
        let stopped = |at: i64| at >= END - Presets::default().senders.wait() * FIVE;
        let store = store_with(7, |at| Some(if stopped(at) { 0.0 } else { 100.0 }));
        send(&store, 7, "fw-01", |at| (!stopped(at)).then_some(10));
        send(&store, 7, "fw-07", |at| (!stopped(at)).then_some(90));
        assert_eq!(sender_readings_at_end(&store, &Marks::default()), BTreeMap::new());
    }

    /// A field the pipeline stops delivering, as after a change renames it, leaves its senders unjudged; a field still
    /// delivered whose every sender stops has them all below.
    #[test]
    fn a_field_no_longer_counted_judges_none_of_its_senders() {
        use crate::heartbeat::{SenderCounts, StreamReport};
        let wait = Presets::default().senders.wait();
        let stopped = |at: i64| at >= END - wait * FIVE;
        let store = store_with(7, |_| Some(100.0));
        send(&store, 7, "fw-01", |at| (!stopped(at)).then_some(10));
        send(&store, 7, "fw-07", |at| (!stopped(at)).then_some(90));
        assert!(
            !sender_readings_at_end(&store, &Marks::default())
                .values()
                .any(|o| *o == Some(Outside::Below)),
            "no worker counted the field"
        );

        let counted = StreamReport {
            senders: vec![SenderCounts {
                source: "s".into(),
                field: "devname".into(),
                values: Vec::new(),
                too_many: false,
            }],
            ..Default::default()
        };
        for at in (END - wait * FIVE..=END).step_by(FIVE as usize) {
            store.record_streams("g", &counted, true, None, at).unwrap();
        }
        let read = sender_readings_at_end(&store, &Marks::default());
        assert_eq!(read["fw-01"], Some(Outside::Below));
        assert_eq!(read["fw-07"], Some(Outside::Below));
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
        let r = reading_at(&store, ended, &Marks::default()).unwrap();
        assert_eq!(windows(&r), [360, 1440]);
        assert_eq!(r.outside, Some(Outside::Below));
        assert_eq!(r.shown().unwrap().window, WIDTHS[2].window);
    }

    /// A source that halves is below once its 6-hour window holds the halving. The windows since it halved teach its
    /// normal nothing until a whole width has passed after them. Each bucket reads 40 to 160 rows, so the halved
    /// source still reads inside the short widths' ranges.
    #[test]
    fn a_source_that_halves_is_below_at_six_hours() {
        let noisy = |at: i64| {
            let mut x = at as u64 ^ 0x9e37_79b9_7f4a_7c15;
            x ^= x >> 33;
            x = x.wrapping_mul(0xff51_afd7_ed55_8ccd);
            x ^= x >> 33;
            40.0 + (x % 121) as f64
        };
        let halved = END - 8 * HOUR;
        let store = store_with(29, |at| Some(if at >= halved { noisy(at) / 2.0 } else { noisy(at) }));
        let r = reading(&store).unwrap();
        let below: Vec<u64> = r
            .judged
            .iter()
            .filter(|j| j.below)
            .map(|j| j.window.as_secs() / 60)
            .collect();
        assert_eq!(below, [360]);
        assert_eq!(r.outside, Some(Outside::Below));
    }

    /// 100 rows a bucket from 06:00 to 20:00 UTC on a working day, a fifth of that at the weekend, and 10 at night.
    fn office(at: i64) -> f64 {
        let hour = at.rem_euclid(DAY_MS) / HOUR;
        // UTC day 0 was a Thursday.
        let working = (at.div_euclid(DAY_MS) + 3).rem_euclid(7) < 5;
        match ((6..20).contains(&hour), working) {
            (false, _) => 10.0,
            (true, true) => 100.0,
            (true, false) => 20.0,
        }
    }

    /// An office's working day is judged against its working days: one that reads at its weekend's level is below,
    /// while its weekend reads within its range.
    #[test]
    fn a_working_day_that_reads_at_its_weekends_level_is_below() {
        let thursday = END - 6 * HOUR;
        let saturday = END - 6 * DAY_MS + 18 * HOUR;
        let wait = Preset::Balanced.wait();
        let dropped = |at: i64| (thursday - wait * FIVE..thursday).contains(&at);
        let store = store_with(29, |at| Some(if dropped(at) { 20.0 } else { office(at) }));
        let learning = Marks::default();
        assert_eq!(
            reading_at(&store, thursday, &learning).unwrap().outside,
            Some(Outside::Below)
        );
        let store = store_with(29, |at| Some(office(at)));
        assert_eq!(reading_at(&store, thursday, &learning).unwrap().outside, None);
        assert_eq!(reading_at(&store, saturday, &learning).unwrap().outside, None);
    }

    /// A sender's days are its own: one that sends in office hours is below on a working day at its weekend's level,
    /// while its source, which reads the same every day, is not.
    #[test]
    fn a_sender_is_judged_against_its_own_kinds_of_day() {
        let thursday = END - 6 * HOUR;
        let wait = Presets::default().senders.wait();
        let dropped = |at: i64| (thursday - wait * FIVE..thursday).contains(&at);
        let store = store_with(29, |_| Some(1000.0));
        send(&store, 29, "fw-01", |at| {
            Some(if dropped(at) { 20 } else { office(at) as u64 })
        });
        // A field with one value is not watched.
        send(&store, 29, "fw-07", |_| Some(900));
        let learning = Marks::default();
        let learned = learn(&store, Tier::OneHour.align(thursday), &learning, None);
        let read: BTreeMap<Option<String>, Option<Outside>> = compute(&store, thursday, &learning, &learned)
            .into_iter()
            .map(|r| (r.sender.map(|s| s.value), r.outside))
            .collect();
        let fw = |value: &str| Some(value.to_string());
        assert_eq!(
            read,
            BTreeMap::from([(None, None), (fw("fw-01"), Some(Outside::Below)), (fw("fw-07"), None)])
        );
    }

    /// A row an hour from this time of day, a week back: the source's rows reach back far enough to be judged, and
    /// it counts as no day at this time of day.
    fn a_week_back(at: i64) -> bool {
        at == END - JUDGED_AFTER_MS - HOUR
    }

    /// Days are counted per time of day: the windows in the half hour after a midnight are that midnight's day.
    #[test]
    fn too_few_days_are_not_judged() {
        let days = USUAL_MIN_DAYS as i64 - 1;
        let since = |start: i64| move |at: i64| (at >= start || a_week_back(at)).then_some(100.0);
        assert_eq!(reading(&store_with(7, since(END - days * DAY_MS + 6 * FIVE))), None);
        assert!(reading(&store_with(7, since(END - days * DAY_MS + 5 * FIVE))).is_some());
    }

    #[test]
    fn a_source_is_judged_once_its_rows_reach_back_a_week() {
        let since = |start: i64| move |at: i64| (at >= start).then_some(100.0);
        assert_eq!(reading(&store_with(7, since(END - JUDGED_AFTER_MS + FIVE))), None);
        assert!(reading(&store_with(7, since(END - JUDGED_AFTER_MS))).is_some());
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
        let none = Marks::default();
        let first = readings.at(&store, END + 60_000, Duration::from_secs(1), &none);
        let again = readings.at(&store, END + 2 * 60_000, Duration::from_secs(1), &none);
        assert!(Arc::ptr_eq(&first, &again));
        let reset = reset_at(END - 30 * DAY_MS);
        let other = readings.at(&store, END + 2 * 60_000, Duration::from_secs(1), &reset);
        assert!(!Arc::ptr_eq(&first, &other));
    }

    #[test]
    fn a_reading_asked_while_another_is_computed_waits_for_it() {
        let store = store_with(7, |_| Some(100.0));
        let readings = Readings::default();
        let none = Marks::default();
        let old = readings.at(&store, END + 60_000, Duration::from_secs(1), &none);
        let computing = readings.computing.lock().unwrap();
        std::thread::scope(|s| {
            let asked = s.spawn(|| readings.at(&store, END + FIVE + 60_000, Duration::from_secs(1), &none));
            std::thread::sleep(Duration::from_millis(100));
            assert!(!asked.is_finished());
            drop(computing);
            assert!(!Arc::ptr_eq(&asked.join().unwrap(), &old));
        });
    }

    fn reset_at(from_millis: i64) -> Marks {
        Marks {
            reset: vec![marks::Reset {
                group: "g".into(),
                source: "s".into(),
                from_millis,
                by: "John".into(),
            }],
            ..Marks::default()
        }
    }

    #[test]
    fn a_source_whose_history_was_reset_has_no_normal_until_what_came_after_reaches_back_a_week() {
        let store = store_with(7, |_| Some(100.0));
        assert_eq!(reading_at(&store, END, &reset_at(END - JUDGED_AFTER_MS + FIVE)), None);
        assert!(reading_at(&store, END, &reset_at(END - JUDGED_AFTER_MS)).is_some());
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
        let warned = Warned::of(store, "g", "s", END - lookback_ms(), &Marks::default());
        !warned.during(ended, width.ms())
    }

    #[test]
    fn a_width_learns_no_window_that_shares_a_bucket_with_one_a_below_warning_read() {
        let (store, outage) = outage_and_stop();
        // Raised on the outage's fourth reading, and seen a heartbeat later. Its preset may have waited the longest any
        // waits.
        let ended = outage + 2 * HOUR;
        let started = outage + 5 * FIVE;
        warned(&store, "source_rows_below", started, Some(ended));
        let first = started - Preset::longest_wait() * FIVE;
        for width in WIDTHS {
            let w = width.ms();
            assert!(learns(&store, first - w, width), "{width:?}: the window before");
            assert!(
                !learns(&store, first + FIVE - w, width),
                "{width:?}: the first that shares a bucket with the first reading it may have waited for"
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

    /// Marked Normal six hours after the drop, or six days after it, the warning ends once the normal is learned again:
    /// a window that contains rows an editor said were normal does not wait its width to be learned.
    #[test]
    fn a_held_below_warning_marked_normal_teaches_its_level_and_ends() {
        for dropped in [END - 6 * HOUR, END - 6 * DAY_MS] {
            let store = store_with(29, |at| Some(if at >= dropped { 30.0 } else { 100.0 }));
            warned(&store, "source_rows_below", dropped + 4 * FIVE, None);
            let held = past(&store, "g", "s", None, END, &Marks::default()).unwrap().warnings[0].clone();
            assert_eq!(
                (held.from_millis, held.to_millis, held.learned),
                (dropped + 4 * FIVE, END, false)
            );
            let normal = said(None, held.from_millis);
            assert_eq!(reading_at(&store, END, &normal).unwrap().outside, None);
            assert!(
                past(&store, "g", "s", None, END + HOUR, &normal).unwrap().warnings[0].learned,
                "the warning is learned whole, including the windows after it was marked"
            );
        }
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
        let store = store_with(7, move |at| {
            (near(at, 2) || at == END - FIVE || a_week_back(at)).then_some(100.0)
        });
        assert_eq!(reading(&store), None);
        let store = store_with(7, move |at| {
            (near(at, 3) || at == END - FIVE || a_week_back(at)).then_some(100.0)
        });
        assert!(reading(&store).is_some());
    }

    #[test]
    fn a_fleet_whose_history_just_arrived_is_judged_at_its_next_reading() {
        let key = serde_json::to_string(&[("group", "g"), ("source", "s"), ("worker", "w")]).unwrap();
        let store = store_with(0, |at| (at >= END - HOUR).then_some(100.0));
        let readings = Readings::default();
        let none = Marks::default();
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
        expected_over(store, "g", "s", None, since_ms, END + 1000, point_ms, &Marks::default()).unwrap()
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

    /// The first days of a week's chart come before its rows reach back [`JUDGED_AFTER_MS`].
    #[test]
    fn a_long_chart_has_its_widest_widths_range_each_point_and_none_where_history_is_short() {
        let store = store_with(7, |_| Some(100.0));
        for (point, width) in [(HOUR, 30 * 60), (6 * HOUR, 6 * 60 * 60)] {
            let points = expected(&store, END - 7 * DAY_MS, point);
            let first = points.first().unwrap().from_millis;
            assert!((END - DAY_MS - point..=END - DAY_MS).contains(&first), "{first}");
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
            None,
            END - HOUR,
            END + 1000,
            BUCKET.bucket_ms(),
            &Marks::default(),
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

    /// A store with swings at ten seconds from 4 to 16 and at a minute from 8 to 12, in each bucket from `from` to `END`.
    fn swung_since(from: i64) -> RegistryStore {
        let store = store_with(0, |_| None);
        let mut swings = Vec::new();
        let mut at = from;
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
        store
    }

    #[test]
    fn a_fine_chart_has_the_range_its_points_swung_in() {
        let store = swung_since(END - 8 * DAY_MS);
        for (width, band) in [(10_000, (4.0, 16.0)), (60_000, (8.0, 12.0))] {
            let points =
                expected_over(&store, "g", "s", None, END - HOUR, END + 1000, width, &Marks::default()).unwrap();
            assert_eq!(points.len(), 13);
            assert!(points.iter().all(|p| (p.low, p.high) == band), "at {width}: {points:?}");
        }
    }

    /// As the wider widths, a fine chart has no range until the source's swings reach back a week.
    #[test]
    fn a_fine_chart_has_no_range_until_its_swings_reach_back_a_week() {
        let fine = |store: &RegistryStore| {
            expected_over(store, "g", "s", None, END - HOUR, END + 1000, 10_000, &Marks::default()).unwrap()
        };
        assert_eq!(fine(&swung_since(END - JUDGED_AFTER_MS - HOUR)).len(), 13);
        let half = fine(&swung_since(END - JUDGED_AFTER_MS - HOUR / 2));
        assert_eq!(half.first().map(|p| p.from_millis), Some(END - HOUR / 2));
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
        let longest =
            |store: &RegistryStore, learning: &Marks| reading_at(store, END, learning).unwrap().gap.unwrap().longest_ms;
        assert_eq!(longest(&store, &Marks::default()), 107 * FIVE);
        warned(
            &store,
            "source_rows_below",
            long + 3 * HOUR,
            Some(long + 6 * HOUR + FIVE),
        );
        assert_eq!(longest(&store, &Marks::default()), 35 * FIVE);
        assert_eq!(longest(&store, &said(None, long + 3 * HOUR)), 107 * FIVE);
    }

    #[test]
    fn a_warning_the_gap_raised_ends_at_the_first_row() {
        let silent = END - 12 * HOUR;
        let store = sparse(35, silent);
        warned(&store, "source_rows_below", END - HOUR, None);
        let key = serde_json::to_string(&[("group", "g"), ("source", "s"), ("worker", "w")]).unwrap();
        store.record_delta(ROWS_IN_METRIC, &key, END, 1.0).unwrap();
        let r = reading_at(&store, END + FIVE, &Marks::default()).unwrap();
        assert_eq!(r.gap.unwrap().silence_ms, 0);
        assert_eq!(r.outside, None);
        store.record_delta(ROWS_IN_METRIC, &key, END + FIVE, 1.0).unwrap();
        assert_eq!(
            reading_at(&store, END + 2 * FIVE, &Marks::default()).unwrap().outside,
            None,
            "the warning ends at a later reading, though the source's last row ended no quiet spell"
        );

        // Neither a bucket nobody reported nor a stop's end is a row: the warning holds through each.
        let held = |store: &RegistryStore| {
            warned(store, "source_rows_below", END - HOUR, None);
            reading_at(store, END, &Marks::default()).unwrap().outside
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
            let gap = reading_at(&store, END - FIVE, &Marks::default()).unwrap().gap.unwrap();
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
            reading_at(&read, END + FIVE, &Marks::default()).unwrap().outside,
            None,
            "the reading after the row ends it"
        );
        let missed = quiet();
        missed.record_delta(ROWS_IN_METRIC, &key, END - FIVE, 1.0).unwrap();
        missed.record_delta(ROWS_IN_METRIC, &key, END, 1.0).unwrap();
        assert_eq!(
            reading_at(&missed, END + FIVE, &Marks::default()).unwrap().outside,
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
    fn past_lists_the_warnings_and_the_longest_quiet_spells_with_what_is_learned() {
        let store = sparse(35, END - 12 * HOUR);
        reading(&store);
        warned(&store, "source_rows_below", END - 9 * DAY_MS, Some(END - 8 * DAY_MS));
        warned(&store, "source_rows_below", END - HOUR, None);
        let past = past(&store, "g", "s", None, END, &Marks::default()).unwrap();
        let span = |from_millis, to_millis| PastSpan {
            from_millis,
            to_millis,
            learned: false,
            holds: false,
            marked_by: None,
            reason: None,
        };
        assert_eq!(
            past.warnings,
            [
                PastSpan {
                    holds: true,
                    ..span(END - HOUR, END)
                },
                span(END - 9 * DAY_MS, END - 8 * DAY_MS)
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
