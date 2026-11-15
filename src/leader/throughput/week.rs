//! A source's days and their kinds, and its usual windows by kind.
//!
//! A source's days start at its quietest hour, where one day's traffic has ended and the next one's has not begun, so
//! a day holds one day's traffic whatever time zone the source keeps. A source whose rows differ by weekday, an
//! office's working days against its weekends, is judged at each time of day against the days of its own kind. Its
//! kinds are the weekdays split in two, or each weekday its own, kept only where they predicted each of the last
//! [`WEEKS`] weeks, held out in turn, better than one kind: a rhythm that is not there passes each week as a coin toss
//! would. Any other source has one kind.
//!
//! A window that reaches into the day before holds the end of yesterday and the start of today, and its usual window
//! is learned from days of today's kind whose day before was of yesterday's kind. Set against today's kind alone, a
//! Monday's 24 hours, which hold most of Sunday's, would read low every week, and the range, which every day shares,
//! would widen to take them in.

use std::collections::{BTreeMap, BTreeSet};

use super::{BUCKET, DAY_MS, MIN_DAYS, NEIGHBORHOOD, Rows, USUAL_DAYS, Warned, Width, quantile};

/// How many weeks the kinds are learned from, each held out in turn, and so how many a grouping must predict.
const WEEKS: i64 = 4;

/// Days a grouping needs before it is tried: two of each weekday.
const MIN_GROUPED_DAYS: usize = 14;

/// The share of a day's buckets that may be missing, or not learned, before the day is left out of the kinds: an
/// outage would otherwise read as a kind of day.
const LEFT_OUT: f64 = 0.1;

const HOUR_MS: i64 = 60 * 60 * 1000;

/// Each weekday its own kind.
const SEVEN: [u8; 7] = [0, 1, 2, 3, 4, 5, 6];

/// The kinds of one source's days: when in the UTC day its days start, and each weekday's kind, Monday first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Week {
    start_ms: i64,
    kinds: [u8; 7],
}

impl Week {
    /// The week as of `as_of`, from the rows of whole days before `as_of`'s UTC day, so that it holds all that day: its
    /// days start at the quietest hour of the [`WEEKS`] weeks of UTC days before, and its kinds are those the [`WEEKS`]
    /// weeks of its own days before predicted.
    pub(super) fn learn(rows: &Rows, warned: &Warned, as_of: i64) -> Week {
        let midnight = as_of.div_euclid(DAY_MS) * DAY_MS;
        let mut week = Week {
            start_ms: quietest_hour(rows, warned, midnight) * HOUR_MS,
            kinds: [0; 7],
        };
        let today = week.day(midnight);
        let days: Vec<Day> = (1..=WEEKS * 7)
            .filter_map(|back| Day::of(rows, warned, &week, today - back, today))
            .collect();
        week.kinds = kinds_of(&days);
        week
    }

    /// The day `at_ms` falls in, counting the one that began at the start time on 1 January 1970 as day 0.
    fn day(&self, at_ms: i64) -> i64 {
        (at_ms - self.start_ms).div_euclid(DAY_MS)
    }

    fn day_start(&self, day: i64) -> i64 {
        day * DAY_MS + self.start_ms
    }

    /// The day the window ending at `ended_ms` ends in, and its last bucket's place in that day.
    fn place(&self, ended_ms: i64) -> (i64, usize) {
        let last = ended_ms - BUCKET.bucket_ms();
        let day = self.day(last);
        (day, ((last - self.day_start(day)) / BUCKET.bucket_ms()) as usize)
    }

    fn kind(&self, day: i64) -> usize {
        self.kinds[weekday(day)] as usize
    }
}

/// Monday 0, from day 0's Thursday.
fn weekday(day: i64) -> usize {
    (day + 3).rem_euclid(7) as usize
}

/// The hour of the UTC day whose rows are fewest, by their median over the [`WEEKS`] weeks of days before `midnight`;
/// the first of equals. An hour counts on a day only where every bucket of it was read and learned.
fn quietest_hour(rows: &Rows, warned: &Warned, midnight: i64) -> i64 {
    (0..24)
        .filter_map(|hour| {
            let mut sums: Vec<f64> = (1..=WEEKS * 7)
                .filter_map(|back| {
                    let to = midnight - back * DAY_MS + (hour + 1) * HOUR_MS;
                    let (rows, missing) = rows.read(to - HOUR_MS, to)?;
                    (missing == 0 && !warned.span_during(to, HOUR_MS)).then_some(rows)
                })
                .collect();
            sums.sort_by(f64::total_cmp);
            (!sums.is_empty()).then(|| (hour, quantile(&sums, 0.5)))
        })
        .min_by(|a, b| a.1.total_cmp(&b.1))
        .map_or(0, |(hour, _)| hour)
}

/// One past day as the kinds are learned from it.
struct Day {
    /// log(1 + rows) in each hour: a weekday that reads half another's differs from it as much at any level.
    hours: [f64; 24],
    weekday: usize,
    /// Whole weeks before today: the week it is held out with.
    week: i64,
}

impl Day {
    /// `day` of `week`, or None where more than [`LEFT_OUT`] of it was missing or not learned.
    fn of(rows: &Rows, warned: &Warned, week: &Week, day: i64, today: i64) -> Option<Day> {
        let step = BUCKET.bucket_ms();
        let start = week.day_start(day);
        let buckets = DAY_MS / step;
        let (_, missing) = rows.read(start, start + DAY_MS)?;
        let unlearned = (1..=buckets)
            .filter(|b| warned.span_during(start + b * step, step))
            .count();
        if missing as f64 > LEFT_OUT * buckets as f64 || unlearned as f64 > LEFT_OUT * buckets as f64 {
            return None;
        }
        let mut hours = [0.0; 24];
        for (hour, out) in (0..).zip(hours.iter_mut()) {
            let from = start + hour * HOUR_MS;
            *out = rows.read(from, from + HOUR_MS)?.0.ln_1p();
        }
        Some(Day {
            hours,
            weekday: weekday(day),
            week: (today - 1 - day) / 7,
        })
    }
}

/// Each weekday's kind: the weekdays split in two, or each its own, whichever predicted the held-out weeks better
/// where it predicted each of them better than no kinds did; one kind where neither did. A held-out day is predicted
/// hour by hour by the median of the other weeks' days of its kind.
fn kinds_of(days: &[Day]) -> [u8; 7] {
    let mut weeks: Vec<i64> = days.iter().map(|d| d.week).collect();
    weeks.sort_unstable();
    weeks.dedup();
    if days.len() < MIN_GROUPED_DAYS || weeks.len() < WEEKS as usize {
        return [0; 7];
    }
    let mut none = vec![0.0; weeks.len()];
    let mut grouped = [vec![0.0; weeks.len()], vec![0.0; weeks.len()]];
    for (n, &held) in weeks.iter().enumerate() {
        let (out, fit): (Vec<&Day>, Vec<&Day>) = days.iter().partition(|d| d.week == held);
        let all = medians(&fit, |_| true);
        none[n] = out.iter().map(|d| distance(&d.hours, &all)).sum();
        for (errors, kinds) in grouped.iter_mut().zip([halves(&fit), SEVEN]) {
            errors[n] = out
                .iter()
                .map(|d| {
                    let of_kind = medians(&fit, |f| kinds[f.weekday] == kinds[d.weekday]);
                    let predicted: Vec<Option<f64>> = of_kind.iter().zip(&all).map(|(k, a)| k.or(*a)).collect();
                    distance(&d.hours, &predicted)
                })
                .sum();
        }
    }
    let total = |errors: &[f64]| errors.iter().sum::<f64>();
    let best = usize::from(total(&grouped[1]) < total(&grouped[0]));
    if !grouped[best].iter().zip(&none).all(|(g, n)| g < n) {
        return [0; 7];
    }
    if best == 0 {
        halves(&days.iter().collect::<Vec<_>>())
    } else {
        SEVEN
    }
}

/// The split of the weekdays into two, Monday's kind 0, whose kinds' days lie closest to their kind's median, hour by
/// hour; the first of equals.
fn halves(days: &[&Day]) -> [u8; 7] {
    (1..64u8)
        .map(|split| {
            let kinds: [u8; 7] = std::array::from_fn(|w| if w == 0 { 0 } else { (split >> (w - 1)) & 1 });
            let spread: f64 = (0..2u8)
                .map(|k| {
                    let mine: Vec<&Day> = days.iter().copied().filter(|d| kinds[d.weekday] == k).collect();
                    let median = medians(&mine, |_| true);
                    mine.iter().map(|d| distance(&d.hours, &median)).sum::<f64>()
                })
                .sum();
            (kinds, spread)
        })
        .min_by(|a, b| a.1.total_cmp(&b.1))
        .map_or([0; 7], |(kinds, _)| kinds)
}

/// The median of `days` that are `mine` at each hour, or None at every hour where none is.
fn medians(days: &[&Day], mine: impl Fn(&Day) -> bool) -> Vec<Option<f64>> {
    let mine: Vec<&&Day> = days.iter().filter(|d| mine(d)).collect();
    (0..24)
        .map(|h| {
            let mut at: Vec<f64> = mine.iter().map(|d| d.hours[h]).collect();
            at.sort_by(f64::total_cmp);
            (!at.is_empty()).then(|| quantile(&at, 0.5))
        })
        .collect()
}

/// The squared distance of `hours` from `to`, at the hours `to` has.
fn distance(hours: &[f64; 24], to: &[Option<f64>]) -> f64 {
    hours
        .iter()
        .zip(to)
        .filter_map(|(x, m)| m.map(|m| (x - m).powi(2)))
        .sum()
}

/// A usual window at each bucket of a day, None where too few days had one.
type ByAt = Vec<Option<f64>>;

/// One width's usual windows by the kinds of day of a [`Week`]. A window is placed by the day its last bucket
/// falls in and that bucket's place in the day, `at`. Its usual window is the median of the windows near `at` on the
/// newest days of its day's kind, and, where it reaches into the day before, on days whose day before was of that day's
/// kind too.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct Weekly {
    week: Week,
    /// By the kind of a day and of the day before it: the usual window ending at each bucket of the day.
    usual: BTreeMap<(usize, usize), ByAt>,
}

impl Weekly {
    /// `width`'s usual windows by `week` as of `as_of`, from its `learned` windows (how many buckets before `as_of`
    /// each ended, and its rows). Each is the median of the windows within [`NEIGHBORHOOD`] of its time on the newest
    /// [`USUAL_DAYS`] days in the last [`WEEKS`] weeks that have one, and None where fewer than [`MIN_DAYS`] days have
    /// one.
    pub(super) fn learn(as_of: i64, width: Width, learned: &[(i64, f64)], week: Week) -> Weekly {
        let step = BUCKET.bucket_ms();
        let per_day = DAY_MS / step;
        let across = NEIGHBORHOOD.as_millis() as i64 / step;
        let today = week.day(as_of);
        let first = today - WEEKS * 7;
        let index = |day: i64| usize::try_from(day - first).ok();
        let mut windows = vec![vec![None; per_day as usize]; (today - first + 1) as usize];
        for &(i, sum) in learned {
            let (day, at) = week.place(as_of - i * step);
            if let Some(of) = index(day).and_then(|d| windows.get_mut(d)) {
                of[at] = Some(sum);
            }
        }
        // A window near `at` that ends in the day before or after counts for the day it is near, as in a daily normal.
        let window_at =
            |day: i64, at: i64| windows.get(index(day + at.div_euclid(per_day))?)?[at.rem_euclid(per_day) as usize];
        let usual = |kind: usize, before: usize, at: i64| {
            let into_before = at + 1 < width.buckets();
            let days = (first..=today)
                .rev()
                .filter(|&day| week.kind(day) == kind && (!into_before || week.kind(day - 1) == before));
            let (mut sums, mut found) = (Vec::new(), 0);
            for day in days {
                if found == USUAL_DAYS as usize {
                    break;
                }
                let had = sums.len();
                sums.extend((at - across..=at + across).filter_map(|near| window_at(day, near)));
                found += usize::from(sums.len() > had);
            }
            sums.sort_by(f64::total_cmp);
            (found >= MIN_DAYS).then(|| quantile(&sums, 0.5))
        };
        let usual = (0..7)
            .map(|weekday| (week.kinds[weekday] as usize, week.kinds[(weekday + 6) % 7] as usize))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .map(|(kind, before)| ((kind, before), (0..per_day).map(|at| usual(kind, before, at)).collect()))
            .collect();
        Weekly { week, usual }
    }

    /// The usual window ending at `ended_ms`, or None where too few days had one.
    pub(super) fn at(&self, ended_ms: i64) -> Option<f64> {
        let (day, at) = self.week.place(ended_ms);
        self.usual[&(self.week.kind(day), self.week.kind(day - 1))][at]
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::super::{Learning, Preset, WIDTHS, normal};
    use super::*;

    const FIVE: i64 = 5 * 60 * 1000;
    /// A UTC midnight that begins a Monday, with more than four weeks before it.
    const MONDAY: i64 = 403 * DAY_MS;

    /// `of(bucket)` read in each five-minute bucket of the `days` days before `until`.
    fn rows(days: i64, until: i64, of: impl Fn(i64) -> f64) -> Rows {
        let from = until - days * DAY_MS;
        let buckets: BTreeMap<i64, f64> = (from..until).step_by(FIVE as usize).map(|at| (at, of(at))).collect();
        Rows::of(&buckets, from, until)
    }

    fn nothing_warned() -> Warned {
        Warned::new("g", "s", Vec::new(), Vec::new(), &Learning::default())
    }

    /// 100 rows a bucket from 06:00 to 20:00 UTC on a working day, a fifth of that at the weekend, and 10 at night.
    fn office(at: i64) -> f64 {
        let hour = at.rem_euclid(DAY_MS) / HOUR_MS;
        match ((6..20).contains(&hour), weekday(at.div_euclid(DAY_MS)) < 5) {
            (false, _) => 10.0,
            (true, true) => 100.0,
            (true, false) => 20.0,
        }
    }

    /// 40 to 160 rows, the same for the same bucket.
    fn noisy(at: i64) -> f64 {
        let mut x = at as u64 ^ 0x9e37_79b9_7f4a_7c15;
        x ^= x >> 33;
        x = x.wrapping_mul(0xff51_afd7_ed55_8ccd);
        x ^= x >> 33;
        40.0 + (x % 121) as f64
    }

    #[test]
    fn an_office_week_tells_its_working_days_from_its_weekend() {
        let week = Week::learn(&rows(30, MONDAY, office), &nothing_warned(), MONDAY + HOUR_MS);
        assert_eq!(week.kinds, [0, 0, 0, 0, 0, 1, 1]);
    }

    #[test]
    fn a_source_with_no_weekly_rhythm_has_one_kind_of_day() {
        let week = Week::learn(&rows(30, MONDAY, noisy), &nothing_warned(), MONDAY + HOUR_MS);
        assert_eq!(week.kinds, [0; 7]);
    }

    /// An office five hours behind UTC works from 13:00 to 01:00 UTC, so a UTC day would hold the end of Friday's work
    /// in Saturday. Its days start at 01:00 UTC, the first of its quiet hours, and Friday's work is all in Friday.
    #[test]
    fn a_day_starts_at_the_quietest_hour() {
        let west = |at: i64| {
            let local = at - 5 * HOUR_MS;
            let hour = local.rem_euclid(DAY_MS) / HOUR_MS;
            let working = (8..20).contains(&hour);
            match (working, weekday(local.div_euclid(DAY_MS)) < 5) {
                (false, _) => 10.0,
                (true, true) => 100.0,
                (true, false) => 20.0,
            }
        };
        let week = Week::learn(&rows(30, MONDAY, west), &nothing_warned(), MONDAY + HOUR_MS);
        assert_eq!(week.start_ms, HOUR_MS);
        assert_eq!(week.kinds, [0, 0, 0, 0, 0, 1, 1]);
    }

    /// The week reads only the days before today's UTC day, so rows read today do not change it.
    #[test]
    fn the_same_rows_give_the_same_week_all_day() {
        let rows = rows(31, MONDAY + DAY_MS, |at| if at >= MONDAY { 0.0 } else { office(at) });
        let warned = nothing_warned();
        let week = Week::learn(&rows, &warned, MONDAY);
        assert_eq!(Week::learn(&rows, &warned, MONDAY), week);
        assert_eq!(Week::learn(&rows, &warned, MONDAY + 23 * HOUR_MS), week);
        assert_eq!(week.kinds, [0, 0, 0, 0, 0, 1, 1]);
    }

    /// A Monday's 24 hours hold most of Sunday. Its usual window is made up of Sunday's end and Monday's start, and a
    /// Saturday's of Friday's end and Saturday's start. A Tuesday's is two working days'.
    #[test]
    fn a_day_window_into_the_day_before_is_that_days_part_and_todays() {
        let rows = rows(30, MONDAY, office);
        let warned = nothing_warned();
        let week = Week::learn(&rows, &warned, MONDAY);
        let normal = normal(&rows, &warned, MONDAY, WIDTHS[3], Preset::Balanced, &week).unwrap();
        let at = |day: i64, hour: i64| normal.usual.at(MONDAY + day * DAY_MS + hour * HOUR_MS);
        // In buckets: a night of 6 hours is 72 at 10, the 4 hours after 20:00 are 48, a day's 14 hours of work 168.
        let (night, evening, working, weekend) = (720.0, 480.0, 16_800.0, 3360.0);
        assert_eq!(at(-7, 6), Some(night + weekend + evening), "Monday");
        assert_eq!(at(-6, 6), Some(night + working + evening), "Tuesday");
        assert_eq!(
            at(-2, 18),
            Some(night + 144.0 * 20.0 + 24.0 * 100.0 + evening),
            "Saturday"
        );
        assert_eq!(at(-2, 0), Some(night + working + evening), "Friday, whole");
        assert_eq!((normal.low, normal.high), (1.0, 1.0));
    }

    /// A working day that reads at the weekend's level is below the range of working days. Set against every day, the
    /// weekends' windows widen the range to take it in.
    #[test]
    fn a_working_day_at_the_weekends_level_is_below_its_kinds_range() {
        let rows = rows(30, MONDAY, office);
        let warned = nothing_warned();
        let tuesday_noon = MONDAY - 6 * DAY_MS + 12 * HOUR_MS;
        let low = |week: &Week| {
            let normal = normal(&rows, &warned, MONDAY, WIDTHS[0], Preset::Balanced, week).unwrap();
            normal.band(tuesday_noon).unwrap().low
        };
        let week = Week::learn(&rows, &warned, MONDAY);
        assert_eq!(low(&week), 70.0);
        let one_kind = Week { kinds: [0; 7], ..week };
        assert!(low(&one_kind) < 20.0, "{}", low(&one_kind));
    }
}
