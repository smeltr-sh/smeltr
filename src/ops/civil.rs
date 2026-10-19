//! The proleptic Gregorian calendar against days since the Unix epoch, by Howard Hinnant's algorithms
//! (<http://howardhinnant.github.io/date_algorithms.html>), and `MICROS_PER_DAY`, which splits an instant into its
//! day and its time of day.

/// Microseconds in a day: `instant.div_euclid(MICROS_PER_DAY)` is the day, `rem_euclid` the time of day.
pub const MICROS_PER_DAY: i64 = 86_400_000_000;

/// Days since the Unix epoch of a proleptic-Gregorian date. A month or day out of its range rolls over;
/// `valid_date_days` refuses it.
pub fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400; // [0, 399]
    let mp = if m > 2 { m - 3 } else { m + 9 } as i64; // [0, 11]
    let doy = (153 * mp + 2) / 5 + d as i64 - 1; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
    era * 146_097 + doe - 719_468
}

/// Days since the Unix epoch as a proleptic-Gregorian `(year, month, day)`.
pub fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097); // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32; // [1, 12]
    (yoe + era * 400 + i64::from(m <= 2), m, d)
}

/// How far from the epoch a year read from text may lie. A day count in 32 bits, the widest date a store holds,
/// reaches about 5.9 million years either way, so every date a table can write reads back; far past this the
/// arithmetic overflows.
const FURTHEST_YEAR: i64 = 6_000_000;

/// `days_from_civil` for a date the calendar has; `None` for a month or day it does not, or a year past
/// `FURTHEST_YEAR`.
pub fn valid_date_days(y: i64, m: u32, d: u32) -> Option<i64> {
    if !(-FURTHEST_YEAR..=FURTHEST_YEAR).contains(&y) {
        return None;
    }
    let days = days_from_civil(y, m, d);
    (civil_from_days(days) == (y, m, d)).then_some(days)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn days_from_civil_matches_known_epoch_values() {
        assert_eq!(days_from_civil(1970, 1, 1), 0);
        assert_eq!(days_from_civil(1969, 12, 31), -1);
        assert_eq!(days_from_civil(2000, 1, 1), 946_684_800 / 86_400);
        assert_eq!(days_from_civil(2024, 2, 29), 19_782); // leap day
        assert_eq!(days_from_civil(2023, 2, 28), 19_416); // non-leap Feb
        assert_eq!(days_from_civil(2025, 12, 31), 20_453);
    }

    #[test]
    fn a_month_or_day_out_of_range_rolls_over() {
        assert_eq!(days_from_civil(2023, 13, 1), days_from_civil(2024, 1, 1));
        assert_eq!(days_from_civil(2023, 2, 30), days_from_civil(2023, 3, 2));
    }

    /// Either side of the epoch and across an era boundary, so the negative-era handling is covered.
    #[test]
    fn days_from_civil_is_civil_from_days_inverse_over_a_range() {
        for days in -800_000i64..10_000 {
            let (y, m, d) = civil_from_days(days);
            assert_eq!(days_from_civil(y, m, d), days, "{y}-{m}-{d}");
        }
    }

    #[test]
    fn a_date_the_calendar_lacks_is_refused() {
        assert_eq!(valid_date_days(2024, 2, 29), Some(19_782));
        assert_eq!(valid_date_days(2023, 2, 29), None);
        assert_eq!(valid_date_days(2023, 4, 31), None);
        assert_eq!(valid_date_days(2023, 13, 1), None);
        assert_eq!(valid_date_days(2023, 0, 1), None);
        assert_eq!(valid_date_days(2023, 1, 0), None);
        assert_eq!(valid_date_days(2023, u32::MAX, 1), None);
        assert_eq!(valid_date_days(2023, 1, u32::MAX), None);
    }

    /// Any signed number can arrive as a year from text; far enough from the epoch the arithmetic would overflow.
    #[test]
    fn a_year_past_the_bound_is_refused() {
        assert!(valid_date_days(FURTHEST_YEAR, 1, 1).is_some());
        assert!(valid_date_days(-FURTHEST_YEAR, 12, 31).is_some());
        assert_eq!(valid_date_days(FURTHEST_YEAR + 1, 1, 1), None);
        assert_eq!(valid_date_days(-FURTHEST_YEAR - 1, 1, 1), None);
        for days in [i64::from(i32::MIN), i64::from(i32::MAX)] {
            let (y, m, d) = civil_from_days(days);
            assert_eq!(valid_date_days(y, m, d), Some(days), "a 32-bit day count reads back");
        }
        assert_eq!(valid_date_days(i64::MAX, 1, 1), None);
        assert_eq!(valid_date_days(i64::MIN, 1, 1), None);
    }
}
