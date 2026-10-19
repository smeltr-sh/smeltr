//! DateTime parts (`year` .. `millisecond`, `dayOfWeek`, `epochMillis`), `now`, `fromEpochMillis`, the `Duration`
//! constructor and reader (`duration`, `countIn`), and the ISO 8601 text an instant renders as.
//!
//! The text lives with the type, not in the JSON writer: `toString(t)`, a template hole, `join` and the JSON output
//! all render through it. `format::json` adds the quotes.

use std::io::Write as _;

use super::civil::{MICROS_PER_DAY, civil_from_days};
use super::helpers::{Operand, short_circuit_empty, side};
use crate::core::column::{Column, DataType};
use crate::core::ctx::Ctx;

/// A span as the writer and `toString` render it: its count of microseconds, the unit it is held in, so every span
/// is written exactly and `parse_duration_text` reads it back.
pub fn write_duration_micros_bare(micros: i64, out: &mut Vec<u8>) {
    write!(out, "{micros}").unwrap();
}

/// A span from its text: the count of microseconds `write_duration_micros_bare` writes, whole or as a finite decimal
/// rounded to the microsecond. Surrounding whitespace is ignored. `None` for anything else.
pub fn parse_duration_text(text: &str) -> Option<i64> {
    let s = text.trim();
    if let Ok(n) = s.parse::<i64>() {
        return Some(n);
    }
    let f = s.parse::<f64>().ok()?.round();
    super::convert::in_i64_range(f).then_some(f as i64)
}

/// Epoch microseconds → `YYYY-MM-DDTHH:MM:SS.ffffffZ`, unquoted.
pub fn write_iso8601_micros_bare(micros: i64, out: &mut Vec<u8>) {
    // Floor division, so an instant before the epoch lands on the earlier day.
    let days = micros.div_euclid(MICROS_PER_DAY);
    let of_day = micros.rem_euclid(MICROS_PER_DAY);
    let (y, m, d) = civil_from_days(days);
    let hour = of_day / 3_600_000_000;
    let min = (of_day / 60_000_000) % 60;
    let sec = (of_day / 1_000_000) % 60;
    let us = of_day % 1_000_000;
    write!(out, "{y:04}-{m:02}-{d:02}T{hour:02}:{min:02}:{sec:02}.{us:06}Z").unwrap();
}

fn extract(ctx: &mut Ctx, ins: &[&Column], f: impl Fn(i64) -> i64) -> Column {
    let len = ins[0].len();
    if let Some(e) = short_circuit_empty(ins, &DataType::I64, len) {
        return e;
    }
    let a = ins[0].datetime().unwrap();
    let mut out = Column::alloc(ctx.pool, &DataType::I64, len);
    {
        let o = out.i64_mut().unwrap();
        for i in 0..len {
            o.values[i] = f(a.values[i]);
        }
        o.validity.bits_mut().copy_from_slice(a.validity.bits());
        o.validity.recount();
    }
    out
}

pub fn dt_year(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    extract(ctx, ins, |us| civil_from_days(us.div_euclid(MICROS_PER_DAY)).0)
}

pub fn dt_month(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    extract(ctx, ins, |us| civil_from_days(us.div_euclid(MICROS_PER_DAY)).1 as i64)
}

pub fn dt_day(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    extract(ctx, ins, |us| civil_from_days(us.div_euclid(MICROS_PER_DAY)).2 as i64)
}

pub fn dt_hour(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    extract(ctx, ins, |us| us.rem_euclid(MICROS_PER_DAY) / 3_600_000_000)
}

pub fn dt_minute(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    extract(ctx, ins, |us| (us.rem_euclid(MICROS_PER_DAY) / 60_000_000) % 60)
}

pub fn dt_second(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    extract(ctx, ins, |us| (us.rem_euclid(MICROS_PER_DAY) / 1_000_000) % 60)
}

/// ISO weekday, `1` = Monday .. `7` = Sunday, as Temporal's `dayOfWeek`. The epoch was a Thursday.
pub fn dt_day_of_week(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    extract(ctx, ins, |us| (us.div_euclid(MICROS_PER_DAY) + 3).rem_euclid(7) + 1)
}

/// `now()`: the instant of the records being run, broadcast to every row. There is no input column, so the row count comes from `Ctx`.
pub fn now(ctx: &mut Ctx, _ins: &[&Column]) -> Column {
    let mut out = Column::alloc(ctx.pool, &DataType::DateTime, ctx.rows);
    out.datetime_mut().unwrap().values.fill(ctx.now_micros);
    out
}

pub fn dt_millisecond(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    extract(ctx, ins, |us| us.rem_euclid(1_000_000) / 1000)
}

/// Temporal's `epochMilliseconds`: floor, so an instant before the epoch rounds toward the earlier millisecond.
pub fn dt_epoch_millis(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    extract(ctx, ins, |us| us.div_euclid(1000))
}

/// A count of `per_unit` microseconds since the epoch as a datetime. Overflow past the `i64` microsecond range is
/// null.
fn from_epoch(ctx: &mut Ctx, ins: &[&Column], per_unit: i64) -> Column {
    let len = ins[0].len();
    if let Some(e) = short_circuit_empty(ins, &DataType::DateTime, len) {
        return e;
    }
    let a = ins[0].i64().unwrap();
    let mut out = Column::alloc(ctx.pool, &DataType::DateTime, len);
    {
        let o = out.datetime_mut().unwrap();
        for i in 0..len {
            match a.values[i].checked_mul(per_unit) {
                Some(us) if a.validity.is_valid(i) => o.values[i] = us,
                _ => {
                    o.values[i] = 0;
                    o.validity.set_valid(i, false);
                }
            }
        }
        o.validity.recount();
    }
    out
}

/// `fromEpochMillis(n)`: Temporal's `Instant.fromEpochMilliseconds`.
pub fn from_epoch_millis(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    from_epoch(ctx, ins, 1000)
}

/// `epochSeconds(t)`: whole seconds since the epoch, flooring so a time before it counts backward rather than
/// toward zero.
pub fn dt_epoch_seconds(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    extract(ctx, ins, |us| us.div_euclid(1_000_000))
}

/// `fromEpochSeconds(n)`: Temporal's `Instant.fromEpochSeconds`. A second reaches the end of the range a thousand
/// times sooner than a millisecond does.
pub fn from_epoch_seconds(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    from_epoch(ctx, ins, 1_000_000)
}

/// Microseconds in one `unit`, as `(numerator, denominator)`: Temporal's names, their singulars, and the usual short
/// forms. Unknown is `None`.
///
/// A week is here because seven days is seven days. A month and a year are not: their length depends on which one,
/// and a `Duration` is a count with no calendar attached to answer that. Temporal refuses them for the same reason
/// unless given a `relativeTo` anchor.
///
/// A ratio rather than a count because a nanosecond is a thousandth of the stored unit. Keeping it exact means
/// `duration` multiplies before it divides, so a whole number of microseconds written in nanoseconds is exact, and
/// `countIn` divides by the ratio rather than by a rounded scale.
fn unit_scale(unit: &str) -> Option<(i64, i64)> {
    Some(match unit.trim().to_ascii_lowercase().as_str() {
        "ns" | "nanosecond" | "nanoseconds" => (1, 1_000),
        "us" | "microsecond" | "microseconds" => (1, 1),
        "ms" | "millisecond" | "milliseconds" => (1_000, 1),
        "s" | "sec" | "second" | "seconds" => (1_000_000, 1),
        "m" | "min" | "minute" | "minutes" => (60_000_000, 1),
        "h" | "hour" | "hours" => (3_600_000_000, 1),
        "d" | "day" | "days" => (86_400_000_000, 1),
        "w" | "week" | "weeks" => (604_800_000_000, 1),
        _ => return None,
    })
}

/// Whether `unit` names a span this table can convert. `plan::resolve` asks before the pipeline runs, so a unit
/// written as a literal that will never convert is a compile error rather than a column of nulls.
pub fn is_known_unit(unit: &str) -> bool {
    unit_scale(unit).is_some()
}

/// `duration(n, unit)`: `n` whole or fractional. An unknown unit, or a span past the `i64` microsecond range, is null.
pub fn duration_from(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    let len = ins[0].len();
    if let Some(e) = short_circuit_empty(ins, &DataType::Duration, len) {
        return e;
    }
    let units = side::var(ins[1]).unwrap();
    let mut out = Column::alloc(ctx.pool, &DataType::Duration, len);
    {
        let o = out.duration_mut().unwrap();
        let (ints, floats) = (ins[0].i64().ok(), ins[0].f64().ok());
        for i in 0..len {
            let n_valid = ints.as_ref().is_some_and(|c| c.validity.is_valid(i))
                || floats.as_ref().is_some_and(|c| c.validity.is_valid(i));
            let scale = units
                .is_valid(i)
                .then(|| unit_scale(&String::from_utf8_lossy(units.at(i))))
                .flatten();
            let span = match (n_valid, scale) {
                (true, Some((num, den))) => match (&ints, &floats) {
                    // Multiply before dividing: `duration(1000, "ns")` is exactly one microsecond, and a span finer
                    // than the stored microsecond truncates toward zero, as a fractional count of any unit already does.
                    (Some(c), _) => c.values[i].checked_mul(num).map(|v| v / den),
                    // `as i64` saturates rather than wrapping, so the range has to be tested before the cast or an
                    // impossible span would become `i64::MAX` instead of null.
                    (_, Some(c)) => {
                        let f = c.values[i] * num as f64 / den as f64;
                        (f.is_finite() && f.abs() <= i64::MAX as f64).then_some(f as i64)
                    }
                    _ => None,
                },
                _ => None,
            };
            match span {
                Some(us) => o.values[i] = us,
                None => {
                    o.values[i] = 0;
                    o.validity.set_valid(i, false);
                }
            }
        }
        o.validity.recount();
    }
    out
}

/// `countIn(d, unit)`: the whole span in `unit`, fractional, as Temporal's `Duration.total`. An unknown unit is null.
pub fn duration_count_in(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    let len = ins[0].len();
    if let Some(e) = short_circuit_empty(ins, &DataType::F64, len) {
        return e;
    }
    let d = ins[0].duration().unwrap();
    let units = side::var(ins[1]).unwrap();
    let mut out = Column::alloc(ctx.pool, &DataType::F64, len);
    {
        let o = out.f64_mut().unwrap();
        for i in 0..len {
            let scale = (d.validity.is_valid(i) && units.is_valid(i))
                .then(|| unit_scale(&String::from_utf8_lossy(units.at(i))))
                .flatten();
            match scale {
                Some((num, den)) => o.values[i] = d.values[i] as f64 * den as f64 / num as f64,
                None => {
                    o.values[i] = 0.0;
                    o.validity.set_valid(i, false);
                }
            }
        }
        o.validity.recount();
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn iso(micros: i64) -> String {
        let mut out = Vec::new();
        write_iso8601_micros_bare(micros, &mut out);
        String::from_utf8(out).unwrap()
    }

    fn micros(micros: i64) -> String {
        let mut out = Vec::new();
        write_duration_micros_bare(micros, &mut out);
        String::from_utf8(out).unwrap()
    }

    /// A span is written as its count of microseconds, exactly, and reads back from that text.
    #[test]
    fn a_span_writes_its_microsecond_count_and_reads_it_back() {
        for (n, text) in [(0, "0"), (1, "1"), (90_000_000, "90000000"), (-90_000_000, "-90000000")] {
            assert_eq!(micros(n), text);
            assert_eq!(parse_duration_text(text), Some(n));
        }
        assert_eq!(micros(i64::MAX), "9223372036854775807");
        assert_eq!(parse_duration_text(" 2.5e6 "), Some(2_500_000));
        assert_eq!(parse_duration_text("soon"), None);
        assert_eq!(parse_duration_text("1e30"), None);
    }

    /// The one text form of an instant: `toString`, a template hole, `join` and the JSON writer all render through
    /// this, so pinning it here pins all four.
    #[test]
    fn an_instant_renders_as_iso8601_with_microseconds() {
        assert_eq!(iso(0), "1970-01-01T00:00:00.000000Z");
        assert_eq!(iso(1_000_000), "1970-01-01T00:00:01.000000Z");
        assert_eq!(iso(1_788_091_200_000_000), "2026-08-30T12:00:00.000000Z");
        // Floor division: an instant before the epoch belongs to the earlier day, not to day 0.
        assert_eq!(iso(-1), "1969-12-31T23:59:59.999999Z");
    }
}
