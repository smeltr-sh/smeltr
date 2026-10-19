//! `parseTime`'s engine: a strftime-like format subset and an ordered candidate list for auto-detect.
//!
//! Auto-detect reads a run of digits as a compact date first -- `%Y%m%d%H%M%S`, then `%Y%m%d` -- and as a Unix
//! epoch when it is neither, before it tries any other text shape. A valid date is the more specific reading, so it
//! wins: `20231114` is the 14th of November, not the 231st day of 1970. The epoch's unit comes from the magnitude of
//! the integer part (`parse_epoch_digits`): seconds below 10^11, milliseconds below 10^14, microseconds below 10^17,
//! nanoseconds past that. Each bound is the year 5138 in its unit, so the only values read in the wrong unit are
//! milliseconds before March 1973, which read as seconds. A sign and a fraction are taken.
//!
//! Specifiers: `%Y` exactly four digits, `%m`/`%d` one or two digits, `%e` a day with an optional leading space
//! (syslog's `Oct  1`), `%H`/`%M`/`%S` two digits, `%I` one or two (with `%p` `AM`/`PM`), `%f` one to six fractional
//! digits, `%z` an offset (`Z`, `+HH:MM`, `+HHMM`, or a fixed named zone), `%b`/`%B` and `%a`/`%A` English
//! month/weekday names (weekdays are consumed, not checked), `%s` a Unix epoch in seconds and `%Q` one in
//! milliseconds (sign and fraction taken, as auto-detect takes them), `%%` a literal percent. Every other byte must
//! match literally and trailing input is a failure. `%s` and `%Q` name the whole instant, so a format that pairs one
//! with a date, time or zone specifier is refused. No two-digit year: every century pivot is wrong for someone. A format
//! with no `%z` reads as UTC.
//!
//! Every field is range-checked, so a stamp that names no instant is `None` rather than one that rolled over: `%H`
//! 0-23, `%I` 1-12, `%M` 0-59, `%S` 0-60, an offset within +/-23:59, and the date through `valid_date_days`. The
//! bounds admit what a real emitter writes rather than the narrowest legal set: `%S` 60 is a leap second, and `%H`
//! 24 at a zero minute, second and fraction is ISO 8601's end-of-day. Each lands on the minute or day it ends,
//! where a clock that does not model them puts it; `24:00:01` is not an hour, and fails.
//!
//! A format with no `%Y` takes its year from a reference instant (`parse_with_format_at`): the reference's year,
//! unless that puts the stamp more than a day ahead of it -- a December line read in January was written last year --
//! or more than eleven months behind it -- the reader's clock behind the writer's: next year. A `Feb 29` that lands
//! in a non-leap year is unparseable. With no reference the year stays 1970.

use super::civil::{MICROS_PER_DAY, civil_from_days, valid_date_days};

/// Consume `min_digits..=max_digits` ASCII digits at `*pos`. Nothing is consumed unless the read succeeds, so a
/// caller may try one and carry on from where it started.
fn take_digits(text: &[u8], pos: &mut usize, min_digits: usize, max_digits: usize) -> Option<i64> {
    let start = *pos;
    let mut end = start;
    while end < text.len() && end - start < max_digits && text[end].is_ascii_digit() {
        end += 1;
    }
    if end - start < min_digits {
        return None;
    }
    let v = std::str::from_utf8(&text[start..end]).ok()?.parse().ok()?;
    *pos = end;
    Some(v)
}

/// `v`, or `None` when it falls outside `[lo, hi]`. A field that names no instant fails the parse rather than rolling
/// over into the next one: `25:00` is not tomorrow at one.
fn in_range(v: i64, lo: i64, hi: i64) -> Option<i64> {
    (lo..=hi).contains(&v).then_some(v)
}

/// `%f`: fractional seconds at microsecond resolution. Every digit is consumed and the first six are kept, so a
/// nanosecond stamp -- Go's `RFC3339Nano`, `java.time.Instant`, Elasticsearch -- is truncated rather than refused,
/// and a short run pads (`.1` is 100000us).
fn take_fraction(text: &[u8], pos: &mut usize) -> Option<i64> {
    let start = *pos;
    while *pos < text.len() && text[*pos].is_ascii_digit() {
        *pos += 1;
    }
    if *pos == start {
        return None;
    }
    let mut micros = 0i64;
    for i in 0..6 {
        micros = micros * 10
            + if start + i < *pos {
                i64::from(text[start + i] - b'0')
            } else {
                0
            };
    }
    Some(micros)
}

/// A signed run of digits with an optional fraction at `*pos`, the shape of an epoch count: `(sign, whole,
/// fraction digits)`. Nothing is consumed on failure. What `%s`, `%Q` and auto-detect all read; each scales it by
/// its own unit through `scale_epoch`.
fn take_count<'a>(text: &'a [u8], pos: &mut usize) -> Option<(bool, i64, &'a [u8])> {
    let mut p = *pos;
    let negative = match text.get(p) {
        Some(b'-') => {
            p += 1;
            true
        }
        Some(b'+') => {
            p += 1;
            false
        }
        _ => false,
    };
    // A count past `i64` fails to parse, which is the range check: the largest unit is nanoseconds.
    let whole = take_digits(text, &mut p, 1, 19)?;
    let mut frac: &[u8] = &[];
    if text.get(p) == Some(&b'.') {
        let start = p + 1;
        let mut end = start;
        while end < text.len() && text[end].is_ascii_digit() {
            end += 1;
        }
        if end == start {
            return None;
        }
        frac = &text[start..end];
        p = end;
    }
    *pos = p;
    Some((negative, whole, frac))
}

/// A count in a unit of `per_unit` microseconds as epoch microseconds. The fraction's first `frac_digits` are the
/// sub-unit part down to the microsecond; the rest are dropped. `None` past the microsecond range.
fn scale_epoch(negative: bool, whole: i64, frac: &[u8], per_unit: i64, frac_digits: usize) -> Option<i64> {
    let mut sub = 0i64;
    for i in 0..frac_digits {
        sub = sub * 10 + frac.get(i).map_or(0, |c| i64::from(c - b'0'));
    }
    let micros = whole.checked_mul(per_unit)?.checked_add(sub)?;
    Some(if negative { -micros } else { micros })
}

/// `%s`/`%Q`: an epoch count in a fixed unit.
fn take_epoch(text: &[u8], pos: &mut usize, per_unit: i64, frac_digits: usize) -> Option<i64> {
    let mut p = *pos;
    let (negative, whole, frac) = take_count(text, &mut p)?;
    let us = scale_epoch(negative, whole, frac, per_unit, frac_digits)?;
    *pos = p;
    Some(us)
}

/// Named zones `%z` accepts, as fixed offsets in hours. `"utc"` precedes `"ut"` so the longer name matches first.
const NAMED_ZONES: &[(&str, i64)] = &[
    ("utc", 0),
    ("gmt", 0),
    ("est", -5),
    ("edt", -4),
    ("cst", -6),
    ("cdt", -5),
    ("mst", -7),
    ("mdt", -6),
    ("pst", -8),
    ("pdt", -7),
    ("ut", 0),
];

/// Every name is ASCII, so this compares bytes: a window that splits a codepoint is simply not this name, and the
/// rest of the table still gets its turn.
fn take_named_zone(text: &[u8], pos: &mut usize) -> Option<i64> {
    for (name, hours) in NAMED_ZONES {
        let len = name.len();
        if *pos + len <= text.len() && text[*pos..*pos + len].eq_ignore_ascii_case(name.as_bytes()) {
            *pos += len;
            return Some(hours * 3_600_000_000);
        }
    }
    None
}

/// `%z`. Returns the offset in microseconds to subtract from wall-clock time to reach UTC.
fn take_offset(text: &[u8], pos: &mut usize) -> Option<i64> {
    if let Some(off) = take_named_zone(text, pos) {
        return Some(off);
    }
    if *pos < text.len() && (text[*pos] == b'Z' || text[*pos] == b'z') {
        *pos += 1;
        return Some(0);
    }
    let sign = match text.get(*pos) {
        Some(b'+') => 1i64,
        Some(b'-') => -1i64,
        _ => return None,
    };
    *pos += 1;
    let hh = in_range(take_digits(text, pos, 2, 2)?, 0, 23)?;
    // `+05:30`, `+0530`, and a bare `+05` -- Postgres writes a whole-hour offset without its minutes.
    let mm = if *pos < text.len() && text[*pos] == b':' {
        *pos += 1;
        in_range(take_digits(text, pos, 2, 2)?, 0, 59)?
    } else {
        match take_digits(text, pos, 2, 2) {
            Some(m) => in_range(m, 0, 59)?,
            None => 0,
        }
    };
    Some(sign * (hh * 3_600_000_000 + mm * 60_000_000))
}

const MONTHS: [&str; 12] = [
    "jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec",
];

/// The index of `word` in `names`, compared without case. Bytes, so no row allocates to look a name up.
fn position_ignoring_case(names: &[&str], word: &[u8]) -> Option<usize> {
    names.iter().position(|n| word.eq_ignore_ascii_case(n.as_bytes()))
}

const FULL_MONTHS: [&str; 12] = [
    "january",
    "february",
    "march",
    "april",
    "may",
    "june",
    "july",
    "august",
    "september",
    "october",
    "november",
    "december",
];

/// The longest month or weekday name (`september`, `wednesday`).
const MAX_WORD_LEN: usize = 9;

/// A maximal run of up to `max_len` ASCII letters.
fn take_word<'a>(text: &'a [u8], pos: &mut usize, max_len: usize) -> Option<&'a [u8]> {
    let start = *pos;
    while *pos < text.len() && *pos - start < max_len && text[*pos].is_ascii_alphabetic() {
        *pos += 1;
    }
    (*pos > start).then(|| &text[start..*pos])
}

/// `%b` and `%B`: a case-insensitive English month name, abbreviated or full. strptime makes the two specifiers one,
/// so a format written with either reads text carrying the other and nobody has to guess which the data used.
fn take_month_name(text: &[u8], pos: &mut usize) -> Option<u32> {
    let start = *pos;
    let word = take_word(text, pos, MAX_WORD_LEN)?;
    if let Some(i) = position_ignoring_case(&FULL_MONTHS, word) {
        return Some(i as u32 + 1);
    }
    // An abbreviation is three letters. A longer run beginning with one keeps only those, so `%b` against `JanX`
    // consumes `Jan` and leaves the rest to the format, as a fixed-width read did.
    let i = position_ignoring_case(&MONTHS, word.get(..3)?)?;
    *pos = start + 3;
    Some(i as u32 + 1)
}

const WEEKDAY_ABBREV: [&str; 7] = ["sun", "mon", "tue", "wed", "thu", "fri", "sat"];
const WEEKDAY_FULL: [&str; 7] = [
    "sunday",
    "monday",
    "tuesday",
    "wednesday",
    "thursday",
    "friday",
    "saturday",
];

/// `%a`/`%A`: an English weekday name, abbreviated or full, consumed and discarded.
fn take_weekday(text: &[u8], pos: &mut usize) -> Option<()> {
    let word = take_word(text, pos, MAX_WORD_LEN)?;
    (position_ignoring_case(&WEEKDAY_ABBREV, word).is_some() || position_ignoring_case(&WEEKDAY_FULL, word).is_some())
        .then_some(())
}

/// `%p`: `Some(true)` for PM.
fn take_am_pm(text: &[u8], pos: &mut usize) -> Option<bool> {
    if *pos + 2 > text.len() {
        return None;
    }
    let word = std::str::from_utf8(&text[*pos..*pos + 2]).ok()?;
    let is_pm = if word.eq_ignore_ascii_case("am") {
        false
    } else if word.eq_ignore_ascii_case("pm") {
        true
    } else {
        return None;
    };
    *pos += 2;
    Some(is_pm)
}

/// Parse `text` against `fmt`, returning epoch microseconds. `None` on any mismatch, trailing input included.
pub fn parse_with_format(text: &str, fmt: &str) -> Option<i64> {
    parse_with_format_at(text, fmt, None)
}

/// The instant a yearless stamp names, given the reference instant it was read against. See the module doc.
fn infer_year(reference: i64, month: u32, day: u32, of_day: i64) -> Option<i64> {
    const DAY: i128 = MICROS_PER_DAY as i128;
    const ELEVEN_MONTHS: i128 = 334 * DAY;
    let (ry, _, _) = civil_from_days(reference.div_euclid(MICROS_PER_DAY));
    // Wide, so a year either side of the calendar's ends still compares. Only the answer has to fit.
    let at = |y: i64| valid_date_days(y, month, day).map(|days| i128::from(days) * DAY + i128::from(of_day));
    let (t, reference) = (at(ry)?, i128::from(reference));
    let t = if t > reference + DAY {
        at(ry - 1)?
    } else if t < reference - ELEVEN_MONTHS {
        at(ry + 1)?
    } else {
        t
    };
    i64::try_from(t).ok()
}

/// Parse `text` against `fmt`, returning epoch microseconds. `None` on any mismatch, trailing input included. A
/// format with no `%Y` infers its year from `reference` (the module doc), or reads 1970 without one.
pub fn parse_with_format_at(text: &str, fmt: &str, reference: Option<i64>) -> Option<i64> {
    let tb = text.as_bytes();
    let fb = fmt.as_bytes();
    let (mut ti, mut fi) = (0usize, 0usize);

    let (mut year, mut month, mut day) = (None::<i64>, 1u32, 1u32);
    let (mut hour, mut min, mut sec, mut micros, mut offset) = (0i64, 0i64, 0i64, 0i64, 0i64);
    let mut is_pm: Option<bool> = None;
    // `%s`/`%Q` name the whole instant; any field specifier beside one is a contradiction.
    let mut epoch: Option<i64> = None;
    let mut fields = 0usize;

    while fi < fb.len() {
        if fb[fi] == b'%' && fi + 1 < fb.len() {
            let spec = fb[fi + 1];
            fi += 2;
            fields += 1;
            match spec {
                b's' => epoch = Some(take_epoch(tb, &mut ti, 1_000_000, 6)?),
                b'Q' => epoch = Some(take_epoch(tb, &mut ti, 1_000, 3)?),
                b'Y' => year = Some(take_digits(tb, &mut ti, 4, 4)?),
                b'm' => month = take_digits(tb, &mut ti, 1, 2)? as u32,
                b'd' => day = take_digits(tb, &mut ti, 1, 2)? as u32,
                b'e' => {
                    if tb.get(ti) == Some(&b' ') {
                        ti += 1;
                    }
                    day = take_digits(tb, &mut ti, 1, 2)? as u32;
                }
                b'H' => hour = in_range(take_digits(tb, &mut ti, 2, 2)?, 0, 24)?,
                b'I' => hour = in_range(take_digits(tb, &mut ti, 1, 2)?, 1, 12)?,
                b'M' => min = in_range(take_digits(tb, &mut ti, 2, 2)?, 0, 59)?,
                b'S' => sec = in_range(take_digits(tb, &mut ti, 2, 2)?, 0, 60)?,
                b'f' => micros = take_fraction(tb, &mut ti)?,
                b'z' => offset = take_offset(tb, &mut ti)?,
                b'b' | b'B' => month = take_month_name(tb, &mut ti)?,
                b'a' | b'A' => take_weekday(tb, &mut ti)?,
                b'p' => is_pm = Some(take_am_pm(tb, &mut ti)?),
                b'%' if tb.get(ti) == Some(&b'%') => {
                    ti += 1;
                    fields -= 1;
                }
                _ => return None,
            }
        } else {
            if tb.get(ti) != Some(&fb[fi]) {
                return None;
            }
            ti += 1;
            fi += 1;
        }
    }
    if ti != tb.len() {
        return None;
    }
    if let Some(us) = epoch {
        return (fields == 1).then_some(us);
    }
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    // Applied after the whole format, since `%I` and `%p` may come in either order.
    if let Some(pm) = is_pm {
        if pm && hour != 12 {
            hour += 12;
        } else if !pm && hour == 12 {
            hour = 0;
        }
    }
    // ISO 8601's end-of-day, and only that: `24:00` is the midnight ending the date, any other 24 is a mis-parse.
    if hour > 24 || (hour == 24 && (min, sec, micros) != (0, 0, 0)) {
        return None;
    }

    let of_day = hour * 3_600_000_000 + min * 60_000_000 + sec * 1_000_000 + micros - offset;
    // `2026-02-30` is unparseable, as JS's `Date.parse` has it, rather than March 2nd.
    match (year, reference) {
        (Some(y), _) => Some(valid_date_days(y, month, day)? * MICROS_PER_DAY + of_day),
        (None, Some(r)) => infer_year(r, month, day, of_day),
        (None, None) => Some(valid_date_days(1970, month, day)? * MICROS_PER_DAY + of_day),
    }
}

/// Every specifier `parse_with_format_at` understands, in the order the reference documents them. Public so the
/// published reference cannot name one the parser lacks, or miss one it has
/// (`the_reference_documents_every_format_specifier`).
pub const FORMAT_SPECIFIERS: &[&str] = &[
    "%Y", "%m", "%d", "%e", "%H", "%I", "%M", "%S", "%f", "%z", "%b", "%B", "%a", "%A", "%p", "%s", "%Q", "%%",
];

/// The specifiers that name a whole instant on their own. A format holds at most one of them and nothing else.
const WHOLE_INSTANT_SPECIFIERS: &[&str] = &["%s", "%Q"];

/// Auto-detect candidates, most common first; the first match wins. The shapes are mutually exclusive
/// (`candidate_formats_are_mutually_exclusive`), which is what makes the sticky hint in `parse_auto_hinted` sound.
const CANDIDATE_FORMATS: &[&str] = &[
    // Compact digits. These two are tried before the epoch reading (`parse_auto_hinted_at`), so they stay first.
    "%Y%m%d%H%M%S",
    "%Y%m%d",
    // ISO 8601, `T` separator.
    "%Y-%m-%dT%H:%M:%S.%f%z",
    "%Y-%m-%dT%H:%M:%S%z",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M%z",
    "%Y-%m-%dT%H:%M",
    // ISO-ish, space separator.
    "%Y-%m-%d %H:%M:%S.%f%z",
    "%Y-%m-%d %H:%M:%S%z",
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
    // Slash dates, year first.
    "%Y/%m/%d %H:%M:%S",
    "%Y/%m/%d",
    // Apache/nginx combined log.
    "%d/%b/%Y:%H:%M:%S %z",
    // US-style slash dates, month first.
    "%m/%d/%Y %I:%M:%S %p",
    "%m/%d/%Y %H:%M:%S",
    "%m/%d/%Y",
    // Written month names.
    "%B %d, %Y %H:%M:%S",
    "%B %d, %Y",
    "%b %d, %Y %H:%M:%S",
    "%b %d, %Y",
    "%b %d %Y",
    "%d %B %Y",
    "%d %b %Y",
    // Syslog (RFC 3164): no year, inferred from the reference.
    "%b %e %H:%M:%S",
    // RFC 2822 / HTTP-date.
    "%a, %d %b %Y %H:%M:%S %z",
    "%d %b %Y %H:%M:%S %z",
    // Bare ISO date, loosest -- tried last.
    "%Y-%m-%d",
];

/// Auto-detect, trying `CANDIDATE_FORMATS[hint]` before the ordered scan. Returns the value and the index of the
/// candidate that matched; an out-of-range hint means no hint.
pub fn parse_auto_hinted(text: &str, hint: usize) -> Option<(i64, usize)> {
    parse_auto_hinted_at(text, hint, None)
}

/// The magnitude at which an epoch count stops reading as the smaller unit: `10^11` of a unit is the year 5138 in
/// seconds, so a count below it is seconds, below the next is milliseconds, and so on. A `usize::MAX` hint index
/// marks an epoch match, since no candidate format has that index.
const EPOCH_UNIT_BOUNDS: [(i64, i64, usize); 3] = [
    (100_000_000_000, 1_000_000, 6),
    (100_000_000_000_000, 1_000, 3),
    (100_000_000_000_000_000, 1, 0),
];
const EPOCH_HINT: usize = usize::MAX;
/// How many of `CANDIDATE_FORMATS` are the all-digit shapes read ahead of the epoch.
const COMPACT_CANDIDATES: usize = 2;

/// A run of digits, with an optional sign and an optional fraction, as epoch microseconds. The unit is the first of
/// seconds, milliseconds, microseconds and nanoseconds whose bound the integer part is below (the module doc). `None`
/// for anything that is not that shape, and for a count that does not fit in microseconds. Surrounding whitespace is
/// ignored, as `parseInt` ignores it: a CSV cell often carries the space after its delimiter.
pub fn parse_epoch_digits(text: &str) -> Option<i64> {
    let b = text.trim().as_bytes();
    let mut pos = 0;
    let (negative, whole, frac) = take_count(b, &mut pos)?;
    if pos != b.len() {
        return None;
    }
    match EPOCH_UNIT_BOUNDS.iter().copied().find(|(bound, _, _)| whole < *bound) {
        Some((_, per_unit, frac_digits)) => scale_epoch(negative, whole, frac, per_unit, frac_digits),
        // Nanoseconds: the fraction is below the microsecond and goes with the last three digits.
        None => Some(if negative { -(whole / 1_000) } else { whole / 1_000 }),
    }
}

/// Whether every `%` in `fmt` opens a specifier `parse_with_format_at` takes, and an epoch specifier stands alone.
/// `Err` names the first one that does not, so a format can be refused where it is written rather than fail every
/// row.
pub fn check_format(fmt: &str) -> Result<(), String> {
    let mut chars = fmt.chars();
    let mut whole: Option<String> = None;
    let mut fields = 0usize;
    while let Some(c) = chars.next() {
        if c != '%' {
            continue;
        }
        let spec = match chars.next() {
            Some(next) => format!("%{next}"),
            None => return Err("a trailing `%` opens no specifier".to_string()),
        };
        if !FORMAT_SPECIFIERS.contains(&spec.as_str()) {
            return Err(format!("`{spec}` is not a specifier parseTime takes"));
        }
        if spec != "%%" {
            fields += 1;
        }
        if WHOLE_INSTANT_SPECIFIERS.contains(&spec.as_str()) {
            whole = Some(spec);
        }
    }
    match whole {
        Some(spec) if fields > 1 => Err(format!("`{spec}` names the whole instant and takes no other specifier")),
        _ => Ok(()),
    }
}

/// `parse_auto_hinted` with the reference a yearless candidate infers its year from.
pub fn parse_auto_hinted_at(text: &str, hint: usize, reference: Option<i64>) -> Option<(i64, usize)> {
    // A run of digits: a compact date if it is one, else an epoch. The compact shapes are the first two candidates
    // and fail on the first byte of any other text, so trying them here costs nothing on the common case.
    for (i, fmt) in CANDIDATE_FORMATS[..COMPACT_CANDIDATES].iter().enumerate() {
        if let Some(us) = parse_with_format_at(text, fmt, reference) {
            return Some((us, i));
        }
    }
    if let Some(us) = parse_epoch_digits(text) {
        return Some((us, EPOCH_HINT));
    }
    if let Some(fmt) = CANDIDATE_FORMATS.get(hint)
        && let Some(us) = parse_with_format_at(text, fmt, reference)
    {
        return Some((us, hint));
    }
    CANDIDATE_FORMATS
        .iter()
        .enumerate()
        .find_map(|(i, fmt)| parse_with_format_at(text, fmt, reference).map(|us| (us, i)))
}

/// Whether `text` reads as a time an author would declare one: a shape the automatic reading takes, with a count
/// only when it has an epoch's digits ([`is_epoch_count`]). Every count reads as some epoch, so its length is what
/// tells a time from an id or a port. `hint` is [`parse_auto_hinted`]'s, kept from value to value of one field.
pub fn reads_as_time(text: &str, hint: &mut usize) -> bool {
    match parse_auto_hinted_at(text, *hint, None) {
        Some((_, EPOCH_HINT)) => is_epoch_count(text),
        Some((_, i)) => {
            *hint = i;
            true
        }
        None => false,
    }
}

/// Whether `text` is an unsigned count with the digits of an epoch in seconds, milliseconds, microseconds or
/// nanoseconds of this era: 10, 13, 16 or 19 before any fraction.
pub fn is_epoch_count(text: &str) -> bool {
    let text = text.trim();
    let whole = text.split_once('.').map_or(text, |(w, _)| w);
    matches!(whole.len(), 10 | 13 | 16 | 19) && whole.bytes().all(|b| b.is_ascii_digit())
}

pub fn parse_auto(text: &str) -> Option<i64> {
    parse_auto_hinted(text, usize::MAX).map(|(us, _)| us)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ops::civil::days_from_civil;

    /// A yearless stamp read against the last instant the type holds, or the first, takes its year by the same rule
    /// as any other. It reads as nothing only when that year's date falls off the calendar.
    #[test]
    fn a_reference_at_either_end_of_the_calendar_reads_without_overflow() {
        let read = |text, reference| parse_with_format_at(text, "%b %d", Some(reference));
        let at = |y, m, d| days_from_civil(y, m, d) * MICROS_PER_DAY;
        assert_eq!(read("Jan 01", i64::MAX), Some(at(294_247, 1, 1)));
        assert_eq!(read("Dec 31", i64::MAX), Some(at(294_246, 12, 31)), "the year before");
        assert_eq!(read("Jan 11", i64::MAX), None, "past the last instant");
        assert_eq!(read("Jan 01", i64::MIN), Some(at(-290_307, 1, 1)), "the year after");
        assert_eq!(read("Dec 31", i64::MIN), None, "before the first instant");
    }

    /// The ladder at each bound: one below reads in the smaller unit, the bound itself in the next. Every case is the
    /// same instant, 2023-11-14T22:13:20Z, written in each unit, plus the two edges of the seconds band.
    #[test]
    fn epoch_digits_pick_the_unit_by_magnitude() {
        let t = 1_700_000_000_000_000i64;
        assert_eq!(parse_epoch_digits("1700000000"), Some(t));
        assert_eq!(parse_epoch_digits("1700000000000"), Some(t));
        assert_eq!(parse_epoch_digits("1700000000000000"), Some(t));
        assert_eq!(parse_epoch_digits("1700000000000000000"), Some(t));
        // 10^11 - 1 is seconds; 10^11 is milliseconds.
        assert_eq!(parse_epoch_digits("99999999999"), Some(99_999_999_999 * 1_000_000));
        assert_eq!(parse_epoch_digits("100000000000"), Some(100_000_000_000 * 1_000));
        // 10^14 - 1 is milliseconds; 10^14 is microseconds.
        assert_eq!(parse_epoch_digits("99999999999999"), Some(99_999_999_999_999 * 1_000));
        assert_eq!(parse_epoch_digits("100000000000000"), Some(100_000_000_000_000));
        // 10^17 - 1 is microseconds; 10^17 is nanoseconds.
        assert_eq!(parse_epoch_digits("99999999999999999"), Some(99_999_999_999_999_999));
        assert_eq!(parse_epoch_digits("100000000000000000"), Some(100_000_000_000_000));
    }

    #[test]
    fn epoch_digits_take_a_sign_and_a_fraction() {
        assert_eq!(parse_epoch_digits(" 1700000000 "), Some(1_700_000_000_000_000));
        assert_eq!(parse_epoch_digits("-1"), Some(-1_000_000));
        assert_eq!(parse_epoch_digits("+1"), Some(1_000_000));
        assert_eq!(parse_epoch_digits("1700000000.5"), Some(1_700_000_000_500_000));
        assert_eq!(parse_epoch_digits("1700000000.123456789"), Some(1_700_000_000_123_456));
        assert_eq!(parse_epoch_digits("1700000000000.25"), Some(1_700_000_000_000_250));
        assert_eq!(parse_epoch_digits("1700000000000000.9"), Some(1_700_000_000_000_000));
    }

    #[test]
    fn epoch_digits_refuse_what_is_not_a_bare_count() {
        for text in ["", "-", "1.", ".5", "1e9", "2026-01-01", "12:30", "1 700", "0x10"] {
            assert_eq!(parse_epoch_digits(text), None, "{text:?}");
        }
        // Past `i64`: not a count in any unit.
        assert_eq!(parse_epoch_digits("9999999999999999999"), None);
    }

    /// A run of digits that is a valid compact date is that date; one that is not is an epoch.
    #[test]
    fn auto_detect_reads_a_compact_date_before_an_epoch() {
        assert_eq!(parse_auto("20231114"), parse_with_format("2023-11-14", "%Y-%m-%d"));
        assert_eq!(
            parse_auto("20231114221320"),
            parse_with_format("2023-11-14 22:13:20", "%Y-%m-%d %H:%M:%S")
        );
        // Month 13: not a date, so the eight digits are seconds.
        assert_eq!(parse_auto("20231314"), Some(20_231_314 * 1_000_000));
        // Ten digits are never a compact date: `%Y%m%d` leaves two over.
        assert_eq!(parse_auto("2023111422"), Some(2_023_111_422 * 1_000_000));
    }

    #[test]
    fn auto_detect_reads_an_epoch_before_any_text_shape() {
        assert_eq!(parse_auto("1700000000"), Some(1_700_000_000_000_000));
        assert_eq!(parse_auto("1700000000000"), Some(1_700_000_000_000_000));
        assert_eq!(parse_auto("2023-11-14T22:13:20Z"), Some(1_700_000_000_000_000));
        // The epoch match leaves the candidate hint alone: the next text row still finds its shape.
        let (_, hint) = parse_auto_hinted("1700000000", usize::MAX).unwrap();
        assert_eq!(
            parse_auto_hinted("2023-11-14T22:13:20Z", hint).map(|(us, _)| us),
            Some(1_700_000_000_000_000)
        );
    }

    /// Every count reads as some epoch, so a count reads as a time only with an epoch's digits; any other shape the
    /// automatic reading takes does.
    #[test]
    fn a_time_is_a_shape_or_a_count_with_an_epochs_digits() {
        let mut hint = usize::MAX;
        for time in [
            "2023-11-14T22:13:20Z",
            "Nov 14 22:13:20",
            "20231114",
            "1700000000",
            "1700000000123.5",
        ] {
            assert!(reads_as_time(time, &mut hint), "{time}");
        }
        for not in ["8080", "17000000001", "-1700000000", "okta:system", ""] {
            assert!(!reads_as_time(not, &mut hint), "{not}");
        }
        assert!(is_epoch_count("1700000000000000000"));
        assert!(!is_epoch_count("1700000000x"));
    }

    #[test]
    fn check_format_names_the_first_specifier_it_lacks() {
        assert_eq!(check_format("%Y/%m/%d %H:%M:%S"), Ok(()));
        assert_eq!(check_format("100%% sure %b"), Ok(()));
        assert!(check_format("%Y-%m-%d %J").unwrap_err().contains("%J"));
        assert!(check_format("%s %H").unwrap_err().contains("%s"));
        assert!(check_format("%Q %Y").unwrap_err().contains("%Q"));
        assert_eq!(check_format("ts=%s"), Ok(()));
        assert_eq!(check_format("%Q%%"), Ok(()));
        assert!(check_format("%Y-%m-%d %").unwrap_err().contains("trailing"));
        assert_eq!(check_format("no specifiers at all"), Ok(()));
    }

    #[test]
    fn iso8601_with_fractional_seconds_and_zulu() {
        let us = parse_with_format("2026-08-25T12:30:45.123456Z", "%Y-%m-%dT%H:%M:%S.%fZ");
        assert_eq!(
            us,
            Some(days_from_civil(2026, 8, 25) * MICROS_PER_DAY + (12 * 3600 + 30 * 60 + 45) * 1_000_000 + 123_456)
        );
    }

    #[test]
    fn positive_offset_is_earlier_in_utc() {
        let with_offset = parse_with_format("2026-01-01T12:00:00+05:00", "%Y-%m-%dT%H:%M:%S%z");
        let utc_equivalent = parse_with_format("2026-01-01T07:00:00Z", "%Y-%m-%dT%H:%M:%SZ");
        assert_eq!(with_offset, utc_equivalent);
    }

    #[test]
    fn negative_offset_without_colon() {
        let a = parse_with_format("2026-01-01T07:00:00-0500", "%Y-%m-%dT%H:%M:%S%z");
        let b = parse_with_format("2026-01-01T12:00:00Z", "%Y-%m-%dT%H:%M:%SZ");
        assert_eq!(a, b);
    }

    #[test]
    fn apache_log_format_with_month_name() {
        let us = parse_with_format("10/Oct/2000:13:55:36 -0700", "%d/%b/%Y:%H:%M:%S %z");
        let expected = parse_with_format("2000-10-10T20:55:36Z", "%Y-%m-%dT%H:%M:%SZ");
        assert_eq!(us, expected);
    }

    #[test]
    fn month_name_matching_is_case_insensitive() {
        assert_eq!(
            parse_with_format("2000-JAN-01", "%Y-%b-%d"),
            parse_with_format("2000-Jan-01", "%Y-%b-%d")
        );
    }

    #[test]
    fn bare_date_defaults_time_to_midnight() {
        let us = parse_with_format("2026-08-25", "%Y-%m-%d").unwrap();
        assert_eq!(us, days_from_civil(2026, 8, 25) * MICROS_PER_DAY);
    }

    #[test]
    fn mismatched_literal_text_fails() {
        assert_eq!(parse_with_format("2026/08/25", "%Y-%m-%d"), None);
    }

    #[test]
    fn trailing_input_the_format_does_not_consume_fails() {
        assert_eq!(parse_with_format("2026-08-25extra", "%Y-%m-%d"), None);
    }

    #[test]
    fn out_of_range_month_or_day_fails() {
        assert_eq!(parse_with_format("2026-13-01", "%Y-%m-%d"), None);
        assert_eq!(parse_with_format("2026-01-32", "%Y-%m-%d"), None);
    }

    #[test]
    fn too_few_digits_fails_rather_than_reading_short() {
        assert_eq!(parse_with_format("202-08-25", "%Y-%m-%d"), None);
    }

    // -- variable-width %m/%d ------------------------------------------------

    #[test]
    fn month_and_day_accept_a_missing_leading_zero() {
        let a = parse_with_format("2026-8-5", "%Y-%m-%d");
        let b = parse_with_format("2026-08-05", "%Y-%m-%d");
        assert!(a.is_some());
        assert_eq!(a, b);
    }

    // -- %B / %A -- full month and weekday names -----------------------------

    #[test]
    fn full_month_name_is_case_insensitive() {
        let a = parse_with_format("August 25, 2026", "%B %d, %Y");
        let b = parse_with_format("august 25, 2026", "%B %d, %Y");
        assert!(a.is_some());
        assert_eq!(a, b);
        assert_eq!(a, parse_with_format("2026-08-25", "%Y-%m-%d"));
    }

    #[test]
    fn unrecognized_month_word_fails() {
        assert_eq!(parse_with_format("Augtober 25, 2026", "%B %d, %Y"), None);
    }

    #[test]
    fn weekday_name_is_consumed_and_discarded_abbreviated_or_full() {
        let expected = parse_with_format("2026-08-25", "%Y-%m-%d");
        assert_eq!(parse_with_format("Tue, 2026-08-25", "%a, %Y-%m-%d"), expected);
        assert_eq!(parse_with_format("Tuesday, 2026-08-25", "%A, %Y-%m-%d"), expected);
        assert_eq!(
            parse_with_format("Friday, 2026-08-25", "%A, %Y-%m-%d"),
            expected,
            "weekday names are not cross-checked"
        );
    }

    #[test]
    fn unrecognized_weekday_word_fails() {
        assert_eq!(parse_with_format("Notaday, 2026-08-25", "%A, %Y-%m-%d"), None);
    }

    // -- %I / %p -- 12-hour clock ---------------------------------------------

    #[test]
    fn twelve_hour_clock_matches_twenty_four_hour_for_ordinary_hours() {
        let am = parse_with_format("2026-08-25 3:30:00 AM", "%Y-%m-%d %I:%M:%S %p");
        let h24 = parse_with_format("2026-08-25 03:30:00", "%Y-%m-%d %H:%M:%S");
        assert_eq!(am, h24);

        let pm = parse_with_format("2026-08-25 3:30:00 PM", "%Y-%m-%d %I:%M:%S %p");
        let h24 = parse_with_format("2026-08-25 15:30:00", "%Y-%m-%d %H:%M:%S");
        assert_eq!(pm, h24);
    }

    #[test]
    fn twelve_am_is_midnight_and_twelve_pm_is_noon() {
        let midnight = parse_with_format("2026-08-25 12:00:00 AM", "%Y-%m-%d %I:%M:%S %p");
        assert_eq!(midnight, parse_with_format("2026-08-25 00:00:00", "%Y-%m-%d %H:%M:%S"));

        let noon = parse_with_format("2026-08-25 12:00:00 PM", "%Y-%m-%d %I:%M:%S %p");
        assert_eq!(noon, parse_with_format("2026-08-25 12:00:00", "%Y-%m-%d %H:%M:%S"));
    }

    #[test]
    fn am_pm_marker_is_case_insensitive() {
        let a = parse_with_format("2026-08-25 3:00:00 pm", "%Y-%m-%d %I:%M:%S %p");
        let b = parse_with_format("2026-08-25 3:00:00 PM", "%Y-%m-%d %I:%M:%S %p");
        assert!(a.is_some());
        assert_eq!(a, b);
    }

    // -- named time zones -----------------------------------------------------

    #[test]
    fn utc_and_gmt_are_zero_offset() {
        let z = parse_with_format("2026-08-25T12:00:00Z", "%Y-%m-%dT%H:%M:%S%z");
        assert_eq!(z, parse_with_format("2026-08-25T12:00:00UTC", "%Y-%m-%dT%H:%M:%S%z"));
        assert_eq!(z, parse_with_format("2026-08-25T12:00:00GMT", "%Y-%m-%dT%H:%M:%S%z"));
        assert_eq!(z, parse_with_format("2026-08-25T12:00:00UT", "%Y-%m-%dT%H:%M:%S%z"));
    }

    #[test]
    fn named_us_zones_apply_their_fixed_offset() {
        let named = parse_with_format("2026-08-25T12:00:00EST", "%Y-%m-%dT%H:%M:%S%z");
        let explicit = parse_with_format("2026-08-25T12:00:00-05:00", "%Y-%m-%dT%H:%M:%S%z");
        assert_eq!(named, explicit);

        let named = parse_with_format("2026-08-25T12:00:00PDT", "%Y-%m-%dT%H:%M:%S%z");
        let explicit = parse_with_format("2026-08-25T12:00:00-07:00", "%Y-%m-%dT%H:%M:%S%z");
        assert_eq!(named, explicit);
    }

    #[test]
    fn unrecognized_zone_name_fails() {
        assert_eq!(parse_with_format("2026-08-25T12:00:00XYZ", "%Y-%m-%dT%H:%M:%S%z"), None);
    }

    #[test]
    fn parse_auto_covers_common_date_and_datetime_shapes() {
        let cases: &[&str] = &[
            "2026-08-25T12:30:45.123456Z",
            "2026-08-25T12:30:45Z",
            "2026-08-25T12:30:45+05:00",
            "2026-08-25T12:30:45",
            "2026-08-25T12:30",
            "2026-08-25 12:30:45",
            "2026-08-25 12:30",
            "2026-8-5",
            "2026/08/25",
            "2026/08/25 12:30:45",
            "08/25/2026",
            "8/25/2026",
            "08/25/2026 12:30:45",
            "08/25/2026 3:30:00 PM",
            "10/Oct/2000:13:55:36 -0700",
            "August 25, 2026",
            "August 25, 2026 12:30:45",
            "Aug 25, 2026",
            "Aug 25 2026",
            "25 August 2026",
            "25 Aug 2026",
            "Tue, 25 Aug 2026 12:30:45 GMT",
            "Tue, 25 Aug 2026 12:30:45 +0000",
            "25 Aug 2026 12:30:45 -0700",
            "2026-08-25",
        ];
        for &s in cases {
            assert!(parse_auto(s).is_some(), "expected {s:?} to parse");
        }
    }

    #[test]
    fn parse_auto_agrees_on_the_same_instant_across_equivalent_shapes() {
        let iso = parse_auto("2026-08-25T15:30:00Z").unwrap();
        assert_eq!(parse_auto("2026-08-25T15:30:00Z"), Some(iso));
        assert_eq!(parse_auto("2026-08-25 15:30:00"), Some(iso));
        assert_eq!(parse_auto("08/25/2026 3:30:00 PM"), Some(iso));
        assert_eq!(parse_auto("August 25, 2026 15:30:00"), Some(iso));
        assert_eq!(parse_auto("Tue, 25 Aug 2026 15:30:00 GMT"), Some(iso));
        assert_eq!(parse_auto("25 Aug 2026 08:30:00 -0700"), Some(iso));
    }

    #[test]
    fn parse_auto_still_rejects_garbage() {
        assert_eq!(parse_auto("not a timestamp"), None);
        assert_eq!(parse_auto(""), None);
        assert_eq!(parse_auto("2026-13-40"), None, "invalid month/day");
    }

    #[test]
    fn a_timestamp_with_no_timezone_defaults_to_utc() {
        let utc = parse_auto("2026-08-25T15:30:00Z").unwrap();
        for s in [
            "2026-08-25T15:30:00",
            "2026-08-25 15:30:00",
            "2026/08/25 15:30:00",
            "08/25/2026 15:30:00",
            "08/25/2026 3:30:00 PM",
            "August 25, 2026 15:30:00",
        ] {
            assert_eq!(parse_auto(s), Some(utc), "{s:?} should read as UTC");
        }
    }

    #[test]
    fn an_explicit_format_with_no_percent_z_also_defaults_to_utc() {
        let with_zone = parse_with_format("2026-08-25T15:30:00Z", "%Y-%m-%dT%H:%M:%S%z");
        let without_zone_in_format = parse_with_format("2026-08-25T15:30:00", "%Y-%m-%dT%H:%M:%S");
        assert_eq!(with_zone, without_zone_in_format);
    }

    #[test]
    fn hinted_and_unhinted_agree_on_every_result() {
        for s in [
            "2026-08-25T12:30:45.123456+05:00",
            "2026-08-25 12:30:45",
            "08/25/2026 3:30:00 PM",
            "August 25, 2026",
            "Tue, 25 Aug 2026 12:30:45 GMT",
            "not a timestamp",
        ] {
            let unhinted = parse_auto(s);
            for hint in [0usize, 5, 13, 26, usize::MAX] {
                assert_eq!(
                    parse_auto_hinted(s, hint).map(|(us, _)| us),
                    unhinted,
                    "{s:?} with hint {hint}"
                );
            }
        }
    }

    /// The caller hands each row's index on to the next row, so a column in one format must keep its hint. `%b` and
    /// `%B` read the same text, so some samples also match an earlier candidate. A scan that ignored the hint would
    /// hand that one back.
    #[test]
    fn a_hint_that_matched_comes_back_unchanged() {
        for (fmt, sample) in one_sample_per_candidate() {
            let hint = CANDIDATE_FORMATS.iter().position(|f| *f == fmt).unwrap();
            assert_eq!(
                parse_auto_hinted(sample, hint).map(|(_, idx)| idx),
                Some(hint),
                "{sample:?} hinted {fmt:?}"
            );
        }
    }

    /// The index handed on is a format that reads the row, so the next row in that format matches on its hint.
    #[test]
    fn a_wrong_hint_hands_on_the_candidate_that_matched() {
        let rfc_idx = CANDIDATE_FORMATS
            .iter()
            .position(|f| *f == "%a, %d %b %Y %H:%M:%S %z")
            .unwrap();
        let (us, idx) = parse_auto_hinted("2026-08-25", rfc_idx).unwrap();
        assert_eq!(Some(us), parse_auto("2026-08-25"));
        assert_eq!(parse_with_format("2026-08-25", CANDIDATE_FORMATS[idx]), Some(us));
    }

    #[test]
    fn an_out_of_range_hint_is_the_same_as_no_hint() {
        assert_eq!(
            parse_auto_hinted("2026-08-25", 9_999).map(|(us, _)| us),
            parse_auto("2026-08-25")
        );
    }

    /// One input per `CANDIDATE_FORMATS` entry.
    fn one_sample_per_candidate() -> Vec<(&'static str, &'static str)> {
        vec![
            ("%Y%m%d%H%M%S", "20260825123045"),
            ("%Y%m%d", "20260825"),
            ("%Y-%m-%dT%H:%M:%S.%f%z", "2026-08-25T12:30:45.123456+05:00"),
            ("%Y-%m-%dT%H:%M:%S%z", "2026-08-25T12:30:45+05:00"),
            ("%Y-%m-%dT%H:%M:%S.%f", "2026-08-25T12:30:45.123456"),
            ("%Y-%m-%dT%H:%M:%S", "2026-08-25T12:30:45"),
            ("%Y-%m-%dT%H:%M%z", "2026-08-25T12:30+05:00"),
            ("%Y-%m-%dT%H:%M", "2026-08-25T12:30"),
            ("%Y-%m-%d %H:%M:%S.%f%z", "2026-08-25 12:30:45.123456+05:00"),
            ("%Y-%m-%d %H:%M:%S%z", "2026-08-25 12:30:45+05:00"),
            ("%Y-%m-%d %H:%M:%S.%f", "2026-08-25 12:30:45.123456"),
            ("%Y-%m-%d %H:%M:%S", "2026-08-25 12:30:45"),
            ("%Y-%m-%d %H:%M", "2026-08-25 12:30"),
            ("%Y/%m/%d %H:%M:%S", "2026/08/25 12:30:45"),
            ("%Y/%m/%d", "2026/08/25"),
            ("%d/%b/%Y:%H:%M:%S %z", "25/Aug/2026:12:30:45 +0500"),
            ("%m/%d/%Y %I:%M:%S %p", "08/25/2026 03:30:45 PM"),
            ("%m/%d/%Y %H:%M:%S", "08/25/2026 15:30:45"),
            ("%m/%d/%Y", "08/25/2026"),
            ("%B %d, %Y %H:%M:%S", "August 25, 2026 12:30:45"),
            ("%B %d, %Y", "August 25, 2026"),
            ("%b %d, %Y %H:%M:%S", "Aug 25, 2026 12:30:45"),
            ("%b %d, %Y", "Aug 25, 2026"),
            ("%b %d %Y", "Aug 25 2026"),
            ("%d %B %Y", "25 August 2026"),
            ("%d %b %Y", "25 Aug 2026"),
            ("%b %e %H:%M:%S", "Aug 25 12:30:45"),
            ("%a, %d %b %Y %H:%M:%S %z", "Tue, 25 Aug 2026 12:30:45 +0500"),
            ("%d %b %Y %H:%M:%S %z", "25 Aug 2026 12:30:45 +0500"),
            ("%Y-%m-%d", "2026-08-25"),
        ]
    }

    #[test]
    fn every_candidate_format_has_exactly_one_sample_above() {
        let samples = one_sample_per_candidate();
        assert_eq!(samples.len(), CANDIDATE_FORMATS.len());
        for (fmt, sample) in &samples {
            assert!(
                CANDIDATE_FORMATS.contains(fmt),
                "{fmt:?} is not in CANDIDATE_FORMATS -- stale sample list"
            );
            assert!(
                parse_with_format(sample, fmt).is_some(),
                "sample {sample:?} does not actually match its own format {fmt:?}"
            );
        }
        for fmt in CANDIDATE_FORMATS {
            assert!(
                samples.iter().any(|(f, _)| f == fmt),
                "{fmt:?} has no sample -- add one to one_sample_per_candidate"
            );
        }
    }

    /// The property the hint relies on: every candidate that matches a
    /// sample yields the same instant.
    #[test]
    fn candidate_formats_are_mutually_exclusive() {
        for (owning_fmt, sample) in one_sample_per_candidate() {
            let hits: Vec<(usize, i64)> = CANDIDATE_FORMATS
                .iter()
                .enumerate()
                .filter_map(|(i, fmt)| parse_with_format(sample, fmt).map(|us| (i, us)))
                .collect();
            assert!(
                !hits.is_empty(),
                "{sample:?} (meant for {owning_fmt:?}) matched nothing"
            );
            let first_value = hits[0].1;
            for (i, value) in &hits {
                assert_eq!(
                    *value, first_value,
                    "{sample:?} matched candidate {i} ({}) with a different \
                     result than candidate {} -- the hint fast path could \
                     silently pick either one",
                    CANDIDATE_FORMATS[*i], hits[0].0
                );
            }
        }
    }

    // -- yearless formats ------------------------------------------------------

    const DAY: i64 = MICROS_PER_DAY;

    fn at(iso: &str) -> i64 {
        parse_with_format(iso, "%Y-%m-%dT%H:%M:%SZ").unwrap()
    }

    #[test]
    fn e_takes_a_space_padded_or_bare_day() {
        let padded = parse_with_format("Oct  1 22:14:15", "%b %e %H:%M:%S");
        let bare = parse_with_format("Oct 1 22:14:15", "%b %e %H:%M:%S");
        let zero = parse_with_format("Oct 01 22:14:15", "%b %e %H:%M:%S");
        assert!(padded.is_some());
        assert_eq!(padded, bare);
        assert_eq!(padded, zero);
        assert_eq!(
            parse_with_format("Oct  1 22:14:15", "%b %d %H:%M:%S"),
            None,
            "`%d` wants a digit"
        );
    }

    #[test]
    fn without_a_reference_a_yearless_stamp_stays_in_1970() {
        let us = parse_with_format("Oct 11 22:14:15", "%b %e %H:%M:%S").unwrap();
        assert_eq!(civil_from_days(us.div_euclid(DAY)), (1970, 10, 11));
    }

    #[test]
    fn a_yearless_stamp_takes_the_references_year() {
        let us = parse_with_format_at("Oct 11 22:14:15", "%b %e %H:%M:%S", Some(at("2026-11-20T00:00:00Z"))).unwrap();
        assert_eq!(us, at("2026-10-11T22:14:15Z"));
    }

    /// A December line read in January was written last year; a January line read by a clock still in December
    /// belongs to next year.
    #[test]
    fn the_year_rolls_over_at_the_boundary_in_both_directions() {
        let fmt = "%b %e %H:%M:%S";
        let late = parse_with_format_at("Dec 31 23:59:58", fmt, Some(at("2027-01-01T00:00:01Z"))).unwrap();
        assert_eq!(late, at("2026-12-31T23:59:58Z"));
        let early = parse_with_format_at("Jan  1 00:00:01", fmt, Some(at("2026-12-31T23:59:58Z"))).unwrap();
        assert_eq!(early, at("2027-01-01T00:00:01Z"));
        // Inside the window nothing moves: a stamp a few hours ahead is clock skew, not last year.
        let skew = parse_with_format_at("Jun 15 13:00:00", fmt, Some(at("2026-06-15T12:00:00Z"))).unwrap();
        assert_eq!(skew, at("2026-06-15T13:00:00Z"));
    }

    #[test]
    fn a_feb_29_that_lands_in_a_non_leap_year_is_unparseable() {
        assert_eq!(
            parse_with_format_at("Feb 29 12:00:00", "%b %e %H:%M:%S", Some(at("2027-03-01T00:00:00Z"))),
            None
        );
        assert_eq!(
            parse_with_format_at("Feb 29 12:00:00", "%b %e %H:%M:%S", Some(at("2028-03-01T00:00:00Z"))),
            Some(at("2028-02-29T12:00:00Z"))
        );
    }

    /// JS's `Date.parse` rejects these; rolling `Feb 30` to `Mar 2` would put a plausible wrong instant in the pipeline.
    #[test]
    fn a_day_past_the_end_of_the_month_is_unparseable() {
        let fmt = "%Y-%m-%d";
        assert_eq!(parse_with_format("2026-02-30", fmt), None);
        assert_eq!(parse_with_format("2026-04-31", fmt), None);
        assert_eq!(parse_with_format("2027-02-29", fmt), None, "not a leap year");
        assert!(parse_with_format("2028-02-29", fmt).is_some(), "a leap year");
        assert!(parse_with_format("2026-01-31", fmt).is_some());
    }

    #[test]
    fn an_explicit_year_ignores_the_reference() {
        let us = parse_with_format_at(
            "2020-05-05T00:00:00Z",
            "%Y-%m-%dT%H:%M:%SZ",
            Some(at("2026-01-01T00:00:00Z")),
        );
        assert_eq!(us, Some(at("2020-05-05T00:00:00Z")));
    }

    #[test]
    fn auto_detect_reads_syslog_against_the_reference() {
        let r = Some(at("2026-11-20T00:00:00Z"));
        let (us, _) = parse_auto_hinted_at("Oct  1 22:14:15", usize::MAX, r).unwrap();
        assert_eq!(us, at("2026-10-01T22:14:15Z"));
        let (us, _) = parse_auto_hinted_at("Oct 11 22:14:15", usize::MAX, r).unwrap();
        assert_eq!(us, at("2026-10-11T22:14:15Z"));
    }

    /// The rule the date already follows: `25:00` names no instant, so it is null rather than tomorrow at one.
    #[test]
    fn a_time_field_past_its_range_is_unparseable() {
        let fmt = "%Y-%m-%dT%H:%M:%S";
        assert_eq!(parse_with_format("2026-01-01T25:00:00", fmt), None);
        assert_eq!(parse_with_format("2026-01-01T00:60:00", fmt), None);
        assert_eq!(parse_with_format("2026-01-01T00:00:99", fmt), None);
        assert!(parse_with_format("2026-01-01T23:59:59", fmt).is_some());
    }

    /// A leap second is a real stamp, not a corrupt one, and lands where a clock that ignores them puts it.
    #[test]
    fn second_sixty_is_the_next_minute() {
        assert_eq!(
            parse_with_format("2016-12-31T23:59:60", "%Y-%m-%dT%H:%M:%S"),
            Some(at("2017-01-01T00:00:00Z"))
        );
    }

    /// `%I` is a 12-hour clock: `13` is a mis-parse, not 1pm.
    #[test]
    fn a_twelve_hour_clock_refuses_an_hour_past_twelve() {
        let fmt = "%Y-%m-%d %I:%M %p";
        assert_eq!(parse_with_format("2026-01-01 13:00 PM", fmt), None);
        assert_eq!(parse_with_format("2026-01-01 00:00 AM", fmt), None);
        assert_eq!(
            parse_with_format("2026-01-01 12:00 AM", fmt),
            Some(at("2026-01-01T00:00:00Z"))
        );
    }

    #[test]
    fn an_offset_past_a_real_one_is_unparseable() {
        let fmt = "%Y-%m-%dT%H:%M:%S%z";
        assert_eq!(parse_with_format("2026-01-01T00:00:00+99:00", fmt), None);
        assert_eq!(parse_with_format("2026-01-01T00:00:00+01:99", fmt), None);
        assert!(parse_with_format("2026-01-01T00:00:00+14:00", fmt).is_some());
    }

    /// Every candidate in `%z`'s table gets its turn, including behind one whose byte window splits a codepoint:
    /// here `utc` spans `UT` and the first byte of `\u{e9}`, and `ut` is what matches.
    #[test]
    fn a_zone_name_is_found_though_a_longer_candidate_straddles_a_codepoint() {
        assert_eq!(
            parse_with_format("2026-01-01 00:00:00 UT\u{e9}", "%Y-%m-%d %H:%M:%S %z\u{e9}"),
            Some(at("2026-01-01T00:00:00Z"))
        );
    }

    /// ISO 8601 spells the midnight ending a day `24:00`, and Postgres and XML Schema both write it. Refusing it
    /// would null a stamp a real system wrote; admitting `24` anywhere else would let a mis-parse through.
    #[test]
    fn twenty_four_hundred_is_the_midnight_that_ends_the_day() {
        let fmt = "%Y-%m-%dT%H:%M:%S";
        assert_eq!(
            parse_with_format("2026-01-01T24:00:00", fmt),
            Some(at("2026-01-02T00:00:00Z"))
        );
        assert_eq!(
            parse_with_format("2026-12-31T24:00:00", fmt),
            Some(at("2027-01-01T00:00:00Z")),
            "and carries the year with it"
        );
        assert_eq!(parse_with_format("2026-01-01T24:00:01", fmt), None);
        assert_eq!(parse_with_format("2026-01-01T24:30:00", fmt), None);
        assert_eq!(
            parse_with_format("2026-01-01T24:00:00.5", "%Y-%m-%dT%H:%M:%S.%f"),
            None,
            "a fraction is past midnight too"
        );
    }

    /// Go's `RFC3339Nano`, `java.time.Instant`, Elasticsearch and Kubernetes all write nine fractional digits.
    /// Reading six and stopping left the rest to fail the next literal, so none of them parsed at all.
    #[test]
    fn a_nanosecond_stamp_is_truncated_not_refused() {
        assert_eq!(
            parse_auto("2026-01-01T00:00:00.123456789Z"),
            Some(at("2026-01-01T00:00:00Z") + 123_456)
        );
        assert_eq!(
            parse_with_format("2026-01-01T00:00:00.999999999", "%Y-%m-%dT%H:%M:%S.%f"),
            Some(at("2026-01-01T00:00:00Z") + 999_999),
            "truncated, never rounded up into the next second"
        );
    }

    /// A fractional run shorter than six digits is a position, not a count: `.1` is a tenth of a second.
    #[test]
    fn a_short_fraction_pads_rather_than_counting_digits() {
        let fmt = "%Y-%m-%dT%H:%M:%S.%f";
        assert_eq!(
            parse_with_format("2026-01-01T00:00:00.1", fmt),
            Some(at("2026-01-01T00:00:00Z") + 100_000)
        );
        assert_eq!(
            parse_with_format("2026-01-01T00:00:00.123", fmt),
            Some(at("2026-01-01T00:00:00Z") + 123_000)
        );
    }

    /// Postgres writes a whole-hour offset as `+05`, with no minutes.
    #[test]
    fn an_offset_may_omit_its_minutes() {
        let fmt = "%Y-%m-%d %H:%M:%S%z";
        assert_eq!(
            parse_with_format("2026-01-01 00:00:00+05", fmt),
            Some(at("2025-12-31T19:00:00Z"))
        );
        assert_eq!(
            parse_with_format("2026-01-01 00:00:00-05", fmt),
            Some(at("2026-01-01T05:00:00Z"))
        );
        assert_eq!(
            parse_with_format("2026-01-01 00:00:00+0530", fmt),
            parse_with_format("2026-01-01 00:00:00+05:30", fmt),
            "the three spellings agree"
        );
    }

    /// strptime makes `%b` and `%B` one specifier. Keeping them apart meant a format had to guess whether the data
    /// abbreviated its month, and was wrong half the time.
    #[test]
    fn a_month_name_reads_under_either_specifier() {
        for fmt in ["%b %d, %Y", "%B %d, %Y"] {
            assert_eq!(
                parse_with_format("Jan 5, 2026", fmt),
                Some(at("2026-01-05T00:00:00Z")),
                "{fmt} against an abbreviation"
            );
            assert_eq!(
                parse_with_format("January 5, 2026", fmt),
                Some(at("2026-01-05T00:00:00Z")),
                "{fmt} against a full name"
            );
        }
        assert_eq!(
            parse_with_format("SEPTEMBER 5, 2026", "%b %d, %Y"),
            Some(at("2026-09-05T00:00:00Z")),
            "and case is still ignored"
        );
    }

    /// A name is still a name: only the twelve, and a longer run keeps its three-letter abbreviation so the rest is
    /// the format's to match.
    #[test]
    fn a_month_name_stops_where_the_abbreviation_does() {
        assert_eq!(parse_with_format("Januar 5, 2026", "%b %d, %Y"), None);
        assert_eq!(parse_with_format("Smarch 5, 2026", "%B %d, %Y"), None);
        assert_eq!(
            parse_with_format("Janx 5, 2026", "%bx %d, %Y"),
            Some(at("2026-01-05T00:00:00Z")),
            "`Jan` is consumed and `x` left to the literal"
        );
    }

    /// [`FORMAT_SPECIFIERS`] is the whole of it: a letter it does not name is not a specifier, and every letter it
    /// does name consumes something. What the published reference is checked against.
    #[test]
    fn format_specifiers_names_exactly_what_the_parser_takes() {
        // One sample per specifier shape; a listed specifier consumes at least one of them.
        let samples = [
            "2026", "08", "8", " 8", "+05:30", "Aug", "August", "Sun", "Sunday", "PM", "123456",
        ];
        // `%s` and `%Q` take a bare count, which every sample above also is; they are checked on their own below.
        for lower in b'a'..=b'z' {
            for letter in [lower, lower.to_ascii_uppercase()] {
                let fmt = format!("%{}", letter as char);
                let listed = FORMAT_SPECIFIERS.contains(&fmt.as_str());
                let parses = samples.iter().any(|s| parse_with_format(s, &fmt).is_some());
                assert_eq!(listed, parses, "{fmt}: listed {listed}, parses {parses}");
            }
        }
        assert!(parse_with_format("%", "%%").is_some(), "%% is a literal percent");
        assert!(parse_with_format("x", "%%").is_none());
    }

    /// `%s` is seconds and `%Q` milliseconds, the unit fixed rather than guessed from magnitude as auto-detect
    /// guesses it. Sign and fraction are taken as `parse_epoch_digits` takes them.
    #[test]
    fn epoch_specifiers_read_a_count_in_a_fixed_unit() {
        let t = 1_700_000_000_000_000i64;
        assert_eq!(parse_with_format("1700000000", "%s"), Some(t));
        assert_eq!(parse_with_format("1700000000000", "%Q"), Some(t));
        // Small counts, which auto-detect would read as seconds either way.
        assert_eq!(parse_with_format("1000", "%Q"), Some(1_000_000));
        assert_eq!(parse_with_format("1000", "%s"), Some(1_000_000_000));
        assert_eq!(parse_with_format("-1", "%s"), Some(-1_000_000));
        assert_eq!(parse_with_format("+1.5", "%s"), Some(1_500_000));
        assert_eq!(parse_with_format("1700000000.123456789", "%s"), Some(t + 123_456));
        assert_eq!(parse_with_format("1700000000000.25", "%Q"), Some(t + 250));
        assert_eq!(parse_with_format("1700000000000.9999", "%Q"), Some(t + 999));
        // Literals around the count still match.
        assert_eq!(parse_with_format("ts=1700000000;", "ts=%s;"), Some(t));
        assert_eq!(parse_with_format("100%", "%Q%%"), Some(100_000));
    }

    #[test]
    fn epoch_specifiers_refuse_what_is_not_a_count() {
        for text in ["", "-", "1.", ".5", "1e9", "2026-01-01", "1 700", " 1"] {
            assert_eq!(parse_with_format(text, "%s"), None, "{text:?}");
            assert_eq!(parse_with_format(text, "%Q"), None, "{text:?}");
        }
        // Past the microsecond range.
        assert_eq!(parse_with_format("9999999999999999999", "%s"), None);
        assert_eq!(parse_with_format("9223372036854775807", "%Q"), None);
        // An epoch names the whole instant; a field beside it is a contradiction.
        assert_eq!(parse_with_format("1700000000 2026", "%s %Y"), None);
        assert_eq!(parse_with_format("1700000000 1700000000", "%s %s"), None);
    }

    /// Auto-detect inherits the range check rather than finding a candidate that swallows the row.
    #[test]
    fn auto_detect_refuses_an_out_of_range_time() {
        assert_eq!(parse_auto("2026-01-01T25:70:99Z"), None);
        assert!(parse_auto("2026-01-01T23:59:59Z").is_some());
    }
}
