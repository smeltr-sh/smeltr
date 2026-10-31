//! A column's default: what a table with the `allowColumnDefaults` feature gives a row that does not write the
//! column. The table keeps it under the column's `CURRENT_DEFAULT` as SQL text; this writer reads it where it is a
//! literal of the column's type, and a table whose default is any other expression is refused.

use std::sync::Arc;

use arrow_array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
    Int64Array, StringArray, TimestampMicrosecondArray, new_null_array,
};
use arrow_schema::{DataType as Arrow, TimeUnit};

use crate::io::table::Kind;
use crate::ops::civil::valid_date_days;

/// The key of a column's metadata that holds its default.
pub const CURRENT_DEFAULT: &str = "CURRENT_DEFAULT";

/// The Arrow type a column of `kind` is written as.
pub fn arrow(kind: &Kind) -> Arrow {
    kind.narrowed().unwrap_or(match kind {
        Kind::Boolean => Arrow::Boolean,
        Kind::Long => Arrow::Int64,
        Kind::Double => Arrow::Float64,
        Kind::Timestamptz => Arrow::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        Kind::String => Arrow::Utf8,
        _ => Arrow::Binary,
    })
}

/// The default `sql` of a column of `kind`, as one row of `arrow(kind)`. None where `sql` is no literal of the type:
/// `NULL`, `true` or `false`, a number, a quoted string, `X'…'` bytes, `DATE '…'`, or `TIMESTAMP '…'` and
/// `TIMESTAMP_NTZ '…'`. A timestamp with a time zone needs one in its literal: the zone a reader of the table would
/// read it in is not the table's.
pub fn value(sql: &str, kind: &Kind) -> Option<ArrayRef> {
    let sql = sql.trim();
    if sql.eq_ignore_ascii_case("null") {
        return Some(new_null_array(&arrow(kind), 1));
    }
    Some(match kind {
        Kind::Boolean => Arc::new(BooleanArray::from(vec![boolean(sql)?])),
        Kind::Byte => Arc::new(Int8Array::from(vec![i8::try_from(integer(sql)?).ok()?])),
        Kind::Short => Arc::new(Int16Array::from(vec![i16::try_from(integer(sql)?).ok()?])),
        Kind::Int => Arc::new(Int32Array::from(vec![i32::try_from(integer(sql)?).ok()?])),
        Kind::Long => Arc::new(Int64Array::from(vec![integer(sql)?])),
        Kind::Float => Arc::new(Float32Array::from(vec![float(sql)? as f32])),
        Kind::Double => Arc::new(Float64Array::from(vec![float(sql)?])),
        Kind::Date => Arc::new(Date32Array::from(vec![date(typed(sql, &["DATE"])?)?])),
        Kind::Timestamp => Arc::new(TimestampMicrosecondArray::from(vec![timestamp(
            typed(sql, &["TIMESTAMP_NTZ", "TIMESTAMP"])?,
            false,
        )?])),
        Kind::Timestamptz => Arc::new(
            TimestampMicrosecondArray::from(vec![timestamp(typed(sql, &["TIMESTAMP"])?, true)?]).with_timezone("UTC"),
        ),
        Kind::String => Arc::new(StringArray::from(vec![string(sql)?])),
        Kind::Binary => Arc::new(BinaryArray::from(vec![hex(sql)?.as_slice()])),
    })
}

fn boolean(sql: &str) -> Option<bool> {
    match sql.to_ascii_lowercase().as_str() {
        "true" => Some(true),
        "false" => Some(false),
        _ => None,
    }
}

/// An integer, with Spark's suffix of a `tinyint`, `smallint` or `bigint` literal or none.
fn integer(sql: &str) -> Option<i64> {
    let digits = sql.strip_suffix(['Y', 'y', 'S', 's', 'L', 'l']).unwrap_or(sql);
    let unsigned = digits.strip_prefix(['-', '+']).unwrap_or(digits);
    if unsigned.is_empty() || !unsigned.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    digits.parse().ok()
}

/// A number, with Spark's suffix of a `float`, `double` or `decimal` literal or none.
fn float(sql: &str) -> Option<f64> {
    let number = sql.strip_suffix("BD").or_else(|| sql.strip_suffix("bd")).unwrap_or(sql);
    let number = number.strip_suffix(['F', 'f', 'D', 'd']).unwrap_or(number);
    let unsigned = number.strip_prefix(['-', '+']).unwrap_or(number);
    let plain = unsigned
        .bytes()
        .all(|b| b.is_ascii_digit() || matches!(b, b'.' | b'e' | b'E' | b'-' | b'+'));
    if !plain || !unsigned.starts_with(|c: char| c.is_ascii_digit() || c == '.') {
        return None;
    }
    number.parse::<f64>().ok().filter(|f| f.is_finite())
}

/// A string literal's text: in single or double quotes, a quote inside doubled. One with a backslash, which Spark reads
/// as an escape, is not read.
fn string(sql: &str) -> Option<String> {
    let quote = sql.chars().next().filter(|q| matches!(q, '\'' | '"'))?;
    let inner = sql.strip_prefix(quote)?.strip_suffix(quote)?;
    if inner.contains('\\') {
        return None;
    }
    let doubled = format!("{quote}{quote}");
    let text = inner.replace(&doubled, "\u{0}");
    if text.contains(quote) {
        return None;
    }
    Some(text.replace('\u{0}', &quote.to_string()))
}

/// The text of a literal `KEYWORD '…'`, one of `keywords`.
fn typed(sql: &str, keywords: &[&str]) -> Option<String> {
    let (keyword, _) = sql.split_once(|c: char| c == '\'' || c == '"' || c.is_whitespace())?;
    if !keywords.iter().any(|k| k.eq_ignore_ascii_case(keyword)) {
        return None;
    }
    string(sql[keyword.len()..].trim_start())
}

/// Days since 1970-01-01 of `yyyy-mm-dd`.
fn date(text: String) -> Option<i32> {
    days(&text)?.try_into().ok()
}

fn days(text: &str) -> Option<i64> {
    let mut parts = text.splitn(3, '-');
    let (y, m, d) = (parts.next()?, parts.next()?, parts.next()?);
    if y.len() != 4 || m.len() != 2 || d.len() != 2 {
        return None;
    }
    valid_date_days(y.parse().ok()?, m.parse().ok()?, d.parse().ok()?)
}

/// Microseconds since 1970-01-01T00:00:00Z of `yyyy-mm-dd[ hh:mm:ss[.ffffff]]`, then `Z` or `±hh:mm` where `zoned`
/// and nothing where not. A time without zone is its wall clock in UTC.
fn timestamp(text: String, zoned: bool) -> Option<i64> {
    let (date, time) = match text.split_once([' ', 'T']) {
        Some((date, time)) => (date, time),
        None if !zoned => (text.as_str(), "00:00:00"),
        None => return None,
    };
    let (time, offset_secs) = match zoned {
        false => (time, 0),
        true => zone(time)?,
    };
    let days = days(date)?;
    let (hms, fraction) = time.split_once('.').unwrap_or((time, ""));
    let mut parts = hms.splitn(3, ':');
    let (h, min, s) = (parts.next()?, parts.next()?, parts.next()?);
    if h.len() != 2
        || min.len() != 2
        || s.len() != 2
        || fraction.len() > 6
        || !fraction.bytes().all(|b| b.is_ascii_digit())
    {
        return None;
    }
    let (h, min, s): (i64, i64, i64) = (h.parse().ok()?, min.parse().ok()?, s.parse().ok()?);
    if h > 23 || min > 59 || s > 59 {
        return None;
    }
    let micros = format!("{fraction:0<6}").parse::<i64>().ok()?;
    let secs = days * 86_400 + h * 3600 + min * 60 + s - offset_secs;
    secs.checked_mul(1_000_000)?.checked_add(micros)
}

/// `time` without its zone, `Z` or `±hh:mm`, and the zone's offset in seconds.
fn zone(time: &str) -> Option<(&str, i64)> {
    if let Some(time) = time.strip_suffix(['Z', 'z']) {
        return Some((time, 0));
    }
    let at = time.rfind(['+', '-'])?;
    let (time, offset) = time.split_at(at);
    let sign = if offset.starts_with('-') { -1 } else { 1 };
    let (h, m) = offset[1..].split_once(':')?;
    if h.len() != 2 || m.len() != 2 {
        return None;
    }
    let (h, m): (i64, i64) = (h.parse().ok()?, m.parse().ok()?);
    (h <= 18 && m <= 59).then_some((time, sign * (h * 3600 + m * 60)))
}

/// The bytes of `X'…'`, in hex.
fn hex(sql: &str) -> Option<Vec<u8>> {
    let text = sql.strip_prefix(['X', 'x']).and_then(string)?;
    if text.len() % 2 != 0 {
        return None;
    }
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(text.get(i..i + 2)?, 16).ok())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Array;
    use arrow_array::cast::AsArray;
    use arrow_array::types::{Date32Type, Int8Type, Int32Type, Int64Type, TimestampMicrosecondType};

    #[test]
    fn a_literal_of_the_columns_type_is_its_value() {
        assert!(value("NULL", &Kind::Long).unwrap().is_null(0));
        assert!(value("TRUE", &Kind::Boolean).unwrap().as_boolean().value(0));
        assert_eq!(
            value("-7", &Kind::Int).unwrap().as_primitive::<Int32Type>().value(0),
            -7
        );
        assert_eq!(value("5Y", &Kind::Byte).unwrap().as_primitive::<Int8Type>().value(0), 5);
        assert_eq!(
            value("42L", &Kind::Long).unwrap().as_primitive::<Int64Type>().value(0),
            42
        );
        assert_eq!(
            value("1.5", &Kind::Double)
                .unwrap()
                .as_primitive::<arrow_array::types::Float64Type>()
                .value(0),
            1.5
        );
        assert_eq!(
            value("2", &Kind::Float)
                .unwrap()
                .as_primitive::<arrow_array::types::Float32Type>()
                .value(0),
            2.0
        );
        assert_eq!(
            value("'it''s'", &Kind::String).unwrap().as_string::<i32>().value(0),
            "it's"
        );
        assert_eq!(value("\"a\"", &Kind::String).unwrap().as_string::<i32>().value(0), "a");
        assert_eq!(
            value("X'00fF'", &Kind::Binary).unwrap().as_binary::<i32>().value(0),
            [0, 255]
        );
        assert_eq!(
            value("DATE'1970-01-01'", &Kind::Date)
                .unwrap()
                .as_primitive::<Date32Type>()
                .value(0),
            0
        );
        assert_eq!(
            value("DATE '1970-01-02'", &Kind::Date)
                .unwrap()
                .as_primitive::<Date32Type>()
                .value(0),
            1
        );
        let ntz = value("TIMESTAMP_NTZ '1970-01-01 00:00:01.5'", &Kind::Timestamp).unwrap();
        assert_eq!(ntz.as_primitive::<TimestampMicrosecondType>().value(0), 1_500_000);
        let zoned = value("TIMESTAMP '1970-01-01 01:00:00+01:00'", &Kind::Timestamptz).unwrap();
        assert_eq!(zoned.as_primitive::<TimestampMicrosecondType>().value(0), 0);
        assert_eq!(zoned.data_type(), &arrow(&Kind::Timestamptz));
    }

    #[test]
    fn an_expression_or_a_literal_of_another_type_is_not_read() {
        for (sql, kind) in [
            ("current_timestamp()", Kind::Timestamptz),
            ("1 + 1", Kind::Long),
            ("300", Kind::Byte),
            ("'7'", Kind::Long),
            ("7", Kind::String),
            ("'a\\nb'", Kind::String),
            ("'a'b'", Kind::String),
            ("DATE '2023-02-29'", Kind::Date),
            ("TIMESTAMP '2023-01-01 00:00:00'", Kind::Timestamptz),
            ("X'abc'", Kind::Binary),
            ("yes", Kind::Boolean),
            ("NaN", Kind::Double),
        ] {
            assert!(value(sql, &kind).is_none(), "{sql} as {kind}");
        }
    }
}
