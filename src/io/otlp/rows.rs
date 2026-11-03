//! What an OTLP destination reads off a row: the record an OTLP receiver gives, as JSON, each value typed as its JSON
//! says. A number written with a fraction or an exponent is a double and any other an int, as a variant's double and
//! int are written; a string is a string, bytes among them, since a row carries bytes as base64 text.

use crate::format::json_scan::Next;

use super::json::{Reader, each, fields, id};
use super::{Any, AnyValue, ArrayValue, InstrumentationScope, KeyValue, KeyValueList, MAX_DEPTH, Resource};

/// The row's own object, each field but a `null` one handed to `field` by its name. A field `field` does not read is
/// skipped: OTLP has nowhere to send it.
pub(super) fn row<'d>(
    row: &'d [u8],
    field: impl FnMut(&str, &mut Reader<'d>) -> Result<bool, String>,
) -> Result<(), String> {
    let mut r = Reader::new(row.trim_ascii()).map_err(|_| "a row is no JSON object".to_string())?;
    fields(&mut r, "a row", field)?;
    if !r.end() {
        return Err("a row is no JSON object".into());
    }
    Ok(())
}

/// A value of any JSON type.
pub(super) fn any(r: &mut Reader, name: &str) -> Result<AnyValue, String> {
    value(r, name, 0)
}

fn value(r: &mut Reader, name: &str, depth: usize) -> Result<AnyValue, String> {
    if depth > MAX_DEPTH {
        return Err(format!("`{name}` nests deeper than {MAX_DEPTH}"));
    }
    let value = match r.value()? {
        Next::Null => None,
        Next::Bool(b) => Some(Any::Bool(b)),
        Next::Number(text) => Some(number(text)),
        Next::Str(text) => Some(Any::String(text.into_owned())),
        Next::Array => {
            let mut values = Vec::new();
            while r.item()? {
                values.push(value(r, name, depth + 1)?);
            }
            Some(Any::Array(ArrayValue { values }))
        }
        Next::Object => Some(Any::Kvlist(KeyValueList {
            values: members(r, name, depth + 1)?,
        })),
    };
    Ok(AnyValue { value })
}

/// An int where the text is a whole number an i64 holds, else a double.
fn number(text: &str) -> Any {
    let whole = !text.bytes().any(|b| matches!(b, b'.' | b'e' | b'E'));
    match text.parse::<i64>() {
        Ok(i) if whole => Any::Int(i),
        // The reader took it as a JSON number, which an f64 reads, if only as an infinity.
        _ => Any::Double(text.parse().unwrap_or(f64::NAN)),
    }
}

/// The members of the object `r` has just opened, each a key and its value. A null member is none, as a variant holds
/// none.
fn members(r: &mut Reader, name: &str, depth: usize) -> Result<Vec<KeyValue>, String> {
    let mut pairs = Vec::new();
    while let Some(key) = r.key()? {
        let value = value(r, name, depth)?;
        if value.value.is_some() {
            pairs.push(KeyValue {
                key: key.into_owned(),
                value: Some(value),
            });
        }
    }
    Ok(pairs)
}

/// An object of attributes, each key to its value.
pub(super) fn attributes(r: &mut Reader, name: &str) -> Result<Vec<KeyValue>, String> {
    if r.value()? != Next::Object {
        return Err(format!("`{name}` is not an object"));
    }
    members(r, name, 1)
}

/// A row's `resource`: its resource's attributes, as an OTLP receiver's `__source.resource` holds them.
pub(super) fn resource(r: &mut Reader) -> Result<Resource, String> {
    Ok(Resource {
        attributes: attributes(r, "resource")?,
        dropped_attributes_count: 0,
    })
}

/// A row's `scope`: its instrumentation scope's `name`, `version` and `attributes`, as an OTLP receiver's
/// `__source.scope` fields hold them.
pub(super) fn scope(r: &mut Reader) -> Result<InstrumentationScope, String> {
    let mut scope = InstrumentationScope::default();
    fields(r, "`scope`", |key, r| {
        match key {
            "name" => scope.name = text(r, "scope.name")?,
            "version" => scope.version = text(r, "scope.version")?,
            "attributes" => scope.attributes = attributes(r, "scope.attributes")?,
            _ => return Ok(false),
        }
        Ok(true)
    })?;
    Ok(scope)
}

pub(super) fn text(r: &mut Reader, name: &str) -> Result<String, String> {
    super::json::text(r, name)
}

/// A datetime, as nanoseconds since the epoch. A row's datetime is text, to the microsecond.
pub(super) fn time(r: &mut Reader, name: &str) -> Result<u64, String> {
    let micros = match r.value()? {
        Next::Str(text) => crate::ops::timeparse::parse_auto(&text),
        _ => None,
    };
    (micros.and_then(|us| u64::try_from(us).ok()))
        .map(|us| us * 1000)
        .ok_or_else(|| format!("`{name}` is not a datetime after 1970"))
}

/// A trace ID, 16 bytes as hex.
pub(super) fn trace_id(r: &mut Reader, name: &str) -> Result<Vec<u8>, String> {
    sized(id(r, name)?, 16, name)
}

/// A span ID, 8 bytes as hex.
pub(super) fn span_id(r: &mut Reader, name: &str) -> Result<Vec<u8>, String> {
    sized(id(r, name)?, 8, name)
}

fn sized(bytes: Vec<u8>, len: usize, name: &str) -> Result<Vec<u8>, String> {
    if bytes.len() != len {
        return Err(format!("`{name}` is not {} hex digits", 2 * len));
    }
    Ok(bytes)
}

/// A count, which a row holds as a double: a whole number from 0.
pub(super) fn count(r: &mut Reader, name: &str) -> Result<u64, String> {
    whole(super::json::double(r, name)?, name)
}

/// A list of counts.
pub(super) fn counts(r: &mut Reader, name: &str) -> Result<Vec<u64>, String> {
    each(r, name, |r| count(r, name))
}

/// A count past what a u64 holds is the largest it holds: a row's double of a count near `u64::MAX` rounds up past
/// it.
fn whole(n: f64, name: &str) -> Result<u64, String> {
    if !(n >= 0.0 && n.fract() == 0.0) {
        return Err(format!("`{name}` is not a whole count"));
    }
    Ok(n as u64)
}

/// A list of numbers. A null is NaN: a row writes a number no JSON holds as null.
pub(super) fn doubles(r: &mut Reader, name: &str) -> Result<Vec<f64>, String> {
    each(r, name, |r| match r.null()? {
        true => Ok(f64::NAN),
        false => super::json::double(r, name).map_err(|_| format!("`{name}` is not a list of numbers")),
    })
}

/// A list of objects, each read by `read`. A null is an object of no fields, as a row writes an empty one.
pub(super) fn objects<'d, T: Default>(
    r: &mut Reader<'d>,
    name: &str,
    mut read: impl FnMut(&str, &mut Reader<'d>, &mut T) -> Result<bool, String>,
) -> Result<Vec<T>, String> {
    each(r, name, |r| {
        let mut item = T::default();
        if r.null()? {
            return Ok(item);
        }
        fields(r, &format!("an item of `{name}`"), |key, r| read(key, r, &mut item))?;
        Ok(item)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn read(json: &str) -> AnyValue {
        any(&mut Reader::new(json.as_bytes()).unwrap(), "v").unwrap()
    }

    fn of(value: Any) -> AnyValue {
        AnyValue { value: Some(value) }
    }

    /// Each value keeps the type its JSON writes: a variant's double is written with a fraction, its int without.
    #[test]
    fn a_value_is_typed_as_its_json_writes_it() {
        assert_eq!(read("1"), of(Any::Int(1)));
        assert_eq!(read("-9223372036854775808"), of(Any::Int(i64::MIN)));
        assert_eq!(read("1.0"), of(Any::Double(1.0)));
        assert_eq!(read("1e3"), of(Any::Double(1000.0)));
        assert_eq!(read("18446744073709551616"), of(Any::Double(18446744073709551616.0)));
        assert_eq!(read("true"), of(Any::Bool(true)));
        assert_eq!(read(r#""a""#), of(Any::String("a".into())));
        assert_eq!(read("null"), AnyValue { value: None });
        assert_eq!(
            read(r#"[1,"a"]"#),
            of(Any::Array(ArrayValue {
                values: vec![of(Any::Int(1)), of(Any::String("a".into()))]
            }))
        );
        assert_eq!(
            read(r#"{"a":1,"b":null}"#),
            of(Any::Kvlist(KeyValueList {
                values: vec![KeyValue {
                    key: "a".into(),
                    value: Some(of(Any::Int(1)))
                }]
            })),
            "a null member is none"
        );
    }

    #[test]
    fn a_value_too_deep_is_refused() {
        let deep = format!("{}{}", "[".repeat(MAX_DEPTH + 2), "]".repeat(MAX_DEPTH + 2));
        assert!(any(&mut Reader::new(deep.as_bytes()).unwrap(), "v").is_err());
    }

    #[test]
    fn a_count_is_whole_and_from_zero() {
        let count = |json: &str| super::count(&mut Reader::new(json.as_bytes()).unwrap(), "n");
        assert_eq!(count("3.0"), Ok(3));
        assert_eq!(count("0"), Ok(0));
        assert!(count("-1").is_err());
        assert!(count("1.5").is_err());
        assert_eq!(
            count("18446744073709551615"),
            Ok(u64::MAX),
            "a u64's largest, as a double rounds it"
        );
    }

    #[test]
    fn a_time_is_nanoseconds_to_the_microsecond() {
        let time = |json: &str| super::time(&mut Reader::new(json.as_bytes()).unwrap(), "t");
        assert_eq!(time(r#""2023-11-14T22:13:20.123456Z""#), Ok(1_700_000_000_123_456_000));
        assert!(time("1").is_err());
        assert!(time(r#""1969-12-31T23:59:59Z""#).is_err());
    }

    #[test]
    fn an_id_is_its_length_in_hex() {
        let trace = |json: &str| trace_id(&mut Reader::new(json.as_bytes()).unwrap(), "traceId");
        assert_eq!(trace(&format!(r#""{}""#, "01".repeat(16))), Ok(vec![1; 16]));
        assert!(trace(&format!(r#""{}""#, "01".repeat(8))).is_err());
        assert!(trace(r#""zz""#).is_err());
    }
}
