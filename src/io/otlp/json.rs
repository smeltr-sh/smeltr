//! OTLP/JSON, read into the protobuf messages, so both encodings render through one path. The body is read once, by
//! `json_scan::Reader`, each message filled as its fields arrive.
//!
//! A field is read by its camelCase name or its protobuf name. A field this module does not know is skipped, as OTLP
//! asks of a receiver, and a `null` one is absent, as protobuf's JSON reads it. Of a field given twice, under either
//! name, the last stands; of a oneof's kinds, the first this module tries stands, whatever their order.

use base64::Engine as _;

use super::{Any, AnyValue, ArrayValue, InstrumentationScope, KeyValue, KeyValueList, Resource};
use crate::format::json_scan::Next;
pub(super) use crate::format::json_scan::Reader;

/// A request: `read` takes the object the body holds, and nothing may follow it.
pub(super) fn request<T>(body: &[u8], read: impl FnOnce(&mut Reader) -> Result<T, String>) -> Result<T, String> {
    let mut r = Reader::new(body).map_err(|_| "not JSON: not UTF-8".to_string())?;
    let request = read(&mut r)?;
    if !r.end() {
        return Err("not JSON: more follows the request".into());
    }
    Ok(request)
}

/// The object `r` is at, each field but a `null` one handed to `field` by its name. A field `field` does not read,
/// which it answers `false`, is skipped. `what` names the object where it is none.
pub(super) fn fields<'d>(
    r: &mut Reader<'d>,
    what: &str,
    mut field: impl FnMut(&str, &mut Reader<'d>) -> Result<bool, String>,
) -> Result<(), String> {
    if r.value()? != Next::Object {
        return Err(format!("{what} is not a JSON object"));
    }
    while let Some(key) = r.key()? {
        if !r.null()? && !field(&key, r)? {
            r.skip()?;
        }
    }
    Ok(())
}

/// The list `r` is at, field `name`, each of its items read by `read`.
pub(super) fn each<'d, T>(
    r: &mut Reader<'d>,
    name: &str,
    mut read: impl FnMut(&mut Reader<'d>) -> Result<T, String>,
) -> Result<Vec<T>, String> {
    if r.value()? != Next::Array {
        return Err(format!("`{name}` is not a list"));
    }
    let mut items = Vec::new();
    while r.item()? {
        items.push(read(r)?);
    }
    Ok(items)
}

pub(super) fn text(r: &mut Reader, name: &str) -> Result<String, String> {
    match r.value()? {
        Next::Str(text) => {
            // An escape grows a string's room as it is read; the message keeps only its text.
            let mut text = text.into_owned();
            text.shrink_to_fit();
            Ok(text)
        }
        _ => Err(format!("`{name}` is not a string")),
    }
}

/// Text decoded by `decode`, read from the document where it holds no escape, so the text and what it decodes to are
/// not held at once. Where it does, the text keeps only its own room while it is decoded.
fn decoded<T>(r: &mut Reader, name: &str, decode: impl FnOnce(&str) -> Option<T>, what: &str) -> Result<T, String> {
    match r.value()? {
        Next::Str(mut text) => {
            if let std::borrow::Cow::Owned(text) = &mut text {
                text.shrink_to_fit();
            }
            decode(&text).ok_or_else(|| format!("`{name}` is not {what}"))
        }
        _ => Err(format!("`{name}` is not a string")),
    }
}

/// An integer, a JSON number or decimal text, as protobuf's JSON takes either.
pub(super) fn int<T: TryFrom<i128>>(r: &mut Reader, name: &str) -> Result<T, String> {
    (integer(r.value()?).and_then(|n| T::try_from(n).ok()))
        .ok_or_else(|| format!("`{name}` is not an integer in range"))
}

fn integer(value: Next) -> Option<i128> {
    match value {
        Next::Number(text) => text.parse().ok(),
        Next::Str(text) => text.parse().ok(),
        _ => None,
    }
}

/// A double, a JSON number or text, as protobuf's JSON writes NaN and the infinities as text.
pub(super) fn double(r: &mut Reader, name: &str) -> Result<f64, String> {
    let read = match r.value()? {
        Next::Number(text) => text.parse().ok(),
        Next::Str(text) => match &*text {
            "NaN" => Some(f64::NAN),
            "Infinity" => Some(f64::INFINITY),
            "-Infinity" => Some(f64::NEG_INFINITY),
            number => number.parse().ok(),
        },
        _ => None,
    };
    read.ok_or_else(|| format!("`{name}` is not a number"))
}

pub(super) fn bool(r: &mut Reader, name: &str) -> Result<bool, String> {
    match r.value()? {
        Next::Bool(b) => Ok(b),
        _ => Err(format!("`{name}` is not true or false")),
    }
}

/// A trace or span ID, as hex text.
pub(super) fn id(r: &mut Reader, name: &str) -> Result<Vec<u8>, String> {
    decoded(r, name, unhex, "hex")
}

/// A list of integers, each a JSON number or decimal text.
pub(super) fn ints<T: TryFrom<i128>>(r: &mut Reader, name: &str) -> Result<Vec<T>, String> {
    let wrong = || format!("`{name}` is not a list of integers in range");
    each(r, name, |r| {
        integer(r.value()?).and_then(|n| T::try_from(n).ok()).ok_or_else(wrong)
    })
}

/// A list of doubles.
pub(super) fn doubles(r: &mut Reader, name: &str) -> Result<Vec<f64>, String> {
    each(r, name, |r| {
        double(r, name).map_err(|_| format!("`{name}` is not a list of numbers"))
    })
}

/// An enum field: its number, as OTLP/JSON writes it, or the name of one of `values`, as protobuf's JSON may.
pub(super) fn enumerated(r: &mut Reader, name: &str, values: &[&str]) -> Result<i32, String> {
    let read = match r.value()? {
        Next::Str(text) => match values.iter().position(|v| *v == text) {
            Some(n) => Some(n as i32),
            None => text.parse().ok(),
        },
        other => integer(other).and_then(|n| i32::try_from(n).ok()),
    };
    read.ok_or_else(|| format!("`{name}` is not one of its enum's numbers or names"))
}

fn unhex(text: &str) -> Option<Vec<u8>> {
    let digits = text.as_bytes();
    if !digits.len().is_multiple_of(2) {
        return None;
    }
    let digit = |d: u8| (d as char).to_digit(16).map(|d| d as u8);
    (digits.chunks(2))
        .map(|pair| Some(digit(pair[0])? << 4 | digit(pair[1])?))
        .collect()
}

/// Base64 in either alphabet, padded or not, as protobuf's JSON takes bytes.
fn unbase64(text: &str) -> Option<Vec<u8>> {
    use base64::engine::{DecodePaddingMode, GeneralPurpose, GeneralPurposeConfig};
    let config = GeneralPurposeConfig::new().with_decode_padding_mode(DecodePaddingMode::Indifferent);
    let standard = GeneralPurpose::new(&base64::alphabet::STANDARD, config);
    let url_safe = GeneralPurpose::new(&base64::alphabet::URL_SAFE, config);
    (standard.decode(text).ok()).or_else(|| url_safe.decode(text).ok())
}

/// Reads one of a oneof's kinds, numbered `kind` by the order this module tries them, into `held`, where the kind
/// with the lowest number given stands. A kind past the one held is skipped unread, so only a kind that may stand
/// must be of its type.
pub(super) fn oneof<'d, T>(
    held: &mut Option<(usize, T)>,
    kind: usize,
    r: &mut Reader<'d>,
    read: impl FnOnce(&mut Reader<'d>) -> Result<T, String>,
) -> Result<(), String> {
    if held.as_ref().is_some_and(|(k, _)| *k < kind) {
        return Ok(r.skip()?);
    }
    *held = Some((kind, read(r)?));
    Ok(())
}

/// A `resource` field.
pub(super) fn resource(r: &mut Reader) -> Result<Resource, String> {
    let mut resource = Resource::default();
    fields(r, "`resource`", |key, r| {
        match key {
            "attributes" => resource.attributes = each(r, key, key_value)?,
            "droppedAttributesCount" | "dropped_attributes_count" => {
                resource.dropped_attributes_count = int(r, "droppedAttributesCount")?
            }
            _ => return Ok(false),
        }
        Ok(true)
    })?;
    Ok(resource)
}

/// A `scope` field.
pub(super) fn scope(r: &mut Reader) -> Result<InstrumentationScope, String> {
    let mut scope = InstrumentationScope::default();
    fields(r, "`scope`", |key, r| {
        match key {
            "name" => scope.name = text(r, key)?,
            "version" => scope.version = text(r, key)?,
            "attributes" => scope.attributes = each(r, key, key_value)?,
            "droppedAttributesCount" | "dropped_attributes_count" => {
                scope.dropped_attributes_count = int(r, "droppedAttributesCount")?
            }
            _ => return Ok(false),
        }
        Ok(true)
    })?;
    Ok(scope)
}

pub(super) fn key_value(r: &mut Reader) -> Result<KeyValue, String> {
    let mut pair = KeyValue::default();
    fields(r, "an attribute", |key, r| {
        match key {
            "key" => pair.key = text(r, key)?,
            "value" => pair.value = Some(any_value(r)?),
            _ => return Ok(false),
        }
        Ok(true)
    })?;
    Ok(pair)
}

/// An `AnyValue`: an object holding one of its kinds, or none.
pub(super) fn any_value(r: &mut Reader) -> Result<AnyValue, String> {
    let mut held = None;
    fields(r, "a value", |key, r| {
        match key {
            "stringValue" | "string_value" => oneof(&mut held, 0, r, |r| Ok(Any::String(text(r, "stringValue")?)))?,
            "boolValue" | "bool_value" => oneof(&mut held, 1, r, |r| Ok(Any::Bool(bool(r, "boolValue")?)))?,
            "intValue" | "int_value" => oneof(&mut held, 2, r, |r| Ok(Any::Int(int(r, "intValue")?)))?,
            "doubleValue" | "double_value" => oneof(&mut held, 3, r, |r| Ok(Any::Double(double(r, "doubleValue")?)))?,
            "arrayValue" | "array_value" => oneof(&mut held, 4, r, |r| {
                let mut array = ArrayValue::default();
                fields(r, "`arrayValue`", |key, r| {
                    if key != "values" {
                        return Ok(false);
                    }
                    array.values = each(r, key, any_value)?;
                    Ok(true)
                })?;
                Ok(Any::Array(array))
            })?,
            "kvlistValue" | "kvlist_value" => oneof(&mut held, 5, r, |r| {
                let mut list = KeyValueList::default();
                fields(r, "`kvlistValue`", |key, r| {
                    if key != "values" {
                        return Ok(false);
                    }
                    list.values = each(r, key, key_value)?;
                    Ok(true)
                })?;
                Ok(Any::Kvlist(list))
            })?,
            "bytesValue" | "bytes_value" => oneof(&mut held, 6, r, |r| {
                Ok(Any::Bytes(decoded(r, "bytesValue", unbase64, "base64")?))
            })?,
            _ => return Ok(false),
        }
        Ok(true)
    })?;
    Ok(AnyValue {
        value: held.map(|(_, value)| value),
    })
}
