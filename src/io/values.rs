//! Values read from outside smeltr, and the columns built from them. A column is built as its declared type all the
//! way down: an array from its elements' column, an object from a column per member, each member its own type.

use serde::{Deserialize, Serialize};

use crate::core::column::DataType;
use crate::core::variant;
use crate::format::json_scan::Reader;

/// One value read from outside smeltr, in smeltr's terms. Times and durations are microseconds, times since the Unix epoch.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    #[serde(with = "float")]
    Float(f64),
    Str(String),
    #[serde(with = "base64")]
    Bytes(Vec<u8>),
    Time(i64),
    Duration(i64),
    /// A list of values, each its own type: an array column's.
    Array(Vec<Value>),
    /// A value with a shape of its own: JSON, in `core::variant`'s encoding.
    #[serde(with = "json")]
    Json(Vec<u8>),
}

impl Value {
    /// About the bytes the value takes in a record: its text's for text, eight for anything fixed.
    pub fn size(&self) -> usize {
        match self {
            Value::Str(s) => s.len(),
            Value::Bytes(b) => b.len(),
            Value::Array(items) => items.iter().map(Value::size).sum(),
            Value::Json(json) => json.len(),
            _ => 8,
        }
    }

    /// About the bytes the value takes in memory: the value itself, and what it owns.
    pub fn held(&self) -> usize {
        std::mem::size_of::<Value>()
            + match self {
                Value::Str(s) => s.len(),
                Value::Bytes(b) => b.len(),
                Value::Array(items) => items.iter().map(Value::held).sum(),
                Value::Json(json) => json.len(),
                _ => 0,
            }
    }
}

/// Bytes as JSON keeps them: base64, their one text spelling.
mod base64 {
    use crate::format::bytes_text::BytesForm;
    use serde::{Deserialize, Deserializer, Serializer, de::Error};

    pub fn serialize<S: Serializer>(bytes: &[u8], out: S) -> Result<S::Ok, S::Error> {
        out.serialize_str(&BytesForm::Base64.text(bytes))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(input: D) -> Result<Vec<u8>, D::Error> {
        let text = String::deserialize(input)?;
        let mut bytes = Vec::new();
        match BytesForm::Base64.read(text.as_bytes(), &mut bytes) {
            true => Ok(bytes),
            false => Err(D::Error::custom("bytes that are not base64")),
        }
    }
}

/// A float as JSON keeps it: a number when finite, else `"NaN"`, `"Infinity"` or `"-Infinity"`. JSON has no
/// number for those, and serde_json would write each as null, so a stored position holding one could not be read
/// back, and comparing values could not tell them from null or from each other.
mod float {
    use serde::{Deserialize, Deserializer, Serializer, de::Error};

    pub fn serialize<S: Serializer>(f: &f64, out: S) -> Result<S::Ok, S::Error> {
        match *f {
            f if f.is_finite() => out.serialize_f64(f),
            f if f.is_nan() => out.serialize_str("NaN"),
            f if f > 0.0 => out.serialize_str("Infinity"),
            _ => out.serialize_str("-Infinity"),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(input: D) -> Result<f64, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Spelled {
            Number(f64),
            Word(String),
        }
        match Spelled::deserialize(input)? {
            Spelled::Number(f) => Ok(f),
            Spelled::Word(w) if w == "NaN" => Ok(f64::NAN),
            Spelled::Word(w) if w == "Infinity" => Ok(f64::INFINITY),
            Spelled::Word(w) if w == "-Infinity" => Ok(f64::NEG_INFINITY),
            Spelled::Word(other) => Err(D::Error::custom(format!("`{other}` is not a float"))),
        }
    }
}

/// A JSON value as stored, from `core::variant`'s encoding: an object's members in key order, so the same value is
/// always the same text. Read back as `values::json` reads one: a `null` member is no entry, and of a key given twice
/// the last value stands.
mod json {
    use std::fmt;

    use serde::de::{self, DeserializeSeed, MapAccess, SeqAccess, Visitor};
    use serde::ser::{SerializeMap, SerializeSeq};
    use serde::{Deserializer, Serialize, Serializer};

    use crate::core::variant;

    pub fn serialize<S: Serializer>(json: &[u8], out: S) -> Result<S::Ok, S::Error> {
        Shown(json).serialize(out)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(input: D) -> Result<Vec<u8>, D::Error> {
        let mut out = Vec::new();
        input.deserialize_any(Written(&mut out))?;
        Ok(out)
    }

    struct Shown<'v>(&'v [u8]);

    impl Serialize for Shown<'_> {
        fn serialize<S: Serializer>(&self, out: S) -> Result<S::Ok, S::Error> {
            match variant::read(self.0) {
                variant::Value::Null => out.serialize_unit(),
                variant::Value::Bool(b) => out.serialize_bool(b),
                variant::Value::I64(i) | variant::Value::DateTime(i) | variant::Value::Duration(i) => {
                    out.serialize_i64(i)
                }
                variant::Value::F64(f) => out.serialize_f64(f),
                variant::Value::Str(s) => out.serialize_str(s),
                variant::Value::Bytes(b) => super::base64::serialize(b, out),
                variant::Value::Array(items) => {
                    let mut seq = out.serialize_seq(Some(items.len()))?;
                    for item in items {
                        seq.serialize_element(&Shown(item))?;
                    }
                    seq.end()
                }
                variant::Value::Object(entries) => {
                    let mut entries: Vec<_> = entries.collect();
                    entries.sort_unstable_by_key(|(key, _)| *key);
                    let mut map = out.serialize_map(Some(entries.len()))?;
                    for (key, member) in entries {
                        map.serialize_entry(key, &Shown(member))?;
                    }
                    map.end()
                }
            }
        }
    }

    /// Writes the value it reads to its buffer.
    struct Written<'o>(&'o mut Vec<u8>);

    impl<'de> DeserializeSeed<'de> for Written<'_> {
        type Value = ();

        fn deserialize<D: Deserializer<'de>>(self, input: D) -> Result<(), D::Error> {
            input.deserialize_any(self)
        }
    }

    impl<'de> Visitor<'de> for Written<'_> {
        type Value = ();

        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("a JSON value")
        }

        fn visit_unit<E: de::Error>(self) -> Result<(), E> {
            variant::put_null(self.0);
            Ok(())
        }

        fn visit_none<E: de::Error>(self) -> Result<(), E> {
            self.visit_unit()
        }

        fn visit_some<D: Deserializer<'de>>(self, input: D) -> Result<(), D::Error> {
            input.deserialize_any(self)
        }

        fn visit_bool<E: de::Error>(self, b: bool) -> Result<(), E> {
            variant::put_bool(self.0, b);
            Ok(())
        }

        fn visit_i64<E: de::Error>(self, i: i64) -> Result<(), E> {
            variant::put_i64(self.0, i);
            Ok(())
        }

        // A whole number past i64 is a float, as `values::json` reads one.
        fn visit_u64<E: de::Error>(self, u: u64) -> Result<(), E> {
            match i64::try_from(u) {
                Ok(i) => variant::put_i64(self.0, i),
                Err(_) => variant::put_f64(self.0, u as f64),
            }
            Ok(())
        }

        fn visit_f64<E: de::Error>(self, f: f64) -> Result<(), E> {
            variant::put_f64(self.0, f);
            Ok(())
        }

        fn visit_str<E: de::Error>(self, s: &str) -> Result<(), E> {
            variant::put_str(self.0, s);
            Ok(())
        }

        fn visit_seq<A: SeqAccess<'de>>(self, mut items: A) -> Result<(), A::Error> {
            let open = variant::begin_array(self.0);
            let mut count = 0;
            while items.next_element_seed(Written(&mut *self.0))?.is_some() {
                count += 1;
            }
            variant::end(self.0, open, count);
            Ok(())
        }

        fn visit_map<A: MapAccess<'de>>(self, mut entries: A) -> Result<(), A::Error> {
            let open = variant::begin_object(self.0);
            while let Some(key) = entries.next_key::<String>()? {
                let at = self.0.len();
                variant::put_key(self.0, &key);
                let value = self.0.len();
                entries.next_value_seed(Written(&mut *self.0))?;
                if self.0[value] == variant::NULL {
                    self.0.truncate(at);
                }
            }
            let count = variant::dedupe(self.0, open);
            variant::end(self.0, open, count);
            Ok(())
        }
    }
}

/// `values` as a column of `ty`, and the rows whose value could not be `ty`, which are null. An array is built
/// from its elements and an object from its members, each as its declared type, so `array<int>` is a list of
/// integers, never a variant. An element that cannot be its type refuses its row's whole array, as the language's
/// coercion does; a member that cannot be its type is null on its own, and refuses its row. Err names a type, at
/// any depth, that no column is built as here; nothing is built then.
pub(crate) fn column(
    pool: &mut crate::core::buffer::BufferPool,
    ty: &DataType,
    values: &[Value],
) -> Result<(crate::core::column::Column, Vec<usize>), String> {
    match unbuilt(ty) {
        Some(t) => Err(format!("a {} value is not built from what a source reads", t.spell())),
        None => Ok(build(pool, ty, values)),
    }
}

/// The first type in `ty`, itself included, that [`column`] does not build.
pub(crate) fn unbuilt(ty: &DataType) -> Option<&DataType> {
    match ty {
        DataType::Str
        | DataType::Bytes
        | DataType::Variant
        | DataType::Bool
        | DataType::I64
        | DataType::F64
        | DataType::DateTime
        | DataType::Duration => None,
        DataType::Array(element) => unbuilt(element),
        DataType::Struct(members, _) => members.iter().find_map(|(_, t)| unbuilt(t)),
        other => Some(other),
    }
}

fn build(
    pool: &mut crate::core::buffer::BufferPool,
    ty: &DataType,
    values: &[Value],
) -> (crate::core::column::Column, Vec<usize>) {
    use crate::core::column::{Column, StrBuilder, VarType};
    use crate::format::bytes_text::BytesForm;
    let n = values.len();
    let mut refused = Vec::new();
    let column = match ty {
        DataType::Str | DataType::Bytes | DataType::Variant => {
            let var = match ty {
                DataType::Str => VarType::Str,
                DataType::Bytes => VarType::Bytes,
                _ => VarType::Variant,
            };
            let mut b = StrBuilder::new(pool, var, n);
            let mut bytes = Vec::new();
            for (row, v) in values.iter().enumerate() {
                match (ty, v) {
                    (_, Value::Null) => b.push_null(),
                    (DataType::Bytes, Value::Bytes(x)) => b.push_grow(pool, x),
                    // JSON has no bytes type, so bytes inside it are text in their one spelling, base64.
                    (DataType::Bytes, Value::Str(t)) => {
                        bytes.clear();
                        match BytesForm::Base64.read(t.as_bytes(), &mut bytes) {
                            true => b.push_grow(pool, &bytes),
                            false => {
                                refused.push(row);
                                b.push_null();
                            }
                        }
                    }
                    (DataType::Bytes, _) => {
                        refused.push(row);
                        b.push_null();
                    }
                    (DataType::Str, v) => match text(v) {
                        Some(t) => b.push_grow(pool, t.as_bytes()),
                        None => {
                            refused.push(row);
                            b.push_null();
                        }
                    },
                    (_, v) => {
                        bytes.clear();
                        put_variant(&mut bytes, v);
                        b.push_grow(pool, &bytes);
                    }
                }
            }
            b.finish()
        }
        DataType::Bool => {
            let mut c = Column::alloc(pool, ty, n);
            let b = c.bool_mut().expect("a bool column");
            for (row, v) in values.iter().enumerate() {
                match v {
                    Value::Bool(x) => {
                        crate::core::bits::set_row(b.values, row, *x);
                    }
                    Value::Null => b.validity.set_valid(row, false),
                    _ => {
                        refused.push(row);
                        b.validity.set_valid(row, false);
                    }
                }
            }
            c
        }
        DataType::I64 | DataType::F64 | DataType::DateTime | DataType::Duration => {
            let mut c = Column::alloc(pool, ty, n);
            for (row, v) in values.iter().enumerate() {
                let ok = match (ty, v) {
                    (_, Value::Null) => false,
                    (DataType::I64, Value::Int(x)) => {
                        c.i64_mut().expect("an i64 column").values[row] = *x;
                        true
                    }
                    (DataType::F64, Value::Float(x)) => {
                        c.f64_mut().expect("an f64 column").values[row] = *x;
                        true
                    }
                    (DataType::F64, Value::Int(x)) => {
                        c.f64_mut().expect("an f64 column").values[row] = *x as f64;
                        true
                    }
                    (DataType::DateTime, Value::Time(x)) => {
                        c.datetime_mut().expect("a datetime column").values[row] = *x;
                        true
                    }
                    // JSON has no time type, so a time inside it is text.
                    (DataType::DateTime, Value::Str(t)) => match crate::ops::timeparse::parse_auto(t) {
                        Some(x) => {
                            c.datetime_mut().expect("a datetime column").values[row] = x;
                            true
                        }
                        None => {
                            refused.push(row);
                            false
                        }
                    },
                    // JSON has no duration type, so a duration inside it is its microseconds, as smeltr writes one.
                    (DataType::Duration, Value::Duration(x) | Value::Int(x)) => {
                        c.duration_mut().expect("a duration column").values[row] = *x;
                        true
                    }
                    _ => {
                        refused.push(row);
                        false
                    }
                };
                if !ok {
                    c.validity_mut()
                        .expect("a fixed column has validity")
                        .set_valid(row, false);
                }
            }
            c
        }
        DataType::Array(element) => {
            let mut offsets = pool.take((n + 1) * 4);
            let mut validity = crate::core::column::Validity::alloc(pool, n);
            let mut items = Vec::new();
            let mut owner = Vec::new();
            {
                let offsets = offsets.as_mut_slice::<u32>();
                offsets[0] = 0;
                for (row, v) in values.iter().enumerate() {
                    match as_items(v) {
                        Some(list) => {
                            owner.extend(std::iter::repeat_n(row, list.len()));
                            items.extend(list);
                        }
                        None => {
                            if !matches!(v, Value::Null) {
                                refused.push(row);
                            }
                            validity.set_valid(row, false);
                        }
                    }
                    offsets[row + 1] = items.len() as u32;
                }
            }
            let (child, bad) = build(pool, element, &items);
            for item in bad {
                let row = owner[item];
                if validity.is_valid(row) {
                    validity.set_valid(row, false);
                    refused.push(row);
                }
            }
            Column::list_from_parts(offsets, child, validity, n)
        }
        DataType::Struct(members, open) => {
            let objects: Vec<Option<&[u8]>> = values
                .iter()
                .enumerate()
                .map(|(row, v)| match v {
                    Value::Json(o) if variant::is_object(o) => Some(o.as_slice()),
                    Value::Null => None,
                    _ => {
                        refused.push(row);
                        None
                    }
                })
                .collect();
            let mut columns = Vec::with_capacity(members.len());
            for (name, member) in members {
                let field: Vec<Value> = objects
                    .iter()
                    .map(|o| {
                        o.and_then(|o| variant::member(o, name))
                            .map_or(Value::Null, from_variant)
                    })
                    .collect();
                let (c, bad) = build(pool, member, &field);
                refused.extend(bad);
                columns.push((name.clone(), c));
            }
            // An open object keeps the members it does not declare, as the language's does.
            let unclaimed = open.then(|| {
                let rest: Vec<Value> = objects
                    .iter()
                    .map(|o| match o {
                        Some(o) => {
                            let variant::Value::Object(entries) = variant::read(o) else {
                                unreachable!("only an object is kept")
                            };
                            let mut rest = Vec::new();
                            let open = variant::begin_object(&mut rest);
                            let mut count = 0;
                            for (key, member) in entries.filter(|(k, _)| !members.iter().any(|(m, _)| m == k)) {
                                variant::put_key(&mut rest, key);
                                rest.extend_from_slice(member);
                                count += 1;
                            }
                            variant::end(&mut rest, open, count);
                            match count {
                                0 => Value::Null,
                                _ => Value::Json(rest),
                            }
                        }
                        None => Value::Null,
                    })
                    .collect();
                build(pool, &DataType::Variant, &rest).0
            });
            refused.sort_unstable();
            refused.dedup();
            Column::struct_with_unclaimed_fields(columns, unclaimed, n)
        }
        other => unreachable!("`column` refuses a {} before building", other.spell()),
    };
    (column, refused)
}

/// The elements of an array value: a database array's, or a JSON array's.
fn as_items(v: &Value) -> Option<Vec<Value>> {
    match v {
        Value::Array(items) => Some(items.clone()),
        Value::Json(json) => match variant::read(json) {
            variant::Value::Array(items) => Some(items.map(from_variant).collect()),
            _ => None,
        },
        _ => None,
    }
}

/// `json`, a JSON value's text, as a value: refused where it is no JSON.
pub(crate) fn json(json: &[u8]) -> Result<Value, String> {
    let mut r = Reader::new(json).map_err(|_| "a JSON value that is not UTF-8".to_string())?;
    let mut out = Vec::new();
    r.variant(&mut out)
        .map_err(|_| "a JSON value that does not read".to_string())?;
    match r.end() {
        true => Ok(Value::Json(out)),
        false => Err("a JSON value with more after it".to_string()),
    }
}

/// A JSON member or element, in `core::variant`'s encoding, as a value: a whole number is an integer, any other
/// number a float.
pub(crate) fn from_variant(v: &[u8]) -> Value {
    match variant::read(v) {
        variant::Value::Null => Value::Null,
        variant::Value::Bool(b) => Value::Bool(b),
        variant::Value::I64(i) => Value::Int(i),
        variant::Value::F64(f) => Value::Float(f),
        variant::Value::Str(s) => Value::Str(s.to_string()),
        variant::Value::Array(items) => Value::Array(items.map(from_variant).collect()),
        variant::Value::DateTime(t) => Value::Time(t),
        variant::Value::Duration(d) => Value::Duration(d),
        variant::Value::Bytes(b) => Value::Bytes(b.to_vec()),
        variant::Value::Object(_) => Value::Json(v.to_vec()),
    }
}

/// A JSON value, in `core::variant`'s encoding, as JSON text: every member and element as it is held, in its order.
/// A float JSON has no number for is null.
pub(crate) fn json_text(json: &[u8], out: &mut Vec<u8>) {
    use crate::format::json::write_json_string;
    match variant::read(json) {
        variant::Value::Null => out.extend_from_slice(b"null"),
        variant::Value::Bool(b) => out.extend_from_slice(if b { b"true" } else { b"false" }),
        variant::Value::I64(i) | variant::Value::DateTime(i) | variant::Value::Duration(i) => {
            out.extend_from_slice(itoa::Buffer::new().format(i).as_bytes())
        }
        variant::Value::F64(f) if f.is_finite() => {
            use std::io::Write as _;
            write!(out, "{f:?}").expect("a Vec takes every write")
        }
        variant::Value::F64(_) => out.extend_from_slice(b"null"),
        variant::Value::Str(s) => write_json_string(s.as_bytes(), out),
        variant::Value::Bytes(b) => {
            out.push(b'"');
            crate::format::bytes_text::BytesForm::Base64.write(b, out);
            out.push(b'"');
        }
        variant::Value::Array(items) => {
            out.push(b'[');
            for (i, item) in items.enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                json_text(item, out);
            }
            out.push(b']');
        }
        variant::Value::Object(entries) => {
            out.push(b'{');
            for (i, (key, member)) in entries.enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                write_json_string(key.as_bytes(), out);
                out.push(b':');
                json_text(member, out);
            }
            out.push(b'}');
        }
    }
}

/// `v` as it is stored: in a position, a key and a row's hash. An object for each kind but null, `{"<kind>": <value>}`,
/// and `"null"` for null. A float is spelled as serde_json spells one, a JSON value as `stored_json` writes it.
pub(crate) fn stored(v: &Value, out: &mut Vec<u8>) {
    use crate::format::json::write_json_string;
    let kind = match v {
        Value::Null => return out.extend_from_slice(br#""null""#),
        Value::Bool(_) => "bool",
        Value::Int(_) => "int",
        Value::Float(_) => "float",
        Value::Str(_) => "str",
        Value::Bytes(_) => "bytes",
        Value::Time(_) => "time",
        Value::Duration(_) => "duration",
        Value::Array(_) => "array",
        Value::Json(_) => "json",
    };
    out.extend_from_slice(b"{\"");
    out.extend_from_slice(kind.as_bytes());
    out.extend_from_slice(b"\":");
    match v {
        Value::Null => unreachable!("written above"),
        Value::Bool(b) => out.extend_from_slice(if *b { b"true" } else { b"false" }),
        Value::Int(i) | Value::Time(i) | Value::Duration(i) => {
            out.extend_from_slice(itoa::Buffer::new().format(*i).as_bytes())
        }
        Value::Float(f) if f.is_finite() => out.extend_from_slice(zmij::Buffer::new().format_finite(*f).as_bytes()),
        Value::Float(f) if f.is_nan() => out.extend_from_slice(br#""NaN""#),
        Value::Float(f) if *f > 0.0 => out.extend_from_slice(br#""Infinity""#),
        Value::Float(_) => out.extend_from_slice(br#""-Infinity""#),
        Value::Str(s) => write_json_string(s.as_bytes(), out),
        Value::Bytes(b) => {
            out.push(b'"');
            crate::format::bytes_text::BytesForm::Base64.write(b, out);
            out.push(b'"');
        }
        Value::Array(items) => {
            out.push(b'[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                stored(item, out);
            }
            out.push(b']');
        }
        Value::Json(json) => stored_json(json, out),
    }
    out.push(b'}');
}

/// A JSON value, in `core::variant`'s encoding, as it is stored: an object's members in key order, so the same value
/// is always the same text, and a float spelled as serde_json spells one, or null where JSON has no number for it.
fn stored_json(json: &[u8], out: &mut Vec<u8>) {
    use crate::format::json::write_json_string;
    match variant::read(json) {
        variant::Value::F64(f) if f.is_finite() => {
            out.extend_from_slice(zmij::Buffer::new().format_finite(f).as_bytes())
        }
        variant::Value::Array(items) => {
            out.push(b'[');
            for (i, item) in items.enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                stored_json(item, out);
            }
            out.push(b']');
        }
        variant::Value::Object(entries) => {
            let mut entries: Vec<_> = entries.collect();
            entries.sort_unstable_by_key(|(key, _)| *key);
            out.push(b'{');
            for (i, (key, member)) in entries.into_iter().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                write_json_string(key.as_bytes(), out);
                out.push(b':');
                stored_json(member, out);
            }
            out.push(b'}');
        }
        // Every other kind is written as `json_text` writes it.
        _ => json_text(json, out),
    }
}

/// `v` as text, for a `str` field: text, or a number or truth spelled out.
fn text(v: &Value) -> Option<String> {
    match v {
        Value::Str(s) => Some(s.clone()),
        Value::Int(x) => Some(x.to_string()),
        Value::Float(x) => Some(x.to_string()),
        Value::Bool(x) => Some(x.to_string()),
        Value::Json(json) => match variant::read(json) {
            variant::Value::Str(s) => Some(s.to_string()),
            variant::Value::I64(x) => Some(x.to_string()),
            variant::Value::F64(x) => Some(x.to_string()),
            _ => None,
        },
        _ => None,
    }
}

/// `v` as the source spelled it, for a failed value's note.
pub(crate) fn spell(v: &Value) -> String {
    match v {
        Value::Str(s) => s.clone(),
        Value::Json(j) => {
            let mut text = Vec::new();
            json_text(j, &mut text);
            String::from_utf8(text).expect("JSON is UTF-8")
        }
        other => {
            let mut text = Vec::new();
            stored(other, &mut text);
            String::from_utf8(text).expect("a stored value is UTF-8")
        }
    }
}

/// `v` in `core::variant`'s encoding.
fn put_variant(out: &mut Vec<u8>, v: &Value) {
    match v {
        Value::Time(x) => variant::put_datetime(out, *x),
        Value::Duration(x) => variant::put_duration(out, *x),
        Value::Null => variant::put_null(out),
        Value::Bool(x) => variant::put_bool(out, *x),
        Value::Int(x) => variant::put_i64(out, *x),
        Value::Float(x) => variant::put_f64(out, *x),
        Value::Str(s) => variant::put_str(out, s),
        Value::Bytes(b) => variant::put_bytes(out, b),
        Value::Json(j) => out.extend_from_slice(j),
        Value::Array(items) => {
            let open = variant::begin_array(out);
            for item in items {
                put_variant(out, item);
            }
            variant::end(out, open, items.len());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every kind of value, stored by hand, is the text serde_json wrote for it: what a position, key and hash stored
    /// before still reads the same.
    #[test]
    fn a_value_is_stored_as_serde_json_stored_it() {
        let mut x: u64 = 0x2545_f491_4f6c_dd1d;
        let mut next = move || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        let texts = ["", "a\"b\\c", "\u{1}\u{1f}\u{7f}\n\t", "é∑😀", "</script>"];
        let documents = [
            r#"{"b":1.5,"a":[1,2.25e-7,{"z":null,"y":"x"}],"c":1e300}"#,
            r#"[0.1,-0.0,123456789012345680000,true]"#,
            r#""text""#,
        ];
        fn any(next: &mut dyn FnMut() -> u64, depth: u32, texts: &[&str], documents: &[&str]) -> Value {
            match next() % if depth > 2 { 9 } else { 10 } {
                0 => Value::Null,
                1 => Value::Bool(next().is_multiple_of(2)),
                2 => Value::Int(next() as i64),
                3 => Value::Float(f64::from_bits(next())),
                4 => Value::Float((next() % 2_000_000) as f64 / 64.0 - 1.0e4),
                5 => Value::Str(texts[(next() % texts.len() as u64) as usize].to_string()),
                6 => Value::Bytes((0..next() % 6).map(|_| next() as u8).collect()),
                7 => Value::Time(next() as i64 >> 8),
                8 => json(documents[(next() % documents.len() as u64) as usize].as_bytes()).unwrap(),
                _ => Value::Array(
                    (0..next() % 4)
                        .map(|_| any(next, depth + 1, texts, documents))
                        .collect(),
                ),
            }
        }
        for _ in 0..20_000 {
            let value = any(&mut next, 0, &texts, &documents);
            let mut ours = Vec::new();
            stored(&value, &mut ours);
            assert_eq!(
                String::from_utf8(ours).unwrap(),
                serde_json::to_string(&value).unwrap(),
                "{value:?}"
            );
        }
        for value in [
            Value::Float(f64::NAN),
            Value::Float(f64::INFINITY),
            Value::Float(f64::NEG_INFINITY),
            Value::Duration(-5),
        ] {
            let mut ours = Vec::new();
            stored(&value, &mut ours);
            assert_eq!(String::from_utf8(ours).unwrap(), serde_json::to_string(&value).unwrap());
        }
    }

    #[test]
    fn bytes_build_a_bytes_column_and_keep_through_json_as_base64() {
        let mut pool = crate::core::buffer::BufferPool::new();
        let values = [
            Value::Bytes(vec![0xde, 0xad]),
            Value::Null,
            Value::Str("3q0=".into()),
            Value::Str("not base64!".into()),
        ];
        let (c, refused) = column(&mut pool, &DataType::Bytes, &values).unwrap();
        assert_eq!(c.data_type(), DataType::Bytes);
        assert_eq!(c.var().unwrap().get(0), [0xde, 0xad]);
        assert!(!c.is_valid(1));
        assert_eq!(c.var().unwrap().get(2), [0xde, 0xad], "text in JSON is bytes in base64");
        assert_eq!(refused, [3]);
        c.reclaim(&mut pool);

        let text = serde_json::to_string(&values[0]).unwrap();
        assert_eq!(text, r#"{"bytes":"3q0="}"#);
        assert_eq!(serde_json::from_str::<Value>(&text).unwrap(), values[0]);
    }

    /// `text` in `core::variant`'s encoding.
    fn encoded(text: &[u8]) -> Vec<u8> {
        match json(text).unwrap() {
            Value::Json(v) => v,
            other => panic!("not JSON: {other:?}"),
        }
    }

    #[test]
    fn a_duration_from_json_is_its_microseconds() {
        let mut pool = crate::core::buffer::BufferPool::new();
        let (c, refused) = column(&mut pool, &DataType::Duration, &[from_variant(&encoded(b"1500000"))]).unwrap();
        assert_eq!(c.duration().unwrap().values[0], 1_500_000);
        assert!(refused.is_empty());
        c.reclaim(&mut pool);
    }

    #[test]
    fn json_keeps_its_members_in_key_order_and_reads_back_as_values_json_reads_it() {
        let read =
            json(br#"{"b": [1, 2.5, "x", null], "a": {"z": true, "y": null}, "c": 18446744073709551615}"#).unwrap();
        let text = serde_json::to_string(&read).unwrap();
        assert_eq!(
            text,
            r#"{"json":{"a":{"z":true},"b":[1,2.5,"x",null],"c":1.8446744073709552e+19}}"#
        );
        let back = serde_json::from_str::<Value>(&text).unwrap();
        assert_eq!(
            serde_json::to_string(&back).unwrap(),
            text,
            "read back, it is the same value"
        );
        assert_eq!(
            serde_json::from_str::<Value>(r#"{"json":{"a":1,"n":null,"a":2}}"#).unwrap(),
            json(br#"{"a":2}"#).unwrap(),
            "a null member is none, and a repeated key's last value stands"
        );
        assert_eq!(
            spell(&read),
            r#"{"b":[1,2.5,"x",null],"a":{"z":true},"c":1.8446744073709552e19}"#
        );
    }

    #[test]
    fn json_refuses_what_is_not_one_json_value() {
        assert!(json(b"{\"a\":").is_err());
        assert!(json(b"1 2").is_err());
        assert!(json(br#"{"a": 1e400}"#).is_err(), "a number no f64 holds is not lost");
        assert!(json(br#"["\ud800"]"#).is_err(), "nor is text that names no character");
        assert_eq!(
            spell(&json(br#"{"m":{},"n":[{}]}"#).unwrap()),
            r#"{"m":{},"n":[{}]}"#,
            "an empty object is kept"
        );
        assert!(json(b"\xff").is_err());
    }
}
