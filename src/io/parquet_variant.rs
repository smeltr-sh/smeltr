//! A variant (`core::variant`) in Parquet's variant encoding, as an Iceberg `variant` column holds it: a metadata
//! buffer, the dictionary of every key in the value, and the value itself, unshredded
//! (<https://github.com/apache/parquet-format/blob/master/VariantEncoding.md>).
//!
//! Each of a variant's values takes the variant type that holds it exactly: a time is a timestamp with time zone in
//! microseconds, bytes are binary, and a span, which the encoding has no type for, is its count of microseconds.

use std::collections::BTreeMap;

use crate::core::variant::{self, Value};

/// The encoding's version this writes.
const VERSION: u8 = 1;

/// The basic types, in a value's header's lowest two bits.
const PRIMITIVE: u8 = 0;
const SHORT_STRING: u8 = 1;
const OBJECT: u8 = 2;
const ARRAY: u8 = 3;

/// The primitive types this writes, in a primitive's header above its basic type.
const NULL: u8 = 0;
const TRUE: u8 = 1;
const FALSE: u8 = 2;
const INT64: u8 = 6;
const DOUBLE: u8 = 7;
const TIMESTAMP_MICROS: u8 = 12;
const BINARY: u8 = 15;
const STRING: u8 = 16;

/// The longest string a short string holds.
const SHORT_STRING_MAX: usize = 63;

/// The variant `b` as its metadata and its value.
pub fn encode(b: &[u8]) -> (Vec<u8>, Vec<u8>) {
    let mut keys = BTreeMap::new();
    collect_keys(b, &mut keys);
    // Sorted, so the metadata says so and a reader may search it.
    for (id, slot) in keys.values_mut().enumerate() {
        *slot = id as u32;
    }
    let metadata = metadata(&keys);
    let mut value = Vec::with_capacity(b.len());
    put_value(b, &keys, &mut value);
    (metadata, value)
}

fn collect_keys<'a>(b: &'a [u8], keys: &mut BTreeMap<&'a str, u32>) {
    match variant::read(b) {
        Value::Array(items) => items.for_each(|item| collect_keys(item, keys)),
        Value::Object(entries) => {
            for (key, value) in entries {
                keys.insert(key, 0);
                collect_keys(value, keys);
            }
        }
        _ => {}
    }
}

/// The metadata of a dictionary of `keys`, sorted.
fn metadata(keys: &BTreeMap<&str, u32>) -> Vec<u8> {
    let bytes: usize = keys.keys().map(|k| k.len()).sum();
    let size = width(bytes.max(keys.len()));
    let mut out = vec![VERSION | 1 << 4 | (size as u8 - 1) << 6];
    put_uint(&mut out, keys.len(), size);
    let mut offset = 0;
    put_uint(&mut out, 0, size);
    for key in keys.keys() {
        offset += key.len();
        put_uint(&mut out, offset, size);
    }
    for key in keys.keys() {
        out.extend_from_slice(key.as_bytes());
    }
    out
}

fn put_value(b: &[u8], keys: &BTreeMap<&str, u32>, out: &mut Vec<u8>) {
    let primitive = |out: &mut Vec<u8>, ty: u8| out.push(ty << 2 | PRIMITIVE);
    match variant::read(b) {
        Value::Null => primitive(out, NULL),
        Value::Bool(true) => primitive(out, TRUE),
        Value::Bool(false) => primitive(out, FALSE),
        Value::I64(v) | Value::Duration(v) => {
            primitive(out, INT64);
            out.extend_from_slice(&v.to_le_bytes());
        }
        Value::F64(v) => {
            primitive(out, DOUBLE);
            out.extend_from_slice(&v.to_le_bytes());
        }
        Value::DateTime(v) => {
            primitive(out, TIMESTAMP_MICROS);
            out.extend_from_slice(&v.to_le_bytes());
        }
        Value::Str(s) if s.len() <= SHORT_STRING_MAX => {
            out.push((s.len() as u8) << 2 | SHORT_STRING);
            out.extend_from_slice(s.as_bytes());
        }
        Value::Str(s) => {
            primitive(out, STRING);
            out.extend_from_slice(&(s.len() as u32).to_le_bytes());
            out.extend_from_slice(s.as_bytes());
        }
        Value::Bytes(v) => {
            primitive(out, BINARY);
            out.extend_from_slice(&(v.len() as u32).to_le_bytes());
            out.extend_from_slice(v);
        }
        Value::Array(items) => {
            let values: Vec<Vec<u8>> = items.map(|item| value_of(item, keys)).collect();
            let large = values.len() > u8::MAX as usize;
            let size = width(values.iter().map(Vec::len).sum());
            out.push(((size as u8 - 1) | u8::from(large) << 2) << 2 | ARRAY);
            put_uint(out, values.len(), if large { 4 } else { 1 });
            put_offsets(out, &values, size);
            values.iter().for_each(|v| out.extend_from_slice(v));
        }
        Value::Object(entries) => {
            // Its fields are listed by key, as the encoding asks; their values may lie in any order.
            let mut fields: Vec<(&str, Vec<u8>)> = entries.map(|(k, v)| (k, value_of(v, keys))).collect();
            fields.sort_by(|a, b| a.0.cmp(b.0));
            let large = fields.len() > u8::MAX as usize;
            let id_size = width(keys.len().saturating_sub(1));
            let size = width(fields.iter().map(|(_, v)| v.len()).sum());
            out.push(((size as u8 - 1) | (id_size as u8 - 1) << 2 | u8::from(large) << 4) << 2 | OBJECT);
            put_uint(out, fields.len(), if large { 4 } else { 1 });
            for (key, _) in &fields {
                put_uint(out, keys[key] as usize, id_size);
            }
            let values: Vec<Vec<u8>> = fields.into_iter().map(|(_, v)| v).collect();
            put_offsets(out, &values, size);
            values.iter().for_each(|v| out.extend_from_slice(v));
        }
    }
}

fn value_of(b: &[u8], keys: &BTreeMap<&str, u32>) -> Vec<u8> {
    let mut out = Vec::new();
    put_value(b, keys, &mut out);
    out
}

/// Each of `values`' start, and the end of the last, each in `size` bytes.
fn put_offsets(out: &mut Vec<u8>, values: &[Vec<u8>], size: usize) {
    let mut offset = 0;
    put_uint(out, 0, size);
    for v in values {
        offset += v.len();
        put_uint(out, offset, size);
    }
}

/// The fewest bytes, one to four, that hold `max`.
fn width(max: usize) -> usize {
    match max {
        0..=0xff => 1,
        0x100..=0xffff => 2,
        0x1_0000..=0xff_ffff => 3,
        _ => 4,
    }
}

fn put_uint(out: &mut Vec<u8>, v: usize, size: usize) {
    out.extend_from_slice(&(v as u32).to_le_bytes()[..size]);
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// A variant built by `build`, in this crate's encoding.
    fn own(build: impl FnOnce(&mut Vec<u8>)) -> Vec<u8> {
        let mut out = Vec::new();
        build(&mut out);
        out
    }

    /// The value as JSON-like text, as arrow-rs's reader of the encoding reads it: what a reader of the spec sees.
    /// Panics where the encoding is invalid, or its dictionary is not sorted, as its header says.
    pub(crate) fn read(metadata: &[u8], value: &[u8]) -> String {
        use parquet_variant::{Variant, VariantMetadata};
        let dictionary = VariantMetadata::try_new(metadata).expect("valid metadata");
        assert!(dictionary.is_sorted(), "the dictionary is sorted, as its header says");
        let v = Variant::try_new(metadata, value).expect("a valid variant");

        fn text(v: &Variant<'_, '_>) -> String {
            match v {
                Variant::Null => "null".into(),
                Variant::BooleanTrue => "true".into(),
                Variant::BooleanFalse => "false".into(),
                Variant::Int64(n) => n.to_string(),
                Variant::Double(f) => format!("{f:?}"),
                Variant::TimestampMicros(t) => format!("ts:{}", t.timestamp_micros()),
                Variant::Binary(b) => format!("bin:{b:?}"),
                Variant::String(s) => format!("{s:?}"),
                Variant::ShortString(s) => format!("{:?}", s.as_str()),
                Variant::List(items) => format!("[{}]", items.iter().map(|i| text(&i)).collect::<Vec<_>>().join(",")),
                Variant::Object(fields) => {
                    let fields: Vec<String> = fields.iter().map(|(k, v)| format!("{k}:{}", text(&v))).collect();
                    format!("{{{}}}", fields.join(","))
                }
                other => panic!("a type this writer never writes: {other:?}"),
            }
        }
        text(&v)
    }

    fn encoded(b: &[u8]) -> String {
        let (metadata, value) = encode(b);
        read(&metadata, &value)
    }

    #[test]
    fn each_value_takes_the_variant_type_that_holds_it() {
        assert_eq!(encoded(&own(variant::put_null)), "null");
        assert_eq!(encoded(&own(|o| variant::put_bool(o, true))), "true");
        assert_eq!(encoded(&own(|o| variant::put_bool(o, false))), "false");
        assert_eq!(encoded(&own(|o| variant::put_i64(o, -7))), "-7");
        assert_eq!(encoded(&own(|o| variant::put_f64(o, 1.5))), "1.5");
        assert_eq!(encoded(&own(|o| variant::put_datetime(o, 1_000_001))), "ts:1000001");
        assert_eq!(encoded(&own(|o| variant::put_duration(o, 90))), "90");
        assert_eq!(encoded(&own(|o| variant::put_bytes(o, &[0, 1]))), "bin:[0, 1]");
        assert_eq!(encoded(&own(|o| variant::put_str(o, "hi"))), "\"hi\"");
        let long = "x".repeat(64);
        let (_, value) = encode(&own(|o| variant::put_str(o, &long)));
        assert_eq!(value[0], STRING << 2, "a string past 63 bytes is no short string");
        assert_eq!(encoded(&own(|o| variant::put_str(o, &long))), format!("{long:?}"));
    }

    #[test]
    fn an_object_lists_its_fields_by_key_and_an_array_keeps_its_order() {
        let b = own(|o| {
            let outer = variant::begin_object(o);
            variant::put_key(o, "z");
            let list = variant::begin_array(o);
            variant::put_i64(o, 2);
            variant::put_null(o);
            variant::put_str(o, "b");
            variant::end(o, list, 3);
            variant::put_key(o, "a");
            let inner = variant::begin_object(o);
            variant::put_key(o, "z");
            variant::put_bool(o, true);
            variant::end(o, inner, 1);
            variant::end(o, outer, 2);
        });
        assert_eq!(encoded(&b), "{a:{z:true},z:[2,null,\"b\"]}");
    }

    #[test]
    fn a_wide_container_takes_wider_offsets_and_counts() {
        let b = own(|o| {
            let list = variant::begin_array(o);
            for i in 0..300 {
                variant::put_str(o, &format!("{i:0>60}"));
            }
            variant::end(o, list, 300);
        });
        let (metadata, value) = encode(&b);
        assert_eq!(value[0] >> 2 >> 2 & 1, 1, "more than 255 elements is large");
        assert_eq!((value[0] >> 2 & 3) + 1, 2, "offsets past 255 bytes take two");
        let text = read(&metadata, &value);
        assert!(text.starts_with(&format!("[{:?},", format!("{:0>60}", 0))));
        assert!(text.ends_with(&format!("{:?}]", format!("{:0>60}", 299))));

        let b = own(|o| {
            let object = variant::begin_object(o);
            for i in 0..300 {
                variant::put_key(o, &format!("k{i:03}"));
                variant::put_i64(o, i);
            }
            variant::end(o, object, 300);
        });
        let (metadata, value) = encode(&b);
        assert_eq!((value[0] >> 2 >> 2 & 3) + 1, 2, "300 keys take two-byte ids");
        assert!(read(&metadata, &value).ends_with("k299:299}"));
    }
}
