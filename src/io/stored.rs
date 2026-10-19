//! Records as a sample, a capture and the test API store them: a record object of its fields, and its `__source`
//! object. They are read once per request, or at most a capture's bound of rows, so `serde_json` holds them.

use std::sync::Arc;

use super::{RecordChunk, SourceRecord, elastic_receiver, hec, otlp};
use crate::core::column::DataType;
use crate::plan::tree::UNCLAIMED;

/// A record as an object: each of its fields by name, as the source read it. What a sample and a capture hold.
pub type RecordObject = serde_json::Map<String, serde_json::Value>;

/// A record's `__source` object: every field its source set, by name below `__source`.
pub type SourceFields = serde_json::Map<String, serde_json::Value>;

/// The value at `path`, a record field's name, in `object`: each of its keys in turn (`plan::tree::segments`).
pub fn value_at<'v>(object: &'v RecordObject, path: &str) -> Option<&'v serde_json::Value> {
    let mut keys = crate::plan::tree::segments(path);
    let mut value = object.get(keys.next()?.as_ref())?;
    for key in keys {
        value = value.as_object()?.get(key.as_ref())?;
    }
    Some(value)
}

/// The top-level keys of `object` that no field of `fields` claims, by its first key: what the root's unclaimed
/// fields hold (`tree::UNCLAIMED`). `None` where every key is claimed.
pub fn unclaimed_of<N: AsRef<str>>(object: &RecordObject, fields: &[(N, DataType)]) -> Option<serde_json::Value> {
    let claimed = |key: &str| {
        (fields.iter()).any(|(n, _)| crate::plan::tree::segments(n.as_ref()).next().is_some_and(|k| k == key))
    };
    let rest: RecordObject = (object.iter())
        .filter(|(k, _)| !claimed(k))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    (!rest.is_empty()).then_some(serde_json::Value::Object(rest))
}

/// Where a stored record lists the fields whose values failed their type when it was taken.
pub const MISTYPED_FIELDS: &str = "smeltr.mistyped";

/// The fields `record` lists as failed under `MISTYPED_FIELDS`, `{"smeltr": {"mistyped": ["id"]}}`, taken out of it: a
/// stored record's own field reads as the record, and this list as which of its values failed their type when it was
/// taken (`RecordChunk::from_objects`). Anything else there is left as a field.
pub fn take_mistyped(record: &mut RecordObject) -> Vec<String> {
    let (outer, inner) = MISTYPED_FIELDS
        .split_once('.')
        .expect("the failed list nests one level");
    let Some(serde_json::Value::Object(tag)) = record.get_mut(outer) else {
        return Vec::new();
    };
    let names: Vec<String> = match tag.get(inner) {
        Some(serde_json::Value::Array(items)) => match items.iter().map(|v| v.as_str().map(str::to_string)).collect() {
            Some(names) => names,
            None => return Vec::new(),
        },
        _ => return Vec::new(),
    };
    tag.remove(inner);
    if tag.is_empty() {
        record.remove(outer);
    }
    names
}

/// A byte record's object: `{"_raw": text}`.
pub fn raw_object(text: impl Into<String>) -> RecordObject {
    RecordObject::from_iter([("_raw".to_string(), serde_json::Value::from(text.into()))])
}

impl SourceRecord {
    /// Row `row` of `chunk`, read at what `fields`, a record's `__source` object, says. A coordinate `fields` lacks is
    /// zero or empty, as a pasted record's are.
    pub fn read(chunk: Arc<RecordChunk>, row: u32, fields: &SourceFields) -> SourceRecord {
        let int = |key: &str| fields.get(key).and_then(serde_json::Value::as_i64).unwrap_or(0);
        let headers = match fields.get("headers") {
            Some(serde_json::Value::Object(h)) => Some(
                h.iter()
                    .filter_map(|(k, v)| Some((k.clone(), v.as_str()?.as_bytes().to_vec())))
                    .collect(),
            ),
            _ => receiver_filed(fields),
        };
        SourceRecord {
            chunk,
            row,
            offset: int("offset"),
            partition: int("partition") as i32,
            // What a kind names its topic: an API source's page, a receiver's sender.
            topic: Arc::from(
                ["topic", "url", "remote"]
                    .into_iter()
                    .find_map(|key| fields.get(key)?.as_str())
                    .unwrap_or(""),
            ),
            headers,
        }
    }
}

impl RecordChunk {
    /// A chunk of `objects`, each a record object as a sample or a capture stores it, read as `fields` declare. A
    /// value that is not its field's type is null and counted failed, as a source counts one. So is a value whose
    /// field `failed` names for its object (`take_mistyped`), a value that failed when it was taken: its text is how the
    /// source spelled it, which its type may well accept. `failed` may be shorter than `objects`. A `bytes` field is
    /// stored as its base64, as a capture writes it. Err for a field of a type a stored record object cannot carry.
    pub fn from_objects<N: AsRef<str>>(
        fields: &[(N, DataType)],
        objects: &[&RecordObject],
        failed: &[Vec<String>],
    ) -> Result<RecordChunk, String> {
        use crate::io::values::{self, Value};
        if let Some((name, ty)) = fields
            .iter()
            .filter(|(_, ty)| *ty != DataType::Bytes)
            .find_map(|(name, ty)| Some((name.as_ref(), values::unbuilt(ty)?)))
        {
            return Err(format!(
                "field `{name}` is declared {}, which a stored record cannot hold",
                ty.spell()
            ));
        }
        let mut pool = crate::core::buffer::BufferPool::new();
        let marked = |row: usize, name: &str| failed.get(row).is_some_and(|f| f.iter().any(|n| n == name));
        let mut failed = Vec::new();
        let mut columns = Vec::with_capacity(fields.len());
        let unclaimed: Vec<Option<serde_json::Value>> = match fields.iter().any(|(n, _)| n.as_ref() == UNCLAIMED) {
            true => objects.iter().map(|o| unclaimed_of(o, fields)).collect(),
            false => Vec::new(),
        };
        for (i, (name, ty)) in fields.iter().enumerate() {
            let name = name.as_ref();
            let read: Vec<Option<&serde_json::Value>> = match name {
                UNCLAIMED => unclaimed.iter().map(Option::as_ref).collect(),
                name => (objects.iter())
                    .map(|o| value_at(o, name).filter(|v| !v.is_null()))
                    .collect(),
            };
            if *ty == DataType::Bytes {
                let mut b =
                    crate::core::column::StrBuilder::new(&mut pool, crate::core::column::VarType::Bytes, read.len());
                let mut bytes = Vec::new();
                for (row, v) in read.iter().enumerate() {
                    bytes.clear();
                    let text = v.and_then(|v| v.as_str());
                    match text {
                        Some(t)
                            if !marked(row, name)
                                && crate::format::bytes_text::BytesForm::Base64.read(t.as_bytes(), &mut bytes) =>
                        {
                            b.push_grow(&mut pool, &bytes)
                        }
                        _ => {
                            if let Some(v) = v {
                                failed.push((row as u32, i, text.map_or_else(|| v.to_string(), str::to_string)));
                            }
                            b.push_null();
                        }
                    }
                }
                columns.push(b.finish());
                continue;
            }
            let given: Vec<Value> = read
                .iter()
                .enumerate()
                .map(|(row, v)| match v {
                    Some(v) if marked(row, name) => {
                        // Stored as the source spelled it.
                        let text = v.as_str().map_or_else(|| v.to_string(), str::to_string);
                        failed.push((row as u32, i, text));
                        Value::Null
                    }
                    Some(v) => {
                        let mut encoded = Vec::new();
                        put_variant(&mut encoded, v);
                        values::from_variant(&encoded)
                    }
                    None => Value::Null,
                })
                .collect();
            let (column, refused) = values::column(&mut pool, ty, &given).expect("every field's type was checked");
            for row in refused {
                let v = read[row].expect("a refused value is present");
                failed.push((row as u32, i, v.to_string()));
            }
            columns.push(column);
        }
        Ok(RecordChunk::new(columns, failed))
    }
}

/// What a stored receiver's record was filed under (`hec::Filed`), from its `__source` object: a HEC event's
/// envelope, or an OTLP log record's resource and scope. `None` for a record that names none of it.
fn receiver_filed(fields: &SourceFields) -> Option<Vec<(String, Vec<u8>)>> {
    let mut filed = Vec::new();
    for key in hec::NAMED
        .into_iter()
        .chain([elastic_receiver::ID, elastic_receiver::PIPELINE])
    {
        if let Some(serde_json::Value::String(text)) = fields.get(key) {
            filed.push((key.to_string(), text.clone().into_bytes()));
        }
    }
    if let Some(micros) = fields
        .get(hec::TIME)
        .and_then(serde_json::Value::as_str)
        .and_then(crate::ops::timeparse::parse_auto)
    {
        filed.push((hec::TIME.to_string(), micros.to_string().into_bytes()));
    }
    // A key with a dot is a field within one: `scope.name` is `name` in the `scope` object.
    let at = |key: &str| match key.split_once('.') {
        Some((outer, inner)) => fields.get(outer)?.get(inner),
        None => fields.get(key),
    };
    for key in [otlp::SCOPE_NAME, otlp::SCOPE_VERSION] {
        if let Some(serde_json::Value::String(text)) = at(key) {
            filed.push((key.to_string(), text.clone().into_bytes()));
        }
    }
    for key in [hec::FIELDS, otlp::RESOURCE, otlp::SCOPE_ATTRIBUTES] {
        if let Some(object @ serde_json::Value::Object(_)) = at(key) {
            let mut encoded = Vec::new();
            put_variant(&mut encoded, object);
            filed.push((key.to_string(), encoded));
        }
    }
    (!filed.is_empty()).then_some(filed)
}

/// `v` in `core::variant`'s encoding.
pub(crate) fn put_variant(out: &mut Vec<u8>, v: &serde_json::Value) {
    use crate::core::variant;
    use serde_json::Value;
    match v {
        Value::Null => variant::put_null(out),
        Value::Bool(b) => variant::put_bool(out, *b),
        Value::Number(n) => match n.as_i64() {
            Some(i) => variant::put_i64(out, i),
            None => variant::put_f64(out, n.as_f64().unwrap_or(f64::NAN)),
        },
        Value::String(text) => variant::put_str(out, text),
        Value::Array(items) => {
            let open = variant::begin_array(out);
            for item in items {
                put_variant(out, item);
            }
            variant::end(out, open, items.len());
        }
        Value::Object(members) => {
            let open = variant::begin_object(out);
            for (key, member) in members {
                variant::put_key(out, key);
                put_variant(out, member);
            }
            variant::end(out, open, members.len());
        }
    }
}
