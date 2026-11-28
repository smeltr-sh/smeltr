//! Records as a sample and a capture hold them: rows of cells under a table's columns. They are read once per request,
//! or at most a capture's bound of rows, so `serde_json` holds them.

use std::sync::Arc;

use super::{RecordChunk, SourceRecord, elastic_receiver, hec, otlp};
use crate::core::column::DataType;
use crate::heartbeat::{CaptureColumn, CapturedRow};
use crate::plan::topology::Entry;

/// A record as an object: each of its fields by name, as the source read it.
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

/// One row of a capture's table, under the table's columns.
#[derive(Clone, Copy)]
pub struct Cells<'a> {
    pub(crate) columns: &'a [CaptureColumn],
    pub(crate) row: &'a CapturedRow,
}

/// `held`, the object at keys `at`, without the keys an entry of `fields` claims under it.
fn unclaimed(held: &RecordObject, at: &[String], fields: &[(Entry, DataType)]) -> RecordObject {
    let claimed = |key: &str| {
        fields.iter().any(|(entry, _)| {
            let keys = entry.keys();
            keys.len() > at.len() && keys.starts_with(at) && keys[at.len()] == key
        })
    };
    (held.iter())
        .filter(|(k, _)| !claimed(k))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

/// Where a table's rows hold one entry of a record: found once for the table, then read from each row.
struct Place<'f> {
    entry: &'f Entry,
    fields: &'f [(Entry, DataType)],
    keys: Vec<String>,
    /// Each column that may hold the entry, nearest first, with how many of the entry's keys the column's name spells.
    /// The keys after those are read inside the column's value.
    held: Vec<(usize, usize)>,
    /// Each column below a field, with its path from the field and whether it holds unclaimed fields there.
    below: Vec<(usize, String, bool)>,
}

impl<'f> Place<'f> {
    /// The place of `entry` of `fields` under `columns`. Its own column is nearest. After it come the columns above it,
    /// each an object's own column or the column of its unclaimed fields, the deepest object first and the root's last.
    fn of(columns: &[CaptureColumn], entry: &'f Entry, fields: &'f [(Entry, DataType)]) -> Place<'f> {
        let keys = entry.keys();
        let (mut held, mut below) = (Vec::new(), Vec::new());
        for (c, column) in columns.iter().enumerate() {
            let (above, rest): (Vec<String>, bool) = match crate::plan::tree::unclaimed_object(&column.name) {
                Some(object) => (crate::plan::tree::object_keys(object).map(Into::into).collect(), true),
                None => (
                    crate::plan::tree::segments(&column.name).map(Into::into).collect(),
                    false,
                ),
            };
            if keys.starts_with(&above) {
                let own = above.len() == keys.len() && rest == entry.field().is_none();
                held.push(((own, above.len()), c));
            } else if entry.field().is_some() && above.starts_with(&keys) {
                let path =
                    (above[keys.len()..].iter()).fold(String::new(), |at, key| crate::plan::tree::child(&at, key));
                below.push((c, path, rest));
            }
        }
        held.sort_unstable_by(|a, b| b.cmp(a));
        let held = held.into_iter().map(|((_, depth), c)| (depth, c)).collect();
        Place {
            entry,
            fields,
            keys,
            held,
            below,
        }
    }

    /// `held`, a field's object, with what `row` holds in each column below the field. `None` where `held` is empty and
    /// the row holds nothing in any of them.
    fn with_below<'a>(
        &self,
        mut held: RecordObject,
        row: &CapturedRow,
    ) -> Option<std::borrow::Cow<'a, serde_json::Value>> {
        use crate::config_file::put_path;
        for (c, path, rest) in &self.below {
            match row.values.get(*c) {
                Some(serde_json::Value::Object(keys)) if *rest && !row.failed.contains(c) => {
                    for (key, value) in keys {
                        put_path(&mut held, &crate::plan::tree::child(path, key), value.clone());
                    }
                }
                Some(value) if !rest && !value.is_null() && !row.failed.contains(c) => {
                    put_path(&mut held, path, value.clone())
                }
                _ => {}
            }
        }
        (!held.is_empty()).then_some(std::borrow::Cow::Owned(serde_json::Value::Object(held)))
    }

    /// What `row` holds here: the nearest column's value, or where the row holds nothing in it, the value at the
    /// entry's place inside the next. A field's object also takes what the row holds in each column below the field. A
    /// row holds nothing in a column its batch did not have (`config_file::sample_table`). `None` where the row holds
    /// nothing in any. `Err` is the text of a cell that failed its type, or of a value on the way that holds no field.
    fn read<'a>(&self, row: &'a CapturedRow) -> Result<Option<std::borrow::Cow<'a, serde_json::Value>>, String> {
        use serde_json::Value;
        'column: for (nearest, &(depth, c)) in self.held.iter().enumerate() {
            let mut value = row.values.get(c).unwrap_or(&Value::Null);
            if row.failed.contains(&c) {
                if nearest > 0 {
                    continue;
                }
                // Stored as the source spelled it.
                return Err(value.as_str().map_or_else(|| value.to_string(), str::to_string));
            }
            for key in &self.keys[depth..] {
                value = match value {
                    Value::Object(held) => match held.get(key) {
                        Some(value) => value,
                        None => continue 'column,
                    },
                    Value::Null => continue 'column,
                    unfit => return Err(unfit.to_string()),
                };
            }
            match (self.entry, value) {
                (_, Value::Null) => {}
                (Entry::Field(_), Value::Object(held)) if !self.below.is_empty() => {
                    return Ok(self.with_below(held.clone(), row));
                }
                (Entry::Field(_), value) => return Ok(Some(std::borrow::Cow::Borrowed(value))),
                (Entry::Unclaimed(_), Value::Object(held)) => {
                    let rest = unclaimed(held, &self.keys, self.fields);
                    if !rest.is_empty() {
                        return Ok(Some(std::borrow::Cow::Owned(Value::Object(rest))));
                    }
                }
                (Entry::Unclaimed(_), unfit) => return Err(unfit.to_string()),
            }
        }
        Ok(self.with_below(RecordObject::new(), row))
    }
}

impl<'a> Cells<'a> {
    /// What this row holds for `entry` of `fields` (`Place::read`).
    pub(crate) fn of(
        &self,
        entry: &Entry,
        fields: &[(Entry, DataType)],
    ) -> Result<Option<std::borrow::Cow<'a, serde_json::Value>>, String> {
        Place::of(self.columns, entry, fields).read(self.row)
    }

    /// The fields this row holds under `object`, a root key: what its place above holds, then each column below it.
    pub(crate) fn under(&self, object: &'static str) -> RecordObject {
        match self.of(&Entry::Field(object.into()), &[]) {
            Ok(Some(value)) => value.as_object().cloned().unwrap_or_default(),
            _ => RecordObject::new(),
        }
    }
}

/// The keys of the object `object` of `record` that no entry of `fields` claims, by its key there: what that object's
/// unclaimed fields hold. `None` where every key is claimed, or `record` holds no object there.
pub fn unclaimed_of(record: &RecordObject, object: &str, fields: &[(Entry, DataType)]) -> Option<serde_json::Value> {
    let at: Vec<String> = Entry::Unclaimed(object.to_string().into()).keys();
    let mut held = record;
    for key in &at {
        held = held.get(key)?.as_object()?;
    }
    let rest = unclaimed(held, &at, fields);
    (!rest.is_empty()).then_some(serde_json::Value::Object(rest))
}

/// A byte record's object: `{"_raw": text}`.
pub fn raw_object(text: impl Into<String>) -> RecordObject {
    RecordObject::from_iter([("_raw".to_string(), serde_json::Value::from(text.into()))])
}

impl SourceRecord {
    /// Row `row` of `chunk`, read at what `fields`, a record's `__source` object, says. A coordinate `fields` lacks is
    /// zero or empty, as a pasted record's are.
    pub(crate) fn read(chunk: Arc<RecordChunk>, row: u32, fields: &SourceFields) -> SourceRecord {
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
    /// A chunk of `rows` of a capture's table under `columns`, read as `fields` declare (`Cells::of`). A value that is
    /// not its field's type is null and counted failed, as a source counts one, and so is a cell its row lists as
    /// failed. A `bytes` field is stored as its base64, as a capture writes it. Err for a field of a type a capture
    /// cannot carry.
    pub fn from_cells(
        fields: &[(Entry, DataType)],
        columns: &[CaptureColumn],
        rows: &[CapturedRow],
    ) -> Result<RecordChunk, String> {
        use crate::io::values::{self, Value};
        if let Some((name, ty)) = fields
            .iter()
            .filter(|(_, ty)| *ty != DataType::Bytes)
            .find_map(|(name, ty)| Some((name, values::unbuilt(ty)?)))
        {
            return Err(format!(
                "field `{name}` is declared {}, which a capture cannot hold",
                ty.spell()
            ));
        }
        let mut pool = crate::core::buffer::BufferPool::new();
        let mut failed = Vec::new();
        let mut out = Vec::with_capacity(fields.len());
        for (i, (entry, ty)) in fields.iter().enumerate() {
            let place = Place::of(columns, entry, fields);
            let cells: Vec<_> = rows.iter().map(|row| place.read(row)).collect();
            let read: Vec<Option<&serde_json::Value>> = (cells.iter().enumerate())
                .map(|(row, cell)| match cell {
                    Ok(value) => value.as_deref(),
                    Err(text) => {
                        failed.push((row as u32, i, text.clone()));
                        None
                    }
                })
                .collect();
            if *ty == DataType::Bytes {
                let mut b =
                    crate::core::column::StrBuilder::new(&mut pool, crate::core::column::VarType::Bytes, read.len());
                let mut bytes = Vec::new();
                for (row, v) in read.iter().enumerate() {
                    bytes.clear();
                    let text = v.and_then(|v| v.as_str());
                    match text {
                        Some(t) if crate::format::bytes_text::BytesForm::Base64.read(t.as_bytes(), &mut bytes) => {
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
                out.push(b.finish());
                continue;
            }
            let given: Vec<Value> = read
                .iter()
                .map(|v| match v {
                    Some(v) => {
                        let mut encoded = Vec::new();
                        put_variant(&mut encoded, v);
                        values::from_variant(crate::core::variant::Encoded::check(&encoded))
                    }
                    None => Value::Null,
                })
                .collect();
            let (column, refused) = values::column(&mut pool, ty, &given).expect("every field's type was checked");
            for row in refused {
                let v = read[row].expect("a refused value is present");
                failed.push((row as u32, i, v.to_string()));
            }
            out.push(column);
        }
        Ok(RecordChunk::new(out, failed))
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
fn put_variant(out: &mut Vec<u8>, v: &serde_json::Value) {
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
