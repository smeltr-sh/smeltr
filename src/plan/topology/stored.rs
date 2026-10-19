//! A source's records as a sample, a capture and the test API store them, and back. Read once per request, or at most
//! a capture's bound of rows, so `serde_json` holds them.

use super::*;

impl SourceMeta {
    /// `record` as its record object: `{"_raw": text}`, `{}` for a source that decodes none, or a row's fields. A
    /// null field is left out, as a document leaves it out.
    pub fn record_object(&self, record: &crate::io::SourceRecord, text: &str) -> crate::io::RecordObject {
        if self.decodes() {
            crate::io::raw_object(text)
        } else if self.reads_bytes() {
            crate::io::RecordObject::new()
        } else {
            serde_json::from_slice(&self.record_json(record, text)).expect("a serialized row is a JSON object")
        }
    }

    /// The records `stored` holds, each a record object and its `__source` object as a sample or a capture stores
    /// them, as this meta's source would have read them, each field a record lists as failed (`io::take_mistyped`) read
    /// as failed (`RecordChunk::from_objects`). Err where a record object is not what this meta's record is.
    ///
    /// A source that `reads_bytes` reads each payload from the `__source` object's `bytes`, the base64 `objects`
    /// writes. A stored record without one, from before `objects` kept it or written by hand, is its `_raw` text's
    /// UTF-8 instead, so its record object must be `{"_raw": text}`.
    pub fn read_stored(
        &self,
        stored: &[(&crate::io::RecordObject, &crate::io::SourceFields)],
    ) -> Result<Vec<crate::io::SourceRecord>, String> {
        if self.reads_bytes() {
            let mut chunk = crate::io::RecordChunkBuilder::new();
            let mut payload = Vec::new();
            for (i, (record, fields)) in stored.iter().enumerate() {
                let bytes = SOURCE_BYTES
                    .strip_prefix("__source.")
                    .expect("a source field is under `__source`");
                payload.clear();
                match (fields.get(bytes), record.get("_raw")) {
                    (Some(serde_json::Value::String(b64)), _) => {
                        if !crate::format::bytes_text::BytesForm::Base64.read(b64.as_bytes(), &mut payload) {
                            return Err(format!("record {}'s `__source.bytes` is not base64", i + 1));
                        }
                    }
                    (None, Some(serde_json::Value::String(raw))) if record.len() == 1 && self.decodes() => {
                        payload.extend_from_slice(raw.as_bytes())
                    }
                    _ if self.decodes() => {
                        return Err(format!(
                            "record {} is neither one `_raw` text nor has base64 `__source.bytes`, and its source \
                             reads bytes",
                            i + 1
                        ));
                    }
                    _ => {
                        return Err(format!(
                            "record {} has no base64 `__source.bytes`, and its source reads nothing else",
                            i + 1
                        ));
                    }
                }
                let time = (fields.get(crate::io::hec::TIME))
                    .and_then(serde_json::Value::as_str)
                    .and_then(crate::ops::timeparse::parse_auto);
                chunk.push_stamped(&payload, 0, 0, std::sync::Arc::from(""), None, time);
            }
            Ok(chunk
                .finish()
                .into_iter()
                .zip(stored)
                .map(|(record, (_, fields))| {
                    let mut record = crate::io::SourceRecord::read(record.chunk, record.row, fields);
                    // A Kafka record's headers are its own; none pasted is none, never what a receiver files.
                    if self.field_type("__source.headers").is_some() && !fields.contains_key("headers") {
                        record.headers = Some(Vec::new());
                    }
                    record
                })
                .collect())
        } else {
            let fields = &self.record;
            let paths: Vec<Vec<String>> = fields
                .iter()
                .map(|(name, _)| {
                    crate::plan::tree::segments(name)
                        .map(std::borrow::Cow::into_owned)
                        .collect()
                })
                .collect();
            let mut objects: Vec<crate::io::RecordObject> =
                stored.iter().map(|(record, _)| (*record).clone()).collect();
            let failed: Vec<Vec<String>> = objects.iter_mut().map(crate::io::take_mistyped).collect();
            // An open record keeps what it does not declare at its top level; below that, a declared object is closed.
            let open = self.is_open();
            if let Some((i, extra)) = objects.iter().enumerate().find_map(|(i, record)| {
                let undeclared = |record: &crate::io::RecordObject| undeclared(record, &mut Vec::new(), "", &paths);
                let claimed: crate::io::RecordObject = match open {
                    true => (record.iter())
                        .filter(|(k, _)| paths.iter().any(|p| p.first() == Some(*k)))
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect(),
                    false => (*record).clone(),
                };
                undeclared(&claimed).map(|k| (i, k))
            }) {
                return Err(format!(
                    "record {} has `{extra}`, a field its source does not declare",
                    i + 1
                ));
            }
            let objects: Vec<&crate::io::RecordObject> = objects.iter().collect();
            let chunk = std::sync::Arc::new(crate::io::RecordChunk::from_objects(fields, &objects, &failed)?);
            Ok(stored
                .iter()
                .enumerate()
                .map(|(row, (_, fields))| crate::io::SourceRecord::read(chunk.clone(), row as u32, fields))
                .collect())
        }
    }

    /// Each record's `__source` object, as JSON: every field this kind declares, whether anything reads it or not,
    /// for records `source` read. `__source.bytes` is its base64, as JSON holds bytes, which `read_stored` reads back.
    pub fn objects(&self, records: &[crate::io::SourceRecord], source: Option<&str>) -> Vec<crate::io::SourceFields> {
        let mut pool = crate::core::buffer::BufferPool::new();
        let mut repaired = Vec::new();
        let stored: Vec<usize> = (0..self.source.len()).collect();
        let columns: Vec<Column> = stored
            .iter()
            .map(|&i| self.column(i, records, source, &mut pool, &mut repaired))
            .collect();
        let named = stored
            .iter()
            .map(|&i| &self.source[i])
            .zip(&columns)
            .map(|((name, _), column)| {
                let name = name
                    .strip_prefix("__source.")
                    .expect("a source field is under `__source`");
                (name, column)
            });
        let serializer = crate::format::record::RecordView::from_named(named);
        let objects = (0..records.len())
            .map(|row| serde_json::from_slice(&serializer.row(row)).expect("a serialized row is a JSON object"))
            .collect();
        for column in columns {
            column.reclaim(&mut pool);
        }
        objects
    }
}
