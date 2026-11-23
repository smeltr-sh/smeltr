//! A source's records as a sample and a capture hold them, and back. Read once per request, or at most a capture's
//! bound of rows, so `serde_json` holds them.

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

    /// The rows of `table`, a capture's, as this meta's source would have read them: each field of its record from
    /// the row's cells (`RecordChunk::from_cells`), and what the source set from the row's `__source` fields. Err where
    /// a row cannot be read as this meta's record.
    ///
    /// A source that `reads_bytes` reads each payload from the row's `__source.bytes`, the base64 `objects` writes. A
    /// row without one, pasted or written by hand, is its `_raw` text's UTF-8 instead.
    pub fn read_table(&self, table: &crate::heartbeat::CaptureBatch) -> Result<Vec<crate::io::SourceRecord>, String> {
        let rows = (table.rows.iter()).map(|row| crate::io::Cells {
            columns: &table.columns,
            row,
        });
        let sources: Vec<crate::io::SourceFields> = rows.clone().map(|cells| cells.under("__source")).collect();
        if !self.reads_bytes() {
            let chunk = crate::io::RecordChunk::from_cells(&self.record, &table.columns, &table.rows)?;
            let chunk = std::sync::Arc::new(chunk);
            return Ok((sources.iter().enumerate())
                .map(|(row, fields)| crate::io::SourceRecord::read(chunk.clone(), row as u32, fields))
                .collect());
        }
        let mut chunk = crate::io::RecordChunkBuilder::new();
        let mut payload = Vec::new();
        for (i, (cells, fields)) in rows.zip(&sources).enumerate() {
            let bytes = SOURCE_BYTES
                .strip_prefix("__source.")
                .expect("a source field is under `__source`");
            let raw = cells.of(&Entry::Field("_raw".into()), &[]);
            payload.clear();
            match (fields.get(bytes), raw.as_ref().map(|raw| raw.as_deref())) {
                (Some(serde_json::Value::String(b64)), _) => {
                    if !crate::format::bytes_text::BytesForm::Base64.read(b64.as_bytes(), &mut payload) {
                        return Err(format!("record {}'s `__source.bytes` is not base64", i + 1));
                    }
                }
                (None, Ok(Some(serde_json::Value::String(raw)))) if self.decodes() => {
                    payload.extend_from_slice(raw.as_bytes())
                }
                _ if self.decodes() => {
                    return Err(format!(
                        "record {} has neither a `_raw` text nor base64 `__source.bytes`, and its source reads bytes",
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
        Ok((chunk.finish().into_iter().zip(&sources))
            .map(|(record, fields)| {
                let mut record = crate::io::SourceRecord::read(record.chunk, record.row, fields);
                // A Kafka record's headers are its own; none pasted is none, never what a receiver files.
                if self.field_type("__source.headers").is_some() && !fields.contains_key("headers") {
                    record.headers = Some(Vec::new());
                }
                record
            })
            .collect())
    }

    /// Each record's `__source` object, as JSON: every field this kind declares, whether anything reads it or not,
    /// for records `source` read. `__source.bytes` is its base64, as JSON holds bytes, which `read_table` reads back.
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
