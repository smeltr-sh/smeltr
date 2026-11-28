use crate::core::batch::Batch;
use crate::core::column::{Column, DataType, Scalar};
use crate::core::error::ExceptionKind;
use crate::core::id::Id;
use crate::core::variant::EncodedBuf;

use super::*;

/// The bucket and key an SQS/S3 record's topic, `bucket/key`, names.
fn object_of(record: &crate::io::SourceRecord) -> (&[u8], &[u8]) {
    let topic = record.topic.as_bytes();
    match topic.iter().position(|&b| b == b'/') {
        Some(at) => (&topic[..at], &topic[at + 1..]),
        None => (topic, &[]),
    }
}

/// The part of an object's key that names its stream: its directories up to the first one with a digit in it, since
/// a date, an hour or a sequence number is where one stream's keys stop agreeing. `logs/okta/2026/10/14/a.json.gz`
/// and `logs/okta/2026/10/15/b.json.gz` are one stream, `logs/okta/`. The object's own name is never part of it.
fn object_prefix(key: &[u8]) -> std::borrow::Cow<'_, str> {
    let dirs = key.iter().rposition(|&b| b == b'/').map_or(&key[..0], |at| &key[..=at]);
    let mut kept = 0;
    for segment in dirs.split_inclusive(|&b| b == b'/') {
        if segment.iter().any(u8::is_ascii_digit) {
            break;
        }
        kept += segment.len();
    }
    String::from_utf8_lossy(&dirs[..kept])
}

impl SourceMeta {
    /// What its kind files `record` under, for the leader to tell one stream reaching a source from another
    /// (`heartbeat::UnmatchedStreams`): a HEC or S2S sender's sourcetype, an Elasticsearch sender's index, an
    /// object's bucket and directory prefix (`object_prefix`), and otherwise the record's topic, which a receiver
    /// fills with the sender's address and Kafka with the topic read. `None` where a sender named nothing.
    pub(crate) fn stream_key<'r>(&self, record: &'r crate::io::SourceRecord) -> Option<std::borrow::Cow<'r, str>> {
        match self.kind {
            "hecReceiver" | "s2sReceiver" => filed(record, "sourcetype").map(String::from_utf8_lossy),
            "elasticReceiver" => filed(record, "index").map(String::from_utf8_lossy),
            _ if self.source.iter().any(|(f, _)| *f == "__source.key") => {
                let (bucket, key) = object_of(record);
                Some(std::borrow::Cow::Owned(format!(
                    "{}/{}",
                    String::from_utf8_lossy(bucket),
                    object_prefix(key)
                )))
            }
            _ => Some(std::borrow::Cow::Borrowed(&record.topic)),
        }
    }

    /// The `__source` field that names what [`Self::stream_key`] files a record under, where a sender is counted
    /// under it (`streams::SenderTally`). `None` where the kind declares none.
    pub(crate) fn stream_field(&self) -> Option<&'static str> {
        let declares = |field: &str| self.source.iter().any(|(f, _)| *f == field);
        match self.kind {
            "hecReceiver" | "s2sReceiver" => Some("__source.sourcetype"),
            "elasticReceiver" => Some("__source.index"),
            _ if declares("__source.key") => Some("__source.key"),
            _ => TOPIC_FIELDS.into_iter().find(|f| declares(f)),
        }
    }

    /// A route condition that takes the records this kind files under `key` ([`Self::stream_key`]). An object's
    /// condition takes its bucket and every key under its prefix, so it also takes a stream filed under a longer
    /// prefix, which is the most a condition can say without the prefix rule itself. `None` where this kind files
    /// no field a condition could read.
    pub(crate) fn stream_condition(&self, key: &str) -> Option<String> {
        let literal = |s: &str| serde_json::to_string(s).expect("a string serializes");
        // A sender that names nothing is keyed as the empty key, and its field reads null.
        let filed_as = |field: &str, key: &str| match key {
            "" => format!("({field} ?? \"\") == \"\""),
            key => format!("{field} == {}", literal(key)),
        };
        let declares = |field: &str| self.source.iter().any(|(f, _)| *f == field);
        match self.kind {
            "hecReceiver" | "s2sReceiver" => Some(filed_as("__source.sourcetype", key)),
            "elasticReceiver" => Some(filed_as("__source.index", key)),
            _ if declares("__source.key") => {
                let (bucket, prefix) = key.split_once('/').unwrap_or((key, ""));
                let bucket = format!("__source.bucket == {}", literal(bucket));
                Some(match prefix {
                    "" => bucket,
                    prefix => format!("{bucket} && startsWith(__source.key, {})", literal(prefix)),
                })
            }
            _ => TOPIC_FIELDS
                .into_iter()
                .find(|f| declares(f))
                .map(|f| format!("{f} == {}", literal(key))),
        }
    }
}

impl SourceMeta {
    /// The meta of a sample captured under `columns`: each column a field of its record, read from the record object by
    /// its path and typed as spelled, `__source` and `__destination` columns as much as any other. A sample that names
    /// no field, a paste, is `_raw` text; the instant it was taken at is the preview's, not a field of its records.
    pub fn of_columns(columns: &[crate::heartbeat::CaptureColumn]) -> Result<SourceMeta, String> {
        let taken_at = |c: &crate::heartbeat::CaptureColumn| c.name == crate::heartbeat::CaptureBatch::TAKEN_AT;
        if columns
            .iter()
            .all(|c| c.name == crate::plan::tree::UNCLAIMED || taken_at(c))
        {
            return Ok(SourceMeta::NONE);
        }
        let mut open = false;
        let mut record = Vec::with_capacity(columns.len());
        for c in columns {
            match crate::plan::tree::unclaimed_object(&c.name) {
                Some("") => open = true,
                // A column of an object's unclaimed fields declares that object open.
                Some(object) => record.push((object.to_string(), DataType::Struct(Vec::new(), true))),
                None => {
                    let ty = crate::expr::parse::parse_type(&c.ty)
                        .map_err(|e| format!("column `{}` has type {:?}, which does not read: {e}", c.name, c.ty))?;
                    record.push((c.name.clone(), ty));
                }
            }
        }
        // `__live` is the one name a pipeline cannot take a value of from its input: the engine primes it.
        let meta = SourceMeta::NONE.declare(record, |key| key != "__live")?;
        Ok(if open { meta.opened() } else { meta })
    }

    /// Field `field` as a column over `records`, in record order, for records `source` read: its name in the
    /// deployment, `None` where there is no deployment to name it. A property of the kind, not of a live `Source`: the
    /// values ride on `SourceRecord`, so transform builds these columns without touching anything ingest owns. Keyed
    /// by the field's name rather than its position, so an entry can move within a source's list without its
    /// extractor staying behind at the old index.
    ///
    /// Each row whose value had bytes that did not decode, replaced with U+FFFD, is pushed to `repaired`.
    pub(super) fn column(
        &self,
        field: usize,
        records: &[crate::io::SourceRecord],
        source: Option<&str>,
        pool: &mut crate::core::buffer::BufferPool,
        repaired: &mut Vec<u32>,
    ) -> Column {
        match self.source[field].0 {
            "__source.name" => str_column(pool, records.iter().map(|_| source.map(str::as_bytes))),
            "__source.type" => str_column(pool, records.iter().map(|_| Some(self.kind.as_bytes()))),
            f if TOPIC_FIELDS.contains(&f) => str_column(pool, records.iter().map(|r| Some(r.topic.as_bytes()))),
            "__source.bucket" => str_column(pool, records.iter().map(|r| Some(object_of(r).0))),
            "__source.key" => str_column(pool, records.iter().map(|r| Some(object_of(r).1))),
            "__source.partition" => i64_column(pool, records.iter().map(|r| r.partition as i64)),
            "__source.offset" => i64_column(pool, records.iter().map(|r| r.offset)),
            "__source.headers" => headers_column(pool, records, repaired),
            "__source.host"
            | "__source.source"
            | "__source.sourcetype"
            | "__source.index"
            | "__source.id"
            | "__source.pipeline"
            | "__source.scope.name"
            | "__source.scope.version" => {
                let key = &self.source[field].0["__source.".len()..];
                if let Some(shared) = shared_filed(records) {
                    let rows = records.len();
                    let Some(value) = filed_at(shared, key) else {
                        return Column::empty(DataType::Str, rows);
                    };
                    let text = String::from_utf8_lossy(value);
                    if matches!(text, std::borrow::Cow::Owned(_)) {
                        repaired.extend(0..rows as u32);
                    }
                    return Column::constant(Scalar::Str(text.into_owned()), rows);
                }
                let values: Vec<Option<&[u8]>> = records.iter().map(|r| filed(r, key)).collect();
                repaired.extend(
                    (values.iter().enumerate())
                        .filter(|(_, v)| v.is_some_and(|v| std::str::from_utf8(v).is_err()))
                        .map(|(row, _)| row as u32),
                );
                let text: Vec<Option<std::borrow::Cow<'_, str>>> =
                    values.iter().map(|v| v.map(String::from_utf8_lossy)).collect();
                str_column(pool, text.iter().map(|t| t.as_deref().map(str::as_bytes)))
            }
            "__source.time" => {
                let mut col = Column::alloc(pool, &DataType::DateTime, records.len());
                let out = col.datetime_mut().expect("a datetime column");
                for (row, r) in records.iter().enumerate() {
                    let micros = r.chunk.time(r.row as usize).or_else(|| {
                        filed(r, crate::io::hec::TIME).and_then(|v| std::str::from_utf8(v).ok()?.parse::<i64>().ok())
                    });
                    match micros {
                        Some(micros) => out.values[row] = micros,
                        None => out.validity.set_valid(row, false),
                    }
                }
                col
            }
            "__source.fields" | "__source.resource" | "__source.scope.attributes" => {
                let key = &self.source[field].0["__source.".len()..];
                if let Some(shared) = shared_filed(records) {
                    return match filed_at(shared, key) {
                        Some(value) => {
                            Column::constant(Scalar::Variant(EncodedBuf::check(value.to_vec())), records.len())
                        }
                        None => Column::empty(DataType::Variant, records.len()),
                    };
                }
                var_column(
                    pool,
                    crate::core::column::VarType::Variant,
                    records.iter().map(|r| filed(r, key)),
                )
            }
            SOURCE_BYTES => {
                let values: Vec<Option<&[u8]>> = self.read_bytes_each(records).collect();
                var_column(pool, crate::core::column::VarType::Bytes, values.into_iter())
            }
            other => unreachable!("no extractor for declared field {other}"),
        }
    }
}

impl SourceMeta {
    /// Put this meta's columns over `records` into `batch`: every field of the record, and each `__source` field
    /// `wanted` names by its index. `text` is each record's payload decoded, read where the source `reads_bytes`;
    /// `source` is what `__source.name` reads.
    ///
    /// Returns each value ingest could not take as it came, as its row, its slot, how, and the value as read: a
    /// `__source` value repaired to valid UTF-8, or a record value that is not its field's type and is null.
    pub fn ingest<T: AsRef<str>>(
        &self,
        batch: &mut Batch,
        records: &[crate::io::SourceRecord],
        text: &[T],
        source: Option<&str>,
        wanted: impl Fn(usize) -> bool,
        pool: &mut crate::core::buffer::BufferPool,
    ) -> Vec<(u32, crate::core::id::SlotId, ExceptionKind, Option<String>)> {
        let mut noted = Vec::new();
        if self.decodes() {
            let values = text.iter().map(|t| Some(t.as_ref().as_bytes()));
            batch.put(crate::core::batch::SLOT_RAW, str_column(pool, values), pool);
        } else if !self.reads_bytes() {
            let fields = &self.record;
            let (chunk, rows) = chunk_rows(records);
            let slot_of = |field: usize| crate::core::id::SlotId::at(crate::core::batch::PREFIX_SLOTS + field);
            for (i, (_, ty)) in fields.iter().enumerate() {
                let slot = slot_of(i);
                let column = match &chunk {
                    Some(chunk) => chunk.column(i).gather(pool, &rows),
                    None => Column::empty(ty.clone(), 0),
                };
                batch.put(slot, column, pool);
            }
            if let Some(chunk) = &chunk {
                for (at, &row) in rows.iter().enumerate() {
                    // A payload that did not read has no field of its own to blame, so its record's first holds it.
                    if let Some(why) = chunk.unread(row) {
                        let kind = ExceptionKind::Unparsable;
                        noted.push((at as u32, slot_of(0), kind, Some(why.to_string())));
                        continue;
                    }
                    let from = chunk.failed().partition_point(|(r, _, _)| *r < row);
                    for (_, field, value) in chunk.failed()[from..].iter().take_while(|(r, _, _)| *r == row) {
                        let kind = ExceptionKind::UnparsableField {
                            to: fields[*field].1.clone(),
                        };
                        let slot = slot_of(*field);
                        noted.push((at as u32, slot, kind, Some(value.clone())));
                    }
                }
            }
        }
        let mut repaired = Vec::new();
        for i in (0..self.source.len()).filter(|i| wanted(*i)) {
            let slot = self.source_slot(i);
            batch.put(slot, self.column(i, records, source, pool, &mut repaired), pool);
            noted.extend(
                repaired
                    .drain(..)
                    .map(|row| (row, slot, ExceptionKind::InvalidUtf8, None)),
            );
        }
        noted
    }
}

impl SourceMeta {
    /// `record`'s payload as the source read it, before any decoding: its `__source.bytes`. `None` for a source
    /// that declares none.
    pub(crate) fn read_bytes<'r>(&self, record: &'r crate::io::SourceRecord) -> Option<&'r [u8]> {
        if self.reads_bytes() { record.bytes() } else { None }
    }

    /// `read_bytes` of each of `records`, looking up each chunk's bytes once rather than once a record.
    pub(crate) fn read_bytes_each<'r>(
        &self,
        records: &'r [crate::io::SourceRecord],
    ) -> impl Iterator<Item = Option<&'r [u8]>> + 'r {
        let reads = self.reads_bytes();
        let mut at: Option<(&'r crate::io::RecordChunk, Option<crate::core::column::VarLen<'r>>)> = None;
        records.iter().map(move |record| {
            if !reads {
                return None;
            }
            let chunk: &'r crate::io::RecordChunk = &record.chunk;
            if !at.as_ref().is_some_and(|(c, _)| std::ptr::eq(*c, chunk)) {
                at = Some((chunk, chunk.read()));
            }
            at.as_ref()
                .and_then(|(_, v)| v.as_ref())
                .map(|v| v.get(record.row as usize))
        })
    }

    /// What a destination writing `field`'s bytes takes of `record`: its `__source.bytes`, or a `bytes` field of its
    /// record. `None` for a null, or a field it does not have, which loading refuses (`check_bytes_fields`).
    pub(crate) fn field_bytes(&self, record: &crate::io::SourceRecord, field: &str) -> Option<Vec<u8>> {
        if field == SOURCE_BYTES {
            return self.read_bytes(record).map(<[u8]>::to_vec);
        }
        if self.reads_bytes() {
            return None;
        }
        let at = self
            .record
            .iter()
            .position(|(entry, t)| entry.field() == Some(field) && *t == DataType::Bytes)?;
        let column = record.chunk.column(at);
        let row = record.row as usize;
        (column.is_valid(row)).then(|| column.varlen().expect("a bytes column").get(row).to_vec())
    }

    /// `record` as the document a pipeline with no steps would write for it: its decoded `text` as `_raw`, nothing
    /// for a source that decodes none, or a row's fields.
    pub fn record_json(&self, record: &crate::io::SourceRecord, text: &str) -> Vec<u8> {
        if self.decodes() {
            let mut json = br#"{"_raw":"#.to_vec();
            crate::format::json::write_json_string(text.as_bytes(), &mut json);
            json.push(b'}');
            json
        } else if self.reads_bytes() {
            b"{}".to_vec()
        } else {
            self.row_view(&record.chunk).row(record.row as usize)
        }
    }

    /// The text of `record`'s field `field`, as a destination writing one field's text takes it: its decoded `text`
    /// for `_raw`, or a row's field. `None` for a field the record does not have, or a null.
    pub(crate) fn record_text(&self, record: &crate::io::SourceRecord, text: &str, field: &str) -> Option<Vec<u8>> {
        if self.reads_bytes() {
            (self.decodes() && field == "_raw").then(|| text.as_bytes().to_vec())
        } else {
            self.row_view(&record.chunk)
                .text(field, record.row as usize)
                .map(String::into_bytes)
        }
    }

    /// A view of `chunk`'s columns under the names this meta declares for them.
    fn row_view<'a>(&'a self, chunk: &'a crate::io::RecordChunk) -> crate::format::record::RecordView<'a> {
        assert!(!self.reads_bytes(), "a row's columns are its record's fields");
        crate::format::record::RecordView::from_places(
            self.record
                .iter()
                .enumerate()
                .map(|(i, (entry, _))| (entry.place(), chunk.column(i))),
        )
    }
}

/// The chunk every one of `records` points into, and each record's row of it. A source hands one poll's rows in one
/// chunk (`io::SourceRecord::chunk`), and parking keeps a batch's rows in one, so a batch has one.
fn chunk_rows(records: &[crate::io::SourceRecord]) -> (Option<std::sync::Arc<crate::io::RecordChunk>>, Vec<u32>) {
    let mut chunk: Option<&std::sync::Arc<crate::io::RecordChunk>> = None;
    let rows = records
        .iter()
        .map(|r| {
            let first = chunk.get_or_insert(&r.chunk);
            assert!(
                std::sync::Arc::ptr_eq(first, &r.chunk),
                "the records of one batch point into one chunk"
            );
            r.row
        })
        .collect();
    (chunk.cloned(), rows)
}

fn str_column<'a>(
    pool: &mut crate::core::buffer::BufferPool,
    values: impl ExactSizeIterator<Item = Option<&'a [u8]>> + Clone,
) -> Column {
    var_column(pool, crate::core::column::VarType::Str, values)
}

fn var_column<'a>(
    pool: &mut crate::core::buffer::BufferPool,
    ty: crate::core::column::VarType,
    values: impl ExactSizeIterator<Item = Option<&'a [u8]>> + Clone,
) -> Column {
    let mut b = crate::core::column::StrBuilder::new(pool, ty, values.len());
    b.reserve(pool, values.clone().flatten().map(<[u8]>::len).sum());
    for v in values {
        match v {
            Some(v) => b.push(v),
            None => b.push_null(),
        }
    }
    b.finish()
}

/// What `record` is filed under at `key` (`io::hec::Filed`), which its source carries as a header, or its chunk for
/// every record of it.
fn filed<'a>(record: &'a crate::io::SourceRecord, key: &str) -> Option<&'a [u8]> {
    filed_at(
        record
            .headers
            .as_deref()
            .or_else(|| record.chunk.filed().map(Vec::as_slice))?,
        key,
    )
}

fn filed_at<'a>(filed: &'a [(String, Vec<u8>)], key: &str) -> Option<&'a [u8]> {
    filed.iter().find(|(k, _)| k == key).map(|(_, v)| v.as_slice())
}

/// What every record of `records` is filed under, where they share one chunk that carries it for them all. Each
/// value of it is then one column's every row.
fn shared_filed(records: &[crate::io::SourceRecord]) -> Option<&[(String, Vec<u8>)]> {
    let first = records.first()?;
    let shared = (records.iter()).all(|r| r.headers.is_none() && std::sync::Arc::ptr_eq(&r.chunk, &first.chunk));
    shared.then(|| first.chunk.filed().map(Vec::as_slice)).flatten()
}

/// Each record's headers as one variant object. A record with none is an empty object, so a read of a header it
/// lacks is null rather than a read of a missing object.
fn headers_column(
    pool: &mut crate::core::buffer::BufferPool,
    records: &[crate::io::SourceRecord],
    repaired: &mut Vec<u32>,
) -> Column {
    use crate::core::variant;
    let mut b = crate::core::column::StrBuilder::new(pool, crate::core::column::VarType::Variant, records.len());
    let mut out = Vec::new();
    for (row, r) in records.iter().enumerate() {
        out.clear();
        let open = variant::begin_object(&mut out);
        let mut lossy = false;
        let headers = r.headers.as_deref().unwrap_or_default();
        for (key, value) in headers {
            let text = String::from_utf8_lossy(value);
            lossy |= matches!(text, std::borrow::Cow::Owned(_));
            variant::put_key(&mut out, key);
            variant::put_str(&mut out, &text);
        }
        let count = match headers.len() {
            0 | 1 => headers.len(),
            _ => variant::dedupe(&mut out, open),
        };
        variant::end(&mut out, open, count);
        if lossy {
            repaired.push(row as u32);
        }
        b.push_grow(pool, &out);
    }
    b.finish()
}

fn i64_column(pool: &mut crate::core::buffer::BufferPool, values: impl ExactSizeIterator<Item = i64>) -> Column {
    let rows = values.len();
    let mut col = Column::alloc(pool, &DataType::I64, rows);
    for (slot, v) in col.i64_mut().unwrap().values.iter_mut().zip(values) {
        *slot = v;
    }
    col
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::encoding::Encoding;

    use crate::core::buffer::BufferPool;

    use crate::heartbeat::pasted_from;

    #[test]
    fn an_objects_stream_is_its_directories_up_to_the_first_dated_one() {
        assert_eq!(object_prefix(b"logs/okta/2026/10/14/a.json.gz"), "logs/okta/");
        assert_eq!(object_prefix(b"logs/okta/2026/10/15/b.json.gz"), "logs/okta/");
        assert_eq!(
            object_prefix(b"okta-logs/a.json"),
            "okta-logs/",
            "the object's name is not the stream's"
        );
        assert_eq!(object_prefix(b"a.json"), "");
        assert_eq!(
            object_prefix(b"dt=2026-10-14/part-0.parquet"),
            "",
            "dated from the root"
        );
    }

    #[test]
    fn a_stream_key_is_what_the_kind_files_a_record_under() {
        let record = |topic: &str, filed: Option<Vec<(String, Vec<u8>)>>| crate::io::SourceRecord {
            chunk: crate::io::RecordChunk::of_bytes(b"{}"),
            row: 0,
            offset: 0,
            partition: 0,
            topic: std::sync::Arc::from(topic),
            headers: filed,
        };
        let filed = |key: &str, value: &str| Some(vec![(key.to_string(), value.as_bytes().to_vec())]);
        let st = record("10.0.0.9", filed("sourcetype", "okta:system"));
        assert_eq!(SourceMeta::HEC_RECEIVER.stream_key(&st).as_deref(), Some("okta:system"));
        assert_eq!(
            SourceMeta::HEC_RECEIVER.stream_key(&record("10.0.0.9", None)),
            None,
            "a sender that named no sourcetype"
        );
        let index = record("10.0.0.9", filed("index", "logs-okta"));
        assert_eq!(
            SourceMeta::ELASTIC_RECEIVER.stream_key(&index).as_deref(),
            Some("logs-okta")
        );
        let object = record("landing/logs/okta/2026/10/14/a.json.gz", None);
        assert_eq!(
            SourceMeta::S3_SQS.stream_key(&object).as_deref(),
            Some("landing/logs/okta/")
        );
        let topic = record("orders", None);
        assert_eq!(SourceMeta::KAFKA.stream_key(&topic).as_deref(), Some("orders"));
        let remote = record("10.0.0.9", None);
        assert_eq!(
            SourceMeta::HTTP_RECEIVER.stream_key(&remote).as_deref(),
            Some("10.0.0.9")
        );
    }

    fn field(meta: &SourceMeta, name: &str) -> usize {
        meta.source_fields()
            .iter()
            .position(|(n, _)| *n == name)
            .expect("declared")
    }

    /// The payload a source read is its `__source.bytes`, byte for byte, invalid UTF-8 included. A typed source
    /// declares none: a lone `bytes` field is a field of its record, written by naming it, and never decoded into `_raw`.
    #[test]
    fn only_a_source_that_reads_payloads_has_source_bytes() {
        let read = b"\xffline\x00".to_vec();
        let record = crate::io::SourceRecord {
            chunk: crate::io::RecordChunk::of_bytes(&read),
            row: 0,
            offset: 0,
            partition: 0,
            topic: std::sync::Arc::from("t"),
            headers: None,
        };
        assert_eq!(SourceMeta::KAFKA.read_bytes(&record), Some(&read[..]));
        assert_eq!(SourceMeta::KAFKA.field_bytes(&record, SOURCE_BYTES), Some(read.clone()));
        assert_eq!(SourceMeta::KAFKA.field_bytes(&record, "_raw"), None);

        let typed = SourceMeta::KAFKA
            .declaring(vec![("payload".into(), DataType::Bytes)])
            .unwrap();
        assert_eq!(typed.read_bytes(&record), None);
        assert_eq!(typed.field_type(SOURCE_BYTES), None);
        assert_eq!(typed.field_bytes(&record, SOURCE_BYTES), None);
        assert_eq!(typed.field_bytes(&record, "payload"), Some(read));
    }

    /// A pipeline reads `__source.bytes` as a `bytes` column of each payload as read, invalid UTF-8 included, beside
    /// `_raw`, the same payload decoded.
    #[test]
    fn a_pipeline_reads_the_payload_as_read() {
        let read = b"caf\xe9".to_vec();
        let records = [crate::io::SourceRecord {
            chunk: crate::io::RecordChunk::of_bytes(&read),
            row: 0,
            offset: 0,
            partition: 0,
            topic: std::sync::Arc::from("t"),
            headers: None,
        }];
        let meta = SourceMeta::KAFKA;
        let layout: Vec<DataType> = crate::plan::compile::ingest_fields(&meta)
            .into_iter()
            .map(|(_, t)| t)
            .collect();
        let mut batch = Batch::new(&layout, 1, 0);
        let mut pool = crate::core::buffer::BufferPool::new();
        let at = meta
            .source_fields()
            .iter()
            .position(|(n, _)| *n == SOURCE_BYTES)
            .unwrap();
        let noted = meta.ingest(&mut batch, &records, &["caf\u{fffd}"], None, |i| i == at, &mut pool);

        assert!(noted.is_empty(), "{noted:?}");
        let column = batch.get(meta.source_slot(at));
        assert_eq!(column.data_type(), DataType::Bytes);
        assert_eq!(column.varlen().unwrap().get(0), &read[..]);
        assert_eq!(
            batch.get(crate::core::batch::SLOT_RAW).varlen().unwrap().get(0),
            "caf\u{fffd}".as_bytes()
        );
        batch.reclaim(&mut pool);
    }

    /// A source that declares `encoding: none` has no `_raw`: ingest writes none, a record with no pipeline is `{}`,
    /// and its payload is `__source.bytes` alone.
    #[test]
    fn a_source_that_decodes_none_has_no_raw() {
        let read = b"caf\xe9".to_vec();
        let records = [crate::io::SourceRecord {
            chunk: crate::io::RecordChunk::of_bytes(&read),
            row: 0,
            offset: 0,
            partition: 0,
            topic: std::sync::Arc::from("t"),
            headers: None,
        }];
        let meta = SourceMeta::KAFKA.with_encoding(Encoding::None);
        assert!(meta.record.is_empty());
        assert!(meta.reads_bytes() && !meta.decodes());
        assert_eq!(meta.field_type("_raw"), None);
        assert_eq!(meta.field_type(SOURCE_BYTES), Some(DataType::Bytes));
        assert_eq!(SourceMeta::KAFKA.with_encoding(Encoding::Auto), SourceMeta::KAFKA);
        // A source that reads no bytes keeps no encoding, so the meta it declares and the meta it runs with are one.
        assert_eq!(
            SourceMeta::MYSQL.with_encoding(Encoding::Fixed(encoding_rs::UTF_8)),
            SourceMeta::MYSQL
        );

        let layout: Vec<DataType> = crate::plan::compile::ingest_fields(&meta)
            .into_iter()
            .map(|(_, t)| t)
            .collect();
        let mut batch = Batch::new(&layout, 1, 0);
        let mut pool = crate::core::buffer::BufferPool::new();
        let at = meta
            .source_fields()
            .iter()
            .position(|(n, _)| *n == SOURCE_BYTES)
            .unwrap();
        let noted = meta.ingest(&mut batch, &records, &[""], None, |i| i == at, &mut pool);
        assert!(noted.is_empty(), "{noted:?}");
        assert_eq!(batch.get(meta.source_slot(at)).varlen().unwrap().get(0), &read[..]);
        batch.reclaim(&mut pool);

        assert_eq!(meta.record_json(&records[0], ""), b"{}");
        assert_eq!(meta.record_text(&records[0], "", "_raw"), None);
        assert_eq!(meta.field_bytes(&records[0], SOURCE_BYTES), Some(read));
    }

    /// The GUI reads a table as records by the same rule (`gui-frontend/src/capture.ts`): both read these cases.
    #[test]
    fn a_table_reads_as_the_records_the_gui_shows() {
        #[derive(serde::Deserialize)]
        struct Case {
            name: String,
            #[serde(flatten)]
            table: crate::heartbeat::CaptureBatch,
            records: Vec<crate::io::RecordObject>,
        }
        let cases: Vec<Case> =
            serde_json::from_str(include_str!("../../../tests/fixtures/table-records.json")).unwrap();
        for case in cases {
            let meta = SourceMeta::of_columns(&case.table.columns).unwrap();
            let read = crate::leader::preview::read_sample(&meta, &case.table).unwrap();
            let records: Vec<_> = (read.records.iter().zip(&read.texts))
                .map(|(record, text)| meta.record_object(record, text))
                .collect();
            assert_eq!(records, case.records, "{}", case.name);
        }
    }

    /// A stored `__source` object keeps the payload as base64 `bytes`, so a record reads back byte for byte, whether
    /// its source decodes it or not.
    #[test]
    fn a_stored_payload_reads_back_as_it_was_read() {
        let read = b"caf\xe9".to_vec();
        let records = [crate::io::SourceRecord {
            chunk: crate::io::RecordChunk::of_bytes(&read),
            row: 0,
            offset: 0,
            partition: 0,
            topic: std::sync::Arc::from("t"),
            headers: None,
        }];
        for meta in [SourceMeta::KAFKA, SourceMeta::KAFKA.with_encoding(Encoding::None)] {
            let fields = meta.objects(&records, None);
            assert_eq!(fields[0]["bytes"], serde_json::json!("Y2Fm6Q=="));
            let record = meta.record_object(&records[0], "caf\u{fffd}");
            let back = meta.read_table(&pasted_from(&[(&record, &fields[0])])).unwrap();
            assert_eq!(meta.read_bytes(&back[0]), Some(&read[..]));
        }
        let none = SourceMeta::KAFKA.with_encoding(Encoding::None);
        let err = none
            .read_table(&pasted_from(&[(
                &crate::io::RecordObject::new(),
                &crate::io::SourceFields::new(),
            )]))
            .unwrap_err();
        assert!(err.contains("`__source.bytes`"), "{err}");
    }

    /// A stored record is read as its source reads: a payload source's must be one `_raw` text, and a typed source's may
    /// hold only the fields it declares, each value that is not its type null and counted failed.
    #[test]
    fn a_stored_record_reads_as_its_source_reads() {
        let object = |v: serde_json::Value| v.as_object().unwrap().clone();
        let fields = crate::io::SourceFields::new();

        let raw = object(serde_json::json!({"_raw": "line"}));
        let records = SourceMeta::KAFKA.read_table(&pasted_from(&[(&raw, &fields)])).unwrap();
        assert_eq!(records[0].bytes(), Some(&b"line"[..]));
        let typed = object(serde_json::json!({"id": 1}));
        let err = SourceMeta::KAFKA
            .read_table(&pasted_from(&[(&typed, &fields)]))
            .unwrap_err();
        assert!(err.contains("record 1"), "{err}");

        let meta = SourceMeta::KAFKA
            .declaring(vec![("id".into(), DataType::I64), ("at".into(), DataType::DateTime)])
            .unwrap();
        let good = object(serde_json::json!({"id": 1, "at": "2026-10-10T00:00:00Z"}));
        let bad = object(serde_json::json!({"id": true}));
        let records = meta
            .read_table(&pasted_from(&[(&good, &fields), (&bad, &fields)]))
            .unwrap();
        let chunk = &records[1].chunk;
        assert_eq!(chunk.failed(), &[(1, 0, "true".to_string())]);
        assert_eq!(
            meta.record_object(&records[0], ""),
            object(serde_json::json!({"id": 1, "at": "2026-10-10T00:00:00.000000Z"}))
        );
        let records = meta.read_table(&pasted_from(&[(&raw, &fields)])).unwrap();
        assert!(
            meta.record_object(&records[0], "").is_empty(),
            "a key its record does not declare is left out"
        );

        // A cell its row lists as failed fails again, as it was spelled, though its type would take it; a bytes
        // field's spelling is not read as base64.
        let listed = SourceMeta::KAFKA
            .declaring(vec![("name".into(), DataType::Str), ("blob".into(), DataType::Bytes)])
            .unwrap();
        let column = |name: &str, ty: &str| crate::heartbeat::CaptureColumn {
            name: name.to_string(),
            ty: ty.to_string(),
        };
        let marked = crate::heartbeat::CaptureBatch {
            columns: vec![column("name", "str"), column("blob", "bytes")],
            rows: vec![crate::heartbeat::CapturedRow {
                values: vec![r#"{"a":1}"#.into(), "true".into()],
                failed: vec![0, 1],
                ..Default::default()
            }],
        };
        let records = listed.read_table(&marked).unwrap();
        assert_eq!(
            records[0].chunk.failed(),
            &[(0, 0, r#"{"a":1}"#.to_string()), (0, 1, "true".to_string())]
        );

        let nested = SourceMeta::KAFKA
            .declaring(vec![
                ("user.name".into(), DataType::Str),
                (r#"labels["a.b"]"#.into(), DataType::I64),
            ])
            .unwrap();
        let stored = object(serde_json::json!({"user": {"name": "ann"}, "labels": {"a.b": 2}}));
        let records = nested.read_table(&pasted_from(&[(&stored, &fields)])).unwrap();
        assert_eq!(
            nested.record_object(&records[0], ""),
            stored,
            "a path's value is where its path names"
        );
        let extra = object(serde_json::json!({"user": {"name": "ann", "age": 3}}));
        let records = nested.read_table(&pasted_from(&[(&extra, &fields)])).unwrap();
        assert_eq!(
            nested.record_object(&records[0], ""),
            object(serde_json::json!({"user": {"name": "ann"}})),
            "a key its record does not declare is left out"
        );
        let flat = object(serde_json::json!({"user": "ann", "labels": {"a.b": 2}}));
        let records = nested.read_table(&pasted_from(&[(&flat, &fields)])).unwrap();
        assert_eq!(
            records[0].chunk.failed(),
            &[(0, 0, r#""ann""#.to_string())],
            "a field under a value that is no object fails with that value"
        );
    }

    /// A stored array or object reads as declared all the way down: an array of its elements' type, an object a
    /// field per member. A value that cannot be its type is failed with its JSON under its own field, and an
    /// undeclared field's type is refused.
    #[test]
    fn a_stored_array_or_object_reads_as_declared() {
        let object = |v: serde_json::Value| v.as_object().unwrap().clone();
        let fields = crate::io::SourceFields::new();
        let meta = SourceMeta::KAFKA
            .declaring(vec![
                ("tags".into(), DataType::Array(Box::new(DataType::I64))),
                (
                    "who".into(),
                    DataType::Struct(
                        vec![
                            ("name".into(), DataType::Str),
                            ("at".into(), DataType::DateTime),
                            ("took".into(), DataType::Duration),
                        ],
                        false,
                    ),
                ),
            ])
            .unwrap();
        let good = object(
            serde_json::json!({"tags": [1, 2], "who": {"name": "ann", "at": "2026-10-10T00:00:00Z", "took": 1500}}),
        );
        let bad = object(serde_json::json!({"tags": [1, "x"], "who": {"name": "bo", "at": "never"}}));
        let records = meta
            .read_table(&pasted_from(&[(&good, &fields), (&bad, &fields)]))
            .unwrap();
        assert_eq!(
            meta.record_object(&records[0], ""),
            object(
                serde_json::json!({"tags": [1, 2], "who": {"name": "ann", "at": "2026-10-10T00:00:00.000000Z", "took": 1500}})
            )
        );
        assert_eq!(
            records[1].chunk.failed(),
            &[(1, 0, r#"[1,"x"]"#.to_string()), (1, 2, r#""never""#.to_string())]
        );
        assert_eq!(
            meta.record_object(&records[1], ""),
            object(serde_json::json!({"who": {"name": "bo"}})),
            "a refused element nulls its array, and a refused member only itself"
        );

        let bytes = SourceMeta::KAFKA
            .declaring(vec![("blobs".into(), DataType::Array(Box::new(DataType::Bytes)))])
            .unwrap();
        let blobs = object(serde_json::json!({"blobs": ["3q0="]}));
        let read = bytes.read_table(&pasted_from(&[(&blobs, &fields)])).unwrap();
        assert_eq!(
            bytes.record_object(&read[0], ""),
            blobs,
            "stored bytes are base64, and read back as bytes"
        );
    }

    /// A record's `__source` object, stored and read back, is the record it was made from.
    #[test]
    fn a_records_source_object_reads_back_as_the_record() {
        let record = crate::io::SourceRecord {
            chunk: crate::io::RecordChunk::of_bytes(Vec::new()),
            row: 0,
            offset: 7,
            partition: 3,
            topic: "t".into(),
            headers: Some(vec![("trace".to_string(), b"t1".to_vec())]),
        };
        let objects = SourceMeta::KAFKA.objects(std::slice::from_ref(&record), Some("orders"));
        assert_eq!(objects[0]["name"], "orders");
        let back = crate::io::SourceRecord::read(crate::io::RecordChunk::of_bytes(Vec::new()), 0, &objects[0]);
        assert_eq!(
            (back.offset, back.partition, &*back.topic, back.headers),
            (7, 3, "t", record.headers)
        );
    }

    #[test]
    fn a_source_column_names_its_source_and_kind_and_notes_a_repaired_header() {
        let mut pool = BufferPool::new();
        let mut record = crate::io::SourceRecord {
            chunk: crate::io::RecordChunk::of_bytes(Vec::new()),
            row: 0,
            offset: 0,
            partition: 0,
            topic: "t".into(),
            headers: None,
        };
        let plain = record.clone();
        record.headers = Some(vec![("k".to_string(), vec![0xff])]);
        let records = [plain, record];
        let meta = SourceMeta::KAFKA;
        let mut repaired = Vec::new();

        let name = meta.column(
            field(&meta, "__source.name"),
            &records,
            Some("orders"),
            &mut pool,
            &mut repaired,
        );
        assert_eq!(name.var().unwrap().get(1), b"orders");
        let unnamed = meta.column(field(&meta, "__source.name"), &records, None, &mut pool, &mut repaired);
        assert!(!unnamed.var().unwrap().validity.is_valid(0), "no deployment names it");
        let kind = meta.column(field(&meta, "__source.type"), &records, None, &mut pool, &mut repaired);
        assert_eq!(kind.var().unwrap().get(0), b"kafka");
        assert!(repaired.is_empty());

        let headers = meta.column(
            field(&meta, "__source.headers"),
            &records,
            None,
            &mut pool,
            &mut repaired,
        );
        assert_eq!(repaired, vec![1], "only the row whose value did not decode");
        let empty = headers.variant().unwrap().get(0);
        assert!(crate::core::variant::is_object(empty) && crate::core::variant::member(empty, "k").is_none());
        for c in [name, unnamed, kind, headers] {
            c.reclaim(&mut pool);
        }
    }

    /// A Kafka record's time is the one its source stamped it with, and null where it stamped none. It stays with its
    /// row when a record is kept apart from its poll, and through a sample.
    #[test]
    fn a_kafka_records_time_is_its_stamp_and_survives_a_sample() {
        let mut pool = BufferPool::new();
        let mut b = crate::io::RecordChunkBuilder::new();
        b.push_stamped(b"a", 0, 0, "t".into(), None, None);
        b.push_stamped(b"b", 1, 0, "t".into(), None, Some(1_700_000_000_250_000));
        let mut records = b.finish();
        let meta = SourceMeta::KAFKA;
        let at = field(&meta, "__source.time");
        let mut repaired = Vec::new();
        let time = meta.column(at, &records, None, &mut pool, &mut repaired);
        let read = time.datetime().unwrap();
        assert!(!read.validity.is_valid(0), "no stamp reads null");
        assert_eq!(read.values[1], 1_700_000_000_250_000);
        time.reclaim(&mut pool);

        crate::io::own_rows(&mut records[1..]);
        assert_eq!(
            records[1].chunk.time(records[1].row as usize),
            Some(1_700_000_000_250_000)
        );

        let objects = meta.objects(&records[1..], None);
        let back = meta
            .read_table(&pasted_from(&[(&crate::io::RecordObject::new(), &objects[0])]))
            .unwrap();
        assert_eq!(back[0].chunk.time(0), Some(1_700_000_000_250_000));
        let unstamped = meta.objects(&records[..1], None);
        let back = meta
            .read_table(&pasted_from(&[(&crate::io::RecordObject::new(), &unstamped[0])]))
            .unwrap();
        assert_eq!(back[0].chunk.time(0), None);

        let typed = serde_json::json!({"time": "2023-11-14T22:13:20.25Z", "bytes": "b25l"});
        let back = meta
            .read_table(&pasted_from(&[(
                &crate::io::RecordObject::new(),
                typed.as_object().unwrap(),
            )]))
            .unwrap();
        assert_eq!(back[0].chunk.time(0), Some(1_700_000_000_250_000));
        assert_eq!(back[0].headers, Some(Vec::new()), "a pasted time is no header");
    }

    /// Records whose chunk files them all under one value read it as one constant per field, which no row copies.
    #[test]
    fn what_a_chunk_files_its_every_record_under_is_one_constant() {
        use crate::core::variant;
        let mut pool = BufferPool::new();
        let mut resource = Vec::new();
        let open = variant::begin_object(&mut resource);
        variant::put_key(&mut resource, "service.name");
        variant::put_str(&mut resource, "api");
        variant::end(&mut resource, open, 1);
        let filed = vec![
            ("resource".to_string(), resource.clone()),
            ("scope.name".to_string(), b"lib".to_vec()),
        ];
        let chunk = std::sync::Arc::new(
            crate::io::RecordChunk::new(Vec::new(), Vec::new()).filed_under(std::sync::Arc::new(filed)),
        );
        let record = |row| crate::io::SourceRecord {
            chunk: chunk.clone(),
            row,
            offset: row as i64,
            partition: 0,
            topic: "t".into(),
            headers: None,
        };
        let records = [record(0), record(1)];
        let meta = SourceMeta::OTLP_LOGS;
        let mut repaired = Vec::new();
        let mut column = |name| meta.column(field(&meta, name), &records, None, &mut pool, &mut repaired);

        assert_eq!(
            records[0].size(),
            resource.len() + "resource".len() + "lib".len() + "scope.name".len()
        );
        let shared = column("__source.resource");
        assert_eq!(shared.len(), 2);
        assert_eq!(
            shared.constant_value(),
            Some(&Scalar::Variant(EncodedBuf::check(resource)))
        );
        let name = column("__source.scope.name");
        assert_eq!(name.constant_value(), Some(&Scalar::Str("lib".into())));
        let version = column("__source.scope.version");
        assert_eq!(version.data_type(), DataType::Str);
        assert!(
            version.is_unbound(),
            "a value it is not filed under is null on every row"
        );
        assert!(repaired.is_empty());
        for c in [shared, name, version] {
            c.reclaim(&mut pool);
        }
    }
}
