//! Classic checkpoints: the table's state at one version, in one Parquet file.
//!
//! The writer whose commit reaches `delta.checkpointInterval` checkpoints that version, from the newest checkpoint in
//! the log before it:
//!
//! ```mermaid
//! flowchart LR
//!   P[protocol, metaData,<br/>txns, domains] --> W
//!   L[the last checkpoint's file actions,<br/>a row group at a time] -->|less those acted on since,<br/>and expired removes| W
//!   S[file actions of the commits since] -->|less those acted on later| W
//!   W[n.checkpoint.parquet,<br/>uploaded a part at a time] --> T[_last_checkpoint] --> X[expired log entries deleted]
//! ```
//!
//! A checkpoint reads and writes every file action of the table, as every Delta writer's does, and holds a row group
//! of them at a time. The last checkpoint may be any a reader takes: classic, in parts, or V2 with sidecars.

use std::collections::HashMap;
use std::sync::Arc;

use crate::io::files::Upload;
use arrow_array::builder::BooleanBuilder;
use arrow_array::cast::AsArray;
use arrow_array::types::{Int32Type, Int64Type};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Int32Array, Int64Array, ListArray, MapArray, RecordBatch, StringArray, StructArray,
    new_null_array,
};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType as Arrow, Field, Fields, Schema as ArrowSchema};
use parquet::arrow::ArrowWriter;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder};
use parquet::basic::Compression;
use parquet::errors::ParquetError;
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{ChunkReader, Length};
use serde_json::{Map, Value, json};
use tokio_util::bytes::{Buf, Bytes};

use super::log::{
    Action, Checkpoint, DeletionVector, Entry, FileAction, Log, Metadata, Name, Snapshot, commit_name, file_key,
    last_checkpoint, now_ms,
};

/// Where a V2 checkpoint's sidecars are, under `_delta_log/`.
const SIDECARS: &str = "_sidecars";

/// The columns of a checkpoint, one action to a row.
const COLUMNS: [&str; 6] = ["txn", "add", "remove", "metaData", "protocol", "domainMetadata"];

/// Rows converted from the log's JSON at a time.
const ROWS_PER_BATCH: usize = 8192;

/// A checkpoint's row group is written, and uploaded, once it is this large.
const ROW_GROUP_BYTES: usize = 32 * 1024 * 1024;

/// Bytes read from a Parquet file's end in the hope of its whole footer.
const FOOTER_GUESS: u64 = 64 * 1024;

const DAY_MS: i64 = 24 * 60 * 60 * 1000;

/// Whether `version` of a table with `metadata` is one to checkpoint.
pub fn due(metadata: &Metadata, version: i64) -> bool {
    let interval = metadata
        .property("delta.checkpointInterval")
        .and_then(|v| v.trim().parse::<i64>().ok())
        .filter(|&v| v > 0)
        .unwrap_or(10);
    version > 0 && version % interval == 0
}

/// Writes the checkpoint of `snapshot`'s version, then deletes what the table's log retention lets go. Gives the
/// checkpoint. A failed cleanup is logged: the checkpoint stands without it.
pub fn write(log: &Log, snapshot: &Snapshot) -> Result<Checkpoint, String> {
    let now = now_ms();
    let metadata = &snapshot.metadata;
    // A retention not read keeps every remove.
    let expired_before = match retention_ms(metadata, "delta.deletedFileRetentionDuration") {
        Ok(retention) => now.saturating_sub(retention.unwrap_or(7 * DAY_MS)),
        Err(_) => i64::MIN,
    };
    let keep = Keep {
        touched: &snapshot.since.touched,
        expired_before,
    };
    let name = format!("{:020}.checkpoint.parquet", snapshot.version);
    let mut out = Out::new(log, &name)?;

    let mut rows = Rows::default();
    rows.push("protocol", to_value(&snapshot.protocol));
    rows.push("metaData", to_value(metadata));
    // A retention not read keeps every transaction.
    let txn_kept_after = retention_ms(metadata, "delta.setTransactionRetentionDuration")
        .unwrap_or(None)
        .map(|r| now.saturating_sub(r));
    for txn in snapshot.txns.values() {
        // A transaction with no time of its own is kept, as one may still be waiting to see it.
        if txn_kept_after.is_none_or(|after| txn.last_updated.is_none_or(|t| t >= after)) {
            rows.push("txn", to_value(txn));
        }
    }
    for domain in snapshot.domains.values() {
        rows.push("domainMetadata", to_value(domain));
    }
    out.write(&rows.take()?)?;

    if let Some(last) = &snapshot.checkpoint {
        for file in &last.files {
            if file.name.ends_with(".json") {
                log.lines(&file.name, |text| {
                    if let Action::File(f) = Action::parse(text)?
                        && keep.keeps(&f.key, f.deleted, None)
                    {
                        rows.push_file(&f)?;
                        if rows.len() == ROWS_PER_BATCH {
                            out.write(&rows.take()?)?;
                        }
                    }
                    Ok(())
                })?;
                out.write(&rows.take()?)?;
            } else {
                for_each_batch(log, &file.name, file.size, &["add", "remove"], |batch| {
                    out.write_files(filtered(batch, &keep)?)
                })?;
            }
        }
        for sidecar in &last.sidecars {
            let name = format!("{SIDECARS}/{}", sidecar_name(&sidecar.path));
            for_each_batch(log, &name, sidecar.size_in_bytes as u64, &["add", "remove"], |batch| {
                out.write_files(filtered(batch, &keep)?)
            })?;
        }
    }
    write_since(log, snapshot, &keep, &mut out)?;
    let (size, actions) = out.finish()?;

    // Another writer's later checkpoint keeps its place in `_last_checkpoint`.
    if last_checkpoint(log).is_none_or(|v| v <= snapshot.version) {
        let last = json!({"version": snapshot.version, "size": actions, "sizeInBytes": size});
        log.put("_last_checkpoint", last.to_string().into_bytes())?;
    }
    let checkpoint = Checkpoint {
        version: snapshot.version,
        files: vec![Entry {
            name,
            size,
            modified: now,
        }],
        sidecars: Vec::new(),
    };
    if let Err(why) = clean(log, metadata, &checkpoint, now) {
        tracing::warn!(target: "delta.checkpoint", error = %why, "could not delete the log's expired entries");
    }
    Ok(checkpoint)
}

/// Which earlier file actions a checkpoint keeps.
struct Keep<'a> {
    /// Keys acted on since the last checkpoint by a writer not this one, each with the place of its last action.
    touched: &'a HashMap<String, usize>,
    /// A remove of a file deleted before this, in milliseconds since the epoch, has expired.
    expired_before: i64,
}

impl Keep<'_> {
    /// Whether a file action of `key`, a remove of a file deleted at `deleted`, stays. `at` is its place among the
    /// actions since the last checkpoint, or None when it is in the checkpoint, before all of them.
    fn keeps(&self, key: &str, deleted: Option<i64>, at: Option<usize>) -> bool {
        let last = self.touched.get(key).is_none_or(|&last| Some(last) == at);
        last && deleted.is_none_or(|d| d >= self.expired_before)
    }
}

/// The checkpoint being written, a row group at a time, each uploaded as it is written.
struct Out {
    upload: Upload,
    writer: ArrowWriter<Vec<u8>>,
    schema: Arc<ArrowSchema>,
    rows: i64,
}

impl Out {
    fn new(log: &Log, name: &str) -> Result<Out, String> {
        let schema = checkpoint_schema();
        // Snappy, as Spark writes its checkpoints.
        let props = WriterProperties::builder().set_compression(Compression::SNAPPY).build();
        let writer = ArrowWriter::try_new(Vec::new(), schema.clone(), Some(props))
            .map_err(|e| format!("a checkpoint's schema: {e}"))?;
        Ok(Out {
            upload: log.upload(name)?,
            writer,
            schema,
            rows: 0,
        })
    }

    /// Adds `batch`, of the checkpoint's schema.
    fn write(&mut self, batch: &RecordBatch) -> Result<(), String> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let failed = |e: ParquetError| format!("writing a checkpoint: {e}");
        self.writer.write(batch).map_err(failed)?;
        self.rows += batch.num_rows() as i64;
        if self.writer.in_progress_size() >= ROW_GROUP_BYTES {
            self.writer.flush().map_err(failed)?;
            // The writer counts the bytes it wrote; those already in the upload are not asked for again.
            self.upload.write(std::mem::take(self.writer.inner_mut()))?;
        }
        Ok(())
    }

    /// Adds a batch of file actions read from a checkpoint or a sidecar, whatever else its `add` and `remove` hold.
    fn write_files(&mut self, batch: RecordBatch) -> Result<(), String> {
        let rows = batch.num_rows();
        let columns = (self.schema.fields().iter())
            .map(|field| match batch.column_by_name(field.name()) {
                Some(column) => conform(column, field.data_type()),
                None => Ok(new_null_array(field.data_type(), rows)),
            })
            .collect::<Result<Vec<_>, String>>()?;
        let batch =
            RecordBatch::try_new(self.schema.clone(), columns).map_err(|e| format!("a checkpoint's rows: {e}"))?;
        self.write(&batch)
    }

    /// Ends the file. Gives its size, and how many actions it holds.
    fn finish(mut self) -> Result<(u64, i64), String> {
        self.writer.finish().map_err(|e| format!("writing a checkpoint: {e}"))?;
        self.upload.write(std::mem::take(self.writer.inner_mut()))?;
        let size = self.writer.bytes_written() as u64;
        self.upload.finish()?;
        Ok((size, self.rows))
    }
}

/// `array` as a column of type `to`: a struct's fields by name, one it lacks null and one `to` lacks left out.
fn conform(array: &ArrayRef, to: &Arrow) -> Result<ArrayRef, String> {
    if array.data_type() == to {
        return Ok(array.clone());
    }
    match (array.data_type(), to) {
        (Arrow::Struct(_), Arrow::Struct(fields)) => {
            let s = array.as_struct();
            let children = (fields.iter())
                .map(|f| match s.column_by_name(f.name()) {
                    Some(child) => conform(child, f.data_type()),
                    None => Ok(new_null_array(f.data_type(), s.len())),
                })
                .collect::<Result<Vec<_>, String>>()?;
            let conformed = StructArray::try_new(fields.clone(), children, s.nulls().cloned())
                .map_err(|e| format!("a checkpoint's {}: {e}", array.data_type()))?;
            Ok(Arc::new(conformed))
        }
        _ => {
            let values = (0..array.len())
                .map(|row| to_json(array, row))
                .collect::<Result<Vec<_>, String>>()?;
            let values: Vec<Option<&Value>> = values.iter().map(Some).collect();
            to_arrow(&values, to)
        }
    }
}

/// An action as a row's JSON.
fn to_value(action: &impl serde::Serialize) -> Value {
    serde_json::to_value(action).expect("an action serializes")
}

/// Actions in the log's JSON, gathered into batches of the checkpoint's schema.
#[derive(Default)]
struct Rows {
    /// Each row's column, and its action.
    rows: Vec<(usize, Value)>,
}

impl Rows {
    fn len(&self) -> usize {
        self.rows.len()
    }

    /// Adds a row of the action `value` in column `column`.
    fn push(&mut self, column: &str, value: Value) {
        let at = (COLUMNS.iter().position(|c| *c == column)).expect("a column of the checkpoint");
        self.rows.push((at, value));
    }

    fn push_file(&mut self, action: &FileAction) -> Result<(), String> {
        let value: Value =
            serde_json::from_str(action.json.get()).map_err(|e| format!("a file action in the log: {e}"))?;
        self.push(if action.add { "add" } else { "remove" }, value);
        Ok(())
    }

    fn take(&mut self) -> Result<RecordBatch, String> {
        let schema = checkpoint_schema();
        let rows = std::mem::take(&mut self.rows);
        let columns = (schema.fields().iter().enumerate())
            .map(|(i, field)| {
                let values: Vec<Option<&Value>> = rows.iter().map(|(at, v)| (*at == i).then_some(v)).collect();
                to_arrow(&values, field.data_type())
            })
            .collect::<Result<Vec<_>, String>>()?;
        RecordBatch::try_new(schema, columns).map_err(|e| format!("a checkpoint's rows: {e}"))
    }
}

/// The schema of a classic checkpoint: every field the protocol gives each action in one.
fn checkpoint_schema() -> Arc<ArrowSchema> {
    let string_map = || {
        let entries = Fields::from(vec![
            Field::new("key", Arrow::Utf8, false),
            Field::new("value", Arrow::Utf8, true),
        ]);
        Arrow::Map(Arc::new(Field::new("key_value", Arrow::Struct(entries), false)), false)
    };
    let strings = || Arrow::List(Arc::new(Field::new("element", Arrow::Utf8, true)));
    let deletion_vector = || {
        Arrow::Struct(Fields::from(vec![
            Field::new("storageType", Arrow::Utf8, true),
            Field::new("pathOrInlineDv", Arrow::Utf8, true),
            Field::new("offset", Arrow::Int32, true),
            Field::new("sizeInBytes", Arrow::Int32, true),
            Field::new("cardinality", Arrow::Int64, true),
        ]))
    };
    let txn = Fields::from(vec![
        Field::new("appId", Arrow::Utf8, true),
        Field::new("version", Arrow::Int64, true),
        Field::new("lastUpdated", Arrow::Int64, true),
    ]);
    let add = Fields::from(vec![
        Field::new("path", Arrow::Utf8, true),
        Field::new("partitionValues", string_map(), true),
        Field::new("size", Arrow::Int64, true),
        Field::new("modificationTime", Arrow::Int64, true),
        Field::new("dataChange", Arrow::Boolean, true),
        Field::new("stats", Arrow::Utf8, true),
        Field::new("tags", string_map(), true),
        Field::new("deletionVector", deletion_vector(), true),
        Field::new("baseRowId", Arrow::Int64, true),
        Field::new("defaultRowCommitVersion", Arrow::Int64, true),
        Field::new("clusteringProvider", Arrow::Utf8, true),
    ]);
    let remove = Fields::from(vec![
        Field::new("path", Arrow::Utf8, true),
        Field::new("deletionTimestamp", Arrow::Int64, true),
        Field::new("dataChange", Arrow::Boolean, true),
        Field::new("extendedFileMetadata", Arrow::Boolean, true),
        Field::new("partitionValues", string_map(), true),
        Field::new("size", Arrow::Int64, true),
        Field::new("deletionVector", deletion_vector(), true),
        Field::new("baseRowId", Arrow::Int64, true),
        Field::new("defaultRowCommitVersion", Arrow::Int64, true),
    ]);
    let metadata = Fields::from(vec![
        Field::new("id", Arrow::Utf8, true),
        Field::new("name", Arrow::Utf8, true),
        Field::new("description", Arrow::Utf8, true),
        Field::new(
            "format",
            Arrow::Struct(Fields::from(vec![
                Field::new("provider", Arrow::Utf8, true),
                Field::new("options", string_map(), true),
            ])),
            true,
        ),
        Field::new("schemaString", Arrow::Utf8, true),
        Field::new("partitionColumns", strings(), true),
        Field::new("configuration", string_map(), true),
        Field::new("createdTime", Arrow::Int64, true),
    ]);
    let protocol = Fields::from(vec![
        Field::new("minReaderVersion", Arrow::Int32, true),
        Field::new("minWriterVersion", Arrow::Int32, true),
        Field::new("readerFeatures", strings(), true),
        Field::new("writerFeatures", strings(), true),
    ]);
    let domain = Fields::from(vec![
        Field::new("domain", Arrow::Utf8, true),
        Field::new("configuration", Arrow::Utf8, true),
        Field::new("removed", Arrow::Boolean, true),
    ]);
    let columns = [txn, add, remove, metadata, protocol, domain];
    Arc::new(ArrowSchema::new(
        (COLUMNS.iter().zip(columns))
            .map(|(name, fields)| Field::new(*name, Arrow::Struct(fields), true))
            .collect::<Vec<_>>(),
    ))
}

/// The file actions of the commits since the last checkpoint.
fn write_since(log: &Log, snapshot: &Snapshot, keep: &Keep, out: &mut Out) -> Result<(), String> {
    let since = &snapshot.since;
    let mut rows = Rows::default();
    let mut at = 0usize;
    let mut take = |f: &FileAction, rows: &mut Rows| -> Result<(), String> {
        let place = at;
        at += 1;
        if keep.keeps(&f.key, f.deleted, Some(place)) {
            rows.push_file(f)?;
            if rows.len() == ROWS_PER_BATCH {
                out.write(&rows.take()?)?;
            }
        }
        Ok(())
    };
    if since.spilled {
        let first = snapshot.checkpoint.as_ref().map_or(0, |c| c.version + 1);
        for version in first..=snapshot.version {
            let found = log.lines(&commit_name(version), |text| match Action::parse(text)? {
                Action::File(f) => take(&f, &mut rows),
                _ => Ok(()),
            })?;
            if !found {
                return Err(format!(
                    "commit {version} is gone, and the checkpoint at {} needs it",
                    snapshot.version
                ));
            }
        }
    } else {
        for f in &since.actions {
            take(f, &mut rows)?;
        }
    }
    out.write(&rows.take()?)
}

/// `batch`'s rows the checkpoint keeps.
fn filtered(batch: RecordBatch, keep: &Keep) -> Result<RecordBatch, String> {
    let add = batch.column_by_name("add").map(|c| c.as_struct());
    let remove = batch.column_by_name("remove").map(|c| c.as_struct());
    let add_keys = add.map(keys).transpose()?;
    let remove_keys = remove.map(keys).transpose()?;
    let deleted = remove.and_then(|r| r.column_by_name("deletionTimestamp"));
    let mut mask = BooleanBuilder::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let kept = if let Some(key) = add_keys.as_ref().and_then(|k| k[row].as_deref()) {
            keep.keeps(key, None, None)
        } else if let Some(key) = remove_keys.as_ref().and_then(|k| k[row].as_deref()) {
            let at = deleted
                .filter(|d| d.is_valid(row))
                .and_then(|d| d.as_primitive_opt::<Int64Type>().map(|d| d.value(row)));
            keep.keeps(key, at, None)
        } else {
            false
        };
        mask.append_value(kept);
    }
    arrow_select::filter::filter_record_batch(&batch, &mask.finish())
        .map_err(|e| format!("filtering a checkpoint: {e}"))
}

/// Each row's file key, from a column of `add` or `remove` structs; None where the row holds none.
fn keys(column: &StructArray) -> Result<Vec<Option<String>>, String> {
    let path = column
        .column_by_name("path")
        .and_then(|p| p.as_string_opt::<i32>())
        .ok_or("a file action column has no string path")?;
    let dv = column.column_by_name("deletionVector").and_then(|d| d.as_struct_opt());
    let dv_string = |name: &str| {
        dv.and_then(|d| d.column_by_name(name))
            .and_then(|c| c.as_string_opt::<i32>())
    };
    let (storage, inline) = (dv_string("storageType"), dv_string("pathOrInlineDv"));
    let offset = dv
        .and_then(|d| d.column_by_name("offset"))
        .and_then(|c| c.as_primitive_opt::<Int32Type>());
    Ok((0..column.len())
        .map(|row| {
            if column.is_null(row) || path.is_null(row) {
                return None;
            }
            let dv = match (dv, storage, inline) {
                (Some(d), Some(s), Some(i)) if d.is_valid(row) => Some(DeletionVector {
                    storage_type: s.value(row).to_string(),
                    path_or_inline_dv: i.value(row).to_string(),
                    offset: offset.filter(|o| o.is_valid(row)).map(|o| o.value(row)),
                }),
                _ => None,
            };
            Some(file_key(path.value(row), dv.as_ref()))
        })
        .collect())
}

/// `values` as one column of type `to`; None and JSON null are null. A key `to` does not name is left out.
fn to_arrow(values: &[Option<&Value>], to: &Arrow) -> Result<ArrayRef, String> {
    let present = |v: &&Value| !v.is_null();
    let wrong = |v: &Value| format!("a file action holds {v} where the protocol has {to}");
    Ok(match to {
        Arrow::Utf8 => Arc::new(
            values
                .iter()
                .map(|v| {
                    v.filter(present)
                        .map(|v| v.as_str().ok_or_else(|| wrong(v)))
                        .transpose()
                })
                .collect::<Result<StringArray, String>>()?,
        ),
        Arrow::Int64 => Arc::new(
            values
                .iter()
                .map(|v| {
                    v.filter(present)
                        .map(|v| v.as_i64().ok_or_else(|| wrong(v)))
                        .transpose()
                })
                .collect::<Result<Int64Array, String>>()?,
        ),
        Arrow::Int32 => Arc::new(
            values
                .iter()
                .map(|v| {
                    v.filter(present)
                        .map(|v| v.as_i64().and_then(|n| i32::try_from(n).ok()).ok_or_else(|| wrong(v)))
                        .transpose()
                })
                .collect::<Result<Int32Array, String>>()?,
        ),
        Arrow::Boolean => Arc::new(
            values
                .iter()
                .map(|v| {
                    v.filter(present)
                        .map(|v| v.as_bool().ok_or_else(|| wrong(v)))
                        .transpose()
                })
                .collect::<Result<BooleanArray, String>>()?,
        ),
        Arrow::Struct(fields) => {
            let objects: Vec<Option<&Map<String, Value>>> = values
                .iter()
                .map(|v| {
                    v.filter(present)
                        .map(|v| v.as_object().ok_or_else(|| wrong(v)))
                        .transpose()
                })
                .collect::<Result<_, String>>()?;
            let children = fields
                .iter()
                .map(|f| {
                    let child: Vec<Option<&Value>> = objects.iter().map(|o| o.and_then(|o| o.get(f.name()))).collect();
                    to_arrow(&child, f.data_type())
                })
                .collect::<Result<Vec<_>, String>>()?;
            let nulls = NullBuffer::from(objects.iter().map(Option::is_some).collect::<Vec<_>>());
            Arc::new(StructArray::new(fields.clone(), children, Some(nulls)))
        }
        Arrow::Map(entry, sorted) => {
            let Arrow::Struct(kv) = entry.data_type() else {
                unreachable!("a map's entries are a struct")
            };
            let mut offsets = vec![0i32];
            let (mut keys, mut items): (Vec<Option<&str>>, Vec<Option<&Value>>) = (Vec::new(), Vec::new());
            let mut valid = Vec::with_capacity(values.len());
            for v in values {
                let object = v
                    .filter(present)
                    .map(|v| v.as_object().ok_or_else(|| wrong(v)))
                    .transpose()?;
                for (k, item) in object.into_iter().flatten() {
                    keys.push(Some(k));
                    items.push(Some(item));
                }
                offsets.push(keys.len() as i32);
                valid.push(object.is_some());
            }
            let entries = StructArray::new(
                kv.clone(),
                vec![Arc::new(StringArray::from(keys)), to_arrow(&items, kv[1].data_type())?],
                None,
            );
            Arc::new(MapArray::new(
                entry.clone(),
                OffsetBuffer::new(offsets.into()),
                entries,
                Some(NullBuffer::from(valid)),
                *sorted,
            ))
        }
        Arrow::List(item) => {
            let mut offsets = vec![0i32];
            let mut items: Vec<Option<&Value>> = Vec::new();
            let mut valid = Vec::with_capacity(values.len());
            for v in values {
                let array = v
                    .filter(present)
                    .map(|v| v.as_array().ok_or_else(|| wrong(v)))
                    .transpose()?;
                items.extend(array.into_iter().flatten().map(Some));
                offsets.push(items.len() as i32);
                valid.push(array.is_some());
            }
            Arc::new(ListArray::new(
                item.clone(),
                OffsetBuffer::new(offsets.into()),
                to_arrow(&items, item.data_type())?,
                Some(NullBuffer::from(valid)),
            ))
        }
        other => unreachable!("the checkpoint schema holds no {other}"),
    })
}

/// Row `row` of `array` as JSON. A null field of a struct is left out.
fn to_json(array: &ArrayRef, row: usize) -> Result<Value, String> {
    if array.is_null(row) {
        return Ok(Value::Null);
    }
    Ok(match array.data_type() {
        Arrow::Utf8 => json!(array.as_string::<i32>().value(row)),
        Arrow::LargeUtf8 => json!(array.as_string::<i64>().value(row)),
        Arrow::Boolean => json!(array.as_boolean().value(row)),
        Arrow::Int8 => json!(array.as_primitive::<arrow_array::types::Int8Type>().value(row)),
        Arrow::Int16 => json!(array.as_primitive::<arrow_array::types::Int16Type>().value(row)),
        Arrow::Int32 => json!(array.as_primitive::<Int32Type>().value(row)),
        Arrow::Int64 => json!(array.as_primitive::<Int64Type>().value(row)),
        Arrow::Struct(_) => {
            let s = array.as_struct();
            let mut object = Map::new();
            for (field, column) in s.fields().iter().zip(s.columns()) {
                if column.is_valid(row) {
                    object.insert(field.name().clone(), to_json(column, row)?);
                }
            }
            Value::Object(object)
        }
        Arrow::Map(..) => {
            let m = array.as_map();
            let entries = m.value(row);
            let (keys, items) = (entries.column(0), entries.column(1));
            let mut object = Map::new();
            for i in 0..entries.len() {
                let Value::String(key) = to_json(keys, i)? else {
                    return Err(format!("a checkpoint's map has a key of type {}", keys.data_type()));
                };
                object.insert(key, to_json(items, i)?);
            }
            Value::Object(object)
        }
        Arrow::List(_) => {
            let item = array.as_list::<i32>().value(row);
            Value::Array((0..item.len()).map(|i| to_json(&item, i)).collect::<Result<_, _>>()?)
        }
        other => {
            return Err(format!(
                "a checkpoint holds a column of type {other}, which this writer does not read"
            ));
        }
    })
}

/// The actions of a Parquet top-level checkpoint, but for its file actions.
pub fn top_level(log: &Log, name: &str, size: u64) -> Result<Vec<Action>, String> {
    const ROOTS: [&str; 6] = [
        "protocol",
        "metaData",
        "txn",
        "domainMetadata",
        "sidecar",
        "checkpointMetadata",
    ];
    let mut actions = Vec::new();
    for_each_batch(log, name, size, &ROOTS, |batch| {
        for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
            for row in (0..batch.num_rows()).filter(|&row| column.is_valid(row)) {
                let mut object = Map::new();
                object.insert(field.name().clone(), to_json(column, row)?);
                actions.push(Action::parse(&Value::Object(object).to_string())?);
            }
        }
        Ok(())
    })?;
    Ok(actions)
}

/// Calls `each` with every row group of the Parquet file `name`, `size` bytes long, as one batch of its root columns
/// named in `roots`. Only the footer and those columns are fetched, a row group at a time.
fn for_each_batch(
    log: &Log,
    name: &str,
    size: u64,
    roots: &[&str],
    mut each: impl FnMut(RecordBatch) -> Result<(), String>,
) -> Result<(), String> {
    let bad = |e: ParquetError| format!("_delta_log/{name} is no Parquet file this writer reads: {e}");
    let metadata = Arc::new(footer(log, name, size)?);
    let options = ArrowReaderOptions::new();
    let reader_metadata = ArrowReaderMetadata::try_new(metadata.clone(), options).map_err(bad)?;
    let descr = metadata.file_metadata().schema_descr();
    let fields = descr.root_schema().get_fields();
    let picked: Vec<usize> = (0..fields.len())
        .filter(|&i| roots.contains(&fields[i].name()))
        .collect();
    if picked.is_empty() {
        return Ok(());
    }
    for (i, group) in metadata.row_groups().iter().enumerate() {
        if group.num_rows() == 0 {
            continue;
        }
        let ranges: Vec<std::ops::Range<u64>> = (0..group.num_columns())
            .filter(|&c| picked.contains(&descr.get_column_root_idx(c)))
            .map(|c| {
                let (start, length) = group.column(c).byte_range();
                start..start + length
            })
            .collect();
        let pieces = log.ranges(name, &ranges)?;
        let fetched = Fetched {
            len: size,
            pieces: ranges.iter().map(|r| r.start).zip(pieces).collect(),
        };
        let reader = ParquetRecordBatchReaderBuilder::new_with_metadata(fetched, reader_metadata.clone())
            .with_row_groups(vec![i])
            .with_projection(ProjectionMask::roots(descr, picked.iter().copied()))
            .with_batch_size(group.num_rows() as usize)
            .build()
            .map_err(bad)?;
        for batch in reader {
            each(batch.map_err(|e| format!("reading _delta_log/{name}: {e}"))?)?;
        }
    }
    Ok(())
}

/// The footer of the Parquet file `name`, `size` bytes long.
fn footer(log: &Log, name: &str, size: u64) -> Result<ParquetMetaData, String> {
    let bad = |e: ParquetError| format!("_delta_log/{name} is no Parquet file this writer reads: {e}");
    let mut tail = FOOTER_GUESS.min(size);
    loop {
        let start = size - tail;
        let fetched = Fetched {
            len: size,
            pieces: vec![(start, log.range(name, start..size)?)],
        };
        match ParquetMetaDataReader::new().parse_and_finish(&fetched) {
            Ok(metadata) => return Ok(metadata),
            Err(ParquetError::NeedMoreData(needed)) if (needed as u64) > tail && (needed as u64) <= size => {
                tail = needed as u64;
            }
            Err(e) => return Err(bad(e)),
        }
    }
}

/// Parts of a file fetched from the store, read as the whole file is.
struct Fetched {
    len: u64,
    /// Each part's offset in the file, and its bytes.
    pieces: Vec<(u64, Bytes)>,
}

impl Fetched {
    fn slice(&self, start: u64, length: Option<usize>) -> parquet::errors::Result<Bytes> {
        for (offset, bytes) in &self.pieces {
            let end = offset + bytes.len() as u64;
            if (*offset..end).contains(&start) {
                let from = (start - offset) as usize;
                let to = length.map_or(bytes.len(), |l| from + l);
                if to <= bytes.len() {
                    return Ok(bytes.slice(from..to));
                }
            }
        }
        // The footer reader asks for more than was fetched to learn how much it needs.
        Err(ParquetError::NeedMoreData((self.len - start) as usize))
    }
}

impl Length for Fetched {
    fn len(&self) -> u64 {
        self.len
    }
}

impl ChunkReader for Fetched {
    type T = tokio_util::bytes::buf::Reader<Bytes>;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        Ok(self.slice(start, None)?.reader())
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<Bytes> {
        self.slice(start, Some(length))
    }
}

/// The table property `key`, a duration such as `interval 7 days 12 hours`, in milliseconds. None when it is not set;
/// Err when it is set to something not read here, which the caller takes as the longest retention.
fn retention_ms(metadata: &Metadata, key: &str) -> Result<Option<i64>, String> {
    let Some(text) = metadata.property(key) else {
        return Ok(None);
    };
    let unread = || format!("the table's {key} is {text}, not a duration of days, hours, minutes or seconds");
    let lower = text.trim().to_ascii_lowercase();
    let words: Vec<&str> = lower
        .strip_prefix("interval")
        .unwrap_or(&lower)
        .split_whitespace()
        .collect();
    if words.is_empty() || !words.len().is_multiple_of(2) {
        return Err(unread());
    }
    let mut total: i64 = 0;
    for pair in words.chunks(2) {
        let count: i64 = pair[0].parse().map_err(|_| unread())?;
        // Months and years have no fixed length, and Delta refuses them here too.
        let unit_ms = match pair[1].trim_end_matches('s') {
            "millisecond" => 1,
            "second" => 1000,
            "minute" => 60 * 1000,
            "hour" => 60 * 60 * 1000,
            "day" => DAY_MS,
            "week" => 7 * DAY_MS,
            _ => return Err(unread()),
        };
        total = count
            .checked_mul(unit_ms)
            .and_then(|ms| total.checked_add(ms))
            .ok_or_else(unread)?;
    }
    Ok(Some(total))
}

/// Deletes the log entries older than the table's log retention, as the protocol says: every entry before the newest
/// checkpoint so old. `newest` is the checkpoint just written. A V2 checkpoint's sidecars are left to the writer that
/// wrote them.
fn clean(log: &Log, metadata: &Metadata, newest: &Checkpoint, now: i64) -> Result<(), String> {
    if metadata.flag("delta.enableExpiredLogCleanup") == Some(false) {
        return Ok(());
    }
    let cutoff_time = now.saturating_sub(retention_ms(metadata, "delta.logRetentionDuration")?.unwrap_or(30 * DAY_MS));
    // Entries are listed in version order, close to their order in time: the listing stops at the first one kept, and
    // an older one after it is left to a later cleanup.
    let mut old: Vec<Entry> = Vec::new();
    log.list(None, |entry| {
        if covered(&entry.name).is_none() || entry.modified >= cutoff_time {
            // `_last_checkpoint` and `_sidecars/` sort after every version.
            return false;
        }
        old.push(entry);
        true
    })?;
    let cutoff = old.iter().rev().find_map(|e| match Name::of(&e.name) {
        Name::Checkpoint(v) if v <= newest.version => Some(v),
        _ => None,
    });
    let Some(cutoff) = cutoff else {
        return Ok(());
    };
    for entry in &old {
        if covered(&entry.name).is_some_and(|last| last < cutoff) {
            log.delete(&entry.name)?;
        }
    }
    Ok(())
}

/// A sidecar action's path as its name under `_sidecars/`: the protocol encourages the bare name, and allows more.
fn sidecar_name(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}

/// The last version an entry of the log holds: a commit's, a checkpoint's or a checksum's own, and a compacted
/// commit's last. None for an entry of no version.
fn covered(name: &str) -> Option<i64> {
    let (first, rest) = name.split_once('.')?;
    if first.len() != 20 || !first.bytes().all(|b| b.is_ascii_digit()) || name.contains('/') {
        return None;
    }
    let first: i64 = first.parse().ok()?;
    match rest.split_once('.') {
        Some((last, "compacted.json")) if last.len() == 20 => last.parse().ok(),
        _ => Some(first),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fmt;
    use std::ops::Range;
    use std::sync::Mutex;
    use std::time::Duration;

    use arrow_array::builder::{ListBuilder, StringBuilder};
    use async_trait::async_trait;
    use futures_util::StreamExt;
    use futures_util::stream::BoxStream;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions,
        PutOptions, PutPayload, PutResult,
    };

    use super::*;
    use crate::io::objects::runtime;

    const ME: &str = "me";

    /// An in-memory store that lists each object in `aged` that many days older than it is, as no test can wait for.
    #[derive(Debug, Default)]
    struct Aged {
        inner: InMemory,
        aged: Mutex<HashMap<String, u64>>,
    }

    impl fmt::Display for Aged {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "Aged({})", self.inner)
        }
    }

    #[async_trait]
    impl ObjectStore for Aged {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(&self, location: &Path, options: GetOptions) -> object_store::Result<GetResult> {
            self.inner.get_opts(location, options).await
        }

        async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> object_store::Result<Vec<Bytes>> {
            self.inner.get_ranges(location, ranges).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            let aged = self.aged.lock().unwrap().clone();
            self.inner
                .list(prefix)
                .map(move |meta| {
                    meta.map(|mut meta| {
                        if let Some(days) = aged.get(meta.location.as_ref()) {
                            meta.last_modified -= Duration::from_secs(days * 24 * 60 * 60);
                        }
                        meta
                    })
                })
                .boxed()
        }

        async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    fn table() -> (Arc<Aged>, Log) {
        let store = Arc::new(Aged::default());
        let log = Log::new(store.clone(), runtime().unwrap(), Duration::from_secs(5), "t");
        (store, log)
    }

    fn metadata_json(config: &[(&str, &str)]) -> Value {
        let configuration: BTreeMap<&str, &str> = config.iter().copied().collect();
        json!({
            "id": "table-id",
            "format": {"provider": "parquet", "options": {}},
            "schemaString": "{\"type\":\"struct\",\"fields\":[]}",
            "partitionColumns": [],
            "createdTime": 1,
            "configuration": configuration,
        })
    }

    fn metadata(config: &[(&str, &str)]) -> Metadata {
        serde_json::from_value(metadata_json(config)).unwrap()
    }

    fn protocol() -> Value {
        json!({"protocol": {
            "minReaderVersion": 3,
            "minWriterVersion": 7,
            "readerFeatures": ["v2Checkpoint"],
            "writerFeatures": ["v2Checkpoint", "domainMetadata"],
        }})
    }

    fn add(path: &str) -> Value {
        json!({"add": {"path": path, "partitionValues": {}, "size": 10, "modificationTime": 1, "dataChange": true}})
    }

    fn remove(path: &str, deleted: i64) -> Value {
        json!({"remove": {"path": path, "deletionTimestamp": deleted, "dataChange": true}})
    }

    fn txn(app: &str, version: i64) -> Value {
        json!({"txn": {"appId": app, "version": version, "lastUpdated": now_ms()}})
    }

    fn commit(log: &Log, version: i64, lines: &[Value]) {
        let body: Vec<String> = lines.iter().map(Value::to_string).collect();
        assert!(log.create(&commit_name(version), body.join("\n").into_bytes()).unwrap());
    }

    /// Commit 0 of a table with `config`, by this writer, adding `paths`.
    fn create(log: &Log, config: &[(&str, &str)], paths: &[&str]) {
        let mut lines = vec![protocol(), json!({"metaData": metadata_json(config)}), txn(ME, 0)];
        lines.extend(paths.iter().map(|p| add(p)));
        commit(log, 0, &lines);
    }

    /// This writer's commit `version`, adding `paths`.
    fn ours(log: &Log, version: i64, paths: &[&str]) {
        let mut lines = vec![txn(ME, version)];
        lines.extend(paths.iter().map(|p| add(p)));
        commit(log, version, &lines);
    }

    fn load(log: &Log) -> Snapshot {
        Snapshot::load(log, ME).unwrap().unwrap()
    }

    /// Each row of a Parquet file as the action it holds, `{"add": ...}` or `{"remove": ...}`.
    fn parquet_rows(bytes: Bytes) -> Vec<Value> {
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .unwrap()
            .build()
            .unwrap();
        let mut rows = Vec::new();
        for batch in reader {
            let batch = batch.unwrap();
            for row in 0..batch.num_rows() {
                for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
                    if column.is_valid(row) {
                        let mut object = Map::new();
                        object.insert(field.name().clone(), to_json(column, row).unwrap());
                        rows.push(Value::Object(object));
                    }
                }
            }
        }
        rows
    }

    /// Each row of the checkpoint's file, as the action it holds.
    fn file_rows(log: &Log, checkpoint: &Checkpoint) -> Vec<Value> {
        parquet_rows(log.get(&checkpoint.files[0].name).unwrap().unwrap())
    }

    /// The checkpoint's file actions as `add <path>` or `remove <path>`, in order.
    fn named(log: &Log, checkpoint: &Checkpoint) -> Vec<String> {
        file_rows(log, checkpoint)
            .iter()
            .filter_map(|row| {
                let (kind, action) = row.as_object().unwrap().iter().next().unwrap();
                (kind == "add" || kind == "remove").then(|| format!("{kind} {}", action["path"].as_str().unwrap()))
            })
            .collect()
    }

    /// A Parquet file of `lines`' actions in the checkpoint's schema, put at `name`.
    fn put_parquet(log: &Log, name: &str, lines: &[Value]) -> Entry {
        let mut rows = Rows::default();
        for line in lines {
            let (kind, action) = line.as_object().unwrap().iter().next().unwrap();
            rows.push(kind, action.clone());
        }
        let batch = rows.take().unwrap();
        let mut writer = ArrowWriter::try_new(Vec::new(), batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        let bytes = writer.into_inner().unwrap();
        let size = bytes.len() as u64;
        log.put(name, bytes).unwrap();
        Entry {
            name: name.to_string(),
            size,
            modified: 0,
        }
    }

    fn listing(log: &Log) -> Vec<String> {
        let mut names = Vec::new();
        log.list(None, |e| {
            names.push(e.name);
            true
        })
        .unwrap();
        names
    }

    #[test]
    fn every_tenth_version_is_checkpointed_unless_the_table_sets_its_interval_and_version_0_never_is() {
        let default = metadata(&[]);
        assert!(!due(&default, 0));
        assert!(!due(&default, 9));
        assert!(due(&default, 10));
        assert!(due(&default, 20));
        let three = metadata(&[("delta.checkpointInterval", "3")]);
        assert!(!due(&three, 0));
        assert!(!due(&three, 2));
        assert!(due(&three, 3));
        assert!(!due(&three, 10));
        let every = metadata(&[("delta.checkpointInterval", "1")]);
        assert!(!due(&every, 0));
        assert!(due(&every, 1));
        let nonsense = metadata(&[("delta.checkpointInterval", "0")]);
        assert!(due(&nonsense, 10));
        assert!(!due(&nonsense, 3));
    }

    #[test]
    fn a_checkpoint_holds_the_table_state_then_every_file_action_in_one_parquet_file() {
        let (_, log) = table();
        let mut first = vec![
            protocol(),
            json!({"metaData": metadata_json(&[])}),
            txn(ME, 0),
            json!({"domainMetadata": {"domain": "kept", "configuration": "{}", "removed": false}}),
            json!({"domainMetadata": {"domain": "gone", "configuration": "{}", "removed": false}}),
        ];
        first.push(add("a0"));
        commit(&log, 0, &first);
        commit(
            &log,
            1,
            &[
                txn(ME, 1),
                add("a1"),
                json!({"domainMetadata": {"domain": "gone", "configuration": "{}", "removed": true}}),
            ],
        );
        ours(&log, 2, &["a2"]);
        commit(&log, 3, &[txn("other", 5)]);
        let snapshot = load(&log);

        let checkpoint = write(&log, &snapshot).unwrap();

        assert_eq!(checkpoint.version, 3);
        assert_eq!(checkpoint.files[0].name, "00000000000000000003.checkpoint.parquet");
        assert!(checkpoint.sidecars.is_empty());
        let rows = file_rows(&log, &checkpoint);
        let kinds: Vec<&str> = rows
            .iter()
            .map(|l| l.as_object().unwrap().keys().next().unwrap().as_str())
            .collect();
        assert_eq!(
            kinds,
            [
                "protocol",
                "metaData",
                "txn",
                "txn",
                "domainMetadata",
                "add",
                "add",
                "add"
            ]
        );
        assert_eq!(rows[0], protocol());
        assert_eq!(rows[1]["metaData"]["id"], "table-id");
        assert_eq!(rows[5], add("a0"));
        assert_eq!(named(&log, &checkpoint), ["add a0", "add a1", "add a2"]);
        let last: Value = serde_json::from_slice(&log.get("_last_checkpoint").unwrap().unwrap()).unwrap();
        assert_eq!(
            last,
            json!({"version": 3, "size": 8, "sizeInBytes": checkpoint.files[0].size})
        );

        let reloaded = load(&log);
        assert_eq!(reloaded.version, 3);
        assert_eq!(
            reloaded.checkpoint.as_ref().unwrap().files[0].name,
            checkpoint.files[0].name
        );
        assert_eq!(reloaded.protocol, snapshot.protocol);
        assert_eq!(reloaded.metadata, snapshot.metadata);
        assert_eq!(reloaded.txns, snapshot.txns);
        assert_eq!(reloaded.domains, snapshot.domains);
        assert_eq!(reloaded.since.count, 0);
    }

    #[test]
    fn a_checkpoint_larger_than_a_part_is_uploaded_in_parts_and_reads_back_whole() {
        let (_, log) = table();
        // Random names, which compression cannot shrink below a part.
        let paths: Vec<String> = (0..20_000)
            .map(|_| (0..14).map(|_| super::super::log::uuid()).collect())
            .collect();
        let refs: Vec<&str> = paths.iter().map(String::as_str).collect();
        create(&log, &[], &refs);

        let checkpoint = write(&log, &load(&log)).unwrap();

        assert!(
            checkpoint.files[0].size > 8 * 1024 * 1024,
            "{}",
            checkpoint.files[0].size
        );
        assert_eq!(named(&log, &checkpoint).len(), paths.len());
    }

    #[test]
    fn the_next_checkpoint_carries_the_last_ones_files_then_the_commits_since() {
        let (_, log) = table();
        create(&log, &[], &["a0"]);
        ours(&log, 1, &["a1"]);
        write(&log, &load(&log)).unwrap();
        ours(&log, 2, &["a2"]);
        ours(&log, 3, &["a3", "b3"]);

        let second = write(&log, &load(&log)).unwrap();

        assert_eq!(named(&log, &second), ["add a0", "add a1", "add a2", "add a3", "add b3"]);
    }

    #[test]
    fn a_file_another_writer_removes_leaves_only_its_remove_and_that_until_it_expires() {
        let (_, log) = table();
        create(
            &log,
            &[("delta.deletedFileRetentionDuration", "interval 1 days")],
            &["a0", "b0"],
        );
        ours(&log, 1, &["a1"]);
        write(&log, &load(&log)).unwrap();
        let now = now_ms();
        commit(&log, 2, &[remove("a0", now), remove("b0", now - 2 * DAY_MS)]);
        ours(&log, 3, &["a3"]);

        let checkpoint = write(&log, &load(&log)).unwrap();

        assert_eq!(named(&log, &checkpoint), ["add a1", "remove a0", "add a3"]);
        assert!(file_rows(&log, &checkpoint).contains(&remove("a0", now)));
    }

    #[test]
    fn a_file_added_and_removed_since_the_last_checkpoint_leaves_only_its_remove() {
        let (_, log) = table();
        create(&log, &[], &["a0", "x"]);
        let now = now_ms();
        commit(&log, 1, &[remove("x", now)]);
        commit(&log, 2, &[add("y"), remove("y", now)]);

        let checkpoint = write(&log, &load(&log)).unwrap();

        assert_eq!(named(&log, &checkpoint), ["add a0", "remove x", "remove y"]);
    }

    #[test]
    fn commits_read_again_when_too_many_to_hold_give_the_checkpoint_the_held_actions_would() {
        let (_, log) = table();
        create(&log, &[], &["a0", "b0"]);
        ours(&log, 1, &["a1"]);
        write(&log, &load(&log)).unwrap();
        commit(&log, 2, &[remove("a0", now_ms()), add("c2")]);
        ours(&log, 3, &["a3"]);
        commit(&log, 4, &[remove("c2", now_ms())]);
        let held = load(&log);
        assert!(!held.since.spilled);
        let mut spilled = held.clone();
        spilled.since.spilled = true;
        spilled.since.actions.clear();

        let from_held = write(&log, &held).unwrap();
        let held_rows = file_rows(&log, &from_held);
        let from_commits = write(&log, &spilled).unwrap();

        assert_eq!(file_rows(&log, &from_commits), held_rows);
        assert_eq!(
            named(&log, &from_held),
            ["add b0", "add a1", "remove a0", "add a3", "remove c2"]
        );
    }

    #[test]
    fn a_v2_checkpoint_has_its_inline_actions_and_sidecars_carried_into_a_classic_one() {
        let (_, log) = table();
        let now = now_ms();
        // A sidecar another writer wrote, with a column this writer does not know.
        let sidecar = put_parquet(&log, &format!("{SIDECARS}/s.parquet"), &[add("s0"), add("s1")]);
        let lines = [
            json!({"checkpointMetadata": {"version": 2}}),
            protocol(),
            json!({"metaData": metadata_json(&[])}),
            txn(ME, 2),
            add("a0"),
            remove("r0", now),
            remove("r1", now - 30 * DAY_MS),
            json!({"sidecar": {"path": "s.parquet", "sizeInBytes": sidecar.size, "modificationTime": 1}}),
        ];
        let body: Vec<String> = lines.iter().map(Value::to_string).collect();
        log.put(
            "00000000000000000002.checkpoint.80a083e8-7026-4e79-81be-64bd76c43a11.json",
            body.join("\n").into_bytes(),
        )
        .unwrap();
        commit(&log, 3, &[remove("s1", now)]);
        ours(&log, 4, &["a4"]);
        let snapshot = load(&log);
        assert_eq!(snapshot.checkpoint.as_ref().unwrap().sidecars.len(), 1);

        let checkpoint = write(&log, &snapshot).unwrap();

        assert_eq!(
            named(&log, &checkpoint),
            ["add a0", "remove r0", "add s0", "remove s1", "add a4"]
        );
    }

    #[test]
    fn a_checkpoints_add_of_another_shape_is_carried_by_its_fields_names() {
        let (_, log) = table();
        let add_type = Arrow::Struct(Fields::from(vec![
            Field::new("size", Arrow::Int32, true),
            Field::new("path", Arrow::Utf8, true),
            Field::new(
                "stats_parsed",
                Arrow::Struct(Fields::from(vec![Field::new("numRecords", Arrow::Int64, true)])),
                true,
            ),
        ]));
        let adds = to_arrow(
            &[Some(
                &json!({"size": 3, "path": "p0", "stats_parsed": {"numRecords": 2}}),
            )],
            &add_type,
        )
        .unwrap();
        let theirs = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![Field::new("add", add_type, true)])),
            vec![adds],
        )
        .unwrap();
        let mut out = Out::new(&log, "x.parquet").unwrap();

        out.write_files(theirs).unwrap();
        out.finish().unwrap();

        let rows = parquet_rows(log.get("x.parquet").unwrap().unwrap());
        assert_eq!(rows, [json!({"add": {"size": 3, "path": "p0"}})]);
    }

    #[test]
    fn a_checkpoint_in_parts_is_read_whole_and_one_missing_a_part_is_passed_over() {
        let (_, log) = table();
        create(&log, &[], &[]);
        for version in 1..=4 {
            ours(&log, version, &[&format!("a{version}")]);
        }
        put_parquet(
            &log,
            "00000000000000000002.checkpoint.0000000001.0000000002.parquet",
            &[protocol(), add("p0")],
        );
        put_parquet(
            &log,
            "00000000000000000002.checkpoint.0000000002.0000000002.parquet",
            &[json!({"metaData": metadata_json(&[])}), txn(ME, 2), add("p1")],
        );
        // Part 1 of 2 at version 3 is all there is of that one.
        put_parquet(
            &log,
            "00000000000000000003.checkpoint.0000000001.0000000002.parquet",
            &[protocol()],
        );
        let snapshot = load(&log);
        let last = snapshot.checkpoint.as_ref().unwrap();
        assert_eq!((last.version, last.files.len()), (2, 2));
        assert_eq!(snapshot.version, 4);
        assert_eq!(snapshot.our_version(), Some(4));

        let checkpoint = write(&log, &snapshot).unwrap();

        assert_eq!(named(&log, &checkpoint), ["add p0", "add p1", "add a3", "add a4"]);
    }

    #[test]
    fn a_later_checkpoint_keeps_its_place_in_last_checkpoint() {
        let (_, log) = table();
        create(&log, &[], &["a0"]);
        ours(&log, 1, &["a1"]);
        let early = load(&log);
        ours(&log, 2, &["a2"]);
        write(&log, &load(&log)).unwrap();

        write(&log, &early).unwrap();

        let last: Value = serde_json::from_slice(&log.get("_last_checkpoint").unwrap().unwrap()).unwrap();
        assert_eq!(last["version"], 2);
    }

    #[test]
    fn a_snapshot_rebases_on_another_writers_newer_checkpoint_and_not_past_its_own_version() {
        let (_, log) = table();
        create(&log, &[], &["a0"]);
        let mut mine = load(&log);
        ours(&log, 1, &["a1"]);
        let theirs = write(&log, &load(&log)).unwrap();
        ours(&log, 2, &["a2"]);
        mine.advance(&log).unwrap();

        assert!(mine.rebase(&log).unwrap());
        assert_eq!(mine.checkpoint.as_ref().unwrap().files[0].name, theirs.files[0].name);
        assert_eq!((mine.version, mine.since.count), (2, 1));
        let mut same = mine.clone();
        write(&log, &mine).unwrap();
        assert!(!same.rebase(&log).unwrap());
    }

    #[test]
    fn a_file_action_of_the_wrong_type_is_refused() {
        let value = json!({"path": 5});
        let schema = checkpoint_schema();
        assert!(to_arrow(&[Some(&value)], schema.field_with_name("add").unwrap().data_type()).is_err());
    }

    fn string_map() -> Arrow {
        let entries = Fields::from(vec![
            Field::new("key", Arrow::Utf8, false),
            Field::new("value", Arrow::Utf8, true),
        ]);
        Arrow::Map(Arc::new(Field::new("key_value", Arrow::Struct(entries), false)), false)
    }

    #[test]
    fn a_parquet_top_level_checkpoint_gives_its_actions() {
        let (_, log) = table();
        let rows = 5;
        let only = |at: usize| (0..rows).map(|r| r == at).collect::<Vec<bool>>();
        let mut features = ListBuilder::new(StringBuilder::new());
        features.values().append_value("v2Checkpoint");
        features.append(true);
        for _ in 1..rows {
            features.append_null();
        }
        let features = features.finish();
        let protocol_fields = Fields::from(vec![
            Field::new("minReaderVersion", Arrow::Int32, true),
            Field::new("minWriterVersion", Arrow::Int32, true),
            Field::new("writerFeatures", features.data_type().clone(), true),
        ]);
        let protocol = StructArray::new(
            protocol_fields.clone(),
            vec![
                Arc::new(Int32Array::from(vec![Some(3), None, None, None, None])),
                Arc::new(Int32Array::from(vec![Some(7), None, None, None, None])),
                Arc::new(features),
            ],
            Some(NullBuffer::from(only(0))),
        );
        let metadata_type = Arrow::Struct(Fields::from(vec![
            Field::new("id", Arrow::Utf8, true),
            Field::new(
                "format",
                Arrow::Struct(Fields::from(vec![
                    Field::new("provider", Arrow::Utf8, true),
                    Field::new("options", string_map(), true),
                ])),
                true,
            ),
            Field::new("schemaString", Arrow::Utf8, true),
            Field::new("createdTime", Arrow::Int64, true),
            Field::new("configuration", string_map(), true),
        ]));
        let txn_type = Arrow::Struct(Fields::from(vec![
            Field::new("appId", Arrow::Utf8, true),
            Field::new("version", Arrow::Int64, true),
            Field::new("lastUpdated", Arrow::Int64, true),
        ]));
        let sidecar_type = Arrow::Struct(Fields::from(vec![
            Field::new("path", Arrow::Utf8, true),
            Field::new("sizeInBytes", Arrow::Int64, true),
            Field::new("modificationTime", Arrow::Int64, true),
            Field::new("tags", string_map(), true),
        ]));
        let checkpoint_type = Arrow::Struct(Fields::from(vec![Field::new("version", Arrow::Int64, true)]));
        let column = |at: usize, value: Value, to: &Arrow| {
            let values: Vec<Option<&Value>> = (0..rows).map(|r| (r == at).then_some(&value)).collect();
            to_arrow(&values, to).unwrap()
        };
        let meta = metadata_json(&[("delta.checkpointInterval", "5")]);
        let sidecar =
            json!({"path": "s.parquet", "sizeInBytes": 100, "modificationTime": 7, "tags": {"numActions": "3"}});
        let columns: Vec<ArrayRef> = vec![
            Arc::new(protocol),
            column(1, meta, &metadata_type),
            column(2, json!({"appId": ME, "version": 3, "lastUpdated": 11}), &txn_type),
            column(3, sidecar.clone(), &sidecar_type),
            column(4, json!({"version": 3}), &checkpoint_type),
        ];
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("protocol", Arrow::Struct(protocol_fields), true),
            Field::new("metaData", metadata_type, true),
            Field::new("txn", txn_type, true),
            Field::new("sidecar", sidecar_type, true),
            Field::new("checkpointMetadata", checkpoint_type, true),
        ]));
        let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
        let mut writer = ArrowWriter::try_new(Vec::new(), schema, None).unwrap();
        writer.write(&batch).unwrap();
        let bytes = writer.into_inner().unwrap();
        let size = bytes.len() as u64;
        let name = format!("{:020}.checkpoint.{}.parquet", 3, super::super::log::uuid());
        log.put(&name, bytes).unwrap();

        let actions = top_level(&log, &name, size).unwrap();

        assert_eq!(actions.len(), 5);
        let Action::Protocol(p) = &actions[0] else {
            panic!("{:?} is no protocol", actions[0])
        };
        assert_eq!((p.min_reader_version, p.min_writer_version), (3, 7));
        assert_eq!(p.writer_features, Some(vec!["v2Checkpoint".to_string()]));
        let Action::Metadata(m) = &actions[1] else {
            panic!("{:?} is no metaData", actions[1])
        };
        assert_eq!(**m, metadata(&[("delta.checkpointInterval", "5")]));
        let Action::Txn(t) = &actions[2] else {
            panic!("{:?} is no txn", actions[2])
        };
        assert_eq!((t.app_id.as_str(), t.version, t.last_updated), (ME, 3, Some(11)));
        let Action::Sidecar(s) = &actions[3] else {
            panic!("{:?} is no sidecar", actions[3])
        };
        assert_eq!(serde_json::to_value(s).unwrap(), sidecar);
        assert!(matches!(&actions[4], Action::CheckpointMetadata(c) if c.version == 3));

        let snapshot = load(&log);
        assert_eq!(snapshot.version, 3);
        assert_eq!(snapshot.txns[ME].version, 3);
        let checkpoint = snapshot.checkpoint.unwrap();
        assert_eq!(checkpoint.sidecars, vec![s.clone()]);
        assert_eq!(checkpoint.files.len(), 1);
    }

    #[test]
    fn file_actions_come_back_from_a_checkpoint_as_the_log_had_them() {
        let lines = [
            json!({"add": {
                "path": "day=2026-10-11/part-0.parquet",
                "partitionValues": {"day": "2026-10-11", "host": null},
                "size": 1234,
                "modificationTime": 1_700_000_000_000i64,
                "dataChange": true,
                "deletionVector": {
                    "storageType": "u",
                    "pathOrInlineDv": "ab^-aqEH.-t@S}K{vb[*k^",
                    "offset": 4,
                    "sizeInBytes": 40,
                    "cardinality": 6,
                },
                "baseRowId": 10,
            }}),
            json!({"remove": {
                "path": "day=2026-10-10/part-9.parquet",
                "deletionTimestamp": 1_700_000_000_001i64,
                "dataChange": false,
                "partitionValues": {"day": "2026-10-10"},
                "size": 99,
            }}),
            json!({"add": {"path": "bare.parquet", "partitionValues": {}, "size": 1, "modificationTime": 2, "dataChange": true, "tags": {"k": "v"}}}),
        ];
        let mut rows = Rows::default();
        for l in &lines {
            let Action::File(f) = Action::parse(&l.to_string()).unwrap() else {
                panic!("{l} is no file action")
            };
            rows.push_file(&f).unwrap();
        }
        let batch = rows.take().unwrap();
        for (row, l) in lines.iter().enumerate() {
            let (kind, action) = l.as_object().unwrap().iter().next().unwrap();
            assert_eq!(&to_json(batch.column_by_name(kind).unwrap(), row).unwrap(), action);
        }
        let mut writer = ArrowWriter::try_new(Vec::new(), batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        let bytes = Bytes::from(writer.into_inner().unwrap());

        assert_eq!(parquet_rows(bytes), lines);
        let mut rows = Rows::default();
        rows.push("protocol", protocol()["protocol"].clone());
        assert_eq!(
            to_json(rows.take().unwrap().column_by_name("protocol").unwrap(), 0).unwrap(),
            protocol()["protocol"]
        );
    }

    #[test]
    fn a_retention_is_read_as_an_interval_of_a_count_and_a_unit() {
        let read = |text: &str| retention_ms(&metadata(&[("k", text)]), "k");
        assert_eq!(read("interval 7 days"), Ok(Some(7 * DAY_MS)));
        assert_eq!(read("1 week"), Ok(Some(7 * DAY_MS)));
        assert_eq!(read("interval 2 hours"), Ok(Some(2 * 60 * 60 * 1000)));
        assert_eq!(read("INTERVAL 1 DAY"), Ok(Some(DAY_MS)));
        assert_eq!(read("interval 0 days"), Ok(Some(0)));
        assert_eq!(read("30 seconds"), Ok(Some(30_000)));
        assert_eq!(
            read("interval 365 days 1 hour"),
            Ok(Some(365 * DAY_MS + 60 * 60 * 1000))
        );
        assert!(read("garbage").is_err());
        assert!(read("interval").is_err());
        assert!(read("interval seven days").is_err());
        assert!(read("interval 2 fortnights").is_err());
        assert!(read("interval 1 month").is_err());
        assert!(read("interval 1 day 2").is_err());
        assert_eq!(retention_ms(&metadata(&[]), "k"), Ok(None));
    }

    /// A table of this writer's commits 0 to 6 with checkpoints at 2, 4 and 6, each after its commits have aged.
    fn three_checkpoints(log: &Log, config: &[(&str, &str)]) -> Vec<Checkpoint> {
        create(log, config, &["a0"]);
        let mut checkpoints = Vec::new();
        for version in 1..=6 {
            ours(log, version, &[&format!("a{version}")]);
            if version % 2 == 0 {
                // The entries before a checkpoint must be older than the time it is written at.
                std::thread::sleep(Duration::from_millis(5));
                checkpoints.push(write(log, &load(log)).unwrap());
            }
        }
        checkpoints
    }

    #[test]
    fn log_entries_before_the_newest_expired_checkpoint_are_deleted_and_the_table_still_loads() {
        let (_, log) = table();
        let checkpoints = three_checkpoints(&log, &[("delta.logRetentionDuration", "interval 0 days")]);

        let names = listing(&log);
        let versions: Vec<&str> = names
            .iter()
            .filter(|n| !n.starts_with('_'))
            .map(|n| n.as_str())
            .collect();
        assert_eq!(
            versions,
            [
                checkpoints[1].files[0].name.as_str(),
                &commit_name(4),
                &commit_name(5),
                checkpoints[2].files[0].name.as_str(),
                &commit_name(6),
            ]
        );
        let snapshot = load(&log);
        assert_eq!(snapshot.version, 6);
        assert_eq!(snapshot.checkpoint.unwrap().files[0].name, checkpoints[2].files[0].name);
    }

    #[test]
    fn a_table_that_turns_expired_log_cleanup_off_keeps_its_whole_log() {
        let (_, log) = table();
        let checkpoints = three_checkpoints(
            &log,
            &[
                ("delta.logRetentionDuration", "interval 0 days"),
                ("delta.enableExpiredLogCleanup", "false"),
            ],
        );

        let names = listing(&log);
        for version in 0..=6 {
            assert!(names.contains(&commit_name(version)), "commit {version} was deleted");
        }
        for checkpoint in &checkpoints {
            assert!(names.contains(&checkpoint.files[0].name));
        }
    }

    #[test]
    fn an_entry_covers_the_last_version_it_holds() {
        assert_eq!(covered("00000000000000000003.json"), Some(3));
        assert_eq!(covered("00000000000000000003.checkpoint.parquet"), Some(3));
        assert_eq!(
            covered("00000000000000000003.checkpoint.80a083e8-7026-4e79-81be-64bd76c43a11.json"),
            Some(3)
        );
        assert_eq!(
            covered("00000000000000000003.checkpoint.0000000001.0000000002.parquet"),
            Some(3)
        );
        assert_eq!(covered("00000000000000000003.crc"), Some(3));
        assert_eq!(
            covered("00000000000000000003.00000000000000000007.compacted.json"),
            Some(7)
        );
        assert_eq!(covered("_last_checkpoint"), None);
        assert_eq!(covered("_sidecars/00000000000000000003.json"), None);
        assert_eq!(covered("3.json"), None);
    }
}
