//! A seeded run of the Delta committer against an in-memory bucket that fails as armed.
//!
//! Each seed makes a record, a partitioning and maybe an existing table of random protocol and properties, opens the
//! writer on it, then runs a random sequence of steps: commits of files partitioned from random rows, lost answers
//! that land or do not, other writers' commits and checkpoints, and restarts. After every step the log is read back
//! from the bucket and held to what the writer acknowledged:
//!
//! - a writer that refuses the table appends no commit;
//! - a commit, once in the bucket, is never replaced, and every line of it reads;
//! - every checkpoint holds the state the commits up to it replay to;
//! - at the end the log loads at its newest version, every acknowledged file is added exactly once, no file of this
//!   writer's is added twice, and each add's path and partition values are the file's;
//! - a column an earlier table holds narrower, as another engine may have made it, is written as that type, and a
//!   partition value of it reads as that type or its row is refused.
//!
//! It draws from `crate::fuzz`: a failure names its seed, `SMELTR_FUZZ_SEED=n` replays seed `n`, and the run fails
//! when its seeds leave a data type, protocol feature, table property or fault in `COVERED` unreached.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};

use ::parquet::arrow::ArrowWriter;
use ::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use arrow_array::cast::AsArray;
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Float64Array, Int64Array, ListArray, RecordBatch, StringArray,
    StructArray, TimestampMicrosecondArray, new_null_array,
};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::DataType as Arrow;
use object_store::path::Path;
use object_store::{ObjectStore, PutMode, PutPayload};

use super::tests::{Fault, Table, str_col};
use super::*;
use crate::core::buffer::BufferPool;
use crate::core::column::Column;
use crate::format::record::RecordView;
use crate::fuzz::{self, Coverage, Rng};
use crate::io::delta::log::{FileAction, Name, file_key};
use crate::io::parquet::{Chunk, Columns, REST};

/// The seeds a run takes.
const SEEDS: std::ops::Range<u64> = 1..201;

/// What a run must reach over its seeds: every data type, protocol version and feature, table property, fault, other
/// writer's change and commit outcome it draws.
const COVERED: &[&str] = &[
    "type null",
    "type bool",
    "type i64",
    "type f64",
    "type datetime",
    "type duration",
    "type str",
    "type bytes",
    "type variant",
    "type array",
    "type struct",
    "type open struct",
    "new table",
    "existing table",
    "narrowed byte",
    "narrowed short",
    "narrowed integer",
    "narrowed float",
    "narrowed date",
    "narrowed timestamp_ntz",
    "narrowed written as its type",
    "narrowed partition",
    "narrowed partition refused",
    "partitioned",
    "partitioning refused",
    "partition null",
    "partition key too long",
    "partition key near the limit",
    "reader v1",
    "reader v2",
    "reader v3",
    "reader v4",
    "writer v2",
    "writer v3",
    "writer v4",
    "writer v5",
    "writer v6",
    "writer v7",
    "writer v8",
    "writer appendOnly",
    "writer invariants",
    "writer checkConstraints",
    "writer changeDataFeed",
    "writer columnMapping",
    "writer v2Checkpoint",
    "writer domainMetadata",
    "writer deletionVectors",
    "writer vacuumProtocolCheck",
    "writer timestampNtz",
    "writer rowTracking",
    "writer identityColumns",
    "writer generatedColumns",
    "writer clustering",
    "writer typeWidening",
    "reader columnMapping",
    "reader deletionVectors",
    "reader v2Checkpoint",
    "reader timestampNtz",
    "reader typeWidening",
    "mapping none",
    "mapping name",
    "mapping name without maxColumnId",
    "mapping unsupported",
    "check constraint",
    "checkpointInterval",
    "writeStatsAsJson",
    "writeStatsAsStruct",
    "logRetentionDuration",
    "enableExpiredLogCleanup",
    "dataSkippingNumIndexedCols",
    "dataSkippingStatsColumns",
    "clustering column stats",
    "clustering past the indexed columns",
    "row tracking enabled",
    "row tracking suspended",
    "row ids above the high-water mark",
    "other row ids",
    "type widening enabled",
    "type widening disabled",
    "widened",
    "type changes kept",
    "orc",
    "fault Lost",
    "fault Dropped",
    "fault Late",
    "late landed",
    "commit ok",
    "commit conflict",
    "commit unknown",
    "commit transient",
    "footer",
    "other add",
    "other remove",
    "other checkpointInterval",
    "other add column",
    "other drop column",
    "other restore column",
    "other protocol",
    "other table id",
    "other partitioning",
    "other txn",
    "other checkpoint",
    "open refused",
    "restart",
    "checkpoint checked",
    "log cleaned",
];

thread_local! {
    /// What the seeds on this thread reached. The generators take only the PRNG, so they note what they draw here.
    static REACHED: RefCell<BTreeSet<String>> = const { RefCell::new(BTreeSet::new()) };
}

fn reach(name: impl Into<String>) {
    REACHED.with(|r| r.borrow_mut().insert(name.into()));
}

/// Steps after the writer opens.
const STEPS: usize = 24;

/// The name another writer's transactions carry.
const OTHER: &str = "other";

/// Field names: plain, differing only in case, with spaces, unicode and URL escapes, and `rest`, which an open record
/// writes its unclaimed fields under.
const NAMES: [&str; 14] = [
    "a", "b", "n", "ts", "msg", "host", "Host", "x y", "ünï", "a/b=c%", "dt", "rest", "_c", "123",
];

/// Strings that break paths and escapes, and some near the 1024 bytes an S3 key holds.
fn odd_string(rng: &mut Rng) -> String {
    match rng.below(14) {
        0 => String::new(),
        1 => "/".into(),
        2 => "=".into(),
        3 => "%".into(),
        4 => "%2F".into(),
        5 => "a b+c".into(),
        6 => "ünï日本🦀".into(),
        7 => "__HIVE_DEFAULT_PARTITION__".into(),
        8 => "\u{0}\n\t\"\\".into(),
        9 => "x".repeat(900 + rng.below(200)),
        10 => "é".repeat(150 + rng.below(400)),
        11 => "/=%".repeat(rng.below(400)),
        _ => format!("v{}", rng.below(5)),
    }
}

fn odd_i64(rng: &mut Rng) -> i64 {
    let random = rng.next_u64() as i64;
    *rng.pick(&[0, -1, 1, i64::MIN, i64::MAX, random, 42])
}

fn odd_f64(rng: &mut Rng) -> f64 {
    *rng.pick(&[0.0, -0.0, f64::NAN, f64::INFINITY, f64::NEG_INFINITY, 1e308, 0.1, -5.5])
}

/// Microseconds since the epoch: the protocol's example, either end of the years ISO 8601 writes in four digits,
/// and either end of the type.
fn odd_micros(rng: &mut Rng) -> i64 {
    *rng.pick(&[
        0,
        -1,
        1_510_871_468_123_456,
        253_402_300_799_999_999,
        -62_135_596_800_000_000,
        i64::MIN,
        i64::MAX,
    ])
}

fn data_type(rng: &mut Rng, depth: usize) -> DataType {
    let t = match rng.below(if depth < 2 { 15 } else { 12 }) {
        0 => DataType::Null,
        1 => DataType::Bool,
        2 | 3 => DataType::I64,
        4 => DataType::F64,
        5 => DataType::DateTime,
        6 => DataType::Duration,
        7..=9 => DataType::Str,
        10 => DataType::Bytes,
        11 => DataType::Variant,
        12 => DataType::Array(Box::new(data_type(rng, depth + 1))),
        _ => DataType::Struct(fields(rng, 3, depth + 1), rng.chance(4)),
    };
    match &t {
        DataType::Array(_) => reach("type array"),
        t => reach(format!("type {t}")),
    }
    if matches!(t, DataType::Struct(_, true)) {
        reach("type open struct");
    }
    t
}

/// One to `most` fields of distinct names.
fn fields(rng: &mut Rng, most: usize, depth: usize) -> Vec<(String, DataType)> {
    let mut names: Vec<&str> = NAMES.to_vec();
    (0..1 + rng.below(most))
        .map(|_| {
            let name = names.remove(rng.below(names.len()));
            (name.to_string(), data_type(rng, depth))
        })
        .collect()
}

fn record(rng: &mut Rng) -> DataType {
    let open = rng.chance(4);
    if open {
        reach("type open struct");
    }
    DataType::Struct(fields(rng, 5, 0), open)
}

/// The record's top-level columns and their types, `rest` last when it is open.
fn top_level(record: &DataType) -> Vec<(String, DataType)> {
    let DataType::Struct(fields, open) = record else {
        return Vec::new();
    };
    let mut columns = fields.clone();
    if *open {
        columns.push((REST.to_string(), DataType::Str));
    }
    columns
}

/// Up to three distinct names of the record's columns, sometimes one it does not have.
fn partition_by(rng: &mut Rng, record: &DataType) -> Vec<String> {
    let columns = top_level(record);
    let mut by: Vec<String> = Vec::new();
    if columns.is_empty() {
        return by;
    }
    for _ in 0..rng.below(4) {
        let name = rng.pick(&columns).0.clone();
        if !by.contains(&name) {
            by.push(name);
        }
    }
    if rng.chance(12) {
        by.push("missing".into());
    }
    by
}

/// Whether the writer must refuse to partition `record` by `by`: a name that is no top-level column, or one of a
/// type no partition value is written of.
fn partitioning_refused(record: &DataType, by: &[String]) -> bool {
    let columns = top_level(record);
    by.iter().any(|name| {
        !columns.iter().any(|(n, t)| {
            n == name
                && matches!(
                    t,
                    DataType::Bool
                        | DataType::I64
                        | DataType::Duration
                        | DataType::F64
                        | DataType::DateTime
                        | DataType::Str
                        | DataType::Variant
                )
        })
    })
}

/// The writer features of tables made here: the ones the writer supports, and some it does not.
const FEATURES: [&str; 15] = [
    "appendOnly",
    "invariants",
    "checkConstraints",
    "changeDataFeed",
    "columnMapping",
    "v2Checkpoint",
    "domainMetadata",
    "deletionVectors",
    "vacuumProtocolCheck",
    "timestampNtz",
    "rowTracking",
    "identityColumns",
    "generatedColumns",
    "clustering",
    "typeWidening",
];

const READER_FEATURES: [&str; 5] = [
    "columnMapping",
    "deletionVectors",
    "v2Checkpoint",
    "timestampNtz",
    "typeWidening",
];

/// A random protocol, and whether the writer must refuse it.
fn protocol(rng: &mut Rng) -> (Value, bool) {
    let reader = *rng.pick(&[1, 1, 2, 3, 4]);
    let writer = *rng.pick(&[2, 3, 4, 5, 6, 7, 7, 7, 7, 8]);
    let mut p = json!({"minReaderVersion": reader, "minWriterVersion": writer});
    let mut refused = reader > 3 || writer > 7;
    reach(format!("reader v{reader}"));
    reach(format!("writer v{writer}"));
    if writer == 7 {
        let mut features = Vec::new();
        for feature in FEATURES {
            if rng.chance(if WRITER_FEATURES.contains(&feature) { 3 } else { 6 }) {
                features.push(feature);
                reach(format!("writer {feature}"));
            }
        }
        refused |= features.iter().any(|f| !WRITER_FEATURES.contains(f));
        p["writerFeatures"] = json!(features);
    }
    if reader == 3 {
        let features: Vec<&str> = READER_FEATURES.into_iter().filter(|_| rng.chance(3)).collect();
        features.iter().for_each(|f| reach(format!("reader {f}")));
        p["readerFeatures"] = json!(features);
    }
    (p, refused)
}

/// Random table properties, and whether the writer must refuse them.
fn configuration(rng: &mut Rng, metadata: &mut Value, record: &DataType) -> bool {
    let mut refused = false;
    let set = |metadata: &mut Value, key: &str, value: &str| {
        metadata["configuration"][key] = json!(value);
        if let Some(property) = key.strip_prefix("delta.").and_then(|k| k.rsplit('.').next()) {
            reach(property);
        }
    };
    match rng.below(8) {
        0 => {
            set(metadata, "delta.columnMapping.mode", "none");
            reach("mapping none");
        }
        1 | 2 => {
            let mapped = schema::table_schema(record, None, Mapping::Name { max_column_id: 0 }, None).unwrap();
            let Mapping::Name { max_column_id } = mapped.mapping else {
                unreachable!("a mapped schema counts its ids")
            };
            metadata["schemaString"] = json!(mapped.schema.to_json());
            set(metadata, "delta.columnMapping.mode", "name");
            if rng.chance(6) {
                reach("mapping name without maxColumnId");
                refused = true;
            } else {
                reach("mapping name");
                set(metadata, "delta.columnMapping.maxColumnId", &max_column_id.to_string());
            }
        }
        3 => {
            set(metadata, "delta.columnMapping.mode", rng.pick(&["id", "Name", "other"]));
            reach("mapping unsupported");
            refused = true;
        }
        _ => {}
    }
    if rng.chance(8) {
        set(metadata, "delta.constraints.positive", "n > 0");
        reach("check constraint");
        refused = true;
    }
    if rng.chance(2) {
        set(
            metadata,
            "delta.checkpointInterval",
            rng.pick(&["1", "2", "3", "0", "-1", " 2 ", "x"]),
        );
    }
    if rng.chance(4) {
        let value = *rng.pick(&["true", "TRUE", "false"]);
        refused |= value == "false";
        set(metadata, "delta.checkpoint.writeStatsAsJson", value);
    }
    if rng.chance(4) {
        let value = *rng.pick(&["false", "False", "true"]);
        refused |= value == "true";
        set(metadata, "delta.checkpoint.writeStatsAsStruct", value);
    }
    if rng.chance(2) {
        let retention = [
            "interval 0 seconds",
            "interval 1 millisecond",
            "interval 30 days",
            "1 month",
            "soon",
        ];
        set(metadata, "delta.logRetentionDuration", rng.pick(&retention));
    }
    if rng.chance(6) {
        set(metadata, "delta.enableExpiredLogCleanup", "false");
    }
    if rng.chance(4) {
        set(
            metadata,
            "delta.dataSkippingNumIndexedCols",
            rng.pick(&["-1", "0", "1", "x"]),
        );
    }
    if rng.chance(6) {
        set(metadata, "delta.dataSkippingStatsColumns", "n, msg ,x y");
    }
    if rng.chance(20) {
        metadata["format"]["provider"] = json!("orc");
        reach("orc");
        refused = true;
    }
    refused
}

/// Some of `record`'s top-level columns in `metadata`'s schema held narrower, as another engine may have made them.
/// A partition column is left as it is: its random rows' values mostly do not fit, and the run partitions every one.
fn narrow(rng: &mut Rng, metadata: &mut Value, record: &DataType) {
    let DataType::Struct(fields, _) = record else {
        unreachable!("a record is a struct")
    };
    let Some(mut schema) = metadata["schemaString"]
        .as_str()
        .and_then(|s| serde_json::from_str::<Value>(s).ok())
    else {
        return;
    };
    let Some(columns) = schema["fields"].as_array_mut() else {
        return;
    };
    for column in columns {
        let Some((_, t)) = fields.iter().find(|(name, _)| column["name"] == name.as_str()) else {
            continue;
        };
        // An array's element narrows as a column does.
        let (slot, t, within) = match (column["type"].is_object(), t) {
            (true, DataType::Array(element)) => (&mut column["type"]["elementType"], &**element, Some("element")),
            _ => (&mut column["type"], t, None),
        };
        let to = match (slot.as_str(), t) {
            (Some("long"), DataType::I64) => *rng.pick(&["byte", "short", "integer"]),
            (Some("double"), DataType::F64) => "float",
            (Some("timestamp"), DataType::DateTime) => *rng.pick(&["date", "timestamp_ntz"]),
            _ => continue,
        };
        if rng.chance(4) {
            continue;
        }
        *slot = json!(to);
        reach(format!("narrowed {to}"));
        // An earlier change, as a table that widened the column once already records it.
        let from = match to {
            "short" => Some("byte"),
            "integer" => Some("short"),
            "timestamp_ntz" => Some("date"),
            _ => None,
        };
        if let Some(from) = from.filter(|_| rng.chance(2)) {
            let mut change = json!({"fromType": from, "toType": to});
            if let Some(within) = within {
                change["fieldPath"] = json!(within);
            }
            column["metadata"]["delta.typeChanges"] = json!([change]);
        }
    }
    metadata["schemaString"] = json!(schema.to_string());
}

/// The domain an engine that clusters the table writes, over one or two of `metadata`'s top-level primitive columns
/// that do not partition it, and those columns' physical names. None when it has no such column.
fn clustering(rng: &mut Rng, metadata: &Value) -> Option<(Value, Vec<String>)> {
    let schema: Value = serde_json::from_str(metadata["schemaString"].as_str()?).ok()?;
    let partitions = metadata["partitionColumns"].as_array()?;
    let mut columns: Vec<String> = (schema["fields"].as_array()?.iter())
        .filter(|f| f["type"].is_string() && !partitions.contains(&f["name"]))
        .filter_map(|f| {
            let physical = &f["metadata"]["delta.columnMapping.physicalName"];
            physical.as_str().or(f["name"].as_str()).map(str::to_string)
        })
        .collect();
    if columns.is_empty() {
        return None;
    }
    let mut chosen = Vec::new();
    for _ in 0..1 + rng.below(2).min(columns.len() - 1) {
        chosen.push(columns.remove(rng.below(columns.len())));
    }
    let configuration = json!({"clusteringColumns": chosen.iter().map(|c| vec![c]).collect::<Vec<_>>()});
    let domain = json!({"domainMetadata": {"domain": "delta.clustering", "configuration": configuration.to_string(),
                                           "removed": false}});
    Some((domain, chosen))
}

/// Whether the protocol's type widening allows a column of type `from` to become `to`.
fn widens_to(from: &str, to: &str) -> bool {
    let integers = ["byte", "short", "integer", "long"];
    let rank = |t: &str| integers.iter().position(|i| *i == t);
    match (rank(from), rank(to)) {
        (Some(from), Some(to)) => from < to,
        _ => matches!(
            (from, to),
            ("float", "double") | ("byte" | "short" | "integer", "double") | ("date", "timestamp_ntz")
        ),
    }
}

/// The type changes of `field`, a field of a schema's JSON.
fn type_changes(field: &Value) -> Vec<Value> {
    field["metadata"]["delta.typeChanges"]
        .as_array()
        .cloned()
        .unwrap_or_default()
}

/// The domain that holds a row-tracking table's highest row id, `high`.
fn row_tracking(high: i64) -> Value {
    let configuration = json!({"rowIdHighWaterMark": high}).to_string();
    json!({"domainMetadata": {"domain": "delta.rowTracking", "configuration": configuration, "removed": false}})
}

/// The row ids a table tracking them gives its next rows: one above its high-water mark. None when it does not.
fn next_row_id(protocol: &Value, metadata: &Value, high: Option<i64>) -> Option<i64> {
    let tracked = (protocol["writerFeatures"].as_array()).is_some_and(|f| f.contains(&json!("rowTracking")));
    let suspended = metadata["configuration"]["delta.rowTrackingSuspended"] == "true";
    (tracked && !suspended).then(|| high.map_or(0, |high| high + 1))
}

/// The high-water mark `domain`, an action's domainMetadata, sets, if it is row tracking's.
fn high_water_mark(domain: &Value) -> Option<i64> {
    if domain["domain"] != "delta.rowTracking" {
        return None;
    }
    let configuration: Value = serde_json::from_str(domain["configuration"].as_str()?).ok()?;
    configuration["rowIdHighWaterMark"].as_i64()
}

/// Whether `text`, a partition value of a `kind` column, reads as that type. A column the record types alike is true.
fn narrow_text(kind: &Kind, text: &str) -> bool {
    let fits = match kind {
        Kind::Byte => text.parse::<i8>().is_ok(),
        Kind::Short => text.parse::<i16>().is_ok(),
        Kind::Int => text.parse::<i32>().is_ok(),
        Kind::Float => text.parse::<f32>().is_ok(),
        // The protocol's form of a time without zone: a space before the time, and no zone. A year past 9999 takes
        // more digits and a sign.
        Kind::Timestamp => text.contains(' ') && !text.contains('T') && !text.ends_with('Z'),
        _ => return true,
    };
    if fits {
        reach("narrowed partition");
    }
    fits
}

/// Err when a column `committer`'s table holds narrower is not written as that type.
fn kept_narrow(committer: &DeltaCommitter) -> Result<(), String> {
    for column in &committer.schema.fields {
        let schema::Type::Primitive(p) = &column.data_type else {
            continue;
        };
        let (Some(want), Some(at)) = (schema::narrow(p), committer.arrow.index_of(&column.name).ok()) else {
            continue;
        };
        let written = committer.written.field(at).data_type();
        if *written != want {
            return Err(format!(
                "{} is {p} in the table, and its files write it as {written}",
                column.name
            ));
        }
        reach("narrowed written as its type");
    }
    Ok(())
}

/// `record` with fields left out, and sometimes one of another type: the record of an earlier writer.
fn earlier(rng: &mut Rng, record: &DataType) -> DataType {
    let DataType::Struct(fields, open) = record else {
        unreachable!("a record is a struct")
    };
    let mut kept: Vec<(String, DataType)> = fields.iter().filter(|_| !rng.chance(3)).cloned().collect();
    if !kept.is_empty() && rng.chance(8) {
        let at = rng.below(kept.len());
        kept[at].1 = data_type(rng, 1);
    }
    DataType::Struct(kept, *open && !rng.chance(3))
}

/// Random rows' arrays of type `t`, nulls only where `nullable`.
fn array(rng: &mut Rng, t: &Arrow, nullable: bool, rows: usize) -> ArrayRef {
    let valid: Vec<bool> = (0..rows).map(|_| !nullable || !rng.chance(4)).collect();
    let nulls = nullable.then(|| NullBuffer::from(valid.clone()));
    fn each<T>(rng: &mut Rng, valid: &[bool], mut value: impl FnMut(&mut Rng) -> T) -> Vec<Option<T>> {
        valid.iter().map(|&v| v.then(|| value(rng))).collect()
    }
    match t {
        Arrow::Boolean => Arc::new(BooleanArray::from(each(rng, &valid, |r| r.chance(2)))),
        Arrow::Int64 => Arc::new(Int64Array::from(each(rng, &valid, odd_i64))),
        Arrow::Float64 => Arc::new(Float64Array::from(each(rng, &valid, odd_f64))),
        Arrow::Timestamp(_, tz) => {
            Arc::new(TimestampMicrosecondArray::from(each(rng, &valid, odd_micros)).with_timezone_opt(tz.clone()))
        }
        Arrow::Utf8 => Arc::new(StringArray::from(each(rng, &valid, odd_string))),
        Arrow::Binary => Arc::new(BinaryArray::from_iter(each(rng, &valid, |r| {
            odd_string(r).into_bytes()
        }))),
        Arrow::List(element) => {
            let lengths: Vec<usize> = valid.iter().map(|&v| if v { rng.below(3) } else { 0 }).collect();
            let values = array(rng, element.data_type(), element.is_nullable(), lengths.iter().sum());
            Arc::new(ListArray::new(
                element.clone(),
                OffsetBuffer::from_lengths(lengths),
                values,
                nulls,
            ))
        }
        Arrow::Struct(fields) => {
            let children = (fields.iter())
                .map(|f| array(rng, f.data_type(), f.is_nullable(), rows))
                .collect();
            Arc::new(StructArray::new(fields.clone(), children, nulls))
        }
        other => new_null_array(other, rows),
    }
}

/// The footer of a Parquet file of `rows` random rows of `schema`.
fn footer(rng: &mut Rng, schema: &Arc<ArrowSchema>, rows: usize) -> Arc<ParquetMetaData> {
    let columns = (schema.fields().iter())
        .map(|f| array(rng, f.data_type(), f.is_nullable(), rows))
        .collect();
    let batch = RecordBatch::try_new(schema.clone(), columns).expect("the arrays are of the schema's types");
    let mut writer = ArrowWriter::try_new(Vec::new(), schema.clone(), None).unwrap();
    writer.write(&batch).unwrap();
    Arc::new(writer.close().unwrap())
}

/// A column of `rows` random values of `t`, or None for a type no row is built of here.
fn column(rng: &mut Rng, pool: &mut BufferPool, t: &DataType, rows: usize) -> Option<Column> {
    let nulls: Vec<bool> = (0..rows).map(|_| rng.chance(6)).collect();
    let mut column = match t {
        DataType::Str => {
            let values: Vec<String> = (0..rows).map(|_| odd_string(rng)).collect();
            let values: Vec<Option<&str>> = (values.iter().zip(&nulls))
                .map(|(v, &null)| (!null).then_some(v.as_str()))
                .collect();
            return Some(str_col(pool, &values));
        }
        DataType::I64 | DataType::Duration | DataType::DateTime | DataType::F64 | DataType::Bool => {
            Column::alloc(pool, t, rows)
        }
        _ => return None,
    };
    for (row, &null) in nulls.iter().enumerate() {
        match t {
            DataType::I64 => column.i64_mut().unwrap().values[row] = odd_i64(rng),
            DataType::Duration => column.duration_mut().unwrap().values[row] = odd_i64(rng),
            DataType::DateTime => column.datetime_mut().unwrap().values[row] = odd_micros(rng),
            DataType::F64 => column.f64_mut().unwrap().values[row] = odd_f64(rng),
            _ => {
                if rng.chance(2) {
                    column.bool_mut().unwrap().values[row / 64] |= 1 << (row % 64);
                }
            }
        }
        if null {
            match t {
                DataType::I64 => column.i64_mut().unwrap().validity.set_valid(row, false),
                DataType::Duration => column.duration_mut().unwrap().validity.set_valid(row, false),
                DataType::DateTime => column.datetime_mut().unwrap().validity.set_valid(row, false),
                DataType::F64 => column.f64_mut().unwrap().validity.set_valid(row, false),
                _ => column.bool_mut().unwrap().validity.set_valid(row, false),
            }
        }
    }
    Some(column)
}

/// `s` with `URLEncoder`'s escapes undone.
fn url_decode(s: &str) -> Option<String> {
    let mut out = Vec::with_capacity(s.len());
    let mut bytes = s.bytes();
    while let Some(b) = bytes.next() {
        match b {
            b'+' => out.push(b' '),
            b'%' => {
                let hex = [bytes.next()?, bytes.next()?];
                out.push(u8::from_str_radix(std::str::from_utf8(&hex).ok()?, 16).ok()?);
            }
            b => out.push(b),
        }
    }
    String::from_utf8(out).ok()
}

/// A URI path's escapes undone.
fn uri_decode(s: &str) -> Option<String> {
    let mut out = Vec::with_capacity(s.len());
    let mut bytes = s.bytes();
    while let Some(b) = bytes.next() {
        if b == b'%' {
            let hex = [bytes.next()?, bytes.next()?];
            out.push(u8::from_str_radix(std::str::from_utf8(&hex).ok()?, 16).ok()?);
        } else {
            out.push(b);
        }
    }
    String::from_utf8(out).ok()
}

/// The table's state as its actions replay.
#[derive(Debug, Default, PartialEq)]
struct Replay {
    protocol: Option<Value>,
    metadata: Option<Value>,
    /// Each application's last transaction.
    txns: BTreeMap<String, i64>,
    /// Each file's key, and whether its last action adds it.
    files: BTreeMap<String, bool>,
}

impl Replay {
    fn apply(&mut self, action: Action) {
        match action {
            Action::Protocol(p) => self.protocol = Some(serde_json::to_value(p).unwrap()),
            Action::Metadata(m) => self.metadata = Some(serde_json::to_value(m).unwrap()),
            Action::Txn(t) => {
                self.txns.insert(t.app_id, t.version);
            }
            Action::File(f) => {
                self.files.insert(f.key, f.add);
            }
            _ => {}
        }
    }
}

/// What one of this writer's files must be added as.
struct Expected {
    /// The add's path, its escapes undone.
    relative: String,
    partition_values: Value,
    /// The physical names of the clustering columns its stats must count.
    clustering: Vec<String>,
    records: u64,
}

/// One seed's table and what has been seen of it.
struct Run {
    rng: Rng,
    table: Table,
    log: Log,
    record: DataType,
    partition_by: Vec<String>,
    committer: Option<DeltaCommitter>,
    /// Every commit the bucket has held, by version, as first read.
    history: BTreeMap<i64, Vec<u8>>,
    /// The checkpoints already held to the commits.
    checked: BTreeSet<String>,
    /// Files whose commit failed, to be committed again.
    pending: Option<Vec<DataFile>>,
    /// The keys of files a commit acknowledged.
    acked: BTreeSet<String>,
    /// Each of this writer's files, acknowledged or not, by its key.
    ours: BTreeMap<String, Expected>,
    /// Another writer's files in the table, and its last transaction.
    other_files: Vec<String>,
    other_txn: i64,
    /// A column another writer dropped, to add again.
    dropped: Option<schema::StructField>,
    /// The physical names of the table's clustering columns.
    clustering: Vec<String>,
}

/// Runs seed `seed`. Err names the property it breaks.
fn run(seed: u64) -> Result<(), String> {
    let mut rng = Rng::seeded(seed);
    let record = record(&mut rng);
    let partition_by = partition_by(&mut rng, &record);
    let table = Table::new();
    let log = table.log();
    let mut run = Run {
        rng,
        log,
        table,
        record,
        partition_by,
        committer: None,
        history: BTreeMap::new(),
        checked: BTreeSet::new(),
        pending: None,
        acked: BTreeSet::new(),
        ours: BTreeMap::new(),
        other_files: Vec::new(),
        other_txn: 0,
        dropped: None,
        clustering: Vec::new(),
    };
    let must_refuse = run.existing()?;
    run.harvest()?;
    let opened = run.open()?;
    if !opened {
        return run.finish();
    }
    if must_refuse {
        return Err("the writer opened a table it must refuse".into());
    }
    for _ in 0..STEPS {
        match run.rng.below(20) {
            0..=8 => run.commit()?,
            9 | 10 => run.land(),
            11..=15 => run.other_commit()?,
            16 => run.other_checkpoint()?,
            _ => {
                reach("restart");
                run.open()?;
            }
        }
        run.harvest()?;
    }
    run.finish()
}

impl Run {
    /// Whether the writer must refuse to make a table of its partitioning.
    fn partitioning_refused(&self) -> bool {
        let refused = partitioning_refused(&self.record, &self.partition_by);
        match refused {
            true => reach("partitioning refused"),
            false if !self.partition_by.is_empty() => reach("partitioned"),
            false => {}
        }
        refused
    }

    /// Makes the table an earlier writer left, or none. Gives whether the writer must refuse it.
    fn existing(&mut self) -> Result<bool, String> {
        let rng = &mut self.rng;
        if rng.chance(3) {
            reach("new table");
            return Ok(self.partitioning_refused());
        }
        let earlier = earlier(rng, &self.record);
        let by = partition_by(rng, &earlier);
        let Ok(body) = create(&earlier, &by) else {
            reach("new table");
            return Ok(self.partitioning_refused());
        };
        reach("existing table");
        let mut actions: Vec<Value> = body.lines().map(|l| serde_json::from_str(l).unwrap()).collect();
        let mut refused = false;
        for action in &mut actions {
            if let Some(p) = action.get_mut("protocol") {
                let (random, refuses) = protocol(rng);
                *p = random;
                refused |= refuses;
            }
            if let Some(m) = action.get_mut("metaData") {
                refused |= configuration(rng, m, &earlier);
            }
        }
        // A table of writer features knows type widening more often than the protocol draw alone gives, so the seeds
        // reach both a table that widens and one that does not.
        for action in &mut actions {
            let features = action.get_mut("protocol").and_then(|p| p.get_mut("writerFeatures"));
            if let Some(features) = features.and_then(Value::as_array_mut)
                && !features.contains(&json!("typeWidening"))
                && !self.rng.chance(4)
            {
                features.push(json!("typeWidening"));
            }
        }
        let features: Vec<Value> = (actions.iter())
            .find_map(|a| a["protocol"]["writerFeatures"].as_array().cloned())
            .unwrap_or_default();
        let metadata = actions
            .iter_mut()
            .find_map(|a| a.get_mut("metaData"))
            .expect("a create holds metaData");
        // A table that knows type widening is narrowed more often: the writer must widen it if it enables widening,
        // and leave it as it is if not.
        let knows = features.contains(&json!("typeWidening"));
        let widening = knows && self.rng.chance(2);
        if knows {
            metadata["configuration"]["delta.enableTypeWidening"] = json!(widening.to_string());
            reach(if widening {
                "type widening enabled"
            } else {
                "type widening disabled"
            });
        }
        if knows || !self.rng.chance(3) {
            narrow(&mut self.rng, metadata, &earlier);
        }
        if features.contains(&json!("rowTracking")) {
            match self.rng.below(3) {
                0 => {}
                1 => {
                    metadata["configuration"]["delta.enableRowTracking"] = json!("true");
                    reach("row tracking enabled");
                }
                _ => {
                    metadata["configuration"]["delta.rowTrackingSuspended"] = json!("true");
                    reach("row tracking suspended");
                }
            }
        }
        let clustered = features.contains(&json!("clustering"));
        if let Some((domain, columns)) = clustered.then(|| clustering(&mut self.rng, metadata)).flatten() {
            // With no column indexed, only being clustered takes a column stats.
            if self.rng.chance(2) {
                metadata["configuration"]["delta.dataSkippingNumIndexedCols"] = json!("0");
                reach("clustering past the indexed columns");
            }
            actions.push(domain);
            self.clustering = columns;
        }
        if features.contains(&json!("rowTracking")) && self.rng.chance(2) {
            actions.push(row_tracking(self.rng.below(1000) as i64));
        }
        self.put(0, &actions);
        if self.rng.chance(3) {
            self.other_commit()?;
        }
        if self.rng.chance(4) {
            self.other_checkpoint()?;
        }
        Ok(refused)
    }

    /// Writes commit `version` of `actions` where none is, as another writer. False when one is.
    fn put(&self, version: i64, actions: &[Value]) -> bool {
        let body = actions.iter().map(Value::to_string).collect::<Vec<_>>().join("\n");
        let path = Path::from(format!("tbl/_delta_log/{}", commit_name(version)));
        let put = (self.table.store.inner).put_opts(&path, PutPayload::from(body.into_bytes()), PutMode::Create.into());
        match self.table.runtime.block_on(put) {
            Ok(_) => true,
            Err(object_store::Error::AlreadyExists { .. }) => false,
            Err(e) => panic!("{e}"),
        }
    }

    /// The newest version the bucket has held.
    fn latest(&self) -> i64 {
        self.history.keys().next_back().copied().unwrap_or(-1)
    }

    /// Opens the writer, as a destination start does, a fault armed now and then. Gives whether it opened; a refusal
    /// must leave the log as it was.
    fn open(&mut self) -> Result<bool, String> {
        if self.rng.chance(3) {
            self.arm();
        }
        // A restart writes the batches again in files of their own.
        self.pending = None;
        let before = self.latest();
        let by: Vec<&str> = self.partition_by.iter().map(String::as_str).collect();
        let opened = self.table.open(&self.record, &by);
        self.harvest()?;
        match opened {
            Ok(committer) => {
                kept_narrow(&committer)?;
                self.committer = Some(committer);
                Ok(true)
            }
            Err(why) if self.latest() != before => Err(format!(
                "an open of {:?} refused ({why}) appended commits {} to {}",
                self.record,
                before + 1,
                self.latest()
            )),
            Err(_) => {
                reach("open refused");
                self.committer = None;
                Ok(false)
            }
        }
    }

    fn arm(&mut self) {
        let fault = *self.rng.pick(&[Fault::Lost, Fault::Dropped, Fault::Late]);
        reach(format!("fault {fault:?}"));
        self.table.arm(fault);
    }

    /// Writes the create whose answer was lost, where none is.
    fn land(&mut self) {
        let late = self.table.store.late.lock().unwrap().take();
        if let Some((path, payload)) = late {
            reach("late landed");
            let put = (self.table.store.inner).put_opts(&path, payload, PutMode::Create.into());
            match self.table.runtime.block_on(put) {
                Ok(_) | Err(object_store::Error::AlreadyExists { .. }) => {}
                Err(e) => panic!("{e}"),
            }
        }
    }

    /// Commits the files that failed, or new ones, a fault armed now and then.
    fn commit(&mut self) -> Result<(), String> {
        if self.committer.is_none() {
            return Ok(());
        }
        let files = match self.pending.take() {
            Some(files) if !self.rng.chance(5) => files,
            _ => self.files()?,
        };
        if self.rng.chance(3) {
            self.arm();
        }
        let committer = self.committer.as_mut().expect("checked above");
        let committed = committer.commit(&files);
        reach(match &committed {
            Ok(()) => "commit ok",
            Err(CommitError::Conflict) => "commit conflict",
            Err(CommitError::Unknown(_)) => "commit unknown",
            Err(CommitError::Transient(_)) => "commit transient",
        });
        match committed {
            Ok(()) => {
                for file in &files {
                    self.acked.insert(key(file));
                }
            }
            Err(_) => self.pending = Some(files),
        }
        Ok(())
    }

    /// One to three new files, each of a partition random rows fall in. A row whose partition makes too long a key is
    /// refused; any other must be taken.
    fn files(&mut self) -> Result<Vec<DataFile>, String> {
        let committer = self.committer.as_ref().expect("a writer is open");
        let rows = 1 + self.rng.below(3);
        let columns = top_level(&self.record);
        let mut pool = BufferPool::new();
        let mut built: Vec<(String, Column)> = Vec::new();
        let mut types: Vec<(String, DataType)> = Vec::new();
        for partition in &committer.partitions {
            let Some((name, t)) = columns.iter().find(|(n, _)| *n == partition.name && n != REST) else {
                continue;
            };
            if built.iter().any(|(n, _)| n == name) {
                continue;
            }
            if let Some(c) = column(&mut self.rng, &mut pool, t, rows) {
                built.push((name.clone(), c));
                types.push((name.clone(), t.clone()));
            }
        }
        if built.is_empty() {
            built.push(("zz".into(), Column::alloc(&mut pool, &DataType::I64, rows)));
            types.push(("zz".into(), DataType::I64));
        }
        let schema = parquet::Schema::new(&DataType::Struct(types, false)).unwrap();
        let chunk = {
            let view = RecordView::from_named(built.iter().map(|(n, c)| (n.as_str(), c)));
            let at: Vec<u32> = (0..rows as u32).collect();
            Arc::new(Chunk::cut(&schema, &[(&view, &at)], 0, &mut pool))
        };
        for (_, c) in built {
            c.reclaim(&mut pool);
        }
        let partitioner = DeltaPartitioner {
            columns: committer.partitions.clone(),
            unpartitioned_key_bytes: committer.root.key(&format!("{}.parquet", "0".repeat(32))).len(),
        };
        let mut files = Vec::new();
        for row in 0..rows as u32 {
            let owned = OwnedRecord {
                columns: Some(Columns {
                    chunk: chunk.clone(),
                    row,
                }),
                ..Default::default()
            };
            let partition = match partitioner.partition(&owned) {
                Ok(p) => p,
                Err(why) if why.contains("a key may hold") => {
                    reach("partition key too long");
                    continue;
                }
                // A value of the record's wider type that the table's narrower column cannot hold.
                Err(why) if why.contains("column cannot hold") || why.contains("float column's range") => {
                    let narrowed =
                        |p: &PartitionColumn| matches!(p.kind, Kind::Byte | Kind::Short | Kind::Int | Kind::Float);
                    if !committer.partitions.iter().any(narrowed) {
                        return Err(format!("a row's partition was refused: {why}"));
                    }
                    reach("narrowed partition refused");
                    continue;
                }
                Err(why) => return Err(format!("a row's partition was refused: {why}")),
            };
            for (segment, (column, (_, text))) in partition
                .path
                .iter()
                .zip(committer.partitions.iter().zip(&partition.values))
            {
                let (name, value) = segment.split_once('=').ok_or("a partition directory has no =")?;
                // The string `__HIVE_DEFAULT_PARTITION__` names the same directory as null, as Spark writes it: a
                // reader takes the value from the log.
                let value = match value {
                    NULL_PARTITION if text == NULL_PARTITION => text.clone(),
                    NULL_PARTITION => String::new(),
                    value => url_decode(value).unwrap_or_default(),
                };
                if url_decode(name).as_deref() != Some(column.name.as_str()) || value != *text {
                    return Err(format!("the directory {segment} is not {}={text:?}", column.name));
                }
            }
            for (column, (_, text)) in committer.partitions.iter().zip(&partition.values) {
                if !text.is_empty() && !narrow_text(&column.kind, text) {
                    return Err(format!(
                        "{}'s partition value {text:?} is no {}",
                        column.name, column.kind
                    ));
                }
            }
            let path = committer.file_path(&partition, &objects::object_id());
            if partition.values.iter().any(|(_, text)| text.is_empty()) {
                reach("partition null");
            }
            if path.len() > 900 {
                reach("partition key near the limit");
            }
            if path.len() > objects::MAX_KEY_BYTES {
                return Err(format!("a file's key is {} bytes", path.len()));
            }
            let record_count = 1 + self.rng.below(3) as u64;
            let metadata = (!self.rng.chance(4)).then(|| {
                reach("footer");
                footer(&mut self.rng, &committer.written, record_count as usize)
            });
            let partition_values: Map<String, Value> = (partition.values.iter())
                .map(|(physical, text)| {
                    (
                        physical.clone(),
                        if text.is_empty() { Value::Null } else { json!(text) },
                    )
                })
                .collect();
            let file = DataFile {
                path,
                partition,
                size_bytes: 1 + self.rng.below(1000) as u64,
                record_count,
                metadata,
            };
            self.ours.insert(
                key(&file),
                Expected {
                    relative: file.path.trim_start_matches("tbl/").to_string(),
                    partition_values: Value::Object(partition_values),
                    clustering: (self.clustering.iter())
                        .filter(|c| committer.written.field_with_name(c).is_ok())
                        .cloned()
                        .collect(),
                    records: record_count,
                },
            );
            files.push(file);
        }
        Ok(files)
    }

    /// The table as another writer reads it now.
    fn load(&self) -> Result<Snapshot, String> {
        Snapshot::load(&self.log, OTHER)
            .map_err(|e| format!("the log does not load: {e}"))?
            .ok_or_else(|| "the log has no commit".to_string())
    }

    /// Another writer's commit: files added or removed, or the table's properties, columns, protocol or id changed.
    fn other_commit(&mut self) -> Result<(), String> {
        let snapshot = self.load()?;
        let mut metadata = snapshot.metadata.clone();
        let mut schema = StructType::parse(&metadata.schema_string)?;
        let rng = &mut self.rng;
        let mut lines = vec![json!({"commitInfo": {"operation": "WRITE", "engineInfo": OTHER}})];
        match rng.below(12) {
            0..=4 => {
                reach("other add");
                self.other_txn += 1;
                let path = format!("other-{}.parquet", self.other_txn);
                lines.push(json!({"txn": {"appId": OTHER, "version": self.other_txn}}));
                let records = 1 + rng.below(5) as i64;
                let mut add = json!({"path": path, "partitionValues": {}, "size": 1, "modificationTime": 0,
                                     "dataChange": true, "stats": json!({"numRecords": records}).to_string()});
                let high = (snapshot.domains.values()).find_map(|d| high_water_mark(&serde_json::to_value(d).unwrap()));
                let protocol = serde_json::to_value(&snapshot.protocol).unwrap();
                if let Some(base) = next_row_id(&protocol, &serde_json::to_value(&metadata).unwrap(), high) {
                    reach("other row ids");
                    add["baseRowId"] = json!(base);
                    add["defaultRowCommitVersion"] = json!(snapshot.version + 1);
                    // Now and then it reserves ids past those it gives.
                    lines.push(row_tracking(base + records - 1 + rng.below(3) as i64));
                }
                lines.push(json!({"add": add}));
                self.other_files.push(path);
            }
            5 if !self.other_files.is_empty() => {
                reach("other remove");
                let path = self.other_files.remove(rng.below(self.other_files.len()));
                lines.push(json!({"remove": {"path": path, "deletionTimestamp": now_ms(), "dataChange": true}}));
            }
            6 => {
                reach("other checkpointInterval");
                (metadata.configuration).insert("delta.checkpointInterval".into(), (1 + rng.below(3)).to_string());
                lines.push(json!({"metaData": metadata}));
            }
            7 => {
                reach("other add column");
                let mut field = schema::StructField {
                    name: format!("other{}", rng.below(1000)),
                    data_type: schema::Type::Primitive("long".into()),
                    nullable: true,
                    metadata: Map::new(),
                };
                if let Ok(Mapping::Name { max_column_id }) = mapping(&metadata) {
                    let id = max_column_id + 1;
                    field.metadata.insert("delta.columnMapping.id".into(), json!(id));
                    field
                        .metadata
                        .insert("delta.columnMapping.physicalName".into(), json!(format!("col-o{id}")));
                    (metadata.configuration).insert("delta.columnMapping.maxColumnId".into(), id.to_string());
                }
                if !schema.fields.iter().any(|f| f.name.eq_ignore_ascii_case(&field.name)) {
                    schema.fields.push(field);
                }
                metadata.schema_string = schema.to_json();
                lines.push(json!({"metaData": metadata}));
            }
            8 if schema.fields.len() > 1 && self.dropped.is_none() => {
                reach("other drop column");
                self.dropped = Some(schema.fields.remove(rng.below(schema.fields.len())));
                metadata.schema_string = schema.to_json();
                lines.push(json!({"metaData": metadata}));
            }
            8 if self.dropped.is_some() => {
                reach("other restore column");
                schema.fields.push(self.dropped.take().expect("checked above"));
                metadata.schema_string = schema.to_json();
                lines.push(json!({"metaData": metadata}));
            }
            9 => {
                reach("other protocol");
                let mut protocol = snapshot.protocol.clone();
                if protocol.min_writer_version == 7 && rng.chance(2) {
                    let features = protocol.writer_features.get_or_insert_with(Vec::new);
                    features.retain(|f| WRITER_FEATURES.contains(&f.as_str()));
                } else {
                    protocol.min_writer_version = 7;
                    protocol.min_reader_version = protocol.min_reader_version.max(1);
                    let features = protocol.writer_features.get_or_insert_with(Vec::new);
                    features.push((*rng.pick(&["rowTracking", "appendOnly", "domainMetadata"])).into());
                }
                lines.push(json!({"protocol": protocol}));
            }
            10 if rng.chance(4) => {
                reach("other table id");
                metadata.id = uuid();
                lines.push(json!({"metaData": metadata}));
            }
            10 => {
                reach("other partitioning");
                metadata.partition_columns.reverse();
                lines.push(json!({"metaData": metadata}));
            }
            _ => {
                reach("other txn");
                lines.push(json!({"txn": {"appId": OTHER, "version": self.other_txn}}));
            }
        }
        let version = snapshot.version + 1;
        self.put(version, &lines);
        Ok(())
    }

    /// Another writer's checkpoint of the table as it reads it now.
    fn other_checkpoint(&mut self) -> Result<(), String> {
        let snapshot = self.load()?;
        // A checkpoint the table's properties keep from being written is no concern here: the log reads without it.
        reach("other checkpoint");
        let _ = checkpoint::write(&self.log, &snapshot);
        Ok(())
    }

    /// Reads what the log holds now: each new commit is kept, each kept one must be unchanged, and each new
    /// checkpoint must hold the state the commits up to it replay to.
    fn harvest(&mut self) -> Result<(), String> {
        let mut entries = Vec::new();
        self.log.list(None, |e| {
            entries.push(e);
            true
        })?;
        for entry in &entries {
            if let Name::Commit(version) = Name::of(&entry.name) {
                let Some(bytes) = self.log.get(&entry.name)? else {
                    continue;
                };
                match self.history.get(&version) {
                    Some(held) if *held != bytes => return Err(format!("commit {version} was replaced")),
                    Some(_) => {}
                    None => {
                        for line in std::str::from_utf8(&bytes).map_err(|e| e.to_string())?.lines() {
                            Action::parse(line).map_err(|e| format!("commit {version} does not read: {e}"))?;
                        }
                        self.history.insert(version, bytes.to_vec());
                    }
                }
            }
        }
        for entry in &entries {
            if let Name::Checkpoint(version) = Name::of(&entry.name)
                && self.checked.insert(entry.name.clone())
            {
                reach("checkpoint checked");
                let read = self.read_checkpoint(&entry.name, entry.size)?;
                let replayed = self.replay(version)?;
                if read != replayed {
                    return Err(format!(
                        "checkpoint {version} holds {read:?}, and the commits to it replay to {replayed:?}"
                    ));
                }
            }
        }
        Ok(())
    }

    /// The state the commits through `version` replay to.
    fn replay(&self, version: i64) -> Result<Replay, String> {
        let mut replay = Replay::default();
        for v in 0..=version {
            let bytes = self
                .history
                .get(&v)
                .ok_or_else(|| format!("commit {v} was never seen"))?;
            for line in std::str::from_utf8(bytes).unwrap().lines() {
                replay.apply(Action::parse(line)?);
            }
        }
        Ok(replay)
    }

    /// The state a classic checkpoint holds.
    fn read_checkpoint(&self, name: &str, size: u64) -> Result<Replay, String> {
        let mut replay = Replay::default();
        for action in checkpoint::top_level(&self.log, name, size)? {
            replay.apply(action);
        }
        let bytes = self.log.get(name)?.ok_or_else(|| format!("{name} is gone"))?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .and_then(|b| b.build())
            .map_err(|e| format!("{name}: {e}"))?;
        for batch in reader {
            let batch = batch.map_err(|e| format!("{name}: {e}"))?;
            for (column, add) in [("add", true), ("remove", false)] {
                let Some(actions) = batch.column_by_name(column) else {
                    continue;
                };
                let actions = actions.as_struct();
                let paths = actions.column_by_name("path").ok_or("a file action has no path")?;
                let paths = paths.as_string::<i32>();
                for row in (0..batch.num_rows()).filter(|&row| actions.is_valid(row)) {
                    replay.files.insert(file_key(paths.value(row), None), add);
                }
            }
        }
        Ok(replay)
    }

    /// Every row id a commit gives is fresh: above the high-water mark it commits over, in no other file's ids, and
    /// under the mark it leaves. This writer's files take ids, and their commit's version, exactly when the table
    /// tracks rows.
    fn check_row_ids(&self) -> Result<(), String> {
        let (mut protocol, mut metadata, mut high) = (Value::Null, Value::Null, None);
        let mut given: Vec<(String, i64, i64)> = Vec::new();
        for (version, bytes) in &self.history {
            let actions: Vec<Value> = (std::str::from_utf8(bytes).unwrap().lines())
                .map(|l| serde_json::from_str(l).unwrap())
                .collect();
            let next = next_row_id(&protocol, &metadata, high);
            let mut highest = None;
            for add in actions.iter().filter_map(|a| a.get("add")) {
                let key = file_key(add["path"].as_str().unwrap(), None);
                let ours = self.ours.get(&key);
                let records = match ours {
                    Some(expected) => expected.records as i64,
                    None => (add["stats"].as_str())
                        .and_then(|s| serde_json::from_str::<Value>(s).ok())
                        .and_then(|s| s["numRecords"].as_i64())
                        .unwrap_or(0),
                };
                let base = add["baseRowId"].as_i64();
                if ours.is_some() {
                    match (next, base) {
                        (Some(_), None) => {
                            return Err(format!(
                                "commit {version} adds {key} with no row ids, though the table tracks them"
                            ));
                        }
                        (None, Some(_)) => {
                            return Err(format!(
                                "commit {version} gives {key} row ids, though the table tracks none"
                            ));
                        }
                        (Some(next), Some(base)) if base < next => {
                            return Err(format!(
                                "commit {version} gives {key} row ids from {base}, not above its high-water mark {}",
                                next - 1
                            ));
                        }
                        (Some(_), Some(_)) if add["defaultRowCommitVersion"] != json!(version) => {
                            return Err(format!(
                                "commit {version} adds {key} with the row commit version {}",
                                add["defaultRowCommitVersion"]
                            ));
                        }
                        (Some(_), Some(_)) => reach("row ids above the high-water mark"),
                        (None, None) => {}
                    }
                }
                let Some(base) = base.filter(|_| records > 0) else {
                    continue;
                };
                let end = base + records;
                if let Some((other, ..)) = given.iter().find(|(_, b, e)| base < *e && *b < end) {
                    return Err(format!(
                        "commit {version} gives {key} row ids {base}..{end}, some of {other}'s"
                    ));
                }
                given.push((key, base, end));
                highest = highest.max(Some(end - 1));
            }
            for action in &actions {
                if let Some(p) = action.get("protocol") {
                    protocol = p.clone();
                }
                if let Some(m) = action.get("metaData") {
                    metadata = m.clone();
                }
                if let Some(mark) = action.get("domainMetadata").and_then(high_water_mark) {
                    high = Some(mark);
                }
            }
            if highest.is_some() && high < highest {
                return Err(format!(
                    "commit {version} gives row ids up to {highest:?}, above its high-water mark {high:?}"
                ));
            }
        }
        Ok(())
    }

    /// Each schema this writer commits keeps every column's earlier type changes, and changes a column's type only
    /// where the table lets it widen: to a type the protocol allows, recorded after the earlier changes.
    fn check_type_changes(&self) -> Result<(), String> {
        let (mut protocol, mut metadata) = (Value::Null, Value::Null);
        for (version, bytes) in &self.history {
            let actions: Vec<Value> = (std::str::from_utf8(bytes).unwrap().lines())
                .map(|l| serde_json::from_str(l).unwrap())
                .collect();
            let ours = actions.iter().any(|a| a["commitInfo"]["engineInfo"] == "smeltr");
            let changed = actions.iter().find_map(|a| a.get("metaData"));
            if let (true, Some(changed), Some(before)) = (ours, changed, metadata["schemaString"].as_str()) {
                let fields = |schema: &str| {
                    let schema: Value = serde_json::from_str(schema).unwrap();
                    schema["fields"].as_array().cloned().unwrap_or_default()
                };
                let widens = (protocol["writerFeatures"].as_array())
                    .is_some_and(|f| f.contains(&json!("typeWidening")))
                    && metadata["configuration"]["delta.enableTypeWidening"] == "true";
                let after = fields(changed["schemaString"].as_str().unwrap());
                for old in fields(before) {
                    let Some(new) = after.iter().find(|f| f["name"] == old["name"]) else {
                        continue;
                    };
                    let name = &old["name"];
                    let (was, kept) = (type_changes(&old), type_changes(new));
                    if !kept.starts_with(&was) {
                        return Err(format!("commit {version} loses {name}'s type changes {was:?}"));
                    }
                    if !was.is_empty() {
                        reach("type changes kept");
                    }
                    let (from, to, within) = match (&old["type"], &new["type"]) {
                        (from, to) if from == to && kept == was => continue,
                        (from, to) if from == to => {
                            return Err(format!(
                                "commit {version} records type changes {kept:?} of {name}, still {to}"
                            ));
                        }
                        (Value::String(from), Value::String(to)) => (from.as_str(), to.as_str(), None),
                        (from, to) => match (from["elementType"].as_str(), to["elementType"].as_str()) {
                            (Some(from), Some(to)) if from != to => (from, to, Some("element")),
                            _ => return Err(format!("commit {version} changes {name} from {from} to {to}")),
                        },
                    };
                    let partition = (metadata["partitionColumns"].as_array()).is_some_and(|p| p.contains(name));
                    if !widens || partition || !widens_to(from, to) {
                        return Err(format!("commit {version} changes {name} from {from} to {to}"));
                    }
                    let mut change = json!({"fromType": from, "toType": to});
                    if let Some(within) = within {
                        change["fieldPath"] = json!(within);
                    }
                    if kept[was.len()..] != [change.clone()] {
                        return Err(format!(
                            "commit {version} widens {name}, recording {:?}, not {change}",
                            &kept[was.len()..]
                        ));
                    }
                    reach("widened");
                }
            }
            for action in &actions {
                if let Some(p) = action.get("protocol") {
                    protocol = p.clone();
                }
                if let Some(m) = action.get("metaData") {
                    metadata = m.clone();
                }
            }
        }
        Ok(())
    }

    /// The properties of the log as the run left it.
    fn finish(&mut self) -> Result<(), String> {
        let latest = self.latest();
        if latest < 0 {
            return Ok(());
        }
        let missing: Vec<i64> = (0..=latest).filter(|v| !self.history.contains_key(v)).collect();
        if !missing.is_empty() {
            return Err(format!("the log skipped versions {missing:?}"));
        }
        let mut held = 0;
        self.log.list(None, |e| {
            held += matches!(Name::of(&e.name), Name::Commit(_)) as i64;
            true
        })?;
        if held <= latest {
            reach("log cleaned");
        }
        let snapshot = self.load()?;
        if snapshot.version != latest {
            return Err(format!(
                "the log loads at {}, not its newest {latest}",
                snapshot.version
            ));
        }
        self.check_row_ids()?;
        self.check_type_changes()?;
        let replayed = self.replay(latest)?;
        let loaded = Replay {
            protocol: Some(serde_json::to_value(&snapshot.protocol).unwrap()),
            metadata: Some(serde_json::to_value(&snapshot.metadata).unwrap()),
            txns: snapshot.txns.iter().map(|(app, t)| (app.clone(), t.version)).collect(),
            files: replayed.files.clone(),
        };
        if loaded != replayed {
            return Err(format!("the log loads as {loaded:?}, and replays to {replayed:?}"));
        }

        let mut added: BTreeMap<String, usize> = BTreeMap::new();
        let mut transactions: BTreeMap<String, i64> = BTreeMap::new();
        for (version, bytes) in &self.history {
            for line in std::str::from_utf8(bytes).unwrap().lines() {
                match Action::parse(line)? {
                    Action::File(FileAction {
                        add: true, key, json, ..
                    }) => {
                        *added.entry(key.clone()).or_default() += 1;
                        if let Some(expected) = self.ours.get(&key) {
                            let add: Value = serde_json::from_str(json.get()).unwrap();
                            if uri_decode(&key).as_deref() != Some(expected.relative.as_str()) {
                                return Err(format!("commit {version} adds {key}, not {}", expected.relative));
                            }
                            if add["partitionValues"] != expected.partition_values {
                                return Err(format!(
                                    "commit {version} adds {key} with the partition values {}, not {}",
                                    add["partitionValues"], expected.partition_values
                                ));
                            }
                            if let Some(stats) = add["stats"].as_str() {
                                let stats: Value = serde_json::from_str(stats).unwrap();
                                if let Some(c) = (expected.clustering.iter()).find(|c| stats["nullCount"][c].is_null())
                                {
                                    return Err(format!(
                                        "commit {version} adds {key} with no stats for its clustering column {c}"
                                    ));
                                }
                                if !expected.clustering.is_empty() {
                                    reach("clustering column stats");
                                }
                            }
                        }
                    }
                    Action::Txn(t) if t.app_id != OTHER => {
                        let last = transactions.insert(t.app_id.clone(), t.version);
                        if last.is_some_and(|last| last >= t.version) {
                            return Err(format!("commit {version} repeats a transaction of {}", t.app_id));
                        }
                    }
                    _ => {}
                }
            }
        }
        for key in self.ours.keys() {
            let times = added.get(key).copied().unwrap_or(0);
            if times > 1 {
                return Err(format!("{key} is added {times} times"));
            }
        }
        for key in &self.acked {
            if added.get(key) != Some(&1) || replayed.files.get(key) != Some(&true) {
                return Err(format!("{key} was acknowledged and is not in the table once"));
            }
        }
        Ok(())
    }
}

/// The key the log knows `file` by.
fn key(file: &DataFile) -> String {
    file_key(&uri_path(file.path.trim_start_matches("tbl/")), None)
}

#[test]
fn seeded_runs_keep_the_log_whole() {
    REACHED.with(|r| r.borrow_mut().clear());
    fuzz::run(SEEDS, run);
    let mut coverage = Coverage::new(COVERED.iter().copied());
    REACHED.with(|r| r.borrow().iter().for_each(|name| coverage.reach(name.as_str())));
    coverage.assert_complete();
}
