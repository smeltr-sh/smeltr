//! A seeded run of the Iceberg destination against a fake REST catalog and an in-memory bucket.
//!
//! Each seed draws a record, a partitioning, a sign-in, a namespace and table name, delivery limits, maybe a table
//! an earlier record made, and faults for the catalog's commits. One or two writers then deliver batches of random
//! rows, flushing now and then, until every batch is acknowledged. The table is read back from the catalog and the
//! bucket and held to what was delivered:
//!
//! - a writer that cannot take the table, for a partition term its record cannot fill or a column of another type,
//!   is refused before it writes anything;
//! - every row taken is in exactly one live data file, with the values it was delivered with;
//! - every unfit row is refused in its batch, and none is written;
//! - each data file's row count and partition values are its rows', and it lies under the table's location;
//! - the table's schema holds every column of the record, the earlier table's ids kept;
//! - every data file holds every column the earlier table has and the record lacks, as another engine may have added
//!   it, at any depth and of any type, all null, or where a version 3 table gives it a write default, holding that in
//!   every leaf; and none holds a version 3 table's `unknown` column;
//! - a column the earlier table holds narrower, as another engine may have made it, reads back as that type, and a
//!   row with a value it cannot hold is refused; a table partitioned by such a column takes its rows' partitions in
//!   that type;
//! - in a table of format version 3, every live file has row ids of its own, none shared and all below the table's
//!   next row id;
//! - every call for the table carries the sign-in drawn.
//!
//! It draws from `crate::fuzz`: a failure names its seed, `SMELTR_FUZZ_SEED=n` replays seed `n`, and the run fails
//! when its seeds leave anything in `COVERED` unreached.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::time::Instant;

use ::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use arrow_array::cast::AsArray;
use arrow_array::types::{Date32Type, Float32Type, Float64Type, Int32Type, Int64Type, TimestampMicrosecondType};
use arrow_array::{Array, StructArray};
use object_store::memory::InMemory;

use super::tests::{Fake, Fault, serve};
use super::*;
use crate::core::buffer::BufferPool;
use crate::core::column::{Column, StrBuilder, VarType};
use crate::core::variant::{self, Value};
use crate::format::json;
use crate::format::record::RecordView;
use crate::format::tag::{self, Tag};
use crate::io::parquet_variant::tests::read as read_variant;
use crate::fuzz::{self, Coverage, Rng};
use crate::io::iceberg::manifest::Status;
use crate::io::iceberg::metadata::{
    ListType, Literal, NestedField, PartitionField, PartitionSpec, Primitive, StructType, Transform, Type,
};
use crate::io::iceberg::transform::apply;
use crate::io::parquet::{Chunk, Columns};
use crate::io::{BatchCompletion, Destination, Dispatched, OwnedRecord};
use crate::plan::topology::{Compression, SecretRef};

/// The seeds a run takes.
const SEEDS: std::ops::Range<u64> = 1..241;

/// What a run must reach over its seeds.
const COVERED: &[&str] = &[
    "type bool",
    "type i64",
    "type f64",
    "type datetime",
    "type duration",
    "type str",
    "type bytes",
    "type null",
    "type variant",
    "variant written as variant",
    "variant written as text",
    "type array",
    "type struct",
    "partition identity",
    "partition year",
    "partition month",
    "partition day",
    "partition hour",
    "partition bucket",
    "partition truncate",
    "partition nested",
    "partition null",
    "partition path shortened",
    "partitioning refused",
    "new table",
    "existing table",
    "existing table evolved",
    "narrowed int",
    "narrowed float",
    "narrowed date",
    "narrowed timestamp",
    "narrowed nested",
    "row unfit for a narrowed column",
    "partition narrowed int",
    "partition narrowed float",
    "partition narrowed date",
    "partition narrowed timestamp",
    "table codec zstd",
    "table codec gzip",
    "table codec snappy",
    "table codec lz4",
    "table codec uncompressed",
    "table codec brotli",
    "table codec level",
    "table row group size",
    "table target file size",
    "table bloom filter",
    "evolution refused",
    "format version 3",
    "row ids assigned",
    "lacked str",
    "lacked decimal",
    "lacked uuid",
    "lacked fixed",
    "lacked time",
    "lacked list",
    "lacked map",
    "lacked struct",
    "lacked timestamp_ns",
    "lacked geometry",
    "lacked geography",
    "lacked variant",
    "lacked unknown",
    "lacked nested",
    "lacked default",
    "lacked required default",
    "sign-in none",
    "sign-in bearer",
    "sign-in client credentials",
    "sign-in client credentials at a token URL",
    "warehouse",
    "scope",
    "namespace nested",
    "compression none",
    "compression gzip",
    "compression snappy",
    "compression lz4",
    "compression zstd",
    "inFlight 1",
    "inFlight several",
    "maxObjectBytes small",
    "file rolled",
    "writers 2",
    "row unfit",
    "batch all unfit",
    "batches several",
    "flush between",
    "fault Lost",
    "fault Late",
    "fault Dropped",
    "fault Refused",
];

/// The types of a column the earlier table has and the record lacks.
const LACKED: &[&str] = &["str", "decimal", "uuid", "fixed", "time", "list", "map", "struct"];

/// The kinds of column only a table of format version 3 has.
const LACKED_3: &[&str] = &["timestamp_ns", "geometry", "geography", "variant", "unknown"];

/// What a data file holds in a column the record lacks.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Held {
    /// Null in every row.
    Null,
    /// The column's write default in every row, a value in every leaf.
    Default,
    /// No column at all: an `unknown` one.
    Nothing,
}

/// A column of `kind` (`LACKED`), its children's ids drawn from `next`.
fn lacked_type(kind: &str, next: &mut impl FnMut() -> i32) -> Type {
    let other = |name: &str| Type::Primitive(Primitive::Other(name.into()));
    match kind {
        "str" => Type::Primitive(Primitive::String),
        "decimal" => other("decimal(9, 2)"),
        "uuid" => other("uuid"),
        "fixed" => other("fixed[3]"),
        "time" => other("time"),
        "timestamp_ns" => other("timestamp_ns"),
        "geometry" => other("geometry(srid:4326)"),
        "geography" => other("geography(OGC:CRS84, vincenty)"),
        "variant" => other("variant"),
        "unknown" => other("unknown"),
        "list" => Type::List(ListType {
            element_id: next(),
            element_required: false,
            element: Box::new(Type::Primitive(Primitive::Long)),
        }),
        "map" => Type::Map(serde_json::json!({"type": "map", "key-id": next(), "key": "string",
            "value-id": next(), "value": "int", "value-required": false})),
        "struct" => Type::Struct(StructType {
            fields: vec![NestedField {
                id: next(),
                name: "a".into(),
                required: false,
                field_type: Type::Primitive(Primitive::Double),
                doc: None,
                other: Default::default(),
            }],
        }),
        other => unreachable!("{other} is no lacked type"),
    }
}

/// The spec's JSON single value of a column of `kind` (`LACKED`, `LACKED_3`); for a struct, `{}`, whose fields take
/// their own. None for a kind whose default is always null.
fn lacked_default(kind: &str) -> Option<serde_json::Value> {
    use serde_json::json;
    Some(match kind {
        "str" => json!("d"),
        "decimal" => json!("-1.5"),
        "uuid" => json!("f79c3e09-677c-4bbd-a479-3f349cb785e7"),
        "fixed" => json!("0a0b0c"),
        "time" => json!("01:02:03.000004"),
        "list" => json!([1, 2]),
        "map" => json!({"keys": ["k"], "values": [3]}),
        "struct" => json!({}),
        "timestamp_ns" => json!("1970-01-01T00:00:00.000000001"),
        "geometry" | "geography" | "variant" | "unknown" => return None,
        other => unreachable!("{other} is no lacked type"),
    })
}

/// Err when the data file `bytes` holds in a column of `lacked`, by id, other than it is `Held`.
fn holds_lacked(bytes: &[u8], lacked: &[(i32, Held)]) -> Result<(), String> {
    use ::parquet::file::metadata::ParquetMetaDataReader;
    use ::parquet::schema::types::Type as Node;
    let metadata = ParquetMetaDataReader::new()
        .parse_and_finish(&tokio_util::bytes::Bytes::copy_from_slice(bytes))
        .map_err(|e| format!("its footer does not read: {e}"))?;
    let descr = metadata.file_metadata().schema_descr();
    fn ids(node: &Node, out: &mut Vec<i32>) {
        let info = node.get_basic_info();
        if info.has_id() {
            out.push(info.id());
        }
        if node.is_group() {
            node.get_fields().iter().for_each(|f| ids(f, out));
        }
    }
    let mut held = Vec::new();
    ids(descr.root_schema(), &mut held);
    for (id, how) in lacked {
        match (how, held.contains(id)) {
            (Held::Nothing, true) => return Err(format!("it holds the unknown column of id {id}")),
            (Held::Null | Held::Default, false) => return Err(format!("it lacks the column of id {id}")),
            _ => {}
        }
    }
    for group in metadata.row_groups() {
        for (i, column) in group.columns().iter().enumerate() {
            let leaf = descr.column(i);
            let path = leaf.path().parts();
            // The leaf is of a lacked column when it or a group above it carries a lacked id.
            let mut node = descr.root_schema();
            let mut of_lacked = None;
            for part in path {
                let Some(child) = node.get_fields().iter().find(|f| f.name() == part) else {
                    break;
                };
                node = child;
                let info = node.get_basic_info();
                let at = lacked.iter().find(|(id, _)| info.has_id() && *id == info.id());
                of_lacked = of_lacked.or(at.map(|(_, how)| *how));
            }
            let nulls = column.statistics().and_then(|s| s.null_count_opt());
            match of_lacked {
                Some(Held::Null) if nulls != Some(column.num_values() as u64) => {
                    return Err(format!("{} holds a value", column.column_path()));
                }
                Some(Held::Default) if nulls != Some(0) => {
                    return Err(format!(
                        "{} holds a null, and its column has a write default",
                        column.column_path()
                    ));
                }
                _ => {}
            }
        }
    }
    Ok(())
}

/// A column a record may write: its name and type. `ctx` is a struct of `region` and `zone`.
const COLUMNS: &[(&str, &str)] = &[
    ("host", "str"),
    ("msg", "str"),
    ("n", "i64"),
    ("ts", "datetime"),
    ("ok", "bool"),
    ("ratio", "f64"),
    ("dur", "duration"),
    ("ctx", "struct"),
    ("blob", "bytes"),
    ("tags", "array"),
    ("nothing", "null"),
    ("doc", "variant"),
];

fn data_type(kind: &str) -> DataType {
    match kind {
        "str" => DataType::Str,
        "i64" => DataType::I64,
        "datetime" => DataType::DateTime,
        "bool" => DataType::Bool,
        "f64" => DataType::F64,
        "duration" => DataType::Duration,
        "bytes" => DataType::Bytes,
        "variant" => DataType::Variant,
        "array" => DataType::Array(Box::new(DataType::Str)),
        "null" => DataType::Null,
        "struct" => DataType::Struct(
            vec![("region".into(), DataType::Str), ("zone".into(), DataType::I64)],
            false,
        ),
        other => unreachable!("{other}"),
    }
}

/// The leaves a row holds values in, by dotted path, and their kinds. Bytes, lists and nulls are never filled: their
/// rows hold null.
fn leaves(columns: &[(&str, &'static str)]) -> Vec<(String, &'static str)> {
    let mut leaves = vec![("id".to_string(), "i64")];
    for (name, kind) in columns {
        match *kind {
            "struct" => {
                leaves.push((format!("{name}.region"), "str"));
                leaves.push((format!("{name}.zone"), "i64"));
            }
            "bytes" | "array" | "null" => {}
            kind => leaves.push((name.to_string(), kind)),
        }
    }
    leaves
}

fn record_of(columns: &[(&str, &str)]) -> DataType {
    let mut fields = vec![("id".to_string(), DataType::I64)];
    fields.extend(columns.iter().map(|(n, k)| (n.to_string(), data_type(k))));
    DataType::Struct(fields, false)
}

/// One value of a row, as it was delivered and as it reads back.
#[derive(Clone, Debug, PartialEq)]
enum Val {
    Null,
    Bool(bool),
    Long(i64),
    /// Its bits, so NaN equals itself.
    Double(u64),
    Micros(i64),
    Str(String),
    /// Its own bytes (`core::variant`).
    Variant(Vec<u8>),
}

impl Val {
    /// The value as a partition source holds it: a time is `timestamptz`, a duration a `long`.
    fn literal(&self) -> Option<Literal> {
        Some(match self {
            Val::Null => return None,
            Val::Bool(b) => Literal::Boolean(*b),
            Val::Long(v) => Literal::Long(*v),
            Val::Double(bits) => Literal::Double(f64::from_bits(*bits)),
            Val::Micros(us) => Literal::Micros(*us),
            Val::Str(s) => Literal::String(s.clone()),
            Val::Variant(_) => return None,
        })
    }
}

/// `v`, as `narrowed` reads it back from a column held as `kind`, as a partition source of that type holds it.
fn literal_as(v: &Val, kind: &str) -> Option<Literal> {
    Some(match (kind, v.literal()?) {
        ("int", Literal::Long(n)) => Literal::Int(i32::try_from(n).ok()?),
        ("date", Literal::Long(days)) => Literal::Date(i32::try_from(days).ok()?),
        (_, literal) => literal,
    })
}

/// What `v` reads back as from a column the table holds as `narrow`, narrower than the record types it; `None` when
/// the column cannot hold it. A date reads as its days.
fn narrowed(v: &Val, narrow: &str) -> Option<Val> {
    Some(match (narrow, v) {
        (_, Val::Null) => Val::Null,
        ("int", Val::Long(n)) => Val::Long(i32::try_from(*n).ok()?.into()),
        ("float", Val::Double(bits)) => {
            let f = f64::from_bits(*bits) as f32;
            if !f.is_finite() {
                return None;
            }
            Val::Double(f64::from(f).to_bits())
        }
        ("date", Val::Micros(us)) => Val::Long(us.div_euclid(crate::ops::civil::MICROS_PER_DAY)),
        ("timestamp", v) => v.clone(),
        (narrow, v) => unreachable!("{v:?} in a column held as {narrow}"),
    })
}

/// Strings that break paths and escapes.
fn odd_string(rng: &mut Rng) -> String {
    match rng.below(12) {
        0 => String::new(),
        1 => "/".into(),
        2 => "a=b/c".into(),
        3 => "%2F%".into(),
        4 => "a b+c?d#e".into(),
        5 => "ünï日本🦀".into(),
        6 => "\u{0}\n\t\"\\".into(),
        7 => "x".repeat(100 + rng.below(200)),
        8 => "null".into(),
        _ => format!("v{}", rng.below(4)),
    }
}

fn value(rng: &mut Rng, kind: &str) -> Val {
    if rng.chance(8) {
        return Val::Null;
    }
    match kind {
        "str" => Val::Str(odd_string(rng)),
        "i64" | "duration" => {
            let random = rng.next_u64() as i64;
            Val::Long(*rng.pick(&[0, -1, 1, i64::MIN, i64::MAX, 42, random]))
        }
        // Parquet holds no infinity or NaN of a row's: those are written as null.
        "f64" => match *rng.pick(&[0.0, -0.0, f64::NAN, f64::INFINITY, f64::NEG_INFINITY, 1e308, 0.1, -5.5]) {
            v if v.is_finite() => Val::Double(v.to_bits()),
            _ => Val::Null,
        },
        "bool" => Val::Bool(rng.chance(2)),
        "variant" => {
            let mut b = Vec::new();
            tag::encode(&variant_tag(rng, 2), &mut b);
            Val::Variant(b)
        }
        // Either side of 1970, the years ISO 8601 writes in four digits, and an hour that fits no `int` of hours.
        _ => Val::Micros(*rng.pick(&[
            0,
            -1,
            1_510_871_468_123_456,
            253_402_300_799_999_999,
            -62_135_596_800_000_000,
            i64::MAX,
        ])),
    }
}

/// A variant of every type, nested no deeper than `depth`.
fn variant_tag(rng: &mut Rng, depth: usize) -> Tag<'static> {
    let scalars = 8;
    match rng.below(scalars + if depth > 0 { 2 } else { 0 }) {
        0 => Tag::Bool(rng.chance(2)),
        1 => Tag::I64(*rng.pick(&[0, -1, i64::MIN, i64::MAX, 42])),
        2 => Tag::F64(*rng.pick(&[0.0, -0.0, 1e308, -5.5, f64::NAN])),
        3 => Tag::Str(Cow::Owned(odd_string(rng))),
        4 => Tag::DateTime(*rng.pick(&[0, -1, 1_510_871_468_123_456])),
        5 => Tag::Duration(*rng.pick(&[0, 90, -1])),
        6 => Tag::Bytes(Cow::Owned(vec![0, 255, b'"'][..rng.below(4)].to_vec())),
        7 => Tag::Str(Cow::Borrowed("")),
        8 => Tag::Array(
            (0..rng.below(4))
                .map(|_| (!rng.chance(5)).then(|| variant_tag(rng, depth - 1)))
                .collect(),
        ),
        _ => {
            let mut keys: Vec<String> = (0..rng.below(4)).map(|_| odd_string(rng)).collect();
            keys.sort();
            keys.dedup();
            Tag::Object(
                keys.into_iter()
                    .map(|k| (Cow::Owned(k), variant_tag(rng, depth - 1)))
                    .collect(),
            )
        }
    }
}

/// The variant `b` as `parquet_variant`'s reader writes what it reads of it: the type the spec holds each value in.
fn spec_text(b: &[u8]) -> String {
    match variant::read(b) {
        Value::Null => "null".into(),
        Value::Bool(v) => v.to_string(),
        Value::I64(v) | Value::Duration(v) => v.to_string(),
        Value::F64(v) => format!("{v:?}"),
        Value::Str(s) => format!("{s:?}"),
        Value::DateTime(v) => format!("ts:{v}"),
        Value::Bytes(b) => format!("bin:{b:?}"),
        Value::Array(items) => format!("[{}]", items.map(spec_text).collect::<Vec<_>>().join(",")),
        Value::Object(entries) => {
            let mut fields: Vec<(&str, String)> = entries.map(|(k, v)| (k, spec_text(v))).collect();
            fields.sort();
            let fields: Vec<String> = fields.into_iter().map(|(k, v)| format!("{k}:{v}")).collect();
            format!("{{{}}}", fields.join(","))
        }
    }
}

/// A partition term over `columns`, and whether the record can fill it.
fn term(rng: &mut Rng, columns: &[(&str, &str)]) -> (String, bool) {
    let (name, kind) = *rng.pick(columns);
    match (kind, rng.below(4)) {
        ("struct", 0) => (format!("bucket(4, {name}.region)"), true),
        ("struct", _) => (format!("{name}.region"), true),
        ("datetime", 0) => (format!("year({name})"), true),
        ("datetime", 1) => (format!("month({name})"), true),
        ("datetime", 2) => (format!("day({name})"), true),
        ("datetime", _) => (format!("hour({name})"), true),
        ("str" | "i64", 0) => (format!("bucket({}, {name})", 1 + rng.below(16)), true),
        ("str" | "i64", 1) => (format!("truncate({}, {name})", 1 + rng.below(8)), true),
        ("bool" | "str" | "i64" | "f64", _) => (name.to_string(), true),
        // A duration is a `long`, which no time transform takes; a list, bytes or a column of nothing no term here.
        ("duration", 0) => (format!("day({name})"), false),
        ("duration", _) => (format!("bucket(3, {name})"), true),
        (_, 0) => ("nope".to_string(), false),
        _ => (format!("hour({name})"), false),
    }
}

/// One writer's batches: rows by id, each refused or taken.
struct Batch {
    id: u64,
    /// `None` for a row unfit where it is cut.
    rows: Vec<Option<BTreeMap<String, Val>>>,
    /// The rows the destination refuses: the unfit ones, and those with a value a column the table holds narrower
    /// cannot hold.
    refused: Vec<usize>,
    flush: bool,
}

/// A chunk of `rows`' values of `leaves`, cut as egress cuts them, and the rows delivered from it. An unfit row
/// carries its reason.
fn delivered(record: &DataType, leaves: &[(String, &str)], rows: &[Option<BTreeMap<String, Val>>]) -> Vec<OwnedRecord> {
    let mut pool = BufferPool::new();
    let n = rows.len();
    let cell = |row: usize, leaf: &str| -> Val {
        rows[row]
            .as_ref()
            .and_then(|r| r.get(leaf).cloned())
            .unwrap_or(Val::Null)
    };
    let mut built: Vec<(String, Column)> = Vec::new();
    for (leaf, kind) in leaves {
        let column = match *kind {
            "variant" => {
                let mut b = StrBuilder::new(&mut pool, VarType::Variant, n);
                for row in 0..n {
                    match cell(row, leaf) {
                        Val::Variant(v) => b.push_grow(&mut pool, &v),
                        _ => b.push_null(),
                    }
                }
                b.finish()
            }
            "str" => {
                let values: Vec<Val> = (0..n).map(|r| cell(r, leaf)).collect();
                let mut b = StrBuilder::new(&mut pool, VarType::Str, n);
                let len = values
                    .iter()
                    .map(|v| if let Val::Str(s) = v { s.len() } else { 0 })
                    .sum();
                b.reserve(&mut pool, len);
                for v in &values {
                    match v {
                        Val::Str(s) => b.push(s.as_bytes()),
                        _ => b.push_null(),
                    }
                }
                b.finish()
            }
            kind => {
                let t = match kind {
                    "i64" => DataType::I64,
                    "duration" => DataType::Duration,
                    "datetime" => DataType::DateTime,
                    "f64" => DataType::F64,
                    _ => DataType::Bool,
                };
                let mut c = Column::alloc(&mut pool, &t, n);
                for row in 0..n {
                    let v = cell(row, leaf);
                    match (&v, kind) {
                        (Val::Long(x), "i64") => c.i64_mut().unwrap().values[row] = *x,
                        (Val::Long(x), "duration") => c.duration_mut().unwrap().values[row] = *x,
                        (Val::Micros(x), _) => c.datetime_mut().unwrap().values[row] = *x,
                        (Val::Double(bits), _) => c.f64_mut().unwrap().values[row] = f64::from_bits(*bits),
                        (Val::Bool(b), _) => {
                            let bits = c.bool_mut().unwrap();
                            if *b {
                                bits.values[row / 64] |= 1 << (row % 64);
                            } else {
                                bits.values[row / 64] &= !(1 << (row % 64));
                            }
                        }
                        _ => {}
                    }
                    if v == Val::Null {
                        match kind {
                            "i64" => c.i64_mut().unwrap().validity.set_valid(row, false),
                            "duration" => c.duration_mut().unwrap().validity.set_valid(row, false),
                            "datetime" => c.datetime_mut().unwrap().validity.set_valid(row, false),
                            "f64" => c.f64_mut().unwrap().validity.set_valid(row, false),
                            _ => c.bool_mut().unwrap().validity.set_valid(row, false),
                        }
                    }
                }
                c
            }
        };
        built.push((leaf.clone(), column));
    }
    let schema = parquet::Schema::new(record).unwrap();
    let chunk = {
        let view = RecordView::from_named(built.iter().map(|(n, c)| (n.as_str(), c)));
        let at: Vec<u32> = (0..n as u32).collect();
        Arc::new(Chunk::cut(&schema, &[(&view, &at)], 0, &mut pool))
    };
    for (_, c) in built {
        c.reclaim(&mut pool);
    }
    (0..n)
        .map(|row| OwnedRecord {
            unfit: rows[row].is_none().then(|| "no field to write".to_string()),
            columns: Some(Columns {
                chunk: chunk.clone(),
                row: row as u32,
            }),
            ..Default::default()
        })
        .collect()
}

/// Hands `batches` to `d` as egress would, and waits until every one has landed. Answers the rows refused, by batch.
fn drive(
    d: &mut dyn Destination,
    record: &DataType,
    leaves: &[(String, &str)],
    batches: &[Batch],
) -> Result<BTreeMap<u64, Vec<usize>>, String> {
    let mut refused: BTreeMap<u64, Vec<usize>> = BTreeMap::new();
    let mut waiting = Vec::new();
    let take = |done: Vec<BatchCompletion>, waiting: &mut Vec<u64>, refused: &mut BTreeMap<u64, Vec<usize>>| {
        for c in done {
            refused
                .entry(c.batch)
                .or_default()
                .extend(c.rejections.iter().map(|r| r.doc));
            waiting.retain(|b| *b != c.batch);
        }
    };
    let deadline = Instant::now() + Duration::from_secs(120);
    for batch in batches {
        let rows = delivered(record, leaves, &batch.rows);
        loop {
            let (dispatched, now) = d
                .deliver(batch.id, Cow::Borrowed(&rows))
                .map_err(|e| format!("batch {} not delivered: {e:?}", batch.id))?;
            match dispatched {
                Dispatched::NoRoom { .. } => {
                    if Instant::now() > deadline {
                        return Err(format!("batch {} never found room", batch.id));
                    }
                    let done = d.poll_completions().map_err(|e| format!("not landed: {e:?}"))?;
                    take(done, &mut waiting, &mut refused);
                    std::thread::sleep(Duration::from_millis(5));
                }
                Dispatched::InFlight => {
                    refused.entry(batch.id).or_default().extend(now.iter().map(|r| r.doc));
                    waiting.push(batch.id);
                    break;
                }
                Dispatched::Complete => {
                    refused.entry(batch.id).or_default().extend(now.iter().map(|r| r.doc));
                    break;
                }
            }
        }
        if batch.flush {
            d.flush().map_err(|e| format!("not flushed: {e:?}"))?;
        }
        let done = d.poll_completions().map_err(|e| format!("not landed: {e:?}"))?;
        take(done, &mut waiting, &mut refused);
    }
    d.flush().map_err(|e| format!("not flushed: {e:?}"))?;
    while !waiting.is_empty() {
        let done = d.poll_completions().map_err(|e| format!("not landed: {e:?}"))?;
        take(done, &mut waiting, &mut refused);
        if Instant::now() > deadline {
            return Err(format!("batches {waiting:?} never landed"));
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    Ok(refused)
}

/// A catalog client signed in as `config` says, with no proxy: the environment's would carry a loopback call away.
fn client(config: &IcebergDestinationConfig, secrets: &HeldSecrets) -> Catalog {
    let auth = match &config.sign_in {
        CatalogSignIn::None => Auth::None,
        CatalogSignIn::Bearer(token) => Auth::Bearer(secrets.value(token).unwrap()),
        CatalogSignIn::ClientCredentials {
            token_url,
            client_id,
            client_secret,
            scope,
        } => Auth::ClientCredentials {
            token_url: token_url.clone(),
            client_id: client_id.clone(),
            client_secret: secrets.value(client_secret).unwrap(),
            scope: scope.clone(),
        },
        CatalogSignIn::SigV4 { .. } => unreachable!("never drawn"),
    };
    let agent = ureq::Agent::config_builder().proxy(None).build().new_agent();
    Catalog::new(
        config.catalog.clone(),
        config.warehouse.clone(),
        auth,
        agent,
        config.stall_after / 2,
    )
}

/// A writer of `record` to the table, as `destination` builds one, its files in `store`.
fn writer(
    config: &IcebergDestinationConfig,
    secrets: &HeldSecrets,
    record: &DataType,
    store: &Arc<InMemory>,
    runtime: &Arc<tokio::runtime::Runtime>,
) -> Result<FileDestination, IoError> {
    let mut catalog = client(config, secrets);
    let (loaded, schema) = open(&mut catalog, config, record)?;
    assemble(
        config,
        record,
        catalog,
        loaded.metadata,
        schema,
        store.clone(),
        runtime.clone(),
    )
}

fn stored(name: &str) -> SecretRef {
    SecretRef::LeaderStored {
        name: name.to_string(),
        version: 1,
    }
}

/// The field at `path` among `fields`, and its id.
fn field_at<'a>(fields: &'a [NestedField], path: &[&str]) -> Option<&'a NestedField> {
    let field = fields.iter().find(|f| f.name == path[0])?;
    match (&field.field_type, path.len()) {
        (_, 1) => Some(field),
        (Type::Struct(s), _) => field_at(&s.fields, &path[1..]),
        _ => None,
    }
}

/// The dotted path of the field `id` among `fields`.
fn path_of(fields: &[NestedField], id: i32) -> Option<String> {
    fields.iter().find_map(|f| {
        if f.id == id {
            return Some(f.name.clone());
        }
        match &f.field_type {
            Type::Struct(s) => path_of(&s.fields, id).map(|p| format!("{}.{p}", f.name)),
            _ => None,
        }
    })
}

/// The value at `path` of `row` in a Parquet file's batch.
fn read_cell(columns: &StructArray, path: &str, kind: &str, row: usize) -> Result<Val, String> {
    let mut array: &dyn Array = columns;
    for name in path.split('.') {
        let s = array.as_struct_opt().ok_or(format!("{path} is under no struct"))?;
        if s.is_null(row) {
            return Ok(Val::Null);
        }
        array = s
            .column_by_name(name)
            .ok_or(format!("the file has no column {path}"))?
            .as_ref();
    }
    if array.is_null(row) {
        return Ok(Val::Null);
    }
    Ok(match kind {
        "str" => Val::Str(array.as_string::<i32>().value(row).to_string()),
        // As `spec_text`, where the table holds a variant; else its JSON text.
        "variant" => match array.as_struct_opt() {
            Some(v) => {
                let (metadata, value) = (v.column(0).as_binary::<i32>(), v.column(1).as_binary::<i32>());
                Val::Str(read_variant(metadata.value(row), value.value(row)))
            }
            None => Val::Str(array.as_string::<i32>().value(row).to_string()),
        },
        "i64" | "duration" => Val::Long(array.as_primitive::<Int64Type>().value(row)),
        "f64" => Val::Double(array.as_primitive::<Float64Type>().value(row).to_bits()),
        "datetime" | "timestamp" => Val::Micros(array.as_primitive::<TimestampMicrosecondType>().value(row)),
        "int" => Val::Long(array.as_primitive::<Int32Type>().value(row).into()),
        "float" => Val::Double(f64::from(array.as_primitive::<Float32Type>().value(row)).to_bits()),
        "date" => Val::Long(array.as_primitive::<Date32Type>().value(row).into()),
        _ => Val::Bool(array.as_boolean().value(row)),
    })
}

/// A file's rows, by id, read with `leaves`.
fn file_rows(bytes: Vec<u8>, leaves: &[(String, &str)]) -> Result<Vec<BTreeMap<String, Val>>, String> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(tokio_util::bytes::Bytes::from(bytes))
        .map_err(|e| format!("a data file does not read: {e}"))?
        .build()
        .map_err(|e| e.to_string())?;
    let mut rows = Vec::new();
    for batch in reader {
        let batch = batch.map_err(|e| format!("a data file does not read: {e}"))?;
        let columns = StructArray::from(batch);
        for row in 0..columns.len() {
            let mut values = BTreeMap::new();
            for (leaf, kind) in leaves {
                let v = match read_cell(&columns, leaf, kind, row) {
                    Ok(v) => v,
                    // A column added after the file was written reads as null.
                    Err(_) if leaf != "id" => Val::Null,
                    Err(e) => return Err(e),
                };
                values.insert(leaf.clone(), v);
            }
            rows.push(values);
        }
    }
    Ok(rows)
}

/// The table the fake holds, read back from it and from `store`, against the rows taken.
/// Err when the data file `bytes` is not written as the table's `write.*` properties ask: its codec, where this writer
/// has it, and a bloom filter on each column that asks for one.
fn written_as_asked(bytes: &[u8], properties: &BTreeMap<String, String>) -> Result<(), String> {
    use ::parquet::basic::Compression as Codec;
    use ::parquet::file::metadata::ParquetMetaDataReader;
    let metadata = ParquetMetaDataReader::new()
        .parse_and_finish(&tokio_util::bytes::Bytes::copy_from_slice(bytes))
        .map_err(|e| format!("its footer does not read: {e}"))?;
    let codec = properties.get("write.parquet.compression-codec").map(String::as_str);
    for group in metadata.row_groups() {
        for column in group.columns() {
            let written = match column.compression() {
                Codec::UNCOMPRESSED => "uncompressed",
                Codec::GZIP(_) => "gzip",
                Codec::SNAPPY => "snappy",
                Codec::LZ4_RAW => "lz4",
                Codec::ZSTD(_) => "zstd",
                other => return Err(format!("{other:?} is no codec this writer writes")),
            };
            if codec.is_some_and(|c| c != "brotli" && c != written) {
                return Err(format!(
                    "{} is {written}, and the table asks {codec:?}",
                    column.column_path()
                ));
            }
            let path = column.column_path().string();
            let bloom = format!("write.parquet.bloom-filter-enabled.column.{path}");
            if properties.get(&bloom).is_some_and(|v| v == "true") && column.bloom_filter_offset().is_none() {
                return Err(format!("{path} has no bloom filter, and the table asks one"));
            }
        }
    }
    Ok(())
}

fn check_table(
    fake: &Arc<Mutex<Fake>>,
    store: &Arc<InMemory>,
    runtime: &Arc<tokio::runtime::Runtime>,
    leaves: &[(String, &str)],
    lacked: &[(i32, Held)],
    taken: &BTreeMap<i64, BTreeMap<String, Val>>,
    reached: &mut Vec<String>,
) -> Result<(), String> {
    let table = fake.lock().unwrap().table.clone().ok_or("no table was made")?;
    let schema = table.schema().ok_or("the table has no current schema")?;
    for (leaf, _) in leaves {
        let path: Vec<&str> = leaf.split('.').collect();
        if field_at(&schema.fields, &path).is_none() {
            return Err(format!("the table's schema has no column {leaf}"));
        }
    }
    let spec = table.spec().ok_or("the table has no partition spec")?;
    let get = |uri: &str| -> Result<Vec<u8>, String> {
        let key = uri
            .strip_prefix("s3://bucket/")
            .ok_or(format!("{uri} is outside the bucket"))?;
        let store = store.clone();
        let key = Path::parse(key).map_err(|e| format!("{uri}: {e}"))?;
        runtime
            .block_on(async move { store.get(&key).await?.bytes().await })
            .map(|b| b.to_vec())
            .map_err(|e| format!("{uri}: {e}"))
    };
    let mut seen: BTreeMap<i64, usize> = BTreeMap::new();
    // Each live file's row ids, where the table tracks row lineage: [first, end).
    let mut row_ids: Vec<(i64, i64, String)> = Vec::new();
    if let Some(snapshot) = table.current_snapshot() {
        let list = manifest::read_manifest_list(&get(&snapshot.manifest_list)?)?;
        // Two live files of one partition from one commit: the first rolled at its size.
        let mut written = std::collections::BTreeSet::new();
        for m in &list {
            for entry in manifest::read_manifest(&get(&m.manifest_path)?, m)? {
                if entry.status == Status::Deleted {
                    continue;
                }
                if !written.insert(format!("{:?} {:?}", entry.snapshot_id, entry.partition)) {
                    reached.push("file rolled".into());
                }
                if table.format_version >= 3 {
                    let first = entry
                        .first_row_id
                        .ok_or(format!("{} has no first row id in a version 3 table", entry.file_path))?;
                    row_ids.push((first, first + entry.record_count, entry.file_path.clone()));
                }
                if !entry.file_path.starts_with("s3://bucket/wh/t/data/") {
                    return Err(format!("a data file is at {}", entry.file_path));
                }
                // S3 stores no longer key.
                if entry.file_path.len() - "s3://bucket/".len() > 1024 {
                    return Err(format!("a data file's key is {} bytes long", entry.file_path.len()));
                }
                if entry.file_path.contains("/partition-") {
                    reached.push("partition path shortened".into());
                }
                let bytes = get(&entry.file_path)?;
                written_as_asked(&bytes, &table.properties).map_err(|e| format!("{}: {e}", entry.file_path))?;
                holds_lacked(&bytes, lacked).map_err(|e| format!("{}: {e}", entry.file_path))?;
                let rows = file_rows(bytes, leaves)?;
                if rows.len() as i64 != entry.record_count {
                    return Err(format!(
                        "{} holds {} rows, and its manifest says {}",
                        entry.file_path,
                        rows.len(),
                        entry.record_count
                    ));
                }
                for row in &rows {
                    let Some(Val::Long(id)) = row.get("id") else {
                        return Err(format!("a row of {} has no id", entry.file_path));
                    };
                    *seen.entry(*id).or_default() += 1;
                    let Some(sent) = taken.get(id) else {
                        return Err(format!("row {id} was never taken, and is in {}", entry.file_path));
                    };
                    for (leaf, _) in leaves {
                        let mut want = sent.get(leaf).cloned().unwrap_or(Val::Null);
                        if let Val::Variant(b) = &want {
                            let path: Vec<&str> = leaf.split('.').collect();
                            let held = field_at(&schema.fields, &path).map(|f| &f.field_type);
                            want = match held {
                                Some(Type::Primitive(Primitive::Other(t))) if t == "variant" => {
                                    reached.push("variant written as variant".into());
                                    match variant::read(b) {
                                        Value::Null => Val::Null,
                                        _ => Val::Str(spec_text(b)),
                                    }
                                }
                                _ => {
                                    reached.push("variant written as text".into());
                                    let mut doc = Vec::new();
                                    match json::write_variant(b, &mut doc) {
                                        true => Val::Str(String::from_utf8(doc).unwrap()),
                                        false => Val::Null,
                                    }
                                }
                            };
                        }
                        if row.get(leaf) != Some(&want) {
                            return Err(format!("row {id}'s {leaf} reads {:?}, not {want:?}", row.get(leaf)));
                        }
                    }
                    for (i, field) in spec.fields.iter().enumerate() {
                        let path = path_of(&schema.fields, field.source_id)
                            .ok_or(format!("partition field {} has no source", field.name))?;
                        let source = sent.get(&path).cloned().unwrap_or(Val::Null);
                        let kind = leaves.iter().find(|(leaf, _)| *leaf == path).map_or("", |(_, k)| *k);
                        let want = literal_as(&source, kind).and_then(|l| apply(&field.transform, &l));
                        if want.is_none() {
                            reached.push("partition null".into());
                        }
                        let got = entry.partition.get(i).cloned().flatten();
                        // Compared as written, so NaN equals itself.
                        if format!("{got:?}") != format!("{want:?}") {
                            return Err(format!(
                                "row {id} is in {}, whose {} is {got:?}, and its own is {want:?}",
                                entry.file_path, field.name
                            ));
                        }
                    }
                }
            }
        }
    }
    row_ids.sort();
    for pair in row_ids.windows(2) {
        if pair[0].1 > pair[1].0 {
            return Err(format!("{} and {} share row ids", pair[0].2, pair[1].2));
        }
    }
    if let Some((_, end, file)) = row_ids.last()
        && table.next_row_id.is_some_and(|next| next < *end)
    {
        return Err(format!(
            "{file} holds row ids up to {end}, past the table's next row id {:?}",
            table.next_row_id
        ));
    }
    if !row_ids.is_empty() {
        reached.push("row ids assigned".into());
    }
    for id in taken.keys() {
        match seen.get(id) {
            Some(1) => {}
            n => return Err(format!("row {id} is in the table {} times", n.unwrap_or(&0))),
        }
    }
    Ok(())
}

/// One seed's run: what it reached, or what went wrong.
fn run(seed: u64, reached: &mut Vec<String>) -> Result<(), String> {
    let mut rng = Rng::seeded(seed);
    let mut columns: Vec<(&str, &str)> = Vec::new();
    let mut pool: Vec<(&str, &str)> = COLUMNS.to_vec();
    for _ in 0..1 + rng.below(5) {
        let at = rng.below(pool.len());
        columns.push(pool.remove(at));
    }
    for (_, kind) in &columns {
        reached.push(format!("type {kind}"));
    }
    let record = record_of(&columns);
    let leaves = leaves(&columns);

    let fake = Arc::new(Mutex::new(Fake::default()));
    let url = serve(fake.clone());
    let store = Arc::new(InMemory::new());
    let runtime = objects::runtime().map_err(|e| e.message)?;

    let mut secrets = HeldSecrets::default();
    secrets.set_stored([
        ("token".to_string(), 1, "t0ken".to_string()),
        ("client-secret".to_string(), 1, "s3cr&t".to_string()),
    ]);
    // Credited once every call is seen to carry it.
    let (sign_in, signed_as, sign_in_reached) = match rng.below(4) {
        0 => (CatalogSignIn::Bearer(stored("token")), "Bearer t0ken", "sign-in bearer"),
        1 => {
            let at_url = rng.chance(2);
            (
                CatalogSignIn::ClientCredentials {
                    token_url: at_url.then(|| format!("{url}/v1/oauth/tokens")),
                    client_id: "app".into(),
                    client_secret: stored("client-secret"),
                    scope: rng.chance(2).then(|| "PRINCIPAL_ROLE:ALL".into()),
                },
                "Bearer fake-token",
                if at_url {
                    "sign-in client credentials at a token URL"
                } else {
                    "sign-in client credentials"
                },
            )
        }
        _ => (CatalogSignIn::None, "", "sign-in none"),
    };
    let scope = match &sign_in {
        CatalogSignIn::ClientCredentials { scope, .. } => scope.clone(),
        _ => None,
    };
    let warehouse = rng.chance(3).then(|| "wh".to_string());
    let namespace: Vec<String> = (0..1 + rng.below(3))
        .map(|_| rng.pick(&["logs", "a b", "ünï", "x/y", "%"]).to_string())
        .collect();
    if namespace.len() > 1 {
        reached.push("namespace nested".into());
    }
    let (compression, codec) = *rng.pick(&[
        (Compression::None, "none"),
        (Compression::Gzip, "gzip"),
        (Compression::Snappy, "snappy"),
        (Compression::Lz4, "lz4"),
        (Compression::Zstd, "zstd"),
    ]);
    reached.push(format!("compression {codec}"));
    let in_flight = 1 + rng.below(4);
    reached.push(if in_flight == 1 {
        "inFlight 1".into()
    } else {
        "inFlight several".into()
    });
    let max_object_bytes = *rng.pick(&[64 << 10, 100 << 10, 1 << 20, 64 << 20]);
    if max_object_bytes < 1 << 20 {
        reached.push("maxObjectBytes small".into());
    }

    let mut partition_by = Vec::new();
    let mut fits = true;
    let mut names = Vec::new();
    for _ in 0..rng.below(3) {
        let (term, ok) = term(&mut rng, &columns);
        // Two terms naming one partition field are refused at save.
        let (transform, path) = partition_term(&term).map_err(|e| format!("drew an unreadable term: {e}"))?;
        let name = partition_name(&transform, &path);
        if names.contains(&name) {
            continue;
        }
        names.push(name);
        fits &= ok;
        partition_by.push(term);
    }

    let config = IcebergDestinationConfig {
        catalog: url.clone(),
        warehouse,
        sign_in,
        namespace,
        table: rng.pick(&["web", "t", "Web Logs", "ünï"]).to_string(),
        partition_by: partition_by.clone(),
        storage: None,
        compression,
        tls: None,
        in_flight,
        max_object_bytes,
        buffer_bytes: (4 << 20) + rng.below(4 << 20),
        stall_after: Duration::from_secs(5 + rng.below(6) as u64),
        file_interval: Duration::from_secs(60),
    };

    // An earlier writer's table: its record a part of this one's, maybe with a column of another type, and no
    // partitioning.
    let mut evolution_refused = None;
    // The leaves the earlier table holds narrower, and as what.
    let mut narrow: BTreeMap<String, &'static str> = BTreeMap::new();
    // The ids of the columns the earlier table has and the record lacks, and whether each has a write default.
    let mut lacked: Vec<(i32, Held)> = Vec::new();
    let mut version_3 = false;
    let existing = rng.chance(3);
    if existing {
        reached.push("existing table".into());
        let mut earlier: Vec<(&str, &str)> = columns.iter().copied().filter(|_| rng.chance(2)).collect();
        if let Some(first) = earlier.first_mut()
            && rng.chance(4)
            && !matches!(first.1, "null" | "variant")
        {
            first.1 = if first.1 == "str" { "i64" } else { "str" };
            evolution_refused = Some(first.0);
        }
        if earlier.len() < columns.len() {
            reached.push("existing table evolved".into());
        }
        let earlier_config = IcebergDestinationConfig {
            partition_by: Vec::new(),
            ..config.clone()
        };
        let mut catalog = client(&earlier_config, &secrets);
        open(&mut catalog, &earlier_config, &record_of(&earlier)).map_err(|e| format!("the earlier table: {e:?}"))?;
        // As an engine of format version 3 made it, or upgraded it, its row ids already counted up to somewhere.
        if rng.chance(2) {
            let mut fake = fake.lock().unwrap();
            let table = fake.table.as_mut().ok_or("the earlier table was not made")?;
            table.format_version = 3;
            table.next_row_id = Some(rng.below(1000) as i64);
            version_3 = true;
            reached.push("format version 3".into());
        }
        // As another engine made it: some of its columns narrower than the record types them.
        if evolution_refused.is_none() && (earlier.iter().any(|(_, k)| *k == "datetime") || !rng.chance(4)) {
            let mut fake = fake.lock().unwrap();
            let table = fake.table.as_mut().ok_or("the earlier table was not made")?;
            let current = table.current_schema_id;
            let mut sources: Vec<(i32, &str)> = Vec::new();
            let schema = (table.schemas.iter_mut())
                .find(|s| s.schema_id == current)
                .ok_or("the earlier table has no current schema")?;
            for (name, kind) in &earlier {
                let Some(field) = schema.fields.iter_mut().find(|f| f.name == *name) else {
                    continue;
                };
                let (field, leaf, to) = match *kind {
                    "i64" => (field, name.to_string(), (Primitive::Int, "int")),
                    "f64" => (field, name.to_string(), (Primitive::Float, "float")),
                    // By the seed, not a draw: so few seeds hold a time that a draw can miss either.
                    "datetime" if (seed / 2).is_multiple_of(2) => (field, name.to_string(), (Primitive::Date, "date")),
                    "datetime" => (field, name.to_string(), (Primitive::Timestamp, "timestamp")),
                    "struct" => {
                        let Type::Struct(s) = &mut field.field_type else {
                            return Err(format!("{name} is no struct in the earlier table"));
                        };
                        let zone = (s.fields.iter_mut())
                            .find(|f| f.name == "zone")
                            .ok_or(format!("{name} has no zone"))?;
                        reached.push("narrowed nested".into());
                        (zone, format!("{name}.zone"), (Primitive::Int, "int"))
                    }
                    _ => continue,
                };
                // A table seldom holds a time: one that does always narrows it.
                if *kind != "datetime" && rng.chance(5) {
                    continue;
                }
                field.field_type = Type::Primitive(to.0);
                reached.push(format!("narrowed {}", to.1));
                if !leaf.contains('.') {
                    sources.push((field.id, to.1));
                }
                narrow.insert(leaf, to.1);
            }
            // Partitioned by a column it holds narrower, as another engine may have made it.
            // A time where the table holds one, always, as so few seeds do; else by the seed, not a draw.
            let time = (sources.iter().copied()).find(|(_, to)| matches!(*to, "date" | "timestamp"));
            if time.is_some() || (!sources.is_empty() && !rng.chance(3)) {
                let (source_id, to) = time.unwrap_or(sources[(seed / 4) as usize % sources.len()]);
                let transforms: &[Transform] = match to {
                    "int" => &[Transform::Identity, Transform::Bucket(4), Transform::Truncate(10)],
                    "date" => &[
                        Transform::Identity,
                        Transform::Year,
                        Transform::Day,
                        Transform::Bucket(4),
                    ],
                    "timestamp" => &[
                        Transform::Identity,
                        Transform::Month,
                        Transform::Hour,
                        Transform::Bucket(4),
                    ],
                    _ => &[Transform::Identity],
                };
                let transform = rng.pick(transforms).clone();
                table.partition_specs.push(PartitionSpec {
                    spec_id: 1,
                    fields: vec![PartitionField {
                        source_ids: None,
                        name: "narrowed".into(),
                        transform,
                        source_id,
                        field_id: 1000,
                    }],
                });
                table.default_spec_id = 1;
                table.last_partition_id = 1000;
                reached.push(format!("partition narrowed {to}"));
            }
        }
        // As another engine evolved it: a column the record does not write, and one in a struct it does. Always at
        // version 3, which so few seeds reach that a draw can miss one of its types.
        if version_3 || rng.chance(2) {
            let mut fake = fake.lock().unwrap();
            let table = fake.table.as_mut().ok_or("the earlier table was not made")?;
            let current = table.current_schema_id;
            let mut last = table.last_column_id;
            let mut next = || {
                last += 1;
                last
            };
            let schema = (table.schemas.iter_mut())
                .find(|s| s.schema_id == current)
                .ok_or("the earlier table has no current schema")?;
            let kind = match version_3 && rng.chance(2) {
                true => *rng.pick(LACKED_3),
                false => *rng.pick(LACKED),
            };
            let id = next();
            let mut field_type = lacked_type(kind, &mut next);
            // A version 3 table's column may have a write default, which a struct's fields have too.
            let default = lacked_default(kind).filter(|_| version_3 && rng.chance(2));
            let defaulted = default.is_some();
            let mut other = serde_json::Map::new();
            if let Some(default) = default {
                other.insert("write-default".into(), default);
                if let Type::Struct(s) = &mut field_type {
                    s.fields[0].other.insert("write-default".into(), serde_json::json!(0.5));
                }
                reached.push("lacked default".into());
            }
            let required = defaulted && rng.chance(2);
            if required {
                reached.push("lacked required default".into());
            }
            schema.fields.push(NestedField {
                id,
                name: format!("lacked_{kind}"),
                required,
                field_type,
                doc: None,
                other: other.into_iter().collect(),
            });
            let held = match (kind, defaulted) {
                ("unknown", _) => Held::Nothing,
                (_, true) => Held::Default,
                (_, false) => Held::Null,
            };
            lacked.push((id, held));
            reached.push(format!("lacked {kind}"));
            if let Some(Type::Struct(ctx)) = (schema.fields.iter_mut())
                .find(|f| f.name == "ctx")
                .map(|f| &mut f.field_type)
                && rng.chance(2)
            {
                let id = next();
                ctx.fields.push(NestedField {
                    id,
                    name: "lacked".into(),
                    required: false,
                    field_type: Type::Primitive(Primitive::String),
                    doc: None,
                    other: Default::default(),
                });
                lacked.push((id, Held::Null));
                reached.push("lacked nested".into());
            }
            table.last_column_id = last;
        }
        // As another engine set it: how the table's files are written.
        if rng.chance(2) {
            let mut fake = fake.lock().unwrap();
            let table = fake.table.as_mut().ok_or("the earlier table was not made")?;
            let mut set = |key: &str, value: &str| table.properties.insert(format!("write.{key}"), value.to_string());
            let codec = *rng.pick(&["zstd", "gzip", "snappy", "lz4", "uncompressed", "brotli"]);
            set("parquet.compression-codec", codec);
            reached.push(format!("table codec {codec}"));
            if rng.chance(2) {
                set("parquet.compression-level", &(1 + rng.below(9)).to_string());
                reached.push("table codec level".into());
            }
            if rng.chance(2) {
                set("parquet.row-group-size-bytes", rng.pick(&["4096", "65536", "1048576"]));
                reached.push("table row group size".into());
            }
            if rng.chance(3) {
                set("target-file-size-bytes", rng.pick(&["65536", "1048576"]));
                reached.push("table target file size".into());
            }
            let leaves: Vec<&str> = (earlier.iter())
                .filter(|(_, kind)| matches!(*kind, "str" | "i64" | "f64" | "bool" | "datetime" | "bytes"))
                .map(|(name, _)| *name)
                .collect();
            if !leaves.is_empty() && rng.chance(2) {
                let column = *rng.pick(&leaves);
                set(&format!("parquet.bloom-filter-enabled.column.{column}"), "true");
                if rng.chance(2) {
                    set(&format!("parquet.bloom-filter-fpp.column.{column}"), "0.05");
                }
                reached.push("table bloom filter".into());
            }
        }
    } else {
        reached.push("new table".into());
    }
    // A table that exists keeps its own partitioning.
    let refused_by_terms = !existing && !fits;
    if !existing && fits {
        for t in &partition_by {
            let (transform, path) = partition_term(t).map_err(|e| format!("drew an unreadable term: {e}"))?;
            if path.len() > 1 {
                reached.push("partition nested".into());
            }
            let name = transform.to_string();
            reached.push(format!("partition {}", name.split('[').next().unwrap_or_default()));
        }
    }

    let faults: Vec<Fault> = (0..rng.below(4))
        .map(|_| *rng.pick(&[Fault::Lost, Fault::Late, Fault::Dropped, Fault::Refused]))
        .collect();
    fake.lock().unwrap().faults = faults.clone();

    let writers = if rng.chance(3) { 2 } else { 1 };
    let mut built = Vec::new();
    for _ in 0..writers {
        match writer(&config, &secrets, &record, &store, &runtime) {
            Ok(d) => built.push(d),
            Err(e) if refused_by_terms || evolution_refused.is_some() => {
                // Refused for what was drawn, and not for anything else that went wrong.
                let (why, named) = match evolution_refused {
                    Some(column) => ("evolution refused", e.message.contains(column)),
                    None => ("partitioning refused", e.message.contains("partitionBy")),
                };
                if !named {
                    return Err(format!("expected {why}, and the writer was refused for: {e:?}"));
                }
                reached.push(why.into());
                let fake = fake.lock().unwrap();
                if fake.table.as_ref().is_some_and(|t| t.current_snapshot().is_some()) {
                    return Err(format!("a refused writer committed: {e:?}"));
                }
                return Ok(());
            }
            Err(e) => return Err(format!("does not build: {e:?}")),
        }
    }
    if refused_by_terms || evolution_refused.is_some() {
        return Err(format!(
            "built a writer for {partition_by:?}, which the record cannot fill, or over a column of another type"
        ));
    }
    if writers == 2 {
        reached.push("writers 2".into());
    }

    // Each writer's batches, ids unique across them. A row is None when it is unfit.
    let mut next_id = 1i64;
    let mut taken: BTreeMap<i64, BTreeMap<String, Val>> = BTreeMap::new();
    let mut plans: Vec<Vec<Batch>> = Vec::new();
    let mut batch_id = 0;
    let wide = rng.chance(3);
    for _ in 0..writers {
        let count = 1 + rng.below(4);
        if count > 1 {
            reached.push("batches several".into());
        }
        let mut batches = Vec::new();
        for _ in 0..count {
            batch_id += 1;
            let all_unfit = rng.chance(12);
            let mut refused = Vec::new();
            let rows: Vec<Option<BTreeMap<String, Val>>> = (0..1 + rng.below(40))
                .map(|i| {
                    if all_unfit || rng.chance(15) {
                        reached.push("row unfit".into());
                        refused.push(i);
                        return None;
                    }
                    let id = next_id;
                    next_id += 1;
                    let mut row = BTreeMap::new();
                    row.insert("id".to_string(), Val::Long(id));
                    for (leaf, kind) in leaves.iter().skip(1) {
                        let v = if wide && *kind == "str" {
                            Val::Str("w".repeat(2000 + rng.below(4000)))
                        } else {
                            value(&mut rng, kind)
                        };
                        row.insert(leaf.clone(), v);
                    }
                    // What the table reads back: each value a narrower column holds as that column holds it.
                    let mut read = row.clone();
                    for (leaf, to) in &narrow {
                        match read.get(leaf).map(|v| narrowed(v, to)) {
                            Some(Some(v)) => {
                                read.insert(leaf.clone(), v);
                            }
                            Some(None) => {
                                reached.push("row unfit for a narrowed column".into());
                                refused.push(i);
                                return Some(row);
                            }
                            None => {}
                        }
                    }
                    taken.insert(id, read);
                    Some(row)
                })
                .collect();
            if all_unfit {
                reached.push("batch all unfit".into());
            }
            let flush = rng.chance(3);
            if flush {
                reached.push("flush between".into());
            }
            batches.push(Batch {
                id: batch_id,
                rows,
                refused,
                flush,
            });
        }
        plans.push(batches);
    }

    let results: Vec<Result<BTreeMap<u64, Vec<usize>>, String>> = std::thread::scope(|scope| {
        let handles: Vec<_> = (built.into_iter().zip(&plans))
            .map(|(mut d, batches)| {
                let (record, leaves) = (&record, &leaves);
                scope.spawn(move || drive(&mut d, record, leaves, batches))
            })
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().unwrap_or_else(|_| Err("a writer panicked".into())))
            .collect()
    });
    for (refused, batches) in results.into_iter().zip(&plans) {
        let refused = refused?;
        for batch in batches {
            let want = batch.refused.clone();
            let mut got = refused.get(&batch.id).cloned().unwrap_or_default();
            got.sort();
            if got != want {
                return Err(format!("batch {}: refused rows {got:?}, not {want:?}", batch.id));
            }
        }
    }

    // A leaf the table holds narrower reads back as that type.
    let read: Vec<(String, &str)> = (leaves.iter())
        .map(|(leaf, kind)| (leaf.clone(), narrow.get(leaf).copied().unwrap_or(kind)))
        .collect();
    check_table(&fake, &store, &runtime, &read, &lacked, &taken, reached)?;
    let fake = fake.lock().unwrap();
    if let Some(wrong) = fake.signed.iter().find(|s| *s != signed_as) {
        return Err(format!(
            "a call for the table signed in as {wrong:?}, not {signed_as:?}"
        ));
    }
    if signed_as == "Bearer fake-token" && !fake.token_asks.iter().any(|a| a.contains("client_id=app")) {
        return Err(format!("no token was asked for as the client: {:?}", fake.token_asks));
    }
    reached.push(sign_in_reached.into());
    if let Some(scope) = scope {
        // Form-encoded, as the token request carries it.
        let asked = scope.replace(':', "%3A");
        if !fake.token_asks.iter().all(|a| a.contains(&format!("scope={asked}"))) {
            return Err(format!(
                "a token was asked for without the scope {scope:?}: {:?}",
                fake.token_asks
            ));
        }
        reached.push("scope".into());
    }
    if let Some(warehouse) = &config.warehouse {
        if !fake
            .configs
            .iter()
            .all(|c| c.contains(&format!("warehouse={warehouse}")))
        {
            return Err(format!(
                "the catalog's config was asked for without the warehouse: {:?}",
                fake.configs
            ));
        }
        reached.push("warehouse".into());
    }
    // A fault is reached once a commit has met it.
    for f in &faults[..faults.len() - fake.faults.len()] {
        reached.push(format!("fault {f:?}"));
    }
    Ok(())
}

#[test]
fn seeded_runs_write_every_row_once() {
    let mut coverage = Coverage::new(COVERED.iter().copied());
    fuzz::run(SEEDS, |seed| {
        let mut reached = Vec::new();
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run(seed, &mut reached)));
        match outcome {
            Ok(Ok(())) => {
                for name in reached {
                    coverage.reach(name);
                }
                Ok(())
            }
            Ok(Err(why)) => Err(why),
            Err(panic) => Err(format!(
                "panicked: {}",
                (panic.downcast_ref::<String>().cloned())
                    .or_else(|| panic.downcast_ref::<&str>().map(|s| s.to_string()))
                    .unwrap_or_default()
            )),
        }
    });
    coverage.assert_complete();
}
