//! Manifests and manifest lists, in Avro, as format version 2 writes them. A manifest lists data files with their
//! partition values and column statistics; a manifest list lists a snapshot's manifests with a summary of each.
//! Every Avro field carries its Iceberg field id, which is how Trino and Spark resolve them.
//!
//! ```mermaid
//! flowchart LR
//!   D[DataFileEntry<br/>stats from the footer] -->|write_manifest| M[manifest]
//!   M -->|summarize| F[ManifestFile]
//!   P[parent's ManifestFiles] -->|merge_plan, merge| F
//!   F -->|write_manifest_list| L[manifest list]
//! ```
//!
//! A sequence number is assigned only when a snapshot commits, so what this snapshot adds is written without one:
//! a new manifest's `None` is written as the snapshot's number in the list, and an added entry's `None` stays null
//! in its manifest, where a reader inherits it from the list.

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};

use apache_avro::types::Value;
use apache_avro::{Codec, DeflateSettings, Reader, Schema as AvroSchema, Writer};
use aws_lc_rs::rand::{SecureRandom, SystemRandom};
use parquet::file::metadata::ParquetMetaData;
use serde_json::{Value as Json, json};

use crate::io::table::{self, Bound};

use super::metadata::{FORMAT_VERSION, Literal, NestedField, PartitionSpec, Primitive, Schema, Transform, Type};

/// `commit.manifest.target-size-bytes`'s default: the size a merged manifest is packed up to.
pub const TARGET_SIZE_BYTES: u64 = 8 * 1024 * 1024;

/// `commit.manifest.min-count-to-merge`'s default: how many small manifests of one spec there must be before any
/// are merged.
pub const MIN_COUNT_TO_MERGE: usize = 100;

/// The code points of a string bound, and the bytes of a binary one; Iceberg's `write.metadata.metrics.default`.
const TRUNCATE_LENGTH: usize = 16;

/// What an entry says happened to its file in the snapshot that wrote it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Status {
    Existing,
    Added,
    Deleted,
}

impl Status {
    fn code(self) -> i32 {
        match self {
            Status::Existing => 0,
            Status::Added => 1,
            Status::Deleted => 2,
        }
    }

    fn from_code(code: i32) -> Result<Status, String> {
        match code {
            0 => Ok(Status::Existing),
            1 => Ok(Status::Added),
            2 => Ok(Status::Deleted),
            other => Err(format!("unknown manifest entry status {other}")),
        }
    }
}

/// Column statistics, each keyed by field id. Bounds are single-value serialized (the spec's appendix D).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Stats {
    pub column_sizes: BTreeMap<i32, i64>,
    pub value_counts: BTreeMap<i32, i64>,
    pub null_value_counts: BTreeMap<i32, i64>,
    pub lower_bounds: BTreeMap<i32, Vec<u8>>,
    pub upper_bounds: BTreeMap<i32, Vec<u8>>,
}

/// One data file in a manifest. A `None` sequence number or snapshot id is one inherited from the manifest's entry
/// in the manifest list.
#[derive(Clone, Debug, PartialEq)]
pub struct DataFileEntry {
    pub status: Status,
    pub snapshot_id: Option<i64>,
    pub sequence_number: Option<i64>,
    pub file_sequence_number: Option<i64>,
    pub file_path: String,
    pub file_format: String,
    /// One value per partition field, in the spec's order.
    pub partition: Vec<Option<Literal>>,
    pub record_count: i64,
    pub file_size_in_bytes: i64,
    pub stats: Stats,
    /// The data file's fields this writer does not make and another writer set, by name, their union's branch taken:
    /// a merge writes them back unchanged. Each is one of [`CARRIED`].
    pub carried: Vec<(String, Value)>,
}

/// The data file's optional fields this writer never fills, which a merge carries from the manifest it read.
const CARRIED: [&str; 5] = [
    "nan_value_counts",
    "key_metadata",
    "split_offsets",
    "equality_ids",
    "sort_order_id",
];

impl DataFileEntry {
    /// A Parquet file the snapshot `snapshot_id` adds. Its sequence numbers are the snapshot's, assigned at commit.
    pub fn added(
        snapshot_id: i64,
        file_path: String,
        partition: Vec<Option<Literal>>,
        record_count: i64,
        file_size_in_bytes: i64,
        stats: Stats,
    ) -> DataFileEntry {
        DataFileEntry {
            status: Status::Added,
            snapshot_id: Some(snapshot_id),
            sequence_number: None,
            file_sequence_number: None,
            file_path,
            file_format: "PARQUET".to_string(),
            partition,
            record_count,
            file_size_in_bytes,
            stats,
            carried: Vec::new(),
        }
    }
}

/// What a manifest holds: data files, or delete files, which this writer never writes but keeps in a list.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Content {
    Data,
    Deletes,
}

/// A manifest list's entry for one manifest. A count is `None` when the list it was read from, written before format
/// version 2, did not hold it: [`fill_counts`] sets it from the manifest's entries, and it must be set before the
/// entry is written.
#[derive(Clone, Debug, PartialEq)]
pub struct ManifestFile {
    pub manifest_path: String,
    pub manifest_length: i64,
    pub partition_spec_id: i32,
    pub content: Content,
    /// `None` for a manifest this snapshot adds: the snapshot's sequence number, assigned at commit.
    pub sequence_number: Option<i64>,
    /// The least data sequence number of the manifest's live files; `None` when that is the snapshot's own.
    pub min_sequence_number: Option<i64>,
    pub added_snapshot_id: i64,
    pub added_files_count: Option<i32>,
    pub existing_files_count: Option<i32>,
    pub deleted_files_count: Option<i32>,
    pub added_rows_count: Option<i64>,
    pub existing_rows_count: Option<i64>,
    pub deleted_rows_count: Option<i64>,
    /// One per partition field, in the spec's order.
    pub partitions: Vec<FieldSummary>,
}

/// The values one partition field takes across a manifest's entries. Bounds are single-value serialized.
#[derive(Clone, Debug, PartialEq)]
pub struct FieldSummary {
    pub contains_null: bool,
    /// Known only for a floating-point field.
    pub contains_nan: Option<bool>,
    pub lower_bound: Option<Vec<u8>>,
    pub upper_bound: Option<Vec<u8>>,
}

/// Column statistics from a Parquet footer. A leaf without an Iceberg field id is skipped; a leaf inside a list or a
/// map has sizes and counts but no bounds, and so does one whose row groups do not all carry min and max.
pub fn stats(metadata: &ParquetMetaData, schema: &Schema) -> Stats {
    let mut types = HashMap::new();
    bounded_types(&schema.fields, &mut types);
    let mut stats = Stats::default();
    let columns = metadata.file_metadata().schema_descr().num_columns();
    for column in 0..columns {
        let descr = metadata.file_metadata().schema_descr().column(column);
        let info = descr.self_type().get_basic_info();
        if !info.has_id() {
            continue;
        }
        let id = info.id();
        let ty = types.get(&id);
        let kind = ty.and_then(Primitive::kind);
        let leaf = table::column_stats(metadata, column, kind.as_ref());
        stats.column_sizes.insert(id, leaf.size);
        stats.value_counts.insert(id, leaf.values);
        if let Some(nulls) = leaf.nulls {
            stats.null_value_counts.insert(id, nulls);
        }
        if let (Some(ty), Some((lower, upper))) = (ty, leaf.bounds) {
            if let Some(lower) = serialize_bound(lower, ty, false) {
                stats.lower_bounds.insert(id, lower);
            }
            if let Some(upper) = serialize_bound(upper, ty, true) {
                stats.upper_bounds.insert(id, upper);
            }
        }
    }
    stats
}

/// The primitive columns of `fields`, walked through structs only: what is inside a list or a map has no bounds.
fn bounded_types(fields: &[NestedField], into: &mut HashMap<i32, Primitive>) {
    for field in fields {
        match &field.field_type {
            Type::Primitive(p) => {
                into.insert(field.id, p.clone());
            }
            Type::Struct(s) => bounded_types(&s.fields, into),
            Type::List(_) | Type::Map(_) => {}
        }
    }
}

fn serialize_bound(bound: Bound, ty: &Primitive, upper: bool) -> Option<Vec<u8>> {
    match bound {
        Bound::Boolean(b) => Some(vec![b as u8]),
        Bound::Int(v) => Some(v.to_le_bytes().to_vec()),
        Bound::Long(v) => Some(v.to_le_bytes().to_vec()),
        Bound::Float(v) => Some(v.to_le_bytes().to_vec()),
        Bound::Double(v) => Some(v.to_le_bytes().to_vec()),
        Bound::Bytes(bytes) if *ty == Primitive::String => {
            let s = std::str::from_utf8(&bytes).ok()?;
            let truncated = if upper {
                truncate_string_upper(s)
            } else {
                Some(truncate_string_lower(s))
            };
            truncated.map(String::into_bytes)
        }
        Bound::Bytes(bytes) if upper => truncate_binary_upper(&bytes),
        Bound::Bytes(bytes) => Some(bytes[..bytes.len().min(TRUNCATE_LENGTH)].to_vec()),
    }
}

fn truncate_string_lower(s: &str) -> String {
    s.chars().take(TRUNCATE_LENGTH).collect()
}

/// The least string of at most 16 code points that is not below `s`, or `None` when there is none.
fn truncate_string_upper(s: &str) -> Option<String> {
    let mut prefix: Vec<char> = s.chars().take(TRUNCATE_LENGTH + 1).collect();
    if prefix.len() <= TRUNCATE_LENGTH {
        return Some(s.to_string());
    }
    prefix.truncate(TRUNCATE_LENGTH);
    while let Some(last) = prefix.pop() {
        // Surrogates are no code points a string can hold, so the one after U+D7FF is U+E000.
        let next = match last as u32 + 1 {
            0xD800 => 0xE000,
            n => n,
        };
        if let Some(next) = char::from_u32(next) {
            prefix.push(next);
            return Some(prefix.into_iter().collect());
        }
    }
    None
}

/// The least byte string of at most 16 bytes that is not below `bytes`, or `None` when there is none.
fn truncate_binary_upper(bytes: &[u8]) -> Option<Vec<u8>> {
    if bytes.len() <= TRUNCATE_LENGTH {
        return Some(bytes.to_vec());
    }
    let mut prefix = bytes[..TRUNCATE_LENGTH].to_vec();
    while let Some(last) = prefix.pop() {
        if last < u8::MAX {
            prefix.push(last + 1);
            return Some(prefix);
        }
    }
    None
}

/// A manifest of data files, for a table whose current schema is `schema`, partitioned by `spec`.
pub fn write_manifest(schema: &Schema, spec: &PartitionSpec, entries: &[DataFileEntry]) -> Result<Vec<u8>, String> {
    let types = partition_types(schema, spec)?;
    let names: Vec<String> = spec.fields.iter().map(|f| avro_name(&f.name)).collect();
    let partition_fields = spec
        .fields
        .iter()
        .zip(&names)
        .zip(&types)
        .map(|((field, name), ty)| Ok(optional(name, field.field_id, avro_primitive(ty)?)))
        .collect::<Result<Vec<_>, String>>()?;
    let records = entries
        .iter()
        .map(|entry| entry_value(entry, &names, &types))
        .collect::<Result<Vec<_>, String>>()?;
    let metadata = [
        ("schema", serde_json::to_string(schema).map_err(|e| e.to_string())?),
        ("schema-id", schema.schema_id.to_string()),
        (
            "partition-spec",
            serde_json::to_string(&spec.fields).map_err(|e| e.to_string())?,
        ),
        ("partition-spec-id", spec.spec_id.to_string()),
        ("format-version", FORMAT_VERSION.to_string()),
        ("content", "data".to_string()),
    ];
    write_container(&manifest_schema(partition_fields), &metadata, records)
}

/// The list entry for a manifest this snapshot just wrote, at `path`, `length` bytes long, for a table whose current
/// schema is `schema`.
pub fn summarize(
    path: String,
    length: u64,
    schema: &Schema,
    spec: &PartitionSpec,
    snapshot_id: i64,
    entries: &[DataFileEntry],
) -> ManifestFile {
    ManifestFile {
        manifest_path: path,
        manifest_length: length as i64,
        partition_spec_id: spec.spec_id,
        content: Content::Data,
        sequence_number: None,
        min_sequence_number: entries
            .iter()
            .filter(|e| e.status != Status::Deleted)
            .filter_map(|e| e.sequence_number)
            .min(),
        added_snapshot_id: snapshot_id,
        added_files_count: Some(files_count(entries, Status::Added)),
        existing_files_count: Some(files_count(entries, Status::Existing)),
        deleted_files_count: Some(files_count(entries, Status::Deleted)),
        added_rows_count: Some(rows_count(entries, Status::Added)),
        existing_rows_count: Some(rows_count(entries, Status::Existing)),
        deleted_rows_count: Some(rows_count(entries, Status::Deleted)),
        partitions: (spec.fields.iter().enumerate())
            .map(|(i, field)| {
                // A float's bound is 4 bytes, where a `Literal::Double` holding it would write 8.
                let float = field.transform == Transform::Identity
                    && schema
                        .field(field.source_id)
                        .is_some_and(|f| f.field_type == Type::Primitive(Primitive::Float));
                summarize_field(entries, i, float)
            })
            .collect(),
    }
}

/// Sets each count `manifest` lacks from `entries`, the manifest's own as [`read_manifest`] gives them; a count the
/// list held is kept.
pub fn fill_counts(manifest: &mut ManifestFile, entries: &[DataFileEntry]) {
    let files = |status| Some(files_count(entries, status));
    let rows = |status| Some(rows_count(entries, status));
    manifest.added_files_count = manifest.added_files_count.or_else(|| files(Status::Added));
    manifest.existing_files_count = manifest.existing_files_count.or_else(|| files(Status::Existing));
    manifest.deleted_files_count = manifest.deleted_files_count.or_else(|| files(Status::Deleted));
    manifest.added_rows_count = manifest.added_rows_count.or_else(|| rows(Status::Added));
    manifest.existing_rows_count = manifest.existing_rows_count.or_else(|| rows(Status::Existing));
    manifest.deleted_rows_count = manifest.deleted_rows_count.or_else(|| rows(Status::Deleted));
}

fn files_count(entries: &[DataFileEntry], status: Status) -> i32 {
    entries.iter().filter(|e| e.status == status).count() as i32
}

fn rows_count(entries: &[DataFileEntry], status: Status) -> i64 {
    (entries.iter())
        .filter(|e| e.status == status)
        .map(|e| e.record_count)
        .sum()
}

fn summarize_field(entries: &[DataFileEntry], field: usize, float: bool) -> FieldSummary {
    let values = entries.iter().map(|e| e.partition.get(field).and_then(Option::as_ref));
    let contains_null = values.clone().any(|v| v.is_none());
    let mut contains_nan = None;
    let (mut lower, mut upper): (Option<&Literal>, Option<&Literal>) = (None, None);
    for value in values.flatten() {
        if let Literal::Double(d) = value {
            contains_nan = Some(contains_nan.unwrap_or(false) || d.is_nan());
            if d.is_nan() {
                continue;
            }
        }
        if lower.is_none_or(|l| compare(value, l) == Some(Ordering::Less)) {
            lower = Some(value);
        }
        if upper.is_none_or(|u| compare(value, u) == Some(Ordering::Greater)) {
            upper = Some(value);
        }
    }
    FieldSummary {
        contains_null,
        contains_nan,
        lower_bound: lower.map(|l| bound_bytes(l, float)),
        upper_bound: upper.map(|u| bound_bytes(u, float)),
    }
}

fn compare(a: &Literal, b: &Literal) -> Option<Ordering> {
    use Literal as L;
    match (a, b) {
        (L::Boolean(a), L::Boolean(b)) => Some(a.cmp(b)),
        (L::Int(a), L::Int(b)) | (L::Date(a), L::Date(b)) => Some(a.cmp(b)),
        (L::Long(a), L::Long(b)) | (L::Micros(a), L::Micros(b)) => Some(a.cmp(b)),
        (L::Double(a), L::Double(b)) => a.partial_cmp(b),
        (L::String(a), L::String(b)) => Some(a.cmp(b)),
        (L::Binary(a), L::Binary(b)) => Some(a.cmp(b)),
        _ => None,
    }
}

/// A partition bound's single-value serialization; a `float` field's as 4 bytes.
fn bound_bytes(literal: &Literal, float: bool) -> Vec<u8> {
    match literal {
        Literal::Double(v) if float => (*v as f32).to_le_bytes().to_vec(),
        literal => literal_bytes(literal),
    }
}

/// A literal's single-value serialization (the spec's appendix D).
fn literal_bytes(literal: &Literal) -> Vec<u8> {
    match literal {
        Literal::Boolean(b) => vec![*b as u8],
        Literal::Int(v) | Literal::Date(v) => v.to_le_bytes().to_vec(),
        Literal::Long(v) | Literal::Micros(v) => v.to_le_bytes().to_vec(),
        Literal::Double(v) => v.to_le_bytes().to_vec(),
        Literal::String(s) => s.as_bytes().to_vec(),
        Literal::Binary(b) => b.clone(),
    }
}

/// A snapshot's manifest list. A manifest whose sequence numbers are `None` gets `sequence_number`, the snapshot's. A
/// manifest with a count still `None` is an error.
pub fn write_manifest_list(
    snapshot_id: i64,
    parent: Option<i64>,
    sequence_number: i64,
    manifests: &[ManifestFile],
) -> Result<Vec<u8>, String> {
    let records = manifests
        .iter()
        .map(|m| manifest_file_value(m, sequence_number))
        .collect::<Result<Vec<_>, String>>()?;
    let metadata = [
        ("snapshot-id", snapshot_id.to_string()),
        (
            "parent-snapshot-id",
            parent.map_or_else(|| "null".to_string(), |p| p.to_string()),
        ),
        ("sequence-number", sequence_number.to_string()),
        ("format-version", FORMAT_VERSION.to_string()),
    ];
    write_container(&manifest_list_schema(), &metadata, records)
}

/// A snapshot's manifests, from its manifest list.
pub fn read_manifest_list(bytes: &[u8]) -> Result<Vec<ManifestFile>, String> {
    read_container(bytes)?
        .iter()
        .map(|value| {
            let r = record(Some(value), "manifest_file")?;
            let partitions = match field(r, "partitions") {
                Some(Value::Array(items)) => items
                    .iter()
                    .map(|item| {
                        let s = record(Some(item), "field_summary")?;
                        Ok(FieldSummary {
                            contains_null: boolean(field(s, "contains_null"), "contains_null")?,
                            contains_nan: field(s, "contains_nan")
                                .map(|v| boolean(Some(v), "contains_nan"))
                                .transpose()?,
                            lower_bound: field(s, "lower_bound")
                                .map(|v| bytes_of(Some(v), "lower_bound"))
                                .transpose()?,
                            upper_bound: field(s, "upper_bound")
                                .map(|v| bytes_of(Some(v), "upper_bound"))
                                .transpose()?,
                        })
                    })
                    .collect::<Result<_, String>>()?,
                _ => Vec::new(),
            };
            // Format version 1 named the file counts `*_data_files_count` and left every count optional; a list
            // carried over from it may still.
            let files = |name: &str| {
                (field(r, name).or_else(|| field(r, &name.replace("files", "data_files"))))
                    .map(|v| int(Some(v), name))
                    .transpose()
            };
            let rows = |name: &str| field(r, name).map(|v| long(Some(v), name)).transpose();
            Ok(ManifestFile {
                manifest_path: string(field(r, "manifest_path"), "manifest_path")?,
                manifest_length: long(field(r, "manifest_length"), "manifest_length")?,
                partition_spec_id: int(field(r, "partition_spec_id"), "partition_spec_id")?,
                content: match field(r, "content").map(|v| int(Some(v), "content")).transpose()? {
                    None | Some(0) => Content::Data,
                    Some(_) => Content::Deletes,
                },
                sequence_number: field(r, "sequence_number")
                    .map(|v| long(Some(v), "sequence_number"))
                    .transpose()?,
                min_sequence_number: field(r, "min_sequence_number")
                    .map(|v| long(Some(v), "min_sequence_number"))
                    .transpose()?,
                added_snapshot_id: long(field(r, "added_snapshot_id"), "added_snapshot_id")?,
                added_files_count: files("added_files_count")?,
                existing_files_count: files("existing_files_count")?,
                deleted_files_count: files("deleted_files_count")?,
                added_rows_count: rows("added_rows_count")?,
                existing_rows_count: rows("existing_rows_count")?,
                deleted_rows_count: rows("deleted_rows_count")?,
                partitions,
            })
        })
        .collect()
}

/// The entries of the manifest `manifest` names, with what they inherit from it made explicit: the snapshot id of
/// every entry that has none, and the sequence numbers of an added entry that has none.
pub fn read_manifest(bytes: &[u8], manifest: &ManifestFile) -> Result<Vec<DataFileEntry>, String> {
    read_container(bytes)?
        .iter()
        .map(|value| {
            let r = record(Some(value), "manifest_entry")?;
            let status = Status::from_code(int(field(r, "status"), "status")?)?;
            let inherited = |own: Option<i64>| match status {
                Status::Added => own.or(manifest.sequence_number),
                _ => own,
            };
            let optional_long = |name: &str| field(r, name).map(|v| long(Some(v), name)).transpose();
            let file = record(field(r, "data_file"), "data_file")?;
            let partition = match field(file, "partition") {
                Some(Value::Record(values)) => values.iter().map(|(_, v)| literal_of(v)).collect::<Result<_, _>>()?,
                _ => Vec::new(),
            };
            Ok(DataFileEntry {
                status,
                snapshot_id: optional_long("snapshot_id")?.or(Some(manifest.added_snapshot_id)),
                sequence_number: inherited(optional_long("sequence_number")?),
                file_sequence_number: inherited(optional_long("file_sequence_number")?),
                file_path: string(field(file, "file_path"), "file_path")?,
                file_format: string(field(file, "file_format"), "file_format")?,
                partition,
                record_count: long(field(file, "record_count"), "record_count")?,
                file_size_in_bytes: long(field(file, "file_size_in_bytes"), "file_size_in_bytes")?,
                stats: Stats {
                    column_sizes: int_map(field(file, "column_sizes"), |v| long(Some(v), "column_sizes"))?,
                    value_counts: int_map(field(file, "value_counts"), |v| long(Some(v), "value_counts"))?,
                    null_value_counts: int_map(field(file, "null_value_counts"), |v| {
                        long(Some(v), "null_value_counts")
                    })?,
                    lower_bounds: int_map(field(file, "lower_bounds"), |v| bytes_of(Some(v), "lower_bounds"))?,
                    upper_bounds: int_map(field(file, "upper_bounds"), |v| bytes_of(Some(v), "upper_bounds"))?,
                },
                carried: CARRIED
                    .iter()
                    .filter_map(|&name| Some((name.to_string(), field(file, name)?.clone())))
                    .collect(),
            })
        })
        .collect()
}

/// The groups of `manifests` worth merging, each a list of indices to rewrite into one manifest: small data
/// manifests of one partition spec, packed in order up to `target_bytes`, once a spec has `min_count` of them. A
/// group holds two manifests or more.
pub fn merge_plan(manifests: &[ManifestFile], target_bytes: u64, min_count: usize) -> Vec<Vec<usize>> {
    let mut by_spec: Vec<(i32, Vec<usize>)> = Vec::new();
    for (i, m) in manifests.iter().enumerate() {
        // A manifest listed with no sequence number was written before format version 2: its files' sequence
        // number is 0, which a rewrite would lose. It stays as it is.
        if m.content != Content::Data || m.manifest_length as u64 >= target_bytes || m.sequence_number.is_none() {
            continue;
        }
        match by_spec.iter_mut().find(|(spec, _)| *spec == m.partition_spec_id) {
            Some((_, small)) => small.push(i),
            None => by_spec.push((m.partition_spec_id, vec![i])),
        }
    }
    let mut groups = Vec::new();
    for (_, small) in by_spec.into_iter().filter(|(_, small)| small.len() >= min_count) {
        let mut bin: Vec<usize> = Vec::new();
        let mut bin_bytes = 0u64;
        for i in small {
            let length = manifests[i].manifest_length as u64;
            if !bin.is_empty() && bin_bytes + length > target_bytes {
                groups.push(std::mem::take(&mut bin));
                bin_bytes = 0;
            }
            bin.push(i);
            bin_bytes += length;
        }
        groups.push(bin);
    }
    groups.retain(|group| group.len() > 1);
    groups
}

/// The entries of a merged manifest written by the snapshot `snapshot_id`, from the entries of the manifests it
/// replaces, read with `read_manifest` so their sequence numbers are explicit. What an earlier snapshot added or kept
/// is now existing, and what it deleted is gone; what this snapshot added or deleted stays so.
pub fn merge(entries: Vec<DataFileEntry>, snapshot_id: i64) -> Vec<DataFileEntry> {
    entries
        .into_iter()
        .filter_map(|mut entry| {
            let this_snapshot = entry.snapshot_id == Some(snapshot_id);
            match entry.status {
                Status::Deleted if !this_snapshot => None,
                Status::Deleted | Status::Added if this_snapshot => Some(entry),
                _ => {
                    entry.status = Status::Existing;
                    Some(entry)
                }
            }
        })
        .collect()
}

/// The type of each partition field's values: its transform's result type over its source column.
fn partition_types(schema: &Schema, spec: &PartitionSpec) -> Result<Vec<Primitive>, String> {
    spec.fields
        .iter()
        .map(|field| {
            let source = match schema.field(field.source_id).map(|f| &f.field_type) {
                Some(Type::Primitive(p)) => p.clone(),
                Some(other) => return Err(format!("partition field {} is over a {other}", field.name)),
                None => return Err(format!("partition field {} has no source column", field.name)),
            };
            Ok(match &field.transform {
                Transform::Identity | Transform::Truncate(_) | Transform::Void => source,
                Transform::Year | Transform::Month | Transform::Hour | Transform::Bucket(_) => Primitive::Int,
                Transform::Day => Primitive::Date,
                Transform::Other(name) => {
                    return Err(format!("partition transform {name} is not one this writer knows"));
                }
            })
        })
        .collect()
}

/// An Avro name for a partition field, as Iceberg makes one: a character a name cannot hold becomes `_x` and its hex.
fn avro_name(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    for (i, c) in name.chars().enumerate() {
        match c {
            'A'..='Z' | 'a'..='z' | '_' => out.push(c),
            '0'..='9' if i > 0 => out.push(c),
            '0'..='9' => {
                out.push('_');
                out.push(c);
            }
            _ => out.push_str(&format!("_x{:X}", c as u32)),
        }
    }
    out
}

fn avro_primitive(ty: &Primitive) -> Result<Json, String> {
    Ok(match ty {
        Primitive::Boolean => json!("boolean"),
        Primitive::Int => json!("int"),
        Primitive::Long => json!("long"),
        Primitive::Float => json!("float"),
        Primitive::Double => json!("double"),
        Primitive::Date => json!({"type": "int", "logicalType": "date"}),
        Primitive::Timestamp => json!({"type": "long", "logicalType": "timestamp-micros", "adjust-to-utc": false}),
        Primitive::Timestamptz => json!({"type": "long", "logicalType": "timestamp-micros", "adjust-to-utc": true}),
        Primitive::String => json!("string"),
        Primitive::Binary => json!("bytes"),
        Primitive::Other(name) => {
            return Err(format!(
                "a partition value of type {name} is not one this writer writes"
            ));
        }
    })
}

fn required(name: &str, id: i32, ty: Json) -> Json {
    json!({"name": name, "type": ty, "field-id": id})
}

fn optional(name: &str, id: i32, ty: Json) -> Json {
    json!({"name": name, "type": ["null", ty], "default": null, "field-id": id})
}

/// An Iceberg map with int keys, which Avro writes as an array of key/value records.
fn int_map_type(key_id: i32, value_id: i32, value: &str) -> Json {
    json!({
        "type": "array",
        "logicalType": "map",
        "items": {
            "type": "record",
            "name": format!("k{key_id}_v{value_id}"),
            "fields": [required("key", key_id, json!("int")), required("value", value_id, json!(value))],
        },
    })
}

fn list_type(element_id: i32, items: Json) -> Json {
    json!({"type": "array", "items": items, "element-id": element_id})
}

fn manifest_schema(partition_fields: Vec<Json>) -> Json {
    let data_file = json!({
        "type": "record",
        "name": "r2",
        "fields": [
            required("content", 134, json!("int")),
            required("file_path", 100, json!("string")),
            required("file_format", 101, json!("string")),
            required("partition", 102, json!({"type": "record", "name": "r102", "fields": partition_fields})),
            required("record_count", 103, json!("long")),
            required("file_size_in_bytes", 104, json!("long")),
            optional("column_sizes", 108, int_map_type(117, 118, "long")),
            optional("value_counts", 109, int_map_type(119, 120, "long")),
            optional("null_value_counts", 110, int_map_type(121, 122, "long")),
            optional("nan_value_counts", 137, int_map_type(138, 139, "long")),
            optional("lower_bounds", 125, int_map_type(126, 127, "bytes")),
            optional("upper_bounds", 128, int_map_type(129, 130, "bytes")),
            optional("key_metadata", 131, json!("bytes")),
            optional("split_offsets", 132, list_type(133, json!("long"))),
            optional("equality_ids", 135, list_type(136, json!("int"))),
            optional("sort_order_id", 140, json!("int")),
        ],
    });
    json!({
        "type": "record",
        "name": "manifest_entry",
        "fields": [
            required("status", 0, json!("int")),
            optional("snapshot_id", 1, json!("long")),
            optional("sequence_number", 3, json!("long")),
            optional("file_sequence_number", 4, json!("long")),
            required("data_file", 2, data_file),
        ],
    })
}

fn manifest_list_schema() -> Json {
    let field_summary = json!({
        "type": "record",
        "name": "r508",
        "fields": [
            required("contains_null", 509, json!("boolean")),
            optional("contains_nan", 518, json!("boolean")),
            optional("lower_bound", 510, json!("bytes")),
            optional("upper_bound", 511, json!("bytes")),
        ],
    });
    json!({
        "type": "record",
        "name": "manifest_file",
        "fields": [
            required("manifest_path", 500, json!("string")),
            required("manifest_length", 501, json!("long")),
            required("partition_spec_id", 502, json!("int")),
            required("content", 517, json!("int")),
            required("sequence_number", 515, json!("long")),
            required("min_sequence_number", 516, json!("long")),
            required("added_snapshot_id", 503, json!("long")),
            required("added_files_count", 504, json!("int")),
            required("existing_files_count", 505, json!("int")),
            required("deleted_files_count", 506, json!("int")),
            required("added_rows_count", 512, json!("long")),
            required("existing_rows_count", 513, json!("long")),
            required("deleted_rows_count", 514, json!("long")),
            optional("partitions", 507, list_type(508, field_summary)),
            optional("key_metadata", 519, json!("bytes")),
        ],
    })
}

fn some(value: Value) -> Value {
    Value::Union(1, Box::new(value))
}

fn null() -> Value {
    Value::Union(0, Box::new(Value::Null))
}

fn nullable(value: Option<Value>) -> Value {
    value.map_or_else(null, some)
}

fn int_map_value<V>(map: &BTreeMap<i32, V>, value: impl Fn(&V) -> Value) -> Value {
    if map.is_empty() {
        return null();
    }
    some(Value::Array(
        map.iter()
            .map(|(k, v)| Value::Record(vec![("key".into(), Value::Int(*k)), ("value".into(), value(v))]))
            .collect(),
    ))
}

fn entry_value(entry: &DataFileEntry, names: &[String], types: &[Primitive]) -> Result<Value, String> {
    if entry.partition.len() != types.len() {
        return Err(format!(
            "{} has {} partition values for a spec of {} fields",
            entry.file_path,
            entry.partition.len(),
            types.len()
        ));
    }
    let partition = entry
        .partition
        .iter()
        .zip(names.iter().zip(types))
        .map(|(value, (name, ty))| {
            let value = value.as_ref().map(|v| literal_value(v, ty)).transpose()?;
            Ok((name.clone(), nullable(value)))
        })
        .collect::<Result<Vec<_>, String>>()?;
    let stats = &entry.stats;
    let long = |v: &i64| Value::Long(*v);
    let bytes = |v: &Vec<u8>| Value::Bytes(v.clone());
    let data_file = Value::Record(vec![
        ("content".into(), Value::Int(0)),
        ("file_path".into(), Value::String(entry.file_path.clone())),
        ("file_format".into(), Value::String(entry.file_format.clone())),
        ("partition".into(), Value::Record(partition)),
        ("record_count".into(), Value::Long(entry.record_count)),
        ("file_size_in_bytes".into(), Value::Long(entry.file_size_in_bytes)),
        ("column_sizes".into(), int_map_value(&stats.column_sizes, long)),
        ("value_counts".into(), int_map_value(&stats.value_counts, long)),
        (
            "null_value_counts".into(),
            int_map_value(&stats.null_value_counts, long),
        ),
        ("nan_value_counts".into(), carried(entry, "nan_value_counts")),
        ("lower_bounds".into(), int_map_value(&stats.lower_bounds, bytes)),
        ("upper_bounds".into(), int_map_value(&stats.upper_bounds, bytes)),
        ("key_metadata".into(), carried(entry, "key_metadata")),
        ("split_offsets".into(), carried(entry, "split_offsets")),
        ("equality_ids".into(), carried(entry, "equality_ids")),
        ("sort_order_id".into(), carried(entry, "sort_order_id")),
    ]);
    Ok(Value::Record(vec![
        ("status".into(), Value::Int(entry.status.code())),
        ("snapshot_id".into(), nullable(entry.snapshot_id.map(Value::Long))),
        (
            "sequence_number".into(),
            nullable(entry.sequence_number.map(Value::Long)),
        ),
        (
            "file_sequence_number".into(),
            nullable(entry.file_sequence_number.map(Value::Long)),
        ),
        ("data_file".into(), data_file),
    ]))
}

/// `entry`'s value of the optional data file field `name` as it was read, or null.
fn carried(entry: &DataFileEntry, name: &str) -> Value {
    nullable(
        (entry.carried.iter())
            .find(|(n, _)| n == name)
            .map(|(_, value)| value.clone()),
    )
}

/// A partition value as its Avro type holds it.
fn literal_value(literal: &Literal, ty: &Primitive) -> Result<Value, String> {
    use Literal as L;
    use Primitive as P;
    Ok(match (ty, literal) {
        (P::Boolean, L::Boolean(b)) => Value::Boolean(*b),
        (P::Int, L::Int(v)) => Value::Int(*v),
        (P::Long, L::Long(v)) => Value::Long(*v),
        (P::Float, L::Double(v)) => Value::Float(*v as f32),
        (P::Double, L::Double(v)) => Value::Double(*v),
        (P::Date, L::Date(v)) => Value::Date(*v),
        (P::Timestamp | P::Timestamptz, L::Micros(v)) => Value::TimestampMicros(*v),
        (P::String, L::String(s)) => Value::String(s.clone()),
        (P::Binary, L::Binary(b)) => Value::Bytes(b.clone()),
        _ => return Err(format!("partition value {literal:?} is not a {ty}")),
    })
}

/// A partition value as a reader of ours holds it, from its Avro value.
fn literal_of(value: &Value) -> Result<Option<Literal>, String> {
    Ok(Some(match value {
        Value::Union(_, inner) => return literal_of(inner),
        Value::Null => return Ok(None),
        Value::Boolean(b) => Literal::Boolean(*b),
        Value::Int(v) => Literal::Int(*v),
        Value::Long(v) => Literal::Long(*v),
        Value::Float(v) => Literal::Double(*v as f64),
        Value::Double(v) => Literal::Double(*v),
        Value::Date(v) => Literal::Date(*v),
        Value::TimestampMicros(v) | Value::LocalTimestampMicros(v) => Literal::Micros(*v),
        Value::String(s) => Literal::String(s.clone()),
        Value::Bytes(b) | Value::Fixed(_, b) => Literal::Binary(b.clone()),
        other => {
            return Err(format!(
                "a partition value of Avro value {other:?} is not one this reader knows"
            ));
        }
    }))
}

fn manifest_file_value(m: &ManifestFile, sequence_number: i64) -> Result<Value, String> {
    let count =
        |value: Option<Value>, name: &str| value.ok_or_else(|| format!("{} has no {name} to list", m.manifest_path));
    let partitions = m
        .partitions
        .iter()
        .map(|s| {
            Value::Record(vec![
                ("contains_null".into(), Value::Boolean(s.contains_null)),
                ("contains_nan".into(), nullable(s.contains_nan.map(Value::Boolean))),
                ("lower_bound".into(), nullable(s.lower_bound.clone().map(Value::Bytes))),
                ("upper_bound".into(), nullable(s.upper_bound.clone().map(Value::Bytes))),
            ])
        })
        .collect();
    Ok(Value::Record(vec![
        ("manifest_path".into(), Value::String(m.manifest_path.clone())),
        ("manifest_length".into(), Value::Long(m.manifest_length)),
        ("partition_spec_id".into(), Value::Int(m.partition_spec_id)),
        (
            "content".into(),
            Value::Int(match m.content {
                Content::Data => 0,
                Content::Deletes => 1,
            }),
        ),
        (
            "sequence_number".into(),
            Value::Long(m.sequence_number.unwrap_or(sequence_number)),
        ),
        (
            "min_sequence_number".into(),
            Value::Long(m.min_sequence_number.unwrap_or(sequence_number)),
        ),
        ("added_snapshot_id".into(), Value::Long(m.added_snapshot_id)),
        (
            "added_files_count".into(),
            count(m.added_files_count.map(Value::Int), "added_files_count")?,
        ),
        (
            "existing_files_count".into(),
            count(m.existing_files_count.map(Value::Int), "existing_files_count")?,
        ),
        (
            "deleted_files_count".into(),
            count(m.deleted_files_count.map(Value::Int), "deleted_files_count")?,
        ),
        (
            "added_rows_count".into(),
            count(m.added_rows_count.map(Value::Long), "added_rows_count")?,
        ),
        (
            "existing_rows_count".into(),
            count(m.existing_rows_count.map(Value::Long), "existing_rows_count")?,
        ),
        (
            "deleted_rows_count".into(),
            count(m.deleted_rows_count.map(Value::Long), "deleted_rows_count")?,
        ),
        ("partitions".into(), some(Value::Array(partitions))),
        ("key_metadata".into(), null()),
    ]))
}

/// An Avro object container file, deflated, whose header carries `schema` as written here: the parsed schema
/// apache-avro would write instead drops a `logicalType` it does not know (Iceberg's `map`) and the attributes of one
/// it does (`adjust-to-utc`).
fn write_container(schema: &Json, metadata: &[(&str, String)], records: Vec<Value>) -> Result<Vec<u8>, String> {
    let parsed = AvroSchema::parse(schema).map_err(|e| format!("Avro schema: {e}"))?;
    let mut marker = [0u8; 16];
    SystemRandom::new()
        .fill(&mut marker)
        .map_err(|_| "no randomness for an Avro sync marker".to_string())?;

    let metadata: Vec<(&str, &[u8])> = metadata.iter().map(|(k, v)| (*k, v.as_bytes())).collect();
    let header = crate::io::avro::container::header(&schema.to_string(), "deflate", &metadata, &marker);

    let mut writer = Writer::builder()
        .schema(&parsed)
        .writer(header)
        .codec(Codec::Deflate(DeflateSettings::default()))
        .marker(marker)
        .has_header(true)
        .build()
        .map_err(|e| e.to_string())?;
    for record in records {
        writer.append_value(record).map_err(|e| format!("Avro record: {e}"))?;
    }
    writer.into_inner().map_err(|e| e.to_string())
}

fn read_container(bytes: &[u8]) -> Result<Vec<Value>, String> {
    let reader = Reader::new(bytes).map_err(|e| format!("Avro file: {e}"))?;
    reader
        .map(|value| value.map_err(|e| format!("Avro record: {e}")))
        .collect()
}

/// A record field's value, with a union's branch taken; `None` when it is null or absent.
fn field<'a>(record: &'a [(String, Value)], name: &str) -> Option<&'a Value> {
    let value = &record.iter().find(|(n, _)| n == name)?.1;
    match value {
        Value::Union(_, inner) => match &**inner {
            Value::Null => None,
            inner => Some(inner),
        },
        Value::Null => None,
        value => Some(value),
    }
}

fn record<'a>(value: Option<&'a Value>, name: &str) -> Result<&'a [(String, Value)], String> {
    match value {
        Some(Value::Record(fields)) => Ok(fields),
        Some(Value::Union(_, inner)) => record(Some(inner), name),
        _ => Err(format!("{name} is not a record")),
    }
}

fn long(value: Option<&Value>, name: &str) -> Result<i64, String> {
    match value {
        Some(Value::Long(v) | Value::TimestampMicros(v)) => Ok(*v),
        Some(Value::Int(v)) => Ok(*v as i64),
        _ => Err(format!("{name} is not a long")),
    }
}

fn int(value: Option<&Value>, name: &str) -> Result<i32, String> {
    match value {
        Some(Value::Int(v)) => Ok(*v),
        _ => Err(format!("{name} is not an int")),
    }
}

fn boolean(value: Option<&Value>, name: &str) -> Result<bool, String> {
    match value {
        Some(Value::Boolean(b)) => Ok(*b),
        _ => Err(format!("{name} is not a boolean")),
    }
}

fn string(value: Option<&Value>, name: &str) -> Result<String, String> {
    match value {
        Some(Value::String(s)) => Ok(s.clone()),
        _ => Err(format!("{name} is not a string")),
    }
}

fn bytes_of(value: Option<&Value>, name: &str) -> Result<Vec<u8>, String> {
    match value {
        Some(Value::Bytes(b) | Value::Fixed(_, b)) => Ok(b.clone()),
        _ => Err(format!("{name} is not bytes")),
    }
}

/// An Iceberg map with int keys, written as an array of key/value records.
fn int_map<V>(
    value: Option<&Value>,
    convert: impl Fn(&Value) -> Result<V, String>,
) -> Result<BTreeMap<i32, V>, String> {
    let Some(Value::Array(items)) = value else {
        return Ok(BTreeMap::new());
    };
    items
        .iter()
        .map(|item| {
            let pair = record(Some(item), "a map entry")?;
            Ok((
                int(field(pair, "key"), "a map key")?,
                convert(field(pair, "value").unwrap_or(&Value::Null))?,
            ))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::builder::{ListBuilder, StringBuilder};
    use arrow_array::{
        ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
    };
    use arrow_schema::{DataType, Field, TimeUnit as ArrowTimeUnit};
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;

    use super::*;
    use crate::io::iceberg::metadata::PartitionField;

    fn table_schema() -> Schema {
        serde_json::from_value(json!({"type": "struct", "schema-id": 0, "fields": [
            {"id": 1, "name": "ts", "required": false, "type": "timestamptz"},
            {"id": 2, "name": "host", "required": false, "type": "string"},
            {"id": 3, "name": "bytes", "required": false, "type": "long"},
            {"id": 4, "name": "ok", "required": false, "type": "boolean"},
            {"id": 5, "name": "ratio", "required": false, "type": "double"},
            {"id": 6, "name": "tags", "required": false,
             "type": {"type": "list", "element-id": 7, "element-required": false, "element": "string"}}
        ]}))
        .unwrap()
    }

    fn spec() -> PartitionSpec {
        serde_json::from_value(json!({"spec-id": 0, "fields": [
            {"name": "ts_day", "transform": "day", "source-id": 1, "field-id": 1000},
            {"name": "host", "transform": "identity", "source-id": 2, "field-id": 1001}
        ]}))
        .unwrap()
    }

    /// An Avro container's header metadata, decoded by hand so the test does not trust the code it checks.
    fn header(bytes: &[u8]) -> HashMap<String, Vec<u8>> {
        fn long(bytes: &[u8], at: &mut usize) -> i64 {
            let (mut n, mut shift) = (0u64, 0);
            loop {
                let b = bytes[*at];
                *at += 1;
                n |= ((b & 0x7f) as u64) << shift;
                shift += 7;
                if b < 0x80 {
                    return (n >> 1) as i64 ^ -((n & 1) as i64);
                }
            }
        }
        fn bytes_at(bytes: &[u8], at: &mut usize) -> Vec<u8> {
            let len = long(bytes, at) as usize;
            *at += len;
            bytes[*at - len..*at].to_vec()
        }
        assert_eq!(&bytes[..4], b"Obj\x01");
        let mut at = 4;
        let mut metadata = HashMap::new();
        loop {
            let count = long(bytes, &mut at);
            if count == 0 {
                return metadata;
            }
            for _ in 0..count {
                let key = String::from_utf8(bytes_at(bytes, &mut at)).unwrap();
                metadata.insert(key, bytes_at(bytes, &mut at));
            }
        }
    }

    fn added_entry() -> DataFileEntry {
        let mut stats = Stats::default();
        stats.column_sizes.insert(1, 100);
        stats.value_counts.insert(1, 10);
        stats.null_value_counts.insert(1, 0);
        stats.lower_bounds.insert(1, 5i64.to_le_bytes().to_vec());
        stats.upper_bounds.insert(1, 50i64.to_le_bytes().to_vec());
        DataFileEntry::added(
            9,
            "s3://b/t/data/a.parquet".into(),
            vec![Some(Literal::Date(19000)), Some(Literal::String("web-1".into()))],
            10,
            2048,
            stats,
        )
    }

    fn existing_entry() -> DataFileEntry {
        DataFileEntry {
            status: Status::Existing,
            snapshot_id: Some(5),
            sequence_number: Some(3),
            file_sequence_number: Some(3),
            file_path: "s3://b/t/data/b.parquet".into(),
            file_format: "PARQUET".into(),
            partition: vec![Some(Literal::Date(18999)), None],
            record_count: 4,
            file_size_in_bytes: 512,
            stats: Stats::default(),
            carried: Vec::new(),
        }
    }

    #[test]
    fn a_manifest_and_its_list_read_back_with_what_they_inherit_at_commit() {
        let (schema, spec) = (table_schema(), spec());
        let entries = vec![added_entry(), existing_entry()];
        let manifest = write_manifest(&schema, &spec, &entries).unwrap();
        let summary = summarize(
            "s3://b/t/metadata/m0.avro".into(),
            manifest.len() as u64,
            &schema,
            &spec,
            9,
            &entries,
        );
        assert_eq!(summary.sequence_number, None);
        assert_eq!(summary.min_sequence_number, Some(3));
        assert_eq!(
            (summary.added_files_count, summary.existing_files_count),
            (Some(1), Some(1))
        );
        assert_eq!(
            (summary.added_rows_count, summary.existing_rows_count),
            (Some(10), Some(4))
        );
        assert_eq!(
            summary.partitions,
            vec![
                FieldSummary {
                    contains_null: false,
                    contains_nan: None,
                    lower_bound: Some(18999i32.to_le_bytes().to_vec()),
                    upper_bound: Some(19000i32.to_le_bytes().to_vec()),
                },
                FieldSummary {
                    contains_null: true,
                    contains_nan: None,
                    lower_bound: Some(b"web-1".to_vec()),
                    upper_bound: Some(b"web-1".to_vec()),
                },
            ]
        );

        let list = write_manifest_list(9, Some(5), 7, std::slice::from_ref(&summary)).unwrap();
        let listed = read_manifest_list(&list).unwrap();
        assert_eq!(
            listed,
            vec![ManifestFile {
                sequence_number: Some(7),
                ..summary
            }]
        );

        let read = read_manifest(&manifest, &listed[0]).unwrap();
        let added = DataFileEntry {
            sequence_number: Some(7),
            file_sequence_number: Some(7),
            ..added_entry()
        };
        assert_eq!(read, vec![added, existing_entry()]);

        let metadata = header(&manifest);
        let text = |key: &str| String::from_utf8(metadata[key].clone()).unwrap();
        assert_eq!(serde_json::from_str::<Schema>(&text("schema")).unwrap(), schema);
        assert_eq!(
            serde_json::from_str::<Vec<PartitionField>>(&text("partition-spec")).unwrap(),
            spec.fields
        );
        assert_eq!(
            ["schema-id", "partition-spec-id", "format-version", "content"].map(text),
            ["0", "0", "2", "data"].map(String::from)
        );
        assert_eq!(text("avro.codec"), "deflate");

        let metadata = header(&list);
        let text = |key: &str| String::from_utf8(metadata[key].clone()).unwrap();
        assert_eq!(
            ["snapshot-id", "parent-snapshot-id", "sequence-number", "format-version"].map(text),
            ["9", "5", "7", "2"].map(String::from)
        );
    }

    #[test]
    fn every_avro_field_carries_its_field_id() {
        fn check(schema: &Json, path: &str) {
            match schema {
                Json::Array(branches) => branches.iter().for_each(|b| check(b, path)),
                Json::Object(o) => {
                    if let Some(Json::Array(fields)) = o.get("fields") {
                        for f in fields {
                            let path = format!("{path}.{}", f["name"].as_str().unwrap());
                            assert!(f.get("field-id").is_some_and(Json::is_i64), "{path} has no field-id");
                            check(&f["type"], &path);
                        }
                    }
                    if o.get("type") == Some(&json!("array")) {
                        if o.get("logicalType") != Some(&json!("map")) {
                            assert!(
                                o.get("element-id").is_some_and(Json::is_i64),
                                "{path} has no element-id"
                            );
                        }
                        check(&o["items"], &format!("{path}[]"));
                    }
                }
                _ => {}
            }
        }
        let manifest = write_manifest(&table_schema(), &spec(), &[added_entry()]).unwrap();
        let summary = summarize(
            "m".into(),
            manifest.len() as u64,
            &table_schema(),
            &spec(),
            9,
            &[added_entry()],
        );
        let list = write_manifest_list(9, None, 1, &[summary]).unwrap();
        for file in [&manifest, &list] {
            let schema: Json = serde_json::from_slice(&header(file)["avro.schema"]).unwrap();
            check(&schema, "");
        }

        let schema: Json = serde_json::from_slice(&header(&manifest)["avro.schema"]).unwrap();
        let data_file = &schema["fields"][4]["type"]["fields"];
        assert_eq!(
            data_file[3]["type"]["fields"][0],
            json!({"name": "ts_day", "type": ["null", {"type": "int", "logicalType": "date"}], "default": null, "field-id": 1000})
        );
        assert_eq!(data_file[6]["type"][1]["logicalType"], json!("map"));
        assert_eq!(
            String::from_utf8(header(&list)["parent-snapshot-id"].clone()).unwrap(),
            "null"
        );
    }

    fn with_id(field: Field, id: i32) -> Field {
        field.with_metadata(HashMap::from([("PARQUET:field_id".to_string(), id.to_string())]))
    }

    #[test]
    fn stats_come_from_the_footer_by_field_id() {
        let element = Arc::new(with_id(Field::new("element", DataType::Utf8, true), 7));
        let fields = vec![
            with_id(
                Field::new(
                    "ts",
                    DataType::Timestamp(ArrowTimeUnit::Microsecond, Some("UTC".into())),
                    true,
                ),
                1,
            ),
            with_id(Field::new("host", DataType::Utf8, true), 2),
            with_id(Field::new("bytes", DataType::Int64, true), 3),
            with_id(Field::new("ok", DataType::Boolean, true), 4),
            with_id(Field::new("ratio", DataType::Float64, true), 5),
            with_id(Field::new("tags", DataType::List(element.clone()), true), 6),
            Field::new("noid", DataType::Int64, true),
        ];
        let mut tags = ListBuilder::new(StringBuilder::new()).with_field(element);
        for row in [vec!["x", "y"], vec![], vec!["z"]] {
            row.into_iter().for_each(|t| tags.values().append_value(t));
            tags.append(true);
        }
        let columns: Vec<ArrayRef> = vec![
            Arc::new(TimestampMicrosecondArray::from(vec![1000, 3000, 2000]).with_timezone("UTC")),
            Arc::new(StringArray::from(vec![
                Some("a-very-long-hostname-beyond-sixteen"),
                Some("b"),
                None,
            ])),
            Arc::new(Int64Array::from(vec![5, -2, 9])),
            Arc::new(BooleanArray::from(vec![true, true, false])),
            Arc::new(Float64Array::from(vec![0.5, 1.5, -1.0])),
            Arc::new(tags.finish()),
            Arc::new(Int64Array::from(vec![1, 2, 3])),
        ];
        let batch = RecordBatch::try_new(Arc::new(arrow_schema::Schema::new(fields)), columns).unwrap();
        let properties = WriterProperties::builder().set_max_row_group_row_count(Some(2)).build();
        let mut writer = ArrowWriter::try_new(Vec::new(), batch.schema(), Some(properties)).unwrap();
        writer.write(&batch).unwrap();
        let metadata = writer.close().unwrap();
        assert_eq!(metadata.num_row_groups(), 2);

        let stats = stats(&metadata, &table_schema());
        assert_eq!(
            stats.column_sizes.keys().copied().collect::<Vec<_>>(),
            vec![1, 2, 3, 4, 5, 7]
        );
        assert_eq!(stats.value_counts[&1], 3);
        assert_eq!(
            stats.value_counts[&7], 4,
            "a list counts its elements, and an empty list's one null"
        );
        assert_eq!(stats.null_value_counts[&2], 1);
        let bound = |bounds: &BTreeMap<i32, Vec<u8>>, id| bounds.get(&id).cloned();
        assert_eq!(bound(&stats.lower_bounds, 1), Some(1000i64.to_le_bytes().to_vec()));
        assert_eq!(bound(&stats.upper_bounds, 1), Some(3000i64.to_le_bytes().to_vec()));
        assert_eq!(bound(&stats.lower_bounds, 2), Some(b"a-very-long-host".to_vec()));
        assert_eq!(bound(&stats.upper_bounds, 2), Some(b"b".to_vec()));
        assert_eq!(bound(&stats.lower_bounds, 3), Some((-2i64).to_le_bytes().to_vec()));
        assert_eq!(bound(&stats.upper_bounds, 3), Some(9i64.to_le_bytes().to_vec()));
        assert_eq!(
            (bound(&stats.lower_bounds, 4), bound(&stats.upper_bounds, 4)),
            (Some(vec![0]), Some(vec![1]))
        );
        assert_eq!(bound(&stats.lower_bounds, 5), Some((-1.0f64).to_le_bytes().to_vec()));
        assert_eq!(bound(&stats.upper_bounds, 5), Some(1.5f64.to_le_bytes().to_vec()));
        assert_eq!(
            (bound(&stats.lower_bounds, 7), bound(&stats.upper_bounds, 7)),
            (None, None)
        );
    }

    #[test]
    fn a_string_bound_is_truncated_to_sixteen_code_points() {
        let long = "abcdefghijklmnopqrstu";
        assert_eq!(truncate_string_lower(long), "abcdefghijklmnop");
        assert_eq!(truncate_string_upper(long).unwrap(), "abcdefghijklmnoq");
        assert_eq!(truncate_string_upper("short").unwrap(), "short");

        let wide = "ééééééééééééééééé";
        assert_eq!(truncate_string_lower(wide).chars().count(), 16);
        assert_eq!(truncate_string_upper(wide).unwrap(), format!("{}ê", "é".repeat(15)));

        let last_is_max = format!("{}\u{10FFFF}zz", "a".repeat(15));
        assert_eq!(
            truncate_string_upper(&last_is_max).unwrap(),
            format!("{}b", "a".repeat(14))
        );
        assert_eq!(
            truncate_string_upper(&format!("{}\u{D7FF}z", "a".repeat(15))).unwrap(),
            format!("{}\u{E000}", "a".repeat(15))
        );
        assert_eq!(truncate_string_upper(&"\u{10FFFF}".repeat(17)), None);

        assert_eq!(truncate_binary_upper(&[0xFF; 17]), None);
        let mut bytes = vec![1u8; 15];
        bytes.extend([0xFF, 3]);
        assert_eq!(truncate_binary_upper(&bytes), Some([vec![1u8; 14], vec![2]].concat()));
    }

    fn listed(spec_id: i32, length: i64, content: Content) -> ManifestFile {
        ManifestFile {
            manifest_path: String::new(),
            manifest_length: length,
            partition_spec_id: spec_id,
            content,
            sequence_number: Some(1),
            min_sequence_number: Some(1),
            added_snapshot_id: 1,
            added_files_count: Some(0),
            existing_files_count: Some(0),
            deleted_files_count: Some(0),
            added_rows_count: Some(0),
            existing_rows_count: Some(0),
            deleted_rows_count: Some(0),
            partitions: Vec::new(),
        }
    }

    #[test]
    fn a_list_written_before_format_version_2_reads_with_the_counts_it_holds() {
        let schema = json!({
            "type": "record",
            "name": "manifest_file",
            "fields": [
                required("manifest_path", 500, json!("string")),
                required("manifest_length", 501, json!("long")),
                required("partition_spec_id", 502, json!("int")),
                required("added_snapshot_id", 503, json!("long")),
                optional("added_data_files_count", 504, json!("int")),
                optional("existing_data_files_count", 505, json!("int")),
                optional("deleted_data_files_count", 506, json!("int")),
            ],
        });
        let record = Value::Record(vec![
            ("manifest_path".into(), Value::String("m.avro".into())),
            ("manifest_length".into(), Value::Long(40)),
            ("partition_spec_id".into(), Value::Int(0)),
            ("added_snapshot_id".into(), Value::Long(5)),
            ("added_data_files_count".into(), null()),
            ("existing_data_files_count".into(), some(Value::Int(2))),
            ("deleted_data_files_count".into(), null()),
        ]);
        let list = write_container(&schema, &[("format-version", "1".into())], vec![record]).unwrap();
        assert_eq!(
            read_manifest_list(&list).unwrap(),
            vec![ManifestFile {
                manifest_path: "m.avro".into(),
                manifest_length: 40,
                sequence_number: None,
                min_sequence_number: None,
                added_snapshot_id: 5,
                added_files_count: None,
                existing_files_count: Some(2),
                deleted_files_count: None,
                added_rows_count: None,
                existing_rows_count: None,
                deleted_rows_count: None,
                ..listed(0, 40, Content::Data)
            }]
        );
    }

    #[test]
    fn counts_a_list_lacks_are_filled_from_the_manifests_entries() {
        let mut manifest = ManifestFile {
            added_files_count: None,
            existing_files_count: Some(7),
            deleted_files_count: None,
            added_rows_count: None,
            existing_rows_count: None,
            deleted_rows_count: None,
            ..listed(0, 40, Content::Data)
        };
        let deleted = DataFileEntry {
            status: Status::Deleted,
            record_count: 6,
            ..existing_entry()
        };
        fill_counts(
            &mut manifest,
            &[added_entry(), existing_entry(), existing_entry(), deleted],
        );
        let counts = |m: &ManifestFile| {
            (
                [m.added_files_count, m.existing_files_count, m.deleted_files_count],
                [m.added_rows_count, m.existing_rows_count, m.deleted_rows_count],
            )
        };
        assert_eq!(
            counts(&manifest),
            ([Some(1), Some(7), Some(1)], [Some(10), Some(8), Some(6)])
        );
    }

    #[test]
    fn a_manifest_with_a_count_missing_is_not_listed() {
        let manifest = ManifestFile {
            manifest_path: "s3://b/t/metadata/m.avro".into(),
            existing_rows_count: None,
            ..listed(0, 40, Content::Data)
        };
        assert_eq!(
            write_manifest_list(9, None, 1, &[manifest]),
            Err("s3://b/t/metadata/m.avro has no existing_rows_count to list".to_string())
        );
    }

    #[test]
    fn fields_another_writer_set_are_written_back_by_a_merge() {
        let (schema, spec) = (table_schema(), spec());
        let entry = DataFileEntry {
            carried: vec![
                (
                    "split_offsets".into(),
                    Value::Array(vec![Value::Long(4), Value::Long(1024)]),
                ),
                ("sort_order_id".into(), Value::Int(0)),
            ],
            ..existing_entry()
        };
        let manifest = write_manifest(&schema, &spec, std::slice::from_ref(&entry)).unwrap();
        let listed = summarize(
            "m.avro".into(),
            manifest.len() as u64,
            &schema,
            &spec,
            5,
            std::slice::from_ref(&entry),
        );
        let listed = ManifestFile {
            sequence_number: Some(3),
            ..listed
        };
        assert_eq!(read_manifest(&manifest, &listed).unwrap(), vec![entry]);
    }

    #[test]
    fn a_manifest_listed_with_no_sequence_number_is_not_merged() {
        let mut manifests = [
            listed(0, 40, Content::Data),
            listed(0, 40, Content::Data),
            listed(0, 40, Content::Data),
        ];
        manifests[1].sequence_number = None;
        assert_eq!(merge_plan(&manifests, 100, 2), vec![vec![0, 2]]);
    }

    #[test]
    fn small_data_manifests_of_one_spec_are_packed_up_to_the_target() {
        let manifests = [
            listed(0, 40, Content::Data),
            listed(0, 40, Content::Data),
            listed(0, 40, Content::Data),
            listed(0, 200, Content::Data),
            listed(0, 40, Content::Data),
            listed(1, 40, Content::Data),
            listed(1, 40, Content::Data),
            listed(0, 40, Content::Deletes),
            listed(0, 40, Content::Data),
        ];
        assert_eq!(merge_plan(&manifests, 100, 3), vec![vec![0, 1], vec![2, 4]]);
        assert_eq!(merge_plan(&manifests, 100, 6), Vec::<Vec<usize>>::new());
    }

    #[test]
    fn merging_keeps_this_snapshots_changes_and_makes_the_rest_existing() {
        let entry = |status, snapshot_id, sequence_number| DataFileEntry {
            status,
            snapshot_id: Some(snapshot_id),
            sequence_number,
            file_sequence_number: sequence_number,
            ..existing_entry()
        };
        let merged = merge(
            vec![
                entry(Status::Added, 9, None),
                entry(Status::Added, 5, Some(3)),
                entry(Status::Existing, 4, Some(2)),
                entry(Status::Deleted, 5, Some(3)),
                entry(Status::Deleted, 9, Some(2)),
            ],
            9,
        );
        assert_eq!(
            merged,
            vec![
                entry(Status::Added, 9, None),
                entry(Status::Existing, 5, Some(3)),
                entry(Status::Existing, 4, Some(2)),
                entry(Status::Deleted, 9, Some(2)),
            ]
        );
    }
}
