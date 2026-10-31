//! A table's metadata as the Iceberg spec writes it in JSON (`metadata.json`, and a REST catalog's answers): schemas
//! with field ids, partition specs, snapshots. Only what a writer reads or sends is modelled; every other key is kept
//! as it came, so nothing a reader of ours does not know is lost.

use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::io::table::Kind;
pub use crate::io::table::Literal;

/// The format version a table is created at.
pub const FORMAT_VERSION: u8 = 2;

/// The first format version that tracks row lineage.
pub const ROW_LINEAGE_VERSION: u8 = 3;

/// The first format version with a variant type.
pub const VARIANT_VERSION: u8 = 3;

/// `current-snapshot-id` of a table with no snapshot, in tables written before the key became optional.
pub const NO_SNAPSHOT: i64 = -1;

/// The first partition field id; ids below it are columns.
pub const FIRST_PARTITION_ID: i32 = 1000;

/// A table's `metadata.json`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct TableMetadata {
    pub format_version: u8,
    pub table_uuid: String,
    pub location: String,
    #[serde(default)]
    pub last_sequence_number: i64,
    pub last_column_id: i32,
    pub current_schema_id: i32,
    pub schemas: Vec<Schema>,
    pub default_spec_id: i32,
    pub partition_specs: Vec<PartitionSpec>,
    pub last_partition_id: i32,
    #[serde(default)]
    pub properties: BTreeMap<String, String>,
    #[serde(default)]
    pub current_snapshot_id: Option<i64>,
    #[serde(default)]
    pub snapshots: Vec<Snapshot>,
    #[serde(default)]
    pub refs: BTreeMap<String, SnapshotRef>,
    /// Higher than every row id assigned; the next snapshot's `first-row-id`. Format version 3 on.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_row_id: Option<i64>,
    #[serde(flatten)]
    pub other: BTreeMap<String, serde_json::Value>,
}

impl TableMetadata {
    /// The first row id a snapshot committed now takes, when the table tracks row lineage.
    pub fn next_row_id(&self) -> Option<i64> {
        (self.format_version >= ROW_LINEAGE_VERSION).then(|| self.next_row_id.unwrap_or(0))
    }

    pub fn schema(&self) -> Option<&Schema> {
        self.schemas.iter().find(|s| s.schema_id == self.current_schema_id)
    }

    pub fn spec(&self) -> Option<&PartitionSpec> {
        self.partition_specs.iter().find(|s| s.spec_id == self.default_spec_id)
    }

    /// The snapshot `main` points at, the one an append's parent is.
    pub fn current_snapshot(&self) -> Option<&Snapshot> {
        let id = match self.refs.get(MAIN) {
            Some(main) => main.snapshot_id,
            None => self.current_snapshot_id.filter(|&id| id != NO_SNAPSHOT)?,
        };
        self.snapshots.iter().find(|s| s.snapshot_id == id)
    }
}

/// The branch readers read.
pub const MAIN: &str = "main";

/// A schema: a struct whose every field, at any depth, has an id unique in the table.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Schema {
    #[serde(rename = "type", default)]
    pub kind: StructTag,
    #[serde(default)]
    pub schema_id: i32,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub identifier_field_ids: Vec<i32>,
    pub fields: Vec<NestedField>,
}

/// The `"type": "struct"` a schema is written with.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct StructTag;

impl Serialize for StructTag {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str("struct")
    }
}

impl<'de> Deserialize<'de> for StructTag {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        String::deserialize(deserializer).map(|_| StructTag)
    }
}

impl Schema {
    pub fn field(&self, id: i32) -> Option<&NestedField> {
        fn find(fields: &[NestedField], id: i32) -> Option<&NestedField> {
            fields.iter().find_map(|f| {
                if f.id == id {
                    return Some(f);
                }
                match &f.field_type {
                    Type::Struct(s) => find(&s.fields, id),
                    Type::List(l) => match &*l.element {
                        Type::Struct(s) => find(&s.fields, id),
                        _ => None,
                    },
                    _ => None,
                }
            })
        }
        find(&self.fields, id)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct NestedField {
    pub id: i32,
    pub name: String,
    pub required: bool,
    #[serde(rename = "type")]
    pub field_type: Type,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub doc: Option<String>,
    /// The keys this model does not name, such as a version 3 field's defaults, kept as they came so that a schema
    /// evolved from this one keeps them.
    #[serde(flatten)]
    pub other: BTreeMap<String, serde_json::Value>,
}

/// A field's type.
#[derive(Clone, Debug, PartialEq)]
pub enum Type {
    Primitive(Primitive),
    Struct(StructType),
    List(ListType),
    /// A map, kept as written: a record has none, so a table's map column is never written to.
    Map(serde_json::Value),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StructType {
    pub fields: Vec<NestedField>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ListType {
    pub element_id: i32,
    pub element_required: bool,
    pub element: Box<Type>,
}

/// The primitive types. `Other` is one this writer never writes, kept by its name.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Primitive {
    Boolean,
    Int,
    Long,
    Float,
    Double,
    Date,
    Timestamp,
    Timestamptz,
    String,
    Binary,
    Other(String),
}

impl Primitive {
    /// The type as `io::table` reads it; `None` for `Other`.
    pub fn kind(&self) -> Option<Kind> {
        Some(match self {
            Primitive::Boolean => Kind::Boolean,
            Primitive::Int => Kind::Int,
            Primitive::Long => Kind::Long,
            Primitive::Float => Kind::Float,
            Primitive::Double => Kind::Double,
            Primitive::Date => Kind::Date,
            Primitive::Timestamp => Kind::Timestamp,
            Primitive::Timestamptz => Kind::Timestamptz,
            Primitive::String => Kind::String,
            Primitive::Binary => Kind::Binary,
            Primitive::Other(_) => return None,
        })
    }

    fn name(&self) -> &str {
        match self {
            Primitive::Boolean => "boolean",
            Primitive::Int => "int",
            Primitive::Long => "long",
            Primitive::Float => "float",
            Primitive::Double => "double",
            Primitive::Date => "date",
            Primitive::Timestamp => "timestamp",
            Primitive::Timestamptz => "timestamptz",
            Primitive::String => "string",
            Primitive::Binary => "binary",
            Primitive::Other(name) => name,
        }
    }

    fn parse(name: &str) -> Primitive {
        match name {
            "boolean" => Primitive::Boolean,
            "int" => Primitive::Int,
            "long" => Primitive::Long,
            "float" => Primitive::Float,
            "double" => Primitive::Double,
            "date" => Primitive::Date,
            "timestamp" => Primitive::Timestamp,
            "timestamptz" => Primitive::Timestamptz,
            "string" => Primitive::String,
            "binary" => Primitive::Binary,
            other => Primitive::Other(other.to_string()),
        }
    }
}

impl fmt::Display for Primitive {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

impl fmt::Display for Type {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Type::Primitive(p) => p.fmt(f),
            Type::Struct(_) => f.write_str("struct"),
            Type::List(_) => f.write_str("list"),
            Type::Map(_) => f.write_str("map"),
        }
    }
}

impl Serialize for Type {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        match self {
            Type::Primitive(p) => serializer.serialize_str(p.name()),
            Type::Struct(s) => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("type", "struct")?;
                map.serialize_entry("fields", &s.fields)?;
                map.end()
            }
            Type::List(l) => {
                let mut map = serializer.serialize_map(Some(4))?;
                map.serialize_entry("type", "list")?;
                map.serialize_entry("element-id", &l.element_id)?;
                map.serialize_entry("element-required", &l.element_required)?;
                map.serialize_entry("element", &l.element)?;
                map.end()
            }
            Type::Map(m) => m.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for Type {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::Error;
        let value = serde_json::Value::deserialize(deserializer)?;
        match &value {
            serde_json::Value::String(name) => Ok(Type::Primitive(Primitive::parse(name))),
            serde_json::Value::Object(o) => match o.get("type").and_then(|t| t.as_str()) {
                Some("struct") => serde_json::from_value(value)
                    .map(Type::Struct)
                    .map_err(D::Error::custom),
                Some("list") => serde_json::from_value(value).map(Type::List).map_err(D::Error::custom),
                Some("map") => Ok(Type::Map(value)),
                other => Err(D::Error::custom(format!("unknown nested type {other:?}"))),
            },
            other => Err(D::Error::custom(format!(
                "a type is a string or an object, not {other}"
            ))),
        }
    }
}

/// How rows are split into partitions: each field a transform of a source column.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct PartitionSpec {
    pub spec_id: i32,
    pub fields: Vec<PartitionField>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct PartitionField {
    pub name: String,
    pub transform: Transform,
    /// [`NO_SOURCE`] for a version 3 field whose transform takes several columns, which `source_ids` names.
    #[serde(default = "no_source", skip_serializing_if = "is_no_source")]
    pub source_id: i32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_ids: Option<Vec<i32>>,
    pub field_id: i32,
}

/// The `source_id` of a partition field that has none of its own.
pub const NO_SOURCE: i32 = -1;

fn no_source() -> i32 {
    NO_SOURCE
}

fn is_no_source(id: &i32) -> bool {
    *id == NO_SOURCE
}

/// A partition transform, written as the spec names it: `identity`, `day`, `bucket[16]`, `truncate[4]`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Transform {
    Identity,
    Year,
    Month,
    Day,
    Hour,
    Bucket(u32),
    Truncate(u32),
    Void,
    /// One this writer cannot apply, kept by its name; a spec holding it is refused at start.
    Other(String),
}

impl fmt::Display for Transform {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Transform::Identity => f.write_str("identity"),
            Transform::Year => f.write_str("year"),
            Transform::Month => f.write_str("month"),
            Transform::Day => f.write_str("day"),
            Transform::Hour => f.write_str("hour"),
            Transform::Bucket(n) => write!(f, "bucket[{n}]"),
            Transform::Truncate(w) => write!(f, "truncate[{w}]"),
            Transform::Void => f.write_str("void"),
            Transform::Other(name) => f.write_str(name),
        }
    }
}

impl std::str::FromStr for Transform {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Transform, Self::Err> {
        let bracketed = |prefix: &str| {
            s.strip_prefix(prefix)
                .and_then(|r| r.strip_prefix('['))
                .and_then(|r| r.strip_suffix(']'))
                .and_then(|n| n.trim().parse::<u32>().ok())
                .filter(|&n| n > 0)
        };
        Ok(match s {
            "identity" => Transform::Identity,
            "year" => Transform::Year,
            "month" => Transform::Month,
            "day" => Transform::Day,
            "hour" => Transform::Hour,
            "void" => Transform::Void,
            _ => match (bracketed("bucket"), bracketed("truncate")) {
                (Some(n), _) => Transform::Bucket(n),
                (_, Some(w)) => Transform::Truncate(w),
                _ => Transform::Other(s.to_string()),
            },
        })
    }
}

impl Serialize for Transform {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for Transform {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        Ok(s.parse().unwrap_or_else(|never| match never {}))
    }
}

/// One version of the table.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Snapshot {
    pub snapshot_id: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_snapshot_id: Option<i64>,
    #[serde(default)]
    pub sequence_number: i64,
    pub timestamp_ms: i64,
    pub manifest_list: String,
    pub summary: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_id: Option<i32>,
    /// The first row id the snapshot assigns, and how many it assigns. Format version 3 on.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_row_id: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub added_rows: Option<i64>,
}

/// A branch or tag. A commit that moves one keeps its retention: a catalog replaces the whole ref.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct SnapshotRef {
    pub snapshot_id: i64,
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_snapshots_to_keep: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_snapshot_age_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_ref_age_ms: Option<i64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A table as a REST catalog answers it, with keys this model does not name.
    const TABLE: &str = r#"{
      "format-version": 2,
      "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
      "location": "s3://bucket/warehouse/logs",
      "last-sequence-number": 34,
      "last-updated-ms": 1602638573590,
      "last-column-id": 4,
      "current-schema-id": 1,
      "schemas": [
        {"type": "struct", "schema-id": 0, "fields": [{"id": 1, "name": "ts", "required": false, "type": "timestamptz"}]},
        {"type": "struct", "schema-id": 1, "fields": [
          {"id": 1, "name": "ts", "required": false, "type": "timestamptz"},
          {"id": 2, "name": "tags", "required": false, "type": {"type": "list", "element-id": 3, "element-required": false, "element": "string"}},
          {"id": 4, "name": "amount", "required": false, "type": "decimal(9,2)"}
        ]}
      ],
      "default-spec-id": 0,
      "partition-specs": [{"spec-id": 0, "fields": [{"name": "ts_day", "transform": "day", "source-id": 1, "field-id": 1000}]}],
      "last-partition-id": 1000,
      "properties": {"write.format.default": "parquet"},
      "current-snapshot-id": 3055729675574597004,
      "snapshots": [{"snapshot-id": 3055729675574597004, "sequence-number": 34, "timestamp-ms": 1555100955770,
        "manifest-list": "s3://bucket/warehouse/logs/metadata/snap-1.avro", "summary": {"operation": "append"}, "schema-id": 1}],
      "refs": {"main": {"snapshot-id": 3055729675574597004, "type": "branch"}},
      "sort-orders": [{"order-id": 0, "fields": []}],
      "default-sort-order-id": 0
    }"#;

    #[test]
    fn a_table_reads_and_writes_back_with_every_key_it_came_with() {
        let table: TableMetadata = serde_json::from_str(TABLE).unwrap();
        let schema = table.schema().unwrap();
        assert_eq!(schema.fields.len(), 3);
        assert_eq!(
            schema.field(3).map(|f| f.id),
            None,
            "an element id is no field: a list's element is not a named field"
        );
        assert_eq!(
            schema.fields[2].field_type,
            Type::Primitive(Primitive::Other("decimal(9,2)".into()))
        );
        assert_eq!(table.spec().unwrap().fields[0].transform, Transform::Day);
        assert_eq!(table.current_snapshot().unwrap().sequence_number, 34);

        let back: serde_json::Value = serde_json::to_value(&table).unwrap();
        let came: serde_json::Value = serde_json::from_str(TABLE).unwrap();
        assert_eq!(back, came);
    }

    /// A version 3 table, with a field's defaults, a partition field of two columns, and row lineage.
    const TABLE_3: &str = r#"{
      "format-version": 3,
      "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
      "location": "s3://bucket/warehouse/logs",
      "last-sequence-number": 2,
      "last-column-id": 2,
      "current-schema-id": 0,
      "schemas": [{"type": "struct", "schema-id": 0, "fields": [
        {"id": 1, "name": "ts", "required": false, "type": "timestamptz"},
        {"id": 2, "name": "env", "required": false, "type": "string", "initial-default": "prod", "write-default": "dev"}
      ]}],
      "default-spec-id": 0,
      "partition-specs": [
        {"spec-id": 0, "fields": [{"name": "ts_day", "transform": "day", "source-id": 1, "field-id": 1000}]},
        {"spec-id": 1, "fields": [{"name": "both", "transform": "zorder", "source-ids": [1, 2], "field-id": 1001}]}
      ],
      "last-partition-id": 1001,
      "properties": {},
      "next-row-id": 120,
      "refs": {},
      "current-snapshot-id": 7,
      "snapshots": [{"snapshot-id": 7, "sequence-number": 2, "timestamp-ms": 1555100955770,
        "manifest-list": "s3://bucket/warehouse/logs/metadata/snap-7.avro", "summary": {"operation": "append"},
        "first-row-id": 100, "added-rows": 20}]
    }"#;

    #[test]
    fn a_version_3_table_reads_and_writes_back_with_every_key_it_came_with() {
        let table: TableMetadata = serde_json::from_str(TABLE_3).unwrap();
        assert_eq!(table.next_row_id(), Some(120));
        assert_eq!(table.schema().unwrap().fields[1].other["write-default"], "dev");
        let both = &table.partition_specs[1].fields[0];
        assert_eq!((both.source_id, both.source_ids.clone()), (NO_SOURCE, Some(vec![1, 2])));
        let snapshot = table.current_snapshot().unwrap();
        assert_eq!((snapshot.first_row_id, snapshot.added_rows), (Some(100), Some(20)));

        let back: serde_json::Value = serde_json::to_value(&table).unwrap();
        let came: serde_json::Value = serde_json::from_str(TABLE_3).unwrap();
        assert_eq!(back, came);
        // A version 2 table tracks no row lineage, whatever it holds.
        let two = TableMetadata {
            format_version: 2,
            ..table
        };
        assert_eq!(two.next_row_id(), None);
    }

    #[test]
    fn transforms_read_as_the_spec_names_them() {
        for (text, transform) in [
            ("identity", Transform::Identity),
            ("hour", Transform::Hour),
            ("bucket[16]", Transform::Bucket(16)),
            ("truncate[4]", Transform::Truncate(4)),
            ("bucket[0]", Transform::Other("bucket[0]".into())),
            ("zorder", Transform::Other("zorder".into())),
        ] {
            let parsed: Transform = text.parse().unwrap();
            assert_eq!(parsed, transform);
            assert_eq!(parsed.to_string(), text);
        }
    }

    #[test]
    fn a_table_with_no_snapshot_has_no_current_one() {
        let mut table: TableMetadata = serde_json::from_str(TABLE).unwrap();
        table.refs.clear();
        table.current_snapshot_id = Some(NO_SNAPSHOT);
        assert!(table.current_snapshot().is_none());
    }
}
