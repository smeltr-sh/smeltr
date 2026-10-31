//! The table's schema from the route's record, in Delta's JSON schema, with column mapping by name.
//!
//! The record is written as `io::parquet` writes it, and the table's schema mirrors that file field for field: an
//! open struct's `rest` is a string column, a variant a string, a time a `timestamp`. Every column is nullable, since
//! every Parquet column the writer writes is.
//!
//! A missing table takes the record as its schema. A table that exists keeps every column; a field it lacks, at any
//! depth, is added as a nullable column; a field it holds narrower (a `byte`, `short`, `integer`, `float`, `date` or
//! `timestamp_ntz`, as `parquet::narrows` says) is written as that type; a field it holds as another type is refused,
//! and the user renames the field in the pipeline or the column in the table. Under column mapping by name each new
//! column takes the next column id and a physical name of its own, and the Parquet files name their columns by those
//! physical names.

use std::sync::Arc;

use arrow_schema::{DataType as Arrow, Field, Fields, Schema as ArrowSchema};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use serde_json::{Map, Value, json};

use super::log::uuid;
use crate::core::column::DataType;
use crate::io::parquet::{at, columns, narrows};
use crate::io::table::Kind;

const PHYSICAL_NAME: &str = "delta.columnMapping.physicalName";
const COLUMN_ID: &str = "delta.columnMapping.id";
const TYPE_CHANGES: &str = "delta.typeChanges";
const NESTED_IDS: &str = "delta.columnMapping.nested.ids";

/// Spark's JSON struct: the table's schema, or a struct column's type.
#[derive(Clone, Debug, PartialEq)]
pub struct StructType {
    pub fields: Vec<StructField>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct StructField {
    pub name: String,
    pub data_type: Type,
    pub nullable: bool,
    pub metadata: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Type {
    /// A primitive type's name as the log spells it: `long`, `decimal(10,2)`.
    Primitive(String),
    Struct(StructType),
    Array {
        element: Box<Type>,
        contains_null: bool,
    },
    Map {
        key: Box<Type>,
        value: Box<Type>,
        value_contains_null: bool,
    },
}

impl StructType {
    /// The schema in a `metaData` action's `schemaString`.
    pub fn parse(schema_string: &str) -> Result<StructType, String> {
        let value: Value = serde_json::from_str(schema_string).map_err(|e| format!("the table's schema: {e}"))?;
        match parse_type(&value)? {
            Type::Struct(s) => Ok(s),
            other => Err(format!("the table's schema is {}, and must be a struct", other.name())),
        }
    }

    /// The schema as a `metaData` action's `schemaString` carries it.
    pub fn to_json(&self) -> String {
        self.value().to_string()
    }

    fn value(&self) -> Value {
        let fields: Vec<Value> = self
            .fields
            .iter()
            .map(|f| {
                json!({
                    "name": f.name,
                    "type": f.data_type.value(),
                    "nullable": f.nullable,
                    "metadata": f.metadata,
                })
            })
            .collect();
        json!({"type": "struct", "fields": fields})
    }
}

impl StructField {
    /// The name the column has in the data files: its physical name under column mapping, else its name.
    pub fn physical_name(&self) -> &str {
        self.metadata
            .get(PHYSICAL_NAME)
            .and_then(Value::as_str)
            .unwrap_or(&self.name)
    }

    /// The column's id under column mapping, which the data files carry as the Parquet field id.
    pub fn column_id(&self) -> Option<i64> {
        self.metadata.get(COLUMN_ID).and_then(Value::as_i64)
    }
}

impl Type {
    fn value(&self) -> Value {
        match self {
            Type::Primitive(name) => Value::String(name.clone()),
            Type::Struct(s) => s.value(),
            Type::Array { element, contains_null } => json!({
                "type": "array",
                "elementType": element.value(),
                "containsNull": contains_null,
            }),
            Type::Map {
                key,
                value,
                value_contains_null,
            } => json!({
                "type": "map",
                "keyType": key.value(),
                "valueType": value.value(),
                "valueContainsNull": value_contains_null,
            }),
        }
    }

    /// The type's name as a refusal spells it.
    fn name(&self) -> &str {
        match self {
            Type::Primitive(name) => name,
            Type::Struct(_) => "struct",
            Type::Array { .. } => "array",
            Type::Map { .. } => "map",
        }
    }
}

fn parse_type(value: &Value) -> Result<Type, String> {
    let object = match value {
        Value::String(name) => return Ok(Type::Primitive(name.clone())),
        Value::Object(object) => object,
        other => return Err(format!("a type in the table's schema is {other}")),
    };
    let tag = object.get("type").and_then(Value::as_str);
    let get = |key: &str| {
        object
            .get(key)
            .ok_or_else(|| format!("a {} type in the table's schema has no {key}", tag.unwrap_or("")))
    };
    let flag = |key: &str| {
        get(key)?
            .as_bool()
            .ok_or_else(|| format!("{key} in the table's schema is no boolean"))
    };
    match tag {
        Some("struct") => {
            let fields = get("fields")?
                .as_array()
                .ok_or("a struct's fields in the table's schema are no array")?;
            Ok(Type::Struct(StructType {
                fields: fields.iter().map(parse_field).collect::<Result<_, _>>()?,
            }))
        }
        Some("array") => Ok(Type::Array {
            element: Box::new(parse_type(get("elementType")?)?),
            contains_null: flag("containsNull")?,
        }),
        Some("map") => Ok(Type::Map {
            key: Box::new(parse_type(get("keyType")?)?),
            value: Box::new(parse_type(get("valueType")?)?),
            value_contains_null: flag("valueContainsNull")?,
        }),
        _ => Err(format!(
            "the table's schema holds a type this writer does not know: {value}"
        )),
    }
}

fn parse_field(value: &Value) -> Result<StructField, String> {
    let object = value
        .as_object()
        .ok_or_else(|| format!("a field in the table's schema is {value}"))?;
    let name = object
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| format!("a field in the table's schema has no name: {value}"))?;
    let data_type = parse_type(
        object
            .get("type")
            .ok_or_else(|| format!("the table's column {name} has no type"))?,
    )?;
    let nullable = object
        .get("nullable")
        .and_then(Value::as_bool)
        .ok_or_else(|| format!("the table's column {name} does not say whether it is nullable"))?;
    let metadata = match object.get("metadata") {
        None => Map::new(),
        Some(Value::Object(m)) => m.clone(),
        Some(other) => return Err(format!("the table's column {name} has metadata {other}")),
    };
    Ok(StructField {
        name: name.to_string(),
        data_type,
        nullable,
        metadata,
    })
}

/// The table's column mapping: `delta.columnMapping.mode` absent or `none`, or `name` or `id` with
/// `delta.columnMapping.maxColumnId`. A writer writes `name` and `id` alike.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mapping {
    None,
    /// `iceberg` when the table is IcebergCompatV2: each new array's element, and map's key and value, takes an id
    /// under `delta.columnMapping.nested.ids` too, and a column widens only as Iceberg lets it.
    Name {
        max_column_id: i64,
        iceberg: bool,
    },
}

/// The table's schema for a record, and whether it differs from the table's current one.
#[derive(Clone, Debug, PartialEq)]
pub struct Evolution {
    pub schema: StructType,
    /// The table's mapping with the schema's new columns counted: under `Name`, the highest column id assigned.
    pub mapping: Mapping,
    /// Whether `schema` is new to the table: the table is created, a column added or one widened. A commit then sets
    /// it.
    pub changed: bool,
    /// Whether a column was widened.
    pub widened: bool,
}

/// The schema a table must have for `record` to be written to it. With no `current` schema the table is new, and the
/// caller passes `Mapping::None`, or `Mapping::Name { max_column_id: 0, iceberg: false }` to map it. Under `Name` each new column,
/// nested ones too, takes the next id above `max_column_id` and a fresh physical name; under `None` it takes no
/// metadata. When the table lets its columns widen, `widen` names its partition columns, and every other column it
/// holds narrower than the record types is widened to the record's type where the protocol allows, the change
/// recorded under `delta.typeChanges`. A partition column keeps its type: its values are text in the log, which a
/// reader of the wider type may not read.
///
/// Err names the field when the record is no struct, a column holds a type other than the record's, a column is not
/// nullable, or a new column's name differs only in case from another's.
pub fn table_schema(
    record: &DataType,
    current: Option<&StructType>,
    mapping: Mapping,
    widen: Option<&[String]>,
) -> Result<Evolution, String> {
    let DataType::Struct(fields, open) = record else {
        return Err(format!(
            "the record is of type {record}, and a table's schema is a struct"
        ));
    };
    let columns = columns(fields, *open);
    let mut ids = Ids {
        mapping,
        widen: widen.map(<[String]>::to_vec),
        changed: false,
        widened: false,
        type_changes: Vec::new(),
    };
    let fields = match current {
        None => ids.fresh_fields(&columns, "")?,
        Some(current) => ids.evolve_fields(&current.fields, &columns, "")?,
    };
    Ok(Evolution {
        schema: StructType { fields },
        mapping: ids.mapping,
        changed: current.is_none() || ids.changed,
        widened: ids.widened,
    })
}

/// The arrow schema the Parquet files are written with: `arrow` with every field `schema` names, at any depth and
/// through a list's element, renamed to its physical name and carrying its column id under `PARQUET:field_id`.
/// Fields are matched by name; one `schema` does not name is kept as it is.
pub fn physical(arrow: &ArrowSchema, schema: &StructType) -> Arc<ArrowSchema> {
    let fields: Fields = arrow.fields().iter().map(|f| mapped(f, &schema.fields)).collect();
    Arc::new(ArrowSchema::new_with_metadata(fields, arrow.metadata().clone()))
}

fn mapped(field: &Field, table: &[StructField]) -> Field {
    let Some(t) = table.iter().find(|t| t.name == *field.name()) else {
        return field.clone();
    };
    let id = t.column_id();
    let nested = t.metadata.get(NESTED_IDS).and_then(Value::as_object);
    let data_type = mapped_type(field.data_type(), &t.data_type, nested, t.physical_name());
    with_id(field.clone().with_name(t.physical_name()).with_data_type(data_type), id)
}

/// `table`'s type for `arrow`'s, with each list's element carrying its id in `nested`, the ids of the nearest struct
/// field's arrays, under the key `key` and `.element`.
fn mapped_type(arrow: &Arrow, table: &Type, nested: Option<&Map<String, Value>>, key: &str) -> Arrow {
    match (arrow, table) {
        (Arrow::Struct(fields), Type::Struct(s)) => {
            Arrow::Struct(fields.iter().map(|f| mapped(f, &s.fields)).collect())
        }
        (Arrow::List(element), Type::Array { element: t, .. }) => {
            let key = format!("{key}.element");
            let id = nested.and_then(|ids| ids.get(&key)).and_then(Value::as_i64);
            let data_type = mapped_type(element.data_type(), t, nested, &key);
            Arrow::List(Arc::new(with_id(
                element.as_ref().clone().with_data_type(data_type),
                id,
            )))
        }
        (other, Type::Primitive(p)) => narrow(p).unwrap_or_else(|| other.clone()),
        (other, _) => other.clone(),
    }
}

fn with_id(field: Field, id: Option<i64>) -> Field {
    let Some(id) = id else { return field };
    let mut metadata = field.metadata().clone();
    metadata.insert(PARQUET_FIELD_ID_META_KEY.to_string(), id.to_string());
    field.with_metadata(metadata)
}

/// The Parquet type a column of the primitive the log names `p` is written as, where it is narrower than any record
/// type.
pub fn narrow(p: &str) -> Option<Arrow> {
    kind(p).as_ref().and_then(Kind::narrowed)
}

/// The kind of the primitive type the log names `primitive`, when it is one this writer reads a value of.
pub fn kind(primitive: &str) -> Option<Kind> {
    match primitive {
        "boolean" => Some(Kind::Boolean),
        "byte" => Some(Kind::Byte),
        "short" => Some(Kind::Short),
        "integer" => Some(Kind::Int),
        "long" => Some(Kind::Long),
        "float" => Some(Kind::Float),
        "double" => Some(Kind::Double),
        "date" => Some(Kind::Date),
        "timestamp" => Some(Kind::Timestamptz),
        "timestamp_ntz" => Some(Kind::Timestamp),
        "string" => Some(Kind::String),
        "binary" => Some(Kind::Binary),
        _ => None,
    }
}

/// The top-level column `name` a table is partitioned by: its physical name and its kind.
///
/// Err when the table has no such column, or holds it as a type no partition value of this writer's takes.
pub fn partition_column(schema: &StructType, name: &str) -> Result<(String, Kind), String> {
    let field = schema
        .fields
        .iter()
        .find(|f| f.name == name)
        .ok_or_else(|| format!("the table is partitioned by {name}, which is no column of its schema"))?;
    let kind = match &field.data_type {
        Type::Primitive(p) => kind(p),
        _ => None,
    };
    let kind = kind.ok_or_else(|| {
        format!(
            "the table is partitioned by {name}, a {} column, and this writer writes no partition value of that type",
            field.data_type.name()
        )
    })?;
    Ok((field.physical_name().to_string(), kind))
}

/// Whether `new` is `old` with only some of its types widened, as another writer widens a table: each of `old`'s
/// fields still in it, the type of each the same or one the protocol lets it widen to, and each widened field's type
/// changes its earlier ones and more. Files of `old`'s types may still be added to such a table.
pub fn only_widened(old: &StructType, new: &StructType) -> bool {
    old.fields.iter().all(|old| {
        let Some(new) = new.fields.iter().find(|f| f.name == old.name) else {
            return false;
        };
        let changes = |f: &StructField| f.metadata.get(TYPE_CHANGES).and_then(Value::as_array).cloned();
        let (was, now) = (changes(old).unwrap_or_default(), changes(new).unwrap_or_default());
        let recorded = old.data_type == new.data_type || (now.len() > was.len() && now.starts_with(&was));
        old.nullable == new.nullable && recorded && widens(&old.data_type, &new.data_type)
    })
}

/// Whether type `from` is `to`, or widens to it as the protocol's type widening allows.
fn widens(from: &Type, to: &Type) -> bool {
    match (from, to) {
        (Type::Primitive(from), Type::Primitive(to)) => {
            let integers = ["byte", "short", "integer", "long"];
            let rank = |t: &str| integers.iter().position(|i| *i == t);
            from == to
                || rank(from).zip(rank(to)).is_some_and(|(from, to)| from < to)
                || matches!(
                    (from.as_str(), to.as_str()),
                    ("float" | "byte" | "short" | "integer", "double") | ("date", "timestamp_ntz")
                )
        }
        (Type::Struct(from), Type::Struct(to)) => only_widened(from, to),
        (
            Type::Array {
                element: from,
                contains_null: a,
            },
            Type::Array {
                element: to,
                contains_null: b,
            },
        ) => a == b && widens(from, to),
        (
            Type::Map {
                key: k1,
                value: v1,
                value_contains_null: a,
            },
            Type::Map {
                key: k2,
                value: v2,
                value_contains_null: b,
            },
        ) => a == b && widens(k1, k2) && widens(v1, v2),
        _ => false,
    }
}

/// The keys `delta.columnMapping.nested.ids` gives the arrays and maps in `t`, the type of the struct field whose
/// physical name is `key`. A struct's own fields key theirs under their own names.
fn nested_keys(t: &Type, key: &str, out: &mut Vec<String>) {
    let mut at = |suffix: &str, t: &Type| {
        let key = format!("{key}.{suffix}");
        out.push(key.clone());
        nested_keys(t, &key, out);
    };
    match t {
        Type::Array { element, .. } => at("element", element),
        Type::Map { key: k, value, .. } => {
            at("key", k);
            at("value", value);
        }
        Type::Primitive(_) | Type::Struct(_) => {}
    }
}

/// Every struct field in `fields`, at any depth.
fn struct_fields<'a>(fields: &'a [StructField], out: &mut Vec<&'a StructField>) {
    fn of<'a>(t: &'a Type, out: &mut Vec<&'a StructField>) {
        match t {
            Type::Primitive(_) => {}
            Type::Struct(s) => struct_fields(&s.fields, out),
            Type::Array { element, .. } => of(element, out),
            Type::Map { key, value, .. } => {
                of(key, out);
                of(value, out);
            }
        }
    }
    for f in fields {
        out.push(f);
        of(&f.data_type, out);
    }
}

/// The largest id `delta.columnMapping.nested.ids` gives in `schema`; 0 when it gives none.
pub fn max_nested_id(schema: &StructType) -> i64 {
    let mut fields = Vec::new();
    struct_fields(&schema.fields, &mut fields);
    (fields.iter())
        .filter_map(|f| f.metadata.get(NESTED_IDS).and_then(Value::as_object))
        .flat_map(|ids| ids.values().filter_map(Value::as_i64))
        .max()
        .unwrap_or(0)
}

/// What keeps `schema` from an IcebergCompatV2 table this writer writes: an array's element, or a map's key or value,
/// without its id, or a type change Iceberg does not make. The message names the column.
pub fn not_iceberg(schema: &StructType) -> Option<String> {
    let mut fields = Vec::new();
    struct_fields(&schema.fields, &mut fields);
    fields.into_iter().find_map(|f| {
        let mut keys = Vec::new();
        nested_keys(&f.data_type, f.physical_name(), &mut keys);
        let ids = f.metadata.get(NESTED_IDS).and_then(Value::as_object);
        let missing = keys
            .iter()
            .find(|k| !ids.is_some_and(|ids| ids.get(*k).is_some_and(Value::is_i64)));
        if let Some(key) = missing {
            return Some(format!(
                "the table is IcebergCompatV2, and its column {} gives {key} no id in {NESTED_IDS}",
                f.name
            ));
        }
        let changes = f.metadata.get(TYPE_CHANGES).and_then(Value::as_array);
        let change = changes.into_iter().flatten().find(|c| {
            let (from, to) = (c["fromType"].as_str(), c["toType"].as_str());
            !from.zip(to).is_some_and(|(from, to)| iceberg_widens(from, to))
        })?;
        Some(format!(
            "the table is IcebergCompatV2, and its column {} changed type as Iceberg does not: {change}",
            f.name
        ))
    })
}

/// Whether Iceberg's schema evolution changes primitive `from` to `to`: an integer to a wider one, a float to a double,
/// a decimal to one of more precision and the same scale. Iceberg holds a `byte` or `short` as an int.
fn iceberg_widens(from: &str, to: &str) -> bool {
    let integers = ["byte", "short", "integer", "long"];
    let rank = |t: &str| integers.iter().position(|i| *i == t);
    let decimal = |t: &str| {
        let (p, s) = t.strip_prefix("decimal(")?.strip_suffix(')')?.split_once(',')?;
        Some((p.trim().parse::<u32>().ok()?, s.trim().parse::<u32>().ok()?))
    };
    rank(from).zip(rank(to)).is_some_and(|(from, to)| from < to)
        || (from, to) == ("float", "double")
        || decimal(from)
            .zip(decimal(to))
            .is_some_and(|((p1, s1), (p2, s2))| p1 < p2 && s1 == s2)
}

/// The first column, at any depth, whose metadata asks of a writer what this one does not do: compute a generated
/// value or a default, check an invariant, or generate an identity below the top level. The message names the column
/// and what it carries.
pub fn refused_column(schema: &StructType) -> Option<String> {
    if all_void(&schema.fields) {
        return Some("every column of the table is void, which no data file can hold".to_string());
    }
    refused_in(&schema.fields, "")
}

fn is_void(t: &Type) -> bool {
    matches!(t, Type::Primitive(p) if p == "void")
}

/// Whether `t` holds a void anywhere: a data file has no place for it inside an array or a map.
fn holds_void(t: &Type) -> bool {
    match t {
        Type::Primitive(_) => is_void(t),
        Type::Struct(s) => s.fields.iter().any(|f| holds_void(&f.data_type)),
        Type::Array { element, .. } => holds_void(element),
        Type::Map { key, value, .. } => holds_void(key) || holds_void(value),
    }
}

fn all_void(fields: &[StructField]) -> bool {
    !fields.is_empty() && fields.iter().all(|f| is_void(&f.data_type))
}

fn refused_in(fields: &[StructField], path: &str) -> Option<String> {
    let nested = !path.is_empty();
    fields.iter().find_map(|f| {
        let path = at(path, &f.name);
        let key = f.metadata.keys().find(|k| {
            matches!(
                k.as_str(),
                "delta.generationExpression" | "CURRENT_DEFAULT" | "delta.invariants"
            ) || (nested && k.starts_with("delta.identity."))
        });
        match key {
            Some(key) => Some(format!(
                "the table's column {path} carries {key}, which this writer cannot honour"
            )),
            None => refused_type(&f.data_type, &path),
        }
    })
}

fn refused_type(t: &Type, path: &str) -> Option<String> {
    // The protocol's void rules: a writer writes no file while the schema has a void a file cannot leave out.
    if matches!(t, Type::Array { .. } | Type::Map { .. }) && holds_void(t) {
        return Some(format!(
            "the table's column {path} holds a void inside an array or a map, which no data file can hold"
        ));
    }
    match t {
        Type::Primitive(_) => None,
        Type::Struct(s) if all_void(&s.fields) => Some(format!(
            "the table's column {path} is a struct of only void fields, which no data file can hold"
        )),
        Type::Struct(s) => refused_in(&s.fields, path),
        Type::Array { element, .. } => refused_type(element, &format!("{path}[]")),
        Type::Map { key, value, .. } => {
            refused_type(key, &format!("{path}.key")).or_else(|| refused_type(value, &format!("{path}.value")))
        }
    }
}

/// The Delta type's name a record's primitive is written as.
fn primitive(t: &DataType) -> &'static str {
    match t {
        DataType::Bool => "boolean",
        DataType::I64 | DataType::Duration => "long",
        DataType::F64 => "double",
        DataType::DateTime => "timestamp",
        DataType::Str | DataType::Variant => "string",
        DataType::Bytes => "binary",
        DataType::Null => unreachable!("`columns` leaves out a column that holds no value"),
        DataType::Array(_) | DataType::Struct(..) => unreachable!("a nested type is no primitive"),
    }
}

/// The Delta type's name the record's type is written as.
fn written(t: &DataType) -> &'static str {
    match t {
        DataType::Array(_) => "array",
        DataType::Struct(..) => "struct",
        t => primitive(t),
    }
}

/// The type a column the table holds as `p` widens to for the record's `t`, as the protocol's type widening allows;
/// None when it holds `t` as it is, or cannot widen to it.
fn widened(p: &str, t: &DataType) -> Option<&'static str> {
    match (p, primitive(t)) {
        ("byte" | "short" | "integer", "long") => Some("long"),
        ("float", "double") => Some("double"),
        // A time with a zone is no widening of a date: the closest the protocol allows is one without.
        ("date", "timestamp") => Some("timestamp_ntz"),
        _ => None,
    }
}

/// Makes new columns, each with the next id and a fresh physical name under `Name`, and widens columns where the
/// table lets them widen.
struct Ids {
    mapping: Mapping,
    /// The table's partition columns when it lets its columns widen. They are not widened.
    widen: Option<Vec<String>>,
    /// Whether a column was added or widened.
    changed: bool,
    widened: bool,
    /// The type changes of the field being evolved, as `delta.typeChanges` records them, not yet recorded.
    type_changes: Vec<Value>,
}

impl Ids {
    fn new_field(&mut self, name: &str) -> Result<StructField, String> {
        self.changed = true;
        let mut metadata = Map::new();
        if let Mapping::Name { max_column_id, .. } = &mut self.mapping {
            *max_column_id += 1;
            metadata.insert(COLUMN_ID.to_string(), json!(*max_column_id));
            metadata.insert(PHYSICAL_NAME.to_string(), json!(format!("col-{}", uuid())));
        }
        Ok(StructField {
            name: name.to_string(),
            // Set once the field's children have their ids.
            data_type: Type::Primitive(String::new()),
            nullable: true,
            metadata,
        })
    }

    /// Gives each array's element in the new `field`'s type its id under `delta.columnMapping.nested.ids`, after the
    /// ids of the struct fields within, when the table is IcebergCompatV2.
    fn nest(&mut self, field: &mut StructField) {
        let Mapping::Name {
            max_column_id,
            iceberg: true,
        } = &mut self.mapping
        else {
            return;
        };
        let mut keys = Vec::new();
        nested_keys(&field.data_type, field.physical_name(), &mut keys);
        if keys.is_empty() {
            return;
        }
        let ids = keys.into_iter().map(|key| {
            *max_column_id += 1;
            (key, json!(*max_column_id))
        });
        field
            .metadata
            .insert(NESTED_IDS.to_string(), Value::Object(ids.collect()));
    }

    /// New fields for `columns`. A struct's fields take their ids before any field's children do.
    fn fresh_fields(&mut self, columns: &[(String, DataType)], path: &str) -> Result<Vec<StructField>, String> {
        let mut fields = columns
            .iter()
            .map(|(name, _)| self.new_field(name))
            .collect::<Result<Vec<_>, _>>()?;
        for (field, (name, t)) in fields.iter_mut().zip(columns) {
            field.data_type = self.fresh_type(t, &at(path, name))?;
            self.nest(field);
        }
        unique(&fields, path)?;
        Ok(fields)
    }

    fn fresh_type(&mut self, t: &DataType, path: &str) -> Result<Type, String> {
        Ok(match t {
            DataType::Array(element) => Type::Array {
                element: Box::new(self.fresh_type(element, &format!("{path}[]"))?),
                contains_null: true,
            },
            DataType::Struct(fields, open) => Type::Struct(StructType {
                fields: self.fresh_fields(&columns(fields, *open), path)?,
            }),
            t => Type::Primitive(primitive(t).to_string()),
        })
    }

    /// The table's fields `table` with the record's `columns` met: each table field kept, its metadata too, and each
    /// column the table lacks added after them, its id before its children's.
    fn evolve_fields(
        &mut self,
        table: &[StructField],
        columns: &[(String, DataType)],
        path: &str,
    ) -> Result<Vec<StructField>, String> {
        let mut fields = Vec::with_capacity(table.len().max(columns.len()));
        let top = path.is_empty();
        for field in table {
            let path = at(path, &field.name);
            // Every column is written nullable, and a column the record does not write is null in its rows, but for an
            // identity column, whose values the writer generates.
            let written = columns.iter().any(|(name, _)| *name == field.name);
            let generated = top && !written && field.metadata.keys().any(|k| k.starts_with("delta.identity."));
            if !field.nullable && !generated {
                return Err(format!(
                    "the table's column {path} is not nullable, and the record may leave it null"
                ));
            }
            match columns.iter().find(|(name, _)| *name == field.name) {
                Some((_, t)) => {
                    let mut field = field.clone();
                    let outer = std::mem::take(&mut self.type_changes);
                    let partition = path == field.name && self.widen.as_ref().is_some_and(|p| p.contains(&field.name));
                    let widen = if partition { self.widen.take() } else { None };
                    field.data_type = self.evolve_type(&field.data_type, t, &path, "")?;
                    if partition {
                        self.widen = widen;
                    }
                    let changes = std::mem::replace(&mut self.type_changes, outer);
                    if !changes.is_empty() {
                        // The field's earlier changes are kept, as the protocol asks, and its new ones follow them.
                        let recorded = field
                            .metadata
                            .entry(TYPE_CHANGES.to_string())
                            .or_insert_with(|| json!([]));
                        let Some(recorded) = recorded.as_array_mut() else {
                            return Err(format!("the table's column {path} has type changes {recorded}"));
                        };
                        recorded.extend(changes);
                    }
                    fields.push(field);
                }
                None => fields.push(field.clone()),
            }
        }
        for (name, t) in columns {
            if !table.iter().any(|f| f.name == *name) {
                let mut field = self.new_field(name)?;
                field.data_type = self.fresh_type(t, &at(path, name))?;
                self.nest(&mut field);
                fields.push(field);
            }
        }
        unique(&fields, path)?;
        Ok(fields)
    }

    /// The table's type `table` met by the record's `record`. `within` is the path from the nearest struct field to
    /// `table`, as `delta.typeChanges` names it: empty at the field, `element` within its array.
    fn evolve_type(&mut self, table: &Type, record: &DataType, path: &str, within: &str) -> Result<Type, String> {
        match (table, record) {
            (Type::Primitive(p), t)
                if self.widen.is_some() && !matches!(t, DataType::Array(_) | DataType::Struct(..)) =>
            {
                let iceberg = matches!(self.mapping, Mapping::Name { iceberg: true, .. });
                match widened(p, t).filter(|to| !iceberg || iceberg_widens(p, to)) {
                    Some(to) => {
                        let mut change = json!({"fromType": p, "toType": to});
                        if !within.is_empty() {
                            change["fieldPath"] = json!(within);
                        }
                        self.type_changes.push(change);
                        self.changed = true;
                        self.widened = true;
                        Ok(Type::Primitive(to.to_string()))
                    }
                    None => self.kept(table, record, path),
                }
            }
            (Type::Primitive(_), _) => self.kept(table, record, path),
            (Type::Struct(s), DataType::Struct(fields, open)) => Ok(Type::Struct(StructType {
                fields: self.evolve_fields(&s.fields, &columns(fields, *open), path)?,
            })),
            (
                Type::Array {
                    contains_null: false, ..
                },
                DataType::Array(_),
            ) => Err(format!(
                "the table's array {path} holds no null element, and the record may write one"
            )),
            (Type::Array { element, contains_null }, DataType::Array(e)) => {
                let within = if within.is_empty() {
                    "element".to_string()
                } else {
                    format!("{within}.element")
                };
                Ok(Type::Array {
                    element: Box::new(self.evolve_type(element, e, &format!("{path}[]"), &within)?),
                    contains_null: *contains_null,
                })
            }
            _ => self.kept(table, record, path),
        }
    }

    /// The table's primitive type `table`, when it holds the record's `record` as it is or narrower. Err names the
    /// column otherwise.
    fn kept(&self, table: &Type, record: &DataType, path: &str) -> Result<Type, String> {
        match (table, record) {
            (Type::Primitive(p), t)
                if !matches!(t, DataType::Array(_) | DataType::Struct(..))
                    && (p == primitive(t) || narrow(p).is_some_and(|to| narrows(t, &to))) =>
            {
                Ok(table.clone())
            }
            _ => Err(format!(
                "{path} is {} in the table, and the record writes {} there as {}; rename the field in the pipeline \
                 or the column in the table",
                table.name(),
                record.spell(),
                written(record),
            )),
        }
    }
}

/// Refuses two columns of one struct whose names differ only in case, as Delta's column names must not.
fn unique(fields: &[StructField], path: &str) -> Result<(), String> {
    for (i, a) in fields.iter().enumerate() {
        let lower = a.name.to_lowercase();
        if let Some(b) = fields[i + 1..].iter().find(|b| b.name.to_lowercase() == lower) {
            return Err(format!(
                "{} and {} differ only in case, and a Delta table's column names must not; rename the field in the \
                 pipeline or the column in the table",
                at(path, &a.name),
                at(path, &b.name),
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::parquet::REST;

    fn st(fields: &[(&str, DataType)], open: bool) -> DataType {
        DataType::Struct(fields.iter().map(|(n, t)| (n.to_string(), t.clone())).collect(), open)
    }

    fn arr(t: DataType) -> DataType {
        DataType::Array(Box::new(t))
    }

    fn prim(p: &str) -> Type {
        Type::Primitive(p.to_string())
    }

    fn plain(name: &str, data_type: Type) -> StructField {
        StructField {
            name: name.to_string(),
            data_type,
            nullable: true,
            metadata: Map::new(),
        }
    }

    fn strukt(fields: &[StructField]) -> Type {
        Type::Struct(StructType {
            fields: fields.to_vec(),
        })
    }

    fn field<'a>(s: &'a StructType, name: &str) -> &'a StructField {
        s.fields.iter().find(|f| f.name == name).unwrap()
    }

    fn inner(t: &Type) -> &StructType {
        match t {
            Type::Struct(s) => s,
            Type::Array { element, .. } => inner(element),
            other => panic!("{other:?} holds no struct"),
        }
    }

    /// `{ts: datetime, http: {status: i64, hdrs: array<str>}, tags: array<{k: str, ...}>, raw: bytes, v: variant}`
    fn record() -> DataType {
        st(
            &[
                ("ts", DataType::DateTime),
                (
                    "http",
                    st(&[("status", DataType::I64), ("hdrs", arr(DataType::Str))], false),
                ),
                ("tags", arr(st(&[("k", DataType::Str)], true))),
                ("raw", DataType::Bytes),
                ("v", DataType::Variant),
            ],
            false,
        )
    }

    /// Every field's (path, id), a struct's fields in order before their children.
    fn ids(fields: &[StructField], path: &str, out: &mut Vec<(String, Option<i64>)>) {
        for f in fields {
            out.push((at(path, &f.name), f.column_id()));
        }
        for f in fields {
            if let Some(s) = nested(&f.data_type) {
                ids(&s.fields, &at(path, &f.name), out);
            }
        }
    }

    fn nested(t: &Type) -> Option<&StructType> {
        match t {
            Type::Struct(s) => Some(s),
            Type::Array { element, .. } => nested(element),
            _ => None,
        }
    }

    #[test]
    fn the_json_round_trips() {
        let text = r#"{"type":"struct","fields":[
            {"name":"a","type":"integer","nullable":false,"metadata":{"comment":"x","delta.columnMapping.id":1}},
            {"name":"b","type":{"type":"struct","fields":[
                {"name":"d","type":"decimal(10,2)","nullable":true,"metadata":{"custom":{"k":[1,2]}}}]},
             "nullable":true,"metadata":{}},
            {"name":"c","type":{"type":"array","elementType":"long","containsNull":false},"nullable":true,"metadata":{}},
            {"name":"m","type":{"type":"map","keyType":"string","valueType":
                {"type":"array","elementType":"string","containsNull":true},"valueContainsNull":true},
             "nullable":true,"metadata":{}}]}"#;
        let schema = StructType::parse(text).unwrap();
        assert_eq!(
            schema.fields[1].data_type,
            strukt(&[StructField {
                metadata: json!({"custom": {"k": [1, 2]}}).as_object().unwrap().clone(),
                ..plain("d", prim("decimal(10,2)"))
            }])
        );
        assert_eq!(
            schema.fields[3].data_type,
            Type::Map {
                key: Box::new(prim("string")),
                value: Box::new(Type::Array {
                    element: Box::new(prim("string")),
                    contains_null: true
                }),
                value_contains_null: true,
            }
        );
        assert_eq!(schema.fields[0].column_id(), Some(1));
        assert_eq!(schema.fields[0].physical_name(), "a");
        let again = StructType::parse(&schema.to_json()).unwrap();
        assert_eq!(again, schema);
        assert_eq!(
            serde_json::from_str::<Value>(&schema.to_json()).unwrap(),
            serde_json::from_str::<Value>(text).unwrap()
        );
    }

    #[test]
    fn a_new_table_takes_the_record_with_ids_a_level_at_a_time() {
        let e = table_schema(
            &record(),
            None,
            Mapping::Name {
                max_column_id: 0,
                iceberg: false,
            },
            None,
        )
        .unwrap();
        assert!(e.changed);
        assert_eq!(
            e.mapping,
            Mapping::Name {
                max_column_id: 9,
                iceberg: false
            }
        );
        let mut got = Vec::new();
        ids(&e.schema.fields, "", &mut got);
        let want: Vec<(String, Option<i64>)> = [
            "ts",
            "http",
            "tags",
            "raw",
            "v",
            "http.status",
            "http.hdrs",
            "tags.k",
            "tags.rest",
        ]
        .iter()
        .zip(1..)
        .map(|(p, id)| (p.to_string(), Some(id)))
        .collect();
        assert_eq!(got, want);

        let mut names = Vec::new();
        fn physical_names<'a>(fields: &'a [StructField], out: &mut Vec<&'a str>) {
            for f in fields {
                out.push(f.physical_name());
                if let Some(s) = nested(&f.data_type) {
                    physical_names(&s.fields, out);
                }
            }
        }
        physical_names(&e.schema.fields, &mut names);
        assert_eq!(names.len(), 9);
        for n in &names {
            let uuid = n.strip_prefix("col-").unwrap();
            assert_eq!(uuid.len(), 36);
            assert_eq!(&uuid[14..15], "4");
            assert!(
                uuid.chars()
                    .all(|c| c == '-' || c.is_ascii_digit() || ('a'..='f').contains(&c))
            );
        }
        let mut unique = names.clone();
        unique.sort();
        unique.dedup();
        assert_eq!(unique.len(), names.len());

        assert!(e.schema.fields.iter().all(|f| f.nullable));
        assert_eq!(field(&e.schema, "ts").data_type, prim("timestamp"));
        assert_eq!(field(&e.schema, "raw").data_type, prim("binary"));
        assert_eq!(field(&e.schema, "v").data_type, prim("string"));
        assert!(matches!(
            field(&e.schema, "tags").data_type,
            Type::Array {
                contains_null: true,
                ..
            }
        ));
        assert_eq!(
            StructType::parse(&e.schema.to_json()).unwrap(),
            e.schema,
            "the ids are JSON ints"
        );
    }

    #[test]
    fn the_same_record_changes_nothing() {
        let first = table_schema(
            &record(),
            None,
            Mapping::Name {
                max_column_id: 0,
                iceberg: false,
            },
            None,
        )
        .unwrap();
        let again = table_schema(&record(), Some(&first.schema), first.mapping, None).unwrap();
        assert!(!again.changed);
        assert_eq!(again.mapping, first.mapping);
        assert_eq!(again.schema, first.schema);
    }

    fn grown() -> DataType {
        st(
            &[
                ("ts", DataType::DateTime),
                (
                    "http",
                    st(
                        &[
                            ("status", DataType::I64),
                            ("hdrs", arr(DataType::Str)),
                            ("peer", st(&[("ip", DataType::Str)], false)),
                        ],
                        false,
                    ),
                ),
                ("tags", arr(st(&[("k", DataType::Str), ("n", DataType::F64)], true))),
                ("raw", DataType::Bytes),
                ("v", DataType::Variant),
            ],
            false,
        )
    }

    #[test]
    fn a_new_nested_field_is_added_with_a_new_id_and_a_column_not_written_is_kept() {
        let mut table = table_schema(
            &record(),
            None,
            Mapping::Name {
                max_column_id: 0,
                iceberg: false,
            },
            None,
        )
        .unwrap()
        .schema;
        table.fields.push(StructField {
            metadata: json!({COLUMN_ID: 20, PHYSICAL_NAME: "col-gone"})
                .as_object()
                .unwrap()
                .clone(),
            ..plain("gone", prim("integer"))
        });
        let e = table_schema(
            &grown(),
            Some(&table),
            Mapping::Name {
                max_column_id: 20,
                iceberg: false,
            },
            None,
        )
        .unwrap();
        assert!(e.changed);
        assert_eq!(
            e.mapping,
            Mapping::Name {
                max_column_id: 23,
                iceberg: false
            }
        );
        let http = inner(&field(&e.schema, "http").data_type);
        let peer = field(http, "peer");
        assert_eq!(peer.column_id(), Some(21));
        assert!(peer.physical_name().starts_with("col-"));
        assert_eq!(field(inner(&peer.data_type), "ip").column_id(), Some(22));
        let tags = inner(&field(&e.schema, "tags").data_type);
        let names: Vec<&str> = tags.fields.iter().map(|f| f.name.as_str()).collect();
        assert_eq!(names, ["k", REST, "n"]);
        assert_eq!(field(tags, "n").column_id(), Some(23));
        assert_eq!(field(tags, "n").data_type, prim("double"));
        assert_eq!(e.schema.fields[5], table.fields[5]);
        assert_eq!(field(&e.schema, "ts"), field(&table, "ts"));
    }

    #[test]
    fn without_mapping_a_new_field_takes_no_metadata() {
        let table = table_schema(&record(), None, Mapping::None, None).unwrap().schema;
        let e = table_schema(&grown(), Some(&table), Mapping::None, None).unwrap();
        assert!(e.changed);
        assert_eq!(e.mapping, Mapping::None);
        let http = inner(&field(&e.schema, "http").data_type);
        assert_eq!(
            *field(http, "peer"),
            plain("peer", strukt(&[plain("ip", prim("string"))]))
        );
        assert_eq!(
            field(inner(&field(&e.schema, "tags").data_type), "n"),
            &plain("n", prim("double"))
        );
    }

    #[test]
    fn a_type_the_table_holds_otherwise_is_refused() {
        let table = table_schema(&record(), None, Mapping::None, None).unwrap().schema;
        let changed = st(&[("http", st(&[("status", DataType::Str)], false))], false);
        assert_eq!(
            table_schema(&changed, Some(&table), Mapping::None, None).unwrap_err(),
            "http.status is long in the table, and the record writes str there as string; rename the field in the \
             pipeline or the column in the table"
        );
        let deep = st(&[("tags", arr(st(&[("k", DataType::I64)], true)))], false);
        assert!(
            table_schema(&deep, Some(&table), Mapping::None, None)
                .unwrap_err()
                .starts_with("tags[].k is string in the table")
        );
        let flat = st(&[("http", DataType::Str)], false);
        assert!(
            table_schema(&flat, Some(&table), Mapping::None, None)
                .unwrap_err()
                .starts_with("http is struct in the table")
        );
        let mut decimal = table.clone();
        decimal.fields.push(plain("n", prim("decimal(10,2)")));
        assert!(
            table_schema(&st(&[("n", DataType::I64)], false), Some(&decimal), Mapping::None, None)
                .unwrap_err()
                .starts_with("n is decimal(10,2) in the table")
        );
    }

    /// A table that lets its columns widen has each one it holds narrower than the record types widened as the
    /// protocol allows, the change recorded after any earlier one; a time stays without a zone.
    #[test]
    fn a_table_that_widens_has_its_narrower_columns_widened_and_the_changes_recorded() {
        let mut table = StructType { fields: Vec::new() };
        for (name, p) in [("b", "byte"), ("f", "float"), ("d", "date"), ("ntz", "timestamp_ntz")] {
            table.fields.push(plain(name, prim(p)));
        }
        table.fields[0]
            .metadata
            .insert(TYPE_CHANGES.into(), json!([{"fromType": "short", "toType": "byte"}]));
        let element = Type::Array {
            element: Box::new(prim("integer")),
            contains_null: true,
        };
        table.fields.push(plain("xs", element));
        let record = st(
            &[
                ("b", DataType::I64),
                ("f", DataType::F64),
                ("d", DataType::DateTime),
                ("ntz", DataType::DateTime),
                ("xs", arr(DataType::I64)),
            ],
            false,
        );

        let kept = table_schema(&record, Some(&table), Mapping::None, None).unwrap();
        assert!(!kept.changed, "a table that does not widen keeps its own types");
        let widened = table_schema(&record, Some(&table), Mapping::None, Some(&[])).unwrap();
        assert!(widened.changed);
        let fields = &widened.schema.fields;
        let types: Vec<&Type> = fields.iter().map(|f| &f.data_type).collect();
        let longs = Type::Array {
            element: Box::new(prim("long")),
            contains_null: true,
        };
        assert_eq!(
            types,
            [
                &prim("long"),
                &prim("double"),
                &prim("timestamp_ntz"),
                &prim("timestamp_ntz"),
                &longs
            ]
        );
        let changes: Vec<Option<&Value>> = fields.iter().map(|f| f.metadata.get(TYPE_CHANGES)).collect();
        assert_eq!(
            changes,
            [
                Some(&json!([{"fromType": "short", "toType": "byte"}, {"fromType": "byte", "toType": "long"}])),
                Some(&json!([{"fromType": "float", "toType": "double"}])),
                Some(&json!([{"fromType": "date", "toType": "timestamp_ntz"}])),
                None,
                Some(&json!([{"fromType": "integer", "toType": "long", "fieldPath": "element"}])),
            ]
        );
        let again = table_schema(&record, Some(&widened.schema), Mapping::None, Some(&[])).unwrap();
        assert!(!again.changed, "a widened column widens no further");
        let partitioned = table_schema(&record, Some(&table), Mapping::None, Some(&["f".into()])).unwrap();
        assert_eq!(
            partitioned.schema.fields[1].data_type,
            prim("float"),
            "a partition column keeps its type"
        );
    }

    /// A column the table holds narrower than the record types it takes the record's values, each converted as it is
    /// written; one of another type is still refused.
    #[test]
    fn a_column_the_table_holds_narrower_is_written_as_its_type() {
        let mut table = table_schema(&record(), None, Mapping::None, None).unwrap().schema;
        for (name, p) in [
            ("b", "byte"),
            ("s", "short"),
            ("i", "integer"),
            ("f", "float"),
            ("d", "date"),
        ] {
            table.fields.push(plain(name, prim(p)));
        }
        table.fields.push(plain("ntz", prim("timestamp_ntz")));
        let narrow = st(
            &[
                ("b", DataType::I64),
                ("s", DataType::I64),
                ("i", DataType::I64),
                ("f", DataType::F64),
                ("d", DataType::DateTime),
                ("ntz", DataType::DateTime),
            ],
            false,
        );
        let evolved = table_schema(&narrow, Some(&table), Mapping::None, None).unwrap();
        assert!(!evolved.changed, "the table keeps its own types");
        let file = crate::io::parquet::Schema::new(&narrow).unwrap();
        let physical = physical(file.arrow(), &evolved.schema);
        let types: Vec<_> = physical.fields().iter().map(|f| f.data_type().clone()).collect();
        assert_eq!(
            types,
            vec![
                Arrow::Int8,
                Arrow::Int16,
                Arrow::Int32,
                Arrow::Float32,
                Arrow::Date32,
                Arrow::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
            ]
        );
        assert!(
            table_schema(
                &st(&[("i", DataType::Duration)], false),
                Some(&table),
                Mapping::None,
                None
            )
            .unwrap_err()
            .starts_with("i is integer in the table"),
            "a duration is no integer"
        );
    }

    #[test]
    fn a_column_that_is_not_nullable_is_refused_written_or_not() {
        let mut table = table_schema(&record(), None, Mapping::None, None).unwrap().schema;
        table.fields[0].nullable = false;
        assert_eq!(
            table_schema(&record(), Some(&table), Mapping::None, None).unwrap_err(),
            "the table's column ts is not nullable, and the record may leave it null"
        );
        let mut table = table_schema(&record(), None, Mapping::None, None).unwrap().schema;
        let Type::Struct(http) = &mut table.fields[1].data_type else {
            panic!("http is a struct")
        };
        http.fields[0].nullable = false;
        let without = st(&[("http", st(&[("hdrs", arr(DataType::Str))], false))], false);
        assert_eq!(
            table_schema(&without, Some(&table), Mapping::None, None).unwrap_err(),
            "the table's column http.status is not nullable, and the record may leave it null"
        );
    }

    #[test]
    fn a_name_differing_only_in_case_is_refused() {
        let table = table_schema(&record(), None, Mapping::None, None).unwrap().schema;
        let upper = st(&[("TS", DataType::DateTime)], false);
        assert!(
            table_schema(&upper, Some(&table), Mapping::None, None)
                .unwrap_err()
                .starts_with("ts and TS differ only in case")
        );
    }

    #[test]
    fn a_field_that_never_holds_a_value_is_no_column() {
        let empty = st(
            &[
                ("a", DataType::I64),
                ("n", DataType::Null),
                ("l", arr(DataType::Null)),
                ("o", st(&[("n", DataType::Null)], false)),
            ],
            false,
        );
        let schema = table_schema(
            &empty,
            None,
            Mapping::Name {
                max_column_id: 0,
                iceberg: false,
            },
            None,
        )
        .unwrap()
        .schema;
        let names: Vec<&str> = schema.fields.iter().map(|f| f.name.as_str()).collect();
        assert_eq!(names, ["a"]);
    }

    #[test]
    fn an_iceberg_table_gives_each_arrays_element_an_id_and_its_files_carry_it() {
        let iceberg = Mapping::Name {
            max_column_id: 0,
            iceberg: true,
        };
        let e = table_schema(&record(), None, iceberg, None).unwrap();
        assert_eq!(
            e.mapping,
            Mapping::Name {
                max_column_id: 11,
                iceberg: true
            }
        );
        let http = field(&e.schema, "http");
        assert!(
            !http.metadata.contains_key(NESTED_IDS),
            "a struct's own fields key their arrays"
        );
        let hdrs = field(inner(&http.data_type), "hdrs");
        let tags = field(&e.schema, "tags");
        assert_eq!(
            hdrs.metadata[NESTED_IDS],
            json!({format!("{}.element", hdrs.physical_name()): 8})
        );
        assert_eq!(
            tags.metadata[NESTED_IDS],
            json!({format!("{}.element", tags.physical_name()): 11})
        );
        assert_eq!(max_nested_id(&e.schema), 11);
        assert_eq!(not_iceberg(&e.schema), None);

        let arrow = physical(crate::io::parquet::Schema::new(&record()).unwrap().arrow(), &e.schema);
        let element_id = |f: &Field| match f.data_type() {
            Arrow::List(element) => element.metadata().get(PARQUET_FIELD_ID_META_KEY).cloned(),
            other => panic!("{other} is no list"),
        };
        let Arrow::Struct(http) = arrow.field_with_name(http.physical_name()).unwrap().data_type() else {
            panic!("http is a struct")
        };
        assert_eq!(element_id(&http[1]), Some("8".to_string()));
        assert_eq!(
            element_id(arrow.field_with_name(tags.physical_name()).unwrap()),
            Some("11".to_string())
        );
        // And the Parquet file carries them, its list's element node with each.
        let mut writer = parquet::arrow::ArrowWriter::try_new(Vec::new(), arrow.clone(), None).unwrap();
        writer.write(&arrow_array::RecordBatch::new_empty(arrow)).unwrap();
        let file = bytes::Bytes::from(writer.into_inner().unwrap());
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        fn node_ids(t: &parquet::schema::types::Type, out: &mut Vec<i32>) {
            if t.get_basic_info().has_id() {
                out.push(t.get_basic_info().id());
            }
            if t.is_group() {
                t.get_fields().iter().for_each(|f| node_ids(f, out));
            }
        }
        let mut got = Vec::new();
        node_ids(reader.parquet_schema().root_schema(), &mut got);
        got.sort();
        assert_eq!(got, (1..=11).collect::<Vec<_>>());

        let mut table = e.schema.clone();
        let tags = table.fields.iter_mut().find(|f| f.name == "tags").unwrap();
        tags.metadata.remove(NESTED_IDS);
        let why = not_iceberg(&table).unwrap();
        assert!(
            why.contains("its column tags gives") && why.contains(".element no id"),
            "{why}"
        );
        let grown = st(&[("tags", arr(arr(DataType::I64)))], false);
        let e = table_schema(&grown, None, iceberg, None).unwrap();
        let tags = field(&e.schema, "tags");
        let key = |k: &str| format!("{}{k}", tags.physical_name());
        assert_eq!(
            tags.metadata[NESTED_IDS],
            json!({key(".element"): 2, key(".element.element"): 3})
        );
    }

    #[test]
    fn an_iceberg_table_changes_types_only_as_iceberg_does() {
        let table = StructType {
            fields: vec![plain("i", prim("integer")), plain("d", prim("date"))],
        };
        let iceberg = Mapping::Name {
            max_column_id: 0,
            iceberg: true,
        };
        let int = st(&[("i", DataType::I64)], false);
        let widened = table_schema(&int, Some(&table), iceberg, Some(&[])).unwrap();
        assert_eq!(field(&widened.schema, "i").data_type, prim("long"));
        assert_eq!(not_iceberg(&widened.schema), None);
        let date = st(&[("d", DataType::DateTime)], false);
        let mapped = Mapping::Name {
            max_column_id: 0,
            iceberg: false,
        };
        let ntz = table_schema(&date, Some(&table), mapped, Some(&[])).unwrap();
        assert_eq!(field(&ntz.schema, "d").data_type, prim("timestamp_ntz"));
        let why = not_iceberg(&ntz.schema).unwrap();
        assert!(why.contains("its column d changed type"), "{why}");
        let kept = table_schema(&date, Some(&table), iceberg, Some(&[])).unwrap();
        assert!(
            !kept.changed,
            "an Iceberg table keeps its date column, written as a date"
        );

        for (from, to, widens) in [
            ("byte", "integer", true),
            ("short", "long", true),
            ("float", "double", true),
            ("decimal(10,2)", "decimal(12,2)", true),
            ("decimal(10,2)", "decimal(12,3)", false),
            ("integer", "double", false),
            ("long", "integer", false),
            ("date", "timestamp_ntz", false),
        ] {
            assert_eq!(iceberg_widens(from, to), widens, "{from} to {to}");
        }
    }

    #[test]
    fn the_arrow_schema_names_each_field_physically_with_its_id() {
        let record = record();
        let parquet = crate::io::parquet::Schema::new(&record).unwrap();
        let table = table_schema(
            &record,
            None,
            Mapping::Name {
                max_column_id: 0,
                iceberg: false,
            },
            None,
        )
        .unwrap()
        .schema;
        let arrow = physical(parquet.arrow(), &table);
        let id = |f: &Field| {
            f.metadata()
                .get(PARQUET_FIELD_ID_META_KEY)
                .map(|v| v.parse::<i64>().unwrap())
        };

        let http_table = field(&table, "http");
        let http = arrow.field_with_name(http_table.physical_name()).unwrap();
        assert_eq!(id(http), Some(2));
        let Arrow::Struct(children) = http.data_type() else {
            panic!("http is a struct")
        };
        let http_fields = &inner(&http_table.data_type).fields;
        assert_eq!(children[0].name(), http_fields[0].physical_name());
        assert_eq!((id(&children[0]), id(&children[1])), (Some(6), Some(7)));
        let Arrow::List(hdrs) = children[1].data_type() else {
            panic!("hdrs is a list")
        };
        assert_eq!((hdrs.name().as_str(), id(hdrs)), ("item", None));

        let tags_table = field(&table, "tags");
        let tags = arrow.field_with_name(tags_table.physical_name()).unwrap();
        let Arrow::List(element) = tags.data_type() else {
            panic!("tags is a list")
        };
        let Arrow::Struct(tag) = element.data_type() else {
            panic!("a tag is a struct")
        };
        let tag_fields = &inner(&tags_table.data_type).fields;
        assert_eq!(
            (tag[0].name(), tag[1].name()),
            (
                &tag_fields[0].physical_name().to_string(),
                &tag_fields[1].physical_name().to_string()
            )
        );
        assert_eq!((id(tags), id(&tag[0]), id(&tag[1])), (Some(3), Some(8), Some(9)));

        let unmapped = physical(parquet.arrow(), &StructType { fields: vec![] });
        assert_eq!(unmapped.as_ref(), parquet.arrow().as_ref());
        let plain_table = table_schema(&record, None, Mapping::None, None).unwrap().schema;
        let same_names = physical(parquet.arrow(), &plain_table);
        assert_eq!(same_names.field(0).name(), "ts");
        assert_eq!(id(same_names.field(0)), None);
    }

    #[test]
    fn a_partition_column_is_a_top_level_primitive() {
        let mut schema = table_schema(
            &record(),
            None,
            Mapping::Name {
                max_column_id: 0,
                iceberg: false,
            },
            None,
        )
        .unwrap()
        .schema;
        schema.fields.push(plain("day", prim("date")));
        schema.fields.push(plain("d", prim("decimal(10,2)")));
        schema.fields.push(plain("i", prim("short")));
        let ts = field(&schema, "ts").physical_name().to_string();
        assert_eq!(partition_column(&schema, "ts").unwrap(), (ts, Kind::Timestamptz));
        assert_eq!(
            partition_column(&schema, "day").unwrap(),
            ("day".to_string(), Kind::Date)
        );
        assert_eq!(partition_column(&schema, "i").unwrap().1, Kind::Short);
        assert_eq!(
            partition_column(&schema, "http").unwrap_err(),
            "the table is partitioned by http, a struct column, and this writer writes no partition value of that type"
        );
        assert!(
            partition_column(&schema, "d")
                .unwrap_err()
                .contains("a decimal(10,2) column")
        );
        assert_eq!(
            partition_column(&schema, "nope").unwrap_err(),
            "the table is partitioned by nope, which is no column of its schema"
        );
    }

    #[test]
    fn a_column_this_writer_cannot_honour_is_named() {
        let mut schema = table_schema(&record(), None, Mapping::None, None).unwrap().schema;
        assert_eq!(refused_column(&schema), None);
        let Type::Array { element, .. } = &mut schema.fields[2].data_type else {
            panic!("tags is an array")
        };
        let Type::Struct(tag) = element.as_mut() else {
            panic!("a tag is a struct")
        };
        tag.fields[0].metadata.insert("delta.identity.start".into(), json!(1));
        assert_eq!(
            refused_column(&schema).unwrap(),
            "the table's column tags[].k carries delta.identity.start, which this writer cannot honour"
        );
        schema.fields[0]
            .metadata
            .insert("delta.generationExpression".into(), json!("now()"));
        assert_eq!(
            refused_column(&schema).unwrap(),
            "the table's column ts carries delta.generationExpression, which this writer cannot honour"
        );
        for key in ["CURRENT_DEFAULT", "delta.invariants"] {
            let mut s = StructType {
                fields: vec![plain("a", prim("long"))],
            };
            s.fields[0].metadata.insert(key.into(), json!("x"));
            assert!(refused_column(&s).unwrap().contains(key));
        }
    }

    #[test]
    fn a_void_no_data_file_can_hold_is_refused_and_one_it_leaves_out_is_not() {
        let refused = |fields: Vec<StructField>| refused_column(&StructType { fields });
        let array = |t| Type::Array {
            element: Box::new(t),
            contains_null: true,
        };
        let only_void = || {
            Type::Struct(StructType {
                fields: vec![plain("v", prim("void"))],
            })
        };
        assert_eq!(refused(vec![plain("a", prim("long")), plain("v", prim("void"))]), None);
        assert!(
            refused(vec![plain("v", prim("void"))])
                .unwrap()
                .contains("every column")
        );
        assert!(
            refused(vec![plain("a", prim("long")), plain("s", only_void())])
                .unwrap()
                .contains("column s is a struct of only void")
        );
        assert!(
            refused(vec![plain("a", array(prim("void")))])
                .unwrap()
                .contains("column a holds a void")
        );
        assert!(
            refused(vec![plain(
                "a",
                array(Type::Struct(StructType {
                    fields: vec![plain("n", prim("long")), plain("v", prim("void"))],
                }))
            )])
            .unwrap()
            .contains("column a holds a void")
        );
        let map = Type::Map {
            key: Box::new(prim("string")),
            value: Box::new(prim("void")),
            value_contains_null: true,
        };
        assert!(
            refused(vec![plain("m", map)])
                .unwrap()
                .contains("column m holds a void")
        );
    }
}
