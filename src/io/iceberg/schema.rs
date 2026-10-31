//! The table's schema from the route's record.
//!
//! The record is written as `io::parquet` writes it, and the table's schema mirrors that file field for field: an
//! open struct's `rest` is a string column, a variant a string, a time `timestamptz`. Every column is optional, since
//! every Parquet column the writer writes is.
//!
//! A missing table takes the record as its schema, ids assigned as Iceberg assigns fresh ones. A table that exists
//! keeps every column and its id; a field it lacks, at any depth, is added as an optional column with a new id; a
//! field it holds narrower (an `int`, a `float`, a `date` or a `timestamp`, as `parquet::narrows` says) is written as
//! that type; a field it holds as another type is refused, and the user renames the field in the pipeline or the
//! column in the table.

use std::sync::Arc;

use arrow_schema::extension::Uuid;
use arrow_schema::{DataType as Arrow, Field, Fields, Schema as ArrowSchema};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

use super::metadata::{ListType, NestedField, Primitive, Schema, StructTag, StructType, Type};
use crate::core::column::DataType;
use crate::io::parquet::{at, columns, narrows};
use crate::io::table::Kind;

/// The table's schema for a record, and whether it differs from the table's current one.
#[derive(Clone, Debug, PartialEq)]
pub struct Evolution {
    pub schema: Schema,
    /// The highest column id the table has assigned, the schema's new columns' included.
    pub last_column_id: i32,
    /// Whether `schema` is new to the table: the table is created, or a column added. A commit then sets it.
    pub changed: bool,
}

/// The schema a table must have for `record` to be written to it. With no `current` schema the table is new and ids
/// start at 1; otherwise new columns take ids above `last_column_id`, the table's own.
///
/// Err names the field when the record is no struct, a column holds a type other than the record's, or a required
/// column is one the record does not write.
pub fn table_schema(record: &DataType, current: Option<&Schema>, last_column_id: i32) -> Result<Evolution, String> {
    let DataType::Struct(fields, open) = record else {
        return Err(format!(
            "the record is of type {record}, and a table's schema is a struct"
        ));
    };
    let columns = columns(fields, *open);
    match current {
        None => {
            let mut ids = Ids { last: 0 };
            let fields = ids.fresh_fields(&columns);
            Ok(Evolution {
                schema: Schema {
                    kind: StructTag,
                    schema_id: 0,
                    identifier_field_ids: Vec::new(),
                    fields,
                },
                last_column_id: ids.last,
                changed: true,
            })
        }
        Some(current) => {
            let mut ids = Ids { last: last_column_id };
            let fields = ids.evolve_fields(&current.fields, &columns, "")?;
            let changed = ids.last != last_column_id;
            Ok(Evolution {
                schema: Schema {
                    kind: StructTag,
                    schema_id: current.schema_id + i32::from(changed),
                    identifier_field_ids: current.identifier_field_ids.clone(),
                    fields,
                },
                last_column_id: ids.last,
                changed,
            })
        }
    }
}

/// `arrow` with every field, at any depth and a list's element too, carrying its id in `schema` under
/// `PARQUET:field_id`, so a Parquet file written with it carries the table's field ids, and each struct ending with
/// the columns of `schema` it lacks, which its rows write null: a data file holds every column. Fields are matched by
/// name; one `schema` does not name keeps no id.
///
/// Err names a column the record lacks of a type this writer has no Parquet type for.
pub fn with_field_ids(arrow: &ArrowSchema, schema: &Schema) -> Result<Arc<ArrowSchema>, String> {
    let fields = labelled(arrow.fields(), &schema.fields, "")?;
    Ok(Arc::new(ArrowSchema::new_with_metadata(
        fields,
        arrow.metadata().clone(),
    )))
}

/// `fields` as `table` names them, then `table`'s fields they lack.
fn labelled(fields: &Fields, table: &[NestedField], path: &str) -> Result<Fields, String> {
    let mut labelled = (fields.iter())
        .map(|field| match table.iter().find(|t| t.name == *field.name()) {
            Some(t) => with_id(field, t.id, &t.field_type, &at(path, &t.name)),
            None => Ok(field.as_ref().clone()),
        })
        .collect::<Result<Vec<_>, String>>()?;
    for t in table.iter().filter(|t| !fields.iter().any(|f| f.name() == &t.name)) {
        let path = at(path, &t.name);
        labelled.push(column(&t.name, &t.field_type, true, t.id, &path)?);
    }
    Ok(labelled.into())
}

fn with_id(field: &Field, id: i32, ty: &Type, path: &str) -> Result<Field, String> {
    let data_type = match (field.data_type(), ty) {
        (Arrow::Struct(fields), Type::Struct(s)) => Arrow::Struct(labelled(fields, &s.fields, path)?),
        (Arrow::List(element), Type::List(l)) => Arrow::List(Arc::new(with_id(
            element,
            l.element_id,
            &l.element,
            &format!("{path}[]"),
        )?)),
        (other, Type::Primitive(p)) => narrow(p).unwrap_or_else(|| other.clone()),
        (other, _) => other.clone(),
    };
    Ok(with_metadata(field.clone().with_data_type(data_type), id))
}

fn with_metadata(field: Field, id: i32) -> Field {
    let mut metadata = field.metadata().clone();
    metadata.insert(PARQUET_FIELD_ID_META_KEY.to_string(), id.to_string());
    field.with_metadata(metadata)
}

/// The field `name` of `ty`, with id `id`, as a column the record does not write is written: the spec's Parquet type
/// for it, every field in it optional but a map's key.
fn column(name: &str, ty: &Type, nullable: bool, id: i32, path: &str) -> Result<Field, String> {
    let data_type = match ty {
        Type::Primitive(p) => primitive_arrow(p)
            .ok_or_else(|| format!("the table's column {path} is a {p}, a type this writer cannot write"))?,
        Type::Struct(s) => Arrow::Struct(labelled(&Fields::empty(), &s.fields, path)?),
        Type::List(l) => Arrow::List(Arc::new(column(
            "element",
            &l.element,
            true,
            l.element_id,
            &format!("{path}[]"),
        )?)),
        Type::Map(map) => {
            let refused = || format!("the table's column {path} is a map this writer cannot read");
            let part = |part: &str, nullable: bool| {
                let ty: Type = serde_json::from_value(map[part].clone()).map_err(|_| refused())?;
                let id = map[format!("{part}-id")].as_i64().ok_or_else(refused)?;
                column(part, &ty, nullable, id as i32, &format!("{path}.{part}"))
            };
            let entries = Fields::from(vec![part("key", false)?, part("value", true)?]);
            Arrow::Map(Arc::new(Field::new("key_value", Arrow::Struct(entries), false)), false)
        }
    };
    let field = Field::new(name, data_type, nullable);
    let field = match ty {
        // Parquet's UUID type, where a bare fixed[16] would be Iceberg's fixed.
        Type::Primitive(Primitive::Other(t)) if t == "uuid" => field.with_extension_type(Uuid),
        _ => field,
    };
    Ok(with_metadata(field, id))
}

/// The Arrow type of a column of `p`, as the spec has Parquet hold it.
fn primitive_arrow(p: &Primitive) -> Option<Arrow> {
    use arrow_schema::TimeUnit::Microsecond;
    Some(match p {
        Primitive::Boolean => Arrow::Boolean,
        Primitive::Int => Arrow::Int32,
        Primitive::Long => Arrow::Int64,
        Primitive::Float => Arrow::Float32,
        Primitive::Double => Arrow::Float64,
        Primitive::Date => Arrow::Date32,
        Primitive::Timestamp => Arrow::Timestamp(Microsecond, None),
        Primitive::Timestamptz => Arrow::Timestamp(Microsecond, Some("UTC".into())),
        Primitive::String => Arrow::Utf8,
        Primitive::Binary => Arrow::Binary,
        Primitive::Other(name) => match name.as_str() {
            "time" => Arrow::Time64(Microsecond),
            "uuid" => Arrow::FixedSizeBinary(16),
            name => {
                if let Some(n) = name.strip_prefix("fixed[").and_then(|n| n.strip_suffix(']')) {
                    Arrow::FixedSizeBinary(n.parse().ok()?)
                } else {
                    let (p, s) = name.strip_prefix("decimal(")?.strip_suffix(')')?.split_once(',')?;
                    Arrow::Decimal128(p.trim().parse().ok()?, s.trim().parse().ok()?)
                }
            }
        },
    })
}

/// The Parquet type a column of `p` is written as, where it is narrower than any record type.
fn narrow(p: &Primitive) -> Option<Arrow> {
    p.kind().as_ref().and_then(Kind::narrowed)
}

/// The Iceberg type a record's primitive is written as.
fn primitive(t: &DataType) -> Primitive {
    match t {
        DataType::Bool => Primitive::Boolean,
        DataType::I64 | DataType::Duration => Primitive::Long,
        DataType::F64 => Primitive::Double,
        DataType::DateTime => Primitive::Timestamptz,
        DataType::Str | DataType::Variant => Primitive::String,
        DataType::Bytes => Primitive::Binary,
        DataType::Null => unreachable!("`columns` leaves out a column that holds no value"),
        DataType::Array(_) | DataType::Struct(..) => unreachable!("a nested type is no primitive"),
    }
}

/// Assigns column ids, each one above the last.
struct Ids {
    last: i32,
}

impl Ids {
    fn next(&mut self) -> i32 {
        self.last += 1;
        self.last
    }

    /// New fields for `columns`. A struct's fields take their ids before any field's children do, as Iceberg's
    /// `TypeUtil.assignFreshIds` assigns them.
    fn fresh_fields(&mut self, columns: &[(String, DataType)]) -> Vec<NestedField> {
        let ids: Vec<i32> = columns.iter().map(|_| self.next()).collect();
        columns
            .iter()
            .zip(ids)
            .map(|((name, t), id)| NestedField {
                id,
                name: name.clone(),
                required: false,
                field_type: self.fresh_type(t),
                doc: None,
                other: Default::default(),
            })
            .collect()
    }

    fn fresh_type(&mut self, t: &DataType) -> Type {
        match t {
            DataType::Array(element) => {
                let element_id = self.next();
                Type::List(ListType {
                    element_id,
                    element_required: false,
                    element: Box::new(self.fresh_type(element)),
                })
            }
            DataType::Struct(fields, open) => Type::Struct(StructType {
                fields: self.fresh_fields(&columns(fields, *open)),
            }),
            t => Type::Primitive(primitive(t)),
        }
    }

    /// The table's fields `table` with the record's `columns` met: each table field kept, its id too, and each
    /// column the table lacks added after them. A column is added as Iceberg's `UpdateSchema.addColumn` adds one: its
    /// id, then its children's.
    fn evolve_fields(
        &mut self,
        table: &[NestedField],
        columns: &[(String, DataType)],
        path: &str,
    ) -> Result<Vec<NestedField>, String> {
        let mut fields = Vec::with_capacity(table.len().max(columns.len()));
        for field in table {
            let path = at(path, &field.name);
            match columns.iter().find(|(name, _)| *name == field.name) {
                // Every column is written optional: a row may lack any field.
                Some(_) if field.required => {
                    return Err(format!(
                        "the table's column {path} is required, and the record may leave it null"
                    ));
                }
                Some((_, t)) => fields.push(NestedField {
                    field_type: self.evolve_type(&field.field_type, t, &path)?,
                    ..field.clone()
                }),
                None if field.required => {
                    return Err(format!(
                        "the table's column {path} is required, and the record does not write it"
                    ));
                }
                // A row that does not name the column takes its write default, which this writer does not write.
                None if field.other.get("write-default").is_some_and(|d| !d.is_null()) => {
                    return Err(format!(
                        "the table's column {path} has a write default, and the record does not write it"
                    ));
                }
                // Reads as null in the rows written.
                None => fields.push(field.clone()),
            }
        }
        for (name, t) in columns {
            if !table.iter().any(|f| f.name == *name) {
                let id = self.next();
                fields.push(NestedField {
                    id,
                    name: name.clone(),
                    required: false,
                    field_type: self.fresh_type(t),
                    doc: None,
                    other: Default::default(),
                });
            }
        }
        Ok(fields)
    }

    fn evolve_type(&mut self, table: &Type, record: &DataType, path: &str) -> Result<Type, String> {
        match (table, record) {
            (Type::Struct(s), DataType::Struct(fields, open)) => Ok(Type::Struct(StructType {
                fields: self.evolve_fields(&s.fields, &columns(fields, *open), path)?,
            })),
            (Type::List(l), DataType::Array(_)) if l.element_required => Err(format!(
                "the table's list {path} requires its elements, and the record may leave one null"
            )),
            (Type::List(l), DataType::Array(element)) => Ok(Type::List(ListType {
                element: Box::new(self.evolve_type(&l.element, element, &format!("{path}[]"))?),
                ..l.clone()
            })),
            (Type::Primitive(p), t)
                if !matches!(t, DataType::Array(_) | DataType::Struct(..))
                    && (*p == primitive(t) || narrow(p).is_some_and(|to| narrows(t, &to))) =>
            {
                Ok(table.clone())
            }
            _ => Err(format!(
                "{path} is {table} in the table, and the record writes {} there as {}; rename the field in the \
                 pipeline or the column in the table",
                record.spell(),
                written(record),
            )),
        }
    }
}

/// The Iceberg type's name the record's type is written as.
fn written(t: &DataType) -> String {
    match t {
        DataType::Array(_) => "list".to_string(),
        DataType::Struct(..) => "struct".to_string(),
        t => primitive(t).to_string(),
    }
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

    fn field(id: i32, name: &str, field_type: Type) -> NestedField {
        NestedField {
            id,
            name: name.to_string(),
            required: false,
            field_type,
            doc: None,
            other: Default::default(),
        }
    }

    fn prim(p: Primitive) -> Type {
        Type::Primitive(p)
    }

    fn list(element_id: i32, element: Type) -> Type {
        Type::List(ListType {
            element_id,
            element_required: false,
            element: Box::new(element),
        })
    }

    fn strukt(fields: Vec<NestedField>) -> Type {
        Type::Struct(StructType { fields })
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

    #[test]
    fn a_new_table_takes_the_record_with_ids_a_level_at_a_time() {
        let e = table_schema(&record(), None, 0).unwrap();
        assert!(e.changed);
        assert_eq!(e.schema.schema_id, 0);
        assert_eq!(e.last_column_id, 11);
        assert_eq!(
            e.schema.fields,
            vec![
                field(1, "ts", prim(Primitive::Timestamptz)),
                field(
                    2,
                    "http",
                    strukt(vec![
                        field(6, "status", prim(Primitive::Long)),
                        field(7, "hdrs", list(8, prim(Primitive::String))),
                    ])
                ),
                field(
                    3,
                    "tags",
                    list(
                        9,
                        strukt(vec![
                            field(10, "k", prim(Primitive::String)),
                            field(11, REST, prim(Primitive::String)),
                        ])
                    )
                ),
                field(4, "raw", prim(Primitive::Binary)),
                field(5, "v", prim(Primitive::String)),
            ]
        );
    }

    #[test]
    fn the_same_record_changes_nothing() {
        let first = table_schema(&record(), None, 0).unwrap();
        let again = table_schema(&record(), Some(&first.schema), first.last_column_id).unwrap();
        assert!(!again.changed);
        assert_eq!(again.last_column_id, first.last_column_id);
        assert_eq!(again.schema, first.schema);
    }

    #[test]
    fn a_new_nested_field_is_added_with_a_new_id_and_a_column_not_written_is_kept() {
        let mut table = table_schema(&record(), None, 0).unwrap().schema;
        table.schema_id = 3;
        table.fields.push(field(20, "gone", prim(Primitive::Int)));
        let grown = st(
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
        );
        let e = table_schema(&grown, Some(&table), 20).unwrap();
        assert!(e.changed);
        assert_eq!(e.schema.schema_id, 4);
        assert_eq!(e.last_column_id, 23);
        let Type::Struct(http) = &e.schema.fields[1].field_type else {
            panic!("http is a struct")
        };
        assert_eq!(
            http.fields[2],
            field(21, "peer", strukt(vec![field(22, "ip", prim(Primitive::String))]))
        );
        let Type::List(tags) = &e.schema.fields[2].field_type else {
            panic!("tags is a list")
        };
        assert_eq!(
            *tags.element,
            strukt(vec![
                field(10, "k", prim(Primitive::String)),
                field(11, REST, prim(Primitive::String)),
                field(23, "n", prim(Primitive::Double)),
            ])
        );
        assert_eq!(e.schema.fields[5], field(20, "gone", prim(Primitive::Int)));
    }

    /// A column the table holds narrower than the record types it takes the record's values, each converted as it is
    /// written; a duration is still no `int`.
    #[test]
    fn a_column_the_table_holds_narrower_is_written_as_its_type() {
        let table = Schema {
            kind: StructTag,
            schema_id: 0,
            identifier_field_ids: Vec::new(),
            fields: vec![
                field(1, "i", prim(Primitive::Int)),
                field(2, "f", prim(Primitive::Float)),
                field(3, "d", prim(Primitive::Date)),
                field(4, "ts", prim(Primitive::Timestamp)),
            ],
        };
        let narrow = st(
            &[
                ("i", DataType::I64),
                ("f", DataType::F64),
                ("d", DataType::DateTime),
                ("ts", DataType::DateTime),
            ],
            false,
        );
        let evolved = table_schema(&narrow, Some(&table), 4).unwrap();
        assert_eq!(
            (evolved.schema.fields.clone(), evolved.changed),
            (table.fields.clone(), false)
        );
        let file = crate::io::parquet::Schema::new(&narrow).unwrap();
        let types: Vec<_> = (with_field_ids(file.arrow(), &evolved.schema).unwrap().fields().iter())
            .map(|f| f.data_type().clone())
            .collect();
        assert_eq!(
            types,
            vec![
                Arrow::Int32,
                Arrow::Float32,
                Arrow::Date32,
                Arrow::Timestamp(arrow_schema::TimeUnit::Microsecond, None)
            ]
        );
        assert!(
            table_schema(&st(&[("i", DataType::Duration)], false), Some(&table), 4)
                .unwrap_err()
                .starts_with("i is int in the table")
        );
    }

    #[test]
    fn a_type_the_table_holds_otherwise_is_refused() {
        let table = table_schema(&record(), None, 0).unwrap().schema;
        let changed = st(
            &[
                ("http", st(&[("status", DataType::Str)], false)),
                ("ts", DataType::DateTime),
            ],
            false,
        );
        assert_eq!(
            table_schema(&changed, Some(&table), 11).unwrap_err(),
            "http.status is long in the table, and the record writes str there as string; rename the field in the \
             pipeline or the column in the table"
        );
        let mut other = table.clone();
        other
            .fields
            .push(field(12, "n", prim(Primitive::Other("decimal(10, 2)".into()))));
        let wide = st(&[("n", DataType::I64)], false);
        assert!(
            table_schema(&wide, Some(&other), 12)
                .unwrap_err()
                .starts_with("n is decimal(10, 2) in the table")
        );
        let flat = st(&[("http", DataType::Str)], false);
        assert!(
            table_schema(&flat, Some(&table), 11)
                .unwrap_err()
                .starts_with("http is struct in the table")
        );
    }

    #[test]
    fn a_column_the_record_lacks_of_a_type_no_file_here_holds_is_refused() {
        let mut table = table_schema(&record(), None, 0).unwrap().schema;
        let Type::Struct(http) = &mut table.fields[1].field_type else {
            panic!("http is a struct")
        };
        http.fields
            .push(field(12, "v", prim(Primitive::Other("timestamp_ns".into()))));
        let parquet = crate::io::parquet::Schema::new(&record()).unwrap();
        assert_eq!(
            with_field_ids(parquet.arrow(), &table).unwrap_err(),
            "the table's column http.v is a timestamp_ns, a type this writer cannot write"
        );
    }

    #[test]
    fn a_column_with_a_write_default_the_record_does_not_write_is_refused_and_one_it_writes_keeps_it() {
        let mut table = table_schema(&record(), None, 0).unwrap().schema;
        let Type::Struct(http) = &mut table.fields[1].field_type else {
            panic!("http is a struct")
        };
        http.fields[0]
            .other
            .insert("write-default".into(), serde_json::json!(200));
        http.fields[0]
            .other
            .insert("initial-default".into(), serde_json::json!(0));
        let evolved = table_schema(&record(), Some(&table), 11).unwrap().schema;
        let Type::Struct(http) = &evolved.fields[1].field_type else {
            panic!("http is a struct")
        };
        assert_eq!(
            http.fields[0].other["write-default"], 200,
            "a schema evolved keeps a field's defaults"
        );
        assert_eq!(http.fields[0].other["initial-default"], 0);

        let without = st(&[("http", st(&[("hdrs", arr(DataType::Str))], false))], false);
        assert_eq!(
            table_schema(&without, Some(&table), 11).unwrap_err(),
            "the table's column http.status has a write default, and the record does not write it"
        );
    }

    #[test]
    fn a_required_column_the_record_does_not_write_is_refused() {
        let mut table = table_schema(&record(), None, 0).unwrap().schema;
        let Type::Struct(http) = &mut table.fields[1].field_type else {
            panic!("http is a struct")
        };
        http.fields[0].required = true;
        let without = st(&[("http", st(&[("hdrs", arr(DataType::Str))], false))], false);
        assert_eq!(
            table_schema(&without, Some(&table), 11).unwrap_err(),
            "the table's column http.status is required, and the record does not write it"
        );
    }

    #[test]
    fn a_required_column_the_record_writes_is_refused() {
        let mut table = table_schema(&record(), None, 0).unwrap().schema;
        table.fields[0].required = true;
        assert_eq!(
            table_schema(&record(), Some(&table), 11).unwrap_err(),
            "the table's column ts is required, and the record may leave it null"
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
        let schema = table_schema(&empty, None, 0).unwrap().schema;
        let names: Vec<&str> = schema.fields.iter().map(|f| f.name.as_str()).collect();
        assert_eq!(names, ["a"]);
    }

    #[test]
    fn a_map_column_the_record_does_not_write_is_kept() {
        let mut table = table_schema(&st(&[("a", DataType::I64)], false), None, 0)
            .unwrap()
            .schema;
        let map = Type::Map(
            serde_json::json!({"type": "map", "key-id": 3, "key": "string", "value-id": 4,
            "value": "string", "value-required": false}),
        );
        table.fields.push(field(2, "m", map.clone()));
        let e = table_schema(&st(&[("a", DataType::I64)], false), Some(&table), 4).unwrap();
        assert!(!e.changed);
        assert_eq!(e.schema.fields[1].field_type, map);
    }

    #[test]
    fn the_arrow_schema_carries_each_fields_id() {
        let record = record();
        let parquet = crate::io::parquet::Schema::new(&record).unwrap();
        let table = table_schema(&record, None, 0).unwrap().schema;
        let labelled = with_field_ids(parquet.arrow(), &table).unwrap();
        assert!(labelled.contains(parquet.arrow()));
        let id = |f: &Field| {
            f.metadata()
                .get(PARQUET_FIELD_ID_META_KEY)
                .map(|v| v.parse::<i32>().unwrap())
        };

        let http = labelled.field_with_name("http").unwrap();
        assert_eq!(id(http), Some(2));
        let Arrow::Struct(children) = http.data_type() else {
            panic!("http is a struct")
        };
        assert_eq!(id(&children[0]), Some(6));
        let Arrow::List(hdrs) = children[1].data_type() else {
            panic!("hdrs is a list")
        };
        assert_eq!((id(&children[1]), id(hdrs)), (Some(7), Some(8)));

        let tags = labelled.field_with_name("tags").unwrap();
        let Arrow::List(element) = tags.data_type() else {
            panic!("tags is a list")
        };
        let Arrow::Struct(tag) = element.data_type() else {
            panic!("a tag is a struct")
        };
        assert_eq!(
            (id(tags), id(element), id(&tag[0]), id(&tag[1])),
            (Some(3), Some(9), Some(10), Some(11))
        );
        assert_eq!(id(labelled.field_with_name("v").unwrap()), Some(5));
    }
}
