//! A schema is a contract on a pipeline's rows: a table of fields, each a path from the record's root, the type it
//! must hold, whether a row must hold it, and what it means. A bundle declares its schemas as such tables
//! (`SchemaDto`), and `json_schema` reads one from JSON Schema and writes one as it. A pipeline names the schemas it
//! produces, and `check` holds the type of its record (`plan::dictionary`) to them.
//!
//! A field's path spells its keys with dots, as a parser declares a nested field and as `io::avro::fields_of` names
//! one, so a key holding a dot is no schema's to name. A type is spelled as a declaration spells one, so a schema, a
//! parser and a declaration read the same.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use crate::config_file::{SchemaDto, SchemaFieldDto};
use crate::core::column::DataType;

pub mod json_schema;

/// One contract, by its id in the bundle.
#[derive(Debug)]
pub struct Schema {
    pub id: String,
    pub title: String,
    pub description: String,
    /// In the table's order.
    pub fields: Vec<Field>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Field {
    /// Its keys from the record's root, dotted.
    pub path: String,
    /// What the field must hold. An object type holds any object: its fields are rows of their own, under its path.
    pub ty: DataType,
    pub required: bool,
    pub description: String,
}

impl Field {
    /// The field's keys from the record's root.
    pub fn keys(&self) -> Vec<String> {
        self.path.split('.').map(str::to_string).collect()
    }
}

/// The schema `dto` declares. A path listed twice, a type that does not read, and a field under one that holds no
/// object are refused.
pub fn read(dto: &SchemaDto) -> Result<Schema, String> {
    let mut fields: Vec<Field> = Vec::with_capacity(dto.fields.len());
    // By path, so a schema of thousands of rows reads in one pass rather than a search of every row for each.
    let mut by_path: std::collections::HashMap<&str, usize> =
        std::collections::HashMap::with_capacity(dto.fields.len());
    for row in &dto.fields {
        if row.field.is_empty() || row.field.split('.').any(str::is_empty) {
            return Err(format!("`{}` is no field path: keys, dotted", row.field));
        }
        if by_path.insert(&row.field, fields.len()).is_some() {
            return Err(format!("`{}` is listed twice", row.field));
        }
        let ty = crate::expr::parse::parse_type(&row.ty)
            .map_err(|e| format!("`{}`: `{}` is no type: {}", row.field, row.ty, e.message))?;
        fields.push(Field {
            path: row.field.clone(),
            ty,
            required: row.required,
            description: row.description.clone(),
        });
    }
    for f in &fields {
        let mut keys = f.keys();
        while keys.pop().is_some() && !keys.is_empty() {
            let above = keys.join(".");
            if let Some(parent) = by_path.get(above.as_str()).map(|&i| &fields[i])
                && !holds_an_object(&parent.ty)
            {
                return Err(format!(
                    "`{}` is under `{above}`, which is {}",
                    f.path,
                    parent.ty.spell()
                ));
            }
        }
    }
    Ok(Schema {
        id: dto.id.clone(),
        title: if dto.title.is_empty() {
            dto.id.clone()
        } else {
            dto.title.clone()
        },
        description: dto.description.clone(),
        fields,
    })
}

/// `schema` as a bundle declares it.
pub fn to_dto(schema: &Schema) -> SchemaDto {
    SchemaDto {
        id: schema.id.clone(),
        title: schema.title.clone(),
        description: schema.description.clone(),
        fields: schema
            .fields
            .iter()
            .map(|f| SchemaFieldDto {
                field: f.path.clone(),
                ty: f.ty.spell(),
                required: f.required,
                description: f.description.clone(),
            })
            .collect(),
    }
}

/// Whether a value of `ty` is an object, or an array of them, so fields may sit under it.
fn holds_an_object(ty: &DataType) -> bool {
    match ty {
        DataType::Struct(..) => true,
        DataType::Array(e) => holds_an_object(e),
        _ => false,
    }
}

/// Whether a value of type `ty` keeps the contract `wants`. A value whose type only the row knows keeps any; an
/// object keeps any object type, since its fields are contracts of their own; an `i64` keeps `f64`, as it widens.
fn holds(wants: &DataType, ty: &DataType) -> bool {
    use DataType as T;
    match (wants, ty) {
        (_, T::Null | T::Variant) | (T::Variant, _) => true,
        (T::Struct(..), T::Struct(..)) => true,
        (T::Array(w), T::Array(t)) => holds(w, t),
        (T::F64, T::I64) => true,
        _ => wants == ty,
    }
}

/// A field of a record that breaks a schema.
#[derive(Clone, Debug, PartialEq)]
pub struct Finding {
    /// The field's keys from the record's root.
    pub keys: Vec<String>,
    /// The schema's id.
    pub schema: String,
    pub misfit: Misfit,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Misfit {
    /// The field's type is not one the schema's holds.
    Type {
        ty: DataType,
        /// What the schema has it as, spelled as a declaration is.
        wants: String,
    },
    /// The schema requires the field, and the record does not have it.
    Missing,
}

impl Finding {
    /// The field's path as the pipeline editor shows it.
    pub fn path(&self) -> String {
        self.keys.join(".")
    }
}

impl Schema {
    /// The record type of the rows that fit the schema: each field at its path, a field under an array in each of its
    /// elements, and an object the schema names only by the fields under it as an object of those.
    pub fn record_type(&self) -> DataType {
        let mut root = DataType::Struct(Vec::new(), true);
        for f in &self.fields {
            place(&mut root, &f.keys(), &f.ty);
        }
        root
    }
}

/// Puts a field of type `ty` at `keys` under `at`, making each object above it that is not there yet.
fn place(at: &mut DataType, keys: &[String], ty: &DataType) {
    // An object typed `any` holds the fields under it.
    if fields_of_mut(at).is_none() {
        *at = DataType::Struct(Vec::new(), true);
    }
    let fields = fields_of_mut(at).expect("made an object above");
    let (key, below) = keys.split_first().expect("`read` refuses an empty path");
    let i = match fields.iter().position(|(k, _)| k == key) {
        Some(i) if below.is_empty() => return merge(&mut fields[i].1, ty),
        Some(i) => i,
        None if below.is_empty() => return fields.push((key.clone(), ty.clone())),
        None => {
            fields.push((key.clone(), DataType::Struct(Vec::new(), true)));
            fields.len() - 1
        }
    };
    place(&mut fields[i].1, below, ty);
}

/// `declared`, a field's own type, over `at`, the object its fields under it already made.
fn merge(at: &mut DataType, declared: &DataType) {
    match declared {
        DataType::Array(element) if !matches!(at, DataType::Array(_)) => {
            let mut inner = std::mem::replace(at, DataType::Null);
            merge(&mut inner, element);
            *at = DataType::Array(Box::new(inner));
        }
        DataType::Struct(declared, _) => {
            if let Some(fields) = fields_of_mut(at) {
                for (k, t) in declared {
                    if !fields.iter().any(|(have, _)| have == k) {
                        fields.push((k.clone(), t.clone()));
                    }
                }
            }
        }
        _ => {}
    }
}

fn fields_of_mut(ty: &mut DataType) -> Option<&mut Vec<(String, DataType)>> {
    match ty {
        DataType::Struct(fields, _) => Some(fields),
        DataType::Array(element) => fields_of_mut(element),
        _ => None,
    }
}

/// What a record has at each path. An array's elements sit at the array's path, so a field under it is each
/// element's.
struct Shape<'r> {
    root: &'r DataType,
    at: HashMap<Vec<String>, &'r DataType>,
}

impl<'r> Shape<'r> {
    fn of(record: &'r DataType) -> Shape<'r> {
        let mut shape = Shape {
            root: record,
            at: HashMap::new(),
        };
        shape.walk(record, &mut Vec::new());
        shape
    }

    fn walk(&mut self, ty: &'r DataType, keys: &mut Vec<String>) {
        if !keys.is_empty() {
            self.at.insert(keys.clone(), ty);
        }
        if let Some(fields) = fields_of(ty) {
            for (key, inner) in fields {
                keys.push(key.clone());
                self.walk(inner, keys);
                keys.pop();
            }
        }
    }

    /// The object at `keys`, where the record has one: the root, or a field holding an object or an array of them.
    fn object_at(&self, keys: &[String]) -> Option<&'r DataType> {
        if keys.is_empty() {
            return Some(self.root);
        }
        self.at.get(keys).copied().filter(|ty| holds_an_object(ty))
    }
}

/// The fields of an object, or of an array's element where that is one.
fn fields_of(ty: &DataType) -> Option<&[(String, DataType)]> {
    match ty {
        DataType::Struct(fields, _) => Some(fields),
        DataType::Array(element) => fields_of(element),
        _ => None,
    }
}

impl Schema {
    /// Where a record may break the schema: each field whose type the schema's does not hold, and each field the
    /// schema requires that the record lacks, in the schema's order. A field the schema does not name keeps it, as
    /// does one whose type only the row knows. A required field is missing where the object it sits in is there and
    /// does not declare it: an open object may carry the key, but the pipeline has not shown that it does.
    pub fn check(&self, record: &DataType) -> Vec<Finding> {
        let shape = Shape::of(record);
        let mut out = Vec::new();
        for f in &self.fields {
            let keys = f.keys();
            match shape.at.get(&keys) {
                Some(ty) => {
                    if !holds(&f.ty, ty) {
                        out.push(Finding {
                            keys,
                            schema: self.id.clone(),
                            misfit: Misfit::Type {
                                ty: (*ty).clone(),
                                wants: f.ty.spell(),
                            },
                        });
                    }
                }
                None => {
                    if f.required && shape.object_at(&keys[..keys.len() - 1]).is_some() {
                        out.push(Finding {
                            keys,
                            schema: self.id.clone(),
                            misfit: Misfit::Missing,
                        });
                    }
                }
            }
        }
        out
    }

    /// The field at `keys`, a record's keys from its root, where the schema names one.
    pub fn field(&self, keys: &[String]) -> Option<&Field> {
        let path = keys.join(".");
        self.fields.iter().find(|f| f.path == path)
    }
}

/// Where a record breaks the schemas a pipeline produces, all of them at once: a row keeps one of them, so a field
/// breaks them when every schema that names it says so, and a field is missing when every schema requires it.
pub fn check(schemas: &[Arc<Schema>], record: &DataType) -> Vec<Finding> {
    let each: Vec<Vec<Finding>> = schemas.iter().map(|s| s.check(record)).collect();
    let mut out: Vec<Finding> = Vec::new();
    for (i, findings) in each.iter().enumerate() {
        for f in findings {
            let stands = each.iter().enumerate().filter(|(j, _)| *j != i).all(|(j, other)| {
                let alike = other
                    .iter()
                    .any(|o| o.keys == f.keys && same_kind(&o.misfit, &f.misfit));
                match f.misfit {
                    Misfit::Type { .. } => alike || schemas[j].field(&f.keys).is_none(),
                    Misfit::Missing => alike,
                }
            });
            if stands && !out.iter().any(|o| o.keys == f.keys && same_kind(&o.misfit, &f.misfit)) {
                out.push(f.clone());
            }
        }
    }
    out
}

/// A field a schema requires that the record lacks, as a pipeline's author is shown it.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct Missing {
    /// The field's path, as the pipeline editor shows it.
    pub path: String,
    /// What a pipeline writes to keep it.
    pub written_as: String,
    /// The schema's title.
    pub schema: String,
}

/// Each field `findings` say the record lacks, with what to write it as and the schema requiring it.
pub fn missing(schemas: &[Arc<Schema>], findings: &[Finding]) -> Vec<Missing> {
    findings
        .iter()
        .filter(|f| f.misfit == Misfit::Missing)
        .filter_map(|f| {
            let schema = schemas.iter().find(|s| s.id == f.schema)?;
            let field = schema.field(&f.keys)?;
            Some(Missing {
                path: f.path(),
                written_as: field.ty.spell(),
                schema: schema.title.clone(),
            })
        })
        .collect()
}

/// What a check's `findings` against `schemas` say to a pipeline's author: a sentence for each field whose type
/// misfits, with the field's keys, and one for each schema's required fields the record lacks, with none.
pub fn told(schemas: &[Arc<Schema>], findings: &[Finding]) -> Vec<(Option<Vec<String>>, String)> {
    let title = |id: &str| (schemas.iter().find(|s| s.id == id)).map_or(id.to_string(), |s| s.title.clone());
    let mut out = Vec::new();
    let mut missing: BTreeMap<&str, Vec<String>> = BTreeMap::new();
    for f in findings {
        match &f.misfit {
            Misfit::Type { ty, wants } => {
                let path = f.path();
                let schema = title(&f.schema);
                // A timestamp written as epoch milliseconds is the case a function answers.
                let how = match (ty, wants.as_str()) {
                    (DataType::DateTime, "i64") => ": `.epochMillis()` gives one",
                    _ => "",
                };
                out.push((
                    Some(f.keys.clone()),
                    format!(
                        "`{path}` is {}, but {schema} has it as {wants}. Write it as {wants}{how}.",
                        ty.spell()
                    ),
                ));
            }
            Misfit::Missing => missing.entry(&f.schema).or_default().push(format!("`{}`", f.path())),
        }
    }
    for (schema, paths) in missing {
        let schema = title(schema);
        let lack = if paths.len() == 1 { "it" } else { "them" };
        out.push((
            None,
            format!("{schema} requires {}; the rows lack {lack}.", paths.join(", ")),
        ));
    }
    out
}

fn same_kind(a: &Misfit, b: &Misfit) -> bool {
    std::mem::discriminant(a) == std::mem::discriminant(b)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn st(fields: &[(&str, DataType)]) -> DataType {
        DataType::Struct(fields.iter().map(|(k, t)| (k.to_string(), t.clone())).collect(), false)
    }

    fn open(fields: &[(&str, DataType)]) -> DataType {
        DataType::Struct(fields.iter().map(|(k, t)| (k.to_string(), t.clone())).collect(), true)
    }

    fn shown(findings: &[Finding]) -> Vec<String> {
        findings
            .iter()
            .map(|f| match &f.misfit {
                Misfit::Type { ty, wants } => format!("{} {}: {} not {wants}", f.schema, f.path(), ty.spell()),
                Misfit::Missing => format!("{} {}: missing", f.schema, f.path()),
            })
            .collect()
    }

    fn row(field: &str, ty: &str, required: bool) -> SchemaFieldDto {
        SchemaFieldDto {
            field: field.to_string(),
            ty: ty.to_string(),
            required,
            description: String::new(),
        }
    }

    fn declared(id: &str, title: &str, rows: Vec<SchemaFieldDto>) -> Arc<Schema> {
        let dto = SchemaDto {
            id: id.to_string(),
            title: title.to_string(),
            description: String::new(),
            fields: rows,
        };
        Arc::new(read(&dto).unwrap())
    }

    #[test]
    fn a_schemas_record_type_holds_each_field_at_its_path() {
        let schema = declared(
            "s",
            "S",
            vec![
                row("user.name", "str", false),
                row("user", "{...}", true),
                row("src.ip", "str", false),
                row("hits.id", "str", false),
                row("hits", "array<{...}>", false),
                row("tags", "array<str>", false),
            ],
        );
        assert_eq!(
            schema.record_type(),
            open(&[
                ("user", open(&[("name", DataType::Str)])),
                ("src", open(&[("ip", DataType::Str)])),
                ("hits", DataType::Array(Box::new(open(&[("id", DataType::Str)])))),
                ("tags", DataType::Array(Box::new(DataType::Str))),
            ])
        );
    }

    /// A sign-in: a time in epoch milliseconds, objects, and an array of objects.
    fn login() -> Arc<Schema> {
        declared(
            "login",
            "Login",
            vec![
                row("activity_id", "i64", true),
                row("time", "i64", true),
                row("time_dt", "datetime", false),
                row("user", "{...}", true),
                row("user.name", "str", false),
                row("user.uid", "str", false),
                row("src_endpoint.ip", "str", false),
                row("src_endpoint.port", "i64", false),
                row("dst_endpoint", "{...}", false),
                row("status", "str", false),
                row("metadata", "{...}", true),
                row("metadata.product", "str", true),
                row("metadata.version", "str", false),
                row("observables", "array<{...}>", false),
                row("observables.name", "str", false),
                row("score", "f64", false),
                row("extra", "variant", false),
            ],
        )
    }

    /// A network connection, which names `user` and `time` as `login` does and `connection` alone.
    fn connection() -> Arc<Schema> {
        declared(
            "connection",
            "Connection",
            vec![
                row("time", "i64", true),
                row("user.name", "str", false),
                row("connection", "{...}", true),
                row("connection.uid", "str", false),
            ],
        )
    }

    #[test]
    fn a_table_that_does_not_read_is_refused_with_the_row() {
        let refused = |rows: Vec<SchemaFieldDto>| {
            read(&SchemaDto {
                id: "x".to_string(),
                title: String::new(),
                description: String::new(),
                fields: rows,
            })
            .unwrap_err()
        };
        assert_eq!(
            refused(vec![row("a", "str", false), row("a", "i64", false)]),
            "`a` is listed twice"
        );
        assert!(refused(vec![row("a", "string", false)]).starts_with("`a`: `string` is no type"));
        assert_eq!(
            refused(vec![row("a", "str", false), row("a.b", "i64", false)]),
            "`a.b` is under `a`, which is str"
        );
        assert!(refused(vec![row("a..b", "str", false)]).contains("no field path"));
        assert!(
            read(&to_dto(&login())).is_ok(),
            "a schema written and read back is the same"
        );
        assert_eq!(
            read(&SchemaDto {
                id: "t".to_string(),
                ..Default::default()
            })
            .unwrap()
            .title,
            "t"
        );
    }

    #[test]
    fn what_a_check_finds_is_told_a_sentence_a_field_and_one_for_what_a_schema_lacks() {
        let login = login();
        let record = st(&[("time", DataType::DateTime)]);
        let told = told(std::slice::from_ref(&login), &login.check(&record));
        assert_eq!(
            told,
            [
                (
                    Some(vec!["time".to_string()]),
                    "`time` is datetime, but Login has it as i64. Write it as i64: `.epochMillis()` gives one."
                        .to_string()
                ),
                (
                    None,
                    "Login requires `activity_id`, `user`, `metadata`; the rows lack them.".to_string()
                ),
            ]
        );
    }

    #[test]
    fn a_check_names_a_type_a_field_does_not_hold_and_each_required_field_missing() {
        let record = st(&[
            ("time", DataType::DateTime),
            ("time_dt", DataType::DateTime),
            ("user", st(&[("name", DataType::Str), ("uid", DataType::I64)])),
            ("src_endpoint", st(&[("ip", DataType::Str), ("port", DataType::Str)])),
            ("status", DataType::Str),
            ("unmapped", st(&[("anything", DataType::Bytes)])),
            ("metadata", DataType::Variant),
            ("score", DataType::I64),
            ("extra", DataType::Bytes),
        ]);
        assert_eq!(
            shown(&login().check(&record)),
            [
                "login activity_id: missing",
                "login time: datetime not i64",
                "login user.uid: i64 not str",
                "login src_endpoint.port: str not i64",
            ],
            "an i64 keeps f64, and a variant keeps anything"
        );
    }

    #[test]
    fn an_array_the_schema_does_not_name_is_a_misfit_and_one_it_names_holds_its_elements() {
        let record = st(&[
            ("status", DataType::Array(Box::new(DataType::Str))),
            ("observables", DataType::Str),
            ("dst_endpoint", DataType::Array(Box::new(st(&[("ip", DataType::Str)])))),
        ]);
        let mut findings = login().check(&record);
        findings.retain(|f| f.misfit != Misfit::Missing);
        assert_eq!(
            shown(&findings),
            [
                "login dst_endpoint: array<{ip: str}> not {...}",
                "login status: array<str> not str",
                "login observables: str not array<{...}>",
            ]
        );
        let held = login().check(&st(&[(
            "observables",
            DataType::Array(Box::new(st(&[("name", DataType::I64)]))),
        )]));
        assert_eq!(
            shown(&held),
            [
                "login activity_id: missing",
                "login time: missing",
                "login user: missing",
                "login metadata: missing",
                "login observables.name: i64 not str",
            ],
            "a field under an array is each element's"
        );
    }

    #[test]
    fn a_required_field_inside_an_object_is_missing_only_where_the_object_is_there() {
        let login = login();
        let without = login.check(&st(&[]));
        assert!(!without.iter().any(|f| f.keys.len() > 1), "{without:?}");
        let with = login.check(&st(&[("metadata", st(&[("version", DataType::Str)]))]));
        assert!(
            with.iter()
                .any(|f| f.path() == "metadata.product" && f.misfit == Misfit::Missing),
            "{with:?}"
        );
        let opened = login.check(&st(&[("metadata", open(&[("version", DataType::Str)]))]));
        assert!(
            opened.iter().any(|f| f.path() == "metadata.product"),
            "an open object has not shown it holds the field: {opened:?}"
        );
    }

    #[test]
    fn several_schemas_break_where_each_naming_the_field_does_and_require_what_all_require() {
        let record = st(&[
            ("user", DataType::Str),
            ("time", DataType::DateTime),
            ("connection", DataType::I64),
        ]);
        let shown = shown(&check(&[login(), connection()], &record));
        assert_eq!(
            shown,
            [
                "login time: datetime not i64",
                "login user: str not {...}",
                "connection connection: i64 not {...}",
            ],
            "`user` and `time` break both; `connection` is named by one alone, and no field is required by both and \
             missing"
        );
    }
}
