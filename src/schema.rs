//! A schema is a contract on a pipeline's rows: the fields a row may have by name, the type each must be, which a row
//! must have, and what each means. A bundle declares its schemas as JSON Schema (`json_schema`). A pipeline names the
//! schemas it produces, and `check` holds the type of its record (`plan::dictionary`) to them.
//!
//! A schema is a root object and the objects under it, shared where a schema names one object from many places. A
//! key is a name as it is, and a field is an array only where the schema says so.

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::core::column::DataType;

pub mod json_schema;

/// One contract, by its id in the bundle.
#[derive(Debug)]
pub struct Schema {
    pub id: String,
    pub title: String,
    pub description: String,
    objects: Arc<[Object]>,
    root: usize,
}

#[derive(Clone, Debug)]
pub struct Object {
    /// The schema's own name for the object, where it has one: the `$defs` key that names it.
    pub name: String,
    pub description: String,
    pub fields: Vec<Field>,
}

#[derive(Clone, Debug)]
pub struct Field {
    pub name: String,
    pub ty: Type,
    /// The schema's own name for the type: `string`, `date-time`, `user`.
    pub type_name: String,
    pub array: bool,
    pub required: bool,
    pub description: String,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Type {
    Leaf(Leaf),
    /// The object at this index of the schema's objects.
    Object {
        at: usize,
    },
}

/// The value a field must have.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Leaf {
    Str,
    /// A whole number. A duration is one, as it is written: a count of microseconds.
    Long,
    Float,
    Bool,
    /// A date and time, as RFC 3339 text.
    DateTime,
    /// A date and time as milliseconds since the Unix epoch.
    EpochMillis,
    /// Any value at all.
    Json,
}

impl Leaf {
    /// Whether a value of type `ty` keeps this contract.
    pub fn holds(self, ty: &DataType) -> bool {
        use DataType as T;
        match self {
            Leaf::Str => *ty == T::Str,
            Leaf::Long => matches!(ty, T::I64 | T::Duration),
            Leaf::Float => matches!(ty, T::I64 | T::F64),
            Leaf::Bool => *ty == T::Bool,
            Leaf::DateTime => *ty == T::DateTime,
            Leaf::EpochMillis => *ty == T::I64,
            Leaf::Json => true,
        }
    }

    /// The type a pipeline writes to keep this contract, as the pipeline editor spells it.
    pub fn written_as(self) -> &'static str {
        match self {
            Leaf::Str => "str",
            Leaf::Long | Leaf::EpochMillis => "i64",
            Leaf::Float => "f64",
            Leaf::Bool => "bool",
            Leaf::DateTime => "datetime",
            Leaf::Json => "any type",
        }
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
        /// The schema's name for its type.
        type_name: String,
        /// What to write instead, as the pipeline editor spells a type.
        written_as: String,
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
    pub fn root(&self) -> &Object {
        &self.objects[self.root]
    }

    /// The root's index among the schema's objects, which a field holding an object names its own by.
    pub fn root_at(&self) -> usize {
        self.root
    }

    pub fn object(&self, at: usize) -> &Object {
        &self.objects[at]
    }

    /// Where a record may break the schema: each field whose type the schema's does not hold, and each field the
    /// schema requires that the record lacks, in the record's order. A field the schema does not name keeps it, as
    /// does one whose type only the row knows.
    pub fn check(&self, record: &DataType) -> Vec<Finding> {
        let mut out = Vec::new();
        if let DataType::Struct(fields, _) = record {
            self.walk(self.root, fields, &mut Vec::new(), &mut out);
        }
        out
    }

    /// The field at `keys`, a record's keys from its root, where the schema names one.
    pub fn field(&self, keys: &[String]) -> Option<&Field> {
        let mut at = Some(self.root);
        let mut found = None;
        for key in keys {
            let f = self.objects[at?].fields.iter().find(|f| f.name == *key)?;
            at = match f.ty {
                Type::Object { at } => Some(at),
                Type::Leaf(_) => None,
            };
            found = Some(f);
        }
        found
    }

    fn walk(&self, at: usize, fields: &[(String, DataType)], keys: &mut Vec<String>, out: &mut Vec<Finding>) {
        let object = &self.objects[at];
        for (key, ty) in fields {
            if let Some(f) = object.fields.iter().find(|f| f.name == *key) {
                keys.push(key.clone());
                self.value(f, ty, keys, out);
                keys.pop();
            }
        }
        for f in object.fields.iter().filter(|f| f.required) {
            if !fields.iter().any(|(key, _)| *key == f.name) {
                let mut keys = keys.clone();
                keys.push(f.name.clone());
                out.push(Finding {
                    keys,
                    schema: self.id.clone(),
                    misfit: Misfit::Missing,
                });
            }
        }
    }

    fn value(&self, f: &Field, ty: &DataType, keys: &mut Vec<String>, out: &mut Vec<Finding>) {
        let ty = match (f.array, ty) {
            // Only the row knows: no misfit can be seen here.
            (_, DataType::Null | DataType::Variant) => return,
            (true, DataType::Array(element)) => element,
            (false, ty @ DataType::Array(_)) | (true, ty) => return self.misfit(f, ty, keys, out),
            (false, ty) => ty,
        };
        if matches!(ty, DataType::Null | DataType::Variant) {
            return;
        }
        match f.ty {
            Type::Leaf(leaf) if !leaf.holds(ty) => self.misfit(f, ty, keys, out),
            Type::Leaf(_) => {}
            Type::Object { at } if let DataType::Struct(fields, _) = ty => self.walk(at, fields, keys, out),
            Type::Object { .. } => self.misfit(f, ty, keys, out),
        }
    }

    fn misfit(&self, f: &Field, ty: &DataType, keys: &[String], out: &mut Vec<Finding>) {
        let written_as = self.written_as(f);
        out.push(Finding {
            keys: keys.to_vec(),
            schema: self.id.clone(),
            misfit: Misfit::Type {
                ty: ty.clone(),
                type_name: f.type_name.clone(),
                written_as,
            },
        });
    }

    /// The type a pipeline writes to keep `f`, one of this schema's fields, as the pipeline editor spells it.
    pub fn written_as(&self, f: &Field) -> String {
        let element = match f.ty {
            Type::Leaf(leaf) => leaf.written_as(),
            Type::Object { .. } => "struct",
        };
        if f.array {
            format!("array({element})")
        } else {
            element.to_string()
        }
    }

    /// The object at `at`, where the schema has one.
    pub fn object_at(&self, at: usize) -> Option<&Object> {
        self.objects.get(at)
    }

    /// Every field of the schema by path from its root, each with the path's keys, to `depth` objects deep. An
    /// object named from many places, or from within itself, is listed again under each path that reaches it.
    pub fn fields(&self, depth: usize) -> Vec<(Vec<String>, &Field)> {
        let mut out = Vec::new();
        self.list(self.root, depth, &mut Vec::new(), &mut out);
        out
    }

    fn list<'s>(&'s self, at: usize, depth: usize, keys: &mut Vec<String>, out: &mut Vec<(Vec<String>, &'s Field)>) {
        for f in &self.objects[at].fields {
            keys.push(f.name.clone());
            out.push((keys.clone(), f));
            if let Type::Object { at } = f.ty
                && depth > 1
            {
                self.list(at, depth - 1, keys, out);
            }
            keys.pop();
        }
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
                written_as: schema.written_as(field),
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
            Misfit::Type {
                ty,
                type_name,
                written_as,
            } => {
                let path = f.path();
                let schema = title(&f.schema);
                // A timestamp written as epoch milliseconds is the case a function answers.
                let how = match (ty, written_as.as_str()) {
                    (DataType::DateTime, "i64") => ": `.epochMillis()` gives one",
                    _ => "",
                };
                out.push((
                    Some(f.keys.clone()),
                    format!("`{path}` is {ty}, but {schema} maps it as {type_name}. Write it as {written_as}{how}."),
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

/// Builds a schema's objects one at a time.
#[derive(Default)]
pub(crate) struct Objects {
    objects: Vec<Object>,
}

impl Objects {
    pub(crate) fn push(&mut self, object: Object) -> usize {
        self.objects.push(object);
        self.objects.len() - 1
    }

    pub(crate) fn get_mut(&mut self, at: usize) -> &mut Object {
        &mut self.objects[at]
    }

    pub(crate) fn into_shared(self) -> Arc<[Object]> {
        self.objects.into()
    }
}

impl Schema {
    pub(crate) fn new(id: String, title: String, description: String, objects: Arc<[Object]>, root: usize) -> Schema {
        Schema {
            id,
            title,
            description,
            objects,
            root,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn st(fields: &[(&str, DataType)]) -> DataType {
        DataType::Struct(fields.iter().map(|(k, t)| (k.to_string(), t.clone())).collect(), false)
    }

    fn shown(findings: &[Finding]) -> Vec<String> {
        findings
            .iter()
            .map(|f| match &f.misfit {
                Misfit::Type {
                    ty,
                    type_name,
                    written_as,
                } => {
                    format!("{} {}: {ty} not {type_name}, write {written_as}", f.schema, f.path())
                }
                Misfit::Missing => format!("{} {}: missing", f.schema, f.path()),
            })
            .collect()
    }

    /// A sign-in, shaped as OCSF shapes one: a time in epoch milliseconds, objects named from many places, and
    /// arrays of objects.
    fn login() -> Arc<Schema> {
        let endpoint = json!({ "$ref": "#/$defs/endpoint" });
        let doc = json!({
            "title": "Login",
            "type": "object",
            "required": ["activity_id", "time", "user", "metadata"],
            "properties": {
                "activity_id": { "type": "integer" },
                "time": { "type": "integer", "format": "epoch-millis" },
                "time_dt": { "type": "string", "format": "date-time" },
                "user": { "$ref": "#/$defs/user" },
                "src_endpoint": endpoint,
                "dst_endpoint": endpoint,
                "status": { "type": "string" },
                "metadata": {
                    "type": "object",
                    "required": ["product"],
                    "properties": { "product": { "type": "string" }, "version": { "type": "string" } },
                },
                "observables": { "type": "array", "items": { "$ref": "#/$defs/observable" } },
            },
            "$defs": {
                "user": { "type": "object", "properties": { "name": { "type": "string" }, "uid": { "type": "string" } } },
                "endpoint": { "type": "object", "properties": { "ip": { "type": "string" }, "port": { "type": "integer" } } },
                "observable": { "type": "object", "properties": { "name": { "type": "string" } } },
            },
        });
        Arc::new(json_schema::read("login", &doc).unwrap())
    }

    /// A network connection, which names `user` and `time` as `login` does and `connection` alone.
    fn connection() -> Arc<Schema> {
        let doc = json!({
            "title": "Connection",
            "type": "object",
            "required": ["time", "connection"],
            "properties": {
                "time": { "type": "integer", "format": "epoch-millis" },
                "user": { "type": "object", "properties": { "name": { "type": "string" } } },
                "connection": { "type": "object", "properties": { "uid": { "type": "string" } } },
            },
        });
        Arc::new(json_schema::read("connection", &doc).unwrap())
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
                    "`time` is datetime, but Login maps it as epoch-millis. Write it as i64: `.epochMillis()` gives \
                     one."
                        .to_string()
                ),
                (
                    None,
                    "Login requires `activity_id`, `metadata`, `user`; the rows lack them.".to_string()
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
        ]);
        assert_eq!(
            shown(&login().check(&record)),
            [
                "login time: datetime not epoch-millis, write i64",
                "login user.uid: i64 not string, write str",
                "login src_endpoint.port: str not integer, write i64",
                "login activity_id: missing",
            ]
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
                "login status: array(str) not string, write str",
                "login observables: str not observable, write array(struct)",
                "login dst_endpoint: array(struct) not endpoint, write struct",
            ]
        );
        let held = login().check(&st(&[(
            "observables",
            DataType::Array(Box::new(st(&[("name", DataType::Str)]))),
        )]));
        assert!(!held.iter().any(|f| f.path() == "observables"), "{held:?}");
    }

    #[test]
    fn a_required_field_inside_an_object_is_missing_only_where_the_object_is_there() {
        let login = login();
        let without = login.check(&st(&[]));
        assert!(!without.iter().any(|f| f.keys.len() > 1));
        let with = login.check(&st(&[("metadata", st(&[("version", DataType::Str)]))]));
        assert!(
            with.iter()
                .any(|f| f.path() == "metadata.product" && f.misfit == Misfit::Missing),
            "{with:?}"
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
                "login user: str not user, write struct",
                "login time: datetime not epoch-millis, write i64",
                "connection connection: i64 not object, write struct",
            ],
            "`user` and `time` break both; `connection` is named by one alone, and no field is required by both and \
             missing"
        );
    }
}
