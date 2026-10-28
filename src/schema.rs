//! A schema is a contract on a pipeline's rows: the fields a row may have by name, the type each must be, which a row
//! must have, and what each means. ECS and every OCSF event class are built in; a bundle declares its own as JSON
//! Schema (`json_schema`). A pipeline names the schemas it produces, and `check` holds the type of its record
//! (`plan::dictionary`) to them.
//!
//! A schema is a root object and the objects under it, shared where a schema names one object from many places, as
//! OCSF does. How a schema reads a row is its `Reading`: Elasticsearch reads a dotted key as a path and any field as
//! one value or many, so ECS does too; JSON Schema and OCSF read a key as it is, and a field is an array only where
//! the schema says so.

use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};

use crate::core::column::DataType;
use crate::io::elastic_mapping::Ecs;

pub mod ecs;
pub mod json_schema;
pub mod ocsf;
mod ocsf_schema;

pub use ocsf_schema::VERSION as OCSF_VERSION;

/// One contract, by id: `ecs`, `ocsf/<class>`, or a bundle's own.
#[derive(Debug)]
pub struct Schema {
    pub id: String,
    pub title: String,
    pub description: String,
    pub origin: Origin,
    pub reading: Reading,
    objects: Arc<[Object]>,
    root: usize,
}

/// Where a schema comes from.
#[derive(Clone, Debug, PartialEq)]
pub enum Origin {
    Ecs {
        version: &'static str,
    },
    Ocsf {
        version: &'static str,
        class_uid: u32,
        category: u32,
    },
    /// Declared in a bundle, as JSON Schema.
    Bundle,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Reading {
    /// A dotted key is a path, and any field holds one value or an array of them.
    Elasticsearch,
    /// A key is a name as it is, and a field is an array only where the schema says so.
    Json,
}

#[derive(Clone, Debug)]
pub struct Object {
    /// The schema's own name for the object, where it has one: `user`, `network_endpoint`.
    pub name: String,
    pub description: String,
    pub fields: Vec<Field>,
}

#[derive(Clone, Debug)]
pub struct Field {
    pub name: String,
    pub ty: Type,
    /// The schema's own name for the type: `keyword`, `timestamp`, `user`.
    pub type_name: String,
    pub array: bool,
    pub required: bool,
    pub description: String,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Type {
    Leaf(Leaf),
    /// The object at this index of the schema's objects. An implied object is no field of its own but the path to
    /// fields under it, as `source` is in ECS: a value of another type there is no misfit.
    Object {
        at: usize,
        implied: bool,
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
    /// As ECS maps it, for Elasticsearch.
    Ecs(Ecs),
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
            Leaf::Ecs(e) => e.holds(ty),
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
            Leaf::Ecs(e) => e.written_as(),
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
        for segment in keys.iter().flat_map(|k| self.segments(k)) {
            let f = self.objects[at?].fields.iter().find(|f| f.name == segment)?;
            at = match f.ty {
                Type::Object { at, .. } => Some(at),
                Type::Leaf(_) => None,
            };
            found = Some(f);
        }
        found
    }

    fn segments<'k>(&self, key: &'k str) -> Vec<&'k str> {
        match self.reading {
            Reading::Elasticsearch => key.split('.').collect(),
            Reading::Json => vec![key],
        }
    }

    fn walk(&self, at: usize, fields: &[(String, DataType)], keys: &mut Vec<String>, out: &mut Vec<Finding>) {
        let mut present = Vec::new();
        for (key, ty) in fields {
            let segments = self.segments(key);
            present.push(segments[0]);
            // Down the path a dotted key names, to the field its last segment names.
            let mut object = at;
            let mut field = None;
            for (i, segment) in segments.iter().enumerate() {
                let Some(f) = self.objects[object].fields.iter().find(|f| f.name == *segment) else {
                    break;
                };
                match f.ty {
                    _ if i + 1 == segments.len() => field = Some(f),
                    Type::Object { at, .. } => object = at,
                    Type::Leaf(_) => break,
                }
            }
            if let Some(f) = field {
                keys.push(key.clone());
                self.value(f, ty, keys, out);
                keys.pop();
            }
        }
        for f in self.objects[at].fields.iter().filter(|f| f.required) {
            if !present.contains(&f.name.as_str()) {
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
        let ty = match (self.reading, f.array, ty) {
            // Only the row knows: no misfit can be seen here.
            (_, _, DataType::Null | DataType::Variant) => return,
            (Reading::Elasticsearch, _, DataType::Array(element)) => return self.value(f, element, keys, out),
            (Reading::Elasticsearch, _, ty) => ty,
            (Reading::Json, true, DataType::Array(element)) => element,
            (Reading::Json, false, ty @ DataType::Array(_)) | (Reading::Json, true, ty) => {
                return self.misfit(f, ty, keys, out);
            }
            (Reading::Json, false, ty) => ty,
        };
        if matches!(ty, DataType::Null | DataType::Variant) {
            return;
        }
        match f.ty {
            Type::Leaf(leaf) if !leaf.holds(ty) => self.misfit(f, ty, keys, out),
            Type::Leaf(_) => {}
            Type::Object { at, .. } if let DataType::Struct(fields, _) = ty => self.walk(at, fields, keys, out),
            Type::Object { implied: true, .. } => {}
            Type::Object { implied: false, .. } => self.misfit(f, ty, keys, out),
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
        match (self.reading, f.array) {
            (Reading::Json, true) => format!("array({element})"),
            _ => element.to_string(),
        }
    }

    /// The object at `at`, where the schema has one.
    pub fn object_at(&self, at: usize) -> Option<&Object> {
        self.objects.get(at)
    }

    /// Every field of the schema by path from its root, each with the path's keys, to `depth` objects deep. An
    /// object OCSF names from many places, or from within itself, is listed again under each path that reaches it.
    pub fn fields(&self, depth: usize) -> Vec<(Vec<String>, &Field)> {
        let mut out = Vec::new();
        self.list(self.root, depth, &mut Vec::new(), &mut out);
        out
    }

    fn list<'s>(&'s self, at: usize, depth: usize, keys: &mut Vec<String>, out: &mut Vec<(Vec<String>, &'s Field)>) {
        for f in &self.objects[at].fields {
            keys.push(f.name.clone());
            out.push((keys.clone(), f));
            if let Type::Object { at, .. } = f.ty
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
        // That they are missing is the finding's own news, so it names them and says no more.
        out.push((None, format!("{schema} requires: {}.", paths.join(", "))));
    }
    out
}

fn same_kind(a: &Misfit, b: &Misfit) -> bool {
    std::mem::discriminant(a) == std::mem::discriminant(b)
}

/// Every built-in schema, ECS first and then each OCSF class by class uid.
pub fn builtins() -> &'static [Arc<Schema>] {
    static ALL: OnceLock<Vec<Arc<Schema>>> = OnceLock::new();
    ALL.get_or_init(|| {
        std::iter::once(Arc::new(ecs::schema()))
            .chain(ocsf::schemas().into_iter().map(Arc::new))
            .collect()
    })
}

/// The built-in schema `id` names.
pub fn builtin(id: &str) -> Option<Arc<Schema>> {
    builtins().iter().find(|s| s.id == id).cloned()
}

/// Whether `id` is a built-in's, or in the space the built-ins take: a bundle's own schema may take neither.
pub fn reserved(id: &str) -> bool {
    id == "ecs" || id.starts_with("ecs/") || id == "ocsf" || id.starts_with("ocsf/")
}

/// Builds a schema's objects one at a time, an object named again being the one already built.
#[derive(Default)]
pub(crate) struct Objects {
    objects: Vec<Object>,
    named: BTreeMap<String, usize>,
}

impl Objects {
    pub(crate) fn push(&mut self, object: Object) -> usize {
        self.objects.push(object);
        self.objects.len() - 1
    }

    pub(crate) fn get_mut(&mut self, at: usize) -> &mut Object {
        &mut self.objects[at]
    }

    pub(crate) fn named(&self, name: &str) -> Option<usize> {
        self.named.get(name).copied()
    }

    pub(crate) fn name(&mut self, name: &str, at: usize) {
        self.named.insert(name.to_string(), at);
    }

    pub(crate) fn into_shared(self) -> Arc<[Object]> {
        self.objects.into()
    }
}

impl Schema {
    pub(crate) fn new(
        id: String,
        title: String,
        description: String,
        origin: Origin,
        reading: Reading,
        objects: Arc<[Object]>,
        root: usize,
    ) -> Schema {
        Schema {
            id,
            title,
            description,
            origin,
            reading,
            objects,
            root,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn what_a_check_finds_is_told_a_sentence_a_field_and_one_for_what_a_schema_lacks() {
        let auth = builtin("ocsf/authentication").unwrap();
        let record = DataType::Struct(vec![("time".to_string(), DataType::DateTime)], false);
        let told = told(&[auth.clone()], &auth.check(&record));
        assert_eq!(
            told[0],
            (
                Some(vec!["time".to_string()]),
                "`time` is datetime, but OCSF Authentication maps it as timestamp. Write it as i64: \
                 `.epochMillis()` gives one."
                    .to_string()
            )
        );
        assert_eq!(told.len(), 2, "{told:?}");
        assert!(told[1].1.starts_with("OCSF Authentication requires: `activity_id`,"));
    }

    #[test]
    fn ocsf_names_a_type_a_field_does_not_hold_and_each_required_field_missing() {
        let auth = builtin("ocsf/authentication").unwrap();
        let record = st(&[
            ("time", DataType::DateTime),
            ("time_dt", DataType::DateTime),
            ("user", st(&[("name", DataType::Str), ("uid", DataType::I64)])),
            ("src_endpoint", st(&[("ip", DataType::Str), ("port", DataType::Str)])),
            ("status", DataType::Str),
            ("unmapped", st(&[("anything", DataType::Bytes)])),
            ("message", DataType::Variant),
        ]);
        assert_eq!(
            shown(&auth.check(&record)),
            [
                "ocsf/authentication time: datetime not timestamp, write i64",
                "ocsf/authentication user.uid: i64 not string, write str",
                "ocsf/authentication src_endpoint.port: str not port, write i64",
                "ocsf/authentication activity_id: missing",
                "ocsf/authentication category_uid: missing",
                "ocsf/authentication class_uid: missing",
                "ocsf/authentication metadata: missing",
                "ocsf/authentication severity_id: missing",
                "ocsf/authentication type_uid: missing",
            ]
        );
    }

    #[test]
    fn an_array_ocsf_does_not_name_is_a_misfit_and_one_it_names_holds_its_elements() {
        let auth = builtin("ocsf/authentication").unwrap();
        let record = st(&[
            ("status", DataType::Array(Box::new(DataType::Str))),
            ("observables", DataType::Str),
            (
                "enrichments",
                DataType::Array(Box::new(st(&[("name", DataType::Str), ("value", DataType::Str)]))),
            ),
        ]);
        let mut findings = auth.check(&record);
        findings.retain(|f| f.misfit != Misfit::Missing);
        assert_eq!(
            shown(&findings),
            [
                "ocsf/authentication status: array(str) not string, write str",
                "ocsf/authentication observables: str not observable, write array(struct)",
            ]
        );
    }

    #[test]
    fn a_required_field_inside_an_object_is_missing_only_where_the_object_is_there() {
        let auth = builtin("ocsf/authentication").unwrap();
        let without = auth.check(&st(&[]));
        assert!(!without.iter().any(|f| f.keys.len() > 1));
        let with = auth.check(&st(&[("metadata", st(&[("version", DataType::Str)]))]));
        assert!(
            with.iter()
                .any(|f| f.path() == "metadata.product" && f.misfit == Misfit::Missing),
            "{with:?}"
        );
    }

    #[test]
    fn several_schemas_break_where_each_naming_the_field_does_and_require_what_all_require() {
        let auth = builtin("ocsf/authentication").unwrap();
        let network = builtin("ocsf/network_activity").unwrap();
        let record = st(&[
            ("user", DataType::Str),
            ("time", DataType::DateTime),
            ("connection_info", DataType::I64),
        ]);
        let both = check(&[auth.clone(), network.clone()], &record);
        let shown = shown(&both);
        // `user` is required by one and named by both differently: Authentication's is an object, and so is Network
        // Activity's, so a string breaks both.
        assert!(shown.contains(&"ocsf/authentication time: datetime not timestamp, write i64".to_string()));
        assert!(shown.contains(&"ocsf/authentication class_uid: missing".to_string()));
        assert!(!shown.iter().any(|s| s.ends_with("src_endpoint: missing")), "{shown:?}");
        assert!(
            shown.contains(
                &"ocsf/network_activity connection_info: i64 not network_connection_info, write struct".to_string()
            ),
            "a field one schema names alone breaks where that one says: {shown:?}"
        );
    }

    #[test]
    fn every_builtin_has_a_reserved_id_and_ids_are_unique() {
        let ids: Vec<&str> = builtins().iter().map(|s| s.id.as_str()).collect();
        assert!(ids.iter().all(|id| reserved(id)));
        let mut sorted = ids.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(sorted.len(), ids.len());
        assert_eq!(ids[0], "ecs");
        assert!(!reserved("acme-login") && !reserved("ecs-like") && !reserved("ocsfx"));
    }
}
