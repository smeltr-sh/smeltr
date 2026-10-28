//! The schema catalog: the built-in schemas, one object of any schema at a time, a search over every schema's
//! fields, and a built-in written as JSON Schema. The leader's `/api/schemas/*` routes and the website demo's
//! service answer from here. A bundle's own schemas are config like any other: read with `GET /api/config`, saved
//! with `PUT /api/schemas`.

use serde_json::Value;
use std::sync::Arc;

use crate::config_file::SchemaDto;
use crate::schema::{self, Origin, Schema, Type, json_schema, ocsf};

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Catalog {
    ecs_version: &'static str,
    ocsf_version: &'static str,
    /// OCSF's categories, by uid.
    categories: Vec<Category>,
    schemas: Vec<SchemaOut>,
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Category {
    uid: u32,
    name: &'static str,
    caption: &'static str,
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SchemaOut {
    id: String,
    title: String,
    description: String,
    /// `ecs`, `ocsf` or `bundle`.
    origin: &'static str,
    /// The OCSF class's category uid.
    #[serde(skip_serializing_if = "Option::is_none")]
    category: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    class_uid: Option<u32>,
}

impl SchemaOut {
    fn of(s: &Schema) -> SchemaOut {
        let (origin, category, class_uid) = match s.origin {
            Origin::Ecs { .. } => ("ecs", None, None),
            Origin::Ocsf {
                category, class_uid, ..
            } => ("ocsf", Some(category), Some(class_uid)),
            Origin::Bundle => ("bundle", None, None),
        };
        SchemaOut {
            id: s.id.clone(),
            title: s.title.clone(),
            description: s.description.clone(),
            origin,
            category,
            class_uid,
        }
    }
}

/// Every built-in schema, with OCSF's categories.
pub fn catalog() -> Catalog {
    Catalog {
        ecs_version: crate::io::ecs_fields::ECS_VERSION,
        ocsf_version: schema::OCSF_VERSION,
        categories: (ocsf::categories().iter())
            .map(|&(uid, name, caption)| Category { uid, name, caption })
            .collect(),
        schemas: schema::builtins().iter().map(|s| SchemaOut::of(s)).collect(),
    }
}

/// A schema by its built-in id, or one written as JSON Schema: what an Add schema form checks before it saves.
#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ObjectRequest {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    schema: Option<Value>,
    /// The object, from `FieldOut::object`. The schema's root when absent.
    #[serde(default)]
    at: Option<usize>,
}

#[derive(Default, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ObjectOut {
    /// Its index, which a field that holds it names.
    at: usize,
    name: String,
    description: String,
    fields: Vec<FieldOut>,
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FieldOut {
    name: String,
    /// The schema's own name for its type.
    type_name: String,
    /// What a pipeline writes to keep it.
    written_as: String,
    array: bool,
    required: bool,
    description: String,
    /// The object it holds, to open with another request.
    #[serde(skip_serializing_if = "Option::is_none")]
    object: Option<usize>,
}

fn resolve(id: Option<&str>, doc: Option<&Value>) -> Result<Arc<Schema>, String> {
    match (id, doc) {
        (_, Some(doc)) => json_schema::read(id.unwrap_or_default(), doc).map(Arc::new),
        (Some(id), None) => schema::builtin(id).ok_or_else(|| format!("no built-in schema has the id {id:?}")),
        (None, None) => Err("name a built-in schema's id, or give a schema".to_string()),
    }
}

/// One object of a schema, its fields each with what holds it. OCSF's objects name each other, and themselves, so a
/// schema is opened an object at a time.
pub fn object(req: &ObjectRequest) -> Result<ObjectOut, String> {
    resolve(req.id.as_deref(), req.schema.as_ref()).and_then(|s| {
        let at = req.at.unwrap_or_else(|| s.root_at());
        let o = s
            .object_at(at)
            .ok_or_else(|| format!("the schema has no object {at}"))?;
        Ok(ObjectOut {
            at,
            name: o.name.clone(),
            description: o.description.clone(),
            fields: (o.fields.iter())
                .map(|f| FieldOut {
                    name: f.name.clone(),
                    type_name: f.type_name.clone(),
                    written_as: s.written_as(f),
                    array: f.array,
                    required: f.required,
                    description: f.description.clone(),
                    object: match f.ty {
                        Type::Object { at, .. } => Some(at),
                        Type::Leaf(_) => None,
                    },
                })
                .collect(),
        })
    })
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SearchRequest {
    q: String,
    /// The bundle's own, searched beside the built-ins.
    #[serde(default)]
    schemas: Vec<SchemaDto>,
}

#[derive(Default, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Matches {
    matches: Vec<Match>,
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Match {
    id: String,
    /// The first fields whose path holds the words, by path.
    fields: Vec<String>,
}

/// How deep a search reads a schema's objects: OCSF's name each other without end.
const SEARCH_DEPTH: usize = 3;
/// How many matching fields a match names.
const FIELDS_NAMED: usize = 5;

/// Every schema whose id, title or a field's path holds `q`, ignoring case: for someone who knows a field and not the
/// class it is in.
pub fn search(req: &SearchRequest) -> Matches {
    let q = req.q.trim().to_lowercase();
    if q.is_empty() {
        return Matches::default();
    }
    let declared = req
        .schemas
        .iter()
        .filter_map(|d| json_schema::read(&d.id, &d.schema).ok().map(Arc::new));
    let matches = (declared.chain(schema::builtins().iter().cloned()))
        .filter_map(|s| {
            let fields: Vec<String> = (s.fields(SEARCH_DEPTH).into_iter())
                .map(|(keys, _)| keys.join("."))
                .filter(|path| path.to_lowercase().contains(&q))
                .take(FIELDS_NAMED)
                .collect();
            let named = s.id.to_lowercase().contains(&q) || s.title.to_lowercase().contains(&q);
            (named || !fields.is_empty()).then(|| Match {
                id: s.id.clone(),
                fields,
            })
        })
        .collect();
    Matches { matches }
}

#[derive(Default, serde::Serialize)]
pub struct Exported {
    schema: Value,
}

/// A built-in schema as JSON Schema, which a bundle can declare as its own and a catalog can import.
pub fn export(id: &str) -> Result<Exported, String> {
    match schema::builtin(id) {
        Some(s) => Ok(Exported {
            schema: json_schema::write(&s),
        }),
        None => Err(format!("no built-in schema has the id {id:?}")),
    }
}

/// What breaks a schema in `file`, a group's bundle merged with `shared/`: each pipeline's rows against the schemas it
/// produces, and each destination's accepts against what reaches it. `Err` when the bundle does not load.
pub fn findings(
    file: crate::config_file::FileConfig,
    declared_at: &crate::config_file::DeclaredAt,
) -> Result<Vec<crate::config_file::SchemaFinding>, String> {
    let system = crate::config_file::validate_at(file, declared_at).map_err(|e| e.to_string())?;
    // Validating compiled it, so this compile does not fail.
    let compiled = system
        .compile_topology()
        .map_err(|_| "the bundle does not compile".to_string())?;
    Ok(system.schema_findings(&compiled))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn opened(req: Value) -> Result<ObjectOut, String> {
        object(&serde_json::from_value(req).unwrap())
    }

    #[test]
    fn a_schema_opens_an_object_at_a_time() {
        let root = opened(json!({ "id": "ocsf/authentication" })).unwrap();
        let user = root.fields.iter().find(|f| f.name == "user").unwrap();
        assert_eq!((user.type_name.as_str(), user.written_as.as_str()), ("user", "struct"));
        let time = root.fields.iter().find(|f| f.name == "time").unwrap();
        assert_eq!((time.written_as.as_str(), time.required), ("i64", true));

        let inner = opened(json!({ "id": "ocsf/authentication", "at": user.object.unwrap() })).unwrap();
        assert_eq!(inner.name, "user");
        assert!(inner.fields.iter().any(|f| f.name == "name"));

        let declared =
            opened(json!({ "schema": { "type": "object", "properties": { "a": { "type": "boolean" } } } })).unwrap();
        assert_eq!(declared.fields[0].written_as, "bool");
        assert!(opened(json!({ "schema": { "type": "string" } })).is_err());
        assert!(opened(json!({ "id": "ocsf/nothing" })).is_err());
    }

    #[test]
    fn a_search_finds_a_schema_by_a_fields_path() {
        let req = json!({ "q": "LOGON_PROCESS", "schemas": [
            { "id": "mine", "schema": { "type": "object", "properties": { "logon_process": { "type": "string" } } } }
        ] });
        let found = search(&serde_json::from_value(req).unwrap()).matches;
        assert_eq!(found[0].id, "mine", "the bundle's own come first");
        let auth = found.iter().find(|m| m.id == "ocsf/authentication").unwrap();
        assert!(
            auth.fields.iter().any(|f| f.starts_with("logon_process")),
            "{:?}",
            auth.fields
        );
    }
}
