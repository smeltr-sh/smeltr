//! The schema catalog: the built-in schemas, one object of any schema at a time, a search over every schema's
//! fields, and a built-in written as JSON Schema. A bundle's own schemas are config like any other: read with
//! `GET /api/config`, saved with `PUT /api/schemas`.

use axum::Json;
use axum::extract::Query;
use serde_json::Value;
use std::sync::Arc;

use smeltr::config_file::SchemaDto;
use smeltr::leader::envelope::Envelope;
use smeltr::schema::{self, Origin, Schema, Type, json_schema, ocsf};

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

pub async fn builtin() -> Json<Envelope<Catalog>> {
    Json(Envelope::ok(Catalog {
        ecs_version: smeltr::io::ecs_fields::ECS_VERSION,
        ocsf_version: schema::OCSF_VERSION,
        categories: (ocsf::categories().iter())
            .map(|&(uid, name, caption)| Category { uid, name, caption })
            .collect(),
        schemas: schema::builtins().iter().map(|s| SchemaOut::of(s)).collect(),
    }))
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
pub async fn object(Json(req): Json<ObjectRequest>) -> Json<Envelope<ObjectOut>> {
    let opened = resolve(req.id.as_deref(), req.schema.as_ref()).and_then(|s| {
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
    });
    Json(match opened {
        Ok(out) => Envelope::ok(out),
        Err(e) => Envelope::failed_one(e),
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
pub async fn search(Json(req): Json<SearchRequest>) -> Json<Envelope<Matches>> {
    let q = req.q.trim().to_lowercase();
    if q.is_empty() {
        return Json(Envelope::ok(Matches::default()));
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
    Json(Envelope::ok(Matches { matches }))
}

#[derive(serde::Deserialize)]
pub struct ExportQuery {
    id: String,
}

#[derive(Default, serde::Serialize)]
pub struct Exported {
    schema: Value,
}

/// A built-in schema as JSON Schema, which a bundle can declare as its own and a catalog can import.
pub async fn export(Query(q): Query<ExportQuery>) -> Json<Envelope<Exported>> {
    Json(match schema::builtin(&q.id) {
        Some(s) => Envelope::ok(Exported {
            schema: json_schema::write(&s),
        }),
        None => Envelope::failed_one(format!("no built-in schema has the id {:?}", q.id)),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn opened(req: Value) -> Envelope<ObjectOut> {
        let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
        rt.block_on(object(Json(serde_json::from_value(req).unwrap()))).0
    }

    #[test]
    fn a_schema_opens_an_object_at_a_time() {
        let root = opened(json!({ "id": "ocsf/authentication" }));
        assert!(root.ok, "{:?}", root.errors);
        let user = root.body.fields.iter().find(|f| f.name == "user").unwrap();
        assert_eq!((user.type_name.as_str(), user.written_as.as_str()), ("user", "struct"));
        let time = root.body.fields.iter().find(|f| f.name == "time").unwrap();
        assert_eq!((time.written_as.as_str(), time.required), ("i64", true));

        let inner = opened(json!({ "id": "ocsf/authentication", "at": user.object.unwrap() }));
        assert_eq!(inner.body.name, "user");
        assert!(inner.body.fields.iter().any(|f| f.name == "name"));

        let declared = opened(json!({ "schema": { "type": "object", "properties": { "a": { "type": "boolean" } } } }));
        assert_eq!(declared.body.fields[0].written_as, "bool");
        assert!(!opened(json!({ "schema": { "type": "string" } })).ok);
        assert!(!opened(json!({ "id": "ocsf/nothing" })).ok);
    }

    #[test]
    fn a_search_finds_a_schema_by_a_fields_path() {
        let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
        let req = json!({ "q": "LOGON_PROCESS", "schemas": [
            { "id": "mine", "schema": { "type": "object", "properties": { "logon_process": { "type": "string" } } } }
        ] });
        let found = rt
            .block_on(search(Json(serde_json::from_value(req).unwrap())))
            .0
            .body
            .matches;
        assert_eq!(found[0].id, "mine", "the bundle's own come first");
        let auth = found.iter().find(|m| m.id == "ocsf/authentication").unwrap();
        assert!(
            auth.fields.iter().any(|f| f.starts_with("logon_process")),
            "{:?}",
            auth.fields
        );
    }
}
