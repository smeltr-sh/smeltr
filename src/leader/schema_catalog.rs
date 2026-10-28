//! The schema catalog: one object of a schema at a time, and a search over every schema's fields. The leader's
//! `/api/schemas/*` routes and the website demo's service answer from here. A bundle's schemas are config like any
//! other: read with `GET /api/config`, saved with `PUT /api/schemas`.

use serde_json::Value;

use crate::config_file::SchemaDto;
use crate::schema::{Type, json_schema};

/// A schema written as JSON Schema: one the bundle declares, or one an Add schema form checks before it saves.
#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ObjectRequest {
    #[serde(default)]
    id: String,
    schema: Value,
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

/// One object of a schema, its fields each with what holds it. A schema's objects may name each other, and
/// themselves, so a schema is opened an object at a time.
pub fn object(req: &ObjectRequest) -> Result<ObjectOut, String> {
    json_schema::read(&req.id, &req.schema).and_then(|s| {
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
                        Type::Object { at } => Some(at),
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
    /// The bundle's schemas.
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

/// How deep a search reads a schema's objects, which may name each other without end.
const SEARCH_DEPTH: usize = 3;
/// How many matching fields a match names.
const FIELDS_NAMED: usize = 5;

/// Every schema whose id, title or a field's path holds `q`, ignoring case: for someone who knows a field and not the
/// schema it is in.
pub fn search(req: &SearchRequest) -> Matches {
    let q = req.q.trim().to_lowercase();
    if q.is_empty() {
        return Matches::default();
    }
    let matches = (req.schemas.iter())
        .filter_map(|d| json_schema::read(&d.id, &d.schema).ok())
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

    fn login() -> Value {
        json!({
            "type": "object",
            "required": ["time"],
            "properties": {
                "time": { "type": "integer", "format": "epoch-millis" },
                "user": { "$ref": "#/$defs/user" },
                "logon_process": { "type": "string" },
            },
            "$defs": { "user": { "type": "object", "properties": { "name": { "type": "string" } } } },
        })
    }

    #[test]
    fn a_schema_opens_an_object_at_a_time() {
        let root = opened(json!({ "schema": login() })).unwrap();
        let user = root.fields.iter().find(|f| f.name == "user").unwrap();
        assert_eq!((user.type_name.as_str(), user.written_as.as_str()), ("user", "struct"));
        let time = root.fields.iter().find(|f| f.name == "time").unwrap();
        assert_eq!((time.written_as.as_str(), time.required), ("i64", true));

        let inner = opened(json!({ "schema": login(), "at": user.object.unwrap() })).unwrap();
        assert_eq!(inner.name, "user");
        assert!(inner.fields.iter().any(|f| f.name == "name"));

        assert!(opened(json!({ "schema": { "type": "string" } })).is_err());
        assert!(opened(json!({ "schema": login(), "at": 99 })).is_err());
    }

    #[test]
    fn a_search_finds_a_schema_by_a_fields_path() {
        let req = json!({ "q": "LOGON_PROCESS", "schemas": [
            { "id": "other", "schema": { "type": "object", "properties": { "a": { "type": "string" } } } },
            { "id": "login", "schema": login() },
        ] });
        let found = search(&serde_json::from_value(req).unwrap()).matches;
        assert_eq!(found.len(), 1);
        assert_eq!(
            (found[0].id.as_str(), found[0].fields.as_slice()),
            ("login", ["logon_process".to_string()].as_slice())
        );
    }
}
