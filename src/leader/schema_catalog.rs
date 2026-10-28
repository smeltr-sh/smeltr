//! A schema brought in as JSON Schema and handed out as it. The leader's `/api/schemas/*` routes and the website
//! demo's service answer from here. A bundle's schemas are config like any other: read with `GET /api/config`, saved
//! with `PUT /api/schemas`.

use serde_json::Value;

use crate::config_file::SchemaDto;
use crate::schema::json_schema;

/// A JSON Schema document to read as the schema `id`.
#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FromJsonSchema {
    #[serde(default)]
    pub id: String,
    pub schema: Value,
}

/// A schema read from JSON Schema, as a bundle declares one.
#[derive(Default, serde::Serialize)]
pub struct ReadSchema {
    pub schema: SchemaDto,
}

/// The schema `req` declares.
pub fn from_json_schema(req: &FromJsonSchema) -> Result<ReadSchema, String> {
    json_schema::read(&req.id, &req.schema).map(|s| ReadSchema {
        schema: crate::schema::to_dto(&s),
    })
}

/// A schema written as a JSON Schema document.
#[derive(Default, serde::Serialize)]
pub struct WrittenSchema {
    pub schema: Value,
}

/// `schema` as JSON Schema.
pub fn to_json_schema(schema: &SchemaDto) -> Result<WrittenSchema, String> {
    crate::schema::read(schema).map(|s| WrittenSchema {
        schema: json_schema::write(&s),
    })
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

    #[test]
    fn a_json_schema_comes_in_as_rows_and_goes_out_as_one() {
        let doc = json!({
            "type": "object",
            "required": ["time"],
            "properties": {
                "time": { "type": "integer" },
                "user": { "type": "object", "properties": { "name": { "type": "string" } } },
            },
        });
        let schema = from_json_schema(&FromJsonSchema {
            id: "login".to_string(),
            schema: doc,
        })
        .unwrap()
        .schema;
        let rows: Vec<&str> = schema.fields.iter().map(|f| f.field.as_str()).collect();
        assert_eq!(rows, ["time", "user", "user.name"]);
        assert!(schema.fields[0].required);
        let out = to_json_schema(&schema).unwrap().schema;
        assert_eq!(out["properties"]["user"]["properties"]["name"]["type"], "string");
        assert!(
            from_json_schema(&FromJsonSchema {
                id: String::new(),
                schema: json!({ "type": "string" })
            })
            .is_err()
        );
    }
}
