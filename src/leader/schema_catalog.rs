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
#[derive(Debug, Default, serde::Serialize)]
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

/// The schema `id` an importer pipeline produced: each row it wrote has a `field`, a `type`, and may have `required`
/// and `description`, as a schema's table does. `rows` is the pipeline's output (`preview::RowOut`), in which a row
/// without those, or with them wrong, is refused by its number.
pub fn from_rows(id: &str, title: &str, rows: &[crate::leader::preview::RowOut]) -> Result<ReadSchema, String> {
    let mut fields = Vec::new();
    for row in rows {
        let Some(json) = row.json.as_ref().and_then(Value::as_object) else {
            continue;
        };
        let text = |key: &str| -> Result<Option<String>, String> {
            match json.get(key) {
                None | Some(Value::Null) => Ok(None),
                Some(Value::String(s)) => Ok(Some(s.clone())),
                Some(other) => Err(format!("row {}: `{key}` is {other}, not text", row.row)),
            }
        };
        let field = text("field")?.ok_or_else(|| format!("row {}: no `field`", row.row))?;
        let ty = text("type")?.ok_or_else(|| format!("row {}: no `type` for `{field}`", row.row))?;
        let required = match json.get("required") {
            None | Some(Value::Null) => false,
            Some(Value::Bool(b)) => *b,
            Some(other) => return Err(format!("row {}: `required` is {other}, not a bool", row.row)),
        };
        fields.push(crate::config_file::SchemaFieldDto {
            field,
            ty,
            required,
            description: text("description")?.unwrap_or_default(),
        });
    }
    if fields.is_empty() {
        return Err("the importer wrote no rows".to_string());
    }
    let schema = SchemaDto {
        id: id.to_string(),
        title: title.to_string(),
        description: String::new(),
        fields,
    };
    crate::schema::read(&schema)?;
    Ok(ReadSchema { schema })
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

    #[test]
    fn an_importers_rows_are_a_schema_and_a_row_lacking_its_field_or_type_is_refused() {
        use crate::leader::preview::RowOut;
        let row = |n: u32, json: Value| RowOut {
            row: n,
            json: Some(json),
            exceptions: Vec::new(),
            steps: None,
            dropped_by: None,
        };
        let rows = [
            row(
                0,
                json!({ "field": "@timestamp", "type": "datetime", "required": true, "description": "When." }),
            ),
            row(1, json!({ "field": "source.ip", "type": "str" })),
            row(2, json!(null)),
        ];
        let schema = from_rows("ecs", "ECS", &rows).unwrap().schema;
        assert_eq!(schema.fields.len(), 2);
        assert!(schema.fields[0].required);
        assert_eq!(schema.fields[1].ty, "str");
        let refused = |rows: &[RowOut]| from_rows("x", "", rows).unwrap_err();
        assert_eq!(refused(&[row(3, json!({ "type": "str" }))]), "row 3: no `field`");
        assert_eq!(refused(&[row(4, json!({ "field": "a" }))]), "row 4: no `type` for `a`");
        assert_eq!(
            refused(&[row(5, json!({ "field": "a", "type": 1 }))]),
            "row 5: `type` is 1, not text"
        );
        assert!(refused(&[row(6, json!({ "field": "a", "type": "nope" }))]).starts_with("`a`: `nope` is no type"));
        assert_eq!(refused(&[]), "the importer wrote no rows");
    }
}
