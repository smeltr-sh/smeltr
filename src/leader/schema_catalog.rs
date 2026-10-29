//! A schema brought in as JSON Schema or by an importer pipeline, and handed out as JSON Schema. The leader's
//! `/api/preview/from-json-schema`, `/api/preview/to-json-schema` and `/api/areas/:area/preview/schema-import`, and
//! the website demo's service, answer from here. A bundle's schemas are config like any other: read with
//! `GET /api/areas/:area`, saved with `PUT /api/areas/:area/schemas`.

use serde_json::Value;

use crate::config_file::SchemaDto;
use crate::io::line_breaker::{LineBreaker, PER_LINE};
use crate::io::raw_object;
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

/// An uploaded file as the records an importer runs over, cut as a source cuts its bytes: where `line_breaker` breaks
/// them, or a record per line without one. A file such as ECS's `fields.csv`, whose quoted cells hold line ends, names
/// how each of its records starts. `Err` names a pattern a source would refuse.
pub fn records_of(text: &str, line_breaker: Option<&str>) -> Result<Vec<crate::io::RecordObject>, String> {
    let pattern = line_breaker.unwrap_or(PER_LINE);
    if let Some(refusal) = crate::config_file::line_breaker_refusal("the import", pattern) {
        return Err(refusal);
    }
    let breaker = LineBreaker(regex::bytes::Regex::new(pattern).expect("line_breaker_refusal compiled it"));
    let bytes = text.as_bytes();
    Ok(breaker
        .events(bytes, true)
        .map(|event| raw_object(String::from_utf8_lossy(&bytes[event])))
        .collect())
}

/// The schema `id` an importer pipeline produced: each row it wrote has a `field`, a `type`, and may have `required`
/// and `description`, as a schema's table does. `rows` is the pipeline's output (`preview::RowOut`), in which a row
/// without those, or with them wrong, or with an exception, is refused by its number.
pub fn from_rows(id: &str, title: &str, rows: &[crate::leader::preview::RowOut]) -> Result<ReadSchema, String> {
    let mut fields = Vec::new();
    for row in rows {
        // A row a step raised an exception on may have lost a field or written a wrong one, so it is refused rather
        // than read or passed over. A row with none that is gone was dropped on purpose, as a header is.
        if let Some(exception) = row.exceptions.first() {
            return Err(format!("row {}: {}", row.row, exception.message));
        }
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
    fn an_uploaded_file_is_a_record_per_line_without_a_line_breaker() {
        let records = records_of("a,1\r\n\nb,2\n", None).unwrap();
        let raws: Vec<_> = records.iter().map(|r| r["_raw"].as_str().unwrap()).collect();
        assert_eq!(raws, ["a,1", "b,2"]);
    }

    /// A quoted cell that spans lines stays in its row when the pattern names how a row starts.
    #[test]
    fn a_line_breaker_keeps_a_quoted_cell_s_line_ends_in_its_record() {
        let text = "9.0.0,a,\"{\n  x\n}\",d\n9.0.0,b,,e\n";
        let records = records_of(text, Some(r"([\r\n]+)\d+\.\d+\.\d+")).unwrap();
        let raws: Vec<_> = records.iter().map(|r| r["_raw"].as_str().unwrap()).collect();
        assert_eq!(raws, ["9.0.0,a,\"{\n  x\n}\",d", "9.0.0,b,,e"]);
    }

    #[test]
    fn a_line_breaker_a_source_would_refuse_is_refused() {
        let err = records_of("a", Some("no group")).unwrap_err();
        assert!(err.contains("no capture group"), "{err}");
    }

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

    /// A row dropped on purpose is passed over; one an exception dropped or marked is refused by its number.
    #[test]
    fn an_importer_row_with_an_exception_is_refused_by_its_number() {
        use crate::leader::preview::{ExceptionOut, RowOut};
        let row = |n: u32, json: Option<Value>, exceptions: Vec<ExceptionOut>| RowOut {
            row: n,
            json,
            exceptions,
            steps: None,
            dropped_by: None,
        };
        let failed = || ExceptionOut {
            answer: "drop",
            message: "csv: unclosed quote".to_string(),
            step: Some("row".to_string()),
        };
        let field = || Some(json!({ "field": "a", "type": "str" }));
        let header = row(0, None, Vec::new());
        assert_eq!(
            from_rows("x", "", &[header, row(1, field(), Vec::new())])
                .unwrap()
                .schema
                .fields
                .len(),
            1
        );
        for broken in [row(2, None, vec![failed()]), row(2, field(), vec![failed()])] {
            assert_eq!(from_rows("x", "", &[broken]).unwrap_err(), "row 2: csv: unclosed quote");
        }
    }
}
