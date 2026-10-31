//! A schema brought in from a file a standard publishes (JSON Schema, ECS, OCSF, a CSV of fields) or by an importer
//! pipeline, and handed out in an export format. The leader's
//! `/api/preview/schema-read`, `/api/preview/schema-export` and `/api/areas/:area/preview/schema-import`, and
//! the website demo's service, answer from here. A bundle's schemas are config like any other: read with
//! `GET /api/areas/:area`, saved with `PUT /api/areas/:area/schemas`.

use serde_json::Value;

use crate::config_file::SchemaDto;
use crate::io::line_breaker::{LineBreaker, PER_LINE};
use crate::io::raw_object;
use crate::schema::json_schema;

/// What a schema is read from: a file in a format a standard publishes.
#[derive(Clone, Copy, Debug, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ReadFormat {
    /// A JSON Schema document (`schema::json_schema`).
    JsonSchema,
    /// ECS's `fields.csv` or `ecs_flat.yml` (`schema::ecs`).
    Ecs,
    /// One class of `ocsf-schema-compiler`'s output (`schema::ocsf`).
    Ocsf,
    /// A spreadsheet of fields under a header row (`schema::csv`).
    Csv,
}

/// A file to read as the schema `id`.
#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReadRequest {
    #[serde(default)]
    pub id: String,
    pub format: ReadFormat,
    /// The file, as text.
    pub text: String,
    /// The OCSF class to read.
    #[serde(default)]
    pub class: String,
    /// How much of an OCSF class to read.
    #[serde(default)]
    pub cut: crate::schema::ocsf::Cut,
    /// Which column of a CSV holds what; unsaid, its header says.
    #[serde(default)]
    pub columns: Option<crate::schema::csv::Columns>,
    /// The type of each value of a CSV's type column the reader does not know of itself.
    #[serde(default)]
    pub types: std::collections::BTreeMap<String, String>,
    /// Answer what a CSV holds, for its columns and types to be chosen, rather than read it.
    #[serde(default)]
    pub preview: bool,
}

/// A schema read from a file, as a bundle declares one, or, asked for, what a CSV holds before it is read.
#[derive(Debug, Default, serde::Serialize)]
pub struct ReadSchema {
    pub schema: SchemaDto,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub preview: Option<crate::schema::csv::Preview>,
}

/// The schema `req`'s file declares, checked as a bundle's would be.
pub fn read(req: &ReadRequest) -> Result<ReadSchema, String> {
    let json = || serde_json::from_str::<Value>(&req.text).map_err(|e| format!("the file is not JSON: {e}"));
    let fields = match req.format {
        ReadFormat::JsonSchema => {
            let schema = json_schema::read(&req.id, &json()?)?;
            return Ok(ReadSchema {
                schema: crate::schema::to_dto(&schema),
                preview: None,
            });
        }
        ReadFormat::Ecs => crate::schema::ecs::read(&req.text)?,
        ReadFormat::Ocsf => crate::schema::ocsf::read(&json()?, &req.class, &req.cut)?,
        ReadFormat::Csv if req.preview => {
            return Ok(ReadSchema {
                preview: Some(crate::schema::csv::preview(&req.text, req.columns.as_ref())?),
                ..ReadSchema::default()
            });
        }
        ReadFormat::Csv => crate::schema::csv::read(&req.text, req.columns.as_ref(), &req.types)?,
    };
    declared(&req.id, "", fields)
}

/// The schema `id` of `fields`, checked as a bundle's would be.
fn declared(id: &str, title: &str, fields: Vec<crate::config_file::SchemaFieldDto>) -> Result<ReadSchema, String> {
    let schema = SchemaDto {
        id: id.to_string(),
        title: title.to_string(),
        description: String::new(),
        fields,
    };
    crate::schema::read(&schema)?;
    Ok(ReadSchema { schema, preview: None })
}

/// What a schema is exported as.
#[derive(Clone, Copy, Debug, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ExportFormat {
    /// A JSON Schema document.
    JsonSchema,
    /// The body of `PUT /<index>` for an index whose documents fit it (`io::elastic_mapping`).
    Elasticsearch,
}

#[derive(serde::Deserialize)]
pub struct ExportRequest {
    pub schema: SchemaDto,
    pub format: ExportFormat,
}

/// A schema written in an export format.
#[derive(Default, serde::Serialize)]
pub struct ExportedSchema {
    pub document: Value,
}

/// `req`'s schema, written as its format has it.
pub fn export(req: &ExportRequest) -> Result<ExportedSchema, String> {
    let schema = crate::schema::read(&req.schema)?;
    let document = match req.format {
        ExportFormat::JsonSchema => json_schema::write(&schema),
        ExportFormat::Elasticsearch => {
            let mapped = (schema.fields.iter())
                .filter_map(|f| Some((f.keys(), f.elasticsearch.as_ref()?)))
                .collect();
            crate::io::elastic_mapping::mapping(&schema.record_type(), &mapped)
        }
    };
    Ok(ExportedSchema { document })
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
        let elasticsearch = match json.get("elasticsearch") {
            None | Some(Value::Null) => None,
            Some(Value::Object(said)) => Some(said.clone()),
            Some(other) => return Err(format!("row {}: `elasticsearch` is {other}, not a mapping", row.row)),
        };
        fields.push(crate::config_file::SchemaFieldDto {
            field,
            ty,
            required,
            description: text("description")?.unwrap_or_default(),
            example: text("example")?.unwrap_or_default(),
            elasticsearch,
        });
    }
    if fields.is_empty() {
        return Err("the importer wrote no rows".to_string());
    }
    declared(id, title, fields)
}

/// What breaks a schema in `file`, a group's bundle merged with `shared/`: each pipeline's rows against the schemas it
/// produces, and each destination's accepts against what reaches it. With `base`, only what `file` adds to what breaks
/// there, field by field; a `base` that does not load takes nothing away. `Err` when `file` does not load.
pub fn findings(
    file: crate::config_file::FileConfig,
    declared_at: &crate::config_file::DeclaredAt,
    base: Option<(crate::config_file::FileConfig, crate::config_file::DeclaredAt)>,
) -> Result<Vec<crate::config_file::SchemaFinding>, String> {
    let base = base.and_then(|(file, declared_at)| crate::config_file::validate_at(file, &declared_at).ok());
    let (misfits, was) = match base
        .as_ref()
        .and_then(|system| Some((system, system.compiled.as_ref()?)))
    {
        Some((system, compiled)) => (system.schema_misfits(compiled), system.schema_findings(compiled)),
        None => Default::default(),
    };
    let system = crate::config_file::validate_at(file, declared_at).map_err(|e| e.to_string())?;
    let compiled = system.compiled.as_ref().ok_or("the bundle does not compile")?;
    Ok((system.schema_findings_beyond(compiled, &misfits).into_iter())
        .filter(|f| !was.contains(f))
        .collect())
}

#[cfg(test)]
mod fuzz;

#[cfg(test)]
mod fuzz_read;

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn json_schema(id: &str, doc: Value) -> ReadRequest {
        ReadRequest {
            id: id.to_string(),
            format: ReadFormat::JsonSchema,
            text: doc.to_string(),
            class: String::new(),
            cut: Default::default(),
            columns: None,
            types: Default::default(),
            preview: false,
        }
    }

    #[test]
    fn a_file_reads_in_the_format_it_is_named_as_and_is_checked_as_a_bundles_schema() {
        let req = |format, text: &str| ReadRequest {
            id: "x".to_string(),
            format,
            text: text.to_string(),
            class: "authentication".to_string(),
            cut: Default::default(),
            columns: None,
            types: Default::default(),
            preview: false,
        };
        let fields = |r: Result<ReadSchema, String>| r.unwrap().schema.fields.len();
        assert_eq!(fields(read(&req(ReadFormat::Csv, "field,type\na,str\nb,i64"))), 2);
        assert_eq!(
            fields(read(&req(
                ReadFormat::Ecs,
                include_str!("../../tests/gui/fixtures/ecs-fields-excerpt.csv")
            ))),
            14
        );
        assert!(
            fields(read(&req(
                ReadFormat::Ocsf,
                include_str!("../../tests/fixtures/ocsf-authentication.json")
            ))) > 30
        );
        assert_eq!(
            read(&req(ReadFormat::Ocsf, "nope")).unwrap_err(),
            "the file is not JSON: expected ident at line 1 column 2"
        );
        assert!(
            read(&req(ReadFormat::Csv, "field,type,elasticsearch\na,str,long"))
                .unwrap_err()
                .starts_with("`a`: Elasticsearch's `long` cannot hold str")
        );
        assert!(
            read(&req(ReadFormat::Csv, "field,type,elasticsearch\na,str,long"))
                .unwrap_err()
                .starts_with("`a`: Elasticsearch's `long` cannot hold str")
        );
    }

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
        let schema = read(&json_schema("login", doc)).unwrap().schema;
        let rows: Vec<&str> = schema.fields.iter().map(|f| f.field.as_str()).collect();
        assert_eq!(rows, ["time", "user", "user.name"]);
        assert!(schema.fields[0].required);
        let export = |format| {
            export(&ExportRequest {
                schema: schema.clone(),
                format,
            })
            .unwrap()
            .document
        };
        let out = export(ExportFormat::JsonSchema);
        assert_eq!(out["properties"]["user"]["properties"]["name"]["type"], "string");
        let mapping = export(ExportFormat::Elasticsearch);
        assert_eq!(
            mapping["mappings"]["properties"]["user"]["properties"]["name"]["type"],
            "keyword"
        );
        // A field's own mapping is laid over its type's, and JSON Schema has no place for it.
        let mut schema = schema;
        schema.fields[2].elasticsearch = json!({ "type": "wildcard" }).as_object().cloned();
        let request = |format| ExportRequest {
            schema: schema.clone(),
            format,
        };
        let mapping = super::export(&request(ExportFormat::Elasticsearch)).unwrap().document;
        assert_eq!(
            mapping["mappings"]["properties"]["user"]["properties"]["name"],
            json!({ "type": "wildcard" })
        );
        let out = super::export(&request(ExportFormat::JsonSchema)).unwrap().document;
        assert_eq!(
            out["properties"]["user"]["properties"]["name"],
            json!({ "type": "string" })
        );
        assert!(read(&json_schema("", json!({ "type": "string" }))).is_err());
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
            row(
                1,
                json!({ "field": "source.ip", "type": "str", "elasticsearch": { "type": "ip" } }),
            ),
            row(2, json!(null)),
        ];
        let schema = from_rows("ecs", "ECS", &rows).unwrap().schema;
        assert_eq!(schema.fields.len(), 2);
        assert!(schema.fields[0].required);
        assert_eq!(schema.fields[1].ty, "str");
        assert_eq!(
            schema.fields[1].elasticsearch,
            json!({ "type": "ip" }).as_object().cloned()
        );
        let refused = |rows: &[RowOut]| from_rows("x", "", rows).unwrap_err();
        assert_eq!(refused(&[row(3, json!({ "type": "str" }))]), "row 3: no `field`");
        assert_eq!(refused(&[row(4, json!({ "field": "a" }))]), "row 4: no `type` for `a`");
        assert_eq!(
            refused(&[row(5, json!({ "field": "a", "type": 1 }))]),
            "row 5: `type` is 1, not text"
        );
        assert!(refused(&[row(6, json!({ "field": "a", "type": "nope" }))]).starts_with("`a`: `nope` is no type"));
        assert_eq!(
            refused(&[row(7, json!({ "field": "a", "type": "str", "elasticsearch": "ip" }))]),
            "row 7: `elasticsearch` is \"ip\", not a mapping"
        );
        assert!(
            refused(&[row(
                8,
                json!({ "field": "a", "type": "str", "elasticsearch": { "type": "long" } })
            )])
            .starts_with("`a`: Elasticsearch's `long` cannot hold str")
        );
        assert_eq!(refused(&[]), "the importer wrote no rows");
    }

    /// The importer of `docs/examples.md` 9 over a BigQuery table's schema: a row for each column.
    #[test]
    fn the_docs_importer_writes_a_row_for_each_column() {
        // A Windows checkout may hold the file with CRLF.
        let examples = include_str!("../../docs/examples.md").replace("\r\n", "\n");
        let section = examples.split("## 9.").nth(1).expect("the importer's section");
        let body = (section.split("```ts\n").nth(1).and_then(|b| b.split("\n```").next())).expect("its pipeline");
        let config = json!({
            "version": 1,
            "pipelines": [{ "id": "bq", "name": "BigQuery importer",
                            "steps": [{ "id": "rows", "type": "code", "body": body }] }],
        });
        let req: crate::leader::preview::PreviewRequest =
            serde_json::from_value(json!({ "config": config.to_string(), "pipeline": "bq" })).unwrap();
        let text = include_str!("../../tests/fixtures/bigquery-table.json");
        let records = records_of(text, Some(r"(\x00)")).unwrap();
        let preview = crate::leader::preview::run_preview(req, Default::default(), &[], records);
        assert!(preview.succeeded(), "{:?}", preview.errors);
        let schema = from_rows("web", "Web", &preview.body.rows).unwrap().schema;
        let field = |path: &str| {
            let f = (schema.fields.iter().find(|f| f.field == path)).unwrap_or_else(|| panic!("no `{path}`"));
            (f.ty.as_str(), f.required)
        };
        assert_eq!(field("event_time"), ("datetime", true));
        assert_eq!(field("status"), ("i64", false));
        assert_eq!(field("latency_ms"), ("f64", false));
        assert_eq!(field("cached"), ("bool", false));
        assert_eq!(field("tags"), ("array<str>", false));
        assert_eq!(field("headers"), ("{...}", false));
        assert_eq!(schema.fields.len(), 7);
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
