//! A schema from the Elastic Common Schema, as ECS generates it: `generated/csv/fields.csv` or
//! `generated/ecs/ecs_flat.yml`. Each ECS field is a field of the schema, typed as a pipeline writes it, and an ECS type
//! that is not the one its smeltr type maps to of itself says so in `elasticsearch`, so the schema's mapping is ECS's.
//! A multi-field, such as `message.text` beside `message`, is one way Elasticsearch indexes its field, so it is that
//! field's `fields` rather than a field of its own.

use serde_json::{Map, Value, json};

use super::csv;
use crate::config_file::SchemaFieldDto;
use crate::io::elastic_mapping::types_for;

/// The fields ECS requires. Its YAML says so of each; its CSV has no column for it.
const REQUIRED: [&str; 2] = ["@timestamp", "ecs.version"];

/// The fields `text` declares, ECS's CSV or its YAML, told apart by the CSV's header.
pub fn read(text: &str) -> Result<Vec<SchemaFieldDto>, String> {
    let fields = match text.trim_start_matches('\u{feff}').starts_with("ECS_Version,") {
        true => from_csv(text)?,
        false => from_yaml(text)?,
    };
    Ok(with_multi_fields(fields))
}

/// One ECS field as both files give it.
struct Ecs {
    name: String,
    ty: String,
    array: bool,
    required: bool,
    description: String,
    example: String,
    /// Settings ECS gives its mapping beside the type: `ignore_above`, `scaling_factor`, its multi-fields.
    settings: Map<String, Value>,
}

fn from_csv(text: &str) -> Result<Vec<Ecs>, String> {
    let mut rows = csv::rows(text)?.into_iter();
    let (_, header) = rows.next().ok_or("the file is empty")?;
    let at = |name: &str| {
        (header.iter().position(|h| h == name)).ok_or_else(|| format!("ECS's CSV names a `{name}` column"))
    };
    let (field, ty, normalization, example, description) = (
        at("Field")?,
        at("Type")?,
        at("Normalization")?,
        at("Example")?,
        at("Description")?,
    );
    rows.map(|(n, row)| {
        let cell = |i: usize| row.get(i).cloned().unwrap_or_default();
        let name = cell(field);
        if name.is_empty() || cell(ty).is_empty() {
            return Err(format!("row {n}: no `Field` or `Type`"));
        }
        Ok(Ecs {
            required: REQUIRED.contains(&name.as_str()),
            name,
            ty: cell(ty),
            array: cell(normalization).split(',').any(|n| n.trim() == "array"),
            description: cell(description),
            example: cell(example),
            settings: Map::new(),
        })
    })
    .collect()
}

fn from_yaml(text: &str) -> Result<Vec<Ecs>, String> {
    let docs = yaml_rust2::YamlLoader::load_from_str(text).map_err(|e| format!("not ECS's YAML or CSV: {e}"))?;
    let doc = docs.first().map(json_of).unwrap_or(Value::Null);
    let fields = doc
        .as_object()
        .ok_or("not ECS's YAML or CSV: ECS's YAML is a map of fields")?;
    fields
        .iter()
        .map(|(name, f)| {
            let text = |key: &str| f.get(key).and_then(Value::as_str).unwrap_or_default().to_string();
            let ty = text("type");
            if ty.is_empty() {
                return Err(format!("`{name}`: no `type`"));
            }
            let mut settings: Map<String, Value> = ["ignore_above", "scaling_factor"]
                .into_iter()
                .filter_map(|k| Some((k.to_string(), f.get(k)?.clone())))
                .collect();
            // The YAML names a field's multi-fields under it; the CSV gives each a row, folded in later.
            let multi = f.get("multi_fields").and_then(Value::as_array).into_iter().flatten();
            let multi: Map<String, Value> = multi
                .filter_map(|m| Some((m.get("name")?.as_str()?.to_string(), json!({ "type": m.get("type")? }))))
                .collect();
            if !multi.is_empty() {
                settings.insert("fields".to_string(), Value::Object(multi));
            }
            Ok(Ecs {
                name: name.clone(),
                ty,
                array: (f.get("normalize").and_then(Value::as_array)).is_some_and(|n| n.iter().any(|n| n == "array")),
                required: f.get("required") == Some(&Value::Bool(true)),
                description: text("description"),
                example: csv::example_of(f.get("example")),
                settings,
            })
        })
        .collect()
}

/// The schema's fields: each ECS field typed, and each multi-field, a field under one that holds no object, folded
/// into its field's `fields`.
fn with_multi_fields(ecs: Vec<Ecs>) -> Vec<SchemaFieldDto> {
    let mut out: Vec<SchemaFieldDto> = Vec::new();
    for f in ecs {
        let parent = f.name.rsplit_once('.').and_then(|(up, key)| {
            let i = out.iter().position(|o| o.field == up && !o.ty.contains("{...}"))?;
            Some((i, key))
        });
        if let Some((i, key)) = parent {
            let own = own(&out[i].ty);
            let mapping = out[i].elasticsearch.get_or_insert(own);
            let fields = mapping.entry("fields").or_insert_with(|| json!({}));
            fields[key] = json!({ "type": f.ty });
            continue;
        }
        out.push(field(f));
    }
    out
}

fn field(f: Ecs) -> SchemaFieldDto {
    let element = match f.ty.as_str() {
        "date" | "date_nanos" => "datetime",
        "long" | "integer" | "short" | "byte" | "unsigned_long" => "i64",
        "float" | "half_float" | "scaled_float" | "double" => "f64",
        "boolean" => "bool",
        "object" | "geo_point" => "{...}",
        "nested" => "array<{...}>",
        "flattened" => "variant",
        "binary" => "bytes",
        _ => "str",
    };
    let ty = match f.array && !element.starts_with("array") {
        true => format!("array<{element}>"),
        false => element.to_string(),
    };
    // What the smeltr type maps to of itself; a variant maps to nothing until a value comes.
    let maps_as = (ty != "variant").then(|| own(&ty)).and_then(|m| m.get("type").cloned());
    let elasticsearch = (maps_as.as_ref() != Some(&json!(f.ty)) || !f.settings.is_empty()).then(|| {
        let mut said = Map::from_iter([("type".to_string(), json!(f.ty))]);
        said.extend(f.settings);
        said
    });
    SchemaFieldDto {
        field: f.name,
        ty,
        required: f.required,
        description: f.description,
        example: f.example,
        elasticsearch,
    }
}

/// The mapping a field of the smeltr type `ty` has of itself, as its first Elasticsearch type.
fn own(ty: &str) -> Map<String, Value> {
    let first = crate::expr::parse::parse_type(ty)
        .ok()
        .and_then(|t| types_for(&t).first().copied())
        .unwrap_or("keyword");
    Map::from_iter([("type".to_string(), json!(first))])
}

/// A YAML document as JSON. A key that is not text is spelled as YAML wrote it.
pub(super) fn json_of(yaml: &yaml_rust2::Yaml) -> Value {
    use yaml_rust2::Yaml;
    match yaml {
        Yaml::Real(r) => r.parse::<f64>().map(|f| json!(f)).unwrap_or_else(|_| json!(r)),
        Yaml::Integer(i) => json!(i),
        Yaml::String(s) => json!(s),
        Yaml::Boolean(b) => json!(b),
        Yaml::Array(a) => Value::Array(a.iter().map(json_of).collect()),
        Yaml::Hash(h) => Value::Object(
            h.iter()
                .map(|(k, v)| {
                    let key = match json_of(k) {
                        Value::String(s) => s,
                        other => other.to_string(),
                    };
                    (key, json_of(v))
                })
                .collect(),
        ),
        Yaml::Alias(_) | Yaml::Null | Yaml::BadValue => Value::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CSV: &str = include_str!("../../tests/gui/fixtures/ecs-fields-excerpt.csv");
    const YAML: &str = include_str!("../../tests/fixtures/ecs-flat-excerpt.yml");

    fn get<'f>(fields: &'f [SchemaFieldDto], name: &str) -> &'f SchemaFieldDto {
        fields
            .iter()
            .find(|f| f.field == name)
            .unwrap_or_else(|| panic!("no {name}"))
    }

    fn mapped(v: Value) -> Option<Map<String, Value>> {
        v.as_object().cloned()
    }

    #[test]
    fn ecs_csv_reads_as_its_fields_typed() {
        let fields = read(CSV).unwrap();
        assert_eq!(fields.len(), 14);
        let at = get(&fields, "@timestamp");
        assert_eq!((at.ty.as_str(), at.required), ("datetime", true));
        assert_eq!(at.example, "2016-05-23T08:05:34.853Z");
        assert_eq!(get(&fields, "tags").ty, "array<str>");
        assert_eq!(get(&fields, "labels").ty, "{...}");
        assert_eq!(get(&fields, "client.as.number").ty, "i64");
        assert_eq!(get(&fields, "agent.id").elasticsearch, None);
        assert_eq!(
            get(&fields, "message").elasticsearch,
            mapped(json!({ "type": "match_only_text" }))
        );
        let messages = get(&fields, "gen_ai.input.messages");
        assert_eq!(messages.ty, "variant");
        assert_eq!(messages.elasticsearch, mapped(json!({ "type": "flattened" })));
        assert!(messages.example.contains("Weather in Paris?"));
    }

    #[test]
    fn ecs_yaml_reads_as_its_fields_with_their_settings_and_multi_fields() {
        let fields = read(YAML).unwrap();
        let at = get(&fields, "@timestamp");
        assert_eq!((at.ty.as_str(), at.required), ("datetime", true));
        assert!(get(&fields, "ecs.version").required);
        let id = get(&fields, "agent.id");
        assert_eq!(id.ty, "str");
        assert_eq!(id.example, "8a4f500d");
        assert_eq!(
            id.elasticsearch,
            mapped(json!({ "type": "keyword", "ignore_above": 1024 }))
        );
        assert_eq!(
            get(&fields, "user.full_name").elasticsearch,
            mapped(
                json!({ "type": "keyword", "ignore_above": 1024, "fields": { "text": { "type": "match_only_text" } } })
            )
        );
        assert!(!fields.iter().any(|f| f.field == "user.full_name.text"));
        assert_eq!(get(&fields, "tags").ty, "array<str>");
        assert_eq!(get(&fields, "client.ip").elasticsearch, mapped(json!({ "type": "ip" })));
    }

    #[test]
    fn a_csv_multi_field_is_its_fields_own() {
        let text = "ECS_Version,Indexed,Field_Set,Field,Type,Level,Normalization,Example,Description\n\
                    9.0.0,true,user,user.name,keyword,core,,albert,Short name or login of the user.\n\
                    9.0.0,true,user,user.name.text,match_only_text,core,,,Short name or login of the user.\n";
        let fields = read(text).unwrap();
        assert_eq!(fields.len(), 1);
        assert_eq!(
            fields[0].elasticsearch,
            mapped(json!({ "type": "keyword", "fields": { "text": { "type": "match_only_text" } } }))
        );
    }

    #[test]
    fn a_file_that_is_neither_says_so() {
        assert!(read("- a\n- b\n").unwrap_err().starts_with("not ECS's YAML or CSV"));
        assert!(read("a: [\n").unwrap_err().starts_with("not ECS's YAML or CSV"));
        assert_eq!(read("a:\n  level: core\n").unwrap_err(), "`a`: no `type`");
    }
}
