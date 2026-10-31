//! The offline fuzzer of a schema's Elasticsearch export. A schema of fields nested to three keys deep, of every type,
//! arrays and objects among them and keys only their children name, says how some of its fields map: each with a
//! type its own allows, some with settings beside it, and at times one with a type its own cannot hold or none. A
//! schema with such a field is refused, naming it. Any other exports with each field's mapping where the field sits:
//! every setting it says as it says it, the settings its type's own mapping has where it keeps that type, and an
//! object's properties where it stays an object. A field under an object that maps as no object is left out, and a
//! field that says nothing maps as its type would.

use serde_json::{Map, Value, json};

use super::*;
use crate::config_file::SchemaFieldDto;
use crate::fuzz::{self, Coverage, Rng};
use crate::io::elastic_mapping::types_for;

/// The types a field holds of itself, as a schema spells them.
const SCALARS: [&str; 8] = ["str", "i64", "f64", "bool", "datetime", "duration", "bytes", "variant"];

/// Settings a mapping may carry beside its type, which the export passes on as they are.
fn settings() -> [(&'static str, Value); 5] {
    [
        ("ignore_above", json!(1024)),
        ("index", json!(false)),
        ("doc_values", json!(true)),
        ("scaling_factor", json!(100)),
        ("null_value", json!("NA")),
    ]
}

/// Types a field of some type cannot hold: whatever it is, one of these is not among its own.
const STRANGERS: [&str; 4] = ["ip", "date_nanos", "nested", "boolean"];

#[test]
fn a_schema_exports_each_field_as_it_says_it_maps() {
    let every_type: std::collections::BTreeSet<&str> = SCALARS
        .iter()
        .chain(["{...}"].iter())
        .flat_map(|t| types_for(&crate::expr::parse::parse_type(t).unwrap()).iter().copied())
        .collect();
    let mut cov = Coverage::new(
        [
            "an array",
            "an array of objects",
            "a key only its children name",
            "settings beside the type",
            "the same type keeps its own settings",
            "another type sheds its own settings",
            "an object mapped as an object keeps its properties",
            "an object mapped as no object sheds its properties",
            "a field under an object mapped as no object is left out",
            "a field that says nothing maps as its type",
            "a type its field cannot hold is refused",
            "a mapping that names no type is refused",
        ]
        .into_iter()
        .map(String::from)
        .chain(SCALARS.iter().map(|t| format!("a {t} mapped")))
        .chain(every_type.iter().map(|t| format!("as {t}"))),
    );
    fuzz::run(1..501, |seed| {
        let mut rng = Rng::seeded(seed);
        let mut rows = Vec::new();
        draw(&mut rng, "", 0, &mut rows, &mut cov);
        for row in &mut rows {
            if rng.chance(2) {
                row.elasticsearch = said(&mut rng, row, &mut cov);
            }
        }
        let refused = rng.chance(6).then(|| refuse(&mut rng, &mut rows, &mut cov)).flatten();
        let schema = SchemaDto {
            id: "fuzzed".into(),
            title: "fuzzed".into(),
            description: String::new(),
            fields: rows,
        };
        let export = |schema: &SchemaDto| {
            super::export(&ExportRequest {
                schema: schema.clone(),
                format: ExportFormat::Elasticsearch,
            })
            .map(|e| e.document)
        };
        let doc = export(&schema);
        if let Some(path) = refused {
            let refusal = doc.err().ok_or_else(|| format!("`{path}`'s mapping was taken"))?;
            return match refusal.starts_with(&format!("`{path}`: ")) {
                true => Ok(()),
                false => Err(format!("the refusal names another field: {refusal}")),
            };
        }
        let doc = doc?;
        let back = crate::schema::to_dto(&crate::schema::read(&schema)?);
        if back.fields != schema.fields {
            return Err("the schema does not read back as it was declared".into());
        }
        let mut plain = schema.clone();
        plain.fields.iter_mut().for_each(|f| f.elasticsearch = None);
        let own = export(&plain)?;
        for row in &schema.fields {
            check(row, &schema.fields, &doc, &own, &mut cov).map_err(|e| format!("`{}`: {e}", row.field))?;
        }
        Ok(())
    });
    if !fuzz::replaying() {
        cov.assert_complete();
    }
}

/// Fields under `prefix`, each row before the fields under it.
fn draw(rng: &mut Rng, prefix: &str, depth: usize, rows: &mut Vec<SchemaFieldDto>, cov: &mut Coverage) {
    for i in 0..1 + rng.below(4) {
        let field = match prefix {
            "" => format!("k{i}"),
            _ => format!("{prefix}.k{i}"),
        };
        let row = |ty: String| SchemaFieldDto {
            field: field.clone(),
            ty,
            ..Default::default()
        };
        match rng.below(10) {
            0..3 if depth < 2 => {
                let ty = match rng.chance(3) {
                    true => {
                        cov.reach("an array of objects");
                        "array<{...}>"
                    }
                    false => "{...}",
                };
                rows.push(row(ty.into()));
                draw(rng, &field, depth + 1, rows, cov);
            }
            3 if depth < 2 => {
                cov.reach("a key only its children name");
                draw(rng, &field, depth + 1, rows, cov);
            }
            _ => {
                let scalar = rng.pick(&SCALARS);
                let ty = match rng.chance(4) {
                    true => {
                        cov.reach("an array");
                        format!("array<{scalar}>")
                    }
                    false => scalar.to_string(),
                };
                rows.push(row(ty));
            }
        }
    }
}

/// A mapping `row` may say: a type its own allows, and at times settings beside it.
fn said(rng: &mut Rng, row: &SchemaFieldDto, cov: &mut Coverage) -> Option<Map<String, Value>> {
    let ty = crate::expr::parse::parse_type(&row.ty).unwrap();
    let to = *rng.pick(types_for(&ty));
    let scalar = row.ty.trim_start_matches("array<").trim_end_matches('>');
    if SCALARS.contains(&scalar) {
        cov.reach(format!("a {scalar} mapped"));
    }
    cov.reach(format!("as {to}"));
    let mut said = Map::from_iter([("type".to_string(), json!(to))]);
    for (key, value) in settings() {
        if rng.chance(4) {
            cov.reach("settings beside the type");
            said.insert(key.to_string(), value);
        }
    }
    Some(said)
}

/// Makes one field's mapping one its schema refuses, and gives its path.
fn refuse(rng: &mut Rng, rows: &mut [SchemaFieldDto], cov: &mut Coverage) -> Option<String> {
    let row = &mut rows[rng.index(rows.len())?];
    let ty = crate::expr::parse::parse_type(&row.ty).unwrap();
    let said = match rng.chance(3) {
        true => {
            cov.reach("a mapping that names no type is refused");
            json!({ "index": false })
        }
        false => {
            cov.reach("a type its field cannot hold is refused");
            let strangers: Vec<_> = STRANGERS.iter().filter(|t| !types_for(&ty).contains(t)).collect();
            json!({ "type": rng.pick(&strangers) })
        }
    };
    row.elasticsearch = said.as_object().cloned();
    Some(row.field.clone())
}

/// The mapping at `path` in an export, where it has one. An array adds no level: its elements map where it sits.
fn at<'d>(doc: &'d Value, path: &str) -> Option<&'d Value> {
    path.split('.')
        .try_fold(&doc["mappings"], |m, key| m.get("properties")?.get(key))
}

/// `row` as `doc` maps it, against `own`, the export of the same schema with no field saying how it maps.
fn check(
    row: &SchemaFieldDto,
    rows: &[SchemaFieldDto],
    doc: &Value,
    own: &Value,
    cov: &mut Coverage,
) -> Result<(), String> {
    let got = at(doc, &row.field);
    // An object above that maps as no object holds no properties, so nothing under it maps.
    let mut above = row.field.as_str();
    while let Some((up, _)) = above.rsplit_once('.') {
        above = up;
        let shed = rows.iter().any(|r| {
            r.field == above
                && r.elasticsearch
                    .as_ref()
                    .is_some_and(|m| !matches!(m["type"].as_str(), Some("object" | "nested")))
        });
        if shed {
            cov.reach("a field under an object mapped as no object is left out");
            return match got {
                None => Ok(()),
                Some(m) => Err(format!("is mapped as {m} under an object mapped as no object")),
            };
        }
    }
    let own = at(own, &row.field).and_then(Value::as_object);
    let Some(said) = &row.elasticsearch else {
        // An object's properties are its children's, which may say how they map.
        let strip = |m: Option<&Map<String, Value>>| {
            m.map(|m| {
                (m.iter())
                    .filter(|(k, _)| *k != "properties")
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect::<Map<_, _>>()
            })
        };
        cov.reach("a field that says nothing maps as its type");
        return match strip(got.and_then(Value::as_object)) == strip(own) {
            true => Ok(()),
            false => Err(format!("maps as {got:?}, not as its type's {own:?}")),
        };
    };
    let got = got.and_then(Value::as_object).ok_or("is not mapped")?;
    for (key, value) in said {
        if got.get(key) != Some(value) {
            return Err(format!("says {key} is {value}, and the export has {:?}", got.get(key)));
        }
    }
    let says = said["type"].as_str();
    // An object's own mapping names no type; a field whose type only the row knows has none.
    let was = own.map(|m| m.get("type").and_then(Value::as_str).unwrap_or("object"));
    if says == was {
        let own = own.expect("its own mapping");
        if own.keys().any(|k| k != "properties" && !said.contains_key(k)) {
            cov.reach("the same type keeps its own settings");
        }
        for (key, value) in own {
            if !said.contains_key(key) && key != "properties" && got.get(key) != Some(value) {
                return Err(format!("keeps {says:?} and drops its own {key}"));
            }
        }
        if own.contains_key("properties") {
            cov.reach("an object mapped as an object keeps its properties");
        }
        return Ok(());
    }
    let object = matches!(says, Some("object" | "nested"));
    if own.is_some_and(|m| m.keys().any(|k| k != "properties")) {
        cov.reach("another type sheds its own settings");
    }
    if own.is_some_and(|m| m.contains_key("properties")) {
        cov.reach(match object {
            true => "an object mapped as an object keeps its properties",
            false => "an object mapped as no object sheds its properties",
        });
    }
    match got
        .keys()
        .find(|k| !said.contains_key(*k) && !(object && *k == "properties"))
    {
        Some(k) => Err(format!("maps as {says:?} and keeps {k} of its own type's")),
        None => Ok(()),
    }
}
