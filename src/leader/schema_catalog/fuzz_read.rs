//! The offline fuzzer of reading a file as a schema, by each format a reader takes but JSON Schema, which its export's
//! fuzzer reads back.
//!
//! ECS: a field set drawn as ECS draws one, of every ECS type, arrays, objects, keys only their children name and
//! multi-fields, written as both `fields.csv` and `ecs_flat.yml` with text that holds commas, quotes and line ends.
//! Both read as the same schema, the YAML adding the settings it says, and that schema's Elasticsearch mapping is
//! ECS's: each field as the type ECS gives it, each multi-field under its field's `fields`.
//!
//! A CSV of fields: a spreadsheet with its columns in any order and case, columns of its own, padded cells, blank
//! rows and either line end reads as the fields it lists, and one with a row it cannot read names that row.
//!
//! OCSF: classes of objects that hold each other, cycles among them, cut at every depth and set of requirements. A
//! field is taken for each attribute of a level taken, as deep as the cut, and none other; a cut or a file it cannot
//! read says so.

use serde_json::{Map, Value, json};

use super::*;
use crate::config_file::SchemaFieldDto;
use crate::fuzz::{self, Coverage, Rng};
use crate::io::elastic_mapping::types_for;
use crate::schema::ocsf::{Cut, DEEPEST};

/// Text a cell may hold, each piece one a CSV or YAML writer has to take care over.
const TEXT: [&str; 8] = [
    "plain",
    "a, comma",
    "say \"hi\"",
    "two\nlines",
    "crlf\r\nlines",
    "colon: and # hash",
    "'single'",
    "ünïcödé",
];

fn text(rng: &mut Rng) -> String {
    (0..1 + rng.below(3))
        .map(|_| *rng.pick(&TEXT))
        .collect::<Vec<_>>()
        .join(" ")
}

/// `cell` as a CSV writes it: quoted where it holds a comma, a quote or a line end.
fn quoted(cell: &str) -> String {
    match cell.contains([',', '"', '\n', '\r']) {
        true => format!("\"{}\"", cell.replace('"', "\"\"")),
        false => cell.to_string(),
    }
}

fn read_as(format: ReadFormat, text: String) -> Result<Vec<SchemaFieldDto>, String> {
    read(&ReadRequest {
        id: "fuzzed".into(),
        format,
        text,
        class: String::new(),
        cut: Cut::default(),
    })
    .map(|r| r.schema.fields)
}

/// The mapping at `path` in an Elasticsearch export.
fn at<'d>(doc: &'d Value, path: &str) -> Option<&'d Map<String, Value>> {
    path.split('.')
        .try_fold(&doc["mappings"], |m, key| m.get("properties")?.get(key))?
        .as_object()
}

/// The ECS types, each with what a pipeline writes for it.
const ECS_TYPES: [&str; 22] = [
    "keyword",
    "wildcard",
    "constant_keyword",
    "text",
    "match_only_text",
    "ip",
    "version",
    "date",
    "date_nanos",
    "long",
    "integer",
    "short",
    "byte",
    "unsigned_long",
    "float",
    "half_float",
    "scaled_float",
    "double",
    "boolean",
    "binary",
    "geo_point",
    "flattened",
];

/// The ECS types a multi-field is indexed as.
const MULTI: [(&str, &str); 3] = [
    ("text", "match_only_text"),
    ("caseless", "keyword"),
    ("raw", "wildcard"),
];

/// One ECS field as a generator draws it, with what ECS's two files say of it.
struct Ecs {
    name: String,
    ty: &'static str,
    array: bool,
    description: String,
    example: String,
    /// The example is a number, which the YAML writes bare.
    numeric: bool,
    ignore_above: Option<u64>,
    /// Its multi-fields, by key: each its own row in the CSV, under the field in the YAML.
    multi: Vec<(&'static str, &'static str)>,
}

/// Fields under `prefix`, as ECS names them: an object or a key only its children name holds more.
fn draw_ecs(rng: &mut Rng, prefix: &str, depth: usize, out: &mut Vec<Ecs>, cov: &mut Coverage) {
    for i in 0..1 + rng.below(4) {
        let name = match prefix {
            "" => format!("k{i}"),
            _ => format!("{prefix}.k{i}"),
        };
        let field = |ty: &'static str, rng: &mut Rng| Ecs {
            name: name.clone(),
            ty,
            array: false,
            description: text(rng),
            example: String::new(),
            numeric: false,
            ignore_above: None,
            multi: Vec::new(),
        };
        match rng.below(10) {
            0..2 if depth < 2 => {
                let ty = match rng.chance(3) {
                    true => "nested",
                    false => "object",
                };
                cov.reach(format!("ecs {ty}"));
                out.push(field(ty, rng));
                draw_ecs(rng, &name, depth + 1, out, cov);
            }
            2 if depth < 2 => {
                cov.reach("ecs a key only its children name");
                draw_ecs(rng, &name, depth + 1, out, cov);
            }
            _ => {
                let ty = *rng.pick(&ECS_TYPES);
                cov.reach(format!("ecs {ty}"));
                let mut f = field(ty, rng);
                f.array = rng.chance(4);
                if rng.chance(2) {
                    f.numeric = matches!(ty, "long" | "integer" | "short" | "byte") && rng.chance(2);
                    f.example = match f.numeric {
                        true => (rng.below(10_000)).to_string(),
                        false => text(rng),
                    };
                }
                if matches!(ty, "keyword" | "wildcard") && rng.chance(2) {
                    f.ignore_above = Some(1024);
                }
                if matches!(ty, "keyword" | "text" | "wildcard") && rng.chance(3) {
                    cov.reach("ecs a multi-field");
                    let (key, multi) = *rng.pick(&MULTI);
                    f.multi.push((key, multi));
                }
                out.push(f);
            }
        }
    }
}

/// `fields` as ECS's `generated/csv/fields.csv`: one row per field and per multi-field, in name order.
fn ecs_csv(fields: &[Ecs]) -> String {
    let mut rows = vec![(
        String::new(),
        "ECS_Version,Indexed,Field_Set,Field,Type,Level,Normalization,Example,Description".to_string(),
    )];
    for f in fields {
        let row = |name: &str, ty: &str, normalization: &str, example: &str| {
            let set = f.name.split('.').next().unwrap_or_default();
            let cells = [
                "9.1.0",
                "true",
                set,
                name,
                ty,
                "core",
                normalization,
                example,
                &f.description,
            ];
            (name.to_string(), cells.map(quoted).join(","))
        };
        let normalization = match f.array {
            true => "array, to_lower",
            false => "",
        };
        rows.push(row(&f.name, f.ty, normalization, &f.example));
        for (key, multi) in &f.multi {
            rows.push(row(&format!("{}.{key}", f.name), multi, "", ""));
        }
    }
    rows[1..].sort_by(|a, b| a.0.cmp(&b.0));
    rows.into_iter().map(|(_, r)| r + "\n").collect()
}

/// `fields` as ECS's `generated/ecs/ecs_flat.yml`: a map of each field by name, its multi-fields under it.
fn ecs_yaml(fields: &[Ecs], required: &[&str]) -> String {
    let mut sorted: Vec<&Ecs> = fields.iter().collect();
    sorted.sort_by(|a, b| a.name.cmp(&b.name));
    let mut out = String::new();
    for f in sorted {
        let s = |t: &str| Value::String(t.to_string()).to_string();
        out += &format!("{}:\n", s(&f.name));
        out += &format!("  dashed_name: {}\n", s(&f.name.replace('.', "-")));
        out += &format!("  description: {}\n", s(&f.description));
        if !f.example.is_empty() {
            out += &format!(
                "  example: {}\n",
                if f.numeric { f.example.clone() } else { s(&f.example) }
            );
        }
        out += &format!("  flat_name: {}\n", s(&f.name));
        if let Some(n) = f.ignore_above {
            out += &format!("  ignore_above: {n}\n");
        }
        out += "  level: core\n";
        if !f.multi.is_empty() {
            out += "  multi_fields:\n";
            for (key, multi) in &f.multi {
                out += &format!("  - flat_name: {}.{key}\n    name: {key}\n    type: {multi}\n", f.name);
            }
        }
        out += &format!("  name: {}\n", s(f.name.rsplit('.').next().unwrap_or_default()));
        out += "  normalize:";
        out += match f.array {
            true => "\n  - array\n",
            false => " []\n",
        };
        if required.contains(&f.name.as_str()) {
            out += "  required: true\n";
        }
        out += &format!("  short: {}\n", s(&f.description));
        out += &format!("  type: {}\n", f.ty);
    }
    out
}

#[test]
fn ecs_reads_the_same_from_its_csv_and_its_yaml_and_maps_as_ecs_says() {
    let mut cov = Coverage::new(
        ECS_TYPES
            .iter()
            .chain(["object", "nested"].iter())
            .map(|t| format!("ecs {t}"))
            .chain(
                [
                    "ecs a key only its children name",
                    "ecs a multi-field",
                    "ecs a required field",
                    "ecs a setting the yaml says",
                ]
                .map(String::from),
            ),
    );
    fuzz::run(1..301, |seed| {
        let mut rng = Rng::seeded(seed);
        let mut fields = Vec::new();
        draw_ecs(&mut rng, "", 0, &mut fields, &mut cov);
        // ECS requires `@timestamp` and `ecs.version`; the CSV says so of neither, and the reader knows them.
        if rng.chance(2) {
            cov.reach("ecs a required field");
            fields.push(Ecs {
                name: "@timestamp".into(),
                ty: "date",
                array: false,
                description: text(&mut rng),
                example: "2016-05-23T08:05:34.853Z".into(),
                numeric: false,
                ignore_above: None,
                multi: Vec::new(),
            });
        }
        let from_csv = read_as(ReadFormat::Ecs, ecs_csv(&fields)).map_err(|e| format!("the CSV: {e}"))?;
        let from_yaml =
            read_as(ReadFormat::Ecs, ecs_yaml(&fields, &["@timestamp"])).map_err(|e| format!("the YAML: {e}"))?;
        if from_csv.len() != from_yaml.len() {
            return Err(format!(
                "{} fields from the CSV, {} from the YAML",
                from_csv.len(),
                from_yaml.len()
            ));
        }
        for (c, y) in from_csv.iter().zip(&from_yaml) {
            let mut y = y.clone();
            // The YAML says settings the CSV has no column for; without them both say the same.
            if let Some(m) = y.elasticsearch.as_mut()
                && m.remove("ignore_above").is_some()
            {
                cov.reach("ecs a setting the yaml says");
                if c.elasticsearch.is_none() && m.len() == 1 {
                    y.elasticsearch = None;
                }
            }
            if *c != y {
                return Err(format!("the CSV reads {c:?}, the YAML {y:?}"));
            }
        }
        let schema = SchemaDto {
            id: "fuzzed".into(),
            fields: from_yaml.clone(),
            ..Default::default()
        };
        let doc = export(&ExportRequest {
            schema,
            format: ExportFormat::Elasticsearch,
        })?
        .document;
        for f in &fields {
            let read = (from_yaml.iter().find(|r| r.field == f.name)).ok_or_else(|| format!("no `{}`", f.name))?;
            if read.required != (f.name == "@timestamp")
                || read.description != f.description
                || read.example != f.example
            {
                return Err(format!("`{}` reads as {read:?}", f.name));
            }
            if f.array != read.ty.starts_with("array<") && f.ty != "nested" {
                return Err(format!("`{}` is {}", f.name, read.ty));
            }
            let mapped = at(&doc, &f.name).ok_or_else(|| format!("`{}` is not mapped", f.name))?;
            let ty = mapped.get("type").and_then(Value::as_str).unwrap_or("object");
            if ty != f.ty {
                return Err(format!("`{}` is ECS's {}, and maps as {ty}", f.name, f.ty));
            }
            if f.ignore_above.is_some() && mapped.get("ignore_above") != Some(&json!(1024)) {
                return Err(format!("`{}` drops its ignore_above: {mapped:?}", f.name));
            }
            for (key, multi) in &f.multi {
                if mapped["fields"][key]["type"] != json!(multi) {
                    return Err(format!(
                        "`{}`'s multi-field `{key}` is not its {multi}: {mapped:?}",
                        f.name
                    ));
                }
            }
        }
        Ok(())
    });
    if !fuzz::replaying() {
        cov.assert_complete();
    }
}

/// The types a field of a CSV of fields holds of itself.
const SCALARS: [&str; 8] = ["str", "i64", "f64", "bool", "datetime", "duration", "bytes", "variant"];

/// Fields under `prefix`, each before those under it.
fn draw_fields(rng: &mut Rng, prefix: &str, depth: usize, out: &mut Vec<SchemaFieldDto>) {
    for i in 0..1 + rng.below(4) {
        let field = match prefix {
            "" => format!("k{i}"),
            _ => format!("{prefix}.k{i}"),
        };
        let object = depth < 2 && rng.chance(4);
        let ty = match object {
            true => "{...}".to_string(),
            false => match rng.chance(4) {
                true => format!("array<{}>", rng.pick(&SCALARS)),
                false => rng.pick(&SCALARS).to_string(),
            },
        };
        let mapped = types_for(&crate::expr::parse::parse_type(&ty).unwrap());
        out.push(SchemaFieldDto {
            field: field.clone(),
            required: rng.chance(3),
            description: if rng.chance(2) { text(rng) } else { String::new() },
            example: if rng.chance(2) { text(rng) } else { String::new() },
            elasticsearch: (rng.chance(3) && !mapped.is_empty())
                .then(|| Map::from_iter([("type".to_string(), json!(rng.pick(mapped)))])),
            ty,
        });
        if object {
            draw_fields(rng, &field, depth + 1, out);
        }
    }
}

/// The columns a CSV of fields may have, as its reader names them.
const COLUMNS: [&str; 6] = ["field", "type", "required", "description", "example", "elasticsearch"];

#[test]
fn a_csv_of_fields_reads_as_the_fields_it_lists_and_names_a_row_it_cannot() {
    let mut cov = Coverage::new(
        [
            "csv columns in another order",
            "csv a column of its own",
            "csv a header in capitals",
            "csv a padded cell",
            "csv a blank row",
            "csv crlf",
            "csv a column left out",
            "csv a row with no type is refused",
            "csv a required it cannot read is refused",
        ]
        .map(String::from),
    );
    fuzz::run(1..301, |seed| {
        let mut rng = Rng::seeded(seed);
        let mut fields = Vec::new();
        draw_fields(&mut rng, "", 0, &mut fields);
        let mut columns: Vec<&str> = COLUMNS
            .iter()
            .copied()
            .filter(|c| matches!(*c, "field" | "type") || !rng.chance(4))
            .collect();
        if columns.len() < COLUMNS.len() {
            cov.reach("csv a column left out");
            // What the file does not say, a field does not hold.
            for f in &mut fields {
                f.required &= columns.contains(&"required");
                for (name, cell) in [("description", &mut f.description), ("example", &mut f.example)] {
                    if !columns.contains(&name) {
                        cell.clear();
                    }
                }
                if !columns.contains(&"elasticsearch") {
                    f.elasticsearch = None;
                }
            }
        }
        if rng.chance(2) {
            cov.reach("csv a column of its own");
            columns.insert(rng.below(columns.len() + 1), "notes");
        }
        if rng.chance(2) {
            cov.reach("csv columns in another order");
            for i in (1..columns.len()).rev() {
                columns.swap(i, rng.below(i + 1));
            }
        }
        let caps = rng.chance(3);
        if caps {
            cov.reach("csv a header in capitals");
        }
        let end = match rng.chance(2) {
            true => {
                cov.reach("csv crlf");
                "\r\n"
            }
            false => "\n",
        };
        let header: Vec<String> = (columns.iter())
            .map(|c| if caps { c.to_uppercase() } else { c.to_string() })
            .collect();
        let mut text = header.join(",") + end;
        let mut row = 1;
        // Where a row is spoilt, its number and what the reader says of it.
        let mut spoilt: Option<String> = None;
        for f in &fields {
            if rng.chance(8) {
                cov.reach("csv a blank row");
                text += end;
                row += 1;
            }
            row += 1;
            let required = match f.required {
                true => *rng.pick(&["yes", "Y", "TRUE", "1"]),
                false => *rng.pick(&["", "no", "false", "0"]),
            };
            let mut required = required.to_string();
            let mut ty = f.ty.clone();
            if spoilt.is_none() && rng.chance(40) {
                if columns.contains(&"required") && rng.chance(2) {
                    cov.reach("csv a required it cannot read is refused");
                    required = "maybe".into();
                    spoilt = Some(format!("row {row}: `required` is \"maybe\", not yes or no"));
                } else {
                    cov.reach("csv a row with no type is refused");
                    ty.clear();
                    spoilt = Some(format!("row {row}: no `type` for `{}`", f.field));
                }
            }
            let es = f
                .elasticsearch
                .as_ref()
                .and_then(|m| m["type"].as_str())
                .unwrap_or_default();
            let cells: Vec<String> = (columns.iter())
                .map(|c| {
                    let cell = match *c {
                        "field" => f.field.as_str(),
                        "type" => &ty,
                        "required" => &required,
                        "description" => &f.description,
                        "example" => &f.example,
                        "elasticsearch" => es,
                        _ => "a note, of its own",
                    };
                    // A spreadsheet pads a cell at times; a description or an example is read trimmed.
                    match rng.chance(6) && !cell.is_empty() {
                        true => {
                            cov.reach("csv a padded cell");
                            quoted(&format!(" {cell} "))
                        }
                        false => quoted(cell),
                    }
                })
                .collect();
            text += &(cells.join(",") + end);
        }
        let read = read_as(ReadFormat::Csv, text);
        if let Some(said) = spoilt {
            return match read {
                Err(e) if e == said => Ok(()),
                other => Err(format!("a spoilt row reads as {other:?}, not {said:?}")),
            };
        }
        let read = read?;
        let trimmed: Vec<SchemaFieldDto> = (fields.iter())
            .map(|f| SchemaFieldDto {
                description: f.description.trim().to_string(),
                example: f.example.trim().to_string(),
                ..f.clone()
            })
            .collect();
        match read == trimmed {
            true => Ok(()),
            false => Err(format!("reads as {read:?}, not {trimmed:?}")),
        }
    });
    if !fuzz::replaying() {
        cov.assert_complete();
    }
}

/// OCSF's types, and the requirements an attribute may have, a missing one being optional.
const OCSF_TYPES: [&str; 11] = [
    "string_t",
    "integer_t",
    "long_t",
    "port_t",
    "timestamp_t",
    "float_t",
    "boolean_t",
    "datetime_t",
    "json_t",
    "ip_t",
    "object_t",
];
const LEVELS: [&str; 3] = ["required", "recommended", "optional"];

/// Attributes for a class or an object, `objects` the object names they may hold.
fn attributes(rng: &mut Rng, objects: &[String], cov: &mut Coverage) -> Value {
    let mut out = Map::new();
    for i in 0..1 + rng.below(3) {
        let ty = match rng.chance(3) && !objects.is_empty() {
            true => "object_t",
            false => rng.pick(&OCSF_TYPES[..10]),
        };
        cov.reach(format!("ocsf {ty}"));
        let mut a = json!({ "type": ty, "description": text(rng) });
        if ty == "object_t" {
            a["object_type"] = json!(rng.pick(objects));
        }
        if rng.chance(4) {
            a["is_array"] = json!(true);
        }
        if !rng.chance(5) {
            a["requirement"] = json!(rng.pick(&LEVELS));
        }
        if rng.chance(3) {
            a["example"] = match rng.chance(2) {
                true => json!(text(rng)),
                false => json!(rng.below(100)),
            };
        }
        out.insert(format!("a{i}"), a);
    }
    Value::Object(out)
}

/// The fields `cut` takes from `attributes`, by walking the file as the reader should: path, attribute, depth.
fn taken(doc: &Value, attributes: &Value, prefix: &str, depth: usize, cut: &Cut, out: &mut Vec<(String, Value)>) {
    for (key, a) in attributes.as_object().into_iter().flatten() {
        let level = a.get("requirement").and_then(Value::as_str).unwrap_or("optional");
        if !cut.levels.iter().any(|l| l == level) {
            continue;
        }
        let path = match prefix {
            "" => key.clone(),
            _ => format!("{prefix}.{key}"),
        };
        out.push((path.clone(), a.clone()));
        if a["type"] == "object_t" && depth < cut.depth {
            let inner = &doc["objects"][a["object_type"].as_str().unwrap_or_default()]["attributes"];
            taken(doc, inner, &path, depth + 1, cut, out);
        }
    }
}

#[test]
fn an_ocsf_class_reads_as_deep_as_its_cut_and_at_the_levels_taken() {
    let mut cov = Coverage::new(
        OCSF_TYPES
            .iter()
            .map(|t| format!("ocsf {t}"))
            .chain((1..=DEEPEST).map(|d| format!("ocsf depth {d}")))
            .chain(LEVELS.iter().map(|l| format!("ocsf {l} taken")))
            .chain(
                [
                    "ocsf an object that holds itself",
                    "ocsf an object cut off",
                    "ocsf no attribute taken is refused",
                    "ocsf a depth out of range is refused",
                    "ocsf a level OCSF has not is refused",
                    "ocsf an object the file lacks is refused",
                ]
                .map(String::from),
            ),
    );
    fuzz::run(1..301, |seed| {
        let mut rng = Rng::seeded(seed);
        let names: Vec<String> = (0..rng.below(4)).map(|i| format!("o{i}")).collect();
        let mut objects = Map::new();
        for name in &names {
            let attrs = attributes(&mut rng, &names, &mut cov);
            let holds_itself = (attrs.as_object().into_iter().flatten()).any(|(_, a)| a["object_type"] == json!(name));
            if holds_itself {
                cov.reach("ocsf an object that holds itself");
            }
            objects.insert(name.clone(), json!({ "attributes": attrs }));
        }
        // An object a class holds, which the file does not define.
        let lacking = rng.chance(10);
        let mut held = names.clone();
        if lacking {
            held.push("missing".into());
        }
        let class = attributes(&mut rng, &held, &mut cov);
        let doc = json!({ "classes": { "c": { "attributes": class } }, "objects": objects });
        let mut cut = Cut {
            depth: 1 + rng.below(DEEPEST),
            levels: LEVELS.iter().filter(|_| rng.chance(2)).map(|l| l.to_string()).collect(),
        };
        let mut said = None;
        match rng.below(12) {
            0 => {
                cov.reach("ocsf a depth out of range is refused");
                cut.depth = *rng.pick(&[0, DEEPEST + 1]);
                said = Some(format!("a depth is 1 to {DEEPEST}"));
            }
            1 => {
                cov.reach("ocsf a level OCSF has not is refused");
                cut.levels.push("maybe".into());
                said = Some("`maybe` is no OCSF requirement: required, recommended or optional".into());
            }
            _ => {}
        }
        let read = super::read(&ReadRequest {
            id: "fuzzed".into(),
            format: ReadFormat::Ocsf,
            text: doc.to_string(),
            class: "c".into(),
            cut: cut.clone(),
        })
        .map(|r| r.schema.fields);
        if let Some(said) = said {
            return match read {
                Err(e) if e == said => Ok(()),
                other => Err(format!("{other:?}, not {said:?}")),
            };
        }
        let mut want = Vec::new();
        taken(&doc, &doc["classes"]["c"]["attributes"], "", 1, &cut, &mut want);
        let reaches_missing =
            (want.iter()).any(|(p, a)| a["object_type"] == "missing" && p.split('.').count() < cut.depth);
        if reaches_missing {
            cov.reach("ocsf an object the file lacks is refused");
            return match read {
                Err(e) if e.ends_with("holds the object `missing`, which the file does not define") => Ok(()),
                other => Err(format!("a missing object reads as {other:?}")),
            };
        }
        if want.is_empty() {
            cov.reach("ocsf no attribute taken is refused");
            return match read {
                Err(e) if e == "`c` has no attribute of the levels taken" => Ok(()),
                other => Err(format!("a cut that takes nothing reads as {other:?}")),
            };
        }
        let read = read?;
        cov.reach(format!("ocsf depth {}", cut.depth));
        for level in &cut.levels {
            cov.reach(format!("ocsf {level} taken"));
        }
        let paths: Vec<&str> = read.iter().map(|f| f.field.as_str()).collect();
        let wanted: Vec<&str> = want.iter().map(|(p, _)| p.as_str()).collect();
        if paths != wanted {
            return Err(format!("takes {paths:?}, not {wanted:?}"));
        }
        for (f, (path, a)) in read.iter().zip(&want) {
            let object = a["type"] == "object_t";
            if object && path.split('.').count() == cut.depth {
                cov.reach("ocsf an object cut off");
            }
            let checks = [
                (f.required == (a["requirement"] == "required"), "required"),
                (f.ty.starts_with("array<") == (a["is_array"] == true), "array"),
                (f.ty.contains("{...}") == object, "object"),
                (
                    f.description == a["description"].as_str().unwrap_or_default(),
                    "description",
                ),
                (
                    (f.elasticsearch.as_ref().map(|m| m["type"].clone()))
                        == match a["type"].as_str() {
                            Some("ip_t") => Some(json!("ip")),
                            Some("json_t") => Some(json!("flattened")),
                            _ => None,
                        },
                    "mapping",
                ),
            ];
            if let Some((_, what)) = checks.iter().find(|(held, _)| !held) {
                return Err(format!("`{path}`'s {what}: {f:?} from {a}"));
            }
        }
        Ok(())
    });
    if !fuzz::replaying() {
        cov.assert_complete();
    }
}
