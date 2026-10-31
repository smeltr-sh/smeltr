//! A record type (`plan::dictionary`) as an Elasticsearch mapping: the body of `PUT /<index>`, and the `template`
//! of an index template. A schema field may say how it maps (`schema::Field::elasticsearch`), and what it says is laid
//! over the mapping its type would have.

use std::collections::HashMap;

use serde_json::{Map, Value, json};

use crate::core::column::DataType;

/// How long a text's keyword is indexed up to: a longer value is kept but not indexed. ECS sets the same on every
/// keyword, and anything up to 8191 keeps Elasticsearch from refusing a document past 32766 bytes.
pub const IGNORE_ABOVE: u64 = 1024;

/// What a field says of its own mapping, by its keys from the record's root.
pub type Mapped<'a> = HashMap<Vec<String>, &'a Map<String, Value>>;

/// `{"mappings": {"properties": ...}}` for a record of type `record`, each field in `mapped` as it says. A field
/// whose type only the row knows, and that says nothing, is left out, for Elasticsearch to map from the first
/// document that carries it.
pub fn mapping(record: &DataType, mapped: &Mapped) -> Value {
    let properties = field(record, &mut Vec::new(), mapped);
    json!({ "mappings": properties.unwrap_or_else(|| json!({ "properties": {} })) })
}

/// The Elasticsearch types a field of type `ty` may map to: first the one it maps to of itself, where it has one.
pub fn types_for(ty: &DataType) -> &'static [&'static str] {
    match ty {
        DataType::Str => &[
            "keyword",
            "constant_keyword",
            "wildcard",
            "text",
            "match_only_text",
            "ip",
            "version",
            "date",
        ],
        // Integer microseconds, which as a date would read as a time a thousand times too late.
        DataType::Duration => &["long", "integer", "short", "byte", "unsigned_long"],
        DataType::I64 => &[
            "long",
            "integer",
            "short",
            "byte",
            "unsigned_long",
            "double",
            "float",
            "half_float",
            "scaled_float",
            "date",
        ],
        DataType::F64 => &["double", "float", "half_float", "scaled_float"],
        DataType::Bool => &["boolean"],
        DataType::DateTime => &["date", "date_nanos"],
        DataType::Bytes => &["binary"],
        DataType::Struct(..) => &["object", "nested", "flattened", "geo_point"],
        DataType::Array(element) => types_for(element),
        DataType::Variant => &[
            "keyword",
            "text",
            "wildcard",
            "long",
            "double",
            "boolean",
            "date",
            "object",
            "flattened",
            "binary",
        ],
        DataType::Null => &[],
    }
}

/// Why a field of type `ty` cannot map as `said` says, where it cannot: a mapping names its `type`, one a value of
/// `ty` can be indexed as. Its other parameters are Elasticsearch's to check.
pub fn refusal(ty: &DataType, said: &Map<String, Value>) -> Option<String> {
    let allowed = types_for(ty);
    match said.get("type").and_then(Value::as_str) {
        None => Some("an Elasticsearch mapping names its `type`".to_string()),
        Some(name) if allowed.contains(&name) => None,
        Some(name) => Some(format!(
            "Elasticsearch's `{name}` cannot hold {}; it takes {}",
            ty.spell(),
            allowed.join(", ")
        )),
    }
}

/// The mapping of the field at `keys`, of type `ty`, where it has one: what it says over its type's own.
fn field(ty: &DataType, keys: &mut Vec<String>, mapped: &Mapped) -> Option<Value> {
    let own = typed(ty, keys, mapped);
    let Some(said) = mapped.get(keys.as_slice()) else {
        return own;
    };
    let says = said.get("type").and_then(Value::as_str);
    let mut out = match own {
        Some(Value::Object(own)) => {
            // An object's own mapping names no type.
            let same = says == Some(own.get("type").and_then(Value::as_str).unwrap_or("object"));
            // The same type keeps what its own mapping set, `ignore_above` or an object's properties; another keeps
            // only an object's properties, where it still has them.
            own.into_iter()
                .filter(|(k, _)| same || (k == "properties" && matches!(says, Some("object" | "nested"))))
                .collect()
        }
        _ => Map::new(),
    };
    out.extend(said.iter().map(|(k, v)| (k.clone(), v.clone())));
    Some(Value::Object(out))
}

/// The mapping a field of type `ty` has of its own.
fn typed(ty: &DataType, keys: &mut Vec<String>, mapped: &Mapped) -> Option<Value> {
    let name = match ty {
        DataType::Bool => "boolean",
        // A duration is written as integer microseconds.
        DataType::I64 | DataType::Duration => "long",
        DataType::F64 => "double",
        // Milliseconds: Elasticsearch's date_nanos would keep the microseconds, but refuses any date before 1970.
        DataType::DateTime => "date",
        DataType::Str => return Some(json!({ "type": "keyword", "ignore_above": IGNORE_ABOVE })),
        // Elasticsearch's own base64 field, which is how a document carries bytes.
        DataType::Bytes => "binary",
        // Elasticsearch has no array type: a field holds one value or many of one mapping.
        DataType::Array(element) => return typed(element, keys, mapped),
        DataType::Struct(fields, _) => {
            let mut properties = Map::new();
            for (key, t) in fields {
                keys.push(key.clone());
                if let Some(m) = field(t, keys, mapped) {
                    properties.insert(key.clone(), m);
                }
                keys.pop();
            }
            return Some(json!({ "properties": properties }));
        }
        DataType::Null | DataType::Variant => return None,
    };
    Some(json!({ "type": name }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn st(fields: &[(&str, DataType)], open: bool) -> DataType {
        DataType::Struct(fields.iter().map(|(k, t)| (k.to_string(), t.clone())).collect(), open)
    }

    #[test]
    fn every_type_maps_as_the_plan_decided() {
        let record = st(
            &[
                ("@timestamp", DataType::DateTime),
                ("status", DataType::I64),
                ("took", DataType::F64),
                ("ok", DataType::Bool),
                ("wait", DataType::Duration),
                ("url", st(&[("path", DataType::Str), ("query", DataType::Str)], false)),
                ("tags", DataType::Array(Box::new(DataType::Str))),
                ("hits", DataType::Array(Box::new(st(&[("id", DataType::Str)], false)))),
                ("raw", DataType::Bytes),
                ("attrs", DataType::Variant),
                ("never", DataType::Null),
                ("a.b", DataType::Str),
            ],
            true,
        );
        let keyword = json!({ "type": "keyword", "ignore_above": IGNORE_ABOVE });
        let want = json!({ "mappings": { "properties": {
            "@timestamp": { "type": "date" },
            "status": { "type": "long" },
            "took": { "type": "double" },
            "ok": { "type": "boolean" },
            "wait": { "type": "long" },
            "url": { "properties": { "path": keyword, "query": keyword } },
            "tags": keyword,
            "hits": { "properties": { "id": keyword } },
            "raw": { "type": "binary" },
            "a.b": keyword,
        } } });
        assert_eq!(mapping(&record, &Mapped::new()), want);
    }

    #[test]
    fn what_a_field_says_is_laid_over_its_own_mapping() {
        let record = st(
            &[
                ("url", st(&[("full", DataType::Str), ("path", DataType::Str)], false)),
                ("hits", DataType::Array(Box::new(st(&[("id", DataType::Str)], false)))),
                ("labels", st(&[("team", DataType::Str)], true)),
                ("attrs", DataType::Variant),
                ("price", DataType::F64),
            ],
            true,
        );
        let said = |v: Value| v.as_object().unwrap().clone();
        let (wildcard, longer, nested, flattened, any, scaled) = (
            said(json!({ "type": "wildcard" })),
            said(json!({ "type": "keyword", "ignore_above": 4096 })),
            said(json!({ "type": "nested" })),
            said(json!({ "type": "flattened" })),
            said(json!({ "type": "flattened" })),
            said(json!({ "type": "scaled_float", "scaling_factor": 100 })),
        );
        let keys = |path: &str| path.split('.').map(str::to_string).collect::<Vec<_>>();
        let mapped: Mapped = [
            (keys("url.full"), &wildcard),
            (keys("url.path"), &longer),
            (keys("hits"), &nested),
            (keys("labels"), &flattened),
            (keys("attrs"), &any),
            (keys("price"), &scaled),
        ]
        .into_iter()
        .collect();
        let want = json!({ "mappings": { "properties": {
            "url": { "properties": {
                "full": { "type": "wildcard" },
                "path": { "type": "keyword", "ignore_above": 4096 },
            } },
            "hits": { "type": "nested", "properties": { "id": { "type": "keyword", "ignore_above": IGNORE_ABOVE } } },
            "labels": { "type": "flattened" },
            "attrs": { "type": "flattened" },
            "price": { "type": "scaled_float", "scaling_factor": 100 },
        } } });
        assert_eq!(mapping(&record, &mapped), want);
    }

    /// The GUI's Elasticsearch view (`gui-frontend/src/schemas.ts`, `elasticsearchTypes`) carries a hand-written copy
    /// of `types_for`; this keeps its lists equal to these, in order, since the first is the default it names.
    #[test]
    fn the_gui_offers_the_types_each_field_can_hold() {
        // A Windows checkout may hold the file with CRLF.
        let source = include_str!("../../gui-frontend/src/schemas.ts").replace("\r\n", "\n");
        let body = &source[source
            .find("export function elasticsearchTypes")
            .expect("elasticsearchTypes")..];
        let body = &body[..body.find("\n}\n").expect("its end")];
        let listed = |pattern: &str| {
            let re = regex::Regex::new(&format!(r"{pattern}\s*return \[([^\]]*)\]")).unwrap();
            let names = re.captures(body).unwrap_or_else(|| panic!("no list after {pattern}"))[1].to_string();
            let quoted = regex::Regex::new(r"'([a-z_]+)'").unwrap();
            quoted
                .captures_iter(&names)
                .map(|c| c[1].to_string())
                .collect::<Vec<_>>()
        };
        let any_struct = DataType::Struct(Vec::new(), true);
        assert_eq!(
            listed(r"startsWith\('\{'\)\)"),
            types_for(&any_struct),
            "a struct's types differ"
        );
        for ty in [
            DataType::Str,
            DataType::I64,
            DataType::Duration,
            DataType::F64,
            DataType::Bool,
            DataType::DateTime,
            DataType::Bytes,
            DataType::Variant,
        ] {
            assert_eq!(listed(&format!("case '{ty}':")), types_for(&ty), "{ty}'s types differ");
        }
    }

    #[test]
    fn a_mapping_names_a_type_its_field_can_hold() {
        let said = |v: Value| v.as_object().unwrap().clone();
        assert_eq!(refusal(&DataType::Str, &said(json!({ "type": "ip" }))), None);
        assert_eq!(refusal(&DataType::I64, &said(json!({ "type": "date" }))), None);
        assert!(refusal(&DataType::Duration, &said(json!({ "type": "date" }))).is_some());
        assert_eq!(
            refusal(
                &DataType::Array(Box::new(DataType::Str)),
                &said(json!({ "type": "wildcard" }))
            ),
            None
        );
        assert_eq!(
            refusal(&DataType::Str, &said(json!({ "type": "long" }))).unwrap(),
            "Elasticsearch's `long` cannot hold str; it takes keyword, constant_keyword, wildcard, text, \
             match_only_text, ip, version, date"
        );
        assert_eq!(
            refusal(&DataType::Bool, &said(json!({ "index": false }))).unwrap(),
            "an Elasticsearch mapping names its `type`"
        );
    }
}
