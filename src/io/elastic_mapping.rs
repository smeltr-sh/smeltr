//! A record type (`plan::dictionary`) as an Elasticsearch mapping: the body of `PUT /<index>`, and the `template`
//! of an index template. A string field ECS names takes the ECS mapping (`io::ecs_fields`).

use serde_json::{Map, Value, json};

use crate::core::column::DataType;

/// `{"mappings": {"properties": ...}}` for a record of type `record`. A field whose type only the row knows is left
/// out, for Elasticsearch to map from the first document that carries it.
pub fn mapping(record: &DataType) -> Value {
    json!({ "mappings": field(record, &mut String::new()).unwrap_or_else(|| json!({ "properties": {} })) })
}

/// How ECS maps a string field it names, where that differs from a plain keyword. Each accepts any string.
#[derive(Clone, Copy, Debug)]
pub enum Ecs {
    KeywordText,
    MatchOnlyText,
    Wildcard,
    WildcardText,
    /// Kept in the document, neither indexed nor aggregatable: `event.original`.
    Unindexed,
}

/// A string at `path`: ECS's mapping where ECS names it, else a keyword.
fn string(path: &str) -> Value {
    // A longer value is kept but not indexed: past 32766 bytes Elasticsearch would refuse the whole document.
    let keyword = json!({ "type": "keyword", "ignore_above": 8191 });
    let text = json!({ "text": { "type": "match_only_text" } });
    let fields = crate::io::ecs_fields::FIELDS;
    let Ok(i) = fields.binary_search_by(|(name, _)| (*name).cmp(path)) else {
        return keyword;
    };
    match fields[i].1 {
        Ecs::KeywordText => json!({ "type": "keyword", "ignore_above": 8191, "fields": text }),
        Ecs::MatchOnlyText => json!({ "type": "match_only_text" }),
        Ecs::Wildcard => json!({ "type": "wildcard" }),
        Ecs::WildcardText => json!({ "type": "wildcard", "fields": text }),
        Ecs::Unindexed => json!({ "type": "keyword", "index": false, "doc_values": false }),
    }
}

/// `path` is the field's dotted name, as Elasticsearch reads it.
fn field(ty: &DataType, path: &mut String) -> Option<Value> {
    let name = match ty {
        DataType::Bool => "boolean",
        // A duration is written as integer microseconds.
        DataType::I64 | DataType::Duration => "long",
        DataType::F64 => "double",
        // Milliseconds: Elasticsearch's date_nanos would keep the microseconds, but refuses any date before 1970.
        DataType::DateTime => "date",
        DataType::Str => return Some(string(path)),
        // Elasticsearch's own base64 field, which is how a document carries bytes.
        DataType::Bytes => "binary",
        // Elasticsearch has no array type: a field holds one value or many of one mapping.
        DataType::Array(element) => return field(element, path),
        DataType::Struct(fields, _) => {
            let at = path.len();
            let mut properties = Map::new();
            for (key, t) in fields {
                if !path.is_empty() {
                    path.push('.');
                }
                path.push_str(key);
                if let Some(m) = field(t, path) {
                    properties.insert(key.clone(), m);
                }
                path.truncate(at);
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
                ("message", DataType::Str),
                ("error", st(&[("stack_trace", DataType::Str)], false)),
                ("event", st(&[("original", DataType::Str)], false)),
            ],
            true,
        );
        let want = json!({ "mappings": { "properties": {
            "@timestamp": { "type": "date" },
            "status": { "type": "long" },
            "took": { "type": "double" },
            "ok": { "type": "boolean" },
            "wait": { "type": "long" },
            "url": { "properties": { "path": { "type": "wildcard" }, "query": { "type": "keyword", "ignore_above": 8191 } } },
            "tags": { "type": "keyword", "ignore_above": 8191 },
            "hits": { "properties": { "id": { "type": "keyword", "ignore_above": 8191 } } },
            "raw": { "type": "binary" },
            "a.b": { "type": "keyword", "ignore_above": 8191 },
            "message": { "type": "match_only_text" },
            "error": { "properties": { "stack_trace": {
                "type": "wildcard", "fields": { "text": { "type": "match_only_text" } }
            } } },
            "event": { "properties": { "original": { "type": "keyword", "index": false, "doc_values": false } } },
        } } });
        assert_eq!(mapping(&record), want);
    }

    #[test]
    fn the_ecs_table_is_sorted_for_its_search() {
        let names: Vec<&str> = crate::io::ecs_fields::FIELDS.iter().map(|(n, _)| *n).collect();
        assert!(names.windows(2).all(|w| w[0] < w[1]));
    }
}
