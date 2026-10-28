//! A record type (`plan::dictionary`) as an Elasticsearch mapping: the body of `PUT /<index>`, and the `template`
//! of an index template.

use serde_json::{Map, Value, json};

use crate::core::column::DataType;

/// `{"mappings": {"properties": ...}}` for a record of type `record`. A field whose type only the row knows is left
/// out, for Elasticsearch to map from the first document that carries it.
pub fn mapping(record: &DataType) -> Value {
    let properties = field(record);
    json!({ "mappings": properties.unwrap_or_else(|| json!({ "properties": {} })) })
}

/// The mapping of a field of type `ty`, where it has one.
fn field(ty: &DataType) -> Option<Value> {
    let name = match ty {
        DataType::Bool => "boolean",
        // A duration is written as integer microseconds.
        DataType::I64 | DataType::Duration => "long",
        DataType::F64 => "double",
        // Milliseconds: Elasticsearch's date_nanos would keep the microseconds, but refuses any date before 1970.
        DataType::DateTime => "date",
        // A longer value is kept but not indexed, since past 32766 bytes Elasticsearch would refuse the whole document.
        DataType::Str => return Some(json!({ "type": "keyword", "ignore_above": 8191 })),
        // Elasticsearch's own base64 field, which is how a document carries bytes.
        DataType::Bytes => "binary",
        // Elasticsearch has no array type: a field holds one value or many of one mapping.
        DataType::Array(element) => return field(element),
        DataType::Struct(fields, _) => {
            let mut properties = Map::new();
            for (key, t) in fields {
                if let Some(m) = field(t) {
                    properties.insert(key.clone(), m);
                }
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
        let keyword = json!({ "type": "keyword", "ignore_above": 8191 });
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
        assert_eq!(mapping(&record), want);
    }
}
