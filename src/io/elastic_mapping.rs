//! A record type (`plan::dictionary`) as an Elasticsearch mapping: the body of `PUT /<index>`, and the `template`
//! of an index template. A field ECS names takes ECS's mapping (`io::ecs_fields`) where the record's type is one ECS's
//! type holds; where it is not, the field takes its own type's mapping, and the `ecs` schema's check names it.

use serde_json::{Map, Value, json};

use crate::core::column::DataType;

/// `{"mappings": {"properties": ...}}` for a record of type `record`. A field whose type only the row knows is left
/// out, for Elasticsearch to map from the first document that carries it.
pub fn mapping(record: &DataType) -> Value {
    let properties = field(record, &mut Vec::new());
    json!({ "mappings": properties.unwrap_or_else(|| json!({ "properties": {} })) })
}

/// How ECS maps a field it names.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Ecs {
    Keyword,
    KeywordText,
    MatchOnlyText,
    Wildcard,
    WildcardText,
    ConstantKeyword,
    /// Kept in the document, neither indexed nor aggregatable: `event.original`.
    Unindexed,
    /// A long kept the way `Unindexed` keeps a string.
    UnindexedLong,
    Ip,
    Date,
    Long,
    Float,
    Double,
    ScaledFloat,
    Boolean,
    GeoPoint,
    Flattened,
    Object,
    Nested,
}

impl Ecs {
    /// ECS's name for the type, as its field reference spells it.
    pub fn name(self) -> &'static str {
        match self {
            Ecs::Keyword | Ecs::KeywordText | Ecs::Unindexed => "keyword",
            Ecs::MatchOnlyText => "match_only_text",
            Ecs::Wildcard | Ecs::WildcardText => "wildcard",
            Ecs::ConstantKeyword => "constant_keyword",
            Ecs::UnindexedLong | Ecs::Long => "long",
            Ecs::Ip => "ip",
            Ecs::Date => "date",
            Ecs::Float => "float",
            Ecs::Double => "double",
            Ecs::ScaledFloat => "scaled_float",
            Ecs::Boolean => "boolean",
            Ecs::GeoPoint => "geo_point",
            Ecs::Flattened => "flattened",
            Ecs::Object => "object",
            Ecs::Nested => "nested",
        }
    }

    /// The type a pipeline writes for this mapping to hold it, as the pipeline editor spells it.
    pub fn written_as(self) -> &'static str {
        match self {
            Ecs::Date => "datetime",
            Ecs::Long | Ecs::UnindexedLong => "i64",
            Ecs::Float | Ecs::Double | Ecs::ScaledFloat => "f64",
            Ecs::Boolean => "bool",
            Ecs::Flattened | Ecs::Object => "struct",
            Ecs::Nested => "array(struct)",
            Ecs::Keyword
            | Ecs::KeywordText
            | Ecs::MatchOnlyText
            | Ecs::Wildcard
            | Ecs::WildcardText
            | Ecs::ConstantKeyword
            | Ecs::Unindexed
            | Ecs::Ip
            | Ecs::GeoPoint => "str",
        }
    }

    /// Whether a value of type `ty` is one this mapping holds as ECS means it. `ty` is no array, but under `Nested`,
    /// which holds an array of objects as it holds one.
    pub fn holds(self, ty: &DataType) -> bool {
        use DataType as T;
        let leaf = match (self, ty) {
            (Ecs::Nested, T::Array(e)) => &**e,
            _ => ty,
        };
        let number = |t: &T| matches!(t, T::I64 | T::F64);
        match self {
            Ecs::Keyword
            | Ecs::KeywordText
            | Ecs::MatchOnlyText
            | Ecs::Wildcard
            | Ecs::WildcardText
            | Ecs::ConstantKeyword
            | Ecs::Unindexed
            | Ecs::Ip => *leaf == T::Str,
            Ecs::UnindexedLong | Ecs::Long => matches!(leaf, T::I64 | T::Duration),
            Ecs::Float | Ecs::Double | Ecs::ScaledFloat => number(leaf),
            Ecs::Date => *leaf == T::DateTime,
            Ecs::Boolean => *leaf == T::Bool,
            // Elasticsearch reads a point as `"lat,lon"` or as `{lat, lon}`.
            Ecs::GeoPoint => match leaf {
                T::Str => true,
                T::Struct(fields, false) => {
                    let has = |name: &str| fields.iter().any(|(k, t)| k == name && number(t));
                    fields.len() == 2 && has("lat") && has("lon")
                }
                _ => false,
            },
            Ecs::Flattened => matches!(leaf, T::Struct(..) | T::Variant),
            Ecs::Object | Ecs::Nested => matches!(leaf, T::Struct(..)),
        }
    }
}

/// How ECS maps the field at `path`, where ECS names it.
fn ecs(path: &str) -> Option<Ecs> {
    let fields = crate::io::ecs_fields::FIELDS;
    let i = fields.binary_search_by(|(name, ..)| (*name).cmp(path)).ok()?;
    Some(fields[i].1)
}

/// A keyword: a longer value is kept but not indexed, since past 32766 bytes Elasticsearch would refuse the whole
/// document.
fn keyword() -> Value {
    json!({ "type": "keyword", "ignore_above": 8191 })
}

/// The mapping of a field ECS maps as `ecs`, of a type `ecs` holds. An object's fields are `properties`.
fn ecs_mapping(ecs: Ecs, properties: Option<Value>) -> Value {
    let text = json!({ "text": { "type": "match_only_text" } });
    let plain = |name: &str| json!({ "type": name });
    match ecs {
        Ecs::Keyword => keyword(),
        Ecs::KeywordText => json!({ "type": "keyword", "ignore_above": 8191, "fields": text }),
        Ecs::WildcardText => json!({ "type": "wildcard", "fields": text }),
        Ecs::Unindexed => json!({ "type": "keyword", "index": false, "doc_values": false }),
        Ecs::UnindexedLong => json!({ "type": "long", "index": false, "doc_values": false }),
        // ECS gives each of its scaled floats, all of them a usage between 0 and 1, this factor.
        Ecs::ScaledFloat => json!({ "type": "scaled_float", "scaling_factor": 1000 }),
        Ecs::Object => properties.unwrap_or_else(|| json!({ "properties": {} })),
        Ecs::Nested => {
            let mut nested = properties.unwrap_or_else(|| json!({ "properties": {} }));
            nested["type"] = json!("nested");
            nested
        }
        // A value the field cannot read -- `-` for an address, a hostname -- is left out of the index rather than
        // the whole document refused.
        Ecs::Ip | Ecs::GeoPoint => json!({ "type": ecs.name(), "ignore_malformed": true }),
        _ => plain(ecs.name()),
    }
}

/// The mapping of a field of type `ty` at `keys`, where it has one.
fn field(ty: &DataType, keys: &mut Vec<String>) -> Option<Value> {
    // The root is no field, and a field's path is its keys dotted, as Elasticsearch reads them.
    let named = (!keys.is_empty()).then(|| ecs(&keys.join("."))).flatten();
    // Elasticsearch has no array type: a field holds one value or many of one mapping. Only a nested field maps
    // its array as a whole.
    if let DataType::Array(element) = ty
        && !(named == Some(Ecs::Nested) && Ecs::Nested.holds(ty))
    {
        return field(element, keys);
    }
    let held = named.filter(|e| e.holds(ty));
    let name = match (ty, held) {
        (DataType::Array(element), _) => {
            return Some(ecs_mapping(Ecs::Nested, properties(element, keys)));
        }
        (DataType::Struct(..), Some(e @ (Ecs::Object | Ecs::Nested))) => {
            return Some(ecs_mapping(e, properties(ty, keys)));
        }
        (DataType::Struct(..), None) => return properties(ty, keys),
        (_, Some(e)) => return Some(ecs_mapping(e, None)),
        (DataType::Bool, None) => "boolean",
        // A duration is written as integer microseconds.
        (DataType::I64 | DataType::Duration, None) => "long",
        (DataType::F64, None) => "double",
        // Milliseconds: Elasticsearch's date_nanos would keep the microseconds, but refuses any date before 1970.
        (DataType::DateTime, None) => "date",
        (DataType::Str, None) => return Some(keyword()),
        // Elasticsearch's own base64 field, which is how a document carries bytes.
        (DataType::Bytes, None) => "binary",
        (DataType::Null | DataType::Variant, None) => return None,
    };
    Some(json!({ "type": name }))
}

/// `{"properties": ...}` for an object of type `ty`, or `None` for any other type.
fn properties(ty: &DataType, keys: &mut Vec<String>) -> Option<Value> {
    let DataType::Struct(fields, _) = ty else {
        return None;
    };
    let mut properties = Map::new();
    for (key, t) in fields {
        keys.push(key.clone());
        if let Some(m) = field(t, keys) {
            properties.insert(key.clone(), m);
        }
        keys.pop();
    }
    Some(json!({ "properties": properties }))
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
    fn a_field_ecs_names_takes_ecs_type_where_its_own_is_one_ecs_holds() {
        let geo = st(&[("lat", DataType::F64), ("lon", DataType::F64)], false);
        let record = st(
            &[
                (
                    "source",
                    st(
                        &[
                            ("ip", DataType::Str),
                            ("port", DataType::I64),
                            ("geo", st(&[("location", geo)], false)),
                        ],
                        false,
                    ),
                ),
                ("host", st(&[("cpu", st(&[("usage", DataType::F64)], false))], false)),
                ("data_stream", st(&[("type", DataType::Str)], false)),
                (
                    "log",
                    st(
                        &[(
                            "syslog",
                            st(&[("structured_data", st(&[("a", DataType::I64)], true))], false),
                        )],
                        false,
                    ),
                ),
                ("labels", st(&[("env", DataType::Str)], false)),
                (
                    "email",
                    st(
                        &[(
                            "attachments",
                            DataType::Array(Box::new(st(&[("file", st(&[("name", DataType::Str)], false))], false))),
                        )],
                        false,
                    ),
                ),
                (
                    "tls",
                    st(
                        &[(
                            "server",
                            st(&[("x509", st(&[("public_key_exponent", DataType::I64)], false))], false),
                        )],
                        false,
                    ),
                ),
            ],
            false,
        );
        let want = json!({ "mappings": { "properties": {
            "source": { "properties": {
                "ip": { "type": "ip", "ignore_malformed": true },
                "port": { "type": "long" },
                "geo": { "properties": { "location": { "type": "geo_point", "ignore_malformed": true } } },
            } },
            "host": { "properties": { "cpu": { "properties": {
                "usage": { "type": "scaled_float", "scaling_factor": 1000 },
            } } } },
            "data_stream": { "properties": { "type": { "type": "constant_keyword" } } },
            "log": { "properties": { "syslog": { "properties": { "structured_data": { "type": "flattened" } } } } },
            "labels": { "properties": { "env": { "type": "keyword", "ignore_above": 8191 } } },
            "email": { "properties": { "attachments": { "type": "nested", "properties": {
                "file": { "properties": { "name": { "type": "keyword", "ignore_above": 8191 } } },
            } } } },
            "tls": { "properties": { "server": { "properties": { "x509": { "properties": {
                "public_key_exponent": { "type": "long", "index": false, "doc_values": false },
            } } } } } },
        } } });
        assert_eq!(mapping(&record), want);
    }

    #[test]
    fn a_field_whose_type_ecs_does_not_hold_keeps_its_own() {
        let record = st(
            &[
                ("@timestamp", DataType::Str),
                (
                    "source",
                    st(
                        &[
                            ("port", DataType::Array(Box::new(DataType::Str))),
                            ("geo", st(&[("location", DataType::F64)], false)),
                        ],
                        false,
                    ),
                ),
                ("user", st(&[("name", st(&[("first", DataType::Str)], false))], false)),
                ("event", st(&[("duration", DataType::Variant)], false)),
            ],
            false,
        );
        let want = json!({ "mappings": { "properties": {
            "@timestamp": { "type": "keyword", "ignore_above": 8191 },
            "source": { "properties": {
                "port": { "type": "keyword", "ignore_above": 8191 },
                "geo": { "properties": { "location": { "type": "double" } } },
            } },
            "user": { "properties": { "name": { "properties": { "first": { "type": "keyword", "ignore_above": 8191 } } } } },
            "event": { "properties": {} },
        } } });
        assert_eq!(mapping(&record), want);
    }

    /// What a misfit tells the author to write is a type ECS's mapping holds.
    #[test]
    fn each_ecs_type_holds_what_it_says_to_write() {
        let object = st(&[("a", DataType::Str)], true);
        for (name, ecs, _) in crate::io::ecs_fields::FIELDS {
            let ty = match ecs.written_as() {
                "struct" => object.clone(),
                "array(struct)" => DataType::Array(Box::new(object.clone())),
                written => [
                    DataType::DateTime,
                    DataType::I64,
                    DataType::F64,
                    DataType::Bool,
                    DataType::Str,
                ]
                .into_iter()
                .find(|t| t.to_string() == written)
                .unwrap_or_else(|| panic!("{name}: {written} is no type")),
            };
            assert!(ecs.holds(&ty), "{name}: {ecs:?} does not hold {ty:?}");
        }
    }

    #[test]
    fn the_ecs_table_is_sorted_for_its_search() {
        let names: Vec<&str> = crate::io::ecs_fields::FIELDS.iter().map(|(n, ..)| *n).collect();
        assert!(names.windows(2).all(|w| w[0] < w[1]));
    }
}
