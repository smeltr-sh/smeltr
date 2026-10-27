//! ECS as a schema, `ecs`: every field ECS names (`io::ecs_fields`), each as ECS maps it. ECS lists its fields by
//! dotted path, so the objects on a path that ECS does not list itself are implied: `source` holds `source.ip`, and
//! a `source` of another type breaks nothing.

use crate::io::ecs_fields::{ECS_VERSION, FIELDS};
use crate::io::elastic_mapping::Ecs;

use super::{Field, Leaf, Object, Objects, Origin, Reading, Schema, Type};

pub(super) fn schema() -> Schema {
    let mut objects = Objects::default();
    let root = objects.push(object(""));
    for (path, ecs, description) in FIELDS {
        let segments: Vec<&str> = path.split('.').collect();
        let mut at = root;
        for (i, segment) in segments.iter().enumerate() {
            let last = i + 1 == segments.len();
            let existing = objects.get_mut(at).fields.iter().position(|f| f.name == *segment);
            let next = match existing.map(|f| objects.get_mut(at).fields[f].ty) {
                // A field ECS lists after a path through it: the path's object was implied until now.
                Some(Type::Object { at: inner, .. }) if last => {
                    let field = &mut objects.get_mut(at).fields[existing.unwrap()];
                    *field = ecs_field(segment, *ecs, description, inner);
                    inner
                }
                Some(Type::Object { at: inner, .. }) => inner,
                Some(Type::Leaf(_)) => panic!("ECS lists {path} under a field that is no object"),
                None => {
                    let inner = objects.push(object(&segments[..=i].join(".")));
                    let field = match last {
                        true => ecs_field(segment, *ecs, description, inner),
                        false => Field {
                            name: segment.to_string(),
                            ty: Type::Object {
                                at: inner,
                                implied: true,
                            },
                            type_name: "object".to_string(),
                            array: false,
                            required: false,
                            description: String::new(),
                        },
                    };
                    objects.get_mut(at).fields.push(field);
                    inner
                }
            };
            at = next;
        }
    }
    Schema::new(
        "ecs".to_string(),
        "ECS".to_string(),
        "Elastic Common Schema: the fields Elasticsearch, Kibana and Elastic Security read.".to_string(),
        Origin::Ecs { version: ECS_VERSION },
        Reading::Elasticsearch,
        objects.into_shared(),
        root,
    )
}

fn object(path: &str) -> Object {
    Object {
        name: path.to_string(),
        description: String::new(),
        fields: Vec::new(),
    }
}

/// The field ECS lists, with `inner` the object a field ECS maps as one takes for its fields.
fn ecs_field(name: &str, ecs: Ecs, description: &str, inner: usize) -> Field {
    Field {
        name: name.to_string(),
        ty: match ecs {
            Ecs::Object | Ecs::Nested => Type::Object {
                at: inner,
                implied: false,
            },
            e => Type::Leaf(Leaf::Ecs(e)),
        },
        type_name: ecs.name().to_string(),
        array: ecs == Ecs::Nested,
        required: false,
        description: description.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::column::DataType;
    use crate::schema::Misfit;

    #[test]
    fn every_field_ecs_lists_is_found_by_its_path() {
        let ecs = schema();
        for (path, e, _) in FIELDS {
            let keys: Vec<String> = path.split('.').map(str::to_string).collect();
            let f = ecs.field(&keys).unwrap_or_else(|| panic!("{path}"));
            assert_eq!(f.type_name, e.name(), "{path}");
            // As Elasticsearch reads a document, a dotted key is the same path.
            assert!(ecs.field(&[path.to_string()]).is_some(), "{path}");
        }
    }

    fn st(fields: &[(&str, DataType)]) -> DataType {
        DataType::Struct(fields.iter().map(|(k, t)| (k.to_string(), t.clone())).collect(), false)
    }

    #[test]
    fn a_field_whose_type_ecs_does_not_hold_is_named_and_one_it_holds_is_not() {
        let geo = st(&[("lat", DataType::F64), ("lon", DataType::F64)]);
        let record = st(&[
            ("@timestamp", DataType::Str),
            (
                "source",
                st(&[
                    ("ip", DataType::Str),
                    ("port", DataType::Array(Box::new(DataType::Str))),
                    ("geo", st(&[("location", DataType::F64)])),
                ]),
            ),
            ("destination", st(&[("geo", st(&[("location", geo)]))])),
            ("user", st(&[("name", st(&[("first", DataType::Str)]))])),
            ("event", st(&[("duration", DataType::Variant)])),
            ("host", DataType::Str),
            (
                "email",
                st(&[("attachments", DataType::Array(Box::new(st(&[("file", st(&[]))]))))]),
            ),
            ("url.path", DataType::I64),
            ("labels", st(&[("env", DataType::Str)])),
        ]);
        let named: Vec<(String, DataType, String)> = schema()
            .check(&record)
            .into_iter()
            .map(|f| match f.misfit {
                Misfit::Type { ty, type_name, .. } => (f.keys.join("."), ty, type_name),
                Misfit::Missing => panic!("ECS requires no field"),
            })
            .collect();
        // A variant is no misfit: only the row knows its type. `host` is implied, no field of ECS's own.
        assert_eq!(
            named,
            [
                ("@timestamp".to_string(), DataType::Str, "date".to_string()),
                ("source.port".to_string(), DataType::Str, "long".to_string()),
                (
                    "source.geo.location".to_string(),
                    DataType::F64,
                    "geo_point".to_string()
                ),
                (
                    "user.name".to_string(),
                    st(&[("first", DataType::Str)]),
                    "keyword".to_string()
                ),
                ("url.path".to_string(), DataType::I64, "wildcard".to_string()),
            ]
        );
    }
}
