//! A schema from one OCSF class, as `ocsf-schema-compiler` writes OCSF: one JSON document, every class and object with
//! its attributes typed. The class's attributes are fields, and so are those of the objects they hold, as deep as
//! `Cut::depth` keys; an object below that is `{...}`, which a row fits whatever it holds. `Cut::levels` says which
//! of OCSF's requirements are taken: by default `required` and `recommended`, the list OCSF tells a producer to fill.

use serde_json::{Map, Value, json};

use crate::config_file::SchemaFieldDto;

/// How much of a class becomes the schema.
#[derive(Clone, Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Cut {
    /// How many keys deep fields are taken: 1 is the class's own attributes.
    #[serde(default = "default_depth")]
    pub depth: usize,
    /// Which requirements are taken, as OCSF names them: `required`, `recommended`, `optional`.
    #[serde(default = "default_levels")]
    pub levels: Vec<String>,
}

impl Default for Cut {
    fn default() -> Self {
        Cut {
            depth: default_depth(),
            levels: default_levels(),
        }
    }
}

fn default_depth() -> usize {
    2
}

fn default_levels() -> Vec<String> {
    vec!["required".to_string(), "recommended".to_string()]
}

/// The deepest `Cut::depth`. OCSF's objects name each other, so a cut has to stop somewhere.
pub const DEEPEST: usize = 8;

/// The fields of the class `class` in `doc`, cut as `cut` says.
pub fn read(doc: &Value, class: &str, cut: &Cut) -> Result<Vec<SchemaFieldDto>, String> {
    if !(1..=DEEPEST).contains(&cut.depth) {
        return Err(format!("a depth is 1 to {DEEPEST}"));
    }
    if let Some(level) = cut
        .levels
        .iter()
        .find(|l| !["required", "recommended", "optional"].contains(&l.as_str()))
    {
        return Err(format!(
            "`{level}` is no OCSF requirement: required, recommended or optional"
        ));
    }
    let classes = doc
        .get("classes")
        .and_then(Value::as_object)
        .ok_or("not ocsf-schema-compiler's output: it has no `classes`")?;
    let attributes = (classes
        .get(class)
        .and_then(|c| c.get("attributes"))
        .and_then(Value::as_object))
    .ok_or_else(|| format!("the file has no class `{class}`"))?;
    let reader = Reader {
        objects: doc.get("objects").and_then(Value::as_object),
        cut,
    };
    let mut out = Vec::new();
    reader.attributes(attributes, "", 1, &mut out)?;
    if out.is_empty() {
        return Err(format!("`{class}` has no attribute of the levels taken"));
    }
    Ok(out)
}

/// The classes `doc` holds, by name, for a picker.
pub fn classes(doc: &Value) -> Vec<String> {
    let names = doc
        .get("classes")
        .and_then(Value::as_object)
        .into_iter()
        .flat_map(|c| c.keys());
    names.cloned().collect()
}

struct Reader<'d> {
    objects: Option<&'d Map<String, Value>>,
    cut: &'d Cut,
}

impl Reader<'_> {
    fn attributes(
        &self,
        attributes: &Map<String, Value>,
        prefix: &str,
        depth: usize,
        out: &mut Vec<SchemaFieldDto>,
    ) -> Result<(), String> {
        for (key, a) in attributes {
            let requirement = a.get("requirement").and_then(Value::as_str).unwrap_or("optional");
            if !self.cut.levels.iter().any(|l| l == requirement) {
                continue;
            }
            let path = match prefix {
                "" => key.clone(),
                _ => format!("{prefix}.{key}"),
            };
            let ty = a.get("type").and_then(Value::as_str).unwrap_or_default();
            let element = match ty {
                "object_t" => "{...}",
                "boolean_t" => "bool",
                // A timestamp is epoch milliseconds.
                "integer_t" | "long_t" | "port_t" | "timestamp_t" => "i64",
                "float_t" => "f64",
                "datetime_t" => "datetime",
                "json_t" => "variant",
                _ => "str",
            };
            out.push(SchemaFieldDto {
                field: path.clone(),
                ty: match a.get("is_array") == Some(&Value::Bool(true)) {
                    true => format!("array<{element}>"),
                    false => element.to_string(),
                },
                // Inside an object, required holds where the object is there.
                required: requirement == "required",
                description: a
                    .get("description")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                example: super::csv::example_of(a.get("example")),
                // An IP is text Elasticsearch indexes as one, and JSON a value it keeps whole.
                elasticsearch: match ty {
                    "ip_t" => json!({ "type": "ip" }).as_object().cloned(),
                    "json_t" => json!({ "type": "flattened" }).as_object().cloned(),
                    _ => None,
                },
            });
            if ty == "object_t" && depth < self.cut.depth {
                let name = a.get("object_type").and_then(Value::as_str).unwrap_or_default();
                let inner = (self.objects.and_then(|o| o.get(name)))
                    .and_then(|o| o.get("attributes"))
                    .and_then(Value::as_object)
                    .ok_or_else(|| format!("`{path}` holds the object `{name}`, which the file does not define"))?;
                self.attributes(inner, &path, depth + 1, out)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn doc() -> Value {
        serde_json::from_str(include_str!("../../tests/fixtures/ocsf-authentication.json")).unwrap()
    }

    fn field<'f>(fields: &'f [SchemaFieldDto], path: &str) -> Option<&'f SchemaFieldDto> {
        fields.iter().find(|f| f.field == path)
    }

    #[test]
    fn a_class_reads_two_keys_deep_as_ocsf_asks_a_producer_to_fill_it() {
        let fields = read(&doc(), "authentication", &Cut::default()).unwrap();
        let user = field(&fields, "user").unwrap();
        assert_eq!((user.ty.as_str(), user.required), ("{...}", true));
        assert_eq!(field(&fields, "user.name").unwrap().ty, "str");
        assert_eq!(field(&fields, "observables").unwrap().ty, "array<{...}>");
        assert_eq!(
            field(&fields, "src_endpoint.ip").unwrap().elasticsearch,
            json!({ "type": "ip" }).as_object().cloned()
        );
        assert!(fields.iter().all(|f| f.field.split('.').count() <= 2));
        assert!(field(&fields, "action").is_none(), "action is optional");
    }

    #[test]
    fn a_cut_says_how_deep_and_which_requirements() {
        let only_required = Cut {
            depth: 1,
            levels: vec!["required".into()],
        };
        let fields = read(&doc(), "authentication", &only_required).unwrap();
        assert!(fields.iter().all(|f| f.required && !f.field.contains('.')));
        let everything = Cut {
            depth: 2,
            levels: vec!["required".into(), "recommended".into(), "optional".into()],
        };
        let more = read(&doc(), "authentication", &everything).unwrap();
        assert!(field(&more, "action").is_some());
        assert!(more.len() > read(&doc(), "authentication", &Cut::default()).unwrap().len());
    }

    #[test]
    fn a_file_or_a_cut_it_cannot_read_says_so() {
        let cut = Cut::default();
        assert_eq!(
            read(&json!({}), "x", &cut).unwrap_err(),
            "not ocsf-schema-compiler's output: it has no `classes`"
        );
        assert_eq!(read(&doc(), "nope", &cut).unwrap_err(), "the file has no class `nope`");
        let deep = Cut {
            depth: 0,
            ..Cut::default()
        };
        assert_eq!(read(&doc(), "authentication", &deep).unwrap_err(), "a depth is 1 to 8");
        let odd = Cut {
            levels: vec!["maybe".into()],
            ..Cut::default()
        };
        assert_eq!(
            read(&doc(), "authentication", &odd).unwrap_err(),
            "`maybe` is no OCSF requirement: required, recommended or optional"
        );
        assert_eq!(classes(&doc()), vec!["authentication".to_string()]);
    }
}
