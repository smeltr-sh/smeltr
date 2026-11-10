//! A schema from one OCSF class, as `ocsf-schema-compiler` writes OCSF: one JSON document, every class and object with
//! its attributes typed. The class's attributes are fields, and so are those of the objects they hold, as deep as
//! `Cut::depth` keys; an object below that is `{...}`, which a row fits whatever it holds. `Cut::levels` says which
//! of OCSF's requirements are taken: by default `required` and `recommended`, the list OCSF tells a producer to fill.
//! A profile adds attributes to the classes and objects that take it, and a record names the profiles it takes in
//! `metadata.profiles`. Only the attributes of the profiles in `Cut::profiles` are taken, and with any taken,
//! `metadata` and `metadata.profiles` are required at any depth and levels.

use serde_json::{Map, Value, json};

use crate::config_file::{Requirement, SchemaFieldDto};

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
    /// The profiles whose attributes are taken, by name. OCSF applies none unless a record says so.
    #[serde(default)]
    pub profiles: Vec<String>,
}

impl Default for Cut {
    fn default() -> Self {
        Cut {
            depth: default_depth(),
            levels: default_levels(),
            profiles: Vec::new(),
        }
    }
}

fn default_depth() -> usize {
    2
}

fn default_levels() -> Vec<String> {
    vec!["required".to_string(), "recommended".to_string()]
}

/// The fields a record that takes any profile must hold, to name them.
const LISTED: [&str; 2] = ["metadata", "metadata.profiles"];

/// The deepest `Cut::depth`. OCSF's objects name each other, so a cut has to stop somewhere.
pub const DEEPEST: usize = 8;

/// The most fields a cut may make. OCSF's objects hold each other, so a deep cut that takes optional attributes makes
/// tens of thousands, more than a schema anyone reads.
pub const MOST: usize = 10_000;

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
    let (of, attributes) = (classes.get(class))
        .and_then(|c| Some((c, c.get("attributes")?.as_object()?)))
        .ok_or_else(|| format!("the file has no class `{class}`"))?;
    let takes = profiles(of);
    if let Some(p) = cut.profiles.iter().find(|p| !takes.contains(&p.as_str())) {
        let takes = match takes.is_empty() {
            true => "no profiles".to_string(),
            false => takes.join(", "),
        };
        return Err(format!("`{class}` takes no profile `{p}`: it takes {takes}"));
    }
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

/// How a schema read from `class` of `doc` with `cut` was read, for its description, so it can be read again the same
/// way: the release, the class, the cut and the profiles, by OCSF's captions.
pub fn described(doc: &Value, class: &str, cut: &Cut) -> String {
    let caption = |of: &Value, name: &str| of["caption"].as_str().unwrap_or(name).to_string();
    let profiles: Vec<String> = (cut.profiles.iter()).map(|p| caption(&doc["profiles"][p], p)).collect();
    format!(
        "OCSF {}{}, {} {} deep, {} fields, profiles: {}",
        doc["version"].as_str().map_or(String::new(), |v| format!("{v} ")),
        caption(&doc["classes"][class], class),
        cut.depth,
        if cut.depth == 1 { "level" } else { "levels" },
        cut.levels.join(" and "),
        match profiles.is_empty() {
            true => "none".to_string(),
            false => profiles.join(", "),
        }
    )
}

/// The profiles a class or an attribute takes, by name.
fn profiles(of: &Value) -> Vec<&str> {
    let names = of.get("profiles").and_then(Value::as_array).into_iter().flatten();
    names.filter_map(Value::as_str).collect()
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
            let from = profiles(a);
            if !from.is_empty() && !from.iter().any(|p| self.cut.profiles.iter().any(|c| c == p)) {
                continue;
            }
            let path = match prefix {
                "" => key.clone(),
                _ => format!("{prefix}.{key}"),
            };
            // A record that takes a profile names it in `metadata.profiles`, whatever the depth and levels taken.
            let listed = !self.cut.profiles.is_empty() && LISTED.contains(&path.as_str());
            let requirement = match listed {
                true => "required",
                false => a.get("requirement").and_then(Value::as_str).unwrap_or("optional"),
            };
            if !listed && !self.cut.levels.iter().any(|l| l == requirement) {
                continue;
            }
            let ty = a.get("type").and_then(Value::as_str).unwrap_or_default();
            let element = match ty {
                // A timestamp is epoch milliseconds.
                "port_t" | "timestamp_t" => "i64",
                "json_t" => "variant",
                ty => super::named(ty.trim_end_matches("_t")).unwrap_or("str"),
            };
            out.push(SchemaFieldDto {
                field: path.clone(),
                ty: match a.get("is_array") == Some(&Value::Bool(true)) {
                    true => format!("array<{element}>"),
                    false => element.to_string(),
                },
                // Inside an object, a requirement holds where the object is there.
                requirement: Requirement::named(requirement).unwrap_or_default(),
                description: a
                    .get("description")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                example: super::example_of(a.get("example")),
                // An IP is text Elasticsearch indexes as one, and JSON a value it keeps whole.
                elasticsearch: match ty {
                    "ip_t" => json!({ "type": "ip" }).as_object().cloned(),
                    "json_t" => json!({ "type": "flattened" }).as_object().cloned(),
                    _ => None,
                },
            });
            if out.len() > MOST {
                return Err(format!(
                    "the cut makes more than {MOST} fields: take less depth or fewer requirements"
                ));
            }
            if ty == "object_t" && (depth < self.cut.depth || listed) {
                let name = a.get("object_type").and_then(Value::as_str).unwrap_or_default();
                let inner = (self.objects.and_then(|o| o.get(name)))
                    .and_then(|o| o.get("attributes"))
                    .and_then(Value::as_object)
                    .ok_or_else(|| format!("`{path}` holds the object `{name}`, which the file does not define"))?;
                match depth < self.cut.depth {
                    true => self.attributes(inner, &path, depth + 1, out)?,
                    // Below the depth taken, only `metadata.profiles`.
                    false => {
                        let only: Map<String, Value> = inner
                            .iter()
                            .filter(|(k, _)| *k == "profiles")
                            .map(|(k, v)| (k.clone(), v.clone()))
                            .collect();
                        self.attributes(&only, &path, depth + 1, out)?;
                    }
                }
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
        assert_eq!((user.ty.as_str(), user.requirement), ("{...}", Requirement::Required));
        assert_eq!(
            field(&fields, "src_endpoint").unwrap().requirement,
            Requirement::Recommended
        );
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
            ..Cut::default()
        };
        let fields = read(&doc(), "authentication", &only_required).unwrap();
        assert!((fields.iter()).all(|f| f.requirement == Requirement::Required && !f.field.contains('.')));
        let everything = Cut {
            depth: 2,
            levels: vec!["required".into(), "recommended".into(), "optional".into()],
            ..Cut::default()
        };
        let more = read(&doc(), "authentication", &everything).unwrap();
        assert_eq!(field(&more, "action").unwrap().requirement, Requirement::Optional);
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

    /// A class taking the profile `host`, whose `device` holds an object `container` adds an attribute to.
    fn profiled() -> Value {
        let device =
            json!({ "type": "object_t", "object_type": "device", "requirement": "recommended", "profiles": ["host"] });
        let metadata = json!({ "type": "object_t", "object_type": "metadata", "requirement": "required" });
        json!({
            "classes": { "c": { "profiles": ["host", "container"], "attributes": {
                "time": { "type": "timestamp_t", "requirement": "required" },
                "device": device,
                "metadata": metadata,
            } } },
            "objects": {
                "device": { "attributes": {
                    "name": { "type": "string_t", "requirement": "required" },
                    "container": { "type": "string_t", "requirement": "recommended", "profiles": ["container"] },
                } },
                "metadata": { "attributes": {
                    "profiles": { "type": "string_t", "is_array": true, "requirement": "optional" },
                } },
            },
        })
    }

    fn paths(fields: &[SchemaFieldDto]) -> Vec<&str> {
        fields.iter().map(|f| f.field.as_str()).collect()
    }

    #[test]
    fn a_profile_adds_its_attributes_only_where_it_is_taken_at_any_depth() {
        let read_with = |profiles: &[&str]| {
            let cut = Cut {
                profiles: profiles.iter().map(|p| p.to_string()).collect(),
                ..Cut::default()
            };
            let mut fields = read(&profiled(), "c", &cut).unwrap();
            fields.sort_by(|a, b| a.field.cmp(&b.field));
            fields
        };
        assert_eq!(paths(&read_with(&[])), ["metadata", "time"]);
        let host = read_with(&["host"]);
        assert_eq!(
            paths(&host),
            ["device", "device.name", "metadata", "metadata.profiles", "time"]
        );
        let listed = field(&host, "metadata.profiles").unwrap();
        assert_eq!(
            (listed.ty.as_str(), listed.requirement),
            ("array<str>", Requirement::Required)
        );
        assert_eq!(
            paths(&read_with(&["host", "container"])),
            [
                "device",
                "device.container",
                "device.name",
                "metadata",
                "metadata.profiles",
                "time"
            ]
        );
        assert_eq!(
            paths(&read_with(&["container"])),
            ["metadata", "metadata.profiles", "time"]
        );
    }

    #[test]
    fn with_a_profile_metadata_profiles_is_required_at_any_depth_and_levels() {
        let cut = |depth: usize, levels: &[&str]| Cut {
            depth,
            levels: levels.iter().map(|l| l.to_string()).collect(),
            profiles: vec!["host".into()],
        };
        let mut shallow = read(&profiled(), "c", &cut(1, &["required"])).unwrap();
        shallow.sort_by(|a, b| a.field.cmp(&b.field));
        assert_eq!(paths(&shallow), ["metadata", "metadata.profiles", "time"]);
        let mut none_required = read(&profiled(), "c", &cut(2, &["recommended"])).unwrap();
        none_required.sort_by(|a, b| a.field.cmp(&b.field));
        assert_eq!(paths(&none_required), ["device", "metadata", "metadata.profiles"]);
        assert!(
            (none_required.iter())
                .filter(|f| f.field.starts_with("metadata"))
                .all(|f| f.requirement == Requirement::Required)
        );
    }

    #[test]
    fn a_read_is_described_by_its_release_class_cut_and_profiles() {
        let doc = json!({
            "version": "1.9.0",
            "classes": { "c": { "caption": "Network Activity" } },
            "profiles": { "host": { "caption": "Host" }, "cloud": { "caption": "Cloud" } },
        });
        let mut cut = Cut::default();
        assert_eq!(
            described(&doc, "c", &cut),
            "OCSF 1.9.0 Network Activity, 2 levels deep, required and recommended fields, profiles: none"
        );
        cut.profiles = vec!["host".into(), "cloud".into()];
        cut.depth = 3;
        assert_eq!(
            described(&doc, "c", &cut),
            "OCSF 1.9.0 Network Activity, 3 levels deep, required and recommended fields, profiles: Host, Cloud"
        );
        cut.depth = 1;
        cut.levels = vec!["required".into()];
        assert!(described(&doc, "c", &cut).contains(", 1 level deep, required fields,"));
        let unversioned = json!({ "classes": { "c": { "caption": "Network Activity" } } });
        assert!(described(&unversioned, "c", &cut).starts_with("OCSF Network Activity, 1 level deep"));
    }

    #[test]
    fn a_profile_the_class_does_not_take_is_refused() {
        let cut = |p: &str| Cut {
            profiles: vec![p.to_string()],
            ..Cut::default()
        };
        assert_eq!(
            read(&profiled(), "c", &cut("cloud")).unwrap_err(),
            "`c` takes no profile `cloud`: it takes host, container"
        );
        assert_eq!(
            read(&doc(), "authentication", &cut("host")).unwrap_err(),
            "`authentication` takes no profile `host`: it takes no profiles"
        );
    }

    #[test]
    fn a_cut_that_makes_too_many_fields_is_refused() {
        let held = json!({ "type": "object_t", "object_type": "o", "requirement": "required" });
        let attributes = json!({ "a": held, "b": held, "c": held, "d": held });
        let doc = json!({
            "classes": { "c": { "attributes": attributes } },
            "objects": { "o": { "attributes": attributes } },
        });
        let deep = Cut {
            depth: DEEPEST,
            ..Cut::default()
        };
        assert_eq!(
            read(&doc, "c", &deep).unwrap_err(),
            "the cut makes more than 10000 fields: take less depth or fewer requirements"
        );
        assert!(read(&doc, "c", &Cut::default()).is_ok());
    }
}
