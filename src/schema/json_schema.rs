//! A schema as JSON Schema: how one is read in (`read`), and how any schema is exported (`write`).
//!
//! `read` takes the part of JSON Schema that says what a field is: an object's `properties` and `required`, an
//! array's `items`, a `type` and the `format` of a string, `$ref` to `$defs` or `definitions`, and each
//! `description`. A type JSON Schema leaves open -- no `type`, or `anyOf`, `oneOf` or `allOf` -- is a `variant`,
//! and so is an object named from within itself, or deeper than `NESTED`. A `"null"` beside another type is the other
//! type, since any field may be null.

use serde_json::{Map, Value, json};

use super::{Field, Node, Schema};
use crate::core::column::DataType;

const DIALECT: &str = "https://json-schema.org/draft/2020-12/schema";

/// How deep `read` follows objects before it takes one whole, as a `variant`.
const NESTED: usize = 64;

/// The schema `id` that `doc` declares. Its root is an object.
pub fn read(id: &str, doc: &Value) -> Result<Schema, String> {
    let root = doc
        .as_object()
        .filter(|node| is_object(node))
        .ok_or_else(|| "a schema's root is an object: `\"type\": \"object\"` with its `properties`".to_string())?;
    let mut reader = Reader {
        doc,
        fields: Vec::new(),
        within: Vec::new(),
    };
    reader.fields_of(root, "")?;
    let text = |key: &str| doc.get(key).and_then(Value::as_str).map(str::to_string);
    Ok(Schema {
        id: id.to_string(),
        title: text("title").unwrap_or_else(|| id.to_string()),
        description: text("description").unwrap_or_default(),
        fields: reader.fields,
    })
}

struct Reader<'d> {
    doc: &'d Value,
    fields: Vec<Field>,
    /// Each definition being read, outermost first, so one naming itself ends.
    within: Vec<String>,
}

/// What a node declares: its type, the object it holds where its fields are rows of their own (the node itself, or
/// an array's items), and how many definitions reading it entered, which stay entered until its fields are read.
struct Typed<'d> {
    ty: DataType,
    holds: Option<&'d Map<String, Value>>,
    entered: usize,
}

impl<'d> Reader<'d> {
    /// A row for each property of `node`, under `prefix`, and for what each holds.
    fn fields_of(&mut self, node: &'d Map<String, Value>, prefix: &str) -> Result<(), String> {
        let required: Vec<&str> = match node.get("required") {
            Some(Value::Array(names)) => names.iter().filter_map(Value::as_str).collect(),
            _ => Vec::new(),
        };
        let Some(properties) = node.get("properties") else {
            return Ok(());
        };
        let properties = properties
            .as_object()
            .ok_or_else(|| format!("{}: `properties` is an object", shown(prefix)))?;
        for (name, property) in properties {
            let path = if prefix.is_empty() {
                name.clone()
            } else {
                format!("{prefix}.{name}")
            };
            let typed = self.ty(property, &path)?;
            self.fields.push(Field {
                path: path.clone(),
                ty: typed.ty,
                required: required.contains(&name.as_str()),
                description: text(property, "description"),
            });
            let read = match typed.holds {
                Some(object) => self.fields_of(object, &path),
                None => Ok(()),
            };
            self.within.truncate(self.within.len() - typed.entered);
            read?;
        }
        Ok(())
    }

    /// The type `node` declares at `path`.
    fn ty(&mut self, node: &'d Value, path: &str) -> Result<Typed<'d>, String> {
        let leaf = |ty| Typed {
            ty,
            holds: None,
            entered: 0,
        };
        let node = match node {
            Value::Bool(_) => return Ok(leaf(DataType::Variant)),
            Value::Object(node) => node,
            _ => return Err(format!("{}: a schema is an object", shown(path))),
        };
        if let Some(r) = node.get("$ref") {
            return self.reference(r, path);
        }
        let types: Vec<&str> = match node.get("type") {
            Some(Value::String(t)) => vec![t],
            Some(Value::Array(ts)) => ts.iter().filter_map(Value::as_str).filter(|t| *t != "null").collect(),
            _ if node.contains_key("properties") => vec!["object"],
            _ => Vec::new(),
        };
        let format = node.get("format").and_then(Value::as_str);
        Ok(match types.as_slice() {
            ["string"] if format == Some("date-time") => leaf(DataType::DateTime),
            ["string"] => leaf(DataType::Str),
            ["integer"] => leaf(DataType::I64),
            ["number"] => leaf(DataType::F64),
            ["boolean"] => leaf(DataType::Bool),
            ["object"] => Typed {
                ty: DataType::Struct(Vec::new(), true),
                holds: Some(node),
                entered: 0,
            },
            ["array"] => {
                let element = self.ty(node.get("items").unwrap_or(&Value::Bool(true)), path)?;
                Typed {
                    ty: DataType::Array(Box::new(element.ty)),
                    ..element
                }
            }
            // Two types at once, or none: JSON Schema leaves it open, and so does the check.
            _ => leaf(DataType::Variant),
        })
    }

    fn reference(&mut self, r: &Value, path: &str) -> Result<Typed<'d>, String> {
        let r = r
            .as_str()
            .ok_or_else(|| format!("{}: `$ref` is a string", shown(path)))?;
        let name = r
            .strip_prefix("#/$defs/")
            .or_else(|| r.strip_prefix("#/definitions/"))
            .ok_or_else(|| {
                format!(
                    "{}: `$ref` names a definition in the same document, as `#/$defs/<name>`",
                    shown(path)
                )
            })?;
        let def = ["$defs", "definitions"]
            .iter()
            .find_map(|k| self.doc.get(k).and_then(|d| d.get(name)))
            .ok_or_else(|| format!("{}: no definition `{name}`", shown(path)))?;
        // A definition within itself, or past `NESTED`, is one field whose shape only the row knows.
        if self.within.iter().any(|w| w == r) || self.within.len() >= NESTED {
            return Ok(Typed {
                ty: DataType::Variant,
                holds: None,
                entered: 0,
            });
        }
        self.within.push(r.to_string());
        let typed = self.ty(def, path);
        match typed {
            Ok(typed) => Ok(Typed {
                entered: typed.entered + 1,
                ..typed
            }),
            Err(e) => {
                self.within.pop();
                Err(e)
            }
        }
    }
}

/// Whether `node` declares an object: by type, or by having properties.
fn is_object(node: &Map<String, Value>) -> bool {
    node.get("type").and_then(Value::as_str) == Some("object") || node.contains_key("properties")
}

fn text(node: &Value, key: &str) -> String {
    node.get(key).and_then(Value::as_str).unwrap_or_default().to_string()
}

fn shown(at: &str) -> String {
    match at {
        "" => "the schema".to_string(),
        at => format!("`{at}`"),
    }
}

/// `schema` as a JSON Schema document, each object written where it is.
pub fn write(schema: &Schema) -> Value {
    let root = schema.tree();
    let mut doc = Map::new();
    doc.insert("$schema".to_string(), json!(DIALECT));
    doc.insert("title".to_string(), json!(schema.title));
    if !schema.description.is_empty() {
        doc.insert("description".to_string(), json!(schema.description));
    }
    if let Value::Object(object) = root.object() {
        doc.extend(object);
    }
    Value::Object(doc)
}

impl Node<'_> {
    /// The node as the object its children are the properties of.
    fn object(&self) -> Value {
        let mut properties = Map::new();
        let mut required = Vec::new();
        for (key, child) in &self.children {
            let mut value = child.value();
            if let Some(f) = child.field {
                if !f.description.is_empty() {
                    value["description"] = json!(f.description);
                }
                if f.required {
                    required.push(key.clone());
                }
            }
            properties.insert(key.clone(), value);
        }
        let mut out = json!({ "type": "object", "properties": properties });
        if !required.is_empty() {
            out["required"] = json!(required);
        }
        out
    }

    /// The node's own value: its field's type, with its children as the object's properties where it holds one.
    fn value(&self) -> Value {
        match self.field.map(|f| &f.ty) {
            // A key only children name is an object.
            None => self.object(),
            Some(ty) => self.typed(ty),
        }
    }

    fn typed(&self, ty: &DataType) -> Value {
        match ty {
            DataType::Struct(..) => self.object(),
            DataType::Array(element) => json!({ "type": "array", "items": self.typed(element) }),
            DataType::Str => json!({ "type": "string" }),
            DataType::DateTime => json!({ "type": "string", "format": "date-time" }),
            DataType::I64 => json!({ "type": "integer" }),
            DataType::Duration => json!({ "type": "integer", "description": "microseconds" }),
            DataType::F64 => json!({ "type": "number" }),
            DataType::Bool => json!({ "type": "boolean" }),
            DataType::Bytes => json!({ "type": "string", "contentEncoding": "base64" }),
            DataType::Variant | DataType::Null => json!({}),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::to_dto;

    fn login() -> Value {
        json!({
            "title": "Acme login",
            "description": "One sign-in to an Acme application.",
            "type": "object",
            "required": ["at", "user"],
            "properties": {
                "at": { "type": "string", "format": "date-time", "description": "When it happened." },
                "user": { "$ref": "#/$defs/user" },
                "manager": { "$ref": "#/$defs/user" },
                "attempts": { "type": ["integer", "null"] },
                "score": { "type": "number" },
                "tags": { "type": "array", "items": { "type": "string" } },
                "observables": { "type": "array", "items": { "type": "object", "properties": { "name": { "type": "string" } } } },
                "extra": {},
            },
            "$defs": {
                "user": {
                    "type": "object",
                    "required": ["name"],
                    "properties": {
                        "name": { "type": "string" },
                        "manager": { "$ref": "#/$defs/user" },
                    },
                },
            },
        })
    }

    #[test]
    fn a_json_schema_reads_as_a_table_of_fields() {
        let schema = read("acme-login", &login()).unwrap();
        assert_eq!(schema.title, "Acme login");
        let rows: Vec<String> = (to_dto(&schema).fields.iter())
            .map(|r| format!("{} {}{}", r.field, r.ty, if r.required { " required" } else { "" }))
            .collect();
        assert_eq!(
            rows,
            [
                "at datetime required",
                "attempts i64",
                "extra variant",
                "manager {...}",
                "manager.manager variant",
                "manager.name str required",
                "observables array<{...}>",
                "observables.name str",
                "score f64",
                "tags array<str>",
                "user {...} required",
                "user.manager variant",
                "user.name str required",
            ],
            "properties come by key, and an object within itself is one variant field"
        );
        assert_eq!(schema.fields[0].description, "When it happened.");
    }

    #[test]
    fn what_json_schema_cannot_say_of_a_field_is_refused_with_where() {
        let refused = |doc: Value| read("x", &doc).unwrap_err();
        assert!(refused(json!({ "type": "string" })).contains("root is an object"));
        assert!(refused(json!({ "properties": { "a": { "$ref": "other.json#/x" } } })).contains("`a`"));
        assert!(refused(json!({ "properties": { "a": { "$ref": "#/$defs/none" } } })).contains("no definition `none`"));
        assert!(refused(json!({ "properties": { "a": 1 } })).contains("`a`: a schema is an object"));
        let looped = |defs: Value| {
            read(
                "x",
                &json!({ "properties": { "a": { "$ref": "#/$defs/x" } }, "$defs": defs }),
            )
        };
        let rows = |s: Schema| s.fields.iter().map(|f| f.ty.spell()).collect::<Vec<_>>();
        assert_eq!(
            rows(looped(json!({ "x": { "$ref": "#/$defs/x" } })).unwrap()),
            ["variant"]
        );
        assert_eq!(
            rows(looped(json!({ "x": { "$ref": "#/$defs/y" }, "y": { "$ref": "#/$defs/x" } })).unwrap()),
            ["variant"],
            "a definition that comes back to itself is a variant"
        );
    }

    #[test]
    fn a_schema_written_and_read_back_is_the_same() {
        let login = read("acme-login", &login()).unwrap();
        let again = read("acme-login", &write(&login)).unwrap();
        assert_eq!(to_dto(&again), to_dto(&login));
        let doc = write(&login);
        assert_eq!(doc["properties"]["user"]["required"], json!(["name"]));
        assert_eq!(doc["properties"]["observables"]["items"]["type"], "object");
        assert_eq!(doc["properties"]["at"]["description"], "When it happened.");
    }
}
