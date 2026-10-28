//! A schema as JSON Schema: how a bundle declares its own (`read`), and how any schema is exported (`write`).
//!
//! `read` takes the part of JSON Schema that says what a field is: an object's `properties` and `required`, an
//! array's `items`, a `type` and the `format` of a string or an integer, `$ref` to `$defs` or `definitions`, and each
//! `description`. A type JSON Schema leaves open -- no `type`, or `anyOf`, `oneOf` or `allOf` -- holds any value. A
//! `"null"` beside another type is the other type, since any field may be null.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Map, Value, json};

use super::{Field, Leaf, Object, Objects, Schema, Type};

const DIALECT: &str = "https://json-schema.org/draft/2020-12/schema";

/// The format `write` gives a field whose value is milliseconds since the Unix epoch, and `read` takes back.
const EPOCH_MILLIS: &str = "epoch-millis";

/// The schema `id` that `doc` declares. Its root is an object.
pub fn read(id: &str, doc: &Value) -> Result<Schema, String> {
    let mut reader = Reader {
        doc,
        objects: Objects::default(),
        refs: BTreeMap::new(),
        aliases: BTreeSet::new(),
    };
    let root = match reader.ty(doc, "")? {
        (Type::Object { at, .. }, _) => at,
        _ => return Err("a schema's root is an object: `\"type\": \"object\"` with its `properties`".to_string()),
    };
    let text = |key: &str| doc.get(key).and_then(Value::as_str).map(str::to_string);
    Ok(Schema::new(
        id.to_string(),
        text("title").unwrap_or_else(|| id.to_string()),
        text("description").unwrap_or_default(),
        reader.objects.into_shared(),
        root,
    ))
}

struct Reader<'d> {
    doc: &'d Value,
    objects: Objects,
    /// Each `$ref` already read, to the object it names, so a definition naming itself ends.
    refs: BTreeMap<String, usize>,
    /// Each `$ref` to a definition that is no object, while it is read, so one that comes back to itself is refused.
    aliases: BTreeSet<String>,
}

impl Reader<'_> {
    /// The type `node` declares, and the name to show for it. `at` is where `node` is, for an error.
    fn ty(&mut self, node: &Value, at: &str) -> Result<(Type, String), String> {
        let node = match node {
            Value::Bool(_) => return Ok((Type::Leaf(Leaf::Json), "any".to_string())),
            Value::Object(node) => node,
            _ => return Err(format!("{}: a schema is an object", shown(at))),
        };
        if let Some(r) = node.get("$ref") {
            return self.reference(r, at);
        }
        let types: Vec<&str> = match node.get("type") {
            Some(Value::String(t)) => vec![t],
            Some(Value::Array(ts)) => ts.iter().filter_map(Value::as_str).filter(|t| *t != "null").collect(),
            _ if node.contains_key("properties") => vec!["object"],
            _ => Vec::new(),
        };
        let format = node.get("format").and_then(Value::as_str);
        let leaf = |leaf: Leaf, name: &str| Ok((Type::Leaf(leaf), format.unwrap_or(name).to_string()));
        match types.as_slice() {
            ["string"] if format == Some("date-time") => leaf(Leaf::DateTime, "date-time"),
            ["string"] => leaf(Leaf::Str, "string"),
            ["integer"] if format == Some(EPOCH_MILLIS) => leaf(Leaf::EpochMillis, EPOCH_MILLIS),
            ["integer"] => leaf(Leaf::Long, "integer"),
            ["number"] => leaf(Leaf::Float, "number"),
            ["boolean"] => leaf(Leaf::Bool, "boolean"),
            ["object"] => {
                let at = self.object(node, at)?;
                Ok((Type::Object { at }, "object".to_string()))
            }
            ["array"] => Err(format!("{}: an array is a field's, under `items`", shown(at))),
            // Two types at once, or none: JSON Schema leaves it open, and so does the check.
            _ => Ok((Type::Leaf(Leaf::Json), "any".to_string())),
        }
    }

    fn reference(&mut self, r: &Value, at: &str) -> Result<(Type, String), String> {
        let r = r.as_str().ok_or_else(|| format!("{}: `$ref` is a string", shown(at)))?;
        let name = r
            .strip_prefix("#/$defs/")
            .or_else(|| r.strip_prefix("#/definitions/"))
            .ok_or_else(|| {
                format!(
                    "{}: `$ref` names a definition in the same document, as `#/$defs/<name>`",
                    shown(at)
                )
            })?;
        if let Some(&object) = self.refs.get(r) {
            return Ok((Type::Object { at: object }, name.to_string()));
        }
        let doc = self.doc;
        let def = ["$defs", "definitions"]
            .iter()
            .find_map(|k| doc.get(k).and_then(|d| d.get(name)))
            .ok_or_else(|| format!("{}: no definition `{name}`", shown(at)))?;
        let is_object = def.get("type").and_then(Value::as_str) == Some("object") || def.get("properties").is_some();
        if !is_object {
            if !self.aliases.insert(r.to_string()) {
                return Err(format!("{}: `{r}` comes back to itself", shown(at)));
            }
            let read = self.ty(def, at).map(|(ty, _)| (ty, name.to_string()));
            self.aliases.remove(r);
            return read;
        }
        // Named before its fields are read, so a field under it may name it again.
        let object = self.objects.push(Object {
            name: name.to_string(),
            description: text(def, "description"),
            fields: Vec::new(),
        });
        self.refs.insert(r.to_string(), object);
        let fields = self.fields(def, at)?;
        self.objects.get_mut(object).fields = fields;
        Ok((Type::Object { at: object }, name.to_string()))
    }

    fn object(&mut self, node: &Map<String, Value>, at: &str) -> Result<usize, String> {
        let node = Value::Object(node.clone());
        let fields = self.fields(&node, at)?;
        Ok(self.objects.push(Object {
            name: String::new(),
            description: text(&node, "description"),
            fields,
        }))
    }

    fn fields(&mut self, node: &Value, at: &str) -> Result<Vec<Field>, String> {
        let required: Vec<&str> = match node.get("required") {
            Some(Value::Array(names)) => names.iter().filter_map(Value::as_str).collect(),
            _ => Vec::new(),
        };
        let Some(properties) = node.get("properties") else {
            return Ok(Vec::new());
        };
        let properties = properties
            .as_object()
            .ok_or_else(|| format!("{}: `properties` is an object", shown(at)))?;
        let mut fields = Vec::new();
        for (name, property) in properties {
            let path = if at.is_empty() {
                name.clone()
            } else {
                format!("{at}.{name}")
            };
            let items = match property.get("type") {
                Some(Value::String(t)) if t == "array" => Some(property.get("items").unwrap_or(&Value::Bool(true))),
                _ => None,
            };
            let (ty, type_name) = self.ty(items.unwrap_or(property), &path)?;
            fields.push(Field {
                name: name.clone(),
                ty,
                type_name,
                array: items.is_some(),
                required: required.contains(&name.as_str()),
                description: text(property, "description"),
            });
        }
        Ok(fields)
    }
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

/// `schema` as a JSON Schema document. Each object the schema names is a definition, and every other object is
/// written where it is.
pub fn write(schema: &Schema) -> Value {
    let mut defs = Map::new();
    let mut root = object(schema, schema.root, &mut defs);
    let root = root.as_object_mut().expect("an object is written as one");
    let mut doc = Map::new();
    doc.insert("$schema".to_string(), json!(DIALECT));
    doc.insert("title".to_string(), json!(schema.title));
    if !schema.description.is_empty() {
        doc.insert("description".to_string(), json!(schema.description));
    }
    doc.append(root);
    if !defs.is_empty() {
        doc.insert("$defs".to_string(), Value::Object(defs));
    }
    Value::Object(doc)
}

fn object(schema: &Schema, at: usize, defs: &mut Map<String, Value>) -> Value {
    let o = schema.object(at);
    let mut properties = Map::new();
    for f in &o.fields {
        let mut value = field(schema, f, defs);
        if f.array {
            value = json!({ "type": "array", "items": value });
        }
        if !f.description.is_empty() {
            value["description"] = json!(f.description);
        }
        properties.insert(f.name.clone(), value);
    }
    let mut out = json!({ "type": "object", "properties": properties });
    if !o.description.is_empty() {
        out["description"] = json!(o.description);
    }
    let required: Vec<&str> = o
        .fields
        .iter()
        .filter(|f| f.required)
        .map(|f| f.name.as_str())
        .collect();
    if !required.is_empty() {
        out["required"] = json!(required);
    }
    out
}

fn field(schema: &Schema, f: &Field, defs: &mut Map<String, Value>) -> Value {
    match f.ty {
        Type::Leaf(leaf) => leaf_type(leaf),
        // A named object is a definition.
        Type::Object { at } if schema.object(at).name.is_empty() => object(schema, at, defs),
        Type::Object { at } => {
            let name = schema.object(at).name.clone();
            if !defs.contains_key(&name) {
                // Named before it is written, so an object under it naming it again is a reference.
                defs.insert(name.clone(), Value::Null);
                let written = object(schema, at, defs);
                defs.insert(name.clone(), written);
            }
            json!({ "$ref": format!("#/$defs/{name}") })
        }
    }
}

fn leaf_type(leaf: Leaf) -> Value {
    match leaf {
        Leaf::Str => json!({ "type": "string" }),
        Leaf::Long => json!({ "type": "integer" }),
        Leaf::Float => json!({ "type": "number" }),
        Leaf::Bool => json!({ "type": "boolean" }),
        Leaf::DateTime => json!({ "type": "string", "format": "date-time" }),
        Leaf::EpochMillis => json!({ "type": "integer", "format": EPOCH_MILLIS }),
        Leaf::Json => json!({}),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::column::DataType;
    use crate::schema::Misfit;

    fn st(fields: &[(&str, DataType)]) -> DataType {
        DataType::Struct(fields.iter().map(|(k, t)| (k.to_string(), t.clone())).collect(), false)
    }

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
    fn a_json_schema_reads_as_a_schema_and_checks_a_record() {
        let schema = read("acme-login", &login()).unwrap();
        assert_eq!(schema.title, "Acme login");
        let record = st(&[
            ("at", DataType::Str),
            ("user", st(&[("manager", st(&[("name", DataType::I64)]))])),
            ("attempts", DataType::I64),
            ("score", DataType::I64),
            ("tags", DataType::Str),
            ("extra", DataType::Bytes),
        ]);
        let shown: Vec<String> = schema
            .check(&record)
            .iter()
            .map(|f| match &f.misfit {
                Misfit::Type {
                    ty,
                    type_name,
                    written_as,
                } => format!("{}: {ty} not {type_name}, write {written_as}", f.path()),
                Misfit::Missing => format!("{}: missing", f.path()),
            })
            .collect();
        assert_eq!(
            shown,
            [
                "at: str not date-time, write datetime",
                "user.manager.name: i64 not string, write str",
                "user.name: missing",
                "tags: str not string, write array(str)",
            ]
        );
    }

    #[test]
    fn what_json_schema_cannot_say_of_a_field_is_refused_with_where() {
        let refused = |doc: Value| read("x", &doc).unwrap_err();
        assert!(refused(json!({ "type": "string" })).contains("root is an object"));
        assert!(refused(json!({ "properties": { "a": { "$ref": "other.json#/x" } } })).contains("`a`"));
        assert!(refused(json!({ "properties": { "a": { "$ref": "#/$defs/none" } } })).contains("no definition `none`"));
        assert!(refused(json!({ "properties": { "a": 1 } })).contains("`a`: a schema is an object"));
        let looped = |defs: Value| refused(json!({ "properties": { "a": { "$ref": "#/$defs/x" } }, "$defs": defs }));
        assert!(looped(json!({ "x": { "$ref": "#/$defs/x" } })).contains("comes back to itself"));
        assert!(looped(json!({ "x": { "$ref": "#/$defs/y" }, "y": { "$ref": "#/$defs/x" } })).contains("comes back"));
        let twice = json!({ "properties": { "a": { "$ref": "#/$defs/s" }, "b": { "$ref": "#/$defs/s" } },
                            "$defs": { "s": { "type": "string" } } });
        assert!(read("x", &twice).is_ok(), "an alias named twice is no loop");
    }

    #[test]
    fn a_schema_written_and_read_back_is_the_same() {
        let login = read("acme-login", &login()).unwrap();
        let again = read("acme-login", &write(&login)).unwrap();
        assert_eq!(write(&again), write(&login));
        assert_eq!(again.title, login.title);
    }
}
