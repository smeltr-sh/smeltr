//! The data dictionary: the record a pipeline writes, as one `DataType`. The compiler knows every field's type but
//! the ones only a row knows -- a variant, an open object's unclaimed fields -- and a sample's output can fill those
//! in. Each system that wants a schema renders this type its own way (`io::elastic_mapping`).

use crate::core::column::DataType;
use crate::plan::plan::Plan;
use crate::plan::tree::{self, Node, Object};

/// The record `plan` writes: every field the serializer writes, so the engine's own `__` fields are left out and
/// `tag_field` is in, since any row may carry it. An object is open where it holds unclaimed fields.
pub fn record_type(plan: &Plan, tag_field: &str) -> DataType {
    let mut root = plan.tree.clone();
    root.fields.retain(|key, _| !tree::is_internal(key));
    let layout = plan.delivery_layout();
    // A row with an exception not ignored holds the tag there, and any other row what the pipeline left, so the field
    // takes either.
    let tag = tag_field.rsplit('.').fold(crate::plan::output::tag_type(), |ty, key| {
        DataType::Struct(vec![(key.to_string(), ty)], false)
    });
    meet(object_type(&root, &layout), &tag)
}

fn object_type(o: &Object, layout: &[DataType]) -> DataType {
    let fields = o.fields.iter().map(|(key, node)| {
        let ty = match node {
            Node::Leaf(slot) => layout[*slot].clone(),
            Node::Object(o) => object_type(o, layout),
        };
        (key.clone(), ty)
    });
    DataType::Struct(fields.collect(), o.unclaimed.is_some())
}

/// `record` with what the sample's output `rows` show, where only a row knows the type: a variant or a null takes
/// the type its values share, and an open object gains the keys it does not name. Values whose kinds disagree stay
/// a variant. A string is a string: nothing is read as a date.
pub fn with_sample<'a>(record: &DataType, rows: impl IntoIterator<Item = &'a serde_json::Value>) -> DataType {
    let mut seen = rows
        .into_iter()
        .fold(DataType::Null, |acc, row| meet(acc, &json_type(row)));
    if let DataType::Struct(fields, _) = &mut seen {
        fields.retain(|(key, _)| !tree::is_internal(key));
    }
    filled(record, &seen)
}

fn json_type(v: &serde_json::Value) -> DataType {
    use serde_json::Value;
    match v {
        Value::Null => DataType::Null,
        Value::Bool(_) => DataType::Bool,
        Value::Number(n) if n.is_i64() => DataType::I64,
        Value::Number(_) => DataType::F64,
        Value::String(_) => DataType::Str,
        Value::Array(items) => DataType::Array(Box::new(
            items.iter().fold(DataType::Null, |acc, e| meet(acc, &json_type(e))),
        )),
        Value::Object(fields) => {
            DataType::Struct(fields.iter().map(|(k, v)| (k.clone(), json_type(v))).collect(), false)
        }
    }
}

/// Where two types meet as the type of a value that is either: `DataType::unify`, except that a disagreement is a
/// variant at that field alone, so one field's mixed kinds do not take its object's other fields with it.
pub(crate) fn meet(a: DataType, b: &DataType) -> DataType {
    match (a, b) {
        (DataType::Struct(mut fa, oa), DataType::Struct(fb, ob)) => {
            for (key, y) in fb {
                match fa.iter_mut().find(|(k, _)| k == key) {
                    Some((_, x)) => *x = meet(std::mem::replace(x, DataType::Null), y),
                    None => fa.push((key.clone(), y.clone())),
                }
            }
            DataType::Struct(fa, oa || *ob)
        }
        (DataType::Array(x), DataType::Array(y)) => DataType::Array(Box::new(meet(*x, y))),
        (a, b) => a.unify(b).unwrap_or(DataType::Variant),
    }
}

fn filled(ty: &DataType, seen: &DataType) -> DataType {
    match (ty, seen) {
        (DataType::Variant | DataType::Null, _) => seen.clone(),
        (DataType::Array(t), DataType::Array(s)) => DataType::Array(Box::new(filled(t, s))),
        (DataType::Struct(fields, open), DataType::Struct(seen_fields, _)) => {
            let find = |key: &str| seen_fields.iter().find(|(k, _)| k == key).map(|(_, t)| t);
            let mut out: Vec<(String, DataType)> = fields
                .iter()
                .map(|(key, t)| (key.clone(), find(key).map_or_else(|| t.clone(), |s| filled(t, s))))
                .collect();
            if *open {
                let unnamed = seen_fields.iter().filter(|(k, _)| !fields.iter().any(|(n, _)| n == k));
                out.extend(unnamed.cloned());
            }
            DataType::Struct(out, *open)
        }
        _ => ty.clone(),
    }
}

/// Each leaf of `record` by its name, with its type spelled, in name order: an object's fields, then, where it is
/// open, its unclaimed fields as `name.*`, a variant.
pub fn fields(record: &DataType) -> Vec<(String, String)> {
    let mut out = Vec::new();
    leaves(record, &mut String::new(), &mut out);
    out
}

fn leaves(ty: &DataType, path: &mut String, out: &mut Vec<(String, String)>) {
    let DataType::Struct(fields, open) = ty else {
        out.push((path.clone(), ty.spell()));
        return;
    };
    let at = path.len();
    for (key, t) in fields {
        tree::push_key(path, key);
        leaves(t, path, out);
        path.truncate(at);
    }
    if *open {
        tree::push_key(path, "*");
        out.push((path.clone(), DataType::Variant.spell()));
        path.truncate(at);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn st(fields: &[(&str, DataType)], open: bool) -> DataType {
        DataType::Struct(fields.iter().map(|(k, t)| (k.to_string(), t.clone())).collect(), open)
    }

    #[test]
    fn the_sample_fills_only_what_a_row_knows() {
        let record = st(&[("a", DataType::I64), ("v", DataType::Variant)], true);
        let rows = [
            json!({"a": 1, "v": 1, "x": "s", "mix": 1, "o": {"n": 1}}),
            json!({"a": 2, "v": 2.5, "x": "t", "mix": "one", "o": {"n": 2, "m": true}}),
        ];
        let want = st(
            &[
                ("a", DataType::I64),
                ("v", DataType::F64),
                ("mix", DataType::Variant),
                ("o", st(&[("n", DataType::I64), ("m", DataType::Bool)], false)),
                ("x", DataType::Str),
            ],
            true,
        );
        assert_eq!(with_sample(&record, &rows), want);
    }

    #[test]
    fn a_closed_object_gains_no_keys_from_the_sample() {
        let record = st(&[("a", DataType::Str)], false);
        assert_eq!(with_sample(&record, &[json!({"a": "s", "b": 1})]), record);
    }

    #[test]
    fn internal_fields_of_the_sample_are_left_out() {
        let record = st(&[], true);
        assert_eq!(with_sample(&record, &[json!({"__live": true})]), record);
    }

    #[test]
    fn fields_name_every_leaf_and_the_unclaimed() {
        let record = st(
            &[
                ("a.b", DataType::Str),
                ("url", st(&[("path", DataType::Str)], true)),
                ("tags", DataType::Array(Box::new(DataType::Str))),
            ],
            false,
        );
        assert_eq!(
            fields(&record),
            vec![
                (r#"["a.b"]"#.to_string(), "str".to_string()),
                ("url.path".to_string(), "str".to_string()),
                ("url.*".to_string(), "variant".to_string()),
                ("tags".to_string(), "array<str>".to_string()),
            ]
        );
    }
}
