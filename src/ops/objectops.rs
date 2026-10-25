//! Functions over an object's keys. An object is its fields, which the compiler knows, and its unclaimed fields, one
//! variant object per row (`core::variant`), which only the row knows. The compiler types each call
//! (`plan::resolve`), so a kernel that builds values is handed their type.
//!
//! A key holding a dot is a path, as a name is: `a.b` is the key `b` inside the object at `a`.

use std::borrow::Cow;

use crate::core::column::{Column, DataType, StrBuilder, VarType};
use crate::core::ctx::Ctx;
use crate::format::tag::{self, Tag};

/// A function over an object's keys, as `IrKind::ObjectOp` carries it.
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum ObjectOp {
    /// `keys(o)`: the object's keys on each row, in the order the serializer writes them: each field that holds a
    /// value, then each of its unclaimed fields' keys that no field has.
    Keys,
    /// `entries(o)`: each key `keys` gives, with its value, as `{key, value}`.
    Entries,
    /// `fromEntries(pairs)`: an open object with no fields, of the pairs' keys and values, a later pair of a key
    /// replacing an earlier one's. A pair with a null key is left out.
    FromEntries,
    /// `omit(o, k, ...)`: `o` without those keys. The compiler lowers it to an object whose unclaimed fields are a
    /// merge, so no kernel runs it.
    Omit,
    /// The value at the key argument 1 gives in unclaimed fields, argument 0: a variant, null where the row's object
    /// has no such key.
    Get,
}

impl ObjectOp {
    /// The ones a pipeline calls by name.
    pub const NAMED: [ObjectOp; 4] = [ObjectOp::Keys, ObjectOp::Entries, ObjectOp::FromEntries, ObjectOp::Omit];

    /// The name a pipeline calls it by, where it has one.
    pub const fn name(self) -> Option<&'static str> {
        match self {
            ObjectOp::Keys => Some("keys"),
            ObjectOp::Entries => Some("entries"),
            ObjectOp::FromEntries => Some("fromEntries"),
            ObjectOp::Omit => Some("omit"),
            ObjectOp::Get => None,
        }
    }

    /// The call `name` is, where it is one of these.
    pub fn by_name(name: &str) -> Option<ObjectOp> {
        ObjectOp::NAMED.into_iter().find(|op| op.name() == Some(name))
    }

    /// What its first argument must be, as a type error spells it.
    pub const fn takes(self) -> &'static str {
        match self {
            ObjectOp::FromEntries => "array",
            _ => "object",
        }
    }

    /// Whether its first argument may be of type `ty`: an object, or for `fromEntries` an array of `{key, value}`
    /// objects whose keys are strings. Null is a `fromEntries` of nothing.
    pub fn accepts(self, ty: &DataType) -> bool {
        let entry = |e: &DataType| match e {
            DataType::Null => true,
            DataType::Struct(fields, _) => {
                let of = |n: &str| fields.iter().find(|(k, _)| k == n).map(|(_, t)| t);
                matches!(
                    (of("key"), of("value")),
                    (Some(DataType::Str | DataType::Null), Some(_))
                )
            }
            _ => false,
        };
        match self {
            ObjectOp::FromEntries => match ty {
                DataType::Null => true,
                DataType::Array(e) => entry(e),
                _ => false,
            },
            _ => matches!(ty, DataType::Struct(..)),
        }
    }
}

/// `keys(o)` over `object`, a struct column: an `array<str>` per row, its unclaimed fields' keys among them where
/// the column holds any.
pub fn keys(ctx: &mut Ctx, object: &Column) -> Column {
    entry_list(ctx, object, |_, keys, _| keys)
}

/// `entries(o)` over `object`, a struct column: an `array<{key: str, value: T}>` per row, `value_ty` being `T`. The
/// compiler met every value's type as `T` (`DataType::unify`), so each fits it.
pub fn entries(ctx: &mut Ctx, object: &Column, value_ty: &DataType) -> Column {
    entry_list(ctx, object, |ctx, keys, values| {
        let total = keys.len();
        let value_col = column_of(ctx, &values, value_ty);
        Column::struct_from_parts(vec![("key".to_string(), keys), ("value".to_string(), value_col)], total)
    })
}

/// A list per row of `object`'s entries, in `keys`' order. `child` makes the list's child from the keys, a string
/// column, and the values beside them.
fn entry_list<'a>(
    ctx: &mut Ctx,
    object: &'a Column,
    child: impl FnOnce(&mut Ctx, Column, Vec<Option<Tag<'a>>>) -> Column,
) -> Column {
    let rows = object.len();
    let per_row: Vec<Vec<(Cow<'_, str>, Tag<'_>)>> = (0..rows).map(|row| each_entry(object, row)).collect();
    let validity = valid_where(ctx, rows, |row| object.is_valid(row) || !per_row[row].is_empty());
    let total: usize = per_row.iter().map(Vec::len).sum();
    let mut keys = StrBuilder::new(ctx.pool, VarType::Str, total);
    let mut values: Vec<Option<Tag<'_>>> = Vec::with_capacity(total);
    let mut offsets = ctx.pool.take((rows + 1) * 4);
    {
        let o = offsets.as_mut_slice::<u32>();
        o[0] = 0;
        let mut at = 0u32;
        for (row, pairs) in per_row.into_iter().enumerate() {
            at += pairs.len() as u32;
            o[row + 1] = at;
            for (k, v) in pairs {
                keys.push_grow(ctx.pool, k.as_bytes());
                values.push(Some(v));
            }
        }
    }
    let child = child(ctx, keys.finish(), values);
    Column::list_from_parts(offsets, child, validity, rows)
}

/// `fromEntries(pairs)` over `pairs`, an array of `{key: str, value: T}`: a struct column with no fields and the
/// pairs as its unclaimed fields.
pub fn from_entries(ctx: &mut Ctx, pairs: &Column) -> Column {
    let rows = pairs.len();
    let objects: Vec<Option<Tag<'_>>> = match pairs.list() {
        Ok(list) => {
            let (keys, values) = (list.child.field("key"), list.child.field("value"));
            (0..rows)
                .map(|row| {
                    if !list.validity.is_valid(row) {
                        return None;
                    }
                    let writes = (list.offsets[row] as usize..list.offsets[row + 1] as usize)
                        .filter_map(|i| match keys.and_then(|k| tag::row_tag(k, i)) {
                            Some(Tag::Str(key)) => Some((key, values.and_then(|v| tag::row_tag(v, i)))),
                            _ => None,
                        })
                        .collect();
                    let mut fields = Vec::new();
                    tag::write_each(&mut fields, writes);
                    Some(Tag::Object(fields))
                })
                .collect()
        }
        Err(_) => vec![None; rows],
    };
    let unclaimed = tag::encode_column(&objects, ctx.pool);
    Column::struct_with_unclaimed_fields(Vec::new(), Some(unclaimed), rows)
}

/// The value at `key` in `objects`, unclaimed fields, on every row: a variant column.
pub fn get(ctx: &mut Ctx, objects: &Column, key: &Column) -> Column {
    let rows = objects.len();
    let mut b = StrBuilder::new(ctx.pool, VarType::Variant, rows);
    let mut buf = Vec::new();
    for row in 0..rows {
        let found = match (tag::row_tag(objects, row), tag::row_tag(key, row)) {
            (Some(Tag::Object(fields)), Some(Tag::Str(key))) => {
                fields.iter().rev().find(|(k, _)| *k == key).map(|(_, v)| v.clone())
            }
            _ => None,
        };
        match found {
            Some(v) => {
                buf.clear();
                tag::encode(&v, &mut buf);
                b.push_grow(ctx.pool, &buf);
            }
            None => b.push_null(),
        }
    }
    b.finish()
}

/// The keys and values of `object`'s row `row`, in `keys`' order.
fn each_entry(object: &Column, row: usize) -> Vec<(Cow<'_, str>, Tag<'_>)> {
    let DataType::Struct(fields, _) = object.data_type() else {
        return Vec::new();
    };
    let mut out: Vec<(Cow<'_, str>, Tag<'_>)> = Vec::new();
    for (name, _) in &fields {
        if let Some(v) = object.field(name).and_then(|f| tag::row_tag(f, row)) {
            out.push((Cow::Owned(name.clone()), v));
        }
    }
    if let Some(Tag::Object(unclaimed)) = object.unclaimed_fields().and_then(|u| tag::row_tag(u, row)) {
        out.extend(
            unclaimed
                .into_iter()
                .filter(|(k, _)| !fields.iter().any(|(n, _)| n == k)),
        );
    }
    out
}

/// A column of `ty` holding `values`, each of which fits it.
fn column_of(ctx: &mut Ctx, values: &[Option<Tag<'_>>], ty: &DataType) -> Column {
    match ty {
        DataType::Variant => tag::encode_column(values, ctx.pool),
        DataType::Null => Column::empty(DataType::Null, values.len()),
        ty => {
            let cells = values.iter().map(|v| v.as_ref().map(crate::format::typed::Cell::Tag));
            crate::format::typed::build(cells, values.len(), ty, ctx.now_micros, ctx.pool).0
        }
    }
}

/// A validity of `rows` rows, set where `valid` says.
fn valid_where(ctx: &mut Ctx, rows: usize, valid: impl Fn(usize) -> bool) -> crate::core::column::Validity {
    let mut v = crate::core::column::Validity::alloc_null(ctx.pool, rows);
    for row in (0..rows).filter(|&r| valid(r)) {
        v.set_valid(row, true);
    }
    v
}
