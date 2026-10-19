//! `Tag`: one value as a parser decoded it, and the two ways a parser's values become a column.
//!
//! A declared field is written as its type (`typed::TypedBuilder`), and that builder is also what a declaration
//! downstream of the parser runs on a variant (`decode_column`), so a declared type means the same thing wherever it
//! is declared. The keys nothing claims are encoded as they arrived, one variant object per row (`encode_column`,
//! `core::variant`): the unclaimed fields.

use std::borrow::{BorrowMut, Cow};

use crate::core::bits;
use crate::core::buffer::BufferPool;
use crate::core::column::{Column, DataType, StrBuilder, VarType};
use crate::core::keys::Keys;
use crate::core::tally::TagCounts;
use crate::core::variant::{self, Value};
use crate::format::typed::Failed;
use crate::ops::timeparse::{is_epoch_count, reads_as_time};

/// One parsed value. `Str` borrows from the input when the scanner found nothing to unescape.
///
/// `Object` is an object a parser or an operator builds whole, such as the keys nothing claims, nested back into one
/// object per row.
#[derive(Clone, Debug, PartialEq)]
pub enum Tag<'d> {
    Bool(bool),
    I64(i64),
    F64(f64),
    Str(Cow<'d, str>),
    /// `None` is a JSON `null`, which holds its place.
    Array(Vec<Option<Tag<'d>>>),
    /// In encounter order. A `null`-valued key contributes no entry (null == missing).
    Object(Vec<(Cow<'d, str>, Tag<'d>)>),
    /// Microseconds since the epoch.
    DateTime(i64),
    /// Microseconds.
    Duration(i64),
    Bytes(Cow<'d, [u8]>),
}

/// An object's keys and values, in the order the record had them.
pub type Fields<'d> = Vec<(Cow<'d, str>, Tag<'d>)>;

/// A value written piece by piece as a walk reads it: into its encoding (`Bytes`), or into a declared column
/// (`typed::Slot`). A container's members arrive between its begin and its end, each through the
/// child its emitter hands out for it.
pub trait Emit {
    type Child<'a>: Emit
    where
        Self: 'a;
    fn scalar(&mut self, tag: &Tag<'_>);
    /// A null element of an array, which holds its place.
    fn null(&mut self);
    fn begin_array(&mut self);
    /// Where the next element goes.
    fn element(&mut self) -> Self::Child<'_>;
    fn end_array(&mut self, count: usize);
    fn begin_object(&mut self);
    /// Where `key`'s value goes.
    fn field(&mut self, key: &str) -> Self::Child<'_>;
    /// The field opened last held nothing, so the object has no entry for it.
    fn unfield(&mut self);
    /// `count` distinct keys. Where `repeats`, a key came more than once: its last value stands, in its first place.
    fn end_object(&mut self, count: usize, repeats: bool);
}

/// The encoding (`core::variant`) of a value, appended to `out`: a buffer, or a borrow of one.
pub struct Bytes<B> {
    pub out: B,
    /// Where the container being written began.
    open: variant::Open,
    /// Where its field opened last began.
    mark: usize,
}

impl<B: BorrowMut<Vec<u8>>> Bytes<B> {
    pub fn new(out: B) -> Self {
        Bytes {
            out,
            open: variant::Open::default(),
            mark: 0,
        }
    }
}

impl<B: BorrowMut<Vec<u8>>> Emit for Bytes<B> {
    type Child<'a>
        = Bytes<&'a mut Vec<u8>>
    where
        Self: 'a;

    #[inline]
    fn scalar(&mut self, tag: &Tag<'_>) {
        encode(tag, self.out.borrow_mut());
    }

    fn null(&mut self) {
        variant::put_null(self.out.borrow_mut());
    }

    fn begin_array(&mut self) {
        self.open = variant::begin_array(self.out.borrow_mut());
    }

    #[inline]
    fn element(&mut self) -> Bytes<&mut Vec<u8>> {
        Bytes::new(self.out.borrow_mut())
    }

    fn end_array(&mut self, count: usize) {
        variant::end(self.out.borrow_mut(), self.open, count);
    }

    fn begin_object(&mut self) {
        self.open = variant::begin_object(self.out.borrow_mut());
    }

    #[inline]
    fn field(&mut self, key: &str) -> Bytes<&mut Vec<u8>> {
        let out = self.out.borrow_mut();
        self.mark = out.len();
        variant::put_key(out, key);
        Bytes::new(out)
    }

    fn unfield(&mut self) {
        self.out.borrow_mut().truncate(self.mark);
    }

    fn end_object(&mut self, count: usize, repeats: bool) {
        let out = self.out.borrow_mut();
        let count = if repeats {
            variant::dedupe(out, self.open)
        } else {
            count
        };
        variant::end(out, self.open, count);
    }
}

/// Write the value `b` encodes (`core::variant`) into `out`. A null writes nothing.
pub fn emit(b: &[u8], out: &mut impl Emit) {
    match variant::read(b) {
        Value::Null => {}
        Value::Array(items) => {
            let count = items.len();
            out.begin_array();
            for e in items {
                let mut child = out.element();
                match variant::read(e) {
                    Value::Null => child.null(),
                    _ => emit(e, &mut child),
                }
            }
            out.end_array(count);
        }
        Value::Object(entries) => {
            let count = entries.len();
            out.begin_object();
            for (key, value) in entries {
                emit(value, &mut out.field(key));
            }
            out.end_object(count, false);
        }
        _ => out.scalar(&decode(b).expect("a scalar")),
    }
}

/// A variant column of `values`, null where a row holds none.
pub fn encode_column(values: &[Option<Tag<'_>>], pool: &mut BufferPool) -> Column {
    let mut b = StrBuilder::new(pool, VarType::Variant, values.len());
    let mut buf = Vec::new();
    for v in values {
        match v {
            Some(tag) => {
                buf.clear();
                encode(tag, &mut buf);
                b.push_grow(pool, &buf);
            }
            None => b.push_null(),
        }
    }
    b.finish()
}

/// Append `tag`'s encoding (`core::variant`). An object's repeated key keeps its last value, in the first one's
/// place.
#[inline]
pub fn encode(tag: &Tag<'_>, out: &mut Vec<u8>) {
    match tag {
        Tag::Bool(v) => variant::put_bool(out, *v),
        Tag::I64(v) => variant::put_i64(out, *v),
        Tag::F64(v) => variant::put_f64(out, *v),
        Tag::Str(s) => variant::put_str(out, s),
        Tag::DateTime(v) => variant::put_datetime(out, *v),
        Tag::Duration(v) => variant::put_duration(out, *v),
        Tag::Bytes(b) => variant::put_bytes(out, b),
        Tag::Array(_) | Tag::Object(_) => encode_container(tag, out),
    }
}

/// `encode` for an array or an object, out of line: `encode` inlines into the walk's loops for the scalars.
fn encode_container(tag: &Tag<'_>, out: &mut Vec<u8>) {
    match tag {
        Tag::Array(elements) => {
            let open = variant::begin_array(out);
            for e in elements {
                match e {
                    Some(e) => encode(e, out),
                    None => variant::put_null(out),
                }
            }
            variant::end(out, open, elements.len());
        }
        Tag::Object(fields) => {
            let open = variant::begin_object(out);
            for (key, value) in fields {
                variant::put_key(out, key);
                encode(value, out);
            }
            let count = if repeats(fields) {
                variant::dedupe(out, open)
            } else {
                fields.len()
            };
            variant::end(out, open, count);
        }
        _ => unreachable!("a container"),
    }
}

/// Whether a key of `fields` comes more than once.
fn repeats(fields: &[(Cow<'_, str>, Tag<'_>)]) -> bool {
    let mut keys = Keys::default();
    fields.iter().any(|(key, _)| keys.insert(key.as_ref(), ()).is_err())
}

/// The value `b` holds, as a tag borrowing its text. `None` for a null.
pub fn decode(b: &[u8]) -> Option<Tag<'_>> {
    Some(match variant::read(b) {
        Value::Null => return None,
        Value::Bool(v) => Tag::Bool(v),
        Value::I64(v) => Tag::I64(v),
        Value::F64(v) => Tag::F64(v),
        Value::Str(s) => Tag::Str(Cow::Borrowed(s)),
        Value::DateTime(v) => Tag::DateTime(v),
        Value::Duration(v) => Tag::Duration(v),
        Value::Bytes(b) => Tag::Bytes(Cow::Borrowed(b)),
        Value::Array(items) => Tag::Array(items.map(decode).collect()),
        Value::Object(entries) => Tag::Object(
            entries
                .filter_map(|(k, v)| decode(v).map(|v| (Cow::Borrowed(k), v)))
                .collect(),
        ),
    })
}

/// A variant column decoded to `ty` by the declared-type table (`typed::TypedBuilder`): each row that does not fit is
/// null and returned. A time read with no year takes it from `now`.
pub fn decode_column(col: &Column, ty: &DataType, now: i64, pool: &mut BufferPool) -> (Column, Vec<Failed>) {
    use crate::core::column::Scalar;
    use crate::format::typed::Cell;
    let rows = col.len();
    let one = match col.constant_value() {
        Some(Scalar::Variant(value)) => Some(value.as_slice()),
        _ => None,
    };
    let values = (0..rows).map(|row| match (one, col.variant()) {
        (Some(value), _) => Some(Cell::Variant(value)),
        (None, Ok(v)) if v.validity.is_valid(row) => Some(Cell::Variant(v.get(row))),
        _ => None,
    });
    crate::format::typed::build(values, rows, ty, now, pool)
}

/// Any column as a variant column: each row's value, of its own type. A struct's null field is no key.
pub fn to_variant(col: &Column, pool: &mut BufferPool) -> Column {
    if let Some(value @ crate::core::column::Scalar::Variant(_)) = col.constant_value() {
        return Column::filled(pool, value, col.len());
    }
    if col.data_type() == DataType::Variant {
        return col.deep_copy(pool);
    }
    let values: Vec<Option<Tag<'_>>> = (0..col.len()).map(|row| row_tag(col, row)).collect();
    encode_column(&values, pool)
}

/// Row `row` of `col` as a tag. `None` for a null.
pub fn row_tag(col: &Column, row: usize) -> Option<Tag<'_>> {
    use crate::core::column::Scalar;
    if col.is_unbound() {
        return None;
    }
    if let Some(value) = col.constant_value() {
        return Some(match value {
            Scalar::Bool(v) => Tag::Bool(*v),
            Scalar::I64(v) => Tag::I64(*v),
            Scalar::F64(v) => Tag::F64(*v),
            Scalar::Str(v) => Tag::Str(Cow::Borrowed(v)),
            Scalar::Bytes(v) => Tag::Bytes(Cow::Borrowed(v)),
            Scalar::DateTime(v) => Tag::DateTime(*v),
            Scalar::Duration(v) => Tag::Duration(*v),
            Scalar::Variant(v) => return decode(v),
        });
    }
    if !col.is_valid(row) {
        return None;
    }
    Some(match col.data_type() {
        DataType::Null => return None,
        DataType::Bool => Tag::Bool(bits::get_row(col.bool().ok()?.values, row)),
        DataType::I64 => Tag::I64(col.i64().ok()?.values[row]),
        DataType::F64 => Tag::F64(col.f64().ok()?.values[row]),
        DataType::Duration => Tag::Duration(col.duration().ok()?.values[row]),
        DataType::DateTime => Tag::DateTime(col.datetime().ok()?.values[row]),
        DataType::Str => Tag::Str(Cow::Borrowed(
            std::str::from_utf8(col.var().ok()?.get(row)).expect("a str column is UTF-8"),
        )),
        DataType::Bytes => Tag::Bytes(Cow::Borrowed(col.var().ok()?.get(row))),
        DataType::Variant => return decode(col.variant().ok()?.get(row)),
        DataType::Array(_) => {
            let list = col.list().ok()?;
            let (lo, hi) = (list.offsets[row] as usize, list.offsets[row + 1] as usize);
            Tag::Array((lo..hi).map(|i| row_tag(list.child, i)).collect())
        }
        DataType::Struct(fields, _) => {
            let mut object = Vec::new();
            for (name, _) in &fields {
                if let Some(value) = col.field(name).and_then(|f| row_tag(f, row)) {
                    object.push((Cow::Owned(name.to_string()), value));
                }
            }
            // The unclaimed fields follow the declared ones; a key a field holds is the field's.
            if let Some(Tag::Object(unclaimed_fields)) = col.unclaimed_fields().and_then(|u| row_tag(u, row)) {
                lay_beneath(&mut object, unclaimed_fields);
            }
            if object.is_empty() {
                return None;
            }
            Tag::Object(object)
        }
    })
}

/// `unclaimed_fields` laid beneath `object`'s keys: a key `object` holds is its own, and the rest follow it.
fn lay_beneath<'d>(object: &mut Fields<'d>, unclaimed_fields: Fields<'d>) {
    let keys: Keys<_, _> = object.drain(..).chain(unclaimed_fields).collect();
    object.extend(keys);
}

/// The keys of `col`, a struct, that `names` does not name, its own unclaimed fields among them: a variant object per
/// row. `None` where no row has any.
pub fn unclaimed_fields_beyond(col: &Column, names: &[(String, DataType)], pool: &mut BufferPool) -> Option<Column> {
    let DataType::Struct(fields, _) = col.data_type() else {
        return None;
    };
    let named = |k: &str| names.iter().any(|(m, _)| m == k);
    let left: Vec<&String> = fields.iter().map(|(n, _)| n).filter(|n| !named(n)).collect();
    let unclaimed = col.unclaimed_fields();
    let names_unclaimed = unclaimed.is_some_and(|u| {
        u.variant().is_ok_and(|v| {
            (0..u.len())
                .any(|r| v.validity.is_valid(r) && names.iter().any(|(n, _)| variant::member(v.get(r), n).is_some()))
        })
    });
    if left.is_empty() && !names_unclaimed {
        return unclaimed.map(|u| u.deep_copy(pool));
    }
    let values: Vec<Option<Tag<'_>>> = (0..col.len())
        .map(|row| {
            let mut object = Vec::new();
            for name in &left {
                if let Some(value) = col.field(name).and_then(|f| row_tag(f, row)) {
                    object.push((Cow::Owned(name.to_string()), value));
                }
            }
            if let Some(Tag::Object(mut unclaimed_fields)) = unclaimed.and_then(|u| row_tag(u, row)) {
                unclaimed_fields.retain(|(k, _)| !named(k));
                lay_beneath(&mut object, unclaimed_fields);
            }
            (!object.is_empty()).then_some(Tag::Object(object))
        })
        .collect();
    values.iter().any(Option::is_some).then(|| encode_column(&values, pool))
}

/// A scalar's text, as `toString` renders it.
pub(crate) fn stringify_scalar<'a>(tag: &'a Tag<'_>) -> Cow<'a, str> {
    match tag {
        Tag::Bool(b) => Cow::Borrowed(if *b { "true" } else { "false" }),
        Tag::I64(i) | Tag::Duration(i) => Cow::Owned(i.to_string()),
        Tag::F64(f) => Cow::Owned(f.to_string()),
        Tag::Str(s) => Cow::Borrowed(s),
        Tag::DateTime(t) => {
            let mut out = Vec::new();
            crate::ops::datetime::write_iso8601_micros_bare(*t, &mut out);
            Cow::Owned(String::from_utf8(out).expect("ASCII"))
        }
        Tag::Bytes(b) => Cow::Owned(crate::format::bytes_text::BytesForm::Base64.text(b)),
        Tag::Array(_) | Tag::Object(_) => unreachable!("a scalar"),
    }
}

/// The encoded container `b` as JSON text.
pub(crate) fn json_text(b: &[u8]) -> String {
    let mut out = Vec::new();
    if !crate::format::json::write_variant(b, &mut out) {
        out.extend_from_slice(b"{}");
    }
    String::from_utf8(out).expect("JSON text")
}

/// What a preview proposes to declare a field as, from the values it held: one kind is that kind's type, integers
/// beside floats are `f64`, an array proposes its elements' own suggestion, and any other mix is `str`. A suggestion
/// narrows nothing: an undeclared key stays in the unclaimed fields as it arrived.
#[derive(Default)]
pub struct Suggestion {
    counts: TagCounts,
    elements: Option<Box<Suggestion>>,
    /// The time shape the last value read as, tried first on the next.
    hint: usize,
}

impl Suggestion {
    pub fn saw(&mut self, tag: &Tag<'_>) {
        let c = &mut self.counts;
        c.present += 1;
        match tag {
            Tag::Bool(_) => c.bools += 1,
            Tag::I64(i) => {
                c.ints += 1;
                c.times += u32::from(is_epoch_count(&i.to_string()));
            }
            // A span as JSON writes it.
            Tag::Duration(_) => c.ints += 1,
            Tag::F64(f) => {
                c.floats += 1;
                c.times += u32::from(is_epoch_count(&f.to_string()));
            }
            Tag::Str(s) => {
                c.strings += 1;
                c.times += u32::from(reads_as_time(s, &mut self.hint));
            }
            // A time and bytes as JSON writes them.
            Tag::DateTime(_) => {
                c.strings += 1;
                c.times += 1;
            }
            Tag::Bytes(_) => c.strings += 1,
            Tag::Object(_) => c.objects += 1,
            Tag::Array(elements) => {
                c.arrays += 1;
                let inner = self.elements.get_or_insert_with(Box::default);
                for e in elements.iter().flatten() {
                    inner.saw(e);
                }
            }
        }
    }

    pub fn counts(&self) -> TagCounts {
        self.counts
    }

    pub fn suggest(&self) -> DataType {
        let c = &self.counts;
        match c.kinds() {
            0 => DataType::Null,
            1 if c.bools > 0 => DataType::Bool,
            1 if c.ints > 0 => DataType::I64,
            1 if c.floats > 0 => DataType::F64,
            1 if c.strings > 0 => DataType::Str,
            1 if c.arrays > 0 => {
                DataType::Array(Box::new(self.elements.as_ref().map_or(DataType::Null, |e| e.suggest())))
            }
            1 => DataType::Struct(Vec::new(), false),
            2 if c.ints > 0 && c.floats > 0 => DataType::F64,
            _ => DataType::Str,
        }
    }
}

/// Each of `writes` laid over `fields` in turn: its key goes from where it was, and a value lands at the end, where
/// nothing takes the key away. The same as one write at a time, in time linear in their number.
pub fn write_each<'d>(fields: &mut Vec<(Cow<'d, str>, Tag<'d>)>, writes: Vec<(Cow<'d, str>, Option<Tag<'d>>)>) {
    let mut last = Keys::default();
    for (key, value) in writes {
        if let Some(i) = last.find(&key) {
            last.take(i);
        }
        last.push(key, value);
    }
    fields.retain(|(k, _)| last.find(k).is_none());
    fields.extend(last.into_iter().filter_map(|(k, v)| Some((k, v?))));
}

#[cfg(test)]
mod tests {
    use super::*;

    fn object_tag<'d>(fields: Vec<(&'d str, Tag<'d>)>) -> Tag<'d> {
        Tag::Object(fields.into_iter().map(|(k, v)| (Cow::Borrowed(k), v)).collect())
    }

    #[test]
    fn a_value_is_encoded_as_it_arrived_and_decodes_back() {
        let tag = object_tag(vec![
            ("n", Tag::I64(6)),
            ("f", Tag::F64(6.0)),
            (
                "xs",
                Tag::Array(vec![Some(Tag::Str("a".into())), None, Some(Tag::Bool(false))]),
            ),
            ("o", object_tag(vec![("k", Tag::Str("v".into()))])),
        ]);
        let mut buf = Vec::new();
        encode(&tag, &mut buf);
        assert_eq!(
            decode(&buf),
            Some(tag),
            "an integer stays an integer and a float a float"
        );
    }

    #[test]
    fn a_repeated_key_keeps_its_last_value_in_its_first_place() {
        let tag = object_tag(vec![("a", Tag::I64(1)), ("b", Tag::I64(2)), ("a", Tag::I64(3))]);
        let mut buf = Vec::new();
        encode(&tag, &mut buf);
        assert_eq!(
            decode(&buf),
            Some(object_tag(vec![("a", Tag::I64(3)), ("b", Tag::I64(2))]))
        );
    }

    #[test]
    fn rows_that_disagree_each_keep_their_own_value() {
        let mut pool = BufferPool::new();
        let values = [
            Some(Tag::I64(5)),
            Some(Tag::Str("five".into())),
            None,
            Some(object_tag(vec![("x", Tag::I64(1))])),
        ];
        let col = encode_column(&values, &mut pool);
        assert_eq!(col.data_type(), DataType::Variant);
        let v = col.variant().unwrap();
        assert_eq!(decode(v.get(0)), Some(Tag::I64(5)));
        assert_eq!(decode(v.get(1)), Some(Tag::Str("five".into())));
        assert!(!v.validity.is_valid(2), "a row nothing set is null");
        assert_eq!(decode(v.get(3)), Some(object_tag(vec![("x", Tag::I64(1))])));
        col.reclaim(&mut pool);
    }

    #[test]
    fn a_declaration_downstream_decodes_as_the_parser_would() {
        let mut pool = BufferPool::new();
        let values = [
            Some(Tag::I64(1)),
            Some(Tag::Str("40".into())),
            Some(Tag::Str("nope".into())),
        ];
        let col = encode_column(&values, &mut pool);
        let (out, failed) = decode_column(&col, &DataType::I64, 0, &mut pool);
        assert_eq!(failed, vec![(2, Some("\"nope\"".into()))]);
        let f = out.i64().unwrap();
        assert_eq!(&f.values[..2], &[1, 40]);
        assert!(!f.validity.is_valid(2));
        out.reclaim(&mut pool);
        col.reclaim(&mut pool);
    }

    #[test]
    fn a_suggestion_follows_the_kinds_the_rows_held() {
        let suggest = |tags: Vec<Tag<'static>>| {
            let mut s = Suggestion::default();
            for t in &tags {
                s.saw(t);
            }
            s.suggest()
        };
        assert_eq!(suggest(vec![]), DataType::Null);
        assert_eq!(suggest(vec![Tag::I64(1), Tag::I64(2)]), DataType::I64);
        assert_eq!(suggest(vec![Tag::I64(1), Tag::F64(2.5)]), DataType::F64);
        assert_eq!(suggest(vec![Tag::I64(1), Tag::Str("x".into())]), DataType::Str);
        assert_eq!(
            suggest(vec![Tag::Array(vec![Some(Tag::I64(1)), None])]),
            DataType::Array(Box::new(DataType::I64))
        );
    }

    /// A time is counted beside its kind, so `kinds` and the suggestion are what they were.
    #[test]
    fn a_suggestion_counts_the_values_that_read_as_a_time() {
        let mut s = Suggestion::default();
        for t in [
            Tag::Str("2023-11-14T22:13:20Z".into()),
            Tag::Str("okta:system".into()),
            Tag::I64(1_700_000_000),
            Tag::I64(8080),
            Tag::F64(1_700_000_000.25),
            Tag::DateTime(0),
        ] {
            s.saw(&t);
        }
        let c = s.counts();
        assert_eq!((c.present, c.times), (6, 4));
        assert_eq!(c.kinds(), 3);
    }
}
