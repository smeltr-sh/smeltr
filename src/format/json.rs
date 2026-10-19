//! Batch → JSON serialization.
//!
//! Columns are flat, one per leaf (design doc, Columnar data model). Nesting happens once, here: the serializer walks
//! the plan's name tree (`plan::tree`), which gives each object its fields and the slot of its unclaimed fields. At
//! every object it writes the fields, then the unclaimed fields' keys that no field claims and no rename took away.
//! `Struct` columns nest structurally, a field's name split at its dots as a path is.
//!
//! Every non-null field is serialized; nulling a field is how a pipeline excludes it. A null field is omitted from its
//! object, and an object all of whose fields are null disappears too. Array elements are the exception: a null element
//! is written as `null`, since omitting it would shift every later index.
//!
//! A variant writes each row's value as it is.
//!
//! `DateTime` writes as ISO 8601 (`"1970-01-01T00:00:00.000000Z"`);
//! `Duration` writes as an integer count of microseconds, the unit it is held in.
//! `Bytes` writes as a base64 string ([`BytesForm`]).

use std::io::Write as _;

use crate::core::bits;
use crate::core::column::{Column, DataType, Scalar};
use crate::core::variant::{self, Value};
use crate::format::bytes_text::BytesForm;
use crate::format::record::{At, Field, Leaf, Object, RecordView, View};

/// How deep a key sits in a document before a dotted name stops nesting, counted from the row's own object: the
/// objects the data nests and the objects a dotted name makes count alike. A name reaching this level keeps its
/// remaining segments as one key. Half of `json_scan`'s bound on the input, so a document this writes reads back
/// through that scanner with room for the nesting the data itself had; and it is what keeps a key a producer
/// controls from recursing the writer off its stack, however many structs and arrays it sits inside.
pub(crate) const MAX_NESTING: usize = 64;

impl RecordView<'_> {
    /// The text of the value at `name`, a dotted name, on `row`, as `scalar_text` gives it: a field's, or a key of
    /// the unclaimed fields of the deepest object on the way to it. For a field used as an identity.
    pub fn text(&self, name: &str, row: usize) -> Option<String> {
        match self.at(name, row)? {
            At::Leaf(col) => scalar_text(col, row),
            At::Unclaimed(value) => variant_text(value),
        }
    }

    /// Write `row`'s JSON document into `out`, clearing it first.
    pub fn write_row(&self, row: usize, out: &mut Vec<u8>) {
        out.clear();
        if !write_object(&self.root, None, &self.leaves, row, 1, out) {
            out.extend_from_slice(b"{}");
        }
    }

    pub fn row(&self, row: usize) -> Vec<u8> {
        let mut out = Vec::new();
        self.write_row(row, &mut out);
        out
    }
}
/// Write `o` for `row`: its fields, then its unclaimed fields' keys no field claims. Returns whether anything was
/// written; `false` means every field under here was null, and the caller omits it. Where nothing is written and the
/// unclaimed fields hold a value that is no object, that value is written instead. `inherited` is what the parent's
/// unclaimed fields hold at this object's key. `depth` is the level of this object's keys, 1 at the row's own object.
pub(crate) fn write_object(
    o: &Object,
    inherited: Option<&[u8]>,
    leaves: &[Leaf],
    row: usize,
    depth: usize,
    out: &mut Vec<u8>,
) -> bool {
    let own = o.unclaimed_at(row, inherited);
    let own_object = own.filter(|v| variant::is_object(v));
    let start = out.len();
    out.push(b'{');
    let mut wrote_any = false;
    for (key, field) in &o.fields {
        let before = out.len();
        if wrote_any {
            out.push(b',');
        }
        out.extend_from_slice(&key.quoted);
        let wrote = match field {
            Field::Leaf(col, idx) => write_leaf(col, &leaves[*idx], row, depth, out),
            Field::Object(child) => {
                let below = o.member(own_object, &key.raw);
                write_object(child, below, leaves, row, depth + 1, out)
            }
        };
        if wrote {
            wrote_any = true;
        } else {
            out.truncate(before);
        }
    }
    if let Some(Value::Object(entries)) = own_object.map(variant::read) {
        wrote_any = write_members(entries, |key| o.claims(key), wrote_any, out);
    }
    if wrote_any {
        out.push(b'}');
        return true;
    }
    out.truncate(start);
    own.is_some_and(|v| !variant::is_object(v) && write_variant(v, out))
}

#[inline(always)]
fn write_leaf(col: &Column, leaf: &Leaf, row: usize, depth: usize, out: &mut Vec<u8>) -> bool {
    match &leaf.view {
        View::Str(v) => {
            if !v.validity.is_valid(row) {
                return false;
            }
            write_json_string(v.get(row), out);
            true
        }
        View::I64(f) => {
            if !f.validity.is_valid(row) {
                return false;
            }
            out.extend_from_slice(itoa::Buffer::new().format(f.values[row]).as_bytes());
            true
        }
        View::Other => write_value_at(col, &leaf.ty, row, depth, out),
    }
}

/// One variant as JSON, as it arrived. `false`, writing nothing, where there is nothing to write: a null, a
/// float JSON cannot hold, or an object with nothing in it, as an object of null fields is omitted.
pub(crate) fn write_variant(b: &[u8], out: &mut Vec<u8>) -> bool {
    match variant::read(b) {
        Value::Null => false,
        Value::Bool(v) => {
            out.extend_from_slice(if v { b"true" } else { b"false" });
            true
        }
        Value::I64(v) => {
            out.extend_from_slice(itoa::Buffer::new().format(v).as_bytes());
            true
        }
        // As an `F64` column writes it, so a float stays a float.
        Value::F64(v) => {
            if !v.is_finite() {
                return false;
            }
            write!(out, "{v:?}").unwrap();
            true
        }
        Value::Str(s) => {
            write_json_string(s.as_bytes(), out);
            true
        }
        // A time, a span and bytes as their columns write them.
        Value::DateTime(v) => {
            write_iso8601_micros(v, out);
            true
        }
        Value::Duration(v) => {
            crate::ops::datetime::write_duration_micros_bare(v, out);
            true
        }
        Value::Bytes(b) => {
            write_base64_string(b, out);
            true
        }
        Value::Array(items) => {
            out.push(b'[');
            for (i, item) in items.enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                if !write_variant(item, out) {
                    out.extend_from_slice(b"null");
                }
            }
            out.push(b']');
            true
        }
        Value::Object(entries) => write_members_object(entries, |_| false, out),
    }
}

/// The members of a variant object `claimed` leaves, as one JSON object. `false`, writing nothing, where none has a
/// value.
pub(crate) fn write_members_object<'v>(
    entries: impl Iterator<Item = (&'v str, &'v [u8])>,
    claimed: impl Fn(&str) -> bool,
    out: &mut Vec<u8>,
) -> bool {
    let start = out.len();
    out.push(b'{');
    if write_members(entries, claimed, false, out) {
        out.push(b'}');
        return true;
    }
    out.truncate(start);
    false
}

/// The members of a variant object `claimed` leaves, each `"key":value`, a comma before each where `wrote_any` or one
/// came before it. A member with no value is left out. Whether anything was written, counting `wrote_any`.
fn write_members<'v>(
    entries: impl Iterator<Item = (&'v str, &'v [u8])>,
    claimed: impl Fn(&str) -> bool,
    mut wrote_any: bool,
    out: &mut Vec<u8>,
) -> bool {
    for (key, value) in entries {
        if claimed(key) {
            continue;
        }
        let before = out.len();
        if wrote_any {
            out.push(b',');
        }
        write_json_string(key.as_bytes(), out);
        out.push(b':');
        if write_variant(value, out) {
            wrote_any = true;
        } else {
            out.truncate(before);
        }
    }
    wrote_any
}

/// `write_object` over a `Struct`'s fields, whose names are literal keys rather than dotted paths. `depth` is the
/// level of the fields.
fn write_object_fields<'a>(
    fields: impl IntoIterator<Item = (&'a str, &'a Column, &'a DataType)>,
    row: usize,
    depth: usize,
    out: &mut Vec<u8>,
) -> bool {
    let start = out.len();
    out.push(b'{');
    let mut wrote_any = false;
    for (key, col, ty) in fields {
        let before = out.len();
        if wrote_any {
            out.push(b',');
        }
        write_json_string(key.as_bytes(), out);
        out.push(b':');
        if write_value_at(col, ty, row, depth, out) {
            wrote_any = true;
        } else {
            out.truncate(before);
        }
    }
    if wrote_any {
        out.push(b'}');
        true
    } else {
        out.truncate(start);
        false
    }
}

// ---------------------------------------------------------------------------
// Scalar / recursive value writing
// ---------------------------------------------------------------------------

/// Write `col[row]` as JSON, as its own document: a struct's fields are the document's own keys, at level 1. Returns
/// `false`, writing nothing, when the value is null. `ty` is `col`'s own type, which every caller already has.
pub(crate) fn write_value(col: &Column, ty: &DataType, row: usize, out: &mut Vec<u8>) -> bool {
    write_value_at(col, ty, row, 0, out)
}

/// `write_value` for a value whose key sits `depth` levels into a document. The level bounds how far a dotted name
/// inside a struct still nests (`MAX_NESTING`).
#[inline(always)]
fn write_value_at(col: &Column, ty: &DataType, row: usize, depth: usize, out: &mut Vec<u8>) -> bool {
    if col.is_unbound() {
        return false;
    }
    // A constant column is written from its value; nothing here needs it as rows.
    if let Some(value) = col.constant_value() {
        return write_scalar(value, out);
    }
    match ty {
        // A null column holds no value.
        DataType::Null => false,
        DataType::Bool => {
            let b = col.bool().unwrap();
            if !b.validity.is_valid(row) {
                return false;
            }
            out.extend_from_slice(if bits::get_row(b.values, row) {
                b"true"
            } else {
                b"false"
            });
            true
        }
        DataType::I64 => {
            let f = col.i64().unwrap();
            if !f.validity.is_valid(row) {
                return false;
            }
            out.extend_from_slice(itoa::Buffer::new().format(f.values[row]).as_bytes());
            true
        }
        DataType::F64 => {
            let f = col.f64().unwrap();
            if !f.validity.is_valid(row) {
                return false;
            }
            let v = f.values[row];
            // JSON has no NaN/Infinity; a dropped field beats invalid output.
            if !v.is_finite() {
                return false;
            }
            // `{v:?}`, not `{v}`: an F64 always carries a decimal point or an exponent, so `6.0` cannot be read back as
            // an integer -- which decides an Elasticsearch mapping on the first document it sees. It is also the
            // shortest round-tripping form, where Display spells `1e300` as three hundred digits.
            write!(out, "{v:?}").unwrap();
            true
        }
        DataType::DateTime => {
            let f = col.datetime().unwrap();
            if !f.validity.is_valid(row) {
                return false;
            }
            write_iso8601_micros(f.values[row], out);
            true
        }
        DataType::Duration => {
            let f = col.duration().unwrap();
            if !f.validity.is_valid(row) {
                return false;
            }
            crate::ops::datetime::write_duration_micros_bare(f.values[row], out);
            true
        }
        DataType::Str => {
            let v = col.var().unwrap();
            if !v.validity.is_valid(row) {
                return false;
            }
            write_json_string(v.get(row), out);
            true
        }
        DataType::Bytes => {
            let v = col.var().unwrap();
            if !v.validity.is_valid(row) {
                return false;
            }
            write_base64_string(v.get(row), out);
            true
        }
        DataType::Array(elem_ty) => {
            let l = col.list().unwrap();
            if !l.validity.is_valid(row) {
                return false;
            }
            let (s, e) = (l.offsets[row] as usize, l.offsets[row + 1] as usize);
            out.push(b'[');
            for (i, idx) in (s..e).enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                write_value_or_null(l.child, elem_ty, idx, depth + 1, out);
            }
            out.push(b']');
            true
        }
        // A struct holding unclaimed fields is written from the row's whole object: its fields, then those keys.
        DataType::Struct(..) if col.unclaimed_fields().is_some() => match crate::format::tag::row_tag(col, row) {
            Some(tag) => {
                let mut bytes = Vec::new();
                crate::format::tag::encode(&tag, &mut bytes);
                write_variant(&bytes, out)
            }
            None => false,
        },
        DataType::Struct(fields, _) => {
            let named = fields.iter().map(|(n, t)| (n.as_str(), col.field(n).unwrap(), t));
            write_object_fields(named, row, depth + 1, out)
        }
        DataType::Variant => {
            let v = col.variant().unwrap();
            v.validity.is_valid(row) && write_variant(v.get(row), out)
        }
    }
}

/// One value as JSON, the way `write_value` writes a row of its type. `false` for a float JSON cannot hold.
pub(crate) fn write_scalar(value: &Scalar, out: &mut Vec<u8>) -> bool {
    match value {
        Scalar::Bool(b) => out.extend_from_slice(if *b { b"true" } else { b"false" }),
        Scalar::I64(x) => out.extend_from_slice(itoa::Buffer::new().format(*x).as_bytes()),
        Scalar::F64(x) => {
            if !x.is_finite() {
                return false;
            }
            write!(out, "{x:?}").unwrap();
        }
        Scalar::DateTime(x) => write_iso8601_micros(*x, out),
        Scalar::Duration(x) => crate::ops::datetime::write_duration_micros_bare(*x, out),
        Scalar::Str(s) => write_json_string(s.as_bytes(), out),
        Scalar::Bytes(b) => write_base64_string(b, out),
        Scalar::Variant(v) => return write_variant(v, out),
    }
    true
}

/// One scalar's text as this row's JSON would show it, except that a `Str`/`Bytes` comes back unquoted. For a field
/// used as an identity (an Elasticsearch `_id`): a `DateTime` renders as its ISO-8601 text, not a micros count. `None`
/// for null, unbound, `Array` and `Struct`, and a variant array or object.
pub fn scalar_text(col: &Column, row: usize) -> Option<String> {
    if col.is_unbound() {
        return None;
    }
    let ty = col.data_type();
    match ty {
        DataType::Array(_) | DataType::Struct(..) => None,
        DataType::Variant => {
            if let Some(Scalar::Variant(value)) = col.constant_value() {
                return variant_text(value);
            }
            let v = col.variant().ok()?;
            variant_text(v.validity.is_valid(row).then(|| v.get(row))?)
        }
        DataType::Str | DataType::Bytes => {
            match col.constant_value() {
                Some(Scalar::Str(s)) => return Some(s.clone()),
                Some(Scalar::Bytes(b)) => return Some(BytesForm::Base64.text(b)),
                _ => {}
            }
            let v = col.var().ok()?;
            let value = v.validity.is_valid(row).then(|| v.get(row))?;
            Some(match ty {
                DataType::Bytes => BytesForm::Base64.text(value),
                _ => String::from_utf8_lossy(value).into_owned(),
            })
        }
        _ => {
            let mut out = Vec::new();
            write_value(col, &ty, row, &mut out).then(|| String::from_utf8_lossy(&out).into_owned())
        }
    }
}

/// `scalar_text` for one variant.
fn variant_text(b: &[u8]) -> Option<String> {
    match variant::read(b) {
        Value::Str(s) => Some(s.to_string()),
        Value::Bytes(b) => Some(BytesForm::Base64.text(b)),
        Value::Array(_) | Value::Object(_) => None,
        _ => {
            let mut out = Vec::new();
            write_variant(b, &mut out).then(|| String::from_utf8_lossy(&out).into_owned())
        }
    }
}

/// `write_value_at`, but a null value writes `null`.
#[inline(always)]
fn write_value_or_null(col: &Column, ty: &DataType, row: usize, depth: usize, out: &mut Vec<u8>) {
    if !write_value_at(col, ty, row, depth, out) {
        out.extend_from_slice(b"null");
    }
}

/// `bytes` as a JSON string: base64 ([`BytesForm`]), quoted. The base64 alphabet needs no escape.
fn write_base64_string(bytes: &[u8], out: &mut Vec<u8>) {
    out.push(b'"');
    BytesForm::Base64.write(bytes, out);
    out.push(b'"');
}

/// `s` must be valid UTF-8. `pub(crate)` so `metrics` shares one escaper.
pub(crate) fn write_json_string(s: &[u8], out: &mut Vec<u8>) {
    out.push(b'"');
    let mut clean = 0; // start of the run of bytes that need no escape
    let mut i = 0;
    while i < s.len() {
        // Eight bytes at a time while none of them is a quote, a backslash or a control character: most of a
        // document is such runs, and one word test settles eight bytes.
        if i + 8 <= s.len() {
            let word = u64::from_le_bytes(s[i..i + 8].try_into().expect("eight bytes"));
            if !has_byte_needing_escape(word) {
                i += 8;
                continue;
            }
        }
        let b = s[i];
        let escaped: &[u8] = match b {
            b'"' => b"\\\"",
            b'\\' => b"\\\\",
            0x08 => b"\\b",
            0x0C => b"\\f",
            b'\n' => b"\\n",
            b'\r' => b"\\r",
            b'\t' => b"\\t",
            0x00..=0x1F => {
                out.extend_from_slice(&s[clean..i]);
                write!(out, "\\u{b:04x}").unwrap();
                i += 1;
                clean = i;
                continue;
            }
            _ => {
                i += 1;
                continue;
            }
        };
        out.extend_from_slice(&s[clean..i]);
        out.extend_from_slice(escaped);
        i += 1;
        clean = i;
    }
    out.extend_from_slice(&s[clean..]);
    out.push(b'"');
}

/// Whether any byte of `word` is `"`, `\` or below 0x20. The classic word-at-a-time test: a byte equal to `x` is
/// found as a zero in `word ^ x`, and a zero byte in `v` sets the top bit of that byte in
/// `(v - 0x01..) & !v & 0x80..`; a byte below 0x20 sets the top bit of `(v - 0x20..)` where `v`'s own top bit is clear.
///
/// The error is one-sided, which is what makes it safe to skip eight bytes on a `false`. A borrow out of one byte can
/// light the byte above it, so the answer is sometimes yes where it should be no, and that costs one skipped fast
/// path. It is never no where it should be yes: a byte needing an escape sets its own high bit in the mask, and the
/// below-0x20 test holds for any constant up to 0x80.
#[inline(always)]
fn has_byte_needing_escape(word: u64) -> bool {
    const ONES: u64 = 0x0101_0101_0101_0101;
    const HIGHS: u64 = 0x8080_8080_8080_8080;
    let has_zero = |v: u64| v.wrapping_sub(ONES) & !v & HIGHS != 0;
    let below_space = word.wrapping_sub(0x2020_2020_2020_2020) & !word & HIGHS != 0;
    below_space || has_zero(word ^ (ONES * u64::from(b'"'))) || has_zero(word ^ (ONES * u64::from(b'\\')))
}

// ---------------------------------------------------------------------------
// Timestamps
// ---------------------------------------------------------------------------

/// Epoch microseconds → `"YYYY-MM-DDTHH:MM:SS.ffffffZ"`. The text is `ops::datetime`'s; the quotes are JSON's.
fn write_iso8601_micros(micros: i64, out: &mut Vec<u8>) {
    out.push(b'"');
    crate::ops::datetime::write_iso8601_micros_bare(micros, out);
    out.push(b'"');
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::buffer::BufferPool;
    use crate::core::column::{Column, StrBuilder, VarType};
    use crate::format::tag::Tag;
    use crate::plan::tree::{self, Node};

    fn i64_col(pool: &mut BufferPool, vals: &[Option<i64>]) -> Column {
        let mut c = Column::alloc(pool, &DataType::I64, vals.len());
        {
            let f = c.i64_mut().unwrap();
            for (i, v) in vals.iter().enumerate() {
                match v {
                    Some(x) => f.values[i] = *x,
                    None => f.validity.set_valid(i, false),
                }
            }
        }
        c
    }

    fn str_col(pool: &mut BufferPool, vals: &[Option<&str>]) -> Column {
        let mut sb = StrBuilder::new(pool, VarType::Str, vals.len());
        sb.reserve(pool, vals.iter().flatten().map(|s| s.len()).sum());
        for v in vals {
            match v {
                Some(s) => sb.push(s.as_bytes()),
                None => sb.push_null(),
            }
        }
        sb.finish()
    }

    fn bool_col(pool: &mut BufferPool, vals: &[bool]) -> Column {
        let mut c = Column::alloc(pool, &DataType::Bool, vals.len());
        {
            let b = c.bool_mut().unwrap();
            for (i, v) in vals.iter().enumerate() {
                crate::core::bits::set_row(b.values, i, *v);
            }
        }
        c
    }

    // -- JSON string escaping ------------------------------------------------

    #[test]
    fn string_escaping_covers_the_json_special_set() {
        let mut out = Vec::new();
        write_json_string(b"a\"b\\c\nd\te", &mut out);
        assert_eq!(out, br#""a\"b\\c\nd\te""#);
    }

    /// The byte-at-a-time loop `write_json_string` replaced. The oracle it is checked against, so it carries every
    /// escape the real one does.
    fn escape_one_at_a_time(s: &[u8]) -> Vec<u8> {
        let mut out = vec![b'"'];
        for &b in s {
            match b {
                b'"' => out.extend_from_slice(b"\\\""),
                b'\\' => out.extend_from_slice(b"\\\\"),
                0x08 => out.extend_from_slice(b"\\b"),
                0x0C => out.extend_from_slice(b"\\f"),
                b'\n' => out.extend_from_slice(b"\\n"),
                b'\r' => out.extend_from_slice(b"\\r"),
                b'\t' => out.extend_from_slice(b"\\t"),
                0x00..=0x1F => out.extend_from_slice(format!("\\u{b:04x}").as_bytes()),
                _ => out.push(b),
            }
        }
        out.push(b'"');
        out
    }

    /// The word-at-a-time escaper writes what the byte-at-a-time loop writes. Eight-byte steps mean a byte needing an
    /// escape can fall anywhere in a word, or beside another one, so both are swept: every byte value at every
    /// position, then random strings over the whole byte range.
    #[test]
    fn escaping_is_the_same_at_every_offset() {
        let mut fast = Vec::new();
        for len in 0..25 {
            for pos in 0..len {
                for byte in 0..=u8::MAX {
                    let mut input = vec![b'a'; len];
                    input[pos] = byte;
                    fast.clear();
                    write_json_string(&input, &mut fast);
                    assert_eq!(fast, escape_one_at_a_time(&input), "len {len} pos {pos} byte {byte:#x}");
                }
            }
        }

        // One seeded byte never puts two escapes in a word, nor one next to a byte above 0x7F. These do.
        let mut seed = 0x243f_6a88_85a3_08d3u64;
        let mut next = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        for _ in 0..20_000 {
            let len = usize::from(next().to_le_bytes()[0]) % 41;
            let input: Vec<u8> = (0..len).map(|_| next().to_le_bytes()[0]).collect();
            fast.clear();
            write_json_string(&input, &mut fast);
            assert_eq!(fast, escape_one_at_a_time(&input), "{input:x?}");
        }

        let mut out = Vec::new();
        write_json_string("héllo wörld ✓ and a tab\there".as_bytes(), &mut out);
        assert_eq!(out, b"\"h\xc3\xa9llo w\xc3\xb6rld \xe2\x9c\x93 and a tab\\there\"");
    }

    #[test]
    fn control_characters_use_u_escapes() {
        let mut out = Vec::new();
        write_json_string(&[0x01], &mut out);
        assert_eq!(out, b"\"\\u0001\"");
    }

    // -- object shape / null omission ----------------------------------------

    #[test]
    fn flat_field_round_trips() {
        let mut pool = BufferPool::new();
        let name = i64_col(&mut pool, &[Some(1), Some(2)]);
        let s = RecordView::from_named([("id", &name)]);
        assert_eq!(s.row(0), br#"{"id":1}"#);
        assert_eq!(s.row(1), br#"{"id":2}"#);
        name.reclaim(&mut pool);
    }

    #[test]
    fn dotted_names_nest() {
        let mut pool = BufferPool::new();
        let ip = str_col(&mut pool, &[Some("1.2.3.4")]);
        let port = i64_col(&mut pool, &[Some(443)]);
        let s = RecordView::from_named([("source.ip", &ip), ("source.port", &port)]);
        assert_eq!(s.row(0), br#"{"source":{"ip":"1.2.3.4","port":443}}"#);
        let s = RecordView::from_named([(r#"labels["app.kubernetes.io/name"]"#, &ip), (r#"["a.b"]"#, &port)]);
        assert_eq!(
            s.row(0),
            br#"{"labels":{"app.kubernetes.io/name":"1.2.3.4"},"a.b":443}"#
        );
        ip.reclaim(&mut pool);
        port.reclaim(&mut pool);
    }

    #[test]
    fn double_underscore_fields_are_hidden_by_default() {
        let mut pool = BufferPool::new();
        let a = i64_col(&mut pool, &[Some(1)]);
        let topic = str_col(&mut pool, &[Some("urgent")]);
        let s = over_tree(&[("a", &a), ("__destination.topic", &topic)], None, false);
        assert_eq!(s.row(0), br#"{"a":1}"#);
        a.reclaim(&mut pool);
        topic.reclaim(&mut pool);
    }

    #[test]
    fn a_single_underscore_field_is_ordinary_output_not_internal() {
        let mut pool = BufferPool::new();
        let a = str_col(&mut pool, &[Some("x")]);
        let s = RecordView::from_named([("_a", &a)]);
        assert_eq!(s.row(0), br#"{"_a":"x"}"#);
        a.reclaim(&mut pool);
    }

    #[test]
    fn new_with_internal_serializes_double_underscore_fields_too() {
        let mut pool = BufferPool::new();
        let a = i64_col(&mut pool, &[Some(1)]);
        let topic = str_col(&mut pool, &[Some("urgent")]);
        let s = over_tree(&[("a", &a), ("__destination.topic", &topic)], None, true);
        assert_eq!(s.row(0), br#"{"__destination":{"topic":"urgent"},"a":1}"#);
        a.reclaim(&mut pool);
        topic.reclaim(&mut pool);
    }

    #[test]
    fn null_field_is_omitted_not_written_as_null() {
        let mut pool = BufferPool::new();
        let a = i64_col(&mut pool, &[Some(1)]);
        let b = i64_col(&mut pool, &[None]);
        let s = RecordView::from_named([("a", &a), ("b", &b)]);
        assert_eq!(s.row(0), br#"{"a":1}"#);
        a.reclaim(&mut pool);
        b.reclaim(&mut pool);
    }

    #[test]
    fn object_with_every_field_null_disappears_entirely() {
        let mut pool = BufferPool::new();
        let a = i64_col(&mut pool, &[None]);
        let s = RecordView::from_named([("outer.a", &a)]);
        assert_eq!(s.row(0), b"{}");
        a.reclaim(&mut pool);
    }

    #[test]
    fn empty_row_is_an_empty_object() {
        let s = RecordView::from_named(std::iter::empty());
        assert_eq!(s.row(0), b"{}");
    }

    #[test]
    fn bool_values() {
        let mut pool = BufferPool::new();
        let b = bool_col(&mut pool, &[true, false]);
        let s = RecordView::from_named([("ok", &b)]);
        assert_eq!(s.row(0), br#"{"ok":true}"#);
        assert_eq!(s.row(1), br#"{"ok":false}"#);
        b.reclaim(&mut pool);
    }

    /// A span writes as its microsecond count, a JSON number: zero is `0`, and a negative keeps its sign.
    #[test]
    fn duration_writes_its_microsecond_count() {
        let mut pool = BufferPool::new();
        let cases = [
            (5i64, "5"),
            (0, "0"),
            (-5, "-5"),
            (i64::MAX, "9223372036854775807"),
            (i64::MIN, "-9223372036854775808"),
        ];
        let mut c = Column::alloc(&mut pool, &DataType::Duration, cases.len());
        for (i, (micros, _)) in cases.iter().enumerate() {
            c.duration_mut().unwrap().values[i] = *micros;
        }
        let s = RecordView::from_named([("event.duration", &c)]);
        for (row, (micros, want)) in cases.iter().enumerate() {
            let want = format!(r#"{{"event":{{"duration":{want}}}}}"#);
            assert_eq!(s.row(row), want.as_bytes(), "{micros} micros");
        }
        c.reclaim(&mut pool);
    }

    /// An `F64` always carries a decimal point or an exponent. A whole float written as `6` is indistinguishable from
    /// an `I64`, and the first document decides an Elasticsearch mapping for every one after it.
    #[test]
    fn a_float_always_writes_a_decimal_point_or_an_exponent() {
        let mut pool = BufferPool::new();
        let cases = [
            (6.0, "6.0"),
            (1.5, "1.5"),
            (-0.0, "-0.0"),
            (0.1 + 0.2, "0.30000000000000004"),
            // Display spells this one with three hundred digits.
            (1e300, "1e300"),
            (1e-7, "1e-7"),
        ];
        let mut c = Column::alloc(&mut pool, &DataType::F64, cases.len());
        for (i, (v, _)) in cases.iter().enumerate() {
            c.f64_mut().unwrap().values[i] = *v;
        }
        let s = RecordView::from_named([("v", &c)]);
        for (row, (v, want)) in cases.iter().enumerate() {
            assert_eq!(s.row(row), format!(r#"{{"v":{want}}}"#).as_bytes(), "{v}");
        }
        c.reclaim(&mut pool);
    }

    /// A constant column writes as its rows would, without building them.
    #[test]
    fn a_constant_column_writes_its_value() {
        let mut pool = BufferPool::new();
        let cases: Vec<(Scalar, &str)> = vec![
            (Scalar::Str("a\"b".into()), r#""a\"b""#),
            (Scalar::I64(-42), "-42"),
            (Scalar::F64(6.0), "6.0"),
            (Scalar::Bool(false), "false"),
            (Scalar::DateTime(0), r#""1970-01-01T00:00:00.000000Z""#),
            (Scalar::Duration(1_500), "1500"),
        ];
        for (value, want) in cases {
            let c = Column::constant(value.clone(), 2);
            let s = RecordView::from_named([("v", &c)]);
            assert_eq!(s.row(1), format!(r#"{{"v":{want}}}"#).as_bytes(), "{value:?}");
            let rows = Column::filled(&mut pool, &value, 2);
            let s = RecordView::from_named([("v", &rows)]);
            assert_eq!(s.row(1), format!(r#"{{"v":{want}}}"#).as_bytes(), "{value:?} as rows");
            rows.reclaim(&mut pool);
            c.reclaim(&mut pool);
        }
        let nan = Column::constant(Scalar::F64(f64::NAN), 1);
        assert_eq!(RecordView::from_named([("v", &nan)]).row(0), b"{}");
        nan.reclaim(&mut pool);
    }

    /// A struct's field name is one key, dots and all; a name nests a record to the serializer's bound.
    #[test]
    fn a_struct_field_is_one_key_and_a_name_nests_to_the_bound() {
        let mut pool = BufferPool::new();
        let deep = vec!["k"; 300].join(".");
        let one = Column::filled(&mut pool, &Scalar::I64(1), 1);
        let s = Column::struct_from_parts(vec![(deep.clone(), one.deep_copy(&mut pool))], 1);
        let objects = |doc: &[u8]| doc.iter().filter(|b| **b == b'{').count();

        let mut standalone = Vec::new();
        assert!(write_value(&s, &s.data_type(), 0, &mut standalone));
        assert_eq!(objects(&standalone), 1);

        let mut in_row = Vec::new();
        RecordView::from_named([(deep.as_str(), &one)]).write_row(0, &mut in_row);
        assert_eq!(objects(&in_row), MAX_NESTING);
        s.reclaim(&mut pool);
        one.reclaim(&mut pool);
    }

    // -- the tree and its unclaimed fields ------------------------------------------

    /// A serializer over a record of `fields` at their names, slot `i` holding the `i`th, and `unclaimed` as the
    /// unclaimed fields of the object `at`.
    fn over_tree<'a>(
        fields: &[(&str, &'a Column)],
        unclaimed: Option<(&str, &'a Column)>,
        show_internal: bool,
    ) -> RecordView<'a> {
        let mut tree = tree::Object::default();
        let mut cols: Vec<&'a Column> = Vec::new();
        for (name, col) in fields {
            tree.put(name, Node::Leaf(cols.len()));
            cols.push(col);
        }
        if let Some((at, col)) = unclaimed {
            if tree.object(at).is_none() {
                tree.put(at, Node::Object(tree::Object::default()));
            }
            tree.object_mut(at).unwrap().unclaimed = Some(cols.len());
            cols.push(col);
        }
        if show_internal {
            RecordView::new_with_internal(&tree, move |s| cols[s])
        } else {
            RecordView::new(&tree, move |s| cols[s])
        }
    }

    /// One object per row, a variant column.
    fn objects(pool: &mut BufferPool, rows: &[&[(&str, i64)]]) -> Column {
        let values: Vec<Option<Tag<'_>>> = rows
            .iter()
            .map(|keys| {
                Some(Tag::Object(
                    keys.iter().map(|(k, v)| ((*k).into(), Tag::I64(*v))).collect(),
                ))
            })
            .collect();
        crate::format::tag::encode_column(&values, pool)
    }

    /// At every object the fields come first, then the unclaimed fields' keys no field claims. A field claims its key
    /// on every row, null or not.
    #[test]
    fn the_unclaimed_fields_follow_the_fields_less_the_keys_they_claim() {
        let mut pool = BufferPool::new();
        let a = i64_col(&mut pool, &[Some(1), None]);
        let b = i64_col(&mut pool, &[Some(2), Some(2)]);
        let rest = objects(&mut pool, &[&[("a", 7), ("z", 8)], &[("a", 7), ("y", 9)]]);
        let s = over_tree(&[("a", &a), ("o.b", &b)], Some(("", &rest)), false);
        assert_eq!(s.row(0), br#"{"a":1,"o":{"b":2},"z":8}"#);
        assert_eq!(s.row(1), br#"{"o":{"b":2},"y":9}"#);
        let s = over_tree(&[("o.b", &b)], Some(("o", &rest)), false);
        assert_eq!(s.row(0), br#"{"o":{"b":2,"a":7,"z":8}}"#);
        for c in [a, b, rest] {
            c.reclaim(&mut pool);
        }
    }

    /// A `__` key among the record's unclaimed fields is the engine's too, wherever it came from; one deeper is data.
    #[test]
    fn a_double_underscore_unclaimed_key_is_hidden_at_the_record_alone() {
        let mut pool = BufferPool::new();
        let rest = objects(&mut pool, &[&[("__secret", 1), ("k", 3)]]);
        let s = over_tree(&[], Some(("", &rest)), false);
        assert_eq!(s.row(0), br#"{"k":3}"#);
        let s = over_tree(&[], Some(("p", &rest)), false);
        assert_eq!(s.row(0), br#"{"p":{"__secret":1,"k":3}}"#);
        rest.reclaim(&mut pool);
    }

    /// An object `with_field` makes on the way to its name holds the record's own key there beneath the field.
    #[test]
    fn an_object_a_field_makes_holds_the_records_key_beneath_it() {
        let mut pool = BufferPool::new();
        let flag = bool_col(&mut pool, &[true, false]);
        let tag = Tag::Object(vec![("tag".into(), Tag::Object(vec![("n".into(), Tag::I64(1))]))]);
        let record = crate::format::tag::encode_column(&[Some(tag.clone()), Some(tag)], &mut pool);
        let s = over_tree(&[], Some(("", &record)), false).with_field("tag.failed", &flag);
        assert_eq!(s.row(0), br#"{"tag":{"failed":true,"n":1}}"#);
        assert_eq!(s.row(1), br#"{"tag":{"failed":false,"n":1}}"#);
        flag.reclaim(&mut pool);
        record.reclaim(&mut pool);
    }

    /// `text` reads a field, or a key of the unclaimed fields of the deepest object on the way.
    #[test]
    fn text_reads_a_field_or_an_unclaimed_key() {
        let mut pool = BufferPool::new();
        let b = str_col(&mut pool, &[Some("x")]);
        let rest = objects(&mut pool, &[&[("id", 5)]]);
        let s = over_tree(&[("o.b", &b)], Some(("o", &rest)), false);
        assert_eq!(s.text("o.b", 0).as_deref(), Some("x"));
        assert_eq!(s.text("o.id", 0).as_deref(), Some("5"));
        assert_eq!(s.text("o.none", 0), None);
        assert_eq!(s.text("o", 0), None);
        b.reclaim(&mut pool);
        rest.reclaim(&mut pool);
    }

    /// `text` leaves out a key a rename took away, as the document does (rule 6).
    #[test]
    fn text_leaves_out_a_key_a_rename_took_away() {
        let mut pool = BufferPool::new();
        let rest = objects(&mut pool, &[&[("id", 5), ("k", 1)]]);
        let tree = tree::Object {
            unclaimed: Some(0),
            removed: vec!["id".into()],
            ..Default::default()
        };
        let s = RecordView::new(&tree, |_| &rest);
        assert_eq!(s.text("id", 0), None);
        assert_eq!(s.text("k", 0).as_deref(), Some("1"));
        assert_eq!(s.row(0), br#"{"k":1}"#);
        rest.reclaim(&mut pool);
    }

    // -- arrays ----------------------------------------------------------------

    #[test]
    fn array_elements_use_null_not_omission() {
        let mut pool = BufferPool::new();
        let child = i64_col(&mut pool, &[Some(1), None, Some(3)]);
        let mut offsets = pool.take(2 * 4);
        offsets.as_mut_slice::<u32>().copy_from_slice(&[0, 3]);
        let validity = crate::core::column::Validity::alloc(&mut pool, 1);
        let arr = Column::list_from_parts(offsets, child, validity, 1);
        let s = RecordView::from_named([("nums", &arr)]);
        assert_eq!(s.row(0), br#"{"nums":[1,null,3]}"#);
        arr.reclaim(&mut pool);
    }

    #[test]
    fn write_row_clears_the_buffer_first() {
        let mut pool = BufferPool::new();
        let a = i64_col(&mut pool, &[Some(1), Some(2), Some(3)]);
        let s = RecordView::from_named([("a", &a)]);
        let mut out = Vec::new();
        for row in 0..3 {
            s.write_row(row, &mut out);
            assert!(out.starts_with(b"{\"a\":"));
        }
        a.reclaim(&mut pool);
    }
}
