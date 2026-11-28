//! The bytes of one variant: what a variant column holds per row.
//!
//! A variant's shape is the row's, so the value carries its shape with it. Each row of a variant column is
//! one encoded value, laid out as a tag byte and a payload:
//!
//! ```text
//! null    00
//! false   01
//! true    02
//! i64     03  8 bytes
//! f64     04  8 bytes
//! str     05  len:u32  utf8 bytes
//! array   06  body:u32  count:u32  value...
//! object  07  body:u32  count:u32  (len:u32 key value)...
//! time    08  8 bytes
//! span    09  8 bytes
//! bytes   0a  len:u32  bytes
//! ```
//!
//! A time is microseconds since the epoch, and a span a count of microseconds, as their columns hold them. Integers
//! are little-endian. `body` is the length of everything after it, so a reader steps over a container
//! without walking it. An object's keys are unique and in the order the record had them. A null inside an object is no
//! entry, as a JSON `null` under a key is missing; a null element of an array holds its place.
//!
//! The bytes never leave the process: nothing persists a column, so this encoding is free to change.

use crate::core::buffer::BufferPool;
use crate::core::column::{Column, StrBuilder, VarType};
use std::collections::HashMap;

/// The tag bytes.
pub(crate) const NULL: u8 = 0;
pub(crate) const FALSE: u8 = 1;
pub(crate) const TRUE: u8 = 2;
pub(crate) const I64: u8 = 3;
pub(crate) const F64: u8 = 4;
pub(crate) const STR: u8 = 5;
pub(crate) const ARRAY: u8 = 6;
pub(crate) const OBJECT: u8 = 7;
pub(crate) const DATETIME: u8 = 8;
pub(crate) const DURATION: u8 = 9;
pub(crate) const BYTES: u8 = 10;

/// One encoded value whose strings and keys are UTF-8, so reading one needs no check of its own. A sealed variant
/// column's rows are these (`Column::variant`); `Encoded::check` makes one of bytes from anywhere else.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Encoded<'a>(&'a [u8]);

impl<'a> Encoded<'a> {
    /// The value at the start of `b`. Panics if a string or key in it is not UTF-8, which is a bug: the bytes are written
    /// with `put_*`, which takes text as `&str`.
    pub fn check(b: &'a [u8]) -> Encoded<'a> {
        assert!(text_is_utf8(b), "a variant's strings and keys are UTF-8");
        Encoded(b)
    }

    /// # Safety
    /// Every string and key in the value `b` starts with must be UTF-8.
    pub(crate) unsafe fn unchecked(b: &'a [u8]) -> Encoded<'a> {
        Encoded(b)
    }

    pub fn bytes(self) -> &'a [u8] {
        self.0
    }
}

/// An `Encoded` value that owns its bytes, as a constant column holds one.
#[derive(Clone, Debug, PartialEq)]
pub struct EncodedBuf(Vec<u8>);

impl EncodedBuf {
    /// Panics as `Encoded::check` does.
    pub fn check(b: Vec<u8>) -> EncodedBuf {
        Encoded::check(&b);
        EncodedBuf(b)
    }

    /// # Safety
    /// Every string and key in the value `b` starts with must be UTF-8.
    pub(crate) unsafe fn unchecked(b: Vec<u8>) -> EncodedBuf {
        EncodedBuf(b)
    }

    pub fn get(&self) -> Encoded<'_> {
        Encoded(&self.0)
    }
}

impl From<Encoded<'_>> for EncodedBuf {
    fn from(v: Encoded<'_>) -> EncodedBuf {
        EncodedBuf(v.0.to_vec())
    }
}

/// Whether every string and key in the value `b` starts with is UTF-8.
pub(crate) fn text_is_utf8(b: &[u8]) -> bool {
    let utf8 = |s: &[u8]| s.is_ascii() || simdutf8::basic::from_utf8(s).is_ok();
    match b[0] {
        STR => utf8(&b[5..5 + u32_at(b, 1) as usize]),
        ARRAY | OBJECT => {
            let end = 5 + u32_at(b, 1) as usize;
            let mut at = 9;
            while at < end {
                if b[0] == OBJECT {
                    let klen = u32_at(b, at) as usize;
                    if !utf8(&b[at + 4..at + 4 + klen]) {
                        return false;
                    }
                    at += 4 + klen;
                }
                if !text_is_utf8(&b[at..end]) {
                    return false;
                }
                at += len_of(&b[at..end]);
            }
            true
        }
        _ => true,
    }
}

/// One value, read in place.
#[derive(Clone, Copy, Debug)]
pub enum Value<'a> {
    Null,
    Bool(bool),
    I64(i64),
    F64(f64),
    Str(&'a str),
    Array(Items<'a>),
    Object(Entries<'a>),
    DateTime(i64),
    Duration(i64),
    Bytes(&'a [u8]),
}

/// An array's elements, each as its own encoded value.
#[derive(Clone, Copy, Debug)]
pub struct Items<'a> {
    rest: &'a [u8],
    left: u32,
}

/// An object's entries, each a key and its encoded value.
#[derive(Clone, Copy, Debug)]
pub struct Entries<'a> {
    rest: &'a [u8],
    left: u32,
}

fn u32_at(b: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(b[at..at + 4].try_into().expect("four bytes"))
}

fn i64_at(b: &[u8]) -> i64 {
    i64::from_le_bytes(b[1..9].try_into().expect("eight bytes"))
}

/// The value `v` holds. The bytes are this module's own, so a malformed one is a bug and panics.
#[inline(always)]
pub fn read(v: Encoded<'_>) -> Value<'_> {
    let b = v.0;
    match b[0] {
        NULL => Value::Null,
        FALSE => Value::Bool(false),
        TRUE => Value::Bool(true),
        I64 => Value::I64(i64_at(b)),
        F64 => Value::F64(f64::from_le_bytes(b[1..9].try_into().expect("eight bytes"))),
        DATETIME => Value::DateTime(i64_at(b)),
        DURATION => Value::Duration(i64_at(b)),
        STR => {
            let len = u32_at(b, 1) as usize;
            // SAFETY: an `Encoded`'s strings are UTF-8.
            Value::Str(unsafe { std::str::from_utf8_unchecked(&b[5..5 + len]) })
        }
        BYTES => Value::Bytes(&b[5..5 + u32_at(b, 1) as usize]),
        ARRAY | OBJECT => {
            let body = u32_at(b, 1) as usize;
            let left = u32_at(b, 5);
            let rest = &b[9..5 + body];
            if b[0] == ARRAY {
                Value::Array(Items { rest, left })
            } else {
                Value::Object(Entries { rest, left })
            }
        }
        other => panic!("not a variant tag: {other}"),
    }
}

/// How many bytes the value `b` starts with takes.
#[inline]
pub(crate) fn len_of(b: &[u8]) -> usize {
    debug_assert!(b[0] <= BYTES, "not a variant tag: {}", b[0]);
    match b[0] {
        NULL | FALSE | TRUE => 1,
        I64 | F64 | DATETIME | DURATION => 9,
        // A str, bytes, an array or an object: its length follows the tag.
        _ => 5 + u32_at(b, 1) as usize,
    }
}

/// Whether `v` holds an object.
pub(crate) fn is_object(v: Encoded) -> bool {
    v.0.first() == Some(&OBJECT)
}

/// The value at `key` inside the object `v`. `None` where `v` is no object or has no such key.
pub(crate) fn member<'a>(v: Encoded<'a>, key: &str) -> Option<Encoded<'a>> {
    at(v, Key::Member(key.as_bytes()))
}

/// What a variant is read at: an object's member by name, or an array's element by position.
#[derive(Clone, Copy)]
pub(crate) enum Key<'k> {
    Member(&'k [u8]),
    /// Counts from the end when negative, as `at` does.
    Element(i64),
}

/// The value at `key` inside `v`. `None` where:
/// - `v` holds no value at `key`, or null there;
/// - `key` is a member and `v` is no object, or `key` is an element and `v` is no array.
pub(crate) fn at<'a>(v: Encoded<'a>, key: Key) -> Option<Encoded<'a>> {
    match (read(v), key) {
        (Value::Object(entries), Key::Member(key)) => {
            entries.into_iter().find(|(k, _)| k.as_bytes() == key).map(|(_, v)| v)
        }
        (Value::Array(items), Key::Element(i)) => {
            let at = crate::ops::helpers::position(i, items.len())?;
            // A null element holds its place in the array, and is nothing as a value.
            items.into_iter().nth(at).filter(|v| v.0[0] != NULL)
        }
        _ => None,
    }
}

impl<'a> Items<'a> {
    pub(crate) fn len(&self) -> usize {
        self.left as usize
    }

    pub fn is_empty(&self) -> bool {
        self.left == 0
    }
}

impl<'a> Iterator for Items<'a> {
    type Item = Encoded<'a>;

    #[inline]
    fn next(&mut self) -> Option<Encoded<'a>> {
        if self.left == 0 {
            return None;
        }
        let n = len_of(self.rest);
        let (value, rest) = self.rest.split_at(n);
        self.rest = rest;
        self.left -= 1;
        // SAFETY: the items are inside an `Encoded` array, whose strings and keys are all UTF-8.
        Some(unsafe { Encoded::unchecked(value) })
    }
}

impl<'a> Entries<'a> {
    pub(crate) fn len(&self) -> usize {
        self.left as usize
    }

    pub fn is_empty(&self) -> bool {
        self.left == 0
    }
}

impl<'a> Iterator for Entries<'a> {
    type Item = (&'a str, Encoded<'a>);

    #[inline]
    fn next(&mut self) -> Option<(&'a str, Encoded<'a>)> {
        if self.left == 0 {
            return None;
        }
        let klen = u32_at(self.rest, 0) as usize;
        // SAFETY: the entries are inside an `Encoded` object, whose strings and keys are all UTF-8.
        let key = unsafe { std::str::from_utf8_unchecked(&self.rest[4..4 + klen]) };
        let after = &self.rest[4 + klen..];
        let n = len_of(after);
        let (value, rest) = after.split_at(n);
        self.rest = rest;
        self.left -= 1;
        // SAFETY: as for the key.
        let value = unsafe { Encoded::unchecked(value) };
        Some((key, value))
    }
}

/// The value at `key` of each row's object in the variant column `col`, as a variant column: null where a row holds
/// no object or the object has no such key.
pub(crate) fn member_column(col: &Column, key: &str, pool: &mut BufferPool) -> Column {
    at_column(col, |_| Some(Key::Member(key.as_bytes())), pool)
}

/// The value at the key `key_of` gives each row of the variant column `col`, as a variant column: null where the row
/// has no key or its value holds nothing at it.
pub(crate) fn at_column<'k>(col: &Column, key_of: impl Fn(usize) -> Option<Key<'k>>, pool: &mut BufferPool) -> Column {
    let mut b = StrBuilder::new(pool, VarType::Variant, col.len());
    match col.variant() {
        Ok(v) => {
            for row in 0..col.len() {
                let value = v.validity.is_valid(row).then(|| key_of(row)).flatten();
                match value.and_then(|key| at(v.get(row), key)) {
                    Some(v) => b.push_encoded(pool, v),
                    None => b.push_null(),
                }
            }
        }
        Err(_) => (0..col.len()).for_each(|_| b.push_null()),
    }
    b.finish()
}

// -- writing -----------------------------------------------------------------

/// Append a null.
#[inline]
pub(crate) fn put_null(out: &mut Vec<u8>) {
    out.push(NULL);
}

#[inline]
pub(crate) fn put_bool(out: &mut Vec<u8>, v: bool) {
    out.push(if v { TRUE } else { FALSE });
}

#[inline]
pub(crate) fn put_i64(out: &mut Vec<u8>, v: i64) {
    put_word(out, I64, v.to_le_bytes());
}

#[inline]
pub(crate) fn put_f64(out: &mut Vec<u8>, v: f64) {
    put_word(out, F64, v.to_le_bytes());
}

#[inline]
pub fn put_str(out: &mut Vec<u8>, s: &str) {
    out.push(STR);
    put_len(out, s.len());
    out.extend_from_slice(s.as_bytes());
}

/// Append a time, in microseconds since the epoch.
#[inline]
pub(crate) fn put_datetime(out: &mut Vec<u8>, micros: i64) {
    put_word(out, DATETIME, micros.to_le_bytes());
}

/// Append a span, in microseconds.
#[inline]
pub(crate) fn put_duration(out: &mut Vec<u8>, micros: i64) {
    put_word(out, DURATION, micros.to_le_bytes());
}

/// A tag and its eight bytes, in one append.
#[inline]
fn put_word(out: &mut Vec<u8>, tag: u8, word: [u8; 8]) {
    let mut b = [tag; 9];
    b[1..].copy_from_slice(&word);
    out.extend_from_slice(&b);
}

pub(crate) fn put_bytes(out: &mut Vec<u8>, b: &[u8]) {
    out.push(BYTES);
    put_len(out, b.len());
    out.extend_from_slice(b);
}

/// Append an object key. Its value is appended next.
pub fn put_key(out: &mut Vec<u8>, key: &str) {
    put_len(out, key.len());
    out.extend_from_slice(key.as_bytes());
}

fn put_len(out: &mut Vec<u8>, n: usize) {
    let n = u32::try_from(n).expect("a variant value holds less than 4 GiB");
    out.extend_from_slice(&n.to_le_bytes());
}

/// A container being written: `begin_array` or `begin_object`, its members, then `end` with how many there were.
#[must_use]
#[derive(Clone, Copy, Default)]
pub struct Open(usize);

pub(crate) fn begin_array(out: &mut Vec<u8>) -> Open {
    begin(out, ARRAY)
}

pub fn begin_object(out: &mut Vec<u8>) -> Open {
    begin(out, OBJECT)
}

fn begin(out: &mut Vec<u8>, tag: u8) -> Open {
    out.push(tag);
    let at = out.len();
    out.extend_from_slice(&[0; 8]);
    Open(at)
}

/// Keep one entry per key of the object `open` began in `out`, whose entries are all written: a repeated key keeps
/// its last value, in its first place. Returns how many entries are left. Time is linear in the entries.
pub(crate) fn dedupe(out: &mut Vec<u8>, open: Open) -> usize {
    let start = open.0 + 8;
    // Each entry's start, its value's start, and its end.
    let mut entries = Vec::new();
    let mut at = start;
    while at < out.len() {
        let value = at + 4 + u32_at(out, at) as usize;
        let end = value + len_of(&out[value..]);
        entries.push((at, value, end));
        at = end;
    }
    let mut body = Vec::with_capacity(out.len() - start);
    let mut count = 0;
    let key = |&(at, value, _): &(usize, usize, usize)| &out[at + 4..value];
    let mut last: HashMap<&[u8], usize> = HashMap::with_capacity(entries.len());
    for (i, e) in entries.iter().enumerate() {
        last.insert(key(e), i);
    }
    for e in &entries {
        if let Some(i) = last.remove(key(e)) {
            body.extend_from_slice(&out[e.0..e.1]);
            body.extend_from_slice(&out[entries[i].1..entries[i].2]);
            count += 1;
        }
    }
    out.truncate(start);
    out.extend_from_slice(&body);
    count
}

/// Close a container `begin_*` opened, with `count` elements or entries written since.
pub fn end(out: &mut [u8], open: Open, count: usize) {
    let body = out.len() - open.0 - 4;
    let body = u32::try_from(body).expect("a variant value holds less than 4 GiB");
    let count = u32::try_from(count).expect("a variant value holds less than 4 GiB");
    out[open.0..open.0 + 4].copy_from_slice(&body.to_le_bytes());
    out[open.0 + 4..open.0 + 8].copy_from_slice(&count.to_le_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_value_reads_back_as_it_was_written() {
        let mut out = Vec::new();
        let o = begin_object(&mut out);
        put_key(&mut out, "a");
        put_i64(&mut out, -7);
        put_key(&mut out, "b");
        let a = begin_array(&mut out);
        put_str(&mut out, "x");
        put_null(&mut out);
        put_f64(&mut out, 1.5);
        end(&mut out, a, 3);
        put_key(&mut out, "c");
        put_bool(&mut out, true);
        put_key(&mut out, "t");
        put_datetime(&mut out, -1);
        put_key(&mut out, "d");
        put_duration(&mut out, 5);
        put_key(&mut out, "y");
        put_bytes(&mut out, &[0xff, 0]);
        end(&mut out, o, 6);

        assert_eq!(len_of(&out), out.len());
        let out = Encoded::check(&out);
        let Value::Object(entries) = read(out) else {
            panic!("an object")
        };
        let keys: Vec<&str> = entries.map(|(k, _)| k).collect();
        assert_eq!(keys, ["a", "b", "c", "t", "d", "y"], "in the order written");
        assert!(matches!(read(member(out, "a").unwrap()), Value::I64(-7)));
        assert!(matches!(read(member(out, "c").unwrap()), Value::Bool(true)));
        assert!(matches!(read(member(out, "t").unwrap()), Value::DateTime(-1)));
        assert!(matches!(read(member(out, "d").unwrap()), Value::Duration(5)));
        assert!(matches!(read(member(out, "y").unwrap()), Value::Bytes([0xff, 0])));
        assert!(member(out, "z").is_none());
        let Value::Array(items) = read(member(out, "b").unwrap()) else {
            panic!("an array")
        };
        let items: Vec<Encoded> = items.collect();
        assert!(matches!(read(items[0]), Value::Str("x")));
        assert!(matches!(read(items[1]), Value::Null));
        assert!(matches!(read(items[2]), Value::F64(f) if f == 1.5));
    }

    #[test]
    fn a_scalar_has_no_member() {
        let mut out = Vec::new();
        put_str(&mut out, "k");
        let out = Encoded::check(&out);
        assert!(member(out, "k").is_none());
        assert!(!is_object(out));
    }

    #[test]
    #[should_panic(expected = "a variant's strings and keys are UTF-8")]
    fn a_key_that_is_not_utf8_is_refused() {
        let mut out = Vec::new();
        let o = begin_object(&mut out);
        put_key(&mut out, "k");
        put_str(&mut out, "v");
        end(&mut out, o, 1);
        // The key's one byte, after the tag, the body's length, the count and the key's length.
        out[13] = 0x80;
        Encoded::check(&out);
    }

    #[test]
    #[should_panic(expected = "a variant's strings and keys are UTF-8")]
    fn a_variant_column_with_a_string_that_is_not_utf8_is_refused_when_sealed() {
        let mut pool = BufferPool::new();
        let mut out = Vec::new();
        put_str(&mut out, "é");
        out[6] = 0xff;
        let mut b = StrBuilder::new(&mut pool, VarType::Variant, 1);
        b.push_grow(&mut pool, &out);
        b.finish();
    }
}
