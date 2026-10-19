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
pub const NULL: u8 = 0;
pub const FALSE: u8 = 1;
pub const TRUE: u8 = 2;
pub const I64: u8 = 3;
pub const F64: u8 = 4;
pub const STR: u8 = 5;
pub const ARRAY: u8 = 6;
pub const OBJECT: u8 = 7;
pub const DATETIME: u8 = 8;
pub const DURATION: u8 = 9;
pub const BYTES: u8 = 10;

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

/// An array's elements, each as its own encoded bytes.
#[derive(Clone, Copy, Debug)]
pub struct Items<'a> {
    rest: &'a [u8],
    left: u32,
}

/// An object's entries, each a key and its value's encoded bytes.
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

/// The value `b` starts with. The bytes are this module's own, so a malformed one is a bug and panics.
#[inline(always)]
pub fn read(b: &[u8]) -> Value<'_> {
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
            Value::Str(std::str::from_utf8(&b[5..5 + len]).expect("a str is written from a str"))
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
pub fn len_of(b: &[u8]) -> usize {
    debug_assert!(b[0] <= BYTES, "not a variant tag: {}", b[0]);
    match b[0] {
        NULL | FALSE | TRUE => 1,
        I64 | F64 | DATETIME | DURATION => 9,
        // A str, bytes, an array or an object: its length follows the tag.
        _ => 5 + u32_at(b, 1) as usize,
    }
}

/// Whether `b` holds an object.
pub fn is_object(b: &[u8]) -> bool {
    b.first() == Some(&OBJECT)
}

/// The value at `key` inside the object `b`, as its own bytes. `None` where `b` is no object or has no such key.
pub fn member<'a>(b: &'a [u8], key: &str) -> Option<&'a [u8]> {
    at(b, Key::Member(key.as_bytes()))
}

/// What a variant is read at: an object's member by name, or an array's element by position.
#[derive(Clone, Copy)]
pub enum Key<'k> {
    Member(&'k [u8]),
    /// Counts from the end when negative, as `at` does.
    Element(i64),
}

/// The value at `key` inside `b`, as its own bytes. `None` where `b` holds nothing there or null, a key of the
/// other kind among it.
pub fn at<'a>(b: &'a [u8], key: Key) -> Option<&'a [u8]> {
    match (read(b), key) {
        (Value::Object(entries), Key::Member(key)) => {
            entries.into_iter().find(|(k, _)| k.as_bytes() == key).map(|(_, v)| v)
        }
        (Value::Array(items), Key::Element(i)) => {
            let at = crate::ops::helpers::position(i, items.len())?;
            // A null element holds its place in the array, and is nothing as a value.
            items.into_iter().nth(at).filter(|v| v[0] != NULL)
        }
        _ => None,
    }
}

impl<'a> Items<'a> {
    pub fn len(&self) -> usize {
        self.left as usize
    }

    pub fn is_empty(&self) -> bool {
        self.left == 0
    }
}

impl<'a> Iterator for Items<'a> {
    type Item = &'a [u8];

    #[inline]
    fn next(&mut self) -> Option<&'a [u8]> {
        if self.left == 0 {
            return None;
        }
        let n = len_of(self.rest);
        let (value, rest) = self.rest.split_at(n);
        self.rest = rest;
        self.left -= 1;
        Some(value)
    }
}

impl<'a> Entries<'a> {
    pub fn len(&self) -> usize {
        self.left as usize
    }

    pub fn is_empty(&self) -> bool {
        self.left == 0
    }
}

impl<'a> Iterator for Entries<'a> {
    type Item = (&'a str, &'a [u8]);

    #[inline]
    fn next(&mut self) -> Option<(&'a str, &'a [u8])> {
        if self.left == 0 {
            return None;
        }
        let klen = u32_at(self.rest, 0) as usize;
        let key = std::str::from_utf8(&self.rest[4..4 + klen]).expect("a key is written from a str");
        let after = &self.rest[4 + klen..];
        let n = len_of(after);
        let (value, rest) = after.split_at(n);
        self.rest = rest;
        self.left -= 1;
        Some((key, value))
    }
}

/// The value at `key` of each row's object in the variant column `col`, as a variant column: null where a row holds
/// no object or the object has no such key.
pub fn member_column(col: &Column, key: &str, pool: &mut BufferPool) -> Column {
    at_column(col, |_| Some(Key::Member(key.as_bytes())), pool)
}

/// The value at the key `key_of` gives each row of the variant column `col`, as a variant column: null where the row
/// has no key or its value holds nothing at it.
pub fn at_column<'k>(col: &Column, key_of: impl Fn(usize) -> Option<Key<'k>>, pool: &mut BufferPool) -> Column {
    let mut b = StrBuilder::new(pool, VarType::Variant, col.len());
    match col.variant() {
        Ok(v) => {
            for row in 0..col.len() {
                let value = v.validity.is_valid(row).then(|| key_of(row)).flatten();
                match value.and_then(|key| at(v.get(row), key)) {
                    Some(bytes) => b.push_grow(pool, bytes),
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
pub fn put_null(out: &mut Vec<u8>) {
    out.push(NULL);
}

#[inline]
pub fn put_bool(out: &mut Vec<u8>, v: bool) {
    out.push(if v { TRUE } else { FALSE });
}

#[inline]
pub fn put_i64(out: &mut Vec<u8>, v: i64) {
    put_word(out, I64, v.to_le_bytes());
}

#[inline]
pub fn put_f64(out: &mut Vec<u8>, v: f64) {
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
pub fn put_datetime(out: &mut Vec<u8>, micros: i64) {
    put_word(out, DATETIME, micros.to_le_bytes());
}

/// Append a span, in microseconds.
#[inline]
pub fn put_duration(out: &mut Vec<u8>, micros: i64) {
    put_word(out, DURATION, micros.to_le_bytes());
}

/// A tag and its eight bytes, in one append.
#[inline]
fn put_word(out: &mut Vec<u8>, tag: u8, word: [u8; 8]) {
    let mut b = [tag; 9];
    b[1..].copy_from_slice(&word);
    out.extend_from_slice(&b);
}

pub fn put_bytes(out: &mut Vec<u8>, b: &[u8]) {
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

pub fn begin_array(out: &mut Vec<u8>) -> Open {
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
pub fn dedupe(out: &mut Vec<u8>, open: Open) -> usize {
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
        let Value::Object(entries) = read(&out) else {
            panic!("an object")
        };
        let keys: Vec<&str> = entries.map(|(k, _)| k).collect();
        assert_eq!(keys, ["a", "b", "c", "t", "d", "y"], "in the order written");
        assert!(matches!(read(member(&out, "a").unwrap()), Value::I64(-7)));
        assert!(matches!(read(member(&out, "c").unwrap()), Value::Bool(true)));
        assert!(matches!(read(member(&out, "t").unwrap()), Value::DateTime(-1)));
        assert!(matches!(read(member(&out, "d").unwrap()), Value::Duration(5)));
        assert!(matches!(read(member(&out, "y").unwrap()), Value::Bytes([0xff, 0])));
        assert!(member(&out, "z").is_none());
        let Value::Array(items) = read(member(&out, "b").unwrap()) else {
            panic!("an array")
        };
        let items: Vec<&[u8]> = items.collect();
        assert!(matches!(read(items[0]), Value::Str("x")));
        assert!(matches!(read(items[1]), Value::Null));
        assert!(matches!(read(items[2]), Value::F64(f) if f == 1.5));
    }

    #[test]
    fn a_scalar_has_no_member() {
        let mut out = Vec::new();
        put_str(&mut out, "k");
        assert!(member(&out, "k").is_none());
        assert!(!is_object(&out));
    }
}
