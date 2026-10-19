//! A hand-rolled JSON scanner: parse one document and hand each key and its value to a caller-supplied `Sink`, the
//! spread kernel (`format::spread`), with no intermediate tree. A `serde_json::Value` tree allocates for every object,
//! string and array and is thrown away after one read. A value the sink copies is encoded straight into its bytes
//! (`core::variant`).
//!
//! `_raw` is untrusted input. Nothing here panics on it: every path returns `Result`/`bool`, nesting is bounded by
//! `MAX_DEPTH`, and string content is validated as UTF-8. The tests differential-test this scanner against `serde_json`
//! (a dev-dependency).
//!
//! The scanner reads what log systems write beyond RFC 8259 and `serde_json`: a trailing comma before `}` or `]`, raw
//! control bytes in a string, `\xHH` escapes (nginx, Apache), a surrogate escape without its partner, and `NaN` and
//! `Infinity` (Python). Each is read only where strict JSON fails, so a valid document reads as it always did. Comments,
//! single quotes, unquoted keys and the rest of JSON5 stay rejected.

use std::borrow::Cow;

use crate::core::buffer::check;
use crate::core::keys::Keys;
use crate::format::json::MAX_NESTING;
use crate::format::spread::{Lost, Step};
use crate::format::tag::{self, Bytes, Emit, Tag};
use crate::format::typed::Slot;

/// Nesting limit, the same as `serde_json`'s default. A stack overflow aborts rather than unwinding, so this must be
/// exhausted first.
const MAX_DEPTH: u32 = 128;

/// Receives each key of the document and its value, as the step it answers asks: a JSON `null` is no value and is not
/// entered. A `Tag::Str` and a key borrow from the input when nothing needed unescaping.
pub trait Sink<'d> {
    /// Enter `key`; `object` says its value is an object.
    fn enter(&mut self, key: Cow<'d, str>, object: bool) -> Step;
    /// Where a `Step::Whole` value is written.
    type Leaf<'a>: Emit
    where
        Self: 'a;
    /// Where the value of a `Step::Whole` goes.
    fn leaf(&mut self) -> Self::Leaf<'_>;
    /// A scalar `Step::Whole` value, written to its leaf.
    fn scalar(&mut self, tag: &Tag<'_>) {
        self.leaf().scalar(tag);
    }
    /// The buffer a `Step::Copy` value is encoded into, its key's depth, and whether a key under it splits at its dots.
    fn copy(&mut self) -> (&mut Vec<u8>, usize, bool);
    /// The copied value is complete.
    fn copied(&mut self);
    /// The copied value is dropped: it held nothing, or the scanner enters it instead.
    fn uncopy(&mut self);
    /// A key repeats at `path` inside the value of the key entered last, once that value is written. The value keeps
    /// the key's last value, in its first place.
    fn repeated(&mut self, path: &[Cow<'d, str>]);
    /// The scalar of a `Step::Refused`.
    fn refused(&mut self, _tag: &Tag<'_>) {}
    /// Leave the key entered last.
    fn leave(&mut self);
}

/// What a row's scan reuses from the last row's: the keys of the objects being copied.
#[derive(Default)]
pub struct Scratch<'d> {
    /// The keys of the object being copied at each depth.
    keys: Vec<Keys<Cow<'d, str>, ()>>,
    /// What the last document's read lost.
    pub lost: Lost,
}

/// Parse `bytes` as one JSON document.
///
/// Returns `true` for a well-formed document whose top-level value is an object with nothing but whitespace after it.
/// Everything else is `false`.
pub fn parse_object<'d>(bytes: &'d [u8], scratch: &mut Scratch<'d>, sink: &mut impl Sink<'d>) -> bool {
    // One check of the whole row: checking each string costs more in calls than in bytes. Any byte that is not UTF-8
    // would fail the row anyway, inside a string or out.
    let Ok(text) = std::str::from_utf8(bytes) else {
        scratch.lost = Lost::default();
        return false;
    };
    let mut s = Scanner {
        b: bytes,
        t: text,
        i: 0,
        lost: Lost::default(),
    };
    s.skip_ws();
    let ok = s.peek() == Some(b'{') && scan_object(&mut s, scratch, sink, 0).is_ok() && {
        s.skip_ws();
        s.i == s.b.len()
    };
    // A document that fails is a parse exception, which says enough.
    scratch.lost = if ok { s.lost } else { Lost::default() };
    ok
}

struct Scanner<'a> {
    b: &'a [u8],
    /// `b`, validated once.
    t: &'a str,
    i: usize,
    lost: Lost,
}

impl<'a> Scanner<'a> {
    #[inline]
    fn peek(&self) -> Option<u8> {
        self.b.get(self.i).copied()
    }

    #[inline]
    fn bump(&mut self) -> Option<u8> {
        let c = self.peek();
        if c.is_some() {
            self.i += 1;
        }
        c
    }

    #[inline]
    fn skip_ws(&mut self) {
        while matches!(self.peek(), Some(b' ' | b'\t' | b'\n' | b'\r')) {
            self.i += 1;
        }
    }

    fn expect(&mut self, c: u8) -> Result<(), ()> {
        if self.bump() == Some(c) { Ok(()) } else { Err(()) }
    }

    fn expect_literal(&mut self, lit: &[u8]) -> Result<(), ()> {
        if self.b[self.i..].starts_with(lit) {
            self.i += lit.len();
            Ok(())
        } else {
            Err(())
        }
    }

    /// A value at `at` that is not JSON, read again as a number an f64 cannot hold: a number past its range, which
    /// `scan_number` refused having read it whole, or `NaN`, `Infinity` or `-Infinity`. No value, and the read lost one.
    #[cold]
    #[inline(never)]
    fn non_finite<T>(&mut self, at: usize) -> Result<Option<T>, ()> {
        let read = self.t.get(at..self.i).unwrap_or("");
        let whole = read.ends_with(|c: char| c.is_ascii_digit());
        if !(whole && read.parse::<f64>().is_ok_and(f64::is_infinite)) {
            let words: [&[u8]; 3] = [b"NaN", b"Infinity", b"-Infinity"];
            let word = words.iter().find(|w| self.b[at..].starts_with(w)).ok_or(())?;
            self.i = at + word.len();
        }
        self.lost.non_finite = true;
        Ok(None)
    }

    /// Advances to the next `"` or `\`, or to the end. Eight bytes at a time, as `serde_json` does.
    #[inline]
    fn skip_plain(&mut self) {
        const ONES: u64 = u64::MAX / 255;
        for chunk in self.b[self.i..].as_chunks::<8>().0 {
            let w = u64::from_le_bytes(*chunk);
            let q = w ^ (ONES * b'"' as u64);
            let b = w ^ (ONES * b'\\' as u64);
            let hit = ((q.wrapping_sub(ONES) & !q) | (b.wrapping_sub(ONES) & !b)) & (ONES << 7);
            if hit != 0 {
                self.i += hit.trailing_zeros() as usize / 8;
                return;
            }
            self.i += 8;
        }
        while matches!(self.peek(), Some(c) if c != b'"' && c != b'\\') {
            self.i += 1;
        }
    }

    /// Skips whitespace, then consumes `c` if it is next. The trailing-comma lookahead after a comma.
    #[inline]
    fn skip_ws_then_bump_if(&mut self, c: u8) -> bool {
        self.skip_ws();
        if self.peek() == Some(c) {
            self.i += 1;
            true
        } else {
            false
        }
    }
}

/// `s` positioned at `{`, not yet consumed. Each key goes to `sink`. Every value the sink does not enter is copied: one
/// walk of a value, whichever step takes it.
fn scan_object<'d>(
    s: &mut Scanner<'d>,
    scratch: &mut Scratch<'d>,
    sink: &mut impl Sink<'d>,
    depth: u32,
) -> Result<(), ()> {
    if depth >= MAX_DEPTH {
        return Err(());
    }
    s.bump(); // '{'
    s.skip_ws();
    if s.peek() == Some(b'}') {
        s.bump();
        return Ok(());
    }
    loop {
        s.skip_ws();
        if s.peek() != Some(b'"') {
            return Err(());
        }
        let key = scan_string(s)?;
        s.skip_ws();
        s.expect(b':')?;
        s.skip_ws();
        // A scalar is read once, whichever step takes it: the common case, and the cheapest.
        let scalar = match s.peek() {
            Some(b'{' | b'[') => None,
            _ => Some(scan_scalar(s)?),
        };
        // A `null`, or a number no f64 holds, is no value: absent, no key.
        if scalar.as_ref().is_none_or(Option::is_some) {
            let object = s.peek() == Some(b'{');
            let step = sink.enter(key, object);
            if let Some(Some(tag)) = scalar {
                match step {
                    Step::Whole => sink.scalar(&tag),
                    Step::Copy => {
                        tag::encode(&tag, sink.copy().0);
                        sink.copied();
                    }
                    Step::Refused => sink.refused(&tag),
                    Step::Into | Step::Skip => {}
                }
            } else if step == Step::Into {
                scan_object(s, scratch, sink, depth + 1)?;
            } else if step == Step::Copy {
                let start = s.i;
                let (out, level, split) = sink.copy();
                let mut copy = Copier::new(&mut scratch.keys, split);
                let kept = copy.value(s, &mut Bytes::new(out), depth + 1, Some(level), None);
                let repeats = copy.repeats;
                match kept {
                    Ok(kept) => {
                        repeats.iter().for_each(|path| sink.repeated(path));
                        match kept {
                            true => sink.copied(),
                            false => sink.uncopy(),
                        }
                    }
                    Err(Fail::Bad) => return Err(()),
                    // An object too deep to copy whole, or one whose keys split: entered key by key instead.
                    Err(Fail::Enter) => {
                        sink.uncopy();
                        s.i = start;
                        scan_object(s, scratch, sink, depth + 1)?;
                    }
                }
            } else {
                // A container taken whole is written straight to its leaf; a skipped one, nowhere.
                let mut copy = Copier::new(&mut scratch.keys, false);
                if step == Step::Whole {
                    copy.value(s, &mut sink.leaf(), depth + 1, None, None).map_err(|_| ())?;
                    copy.repeats.iter().for_each(|path| sink.repeated(path));
                } else {
                    copy.value(s, &mut Slot::Skip, depth + 1, None, None).map_err(|_| ())?;
                }
            }
            sink.leave();
            check();
        }

        s.skip_ws();
        match s.bump() {
            Some(b',') => {
                if s.skip_ws_then_bump_if(b'}') {
                    return Ok(());
                }
                continue;
            }
            Some(b'}') => return Ok(()),
            _ => return Err(()),
        }
    }
}

/// Why a value was not copied.
enum Fail {
    /// Malformed.
    Bad,
    /// An object past the nesting bound, or one whose keys split at their dots: the scanner enters it key by key.
    Enter,
}

/// The keys from the copied value down to the object being copied, innermost first.
struct Up<'p, 'd> {
    key: &'p Cow<'d, str>,
    up: Option<&'p Up<'p, 'd>>,
}

/// One value, written to an emitter (`tag::Emit`) as `s` walks it. A repeated key keeps its last value, in its first
/// place, and its path joins `repeats`.
struct Copier<'c, 'd> {
    /// The keys of the object being copied at each depth.
    keys: &'c mut Vec<Keys<Cow<'d, str>, ()>>,
    split: bool,
    repeats: Vec<Vec<Cow<'d, str>>>,
}

impl<'c, 'd> Copier<'c, 'd> {
    fn new(keys: &'c mut Vec<Keys<Cow<'d, str>, ()>>, split: bool) -> Self {
        Copier {
            keys,
            split,
            repeats: Vec::new(),
        }
    }

    /// Write the value `s` is at to `out`. `level` is how many keys deep an object it holds has its keys, `None` inside
    /// an array or a value taken whole; under `self.split` a key outside an array splits at its dots, which only a
    /// walk key by key does. `Ok(false)` where there is nothing to keep: no value, or an object under a key holding nothing,
    /// which the caller takes back. An array keeps an empty object and a null element, which hold their places.
    // Inlined into its callers: the walk's inner loop, measured about 3% faster on copied containers.
    #[inline(always)]
    fn value<O: Emit>(
        &mut self,
        s: &mut Scanner<'d>,
        out: &mut O,
        depth: u32,
        level: Option<usize>,
        up: Option<&Up<'_, 'd>>,
    ) -> Result<bool, Fail> {
        match s.peek() {
            Some(b'{') => self.object(s, out, depth, level, up),
            Some(b'[') => self.array(s, out, depth, up).map(|()| true),
            _ => match scan_scalar(s).map_err(|()| Fail::Bad)? {
                Some(tag) => {
                    out.scalar(&tag);
                    Ok(true)
                }
                None => Ok(false),
            },
        }
    }

    /// `s` positioned at `{`, not yet consumed.
    fn object<O: Emit>(
        &mut self,
        s: &mut Scanner<'d>,
        out: &mut O,
        depth: u32,
        level: Option<usize>,
        up: Option<&Up<'_, 'd>>,
    ) -> Result<bool, Fail> {
        if depth >= MAX_DEPTH {
            return Err(Fail::Bad);
        }
        // Past the nesting bound the rest of a name is one key, which only a walk key by key spells.
        if level.is_some_and(|l| l + 1 >= MAX_NESTING) {
            return Err(Fail::Enter);
        }
        s.bump(); // '{'
        s.skip_ws();
        out.begin_object();
        let d = depth as usize;
        if self.keys.len() <= d {
            self.keys.resize_with(d + 1, Keys::default);
        }
        self.keys[d].clear();
        let mut repeats = false;
        if s.peek() == Some(b'}') {
            s.bump();
        } else {
            loop {
                s.skip_ws();
                if s.peek() != Some(b'"') {
                    return Err(Fail::Bad);
                }
                let key = scan_string(s).map_err(|()| Fail::Bad)?;
                if self.split && level.is_some() && key.contains('.') {
                    return Err(Fail::Enter);
                }
                s.skip_ws();
                s.expect(b':').map_err(|()| Fail::Bad)?;
                s.skip_ws();
                let link = Up { key: &key, up };
                let kept = self.value(s, &mut out.field(&key), depth + 1, level.map(|l| l + 1), Some(&link))?;
                check();
                if kept {
                    if self.keys[d].find(&key).is_some() {
                        repeats = true;
                        self.repeat(&link);
                    } else {
                        self.keys[d].push(key, ());
                    }
                } else {
                    out.unfield();
                }
                s.skip_ws();
                match s.bump() {
                    Some(b',') => {
                        if s.skip_ws_then_bump_if(b'}') {
                            break;
                        }
                        continue;
                    }
                    Some(b'}') => break,
                    _ => return Err(Fail::Bad),
                }
            }
        }
        let count = self.keys[d].len();
        out.end_object(count, repeats);
        Ok(count > 0 || level.is_none())
    }

    /// Note a repeat of the key `at` names, by its path from the copied value.
    fn repeat(&mut self, at: &Up<'_, 'd>) {
        let mut path = Vec::new();
        let mut up = Some(at);
        while let Some(u) = up {
            path.push(u.key.clone());
            up = u.up;
        }
        path.reverse();
        self.repeats.push(path);
    }

    /// `s` positioned at `[`, not yet consumed.
    fn array<O: Emit>(
        &mut self,
        s: &mut Scanner<'d>,
        out: &mut O,
        depth: u32,
        up: Option<&Up<'_, 'd>>,
    ) -> Result<(), Fail> {
        if depth >= MAX_DEPTH {
            return Err(Fail::Bad);
        }
        s.bump(); // '['
        s.skip_ws();
        out.begin_array();
        let mut count = 0;
        if s.peek() == Some(b']') {
            s.bump();
        } else {
            loop {
                s.skip_ws();
                let mut element = out.element();
                if !self.value(s, &mut element, depth + 1, None, up)? {
                    element.null();
                }
                check();
                count += 1;
                s.skip_ws();
                match s.bump() {
                    Some(b',') => {
                        if s.skip_ws_then_bump_if(b']') {
                            break;
                        }
                        continue;
                    }
                    Some(b']') => break,
                    _ => return Err(Fail::Bad),
                }
            }
        }
        out.end_array(count);
        Ok(())
    }
}

/// A non-container value. `Ok(None)` is no value: a JSON `null`, or a number no f64 holds.
fn scan_scalar<'d>(s: &mut Scanner<'d>) -> Result<Option<Tag<'d>>, ()> {
    match s.peek() {
        Some(b'"') => scan_string(s).map(|v| Some(Tag::Str(v))),
        Some(b't') => {
            s.expect_literal(b"true")?;
            Ok(Some(Tag::Bool(true)))
        }
        Some(b'f') => {
            s.expect_literal(b"false")?;
            Ok(Some(Tag::Bool(false)))
        }
        Some(b'n') => {
            s.expect_literal(b"null")?;
            Ok(None)
        }
        Some(b'-') | Some(b'0'..=b'9') => {
            let at = s.i;
            scan_number(s).map(Some).or_else(|()| s.non_finite(at))
        }
        _ => s.non_finite(s.i),
    }
}

/// `s` positioned at the opening `"`. A string with no escape borrows from the input; only a decoded escape forces an
/// owned copy.
fn scan_string<'d>(s: &mut Scanner<'d>) -> Result<Cow<'d, str>, ()> {
    s.bump(); // opening quote
    let start = s.i;
    s.skip_plain();
    let run = s.t.get(start..s.i).ok_or(())?;
    match s.bump() {
        Some(b'"') => Ok(Cow::Borrowed(run)),
        Some(b'\\') => {
            let mut out = String::from(run);
            scan_escape(s, &mut out)?;
            scan_string_tail(s, &mut out)?;
            Ok(Cow::Owned(out))
        }
        _ => Err(()), // EOF
    }
}

/// Continues an already-escaped string from `out` to the closing quote.
fn scan_string_tail(s: &mut Scanner, out: &mut String) -> Result<(), ()> {
    loop {
        let start = s.i;
        s.skip_plain();
        if s.i > start {
            out.push_str(s.t.get(start..s.i).ok_or(())?);
        }
        match s.bump() {
            Some(b'"') => return Ok(()),
            Some(b'\\') => scan_escape(s, out)?,
            _ => return Err(()), // EOF
        }
    }
}

fn scan_escape(s: &mut Scanner, out: &mut String) -> Result<(), ()> {
    match s.bump() {
        Some(b'"') => out.push('"'),
        Some(b'\\') => out.push('\\'),
        Some(b'/') => out.push('/'),
        Some(b'b') => out.push('\u{8}'),
        Some(b'f') => out.push('\u{c}'),
        Some(b'n') => out.push('\n'),
        Some(b'r') => out.push('\r'),
        Some(b't') => out.push('\t'),
        Some(b'u') => {
            let mut cp = scan_hex(s, 4)?;
            if (0xD800..=0xDBFF).contains(&cp) && s.b[s.i..].starts_with(b"\\u") {
                let at = s.i;
                s.i += 2;
                let low = scan_hex(s, 4)?;
                match (0xDC00..=0xDFFF).contains(&low) {
                    true => cp = 0x10000 + ((cp - 0xD800) << 10) + (low - 0xDC00),
                    false => s.i = at,
                }
            }
            // A surrogate without its partner names no character.
            let c = char::from_u32(cp);
            s.lost.replaced |= c.is_none();
            out.push(c.unwrap_or(char::REPLACEMENT_CHARACTER));
        }
        Some(b'x') => {
            // A run of `\xHH` is bytes, decoded together: nginx writes `é` as `\xC3\xA9`.
            let mut bytes = vec![scan_hex(s, 2)? as u8];
            while s.b[s.i..].starts_with(b"\\x") {
                s.i += 2;
                bytes.push(scan_hex(s, 2)? as u8);
            }
            let text = String::from_utf8_lossy(&bytes);
            s.lost.replaced |= matches!(text, Cow::Owned(_));
            out.push_str(&text);
        }
        // Any other escaped character stands for itself; a non-ASCII one is left to the run after the backslash.
        Some(c) if c.is_ascii() => out.push(c as char),
        Some(_) => s.i -= 1,
        None => return Err(()),
    }
    Ok(())
}

/// `n` hex digits.
fn scan_hex(s: &mut Scanner, n: usize) -> Result<u32, ()> {
    let mut v = 0u32;
    for _ in 0..n {
        let d = match s.bump().ok_or(())? {
            c @ b'0'..=b'9' => c - b'0',
            c @ b'a'..=b'f' => c - b'a' + 10,
            c @ b'A'..=b'F' => c - b'A' + 10,
            _ => return Err(()),
        };
        v = v * 16 + d as u32;
    }
    Ok(v)
}

/// `s` positioned at `-` or a digit. A `.` or exponent makes the token float-shaped; the conversion itself is
/// `str::parse`, which is correctly rounded.
fn scan_number<'d>(s: &mut Scanner<'d>) -> Result<Tag<'d>, ()> {
    let (text, is_float) = number_text(s)?;
    // An out-of-range exponent is infinite, which an f64 column does not hold: refused here, read again as no value by
    // `Scanner::non_finite`.
    let float = || crate::ops::convert::parse_f64(text).map(Tag::F64).ok_or(());
    if is_float {
        float()
    } else {
        match text.parse::<i64>() {
            Ok(i) => Ok(Tag::I64(i)),
            // Does not fit i64: read as f64, which a declared `i64` field then converts by the coercion table.
            Err(_) => float(),
        }
    }
}

/// The number `s` is at, `-` or a digit, as its text, and whether it is float-shaped: a `.` or an exponent.
fn number_text<'d>(s: &mut Scanner<'d>) -> Result<(&'d str, bool), ()> {
    let start = s.i;
    let mut is_float = false;

    if s.peek() == Some(b'-') {
        s.i += 1;
    }
    match s.bump() {
        Some(b'0') => {}
        Some(b'1'..=b'9') => {
            while matches!(s.peek(), Some(b'0'..=b'9')) {
                s.i += 1;
            }
        }
        _ => return Err(()),
    }
    if s.peek() == Some(b'.') {
        is_float = true;
        s.i += 1;
        if !matches!(s.peek(), Some(b'0'..=b'9')) {
            return Err(());
        }
        while matches!(s.peek(), Some(b'0'..=b'9')) {
            s.i += 1;
        }
    }
    if matches!(s.peek(), Some(b'e' | b'E')) {
        is_float = true;
        s.i += 1;
        if matches!(s.peek(), Some(b'+' | b'-')) {
            s.i += 1;
        }
        if !matches!(s.peek(), Some(b'0'..=b'9')) {
            return Err(());
        }
        while matches!(s.peek(), Some(b'0'..=b'9')) {
            s.i += 1;
        }
    }

    Ok((s.t.get(start..s.i).ok_or(())?, is_float))
}

/// A document read one value at a time, the caller naming what each value must be: for a shape read into structs of
/// the caller's own, such as an OTLP export or a HEC envelope, where `parse_object` hands a document's fields to a
/// sink. It reads what `parse_object` reads, a trailing comma, raw control bytes and `\xHH` in a string among it, but
/// a bare `NaN` or `Infinity` is no number here.
pub struct Reader<'d> {
    s: Scanner<'d>,
    depth: u32,
    /// The container just opened has had no member yet, so its first takes no comma.
    first: bool,
}

/// The document is no JSON, or nests past the bound.
#[derive(Debug, PartialEq)]
pub struct Malformed;

impl From<Malformed> for String {
    fn from(_: Malformed) -> String {
        "not JSON".to_string()
    }
}

/// The value a reader is at. A container's opening is read: [`Reader::key`] or [`Reader::item`] reads on into it.
#[derive(Debug, PartialEq)]
pub enum Next<'d> {
    Null,
    Bool(bool),
    /// A number as the document writes it, for the caller to read at the width it needs.
    Number(&'d str),
    Str(Cow<'d, str>),
    Object,
    Array,
}

impl<'d> Reader<'d> {
    /// `bytes`, at their first value. `Malformed` where they are not UTF-8: one check of the whole document, as
    /// `parse_object` makes.
    pub fn new(bytes: &'d [u8]) -> Result<Reader<'d>, Malformed> {
        Ok(Reader::of_str(std::str::from_utf8(bytes).map_err(|_| Malformed)?))
    }

    /// `text`, at its first value: for a document read in parts, each part from where the last one ended, without
    /// checking the whole again.
    pub fn of_str(text: &'d str) -> Reader<'d> {
        let s = Scanner {
            b: text.as_bytes(),
            t: text,
            i: 0,
            lost: Lost::default(),
        };
        Reader {
            s,
            depth: 0,
            first: false,
        }
    }

    /// The bytes read so far.
    pub fn at(&self) -> usize {
        self.s.i
    }

    /// Whether nothing but whitespace is left.
    pub fn end(&mut self) -> bool {
        self.s.skip_ws();
        self.s.i == self.s.b.len()
    }

    /// The value the reader is at.
    pub fn value(&mut self) -> Result<Next<'d>, Malformed> {
        self.s.skip_ws();
        let s = &mut self.s;
        let read = match s.peek() {
            Some(open @ (b'{' | b'[')) => {
                if self.depth >= MAX_DEPTH {
                    return Err(Malformed);
                }
                s.i += 1;
                self.depth += 1;
                self.first = true;
                return Ok(if open == b'{' { Next::Object } else { Next::Array });
            }
            Some(b'"') => scan_string(s).map(Next::Str),
            Some(b't') => s.expect_literal(b"true").map(|()| Next::Bool(true)),
            Some(b'f') => s.expect_literal(b"false").map(|()| Next::Bool(false)),
            Some(b'n') => s.expect_literal(b"null").map(|()| Next::Null),
            Some(b'-' | b'0'..=b'9') => number_text(s).map(|(text, _)| Next::Number(text)),
            _ => Err(()),
        };
        read.map_err(|()| Malformed)
    }

    /// The next key of the object open, with its `:` read, or `None` at the object's end.
    pub fn key(&mut self) -> Result<Option<Cow<'d, str>>, Malformed> {
        if !self.more(b'}')? {
            return Ok(None);
        }
        let s = &mut self.s;
        if s.peek() != Some(b'"') {
            return Err(Malformed);
        }
        let key = scan_string(s).map_err(|()| Malformed)?;
        s.skip_ws();
        s.expect(b':').map_err(|()| Malformed)?;
        Ok(Some(key))
    }

    /// Whether the array open has another element, which [`Reader::value`] reads. `false` at the array's end.
    pub fn item(&mut self) -> Result<bool, Malformed> {
        self.more(b']')
    }

    /// Past the comma before the open container's next member, or past `close`, its end.
    fn more(&mut self, close: u8) -> Result<bool, Malformed> {
        let s = &mut self.s;
        s.skip_ws();
        if !std::mem::replace(&mut self.first, false) {
            match s.peek() {
                Some(b',') => {
                    s.i += 1;
                    s.skip_ws();
                }
                Some(c) if c == close => {}
                _ => return Err(Malformed),
            }
        }
        if s.peek() != Some(close) {
            return Ok(true);
        }
        s.i += 1;
        self.depth -= 1;
        Ok(false)
    }

    /// Whether the value the reader is at is `null`, read past it if so.
    pub fn null(&mut self) -> Result<bool, Malformed> {
        self.s.skip_ws();
        if self.s.peek() != Some(b'n') {
            return Ok(false);
        }
        self.s.expect_literal(b"null").map_err(|()| Malformed)?;
        Ok(true)
    }

    /// Past the value the reader is at, whatever it holds.
    pub fn skip(&mut self) -> Result<(), Malformed> {
        match self.value()? {
            Next::Object => {
                while self.key()?.is_some() {
                    self.skip()?;
                }
            }
            Next::Array => {
                while self.item()? {
                    self.skip()?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// The value the reader is at, appended to `out` in `core::variant`'s encoding, as `parse_object` copies one: a
    /// `null` member of an object is no entry, and of a key given twice the last value stands, in the first one's
    /// place. A `null` is a variant null. A number no f64 holds, `NaN`, `Infinity` or text that names no character
    /// refuses the value, which would otherwise lose it.
    pub fn variant(&mut self, out: &mut Vec<u8>) -> Result<(), Malformed> {
        self.s.skip_ws();
        (self.s.lost.non_finite, self.s.lost.replaced) = (false, false);
        let mut keys = Vec::new();
        let mut copy = Copier::new(&mut keys, false);
        let kept =
            (copy.value(&mut self.s, &mut Bytes::new(&mut *out), self.depth, None, None)).map_err(|_| Malformed)?;
        if self.s.lost.non_finite || self.s.lost.replaced {
            return Err(Malformed);
        }
        if !kept {
            crate::core::variant::put_null(out);
        }
        Ok(())
    }

    /// The value the reader is at, skipped, as the bytes the document writes it in.
    pub fn raw(&mut self) -> Result<&'d [u8], Malformed> {
        self.s.skip_ws();
        let start = self.s.i;
        self.skip()?;
        Ok(&self.s.b[start..self.s.i])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::tree;

    /// Every value at its name, flattened, last write winning per name: a sink that enters each object. A value is kept
    /// as its encoding, decoded when the scan ends.
    #[derive(Default)]
    struct Collect<'d> {
        path: Vec<Cow<'d, str>>,
        got: Vec<(String, Vec<u8>)>,
        repeats: Vec<String>,
    }
    impl<'d> Sink<'d> for Collect<'d> {
        fn enter(&mut self, key: Cow<'d, str>, object: bool) -> Step {
            self.path.push(key);
            if object { Step::Into } else { Step::Whole }
        }
        type Leaf<'a>
            = Bytes<&'a mut Vec<u8>>
        where
            Self: 'a;
        fn leaf(&mut self) -> Bytes<&mut Vec<u8>> {
            let mut name = String::new();
            for k in &self.path {
                tree::push_key(&mut name, k);
            }
            let at = match self.got.iter().position(|(p, _)| *p == name) {
                Some(at) => at,
                None => {
                    self.got.push((name, Vec::new()));
                    self.got.len() - 1
                }
            };
            let bytes = &mut self.got[at].1;
            bytes.clear();
            Bytes::new(bytes)
        }
        fn repeated(&mut self, path: &[Cow<'d, str>]) {
            let mut name = String::new();
            for k in self.path.iter().chain(path) {
                tree::push_key(&mut name, k);
            }
            self.repeats.push(name);
        }
        fn copy(&mut self) -> (&mut Vec<u8>, usize, bool) {
            unreachable!("nothing is copied")
        }
        fn copied(&mut self) {}
        fn uncopy(&mut self) {}
        fn leave(&mut self) {
            self.path.pop();
        }
    }

    fn scan(doc: &str) -> Option<Vec<(String, Tag<'static>)>> {
        collect(doc).map(|sink| decoded(sink.got))
    }

    fn collect(doc: &str) -> Option<Collect<'_>> {
        let mut sink = Collect::default();
        parse_object(doc.as_bytes(), &mut Scratch::default(), &mut sink).then_some(sink)
    }

    fn decoded(got: Vec<(String, Vec<u8>)>) -> Vec<(String, Tag<'static>)> {
        let decode = |b: Vec<u8>| tag::decode(Box::leak(b.into_boxed_slice())).expect("a value");
        got.into_iter().map(|(name, b)| (name, decode(b))).collect()
    }

    /// What copying the value of `doc`'s one key encodes, decoded.
    fn copied(doc: &str) -> Option<Option<Tag<'_>>> {
        struct One(Vec<u8>, bool);
        impl<'d> Sink<'d> for One {
            fn enter(&mut self, _: Cow<'d, str>, _: bool) -> Step {
                Step::Copy
            }
            type Leaf<'a> = Slot<'a>;
            fn leaf(&mut self) -> Slot<'_> {
                Slot::Skip
            }
            fn repeated(&mut self, _: &[Cow<'d, str>]) {}
            fn copy(&mut self) -> (&mut Vec<u8>, usize, bool) {
                (&mut self.0, 1, false)
            }
            fn copied(&mut self) {
                self.1 = true;
            }
            fn uncopy(&mut self) {
                self.0.clear();
            }
            fn leave(&mut self) {}
        }
        let mut sink = One(Vec::new(), false);
        if !parse_object(doc.as_bytes(), &mut Scratch::default(), &mut sink) {
            return None;
        }
        let bytes = Box::leak(sink.0.into_boxed_slice());
        Some(sink.1.then(|| tag::decode(bytes)).flatten())
    }

    #[test]
    fn flat_object() {
        let got = scan(r#"{"a":1,"b":"x","c":true}"#).unwrap();
        assert_eq!(
            got,
            vec![
                ("a".into(), Tag::I64(1)),
                ("b".into(), Tag::Str("x".into())),
                ("c".into(), Tag::Bool(true)),
            ]
        );
    }

    #[test]
    fn nested_object_flattens_to_dotted_paths() {
        let got = scan(r#"{"source":{"ip":"1.2.3.4","port":443}}"#).unwrap();
        assert_eq!(
            got,
            vec![
                ("source.ip".into(), Tag::Str("1.2.3.4".into())),
                ("source.port".into(), Tag::I64(443)),
            ]
        );
    }

    #[test]
    fn deeply_nested_object() {
        let got = scan(r#"{"a":{"b":{"c":{"d":1}}}}"#).unwrap();
        assert_eq!(got, vec![("a.b.c.d".into(), Tag::I64(1))]);
    }

    #[test]
    fn null_and_missing_contribute_no_tag() {
        let got = scan(r#"{"a":null,"b":1}"#).unwrap();
        assert_eq!(got, vec![("b".into(), Tag::I64(1))]);
    }

    #[test]
    fn empty_object_scans_to_nothing() {
        assert!(scan("{}").unwrap().is_empty());
        assert!(scan(r#"{"a":{}}"#).unwrap().is_empty());
    }

    #[test]
    fn an_array_decodes_its_elements() {
        let got = scan(r#"{"tags":[1, "x", true]}"#).unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].0, "tags");
        match &got[0].1 {
            Tag::Array(elements) => {
                assert!(matches!(elements[0], Some(Tag::I64(1))));
                assert!(matches!(&elements[1], Some(Tag::Str(s)) if s == "x"));
                assert!(matches!(elements[2], Some(Tag::Bool(true))));
            }
            other => panic!("expected Array, got {other:?}"),
        }
    }

    #[test]
    fn a_nested_array_element_decodes_recursively() {
        let got = scan(r#"{"tags":[1, [2, 3]]}"#).unwrap();
        match &got[0].1 {
            Tag::Array(elements) => {
                assert!(matches!(elements[0], Some(Tag::I64(1))));
                match &elements[1] {
                    Some(Tag::Array(inner)) => {
                        assert!(matches!(inner[0], Some(Tag::I64(2))));
                        assert!(matches!(inner[1], Some(Tag::I64(3))));
                    }
                    other => panic!("expected a nested Array element, got {other:?}"),
                }
            }
            other => panic!("expected Array, got {other:?}"),
        }
    }

    #[test]
    fn an_object_element_decodes_into_tag_object() {
        let got = scan(r#"{"a":[{"x":1},{"y":2}],"b":3}"#).unwrap();
        assert_eq!(got[1], ("b".into(), Tag::I64(3)));
        match &got[0].1 {
            Tag::Array(elements) => {
                assert_eq!(elements.len(), 2);
                match &elements[0] {
                    Some(Tag::Object(fields)) => {
                        assert_eq!(fields.len(), 1);
                        assert_eq!(fields[0].0, "x");
                        assert!(matches!(fields[0].1, Tag::I64(1)));
                    }
                    other => panic!("expected an Object element, got {other:?}"),
                }
            }
            other => panic!("expected Array, got {other:?}"),
        }
    }

    #[test]
    fn a_null_valued_key_inside_an_object_element_contributes_no_field() {
        let got = scan(r#"{"tags":[{"x":1,"y":null}]}"#).unwrap();
        match &got[0].1 {
            Tag::Array(elements) => match &elements[0] {
                Some(Tag::Object(fields)) => {
                    assert_eq!(fields.len(), 1);
                    assert_eq!(fields[0].0, "x");
                }
                other => panic!("expected an Object element, got {other:?}"),
            },
            other => panic!("expected Array, got {other:?}"),
        }
    }

    #[test]
    fn string_escapes_decode_correctly() {
        let got = scan(r#"{"s":"a\"b\\c\nd\teA"}"#).unwrap();
        match &got[0].1 {
            Tag::Str(s) => assert_eq!(s, "a\"b\\c\nd\teA"),
            other => panic!("{other:?}"),
        }
    }

    /// `\uXXXX` names a UTF-16 code unit, so a character past the BMP arrives as a surrogate pair and must decode to
    /// one character, not two.
    #[test]
    fn a_u_escape_decodes_to_the_character_it_names() {
        for (doc, want) in [
            (r#"{"s":"\u00e9"}"#, "\u{e9}"),
            (r#"{"s":"\u00E9"}"#, "\u{e9}"),
            (r#"{"s":"\ud83d\ude00"}"#, "\u{1F600}"),
        ] {
            assert_eq!(scan(doc).unwrap(), vec![("s".into(), Tag::Str(want.into()))], "{doc}");
        }
    }

    #[test]
    fn a_surrogate_without_its_partner_reads_as_a_replacement_character() {
        for (doc, want) in [
            (r#"{"s":"\ud83d"}"#, "\u{FFFD}"),
            (r#"{"s":"\ud83dx"}"#, "\u{FFFD}x"),
            (r#"{"s":"\udc00"}"#, "\u{FFFD}"),
            (r#"{"s":"\ud83d\u0041"}"#, "\u{FFFD}A"),
            (r#"{"s":"\ud83d\ud83d\ude00"}"#, "\u{FFFD}\u{1F600}"),
        ] {
            assert_eq!(
                read(doc),
                (Some(vec![("s".into(), Tag::Str(want.into()))]), REPLACED),
                "{doc}"
            );
        }
    }

    #[test]
    fn integer_and_float_numbers_are_distinguished() {
        let got = scan(r#"{"a":5,"b":5.5,"c":5e2,"d":-3}"#).unwrap();
        assert!(matches!(got[0].1, Tag::I64(5)));
        assert!(matches!(got[1].1, Tag::F64(x) if x == 5.5));
        assert!(matches!(got[2].1, Tag::F64(x) if x == 500.0));
        assert!(matches!(got[3].1, Tag::I64(-3)));
    }

    #[test]
    fn integer_overflowing_i64_widens_to_f64() {
        let got = scan(r#"{"a":99999999999999999999}"#).unwrap();
        assert!(matches!(got[0].1, Tag::F64(_)));
    }

    #[test]
    fn trailing_garbage_is_rejected() {
        assert!(scan(r#"{"a":1} garbage"#).is_none());
    }

    #[test]
    fn top_level_non_object_is_rejected() {
        assert!(scan(r#""just a string""#).is_none());
        assert!(scan(r#"42"#).is_none());
        assert!(scan(r#"[1,2,3]"#).is_none());
    }

    #[test]
    fn malformed_strings_and_braces_are_rejected() {
        assert!(scan(r#"{"a":"no closing quote}"#).is_none());
        assert!(scan(r#"{"a":"\x4"}"#).is_none());
        assert!(scan(r#"{"a":"\u12"}"#).is_none());
        assert!(scan(r#"{"a":"x\"#).is_none());
        assert!(scan(r#"{"a":nan}"#).is_none());
        assert!(scan(r#"{"a":-Inf}"#).is_none());
        assert!(scan(r#"{"a":1"#).is_none());
        assert!(scan(r#"{"a":1}}"#).is_none());
    }

    #[test]
    fn invalid_utf8_in_raw_bytes_is_rejected_not_panicking() {
        let mut doc = br#"{"a":""#.to_vec();
        doc.push(0xFF);
        doc.extend_from_slice(br#""}"#);
        let mut sink = Collect::default();
        assert!(!parse_object(&doc, &mut Scratch::default(), &mut sink));
    }

    #[test]
    fn deeply_nested_input_fails_cleanly_instead_of_overflowing_the_stack() {
        let mut doc = String::new();
        for _ in 0..(MAX_DEPTH as usize + 10) {
            doc.push_str(r#"{"a":"#);
        }
        doc.push('1');
        for _ in 0..(MAX_DEPTH as usize + 10) {
            doc.push('}');
        }
        assert!(scan(&doc).is_none());
    }

    /// A copied value is the value less what holds nothing: a null, and an object under a key that holds nothing. A
    /// repeated key keeps its last value, in its first place, inside an array too.
    #[test]
    fn a_copied_value_keeps_what_holds_something() {
        let int = Tag::I64;
        let obj = |fields: Vec<(&'static str, Tag<'static>)>| {
            Tag::Object(fields.into_iter().map(|(k, v)| (k.into(), v)).collect())
        };
        let cases = [
            (r#"{"v":-2.5e3}"#, Some(Tag::F64(-2500.0))),
            (r#"{"v":{"a":null,"b":{}}}"#, None),
            (
                r#"{"v":{"a":{"x":1},"b":2,"c":null}}"#,
                Some(obj(vec![("a", obj(vec![("x", int(1))])), ("b", int(2))])),
            ),
            (
                r#"{"v":[{"x":1,"y":0,"x":2},[{"z":3,"z":4}],null,{}]}"#,
                Some(Tag::Array(vec![
                    Some(obj(vec![("x", int(2)), ("y", int(0))])),
                    Some(Tag::Array(vec![Some(obj(vec![("z", int(4))]))])),
                    None,
                    Some(obj(vec![])),
                ])),
            ),
        ];
        for (doc, want) in cases {
            assert_eq!(copied(doc), Some(want), "{doc}");
        }
        // More keys than a scan reads, every key repeated once.
        let keys = |v: usize| (0..40).map(|i| format!(r#""k{i}":{v}"#)).collect::<Vec<_>>().join(",");
        let doc = format!(r#"{{"v":[{{{},{}}}]}}"#, keys(1), keys(2));
        let want = Tag::Object((0..40).map(|i| (format!("k{i}").into(), int(2))).collect());
        assert_eq!(copied(&doc), Some(Some(Tag::Array(vec![Some(want)]))));
    }

    /// A value taken whole is the same walk: its repeated keys are reported by path, and it keeps the last copy.
    #[test]
    fn a_whole_value_reports_each_repeated_key() {
        let sink = collect(r#"{"v":[{"a":1,"a":2,"b":{"c":1,"c":2}}],"w":[{"d":1,"d":1}]}"#).unwrap();
        assert_eq!(sink.repeats, ["v.a", "v.b.c", "w.d"]);
        let got = decoded(sink.got);
        let b = Tag::Object(vec![("c".into(), Tag::I64(2))]);
        let v = Tag::Array(vec![Some(Tag::Object(vec![
            ("a".into(), Tag::I64(2)),
            ("b".into(), b),
        ]))]);
        assert_eq!(got[0], ("v".to_string(), v));
    }

    // -- zero-copy strings ----------------------------------------------------

    #[test]
    fn a_string_with_no_escapes_borrows_from_the_input() {
        let borrowed = |text: &str| {
            let mut s = Scanner {
                b: text.as_bytes(),
                t: text,
                i: 0,
                lost: Lost::default(),
            };
            matches!(scan_string(&mut s), Ok(Cow::Borrowed(_)))
        };
        assert_eq!(
            [r#""plain""#, r#""""#, r#""esc\\aped""#].map(borrowed),
            [true, true, false]
        );
    }

    // -- differential testing against serde_json ----------------------------

    /// Compared numerically rather than via `Debug` text, which differs between the two sides for floats.
    #[derive(Debug, Clone, PartialEq)]
    enum Norm {
        /// `-0` is `I64(0)` here (no `.` or exponent) and a float in `serde_json`; both normalize to this.
        Zero,
        Bool(bool),
        I64(i64),
        F64(f64),
        Str(String),
        /// Arrays are compared for presence only; whitespace inside may legitimately differ.
        Array,
    }

    fn oracle_flatten(v: &serde_json::Value, path: &mut String, out: &mut Vec<(String, Norm)>) {
        match v {
            serde_json::Value::Object(m) => {
                for (k, v) in m {
                    let base = path.len();
                    tree::push_key(path, k);
                    oracle_flatten(v, path, out);
                    path.truncate(base);
                }
            }
            serde_json::Value::Null => {}
            serde_json::Value::Bool(b) => out.push((path.clone(), Norm::Bool(*b))),
            serde_json::Value::Number(n) => {
                let norm = if n.as_f64() == Some(0.0) {
                    Norm::Zero
                } else if let Some(i) = n.as_i64() {
                    Norm::I64(i)
                } else {
                    Norm::F64(n.as_f64().unwrap())
                };
                out.push((path.clone(), norm));
            }
            serde_json::Value::String(s) => out.push((path.clone(), Norm::Str(s.clone()))),
            serde_json::Value::Array(_) => out.push((path.clone(), Norm::Array)),
        }
    }

    fn differential_case(doc: &str) {
        let ours = scan(doc);
        let theirs: Result<serde_json::Value, _> = serde_json::from_str(doc);
        match (ours, theirs) {
            (Some(got), Ok(serde_json::Value::Object(v))) => {
                let mut expected = Vec::new();
                oracle_flatten(&serde_json::Value::Object(v), &mut String::new(), &mut expected);
                let mut got_norm: Vec<(String, Norm)> = got
                    .iter()
                    .map(|(p, t)| {
                        let n = match t {
                            Tag::Array(_) => Norm::Array,
                            Tag::I64(0) => Norm::Zero,
                            Tag::F64(x) if *x == 0.0 => Norm::Zero,
                            Tag::Bool(b) => Norm::Bool(*b),
                            Tag::I64(i) => Norm::I64(*i),
                            Tag::F64(x) => Norm::F64(*x),
                            Tag::Str(s) => Norm::Str(s.to_string()),
                            Tag::Object(_) => {
                                unreachable!("a top-level object field flattens")
                            }
                            Tag::DateTime(_) | Tag::Duration(_) | Tag::Bytes(_) => {
                                unreachable!("JSON text has no time, span or bytes")
                            }
                        };
                        (p.clone(), n)
                    })
                    .collect();
                // Paths are unique per document; order is not a guarantee on either side.
                got_norm.sort_by(|a, b| a.0.cmp(&b.0));
                expected.sort_by(|a, b| a.0.cmp(&b.0));
                assert_eq!(got_norm, expected, "mismatch on {doc}");
            }
            (None, Ok(serde_json::Value::Object(_))) => {
                panic!("scanner rejected a document serde_json accepted: {doc}")
            }
            (Some(_), Err(_)) | (Some(_), Ok(_)) => {
                panic!("scanner accepted a document serde_json rejected or read differently: {doc}")
            }
            (None, Err(_)) | (None, Ok(_)) => {} // both agree it's unusable
        }
    }

    /// What `Reader` reads of the value it is at, as `serde_json` would hold it.
    fn read_value(r: &mut Reader) -> Result<serde_json::Value, Malformed> {
        use serde_json::Value;
        Ok(match r.value()? {
            Next::Null => Value::Null,
            Next::Bool(b) => Value::Bool(b),
            Next::Number(text) => serde_json::from_str(text).map_err(|_| Malformed)?,
            Next::Str(text) => Value::String(text.into_owned()),
            Next::Object => {
                let mut members = serde_json::Map::new();
                while let Some(key) = r.key()? {
                    members.insert(key.into_owned(), read_value(r)?);
                }
                Value::Object(members)
            }
            Next::Array => {
                let mut items = Vec::new();
                while r.item()? {
                    items.push(read_value(r)?);
                }
                Value::Array(items)
            }
        })
    }

    /// `doc` read whole by `Reader`, or `None` where it is no JSON.
    fn read_whole(doc: &str) -> Option<serde_json::Value> {
        let mut r = Reader::new(doc.as_bytes()).ok()?;
        let value = read_value(&mut r).ok()?;
        r.end().then_some(value)
    }

    /// `Reader` reads what `serde_json` reads, and refuses what it refuses, at the top level and within.
    fn reader_case(doc: &str) {
        let theirs = serde_json::from_str::<serde_json::Value>(doc).ok();
        assert_eq!(read_whole(doc), theirs, "the reader differs on {doc}");
        let wrapped = format!("[{doc},{{\"k\":{doc}}}]");
        let theirs = serde_json::from_str::<serde_json::Value>(&wrapped).ok();
        assert_eq!(read_whole(&wrapped), theirs, "the reader differs on {wrapped}");
    }

    #[test]
    fn the_reader_reads_as_serde_json_does() {
        let docs = [
            "{}",
            "[]",
            "1",
            "-0.5e3",
            r#""a\"b\u00e9\ud83d\ude00""#,
            "null",
            "true",
            r#"{"a":[1,[2,[]],{"b":{}}],"c":null,"d":"x"}"#,
            r#"  { "a" : [ 1 , 2 ] }  "#,
            r#"{"a":1,"a":2}"#,
            r#"{"a":18446744073709551615,"b":-9223372036854775809}"#,
            "[1 2]",
            r#"{"a" 1}"#,
            r#"{"a":1 "b":2}"#,
            "{,}",
            "[,1]",
            "[1,,2]",
            r#"{"a":}"#,
            "[",
            r#"{"a":1"#,
            "01",
            "tru",
            "NaN",
            "{} {}",
            "",
        ];
        for doc in docs {
            reader_case(doc);
        }
    }

    #[test]
    fn the_reader_takes_a_trailing_comma_and_skips_a_value_whole() {
        assert_eq!(read_whole("[1,]"), Some(serde_json::json!([1])));
        assert_eq!(read_whole(r#"{"a":1,}"#), Some(serde_json::json!({"a": 1})));
        let doc = br#"[{"a":[1,{"b":"\""}]} , 7]"#;
        let mut r = Reader::new(doc).unwrap();
        assert_eq!(r.value(), Ok(Next::Array));
        assert!(r.item().unwrap());
        assert_eq!(r.raw().unwrap(), br#"{"a":[1,{"b":"\""}]}"#);
        assert!(r.item().unwrap());
        assert_eq!(r.value(), Ok(Next::Number("7")));
        assert!(!r.item().unwrap());
        assert!(r.end());
    }

    #[test]
    fn the_reader_copies_a_value_as_a_variant() {
        fn shown(b: &[u8]) -> serde_json::Value {
            use crate::core::variant::{self, Value as V};
            match variant::read(b) {
                V::Null => serde_json::Value::Null,
                V::Bool(b) => b.into(),
                V::I64(i) => i.into(),
                V::F64(f) => f.into(),
                V::Str(s) => s.into(),
                V::Array(items) => items.map(shown).collect(),
                V::Object(entries) => serde_json::Value::Object(entries.map(|(k, v)| (k.into(), shown(v))).collect()),
                other => panic!("JSON holds no {other:?}"),
            }
        }
        let copied = |doc: &str| {
            let mut out = Vec::new();
            Reader::new(doc.as_bytes())
                .unwrap()
                .variant(&mut out)
                .map(|()| shown(&out))
        };
        assert_eq!(
            copied(r#"{"b":1,"a":[null,{}],"n":null,"b":2.5}"#),
            Ok(serde_json::json!({"b": 2.5, "a": [null, {}]}))
        );
        assert_eq!(copied("null"), Ok(serde_json::Value::Null));
        assert_eq!(copied(r#""x""#), Ok(serde_json::json!("x")));
        assert_eq!(copied("[1,"), Err(Malformed));
        let mut r = Reader::new(br#"[{"a":1},2]"#).unwrap();
        let mut out = Vec::new();
        assert_eq!(r.value(), Ok(Next::Array));
        assert!(r.item().unwrap());
        r.variant(&mut out).unwrap();
        assert!(r.item().unwrap());
        assert_eq!(r.value(), Ok(Next::Number("2")));
        assert!(!r.item().unwrap());
    }

    #[test]
    fn the_reader_refuses_nesting_past_the_bound() {
        let deep = |n: usize| format!("{}{}", "[".repeat(n), "]".repeat(n));
        assert!(read_whole(&deep(MAX_DEPTH as usize)).is_some());
        assert!(read_whole(&deep(MAX_DEPTH as usize + 1)).is_none());
        assert!(Reader::new(b"\"\xff\"").is_err(), "no UTF-8");
    }

    #[test]
    fn differential_against_serde_json_over_a_document_corpus() {
        let docs = [
            r#"{}"#,
            r#"{"a":1}"#,
            r#"{"a":1,"b":2.5,"c":"x","d":true,"e":false,"f":null}"#,
            r#"{"source":{"ip":"1.2.3.4","geo":{"country":"US"}}}"#,
            r#"{"a":"quote\"inside","b":"back\\slash","c":"tab\there"}"#,
            r#"{"a":"éè"}"#,
            r#"{"a":0,"b":-0,"c":123456789012,"d":-123456789012}"#,
            r#"{"a":1.5e10,"b":1.5e-10,"c":-1.5E+3}"#,
            r#"{"a":[1,2,3],"b":[{"x":1}],"c":[]}"#,
            r#"{"empty":{},"nested_empty":{"a":{}}}"#,
            r#"{"unicode":"😀 party"}"#,
            r#"  {  "a" : 1 , "b" : 2  }  "#,
        ];
        for d in docs {
            differential_case(d);
        }
    }

    /// Syntax corners a corner-cutting scanner could plausibly get wrong. What log systems write beyond JSON is
    /// deliberately absent: the tests under its heading read it. Every other JSON5 extension is here, and refused.
    #[test]
    fn differential_against_serde_json_over_edge_cases() {
        let docs = [
            r#"{"a":1e5,"b":1E5,"c":1e+5,"d":1e0,"e":0e0,"f":1.0e5,"g":1.5e0}"#,
            r#"{"a":1e-400}"#,
            r#"{"a":1,"a":2,"a":3}"#,
            r#"{"a":01}"#,
            r#"{"a":-01}"#,
            r#"{"a":+1}"#,
            r#"{"a":.5}"#,
            r#"{"a":5.}"#,
            r#"{"a":0x1F}"#,
            r#"{a:1}"#,
            r#"{'a':1}"#,
            r#"{"a":'x'}"#,
            "{\"a\":1 // comment\n}",
            "{\"a\":1 /* comment */}",
            "{\"a\":\u{a0}1}", // whitespace JSON does not count
        ];
        for d in docs {
            differential_case(d);
        }
    }

    /// The string scan reads eight bytes at a time: a quote or backslash at every place in a chunk, and in the tail
    /// past the last whole chunk, beside multi-byte characters.
    #[test]
    fn differential_against_serde_json_at_every_place_in_a_chunk() {
        for len in 0..20 {
            for at in 0..len {
                for special in ["\\\"", "\\\\", "\\n", "é"] {
                    let mut text: Vec<&str> = vec!["a"; len];
                    text[at] = special;
                    differential_case(&format!(r#"{{"k{at}":"{}","z":"é"}}"#, text.concat()));
                }
            }
        }
    }

    // -- what log systems write beyond JSON -------------------------------------

    const CLEAN: Lost = Lost {
        replaced: false,
        non_finite: false,
    };
    const REPLACED: Lost = Lost {
        replaced: true,
        non_finite: false,
    };
    const NON_FINITE: Lost = Lost {
        replaced: false,
        non_finite: true,
    };

    /// The fields, if the document parsed, and what the read lost.
    fn read(doc: &str) -> (Option<Vec<(String, Tag<'static>)>>, Lost) {
        let mut sink = Collect::default();
        let mut scratch = Scratch::default();
        let ok = parse_object(doc.as_bytes(), &mut scratch, &mut sink);
        (ok.then(|| decoded(sink.got)), scratch.lost)
    }

    fn str_field(doc: &str) -> (String, Lost) {
        match read(doc) {
            (Some(got), lost) => match &got[..] {
                [(_, Tag::Str(s))] => (s.to_string(), lost),
                other => panic!("{doc}: {other:?}"),
            },
            (None, _) => panic!("{doc} did not parse"),
        }
    }

    #[test]
    fn a_hex_byte_escape_decodes_its_run_as_utf8() {
        assert_eq!(str_field(r#"{"m":"GET \x22/a\x22"}"#), ("GET \"/a\"".into(), CLEAN));
        assert_eq!(str_field(r#"{"m":"caf\xC3\xA9!"}"#), ("café!".into(), CLEAN));
        assert_eq!(str_field(r#"{"m":"caf\xc3\xa9\n"}"#), ("café\n".into(), CLEAN));
        assert_eq!(str_field(r#"{"m":"caf\xE9"}"#), ("caf\u{FFFD}".into(), REPLACED));
    }

    #[test]
    fn a_raw_control_byte_in_a_string_is_kept() {
        assert_eq!(str_field("{\"m\":\"a\tb\nc\u{1}\"}"), ("a\tb\nc\u{1}".into(), CLEAN));
        assert_eq!(str_field("{\"m\":\"a\\\"\tb\"}"), ("a\"\tb".into(), CLEAN));
    }

    #[test]
    fn any_other_escaped_character_stands_for_itself() {
        assert_eq!(str_field(r#"{"m":"it\'s \q"}"#), ("it's q".into(), CLEAN));
        assert_eq!(str_field("{\"m\":\"x\\\ny\"}"), ("x\ny".into(), CLEAN));
        assert_eq!(str_field(r#"{"m":"\é"}"#), ("é".into(), CLEAN));
    }

    #[test]
    fn a_number_an_f64_cannot_hold_is_no_value() {
        for doc in [
            r#"{"v":NaN,"n":1}"#,
            r#"{"v":Infinity,"n":1}"#,
            r#"{"v":-Infinity,"n":1}"#,
            r#"{"v":1e999,"n":1}"#,
            r#"{"v":-1e400,"n":1}"#,
        ] {
            assert_eq!(read(doc), (Some(vec![("n".into(), Tag::I64(1))]), NON_FINITE), "{doc}");
        }
        assert_eq!(read(r#"{"v":[1,NaN]}"#).1, NON_FINITE);
        // A malformed number stays malformed, however large.
        assert_eq!(read(&format!("{{\"v\":{}.}}", "9".repeat(400))), (None, CLEAN));
    }

    #[test]
    fn a_document_that_fails_reports_no_loss() {
        assert_eq!(read(r#"{"v":NaN,"#), (None, CLEAN));
        assert_eq!(read(r#"{"m":"\xE9"}x"#), (None, CLEAN));
    }

    #[test]
    fn a_strict_document_loses_nothing() {
        assert_eq!(read(r#"{"a":"\u00e9\ud83d\ude00","b":-1.5e3,"c":[-2]}"#).1, CLEAN);
    }

    // -- trailing commas ------------------------------------------------------

    #[test]
    fn trailing_comma_in_object_is_accepted() {
        let got = scan(r#"{"a":1,"b":2,}"#).unwrap();
        assert_eq!(got, vec![("a".into(), Tag::I64(1)), ("b".into(), Tag::I64(2))]);
        let got = scan(r#"{"source":{"ip":"1.2.3.4",}}"#).unwrap();
        assert_eq!(got, vec![("source.ip".into(), Tag::Str("1.2.3.4".into()))]);
    }

    #[test]
    fn a_comma_that_stands_in_for_nothing_is_still_rejected() {
        assert!(scan(r#"{,}"#).is_none());
        assert!(scan(r#"{"a":1,,"b":2}"#).is_none());
    }

    #[test]
    fn trailing_comma_in_an_array_is_accepted() {
        let got = scan(r#"{"tags":[1,2,]}"#).unwrap();
        match &got[0].1 {
            Tag::Array(elements) => {
                assert_eq!(elements, &vec![Some(Tag::I64(1)), Some(Tag::I64(2))]);
            }
            other => panic!("expected Array, got {other:?}"),
        }
        let got = scan(r#"{"a":[{"x":1,},{"y":2}],"b":3}"#).unwrap();
        assert_eq!(got[1], ("b".into(), Tag::I64(3)));
    }
}
