//! String kernels.
//!
//! `length`, `slice` and `indexOf` all count Unicode characters, so the three agree and a slice never lands
//! mid-codepoint. JS counts UTF-16 code units, and can. Case mapping and `trim` are Unicode-aware.

use super::helpers::{Operand, alloc_with_merged_validity, short_circuit_empty, side};
use crate::core::bits;
use crate::core::column::{Column, DataType, StrBuilder, VarType};
use crate::core::ctx::Ctx;

/// N-ary concatenation: measure, reserve once, then write each byte once. Null propagates. A constant operand is
/// one value, written into every row.
pub fn concat_n(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    let len = ins[0].len();
    if let Some(e) = short_circuit_empty(ins, &DataType::Str, len) {
        return e;
    }

    let parts: Vec<_> = ins.iter().map(|c| side::var(c).unwrap()).collect();
    let total: usize = parts.iter().map(|p| p.total_bytes(len)).sum();

    let words = bits::words_for(len);
    let mut valid = vec![!0u64; words];
    for v in parts.iter().filter_map(|p| p.validity()) {
        for (dst, src) in valid.iter_mut().zip(v.bits()) {
            *dst &= *src;
        }
    }
    bits::mask_tail(&mut valid, len);

    let mut b = StrBuilder::new(ctx.pool, VarType::Str, len);
    b.reserve(ctx.pool, total);

    let mut scratch: Vec<u8> = Vec::with_capacity(64);
    for i in 0..len {
        if !bits::get_row(&valid, i) {
            b.push_null();
            continue;
        }
        scratch.clear();
        for p in &parts {
            scratch.extend_from_slice(p.at(i));
        }
        b.push(&scratch);
    }
    b.finish()
}

/// The table's entry for un-flattened `Str + Str`.
pub fn concat_str_str(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    concat_n(ctx, ins)
}

/// `length`: characters for `Str`, bytes for `Bytes`. An all-ASCII column is an offset subtraction per row.
pub fn str_length(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    let len = ins[0].len();
    if let Some(e) = short_circuit_empty(ins, &DataType::I64, len) {
        return e;
    }
    let v = ins[0].var().unwrap();
    let mut out = Column::alloc(ctx.pool, &DataType::I64, len);
    {
        let o = out.i64_mut().unwrap();
        if v.ascii || ins[0].data_type() == DataType::Bytes {
            for i in 0..len {
                o.values[i] = v.row_len(i) as i64;
            }
        } else {
            for i in 0..len {
                o.values[i] = String::from_utf8_lossy(v.get(i)).chars().count() as i64;
            }
        }
        o.validity.bits_mut().copy_from_slice(v.validity.bits());
        o.validity.recount();
    }
    out
}

/// Byte range for character positions `[start, end)`; negative positions count from the end, both clamp to `[0, chars]`
/// (JS `slice` rules).
fn char_slice_bytes(text: &str, ascii: bool, start: i64, end: i64) -> (usize, usize) {
    use crate::ops::helpers::clamped;
    if ascii {
        let n = text.len();
        (clamped(start, n), clamped(end, n))
    } else {
        let boundaries: Vec<usize> = text
            .char_indices()
            .map(|(i, _)| i)
            .chain(std::iter::once(text.len()))
            .collect();
        let n = boundaries.len() - 1;
        (boundaries[clamped(start, n)], boundaries[clamped(end, n)])
    }
}

/// `slice(value, start[, end])`. `start >= end` after clamping yields `""`.
pub fn str_slice(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    let len = ins[0].len();
    if let Some(e) = short_circuit_empty(ins, &DataType::Str, len) {
        return e;
    }
    let v = ins[0].var().unwrap();
    let starts = side::i64(ins[1]);
    let ends = ins.get(2).and_then(|c| side::i64(c));
    let mut b = StrBuilder::new(ctx.pool, VarType::Str, len);
    for i in 0..len {
        let start_ok = starts.as_ref().is_some_and(|s| s.is_valid(i));
        let end_ok = ins.len() < 3 || ends.as_ref().is_some_and(|e| e.is_valid(i));
        if !v.validity.is_valid(i) || !start_ok || !end_ok {
            b.push_null();
            continue;
        }
        let text = String::from_utf8_lossy(v.get(i));
        let n_units = if v.ascii {
            text.len() as i64
        } else {
            text.chars().count() as i64
        };
        let raw_start = starts.as_ref().unwrap().at(i);
        let raw_end = ends.as_ref().map_or(n_units, |e| e.at(i));
        let (s, e) = char_slice_bytes(&text, v.ascii, raw_start, raw_end);
        if s < e {
            b.push_grow(ctx.pool, text[s..e].as_bytes());
        } else {
            b.push_grow(ctx.pool, b"");
        }
    }
    b.finish()
}

/// The character position of `needle` in `text`, from the start or the end, or `None`. `ascii` says the text is, so
/// the byte index is the answer. The one search behind `indexOf`, `lastIndexOf` and `includes`: sharing it is what
/// stops the three disagreeing about what containment means, on a `Bytes` column or anywhere else.
fn find_char_pos(text: &[u8], ascii: bool, needle: &[u8], from_end: bool) -> Option<i64> {
    let text = String::from_utf8_lossy(text);
    let pat = String::from_utf8_lossy(needle);
    let byte_idx = if from_end {
        text.rfind(pat.as_ref())
    } else {
        text.find(pat.as_ref())
    }?;
    Some(if ascii {
        byte_idx as i64
    } else {
        text[..byte_idx].chars().count() as i64
    })
}

/// `indexOf` and `lastIndexOf`: character position of an occurrence, or `-1`.
fn index_of(ctx: &mut Ctx, ins: &[&Column], from_end: bool) -> Column {
    let len = ins[0].len();
    if let Some(e) = short_circuit_empty(ins, &DataType::I64, len) {
        return e;
    }
    let v = ins[0].var().unwrap();
    let needle = side::var(ins[1]).unwrap();
    let mut out = alloc_with_merged_validity(ctx, &DataType::I64, len, &[Some(v.validity), needle.validity()]);
    {
        let o = out.i64_mut().unwrap();
        for i in 0..len {
            o.values[i] = if o.validity.is_valid(i) {
                find_char_pos(v.get(i), v.ascii, needle.at(i), from_end).unwrap_or(-1)
            } else {
                -1
            };
        }
    }
    out
}

/// `indexOf(value, needle)`: character position of the first occurrence, or `-1`.
pub fn str_index_of(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    index_of(ctx, ins, false)
}

macro_rules! str_predicate {
    ($name:ident, $f:expr) => {
        pub fn $name(ctx: &mut Ctx, ins: &[&Column]) -> Column {
            let len = ins[0].len();
            if let Some(e) = short_circuit_empty(ins, &DataType::Bool, len) {
                return e;
            }
            let v = ins[0].var().unwrap();
            let pat = side::var(ins[1]).unwrap();
            let mut out = alloc_with_merged_validity(ctx, &DataType::Bool, len, &[Some(v.validity), pat.validity()]);
            {
                let o = out.bool_mut().unwrap();
                let f: fn(&[u8], &[u8]) -> bool = $f;
                for i in 0..len {
                    let hit = o.validity.is_valid(i) && f(v.get(i), pat.at(i));
                    bits::set_row(o.values, i, hit);
                }
            }
            out
        }
    };
}

str_predicate!(str_starts_with, |v, p| v.starts_with(p));
str_predicate!(str_ends_with, |v, p| v.ends_with(p));

/// `includes(value, needle)`: [`find_char_pos`] read as a yes or no, so `includes(a, b)` is `indexOf(a, b) >= 0` by
/// construction rather than by agreement.
pub fn str_includes(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    let len = ins[0].len();
    if let Some(e) = short_circuit_empty(ins, &DataType::Bool, len) {
        return e;
    }
    let v = ins[0].var().unwrap();
    let needle = side::var(ins[1]).unwrap();
    let mut out = alloc_with_merged_validity(ctx, &DataType::Bool, len, &[Some(v.validity), needle.validity()]);
    {
        let o = out.bool_mut().unwrap();
        for i in 0..len {
            let hit = o.validity.is_valid(i) && find_char_pos(v.get(i), v.ascii, needle.at(i), false).is_some();
            bits::set_row(o.values, i, hit);
        }
    }
    out
}

macro_rules! str_case {
    ($name:ident, $f:ident) => {
        pub fn $name(ctx: &mut Ctx, ins: &[&Column]) -> Column {
            let len = ins[0].len();
            if let Some(e) = short_circuit_empty(ins, &DataType::Str, len) {
                return e;
            }
            let v = ins[0].var().unwrap();
            let mut b = StrBuilder::new(ctx.pool, VarType::Str, len);
            for i in 0..len {
                if !v.validity.is_valid(i) {
                    b.push_null();
                    continue;
                }
                let text = String::from_utf8_lossy(v.get(i));
                let out: String = text.$f();
                b.push_grow(ctx.pool, out.as_bytes());
            }
            b.finish()
        }
    };
}

str_case!(str_to_upper, to_uppercase);
str_case!(str_to_lower, to_lowercase);

pub fn str_trim(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    let len = ins[0].len();
    if let Some(e) = short_circuit_empty(ins, &DataType::Str, len) {
        return e;
    }
    let v = ins[0].var().unwrap();
    let mut b = StrBuilder::new(ctx.pool, VarType::Str, len);
    for i in 0..len {
        if !v.validity.is_valid(i) {
            b.push_null();
            continue;
        }
        let text = String::from_utf8_lossy(v.get(i));
        b.push_grow(ctx.pool, text.trim().as_bytes());
    }
    b.finish()
}

macro_rules! str_trim_side {
    ($name:ident, $f:ident) => {
        pub fn $name(ctx: &mut Ctx, ins: &[&Column]) -> Column {
            let len = ins[0].len();
            if let Some(e) = short_circuit_empty(ins, &DataType::Str, len) {
                return e;
            }
            let v = ins[0].var().unwrap();
            let mut b = StrBuilder::new(ctx.pool, VarType::Str, len);
            for i in 0..len {
                if !v.validity.is_valid(i) {
                    b.push_null();
                    continue;
                }
                let text = String::from_utf8_lossy(v.get(i));
                b.push_grow(ctx.pool, text.$f().as_bytes());
            }
            b.finish()
        }
    };
}

str_trim_side!(str_trim_start, trim_start);
str_trim_side!(str_trim_end, trim_end);

/// JS `padStart`/`padEnd`, counted in characters. `None` past [`BUILD_MAX_BYTES`].
fn pad(text: &str, target: i64, pad: &str, at_start: bool) -> Option<String> {
    let len = text.chars().count() as i64;
    if target <= len || pad.is_empty() {
        return Some(text.to_string());
    }
    let wanted = (target - len) as usize;
    // Widest, so the estimate never understates the result.
    let widest = pad.chars().map(char::len_utf8).max().unwrap_or(1);
    if wanted.saturating_mul(widest).saturating_add(text.len()) > BUILD_MAX_BYTES {
        return None;
    }
    let fill: String = pad.chars().cycle().take(wanted).collect();
    Some(if at_start {
        fill + text
    } else {
        text.to_string() + &fill
    })
}

macro_rules! str_pad {
    ($name:ident, $at_start:expr) => {
        /// `(value, length[, pad])`; the pad defaults to a space.
        pub fn $name(ctx: &mut Ctx, ins: &[&Column]) -> Column {
            let len = ins[0].len();
            if let Some(e) = short_circuit_empty(ins, &DataType::Str, len) {
                return e;
            }
            let v = ins[0].var().unwrap();
            let target = side::i64(ins[1]).unwrap();
            let pad_col = ins.get(2).and_then(|c| side::var(c));
            let mut b = StrBuilder::new(ctx.pool, VarType::Str, len);
            for i in 0..len {
                let pad_ok = ins.len() < 3 || pad_col.as_ref().is_some_and(|p| p.is_valid(i));
                if !v.validity.is_valid(i) || !target.is_valid(i) || !pad_ok {
                    b.push_null();
                    continue;
                }
                let text = String::from_utf8_lossy(v.get(i));
                let padding = pad_col
                    .as_ref()
                    .map(|p| String::from_utf8_lossy(p.at(i)))
                    .unwrap_or(std::borrow::Cow::Borrowed(" "));
                match pad(&text, target.at(i), &padding, $at_start) {
                    Some(out) => b.push_grow(ctx.pool, out.as_bytes()),
                    None => b.push_null(),
                }
            }
            b.finish()
        }
    };
}

str_pad!(str_pad_start, true);
str_pad!(str_pad_end, false);

/// The most one row's `repeat`, `padStart` or `padEnd` will build. JS throws a `RangeError` past its own limit; here
/// the row is null, so a `repeat(_raw, 1000000)` or a `padStart(x, 2000000000)` cannot take the worker down.
const BUILD_MAX_BYTES: usize = 16 << 20;

/// `repeat(value, count)`. A negative count, or a result over [`BUILD_MAX_BYTES`], is null.
pub fn str_repeat(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    let len = ins[0].len();
    if let Some(e) = short_circuit_empty(ins, &DataType::Str, len) {
        return e;
    }
    let v = ins[0].var().unwrap();
    let count = side::i64(ins[1]).unwrap();
    let mut b = StrBuilder::new(ctx.pool, VarType::Str, len);
    for i in 0..len {
        if !v.validity.is_valid(i) || !count.is_valid(i) || count.at(i) < 0 {
            b.push_null();
            continue;
        }
        let n = count.at(i) as usize;
        // Through `from_utf8_lossy` like every other kernel that writes a `Str`: the argument may be `Bytes`, and a
        // `Str` column is valid UTF-8 by invariant.
        let text = String::from_utf8_lossy(v.get(i));
        match text.len().checked_mul(n) {
            Some(total) if total <= BUILD_MAX_BYTES => b.push_grow(ctx.pool, text.repeat(n).as_bytes()),
            _ => b.push_null(),
        }
    }
    b.finish()
}

/// `lastIndexOf(value, needle)`: character position of the last occurrence, or `-1`.
pub fn str_last_index_of(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    index_of(ctx, ins, true)
}

/// `at(value, index)`: the character at `index`, a negative index counting from the end. Out of range is null, where
/// JS gives `undefined`, and an `IndexOutOfRange` warning, as on an array.
pub fn str_at(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    let len = ins[0].len();
    if let Some(e) = short_circuit_empty(ins, &DataType::Str, len) {
        return e;
    }
    let v = ins[0].var().unwrap();
    let index = side::i64(ins[1]).unwrap();
    let mut b = StrBuilder::new(ctx.pool, VarType::Str, len);
    let mut buf = [0u8; 4];
    for i in 0..len {
        if !v.validity.is_valid(i) || !index.is_valid(i) {
            b.push_null();
            continue;
        }
        let text = String::from_utf8_lossy(v.get(i));
        let n = text.chars().count();
        match crate::ops::helpers::position(index.at(i), n).and_then(|k| text.chars().nth(k)) {
            Some(c) => b.push_grow(ctx.pool, c.encode_utf8(&mut buf).as_bytes()),
            None => {
                ctx.raise(i, crate::core::error::ExceptionKind::IndexOutOfRange);
                b.push_null()
            }
        }
    }
    b.finish()
}
