//! Explicit coercions: `toString`, `parseInt`, `parseFloat`, `parseTime`, and between text and bytes: `decode`,
//! `encode`, `parseBytes`. There is no implicit `Str` <-> number coercion (design doc, Language). A row that does not
//! parse is null, not an exception.

use std::io::Write as _;

use super::datetime;
use super::helpers::{Operand, alloc_with_merged_validity, short_circuit_empty, side};
use super::timeparse;
use crate::core::bits;
use crate::core::column::{Column, DataType, Scalar, StrBuilder, VarLen, VarType};
use crate::core::ctx::Ctx;
use crate::core::error::ExceptionKind;
use crate::format::bytes_text::BytesForm;
use crate::format::encoding::{Charset, Encoding};
use crate::ops::OpKernel;

/// Whether `col` is already the text a `Str` blend wants. An unbound column counts: it has no values to render, and
/// nothing reads one.
pub fn is_text(col: &Column) -> bool {
    col.is_unbound() || col.data_type() == DataType::Str
}

/// A column as the text the design doc's Language table sends a blend to.
///
/// A scalar renders as `toString` does; a container renders as its JSON, as `tag::json_text` gives it on the same
/// table. A caller holding text keeps its own column ([`is_text`]).
pub fn stringify(ctx: &mut Ctx, col: &Column) -> Column {
    let ty = col.data_type();
    debug_assert!(!is_text(col), "text is already text; the caller keeps its column");
    match ty {
        DataType::Array(_) | DataType::Struct(..) => {
            let len = col.len();
            let mut b = StrBuilder::new(ctx.pool, VarType::Str, len);
            let mut scratch = Vec::new();
            for row in 0..len {
                scratch.clear();
                if crate::format::json::write_value(col, &ty, row, &mut scratch) {
                    // A container's rendered length is not known before rendering it, so the builder grows rather
                    // than being reserved against a measured total.
                    b.push_grow(ctx.pool, &scratch);
                } else {
                    b.push_null();
                }
            }
            b.finish()
        }
        _ => to_string(ctx, &[col]),
    }
}

/// One value as `toString` renders a row of it. Text is its own rendering.
pub fn scalar_string(ctx: &mut Ctx, value: &Scalar) -> String {
    match value {
        Scalar::Str(s) => s.clone(),
        Scalar::Bytes(b) => BytesForm::Base64.text(b),
        _ => {
            let one = Column::filled(ctx.pool, value, 1);
            let text = to_string(ctx, &[&one]);
            let rendered = text.var().expect("toString renders every non-text scalar");
            let s = String::from_utf8_lossy(rendered.get(0)).into_owned();
            text.reclaim(ctx.pool);
            one.reclaim(ctx.pool);
            s
        }
    }
}

/// I64/F64/Bool/Bytes/DateTime/Duration -> Str. Bytes take an optional second argument, the form to write them in.
/// A `Str` argument never reaches here: `plan::resolve` elides `toString` of a string.
pub fn to_string(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    let len = ins[0].len();
    if let Some(e) = short_circuit_empty(ins, &DataType::Str, len) {
        return e;
    }
    let a = ins[0];
    let mut b = StrBuilder::new(ctx.pool, VarType::Str, len);
    let mut scratch = Vec::new();
    match a.data_type() {
        DataType::I64 => {
            let f = a.i64().unwrap();
            for i in 0..len {
                if f.validity.is_valid(i) {
                    scratch.clear();
                    write!(scratch, "{}", f.values[i]).unwrap();
                    b.push_grow(ctx.pool, &scratch);
                } else {
                    b.push_null();
                }
            }
        }
        DataType::F64 => {
            let f = a.f64().unwrap();
            for i in 0..len {
                if f.validity.is_valid(i) {
                    scratch.clear();
                    write!(scratch, "{}", f.values[i]).unwrap();
                    b.push_grow(ctx.pool, &scratch);
                } else {
                    b.push_null();
                }
            }
        }
        DataType::Bool => {
            let f = a.bool().unwrap();
            for i in 0..len {
                if !f.validity.is_valid(i) {
                    b.push_null();
                } else if bits::get_row(f.values, i) {
                    b.push_grow(ctx.pool, b"true");
                } else {
                    b.push_grow(ctx.pool, b"false");
                }
            }
        }
        // The microsecond count the JSON writer emits, so `toString(d)` and the output agree.
        DataType::Duration => {
            let f = a.duration().unwrap();
            for i in 0..len {
                if f.validity.is_valid(i) {
                    scratch.clear();
                    datetime::write_duration_micros_bare(f.values[i], &mut scratch);
                    b.push_grow(ctx.pool, &scratch);
                } else {
                    b.push_null();
                }
            }
        }
        // The ISO 8601 text the JSON writer emits, so `toString(t)` and the output agree.
        DataType::DateTime => {
            let f = a.datetime().unwrap();
            for i in 0..len {
                if f.validity.is_valid(i) {
                    scratch.clear();
                    datetime::write_iso8601_micros_bare(f.values[i], &mut scratch);
                    b.push_grow(ctx.pool, &scratch);
                } else {
                    b.push_null();
                }
            }
        }
        // In the form named, base64 where none is, as the JSON writer writes bytes.
        DataType::Bytes => {
            let v = a.var().unwrap();
            let forms = ins.get(1).and_then(|c| side::var(c));
            for i in 0..len {
                match row_form(forms.as_ref(), i) {
                    // Straight into the column: hex and binary outgrow their input, and the pool's budget refuses a
                    // row too large before it is written.
                    Some(form) if v.validity.is_valid(i) => {
                        let bytes = v.get(i);
                        b.push_written_grow(ctx.pool, form.text_len(bytes.len()), |out| form.write_to(bytes, out));
                    }
                    _ => b.push_null(),
                }
            }
        }
        _ => return Column::empty(DataType::Str, len),
    }
    b.finish()
}

/// `parseInt(value)` is base 10; `parseInt(value, radix)` parses in base 2..=36 (a `0x` prefix is stripped at 16). An
/// out-of-range radix is a null row.
pub fn parse_int(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    let len = ins[0].len();
    if let Some(e) = short_circuit_empty(ins, &DataType::I64, len) {
        return e;
    }
    let v = ins[0].var().unwrap();
    let radix = ins.get(1).and_then(|c| side::i64(c));
    let mut out = alloc_with_merged_validity(ctx, &DataType::I64, len, &[Some(v.validity), None]);
    {
        let o = out.i64_mut().unwrap();
        for i in 0..len {
            // Every row is written: a pooled buffer is not zeroed.
            let radix_ok = ins.len() < 2 || radix.as_ref().is_some_and(|r| r.is_valid(i));
            if !o.validity.is_valid(i) || !radix_ok {
                o.values[i] = 0;
                o.validity.set_valid(i, false);
                continue;
            }
            let parsed = std::str::from_utf8(v.get(i)).ok().and_then(|s| {
                let s = s.trim();
                match radix.as_ref() {
                    None => s.parse::<i64>().ok(),
                    Some(r) => {
                        let base = r.at(i);
                        if !(2..=36).contains(&base) {
                            return None;
                        }
                        let digits = if base == 16 {
                            s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")).unwrap_or(s)
                        } else {
                            s
                        };
                        i64::from_str_radix(digits, base as u32).ok()
                    }
                }
            });
            match parsed {
                Some(n) => o.values[i] = n,
                None => {
                    o.values[i] = 0;
                    o.validity.set_valid(i, false);
                }
            }
        }
        o.validity.recount();
    }
    out
}

/// A parse function's call: `kernel`, and a `CoercionRefused` on each row whose arguments are all there and whose
/// text did not read. A blank text is null and clean, as the coercion table has it. The kernels stay quiet for the
/// parser and the lookup, which owe their own exceptions.
pub fn read_or_refuse(ctx: &mut Ctx, ins: &[&Column], kernel: OpKernel) -> Column {
    refuse(ctx, ins, kernel, true)
}

/// `read_or_refuse` for a conversion that reads every value, blank included, so its only null is an argument it
/// cannot read: `decode`, `encode`, `parseBytes`, `toString(bytes, form)`.
pub fn convert_or_refuse(ctx: &mut Ctx, ins: &[&Column], kernel: OpKernel) -> Column {
    refuse(ctx, ins, kernel, false)
}

fn refuse(ctx: &mut Ctx, ins: &[&Column], kernel: OpKernel, blank_is_clean: bool) -> Column {
    let out = kernel(ctx, ins);
    let Some(text) = side::var(ins[0]) else { return out };
    let to = out.data_type();
    for row in 0..out.len() {
        let read = out.is_valid(row) || ins.iter().any(|c| !c.is_valid(row));
        let blank = blank_is_clean && std::str::from_utf8(text.at(row)).is_ok_and(|s| s.trim().is_empty());
        if !read && !blank {
            ctx.raise(row, ExceptionKind::CoercionRefused { to: to.clone() });
        }
    }
    out
}

/// `parseHex(value)` is `parseInt(value, 16)`: a `0x` prefix is accepted.
pub fn parse_hex(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    let len = ins[0].len();
    let mut radix = Column::alloc(ctx.pool, &DataType::I64, len);
    radix.i64_mut().unwrap().values.fill(16);
    let out = parse_int(ctx, &[ins[0], &radix]);
    radix.reclaim(ctx.pool);
    out
}

/// Str -> F64. A parse that overflows to infinity is null.
pub fn parse_float(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    let len = ins[0].len();
    if let Some(e) = short_circuit_empty(ins, &DataType::F64, len) {
        return e;
    }
    let v = ins[0].var().unwrap();
    let mut out = alloc_with_merged_validity(ctx, &DataType::F64, len, &[Some(v.validity), None]);
    {
        let o = out.f64_mut().unwrap();
        for i in 0..len {
            if !o.validity.is_valid(i) {
                o.values[i] = 0.0;
                continue;
            }
            let parsed = std::str::from_utf8(v.get(i)).ok().and_then(parse_f64);
            match parsed {
                Some(f) => o.values[i] = f,
                None => {
                    o.values[i] = 0.0;
                    o.validity.set_valid(i, false);
                }
            }
        }
        o.validity.recount();
    }
    out
}

/// The text a duration reads from: `datetime::parse_duration_text`, under the name this module's arms use.
pub(crate) fn timeparse_duration(s: &str) -> Option<i64> {
    datetime::parse_duration_text(s)
}

/// `parseDuration(value)`: the count of microseconds `toString` writes, read back. A miss is null.
pub fn parse_duration(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    let len = ins[0].len();
    if let Some(e) = short_circuit_empty(ins, &DataType::Duration, len) {
        return e;
    }
    let v = ins[0].var().unwrap();
    let mut out = alloc_with_merged_validity(ctx, &DataType::Duration, len, &[Some(v.validity), None]);
    {
        let o = out.duration_mut().unwrap();
        for i in 0..len {
            if !o.validity.is_valid(i) {
                o.values[i] = 0;
                continue;
            }
            match std::str::from_utf8(v.get(i))
                .ok()
                .and_then(datetime::parse_duration_text)
            {
                Some(n) => o.values[i] = n,
                None => {
                    o.values[i] = 0;
                    o.validity.set_valid(i, false);
                }
            }
        }
        o.validity.recount();
    }
    out
}

/// `parseTime(value, format)` for one format known when the plan compiled: what the convert pass after a parse runs over a declared
/// `datetime` field. The year a yearless format lacks comes from the batch's `now()`, as it does with no reference.
pub fn parse_time_with_format(ctx: &mut Ctx, value: &Column, fmt: &str) -> Column {
    let len = value.len();
    if let Some(e) = short_circuit_empty(&[value], &DataType::DateTime, len) {
        return e;
    }
    let now = ctx.now_micros;
    let v = value.var().unwrap();
    let mut out = alloc_with_merged_validity(ctx, &DataType::DateTime, len, &[Some(v.validity)]);
    {
        let o = out.datetime_mut().unwrap();
        for i in 0..len {
            if !o.validity.is_valid(i) {
                o.values[i] = 0;
                continue;
            }
            match timeparse::parse_with_format_at(&String::from_utf8_lossy(v.get(i)), fmt, Some(now)) {
                Some(us) => o.values[i] = us,
                None => {
                    o.values[i] = 0;
                    o.validity.set_valid(i, false);
                }
            }
        }
        o.validity.recount();
    }
    out
}

/// `parseTime(value[, format][, reference])`: a `Str` second argument is the format, a `DateTime` one the reference
/// a yearless stamp takes its year from; without one the reference is the batch's `now()`.
///
/// Auto-detect gives each row the value it would get alone. The rows before it never change it: preview runs one
/// record as a batch of one, and an answer that leaned on its neighbors would make preview lie. The previous row's
/// winning candidate rides along as a hint, which is sound because any two candidates that read a text read the same
/// instant.
pub fn parse_time(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    let len = ins[0].len();
    if let Some(e) = short_circuit_empty(ins, &DataType::DateTime, len) {
        return e;
    }
    let now = ctx.now_micros;
    let v = ins[0].var().unwrap();
    let fmt_col = ins
        .get(1)
        .filter(|c| c.data_type() != DataType::DateTime)
        .and_then(|c| side::var(c));
    let has_fmt_arg = ins.len() >= 2 && ins[1].data_type() != DataType::DateTime;
    let ref_col = ins
        .iter()
        .skip(1)
        .find(|c| c.data_type() == DataType::DateTime)
        .and_then(|c| side::datetime(c));
    let mut out = Column::alloc(ctx.pool, &DataType::DateTime, len);
    {
        let o = out.datetime_mut().unwrap();
        let mut hint = usize::MAX;
        for i in 0..len {
            let fmt_ok = !has_fmt_arg || fmt_col.as_ref().is_some_and(|f| f.is_valid(i));
            let ref_ok = ref_col.as_ref().is_none_or(|r| r.is_valid(i));
            if !v.validity.is_valid(i) || !fmt_ok || !ref_ok {
                o.values[i] = 0;
                o.validity.set_valid(i, false);
                continue;
            }
            let reference = Some(ref_col.as_ref().map_or(now, |r| r.at(i)));
            let text = String::from_utf8_lossy(v.get(i));
            let parsed = match &fmt_col {
                Some(f) => {
                    let fmt = String::from_utf8_lossy(f.at(i));
                    timeparse::parse_with_format_at(&text, &fmt, reference)
                }
                None => timeparse::parse_auto_hinted_at(&text, hint, reference).map(|(us, idx)| {
                    hint = idx;
                    us
                }),
            };
            match parsed {
                Some(us) => o.values[i] = us,
                None => {
                    o.values[i] = 0;
                    o.validity.set_valid(i, false);
                }
            }
        }
        o.validity.recount();
    }
    out
}

// ---------------------------------------------------------------------------
// The coercion table
// ---------------------------------------------------------------------------

/// Whether a column of `from` is already what `to` asks for, so the coercion is the column itself.
pub fn coerce_is_identity(from: &DataType, to: &DataType) -> bool {
    from == to
}

/// The coercion table (design doc, Language) applied at run time: every row of `col` becomes `to`, or
/// null with a `CoercionRefused` exception. A blank text under a number, bool or time is null and clean. A refused
/// element refuses the row's whole array; a refused leaf of a struct is null on its own.
pub fn coerce(ctx: &mut Ctx, col: &Column, to: &DataType) -> Column {
    let (out, refused) = coerce_with_refusals(ctx, col, to);
    if let Some(refused) = refused {
        for row in (0..col.len()).filter(|&r| bits::get_row(&refused, r)) {
            ctx.raise(row, ExceptionKind::CoercionRefused { to: to.clone() });
        }
    }
    out
}

/// `coerce`, with the refused rows as a bitmap for the caller to raise: an array's element refusals are its
/// rows', not the child's, and a parser owes its own field exception rather than a coercion's.
pub fn coerce_with_refusals(ctx: &mut Ctx, col: &Column, to: &DataType) -> (Column, Option<Vec<u64>>) {
    let len = col.len();
    let from = col.data_type();
    if col.is_unbound() {
        return (Column::empty(to.clone(), len), None);
    }
    if coerce_is_identity(&from, to) {
        return (col.deep_copy(ctx.pool), None);
    }
    if *to == DataType::Variant {
        return (crate::format::tag::to_variant(col, ctx.pool), None);
    }
    if from == DataType::Variant {
        return from_variant(ctx, col, to);
    }
    // The table's rows, by type: `coercible` is the same table asked of types. Text arms read `var()`, which
    // answers for bytes too, so only a `Str` column reaches one.
    use DataType as T;
    match (to, &from) {
        (T::Bool, T::Str) => from_text(ctx, &col.var().unwrap(), to, len, |s| parse_bool(s).map(Scalar::Bool)),
        (T::I64, T::F64) => {
            let f = col.f64().unwrap();
            from_fixed(ctx, len, f.validity, to, |i| round_i64(f.values[i]).map(Scalar::I64))
        }
        (T::I64, T::Str) => from_text(ctx, &col.var().unwrap(), to, len, |s| parse_i64(s).map(Scalar::I64)),
        (T::F64, T::I64) => {
            let f = col.i64().unwrap();
            from_fixed(ctx, len, f.validity, to, |i| Some(Scalar::F64(f.values[i] as f64)))
        }
        (T::F64, T::Str) => from_text(ctx, &col.var().unwrap(), to, len, |s| parse_f64(s).map(Scalar::F64)),
        // Bytes render as `toString` renders them, in base64.
        (T::Str, _) => (stringify(ctx, col), None),
        (T::DateTime, T::I64) => {
            let f = col.i64().unwrap();
            from_fixed(ctx, len, f.validity, to, |i| {
                timeparse::parse_epoch_digits(&f.values[i].to_string()).map(Scalar::DateTime)
            })
        }
        (T::DateTime, T::F64) => {
            let f = col.f64().unwrap();
            from_fixed(ctx, len, f.validity, to, |i| {
                timeparse::parse_epoch_digits(&format!("{}", f.values[i])).map(Scalar::DateTime)
            })
        }
        (T::DateTime, T::Str) => {
            let mut hint = usize::MAX;
            let now = ctx.now_micros;
            from_text(ctx, &col.var().unwrap(), to, len, |s| {
                read_time(s, None, &mut hint, now).map(Scalar::DateTime)
            })
        }
        (T::Duration, T::I64) => {
            let f = col.i64().unwrap();
            from_fixed(ctx, len, f.validity, to, |i| Some(Scalar::Duration(f.values[i])))
        }
        (T::Duration, T::F64) => {
            let f = col.f64().unwrap();
            from_fixed(ctx, len, f.validity, to, |i| {
                let x = f.values[i].round();
                in_i64_range(x).then_some(Scalar::Duration(x as i64))
            })
        }
        (T::Duration, T::Str) => from_text(ctx, &col.var().unwrap(), to, len, |s| {
            timeparse_duration(s).map(Scalar::Duration)
        }),
        (T::Array(elem), T::Array(_)) => {
            let list = col.list().unwrap();
            let (child, child_refused) = coerce_with_refusals(ctx, list.child, elem);
            let mut refused: Option<Vec<u64>> = None;
            let mut validity = list.validity.deep_copy(ctx.pool);
            if let Some(child_refused) = child_refused {
                let mut rows_refused = vec![0u64; bits::words_for(len)];
                for r in 0..len {
                    let (lo, hi) = (list.offsets[r] as usize, list.offsets[r + 1] as usize);
                    if validity.is_valid(r) && (lo..hi).any(|i| bits::get_row(&child_refused, i)) {
                        bits::set_row(&mut rows_refused, r, true);
                        validity.set_valid(r, false);
                    }
                }
                refused = Some(rows_refused);
            }
            let mut offsets = ctx.pool.take((len + 1) * 4);
            offsets.as_mut_slice::<u32>()[..=len].copy_from_slice(&list.offsets[..=len]);
            (Column::list_from_parts(offsets, child, validity, len), refused)
        }
        (T::Struct(fields, open), T::Struct(..)) => {
            let mut refused: Option<Vec<u64>> = None;
            let mut out = Vec::with_capacity(fields.len());
            for (name, ty) in fields {
                // A field the value holds only as a key of its unclaimed fields is read from there.
                let member = match (col.field(name), col.unclaimed_fields()) {
                    (None, Some(u)) => Some(crate::core::variant::member_column(u, name, ctx.pool)),
                    _ => None,
                };
                let leaf = match col.field(name).or(member.as_ref()) {
                    Some(f) => {
                        let (c, r) = coerce_with_refusals(ctx, f, ty);
                        if let Some(r) = r {
                            let acc = refused.get_or_insert_with(|| vec![0u64; bits::words_for(len)]);
                            for (a, b) in acc.iter_mut().zip(&r) {
                                *a |= *b;
                            }
                        }
                        c
                    }
                    None => Column::empty(ty.clone(), len),
                };
                if let Some(m) = member {
                    m.reclaim(ctx.pool);
                }
                out.push((name.clone(), leaf));
            }
            // What the type does not name stays, as the struct's unclaimed fields, where the type is open. A
            // closed type drops each such key.
            let unclaimed_fields = match open {
                true => crate::format::tag::unclaimed_fields_beyond(col, fields, ctx.pool),
                false => None,
            };
            (
                Column::struct_with_unclaimed_fields(out, unclaimed_fields, len),
                refused,
            )
        }
        // Text reads as `parseBytes` reads it, in base64.
        (T::Bytes, T::Str) => {
            let out = parse_bytes(ctx, &[col]);
            let mut refused: Option<Vec<u64>> = None;
            for r in (0..len).filter(|&r| col.is_valid(r) && !out.is_valid(r)) {
                bits::set_row(refused.get_or_insert_with(|| vec![0u64; bits::words_for(len)]), r, true);
            }
            (out, refused)
        }
        // Every other pair is refused. `Null` is the engine's, never a declaration's, so nothing coerces to it.
        _ => refuse_all(col, to),
    }
}

/// A variant column as `to`, decoded by the table a parser's declared field is (`format::tag::decode_column`), so
/// a type declared downstream of a parser means what it means at the parser. A closed object keeps its fields and
/// drops each other key.
fn from_variant(ctx: &mut Ctx, col: &Column, to: &DataType) -> (Column, Option<Vec<u64>>) {
    let len = col.len();
    if *to == DataType::Null {
        return refuse_all(col, to);
    }
    let (out, failed) = crate::format::tag::decode_column(col, to, ctx.now_micros, ctx.pool);
    let refused = (!failed.is_empty()).then(|| {
        let mut r = vec![0u64; bits::words_for(len)];
        for (row, _) in failed {
            bits::set_row(&mut r, row, true);
        }
        r
    });
    (out, refused)
}

/// Every valid row refused: the column is null, and each valid row is a refusal.
fn refuse_all(col: &Column, to: &DataType) -> (Column, Option<Vec<u64>>) {
    let len = col.len();
    let mut refused = vec![0u64; bits::words_for(len)];
    for r in (0..len).filter(|&r| col.is_valid(r)) {
        bits::set_row(&mut refused, r, true);
    }
    (Column::empty(to.clone(), len), Some(refused))
}

fn text_of(bytes: &[u8]) -> std::borrow::Cow<'_, str> {
    String::from_utf8_lossy(bytes)
}

// The text half of the table: what a text value means under a declared scalar type, wherever it is read. Padding is
// never the value, so each trims.

pub fn parse_bool(s: &str) -> Option<bool> {
    match s.trim() {
        "true" => Some(true),
        "false" => Some(false),
        _ => None,
    }
}

/// A number, however rendered, to the nearest `i64`: `2`, `+5`, `007`, `2.0`, `2e3`, and `2.5` is `3`. None where
/// it is not a number or the result is outside `i64`.
pub fn parse_i64(s: &str) -> Option<i64> {
    let s = s.trim();
    if let Ok(n) = s.parse::<i64>() {
        return Some(n);
    }
    round_i64(s.parse::<f64>().ok()?)
}

/// `f` rounded to the nearest `i64`, half away from zero; none where the result is outside `i64`.
pub(crate) fn round_i64(f: f64) -> Option<i64> {
    let r = f.round();
    in_i64_range(r).then_some(r as i64)
}

/// Whether `f` is finite and `f as i64` is exact: the half-open range, since `2^63` itself is the first value out.
pub(crate) fn in_i64_range(f: f64) -> bool {
    f.is_finite() && (-9_223_372_036_854_775_808.0..9_223_372_036_854_775_808.0).contains(&f)
}

/// A time read from text: in `format`, or else by the automatic reading, which tries first the shape that matched
/// last (`hint`). A reading with no year takes it from `now`.
pub fn read_time(s: &str, format: Option<&str>, hint: &mut usize, now: i64) -> Option<i64> {
    match format {
        Some(fmt) => timeparse::parse_with_format_at(s, fmt, Some(now)),
        None => timeparse::parse_auto_hinted_at(s, *hint, Some(now)).map(|(us, i)| {
            *hint = i;
            us
        }),
    }
}

pub fn parse_f64(s: &str) -> Option<f64> {
    s.trim().parse::<f64>().ok().filter(|f| f.is_finite())
}

/// A text column coerced row by row: `parse` reads the trimmed text, a blank is null and clean, and anything
/// `parse` refuses is a refusal.
fn from_text(
    ctx: &mut Ctx,
    v: &crate::core::column::VarLen<'_>,
    to: &DataType,
    len: usize,
    mut parse: impl FnMut(&str) -> Option<Scalar>,
) -> (Column, Option<Vec<u64>>) {
    let mut values: Vec<Option<Scalar>> = Vec::with_capacity(len);
    let mut refused: Option<Vec<u64>> = None;
    for i in 0..len {
        if !v.validity.is_valid(i) {
            values.push(None);
            continue;
        }
        let text = text_of(v.get(i));
        let s = text.trim();
        if s.is_empty() {
            values.push(None);
            continue;
        }
        match parse(s) {
            Some(x) => values.push(Some(x)),
            None => {
                bits::set_row(refused.get_or_insert_with(|| vec![0u64; bits::words_for(len)]), i, true);
                values.push(None);
            }
        }
    }
    (build_scalars(ctx, to, &values), refused)
}

/// A fixed-width column coerced row by row: `convert(i)` gives row `i`'s value, or none to refuse it.
fn from_fixed(
    ctx: &mut Ctx,
    len: usize,
    validity: &crate::core::column::Validity,
    to: &DataType,
    mut convert: impl FnMut(usize) -> Option<Scalar>,
) -> (Column, Option<Vec<u64>>) {
    let mut values: Vec<Option<Scalar>> = Vec::with_capacity(len);
    let mut refused: Option<Vec<u64>> = None;
    for i in 0..len {
        if !validity.is_valid(i) {
            values.push(None);
            continue;
        }
        match convert(i) {
            Some(x) => values.push(Some(x)),
            None => {
                bits::set_row(refused.get_or_insert_with(|| vec![0u64; bits::words_for(len)]), i, true);
                values.push(None);
            }
        }
    }
    (build_scalars(ctx, to, &values), refused)
}

/// A column of `to` from one scalar per row, null where there is none.
fn build_scalars(ctx: &mut Ctx, to: &DataType, values: &[Option<Scalar>]) -> Column {
    let len = values.len();
    if values.iter().all(Option::is_none) {
        return Column::empty(to.clone(), len);
    }
    match to {
        DataType::Str => {
            let mut b = StrBuilder::new(ctx.pool, VarType::Str, len);
            for v in values {
                match v {
                    Some(Scalar::Str(s)) => b.push_grow(ctx.pool, s.as_bytes()),
                    _ => b.push_null(),
                }
            }
            b.finish()
        }
        DataType::Bool => {
            let mut out = Column::alloc(ctx.pool, to, len);
            let o = out.bool_mut().unwrap();
            for (i, v) in values.iter().enumerate() {
                let b = matches!(v, Some(Scalar::Bool(true)));
                bits::set_row(o.values, i, b);
                o.validity.set_valid(i, v.is_some());
            }
            o.validity.recount();
            out
        }
        DataType::F64 => {
            let mut out = Column::alloc(ctx.pool, to, len);
            let o = out.f64_mut().unwrap();
            for (i, v) in values.iter().enumerate() {
                match v {
                    Some(Scalar::F64(x)) => {
                        o.values[i] = *x;
                        o.validity.set_valid(i, true);
                    }
                    _ => {
                        o.values[i] = 0.0;
                        o.validity.set_valid(i, false);
                    }
                }
            }
            o.validity.recount();
            out
        }
        DataType::I64 | DataType::DateTime | DataType::Duration => {
            let mut out = Column::alloc(ctx.pool, to, len);
            let o = match to {
                DataType::I64 => out.i64_mut().unwrap(),
                DataType::DateTime => out.datetime_mut().unwrap(),
                _ => out.duration_mut().unwrap(),
            };
            for (i, v) in values.iter().enumerate() {
                match v {
                    Some(Scalar::I64(x) | Scalar::DateTime(x) | Scalar::Duration(x)) => {
                        o.values[i] = *x;
                        o.validity.set_valid(i, true);
                    }
                    _ => {
                        o.values[i] = 0;
                        o.validity.set_valid(i, false);
                    }
                }
            }
            o.validity.recount();
            out
        }
        other => unreachable!("build_scalars builds scalars, not {other}"),
    }
}

/// The charset a row's label names, looked up once per run of the same label. `None` for a label
/// `format::encoding` does not know.
struct Charsets<'a> {
    last: Option<(&'a [u8], Option<Encoding>)>,
}

impl<'a> Charsets<'a> {
    fn new() -> Self {
        Charsets { last: None }
    }

    fn get(&mut self, label: &'a [u8]) -> Option<Encoding> {
        match self.last {
            Some((l, e)) if l == label => e,
            _ => {
                let e = std::str::from_utf8(label).ok().and_then(Encoding::for_label);
                self.last = Some((label, e));
                e
            }
        }
    }
}

/// `decode(bytes, label)`: the bytes read as text in the encoding the label names, `"auto"` included, as a source's
/// `encoding` reads its records. Bytes that do not decode become U+FFFD. Null where the label names no encoding, or
/// names `"none"`.
pub fn decode(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    let len = ins[0].len();
    if let Some(e) = short_circuit_empty(ins, &DataType::Str, len) {
        return e;
    }
    let (Some(v), Some(labels)) = (side::var(ins[0]), side::var(ins[1])) else {
        return Column::empty(DataType::Str, len);
    };
    let mut charsets = Charsets::new();
    let mut b = StrBuilder::new(ctx.pool, VarType::Str, len);
    for i in 0..len {
        let encoding = (v.is_valid(i) && labels.is_valid(i))
            .then(|| charsets.get(labels.at(i)))
            .flatten();
        match encoding {
            Some(Encoding::None) | None => b.push_null(),
            Some(encoding) => b.push_grow(ctx.pool, encoding.decode(v.at(i)).text.as_bytes()),
        }
    }
    b.finish()
}

/// `encode(text, label)`: the text's bytes in the charset the label names. Null where the label names none, names
/// `"auto"`, which detects and so has nothing to write, or where the charset cannot spell a character of the text.
pub fn encode(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    let len = ins[0].len();
    if let Some(e) = short_circuit_empty(ins, &DataType::Bytes, len) {
        return e;
    }
    let (Some(v), Some(labels)) = (side::var(ins[0]), side::var(ins[1])) else {
        return Column::empty(DataType::Bytes, len);
    };
    let mut charsets = Charsets::new();
    let mut b = StrBuilder::new(ctx.pool, VarType::Bytes, len);
    let mut out = Vec::new();
    for i in 0..len {
        let charset = match (v.is_valid(i) && labels.is_valid(i))
            .then(|| charsets.get(labels.at(i)))
            .flatten()
        {
            Some(Encoding::Fixed(cs)) => cs,
            _ => {
                b.push_null();
                continue;
            }
        };
        // SAFETY: a `Str` column holds valid UTF-8 by the columnar model's invariant.
        let text = unsafe { std::str::from_utf8_unchecked(v.at(i)) };
        out.clear();
        if encode_text(text, charset, &mut out) {
            b.push_grow(ctx.pool, &out);
        } else {
            b.push_null();
        }
    }
    b.finish()
}

/// `text` in `charset`, or false where the charset cannot spell one of its characters. UTF-16 is written here:
/// encoding_rs follows WHATWG, whose encoders write UTF-8 for it.
fn encode_text(text: &str, charset: Charset, out: &mut Vec<u8>) -> bool {
    if charset == encoding_rs::UTF_16LE {
        out.extend(text.encode_utf16().flat_map(u16::to_le_bytes));
        return true;
    }
    if charset == encoding_rs::UTF_16BE {
        out.extend(text.encode_utf16().flat_map(u16::to_be_bytes));
        return true;
    }
    let (bytes, _, unmappable) = charset.encode(text);
    out.extend_from_slice(&bytes);
    !unmappable
}

/// `parseBytes(text)` and `parseBytes(text, form)`: the bytes `toString` wrote as this text, in the form named
/// (`format::bytes_text`), base64 where none is. Null where the text is not that form, or the form is no form.
pub fn parse_bytes(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    let len = ins[0].len();
    if let Some(e) = short_circuit_empty(ins, &DataType::Bytes, len) {
        return e;
    }
    let v = ins[0].var().unwrap();
    let forms = ins.get(1).and_then(|c| side::var(c));
    let mut b = StrBuilder::new(ctx.pool, VarType::Bytes, len);
    let mut out = Vec::new();
    for i in 0..len {
        out.clear();
        let form = row_form(forms.as_ref(), i);
        if v.validity.is_valid(i) && form.is_some_and(|f| f.read(v.get(i), &mut out)) {
            b.push_grow(ctx.pool, &out);
        } else {
            b.push_null();
        }
    }
    b.finish()
}

/// The bytes form row `i` names, base64 where no form argument was given. `None` where the row's form is null or
/// names no form.
fn row_form(forms: Option<&super::helpers::Side<'_, VarLen<'_>, &[u8]>>, i: usize) -> Option<BytesForm> {
    match forms {
        None => Some(BytesForm::Base64),
        Some(f) => f.is_valid(i).then(|| BytesForm::for_label(f.at(i))).flatten(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn text_table_reads_padded_and_signed_numbers_and_only_true_or_false() {
        assert_eq!(parse_i64("+5"), Some(5));
        assert_eq!(parse_i64("007"), Some(7));
        assert_eq!(parse_i64("2.5"), Some(3));
        assert_eq!(parse_i64("-2.5"), Some(-3));
        assert_eq!(parse_i64("1e300"), None);
        assert_eq!(parse_i64("abc"), None);
        assert_eq!(parse_bool("yes"), None);
    }

    #[test]
    fn f64_rounds_to_i64_half_away_from_zero_and_refuses_outside_the_range() {
        use crate::core::buffer::BufferPool;
        use crate::core::error::ErrorSink;

        let mut pool = BufferPool::new();
        let mut errors = ErrorSink::new();
        let input = [2.5, -2.5, 0.49, 1e300, f64::NAN];
        let mut col = Column::alloc(&mut pool, &DataType::F64, input.len());
        col.f64_mut().unwrap().values.copy_from_slice(&input);
        let out = {
            let mut ctx = Ctx::new(&mut pool, &mut errors);
            coerce(&mut ctx, &col, &DataType::I64)
        };
        let o = out.i64().unwrap();
        let got: Vec<Option<i64>> = (0..input.len())
            .map(|i| o.validity.is_valid(i).then_some(o.values[i]))
            .collect();
        assert_eq!(got, [Some(3), Some(-3), Some(0), None, None]);
        let refused: Vec<u32> = errors.entries().iter().map(|a| a.row).collect();
        assert_eq!(refused, [3, 4]);
        assert!(
            errors
                .entries()
                .iter()
                .all(|a| matches!(a.kind, ExceptionKind::CoercionRefused { .. }))
        );
        out.reclaim(&mut pool);
        col.reclaim(&mut pool);
    }

    /// A blank text is clean only where blank is what failed to read: `encode("", "bogus")` names no charset.
    #[test]
    fn a_label_that_names_nothing_is_refused_even_for_blank_text() {
        use crate::core::buffer::BufferPool;
        use crate::core::error::ErrorSink;

        let mut pool = BufferPool::new();
        let mut errors = ErrorSink::new();
        let strs = |pool: &mut BufferPool, values: &[&str]| {
            let mut b = StrBuilder::new(pool, VarType::Str, values.len());
            for v in values {
                b.push_grow(pool, v.as_bytes());
            }
            b.finish()
        };
        let text = strs(&mut pool, &["", "", "a"]);
        let labels = strs(&mut pool, &["utf-8", "bogus", "bogus"]);
        let out = {
            let mut ctx = Ctx::new(&mut pool, &mut errors);
            convert_or_refuse(&mut ctx, &[&text, &labels], encode)
        };
        assert!(out.is_valid(0));
        let refused: Vec<u32> = errors.entries().iter().map(|a| a.row).collect();
        assert_eq!(refused, [1, 2]);
        for c in [out, text, labels] {
            c.reclaim(&mut pool);
        }
    }
}
