//! Logical kernels, in n-ary form, and the operators that read null as a value rather than propagating it.
//!
//! `&&`, `||` and `!` read null as false (design doc, Language), so none of them yields null. `??` takes the first non-null operand per row. None of these may use `short_circuit_empty`.
//! Each reads a literal operand as one value.

use super::arrayops;
use super::helpers::{self, One, Operand, Side, side};
use crate::core::bits;
use crate::core::buffer::BufferPool;
use crate::core::column::{Column, DataType, Scalar, StrBuilder, VarType};
use crate::core::ctx::Ctx;

/// A Bool column's `(values, validity)` words; an unbound operand reads as all-null (`zero` for both).
fn bool_words<'a>(c: &'a Column, zero: &'a [u64]) -> (&'a [u64], &'a [u64]) {
    match c.bool() {
        Ok(b) => (b.values, b.validity.bits()),
        Err(_) => (zero, zero),
    }
}

/// The value a literal operand of `&&` or `||` holds. `None` is rows.
fn one_bool(c: &Column) -> Option<bool> {
    match c.constant_value() {
        Some(Scalar::Bool(b)) => Some(*b),
        _ => None,
    }
}

/// True where every operand is true. A null operand reads as false. Never null.
pub fn and_n(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    logic_n(ctx, ins, false)
}

/// True where some operand is true. A null operand reads as false. Never null.
pub fn or_n(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    logic_n(ctx, ins, true)
}

/// `&&` and `||` are one shape. One value decides a row wherever it appears: `false` for `&&`, `true` for `||`. A
/// null is false, so it decides a row of `&&` and nothing in `||`.
///
/// A literal is read before any row: a deciding one decides every row, and the other kind drops out.
fn logic_n(ctx: &mut Ctx, ins: &[&Column], deciding: bool) -> Column {
    let len = ins[0].len();
    if ins.iter().any(|c| one_bool(c) == Some(deciding)) {
        return Column::filled(ctx.pool, &Scalar::Bool(deciding), len);
    }
    let rows: Vec<&Column> = ins.iter().copied().filter(|c| one_bool(c).is_none()).collect();
    if rows.is_empty() {
        return Column::filled(ctx.pool, &Scalar::Bool(!deciding), len);
    }

    let words = bits::words_for(len);
    let zero = vec![0u64; words];
    let mut out = Column::alloc(ctx.pool, &DataType::Bool, len);
    {
        let o = out.bool_mut().unwrap();
        o.values.fill(if deciding { 0 } else { !0u64 });
        for c in rows {
            let (v, valid) = bool_words(c, &zero);
            for w in 0..words {
                let truth = v[w] & valid[w];
                if deciding {
                    o.values[w] |= truth;
                } else {
                    o.values[w] &= truth;
                }
            }
        }
        bits::mask_tail(o.values, len);
        bits::fill_valid(o.validity.bits_mut(), len);
        o.validity.recount();
    }
    out
}

/// `!x`, true where `x` is null. Never null.
pub fn not_bool(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    let len = ins[0].len();
    let Ok(a) = ins[0].bool() else {
        return Column::filled(ctx.pool, &Scalar::Bool(true), len);
    };
    let mut out = Column::alloc(ctx.pool, &DataType::Bool, len);
    {
        let o = out.bool_mut().unwrap();
        for w in 0..o.values.len() {
            o.values[w] = !(a.values[w] & a.validity.bits()[w]);
        }
        bits::mask_tail(o.values, len);
        bits::fill_valid(o.validity.bits_mut(), len);
        o.validity.recount();
    }
    out
}

/// `a ?? b ?? c`: the first non-null operand per row. `winners` decides who takes each row, reading operands until
/// every row is taken, so a rarely-null `a` barely reads `b`; `take_met` then copies, at the type the operands meet at.
/// A literal holds a value in every row, so it takes every row still open and ends the read.
pub fn coalesce_n(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    let len = ins[0].len();
    let takes = winners(ins, len);
    let takes: Vec<&[u64]> = takes.iter().map(Vec::as_slice).collect();
    take_met(ctx, ins, &takes, len)
}

/// Which operand takes each row: the first, in order, that holds a value there. One mask per operand; a row is in at
/// most one. Operands after the one that takes the last open row are not read.
fn winners(ins: &[&Column], len: usize) -> Vec<Vec<u64>> {
    let words = bits::words_for(len);
    let mut resolved = vec![0u64; words];
    let mut takes = vec![vec![0u64; words]; ins.len()];
    for (c, mask) in ins.iter().zip(takes.iter_mut()) {
        if bits::all_set(&resolved, len) {
            break;
        }
        rows_with_a_value(c, len, mask);
        for w in 0..words {
            mask[w] &= !resolved[w];
            resolved[w] |= mask[w];
        }
    }
    takes
}

/// Set the bits of `out` for the rows `c` holds a value in; the rest keep what they were. A constant holds one in
/// every row. A struct holds one where any leaf or its unclaimed fields do. An unbound column holds none.
pub fn rows_with_a_value(c: &Column, len: usize, out: &mut [u64]) {
    match (c.constant_value(), c.validity()) {
        (Some(_), _) => bits::fill_valid(out, len),
        (None, Some(v)) => {
            for (o, &b) in out.iter_mut().zip(v.bits()) {
                *o |= b;
            }
            bits::mask_tail(out, len);
        }
        (None, None) => {
            let DataType::Struct(fields, _) = c.data_type() else {
                return;
            };
            for (name, _) in &fields {
                if let Some(leaf) = c.field(name) {
                    rows_with_a_value(leaf, len, out);
                }
            }
            if let Some(unclaimed_fields) = c.unclaimed_fields() {
                rows_with_a_value(unclaimed_fields, len, out);
            }
        }
    }
}

/// The one row picker: row `i` of a column of `ty` comes from the source whose mask holds `i`, and is null where none
/// does or where that source holds no value there. `c ? a : b` is `take([a, b], [c, !c])`; `a ?? b` is
/// `take([a, b], [has(a), !has(a)])`. The masks are disjoint.
///
/// A struct is taken per field, under the same masks, so a row is one source's whole struct row. Its fields are every
/// field any source has; a field a source lacks is null on the rows that source wins. Each field meets at the type its
/// columns hold (`meet`), so a field held as text beside one held as a variant is taken as a variant, each value as
/// it is.
pub(crate) fn take(ctx: &mut Ctx, ins: &[&Column], takes: &[&[u64]], ty: &DataType, len: usize) -> Column {
    match ty {
        // A `null` type holds no value: its column is `Empty`, as `build`'s is.
        DataType::Null => Column::empty(DataType::Null, len),
        DataType::Str | DataType::Bytes | DataType::Variant => take_varlen(ctx, ins, takes, ty, len),
        DataType::Array(elem_ty) => arrayops::take_list(ctx, ins, takes, elem_ty, len),
        DataType::Struct(..) => {
            // An unbound struct has no fields to read, and the rows it wins are null.
            let (cols, takes): (Vec<&Column>, Vec<&[u64]>) = ins
                .iter()
                .copied()
                .zip(takes.iter().copied())
                .filter(|(c, _)| !c.is_unbound())
                .unzip();
            let mut names: Vec<String> = Vec::new();
            for c in &cols {
                if let DataType::Struct(fields, _) = c.data_type() {
                    for (name, _) in fields {
                        if !names.contains(&name) {
                            names.push(name);
                        }
                    }
                }
            }
            let out = names
                .into_iter()
                .map(|name| {
                    let held = cols.iter().find_map(|c| c.field(&name)).expect("listed from a source");
                    let missing = Column::empty(held.data_type(), len);
                    let parts: Vec<&Column> = cols.iter().map(|c| c.field(&name).unwrap_or(&missing)).collect();
                    let field = take_met(ctx, &parts, &takes, len);
                    (name, field)
                })
                .collect();
            let unclaimed_fields = cols.iter().any(|c| c.unclaimed_fields().is_some()).then(|| {
                let missing = Column::empty(DataType::Variant, len);
                let parts: Vec<&Column> = cols.iter().map(|c| c.unclaimed_fields().unwrap_or(&missing)).collect();
                take(ctx, &parts, &takes, &DataType::Variant, len)
            });
            Column::struct_with_unclaimed_fields(out, unclaimed_fields, len)
        }
        DataType::Bool => take_bool(ctx.pool, ins, takes, len),
        _ => take_masked(ctx.pool, ty, ins, takes, len),
    }
}

/// `take` at the type `ins` meet at, each source not already a variant held as one first where they meet as a
/// variant.
pub(crate) fn take_met(ctx: &mut Ctx, ins: &[&Column], takes: &[&[u64]], len: usize) -> Column {
    let ty = meet(ins);
    let rendered: Vec<Option<Column>> = ins
        .iter()
        .map(|c| {
            (ty == DataType::Variant && !c.is_unbound() && c.data_type() != DataType::Variant)
                .then(|| crate::format::tag::to_variant(c, ctx.pool))
        })
        .collect();
    let sources: Vec<&Column> = ins
        .iter()
        .zip(&rendered)
        .map(|(c, r)| r.as_ref().unwrap_or(c))
        .collect();
    let out = take(ctx, &sources, takes, &ty, len);
    drop(sources);
    for c in rendered.into_iter().flatten() {
        c.reclaim(ctx.pool);
    }
    out
}

/// The type the columns meet at, from what they hold. Structs meet as a struct, their own fields
/// met one by one, whatever an unbound column beside them declares. Otherwise a variant anywhere makes a variant, the
/// bound columns blend, and an unbound column's declared type widens the blend where the two unify. With nothing
/// bound, the declared types unified.
fn meet(ins: &[&Column]) -> DataType {
    let types: Vec<(bool, DataType)> = ins.iter().map(|c| (c.is_unbound(), c.data_type())).collect();
    let mut bound = types.iter().filter(|(unbound, _)| !unbound).map(|(_, t)| t);
    if bound.clone().next().is_some() && bound.all(|t| matches!(t, DataType::Struct(..))) {
        return DataType::Struct(Vec::new(), false);
    }
    if types.iter().any(|(_, t)| *t == DataType::Variant) {
        return DataType::Variant;
    }
    let Some(mut ty) = helpers::blend_types(types.iter().filter(|(unbound, _)| !unbound).map(|(_, t)| t.clone()))
    else {
        // A `null` literal declares nothing: an unbound array beside it is still an array. Types that do not unify
        // meet as a variant, as bound ones do.
        let mut declared = types.iter().map(|(_, t)| t.clone());
        let first = declared.next().unwrap_or(DataType::Null);
        return declared.fold(first, |ty, t| t.unify(&ty).unwrap_or(DataType::Variant));
    };
    for (_, declared) in types.iter().filter(|(unbound, _)| *unbound) {
        ty = declared.unify(&ty).unwrap_or(ty);
    }
    ty
}

/// A varlen column cannot be patched in place, so text is a rebuild. The reserve is what every source holds, read
/// off its offsets, rather than a pass over the rows to find each winner's length first.
fn take_varlen(ctx: &mut Ctx, ins: &[&Column], takes: &[&[u64]], ty: &DataType, len: usize) -> Column {
    let var_type = match ty {
        DataType::Bytes => VarType::Bytes,
        DataType::Variant => VarType::Variant,
        _ => VarType::Str,
    };
    let readers: Vec<_> = ins
        .iter()
        .map(|c| match var_type {
            VarType::Variant => c.variant().ok().map(|v| {
                let validity = v.validity;
                Side::Rows(v, validity)
            }),
            _ => side::var(c),
        })
        .collect();
    let total: usize = readers.iter().flatten().map(|r| r.total_bytes(len)).sum();
    let mut b = StrBuilder::new(ctx.pool, var_type, len);
    b.reserve(ctx.pool, total);
    // A word at a time: which source holds a value under its mask, for 64 rows at once.
    let mut held = vec![0u64; ins.len()];
    for w in 0..bits::words_for(len) {
        let mut any = 0u64;
        for ((h, reader), take) in held.iter_mut().zip(&readers).zip(takes) {
            *h = reader
                .as_ref()
                .map_or(0, |r| take[w] & r.validity().map_or(!0, |v| v.bits()[w]));
            any |= *h;
        }
        for bit in 0..(len - w * 64).min(64) {
            if any >> bit & 1 == 0 {
                b.push_null();
                continue;
            }
            let k = held
                .iter()
                .position(|h| h >> bit & 1 != 0)
                .expect("a source holds this row");
            b.push(readers[k].as_ref().expect("held").at(w * 64 + bit));
        }
    }
    b.finish()
}

/// `take` for a fixed-width column (`I64`, `F64`, `DateTime`, `Duration`): each source's rows copied under its mask,
/// one source at a time. A source is a bound column or a constant of `ty`.
fn take_masked(pool: &mut BufferPool, ty: &DataType, sources: &[&Column], masks: &[&[u64]], len: usize) -> Column {
    let words = bits::words_for(len);
    let mut out = Column::alloc(pool, ty, len);
    let mut valid = vec![0u64; words];
    let mut held = vec![0u64; words];
    for (src, mask) in sources.iter().zip(masks) {
        held.copy_from_slice(&mask[..words]);
        match (src.constant_value(), src.validity()) {
            (Some(_), _) => {}
            (None, Some(v)) => {
                for (h, &b) in held.iter_mut().zip(v.bits()) {
                    *h &= b;
                }
            }
            (None, None) => continue,
        }
        if !bits::any_set(&held, len) {
            continue;
        }
        copy_rows(&mut out, src, &held, len);
        for (v, &h) in valid.iter_mut().zip(&held) {
            *v |= h;
        }
    }
    let v = out.validity_mut().unwrap();
    v.bits_mut().copy_from_slice(&valid);
    v.recount();
    out
}

/// `take` for Bool: all words, one pass per source, a word's values and validity at once.
fn take_bool(pool: &mut BufferPool, sources: &[&Column], masks: &[&[u64]], len: usize) -> Column {
    let words = bits::words_for(len);
    let mut out = Column::alloc(pool, &DataType::Bool, len);
    {
        let o = out.bool_mut().unwrap();
        o.values.fill(0);
        let valid = o.validity.bits_mut();
        valid.fill(0);
        for (src, mask) in sources.iter().zip(masks) {
            match (src.bool(), src.constant_value()) {
                (Ok(s), _) => {
                    let held = s.validity.bits();
                    for w in 0..words {
                        o.values[w] |= s.values[w] & mask[w];
                        valid[w] |= held[w] & mask[w];
                    }
                }
                (Err(_), Some(Scalar::Bool(x))) => {
                    for w in 0..words {
                        if *x {
                            o.values[w] |= mask[w];
                        }
                        valid[w] |= mask[w];
                    }
                }
                _ => {}
            }
        }
        o.validity.recount();
    }
    out
}

/// Copy the rows `mask` selects from `src` into `dst`.
fn copy_rows(dst: &mut Column, src: &Column, mask: &[u64], len: usize) {
    const UNIFIED: &str = "a source unifies with the column built";
    match dst.data_type() {
        DataType::I64 => {
            let d = dst.i64_mut().unwrap();
            write_side(d.values, side::i64(src).expect(UNIFIED), mask, len);
        }
        DataType::DateTime => {
            let d = dst.datetime_mut().unwrap();
            write_side(d.values, side::datetime(src).expect(UNIFIED), mask, len);
        }
        DataType::Duration => {
            let d = dst.duration_mut().unwrap();
            write_side(d.values, side::duration(src).expect(UNIFIED), mask, len);
        }
        DataType::F64 => {
            let d = dst.f64_mut().unwrap();
            write_side(d.values, side::f64(src).expect(UNIFIED), mask, len);
        }
        _ => unreachable!("take_masked takes only fixed-width types"),
    }
}

/// `write_rows` with the operand's shape matched once, not per row.
fn write_side<T: Copy>(d: &mut [T], s: Side<'_, &[T], T>, mask: &[u64], len: usize) {
    match s {
        Side::Rows(v, _) => write_rows(d, &v, mask, len),
        Side::One(x) => write_rows(d, &One(x), mask, len),
    }
}

/// `d[i] = s.at(i)` for every row `mask` selects.
fn write_rows<T: Copy>(d: &mut [T], s: &impl Operand<T>, mask: &[u64], len: usize) {
    bits::for_each_set(mask, len, |i| d[i] = s.at(i));
}

/// `isNull(x)`. Never null itself. A `Struct` row is null when every leaf is null (design doc, Language: a
/// null struct and a struct of all-null fields are one thing).
pub fn is_null(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    let len = ins[0].len();
    let mut out = Column::alloc(ctx.pool, &DataType::Bool, len);
    {
        let o = out.bool_mut().unwrap();
        let mut held = vec![0u64; bits::words_for(len)];
        rows_with_a_value(ins[0], len, &mut held);
        bits::not_into(o.values, &held, len);
        o.validity.set_all_valid();
    }
    out
}
