//! Comparison kernels. Every one produces a `Bool` bitmap, written a word at a time. A kernel's row is null where an
//! operand is; `settle_nulls` then gives that row the value JavaScript does.

use super::helpers::{One, Operand, Side, alloc_with_merged_validity, native, short_circuit_empty, two_sides};
use crate::core::bits;
use crate::core::column::{Column, DataType, Validity};
use crate::core::ctx::Ctx;
use crate::ops::BinOp;

/// Write `f(i)` for every row (null rows included; their bit is never read) into a Bool value bitmap, 64 rows per
/// store.
#[inline]
fn pack_bits(out_words: &mut [u64], len: usize, mut f: impl FnMut(usize) -> bool) {
    for (wi, word) in out_words.iter_mut().enumerate() {
        let base = wi * 64;
        let n = (len - base).min(64);
        let mut w = 0u64;
        for b in 0..n {
            if f(base + b) {
                w |= 1u64 << b;
            }
        }
        *word = w;
    }
    bits::mask_tail(out_words, len);
}

macro_rules! cmp_fixed {
    ($name:ident, $la:ident, $ra:ident, $lt:ty, $rt:ty, $f:expr) => {
        pub fn $name(ctx: &mut Ctx, ins: &[&Column]) -> Column {
            fn run<A: Operand<native!($la)>, B: Operand<native!($ra)>>(
                ctx: &mut Ctx,
                a: A,
                b: B,
                va: Option<&Validity>,
                vb: Option<&Validity>,
                len: usize,
            ) -> Column {
                let mut out = alloc_with_merged_validity(ctx, &DataType::Bool, len, &[va, vb]);
                {
                    let o = out.bool_mut().unwrap();
                    let f = $f;
                    pack_bits(o.values, len, |i| f(a.at(i) as $lt, b.at(i) as $rt));
                }
                out
            }
            let len = ins[0].len();
            if let Some(e) = short_circuit_empty(ins, &DataType::Bool, len) {
                return e;
            }
            two_sides!(ctx, ins, len, $la, $ra, run)
        }
    };
}

cmp_fixed!(eq_i64_i64, i64, i64, i64, i64, |x: i64, y: i64| x == y);
cmp_fixed!(ne_i64_i64, i64, i64, i64, i64, |x: i64, y: i64| x != y);
cmp_fixed!(lt_i64_i64, i64, i64, i64, i64, |x: i64, y: i64| x < y);
cmp_fixed!(le_i64_i64, i64, i64, i64, i64, |x: i64, y: i64| x <= y);
cmp_fixed!(gt_i64_i64, i64, i64, i64, i64, |x: i64, y: i64| x > y);
cmp_fixed!(ge_i64_i64, i64, i64, i64, i64, |x: i64, y: i64| x >= y);

cmp_fixed!(eq_f64_f64, f64, f64, f64, f64, |x: f64, y: f64| x == y);
cmp_fixed!(ne_f64_f64, f64, f64, f64, f64, |x: f64, y: f64| x != y);
cmp_fixed!(lt_f64_f64, f64, f64, f64, f64, |x: f64, y: f64| x < y);
cmp_fixed!(le_f64_f64, f64, f64, f64, f64, |x: f64, y: f64| x <= y);
cmp_fixed!(gt_f64_f64, f64, f64, f64, f64, |x: f64, y: f64| x > y);
cmp_fixed!(ge_f64_f64, f64, f64, f64, f64, |x: f64, y: f64| x >= y);

cmp_fixed!(eq_dt_dt, datetime, datetime, i64, i64, |x: i64, y: i64| x == y);
cmp_fixed!(ne_dt_dt, datetime, datetime, i64, i64, |x: i64, y: i64| x != y);
cmp_fixed!(lt_dt_dt, datetime, datetime, i64, i64, |x: i64, y: i64| x < y);
cmp_fixed!(le_dt_dt, datetime, datetime, i64, i64, |x: i64, y: i64| x <= y);
cmp_fixed!(gt_dt_dt, datetime, datetime, i64, i64, |x: i64, y: i64| x > y);
cmp_fixed!(ge_dt_dt, datetime, datetime, i64, i64, |x: i64, y: i64| x >= y);

cmp_fixed!(eq_dur_dur, duration, duration, i64, i64, |x: i64, y: i64| x == y);
cmp_fixed!(ne_dur_dur, duration, duration, i64, i64, |x: i64, y: i64| x != y);
cmp_fixed!(lt_dur_dur, duration, duration, i64, i64, |x: i64, y: i64| x < y);
cmp_fixed!(le_dur_dur, duration, duration, i64, i64, |x: i64, y: i64| x <= y);
cmp_fixed!(gt_dur_dur, duration, duration, i64, i64, |x: i64, y: i64| x > y);
cmp_fixed!(ge_dur_dur, duration, duration, i64, i64, |x: i64, y: i64| x >= y);

// Bool operands are bitmaps, so equality is one word op per 64 rows.
macro_rules! cmp_bool {
    ($name:ident, $f:expr) => {
        pub fn $name(ctx: &mut Ctx, ins: &[&Column]) -> Column {
            let len = ins[0].len();
            if let Some(e) = short_circuit_empty(ins, &DataType::Bool, len) {
                return e;
            }
            let a = ins[0].bool().unwrap();
            let b = ins[1].bool().unwrap();
            let mut out = alloc_with_merged_validity(ctx, &DataType::Bool, len, &[Some(a.validity), Some(b.validity)]);
            {
                let o = out.bool_mut().unwrap();
                let f = $f;
                for i in 0..o.values.len() {
                    o.values[i] = f(a.values[i], b.values[i]);
                }
                bits::mask_tail(o.values, len);
            }
            out
        }
    };
}

cmp_bool!(eq_bool_bool, |x: u64, y: u64| !(x ^ y));
cmp_bool!(ne_bool_bool, |x: u64, y: u64| x ^ y);

// Bytewise, which for UTF-8 is code-point order. Not locale collation.
macro_rules! cmp_var {
    ($name:ident, $f:expr) => {
        pub fn $name(ctx: &mut Ctx, ins: &[&Column]) -> Column {
            fn run<'a, A: Operand<&'a [u8]>, B: Operand<&'a [u8]>>(
                ctx: &mut Ctx,
                a: A,
                b: B,
                va: Option<&Validity>,
                vb: Option<&Validity>,
                len: usize,
            ) -> Column {
                let mut out = alloc_with_merged_validity(ctx, &DataType::Bool, len, &[va, vb]);
                {
                    let o = out.bool_mut().unwrap();
                    let f = $f;
                    pack_bits(o.values, len, |i| f(a.at(i), b.at(i)));
                }
                out
            }
            let len = ins[0].len();
            if let Some(e) = short_circuit_empty(ins, &DataType::Bool, len) {
                return e;
            }
            two_sides!(ctx, ins, len, var, var, run)
        }
    };
}

cmp_var!(eq_var_var, |x: &[u8], y: &[u8]| x == y);
cmp_var!(ne_var_var, |x: &[u8], y: &[u8]| x != y);
cmp_var!(lt_var_var, |x: &[u8], y: &[u8]| x < y);
cmp_var!(le_var_var, |x: &[u8], y: &[u8]| x <= y);
cmp_var!(gt_var_var, |x: &[u8], y: &[u8]| x > y);
cmp_var!(ge_var_var, |x: &[u8], y: &[u8]| x >= y);

/// Each row of `a op b` that `out` leaves null, settled: `==` is true where both operands are null, `!=` is its
/// negation, and an ordering is false. `out` is what the kernel gave, or all null where no kernel takes the two types.
pub fn settle_nulls(ctx: &mut Ctx, op: BinOp, a: &Column, b: &Column, out: Column) -> Column {
    let len = out.len();
    if !out.has_nulls() {
        return out;
    }
    let words = bits::words_for(len);
    let valid = |c: &Column| -> Vec<u64> {
        let mut w = vec![0u64; words];
        match c.validity() {
            Some(v) => w.copy_from_slice(&v.bits()[..words]),
            None if c.is_unbound() => {}
            None => bits::fill_valid(&mut w, len),
        }
        w
    };
    let (va, vb, known) = (valid(a), valid(b), valid(&out));
    let zero = vec![0u64; words];
    let got = out.bool().map(|o| o.values).unwrap_or(&zero);
    let mut settled = Column::alloc(ctx.pool, &DataType::Bool, len);
    {
        let o = settled.bool_mut().unwrap();
        for w in 0..words {
            let null_row = match op {
                BinOp::Eq => !va[w] & !vb[w],
                BinOp::Ne => va[w] | vb[w],
                _ => 0,
            };
            o.values[w] = (got[w] & known[w]) | (null_row & !known[w]);
        }
        bits::mask_tail(o.values, len);
        bits::fill_valid(o.validity.bits_mut(), len);
        o.validity.recount();
    }
    out.reclaim(ctx.pool);
    settled
}
