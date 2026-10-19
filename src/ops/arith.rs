//! Arithmetic kernels.
//!
//! The table pairs like with like: an `I64` beside an `F64` is widened by the compiler before any kernel runs. An
//! `I64` row whose result does not fit is null and marked `Overflow` (design doc, Language). The loop runs with
//! wrapping arithmetic and a branchless witness; only a batch the witness tripped on is walked again to find its rows.

use super::helpers::{
    One, Operand, Rows, Side, alloc_with_merged_validity, f64_values, native, short_circuit_empty, side, two_sides,
};
use crate::core::column::{Column, DataType, Validity};
use crate::core::ctx::Ctx;
use crate::core::error::ExceptionKind;
use crate::ops::convert::in_i64_range;

/// Null and raise every row of `out` that `overflowed`. A row null from its inputs is left alone: its value is
/// whatever the input buffer held, and it has not failed.
fn fail_overflowed(ctx: &mut Ctx, out: &mut Column, len: usize, overflowed: impl Fn(usize) -> bool) {
    let o = out.i64_mut().unwrap();
    for i in 0..len {
        if overflowed(i) && o.validity.is_valid(i) {
            o.values[i] = 0;
            o.validity.set_valid(i, false);
            ctx.raise(i, ExceptionKind::Overflow);
        }
    }
}

macro_rules! int_arith {
    ($name:ident, $wrapping:ident, $ovf:expr) => {
        pub fn $name(ctx: &mut Ctx, ins: &[&Column]) -> Column {
            fn run<A: Operand<i64>, B: Operand<i64>>(
                ctx: &mut Ctx,
                a: A,
                b: B,
                va: Option<&Validity>,
                vb: Option<&Validity>,
                len: usize,
            ) -> Column {
                let mut out = alloc_with_merged_validity(ctx, &DataType::I64, len, &[va, vb]);
                let mut acc: i64 = 0;
                {
                    let o = out.i64_mut().unwrap();
                    for i in 0..len {
                        let (x, y) = (a.at(i), b.at(i));
                        let r = x.$wrapping(y);
                        o.values[i] = r;
                        acc |= $ovf(x, y, r);
                    }
                }
                if acc < 0 {
                    fail_overflowed(ctx, &mut out, len, |i| {
                        let (x, y) = (a.at(i), b.at(i));
                        $ovf(x, y, x.$wrapping(y)) < 0
                    });
                }
                out
            }
            let len = ins[0].len();
            if let Some(e) = short_circuit_empty(ins, &DataType::I64, len) {
                return e;
            }
            two_sides!(ctx, ins, len, i64, i64, run)
        }
    };
}

// Signed overflow witness: the sign bit of `(a ^ r) & (b ^ r)` for add, `(a ^ b) & (a ^ r)` for sub.
int_arith!(add_i64_i64, wrapping_add, |x: i64, y: i64, r: i64| (x ^ r) & (y ^ r));
int_arith!(sub_i64_i64, wrapping_sub, |x: i64, y: i64, r: i64| (x ^ y) & (x ^ r));

pub fn mul_i64_i64(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    fn run<A: Operand<i64>, B: Operand<i64>>(
        ctx: &mut Ctx,
        a: A,
        b: B,
        va: Option<&Validity>,
        vb: Option<&Validity>,
        len: usize,
    ) -> Column {
        let overflows = |i: usize| {
            let wide = (a.at(i) as i128) * (b.at(i) as i128);
            (wide as i64, wide != (wide as i64) as i128)
        };
        let mut out = alloc_with_merged_validity(ctx, &DataType::I64, len, &[va, vb]);
        let mut overflowed = false;
        {
            let o = out.i64_mut().unwrap();
            for i in 0..len {
                let (r, ovf) = overflows(i);
                o.values[i] = r;
                overflowed |= ovf;
            }
        }
        if overflowed {
            fail_overflowed(ctx, &mut out, len, |i| overflows(i).1);
        }
        out
    }
    let len = ins[0].len();
    if let Some(e) = short_circuit_empty(ins, &DataType::I64, len) {
        return e;
    }
    two_sides!(ctx, ins, len, i64, i64, run)
}

/// A row whose divisor is zero is null and marked `DivideByZero`.
macro_rules! div_like {
    ($name:ident, $la:ident, $ra:ident, $out:expr, $zero:expr, $f:expr) => {
        pub fn $name(ctx: &mut Ctx, ins: &[&Column]) -> Column {
            fn run<A: Operand<native!($la)>, B: Operand<native!($ra)>>(
                ctx: &mut Ctx,
                a: A,
                b: B,
                va: Option<&Validity>,
                vb: Option<&Validity>,
                len: usize,
            ) -> Column {
                let out_ty = $out;
                let mut out = alloc_with_merged_validity(ctx, &out_ty, len, &[va, vb]);
                let mut zeros: Vec<u32> = Vec::new();
                {
                    let f = $f;
                    // A null operand is null already, thrown for where it was made: only a value divided by zero throws.
                    let held = |v: Option<&Validity>, i| v.is_none_or(|v| v.is_valid(i));
                    write_rows(&mut out, &out_ty, |i| {
                        if b.at(i) == $zero {
                            if held(va, i) && held(vb, i) {
                                zeros.push(i as u32);
                            }
                            None
                        } else {
                            Some(f(a.at(i), b.at(i)))
                        }
                    });
                }
                for row in zeros {
                    ctx.raise(row as usize, ExceptionKind::DivideByZero);
                }
                out
            }
            let len = ins[0].len();
            if let Some(e) = short_circuit_empty(ins, &$out, len) {
                return e;
            }
            two_sides!(ctx, ins, len, $la, $ra, run)
        }
    };
}

/// Fill `out` (any 8-byte type) row by row; `None` nulls the row.
fn write_rows<T: Copy + Into<Slot>>(out: &mut Column, ty: &DataType, mut f: impl FnMut(usize) -> Option<T>) {
    let len = out.len();
    match ty {
        DataType::F64 => {
            let o = out.f64_mut().unwrap();
            for i in 0..len {
                match f(i) {
                    Some(v) => o.values[i] = Slot::from_value(v).as_f64(),
                    None => {
                        o.values[i] = 0.0;
                        o.validity.set_valid(i, false);
                    }
                }
            }
        }
        _ => {
            let o = match ty {
                DataType::Duration => out.duration_mut().unwrap(),
                _ => out.i64_mut().unwrap(),
            };
            for i in 0..len {
                match f(i) {
                    Some(v) => o.values[i] = Slot::from_value(v).as_i64(),
                    None => {
                        o.values[i] = 0;
                        o.validity.set_valid(i, false);
                    }
                }
            }
        }
    }
}

/// One 8-byte value of either kind, so `write_rows` can take both.
#[derive(Clone, Copy)]
enum Slot {
    I(i64),
    F(f64),
}

impl Slot {
    fn from_value<T: Into<Slot>>(v: T) -> Slot {
        v.into()
    }
    fn as_i64(self) -> i64 {
        match self {
            Slot::I(v) => v,
            Slot::F(v) => v as i64,
        }
    }
    fn as_f64(self) -> f64 {
        match self {
            Slot::I(v) => v as f64,
            Slot::F(v) => v,
        }
    }
}

impl From<i64> for Slot {
    fn from(v: i64) -> Slot {
        Slot::I(v)
    }
}

impl From<f64> for Slot {
    fn from(v: f64) -> Slot {
        Slot::F(v)
    }
}

// `/` on integers yields F64 (design doc, Language).
div_like!(div_i64_i64, i64, i64, DataType::F64, 0, |x: i64, y: i64| x as f64
    / y as f64);
// `idiv(a, b)`: integer division truncating toward zero, the pair of `%`. `i64::MIN / -1` traps, hence
// `wrapping_div`.
div_like!(idiv_wrapping, i64, i64, DataType::I64, 0, |x: i64, y: i64| x
    .wrapping_div(y));

div_like!(rem_f64_f64, f64, f64, DataType::F64, 0.0, |x: f64, y: f64| x % y);
div_like!(idiv_f64_f64, f64, f64, DataType::F64, 0.0, |x: f64, y: f64| (x / y)
    .trunc());

/// Whether a `numeric_join` function answers `i64` over `ins`: every column is `i64` or all null, as
/// `Ret::NumericJoin` types it, so the column has the type the compiler gave the call.
fn joins_to_i64(ins: &[&Column]) -> bool {
    ins.iter()
        .all(|c| matches!(c.data_type(), DataType::I64 | DataType::Null))
}

/// `idiv(a, b)`: an `i64` from two `i64`s, else an `f64`, an `i64` beside an `f64` widening, as `%` gives.
pub fn idiv_num(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    if joins_to_i64(ins) {
        idiv_i64_i64(ctx, ins)
    } else {
        idiv_f64_f64(ctx, ins)
    }
}

/// `idiv(a, b)`, where `i64::MIN / -1`, the one quotient outside `i64`, fails its row.
pub fn idiv_i64_i64(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    let mut out = idiv_wrapping(ctx, ins);
    let len = out.len();
    if let (Some(a), Some(b)) = (side::i64(ins[0]), side::i64(ins[1])) {
        let (a, b) = (a.rows(len), b.rows(len));
        fail_overflowed(ctx, &mut out, len, |i| a[i] == i64::MIN && b[i] == -1);
    }
    out
}
// `i64::MIN % -1` traps, hence `wrapping_rem`.
div_like!(rem_i64_i64, i64, i64, DataType::I64, 0, |x: i64, y: i64| x
    .wrapping_rem(y));
div_like!(
    rem_dur_dur,
    duration,
    duration,
    DataType::Duration,
    0,
    |x: i64, y: i64| x.wrapping_rem(y)
);
div_like!(div_dur_dur, duration, duration, DataType::F64, 0, |x: i64, y: i64| x
    as f64
    / y as f64);
div_like!(div_dur_i64, duration, i64, DataType::Duration, 0, |x: i64, y: i64| x
    .wrapping_div(y));
div_like!(
    div_dur_f64,
    duration,
    f64,
    DataType::Duration,
    0.0,
    |x: i64, y: f64| (x as f64 / y) as i64
);

macro_rules! float_arith {
    ($name:ident, $la:ident, $ra:ident, $f:expr) => {
        pub fn $name(ctx: &mut Ctx, ins: &[&Column]) -> Column {
            fn run<A: Operand<native!($la)>, B: Operand<native!($ra)>>(
                ctx: &mut Ctx,
                a: A,
                b: B,
                va: Option<&Validity>,
                vb: Option<&Validity>,
                len: usize,
            ) -> Column {
                let mut out = alloc_with_merged_validity(ctx, &DataType::F64, len, &[va, vb]);
                {
                    let o = out.f64_mut().unwrap();
                    let f = $f;
                    for i in 0..len {
                        o.values[i] = f(a.at(i) as f64, b.at(i) as f64);
                    }
                }
                out
            }
            let len = ins[0].len();
            if let Some(e) = short_circuit_empty(ins, &DataType::F64, len) {
                return e;
            }
            two_sides!(ctx, ins, len, $la, $ra, run)
        }
    };
}

float_arith!(add_f64_f64, f64, f64, |x: f64, y: f64| x + y);
float_arith!(sub_f64_f64, f64, f64, |x: f64, y: f64| x - y);
float_arith!(mul_f64_f64, f64, f64, |x: f64, y: f64| x * y);
float_arith!(pow_f64_f64, f64, f64, |x: f64, y: f64| x.powf(y));
// A power is F64 whatever its operands (design doc, Language).
float_arith!(pow_i64_i64, i64, i64, |x: f64, y: f64| x.powf(y));

/// Float division. Divide by zero is null, not infinity.
pub fn div_f64_f64(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    let len = ins[0].len();
    if let Some(e) = short_circuit_empty(ins, &DataType::F64, len) {
        return e;
    }
    let av = f64_values(ins[0]).unwrap();
    let bv = f64_values(ins[1]).unwrap();
    let mut out = alloc_with_merged_validity(ctx, &DataType::F64, len, &[ins[0].validity(), ins[1].validity()]);
    let mut zeros: Vec<u32> = Vec::new();
    // A null operand is null already, thrown for where it was made: only a value divided by zero throws.
    let held = |c: &Column, i| c.validity().is_none_or(|v| v.is_valid(i));
    write_rows(&mut out, &DataType::F64, |i| {
        if bv[i] == 0.0 {
            if held(ins[0], i) && held(ins[1], i) {
                zeros.push(i as u32);
            }
            None
        } else {
            Some(av[i] / bv[i])
        }
    });
    for row in zeros {
        ctx.raise(row as usize, ExceptionKind::DivideByZero);
    }
    out
}

// DateTime and Duration are both i64 microseconds; the type table keeps instants and spans apart.
macro_rules! temporal {
    ($name:ident, $la:ident, $ra:ident, $out:expr, $f:expr) => {
        pub fn $name(ctx: &mut Ctx, ins: &[&Column]) -> Column {
            fn run<A: Operand<native!($la)>, B: Operand<native!($ra)>>(
                ctx: &mut Ctx,
                a: A,
                b: B,
                va: Option<&Validity>,
                vb: Option<&Validity>,
                len: usize,
            ) -> Column {
                let out_ty = $out;
                let mut out = alloc_with_merged_validity(ctx, &out_ty, len, &[va, vb]);
                {
                    let o = match out_ty {
                        DataType::DateTime => out.datetime_mut().unwrap(),
                        _ => out.duration_mut().unwrap(),
                    };
                    let f = $f;
                    for i in 0..len {
                        o.values[i] = f(a.at(i), b.at(i));
                    }
                }
                out
            }
            let len = ins[0].len();
            if let Some(e) = short_circuit_empty(ins, &$out, len) {
                return e;
            }
            two_sides!(ctx, ins, len, $la, $ra, run)
        }
    };
}

temporal!(add_dt_dur, datetime, duration, DataType::DateTime, |x: i64, y: i64| x
    .wrapping_add(y));
temporal!(add_dur_dt, duration, datetime, DataType::DateTime, |x: i64, y: i64| x
    .wrapping_add(y));
temporal!(sub_dt_dur, datetime, duration, DataType::DateTime, |x: i64, y: i64| x
    .wrapping_sub(y));
temporal!(sub_dt_dt, datetime, datetime, DataType::Duration, |x: i64, y: i64| x
    .wrapping_sub(y));
temporal!(add_dur_dur, duration, duration, DataType::Duration, |x: i64, y: i64| x
    .wrapping_add(y));
temporal!(sub_dur_dur, duration, duration, DataType::Duration, |x: i64, y: i64| x
    .wrapping_sub(y));
temporal!(mul_dur_i64, duration, i64, DataType::Duration, |x: i64, y: i64| x
    .wrapping_mul(y));
temporal!(mul_i64_dur, i64, duration, DataType::Duration, |x: i64, y: i64| x
    .wrapping_mul(y));
temporal!(
    mul_dur_f64,
    duration,
    f64,
    DataType::Duration,
    |x: i64, y: f64| (x as f64 * y) as i64
);
temporal!(
    mul_f64_dur,
    f64,
    duration,
    DataType::Duration,
    |x: f64, y: i64| (x * y as f64) as i64
);

macro_rules! unary_i64 {
    ($name:ident, $acc:ident, $mut_acc:ident, $ty:expr, $f:expr) => {
        pub fn $name(ctx: &mut Ctx, ins: &[&Column]) -> Column {
            let len = ins[0].len();
            let out_ty = $ty;
            if let Some(e) = short_circuit_empty(ins, &out_ty, len) {
                return e;
            }
            let a = ins[0].$acc().unwrap();
            let mut out = alloc_with_merged_validity(ctx, &out_ty, len, &[Some(a.validity), None]);
            {
                let o = out.$mut_acc().unwrap();
                let f = $f;
                for i in 0..len {
                    o.values[i] = f(a.values[i]);
                }
            }
            out
        }
    };
}

unary_i64!(neg_wrapping, i64, i64_mut, DataType::I64, |x: i64| x.wrapping_neg());

/// `-x`, where `-i64::MIN`, the one negation outside `i64`, fails its row.
pub fn neg_i64(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    let mut out = neg_wrapping(ctx, ins);
    if let Ok(a) = ins[0].i64() {
        let len = out.len();
        fail_overflowed(ctx, &mut out, len, |i| a.values[i] == i64::MIN);
    }
    out
}
unary_i64!(bitnot_i64, i64, i64_mut, DataType::I64, |x: i64| !x);
// A Duration is i64 microseconds in its own column; negating it through the I64 accessor would panic.
unary_i64!(neg_duration, duration, duration_mut, DataType::Duration, |x: i64| x
    .wrapping_neg());

/// F64 -> I64 rounding. Only registered for F64; a value outside `i64`'s range is null and an `Overflow`.
macro_rules! round_f64_to_i64 {
    ($name:ident, $f:ident) => {
        pub fn $name(ctx: &mut Ctx, ins: &[&Column]) -> Column {
            let len = ins[0].len();
            if let Some(e) = short_circuit_empty(ins, &DataType::I64, len) {
                return e;
            }
            let a = ins[0].f64().unwrap();
            let mut out = alloc_with_merged_validity(ctx, &DataType::I64, len, &[Some(a.validity), None]);
            {
                let o = out.i64_mut().unwrap();
                for i in 0..len {
                    o.values[i] = a.values[i].$f() as i64;
                }
            }
            fail_overflowed(ctx, &mut out, len, |i| !in_i64_range(a.values[i].$f()));
            out
        }
    };
}

round_f64_to_i64!(floor_f64, floor);
round_f64_to_i64!(ceil_f64, ceil);
round_f64_to_i64!(trunc_f64, trunc);
// Half away from zero, as `as i64` rounds.
round_f64_to_i64!(round_f64, round);

pub fn neg_f64(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    let len = ins[0].len();
    if let Some(e) = short_circuit_empty(ins, &DataType::F64, len) {
        return e;
    }
    let a = ins[0].f64().unwrap();
    let mut out = alloc_with_merged_validity(ctx, &DataType::F64, len, &[Some(a.validity), None]);
    {
        let o = out.f64_mut().unwrap();
        for i in 0..len {
            o.values[i] = -a.values[i];
        }
    }
    out
}

/// `abs(x)` keeps its input's type; `abs(i64::MIN)`, outside `i64`, fails its row, as `neg_i64` does.
pub fn abs_num(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    let len = ins[0].len();
    if ins[0].data_type() == DataType::I64 {
        if let Some(e) = short_circuit_empty(ins, &DataType::I64, len) {
            return e;
        }
        let a = ins[0].i64().unwrap();
        let mut out = alloc_with_merged_validity(ctx, &DataType::I64, len, &[Some(a.validity), None]);
        {
            let o = out.i64_mut().unwrap();
            for i in 0..len {
                o.values[i] = a.values[i].wrapping_abs();
            }
        }
        // `abs(i64::MIN)` is outside `i64`.
        fail_overflowed(ctx, &mut out, len, |i| a.values[i] == i64::MIN);
        out
    } else {
        if let Some(e) = short_circuit_empty(ins, &DataType::F64, len) {
            return e;
        }
        let a = ins[0].f64().unwrap();
        let mut out = alloc_with_merged_validity(ctx, &DataType::F64, len, &[Some(a.validity), None]);
        {
            let o = out.f64_mut().unwrap();
            for i in 0..len {
                o.values[i] = a.values[i].abs();
            }
        }
        out
    }
}

/// `min`/`max`: one pass, and `I64` only when every argument is. Two arguments, the common call, go through
/// `two_sides!`, so a constant is read from a register; three or more take each argument as a slice.
macro_rules! minmax {
    ($name:ident, $fi:expr, $ff:expr) => {
        pub fn $name(ctx: &mut Ctx, ins: &[&Column]) -> Column {
            fn run_i64<A: Operand<i64>, B: Operand<i64>>(
                ctx: &mut Ctx,
                a: A,
                b: B,
                va: Option<&Validity>,
                vb: Option<&Validity>,
                len: usize,
            ) -> Column {
                let mut out = alloc_with_merged_validity(ctx, &DataType::I64, len, &[va, vb]);
                {
                    let o = out.i64_mut().unwrap();
                    let f: fn(i64, i64) -> i64 = $fi;
                    for i in 0..len {
                        o.values[i] = f(a.at(i), b.at(i));
                    }
                }
                out
            }
            fn run_f64<A: Operand<f64>, B: Operand<f64>>(
                ctx: &mut Ctx,
                a: A,
                b: B,
                va: Option<&Validity>,
                vb: Option<&Validity>,
                len: usize,
            ) -> Column {
                let mut out = alloc_with_merged_validity(ctx, &DataType::F64, len, &[va, vb]);
                {
                    let o = out.f64_mut().unwrap();
                    let f: fn(f64, f64) -> f64 = $ff;
                    for i in 0..len {
                        o.values[i] = f(a.at(i), b.at(i));
                    }
                }
                out
            }
            let len = ins[0].len();
            let all_i64 = joins_to_i64(ins);
            let out_ty = if all_i64 { DataType::I64 } else { DataType::F64 };
            if let Some(e) = short_circuit_empty(ins, &out_ty, len) {
                return e;
            }
            if ins.len() != 2 {
                return minmax_n(ctx, ins, len, all_i64, $fi, $ff);
            }
            if all_i64 {
                two_sides!(ctx, ins, len, i64, i64, run_i64)
            } else {
                two_sides!(ctx, ins, len, f64, f64, run_f64)
            }
        }
    };
}

minmax!(min_num, i64::min, f64::min);
minmax!(max_num, i64::max, f64::max);

/// `min`/`max` over three or more arguments, each as a slice: a constant is filled once, outside the pool, and the
/// loop stays a slice loop.
fn minmax_n(
    ctx: &mut Ctx,
    ins: &[&Column],
    len: usize,
    all_i64: bool,
    fi: fn(i64, i64) -> i64,
    ff: fn(f64, f64) -> f64,
) -> Column {
    let bitmaps: Vec<_> = ins.iter().map(|c| c.validity()).collect();
    let out_ty = if all_i64 { DataType::I64 } else { DataType::F64 };
    let mut out = alloc_with_merged_validity(ctx, &out_ty, len, &bitmaps);
    if all_i64 {
        let sides: Vec<_> = ins.iter().map(|c| side::i64(c).unwrap()).collect();
        let cols: Vec<_> = sides.iter().map(|s| s.rows(len)).collect();
        let o = out.i64_mut().unwrap();
        for i in 0..len {
            o.values[i] = cols[1..].iter().fold(cols[0][i], |acc, c| fi(acc, c[i]));
        }
    } else {
        let sides: Vec<_> = ins.iter().map(|c| side::f64(c).unwrap()).collect();
        let cols: Vec<_> = sides.iter().map(|s| s.rows(len)).collect();
        let o = out.f64_mut().unwrap();
        for i in 0..len {
            o.values[i] = cols[1..].iter().fold(cols[0][i], |acc, c| ff(acc, c[i]));
        }
    }
    out
}

/// `clamp(value, lo, hi)`: the value pulled into `lo..=hi`. An inverted range satisfies nothing, so a row whose `lo`
/// exceeds its `hi` is null rather than silently answering with one end of it -- the bounds are usually constants, and
/// a mistake in them should be visible.
pub fn clamp_num(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    let len = ins[0].len();
    let all_i64 = joins_to_i64(ins);
    let out_ty = if all_i64 { DataType::I64 } else { DataType::F64 };
    if let Some(e) = short_circuit_empty(ins, &out_ty, len) {
        return e;
    }
    let bitmaps = [ins[0].validity(), ins[1].validity(), ins[2].validity()];
    let mut out = alloc_with_merged_validity(ctx, &out_ty, len, &bitmaps);
    // Each operand as a slice: a constant bound is filled once, outside the pool, and the loop stays a slice loop.
    if all_i64 {
        let sides = [
            side::i64(ins[0]).unwrap(),
            side::i64(ins[1]).unwrap(),
            side::i64(ins[2]).unwrap(),
        ];
        let [values, los, his] = sides.each_ref().map(|s| s.rows(len));
        let o = out.i64_mut().unwrap();
        for i in 0..len {
            if los[i] <= his[i] {
                o.values[i] = values[i].max(los[i]).min(his[i]);
            } else {
                o.values[i] = 0;
                o.validity.set_valid(i, false);
            }
        }
    } else {
        let sides = [
            side::f64(ins[0]).unwrap(),
            side::f64(ins[1]).unwrap(),
            side::f64(ins[2]).unwrap(),
        ];
        let [values, los, his] = sides.each_ref().map(|s| s.rows(len));
        let o = out.f64_mut().unwrap();
        for i in 0..len {
            if los[i] <= his[i] {
                o.values[i] = values[i].max(los[i]).min(his[i]);
            } else {
                o.values[i] = 0.0;
                o.validity.set_valid(i, false);
            }
        }
    }
    out
}

macro_rules! bitwise {
    ($name:ident, $f:expr) => {
        pub fn $name(ctx: &mut Ctx, ins: &[&Column]) -> Column {
            let len = ins[0].len();
            if let Some(e) = short_circuit_empty(ins, &DataType::I64, len) {
                return e;
            }
            let a = ins[0].i64().unwrap();
            let b = ins[1].i64().unwrap();
            let mut out = alloc_with_merged_validity(ctx, &DataType::I64, len, &[Some(a.validity), Some(b.validity)]);
            {
                let o = out.i64_mut().unwrap();
                let f = $f;
                for i in 0..len {
                    o.values[i] = f(a.values[i], b.values[i]);
                }
            }
            out
        }
    };
}

bitwise!(bitand_i64, |x: i64, y: i64| x & y);
bitwise!(bitor_i64, |x: i64, y: i64| x | y);
bitwise!(bitxor_i64, |x: i64, y: i64| x ^ y);
// Shift counts are masked to 0..64 rather than being UB.
bitwise!(shl_i64, |x: i64, y: i64| x.wrapping_shl(y as u32));
bitwise!(shr_i64, |x: i64, y: i64| x.wrapping_shr(y as u32));

/// `sign`: `-1`, `0` or `1` as `I64` for either numeric type. JS returns `NaN` for `NaN`; here that row is null.
pub fn sign_num(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    let len = ins[0].len();
    if let Some(e) = short_circuit_empty(ins, &DataType::I64, len) {
        return e;
    }
    if ins[0].data_type() == DataType::I64 {
        let a = ins[0].i64().unwrap();
        let mut out = alloc_with_merged_validity(ctx, &DataType::I64, len, &[Some(a.validity), None]);
        {
            let o = out.i64_mut().unwrap();
            for i in 0..len {
                o.values[i] = a.values[i].signum();
            }
        }
        out
    } else {
        let a = ins[0].f64().unwrap();
        let mut out = alloc_with_merged_validity(ctx, &DataType::I64, len, &[Some(a.validity), None]);
        {
            let o = out.i64_mut().unwrap();
            for i in 0..len {
                let x = a.values[i];
                if x.is_nan() {
                    o.values[i] = 0;
                    o.validity.set_valid(i, false);
                } else {
                    o.values[i] = if x > 0.0 {
                        1
                    } else if x < 0.0 {
                        -1
                    } else {
                        0
                    };
                }
            }
            o.validity.recount();
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::buffer::BufferPool;
    use crate::core::column::Scalar;
    use crate::core::error::ErrorSink;
    use crate::ops::OpKernel;

    /// An I64 column of `values`; a row in `nulls` is null.
    fn i64s(p: &mut BufferPool, values: &[i64], nulls: &[usize]) -> Column {
        let mut c = Column::alloc(p, &DataType::I64, values.len());
        let f = c.i64_mut().unwrap();
        f.values.copy_from_slice(values);
        for &i in nulls {
            f.validity.set_valid(i, false);
        }
        c
    }

    /// Each row of `k`'s I64 output, `None` where null, and `(row, kind)` per exception.
    fn run(p: &mut BufferPool, k: OpKernel, ins: &[&Column]) -> (Vec<Option<i64>>, Vec<(u32, ExceptionKind)>) {
        let mut errors = ErrorSink::new();
        let out = {
            let mut ctx = Ctx::new(p, &mut errors);
            k(&mut ctx, ins)
        };
        assert_eq!(out.data_type(), DataType::I64);
        let f = out.i64().unwrap();
        let rows = (0..out.len())
            .map(|i| f.validity.is_valid(i).then(|| f.values[i]))
            .collect();
        out.reclaim(p);
        (rows, errors.entries().iter().map(|a| (a.row, a.kind.clone())).collect())
    }

    #[test]
    fn an_overflowing_row_is_null_and_marked_and_its_neighbours_keep_their_values() {
        let mut p = BufferPool::new();
        type Case = (OpKernel, &'static [i64], &'static [i64], &'static [Option<i64>]);
        let cases: [Case; 3] = [
            (add_i64_i64, &[i64::MAX, 1, -5], &[1, 1, 3], &[None, Some(2), Some(-2)]),
            (sub_i64_i64, &[i64::MIN, 4, 0], &[1, 1, 3], &[None, Some(3), Some(-3)]),
            (mul_i64_i64, &[i64::MAX, 3, -2], &[2, 3, 4], &[None, Some(9), Some(-8)]),
        ];
        for (k, a, b, want) in cases {
            let (a, b) = (i64s(&mut p, a, &[]), i64s(&mut p, b, &[]));
            let (rows, exceptions) = run(&mut p, k, &[&a, &b]);
            assert_eq!(rows, want);
            assert_eq!(exceptions, vec![(0, ExceptionKind::Overflow)]);
            a.reclaim(&mut p);
            b.reclaim(&mut p);
        }
    }

    #[test]
    fn a_constant_operand_overflows_the_same_rows() {
        let mut p = BufferPool::new();
        let a = i64s(&mut p, &[i64::MAX, 1], &[]);
        let one = Column::constant(Scalar::I64(1), 2);
        let (rows, exceptions) = run(&mut p, add_i64_i64, &[&a, &one]);
        assert_eq!(rows, vec![None, Some(2)]);
        assert_eq!(exceptions, vec![(0, ExceptionKind::Overflow)]);
        a.reclaim(&mut p);
        one.reclaim(&mut p);
    }

    #[test]
    fn a_null_row_whose_bytes_would_overflow_is_not_a_failure() {
        let mut p = BufferPool::new();
        let a = i64s(&mut p, &[i64::MAX, 1], &[0]);
        let b = i64s(&mut p, &[1, 1], &[]);
        let (rows, exceptions) = run(&mut p, add_i64_i64, &[&a, &b]);
        assert_eq!(rows, vec![None, Some(2)]);
        assert!(exceptions.is_empty(), "{exceptions:?}");
        a.reclaim(&mut p);
        b.reclaim(&mut p);
    }

    #[test]
    fn idiv_truncates_toward_zero_and_a_zero_divisor_is_null() {
        let mut p = BufferPool::new();
        let a = i64s(&mut p, &[7, -7, 1], &[]);
        let b = i64s(&mut p, &[2, 2, 0], &[]);
        let (rows, exceptions) = run(&mut p, idiv_i64_i64, &[&a, &b]);
        assert_eq!(rows, vec![Some(3), Some(-3), None]);
        assert_eq!(exceptions, vec![(2, ExceptionKind::DivideByZero)]);
        a.reclaim(&mut p);
        b.reclaim(&mut p);
    }

    /// The three results outside `i64` that are not a sum, difference or product: `MIN / -1`, `-MIN` and
    /// `abs(MIN)`. Each fails its row; the row beside it keeps its value.
    #[test]
    fn idiv_neg_and_abs_of_min_fail_their_row() {
        let mut p = BufferPool::new();
        let a = i64s(&mut p, &[i64::MIN, -6], &[]);
        let b = i64s(&mut p, &[-1, -1], &[]);
        let overflow = vec![(0, ExceptionKind::Overflow)];
        assert_eq!(
            run(&mut p, idiv_i64_i64, &[&a, &b]),
            (vec![None, Some(6)], overflow.clone())
        );
        assert_eq!(run(&mut p, neg_i64, &[&a]), (vec![None, Some(6)], overflow.clone()));
        assert_eq!(run(&mut p, abs_num, &[&a]), (vec![None, Some(6)], overflow));
        a.reclaim(&mut p);
        b.reclaim(&mut p);
    }
}
