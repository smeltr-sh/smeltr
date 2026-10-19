//! Kernel behavior tests.
//!
//! Separate from `table.rs`'s tests, which only check that the type table is
//! internally consistent. These check that the kernels compute the right thing.

use smeltr::core::bits;
use smeltr::core::buffer::BufferPool;
use smeltr::core::column::{Column, DataType, Scalar, StrBuilder, Validity, VarType};
use smeltr::core::ctx::{CompiledPattern, Ctx};
use smeltr::core::error::{ErrorSink, ExceptionKind};
use smeltr::ops::stdfuncs::{regex_replace, regex_test};
use smeltr::ops::{
    BinOp, OpKernel, UnOp, arith, arrayops, cmp, convert, datetime, logic, netops, strops, table, timeparse,
};

fn i64_col(p: &mut BufferPool, vals: &[i64]) -> Column {
    let mut c = Column::alloc(p, &DataType::I64, vals.len());
    c.i64_mut().unwrap().values.copy_from_slice(vals);
    c
}

fn f64_col(p: &mut BufferPool, vals: &[f64]) -> Column {
    let mut c = Column::alloc(p, &DataType::F64, vals.len());
    c.f64_mut().unwrap().values.copy_from_slice(vals);
    c
}

/// `None` means null.
fn opt_i64_col(p: &mut BufferPool, vals: &[Option<i64>]) -> Column {
    let mut c = Column::alloc(p, &DataType::I64, vals.len());
    {
        let f = c.i64_mut().unwrap();
        for (i, v) in vals.iter().enumerate() {
            match v {
                Some(x) => f.values[i] = *x,
                None => {
                    f.values[i] = 0;
                    f.validity.set_valid(i, false);
                }
            }
        }
    }
    c
}

fn bool_col(p: &mut BufferPool, vals: &[Option<bool>]) -> Column {
    let mut c = Column::alloc(p, &DataType::Bool, vals.len());
    {
        let b = c.bool_mut().unwrap();
        for (i, v) in vals.iter().enumerate() {
            match v {
                Some(x) => {
                    bits::set_row(b.values, i, *x);
                }
                None => {
                    bits::set_row(b.values, i, false);
                    b.validity.set_valid(i, false);
                }
            }
        }
    }
    c
}

fn str_col(p: &mut BufferPool, vals: &[&str]) -> Column {
    let mut b = StrBuilder::new(p, VarType::Str, vals.len());
    let total: usize = vals.iter().map(|s| s.len()).sum();
    b.reserve(p, total);
    for s in vals {
        b.push(s.as_bytes());
    }
    b.finish()
}

fn bytes_col(p: &mut BufferPool, vals: &[&[u8]]) -> Column {
    let mut b = StrBuilder::new(p, VarType::Bytes, vals.len());
    let total: usize = vals.iter().map(|s| s.len()).sum();
    b.reserve(p, total);
    for s in vals {
        b.push(s);
    }
    b.finish()
}

/// `None` means null.
fn opt_f64_col(p: &mut BufferPool, vals: &[Option<f64>]) -> Column {
    let mut c = Column::alloc(p, &DataType::F64, vals.len());
    {
        let f = c.f64_mut().unwrap();
        for (i, v) in vals.iter().enumerate() {
            match v {
                Some(x) => f.values[i] = *x,
                None => f.validity.set_valid(i, false),
            }
        }
    }
    c
}

/// `None` means null.
fn opt_str_col(p: &mut BufferPool, vals: &[Option<&str>]) -> Column {
    let mut b = StrBuilder::new(p, VarType::Str, vals.len());
    let total: usize = vals.iter().flatten().map(|s| s.len()).sum();
    b.reserve(p, total);
    for v in vals {
        match v {
            Some(s) => b.push(s.as_bytes()),
            None => b.push_null(),
        }
    }
    b.finish()
}

fn read_strs(c: &Column) -> Vec<Option<String>> {
    let v = c.var().unwrap();
    (0..c.len())
        .map(|i| {
            v.validity
                .is_valid(i)
                .then(|| String::from_utf8_lossy(v.get(i)).into_owned())
        })
        .collect()
}

fn read_i64s(c: &Column) -> Vec<Option<i64>> {
    let f = c.i64().unwrap();
    (0..c.len())
        .map(|i| f.validity.is_valid(i).then_some(f.values[i]))
        .collect()
}

fn read_f64s(c: &Column) -> Vec<Option<f64>> {
    let f = c.f64().unwrap();
    (0..c.len())
        .map(|i| f.validity.is_valid(i).then_some(f.values[i]))
        .collect()
}

fn pattern(src: &str, case_insensitive: bool) -> CompiledPattern {
    let regex = regex::RegexBuilder::new(src)
        .case_insensitive(case_insensitive)
        .build()
        .unwrap();
    CompiledPattern { regex }
}

fn read_bools(c: &Column) -> Vec<Option<bool>> {
    let b = c.bool().unwrap();
    (0..c.len())
        .map(|i| {
            if b.validity.is_valid(i) {
                Some(bits::get_row(b.values, i))
            } else {
                None
            }
        })
        .collect()
}

/// Run a kernel, returning the output column and any exceptions raised.
fn run(p: &mut BufferPool, k: fn(&mut Ctx, &[&Column]) -> Column, ins: &[&Column]) -> (Column, Vec<ExceptionKind>) {
    let mut errors = ErrorSink::new();
    let out = {
        let mut ctx = Ctx::new(p, &mut errors);
        k(&mut ctx, ins)
    };
    (out, errors.entries().iter().map(|a| a.kind.clone()).collect())
}

// ---------------------------------------------------------------------------
// Arithmetic
// ---------------------------------------------------------------------------

#[test]
fn add_i64_basic() {
    let mut p = BufferPool::new();
    let a = i64_col(&mut p, &[1, 2, 3]);
    let b = i64_col(&mut p, &[10, 20, 30]);
    let (out, ann) = run(&mut p, arith::add_i64_i64, &[&a, &b]);
    assert_eq!(out.i64().unwrap().values, &[11, 22, 33]);
    assert!(ann.is_empty());
    for c in [out, a, b] {
        c.reclaim(&mut p);
    }
}

#[test]
fn null_propagates_through_arithmetic() {
    let mut p = BufferPool::new();
    let a = opt_i64_col(&mut p, &[Some(1), None, Some(3)]);
    let b = i64_col(&mut p, &[10, 20, 30]);
    let (out, _) = run(&mut p, arith::add_i64_i64, &[&a, &b]);
    let f = out.i64().unwrap();
    assert!(f.validity.is_valid(0));
    assert!(!f.validity.is_valid(1), "null in, null out");
    assert!(f.validity.is_valid(2));
    assert_eq!(f.validity.valid_count(), 2);
    for c in [out, a, b] {
        c.reclaim(&mut p);
    }
}

/// An `i64` result that overflows is a failure on its row: that row is null and marked `Overflow`. The column
/// stays `I64`, and its other rows keep their values.
#[test]
fn an_overflowing_row_is_null_and_marked_while_its_neighbours_keep_their_values() {
    let mut p = BufferPool::new();
    let a = i64_col(&mut p, &[1, i64::MAX, 3]);
    let b = i64_col(&mut p, &[1, 1, 1]);
    let (out, ann) = run(&mut p, arith::add_i64_i64, &[&a, &b]);

    assert_eq!(out.data_type(), DataType::I64);
    assert_eq!(read_i64s(&out), vec![Some(2), None, Some(4)]);
    assert_eq!(ann, vec![ExceptionKind::Overflow]);
    for c in [out, a, b] {
        c.reclaim(&mut p);
    }
}

#[test]
fn no_overflow_means_no_exception() {
    let mut p = BufferPool::new();
    let a = i64_col(&mut p, &[i64::MAX, i64::MIN]);
    let b = i64_col(&mut p, &[0, 0]);
    let (out, ann) = run(&mut p, arith::add_i64_i64, &[&a, &b]);
    assert_eq!(out.data_type(), DataType::I64, "must stay integral");
    assert!(ann.is_empty());
    for c in [out, a, b] {
        c.reclaim(&mut p);
    }
}

/// A row null from its inputs stays null and is not a failure; an overflowing row beside it fails on its own; a
/// row in range keeps its value.
#[test]
fn an_overflowing_row_fails_alone_beside_a_null_row() {
    let mut p = BufferPool::new();
    let a = opt_i64_col(&mut p, &[Some(i64::MAX), None, Some(5)]);
    let b = i64_col(&mut p, &[1, 1, 1]);
    let (out, ann) = run(&mut p, arith::add_i64_i64, &[&a, &b]);
    assert_eq!(out.data_type(), DataType::I64);
    assert_eq!(read_i64s(&out), vec![None, None, Some(6)]);
    assert_eq!(ann, vec![ExceptionKind::Overflow], "the null row is not a failure");
    for c in [out, a, b] {
        c.reclaim(&mut p);
    }
}

#[test]
fn subtraction_overflow_is_null_and_marked() {
    let mut p = BufferPool::new();
    let a = i64_col(&mut p, &[i64::MIN, 0]);
    let b = i64_col(&mut p, &[1, 1]);
    let (out, ann) = run(&mut p, arith::sub_i64_i64, &[&a, &b]);
    assert_eq!(out.data_type(), DataType::I64);
    assert_eq!(read_i64s(&out), vec![None, Some(-1)]);
    assert_eq!(ann, vec![ExceptionKind::Overflow]);
    for c in [out, a, b] {
        c.reclaim(&mut p);
    }
}

/// A product that wraps `i64` yet fits `i128` is still an overflow.
#[test]
fn multiplication_overflow_is_null_and_marked() {
    let mut p = BufferPool::new();
    let a = i64_col(&mut p, &[1 << 40, 1 << 20]);
    let b = i64_col(&mut p, &[1 << 40, 1 << 20]);
    let (out, ann) = run(&mut p, arith::mul_i64_i64, &[&a, &b]);
    assert_eq!(out.data_type(), DataType::I64);
    assert_eq!(read_i64s(&out), vec![None, Some(1 << 40)]);
    assert_eq!(ann, vec![ExceptionKind::Overflow]);
    for c in [out, a, b] {
        c.reclaim(&mut p);
    }
}

#[test]
fn integer_division_yields_float() {
    let mut p = BufferPool::new();
    let a = i64_col(&mut p, &[7, 10]);
    let b = i64_col(&mut p, &[2, 4]);
    let (out, _) = run(&mut p, arith::div_i64_i64, &[&a, &b]);
    assert_eq!(out.f64().unwrap().values, &[3.5, 2.5], "not truncated");
    for c in [out, a, b] {
        c.reclaim(&mut p);
    }
}

#[test]
fn divide_by_zero_is_null_not_infinity() {
    let mut p = BufferPool::new();
    let a = i64_col(&mut p, &[1, 2]);
    let b = i64_col(&mut p, &[0, 2]);
    let (out, ann) = run(&mut p, arith::div_i64_i64, &[&a, &b]);
    let f = out.f64().unwrap();
    assert!(!f.validity.is_valid(0));
    assert!(f.validity.is_valid(1));
    assert!(ann.iter().any(|k| matches!(k, ExceptionKind::DivideByZero)));
    for c in [out, a, b] {
        c.reclaim(&mut p);
    }
}

#[test]
fn modulo_by_zero_is_null() {
    let mut p = BufferPool::new();
    let a = i64_col(&mut p, &[7]);
    let b = i64_col(&mut p, &[0]);
    let (out, ann) = run(&mut p, arith::rem_i64_i64, &[&a, &b]);
    assert!(!out.i64().unwrap().validity.is_valid(0));
    assert!(!ann.is_empty());
    for c in [out, a, b] {
        c.reclaim(&mut p);
    }
}

#[test]
fn min_rem_negative_one_does_not_trap() {
    // i64::MIN % -1 traps on x86 idiv. wrapping_rem avoids it.
    let mut p = BufferPool::new();
    let a = i64_col(&mut p, &[i64::MIN]);
    let b = i64_col(&mut p, &[-1]);
    let (out, _) = run(&mut p, arith::rem_i64_i64, &[&a, &b]);
    assert_eq!(out.i64().unwrap().values[0], 0);
    for c in [out, a, b] {
        c.reclaim(&mut p);
    }
}

#[test]
fn empty_input_short_circuits_to_empty_output() {
    let mut p = BufferPool::new();
    let a = Column::empty(DataType::I64, 4);
    let b = i64_col(&mut p, &[1, 2, 3, 4]);
    let (out, _) = run(&mut p, arith::add_i64_i64, &[&a, &b]);
    assert!(out.is_unbound(), "no buffers should be allocated at all");
    assert_eq!(out.len(), 4);
    assert!(out.has_nulls());
    for c in [out, a, b] {
        c.reclaim(&mut p);
    }
}

// ---------------------------------------------------------------------------
// Temporal
// ---------------------------------------------------------------------------

#[test]
fn datetime_minus_datetime_is_a_duration_in_micros() {
    let mut p = BufferPool::new();
    let mut a = Column::alloc(&mut p, &DataType::DateTime, 1);
    let mut b = Column::alloc(&mut p, &DataType::DateTime, 1);
    a.datetime_mut().unwrap().values[0] = 5_000_000; // t = 5s
    b.datetime_mut().unwrap().values[0] = 2_000_000; // t = 2s
    let (out, _) = run(&mut p, arith::sub_dt_dt, &[&a, &b]);
    assert_eq!(out.data_type(), DataType::Duration);
    assert_eq!(out.duration().unwrap().values[0], 3_000_000, "3s in micros");
    for c in [out, a, b] {
        c.reclaim(&mut p);
    }
}

fn datetime_col(p: &mut BufferPool, vals: &[i64]) -> Column {
    let mut c = Column::alloc(p, &DataType::DateTime, vals.len());
    c.datetime_mut().unwrap().values.copy_from_slice(vals);
    c
}

fn duration_col(p: &mut BufferPool, vals: &[i64]) -> Column {
    let mut c = Column::alloc(p, &DataType::Duration, vals.len());
    c.duration_mut().unwrap().values.copy_from_slice(vals);
    c
}

#[test]
fn add_datetime_and_duration_advances_time() {
    let mut p = BufferPool::new();
    let t = datetime_col(&mut p, &[5_000_000]);
    let d = duration_col(&mut p, &[2_000_000]);
    let (out1, _) = run(&mut p, arith::add_dt_dur, &[&t, &d]);
    assert_eq!(out1.data_type(), DataType::DateTime);
    assert_eq!(out1.datetime().unwrap().values[0], 7_000_000);
    let (out2, _) = run(&mut p, arith::add_dur_dt, &[&d, &t]);
    assert_eq!(out2.data_type(), DataType::DateTime);
    assert_eq!(out2.datetime().unwrap().values[0], 7_000_000, "commutative");
    for c in [out1, out2, t, d] {
        c.reclaim(&mut p);
    }
}

#[test]
fn subtracting_duration_from_datetime_moves_time_back() {
    let mut p = BufferPool::new();
    let t = datetime_col(&mut p, &[5_000_000]);
    let d = duration_col(&mut p, &[2_000_000]);
    let (out, _) = run(&mut p, arith::sub_dt_dur, &[&t, &d]);
    assert_eq!(out.data_type(), DataType::DateTime);
    assert_eq!(out.datetime().unwrap().values[0], 3_000_000);
    for c in [out, t, d] {
        c.reclaim(&mut p);
    }
}

#[test]
fn duration_plus_duration_sums_micros() {
    let mut p = BufferPool::new();
    let a = duration_col(&mut p, &[1_000_000]);
    let b = duration_col(&mut p, &[2_000_000]);
    let (out, _) = run(&mut p, arith::add_dur_dur, &[&a, &b]);
    assert_eq!(out.data_type(), DataType::Duration);
    assert_eq!(out.duration().unwrap().values[0], 3_000_000);
    for c in [out, a, b] {
        c.reclaim(&mut p);
    }
}

#[test]
fn duration_minus_duration_subtracts_micros() {
    let mut p = BufferPool::new();
    let a = duration_col(&mut p, &[3_000_000]);
    let b = duration_col(&mut p, &[1_000_000]);
    let (out, _) = run(&mut p, arith::sub_dur_dur, &[&a, &b]);
    assert_eq!(out.duration().unwrap().values[0], 2_000_000);
    for c in [out, a, b] {
        c.reclaim(&mut p);
    }
}

#[test]
fn duration_modulo_duration_wraps() {
    let mut p = BufferPool::new();
    let a = duration_col(&mut p, &[7_000_000]);
    let b = duration_col(&mut p, &[2_000_000]);
    let (out, _) = run(&mut p, arith::rem_dur_dur, &[&a, &b]);
    assert_eq!(out.data_type(), DataType::Duration);
    assert_eq!(out.duration().unwrap().values[0], 1_000_000);
    for c in [out, a, b] {
        c.reclaim(&mut p);
    }
}

#[test]
fn duration_modulo_by_zero_duration_is_null() {
    let mut p = BufferPool::new();
    let a = duration_col(&mut p, &[7_000_000]);
    let b = duration_col(&mut p, &[0]);
    let (out, ann) = run(&mut p, arith::rem_dur_dur, &[&a, &b]);
    assert!(!out.duration().unwrap().validity.is_valid(0));
    assert!(ann.iter().any(|k| matches!(k, ExceptionKind::DivideByZero)));
    for c in [out, a, b] {
        c.reclaim(&mut p);
    }
}

#[test]
fn float_modulo_by_zero_is_null() {
    let mut p = BufferPool::new();
    let a = f64_col(&mut p, &[5.5]);
    let b = f64_col(&mut p, &[0.0]);
    let (out, ann) = run(&mut p, arith::rem_f64_f64, &[&a, &b]);
    assert!(!out.f64().unwrap().validity.is_valid(0));
    assert!(ann.iter().any(|k| matches!(k, ExceptionKind::DivideByZero)));
    for c in [out, a, b] {
        c.reclaim(&mut p);
    }
}

#[test]
fn a_null_operand_beside_a_zero_divisor_throws_nothing() {
    let mut p = BufferPool::new();
    for (a, b) in [(vec![Some(5)], vec![None]), (vec![None], vec![Some(0)])] {
        let a = opt_i64_col(&mut p, &a);
        let b = opt_i64_col(&mut p, &b);
        let (out, ann) = run(&mut p, arith::div_i64_i64, &[&a, &b]);
        assert!(!out.f64().unwrap().validity.is_valid(0));
        assert!(ann.is_empty(), "{ann:?}");
        for c in [out, a, b] {
            c.reclaim(&mut p);
        }
    }
}

#[test]
fn duration_divided_by_duration_is_a_unitless_ratio() {
    let mut p = BufferPool::new();
    let a = duration_col(&mut p, &[6_000_000]);
    let b = duration_col(&mut p, &[3_000_000]);
    let (out, _) = run(&mut p, arith::div_dur_dur, &[&a, &b]);
    assert_eq!(out.data_type(), DataType::F64);
    assert_eq!(out.f64().unwrap().values[0], 2.0);
    for c in [out, a, b] {
        c.reclaim(&mut p);
    }
}

#[test]
fn dividing_duration_by_zero_duration_is_null() {
    let mut p = BufferPool::new();
    let a = duration_col(&mut p, &[6_000_000]);
    let b = duration_col(&mut p, &[0]);
    let (out, ann) = run(&mut p, arith::div_dur_dur, &[&a, &b]);
    assert!(!out.f64().unwrap().validity.is_valid(0));
    assert!(ann.iter().any(|k| matches!(k, ExceptionKind::DivideByZero)));
    for c in [out, a, b] {
        c.reclaim(&mut p);
    }
}

#[test]
fn duration_scaled_by_integer() {
    let mut p = BufferPool::new();
    let d = duration_col(&mut p, &[1_000_000]);
    let n = i64_col(&mut p, &[3]);
    let (out1, _) = run(&mut p, arith::mul_dur_i64, &[&d, &n]);
    assert_eq!(out1.data_type(), DataType::Duration);
    assert_eq!(out1.duration().unwrap().values[0], 3_000_000);
    let (out2, _) = run(&mut p, arith::mul_i64_dur, &[&n, &d]);
    assert_eq!(out2.duration().unwrap().values[0], 3_000_000, "commutative");
    for c in [out1, out2, d, n] {
        c.reclaim(&mut p);
    }
}

#[test]
fn duration_scaled_by_float() {
    let mut p = BufferPool::new();
    let d = duration_col(&mut p, &[1_000_000]);
    let f = f64_col(&mut p, &[1.5]);
    let (out1, _) = run(&mut p, arith::mul_dur_f64, &[&d, &f]);
    assert_eq!(out1.duration().unwrap().values[0], 1_500_000);
    let (out2, _) = run(&mut p, arith::mul_f64_dur, &[&f, &d]);
    assert_eq!(out2.duration().unwrap().values[0], 1_500_000, "commutative");
    for c in [out1, out2, d, f] {
        c.reclaim(&mut p);
    }
}

#[test]
fn duration_divided_by_integer() {
    let mut p = BufferPool::new();
    let d = duration_col(&mut p, &[9_000_000]);
    let n = i64_col(&mut p, &[3]);
    let (out, _) = run(&mut p, arith::div_dur_i64, &[&d, &n]);
    assert_eq!(out.data_type(), DataType::Duration);
    assert_eq!(out.duration().unwrap().values[0], 3_000_000);
    for c in [out, d, n] {
        c.reclaim(&mut p);
    }
}

#[test]
fn dividing_duration_by_zero_integer_is_null() {
    let mut p = BufferPool::new();
    let d = duration_col(&mut p, &[9_000_000]);
    let n = i64_col(&mut p, &[0]);
    let (out, ann) = run(&mut p, arith::div_dur_i64, &[&d, &n]);
    assert!(!out.duration().unwrap().validity.is_valid(0));
    assert!(ann.iter().any(|k| matches!(k, ExceptionKind::DivideByZero)));
    for c in [out, d, n] {
        c.reclaim(&mut p);
    }
}

#[test]
fn duration_divided_by_float() {
    let mut p = BufferPool::new();
    let d = duration_col(&mut p, &[9_000_000]);
    let f = f64_col(&mut p, &[3.0]);
    let (out, _) = run(&mut p, arith::div_dur_f64, &[&d, &f]);
    assert_eq!(out.duration().unwrap().values[0], 3_000_000);
    for c in [out, d, f] {
        c.reclaim(&mut p);
    }
}

#[test]
fn dividing_duration_by_zero_float_is_null() {
    let mut p = BufferPool::new();
    let d = duration_col(&mut p, &[9_000_000]);
    let f = f64_col(&mut p, &[0.0]);
    let (out, ann) = run(&mut p, arith::div_dur_f64, &[&d, &f]);
    assert!(!out.duration().unwrap().validity.is_valid(0));
    assert!(ann.iter().any(|k| matches!(k, ExceptionKind::DivideByZero)));
    for c in [out, d, f] {
        c.reclaim(&mut p);
    }
}

// ---------------------------------------------------------------------------
// Comparison
// ---------------------------------------------------------------------------

#[test]
fn comparison_produces_a_bitmap() {
    let mut p = BufferPool::new();
    let a = i64_col(&mut p, &[1, 5, 3]);
    let b = i64_col(&mut p, &[2, 2, 3]);
    let (out, _) = run(&mut p, cmp::lt_i64_i64, &[&a, &b]);
    assert_eq!(out.data_type(), DataType::Bool);
    assert_eq!(read_bools(&out), vec![Some(true), Some(false), Some(false)]);
    for c in [out, a, b] {
        c.reclaim(&mut p);
    }
}

#[test]
fn null_compared_to_anything_is_null_not_false() {
    let mut p = BufferPool::new();
    let a = opt_i64_col(&mut p, &[None, Some(1)]);
    let b = opt_i64_col(&mut p, &[None, Some(1)]);
    let (out, _) = run(&mut p, cmp::eq_i64_i64, &[&a, &b]);
    assert_eq!(
        read_bools(&out),
        vec![None, Some(true)],
        "null == null is null, per SQL semantics"
    );
    for c in [out, a, b] {
        c.reclaim(&mut p);
    }
}

#[test]
fn comparison_across_word_boundary() {
    let mut p = BufferPool::new();
    let vals: Vec<i64> = (0..200).collect();
    let a = i64_col(&mut p, &vals);
    let b = i64_col(&mut p, &vec![100i64; 200]);
    let (out, _) = run(&mut p, cmp::lt_i64_i64, &[&a, &b]);
    let bo = out.bool().unwrap();
    assert_eq!(bits::popcount(bo.values), 100);
    assert!(bits::get_row(bo.values, 99));
    assert!(!bits::get_row(bo.values, 100));
    for c in [out, a, b] {
        c.reclaim(&mut p);
    }
}

#[test]
fn string_comparison_is_bytewise() {
    let mut p = BufferPool::new();
    let a = str_col(&mut p, &["apple", "THREAT", "zebra"]);
    let b = str_col(&mut p, &["apple", "TRAFFIC", "aardvark"]);
    let (out, _) = run(&mut p, cmp::eq_var_var, &[&a, &b]);
    assert_eq!(read_bools(&out), vec![Some(true), Some(false), Some(false)]);
    for c in [out, a, b] {
        c.reclaim(&mut p);
    }
}

#[test]
fn bool_equality_is_xnor() {
    let mut p = BufferPool::new();
    let a = bool_col(&mut p, &[Some(true), Some(true), Some(false)]);
    let b = bool_col(&mut p, &[Some(true), Some(false), Some(false)]);
    let (out, _) = run(&mut p, cmp::eq_bool_bool, &[&a, &b]);
    assert_eq!(read_bools(&out), vec![Some(true), Some(false), Some(true)]);
    for c in [out, a, b] {
        c.reclaim(&mut p);
    }
}

// ---------------------------------------------------------------------------
// Logic: null reads as false
// ---------------------------------------------------------------------------

#[test]
fn and_reads_null_as_false() {
    let mut p = BufferPool::new();
    //                    T&T   T&F   F&N    T&N   N&N
    let a = bool_col(&mut p, &[Some(true), Some(true), Some(false), Some(true), None]);
    let b = bool_col(&mut p, &[Some(true), Some(false), None, None, None]);
    let (out, _) = run(&mut p, logic::and_n, &[&a, &b]);
    assert_eq!(
        read_bools(&out),
        vec![Some(true), Some(false), Some(false), Some(false), Some(false)]
    );
    for c in [out, a, b] {
        c.reclaim(&mut p);
    }
}

#[test]
fn or_reads_null_as_false() {
    let mut p = BufferPool::new();
    let a = bool_col(&mut p, &[Some(false), Some(true), Some(false), None]);
    let b = bool_col(&mut p, &[Some(false), None, None, None]);
    let (out, _) = run(&mut p, logic::or_n, &[&a, &b]);
    assert_eq!(
        read_bools(&out),
        vec![Some(false), Some(true), Some(false), Some(false)]
    );
    for c in [out, a, b] {
        c.reclaim(&mut p);
    }
}

#[test]
fn and_n_is_genuinely_n_ary() {
    let mut p = BufferPool::new();
    let a = bool_col(&mut p, &[Some(true), Some(true)]);
    let b = bool_col(&mut p, &[Some(true), Some(true)]);
    let c = bool_col(&mut p, &[Some(true), Some(false)]);
    let d = bool_col(&mut p, &[Some(true), Some(true)]);
    let (out, _) = run(&mut p, logic::and_n, &[&a, &b, &c, &d]);
    assert_eq!(read_bools(&out), vec![Some(true), Some(false)]);
    for x in [out, a, b, c, d] {
        x.reclaim(&mut p);
    }
}

#[test]
fn an_unbound_operand_reads_as_false() {
    // An unresolved field is null on every row. The logical kernels must NOT use the short-circuit-on-empty prologue.
    let mut p = BufferPool::new();
    let a = bool_col(&mut p, &[Some(false), Some(true)]);
    let b = Column::empty(DataType::Bool, 2);
    let (and, _) = run(&mut p, logic::and_n, &[&a, &b]);
    assert_eq!(read_bools(&and), vec![Some(false), Some(false)]);
    let (or, _) = run(&mut p, logic::or_n, &[&a, &b]);
    assert_eq!(read_bools(&or), vec![Some(false), Some(true)]);
    for c in [and, or, a, b] {
        c.reclaim(&mut p);
    }
}

#[test]
fn and_and_or_read_a_literal_as_one_value() {
    // A literal operand meets the kernel as a constant column. `true` drops out of `&&` and decides `||`, `false`
    // the reverse, on either side, and the answer is the truth table's.
    let mut p = BufferPool::new();
    let a = bool_col(&mut p, &[Some(true), Some(false), None]);
    let same = vec![Some(true), Some(false), Some(false)];
    let cases: [(OpKernel, bool, Vec<Option<bool>>); 4] = [
        (logic::and_n, true, same.clone()),
        (logic::and_n, false, vec![Some(false); 3]),
        (logic::or_n, true, vec![Some(true); 3]),
        (logic::or_n, false, same),
    ];
    for (k, value, want) in cases {
        let one = Column::constant(Scalar::Bool(value), 3);
        for ins in [[&one, &a], [&a, &one]] {
            let (out, _) = run(&mut p, k, &ins);
            assert_eq!(read_bools(&out), want, "literal {value}");
            out.reclaim(&mut p);
        }
        one.reclaim(&mut p);
    }

    // Two literals decide every row between them. An unbound operand is false beside either.
    let t = Column::constant(Scalar::Bool(true), 2);
    let f = Column::constant(Scalar::Bool(false), 2);
    let unbound = Column::empty(DataType::Bool, 2);
    let (both, _) = run(&mut p, logic::and_n, &[&t, &f]);
    assert_eq!(read_bools(&both), vec![Some(false); 2]);
    let (decided, _) = run(&mut p, logic::or_n, &[&unbound, &t]);
    assert_eq!(read_bools(&decided), vec![Some(true); 2]);
    let (open, _) = run(&mut p, logic::and_n, &[&t, &unbound]);
    assert_eq!(read_bools(&open), vec![Some(false); 2]);
    for c in [both, decided, open, a, t, f, unbound] {
        c.reclaim(&mut p);
    }
}

#[test]
fn not_of_null_is_true() {
    let mut p = BufferPool::new();
    let a = bool_col(&mut p, &[Some(true), Some(false), None]);
    let (out, _) = run(&mut p, logic::not_bool, &[&a]);
    assert_eq!(read_bools(&out), vec![Some(false), Some(true), Some(true)]);
    let unbound = Column::empty(DataType::Bool, 3);
    let (all, _) = run(&mut p, logic::not_bool, &[&unbound]);
    assert_eq!(read_bools(&all), vec![Some(true); 3]);
    for c in [out, a, all, unbound] {
        c.reclaim(&mut p);
    }
}

#[test]
fn is_null_is_never_itself_null() {
    let mut p = BufferPool::new();
    let a = opt_i64_col(&mut p, &[Some(1), None]);
    let (out, _) = run(&mut p, logic::is_null, &[&a]);
    assert_eq!(read_bools(&out), vec![Some(false), Some(true)]);
    for c in [out, a] {
        c.reclaim(&mut p);
    }
}

// ---------------------------------------------------------------------------
// Coalesce
// ---------------------------------------------------------------------------

#[test]
fn coalesce_takes_first_non_null_per_row() {
    let mut p = BufferPool::new();
    let a = opt_i64_col(&mut p, &[Some(1), None, None]);
    let b = opt_i64_col(&mut p, &[Some(9), Some(2), None]);
    let c = opt_i64_col(&mut p, &[Some(9), Some(9), Some(3)]);
    let (out, _) = run(&mut p, logic::coalesce_n, &[&a, &b, &c]);
    let f = out.i64().unwrap();
    assert_eq!(f.values, &[1, 2, 3]);
    assert_eq!(f.validity.valid_count(), 3);
    for x in [out, a, b, c] {
        x.reclaim(&mut p);
    }
}

#[test]
fn coalesce_tests_null_not_truthiness() {
    // The `??` vs `||` distinction: 0 is not null, so it wins.
    let mut p = BufferPool::new();
    let a = opt_i64_col(&mut p, &[Some(0)]);
    let b = opt_i64_col(&mut p, &[Some(5)]);
    let (out, _) = run(&mut p, logic::coalesce_n, &[&a, &b]);
    assert_eq!(out.i64().unwrap().values, &[0]);
    for c in [out, a, b] {
        c.reclaim(&mut p);
    }
}

#[test]
fn coalesce_leaves_a_row_null_when_every_operand_is_null() {
    let mut p = BufferPool::new();
    let a = opt_i64_col(&mut p, &[None]);
    let b = opt_i64_col(&mut p, &[None]);
    let (out, _) = run(&mut p, logic::coalesce_n, &[&a, &b]);
    assert!(!out.i64().unwrap().validity.is_valid(0));
    for c in [out, a, b] {
        c.reclaim(&mut p);
    }
}

#[test]
fn coalesce_skips_unbound_operands() {
    let mut p = BufferPool::new();
    let a = Column::empty(DataType::I64, 2);
    let b = opt_i64_col(&mut p, &[Some(7), Some(8)]);
    let (out, _) = run(&mut p, logic::coalesce_n, &[&a, &b]);
    assert_eq!(out.i64().unwrap().values, &[7, 8]);
    for c in [out, a, b] {
        c.reclaim(&mut p);
    }
}

#[test]
fn coalesce_takes_first_non_null_string_per_row() {
    // Varlen columns can't be patched in place, so `coalesce_n` rebuilds them
    // via `StrBuilder` — this is the regression test for that path.
    let mut p = BufferPool::new();
    let mut a = str_col(&mut p, &["x", "y", "z"]);
    a.validity_mut().unwrap().set_valid(1, false);
    a.validity_mut().unwrap().set_valid(2, false);
    let b = str_col(&mut p, &["9", "2", "9"]);
    let (out, _) = run(&mut p, logic::coalesce_n, &[&a, &b]);
    let v = out.var().unwrap();
    assert_eq!(v.get(0), b"x");
    assert_eq!(v.get(1), b"2");
    assert_eq!(v.get(2), b"9");
    assert_eq!(v.validity.valid_count(), 3);
    for c in [out, a, b] {
        c.reclaim(&mut p);
    }
}

#[test]
fn coalesce_over_strings_leaves_a_row_null_when_every_operand_is_null() {
    let mut p = BufferPool::new();
    let mut a = str_col(&mut p, &["x"]);
    a.validity_mut().unwrap().set_valid(0, false);
    let mut b = str_col(&mut p, &["y"]);
    b.validity_mut().unwrap().set_valid(0, false);
    let (out, _) = run(&mut p, logic::coalesce_n, &[&a, &b]);
    assert!(!out.var().unwrap().validity.is_valid(0));
    for c in [out, a, b] {
        c.reclaim(&mut p);
    }
}

/// Operands that cannot widen together meet as a variant: each row keeps the value it took, as it is, and nothing is
/// marked, since nothing was lost.
#[test]
fn coalesce_meets_operands_that_cannot_widen_as_a_variant() {
    use smeltr::format::tag::{Tag, decode};
    let mut p = BufferPool::new();
    let a = opt_str_col(&mut p, &[None, Some("b")]);
    let b = i64_col(&mut p, &[10, 20]);
    let (out, kinds) = run(&mut p, logic::coalesce_n, &[&a, &b]);

    assert_eq!(out.data_type(), DataType::Variant);
    let v = out.variant().unwrap();
    assert_eq!(decode(v.get(0)), Some(Tag::I64(10)), "a number stays a number");
    assert_eq!(decode(v.get(1)), Some(Tag::Str("b".into())));
    assert!(kinds.is_empty(), "got {kinds:?}");
    for x in [out, a, b] {
        x.reclaim(&mut p);
    }
}

#[test]
fn coalesce_over_bytes_basic() {
    let mut p = BufferPool::new();
    let mut a = bytes_col(&mut p, &[b"\x00\x01", b"\x02"]);
    a.validity_mut().unwrap().set_valid(1, false);
    let b = bytes_col(&mut p, &[b"\xff", b"\x03"]);
    let (out, _) = run(&mut p, logic::coalesce_n, &[&a, &b]);
    let v = out.var().unwrap();
    assert_eq!(v.get(0), b"\x00\x01");
    assert_eq!(v.get(1), b"\x03");
    for c in [out, a, b] {
        c.reclaim(&mut p);
    }
}

#[test]
fn coalesce_reads_a_literal_as_one_value() {
    // A literal operand meets the kernel as a constant column. It holds a value in every row, so it takes every row
    // still open, wherever it stands in the chain.
    let mut p = BufferPool::new();
    let a = opt_i64_col(&mut p, &[Some(1), None, None]);
    let b = opt_i64_col(&mut p, &[Some(9), Some(2), None]);
    let zero = Column::constant(Scalar::I64(0), 3);
    let (last, _) = run(&mut p, logic::coalesce_n, &[&a, &b, &zero]);
    assert_eq!(read_i64s(&last), vec![Some(1), Some(2), Some(0)]);
    let (first, _) = run(&mut p, logic::coalesce_n, &[&zero, &a]);
    assert_eq!(read_i64s(&first), vec![Some(0); 3]);
    for c in [last, first, a, b, zero] {
        c.reclaim(&mut p);
    }
}

#[test]
fn coalesce_over_text_reads_a_literal_as_one_value() {
    let mut p = BufferPool::new();
    let a = opt_str_col(&mut p, &[Some("x"), None, None]);
    let dash = Column::constant(Scalar::Str("-".into()), 3);
    let (out, _) = run(&mut p, logic::coalesce_n, &[&a, &dash]);
    assert_eq!(read_strs(&out), ["x", "-", "-"].map(|s| Some(s.to_string())));

    // A literal that meets the text as a variant is held as one, a number still.
    let seven = Column::constant(Scalar::I64(7), 3);
    let (rendered, kinds) = run(&mut p, logic::coalesce_n, &[&a, &seven]);
    assert_eq!(rendered.data_type(), DataType::Variant);
    let v = rendered.variant().unwrap();
    use smeltr::format::tag::{Tag, decode};
    assert_eq!(decode(v.get(0)), Some(Tag::Str("x".into())));
    assert_eq!(decode(v.get(2)), Some(Tag::I64(7)));
    assert!(kinds.is_empty(), "got {kinds:?}");
    for c in [out, rendered, a, dash, seven] {
        c.reclaim(&mut p);
    }
}

#[test]
fn coalesce_takes_the_first_array_per_row() {
    // A row is one operand's whole array: the first whose row is not null. An empty array is a value; an unbound
    // operand never wins.
    let mut p = BufferPool::new();
    let mut a = arr_col(&mut p, &[&["a", "b"], &["x"], &["x"], &["x"]]);
    for row in 1..4 {
        a.validity_mut().unwrap().set_valid(row, false);
    }
    let mut b = arr_col(&mut p, &[&["z"], &["c"], &[], &["x"]]);
    b.validity_mut().unwrap().set_valid(3, false);
    let unbound = Column::empty(DataType::Array(Box::new(DataType::Str)), 4);
    let (out, _) = run(&mut p, logic::coalesce_n, &[&unbound, &a, &b]);
    assert_eq!(out.data_type(), DataType::Array(Box::new(DataType::Str)));
    let text = |row: &[&str]| Some(row.iter().map(|s| Some(s.to_string())).collect::<Vec<_>>());
    assert_eq!(
        read_str_lists(&out),
        vec![text(&["a", "b"]), text(&["c"]), text(&[]), None]
    );
    for c in [out, a, b, unbound] {
        c.reclaim(&mut p);
    }
}

#[test]
fn coalesce_takes_the_first_struct_row() {
    // A struct row holds a value when any leaf does. The winner's whole row is taken, its null fields included.
    let mut p = BufferPool::new();
    let s = Column::struct_from_parts(
        vec![
            ("x".to_string(), opt_i64_col(&mut p, &[Some(1), None, None, None])),
            (
                "y".to_string(),
                opt_str_col(&mut p, &[Some("a"), Some("b"), None, None]),
            ),
        ],
        4,
    );
    let t = Column::struct_from_parts(
        vec![
            ("x".to_string(), opt_i64_col(&mut p, &[Some(9), Some(2), Some(3), None])),
            (
                "y".to_string(),
                opt_str_col(&mut p, &[Some("z"), Some("q"), None, None]),
            ),
        ],
        4,
    );
    let (out, _) = run(&mut p, logic::coalesce_n, &[&s, &t]);
    assert_eq!(read_i64s(out.field("x").unwrap()), vec![Some(1), None, Some(3), None]);
    assert_eq!(
        read_strs(out.field("y").unwrap()),
        [Some("a"), Some("b"), None, None].map(|v| v.map(str::to_string))
    );

    // A field the winner never had is null in the rows it wins.
    let u = Column::struct_from_parts(
        vec![
            ("x".to_string(), Column::empty(DataType::I64, 2)),
            ("y".to_string(), opt_str_col(&mut p, &[Some("a"), None])),
        ],
        2,
    );
    let v = Column::struct_from_parts(
        vec![
            ("x".to_string(), opt_i64_col(&mut p, &[Some(1), Some(2)])),
            ("y".to_string(), opt_str_col(&mut p, &[Some("p"), Some("q")])),
        ],
        2,
    );
    let (out2, _) = run(&mut p, logic::coalesce_n, &[&u, &v]);
    assert_eq!(read_i64s(out2.field("x").unwrap()), vec![None, Some(2)]);
    assert_eq!(
        read_strs(out2.field("y").unwrap()),
        [Some("a"), Some("q")].map(|v| v.map(str::to_string))
    );
    for c in [out, out2, s, t, u, v] {
        c.reclaim(&mut p);
    }
}

#[test]
fn coalesce_and_ternary_meet_a_struct_field_as_its_columns_hold_it() {
    // `a.k` is text and `b.k` a variant, so the field meets as a variant and `a`'s text is held as one, value by value.
    // Row 0: `a` holds nothing, and both `a ?? b` and `c ? a : b` (`c` false) take `b`. Row 1: both take `a`.
    use smeltr::core::batch::{Batch, RAW_SLOTS, raw_layout};
    use smeltr::expr::ast::Span;
    use smeltr::expr::ir::{Ir, IrKind};

    let mut p = BufferPool::new();
    let a = |p: &mut BufferPool| {
        Column::struct_from_parts(
            vec![
                ("k".to_string(), opt_str_col(p, &[None, Some("x")])),
                ("n".to_string(), opt_i64_col(p, &[None, Some(1)])),
            ],
            2,
        )
    };
    let b = |p: &mut BufferPool| {
        let ints = i64_col(p, &[5, 6]);
        let k = smeltr::format::tag::to_variant(&ints, p);
        ints.reclaim(p);
        Column::struct_from_parts(vec![("k".to_string(), k)], 2)
    };

    let (ca, cb) = (a(&mut p), b(&mut p));
    let (coalesced, _) = run(&mut p, logic::coalesce_n, &[&ca, &cb]);

    let (ta, tb) = (ca.data_type(), cb.data_type());
    let mut layout = raw_layout();
    layout.extend([ta.clone(), tb.clone(), DataType::Bool]);
    let mut batch = Batch::new(&layout, 2, 0);
    batch.put(RAW_SLOTS, ca, &mut p);
    batch.put(RAW_SLOTS + 1, cb, &mut p);
    let cond = bool_col(&mut p, &[Some(false), Some(true)]);
    batch.put(RAW_SLOTS + 2, cond, &mut p);
    let field = |slot, ty| Ir::new(ty, Span::default(), IrKind::Field(slot));
    let ir = Ir::new(
        ta.unify(&tb).unwrap(),
        Span::default(),
        IrKind::Ternary {
            cond: Box::new(field(RAW_SLOTS + 2, DataType::Bool)),
            then: Box::new(field(RAW_SLOTS, ta)),
            else_: Box::new(field(RAW_SLOTS + 1, tb)),
        },
    );
    let mut errors = ErrorSink::new();
    let selected = smeltr::expr::eval::eval(&ir, &batch, &mut Ctx::new(&mut p, &mut errors));

    let rows = |c: &Column| -> Vec<Option<Vec<u8>>> {
        let v = c.variant().expect("the field meets as a variant");
        (0..c.len())
            .map(|i| v.validity.is_valid(i).then(|| v.get(i).to_vec()))
            .collect()
    };
    assert_eq!(coalesced.data_type(), selected.data_type());
    let k = rows(coalesced.field("k").unwrap());
    assert!(k.iter().all(Option::is_some), "`??` keeps `a`'s text: {k:?}");
    assert_eq!(k, rows(selected.field("k").unwrap()));
    assert_eq!(read_i64s(coalesced.field("n").unwrap()), vec![None, Some(1)]);
    assert_eq!(read_i64s(selected.field("n").unwrap()), vec![None, Some(1)]);
    for c in [coalesced, selected] {
        c.reclaim(&mut p);
    }
    batch.reclaim(&mut p);
}

/// A list over `child` whose rows have the given lengths, `None` for a null row.
fn list_col(p: &mut BufferPool, child: Column, rows: &[Option<u32>]) -> Column {
    let mut offsets = p.take((rows.len() + 1) * 4);
    let mut acc = vec![0u32];
    let mut validity = Validity::alloc(p, rows.len());
    for (i, r) in rows.iter().enumerate() {
        acc.push(acc[i] + r.unwrap_or(0));
        if r.is_none() {
            validity.set_valid(i, false);
        }
    }
    offsets.as_mut_slice::<u32>().copy_from_slice(&acc);
    Column::list_from_parts(offsets, child, validity, rows.len())
}

#[test]
fn coalesce_over_structs_gathers_an_array_field() {
    // The winner's row is taken whole: its array field through the list gather, its scalar through the fixed one.
    let mut p = BufferPool::new();
    let mut s_tags = arr_col(&mut p, &[&["a"], &["x"]]);
    s_tags.validity_mut().unwrap().set_valid(1, false);
    let s = Column::struct_from_parts(
        vec![
            ("tags".to_string(), s_tags),
            ("n".to_string(), opt_i64_col(&mut p, &[Some(1), None])),
        ],
        2,
    );
    let t = Column::struct_from_parts(
        vec![
            ("tags".to_string(), arr_col(&mut p, &[&["p"], &["q", "r"]])),
            ("n".to_string(), opt_i64_col(&mut p, &[Some(8), Some(9)])),
        ],
        2,
    );
    let (out, _) = run(&mut p, logic::coalesce_n, &[&s, &t]);
    let text = |row: &[&str]| Some(row.iter().map(|s| Some(s.to_string())).collect::<Vec<_>>());
    assert_eq!(
        read_str_lists(out.field("tags").unwrap()),
        vec![text(&["a"]), text(&["q", "r"])]
    );
    assert_eq!(read_i64s(out.field("n").unwrap()), vec![Some(1), Some(9)]);
    for c in [out, s, t] {
        c.reclaim(&mut p);
    }
}

#[test]
fn coalesce_reads_three_arrays_in_order() {
    let mut p = BufferPool::new();
    let mut a = arr_col(&mut p, &[&["a"], &["x"], &["x"]]);
    a.validity_mut().unwrap().set_valid(1, false);
    a.validity_mut().unwrap().set_valid(2, false);
    let mut b = arr_col(&mut p, &[&["x"], &["b"], &["x"]]);
    b.validity_mut().unwrap().set_valid(0, false);
    b.validity_mut().unwrap().set_valid(2, false);
    let c = arr_col(&mut p, &[&["z"], &["z"], &["c"]]);
    let (out, _) = run(&mut p, logic::coalesce_n, &[&a, &b, &c]);
    let text = |row: &[&str]| Some(row.iter().map(|s| Some(s.to_string())).collect::<Vec<_>>());
    assert_eq!(read_str_lists(&out), vec![text(&["a"]), text(&["b"]), text(&["c"])]);
    for c in [out, a, b, c] {
        c.reclaim(&mut p);
    }
}

#[test]
fn an_all_empty_array_column_has_an_unbound_child_and_still_gathers() {
    // A parser that saw only `[]` gives a list whose child is an unbound column of zero rows. Every row of it is an
    // empty array, a value, and a zero-length span reads nothing from the child.
    let mut p = BufferPool::new();
    let empties = list_col(&mut p, Column::empty(DataType::Str, 0), &[Some(0), Some(0)]);
    let ys = arr_col(&mut p, &[&["y"], &["y"]]);
    let (out, _) = run(&mut p, logic::coalesce_n, &[&empties, &ys]);
    assert_eq!(read_str_lists(&out), vec![Some(vec![]), Some(vec![])]);
    let (cat, _) = run(&mut p, arrayops::array_concat, &[&empties, &ys]);
    let text = |row: &[&str]| Some(row.iter().map(|s| Some(s.to_string())).collect::<Vec<_>>());
    assert_eq!(read_str_lists(&cat), vec![text(&["y"]), text(&["y"])]);
    for c in [out, cat, empties, ys] {
        c.reclaim(&mut p);
    }
}

/// `??` over two structs of one shape, or two arrays of one element type, keeps that shape: an `f64` field stays
/// an `f64` field, an `array<f64>` stays an `array<f64>`, and no row is marked. The compiler widens an `i64`
/// side to `f64` before the kernel runs, so both operands arrive as the same type.
#[test]
fn coalesce_keeps_an_f64_field_and_an_f64_element_type() {
    let mut p = BufferPool::new();
    let s = Column::struct_from_parts(vec![("n".to_string(), opt_f64_col(&mut p, &[Some(1.0), None]))], 2);
    let t = Column::struct_from_parts(vec![("n".to_string(), opt_f64_col(&mut p, &[Some(9.0), Some(2.5)]))], 2);
    let (out, kinds) = run(&mut p, logic::coalesce_n, &[&s, &t]);
    assert_eq!(
        out.data_type(),
        DataType::Struct(vec![("n".to_string(), DataType::F64)], false)
    );
    assert_eq!(read_f64s(out.field("n").unwrap()), vec![Some(1.0), Some(2.5)]);
    assert!(kinds.is_empty(), "no coercion: {kinds:?}");

    let xs_elems = f64_col(&mut p, &[1.0, 2.0]);
    let xs = list_col(&mut p, xs_elems, &[Some(2), None]);
    let ys_elems = f64_col(&mut p, &[9.5, 3.5]);
    let ys = list_col(&mut p, ys_elems, &[Some(1), Some(1)]);
    let (out2, _) = run(&mut p, logic::coalesce_n, &[&xs, &ys]);
    assert_eq!(out2.data_type(), DataType::Array(Box::new(DataType::F64)));
    let l = out2.list().unwrap();
    assert_eq!(l.offsets, &[0, 2, 3]);
    assert_eq!(l.child.f64().unwrap().values, &[1.0, 2.0, 3.5]);
    for c in [out, out2, s, t, xs, ys] {
        c.reclaim(&mut p);
    }
}

// ---------------------------------------------------------------------------
// Strings
// ---------------------------------------------------------------------------

#[test]
fn concat_n_writes_each_byte_once() {
    let mut p = BufferPool::new();
    let a = str_col(&mut p, &["John", "Jane"]);
    let sp = str_col(&mut p, &[" ", " "]);
    let b = str_col(&mut p, &["Smith", "Doe"]);
    let (out, _) = run(&mut p, strops::concat_n, &[&a, &sp, &b]);
    let v = out.var().unwrap();
    assert_eq!(v.get(0), b"John Smith");
    assert_eq!(v.get(1), b"Jane Doe");
    assert_eq!(v.offsets, &[0, 10, 18]);
    for c in [out, a, sp, b] {
        c.reclaim(&mut p);
    }
}

#[test]
fn concat_propagates_null() {
    let mut p = BufferPool::new();
    let mut a = str_col(&mut p, &["x", "y"]);
    a.validity_mut().unwrap().set_valid(1, false);
    let b = str_col(&mut p, &["1", "2"]);
    let (out, _) = run(&mut p, strops::concat_n, &[&a, &b]);
    let v = out.var().unwrap();
    assert_eq!(v.get(0), b"x1");
    assert!(!v.validity.is_valid(1));
    for c in [out, a, b] {
        c.reclaim(&mut p);
    }
}

#[test]
fn concat_reads_a_literal_as_one_value() {
    // A literal operand meets the kernel as a constant column, written into every row from the one copy.
    let mut p = BufferPool::new();
    let mut a = str_col(&mut p, &["John", "Jane", "x"]);
    a.validity_mut().unwrap().set_valid(2, false);
    let title = Column::constant(Scalar::Str("Dr. ".into()), 3);
    let dot = Column::constant(Scalar::Str(".".into()), 3);
    let (out, _) = run(&mut p, strops::concat_n, &[&title, &a, &dot]);
    assert_eq!(
        read_strs(&out),
        [Some("Dr. John.".to_string()), Some("Dr. Jane.".to_string()), None]
    );
    for c in [out, a, title, dot] {
        c.reclaim(&mut p);
    }
}

#[test]
fn length_uses_the_ascii_fast_path() {
    let mut p = BufferPool::new();
    let a = str_col(&mut p, &["hello", ""]);
    assert!(a.var().unwrap().ascii);
    let (out, _) = run(&mut p, strops::str_length, &[&a]);
    assert_eq!(out.i64().unwrap().values, &[5, 0]);
    for c in [out, a] {
        c.reclaim(&mut p);
    }
}

/// Characters, not JS's UTF-16 code units: "👋" is one, so `length` agrees with `slice` and `indexOf`.
#[test]
fn length_counts_characters() {
    let mut p = BufferPool::new();
    // "café" is 5 bytes and 4 chars; "👋" is 4 bytes and 1 char (JS would say 2).
    let a = str_col(&mut p, &["caf\u{e9}", "\u{1F44B}"]);
    assert!(!a.var().unwrap().ascii);
    let (out, _) = run(&mut p, strops::str_length, &[&a]);
    assert_eq!(out.i64().unwrap().values, &[4, 1]);
    for c in [out, a] {
        c.reclaim(&mut p);
    }
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

/// Every binary kernel returns the type `result_type` declares for its operands. The compiler types slots from the
/// table and the runtime types them from the kernel, so the two must agree. The operands are small, so no I64 kernel
/// overflows: that is the one case where a kernel may widen to F64.
#[test]
fn every_binop_kernel_returns_its_declared_type() {
    fn operand(p: &mut BufferPool, ty: &DataType) -> Column {
        match ty {
            DataType::I64 => i64_col(p, &[2, 3]),
            DataType::F64 => f64_col(p, &[2.0, 0.5]),
            DataType::Bool => bool_col(p, &[Some(true), Some(false)]),
            DataType::Str => str_col(p, &["a", "b"]),
            DataType::Bytes => bytes_col(p, &[b"a", b"b"]),
            DataType::DateTime => datetime_col(p, &[2_000_000, 3_000_000]),
            DataType::Duration => duration_col(p, &[2_000_000, 3_000_000]),
            _ => arr_col(p, &[&["a"], &["b"]]),
        }
    }
    let types = [
        DataType::I64,
        DataType::F64,
        DataType::Bool,
        DataType::Str,
        DataType::Bytes,
        DataType::DateTime,
        DataType::Duration,
        DataType::Array(Box::new(DataType::Str)),
    ];
    let mut p = BufferPool::new();
    for op in BinOp::ALL {
        for l in &types {
            for r in &types {
                let Some(declared) = table::result_type(op, l, r) else {
                    continue;
                };
                let what = format!("{l:?} {} {r:?}", op.symbol());
                let kernel = table::kernel(op, l, r).unwrap_or_else(|| panic!("{what} has no kernel"));
                let (lhs, rhs) = (operand(&mut p, l), operand(&mut p, r));
                let (out, _) = run(&mut p, kernel, &[&lhs, &rhs]);
                let got = out.data_type();
                assert_eq!(got, declared, "{what}");
                for c in [out, lhs, rhs] {
                    c.reclaim(&mut p);
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Stdlib: toString / parseInt / parseFloat / round
// ---------------------------------------------------------------------------

#[test]
fn to_string_converts_i64_f64_and_bool() {
    let mut p = BufferPool::new();
    let i = i64_col(&mut p, &[42, -7]);
    let (out, _) = run(&mut p, convert::to_string, &[&i]);
    assert_eq!(read_strs(&out), vec![Some("42".to_string()), Some("-7".to_string())]);
    out.reclaim(&mut p);

    let f = f64_col(&mut p, &[1.5]);
    let (out, _) = run(&mut p, convert::to_string, &[&f]);
    assert_eq!(read_strs(&out), vec![Some("1.5".to_string())]);
    out.reclaim(&mut p);
    f.reclaim(&mut p);

    let b = bool_col(&mut p, &[Some(true), Some(false)]);
    let (out, _) = run(&mut p, convert::to_string, &[&b]);
    assert_eq!(
        read_strs(&out),
        vec![Some("true".to_string()), Some("false".to_string())]
    );
    for c in [out, i, b] {
        c.reclaim(&mut p);
    }
}

#[test]
fn to_string_propagates_null() {
    let mut p = BufferPool::new();
    let a = opt_i64_col(&mut p, &[Some(1), None]);
    let (out, _) = run(&mut p, convert::to_string, &[&a]);
    assert_eq!(read_strs(&out), vec![Some("1".to_string()), None]);
    for c in [out, a] {
        c.reclaim(&mut p);
    }
}

#[test]
fn parse_int_is_lenient_like_str_parse() {
    let mut p = BufferPool::new();
    let a = opt_str_col(&mut p, &[Some("  42  "), Some("not a number"), None]);
    let (out, _) = run(&mut p, convert::parse_int, &[&a]);
    assert_eq!(read_i64s(&out), vec![Some(42), None, None]);
    for c in [out, a] {
        c.reclaim(&mut p);
    }
}

#[test]
fn parse_float_rejects_non_finite_results() {
    let mut p = BufferPool::new();
    let a = opt_str_col(&mut p, &[Some("3.5"), Some("1e400"), Some("garbage")]);
    let (out, _) = run(&mut p, convert::parse_float, &[&a]);
    assert_eq!(read_f64s(&out), vec![Some(3.5), None, None]);
    for c in [out, a] {
        c.reclaim(&mut p);
    }
}

#[test]
fn round_f64_propagates_null() {
    let mut p = BufferPool::new();
    let a = opt_f64_col(&mut p, &[Some(1.1), None]);
    let (out, _) = run(&mut p, arith::round_f64, &[&a]);
    assert_eq!(read_i64s(&out), vec![Some(1), None]);
    for c in [out, a] {
        c.reclaim(&mut p);
    }
}

// ---------------------------------------------------------------------------
// Stdlib: regex match / replace
// ---------------------------------------------------------------------------

#[test]
fn regex_test_reports_per_row_hits() {
    let mut p = BufferPool::new();
    let a = str_col(&mut p, &["error: disk full", "info: ok", "ERROR: oops"]);
    let pat = pattern("error", false);
    let mut errors = ErrorSink::new();
    let out = {
        let mut ctx = Ctx::new(&mut p, &mut errors);
        regex_test(&mut ctx, &a, &pat)
    };
    assert_eq!(read_bools(&out), vec![Some(true), Some(false), Some(false)]);
    for c in [out, a] {
        c.reclaim(&mut p);
    }
}

#[test]
fn regex_test_propagates_null() {
    let mut p = BufferPool::new();
    let a = opt_str_col(&mut p, &[Some("error"), None]);
    let pat = pattern("error", false);
    let mut errors = ErrorSink::new();
    let out = {
        let mut ctx = Ctx::new(&mut p, &mut errors);
        regex_test(&mut ctx, &a, &pat)
    };
    assert_eq!(read_bools(&out), vec![Some(true), None]);
    for c in [out, a] {
        c.reclaim(&mut p);
    }
}

#[test]
fn regex_replace_without_global_replaces_first_match_only() {
    let mut p = BufferPool::new();
    let a = str_col(&mut p, &["a-b-c"]);
    let r = str_col(&mut p, &["_"]);
    let pat = pattern("-", false);
    let mut errors = ErrorSink::new();
    let out = {
        let mut ctx = Ctx::new(&mut p, &mut errors);
        regex_replace(&mut ctx, &a, &pat, &r, false)
    };
    assert_eq!(read_strs(&out), vec![Some("a_b-c".to_string())]);
    for c in [out, a, r] {
        c.reclaim(&mut p);
    }
}

#[test]
fn regex_replace_global_replaces_every_match() {
    let mut p = BufferPool::new();
    let a = str_col(&mut p, &["a-b-c"]);
    let r = str_col(&mut p, &["_"]);
    let pat = pattern("-", false);
    let mut errors = ErrorSink::new();
    let out = {
        let mut ctx = Ctx::new(&mut p, &mut errors);
        regex_replace(&mut ctx, &a, &pat, &r, true)
    };
    assert_eq!(read_strs(&out), vec![Some("a_b_c".to_string())]);
    for c in [out, a, r] {
        c.reclaim(&mut p);
    }
}

#[test]
fn regex_replace_supports_capture_group_references() {
    let mut p = BufferPool::new();
    let a = str_col(&mut p, &["2026-08-25"]);
    let r = str_col(&mut p, &["$3/$2/$1"]);
    let pat = pattern(r"(\d+)-(\d+)-(\d+)", false);
    let mut errors = ErrorSink::new();
    let out = {
        let mut ctx = Ctx::new(&mut p, &mut errors);
        regex_replace(&mut ctx, &a, &pat, &r, false)
    };
    assert_eq!(read_strs(&out), vec![Some("25/08/2026".to_string())]);
    for c in [out, a, r] {
        c.reclaim(&mut p);
    }
}

#[test]
fn regex_replace_propagates_null_from_either_side() {
    let mut p = BufferPool::new();
    let a = opt_str_col(&mut p, &[Some("a-b"), None, Some("c-d")]);
    let r = opt_str_col(&mut p, &[Some("_"), Some("_"), None]);
    let pat = pattern("-", false);
    let mut errors = ErrorSink::new();
    let out = {
        let mut ctx = Ctx::new(&mut p, &mut errors);
        regex_replace(&mut ctx, &a, &pat, &r, false)
    };
    assert_eq!(read_strs(&out), vec![Some("a_b".to_string()), None, None]);
    for c in [out, a, r] {
        c.reclaim(&mut p);
    }
}

// ---------------------------------------------------------------------------
// Stdlib: string methods (slice, indexOf, case, trim, startsWith/endsWith/
// includes)
// ---------------------------------------------------------------------------

#[test]
fn slice_two_and_three_arg_forms() {
    let mut p = BufferPool::new();
    let v = str_col(&mut p, &["hello world", "hello world"]);
    let start = i64_col(&mut p, &[6, 0]);
    let (out2, _) = run(&mut p, strops::str_slice, &[&v, &start]);
    assert_eq!(
        read_strs(&out2),
        vec![Some("world".to_string()), Some("hello world".to_string())]
    );
    out2.reclaim(&mut p);

    let end = i64_col(&mut p, &[11, 5]);
    let (out3, _) = run(&mut p, strops::str_slice, &[&v, &start, &end]);
    assert_eq!(
        read_strs(&out3),
        vec![Some("world".to_string()), Some("hello".to_string())]
    );
    for c in [out3, v, start, end] {
        c.reclaim(&mut p);
    }
}

#[test]
fn slice_negative_indices_count_from_the_end() {
    let mut p = BufferPool::new();
    let v = str_col(&mut p, &["hello world", "hello world"]);
    let start = i64_col(&mut p, &[-5, 0]);
    let end = i64_col(&mut p, &[11, -6]);
    let (out, _) = run(&mut p, strops::str_slice, &[&v, &start, &end]);
    assert_eq!(
        read_strs(&out),
        vec![Some("world".to_string()), Some("hello".to_string())]
    );
    for c in [out, v, start, end] {
        c.reclaim(&mut p);
    }
}

#[test]
fn slice_clamps_out_of_range_indices() {
    let mut p = BufferPool::new();
    let v = str_col(&mut p, &["hi"]);
    let start = i64_col(&mut p, &[-100]);
    let end = i64_col(&mut p, &[100]);
    let (out, _) = run(&mut p, strops::str_slice, &[&v, &start, &end]);
    assert_eq!(read_strs(&out), vec![Some("hi".to_string())]);
    for c in [out, v, start, end] {
        c.reclaim(&mut p);
    }
}

#[test]
fn slice_start_past_end_yields_empty_string_not_null() {
    let mut p = BufferPool::new();
    let v = str_col(&mut p, &["hi"]);
    let start = i64_col(&mut p, &[2]);
    let end = i64_col(&mut p, &[0]);
    let (out, _) = run(&mut p, strops::str_slice, &[&v, &start, &end]);
    assert_eq!(read_strs(&out), vec![Some(String::new())]);
    for c in [out, v, start, end] {
        c.reclaim(&mut p);
    }
}

#[test]
fn slice_propagates_null_from_value_start_or_end() {
    let mut p = BufferPool::new();
    let v = opt_str_col(&mut p, &[None, Some("hi"), Some("hi")]);
    let start = opt_i64_col(&mut p, &[Some(0), None, Some(0)]);
    let end = opt_i64_col(&mut p, &[Some(1), Some(1), None]);
    let (out, _) = run(&mut p, strops::str_slice, &[&v, &start, &end]);
    assert_eq!(read_strs(&out), vec![None, None, None]);
    for c in [out, v, start, end] {
        c.reclaim(&mut p);
    }
}

#[test]
fn slice_uses_character_boundaries_on_non_ascii_text() {
    let mut p = BufferPool::new();
    // "café" is 5 bytes, 4 chars: c-a-f-é. slice(0,3) must land on "caf",
    // not split é's multi-byte encoding.
    let v = str_col(&mut p, &["caf\u{e9}"]);
    let start = i64_col(&mut p, &[0]);
    let end = i64_col(&mut p, &[3]);
    let (out, _) = run(&mut p, strops::str_slice, &[&v, &start, &end]);
    assert_eq!(read_strs(&out), vec![Some("caf".to_string())]);
    for c in [out, v, start, end] {
        c.reclaim(&mut p);
    }
}

#[test]
fn index_of_finds_position_or_reports_negative_one() {
    let mut p = BufferPool::new();
    let v = str_col(&mut p, &["hello world", "hello world"]);
    let needle = str_col(&mut p, &["world", "xyz"]);
    let (out, _) = run(&mut p, strops::str_index_of, &[&v, &needle]);
    assert_eq!(out.i64().unwrap().values, &[6, -1]);
    for c in [out, v, needle] {
        c.reclaim(&mut p);
    }
}

#[test]
fn index_of_propagates_null() {
    let mut p = BufferPool::new();
    let v = opt_str_col(&mut p, &[Some("hi"), None]);
    let needle = opt_str_col(&mut p, &[None, Some("h")]);
    let (out, _) = run(&mut p, strops::str_index_of, &[&v, &needle]);
    assert_eq!(read_i64s(&out), vec![None, None]);
    for c in [out, v, needle] {
        c.reclaim(&mut p);
    }
}

#[test]
fn to_upper_and_to_lower_are_unicode_aware() {
    let mut p = BufferPool::new();
    let v = str_col(&mut p, &["café", "STRASSE", "\u{df}"]); // ß
    let (out, _) = run(&mut p, strops::str_to_upper, &[&v]);
    assert_eq!(
        read_strs(&out),
        vec![
            Some("CAFÉ".to_string()),
            Some("STRASSE".to_string()),
            Some("SS".to_string()),
        ]
    );
    out.reclaim(&mut p);

    let v2 = str_col(&mut p, &["CAFÉ"]);
    let (out2, _) = run(&mut p, strops::str_to_lower, &[&v2]);
    assert_eq!(read_strs(&out2), vec![Some("café".to_string())]);
    for c in [out2, v, v2] {
        c.reclaim(&mut p);
    }
}

#[test]
fn trim_removes_unicode_whitespace() {
    let mut p = BufferPool::new();
    let v = str_col(&mut p, &["  hi  ", "\t\nhi\u{a0}"]);
    let (out, _) = run(&mut p, strops::str_trim, &[&v]);
    assert_eq!(read_strs(&out), vec![Some("hi".to_string()), Some("hi".to_string())]);
    for c in [out, v] {
        c.reclaim(&mut p);
    }
}

#[test]
fn starts_with_ends_with_includes_basic() {
    let mut p = BufferPool::new();
    let v = str_col(&mut p, &["hello world"]);
    let pre = str_col(&mut p, &["hello"]);
    let (out, _) = run(&mut p, strops::str_starts_with, &[&v, &pre]);
    assert_eq!(read_bools(&out), vec![Some(true)]);
    out.reclaim(&mut p);

    let suf = str_col(&mut p, &["world"]);
    let (out, _) = run(&mut p, strops::str_ends_with, &[&v, &suf]);
    assert_eq!(read_bools(&out), vec![Some(true)]);
    out.reclaim(&mut p);

    let mid = str_col(&mut p, &["lo wo"]);
    let (out, _) = run(&mut p, strops::str_includes, &[&v, &mid]);
    assert_eq!(read_bools(&out), vec![Some(true)]);
    for c in [out, v, pre, suf, mid] {
        c.reclaim(&mut p);
    }
}

#[test]
fn includes_handles_the_empty_needle() {
    let mut p = BufferPool::new();
    let v = str_col(&mut p, &["hi", ""]);
    let needle = str_col(&mut p, &["", ""]);
    let (out, _) = run(&mut p, strops::str_includes, &[&v, &needle]);
    assert_eq!(read_bools(&out), vec![Some(true), Some(true)]);
    for c in [out, v, needle] {
        c.reclaim(&mut p);
    }
}

#[test]
fn starts_with_propagates_null() {
    let mut p = BufferPool::new();
    let v = opt_str_col(&mut p, &[Some("hi"), None]);
    let pre = opt_str_col(&mut p, &[None, Some("h")]);
    let (out, _) = run(&mut p, strops::str_starts_with, &[&v, &pre]);
    assert_eq!(read_bools(&out), vec![None, None]);
    for c in [out, v, pre] {
        c.reclaim(&mut p);
    }
}

// ---------------------------------------------------------------------------
// Stdlib: parseTime
// ---------------------------------------------------------------------------

fn read_datetimes(c: &Column) -> Vec<Option<i64>> {
    let f = c.datetime().unwrap();
    (0..c.len())
        .map(|i| f.validity.is_valid(i).then_some(f.values[i]))
        .collect()
}

/// Preview parses one record alone, so a row must parse the same whatever rows come before it. Each row here changes
/// format, so the hint the kernel carries from the row before is wrong on every row.
#[test]
fn parse_time_auto_detect_gives_each_row_of_a_mixed_column_what_it_gives_alone() {
    let mut p = BufferPool::new();
    let texts = [
        "2026-08-25T12:30:00Z",
        "2026-08-25 12:30:00",
        "2026/08/25",
        "08/25/2026",
        "08/25/2026 3:30:00 PM",
        "August 25, 2026",
        "Tue, 25 Aug 2026 12:30:00 GMT",
        "25 Aug 2026",
        "not a timestamp",
    ];
    let v = str_col(&mut p, &texts);
    let (out, _) = run(&mut p, convert::parse_time, &[&v]);
    let vals = read_datetimes(&out);
    for (i, s) in texts.iter().enumerate() {
        assert_eq!(vals[i], timeparse::parse_auto(s), "row {i}: {s}");
    }
    // Equal to `parse_auto` proves nothing if both miss.
    assert!(vals[..8].iter().all(Option::is_some), "{vals:?}");
    for c in [out, v] {
        c.reclaim(&mut p);
    }
}

#[test]
fn parse_time_auto_detect_stays_correct_on_a_homogeneous_column() {
    // The common case the sticky hint targets: every row the same format
    // (one source's rows, one log format). Distinct seconds per row so
    // this can't degenerate into checking one cached value against
    // itself.
    let mut p = BufferPool::new();
    let rows: Vec<String> = (0..20).map(|i| format!("2026-08-25T12:30:{:02}Z", i)).collect();
    let refs: Vec<&str> = rows.iter().map(String::as_str).collect();
    let v = str_col(&mut p, &refs);
    let (out, _) = run(&mut p, convert::parse_time, &[&v]);
    let vals = read_datetimes(&out);
    for (i, val) in vals.iter().enumerate() {
        let expected = timeparse::parse_auto(&rows[i]);
        assert_eq!(*val, expected, "row {i}");
    }
    for c in [out, v] {
        c.reclaim(&mut p);
    }
}

#[test]
fn parse_time_auto_detect_recovers_after_the_format_changes_mid_column() {
    // A format switch partway through (still realistic -- a source
    // rotating a log format) must not corrupt anything after it: the
    // stale hint from before the switch has to fail and fall back
    // correctly for every row of the new format.
    let mut p = BufferPool::new();
    let v = str_col(
        &mut p,
        &[
            "2026-08-25T12:30:00Z",
            "2026-08-25T12:30:01Z",
            "2026-08-25T12:30:02Z",
            "08/25/2026 12:30:03",
            "08/25/2026 12:30:04",
            "08/25/2026 12:30:05",
        ],
    );
    let (out, _) = run(&mut p, convert::parse_time, &[&v]);
    let vals = read_datetimes(&out);
    for (i, s) in [
        "2026-08-25T12:30:00Z",
        "2026-08-25T12:30:01Z",
        "2026-08-25T12:30:02Z",
        "08/25/2026 12:30:03",
        "08/25/2026 12:30:04",
        "08/25/2026 12:30:05",
    ]
    .iter()
    .enumerate()
    {
        assert_eq!(vals[i], timeparse::parse_auto(s), "row {i}");
    }
    for c in [out, v] {
        c.reclaim(&mut p);
    }
}

#[test]
fn parse_time_unparseable_text_is_null_not_a_failure() {
    let mut p = BufferPool::new();
    let v = str_col(&mut p, &["not a timestamp"]);
    let (out, ann) = run(&mut p, convert::parse_time, &[&v]);
    assert_eq!(read_datetimes(&out), vec![None]);
    assert!(ann.is_empty(), "a miss is unremarkable, not an exception");
    for c in [out, v] {
        c.reclaim(&mut p);
    }
}

#[test]
fn parse_time_propagates_null_from_value_or_format() {
    let mut p = BufferPool::new();
    let v = opt_str_col(&mut p, &[None, Some("2026-08-25")]);
    let fmt = opt_str_col(&mut p, &[Some("%Y-%m-%d"), None]);
    let (out, _) = run(&mut p, convert::parse_time, &[&v, &fmt]);
    assert_eq!(read_datetimes(&out), vec![None, None]);
    for c in [out, v, fmt] {
        c.reclaim(&mut p);
    }
}

// ---------------------------------------------------------------------------
// Stdlib: math (abs, min, max, floor, ceil)
// ---------------------------------------------------------------------------

#[test]
fn abs_preserves_i64_and_fails_the_row_of_i64_min() {
    let mut p = BufferPool::new();
    let a = i64_col(&mut p, &[-5, 5, 0, i64::MIN]);
    let (out, _) = run(&mut p, arith::abs_num, &[&a]);
    assert_eq!(out.data_type(), DataType::I64);
    let f = out.i64().unwrap();
    assert_eq!(&f.values[..3], &[5, 5, 0]);
    assert!(!f.validity.is_valid(3));
    for c in [out, a] {
        c.reclaim(&mut p);
    }
}

#[test]
fn abs_preserves_f64() {
    let mut p = BufferPool::new();
    let a = f64_col(&mut p, &[-5.5, 5.5]);
    let (out, _) = run(&mut p, arith::abs_num, &[&a]);
    assert_eq!(out.data_type(), DataType::F64);
    assert_eq!(out.f64().unwrap().values, &[5.5, 5.5]);
    for c in [out, a] {
        c.reclaim(&mut p);
    }
}

#[test]
fn min_max_i64_stay_i64() {
    let mut p = BufferPool::new();
    let a = i64_col(&mut p, &[1, 9]);
    let b = i64_col(&mut p, &[4, 2]);
    let (out, _) = run(&mut p, arith::min_num, &[&a, &b]);
    assert_eq!(out.data_type(), DataType::I64);
    assert_eq!(out.i64().unwrap().values, &[1, 2]);
    out.reclaim(&mut p);

    let (out, _) = run(&mut p, arith::max_num, &[&a, &b]);
    assert_eq!(out.data_type(), DataType::I64);
    assert_eq!(out.i64().unwrap().values, &[4, 9]);
    for c in [out, a, b] {
        c.reclaim(&mut p);
    }
}

#[test]
fn min_max_propagate_null() {
    let mut p = BufferPool::new();
    let a = opt_i64_col(&mut p, &[Some(1), None]);
    let b = opt_i64_col(&mut p, &[None, Some(2)]);
    let (out, _) = run(&mut p, arith::min_num, &[&a, &b]);
    assert_eq!(read_i64s(&out), vec![None, None]);
    for c in [out, a, b] {
        c.reclaim(&mut p);
    }
}

#[test]
fn floor_and_ceil_round_toward_the_right_direction() {
    let mut p = BufferPool::new();
    let a = f64_col(&mut p, &[2.7, -2.7]);
    let (out, _) = run(&mut p, arith::floor_f64, &[&a]);
    assert_eq!(out.i64().unwrap().values, &[2, -3]);
    out.reclaim(&mut p);

    let (out, _) = run(&mut p, arith::ceil_f64, &[&a]);
    assert_eq!(out.i64().unwrap().values, &[3, -2]);
    for c in [out, a] {
        c.reclaim(&mut p);
    }
}

#[test]
fn floor_propagates_null() {
    let mut p = BufferPool::new();
    let a = opt_f64_col(&mut p, &[Some(1.1), None]);
    let (out, _) = run(&mut p, arith::floor_f64, &[&a]);
    assert_eq!(read_i64s(&out), vec![Some(1), None]);
    for c in [out, a] {
        c.reclaim(&mut p);
    }
}

// ---------------------------------------------------------------------------
// Stdlib: DateTime parts
// ---------------------------------------------------------------------------

// 2026-08-25T12:30:45Z, a Tuesday.
const SAMPLE_DT: i64 = 1_787_661_045_000_000;

#[test]
fn extracts_every_part_of_a_known_instant() {
    let mut p = BufferPool::new();
    let c = datetime_col(&mut p, &[SAMPLE_DT]);

    let (year, _) = run(&mut p, datetime::dt_year, &[&c]);
    assert_eq!(year.i64().unwrap().values[0], 2026);
    year.reclaim(&mut p);

    let (month, _) = run(&mut p, datetime::dt_month, &[&c]);
    assert_eq!(month.i64().unwrap().values[0], 8);
    month.reclaim(&mut p);

    let (day, _) = run(&mut p, datetime::dt_day, &[&c]);
    assert_eq!(day.i64().unwrap().values[0], 25);
    day.reclaim(&mut p);

    let (hour, _) = run(&mut p, datetime::dt_hour, &[&c]);
    assert_eq!(hour.i64().unwrap().values[0], 12);
    hour.reclaim(&mut p);

    let (minute, _) = run(&mut p, datetime::dt_minute, &[&c]);
    assert_eq!(minute.i64().unwrap().values[0], 30);
    minute.reclaim(&mut p);

    let (second, _) = run(&mut p, datetime::dt_second, &[&c]);
    assert_eq!(second.i64().unwrap().values[0], 45);
    second.reclaim(&mut p);

    let (dow, _) = run(&mut p, datetime::dt_day_of_week, &[&c]);
    assert_eq!(dow.i64().unwrap().values[0], 2, "Tuesday");
    dow.reclaim(&mut p);

    c.reclaim(&mut p);
}

/// `round` breaks a tie away from zero, as `as i64` does, and `0.49999999999999994` does not carry to `1`.
#[test]
fn round_breaks_ties_away_from_zero() {
    let mut p = BufferPool::new();
    let a = opt_f64_col(
        &mut p,
        &[Some(2.5), Some(-2.5), Some(-2.6), Some(0.49999999999999994), Some(-0.5)],
    );
    let (out, _) = run(&mut p, arith::round_f64, &[&a]);
    assert_eq!(read_i64s(&out), vec![Some(3), Some(-3), Some(-3), Some(0), Some(-1)]);
    for c in [out, a] {
        c.reclaim(&mut p);
    }
}

/// A value outside `i64` fails its row in `round`, `floor`, `ceil` and `trunc`, as it does in `as i64`.
#[test]
fn rounding_out_of_i64_fails_the_row() {
    let mut p = BufferPool::new();
    let a = opt_f64_col(&mut p, &[Some(1.5), Some(1e300), Some(-1e300), Some(f64::NAN), None]);
    for k in [arith::round_f64, arith::floor_f64, arith::ceil_f64, arith::trunc_f64] {
        let (out, ann) = run(&mut p, k, &[&a]);
        let first = read_i64s(&out)[0];
        assert_eq!(read_i64s(&out), vec![first, None, None, None, None]);
        assert_eq!(ann, vec![ExceptionKind::Overflow; 3]);
        out.reclaim(&mut p);
    }
    a.reclaim(&mut p);
}

#[test]
fn a_sunday_is_seven_and_a_monday_is_one() {
    let mut p = BufferPool::new();
    // 1970-01-04 was a Sunday, 1970-01-05 a Monday.
    let c = datetime_col(&mut p, &[3 * 86_400_000_000, 4 * 86_400_000_000]);
    let (dow, _) = run(&mut p, datetime::dt_day_of_week, &[&c]);
    assert_eq!(dow.i64().unwrap().values, &[7, 1]);
    for col in [dow, c] {
        col.reclaim(&mut p);
    }
}

#[test]
fn epoch_is_a_thursday() {
    let mut p = BufferPool::new();
    let c = datetime_col(&mut p, &[0]);
    let (dow, _) = run(&mut p, datetime::dt_day_of_week, &[&c]);
    assert_eq!(dow.i64().unwrap().values[0], 4);
    for col in [dow, c] {
        col.reclaim(&mut p);
    }
}

#[test]
fn before_epoch_rounds_toward_the_earlier_day() {
    let mut p = BufferPool::new();
    // One microsecond before epoch: 1969-12-31T23:59:59.999999Z.
    let c = datetime_col(&mut p, &[-1]);
    let (year, _) = run(&mut p, datetime::dt_year, &[&c]);
    assert_eq!(year.i64().unwrap().values[0], 1969);
    year.reclaim(&mut p);
    let (month, _) = run(&mut p, datetime::dt_month, &[&c]);
    assert_eq!(month.i64().unwrap().values[0], 12);
    month.reclaim(&mut p);
    let (day, _) = run(&mut p, datetime::dt_day, &[&c]);
    assert_eq!(day.i64().unwrap().values[0], 31);
    day.reclaim(&mut p);
    let (hour, _) = run(&mut p, datetime::dt_hour, &[&c]);
    assert_eq!(hour.i64().unwrap().values[0], 23);
    hour.reclaim(&mut p);
    c.reclaim(&mut p);
}

#[test]
fn datetime_part_extraction_propagates_null() {
    let mut p = BufferPool::new();
    let mut c = Column::alloc(&mut p, &DataType::DateTime, 2);
    {
        let f = c.datetime_mut().unwrap();
        f.values[0] = SAMPLE_DT;
        f.validity.set_valid(1, false);
    }
    let (year, _) = run(&mut p, datetime::dt_year, &[&c]);
    assert_eq!(read_i64s(&year), vec![Some(2026), None]);
    for col in [year, c] {
        col.reclaim(&mut p);
    }
}

#[test]
fn datetime_part_extraction_short_circuits_on_unbound_input() {
    let mut p = BufferPool::new();
    let c = Column::empty(DataType::DateTime, 3);
    let (year, _) = run(&mut p, datetime::dt_year, &[&c]);
    assert!(year.is_unbound());
    assert_eq!(year.len(), 3);
    year.reclaim(&mut p);
}

// ---------------------------------------------------------------------------
// Stdlib: Array length
// ---------------------------------------------------------------------------

/// Builds an `Array(Str)` column: each element of `rows` becomes one row's
/// element span, back-to-back in a single flattened `Str` child.
fn arr_col(p: &mut BufferPool, rows: &[&[&str]]) -> Column {
    let flat: Vec<&str> = rows.iter().flat_map(|r| r.iter().copied()).collect();
    let child = str_col(p, &flat);
    let mut offsets = p.take((rows.len() + 1) * 4);
    let mut acc = vec![0u32];
    let mut running = 0u32;
    for r in rows {
        running += r.len() as u32;
        acc.push(running);
    }
    offsets.as_mut_slice::<u32>().copy_from_slice(&acc);
    let validity = Validity::alloc(p, rows.len());
    Column::list_from_parts(offsets, child, validity, rows.len())
}

#[test]
fn array_length_counts_elements_per_row() {
    let mut p = BufferPool::new();
    let a = arr_col(&mut p, &[&["a", "b", "c"], &[], &["x"]]);
    let (out, _) = run(&mut p, arrayops::array_length, &[&a]);
    assert_eq!(out.i64().unwrap().values, &[3, 0, 1]);
    for c in [out, a] {
        c.reclaim(&mut p);
    }
}

#[test]
fn array_length_propagates_a_null_row() {
    let mut p = BufferPool::new();
    let child = str_col(&mut p, &["b", "c"]);
    let mut offsets = p.take(3 * 4);
    offsets.as_mut_slice::<u32>().copy_from_slice(&[0, 0, 2]);
    let mut validity = Validity::alloc(&mut p, 2);
    validity.set_valid(0, false);
    let a = Column::list_from_parts(offsets, child, validity, 2);

    let (out, _) = run(&mut p, arrayops::array_length, &[&a]);
    assert_eq!(read_i64s(&out), vec![None, Some(2)]);
    for c in [out, a] {
        c.reclaim(&mut p);
    }
}

#[test]
fn array_length_short_circuits_on_unbound_input() {
    let mut p = BufferPool::new();
    let a = Column::empty(DataType::Array(Box::new(DataType::Str)), 4);
    let (out, _) = run(&mut p, arrayops::array_length, &[&a]);
    assert!(out.is_unbound());
    assert_eq!(out.len(), 4);
    out.reclaim(&mut p);
}

#[test]
fn length_dispatches_between_text_and_array_by_runtime_type() {
    let mut p = BufferPool::new();
    let s = str_col(&mut p, &["hello"]);
    let (out, _) = run(&mut p, smeltr::ops::stdfuncs::length_any, &[&s]);
    assert_eq!(out.i64().unwrap().values, &[5]);
    out.reclaim(&mut p);
    s.reclaim(&mut p);

    let a = arr_col(&mut p, &[&["a", "b"]]);
    let (out, _) = run(&mut p, smeltr::ops::stdfuncs::length_any, &[&a]);
    assert_eq!(out.i64().unwrap().values, &[2]);
    out.reclaim(&mut p);
    a.reclaim(&mut p);
}

// ---------------------------------------------------------------------------
// Stdlib: regex match (groups), regex split, JS replacement references
// ---------------------------------------------------------------------------

/// Rows of `Array(Str)`: `None` is a null row, `Some(vec![.., None, ..])` a null element.
fn read_str_lists(c: &Column) -> Vec<Option<Vec<Option<String>>>> {
    let l = c.list().unwrap();
    let elems = read_strs(l.child);
    (0..c.len())
        .map(|row| {
            l.validity
                .is_valid(row)
                .then(|| elems[l.offsets[row] as usize..l.offsets[row + 1] as usize].to_vec())
        })
        .collect()
}

#[test]
fn regex_match_returns_the_whole_match_and_its_groups_or_null() {
    let mut p = BufferPool::new();
    let a = opt_str_col(&mut p, &[Some("user=alice id=7"), Some("no fields here"), None]);
    let pat = pattern(r"user=(\w+) id=(\d+)", false);
    let mut errors = ErrorSink::new();
    let out = {
        let mut ctx = Ctx::new(&mut p, &mut errors);
        smeltr::ops::arrayops::regex_match(&mut ctx, &a, &pat)
    };
    assert_eq!(
        read_str_lists(&out),
        vec![
            Some(vec![
                Some("user=alice id=7".to_string()),
                Some("alice".to_string()),
                Some("7".to_string())
            ]),
            None,
            None,
        ]
    );
    for c in [out, a] {
        c.reclaim(&mut p);
    }
}

/// JS leaves an unmatched optional group `undefined`; here it is a null element, not an absent one.
#[test]
fn regex_match_keeps_an_unmatched_group_as_a_null_element() {
    let mut p = BufferPool::new();
    let a = str_col(&mut p, &["ab"]);
    let pat = pattern(r"(a)(x)?(b)", false);
    let mut errors = ErrorSink::new();
    let out = {
        let mut ctx = Ctx::new(&mut p, &mut errors);
        smeltr::ops::arrayops::regex_match(&mut ctx, &a, &pat)
    };
    assert_eq!(
        read_str_lists(&out),
        vec![Some(vec![
            Some("ab".to_string()),
            Some("a".to_string()),
            None,
            Some("b".to_string())
        ])]
    );
    for c in [out, a] {
        c.reclaim(&mut p);
    }
}

#[test]
fn regex_split_splits_on_every_match() {
    let mut p = BufferPool::new();
    let a = opt_str_col(&mut p, &[Some("a, b,c"), Some("solo"), None]);
    let pat = pattern(r",\s*", false);
    let mut errors = ErrorSink::new();
    let out = {
        let mut ctx = Ctx::new(&mut p, &mut errors);
        smeltr::ops::arrayops::regex_split(&mut ctx, &a, &pat)
    };
    assert_eq!(
        read_str_lists(&out),
        vec![
            Some(vec![
                Some("a".to_string()),
                Some("b".to_string()),
                Some("c".to_string())
            ]),
            Some(vec![Some("solo".to_string())]),
            None,
        ]
    );
    for c in [out, a] {
        c.reclaim(&mut p);
    }
}

/// JS: `"abc".split("")` is the characters, not the whole string.
#[test]
fn split_on_an_empty_delimiter_yields_characters() {
    let mut p = BufferPool::new();
    let a = str_col(&mut p, &["ab\u{e9}", ""]);
    let d = str_col(&mut p, &["", ""]);
    let (out, _) = run(&mut p, arrayops::str_split, &[&a, &d]);
    assert_eq!(
        read_str_lists(&out),
        vec![
            Some(vec![
                Some("a".to_string()),
                Some("b".to_string()),
                Some("\u{e9}".to_string())
            ]),
            Some(vec![]),
        ]
    );
    for c in [out, a, d] {
        c.reclaim(&mut p);
    }
}

/// One set of capture slots serves the batch, so a group that hit in one row must read as null in the next row
/// where it did not: the engine clears the slots on every search.
#[test]
fn regex_match_clears_the_capture_slots_between_rows() {
    let mut p = BufferPool::new();
    let a = str_col(&mut p, &["a", "b"]);
    let pat = pattern(r"(a)|(b)", false);
    let mut errors = ErrorSink::new();
    let out = {
        let mut ctx = Ctx::new(&mut p, &mut errors);
        smeltr::ops::arrayops::regex_match(&mut ctx, &a, &pat)
    };
    assert_eq!(
        read_str_lists(&out),
        vec![
            Some(vec![Some("a".to_string()), Some("a".to_string()), None]),
            Some(vec![Some("b".to_string()), None, Some("b".to_string())]),
        ]
    );
    for c in [out, a] {
        c.reclaim(&mut p);
    }
}

/// An empty match is an empty string, whole match and group alike; null is only a group that took no part.
#[test]
fn an_empty_match_is_an_empty_string_not_null() {
    let mut p = BufferPool::new();
    let a = str_col(&mut p, &["abc"]);
    let mut errors = ErrorSink::new();
    let whole = {
        let mut ctx = Ctx::new(&mut p, &mut errors);
        smeltr::ops::arrayops::regex_match(&mut ctx, &a, &pattern(r"x*", false))
    };
    let grouped = {
        let mut ctx = Ctx::new(&mut p, &mut errors);
        smeltr::ops::arrayops::regex_match(&mut ctx, &a, &pattern(r"(x*)", false))
    };
    assert_eq!(read_str_lists(&whole), vec![Some(vec![Some(String::new())])]);
    assert_eq!(
        read_str_lists(&grouped),
        vec![Some(vec![Some(String::new()), Some(String::new())])]
    );
    for c in [whole, grouped, a] {
        c.reclaim(&mut p);
    }
}

/// A trailing delimiter leaves an empty piece at the very end of the row, which for the last row is the end of the
/// column's buffer: the one span whose end is the buffer's end.
#[test]
fn an_empty_piece_at_the_buffers_end_and_across_rows() {
    let mut p = BufferPool::new();
    let a = str_col(&mut p, &["a,", "b,"]);
    let d = str_col(&mut p, &[",", ","]);
    let (out, _) = run(&mut p, arrayops::str_split, &[&a, &d]);
    assert_eq!(
        read_str_lists(&out),
        vec![
            Some(vec![Some("a".to_string()), Some(String::new())]),
            Some(vec![Some("b".to_string()), Some(String::new())]),
        ]
    );
    for c in [out, a, d] {
        c.reclaim(&mut p);
    }
}

/// A batch of no rows goes through every kernel and comes out as no rows.
#[test]
fn a_zero_row_input_gives_a_zero_row_list() {
    let mut p = BufferPool::new();
    let a = str_col(&mut p, &[]);
    let d = str_col(&mut p, &[]);
    let (split, _) = run(&mut p, arrayops::str_split, &[&a, &d]);
    let mut errors = ErrorSink::new();
    let pat = pattern(r"(a)|,", false);
    let (by_regex, matched) = {
        let mut ctx = Ctx::new(&mut p, &mut errors);
        (
            smeltr::ops::arrayops::regex_split(&mut ctx, &a, &pat),
            smeltr::ops::arrayops::regex_match(&mut ctx, &a, &pat),
        )
    };
    for c in [&split, &by_regex, &matched] {
        assert_eq!(c.len(), 0);
        assert_eq!(read_str_lists(c), Vec::<Option<Vec<Option<String>>>>::new());
    }
    for c in [split, by_regex, matched, a, d] {
        c.reclaim(&mut p);
    }
}

/// JS spells a named group `$<name>` and the whole match `$&`; the regex crate spells them `${name}` and `${0}`.
/// Both spellings work, and `$$` stays a literal dollar.
#[test]
fn regex_replace_accepts_js_capture_references() {
    let mut p = BufferPool::new();
    let a = str_col(&mut p, &["2026-09-04"]);
    let r = str_col(&mut p, &["$<m>/$<y> [$&] $$ ${y}"]);
    let pat = pattern(r"(?<y>\d{4})-(?<m>\d{2})", false);
    let mut errors = ErrorSink::new();
    let out = {
        let mut ctx = Ctx::new(&mut p, &mut errors);
        regex_replace(&mut ctx, &a, &pat, &r, false)
    };
    assert_eq!(read_strs(&out), vec![Some("09/2026 [2026-09] $ 2026-04".to_string())]);
    for c in [out, a, r] {
        c.reclaim(&mut p);
    }
}

// ---------------------------------------------------------------------------
// Stdlib: isIp, ipVersion, ipNormalize, ipScope, inCidr, cidrNetwork
// ---------------------------------------------------------------------------

/// Text that is not an address is null everywhere but `isIp`, which answers `false`. Null stays null.
#[test]
fn ip_functions_answer_null_on_text_that_is_not_an_address() {
    let mut p = BufferPool::new();
    let a = opt_str_col(&mut p, &[Some("10.1.2.3"), Some("nope"), None, Some(" ::1 ")]);
    let (is, _) = run(&mut p, netops::is_ip, &[&a]);
    let (ver, _) = run(&mut p, netops::ip_version, &[&a]);
    let (norm, _) = run(&mut p, netops::ip_normalize, &[&a]);
    let (scope, _) = run(&mut p, netops::ip_scope, &[&a]);
    assert_eq!(read_bools(&is), vec![Some(true), Some(false), None, Some(true)]);
    assert_eq!(read_i64s(&ver), vec![Some(4), None, None, Some(6)]);
    assert_eq!(
        read_strs(&norm),
        vec![Some("10.1.2.3".into()), None, None, Some("::1".into())]
    );
    assert_eq!(
        read_strs(&scope),
        vec![Some("private".into()), None, None, Some("loopback".into())]
    );
    for c in [is, ver, norm, scope, a] {
        c.reclaim(&mut p);
    }
}

#[test]
fn ip_normalize_compresses_v6_and_lowercases() {
    let mut p = BufferPool::new();
    let a = str_col(
        &mut p,
        &["2001:0DB8:0000:0000:0000:0000:0000:0001", "::FFFF:10.1.2.3", "1.2.3.4"],
    );
    let (out, _) = run(&mut p, netops::ip_normalize, &[&a]);
    assert_eq!(
        read_strs(&out),
        vec![
            Some("2001:db8::1".into()),
            Some("::ffff:10.1.2.3".into()),
            Some("1.2.3.4".into())
        ]
    );
    for c in [out, a] {
        c.reclaim(&mut p);
    }
}

/// An IPv6 zone names a link: every function reads the address before it, and only `ipNormalize` writes it back.
/// An IPv4 octet with a leading zero is not an address.
#[test]
fn ip_functions_read_a_zone_and_refuse_a_leading_zero() {
    let mut p = BufferPool::new();
    let a = str_col(&mut p, &["FE80::1%eth0", "10.1.2.3%eth0", "01.2.3.4"]);
    let (ver, _) = run(&mut p, netops::ip_version, &[&a]);
    let (norm, _) = run(&mut p, netops::ip_normalize, &[&a]);
    let (scope, _) = run(&mut p, netops::ip_scope, &[&a]);
    assert_eq!(read_i64s(&ver), vec![Some(6), None, None]);
    assert_eq!(read_strs(&norm), vec![Some("fe80::1%eth0".into()), None, None]);
    assert_eq!(read_strs(&scope), vec![Some("link-local".into()), None, None]);
    for c in [ver, norm, scope, a] {
        c.reclaim(&mut p);
    }
}

#[test]
fn ip_scope_names_every_range_in_both_families() {
    let mut p = BufferPool::new();
    let a = str_col(
        &mut p,
        &[
            "0.0.0.0",
            "127.0.0.1",
            "169.254.1.1",
            "224.0.0.1",
            "10.0.0.1",
            "172.16.0.1",
            "192.168.1.1",
            "8.8.8.8",
            "::",
            "::1",
            "fe80::1",
            "ff02::1",
            "fc00::1",
            "fdff::1",
            "2001:db8::1",
        ],
    );
    let (out, _) = run(&mut p, netops::ip_scope, &[&a]);
    let got: Vec<String> = read_strs(&out).into_iter().map(Option::unwrap).collect();
    assert_eq!(
        got,
        [
            "unspecified",
            "loopback",
            "link-local",
            "multicast",
            "private",
            "private",
            "private",
            "public",
            "unspecified",
            "loopback",
            "link-local",
            "multicast",
            "private",
            "private",
            "public",
        ]
    );
    for c in [out, a] {
        c.reclaim(&mut p);
    }
}

/// Any block hit is a hit. A block of the other family holds nothing. A block that does not parse makes the row null,
/// as does an address that does not.
#[test]
fn in_cidr_takes_any_number_of_blocks() {
    let mut p = BufferPool::new();
    let a = opt_str_col(
        &mut p,
        &[Some("10.1.2.3"), Some("10.1.2.3"), Some("192.168.0.9"), Some("x"), None],
    );
    let b1 = str_col(
        &mut p,
        &["10.0.0.0/8", "::/0", "10.0.0.0/8", "10.0.0.0/8", "10.0.0.0/8"],
    );
    let b2 = str_col(
        &mut p,
        &[
            "192.168.0.0/16",
            "192.168.0.0/16",
            "192.168.0.0/16",
            "192.168.0.0/16",
            "10.0.0.0/8",
        ],
    );
    let (one, _) = run(&mut p, netops::in_cidr, &[&a, &b1]);
    let (two, _) = run(&mut p, netops::in_cidr, &[&a, &b1, &b2]);
    assert_eq!(read_bools(&one), vec![Some(true), Some(false), Some(false), None, None]);
    assert_eq!(read_bools(&two), vec![Some(true), Some(false), Some(true), None, None]);

    let bad = str_col(
        &mut p,
        &["10.0.0.0/33", "10.1.2.3", "10.0.0.0/8", "10.0.0.0/8", "10.0.0.0/8"],
    );
    let (with_bad, _) = run(&mut p, netops::in_cidr, &[&a, &bad]);
    assert_eq!(read_bools(&with_bad), vec![None, Some(true), Some(false), None, None]);

    // A literal block is a constant column, parsed once and read in every row, at any position past the address.
    let literal = Column::constant(Scalar::Str("10.0.0.0/8".into()), 5);
    let (lit, _) = run(&mut p, netops::in_cidr, &[&a, &literal]);
    assert_eq!(read_bools(&lit), vec![Some(true), Some(true), Some(false), None, None]);
    let (third, _) = run(&mut p, netops::in_cidr, &[&a, &b2, &literal]);
    assert_eq!(read_bools(&third), vec![Some(true), Some(true), Some(true), None, None]);

    // A v6 address in a v6 block. A null block is a null row.
    let v6 = str_col(&mut p, &["2001:db8::1", "2001:db8::1", "2001:db8::1"]);
    let v6_blocks = opt_str_col(&mut p, &[Some("2001:db8::/32"), Some("2001:db9::/32"), None]);
    let (six, _) = run(&mut p, netops::in_cidr, &[&v6, &v6_blocks]);
    assert_eq!(read_bools(&six), vec![Some(true), Some(false), None]);
    for c in [
        one, two, with_bad, lit, third, six, a, b1, b2, bad, literal, v6, v6_blocks,
    ] {
        c.reclaim(&mut p);
    }
}

#[test]
fn cidr_network_masks_to_the_prefix() {
    let mut p = BufferPool::new();
    let a = opt_str_col(
        &mut p,
        &[
            Some("10.1.2.3"),
            Some("10.1.2.3"),
            Some("2001:db8:abcd::1"),
            Some("10.1.2.3"),
            None,
        ],
    );
    let n = i64_col(&mut p, &[24, 0, 32, 33, 8]);
    let (out, _) = run(&mut p, netops::cidr_network, &[&a, &n]);
    assert_eq!(
        read_strs(&out),
        vec![
            Some("10.1.2.0/24".into()),
            Some("0.0.0.0/0".into()),
            Some("2001:db8::/32".into()),
            None,
            None
        ]
    );
    let literal = Column::constant(Scalar::I64(16), 5);
    let (lit, _) = run(&mut p, netops::cidr_network, &[&a, &literal]);
    assert_eq!(read_strs(&lit)[0], Some("10.1.0.0/16".into()));
    for c in [out, lit, a, n, literal] {
        c.reclaim(&mut p);
    }
}

// ---------------------------------------------------------------------------
// Stdlib: trimStart/trimEnd, padStart/padEnd, repeat, lastIndexOf, at
// ---------------------------------------------------------------------------

#[test]
fn trim_start_and_trim_end_take_one_side_each() {
    let mut p = BufferPool::new();
    let a = opt_str_col(&mut p, &[Some("  x \t"), None]);
    let (s, _) = run(&mut p, strops::str_trim_start, &[&a]);
    let (e, _) = run(&mut p, strops::str_trim_end, &[&a]);
    assert_eq!(read_strs(&s), vec![Some("x \t".to_string()), None]);
    assert_eq!(read_strs(&e), vec![Some("  x".to_string()), None]);
    for c in [s, e, a] {
        c.reclaim(&mut p);
    }
}

/// JS: the pad repeats and is cut to fit, counted in characters; a short target or an empty pad changes nothing.
#[test]
fn pad_start_and_pad_end_follow_js() {
    let mut p = BufferPool::new();
    let a = str_col(&mut p, &["5", "ab", "caf\u{e9}", "long"]);
    let n = i64_col(&mut p, &[3, 5, 6, 2]);
    let pad = str_col(&mut p, &["0", "xy", "-", ""]);
    let (s, _) = run(&mut p, strops::str_pad_start, &[&a, &n, &pad]);
    let (e, _) = run(&mut p, strops::str_pad_end, &[&a, &n, &pad]);
    assert_eq!(
        read_strs(&s),
        vec![
            Some("005".into()),
            Some("xyxab".into()),
            Some("--caf\u{e9}".into()),
            Some("long".into())
        ]
    );
    assert_eq!(
        read_strs(&e),
        vec![
            Some("500".into()),
            Some("abxyx".into()),
            Some("caf\u{e9}--".into()),
            Some("long".into())
        ]
    );
    // The pad defaults to a space.
    let (d, _) = run(&mut p, strops::str_pad_start, &[&a, &n]);
    assert_eq!(read_strs(&d)[1], Some("   ab".into()));
    for c in [s, e, d, a, n, pad] {
        c.reclaim(&mut p);
    }
}

#[test]
fn repeat_refuses_a_negative_count_and_an_oversized_result() {
    let mut p = BufferPool::new();
    let a = str_col(&mut p, &["ab", "ab", "ab", "x"]);
    let n = i64_col(&mut p, &[3, 0, -1, 100_000_000]);
    let (out, _) = run(&mut p, strops::str_repeat, &[&a, &n]);
    assert_eq!(
        read_strs(&out),
        vec![Some("ababab".into()), Some("".into()), None, None]
    );
    for c in [out, a, n] {
        c.reclaim(&mut p);
    }
}

#[test]
fn last_index_of_counts_characters_from_the_last_hit() {
    let mut p = BufferPool::new();
    let a = str_col(&mut p, &["a-b-c", "caf\u{e9}-x-y", "none"]);
    let needle = str_col(&mut p, &["-", "-", "-"]);
    let (out, _) = run(&mut p, strops::str_last_index_of, &[&a, &needle]);
    assert_eq!(read_i64s(&out), vec![Some(3), Some(6), Some(-1)]);
    for c in [out, a, needle] {
        c.reclaim(&mut p);
    }
}

#[test]
fn at_indexes_characters_and_counts_back_from_the_end() {
    let mut p = BufferPool::new();
    let a = str_col(&mut p, &["h\u{e9}llo", "h\u{e9}llo", "hi", "hi"]);
    let idx = i64_col(&mut p, &[1, -1, 5, -3]);
    let (out, _) = run(&mut p, strops::str_at, &[&a, &idx]);
    assert_eq!(
        read_strs(&out),
        vec![Some("\u{e9}".into()), Some("o".into()), None, None]
    );
    for c in [out, a, idx] {
        c.reclaim(&mut p);
    }
}

// ---------------------------------------------------------------------------
// Stdlib: trunc, sign, millisecond, epochMillis, fromEpochMillis, duration, total
// ---------------------------------------------------------------------------

#[test]
fn trunc_drops_the_fraction_toward_zero() {
    let mut p = BufferPool::new();
    let a = f64_col(&mut p, &[2.7, -2.7, 0.2]);
    let (out, _) = run(&mut p, arith::trunc_f64, &[&a]);
    assert_eq!(out.i64().unwrap().values, &[2, -2, 0]);
    for c in [out, a] {
        c.reclaim(&mut p);
    }
}

#[test]
fn sign_is_minus_one_zero_or_one_and_nan_is_null() {
    let mut p = BufferPool::new();
    let ints = i64_col(&mut p, &[-7, 0, 9]);
    let (si, _) = run(&mut p, arith::sign_num, &[&ints]);
    assert_eq!(read_i64s(&si), vec![Some(-1), Some(0), Some(1)]);
    let floats = f64_col(&mut p, &[-0.5, 0.0, 2.5, f64::NAN]);
    let (sf, _) = run(&mut p, arith::sign_num, &[&floats]);
    assert_eq!(read_i64s(&sf), vec![Some(-1), Some(0), Some(1), None]);
    for c in [si, sf, ints, floats] {
        c.reclaim(&mut p);
    }
}

#[test]
fn epoch_millis_round_trips_through_from_epoch_millis() {
    let mut p = BufferPool::new();
    let t = datetime_col(&mut p, &[1_756_125_045_123_456, -1]);
    let (ms, _) = run(&mut p, datetime::dt_epoch_millis, &[&t]);
    assert_eq!(
        read_i64s(&ms),
        vec![Some(1_756_125_045_123), Some(-1)],
        "floor, as Temporal"
    );
    let (msec, _) = run(&mut p, datetime::dt_millisecond, &[&t]);
    assert_eq!(read_i64s(&msec), vec![Some(123), Some(999)]);
    let (back, _) = run(&mut p, datetime::from_epoch_millis, &[&ms]);
    assert_eq!(read_datetimes(&back), vec![Some(1_756_125_045_123_000), Some(-1000)]);
    for c in [ms, msec, back, t] {
        c.reclaim(&mut p);
    }
}

#[test]
fn duration_builds_from_a_count_and_a_unit_and_total_reads_it_back() {
    let mut p = BufferPool::new();
    let n = i64_col(&mut p, &[90, 2, 1, 5]);
    let unit = str_col(&mut p, &["seconds", "h", "DAY", "fortnights"]);
    let (d, _) = run(&mut p, datetime::duration_from, &[&n, &unit]);
    assert_eq!(
        read_durations(&d),
        vec![Some(90_000_000), Some(7_200_000_000), Some(86_400_000_000), None],
        "an unknown unit is null"
    );
    let as_unit = str_col(&mut p, &["minutes", "s", "ms", "s"]);
    let (t, _) = run(&mut p, datetime::duration_count_in, &[&d, &as_unit]);
    assert_eq!(read_f64s(&t), vec![Some(1.5), Some(7200.0), Some(86_400_000.0), None]);
    let f = f64_col(&mut p, &[1.5]);
    let secs = str_col(&mut p, &["s"]);
    let (df, _) = run(&mut p, datetime::duration_from, &[&f, &secs]);
    assert_eq!(read_durations(&df), vec![Some(1_500_000)]);
    for c in [d, t, df, n, unit, as_unit, f, secs] {
        c.reclaim(&mut p);
    }
}

/// Seven days is seven days, so a week is a unit. A month and a year are not: how long they are depends on which
/// one, and a `Duration` carries no calendar to answer that.
#[test]
fn a_week_is_a_unit_and_a_month_or_year_is_not() {
    let mut p = BufferPool::new();
    let n = i64_col(&mut p, &[1, 2, 1, 1]);
    let unit = str_col(&mut p, &["w", "weeks", "month", "year"]);
    let (d, _) = run(&mut p, datetime::duration_from, &[&n, &unit]);
    assert_eq!(
        read_durations(&d),
        vec![Some(604_800_000_000), Some(1_209_600_000_000), None, None],
        "a week converts; a month and a year have no fixed length to convert"
    );

    let as_weeks = str_col(&mut p, &["w", "w", "w", "w"]);
    let (t, _) = run(&mut p, datetime::duration_count_in, &[&d, &as_weeks]);
    assert_eq!(read_f64s(&t), vec![Some(1.0), Some(2.0), None, None]);

    for c in [d, t, n, unit, as_weeks] {
        c.reclaim(&mut p);
    }
}

/// A nanosecond is a thousandth of the stored microsecond, so the scale is a ratio: a whole number of microseconds
/// written in nanoseconds is exact, and anything finer truncates toward zero the way a fractional count already does.
#[test]
fn nanoseconds_convert_both_ways_and_truncate_below_a_microsecond() {
    let mut p = BufferPool::new();
    let n = i64_col(&mut p, &[1_000, 1_500, 1, 1_000_000_000]);
    let unit = str_col(&mut p, &["ns", "ns", "nanosecond", "ns"]);
    let (d, _) = run(&mut p, datetime::duration_from, &[&n, &unit]);
    assert_eq!(
        read_durations(&d),
        vec![Some(1), Some(1), Some(0), Some(1_000_000)],
        "1000ns is exactly 1us; 1500ns truncates to 1us; 1ns is below the stored resolution"
    );

    let secs = i64_col(&mut p, &[1]);
    let s_unit = str_col(&mut p, &["s"]);
    let (one_s, _) = run(&mut p, datetime::duration_from, &[&secs, &s_unit]);
    let as_ns = str_col(&mut p, &["ns"]);
    let (t, _) = run(&mut p, datetime::duration_count_in, &[&one_s, &as_ns]);
    assert_eq!(
        read_f64s(&t),
        vec![Some(1_000_000_000.0)],
        "a second is 1e9 nanoseconds"
    );

    for c in [d, n, unit, secs, s_unit, one_s, as_ns, t] {
        c.reclaim(&mut p);
    }
}

fn read_durations(c: &Column) -> Vec<Option<i64>> {
    let f = c.duration().unwrap();
    (0..c.len())
        .map(|i| f.validity.is_valid(i).then(|| f.values[i]))
        .collect()
}

// ---------------------------------------------------------------------------
// Stdlib: the JS Array surface -- slice, at, includes, indexOf, join, concat
// ---------------------------------------------------------------------------

#[test]
fn array_slice_clamps_and_counts_negative_indices_from_the_end() {
    let mut p = BufferPool::new();
    let a = arr_col(&mut p, &[&["a", "b", "c", "d"], &["a", "b", "c", "d"], &["x"]]);
    let starts = i64_col(&mut p, &[1, -2, 5]);
    let ends = i64_col(&mut p, &[3, 99, 9]);
    let (out, _) = run(&mut p, arrayops::array_slice, &[&a, &starts, &ends]);
    assert_eq!(
        read_str_lists(&out),
        vec![
            Some(vec![Some("b".into()), Some("c".into())]),
            Some(vec![Some("c".into()), Some("d".into())]),
            Some(vec![]),
        ]
    );
    let (tail, _) = run(&mut p, arrayops::array_slice, &[&a, &starts]);
    assert_eq!(
        read_str_lists(&tail)[0],
        Some(vec![Some("b".into()), Some("c".into()), Some("d".into())])
    );
    for c in [out, tail, a, starts, ends] {
        c.reclaim(&mut p);
    }
}

#[test]
fn array_at_reads_an_element_and_is_null_out_of_range() {
    let mut p = BufferPool::new();
    let a = arr_col(&mut p, &[&["a", "b", "c"], &["a", "b", "c"], &["a"]]);
    let idx = i64_col(&mut p, &[1, -1, 3]);
    let (out, _) = run(&mut p, arrayops::array_at, &[&a, &idx]);
    assert_eq!(read_strs(&out), vec![Some("b".into()), Some("c".into()), None]);
    for c in [out, a, idx] {
        c.reclaim(&mut p);
    }
}

#[test]
fn array_includes_and_index_of_compare_by_value() {
    let mut p = BufferPool::new();
    let a = arr_col(&mut p, &[&["x", "y", "z"], &["x", "y", "z"], &[]]);
    let needle = str_col(&mut p, &["z", "q", "x"]);
    let (inc, _) = run(&mut p, arrayops::array_includes, &[&a, &needle]);
    let (pos, _) = run(&mut p, arrayops::array_index_of, &[&a, &needle]);
    assert_eq!(read_bools(&inc), vec![Some(true), Some(false), Some(false)]);
    assert_eq!(read_i64s(&pos), vec![Some(2), Some(-1), Some(-1)]);
    for c in [inc, pos, a, needle] {
        c.reclaim(&mut p);
    }
}

#[test]
fn array_join_and_concat_follow_js() {
    let mut p = BufferPool::new();
    let a = arr_col(&mut p, &[&["a", "b"], &[]]);
    let b = arr_col(&mut p, &[&["c"], &["d", "e"]]);
    let sep = str_col(&mut p, &["-", "-"]);
    let (joined, _) = run(&mut p, arrayops::array_join, &[&a, &sep]);
    assert_eq!(read_strs(&joined), vec![Some("a-b".into()), Some("".into())]);
    let (cat, _) = run(&mut p, arrayops::array_concat, &[&a, &b]);
    assert_eq!(
        read_str_lists(&cat),
        vec![
            Some(vec![Some("a".into()), Some("b".into()), Some("c".into())]),
            Some(vec![Some("d".into()), Some("e".into())]),
        ]
    );
    for c in [joined, cat, a, b, sep] {
        c.reclaim(&mut p);
    }
}

/// Every unary kernel is registered against the operand type it actually reads. `-` on a `Duration` was wired to the
/// `I64` kernel, which panicked on the accessor.
#[test]
fn every_unop_kernel_runs_on_its_declared_operand_type() {
    let mut p = BufferPool::new();
    for op in UnOp::ALL {
        for ty in [DataType::I64, DataType::F64, DataType::Bool, DataType::Duration] {
            let Some(expected) = table::unop_result(op, &ty) else {
                continue;
            };
            let operand = match ty {
                DataType::F64 => f64_col(&mut p, &[1.0]),
                DataType::Bool => bool_col(&mut p, &[Some(true)]),
                DataType::Duration => duration_col(&mut p, &[1]),
                _ => i64_col(&mut p, &[1]),
            };
            let kernel = table::unop_kernel(op, &ty).unwrap_or_else(|| panic!("{} {ty:?} has no kernel", op.symbol()));
            let (out, _) = run(&mut p, kernel, &[&operand]);
            assert_eq!(out.data_type(), expected, "{} {ty:?}", op.symbol());
            out.reclaim(&mut p);
            operand.reclaim(&mut p);
        }
    }
}
