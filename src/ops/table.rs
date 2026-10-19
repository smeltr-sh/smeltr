//! The operator table: one row per `(op, lhs, rhs)`, from which both the type judgment (`result_type`) and the
//! dispatcher (`kernel`) are generated.
//!
//! Every kernel returns the type `result_type` declares for its row. The compiler types slots and accumulators from
//! `result_type`, and the runtime types them from what the kernel returned, so the two agree.
//!
//! `Coalesce` is not tabulated: its result is `DataType::unify` of the operands.

use super::{BinOp, OpKernel, UnOp, arith, arrayops, cmp, logic, strops};

/// `Array(T) + Array(T)` -> `Array(T)`, which no `binops!` row can express: the macro matches `DataType::$lhs` as a
/// bare pattern and an `Array` carries its element type.
///
/// Both sides must be the same array. `+` never pushes a scalar onto an array -- that is `push`, and making the
/// operator mean both would make `xs + y` ambiguous the moment `y` is itself an array.
fn array_add(op: BinOp, lhs: &DataType, rhs: &DataType) -> Option<DataType> {
    let (DataType::Array(a), DataType::Array(b)) = (lhs, rhs) else {
        return None;
    };
    // The elements meet as `concat`'s do: an `i64` array beside an `f64` one is an array of `f64`.
    let elem = a.unify(b)?;
    (op == BinOp::Add && crate::ops::stdfuncs::is_pushable_scalar(&elem)).then(|| DataType::Array(Box::new(elem)))
}
use crate::core::column::DataType;

macro_rules! binops {
    ($($op:ident, $lhs:ident, $rhs:ident => $res:ident, $kern:path;)*) => {
        /// `None` means the operator is not defined for these types: a compile error, never a coercion.
        pub fn result_type(op: BinOp, lhs: &DataType, rhs: &DataType) -> Option<DataType> {
            if let Some(t) = array_add(op, lhs, rhs) {
                return Some(t);
            }
            match (op, lhs, rhs) {
                $((BinOp::$op, DataType::$lhs, DataType::$rhs) => Some(DataType::$res),)*
                _ => None,
            }
        }

        /// Dispatch on the operands' runtime types.
        pub fn kernel(op: BinOp, lhs: &DataType, rhs: &DataType) -> Option<OpKernel> {
            if array_add(op, lhs, rhs).is_some() {
                return Some(arrayops::array_concat as OpKernel);
            }
            match (op, lhs, rhs) {
                $((BinOp::$op, DataType::$lhs, DataType::$rhs) => Some($kern as OpKernel),)*
                _ => None,
            }
        }

        #[cfg(test)]
        pub fn all_rows() -> Vec<(BinOp, DataType, DataType, DataType)> {
            vec![
                $((BinOp::$op, DataType::$lhs, DataType::$rhs, DataType::$res),)*
            ]
        }
    };
}

binops! {
    // Two numbers: `I64` beside `F64` has no row; `plan::resolve` widens the `I64` and asks again. Division and a
    // power are F64.
    Add, I64, I64 => I64, arith::add_i64_i64;
    Sub, I64, I64 => I64, arith::sub_i64_i64;
    Mul, I64, I64 => I64, arith::mul_i64_i64;
    Div, I64, I64 => F64, arith::div_i64_i64;
    Mod, I64, I64 => I64, arith::rem_i64_i64;
    Pow, I64, I64 => F64, arith::pow_i64_i64;

    Add, F64, F64 => F64, arith::add_f64_f64;
    Sub, F64, F64 => F64, arith::sub_f64_f64;
    Mul, F64, F64 => F64, arith::mul_f64_f64;
    Div, F64, F64 => F64, arith::div_f64_f64;
    Mod, F64, F64 => F64, arith::rem_f64_f64;
    Pow, F64, F64 => F64, arith::pow_f64_f64;

    // Instants and spans are both i64 microseconds; the table keeps them apart. DateTime + DateTime is deliberately
    // absent.
    Add, DateTime, Duration => DateTime, arith::add_dt_dur;
    Add, Duration, DateTime => DateTime, arith::add_dur_dt;
    Sub, DateTime, Duration => DateTime, arith::sub_dt_dur;
    Sub, DateTime, DateTime => Duration, arith::sub_dt_dt;
    Add, Duration, Duration => Duration, arith::add_dur_dur;
    Sub, Duration, Duration => Duration, arith::sub_dur_dur;
    Mod, Duration, Duration => Duration, arith::rem_dur_dur;
    Div, Duration, Duration => F64,      arith::div_dur_dur;
    Mul, Duration, I64      => Duration, arith::mul_dur_i64;
    Mul, I64,      Duration => Duration, arith::mul_i64_dur;
    Mul, Duration, F64      => Duration, arith::mul_dur_f64;
    Mul, F64,      Duration => Duration, arith::mul_f64_dur;
    Div, Duration, I64      => Duration, arith::div_dur_i64;
    Div, Duration, F64      => Duration, arith::div_dur_f64;

    // No Str + I64: implicit stringification is out.
    Add, Str, Str => Str, strops::concat_str_str;

    Eq, I64, I64 => Bool, cmp::eq_i64_i64;
    Ne, I64, I64 => Bool, cmp::ne_i64_i64;
    Lt, I64, I64 => Bool, cmp::lt_i64_i64;
    Le, I64, I64 => Bool, cmp::le_i64_i64;
    Gt, I64, I64 => Bool, cmp::gt_i64_i64;
    Ge, I64, I64 => Bool, cmp::ge_i64_i64;

    Eq, F64, F64 => Bool, cmp::eq_f64_f64;
    Ne, F64, F64 => Bool, cmp::ne_f64_f64;
    Lt, F64, F64 => Bool, cmp::lt_f64_f64;
    Le, F64, F64 => Bool, cmp::le_f64_f64;
    Gt, F64, F64 => Bool, cmp::gt_f64_f64;
    Ge, F64, F64 => Bool, cmp::ge_f64_f64;

    Eq, DateTime, DateTime => Bool, cmp::eq_dt_dt;
    Ne, DateTime, DateTime => Bool, cmp::ne_dt_dt;
    Lt, DateTime, DateTime => Bool, cmp::lt_dt_dt;
    Le, DateTime, DateTime => Bool, cmp::le_dt_dt;
    Gt, DateTime, DateTime => Bool, cmp::gt_dt_dt;
    Ge, DateTime, DateTime => Bool, cmp::ge_dt_dt;

    Eq, Duration, Duration => Bool, cmp::eq_dur_dur;
    Ne, Duration, Duration => Bool, cmp::ne_dur_dur;
    Lt, Duration, Duration => Bool, cmp::lt_dur_dur;
    Le, Duration, Duration => Bool, cmp::le_dur_dur;
    Gt, Duration, Duration => Bool, cmp::gt_dur_dur;
    Ge, Duration, Duration => Bool, cmp::ge_dur_dur;

    Eq, Bool, Bool => Bool, cmp::eq_bool_bool;
    Ne, Bool, Bool => Bool, cmp::ne_bool_bool;

    Eq, Str, Str => Bool, cmp::eq_var_var;
    Ne, Str, Str => Bool, cmp::ne_var_var;
    Lt, Str, Str => Bool, cmp::lt_var_var;
    Le, Str, Str => Bool, cmp::le_var_var;
    Gt, Str, Str => Bool, cmp::gt_var_var;
    Ge, Str, Str => Bool, cmp::ge_var_var;

    Eq, Bytes, Bytes => Bool, cmp::eq_var_var;
    Ne, Bytes, Bytes => Bool, cmp::ne_var_var;

    // The n-ary kernels handle a two-element slice.
    And, Bool, Bool => Bool, logic::and_n;
    Or,  Bool, Bool => Bool, logic::or_n;

    BitAnd, I64, I64 => I64, arith::bitand_i64;
    BitOr,  I64, I64 => I64, arith::bitor_i64;
    BitXor, I64, I64 => I64, arith::bitxor_i64;
    Shl,    I64, I64 => I64, arith::shl_i64;
    Shr,    I64, I64 => I64, arith::shr_i64;
}

/// Whether the kernel for `(op, lhs, rhs)` reads a constant operand as one value. The others read rows through an
/// accessor that refuses a constant, so the dispatcher gives them rows. The set is the kernels that read a constant
/// as one value: those built on `helpers::side`, and the n-ary kernels.
pub fn takes_constant(op: BinOp, lhs: &DataType, rhs: &DataType) -> bool {
    use DataType::*;
    let fixed = |t: &DataType| matches!(t, I64 | F64 | DateTime | Duration);
    match op {
        BinOp::Eq | BinOp::Ne | BinOp::Lt | BinOp::Le | BinOp::Gt | BinOp::Ge => {
            (fixed(lhs) && fixed(rhs)) || matches!((lhs, rhs), (Str | Bytes, Str | Bytes))
        }
        BinOp::Add => (fixed(lhs) && fixed(rhs)) || matches!((lhs, rhs), (Str, Str)),
        BinOp::Sub | BinOp::Mul | BinOp::Mod | BinOp::Pow => fixed(lhs) && fixed(rhs),
        BinOp::And | BinOp::Or => matches!((lhs, rhs), (Bool, Bool)),
        // `??` has no table row: `apply_binary` runs `coalesce_n`, which takes a constant of any type.
        BinOp::Coalesce => true,
        // `div_like`. `div_f64_f64` reads F64 through a slice.
        BinOp::Div => matches!((lhs, rhs), (I64, I64) | (Duration, Duration | I64 | F64)),
        _ => false,
    }
}

pub fn unop_result(op: UnOp, operand: &DataType) -> Option<DataType> {
    match (op, operand) {
        (UnOp::Neg, DataType::I64) => Some(DataType::I64),
        (UnOp::Neg, DataType::F64) => Some(DataType::F64),
        (UnOp::Neg, DataType::Duration) => Some(DataType::Duration),
        (UnOp::Not, DataType::Bool) => Some(DataType::Bool),
        (UnOp::BitNot, DataType::I64) => Some(DataType::I64),
        _ => None,
    }
}

pub fn unop_kernel(op: UnOp, operand: &DataType) -> Option<OpKernel> {
    Some(match (op, operand) {
        (UnOp::Neg, DataType::I64) => arith::neg_i64 as OpKernel,
        (UnOp::Neg, DataType::F64) => arith::neg_f64 as OpKernel,
        (UnOp::Neg, DataType::Duration) => arith::neg_duration as OpKernel,
        (UnOp::Not, DataType::Bool) => logic::not_bool as OpKernel,
        (UnOp::BitNot, DataType::I64) => arith::bitnot_i64 as OpKernel,
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::buffer::BufferPool;
    use crate::core::column::Column;
    use crate::ops::testing::{one_of, rows_of, run};
    use DataType::*;

    /// Every kernel `takes_constant` names gives, for a constant operand, what it gives for that value in every row:
    /// the same rows, the same nulls, the same type, the same exceptions. On either side, and on both.
    #[test]
    fn a_constant_operand_is_the_value_in_every_row() {
        let mut p = BufferPool::new();
        for (op, l, r, _) in all_rows() {
            if !takes_constant(op, &l, &r) {
                continue;
            }
            let k = kernel(op, &l, &r).unwrap();
            let (lrows, rrows) = (rows_of(&mut p, &l), rows_of(&mut p, &r));
            let (lone, rone) = (Column::constant(one_of(&l), 4), Column::constant(one_of(&r), 4));
            let (lfill, rfill) = (
                Column::filled(&mut p, &one_of(&l), 4),
                Column::filled(&mut p, &one_of(&r), 4),
            );
            let what = format!("{l:?} {} {r:?}", op.symbol());
            assert_eq!(
                run(&mut p, k, &[&lone, &rrows]),
                run(&mut p, k, &[&lfill, &rrows]),
                "{what}: constant left"
            );
            assert_eq!(
                run(&mut p, k, &[&lrows, &rone]),
                run(&mut p, k, &[&lrows, &rfill]),
                "{what}: constant right"
            );
            assert_eq!(
                run(&mut p, k, &[&lone, &rone]),
                run(&mut p, k, &[&lfill, &rfill]),
                "{what}: both constant"
            );
            for c in [lrows, rrows, lone, rone, lfill, rfill] {
                c.reclaim(&mut p);
            }
        }
    }

    /// `??` has no table row, so the sweep above never reaches it: `takes_constant` says `true` for it outright. Its
    /// kernel gives, for a constant of any scalar type, what it gives for that value in every row.
    #[test]
    fn coalesce_reads_a_constant_as_the_value_in_every_row() {
        let mut p = BufferPool::new();
        for ty in [Bool, I64, F64, DateTime, Duration, Str, Bytes] {
            let rows = rows_of(&mut p, &ty);
            let one = Column::constant(one_of(&ty), 4);
            let fill = Column::filled(&mut p, &one_of(&ty), 4);
            let k = crate::ops::logic::coalesce_n;
            assert_eq!(
                run(&mut p, k, &[&one, &rows]),
                run(&mut p, k, &[&fill, &rows]),
                "{ty:?}: constant left"
            );
            assert_eq!(
                run(&mut p, k, &[&rows, &one]),
                run(&mut p, k, &[&rows, &fill]),
                "{ty:?}: constant right"
            );
            assert_eq!(
                run(&mut p, k, &[&one, &one]),
                run(&mut p, k, &[&fill, &fill]),
                "{ty:?}: both constant"
            );
            for c in [rows, one, fill] {
                c.reclaim(&mut p);
            }
        }
    }

    /// The set `takes_constant` names is exactly the set that survives one: a kernel outside it refuses the column.
    /// The refusals are panics, caught here and printed with the rest; the hook is process-wide and stays put.
    #[test]
    fn takes_constant_names_the_kernels_that_take_one() {
        for (op, l, r, _) in all_rows() {
            if matches!(l, Array(_)) || matches!(r, Array(_)) {
                assert!(!takes_constant(op, &l, &r));
                continue;
            }
            let k = kernel(op, &l, &r).unwrap();
            let survives = std::panic::catch_unwind(|| {
                let mut p = BufferPool::new();
                let rrows = rows_of(&mut p, &r);
                let lone = Column::constant(one_of(&l), 4);
                run(&mut p, k, &[&lone, &rrows]);
                rrows.reclaim(&mut p);
                lone.reclaim(&mut p);
            })
            .is_ok();
            assert_eq!(survives, takes_constant(op, &l, &r), "{l:?} {} {r:?}", op.symbol());
        }
    }

    #[test]
    fn undefined_pairs_are_rejected() {
        assert_eq!(result_type(BinOp::Add, &Str, &I64), None);
        assert_eq!(result_type(BinOp::Add, &I64, &Str), None);
        assert_eq!(result_type(BinOp::Add, &DateTime, &DateTime), None);
        assert_eq!(result_type(BinOp::Add, &DateTime, &I64), None);
        assert_eq!(result_type(BinOp::Lt, &DateTime, &Duration), None);
        assert_eq!(result_type(BinOp::BitAnd, &F64, &F64), None);
        assert_eq!(result_type(BinOp::BitAnd, &Bool, &Bool), None);
        assert_eq!(result_type(BinOp::Lt, &Str, &I64), None);
        assert_eq!(result_type(BinOp::Lt, &Bool, &Bool), None);
        assert_eq!(unop_result(UnOp::Neg, &DateTime), None);
        assert_eq!(unop_result(UnOp::Not, &I64), None);
    }
}
