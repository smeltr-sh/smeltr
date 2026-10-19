//! Evaluator tests: hand-built `Ir` over real batches.
//!
//! No parser yet — the trees are constructed directly, which is exactly the
//! phase-2 milestone. This is the first code in the project that computes.

use smeltr::core::batch::{Batch, RAW_SLOTS, raw_layout};
use smeltr::core::buffer::BufferPool;
use smeltr::core::column::{Column, DataType, Scalar, StrBuilder, Validity, VarType};
use smeltr::core::ctx::Ctx;
use smeltr::core::error::{ErrorSink, ExceptionKind};
use smeltr::core::{bits, id::SlotId};
use smeltr::expr::ast::{Span, Value};
use smeltr::expr::eval::{eval, eval_for_slot};
use smeltr::expr::ir::{Ir, IrKind, flatten};
use smeltr::ops::stdfuncs::StdFunc;
use smeltr::ops::{BinOp, UnOp};

const A: SlotId = RAW_SLOTS; // 4
const B: SlotId = RAW_SLOTS + 1; // 5
const C: SlotId = RAW_SLOTS + 2; // 6

fn layout(extra: &[DataType]) -> Vec<DataType> {
    let mut l = raw_layout();
    l.extend_from_slice(extra);
    l
}

fn lit(v: Value, ty: DataType) -> Ir {
    Ir::new(ty, Span::default(), IrKind::Literal(v))
}

fn field(slot: SlotId, ty: DataType) -> Ir {
    Ir::new(ty, Span::default(), IrKind::Field(slot))
}

fn bin(op: BinOp, ty: DataType, l: Ir, r: Ir) -> Ir {
    Ir::new(
        ty,
        Span::default(),
        IrKind::Binary {
            op,
            lhs: Box::new(l),
            rhs: Box::new(r),
        },
    )
}

fn call(func: StdFunc, ty: DataType, args: Vec<Ir>) -> Ir {
    Ir::new(ty, Span::default(), IrKind::Call { func, args })
}

fn ternary(ty: DataType, c: Ir, t: Ir, e: Ir) -> Ir {
    Ir::new(
        ty,
        Span::default(),
        IrKind::Ternary {
            cond: Box::new(c),
            then: Box::new(t),
            else_: Box::new(e),
        },
    )
}

struct Env {
    pool: BufferPool,
    errors: ErrorSink,
}

impl Env {
    fn new() -> Env {
        Env {
            pool: BufferPool::new(),
            errors: ErrorSink::new(),
        }
    }

    fn run(&mut self, ir: &Ir, batch: &Batch) -> Column {
        let mut ctx = Ctx::new(&mut self.pool, &mut self.errors);
        eval(ir, batch, &mut ctx)
    }
}

fn put_i64(env: &mut Env, b: &mut Batch, slot: SlotId, vals: &[i64]) {
    let mut c = Column::alloc(&mut env.pool, &DataType::I64, vals.len());
    c.i64_mut().unwrap().values.copy_from_slice(vals);
    b.put(slot, c, &mut env.pool);
}

fn put_opt_i64(env: &mut Env, b: &mut Batch, slot: SlotId, vals: &[Option<i64>]) {
    let mut c = Column::alloc(&mut env.pool, &DataType::I64, vals.len());
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
    b.put(slot, c, &mut env.pool);
}

fn put_str(env: &mut Env, b: &mut Batch, slot: SlotId, vals: &[&str]) {
    let mut sb = StrBuilder::new(&mut env.pool, VarType::Str, vals.len());
    sb.reserve(&mut env.pool, vals.iter().map(|s| s.len()).sum());
    for s in vals {
        sb.push(s.as_bytes());
    }
    let c = sb.finish();
    b.put(slot, c, &mut env.pool);
}

fn put_bool(env: &mut Env, b: &mut Batch, slot: SlotId, vals: &[bool]) {
    let mut c = Column::alloc(&mut env.pool, &DataType::Bool, vals.len());
    {
        let bb = c.bool_mut().unwrap();
        for (i, v) in vals.iter().enumerate() {
            bits::set_row(bb.values, i, *v);
        }
    }
    b.put(slot, c, &mut env.pool);
}

fn strs(c: &Column) -> Vec<String> {
    let v = c.var().unwrap();
    (0..c.len())
        .map(|i| String::from_utf8_lossy(v.get(i)).into_owned())
        .collect()
}

// ---------------------------------------------------------------------------

#[test]
fn field_reference_reads_the_slot() {
    let mut env = Env::new();
    let mut b = Batch::new(&layout(&[DataType::I64]), 3, 0);
    put_i64(&mut env, &mut b, A, &[1, 2, 3]);

    let out = env.run(&field(A, DataType::I64), &b);
    assert_eq!(out.i64().unwrap().values, &[1, 2, 3]);
    out.reclaim(&mut env.pool);
    b.reclaim(&mut env.pool);
}

#[test]
fn literal_broadcasts_to_the_batch_length() {
    let mut env = Env::new();
    let b = Batch::new(&layout(&[]), 5, 0);
    let out = env.run(&lit(Value::I64(7), DataType::I64), &b);
    assert_eq!(out.i64().unwrap().values, &[7; 5]);
    out.reclaim(&mut env.pool);
    b.reclaim(&mut env.pool);
}

#[test]
fn null_literal_allocates_nothing() {
    let mut env = Env::new();
    let b = Batch::new(&layout(&[]), 100, 0);
    let out = env.run(&lit(Value::Null, DataType::I64), &b);
    assert!(out.is_unbound(), "null costs zero buffers");
    assert_eq!(out.len(), 100);
    out.reclaim(&mut env.pool);
    b.reclaim(&mut env.pool);
}

#[test]
fn arithmetic_over_a_field_and_a_literal() {
    // a * 2
    let mut env = Env::new();
    let mut b = Batch::new(&layout(&[DataType::I64]), 4, 0);
    put_i64(&mut env, &mut b, A, &[1, 2, 3, 4]);

    let ir = bin(
        BinOp::Mul,
        DataType::I64,
        field(A, DataType::I64),
        lit(Value::I64(2), DataType::I64),
    );
    let out = env.run(&ir, &b);
    assert_eq!(out.i64().unwrap().values, &[2, 4, 6, 8]);
    out.reclaim(&mut env.pool);
    b.reclaim(&mut env.pool);
}

#[test]
fn a_literal_operand_costs_no_rows() {
    // `a == 200` over 4096 rows. The kernel reads the literal as one value, so the pool pays for the Bool result and
    // nothing the size of an I64 column.
    let budget = smeltr::core::buffer::MemoryBudget::new(1 << 24);
    let mut env = Env::new();
    env.pool = BufferPool::with_budget(budget.clone());
    let rows = 4096;
    let mut b = Batch::new(&layout(&[DataType::I64]), rows, 0);
    put_i64(&mut env, &mut b, A, &(0..rows as i64).collect::<Vec<_>>());
    let before = budget.used();

    let ir = bin(
        BinOp::Eq,
        DataType::Bool,
        field(A, DataType::I64),
        lit(Value::I64(200), DataType::I64),
    );
    let out = env.run(&ir, &b);
    let paid = budget.used() - before;
    assert!(
        paid < rows * 8,
        "the pool paid {paid} bytes; a column of the literal would be {}",
        rows * 8
    );
    let bits = out.bool().unwrap();
    assert!(bits::get_row(bits.values, 200));
    assert!(!bits::get_row(bits.values, 201));
    out.reclaim(&mut env.pool);
    b.reclaim(&mut env.pool);
}

#[test]
fn a_kernel_outside_the_constant_set_still_gets_rows() {
    // `flags & 1`: the bitwise kernels read rows, so the dispatcher gives them rows for the literal.
    let mut env = Env::new();
    let mut b = Batch::new(&layout(&[DataType::I64]), 3, 0);
    put_i64(&mut env, &mut b, A, &[3, 6, 7]);
    let ir = bin(
        BinOp::BitAnd,
        DataType::I64,
        field(A, DataType::I64),
        lit(Value::I64(1), DataType::I64),
    );
    let out = env.run(&ir, &b);
    assert_eq!(out.i64().unwrap().values, &[1, 0, 1]);
    out.reclaim(&mut env.pool);
    b.reclaim(&mut env.pool);
}

#[test]
fn a_string_literal_compares_as_one_value() {
    let mut env = Env::new();
    let mut b = Batch::new(&layout(&[DataType::Str]), 3, 0);
    put_str(&mut env, &mut b, A, &["a", "b", "ab"]);
    let ir = bin(
        BinOp::Eq,
        DataType::Bool,
        field(A, DataType::Str),
        lit(Value::Str("b".into()), DataType::Str),
    );
    let out = env.run(&ir, &b);
    let bits = out.bool().unwrap();
    assert_eq!(
        (0..3).map(|i| bits::get_row(bits.values, i)).collect::<Vec<_>>(),
        [false, true, false]
    );
    out.reclaim(&mut env.pool);
    b.reclaim(&mut env.pool);
}

#[test]
fn a_literal_argument_costs_no_rows() {
    // `includes(s, "x")` over 4096 rows. The kernel reads the literal as one value, so the pool pays for the Bool
    // result and nothing the size of a Str column.
    let budget = smeltr::core::buffer::MemoryBudget::new(1 << 24);
    let mut env = Env::new();
    env.pool = BufferPool::with_budget(budget.clone());
    let rows = 4096;
    let mut b = Batch::new(&layout(&[DataType::Str]), rows, 0);
    let vals: Vec<&str> = (0..rows).map(|i| if i % 2 == 0 { "ax" } else { "a" }).collect();
    put_str(&mut env, &mut b, A, &vals);
    let before = budget.used();

    let ir = call(
        StdFunc::Includes,
        DataType::Bool,
        vec![field(A, DataType::Str), lit(Value::Str("x".into()), DataType::Str)],
    );
    let out = env.run(&ir, &b);
    let paid = budget.used() - before;
    assert!(
        paid < rows * 4,
        "the pool paid {paid} bytes; the offsets of a column of the literal alone would be {}",
        (rows + 1) * 4
    );
    let bits = out.bool().unwrap();
    assert!(bits::get_row(bits.values, 0));
    assert!(!bits::get_row(bits.values, 1));
    out.reclaim(&mut env.pool);
    b.reclaim(&mut env.pool);
}

#[test]
fn a_function_outside_the_constant_set_still_gets_rows() {
    // `repeat("-", n)`: the kernel reads its text as rows, so the dispatcher gives it rows for the literal.
    let mut env = Env::new();
    let mut b = Batch::new(&layout(&[DataType::I64]), 3, 0);
    put_i64(&mut env, &mut b, A, &[0, 2, 3]);
    let ir = call(
        StdFunc::Repeat,
        DataType::Str,
        vec![lit(Value::Str("-".into()), DataType::Str), field(A, DataType::I64)],
    );
    let out = env.run(&ir, &b);
    assert_eq!(strs(&out), ["", "--", "---"]);
    out.reclaim(&mut env.pool);
    b.reclaim(&mut env.pool);
}

#[test]
fn nested_expression_a_plus_b_times_2() {
    // The AST doc's own example: a + b * 2, with Mul as a child of Add.
    let mut env = Env::new();
    let mut b = Batch::new(&layout(&[DataType::I64, DataType::I64]), 3, 0);
    put_i64(&mut env, &mut b, A, &[100, 200, 300]);
    put_i64(&mut env, &mut b, B, &[1, 2, 3]);

    let ir = bin(
        BinOp::Add,
        DataType::I64,
        field(A, DataType::I64),
        bin(
            BinOp::Mul,
            DataType::I64,
            field(B, DataType::I64),
            lit(Value::I64(2), DataType::I64),
        ),
    );
    let out = env.run(&ir, &b);
    assert_eq!(out.i64().unwrap().values, &[102, 204, 306]);
    out.reclaim(&mut env.pool);
    b.reclaim(&mut env.pool);
}

#[test]
fn unary_negation() {
    let mut env = Env::new();
    let mut b = Batch::new(&layout(&[DataType::I64]), 3, 0);
    put_i64(&mut env, &mut b, A, &[1, -2, 3]);

    let ir = Ir::new(
        DataType::I64,
        Span::default(),
        IrKind::Unary {
            op: UnOp::Neg,
            operand: Box::new(field(A, DataType::I64)),
        },
    );
    let out = env.run(&ir, &b);
    assert_eq!(out.i64().unwrap().values, &[-1, 2, -3]);
    out.reclaim(&mut env.pool);
    b.reclaim(&mut env.pool);
}

#[test]
fn null_propagates_through_a_whole_tree() {
    let mut env = Env::new();
    let mut b = Batch::new(&layout(&[DataType::I64, DataType::I64]), 3, 0);
    put_opt_i64(&mut env, &mut b, A, &[Some(1), None, Some(3)]);
    put_i64(&mut env, &mut b, B, &[10, 10, 10]);

    let ir = bin(
        BinOp::Mul,
        DataType::I64,
        bin(
            BinOp::Add,
            DataType::I64,
            field(A, DataType::I64),
            field(B, DataType::I64),
        ),
        lit(Value::I64(2), DataType::I64),
    );
    let out = env.run(&ir, &b);
    let f = out.i64().unwrap();
    assert_eq!(f.values[0], 22);
    assert!(!f.validity.is_valid(1), "null survives two levels");
    assert_eq!(f.values[2], 26);
    out.reclaim(&mut env.pool);
    b.reclaim(&mut env.pool);
}

#[test]
fn unresolved_field_yields_null_not_a_crash() {
    // An Empty slot is what an unresolved reference compiles to. The whole
    // expression should evaluate to null without allocating.
    let mut env = Env::new();
    let mut b = Batch::new(&layout(&[DataType::I64, DataType::I64]), 3, 0);
    put_i64(&mut env, &mut b, A, &[1, 2, 3]);
    // Slot B is never written — it stays Empty.

    let ir = bin(
        BinOp::Add,
        DataType::I64,
        field(A, DataType::I64),
        field(B, DataType::I64),
    );
    let out = env.run(&ir, &b);
    assert!(out.is_unbound());
    assert!(out.has_nulls());
    out.reclaim(&mut env.pool);
    b.reclaim(&mut env.pool);
}

/// An `i64` sum that overflows is null on that row and marked `Overflow`; the
/// column stays `i64` and the other rows keep their values. The null flows up
/// through the outer Add like any other null.
#[test]
fn overflow_is_null_on_its_row_and_the_column_stays_i64() {
    let mut env = Env::new();
    let mut b = Batch::new(&layout(&[DataType::I64]), 2, 0);
    put_i64(&mut env, &mut b, A, &[i64::MAX, 1]);

    let ir = bin(
        BinOp::Add,
        DataType::I64,
        bin(
            BinOp::Add,
            DataType::I64,
            field(A, DataType::I64),
            lit(Value::I64(1), DataType::I64),
        ),
        lit(Value::I64(10), DataType::I64),
    );
    let out = env.run(&ir, &b);
    assert_eq!(out.data_type(), DataType::I64, "the column stays i64");
    let f = out.i64().unwrap();
    assert!(!f.validity.is_valid(0), "the overflowed row is null");
    assert_eq!(f.values[1], 12, "the other row keeps its value");
    let overflow: Vec<_> = env
        .errors
        .entries()
        .iter()
        .filter(|a| matches!(a.kind, ExceptionKind::Overflow))
        .collect();
    assert_eq!(overflow.len(), 1, "one Overflow exception");
    assert_eq!(overflow[0].row, 0);
    assert!(overflow[0].kind.caught());
    out.reclaim(&mut env.pool);
    b.reclaim(&mut env.pool);
}

// ---------------------------------------------------------------------------
// Ternary / select
// ---------------------------------------------------------------------------

#[test]
fn ternary_selects_per_row() {
    let mut env = Env::new();
    let mut b = Batch::new(&layout(&[DataType::I64, DataType::I64, DataType::Bool]), 4, 0);
    put_i64(&mut env, &mut b, A, &[1, 2, 3, 4]);
    put_i64(&mut env, &mut b, B, &[10, 20, 30, 40]);
    put_bool(&mut env, &mut b, C, &[true, false, true, false]);

    let ir = ternary(
        DataType::I64,
        field(C, DataType::Bool),
        field(A, DataType::I64),
        field(B, DataType::I64),
    );
    let out = env.run(&ir, &b);
    assert_eq!(out.i64().unwrap().values, &[1, 20, 3, 40]);
    out.reclaim(&mut env.pool);
    b.reclaim(&mut env.pool);
}

/// A side no row takes is not evaluated, and a side every row takes is the whole result. Either way the column and the
/// exceptions are what selecting per row gives. `then` overflows on row 0, so its exception shows which side row 0
/// took.
#[test]
fn a_ternary_gives_the_rows_each_took_whether_or_not_a_side_is_skipped() {
    for (cond, want, overflows) in [
        ([false, false], [Some(10), Some(20)], 0),
        ([true, true], [None, Some(2)], 1),
        ([true, false], [None, Some(20)], 1),
        ([false, true], [Some(10), Some(2)], 0),
    ] {
        let mut env = Env::new();
        let mut b = Batch::new(&layout(&[DataType::I64, DataType::I64, DataType::Bool]), 2, 0);
        put_i64(&mut env, &mut b, A, &[i64::MAX, 1]);
        put_i64(&mut env, &mut b, B, &[10, 20]);
        put_bool(&mut env, &mut b, C, &cond);
        let then = bin(
            BinOp::Add,
            DataType::I64,
            field(A, DataType::I64),
            lit(Value::I64(1), DataType::I64),
        );
        let ir = ternary(DataType::I64, field(C, DataType::Bool), then, field(B, DataType::I64));
        let out = env.run(&ir, &b);
        let f = out.i64().unwrap();
        let got: Vec<Option<i64>> = (0..2).map(|r| f.validity.is_valid(r).then(|| f.values[r])).collect();
        assert_eq!(got, want, "{cond:?}");
        let overflow = env
            .errors
            .entries()
            .iter()
            .filter(|a| matches!(a.kind, ExceptionKind::Overflow))
            .count();
        assert_eq!(
            overflow, overflows,
            "{cond:?}: only a row that took `then` keeps its overflow"
        );
        out.reclaim(&mut env.pool);
        b.reclaim(&mut env.pool);
    }
}

/// A side every row takes that holds nothing, a `null` literal, is the blend's type rather than `null`'s: a reader of
/// the slot dispatches on it.
#[test]
fn a_skipped_blend_of_null_keeps_the_blends_type() {
    let mut env = Env::new();
    let mut b = Batch::new(&layout(&[DataType::Str, DataType::Bool]), 2, 0);
    put_str(&mut env, &mut b, A, &["a", "b"]);
    put_bool(&mut env, &mut b, B, &[false, false]);
    let ir = ternary(
        DataType::Str,
        field(B, DataType::Bool),
        field(A, DataType::Str),
        lit(Value::Null, DataType::Null),
    );
    let out = env.run(&ir, &b);
    assert_eq!(out.data_type(), DataType::Str);
    assert!(out.is_unbound(), "both rows are null");
    out.reclaim(&mut env.pool);
    b.reclaim(&mut env.pool);
}

/// One branch of a blend: the type its slot declares, and the values it carries.
/// `Absent` is a batch that supplied none -- an unbound column that still
/// declares a type.
#[derive(Clone)]
enum Branch {
    I64(&'static [i64]),
    Str(&'static [&'static str]),
    Absent(DataType),
}

impl Branch {
    fn ty(&self) -> DataType {
        match self {
            Branch::I64(_) => DataType::I64,
            Branch::Str(_) => DataType::Str,
            Branch::Absent(t) => t.clone(),
        }
    }
}

fn put_branch(env: &mut Env, b: &mut Batch, slot: SlotId, br: &Branch) {
    match br {
        Branch::I64(v) => put_i64(env, b, slot, v),
        Branch::Str(v) => put_str(env, b, slot, v),
        Branch::Absent(t) => b.put(slot, Column::empty(t.clone(), 2), &mut env.pool),
    }
}

/// `cond ? then : else_` over two rows with `cond` = `[true, false]`, so row 0
/// takes `then` and row 1 takes `else_`. `node` is the type the compiler gave
/// the node, which answers only when neither branch carries data.
fn blend(env: &mut Env, then: Branch, else_: Branch, node: DataType) -> Column {
    let (ta, tb) = (then.ty(), else_.ty());
    let mut b = Batch::new(&layout(&[ta.clone(), tb.clone(), DataType::Bool]), 2, 0);
    put_branch(env, &mut b, A, &then);
    put_branch(env, &mut b, B, &else_);
    put_bool(env, &mut b, C, &[true, false]);
    let ir = ternary(node, field(C, DataType::Bool), field(A, ta), field(B, tb));
    let out = env.run(&ir, &b);
    b.reclaim(&mut env.pool);
    out
}

/// The design doc's Language coercion table, asked across a blend's branches rather than one
/// column's rows: equal types stay, and anything else is a variant. A dynamic slot's type arrives with the data, so
/// the branches decide -- not the type the compiler pinned on the node.
#[test]
fn a_blend_meets_at_the_coercion_table() {
    let mut env = Env::new();

    // single tag -> that type
    let out = blend(&mut env, Branch::I64(&[10, 20]), Branch::I64(&[30, 40]), DataType::I64);
    assert_eq!(out.data_type(), DataType::I64);
    assert_eq!(out.i64().unwrap().values, &[10, 40]);
    out.reclaim(&mut env.pool);

    // anything else -> a variant, whichever side each arrives on: each row keeps its own value
    use smeltr::format::tag::{Tag, decode};
    for (then, else_, want) in [
        (
            Branch::Str(&["a", "b"]),
            Branch::I64(&[10, 20]),
            [Tag::Str("a".into()), Tag::I64(20)],
        ),
        (
            Branch::I64(&[10, 20]),
            Branch::Str(&["a", "b"]),
            [Tag::I64(10), Tag::Str("b".into())],
        ),
    ] {
        let out = blend(&mut env, then, else_, DataType::Str);
        assert_eq!(out.data_type(), DataType::Variant);
        let v = out.variant().unwrap();
        assert_eq!([decode(v.get(0)), decode(v.get(1))], want.map(Some));
        out.reclaim(&mut env.pool);
    }
    assert!(env.errors.entries().is_empty(), "nothing is lost, so nothing is noted");

    // no branch carries data -> the type the branches declare
    let out = blend(
        &mut env,
        Branch::Absent(DataType::I64),
        Branch::Absent(DataType::I64),
        DataType::I64,
    );
    assert_eq!(out.data_type(), DataType::I64);
    out.reclaim(&mut env.pool);
}

#[test]
fn ternary_over_strings_rebuilds() {
    let mut env = Env::new();
    let mut b = Batch::new(&layout(&[DataType::Str, DataType::Str, DataType::Bool]), 3, 0);
    put_str(&mut env, &mut b, A, &["yes", "yes", "yes"]);
    put_str(&mut env, &mut b, B, &["no", "no", "no"]);
    put_bool(&mut env, &mut b, C, &[true, false, true]);

    let ir = ternary(
        DataType::Str,
        field(C, DataType::Bool),
        field(A, DataType::Str),
        field(B, DataType::Str),
    );
    let out = env.run(&ir, &b);
    assert_eq!(strs(&out), vec!["yes", "no", "yes"]);
    out.reclaim(&mut env.pool);
    b.reclaim(&mut env.pool);
}

#[test]
fn a_null_condition_takes_the_else_branch() {
    let mut env = Env::new();
    let mut b = Batch::new(&layout(&[DataType::I64, DataType::I64, DataType::Bool]), 2, 0);
    put_i64(&mut env, &mut b, A, &[1, 2]);
    put_i64(&mut env, &mut b, B, &[10, 20]);
    let mut cond = Column::alloc(&mut env.pool, &DataType::Bool, 2);
    {
        let bb = cond.bool_mut().unwrap();
        bits::set_row(bb.values, 0, true);
        bb.validity.set_valid(1, false); // row 1's condition is null
    }
    b.put(C, cond, &mut env.pool);

    let ir = ternary(
        DataType::I64,
        field(C, DataType::Bool),
        field(A, DataType::I64),
        field(B, DataType::I64),
    );
    let out = env.run(&ir, &b);
    let f = out.i64().unwrap();
    assert_eq!(f.values[0], 1);
    assert_eq!(f.values[1], 20, "a null condition is false");
    assert!(f.validity.is_valid(1));
    out.reclaim(&mut env.pool);
    b.reclaim(&mut env.pool);
}

// ---------------------------------------------------------------------------
// N-ary
// ---------------------------------------------------------------------------

#[test]
fn flattened_concat_evaluates_end_to_end() {
    // first + " " + last, flattened to ConcatN, evaluated in one kernel call.
    let mut env = Env::new();
    let mut b = Batch::new(&layout(&[DataType::Str, DataType::Str]), 2, 0);
    put_str(&mut env, &mut b, A, &["John", "Jane"]);
    put_str(&mut env, &mut b, B, &["Smith", "Doe"]);

    let ir = bin(
        BinOp::Add,
        DataType::Str,
        bin(
            BinOp::Add,
            DataType::Str,
            field(A, DataType::Str),
            lit(Value::Str(" ".into()), DataType::Str),
        ),
        field(B, DataType::Str),
    );
    let flat = flatten(ir);
    assert!(matches!(flat.kind, IrKind::ConcatN(ref v) if v.len() == 3));

    let out = env.run(&flat, &b);
    assert_eq!(strs(&out), vec!["John Smith", "Jane Doe"]);
    out.reclaim(&mut env.pool);
    b.reclaim(&mut env.pool);
}

#[test]
fn flattened_and_evaluates_end_to_end() {
    let mut env = Env::new();
    let mut b = Batch::new(&layout(&[DataType::Bool, DataType::Bool, DataType::Bool]), 4, 0);
    put_bool(&mut env, &mut b, A, &[true, true, true, false]);
    put_bool(&mut env, &mut b, B, &[true, true, false, true]);
    put_bool(&mut env, &mut b, C, &[true, false, true, true]);

    let ir = bin(
        BinOp::And,
        DataType::Bool,
        bin(
            BinOp::And,
            DataType::Bool,
            field(A, DataType::Bool),
            field(B, DataType::Bool),
        ),
        field(C, DataType::Bool),
    );
    let flat = flatten(ir);
    assert!(matches!(flat.kind, IrKind::AndN(ref v) if v.len() == 3));

    let out = env.run(&flat, &b);
    let bb = out.bool().unwrap();
    assert!(bits::get_row(bb.values, 0));
    assert!(!bits::get_row(bb.values, 1));
    assert_eq!(bits::popcount(bb.values), 1);
    out.reclaim(&mut env.pool);
    b.reclaim(&mut env.pool);
}

#[test]
fn coalesce_chain_picks_first_non_null() {
    let mut env = Env::new();
    let mut b = Batch::new(&layout(&[DataType::I64, DataType::I64]), 3, 0);
    put_opt_i64(&mut env, &mut b, A, &[Some(1), None, None]);
    put_opt_i64(&mut env, &mut b, B, &[Some(9), Some(2), None]);

    let ir = bin(
        BinOp::Coalesce,
        DataType::I64,
        bin(
            BinOp::Coalesce,
            DataType::I64,
            field(A, DataType::I64),
            field(B, DataType::I64),
        ),
        lit(Value::I64(-1), DataType::I64),
    );
    let flat = flatten(ir);
    assert!(matches!(flat.kind, IrKind::CoalesceN(ref v) if v.len() == 3));

    let out = env.run(&flat, &b);
    assert_eq!(out.i64().unwrap().values, &[1, 2, -1]);
    out.reclaim(&mut env.pool);
    b.reclaim(&mut env.pool);
}

/// A pool over a budget that counts what the pool takes from the system. A column the pool has taken back is handed
/// out again for nothing, so each cost test has a pool of its own.
fn budgeted() -> (smeltr::core::buffer::MemoryBudget, Env) {
    let budget = smeltr::core::buffer::MemoryBudget::new(1 << 24);
    let mut env = Env::new();
    env.pool = BufferPool::with_budget(budget.clone());
    (budget, env)
}

#[test]
fn a_literal_in_a_concat_chain_costs_no_rows() {
    // `"p" + s` over 4096 rows. The kernel reads the literal as one value, so the pool pays for the result and
    // nothing the size of a column of the literal.
    let (budget, mut env) = budgeted();
    let rows = 4096;
    let mut b = Batch::new(&layout(&[DataType::Str]), rows, 0);
    let vals: Vec<&str> = (0..rows).map(|i| if i % 2 == 0 { "a" } else { "" }).collect();
    put_str(&mut env, &mut b, A, &vals);

    let ir = flatten(bin(
        BinOp::Add,
        DataType::Str,
        lit(Value::Str("p".into()), DataType::Str),
        field(A, DataType::Str),
    ));
    assert!(matches!(ir.kind, IrKind::ConcatN(_)));
    let before = budget.used();
    let out = env.run(&ir, &b);
    let paid = budget.used() - before;
    assert!(
        paid < rows * 12,
        "the pool paid {paid} bytes; the offsets of a column of the literal alone would be {}",
        (rows + 1) * 4
    );
    assert_eq!(strs(&out)[..2], ["pa", "p"]);
    out.reclaim(&mut env.pool);
    b.reclaim(&mut env.pool);
}

#[test]
fn a_literal_in_a_coalesce_chain_costs_no_rows() {
    // `n ?? 0` over 4096 rows: the pool pays for the result, whose values alone are as much as a column of the
    // literal would be.
    let (budget, mut env) = budgeted();
    let rows = 4096;
    let mut b = Batch::new(&layout(&[DataType::I64]), rows, 0);
    let nums: Vec<Option<i64>> = (0..rows).map(|i| (i % 2 == 0).then_some(i as i64 + 1)).collect();
    put_opt_i64(&mut env, &mut b, A, &nums);

    let ir = flatten(bin(
        BinOp::Coalesce,
        DataType::I64,
        field(A, DataType::I64),
        lit(Value::I64(0), DataType::I64),
    ));
    assert!(matches!(ir.kind, IrKind::CoalesceN(_)));
    let before = budget.used();
    let out = env.run(&ir, &b);
    let paid = budget.used() - before;
    assert!(
        paid < rows * 12,
        "the pool paid {paid} bytes; the result's values alone are {}",
        rows * 8
    );
    assert_eq!(out.i64().unwrap().values[..2], [1, 0]);
    out.reclaim(&mut env.pool);
    b.reclaim(&mut env.pool);
}

/// A list over `child` whose rows have the given lengths, `None` for a null row.
fn list_over(env: &mut Env, child: Column, rows: &[Option<u32>]) -> Column {
    let mut offsets = env.pool.take((rows.len() + 1) * 4);
    let mut acc = vec![0u32];
    let mut validity = Validity::alloc(&mut env.pool, rows.len());
    for (i, r) in rows.iter().enumerate() {
        acc.push(acc[i] + r.unwrap_or(0));
        if r.is_none() {
            validity.set_valid(i, false);
        }
    }
    offsets.as_mut_slice::<u32>().copy_from_slice(&acc);
    Column::list_from_parts(offsets, child, validity, rows.len())
}

fn i64_lists(env: &mut Env, rows: &[Option<&[i64]>]) -> Column {
    let flat: Vec<i64> = rows.iter().flatten().flat_map(|r| r.iter().copied()).collect();
    let mut child = Column::alloc(&mut env.pool, &DataType::I64, flat.len());
    child.i64_mut().unwrap().values.copy_from_slice(&flat);
    let lens: Vec<Option<u32>> = rows.iter().map(|r| r.map(|r| r.len() as u32)).collect();
    list_over(env, child, &lens)
}

fn put_i64_lists(env: &mut Env, b: &mut Batch, slot: SlotId, rows: &[Option<&[i64]>]) {
    let col = i64_lists(env, rows);
    b.put(slot, col, &mut env.pool);
}

fn str_lists(env: &mut Env, rows: &[Option<&[&str]>]) -> Column {
    let flat: Vec<&str> = rows.iter().flatten().flat_map(|r| r.iter().copied()).collect();
    let mut sb = StrBuilder::new(&mut env.pool, VarType::Str, flat.len());
    sb.reserve(&mut env.pool, flat.iter().map(|s| s.len()).sum());
    for s in &flat {
        sb.push(s.as_bytes());
    }
    let child = sb.finish();
    let lens: Vec<Option<u32>> = rows.iter().map(|r| r.map(|r| r.len() as u32)).collect();
    list_over(env, child, &lens)
}

#[test]
fn a_ternary_over_nested_arrays_blends_per_row() {
    // Two arrays of arrays, and each row takes one side's whole array, its inner arrays included.
    let mut env = Env::new();
    let ty = DataType::Array(Box::new(DataType::Array(Box::new(DataType::I64))));
    let mut b = Batch::new(&layout(&[ty.clone(), ty.clone(), DataType::Bool]), 2, 0);
    let inner = i64_lists(&mut env, &[Some(&[1, 2]), Some(&[3]), Some(&[4])]);
    let outer = list_over(&mut env, inner, &[Some(2), Some(1)]);
    b.put(A, outer, &mut env.pool);
    let inner = i64_lists(&mut env, &[Some(&[9]), Some(&[8]), Some(&[7])]);
    let outer = list_over(&mut env, inner, &[Some(1), Some(2)]);
    b.put(B, outer, &mut env.pool);
    put_bool(&mut env, &mut b, C, &[true, false]);

    let ir = ternary(
        ty.clone(),
        field(C, DataType::Bool),
        field(A, ty.clone()),
        field(B, ty.clone()),
    );
    let out = env.run(&ir, &b);
    assert_eq!(out.data_type(), ty);
    let rows = out.list().unwrap();
    assert_eq!(rows.offsets, &[0, 2, 4]);
    let inner = rows.child.list().unwrap();
    assert_eq!(inner.offsets, &[0, 2, 3, 4, 5]);
    assert_eq!(inner.child.i64().unwrap().values, &[1, 2, 3, 8, 7]);
    out.reclaim(&mut env.pool);
    b.reclaim(&mut env.pool);
}

#[test]
fn a_ternary_over_arrays_whose_elements_cannot_widen_meets_as_a_variant() {
    // A declared `array<str>` against arrays of arrays: the elements cannot widen, so the whole blend meets as a
    // variant, as `??` does, each row keeping its own array. Blending the elements alone read a list child as text
    // and panicked.
    let mut env = Env::new();
    let flat = DataType::Array(Box::new(DataType::Str));
    let mut b = Batch::new(&layout(&[flat.clone(), DataType::I64, DataType::Bool]), 2, 0);
    let xs = str_lists(&mut env, &[Some(&["a"]), Some(&["b"])]);
    b.put(A, xs, &mut env.pool);
    let inner = i64_lists(&mut env, &[Some(&[1, 2]), Some(&[3])]);
    let d = list_over(&mut env, inner, &[Some(1), Some(1)]);
    b.put(B, d, &mut env.pool);
    put_bool(&mut env, &mut b, C, &[true, false]);

    let ir = ternary(
        flat.clone(),
        field(C, DataType::Bool),
        field(A, flat),
        field(B, DataType::I64),
    );
    let out = env.run(&ir, &b);
    assert_eq!(out.data_type(), DataType::Variant);
    use smeltr::format::tag::{Tag, decode};
    let v = out.variant().unwrap();
    assert_eq!(decode(v.get(0)), Some(Tag::Array(vec![Some(Tag::Str("a".into()))])));
    assert_eq!(
        decode(v.get(1)),
        Some(Tag::Array(vec![Some(Tag::Array(vec![Some(Tag::I64(3))]))]))
    );
    out.reclaim(&mut env.pool);
    b.reclaim(&mut env.pool);
}

#[test]
fn coalesce_over_array_fields_takes_a_whole_array_per_row() {
    // `xs ?? ys` on two array fields: a row is the first operand's whole array, null where both are.
    let mut env = Env::new();
    let ty = DataType::Array(Box::new(DataType::I64));
    let mut b = Batch::new(&layout(&[ty.clone(), ty.clone()]), 3, 0);
    put_i64_lists(&mut env, &mut b, A, &[Some(&[1, 2]), None, None]);
    put_i64_lists(&mut env, &mut b, B, &[Some(&[9]), Some(&[3]), None]);

    let ir = flatten(bin(
        BinOp::Coalesce,
        ty.clone(),
        field(A, ty.clone()),
        field(B, ty.clone()),
    ));
    let out = env.run(&ir, &b);
    assert_eq!(out.data_type(), ty);
    let l = out.list().unwrap();
    assert_eq!(l.offsets, &[0, 2, 3, 3]);
    assert_eq!(l.child.i64().unwrap().values, &[1, 2, 3]);
    assert!(l.validity.is_valid(1));
    assert!(!l.validity.is_valid(2));
    out.reclaim(&mut env.pool);
    b.reclaim(&mut env.pool);
}

#[test]
fn comparison_feeds_a_ternary() {
    // a > 2 ? a : 0 — the shape a Drop condition or a guard takes.
    let mut env = Env::new();
    let mut b = Batch::new(&layout(&[DataType::I64]), 4, 0);
    put_i64(&mut env, &mut b, A, &[1, 2, 3, 4]);

    let cond = bin(
        BinOp::Gt,
        DataType::Bool,
        field(A, DataType::I64),
        lit(Value::I64(2), DataType::I64),
    );
    let ir = ternary(
        DataType::I64,
        cond,
        field(A, DataType::I64),
        lit(Value::I64(0), DataType::I64),
    );
    let out = env.run(&ir, &b);
    assert_eq!(out.i64().unwrap().values, &[0, 0, 3, 4]);
    out.reclaim(&mut env.pool);
    b.reclaim(&mut env.pool);
}

#[test]
fn bare_field_result_is_copied_not_aliased() {
    // `x = y` must produce an independent column; mutating the result must not
    // disturb slot y.
    let mut env = Env::new();
    let mut b = Batch::new(&layout(&[DataType::I64]), 3, 0);
    put_i64(&mut env, &mut b, A, &[1, 2, 3]);

    let mut out = env.run(&field(A, DataType::I64), &b);
    out.i64_mut().unwrap().values[0] = 999;
    assert_eq!(b.get(A).i64().unwrap().values, &[1, 2, 3], "source untouched");
    assert_eq!(out.i64().unwrap().values[0], 999);
    out.reclaim(&mut env.pool);
    b.reclaim(&mut env.pool);
}

// ---------------------------------------------------------------------------
// Array / Object / Index (expression-language gaps, pass 2)
// ---------------------------------------------------------------------------

fn array_lit(elem_ty: DataType, items: Vec<Ir>) -> Ir {
    Ir::new(
        DataType::Array(Box::new(elem_ty)),
        Span::default(),
        IrKind::Array(items),
    )
}

fn index_ir(ty: DataType, base: Ir, index: Ir) -> Ir {
    Ir::new(
        ty,
        Span::default(),
        IrKind::Index {
            base: Box::new(base),
            index: Box::new(index),
        },
    )
}

/// Read a List column's flat child buffer as `Option<i64>`, whole-column
/// (row `r`'s elements are `[r*n, r*n+n)`, per `resolve_array`'s fixed
/// arity — tests slice into this themselves rather than a per-row helper).
fn list_child_i64(c: &Column) -> Vec<Option<i64>> {
    let l = c.list().unwrap();
    let ch = l.child.i64().unwrap();
    (0..l.child.len())
        .map(|i| ch.validity.is_valid(i).then_some(ch.values[i]))
        .collect()
}

#[test]
fn array_literal_with_a_null_element_is_null_at_that_slot_only() {
    // [a, null]
    let mut env = Env::new();
    let mut b = Batch::new(&layout(&[DataType::I64]), 2, 0);
    put_i64(&mut env, &mut b, A, &[1, 10]);

    let ir = array_lit(
        DataType::I64,
        vec![field(A, DataType::I64), lit(Value::Null, DataType::I64)],
    );
    let out = env.run(&ir, &b);
    assert_eq!(
        list_child_i64(&out),
        vec![Some(1), None, Some(10), None],
        "the null literal contributes null at its own slot in every row"
    );
    out.reclaim(&mut env.pool);
    b.reclaim(&mut env.pool);
}

#[test]
fn array_index_reads_the_matching_element() {
    // [a, b, c][idx]
    let mut env = Env::new();
    let mut b = Batch::new(
        &layout(&[DataType::I64, DataType::I64, DataType::I64, DataType::I64]),
        3,
        0,
    );
    put_i64(&mut env, &mut b, A, &[1, 1, 1]);
    put_i64(&mut env, &mut b, B, &[2, 2, 2]);
    put_i64(&mut env, &mut b, C, &[3, 3, 3]);
    let idx_slot = RAW_SLOTS + 3;
    put_i64(&mut env, &mut b, idx_slot, &[0, 1, 2]);

    let arr = array_lit(
        DataType::I64,
        vec![
            field(A, DataType::I64),
            field(B, DataType::I64),
            field(C, DataType::I64),
        ],
    );
    let ir = index_ir(DataType::I64, arr, field(idx_slot, DataType::I64));
    let out = env.run(&ir, &b);
    assert_eq!(out.i64().unwrap().values, &[1, 2, 3]);
    out.reclaim(&mut env.pool);
    b.reclaim(&mut env.pool);
}

/// This path serves only a declared array of objects now -- everything else `[...]` reaches lowers to `at` -- so what
/// it must do is answer the way `at` does: count a negative index from the end, and read null past either end.
#[test]
fn array_index_counts_a_negative_from_the_end_and_is_null_past_either_end() {
    let mut env = Env::new();
    let mut b = Batch::new(&layout(&[DataType::I64, DataType::I64, DataType::I64]), 4, 0);
    put_i64(&mut env, &mut b, A, &[1, 1, 1, 1]);
    put_i64(&mut env, &mut b, B, &[2, 2, 2, 2]);
    put_i64(&mut env, &mut b, C, &[99, -1, -2, -3]); // out of range, last, first, past the front

    let arr = array_lit(
        DataType::I64,
        vec![field(A, DataType::I64), field(B, DataType::I64)], // len 2
    );
    let ir = index_ir(DataType::I64, arr, field(C, DataType::I64));
    let out = env.run(&ir, &b);
    let f = out.i64().unwrap();
    assert!(!f.validity.is_valid(0), "index 99 is past the end");
    assert_eq!(f.values[1], 2, "-1 is the last element");
    assert_eq!(f.values[2], 1, "-2 is the first");
    assert!(!f.validity.is_valid(3), "-3 is past the front of a two-element array");
    out.reclaim(&mut env.pool);
    b.reclaim(&mut env.pool);
}

#[test]
fn array_index_propagates_null_from_a_null_index() {
    let mut env = Env::new();
    let mut b = Batch::new(&layout(&[DataType::I64, DataType::I64]), 2, 0);
    put_i64(&mut env, &mut b, A, &[7, 7]);
    put_opt_i64(&mut env, &mut b, B, &[Some(0), None]);

    let arr = array_lit(DataType::I64, vec![field(A, DataType::I64)]);
    let ir = index_ir(DataType::I64, arr, field(B, DataType::I64));
    let out = env.run(&ir, &b);
    let f = out.i64().unwrap();
    assert_eq!(f.values[0], 7);
    assert!(f.validity.is_valid(0));
    assert!(!f.validity.is_valid(1), "a null index reads as null");
    out.reclaim(&mut env.pool);
    b.reclaim(&mut env.pool);
}

#[test]
fn bare_field_reference_to_a_struct_column_is_deep_copied() {
    // `x = y` where `y` is a Struct-typed slot must not alias its buffers —
    // this is what `Column::deep_copy`'s List/Struct arm exists for (before
    // it, this was `unimplemented!()` and this whole test would panic).
    let mut env = Env::new();
    let struct_ty = DataType::Struct(vec![("x".to_string(), DataType::I64)], false);
    let mut b = Batch::new(&layout(std::slice::from_ref(&struct_ty)), 2, 0);
    let mut inner = Column::alloc(&mut env.pool, &DataType::I64, 2);
    inner.i64_mut().unwrap().values.copy_from_slice(&[1, 2]);
    let s = Column::struct_from_parts(vec![("x".to_string(), inner)], 2);
    b.put(A, s, &mut env.pool);

    let out = env.run(&field(A, struct_ty), &b);
    assert_eq!(out.field("x").unwrap().i64().unwrap().values, &[1, 2]);

    // Prove independence: reclaim the copy, then immediately re-take a
    // buffer of the exact same size and stomp it. If `deep_copy` had
    // aliased `b`'s buffers instead of copying them, this would corrupt
    // `b`'s own field.
    out.reclaim(&mut env.pool);
    let mut stomp = env.pool.take(2 * 8);
    stomp.as_mut_slice::<i64>()[..2].copy_from_slice(&[777, 888]);
    env.pool.give(stomp);
    assert_eq!(
        b.get(A).field("x").unwrap().i64().unwrap().values,
        &[1, 2],
        "source untouched by reclaiming/reusing the copy's buffers"
    );
    b.reclaim(&mut env.pool);
}

#[test]
fn large_batch_crosses_word_boundaries() {
    let mut env = Env::new();
    let n = 1000;
    let mut b = Batch::new(&layout(&[DataType::I64]), n, 0);
    let vals: Vec<i64> = (0..n as i64).collect();
    put_i64(&mut env, &mut b, A, &vals);

    let ir = bin(
        BinOp::Lt,
        DataType::Bool,
        field(A, DataType::I64),
        lit(Value::I64(500), DataType::I64),
    );
    let out = env.run(&ir, &b);
    assert_eq!(bits::popcount(out.bool().unwrap().values), 500);
    out.reclaim(&mut env.pool);
    b.reclaim(&mut env.pool);
}

/// A bare literal stays one value only where the caller says nothing will read the slot. Anywhere else -- the slot
/// has a reader, or a kernel around the literal indexes rows -- it is rows, because every accessor refuses an
/// unexpanded constant.
#[test]
fn a_literal_keeps_its_value_only_for_an_output_only_slot() {
    let mut env = Env::new();
    let mut b = Batch::new(&layout(&[DataType::Str]), 4, 0);
    put_str(&mut env, &mut b, A, &["w", "x", "y", "z"]);
    let text = lit(Value::Str("panw".into()), DataType::Str);

    let kept = {
        let mut ctx = Ctx::new(&mut env.pool, &mut env.errors);
        eval_for_slot(&text, &b, &mut ctx, true)
    };
    assert_eq!(kept.constant_value(), Some(&Scalar::Str("panw".into())));
    assert_eq!(kept.len(), 4);
    assert!(kept.is_valid(3) && !kept.has_nulls());

    let rows = {
        let mut ctx = Ctx::new(&mut env.pool, &mut env.errors);
        eval_for_slot(&text, &b, &mut ctx, false)
    };
    assert_eq!(rows.constant_value(), None, "a slot with a reader gets rows");
    assert_eq!(rows.var().unwrap().get(3), b"panw");

    // Joined to a field it is the kernel's, and the kernel indexes rows. The caller's word does not reach it.
    let joined = flatten(bin(BinOp::Add, DataType::Str, text.clone(), field(A, DataType::Str)));
    let inside = {
        let mut ctx = Ctx::new(&mut env.pool, &mut env.errors);
        eval_for_slot(&joined, &b, &mut ctx, true)
    };
    assert_eq!(inside.constant_value(), None);
    assert_eq!(inside.var().unwrap().get(1), b"panwx");

    // Joined to another literal it never reaches a kernel: `flatten` folded the two before this was asked.
    let both = flatten(bin(BinOp::Add, DataType::Str, text.clone(), text.clone()));
    let folded = {
        let mut ctx = Ctx::new(&mut env.pool, &mut env.errors);
        eval_for_slot(&both, &b, &mut ctx, true)
    };
    assert_eq!(folded.constant_value(), Some(&Scalar::Str("panwpanw".into())));

    for c in [kept, rows, inside, folded] {
        c.reclaim(&mut env.pool);
    }
    b.reclaim(&mut env.pool);
}

/// Two literal operands fold to one literal, and the literal is what the kernel would have computed. Overflow does
/// not fold: the kernel promotes the column to F64 there, and a literal carrying the node's own I64 type cannot
/// promise that.
#[test]
fn folding_two_literals_gives_what_the_kernel_gives() {
    let mut env = Env::new();
    let b = Batch::new(&layout(&[]), 3, 0);
    let int = |x: i64| lit(Value::I64(x), DataType::I64);

    for (op, x, y) in [(BinOp::Add, 2i64, 3i64), (BinOp::Sub, 2, 3), (BinOp::Mul, 1000, 60)] {
        let raw = bin(op, DataType::I64, int(x), int(y));
        let folded = flatten(raw.clone());
        assert!(matches!(folded.kind, IrKind::Literal(_)), "{op:?} {x} {y} folds");
        let kernel = env.run(&raw, &b);
        let constant = env.run(&folded, &b);
        assert_eq!(
            kernel.i64().unwrap().values,
            constant.i64().unwrap().values,
            "{op:?} {x} {y}"
        );
        kernel.reclaim(&mut env.pool);
        constant.reclaim(&mut env.pool);
    }

    for (op, x, y) in [(BinOp::Add, i64::MAX, 1i64), (BinOp::Mul, i64::MIN, -1)] {
        let folded = flatten(bin(op, DataType::I64, int(x), int(y)));
        assert!(
            !matches!(folded.kind, IrKind::Literal(_)),
            "{op:?} {x} {y} overflows, so it stays the kernel's"
        );
    }

    // Text joins end to end, the whole chain at once.
    let text = |t: &str| lit(Value::Str(t.into()), DataType::Str);
    let raw = bin(
        BinOp::Add,
        DataType::Str,
        bin(BinOp::Add, DataType::Str, text("a"), text("b")),
        text("c"),
    );
    let folded = flatten(raw.clone());
    assert!(matches!(&folded.kind, IrKind::Literal(Value::Str(t)) if t == "abc"));
    let kernel = env.run(&raw, &b);
    let constant = env.run(&folded, &b);
    assert_eq!(constant.var().unwrap().get(2), b"abc");
    assert_eq!(kernel.var().unwrap().get(2), constant.var().unwrap().get(2));
    kernel.reclaim(&mut env.pool);
    constant.reclaim(&mut env.pool);
    b.reclaim(&mut env.pool);
}
