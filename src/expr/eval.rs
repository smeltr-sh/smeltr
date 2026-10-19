//! The expression evaluator: a tree walk over whole columns.
//!
//! Each node returns a column and its children are reclaimed as the recursion unwinds, so the pool's high-water mark is
//! bounded by tree depth. A bare `Field` borrows the batch's column (`Val::Ref`); only the top-level `eval`
//! materializes.
//!
//! Binary operators dispatch on the children's *runtime* types, not the `Ir` type: an I64 kernel that overflowed
//! returns F64 and the parent must pick the F64 kernel.

use crate::core::batch::Batch;
use crate::core::bits;
use crate::core::column::{Column, DataType, Scalar, StrBuilder, VarType};
use crate::core::ctx::Ctx;
use crate::core::error::Read;
use crate::expr::ast::Value;
use crate::expr::ir::{Ir, IrKind, Layer};
use crate::ops::arrayops::{Piece, build_from_pieces};
use crate::ops::logic::take_met;
use crate::ops::objectops::ObjectOp;
use crate::ops::{BinOp, UnOp, convert, table};

/// A borrow of a batch column or an owned intermediate.
pub enum Val<'a> {
    Ref(&'a Column),
    Owned(Column),
}

impl<'a> Val<'a> {
    pub fn as_col(&self) -> &Column {
        match self {
            Val::Ref(c) => c,
            Val::Owned(c) => c,
        }
    }

    /// Return an owned intermediate to the pool; a borrow belongs to the batch.
    pub fn release(self, ctx: &mut Ctx) {
        if let Val::Owned(c) = self {
            c.reclaim(ctx.pool);
        }
    }

    fn into_owned(self, ctx: &mut Ctx) -> Column {
        match self {
            Val::Owned(c) => c,
            Val::Ref(c) => c.deep_copy(ctx.pool),
        }
    }
}

/// Evaluate `ir` over `batch`, producing an owned column with rows.
pub fn eval(ir: &Ir, batch: &Batch, ctx: &mut Ctx) -> Column {
    eval_for_slot(ir, batch, ctx, false)
}

/// `eval`, for a caller that knows the serializer is the only reader of the slot this lands in. A bare literal then
/// stays a constant column, because the serializer writes the value and never asks for rows. `null` is not one: an
/// all-null column is `Empty`.
///
/// `keep_constant` is `Instr::constant_ok`. False is always correct, and costs what a literal has always cost.
pub fn eval_for_slot(ir: &Ir, batch: &Batch, ctx: &mut Ctx, keep_constant: bool) -> Column {
    if keep_constant
        && let IrKind::Literal(v) = &ir.kind
        && let Some(scalar) = scalar_of(v)
    {
        return Column::constant(scalar, batch.rows());
    }
    let v = eval_val(ir, batch, ctx);
    v.into_owned(ctx)
}

/// `ir`'s value. Wherever the compiler typed it a variant, it is a variant column, whatever its node built: a
/// variant's column never depends on what the batch happened to hold.
fn eval_val<'a>(ir: &Ir, batch: &'a Batch, ctx: &mut Ctx) -> Val<'a> {
    let v = eval_node(ir, batch, ctx);
    if ir.ty != DataType::Variant || v.as_col().data_type() == DataType::Variant {
        return v;
    }
    let out = match v.as_col().is_unbound() {
        true => Column::empty(DataType::Variant, batch.rows()),
        false => crate::format::tag::to_variant(v.as_col(), ctx.pool),
    };
    v.release(ctx);
    Val::Owned(out)
}

fn eval_node<'a>(ir: &Ir, batch: &'a Batch, ctx: &mut Ctx) -> Val<'a> {
    let rows = batch.rows();
    match &ir.kind {
        // Every arm below indexes rows, so a constant field becomes rows here, from the caller's pool -- the same
        // buffers, recycled, that a broadcast literal has always been given. A pool of its own would allocate fresh
        // and zero what `take` hands back dirty.
        IrKind::Field(slot) => match batch.peek(*slot) {
            col if col.constant_value().is_some() => {
                Val::Owned(Column::filled(ctx.pool, col.constant_value().expect("checked"), rows))
            }
            col => Val::Ref(col),
        },

        IrKind::Literal(v) => Val::Owned(broadcast(v, &ir.ty, rows, ctx)),

        IrKind::Unary { op, operand } => {
            let a = eval_val(operand, batch, ctx);
            let start = ctx.errors.len();
            let out = match table::unop_kernel(*op, &a.as_col().data_type()) {
                Some(k) => k(ctx, &[a.as_col()]),
                // `!null` is true, whatever type the absent value was declared.
                None if *op == UnOp::Not && a.as_col().is_unbound() => {
                    Column::filled(ctx.pool, &Scalar::Bool(true), rows)
                }
                None => Column::empty(ir.ty.clone(), rows),
            };
            name_reads(ctx, start, op.symbol(), &[&**operand], &[a.as_col()]);
            a.release(ctx);
            Val::Owned(out)
        }

        // A literal operand stays one value where the kernel reads it as one; `apply_binary` gives rows to the rest.
        IrKind::Binary { op, lhs, rhs } => {
            let a = eval_operand(lhs, batch, ctx);
            let b = eval_operand(rhs, batch, ctx);
            let start = ctx.errors.len();
            let out = apply_binary(*op, a.as_col(), b.as_col(), &ir.ty, rows, ctx);
            name_reads(ctx, start, op.symbol(), &[&**lhs, &**rhs], &[a.as_col(), b.as_col()]);
            a.release(ctx);
            b.release(ctx);
            Val::Owned(out)
        }

        // `+` joins text and arrays, and `flatten` folds a chain of either into one node. The kernel follows the
        // node's type, so `xs + ys + zs` is one pass rather than two with an intermediate.
        IrKind::ConcatN(items) => {
            let arrays = matches!(ir.ty, DataType::Array(_));
            Val::Owned(eval_nary(items, batch, ctx, move |ctx, cols| {
                if arrays {
                    crate::ops::arrayops::array_concat(ctx, cols)
                } else {
                    crate::ops::strops::concat_n(ctx, cols)
                }
            }))
        }
        IrKind::AndN(items) => Val::Owned(eval_short(items, batch, ctx, ShortCircuit::And, |ctx, cols| {
            crate::ops::logic::and_n(ctx, cols)
        })),
        IrKind::OrN(items) => Val::Owned(eval_short(items, batch, ctx, ShortCircuit::Or, |ctx, cols| {
            crate::ops::logic::or_n(ctx, cols)
        })),
        IrKind::CoalesceN(items) => Val::Owned(eval_short(items, batch, ctx, ShortCircuit::Coalesce, |ctx, cols| {
            crate::ops::logic::coalesce_n(ctx, cols)
        })),

        // Both branches evaluate over the whole batch and the result is selected per row, unless no row takes one:
        // that one is not evaluated, since nothing of it would be selected. A branch's exceptions stay only on the
        // rows that took it, so a row reads as if its own branch alone had run.
        IrKind::Ternary { cond, then, else_ } => {
            let c = eval_val(cond, batch, ctx);
            let taken = truthy_rows(c.as_col(), rows);
            let took = bits::popcount(&taken);
            if took == 0 || took == rows {
                c.release(ctx);
                let side = eval_val(if took == 0 { else_ } else { then }, batch, ctx);
                // An unbound side declares its own type, a `null` literal's being `null`; the blend's is the one the
                // other side would have given it, and a reader such as an array loop dispatches on it.
                if side.as_col().is_unbound() && side.as_col().data_type() != ir.ty {
                    side.release(ctx);
                    return Val::Owned(Column::empty(ir.ty.clone(), rows));
                }
                return side;
            }
            let then_start = ctx.errors.len();
            let t = eval_val(then, batch, ctx);
            let else_start = ctx.errors.len();
            let e = eval_val(else_, batch, ctx);
            if ctx.errors.len() > then_start {
                // The upper range first: dropping from it leaves the lower range's indices where they are.
                let end = ctx.errors.len();
                ctx.errors
                    .retract_range(else_start, end, |row| !bits::get_row(&taken, row as usize));
                ctx.errors
                    .retract_range(then_start, else_start, |row| bits::get_row(&taken, row as usize));
            }
            let out = select(c.as_col(), t.as_col(), e.as_col(), rows, ctx);
            c.release(ctx);
            t.release(ctx);
            e.release(ctx);
            Val::Owned(out)
        }

        // A column already of the type passes through, untouched and uncopied.
        IrKind::SetElement { array, index, value } => {
            let (a, i, v) = (
                eval_val(array, batch, ctx),
                eval_operand(index, batch, ctx),
                eval_val(value, batch, ctx),
            );
            let start = ctx.errors.len();
            let out = crate::ops::arrayops::array_set(ctx, a.as_col(), i.as_col(), v.as_col(), &ir.ty);
            name_reads(ctx, start, "", &[&**array, &**index], &[a.as_col(), i.as_col()]);
            for x in [a, i, v] {
                x.release(ctx);
            }
            Val::Owned(out)
        }

        IrKind::Coerce { to, value } => {
            let v = eval_val(value, batch, ctx);
            if v.as_col().is_unbound() {
                v.release(ctx);
                return Val::Owned(Column::empty(to.clone(), rows));
            }
            if convert::coerce_is_identity(&v.as_col().data_type(), to) {
                return v;
            }
            let start = ctx.errors.len();
            let out = convert::coerce(ctx, v.as_col(), to);
            name_reads(ctx, start, "", &[&**value], &[v.as_col()]);
            v.release(ctx);
            Val::Owned(out)
        }

        IrKind::Call { func, args } => Val::Owned(eval_call(*func, args, &ir.ty, batch, ctx, rows)),
        IrKind::ObjectOp { op, args } => Val::Owned(eval_object_op(*op, args, &ir.ty, batch, ctx)),
        IrKind::Index { base, index } => Val::Owned(eval_index(base, index, &ir.ty, batch, ctx, rows)),
        IrKind::Array(items) => Val::Owned(eval_array(items, &ir.ty, batch, ctx, rows)),
        IrKind::Object { fields, unclaimed } => Val::Owned(eval_object(fields, unclaimed.as_deref(), batch, ctx, rows)),
        IrKind::Unclaimed(value) => {
            let v = eval_val(value, batch, ctx);
            let out = unclaimed_of(v.as_col(), rows, ctx);
            v.release(ctx);
            Val::Owned(out)
        }
        IrKind::Merge(layers) => Val::Owned(eval_merge(layers, batch, ctx, rows)),
        IrKind::Regex(_) => {
            unreachable!("a bare regex literal is a compile error (plan::resolve)")
        }
    }
}

/// An array literal has fixed arity: row `r`'s elements are child rows `[r*n, r*n+n)`, one from each item.
fn eval_array(items: &[Ir], ty: &DataType, batch: &Batch, ctx: &mut Ctx, rows: usize) -> Column {
    let DataType::Array(elem_ty) = ty else {
        unreachable!("Array Ir is always typed Array (plan::resolve::resolve_array)")
    };
    let n = items.len();
    let vals: Vec<Val> = items.iter().map(|i| eval_val(i, batch, ctx)).collect();
    let cols: Vec<&Column> = vals.iter().map(|v| v.as_col()).collect();
    let child = build_from_pieces(ctx, elem_ty, rows, rows * n, &|r, out: &mut Vec<Piece<'_>>| {
        out.extend(cols.iter().map(|c| Piece::One(c, r)))
    });
    for v in vals {
        v.release(ctx);
    }

    let mut offsets = ctx.pool.take((rows + 1) * 4);
    {
        let o = offsets.as_mut_slice::<u32>();
        for (r, slot) in o[..=rows].iter_mut().enumerate() {
            *slot = (r * n) as u32;
        }
    }
    let validity = crate::core::column::Validity::alloc(ctx.pool, rows);
    Column::list_from_parts(offsets, child, validity, rows)
}

fn eval_object(fields: &[(String, Ir)], unclaimed: Option<&Ir>, batch: &Batch, ctx: &mut Ctx, rows: usize) -> Column {
    let cols = fields
        .iter()
        .map(|(name, ir)| (name.clone(), eval(ir, batch, ctx)))
        .collect();
    let unclaimed = unclaimed.map(|u| eval(u, batch, ctx)).filter(|u| !u.is_unbound());
    Column::struct_with_unclaimed_fields(cols, unclaimed, rows)
}

/// `IrKind::Unclaimed`: a struct's unclaimed fields, or a variant's value on each row where it is an object.
fn unclaimed_of(col: &Column, rows: usize, ctx: &mut Ctx) -> Column {
    if let Some(u) = col.unclaimed_fields() {
        return u.deep_copy(ctx.pool);
    }
    let Ok(v) = col.variant() else {
        return Column::empty(DataType::Variant, rows);
    };
    let mut b = StrBuilder::new(ctx.pool, VarType::Variant, rows);
    for row in 0..rows {
        match v.validity.is_valid(row).then(|| v.get(row)) {
            Some(bytes) if crate::core::variant::is_object(bytes) => b.push_grow(ctx.pool, bytes),
            _ => b.push_null(),
        }
    }
    b.finish()
}

/// `IrKind::Merge`: each row's object is built from the layers in order. A key lays its value at its name, replacing
/// everything under it, and a null there removes it; a key the row gives as null lays nothing. A spread lays each key
/// of the object the row holds. A row whose object ends up empty is null.
fn eval_merge(layers: &[Layer], batch: &Batch, ctx: &mut Ctx, rows: usize) -> Column {
    use crate::format::tag::{self, Tag};
    // Each key only the row knows, and each layer's value.
    let cols: Vec<(Option<Column>, Column)> = layers
        .iter()
        .map(|l| match l {
            Layer::Key(key, ir) => (l.name().is_none().then(|| eval(key, batch, ctx)), eval(ir, batch, ctx)),
            Layer::Spread(ir) => (None, eval(ir, batch, ctx)),
        })
        .collect();
    let values: Vec<Option<Tag<'_>>> = (0..rows)
        .map(|row| {
            let mut fields = Vec::new();
            for (layer, (key, col)) in layers.iter().zip(&cols) {
                match (layer, tag::row_tag(col, row)) {
                    (Layer::Key(..), value) => {
                        let name = match (layer.name(), key.as_ref().and_then(|k| tag::row_tag(k, row))) {
                            (Some(name), _) => std::borrow::Cow::Borrowed(name),
                            (None, Some(Tag::Str(name))) => name,
                            (None, _) => continue,
                        };
                        fields.retain(|(k, _)| *k != name);
                        if let Some(value) = value {
                            fields.push((std::borrow::Cow::Owned(name.into_owned()), value));
                        }
                    }
                    (Layer::Spread(_), Some(Tag::Object(entries))) if fields.is_empty() => fields = entries,
                    (Layer::Spread(_), Some(Tag::Object(entries))) => {
                        tag::write_each(&mut fields, entries.into_iter().map(|(k, v)| (k, Some(v))).collect());
                    }
                    (Layer::Spread(_), _) => {}
                }
            }
            (!fields.is_empty()).then_some(Tag::Object(fields))
        })
        .collect();
    let out = tag::encode_column(&values, ctx.pool);
    drop(values);
    for c in cols.into_iter().flat_map(|(k, v)| k.into_iter().chain([v])) {
        c.reclaim(ctx.pool);
    }
    out
}

/// `IrKind::ObjectOp`. `ty` is the call's own type, which gives `entries` its values' type.
fn eval_object_op(op: ObjectOp, args: &[Ir], ty: &DataType, batch: &Batch, ctx: &mut Ctx) -> Column {
    use crate::ops::objectops as o;
    let vals: Vec<Val> = args.iter().map(|a| eval_operand(a, batch, ctx)).collect();
    let cols: Vec<&Column> = vals.iter().map(|v| v.as_col()).collect();
    let out = match op {
        ObjectOp::Keys => o::keys(ctx, cols[0]),
        ObjectOp::Entries => {
            let value_ty = match ty {
                DataType::Array(pair) => match pair.as_ref() {
                    DataType::Struct(fields, _) => fields.iter().find(|(n, _)| n == "value").map(|(_, t)| t.clone()),
                    _ => None,
                },
                _ => None,
            };
            o::entries(ctx, cols[0], &value_ty.unwrap_or(DataType::Null))
        }
        ObjectOp::FromEntries => o::from_entries(ctx, cols[0]),
        ObjectOp::Omit => unreachable!("`omit` lowers to an object and a merge of its unclaimed fields"),
        ObjectOp::Get => o::get(ctx, cols[0], cols[1]),
    };
    for v in vals {
        v.release(ctx);
    }
    out
}

/// `base[index]`, dispatched on `base`'s `Ir` type. `ty` is the result type, used only for the empty fallback.
fn eval_index(base: &Ir, index: &Ir, ty: &DataType, batch: &Batch, ctx: &mut Ctx, rows: usize) -> Column {
    match &base.ty {
        DataType::Array(elem_ty) => {
            let b = eval_val(base, batch, ctx);
            let idx = eval_val(index, batch, ctx);
            let start = ctx.errors.len();
            let out = array_index(b.as_col(), idx.as_col(), elem_ty, rows, ctx);
            name_reads(ctx, start, "", &[base, index], &[b.as_col(), idx.as_col()]);
            b.release(ctx);
            idx.release(ctx);
            out
        }
        // A struct's field by name.
        DataType::Struct(..) => {
            let IrKind::Literal(Value::Str(key)) = &index.kind else {
                unreachable!("struct index key is always a literal (plan::resolve::resolve_element_access)")
            };
            let b = eval_val(base, batch, ctx);
            let out = match b.as_col().field(key) {
                Some(field) => field.deep_copy(ctx.pool),
                None => Column::empty(ty.clone(), rows),
            };
            b.release(ctx);
            out
        }
        // What a variant holds at a key: an object's member at a string, an array's element at a number, null
        // where the row's value holds nothing there. A literal key is read as itself, never broadcast to a column.
        DataType::Variant => {
            use crate::core::variant::{Key, at_column, member_column};
            let b = eval_val(base, batch, ctx);
            let out = match &index.kind {
                IrKind::Literal(Value::Str(key)) => member_column(b.as_col(), key, ctx.pool),
                _ => {
                    let k = eval_val(index, batch, ctx);
                    let col = k.as_col();
                    let out = if let Ok(s) = col.var() {
                        at_column(
                            b.as_col(),
                            |r| s.validity.is_valid(r).then(|| Key::Member(s.get(r))),
                            ctx.pool,
                        )
                    } else if let Ok(n) = col.i64() {
                        at_column(
                            b.as_col(),
                            |r| n.validity.is_valid(r).then(|| Key::Element(n.values[r])),
                            ctx.pool,
                        )
                    } else {
                        Column::empty(DataType::Variant, rows)
                    };
                    k.release(ctx);
                    out
                }
            };
            b.release(ctx);
            out
        }
        other => unreachable!("cannot index {other:?} (plan::resolve::resolve_element_access)"),
    }
}

/// `base[idx]` for an `Array(T)` whose `T` is not a scalar `at` has a row for -- a declared array of objects.
/// Everything else `[...]` lowers to `at` (`plan::resolve::resolve_element_access`), so this path exists only for that
/// receiver and must agree with it: null propagates, a negative index counts from the end, and out of range is null.
fn array_index(base: &Column, idx: &Column, elem_ty: &DataType, rows: usize, ctx: &mut Ctx) -> Column {
    let Ok(list) = base.list() else {
        return Column::empty(elem_ty.clone(), rows);
    };
    let Ok(idx) = idx.i64() else {
        return Column::empty(elem_ty.clone(), rows);
    };
    let targets: Vec<Option<usize>> = (0..rows)
        .map(|r| {
            if !list.validity.is_valid(r) || !idx.validity.is_valid(r) {
                return None;
            }
            let len = (list.offsets[r + 1] - list.offsets[r]) as usize;
            crate::ops::helpers::position(idx.values[r], len).map(|i| list.offsets[r] as usize + i)
        })
        .collect();
    crate::ops::arrayops::raise_outside(
        ctx,
        list.validity,
        |r| idx.validity.is_valid(r),
        |r| targets[r].is_none(),
        rows,
    );
    let child = list.child;
    build_from_pieces(ctx, elem_ty, rows, rows, &|r, out: &mut Vec<Piece<'_>>| {
        out.push(targets[r].map_or(Piece::Null(1), |i| Piece::One(child, i)))
    })
}

/// `IrKind::Call`. A `Pattern` argument (`match`, `test`, `replace`, regex `split`) is read off the tree (`args[1]` is
/// `IrKind::Regex`) rather than evaluated.
fn eval_call(
    func: crate::ops::stdfuncs::StdFunc,
    args: &[Ir],
    expected: &DataType,
    batch: &Batch,
    ctx: &mut Ctx,
    rows: usize,
) -> Column {
    use crate::ops::arrayops::{regex_match, regex_split};
    use crate::ops::stdfuncs::{StdFunc, regex_replace, regex_test};

    match func {
        StdFunc::Match => {
            let value = eval_val(&args[0], batch, ctx);
            let out = match pattern_of(&args[1], ctx) {
                Some(p) => regex_match(ctx, value.as_col(), p),
                None => Column::empty(DataType::Array(Box::new(DataType::Str)), rows),
            };
            value.release(ctx);
            out
        }
        StdFunc::Test => {
            let value = eval_val(&args[0], batch, ctx);
            let out = match pattern_of(&args[1], ctx) {
                Some(p) => regex_test(ctx, value.as_col(), p),
                None => Column::empty(DataType::Bool, rows),
            };
            value.release(ctx);
            out
        }
        // The string-delimiter form has a kernel and takes the ordinary path below.
        StdFunc::Split if matches!(args[1].kind, IrKind::Regex(_)) => {
            let value = eval_val(&args[0], batch, ctx);
            let out = match pattern_of(&args[1], ctx) {
                Some(p) => regex_split(ctx, value.as_col(), p),
                None => Column::empty(DataType::Array(Box::new(DataType::Str)), rows),
            };
            value.release(ctx);
            out
        }
        StdFunc::ReplaceFirst | StdFunc::ReplaceAll => {
            let all = func == StdFunc::ReplaceAll;
            let value = eval_val(&args[0], batch, ctx);
            let replacement = eval_operand(&args[2], batch, ctx);
            let out = match pattern_of(&args[1], ctx) {
                Some(p) => regex_replace(ctx, value.as_col(), p, replacement.as_col(), all),
                None => Column::empty(DataType::Str, rows),
            };
            value.release(ctx);
            replacement.release(ctx);
            out
        }
        // A literal argument stays one value where the kernel reads it as one; the rest are given rows.
        _ => {
            let vals: Vec<Val> = args.iter().map(|a| eval_operand(a, batch, ctx)).collect();
            let cols: Vec<&Column> = vals.iter().map(|v| v.as_col()).collect();
            ctx.rows = rows;
            let start = ctx.errors.len();
            let out = with_rows_where_refused(
                ctx,
                &cols,
                rows,
                |i| func.takes_constant_at(i),
                |ctx, cols| match func.kernel() {
                    Some(k) => k(ctx, cols),
                    None => Column::empty(expected.clone(), rows),
                },
            );
            name_reads(ctx, start, func.name(), args, &cols);
            for v in vals {
                v.release(ctx);
            }
            out
        }
    }
}

/// Fill each exception raised since `start` with what it read: `call`, and each argument's value on its row, with its
/// slot where the argument is a plain field read. Every kernel call in a tree passes through here, so no kernel names
/// its own operands.
fn name_reads(
    ctx: &mut Ctx,
    start: usize,
    call: &'static str,
    args: &[impl std::borrow::Borrow<Ir>],
    cols: &[&Column],
) {
    let source = |a: &Ir| match a.kind {
        IrKind::Field(slot) => Some(slot),
        _ => None,
    };
    let read = |row| {
        (args.iter().zip(cols))
            .map(|(a, c)| Read::of(source(a.borrow()), c, row))
            .collect()
    };
    if ctx.errors.len() > start {
        ctx.errors.fill_from(start, call, read);
    }
}

/// `None` for an id outside `Ctx::patterns`: an empty column rather than a panic.
fn pattern_of<'a>(ir: &Ir, ctx: &Ctx<'a>) -> Option<&'a crate::core::ctx::CompiledPattern> {
    match ir.kind {
        IrKind::Regex(id) => ctx.patterns.get(id),
        _ => None,
    }
}

/// A literal operand stays one value: the text, `&&`, `||` and `??` kernels read it as one, and an array chain has
/// none.
fn eval_nary(items: &[Ir], batch: &Batch, ctx: &mut Ctx, k: impl Fn(&mut Ctx, &[&Column]) -> Column) -> Column {
    let vals: Vec<Val> = items.iter().map(|i| eval_operand(i, batch, ctx)).collect();
    let cols: Vec<&Column> = vals.iter().map(|v| v.as_col()).collect();
    let out = k(ctx, &cols);
    for v in vals {
        v.release(ctx);
    }
    out
}

/// Which rows an operand of `&&`, `||` or `??` still decides, given the operands before it: JavaScript stops reading
/// at the first `false`, `true`, or non-null. Each is what `ops::logic` computes; this only says which rows a later
/// operand's exceptions belong to.
#[derive(Clone, Copy)]
enum ShortCircuit {
    And,
    Or,
    Coalesce,
}

/// `eval_nary` for an operator JavaScript short-circuits. Every operand still runs on every row; an operand's
/// exceptions are then kept only on the rows an earlier operand had not already decided. The open rows are computed
/// only when an operand marked, so a clean batch pays nothing.
fn eval_short(
    items: &[Ir],
    batch: &Batch,
    ctx: &mut Ctx,
    op: ShortCircuit,
    k: impl Fn(&mut Ctx, &[&Column]) -> Column,
) -> Column {
    let rows = batch.rows();
    let mut vals: Vec<Val> = Vec::with_capacity(items.len());
    for (i, item) in items.iter().enumerate() {
        let start = ctx.errors.len();
        vals.push(eval_operand(item, batch, ctx));
        if i > 0 && ctx.errors.len() > start {
            let open = open_rows(&vals[..i], op, rows);
            ctx.errors.retract_from(start, |row| bits::get_row(&open, row as usize));
        }
    }
    let cols: Vec<&Column> = vals.iter().map(|v| v.as_col()).collect();
    let out = k(ctx, &cols);
    for v in vals {
        v.release(ctx);
    }
    out
}

/// Rows none of `prefix` decided, as a bitmask.
fn open_rows(prefix: &[Val], op: ShortCircuit, rows: usize) -> Vec<u64> {
    let words = bits::words_for(rows);
    let mut open = vec![!0u64; words];
    for v in prefix {
        let c = v.as_col();
        let decided = match op {
            ShortCircuit::And => {
                let mut t = truthy_rows(c, rows);
                for w in t.iter_mut() {
                    *w = !*w;
                }
                t
            }
            ShortCircuit::Or => truthy_rows(c, rows),
            ShortCircuit::Coalesce => {
                let mut has = vec![0u64; words];
                crate::ops::logic::rows_with_a_value(c, rows, &mut has);
                has
            }
        };
        for w in 0..words {
            open[w] &= !decided[w];
        }
    }
    bits::mask_tail(&mut open, rows);
    open
}

/// Rows where `c` is `true`: a null reads as false, and a column that is not `Bool` is false everywhere.
fn truthy_rows(c: &Column, rows: usize) -> Vec<u64> {
    let words = bits::words_for(rows);
    let mut out = vec![0u64; words];
    match (c.constant_value(), c.bool()) {
        (Some(Scalar::Bool(true)), _) => bits::fill_valid(&mut out, rows),
        (Some(_), _) => {}
        (None, Ok(b)) => bits::and_into(&mut out, b.validity.bits(), b.values, rows),
        (None, Err(_)) => {}
    }
    bits::mask_tail(&mut out, rows);
    out
}

/// An operator's operand or a call's argument. A literal is a constant column, not rows: the kernel decides what it
/// takes.
fn eval_operand<'a>(ir: &Ir, batch: &'a Batch, ctx: &mut Ctx) -> Val<'a> {
    if let IrKind::Literal(v) = &ir.kind
        && let Some(scalar) = scalar_of(v)
    {
        return Val::Owned(Column::constant(scalar, batch.rows()));
    }
    eval_val(ir, batch, ctx)
}

/// Run `k` over `cols`, with rows in place of each constant at a position `takes` refuses. The rows come from the
/// pool and go back to it after. With nothing to replace, `k` runs on `cols` as they are.
fn with_rows_where_refused(
    ctx: &mut Ctx,
    cols: &[&Column],
    rows: usize,
    takes: impl Fn(usize) -> bool,
    k: impl FnOnce(&mut Ctx, &[&Column]) -> Column,
) -> Column {
    let refused = |i: usize, c: &Column| !takes(i) && c.constant_value().is_some();
    if !cols.iter().enumerate().any(|(i, c)| refused(i, c)) {
        return k(ctx, cols);
    }
    let mut filled: Vec<Option<Column>> = Vec::with_capacity(cols.len());
    for (i, c) in cols.iter().enumerate() {
        let value = c.constant_value().filter(|_| refused(i, c));
        filled.push(value.map(|v| Column::filled(ctx.pool, v, rows)));
    }
    let given: Vec<&Column> = filled.iter().zip(cols).map(|(f, c)| f.as_ref().unwrap_or(c)).collect();
    let out = k(ctx, &given);
    for f in filled.into_iter().flatten() {
        f.reclaim(ctx.pool);
    }
    out
}

/// Dispatch a binary operator on the operands' runtime types.
fn apply_binary(op: BinOp, a: &Column, b: &Column, expected: &DataType, rows: usize, ctx: &mut Ctx) -> Column {
    let (at, bt) = (a.data_type(), b.data_type());
    let takes = table::takes_constant(op, &at, &bt);
    with_rows_where_refused(
        ctx,
        &[a, b],
        rows,
        |_| takes,
        |ctx, ins| {
            // These read an operand of any type, an absent one included, so none waits on a kernel for its types.
            match op {
                BinOp::Coalesce => return crate::ops::logic::coalesce_n(ctx, ins),
                BinOp::And => return crate::ops::logic::and_n(ctx, ins),
                BinOp::Or => return crate::ops::logic::or_n(ctx, ins),
                _ => {}
            }
            let out = match table::kernel(op, &at, &bt) {
                Some(k) => k(ctx, ins),
                None => Column::empty(expected.clone(), rows),
            };
            if op.is_comparison() {
                crate::ops::cmp::settle_nulls(ctx, op, ins[0], ins[1], out)
            } else {
                out
            }
        },
    )
}

/// A literal's value, or `None` for `null`.
fn scalar_of(v: &Value) -> Option<Scalar> {
    Some(match v {
        Value::Null => return None,
        Value::I64(x) => Scalar::I64(*x),
        Value::F64(x) => Scalar::F64(*x),
        Value::DateTime(x) => Scalar::DateTime(*x),
        Value::Duration(x) => Scalar::Duration(*x),
        Value::Bool(x) => Scalar::Bool(*x),
        Value::Str(s) => Scalar::Str(s.clone()),
        Value::Bytes(x) => Scalar::Bytes(x.clone()),
    })
}

/// Broadcast a literal to `rows` rows. `null` is an `Empty` column.
fn broadcast(v: &Value, ty: &DataType, rows: usize, ctx: &mut Ctx) -> Column {
    match scalar_of(v) {
        Some(scalar) => Column::filled(ctx.pool, &scalar, rows),
        None => Column::empty(ty.clone(), rows),
    }
}

/// Per-row blend `cond ? then : else_`; guarded assignment compiles to this. A null condition takes `else_`. The rows
/// are `take_met`'s, under the masks `cond` and `!cond`, at the type the two branches meet at.
fn select(cond: &Column, then: &Column, else_: &Column, rows: usize, ctx: &mut Ctx) -> Column {
    let words = bits::words_for(rows);
    let mut take_then = vec![0u64; words];
    if let Ok(c) = cond.bool() {
        bits::and_into(&mut take_then, c.validity.bits(), c.values, rows);
    }
    let mut take_else: Vec<u64> = take_then.iter().map(|t| !t).collect();
    bits::mask_tail(&mut take_then, rows);
    bits::mask_tail(&mut take_else, rows);
    take_met(ctx, &[then, else_], &[&take_then, &take_else], rows)
}
