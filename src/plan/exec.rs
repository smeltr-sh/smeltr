//! The executor: run a compiled `Plan` against a `Batch`. Every slot an instruction reads is baked into its `Ir`, so an
//! instruction is a direct call into `expr::eval`.

use std::time::{Duration, Instant};

use crate::core::batch::{Batch, SLOT_LIVE};
use crate::core::bits;
use crate::core::buffer::{self, BufferPool, Scratch};
use crate::core::column::{Column, DataType, StrBuilder, Validity, VarType};
use crate::core::ctx::{Ctx, OpenRegion};
use crate::core::error::{ExceptionKind, Read};
use crate::core::id::{FunctionId, SlotId};
use crate::expr::ast::Region;
use crate::expr::eval::{eval, eval_for_slot};
use crate::format::{csv_parse, grok_parse, json_parse, kv_parse, spread};
use crate::lookup;
use crate::metrics::MetricsHandle;
use crate::ops::arrayops::{Piece, build_from_pieces};
use crate::ops::convert::{self, parse_bool};
use crate::plan::compile::{ArrayFold, Call, Instr, Loop, Op, Output, Plan, TryStep};
use crate::plan::config::{ParserKind, TimeSource};

/// Run every instruction, with `__live` primed all-true. `__source.offset` and `_raw` must already be in the batch.
pub fn run<'a>(plan: &'a Plan, batch: &mut Batch, ctx: &mut Ctx<'a>) {
    let rows = batch.rows();
    let live = all_true(ctx.pool, rows);
    run_scoped(plan, batch, live, ctx, None);
}

/// Metrics labels threaded through the loop executors, which attribute a body's instructions to the enclosing function.
#[derive(Clone, Copy)]
struct MetricsScope<'a> {
    pipeline: &'a str,
    metrics: Option<&'a MetricsHandle>,
}

/// `run` with `__live` primed to `initial_live` (a route's match mask): a row the route did not match starts already
/// dropped.
///
/// With `metrics`, records `smeltr_function_calls_total` per instruction dispatched -- a loop body's included, so a step's
/// count is how many waves its loop took -- `smeltr_function_seconds_total` per top-level instruction, `smeltr_rows_dropped_total`
/// for a `Drop` step's instruction, and `smeltr_rows_errored_total` per `(function, kind)` over the exceptions this call
/// added.
pub fn run_scoped<'a>(
    plan: &'a Plan,
    batch: &mut Batch,
    initial_live: Column,
    ctx: &mut Ctx<'a>,
    metrics: Option<&MetricsHandle>,
) {
    ctx.patterns = &plan.compiled_patterns;
    batch.put(SLOT_LIVE, initial_live, ctx.pool);

    // Positional, resolved to the author's own id at the wire (`Plan::id`).
    let pipeline = plan.id_label.as_str();

    // Only needed for `smeltr_rows_dropped_total`; every instruction's own prior `__live` reference is baked into its `Ir`.
    let mut live_slot = SLOT_LIVE;
    // What this call added, for the error metric. `Unroll` remaps from the wider `ctx.pipeline_error_start` instead:
    // ingest's decode notes share these rows, but belong to the record, not to each route that delivers it.
    let own_start = ctx.errors.len();

    for instr in &plan.instrs {
        // A `Drop` step's instruction is the one whose output slot is named `__live`.
        let redefines_live = metrics.is_some() && plan.provenance.get(instr.out).is_some_and(|o| o.name == "__live");
        let live_before = if redefines_live {
            live_popcount(batch, live_slot)
        } else {
            0
        };
        let started = metrics.map(|_| Instant::now());
        // One `Scratch` per top-level instruction: a loop's is charged for everything its trips hold.
        let scratch = Scratch::begin(ctx.pool.budget());
        run_instr(&plan.slot_layout, instr, batch, ctx, MetricsScope { pipeline, metrics });
        buffer::check();
        drop(scratch);

        if let Some(m) = metrics {
            if redefines_live {
                live_slot = instr.out;
                let dropped = live_before.saturating_sub(live_popcount(batch, live_slot));
                incr_per_function(m, pipeline, instr.function, "smeltr_rows_dropped_total", dropped);
            }
            // A loop is timed here, at its own dispatch, and not inside: its body runs once per wave over a
            // handful of live rows, so per-wave timing cost more than the instruction it measured. One reading of
            // the whole loop per run says what that was trying to.
            if let Some(t0) = started {
                let seconds = t0.elapsed().as_secs_f64();
                incr_per_function(m, pipeline, instr.function, "smeltr_function_calls_total", 1);
                let function_label = instr.function.to_string();
                m.incr_seconds(
                    "smeltr_function_seconds_total",
                    &[("pipeline", pipeline), ("function", function_label.as_str())],
                    seconds,
                );
            }
        }
    }

    if let Some(m) = metrics {
        record_error_metrics(m, pipeline, ctx.errors, own_start);
    }
}

/// Run one instruction on `batch`: the pipeline's, or a loop's wave.
fn run_instr(layout: &[DataType], instr: &Instr, batch: &mut Batch, ctx: &mut Ctx, scope: MetricsScope) {
    ctx.function = instr.function;
    ctx.field = Some(instr.out);
    if let Some(dead) = instr.reuse {
        batch.take_dead(dead).reclaim(ctx.pool);
    }
    if let Some(written) = skipped(instr, batch) {
        // Every reader takes the prior on every row: a null of the slot's type is all it needs.
        for slot in written {
            batch.put(slot, Column::empty(layout[slot].clone(), batch.rows()), ctx.pool);
        }
        return;
    }
    let err_start = ctx.errors.len();
    // The region the instruction runs in, before a `Begin` opens its own.
    let held = ctx.regions.last().copied().flatten().map(|r| r.mask);
    match &instr.op {
        Op::Eval { ir } => {
            let col = eval_for_slot(ir, batch, ctx, instr.constant_ok);
            batch.put(instr.out, col, ctx.pool);
        }
        Op::Call {
            input,
            guard,
            outputs,
            call,
        } => {
            let (input, guard) = (*input, *guard);
            match call {
                Call::Parse { kind, matcher, objects } => match kind {
                    ParserKind::Json { split_dots } => {
                        let json = json_parse::Json(*split_dots);
                        spread::parse_batch(json, input, outputs, objects, guard, batch, ctx)
                    }
                    ParserKind::Csv(config) => {
                        let csv = csv_parse::Csv::new(config, outputs, batch.peek(input));
                        spread::parse_batch(csv, input, outputs, objects, guard, batch, ctx)
                    }
                    ParserKind::KeyValue(config) => {
                        spread::parse_batch(kv_parse::Kv(config), input, outputs, objects, guard, batch, ctx)
                    }
                    ParserKind::Cef { split_dots } => {
                        spread::parse_batch(kv_parse::Cef(*split_dots), input, outputs, objects, guard, batch, ctx)
                    }
                    ParserKind::Variant => {
                        spread::parse_batch(spread::Variant, input, outputs, objects, guard, batch, ctx)
                    }
                    ParserKind::Grok(config) => {
                        let matcher = matcher.as_deref().expect("a grok parse has its matcher");
                        let grok = grok_parse::Grok::new(matcher, config.split_dots, outputs, batch.peek(input));
                        spread::parse_batch(grok, input, outputs, objects, guard, batch, ctx)
                    }
                },
                Call::Lookup(l) => {
                    lookup::run(input, l, outputs, guard, batch, ctx.pool);
                    convert_outputs(outputs, batch, ctx);
                }
            }
        }
        Op::Loop(l) => run_loop(layout, l, batch, ctx, instr.guard, scope),
        Op::ArrayFold(l) => run_array_fold(layout, l, batch, ctx, instr.guard, scope),
        Op::Unroll { array, live, clear } => run_unroll(*array, *live, clear, instr.out, batch, layout, ctx),
        Op::Begin { region, mask } => {
            let col = match mask {
                Some(mask) => clearable(eval(mask, batch, ctx), ctx.pool),
                None => all_true(ctx.pool, batch.rows()),
            };
            batch.put(instr.out, col, ctx.pool);
            let mark = ctx.errors.len();
            let open = OpenRegion {
                region: *region,
                mask: instr.out,
                mark,
            };
            ctx.regions.push(Some(open));
        }
        Op::End { .. } => {
            ctx.regions.pop().flatten().expect("a region is open");
        }
        Op::Leave { to, rows } => {
            let leaving: Vec<u32> = (0..batch.rows() as u32)
                .filter(|&r| row_is_live(batch, Some(*rows), r as usize))
                .collect();
            leave(batch, ctx, &leaving, Some(*to));
        }
        Op::Try(TryStep::Throw { caught, rows, handled }) => {
            let live = row_mask(batch, &rows.iter().copied().collect::<Vec<_>>());
            if let Some((_, kept)) = ctx.caught.iter_mut().find(|(s, _)| s == caught) {
                // Taken once: a row a second `throw` reaches has nothing left to raise.
                let (thrown, rest): (Vec<_>, _) = std::mem::take(kept).into_iter().partition(|a| live[a.row as usize]);
                *kept = rest;
                ctx.errors
                    .extend(thrown.into_iter().filter(|a| !handled.contains(&a.kind.index())));
            }
        }
        Op::Try(step) => {
            let col = run_try(step, instr.out, batch, ctx);
            batch.put(instr.out, col, ctx.pool);
        }
    }
    retract_masked(instr, err_start, batch, ctx);
    leave_failures(batch, ctx, held, err_start);
}

/// `rows` leave every region from the innermost out to their target: for a `Leave`, the nearest region of its kind
/// `to`; for an exception, the nearest `try` with a `catch`, or with none the outermost `try`. A target beyond this
/// batch's regions, before a wave's start, takes every region this batch runs: the row is done with the loop, and a
/// `Leave` finishes in the loop's batch (`Ctx::leaving`). An exception finishes there on its own, as one there.
fn leave(batch: &mut Batch, ctx: &mut Ctx, rows: &[u32], to: Option<Region>) {
    let is = |r: &Option<OpenRegion>, f: &dyn Fn(Region) -> bool| r.is_some_and(|r| f(r.region));
    let target = match to {
        Some(to) => ctx.regions.iter().rposition(|r| is(r, &|k| k == to)),
        None => (ctx.regions.iter())
            .rposition(|r| is(r, &|k| k == Region::Try { catches: true }))
            .or_else(|| {
                ctx.regions
                    .iter()
                    .position(|r| is(r, &|k| matches!(k, Region::Try { .. })))
            }),
    };
    let Some(target) = target else {
        return;
    };
    let from = match ctx.regions.iter().rposition(Option::is_none) {
        Some(wave) if wave > target => {
            if let Some(to) = to {
                ctx.leaving.extend(rows.iter().map(|&r| (r, to)));
            }
            wave + 1
        }
        _ => target,
    };
    for open in ctx.regions[from..].iter().flatten() {
        let b = batch.get_mut(open.mask).bool_mut().expect("a region's mask is Bool");
        for &row in rows {
            bits::set_row(b.values, row as usize, false);
        }
    }
}

/// Inside a `try`, the rows an instruction failed on since `start` leave (`leave`). Only a row `held`, the mask of the
/// region it ran in, selects ran it.
fn leave_failures(batch: &mut Batch, ctx: &mut Ctx, held: Option<SlotId>, start: usize) {
    let in_try = |ctx: &Ctx| (ctx.regions.iter().flatten()).any(|r| matches!(r.region, Region::Try { .. }));
    if ctx.errors.len() <= start || !in_try(ctx) {
        return;
    }
    let mut failed: Vec<u32> = (ctx.errors.entries()[start..].iter())
        .filter(|a| a.kind.caught() && row_is_live(batch, held, a.row as usize))
        .map(|a| a.row)
        .collect();
    failed.sort_unstable();
    failed.dedup();
    leave(batch, ctx, &failed, None);
}

fn incr_per_function(metrics: &MetricsHandle, pipeline: &str, function: FunctionId, name: &'static str, delta: u64) {
    let function_label = function.to_string();
    metrics.incr(
        name,
        &[("pipeline", pipeline), ("function", function_label.as_str())],
        delta,
    );
}

fn live_popcount(batch: &Batch, slot: SlotId) -> u64 {
    batch
        .peek(slot)
        .bool()
        .ok()
        .map(|b| bits::popcount(b.values) as u64)
        .unwrap_or(0)
}

/// One step of a `try`, writing `out`. The mark is the error sink's length when the `try` began: the exceptions since it
/// are the body's, since nothing before the mark is retracted while the body runs.
fn run_try(step: &TryStep, out: SlotId, batch: &Batch, ctx: &mut Ctx) -> Column {
    let rows = batch.rows();
    // The innermost `try` this batch runs, which the step is inside.
    let mark = (ctx.regions.iter().rev().map_while(|r| *r))
        .find(|r| matches!(r.region, Region::Try { .. }))
        .expect("a `try` is open")
        .mark;
    let (TryStep::Kind { rows: left, .. } | TryStep::Caught { rows: left, .. }) = step else {
        unreachable!("`run_instr` runs a `throw`")
    };
    let live = left.map(|g| selected(batch.peek(g)));
    let entries = (ctx.errors.entries()[mark..].iter()).filter(|a| live.as_ref().is_none_or(|l| l[a.row as usize]));
    match step {
        TryStep::Kind { kinds, .. } => {
            let mut firsts = vec![None; rows];
            for a in entries {
                if kinds.contains(&a.kind.index()) {
                    firsts[a.row as usize].get_or_insert(a.kind.label());
                }
            }
            let mut b = StrBuilder::new(ctx.pool, VarType::Str, rows);
            for kind in firsts {
                match kind {
                    Some(k) => b.push_grow(ctx.pool, k.as_bytes()),
                    None => b.push_null(),
                }
            }
            b.finish()
        }
        TryStep::Caught { names, kinds, keep, .. } => {
            let mut texts: Vec<Vec<String>> = vec![Vec::new(); rows];
            let mut failed = vec![false; rows];
            // A row may carry an exception per key of a hostile record, so a repeat is found in constant time.
            let mut seen = std::collections::HashSet::new();
            for a in entries {
                failed[a.row as usize] |= a.kind.caught();
                if kinds.contains(&a.kind.index()) {
                    let text = a.message(names);
                    if seen.insert((a.row, text.clone())) {
                        texts[a.row as usize].push(text);
                    }
                }
            }
            // A marked row keeps none of the body's notes, until a `throw` gives them back; a row that did not fail
            // keeps them all.
            if *keep {
                let taken = (ctx.errors.entries()[mark..].iter())
                    .filter(|a| failed[a.row as usize])
                    .cloned()
                    .collect();
                match ctx.caught.iter_mut().find(|(s, _)| *s == out) {
                    Some((_, kept)) => *kept = taken,
                    None => ctx.caught.push((out, taken)),
                }
            }
            ctx.errors.retract_from(mark, |row| !failed[row as usize]);
            let mut b = StrBuilder::new(ctx.pool, VarType::Str, rows);
            for row in &texts {
                match row.is_empty() {
                    true => b.push_null(),
                    false => b.push_grow(ctx.pool, row.join("; ").as_bytes()),
                }
            }
            b.finish()
        }
        TryStep::Throw { .. } => unreachable!("`run_instr` runs a `throw`"),
    }
}

/// Turn each output a lookup wrote as text into its declared type. Text that does not fit is null on its row and the
/// row is marked `UnparsableField`, as a declared parser field's is. A row table never reaches that branch: the
/// compiler checked its cells. Under `Multiplicity::All` the slot is an array of text and each element is converted,
/// on the same offsets.
fn convert_outputs(outputs: &[Output], batch: &mut Batch, ctx: &mut Ctx) {
    for out in outputs {
        if out.ty == DataType::Str {
            continue;
        }
        let col = batch.get(out.slot);
        let converted = match col.list() {
            Ok(list) => {
                let rows = col.len();
                let child = convert_text(ctx, list.child, &out.ty, out.time_source.as_ref());
                // An element that failed names its row through the offsets.
                let mut row = 0;
                for i in 0..list.child.len() {
                    while (list.offsets[row + 1] as usize) <= i {
                        row += 1;
                    }
                    if list.child.is_valid(i) && !child.is_valid(i) {
                        let kind = ExceptionKind::UnparsableField { to: out.ty.clone() };
                        ctx.raise_at(row, out.slot, kind).read = vec![Read::of(None, list.child, i)];
                    }
                }
                let mut offsets = ctx.pool.take(list.offsets.len() * 4);
                offsets.as_mut_slice::<u32>().copy_from_slice(list.offsets);
                let mut validity = Validity::alloc_null(ctx.pool, rows);
                for r in 0..rows {
                    validity.set_valid(r, list.validity.is_valid(r));
                }
                Column::list_from_parts(offsets, child, validity, rows)
            }
            Err(_) => {
                let converted = convert_text(ctx, col, &out.ty, out.time_source.as_ref());
                for row in 0..col.len() {
                    if col.is_valid(row) && !converted.is_valid(row) {
                        let kind = ExceptionKind::UnparsableField { to: out.ty.clone() };
                        ctx.raise_at(row, out.slot, kind).read = vec![Read::of(Some(out.slot), col, row)];
                    }
                }
                converted
            }
        };
        batch.put(out.slot, converted, ctx.pool);
    }
}

/// One text column read as `ty`. A number or a datetime is the kernel `parseInt`, `parseFloat` or `parseTime` runs;
/// a bool is the declared parser field's reading, `true` or `false`, which no kernel offers.
fn convert_text(ctx: &mut Ctx, text: &Column, ty: &DataType, time_source: Option<&TimeSource>) -> Column {
    match ty {
        DataType::I64 => convert::parse_int(ctx, &[text]),
        DataType::F64 => convert::parse_float(ctx, &[text]),
        DataType::DateTime => match time_source {
            Some(TimeSource::Format(fmt)) => convert::parse_time_with_format(ctx, text, fmt),
            _ => convert::parse_time(ctx, &[text]),
        },
        DataType::Bool => {
            let len = text.len();
            let mut out = Column::alloc(ctx.pool, &DataType::Bool, len);
            let b = out.bool_mut().expect("a Bool column");
            b.validity.set_all_null();
            if let Ok(v) = text.var() {
                for i in 0..len {
                    let parsed = v
                        .validity
                        .is_valid(i)
                        .then(|| std::str::from_utf8(v.get(i)).ok().and_then(parse_bool))
                        .flatten();
                    if let Some(x) = parsed {
                        bits::set_row(b.values, i, x);
                        b.validity.set_valid(i, true);
                    }
                }
            }
            out
        }
        _ => unreachable!("an output staged as text is a scalar"),
    }
}

/// What an instruction under a guard noted on a masked row is not the row's: the instruction ran on every row, and
/// the row never takes its result (design doc, Columnar data model). Retracted after the instruction, from `start`,
/// the sink's length before it. A `for` or `while` is exempt: it runs its masked rows not at all, and what it notes
/// when its results assemble is those rows' own.
fn retract_masked(instr: &Instr, start: usize, batch: &Batch, ctx: &mut Ctx) {
    let guard = match &instr.op {
        Op::Loop(_) => None,
        // A call carries its guard on the op: `Instr::guard` marks a row barrier, which a call is not.
        Op::Call { guard, .. } => *guard,
        _ => instr.guard,
    };
    if let Some(g) = guard
        && ctx.errors.len() > start
    {
        let taken = row_mask(batch, &[g]);
        ctx.errors.retract_from(start, |row| taken[row as usize]);
    }
}

/// Each row's selection under every one of `slots`, as guards.
fn row_mask(batch: &Batch, slots: &[SlotId]) -> Vec<bool> {
    (0..batch.rows())
        .map(|row| slots.iter().all(|&s| row_is_live(batch, Some(s), row)))
        .collect()
}

/// The slots `instr` writes where its guard takes no row of `batch`: an `Eval` lifted ahead of the
/// guarded expression that reads it (`Instr::guard`), or a call under a guard. What either writes is read only on the
/// rows its guard takes, by a blend that keeps the prior elsewhere, so it is not computed. A parse that merges into
/// an object's unclaimed fields is the exception and runs. `None` where it runs.
fn skipped(instr: &Instr, batch: &Batch) -> Option<Vec<SlotId>> {
    let (guard, written) = match &instr.op {
        Op::Eval { .. } => (instr.guard?, vec![instr.out]),
        Op::Call {
            guard: Some(guard),
            outputs,
            call,
            ..
        } => {
            let mut written: Vec<SlotId> = outputs.iter().map(|o| o.slot).collect();
            if let Call::Parse { objects, .. } = call {
                // A merge's unclaimed fields are the object's on every row, unblended: the kernel carries the prior
                // through a row the guard skips, so it runs.
                if objects.iter().any(|o| o.under.is_some()) {
                    return None;
                }
                written.extend(objects.iter().map(|o| o.out));
            }
            (*guard, written)
        }
        _ => return None,
    };
    let g = batch.peek(guard).bool().ok()?;
    let rows = batch.rows();
    let words = bits::words_for(rows);
    let mut taken = vec![0u64; words];
    bits::and_into(&mut taken, g.validity.bits(), g.values, rows);
    (bits::popcount(&taken) == 0).then_some(written)
}

/// Whether `row` is selected by `guard` (`None` selects every row).
fn row_is_live(batch: &Batch, guard: Option<SlotId>, row: usize) -> bool {
    guard.is_none_or(|g| is_set(batch.peek(g), row))
}

/// Whether the Bool column `col` is true at `row`.
fn is_set(col: &Column, row: usize) -> bool {
    let b = col.bool().unwrap();
    b.validity.is_valid(row) && bits::get_row(b.values, row)
}

/// `smeltr_rows_errored_total{pipeline,function,kind}` over the exceptions added since `err_start`, grouped so a batch of
/// one exception costs one write.
fn record_error_metrics(
    metrics: &MetricsHandle,
    pipeline: &str,
    errors: &crate::core::error::ErrorSink,
    err_start: usize,
) {
    let mut counts: std::collections::HashMap<(FunctionId, &'static str), u64> = std::collections::HashMap::new();
    for a in &errors.entries()[err_start..] {
        *counts.entry((a.function, a.kind.label())).or_insert(0) += 1;
    }
    for ((function, kind), count) in counts {
        let function_label = function.to_string();
        metrics.incr(
            "smeltr_rows_errored_total",
            &[
                ("pipeline", pipeline),
                ("function", function_label.as_str()),
                ("kind", kind),
            ],
            count,
        );
    }
}

/// `Op::Loop`, as a wavefront: each trip runs `body` once over the rows still running, gathered into a wave. A row is
/// done when its array runs out, or when a trip cleared its bit in `stay`; the wave compacts by `gather`. A row `guard`
/// masks out, or whose array is empty or unbound, takes no trip and its accumulators retain. Rows finish out of order,
/// so final values are reassembled in row order by `assemble`. A loop with no array stops at its budget, which belongs
/// to the `Code` block (`Ctx::loop_owner`), and marks each row it stops.
///
/// With metrics, records `smeltr_rows_in_total` (batch width) and `smeltr_rows_out_total` (rows the guard let through).
fn run_loop(
    layout: &[DataType],
    spec: &Loop,
    batch: &mut Batch,
    ctx: &mut Ctx,
    guard: Option<SlotId>,
    scope: MetricsScope,
) {
    let rows = batch.rows();
    // `run_wave` overwrites `ctx.function` per nested instruction.
    let function = ctx.function;

    let live: Vec<u32> = (0..rows as u32)
        .filter(|&r| row_is_live(batch, guard, r as usize))
        .collect();
    let rows_out = live.len() as u64;
    // Each row's element count, where the loop walks an array.
    let lens: Option<Vec<u32>> = spec.array.map(|(array, _)| {
        let list = batch.peek(array).list().ok();
        let len = |r: usize| match &list {
            Some(l) if l.validity.is_valid(r) => l.offsets[r + 1] - l.offsets[r],
            _ => 0,
        };
        (0..rows).map(len).collect()
    });
    let more = |row: u32, trips: u32| lens.as_ref().is_none_or(|l| l[row as usize] > trips);
    let mut active: Vec<u32> = live.into_iter().filter(|&r| more(r, 0)).collect();

    // Outer slots the body reads besides the element and the accumulators, constant per row for the whole loop.
    let extra_slots = spec.wave_reads();

    // Chunk 0 holds every row's pre-loop values, indexed by row, and is the final answer for a row that takes no trip.
    let mut chunks: Vec<Vec<Column>> = vec![
        spec.accumulators
            .iter()
            .map(|(read_slot, write_slot)| match read_slot {
                Some(r) => batch.peek(*r).deep_copy(ctx.pool),
                None => Column::empty(batch.peek(*write_slot).data_type(), rows),
            })
            .collect(),
    ];
    let mut src_of_row: Vec<(u32, u32)> = (0..rows as u32).map(|r| (0, r)).collect();

    let mut cur: Vec<Column> = spec
        .accumulators
        .iter()
        .map(|(read_slot, write_slot)| match read_slot {
            Some(r) => batch.peek(*r).gather(ctx.pool, &active),
            None => Column::empty(batch.peek(*write_slot).data_type(), active.len()),
        })
        .collect();

    if spec.budget.is_some() && ctx.loop_owner != Some(function) {
        ctx.loop_owner = Some(function);
        ctx.loop_spent = Duration::ZERO;
    }
    let started = Instant::now();
    let mut trips = 0u32;
    let mut budget_spent = false;
    let mut leaving = Vec::new();
    let mut elem_idx: Vec<u32> = Vec::new();

    while !active.is_empty() {
        if spec.budget.is_some_and(|b| ctx.loop_spent + started.elapsed() >= b) {
            budget_spent = true;
            break;
        }
        let n = active.len();
        let mut wave = Batch::new(layout, n, 0);

        if let Some((array, element)) = spec.array {
            let list = batch
                .peek(array)
                .list()
                .expect("a non-zero element count implies a real list column");
            elem_idx.clear();
            elem_idx.extend(active.iter().map(|&r| list.offsets[r as usize] + trips));
            let elems = list.child.gather(ctx.pool, &elem_idx);
            wave.put(element, elems, ctx.pool);
        }
        for ((read_slot, _), c) in spec.accumulators.iter().zip(cur.drain(..)) {
            match read_slot {
                Some(r) => wave.put(*r, c, ctx.pool),
                None => c.reclaim(ctx.pool),
            }
        }
        for &s in &extra_slots {
            let v = batch.peek(s).gather(ctx.pool, &active);
            wave.put(s, v, ctx.pool);
        }

        leaving.extend(run_wave(layout, &spec.body, &mut wave, &active, ctx, scope));

        // Read before the accumulators are taken.
        let stayed = spec.stay.map(|s| row_mask(&wave, &[s]));
        cur = spec.accumulators.iter().map(|(_, w)| wave.take_dead(*w)).collect();
        wave.reclaim(ctx.pool);
        trips += 1;

        let (keep, stopping): (Vec<u32>, Vec<u32>) = (0..n as u32)
            .partition(|&i| more(active[i as usize], trips) && stayed.as_ref().is_none_or(|s| s[i as usize]));
        if !stopping.is_empty() {
            record_exits(&stopping, &active, &mut cur, &mut chunks, &mut src_of_row, ctx);
            let compacted: Vec<Column> = cur.iter().map(|c| c.gather(ctx.pool, &keep)).collect();
            for c in std::mem::replace(&mut cur, compacted) {
                c.reclaim(ctx.pool);
            }
            active = keep.iter().map(|&i| active[i as usize]).collect();
        }
    }

    if spec.budget.is_some() {
        ctx.loop_spent += started.elapsed();
    }
    if budget_spent {
        let stalled: Vec<u32> = (0..active.len() as u32).collect();
        for &i in &stalled {
            ctx.errors.raise(
                active[i as usize] as usize,
                function,
                None,
                ExceptionKind::LoopBudgetExceeded { trips },
            );
        }
        record_exits(&stalled, &active, &mut cur, &mut chunks, &mut src_of_row, ctx);
    }
    for c in cur {
        c.reclaim(ctx.pool);
    }

    assemble_results(&spec.accumulators, &chunks, &src_of_row, rows, batch, ctx);
    for chunk in chunks {
        for c in chunk {
            c.reclaim(ctx.pool);
        }
    }
    // A row that left a region around the loop leaves it here, in the loop's batch.
    for (row, to) in leaving {
        leave(batch, ctx, &[row], Some(to));
    }

    if let Some(m) = scope.metrics {
        incr_per_function(m, scope.pipeline, function, "smeltr_rows_in_total", rows as u64);
        incr_per_function(m, scope.pipeline, function, "smeltr_rows_out_total", rows_out);
    }
}

/// Put each accumulator's final value, gathered from `chunks` in row order, into its `write_slot`.
///
/// Each result takes the type its chunks carry, as `select` takes the type its branches carry. A variant or `null`
/// accumulator's slot fixes no column type, and the pre-loop value and each wave's can differ, so the slot's type says
/// nothing.
fn assemble_results(
    accumulators: &[(Option<SlotId>, SlotId)],
    chunks: &[Vec<Column>],
    src_of_row: &[(u32, u32)],
    rows: usize,
    batch: &mut Batch,
    ctx: &mut Ctx,
) {
    debug_assert_eq!(src_of_row.len(), rows);
    for (acc, (_, write_slot)) in accumulators.iter().enumerate() {
        let sources: Vec<&Column> = chunks.iter().map(|c| &c[acc]).collect();
        let fallback = batch.peek(*write_slot).data_type();
        let col = assemble(&sources, src_of_row, fallback, ctx);
        batch.put(*write_slot, col, ctx.pool);
    }
}

/// Row `r` is row `src_of_row[r].1` of source `src_of_row[r].0`. The result takes the type the sources rows are read
/// from carry, `fallback` where none is bound. The compiler holds an accumulator to one type
/// (`check_accumulator_types`), so sources differ only where one is a variant, and then each value is held as one.
fn assemble(sources: &[&Column], src_of_row: &[(u32, u32)], fallback: DataType, ctx: &mut Ctx) -> Column {
    // A source no row takes its value from, the pre-loop values where every row ran, says nothing about the result.
    let mut used = vec![false; sources.len()];
    for &(source, _) in src_of_row {
        used[source as usize] = true;
    }
    let bound = sources
        .iter()
        .zip(&used)
        .filter(|&(c, &u)| u && !c.is_unbound())
        .map(|(c, _)| c.data_type());
    let ty = crate::ops::helpers::blend_types(bound).unwrap_or(fallback);
    // Sources meeting as a variant are held as one first, each value as it is.
    let held: Vec<Option<Column>> = sources
        .iter()
        .zip(&used)
        .map(|(c, &u)| {
            (u && ty == DataType::Variant && !c.is_unbound() && c.data_type() != DataType::Variant)
                .then(|| crate::format::tag::to_variant(c, ctx.pool))
        })
        .collect();
    let rows = src_of_row.len();
    let col = build_from_pieces(ctx, &ty, rows, rows, &|row, out| {
        let (source, idx) = (src_of_row[row].0 as usize, src_of_row[row].1 as usize);
        out.push(Piece::One(held[source].as_ref().unwrap_or(sources[source]), idx));
    });
    for h in held.into_iter().flatten() {
        h.reclaim(ctx.pool);
    }
    col
}

/// How long one `Code` block may spend inside `while` loops per pipeline run. Checked between trips: a vectorized
/// kernel cannot be interrupted. Sized against librdkafka's `max.poll.interval.ms`, the constraint that binds.
///
/// Per `Code` block, not per loop, per pipeline run or per batch. Per loop is no bound: a `while` inside a row-barrier
/// `for` is charged once per element of the outer array, a multiplier the data picks. Per run or per batch widens the
/// blast radius: one step's runaway loop would starve the next step's legitimate one, whose rows would then carry
/// `LoopBudgetExceeded` for a bug they never touched. One budget per block per pipeline run is what an operator can
/// read off their own configuration; an array's length is not. A batch runs its pipelines once per slice
/// (`cpu_cache::slice_bytes`), so a batch can spend the budget once per slice.
pub const DEFAULT_LOOP_BUDGET: Duration = Duration::from_millis(100);

/// Lift the accumulator values of the rows at `positions` (indices into `active`/`cur`) into a fresh chunk and point
/// those rows at it.
fn record_exits(
    positions: &[u32],
    active: &[u32],
    cur: &mut [Column],
    chunks: &mut Vec<Vec<Column>>,
    src_of_row: &mut [(u32, u32)],
    ctx: &mut Ctx,
) {
    let chunk = chunks.len() as u32;
    chunks.push(cur.iter().map(|c| c.gather(ctx.pool, positions)).collect());
    for (j, &p) in positions.iter().enumerate() {
        src_of_row[active[p as usize] as usize] = (chunk, j as u32);
    }
}

/// Run a loop body against a wave. What the body marks is keyed to the wave's rows, and `active` maps each back to
/// the row of the batch the loop runs over. Returns, in those rows, the rows that leave a region around the loop.
fn run_wave(
    layout: &[DataType],
    body: &[Instr],
    wave: &mut Batch,
    active: &[u32],
    ctx: &mut Ctx,
    scope: MetricsScope,
) -> Vec<(u32, Region)> {
    let start = ctx.errors.len();
    let outer = std::mem::take(&mut ctx.leaving);
    ctx.regions.push(None);
    for instr in body {
        run_instr(layout, instr, wave, ctx, scope);
        // Counted, never timed. A `FunctionId` is a step's, so every instruction a step compiles to shares one:
        // this count is that step's dispatches, which is how many waves its loop took -- what says a `break` left
        // the wavefront rather than spinning to the budget. A loop is timed at its own top-level dispatch.
        if !matches!(instr.op, Op::Loop(_))
            && let Some(m) = scope.metrics
        {
            incr_per_function(m, scope.pipeline, instr.function, "smeltr_function_calls_total", 1);
        }
    }
    let begun = ctx.regions.pop();
    debug_assert!(matches!(begun, Some(None)), "a loop body closes every region it opens");
    ctx.errors.remap_rows_from(start, |row| vec![active[row as usize]]);
    let leaving = std::mem::replace(&mut ctx.leaving, outer);
    leaving.into_iter().map(|(r, to)| (active[r as usize], to)).collect()
}

/// The exploded batch the vectorized loops evaluate over: one pseudo-row per (row, element). `referenced` slots are
/// broadcast by `gather` against a repeat index built from the array's offsets; the array's child column becomes
/// `element_slot`. An unbound array is zero elements. So is a masked row's: under a guard the op visits the rows the
/// loop would, and nothing a masked row holds can raise or cost.
///
/// Returns the batch, each row's span in it, and each pseudo-row's row.
fn explode_array(
    layout: &[DataType],
    batch: &Batch,
    array: SlotId,
    element_slot: SlotId,
    referenced: &[SlotId],
    guard: Option<SlotId>,
    ctx: &mut Ctx,
) -> (Batch, Vec<u32>, Vec<u32>) {
    let rows = batch.rows();
    let element_ty = match batch.peek(array).data_type() {
        DataType::Array(t) => *t,
        _ => unreachable!("an array-loop's array slot is always Array(_)-typed"),
    };
    let list = batch.peek(array).list().ok();
    let array_offsets: Vec<u32> = list.as_ref().map_or(vec![0; rows + 1], |l| l.offsets.to_vec());
    // A null row's offsets may still span elements.
    let holds = |row: usize| list.as_ref().is_some_and(|l| l.validity.is_valid(row));
    // Each row's span in the exploded batch, and which child elements it takes: every element without a guard,
    // none for a masked or null row.
    let mut offsets = vec![0u32; rows + 1];
    let mut element_row: Vec<u32> = Vec::new();
    let mut element_idx: Vec<u32> = Vec::new();
    for row in 0..rows {
        if holds(row) && row_is_live(batch, guard, row) {
            for i in array_offsets[row]..array_offsets[row + 1] {
                element_row.push(row as u32);
                element_idx.push(i);
            }
        }
        offsets[row + 1] = element_row.len() as u32;
    }

    let mut exploded = Batch::new(layout, element_row.len(), 0);
    for &slot in referenced {
        let col = batch.peek(slot).gather(ctx.pool, &element_row);
        exploded.put(slot, col, ctx.pool);
    }
    let elem_col = match list {
        Some(list) if guard.is_none() && !list.validity.has_nulls() => list.child.deep_copy(ctx.pool),
        Some(list) => list.child.gather(ctx.pool, &element_idx),
        None => Column::empty(element_ty, 0),
    };
    exploded.put(element_slot, elem_col, ctx.pool);

    (exploded, offsets, element_row)
}

/// `Op::Unroll`: replace the batch with one row per element of `array`, via `Batch::gather`. A row that is dropped, or
/// whose array is empty or absent, contributes no rows. An unroll never sits under a guard.
///
/// The consumed array slot and the `clear` slots are released before the gather: duplicating an N-element array (or
/// `_raw`) into each of its N output rows is quadratic in the fan-out of one record.
fn run_unroll(
    array: SlotId,
    live: SlotId,
    clear: &[SlotId],
    out: SlotId,
    batch: &mut Batch,
    layout: &[DataType],
    ctx: &mut Ctx,
) {
    let rows = batch.rows();
    let element_ty = match batch.peek(array).data_type() {
        DataType::Array(t) => *t,
        _ => unreachable!("Op::Unroll's array slot is always Array(_)-typed"),
    };
    let list = batch.peek(array).list().ok();
    let offsets: Vec<u32> = list.as_ref().map_or(vec![0; rows + 1], |l| l.offsets.to_vec());
    let null_row = |row: usize| list.as_ref().is_some_and(|l| !l.validity.is_valid(row));

    let live_col = batch.peek(live).bool().ok();
    let is_live_row = |row: usize| -> bool {
        match &live_col {
            Some(b) => b.validity.is_valid(row) && bits::get_row(b.values, row),
            None => true,
        }
    };

    let mut survivors: Vec<u32> = Vec::new();
    let element_row: Vec<u32> = (0..rows)
        .flat_map(|row| {
            let (s, e) = if is_live_row(row) && !null_row(row) {
                (offsets[row] as usize, offsets[row + 1] as usize)
            } else {
                (0, 0)
            };
            for idx in s..e {
                survivors.push(idx as u32);
            }
            std::iter::repeat_n(row as u32, e - s)
        })
        .collect();

    let elem_col = match batch.peek(array).list() {
        Ok(list) => list.child.gather(ctx.pool, &survivors),
        Err(_) => Column::empty(element_ty, 0),
    };

    // Exceptions recorded by this pipeline run are keyed to the old row numbering; remap them the way the batch is
    // about to be.
    if ctx.errors.len() > ctx.pipeline_error_start {
        let mut new_rows_of: std::collections::HashMap<u32, Vec<u32>> = std::collections::HashMap::new();
        for (new_row, &old_row) in element_row.iter().enumerate() {
            new_rows_of.entry(old_row).or_default().push(new_row as u32);
        }
        ctx.errors.remap_rows_from(ctx.pipeline_error_start, |old_row| {
            new_rows_of.get(&old_row).cloned().unwrap_or_default()
        });
    }

    let array_ty = batch.peek(array).data_type();
    batch.put(array, Column::empty(array_ty, rows), ctx.pool);

    if !clear.is_empty() {
        for &slot in clear {
            let ty = batch.peek(slot).data_type();
            batch.put(slot, Column::empty(ty, rows), ctx.pool);
        }
    }

    let partition = batch.partition();
    let taken = std::mem::replace(batch, Batch::new(layout, 0, partition));
    let mut new_batch = taken.gather(ctx.pool, &element_row);
    new_batch.put(out, elem_col, ctx.pool);
    *batch = new_batch;
}

/// `Op::ArrayFold`: the body runs once over the exploded batch, then each fold folds each row's span of the elements
/// its write ran on. A masked-out row has no elements, and keeps its seeds.
fn run_array_fold(
    layout: &[DataType],
    spec: &ArrayFold,
    batch: &mut Batch,
    ctx: &mut Ctx,
    guard: Option<SlotId>,
    scope: MetricsScope,
) {
    use crate::plan::compile::FoldOp;
    let rows = batch.rows();
    let (mut exploded, offsets, element_row) = explode_array(
        layout,
        batch,
        spec.array,
        spec.element_slot,
        &spec.element_reads(),
        guard,
        ctx,
    );
    let leaving = run_wave(layout, &spec.body, &mut exploded, &element_row, ctx, scope);
    debug_assert!(leaving.is_empty(), "nothing in a vectorized body leaves the loop");

    for fold in &spec.folds {
        // What a kernel notes is the accumulator's.
        ctx.field = Some(fold.out);
        let kept = fold.ran.map(|r| selected(exploded.peek(r)));
        let keep = |i: usize| kept.as_ref().is_none_or(|k| k[i]);
        let mapped = exploded.take_dead(fold.value);
        let seed_col = batch.peek(fold.seed);
        let result = match fold.op {
            FoldOp::Push => fold_push(seed_col, mapped, kept.as_deref(), &offsets, rows, ctx),
            FoldOp::Last => fold_last(mapped, &offsets, rows, &keep, seed_col, ctx),
            FoldOp::Add | FoldOp::Mul => {
                // The compiler holds seed and map to one type. An unbound seed is null on every row and stays null
                // through any trip. An unbound map holds no value in any element: a row with an element folds to
                // null, as a null element does in the kernels, and a row without keeps its seed.
                let out = match (seed_col.data_type(), mapped.data_type()) {
                    _ if seed_col.is_unbound() => Column::empty(mapped.data_type(), rows),
                    _ if mapped.is_unbound() => {
                        let src_of_row: Vec<(u32, u32)> = (0..rows)
                            .map(|row| match ran(&offsets, row, &keep) {
                                true => (0, 0),
                                false => (1, row as u32),
                            })
                            .collect();
                        assemble(&[&mapped, seed_col], &src_of_row, seed_col.data_type(), ctx)
                    }
                    (DataType::I64, DataType::I64) => fold_i64(seed_col, &mapped, &offsets, rows, &keep, fold.op, ctx),
                    (DataType::F64, DataType::F64) => {
                        fold_f64(seed_col, &mapped, &offsets, rows, &keep, fold.op, ctx.pool)
                    }
                    (_, DataType::Str | DataType::Bytes) => {
                        fold_var(seed_col, &mapped, &offsets, rows, &keep, ctx.pool)
                    }
                    _ => unreachable!("fold_has_kernel only picks Add/Mul for numeric or text maps"),
                };
                mapped.reclaim(ctx.pool);
                out
            }
            FoldOp::And | FoldOp::Or => {
                let folded = fold_bool(seed_col, &mapped, &offsets, rows, &keep, fold.op, ctx.pool);
                mapped.reclaim(ctx.pool);
                if seed_col.data_type() == DataType::Bool {
                    folded
                } else {
                    // A `null` seed: a row that runs no trip keeps it, and the types meet as a general loop's do.
                    let src_of_row: Vec<(u32, u32)> = (0..rows)
                        .map(|row| match ran(&offsets, row, &keep) {
                            true => (0, row as u32),
                            false => (1, row as u32),
                        })
                        .collect();
                    let out = assemble(&[&folded, seed_col], &src_of_row, DataType::Bool, ctx);
                    folded.reclaim(ctx.pool);
                    out
                }
            }
        };
        batch.put(fold.out, result, ctx.pool);
    }
    exploded.reclaim(ctx.pool);
}

/// Whether the write ran on any of `row`'s elements.
fn ran(offsets: &[u32], row: usize, keep: &impl Fn(usize) -> bool) -> bool {
    (offsets[row] as usize..offsets[row + 1] as usize).any(keep)
}

/// Each row's selection under the Bool column `col`: a null, or a column of another type, which a `null` condition
/// gives, selects none.
fn selected(col: &Column) -> Vec<bool> {
    let b = col.bool().ok();
    (0..col.len())
        .map(|i| {
            b.as_ref()
                .is_some_and(|b| b.validity.is_valid(i) && bits::get_row(b.values, i))
        })
        .collect()
}

/// `FoldOp::Push`: each row's seed, then each of its elements' values, as `push` appends them, a null as a position:
/// where `kept` is, only the elements it selects. A row that appends nothing keeps its seed, null included. Where
/// every seed is empty and there is no filter, the values are the elements as they stand.
fn fold_push(
    seed_col: &Column,
    mapped: Column,
    kept: Option<&[bool]>,
    offsets: &[u32],
    rows: usize,
    ctx: &mut Ctx,
) -> Column {
    let out_ty = match (seed_col.data_type(), mapped.data_type()) {
        (DataType::Array(e), m) => e.unify(&m).unwrap_or(*e),
        (_, m) => m,
    };
    let seed = seed_col.list().ok();
    let seed_span = |row: usize| {
        seed.as_ref()
            .filter(|l| l.validity.is_valid(row))
            .map(|l| (l.offsets[row] as usize, l.offsets[row + 1] as usize))
    };
    let appends = |i: usize| kept.is_none_or(|k| k[i]);
    let mut validity = Validity::alloc_null(ctx.pool, rows);
    for row in 0..rows {
        if ran(offsets, row, &appends) || seed_span(row).is_some() {
            validity.set_valid(row, true);
        }
    }
    let seeds_empty = (0..rows).all(|row| seed_span(row).is_none_or(|(s, e)| s == e));
    if seeds_empty && kept.is_none() && mapped.data_type() == out_ty {
        let mut offsets_buf = ctx.pool.take((rows + 1) * 4);
        offsets_buf.as_mut_slice::<u32>().copy_from_slice(offsets);
        return Column::list_from_parts(offsets_buf, mapped, validity, rows);
    }
    // Each row's appended elements are `picked[from[row]..from[row + 1]]`.
    let mut picked: Vec<usize> = Vec::new();
    let mut from = vec![0usize; rows + 1];
    let mut out_offsets = vec![0u32; rows + 1];
    for row in 0..rows {
        picked.extend((offsets[row] as usize..offsets[row + 1] as usize).filter(|&i| appends(i)));
        from[row + 1] = picked.len();
        let seeded = seed_span(row).map_or(0, |(s, e)| e - s);
        out_offsets[row + 1] = out_offsets[row] + (seeded + from[row + 1] - from[row]) as u32;
    }
    let total = out_offsets[rows] as usize;
    let child = build_from_pieces(ctx, &out_ty, rows, total, &|row, out: &mut Vec<Piece<'_>>| {
        if let (Some((s, e)), Some(l)) = (seed_span(row), seed.as_ref()) {
            out.push(Piece::Span(l.child, s, e));
        }
        out.extend(picked[from[row]..from[row + 1]].iter().map(|&i| Piece::One(&mapped, i)));
    });
    mapped.reclaim(ctx.pool);
    let mut offsets_buf = ctx.pool.take((rows + 1) * 4);
    offsets_buf.as_mut_slice::<u32>().copy_from_slice(&out_offsets);
    Column::list_from_parts(offsets_buf, child, validity, rows)
}

/// `FoldOp::Last`: the last element's value of those `keep` takes, else the seed.
fn fold_last(
    mapped: Column,
    offsets: &[u32],
    rows: usize,
    keep: &impl Fn(usize) -> bool,
    seed_col: &Column,
    ctx: &mut Ctx,
) -> Column {
    // A row that ran takes its last element; the rest keep the seed. The result takes the type those rows carry, as a
    // general loop's does: the compiler holds a typed value to the seed's type, but a `null` or a variant is untyped
    // and meets the seed here.
    let src_of_row: Vec<(u32, u32)> = (0..rows)
        .map(
            |row| match (offsets[row] as usize..offsets[row + 1] as usize).rfind(|&i| keep(i)) {
                Some(i) => (0, i as u32),
                None => (1, row as u32),
            },
        )
        .collect();
    let out = assemble(&[&mapped, seed_col], &src_of_row, mapped.data_type(), ctx);
    mapped.reclaim(ctx.pool);
    out
}

/// `Add`/`Mul` on I64. A row whose accumulator overflows is null and marked `Overflow`: the exception is the
/// row's, and its neighbours keep their i64.
fn fold_i64(
    seed_col: &Column,
    mapped: &Column,
    offsets: &[u32],
    rows: usize,
    keep: &impl Fn(usize) -> bool,
    op: crate::plan::compile::FoldOp,
    ctx: &mut Ctx,
) -> Column {
    use crate::plan::compile::FoldOp;
    let sv = seed_col.i64().unwrap();
    let mv = mapped.i64().unwrap();

    let mut out = Column::alloc(ctx.pool, &DataType::I64, rows);
    {
        let o = out.i64_mut().unwrap();
        for row in 0..rows {
            let (start, end) = (offsets[row] as usize, offsets[row + 1] as usize);
            let mut acc = sv.values[row];
            let mut acc_valid = sv.validity.is_valid(row);
            for i in (start..end).filter(|&i| keep(i)) {
                if !acc_valid || !mv.validity.is_valid(i) {
                    acc_valid = false;
                    continue;
                }
                let (checked, call) = if op == FoldOp::Add {
                    (acc.checked_add(mv.values[i]), "+")
                } else {
                    (acc.checked_mul(mv.values[i]), "*")
                };
                match checked {
                    Some(v) => acc = v,
                    None => {
                        acc_valid = false;
                        let a = ctx.raise(row, ExceptionKind::Overflow);
                        let operands = [acc, mv.values[i]].map(|v| Read::new(None, Some(v.to_string())));
                        (a.call, a.read) = (call, operands.to_vec());
                    }
                }
            }
            o.values[row] = acc;
            o.validity.set_valid(row, acc_valid);
        }
    }
    out
}

fn fold_f64(
    seed_col: &Column,
    mapped: &Column,
    offsets: &[u32],
    rows: usize,
    keep: &impl Fn(usize) -> bool,
    op: crate::plan::compile::FoldOp,
    pool: &mut BufferPool,
) -> Column {
    use crate::plan::compile::FoldOp;
    let sv = seed_col.f64().unwrap();
    let mv = mapped.f64().unwrap();
    let mut out = Column::alloc(pool, &DataType::F64, rows);
    {
        let o = out.f64_mut().unwrap();
        for row in 0..rows {
            let (start, end) = (offsets[row] as usize, offsets[row + 1] as usize);
            let mut acc = sv.values[row];
            let mut acc_valid = sv.validity.is_valid(row);
            for i in (start..end).filter(|&i| keep(i)) {
                if !acc_valid || !mv.validity.is_valid(i) {
                    acc_valid = false;
                    continue;
                }
                acc = if op == FoldOp::Add {
                    acc + mv.values[i]
                } else {
                    acc * mv.values[i]
                };
            }
            o.values[row] = acc;
            o.validity.set_valid(row, acc_valid);
        }
    }
    out
}

/// `And`/`Or`, as `ops::logic` reads them. A row whose loop runs no trip keeps a Bool seed, null included; a seed of
/// another type reads as null here, and `run_array_fold` keeps it.
fn fold_bool(
    seed_col: &Column,
    mapped: &Column,
    offsets: &[u32],
    rows: usize,
    keep: &impl Fn(usize) -> bool,
    op: crate::plan::compile::FoldOp,
    pool: &mut BufferPool,
) -> Column {
    use crate::plan::compile::FoldOp;
    // A `null` seed or map carries no bitmap.
    let read = |c: &Column, i: usize| {
        c.bool()
            .ok()
            .filter(|b| b.validity.is_valid(i))
            .map(|b| bits::get_row(b.values, i))
    };
    let mut out = Column::alloc(pool, &DataType::Bool, rows);
    {
        let o = out.bool_mut().unwrap();
        for row in 0..rows {
            let (start, end) = (offsets[row] as usize, offsets[row + 1] as usize);
            let mut acc = read(seed_col, row);
            for i in (start..end).filter(|&i| keep(i)) {
                let v = read(mapped, i);
                // `acc && v` or `acc || v`, which read null as false and yield true or false.
                let (a, v) = (acc.unwrap_or(false), v.unwrap_or(false));
                acc = Some(match op {
                    FoldOp::And => a && v,
                    FoldOp::Or => a || v,
                    _ => unreachable!("fold_bool only runs for And/Or"),
                });
            }
            match acc {
                Some(v) => {
                    bits::set_row(o.values, row, v);
                    o.validity.set_valid(row, true);
                }
                None => o.validity.set_valid(row, false),
            }
        }
    }
    out
}

/// `Add` on Str/Bytes: concatenation. A null seed or element nulls the row.
fn fold_var(
    seed_col: &Column,
    mapped: &Column,
    offsets: &[u32],
    rows: usize,
    keep: &impl Fn(usize) -> bool,
    pool: &mut BufferPool,
) -> Column {
    let sv = seed_col.var().unwrap();
    let mv = mapped.var().unwrap();
    let var_type = if matches!(mapped.data_type(), DataType::Bytes) {
        VarType::Bytes
    } else {
        VarType::Str
    };
    let mut b = StrBuilder::new(pool, var_type, rows);
    for row in 0..rows {
        let (start, end) = (offsets[row] as usize, offsets[row + 1] as usize);
        if !sv.validity.is_valid(row) {
            b.push_null();
            continue;
        }
        if !(start..end).any(keep) {
            b.push_grow(pool, sv.get(row));
            continue;
        }
        let mut valid = true;
        let mut buf: Vec<u8> = sv.get(row).to_vec();
        for i in (start..end).filter(|&i| keep(i)) {
            if !mv.validity.is_valid(i) {
                valid = false;
                break;
            }
            buf.extend_from_slice(mv.get(i));
        }
        if valid {
            b.push_grow(pool, &buf);
        } else {
            b.push_null();
        }
    }
    b.finish()
}

/// `mask` as a region's mask, whose bits `leave` clears: a Bool column with no null, false where `mask` is null.
fn clearable(mask: Column, pool: &mut BufferPool) -> Column {
    if mask.bool().is_ok_and(|b| !b.validity.has_nulls()) {
        return mask;
    }
    let mut col = all_true(pool, mask.len());
    let b = col.bool_mut().expect("allocated as Bool");
    for row in 0..mask.len() {
        bits::set_row(b.values, row, is_set(&mask, row));
    }
    mask.reclaim(pool);
    col
}

fn all_true(pool: &mut crate::core::buffer::BufferPool, rows: usize) -> Column {
    let mut c = Column::alloc(pool, &DataType::Bool, rows);
    bits::fill_valid(c.bool_mut().unwrap().values, rows);
    c
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::error::ErrorSink;
    use crate::plan::compile::FoldOp;

    fn i64s(p: &mut BufferPool, values: &[i64]) -> Column {
        let mut c = Column::alloc(p, &DataType::I64, values.len());
        c.i64_mut().unwrap().values.copy_from_slice(values);
        c
    }

    /// An overflowing row of an `Add` fold is null and marked; the rows beside it keep their i64.
    #[test]
    fn an_overflowing_fold_row_is_null_and_marked_while_its_neighbours_keep_i64() {
        let mut p = BufferPool::new();
        let mut errors = ErrorSink::new();
        let seed = i64s(&mut p, &[0, 0, 5]);
        // Row 0: [i64::MAX, 1]; row 1: [2, 3]; row 2: [].
        let mapped = i64s(&mut p, &[i64::MAX, 1, 2, 3]);
        let offsets = [0u32, 2, 4, 4];
        let out = {
            let mut ctx = Ctx::new(&mut p, &mut errors);
            fold_i64(&seed, &mapped, &offsets, 3, &|_| true, FoldOp::Add, &mut ctx)
        };
        assert_eq!(out.data_type(), DataType::I64);
        let o = out.i64().unwrap();
        let rows: Vec<Option<i64>> = (0..3).map(|i| o.validity.is_valid(i).then(|| o.values[i])).collect();
        assert_eq!(rows, vec![None, Some(5), Some(5)]);
        let exceptions: Vec<(u32, ExceptionKind)> = errors.entries().iter().map(|a| (a.row, a.kind.clone())).collect();
        assert_eq!(exceptions, vec![(0, ExceptionKind::Overflow)]);
        out.reclaim(&mut p);
        seed.reclaim(&mut p);
        mapped.reclaim(&mut p);
    }
}
