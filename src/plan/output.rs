//! Run a `Plan` and serialize the result: `exec::run` plus `format::record::RecordView`, over `Plan::tree`.

use std::collections::HashMap;

use crate::core::batch::Batch;
use crate::core::bits;
use crate::core::buffer::BufferPool;
use crate::core::column::{Bits, Column, DataType, StrBuilder, VarType};
use crate::core::ctx::Ctx;
use crate::core::error::{ErrorSink, Exception};
use crate::core::id::SlotId;
use crate::core::variant;
use crate::format::record::RecordView;
use crate::plan::config::{Answer, OnException};
use crate::plan::exec::run;
use crate::plan::plan::Plan;

/// Run `plan` and serialize every row still live, in row order. Dropped rows are absent.
pub fn run_to_json<'a>(plan: &'a Plan, batch: &mut Batch, ctx: &mut Ctx<'a>) -> Vec<Vec<u8>> {
    run(plan, batch, ctx);
    // `peek`, not `get`: a source's metadata field is live whether or not an instruction reads it, and the runtime
    // builds a column only for one something reads (`Plan::reads_meta`).
    let serializer = RecordView::new(&plan.tree, |slot| batch.peek(slot));
    let live = batch.peek(plan.live_slot).bool().unwrap();

    let mut out = Vec::new();
    for row in 0..batch.rows() {
        if live.validity.is_valid(row) && bits::get_row(live.values, row) {
            out.push(serializer.row(row));
        }
    }
    out
}

/// One row's fate: its JSON if it survived, and every exception against it. Exceptions are already in final row
/// numbering, `Unroll` included.
pub struct PreviewRow {
    pub row: u32,
    /// Whether the preview's filter is true for the row. Every row matches when there is no filter.
    pub matched: bool,
    pub json: Option<Vec<u8>>,
    /// Each exception's message, with the answer its kind takes and the step that raised it, by its own id. No step
    /// for one raised reading the record, before any step.
    pub exceptions: Vec<(&'static str, String, Option<String>)>,
}

/// `run_to_json` for every row, with exceptions and the tag `on_exception` names: the GUI's preview. A row is shown as
/// its answer leaves it: tagged, as it is, or absent under `Drop` and `Deliver`; under `Block` it is shown tagged,
/// its exceptions saying why the source would stop. `show_internal` serializes `__`-prefixed fields too.
///
/// `batch` has the plan's `delivery_layout`: the tag is written into it, as `runtime` hands a row over, so `filter`
/// reads it too. `filter` sets each row's `matched`: a dropped row has no record to read and never matches.
pub fn run_to_json_marked<'a>(
    plan: &'a Plan,
    on_exception: &OnException,
    batch: &mut Batch,
    ctx: &mut Ctx<'a>,
    show_internal: bool,
    filter: Option<&RowFilter>,
) -> Vec<PreviewRow> {
    run(plan, batch, ctx);
    let rows = batch.rows();

    // Same map `runtime` resolves `row_exceptions`' field names from.
    let slot_names = plan.slot_names();

    let live = batch.peek(plan.live_slot).bool().unwrap();
    let (answers, raised) = answer_rows(
        ctx.errors.entries(),
        on_exception,
        &live,
        rows,
        &slot_names,
        &plan.step_ids,
    );
    let shown: Vec<bool> = (0..rows)
        .map(|row| {
            let dropped = matches!(answers[row], Answer::Drop | Answer::Deliver(_));
            live.validity.is_valid(row) && bits::get_row(live.values, row) && !dropped
        })
        .collect();
    // A pipeline previewed on its own runs under no route.
    let at = RaisedIn {
        route: None,
        pipeline: &plan.name,
    };
    let tag_slot = plan.tag_slot();
    batch.put(tag_slot, tag_column(&raised, at, rows, ctx.pool), ctx.pool);

    // The filter's own exceptions are not the pipeline's: a row it cannot decide on is left out, and marks nothing.
    let kept = filter.map(|f| {
        let mut errors = ErrorSink::new();
        let mut fctx = Ctx::at(ctx.pool, &mut errors, ctx.now_micros);
        fctx.patterns = &f.patterns;
        let mask = crate::expr::eval::eval(&f.ir, batch, &mut fctx);
        let kept: Vec<bool> = (0..rows)
            .map(|row| {
                mask.bool()
                    .is_ok_and(|b| b.validity.is_valid(row) && bits::get_row(b.values, row))
            })
            .collect();
        mask.reclaim(ctx.pool);
        kept
    });

    let record = || {
        if show_internal {
            RecordView::new_with_internal(&plan.tree, |slot| batch.peek(slot))
        } else {
            RecordView::new(&plan.tree, |slot| batch.peek(slot))
        }
    };
    let plain = record();
    // The tag first, as `runtime` hands it over.
    let tagged = record().with_field(on_exception.tag_field.as_str(), batch.peek(tag_slot));

    let mut by_row: HashMap<u32, Vec<(&'static str, String, Option<String>)>> = HashMap::new();
    for a in ctx.errors.entries() {
        let message = a.message(&slot_names);
        let messages = by_row.entry(a.row).or_default();
        // Two different coercions on one row are two different facts; dedup on the message, not the kind.
        if !messages.iter().any(|(_, m, _)| *m == message) {
            let step = plan.step_ids.get(a.function as usize).cloned();
            messages.push((on_exception.answer(&a.kind).label(), message, step));
        }
    }

    (0..rows)
        .map(|row| PreviewRow {
            row: row as u32,
            matched: kept.as_ref().is_none_or(|kept| shown[row] && kept[row]),
            json: shown[row].then(|| tag_or_not(&tagged, &plain, &raised, row).row(row)),
            exceptions: by_row.remove(&(row as u32)).unwrap_or_default(),
        })
        .collect()
}

/// A Bool over a pipeline's finished record (`topology::output_condition`): the rows a preview shows.
pub struct RowFilter {
    pub ir: crate::expr::ir::Ir,
    pub patterns: Vec<crate::core::ctx::CompiledPattern>,
}

/// Each row's answer: its exceptions' answers joined (`Answer::join`), `Ignore` on a row nothing marked or the pipeline
/// dropped. With it, each row's exceptions (`row_exceptions`): every exception on it that is not ignored. A row with any
/// carries the tag, whatever its answer; under drop and block it is not sent.
pub fn answer_rows(
    entries: &[Exception],
    on_exception: &OnException,
    live: &Bits,
    rows: usize,
    slot_names: &HashMap<SlotId, String>,
    step_ids: &[String],
) -> (Vec<Answer>, HashMap<u32, Vec<RowException>>) {
    let mut answers = vec![Answer::Ignore; rows];
    for a in entries {
        let row = a.row as usize;
        if let Some(answer) = answers
            .get_mut(row)
            .filter(|_| live.validity.is_valid(row) && bits::get_row(live.values, row))
        {
            answer.join(on_exception.answer(&a.kind));
        }
    }
    let kept = entries.iter().filter(|a| {
        answers
            .get(a.row as usize)
            .is_some_and(|answer| *answer != Answer::Ignore)
            && *on_exception.answer(&a.kind) != Answer::Ignore
    });
    let raised = row_exceptions(kept, slot_names, step_ids);
    (answers, raised)
}

/// One exception on a row, as its tag lists it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RowException {
    /// The step that raised it, by its own id. `None` for one raised reading the record, before any step.
    pub step: Option<String>,
    /// `ExceptionKind::label`.
    pub kind: &'static str,
    pub message: String,
}

/// Where a row's exceptions happened, laid beside them in its tag. `None` where nothing names it: a pipeline previewed
/// on its own has no route.
#[derive(Clone, Copy, Debug)]
pub struct RaisedIn<'a> {
    pub route: Option<&'a str>,
    pub pipeline: &'a str,
}

/// The tag's type: what `tag_column` writes on a row with exceptions.
pub fn tag_type() -> DataType {
    let exception = DataType::Struct(
        vec![
            ("step".to_string(), DataType::Str),
            ("kind".to_string(), DataType::Str),
            ("message".to_string(), DataType::Str),
        ],
        false,
    );
    DataType::Struct(
        vec![
            ("route".to_string(), DataType::Str),
            ("pipeline".to_string(), DataType::Str),
            ("exceptions".to_string(), DataType::Array(Box::new(exception))),
        ],
        false,
    )
}

/// The tag's column, a variant of `tag_type`: on a row with exceptions, where they happened and each one in the
/// order it happened. Null on every other row.
pub fn tag_column(
    raised: &HashMap<u32, Vec<RowException>>,
    at: RaisedIn<'_>,
    rows: usize,
    pool: &mut BufferPool,
) -> Column {
    let mut b = StrBuilder::new(pool, VarType::Variant, rows);
    let mut value = Vec::new();
    for row in 0..rows {
        let Some(raised) = raised.get(&(row as u32)) else {
            b.push_null();
            continue;
        };
        value.clear();
        let object = variant::begin_object(&mut value);
        let mut entries = 0;
        if let Some(route) = at.route {
            put(&mut value, "route", route);
            entries += 1;
        }
        put(&mut value, "pipeline", at.pipeline);
        variant::put_key(&mut value, "exceptions");
        let list = variant::begin_array(&mut value);
        for f in raised {
            let each = variant::begin_object(&mut value);
            if let Some(step) = &f.step {
                put(&mut value, "step", step);
            }
            put(&mut value, "kind", f.kind);
            put(&mut value, "message", &f.message);
            variant::end(&mut value, each, 2 + usize::from(f.step.is_some()));
        }
        variant::end(&mut value, list, raised.len());
        variant::end(&mut value, object, entries + 2);
        b.push_grow(pool, &value);
    }
    b.finish()
}

fn put(value: &mut Vec<u8>, key: &str, text: &str) {
    variant::put_key(value, key);
    variant::put_str(value, text);
}

/// The serializer for `row`: the tagged one where the row has exceptions, else the record as the pipeline left it, so a
/// healthy row keeps whatever it holds at the tag field.
pub fn tag_or_not<'s, 'a>(
    tagged: &'s RecordView<'a>,
    plain: &'s RecordView<'a>,
    raised: &HashMap<u32, Vec<RowException>>,
    row: usize,
) -> &'s RecordView<'a> {
    if raised.contains_key(&(row as u32)) {
        tagged
    } else {
        plain
    }
}

/// Row -> each exception on it, deduped, in first-seen order: what its tag lists. `step_ids` names each step by the
/// `FunctionId` that raised it.
pub fn row_exceptions<'a>(
    entries: impl IntoIterator<Item = &'a Exception>,
    slot_names: &HashMap<SlotId, String>,
    step_ids: &[String],
) -> HashMap<u32, Vec<RowException>> {
    let mut out: HashMap<u32, Vec<RowException>> = HashMap::new();
    for a in entries {
        let exception = RowException {
            step: step_ids.get(a.function as usize).cloned(),
            kind: a.kind.label(),
            message: a.message(slot_names),
        };
        let raised = out.entry(a.row).or_default();
        if !raised.contains(&exception) {
            raised.push(exception);
        }
    }
    out
}
