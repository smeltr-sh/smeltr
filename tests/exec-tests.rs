//! Executor tests: config in, `Plan` out, `Plan` run against a `Batch`.
//!
//! `compile-tests.rs` proves the compiler emits the right instructions.
//! `eval-tests.rs` proves a hand-built `Ir` computes the right column. Neither
//! proves the two connect — that a compiled `Plan`'s instructions, run in
//! order against a real batch, produce the values the pipeline says they
//! should. That is the phase-3 milestone this file closes out: "something
//! runnable."
//!
//! Pipelines here stick to assignments, `drop`, `move` and `unroll` over
//! literal-only expressions — nothing here needs a parser.
//! Every `ParserKind` now has a real kernel and its own end-to-end test
//! file (`pipeline-integration-tests.rs` for JSON, `csv-parser-tests.rs`,
//! `kv-parser-tests.rs`, `grok-parser-tests.rs`), so there's nothing left
//! for this file to pin about `Parser` specifically.

use smeltr::core::batch::Batch;
use smeltr::core::column::DataType;
use smeltr::core::id::FunctionId;
use smeltr::format::json::scalar_text;
use smeltr::plan::compile::Plan;
use smeltr::plan::config::{FunctionKind, Step};

use crate::harness;
use harness::*;

fn drop_step(id: FunctionId, cond: &str) -> Step {
    func(id, FunctionKind::Drop { cond: cond.to_string() })
}

/// The slot a name resolves to *at the end of compilation*. SSA means every
/// assignment appends a fresh `SlotOrigin` rather than overwriting one, so a
/// name reassigned more than once (`__live` across multiple `Drop`s, say)
/// appears several times in `provenance` — the last one is the current
/// binding.
fn slot_named(plan: &Plan, name: &str) -> usize {
    plan.provenance
        .iter()
        .rev()
        .find(|o| o.name == name)
        .unwrap_or_else(|| panic!("no slot named `{name}` in {:?}", plan.provenance))
        .slot
}

// ---------------------------------------------------------------------------

#[test]
fn eval_computes_over_a_real_batch() {
    let plan = compile_ok(vec![code(0, "x = 10;"), code(1, "y = x * 2;")]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 4, 0);
    env.exec(&plan, &mut b);

    let y = slot_named(&plan, "y");
    assert_eq!(b.get(y).i64().unwrap().values, &[20, 20, 20, 20]);
    b.reclaim(&mut env.pool);
}

#[test]
fn rename_survives_execution_unchanged() {
    // Rename emits no instruction (the phase-3 acceptance test in
    // compile-tests.rs); this proves the *executor* agrees — the renamed
    // name resolves to a slot that actually holds the right data at runtime,
    // not just at compile time.
    let plan = compile_ok(vec![code(0, "y = 10 * 2;"), code(1, "move(y, total)")]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 3, 0);
    env.exec(&plan, &mut b);

    // `10 * 2` folds to a literal before it reaches a kernel, and nothing reads the slot, so it holds one value
    // for the whole batch. Read it the way the serializer does.
    // The rename binds `total` to `y`'s slot without an instruction, so only the tree knows the name.
    let total = plan.tree.leaf("total").expect("`total` is a leaf");
    assert_eq!(plan.tree.leaf("y"), None);
    for row in 0..3 {
        assert_eq!(scalar_text(b.get(total), row).as_deref(), Some("20"));
    }
    b.reclaim(&mut env.pool);
}

#[test]
fn drop_clears_live_for_matching_rows() {
    let plan = compile_ok(vec![code(0, "total = 20;"), code(1, "if (total > 15) drop()")]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 5, 0);
    env.exec(&plan, &mut b);

    // `__live` starts all-true; Drop's condition is true for every row here.
    let live = slot_named(&plan, "__live");
    let l = b.get(live).bool().unwrap();
    for i in 0..5 {
        assert!(l.validity.is_valid(i), "row {i} should still be valid");
        assert!(
            !smeltr::core::bits::get_row(l.values, i),
            "row {i} should have been dropped"
        );
    }
    b.reclaim(&mut env.pool);
}

#[test]
fn drop_with_an_unknown_condition_does_not_drop() {
    // A Drop condition that reads a null field (e.g. one nulled by an
    // upstream Unparsable) is neither definitely true nor definitely
    // false -- "unknown is not true", so the row
    // must stay live. Before the fix, Kleene AND/NOT made the new `__live`
    // binding itself invalid rather than `true`, and every `__live` reader
    // treats an invalid row exactly like a dropped one.
    let plan = compile_ok(vec![code(0, "x: i64 = null;"), code(1, "if (x == 5) drop()")]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 3, 0);
    env.exec(&plan, &mut b);

    let live = slot_named(&plan, "__live");
    let l = b.get(live).bool().unwrap();
    for i in 0..3 {
        assert!(l.validity.is_valid(i), "row {i}'s liveness must be known");
        assert!(
            smeltr::core::bits::get_row(l.values, i),
            "row {i} must stay live -- an unknown Drop condition never drops"
        );
    }
    b.reclaim(&mut env.pool);
}

#[test]
fn drop_with_a_blank_condition_never_drops() {
    // An empty condition string means "no condition set yet," authored as
    // one boolean field -- not a parse error, and not "drop nothing" via
    // some separate opt-out mechanism.
    let plan = compile_ok(vec![code(0, "x = 1;"), drop_step(1, "")]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 3, 0);
    env.exec(&plan, &mut b);

    let live = slot_named(&plan, "__live");
    let l = b.get(live).bool().unwrap();
    for i in 0..3 {
        assert!(l.validity.is_valid(i));
        assert!(smeltr::core::bits::get_row(l.values, i), "row {i} must stay live");
    }
    b.reclaim(&mut env.pool);
}

#[test]
fn unroll_fans_out_rows_and_drops_parent_fields_by_default() {
    // Two source rows, each with the same 3-element array -- Unroll must
    // grow the batch to 6 rows with `tags` rebound from Array(I64) to I64
    // holding each element in order.
    //
    // `region` is *not* carried onto those rows. Duplicating a parent
    // field into every element is quadratic in the fan-out, and `_raw`
    // (the parent record itself, O(N) bytes for an N-element array) makes
    // that the dominant cost: 40KB in, 800MB out at 20_000 elements. So
    // the default drops parent fields and an author opts back in.
    let plan = compile_ok(vec![
        code(0, r#"region = "us"; tags = [10, 20, 30];"#),
        code(1, "unroll(tags)"),
    ]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 2, 0);
    env.exec(&plan, &mut b);

    assert_eq!(b.rows(), 6, "2 source rows x 3 elements each");
    let tags = slot_named(&plan, "tags");
    assert_eq!(b.get(tags).i64().unwrap().values, &[10, 20, 30, 10, 20, 30]);
    let region = slot_named(&plan, "region");
    assert!(
        b.get(region).is_unbound(),
        "the default mode must release parent fields, not duplicate them"
    );
    b.reclaim(&mut env.pool);
}

#[test]
fn unroll_duplicates_parent_fields_when_the_author_opts_in() {
    // The same pipeline with `keepParentFields: true` -- `region` is an
    // ordinary field Unroll then never touches, duplicated across every
    // row an original row fanned out into ("everything survives unless
    // explicitly dropped"). Paying the fan-out cost is fine when it is
    // asked for; the point of the default is that it stops being silent.
    let plan = compile_ok(vec![
        code(0, r#"region = "us"; tags = [10, 20, 30];"#),
        code(1, "unroll(tags, true)"),
    ]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 2, 0);
    env.exec(&plan, &mut b);

    assert_eq!(b.rows(), 6);
    let tags = slot_named(&plan, "tags");
    assert_eq!(b.get(tags).i64().unwrap().values, &[10, 20, 30, 10, 20, 30]);
    // `region` is a literal nothing reads, so it is one value and a row count, and `gather` carried the value into
    // the wider batch rather than six copies of it. Read it the way the serializer does.
    let region = slot_named(&plan, "region");
    let region_col = b.get(region);
    for row in 0..6 {
        assert_eq!(scalar_text(region_col, row).as_deref(), Some("us"));
    }
    b.reclaim(&mut env.pool);
}

#[test]
fn unroll_skips_rows_already_dropped_before_it_runs() {
    // A memory-bound mitigation, not just a correctness nicety (`status.md`'s "Unroll's output is not bounded at
    // dispatch"): a row
    // dropped earlier in the pipeline contributes zero output rows to
    // Unroll's fan-out, rather than wastefully exploding data nothing
    // downstream will ever see.
    let plan = compile_ok(vec![
        code(0, "tags = [1, 2, 3];"),
        code(1, "drop()"), // drops every row
        code(2, "unroll(tags)"),
    ]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 2, 0);
    env.exec(&plan, &mut b);

    assert_eq!(
        b.rows(),
        0,
        "every source row was already dropped, so Unroll fans out to nothing"
    );
    b.reclaim(&mut env.pool);
}

#[test]
fn unroll_releases_the_source_array_instead_of_copying_it_into_every_output_row() {
    // `Batch::gather` copies every slot once per output row, so a row that
    // fans out into N elements would copy its whole N-element array N
    // times -- quadratic in the fan-out of a single record, for data
    // nothing reads: the compiler rebinds the array's name to the
    // flattened per-element column, and an unnamed slot is never
    // serialized.
    //
    // Measured on one ~400KB record holding 200_000 elements, that asked
    // for ~137GB. An allocation that size aborts the process instead of
    // unwinding, so `Worker::tick`'s `catch_unwind` cannot contain it and
    // the batch never commits -- the worker dies and crash-loops on the
    // same record after restart. At a survivable 20_000 elements it was
    // still 5.6GB peak RSS against 2.4GB once the array is released.
    //
    // So this asserts the source array slot is left unbound after Unroll.
    // It is a memory property with no visible effect on output, which is
    // exactly the kind that regresses silently -- every other unroll test
    // here passes either way.
    let plan = compile_ok(vec![
        code(0, r#"region = "us"; tags = [10, 20, 30];"#),
        code(1, "unroll(tags)"),
    ]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 2, 0);
    env.exec(&plan, &mut b);

    // `tags` now names the flattened I64 column; the original Array(I64)
    // slot is the one Unroll consumed and should have released.
    let array_slot = plan
        .slot_layout
        .iter()
        .position(|t| matches!(t, DataType::Array(_)))
        .expect("the pre-unroll Array(I64) slot is still in the layout");
    assert!(
        b.get(array_slot).is_unbound(),
        "Unroll left the source array bound, so `gather` copied the whole \
         array into all {} output rows -- quadratic in the fan-out",
        b.rows()
    );

    // ...and the fan-out itself is still exactly right.
    assert_eq!(b.rows(), 6);
    let tags = slot_named(&plan, "tags");
    assert_eq!(b.get(tags).i64().unwrap().values, &[10, 20, 30, 10, 20, 30]);
    b.reclaim(&mut env.pool);
}
