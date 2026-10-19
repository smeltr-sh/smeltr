//! A compiled `Plan` against a real batch, serialized through
//! `plan::output::run_to_json`. No parser step: per-row variation comes
//! from the reserved `__source.offset` slot, seeded directly.

use smeltr::core::batch::Batch;

use smeltr::core::column::Column;
use smeltr::core::column::DataType;
use smeltr::core::error::ExceptionKind;
use smeltr::plan::config::{FunctionKind, Step};

use smeltr::plan::topology::SourceMeta;

use crate::harness;
use harness::*;

/// Seed the reserved `__source.offset` slot with per-row values. Stands in for
/// varying input a real parser would eventually supply.
fn seed_offset(env: &mut Env, batch: &mut Batch, vals: &[i64]) {
    let mut c = Column::alloc(&mut env.pool, &DataType::I64, vals.len());
    c.i64_mut().unwrap().values.copy_from_slice(vals);
    batch.put(slot_of(SourceMeta::KAFKA, "__source.offset"), c, &mut env.pool);
}

fn json(bytes: &[u8]) -> &str {
    std::str::from_utf8(bytes).unwrap()
}

/// `n` is null where `__source.offset` is 0, so a condition on `n` is null there. `y` is `"prior"` on every row.
fn n_null_on_offset_zero() -> Step {
    code(0, r#"n = __source.offset > 0 ? __source.offset : null; y = "prior";"#)
}

// ---------------------------------------------------------------------------

#[test]
fn a_pipeline_with_no_functions_outputs_the_raw_input_unchanged() {
    // "Run nothing, get the input back" -- an empty pipeline must not
    // silently swallow its input.
    let plan = compile_ok(vec![]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 2, 0);
    seed_raw(&mut env, &mut b, &["hello world", "{\"a\":1}"]);

    let rows = env.run(&plan, &mut b);
    assert_eq!(rows.len(), 2);
    assert_eq!(json(&rows[0]), r#"{"_raw":"hello world"}"#);
    assert_eq!(json(&rows[1]), r#"{"_raw":"{\"a\":1}"}"#);
    b.reclaim(&mut env.pool);
}

#[test]
fn a_parser_or_eval_pipeline_still_carries_raw() {
    // `_raw` is a real value like any other field: it survives a
    // `Parser`/`Eval` step running, same as `x` does, rather than being
    // silently consumed on the pipeline's behalf.
    let plan = compile_ok(vec![code(0, "x = 1;")]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_raw(&mut env, &mut b, &["hello world"]);

    let rows = env.run(&plan, &mut b);
    assert_eq!(json(&rows[0]), r#"{"_raw":"hello world","x":1}"#);
    b.reclaim(&mut env.pool);
}

#[test]
fn raw_null_clears_it_like_any_other_field() {
    // The pipeline author's own escape hatch: an ordinary assignment,
    // same as clearing any other field, rather than a special "drop
    // _raw" mechanism.
    let plan = compile_ok(vec![code(0, "x = 1;"), code(1, "_raw = null;")]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_raw(&mut env, &mut b, &["hello world"]);

    let rows = env.run(&plan, &mut b);
    assert_eq!(json(&rows[0]), r#"{"x":1}"#);
    b.reclaim(&mut env.pool);
}

#[test]
fn varying_input_drives_varying_output() {
    let plan = compile_ok(vec![
        code(0, "status = __source.offset;"),
        code(1, r#"severity = status >= 400 ? "high" : "low";"#),
    ]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 3, 0);
    seed_offset(&mut env, &mut b, &[200, 500, 404]);

    let rows = env.run(&plan, &mut b);
    assert_eq!(rows.len(), 3);
    assert_eq!(json(&rows[0]), r#"{"severity":"low","status":200}"#);
    assert_eq!(json(&rows[1]), r#"{"severity":"high","status":500}"#);
    assert_eq!(json(&rows[2]), r#"{"severity":"high","status":404}"#);
    b.reclaim(&mut env.pool);
}

#[test]
fn dropped_rows_are_excluded_and_order_is_preserved() {
    let plan = compile_ok(vec![
        code(0, "status = __source.offset;"),
        code(1, "if (status >= 400) drop()"),
    ]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 5, 0);
    seed_offset(&mut env, &mut b, &[200, 500, 201, 404, 250]);

    let rows = env.run(&plan, &mut b);
    assert_eq!(rows.len(), 3, "only the < 400 rows survive");
    assert_eq!(json(&rows[0]), r#"{"status":200}"#);
    assert_eq!(json(&rows[1]), r#"{"status":201}"#);
    assert_eq!(json(&rows[2]), r#"{"status":250}"#);
    b.reclaim(&mut env.pool);
}

#[test]
fn everything_dropped_yields_no_output_rows() {
    let plan = compile_ok(vec![code(0, "x = 1;"), code(1, "if (x == 1) drop()")]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 4, 0);
    let rows = env.run(&plan, &mut b);
    assert!(rows.is_empty());
    b.reclaim(&mut env.pool);
}

#[test]
fn rename_survives_to_the_serialized_output() {
    let plan = compile_ok(vec![
        code(0, r#"srcip = "10.0.0.1";"#),
        code(1, "move(srcip, source.ip)"),
    ]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    let rows = env.run(&plan, &mut b);
    assert_eq!(json(&rows[0]), r#"{"source":{"ip":"10.0.0.1"}}"#);
    b.reclaim(&mut env.pool);
}

#[test]
fn conditional_branches_produce_the_right_value_per_row() {
    let plan = compile_ok(vec![
        code(0, "status = __source.offset;"),
        code(
            1,
            r#"severity: str = null;
            if (status >= 400) { severity = "high"; } else { severity = "low"; }"#,
        ),
    ]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 2, 0);
    seed_offset(&mut env, &mut b, &[500, 200]);

    let rows = env.run(&plan, &mut b);
    assert_eq!(json(&rows[0]), r#"{"severity":"high","status":500}"#);
    assert_eq!(json(&rows[1]), r#"{"severity":"low","status":200}"#);
    b.reclaim(&mut env.pool);
}

#[test]
fn a_nested_else_runs_only_where_its_outer_branch_does() {
    let plan = compile_ok(vec![
        code(0, "status = __source.offset;"),
        code(
            1,
            r#"inner: str = null;
            outer: str = null;
            if (status >= 100) {
                if (status >= 400) { inner = "high"; } else { inner = "low"; }
            } else {
                outer = "none";
            }"#,
        ),
    ]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 3, 0);
    seed_offset(&mut env, &mut b, &[500, 200, 50]);

    let rows = env.run(&plan, &mut b);
    assert_eq!(json(&rows[0]), r#"{"inner":"high","status":500}"#);
    assert_eq!(json(&rows[1]), r#"{"inner":"low","status":200}"#);
    assert_eq!(json(&rows[2]), r#"{"outer":"none","status":50}"#);
    b.reclaim(&mut env.pool);
}

#[test]
fn a_null_condition_keeps_the_prior_value_of_what_its_branch_assigns() {
    let plan = compile_ok(vec![n_null_on_offset_zero(), code(1, r#"if (n > 0) { y = "then"; }"#)]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 2, 0);
    seed_offset(&mut env, &mut b, &[0, 7]);

    let rows = env.run(&plan, &mut b);
    assert_eq!(json(&rows[0]), r#"{"y":"prior"}"#);
    assert_eq!(json(&rows[1]), r#"{"n":7,"y":"then"}"#);
    b.reclaim(&mut env.pool);
}

#[test]
fn a_filter_null_on_a_row_keeps_the_prior_value_of_what_its_step_assigns() {
    let mut filtered = func(
        1,
        FunctionKind::Eval {
            assignments: vec![("y".into(), "\"filtered\"".into())],
        },
    );
    if let Step::Function(f) = &mut filtered {
        f.filter = Some("n > 0".into());
    }
    let plan = compile_ok(vec![n_null_on_offset_zero(), filtered]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 2, 0);
    seed_offset(&mut env, &mut b, &[0, 7]);

    let rows = env.run(&plan, &mut b);
    assert_eq!(json(&rows[0]), r#"{"y":"prior"}"#);
    assert_eq!(json(&rows[1]), r#"{"n":7,"y":"filtered"}"#);
    b.reclaim(&mut env.pool);
}

#[test]
fn a_drop_under_a_null_condition_keeps_the_row() {
    let plan = compile_ok(vec![n_null_on_offset_zero(), code(1, "if (n > 0) drop()")]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 2, 0);
    seed_offset(&mut env, &mut b, &[0, 7]);

    let rows = env.run(&plan, &mut b);
    assert_eq!(rows.len(), 1, "the row with a true condition drops");
    assert_eq!(json(&rows[0]), r#"{"y":"prior"}"#);
    b.reclaim(&mut env.pool);
}

#[test]
fn an_if_else_under_a_null_outer_condition_runs_neither_branch() {
    let plan = compile_ok(vec![
        n_null_on_offset_zero(),
        code(1, "b = true; z = 0;"),
        code(2, r#"if (n > 0) { if (b) { z = 1; } else { y = "else"; } }"#),
    ]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 2, 0);
    seed_offset(&mut env, &mut b, &[0, 7]);

    let rows = env.run(&plan, &mut b);
    assert_eq!(json(&rows[0]), r#"{"b":true,"y":"prior","z":0}"#);
    assert_eq!(json(&rows[1]), r#"{"b":true,"n":7,"y":"prior","z":1}"#);
    b.reclaim(&mut env.pool);
}

#[test]
fn an_if_else_whose_own_condition_is_null_runs_its_else() {
    let plan = compile_ok(vec![
        n_null_on_offset_zero(),
        code(1, "z = 0;"),
        code(2, r#"if (true) { if (n > 0) { z = 1; } else { y = "else"; } }"#),
    ]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 2, 0);
    seed_offset(&mut env, &mut b, &[0, 7]);

    let rows = env.run(&plan, &mut b);
    assert_eq!(json(&rows[0]), r#"{"y":"else","z":0}"#);
    assert_eq!(json(&rows[1]), r#"{"n":7,"y":"prior","z":1}"#);
    b.reclaim(&mut env.pool);
}

#[test]
fn null_fields_are_omitted_end_to_end() {
    // The "else" branch never assigns `note`, so on that row it keeps its
    // declared null — omitted from output, not written as `"note":null`.
    let plan = compile_ok(vec![
        code(0, "status = __source.offset;"),
        code(1, r#"note: str = null; if (status >= 400) { note = "error"; }"#),
    ]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 2, 0);
    seed_offset(&mut env, &mut b, &[500, 200]);

    let rows = env.run(&plan, &mut b);
    assert_eq!(json(&rows[0]), r#"{"note":"error","status":500}"#);
    assert_eq!(json(&rows[1]), r#"{"status":200}"#, "note omitted, not null");
    b.reclaim(&mut env.pool);
}

#[test]
fn the_conditional_source_split_resolves_correctly_end_to_end() {
    // A scalar `source` re-expressed as either `source.ip` or `source.address`: a typed assignment, since a name
    // keeps its type. The object literal reads the old `source` and replaces it, so `source` itself is gone and
    // nothing conflicts.
    let plan = compile_ok(vec![
        code(0, "status = __source.offset;"),
        code(1, r#"source = status < 100 ? "192.168.1.1" : "2001:db8::1";"#),
        code(
            2,
            "source: {ip: str, address: str} = {ip: status < 100 ? source : null, address: status >= 100 ? source : null};",
        ),
    ]);
    assert!(plan.warnings.is_empty(), "{:?}", plan.warnings);

    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 2, 0);
    seed_offset(&mut env, &mut b, &[50, 150]);

    let rows = env.run(&plan, &mut b);
    // "status" is never nulled out or shadowed, so it's live and non-null
    // like any other field -- "every non-null field gets serialized, full
    // stop" (design doc) applies to it exactly the same as to "source.ip"/
    // "source.address".
    assert_eq!(json(&rows[0]), r#"{"source":{"ip":"192.168.1.1"},"status":50}"#);
    assert_eq!(json(&rows[1]), r#"{"source":{"address":"2001:db8::1"},"status":150}"#);
    b.reclaim(&mut env.pool);
}

#[test]
fn reserved_control_fields_never_appear_in_output() {
    // `__live`/`__source.offset` are pure routing/control state with no
    // meaningful value to a caller, unlike `_raw` (a real value, and
    // ordinarily present -- see `a_parser_or_eval_pipeline_still_carries_raw`).
    let plan = compile_ok(vec![code(0, "x = 1;"), code(1, "if (false) drop()")]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_offset(&mut env, &mut b, &[42]);

    let rows = env.run(&plan, &mut b);
    let text = json(&rows[0]);
    for reserved in ["__live", "__source.offset"] {
        assert!(!text.contains(reserved), "{text} leaked {reserved}");
    }
    b.reclaim(&mut env.pool);
}

#[test]
fn a_larger_batch_round_trips_row_for_row() {
    let plan = compile_ok(vec![
        code(0, "status = __source.offset;"),
        code(1, r#"severity = status >= 400 ? "high" : "low";"#),
        code(2, "if (status == 0) drop()"),
    ]);
    let mut env = Env::new();
    let n = 500;
    let mut b = Batch::new(&plan.slot_layout, n, 0);
    let vals: Vec<i64> = (0..n as i64).collect();
    seed_offset(&mut env, &mut b, &vals);

    let rows = env.run(&plan, &mut b);
    assert_eq!(rows.len(), n - 1, "row 0 is dropped by status == 0");
    assert_eq!(
        json(&rows[0]),
        r#"{"severity":"low","status":1}"#,
        "first surviving row is offset 1"
    );
    assert_eq!(
        json(rows.last().unwrap()),
        format!(r#"{{"severity":"high","status":{}}}"#, n - 1)
    );
    b.reclaim(&mut env.pool);
}

// ---------------------------------------------------------------------------
// What an expression *means*, observable in the value it computes.
//
// `expr::parse`'s own tests pin precedence structurally and `tests/ops.rs`
// pins the kernels. These are the composition the two only imply: the real
// compiler and evaluator agreeing, from source text to a serialized number.
// Asserted on the parsed value rather than the row text, because the shape of
// the row is what the tests above are for.
// ---------------------------------------------------------------------------

fn value(bytes: &[u8]) -> serde_json::Value {
    serde_json::from_slice(bytes).unwrap()
}

/// `body` over `__source.offset`, run against `offsets`, parsed.
fn compute(body: &str, offsets: &[i64]) -> Vec<serde_json::Value> {
    let plan = compile_ok(vec![code(0, body)]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, offsets.len(), 0);
    seed_offset(&mut env, &mut b, offsets);
    let rows = env.run(&plan, &mut b);
    let out = rows.iter().map(|r| value(r)).collect();
    b.reclaim(&mut env.pool);
    out
}

#[test]
fn precedence_and_parentheses_change_the_computed_value() {
    let got = compute(
        "bare = __source.offset + 2 * 3; grouped = (__source.offset + 2) * 3;",
        &[100, 200],
    );
    assert_eq!(got[0]["bare"], 106);
    assert_eq!(got[0]["grouped"], 306);
    assert_eq!(got[1]["bare"], 206);
    assert_eq!(got[1]["grouped"], 606);
}

#[test]
fn associativity_is_observable_in_the_result() {
    let got = compute("chained = __source.offset - 10 - 2; power = 2 ** 3 ** 2;", &[100]);
    assert_eq!(got[0]["chained"], 88, "(x - 10) - 2, not x - (10 - 2)");
    assert_eq!(got[0]["power"], 512.0, "2 ** (3 ** 2), not (2 ** 3) ** 2");
}

/// `/` between two integers is a float, and a `/` in that position is division
/// rather than the start of a regex literal -- the lexer would otherwise scan
/// to end of input looking for the closing slash.
#[test]
fn integer_division_yields_a_float() {
    let got = compute("half = __source.offset / 2;", &[7, 10]);
    assert_eq!(got[0]["half"], 3.5);
    assert_eq!(got[1]["half"], 5.0);
}

/// Four operands, one `ConcatN` rather than three binary concats.
#[test]
fn a_concat_chain_flattens_and_still_computes() {
    let plan = compile_ok(vec![code(0, r#"joined = _raw + "-" + _raw + "!";"#)]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 2, 0);
    seed_raw(&mut env, &mut b, &["a", "b"]);
    let rows = env.run(&plan, &mut b);
    assert_eq!(value(&rows[0])["joined"], "a-a!");
    assert_eq!(value(&rows[1])["joined"], "b-b!");
    b.reclaim(&mut env.pool);
}

/// An `i64` sum that overflows is null on that row and marked `Overflow`;
/// the column stays `i64`, the null flows through the rest of the tree, and
/// the other rows keep their values.
#[test]
fn overflow_is_null_on_its_row_and_the_expression_stays_i64() {
    let plan = compile_ok(vec![code(
        0,
        "big = 9223372036854775807 - __source.offset; sum = big + 1 + 10;",
    )]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 2, 0);
    seed_offset(&mut env, &mut b, &[0, 100]);
    let rows = env.run(&plan, &mut b);
    let (first, second) = (value(&rows[0]), value(&rows[1]));
    assert!(first.get("sum").is_none(), "the overflowed row is null: {first}");
    assert_eq!(second["sum"], 9223372036854775807i64 - 100 + 11);
    assert!(second["sum"].is_i64(), "the column stays i64: {second}");
    let overflow: Vec<_> = env
        .errors
        .entries()
        .iter()
        .filter(|a| matches!(a.kind, ExceptionKind::Overflow))
        .collect();
    assert_eq!(overflow.len(), 1, "{:?}", env.errors.entries());
    assert_eq!(overflow[0].row, 0);
    b.reclaim(&mut env.pool);
}

#[test]
fn hex_binary_and_underscored_literals_reach_the_value() {
    let got = compute(
        "hex = __source.offset + 0xFF; grouped = __source.offset + 1_000; octal = __source.offset + 0o17;",
        &[0],
    );
    assert_eq!(got[0]["hex"], 255);
    assert_eq!(got[0]["grouped"], 1000);
    assert_eq!(got[0]["octal"], 15);
}

#[test]
fn comments_are_skipped_without_changing_the_value() {
    let got = compute("doubled = __source.offset /* twice */ * 2; // trailing", &[5]);
    assert_eq!(got[0]["doubled"], 10);
}

#[test]
fn bitwise_operators_compute_end_to_end() {
    let got = compute(
        "masked = __source.offset & 0b0110; set = __source.offset | 1; shifted = __source.offset << 2; negated = -__source.offset;",
        &[0b1010],
    );
    assert_eq!(got[0]["masked"], 0b0010);
    assert_eq!(got[0]["set"], 0b1011);
    assert_eq!(got[0]["shifted"], 0b101000);
    assert_eq!(got[0]["negated"], -10);
}

/// Null propagates through arithmetic, and a null field is omitted from the
/// row rather than written as `null`.
#[test]
fn a_null_operand_makes_the_field_disappear() {
    let got = compute("sum = __source.offset + null;", &[1]);
    assert!(got[0].get("sum").is_none(), "got {}", got[0]);
}
