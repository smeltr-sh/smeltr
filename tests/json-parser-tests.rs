//! End-to-end tests for the JSON parser kernel: a compiled `Plan` that parses
//! `_raw` with `json(_raw)`, run against JSON documents, out through
//! `plan::output::run_to_json`.

use smeltr::core::batch::Batch;
use smeltr::core::column::DataType;
use smeltr::core::error::ExceptionKind;
use smeltr::plan::config::{DeclaredField, FunctionConfig, FunctionKind, ParserKind, Step};

use crate::harness;
use harness::*;

/// A parse of `_raw` as JSON, after `declared`: `a: i64; b.c: str;`.
fn json(declared: &str) -> String {
    format!("{declared} Object.assign($, json(_raw));")
}

// ---------------------------------------------------------------------------

#[test]
fn objects_flatten_into_dotted_names() {
    let plan = compile_ok(vec![code(0, &json("source.ip: str;")), code(1, "out = source.ip;")]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_raw(&mut env, &mut b, &[r#"{"source":{"ip":"1.2.3.4"}}"#]);

    let rows = env.run(&plan, &mut b);
    assert_eq!(
        parsed(&rows[0]),
        parsed(br#"{"_raw":"{\"source\":{\"ip\":\"1.2.3.4\"}}","out":"1.2.3.4","source":{"ip":"1.2.3.4"}}"#)
    );
    b.reclaim(&mut env.pool);
}

/// A live output field's buffer is never reused by a later same-shape
/// instruction before serialization reads it.
#[test]
fn a_live_field_survives_a_later_instruction_with_a_matching_reuse_shape() {
    let plan = compile_ok(vec![
        code(0, &json("status: i64;")),
        code(1, "severity = status >= 400 ? \"high\" : \"low\";"),
        code(2, "__scratch = 1;"),
    ]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_raw(&mut env, &mut b, &[r#"{"status":200}"#]);

    let rows = env.run(&plan, &mut b);
    assert_eq!(
        parsed(&rows[0]),
        parsed(br#"{"_raw":"{\"status\":200}","severity":"low","status":200}"#)
    );
    b.reclaim(&mut env.pool);
}

/// An undeclared key passes through as it arrived: each row keeps its own number, an integer beside a float.
#[test]
fn an_undeclared_field_keeps_each_rows_number_as_it_arrived() {
    let plan = compile_ok(vec![code(0, &json(""))]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 2, 0);
    seed_raw(&mut env, &mut b, &[r#"{"port":80}"#, r#"{"port":8080.5}"#]);

    let rows = env.run(&plan, &mut b);
    assert_eq!(parsed(&rows[0]), parsed(br#"{"_raw":"{\"port\":80}","port":80}"#));
    assert_eq!(
        parsed(&rows[1]),
        parsed(br#"{"_raw":"{\"port\":8080.5}","port":8080.5}"#)
    );
    b.reclaim(&mut env.pool);
}

/// Rows that disagree each keep their own value: nothing is rendered as text.
#[test]
fn an_undeclared_field_keeps_each_rows_kind() {
    let plan = compile_ok(vec![code(0, &json(""))]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 2, 0);
    seed_raw(&mut env, &mut b, &[r#"{"flag":true}"#, r#"{"flag":5}"#]);

    let rows = env.run(&plan, &mut b);
    assert_eq!(parsed(&rows[0]), parsed(br#"{"_raw":"{\"flag\":true}","flag":true}"#));
    assert_eq!(parsed(&rows[1]), parsed(br#"{"_raw":"{\"flag\":5}","flag":5}"#));
    b.reclaim(&mut env.pool);
}

#[test]
fn a_missing_key_is_per_row_null_not_a_failure() {
    let plan = compile_ok(vec![code(0, &json("maybe: i64;")), code(1, "out = maybe;")]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 2, 0);
    seed_raw(&mut env, &mut b, &[r#"{"maybe":1}"#, r#"{"other":2}"#]);

    let rows = env.run(&plan, &mut b);
    assert_eq!(
        parsed(&rows[0]),
        parsed(br#"{"_raw":"{\"maybe\":1}","maybe":1,"out":1}"#)
    );
    assert_eq!(parsed(&rows[1]), parsed(br#"{"_raw":"{\"other\":2}","other":2}"#));
    assert!(env.errors.is_empty());
    b.reclaim(&mut env.pool);
}

#[test]
fn malformed_json_nulls_that_row_and_marks_without_affecting_others() {
    let plan = compile_ok(vec![code(0, &json("x: i64;")), code(1, "out = x;")]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 3, 0);
    seed_raw(&mut env, &mut b, &[r#"{"x":1}"#, "not json at all", r#"{"x":3}"#]);

    let rows = env.run(&plan, &mut b);
    assert_eq!(parsed(&rows[0]), parsed(br#"{"_raw":"{\"x\":1}","out":1,"x":1}"#));
    assert_eq!(parsed(&rows[1]), parsed(br#"{"_raw":"not json at all"}"#));
    assert_eq!(parsed(&rows[2]), parsed(br#"{"_raw":"{\"x\":3}","out":3,"x":3}"#));
    assert!(
        env.errors
            .entries()
            .iter()
            .any(|a| a.row == 1 && matches!(a.kind, ExceptionKind::Unparsable)),
        "{:?}",
        env.errors.entries()
    );
    b.reclaim(&mut env.pool);
}

/// A lossy read is marked on the row it lost from, and the row keeps the rest.
#[test]
fn a_read_that_loses_a_value_warns_on_its_row() {
    let plan = compile_ok(vec![code(0, &json("v: f64; m: str;"))]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 3, 0);
    seed_raw(
        &mut env,
        &mut b,
        &[
            r#"{"v":NaN,"m":"a"}"#,
            r#"{"v":1.5,"m":"caf\xE9"}"#,
            r#"{"v":2.5,"m":"\x22"}"#,
        ],
    );

    let rows = env.run(&plan, &mut b);
    assert_eq!(
        parsed(&rows[0]),
        parsed(br#"{"_raw":"{\"v\":NaN,\"m\":\"a\"}","m":"a"}"#)
    );
    assert_eq!(
        parsed(&rows[1]),
        parsed(r#"{"_raw":"{\"v\":1.5,\"m\":\"caf\\xE9\"}","m":"caf\ufffd","v":1.5}"#.as_bytes())
    );
    assert_eq!(
        parsed(&rows[2]),
        parsed(br#"{"_raw":"{\"v\":2.5,\"m\":\"\\x22\"}","m":"\"","v":2.5}"#)
    );
    let kinds: Vec<_> = env.errors.entries().iter().map(|a| (a.row, a.kind.label())).collect();
    assert_eq!(kinds, [(0, "NaN"), (1, "InvalidUtf8")]);
    b.reclaim(&mut env.pool);
}

#[test]
fn a_non_object_top_level_value_is_also_a_parse_failure() {
    let plan = compile_ok(vec![code(0, &json("x: i64;")), code(1, "out = x;")]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_raw(&mut env, &mut b, &[r#"[1,2,3]"#]);

    let rows = env.run(&plan, &mut b);
    assert_eq!(parsed(&rows[0]), parsed(br#"{"_raw":"[1,2,3]"}"#));
    assert_eq!(env.errors.len(), 1);
    b.reclaim(&mut env.pool);
}

#[test]
fn declared_type_mismatch_marks_the_fields_own_slot_and_nulls_that_row() {
    let plan = compile_ok(vec![code(0, &json("status: i64;"))]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 2, 0);
    seed_raw(&mut env, &mut b, &[r#"{"status":200}"#, r#"{"status":"five-hundred"}"#]);

    let rows = env.run(&plan, &mut b);
    assert_eq!(parsed(&rows[0]), parsed(br#"{"_raw":"{\"status\":200}","status":200}"#));
    assert_eq!(
        parsed(&rows[1]),
        parsed(br#"{"_raw":"{\"status\":\"five-hundred\"}"}"#),
        "mismatched type -> null, not a crash"
    );
    assert!(
        env.errors
            .entries()
            .iter()
            .any(|a| a.row == 1 && matches!(a.kind, ExceptionKind::UnparsableField { .. })),
        "{:?}",
        env.errors.entries()
    );
    b.reclaim(&mut env.pool);
}

/// A failure taken back by a rewrite, through the parser rather than the sink alone: the later key's value is the
/// row's, and the exception follows the order the keys came in.
#[test]
fn a_failed_declared_cell_is_taken_back_by_a_later_key_in_the_row() {
    let plan = compile_ok(vec![code(0, &json("a: i64;"))]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 2, 0);
    seed_raw(&mut env, &mut b, &[r#"{"a":"x","a":2}"#, r#"{"a":2,"a":"x"}"#]);

    let rows = env.run_json(&plan, &mut b);
    assert_eq!(rows[0]["a"], serde_json::json!(2), "the failure was taken back");
    assert_eq!(
        rows[1]["a"],
        serde_json::Value::Null,
        "the last write is the one that failed"
    );
    let failed: Vec<u32> = env
        .errors
        .entries()
        .iter()
        .filter(|a| matches!(a.kind, ExceptionKind::UnparsableField { .. }))
        .map(|a| a.row)
        .collect();
    assert_eq!(failed, vec![1], "{:?}", env.errors.entries());
    b.reclaim(&mut env.pool);
}

/// A declared `Str` cell that failed on a container is taken back by a later key in the same row: the builder's
/// position and validity bit return to the row, so the rewrite is what the row carries, valid, with no type failure.
/// The repeated key is a contradiction of the record, named by no field, and that is the row's one exception.
#[test]
fn a_rewrite_after_a_failed_str_cell_is_valid_with_no_type_failure() {
    let plan = compile_ok(vec![code(0, &json("s: str;"))]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_raw(&mut env, &mut b, &[r#"{"s":{"o":1},"s":"fourth"}"#]);

    let rows = env.run_json(&plan, &mut b);
    assert_eq!(rows[0]["s"], serde_json::json!("fourth"));
    let kinds: Vec<_> = env.errors.entries().iter().map(|a| (a.field, a.kind.clone())).collect();
    assert_eq!(kinds, vec![(None, ExceptionKind::KeyContradiction { key: "s".into() })]);
    b.reclaim(&mut env.pool);
}

/// A declared scalar's column is charged from the start of the batch, so a batch whose records fit and whose sinks
/// do not is refused before a row is parsed. The refusal leaves the batch as it was and the budget clean once the
/// batch is given back.
#[test]
fn a_batch_whose_sinks_do_not_fit_is_refused_before_a_row_is_parsed() {
    use smeltr::core::buffer::{BufferPool, MemoryBudget, catch_over_budget};
    use smeltr::core::ctx::Ctx;
    use smeltr::core::error::ErrorSink;

    let plan = compile_ok(vec![code(0, &json("s: str;"))]);
    let texts: Vec<String> = (0..4096).map(|i| format!(r#"{{"s":"value-{i}"}}"#)).collect();
    let texts: Vec<&str> = texts.iter().map(String::as_str).collect();

    // What the records alone cost, measured. The budget leaves 4 KiB past it, less than the sink's offsets alone.
    let seeded = {
        let probe = MemoryBudget::new(usize::MAX);
        let mut pool = BufferPool::with_budget(probe.clone());
        let b = raw_batch(&plan, &texts, &mut pool);
        let used = probe.used();
        b.reclaim(&mut pool);
        used
    };
    let budget = MemoryBudget::new(seeded + 4096);
    let mut pool = BufferPool::with_budget(budget.clone());
    let mut errors = ErrorSink::new();
    let mut b = raw_batch(&plan, &texts, &mut pool);

    let refused = catch_over_budget(|| {
        let mut ctx = Ctx::new(&mut pool, &mut errors);
        smeltr::plan::exec::run(&plan, &mut b, &mut ctx);
    });
    assert!(refused.is_err(), "the sinks alone are more than 4 KiB");
    assert!(errors.entries().is_empty(), "no row was parsed");
    b.reclaim(&mut pool);
    drop(pool);
    assert_eq!(budget.used(), 0, "nothing stayed charged");
}

#[test]
fn a_key_nothing_claims_still_reaches_the_output() {
    let plan = compile_ok(vec![code(0, &json(""))]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_raw(&mut env, &mut b, &[r#"{"untouched":"still here"}"#]);

    let rows = env.run(&plan, &mut b);
    assert_eq!(
        parsed(&rows[0]),
        parsed(br#"{"_raw":"{\"untouched\":\"still here\"}","untouched":"still here"}"#)
    );
    b.reclaim(&mut env.pool);
}

#[test]
fn a_scalar_element_array_leaves_as_it_arrived() {
    let plan = compile_ok(vec![code(0, &json(""))]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_raw(&mut env, &mut b, &[r#"{"tags":[1,2,3]}"#]);

    let rows = env.run(&plan, &mut b);
    assert_eq!(
        parsed(&rows[0]),
        parsed(br#"{"_raw":"{\"tags\":[1,2,3]}","tags":[1,2,3]}"#)
    );
    b.reclaim(&mut env.pool);
}

#[test]
fn an_array_of_mixed_integers_and_floats_keeps_each_elements_kind() {
    let plan = compile_ok(vec![code(0, &json(""))]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_raw(&mut env, &mut b, &[r#"{"vals":[1,2.5,3]}"#]);

    let rows = env.run(&plan, &mut b);
    assert_eq!(
        parsed(&rows[0]),
        parsed(br#"{"_raw":"{\"vals\":[1,2.5,3]}","vals":[1,2.5,3]}"#)
    );
    b.reclaim(&mut env.pool);
}

#[test]
fn an_array_of_arrays_leaves_as_it_arrived() {
    let plan = compile_ok(vec![code(0, &json(""))]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_raw(&mut env, &mut b, &[r#"{"tags":[[1,2],[3]]}"#]);

    let rows = env.run(&plan, &mut b);
    assert_eq!(
        parsed(&rows[0]),
        parsed(br#"{"_raw":"{\"tags\":[[1,2],[3]]}","tags":[[1,2],[3]]}"#)
    );
    b.reclaim(&mut env.pool);
}

#[test]
fn an_array_of_objects_leaves_as_it_arrived() {
    let plan = compile_ok(vec![code(0, &json(""))]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_raw(&mut env, &mut b, &[r#"{"items":[{"id":1},{"id":2}]}"#]);

    let rows = env.run(&plan, &mut b);
    assert_eq!(
        parsed(&rows[0]),
        parsed(br#"{"_raw":"{\"items\":[{\"id\":1},{\"id\":2}]}","items":[{"id":1},{"id":2}]}"#)
    );
    b.reclaim(&mut env.pool);
}

#[test]
fn an_array_of_elements_of_different_shapes_keeps_each_element() {
    let plan = compile_ok(vec![code(0, &json(""))]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_raw(&mut env, &mut b, &[r#"{"tags":[{"x":1},[2,3]]}"#]);

    let rows = env.run(&plan, &mut b);
    assert_eq!(
        parsed(&rows[0]),
        parsed(br#"{"_raw":"{\"tags\":[{\"x\":1},[2,3]]}","tags":[{"x":1},[2,3]]}"#)
    );
    b.reclaim(&mut env.pool);
}

#[test]
fn a_declared_array_of_struct_field_coerces_nested_types() {
    let plan = compile_ok(vec![code(0, &json("items: array<{id: i64}>;"))]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 2, 0);
    seed_raw(
        &mut env,
        &mut b,
        &[
            r#"{"items":[{"id":1},{"id":2}]}"#,
            r#"{"items":[{"id":"not a number"}]}"#,
        ],
    );

    let rows = env.run(&plan, &mut b);
    assert_eq!(
        parsed(&rows[0]),
        parsed(br#"{"_raw":"{\"items\":[{\"id\":1},{\"id\":2}]}","items":[{"id":1},{"id":2}]}"#)
    );
    assert_eq!(
        parsed(&rows[1]),
        parsed(br#"{"_raw":"{\"items\":[{\"id\":\"not a number\"}]}"}"#),
        "a bad nested field nulls the whole row"
    );
    assert!(
        env.errors
            .entries()
            .iter()
            .any(|a| a.row == 1 && matches!(a.kind, ExceptionKind::UnparsableField { .. })),
        "{:?}",
        env.errors.entries()
    );
    b.reclaim(&mut env.pool);
}

#[test]
fn a_field_thats_an_array_in_one_row_and_a_scalar_in_another_keeps_both() {
    let plan = compile_ok(vec![code(0, &json(""))]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 2, 0);
    seed_raw(&mut env, &mut b, &[r#"{"v":[1,2]}"#, r#"{"v":"scalar"}"#]);

    let rows = env.run(&plan, &mut b);
    assert_eq!(parsed(&rows[0]), parsed(br#"{"_raw":"{\"v\":[1,2]}","v":[1,2]}"#));
    assert_eq!(
        parsed(&rows[1]),
        parsed(br#"{"_raw":"{\"v\":\"scalar\"}","v":"scalar"}"#)
    );
    b.reclaim(&mut env.pool);
}

#[test]
fn an_empty_array_is_a_real_zero_length_array_not_null() {
    let plan = compile_ok(vec![code(0, &json(""))]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_raw(&mut env, &mut b, &[r#"{"tags":[]}"#]);

    let rows = env.run(&plan, &mut b);
    assert_eq!(parsed(&rows[0]), parsed(br#"{"_raw":"{\"tags\":[]}","tags":[]}"#));
    b.reclaim(&mut env.pool);
}

#[test]
fn a_missing_array_field_stays_null_across_rows_that_do_have_it() {
    let plan = compile_ok(vec![code(0, &json(""))]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 2, 0);
    seed_raw(&mut env, &mut b, &[r#"{"tags":[1,2]}"#, r#"{}"#]);

    let rows = env.run(&plan, &mut b);
    assert_eq!(parsed(&rows[0]), parsed(br#"{"_raw":"{\"tags\":[1,2]}","tags":[1,2]}"#));
    assert_eq!(parsed(&rows[1]), parsed(br#"{"_raw":"{}"}"#));
    b.reclaim(&mut env.pool);
}

/// Selecting a branch isn't the same as that branch having a value: `select_list` used to mark a row valid just
/// because the condition picked it, even when the picked branch's own row was null (a declared array field the
/// document didn't have) -- serializing an empty array where the row should stay null and be omitted entirely.
#[test]
fn a_ternary_over_arrays_stays_null_when_the_selected_branch_is_itself_null() {
    let plan = compile_ok(vec![
        code(0, &json("tags: array<i64>; flag: bool;")),
        code(1, "other = [9, 9]; picked = flag ? tags : other;"),
    ]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 2, 0);
    seed_raw(&mut env, &mut b, &[r#"{"tags":[1,2],"flag":true}"#, r#"{"flag":true}"#]);
    let rows = env.run(&plan, &mut b);
    assert_eq!(parsed(&rows[0])["picked"], serde_json::json!([1, 2]));
    assert!(
        parsed(&rows[1]).get("picked").is_none(),
        "the selected branch (`tags`) is itself null on this row, so `picked` stays null, not an empty array"
    );
    b.reclaim(&mut env.pool);
}

/// Two array literals built from one declared field blend as arrays of its type.
#[test]
fn a_ternary_over_arrays_built_from_a_declared_field_blends() {
    let plan = compile_ok(vec![
        code(0, &json("s: str; flag: bool;")),
        code(1, "a = [s]; b = [s]; picked = flag ? a : b;"),
    ]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_raw(&mut env, &mut b, &[r#"{"s":"hello","flag":true}"#]);
    let rows = env.run(&plan, &mut b);
    assert_eq!(parsed(&rows[0])["picked"], serde_json::json!(["hello"]));
    b.reclaim(&mut env.pool);
}

#[test]
fn a_declared_array_i64_field_coerces_and_narrows_strictly() {
    let plan = compile_ok(vec![code(0, &json("tags: array<i64>;"))]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 2, 0);
    seed_raw(
        &mut env,
        &mut b,
        &[r#"{"tags":[1,2,3]}"#, r#"{"tags":["not","numbers"]}"#],
    );

    let rows = env.run(&plan, &mut b);
    assert_eq!(
        parsed(&rows[0]),
        parsed(br#"{"_raw":"{\"tags\":[1,2,3]}","tags":[1,2,3]}"#)
    );
    assert_eq!(
        parsed(&rows[1]),
        parsed(br#"{"_raw":"{\"tags\":[\"not\",\"numbers\"]}"}"#),
        "wrongly-typed element -> null row"
    );
    assert!(
        env.errors
            .entries()
            .iter()
            .any(|a| a.row == 1 && matches!(a.kind, ExceptionKind::UnparsableField { .. })),
        "{:?}",
        env.errors.entries()
    );
    b.reclaim(&mut env.pool);
}

#[test]
fn a_duplicate_key_is_last_write_wins_through_the_real_dynamic_route() {
    let plan = compile_ok(vec![code(0, &json(""))]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_raw(&mut env, &mut b, &[r#"{"a":1,"a":2,"a":3}"#]);

    let rows = env.run(&plan, &mut b);
    assert_eq!(
        parsed(&rows[0]),
        parsed(br#"{"_raw":"{\"a\":1,\"a\":2,\"a\":3}","a":3}"#)
    );
    b.reclaim(&mut env.pool);
}

#[test]
fn a_number_that_overflows_to_infinity_is_no_value_and_a_warning_not_a_silent_null() {
    let plan = compile_ok(vec![code(0, &json(""))]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_raw(&mut env, &mut b, &[r#"{"a":1e400}"#]);

    let rows = env.run(&plan, &mut b);
    assert_eq!(
        parsed(&rows[0]),
        parsed(br#"{"_raw":"{\"a\":1e400}"}"#),
        "overflow -> no value, not silent"
    );
    assert!(
        env.errors
            .entries()
            .iter()
            .all(|a| a.row == 0 && a.kind == ExceptionKind::NaN),
        "{:?}",
        env.errors.entries()
    );
    assert_eq!(env.errors.len(), 1);
    b.reclaim(&mut env.pool);
}

/// A number no f64 holds is no value, as `null` is: a declared object at its name sees nothing.
#[test]
fn a_number_no_f64_holds_meets_a_declared_object_as_null_does() {
    let plan = compile_ok(vec![code(0, &json("s: {a: i64};"))]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_raw(&mut env, &mut b, &[r#"{"s":NaN}"#]);

    env.run(&plan, &mut b);
    let kinds: Vec<_> = env.errors.entries().iter().map(|a| a.kind.label()).collect();
    assert_eq!(kinds, ["NaN"], "{:?}", env.errors.entries());
    b.reclaim(&mut env.pool);
}

/// A record's keys are data, however many there are: every one reaches the output where the record had it.
#[test]
fn a_high_cardinality_key_space_keeps_every_key() {
    let n = 1000;
    let doc = {
        let mut s = String::from("{\"metrics\":{");
        for i in 0..n {
            if i > 0 {
                s.push(',');
            }
            s.push_str(&format!(r#""k{i}":{i}"#));
        }
        s.push_str("}}");
        s
    };
    let plan = compile_ok(vec![code(0, &json(""))]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_raw(&mut env, &mut b, &[&doc]);

    let rows = env.run(&plan, &mut b);
    let v = parsed(&rows[0]);
    let metrics = v["metrics"].as_object().expect("an object");
    assert_eq!(metrics.len(), n);
    for i in 0..n {
        assert_eq!(metrics.get(&format!("k{i}")).unwrap(), &serde_json::json!(i));
    }
    b.reclaim(&mut env.pool);
}

/// One output row per element with every other field duplicated; an empty
/// or absent array contributes no rows.
#[test]
fn unroll_keeping_parents_duplicates_named_and_dynamic_fields_end_to_end() {
    let plan = compile_ok(vec![
        code(0, &json("tags: array<i64>;")),
        code(1, "unroll(tags, true);"),
    ]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 3, 0);
    seed_raw(
        &mut env,
        &mut b,
        &[
            r#"{"tags":[1,2],"host":"web01"}"#,
            r#"{"tags":[],"host":"web02"}"#,
            r#"{"host":"web03"}"#,
        ],
    );

    let rows = env.run(&plan, &mut b);
    assert_eq!(rows.len(), 2, "row 0's 2 elements; rows 1 and 2 contribute none");
    assert_eq!(
        parsed(&rows[0]),
        parsed(br#"{"_raw":"{\"tags\":[1,2],\"host\":\"web01\"}","tags":1,"host":"web01"}"#)
    );
    assert_eq!(
        parsed(&rows[1]),
        parsed(br#"{"_raw":"{\"tags\":[1,2],\"host\":\"web01\"}","tags":2,"host":"web01"}"#)
    );
    b.reclaim(&mut env.pool);
}

#[test]
fn unroll_excludes_rows_dropped_earlier_in_the_pipeline() {
    let plan = compile_ok(vec![
        code(0, &json("tags: array<i64>; host: str;")),
        code(1, "if (host == \"web02\") drop();"),
        code(2, "unroll(tags);"),
    ]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 2, 0);
    seed_raw(
        &mut env,
        &mut b,
        &[r#"{"tags":[1,2],"host":"web01"}"#, r#"{"tags":[3,4],"host":"web02"}"#],
    );

    let rows = env.run(&plan, &mut b);
    assert_eq!(
        rows.len(),
        2,
        "web02's row was dropped before Unroll, so its own 2 elements never fan out"
    );
    // Default mode drops the parent's fields, `_raw` and `host` included.
    assert_eq!(parsed(&rows[0]), parsed(br#"{"tags":1}"#));
    assert_eq!(parsed(&rows[1]), parsed(br#"{"tags":2}"#));
    b.reclaim(&mut env.pool);
}

#[test]
fn a_realistic_multi_field_document_round_trips() {
    let plan = compile_ok(vec![
        code(0, &json("status: i64;")),
        code(1, "severity = status >= 400 ? \"high\" : \"low\";"),
    ]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_raw(
        &mut env,
        &mut b,
        &[r#"{"status":500,"source":{"ip":"10.0.0.1","port":443},"host":"web01"}"#],
    );

    let rows = env.run(&plan, &mut b);
    assert_eq!(
        parsed(&rows[0]),
        parsed(
            br#"{"_raw":"{\"status\":500,\"source\":{\"ip\":\"10.0.0.1\",\"port\":443},\"host\":\"web01\"}","severity":"high","status":500,"source":{"ip":"10.0.0.1","port":443},"host":"web01"}"#
        )
    );
    b.reclaim(&mut env.pool);
}

/// A parse of `tags`, an array of objects: what a JSON array of objects parses to.
const TAGS: &str = "tags: array<{name: str, n: i64}>; Object.assign($, json(_raw));";

/// A field of an element of a declared array of objects reads by `.name`
/// and by `["name"]`, and the two agree.
#[test]
fn a_field_of_an_object_inside_an_array_reads_by_dot_and_by_bracket() {
    let plan = compile_ok(vec![
        code(0, TAGS),
        code(
            1,
            "by_dot = tags[0].name; by_bracket = tags[0][\"name\"]; second_num = tags[1].n;",
        ),
    ]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_raw(
        &mut env,
        &mut b,
        &[r#"{"tags":[{"name":"a","n":1},{"name":"b","n":2}]}"#],
    );
    let rows = env.run(&plan, &mut b);
    let got = parsed(&rows[0]);
    assert_eq!(got["by_dot"], serde_json::json!("a"));
    assert_eq!(got["by_dot"], got["by_bracket"]);
    assert_eq!(got["second_num"], serde_json::json!(2));
    b.reclaim(&mut env.pool);
}

/// An out-of-range element reads null in every field.
#[test]
fn an_out_of_range_object_element_reads_null_in_every_field() {
    let plan = compile_ok(vec![
        code(0, TAGS),
        code(1, "missing = tags[9].name; missing_n = tags[9].n;"),
    ]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_raw(&mut env, &mut b, &[r#"{"tags":[{"name":"a","n":1}]}"#]);
    let rows = env.run(&plan, &mut b);
    let got = parsed(&rows[0]);
    assert!(got.get("missing").is_none(), "got {got}");
    assert!(got.get("missing_n").is_none(), "got {got}");
    b.reclaim(&mut env.pool);
}

/// A row with no array reads null without disturbing its neighbor.
#[test]
fn a_row_with_no_array_at_all_reads_null_without_disturbing_its_neighbor() {
    let plan = compile_ok(vec![code(0, TAGS), code(1, "first = tags[0].name;")]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 2, 0);
    seed_raw(
        &mut env,
        &mut b,
        &[r#"{"tags":[{"name":"a","n":1}]}"#, r#"{"other":1}"#],
    );
    let rows = env.run(&plan, &mut b);
    assert_eq!(parsed(&rows[0])["first"], serde_json::json!("a"));
    assert!(parsed(&rows[1]).get("first").is_none());
    b.reclaim(&mut env.pool);
}

/// Dot access chains through a nested object inside an element.
#[test]
fn dot_access_chains_through_a_nested_object_inside_an_element() {
    let plan = compile_ok(vec![
        code(0, &json("sites: array<{geo: {city: str}}>;")),
        code(1, "c = sites[0].geo.city;"),
    ]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_raw(&mut env, &mut b, &[r#"{"sites":[{"geo":{"city":"Oslo"}}]}"#]);
    let rows = env.run(&plan, &mut b);
    assert_eq!(parsed(&rows[0])["c"], serde_json::json!("Oslo"));
    b.reclaim(&mut env.pool);
}

/// A declared scalar coerces a quoted value that is an exact rendering of
/// it, without a parse failure.
#[test]
fn a_declared_scalar_coerces_a_quoted_value_of_that_type() {
    let plan = compile_ok(vec![code(0, &json("n: i64; f: f64; b: bool;"))]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_raw(&mut env, &mut b, &[r#"{"n":"40","f":"1.5","b":"true"}"#]);
    let rows = env.run(&plan, &mut b);
    let got = parsed(&rows[0]);
    assert_eq!(got["n"], serde_json::json!(40));
    assert_eq!(got["f"], serde_json::json!(1.5));
    assert_eq!(got["b"], serde_json::json!(true));
    assert!(env.errors.entries().is_empty(), "{:?}", env.errors.entries());
    b.reclaim(&mut env.pool);
}

/// A scalar under a declared `Str` stringifies.
#[test]
fn a_declared_str_stringifies_a_scalar() {
    let plan = compile_ok(vec![code(0, &json("a: str; b: str; c: str;"))]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_raw(&mut env, &mut b, &[r#"{"a":40,"b":1.5,"c":true}"#]);
    let rows = env.run(&plan, &mut b);
    let got = parsed(&rows[0]);
    assert_eq!(got["a"], serde_json::json!("40"));
    assert_eq!(got["c"], serde_json::json!("true"));
    b.reclaim(&mut env.pool);
}

/// A declared `i64` takes a fraction by rounding it, half away from zero,
/// quoted or not: nothing truncates.
#[test]
fn a_declared_i64_rounds_a_fraction_half_away_from_zero() {
    for (raw, want) in [
        (r#"{"v":1.9}"#, 2),
        (r#"{"v":"1.9"}"#, 2),
        (r#"{"v":2.5}"#, 3),
        (r#"{"v":-2.5}"#, -3),
        (r#"{"v":0.4}"#, 0),
    ] {
        let plan = compile_ok(vec![code(0, &json("v: i64;"))]);
        let mut env = Env::new();
        let mut b = Batch::new(&plan.slot_layout, 1, 0);
        seed_raw(&mut env, &mut b, &[raw]);
        let rows = env.run(&plan, &mut b);
        assert_eq!(parsed(&rows[0])["v"], serde_json::json!(want), "{raw}");
        assert!(env.errors.entries().is_empty(), "{raw}: {:?}", env.errors.entries());
        b.reclaim(&mut env.pool);
    }
}

/// A number outside `i64` has no rounding into it: the field is null and the
/// row records the failure, quoted or not.
#[test]
fn a_declared_i64_refuses_a_value_outside_its_range() {
    for raw in [r#"{"v":1e300}"#, r#"{"v":"1e300"}"#, r#"{"v":-1e300}"#] {
        let plan = compile_ok(vec![code(0, &json("v: i64;"))]);
        let mut env = Env::new();
        let mut b = Batch::new(&plan.slot_layout, 1, 0);
        seed_raw(&mut env, &mut b, &[raw]);
        let rows = env.run(&plan, &mut b);
        assert!(parsed(&rows[0]).get("v").is_none(), "{raw} does not fit an i64");
        assert!(
            env.errors
                .entries()
                .iter()
                .any(|a| matches!(a.kind, ExceptionKind::UnparsableField { .. })),
            "{raw} should record a parse failure"
        );
        b.reclaim(&mut env.pool);
    }
}

/// A string that is not a rendering of the type is still a failure.
#[test]
fn a_declared_scalar_still_refuses_an_uncoercible_string() {
    for raw in [r#"{"v":"abc"}"#, r#"{"v":"4 0"}"#, r#"{"v":""}"#] {
        let plan = compile_ok(vec![code(0, &json("v: i64;"))]);
        let mut env = Env::new();
        let mut b = Batch::new(&plan.slot_layout, 1, 0);
        seed_raw(&mut env, &mut b, &[raw]);
        let rows = env.run(&plan, &mut b);
        assert!(parsed(&rows[0]).get("v").is_none(), "{raw} must not coerce");
        b.reclaim(&mut env.pool);
    }
}

/// An object under a declared scalar coerces by the one table, as a declaration anywhere does: `str` takes its JSON
/// text, and a number refuses it, a failure recorded on the row.
#[test]
fn a_nested_value_under_a_declared_scalar_coerces_by_the_table() {
    let plan = compile_ok(vec![code(0, &json("v: str;"))]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_raw(&mut env, &mut b, &[r#"{"v":{"a":1}}"#]);
    let rows = env.run(&plan, &mut b);
    assert_eq!(parsed(&rows[0])["v"], r#"{"a":1}"#);
    assert!(env.errors.entries().is_empty(), "{:?}", env.errors.entries());
    b.reclaim(&mut env.pool);

    let plan = compile_ok(vec![code(0, &json("v: i64;"))]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_raw(&mut env, &mut b, &[r#"{"v":{"a":1}}"#]);
    let rows = env.run(&plan, &mut b);
    assert!(parsed(&rows[0]).get("v").is_none(), "an object is no i64");
    assert!(
        env.errors
            .entries()
            .iter()
            .any(|a| matches!(a.kind, ExceptionKind::UnparsableField { .. })),
        "and it is recorded, not dropped silently"
    );
    b.reclaim(&mut env.pool);
}

/// A whole number fits `i64` however it is rendered, and a fraction rounds into it.
#[test]
fn a_declared_i64_takes_a_number_however_rendered() {
    let plan = compile_ok(vec![code(0, &json("n: i64;"))]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 4, 0);
    seed_raw(
        &mut env,
        &mut b,
        &[r#"{"n":2.0}"#, r#"{"n":"2e3"}"#, r#"{"n":1.5}"#, r#"{"n":"5"}"#],
    );
    let rows = env.run(&plan, &mut b);
    assert_eq!(parsed(&rows[0])["n"], 2);
    assert_eq!(parsed(&rows[1])["n"], 2000);
    assert_eq!(parsed(&rows[2])["n"], 2, "1.5 rounds half away from zero");
    assert_eq!(parsed(&rows[3])["n"], 5);
    assert!(env.errors.entries().is_empty(), "{:?}", env.errors.entries());
    b.reclaim(&mut env.pool);
}

/// A declared top-level `Struct` receives its object, with coercion and
/// enforcement reaching inside it.
#[test]
fn a_declared_struct_field_receives_its_object_and_enforces_inner_types() {
    let plan = compile_ok(vec![code(0, &json("v: {a: i64};"))]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 3, 0);
    seed_raw(
        &mut env,
        &mut b,
        &[r#"{"v":{"a":1}}"#, r#"{"v":{"a":"7"}}"#, r#"{"v":{"a":"abc"}}"#],
    );
    let rows = env.run(&plan, &mut b);
    assert_eq!(parsed(&rows[0])["v"], serde_json::json!({"a": 1}));
    assert_eq!(parsed(&rows[1])["v"], serde_json::json!({"a": 7}));
    assert!(parsed(&rows[2]).get("v").is_none());
    b.reclaim(&mut env.pool);
}

/// An open declared object types the keys it names and keeps the rest as they arrived: its declaration adds types and
/// drops nothing, at any depth.
#[test]
fn a_declared_struct_keeps_the_keys_it_does_not_name() {
    let plan = compile_ok(vec![
        code(0, &json("v: {a: i64, ...}; items: array<{a: i64, ...}>;")),
        code(1, "first = items[0]; n = v.a + 1;"),
    ]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_raw(
        &mut env,
        &mut b,
        &[r#"{"v":{"a":"1","zz":9},"items":[{"a":2,"k":[1]},{"a":3}]}"#],
    );
    let rows = env.run(&plan, &mut b);
    let row = parsed(&rows[0]);
    assert_eq!(row["v"], serde_json::json!({"a": 1, "zz": 9}), "`a` typed, `zz` kept");
    assert_eq!(row["n"], 2);
    assert_eq!(row["items"], serde_json::json!([{"a": 2, "k": [1]}, {"a": 3}]));
    assert_eq!(
        row["first"],
        serde_json::json!({"a": 2, "k": [1]}),
        "an element keeps its own"
    );
    b.reclaim(&mut env.pool);
}

/// A closed declared object types the keys it names and drops the rest, silently: the declaration says so.
#[test]
fn a_closed_declared_struct_drops_the_keys_it_does_not_name() {
    let plan = compile_ok(vec![code(0, &json("v: {a: i64};")), code(1, "n = v.a + 1;")]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_raw(
        &mut env,
        &mut b,
        &[r#"{"v":{"a":"1","zz":9},"items":[{"a":2,"k":[1]},{"a":3}]}"#],
    );
    let rows = env.run(&plan, &mut b);
    let row = parsed(&rows[0]);
    assert_eq!(row["v"], serde_json::json!({"a": 1}), "`a` typed, `zz` dropped");
    assert_eq!(row["n"], 2);
    assert!(env.errors.entries().is_empty(), "{:?}", env.errors.entries());
    b.reclaim(&mut env.pool);
}

/// A closed object inside a declared array keeps the fields it names and drops every other key, silently.
#[test]
fn a_closed_object_in_a_declared_array_drops_the_keys_it_does_not_name() {
    let plan = compile_ok(vec![code(0, &json("items: array<{a: i64}>;"))]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 2, 0);
    seed_raw(
        &mut env,
        &mut b,
        &[r#"{"items":[{"a":"2","k":[1]},{"a":3}]}"#, r#"{"items":[{"a":4}]}"#],
    );
    let rows = env.run(&plan, &mut b);
    assert_eq!(
        parsed(&rows[0])["items"],
        serde_json::json!([{"a": 2}, {"a": 3}]),
        "`a` typed, `k` dropped"
    );
    assert_eq!(parsed(&rows[1])["items"], serde_json::json!([{"a": 4}]));
    assert!(env.errors.entries().is_empty(), "{:?}", env.errors.entries());
    assert!(env.errors.error_rows().is_empty(), "{:?}", env.errors.entries());
    b.reclaim(&mut env.pool);
}

/// Coercion applies at every level of a declared type.
#[test]
fn coercion_reaches_inside_a_declared_array_of_objects() {
    let plan = compile_ok(vec![
        code(0, &json("rows: array<{n: i64, ok: bool}>;")),
        code(1, "first = rows[0].n;"),
    ]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_raw(
        &mut env,
        &mut b,
        &[r#"{"rows":[{"n":"7","ok":"false"},{"n":8,"ok":true}]}"#],
    );
    let rows = env.run(&plan, &mut b);
    assert_eq!(parsed(&rows[0])["first"], serde_json::json!(7));
    b.reclaim(&mut env.pool);
}

/// A field of each declared type, assigned under a guard, keeps its value and its type: the blend reads each column
/// as its own type.
#[test]
fn a_field_assigned_under_a_guard_keeps_its_value() {
    let raw = r#"{"s":"txt","f":1.5,"b":true,"i":7,"arr":[1,2],"n":9}"#;
    let parse = json("s: str; f: f64; b: bool; i: i64; arr: array<i64>; n: i64;");
    let expected: &[(&str, &str, serde_json::Value)] = &[
        ("s", "str", serde_json::json!("txt")),
        ("f", "f64", serde_json::json!(1.5)),
        ("b", "bool", serde_json::json!(true)),
        ("i", "i64", serde_json::json!(7)),
        ("arr", "array<i64>", serde_json::json!([1, 2])),
    ];
    for (field, ty, want) in expected {
        let plan = compile_ok(vec![
            code(0, &parse),
            code(3, &format!("out: {ty} = null;")),
            Step::If {
                id: 2,
                cond: "n > 1".into(),
                then: vec![code(1, &format!("out = {field};"))],
                else_: vec![],
            },
        ]);
        let mut env = Env::new();
        let mut b = Batch::new(&plan.slot_layout, 1, 0);
        seed_raw(&mut env, &mut b, &[raw]);
        let rows = env.run(&plan, &mut b);
        assert_eq!(&parsed(&rows[0])["out"], want, "field {field}");
        b.reclaim(&mut env.pool);
    }
}

/// The guard still governs: a row the condition excludes gets no value.
#[test]
fn a_guard_that_excludes_a_row_still_leaves_the_field_unset() {
    let plan = compile_ok(vec![
        code(0, &json("s: str; n: i64;")),
        code(3, "out: str = null;"),
        Step::If {
            id: 2,
            cond: "n < 0".into(),
            then: vec![code(1, "out = s;")],
            else_: vec![],
        },
    ]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_raw(&mut env, &mut b, &[r#"{"s":"txt","n":9}"#]);
    let rows = env.run(&plan, &mut b);
    assert!(parsed(&rows[0]).get("out").is_none());
    b.reclaim(&mut env.pool);
}

/// A guarded dynamic parser parses only the rows its guard selects; another row keeps what it held.
/// `csv-parser-tests.rs`'s `a_guarded_parser_leaves_a_masked_row_alone` covers the static case.
#[test]
fn a_guarded_dynamic_parser_parses_the_rows_it_selects() {
    let plan = compile_ok(vec![
        code(0, &json("kind: str;")),
        code(1, "amt = 0;"),
        Step::If {
            id: 3,
            cond: "kind == \"traffic\"".into(),
            then: vec![code(2, &json(""))],
            else_: vec![],
        },
    ]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 2, 0);
    seed_raw(
        &mut env,
        &mut b,
        &[r#"{"kind":"traffic","amt":5}"#, r#"{"kind":"other","amt":7}"#],
    );
    let rows = env.run(&plan, &mut b);
    assert_eq!(parsed(&rows[0])["amt"], serde_json::json!(5));
    assert_eq!(parsed(&rows[1])["amt"], serde_json::json!(0));
    b.reclaim(&mut env.pool);
}

/// Per-row, not per-batch: the blend must pick each row's own branch.
#[test]
fn a_guarded_assignment_blends_row_by_row() {
    let plan = compile_ok(vec![
        code(0, &json("s: str; n: i64;")),
        code(1, "out = \"seed\";"),
        Step::If {
            id: 3,
            cond: "n > 5".into(),
            then: vec![code(2, "out = s;")],
            else_: vec![],
        },
    ]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 2, 0);
    seed_raw(&mut env, &mut b, &[r#"{"s":"hit","n":9}"#, r#"{"s":"miss","n":1}"#]);
    let rows = env.run(&plan, &mut b);
    assert_eq!(parsed(&rows[0])["out"], serde_json::json!("hit"));
    assert_eq!(parsed(&rows[1])["out"], serde_json::json!("seed"));
    b.reclaim(&mut env.pool);
}

/// An array literal built from declared fields keeps their values.
#[test]
fn an_array_literal_of_declared_fields_keeps_their_values() {
    let raw = r#"{"s":"txt","f":1.5,"b":true,"n":9}"#;
    let cases: &[(&str, serde_json::Value)] = &[
        ("[s, s]", serde_json::json!(["txt", "txt"])),
        ("[f, f]", serde_json::json!([1.5, 1.5])),
        ("[b, b]", serde_json::json!([true, true])),
        ("[n, n]", serde_json::json!([9, 9])),
        // A mix of a declared field and a literal of the same type.
        ("[s, \"lit\"]", serde_json::json!(["txt", "lit"])),
    ];
    let parse = json("s: str; f: f64; b: bool; n: i64;");
    for (expr, want) in cases {
        let plan = compile_ok(vec![code(0, &parse), code(1, &format!("out = {expr};"))]);
        let mut env = Env::new();
        let mut b = Batch::new(&plan.slot_layout, 1, 0);
        seed_raw(&mut env, &mut b, &[raw]);
        let rows = env.run(&plan, &mut b);
        assert_eq!(&parsed(&rows[0])["out"], want, "expr {expr}");
        b.reclaim(&mut env.pool);
    }
}

/// An array literal of nulls is all null.
#[test]
fn an_array_literal_of_nulls_is_still_all_null() {
    let plan = compile_ok(vec![code(0, &json("nosuch: i64;")), code(1, "out = [nosuch, nosuch];")]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_raw(&mut env, &mut b, &[r#"{"n":9}"#]);
    let rows = env.run(&plan, &mut b);
    assert_eq!(parsed(&rows[0])["out"], serde_json::json!([null, null]));
    b.reclaim(&mut env.pool);
}

// -- declared `datetime` fields: sugar over a parser-pinned `Str` -------------
//
// `ts: datetime` is what an expression sees; the parse's `formats`, or auto without one, is what tells the convert pass
// after the parse how to read the text once the parse has pinned the field to `Str`. A JSON number stringifies into that `Str` like any other
// declared `Str` field, so a number and a digit string arrive the same. The field is parsed exactly as an ordinary
// declared `Str` field would be, then converted.

/// `ts` declared `datetime` and read auto, with no format.
const AUTO: &str = "ts: datetime; Object.assign($, json(_raw));";

/// `parse`, which declares a `datetime` field named `ts`, and a batch of `records` through it and `evals`.
fn datetime_rows(parse: &str, evals: &str, records: &[&str]) -> (Vec<serde_json::Value>, Env) {
    let plan = compile_ok(vec![code(0, parse), code(1, evals)]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, records.len(), 0);
    seed_raw(&mut env, &mut b, records);
    let rows: Vec<serde_json::Value> = env.run(&plan, &mut b).iter().map(|r| parsed(r)).collect();
    b.reclaim(&mut env.pool);
    (rows, env)
}

/// Auto reads a number, the same digits as text, a fraction, and a text stamp, in one batch, and every unit of the
/// epoch ladder lands on the same instant.
#[test]
fn a_declared_datetime_field_reads_numbers_digit_strings_and_text_stamps() {
    let (rows, env) = datetime_rows(
        AUTO,
        "back = epochMillis(ts);",
        &[
            r#"{"ts":1700000000}"#,
            r#"{"ts":"1700000000"}"#,
            r#"{"ts":1700000000000}"#,
            r#"{"ts":1700000000000000}"#,
            r#"{"ts":1700000000000000000}"#,
            r#"{"ts":1700000000.5}"#,
            r#"{"ts":"2023-11-14T22:13:20Z"}"#,
            r#"{"ts":"2023/11/14 22:13:20"}"#,
        ],
    );
    for (i, row) in rows.iter().enumerate() {
        let want = if i == 5 {
            1_700_000_000_500i64
        } else {
            1_700_000_000_000i64
        };
        assert_eq!(row["back"], serde_json::json!(want), "row {i}: {row}");
    }
    assert!(env.errors.entries().is_empty(), "{:?}", env.errors.entries());
}

#[test]
fn a_declared_datetime_field_with_a_format_reads_that_shape() {
    let (rows, env) = datetime_rows(
        r#"ts: datetime; Object.assign($, json(_raw, {formats: {ts: "%d/%m/%Y %H:%M"}}));"#,
        "y = year(ts); m = month(ts); d = day(ts); h = hour(ts);",
        &[r#"{"ts":"15/03/2024 07:30"}"#],
    );
    assert_eq!(rows[0]["y"], serde_json::json!(2024));
    assert_eq!(rows[0]["m"], serde_json::json!(3));
    assert_eq!(rows[0]["d"], serde_json::json!(15));
    assert_eq!(rows[0]["h"], serde_json::json!(7));
    assert!(env.errors.entries().is_empty(), "{:?}", env.errors.entries());
}

/// `""` under a declared number, bool or datetime is an empty value, not one that failed: null, and not a failure.
/// A declared `str` keeps it.
#[test]
fn an_empty_string_under_a_typed_field_is_null_and_clean() {
    let plan = compile_ok(vec![code(
        0,
        &json("i: i64; f: f64; b: bool; ts: datetime; s: str; a: array<i64>;"),
    )]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_raw(
        &mut env,
        &mut b,
        &[r#"{"i":"","f":" ","b":"","ts":"","s":"","a":["1","",3]}"#],
    );
    let rows = env.run(&plan, &mut b);
    let doc = parsed(&rows[0]);
    for f in ["i", "f", "b", "ts"] {
        assert_eq!(doc[f], serde_json::Value::Null, "`{f}`: {doc}");
    }
    assert_eq!(doc["s"], serde_json::json!(""), "{doc}");
    assert_eq!(doc["a"], serde_json::json!([1, null, 3]), "{doc}");
    assert!(env.errors.entries().is_empty(), "{:?}", env.errors.entries());
    b.reclaim(&mut env.pool);
}

/// Text that names no instant fails only its own row, under auto and under a format alike, with the
/// `UnparsableField` an ordinary declared `I64` field gets. A format also fails the row auto would have read: the
/// space form is a shape auto knows and the `T` format does not.
#[test]
fn a_value_that_is_not_a_time_fails_only_its_own_row() {
    let format = r#"ts: datetime; Object.assign($, json(_raw, {formats: {ts: "%Y-%m-%dT%H:%M:%SZ"}}));"#;
    for (how, want) in [(AUTO, vec![1]), (format, vec![1, 2])] {
        let (rows, env) = datetime_rows(
            how,
            "",
            &[
                r#"{"ts":"2023-11-14T22:13:20Z"}"#,
                r#"{"ts":"not-a-time"}"#,
                r#"{"ts":"2023-11-14 22:13:20"}"#,
            ],
        );
        assert!(rows[0].get("ts").is_some(), "{how}: {:?}", rows[0]);
        assert!(
            rows[1].get("ts").is_none(),
            "{how}: mismatched shape -> null, not a crash: {:?}",
            rows[1]
        );
        let failed: Vec<u32> = env
            .errors
            .entries()
            .iter()
            .filter(|a| matches!(a.kind, ExceptionKind::UnparsableField { .. }))
            .map(|a| a.row)
            .collect();
        assert_eq!(failed, want, "{how}: {:?}", env.errors.entries());
    }
}

/// A missing field is a null, not a failure: there was no text to misread.
#[test]
fn an_absent_datetime_field_is_null_without_a_parse_failure() {
    let (rows, env) = datetime_rows(AUTO, "", &[r#"{"other":1}"#]);
    assert!(rows[0].get("ts").is_none(), "{:?}", rows[0]);
    assert!(
        !env.errors
            .entries()
            .iter()
            .any(|a| matches!(a.kind, ExceptionKind::UnparsableField { .. })),
        "{:?}",
        env.errors.entries()
    );
}

/// A name is a value or an object: one parser declaring both `a` and `a.b` is a compile error on the parser's step.
#[test]
fn a_parser_declaring_a_name_and_a_path_under_it_is_a_compile_error() {
    let declared = vec![
        DeclaredField {
            name: "a".into(),
            ty: DataType::Struct(vec![("b".into(), DataType::I64), ("c".into(), DataType::I64)], false),
            time_source: None,
        },
        DeclaredField {
            name: "a.b".into(),
            ty: DataType::I64,
            time_source: None,
        },
    ];
    let parser = FunctionKind::Parser {
        input: "_raw".into(),
        into: String::new(),
        kind: ParserKind::Json { split_dots: false },
        declared,
    };
    let e = compile_err(vec![func(0, parser)]);
    assert!(
        e.iter()
            .any(|x| x.function == Some(0) && x.message.contains("`a` is already")),
        "{e:?}"
    );
}

/// `docs/architecture/language.md`: `a.b` and `a["b"]` are one read of one path. Both reach the leaf `b` of the object
/// `a` a parser declared, and the same bracket read reaches its sibling `c`.
#[test]
fn dot_and_index_syntax_reach_the_same_leaf() {
    let plan = compile_ok(vec![
        code(0, &json("a: {b: i64, c: i64};")),
        code(1, "dot_form = a.b; index_form = a[\"b\"]; sibling = a[\"c\"];"),
    ]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_raw(&mut env, &mut b, &[r#"{"a":{"b":1,"c":2}}"#]);

    let rows = env.run(&plan, &mut b);
    let row = parsed(&rows[0]);
    assert_eq!(row["dot_form"], 1, "{row:?}");
    assert_eq!(row["index_form"], 1, "{row:?}");
    assert_eq!(row["sibling"], 2, "{row:?}");
    b.reclaim(&mut env.pool);
}

/// An open declared object is its leaves and its unclaimed fields: with `splitDots`, a key under it arrives by its
/// path, however the record spells it, and each key no leaf takes lands in its unclaimed fields.
#[test]
fn a_declared_objects_keys_meet_however_the_record_spells_them() {
    let plan = compile_ok(vec![code(
        0,
        "a: {b: i64, ...}; Object.assign($, json(_raw, {splitDots: true}));",
    )]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 2, 0);
    seed_raw(
        &mut env,
        &mut b,
        &[r#"{"a.b":"1","a":{"c":2},"a.d":{"e":3}}"#, r#"{"a":{"b":4}}"#],
    );
    let rows = env.run(&plan, &mut b);
    assert_eq!(
        parsed(&rows[0])["a"],
        serde_json::json!({"b": 1, "c": 2, "d": {"e": 3}})
    );
    assert_eq!(parsed(&rows[1])["a"], serde_json::json!({"b": 4}));
    assert!(plan.tree.leaf("a.b").is_some(), "a declared object's field is a leaf");
    b.reclaim(&mut env.pool);
}

/// A value that is no object, at a declared object's name, fails the coercion: nothing is written there and the row
/// is marked. A failing leaf of the object is marked at the leaf.
#[test]
fn a_declared_object_that_meets_no_object_fails_its_row() {
    let plan = compile_ok(vec![code(0, &json("a: {b: i64};"))]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 2, 0);
    seed_raw(&mut env, &mut b, &[r#"{"a":5,"k":1}"#, r#"{"a":{"b":"x"}}"#]);
    let rows = env.run(&plan, &mut b);
    assert_eq!(
        parsed(&rows[0]),
        serde_json::json!({"_raw": r#"{"a":5,"k":1}"#, "k": 1})
    );
    assert!(parsed(&rows[1]).get("a").is_none());
    let failed: Vec<u32> = env
        .errors
        .entries()
        .iter()
        .filter(|a| matches!(a.kind, ExceptionKind::UnparsableField { .. }))
        .map(|a| a.row)
        .collect();
    assert_eq!(failed, vec![0, 1]);
    // The exception names the field by the name a failure message cites it with.
    let names = plan.slot_names();
    assert!(
        env.errors.entries().iter().any(|a| a.row == 1
            && matches!(a.kind, ExceptionKind::UnparsableField { .. })
            && a.field.and_then(|f| names.get(&f)).map(String::as_str) == Some("a.b")),
        "{:?}",
        env.errors.entries()
    );
    b.reclaim(&mut env.pool);
}

/// A key goes to the deepest object the tree holds at its name, whichever step made that object.
#[test]
fn a_parse_lays_a_key_into_the_object_an_earlier_step_made() {
    let plan = compile_ok(vec![code(0, "http.x = 1;"), code(1, &json(""))]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_raw(&mut env, &mut b, &[r#"{"http":{"method":"GET"},"k":2}"#]);
    let rows = env.run(&plan, &mut b);
    let row = parsed(&rows[0]);
    assert_eq!(row["http"], serde_json::json!({"x": 1, "method": "GET"}));
    assert_eq!(row["k"], 2);
    b.reclaim(&mut env.pool);
}

/// A declared path keeps the rest of its head: the keys an earlier parse laid under it stay beside the leaf.
#[test]
fn a_declared_path_keeps_what_an_earlier_parse_laid_beside_it() {
    let plan = compile_ok(vec![
        code(0, &json("body: str;")),
        code(1, "Object.assign($, json(body)); a.b: i64 = a.b;"),
    ]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_raw(
        &mut env,
        &mut b,
        &[r#"{"a":{"z":1},"body":"{\"a\":{\"b\":\"2\",\"y\":3}}"}"#],
    );
    let rows = env.run(&plan, &mut b);
    assert_eq!(parsed(&rows[0])["a"], serde_json::json!({"b": 2, "z": 1, "y": 3}));
    b.reclaim(&mut env.pool);
}

/// An open object that holds `null` gives way to a declared path under it, and the keys the record holds beside that
/// path stay with it.
#[test]
fn a_declared_path_under_a_null_keeps_the_keys_beside_it() {
    let plan = compile_ok(vec![
        code(0, "x: {...} = null;"),
        code(1, "Object.assign($, json(_raw)); x.y: i64 = x.y;"),
    ]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_raw(&mut env, &mut b, &[r#"{"x":{"y":1,"z":2}}"#]);
    let rows = env.run(&plan, &mut b);
    assert_eq!(parsed(&rows[0])["x"], serde_json::json!({"y": 1, "z": 2}));
    b.reclaim(&mut env.pool);
}

/// A key is one key, whatever it holds: a key with a dot, a bracket or a quote lands as one unclaimed key and is
/// written back as it came.
#[test]
fn a_key_holding_a_dot_a_bracket_or_a_quote_round_trips() {
    let plan = compile_ok(vec![code(0, &json(""))]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    let doc = r#"{"a.b":1,"a[0]":2,"say \"hi\"":3,"a":{"b":4}}"#;
    seed_raw(&mut env, &mut b, &[doc]);
    let rows = env.run(&plan, &mut b);
    let mut row = parsed(&rows[0]);
    row.as_object_mut().unwrap().remove("_raw");
    assert_eq!(row, serde_json::from_str::<serde_json::Value>(doc).unwrap());
    assert!(env.errors.entries().is_empty(), "{:?}", env.errors.entries());
    b.reclaim(&mut env.pool);
}

/// `splitDots` reads a flat dotted key as its path, so it meets the field declared there.
#[test]
fn split_dots_nests_a_flat_key_into_its_declared_field() {
    for (split_dots, want) in [
        (true, serde_json::json!({"geo": {"city": "Oslo"}})),
        (false, serde_json::json!({"geo.city": "Oslo"})),
    ] {
        let parse = format!("geo: {{city: str}}; Object.assign($, json(_raw, {{splitDots: {split_dots}}}));");
        let plan = compile_ok(vec![code(0, &parse)]);
        let mut env = Env::new();
        let mut b = Batch::new(&plan.slot_layout, 1, 0);
        seed_raw(&mut env, &mut b, &[r#"{"geo.city":"Oslo"}"#]);
        let rows = env.run(&plan, &mut b);
        let mut row = parsed(&rows[0]);
        row.as_object_mut().unwrap().remove("_raw");
        assert_eq!(row, want, "splitDots: {split_dots}");
        b.reclaim(&mut env.pool);
    }
}

/// A declared name may hold a key that is not plain, spelled in brackets, and the key fills it.
#[test]
fn a_declared_bracketed_key_fills_from_the_record() {
    let plan = compile_ok(vec![code(0, &json(r#"labels: {"app.kubernetes.io/name": str, ...};"#))]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_raw(
        &mut env,
        &mut b,
        &[r#"{"labels":{"app":"cart","app.kubernetes.io/name":"cart"}}"#],
    );
    let rows = env.run(&plan, &mut b);
    let row = parsed(&rows[0]);
    assert_eq!(
        row["labels"],
        serde_json::json!({"app": "cart", "app.kubernetes.io/name": "cart"})
    );
    let slot = plan
        .tree
        .leaf(r#"labels["app.kubernetes.io/name"]"#)
        .expect("a declared leaf");
    assert_eq!(plan.slot_layout[slot], DataType::Str);
    b.reclaim(&mut env.pool);
}

/// `step` after a parse of `_raw`, run on a client and a server row whose `addr` no parser declared.
fn run_after_parse(step: Step) -> Vec<serde_json::Value> {
    let plan = compile_ok(vec![code(0, &json("")), step]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 2, 0);
    seed_raw(
        &mut env,
        &mut b,
        &[
            r#"{"kind":"client","addr":"10.0.0.1"}"#,
            r#"{"kind":"server","addr":"10.0.0.2"}"#,
        ],
    );
    let rows = env.run_json(&plan, &mut b);
    b.reclaim(&mut env.pool);
    rows
}

const SERVER: &str = r#"(kind as str) == "server""#;

#[test]
fn a_filtered_rename_keeps_an_undeclared_source_on_the_rows_it_skips() {
    let rename = FunctionKind::Rename {
        pairs: vec![("addr".into(), "server_addr".into())],
    };
    let rows = run_after_parse(Step::Function(FunctionConfig::new(1, rename).with_filter(SERVER)));
    assert_eq!(rows[0]["addr"], "10.0.0.1");
    assert_eq!(rows[0].get("server_addr"), None);
    assert_eq!(rows[1].get("addr"), None);
    assert_eq!(rows[1]["server_addr"], "10.0.0.2");
}

#[test]
fn a_filtered_eval_keeps_an_undeclared_target_on_the_rows_it_skips() {
    let eval = FunctionKind::Eval {
        assignments: vec![("addr".into(), r#""x""#.into())],
    };
    let rows = run_after_parse(Step::Function(FunctionConfig::new(1, eval).with_filter(SERVER)));
    assert_eq!(rows[0]["addr"], "10.0.0.1");
    assert_eq!(rows[1]["addr"], "x");
}

#[test]
fn a_null_write_in_an_if_keeps_an_undeclared_key_on_the_rows_it_skips() {
    let rows = run_after_parse(code(1, &format!("if ({SERVER}) {{ addr = null; }}")));
    assert_eq!(rows[0]["addr"], "10.0.0.1");
    assert_eq!(rows[1].get("addr"), None);
}
