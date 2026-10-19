//! End-to-end tests for array/object/index expressions: a compiled pipeline
//! whose code steps build and read `Array`/`Struct` columns, JSON out.

use smeltr::core::batch::Batch;
use smeltr::plan::topology::SourceMeta;

use smeltr::core::column::{Column, DataType};
use smeltr::core::error::ExceptionKind;

use crate::harness;
use harness::*;

fn seed_offset(env: &mut Env, batch: &mut Batch, vals: &[i64]) {
    let mut c = Column::alloc(&mut env.pool, &DataType::I64, vals.len());
    c.i64_mut().unwrap().values.copy_from_slice(vals);
    batch.put(slot_of(SourceMeta::KAFKA, "__source.offset"), c, &mut env.pool);
}

// ---------------------------------------------------------------------------

#[test]
fn array_literal_serializes_as_a_json_array() {
    let plan = compile_ok(vec![
        code(0, "status = __source.offset;"),
        code(1, "codes = [status, status + 1, 0];"),
    ]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 2, 0);
    seed_offset(&mut env, &mut b, &[200, 404]);

    let rows = env.run_json(&plan, &mut b);
    assert_eq!(rows[0]["codes"], serde_json::json!([200, 201, 0]));
    assert_eq!(rows[1]["codes"], serde_json::json!([404, 405, 0]));
    b.reclaim(&mut env.pool);
}

#[test]
fn object_literal_serializes_as_a_json_object() {
    let plan = compile_ok(vec![
        code(0, "status = __source.offset;"),
        code(1, "info = { code: status, ok: status < 400, label: toString(status) };"),
    ]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 2, 0);
    seed_offset(&mut env, &mut b, &[200, 500]);

    let rows = env.run_json(&plan, &mut b);
    assert_eq!(
        rows[0]["info"],
        serde_json::json!({"code": 200, "ok": true, "label": "200"})
    );
    assert_eq!(
        rows[1]["info"],
        serde_json::json!({"code": 500, "ok": false, "label": "500"})
    );
    b.reclaim(&mut env.pool);
}

#[test]
fn array_index_reads_an_element_by_a_dynamic_row_index() {
    let plan = compile_ok(vec![
        code(0, "i = __source.offset;"),
        code(1, "codes = [10, 20, 30]; picked = codes[i];"),
    ]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 3, 0);
    seed_offset(&mut env, &mut b, &[0, 1, 99]);

    let rows = env.run_json(&plan, &mut b);
    assert_eq!(rows[0]["picked"], 10);
    assert_eq!(rows[1]["picked"], 20);
    assert!(
        rows[2].get("picked").is_none(),
        "out-of-range index reads null, which is omitted from output"
    );
    b.reclaim(&mut env.pool);
}

#[test]
fn struct_index_reads_a_named_field_by_a_literal_key() {
    let plan = compile_ok(vec![
        code(0, "status = __source.offset;"),
        code(1, r#"info = { code: status }; picked = info["code"];"#),
    ]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_offset(&mut env, &mut b, &[418]);

    let rows = env.run_json(&plan, &mut b);
    assert_eq!(rows[0]["picked"], 418);
    b.reclaim(&mut env.pool);
}

#[test]
fn nested_object_in_object_serializes_correctly() {
    let plan = compile_ok(vec![
        code(0, "status = __source.offset;"),
        code(1, "outer = { inner: { code: status } };"),
    ]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_offset(&mut env, &mut b, &[7]);

    let rows = env.run_json(&plan, &mut b);
    assert_eq!(rows[0]["outer"], serde_json::json!({"inner": {"code": 7}}));
    b.reclaim(&mut env.pool);
}

#[test]
fn a_bare_field_reassignment_of_a_struct_column_round_trips() {
    let plan = compile_ok(vec![
        code(0, "status = __source.offset;"),
        code(1, "info = { code: status };"),
        code(2, "copy = info;"),
    ]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_offset(&mut env, &mut b, &[201]);

    let rows = env.run_json(&plan, &mut b);
    assert_eq!(rows[0]["copy"], serde_json::json!({"code": 201}));
    b.reclaim(&mut env.pool);
}

/// `cond ? structA : structB` blends per row like any other ternary once both branches carry the identical
/// `Struct` shape: `blend` recurses into each named field with the same guard, rather than reading the Struct
/// through the wrong accessor (there is no top-level Struct storage to blend at all).
#[test]
fn ternary_over_struct_branches_blends_per_row() {
    let plan = compile_ok(vec![
        code(0, "status = __source.offset;"),
        code(
            1,
            "a = { code: status }; b = { code: status + 1000 }; picked = status < 300 ? a : b;",
        ),
    ]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 2, 0);
    seed_offset(&mut env, &mut b, &[200, 404]);

    let rows = env.run_json(&plan, &mut b);
    assert_eq!(rows[0]["picked"], serde_json::json!({"code": 200}));
    assert_eq!(rows[1]["picked"], serde_json::json!({"code": 1404}));
    b.reclaim(&mut env.pool);
}

/// A `Struct` field that is `i64` on one branch of a ternary and `f64` on the other is widened by the compiler:
/// the picked field is `f64` on every row, the `i64` side widened, the same as a mixed-numeric ternary over scalars.
#[test]
fn ternary_over_struct_branches_widens_a_field_that_is_i64_on_one_side_and_f64_on_the_other() {
    let plan = compile_ok(vec![
        code(0, "status = __source.offset;"),
        code(
            1,
            "a = { n: status / 2 }; b = { n: status }; picked = status > 100 ? a : b;",
        ),
    ]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 2, 0);
    seed_offset(&mut env, &mut b, &[201, 5]);

    let rows = env.run_json(&plan, &mut b);
    assert_eq!(rows[0]["picked"], serde_json::json!({"n": 100.5}));
    assert_eq!(rows[1]["picked"], serde_json::json!({"n": 5.0}));
    assert!(
        rows[1]["picked"]["n"].is_f64(),
        "the i64 side is widened: {}",
        rows[1]["picked"]
    );
    b.reclaim(&mut env.pool);
}

/// An `i64` field that overflows inside a `Struct` branch is null on that row, with the row marked `Overflow`;
/// the field stays `i64`, and the other rows keep their values.
#[test]
fn an_overflowing_struct_field_is_null_on_its_row_and_stays_i64() {
    let plan = compile_ok(vec![
        code(0, "status = __source.offset;"),
        code(
            1,
            "a = { n: status + 1 }; b = { n: status }; picked = status > 1000000 ? a : b;",
        ),
    ]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 2, 0);
    seed_offset(&mut env, &mut b, &[i64::MAX, 5]);

    let rows = env.run_json(&plan, &mut b);
    assert!(
        rows[0]["picked"]["n"].is_null(),
        "the overflowed field is null: {}",
        rows[0]["picked"]
    );
    assert_eq!(rows[1]["picked"], serde_json::json!({"n": 5}));
    assert!(
        rows[1]["picked"]["n"].is_i64(),
        "the field stays i64: {}",
        rows[1]["picked"]
    );
    assert!(
        env.errors
            .entries()
            .iter()
            .any(|e| e.row == 0 && matches!(e.kind, ExceptionKind::Overflow)),
        "{:?}",
        env.errors.entries()
    );
    b.reclaim(&mut env.pool);
}

/// A null condition takes the else array branch, the same as any other blend.
#[test]
fn a_null_condition_takes_the_else_array_branch() {
    let plan = compile_ok(vec![code(
        0,
        "a = [1, 2]; b = [3, 4]; cond = false ? true : null; picked = cond ? a : b;",
    )]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);

    let rows = env.run_json(&plan, &mut b);
    assert!(rows[0].get("cond").is_none(), "the condition is null: {}", rows[0]);
    assert_eq!(rows[0]["picked"], serde_json::json!([3, 4]));
    b.reclaim(&mut env.pool);
}

/// `[]` has no element to take a type from: its name takes the type the writes after it give, and with none it is
/// refused with the typed write to add. Given its type, the array holds what is pushed to it.
#[test]
fn an_empty_array_literal_is_refused_until_it_is_given_a_type() {
    let plan = compile_ok(vec![code(0, "x = []; x = push(x, 7);")]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    assert_eq!(env.run_json(&plan, &mut b)[0]["x"], serde_json::json!([7]));
    b.reclaim(&mut env.pool);
    let errs = compile_err(vec![code(0, "x = []; y = x;")]);
    assert!(
        errs.iter()
            .any(|e| e.to_string().contains("`x` has no type") && e.to_string().contains("`x: type = []`")),
        "{errs:?}"
    );

    for (ty, value, want) in [
        ("i64", "7", serde_json::json!([7])),
        ("f64", "1.5", serde_json::json!([1.5])),
        ("bool", "true", serde_json::json!([true])),
    ] {
        let body = format!("x: array<{ty}> = []; x = push(x, {value});");
        let plan = compile_ok(vec![code(0, &body)]);
        let mut env = Env::new();
        let mut b = Batch::new(&plan.slot_layout, 1, 0);
        assert_eq!(env.run_json(&plan, &mut b)[0]["x"], want, "{body}");
        b.reclaim(&mut env.pool);
    }
}

#[test]
fn mismatched_array_element_types_are_a_compile_error() {
    let errs = compile_err(vec![code(0, r#"x = [1, "a"];"#)]);
    assert!(errs.iter().any(|e| e.to_string().contains("same type")));
}

#[test]
fn indexing_a_struct_with_an_unknown_field_is_a_compile_error() {
    let errs = compile_err(vec![code(0, r#"obj = { a: 1 }; x = obj["nope"];"#)]);
    assert!(errs.iter().any(|e| e.to_string().contains("nope")));
}

/// A `__source` field every record of a batch shares is one value, and a declaration spreads it into each row.
#[test]
fn a_declared_key_of_a_shared_source_variant_is_each_rows() {
    use smeltr::core::column::Scalar;
    let plan = compile_ok_for(
        SourceMeta::OTLP_LOGS,
        vec![code(
            0,
            r#"__source.resource: {"service.name": str, ...} = __source.resource; svc = __source.resource["service.name"];"#,
        )],
    );
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 2, 0);
    seed_raw_empty(&mut env, &mut b, 2);
    let mut resource = Vec::new();
    let open = smeltr::core::variant::begin_object(&mut resource);
    smeltr::core::variant::put_key(&mut resource, "service.name");
    smeltr::core::variant::put_str(&mut resource, "api");
    smeltr::core::variant::end(&mut resource, open, 1);
    let shared = Column::constant(Scalar::Variant(resource), 2);
    b.put(
        slot_of(SourceMeta::OTLP_LOGS, "__source.resource"),
        shared,
        &mut env.pool,
    );

    let rows = env.run_json(&plan, &mut b);
    assert_eq!(rows[0]["svc"], "api", "{}", rows[0]);
    assert_eq!(rows[1]["svc"], "api", "{}", rows[1]);
    b.reclaim(&mut env.pool);
}
