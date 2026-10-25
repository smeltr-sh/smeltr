//! `FunctionKind::Code`: the JS-like statement block of the design doc's Language,
//! compiled to the same `Op::Eval` instructions `Eval` emits. Each test
//! pins one language property; compile errors are checked by kind, not
//! wording.

use serde_json::{Value, json};
use smeltr::core::batch::Batch;
use smeltr::core::bits;
use smeltr::core::buffer::BufferPool;
use smeltr::core::column::{Column, DataType};
use smeltr::core::ctx::Ctx;
use smeltr::core::error::{ErrorSink, ExceptionKind};
use smeltr::core::id::FunctionId;
use smeltr::metrics::{MetricValue, Metrics, MetricsHandle};
use smeltr::plan::config::{DeclaredField, FunctionKind, ParserKind, PipelineConfig, Step, TimeSource};
use smeltr::plan::exec::{DEFAULT_LOOP_BUDGET, run_scoped};
use smeltr::plan::output::run_to_json_marked;
use smeltr::plan::plan::{Op, Plan, compile};
use std::time::Instant;

use crate::harness;
use harness::*;

// -- plan construction ------------------------------------------------------

/// Parses `_raw` as JSON into the record, each key it reads one key.
const JSON: &str = "Object.assign($, json(_raw));";

/// Parses `_raw` as JSON into the record, each key it reads split at its dots into a path.
const JSON_SPLIT: &str = "Object.assign($, json(_raw, {splitDots: true}));";

/// `fields`' declarations, then `JSON`.
fn json(fields: &str) -> String {
    format!("{fields} {JSON}")
}

/// `fields`' declarations, then `JSON_SPLIT`.
fn json_split(fields: &str) -> String {
    format!("{fields} {JSON_SPLIT}")
}

/// A JSON parser step over `_raw` declaring `declared`, for a name only a step's field spells.
fn parse(id: FunctionId, declared: &[(&str, DataType)]) -> Step {
    let declared = declared
        .iter()
        .map(|(n, t)| DeclaredField {
            name: n.to_string(),
            ty: t.clone(),
            time_source: None,
        })
        .collect();
    let kind = ParserKind::Json { split_dots: false };
    func(
        id,
        FunctionKind::Parser {
            input: "_raw".to_string(),
            into: "$".into(),
            kind,
            declared,
        },
    )
}

fn guarded(id: FunctionId, cond: &str, then: Step) -> Step {
    Step::If {
        id,
        cond: cond.into(),
        then: vec![then],
        else_: vec![],
    }
}

/// Step 0, parsing `nums: array<i64>`.
fn nums() -> Step {
    code(0, &json("nums: array<i64>;"))
}

/// Step 0, parsing `pairs: array<{key: str, value: str}>`.
fn pairs() -> Step {
    code(0, &json("pairs: array<{key: str, value: str}>;"))
}

/// Step `id`, binding `name` to the row's `pairs` as one object, one key per pair, a later key winning and a key
/// holding a dot one key: a variant, a value whose shape only the row knows. It is null where `pairs` holds none. The
/// object is written into an open object of variants by a key only the row knows, since a literal key would name a
/// field of its own type, and read back out.
fn build(id: FunctionId, name: &str) -> Step {
    code(
        id,
        &format!("let held: {{...}} = {{}}; let key = \"v\"; held[key] = fromEntries(pairs); {name} = held.v;"),
    )
}

/// A record holding `pairs`, one `{key, value}` per entry, beside `rest`'s fields.
fn pairs_doc(entries: &[(&str, Value)], rest: Value) -> String {
    let mut doc = rest;
    doc["pairs"] = entries.iter().map(|(k, v)| json!({"key": k, "value": v})).collect();
    doc.to_string()
}

/// A 1,000-element `nums` document, every element above 10, with `extra` spliced in after it.
fn long_nums_doc(extra: &str) -> String {
    let elems: Vec<String> = (11..=1010).map(|i| i.to_string()).collect();
    format!("{{\"nums\":[{}]{extra}}}", elems.join(","))
}

/// The argmax body `recognize_fold` rejects, so it compiles to `Op::Loop`.
const ARGMAX: &str = "let best = -1; let bestX = 0;
    for (let x of xs) { if (x > best) { best = x; bestX = x; } }
    top = bestX;";

fn pipeline(body: Vec<Step>) -> PipelineConfig {
    PipelineConfig {
        id: 0,
        name: "test".into(),
        body,
        step_ids: Vec::new(),
        lookup_tables: Default::default(),
        grok_patterns: Default::default(),
    }
}

fn ok(body: Vec<Step>) -> Plan {
    compile(&pipeline(body), &smeltr::plan::topology::SourceMeta::KAFKA).unwrap_or_else(|e| {
        panic!(
            "compile failed: {}",
            e.iter().map(|x| x.to_string()).collect::<Vec<_>>().join("; ")
        )
    })
}

fn err(body: Vec<Step>) -> String {
    match compile(&pipeline(body), &smeltr::plan::topology::SourceMeta::KAFKA) {
        Ok(_) => panic!("expected a compile error"),
        Err(e) => e.iter().map(|x| x.to_string()).collect::<Vec<_>>().join("; "),
    }
}

// -- execution --------------------------------------------------------------

/// Runs `plan` against `docs` (each `_raw` bytes) and returns each row's JSON.
fn run_json(plan: &Plan, docs: &[&str], show_internal: bool) -> Vec<Value> {
    let mut pool = BufferPool::new();
    let mut errors = ErrorSink::new();
    let mut batch = raw_batch(plan, docs, &mut pool);
    let mut ctx = Ctx::new(&mut pool, &mut errors);
    let rows = run_to_json_marked(plan, &Default::default(), &mut batch, &mut ctx, show_internal, None)
        .iter()
        .filter_map(|r| r.json.as_ref())
        .map(|row| serde_json::from_slice(row).unwrap())
        .collect();
    batch.reclaim(&mut pool);
    rows
}

fn run(plan: &Plan, docs: &[&str]) -> Vec<Value> {
    run_json(plan, docs, false)
}

/// The field names row 0 shows with `__`-prefixed internal fields serialized.
fn internal_keys(plan: &Plan, docs: &[&str]) -> (Value, Vec<String>) {
    let row = run_json(plan, docs, true).remove(0);
    let keys = row.as_object().unwrap().keys().cloned().collect();
    (row, keys)
}

/// Runs `plan` through `run_scoped` with every row live; returns its exceptions.
fn run_scoped_on(plan: &Plan, docs: &[&str], metrics: Option<&MetricsHandle>) -> ErrorSink {
    let mut pool = BufferPool::new();
    let mut errors = ErrorSink::new();
    let mut batch = raw_batch(plan, docs, &mut pool);
    let mut live = Column::alloc(&mut pool, &DataType::Bool, docs.len());
    bits::fill_valid(live.bool_mut().unwrap().values, docs.len());
    {
        let mut ctx = Ctx::new(&mut pool, &mut errors);
        run_scoped(plan, &mut batch, live, &mut ctx, metrics);
    }
    batch.reclaim(&mut pool);
    errors
}

fn budget_exceeded(errors: &ErrorSink) -> usize {
    errors
        .entries()
        .iter()
        .filter(|a| matches!(a.kind, ExceptionKind::LoopBudgetExceeded { .. }))
        .count()
}

fn run_with_metrics(plan: &Plan, docs: &[&str]) -> MetricsHandle {
    let metrics = MetricsHandle::new(std::sync::Arc::new(Metrics::new()), "t0");
    run_scoped_on(plan, docs, Some(&metrics));
    metrics
}

/// The sample for `name{labels}` (labels order-insensitive), or `None`.
fn counter(m: &Metrics, name: &str, labels: &[(&str, &str)]) -> Option<u64> {
    let want: std::collections::HashSet<(&str, &str)> = labels.iter().map(|(k, v)| (*k, *v)).collect();
    m.snapshot().into_iter().find_map(|s| {
        if s.key.name != name {
            return None;
        }
        let got: std::collections::HashSet<(&str, &str)> = s.key.labels.iter().map(|(k, v)| (*k, v.as_str())).collect();
        // Subset: every sample also carries the writing thread's label.
        match want.is_subset(&got).then_some(s.value) {
            Some(MetricValue::Counter(v)) => Some(v),
            _ => None,
        }
    })
}

fn function_metric(m: &MetricsHandle, name: &str, function: FunctionId) -> u64 {
    let f = function.to_string();
    // `PipelineConfig::id`, which is what the label carries -- the worker resolves it to the author's own id on its
    // way to the leader, and nothing here is a worker.
    counter(m.registry(), name, &[("pipeline", "0"), ("function", &f)])
        .unwrap_or_else(|| panic!("function {f} recorded no {name}"))
}

fn function_calls(plan: &Plan, docs: &[&str], function: FunctionId) -> u64 {
    function_metric(&run_with_metrics(plan, docs), "smeltr_function_calls_total", function)
}

/// `(body instructions, other top-level instructions)` of `function`'s one `Op::Loop`.
fn foreach_shape(plan: &Plan, function: FunctionId) -> (usize, usize) {
    let body = plan
        .instrs
        .iter()
        .find_map(|i| match &i.op {
            Op::Loop(l) if i.function == function => Some(l.body.len()),
            _ => None,
        })
        .expect("one Op::Loop instruction");
    let other = plan
        .instrs
        .iter()
        .filter(|i| i.function == function && !matches!(i.op, Op::Loop(_)))
        .count();
    (body, other)
}

/// Function 1's loop over the 1,000-element `doc` cost fewer than ten steps' worth of body dispatches.
fn assert_few_steps(plan: &Plan, doc: &str, why: &str) {
    let (body_len, other) = foreach_shape(plan, 1);
    let calls = function_calls(plan, &[doc], 1);
    assert!(
        calls < (other + 10 * body_len) as u64,
        "{calls} dispatches ({other} + steps * {body_len}) for {why} of 1000: \
         the row should leave the wavefront"
    );
}

/// Each row's values at `keys`, `Null` where absent.
fn cols(rows: &[Value], keys: &[&str]) -> Vec<Vec<Value>> {
    rows.iter()
        .map(|r| keys.iter().map(|k| r.get(k).cloned().unwrap_or(Value::Null)).collect())
        .collect()
}

/// Expected `cols` output: `rows![[1, "a"], [null, 2]]`.
macro_rules! rows {
    ($([$($v:tt),* $(,)?]),* $(,)?) => { vec![$(vec![$(json!($v)),*]),*] };
}

// -- statements and scoping -------------------------------------------------

/// `let` locals compute, bare expressions run and are discarded, and no local is serialized.
#[test]
fn statements_compute_and_locals_stay_out_of_the_output() {
    let plan = ok(vec![
        code(0, &json("status: i64;")),
        code(
            1,
            r#"status + 1; let doubled = status * 2; severity = doubled > 800 ? "high" : "low";"#,
        ),
    ]);
    let rows = run(&plan, &[r#"{"status":200}"#, r#"{"status":500}"#]);
    assert_eq!(
        cols(&rows, &["status", "severity", "doubled"]),
        rows![[200, "low", null], [500, "high", null],]
    );
}

/// A `let` shadows a same-named field for its block, and the field is restored after it.
#[test]
fn a_local_shadows_and_then_restores_a_same_named_field() {
    let plan = ok(vec![
        code(0, "x = 1000;"),
        code(1, "let x = 1; y = x;"),
        code(2, "z = x;"),
    ]);
    assert_eq!(cols(&run(&plan, &["{}"]), &["x", "y", "z"]), rows![[1000, 1, 1000]]);
}

/// A `let` shadows a field from the `let` on: a write to the name before it, in the same block, writes the field.
#[test]
fn a_write_before_a_shadowing_let_writes_the_field() {
    for body in ["b = a; let b = \"x\";", "if (a > 0) { b = a; let b = \"x\"; }"] {
        let plan = ok(vec![code(0, &json("a: i64; b: i64;")), code(1, body)]);
        assert_eq!(run(&plan, &[r#"{"a": 1, "b": 2}"#])[0]["b"], 1, "{body}");
    }
}

/// A `let` types its local by its own value, whatever field it shadows: `[]` has no type there as anywhere.
#[test]
fn a_shadowing_let_takes_no_type_from_the_field() {
    let e = err(vec![code(0, &json("u: str;")), code(1, "let u = [];")]);
    assert!(e.contains("`u` has no type"), "{e}");
}

/// A `[]` takes its type from the writes to its own binding: a local of the same name, inside its block or after it,
/// is another binding.
#[test]
fn an_untyped_local_takes_its_type_from_its_own_binding() {
    for body in [
        "let u = []; if (true) { let u = 5; n = u; } u = push(u, 1); m = u;",
        "let u = []; for (let x of [1, 2]) { let u = \"s\"; t = u; } u = push(u, 1); m = u;",
    ] {
        let plan = ok(vec![code(0, body)]);
        assert_eq!(run(&plan, &["{}"])[0]["m"], json!([1]), "{body}");
    }
    for body in [
        "if (true) { let u = []; } let u = [1]; n = length(u);",
        "if (true) { let u = []; } u = \"x\";",
        "for (let x of [1]) { let u = []; } for (let y of [2]) { let u = 1; u = 2; }",
    ] {
        let e = err(vec![code(0, body)]);
        assert!(e.contains("`u` has no type"), "{body}: {e}");
    }
}

/// Sibling `if`/`else` blocks may each declare the same local name.
#[test]
fn sibling_branches_may_each_declare_the_same_local() {
    let plan = ok(vec![code(
        0,
        "y: i64 = null; if (true) { let x = 1; y = x; } else { let x = 2; y = x; }",
    )]);
    assert_eq!(run(&plan, &["{}"])[0]["y"], 1);
}

/// A guarded `Code` block blends per row: masked rows keep their prior value.
#[test]
fn a_guarded_code_block_only_touches_matching_rows() {
    let plan = ok(vec![
        code(0, &json("status: i64;")),
        code(1, "severity = \"unknown\";"),
        guarded(3, "status >= 400", code(2, r#"severity = "high";"#)),
    ]);
    let rows = run(&plan, &[r#"{"status":200}"#, r#"{"status":500}"#]);
    assert_eq!(cols(&rows, &["severity"]), rows![["unknown"], ["high"]]);
}

/// Array literals, object literals and stdlib calls work inside a block.
#[test]
fn a_code_block_can_use_object_and_array_literals_and_stdlib_functions() {
    let plan = ok(vec![code(
        0,
        r#"let parts = ["a", "b", "c"]; count = length(parts); info = { n: count, up: toUpperCase("hi") };"#,
    )]);
    let rows = run(&plan, &["{}"]);
    assert_eq!(rows[0]["count"], 3);
    assert_eq!(rows[0]["info"], json!({"n": 3, "up": "HI"}));
}

/// A null reads as false in a condition, and a comparison or `!`, `&&` and `||` is true or false on every row.
#[test]
fn null_is_false() {
    let plan = ok(vec![
        code(0, &json("m: i64; k: i64; b: bool;")),
        code(
            1,
            r#"eq = m == 1; ne = m != 1; lt = m < 1; ge = m >= 1; same = m == k;
               not = !(m == 1); nb = !b; and = b && true; or = b || false; pick = m == 1 ? 1 : 2;
               q = 5; if (m == 1) { q = 1; } else { q = 2; }
               r = 5; if (b) { r = 1; } else { r = 2; }
               w = 5; sum = m + 1;"#,
        ),
        guarded(3, "b", code(2, "w = 1;")),
    ]);
    let keys = [
        "eq", "ne", "lt", "ge", "same", "not", "nb", "and", "or", "pick", "q", "r", "w", "sum",
    ];
    let got = cols(&run(&plan, &["{}", r#"{"m":1,"k":2,"b":true}"#]), &keys);
    assert_eq!(
        got,
        vec![
            vec![
                json!(false),
                json!(true),
                json!(false),
                json!(false),
                json!(true),
                json!(true),
                json!(true),
                json!(false),
                json!(false),
                json!(2),
                json!(2),
                json!(2),
                json!(5),
                Value::Null,
            ],
            vec![
                json!(true),
                json!(false),
                json!(false),
                json!(true),
                json!(false),
                json!(false),
                json!(false),
                json!(true),
                json!(true),
                json!(1),
                json!(1),
                json!(1),
                json!(1),
                json!(2),
            ],
        ]
    );
}

/// `!`, a comparison, `&&` and `||` type as Bool when every operand is null, a declared field the record lacks
/// included, so what holds one takes Bool.
#[test]
fn logic_over_nulls_types_as_bool() {
    let plan = ok(vec![
        code(0, &json("m: bool; k: bool;")),
        code(
            1,
            "kept = [!null].filter(x => x); same = [m == k].filter(x => x); either = [m || k].filter(x => x);",
        ),
    ]);
    assert_eq!(
        cols(&run(&plan, &["{}"]), &["kept", "same", "either"]),
        rows![[[true], [true], []]]
    );
}

/// `if`/`else if`/`else` branches per row; an `if` with no `else` leaves unmatched rows null.
#[test]
fn if_else_chains_branch_per_row() {
    let status = || code(0, &json("status: i64;"));
    let chain = ok(vec![
        status(),
        code(
            1,
            r#"severity: str = null;
               if (status >= 500) { severity = "critical"; }
               else if (status >= 400) { severity = "high"; }
               else { severity = "low"; }"#,
        ),
    ]);
    let no_else = ok(vec![
        status(),
        code(1, r#"severity: str = null; if (status >= 400) { severity = "high"; }"#),
    ]);
    let docs = [r#"{"status":200}"#, r#"{"status":450}"#, r#"{"status":500}"#];
    assert_eq!(
        cols(&run(&chain, &docs), &["severity"]),
        rows![["low"], ["high"], ["critical"]]
    );
    assert_eq!(
        cols(&run(&no_else, &docs), &["severity"]),
        rows![[null], ["high"], ["high"]]
    );
}

/// An `if` with an `else` runs its condition once a row, so a row the condition fails on counts once.
#[test]
fn an_if_else_counts_a_failing_row_once() {
    let plan = ok(vec![
        code(0, &json("n: i64;")),
        // Divides by zero where `n` is 0.
        code(
            1,
            "one: bool = null; if (1 / n == 1) { one = true; } else { one = false; }",
        ),
    ]);
    let m = run_with_metrics(&plan, &[r#"{"n":1}"#, r#"{"n":0}"#]);
    assert_eq!(function_metric(&m, "smeltr_rows_errored_total", 1), 1);
}

/// A `catch` that rethrows leaves its exception counted, once; one that does not leaves it uncounted.
#[test]
fn a_rethrown_exception_counts_once_and_a_caught_one_not_at_all() {
    let count = |catch: &str| {
        let body = format!("try {{ n: i64 = parseInt(s); }} catch (e) {{ {catch} }}");
        let plan = ok(vec![code(0, &json("s: str;")), code(1, &body)]);
        let m = run_with_metrics(&plan, &[r#"{"s":"x"}"#, r#"{"s":"7"}"#]);
        counter(
            m.registry(),
            "smeltr_rows_errored_total",
            &[("pipeline", "0"), ("function", "1")],
        )
    };
    assert_eq!(count("throw e; throw e;"), Some(1));
    assert_eq!(count(""), None);
}

/// `if` and `for` nest in either order, with block scoping at every level.
#[test]
fn if_and_for_nest_in_either_order() {
    let plan = ok(vec![
        code(0, &json("status: i64;")),
        code(
            1,
            r#"let evens = 0; penalty: i64 = null;
               for (let x of [1, 2, 3, 4, 5]) { if (x % 2 == 0) { evens = evens + 1; } }
               count = evens;
               if (status >= 400) {
                 let sum = 0;
                 for (let x of [10, 20]) { sum = sum + x; }
                 penalty = sum;
               }"#,
        ),
    ]);
    let rows = run(&plan, &[r#"{"status":200}"#, r#"{"status":500}"#]);
    assert_eq!(cols(&rows, &["count", "penalty"]), rows![[2, null], [2, 30]]);
}

// -- names across a dynamic parser ------------------------------------------

/// A name bound as paths is those paths to every reader after it, a dynamic parser between them included, and a copy
/// of the name is its bound leaves. A write under a name the parser left as a record key keeps the rest of that key:
/// `acc.total` makes `acc` an object, and the record's other keys under `acc` are its unclaimed fields.
#[test]
fn a_name_bound_as_paths_is_read_as_paths_across_a_parser() {
    let plan = ok(vec![
        code(0, "o = {a: 1};"),
        code(1, JSON),
        code(
            2,
            "q = o; acc.total = 0; for (let v of [1, 2]) { acc.total = acc.total + v; }
             empty = isNull(acc); whole = acc;",
        ),
    ]);
    let rows = run(&plan, &[r#"{"o":5,"acc":{"total":9,"more":1}}"#]);
    assert_eq!(
        cols(&rows, &["q", "empty", "whole"]),
        rows![[{"a": 1}, false, {"total": 3, "more": 1}]]
    );
}

/// A method's receiver is a read of its name, so a receiver nothing bound is a compile error of the step that calls
/// the method: on a closed record, not a field; after a JSON parse, a variant, whose type the method needs.
#[test]
fn a_methods_receiver_nothing_bound_is_a_compile_error() {
    let e = err(vec![code(1, "low = msg.toLowerCase();")]);
    assert!(e.contains("function 1: `msg` is not a field"), "{e}");
    let e = err(vec![code(0, JSON), code(1, "low = msg.toLowerCase();")]);
    assert!(e.contains("function 1: `msg` is a variant"), "{e}");
}

/// A write under an `if`, or in a loop body that runs no trips, leaves the value from before it where it does
/// not run: here, the value the parser declares.
#[test]
fn a_write_that_may_not_run_keeps_the_value_before_it() {
    let plan = ok(vec![
        code(0, &json("nums: array<i64>; host: str; last: i64; x: i64;")),
        code(1, r#"if (host == "a") { x = 1; }"#),
        code(2, "for (let v of nums) { last = v; } seen = last; kept = x;"),
    ]);
    let rows = run(
        &plan,
        &[
            r#"{"nums":[],"last":7,"x":5,"host":"b"}"#,
            r#"{"nums":[1,2],"last":7,"x":5,"host":"a"}"#,
        ],
    );
    assert_eq!(cols(&rows, &["seen", "kept"]), rows![[7, 5], [2, 1]]);
}

// -- `for` over a literal array ---------------------------------------------

/// A literal array unrolls in order, an empty one runs zero times, and the loop variable shadows then restores an outer field.
#[test]
fn a_for_over_a_literal_array_unrolls_at_compile_time() {
    let plan = ok(vec![
        code(0, "x = 1000;"),
        code(
            1,
            "let sum = 0; for (let x of [1, 2, 3]) { sum = sum + x; last = x; }
             for (let e of []) { sum = 100; }
             total = sum; z = x;",
        ),
    ]);
    assert_eq!(cols(&run(&plan, &["{}"]), &["total", "last", "z"]), rows![[6, 3, 1000]]);
}

// -- `for` over a real array: `Op::ArrayFold` and `Op::Loop` ----------------

/// A single associative accumulator is `Op::ArrayFold`: vectorized, not a row barrier, with null read as false in `&&` and `||` and null-propagating concat.
#[test]
fn a_single_accumulator_loop_folds_without_a_row_barrier() {
    let sum = "let t = 0; for (let x of xs) { t = t + x; } out = t;";
    let all = "let a = true; for (let x of xs) { a = x && a; } out = a;";
    let cases: &[(&str, &str, Value)] = &[
        ("[1, 2, 3]", sum, json!(6)),
        ("[7]", sum, json!(7)),
        (
            "[2, 3, 4]",
            "let t = 1; for (let x of xs) { t = t * x; } out = t;",
            json!(24),
        ),
        (
            r#"["a", "b", "c"]"#,
            r#"let s = ""; for (let x of xs) { s = s + x; } out = s;"#,
            json!("abc"),
        ),
        ("[true, null, false]", all, json!(false)),
        ("[true, null, true]", all, json!(false)),
        (
            "[false, null]",
            "let a: bool = null; for (let x of xs) { a = x || a; } out = a;",
            json!(false),
        ),
        (
            "[false, null, true]",
            "let a = false; for (let x of xs) { a = x || a; } out = a;",
            json!(true),
        ),
    ];
    for (xs, body, want) in cases {
        let plan = ok(vec![code(0, &format!("xs = {xs};")), code(1, body)]);
        assert_eq!(plan.barriers.len(), 0, "{xs} {body}: {:?}", plan.barriers);
        for got in cols(&run(&plan, &["{}", "{}"]), &["out"]) {
            if want.is_f64() {
                assert_eq!(got[0].as_f64(), want.as_f64(), "{xs} {body}");
            } else {
                assert_eq!(got[0], *want, "{xs} {body}");
            }
        }
    }
}

/// A body `recognize_fold` rejects is `Op::Loop`: a declared row barrier that still computes across elements and rows.
#[test]
fn an_unrecognized_loop_body_is_a_row_barrier_and_still_computes() {
    let plan = ok(vec![code(0, "xs = [3, 1, 5, 2];"), code(1, ARGMAX)]);
    assert_eq!(plan.barriers.len(), 1, "{:?}", plan.barriers);
    for row in run(&plan, &["{}", "{}", "{}"]) {
        assert_eq!(row["top"], 5);
    }
}

/// A literal read only inside a loop body is rows, not one value. The body reads it from the wave, which gathers
/// it from the outer batch, so liveness marks it as read there and the writer is not told to keep a constant.
#[test]
fn a_literal_a_loop_body_reads_is_rows() {
    let plan = ok(vec![
        code(0, "xs = [3, 1, 5];"),
        code(
            1,
            r#"tag = "panw"; let best = -1; let seen = "";
               for (let x of xs) { if (x > best) { best = x; seen = tag; } }
               top = best; out = seen;"#,
        ),
    ]);
    assert_eq!(plan.barriers.len(), 1, "{:?}", plan.barriers);
    let tag = plan.provenance.iter().rfind(|o| o.name == "tag").unwrap();
    let writer = plan
        .instrs
        .iter()
        .find(|i| i.out == tag.slot && matches!(i.op, Op::Eval { .. }))
        .expect("the literal's own instruction");
    assert!(!writer.constant_ok, "the loop body reads `tag`");
    for row in run(&plan, &["{}", "{}"]) {
        assert_eq!(row["out"], "panw");
        assert_eq!(row["top"], 5);
    }
}

/// The same for a `while` body. Its reads reach the outer batch through the same wave, so liveness marks them the
/// same way, and the two loop shapes cannot drift apart on this.
#[test]
fn a_literal_a_while_body_reads_is_rows() {
    let plan = ok(vec![code(
        0,
        r#"tag = "panw"; let i = 0; let seen = "";
           while (i < 3) { seen = tag; i = i + 1; }
           out = seen;"#,
    )]);
    let tag = plan.provenance.iter().rfind(|o| o.name == "tag").unwrap();
    let writer = plan
        .instrs
        .iter()
        .find(|i| i.out == tag.slot && matches!(i.op, Op::Eval { .. }))
        .expect("the literal's own instruction");
    assert!(!writer.constant_ok, "the while body reads `tag`");
    for row in run(&plan, &["{}", "{}"]) {
        assert_eq!(row["out"], "panw");
    }
}

/// A row barrier's body instructions are counted once per wavefront step, not per element, and its own dispatch is not counted twice.
#[test]
fn a_row_barrier_bodys_metrics_count_steps_not_elements() {
    let plan = ok(vec![code(0, "xs = [3, 1, 5];"), code(1, ARGMAX)]);
    let (body_len, other) = foreach_shape(&plan, 1);
    let steps = 3;
    let recorded = function_calls(&plan, &["{}", "{}", "{}"], 1);
    assert_eq!(
        recorded,
        // `other` counts this step's instructions bar the loop; the loop dispatches once itself, which is also
        // where it is timed now.
        (other + 1 + steps * body_len) as u64,
        "3 rows x 3 elements is 9 element-executions but {steps} steps"
    );
}

/// A row barrier records `rows_in` as the batch width and `rows_out` as the rows its guard let through.
#[test]
fn a_row_barrier_records_rows_in_and_rows_out_for_guard_masked_rows() {
    let plan = ok(vec![
        code(0, &json("flag: bool;")),
        code(1, "xs = [1, 2, 3];"),
        code(4, "top: i64 = null;"),
        guarded(3, "flag", code(2, ARGMAX)),
    ]);
    let m = run_with_metrics(&plan, &[r#"{"flag":true}"#, r#"{"flag":false}"#, r#"{"flag":true}"#]);
    assert_eq!(function_metric(&m, "smeltr_rows_in_total", 2), 3);
    assert_eq!(function_metric(&m, "smeltr_rows_out_total", 2), 2);
}

/// Ragged rows each stop at their own last element, and `Str` results are reassembled in row order.
#[test]
fn ragged_rows_each_stop_at_their_own_last_element_in_row_order() {
    let plan = ok(vec![
        code(0, &json("parts: array<str>;")),
        code(
            1,
            r#"let acc = ""; let count = 0;
               for (let p of parts) { acc = acc + p; count = count + 1; }
               joined = acc; n = count;"#,
        ),
    ]);
    // Element counts 1/5/3/4: the finishing order is a 4-cycle, so a
    // permutation confused with its inverse moves rows.
    let rows = run(
        &plan,
        &[
            r#"{"parts":["a"]}"#,
            r#"{"parts":["b","c","d","e","f"]}"#,
            r#"{"parts":["g","h","i"]}"#,
            r#"{"parts":["j","k","l","m"]}"#,
        ],
    );
    assert_eq!(
        cols(&rows, &["joined", "n"]),
        rows![["a", 1], ["bcdef", 5], ["ghi", 3], ["jklm", 4],]
    );
}

/// An empty array runs no elements: accumulators bound to null stay null and the others keep their prior value.
#[test]
fn an_empty_array_runs_no_elements_and_retains() {
    let plan = ok(vec![
        nums(),
        code(1, "total = 100;"),
        code(
            2,
            "a: i64 = null; b: i64 = null; for (let x of nums) { a = x; b = a + 1; total = total + x; }",
        ),
    ]);
    let rows = run(&plan, &[r#"{"nums":[]}"#, r#"{"nums":[5]}"#]);
    assert_eq!(
        cols(&rows, &["a", "b", "total"]),
        rows![[null, null, 100], [5, 6, 105],]
    );
}

/// A guarded assignment, a ternary and `??` blend an array of arrays per row, as they blend a flat one.
#[test]
fn a_blend_over_nested_arrays_picks_per_row() {
    let plan = ok(vec![
        code(
            0,
            &json("rowsOf: array<array<i64>>; other: array<array<i64>>; maybe: array<array<i64>>; c: bool;"),
        ),
        code(
            1,
            "picked = c ? other : [[0]]; either = maybe ?? other; if (c) { rowsOf = other; }",
        ),
    ]);
    let rows = run(
        &plan,
        &[
            r#"{"rowsOf":[[1,2],[3]],"other":[[9]],"maybe":null,"c":true}"#,
            r#"{"rowsOf":[[4]],"other":[[5],[6,7]],"maybe":[[8],[]],"c":false}"#,
        ],
    );
    assert_eq!(rows[0]["rowsOf"], json!([[9]]));
    assert_eq!(rows[1]["rowsOf"], json!([[4]]));
    assert_eq!(rows[0]["picked"], json!([[9]]));
    assert_eq!(rows[1]["picked"], json!([[0]]));
    assert_eq!(rows[0]["either"], json!([[9]]));
    assert_eq!(rows[1]["either"], json!([[8], []]));
}

/// An array literal nests: an array of arrays, of objects, and of objects holding arrays. A null item is a null
/// element, and an `i64` beside an `f64` meets as a number at any depth.
#[test]
fn an_array_literal_nests() {
    let plan = ok(vec![
        code(0, &json("x: i64; f: f64;")),
        code(
            1,
            "grid = [[x, 2], [3]]; objs = [{k: x, xs: [x, 5]}, {k: 6, xs: [7]}]; holes = [[x], null]; \
             mixed = [[x], [f]]; second = grid[1]; past = grid[5]; \
             lens = grid.map(r => length(r)); long = grid.filter(r => length(r) > 1);",
        ),
    ]);
    let rows = run(&plan, &[r#"{"x":1,"f":2.5}"#, r#"{"f":0.5}"#]);
    assert_eq!(rows[0]["grid"], json!([[1, 2], [3]]));
    assert_eq!(rows[0]["second"], json!([3]));
    assert!(rows[0].get("past").is_none_or(Value::is_null), "{}", rows[0]);
    assert_eq!(rows[0]["lens"], json!([2, 1]));
    assert_eq!(rows[0]["long"], json!([[1, 2]]));
    assert_eq!(rows[1]["grid"], json!([[null, 2], [3]]));
    assert_eq!(rows[0]["objs"], json!([{"k": 1, "xs": [1, 5]}, {"k": 6, "xs": [7]}]));
    assert_eq!(rows[0]["holes"], json!([[1], null]));
    let mixed: Vec<Vec<f64>> = serde_json::from_value(rows[0]["mixed"].clone()).unwrap();
    assert_eq!(mixed, vec![vec![1.0], vec![2.5]]);
}

/// A declared array of objects whose objects hold arrays: an element read and a guarded write both carry the whole
/// element, its arrays included.
#[test]
fn an_array_of_objects_holding_arrays_reads_and_blends_whole() {
    let plan = ok(vec![
        code(0, &json("objs: array<{k: str, xs: array<i64>}>; c: bool;")),
        code(
            1,
            r#"first = objs[0]; last = objs[-1]; if (c) { objs = [{k: "z", xs: [0, 0]}]; }"#,
        ),
    ]);
    let rows = run(
        &plan,
        &[
            r#"{"objs":[{"k":"a","xs":[1,2]},{"k":"b","xs":[]}],"c":true}"#,
            r#"{"objs":[{"k":"c","xs":[3]}],"c":false}"#,
        ],
    );
    assert_eq!(rows[0]["first"], json!({"k": "a", "xs": [1, 2]}));
    assert_eq!(rows[0]["last"], json!({"k": "b", "xs": []}));
    assert_eq!(rows[1]["first"], json!({"k": "c", "xs": [3]}));
    assert_eq!(rows[0]["objs"], json!([{"k": "z", "xs": [0, 0]}]));
    assert_eq!(rows[1]["objs"], json!([{"k": "c", "xs": [3]}]));
}

/// Nested row-barrier loops are correct at both levels, and a body reads outer fields (two scopes up included) from its own row.
#[test]
fn nested_row_barrier_loops_read_outer_fields_from_their_own_row() {
    let plan = ok(vec![
        code(0, &json("rowsOf: array<array<i64>>; mult: i64; label: str;")),
        code(
            1,
            r#"let total = 0; let cells = 0; let tag = ""; let tag2 = "";
               for (let r of rowsOf) {
                 tag = label;
                 for (let c of r) { total = total + c * mult; cells = cells + 1; tag2 = label; }
               }
               sum = total; n = cells; out = tag; out2 = tag2;"#,
        ),
    ]);
    let rows = run(
        &plan,
        &[
            r#"{"rowsOf":[[1,2],[3]],"mult":10,"label":"a"}"#,
            r#"{"rowsOf":[[10,20,30]],"mult":100,"label":"b"}"#,
            r#"{"rowsOf":[[1],[1],[1],[1]],"mult":2,"label":"c"}"#,
        ],
    );
    assert_eq!(
        cols(&rows, &["sum", "n", "out", "out2"]),
        rows![[60, 3, "a", "a"], [6000, 3, "b", "b"], [8, 4, "c", "c"],]
    );
}

/// A row a loop's guard masks out retains its prior value while its neighbors run, for every loop shape.
#[test]
fn a_guarded_out_row_retains_while_its_neighbors_still_run() {
    let bodies = [
        "let t = 0; for (let x of nums) { t = t + x; } total = t;",
        "total = 0; count = 0; for (let x of nums) { total = total + x; count = count + 1; }",
        "total = 0; count = 0;
         while (count < length(nums)) { total = total + nums[count]; count = count + 1; }",
    ];
    for body in bodies {
        let plan = ok(vec![
            code(0, &json("nums: array<i64>; on: bool;")),
            code(1, "total = -1; count = -1;"),
            guarded(3, "on", code(2, body)),
        ]);
        let rows = run(
            &plan,
            &[
                r#"{"nums":[1,2,3],"on":true}"#,
                r#"{"nums":[9,9,9,9],"on":false}"#,
                r#"{"nums":[4,5],"on":true}"#,
            ],
        );
        assert_eq!(cols(&rows, &["total"]), rows![[6], [(-1)], [9]], "{body}");
        assert_eq!(rows[1]["count"], -1, "{body}");
    }
}

/// Accumulating a scalar field of a struct element is the supported shape.
#[test]
fn accumulating_a_field_of_a_struct_element_works() {
    let plan = ok(vec![
        code(0, &json("items: array<{n: i64}>;")),
        code(1, "let acc = 0; for (let t of items) { acc = acc + t.n; } out = acc;"),
    ]);
    assert_eq!(run(&plan, &[r#"{"items":[{"n":1},{"n":2},{"n":4}]}"#])[0]["out"], 7);
}

/// A loop's accumulators are every name its body binds outside the loop variable. A copy of a name that is paths binds
/// each path, and a path under the loop variable stays the variable's. A fold whose value reads the accumulator runs
/// per element, so each trip sees the one before.
#[test]
fn a_loops_accumulators_are_what_its_body_binds() {
    let plan = ok(vec![
        code(0, &json("nums: array<i64>; pairs: array<{key: str}>;")),
        code(
            1,
            "o.a = 1; o.b = 2; acc: {a: i64, b: i64, ...} = null; last: i64 = null; for (let x of nums) { acc = o; last = x; }
             for (let t of pairs) { t.tag = t.key; }
             total = 1; for (let x of nums) { total = total + total * x; }",
        ),
    ]);
    let (row, keys) = internal_keys(&plan, &[r#"{"nums":[1,2,3],"pairs":[{"key":"a"}]}"#]);
    assert_eq!(
        cols(&[row], &["acc", "last", "total"]),
        rows![[{"a": 1, "b": 2}, 3, 24]]
    );
    assert!(
        !keys.iter().any(|k| k == "t"),
        "the loop variable's path leaked: {keys:?}"
    );
}

// -- `map` and `filter` ------------------------------------------------------

/// `map` and `filter` map and filter every element vectorized, in either call syntax, may read outer fields, chain,
/// and sit inside a larger expression. Filtering everything out gives an empty array.
#[test]
fn map_and_filter_are_vectorized() {
    let cases: &[(&str, &str, Value)] = &[
        ("[1, 2, 3]", "out = xs.map(x => x * 2);", json!([2, 4, 6])),
        ("[1, 2, 3, 4]", "out = xs.filter(x => x % 2 == 0);", json!([2, 4])),
        ("[1, 2, 3]", "out = map(xs, x => x * 2);", json!([2, 4, 6])),
        ("[1, 2, 3]", "out = xs.map((x) => x * 2);", json!([2, 4, 6])),
        (
            "[1, 2, 3, 4]",
            "out = xs.filter(x => x > threshold).map(x => x + threshold);",
            json!([5, 6]),
        ),
        (
            "[1, 2, 3, 4]",
            "out = xs.map(x => x * 10).filter(x => x > 25);",
            json!([30, 40]),
        ),
        ("[1, 2, 3]", "out = xs.filter(x => x > 100);", json!([])),
        (r#"["a", "b"]"#, r#"out = xs.map(x => x + "!");"#, json!(["a!", "b!"])),
        ("[1, 2, 3, 4]", "out = length(xs.filter(x => x > 1)) + 1;", json!(4)),
        ("[1, 2, 3]", "let d = xs.map(x => x * 2); out = d;", json!([2, 4, 6])),
        ("[1, 2, 3]", "out = [9].map(x => x + 1);", json!([10])),
    ];
    for (xs, body, want) in cases {
        let plan = ok(vec![code(0, &format!("xs = {xs}; threshold = 2;")), code(1, body)]);
        assert_eq!(plan.barriers.len(), 0, "{body}: {:?}", plan.barriers);
        assert_eq!(run(&plan, &["{}"])[0]["out"], *want, "{body}");
    }
}

/// An element stays only where the predicate is a true Bool. A record whose value is not a bool is refused by the
/// declaration: the predicate reads null, keeps no element, and the batch runs on.
#[test]
fn a_filter_keeps_an_element_only_where_its_predicate_is_a_true_bool() {
    let plan = ok(vec![
        code(0, &json("xs: array<i64>; flag: bool;")),
        code(1, "out = xs.filter(v => flag ?? false);"),
    ]);
    let bools = [
        r#"{"xs":[1,2],"flag":true}"#,
        r#"{"xs":[1,2],"flag":false}"#,
        r#"{"xs":[1,2]}"#,
    ];
    assert_eq!(cols(&run(&plan, &bools), &["out"]), rows![[[1, 2]], [[]], [[]]]);
    let text = [r#"{"xs":[1,2],"flag":"yes"}"#];
    assert_eq!(cols(&run(&plan, &text), &["out"]), rows![[[]]]);
    // One batch holding a Bool, a text and a number: the two that are no bool are refused, and the batch still runs.
    let mixed = [bools[0], text[0], r#"{"xs":[1,2],"flag":3}"#];
    assert_eq!(run(&plan, &mixed).len(), 3);
    let errors = run_scoped_on(&plan, &mixed, None);
    let refused: Vec<u32> = errors
        .entries()
        .iter()
        .filter(|a| matches!(a.kind, ExceptionKind::UnparsableField { .. }))
        .map(|a| a.row)
        .collect();
    assert_eq!(refused, vec![1, 2], "{:?}", errors.entries());
}

/// A fold answers what the general loop answers: a row that runs no trip keeps the value from before it, whatever
/// its type. The second body of each pair adds a statement that reads the accumulator, which makes the loop a row
/// barrier.
#[test]
fn a_fold_answers_as_the_general_loop_does() {
    let docs = [r#"{"xs":[],"flag":true}"#, r#"{"xs":[2],"flag":true}"#];
    for update in ["a = v > 1 && a;", "a = v > 1 || a;", "a = v > 1;"] {
        let body =
            |extra: &str| format!("let a = flag ?? false; let n = 0; for (let v of xs) {{ {update}{extra} }} out = a;");
        let answers = |body: String, barriers: usize| {
            let plan = ok(vec![code(0, &json("xs: array<i64>; flag: bool;")), code(1, &body)]);
            assert_eq!(plan.barriers.len(), barriers, "{body}");
            run(&plan, &docs)
                .iter()
                .map(|r| r["out"].clone())
                .collect::<Vec<Value>>()
        };
        let (fold, general) = (answers(body(""), 0), answers(body(" if (a) { n = n + 1; }"), 1));
        assert_eq!(fold, general, "{update}");
        assert_eq!(fold[0], json!(true), "{update}");
    }
}

/// An arrow's parameter is an element, not a field: nothing the arrow binds is live after it, and a dotted read
/// through the parameter reaches into the element.
#[test]
fn an_arrow_parameter_is_not_a_field() {
    let plan = ok(vec![
        code(0, &json("items: array<{n: i64, tag: str}>;")),
        code(1, r#"out = items.filter(it => it.n > 1).map(it => it.tag + "!");"#),
    ]);
    assert!(
        !plan
            .tree
            .leaves()
            .iter()
            .any(|(n, _)| n == "it" || n.starts_with("it.")),
        "{:?}",
        plan.tree.leaves()
    );
    let doc = r#"{"items":[{"n":1,"tag":"a"},{"n":2,"tag":"b"},{"n":3,"tag":"c"}]}"#;
    assert_eq!(run(&plan, &[doc])[0]["out"], json!(["b!", "c!"]));
}

/// A `for` whose accumulator reads a `filter` folds, with the `filter`'s own loop inside it running once per element.
#[test]
fn a_loop_accumulating_a_filtered_length_folds() {
    let plan = ok(vec![
        code(0, &json("groups: array<{xs: array<i64>}>;")),
        code(
            1,
            "let n = 0; for (let g of groups) { n = n + length(g.xs.filter(x => x > 1)); } out = n;",
        ),
    ]);
    assert!(plan.barriers.is_empty());
    let doc = r#"{"groups":[{"xs":[1,2,3]},{"xs":[5]},{"xs":[0]}]}"#;
    assert_eq!(run(&plan, &[doc])[0]["out"], json!(3));
}

/// A guarded map gives a masked row null, and so does a guarded `Array` assignment.
#[test]
fn a_guarded_array_assignment_blends_per_row() {
    let plan = ok(vec![
        code(0, &json("status: i64;")),
        code(1, "xs = [1, 2, 3];"),
        code(4, "out: array<i64> = null; count: i64 = null;"),
        guarded(
            3,
            "status >= 400",
            code(2, "let d = xs.map(x => x * 2); out = d; count = length(d);"),
        ),
    ]);
    let rows = run(&plan, &[r#"{"status":500}"#, r#"{"status":100}"#]);
    assert_eq!(cols(&rows, &["out", "count"]), rows![[[2, 4, 6], 3], [null, null]]);
}

/// A struct field whose name holds a dot is one key: it serializes as written, and a field named by its head is
/// another key beside it.
#[test]
fn a_dotted_struct_field_serializes_as_one_key() {
    let plan = ok(vec![
        code(0, &json("n: i64;")),
        code(
            1,
            r#"out = n > 0 ? {"a.b": 1, "a.c": 2, d: 3} : {"a.b": 4, "a.c": 5, d: 6};"#,
        ),
    ]);
    let rows = run(&plan, &[r#"{"n":1}"#, r#"{"n":-1}"#]);
    assert_eq!(rows[0]["out"], json!({"a.b": 1, "a.c": 2, "d": 3}));
    assert_eq!(rows[1]["out"], json!({"a.b": 4, "a.c": 5, "d": 6}));

    // A literal's entries apply in order, and a later key replaces only the key of its own name: `a` and `a.b` are two
    // keys. A null field writes nothing.
    let plan = ok(vec![
        code(0, &json("n: i64;")),
        code(1, r#"out = {"a.b": 1, a: null, "a.c": 2};"#),
    ]);
    let rows = run(&plan, &[r#"{"n":1}"#]);
    assert_eq!(rows[0]["out"], json!({"a.b": 1, "a.c": 2}));
    let plan = ok(vec![code(0, &json("n: i64;")), code(1, r#"out = {a: 2, "a.b": 3};"#)]);
    let rows = run(&plan, &[r#"{"n":1}"#]);
    assert_eq!(rows[0]["out"], json!({"a": 2, "a.b": 3}));

    // A key holding any number of dots is one key: the writer does not recurse on it.
    let deep = vec!["k"; 300].join(".");
    let plan = ok(vec![
        code(0, &json("n: i64;")),
        code(1, &format!(r#"out = n > 0 ? {{"{deep}": 1}} : {{"{deep}": 2}};"#)),
    ]);
    let row = &run(&plan, &[r#"{"n":1}"#])[0];
    assert_eq!(row["out"], json!({ deep.clone(): 1 }));
}

/// A path of more steps than the nesting bound nests to the bound and keeps the rest as one key: a producer-controlled
/// path cannot recurse the writer off its stack.
#[test]
fn a_path_past_the_nesting_bound_keeps_the_rest_as_one_key() {
    let deep = vec!["k"; 300].join(".");
    let plan = ok(vec![code(0, &json("n: i64;")), code(1, &format!("flat.{deep} = 1;"))]);
    let row = &run(&plan, &[r#"{"n":1}"#])[0];
    let mut cur = &row["flat"];
    let mut levels = 1;
    while let Some(obj) = cur.as_object() {
        let (k, v) = obj.iter().next().expect("one key per level");
        levels += 1;
        if k.contains('.') {
            assert_eq!(*v, json!(1));
            break;
        }
        cur = v;
    }
    assert_eq!(levels, 64, "the last key sits at the nesting bound");
}

/// A local scopes its paths. `o.key` written under `let o` leaves with the block: it is not serialized, and an outer
/// field of that name is hidden inside the block and back after it.
#[test]
fn a_locals_dotted_children_leave_with_the_block() {
    let plan = ok(vec![
        code(0, JSON),
        code(
            1,
            r#"let o = {key: 1}; o.key = 5; o.extra = "x"; inner = o.key; whole = o;
               if (true) { let p = {key: 1}; p.key = 2; }"#,
        ),
    ]);
    let (row, keys) = internal_keys(&plan, &[r#"{"other":1}"#]);
    assert_eq!(cols(&[row], &["inner", "whole"]), rows![[5, {"key": 5, "extra": "x"}]]);
    assert!(
        !keys
            .iter()
            .any(|k| ["o", "p"].iter().any(|l| k == l || k.starts_with(&format!("{l}.")))),
        "a local leaked: {keys:?}"
    );

    // An outer field `o.key` is shadowed inside the block and read back after it.
    let plan = ok(vec![
        code(0, "o.key = \"outer\";"),
        code(1, r#"if (true) { let o = {key: "x"}; o.key = "inner"; } out = o.key;"#),
    ]);
    assert_eq!(cols(&run(&plan, &["{}"]), &["out"]), rows![["outer"]]);

    // So is an outer `p` itself, where the local's value binds only paths under it.
    let plan = ok(vec![
        code(0, &json("p: i64;")),
        code(
            1,
            "whole: {k: i64} = null; if (true) { let p = {k: 1}; whole = p; } after = p;",
        ),
    ]);
    assert_eq!(
        cols(&run(&plan, &[r#"{"p":5}"#]), &["whole", "after"]),
        rows![[{"k": 1}, 5]]
    );
}

/// An object literal bound to a name is its paths: `o = {k: 1}` is `o.k = 1`, so a dotted write afterward is a
/// rebind of that slot and the output nests as it does for any flat name. A whole read copies leaf by leaf; in
/// expression position it is the struct the leaves make. A literal's leaves resolve before any binds, so a leaf may
/// read what the same assignment replaces. `{}` writes nothing.
#[test]
fn a_bound_object_literal_is_its_paths() {
    let plan = ok(vec![
        code(0, &json("status: i64;")),
        code(
            1,
            r#"o = {key: 1, geo: {city: "a"}, tags: [1, 2]};
               o.key = 5; o.geo.country = "b"; o.extra.deep = true;
               whole = o; city = o.geo.city; n = length(o.tags);
               nothing = {}; oIsNull = o == null;
               p = {a: 7, b: 0}; p = {a: 1, b: p.a}; swapped = p.b;
               o.flag: str = null; if (status >= 400) { o.key = 9; o.flag = "x"; }"#,
        ),
    ]);
    let rows = run(&plan, &[r#"{"status":500}"#, r#"{"status":200}"#]);
    let want_o_hot =
        json!({"key": 9, "geo": {"city": "a", "country": "b"}, "tags": [1, 2], "extra": {"deep": true}, "flag": "x"});
    let want_o_cold = json!({"key": 5, "geo": {"city": "a", "country": "b"}, "tags": [1, 2], "extra": {"deep": true}});
    let want_whole = json!({"key": 5, "geo": {"city": "a", "country": "b"}, "tags": [1, 2], "extra": {"deep": true}});
    assert_eq!(rows[0]["o"], want_o_hot);
    assert_eq!(rows[1]["o"], want_o_cold);
    assert_eq!(rows[0]["whole"], want_whole, "a copy taken before the guarded writes");
    assert_eq!(
        cols(&rows, &["city", "n", "oIsNull", "swapped"]),
        rows![["a", 2, false, 7], ["a", 2, false, 7]]
    );
    assert!(rows[0].get("nothing").is_none(), "an empty literal writes nothing");
    assert!(
        !plan.instrs.iter().any(|i| matches!(i.out_ty, DataType::Struct(..))),
        "no struct slot for a bound literal"
    );
}

/// A literal's leaves are what every pass sees: a loop accumulates them, a fold takes a one-leaf literal, they stay
/// bound across a parser and move through a `Rename`, an early return seeds them, and `o["k"]` on a name that is paths
/// reads the leaf with the leaf's own type.
#[test]
fn a_literals_leaves_reach_every_pass() {
    let plan = ok(vec![
        nums(),
        code(
            1,
            "last: {value: i64} = null; for (let x of nums) { last = {value: x}; }
             acc.n = 0; for (let x of nums) { acc = {top: x, n: acc.n + 1}; }",
        ),
    ]);
    let rows = run(&plan, &[r#"{"nums":[4,5,6]}"#]);
    assert_eq!(rows[0]["last"], json!({"value": 6}));
    assert_eq!(rows[0]["acc"], json!({"top": 6, "n": 3}));

    // An empty object ahead of the one leaf; a bracket read of the accumulator; a path written in one unrolled
    // iteration is gone in the next.
    let plan = ok(vec![
        nums(),
        code(
            1,
            r#"one: {value: i64} = null; for (let x of nums) { one = {empty: {}, value: x}; }
               acc.total = 0; for (let x of nums) { acc = {total: acc["total"] + x}; }
               for (let p of [{key: 1}, {key: 2}]) { if (p.key == 1) { p.tmp = "x"; } seen = p.tmp; }"#,
        ),
    ]);
    let (row, _) = internal_keys(&plan, &[r#"{"nums":[4,5,6]}"#]);
    assert_eq!(row["one"], json!({"value": 6}));
    assert_eq!(row["acc"], json!({"total": 15}));
    assert!(row.get("seen").is_none(), "{row}");

    // A whole read of the accumulator is a self-reference: the loop runs per element, and each trip sees the last. A
    // literal iterable is the enclosing scope's, so an item can name the variable it is about to shadow.
    let plan = ok(vec![code(
        0,
        "nums = [4, 5, 6]; acc.total: i64 = null; for (let x of nums) { acc = {total: isNull(acc) ? x : 0}; }
         first = {a: 1}; got: i64 = null; for (let first of [first]) { got = first.a; }",
    )]);
    let rows = run(&plan, &["{}"]);
    assert_eq!(rows[0]["acc"], json!({"total": 0}));
    assert_eq!(rows[0]["got"], 1);
    assert_eq!(rows[0]["first"], json!({"a": 1}));

    let plan = ok(vec![
        code(0, "o = {a: 1}; r.b = 2;"),
        code(1, r#"i["k"] = {a: 4};"#),
        code(2, JSON),
        code(3, "move(r, r2); move(r2, s);"),
        code(4, "out = o.a + s.b + i.k.a;"),
    ]);
    assert_eq!(run(&plan, &[r#"{"z":0}"#])[0]["out"], 7);

    let plan = ok(vec![
        code(0, &json("n: i64; s: str;")),
        code(
            1,
            r#"function f(x: i64): {kind: str, extra: {deep: i64}} { if (x > 0) { return {kind: "pos", extra: {deep: 1}}; } return {kind: "neg"}; }
               out = f(n); dyn = {a: s}; up = toUpperCase(dyn["a"]);"#,
        ),
    ]);
    let rows = run(&plan, &[r#"{"n":1,"s":"hi"}"#, r#"{"n":-1,"s":"yo"}"#]);
    assert_eq!(rows[0]["out"], json!({"kind": "pos", "extra": {"deep": 1}}));
    assert_eq!(rows[1]["out"], json!({"kind": "neg"}));
    assert_eq!(cols(&rows, &["up"]), rows![["HI"], ["YO"]]);
}

/// A rename is literal. After `foo` becomes `source`, `source.ip` is the record's `foo.ip`, not the record's own
/// `source.ip`, and nothing is left under `foo`: a read there is null, since the record's `foo` went with the rename,
/// and a later rename's source there is a compile error of the step that names it.
#[test]
fn a_rename_of_a_declared_name_moves_its_paths() {
    let foo = || code(0, &json("foo: {ip: str, port: i64, ...};"));
    let rename = || code(1, "move(foo, source);");
    let plan = ok(vec![foo(), rename(), code(2, "ip = source.ip;")]);
    let rows = run(
        &plan,
        &[r#"{"foo":{"ip":"1.2.3.4","port":80},"source":{"ip":"shadowed"}}"#],
    );
    assert_eq!(cols(&rows, &["ip"]), rows![["1.2.3.4"]]);
    let e = err(vec![foo(), rename(), code(2, "move(foo.port, port);")]);
    assert!(e.contains("function 2: `foo.port` is not a field"), "{e}");
    let plan = ok(vec![foo(), rename(), code(2, "gone = foo.ip;")]);
    let rows = run(&plan, &[r#"{"foo":{"ip":"1.2.3.4","port":80}}"#]);
    assert_eq!(cols(&rows, &["gone"]), rows![[null]]);
}

/// A copy and a rename carry the paths bound under the name: `q.a`, `p.a` and `lit.n.a` are the record's `o.a`. A
/// parser after them writes the keys its input has over what they carried, the last write winning, and leaves the
/// rest. A local's copy leaves with its block, so on a closed record a read of it after the block is a compile error.
#[test]
fn a_copy_and_a_rename_carry_the_records_paths() {
    let doc = r#"{"o":{"a":"record"},"inner":"{\"q\":{\"a\":\"inner\"},\"p\":{\"a\":\"inner\"}}"}"#;
    let declared = || code(0, &json("o: {a: str, ...}; inner: str;"));
    let plan = ok(vec![
        declared(),
        code(
            1,
            "o.k = 1; q = o; lit = {n: o}; seen = o.a; local: str = null; if (true) { let l = o; local = l.a; }",
        ),
        code(2, "move(o, p);"),
        code(5, "qa0 = q.a; pa0 = p.a;"),
        code(3, "Object.assign($, json(inner));"),
        code(4, "qa = q.a; qk = q.k; pa = p.a; na = lit.n.a;"),
    ]);
    assert_eq!(
        cols(
            &run(&plan, &[doc]),
            &["seen", "local", "qa0", "pa0", "qa", "qk", "pa", "na"]
        ),
        rows![["record", "record", "record", "record", "inner", 1, "inner", "record"]]
    );
    let e = err(vec![
        code(0, "o = {a: \"record\"};"),
        code(1, "local: str = null; if (true) { let l = o; local = l.a; }"),
        code(2, "la = l.a;"),
    ]);
    assert!(e.contains("function 2: `l.a` is not a field"), "{e}");
}

/// An object literal's copies read every value before binding any, so `b` takes the `x.a` from before the assignment.
/// A local that holds a copy passes it on: `q.a` is `o`, as `l.a` was, until a later parse whose input has `q.a`
/// writes over it. `x` keeps its type, so `b` is a key among its unclaimed fields, which a later parse lays its own
/// keys beside. `y`'s quoted key `a.b` is one key beside `a`, so `y.a.b` is `o.b`.
#[test]
fn copies_in_one_assignment_apply_at_once_and_pass_through_locals() {
    let doc = r#"{"o":{"z":"o","b":{"z":"ob"}},"r":{"z":"r"},"x":{"a":{"z":"x"}},"inner":"{\"x\":{\"c\":{\"z\":\"inner\"}},\"q\":{\"a\":{\"z\":\"inner\"}}}"}"#;
    let plan = ok(vec![
        code(
            0,
            &json("o: {z: str, b: {z: str, ...}, ...}; r: {z: str, ...}; x: {a: {z: str, ...}, ...}; inner: str;"),
        ),
        code(
            1,
            "x = {a: o, b: x.a}; y = {\"a\": o, \"a.b\": r}; q: {a: {b: {z: str, ...}, z: str, ...}} = null; if (true) { let l = {a: o}; q = l; } qa0 = q.a.z;",
        ),
        code(2, "Object.assign($, json(inner));"),
        code(3, r#"xb = x.b.z; qa = q.a.z; yab = y.a.b.z; ydot = y["a.b"].z;"#),
    ]);
    assert_eq!(
        cols(&run(&plan, &[doc]), &["xb", "qa0", "qa", "yab", "ydot"]),
        rows![["x", "o", "inner", "ob", "r"]]
    );
}

/// A local's copy takes the name's leaves, and a path under the local is read from it. After the block, `l.a` is the
/// parser's field of that spelling.
#[test]
fn a_locals_copy_stays_in_its_scope() {
    let plan = ok(vec![
        code(0, &json("o: {k: i64, b: i64, ...}; l: {a: str, ...};")),
        code(1, "y: i64 = null; if (true) { let l = o; l.b = 1; y = l.k + l.b; }"),
        code(2, "out = l.a;"),
    ]);
    let rows = run(&plan, &[r#"{"o":{"k":1,"a":"o"},"l":{"a":"l"}}"#]);
    assert_eq!(cols(&rows, &["y", "out"]), rows![[2, "l"]]);
}

/// An eval's assignments are its step's writes in order, and a name is a value or an object, never both: `o.k` after
/// `o = 5` is a path under a scalar, which is a compile error of the step.
#[test]
fn an_eval_cannot_write_a_path_under_a_value_it_bound() {
    let e = err(vec![code(0, "o = 5; o.k = 1;")]);
    assert!(e.contains("function 0: `o` is i64 and has no fields"), "{e}");
}

/// `move` moves a name's paths with it, whether or not the name has a slot of its own. Moving a name to itself
/// leaves it where it is.
#[test]
fn rename_moves_a_names_paths() {
    let plan = ok(vec![
        code(0, "o = {k: 1, n: {d: 2}}; s.x = 3;"),
        code(1, "move(o, o); move(o, q); move(s, t);"),
        code(2, "out = q.k + q.n.d + t.x;"),
    ]);
    let (row, keys) = internal_keys(&plan, &["{}"]);
    assert_eq!(row["out"], 6);
    assert_eq!(row["q"], json!({"k": 1, "n": {"d": 2}}));
    assert!(!keys.iter().any(|k| k == "o" || k == "s"), "{keys:?}");
}

/// A struct value exists where the data has one: a literal in expression position stays a struct, and a guarded
/// assignment of it blends against what the parser declares under the name, null where the record holds nothing. An
/// array element read by path is unchanged, and a path written under a `for` variable leaves with the loop.
#[test]
fn a_struct_value_stays_where_the_data_has_one() {
    let plan = ok(vec![
        code(0, &json("out: {k: i64, ...}; status: i64;")),
        guarded(2, "status >= 400", code(1, "out = status > 450 ? {a: 1} : {a: 2};")),
    ]);
    let rows = run(
        &plan,
        &[
            r#"{"status":500}"#,
            r#"{"status":200}"#,
            r#"{"status":200,"out":{"k":1}}"#,
        ],
    );
    assert_eq!(rows[0]["out"], json!({"a": 1}));
    assert!(rows[1].get("out").is_none(), "{}", rows[1]);
    assert_eq!(rows[2]["out"], json!({"k": 1}));

    let plan = ok(vec![
        pairs(),
        code(
            1,
            r#"keys = ""; for (let p of pairs) { p.tmp = p.key + "!"; keys = keys + p.tmp; } out = keys;"#,
        ),
    ]);
    let (row, keys) = internal_keys(
        &plan,
        &[r#"{"pairs":[{"key":"a","value":"1"},{"key":"b","value":"2"}]}"#],
    );
    assert_eq!(row["out"], "a!b!");
    assert!(
        !keys.iter().any(|k| k == "p" || k.starts_with("p.")),
        "a loop variable leaked: {keys:?}"
    );
}

/// An arrow's parameter hides an outer path of the same name: `x.key` inside the arrow is the element's field.
#[test]
fn an_arrow_parameter_hides_an_outer_path() {
    let plan = ok(vec![
        code(0, &json("items: array<{key: i64}>;")),
        code(1, "x.key = \"outer\";"),
        code(2, "out = items.map(x => x.key);"),
    ]);
    assert_eq!(
        run(&plan, &[r#"{"items":[{"key":1},{"key":2}]}"#])[0]["out"],
        json!([1, 2])
    );
}

// -- functions as values -----------------------------------------------------

/// A function is a value: an arrow bound with `let`, passed to a function and called there, with a block body, any
/// number of parameters, and no types written.
#[test]
fn a_function_is_a_value() {
    let plan = ok(vec![code(
        0,
        "let double = x => x * 2; let add = (a, b) => { let s = a + b; s }; let one = () => 1;
         function twice(f, v) { f(f(v)) }
         a = double(3); b = add(1, 2); c = one(); d = twice(double, 5); e = twice(x => x + 1, 5);",
    )]);
    assert_eq!(
        cols(&run(&plan, &["{}"]), &["a", "b", "c", "d", "e"]),
        rows![[6, 3, 1, 20, 7]]
    );
}

/// An arrow reads the names where it was written, whatever the function it is passed to binds: a local of that
/// function, or the loop variable of the prelude's `map`.
#[test]
fn an_arrow_reads_the_names_where_it_was_written() {
    let plan = ok(vec![
        code(0, &json("xs: array<i64>;")),
        code(
            1,
            "function apply(f, v) { let x = 100; f(v) } let x = 1;
             a = apply(y => y + x, 1); b = xs.map(y => y + x);",
        ),
    ]);
    assert_eq!(cols(&run(&plan, &[r#"{"xs":[1,2]}"#]), &["a", "b"]), rows![[2, [2, 3]]]);
}

/// A parameter holds its argument as the call found it: an arrow passed beside it that writes the name the argument
/// read changes the name, not the parameter.
#[test]
fn a_parameter_holds_its_argument_as_the_call_found_it() {
    let plan = ok(vec![code(
        0,
        "function g(v, f) { f(); v }
         function h() { let x = 1; let r = g(x, () => { x = 2; }); r }
         function k(p) { g(p, () => { p = 2; }) }
         function m() { let x = 1; let w = () => { x = 2; 0 }; let n = (v) => { w(); v }; n(x) }
         a = h(); b = k(1); c = m();",
    )]);
    assert_eq!(cols(&run(&plan, &["{}"]), &["a", "b", "c"]), rows![[1, 1, 1]]);
}

/// An Eval step's assignment is a one-line code block: it calls a function as code does.
#[test]
fn an_eval_assignment_calls_functions_as_code_does() {
    let eval = FunctionKind::Eval {
        assignments: vec![("out".into(), "xs.map(x => x * 2)".into())],
    };
    let plan = ok(vec![code(0, &json("xs: array<i64>;")), func(1, eval)]);
    assert_eq!(run(&plan, &[r#"{"xs":[1,2]}"#])[0]["out"], json!([2, 4]));
}

/// `&&`, `||` and `??` give what they give without a call on their right, which runs only on the rows their left
/// leaves undecided: null reads as false.
#[test]
fn a_logical_operator_over_a_call_reads_null_as_false() {
    let plan = ok(vec![
        code(0, &json("c: bool;")),
        code(
            1,
            "function yes() { let x = true; x } a = c && yes(); o = c || yes(); n = c ?? yes();",
        ),
    ]);
    let rows = run(&plan, &[r#"{}"#, r#"{"c":false}"#, r#"{"c":true}"#]);
    assert_eq!(
        cols(&rows, &["a", "o", "n"]),
        rows![[false, true, true], [false, true, false], [true, true, true]]
    );
}

/// A function's `let` holds from where it is written to the end of its block, as one in code does: outside the block
/// the name is the one around it.
#[test]
fn a_functions_let_is_scoped_to_its_block() {
    let plan = ok(vec![code(
        0,
        "let t = true; let x = 1;
         let f = () => { if (t) { let x = 2; } x };
         function g(h) { let x = 5; if (t) { let x = 7; } h() + x }
         function cnt(n, h, acc) { let x = 100; if (n == 0) { return acc; } return cnt(n - 1, h, h(acc)); }
         a = f(); b = g(() => x); c = cnt(3, (v) => v + x, 0);",
    )]);
    assert_eq!(cols(&run(&plan, &["{}"]), &["a", "b", "c"]), rows![[1, 6, 3]]);
}

/// A function's `let` of an arrow names a function at any depth of its body, as it does at the top.
#[test]
fn a_function_names_an_arrow_at_any_depth() {
    let plan = ok(vec![code(
        0,
        "function f(v) { let r = 0; if (v > 0) { let h = (y) => y + 1; r = h(v); } r } out = f(2);",
    )]);
    assert_eq!(run(&plan, &["{}"])[0]["out"], json!(3));
}

/// `map` and `filter` take any element and give any value: a nested array, an object, a chain of them. A row whose
/// array is empty or null gives an empty array.
#[test]
fn map_and_filter_take_and_give_any_value() {
    let plan = ok(vec![
        code(0, &json("grid: array<array<i64>>; xs: array<i64>;")),
        code(
            1,
            r#"doubled = grid.map(r => r.map(x => x * 2));
               objects = xs.map(x => ({n: x, big: x > 1}));
               names = xs.filter(x => x > 1).map(x => "n" + toString(x));"#,
        ),
    ]);
    let rows = run(
        &plan,
        &[
            r#"{"grid":[[1,2],[3]],"xs":[1,2,3]}"#,
            r#"{"grid":[],"xs":[]}"#,
            r#"{"grid":null,"xs":null}"#,
        ],
    );
    assert_eq!(
        cols(&rows, &["doubled", "objects", "names"]),
        rows![
            [
                [[2, 4], [6]],
                [{"n": 1, "big": false}, {"n": 2, "big": true}, {"n": 3, "big": true}],
                ["n2", "n3"]
            ],
            [[], [], []],
            [[], [], []]
        ]
    );
}

/// A loop that appends to an array, under an `if` or not, is one vectorized fold, not a row barrier, and appends after
/// what the array held.
#[test]
fn a_loop_that_pushes_is_a_fold() {
    let plan = ok(vec![
        code(0, &json("xs: array<i64>;")),
        code(
            1,
            "let o: array<i64> = [7]; for (let x of xs) { if (x > 1) { o = push(o, x * 10); } } out = o;",
        ),
    ]);
    assert_eq!(plan.barriers.len(), 0, "{:?}", plan.barriers);
    assert!(plan.instrs.iter().any(|i| matches!(i.op, Op::ArrayFold(_))));
    assert_eq!(run(&plan, &[r#"{"xs":[1,2,3]}"#])[0]["out"], json!([7, 20, 30]));
}

/// A call standing alone runs for what it writes, and its value is dropped.
#[test]
fn a_call_alone_is_a_statement() {
    let plan = ok(vec![code(
        0,
        "let n = 0; function bump() { n = n + 1; } function note(x) { seen = x; x + 1 } bump(); bump(); note(3); out = n;",
    )]);
    assert_eq!(cols(&run(&plan, &["{}"]), &["out", "seen"]), rows![[2, 3]]);
}

/// A name that holds a function holds nothing else, and a function passed to itself is recursion, which has no bounded
/// expansion.
#[test]
fn a_function_value_is_refused_where_it_cannot_be_one() {
    for (body, want) in [
        ("let f = x => x; f = 3;", "names a function"),
        ("let f = x => x; out = f;", "is a function"),
        (
            "function apply(g, v) { return g(g, v); } out = apply((h, v) => h(h, v), 1);",
            "calls itself",
        ),
    ] {
        let e = err(vec![code(0, body)]);
        assert!(e.contains(want), "{body}\n  expected `{want}`, got: {e}");
    }
}

/// Functions calling the next twice over double at every level, within the nesting bound. The count of calls inlined
/// is bounded too, so the expansion is refused rather than built.
#[test]
fn a_doubling_call_chain_is_refused_before_it_is_built() {
    let last = 20;
    let mut body = String::from("function f20() { return 1; } ");
    for n in (0..last).rev() {
        body.push_str(&format!("function f{n}() {{ return f{m}() + f{m}(); }} ", m = n + 1));
    }
    body.push_str("out = f0();");
    let e = err(vec![code(0, &body)]);
    assert!(e.contains("calls inlined"), "{e}");
}

// -- names the bare form cannot spell ---------------------------------------

/// `$` is the root object, so `$.a` is the name `a`, whether it reads a field, a key only the row holds, or writes.
#[test]
fn a_path_under_the_root_is_the_name_it_spells() {
    let plan = ok(vec![
        code(0, &json("o: {a: str, ...};")),
        code(1, "x = $.o.a; u = $.t; $.y = $.o.a.length();"),
    ]);
    let rows = run(&plan, &[r#"{"o":{"a":"abc"},"t":5}"#]);
    assert_eq!(cols(&rows, &["x", "u", "y"]), rows![["abc", 5, 3]]);
}

/// `$["..."]` reads a field whose name holds a character a bare name cannot: ECS's `@timestamp`, a header's
/// `user-agent`, a dot. It is one key like any other, so an eval, a code block and an assignment all take it, and a
/// dot after it extends the name.
#[test]
fn a_quoted_name_reads_and_writes_a_field_a_bare_name_cannot_spell() {
    let plan = compile_ok(vec![
        code(
            0,
            &json(r#"$["geo-ip"]: {city: str, ...}; $["@timestamp"]: str; $["user-agent"]: str;"#),
        ),
        code(
            1,
            r#"stamp = $["@timestamp"]; ua = $["user-agent"].toUpperCase(); city = $["geo-ip"].city;"#,
        ),
        code(
            2,
            r#"
            $["x-y"] = $["user-agent"].length();
            $["src.port"] = 443;
            "#,
        ),
    ]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_raw(
        &mut env,
        &mut b,
        &[r#"{"@timestamp":"2024-01-02T03:04:05Z","user-agent":"curl","geo-ip":{"city":"Oslo"}}"#],
    );
    let rows = env.run(&plan, &mut b);
    let doc = parsed(&rows[0]);
    assert_eq!(doc["stamp"], serde_json::json!("2024-01-02T03:04:05Z"), "{doc}");
    assert_eq!(doc["ua"], serde_json::json!("CURL"), "{doc}");
    assert_eq!(doc["city"], serde_json::json!("Oslo"), "{doc}");
    assert_eq!(doc["x-y"], serde_json::json!(4), "{doc}");
    assert_eq!(doc["src.port"], serde_json::json!(443), "{doc}");
    assert!(doc.get("src").is_none(), "{doc}");
    assert!(env.errors.entries().is_empty(), "{:?}", env.errors.entries());
    b.reclaim(&mut env.pool);
}

/// The name is an ordinary string literal, so every key JSON can hold is reachable: a space, a quote, a backslash,
/// a dot, a character outside ASCII, a `$`-sign name, an empty name. A declared name spells a key holding any of
/// `.` `[` `]` `"` `\` or none at all as a JSON string in brackets.
#[test]
fn a_quoted_name_holds_any_json_key() {
    let plan = compile_ok(vec![
        parse(
            0,
            &[
                ("first name", DataType::I64),
                (r#"["say \"hi\""]"#, DataType::I64),
                (r#"["a\\b"]"#, DataType::I64),
                (r#"["x.y"]"#, DataType::I64),
                ("naïve ☃", DataType::I64),
                (r#"[""]"#, DataType::I64),
            ],
        ),
        code(
            1,
            r#"space = $["first name"]; quote = $["say \"hi\""]; slash = $["a\\b"]; dot = $["x.y"];
               uni = $["naïve ☃"]; esc = $["na\u00efve ☃"]; empty = $[""];"#,
        ),
    ]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_raw(
        &mut env,
        &mut b,
        &[r#"{"first name":1,"say \"hi\"":2,"a\\b":3,"naïve ☃":4,"":5,"x.y":6}"#],
    );
    let rows = env.run(&plan, &mut b);
    let doc = parsed(&rows[0]);
    for (field, want) in [
        ("space", 1),
        ("quote", 2),
        ("slash", 3),
        ("dot", 6),
        ("uni", 4),
        ("esc", 4),
        ("empty", 5),
    ] {
        assert_eq!(doc[field], json!(want), "`{field}`: {doc}");
    }
    assert!(env.errors.entries().is_empty(), "{:?}", env.errors.entries());
    b.reclaim(&mut env.pool);
}

/// The bare spelling is a compile error that says how to write it.
#[test]
fn a_bare_name_with_an_at_sign_is_refused_with_the_quoted_form() {
    let errors = compile_err(vec![code(0, "stamp = @timestamp;")]);
    assert!(
        errors.iter().any(|e| e.message.contains(r#"$["@timestamp"]"#)),
        "{errors:?}"
    );
}

// -- split / push / parseInt / null comparison ------------------------------

/// `split` builds a per-row array; method syntax desugars to it; an empty delimiter splits into characters, as in JS;
/// null propagates.
#[test]
fn split_builds_an_array_from_a_string() {
    let plan = ok(vec![
        code(0, &json("absent: str;")),
        code(
            1,
            "parts = split(\"a,b,c\", \",\"); method = \"a,b,c\".split(\",\"); chars = split(\"abc\", \"\"); missing = split(absent, \",\");",
        ),
    ]);
    assert_eq!(
        cols(&run(&plan, &["{}"]), &["parts", "method", "chars", "missing"]),
        rows![[["a", "b", "c"], ["a", "b", "c"], ["a", "b", "c"], null]]
    );
}

/// `push` treats a null array as empty and accumulates across independently guarded steps; a row matching no guard has
/// no array.
#[test]
fn push_accumulates_across_independently_guarded_steps() {
    let plan = ok(vec![
        code(0, &json("kind: str; tags: array<str>;")),
        guarded(
            3,
            "kind == \"a\" || kind == \"ab\"",
            code(1, "tags = push(tags, \"from-a\");"),
        ),
        guarded(
            4,
            "kind == \"b\" || kind == \"ab\"",
            code(2, "tags = push(tags, \"from-b\");"),
        ),
    ]);
    let rows = run(
        &plan,
        &[
            r#"{"kind": "a"}"#,
            r#"{"kind": "b"}"#,
            r#"{"kind": "ab"}"#,
            r#"{"kind": "none"}"#,
        ],
    );
    assert_eq!(
        cols(&rows, &["tags"]),
        rows![[["from-a"]], [["from-b"]], [["from-a", "from-b"]], [null]]
    );
}

/// An array branch a row did not take is null, as any other type's is: `== null` is true on it and `??` falls
/// through it.
#[test]
fn an_array_branch_a_row_did_not_take_is_null() {
    let plan = ok(vec![
        code(0, &json("flag: bool; xs: array<i64>;")),
        code(1, "r = flag ? null : xs; gone = r == null; n = r ?? [9];"),
    ]);
    let rows = run(&plan, &[r#"{"flag":true,"xs":[1]}"#, r#"{"flag":false,"xs":[1]}"#]);
    assert_eq!(
        cols(&rows, &["r", "gone", "n"]),
        rows![[null, true, [9]], [[1], false, [1]]]
    );
}

/// `[]` is an empty array, a value: it is written, it has a length, and `??` keeps it.
#[test]
fn an_empty_array_literal_is_a_value_not_null() {
    let plan = ok(vec![code(
        0,
        "x: array<i64> = []; n = length([]); gone = isNull([]); kept = [] ?? [9]; none = [] == null;
         both: array<i64> = [] + [];
         s: str = [] as str;",
    )]);
    assert_eq!(
        cols(&run(&plan, &["{}"]), &["x", "n", "gone", "kept", "none", "both", "s"]),
        rows![[[], 0, false, [], false, [], "[]"]]
    );
}

/// `[]` takes the element type of the array it meets: a ternary's other branch, `+`, `concat`, `??`, a pushed value, a
/// typed assignment, an argument and a return.
#[test]
fn an_empty_array_takes_the_element_type_it_meets() {
    let plan = ok(vec![
        code(0, &json("flag: bool; tags: array<str>;")),
        code(
            1,
            "function first(xs: array<str>): array<str> { return xs; }
             function none(): array<i64> { return []; }
             a = flag ? [] : [\"x\"]; a2 = push(a, \"w\");
             b = [] + [\"y\"]; c = concat([], [\"z\"]); d = push([], true);
             e: array<str> = []; f = [[], [1]]; g = tags ?? [];
             h = first([]); i = none();",
        ),
    ]);
    let rows = run(&plan, &[r#"{"flag":true,"tags":["t"]}"#, r#"{"flag":false}"#]);
    assert_eq!(
        cols(&rows, &["a", "a2", "b", "c", "d", "e", "f", "g", "h", "i"]),
        rows![
            [[], ["w"], ["y"], ["z"], [true], [], [[], [1]], ["t"], [], []],
            [["x"], ["x", "w"], ["y"], ["z"], [true], [], [[], [1]], [], [], []],
        ]
    );
}

/// An array that holds no element has no say in the element type a kernel builds, whatever its column's own type: a
/// declared array that is empty on every row of the batch meets `[]`, and a pushed value still decides.
#[test]
fn an_array_holding_no_element_leaves_the_element_type_to_the_rest() {
    let plan = ok(vec![
        code(0, &json("flag: bool; tags: array<str>;")),
        code(
            1,
            "out = flag ? [] : tags; out2 = push(out, \"p\"); c = concat([], tags); c2 = push(c, \"q\");
             n = tags ?? []; n2 = push(n, \"r\");",
        ),
    ]);
    let rows = run(&plan, &[r#"{"flag":true,"tags":[]}"#, r#"{"flag":false,"tags":[]}"#]);
    assert_eq!(
        cols(&rows, &["out", "out2", "c", "c2", "n", "n2"]),
        rows![[[], ["p"], [], ["q"], [], ["r"]], [[], ["p"], [], ["q"], [], ["r"]],]
    );
}

/// `[]` is an array: it meets nothing else, so a ternary over `[]` and text is a variant, and a loop, which
/// accumulates no array, cannot start from one.
#[test]
fn an_empty_array_meets_nothing_but_an_array() {
    let plan = ok(vec![code(0, &json("flag: bool;")), code(1, "x = flag ? [] : \"a\";")]);
    let x = plan.provenance.iter().rfind(|o| o.name == "x").unwrap();
    assert_eq!(x.ty, DataType::Variant);
    assert_eq!(
        cols(&run(&plan, &[r#"{"flag":true}"#, r#"{"flag":false}"#]), &["x"]),
        rows![[[]], ["a"]]
    );
    for (body, want) in [
        ("x = [] + 1;", "operator + is not defined for array<null> and i64"),
        ("x: i64 = [];", "`[]` is array<null>, not i64"),
        (
            "function f(s: str): str { return s; } x = f([]);",
            "argument `s` of `f` is array<null>, not str",
        ),
        (
            "function f(): str { return []; } x = f();",
            "the return of `f` is array<null>, not str",
        ),
        (
            "e: array<i64> = []; x = {...e};",
            "a spread takes an object, not array<i64>",
        ),
        (
            "let xs: array<i64> = []; for (let e of items) { xs = e; } out = xs;",
            "`e` is i64, not array<i64>; to change the type of `xs`, write `xs: i64 = e`",
        ),
    ] {
        let e = err(vec![code(0, &json("items: array<i64>;")), code(1, body)]);
        assert!(e.contains(want), "{body}\n  expected `{want}`, got: {e}");
    }
}

/// `null` and `[]` meet at any depth as they meet at the top: inside an array's element and an object's field, under a
/// ternary, `??`, `concat`, `push`, a declaration and an argument. The value that meets them keeps its own type, and
/// nothing is written as text.
#[test]
fn null_and_an_empty_array_meet_at_any_depth() {
    let plan = ok(vec![
        code(0, &json("flag: bool;")),
        code(
            1,
            "function count(xs: array<bool>): i64 { return length(xs); }
             a = flag ? {tags: []} : {tags: [\"x\"]}; b = flag ? {v: null} : {v: \"x\"};
             c = flag ? [null] : [\"x\"]; d = flag ? [[]] : [[\"x\"]];
             e = [{v: null}, {v: \"x\"}]; f = [{tags: []}, {tags: [\"x\"]}];
             g: array<bool> = [null]; h: {v: bool} = {v: null}; i = count([null]);
             j = concat([null], [\"x\"]); k = push([null], \"x\"); l = [null] ?? [\"x\"]; m = [[]] ?? [[\"x\"]];
             held: array<str> = [null]; n = flag ? held : [\"x\"];",
        ),
    ]);
    let rows = run(&plan, &[r#"{"flag":true}"#, r#"{"flag":false}"#]);
    assert_eq!(
        cols(&rows, &["a", "b", "c", "d", "n"]),
        rows![
            [{"tags": []}, null, [null], [[]], [null]],
            [{"tags": ["x"]}, {"v": "x"}, ["x"], [["x"]], ["x"]],
        ]
    );
    // The rest do not read `flag`. An object whose every field is null is nothing, so it is not written, in an array
    // or out of one.
    assert_eq!(
        cols(&rows[..1], &["e", "f", "g", "h", "i", "j", "k", "l", "m"]),
        rows![[
            [null, {"v": "x"}],
            [{"tags": []}, {"tags": ["x"]}],
            [null],
            null,
            1,
            [null, "x"],
            [null, "x"],
            [null],
            [[]]
        ]]
    );
}

/// `a[i] = x` writes position `i` of the array `a`, a negative `i` counting from the end. A null value clears the
/// position, which stays. The value is converted to the element type, and a declared array sized by `resize` holds
/// null at every position until one is written.
#[test]
fn an_element_write_sets_a_position() {
    let plan = ok(vec![code(
        0,
        "a = [1, 2, 3]; a[1] = 9; a[-1] = 7;
         b = [1, 2]; b[0] = null;
         c = [1.0, 2]; c[0] = 3;
         d: array<str> = resize([], 3); d[1] = \"x\"; d2 = push(d, \"y\");
         let e = [\"p\", \"q\"]; e[0] = \"z\"; f = e;
         g = [1]; g[parseInt(\"nope\")] = 5;",
    )]);
    assert_eq!(
        cols(&run(&plan, &["{}"]), &["a", "b", "c", "d", "d2", "f", "g"]),
        rows![[
            [1, 9, 7],
            [null, 2],
            [3.0, 2.0],
            [null, "x", null],
            [null, "x", null, "y"],
            ["z", "q"],
            [1]
        ]]
    );
    // `parseInt("nope")` is the one failure: the null index writes nothing.
    let errors = run_scoped_on(&plan, &["{}"], None);
    assert!(
        matches!(errors.entries(), [a] if matches!(a.kind, ExceptionKind::CoercionRefused { .. })),
        "{:?}",
        errors.entries()
    );
    // An `f64` written into an `array<i64>` is refused; the message names the element and its `as`.
    let e = err(vec![code(0, "c = [1, 2]; c[0] = 2.5;")]);
    assert!(
        e.contains("`2.5` is f64, not i64") && e.contains("`c[0] = 2.5 as i64`"),
        "{e}"
    );
}

/// Each row writes its own position, and a guarded write keeps the array from before it on the rows it skips.
#[test]
fn an_element_write_runs_per_row_and_under_a_guard() {
    let plan = ok(vec![
        code(0, &json("xs: array<str>; i: i64; flag: bool;")),
        code(1, "xs[i] = \"w\"; ys = [0, 0]; if (flag) { ys[i] = 1; }"),
    ]);
    let docs = [
        r#"{"xs":["a","b"],"i":0,"flag":true}"#,
        r#"{"xs":["a","b"],"i":-1,"flag":false}"#,
    ];
    assert_eq!(
        cols(&run(&plan, &docs), &["xs", "ys"]),
        rows![[["w", "b"], [1, 0]], [["a", "w"], [0, 0]]]
    );
}

/// An element write reaches an array inside a declared object, as a read of the same path does. A computed key on a
/// name bound to an open object keeps its old meaning: a loop that builds an object.
#[test]
fn an_element_write_reaches_a_declared_field_and_leaves_an_object_build_alone() {
    let plan = ok(vec![
        code(0, &json("m: {xs: array<i64>};")),
        code(1, "m.xs[0] = 1; n = m;"),
    ]);
    assert_eq!(
        cols(&run(&plan, &[r#"{"m":{"xs":[5,6]}}"#]), &["n"]),
        rows![[{"xs": [1, 6]}]]
    );

    let plan = ok(vec![
        pairs(),
        code(1, "obj: {...} = {}; for (let p of pairs) { obj[p.key] = p.value; }"),
    ]);
    let doc = r#"{"pairs":[{"key":"a","value":"1"},{"key":"b","value":"2"}]}"#;
    assert_eq!(cols(&run(&plan, &[doc]), &["obj"]), rows![[{"a": "1", "b": "2"}]]);
}

/// A position outside the array: the read is null, the write does nothing, and the row carries a warning. Rows inside
/// carry none.
#[test]
fn a_position_outside_the_array_warns() {
    let plan = ok(vec![
        code(0, &json("xs: array<i64>; i: i64;")),
        code(1, "r = xs[i]; w = xs; w[i] = 0;"),
    ]);
    let docs = [
        r#"{"xs":[1,2],"i":1}"#,
        r#"{"xs":[1,2],"i":2}"#,
        r#"{"xs":[1,2],"i":-3}"#,
    ];
    assert_eq!(
        cols(&run(&plan, &docs), &["r", "w"]),
        rows![[2, [1, 0]], [null, [1, 2]], [null, [1, 2]]]
    );
    let warned: Vec<(u32, ExceptionKind)> = run_scoped_on(&plan, &docs, None)
        .entries()
        .iter()
        .map(|a| (a.row, a.kind.clone()))
        .collect();
    let outside = ExceptionKind::IndexOutOfRange;
    assert_eq!(
        warned,
        [
            (1, outside.clone()),
            (2, outside.clone()),
            (1, outside.clone()),
            (2, outside)
        ],
        "the read, then the write"
    );

    // A row answers only for the writes it runs: one a guard skips does not warn.
    let plan = ok(vec![
        code(0, &json("xs: array<i64>; flag: bool;")),
        code(1, "if (flag) { xs[5] = 0; }"),
    ]);
    let docs = [r#"{"xs":[1],"flag":true}"#, r#"{"xs":[1],"flag":false}"#];
    let warned: Vec<u32> = run_scoped_on(&plan, &docs, None)
        .entries()
        .iter()
        .map(|a| a.row)
        .collect();
    assert_eq!(warned, [0]);
}

#[test]
fn a_position_outside_the_string_warns() {
    let plan = ok(vec![
        code(0, &json("s: str; i: i64;")),
        code(1, "r = s[i]; t = s.at(i);"),
    ]);
    let docs = [r#"{"s":"ab","i":1}"#, r#"{"s":"ab","i":2}"#, r#"{"s":"ab","i":-3}"#];
    assert_eq!(
        cols(&run(&plan, &docs), &["r", "t"]),
        rows![["b", "b"], [null, null], [null, null]]
    );
    let warned: Vec<(u32, ExceptionKind)> = run_scoped_on(&plan, &docs, None)
        .entries()
        .iter()
        .map(|a| (a.row, a.kind.clone()))
        .collect();
    let outside = ExceptionKind::IndexOutOfRange;
    assert_eq!(
        warned,
        [
            (1, outside.clone()),
            (2, outside.clone()),
            (1, outside.clone()),
            (2, outside)
        ]
    );
}

/// An element write needs an array it can read and a value its elements meet.
#[test]
fn an_element_write_needs_an_array_and_a_value_its_elements_meet() {
    for (body, want) in [
        (
            "for (let e of pairs) { xs[e.key] = e.value; } xs[0] = 1;",
            "an object's key is str, found i64",
        ),
        ("xs[0] = 1;", "an object's key is str, found i64"),
        ("a = [1]; a[0] = \"x\";", "`a[...] = value` writes str into array<i64>"),
        ("a = [1]; a[\"0\" + \"\"] = 1;", "an index must be i64"),
        (
            "n = 1; n[0] = 1;",
            "writes a key of an object or a position of an array, and `n` is i64",
        ),
    ] {
        let e = err(vec![
            code(0, &json("ns: array<i64>; pairs: array<{key: str, value: str}>;")),
            code(1, body),
        ]);
        assert!(e.contains(want), "{body}\n  expected `{want}`, got: {e}");
    }
}

/// An array's positions are its structure: a position may hold nothing, which reads as null and stays where it is. A
/// pushed null is a position too, as it is in `[1, null]`.
#[test]
fn a_null_position_stays_where_it_is() {
    let plan = ok(vec![code(
        0,
        "a = [null, 1]; n = length(a); b = push([null], \"x\"); c = push([1], null); e = at([null, 1], 1);",
    )]);
    assert_eq!(
        cols(&run(&plan, &["{}"]), &["a", "n", "b", "c", "e"]),
        rows![[[null, 1], 2, [null, "x"], [1, null], 1]]
    );
}

/// A value of type `null` holds nothing, wherever a kernel makes it: an element read of `[]` or `[null]`, a slice, a
/// map or a loop over `[]`. It has no type of its own, so a first write of it names one.
#[test]
fn a_null_typed_result_holds_nothing() {
    let plan = ok(vec![code(
        0,
        "a: i64 = at([], 0); b: i64 = [null][0]; c: array<i64> = [[]][0]; d: array<i64> = slice([], 1);
         e: array<i64> = [] + []; f: array<i64> = concat([], []); g: array<i64> = push(null, null);
         h = length([null]); let n = 0; for (let x of []) { n = n + 1; } i = n;
         k: i64 = null; m = k + 1;",
    )]);
    assert_eq!(
        cols(
            &run(&plan, &["{}"]),
            &["a", "b", "c", "d", "e", "f", "g", "h", "i", "m"]
        ),
        rows![[null, null, [], [], [], [], [null], 1, 0, null]]
    );
}

/// A typed assignment gives a value's null parts the stated type. A declaration gives a bound name no type: it is a
/// compile error that names the typed assignment.
#[test]
fn a_typed_assignment_gives_null_parts_their_type() {
    let plan = ok(vec![code(
        0,
        "x: str = null; y: array<bool> = [null]; z: array<{v: str}> = [{v: null}]; z2 = push(y, true);",
    )]);
    assert_eq!(
        cols(&run(&plan, &["{}"]), &["x", "y", "z", "z2"]),
        rows![[null, [null], [null], [null, true]]]
    );
    let plan = ok(vec![code(
        0,
        "p: array<{b: str, a: str}> = [{a: null, b: null}]; q: array<{b: str, a: str}> = [{a: \"s\", b: \"t\"}];",
    )]);
    assert_eq!(
        cols(&run(&plan, &["{}"]), &["p", "q"]),
        rows![[[null], [{"a": "s", "b": "t"}]]],
        "an object type's fields meet by name, in any order"
    );
    let e = err(vec![code(0, "w = [1]; w: array<f64>;")]);
    assert!(e.contains("`w` is already array<i64>"), "{e}");
    let e = err(vec![code(0, "x: i64 = null; x: str;")]);
    assert!(
        e.contains("`x` is already i64; to change the type of `x` to str, write `x: str = x as str`"),
        "{e}"
    );
}

/// `x == null` / `x != null` are a nullness test, the exception to null propagation.
#[test]
fn comparing_against_the_null_literal_is_a_nullness_test() {
    let plan = ok(vec![
        code(0, &json("status: i64;")),
        code(5, "has: bool = null; missing: bool = null;"),
        guarded(3, "status != null", code(1, "has = true;")),
        guarded(4, "status == null", code(2, "missing = true;")),
    ]);
    let rows = run(&plan, &[r#"{"status": 200}"#, r#"{"other": 1}"#]);
    assert_eq!(cols(&rows, &["has", "missing"]), rows![[true, null], [null, true],]);
}

/// `parseInt` defaults to base 10, takes a radix, strips `0x` at 16, and is null for a bad radix or digit.
#[test]
fn parse_int_with_a_radix() {
    let plan = ok(vec![code(
        0,
        "ten = parseInt(\"42\"); hex = parseInt(\"ff\", 16); prefixed = parseInt(\"0xFF\", 16); octal = parseInt(\"777\", 8); binary = parseInt(\"1010\", 2); bad_radix = parseInt(\"12\", 1); bad_digit = parseInt(\"g\", 16);",
    )]);
    let keys = ["ten", "hex", "prefixed", "octal", "binary", "bad_radix", "bad_digit"];
    assert_eq!(
        cols(&run(&plan, &["{}"]), &keys),
        rows![[42, 255, 255, 511, 10, null, null,]]
    );
}

// -- open objects: read and written by key ----------------------------------

/// A loop writing a name declared open by a key only the row knows builds the object, by bracket or dot access,
/// whatever else its body does; the last duplicate key wins and an empty array leaves the name as declared, null.
#[test]
fn a_loop_writing_by_key_builds_an_open_object_from_pairs() {
    let docs = [
        r#"{"pairs": [{"key": "x", "value": "y"}, {"key": "a", "value": "b"}]}"#,
        r#"{"pairs": [{"key": "x", "value": "first"}, {"key": "x", "value": "second"}]}"#,
        r#"{"pairs": []}"#,
        r#"{"pairs": [{"key": "q", "value": "1"}]}"#,
    ];
    for body in [
        "for (let p of pairs) { obj[p[\"key\"]] = p[\"value\"]; }",
        "for (let p of pairs) { obj[p.key] = p.value; }",
        "for (let p of pairs) { let k = p.key; obj[k] = p.value; }",
    ] {
        let plan = ok(vec![pairs(), code(1, &format!("obj: {{...}} = null; {body}"))]);
        assert_eq!(
            cols(&run(&plan, &docs), &["obj"]),
            rows![
                [{"x": "y", "a": "b"}],
                [{"x": "second"}],
                [null],
                [{"q": "1"}],
            ],
            "{body}"
        );
    }
}

/// A guarded loop writing by key leaves the name null on the rows it skips: the null it was declared with.
#[test]
fn a_guarded_loop_writing_by_key_leaves_the_name_null_on_the_rows_it_skips() {
    let plan = ok(vec![
        pairs(),
        code(1, "flag = length(pairs) > 1;"),
        code(4, "obj: {...} = null;"),
        guarded(3, "flag", code(2, "for (let p of pairs) { obj[p.key] = p.value; }")),
    ]);
    let rows = run(
        &plan,
        &[
            r#"{"pairs": [{"key": "x", "value": "y"}, {"key": "a", "value": "b"}]}"#,
            r#"{"pairs": [{"key": "x", "value": "y"}]}"#,
        ],
    );
    assert_eq!(cols(&rows, &["obj"]), rows![[{"x": "y", "a": "b"}], [null]]);
}

/// A loop writing by key adds its keys to what an open target held, whether a step or a JSON parser declared it open.
/// A key naming the field `z` writes the field. A row the loop does not run, masked or with an empty array, keeps the
/// target whole. A closed target, an object literal's or one a parser declared closed, is a compile error.
#[test]
fn a_loop_writing_by_key_adds_to_what_an_open_target_held() {
    let docs = [
        r#"{"m": {"z": "0"}, "pairs": [{"key": "x", "value": "y"}]}"#,
        r#"{"m": {"z": "0"}, "pairs": [{"key": "z", "value": "new"}]}"#,
        r#"{"m": {"z": "0"}, "pairs": []}"#,
        r#"{"m": {"z": "0"}, "pairs": [{"key": "x", "value": "a"}, {"key": "q", "value": "b"}]}"#,
    ];
    let declared = |m: &str| {
        vec![code(
            0,
            &json(&format!("pairs: array<{{key: str, value: str}}>; m: {m};")),
        )]
    };
    let writes = [
        code(1, "flag = length(pairs) < 2;"),
        guarded(3, "flag", code(2, "for (let p of pairs) { m[p.key] = p.value; }")),
    ];
    let opened = vec![pairs(), code(4, "m: {z: str, ...} = {z: \"0\"};")];
    let built = vec![pairs(), code(4, "m = {z: \"0\"};")];
    for prior in [opened, declared("{z: str, ...}"), built] {
        let label = format!("{prior:?}");
        let steps = prior.into_iter().chain(writes.clone()).collect();
        assert_eq!(
            cols(&run(&ok(steps), &docs), &["m"]),
            rows![
                [{"z": "0", "x": "y"}],
                [{"z": "new"}],
                [{"z": "0"}],
                [{"z": "0"}],
            ],
            "{label}"
        );
    }
    let bound = vec![pairs(), code(4, "m: {z: str} = {z: \"0\"};")];
    for prior in [bound, declared("{z: str}")] {
        let e = err(prior.into_iter().chain(writes.clone()).collect());
        assert!(e.contains("`m` is closed"), "{e}");
    }
}

/// A loop writing by key into a field of an object a JSON parser declared adds its keys to that field, which it
/// declared open, and a field the object lacks is declared open first.
/// A closed field, a declared one's, is a compile error.
#[test]
fn a_loop_writing_by_key_adds_to_an_open_field_of_a_declared_object() {
    let steps = |decl: &str, target: &str| {
        vec![
            code(
                0,
                &json("pairs: array<{key: str, value: str}>; m: {child: {z: str, ...}, w: str, ...};"),
            ),
            code(
                1,
                &format!("{decl} for (let p of pairs) {{ {target}[p.key] = p.value; }} k = {target};"),
            ),
        ]
    };
    let docs = [
        r#"{"m": {"child": {"z": "0"}, "w": "1"}, "pairs": [{"key": "x", "value": "y"}]}"#,
        r#"{"m": {"child": {"z": "0"}, "w": "1"}, "pairs": []}"#,
    ];
    let cases = [
        (
            "",
            "m.child",
            rows![
                [{"child": {"z": "0", "x": "y"}, "w": "1"}, {"z": "0", "x": "y"}],
                [{"child": {"z": "0"}, "w": "1"}, {"z": "0"}],
            ],
        ),
        (
            "m.other: {...} = null;",
            "m.other",
            rows![
                [{"child": {"z": "0"}, "w": "1", "other": {"x": "y"}}, {"x": "y"}],
                [{"child": {"z": "0"}, "w": "1"}, null],
            ],
        ),
    ];
    for (decl, target, want) in cases {
        let plan = ok(steps(decl, target));
        assert_eq!(cols(&run(&plan, &docs), &["m", "k"]), want, "{target}");
    }
    let e = err(vec![
        pairs(),
        code(
            1,
            "m: {child: {z: str}, w: str} = {child: {z: \"0\"}, w: \"1\"}; for (let p of pairs) { m.child[p.key] = p.value; }",
        ),
    ]);
    assert!(e.contains("`m.child` is closed"), "{e}");
}

/// A keyed write in a loop inside another loop carries its keys from trip to trip, on a variant declared open as on a
/// name declared open. A variant not declared open, or a closed object, is a compile error.
#[test]
fn a_keyed_write_in_a_nested_loop_carries_its_keys_from_trip_to_trip() {
    let body = r#"let rows = ["a,b", "c"]; for (let r of rows) { for (let v of split(r, ",")) { t[v] = 1; } }"#;
    let plan = ok(vec![
        pairs(),
        build(1, "o"),
        code(2, &format!("t: {{...}} = o; {body}")),
    ]);
    let doc = pairs_doc(&[("z", json!("0"))], json!({}));
    assert_eq!(
        cols(&run(&plan, &[&doc]), &["t"]),
        rows![[{"z": "0", "a": 1, "b": 1, "c": 1}]]
    );
    let plan = ok(vec![code(0, &format!("t: {{...}} = null; {body}"))]);
    assert_eq!(cols(&run(&plan, &["{}"]), &["t"]), rows![[{"a": 1, "b": 1, "c": 1}]]);
    for (seed, want) in [
        ("t = o;", "`t` is a variant"),
        ("t: {z: i64} = {z: 0};", "`t` is closed"),
    ] {
        let e = err(vec![pairs(), build(1, "o"), code(2, &format!("{seed} {body}"))]);
        assert!(e.contains(want), "{seed}: {e}");
    }
}

/// After a loop writes an open object by key, a read by name finds what the row holds: the loop's write where it
/// wrote the key, whether the key named the field or an unclaimed key, and the base's value elsewhere.
#[test]
fn a_read_after_a_keyed_write_finds_what_the_row_holds() {
    let plan = ok(vec![
        pairs(),
        code(1, "m: {z: str, ...} = {z: \"0\"};"),
        code(2, "for (let p of pairs) { m[p.key] = p.value; }"),
        code(3, "n = m.z; k = m[\"someField\"];"),
    ]);
    let docs = [
        r#"{"pairs": [{"key": "z", "value": "new"}, {"key": "someField", "value": "s"}]}"#,
        r#"{"pairs": []}"#,
    ];
    assert_eq!(cols(&run(&plan, &docs), &["n", "k"]), rows![["new", "s"], ["0", null]]);
}

/// A key only the row knows may name any field of the target, so the value meets each field's type by the table: a
/// row whose key names a field no value of its type fits leaves it null, with an exception. A declared closed target
/// is a compile error.
#[test]
fn a_keyed_write_refuses_a_row_whose_field_the_value_does_not_fit() {
    let write = "for (let p of pairs) { m[p.key] = 5; }";
    let plan = ok(vec![
        code(0, &json("pairs: array<{key: str, value: str}>; m: {z: bool, ...};")),
        code(1, write),
    ]);
    let docs = [
        r#"{"pairs": [{"key": "z", "value": ""}], "m": {"z": true}}"#,
        r#"{"pairs": [{"key": "y", "value": ""}], "m": {"z": true}}"#,
    ];
    assert_eq!(cols(&run(&plan, &docs), &["m"]), rows![[null], [{"z": true, "y": 5}]]);
    let errors = run_scoped_on(&plan, &docs, None);
    let refused: Vec<(u32, String)> = errors
        .entries()
        .iter()
        .map(|a| (a.row, a.message(&plan.slot_names())))
        .collect();
    assert_eq!(refused, [(0, "5, which could not convert to bool in m.z".to_string())]);
    let e = err(vec![pairs(), code(1, &format!("m: {{z: i64}} = {{z: 1}}; {write}"))]);
    assert!(e.contains("`m` is closed"), "{e}");
}

/// An object keeps its type as a leaf does: a named write of a value that is no object is a compile error naming the
/// typed assignment, and a keyed write converts the value to the object on each row its key names it, a row that
/// refuses leaving the object null with an exception. A null takes the object out.
#[test]
fn an_object_keeps_its_type() {
    let decl = "o: {a: {x: i64}, b: i64, ...} = {a: {x: 1}, b: 2}; ";
    let e = err(vec![code(0, JSON), code(1, &format!("{decl}o.a = 5;"))]);
    assert!(
        e.contains("`5` is i64, not {x: i64}; to change the type of `o.a`, write `o.a: i64 = 5`"),
        "{e}"
    );
    let plan = ok(vec![code(0, &json("k: str;")), code(1, &format!("{decl}o[k] = $.v;"))]);
    let docs = [
        r#"{"k": "a", "v": {"x": 9}}"#,
        r#"{"k": "a", "v": "z"}"#,
        r#"{"k": "b", "v": null}"#,
    ];
    assert_eq!(
        cols(&run(&plan, &docs), &["o"]),
        rows![[{"a": {"x": 9}, "b": 2}], [{"b": 2}], [{"a": {"x": 1}}]]
    );
    let errors = run_scoped_on(&plan, &docs, None);
    let refused: Vec<(u32, String)> = errors
        .entries()
        .iter()
        .map(|a| (a.row, a.message(&plan.slot_names())))
        .collect();
    assert_eq!(
        refused,
        [(1, r#""z", which could not convert to {x: i64} in o.a"#.to_string())]
    );
}

/// A row whose value a typed assignment cannot convert names the field it left null, by its full path.
#[test]
fn a_refused_conversion_names_its_field() {
    let plan = ok(vec![
        code(0, JSON),
        code(1, "x: i64 = $.v; o = {n: 1}; o.n: i64 = $.v;"),
    ]);
    let errors = run_scoped_on(&plan, &[r#"{"v": "a"}"#], None);
    let refused: Vec<String> = errors
        .entries()
        .iter()
        .filter(|a| matches!(a.kind, ExceptionKind::CoercionRefused { .. }))
        .map(|a| a.message(&plan.slot_names()))
        .collect();
    assert_eq!(
        refused,
        [
            r#""a", which could not convert to i64 in x"#,
            r#""a", which could not convert to i64 in o.n"#
        ],
        "{:?}",
        errors.entries()
    );
}

/// A keyed write coerces the value to the field the key names on each row, as a declaration does: a row it does not
/// fit is refused there, and a row whose key names another field or an unclaimed key is untouched by it.
#[test]
fn a_keyed_write_coerces_the_value_to_the_field_the_key_names_on_each_row() {
    let plan = ok(vec![code(0, &json("n: i64; k: str; v: str;")), code(1, "$[k] = v;")]);
    let rows = run(
        &plan,
        &[
            r#"{"n": 1, "k": "n", "v": "5"}"#,
            r#"{"n": 1, "k": "n", "v": "x"}"#,
            r#"{"n": 1, "k": "other", "v": "x"}"#,
        ],
    );
    assert_eq!(rows[0]["n"], json!(5), "{rows:?}");
    assert_eq!(
        rows[1]["smeltr"]["exception"]["exceptions"][0],
        json!({"kind": "CoercionRefused", "message": "v is \"x\", which could not convert to i64 in n"}),
        "{rows:?}"
    );
    assert_eq!((&rows[2]["n"], &rows[2]["other"]), (&json!(1), &json!("x")), "{rows:?}");
}

/// `a["b"] = v;` with a literal key outside a loop is ordinary dotted assignment.
#[test]
fn index_assign_with_a_literal_string_key_is_sugar_for_dot_assignment() {
    let bracket = ok(vec![code(0, "v = \"test\";"), code(1, "a[\"b\"] = v;")]);
    let dot = ok(vec![code(0, "v = \"test\";"), code(1, "a.b = v;")]);
    let bracket_rows = run(&bracket, &["{}"]);
    assert_eq!(bracket_rows[0]["a"], json!({"b": "test"}));
    assert_eq!(bracket_rows, run(&dot, &["{}"]));
}

/// A write by a key only the row knows, to a name nothing binds, makes an open object holding it, outside a loop too.
#[test]
fn a_keyed_write_to_a_name_nothing_binds_makes_an_open_object() {
    let plan = ok(vec![code(0, "let k = \"x\"; obj[k] = \"y\"; out = obj[k];")]);
    assert_eq!(cols(&run(&plan, &["{}"]), &["obj", "out"]), rows![[{"x": "y"}, "y"]]);
}

/// The unclaimed fields of `m: {...}` are read as variants by path, by a literal key and by a key only the row
/// knows. A key the row's object lacks reads null.
#[test]
fn an_open_object_is_read_by_path_literal_key_and_computed_key() {
    let plan = ok(vec![
        code(0, &json("k: str; m: {...};")),
        code(1, "p = m.a; l = m[\"a\"]; let v: str = m[k]; c = toUpperCase(v);"),
    ]);
    let docs = [
        r#"{"m": {"a": "x", "b": "y"}, "k": "b"}"#,
        r#"{"m": {"a": "x"}, "k": "b"}"#,
    ];
    assert_eq!(
        cols(&run(&plan, &docs), &["p", "l", "c"]),
        rows![["x", "x", "Y"], ["x", "x", null]]
    );
}

/// A bracket read under a variant is a variant whatever its key: a string reads an object's member, a number an
/// array's element, counting from the end when negative. A row whose value holds nothing at the key reads null.
#[test]
fn a_bracket_read_under_a_variant_reads_each_rows_key() {
    let plan = ok(vec![
        code(0, &json("v: variant; m: {...}; k: str; i: i64;")),
        code(
            1,
            "a = v[k]; b = v.xs[i]; c = v[-1]; d = m.x[0]; e = m.x[k]; f = v.xs[i].k;",
        ),
    ]);
    let docs = [
        r#"{"v": {"k": 1, "xs": [{"k": 2}, 3]}, "m": {"x": [4, 5]}, "k": "k", "i": 0}"#,
        r#"{"v": [6, 7], "m": {"x": {"k": 8}}, "k": "k", "i": -1}"#,
        r#"{"v": {"xs": 9}, "m": {"x": 10}, "i": 1}"#,
    ];
    assert_eq!(
        cols(&run(&plan, &docs), &["a", "b", "c", "d", "e", "f"]),
        rows![
            [1, {"k": 2}, null, 4, null, 2],
            [null, null, 7, null, 8, null],
            [null, null, null, null, null, null]
        ]
    );
}

/// A null element of a variant's array reads null: `??` passes it over.
#[test]
fn a_null_element_under_a_variant_reads_null() {
    let plan = ok(vec![
        code(0, &json("v: variant;")),
        code(1, "a = v[0] ?? \"none\"; b = isNull(v[0]);"),
    ]);
    assert_eq!(
        cols(&run(&plan, &[r#"{"v": [null, 1]}"#]), &["a", "b"]),
        rows![["none", true]]
    );
}

/// A key under a variant is a string or a number: a variant key is given a type first, and any other type is a
/// compile error.
#[test]
fn a_key_under_a_variant_is_a_str_or_an_i64() {
    let e = err(vec![code(0, &json("v: variant; w: variant;")), code(1, "a = v[w];")]);
    assert!(e.contains("`w` is a variant"), "{e}");
    let e = err(vec![code(0, &json("v: variant; f: f64;")), code(1, "a = v[f];")]);
    assert!(e.contains("a key under a variant is a str or an i64, found f64"), "{e}");
}

/// `$` is the record, and a JSON parse leaves it open: a key only the row knows reads a field of that name first, else
/// the record's other keys. Its parsers type it, so a declaration of `$` is a compile error.
#[test]
fn a_computed_key_reads_the_record_after_a_json_parse() {
    let plan = ok(vec![code(0, &json("k: str;")), code(1, "out = $[k];")]);
    let docs = [
        r#"{"a": 1, "k": "a"}"#,
        r#"{"a": 1, "k": "k"}"#,
        r#"{"a": 1, "k": "c"}"#,
    ];
    assert_eq!(cols(&run(&plan, &docs), &["out"]), rows![[1], ["k"], [null]]);
    let e = err(vec![code(0, &json("k: str;")), code(1, "$: {...}; out = $[k];")]);
    assert!(e.contains("`$` is the record"), "{e}");
}

/// A bound object has its value's type, open or closed: a declaration of it is a compile error that states that type,
/// and names the typed assignment where the declared type differs.
#[test]
fn a_declaration_of_a_bound_object_is_a_compile_error() {
    for (body, want) in [
        (
            "m: {a: i64};",
            "`m` is already {a: i64, ...}; to change the type of `m` to {a: i64}, write `m: {a: i64} = m as {a: i64}`",
        ),
        ("m: {a: i64, ...};", "`m` is already {a: i64, ...}"),
        ("m: {a: i64} = m; m: {a: i64};", "`m` is already {a: i64}"),
    ] {
        let e = err(vec![code(0, &json("m: {a: i64, ...};")), code(1, body)]);
        assert!(e.contains(want), "{body}: {e}");
    }
}

/// A key only the row knows reads a closed object's fields as it reads any object's, one key each, and writes no key
/// of one: a closed object has nowhere to put a key it lacks. The record is closed where no JSON parser has run.
#[test]
fn a_computed_key_reads_a_closed_object_and_writes_none() {
    let plan = ok(vec![
        code(0, "k = \"a\"; p = \"b.c\";"),
        code(1, r#"o = {a: 1, b: {c: 2}, "b.c": 3}; x = o[k]; y = o[p]; z = $[p];"#),
    ]);
    assert_eq!(cols(&run(&plan, &["{}"]), &["x", "y", "z"]), rows![[1, 3, null]]);
    for (body, hint) in [
        ("o: {a: i64} = {a: 1}; o[k] = 2;", "`o: {...} = o as {...}`"),
        ("$[k] = 2;", "a `json` parser"),
    ] {
        let e = err(vec![code(0, "k = \"a\";"), code(1, body)]);
        assert!(e.contains("is closed") && e.contains(hint), "{body}: {e}");
    }
    // The hint is code that compiles.
    ok(vec![
        code(0, "k = \"a\";"),
        code(1, "o = {a: 1}; o: {...} = o as {...}; o[k] = 2;"),
    ]);
}

/// A key only the row knows is one key, as a key in brackets is: a dot in it is a character. It reads, writes and
/// omits the key of that name at its object, a field or else a key among the object's unclaimed fields; the path
/// `m.a.n` and the key `a.n` are two things. A row whose key names an object the value does not fit leaves that
/// object null, with an exception.
#[test]
fn a_computed_key_is_one_key() {
    let docs: Vec<String> = ["a.n", "a", "b", "c.d"]
        .iter()
        .map(|k| format!(r#"{{"k":"{k}","m":{{"a":{{"n":1,"z":"q"}},"b":5,"a.n":9}}}}"#))
        .collect();
    let docs: Vec<&str> = docs.iter().map(String::as_str).collect();
    let declared = || code(0, &json("m: {a: {n: i64, ...}, ...}; k: str;"));
    let read = ok(vec![declared(), code(1, "x = m[k]; r = omit(m, k);")]);
    assert_eq!(
        cols(&run(&read, &docs), &["x", "r"]),
        rows![
            [9, {"a": {"n": 1, "z": "q"}, "b": 5}],
            [{"n": 1, "z": "q"}, {"b": 5, "a.n": 9}],
            [5, {"a": {"n": 1, "z": "q"}, "a.n": 9}],
            [null, {"a": {"n": 1, "z": "q"}, "b": 5, "a.n": 9}]
        ]
    );
    let write = ok(vec![declared(), code(1, "m[k] = 7;")]);
    assert_eq!(
        cols(&run(&write, &docs), &["m"]),
        rows![
            [{"a": {"n": 1, "z": "q"}, "b": 5, "a.n": 7}],
            [{"b": 5, "a.n": 9}],
            [{"a": {"n": 1, "z": "q"}, "b": 7, "a.n": 9}],
            [{"a": {"n": 1, "z": "q"}, "b": 5, "a.n": 9, "c.d": 7}]
        ]
    );
}

/// A parser's `m: {...}` keeps each value the record holds under `m` as it arrived. A read of one is a variant, and
/// a declaration fits it by the table: a value it refuses is null, and the row carries an error.
#[test]
fn an_open_object_keeps_each_value_and_a_declared_read_fits_it() {
    let plan = ok(vec![code(0, &json("m: {...};")), code(1, "let a: i64 = m.a; out = a;")]);
    let docs = [r#"{"m": {"a": "1", "b": 2}}"#, r#"{"m": {"a": "x"}}"#];
    assert_eq!(
        cols(&run(&plan, &docs), &["m", "out"]),
        rows![[{"a": "1", "b": 2}, 1], [{"a": "x"}, null]]
    );
    let errors = run_scoped_on(&plan, &docs, None);
    assert_eq!(errors.error_rows(), vec![1], "{:?}", errors.entries());
    assert!(
        errors
            .entries()
            .iter()
            .any(|a| matches!(a.kind, ExceptionKind::CoercionRefused { .. })),
        "{:?}",
        errors.entries()
    );
}

/// A plan whose step 1 runs `body` over `m: {a: str, ...}`, open, and `k`, a key each row gives.
fn keyed(body: &str) -> Plan {
    ok(vec![code(0, &json("k: str; m: {a: str, ...};")), code(1, body)])
}

/// `o[k] = v` writes the field `k` names where there is one, else the key in the unclaimed fields.
#[test]
fn a_keyed_write_writes_the_field_its_key_names_else_an_unclaimed_key() {
    let docs = [
        r#"{"m": {"a": "0", "b": "1"}, "k": "a"}"#,
        r#"{"m": {"a": "0", "b": "1"}, "k": "b"}"#,
        r#"{"m": {"a": "0", "b": "1"}, "k": "c"}"#,
    ];
    let plan = keyed("m[k] = \"new\"; f = m.a;");
    assert_eq!(
        cols(&run(&plan, &docs), &["m", "f"]),
        rows![
            [{"a": "new", "b": "1"}, "new"],
            [{"a": "0", "b": "new"}, "0"],
            [{"a": "0", "b": "1", "c": "new"}, "0"],
        ]
    );
}

/// `o[k] = null` takes the key out, a field's as an unclaimed one's.
#[test]
fn a_keyed_write_of_null_takes_the_key_out() {
    let docs = [
        r#"{"m": {"a": "0", "b": "1"}, "k": "a"}"#,
        r#"{"m": {"a": "0", "b": "1"}, "k": "b"}"#,
    ];
    assert_eq!(
        cols(&run(&keyed("m[k] = null;"), &docs), &["m"]),
        rows![[{"b": "1"}], [{"a": "0"}]]
    );
}

/// A null key writes nothing.
#[test]
fn a_keyed_write_with_a_null_key_writes_nothing() {
    let docs = [r#"{"m": {"a": "0", "b": "1"}}"#];
    assert_eq!(
        cols(&run(&keyed("m[k] = \"new\";"), &docs), &["m"]),
        rows![[{"a": "0", "b": "1"}]]
    );
}

/// `keys(o)` is each field that holds a value, then, where `o` is open, each unclaimed key: an object a JSON parser
/// gives has the record's other keys under it, and an object literal has only its fields.
#[test]
fn keys_are_the_fields_holding_a_value_then_the_unclaimed_keys() {
    let plan = ok(vec![
        code(0, &json("m: {a: i64, ...};")),
        code(1, "mk = keys(m); c = {a: m.a}; ck = keys(c);"),
    ]);
    let docs = [r#"{"m": {"b": 2, "a": 1}}"#, r#"{"m": {"b": 2}}"#];
    assert_eq!(
        cols(&run(&plan, &docs), &["mk", "ck"]),
        rows![[["a", "b"], ["a"]], [["b"], []]]
    );
}

/// `entries(o)` pairs each key `keys` gives with its value, as `{key, value}`.
#[test]
fn entries_pairs_each_key_with_its_value() {
    let plan = ok(vec![code(0, &json("m: {a: i64, ...};")), code(1, "e = entries(m);")]);
    assert_eq!(
        cols(&run(&plan, &[r#"{"m": {"b": 2, "a": 1}}"#]), &["e"]),
        rows![[[{"key": "a", "value": 1}, {"key": "b", "value": 2}]]]
    );
}

/// `entries` gives every value as one type, so a closed object's fields that do not meet are a compile error. An
/// open object's values are variants.
#[test]
fn entries_whose_values_do_not_meet_is_a_compile_error() {
    let e = err(vec![code(0, "o = {a: 1, b: true}; e = entries(o);")]);
    assert!(e.contains("`entries` gives every value as one type"), "{e}");
    let plan = ok(vec![
        code(0, &json("m: {a: i64, b: bool, ...};")),
        code(1, "e = entries(m);"),
    ]);
    assert_eq!(
        cols(&run(&plan, &[r#"{"m": {"a": 1, "b": true, "c": "x"}}"#]), &["e"]),
        rows![[[{"key": "a", "value": 1}, {"key": "b", "value": true}, {"key": "c", "value": "x"}]]]
    );
}

/// `fromEntries(pairs)` is an open object holding each pair, a later key winning, each read by key as a variant.
#[test]
fn from_entries_is_an_open_object_of_its_pairs() {
    let plan = ok(vec![
        code(0, &json("pairs: array<{key: str, value: str}>; k: str;")),
        code(1, "o = fromEntries(pairs); let x: str = o[k]; v = toUpperCase(x);"),
    ]);
    let doc = pairs_doc(
        &[("x", json!("a")), ("y", json!("b")), ("x", json!("c"))],
        json!({"k": "x"}),
    );
    assert_eq!(
        cols(&run(&plan, &[&doc]), &["o", "v"]),
        rows![[{"x": "c", "y": "b"}, "C"]]
    );
}

/// `omit(o, k, ...)` is `o` without those keys: a literal key drops its field, a key only the row knows nulls the
/// field it names on that row, and each is taken out of the unclaimed fields.
#[test]
fn omit_drops_a_literal_keys_field_and_nulls_a_computed_keys_on_its_row() {
    let docs = [
        r#"{"m": {"a": "0", "b": "1"}, "k": "a"}"#,
        r#"{"m": {"a": "0", "b": "1"}, "k": "b"}"#,
    ];
    let plan = keyed("l = omit(m, \"a\"); c = omit(m, k);");
    assert_eq!(
        cols(&run(&plan, &docs), &["l", "c"]),
        rows![[{"b": "1"}, {"b": "1"}], [{"b": "1"}, {"a": "0"}]]
    );
}

/// `keys`, `entries` and `omit` are also methods of the object: `o.keys()` is `keys(o)`.
#[test]
fn a_function_over_an_objects_keys_is_also_a_method() {
    let plan =
        keyed("mk = m.keys(); fk = keys(m); me = m.entries(); fe = entries(m); mo = m.omit(k); fo = omit(m, k);");
    let rows = run(&plan, &[r#"{"m": {"a": "0", "b": "1"}, "k": "a"}"#]);
    for (method, function) in [("mk", "fk"), ("me", "fe"), ("mo", "fo")] {
        assert!(!rows[0][method].is_null(), "{method}: {}", rows[0]);
        assert_eq!(rows[0][method], rows[0][function], "{method}");
    }
}

/// Under a guard, an object open on some rows is open: a read of a key it has no field for is a variant, the key's
/// value where the row's object holds it and null where the row's object was closed.
#[test]
fn an_object_open_on_some_rows_under_a_guard_is_open() {
    let plan = ok(vec![
        code(0, &json("flag: bool; pairs: array<{key: str, value: str}>;")),
        code(
            1,
            "o: {a: str, ...} = {a: \"1\"}; if (flag) { o = fromEntries(pairs); } b = o.b;",
        ),
    ]);
    let docs = [
        r#"{"flag": true, "pairs": [{"key": "b", "value": "2"}]}"#,
        r#"{"flag": false, "pairs": [{"key": "b", "value": "2"}]}"#,
    ];
    assert_eq!(
        cols(&run(&plan, &docs), &["o", "b"]),
        rows![[{"b": "2"}, "2"], [{"a": "1"}, null]]
    );
}

// -- sugar: compound assignment, `++`/`--`, template literals ---------------

/// Compound assignment, `??=` and `++`/`--` desugar onto plain assignment.
#[test]
fn compound_assignment_and_increment_desugar_to_assignment() {
    let plan = ok(vec![
        code(0, &json("status: i64; host: str;")),
        code(
            1,
            r#"let n = status; n += 100; n -= 1; n *= 2; n++; n++; n--; total = n;
               let h = host; h ??= "unknown"; resolved = h;"#,
        ),
    ]);
    let rows = run(&plan, &[r#"{"status":200,"host":"web01"}"#, r#"{"status":10}"#]);
    assert_eq!(
        cols(&rows, &["total", "resolved"]),
        rows![
            // ((status + 100 - 1) * 2) + 2 - 1
            [599, "web01"],
            [219, "unknown"],
        ]
    );
}

/// Template literals interpolate whole expressions, stringify non-strings, and close holes past nested braces and strings.
#[test]
fn template_literals_desugar_to_concatenation() {
    let plan = ok(vec![
        code(0, &json("host: str; status: i64;")),
        code(
            1,
            r#"msg = `host=${host} status=${status}`;
               sev = `severity=${status >= 400 ? "high" : "low"}`;
               plain = `just text`; empty = ``;
               braces = `v=${ "}" } w=${ "a}b" }`;"#,
        ),
    ]);
    let rows = run(
        &plan,
        &[r#"{"host":"web01","status":500}"#, r#"{"host":"db","status":200}"#],
    );
    assert_eq!(
        cols(&rows, &["msg", "sev", "plain", "empty", "braces"]),
        rows![
            ["host=web01 status=500", "severity=high", "just text", "", "v=} w=a}b",],
            ["host=db status=200", "severity=low", "just text", "", "v=} w=a}b",],
        ]
    );
}

// -- user-defined functions: inlined, not called ----------------------------

/// Calls inline: declarations hoist, bodies may have locals and call other functions, and each call gets fresh locals that capture nothing.
#[test]
fn a_function_call_is_inlined_with_fresh_locals() {
    let plan = ok(vec![
        code(0, &json("a: i64; b: i64;")),
        code(
            1,
            "let t = 100; x = adjusted(a); y = adjusted(b); kept = t;
             function adjusted(n: i64): i64 { if (n < 0) { return 0; } let t = n + 10; return inc(t) * 2; }
             function inc(n: i64): i64 { return n + 1; }",
        ),
    ]);
    let rows = run(&plan, &[r#"{"a":3,"b":4}"#, r#"{"a":-4,"b":9}"#]);
    assert_eq!(cols(&rows, &["x", "y", "kept"]), rows![[28, 30, 100], [0, 40, 100],]);
}

/// A parameter or local is its own at the head of a dotted name too: `p.key`, `xs.length()` and `xs.map(...)` read
/// the argument, never a field of that name where the function is called. A call's own name is a function's, and a
/// parameter of that name leaves it alone.
#[test]
fn a_dotted_read_of_a_parameter_reads_the_argument() {
    let plan = ok(vec![
        code(
            0,
            r#"xs = [1, 2, 3]; other = [9]; p = {key: "outer"}; q = {key: "arg"};"#,
        ),
        code(
            1,
            r#"function len(xs: array<i64>): i64 { return xs.length(); }
               function key(p: {key: str}): str { return p.key; }
               function doubled(xs: array<i64>): array<i64> { return xs.map(x => x * 2); }
               function local(n: str): str { let o = {key: ""}; o.key = n; return o.key; }
               function inc(n: i64): i64 { return n + 1; }
               function shadow(inc: i64): i64 { return inc(inc); }
               a = len(other); b = key(q); c = doubled(other); d = local("l"); e = shadow(1);"#,
        ),
    ]);
    let (row, keys) = internal_keys(&plan, &["{}"]);
    assert_eq!(
        cols(&[row], &["a", "b", "c", "d", "e"]),
        rows![[1, "arg", [18], "l", 2]]
    );
    assert!(!keys.iter().any(|k| k == "o"), "a local's field leaked: {keys:?}");
}

/// A function's body is compiled where it is called: a field it reads is read by the calling step, a read of a name
/// nothing bound on a closed record is that step's compile error, and a body nothing calls writes nothing.
#[test]
fn a_function_body_is_checked_where_it_is_called() {
    let plan = ok(vec![
        code(0, &json("http: {status: i64, ...}; host: str;")),
        code(
            1,
            "function fromField(): i64 { return http.status + 1; }
             function unused(): i64 { host = \"clobbered\"; return 0; }
             out = fromField(); h = host;",
        ),
    ]);
    let rows = run(&plan, &[r#"{"http":{"status":41},"host":"h1"}"#]);
    assert_eq!(cols(&rows, &["out", "h"]), rows![[42, "h1"]]);
    let e = err(vec![code(
        1,
        "function fromField(): i64 { return http.status + 1; } out = fromField();",
    )]);
    assert!(e.contains("function 1: `http.status` is not a field"), "{e}");
}

/// Early `return` is per-row predication: guard clauses, work after a guard, both-branches-return, and a return nested in an `else`.
#[test]
fn early_return_takes_a_per_row_exit() {
    let cases: &[(&str, &[(i64, i64)])] = &[
        (
            "function f(x: i64): i64 { if (x < 0) { return -1; } let d = x * 2; return d + 1; }",
            &[(-9, -1), (3, 7)],
        ),
        (
            "function f(x: i64): i64 { if (x < 0) { return 0; } if (x > 100) { return 100; } return x; }",
            &[(-5, 0), (50, 50), (500, 100)],
        ),
        (
            "function f(x: i64): i64 { if (x >= 500) { return 3; } else { if (x >= 400) { return 2; } else { return 1; } } }",
            &[(503, 3), (404, 2), (200, 1)],
        ),
        (
            "function f(x: i64): i64 { if (x > 100) { return 100; } else { if (x < 0) { return 0; } } return x + 1000; }",
            &[(200, 100), (-1, 0), (5, 1005)],
        ),
    ];
    for (f, io) in cases {
        let plan = ok(vec![code(0, &json("n: i64;")), code(1, &format!("{f} out = f(n);"))]);
        let docs: Vec<String> = io.iter().map(|(n, _)| format!(r#"{{"n":{n}}}"#)).collect();
        let docs: Vec<&str> = docs.iter().map(String::as_str).collect();
        let want: Vec<Vec<Value>> = io.iter().map(|(_, o)| vec![json!(o)]).collect();
        assert_eq!(cols(&run(&plan, &docs), &["out"]), want, "{f}");
    }
}

/// A function's last expression with no `;` after it is its value, and so is each branch's of a final `if`/`else`.
#[test]
fn a_tail_expression_is_the_value() {
    let cases: &[(&str, &[(i64, i64)])] = &[
        ("function f(x: i64): i64 { let d = x * 2; d + 1 }", &[(3, 7)]),
        ("function f(x: i64): i64 { x }", &[(4, 4)]),
        (
            "function f(x: i64): i64 { if (x < 0) { 0 } else if (x > 100) { 100 } else { x } }",
            &[(-5, 0), (50, 50), (500, 100)],
        ),
        (
            "function f(x: i64): i64 { if (x < 0) { return -1; } x + 1000 }",
            &[(-1, -1), (5, 1005)],
        ),
        // Case 1 falls into `default`, so both hold a copy of its tail.
        (
            "function f(x: i64): i64 { switch (x) { case 1: if (x > 0) { 10 } else { 20 } default: if (x > 5) { 30 } else { 40 } } }",
            &[(1, 40), (7, 30), (2, 40)],
        ),
    ];
    for (f, io) in cases {
        let plan = ok(vec![
            parse(0, &[("n", DataType::I64)]),
            code(1, &format!("{f} out = f(n);")),
        ]);
        let docs: Vec<String> = io.iter().map(|(n, _)| format!(r#"{{"n":{n}}}"#)).collect();
        let docs: Vec<&str> = docs.iter().map(String::as_str).collect();
        let want: Vec<Vec<Value>> = io.iter().map(|(_, o)| vec![json!(o)]).collect();
        assert_eq!(cols(&run(&plan, &docs), &["out"]), want, "{f}");
    }
}

/// A function cannot take a built-in's name, a statement's, a parser's or a function's: there is one namespace. A
/// `let` of an arrow is a function too.
#[test]
fn a_function_cannot_take_a_builtin_name() {
    for name in ["drop", "json", "length", "map", "keys", "rest"] {
        for body in [
            format!("function {name}(x: i64): i64 {{ x }} out = 1;"),
            format!("let {name} = (x) => x; out = 1;"),
        ] {
            let e = err(vec![code(0, &body)]);
            assert!(e.contains("built-in"), "{body}: {e}");
        }
    }
}

/// A recursive function takes a function its every tail call passes on unchanged; one that changes is refused.
#[test]
fn a_recursive_function_passes_a_function_on_unchanged() {
    let plan = ok(vec![code(
        0,
        "function cnt(n, f, acc) { if (n == 0) { return acc; } return cnt(n - 1, f, f(acc)); }
         let k = 10; a = cnt(3, (x) => x + k, 0);",
    )]);
    assert_eq!(run(&plan, &["{}"])[0]["a"], json!(30));
    let e = err(vec![code(
        0,
        "function cnt(n, f, acc) { if (n == 0) { return acc; } return cnt(n - 1, (x) => x, f(acc)); }
         a = cnt(3, (x) => x + 1, 0);",
    )]);
    assert!(
        e.contains("`f` is a function, so `cnt` must pass it to itself unchanged"),
        "{e}"
    );
}

/// Returning from inside a `while` exits the loop for that row rather than spinning to the budget.
#[test]
fn a_return_inside_a_while_exits_the_loop() {
    let plan = ok(vec![
        code(0, &json("n: i64;")),
        code(
            1,
            "function firstMultipleOfSeven(limit: i64): i64 {
               let i = 1;
               while (i < limit) { if (i % 7 == 0) { return i; } i = i + 1; }
               return -1;
             }
             out = firstMultipleOfSeven(n);",
        ),
    ]);
    let docs = [r#"{"n":30}"#, r#"{"n":5}"#, r#"{"n":0}"#];
    assert_eq!(cols(&run(&plan, &docs), &["out"]), rows![[7], [(-1)], [(-1)]]);
    assert_eq!(budget_exceeded(&run_scoped_on(&plan, &docs, None)), 0);
}

/// Returning from inside a `for` guards the rest of the body and drops the row from the wavefront, so later elements are not visited.
#[test]
fn a_return_inside_a_for_stops_visiting_elements() {
    let plan = ok(vec![
        nums(),
        code(
            1,
            "touched = 0;
             function firstBig(xs: array<i64>): i64 {
               for (let x of xs) { touched = touched + 1; if (x > 10) { return x; } }
               return -1;
             }
             out = firstBig(nums);",
        ),
    ]);
    let rows = run(
        &plan,
        &[
            r#"{"nums":[20,1,2]}"#,
            r#"{"nums":[1,2,30]}"#,
            r#"{"nums":[1,2]}"#,
            r#"{"nums":[]}"#,
        ],
    );
    assert_eq!(
        cols(&rows, &["out", "touched"]),
        rows![[20, 1], [30, 3], [(-1), 2], [(-1), 0],]
    );
    assert_few_steps(&plan, &long_nums_doc(""), "a return on element 1");
}

// -- `break` and `continue` -------------------------------------------------

/// `break` leaves a `for` at that row's own element and drops the row from the wavefront; `continue` skips one element.
#[test]
fn break_and_continue_in_a_for_loop() {
    let loop_with = |exit| {
        ok(vec![
            code(0, &json("nums: array<i64>; limit: i64;")),
            code(
                1,
                &format!("total = 0; for (let x of nums) {{ if (x > limit) {{ {exit}; }} total = total + x; }}"),
            ),
        ])
    };
    let (stop, skip) = (loop_with("break"), loop_with("continue"));
    let docs = [
        r#"{"nums":[1,2,20,3],"limit":10}"#,
        r#"{"nums":[1,2,3,4],"limit":100}"#,
        r#"{"nums":[1,2,3,4],"limit":0}"#,
    ];
    assert_eq!(cols(&run(&stop, &docs), &["total"]), rows![[3], [10], [0]]);
    assert_eq!(cols(&run(&skip, &docs), &["total"]), rows![[6], [10], [0]]);
    assert_few_steps(&stop, &long_nums_doc(",\"limit\":0"), "a break on element 1");
}

/// `break` leaves a `while` (plain or called condition) without spinning to the budget; `continue` takes the next trip.
#[test]
fn break_and_continue_in_a_while_loop() {
    let stop = "i = 0; total = 0;
        while (i < 100) { if (i > 4) { break; } total = total + i; i = i + 1; }";
    let called = "function under(x: i64, cap: i64): bool { return x < cap; } i = 0; total = 0;
        while (under(i, 100)) { if (i > 4) { break; } total = total + i; i = i + 1; }";
    for src in [stop, called] {
        let plan = ok(vec![code(0, src)]);
        assert_eq!(run(&plan, &["{}"])[0]["total"], 10, "0+1+2+3+4: {src}");
        assert_eq!(budget_exceeded(&run_scoped_on(&plan, &["{}"], None)), 0, "{src}");
    }
    let skip = ok(vec![code(
        0,
        "i = 0; total = 0; while (i < 6) { i = i + 1; if (i == 3) { continue; } total = total + i; }",
    )]);
    assert_eq!(run(&skip, &["{}"])[0]["total"], 18, "1+2+4+5+6");
}

/// A `Code` block's locals are not fields: the loop variables of nested loops are none of them live after the block.
#[test]
fn a_code_blocks_locals_are_not_fields() {
    let plan = ok(vec![
        code(0, &json("rowsOf: array<array<i64>>;")),
        code(
            1,
            "total = 0;
             for (let r of rowsOf) {
               for (let c of r) { if (c > 10) { break; } if (c == 2) { continue; } total = total + c; }
             }",
        ),
    ]);
    let leaves = plan.tree.leaves();
    let live: Vec<&str> = leaves.iter().map(|(n, _)| n.as_str()).collect();
    assert!(!live.iter().any(|n| *n == "r" || *n == "c"), "{live:?}");
    let rows = run(&plan, &[r#"{"rowsOf":[[1,2,3,20,4],[5]]}"#]);
    assert_eq!(rows[0]["total"], 9, "1 and 3 before the break, then 5");
}

/// `break` leaves only the innermost loop.
#[test]
fn break_leaves_only_the_innermost_loop() {
    let plan = ok(vec![
        code(0, &json("rowsOf: array<array<i64>>;")),
        code(
            1,
            "total = 0; outer = 0;
             for (let r of rowsOf) {
               outer = outer + 1;
               for (let c of r) { if (c > 10) { break; } total = total + c; }
             }",
        ),
    ]);
    let rows = run(&plan, &[r#"{"rowsOf":[[1,20,2],[3,4],[99]]}"#]);
    assert_eq!(cols(&rows, &["outer", "total"]), rows![[3, 8]]);
}

/// `break` leaves the loop and `return` leaves the function; the two flags are independent.
#[test]
fn break_and_return_coexist_in_one_loop_body() {
    let plan = ok(vec![
        nums(),
        code(
            1,
            "function scan(xs: array<i64>): i64 {
               let seen = 0;
               for (let x of xs) {
                 if (x < 0) { return -1; }
                 if (x > 10) { break; }
                 seen = seen + x;
               }
               return seen;
             }
             out = scan(nums);",
        ),
    ]);
    let rows = run(
        &plan,
        &[r#"{"nums":[1,2,20,4]}"#, r#"{"nums":[1,-5,3]}"#, r#"{"nums":[1,2,3]}"#],
    );
    assert_eq!(cols(&rows, &["out"]), rows![[3], [(-1)], [6]]);
}

/// Every temporary the inliner mints (`__inl*`) is a local, absent even from the internal-fields view.
#[test]
fn compiler_temporaries_are_invisible_even_with_internal_fields_shown() {
    let plan = ok(vec![code(
        0,
        "function f(x: i64): i64 { if (x > 0) { return 1; } return 2; }
         function done(y: i64): bool { return y > 5; }
         function down(k: i64, acc: i64): i64 { if (k <= 0) { return acc; } return down(k - 1, acc + k); }
         out = f(1); total = down(3, 0);
         i = 0; while (!done(i)) { i = i + 1; if (i == 2) { continue; } if (i > 4) { break; } }",
    )]);
    let (row, keys) = internal_keys(&plan, &["{}"]);
    assert!(keys.iter().all(|k| !k.starts_with("__inl")), "{keys:?}");
    assert_eq!(cols(&[row], &["out", "total", "i"]), rows![[1, 6, 5]]);
}

// -- `while`: the unbounded loop --------------------------------------------

/// A `while` is a row barrier whose rows each run their own number of trips, zero included, reassembled in row order.
#[test]
fn rows_leave_a_while_loop_at_their_own_trip() {
    let plan = ok(vec![
        code(0, &json("n: i64;")),
        code(1, "i = 0; acc = 0; s = \"\";"),
        code(2, r#"while (i < n) { acc = acc + i; s = s + "x"; i = i + 1; }"#),
    ]);
    assert_eq!(plan.barriers.len(), 1, "{:?}", plan.barriers);
    // Trip counts 3/0/6/1/2: the exit order is a 5-cycle, so a permutation
    // confused with its inverse moves rows.
    let rows = run(
        &plan,
        &[r#"{"n":3}"#, r#"{"n":0}"#, r#"{"n":6}"#, r#"{"n":1}"#, r#"{"n":2}"#],
    );
    assert_eq!(
        cols(&rows, &["acc", "i", "s"]),
        rows![[3, 3, "xxx"], [0, 0, ""], [15, 6, "xxxxxx"], [0, 1, "x"], [1, 2, "xx"],]
    );
}

/// A null condition exits rather than looping.
#[test]
fn a_null_condition_exits_rather_than_looping_forever() {
    let plan = ok(vec![
        code(0, &json("limit: i64;")),
        code(1, "i = 0;"),
        code(2, "while (i < limit) { i = i + 1; }"),
    ]);
    let rows = run(&plan, &[r#"{"limit":2}"#, "{}"]);
    assert_eq!(cols(&rows, &["i"]), rows![[2], [0]]);
}

/// A `for` nested inside a `while` is correct at both levels.
#[test]
fn a_for_loop_nested_inside_a_while_is_correct_at_both_levels() {
    let plan = ok(vec![
        code(0, &json("nums: array<i64>; reps: i64;")),
        code(1, "r = 0; total = 0; hits = 0;"),
        code(
            2,
            "while (r < reps) {
               for (let x of nums) { total = total + x; hits = hits + 1; }
               r = r + 1;
             }",
        ),
    ]);
    let rows = run(
        &plan,
        &[
            r#"{"nums":[1,2],"reps":3}"#,
            r#"{"nums":[10],"reps":1}"#,
            r#"{"nums":[1,1,1],"reps":2}"#,
        ],
    );
    assert_eq!(cols(&rows, &["total", "hits"]), rows![[9, 6], [10, 1], [6, 6],]);
}

/// A called condition is re-evaluated every trip by rotating the loop, may itself return early, and rows still exit at their own trip; the body may call too.
#[test]
fn a_while_condition_and_body_may_call_user_defined_functions() {
    let plan = ok(vec![
        code(0, &json("n: i64;")),
        code(1, "i = 0; acc = 0;"),
        code(
            2,
            "function keepGoing(x: i64, limit: i64): bool { if (limit < 0) { return false; } return x < limit; }
             function twice(x: i64): i64 { return x * 2; }
             while (keepGoing(i, n)) { acc = acc + twice(i); i = i + 1; }",
        ),
    ]);
    let rows = run(&plan, &[r#"{"n":3}"#, r#"{"n":0}"#, r#"{"n":5}"#, r#"{"n":-1}"#]);
    assert_eq!(cols(&rows, &["i", "acc"]), rows![[3, 6], [0, 0], [5, 20], [0, 0],]);
}

/// A declared field accumulates inside a loop; a typed accumulator is widened before the loop instead, and a zero-trip row keeps the widened seed.
#[test]
fn a_declared_field_accumulates_and_a_widened_accumulator_keeps_its_seed() {
    let n = || code(0, &json("n: i64;"));
    let declared = ok(vec![
        code(0, &json("n: i64; host: str;")),
        code(1, r#"i = 0; while (i < n) { host = host + "x"; i = i + 1; }"#),
    ]);
    assert_eq!(run(&declared, &[r#"{"n":2,"host":"a"}"#])[0]["host"], "axx");

    let widened = ok(vec![
        n(),
        code(1, "i = 0; x = 0.0;"),
        code(2, "while (i < n) { x = x + 0.5; i = i + 1; }"),
    ]);
    let rows = run(&widened, &[r#"{"n":0}"#, r#"{"n":3}"#]);
    assert_eq!(rows[0]["x"], 0.0);
    assert_eq!(rows[1]["x"], 1.5);
}

/// A declared field absent from the data still speaks with its declared type:
/// the parser allocates its typed column whether or not the record fills it
/// (design doc, Language). So the blend widens, and the same expression cannot emit an integer
/// for one batch and a float for the next depending on which records happened
/// to share it -- the first document is what an Elasticsearch mapping is
/// inferred from.
#[test]
fn a_declared_field_absent_from_the_data_still_widens_the_blend() {
    let plan = || {
        ok(vec![
            code(0, &json("flag: bool; count: i64; ratio: f64;")),
            code(1, "out = flag ? count : ratio;"),
        ])
    };
    let present = run(&plan(), &[r#"{"flag":true,"count":10,"ratio":2.5}"#]);
    let absent = run(&plan(), &[r#"{"flag":true,"count":10}"#]);

    assert_eq!(present[0]["out"], 10.0);
    assert_eq!(absent[0]["out"], 10.0);
    assert!(
        present[0]["out"].is_f64() && absent[0]["out"].is_f64(),
        "the number type must not depend on whether the batch carried `ratio`"
    );
}

/// A blend of a variant with another type is a variant: each row keeps the value of the branch it took, as that
/// branch had it.
#[test]
fn a_blend_of_a_variant_against_another_type_keeps_each_rows_value() {
    let docs = [
        pairs_doc(&[("host", json!("a"))], json!({"flag": true})),
        pairs_doc(&[("host", json!("a"))], json!({"flag": false})),
    ];
    let docs: Vec<&str> = docs.iter().map(String::as_str).collect();
    let blend = |expr: &str| {
        ok(vec![
            code(0, &json("flag: bool; pairs: array<{key: str, value: str}>;")),
            build(1, "m"),
            code(2, &format!("out = {expr};")),
        ])
    };

    let rows = run(&blend("flag ? m.host : 0"), &docs);
    assert_eq!(rows[0]["out"], "a");
    assert_eq!(rows[1]["out"], json!(0), "the number stays a number");

    let rows = run(&blend("flag ? 0 : m.host"), &docs);
    assert_eq!(rows[0]["out"], json!(0));
    assert_eq!(rows[1]["out"], "a", "the text stays text");
}

// -- the loop budget --------------------------------------------------------

/// A non-terminating loop stops at its block's budget, shared by every `while` in the block, and every row still running is marked with an `Error`.
#[test]
fn a_non_terminating_loop_stops_at_its_blocks_budget_and_says_so() {
    let plan = ok(vec![
        code(0, "i = 0; j = 0;"),
        code(1, "while (i >= 0) { i = i + 1; } while (j >= 0) { j = j + 1; }"),
    ]);
    let rows = run(&plan, &["{}"]);
    assert!(rows[0]["i"].as_i64().unwrap() > 0, "the first loop gets the budget");
    assert_eq!(rows[0]["j"], 0, "the second finds it spent and runs no trips");

    let started = Instant::now();
    let errors = run_scoped_on(&plan, &["{}", "{}"], None);
    assert!(started.elapsed() < DEFAULT_LOOP_BUDGET * 4, "not a bounded stop");
    assert_eq!(errors.error_rows(), vec![0, 1]);
    assert_eq!(budget_exceeded(&errors), 4, "both loops on both rows say so");
    for a in errors.entries() {
        assert!(a.kind.caught());
    }
}

/// A different `Code` block gets its own budget.
#[test]
fn a_second_code_block_gets_its_own_budget() {
    let plan = ok(vec![
        code(0, "i = 0; j = 0;"),
        code(1, "while (i >= 0) { i = i + 1; }"),
        code(2, "while (j >= 0) { j = j + 1; }"),
    ]);
    let rows = run(&plan, &["{}"]);
    assert!(rows[0]["i"].as_i64().unwrap() > 0);
    assert!(rows[0]["j"].as_i64().unwrap() > 0, "a separate step, a separate budget");
}

/// A `while` nested in a `for` shares the block's budget rather than getting a fresh one per element.
#[test]
fn a_while_nested_in_a_for_does_not_get_a_fresh_budget_per_element() {
    let plan = ok(vec![
        nums(),
        code(1, "i = 0; c = 0;"),
        code(2, "for (let e of nums) { while (i >= 0) { i = i + 1; } c = c + 1; }"),
    ]);
    let started = Instant::now();
    let rows = run(&plan, &[r#"{"nums":[1,2,3,4,5]}"#]);
    let elapsed = started.elapsed();
    assert_eq!(rows[0]["c"], 5, "the outer loop still runs every element");
    assert!(elapsed < DEFAULT_LOOP_BUDGET * 3, "{elapsed:?} scales with the array");
}

/// A recursion that never bottoms out is bounded by the loop budget and says so.
#[test]
fn a_recursion_that_never_bottoms_out_hits_the_loop_budget() {
    let plan = ok(vec![code(
        0,
        "function forever(k: i64): i64 { if (k < 0) { return 0; } return forever(k + 1); } out = forever(1);",
    )]);
    let started = Instant::now();
    let errors = run_scoped_on(&plan, &["{}"], None);
    assert!(started.elapsed() < DEFAULT_LOOP_BUDGET * 4);
    assert!(budget_exceeded(&errors) > 0, "and it has to say so");
}

// -- tail-call recursion ----------------------------------------------------

/// A tail-recursive function is a loop: rows recurse to their own depth, a guarded tail call seals the base case, code after a return stays dead, a return or tail call may sit inside a `for` or `while`, permuted arguments swap, and an invariant `Array` parameter is bound once.
#[test]
fn tail_recursion_compiles_to_a_loop() {
    let cases: &[(&str, &[(&str, i64)])] = &[
        (
            "function down(k: i64, acc: i64): i64 { if (k <= 0) { return acc; } return down(k - 1, acc + k); } out = down(n, 0);",
            &[
                (r#"{"n":4}"#, 10),
                (r#"{"n":0}"#, 0),
                (r#"{"n":10}"#, 55),
                (r#"{"n":1}"#, 1),
            ],
        ),
        (
            "function down(k: i64, acc: i64): i64 { if (k > 0) { return down(k - 1, acc + k); } return acc; } out = down(n, 0);",
            &[(r#"{"n":4}"#, 10), (r#"{"n":0}"#, 0), (r#"{"n":2}"#, 3)],
        ),
        (
            "function down(k: i64, acc: i64): i64 { if (k <= 0) { return acc; } return down(k - 1, acc + k); acc = 999; } out = down(n, 0);",
            &[(r#"{"n":4}"#, 10)],
        ),
        (
            "function f(a: i64, b: i64, k: i64): i64 { if (k <= 0) { return a; } return f(b, a, k - 1); } out = f(1, 2, n);",
            &[
                (r#"{"n":0}"#, 1),
                (r#"{"n":1}"#, 2),
                (r#"{"n":2}"#, 1),
                (r#"{"n":3}"#, 2),
            ],
        ),
        (
            "function sum(xs: array<i64>, i: i64, acc: i64): i64 { if (i >= length(xs)) { return acc; } return sum(xs, i + 1, acc + xs[i]); } out = sum(nums, 0, 0);",
            &[
                (r#"{"nums":[1,2,3]}"#, 6),
                (r#"{"nums":[]}"#, 0),
                (r#"{"nums":[7]}"#, 7),
            ],
        ),
        (
            "function f(k: i64): i64 { for (let e of [1, 2, 3]) { if (e > k) { return e; } } if (k <= 0) { return 0; } return f(k - 1); } out = f(n);",
            &[(r#"{"n":0}"#, 1), (r#"{"n":2}"#, 3), (r#"{"n":5}"#, 3)],
        ),
        (
            "function f(k: i64): i64 { let i: i64 = 0; while (i < 3) { if (i * 10 == k) { return i; } i = i + 1; } if (k <= 0) { return -1; } return f(k - 1); } out = f(n);",
            &[
                (r#"{"n":20}"#, 2),
                (r#"{"n":12}"#, 1),
                (r#"{"n":5}"#, 0),
                (r#"{"n":-3}"#, -1),
            ],
        ),
        (
            "function f(k: i64, acc: i64): i64 { if (k <= 0) { return acc; } for (let e of [1, 2]) { if (e == 2) { return f(k - 1, acc + k); } } return 0; } out = f(n, 0);",
            &[(r#"{"n":4}"#, 10), (r#"{"n":0}"#, 0)],
        ),
    ];
    for (src, io) in cases {
        let plan = ok(vec![code(0, &json("n: i64; nums: array<i64>;")), code(1, src)]);
        assert_eq!(plan.barriers.len(), 1, "{src}: {:?}", plan.barriers);
        let docs: Vec<&str> = io.iter().map(|(d, _)| *d).collect();
        let want: Vec<Vec<Value>> = io.iter().map(|(_, o)| vec![json!(o)]).collect();
        assert_eq!(cols(&run(&plan, &docs), &["out"]), want, "{src}");
    }
}

// -- reserved names ---------------------------------------------------------

/// The `__` prefix on its own stays available to authors; it means "does not serialize".
#[test]
fn an_ordinary_double_underscore_field_is_still_allowed() {
    let plan = ok(vec![code(0, r#"__destination.topic = "audit"; keep = 1;"#)]);
    let rows = run(&plan, &["{}"]);
    assert_eq!(cols(&rows, &["keep", "__destination.topic"]), rows![[1, null]]);
}

// -- what the language refuses ----------------------------------------------

/// Every construct the language refuses is a compile error of the named kind.
#[test]
fn refused_constructs_are_compile_errors() {
    // One `Code` block, no upstream steps.
    let blocks: &[(&str, &str)] = &[
        // statements and scoping
        ("let x = ;", "parse error"),
        ("let x = 1; let x = 2; y = x;", "already declared"),
        (
            "if (true) { let temp = 1; y = temp; } z = temp;",
            "`temp` is not a field",
        ),
        ("for (let x of [1, 2, 3]) { y = x; } total = x;", "`x` is not a field"),
        ("let count = 5; for (let x of count) { y = x; }", "Array"),
        ("let n = 1; let m = n++; out = m;", "parse error"),
        ("let o.key = 1; out = o.key;", "cannot be dotted"),
        ("for (let p.x of [1, 2]) { out = p.x; }", "cannot be dotted"),
        ("out = {a: 1, a: 2};", "duplicate key"),
        ("out = {...{a: {b: 1}}, a: 2};", "duplicate key"),
        ("out = {a: 1, ...{...{a: 2}}};", "duplicate key"),
        (
            "if (true) { let o = {a: {b: 1}}; } out = isNull(o.a);",
            "`o.a` is not a field",
        ),
        (
            "function f(x: bool): i64 { if (x) { return {a: 1}; } return 2; } out = f(true);",
            "the return of `f` is {a: i64}, not i64",
        ),
        // arrays and structs
        ("x = push(split(\"a,b\", \",\"), 5);", "push"),
        ("out = [1, 2, 3].filter(x => x);", "must be bool"),
        ("let f = x => x + 1; out = f;", "is a function"),
        ("out = [1, 2].map(x);", "`x` is not a field"),
        ("out = map();", "takes 2 argument"),
        ("out = [1, 2].map(x => x, 3);", "takes 2 argument"),
        ("out = [1, 2].map(a.b => 1);", "plain name"),
        ("let n = 5; out = n.map(x => x);", "`for...of` needs"),
        (
            "obj: {...} = null; let xs = [1, 2, 3]; for (let x of xs) { obj[x] = x; }",
            "an object's key is str",
        ),
        (
            "obj: {...} = null; let ps = [{key: \"a\", value: \"b\"}]; for (let p of ps) { obj[p.key] = p.nosuch; }",
            "`p.nosuch` is not a field",
        ),
        (
            "let acc = 0; let nums = [1, 2]; for (let n of nums) { acc = nums; } out = acc;",
            "`nums` is array<i64>, not i64",
        ),
        // functions
        (
            "function add(a: i64, b: i64): i64 { return a + b; } out = add(1);",
            "argument",
        ),
        (
            "function f(n: i64): i64 { if (n > 0) { return 1; } let t = n; } out = f(1);",
            "without returning",
        ),
        (
            "function f(k: i64): i64 { if (k > 9) { return f(k - 1); } for (let e of [1, 2]) { if (e > k) { return e; } } } out = f(n);",
            "without returning",
        ),
        (
            "function f(n: i64): i64 { return n; } function f(n: i64): i64 { return n; } out = f(1);",
            "more than once",
        ),
        (
            "function a(n: i64): i64 { return b(n); } function b(n: i64): i64 { return a(n); } out = a(1);",
            "calls itself",
        ),
        (
            "function f(k: i64): i64 { if (k <= 0) { return 0; } return f(k - 1) + k; } out = f(3);",
            "tail position",
        ),
        (
            "function f(k: i64): i64 { return k <= 0 ? 0 : f(k - 1); } out = f(3);",
            "ternary",
        ),
        // loops
        ("x = 1; break;", "`break` is only valid inside a loop"),
        ("if (1 > 0) { continue; }", "`continue` is only valid inside a loop"),
        (
            "function check(x: i64): i64 { if (x > 10) { break; } return x; } total = 0; for (let n of [1, 2]) { total = total + check(n); }",
            "`break` is only valid inside a loop",
        ),
        ("let i = 0; while (i) { i = i + 1; }", "must be bool"),
        (
            "let i = 0; while (i < 3) { i = i + 1; return 1; }",
            "`return` is only valid inside a `function` body",
        ),
        (
            "function g(): {a: i64, b: i64} { return {a: 1, b: 2}; } function f(k: i64): {a: i64} { if (k <= 0) { return g(); } return f(k - 1); } out = f(2);",
            "has a field `b` its type does not name",
        ),
        (
            "let i = 0; let j = 0; while (i < 3) { let k = j + 1; }",
            "assigns nothing",
        ),
        (
            "let i = 0; let j = 0; function done(x: i64): bool { return x >= 3; } while (!done(i)) { let k = j + 1; }",
            "assigns nothing",
        ),
        (
            "function under(x: i64): bool { return x < 3; } let i = 0; while (under(i)) { let j = i; j = j + 1; }",
            "assigns nothing",
        ),
        ("let i = 0; while (i < 3) { let j = i; j = j + 1; }", "assigns nothing"),
        (
            "let i = 0; let x = 1; while (i < 2) { x = \"s\"; i = i + 1; }",
            "`\"s\"` is str, not i64",
        ),
        (
            "let x = 1; let c = 0; let nums = [1, 2]; for (let e of nums) { x = \"s\"; c = c + 1; }",
            "`\"s\"` is str, not i64",
        ),
        // reserved names: the compiler's own prefix, and the pipeline's exception tag
        ("let __inl2_ret = 1; out = __inl2_ret;", "reserves"),
        (
            "function __inl9_f(a: i64): i64 { return a; } out = __inl9_f(1);",
            "reserves",
        ),
        ("let __loc1_a = 1; out = __loc1_a;", "reserves"),
        ("out = __loc1_s.toString();", "reserves"),
    ];
    for (body, kind) in blocks {
        let e = err(vec![code(0, body)]);
        assert!(e.contains(kind), "`{body}`\n  expected `{kind}`, got: {e}");
    }

    // Shapes that need a parser-declared type or a guard upstream.
    let steps: Vec<(Vec<Step>, &str)> = vec![(
        vec![
            code(0, &json("items: array<{k: str, n: i64}>;")),
            code(1, "let acc = 0; for (let t of items) { acc = t; } out = acc;"),
        ],
        "`t` is {k: str, n: i64}, not i64; to change the type of `acc`, write `acc: {k: str, n: i64} = t`",
    )];
    for (steps, kind) in steps {
        let label = format!("{steps:?}");
        let e = err(steps);
        assert!(e.contains(kind), "{label}\n  expected `{kind}`, got: {e}");
    }
}

/// A pushed `f64` widens an `i64` array to `array<f64>`, and a pushed `i64` widens to the array's `f64`. The
/// widened array is a new type, so writing it back to the `array<i64>` name is a compile error naming the typed
/// assignment.
#[test]
fn a_pushed_value_meets_the_arrays_element_type() {
    let plan = ok(vec![
        code(0, &json("n: i64;")),
        code(
            1,
            "let x: array<i64> = []; x = push(x, 1); x = push(x, 2); out = x; y: array<f64> = push(x, 2.5); z = push([1.5], n);",
        ),
    ]);
    // `out = x` names `x`'s slot, so the slot is found through the tree.
    let slot = plan.tree.leaf("out").unwrap();
    assert_eq!(plan.provenance[slot].ty, DataType::Array(Box::new(DataType::I64)));
    let rows = run(&plan, &[r#"{"n":1}"#]);
    assert_eq!(rows[0]["out"], json!([1, 2]));
    assert_eq!(rows[0]["y"], json!([1.0, 2.0, 2.5]));
    assert_eq!(rows[0]["z"], json!([1.5, 1.0]));
    let e = err(vec![code(0, "x = [1]; x = push(x, 2.5);")]);
    assert!(
        e.contains("`push(x, 2.5)` is array<f64>, not array<i64>")
            && e.contains("write `x: array<f64> = push(x, 2.5)`"),
        "{e}"
    );
}

/// A pushed `F64` value with no valid row in this batch (`parseFloat` on unparseable text) still makes the array
/// `array<f64>`: the type is the compiler's, not the batch's.
#[test]
fn pushing_an_all_null_f64_value_still_makes_the_array_f64() {
    let plan = ok(vec![code(
        0,
        "let n = parseFloat(\"nope\"); let arr = push([1], n); out = concat(arr, [1.5]);",
    )]);
    let out = plan.provenance.iter().rfind(|o| o.name == "out").unwrap();
    assert_eq!(out.ty, DataType::Array(Box::new(DataType::F64)));
    let rows = run(&plan, &[r#"{}"#]);
    assert_eq!(rows[0]["out"], json!([1.0, null, 1.5]));
}

/// A loop that pushes under an `if` computes the value only where the `if` holds: an element it leaves notes
/// nothing.
#[test]
fn a_filtered_push_notes_nothing_for_an_element_it_leaves() {
    let plan = ok(vec![
        code(0, &json("xs: array<i64>;")),
        code(
            1,
            "let o: array<f64> = []; for (let x of xs) { if (x != 0) { o = push(o, 100 / x); } } out = o;",
        ),
    ]);
    let docs = [r#"{"xs":[0,4]}"#];
    assert_eq!(run(&plan, &docs)[0]["out"], json!([25.0]));
    let errors = run_scoped_on(&plan, &docs, None);
    assert!(errors.entries().is_empty(), "{:?}", errors.entries());
}

/// A body whose trips do not depend on one another runs once over every element, and answers what the general loop,
/// which a `try` around it forces, answers.
#[test]
fn an_independent_body_folds_and_answers_as_the_general_loop_does() {
    let docs = [
        r#"{"xs":[],"groups":[]}"#,
        r#"{"xs":[1,3,null,5],"groups":[{"xs":[1,2]},{"xs":[]},{"xs":[3,0]}]}"#,
        r#"{"xs":null,"groups":null}"#,
        r#"{"xs":[4],"groups":[{"xs":null}]}"#,
    ];
    let cases = [
        (
            "let p = 0; let n = 0;",
            "for (let x of xs) { if (x > 2) { p = p + x; } else { n = n + 1; } }",
            "pos = p; neg = n;",
        ),
        (
            "let last = -1; let s = \"\"; let any = false;",
            "for (let x of xs) { let y = x * 2; if (y > 4) { last = y; s = s + \"y\"; } any = y > 8 || any; }",
            "last2 = last; s2 = s; any2 = any;",
        ),
        ("", "out = xs.map(x => { if (x == null) { return 0; } x * 10 });", ""),
        ("", "out = xs.map(x => { let y = x * 2; y + 1 });", ""),
        ("", "out = xs.filter(x => x > 1).map(x => x - 1);", ""),
        ("", "out = groups.map(g => g.xs.filter(x => x > 1));", ""),
    ];
    for (before, lp, after) in cases {
        let answers = |lp: String, folds: bool| {
            let plan = ok(vec![
                code(0, &json("xs: array<i64>; groups: array<{xs: array<i64>}>;")),
                code(1, &format!("{before} {lp} {after}")),
            ]);
            assert_eq!(plan.barriers.is_empty(), folds, "{lp}");
            run(&plan, &docs)
        };
        let general = answers(format!("try {{ {lp} }} catch (e) {{}}"), false);
        assert_eq!(answers(lp.to_string(), true), general, "{lp}");
    }
}

/// A body that reads its accumulator through a method, or prepends text to it, depends on the trip before, and runs
/// trip by trip.
#[test]
fn a_body_reading_its_accumulator_by_a_method_or_prepending_runs_per_trip() {
    for (decl, src, doc, want) in [
        (
            "xs: array<i64>;",
            "let seen = [-1]; for (let x of xs) { if (!seen.includes(x)) { seen = push(seen, x); } } out = seen;",
            r#"{"xs":[1,1,2]}"#,
            json!([-1, 1, 2]),
        ),
        (
            "xs: array<i64>;",
            "let o = [0]; for (let x of xs) { o = o.push(x); } out = o;",
            r#"{"xs":[1,2,3]}"#,
            json!([0, 1, 2, 3]),
        ),
        (
            "xs: array<str>;",
            r#"let s = ""; for (let x of xs) { if (x != "") { s = x + s; } } out = s;"#,
            r#"{"xs":["a","b","c"]}"#,
            json!("cba"),
        ),
    ] {
        let plan = ok(vec![code(0, &json(decl)), code(1, src)]);
        assert_eq!(plan.barriers.len(), 1, "{src}");
        assert_eq!(run(&plan, &[doc])[0]["out"], want, "{src}");
    }
}

/// `acc && e` runs `e` only while `acc` is true, and `acc || e` only while it is false: such a loop runs trip by trip,
/// and a division a decided row skips notes nothing.
#[test]
fn a_logical_accumulator_first_short_circuits_each_trip() {
    for (src, want) in [
        (
            "let a = true; for (let x of xs) { a = a && 100 / x > 1; } out = a;",
            false,
        ),
        (
            "let a = false; for (let x of xs) { a = a || 100 / x < 1; } out = a;",
            true,
        ),
    ] {
        let plan = ok(vec![code(0, &json("xs: array<i64>;")), code(1, src)]);
        assert_eq!(plan.barriers.len(), 1, "{src}");
        let docs = [r#"{"xs":[200,0]}"#];
        assert_eq!(run(&plan, &docs)[0]["out"], json!(want), "{src}");
        let errors = run_scoped_on(&plan, &docs, None);
        assert!(errors.entries().is_empty(), "{src}: {:?}", errors.entries());
    }
}

/// An overwritten variant accumulator stays a variant, which a row that runs no trip keeps: an operator on it is
/// refused, as it is after the general loop.
#[test]
fn an_overwritten_variant_accumulator_stays_a_variant() {
    let e = err(vec![
        code(0, &json("xs: array<i64>; v: variant;")),
        code(1, "let t = v; for (let x of xs) { t = x; } out = t + 1;"),
    ]);
    assert!(e.contains("`t` is a variant"), "{e}");
}

/// What a folded accumulator notes names the accumulator, as the general loop's write does.
#[test]
fn a_folded_accumulators_overflow_names_it() {
    let plan = ok(vec![
        code(0, &json("xs: array<i64>;")),
        code(1, "total = 0; for (let x of xs) { total = total + x; let z = x; }"),
    ]);
    assert!(plan.barriers.is_empty());
    let errors = run_scoped_on(&plan, &[r#"{"xs":[9223372036854775807,1]}"#], None);
    let names = plan.slot_names();
    let fields: Vec<_> = errors
        .entries()
        .iter()
        .map(|e| e.field.and_then(|f| names.get(&f).cloned()))
        .collect();
    assert_eq!(fields, [Some("total".to_string())]);
}

/// An operator folding a variant accumulator is refused, as it is in the general loop.
#[test]
fn a_loop_folding_a_variant_with_an_operator_is_refused() {
    let e = err(vec![
        code(0, &json("xs: array<i64>; v: variant;")),
        code(1, "let t = v; for (let x of xs) { t = t && x > 1; } out = t;"),
    ]);
    assert!(e.contains("`t` is a variant"), "{e}");
}

/// An element an `if` in a folded body skips notes nothing, whatever the accumulator folds.
#[test]
fn a_folded_body_notes_nothing_for_an_element_its_if_skips() {
    let plan = ok(vec![
        code(0, &json("xs: array<i64>;")),
        code(
            1,
            "let t = 0.0; for (let x of xs) { if (x != 0) { t = t + 100 / x; } } out = t;",
        ),
    ]);
    assert!(plan.barriers.is_empty());
    let docs = [r#"{"xs":[0,4]}"#];
    assert_eq!(run(&plan, &docs)[0]["out"], json!(25.0));
    let errors = run_scoped_on(&plan, &docs, None);
    assert!(errors.entries().is_empty(), "{:?}", errors.entries());
}

/// An `i64` pushed onto an `array<f64>` in a loop widens, as one `push` does.
#[test]
fn a_loop_pushing_i64_onto_f64_widens_each_value() {
    let plan = ok(vec![
        code(0, &json("xs: array<i64>;")),
        code(
            1,
            "let o: array<f64> = [0.5]; for (let x of xs) { o = push(o, x); } out = o;",
        ),
    ]);
    assert_eq!(run(&plan, &[r#"{"xs":[1,2]}"#])[0]["out"], json!([0.5, 1.0, 2.0]));
}

/// A pushed array widens at any depth, as one `push` does.
#[test]
fn a_loop_pushing_widens_a_value_at_any_depth() {
    let plan = ok(vec![
        code(0, &json("grid: array<array<i64>>;")),
        code(
            1,
            "let o: array<array<f64>> = []; for (let row of grid) { o = push(o, row); } out = o;",
        ),
    ]);
    assert_eq!(run(&plan, &[r#"{"grid":[[1,2]]}"#])[0]["out"], json!([[1.0, 2.0]]));
}

/// A chain of names first written `[]`, each typed by the next, settles however long it is.
#[test]
fn a_long_chain_of_names_first_written_empty_settles() {
    let names: Vec<String> = (0..12).map(|i| format!("a{i}")).collect();
    let mut body: String = names.iter().map(|n| format!("let {n} = []; ")).collect();
    for pair in names.windows(2) {
        body += &format!("{0} = push({0}, {1}); ", pair[0], pair[1]);
    }
    body += "a11 = push(a11, 1); out = a0;";
    let plan = ok(vec![code(0, &body)]);
    assert_eq!(run(&plan, &["{}"])[0]["out"], json!([[]]));
}

/// A loop that pushes nothing leaves a null array null: one with no element, or whose `if` takes none.
#[test]
fn a_loop_that_pushes_nothing_keeps_a_null_array() {
    for (body, doc) in [
        (
            "let o: array<i64> = null; for (let x of xs) { o = push(o, x); } out = o;",
            r#"{"xs":[]}"#,
        ),
        (
            "let o: array<i64> = null; for (let x of xs) { if (x > 10) { o = push(o, x); } } out = o;",
            r#"{"xs":[1]}"#,
        ),
    ] {
        let plan = ok(vec![code(0, &json("xs: array<i64>;")), code(1, body)]);
        let rows = run(&plan, &[doc]);
        assert!(rows.iter().all(|r| r.get("out").is_none()), "{body}: {rows:?}");
    }
}

/// Two blocks may each bind a name of one spelling first written `[]`: each takes the type of its own writes.
#[test]
fn each_block_types_its_own_name_first_written_empty() {
    let plan = ok(vec![
        code(0, &json("a: bool;")),
        code(
            1,
            "if (a) { let o = []; o = push(o, 1); x = o; } else { let o = []; o = push(o, 2.5); y = o; }",
        ),
    ]);
    let rows = run(&plan, &[r#"{"a":true}"#, r#"{"a":false}"#]);
    assert_eq!(cols(&rows, &["x", "y"]), rows![[[1], null], [null, [2.5]]]);
}

/// A first write binds a name to its value's type, and `null` and `[]` have none: a first write of a value that holds
/// null at any depth is a compile error naming the typed assignment, bound by `let` or not. The typed assignment
/// binds the name, and a guarded write of null to it is fine.
#[test]
fn a_first_write_of_null_names_the_type() {
    for (body, want) in [
        ("x = null;", "`x` has no type: `null` is null; write `x: type = null`"),
        (
            "let x = null;",
            "`x` has no type: `null` is null; write `x: type = null`",
        ),
        (
            "xs = [];",
            "`xs` has no type: `[]` is array<null>; write `xs: type = []`",
        ),
        (
            "o = {name: null};",
            "`o.name` has no type in `{name: null}`; write `o: type = {name: null}`, naming its type",
        ),
    ] {
        let e = err(vec![code(0, body)]);
        assert!(e.contains("has no type"), "{body}: {e}");
        assert!(e.contains(want), "{body}\n  expected `{want}`, got: {e}");
    }
    let plan = ok(vec![
        code(0, &json("flag: bool;")),
        code(
            1,
            "let seed: str = null; let x = seed; if (flag) { x = null; } if (flag) { x = \"ok\"; } out = x;
             xs: array<str> = []; o: {name: str} = {name: null};",
        ),
    ]);
    let rows = run(&plan, &[r#"{"flag":true}"#, r#"{"flag":false}"#]);
    assert_eq!(rows[0]["out"], json!("ok"));
    assert!(rows[1].get("out").is_none());
}

/// A `while` tests its condition at the end of each trip, so a row takes a trip only where the condition held:
/// `while (i < 2)` from 0 runs its body twice, with no third trip that only fails the test.
#[test]
fn a_while_takes_a_trip_only_where_its_condition_held() {
    let plan = ok(vec![code(0, "let i = 0; while (i < 2) { i = i + 1; } out = i;")]);
    let (body, other) = foreach_shape(&plan, 0);
    assert_eq!(function_calls(&plan, &["{}"], 0), (other + 1 + 2 * body) as u64);
    assert_eq!(cols(&run(&plan, &["{}"]), &["out"]), rows![[2]]);
}

/// A loop accumulates an array as it does a scalar: a row that runs keeps the body's last value, one that runs no trip
/// its value from before.
#[test]
fn a_loop_accumulates_an_array() {
    let plan = ok(vec![code(
        0,
        "let x: array<i64> = null; let i = 0; while (i < 1) { x = [1]; i = i + 1; } out = x;",
    )]);
    assert_eq!(cols(&run(&plan, &["{}"]), &["out"]), rows![[[1]]]);

    let plan = ok(vec![
        parse(0, &[("ns", DataType::Array(Box::new(DataType::I64)))]),
        code(1, "a = [1, 2]; for (let x of ns) { a[0] = x; }"),
    ]);
    let rows = run(&plan, &[r#"{"ns": [5, 6]}"#, r#"{"ns": []}"#]);
    assert_eq!(cols(&rows, &["a"]), rows![[[6, 2]], [[1, 2]]]);
}

/// A row-barrier loop's result takes the type its values carry. A member of a built object is a variant, so a value
/// written in the body meets the pre-loop value as a variant: each row keeps its own, a number a number.
#[test]
fn a_loops_results_take_the_type_their_values_carry() {
    let docs = [
        pairs_doc(&[("a", json!(5)), ("k", json!(1))], json!({"n": [1, 2]})),
        pairs_doc(&[("a", json!(6)), ("k", json!(3))], json!({"n": []})),
    ];
    let docs: Vec<&str> = docs.iter().map(String::as_str).collect();
    let plan = |body: &str| {
        ok(vec![
            code(0, &json("n: array<i64>; pairs: array<{key: str, value: i64}>;")),
            build(1, "m"),
            code(2, body),
        ])
    };
    assert_eq!(
        cols(
            &run(
                &plan(
                    r#"q = m.a; w = m.k; a: variant = null; k: variant = null; for (let x of n) { a = m.a; k = m.k; q = "s"; w = 0.5; }"#
                ),
                &docs
            ),
            &["a", "k", "q", "w"]
        ),
        rows![[5, 1, "s", 0.5], [null, null, 6, 3]]
    );

    // A name bound before the loop keeps its type in it: the member is written `as` it.
    let every = plan(r#"q = "s"; r: i64 = null; for (let x of n) { q = m.k as str; r = 1; }"#);
    assert_eq!(cols(&run(&every, &docs[..1]), &["q"]), rows![["1"]]);
}

/// A row answers only for the value it took (design doc, Columnar data model). Each body here divides by an element
/// on a path row 0 does not take and row 1 does, so row 1 alone fails.
#[test]
fn a_row_does_not_answer_for_a_branch_it_did_not_take() {
    for body in [
        // A `for` under a guard, compiled to a fold.
        "let total = 0.0; if (flag) { for (let x of xs) { total += 100 / x; } }",
        // A `map` in a ternary's branch.
        "r = flag ? xs.map(x => 100 / x) : xs.map(x => 1.0);",
        "r = !flag ? xs.map(x => 1.0) : xs.map(x => 100 / x);",
        // A `map` under an `if`.
        "r: array<f64> = null; if (flag) { r = xs.map(x => 100 / x); }",
        // A `map` in an operand the row never reaches.
        "b = flag && xs.map(x => 100 / x)[0] > 1;",
        "b = !flag || xs.map(x => 100 / x)[0] > 1;",
        // The coalesce falls through where its left side is null.
        "n = (flag ? null : 1.0) ?? xs.map(x => 100 / x)[0];",
        // The condition of a ternary holding a `map` is computed ahead of it, under the region it sits in.
        "r: array<i64> = null; if (flag) { r = (100 / xs[0] > 1) ? xs.map(x => x) : xs.map(x => x * 2); }",
        // So is the prefix of an operand holding one.
        "b: bool = null; if (flag) { b = 100 / xs[0] > 1 && xs.map(x => x)[0] > 0; }",
        // A loop's iterable, and a map's computed source, are read under the region too.
        "k: i64 = null; if (flag) { for (let x of (100 / xs[0] > 1 ? xs : xs)) { k = x; } }",
        "r: array<i64> = null; if (flag) { r = (100 / xs[0] > 1 ? xs : xs).map(x => x + 1); }",
        // A nested `if` with an `else` computes its condition ahead, under the region it sits in.
        "k: i64 = null; if (flag) { if (100 / xs[0] > 1) { k = 1; } else { k = 2; } }",
        // A guarded fold inside another loop retracts as one at the top level does.
        "let total = 0.0; for (let y of xs) { if (flag) { for (let x of xs) { total += 100 / x; } } }",
    ] {
        let plan = ok(vec![code(0, &json("flag: bool; xs: array<i64>;")), code(1, body)]);
        let docs = [r#"{"flag":false,"xs":[0]}"#, r#"{"flag":true,"xs":[0]}"#];
        let errors = run_scoped_on(&plan, &docs, None);
        assert_eq!(errors.error_rows(), vec![1], "{body}: {:?}", errors.entries());
    }
}

/// A general loop runs its masked rows not at all: a masked row keeps its seed as it was, a number beside the text a
/// row that ran wrote, and nothing is noted.
#[test]
fn a_masked_rows_kept_seed_stays_as_it_was() {
    let plan = ok(vec![
        code(
            0,
            &json("flag: bool; xs: array<i64>; pairs: array<{key: str, value: i64}>;"),
        ),
        build(1, "m"),
        code(
            2,
            r#"v = m.v; k: i64 = null; if (flag) { for (let x of xs) { v = "s"; k = x; } }"#,
        ),
    ]);
    let docs = [
        pairs_doc(&[("v", json!(1))], json!({"flag": false, "xs": [1]})),
        pairs_doc(&[("v", json!(1))], json!({"flag": true, "xs": [1]})),
    ];
    let docs: Vec<&str> = docs.iter().map(String::as_str).collect();
    assert_eq!(cols(&run(&plan, &docs), &["v"]), rows![[1], ["s"]]);
    let errors = run_scoped_on(&plan, &docs, None);
    assert!(errors.entries().is_empty(), "{:?}", errors.entries());
}

/// The operands before a `map` reach the chain as themselves: a declared text beside another is what the chain
/// takes, and nothing is rendered on the way.
#[test]
fn the_operands_ahead_of_a_map_keep_their_types() {
    let plan = ok(vec![
        code(0, &json("xs: array<i64>; a: str; b: str;")),
        code(1, r#"r = a ?? b ?? xs.map(x => "f")[0];"#),
    ]);
    let docs = [
        r#"{"xs":[1],"a":"9007199254740993"}"#,
        r#"{"xs":[1],"b":"1.5"}"#,
        r#"{"xs":[1]}"#,
    ];
    assert_eq!(
        cols(&run(&plan, &docs), &["r"]),
        rows![["9007199254740993"], ["1.5"], ["f"]]
    );
    // A number beside the chain's text does not meet it: the result is a variant, each row keeping its own value.
    let plan = ok(vec![
        code(0, &json("xs: array<i64>; a: i64;")),
        code(1, r#"r = a ?? xs.map(x => "f")[0];"#),
    ]);
    let (_, r) = plan.tree.leaves().into_iter().find(|(n, _)| n == "r").unwrap();
    assert_eq!(plan.slot_layout[r], DataType::Variant);
    assert_eq!(
        cols(&run(&plan, &[r#"{"xs":[1],"a":9}"#, r#"{"xs":[1]}"#]), &["r"]),
        rows![[9], ["f"]]
    );
}

/// An operand ahead of a map runs on the rows the operands before it left open, as it would in the chain.
#[test]
fn an_operand_ahead_of_a_map_runs_where_the_chain_reaches_it() {
    for body in [
        "n = a ?? 100 / d ?? xs.map(x => 1.0)[0];",
        "b = a == null && 100 / d > 1 && xs.map(x => x)[0] > 0;",
        "b = a != null || 100 / d > 1 || xs.map(x => x)[0] > 0;",
    ] {
        let plan = ok(vec![code(0, &json("a: i64; d: i64; xs: array<i64>;")), code(1, body)]);
        let docs = [r#"{"a":1,"d":0,"xs":[1]}"#, r#"{"d":0,"xs":[1]}"#];
        let errors = run_scoped_on(&plan, &docs, None);
        assert_eq!(errors.error_rows(), vec![1], "{body}: {:?}", errors.entries());
    }
}

/// Two maps in one chain: the guard of the second is built from null tests, not from a coalesce of its own, and
/// each row takes the first operand that has a value.
#[test]
fn a_chain_with_two_maps_takes_the_first_value() {
    let plan = ok(vec![
        code(0, &json("xs: array<i64>; a: str;")),
        code(1, r#"r = a ?? xs.map(x => "first")[0] ?? xs.map(x => "second")[0];"#),
    ]);
    let docs = [r#"{"a":"7","xs":[1]}"#, r#"{"xs":[1]}"#, r#"{"xs":[]}"#];
    assert_eq!(cols(&run(&plan, &docs), &["r"]), rows![["7"], ["first"], [null]]);
    // Only the row whose array is empty reads past its end, once for each map it reaches.
    let errors = run_scoped_on(&plan, &docs, None);
    let outside: Vec<(u32, &ExceptionKind)> = errors.entries().iter().map(|a| (a.row, &a.kind)).collect();
    assert_eq!(
        outside,
        [
            (2, &ExceptionKind::IndexOutOfRange),
            (2, &ExceptionKind::IndexOutOfRange)
        ],
        "{:?}",
        errors.entries()
    );
}

/// A ternary over a variant keeps each row's value as the branch it took had it, and notes nothing.
#[test]
fn a_ternary_over_a_variant_keeps_each_rows_value() {
    let plan = ok(vec![
        code(0, &json("flag: bool; pairs: array<{key: str, value: i64}>;")),
        build(1, "m"),
        code(2, r#"r = flag ? m.n : "x";"#),
    ]);
    let docs = [
        pairs_doc(&[("n", json!(5))], json!({"flag": true})),
        r#"{"flag":true}"#.to_string(),
        pairs_doc(&[("n", json!(5))], json!({"flag": false})),
    ];
    let docs: Vec<&str> = docs.iter().map(String::as_str).collect();
    let out = run(&plan, &docs);
    assert_eq!(out[0]["r"], json!(5), "a number stays a number");
    assert!(out[1].get("r").is_none(), "{}", out[1]);
    assert_eq!(out[2]["r"], json!("x"));
    let errors = run_scoped_on(&plan, &docs, None);
    assert!(errors.entries().is_empty(), "{:?}", errors.entries());
}

/// A batch whose rows all take one side of an `if` or a `?:` does not compute the other side. Run alone, each row
/// takes that path; run together, the rows blend. Every row gets the same value and the same exceptions either way.
/// `n + 1` overflows on a row whose `n` is the largest i64, so an exception shows which side a row took.
#[test]
fn a_side_no_row_takes_gives_each_row_what_blending_gives() {
    let plan = ok(vec![
        code(0, &json("level: str; msg: str; n: i64;")),
        code(
            1,
            r#"out = ""; m = 0;
               if (level == "ERROR") { out = toUpperCase(msg); m = n + 1; } else { out = msg; }
               t = level == "ERROR" ? sha256(msg) : msg;
               u = level == "INFO" ? n + 1 : 0;"#,
        ),
    ]);
    let max = i64::MAX;
    let docs = [
        format!(r#"{{"level":"ERROR","msg":"disk full","n":{max}}}"#),
        format!(r#"{{"level":"INFO","msg":"ok","n":{max}}}"#),
        r#"{"level":"ERROR","msg":"late","n":1}"#.to_string(),
        r#"{"level":"INFO","msg":"fine","n":2}"#.to_string(),
    ];
    let docs: Vec<&str> = docs.iter().map(String::as_str).collect();
    let noted = |errors: &ErrorSink, row: u32| -> Vec<(u32, String)> {
        let mut v: Vec<(u32, String)> = errors
            .entries()
            .iter()
            .map(|a| (row + a.row, format!("{:?}", a.kind)))
            .collect();
        v.sort();
        v
    };

    let together = run(&plan, &docs);
    let together_noted = noted(&run_scoped_on(&plan, &docs, None), 0);
    let mut alone = Vec::new();
    let mut alone_noted = Vec::new();
    for (row, doc) in docs.iter().enumerate() {
        alone.extend(run(&plan, &[doc]));
        alone_noted.extend(noted(&run_scoped_on(&plan, &[doc], None), row as u32));
    }
    alone_noted.sort();
    assert_eq!(together, alone);
    assert_eq!(together_noted, alone_noted);
    assert_eq!(together[0]["out"], "DISK FULL");
    assert_eq!(together[1]["out"], "ok");
    assert_eq!(together[2]["m"], 2);
    assert_eq!(together[3]["u"], 3);
    let overflowed: Vec<u32> = together_noted
        .iter()
        .filter(|(_, k)| k == "Overflow")
        .map(|(r, _)| *r)
        .collect();
    assert_eq!(
        overflowed,
        vec![0, 1],
        "row 0 took `n + 1` in the `if`, row 1 in `u`'s `?:`"
    );
}

/// What a guarded block computes ahead of its blends -- a loop's array, a parser's input, a parse -- is not computed on a
/// batch the guard takes no row of. Run alone, each row takes that path; run together, the rows compute it. Every row
/// gets the same value and the same exceptions either way.
#[test]
fn a_guarded_blocks_lifted_work_gives_each_row_what_blending_gives() {
    let plan = ok(vec![
        code(0, &json("level: str; msg: str;")),
        code(
            1,
            r#"words = 0; user = "none";
               if (level == "ERROR") {
                 for (let w of split(toUpperCase(msg), " ")) { words = words + 1; }
                 let kvs = kv(replaceAll(msg, /;/, " "), {pairSep: " ", kvSep: "="});
                 user = (kvs.user ?? "unknown") as str;
               }"#,
        ),
    ]);
    let docs = [
        r#"{"level":"ERROR","msg":"user=ann;code=7 disk full"}"#,
        r#"{"level":"INFO","msg":"user=bob all fine"}"#,
        r#"{"level":"ERROR","msg":"late"}"#,
    ];
    let noted = |errors: &ErrorSink, row: u32| -> Vec<(u32, String)> {
        let mut v: Vec<(u32, String)> = errors
            .entries()
            .iter()
            .map(|a| (row + a.row, format!("{:?}", a.kind)))
            .collect();
        v.sort();
        v
    };
    let together = run(&plan, &docs);
    let together_noted = noted(&run_scoped_on(&plan, &docs, None), 0);
    let mut alone = Vec::new();
    let mut alone_noted = Vec::new();
    for (row, doc) in docs.iter().enumerate() {
        alone.extend(run(&plan, &[doc]));
        alone_noted.extend(noted(&run_scoped_on(&plan, &[doc], None), row as u32));
    }
    alone_noted.sort();
    assert_eq!(together, alone);
    assert_eq!(together_noted, alone_noted);
    assert_eq!(
        cols(&together, &["words", "user"]),
        rows![[3, "ann"], [0, "none"], [1, "unknown"]]
    );
}

/// A guarded parse that merges into the record keeps the record's undeclared keys on a row the guard does not take,
/// whether or not any row of the batch takes it.
#[test]
fn a_guarded_merge_keeps_the_records_keys_on_a_batch_no_row_takes() {
    let plan = ok(vec![
        code(0, &json("level: str; msg: str;")),
        code(1, r#"if (level == "ERROR") { Object.assign($, json(msg)); }"#),
    ]);
    let info = r#"{"level":"INFO","msg":"{}","host":"web-1"}"#;
    let error = r#"{"level":"ERROR","msg":"{\"code\":7}","host":"web-2"}"#;
    assert_eq!(run(&plan, &[info])[0]["host"], "web-1", "a batch no row takes");
    let both = run(&plan, &[info, error]);
    assert_eq!(both[0]["host"], "web-1");
    assert_eq!(both[1]["host"], "web-2");
    assert_eq!(both[1]["code"], 7);
}

/// The operands before a `map` are computed once: a divide among them is one exception, not one per reader.
#[test]
fn a_prefix_ahead_of_a_map_is_computed_once() {
    for body in [
        "b = 100 / d > 1 && xs.map(x => x)[0] > 0;",
        "b = 100 / d > 1 || xs.map(x => x)[0] > 0;",
        "n = 100 / d ?? xs.map(x => 1.0)[0];",
    ] {
        let plan = ok(vec![code(0, &json("d: i64; xs: array<i64>;")), code(1, body)]);
        let errors = run_scoped_on(&plan, &[r#"{"d":0,"xs":[1]}"#], None);
        let divides = errors
            .entries()
            .iter()
            .filter(|a| matches!(a.kind, ExceptionKind::DivideByZero))
            .count();
        assert_eq!(divides, 1, "{body}: {:?}", errors.entries());
    }
}

/// A fold is the general loop run at once, and an accumulator keeps its seed's type in both: an overwrite of another
/// type is a compile error, `as` converts it, and a value `as` refuses is null on its row.
#[test]
fn a_fold_keeps_its_seeds_type_as_the_loop_does() {
    for body in [
        "let total = 0; for (let x of xs) { total = x as i64; } r = total;",
        "k: str = null; let total = 0; for (let x of xs) { total = x as i64; k = x; } r = total;",
    ] {
        let plan = ok(vec![code(0, &json("xs: array<str>;")), code(1, body)]);
        let rows = run(&plan, &[r#"{"xs":["1","2"]}"#, r#"{"xs":["a"]}"#]);
        assert_eq!(cols(&rows, &["r"]), rows![[2], [null]], "{body}");
    }
    for body in [
        "let total = 0; for (let x of xs) { total = x; } r = total;",
        "k: str = null; let total = 0; for (let x of xs) { total = x; k = x; } r = total;",
    ] {
        let e = err(vec![code(0, &json("xs: array<str>;")), code(1, body)]);
        assert!(
            e.contains("`x` is str, not i64; to convert the value, write `x as i64`"),
            "{body}\n  got: {e}"
        );
    }

    // A value the checker left untyped is compatible with any seed, as it is in the general loop.
    let plan = ok(vec![
        code(0, &json("xs: array<i64>;")),
        code(1, "let total = 0.0; for (let x of xs) { total = null; } r = total;"),
    ]);
    assert_eq!(
        cols(&run(&plan, &[r#"{"xs":[4]}"#, r#"{"xs":[]}"#]), &["r"]),
        rows![[null], [0.0]]
    );
}

/// An `f64` accumulator widens each `i64` element to `f64`, in a fold as in the general loop: the compiler coerces the
/// element, and the accumulator is `f64` on every row.
#[test]
fn an_f64_accumulator_widens_its_i64_elements() {
    for body in [
        "let total = 0.0; for (let x of xs) { total += x; } r = total;",
        "k: i64 = null; let total = 0.0; for (let x of xs) { total += x; k = x; } r = total;",
    ] {
        let plan = ok(vec![code(0, &json("xs: array<i64>;")), code(1, body)]);
        let r = plan.tree.leaf("r").unwrap();
        assert_eq!(plan.provenance[r].ty, DataType::F64, "{body}");
        assert_eq!(
            cols(&run(&plan, &[r#"{"xs":[1,2]}"#, r#"{"xs":[]}"#]), &["r"]),
            rows![[3.0], [0.0]],
            "{body}"
        );
    }
}

/// An `i64` accumulator refuses an `f64` value, in a fold as in the general loop: the fix is an `f64` seed.
#[test]
fn an_i64_accumulator_refuses_an_f64_value() {
    for body in [
        "let total = 0; for (let x of xs) { total += 100 / x; } r = total;",
        "let total = 0; for (let x of xs) { total += 100 / x; k = x; } r = total;",
    ] {
        let e = err(vec![code(0, &json("xs: array<i64>;")), code(1, body)]);
        assert!(
            e.contains("`total`") && e.contains("f64") && e.contains("i64"),
            "{body}\n  got: {e}"
        );
    }
}

/// What the fold has no kernel for is the general loop's: a date plus a span folds there, not in a panic.
#[test]
fn a_fold_without_a_kernel_is_the_general_loops() {
    let plan = ok(vec![
        code(0, &json("xs: array<i64>;")),
        code(
            1,
            r#"let t = now(); for (let x of xs) { t += duration(x, "s"); } r = t;"#,
        ),
    ]);
    let rows = run(&plan, &[r#"{"xs":[1,2]}"#]);
    assert!(rows[0]["r"].is_string(), "{}", rows[0]);
}

/// An array absent on every row is an unbound map: every row keeps its seed, whatever the seed's type.
#[test]
fn a_fold_over_an_absent_array_keeps_the_seed() {
    let plan = ok(vec![
        code(0, &json("xs: array<f64>;")),
        code(1, "let total = 0.0; for (let x of xs) { total += x; } r = total;"),
    ]);
    assert_eq!(
        cols(&run(&plan, &["{}", r#"{"xs":null}"#, r#"{"xs":[]}"#]), &["r"]),
        rows![[0.0], [0.0], [0.0]]
    );
}

/// A map with no value in any element is unbound, as an absent array is. A row with an element folds to null, as
/// a null element does; a row without keeps its seed; a null seed stays null.
#[test]
fn a_fold_over_an_all_null_map_is_null_where_there_are_elements() {
    let plan = ok(vec![
        code(0, &json("xs: array<i64>;")),
        code(1, "let total = 1; for (let x of xs) { total += x; } r = total;"),
    ]);
    assert_eq!(
        cols(&run(&plan, &[r#"{"xs":[null]}"#, r#"{"xs":[]}"#]), &["r"]),
        rows![[null], [1]]
    );
    let plan = ok(vec![
        code(0, &json("xs: array<i64>;")),
        code(1, "let total: i64 = null; for (let x of xs) { total += x; } r = total;"),
    ]);
    assert_eq!(cols(&run(&plan, &[r#"{"xs":[1,2]}"#]), &["r"]), rows![[null]]);
}

/// A guarded fold visits the rows the loop would. An overflow among a masked row's elements is never computed, so
/// it cannot promote the column, and the masked row keeps its integer seed exact.
#[test]
fn a_guarded_fold_never_computes_a_masked_rows_elements() {
    let plan = ok(vec![
        code(0, &json("flag: bool; n: i64; xs: array<i64>;")),
        code(
            1,
            "let total = n; if (flag) { for (let x of xs) { total += x + x; } } r = total;",
        ),
    ]);
    let docs = [
        r#"{"flag":false,"n":9007199254740993,"xs":[9223372036854775807]}"#,
        r#"{"flag":true,"n":1,"xs":[2]}"#,
    ];
    assert_eq!(cols(&run(&plan, &docs), &["r"]), rows![[9007199254740993_i64], [5]]);
}

/// A fold that assigns a variant to its accumulator keeps the accumulator's type: `as` converts the variant on each
/// row, and a plain write of it is a compile error naming `as`.
#[test]
fn a_folds_variant_write_needs_as() {
    let plan = ok(vec![
        code(0, &json("xs: array<i64>; pairs: array<{key: str, value: str}>;")),
        build(1, "m"),
        code(
            2,
            r#"let total = 0.0; for (let x of xs) { total = m.host as f64; } r = total + 1;"#,
        ),
    ]);
    let doc = pairs_doc(&[("host", json!("1.5"))], json!({"xs": [1]}));
    assert_eq!(cols(&run(&plan, &[&doc]), &["r"]), rows![[2.5]]);
    let plan = ok(vec![
        code(0, &json("xs: array<i64>; host: str;")),
        code(
            1,
            r#"let total = ""; for (let x of xs) { total = host; } r = total + "!";"#,
        ),
    ]);
    assert_eq!(cols(&run(&plan, &[r#"{"xs":[1],"host":"h"}"#]), &["r"]), rows![["h!"]]);
    let e = err(vec![
        code(0, &json("xs: array<i64>; pairs: array<{key: str, value: str}>;")),
        build(1, "m"),
        code(
            2,
            r#"let total = 0.0; for (let x of xs) { total = m.host; } r = total + 1;"#,
        ),
    ]);
    assert!(e.contains("`m.host as f64`"), "{e}");
}

/// An `i64` fold that overflows is null on that row, marked `Overflow`, and `i64` on the rows beside it, whether
/// the overflow is a trip's or the seed's own. The failed rows are the marked rows and no others.
#[test]
fn a_folds_overflow_is_null_on_its_row_and_i64_beside_it() {
    let plan = ok(vec![
        code(0, &json("n: i64; xs: array<i64>;")),
        code(
            1,
            "let total = n; for (let x of xs) { total += x; } r = total;
             let s = n + n; for (let x of xs) { s += x; } seeded = s;",
        ),
    ]);
    let ty = |name: &str| plan.provenance[plan.tree.leaf(name).unwrap()].ty.clone();
    assert_eq!(ty("r"), DataType::I64);
    assert_eq!(ty("seeded"), DataType::I64);
    let docs = [
        r#"{"n":9223372036854775807,"xs":[1]}"#,
        r#"{"n":2,"xs":[3,4]}"#,
        r#"{"n":4611686018427387904,"xs":[0]}"#,
    ];
    assert_eq!(
        cols(&run(&plan, &docs), &["r", "seeded"]),
        rows![[null, null], [9, 11], [4611686018427387904_i64, null]]
    );
    let errors = run_scoped_on(&plan, &docs, None);
    assert!(
        errors
            .entries()
            .iter()
            .all(|a| matches!(a.kind, ExceptionKind::Overflow)),
        "{:?}",
        errors.entries()
    );
    assert_eq!(errors.error_rows(), vec![0, 2], "{:?}", errors.entries());
}

/// What a loop body marks lands on the row of the batch it belongs to, through any depth of loops. Row 0 runs no
/// trips; row 1's divides by zero.
#[test]
fn a_loop_bodys_exceptions_land_on_their_own_rows() {
    let plan = ok(vec![
        code(0, &json("n: array<i64>; d: i64; pairs: array<{key: str, value: str}>;")),
        build(1, "o"),
        code(
            2,
            "a: variant = null; b: i64 = null; c: f64 = null; for (let x of n) { a = o.k; for (let y of n) { a = o.a; b = 1; } c = x > 0 ? 1 / d : 0; }",
        ),
    ]);
    let o = [("a", json!("s")), ("k", json!("1"))];
    let docs = [
        pairs_doc(&o, json!({"n": [], "d": 0})),
        pairs_doc(&o, json!({"n": [1], "d": 0})),
    ];
    let docs: Vec<&str> = docs.iter().map(String::as_str).collect();
    let mut pool = BufferPool::new();
    let mut errors = ErrorSink::new();
    let mut batch = raw_batch(&plan, &docs, &mut pool);
    let mut ctx = Ctx::new(&mut pool, &mut errors);
    let rows = run_to_json_marked(&plan, &Default::default(), &mut batch, &mut ctx, false, None);
    batch.reclaim(&mut pool);
    assert!(rows[0].exceptions.is_empty(), "{:?}", rows[0].exceptions);
    assert!(!rows[1].exceptions.is_empty());

    // An element's exception lands on the row whose array holds it, in a loop body or not.
    for body in [
        "r = z.map(v => 1 / v);",
        "r: f64 = null; k: i64 = null; for (let x of n) { r = z.map(v => 1 / v)[0]; k = 1; }",
    ] {
        let plan = ok(vec![code(0, &json("n: array<i64>; z: array<i64>;")), code(1, body)]);
        let docs = [r#"{"n":[],"z":[1]}"#, r#"{"n":[1],"z":[0,0]}"#];
        let mut pool = BufferPool::new();
        let mut errors = ErrorSink::new();
        let mut batch = raw_batch(&plan, &docs, &mut pool);
        let mut ctx = Ctx::new(&mut pool, &mut errors);
        let rows = run_to_json_marked(&plan, &Default::default(), &mut batch, &mut ctx, false, None);
        batch.reclaim(&mut pool);
        assert!(rows[0].exceptions.is_empty(), "{body}: {:?}", rows[0].exceptions);
        assert!(!rows[1].exceptions.is_empty(), "{body}");
    }
}

// -- assignment replaces, spread and Object.assign ------------------------------

/// A row without `_raw`, which every row carries.
fn fields(row: &Value) -> Value {
    let mut row = row.clone();
    row.as_object_mut().unwrap().remove("_raw");
    row
}

/// An assignment writes everything under its target: a field the value lacks is null, a key an open object has no
/// field for joins its unclaimed fields, and the record's keys under the name go.
#[test]
fn an_assignment_replaces_everything_under_its_target() {
    let plan = ok(vec![
        code(0, JSON),
        code(
            1,
            "r: {z: i64, ...} = {z: 5}; r = {k: 2}; o = {a: 1, b: 2}; o = {b: o.a}; rec = 7;",
        ),
    ]);
    let rows = run(&plan, &[r#"{"rec":{"x":1,"y":2}}"#]);
    assert_eq!(fields(&rows[0]), json!({"r": {"k": 2}, "o": {"b": 1}, "rec": 7}));
}

/// A path under a scalar is a compile error: read, the type has no such path; written, nothing can be written under
/// it.
#[test]
fn a_path_under_a_scalar_is_a_compile_error() {
    let e = err(vec![code(0, JSON), code(1, "rec = 7; gone = rec.x;")]);
    assert!(e.contains("`rec` is i64 and has no fields"), "{e}");
    let e = err(vec![code(0, JSON), code(1, "rec = 7; rec.x = 1;")]);
    assert!(e.contains("function 1: `rec` is i64 and has no fields"), "{e}");
}

/// Under a guard, what an assignment replaces is null only on the rows it ran, and the parser's declared paths
/// likewise.
#[test]
fn a_guarded_assignment_replaces_only_on_its_rows() {
    let plan = ok(vec![
        code(0, &json("o: {x: i64, ...}; flag: bool;")),
        code(1, "o.k = 1; if (flag) { o = {n: 2}; }"),
    ]);
    let rows = run(
        &plan,
        &[r#"{"flag":true,"o":{"x":1}}"#, r#"{"flag":false,"o":{"x":1}}"#],
    );
    assert_eq!(fields(&rows[0]), json!({"flag": true, "o": {"n": 2}}));
    assert_eq!(fields(&rows[1]), json!({"flag": false, "o": {"k": 1, "x": 1}}));
}

/// A loop body's assignment replaces on the rows it ran for, and keeps the old value where the loop ran no trips.
#[test]
fn a_loop_bodys_assignment_replaces_what_it_writes_over() {
    let plan = ok(vec![
        nums(),
        code(1, "o = {a: 1}; for (let n of nums) { o = {a: n + 1}; }"),
    ]);
    let rows = run(&plan, &[r#"{"nums":[1,2]}"#, r#"{"nums":[]}"#]);
    assert_eq!(rows[0]["o"], json!({"a": 3}));
    assert_eq!(rows[1]["o"], json!({"a": 1}));
}

/// A rename of an open declared object moves the record's keys the declaration does not name with it, over whatever
/// the record held at the new name, and keeps them whether the new name sits under the old one or above it.
#[test]
fn a_rename_moves_the_records_unnamed_keys() {
    let plan = ok(vec![code(0, &json("a: {x: i64, ...};")), code(1, "move(a, b);")]);
    let rows = run(&plan, &[r#"{"a":{"x":1,"y":{"z":2}},"b":{"w":3}}"#]);
    assert_eq!(fields(&rows[0]), json!({"b": {"x": 1, "y": {"z": 2}}}));
}

#[test]
fn a_rename_into_or_out_of_its_own_path_keeps_the_records_keys() {
    // An open object type that names no key: every key under it is the record's.
    let down = ok(vec![code(0, &json("a: {...};")), code(1, "move(a, a.b);")]);
    let rows = run(&down, &[r#"{"a":{"b":{"x":1},"y":2}}"#]);
    assert_eq!(fields(&rows[0]), json!({"a": {"b": {"b": {"x": 1}, "y": 2}}}));
    let up = ok(vec![code(0, &json("a: {b: {...}, ...};")), code(1, "move(a.b, a);")]);
    let rows = run(&up, &[r#"{"a":{"b":{"x":1},"y":2}}"#]);
    assert_eq!(fields(&rows[0]), json!({"a": {"x": 1}}));
    // A leaf moves under its own name too: the leaf leaves the name, so nothing stands above the path.
    let leaf = ok(vec![code(0, &json("host: str;")), code(1, "move(host, host.ip);")]);
    let rows = run(&leaf, &[r#"{"host":"1.2.3.4","z":1}"#]);
    assert_eq!(fields(&rows[0]), json!({"host": {"ip": "1.2.3.4"}, "z": 1}));
}

/// A copy of an open declared object carries the record's keys the declaration does not name, and keeps them where
/// they were.
#[test]
fn a_copy_carries_the_records_unnamed_keys() {
    let plan = ok(vec![
        code(0, &json("flag: bool; o: {x: i64, ...};")),
        code(1, "q = o; g: {x: i64, ...} = null; if (flag) { g = o; }"),
    ]);
    let rows = run(
        &plan,
        &[
            r#"{"flag":true,"o":{"x":1,"y":5}}"#,
            r#"{"flag":false,"o":{"x":2,"y":6}}"#,
        ],
    );
    assert_eq!(
        fields(&rows[0]),
        json!({"flag": true, "o": {"x": 1, "y": 5}, "q": {"x": 1, "y": 5}, "g": {"x": 1, "y": 5}})
    );
    assert_eq!(
        fields(&rows[1]),
        json!({"flag": false, "o": {"x": 2, "y": 6}, "q": {"x": 2, "y": 6}})
    );
}

/// `{...p, k: v}`: `p`'s keys, then `k` over them. A spread takes an object: a spread of a scalar is a compile error.
#[test]
fn a_spread_lays_down_its_sources_keys_and_a_later_key_replaces_one() {
    let plan = ok(vec![code(
        0,
        "p = {a: 1, k: {x: 1, y: 2}}; t = {...p, k: 3}; u = {...p, ...{b: 4}};",
    )]);
    let rows = run(&plan, &["{}"]);
    assert_eq!(rows[0]["t"], json!({"a": 1, "k": 3}));
    assert_eq!(rows[0]["u"], json!({"a": 1, "k": {"x": 1, "y": 2}, "b": 4}));
    let e = err(vec![code(0, "s = 5; v = {...s, c: 6};")]);
    assert!(e.contains("a spread takes an object, not i64"), "{e}");
}

/// A spread lays an object over an object leaf by leaf, each only on the rows its source has it, and the source's
/// unclaimed fields a key at a time.
#[test]
fn a_spread_lays_an_object_leaf_by_leaf_where_its_source_has_it() {
    let plan = ok(vec![
        code(0, &json("p: {k: {y: i64, ...}, m: i64};")),
        code(1, "d = {k: {x: 1}, m: 2}; t = {...d, ...p};"),
        code(2, "tky = t.k.y;"),
    ]);
    let rows = run(
        &plan,
        &[r#"{"p":{"k":{"y":5}}}"#, r#"{"p":{"m":9}}"#, r#"{"p":{"k":{"z":7}}}"#],
    );
    assert_eq!(rows[0]["t"], json!({"k": {"x": 1, "y": 5}, "m": 2}));
    assert_eq!(rows[1]["t"], json!({"k": {"x": 1}, "m": 9}));
    // `p.k.z` is a key the declaration does not name: it joins `k`'s keys on its row.
    assert_eq!(rows[2]["t"], json!({"k": {"x": 1, "z": 7}, "m": 2}));
}

/// A spread of a declared struct value lays down its fields, and of an open one the record's keys the declaration does
/// not name too.
#[test]
fn a_spread_of_a_struct_value_lays_down_its_fields() {
    let plan = ok(vec![
        code(0, &json("v: {a: i64, b: i64}; rec: {a: i64, ...};")),
        code(1, "t = {...v}; u = {b: 0, ...v, c: 3}; w = {...rec, a: 9};"),
    ]);
    let rows = run(&plan, &[r#"{"v":{"a":1,"b":2},"rec":{"a":1,"z":{"q":true}}}"#]);
    assert_eq!(rows[0]["t"], json!({"a": 1, "b": 2}));
    assert_eq!(rows[0]["u"], json!({"a": 1, "b": 2, "c": 3}));
    assert_eq!(rows[0]["w"], json!({"a": 9, "z": {"q": true}}));
}

/// A copy of an open declared object is the record's whole value under it, every key it does not name included, and a
/// key it names is read from the value.
#[test]
fn a_whole_read_takes_every_key_the_record_holds() {
    let plan = ok(vec![
        code(0, &json("o: {k299: i64, ...};")),
        code(1, "q = o; last = o.k299;"),
    ]);
    let keys: Vec<String> = (0..300).map(|i| format!(r#""k{i}":{i}"#)).collect();
    let doc = format!(r#"{{"o":{{{}}}}}"#, keys.join(","));
    let rows = run(&plan, &[&doc]);
    assert_eq!(rows[0]["last"], 299);
    assert_eq!(rows[0]["q"]["k0"], 0);
    assert_eq!(rows[0]["q"]["k299"], 299);
    assert_eq!(rows[0]["o"], rows[0]["q"]);
}

/// A name a step replaces on every row is discarded by every parser before the step.
#[test]
fn a_replacement_reaches_every_parser_before_it() {
    let doc = r#"{"o":{"x":1},"inner":"{\"o\":{\"y\":2}}"}"#;
    let plan = ok(vec![
        code(0, &json("inner: str;")),
        code(1, "Object.assign($, json(inner));"),
        code(2, "o = 5;"),
    ]);
    let rows = run(&plan, &[doc]);
    assert_eq!(rows[0]["o"], 5);
}

/// A declared path keeps the last of its duplicate keys, as any field does, and the record's other keys under its head
/// stay in the output.
#[test]
fn a_declared_path_keeps_the_last_duplicate() {
    let plan = ok(vec![code(0, &json("o: {a: i64, ...};"))]);
    let rows = run(&plan, &[r#"{"o":{"a":1,"a":2,"z":3}}"#]);
    assert_eq!(rows[0]["o"], json!({"a": 2, "z": 3}));
}

/// A path written under a record key makes the name an object: where the record's value there is not one, the path
/// replaces it, and where it is one, its other keys stay. A whole read of the name is its leaves and those keys. A
/// path written under a step's scalar is a compile error.
#[test]
fn a_path_written_under_a_records_scalar_replaces_it() {
    let e = err(vec![code(0, &json("flag: bool;")), code(1, "s = 5; s.b = 1;")]);
    assert!(e.contains("function 1: `s` is i64 and has no fields"), "{e}");

    let plan = ok(vec![code(0, &json("flag: bool;")), code(1, "o.a = 9; whole = o;")]);
    // One record per batch: a scalar and an object under one name across rows would render as text.
    let rows = run(&plan, &[r#"{"flag":true,"o":5}"#]);
    assert_eq!(
        fields(&rows[0]),
        json!({"flag": true, "o": {"a": 9}, "whole": {"a": 9}})
    );
    let rows = run(&plan, &[r#"{"flag":false,"o":{"z":1}}"#]);
    assert_eq!(
        fields(&rows[0]),
        json!({"flag": false, "o": {"a": 9, "z": 1}, "whole": {"a": 9, "z": 1}})
    );
}

/// Under `splitDots`, a flat key written after an object with a duplicate key replaces the last duplicate, so the
/// flat key wins.
#[test]
fn a_flat_key_wins_over_the_duplicates_before_it() {
    let plan = ok(vec![code(0, &json_split("o: {a: i64, ...};")), code(1, "q = o;")]);
    let rows = run(&plan, &[r#"{"o":{"a":1,"a":2},"o.a":3}"#]);
    assert_eq!(rows[0]["q"], json!({"a": 3}));
}

/// A spread of a declared struct lays down a field whose name holds a dot as one key, and the output writes it as it
/// is named.
#[test]
fn a_spread_of_a_struct_with_a_dotted_field_name_lays_it_down_as_one_key() {
    let plan = ok(vec![
        code(0, &json(r#"v: {"a.b": i64, c: i64};"#)),
        code(1, "t = {...v, d: 4};"),
    ]);
    let rows = run(&plan, &[r#"{"v":{"a.b":1,"c":2}}"#]);
    assert_eq!(rows[0]["t"], json!({"a.b": 1, "c": 2, "d": 4}));
}

/// `!` is Bool: written over the declared object `q`, guarded or not, it is a compile error that names the typed
/// assignment, since an object keeps its type.
#[test]
fn a_negation_is_bool_and_does_not_replace_an_object() {
    let declared = || code(0, &json("q: {a: i64, ...}; flag: bool; p: bool;"));
    for body in ["q = !p;", "if (flag) { q = !p; }"] {
        let e = err(vec![declared(), code(1, body)]);
        assert!(
            e.contains("is bool, not {a: i64, ...}; to change the type of `q`, write `q: bool = "),
            "{body}: {e}"
        );
    }
}

/// A path bound before a parser and the record's object at the name come out as one object: the parse is the last
/// write, so the record's key wins where it has one, a path the record lacks keeps its value, and the rest of the
/// record's keys under the name come through in the output.
#[test]
fn a_path_bound_before_a_parser_lies_over_the_records_object() {
    let plan = ok(vec![code(0, "o.a = 1;"), code(1, JSON)]);
    let rows = run(&plan, &[r#"{"o":{"a":2,"b":3}}"#, r#"{"o":{"b":3}}"#]);
    assert_eq!(rows[0]["o"], json!({"a": 2, "b": 3}));
    assert_eq!(rows[1]["o"], json!({"a": 1, "b": 3}));
}

/// A key after a spread replaces the struct's field of its own name whole, in a value as in the output. A field whose
/// name holds a dot is another key, and stays.
#[test]
fn a_key_after_a_spread_replaces_only_the_field_of_its_name() {
    let plan = ok(vec![
        code(0, &json(r#"v: {a: {b: i64}, "a.b": i64, c: i64};"#)),
        code(1, "t = {...v, a: 4}; u = {k: {...v, a: 4}};"),
    ]);
    let rows = run(&plan, &[r#"{"v":{"a":{"b":3},"a.b":1,"c":2}}"#]);
    assert_eq!(rows[0]["t"], json!({"a": 4, "a.b": 1, "c": 2}));
    assert_eq!(rows[0]["u"], json!({"k": {"a": 4, "a.b": 1, "c": 2}}));
}

/// A field a declared name took out of an object is out of the object: with the name nulled, the object beneath it
/// shows, without the field. Each row's value is its own: where the record's value under the declared object is no
/// object, the object is null on that row.
#[test]
fn a_field_taken_out_of_an_object_is_out_of_it() {
    let plan = ok(vec![code(0, &json("o: {a: i64, ...};")), code(1, "q = o; o.a = null;")]);
    let rows = run(&plan, &[r#"{"o":{"a":1,"b":2}}"#, r#"{"o":5}"#]);
    assert_eq!(rows[0]["o"], json!({"b": 2}));
    assert_eq!(rows[1]["o"], Value::Null);
}

/// A key holding a dot, in brackets on a dynamic value, is one key, as it is on an object.
#[test]
fn a_dotted_key_in_brackets_on_a_dynamic_value_is_one_key() {
    let plan = ok(vec![
        pairs(),
        build(1, "p"),
        code(2, "q = p; out = q[\"a.b\"]; a = q[\"a\"]; one = q[\"c\"];"),
    ]);
    let doc = pairs_doc(&[("a", json!("x")), ("a.b", json!("7")), ("c", json!("1"))], json!({}));
    let rows = run(&plan, &[&doc]);
    assert_eq!(cols(&rows, &["out", "a", "one"]), rows![["7", "x", "1"]]);
}

/// A string key in brackets on a name is that name's key, as `$["k"]` is: it is declared, written, read, deleted and
/// moved as the one name.
#[test]
fn a_string_key_on_a_name_is_one_name_everywhere() {
    let doc = r#"{"labels": {"app.kubernetes.io/name": "web", "t": "x"}}"#;
    let declared = json(r#"labels["app.kubernetes.io/name"]: str;"#);
    let plan = ok(vec![
        code(0, &declared),
        code(1, r#"out = labels["app.kubernetes.io/name"];"#),
    ]);
    assert_eq!(run(&plan, &[doc])[0]["out"], json!("web"));

    let plan = ok(vec![code(0, r#"o["a"]["b.c"]: i64 = 5;"#)]);
    assert_eq!(run(&plan, &["{}"])[0]["o"], json!({"a": {"b.c": 5}}));
    let plan = ok(vec![code(0, &json(r#"n["x.y"]: i64;"#))]);
    assert_eq!(run(&plan, &[r#"{"n": {"x.y": "7"}}"#])[0]["n"], json!({"x.y": 7}));

    let nested = r#"{"o": {"a": {"c": 2}, "a.b": 1}}"#;
    let plan = ok(vec![code(0, JSON), code(1, r#"o["a"]["b"] = 1;"#)]);
    assert_eq!(run(&plan, &[nested])[0]["o"]["a"], json!({"b": 1, "c": 2}));
    let plan = ok(vec![code(0, JSON), code(1, r#"delete(o["a.b"]);"#)]);
    assert_eq!(run(&plan, &[nested])[0]["o"], json!({"a": {"c": 2}}));
    let plan = ok(vec![code(0, JSON), code(1, r#"move(o["a.b"], z);"#)]);
    let row = &run(&plan, &[nested])[0];
    assert_eq!((&row["o"], &row["z"]), (&json!({"a": {"c": 2}}), &json!(1)));

    let plan = ok(vec![code(0, r#"($)["a"] = 1; b = at($, "a"); c = ($)["a"];"#)]);
    assert_eq!(cols(&run(&plan, &["{}"]), &["a", "b", "c"]), rows![[1, 1, 1]]);

    assert!(err(vec![code(0, r#"labels["k"]: str; labels["k"]: i64;"#)]).contains("is already str"));
    assert!(err(vec![code(0, "tags[0]: i64;")]).contains("parse error"));
}

/// A key holding a dot, in brackets on an object value, is one key, as it is on a named object; member access is
/// the path.
#[test]
fn a_dotted_key_in_brackets_on_an_object_value_is_one_key() {
    let plan = ok(vec![
        code(0, &json(r#"a: array<{x: {y: i64}, "x.y": i64}>;"#)),
        code(1, "out = a[0][\"x.y\"]; path = a[0].x.y;"),
    ]);
    let rows = run(&plan, &[r#"{"a": [{"x": {"y": 7}, "x.y": 8}]}"#]);
    assert_eq!(cols(&rows, &["out", "path"]), rows![[8, 7]]);
}

/// A spread lays each of its fields over the target's field of that name. A scalar field laid over an object field
/// does not meet it, so it is a compile error. A field whose name holds a dot is another key, and the spread lays its
/// field beside it.
#[test]
fn a_spread_field_over_an_object_field_is_a_compile_error() {
    let v = || code(0, &json("v: {a: i64};"));
    let e = err(vec![v(), code(1, "u = {k: {a: {b: 1}, ...v}};")]);
    assert!(
        e.contains("function 1: a spread lays `a` as i64 over an object, and the two do not meet"),
        "{e}"
    );
    let plan = ok(vec![v(), code(1, "u = {k: {\"a.b\": 1, ...v}};")]);
    assert_eq!(
        run(&plan, &[r#"{"v":{"a":5}}"#])[0]["u"],
        json!({"k": {"a": 5, "a.b": 1}})
    );
}

/// A literal's quoted key holding a dot is one key: beside a key a spread laid as a value it is another key, and a
/// `null` under the head's name writes nothing beside it.
#[test]
fn a_dotted_key_beside_a_spreads_value_in_a_literal_is_one_key() {
    let v = || code(0, &json("v: {k: i64};"));
    let plan = ok(vec![v(), code(1, "u = {...v, \"k.c\": 2};")]);
    assert_eq!(run(&plan, &[r#"{"v":{"k":1}}"#])[0]["u"], json!({"k": 1, "k.c": 2}));
    let plan = ok(vec![v(), code(1, "u = {k: null, \"k.c\": 2};")]);
    assert_eq!(run(&plan, &["{}"])[0]["u"], json!({"k.c": 2}));
}

/// A flat key with any number of dots, under the head of a declared path, nests only to the serializer's bound.
#[test]
fn a_deep_dotted_key_under_a_declared_head_nests_to_the_bound() {
    let plan = ok(vec![code(0, &json("o: {x: i64, ...};"))]);
    let key = "a.".repeat(20000) + "a";
    let doc = format!(r#"{{"o":{{"x":1}},"o.{key}":2}}"#);
    let rows = run(&plan, &[&doc]);
    assert_eq!(rows[0]["o"]["x"], 1);
    let depth = rows[0]["o"].to_string().matches('{').count();
    assert!(depth <= 64, "{depth}");
}

/// A spread's object is an element like any object. A spread of a variant is an object too, whose unclaimed fields
/// are the variant's keys.
#[test]
fn an_array_element_that_is_a_merge_has_the_merges_shape() {
    let plan = ok(vec![
        code(0, &json("v: {a: i64}; flag: bool;")),
        code(1, "out = at([{...v}], 0); maybe = at([flag ? {...v} : null], 0);"),
    ]);
    let rows = run(
        &plan,
        &[r#"{"v":{"a":1},"flag":true}"#, r#"{"v":{"a":2},"flag":false}"#],
    );
    assert_eq!(rows[0]["out"], json!({"a": 1}));
    assert_eq!(rows[0]["maybe"], json!({"a": 1}));
    assert!(rows[1].get("maybe").is_none(), "{}", rows[1]);
    let plan = ok(vec![pairs(), build(1, "w"), code(2, "out = at([{...w, n: 1}], 0);")]);
    let rows = run(&plan, &[&pairs_doc(&[("a", json!("x"))], json!({}))]);
    assert_eq!(rows[0]["out"], json!({"a": "x", "n": 1}));
}

/// A record's object of many keys, with a path a step writes under it, writes every key.
#[test]
fn a_whole_object_of_many_keys_beside_a_bound_path_writes_every_key() {
    let plan = ok(vec![code(0, JSON), code(1, "o.x = 1;")]);
    let body: Vec<String> = (0..5000).map(|i| format!(r#""k{i}":{i}"#)).collect();
    let doc = format!(r#"{{"o":{{{}}}}}"#, body.join(","));
    let rows = run(&plan, &[&doc]);
    assert_eq!(rows[0]["o"]["x"], 1);
    assert_eq!(rows[0]["o"]["k0"], 0);
    assert_eq!(rows[0]["o"]["k4999"], 4999);
    assert_eq!(rows[0]["o"].as_object().unwrap().len(), 5001);
}

/// A path declared under a key lies over that key's own object: the record's other fields under the key survive in the
/// output.
#[test]
fn a_declared_path_lies_over_its_keys_own_object() {
    let plan = ok(vec![code(0, &json("o: {a: {b: i64, ...}, ...};"))]);
    let rows = run(&plan, &[r#"{"o":{"a":{"b":1,"c":3},"d":4}}"#]);
    assert_eq!(rows[0]["o"], json!({"a": {"b": 1, "c": 3}, "d": 4}));
    // Two keys that contradict after flattening: one survives.
    let rows = run(&plan, &[r#"{"o":{"a":{"b":1}},"o.a":5}"#]);
    assert_eq!(rows[0]["o"]["a"]["b"], 1, "{}", rows[0]);
}

/// Two or more keys of one record that contradict after flattening keep one and carry an error exception. A key
/// contradicts by repeating, or under `splitDots` by splitting into a path through another key's value; a key that
/// is not split is one key and contradicts no other.
#[test]
fn contradicting_keys_keep_one_and_carry_an_error() {
    let count = |plan: &Plan, doc: &str| {
        run_scoped_on(plan, &[doc], None)
            .entries()
            .iter()
            .filter(|a| matches!(a.kind, ExceptionKind::KeyContradiction { .. }))
            .count()
    };
    let whole = ok(vec![code(0, JSON)]);
    assert_eq!(count(&whole, r#"{"a":1,"a":2}"#), 1);
    assert_eq!(count(&whole, r#"{"o":{"a":1,"a":2}}"#), 1);
    assert_eq!(count(&whole, r#"{"a":1,"a.b":2}"#), 0);
    assert_eq!(count(&whole, r#"{"o":{"a":{"c":1}},"o.a":5}"#), 0);
    // Inside an array element, and inside a value a declared name takes whole, unless its closed type drops the key.
    assert_eq!(count(&whole, r#"{"a":[{"k":1,"k":2}]}"#), 1);
    let leaf = ok(vec![code(0, &json("o: {x: i64};"))]);
    assert_eq!(count(&leaf, r#"{"o":{"x":1,"x":2}}"#), 1);
    assert_eq!(count(&leaf, r#"{"o":{"x":1,"y":1,"y":2}}"#), 0);
    let plan = ok(vec![code(0, JSON_SPLIT)]);
    assert_eq!(count(&plan, r#"{"a":1,"a":2}"#), 1);
    assert_eq!(count(&plan, r#"{"a":1,"a.b":2}"#), 1);
    assert_eq!(count(&plan, r#"{"a.b":2,"a":1}"#), 1);
    assert_eq!(count(&plan, r#"{"a":[1],"a.b":2}"#), 1);
    // Inside an object: a key that repeats, a key beside a path under it, and a flat key over a nested object.
    assert_eq!(count(&plan, r#"{"o":{"a":1,"a":2}}"#), 1);
    assert_eq!(count(&plan, r#"{"o":{"a":1,"a.b":2}}"#), 1);
    assert_eq!(count(&plan, r#"{"o":{"a":{"x":1},"a.b":2}}"#), 0);
    assert_eq!(count(&plan, r#"{"o":{"a":{"c":1}},"o.a":5}"#), 1);
    // A value at a declared object's name, after a path under it.
    let declared = ok(vec![code(0, &json_split("o: {x: i64, ...};"))]);
    assert_eq!(count(&declared, r#"{"o.y":2,"o":5}"#), 1);
    assert_eq!(count(&declared, r#"{"o":5,"o.y":2}"#), 1);
    // A record that only nests carries none, and a key past the depth bound is one key, as it is written.
    assert_eq!(count(&plan, r#"{"o":{"a":{"c":1}},"o.a.d":2,"x":{"y":1},"x.z":3}"#), 0);
    let deep = vec!["s"; 64].join(".");
    assert_eq!(count(&plan, &format!(r#"{{"{deep}":1,"{deep}.x":2}}"#)), 0);
    let rows = run(&plan, &[r#"{"a":1,"a":2}"#]);
    assert_eq!(rows[0]["a"], 2);
}

/// A declared object or array written in pieces keeps its unclaimed fields and its later value whatever the earlier
/// pieces held: a repeated key a closed object drops, an unclaimed key that is null, and an array written twice.
#[test]
fn declared_containers_written_in_pieces_keep_the_later_value() {
    let elements = |ty: &str| ok(vec![code(0, &json(&format!("xs: array<{ty}>;")))]);
    let rows = run(&elements("{a: i64}"), &[r#"{"xs":[{"z":1,"z":2,"a":3}]}"#]);
    assert_eq!(rows[0]["xs"][0]["a"], 3, "{}", rows[0]);
    let rows = run(&elements("{a: i64, ...}"), &[r#"{"xs":[{"z":null,"y":1}]}"#]);
    assert_eq!(rows[0]["xs"][0]["y"], 1, "{}", rows[0]);
    let rows = run(&elements("{ys: array<i64>}"), &[r#"{"xs":[{"ys":["a"],"ys":[1]}]}"#]);
    assert_eq!(rows[0]["xs"][0]["ys"][0], 1, "{}", rows[0]);
}

/// A declared `datetime` or `duration` reads as an `i64` does, wherever it sits: at the top, in an array, in an
/// array element's object, from a parse or from a value assigned to a declared name.
#[test]
fn a_time_reads_at_any_depth() {
    let plan = ok(vec![code(
        0,
        &json("t: datetime; ts: array<datetime>; xs: array<{t: datetime, d: duration, ...}>;"),
    )]);
    let t = "2024-01-01T00:00:00Z";
    let doc = format!(r#"{{"t":"{t}","ts":["{t}"],"xs":[{{"t":"{t}","d":"1000000"}}]}}"#);
    let rows = run(&plan, &[&doc]);
    let want = "2024-01-01T00:00:00.000000Z";
    assert_eq!(rows[0]["t"], want, "{}", rows[0]);
    assert_eq!(rows[0]["ts"][0], want, "{}", rows[0]);
    assert_eq!(rows[0]["xs"][0]["t"], want, "{}", rows[0]);
    assert!(!rows[0]["xs"][0]["d"].is_null(), "{}", rows[0]);
    assert!(rows[0].get("error").is_none(), "{}", rows[0]);
    let assigned = ok(vec![code(0, &format!("{JSON} r: array<{{t: datetime, ...}}> = xs;"))]);
    let rows = run(&assigned, &[&doc]);
    assert_eq!(rows[0]["r"][0]["t"], want, "{}", rows[0]);
}

/// Keys contradict only where they are kept: a closed object drops the keys it has no field for unfolded, so they
/// contradict nothing. Under `splitDots`, a Kubernetes pod's labels, `app` beside `app.kubernetes.io/name`, contradict
/// only where the labels are kept; read whole, they are two keys and contradict nothing.
#[test]
fn keys_a_closed_object_drops_contradict_nothing() {
    let count = |plan: &Plan, doc: &str| {
        run_scoped_on(plan, &[doc], None)
            .entries()
            .iter()
            .filter(|a| matches!(a.kind, ExceptionKind::KeyContradiction { .. }))
            .count()
    };
    let doc = r#"{"k":{"ns":"shop","labels":{"app":"cart","app.kubernetes.io/name":"cart"}}}"#;
    let dropped = ok(vec![code(0, &json_split("k: {ns: str};"))]);
    assert_eq!(count(&dropped, doc), 0);
    let kept_fields = "k: {ns: str, labels: {...}};";
    let kept = ok(vec![code(0, &json_split(kept_fields))]);
    assert_eq!(count(&kept, doc), 1);
    assert_eq!(count(&ok(vec![code(0, &json(kept_fields))]), doc), 0);
    // The same keys flattened into the record's own names, under a closed object and an open one.
    let flat = r#"{"k.ns":"shop","k.labels.app":"cart","k.labels.app.kubernetes.io/name":"cart"}"#;
    assert_eq!(count(&dropped, flat), 0);
    assert_eq!(count(&ok(vec![code(0, JSON_SPLIT)]), flat), 1);
}

/// A text parser's later occurrence of a key replaces the earlier, and its keys that contradict carry the error as a
/// JSON record's do.
#[test]
fn a_text_parsers_keys_replace_and_contradict_as_jsons_do() {
    let plan = ok(vec![code(
        0,
        r#"Object.assign($, kv(_raw, {pairSep: " ", kvSep: "=", quote: "\""}));"#,
    )]);
    let docs = ["o.a=1 o=5", "a=1 a=2", "x=1 y=2"];
    let rows = run(&plan, &docs);
    assert_eq!(rows[0]["o"], "5");
    assert_eq!(rows[1]["a"], "2");
    let contradicted: Vec<u32> = run_scoped_on(&plan, &docs, None)
        .entries()
        .iter()
        .filter(|a| matches!(a.kind, ExceptionKind::KeyContradiction { .. }))
        .map(|a| a.row)
        .collect();
    assert_eq!(contradicted, vec![0, 1]);
}

/// A path under a variant reads the row's member there. A write under a variant is a compile error of the step that
/// makes it, whatever the step: an assignment, guarded or not, or a rename out of the variant, within it or into it.
#[test]
fn a_write_under_a_variant_is_a_compile_error() {
    let plan = ok(vec![pairs(), build(1, "o"), code(2, "q = o; out = q.a; whole = q;")]);
    let doc = pairs_doc(&[("a", json!("c"))], json!({}));
    let rows = run(&plan, &[&doc]);
    assert_eq!(rows[0]["out"], "c");
    assert_eq!(rows[0]["whole"], json!({"a": "c"}));

    // Step 3 after `q`, a copy of the variant `o`.
    let after_copy = |step: Step| {
        vec![
            code(0, &json("pairs: array<{key: str, value: str}>; r: i64;")),
            build(1, "o"),
            code(2, "q = o;"),
            step,
        ]
    };
    let cases: Vec<(Vec<Step>, FunctionId)> = vec![
        (vec![pairs(), build(1, "o"), code(2, "q = o; q.a = 9;")], 2),
        (vec![pairs(), build(1, "o"), code(2, "q = o; q.a.b: i64 = null;")], 2),
        (after_copy(code(3, "move(q.a, x);")), 3),
        (after_copy(code(3, "move(q.a, q);")), 3),
        (after_copy(code(3, "move(q.a, q.a.b); move(q.b, q.c);")), 3),
        (after_copy(code(3, "move(r, q.a);")), 3),
    ];
    for (steps, id) in cases {
        let label = format!("{steps:?}");
        let e = err(steps);
        let step = format!("function {id}: ");
        assert!(
            e.split("; ")
                .any(|m| m.starts_with(&step) && m.contains("a write under a variant is refused")),
            "{label}\n  got: {e}"
        );
    }
}

/// A declared struct's dotted field is one key, and a path bound under its head is another beside it.
#[test]
fn a_dotted_struct_field_is_one_key_beside_a_path_bound_under_its_head() {
    let plan = ok(vec![code(0, &json(r#"o: {"a.b": i64};"#)), code(1, "o.a.c = 2;")]);
    let rows = run(&plan, &[r#"{"o":{"a.b":1}}"#]);
    assert_eq!(rows[0]["o"], json!({"a.b": 1, "a": {"c": 2}}));
}

/// A struct against a value whose shape is the batch's blends per field, in a ternary and under a guard; the
/// result's shape is the batch's. A struct against a plain null still does not.
#[test]
fn a_struct_blends_against_a_merge() {
    let plan = ok(vec![
        code(
            0,
            &json("v: {a: i64, ...}; flag: bool; pairs: array<{key: str, value: str}>;"),
        ),
        build(1, "p"),
        code(2, "out = flag ? {...p} : {a: 1}; if (flag) { v = {...p}; }"),
    ]);
    let docs = [
        pairs_doc(&[("b", json!("2"))], json!({"flag": true, "v": {"a": 7}})),
        pairs_doc(&[("b", json!("2"))], json!({"flag": false, "v": {"a": 7}})),
    ];
    let rows = run(&plan, &[&docs[0], &docs[1]]);
    assert_eq!(rows[0]["out"], json!({"b": "2"}));
    assert_eq!(rows[1]["out"], json!({"a": 1}));
    assert_eq!(rows[0]["v"], json!({"b": "2"}));
    assert_eq!(rows[1]["v"], json!({"a": 7}));
    // `null` belongs to every type, a struct included: the struct where the branch is taken, null elsewhere.
    let plan = ok(vec![
        code(0, &json("flag: bool;")),
        code(1, "alt = flag ? {a: 1} : null;"),
    ]);
    let rows = run(&plan, &[r#"{"flag":true}"#, r#"{"flag":false}"#]);
    assert_eq!(rows[0]["alt"], json!({"a": 1}));
    assert_eq!(rows[1].get("alt"), None);
}

/// A map whose arrow returns a spread's object is an array of those objects, a spread of a variant included.
#[test]
fn a_map_returning_a_merge_has_the_merges_shape() {
    let plan = ok(vec![
        code(0, &json("v: {a: i64}; xs: array<i64>;")),
        code(1, "out = xs.map(x => ({...v, x: x}));"),
    ]);
    let rows = run(&plan, &[r#"{"v":{"a":1},"xs":[5,6]}"#]);
    assert_eq!(rows[0]["out"], json!([{"a": 1, "x": 5}, {"a": 1, "x": 6}]));
    let plan = ok(vec![
        code(0, &json("xs: array<i64>; pairs: array<{key: str, value: str}>;")),
        build(1, "w"),
        code(2, "out = xs.map(x => ({...w, x: x}));"),
    ]);
    let rows = run(&plan, &[&pairs_doc(&[("a", json!("y"))], json!({"xs": [5, 6]}))]);
    assert_eq!(rows[0]["out"], json!([{"a": "y", "x": 5}, {"a": "y", "x": 6}]));
}

/// A later parser assigns what it declares by presence: where its input lacks a key, the name keeps what it held, a
/// value an earlier step bound or the record's key coerced to the declared type. A declared object the record already
/// holds takes the parse's keys over its own. A name an earlier step bound at another type is a compile error.
#[test]
fn a_later_parser_keeps_what_its_input_lacks() {
    let inner = |declared: &[(&str, DataType)]| {
        func(
            1,
            FunctionKind::Parser {
                input: "inner".to_string(),
                into: "$".into(),
                kind: ParserKind::Json { split_dots: false },
                declared: declared
                    .iter()
                    .map(|(n, t)| DeclaredField {
                        name: n.to_string(),
                        ty: t.clone(),
                        time_source: None,
                    })
                    .collect(),
            },
        )
    };
    let earlier = || code(0, &json("o: {a: i64, ...}; inner: str;"));
    let closed = DataType::Struct(vec![("a".to_string(), DataType::I64)], false);
    let e = err(vec![earlier(), inner(&[("o", closed)])]);
    assert!(e.contains("function 1: `o` is already {a: i64, ...}"), "{e}");
    let open = DataType::Struct(vec![("a".to_string(), DataType::I64)], true);
    let plan = ok(vec![earlier(), inner(&[("o", open)])]);
    let rows = run(
        &plan,
        &[
            r#"{"o":{"a":1,"z":2},"inner":"{}"}"#,
            r#"{"o":{"a":1,"z":2},"inner":"{\"o\":{\"a\":3,\"y\":4}}"}"#,
        ],
    );
    assert_eq!(rows[0]["o"], json!({"a": 1, "z": 2}));
    assert_eq!(rows[1]["o"], json!({"a": 3, "y": 4, "z": 2}));

    let plan = ok(vec![code(0, &json("inner: str;")), inner(&[("x", DataType::I64)])]);
    let docs = [r#"{"x":1,"inner":"{}"}"#, r#"{"x":1,"inner":"{\"x\":2}"}"#];
    let x: Vec<_> = run(&plan, &docs).iter().map(|r| r["x"].clone()).collect();
    assert_eq!(x, [json!(1), json!(2)]);
    // A key the input has wins without coercing the record's.
    assert!(
        run_scoped_on(&plan, &[r#"{"x":{"k":1},"inner":"{\"x\":2}"}"#], None)
            .entries()
            .is_empty()
    );
}

/// A parser with an `into` writes under that object what it would write into the record: its declared fields and
/// every key it finds. `$` names the record. An object the name already holds takes the parse's keys over its own; a
/// name an earlier step bound to anything else is a compile error, and a record key that is no object is replaced.
#[test]
fn a_parser_writes_into_the_object_it_names() {
    let into = |id, input: &str, into: &str, declared: Vec<DeclaredField>| {
        func(
            id,
            FunctionKind::Parser {
                input: input.to_string(),
                into: into.to_string(),
                kind: ParserKind::Json { split_dots: false },
                declared,
            },
        )
    };
    let field = |name: &str, ty: DataType| DeclaredField {
        name: name.to_string(),
        ty,
        time_source: None,
    };
    let doc = r#"{"user":"ana","ms":12}"#;

    let plan = ok(vec![into(0, "_raw", "app", vec![field("ms", DataType::I64)])]);
    let row = run(&plan, &[doc]).remove(0);
    assert_eq!(row["app"], json!({"user": "ana", "ms": 12}));
    assert_eq!((row.get("user"), row.get("ms")), (None, None), "{row}");

    let plan = ok(vec![into(0, "_raw", "app", vec![])]);
    assert_eq!(run(&plan, &[doc])[0]["app"], json!({"user": "ana", "ms": 12}));

    let plan = ok(vec![into(0, "_raw", "a.b", vec![field("ms", DataType::I64)])]);
    assert_eq!(run(&plan, &[doc])[0]["a"], json!({"b": {"user": "ana", "ms": 12}}));

    let plan = ok(vec![into(0, "_raw", "$", vec![field("ms", DataType::I64)])]);
    let row = run(&plan, &[doc]).remove(0);
    assert_eq!((&row["user"], &row["ms"]), (&json!("ana"), &json!(12)));

    // `app` is a key the record holds, then a field an earlier step declared: the parse merges into it either way.
    let doc = r#"{"app":{"user":"bo","z":1},"inner":"{\"user\":\"ana\",\"ms\":12}"}"#;
    let want = json!({"user": "ana", "ms": 12, "z": 1});
    let plan = ok(vec![
        code(0, &json("inner: str;")),
        into(1, "inner", "app", vec![field("ms", DataType::I64)]),
    ]);
    assert_eq!(run(&plan, &[doc])[0]["app"], want);
    let plan = ok(vec![
        code(0, &json("inner: str; app: {user: str, ...};")),
        into(1, "inner", "app", vec![field("ms", DataType::I64)]),
    ]);
    assert_eq!(run(&plan, &[doc])[0]["app"], want);

    // A formatted `datetime` is read by the parse's own key, not the name it lands at.
    let ts = DeclaredField {
        name: "ts".to_string(),
        ty: DataType::DateTime,
        time_source: Some(TimeSource::Format("%d/%m/%Y %H:%M".into())),
    };
    let plan = ok(vec![into(0, "_raw", "app", vec![ts])]);
    let row = run(&plan, &[r#"{"ts":"15/03/2024 07:30"}"#]).remove(0);
    assert_eq!(row["app"]["ts"], json!("2024-03-15T07:30:00.000000Z"));

    // Under a filter, only the rows it takes are parsed.
    let Step::Function(mut filtered) = into(0, "_raw", "app", vec![field("ms", DataType::I64)]) else {
        unreachable!("a function step")
    };
    filtered.filter = Some(r#"startsWith(_raw, "{\"u")"#.into());
    let plan = ok(vec![Step::Function(filtered)]);
    let rows = run(&plan, &[r#"{"user":"ana","ms":12}"#, r#"{"ms":7}"#]);
    assert_eq!(rows[0]["app"], json!({"user": "ana", "ms": 12}));
    assert_eq!(
        rows[1].get("app").filter(|a| !a.is_null() && *a != &json!({})),
        None,
        "{}",
        rows[1]
    );

    // A record whose own `app` is no object: the parsed object replaces it, and the row is marked.
    let docs = [r#"{"app":"x","inner":"{\"a\":1}"}"#];
    let plan = ok(vec![code(0, &json("inner: str;")), into(1, "inner", "app", vec![])]);
    assert_eq!(run(&plan, &docs)[0]["app"], json!({"a": 1}));
    let marks: Vec<_> = run_scoped_on(&plan, &docs, None)
        .entries()
        .iter()
        .map(|e| e.kind.label())
        .collect();
    assert_eq!(marks, ["CoercionRefused"]);

    let e = err(vec![code(0, &json("app: str;")), into(1, "_raw", "app", vec![])]);
    assert!(e.contains("a spread takes an object, not str"), "{e}");
    // A blank `into` names nothing, as a blank input does.
    let e = err(vec![into(0, "_raw", "", vec![])]);
    assert!(e.contains("`` is not a field"), "{e}");
}

/// A later parser's declared name replaces what an earlier parser passed through under it, as an assignment does.
/// Under a guard, the earlier parser's declared value stands where the guard does not hold.
#[test]
fn a_later_parsers_declared_name_replaces_what_an_earlier_one_found_under_it() {
    let inner = |id| {
        func(
            id,
            FunctionKind::Parser {
                input: "inner".to_string(),
                into: "$".into(),
                kind: ParserKind::Json { split_dots: false },
                declared: vec![DeclaredField {
                    name: "o".to_string(),
                    ty: DataType::I64,
                    time_source: None,
                }],
            },
        )
    };
    let plan = ok(vec![code(0, &json("inner: str;")), inner(1)]);
    let rows = run(&plan, &[r#"{"o":{"a":1},"inner":"{\"o\":5}"}"#]);
    assert_eq!(rows[0]["o"], 5);
    // A dynamic parser cannot sit under a guard; a Grok parser can, and declares its names the same way.
    let grok = func(
        1,
        FunctionKind::Parser {
            input: "inner".to_string(),
            into: "$".into(),
            kind: ParserKind::Grok(smeltr::plan::config::GrokConfig {
                split_dots: true,
                patterns: vec!["%{INT:o}".to_string()],
                custom_patterns: vec![],
            }),
            declared: vec![DeclaredField {
                name: "o".to_string(),
                ty: DataType::I64,
                time_source: None,
            }],
        },
    );
    let plan = ok(vec![
        code(0, &json("flag: bool; o: i64; inner: str;")),
        guarded(2, "flag", grok),
    ]);
    let rows = run(
        &plan,
        &[
            r#"{"flag":true,"o":3,"inner":"5"}"#,
            r#"{"flag":false,"o":3,"inner":"5"}"#,
        ],
    );
    assert_eq!(rows[0]["o"], 5);
    assert_eq!(rows[1]["o"], 3);
}

/// A rename of a name to itself moves nothing: the record's keys under the name stay in the output.
#[test]
fn a_rename_to_itself_leaves_the_records_keys() {
    let plan = ok(vec![code(0, "o.a = 1;"), code(1, JSON), code(2, "move(o, o);")]);
    let rows = run(&plan, &[r#"{"o":{"b":2}}"#]);
    assert_eq!(rows[0]["o"], json!({"a": 1, "b": 2}));
}

/// A later parser declares its name only where the name does not hold that type already, so a name an earlier parser
/// declared paths under is a compile error until a typed assignment replaces those paths; the parse then writes the
/// name. Under a guard the replacement would leave the name a value on some rows and an object on others, so it is a
/// compile error too.
#[test]
fn a_later_parsers_declared_name_replaces_the_paths_declared_under_it() {
    let declared_o = || {
        vec![DeclaredField {
            name: "o".to_string(),
            ty: DataType::I64,
            time_source: None,
        }]
    };
    let inner = || {
        func(
            1,
            FunctionKind::Parser {
                input: "inner".to_string(),
                into: "$".into(),
                kind: ParserKind::Json { split_dots: false },
                declared: declared_o(),
            },
        )
    };
    let earlier = || code(0, &json("o: {a: i64, ...}; inner: str;"));
    let e = err(vec![earlier(), inner()]);
    assert!(e.contains("function 1: `o` is already {a: i64, ...}"), "{e}");
    let plan = ok(vec![earlier(), code(2, "o: i64 = null;"), inner()]);
    let rows = run(&plan, &[r#"{"o":{"a":1},"inner":"{\"o\":5}"}"#]);
    assert_eq!(rows[0]["o"], 5);

    let grok = func(
        1,
        FunctionKind::Parser {
            input: "inner".to_string(),
            into: "$".into(),
            kind: ParserKind::Grok(smeltr::plan::config::GrokConfig {
                split_dots: true,
                patterns: vec!["%{INT:o}".to_string()],
                custom_patterns: vec![],
            }),
            declared: declared_o(),
        },
    );
    let e = err(vec![
        code(0, &json("o: {a: i64, ...}; flag: bool; inner: str;")),
        guarded(2, "flag", grok),
    ]);
    assert!(e.contains("function 1: `o`"), "{e}");
    assert!(e.contains("a value or an object on every row"), "{e}");
}

/// A variant only moves: an array literal holding one, or a map returning one, is refused by name.
#[test]
fn an_array_of_variants_is_refused() {
    for body in ["first = at([p, q], 0);", "m = at(xs.map(x => p), 1);"] {
        let e = err(vec![
            code(0, &json("xs: array<i64>; pairs: array<{key: str, value: str}>;")),
            build(1, "p"),
            build(2, "q"),
            code(3, body),
        ]);
        assert!(e.contains("`p` is a variant") || e.contains("a variant"), "{body}: {e}");
    }
}

/// An element of an array holding objects declared as such reads whole, from a literal and from a map; out of range
/// is null in every leaf.
#[test]
fn an_element_of_an_array_of_objects_reads_whole() {
    let plan = ok(vec![
        code(0, &json("xs: array<i64>; p: {a: {c: i64}}; q: {a: {c: i64}};")),
        code(
            1,
            "first = at([p, q], 0); last = at([p, q], -1); gone = at([p], 3); m = at(xs.map(x => p), 1);",
        ),
    ]);
    let rows = run(
        &plan,
        &[
            r#"{"p":{"a":{"c":1}},"q":{"a":{"c":2}},"xs":[1,2]}"#,
            r#"{"p":{"a":{"c":3}},"q":{"a":{"c":4}},"xs":[1]}"#,
        ],
    );
    assert_eq!(rows[0]["first"], json!({"a": {"c": 1}}));
    assert_eq!(rows[0]["last"], json!({"a": {"c": 2}}));
    assert!(rows[0].get("gone").is_none(), "{}", rows[0]);
    assert_eq!(rows[0]["m"], json!({"a": {"c": 1}}));
    assert_eq!(rows[1]["first"], json!({"a": {"c": 3}}));
    assert!(rows[1].get("m").is_none(), "{}", rows[1]);
}

/// `Object.assign(p, a, b)` is `p = {...p, ...a, ...b}`.
#[test]
fn object_assign_spreads_its_sources_into_its_target() {
    let plan = ok(vec![
        code(0, &json("p: {a: i64, k: i64, ...}; q: {b: i64, ...};")),
        code(1, "Object.assign(p, {k: 1}, q);"),
    ]);
    let rows = run(&plan, &[r#"{"p":{"a":1,"k":0},"q":{"b":2}}"#]);
    assert_eq!(rows[0]["p"], json!({"a": 1, "k": 1, "b": 2}));
}

/// `Object.assign` keeps its target's type, under a guard too: the sources' keys convert to it on each row, and a
/// plain write of the same spread is a compile error.
#[test]
fn object_assign_keeps_its_targets_type() {
    let decl = || code(0, &json("p: {a: i64, ...}; q: {b: i64, ...}; f: bool;"));
    let plan = ok(vec![decl(), code(1, "if (f) { Object.assign(p, q); }")]);
    let rows = run(
        &plan,
        &[
            r#"{"f":true,"p":{"a":1},"q":{"a":"7"}}"#,
            r#"{"f":false,"p":{"a":1},"q":{"a":"7"}}"#,
        ],
    );
    assert_eq!(rows[0]["p"], json!({"a": 7}));
    assert_eq!(rows[1]["p"], json!({"a": 1}));
    let e = err(vec![decl(), code(1, "p = {...p, ...q};")]);
    assert!(e.contains("not {a: i64, ...}"), "{e}");
}

/// A spread takes a name or an object literal, and only in an object literal a name is assigned.
#[test]
fn a_spread_of_anything_else_is_refused() {
    let e = err(vec![code(0, "p = {a: 1}; t = {...(p.a + 1)};")]);
    assert!(e.contains("spread"), "{e}");
    let e = err(vec![code(0, "Object.assign(1, {a: 1});")]);
    assert!(e.contains("Object.assign"), "{e}");
    let e = err(vec![code(0, "x = Object.assign();")]);
    assert!(e.contains("Object.assign"), "{e}");
}

/// A spread of a name nothing binds is a compile error of the step that writes it, `Object.assign`'s target included.
#[test]
fn a_spread_of_a_name_nothing_binds_is_a_compile_error() {
    for (body, name) in [("t = {...d, k: 1};", "d"), ("Object.assign(t, {k: 1});", "t")] {
        let e = err(vec![code(0, JSON), code(1, body)]);
        assert!(
            e.contains(&format!("function 1: `{name}` is not a field")),
            "{body}: {e}"
        );
    }
}

/// A key of a spread's result is read from the value the spread laid down.
#[test]
fn a_key_of_a_spreads_result_is_read_from_what_it_laid_down() {
    let plan = ok(vec![
        code(0, &json("p: {a: i64, c: i64, k: i64};")),
        code(1, "t = {...p, k: 2}; let l = {...p}; a = t.a; b = t.k; c = l.c;"),
    ]);
    let rows = run(&plan, &[r#"{"p":{"a":1,"c":3,"k":9}}"#]);
    assert_eq!(cols(&rows, &["a", "b", "c"]), rows![[1, 2, 3]]);
    assert_eq!(rows[0]["t"], json!({"a": 1, "c": 3, "k": 2}));
}

/// A rename is `b = a`, then `a` is gone, bound in code or declared on a parser.
#[test]
fn a_rename_replaces_what_its_target_held() {
    let bound = ok(vec![code(0, "a = 1; b = {k: 2};"), code(1, "move(a, b);")]);
    assert_eq!(fields(&run(&bound, &["{}"])[0]), json!({"b": 1}));
    let declared = ok(vec![
        code(0, &json("a: {x: i64, ...};")),
        code(1, "b = 2;"),
        code(2, "move(a, b);"),
    ]);
    let rows = run(&declared, &[r#"{"a":{"x":1}}"#]);
    assert_eq!(fields(&rows[0]), json!({"b": {"x": 1}}));
}

/// A rename into its own path moves every key before it writes any, so none lands on another still to move.
#[test]
fn a_rename_into_its_own_path_moves_every_key_once() {
    let plan = ok(vec![
        code(0, &json("a: {b: {x: i64, b: {x: i64, ...}, ...}, ...};")),
        code(1, "move(a, a.b);"),
    ]);
    let rows = run(&plan, &[r#"{"a":{"b":{"x":1,"b":{"x":2}}}}"#]);
    assert_eq!(fields(&rows[0]), json!({"a": {"b": {"b": {"x": 1, "b": {"x": 2}}}}}));
}

/// A key written after a copy replaces what the copy put under it, above or below.
#[test]
fn a_later_key_replaces_a_copy_below_it() {
    let plan = ok(vec![code(0, JSON), code(1, "let c = {k: {x: 1}}; t = {...c, k: 2};")]);
    let rows = run(&plan, &[r#"{}"#]);
    assert_eq!(fields(&rows[0])["t"], json!({"k": 2}));
}

// -- declarations and the coercion table ------------------------------------

/// A parser's declaration coerces the record: a text that is the type's rendering arrives as the type, and a text that
/// is not is null with the field's own failure. `name: type;` in code types only a name nothing binds and no open
/// object may hold, so after a JSON parse, whose record is open, it is a compile error of its step.
#[test]
fn a_declaration_coerces_the_record() {
    let plan = ok(vec![code(0, &json("n: i64;")), code(1, "out = n + 1;")]);
    let docs = [r#"{"n":"41"}"#, r#"{"n":"x"}"#, r#"{"n":" 2 "}"#, r#"{}"#];
    assert_eq!(cols(&run(&plan, &docs), &["out"]), rows![[42], [null], [3], [null]]);
    let errors = run_scoped_on(&plan, &docs, None);
    let refused: Vec<u32> = errors
        .entries()
        .iter()
        .filter(|a| matches!(a.kind, ExceptionKind::UnparsableField { .. }))
        .map(|a| a.row)
        .collect();
    assert_eq!(refused, vec![1], "{:?}", errors.entries());
    let e = err(vec![code(0, JSON), code(1, "n: i64; out = n + 1;")]);
    assert!(
        e.contains("function 1: `n` may be a key of the record; to give it a type, write `n: i64 = n`"),
        "{e}"
    );
}

/// A declaration states a name's type once, before anything binds it: the name is null, of that type, until a value is
/// assigned. On a bound name, a path under one, or a key an open object may hold, it is a compile error that names the
/// typed assignment, where one gives the name the declared type.
#[test]
fn a_declaration_states_a_type_once() {
    let plan = ok(vec![code(0, "n: i64; out = n + 1;")]);
    assert_eq!(cols(&run(&plan, &["{}"]), &["out"]), rows![[null]]);
    for (body, want) in [
        ("x = 1; x: i64;", "`x` is already i64"),
        (
            "o = {b: 1}; o.a: str;",
            "`o` is already {b: i64}; to change the type of `o` to {b: i64, a: str}, write `o: {b: i64, a: str} = o as {b: i64, a: str}`",
        ),
        ("x = 1; x.a: str;", "`x` is i64 and has no fields"),
    ] {
        let e = err(vec![code(0, body)]);
        assert!(e.contains(want), "{body}: {e}");
    }
}

/// A write to a bound leaf keeps the leaf's type (rule 5): the value is coerced to it, a pair the table lacks is a
/// compile error naming the typed assignment that changes the type, and a row that refuses is null with an
/// exception. A null bound by a typed assignment takes a value of that type; only `name: type = e` changes a type.
#[test]
fn a_write_to_a_bound_leaf_keeps_its_type() {
    let plan = ok(vec![code(
        0,
        "x: i64; x = \"5\" as i64; n: i64 = null; n = 5; y: array<str>; y: str = \"x\"; f: f64; f = 2;",
    )]);
    assert_eq!(
        cols(&run(&plan, &["{}"]), &["x", "n", "y", "f"]),
        rows![[5, 5, "x", 2.0]]
    );
    let plan = ok(vec![code(0, &json("v: str;")), code(1, "x: i64 = 0; x = v as i64;")]);
    let errors = run_scoped_on(&plan, &[r#"{"v": "7"}"#, r#"{"v": "a"}"#], None);
    let refused: Vec<u32> = errors
        .entries()
        .iter()
        .filter(|a| matches!(a.kind, ExceptionKind::CoercionRefused { .. }))
        .map(|a| a.row)
        .collect();
    assert_eq!(refused, vec![1], "{:?}", errors.entries());
    // Each refusal names the types; where the table converts the pair it names `as`; and it names the typed
    // assignment that changes the name's type.
    for (body, wants) in [
        (
            "x = 1; x = \"a\";",
            &[
                "`\"a\"` is str, not i64",
                "to convert the value, write `\"a\" as i64`",
                "to change the type of `x`, write `x: str = \"a\"`",
            ][..],
        ),
        (
            "x: i64; x = \"5\";",
            &[
                "`\"5\"` is str, not i64",
                "to convert the value, write `\"5\" as i64`",
                "to change the type of `x`, write `x: str = \"5\"`",
            ][..],
        ),
        (
            "let v = \"7\"; x: i64 = 0; x = v;",
            &[
                "`v` is str, not i64",
                "to convert the value, write `v as i64`",
                "to change the type of `x`, write `x: str = v`",
            ][..],
        ),
        ("y: array<str>; y = \"x\";", &["`\"x\"` is str, not array<str>"][..]),
        (
            "x = 1; x = {a: 1};",
            &[
                "`{a: 1}` is {a: i64}, not i64",
                "to change the type of `x`, write `x: {a: i64} = {a: 1}`",
            ][..],
        ),
        (
            "x = 1; x = 2.5;",
            &[
                "`2.5` is f64, not i64",
                "to convert the value, write `2.5 as i64`",
                "to change the type of `x`, write `x: f64 = 2.5`",
            ][..],
        ),
        (
            "o = {a: 1}; o = {a: \"x\"};",
            &["`{a: \"x\"}` is {a: str}, not {a: i64}"][..],
        ),
        (
            "x = [1]; x = [\"a\"];",
            &["`[\"a\"]` is array<str>, not array<i64>"][..],
        ),
    ] {
        let e = err(vec![code(0, body)]);
        for want in wants {
            assert!(e.contains(want), "{body}\n  expected `{want}`, got: {e}");
        }
    }
}

/// `??` reads a variant's presence, not its content: it takes a variant, and gives one, which the typed assignment
/// converts.
#[test]
fn a_variant_takes_a_default_by_presence() {
    let plan = ok(vec![code(0, JSON), code(1, r#"y: str = $.a ?? "none";"#)]);
    let rows = run(&plan, &["{}", r#"{"a": "z"}"#]);
    assert_eq!(cols(&rows, &["a", "y"]), rows![[null, "none"], ["z", "z"]]);
}

/// A typed assignment converts a name bound to a variant, by the same table. A refusal is null, and an error on the
/// row. A declaration on that name is a compile error that points at the assignment.
#[test]
fn a_typed_assignment_converts_a_bound_variant() {
    let plan = ok(vec![
        pairs(),
        build(1, "m"),
        code(2, "q = m.o; q: i64 = q; out = q + 1;"),
    ]);
    let docs = [
        pairs_doc(&[("o", json!("7"))], json!({})),
        pairs_doc(&[("o", json!("x"))], json!({})),
    ];
    let docs: Vec<&str> = docs.iter().map(String::as_str).collect();
    assert_eq!(cols(&run(&plan, &docs), &["out"]), rows![[8], [null]]);
    let errors = run_scoped_on(&plan, &docs, None);
    assert_eq!(errors.error_rows(), vec![1], "{:?}", errors.entries());
    assert!(
        errors
            .entries()
            .iter()
            .any(|a| matches!(a.kind, ExceptionKind::CoercionRefused { to: DataType::I64, .. })),
        "{:?}",
        errors.entries()
    );
    let e = err(vec![pairs(), build(1, "m"), code(2, "q = m.o; q: i64; out = q + 1;")]);
    assert!(
        e.contains("`q` is already variant; to change the type of `q` to i64, write `q: i64 = q as i64`"),
        "{e}"
    );
    // A declaration cannot change a type a value already has.
    let e = err(vec![code(0, JSON), code(1, "x = \"5\"; x: i64;")]);
    assert!(e.contains("already str"), "{e}");
    let e = err(vec![code(0, JSON), code(1, "let l = 1; l: str;")]);
    assert!(e.contains("local"), "{e}");
}

/// `name: type = expr` converts nothing of known type: it widens `i64` to `f64` and takes null, and any other pair
/// is a compile error naming `as`. `as` converts by the table: text to a number, a number to text, an epoch to a
/// time, a float to an integer rounded half away from zero. What the table refuses is null and an error.
#[test]
fn a_typed_assignment_takes_a_converted_value_and_converts_none() {
    let plan = ok(vec![
        code(0, &json("s: str; x: f64;")),
        code(
            1,
            "n: i64 = s as i64; f: f64 = 2; t: str = 5 as str; d: datetime = 1700000000 as datetime;
             w: i64 = x as i64; b: bool = s as bool; k: str = [1, 2] as str; z: f64 = null;",
        ),
    ]);
    let docs = [r#"{"s":"5","x":2.0}"#, r#"{"s":"true","x":1.5}"#];
    let rows = run(&plan, &docs);
    assert_eq!(
        cols(&rows, &["n", "f", "t", "w", "b", "k", "z"]),
        rows![
            [5, 2.0, "5", 2, null, "[1,2]", null],
            [null, 2.0, "5", 2, true, "[1,2]", null]
        ]
    );
    assert_eq!(rows[0]["d"], "2023-11-14T22:13:20.000000Z");
    let errors = run_scoped_on(&plan, &docs, None);
    assert_eq!(errors.error_rows(), vec![0, 1], "{:?}", errors.entries());
    for (body, want) in [
        (
            "n: i64 = s;",
            "`s` is str, not i64; to convert the value, write `n: i64 = s as i64`",
        ),
        (
            "w: i64 = x;",
            "`x` is f64, not i64; to convert the value, write `w: i64 = x as i64`",
        ),
        (
            "t: str = 5;",
            "`5` is i64, not str; to convert the value, write `t: str = 5 as str`",
        ),
        ("xs: array<i64> = s;", "`s` is str, not array<i64>"),
    ] {
        let e = err(vec![code(0, &json("s: str; x: f64;")), code(1, body)]);
        assert!(e.contains(want), "{body}\n  expected `{want}`, got: {e}");
    }
}

/// A typed local converts by the table, as `name: type = expr` does. A parameter and a return are checked: the
/// value must already have the declared type, where a `number` meets `i64` or `f64` and an `i64` widens to `f64`.
#[test]
fn a_typed_local_converts_and_a_parameter_and_return_are_checked() {
    let plan = ok(vec![
        code(0, &json("s: str;")),
        code(
            1,
            "function twice(p: i64): i64 { return p * 2; } function half(p: i64): f64 { return p; }
             let n: i64 = s as i64; out = twice(n); wide = half(n);",
        ),
    ]);
    assert_eq!(
        cols(&run(&plan, &[r#"{"s":"21"}"#]), &["out", "wide"]),
        rows![[42, 21.0]]
    );
    for (body, want) in [
        (
            "function twice(p: i64): i64 { return p * 2; } out = twice(s);",
            "`s` is str, but `twice` takes i64",
        ),
        (
            "function f(p: i64): str { return p; } out = f(1);",
            "the return of `f` is i64, not str",
        ),
        (
            "function f(p: f64): i64 { return p; } out = f(1.5);",
            "the return of `f` is f64, not i64",
        ),
    ] {
        let e = err(vec![code(0, &json("s: str;")), code(1, body)]);
        assert!(e.contains(want), "{body}\n  expected `{want}`, got: {e}");
        assert!(!e.contains("__inl"), "an internal name leaked: {e}");
    }
    // An object carrying a field the parameter's type does not name does not fit it.
    let e = err(vec![code(
        0,
        "function f(p: {x: i64}): i64 { return p.x; } q = {x: 1, k: 2}; out = f(q);",
    )]);
    assert!(e.contains("`q` is {k: i64, x: i64}, but `f` takes {x: i64}"), "{e}");
    assert!(!e.contains("__inl"), "an internal name leaked: {e}");
}

/// An `i64` met by an `f64` widens to `f64` wherever two values meet: a ternary, an array literal, `min`, a write to
/// an `f64` accumulator. The compiler coerces the `i64` side, so the result's type is the same on every row.
#[test]
fn an_i64_beside_an_f64_widens_to_f64() {
    let plan = ok(vec![
        code(0, &json("flag: bool; a: i64; b: f64;")),
        code(
            1,
            "picked = flag ? a : b; arr = [a, b]; m = min(a, b); sum = a + 1; let t = 0.0; for (let x of arr) { t = t + x; } total = t;",
        ),
    ]);
    let ty = |name: &str| plan.provenance.iter().rfind(|o| o.name == name).unwrap().ty.clone();
    assert_eq!(ty("picked"), DataType::F64);
    assert_eq!(ty("arr"), DataType::Array(Box::new(DataType::F64)));
    assert_eq!(ty("m"), DataType::F64);
    assert_eq!(ty("sum"), DataType::I64);
    let rows = run(
        &plan,
        &[r#"{"flag":true,"a":3,"b":1.5}"#, r#"{"flag":false,"a":3,"b":1.5}"#],
    );
    assert_eq!(
        cols(&rows, &["picked", "m", "sum", "total"]),
        rows![[3.0, 1.5, 4, 4.5], [1.5, 1.5, 4, 4.5]]
    );
    assert_eq!(rows[0]["arr"], json!([3.0, 1.5]));
    // An index names `i64`: an `f64` is refused, and `as i64` rounds it.
    let e = err(vec![
        code(0, &json("h: f64;")),
        code(1, "parts = split(\"a,b,c\", \",\"); ath = parts[h + 1];"),
    ]);
    assert!(e.contains("an index must be i64, found f64"), "{e}");
    let plan = ok(vec![
        code(0, &json("i: i64; h: f64;")),
        code(
            1,
            "parts = split(\"a,b,c\", \",\"); at1 = parts[i + 1]; ath = parts[(h + 1) as i64]; bits = (i + 4) & 6;",
        ),
    ]);
    assert_eq!(
        cols(&run(&plan, &[r#"{"i":1,"h":0.4}"#]), &["at1", "ath", "bits"]),
        rows![["c", "b", 4]]
    );
}

/// A condition is `bool`: any other type is a compile error, in a statement, a step, a filter and a drop.
#[test]
fn a_condition_must_be_bool() {
    let n = || code(0, &json("n: i64; s: str;"));
    let e = err(vec![n(), code(1, "if (n) { out = 1; }")]);
    assert!(e.contains("must be bool"), "{e}");
    let e = err(vec![n(), code(1, "out = s ? 1 : 2;")]);
    assert!(e.contains("must be bool"), "{e}");
    let e = err(vec![n(), guarded(2, "n + 1", code(1, "out = 1;"))]);
    assert!(e.contains("must be bool"), "{e}");
    let mut filtered = code(1, "out = 1;");
    if let Step::Function(f) = &mut filtered {
        f.filter = Some("s".into());
    }
    let e = err(vec![n(), filtered]);
    assert!(e.contains("must be bool"), "{e}");
    let e = err(vec![n(), func(1, FunctionKind::Drop { cond: "n".into() })]);
    assert!(e.contains("must be bool"), "{e}");
}

/// A variant moves: it is assigned, copied, spread, serialized and tested for null, and nothing else.
#[test]
fn a_variant_moves_and_nothing_else() {
    let plan = ok(vec![
        pairs(),
        build(1, "o"),
        code(2, "q = o; r = {...o, k: 1}; has = o != null; none = isNull(o) ? 1 : 0;"),
    ]);
    let doc = pairs_doc(&[("a", json!("x"))], json!({}));
    let rows = run(&plan, &[&doc, r#"{}"#]);
    assert_eq!(rows[0]["q"], json!({"a": "x"}));
    assert_eq!(rows[0]["r"], json!({"a": "x", "k": 1}));
    assert_eq!(cols(&rows, &["has", "none"]), rows![[true, 0], [false, 1]]);
    for body in [
        "out = o + 1;",
        "out = o == 5;",
        "out = toString(o);",
        "out = o.a.length();",
        "out = `${o}`;",
    ] {
        let e = err(vec![pairs(), build(1, "o"), code(2, body)]);
        assert!(
            e.contains("is a variant") && e.contains("give it a type first"),
            "{body}: {e}"
        );
    }
}

/// A number array holds `i64` or `f64` by what it holds, and a search meets its needle by value across the two.
#[test]
fn a_number_array_is_searched_by_value() {
    let plan = ok(vec![
        code(0, &json("a: i64; b: f64;")),
        code(
            1,
            "arr = [a, b]; has = includes(arr, a + 0); at = indexOf(arr, b); missing = indexOf([a], 1.5);",
        ),
    ]);
    assert_eq!(
        cols(&run(&plan, &[r#"{"a":1,"b":1.5}"#]), &["has", "at", "missing"]),
        rows![[true, 1, (-1)]]
    );
}

/// `duration` takes a number: a division's fraction reaches it.
#[test]
fn duration_takes_a_number() {
    let plan = ok(vec![
        code(0, &json("n: i64;")),
        code(1, "d = duration(n / 2, \"s\"); ms = countIn(d, \"ms\");"),
    ]);
    assert_eq!(cols(&run(&plan, &[r#"{"n":5}"#]), &["ms"]), rows![[2500.0]]);
}

/// A function declares its return type: each `return` is checked against it where it is written, as a tail call's
/// argument is against its parameter.
#[test]
fn a_function_declares_its_return_type() {
    let plan = ok(vec![
        code(0, &json("b: bool; n: i64;")),
        code(
            1,
            "function f(b: bool): str { if (b) { return \"yes\"; } else { return null; } }
             function down(k: i64, acc: i64): i64 { if (k <= 0) { return acc; } return down(k - 1, acc + k); }
             out = f(b); tail = down(n, 0);",
        ),
    ]);
    assert_eq!(
        cols(
            &run(&plan, &[r#"{"b":true,"n":3}"#, r#"{"b":false,"n":3}"#]),
            &["out", "tail"]
        ),
        rows![["yes", 6], [null, 6]]
    );
    for (body, want) in [
        (
            "function f(b: bool): str { if (b) { return 1; } else { return true; } } out = f(true);",
            "the return of `f` is i64, not str",
        ),
        (
            "function down(k: i64, acc: i64): i64 { if (k <= 0) { return acc; } return down(\"0\", acc + k); } out = down(3, 0);",
            "argument `k` of `down` is str, not i64",
        ),
        (
            "function f(x: i64): i64 { x: str; return x; } out = f(1);",
            "`x` is a parameter of `f`",
        ),
        (
            "function f(x: i64): i64 { let t = x; t: str; return t; } out = f(1);",
            "`t` is a local of `f`",
        ),
    ] {
        let e = err(vec![code(0, body)]);
        assert!(e.contains(want), "{body}\n  expected `{want}`, got: {e}");
    }
}

/// `??` over two structs takes the union of their fields: a field one side lacks is null on the rows it wins, and a
/// path under the result reads that field.
#[test]
fn a_coalesce_of_two_structs_takes_the_union_of_their_fields() {
    let plan = ok(vec![
        code(0, &json("p: {x: i64}; q: {y: i64};")),
        code(1, "r = p ?? q; rx = r.x; ry = r.y;"),
    ]);
    let rows = run(&plan, &[r#"{"p":{"x":1}}"#, r#"{"q":{"y":2}}"#]);
    assert_eq!(rows[0]["r"], json!({"x": 1}));
    assert_eq!(cols(&rows, &["rx", "ry"]), rows![[1, null], [null, 2]]);
    assert_eq!(rows[1]["r"], json!({"y": 2}));
}

/// `??` takes the type its operands meet at, an operand that holds nothing included: `i64 ?? f64` is `f64`, as compiled.
#[test]
fn a_coalesce_beside_an_empty_operand_meets_its_type() {
    let plan = ok(vec![code(0, &json("i: i64;")), code(1, "x: f64 = null; out = i ?? x;")]);
    let rows = run(&plan, &[r#"{"i":1}"#, r#"{}"#]);
    assert_eq!(cols(&rows, &["out"]), rows![[1.0], [null]]);
}

/// A push onto a null array stores what was pushed: an `i64` stays `i64`.
#[test]
fn a_push_onto_a_null_number_array_keeps_integer_storage() {
    let plan = ok(vec![
        code(0, &json("f: f64;")),
        code(
            1,
            "let xs: array<i64> = null; ints = push(xs, 1); floats = push(xs, f);",
        ),
    ]);
    let rows = run(&plan, &[r#"{"f":1.5}"#]);
    assert_eq!(rows[0]["ints"], json!([1]));
    assert_eq!(rows[0]["floats"], json!([1.5]));
}

/// Array `+` meets its element types as `concat` does: an `i64` beside an `i64`, and an `i64` beside an `f64`, whose
/// elements widen to `f64`.
#[test]
fn array_addition_unifies_its_element_types() {
    let plan = ok(vec![
        code(0, &json("n: i64; f: f64;")),
        code(1, "both = [n + 0] + [2]; mixed = [1] + [f];"),
    ]);
    let rows = run(&plan, &[r#"{"n":3,"f":1.5}"#]);
    assert_eq!(rows[0]["both"], json!([3, 2]));
    assert_eq!(rows[0]["mixed"], json!([1.0, 1.5]));
}

/// A spread of objects whose shapes the compiler knows has a known shape: each field a spread lays over another meets
/// it as a ternary's branches do. So a merged object's fields read by their types, and a function may return one. An
/// object a JSON parser gives is open, and so is a spread of it.
#[test]
fn a_spread_of_known_objects_has_a_known_shape() {
    let plan = ok(vec![
        code(0, &json("n: i64; flag: bool; o: {x: i64, k: {v: i64, ...}, ...};")),
        code(
            1,
            "function f(o: {x: i64, k: {v: i64, ...}, ...}): {x: i64, k: {v: i64, ...}, y: i64, ...} { return {...o, y: n}; }
             r = f(o);
             s = flag ? {x: 1, k: {v: 2}} : {x: 3, k: {v: 4}}; b = {...s, y: \"t\"}; bx = b.x + 1; kv = b.k.v;",
        ),
    ]);
    let rows = run(
        &plan,
        &[
            r#"{"n":7,"flag":true,"o":{"x":1,"k":{"v":2}}}"#,
            r#"{"n":8,"flag":false,"o":{"x":5,"k":{"v":6}}}"#,
        ],
    );
    assert_eq!(rows[0]["r"], json!({"x": 1, "k": {"v": 2}, "y": 7}));
    assert_eq!(rows[1]["r"], json!({"x": 5, "k": {"v": 6}, "y": 8}));
    assert_eq!(cols(&rows, &["bx", "kv"]), rows![[2, 2], [4, 4]]);
    assert_eq!(rows[0]["b"], json!({"x": 1, "k": {"v": 2}, "y": "t"}));

    for (body, want) in [
        (
            "function g(o: {x: i64, ...}): {x: str, ...} { return {...o}; } s = g(o);",
            "the return of `g` is {x: i64, ...}, not {x: str, ...}",
        ),
        ("m = {...n};", "a spread takes an object, not i64"),
    ] {
        let declared = code(0, &json("o: {x: i64, ...}; flag: bool; n: i64;"));
        let e = err(vec![declared, code(1, body)]);
        assert!(e.contains(want), "{body}\n  expected `{want}`, got: {e}");
    }
}

/// A returned object literal is checked field by field against its object return type, nested where both nest. A
/// field the type does not name is an error, and a field the literal lacks is a typed null, so a caller reads every
/// leaf the type promises.
#[test]
fn a_returned_object_is_checked_field_by_field() {
    let plan = ok(vec![
        code(0, &json("n: i64;")),
        code(
            1,
            "function f(n: i64): {x: i64, k: {v: f64}} { if (n < 0) { return {x: 0}; } return {x: n, k: {v: n}}; }
             o = f(n); ox = o.x;",
        ),
    ]);
    let rows = run(&plan, &[r#"{"n":7}"#, r#"{"n":-1}"#]);
    assert_eq!(rows[0]["o"], json!({"x": 7, "k": {"v": 7.0}}));
    assert_eq!(rows[0]["ox"], json!(7));
    assert_eq!(rows[1]["o"], json!({"x": 0}));

    let plan = ok(vec![
        code(0, &json("n: i64;")),
        code(
            1,
            "function f(n: i64): {x: i64, y: str, k: {v: str}} { return {x: n}; } o = f(n); oy = o.y; okv = o.k.v; gone = isNull(o.k);",
        ),
    ]);
    let rows = run(&plan, &[r#"{"n":7}"#]);
    assert_eq!(rows[0]["o"], json!({"x": 7}));
    assert_eq!(cols(&rows, &["oy", "okv", "gone"]), rows![[null, null, true]]);

    for (body, want) in [
        (
            "function f(n: i64): {x: str} { return {x: n}; } o = f(1);",
            "field `x` of the return of `f` is i64, not str",
        ),
        (
            "function f(n: i64): {k: {v: str}} { return {k: {v: n}}; } o = f(1);",
            "field `k.v` of the return of `f` is i64, not str",
        ),
        (
            "function f(n: i64): {x: i64} { return {x: n, extra: n}; } o = f(1);",
            "the return of `f` has a field `extra` its type does not name",
        ),
    ] {
        let e = err(vec![code(0, body)]);
        assert!(e.contains(want), "{body}\n  expected `{want}`, got: {e}");
    }
}

/// `as i64` takes a float anywhere in the `i64` range, rounded half away from zero, and text however it spells the
/// value: a decimal tail, an exponent, or both. Beyond the range it is refused.
#[test]
fn a_float_in_the_i64_range_converts_to_i64_by_as() {
    let plan = ok(vec![
        code(0, &json("s: str;")),
        code(1, "big = 1e18 as i64; n = s as i64;"),
    ]);
    let docs = [
        r#"{"s":"123.0"}"#,
        r#"{"s":"-9223372036854775808.00"}"#,
        r#"{"s":"1.50e1"}"#,
        r#"{"s":"2e3"}"#,
        r#"{"s":"9223372036854775808"}"#,
        r#"{"s":"2.5"}"#,
        r#"{"s":"-2.5"}"#,
        r#"{"s":"1e400"}"#,
    ];
    let rows = run(&plan, &docs);
    assert_eq!(
        cols(&rows, &["big", "n"]),
        rows![
            [1000000000000000000i64, 123],
            [1000000000000000000i64, (-9223372036854775808i64)],
            [1000000000000000000i64, 15],
            [1000000000000000000i64, 2000],
            [1000000000000000000i64, null],
            [1000000000000000000i64, 3],
            [1000000000000000000i64, (-3)],
            [1000000000000000000i64, null]
        ]
    );
    let errors = run_scoped_on(&plan, &docs, None);
    assert_eq!(errors.error_rows(), vec![4, 7], "{:?}", errors.entries());
    let e = err(vec![code(0, "n: number;")]);
    assert!(e.contains("unknown type `number`"), "{e}");
}

/// `as` to an object type converts an object's variant leaves, each by its type, and the typed assignment takes
/// the converted object.
#[test]
fn a_conversion_to_an_object_type_converts_each_variant_leaf() {
    let plan = ok(vec![
        pairs(),
        build(1, "m"),
        code(
            2,
            "q = {a: m.p, k: {v: m.p}}; q: {a: i64, k: {v: str}} = q as {a: i64, k: {v: str}}; out = q.a + 1; kv = q.k.v;",
        ),
    ]);
    let doc = pairs_doc(&[("p", json!("2"))], json!({}));
    let rows = run(&plan, &[&doc]);
    assert_eq!(cols(&rows, &["out", "kv"]), rows![[3, "2"]]);
    let e = err(vec![code(0, "q = {a: 1}; q: i64;")]);
    assert!(
        e.contains("`q` is already {a: i64}; to change the type of `q` to i64"),
        "{e}"
    );
}

/// A written value converts to the type held (O.3-O.7): an object literal of another shape is a compile error naming
/// the typed assignment; a variant that does not fit leaves the object null on that row, marked; an open value
/// over a closed object is a compile error naming both ways out; `as` converts the value and the name keeps its
/// type; a converted value of another shape is still refused. `$ = e` is not built.
#[test]
fn an_object_keeps_its_type_and_as_converts_a_value() {
    let pairs = r#"let pairs = [{key: "a", value: "5"}, {key: "z", value: "y"}];"#;
    for (body, want) in [
        (
            r#"o = {a: 1}; o = {b: "x"};"#,
            r#"`{b: "x"}` is {b: str}, not {a: i64}; to change the type of `o`, write `o: {b: str} = {b: "x"}`"#,
        ),
        (
            &format!("o = {{a: 1}}; {pairs} o = fromEntries(pairs);"),
            "`fromEntries(pairs)` is {...}, not {a: i64}; to convert the value, write `fromEntries(pairs) as \
             {a: i64}`; to change the type of `o`, write `o: {...} = fromEntries(pairs)`",
        ),
        (
            r#"o = {a: 1}; o = {b: "x"} as {b: str};"#,
            r#"`{b: "x"} as {b: str}` is {b: str}, not {a: i64}; to change the type of `o`, write `o: {b: str} = {b: "x"}`"#,
        ),
    ] {
        let e = err(vec![code(0, JSON), code(1, body)]);
        assert!(e.contains(want), "{body}:\n  want {want}\n  got {e}");
    }
    let plan = ok(vec![
        code(0, JSON),
        code(
            1,
            &format!(
                "o = {{a: 1}}; o = $.user as {{a: i64}}; {pairs} p = {{a: 1}}; p = fromEntries(pairs) as {{a: i64}};"
            ),
        ),
    ]);
    let docs = [r#"{"user": {"a": 7}}"#, r#"{"user": 7}"#];
    assert_eq!(
        cols(&run(&plan, &docs), &["o", "p"]),
        rows![[{"a": 7}, {"a": 5}], [null, {"a": 5}]]
    );
    let errors = run_scoped_on(&plan, &docs, None);
    let refused: Vec<(u32, String)> = errors
        .entries()
        .iter()
        .map(|a| (a.row, a.message(&plan.slot_names())))
        .collect();
    assert_eq!(
        refused,
        [(1, "7, which could not convert to {a: i64} in o".to_string())]
    );
}

/// A null condition reads false in a `while`, an arrow filter, a route, a delivery and a capture, as it does in an
/// `if`: the one helper decides for every site.
#[test]
fn a_null_condition_reads_false_everywhere() {
    let plan = ok(vec![
        code(0, &json("xs: array<i64>;")),
        code(
            1,
            "let i = 0; while (null) { i = i + 1; } trips = i; kept = xs.filter(x => null);",
        ),
    ]);
    assert_eq!(
        cols(&run(&plan, &[r#"{"xs":[1,2]}"#]), &["trips", "kept"]),
        rows![[0, []]]
    );
}

/// A duration reads from its own text: the microsecond count `toString` writes is what a typed assignment, from text
/// or from a variant, and `parseDuration` read back. Text that is no count refuses under a typed assignment and is
/// null from the explicit conversion.
#[test]
fn a_duration_reads_its_own_text() {
    let plan = ok(vec![
        code(0, &json("s: str; pairs: array<{key: str, value: str}>;")),
        build(1, "m"),
        code(
            2,
            "d: duration = s as duration; back = toString(d); p = parseDuration(s); same = p == d;
             w = m.w; w: duration = w; secs = countIn(w, \"s\");",
        ),
    ]);
    let docs = [
        pairs_doc(&[("w", json!("1500000"))], json!({"s": "90000000"})),
        pairs_doc(&[("w", json!("2.5e6"))], json!({"s": "soon"})),
        pairs_doc(
            &[("w", json!("-9.223372036854776e18"))],
            json!({"s": "-9.223372036854776e18"}),
        ),
        pairs_doc(&[("w", json!(""))], json!({"s": " "})),
    ];
    let docs: Vec<&str> = docs.iter().map(String::as_str).collect();
    let rows = run(&plan, &docs);
    // A blank under a duration is nothing: null, and no failure.
    assert_eq!(
        cols(&rows, &["back", "same", "secs"]),
        rows![
            ["90000000", true, 1.5],
            [null, true, 2.5],
            ["-9223372036854775808", true, (-9223372036854.776)],
            [null, true, null]
        ]
    );
    let errors = run_scoped_on(&plan, &docs, None);
    assert_eq!(errors.error_rows(), vec![1], "{:?}", errors.entries());
}

/// An open conversion of a variant keeps what it does not name, as an open declaration at the parser does, and a blend
/// of two such objects keeps each row's own.
#[test]
fn an_open_conversion_of_a_variant_keeps_the_keys_it_does_not_name() {
    let plan = ok(vec![
        code(0, &json("flag: bool; pairs: array<{key: str, value: str}>;")),
        build(1, "o"),
        code(2, "q: {a: i64, ...} = o; p = flag ? q : {a: 0};"),
    ]);
    let docs = [
        pairs_doc(&[("a", json!("5")), ("b", json!("1"))], json!({"flag": true})),
        pairs_doc(&[("a", json!("1"))], json!({"flag": false})),
    ];
    let rows = run(&plan, &[&docs[0], &docs[1]]);
    assert_eq!(rows[0]["q"], json!({"a": 5, "b": "1"}));
    assert_eq!(rows[0]["p"], json!({"a": 5, "b": "1"}));
    assert_eq!(rows[1]["p"], json!({"a": 0}));
}

/// A closed conversion of a variant, `q: {name: str} = q`, keeps the fields it names and drops each other key,
/// silently, as a parser's closed object does.
#[test]
fn a_closed_conversion_of_a_variant_drops_the_keys_it_does_not_name() {
    let plan = ok(vec![code(0, JSON), code(1, "q = $.user; q: {name: str} = q;")]);
    let docs = [r#"{"user": {"name": "ann", "age": 5}}"#, r#"{"user": {"name": "bo"}}"#];
    let rows = run(&plan, &docs);
    assert_eq!(rows[0]["q"], json!({"name": "ann"}), "{rows:?}");
    assert_eq!(rows[1]["q"], json!({"name": "bo"}), "{rows:?}");
    let errors = run_scoped_on(&plan, &docs, None);
    assert!(errors.entries().is_empty(), "{:?}", errors.entries());
    let e = err(vec![code(0, JSON), code(1, "q = $.user; q: {name: str};")]);
    assert!(
        e.contains("`q` is already variant; to change the type of `q` to {name: str}, write `q: {name: str} = q as {name: str}`"),
        "{e}"
    );
}

/// A declaration of an object type over a variant converts it: a value that does not fit, a leaf's, a nested
/// object's or the whole value's, is a refused conversion, an error, and that value is null.
#[test]
fn a_variant_that_does_not_fit_its_declaration_is_a_refused_conversion() {
    let plan = ok(vec![code(0, JSON), code(1, "q = $.v; q: {a: i64, b: {c: i64}} = q;")]);
    let docs = [
        r#"{"v": {"a": "x"}}"#,
        r#"{"v": {"b": 5}}"#,
        r#"{"v": 5}"#,
        r#"{"v": null}"#,
        r#"{"v": {"a": 1, "b": {"c": 2}}}"#,
    ];
    let rows = run(&plan, &docs);
    assert_eq!(rows[4]["q"], json!({"a": 1, "b": {"c": 2}}), "{rows:?}");
    let errors = run_scoped_on(&plan, &docs, None);
    let mut failed: Vec<u32> = errors.entries().iter().map(|a| a.row).collect();
    failed.sort();
    assert_eq!(failed, [0, 1, 2], "{:?}", errors.entries());
    assert!(
        errors
            .entries()
            .iter()
            .all(|a| matches!(a.kind, ExceptionKind::CoercionRefused { .. })),
        "{:?}",
        errors.entries()
    );
}

/// A conversion to an object type fills each declared field from a key of the value's unclaimed fields. An open type
/// keeps the other keys as its unclaimed fields. A closed type drops each other key, silently. A closed value
/// converted to a closed type with fewer fields drops the rest the same way.
#[test]
fn a_conversion_to_an_object_type_takes_its_fields_from_the_unclaimed_keys() {
    let doc = r#"{"pairs":[{"key":"a","value":"x"},{"key":"z","value":"y"}]}"#;
    let plan = |body: &str| {
        ok(vec![
            code(0, &json("pairs: array<{key: str, value: str}>;")),
            code(1, body),
        ])
    };
    let silent = |plan: &Plan| {
        let errors = run_scoped_on(plan, &[doc], None);
        assert!(errors.entries().is_empty(), "{:?}", errors.entries());
    };

    let open = plan("o: {a: str, ...} = fromEntries(pairs); out = o.a;");
    let rows = run(&open, &[doc]);
    assert_eq!(rows[0]["o"], json!({"a": "x", "z": "y"}), "{rows:?}");
    assert_eq!(rows[0]["out"], json!("x"), "{rows:?}");

    let closed = plan("o: {a: str} = fromEntries(pairs) as {a: str}; out = o.a;");
    let rows = run(&closed, &[doc]);
    assert_eq!(rows[0]["o"], json!({"a": "x"}), "{rows:?}");
    assert_eq!(rows[0]["out"], json!("x"), "{rows:?}");
    silent(&closed);

    let narrowed = plan("y = {b: 1, c: 2}; w: {b: i64} = y as {b: i64};");
    let rows = run(&narrowed, &[doc]);
    assert_eq!(rows[0]["w"], json!({"b": 1}), "{rows:?}");
    silent(&narrowed);
}

/// A declared path's head is written as the record holds it: an object keeps the keys the path does not name. The
/// head is a declared object, so where the record's value there is no object, the head is null on that row.
#[test]
fn a_declared_paths_head_is_written_as_the_record_holds_it() {
    let plan = ok(vec![code(0, &json("o: {a: i64, ...};"))]);
    let rows = run(&plan, &[r#"{"o":5}"#, r#"{"o":{"a":1,"b":2}}"#, r#"{"o":{"a":1}}"#]);
    assert_eq!(cols(&rows, &["o"]), rows![[null], [{"a": 1, "b": 2}], [{"a": 1}]]);
}

/// A copy shares its source's slot until a loop assigns one of them: the loop's value lands in a slot of its own, so
/// the copy keeps the value from before the loop.
#[test]
fn a_copy_keeps_its_value_when_a_loop_assigns_the_source() {
    let plan = ok(vec![
        code(0, &json("pos: i64; xs: array<i64>;")),
        code(
            1,
            "start = pos; while (pos < 3) { pos = pos + 1; } delta = pos - start;",
        ),
        code(
            2,
            "first = pos; let n = 0; for (let x of xs) { n = n + 1; pos = pos + x * n; } grown = pos - first;",
        ),
    ]);
    let rows = run(&plan, &[r#"{"pos":0,"xs":[1,2]}"#]);
    assert_eq!(
        cols(&rows, &["start", "delta", "first", "grown", "pos"]),
        rows![[0, 3, 3, 5, 8]]
    );
}

/// A copy of the array an unroll takes apart is its own field: every row the unroll makes keeps it whole.
#[test]
fn a_copy_of_an_unrolled_array_stays_whole() {
    let plan = ok(vec![
        code(0, &json("items: array<i64>;")),
        code(1, "copy = items;"),
        code(2, "unroll(items, true);"),
    ]);
    let rows = run(&plan, &[r#"{"items":[1,2]}"#]);
    assert_eq!(cols(&rows, &["items", "copy"]), rows![[1, [1, 2]], [2, [1, 2]]]);
}

/// A field a `let` hides is not dead to an unroll: the block's end puts it back, and every row the unroll makes keeps
/// it.
#[test]
fn a_field_a_let_hides_survives_an_unroll() {
    let plan = ok(vec![
        code(0, "y = 7; a = [1, 2];"),
        code(1, "let y = 0; unroll(a, true);"),
    ]);
    let rows = run(&plan, &["{}"]);
    assert_eq!(cols(&rows, &["a", "y"]), rows![[1, 7], [2, 7]]);
}

/// An unroll that keeps no parent field releases a field a `let` hides as it releases the rest: the block's end puts
/// the name back, empty.
#[test]
fn an_unroll_releases_a_field_a_let_hides() {
    let plan = ok(vec![
        code(0, "y = 7; z = 8; a = [1, 2];"),
        code(1, "let y = 0; unroll(a, false);"),
    ]);
    let rows = run(&plan, &["{}"]);
    assert_eq!(cols(&rows, &["a", "y", "z"]), rows![[1, null, null], [2, null, null]]);
}

/// A local named for one of smeltr's own fields is the step's own: an unroll and a `drop()` act on the record's field,
/// which the unroll keeps.
#[test]
fn a_local_named_for_a_control_field_leaves_the_field_alone() {
    let plan = ok(vec![
        code(0, "a = [1, 2];"),
        code(1, "let __live = false; unroll(a, false);"),
    ]);
    let rows = run(&plan, &["{}"]);
    assert_eq!(cols(&rows, &["a"]), rows![[1], [2]]);
    for body in [
        "let __live = true; drop();",
        "a = 1; let __live = true; if (a == 1) { drop(); }",
    ] {
        assert!(run(&ok(vec![code(0, body)]), &["{}"]).is_empty(), "{body}");
    }
}

/// A local is the step's own, not a parent field: it keeps its value on every row an unroll makes, as does a copy of
/// it.
#[test]
fn a_local_keeps_its_value_across_an_unroll() {
    let plan = ok(vec![
        code(0, "y = 7; a = [1, 2];"),
        code(
            1,
            "let g = 5; let o = {k: y}; let y = y; unroll(a, false); w = g; v = o.k; u = y;",
        ),
    ]);
    let rows = run(&plan, &["{}"]);
    assert_eq!(
        cols(&rows, &["a", "w", "v", "u", "y"]),
        rows![[1, 5, 7, 7, null], [2, 5, 7, 7, null]]
    );
}

/// A guarded write that would turn an object under its target into a value is refused: the object keeps its type.
#[test]
fn a_guarded_write_that_changes_a_nested_shape_is_refused() {
    let e = err(vec![
        code(0, &json("flag: bool;")),
        code(1, "a = {b: {c: 1}}; if (flag) { a = {b: 2}; }"),
    ]);
    assert!(
        e.contains("`{b: 2}` is {b: i64}, not {b: {c: i64}}; to change the type of `a`, write `a: {b: i64} = {b: 2}`"),
        "{e}"
    );
}

/// A write two loops deep under a record's key keeps the rest of that key, as one outside any loop does.
#[test]
fn a_write_in_a_nested_loop_keeps_the_rest_of_the_records_key() {
    let plan = ok(vec![
        code(0, &json("xs: array<i64>; ys: array<i64>;")),
        code(
            1,
            "o.k: i64 = null; for (let x of xs) { for (let y of ys) { o.k = x + y; } }",
        ),
    ]);
    let rows = run(&plan, &[r#"{"xs":[1,2],"ys":[10],"o":{"z":1}}"#]);
    assert_eq!(rows[0]["o"], json!({"k": 12, "z": 1}));
}

/// An empty object claims its name: the record's key of that name is gone, and an object that holds nothing is not
/// written.
#[test]
fn an_empty_object_claims_its_name() {
    let plan = ok(vec![code(0, JSON), code(1, "a = {};")]);
    let rows = run(&plan, &[r#"{"a":{"x":1}}"#]);
    assert_eq!(fields(&rows[0]), json!({}));
}

/// A spread of an object lays its unclaimed keys over the fields before it, as it lays its leaves.
#[test]
fn a_spread_lays_an_objects_unclaimed_keys_over_the_fields_before_it() {
    let plan = ok(vec![
        code(0, JSON),
        code(1, r#"user.id = 5; t = {name: "anonymous", ...user};"#),
    ]);
    let rows = run(&plan, &[r#"{"user":{"name":"ann"}}"#]);
    assert_eq!(rows[0]["t"], json!({"name": "ann", "id": 5}));
}

/// A second parse lays only the keys it found: a key an earlier parse laid, and a later step moved or replaced, stays
/// moved or replaced, at the record's top and in an object.
#[test]
fn a_second_parse_lays_only_the_keys_it_found() {
    let plan = ok(vec![
        code(0, &json("body: str;")),
        code(1, r#"x.a = 1; http = {verb: "GET"};"#),
        code(2, "move(x, y);"),
        code(3, "Object.assign($, json(body));"),
    ]);
    let rows = run(
        &plan,
        &[r#"{"x":{"z":1},"http":{"method":"GET","path":"/"},"body":"{\"k\":2}"}"#],
    );
    let mut row = fields(&rows[0]);
    row.as_object_mut().unwrap().remove("body");
    assert_eq!(row, json!({"y": {"a": 1, "z": 1}, "http": {"verb": "GET"}, "k": 2}));
}

/// A key a step claimed stays claimed after a rename takes its name away: the record's own value there stays gone,
/// and the rename emits nothing (rules 3 and 6).
#[test]
fn a_renamed_claim_keeps_the_records_key_gone() {
    let plan = ok(vec![code(0, JSON), code(1, "x = 1;"), code(2, "move(x, y);")]);
    assert!(plan.instrs.iter().all(|i| i.function != 2), "the rename emits nothing");
    let rows = run(&plan, &[r#"{"x":5,"k":1}"#]);
    assert_eq!(fields(&rows[0]), json!({"y": 1, "k": 1}));
}

/// An object whose only key a rename took away is null.
#[test]
fn an_object_left_with_only_a_key_a_rename_took_away_is_null() {
    let plan = ok(vec![
        code(0, JSON),
        code(1, "user.id = 1;"),
        code(2, "move(user.id, uid);"),
        code(3, "gone = isNull(user);"),
    ]);
    let rows = run(&plan, &[r#"{"user":{"id":5}}"#, r#"{"user":{"id":6,"name":"bo"}}"#]);
    assert_eq!(rows[0]["gone"], json!(true));
    assert_eq!(rows[1]["gone"], json!(false));
}

/// A copy of an object a rename took a key out of carries the removal and emits nothing (rule 3); a spread of it
/// lays the object's other keys and not that one (rule 3).
#[test]
fn a_copy_and_a_spread_leave_out_a_key_a_rename_took_away() {
    let plan = ok(vec![
        code(0, JSON),
        code(1, "user.id = 1;"),
        code(2, "move(user.id, uid);"),
        code(3, "c = user;"),
        code(4, "s = {...user, extra: 2}; t = {id: 0, ...user};"),
    ]);
    assert!(plan.instrs.iter().all(|i| i.function != 2), "the rename emits nothing");
    assert!(plan.instrs.iter().all(|i| i.function != 3), "the copy emits nothing");
    let rows = run(&plan, &[r#"{"user":{"id":5,"name":"ann"}}"#]);
    assert_eq!(
        fields(&rows[0]),
        json!({
            "user": {"name": "ann"},
            "uid": 1,
            "c": {"name": "ann"},
            "s": {"name": "ann", "extra": 2},
            "t": {"id": 0, "name": "ann"},
        })
    );
}

/// A guarded write over an object a rename took a key out of keeps that key gone on the rows the guard leaves alone.
#[test]
fn a_guarded_write_keeps_a_key_a_rename_took_away_gone() {
    let plan = ok(vec![
        code(0, &json("flag: bool;")),
        code(1, "user.id = 1;"),
        code(2, "move(user.id, uid);"),
        code(3, "if (flag) { user = {...user, extra: 2}; }"),
    ]);
    let rows = run(
        &plan,
        &[
            r#"{"flag":false,"user":{"id":5,"name":"ann"}}"#,
            r#"{"flag":true,"user":{"id":6,"name":"bo"}}"#,
        ],
    );
    assert_eq!(rows[0]["user"], json!({"name": "ann"}));
    assert_eq!(rows[1]["user"], json!({"name": "bo", "extra": 2}));
}

/// A later parse that finds the key again writes it: the removal covers only what the record held before (rule 3).
#[test]
fn a_later_parse_writes_a_key_a_rename_took_away() {
    let plan = ok(vec![
        code(0, &json("body: str;")),
        code(1, "x = 1;"),
        code(2, "move(x, y);"),
        code(3, "Object.assign($, json(body));"),
    ]);
    let rows = run(&plan, &[r#"{"x":5,"body":"{\"x\":7}","k":1}"#]);
    let mut row = fields(&rows[0]);
    row.as_object_mut().unwrap().remove("body");
    assert_eq!(row, json!({"y": 1, "x": 7, "k": 1}));
}

/// An object with a variant leaf is an object: a ternary of two gives an object, and a write under it keeps the rest
/// of it.
#[test]
fn an_object_with_a_variant_leaf_stays_an_object() {
    let plan = ok(vec![
        code(0, &json("flag: bool; pairs: array<{key: str, value: i64}>;")),
        build(1, "m"),
        code(2, "x = {a: m, b: 1}; z = {a: m, b: 2}; y = flag ? x : z; y.k = 3;"),
    ]);
    let rows = run(
        &plan,
        &[
            &pairs_doc(&[("p", json!(7))], json!({"flag": true})),
            &pairs_doc(&[("q", json!(8))], json!({"flag": false})),
        ],
    );
    assert_eq!(rows[0]["y"], json!({"a": {"p": 7}, "b": 1, "k": 3}));
    assert_eq!(rows[1]["y"], json!({"a": {"q": 8}, "b": 2, "k": 3}));
}

/// A rename of a declared object's leaf moves the leaf, as a read of it finds it: the object keeps the rest.
#[test]
fn a_rename_of_a_declared_objects_leaf_moves_it() {
    let plan = ok(vec![
        code(0, &json("o: {a: i64, c: i64, ...};")),
        code(1, "move(o.a, b);"),
    ]);
    let rows = run(&plan, &[r#"{"o":{"a":1,"c":2,"z":3}}"#]);
    assert_eq!(fields(&rows[0]), json!({"b": 1, "o": {"c": 2, "z": 3}}));
}

/// A leaf a later spread lays over an earlier spread's unclaimed fields falls back to the key there, where the leaf
/// holds nothing (rule 3).
#[test]
fn a_spreads_leaf_falls_back_to_an_earlier_spreads_unclaimed_key() {
    let plan = ok(vec![
        code(0, &json("a: {z: i64, ...}; b: {z: i64, ...}; kk: i64;")),
        code(1, "o.k = kk; y = {...a, ...o}; w = {...a, ...b, ...o};"),
    ]);
    let doc = json!({"a": {"z": 0, "k": 1}, "b": {"z": 3, "k": 2}});
    let mut with_kk = doc.clone();
    with_kk["kk"] = json!(5);
    let rows = run(&plan, &[&doc.to_string(), &with_kk.to_string()]);
    assert_eq!(rows[0]["y"], json!({"z": 0, "k": 1}));
    assert_eq!(rows[0]["w"], json!({"z": 3, "k": 2}), "the later spread's key wins");
    assert_eq!(rows[1]["y"], json!({"z": 0, "k": 5}));
}

/// An object a spread in a literal makes under a key keeps what an earlier spread's unclaimed fields hold there, as a
/// write under the key in an assignment does (rule 3). A quoted key holding a dot is one key beside it.
#[test]
fn a_literal_keeps_the_rest_of_an_earlier_spreads_unclaimed_key() {
    let plan = ok(vec![
        code(0, &json("a: {z: i64, ...}; kk: i64;")),
        code(
            1,
            r#"o.k.z = kk; y = {...a, ...o}; p = {...a, "k.c": 2}; q = {...a}; q.k.c = 2;"#,
        ),
    ]);
    let rows = run(&plan, &[r#"{"a":{"z":0,"k":{"z":1,"w":2}}}"#]);
    assert_eq!(rows[0]["y"], json!({"z": 0, "k": {"z": 1, "w": 2}}));
    assert_eq!(rows[0]["p"], json!({"z": 0, "k": {"z": 1, "w": 2}, "k.c": 2}));
    assert_eq!(rows[0]["q"], json!({"z": 0, "k": {"z": 1, "w": 2, "c": 2}}));
}

/// An object literal is one object wherever it is written: as a value, it lays down what it does bound to a name. An
/// object's unclaimed keys lay over the fields before it, and a later leaf falls back to an earlier spread's unclaimed
/// key where it holds nothing.
#[test]
fn an_object_literal_as_a_value_lays_down_what_it_does_bound_to_a_name() {
    let plan = ok(vec![
        code(0, JSON),
        code(
            1,
            r#"user.id = 5; p: {name: str} = {name: null};
               t = {name: "anonymous", ...user}; vt = at([{name: "anonymous", ...user}], 0);
               u = {...user, ...p}; vu = at([{...user, ...p}], 0);"#,
        ),
    ]);
    let rows = run(&plan, &[r#"{"user":{"name":"ann"}}"#, r#"{"user":{}}"#]);
    assert_eq!(rows[0]["t"], json!({"name": "ann", "id": 5}));
    assert_eq!(rows[0]["u"], json!({"name": "ann", "id": 5}));
    assert_eq!(rows[1]["t"], json!({"name": "anonymous", "id": 5}));
    assert_eq!(rows[1]["u"], json!({"id": 5}));
    for row in &rows {
        assert_eq!(row["vt"], row["t"], "{row}");
        assert_eq!(row["vu"], row["u"], "{row}");
    }
}

/// A path under a variant is typed by converting the variant to an open object holding it, `q: {a: str, ...} = q`, at
/// any depth. A declaration of the path is a compile error that names that conversion.
#[test]
fn a_path_under_a_variant_is_converted_not_declared() {
    let docs = [
        r#"{"user": {"name": "ann", "age": 5, "a": {"b": "7", "c": 1}}}"#,
        r#"{"user": {"name": 7}}"#,
        r#"{"user": 3}"#,
    ];
    for (path, ty, to, conversion) in [
        ("q.name", "str", "{name: str, ...}", "q: {name: str, ...} = q"),
        (
            "q.a.b",
            "i64",
            "{a: {b: i64, ...}, ...}",
            "q: {a: {b: i64, ...}, ...} = q",
        ),
    ] {
        let e = err(vec![
            code(0, JSON),
            code(1, &format!("q = $.user; {path}: {ty}; n = {path};")),
        ]);
        let want = format!("`q` is already variant; to change the type of `q` to {to}, write `{conversion} as {to}`");
        assert!(e.contains(&want), "{path}: {e}");
        let rows = run(
            &ok(vec![
                code(0, JSON),
                code(1, &format!("q = $.user; {conversion}; n = {path};")),
            ]),
            &docs,
        );
        assert!(
            rows[0]["n"] == json!("ann") || rows[0]["n"] == json!(7),
            "{conversion}: {rows:?}"
        );
    }
}

/// An open object does not stand where a closed type is declared: a literal that spreads one is open, as is a JSON
/// object, and a variant's keys only the row knows.
#[test]
fn a_declared_closed_type_refuses_an_open_value() {
    for (body, want) in [
        ("function f(): {a: str} { return {...w}; } out = f();", "not {a: str}"),
        (
            "function f(c: bool): {a: str} { return c ? {...w} : {a: \"x\"}; } out = f(true);",
            "not {a: str}",
        ),
        (
            "function f(): {a: str} { return null ?? {...w}; } out = f();",
            "not {a: str}",
        ),
        ("function f(): {a: str} { return {...o}; } out = f();", "not {a: str}"),
        (
            "function f(p: {a: str}): str { return p.a; } out = f(o);",
            "`o` is {a: str, ...}, but `f` takes {a: str}. To drop `o`'s other keys, write `f(o as {a: str})`",
        ),
    ] {
        let e = err(vec![
            code(0, &json("o: {a: str, ...}; pairs: array<{key: str, value: str}>;")),
            build(1, "w"),
            code(2, body),
        ]);
        assert!(e.contains(want), "{body}: {e}");
    }
    ok(vec![
        code(0, &json("o: {a: str, ...};")),
        code(1, "function f(): {a: str, ...} { return {a: \"x\"}; } out = f();"),
    ]);
}

/// A variant holds a value of any type, a time, a span and bytes among them, exactly: written by a key only the row
/// knows, read by one, listed by `entries` and converted back, each is the value it was.
#[test]
fn a_variant_holds_a_time_a_span_and_bytes_exactly() {
    let plan = ok(vec![
        pairs(),
        code(
            1,
            r#"
            let held: {...} = {};
            let kd = "d"; let kt = "t";
            held[kd] = duration(1500, "us");
            held[kt] = parseTime("2024-01-02T03:04:05.123456Z");
            d = held.d; d: duration = d;
            t = held.t; t: datetime = t;
            late = t + d;
            whole = held;
            o: {...} = {};
            o.s = duration(2, "ms");
            s = o[kd == "d" ? "s" : "x"]; s: duration = s;
            e = entries(o);
            spans: {...} = null;
            for (let p of pairs) { spans[p.key] = duration(1, "s"); }
            "#,
        ),
    ]);
    let doc = pairs_doc(&[("a", json!("1"))], json!({}));
    let rows = run(&plan, &[&doc]);
    let row = &rows[0];
    assert_eq!(row["d"], json!(1500), "{row}");
    assert_eq!(row["t"], json!("2024-01-02T03:04:05.123456Z"), "{row}");
    assert_eq!(row["late"], json!("2024-01-02T03:04:05.124956Z"), "{row}");
    assert_eq!(
        row["whole"],
        json!({"d": 1500, "t": "2024-01-02T03:04:05.123456Z"}),
        "{row}"
    );
    assert_eq!(row["s"], json!(2000), "{row}");
    assert_eq!(row["e"], json!([{"key": "s", "value": 2000}]), "{row}");
    assert_eq!(row["spans"], json!({"a": 1000000}), "{row}");
    assert!(row.get("error").is_none(), "{row}");

    // Bytes that are no text are still bytes.
    let plan = ok(vec![code(
        0,
        r#"let held: {...} = {}; let k = "b"; held[k] = _raw; b = held.b; b: bytes = b;"#,
    )]);
    let mut env = Env::new();
    let mut batch = Batch::new(&plan.slot_layout, 1, 0);
    let mut raw = smeltr::core::column::StrBuilder::new(&mut env.pool, smeltr::core::column::VarType::Bytes, 1);
    raw.reserve(&mut env.pool, 2);
    raw.push(b"a\xE9");
    batch.put(smeltr::core::batch::SLOT_RAW, raw.finish(), &mut env.pool);
    let rows = env.run_json(&plan, &mut batch);
    assert_eq!(rows[0]["b"], json!("Yek="), "bytes write as their base64: {}", rows[0]);
    assert!(rows[0].get("error").is_none(), "{}", rows[0]);
    batch.reclaim(&mut env.pool);
}

/// Bytes and text convert through base64, by `as` and by a parser's declared field alike: text that is not base64
/// refuses.
#[test]
fn bytes_and_text_convert_through_base64() {
    let plan = ok(vec![
        code(0, &json("raw: bytes; bad: bytes;")),
        code(
            1,
            r#"let b = "Y2Fm6Q==" as bytes; n = length(b); s = b as str; refused = "not base64!" as bytes;
               parsed = length(raw);"#,
        ),
    ]);
    let rows = run(&plan, &[r#"{"raw":"Y2Fm6Q==","bad":"caf\u00e9"}"#]);
    let row = &rows[0];
    assert_eq!(row["n"], json!(4), "{row}");
    assert_eq!(row["s"], json!("Y2Fm6Q=="), "{row}");
    assert_eq!(row["refused"], Value::Null, "{row}");
    assert_eq!(row["parsed"], json!(4), "{row}");
    assert_eq!(row["bad"], Value::Null, "{row}");
    let refusals = row["smeltr"]["exception"]["exceptions"].to_string();
    assert!(
        refusals.contains("as bytes in bad") && refusals.contains("to bytes in refused"),
        "{row}"
    );
}

/// A first write inside an `if` or a loop binds the name at the top level, typed by every write to it in the
/// construct, and null on a row no arm ran; a declaration there states the type once, and the construct's other
/// writes keep it.
#[test]
fn a_first_write_in_a_construct_binds_the_name_at_the_top_level() {
    let seed =
        r#"c = _raw == "T"; xs = c ? [1, 2] : [0].filter(x => c); pairs = [{key: "k", value: "v"}].filter(p => c);"#;
    let cases: &[(&str, &str, Value, Value)] = &[
        ("if (c) { x = 1; } else { x = \"1\"; }", "x", json!(1), json!("1")),
        ("if (c) { x = 1; }", "x", json!(1), json!(null)),
        ("if (c) { x = 1; } else { x = 2.5; }", "x", json!(1.0), json!(2.5)),
        ("if (c) { x = 1; } else { x = null; }", "x", json!(1), json!(null)),
        ("for (let v of xs) { x = v; }", "x", json!(2), json!(null)),
        ("if (c) { for (let v of xs) { x = v; } }", "x", json!(2), json!(null)),
        (
            "if (c) { if (xs[0] > 1) { x = \"big\"; } else { x = 3; } }",
            "x",
            json!(3),
            json!(null),
        ),
        ("if (c) { let t = 1; x = t; y = x + 1; }", "y", json!(2), json!(null)),
        ("if (c) { x = \"a\"; } x = x + \"b\";", "x", json!("ab"), json!(null)),
        (
            "if (c) { x.z = 1; } else { x.z = 2; }",
            "x",
            json!({"z": 1}),
            json!({"z": 2}),
        ),
        (
            "if (c) { x = {a: 1}; } else { x = {a: 2, b: \"x\"}; }",
            "x",
            json!({"a": 1}),
            json!({"a": 2, "b": "x"}),
        ),
        (
            "for (let p of pairs) { x[p.key] = p.value; }",
            "x",
            json!({"k": "v"}),
            json!(null),
        ),
        (
            "if (c) { x: variant = 1; } else { x = \"1\"; }",
            "x",
            json!(1),
            json!("1"),
        ),
    ];
    for (body, name, on, off) in cases {
        let plan = ok(vec![code(0, seed), code(1, body)]);
        assert_eq!(
            cols(&run(&plan, &["T", "F"]), &[name]),
            vec![vec![on.clone()], vec![off.clone()]],
            "{body}"
        );
    }
    let plan = ok(vec![
        code(0, "c = _raw == \"T\";"),
        Step::If {
            id: 3,
            cond: "c".into(),
            then: vec![code(1, "x = 1;")],
            else_: vec![code(2, "x = \"1\";")],
        },
    ]);
    assert_eq!(cols(&run(&plan, &["T", "F"]), &["x"]), rows![[1], ["1"]]);

    for (body, want) in [
        ("if (c) { x: str = \"1\"; } else { x = 1; }", "`1` is i64, not str"),
        ("if (c) { x = 1; } else { x: str = \"1\"; }", "`1` is i64, not str"),
        (
            "if (c) { x: str = \"1\"; } else { x: i64 = 1; }",
            "`x` is declared twice",
        ),
        (
            "x = 1; if (c) { x: str = \"a\"; }",
            "`x: str` is inside an `if` or a loop",
        ),
        ("if (c) { x = null; }", "`x` has no type"),
        ("if (c) { x = []; }", "`x` has no type"),
    ] {
        let e = err(vec![code(0, seed), code(1, body)]);
        assert!(e.contains(want), "{body}\n  got: {e}");
    }
}

/// Where the record may hold a key of the name, a first write inside an `if` or a loop declares the name from that key,
/// converted, before the construct: a row no write runs on keeps the key.
#[test]
fn a_first_write_in_a_construct_of_a_key_the_record_may_hold_keeps_the_key() {
    for (body, skipped, kept) in [
        (
            "if (flag) { a.b.c = 5; }",
            r#"{"flag":false,"a":{"b":{"c":"9"}}}"#,
            json!({"a": {"b": {"c": 9}}}),
        ),
        ("if (flag) { x = 5; }", r#"{"flag":false,"x":"3"}"#, json!({"x": 3})),
        (
            "if (flag) { o = {key: 1}; }",
            r#"{"flag":false,"o":{"key":"2"}}"#,
            json!({"o": {"key": 2}}),
        ),
        (
            "for (let e of arr) { x = e; }",
            r#"{"arr":[],"x":"4"}"#,
            json!({"x": 4}),
        ),
        (
            "for (let e of arr) { for (let f of arr) { x = f; } }",
            r#"{"arr":[],"x":"4"}"#,
            json!({"x": 4}),
        ),
        (
            "for (let e of arr) { o[\"k\"] = e; }",
            r#"{"arr":[],"o":{"k":"6"}}"#,
            json!({"o": {"k": 6}}),
        ),
    ] {
        let plan = ok(vec![code(0, &json("flag: bool; arr: array<i64>;")), code(1, body)]);
        let row = &run(&plan, &[skipped])[0];
        for (k, v) in kept.as_object().unwrap() {
            assert_eq!(&row[k], v, "{body}\n  got: {row}");
        }
    }
}

/// An open object declared again with a field its unclaimed fields hold reads that field, as does a copy of it, and
/// the object keeps its keys.
#[test]
fn a_redeclared_open_object_reads_a_field_its_unclaimed_fields_held() {
    for redeclare in [
        "o: {a: i64, b: i64, ...} = o as {a: i64, b: i64, ...};",
        "o: {a: i64, b: i64, ...} = o;",
    ] {
        let plan = ok(vec![
            code(0, &json("o: {a: i64, ...};")),
            code(1, &format!("{redeclare} x = o.b; y = o; z = y.b;")),
        ]);
        let out = run(&plan, &[r#"{"o":{"a":1,"b":2,"c":3}}"#]);
        assert_eq!(out[0]["x"], 2, "{redeclare}");
        assert_eq!(out[0]["z"], 2, "{redeclare}");
        assert_eq!(out[0]["o"], json!({"a": 1, "b": 2, "c": 3}), "{redeclare}");
        assert_eq!(out[0]["y"], json!({"a": 1, "b": 2, "c": 3}), "{redeclare}");
    }
}

/// An index null on every row of a batch, as one read past every array's end is, gives no row a position, so every row's insert is null.
#[test]
fn an_insert_at_an_index_null_on_every_row_is_null() {
    let plan = ok(vec![
        code(0, &json("xs: array<i64>;")),
        code(1, "gone = isNull(insert(xs, xs[7], 9));"),
    ]);
    let docs = [r#"{"xs":[1,2]}"#, r#"{"xs":[3]}"#];
    assert_eq!(cols(&run(&plan, &docs), &["gone"]), rows![[true], [true]]);
}

/// `null ?? xs` is still an array where neither side holds a value: a loop over it runs no times.
#[test]
fn a_loop_over_a_coalesce_null_on_every_row_runs_no_times() {
    let plan = ok(vec![
        code(0, &json("xs: array<i64>; ys: array<str>;")),
        code(1, "n = 0; for (let w of (null ?? ([ys])[xs[7]])) { n = n + 1; }"),
    ]);
    let docs = [r#"{"xs":[1],"ys":["a"]}"#, r#"{"xs":[2],"ys":["b","c"]}"#];
    assert_eq!(cols(&run(&plan, &docs), &["n"]), rows![[0], [0]]);
}

/// `s.toString()` is `toString(s)`: of a string, that string.
#[test]
fn to_string_of_a_string_is_that_string_however_written() {
    let plan = ok(vec![
        code(0, &json("s: str;")),
        code(1, "let l = s; a = toString(s); m = s.toString(); n = l.toString();"),
    ]);
    assert_eq!(
        cols(&run(&plan, &[r#"{"s":"x"}"#]), &["a", "m", "n"]),
        rows![["x", "x", "x"]]
    );
}

/// `null ?? rhs` is `rhs`, whether or not `rhs` calls a function that expands to statements.
#[test]
fn null_coalesced_with_a_map_is_the_map() {
    let plan = ok(vec![
        code(0, &json("xs: array<i64>;")),
        code(
            1,
            "m = null ?? map(xs, (v) => v + 1); k = null ?? xs.filter((v) => v > 1);",
        ),
    ]);
    assert_eq!(
        cols(&run(&plan, &[r#"{"xs":[1,2]}"#]), &["m", "k"]),
        rows![[[2, 3], [2]]]
    );
}

/// An array that is null on a row has no elements there, however the rows beside it hold theirs: every loop over it
/// runs no trip, and an unroll makes no row of it.
#[test]
fn a_null_array_has_no_elements_to_loop_over() {
    let plan = ok(vec![
        code(0, &json("bo: bool; xs: array<i64>; ys: array<str>;")),
        code(
            1,
            "let n = bo ? xs : (ys as array<i64>); \
             c = 0; for (let l of n) { c = c + 1; } \
             p: array<i64> = []; for (let l of n) { p = push(p, l); } \
             w = 0; for (let l of n) { if (l > 5) { break; } w = w + 1; }",
        ),
    ]);
    let docs = [
        r#"{"bo":false,"xs":[1],"ys":["a","b"]}"#,
        r#"{"bo":true,"xs":[1,2,3],"ys":["c"]}"#,
    ];
    assert_eq!(
        cols(&run(&plan, &docs), &["c", "p", "w"]),
        rows![[0, [], 0], [3, [1, 2, 3], 3]]
    );
    for (i, doc) in docs.iter().enumerate() {
        assert_eq!(run(&plan, &[doc]), run(&plan, &docs)[i..=i], "row {i} alone");
    }

    let plan = ok(vec![
        code(0, &json("bo: bool; xs: array<i64>; ys: array<str>;")),
        code(1, "a = bo ? xs : (ys as array<i64>); unroll(a, true);"),
    ]);
    assert_eq!(cols(&run(&plan, &docs), &["a"]), rows![[1], [2], [3]]);
}

/// A write whose value fails in a `try` never happens: the name keeps what it held, as when the value goes through a
/// `let` first.
#[test]
fn a_write_that_fails_in_a_try_keeps_the_old_value() {
    let plan = ok(vec![
        code(0, &json("xs: array<i64>;")),
        code(
            1,
            "a = 1; try { a = xs[7]; } catch (e) {} \
             b = 2; try { let t: i64 = xs[7]; b = t; } catch (e) {} \
             o = {p: 3, q: 4}; try { o = {p: 5, q: xs[7] + 1}; } catch (e) {} \
             n = 0; for (let x of xs) { try { n = n + xs[x]; } catch (e) {} }",
        ),
    ]);
    let docs = [r#"{"xs":[1]}"#, r#"{"xs":[0,1,2,3,4,5,6,7]}"#];
    assert_eq!(
        cols(&run(&plan, &docs), &["a", "b", "o", "n"]),
        rows![[1, 2, {"p": 3, "q": 4}, 0], [7, 7, {"p": 5, "q": 8}, 28]]
    );
}

/// A function reads the names where it was written: a local where it is called, however named, is not one it sees.
#[test]
fn a_function_reads_the_names_where_it_was_written() {
    let plan = ok(vec![
        code(0, &json("a: i64; bo: bool;")),
        code(
            1,
            "function g() { a + 1 } let k = 10; function h() { k + a } \
             let f = () => a * 2; \
             if (bo) { let a = 100; let k = 1000; n = g(); m = h(); p = f(); }",
        ),
    ]);
    assert_eq!(
        cols(&run(&plan, &[r#"{"a":1,"bo":true}"#]), &["n", "m", "p"]),
        rows![[2, 11, 2]]
    );

    // A data local of a name an arrow once held is data: a function's name there is not its.
    let plan = ok(vec![
        code(0, &json("x: i64;")),
        code(
            1,
            "function g() { x } if (false) { let f = () => 0; } \
             if (true) { let f = 100; let x = f; n = g(); }",
        ),
    ]);
    assert_eq!(cols(&run(&plan, &[r#"{"x":7}"#]), &["n"]), rows![[7]]);

    // An argument is its value when the call is made, though the body writes the local it came from.
    let plan = ok(vec![code(1, "let a = 1; function g(p) { a = 5; p } n = g(a); m = a;")]);
    assert_eq!(cols(&run(&plan, &["{}"]), &["n", "m"]), rows![[1, 5]]);

    // A `catch` variable is a local: a function called in its block reads the field of that name.
    let plan = ok(vec![
        code(0, &json("e: str; s: str;")),
        code(
            1,
            "function g() { e } let h = (x) => x; \
             try { n: i64 = parseInt(s); } catch (e) { m = g(); k = h(e.kind); }",
        ),
    ]);
    assert_eq!(
        cols(&run(&plan, &[r#"{"e":"field","s":"x"}"#]), &["m", "k"]),
        rows![["field", "CoercionRefused"]]
    );

    // Text in a name the user wrote is that name, whatever it looks like.
    let plan = ok(vec![
        code(0, &json("a__loc1_b: i64;")),
        code(
            1,
            "function g() { a__loc1_b } let a__loc1_b = 2; let ab = 3; n = g() + ab;",
        ),
    ]);
    assert_eq!(cols(&run(&plan, &[r#"{"a__loc1_b":1}"#]), &["n"]), rows![[4]]);

    // A message names the local as the user wrote it.
    let e = err(vec![code(1, "let a = 1; function g() { a } let a = 2; n = g();")]);
    assert!(e.contains("`a` is already declared in this code block"), "{e}");
}

/// A null divisor is no division by zero: the quotient is null, and the row says nothing, alone or in a batch.
#[test]
fn a_float_divided_by_null_throws_nothing() {
    let plan = ok(vec![
        code(0, &json("f: f64;")),
        code(1, "n = 1.0 / sign(f); m = f / 2.0;"),
    ]);
    let docs = [r#"{"f":1.5}"#, "{}"];
    let all = run(&plan, &docs);
    assert_eq!(
        cols(&all, &["n", "m", "smeltr"]),
        rows![[1.0, 0.75, null], [null, null, null]]
    );
    assert_eq!(run(&plan, &docs[1..]), all[1..]);
}
