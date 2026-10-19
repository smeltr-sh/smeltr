//! End-to-end tests for the Lookup function: a real compiled pipeline,
//! `_raw`/`__source.offset`/an Eval-derived field as the join key, a real lookup
//! file on disk, output columns out. Mirrors `grok-parser-tests.rs`'s
//! shape; see `lookup.rs`'s own unit tests for the lower-level coverage
//! (file/table parsing, match-kind/multiplicity semantics, stringification
//! of non-`Str` keys) this builds on.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};

use smeltr::core::batch::Batch;

use smeltr::core::column::{Column, DataType};
use smeltr::core::error::ExceptionKind;
use smeltr::plan::config::{CompileError, LookupAt, LookupFile, LookupSource, PipelineConfig, Step};
use smeltr::plan::plan::{Plan, compile};

use smeltr::plan::topology::SourceMeta;

use crate::harness;
use harness::*;

/// `status` is `__source.offset`; its `status_text` is the `message` the table `codes` has for it as `code`.
const STATUS_TEXT: &str = r#"status = __source.offset; Object.assign($, lookup("codes", status, {column: "code", fields: {status_text: "message"}}));"#;

/// The harness's pipeline, with `tables` as the bundle's lookup tables.
fn pipeline_with(body: Vec<Step>, tables: &[(&str, LookupSource)]) -> PipelineConfig {
    PipelineConfig {
        lookup_tables: tables
            .iter()
            .map(|(id, source)| (id.to_string(), Ok(source.clone())))
            .collect::<HashMap<_, _>>(),
        ..pipeline(body)
    }
}

/// `harness::compile_ok`, with `tables` as the bundle's lookup tables.
fn compile_ok_with(body: Vec<Step>, tables: &[(&str, LookupSource)]) -> Plan {
    compile(&pipeline_with(body, tables), &SourceMeta::KAFKA).unwrap_or_else(|e| {
        panic!(
            "compile failed: {}",
            e.iter().map(|x| x.to_string()).collect::<Vec<_>>().join("; ")
        )
    })
}

/// `harness::compile_err`, with `tables` as the bundle's lookup tables.
fn compile_err_with(body: Vec<Step>, tables: &[(&str, LookupSource)]) -> Vec<CompileError> {
    compile(&pipeline_with(body, tables), &SourceMeta::KAFKA).expect_err("expected a compile error")
}

/// A comma-delimited CSV table read from `path`.
fn csv(path: &str) -> LookupSource {
    LookupSource::Csv {
        file: LookupFile {
            text: path.to_string(),
            at: LookupAt::Path(path.into()),
        },
        delimiter: b',',
    }
}

/// An MMDB table read from `path`.
fn mmdb(path: &str) -> LookupSource {
    LookupSource::Mmdb {
        file: LookupFile {
            text: path.to_string(),
            at: LookupAt::Path(path.into()),
        },
    }
}

/// A table authored in the config.
fn inline(headers: &[&str], rows: &[&[&str]]) -> LookupSource {
    LookupSource::Inline {
        headers: headers.iter().map(|h| h.to_string()).collect(),
        rows: rows.iter().map(|r| r.iter().map(|c| c.to_string()).collect()).collect(),
    }
}

/// A real lookup file on disk -- `contents` written verbatim (a header row
/// plus data rows, `lookup::load_csv`'s own requirement), a unique path
/// per call so parallel tests don't collide. Callers are responsible for
/// cleanup (`std::fs::remove_file`); left in place on an assertion panic
/// is harmless (a stray temp file), not a correctness issue.
fn temp_lookup_file(contents: &str) -> String {
    static COUNTER: AtomicU32 = AtomicU32::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!("smeltr-lookup-integration-{}-{n}.csv", std::process::id()));
    std::fs::write(&path, contents).unwrap();
    path.to_str().unwrap().to_string()
}

fn seed_offset(env: &mut Env, batch: &mut Batch, offsets: &[i64]) {
    let mut c = Column::alloc(&mut env.pool, &DataType::I64, offsets.len());
    c.i64_mut().unwrap().values.copy_from_slice(offsets);
    batch.put(slot_of(SourceMeta::KAFKA, "__source.offset"), c, &mut env.pool);
}

// ---------------------------------------------------------------------------

#[test]
fn a_matching_key_resolves_to_its_table_value() {
    let path = temp_lookup_file("code,message\n200,ok\n404,not found\n500,server error\n");
    let plan = compile_ok_with(vec![code(0, STATUS_TEXT)], &[("codes", csv(&path))]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 2, 0);
    seed_offset(&mut env, &mut b, &[200, 404]);
    seed_raw_empty(&mut env, &mut b, 2);

    let rows = env.run(&plan, &mut b);
    assert_eq!(
        parsed(&rows[0]),
        parsed(br#"{"_raw":"","status":200,"status_text":"ok"}"#)
    );
    assert_eq!(
        parsed(&rows[1]),
        parsed(br#"{"_raw":"","status":404,"status_text":"not found"}"#)
    );
    b.reclaim(&mut env.pool);
    std::fs::remove_file(path).unwrap();
}

/// A key computed by arithmetic is a `number`, and a number is a key.
#[test]
fn a_computed_number_is_a_lookup_key() {
    let path = temp_lookup_file("code,message\n200,ok\n404,not found\n");
    let plan = compile_ok_with(
        vec![code(
            0,
            r#"status = __source.offset + 0; Object.assign($, lookup("codes", status, {column: "code", fields: {status_text: "message"}}));"#,
        )],
        &[("codes", csv(&path))],
    );
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 2, 0);
    seed_offset(&mut env, &mut b, &[200, 404]);
    seed_raw_empty(&mut env, &mut b, 2);

    let rows = env.run(&plan, &mut b);
    assert_eq!(
        parsed(&rows[0]),
        parsed(br#"{"_raw":"","status":200,"status_text":"ok"}"#)
    );
    assert_eq!(
        parsed(&rows[1]),
        parsed(br#"{"_raw":"","status":404,"status_text":"not found"}"#)
    );
    b.reclaim(&mut env.pool);
    std::fs::remove_file(path).unwrap();
}

#[test]
fn a_miss_with_no_default_is_null_not_a_failure() {
    let path = temp_lookup_file("code,message\n200,ok\n");
    let plan = compile_ok_with(vec![code(0, STATUS_TEXT)], &[("codes", csv(&path))]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_offset(&mut env, &mut b, &[999]);
    seed_raw_empty(&mut env, &mut b, 1);

    let rows = env.run(&plan, &mut b);
    assert_eq!(parsed(&rows[0]), parsed(br#"{"_raw":"","status":999}"#));
    assert!(
        env.errors.entries().is_empty(),
        "a miss is unremarkable, not an exception"
    );
    b.reclaim(&mut env.pool);
    std::fs::remove_file(path).unwrap();
}

#[test]
fn a_miss_with_a_default_uses_it() {
    let path = temp_lookup_file("code,message\n200,ok\n");
    let plan = compile_ok_with(
        vec![code(
            0,
            r#"status = __source.offset; Object.assign($, lookup("codes", status, {column: "code", fields: {status_text: ["message", "unknown"]}}));"#,
        )],
        &[("codes", csv(&path))],
    );
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_offset(&mut env, &mut b, &[999]);
    seed_raw_empty(&mut env, &mut b, 1);

    let rows = env.run(&plan, &mut b);
    assert_eq!(
        parsed(&rows[0]),
        parsed(br#"{"_raw":"","status":999,"status_text":"unknown"}"#)
    );
    b.reclaim(&mut env.pool);
    std::fs::remove_file(path).unwrap();
}

#[test]
fn a_quoted_lookup_value_can_contain_the_delimiter() {
    let path = temp_lookup_file("code,message\n200,\"ok, all good\"\n");
    let plan = compile_ok_with(vec![code(0, STATUS_TEXT)], &[("codes", csv(&path))]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_offset(&mut env, &mut b, &[200]);
    seed_raw_empty(&mut env, &mut b, 1);

    let rows = env.run(&plan, &mut b);
    assert_eq!(
        parsed(&rows[0]),
        parsed(br#"{"_raw":"","status":200,"status_text":"ok, all good"}"#)
    );
    b.reclaim(&mut env.pool);
    std::fs::remove_file(path).unwrap();
}

#[test]
fn chaining_two_lookups_works_like_any_other_field_reference() {
    let severity_path = temp_lookup_file("message,level\nok,low\nnot found,high\n");
    let text_path = temp_lookup_file("code,message\n200,ok\n404,not found\n");
    let plan = compile_ok_with(
        vec![code(
            0,
            &(STATUS_TEXT.to_owned()
                + r#" Object.assign($, lookup("severities", status_text, {column: "message", fields: {severity: "level"}}));"#),
        )],
        &[("codes", csv(&text_path)), ("severities", csv(&severity_path))],
    );
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_offset(&mut env, &mut b, &[404]);
    seed_raw_empty(&mut env, &mut b, 1);

    let rows = env.run(&plan, &mut b);
    // Live fields serialize alphabetically (`plan::output::run_to_json`'s
    // own convention, unrelated to declaration order): severity < status <
    // status_text.
    assert_eq!(
        parsed(&rows[0]),
        parsed(br#"{"_raw":"","severity":"high","status":404,"status_text":"not found"}"#)
    );
    b.reclaim(&mut env.pool);
    std::fs::remove_file(severity_path).unwrap();
    std::fs::remove_file(text_path).unwrap();
}

#[test]
fn multiple_outputs_land_in_their_own_fields() {
    let path = temp_lookup_file("ip,city,country\n10.0.0.1,Springfield,US\n");
    let plan = compile_ok_with(
        vec![code(
            0,
            r#"client_ip = "10.0.0.1"; geo = lookup("geo", client_ip, {column: "ip", fields: {city: "city", country: "country"}});"#,
        )],
        &[("geo", csv(&path))],
    );
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_raw_empty(&mut env, &mut b, 1);

    let rows = env.run(&plan, &mut b);
    assert_eq!(
        parsed(&rows[0]),
        parsed(br#"{"_raw":"","client_ip":"10.0.0.1","geo":{"city":"Springfield","country":"US"}}"#)
    );
    b.reclaim(&mut env.pool);
    std::fs::remove_file(path).unwrap();
}

#[test]
fn a_missing_lookup_file_is_a_compile_error() {
    let errs = compile_err_with(
        vec![code(
            0,
            r#"Object.assign($, lookup("codes", __source.offset, {column: "code", fields: {out: "message"}}));"#,
        )],
        &[("codes", csv("/does/not/exist/smeltr-lookup.csv"))],
    );
    assert!(!errs.is_empty());
}

#[test]
fn a_malformed_lookup_file_is_a_compile_error() {
    let path = temp_lookup_file("only-one-header\nonly-one-field-here\n");
    let errs = compile_err_with(
        vec![code(
            0,
            r#"Object.assign($, lookup("t", __source.offset, {column: "only-one-header", fields: {out: "nosuch"}}));"#,
        )],
        &[("t", csv(&path))],
    );
    assert!(!errs.is_empty());
    std::fs::remove_file(path).unwrap();
}

#[test]
fn an_unbound_input_field_is_a_compile_error() {
    let path = temp_lookup_file("a,b\n1,2\n");
    let errs = compile_err_with(
        vec![code(
            0,
            r#"Object.assign($, lookup("t", no_such_field, {column: "a", fields: {out: "b"}}));"#,
        )],
        &[("t", csv(&path))],
    );
    assert!(!errs.is_empty());
    std::fs::remove_file(path).unwrap();
}

#[test]
fn an_unknown_output_column_is_a_compile_error() {
    let path = temp_lookup_file("a,b\n1,2\n");
    let errs = compile_err_with(
        vec![code(
            0,
            r#"Object.assign($, lookup("t", __source.offset, {column: "a", fields: {out: "no_such_column"}}));"#,
        )],
        &[("t", csv(&path))],
    );
    assert!(!errs.is_empty());
    std::fs::remove_file(path).unwrap();
}

// -- typed outputs -----------------------------------------------------------

/// A key under a variant is a variant. A lookup reads its key, so a variant one is refused with the typed assignment
/// to add; given a type, it matches. A `null` key belongs to every type: it compiles and misses on every row. `m` is
/// the variant an open `{...}` holds under a key only the row knows.
#[test]
fn a_variant_key_is_refused_until_it_is_given_a_type() {
    let json = || code(0, "pairs: array<{key: str, value: str}>; Object.assign($, json(_raw));");
    let table = |id| {
        code(
            id,
            r#"Object.assign($, lookup("hosts", ip, {column: "ip", first: true, fields: {name: "name"}}));"#,
        )
    };
    let hosts = [("hosts", inline(&["ip", "name"], &[&["10.0.0.1", "gw"]]))];
    let build = || {
        code(
            1,
            "let w: {...} = {}; let k = \"v\"; w[k] = fromEntries(pairs); m = w.v; ip = m.ip;",
        )
    };

    let errs = compile_err_with(vec![json(), build(), table(2)], &hosts);
    assert!(
        errs.iter()
            .any(|e| e.to_string().contains("lookup input `ip` is a variant")
                && e.to_string().contains("`ip: str = ip;`")),
        "{errs:?}"
    );

    let plan = compile_ok_with(vec![json(), build(), code(2, "ip: str = ip;"), table(3)], &hosts);
    let mut env = Env::new();
    let mut b = raw_batch(
        &plan,
        &[
            r#"{"pairs":[{"key":"ip","value":"10.0.0.1"}]}"#,
            r#"{"pairs":[{"key":"ip","value":"10.0.0.2"}]}"#,
        ],
        &mut env.pool,
    );
    let rows = env.run_json(&plan, &mut b);
    assert_eq!(rows[0]["name"], "gw", "{}", rows[0]);
    assert!(rows[1].get("name").is_none(), "{}", rows[1]);
    b.reclaim(&mut env.pool);

    let plan = compile_ok_with(vec![code(0, "ip: str = null;"), table(1)], &hosts);
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_raw_empty(&mut env, &mut b, 1);
    let rows = env.run_json(&plan, &mut b);
    assert!(rows[0].get("name").is_none(), "{}", rows[0]);
    b.reclaim(&mut env.pool);
}

#[test]
fn a_typed_output_lands_as_its_type_and_a_miss_default_does_too() {
    let apps = inline(
        &["name", "risk", "sanctioned", "weight", "seen"],
        &[&["ssl", "4", "true", "0.5", "2026-03-01T00:00:00Z"]],
    );
    let plan = compile_ok_with(
        vec![code(
            0,
            r#"app = __source.offset == 0 ? "ssl" : "nothing";
            risk: i64; ok: bool; w: f64; seen: datetime; matched: str;
            Object.assign($, lookup("apps", app, {column: "name", first: true,
                fields: {risk: ["risk", "0"], ok: "sanctioned", w: "weight", seen: "seen", matched: "name"}}));
            next = risk + 1;"#,
        )],
        &[("apps", apps)],
    );
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 2, 0);
    seed_offset(&mut env, &mut b, &[0, 1]);
    seed_raw_empty(&mut env, &mut b, 2);

    let rows = env.run_json(&plan, &mut b);
    assert_eq!(rows[0]["risk"], 4, "{}", rows[0]);
    assert_eq!(rows[0]["next"], 5, "an i64 output adds like one");
    assert_eq!(rows[0]["ok"], true);
    assert_eq!(rows[0]["w"], 0.5);
    assert_eq!(rows[0]["seen"], "2026-03-01T00:00:00.000000Z");
    assert_eq!(rows[0]["matched"], "ssl", "the key column is an output like any other");
    assert_eq!(rows[1]["risk"], 0, "the default is typed too: {}", rows[1]);
    assert_eq!(rows[1]["next"], 1);
    assert!(rows[1].get("ok").is_none(), "a miss with no default is null");
    assert!(env.errors.entries().is_empty(), "{:?}", env.errors.entries());
    b.reclaim(&mut env.pool);
}

#[test]
fn every_match_under_all_is_typed() {
    let plan = compile_ok_with(
        vec![code(
            0,
            r#"k = "a"; ns: array<i64>; Object.assign($, lookup("t", k, {column: "k", all: true, fields: {ns: "n"}}));"#,
        )],
        &[("t", inline(&["k", "n"], &[&["a", "1"], &["a", "2"], &["b", "3"]]))],
    );
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_raw_empty(&mut env, &mut b, 1);
    let rows = env.run_json(&plan, &mut b);
    assert_eq!(rows[0]["ns"], serde_json::json!([1, 2]), "{}", rows[0]);
    b.reclaim(&mut env.pool);
}

#[test]
fn a_row_table_cell_that_does_not_fit_the_type_is_a_compile_error() {
    let errs = compile_err_with(
        vec![code(
            0,
            r#"app = "ssl"; risk: i64; Object.assign($, lookup("apps", app, {column: "name", first: true, fields: {risk: "risk"}}));"#,
        )],
        &[("apps", inline(&["name", "risk"], &[&["ssl", "4"], &["dns", "n/a"]]))],
    );
    let msg = &errs[0].message;
    assert!(msg.contains("`risk` is i64"), "{msg}");
    assert!(msg.contains("\"n/a\" at row 2"), "{msg}");
}

#[test]
fn an_mmdb_value_that_does_not_fit_is_null_with_a_field_parse_failure() {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/src/lookup/test-data/every-type.mmdb");
    let plan = compile_ok_with(
        vec![code(
            0,
            r#"ip = "10.1.5.5"; i: i64; b: bool; f: f64; too_big: i64;
            Object.assign($, lookup("geo", ip, {match: "cidr", fields: {i: "i", b: "bt", f: "f64", too_big: "u64"}}));"#,
        )],
        &[("geo", mmdb(path))],
    );
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_raw_empty(&mut env, &mut b, 1);
    let rows = env.run_json(&plan, &mut b);
    assert_eq!(rows[0]["i"], -5, "{}", rows[0]);
    assert_eq!(rows[0]["b"], true);
    assert_eq!(rows[0]["f"], -97.822);
    assert!(rows[0].get("too_big").is_none(), "{}", rows[0]);
    let failures: Vec<_> = env
        .errors
        .entries()
        .iter()
        .filter(|a| matches!(a.kind, ExceptionKind::UnparsableField { .. }))
        .collect();
    assert_eq!(failures.len(), 1, "{failures:?}");
    assert_eq!(failures[0].row, 0);
    b.reclaim(&mut env.pool);
}

#[test]
fn a_table_with_two_columns_of_one_name_is_a_compile_error() {
    let errs = compile_err_with(
        vec![code(
            0,
            r#"k = "a"; Object.assign($, lookup("t", k, {column: "k", first: true, fields: {v: "v"}}));"#,
        )],
        &[("t", inline(&["k", "v", "v"], &[&["a", "1", "2"]]))],
    );
    assert!(errs[0].message.contains("two columns named `v`"), "{}", errs[0].message);
}

#[test]
fn an_output_type_that_is_not_one_value_is_a_compile_error() {
    let errs = compile_err_with(
        vec![code(
            0,
            r#"k = "a"; v: bytes; Object.assign($, lookup("t", k, {column: "k", first: true, fields: {v: "v"}}));"#,
        )],
        &[("t", inline(&["k", "v"], &[]))],
    );
    assert!(errs[0].message.contains("`v` is bytes"), "{}", errs[0].message);
}

// -- under a guard -------------------------------------------------------------

/// `k` is "x" and `y` is "prior" on every row; `a` is true, false and null on rows 0, 1 and 2.
const PRIOR_AND_GUARD: &str =
    r#"k = "x"; y = "prior"; a = __source.offset == 0 ? true : (__source.offset == 1 ? false : null);"#;

/// Looks `k` up in the table `found` (`found_table`), writing its `v` to `y`.
const FOUND_INTO_Y: &str = r#"Object.assign($, lookup("found", k, {column: "k", fields: {y: "v"}}))"#;

/// The table `FOUND_INTO_Y` names: "x" maps to "found".
fn found_table() -> (&'static str, LookupSource) {
    ("found", inline(&["k", "v"], &[&["x", "found"]]))
}

fn run_three(plan: &Plan) -> (Vec<serde_json::Value>, Env) {
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 3, 0);
    seed_offset(&mut env, &mut b, &[0, 1, 2]);
    seed_raw_empty(&mut env, &mut b, 3);
    let rows = env.run_json(plan, &mut b);
    b.reclaim(&mut env.pool);
    (rows, env)
}

/// A lookup under a guard writes only the rows the guard takes, as a guarded assignment does.
#[test]
fn a_filtered_lookup_writes_only_the_rows_its_filter_takes() {
    let body = format!("{PRIOR_AND_GUARD} if (a) {{ {FOUND_INTO_Y} }}");
    let plan = compile_ok_with(vec![code(0, &body)], &[found_table()]);
    let (rows, _) = run_three(&plan);
    let y: Vec<_> = rows.iter().map(|r| r["y"].clone()).collect();
    assert_eq!(y, ["found", "prior", "prior"]);
}

#[test]
fn a_lookup_in_an_else_writes_only_the_rows_the_else_takes() {
    let body = format!("{PRIOR_AND_GUARD} t: i64 = null; if (a) {{ t = 1; }} else {{ {FOUND_INTO_Y} }}");
    let plan = compile_ok_with(vec![code(0, &body)], &[found_table()]);
    let (rows, _) = run_three(&plan);
    let y: Vec<_> = rows.iter().map(|r| r["y"].clone()).collect();
    assert_eq!(y, ["prior", "found", "found"]);
}

/// A value that does not fit its type marks the row the filter takes, and only that row.
#[test]
fn a_filtered_mmdb_lookup_marks_only_the_row_its_filter_takes() {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/src/lookup/test-data/every-type.mmdb");
    let body = format!(
        r#"{PRIOR_AND_GUARD} ip = "10.1.5.5"; too_big = 7;
        if (a) {{ Object.assign($, lookup("geo", ip, {{match: "cidr", fields: {{too_big: "u64"}}}})) }}"#
    );
    let plan = compile_ok_with(vec![code(0, &body)], &[("geo", mmdb(path))]);
    let (rows, env) = run_three(&plan);
    // A value that does not fit is null, so the field keeps what it held, as a parse's field does.
    assert_eq!(rows[0]["too_big"], 7, "{}", rows[0]);
    assert_eq!(rows[1]["too_big"], 7);
    assert_eq!(rows[2]["too_big"], 7);
    let marked: Vec<u32> = env
        .errors
        .entries()
        .iter()
        .filter(|a| matches!(a.kind, ExceptionKind::UnparsableField { .. }))
        .map(|a| a.row)
        .collect();
    assert_eq!(marked, [0]);
}

/// A key every record of a batch shares, a `__source` field held as one value, matches on each row.
#[test]
fn a_shared_source_key_matches_on_each_row() {
    use smeltr::core::column::Scalar;
    let body = r#"Object.assign($, lookup("owners", __source.scope.name, {column: "lib", fields: {team: "team"}}));"#;
    let plan = compile(
        &pipeline_with(
            vec![code(0, body)],
            &[("owners", inline(&["lib", "team"], &[&["io", "infra"]]))],
        ),
        &SourceMeta::OTLP_LOGS,
    )
    .unwrap_or_else(|e| panic!("{e:?}"));
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 2, 0);
    seed_raw_empty(&mut env, &mut b, 2);
    let shared = Column::constant(Scalar::Str("io".into()), 2);
    b.put(
        slot_of(SourceMeta::OTLP_LOGS, "__source.scope.name"),
        shared,
        &mut env.pool,
    );

    let rows = env.run_json(&plan, &mut b);
    assert_eq!(rows[0]["team"], "infra", "{}", rows[0]);
    assert_eq!(rows[1]["team"], "infra", "{}", rows[1]);
    b.reclaim(&mut env.pool);
}
