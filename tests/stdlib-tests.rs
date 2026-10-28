//! End-to-end tests for the expression language's stdlib `Call`s and regex
//! `match`/`replace`: a real compiled pipeline, code steps whose
//! expressions exercise every `ops::stdfuncs::StdFunc` variant plus regex
//! literals, output JSON out. Mirrors `lookup-tests.rs`'s shape.
//!
//! `src/ops/stdfuncs.rs` and `tests/ops.rs`'s stdlib section cover the
//! kernels in isolation; this file proves the whole path — parse, resolve
//! (including compile-time regex interning), and eval — agrees end to end.

use smeltr::core::batch::Batch;
use smeltr::plan::topology::SourceMeta;

use smeltr::core::column::{Column, DataType};
use smeltr::core::id::FunctionId;
use smeltr::plan::compile::compile;
use smeltr::plan::config::{CompileError, PipelineConfig, Step};

use crate::harness;
use harness::*;

fn compile_err(body: Vec<Step>) -> Vec<CompileError> {
    let p = PipelineConfig {
        id: 0,
        name: "test".into(),
        body,
        step_ids: Vec::new(),
        lookup_tables: Default::default(),
        grok_patterns: Default::default(),
    };
    compile(&p, &SourceMeta::KAFKA).expect_err("expected a compile error")
}

/// Stands in for varying per-row input the same way `pipeline-integration-
/// tests.rs` does — no `Parser` step needed for these expressions.
fn seed_offset(env: &mut Env, batch: &mut Batch, vals: &[i64]) {
    let mut c = Column::alloc(&mut env.pool, &DataType::I64, vals.len());
    c.i64_mut().unwrap().values.copy_from_slice(vals);
    batch.put(slot_of(SourceMeta::KAFKA, "__source.offset"), c, &mut env.pool);
}

// ---------------------------------------------------------------------------

#[test]
fn isnull_length_and_tostring_work_end_to_end() {
    let plan = compile_ok(vec![
        code(0, "status = __source.offset;"),
        code(
            1,
            r#"
            label = status >= 400 ? "error case" : "ok case";
            always_null = status == 999 ? "unreachable" : null;
            label_is_null = isNull(label);
            always_null_is_null = isNull(always_null);
            label_len = length(label);
            status_str = toString(status);
            "#,
        ),
    ]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 2, 0);
    seed_offset(&mut env, &mut b, &[200, 404]);

    let rows = env.run_json(&plan, &mut b);
    assert_eq!(rows[0]["label"], "ok case");
    assert_eq!(rows[0]["label_is_null"], false);
    assert_eq!(rows[0]["always_null_is_null"], true);
    assert!(rows[0].get("always_null").is_none(), "null fields are omitted");
    assert_eq!(rows[0]["label_len"], 7);
    assert_eq!(rows[0]["status_str"], "200");
    assert_eq!(rows[1]["label"], "error case");
    assert_eq!(rows[1]["label_len"], 10);
    assert_eq!(rows[1]["status_str"], "404");
    b.reclaim(&mut env.pool);
}

#[test]
fn parseint_parsefloat_and_round_work_end_to_end() {
    let plan = compile_ok(vec![code(
        0,
        r#"
        good_int = parseInt("  42  ");
        bad_int = parseInt("nope");
        hex = parseHex("0x1f");
        bare_hex = parseHex("ff");
        bad_hex = parseHex("zz");
        good_float = parseFloat("3.5");
        rounded = round(2.6);
        "#,
    )]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_offset(&mut env, &mut b, &[0]);

    let rows = env.run_json(&plan, &mut b);
    assert_eq!(rows[0]["good_int"], 42);
    assert_eq!(rows[0]["hex"], 31);
    assert_eq!(rows[0]["bare_hex"], 255);
    assert!(rows[0].get("bad_hex").is_none(), "not hex: null");
    assert!(rows[0].get("bad_int").is_none(), "null fields are omitted");
    assert_eq!(rows[0]["good_float"], 3.5);
    assert_eq!(rows[0]["rounded"], 3);
    b.reclaim(&mut env.pool);
}

#[test]
fn ip_functions_work_end_to_end() {
    let plan = compile_ok(vec![code(
        0,
        r#"
        is_ip = isIp("10.1.2.3");
        not_ip = "10.1.2".isIp();
        version = ipVersion("::1");
        normal = ipNormalize("2001:0DB8::0001");
        scope = "192.168.1.1".ipScope();
        in_one = inCidr("10.1.2.3", "10.0.0.0/8");
        in_many = "10.1.2.3".inCidr("192.168.0.0/16", "10.0.0.0/8");
        in_none = inCidr("10.1.2.3", "192.168.0.0/16");
        bad_block = inCidr("10.1.2.3", "10.0.0.0/40");
        network = cidrNetwork("10.1.2.3", 24);
        "#,
    )]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_offset(&mut env, &mut b, &[0]);

    let rows = env.run_json(&plan, &mut b);
    assert_eq!(rows[0]["is_ip"], true);
    assert_eq!(rows[0]["not_ip"], false);
    assert_eq!(rows[0]["version"], 6);
    assert_eq!(rows[0]["normal"], "2001:db8::1");
    assert_eq!(rows[0]["scope"], "private");
    assert_eq!(rows[0]["in_one"], true);
    assert_eq!(rows[0]["in_many"], true);
    assert_eq!(rows[0]["in_none"], false);
    assert!(rows[0].get("bad_block").is_none(), "a block that does not parse: null");
    assert_eq!(rows[0]["network"], "10.1.2.0/24");
    b.reclaim(&mut env.pool);
}

#[test]
fn regex_match_and_replace_work_end_to_end() {
    let plan = compile_ok(vec![
        code(0, "status = __source.offset;"),
        code(
            1,
            r#"
            label = status >= 400 ? "error case" : "ok case";
            is_error = test(label, /error/);
            renamed = replaceAll(label, /case/, "scenario");
            "#,
        ),
    ]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 2, 0);
    seed_offset(&mut env, &mut b, &[200, 404]);

    let rows = env.run_json(&plan, &mut b);
    assert_eq!(rows[0]["is_error"], false);
    assert_eq!(rows[0]["renamed"], "ok scenario");
    assert_eq!(rows[1]["is_error"], true);
    assert_eq!(rows[1]["renamed"], "error scenario");
    b.reclaim(&mut env.pool);
}

#[test]
fn regex_match_is_case_insensitive_with_the_i_flag() {
    let plan = compile_ok(vec![code(
        0,
        r#"
        label = "ERROR: disk full";
        matched = test(label, /error/i);
        "#,
    )]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_offset(&mut env, &mut b, &[0]);

    let rows = env.run_json(&plan, &mut b);
    assert_eq!(rows[0]["matched"], true);
    b.reclaim(&mut env.pool);
}

#[test]
fn a_bare_regex_literal_outside_match_replace_is_a_compile_error() {
    let errs = compile_err(vec![code(0, r#"hit = "x" == /x/;"#)]);
    assert!(
        errs.iter().any(|e| e.to_string().contains("match, replace")),
        "{errs:?}"
    );
}

#[test]
fn an_unknown_stdlib_function_is_a_compile_error() {
    let errs = compile_err(vec![code(0, "hit = totallyMadeUp(1);")]);
    assert!(errs.iter().any(|e| e.to_string().contains("totallyMadeUp")), "{errs:?}");
}

#[test]
fn string_methods_work_end_to_end() {
    let plan = compile_ok(vec![
        code(0, "status = __source.offset;"),
        code(1, r#"label = status >= 400 ? "  Error Case  " : "ok case";"#),
        code(
            2,
            r#"
            trimmed = trim(label);
            upper = toUpperCase(label);
            lower = toLowerCase(label);
            first_word = slice(trim(label), 0, indexOf(trim(label), " "));
            has_error = includes(label, "Error");
            starts_ok = startsWith(label, "ok");
            ends_case = endsWith(trim(label), "Case");
            "#,
        ),
    ]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 2, 0);
    seed_offset(&mut env, &mut b, &[200, 404]);

    let rows = env.run_json(&plan, &mut b);
    assert_eq!(rows[0]["trimmed"], "ok case");
    assert_eq!(rows[0]["upper"], "OK CASE");
    assert_eq!(rows[0]["lower"], "ok case");
    assert_eq!(rows[0]["first_word"], "ok");
    assert_eq!(rows[0]["has_error"], false);
    assert_eq!(rows[0]["starts_ok"], true);
    assert_eq!(rows[0]["ends_case"], false);

    assert_eq!(rows[1]["trimmed"], "Error Case");
    assert_eq!(rows[1]["upper"], "  ERROR CASE  ");
    assert_eq!(rows[1]["first_word"], "Error");
    assert_eq!(rows[1]["has_error"], true);
    assert_eq!(rows[1]["starts_ok"], false);
    assert_eq!(rows[1]["ends_case"], true);
    b.reclaim(&mut env.pool);
}

#[test]
fn slice_supports_negative_indices_end_to_end() {
    let plan = compile_ok(vec![code(
        0,
        r#"
        s = "hello world";
        last_word = slice(s, -5);
        first_word = slice(s, 0, -6);
        "#,
    )]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_offset(&mut env, &mut b, &[0]);

    let rows = env.run_json(&plan, &mut b);
    assert_eq!(rows[0]["last_word"], "world");
    assert_eq!(rows[0]["first_word"], "hello");
    b.reclaim(&mut env.pool);
}

#[test]
fn string_predicate_on_a_non_text_argument_is_a_compile_error() {
    let errs = compile_err(vec![code(
        0,
        r#"
        n = 1;
        hit = startsWith(n, "x");
        "#,
    )]);
    assert!(errs.iter().any(|e| e.to_string().contains("startsWith")), "{errs:?}");
}

#[test]
fn parse_time_auto_detect_works_end_to_end_and_serializes_as_iso8601() {
    let plan = compile_ok(vec![code(
        0,
        r#"
        raw = "2026-08-25T12:00:00.000000Z";
        ts = parseTime(raw);
        "#,
    )]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_offset(&mut env, &mut b, &[0]);

    let rows = env.run_json(&plan, &mut b);
    assert_eq!(rows[0]["ts"], "2026-08-25T12:00:00.000000Z");
    b.reclaim(&mut env.pool);
}

#[test]
fn parse_time_with_explicit_format_end_to_end() {
    let plan = compile_ok(vec![code(
        0,
        r#"
        raw = "10/Oct/2000:13:55:36 -0700";
        ts = parseTime(raw, "%d/%b/%Y:%H:%M:%S %z");
        "#,
    )]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_offset(&mut env, &mut b, &[0]);

    let rows = env.run_json(&plan, &mut b);
    assert_eq!(rows[0]["ts"], "2000-10-10T20:55:36.000000Z");
    b.reclaim(&mut env.pool);
}

#[test]
fn parsed_times_support_datetime_arithmetic_end_to_end() {
    let plan = compile_ok(vec![code(
        0,
        r#"
        a = parseTime("2026-08-25T12:00:00Z");
        b = parseTime("2026-08-25T12:00:05Z");
        elapsed = b - a;
        "#,
    )]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_offset(&mut env, &mut b, &[0]);

    let rows = env.run_json(&plan, &mut b);
    // `Duration` serializes as its microsecond count.
    assert_eq!(rows[0]["elapsed"], 5_000_000i64);
    b.reclaim(&mut env.pool);
}

#[test]
fn parse_time_auto_detect_handles_a_variety_of_common_shapes_end_to_end() {
    let plan = compile_ok(vec![code(
        0,
        r#"
        iso = parseTime("2026-08-25T15:30:00Z");
        space = parseTime("2026-08-25 15:30:00");
        us_slash = parseTime("08/25/2026 3:30:00 PM");
        written = parseTime("August 25, 2026 15:30:00");
        rfc2822 = parseTime("Tue, 25 Aug 2026 15:30:00 GMT");
        "#,
    )]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_offset(&mut env, &mut b, &[0]);

    let rows = env.run_json(&plan, &mut b);
    let expected = "2026-08-25T15:30:00.000000Z";
    assert_eq!(rows[0]["iso"], expected);
    assert_eq!(rows[0]["space"], expected);
    assert_eq!(rows[0]["us_slash"], expected);
    assert_eq!(rows[0]["written"], expected);
    assert_eq!(rows[0]["rfc2822"], expected);
    b.reclaim(&mut env.pool);
}

#[test]
fn parse_time_unparseable_text_is_null_and_omitted_from_output() {
    let plan = compile_ok(vec![code(
        0,
        r#"
        raw = "not a timestamp";
        ts = parseTime(raw);
        "#,
    )]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_offset(&mut env, &mut b, &[0]);

    let rows = env.run_json(&plan, &mut b);
    assert!(rows[0].get("ts").is_none());
    b.reclaim(&mut env.pool);
}

#[test]
fn math_stdlib_works_end_to_end() {
    let plan = compile_ok(vec![
        code(0, "status = __source.offset;"),
        code(
            1,
            "
            delta = abs(status - 250);
            clamped_low = max(status, 250);
            clamped_high = min(status, 250);
            ratio_floor = floor(status / 3.0);
            ratio_ceil = ceil(status / 3.0);
            ",
        ),
    ]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 2, 0);
    seed_offset(&mut env, &mut b, &[200, 500]);

    let rows = env.run_json(&plan, &mut b);
    assert_eq!(rows[0]["delta"], 50);
    assert_eq!(rows[0]["clamped_low"], 250);
    assert_eq!(rows[0]["clamped_high"], 200);
    assert_eq!(rows[0]["ratio_floor"], 66);
    assert_eq!(rows[0]["ratio_ceil"], 67);

    assert_eq!(rows[1]["delta"], 250);
    assert_eq!(rows[1]["clamped_low"], 500);
    assert_eq!(rows[1]["clamped_high"], 250);
    b.reclaim(&mut env.pool);
}

/// The result's number type follows the arguments, not the winner: the first document decides an Elasticsearch
/// mapping. `== 1.0` also matches the integer 1, so each integer winner is checked for a float as well.
#[test]
fn min_max_widen_to_f64_end_to_end_on_a_mixed_pair() {
    let plan = compile_ok(vec![code(
        0,
        "
        i = 1;
        f = 1.5;
        lo = min(i, f);
        hi = max(i, f);
        top = max(2, f);
        ",
    )]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_offset(&mut env, &mut b, &[0]);

    let rows = env.run_json(&plan, &mut b);
    assert_eq!(rows[0]["lo"], 1.0);
    assert_eq!(rows[0]["hi"], 1.5);
    assert_eq!(rows[0]["top"], 2.0);
    for field in ["lo", "top"] {
        assert!(rows[0][field].is_f64(), "{field}: {}", rows[0]);
    }
    b.reclaim(&mut env.pool);
}

#[test]
fn min_on_a_non_numeric_argument_is_a_compile_error() {
    let errs = compile_err(vec![code(
        0,
        r#"
        s = "x";
        hit = min(s, 1);
        "#,
    )]);
    assert!(errs.iter().any(|e| e.to_string().contains("min")));
}

#[test]
fn datetime_parts_work_end_to_end() {
    let plan = compile_ok(vec![code(
        0,
        r#"
        t = parseTime("2026-08-25T12:30:45Z");
        y = year(t);
        mo = month(t);
        d = day(t);
        h = hour(t);
        mi = minute(t);
        s = second(t);
        dow = dayOfWeek(t);
        iso = toString(t);
        span = toString(t - parseTime("2026-08-25T12:30:44Z"));
        bad_day = parseTime("2026-02-30");
        "#,
    )]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_offset(&mut env, &mut b, &[0]);

    let rows = env.run_json(&plan, &mut b);
    assert_eq!(rows[0]["y"], 2026);
    assert_eq!(rows[0]["mo"], 8);
    assert_eq!(rows[0]["d"], 25);
    assert_eq!(rows[0]["h"], 12);
    assert_eq!(rows[0]["mi"], 30);
    assert_eq!(rows[0]["s"], 45);
    assert_eq!(rows[0]["dow"], 2, "2026-08-25 is a Tuesday");
    assert_eq!(
        rows[0]["iso"], "2026-08-25T12:30:45.000000Z",
        "the JSON writer's own rendering"
    );
    assert_eq!(rows[0]["span"], "1000000", "one second, in the writer's microseconds");
    assert!(
        rows[0].get("bad_day").is_none(),
        "February 30th is unparseable, not March 2nd"
    );
    b.reclaim(&mut env.pool);
}

#[test]
fn array_length_works_end_to_end() {
    let plan = compile_ok(vec![code(
        0,
        r#"
        via_func = length(["a", "b", "c"]);
        via_method = ["a", "b", "c"].length();
        empty_ish = ["x"].length();
        "#,
    )]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_offset(&mut env, &mut b, &[0]);

    let rows = env.run_json(&plan, &mut b);
    assert_eq!(rows[0]["via_func"], 3);
    assert_eq!(rows[0]["via_method"], 3);
    assert_eq!(rows[0]["empty_ish"], 1);
    b.reclaim(&mut env.pool);
}

#[test]
fn method_call_syntax_matches_free_function_syntax_on_a_bare_field() {
    let plan = compile_ok(vec![
        code(0, "status = __source.offset;"),
        code(1, r#"label = status >= 400 ? "  Error Case  " : "ok case";"#),
        code(
            2,
            "
            via_method = label.trim().toUpperCase();
            via_func = toUpperCase(trim(label));
            ",
        ),
    ]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 2, 0);
    seed_offset(&mut env, &mut b, &[200, 404]);

    let rows = env.run_json(&plan, &mut b);
    assert_eq!(rows[0]["via_method"], rows[0]["via_func"]);
    assert_eq!(rows[0]["via_method"], "OK CASE");
    assert_eq!(rows[1]["via_method"], rows[1]["via_func"]);
    assert_eq!(rows[1]["via_method"], "ERROR CASE");
    b.reclaim(&mut env.pool);
}

/// `now()` has no argument to take a row count from; it reads the batch's from `Ctx`, and so does its instant, so every
/// row sees the same value. `match` is a row like any other now, so the method form reaches it too.
#[test]
fn now_and_method_form_match_work_end_to_end() {
    let plan = compile_ok(vec![code(
        0,
        r#"
        stamp = now();
        stamp_year = year(now());
        hit = "abc".match(/b/);
        miss = "abc".match(/z/);
        "#,
    )]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 3, 0);
    seed_offset(&mut env, &mut b, &[1, 2, 3]);

    let rows = env.run_json(&plan, &mut b);
    let stamp = rows[0]["stamp"].as_str().expect("a DateTime renders as ISO 8601 text");
    assert!(stamp.ends_with('Z') && stamp.contains('T'), "{stamp}");
    assert_eq!(rows[1]["stamp"], stamp, "one instant per batch");
    assert_eq!(rows[2]["stamp"], stamp);
    assert!(rows[0]["stamp_year"].as_i64().unwrap() >= 2026);
    assert_eq!(rows[0]["hit"], serde_json::json!(["b"]));
    assert!(
        rows[0].get("miss").is_none(),
        "no match is null, and null fields are omitted"
    );
    b.reclaim(&mut env.pool);
}

/// `match` is JS's: the whole match and its groups, or null; `test` is the yes/no; `split` takes a regex or a
/// string, and an empty string splits into characters; `replace` takes JS's `$<name>` and `$&`.
#[test]
fn regex_family_follows_javascript_end_to_end() {
    let plan = compile_ok(vec![code(
        0,
        r#"
        groups = match("user=alice id=7", /user=(\w+) id=(\d+)/);
        first_group = match("user=alice id=7", /user=(\w+)/)[1];
        miss = match("nothing", /user=(\w+)/);
        yes = test("user=alice", /user=/);
        no = test("nobody", /user=/);
        by_regex = split("a, b,c", /,\s*/);
        by_text = split("a,b", ",");
        chars = split("abc", "");
        swapped = replaceFirst("2026-09", /(?<y>\d+)-(?<m>\d+)/, "$<m>/$<y>");
        wrapped = replaceFirst("abc", /b/, "[$&]");
        "#,
    )]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_offset(&mut env, &mut b, &[0]);

    let rows = env.run_json(&plan, &mut b);
    assert_eq!(rows[0]["groups"], serde_json::json!(["user=alice id=7", "alice", "7"]));
    assert_eq!(rows[0]["first_group"], "alice");
    assert!(
        rows[0].get("miss").is_none(),
        "no match is null, and null fields are omitted"
    );
    assert_eq!(rows[0]["yes"], true);
    assert_eq!(rows[0]["no"], false);
    assert_eq!(rows[0]["by_regex"], serde_json::json!(["a", "b", "c"]));
    assert_eq!(rows[0]["by_text"], serde_json::json!(["a", "b"]));
    assert_eq!(rows[0]["chars"], serde_json::json!(["a", "b", "c"]));
    assert_eq!(rows[0]["swapped"], "09/2026");
    assert_eq!(rows[0]["wrapped"], "a[b]c");
    b.reclaim(&mut env.pool);
}

/// No flag chooses an overload any more: `g` is gone, and `replaceFirst`/`replaceAll` say which they are. A flag the
/// engine does not act on is rejected rather than silently accepted.
#[test]
fn a_flag_that_does_nothing_is_a_compile_error() {
    for src in ["match(\"aa\", /a/g)", "test(\"aa\", /a/y)", "split(\"aa\", /a/u)"] {
        let errs = compile_err(vec![code(0, &format!("m = {src};"))]);
        assert!(
            errs.iter().any(|e| e.to_string().contains("unknown regex flag")),
            "{src}: {errs:?}"
        );
    }
}

/// A yearless syslog stamp takes its year from the reference argument, or from the batch's `now()` without one, with
/// the December/January rollover applied either way.
#[test]
fn syslog_timestamps_parse_with_an_inferred_year() {
    let plan = compile_ok(vec![
        code(
            0,
            r#"
            ref_dec = parseTime("2026-12-01T00:00:00Z");
            explicit = parseTime("Oct 11 22:14:15", "%b %e %H:%M:%S", parseTime("2026-12-01T00:00:00Z"));
            rolled = parseTime("Dec 31 23:59:58", "%b %e %H:%M:%S", parseTime("2027-01-01T00:00:01Z"));
            auto_ref = parseTime("Oct  1 22:14:15", parseTime("2026-12-01T00:00:00Z"));
            auto_now = parseTime("Oct  1 22:14:15");
            "#,
        ),
        code(
            1,
            "
            explicit_y = year(explicit);
            rolled_y = year(rolled);
            auto_ref_y = year(auto_ref);
            auto_ref_d = day(auto_ref);
            auto_now_m = month(auto_now);
            auto_now_d = day(auto_now);
            auto_now_gap = year(now()) - year(auto_now);
            ",
        ),
    ]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_offset(&mut env, &mut b, &[0]);

    let rows = env.run_json(&plan, &mut b);
    assert_eq!(rows[0]["explicit_y"], 2026);
    assert_eq!(
        rows[0]["rolled_y"], 2026,
        "a December line read on January 1st was written last year"
    );
    assert_eq!(rows[0]["auto_ref_y"], 2026);
    assert_eq!(rows[0]["auto_ref_d"], 1, "RFC 3164 space-pads the day");
    assert_eq!(rows[0]["auto_now_m"], 10);
    assert_eq!(rows[0]["auto_now_d"], 1);
    let gap = rows[0]["auto_now_gap"].as_i64().unwrap();
    assert!(
        (0..=1).contains(&gap),
        "this year, or last year if it is early January: {gap}"
    );
    b.reclaim(&mut env.pool);
}

#[test]
fn the_string_additions_work_end_to_end() {
    let plan = compile_ok(vec![code(
        0,
        r#"
        ts = trimStart("  x ");
        te = "  x ".trimEnd();
        ps = padStart("5", 3, "0");
        ps_default = padStart("ab", 4);
        pe = padEnd("ab", 5, "xy");
        rep = repeat("ab", 3);
        rep_neg = repeat("ab", -1);
        li = lastIndexOf("a-b-c", "-");
        at_last = at("hu{e9}llo", -1);
        at_oob = at("hi", 5);
        "#,
    )]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_offset(&mut env, &mut b, &[0]);

    let rows = env.run_json(&plan, &mut b);
    assert_eq!(rows[0]["ts"], "x ");
    assert_eq!(rows[0]["te"], "  x");
    assert_eq!(rows[0]["ps"], "005");
    assert_eq!(rows[0]["ps_default"], "  ab");
    assert_eq!(rows[0]["pe"], "abxyx");
    assert_eq!(rows[0]["rep"], "ababab");
    assert!(rows[0].get("rep_neg").is_none(), "a negative count is null");
    assert_eq!(rows[0]["li"], 3);
    assert_eq!(rows[0]["at_last"], "o");
    assert!(rows[0].get("at_oob").is_none(), "out of range is null");
    b.reclaim(&mut env.pool);
}

#[test]
fn the_number_and_time_additions_work_end_to_end() {
    let plan = compile_ok(vec![
        code(
            0,
            r#"
            t = parseTime("2026-08-25T12:30:45.123Z");
            tr = trunc(-2.7);
            sg = sign(-3);
            ms = millisecond(parseTime("2026-08-25T12:30:45.123Z"));
            em = epochMillis(parseTime("1970-01-01T00:00:01Z"));
            fe = toString(fromEpochMillis(1000));
            es = epochSeconds(parseTime("1970-01-01T00:00:01Z"));
            fs = toString(fromEpochSeconds(1));
            d = duration(90, "seconds");
            d_float = duration(1.5, "s");
            "#,
        ),
        code(
            1,
            r#"
            total_min = countIn(d, "minutes");
            later = toString(t + d_float);
            "#,
        ),
    ]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_offset(&mut env, &mut b, &[0]);

    let rows = env.run_json(&plan, &mut b);
    assert_eq!(rows[0]["tr"], -2);
    assert_eq!(rows[0]["sg"], -1);
    assert_eq!(rows[0]["ms"], 123);
    assert_eq!(rows[0]["em"], 1000);
    assert_eq!(rows[0]["fe"], "1970-01-01T00:00:01.000000Z");
    assert_eq!(rows[0]["es"], 1);
    assert_eq!(rows[0]["fs"], "1970-01-01T00:00:01.000000Z");
    assert_eq!(rows[0]["total_min"], 1.5);
    assert_eq!(rows[0]["later"], "2026-08-25T12:30:46.623000Z");
    b.reclaim(&mut env.pool);
}

/// The JS `Array` surface, polymorphic with the string forms where the name is shared, as `length` already was.
#[test]
fn the_array_functions_work_end_to_end() {
    let plan = compile_ok(vec![
        code(
            0,
            r#"
            xs = [10, 20, 30];
            names = ["a", "b"];
            "#,
        ),
        code(
            1,
            r#"
            tail = slice(xs, 1);
            middle = xs.slice(-2, 2);
            last = at(xs, -1);
            oob = at(xs, 7);
            has = includes(xs, 20);
            hasnt = includes(xs, 99);
            pos = indexOf(xs, 30);
            joined = join(xs, ", ");
            both = concat(names, ["c"]);
            still_str = includes("abc", "b");
            still_slice = slice("abc", 1);
            "#,
        ),
    ]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_offset(&mut env, &mut b, &[0]);

    let rows = env.run_json(&plan, &mut b);
    assert_eq!(rows[0]["tail"], serde_json::json!([20, 30]));
    assert_eq!(rows[0]["middle"], serde_json::json!([20]));
    assert_eq!(rows[0]["last"], 30);
    assert!(rows[0].get("oob").is_none(), "out of range is null");
    assert_eq!(rows[0]["has"], true);
    assert_eq!(rows[0]["hasnt"], false);
    assert_eq!(rows[0]["pos"], 2);
    assert_eq!(rows[0]["joined"], "10, 20, 30");
    assert_eq!(rows[0]["both"], serde_json::json!(["a", "b", "c"]));
    assert_eq!(rows[0]["still_str"], true);
    assert_eq!(rows[0]["still_slice"], "bc");
    b.reclaim(&mut env.pool);
}

/// `includes` and `indexOf` search the same way, so this cannot come apart.
#[test]
fn includes_agrees_with_index_of() {
    let cases = [
        ("\"abcabc\"", "\"cab\""),
        ("\"abc\"", "\"\""),
        ("\"\"", "\"x\""),
        ("\"abc\"", "\"abcd\""),
        ("\"na\\u{e9}ve\"", "\"\\u{e9}v\""),
    ];
    for (text, needle) in cases {
        let plan = compile_ok(vec![code(
            0,
            &format!("has = includes({text}, {needle}); pos = indexOf({text}, {needle});"),
        )]);
        let mut env = Env::new();
        let mut b = Batch::new(&plan.slot_layout, 1, 0);
        seed_offset(&mut env, &mut b, &[0]);
        let rows = env.run_json(&plan, &mut b);
        let pos = rows[0]["pos"].as_i64().unwrap();
        assert_eq!(rows[0]["has"], pos >= 0, "includes({text}, {needle}) against pos {pos}");
        b.reclaim(&mut env.pool);
    }
}

#[test]
fn an_array_function_refuses_an_element_of_the_wrong_type() {
    let errs = compile_err(vec![code(0, r#"bad = includes([1, 2], "x");"#)]);
    assert!(errs.iter().any(|e| e.to_string().contains("includes")), "{errs:?}");
}

/// A null where an index or a count goes is a null row, not a panic: a
/// parameter kind accepts `Null`, so every kernel behind one has to.
#[test]
fn a_null_index_or_count_is_a_null_row_not_a_panic() {
    let plan = compile_ok(vec![code(
        0,
        r#"
        nums = split("a,b,c", ",");
        sliced = slice(nums, null);
        sliced_end = slice(nums, 0, null);
        element = at(nums, null);
        padded = padStart("x", null);
        repeated = repeat("x", null);
        text_sliced = slice("abc", null);
        "#,
    )]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_offset(&mut env, &mut b, &[0]);

    let rows = env.run_json(&plan, &mut b);
    for field in ["sliced", "sliced_end", "element", "padded", "repeated", "text_sliced"] {
        assert!(rows[0].get(field).is_none(), "{field} should be null: {}", rows[0]);
    }
    b.reclaim(&mut env.pool);
}

/// A count is a per-row number, so one row must not be able to ask for more
/// memory than the worker has. Past the cap the row is null, as JS's
/// `RangeError` case is here.
#[test]
fn a_result_too_large_to_build_is_null() {
    let plan = compile_ok(vec![code(
        0,
        r#"
        pad_huge = padStart("x", 2000000000);
        pad_max = padEnd("x", 9223372036854775807, "ab");
        repeat_huge = repeat("xy", 2000000000);
        pad_ok = padStart("7", 3, "0");
        "#,
    )]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_offset(&mut env, &mut b, &[0]);

    let rows = env.run_json(&plan, &mut b);
    for field in ["pad_huge", "pad_max", "repeat_huge"] {
        assert!(rows[0].get(field).is_none(), "{field} should be null: {}", rows[0]);
    }
    assert_eq!(rows[0]["pad_ok"], "007", "the ordinary case still pads");
    b.reclaim(&mut env.pool);
}

/// A unit is a literal in almost every use, so one that will never convert is refused when the pipeline compiles
/// rather than leaving a column of nulls that reads like missing data. The message names what a span can be counted
/// in, and why a month and a year are not on the list.
#[test]
fn an_unknown_unit_written_as_a_literal_is_a_compile_error() {
    for src in [
        "duration(1, \"y\")",
        "duration(293, \"year\")",
        "duration(1, \"months\")",
        "countIn(duration(1, \"s\"), \"fortnight\")",
    ] {
        let errs = compile_err(vec![code(0, &format!("x = {src};"))]);
        let msg = errs.iter().map(|e| e.to_string()).collect::<Vec<_>>().join("; ");
        assert!(msg.contains("is not a unit of time"), "{src}: {msg}");
        assert!(
            msg.contains("vary in length"),
            "{src}: the message says why a year is not one: {msg}"
        );
    }
}

/// A unit that is not a literal has nothing to check before the rows arrive, so it stays a runtime null.
#[test]
fn a_unit_computed_per_row_is_still_a_runtime_null() {
    let plan = compile_ok(vec![code(
        0,
        r#"
        unit = "nope";
        span = duration(1, unit);
        good = duration(1, "w");
        "#,
    )]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_offset(&mut env, &mut b, &[0]);

    let rows = env.run_json(&plan, &mut b);
    assert!(
        rows[0].get("span").is_none(),
        "an unknown unit at runtime is null: {}",
        rows[0]
    );
    assert_eq!(
        rows[0]["good"], 604_800_000_000i64,
        "a week is seven days, in microseconds"
    );
    b.reclaim(&mut env.pool);
}

/// A duration holds microseconds and is written as that count, so the whole `i64` range renders exactly. What is
/// null is a span whose microseconds do not fit: the multiply in `duration(n, unit)`
/// on the whole path, and the out-of-range cast on the fractional one, where `as i64` saturates rather than
/// wrapping.
/// A second reaches the end of the microsecond range a thousand times sooner than a millisecond does, so the same
/// count overflows one and not the other. Both answer null rather than a wrapped date.
#[test]
fn an_epoch_overflows_seconds_before_it_overflows_millis() {
    let plan = compile_ok(vec![code(
        0,
        "
        as_seconds = toString(fromEpochSeconds(10000000000000));
        as_millis = toString(fromEpochMillis(10000000000000));
        ",
    )]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_offset(&mut env, &mut b, &[0]);

    let rows = env.run_json(&plan, &mut b);
    assert!(
        rows[0].get("as_seconds").is_none(),
        "1e13 seconds is past the microsecond range: {}",
        rows[0]
    );
    assert_eq!(
        rows[0]["as_millis"], "2286-11-20T17:46:40.000000Z",
        "the same count as milliseconds is an ordinary date"
    );
    b.reclaim(&mut env.pool);
}

#[test]
fn a_duration_past_the_microsecond_range_is_null() {
    let plan = compile_ok(vec![code(
        0,
        r#"
        too_big = duration(9223372036854776, "ms");
        too_big_float = countIn(duration(1.0e30, "s"), "s");
        too_big_negative = duration(-9223372036854776, "ms");
        rendered = toString(duration(90, "s"));
        zero = toString(duration(0, "s"));
        largest = toString(duration(9223372036854775807, "us"));
        total_largest = countIn(duration(9223372036854775, "us"), "us");
        "#,
    )]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_offset(&mut env, &mut b, &[0]);

    let rows = env.run_json(&plan, &mut b);
    for field in ["too_big", "too_big_float", "too_big_negative"] {
        assert!(rows[0].get(field).is_none(), "{field} should be null: {}", rows[0]);
    }
    assert_eq!(rows[0]["rendered"], "90000000", "microseconds, as the writer emits");
    assert_eq!(rows[0]["zero"], "0");
    assert_eq!(
        rows[0]["largest"], "9223372036854775807",
        "the whole microsecond range renders exactly"
    );
    assert_eq!(
        rows[0]["total_largest"], 9223372036854775.0,
        "the largest span still answers"
    );
    b.reclaim(&mut env.pool);
}

/// `repeat` writes a `Str`, and a `Str` column is valid UTF-8 by invariant, so
/// a `Bytes` argument goes through the same replacement every other
/// string kernel applies.
#[test]
fn repeat_of_invalid_bytes_produces_valid_text() {
    let plan = compile_ok(vec![code(0, "out = repeat(_raw, 2);")]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    {
        use smeltr::core::batch::SLOT_RAW;
        use smeltr::core::column::{StrBuilder, VarType};
        let mut raw = StrBuilder::new(&mut env.pool, VarType::Bytes, 1);
        raw.reserve(&mut env.pool, 4);
        raw.push(b"a\xE9");
        b.put(SLOT_RAW, raw.finish(), &mut env.pool);
    }

    let rows = env.run_json(&plan, &mut b);
    assert_eq!(rows[0]["out"], "a\u{FFFD}a\u{FFFD}");
    b.reclaim(&mut env.pool);
}

/// The array functions take an array of scalars, so an array of structs is
/// refused where it is written.
#[test]
fn an_array_of_structs_is_refused_by_the_array_functions() {
    let parser = || code(0, "pairs: array<{k: str}>; Object.assign($, json(_raw));");
    for expr in [
        "slice(pairs, 0, 1)",
        "concat(pairs, pairs)",
        "join(pairs, \",\")",
        "includes(pairs, 1)",
    ] {
        let errs = compile_err(vec![parser(), code(1, &format!("bad = {expr};"))]);
        assert!(
            errs.iter().any(|e| e.to_string().contains("array of scalars")),
            "{expr}: {errs:?}"
        );
    }
    // `at` is the exception: it is what `pairs[0]` is, and an element of a declared array of objects is exactly what
    // the declaration is for. `plan::resolve::resolve_element_access` types it from the declaration rather than from an
    // `Array` parameter row.
    let plan = compile_ok(vec![
        parser(),
        code(
            1,
            r#"
            bracket = pairs[0]["k"];
            call = at(at(pairs, 0), "k");
            "#,
        ),
    ]);
    drop(plan);

    // `length` reads offsets only, so it still answers.
    let plan = compile_ok(vec![parser(), code(1, "n = length(pairs);")]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    {
        use smeltr::core::batch::SLOT_RAW;
        use smeltr::core::column::{StrBuilder, VarType};
        let doc = br#"{"pairs":[{"k":"a"},{"k":"b"}]}"#;
        let mut raw = StrBuilder::new(&mut env.pool, VarType::Bytes, 1);
        raw.reserve(&mut env.pool, doc.len());
        raw.push(doc);
        b.put(SLOT_RAW, raw.finish(), &mut env.pool);
    }
    let rows = env.run_json(&plan, &mut b);
    assert_eq!(rows[0]["n"], 2);
    b.reclaim(&mut env.pool);
}

/// A `null` argument meets `i64` as `i64`, so `min`, `max`, `clamp` and `idiv` over it and integers are `i64`, null on
/// every row, and blend with another `i64`.
#[test]
fn a_null_argument_beside_integers_gives_an_i64() {
    let plan = compile_ok(vec![code(
        0,
        r#"
        lo = min(null, 2) ?? 3;
        hi = max(2, null) ?? 4;
        within = clamp(null, 0, 10) ?? 5;
        divided = idiv(null, 2) ?? 6;
        "#,
    )]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_offset(&mut env, &mut b, &[0]);
    let rows = env.run_json(&plan, &mut b);
    for (field, want) in [("lo", 3), ("hi", 4), ("within", 5), ("divided", 6)] {
        assert_eq!(rows[0][field], want, "{field}: {}", rows[0]);
    }
    b.reclaim(&mut env.pool);
}

/// `clamp` names its bounds, where `max(lo, min(v, hi))` makes a reader work out which is which. An inverted range is
/// null: nothing satisfies it, and answering with one of the ends would hide a mistake in what are usually constants.
#[test]
fn clamp_pulls_a_value_into_its_bounds_and_is_null_when_they_invert() {
    let plan = compile_ok(vec![code(
        0,
        r#"
        above = clamp(150, 0, 100);
        below = clamp(-20, 0, 100);
        inside = clamp(64, 0, 100);
        at_lo = clamp(0, 0, 100);
        at_hi = clamp(100, 0, 100);
        widened = clamp(1, 0.5, 2);
        inverted = clamp(1, 10, 0);
        null_bound = clamp(1, 0, parseInt("nope"));
        "#,
    )]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_offset(&mut env, &mut b, &[0]);

    let rows = env.run_json(&plan, &mut b);
    assert_eq!(rows[0]["above"], 100, "above the range clamps to `hi`");
    assert_eq!(rows[0]["below"], 0, "below it clamps to `lo`");
    assert_eq!(rows[0]["inside"], 64, "inside it is itself");
    assert_eq!(rows[0]["at_lo"], 0, "the bounds are inclusive");
    assert_eq!(rows[0]["at_hi"], 100, "the bounds are inclusive");
    assert_eq!(rows[0]["widened"], 1.0, "one float bound widens the result to F64");
    // `== 1.0` also matches the integer 1. The integer wins here, and must still be written as a float.
    assert!(rows[0]["widened"].is_f64(), "{}", rows[0]);
    for field in ["inverted", "null_bound"] {
        assert!(rows[0].get(field).is_none(), "{field} should be null: {}", rows[0]);
    }
    b.reclaim(&mut env.pool);
}

/// `xs[i]` is `at(xs, i)`, so the two cannot disagree about a negative index or a miss.
#[test]
fn bracket_indexing_is_at_including_from_the_end() {
    let plan = compile_ok(vec![code(
        0,
        r#"
        last = split("a,b,c", ",")[-1];
        first_from_end = split("a,b,c", ",")[-3];
        past_the_front = split("a,b,c", ",")[-4];
        first = split("a,b,c", ",")[0];
        past_the_end = split("a,b,c", ",")[99];
        agrees_with_at = split("a,b,c", ",")[-1] == at(split("a,b,c", ","), -1);
        string_char = "smeltr"[0];
        string_last = "smeltr"[-1];
        "#,
    )]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_offset(&mut env, &mut b, &[0]);

    let rows = env.run_json(&plan, &mut b);
    assert_eq!(rows[0]["last"], "c", "-1 is the last element");
    assert_eq!(rows[0]["first_from_end"], "a", "-3 reaches the first of three");
    assert_eq!(rows[0]["first"], "a");
    assert_eq!(rows[0]["agrees_with_at"], true);
    assert_eq!(rows[0]["string_char"], "s", "a string indexes too");
    assert_eq!(rows[0]["string_last"], "r");
    for field in ["past_the_front", "past_the_end"] {
        assert!(rows[0].get(field).is_none(), "{field} should be null: {}", rows[0]);
    }
    b.reclaim(&mut env.pool);
}

/// `x[i]` and `at(x, i)` are one resolution, so every receiver either works in both spellings or fails in both with
/// the same message -- including the two that are typed from the key rather than from a table row.
#[test]
fn bracket_and_at_are_the_same_call_on_every_receiver() {
    let parser = |id| {
        code(
            id,
            "tags: array<{name: str, ...}>; geo: {name: str, ...}; Object.assign($, json(_raw));",
        )
    };
    // A JSON parser's `geo` is open, and keeps its type. Converted to a closed type, it has no key but its own.
    let open = || vec![parser(0)];
    let closed = || vec![parser(0), code(1, "geo: {name: str} = geo as {name: str};")];
    let then = |mut steps: Vec<Step>, expr: &str| {
        steps.push(code(steps.len() as FunctionId, &format!("out = {expr};")));
        steps
    };
    // Each pair is the same call written both ways.
    for (bracket, call) in [
        ("geo[\"name\"]", "at(geo, \"name\")"),
        // A key open `geo` does not declare reads its unclaimed fields.
        ("geo[\"nope\"]", "at(geo, \"nope\")"),
        ("tags[0]", "at(tags, 0)"),
        ("tags[0][\"name\"]", "at(at(tags, 0), \"name\")"),
        ("tags[-1][\"name\"]", "at(at(tags, -1), \"name\")"),
        ("\"smeltr\"[0]", "at(\"smeltr\", 0)"),
    ] {
        compile_ok(then(open(), bracket));
        compile_ok(then(open(), call));
    }
    // Closed `geo` reads its own key, so what refuses below is the key, not the binding.
    compile_ok(then(closed(), "geo[\"name\"]"));
    for (geo, bracket, call) in [
        (closed(), "geo[\"nope\"]", "at(geo, \"nope\")"),
        (open(), "geo[1]", "at(geo, 1)"),
        (closed(), "geo[1]", "at(geo, 1)"),
    ] {
        let a = compile_err(then(geo.clone(), bracket));
        let b = compile_err(then(geo, call));
        assert_eq!(
            a.iter().map(|e| e.to_string()).collect::<Vec<_>>(),
            b.iter().map(|e| e.to_string()).collect::<Vec<_>>(),
            "`{bracket}` and `{call}` must be refused identically"
        );
    }
}

/// One call, one pass. `push` and `concat` take every value at once because the alternative is a loop, and a loop
/// that pushes reallocates the whole array column on every iteration; `min`/`max` take every argument because a
/// chain of two-argument calls is an intermediate column each.
#[test]
fn push_concat_min_and_max_take_any_number_of_arguments() {
    let plan = compile_ok(vec![code(
        0,
        r#"
        pushed = push(split("a,b", ","), "c", "d", "e");
        pushed_one = push(split("a,b", ","), "c");
        joined = concat(split("a,b", ","), split("c,d", ","), split("e,f", ","));
        smallest = min(5, 3, 9, 1);
        largest = max(5, 3, 9, 1);
        widened = min(5, 3.5, 9);
        null_arg = min(5, 3, parseInt("nope"));
        keeps_null = push(split("a", ","), "b", parseInt("nope").toString(), "c");
        // A literal `null` carries the pinned `I64` slot type, not the array's `Str`. The element type comes
        // from the array, so this is `["a", null]`, not a panic reading the array as `I64`.
        push_literal_null = push(split("a", ","), null);
        insert_literal_null = insert(split("a,b", ","), 1, null);
        "#,
    )]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_offset(&mut env, &mut b, &[0]);

    let rows = env.run_json(&plan, &mut b);
    assert_eq!(rows[0]["pushed"], serde_json::json!(["a", "b", "c", "d", "e"]));
    assert_eq!(rows[0]["pushed_one"], serde_json::json!(["a", "b", "c"]));
    assert_eq!(rows[0]["joined"], serde_json::json!(["a", "b", "c", "d", "e", "f"]));
    assert_eq!(rows[0]["smallest"], 1);
    assert_eq!(rows[0]["largest"], 9);
    assert_eq!(rows[0]["widened"], 3.5, "one float argument widens the result");
    assert!(rows[0].get("null_arg").is_none(), "a null argument is a null result");
    assert_eq!(
        rows[0]["keeps_null"],
        serde_json::json!(["a", "b", null, "c"]),
        "a pushed null is a position, as it is in an array literal"
    );
    assert_eq!(
        rows[0]["push_literal_null"],
        serde_json::json!(["a", null]),
        "a literal null is a position, and does not pick the builder from its pinned type"
    );
    assert_eq!(rows[0]["insert_literal_null"], serde_json::json!(["a", null, "b"]));
    b.reclaim(&mut env.pool);
}

/// A variadic function has a minimum arity, not a set of them, and its tail has to agree with itself.
#[test]
fn a_variadic_call_is_refused_for_arity_or_for_a_mismatched_tail() {
    for (expr, wanted) in [
        ("push(split(\"a\", \",\"))", "takes 2 or more arguments"),
        ("min(1)", "takes 2 or more arguments"),
        ("push(split(\"a\", \",\"), \"b\", 1)", "is not defined"),
        ("concat(split(\"a\", \",\"), 1)", "is not defined"),
        ("min(1, \"x\")", "is not defined"),
        // `clamp` is not variadic: three arguments is its meaning, not a repetition.
        ("clamp(1, 2, 3, 4)", "takes 3 arguments"),
    ] {
        let errs = compile_err(vec![code(0, &format!("bad = {expr};"))]);
        assert!(
            errs.iter().any(|e| e.to_string().contains(wanted)),
            "`{expr}` should be refused with {wanted:?}: {errs:?}"
        );
    }
}

/// `+` joins two arrays of the same element type, and nothing else. It never pushes a scalar: `xs + y` would be
/// ambiguous the moment `y` is itself an array.
#[test]
fn plus_joins_two_arrays_and_refuses_a_scalar() {
    let plan = compile_ok(vec![code(
        0,
        r#"
        pair = split("a,b", ",") + split("c,d", ",");
        chain = split("a", ",") + split("b", ",") + split("c", ",") + split("d", ",");
        same_as_concat = concat(split("a", ","), split("b", ","));
        "#,
    )]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_offset(&mut env, &mut b, &[0]);

    let rows = env.run_json(&plan, &mut b);
    assert_eq!(rows[0]["pair"], serde_json::json!(["a", "b", "c", "d"]));
    assert_eq!(rows[0]["chain"], serde_json::json!(["a", "b", "c", "d"]));
    assert_eq!(rows[0]["same_as_concat"], serde_json::json!(["a", "b"]));
    b.reclaim(&mut env.pool);

    for expr in [
        "split(\"a,b\", \",\") + \"c\"",
        "\"c\" + split(\"a,b\", \",\")",
        "split(\"a,b\", \",\") + 1",
    ] {
        let errs = compile_err(vec![code(0, &format!("bad = {expr};"))]);
        assert!(
            errs.iter().any(|e| e.to_string().contains("not defined")),
            "`{expr}` should not compile: {errs:?}"
        );
    }
}

/// `remove` takes one element out: the ones after it move up. A negative index counts from the end, as it does for
/// `at` and `insert`, an index outside the array removes nothing, and a null array or index is a null row. A position
/// that holds nothing is an element like any other.
#[test]
fn remove_takes_out_the_element_at_a_position_that_counts_from_either_end() {
    let plan = compile_ok(vec![
        code(
            0,
            r#"
            middle = remove(split("a,b,c", ","), 1);
            front = remove(split("a,b,c", ","), 0);
            last = remove(split("a,b,c", ","), -1);
            past_end = remove(split("a,b,c", ","), 3);
            past_front = remove(split("a,b,c", ","), -4);
            method = split("a,b", ",").remove(0);
            nothing_there = [null, 1].remove(0);
            null_index = remove(split("a,b", ","), parseInt("nope"));
            null_array = remove(split(parseInt("nope").toString(), ","), 0);
            "#,
        ),
        code(1, "empty: array<str> = remove([], 0);"),
    ]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_offset(&mut env, &mut b, &[0]);

    let rows = env.run_json(&plan, &mut b);
    assert_eq!(rows[0]["middle"], serde_json::json!(["a", "c"]));
    assert_eq!(rows[0]["front"], serde_json::json!(["b", "c"]));
    assert_eq!(rows[0]["last"], serde_json::json!(["a", "b"]));
    assert_eq!(
        rows[0]["past_end"],
        serde_json::json!(["a", "b", "c"]),
        "an index outside the array removes nothing"
    );
    assert_eq!(rows[0]["past_front"], rows[0]["past_end"]);
    assert_eq!(rows[0]["method"], serde_json::json!(["b"]));
    assert_eq!(rows[0]["nothing_there"], serde_json::json!([1]));
    assert_eq!(rows[0]["empty"], serde_json::json!([]));
    for name in ["null_index", "null_array"] {
        assert!(rows[0].get(name).is_none(), "{name}: {}", rows[0]);
    }
    b.reclaim(&mut env.pool);

    // The index varies per row: each row drops its own element.
    let plan = compile_ok(vec![code(
        0,
        r#"out = remove(split("a,b,c", ","), __source.offset - 1);"#,
    )]);
    let mut b = Batch::new(&plan.slot_layout, 5, 0);
    seed_offset(&mut env, &mut b, &[0, 1, 2, 3, 4]);
    let out: Vec<_> = env.run_json(&plan, &mut b).iter().map(|r| r["out"].clone()).collect();
    assert_eq!(
        out,
        [
            serde_json::json!(["a", "b"]),
            serde_json::json!(["b", "c"]),
            serde_json::json!(["a", "c"]),
            serde_json::json!(["a", "b"]),
            serde_json::json!(["a", "b", "c"]),
        ]
    );
    b.reclaim(&mut env.pool);

    let errs = compile_err(vec![code(0, r#"bad = remove(split("a", ","), "0");"#)]);
    assert!(
        errs.iter().any(|e| e.to_string().contains("is not defined")),
        "an index is i64: {errs:?}"
    );
}

/// `resize` gives an array exactly `size` positions: its first `size`, or its own followed by positions that hold
/// nothing. A negative or null size, or a null array, is a null row. More positions than a column can count is a
/// refusal of the memory budget, which the runtime takes as it takes any.
#[test]
fn resize_gives_an_array_exactly_its_size() {
    let plan = compile_ok(vec![
        code(
            0,
            r#"
            shrink = resize(split("a,b,c", ","), 2);
            grow = resize(split("a,b", ","), 4);
            zero = resize(split("a,b", ","), 0);
            method = split("a", ",").resize(2);
            negative = resize(split("a", ","), -1);
            null_size = resize(split("a", ","), parseInt("nope"));
            null_array = resize(split(parseInt("nope").toString(), ","), 1);
            "#,
        ),
        code(1, "from_nothing: array<str> = resize([], 2);"),
    ]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_offset(&mut env, &mut b, &[0]);
    let rows = env.run_json(&plan, &mut b);
    assert_eq!(rows[0]["shrink"], serde_json::json!(["a", "b"]));
    assert_eq!(rows[0]["grow"], serde_json::json!(["a", "b", null, null]));
    assert_eq!(rows[0]["zero"], serde_json::json!([]));
    assert_eq!(rows[0]["from_nothing"], serde_json::json!([null, null]));
    assert_eq!(rows[0]["method"], serde_json::json!(["a", null]));
    for name in ["negative", "null_size", "null_array"] {
        assert!(rows[0].get(name).is_none(), "{name}: {}", rows[0]);
    }
    b.reclaim(&mut env.pool);

    // The size varies per row.
    let plan = compile_ok(vec![code(0, r#"out = resize(split("a,b", ","), __source.offset);"#)]);
    let mut b = Batch::new(&plan.slot_layout, 4, 0);
    seed_offset(&mut env, &mut b, &[0, 1, 2, 3]);
    let out: Vec<_> = env.run_json(&plan, &mut b).iter().map(|r| r["out"].clone()).collect();
    assert_eq!(
        out,
        [
            serde_json::json!([]),
            serde_json::json!(["a"]),
            serde_json::json!(["a", "b"]),
            serde_json::json!(["a", "b", null]),
        ]
    );
    b.reclaim(&mut env.pool);

    let plan = compile_ok(vec![code(0, "huge: array<i64> = resize([], 5000000000);")]);
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_offset(&mut env, &mut b, &[0]);
    let refused = smeltr::core::buffer::catch_over_budget(|| env.run_json(&plan, &mut b));
    assert!(refused.is_err(), "more positions than an offset counts is refused");
    b.reclaim(&mut env.pool);
}

/// `insert` is `push` with the split somewhere other than the end -- one kernel, one allocation. A negative index
/// counts from the end as it does for `at` and `slice`, so `-1` is *before* the last element, not after it: appending
/// is `push`, and no negative index names the gap past the end.
#[test]
fn insert_puts_values_at_a_position_that_counts_from_either_end() {
    let plan = compile_ok(vec![code(
        0,
        r#"
        middle = insert(split("a,b,d,e", ","), 2, "c");
        front = insert(split("a,b,c", ","), 0, "z");
        before_last = insert(split("a,b,c", ","), -1, "z");
        at_end = insert(split("a,b,c", ","), 3, "z");
        past_end = insert(split("a,b,c", ","), 99, "z");
        past_front = insert(split("a,b,c", ","), -99, "z");
        several = insert(split("a,b", ","), 1, "x", "y", "z");
        method = split("a,b", ",").insert(1, "z");
        null_value = insert(split("a,b", ","), 1, parseInt("nope").toString());
        null_index = insert(split("a,b", ","), parseInt("nope"), "z");
        push_is_the_end = push(split("a,b", ","), "z");
        "#,
    )]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_offset(&mut env, &mut b, &[0]);

    let rows = env.run_json(&plan, &mut b);
    assert_eq!(rows[0]["middle"], serde_json::json!(["a", "b", "c", "d", "e"]));
    assert_eq!(rows[0]["front"], serde_json::json!(["z", "a", "b", "c"]));
    assert_eq!(
        rows[0]["before_last"],
        serde_json::json!(["a", "b", "z", "c"]),
        "-1 is the last element, so inserting at it goes before it"
    );
    assert_eq!(rows[0]["at_end"], serde_json::json!(["a", "b", "c", "z"]));
    assert_eq!(
        rows[0]["past_end"], rows[0]["at_end"],
        "an index past either end clamps"
    );
    assert_eq!(
        rows[0]["past_front"], rows[0]["front"],
        "an index past either end clamps"
    );
    assert_eq!(rows[0]["several"], serde_json::json!(["a", "x", "y", "z", "b"]));
    assert_eq!(rows[0]["method"], serde_json::json!(["a", "z", "b"]));
    assert_eq!(
        rows[0]["null_value"],
        serde_json::json!(["a", null, "b"]),
        "a null value is a position, as it is for `push`"
    );
    assert!(
        rows[0].get("null_index").is_none(),
        "a null index has no position to mean: {}",
        rows[0]
    );
    assert_eq!(
        rows[0]["push_is_the_end"],
        serde_json::json!(["a", "b", "z"]),
        "`push` is this kernel with the split at the end"
    );
    b.reclaim(&mut env.pool);

    for (expr, wanted) in [
        ("insert(split(\"a\", \",\"), 1)", "takes 3 or more arguments"),
        ("insert(split(\"a\", \",\"), 1, 9)", "is not defined"),
    ] {
        let errs = compile_err(vec![code(0, &format!("bad = {expr};"))]);
        assert!(
            errs.iter().any(|e| e.to_string().contains(wanted)),
            "`{expr}` should be refused with {wanted:?}: {errs:?}"
        );
    }
}

#[test]
fn web_functions_return_objects_end_to_end() {
    let plan = compile_ok(vec![
        code(0, "Object.assign($, json(_raw)); u: str = u; host: str = host;"),
        code(
            1,
            r#"
            url = parseUrl(u);
            domain = registeredDomain(host);
            port = parseUrl(u).port;
            ext = url.extension;
            "#,
        ),
    ]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 2, 0);
    seed_raw(
        &mut env,
        &mut b,
        &[
            r#"{"u": "http://user:pw@www.example.com:8080/a/b.php?q=%20x#frag", "host": "www.foo.example.co.uk"}"#,
            r#"{"u": "not a url", "host": "nope.invalidtld"}"#,
        ],
    );
    let rows = env.run_json(&plan, &mut b);
    assert_eq!(
        rows[0]["url"],
        serde_json::json!({
            "domain": "www.example.com",
            "fragment": "frag",
            "path": "/a/b.php",
            "extension": "php",
            "port": 8080,
            "query": "q= x",
            "scheme": "http",
            "user_info": "user:pw",
            "username": "user",
            "password": "pw",
        })
    );
    assert_eq!(rows[0]["port"], 8080);
    assert_eq!(rows[0]["ext"], "php");
    assert_eq!(
        rows[0]["domain"],
        serde_json::json!({
            "domain": "www.foo.example.co.uk",
            "registered_domain": "example.co.uk",
            "top_level_domain": "co.uk",
            "subdomain": "www.foo",
        })
    );
    assert!(
        rows[1].get("url").is_none(),
        "text that is not a URL is null: {}",
        rows[1]
    );
    assert!(
        rows[1].get("domain").is_none(),
        "a host with no known suffix is null: {}",
        rows[1]
    );
    b.reclaim(&mut env.pool);
}

#[test]
fn hash_and_text_functions_work_end_to_end() {
    let plan = compile_ok(vec![code(
        0,
        r#"
        sha = sha256("abc");
        decoded = urlDecode("a%20b+c");
        stray = urlDecode("100%+5%zz");
        stripped = htmlStrip("<p>a &amp; b</p><script>x()</script>c");
        sorted = sort(["b", "c", "a"]);
        reversed = [3, 1, 2].sort(true);
        flow = communityId("128.232.110.120", 34855, "66.35.250.204", 80, "TCP");
        signed = htmlStrip("<b>&#+65;</b>");
        no_type = communityId("10.0.0.1", 70000, "10.0.0.2", 0, "icmp");
        mixed = communityId("10.0.0.1", 1, "::1", 2, "tcp");
        reserved = communityId("10.0.0.1", 0, "10.0.0.2", 0, 255);
        md = md5("abc");
        named = htmlStrip("<p>&ouml;&alpha;&AMP;</p>");
        scripture = htmlStrip("<script>a</scripture>b</SCRIPT >c");
        bare = parseUrl("report.pdf").extension;
        user = parseUrl("http://alice@example.com/");
        linux = parseUserAgent("Mozilla/5.0 (X11; Linux x86_64; rv:120.0) Gecko/20100101 Firefox/120.0").os;
        "#,
    )]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_offset(&mut env, &mut b, &[0]);
    let rows = env.run_json(&plan, &mut b);
    assert_eq!(
        rows[0]["sha"],
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    );
    assert_eq!(rows[0]["decoded"], "a b c");
    assert_eq!(rows[0]["stray"], "100% 5%zz");
    assert_eq!(rows[0]["stripped"], "\na & b\nc");
    assert_eq!(rows[0]["sorted"], serde_json::json!(["a", "b", "c"]));
    assert_eq!(rows[0]["reversed"], serde_json::json!([3, 2, 1]));
    assert_eq!(rows[0]["flow"], "1:LQU9qZlK+B5F3KDmev6m5PMibrg=");
    assert_eq!(rows[0]["signed"], "&#+65;");
    assert!(rows[0].get("no_type").is_none(), "an ICMP type past 65535 has no flow");
    assert!(
        rows[0].get("mixed").is_none(),
        "an IPv4 and an IPv6 endpoint are no flow"
    );
    assert!(rows[0]["reserved"].as_str().is_some_and(|id| id.starts_with("1:")));
    assert_eq!(rows[0]["md"], "900150983cd24fb0d6963f7d28e17f72");
    assert_eq!(rows[0]["named"], "\nöα&\n");
    assert_eq!(rows[0]["scripture"], "c");
    assert_eq!(rows[0]["bare"], "pdf");
    assert_eq!(rows[0]["user"]["username"], "alice");
    assert!(rows[0]["user"].get("password").is_none());
    assert_eq!(rows[0]["linux"], serde_json::json!({"name": "Linux", "full": "Linux"}));
    b.reclaim(&mut env.pool);
}

#[test]
fn parse_user_agent_names_browser_os_and_device() {
    let plan = compile_ok(vec![
        code(0, "Object.assign($, json(_raw)); ua: str = ua;"),
        code(1, "agent = parseUserAgent(ua); browser = agent.name;"),
    ]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 3, 0);
    seed_raw(
        &mut env,
        &mut b,
        &[
            r#"{"ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.6099.109 Safari/537.36"}"#,
            r#"{"ua": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Mobile/15E148 Safari/604.1"}"#,
            r#"{"ua": "nothing anyone knows"}"#,
        ],
    );
    let rows = env.run_json(&plan, &mut b);
    assert_eq!(
        rows[0]["agent"],
        serde_json::json!({
            "name": "Chrome",
            "version": "120.0.6099.109",
            "os": {"name": "Windows", "version": "10", "full": "Windows 10"},
            "device": {"name": "Other"},
        })
    );
    assert_eq!(rows[1]["agent"]["name"], "Mobile Safari");
    assert_eq!(rows[1]["agent"]["os"]["name"], "iOS");
    assert_eq!(rows[1]["agent"]["device"]["name"], "iPhone");
    assert_eq!(
        rows[2]["agent"],
        serde_json::json!({"name": "Other", "device": {"name": "Other"}})
    );
    assert_eq!(rows[0]["browser"], "Chrome");
    b.reclaim(&mut env.pool);
}
