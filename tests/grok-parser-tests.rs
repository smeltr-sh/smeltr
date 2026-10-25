//! End-to-end tests for the Grok parser kernel: `_raw` log lines in,
//! serialized JSON out through `plan::output::run_to_json`.

use smeltr::core::batch::Batch;
use smeltr::core::column::DataType;
use smeltr::core::error::ExceptionKind;
use smeltr::core::id::FunctionId;
use smeltr::plan::config::{DeclaredField, FunctionKind, GrokConfig, ParserKind, Step, TimeSource};

use crate::harness;
use harness::*;

/// A grok parser step, kept for what only a step has: a filter of its own.
fn grok_parser(id: FunctionId, config: GrokConfig, declared: &[(&str, DataType)]) -> Step {
    func(
        id,
        FunctionKind::Parser {
            input: "_raw".into(),
            into: String::new(),
            kind: ParserKind::Grok(config),
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
}

fn grok(patterns: &[&str]) -> GrokConfig {
    GrokConfig {
        split_dots: true,
        patterns: patterns.iter().map(|s| s.to_string()).collect(),
        custom_patterns: vec![],
    }
}

// ---------------------------------------------------------------------------

#[test]
fn a_matching_line_produces_typed_declared_fields() {
    let plan = compile_ok(vec![code(
        0,
        r#"action: str; status: i64; Object.assign($, grok(_raw, ["%{WORD:action} %{INT:status}"]));"#,
    )]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_raw(&mut env, &mut b, &["login 200"]);

    let rows = env.run(&plan, &mut b);
    assert_eq!(
        parsed(&rows[0]),
        parsed(br#"{"_raw":"login 200","action":"login","status":200}"#)
    );
    b.reclaim(&mut env.pool);
}

/// A capture nothing declares is still a field the pattern names, so it arrives as the string it matched.
#[test]
fn a_capture_nothing_declares_is_a_str_field() {
    let plan = compile_ok(vec![code(
        0,
        r#"action: str; Object.assign($, grok(_raw, ["%{WORD:action} %{INT:status}"]));"#,
    )]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_raw(&mut env, &mut b, &["login 200"]);

    let rows = env.run(&plan, &mut b);
    assert_eq!(
        parsed(&rows[0]),
        parsed(br#"{"_raw":"login 200","action":"login","status":"200"}"#)
    );
    b.reclaim(&mut env.pool);
}

/// Leaving the capture unnamed is that way: it compiles to a non-capturing group, so nothing is declared and nothing is
/// dropped behind the author's back.
#[test]
fn an_unnamed_capture_needs_no_declaration() {
    let plan = compile_ok(vec![code(
        0,
        r#"action: str; Object.assign($, grok(_raw, ["%{WORD:action} %{INT}"]));"#,
    )]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_raw(&mut env, &mut b, &["login 200"]);

    let rows = env.run(&plan, &mut b);
    assert_eq!(parsed(&rows[0]), parsed(br#"{"_raw":"login 200","action":"login"}"#));
    b.reclaim(&mut env.pool);
}

/// A declared field no pattern captures never arrives: it is null on every row, and the serializer omits it.
#[test]
fn a_declared_field_no_pattern_captures_is_null() {
    let plan = compile_ok(vec![code(
        0,
        r#"action: str; status: i64; Object.assign($, grok(_raw, ["%{WORD:action}"]));"#,
    )]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_raw(&mut env, &mut b, &["login"]);

    let rows = env.run(&plan, &mut b);
    assert_eq!(parsed(&rows[0]), parsed(br#"{"_raw":"login","action":"login"}"#));
    assert!(env.errors.entries().is_empty(), "{:?}", env.errors.entries());
    b.reclaim(&mut env.pool);
}

#[test]
fn a_line_matching_no_candidate_pattern_is_a_parse_failure() {
    let plan = compile_ok(vec![code(
        0,
        r#"n: i64; Object.assign($, grok(_raw, ["^%{INT:n}$"]));"#,
    )]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 2, 0);
    seed_raw(&mut env, &mut b, &["not a number", "42"]);

    let rows = env.run(&plan, &mut b);
    assert_eq!(parsed(&rows[0]), parsed(br#"{"_raw":"not a number"}"#));
    assert_eq!(parsed(&rows[1]), parsed(br#"{"_raw":"42","n":42}"#));
    assert!(
        env.errors
            .entries()
            .iter()
            .any(|a| a.row == 0 && matches!(a.kind, ExceptionKind::Unparsable)),
        "{:?}",
        env.errors.entries()
    );
    b.reclaim(&mut env.pool);
}

/// A capture that matched nothing is an empty value under a number: null, and not a failure.
#[test]
fn a_blank_capture_under_a_typed_field_is_null_and_clean() {
    let plan = compile_ok(vec![code(
        0,
        r#"status: i64; Object.assign($, grok(_raw, ["status=%{DATA:status} end"]));"#,
    )]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 2, 0);
    seed_raw(&mut env, &mut b, &["status= end", "status=200 end"]);

    let rows = env.run(&plan, &mut b);
    assert_eq!(
        parsed(&rows[0])["status"],
        serde_json::Value::Null,
        "{}",
        parsed(&rows[0])
    );
    assert_eq!(parsed(&rows[1])["status"], serde_json::json!(200));
    assert!(env.errors.entries().is_empty(), "{:?}", env.errors.entries());
    b.reclaim(&mut env.pool);
}

#[test]
fn a_declared_field_that_does_not_coerce_fails_only_that_field() {
    let plan = compile_ok(vec![code(
        0,
        r#"status: i64; Object.assign($, grok(_raw, ["%{WORD:status}"]));"#,
    )]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_raw(&mut env, &mut b, &["oops"]);

    let rows = env.run(&plan, &mut b);
    assert_eq!(
        parsed(&rows[0]),
        parsed(br#"{"_raw":"oops"}"#),
        "mismatched type -> null, not a crash"
    );
    assert!(
        env.errors
            .entries()
            .iter()
            .any(|a| a.row == 0 && matches!(a.kind, ExceptionKind::UnparsableField { .. })),
        "{:?}",
        env.errors.entries()
    );
    b.reclaim(&mut env.pool);
}

/// Also pins the producible set as the union over candidate patterns rather than the one that matched: both `n` and `w`
/// are declared, and each row matches one pattern and leaves the other field null.
#[test]
fn multiple_candidate_patterns_are_tried_in_order_first_match_wins() {
    let plan = compile_ok(vec![code(
        0,
        r#"n: i64; w: str; Object.assign($, grok(_raw, ["^%{INT:n}$", "^%{WORD:w}$"]));"#,
    )]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 2, 0);
    seed_raw(&mut env, &mut b, &["42", "hello"]);

    let rows = env.run(&plan, &mut b);
    assert_eq!(parsed(&rows[0]), parsed(br#"{"_raw":"42","n":42}"#));
    assert_eq!(parsed(&rows[1]), parsed(br#"{"_raw":"hello","w":"hello"}"#));
    b.reclaim(&mut env.pool);
}

#[test]
fn a_realistic_apache_common_log_line_round_trips() {
    // The date and the HTTP version are matched and not kept, so they are left unnamed: naming a capture is how a grok
    // field is declared, and the compiler refuses one nothing declares.
    let plan = compile_ok(vec![code(
        0,
        r#"client: str; method: str; path: str; status: i64; bytes: i64;
        Object.assign($, grok(_raw, [
            '%{IP:client} - - \\[%{HTTPDATE}\\] \\"%{WORD:method} %{URIPATHPARAM:path} HTTP/%{NUMBER}\\" %{INT:status} %{INT:bytes}'
        ]));
        severity = status >= 400 ? "high" : "low";"#,
    )]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_raw(
        &mut env,
        &mut b,
        &[r#"10.0.0.1 - - [10/Oct/2000:13:55:36 -0700] "GET /index.html HTTP/1.1" 200 1043"#],
    );

    let rows = env.run(&plan, &mut b);
    assert_eq!(
        parsed(&rows[0]),
        parsed(
            br#"{"_raw":"10.0.0.1 - - [10/Oct/2000:13:55:36 -0700] \"GET /index.html HTTP/1.1\" 200 1043","severity":"low","client":"10.0.0.1","method":"GET","path":"/index.html","status":200,"bytes":1043}"#
        )
    );
    b.reclaim(&mut env.pool);
}

#[test]
fn a_custom_pattern_extends_the_built_in_library() {
    let plan = compile_ok(vec![code(
        0,
        r#"g: str; who: str;
        Object.assign($, grok(_raw, ["%{GREETING:g} %{WORD:who}"], {patterns: {GREETING: "hi|hello|hey"}}));"#,
    )]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_raw(&mut env, &mut b, &["hey world"]);

    let rows = env.run(&plan, &mut b);
    assert_eq!(
        parsed(&rows[0]),
        parsed(br#"{"_raw":"hey world","g":"hey","who":"world"}"#)
    );
    b.reclaim(&mut env.pool);
}

#[test]
fn an_unknown_pattern_reference_is_a_compile_error_not_a_runtime_panic() {
    let errs = compile_err(vec![code(
        0,
        r#"x: str; Object.assign($, grok(_raw, ["%{NOT_A_REAL_PATTERN:x}"]));"#,
    )]);
    assert!(!errs.is_empty());
}

#[test]
fn an_empty_pattern_list_is_a_compile_error() {
    let errs = compile_err(vec![code(0, "Object.assign($, grok(_raw, []));")]);
    assert!(!errs.is_empty());
}

#[test]
fn dotted_captures_are_one_flat_field_each_and_nest_at_serialization() {
    let plan = compile_ok(vec![code(
        0,
        r#"client: {name: str, port: i64, ...};
        Object.assign($, grok(_raw, ["%{WORD:client.name} %{INT:client.port}"]));"#,
    )]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_raw(&mut env, &mut b, &["alice 8080"]);

    assert!(
        plan.warnings.is_empty(),
        "two captures under one parent are siblings, not a conflict: {:?}",
        plan.warnings.iter().map(|w| w.to_string()).collect::<Vec<_>>()
    );

    let rows = env.run(&plan, &mut b);
    assert_eq!(
        parsed(&rows[0]),
        parsed(br#"{"_raw":"alice 8080","client":{"name":"alice","port":8080}}"#)
    );
    b.reclaim(&mut env.pool);
}

#[test]
fn a_dotted_capture_reads_back_by_its_whole_name() {
    let plan = compile_ok(vec![code(
        0,
        r#"client.name: str; Object.assign($, grok(_raw, ["%{WORD:client.name}"])); greeting = client.name + "!";"#,
    )]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_raw(&mut env, &mut b, &["alice"]);

    let rows = env.run(&plan, &mut b);
    assert_eq!(
        parsed(&rows[0]),
        parsed(br#"{"_raw":"alice","client":{"name":"alice"},"greeting":"alice!"}"#)
    );
    b.reclaim(&mut env.pool);
}

/// The syslog header of a PAN-OS record, captured straight into the ECS names it belongs under. Before a capture could
/// carry a dot this needed a `rename` step per field to move a flat capture onto its ECS name.
#[test]
fn a_syslog_header_captures_straight_into_ecs_names() {
    let plan = compile_ok(vec![code(
        0,
        r#"log.syslog.priority: i64; syslog_time: str; observer.hostname: str; panos: str;
        Object.assign($, grok(_raw, [
            "<%{INT:log.syslog.priority}>%{SYSLOGTIMESTAMP:syslog_time} %{HOSTNAME:observer.hostname} %{GREEDYDATA:panos}"
        ]));"#,
    )]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_raw(
        &mut env,
        &mut b,
        &["<14>Sep 13 20:41:02 fw-edge-01 1,2026/09/13 20:41:02,TRAFFIC,end"],
    );

    let rows = env.run(&plan, &mut b);
    let v: serde_json::Value = serde_json::from_slice(&rows[0]).expect("JSON out");
    assert_eq!(v.pointer("/log/syslog/priority").and_then(|x| x.as_i64()), Some(14));
    assert_eq!(
        v.pointer("/observer/hostname").and_then(|x| x.as_str()),
        Some("fw-edge-01")
    );
    b.reclaim(&mut env.pool);
}

/// A row the guard does not take is not parsed for this step: it keeps its value and carries no mark. A row it takes
/// whose value is not its type is marked, and keeps its value too: the parser assigns it by presence.
#[test]
fn a_filtered_parser_marks_only_the_rows_its_filter_takes() {
    let Step::Function(mut parser) = grok_parser(
        1,
        grok(&["^%{WORD:kind} %{NOTSPACE:n}$"]),
        &[("kind", DataType::Str), ("n", DataType::I64)],
    ) else {
        unreachable!("a function step")
    };
    parser.filter = Some("startsWith(_raw, \"a\")".into());
    let plan = compile_ok(vec![code(0, "n = -1;"), Step::Function(parser)]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 4, 0);
    seed_raw(&mut env, &mut b, &["a 5", "b text", "!!!", "a text"]);

    let rows = env.run(&plan, &mut b);
    let n: Vec<_> = rows.iter().map(|r| parsed(r)["n"].clone()).collect();
    assert_eq!(
        n,
        [
            serde_json::json!(5),
            serde_json::json!(-1),
            serde_json::json!(-1),
            serde_json::json!(-1)
        ]
    );
    let marks: Vec<_> = env.errors.entries().iter().map(|a| (a.row, a.kind.label())).collect();
    assert_eq!(marks, [(3, "UnparsableField")], "{:?}", env.errors.entries());
    b.reclaim(&mut env.pool);
}

/// Two filtered steps may read one `datetime` field, each in its own format: the first declares the field, and the
/// second, which finds it bound, reads its own rows in its own format.
#[test]
fn two_filtered_parsers_read_one_datetime_in_their_own_formats() {
    let step = |id, kind: &str, format: &str| {
        let Step::Function(mut parser) = grok_parser(id, grok(&["^%{WORD:kind} %{GREEDYDATA:ts}$"]), &[]) else {
            unreachable!("a function step")
        };
        let FunctionKind::Parser { declared, .. } = &mut parser.kind else {
            unreachable!("a parser")
        };
        declared.push(DeclaredField {
            name: "ts".into(),
            ty: DataType::DateTime,
            time_source: Some(TimeSource::Format(format.into())),
        });
        parser.filter = Some(format!("startsWith(_raw, \"{kind}\")"));
        Step::Function(parser)
    };
    let plan = compile_ok(vec![step(0, "a", "%Y-%m-%d %H:%M"), step(1, "b", "%d/%m/%Y %H:%M")]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 3, 0);
    seed_raw(
        &mut env,
        &mut b,
        &["a 2024-03-15 07:30", "b 15/03/2024 07:30", "b 2024-03-15 07:30"],
    );

    let rows = env.run(&plan, &mut b);
    let ts: Vec<_> = rows.iter().map(|r| parsed(r)["ts"].clone()).collect();
    let want = serde_json::json!("2024-03-15T07:30:00.000000Z");
    assert_eq!(ts, [want.clone(), want, serde_json::Value::Null]);
    let marks: Vec<_> = env.errors.entries().iter().map(|a| (a.row, a.kind.label())).collect();
    assert_eq!(marks, [(2, "UnparsableField")], "{:?}", env.errors.entries());
    b.reclaim(&mut env.pool);
}
