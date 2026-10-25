//! End-to-end tests for the KeyValue parser kernel: `_raw` `key=value`
//! lines in, serialized JSON out through `plan::output::run_to_json`.

use smeltr::core::batch::Batch;
use smeltr::core::error::ExceptionKind;

use crate::harness;
use harness::*;

// ---------------------------------------------------------------------------

#[test]
fn keys_name_fields_directly() {
    let plan = compile_ok(vec![code(0, "Object.assign($, kv(_raw));")]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_raw(&mut env, &mut b, &["status=200 host=web01"]);

    let rows = env.run(&plan, &mut b);
    assert_eq!(
        parsed(&rows[0]),
        parsed(br#"{"_raw":"status=200 host=web01","status":"200","host":"web01"}"#)
    );
    b.reclaim(&mut env.pool);
}

#[test]
fn a_quoted_value_can_contain_the_pair_separator() {
    let plan = compile_ok(vec![code(0, "Object.assign($, kv(_raw));")]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_raw(&mut env, &mut b, &[r#"msg="hello world" status=200"#]);

    let rows = env.run(&plan, &mut b);
    assert_eq!(
        parsed(&rows[0]),
        parsed(br#"{"_raw":"msg=\"hello world\" status=200","msg":"hello world","status":"200"}"#)
    );
    b.reclaim(&mut env.pool);
}

#[test]
fn a_quoted_value_is_never_sniffed_even_if_it_looks_numeric() {
    let plan = compile_ok(vec![code(0, "Object.assign($, kv(_raw));")]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_raw(&mut env, &mut b, &[r#"zip="00501""#]);

    let rows = env.run(&plan, &mut b);
    assert_eq!(parsed(&rows[0]), parsed(br#"{"_raw":"zip=\"00501\"","zip":"00501"}"#));
    b.reclaim(&mut env.pool);
}

/// Declared `str`, `status=200` is text, as it is undeclared.
#[test]
fn a_declared_type_overrides_sniffing() {
    let plan = compile_ok(vec![code(0, "status: str; Object.assign($, kv(_raw));")]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_raw(&mut env, &mut b, &["status=200"]);

    let rows = env.run(&plan, &mut b);
    assert_eq!(parsed(&rows[0]), parsed(br#"{"_raw":"status=200","status":"200"}"#));
    b.reclaim(&mut env.pool);
}

/// `status=` with nothing after it is an empty value under a number: null, and not a failure.
#[test]
fn a_blank_value_under_a_typed_field_is_null_and_clean() {
    let plan = compile_ok(vec![code(0, "status: i64; Object.assign($, kv(_raw));")]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_raw(&mut env, &mut b, &["status= user=bob"]);

    let rows = env.run(&plan, &mut b);
    let doc = parsed(&rows[0]);
    assert_eq!(doc["status"], serde_json::Value::Null, "{doc}");
    assert_eq!(doc["user"], serde_json::json!("bob"), "{doc}");
    assert!(env.errors.entries().is_empty(), "{:?}", env.errors.entries());
    b.reclaim(&mut env.pool);
}

#[test]
fn a_declared_field_that_does_not_coerce_fails_only_that_field() {
    let plan = compile_ok(vec![code(0, "status: i64; Object.assign($, kv(_raw));")]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_raw(&mut env, &mut b, &["status=oops"]);

    let rows = env.run(&plan, &mut b);
    assert_eq!(
        parsed(&rows[0]),
        parsed(br#"{"_raw":"status=oops"}"#),
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

#[test]
fn a_value_runs_to_the_separator_before_the_next_key() {
    let plan = compile_ok(vec![code(0, "Object.assign($, kv(_raw));")]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_raw(&mut env, &mut b, &["msg=user logged in level=info debug"]);

    let rows = env.run(&plan, &mut b);
    assert_eq!(
        parsed(&rows[0]),
        parsed(br#"{"_raw":"msg=user logged in level=info debug","msg":"user logged in","level":"info debug"}"#)
    );
    b.reclaim(&mut env.pool);
}

#[test]
fn without_next_key_a_word_with_no_kv_separator_is_a_bare_key_holding_true() {
    let plan = compile_ok(vec![code(0, "Object.assign($, kv(_raw, {nextKey: false}));")]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_raw(&mut env, &mut b, &["level=info debug"]);

    let rows = env.run(&plan, &mut b);
    assert_eq!(
        parsed(&rows[0]),
        parsed(br#"{"_raw":"level=info debug","level":"info","debug":"true"}"#)
    );
    assert!(env.errors.entries().is_empty());
    b.reclaim(&mut env.pool);
}

#[test]
fn custom_separators() {
    let plan = compile_ok(vec![code(
        0,
        r#"Object.assign($, kv(_raw, {pairSep: ",", kvSep: ":", quote: null}));"#,
    )]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_raw(&mut env, &mut b, &["a:1,b:2"]);

    let rows = env.run(&plan, &mut b);
    assert_eq!(parsed(&rows[0]), parsed(br#"{"_raw":"a:1,b:2","a":"1","b":"2"}"#));
    b.reclaim(&mut env.pool);
}

/// A key-value key is split at its dots by default; without `splitDots` it is one key.
#[test]
fn a_dotted_key_nests_unless_split_dots_is_off() {
    for (body, want) in [
        ("Object.assign($, kv(_raw));", r#"{"a":{"b":"1","c":"2"}}"#),
        (
            "Object.assign($, kv(_raw, {splitDots: false}));",
            r#"{"a.b":"1","a.c":"2"}"#,
        ),
    ] {
        let plan = compile_ok(vec![code(0, body)]);
        let mut env = Env::new();
        let mut b = Batch::new(&plan.slot_layout, 1, 0);
        seed_raw(&mut env, &mut b, &["a.b=1 a.c=2"]);
        let rows = env.run(&plan, &mut b);
        let mut row = parsed(&rows[0]);
        row.as_object_mut().unwrap().remove("_raw");
        assert_eq!(row, parsed(want.as_bytes()), "{body}");
        b.reclaim(&mut env.pool);
    }
}

/// A configured step carries `escapes` into the call it lowers to.
#[test]
fn a_kv_step_with_escapes_reads_an_escaped_pair_separator() {
    use smeltr::plan::config::{FunctionKind, KvConfig, ParserKind};
    let kind = ParserKind::KeyValue(KvConfig {
        escapes: true,
        ..KvConfig::default()
    });
    let step = FunctionKind::Parser {
        input: "_raw".into(),
        into: String::new(),
        kind,
        declared: Vec::new(),
    };
    let plan = compile_ok(vec![func(0, step)]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_raw(&mut env, &mut b, &[r"msg=a\ b"]);

    let rows = env.run(&plan, &mut b);
    assert_eq!(parsed(&rows[0]), parsed(br#"{"_raw":"msg=a\\ b","msg":"a b"}"#));
    b.reclaim(&mut env.pool);
}

/// `cef` names the header fields as the CEF specification does and each extension key as written.
#[test]
fn cef_reads_the_header_and_the_extension() {
    let plan = compile_ok(vec![code(0, "severity: i64; Object.assign($, cef(_raw));")]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 2, 0);
    seed_raw(
        &mut env,
        &mut b,
        &[
            r"<134>host CEF:0|Acme|FW|1.0|100|Blocked|5|msg=disk full src=10.0.0.1",
            "CEF:0|a",
        ],
    );

    let rows = env.run(&plan, &mut b);
    let row = parsed(&rows[0]);
    assert_eq!(
        (&row["deviceVendor"], &row["severity"]),
        (&serde_json::json!("Acme"), &serde_json::json!(5))
    );
    assert_eq!(
        (&row["msg"], &row["src"]),
        (&serde_json::json!("disk full"), &serde_json::json!("10.0.0.1"))
    );
    assert_eq!(parsed(&rows[1])["version"], "0", "a cut record keeps what it read");
    assert!(env.errors.entries().iter().all(|a| a.kind == ExceptionKind::Unparsable));
    assert_eq!(env.errors.error_rows(), [1]);
    b.reclaim(&mut env.pool);
}
