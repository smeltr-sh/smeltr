//! End-to-end tests for the CSV parser kernel: `_raw` CSV lines in,
//! serialized JSON out through `plan::output::run_to_json`.

use smeltr::core::batch::Batch;
use smeltr::core::column::DataType;
use smeltr::core::ctx::Ctx;
use smeltr::core::error::ExceptionKind;
use smeltr::core::id::FunctionId;
use smeltr::core::tally::FieldTally;
use smeltr::plan::config::{CsvConfig, DeclaredField, FunctionKind, ParserKind, Step};

use crate::harness;
use harness::*;

/// A csv parser step over `headers`, declaring each of `declared`.
fn csv_step(id: FunctionId, headers: &[&str], declared: &[(&str, DataType)]) -> Step {
    let declared = declared
        .iter()
        .map(|(n, t)| DeclaredField {
            name: n.to_string(),
            ty: t.clone(),
            time_source: None,
        })
        .collect();
    let config = CsvConfig {
        split_dots: false,
        delimiter: b',',
        headers: headers.iter().map(|h| Some(h.to_string())).collect(),
        ..CsvConfig::default()
    };
    func(
        id,
        FunctionKind::Parser {
            input: "_raw".into(),
            into: "$".into(),
            kind: ParserKind::Csv(config),
            declared,
        },
    )
}

// ---------------------------------------------------------------------------

/// The tally counts what each row held before the declared type was applied. An unquoted value that is not a
/// number is text to the tally and a failure besides, which is what keeps a proposal honest about a column that is
/// mostly numbers; a quoted number is text to the tally and a number to the column.
#[test]
fn the_tally_calls_unparsed_text_a_string() {
    let plan = compile_ok(vec![code(0, r#"n: i64; Object.assign($, csv(_raw, ["n"]));"#)]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 4, 0);
    seed_raw(&mut env, &mut b, &["42", "\"42\"", "abc", "  7 "]);

    let mut tally = FieldTally::new();
    {
        let mut ctx = Ctx::new(&mut env.pool, &mut env.errors).tallying(&mut tally);
        smeltr::plan::exec::run(&plan, &mut b, &mut ctx);
    }
    let n = tally.parses()[0]
        .fields
        .iter()
        .find(|f| f.name == "n")
        .expect("the declared field is tallied");
    assert_eq!(
        (n.counts.present, n.counts.ints, n.counts.strings, n.failed),
        (4, 2, 2, 1),
        "{:?}",
        n
    );
    b.reclaim(&mut env.pool);
}

/// A masked row is not parsed at all: a line that would not split, or a value that is not its declared type, is
/// neither marked nor counted against the row, and the field keeps what it had. The rows the guard selects parse
/// as before, malformed ones included: a selected row that fails is marked and its field keeps what it had, since the
/// parser assigns it by presence.
#[test]
fn a_guarded_parser_leaves_a_masked_row_alone() {
    let plan = compile_ok(vec![code(
        0,
        r#"kind = _raw.startsWith("traffic") ? "traffic" : "threat";
        amt = -1;
        if (kind == "traffic") {
            Object.assign($, csv(_raw, ["kind", "amt"]));
        }"#,
    )]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 5, 0);
    seed_raw(
        &mut env,
        &mut b,
        &[
            "traffic,42",
            "threat,\"unterminated",
            "traffic,\"7",
            "threat,text",
            "traffic,text",
        ],
    );
    let rows = env.run(&plan, &mut b);
    assert_eq!(parsed(&rows[0])["amt"], serde_json::json!(42));
    assert_eq!(parsed(&rows[1])["amt"], serde_json::json!(-1), "masked: kept its seed");
    assert_eq!(
        parsed(&rows[2])["amt"],
        serde_json::json!(7),
        "selected and cut off: keeps what it read"
    );
    assert_eq!(
        parsed(&rows[3])["amt"],
        serde_json::json!(-1),
        "masked: its type was never checked"
    );
    assert_eq!(
        parsed(&rows[4])["amt"],
        serde_json::json!(-1),
        "selected and not a number: kept its seed"
    );
    let rows_with = |want: fn(&ExceptionKind) -> bool| -> Vec<u32> {
        env.errors
            .entries()
            .iter()
            .filter(|a| want(&a.kind))
            .map(|a| a.row)
            .collect()
    };
    assert_eq!(
        rows_with(|k| matches!(k, ExceptionKind::Unparsable)),
        vec![2],
        "only the selected cut-off row is a parse failure"
    );
    assert_eq!(
        rows_with(|k| matches!(k, ExceptionKind::UnparsableField { .. })),
        vec![4],
        "only the selected row with text for a number is a field failure"
    );
    b.reclaim(&mut env.pool);
}

/// A guarded parser that declares nothing has no blend after it, so nothing after the parse reads the guard. The
/// parse itself does, and liveness says so; otherwise the guard's slot is the nested guard's to free, and it is
/// reclaimed under the parse before it runs.
#[test]
fn a_guarded_parser_with_no_field_still_holds_its_guard() {
    let plan = compile_ok(vec![code(
        0,
        r#"kind = _raw.startsWith("a") ? "a" : "b";
        z: i64 = null;
        if (kind == "a") {
            // Without the parse, the nested guard is the outer guard's last reader.
            if (true) {
                z = 1;
            }
            Object.assign($, csv(_raw, ["kind", "amt"]));
        }"#,
    )]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 2, 0);
    seed_raw(&mut env, &mut b, &["a,1", "b,2"]);
    let rows = env.run(&plan, &mut b);
    assert_eq!(parsed(&rows[0])["z"], serde_json::json!(1));
    assert_eq!(parsed(&rows[1])["z"], serde_json::Value::Null, "masked: never assigned");
    b.reclaim(&mut env.pool);
}

/// Two `Parser` steps, each under its own (mutually exclusive) guard, declaring the same field name: the second
/// one's blend composes against whatever the first one left there, exactly the way two guarded `Eval` assignments
/// to the same name already compose -- each guard's own row gets that step's real value, and a row neither guard
/// selects stays null.
#[test]
fn two_guarded_parsers_on_the_same_field_compose() {
    let plan = compile_ok(vec![
        // Its table names only `kind`: a column the table names is a field, so naming `amt` would bind it as `str`.
        csv_step(0, &["kind"], &[("kind", DataType::Str)]),
        Step::If {
            id: 3,
            cond: "kind == \"a\"".into(),
            then: vec![csv_step(1, &["kind", "amt"], &[("amt", DataType::I64)])],
            else_: vec![],
        },
        Step::If {
            id: 4,
            cond: "kind == \"b\"".into(),
            then: vec![csv_step(2, &["kind", "amt"], &[("amt", DataType::I64)])],
            else_: vec![],
        },
    ]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 3, 0);
    seed_raw(&mut env, &mut b, &["a,100", "b,200", "c,300"]);
    let rows = env.run(&plan, &mut b);
    assert_eq!(parsed(&rows[0])["amt"], serde_json::json!(100));
    assert_eq!(parsed(&rows[1])["amt"], serde_json::json!(200));
    assert!(
        parsed(&rows[2]).get("amt").is_none(),
        "neither guard selected this row, so `amt` was never bound and is omitted"
    );
    b.reclaim(&mut env.pool);
}

#[test]
fn headers_name_columns_positionally() {
    let plan = compile_ok(vec![code(
        0,
        r#"id: i64; name: str; status: i64; Object.assign($, csv(_raw, ["id", "name", "status"]));"#,
    )]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_raw(&mut env, &mut b, &["1,alice,200"]);

    let rows = env.run(&plan, &mut b);
    assert_eq!(
        parsed(&rows[0]),
        parsed(br#"{"_raw":"1,alice,200","id":1,"name":"alice","status":200}"#)
    );
    b.reclaim(&mut env.pool);
}

/// A record carries no header row, so a column past the table has nothing to name it and is dropped. Keeping one is a
/// matter of naming it in the table.
#[test]
fn columns_beyond_the_table_are_dropped() {
    let plan = compile_ok(vec![code(0, r#"a: i64; Object.assign($, csv(_raw, ["a"]));"#)]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_raw(&mut env, &mut b, &["1,2,3"]);

    let rows = env.run(&plan, &mut b);
    assert_eq!(parsed(&rows[0]), parsed(br#"{"_raw":"1,2,3","a":1}"#));
    b.reclaim(&mut env.pool);
}

/// A `None` header is what `"skip"` compiles to: it holds its place, so the columns after it stay aligned, and makes
/// no field.
#[test]
fn a_skipped_column_holds_its_place_and_makes_no_field() {
    let plan = compile_ok(vec![code(
        0,
        r#"status: i64; Object.assign($, csv(_raw, ["status", null, null]));"#,
    )]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_raw(&mut env, &mut b, &["200,true,web01"]);

    let rows = env.run(&plan, &mut b);
    assert_eq!(parsed(&rows[0]), parsed(br#"{"_raw":"200,true,web01","status":200}"#));
    b.reclaim(&mut env.pool);
}

#[test]
fn a_quoted_field_can_contain_the_delimiter() {
    let plan = compile_ok(vec![code(
        0,
        r#"a: str; b: str; Object.assign($, csv(_raw, ["a", "b"]));"#,
    )]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_raw(&mut env, &mut b, &[r#""hello, world",done"#]);

    let rows = env.run(&plan, &mut b);
    assert_eq!(
        parsed(&rows[0]),
        parsed(br#"{"_raw":"\"hello, world\",done","a":"hello, world","b":"done"}"#)
    );
    b.reclaim(&mut env.pool);
}

#[test]
fn a_declared_field_that_does_not_coerce_fails_only_that_field() {
    let plan = compile_ok(vec![code(
        0,
        r#"status: i64; Object.assign($, csv(_raw, ["status"]));"#,
    )]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_raw(&mut env, &mut b, &["not-a-number"]);

    let rows = env.run(&plan, &mut b);
    assert_eq!(
        parsed(&rows[0]),
        parsed(br#"{"_raw":"not-a-number"}"#),
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
fn a_declared_str_field_accepts_a_quoted_numeric_looking_value() {
    let plan = compile_ok(vec![code(0, r#"zip: str; Object.assign($, csv(_raw, ["zip"]));"#)]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_raw(&mut env, &mut b, &[r#""00501""#]);

    let rows = env.run(&plan, &mut b);
    assert_eq!(parsed(&rows[0]), parsed(br#"{"_raw":"\"00501\"","zip":"00501"}"#));
    b.reclaim(&mut env.pool);
}

#[test]
fn doubled_quotes_decode_to_one_literal_quote() {
    let plan = compile_ok(vec![code(0, r#"msg: str; Object.assign($, csv(_raw, ["msg"]));"#)]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_raw(&mut env, &mut b, &[r#""say ""hi""""#]);

    let rows = env.run(&plan, &mut b);
    assert_eq!(
        parsed(&rows[0]),
        parsed(br#"{"_raw":"\"say \"\"hi\"\"\"","msg":"say \"hi\""}"#)
    );
    b.reclaim(&mut env.pool);
}

#[test]
fn an_unterminated_quote_keeps_what_it_read_and_is_a_parse_failure_for_that_row_only() {
    let plan = compile_ok(vec![code(0, r#"a: str; Object.assign($, csv(_raw, ["a"]));"#)]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 2, 0);
    seed_raw(&mut env, &mut b, &[r#""unterminated"#, "ok"]);

    let rows = env.run(&plan, &mut b);
    assert_eq!(
        parsed(&rows[0]),
        parsed(br#"{"_raw":"\"unterminated","a":"unterminated"}"#)
    );
    assert_eq!(parsed(&rows[1]), parsed(br#"{"_raw":"ok","a":"ok"}"#));
    assert!(
        env.errors
            .entries()
            .iter()
            .any(|a| a.row == 0 && matches!(a.kind, ExceptionKind::Unparsable)),
    );
    b.reclaim(&mut env.pool);
}

#[test]
fn tab_delimiter() {
    let plan = compile_ok(vec![code(
        0,
        r#"a: i64; b: i64; Object.assign($, csv(_raw, ["a", "b"], {delimiter: "\t"}));"#,
    )]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_raw(&mut env, &mut b, &["1\t2"]);

    let rows = env.run(&plan, &mut b);
    assert_eq!(parsed(&rows[0]), parsed(br#"{"_raw":"1\t2","a":1,"b":2}"#));
    b.reclaim(&mut env.pool);
}

/// A declared numeric field is trimmed before coercion; a declared `Str`
/// is not.
#[test]
fn a_declared_numeric_field_tolerates_surrounding_whitespace() {
    let plan = compile_ok(vec![code(
        0,
        r#"status: i64; name: str; Object.assign($, csv(_raw, ["status", "name"]));"#,
    )]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_raw(&mut env, &mut b, &["  200  , spaced  "]);
    let rows = env.run(&plan, &mut b);
    let got = parsed(&rows[0]);
    assert_eq!(got["status"], serde_json::json!(200));
    assert_eq!(got["name"], serde_json::json!(" spaced  "));
    b.reclaim(&mut env.pool);
}

/// The datetime sugar is not JSON-specific: CSV stages every value through `Str`, the same text a JSON-declared field
/// is pinned to, so a column of epoch digits here proves the conversion runs after every `ParserKind`. The format
/// form is the PAN-OS shape, which is also one auto-detect knows; the format pins it anyway.
#[test]
fn a_declared_datetime_column_becomes_a_real_datetime() {
    let auto = r#"ts: datetime; Object.assign($, csv(_raw, ["ts"])); back = epochMillis(ts);"#;
    let format = r#"ts: datetime;
        Object.assign($, csv(_raw, ["ts"], {formats: {ts: "%Y/%m/%d %H:%M:%S"}}));
        back = epochMillis(ts);"#;
    for (body, text) in [
        (auto, "1700000000000"),
        (auto, "2023/11/14 22:13:20"),
        (format, "2023/11/14 22:13:20"),
    ] {
        let plan = compile_ok(vec![code(0, body)]);
        let mut env = Env::new();
        let mut b = Batch::new(&plan.slot_layout, 1, 0);
        seed_raw(&mut env, &mut b, &[text]);
        let rows = env.run(&plan, &mut b);
        assert_eq!(
            parsed(&rows[0])["back"],
            serde_json::json!(1_700_000_000_000i64),
            "{body} over {text:?}"
        );
        b.reclaim(&mut env.pool);
    }
}

/// A blank cell under a declared number, bool or datetime is an empty cell, not a value that failed: the field is
/// null, nothing is marked, and the row is clean. Quoted or padded, the same. A declared `str` keeps its `""`.
#[test]
fn a_blank_cell_under_a_typed_column_is_null_and_clean() {
    let plan = compile_ok(vec![code(
        0,
        r#"i: i64; f: f64; b: bool; ts: datetime; s: str;
        Object.assign($, csv(_raw, ["i", "f", "b", "ts", "s"]));"#,
    )]);
    let mut env = Env::new();
    let lines = [
        ",,,,",
        r#""","","","","""#,
        "  ,  ,  ,  ,  ",
        "7,1.5,true,1700000000000,x",
    ];
    let mut b = Batch::new(&plan.slot_layout, lines.len(), 0);
    seed_raw(&mut env, &mut b, &lines);
    let rows = env.run(&plan, &mut b);
    for (row, want_s) in [(0, ""), (1, ""), (2, "  ")] {
        let doc = parsed(&rows[row]);
        for f in ["i", "f", "b", "ts"] {
            assert_eq!(doc[f], serde_json::Value::Null, "row {row} `{f}`: {doc}");
        }
        assert_eq!(doc["s"], serde_json::json!(want_s), "row {row}: {doc}");
    }
    let doc = parsed(&rows[3]);
    assert_eq!(doc["i"], serde_json::json!(7));
    assert_eq!(doc["f"], serde_json::json!(1.5));
    assert_eq!(doc["b"], serde_json::json!(true));
    assert_eq!(doc["s"], serde_json::json!("x"));
    assert!(env.errors.entries().is_empty(), "{:?}", env.errors.entries());
    b.reclaim(&mut env.pool);
}

/// The tally does not count a blank under a typed column as present: a column of numbers with some empty cells
/// proposes as what it is, `present` short of `rows` and every present value an int.
#[test]
fn the_tally_does_not_count_a_blank_under_a_typed_column() {
    let plan = compile_ok(vec![code(0, r#"n: i64; Object.assign($, csv(_raw, ["n"]));"#)]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 4, 0);
    seed_raw(&mut env, &mut b, &["42", "", "7", "\"\""]);

    let mut tally = FieldTally::new();
    {
        let mut ctx = Ctx::new(&mut env.pool, &mut env.errors).tallying(&mut tally);
        smeltr::plan::exec::run(&plan, &mut b, &mut ctx);
    }
    let n = &tally.parses()[0].fields[0];
    assert_eq!(
        (n.rows, n.counts.present, n.counts.ints, n.counts.strings, n.failed),
        (4, 2, 2, 0, 0),
        "{:?}",
        n
    );
    b.reclaim(&mut env.pool);
}

/// A declared field no column is named is a field the parse never writes, so it is null; the column it was meant for
/// is a `str` field of its own.
#[test]
fn a_declared_field_no_header_names_is_null() {
    let plan = compile_ok(vec![code(
        0,
        r#"status: i64; hsot: str; Object.assign($, csv(_raw, ["status", "host"]));"#,
    )]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_raw(&mut env, &mut b, &["200,web01"]);
    let rows = env.run(&plan, &mut b);
    assert_eq!(
        parsed(&rows[0]),
        parsed(br#"{"_raw":"200,web01","status":200,"host":"web01"}"#)
    );
    b.reclaim(&mut env.pool);
}

/// An empty column table names nothing, so a declared field is null on every row.
#[test]
fn a_declared_name_is_null_when_the_column_table_is_empty() {
    let plan = compile_ok(vec![code(0, r#"status: i64; Object.assign($, csv(_raw, []));"#)]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_raw(&mut env, &mut b, &["200"]);
    let rows = env.run(&plan, &mut b);
    assert_eq!(parsed(&rows[0]), parsed(br#"{"_raw":"200"}"#));
    b.reclaim(&mut env.pool);
}

/// A column routes by name, so two columns of one name write one field and the rightmost wins. That holds when the other
/// is a `skip`, which is the case most likely to be typed by accident: it produces nothing, yet its position still
/// overwrote the kept one.
#[test]
fn two_columns_of_one_name_are_refused() {
    let errs = compile_err(vec![code(0, r#"x: i64; Object.assign($, csv(_raw, ["x", "x"]));"#)]);
    assert_eq!(errs.len(), 1, "{errs:?}");
    assert!(
        errs[0].to_string().contains("more than one column is named `x`"),
        "{:?}",
        errs[0]
    );
}

/// Skipped columns have no name, so any number of them sit in one table -- a vendor format with repeated filler
/// columns is ordinary.
#[test]
fn skipped_columns_need_no_name() {
    compile_ok(vec![code(
        0,
        r#"kept: str; Object.assign($, csv(_raw, [null, "kept", null]));"#,
    )]);
}

/// A configured step carries its dialect options into the call it lowers to.
#[test]
fn a_csv_step_keeps_its_dialect_options() {
    let config = CsvConfig {
        delimiter: b' ',
        headers: vec![Some("a".into()), Some("b".into())],
        quote: b'\'',
        escape: Some(b'\\'),
        squeeze: true,
        trim: true,
        ..CsvConfig::default()
    };
    let declared = ["a", "b"].map(|n| DeclaredField {
        name: n.into(),
        ty: DataType::Str,
        time_source: None,
    });
    let step = FunctionKind::Parser {
        input: "_raw".into(),
        into: "$".into(),
        kind: ParserKind::Csv(config),
        declared: declared.into(),
    };
    let plan = compile_ok(vec![func(0, step)]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_raw(&mut env, &mut b, &[r"'x\'y'   z"]);

    let rows = env.run(&plan, &mut b);
    assert_eq!(parsed(&rows[0]), parsed(br#"{"_raw":"'x\\'y'   z","a":"x'y","b":"z"}"#));
    b.reclaim(&mut env.pool);
}

/// A step that rebuilds its input leaves each skipped column empty and keeps every other byte, quotes and all, so
/// each column after a skipped one keeps its place.
#[test]
fn a_rebuilding_step_leaves_each_skipped_column_empty() {
    let config = CsvConfig {
        headers: vec![Some("status".into()), None, Some("host".into()), None],
        rebuild: true,
        ..CsvConfig::default()
    };
    let declared = ["status", "host"].map(|n| DeclaredField {
        name: n.into(),
        ty: DataType::Str,
        time_source: None,
    });
    let step = FunctionKind::Parser {
        input: "_raw".into(),
        into: "$".into(),
        kind: ParserKind::Csv(config),
        declared: declared.into(),
    };
    let plan = compile_ok(vec![func(0, step)]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 2, 0);
    seed_raw(&mut env, &mut b, &[r#"200,"a,b","web 01",x"#, "500,,db,"]);

    let rows = env.run(&plan, &mut b);
    assert_eq!(
        parsed(&rows[0]),
        parsed(br#"{"_raw":"200,,\"web 01\",","status":"200","host":"web 01"}"#)
    );
    assert_eq!(
        parsed(&rows[1]),
        parsed(br#"{"_raw":"500,,db,","status":"500","host":"db"}"#)
    );
    b.reclaim(&mut env.pool);
}

/// With nothing skipped there is nothing to leave out, and the line is the line it read.
#[test]
fn a_rebuilding_step_that_skips_nothing_keeps_the_line() {
    let config = CsvConfig {
        headers: vec![Some("a".into())],
        rebuild: true,
        ..CsvConfig::default()
    };
    let step = FunctionKind::Parser {
        input: "_raw".into(),
        into: "$".into(),
        kind: ParserKind::Csv(config),
        declared: Vec::new(),
    };
    let plan = compile_ok(vec![func(0, step)]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 1, 0);
    seed_raw(&mut env, &mut b, &[r#" "x" ,y"#]);

    let rows = env.run(&plan, &mut b);
    assert_eq!(parsed(&rows[0]), parsed(br#"{"_raw":" \"x\" ,y","a":" \"x\" "}"#));
    b.reclaim(&mut env.pool);
}

/// `csvRebuild` is a function code may call: a squeezed run of delimiters is one, so the column it blanks is the
/// one `csv()` reads, and a line that does not split whole comes back as it was.
#[test]
fn csv_rebuild_blanks_what_csv_skips() {
    let plan = compile_ok(vec![code(
        0,
        r#"_raw = csvRebuild(_raw, ["a", null, "c"], " ", "\"", "", true);"#,
    )]);
    let mut env = Env::new();
    let mut b = Batch::new(&plan.slot_layout, 2, 0);
    seed_raw(&mut env, &mut b, &["1   2  3", r#"1 "2 3"#]);

    let rows = env.run(&plan, &mut b);
    assert_eq!(parsed(&rows[0]), parsed(br#"{"_raw":"1     3"}"#));
    assert_eq!(parsed(&rows[1]), parsed(br#"{"_raw":"1 \"2 3"}"#));
    b.reclaim(&mut env.pool);
}
