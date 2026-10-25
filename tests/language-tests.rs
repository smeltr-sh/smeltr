//! The language's base cases: what a pipeline must do, each as its code, its
//! input records and its output or compile error. A case the compiler does
//! not meet yet is `#[ignore]`d with the work that builds it; `just check`
//! runs those too, reporting without failing, until each is taken off.
//!
//! A case is written in the code the plan lowers every step to, and compiles
//! as one code step.
//!
//! Output key order is not part of a case, and neither are the failure tag's
//! fields. An exception or a compile error is checked for the backticked
//! names it must hold, not its wording.

use serde_json::Value;
use smeltr::core::buffer::BufferPool;
use smeltr::core::ctx::Ctx;
use smeltr::core::error::ErrorSink;
use smeltr::plan::config::LookupSource;
use smeltr::plan::output::run_to_json_marked;
use smeltr::plan::plan::compile;
use smeltr::plan::topology::SourceMeta;

use crate::harness;
use harness::*;

/// What a case must give.
enum Expect {
    /// One output per input row, and the text its exception holds, if it has one.
    Rows(&'static [(&'static str, Option<&'static str>)]),
    /// A compile error holding each of these.
    Error(&'static [&'static str]),
}
use Expect::*;

/// A JSON value whose every object holds each key once: a `Value` would keep only the last of a repeated key.
struct OnceEach;

impl<'de> serde::Deserialize<'de> for OnceEach {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        d.deserialize_any(OnceEach)
    }
}

impl<'de> serde::de::Visitor<'de> for OnceEach {
    type Value = OnceEach;

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("a JSON value")
    }
    fn visit_bool<E>(self, _: bool) -> Result<OnceEach, E> {
        Ok(OnceEach)
    }
    fn visit_i64<E>(self, _: i64) -> Result<OnceEach, E> {
        Ok(OnceEach)
    }
    fn visit_u64<E>(self, _: u64) -> Result<OnceEach, E> {
        Ok(OnceEach)
    }
    fn visit_f64<E>(self, _: f64) -> Result<OnceEach, E> {
        Ok(OnceEach)
    }
    fn visit_str<E>(self, _: &str) -> Result<OnceEach, E> {
        Ok(OnceEach)
    }
    fn visit_unit<E>(self) -> Result<OnceEach, E> {
        Ok(OnceEach)
    }
    fn visit_seq<A: serde::de::SeqAccess<'de>>(self, mut a: A) -> Result<OnceEach, A::Error> {
        while a.next_element::<OnceEach>()?.is_some() {}
        Ok(OnceEach)
    }
    fn visit_map<A: serde::de::MapAccess<'de>>(self, mut a: A) -> Result<OnceEach, A::Error> {
        let mut seen = std::collections::HashSet::new();
        while let Some(k) = a.next_key::<String>()? {
            if !seen.insert(k.clone()) {
                return Err(serde::de::Error::custom(format!("a path written twice: `{k}`")));
            }
            a.next_value::<OnceEach>()?;
        }
        Ok(OnceEach)
    }
}

// -- running ----------------------------------------------------------------

/// Panics unless `code` over `input` gives `expect`.
fn case(code: &str, input: &[&str], expect: Expect) {
    let mut p = pipeline(vec![harness::code(0, code.trim())]);
    let codes = LookupSource::Inline {
        headers: vec!["code".into(), "message".into()],
        rows: vec![vec!["200".into(), "ok".into()], vec!["404".into(), "not found".into()]],
    };
    p.lookup_tables.insert("codes".into(), Ok(codes));
    let plan = compile(&p, &SourceMeta::KAFKA);
    let rows = match (plan, expect) {
        (Err(e), Error(want)) => {
            let got = e.iter().map(|x| x.to_string()).collect::<Vec<_>>().join("; ");
            assert!(want.iter().all(|w| got.contains(w)), "`{code}` gave error: {got}");
            return;
        }
        (Err(e), Rows(_)) => panic!(
            "`{code}` gave error: {}",
            e.iter().map(|x| x.to_string()).collect::<Vec<_>>().join("; ")
        ),
        (Ok(_), Error(want)) => panic!("`{code}` compiled; expected an error holding {want:?}"),
        (Ok(plan), Rows(rows)) => (plan, rows),
    };
    let (plan, want) = rows;
    let mut pool = BufferPool::new();
    let mut errors = ErrorSink::new();
    let mut batch = raw_batch(&plan, input, &mut pool);
    let mut ctx = Ctx::new(&mut pool, &mut errors);
    let mut got = run_to_json_marked(&plan, &Default::default(), &mut batch, &mut ctx, false, None);
    // A dropped row has no output.
    got.retain(|r| r.json.is_some());
    assert_eq!(got.len(), want.len(), "`{code}`: row count");
    for (r, (want_row, want_note)) in got.iter().zip(want) {
        let raw = r.json.as_deref().unwrap();
        let mut row: Value = serde_json::from_slice(raw).unwrap();
        serde_json::from_slice::<OnceEach>(raw).unwrap_or_else(|e| panic!("`{code}`: {e}"));
        for tag in ["_raw", "smeltr"] {
            row.as_object_mut().unwrap().remove(tag);
        }
        let notes = r
            .exceptions
            .iter()
            .map(|(_, m, _)| m.as_str())
            .collect::<Vec<_>>()
            .join("; ");
        let note_ok = match want_note {
            Some(w) => notes.contains(w),
            None => notes.is_empty(),
        };
        let want_row: Value = serde_json::from_str(want_row).unwrap();
        assert!(row == want_row && note_ok, "`{code}` gave {row} ! {notes:?}");
    }
    batch.reclaim(&mut pool);
}

/// One named test per case: `name: code, [input rows], expect;`, a marked case under its `#[ignore]`.
macro_rules! cases {
    ($($(#[$mark:meta])* $name:ident: $code:expr, [$($input:expr),*], $expect:expr;)*) => {$(
        #[test]
        $(#[$mark])*
        fn $name() {
            case($code, &[$($input),*], $expect)
        }
    )*};
}

const USER: &str = r#"{"u": {"name": "a", "age": 3}}"#;
const AB: &str = r#"{"a": {"b": 1}}"#;
const AB_Y: Expect = Rows(&[(r#"{"a": {"b": 1}, "y": 1}"#, None)]);
const ABC: &str = r#"{"a": {"b": 1, "c": 2}}"#;
const K8S: &str = r#"{"labels": {"app": "cart", "app.kubernetes.io/name": "cart"}}"#;

cases! {
    // -- parsers
    a_parser_coerces_a_declared_key_and_keeps_the_rest: "a: i64; Object.assign($, json(_raw))",
        [r#"{"a": "5", "b": 1}"#], Rows(&[(r#"{"a": 5, "b": 1}"#, None)]);
    a_declared_key_that_does_not_coerce_is_null_and_marked: "a: i64; Object.assign($, json(_raw))",
        [r#"{"a": "x"}"#], Rows(&[("{}", Some(r#"_raw.a is "x", which could not parse as i64 in a"#))]);
    a_scalar_where_an_object_is_declared_names_its_key_and_value: "a: {b: i64}; Object.assign($, json(_raw))",
        [r#"{"a": 5}"#], Rows(&[("{}", Some(r#"_raw.a is 5, which could not parse as {b: i64} in a"#))]);
    a_parser_may_not_declare_a_bound_name: "a: i64 = 7; a: i64; Object.assign($, json(_raw))",
        [r#"{"b": 1}"#], Error(&["`a`"]);
    a_parser_declares_a_nested_path: "s.port: i64; Object.assign($, json(_raw))",
        [r#"{"s": {"ip": "1.2.3.4", "port": "443"}}"#], Rows(&[(r#"{"s": {"ip": "1.2.3.4", "port": 443}}"#, None)]);
    a_missing_key_converts_to_null_and_is_not_written: "Object.assign($, json(_raw)) u: str = $.user;",
        [r#"{"b": 1}"#], Rows(&[(r#"{"b": 1}"#, None)]);
    after_csv_the_root_has_only_its_columns: r#"Object.assign($, csv(_raw, ["a", "b"])) u = $.user;"#,
        ["x,y"], Error(&["`user`"]);
    a_key_the_row_lacks_keeps_the_prior_value: "a: i64; a = 7; Object.assign($, json(_raw))",
        [r#"{"b": 1}"#], Rows(&[(r#"{"a": 7, "b": 1}"#, None)]);

    // -- a name's type is stated once
    a_plain_write_of_another_type_names_as: r#"x: i64; x = "5";"#,
        ["{}"], Error(&["`x`", r#"`"5" as i64`"#]);
    a_value_that_fails_on_every_row_is_a_compile_error: r#"x: i64; x = "a";"#,
        ["{}"], Error(&["`x`", r#"`x: str = "a"`"#]);
    a_bare_declaration_after_an_open_parse_is_a_compile_error: "Object.assign($, json(_raw)) x: i64;",
        [r#"{"v": "a"}"#], Error(&["`x`", "`x: i64 = x`"]);
    a_plain_assignment_does_not_change_a_type: r#"y: array<str>; y = "x";"#,
        ["{}"], Error(&["`y`"]);
    a_typed_assignment_changes_a_type: r#"y: array<str>; y: str = "x";"#,
        ["{}"], Rows(&[(r#"{"y": "x"}"#, None)]);
    restating_a_bound_names_type_is_a_compile_error: "x = 1; x: i64;",
        ["{}"], Error(&["`x`"]);
    declaring_a_bound_name_another_type_names_the_conversion: "x = 1; x: str;",
        ["{}"], Error(&["`x`", "`x: str = x as str`"]);
    declaring_a_bound_leaf_names_the_conversion: "o = {a: 1}; o.a: str;",
        ["{}"], Error(&["`o.a`", "`o.a: str = o.a as str`"]);
    a_plain_assignment_keeps_the_first_type: r#"x = 1; x = "a";"#,
        ["{}"], Error(&["`x`", r#"`x: str = "a"`"#]);
    a_float_name_takes_a_float_sum: "total: f64 = 0; total = total + 1.5;",
        ["{}"], Rows(&[(r#"{"total": 1.5}"#, None)]);
    a_null_first_write_takes_the_type_of_the_writes_after: "x = null; x = 5;",
        ["{}"], Rows(&[(r#"{"x": 5}"#, None)]);
    a_null_first_write_no_write_types_is_a_compile_error: "x = null; y = x;",
        ["{}"], Error(&["`x`", "`x: type = null`"]);
    a_scalar_does_not_become_an_object: "x = 1; x = {a: 1};",
        ["{}"], Error(&["`x`"]);

    // -- a typed read of a variant converts by the table, per row
    a_typed_read_of_a_variant_converts_per_row: "Object.assign($, json(_raw)) x: i64 = $.v;",
        [r#"{"v": 5}"#, r#"{"v": "5"}"#, r#"{"v": "five"}"#],
        Rows(&[(r#"{"v": 5, "x": 5}"#, None), (r#"{"v": "5", "x": 5}"#, None), (r#"{"v": "five"}"#, Some(r#""five", which could not convert to i64 in x"#))]);
    a_declaration_before_a_text_parser_converts: "srcport: i64; Object.assign($, kv(_raw))",
        ["srcport=443"], Rows(&[(r#"{"srcport": 443}"#, None)]);
    as_converts_a_value_of_a_known_type: r#"status: str; Object.assign($, grok(_raw, ["%{INT:status}"])) code: i64 = status as i64;"#,
        ["200"], Rows(&[(r#"{"status": "200", "code": 200}"#, None)]);

    // -- every conversion is loud
    a_parse_function_marks_a_failure: "s: str; Object.assign($, json(_raw)) n: i64 = parseInt(s);",
        [r#"{"s": "x"}"#], Rows(&[(r#"{"s": "x"}"#, Some(r#"s is "x", which could not convert to i64 in n"#))]);
    a_formatted_time_marks_a_failure: r#"t: str; Object.assign($, json(_raw)) w: datetime = parseTime(t, "dd/MMM/yyyy:HH:mm:ss Z");"#,
        [r#"{"t": "garbage"}"#], Rows(&[(r#"{"t": "garbage"}"#, Some(r#"t is "garbage", which could not convert to datetime in w"#))]);

    // -- a declaration types the name; `as` converts the value
    o3_a_different_object_does_not_fit: r#"o = {a: 1}; o = {b: "x"};"#,
        ["{}"], Error(&[r#"`{b: "x"}`"#, r#"`o: {b: str} = {b: "x"}`"#]);
    o4_a_variant_needs_as: "Object.assign($, json(_raw)) o = {a: 1}; o = $.user;",
        [r#"{"user": 7}"#], Error(&["`o`", "`user as {a: i64}`"]);
    l1_a_plain_write_of_a_variant_needs_as: "x: i64 = 0; Object.assign($, json(_raw)) x = $.v;",
        [r#"{"v": "5"}"#], Error(&["`x`", "`v as i64`"]);
    o5_an_open_value_needs_as: r#"o = {a: 1}; let pairs = [{key: "a", value: "5"}, {key: "z", value: "y"}]; o = fromEntries(pairs);"#,
        ["{}"], Error(&["`fromEntries(pairs) as {a: i64}`", "`o: {...} = fromEntries(pairs)`"]);
    o6_as_converts_the_value: r#"o = {a: 1}; let pairs = [{key: "a", value: "5"}, {key: "z", value: "y"}]; o = fromEntries(pairs) as {a: i64};"#,
        ["{}"], Rows(&[(r#"{"o": {"a": 5}}"#, None)]);
    o7_as_does_not_change_the_names_type: r#"o = {a: 1}; o = {b: "x"} as {b: str};"#,
        ["{}"], Error(&["`o`", r#"`o: {b: str} = {b: "x"}`"#]);

    // -- a write adds a field
    a_write_adds_a_field_after_csv: r#"Object.assign($, csv(_raw, ["a", "b"])) c = 1;"#,
        ["x,y"], Rows(&[(r#"{"a": "x", "b": "y", "c": 1}"#, None)]);
    a_write_adds_a_field_to_an_open_object: "Object.assign($, json(_raw)) o: {a: i64, ...} = o; o.b = 2;",
        [r#"{"o": {"a": 1, "z": 9}}"#], Rows(&[(r#"{"o": {"a": 1, "z": 9, "b": 2}}"#, None)]);
    a_write_adds_a_field_to_a_closed_object: "o = {a: 1}; o.b = 2;",
        ["{}"], Rows(&[(r#"{"o": {"a": 1, "b": 2}}"#, None)]);
    a_write_fills_a_declared_field: "o = {a: 1}; o: {a: i64, b: i64} = o as {a: i64, b: i64}; o.b = 2;",
        ["{}"], Rows(&[(r#"{"o": {"a": 1, "b": 2}}"#, None)]);

    // -- objects: open and closed
    a_closed_conversion_drops_other_keys: "Object.assign($, json(_raw)) u: {name: str} = u;",
        [USER], Rows(&[(r#"{"u": {"name": "a"}}"#, None)]);
    an_open_conversion_keeps_other_keys: "Object.assign($, json(_raw)) u: {name: str, ...} = u;",
        [USER], Rows(&[(r#"{"u": {"name": "a", "age": 3}}"#, None)]);
    an_open_object_is_not_implicitly_closed:
        "Object.assign($, json(_raw)) function n(p: {name: str}): str { return p.name; } u: {name: str, ...} = u; x = n(u);",
        [USER], Error(&["`u`", "`n(u as {name: str})`"]);
    a_closed_object_passes_as_open:
        r#"function n(p: {name: str, ...}): str { return p.name; } o = {name: "a"}; x = n(o);"#,
        ["{}"], Rows(&[(r#"{"o": {"name": "a"}, "x": "a"}"#, None)]);
    a_closed_object_has_only_its_fields: "o = {a: 1}; y = o.b;",
        ["{}"], Error(&["`o.b`"]);
    an_open_root_reads_an_undeclared_key: "Object.assign($, json(_raw)) y: str = $.k;",
        [r#"{"k": "z"}"#], Rows(&[(r#"{"k": "z", "y": "z"}"#, None)]);

    // -- paths and writes
    a_dotted_path_reads_a_key: "Object.assign($, json(_raw)) y: i64 = $.a.b;", [AB], AB_Y;

    // -- a key is a key: a dot in data is a character, a dot in code is a path step
    a_kubernetes_label_reads_by_its_key: r#"Object.assign($, json(_raw)) name = labels["app.kubernetes.io/name"];"#,
        [K8S], Rows(&[(r#"{"labels": {"app": "cart", "app.kubernetes.io/name": "cart"}, "name": "cart"}"#, None)]);
    brackets_are_one_key_and_dots_a_path: r#"Object.assign($, json(_raw)) a = $["x.y"]; b = x.y; c = $["x"]["y"];"#,
        [r#"{"x.y": 1, "x": {"y": 2}}"#], Rows(&[(r#"{"x.y": 1, "x": {"y": 2}, "a": 1, "b": 2, "c": 2}"#, None)]);
    kv_splits_a_dotted_key: "Object.assign($, kv(_raw))",
        ["a.b=1 a.c=2"], Rows(&[(r#"{"a": {"b": "1", "c": "2"}}"#, None)]);
    kv_keys_that_contradict_are_marked: "Object.assign($, kv(_raw))",
        ["a=1 a.b=2"], Rows(&[(r#"{"a": {"b": "2"}}"#, Some(r#"_raw.a.b contradicts a key before it; the later was kept"#))]);
    a_key_repeated_in_an_array_element_is_marked: "Object.assign($, json(_raw))",
        [r#"{"a": [{"k": 1, "k": 2}]}"#], Rows(&[(r#"{"a": [{"k": 2}]}"#, Some(r#"_raw.a.k contradicts a key before it; the later was kept"#))]);
    a_flat_json_key_stays_one_key: "geo: {city: str}; Object.assign($, json(_raw))",
        [r#"{"geo.city": "Oslo"}"#], Rows(&[(r#"{"geo.city": "Oslo"}"#, None)]);
    split_dots_nests_a_flat_json_key: "geo: {city: str}; Object.assign($, json(_raw, {splitDots: true}))",
        [r#"{"geo.city": "Oslo"}"#], Rows(&[(r#"{"geo": {"city": "Oslo"}}"#, None)]);
    a_dotted_key_is_renamed_by_its_key: r#"Object.assign($, json(_raw)) labels["app_kubernetes_io/name"] = labels["app.kubernetes.io/name"]; labels["app.kubernetes.io/name"] = null;"#,
        [K8S], Rows(&[(r#"{"labels": {"app": "cart", "app_kubernetes_io/name": "cart"}}"#, None)]);
    a_key_holding_brackets_is_one_key: r#"a: array<i64>; Object.assign($, json(_raw)) x = $["a[0]"]; y = a[0];"#,
        [r#"{"a[0]": 1, "a": [7]}"#], Rows(&[(r#"{"a[0]": 1, "a": [7], "x": 1, "y": 7}"#, None)]);
    a_computed_key_is_one_key: r#"Object.assign($, json(_raw)) let k = "a.b"; x = $[k];"#,
        [r#"{"a.b": 1, "a": {"b": 2}}"#], Rows(&[(r#"{"a.b": 1, "a": {"b": 2}, "x": 1}"#, None)]);
    split_dots_splits_keys_inside_a_declared_object: "geo: {loc: {lat: f64}}; Object.assign($, json(_raw, {splitDots: true}))",
        [r#"{"geo": {"loc.lat": 1.5}}"#], Rows(&[(r#"{"geo": {"loc": {"lat": 1.5}}}"#, None)]);
    split_dots_splits_keys_inside_an_open_object: "g: {...}; Object.assign($, json(_raw, {splitDots: true}))",
        [r#"{"g": {"a.b": 1}}"#], Rows(&[(r#"{"g": {"a": {"b": 1}}}"#, None)]);
    csv_headers_are_keys: r#"Object.assign($, csv(_raw, ["src.ip", "bytes"]))"#,
        ["1.2.3.4,10"], Rows(&[(r#"{"src.ip": "1.2.3.4", "bytes": "10"}"#, None)]);
    csv_split_dots_nests_its_headers: r#"Object.assign($, csv(_raw, ["src.ip", "bytes"], {splitDots: true}))"#,
        ["1.2.3.4,10"], Rows(&[(r#"{"src": {"ip": "1.2.3.4"}, "bytes": "10"}"#, None)]);
    a_grok_capture_name_is_a_path: r#"Object.assign($, grok(_raw, ["%{IP:src.ip}"]))"#,
        ["1.2.3.4"], Rows(&[(r#"{"src": {"ip": "1.2.3.4"}}"#, None)]);
    a_grok_capture_name_without_split_dots_is_one_key: r#"Object.assign($, grok(_raw, ["%{IP:src.ip}"], {splitDots: false}))"#,
        ["1.2.3.4"], Rows(&[(r#"{"src.ip": "1.2.3.4"}"#, None)]);
    a_write_under_an_object_keeps_the_rest: "Object.assign($, json(_raw)) a.b = 5;",
        [ABC], Rows(&[(r#"{"a": {"b": 5, "c": 2}}"#, None)]);
    a_write_under_a_key_that_holds_no_object_replaces_it: "Object.assign($, json(_raw)) a.b = 5;",
        [r#"{"a": 3}"#], Rows(&[(r#"{"a": {"b": 5}}"#, None)]);
    delete_removes_a_path: "Object.assign($, json(_raw)) delete(a.b);",
        [ABC], Rows(&[(r#"{"a": {"c": 2}}"#, None)]);
    a_scalar_has_no_fields: "x = 1; x.y = 2;",
        ["{}"], Error(&["`x`"]);

    // -- guards, presence, spreads and moves
    a_guarded_write_runs_only_on_its_rows: "f: bool; Object.assign($, json(_raw)) if (f) x = 1;",
        [r#"{"f": true}"#, r#"{"f": false}"#], Rows(&[(r#"{"f": true, "x": 1}"#, None), (r#"{"f": false}"#, None)]);
    presence_takes_a_variant: r#"Object.assign($, json(_raw)) y: str = $.a ?? "none";"#,
        ["{}", r#"{"a": "z"}"#], Rows(&[(r#"{"y": "none"}"#, None), (r#"{"a": "z", "y": "z"}"#, None)]);
    a_later_spread_wins: "let p = {a: 1, b: 2}; let q = {b: 3}; o = {...p, ...q};",
        ["{}"], Rows(&[(r#"{"o": {"a": 1, "b": 3}}"#, None)]);
    a_rename_is_a_move: "Object.assign($, json(_raw)) let t = a.b; delete(a.b); a.d = t;",
        [ABC], Rows(&[(r#"{"a": {"c": 2, "d": 1}}"#, None)]);
    drop_removes_a_row: r#"Object.assign($, json(_raw)) level: str = $.level; if (level == "debug") drop();"#,
        [r#"{"level": "debug"}"#, r#"{"level": "info"}"#], Rows(&[(r#"{"level": "info"}"#, None)]);

    // -- the serializer and what a write leaves
    a_declared_key_is_written_once: "Object.assign($, json(_raw)) a: str = $.a;",
        [r#"{"z": 1, "a": "x"}"#], Rows(&[(r#"{"a": "x", "z": 1}"#, None)]);
    a_null_write_takes_a_key_out: "Object.assign($, json(_raw)) a = null;",
        [r#"{"a": 1, "b": 2}"#], Rows(&[(r#"{"b": 2}"#, None)]);
    a_guarded_first_write_keeps_the_records_key: "f: bool; Object.assign($, json(_raw)) if (f) { a.b = 5; }",
        [r#"{"f": true, "a": {"b": "1"}}"#, r#"{"f": false, "a": {"b": "1"}}"#, r#"{"f": false, "a": {"b": "x"}}"#],
        Rows(&[(r#"{"f": true, "a": {"b": 5}}"#, None), (r#"{"f": false, "a": {"b": 1}}"#, None), (r#"{"f": false}"#, Some(r#""x", which could not convert to i64 in a.b"#))]);
    a_keyed_write_to_a_new_name_makes_an_object: "Object.assign($, json(_raw)) let k: str = $.k; o[k] = 1;",
        [r#"{"k": "x"}"#], Rows(&[(r#"{"k": "x", "o": {"x": 1}}"#, None)]);
    a_variant_holds_a_datetime_exactly:
        r#"f: bool; Object.assign($, json(_raw)) let t = "2026-09-30T12:00:00Z" as datetime; let v = f ? t : 0; w: datetime = v;"#,
        [r#"{"f": true}"#], Rows(&[(r#"{"f": true, "w": "2026-09-30T12:00:00.000000Z"}"#, None)]);
    a_move_leaves_nothing_under_the_old_name: "Object.assign($, json(_raw)) let t = a; delete(a); c = t;",
        [AB], Rows(&[(r#"{"c": {"b": 1}}"#, None)]);

    // -- the record is a target
    a_record_write_replaces_the_record: "Object.assign($, json(_raw)) $ = {x: $.a};",
        [r#"{"a": 1, "b": 2}"#], Rows(&[(r#"{"x": 1}"#, None)]);
    a_record_write_may_spread_the_record: "Object.assign($, json(_raw)) $ = {...$, x: 1}; delete(a);",
        [r#"{"a": 1, "b": 2}"#], Rows(&[(r#"{"b": 2, "x": 1}"#, None)]);
    a_record_write_takes_an_object: "Object.assign($, json(_raw)) r: {...} = $.a; $ = r;",
        [AB], Rows(&[(r#"{"b": 1}"#, None)]);

    // -- a failure is caught
    a_catch_runs_on_the_rows_the_body_marked: "Object.assign($, json(_raw)) log: str = $.log; m: str = null; try { app: {msg: str, ...} = json(log); m = app.msg; } catch { m = log; }",
        [r#"{"log": "plain text"}"#, r#"{"log": "{\"msg\": \"hi\"}"}"#],
        Rows(&[
            (r#"{"log": "plain text", "m": "plain text"}"#, None),
            (r#"{"log": "{\"msg\": \"hi\"}", "app": {"msg": "hi"}, "m": "hi"}"#, None),
        ]);
    a_catch_binds_the_failure_naming_its_path: "a: i64; try { Object.assign($, json(_raw)) } catch (e) { bad = test(e.message, / in a$/); }",
        [r#"{"a": "x"}"#], Rows(&[(r#"{"bad": true}"#, None)]);
    a_catch_binds_the_kind_and_the_message: r#"s: str; Object.assign($, json(_raw)) k: str = null; b: bool = false; try { n: i64 = parseInt(s); } catch (e) { k = e.kind; b = test(e.message, / in n$/); }"#,
        [r#"{"s": "x"}"#], Rows(&[(r#"{"s": "x", "k": "CoercionRefused", "b": true}"#, None)]);
    a_typed_catch_takes_a_kind_it_names: r#"s: str; Object.assign($, json(_raw)) m: str = null; try { n: i64 = parseInt(s); } catch (e: NaN | CoercionRefused) { m = "caught"; }"#,
        [r#"{"s": "x"}"#], Rows(&[(r#"{"s": "x", "m": "caught"}"#, None)]);
    a_typed_catch_passes_another_kind_to_the_policy: r#"s: str; Object.assign($, json(_raw)) m: str = null; try { n: i64 = parseInt(s); } catch (e: NaN) { m = "caught"; }"#,
        [r#"{"s": "x"}"#], Rows(&[(r#"{"s": "x"}"#, Some(r#"s is "x", which could not convert to i64 in n"#))]);
    a_typed_catch_passes_another_kind_to_the_outer_catch: r#"s: str; Object.assign($, json(_raw)) m: str = null; try { try { o: {v: f64, ...} = json(s); } catch (e: Overflow) { m = "inner"; } m = "after"; } catch (e) { m = e.kind; }"#,
        [r#"{"s": "{\"v\": NaN}"}"#], Rows(&[(r#"{"s": "{\"v\": NaN}", "m": "NaN"}"#, None)]);
    a_typed_catch_clears_only_the_kinds_it_names: r#"a: i64; k: str = null; try { Object.assign($, json(_raw)) } catch (e: NaN) { k = e.kind; }"#,
        [r#"{"a": "x", "v": NaN}"#], Rows(&[(r#"{"k": "NaN"}"#, Some(r#"_raw.a is "x", which could not parse as i64 in a"#))]);
    a_typed_catch_that_continues_gives_back_the_other_kinds: r#"a: i64; k: str = null; for (let x of [1]) { try { Object.assign($, json(_raw)) } catch (e: NaN) { k = e.kind; continue; } }"#,
        [r#"{"a": "x", "v": NaN}"#], Rows(&[(r#"{"k": "NaN"}"#, Some(r#"_raw.a is "x", which could not parse as i64 in a"#))]);
    a_typed_catch_that_returns_gives_back_the_other_kinds: r#"function f(s: str): str { try { o: {a: i64, ...} = json(s); } catch (e: NaN) { return e.kind; } return "none"; } Object.assign($, json(_raw)) s: str = $.s; k = f(s);"#,
        [r#"{"s": "{\"a\": \"x\", \"v\": NaN}"}"#], Rows(&[(r#"{"s": "{\"a\": \"x\", \"v\": NaN}", "k": "NaN"}"#, Some(r#"which could not parse as i64 in o.a"#))]);
    a_typed_catch_that_breaks_gives_back_the_other_kinds: r#"a: i64; k: str = null; for (let x of [1]) { try { Object.assign($, json(_raw)) } catch (e: NaN) { k = e.kind; break; } }"#,
        [r#"{"a": "x", "v": NaN}"#], Rows(&[(r#"{"k": "NaN"}"#, Some(r#"_raw.a is "x", which could not parse as i64 in a"#))]);
    a_catch_that_continues_leaves_the_next_catch_its_kinds: r#"a: i64; k: str = null; m: str = null; for (let x of [1]) { try { Object.assign($, json(_raw)) } catch (e: NaN) { k = e.kind; continue; } catch (f: UnparsableField) { m = f.kind; } }"#,
        [r#"{"a": "x", "v": NaN}"#], Rows(&[(r#"{"k": "NaN"}"#, Some(r#"_raw.a is "x", which could not parse as i64 in a"#))]);
    a_typed_catch_binds_the_first_kind_it_names: r#"k: str = null; b: bool = false; try { Object.assign($, json(_raw)) } catch (e: KeyContradiction) { k = e.kind; b = test(e.message, /^_raw\.a contradicts/); }"#,
        [r#"{"a": 1, "a": 2, "v": NaN}"#], Rows(&[(r#"{"a": 2, "k": "KeyContradiction", "b": true}"#, Some("_raw held a number no f64 holds"))]);
    a_caught_exception_is_not_written: r#"s: str; Object.assign($, json(_raw)) try { n: i64 = parseInt(s); } catch (e) { e.kind = "x"; }"#,
        [r#"{}"#], Error(&["`e.kind`"]);
    a_name_that_shadows_a_caught_exception_is_written: r#"s: str; Object.assign($, json(_raw)) m: i64 = 0; try { n: i64 = parseInt(s); } catch (e) { m = map([1], (e) => { e = e + 1; e })[0]; }"#,
        [r#"{"s": "x"}"#], Rows(&[(r#"{"s": "x", "m": 2}"#, None)]);
    a_local_that_shadows_a_caught_exception_is_written: r#"s: str; Object.assign($, json(_raw)) m: i64 = 0; try { n: i64 = parseInt(s); } catch (e) { let e = 0; e = 1; m = e; }"#,
        [r#"{"s": "x"}"#], Rows(&[(r#"{"s": "x", "m": 1}"#, None)]);
    a_throw_with_no_outer_catch_leaves_the_exception_on_the_row: r#"s: str; Object.assign($, json(_raw)) m: str = null; try { n: i64 = parseInt(s); } catch (e) { m = "seen"; throw e; }"#,
        [r#"{"s": "x"}"#, r#"{"s": "7"}"#],
        Rows(&[(r#"{"s": "x", "m": "seen"}"#, Some(r#"s is "x", which could not convert to i64 in n"#)), (r#"{"s": "7", "n": 7}"#, None)]);
    a_throw_reaches_the_outer_catch: r#"s: str; Object.assign($, json(_raw)) m: str = null; try { try { n: i64 = parseInt(s); } catch (e) { throw e; } m = "after"; } catch (f) { m = f.kind; }"#,
        [r#"{"s": "x"}"#], Rows(&[(r#"{"s": "x", "m": "CoercionRefused"}"#, None)]);
    a_throw_takes_only_a_caught_name: r#"s: str; Object.assign($, json(_raw)) try { n: i64 = parseInt(s); } catch (e) { throw s; }"#,
        [r#"{}"#], Error(&["`throw s`", "`catch`"]);
    a_typed_catch_names_a_kind: r#"s: str; Object.assign($, json(_raw)) try { n: i64 = parseInt(s); } catch (e: Nope) { }"#,
        [r#"{}"#], Error(&["`Nope`", "`NaN`"]);
    a_typed_catch_does_not_name_an_encoding_kind: r#"s: str; Object.assign($, json(_raw)) try { n: i64 = parseInt(s); } catch (e: NaN | InvalidUtf8) { }"#,
        [r#"{}"#], Error(&["`InvalidUtf8`", "encoding"]);
    a_throw_inside_a_loop_in_its_catch_is_refused: r#"Object.assign($, json(_raw)) lines: array<str> = $.lines; try { $.v = parseInt(lines[0]); } catch (e) { for (let s of lines) { throw e; } }"#,
        [r#"{}"#], Error(&["`throw e`", "loop"]);
    two_typed_catches_each_take_their_own_kind: r#"a: i64; m: str = null; try { Object.assign($, json(_raw)) } catch (e: NaN) { m = e.kind; } catch (f: UnparsableField) { m = f.kind; }"#,
        [r#"{"v": NaN}"#, r#"{"a": "x"}"#],
        Rows(&[(r#"{"m": "NaN"}"#, None), (r#"{"m": "UnparsableField"}"#, None)]);
    a_last_untyped_catch_takes_the_rest: r#"a: i64; m: str = null; try { Object.assign($, json(_raw)) } catch (e: NaN) { m = "nan"; } catch (f) { m = f.kind; }"#,
        [r#"{"v": NaN}"#, r#"{"a": "x"}"#],
        Rows(&[(r#"{"m": "nan"}"#, None), (r#"{"m": "UnparsableField"}"#, None)]);
    each_catch_in_turn_takes_the_kinds_it_names_on_one_row: r#"a: i64; m: str = null; try { Object.assign($, json(_raw)) } catch (e: NaN) { m = e.kind; } catch (f: UnparsableField) { m = `${m} ${f.kind}`; }"#,
        [r#"{"a": "x", "v": NaN}"#], Rows(&[(r#"{"m": "NaN UnparsableField"}"#, None)]);
    a_kind_named_by_two_catches_is_refused: r#"s: str; Object.assign($, json(_raw)) try { n: i64 = parseInt(s); } catch (e: NaN) { } catch (f: Overflow | NaN) { }"#,
        [r#"{}"#], Error(&["`NaN`", "two `catch`es"]);
    a_catch_after_an_untyped_catch_is_refused: r#"s: str; Object.assign($, json(_raw)) try { n: i64 = parseInt(s); } catch (e) { } catch (f: NaN) { }"#,
        [r#"{}"#], Error(&["names no kind", "no `catch` follows it"]);

    a_try_stops_at_its_first_failure: r#"s: str; Object.assign($, json(_raw)) n: i64 = null; m: str = null; k: i64 = null; try { n = parseInt(s); m = `${s}!`; } catch { m = "bad"; } k = parseInt(s);"#,
        [r#"{"s": "x"}"#], Rows(&[(r#"{"s": "x", "m": "bad"}"#, Some(r#"s is "x", which could not convert to i64 in k"#))]);
    a_try_without_a_failure_runs_its_body: r#"s: str; Object.assign($, json(_raw)) n: i64 = null; m: str = null; k: i64 = null; try { n = parseInt(s); m = `${s}!`; } catch { m = "bad"; } k = parseInt(s);"#,
        [r#"{"s": "7"}"#], Rows(&[(r#"{"s": "7", "n": 7, "m": "7!", "k": 7}"#, None)]);
    a_try_stops_inside_a_nested_if: r#"s: str; t: bool; Object.assign($, json(_raw)) n: i64 = null; m: str = null; try { if (t) { n = parseInt(s); m = "after"; } } catch { m = "bad"; }"#,
        [r#"{"s": "x", "t": true}"#, r#"{"s": "7", "t": true}"#],
        Rows(&[(r#"{"s": "x", "t": true, "m": "bad"}"#, None), (r#"{"s": "7", "t": true, "n": 7, "m": "after"}"#, None)]);
    a_failure_in_a_catch_stops_the_outer_try: r#"s: str; Object.assign($, json(_raw)) n: i64 = null; m: str = null; try { try { n = parseInt(s); } catch { n = parseInt(s); m = "inner"; } } catch { m = "outer"; }"#,
        [r#"{"s": "x"}"#], Rows(&[(r#"{"s": "x", "m": "outer"}"#, None)]);
    a_loop_inside_a_try_stops_at_the_rows_first_failure: r#"Object.assign($, json(_raw)) lines: array<str> = $.lines; n: i64 = 0; try { for (let s of lines) { n = n + 1; $.v = parseInt(s); } } catch { $.caught = n; }"#,
        [r#"{"lines": ["1", "x", "3"]}"#, r#"{"lines": ["1", "3"]}"#],
        Rows(&[
            (r#"{"lines": ["1", "x", "3"], "n": 2, "v": 1, "caught": 2}"#, None),
            (r#"{"lines": ["1", "3"], "n": 2, "v": 3}"#, None),
        ]);
    a_fold_inside_a_try_stops_at_the_rows_first_failure: r#"Object.assign($, json(_raw)) lines: array<str> = $.lines; n: i64 = 0; m: i64 = null; try { for (let s of lines) { n = n + (parseInt(s) ?? 10); } } catch { m = n; }"#,
        [r#"{"lines": ["1", "x", "3"]}"#], Rows(&[(r#"{"lines": ["1", "x", "3"], "n": 1, "m": 1}"#, None)]);
    a_break_and_a_failure_both_end_a_loop_inside_a_try: r#"Object.assign($, json(_raw)) lines: array<str> = $.lines; n: i64 = 0; m: bool = false; try { for (let s of lines) { if (s == "stop") break; n = n + (parseInt(s) ?? 10); } } catch { m = true; }"#,
        [r#"{"lines": ["1", "stop", "x"]}"#, r#"{"lines": ["x", "2"]}"#],
        Rows(&[
            (r#"{"lines": ["1", "stop", "x"], "n": 1, "m": false}"#, None),
            (r#"{"lines": ["x", "2"], "n": 0, "m": true}"#, None),
        ]);
    a_while_inside_a_try_stops_at_the_rows_first_failure: r#"s: str; Object.assign($, json(_raw)) i: i64 = 0; m: i64 = null; try { while (i < 5) { i = i + 1; if (i == 2) { k: i64 = parseInt(s); } } } catch { m = i; }"#,
        [r#"{"s": "x"}"#, r#"{"s": "7"}"#],
        Rows(&[(r#"{"s": "x", "i": 2, "m": 2}"#, None), (r#"{"s": "7", "i": 5, "k": 7}"#, None)]);
    a_literal_loop_inside_a_try_stops_at_the_rows_first_failure: r#"Object.assign($, json(_raw)) n: i64 = 0; m: i64 = null; try { for (let s of ["x", "2"]) { n = n + 1; $.v = parseInt(s); } } catch { m = n; }"#,
        [r#"{}"#],
        Rows(&[(r#"{"n": 1, "m": 1}"#, None)]);
    a_while_conditions_failure_ends_the_loop_inside_a_try: r#"s: str; Object.assign($, json(_raw)) i: i64 = 0; m: i64 = null; try { while ((parseInt(s) ?? 2) > i) { i = i + 1; } } catch { m = i; }"#,
        [r#"{"s": "x"}"#, r#"{"s": "1"}"#],
        Rows(&[(r#"{"s": "x", "i": 0, "m": 0}"#, None), (r#"{"s": "1", "i": 1}"#, None)]);
    a_break_is_no_failure_and_reaches_no_catch: r#"Object.assign($, json(_raw)) lines: array<str> = $.lines; n: i64 = 0; c: i64 = 0; for (let s of lines) { try { if (s == "stop") { break; } n = n + 1; } catch { c = c + 1; } }"#,
        [r#"{"lines": ["1", "stop", "3"]}"#],
        Rows(&[(r#"{"lines": ["1", "stop", "3"], "n": 1, "c": 0}"#, None)]);
    a_failure_in_a_try_inside_a_loop_leaves_the_try_and_the_loop_runs_on: r#"Object.assign($, json(_raw)) lines: array<str> = $.lines; c: i64 = 0; for (let s of lines) { try { $.v = parseInt(s); } catch { c = c + 1; } }"#,
        [r#"{"lines": ["1", "x", "3"]}"#],
        Rows(&[(r#"{"lines": ["1", "x", "3"], "v": 3, "c": 1}"#, None)]);
    a_continue_leaves_the_trip_and_a_later_failure_the_try: r#"Object.assign($, json(_raw)) lines: array<str> = $.lines; n: i64 = 0; try { for (let s of lines) { if (s == "skip") { continue; } n = n + 1; $.v = parseInt(s); } } catch { $.m = n; }"#,
        [r#"{"lines": ["1", "skip", "x", "4"]}"#],
        Rows(&[(r#"{"lines": ["1", "skip", "x", "4"], "n": 2, "v": 1, "m": 2}"#, None)]);
    a_finally_runs_on_a_row_that_broke_out: r#"Object.assign($, json(_raw)) lines: array<str> = $.lines; f: i64 = 0; for (let s of lines) { try { if (s == "b") { break; } } finally { f = f + 1; } }"#,
        [r#"{"lines": ["a", "b", "c"]}"#],
        Rows(&[(r#"{"lines": ["a", "b", "c"], "f": 2}"#, None)]);
    a_while_condition_may_filter: r#"Object.assign($, json(_raw)) i: i64 = 0; while (length([1, 2].filter(x => x > i)) > 0) { i = i + 1; }"#,
        [r#"{}"#],
        Rows(&[(r#"{"i": 2}"#, None)]);
    a_continue_still_tests_a_while_condition_that_calls_a_function: r#"function below(a: i64, b: i64): bool { return a < b; } Object.assign($, json(_raw)) i: i64 = 0; n: i64 = 0; while (below(i, 3)) { i = i + 1; if (i == 3) { continue; } n = n + 1; }"#,
        [r#"{}"#],
        Rows(&[(r#"{"i": 3, "n": 2}"#, None)]);
    a_failure_stops_a_finally_where_it_happens: r#"s: str; Object.assign($, json(_raw)) b: i64 = 0; try { b = 0; } finally { $.v = parseInt(s); b = 1; }"#,
        [r#"{"s": "x"}"#, r#"{"s": "7"}"#],
        Rows(&[(r#"{"s": "x", "b": 0}"#, Some(r#"s is "x", which could not convert to i64 in v"#)), (r#"{"s": "7", "b": 1, "v": 7}"#, None)]);
    a_while_conditions_failure_is_its_own_rows: r#"s: str; f: bool; Object.assign($, json(_raw)) i: i64 = 0; m: bool = false; if (f) { try { while (parseInt(s) > i) { i = i + 1; } } catch { m = true; } }"#,
        [r#"{"s": "2", "f": false}"#, r#"{"s": "x", "f": true}"#],
        Rows(&[(r#"{"s": "2", "f": false, "i": 0, "m": false}"#, None), (r#"{"s": "x", "f": true, "i": 0, "m": true}"#, None)]);
    a_parse_in_a_loop_writes_a_let_local: r#"Object.assign($, json(_raw)) lines: array<str> = $.lines; c: i64 = 0; for (let s of lines) { let o: {a: i64, ...} = json(s); c = c + (o.a ?? 0); }"#,
        [r#"{"lines": ["{\"a\":1}", "{\"b\":5}", "{\"a\":3}"]}"#],
        Rows(&[(r#"{"lines": ["{\"a\":1}", "{\"b\":5}", "{\"a\":3}"], "c": 4}"#, None)]);
    a_lookup_writes_a_let_local: r#"Object.assign($, json(_raw)) s: str = $.s; let r = lookup("codes", s, {column: "code", fields: {text: "message"}}); t = r.text;"#,
        [r#"{"s": "200"}"#], Rows(&[(r#"{"s": "200", "t": "ok"}"#, None)]);
    a_parse_writes_a_let_local_outside_a_loop: r#"Object.assign($, json(_raw)) s: str = $.s; let o: {a: i64, ...} = json(s); n = o.a;"#,
        [r#"{"s": "{\"a\":7,\"b\":1}"}"#], Rows(&[(r#"{"s": "{\"a\":7,\"b\":1}", "n": 7}"#, None)]);
    a_parse_in_a_loop_lays_each_trips_keys_over_the_last: r#"Object.assign($, json(_raw)) lines: array<str> = $.lines; for (let s of lines) { Object.assign($, json(s)) }"#,
        [r#"{"lines": ["{\"a\":1,\"b\":1}", "{\"a\":2}"]}"#],
        Rows(&[(r#"{"lines": ["{\"a\":1,\"b\":1}", "{\"a\":2}"], "a": 2, "b": 1}"#, None)]);
    a_parse_in_a_loop_into_a_local_is_fresh_each_trip: r#"Object.assign($, json(_raw)) lines: array<str> = $.lines; c: i64 = 0; for (let s of lines) { let o: {a: i64, ...} = {}; Object.assign(o, json(s)); c = c + (o.a ?? 0); }"#,
        [r#"{"lines": ["{\"a\":1}", "{\"b\":5}", "{\"a\":3}"]}"#],
        Rows(&[(r#"{"lines": ["{\"a\":1}", "{\"b\":5}", "{\"a\":3}"], "c": 4}"#, None)]);
    a_parse_failure_in_a_loop_marks_and_the_loop_runs_on: r#"Object.assign($, json(_raw)) lines: array<str> = $.lines; c: i64 = 0; for (let s of lines) { let o: {a: i64, ...} = {}; Object.assign(o, json(s)); c = c + (o.a ?? 0); }"#,
        [r#"{"lines": ["{\"a\":1}", "nope", "{\"a\":3}"]}"#],
        Rows(&[(r#"{"lines": ["{\"a\":1}", "nope", "{\"a\":3}"], "c": 4}"#, Some(r#"s is "nope", which json could not parse"#))]);
    a_lookup_in_a_loop_runs_on_each_trip: r#"Object.assign($, json(_raw)) codes: array<str> = $.codes; t: str = ""; for (let c of codes) { let r = {}; Object.assign(r, lookup("codes", c, {column: "code", fields: {text: "message"}})); t = t + (r.text ?? "-"); }"#,
        [r#"{"codes": ["200", "999", "200"]}"#], Rows(&[(r#"{"codes": ["200", "999", "200"], "t": "ok-ok"}"#, None)]);
    finally_runs_on_every_row: r#"s: str; Object.assign($, json(_raw)) n: i64 = null; m: str = null; d: bool = false; try { n = parseInt(s); m = "after"; } catch { m = "bad"; } finally { d = true; }"#,
        [r#"{"s": "x"}"#, r#"{"s": "7"}"#],
        Rows(&[(r#"{"s": "x", "m": "bad", "d": true}"#, None), (r#"{"s": "7", "n": 7, "m": "after", "d": true}"#, None)]);
    a_try_without_a_catch_keeps_its_failure: r#"s: str; Object.assign($, json(_raw)) n: i64 = null; m: str = null; d: bool = false; try { n = parseInt(s); m = "after"; } finally { d = true; }"#,
        [r#"{"s": "x"}"#], Rows(&[(r#"{"s": "x", "d": true}"#, Some(r#"s is "x", which could not convert to i64 in n"#))]);
    an_uncaught_failure_reaches_the_outer_catch_after_finally: r#"s: str; Object.assign($, json(_raw)) n: i64 = null; d: bool = false; m: str = null; try { try { n = parseInt(s); } finally { d = true; } m = "after"; } catch { m = "outer"; }"#,
        [r#"{"s": "x"}"#], Rows(&[(r#"{"s": "x", "d": true, "m": "outer"}"#, None)]);
    a_parse_inside_an_expression_names_the_write: "s: str; Object.assign($, json(_raw)) x = json(s).a;",
        [r#"{}"#], Error(&["`x = json(…);`"]);
    a_spread_of_a_call_names_the_value_first: "Object.assign($, json(_raw)) x = {...rest($), b: 1};",
        [r#"{"a": 1}"#], Error(&["`let v = …; {...v}`"]);
    a_spread_takes_a_named_rest: "Object.assign($, json(_raw)) let r = rest($); x = {...r, b: 1}; $ = {x: x};",
        [r#"{"a": 1}"#], Rows(&[(r#"{"x": {"a": 1, "b": 1}}"#, None)]);
    a_spread_is_no_statement: "{...json(_raw)}",
        [r#"{}"#], Error(&["write `Object.assign($, e)`"]);
    object_assign_merges_a_parse_into_the_record: r#"h = 1; status: i64; Object.assign($, json(_raw));"#,
        [r#"{"status": "200", "a": 1}"#], Rows(&[(r#"{"h": 1, "status": 200, "a": 1}"#, None)]);
    a_delete_inside_a_try_names_the_try: "a: i64; s: str; Object.assign($, json(_raw)) try { n: i64 = parseInt(s); delete(a); } catch { }",
        [r#"{"a": 1}"#], Error(&["`try`", "`a = null`"]);
    a_try_needs_a_catch_or_a_finally: "Object.assign($, json(_raw)) try { a = 1; }",
        [r#"{}"#], Error(&["`catch` or `finally`"]);

    // -- delete takes many names, a literal takes a computed key, switch
    delete_takes_many_names: "Object.assign($, json(_raw)) delete(a, b);",
        [r#"{"a": 1, "b": 2, "c": 3}"#], Rows(&[(r#"{"c": 3}"#, None)]);
    a_guarded_delete_runs_only_on_its_rows: "f: bool; Object.assign($, json(_raw)) if (f) delete(a);",
        [r#"{"f": true, "a": 1}"#, r#"{"f": false, "a": 1}"#], Rows(&[(r#"{"f": true}"#, None), (r#"{"f": false, "a": 1}"#, None)]);
    a_literal_takes_a_computed_key: r#"Object.assign($, json(_raw)) let p: str = $.p; t: str = {"6": "tcp", "17": "udp"}[p] ?? "other";"#,
        [r#"{"p": 6}"#, r#"{"p": 47}"#], Rows(&[(r#"{"p": 6, "t": "tcp"}"#, None), (r#"{"p": 47, "t": "other"}"#, None)]);
    rest_is_the_unclaimed_fields: "a: i64; Object.assign($, kv(_raw)) r = rest($);",
        ["a=1 b=2 c=3"], Rows(&[(r#"{"a": 1, "b": "2", "c": "3", "r": {"b": "2", "c": "3"}}"#, None)]);
    a_closed_object_has_no_rest: "o = {a: 1}; r = rest(o);",
        ["{}"], Error(&["`o`"]);
    switch_is_an_if_chain: r#"Object.assign($, json(_raw)) c: str = $.c; switch (c) { case "a": x = 1; break; case "b": x = 2; break; default: x = 0; }"#,
        [r#"{"c": "a"}"#, r#"{"c": "z"}"#], Rows(&[(r#"{"c": "a", "x": 1}"#, None), (r#"{"c": "z", "x": 0}"#, None)]);

    // -- the statements and parse functions, where the cases above leave a form out
    a_guarded_delete_of_a_bound_name_is_refused: r#"Object.assign($, json(_raw)) f: bool = $.f; x: i64 = 1; if (f) delete(x);"#,
        [r#"{"f": true}"#], Error(&["`x = null`"]);
    move_renames_a_field: "Object.assign($, json(_raw)) move(a, c);",
        [r#"{"a": 1, "b": 2}"#], Rows(&[(r#"{"b": 2, "c": 1}"#, None)]);
    a_guarded_move_runs_only_on_its_rows: "Object.assign($, json(_raw)) f: bool = $.f; if (f) move(a, c);",
        [r#"{"f": true, "a": 1}"#, r#"{"f": false, "a": 2}"#],
        Rows(&[(r#"{"f": true, "c": 1}"#, None), (r#"{"f": false, "a": 2}"#, None)]);
    a_guarded_move_clears_a_declared_field: "a: str; c: str; f: bool; Object.assign($, json(_raw)) if (f) move(a, c);",
        [r#"{"f": true, "a": "x"}"#, r#"{"f": false, "a": "y"}"#],
        Rows(&[(r#"{"f": true, "c": "x"}"#, None), (r#"{"f": false, "a": "y"}"#, None)]);
    a_guarded_delete_of_a_declared_field_is_refused: "a: str; f: bool; Object.assign($, json(_raw)) if (f) delete(a);",
        [r#"{"f": true, "a": "x"}"#], Error(&["`a = null`"]);
    a_switch_computes_its_subject_once: r#"n: i64; Object.assign($, json(_raw)) x: str = null; switch (n + 1) { case 2: x = "two"; break; case 3: x = "three"; break; default: x = "other"; }"#,
        [r#"{"n": 1}"#, r#"{"n": 2}"#, r#"{"n": 5}"#],
        Rows(&[(r#"{"n": 1, "x": "two"}"#, None), (r#"{"n": 2, "x": "three"}"#, None), (r#"{"n": 5, "x": "other"}"#, None)]);
    switch_falls_through: r#"Object.assign($, json(_raw)) c: str = $.c; x: i64 = 0; y: i64 = 0; switch (c) { case "a": x = 1; case "b": y = 2; break; default: x = 9; }"#,
        [r#"{"c": "a"}"#, r#"{"c": "b"}"#, r#"{"c": "z"}"#],
        Rows(&[
            (r#"{"c": "a", "x": 1, "y": 2}"#, None),
            (r#"{"c": "b", "x": 0, "y": 2}"#, None),
            (r#"{"c": "z", "x": 9, "y": 0}"#, None),
        ]);
    an_else_takes_one_statement_without_braces: r#"Object.assign($, json(_raw)) n: i64 = $.n; if (n > 1) x = "big"; else x = "small";"#,
        [r#"{"n": 2}"#, r#"{"n": 0}"#], Rows(&[(r#"{"n": 2, "x": "big"}"#, None), (r#"{"n": 0, "x": "small"}"#, None)]);
    kv_takes_its_separators: r#"Object.assign($, kv(_raw, {pairSep: ";", kvSep: ":"}))"#,
        ["a:x;b:y"], Rows(&[(r#"{"a": "x", "b": "y"}"#, None)]);
    a_built_object_takes_a_key_only_the_row_knows: r#"let k = "b"; x = {a: 1}; x[k] = "v";"#,
        ["{}"], Rows(&[(r#"{"x": {"a": 1, "b": "v"}}"#, None)]);
    a_parse_lays_keys_into_a_built_object: "x = {a: 1}; Object.assign($, json(_raw)); y = x.b;",
        [r#"{"x": {"b": 2}}"#], Rows(&[(r#"{"x": {"a": 1, "b": 2}, "y": 2}"#, None)]);
    object_assign_parses_into_a_local: r#"let v = {}; Object.assign(v, grok(_raw, ["%{INT:status}"])); s = v.status;"#,
        ["200"], Rows(&[(r#"{"s": "200"}"#, None)]);
    object_assign_merges_each_source_in_turn: r#"x = {a: 1}; Object.assign(x, json(_raw), {c: 3}); y: i64 = x.b;"#,
        [r#"{"b": "2"}"#], Rows(&[(r#"{"x": {"a": 1, "b": "2", "c": 3}, "y": 2}"#, None)]);
    object_assign_keeps_a_closed_object_closed: r#"x: {a: i64} = {a: 1}; Object.assign(x, {b: 2});"#,
        ["{}"], Rows(&[(r#"{"x": {"a": 1}}"#, None)]);
    object_assign_converts_a_parse_to_the_target_s_fields: r#"let v = {a: 1}; Object.assign(v, grok(_raw, ["%{INT:a}"])); t = v.a;"#,
        ["200"], Rows(&[(r#"{"t": 200}"#, None)]);
    a_failed_parse_merges_nothing: r#"Object.assign($, json(_raw)); let v = {}; Object.assign(v, grok($.s, ["%{INT:status}"]));"#,
        [r#"{"s": "1"}"#], Error(&["`s`"]);
    object_assign_has_no_value: r#"o = {a: 1}; $ = Object.assign(o, {b: 2});"#,
        ["{}"], Error(&["`Object.assign` is a statement"]);
    csv_takes_its_delimiter: r#"Object.assign($, csv(_raw, ["a", "b"], {delimiter: "|"}))"#,
        ["1|x"], Rows(&[(r#"{"a": "1", "b": "x"}"#, None)]);
    a_null_header_skips_its_column: r#"Object.assign($, csv(_raw, [null, "b", null]))"#,
        ["1,x,2"], Rows(&[(r#"{"b": "x"}"#, None)]);
    grok_takes_named_patterns: r#"n: i64; Object.assign($, grok(_raw, ["%{NUM:n}"], {patterns: {NUM: "[0-9]+"}}))"#,
        ["42"], Rows(&[(r#"{"n": 42}"#, None)]);
    a_type_declared_under_an_if_converts_every_row:
        r#"kind: str; Object.assign($, json(_raw)); if (kind == "a") { status: i64 = status; }"#,
        [r#"{"kind": "a", "status": "5"}"#, r#"{"kind": "b", "status": "x"}"#],
        Rows(&[(r#"{"kind": "a", "status": 5}"#, None), (r#"{"kind": "b"}"#, Some(r#""x", which could not convert to i64 in status"#))]);
    a_parse_assigned_into_a_name_cites_the_name: r#"Object.assign($, json(_raw)) s: str = $.s; o: {n: i64, ...} = {n: 1}; Object.assign(o, json(s));"#,
        [r#"{"s": "{\"n\": \"x\"}"}"#], Rows(&[(r#"{"o": {"n": 1}, "s": "{\"n\": \"x\"}"}"#, Some(r#"s.n is "x", which could not parse as i64 in o.n"#))]);
    a_variant_assigned_into_a_name_cites_the_name: r#"Object.assign($, json(_raw)) o: {n: i64, ...} = {n: 1}; Object.assign(o, v);"#,
        [r#"{"v": {"n": "x"}}"#], Rows(&[(r#"{"o": {"n": 1}, "v": {"n": "x"}}"#, Some(r#""x", which could not convert to i64 in o.n"#))]);
    object_assign_lays_over_a_variant_its_target_holds: r#"Object.assign($, json(_raw)) o = $.a; Object.assign(o, b);"#,
        [r#"{"a":{"x":1},"b":{"y":2}}"#],
        Rows(&[(r#"{"a":{"x":1},"b":{"y":2},"o":{"x":1,"y":2}}"#, None)]);
    object_assign_into_a_value_is_refused: r#"Object.assign($, json(_raw)) t: str = $.t; o = "s"; Object.assign(o, json(t));"#,
        [r#"{"t":"{}"}"#], Error(&["spread"]);
    a_parse_that_misses_keeps_what_a_local_holds: r#"Object.assign($, json(_raw)) a: str = $.a; b: str = $.b; let v = {}; Object.assign(v, json(a)); Object.assign(v, grok(b, ["%{INT:z}"])); s = v.z;"#,
        [r#"{"a":"{\"z\":\"5\"}","b":"nope"}"#],
        Rows(&[(r#"{"a":"{\"z\":\"5\"}","b":"nope","s":"5"}"#, Some(r#"b is "nope", which grok could not parse"#))]);
    a_guarded_lookup_into_a_local_writes_the_rows_it_takes: r#"Object.assign($, json(_raw)) f: bool = $.f; s: str = $.s; let v = {}; if (f) { Object.assign(v, lookup("codes", s, {column: "code", fields: {text: "message"}})) } t = v.text;"#,
        [r#"{"f": false, "s": "200", "text": "rec"}"#, r#"{"f": true, "s": "999", "text": "rec"}"#, r#"{"f": true, "s": "200", "text": "rec"}"#],
        Rows(&[
            (r#"{"f": false, "s": "200", "text": "rec"}"#, None),
            (r#"{"f": true, "s": "999", "text": "rec"}"#, None),
            (r#"{"f": true, "s": "200", "text": "rec", "t": "ok"}"#, None),
        ]);
    a_parse_written_to_the_record_replaces_it: r#"Object.assign($, json(_raw)) s: str = $.s; n = 1; $ = json(s);"#,
        [r#"{"s": "{\"a\": 1}", "b": 2}"#, r#"{"s": "{\"b\": 3}", "c": 4}"#],
        Rows(&[(r#"{"a": 1}"#, None), (r#"{"b": 3}"#, None)]);
    scalars_that_do_not_meet_blend_to_a_variant: r#"Object.assign($, json(_raw)) f: bool = $.f; s: str = $.s; if (f) { x = 1 } else { x = s }"#,
        [r#"{"f": true, "s": "a"}"#, r#"{"f": false, "s": "a"}"#],
        Rows(&[(r#"{"f": true, "s": "a", "x": 1}"#, None), (r#"{"f": false, "s": "a", "x": "a"}"#, None)]);
    an_object_and_a_scalar_blend_to_a_variant: r#"Object.assign($, json(_raw)) f: bool = $.f; if (f) { o = {a: 1} } else { o = 2 }"#,
        [r#"{"f": true}"#, r#"{"f": false}"#],
        Rows(&[(r#"{"f": true, "o": {"a": 1}}"#, None), (r#"{"f": false, "o": 2}"#, None)]);
    a_guarded_write_over_a_value_of_another_type_blends_to_a_variant: r#"Object.assign($, json(_raw)) f: bool = $.f; x = "s"; if (f) { o = 1 } else { o = x }"#,
        [r#"{"f": true}"#, r#"{"f": false}"#],
        Rows(&[(r#"{"f": true, "x": "s", "o": 1}"#, None), (r#"{"f": false, "x": "s", "o": "s"}"#, None)]);
    a_guarded_parse_into_a_name_keeps_a_skipped_rows_key: r#"Object.assign($, json(_raw)) f: bool = $.f; s: str = $.s; x: {...} = $.x; if (f) { Object.assign(x, grok(s, ["%{INT:n}"])) }"#,
        [r#"{"f": false, "s": "5", "x": {"n": "4"}}"#, r#"{"f": true, "s": "5", "x": {"n": "4"}}"#],
        Rows(&[(r#"{"f": false, "s": "5", "x": {"n": "4"}}"#, None), (r#"{"f": true, "s": "5", "x": {"n": "5"}}"#, None)]);
    object_assign_lays_a_variant_over_what_its_target_holds: r#"Object.assign($, json(_raw)) f: bool = $.f; o = {a: {b: 1}, n: 1}; if (f) { Object.assign(o, v) }"#,
        [r#"{"f": true, "v": {"a": {"c": 2}, "n": 7, "z": 3}}"#, r#"{"f": false, "v": {"n": 7}}"#],
        Rows(&[
            (r#"{"f": true, "v": {"a": {"c": 2}, "n": 7, "z": 3}, "o": {"a": {"b": 1, "c": 2}, "n": 7, "z": 3}}"#, None),
            (r#"{"f": false, "v": {"n": 7}, "o": {"a": {"b": 1}, "n": 1}}"#, None),
        ]);
    object_assign_into_a_name_nothing_binds_is_refused: r#"Object.assign($, json(_raw)) s: str = $.s; Object.assign(q, json(s));"#,
        [r#"{"s": "{}"}"#], Error(&["`q`"]);
    object_assign_under_an_if_writes_the_rows_it_takes:
        r#"kind: str; Object.assign($, json(_raw)); if (kind == "a") { Object.assign($, {tag: "x"}); }"#,
        [r#"{"kind": "a"}"#, r#"{"kind": "b"}"#],
        Rows(&[(r#"{"kind": "a", "tag": "x"}"#, None), (r#"{"kind": "b"}"#, None)]);
    object_assign_under_an_if_takes_an_object_of_known_keys:
        r#"kind: str; Object.assign($, json(_raw)); v = {n: 1}; if (kind == "a") { Object.assign($, v); }"#,
        [r#"{"kind": "a"}"#, r#"{"kind": "b"}"#],
        Rows(&[(r#"{"kind": "a", "v": {"n": 1}, "n": 1}"#, None), (r#"{"kind": "b", "v": {"n": 1}}"#, None)]);
    object_assign_under_an_if_refuses_unknown_keys:
        r#"kind: str; Object.assign($, json(_raw)); v = json(kind); if (kind == "a") { Object.assign($, v); }"#,
        [r#"{"kind": "a"}"#], Error(&["`v` may hold keys nothing names"]);
    a_null_over_an_object_keeps_its_fields_types:
        r#"o: {a: str, b: {c: str}, ...} = json(_raw); o.b = null; p = o; c: str = o.b.c ?? "none";"#,
        [r#"{"a": "x", "b": {"c": "y"}}"#],
        Rows(&[(r#"{"o": {"a": "x"}, "p": {"a": "x"}, "c": "none"}"#, None)]);
    formats_reads_a_datetime_in_its_format:
        r#"ts: datetime; Object.assign($, grok(_raw, ["%{GREEDYDATA:ts}"], {formats: {ts: "%d/%m/%Y %H:%M"}}))"#,
        ["15/03/2024 07:30"], Rows(&[(r#"{"ts": "2024-03-15T07:30:00.000000Z"}"#, None)]);
    a_declared_object_s_datetime_takes_a_format:
        r#"x: {ts: datetime, ...} = json(_raw, {formats: {ts: "%d/%m/%Y %H:%M"}});"#,
        [r#"{"ts": "15/03/2024 07:30", "a": 1}"#],
        Rows(&[(r#"{"x": {"ts": "2024-03-15T07:30:00.000000Z", "a": 1}}"#, None)]);
    only_a_datetime_takes_a_format: r#"n: i64; Object.assign($, grok(_raw, ["%{INT:n}"], {formats: {n: "%Y"}}))"#,
        ["5"], Error(&["`n` is", "only a `datetime`"]);
    a_format_names_a_field_the_call_writes: r#"Object.assign($, grok(_raw, ["%{INT:n}"], {formats: {m: "%Y"}}))"#,
        ["5"], Error(&["`formats` names `m`"]);
    a_parse_writes_an_object: r#"x = grok(_raw, ["%{INT:n} %{WORD:w}"]);"#,
        ["5 a"], Rows(&[(r#"{"x": {"n": "5", "w": "a"}}"#, None)]);
    a_declared_parse_types_its_object: r#"x: {n: i64, ...} = json(_raw);"#,
        [r#"{"n": "5", "m": 1}"#], Rows(&[(r#"{"x": {"n": 5, "m": 1}}"#, None)]);
    a_parse_into_a_bound_name_needs_a_declaration: "x = json(_raw); x = json(_raw);",
        ["{}"], Error(&["`x: {...} = …`"]);
    an_unknown_option_is_refused: r#"Object.assign($, kv(_raw, {sep: ";"}))"#,
        ["a=1"], Error(&["`pairSep`", "`kvSep`", "`quote`"]);
    a_dynamic_parse_inside_an_if_parses_the_rows_it_selects: r#"Object.assign($, json(_raw)) f: bool = $.f; s: str = $.s; if (f) { Object.assign($, json(s)) }"#,
        [r#"{"f": true, "s": "{\"a\": 1, \"k\": \"x\"}", "k": "old"}"#, r#"{"f": false, "s": "{\"a\": 2}", "k": "old"}"#],
        Rows(&[
            (r#"{"f": true, "s": "{\"a\": 1, \"k\": \"x\"}", "k": "x", "a": 1}"#, None),
            (r#"{"f": false, "s": "{\"a\": 2}", "k": "old"}"#, None),
        ]);
    a_kv_parse_inside_an_if_parses_the_rows_it_selects: r#"Object.assign($, json(_raw)) f: bool = $.f; s: str = $.s; if (f) { Object.assign($, kv(s)) }"#,
        [r#"{"f": true, "s": "a=1 k=x", "k": "old"}"#, r#"{"f": false, "s": "a=2", "k": "old"}"#],
        Rows(&[
            (r#"{"f": true, "s": "a=1 k=x", "k": "x", "a": "1"}"#, None),
            (r#"{"f": false, "s": "a=2", "k": "old"}"#, None),
        ]);
    a_variant_spread_inside_an_if_spreads_the_rows_it_selects: r#"Object.assign($, json(_raw)) f: bool = $.f; if (f) { Object.assign($, v) }"#,
        [r#"{"f": true, "v": {"a": 1}}"#, r#"{"f": false, "v": {"a": 2}}"#],
        Rows(&[(r#"{"f": true, "v": {"a": 1}, "a": 1}"#, None), (r#"{"f": false, "v": {"a": 2}}"#, None)]);
    a_parse_into_a_name_inside_an_if_writes_the_rows_it_selects: r#"Object.assign($, json(_raw)) f: bool = $.f; s: str = $.s; if (f) { o = json(s) }"#,
        [r#"{"f": true, "s": "{\"a\": 1}", "o": {"z": 0}}"#, r#"{"f": false, "s": "{\"a\": 2}", "o": {"z": 0}}"#],
        Rows(&[
            (r#"{"f": true, "s": "{\"a\": 1}", "o": {"a": 1}}"#, None),
            (r#"{"f": false, "s": "{\"a\": 2}", "o": {"z": 0}}"#, None),
        ]);
    unroll_inside_a_try_is_refused: "Object.assign($, json(_raw)) try { unroll(a, false); } catch { }",
        [r#"{"a": [1]}"#], Error(&["`unroll`", "`try`"]);
    a_lookup_spreads_its_outputs: r#"Object.assign($, json(_raw)) s: str = $.s; Object.assign($, lookup("codes", s, {column: "code", fields: {text: "message", shown: ["message", "?"]}}))"#,
        [r#"{"s": "200"}"#, r#"{"s": "999"}"#],
        Rows(&[(r#"{"s": "200", "text": "ok", "shown": "ok"}"#, None), (r#"{"s": "999", "shown": "?"}"#, None)]);
    a_declaration_types_a_lookup_output: r#"n: i64; Object.assign($, json(_raw)) s: str = $.s; Object.assign($, lookup("codes", s, {column: "code", fields: {n: "code"}}))"#,
        [r#"{"s": "404"}"#], Rows(&[(r#"{"s": "404", "n": 404}"#, None)]);
    a_lookup_writes_an_object: r#"Object.assign($, json(_raw)) s: str = $.s; x = lookup("codes", s, {column: "code", fields: {text: "message"}});"#,
        [r#"{"s": "200"}"#], Rows(&[(r#"{"s": "200", "x": {"text": "ok"}}"#, None)]);
    a_lookup_into_its_own_input_reads_the_input_first: r#"Object.assign($, json(_raw)) q: str = $.q; q: {text: str} = lookup("codes", q, {column: "code", fields: {text: "message"}});"#,
        [r#"{"q": "200"}"#], Rows(&[(r#"{"q": {"text": "ok"}}"#, None)]);
    a_parse_into_its_own_input_reads_the_input_first: r#"Object.assign($, json(_raw)) q: str = $.q; q: {text: str} = json(q);"#,
        [r#"{"q": "{\"text\": \"ok\"}"}"#], Rows(&[(r#"{"q": {"text": "ok"}}"#, None)]);
    a_lookup_written_to_the_record_reads_its_input_first: r#"Object.assign($, json(_raw)) s: str = $.s; $ = lookup("codes", s, {column: "code", fields: {text: "message"}});"#,
        [r#"{"s": "200"}"#], Rows(&[(r#"{"text": "ok"}"#, None)]);
    a_lookup_names_a_table_the_bundle_has: r#"Object.assign($, json(_raw)) s: str = $.s; Object.assign($, lookup("nope", s, {fields: {t: "message"}}))"#,
        ["{}"], Error(&["`nope`", "codes"]);
    a_function_may_return_from_try_and_catch: r#"function f(s: str): i64 { try { return parseInt(s); } catch { return 0; } } Object.assign($, json(_raw)) s: str = $.s; n = f(s);"#,
        [r#"{"s": "7"}"#, r#"{"s": "z"}"#], Rows(&[(r#"{"s": "7", "n": 7}"#, None), (r#"{"s": "z", "n": 0}"#, None)]);
    a_declared_object_converts_its_datetime: r#"x: {t: datetime, ...} = json(_raw);"#,
        [r#"{"t": "2026-01-01T00:00:00Z"}"#], Rows(&[(r#"{"x": {"t": "2026-01-01T00:00:00.000000Z"}}"#, None)]);
    the_record_takes_an_object: "Object.assign($, json(_raw)) $ = $.a;",
        [r#"{"a": {"b": 1}}"#], Error(&["`$`", "`r: {...} = a; $ = r;`"]);
    deleting_a_local_keeps_the_record_key: "Object.assign($, json(_raw)) let t = 5; delete(t);",
        [r#"{"t": 1, "b": 2}"#], Rows(&[(r#"{"t": 1, "b": 2}"#, None)]);
    a_lookup_into_the_record_keeps_what_it_misses: r#"Object.assign($, json(_raw)) f: bool = $.f; s: str = $.s; if (f) { Object.assign($, lookup("codes", s, {column: "code", fields: {text: "message"}})) }"#,
        [r#"{"f": false, "s": "200", "text": "keep"}"#, r#"{"f": true, "s": "999", "text": "keep"}"#, r#"{"f": true, "s": "200", "text": "keep"}"#],
        Rows(&[
            (r#"{"f": false, "s": "200", "text": "keep"}"#, None),
            (r#"{"f": true, "s": "999", "text": "keep"}"#, None),
            (r#"{"f": true, "s": "200", "text": "ok"}"#, None),
        ]);
    a_guarded_lookup_of_every_match_keeps_its_prior_where_masked: r#"Object.assign($, json(_raw)) f: bool = $.f; s: str = $.s; text: array<str> = $.text; if (f) { Object.assign($, lookup("codes", s, {column: "code", all: true, fields: {text: "message"}})) }"#,
        [r#"{"f": false, "s": "200", "text": ["keep"]}"#, r#"{"f": true, "s": "200", "text": ["keep"]}"#],
        Rows(&[
            (r#"{"f": false, "s": "200", "text": ["keep"]}"#, None),
            (r#"{"f": true, "s": "200", "text": ["ok"]}"#, None),
        ]);
    the_record_leaves_out_the_locals: "Object.assign($, json(_raw)) let t = 1; x = {...$};",
        [r#"{"a": 1}"#], Rows(&[(r#"{"a": 1, "x": {"_raw": "{\"a\": 1}", "a": 1}}"#, None)]);
    the_record_takes_no_field_named_for_a_local: "Object.assign($, json(_raw)) let a = 1; $ = {a: 2, b: 3};",
        ["{}"], Error(&["`a`", "local"]);
    a_closed_type_takes_only_its_fields_from_a_parse: r#"x: {a: i64, c: str} = csv(_raw, ["a", "b"]); y = x.c ?? "none";"#,
        ["1,2"], Rows(&[(r#"{"x": {"a": 1}, "y": "none"}"#, None)]);
    move_replaces_a_name_of_another_type: "Object.assign($, json(_raw)) a: i64 = $.a; c: str = $.c; move(a, c);",
        [r#"{"a": 1, "c": "x"}"#], Rows(&[(r#"{"c": 1}"#, None)]);
    a_csv_parse_in_a_loop_writes_the_record: r#"Object.assign($, json(_raw)) xs: array<str> = $.xs; for (let x of xs) { Object.assign($, csv(x, ["a"])) }"#,
        [r#"{"xs": ["1", "2"]}"#], Rows(&[(r#"{"xs": ["1", "2"], "a": "2"}"#, None)]);
    a_capture_may_not_name_an_engine_field: r#"Object.assign($, grok(_raw, ["%{WORD:__live}"]))"#,
        ["x"], Error(&["`__live`"]);
    a_parse_key_that_names_a_local_declares_it_again: r#"let status = "x"; Object.assign($, grok(_raw, ["%{INT:status}"]));"#,
        ["200"], Error(&["`status` is a local"]);
    a_json_key_leaves_a_local_of_its_name: r#"let status = "x"; Object.assign($, json(_raw)); s = status;"#,
        [r#"{"status": "200"}"#], Rows(&[(r#"{"status": "200", "s": "x"}"#, None)]);
    a_parse_replaces_the_record: "h = 1; $ = json(_raw); n = 2;",
        [r#"{"a": 1}"#], Rows(&[(r#"{"a": 1, "n": 2}"#, None)]);

    // -- two numbers: i64 and f64
    m1_an_i64_met_by_an_f64_widens: r#"n: i64; Object.assign($, json(_raw)) m = n * 1.5;"#,
        [r#"{"n": 3}"#], Rows(&[(r#"{"n": 3, "m": 4.5}"#, None)]);
    m2_an_i64_widens_into_an_f64: r#"n: i64; Object.assign($, json(_raw)) f: f64 = n;"#,
        [r#"{"n": 3}"#], Rows(&[(r#"{"n": 3, "f": 3.0}"#, None)]);
    m3_an_f64_rounds_half_away_from_zero: r#"x: f64; Object.assign($, json(_raw)) n: i64 = x as i64;"#,
        [r#"{"x": 2.5}"#, r#"{"x": -2.5}"#], Rows(&[(r#"{"x": 2.5, "n": 3}"#, None), (r#"{"x": -2.5, "n": -3}"#, None)]);
    m3b_a_declaration_does_not_narrow_an_f64: r#"x: f64; Object.assign($, json(_raw)) n: i64 = x;"#,
        [r#"{"x": 2.5}"#], Error(&["`n: i64 = x as i64`"]);
    m4_division_gives_an_f64: r#"n: i64; Object.assign($, json(_raw)) h = n / 2;"#,
        [r#"{"n": 3}"#], Rows(&[(r#"{"n": 3, "h": 1.5}"#, None)]);
    m5_an_i64_overflow_refuses_the_row: r#"n: i64; Object.assign($, json(_raw)) m = n + 1;"#,
        [r#"{"n": 9223372036854775807}"#], Rows(&[(r#"{"n": 9223372036854775807}"#, Some(r#"n is 9223372036854775807 and 1, whose n + 1 overflowed i64 in m"#))]);
    m6_an_i64_compares_with_an_f64: r#"n: i64; Object.assign($, json(_raw)) e = n == 3.0;"#,
        [r#"{"n": 3}"#], Rows(&[(r#"{"n": 3, "e": true}"#, None)]);
    m7_a_plain_write_does_not_narrow: r#"x = 1; x = 2.5;"#,
        ["{}"], Error(&["`x`", "`2.5 as i64`"]);
    r1_as_rounds_a_literal: r#"x = 1; x = 2.5 as i64;"#,
        ["{}"], Rows(&[(r#"{"x": 3}"#, None)]);
    r2_as_rounds_halves_away_from_zero: r#"x: f64; Object.assign($, json(_raw)) n = x as i64;"#,
        [r#"{"x": 0.5}"#, r#"{"x": 1.5}"#], Rows(&[(r#"{"x": 0.5, "n": 1}"#, None), (r#"{"x": 1.5, "n": 2}"#, None)]);
    r3_an_f64_out_of_range_refuses_the_row: r#"x: f64; Object.assign($, json(_raw)) n: i64 = x as i64;"#,
        [r#"{"x": 1e300}"#], Rows(&[(r#"{"x": 1e300}"#, Some(r#"x is 1e300, which could not convert to i64 in n"#))]);
    r4_a_string_reads_a_number_then_rounds: r#"s: str; Object.assign($, json(_raw)) n: i64 = s as i64;"#,
        [r#"{"s": "2.5"}"#], Rows(&[(r#"{"s": "2.5", "n": 3}"#, None)]);
    r5_as_rounds_an_f64_product: r#"n: i64; Object.assign($, json(_raw)) f: f64 = n; m: i64 = (f * 1.25) as i64;"#,
        [r#"{"n": 2}"#], Rows(&[(r#"{"n": 2, "f": 2.0, "m": 3}"#, None)]);

    d1_integer_division_truncates_toward_zero: r#"n: i64; Object.assign($, json(_raw)) q = idiv(n, 2); r = n % 2;"#,
        [r#"{"n": 7}"#, r#"{"n": -7}"#], Rows(&[(r#"{"n": 7, "q": 3, "r": 1}"#, None), (r#"{"n": -7, "q": -3, "r": -1}"#, None)]);
    d2_integer_division_by_zero_refuses_the_row: r#"n: i64; Object.assign($, json(_raw)) q = idiv(n, 0);"#,
        [r#"{"n": 7}"#], Rows(&[(r#"{"n": 7}"#, Some(r#"0, which idiv(n, 0) divided by in q"#))]);
    d3_integer_division_of_an_f64: r#"x: f64; Object.assign($, json(_raw)) q = idiv(x, 2);"#,
        [r#"{"x": 7.5}"#], Rows(&[(r#"{"x": 7.5, "q": 3.0}"#, None)]);
    // -- a blend of types that do not meet is a variant
    a_blend_declares_an_unbound_name_variant: r#"f: bool; Object.assign($, json(_raw)) v = f ? "a" : 1;"#,
        [r#"{"f": true}"#, r#"{"f": false}"#], Rows(&[(r#"{"f": true, "v": "a"}"#, None), (r#"{"f": false, "v": 1}"#, None)]);
    a_blend_into_a_typed_name_is_a_compile_error: r#"f: bool; Object.assign($, json(_raw)) v: str = ""; v = f ? "a" : 1;"#,
        [r#"{"f": true}"#], Error(&["`v`"]);
    a_name_may_be_declared_variant: r#"f: bool; Object.assign($, json(_raw)) v: variant = null; v = f ? "a" : 1;"#,
        [r#"{"f": true}"#, r#"{"f": false}"#], Rows(&[(r#"{"f": true, "v": "a"}"#, None), (r#"{"f": false, "v": 1}"#, None)]);
    v4_mixed_numbers_are_a_variant: r#"v: variant; Object.assign($, json(_raw)) n: i64 = v;"#,
        [r#"{"v": 1}"#, r#"{"v": 2.5}"#], Rows(&[(r#"{"v": 1, "n": 1}"#, None), (r#"{"v": 2.5, "n": 3}"#, None)]);
}
