//! `readInt`, `readFloat`, `readBytes` and `unpack`: a binary message read out of `bytes`, end to end through the
//! compiler. Each message is written as hex in `_raw`.

use serde_json::{Value, json};
use smeltr::core::buffer::BufferPool;
use smeltr::core::ctx::Ctx;
use smeltr::core::error::ErrorSink;
use smeltr::plan::compile::compile;
use smeltr::plan::output::run_to_json_marked;
use smeltr::plan::topology::SourceMeta;

use crate::harness;

/// Each row of `out = expr`, with `b` the bytes `_raw` spells in hex, and the exceptions every row raised.
fn run(expr: &str, hex: &[&str]) -> (Vec<Value>, Vec<&'static str>) {
    let body = format!("let b = parseBytes(_raw, \"hex\"); out = {expr}; delete(_raw);");
    let config = harness::pipeline(vec![harness::code(0, &body)]);
    let plan = compile(&config, &SourceMeta::KAFKA).unwrap_or_else(|errs| {
        panic!(
            "`{expr}` does not compile: {}",
            errs.iter().map(|e| e.to_string()).collect::<Vec<_>>().join("; ")
        )
    });
    let mut pool = BufferPool::new();
    let mut errors = ErrorSink::new();
    let mut batch = harness::raw_batch(&plan, hex, &mut pool);
    let rows = {
        let mut ctx = Ctx::new(&mut pool, &mut errors);
        run_to_json_marked(&plan, &Default::default(), &mut batch, &mut ctx, false, None)
            .iter()
            .filter_map(|r| r.json.as_ref())
            .map(|row| serde_json::from_slice::<Value>(row).unwrap())
            .map(|row| row.get("out").cloned().unwrap_or(Value::Null))
            .collect()
    };
    batch.reclaim(&mut pool);
    let kinds = errors.entries().iter().map(|e| e.kind.label()).collect();
    (rows, kinds)
}

/// The compile errors of `out = expr`, joined.
fn refused(expr: &str) -> String {
    let body = format!("let b = parseBytes(_raw, \"hex\"); out = {expr};");
    match compile(&harness::pipeline(vec![harness::code(0, &body)]), &SourceMeta::KAFKA) {
        Ok(_) => panic!("`{expr}` compiled"),
        Err(errs) => errs.iter().map(|e| e.to_string()).collect::<Vec<_>>().join("; "),
    }
}

#[test]
fn an_integer_reads_at_its_position_in_its_order() {
    let (rows, kinds) = run(
        "[readInt(b, 1, \"u16\"), readInt(b, 1, \"u16\", \"little\"), readInt(b, 0, \"i8\")]",
        &["ff0102"],
    );
    assert_eq!(rows, [json!([0x0102, 0x0201, -1])]);
    assert!(kinds.is_empty(), "{kinds:?}");
}

#[test]
fn a_float_reads_in_its_order() {
    let (rows, _) = run(
        "[readFloat(b, 0, \"f32\"), readFloat(b, 0, \"f32\", \"little\")]",
        &["3fc00000", "0000c03f"],
    );
    assert_eq!(rows[0][0], json!(1.5));
    assert_eq!(rows[1][1], json!(1.5));
}

#[test]
fn a_read_past_the_end_is_null_and_out_of_range() {
    let (rows, kinds) = run("readInt(b, 1, \"u32\")", &["01020304"]);
    assert_eq!(rows, [Value::Null]);
    assert_eq!(kinds, ["IndexOutOfRange"]);
    let (rows, kinds) = run("readBytes(b, 2, 3)", &["01020304"]);
    assert_eq!(rows, [Value::Null]);
    assert_eq!(kinds, ["IndexOutOfRange"]);
}

#[test]
fn a_type_or_order_it_does_not_know_is_refused() {
    let (rows, kinds) = run(
        "[readInt(b, 0, \"u24\"), readInt(b, 0, \"u8\", \"middle\"), readInt(b, 0, \"f32\"), readFloat(b, 0, \"u16\")]",
        &["01020304"],
    );
    assert_eq!(rows, [json!([null, null, null, null])]);
    assert_eq!(kinds, ["CoercionRefused"; 4]);
}

#[test]
fn a_float_no_f64_holds_is_nan() {
    let (rows, kinds) = run("readFloat(b, 0, \"f32\")", &["7fc00000"]);
    assert_eq!(rows, [Value::Null]);
    assert_eq!(kinds, ["NaN"]);
}

/// A number that reads as no value is one field: the fields after it still read.
#[test]
fn unpack_reads_on_past_a_number_with_no_value() {
    let (rows, kinds) = run(
        r#"unpack(b, {big: "u64", f: "f32", c: "u8"})"#,
        &["ffffffffffffffff7fc0000005"],
    );
    assert_eq!(rows, [json!({"c": 5})]);
    assert_eq!(kinds, ["Overflow", "NaN"]);
}

#[test]
fn a_u64_past_i64_overflows() {
    let (rows, kinds) = run("readInt(b, 0, \"u64\")", &["ffffffffffffffff"]);
    assert_eq!(rows, [Value::Null]);
    assert_eq!(kinds, ["Overflow"]);
}

#[test]
fn unpack_reads_each_field_where_the_one_before_ended() {
    let layout = r#"unpack(b, {
        version: "u8",
        len: "u16",
        host: {type: "str", length: "len"},
        port: {type: "u16", order: "little"},
        tag: {type: "bytes", prefix: "u8"},
        temp: "f32",
        body: "bytes",
    })"#;
    // 01 | 0003 "abc" | 5000 (80, little) | 02 "hi" | 1.5 | "zz"
    let (rows, kinds) = run(layout, &["01000361626350000268693fc000007a7a"]);
    assert!(kinds.is_empty(), "{kinds:?}");
    let want = json!({"version": 1, "len": 3, "host": "abc", "port": 80, "tag": "aGk=", "temp": 1.5, "body": "eno="});
    assert_eq!(rows, [want]);
}

#[test]
fn a_field_the_message_is_too_short_for_is_null_and_so_is_every_field_after() {
    let (rows, kinds) = run(
        r#"b.unpack({a: "u8", s: {type: "str", prefix: "u8"}, c: "u8"})"#,
        &["0105616263"],
    );
    assert_eq!(rows, [json!({"a": 1})]);
    assert_eq!(kinds, ["IndexOutOfRange"]);
}

#[test]
fn unpack_writes_into_the_record() {
    let body = "Object.assign($, unpack(parseBytes(_raw, \"hex\"), {a: \"u16\", b: \"i16\"}, {order: \"little\"})); \
                delete(_raw);";
    let config = harness::pipeline(vec![harness::code(0, body)]);
    let plan = compile(&config, &SourceMeta::KAFKA).unwrap_or_else(|e| panic!("{}", e[0]));
    let mut pool = BufferPool::new();
    let mut errors = ErrorSink::new();
    let mut batch = harness::raw_batch(&plan, &["0100ffff"], &mut pool);
    let rows: Vec<Value> = {
        let mut ctx = Ctx::new(&mut pool, &mut errors);
        run_to_json_marked(&plan, &Default::default(), &mut batch, &mut ctx, false, None)
            .iter()
            .filter_map(|r| r.json.as_ref())
            .map(|row| serde_json::from_slice(row).unwrap())
            .collect()
    };
    batch.reclaim(&mut pool);
    assert_eq!(rows, [json!({"a": 1, "b": -1})]);
}

#[test]
fn a_layout_that_cannot_be_read_is_refused_when_it_compiles() {
    for (layout, says) in [
        (r#"{a: "u24"}"#, "`a`'s type is"),
        (r#"{a: "bytes", b: "u8"}"#, "`a` runs to the end of the message"),
        (
            r#"{a: {type: "str", length: "n"}}"#,
            "names `n`, which is not a field before it",
        ),
        (
            r#"{n: "f32", a: {type: "str", length: "n"}}"#,
            "which is not an integer",
        ),
        (
            r#"{a: {type: "bytes", length: 1, prefix: "u8"}}"#,
            "a `length` or a `prefix`, not both",
        ),
        (r#"{a: {type: "u8", charset: "utf-8"}}"#, "takes no `charset`"),
        (r#"{a: {type: "u8", order: "middle"}}"#, "\"big\" or \"little\""),
        (r#"{}"#, "names no field"),
        (r#"{a: {type: "str", charset: "klingon"}}"#, "an encoding's name"),
        (
            r#"{a: {type: "bytes", length: 2, order: "little"}}"#,
            "has no `prefix` for an `order` to read",
        ),
    ] {
        let errs = refused(&format!("unpack(b, {layout})"));
        assert!(errs.contains(says), "{layout}: {errs}");
    }
    assert!(refused(r#"unpack(b, {a: "u8"}, {endian: "big"})"#).contains("takes no option `endian`"));
    assert!(refused(r#"unpack("text", {a: "u8"})"#).contains("bytes"));
}

/// A field's name may hold a control character.
#[test]
fn a_field_may_have_any_name() {
    let (rows, kinds) = run(r#"unpack(b, {"\u0008\u{c}\u0001": "u8"})"#, &["07"]);
    assert_eq!(rows, [json!({"\u{8}\u{c}\u{1}": 7})]);
    assert!(kinds.is_empty(), "{kinds:?}");
}

/// `indexOf`, `lastIndexOf` and `includes` search bytes for a byte's number or a run of bytes, by byte position.
#[test]
fn a_byte_or_a_run_is_found_by_its_byte_position() {
    let (rows, kinds) = run(
        r#"[b.indexOf(0), b.indexOf(0, 2), b.indexOf(parseBytes("0d0a", "hex")), b.lastIndexOf(0), b.lastIndexOf(0, -3),
            b.indexOf(256), b.indexOf(0x7a, -1), b.includes(0x61, 1) ? 1 : 0, b.includes(parseBytes("0a", "hex")) ? 1 : 0]"#,
        // "a" 00 "b" 00 0d 0a "z"
        &["610062000d0a7a"],
    );
    assert_eq!(rows, [json!([1, 3, 4, 3, 3, -1, 6, 0, 1])]);
    assert!(kinds.is_empty(), "{kinds:?}");
}

/// A zero-terminated string, read with `indexOf` and `readBytes`.
#[test]
fn a_zero_terminated_string_reads_up_to_its_zero() {
    let (rows, _) = run(
        r#"{name: decode(readBytes(b, 1, b.indexOf(0, 1) - 1), "utf-8"), next: readInt(b, b.indexOf(0, 1) + 1, "u8")}"#,
        &["0568690007"],
    );
    assert_eq!(rows, [json!({"name": "hi", "next": 7})]);
}
