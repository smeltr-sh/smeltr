//! End to end: a config JSON string in, a running pipeline out. The
//! library's own tests cover load and save; this covers the last mile,
//! that `load_str`'s output compiles and runs against real `_raw` bytes.

use smeltr::config_file::{load_bundle_dir, load_str};
use smeltr::core::batch::{Batch, RAW_SLOTS, SLOT_LIVE, SLOT_RAW, raw_layout};

/// `__source.offset` for a Kafka source: third of the three fields it declares.
const SLOT_OFFSET: usize = RAW_SLOTS + 2;
use smeltr::core::bits;
use smeltr::core::buffer::BufferPool;
use smeltr::core::column::{Column, DataType, StrBuilder, VarType};
use smeltr::core::ctx::Ctx;
use smeltr::core::error::ErrorSink;
use smeltr::core::error::ExceptionKind;
use smeltr::plan::compile::compile;
use smeltr::plan::config::{Answer, Payload};
use smeltr::plan::output::run_to_json;
use smeltr::plan::topology::{DocIdPolicy, route_masks};

fn join_errors(e: &[smeltr::plan::config::CompileError]) -> String {
    e.iter().map(|x| x.to_string()).collect::<Vec<_>>().join("; ")
}

/// Every problem the check a save runs (`config_file::validate`) finds in `json`, joined.
fn refusal(json: &str) -> String {
    let file: smeltr::config_file::FileConfig = serde_json::from_str(json).expect("parses");
    let errors = smeltr::config_file::validation_errors(file);
    errors.iter().map(|e| e.to_string()).collect::<Vec<_>>().join("; ")
}

/// Compiles `pipeline` and runs `docs` through it as `_raw`.
fn run_pipeline(pipeline: &smeltr::plan::config::PipelineConfig, docs: &[&str]) -> Vec<serde_json::Value> {
    let plan = compile(pipeline, &smeltr::plan::topology::SourceMeta::KAFKA)
        .unwrap_or_else(|e| panic!("compile: {}", join_errors(&e)));
    let mut pool = BufferPool::new();
    let mut errors = ErrorSink::new();
    let mut batch = Batch::new(&plan.slot_layout, docs.len(), 0);
    let mut b = StrBuilder::new(&mut pool, VarType::Bytes, docs.len());
    b.reserve(&mut pool, docs.iter().map(|d| d.len()).sum());
    for d in docs {
        b.push(d.as_bytes());
    }
    batch.put(SLOT_RAW, b.finish(), &mut pool);
    let mut ctx = Ctx::new(&mut pool, &mut errors);
    let rows = run_to_json(&plan, &mut batch, &mut ctx)
        .iter()
        .map(|row| serde_json::from_slice(row).unwrap())
        .collect();
    batch.reclaim(&mut pool);
    rows
}

/// A config with one Kafka source, `destinations`, `pipelines` and
/// `routes` spliced in.
fn cfg(destinations: &str, pipelines: &str, routes: &str) -> String {
    format!(
        r#"{{"version":1,
            "sources":[{{"id":"s","type":"kafka","brokers":"b:9092","topics":["t"],"groupId":"g"}}],
            "destinations":[{destinations}],
            "pipelines":[{pipelines}],
            "routes":[{routes}]}}"#
    )
}

const KAFKA_OUT: &str = r#"{"id":"d","type":"kafka","brokers":"b:9092","defaultTopic":"o"}"#;
const EMPTY_PIPELINE: &str = r#"{"id":"p","name":"p","steps":[]}"#;

/// Every step kind but `lookup`: parse JSON, compute `severity`, branch on
/// it inside an `if`.
fn example_config_json() -> &'static str {
    r#"{
      "version": 1,
      "sources": [
        { "id": "kafka-main", "type": "kafka", "brokers": "b1:9092", "topics": ["logs"], "groupId": "smeltr" }
      ],
      "destinations": [
        { "id": "kafka-out", "type": "kafka", "brokers": "b1:9092", "defaultTopic": "logs-out" }
      ],
      "pipelines": [
        {
          "id": "main",
          "name": "Main pipeline",
          "steps": [
            { "id": "parse", "type": "parser", "input": "_raw", "kind": { "type": "json" },
              "declared": [ { "name": "status", "type": "i64" } ] },
            { "id": "severity", "type": "eval",
              "assignments": [ { "target": "severity", "expr": "status >= 400 ? \"high\" : \"low\"" } ] },
            { "id": "declare-alert", "type": "code", "body": "alert: bool = null;" },
            { "id": "high-only", "type": "if", "condition": "severity == \"high\"",
              "then": [
                { "id": "tag-high", "type": "eval", "assignments": [ { "target": "alert", "expr": "true" } ] }
              ],
              "else": [] }
          ]
        }
      ],
      "routes": [
        { "id": "catch-all", "source": "kafka-main", "condition": null, "pipeline": "main",
          "deliver": [ { "to": ["kafka-out"] } ] }
      ]
    }"#
}

fn assert_severity_rows(rows: &[serde_json::Value]) {
    assert_eq!(rows[0]["status"], 200);
    assert_eq!(rows[0]["severity"], "low");
    assert!(rows[0].get("alert").is_none());
    assert_eq!(rows[1]["status"], 500);
    assert_eq!(rows[1]["severity"], "high");
    assert_eq!(rows[1]["alert"], true);
}

#[test]
fn a_gui_authored_config_loads_compiles_and_runs() {
    let system = load_str(example_config_json()).unwrap_or_else(|e| panic!("load failed: {e}"));
    assert_eq!(system.pipelines.len(), 1);
    assert_eq!(system.topology.routes.len(), 1);
    system
        .compile_topology()
        .unwrap_or_else(|e| panic!("topology: {}", join_errors(&e)));
    let rows = run_pipeline(&system.pipelines[0], &[r#"{"status":200}"#, r#"{"status":500}"#]);
    assert_severity_rows(&rows);
}

/// The wire shape end to end: a config JSON declaring a field `"datetime"`, exactly what the GUI editor writes,
/// loads, compiles, and produces a field an expression can call `year` on -- the sugar's whole path, not just the DTO
/// or the compiler in isolation. The bare type detects the shape, so an epoch count and a text stamp both land.
#[test]
fn a_declared_datetime_field_loads_compiles_and_runs() {
    let json = r#"{
      "version": 1,
      "pipelines": [{
        "id": "p", "name": "p",
        "steps": [
          { "id": "parse", "type": "parser", "input": "_raw", "kind": { "type": "json" },
            "declared": [ { "name": "ts", "type": "datetime" } ] },
          { "id": "extract", "type": "eval",
            "assignments": [ { "target": "year", "expr": "year(ts)" } ] }
        ]
      }]
    }"#;
    let system = load_str(json).unwrap_or_else(|e| panic!("load failed: {e}"));
    let rows = run_pipeline(
        &system.pipelines[0],
        &[r#"{"ts":1700000000000}"#, r#"{"ts":"2023/11/14 22:13:20"}"#],
    );
    assert_eq!(rows[0]["year"], 2023);
    assert_eq!(rows[1]["year"], 2023);
}

/// The other wire shape: `{"type":"datetime","format":...}` pins the text to one `parseTime` format.
#[test]
fn a_declared_datetime_field_with_a_format_loads_compiles_and_runs() {
    let json = r#"{
      "version": 1,
      "pipelines": [{
        "id": "p", "name": "p",
        "steps": [
          { "id": "parse", "type": "parser", "input": "_raw", "kind": { "type": "json" },
            "declared": [ { "name": "ts", "type": { "type": "datetime", "format": "%d/%m/%Y" } } ] },
          { "id": "extract", "type": "eval",
            "assignments": [ { "target": "month", "expr": "month(ts)" } ] }
        ]
      }]
    }"#;
    let system = load_str(json).unwrap_or_else(|e| panic!("load failed: {e}"));
    let rows = run_pipeline(&system.pipelines[0], &[r#"{"ts":"03/11/2023"}"#]);
    assert_eq!(rows[0]["month"], 11);
}

/// A step's own declaration holds after a second parser runs: the first parser's declared `datetime` is still one, so
/// `year` reads it.
#[test]
fn a_second_parser_does_not_cost_the_first_one_its_declaration() {
    let json = r#"{
      "version": 1,
      "pipelines": [{
        "id": "p", "name": "p",
        "steps": [
          { "id": "parse", "type": "parser", "input": "_raw", "kind": { "type": "json" },
            "declared": [ { "name": "ts", "type": "datetime" } ] },
          { "id": "extract", "type": "eval",
            "assignments": [ { "target": "year", "expr": "year(ts)" } ] },
          { "id": "again", "type": "parser", "input": "_raw", "kind": { "type": "json" } }
        ]
      }]
    }"#;
    let system = load_str(json).unwrap_or_else(|e| panic!("load failed: {e}"));
    let rows = run_pipeline(&system.pipelines[0], &[r#"{"ts":1700000000000}"#]);
    // The second parser declares nothing, so it does not claim `ts`: the declaration upstream stands, converted.
    assert_eq!(rows[0]["ts"], "2023-11-14T22:13:20.000000Z", "{}", rows[0]);
    assert_eq!(rows[0]["year"], 2023, "{}", rows[0]);
}

/// A later parser that declares a bound name another type is a compile error: it assigns the name by presence, and one
/// column cannot hold both types.
#[test]
fn a_second_parser_cannot_declare_a_bound_name_another_type() {
    let json = r#"{
      "version": 1,
      "pipelines": [{
        "id": "p", "name": "p",
        "steps": [
          { "id": "parse", "type": "parser", "input": "_raw", "kind": { "type": "json" },
            "declared": [ { "name": "ts", "type": "datetime" } ] },
          { "id": "extract", "type": "eval",
            "assignments": [ { "target": "year", "expr": "year(ts)" } ] },
          { "id": "again", "type": "parser", "input": "_raw", "kind": { "type": "json" },
            "declared": [ { "name": "ts", "type": "i64" } ] }
        ]
      }]
    }"#;
    let system = load_str(json).unwrap_or_else(|e| panic!("load failed: {e}"));
    let e = join_errors(&compile(&system.pipelines[0], &smeltr::plan::topology::SourceMeta::KAFKA).unwrap_err());
    assert!(e.contains("`ts` is already datetime"), "{e}");
}

/// A key the second parser's input lacks on a row leaves the name's value alone: each parser assigns a declared name by
/// presence (plan rule 3).
#[test]
fn a_second_parser_leaves_a_name_its_input_lacks_alone() {
    let json = r#"{
      "version": 1,
      "pipelines": [{
        "id": "p", "name": "p",
        "steps": [
          { "id": "parse", "type": "parser", "input": "_raw", "kind": { "type": "json" },
            "declared": [ { "name": "x", "type": "i64" }, { "name": "inner", "type": "str" } ] },
          { "id": "again", "type": "parser", "input": "inner", "kind": { "type": "json" },
            "declared": [ { "name": "x", "type": "i64" } ] }
        ]
      }]
    }"#;
    let system = load_str(json).unwrap_or_else(|e| panic!("load failed: {e}"));
    let rows = run_pipeline(
        &system.pipelines[0],
        &[r#"{"x":1,"inner":"{}"}"#, r#"{"x":1,"inner":"{\"x\":2}"}"#],
    );
    assert_eq!(rows[0]["x"], 1, "{}", rows[0]);
    assert_eq!(rows[1]["x"], 2, "{}", rows[1]);
}

/// A field an earlier step computed keeps its value through the parser after it, and the parser's keys reach the
/// output beside it. One parser is enough: this pipeline has no second one.
#[test]
fn an_eval_before_the_only_parser_survives_it() {
    let json = r#"{
      "version": 1,
      "pipelines": [{
        "id": "p", "name": "p",
        "steps": [
          { "id": "e", "type": "eval", "assignments": [ { "target": "x", "expr": "1" } ] },
          { "id": "parse", "type": "parser", "input": "_raw", "kind": { "type": "json" } }
        ]
      }]
    }"#;
    let system = load_str(json).unwrap_or_else(|e| panic!("load failed: {e}"));
    let rows = run_pipeline(&system.pipelines[0], &[r#"{"a":1}"#]);
    assert_eq!(rows[0]["x"], 1, "the eval's own field: {}", rows[0]);
    assert_eq!(rows[0]["a"], 1, "and the parser's: {}", rows[0]);
}

/// The same with two parsers: the second reads `body` and has no business touching either the first parser's
/// declared `ts` or the eval's `yr`.
#[test]
fn a_field_computed_before_a_second_parser_reaches_the_output() {
    let json = r#"{
      "version": 1,
      "pipelines": [{
        "id": "p", "name": "p",
        "steps": [
          { "id": "parse", "type": "parser", "input": "_raw", "kind": { "type": "json" },
            "declared": [ { "name": "ts", "type": "i64" }, { "name": "body", "type": "str" } ] },
          { "id": "extract", "type": "eval", "assignments": [ { "target": "yr", "expr": "ts + 1" } ] },
          { "id": "inner", "type": "parser", "input": "body", "kind": { "type": "json" } }
        ]
      }]
    }"#;
    let system = load_str(json).unwrap_or_else(|e| panic!("load failed: {e}"));
    let rows = run_pipeline(&system.pipelines[0], &[r#"{"ts":1700000000000,"body":"{\"k\":1}"}"#]);
    assert_eq!(rows[0]["ts"], 1_700_000_000_000i64, "{}", rows[0]);
    assert_eq!(rows[0]["yr"], 1_700_000_000_001i64, "{}", rows[0]);
    assert_eq!(rows[0]["k"], 1, "the second parser's own field: {}", rows[0]);
}

/// The envelope shape: grok pulls a level and a JSON body off the line, a second parser reads the body. Both grok
/// captures were lost.
#[test]
fn a_grok_capture_survives_a_following_json_parser() {
    let json = r#"{
      "version": 1,
      "pipelines": [{
        "id": "p", "name": "p",
        "steps": [
          { "id": "line", "type": "parser", "input": "_raw",
            "kind": { "type": "grok", "patterns": ["%{WORD:level} %{GREEDYDATA:body}"] },
            "declared": [ { "name": "level", "type": "str" }, { "name": "body", "type": "str" } ] },
          { "id": "inner", "type": "parser", "input": "body", "kind": { "type": "json" } }
        ]
      }]
    }"#;
    let system = load_str(json).unwrap_or_else(|e| panic!("load failed: {e}"));
    // A grok line is raw text, not JSON: the body needs no escaping.
    let rows = run_pipeline(&system.pipelines[0], &[r#"INFO {"k":1}"#]);
    assert_eq!(rows[0]["level"], "INFO", "{}", rows[0]);
    assert_eq!(rows[0]["k"], 1, "{}", rows[0]);
}

/// A route preview's records name the source they stand for, and its kind.
#[test]
fn a_previewed_record_names_its_source() {
    let pipeline = r#"{"id":"p","name":"p","steps":[
        {"id":"name","type":"eval","assignments":[{"target":"from","expr":"__source.name + \"/\" + __source.type"}]}]}"#;
    let route = r#"{"id":"r","source":"s","pipeline":"p","deliver":[{"to":["d"]}]}"#;
    let json = cfg(KAFKA_OUT, pipeline, route);
    let system = load_str(&json).unwrap_or_else(|e| panic!("load failed: {e}"));
    let compiled = system
        .compile_topology()
        .unwrap_or_else(|e| panic!("compile: {}", join_errors(&e)));
    let source = &system.topology.sources[0];

    let records = sample_records(&[("t", "{}")]);
    let rows = smeltr::runtime::preview_routes(&compiled, source, &records, budget());
    let doc = &rows[0].through[0].documents[0];
    assert!(doc.document.contains(r#""from":"s/kafka""#), "{}", doc.document);
}

/// `__destination.topic` is an ordinary eval target, not a reserved name. A parser after the eval must leave it be, or the row
/// falls back to the destination's default topic.
#[test]
fn a_routing_field_set_before_a_parser_survives_it() {
    let pipeline = r#"{"id":"p","name":"p","steps":[
        {"id":"route","type":"eval","assignments":[{"target":"__destination.topic","expr":"\"audit\""},
            {"target":"__destination.prefix","expr":"\"tenant/a/\""}]},
        {"id":"parse","type":"parser","input":"_raw","kind":{"type":"json"}}]}"#;
    let route = r#"{"id":"r","source":"s","pipeline":"p","deliver":[{"to":["d"]}]}"#;
    let json = cfg(KAFKA_OUT, pipeline, route);
    let system = load_str(&json).unwrap_or_else(|e| panic!("load failed: {e}"));
    let compiled = system
        .compile_topology()
        .unwrap_or_else(|e| panic!("compile: {}", join_errors(&e)));
    let source = &system.topology.sources[0];

    let records = sample_records(&[("t", r#"{"a":1}"#)]);
    let rows = smeltr::runtime::preview_routes(&compiled, source, &records, budget());
    let doc = &rows[0].through[0].documents[0];
    assert_eq!(
        doc.destination.get("topic").map(String::as_str),
        Some("audit"),
        "{}",
        doc.document
    );
    assert_eq!(
        doc.destination.get("prefix").map(String::as_str),
        Some("tenant/a/"),
        "the preview shows it"
    );
}

/// A declaration in code of a key no parser declared is a compile error that names the conversion. Neither parser
/// declares `ts`, so the key passes through in the record's unclaimed fields: `ts: i64;` in the code step after them
/// is an error that points at `ts: i64 = ts`. The read in `yr = ts + 1` is then a variant read of those fields, and a
/// sum needs a type, so it is a second error. Each error names that step.
#[test]
fn a_declaration_in_code_of_a_key_no_parser_declared_does_not_compile() {
    let json = r#"{
      "version": 1,
      "pipelines": [{
        "id": "p", "name": "p",
        "steps": [
          { "id": "outer", "type": "parser", "input": "_raw", "kind": { "type": "json" },
            "declared": [ { "name": "body", "type": "str" } ] },
          { "id": "inner", "type": "parser", "input": "body", "kind": { "type": "json" } },
          { "id": "e", "type": "code", "body": "ts: i64; yr = ts + 1;" }
        ]
      }]
    }"#;
    let system = load_str(json).unwrap_or_else(|e| panic!("load failed: {e}"));
    let pipeline = &system.pipelines[0];
    let errs = compile(pipeline, &smeltr::plan::topology::SourceMeta::KAFKA)
        .expect_err("a declaration of a name nothing bound must not compile");
    let conversion = "`ts` may be a key of the record; to give it a type, write `ts: i64 = ts`";
    assert!(
        errs.iter().any(|e| e.message.contains(conversion)),
        "{}",
        join_errors(&errs)
    );
    for e in &errs {
        assert!(
            e.message.contains("`ts` may be a key of the record") || e.message.contains("`ts` is a variant"),
            "{}",
            join_errors(&errs)
        );
        assert_eq!(
            system.step_id_for(Some(pipeline.id), e.function).as_deref(),
            Some("e"),
            "{}",
            join_errors(&errs)
        );
    }
}

/// A declaration is a definite write, so a later parser leaves the name alone.
#[test]
fn declaring_it_on_the_first_parser_keeps_the_value_too() {
    let json = r#"{
      "version": 1,
      "pipelines": [{
        "id": "p", "name": "p",
        "steps": [
          { "id": "outer", "type": "parser", "input": "_raw", "kind": { "type": "json" },
            "declared": [ { "name": "ts", "type": "i64" }, { "name": "body", "type": "str" } ] },
          { "id": "inner", "type": "parser", "input": "body", "kind": { "type": "json" } },
          { "id": "e", "type": "eval", "assignments": [ { "target": "yr", "expr": "ts + 1" } ] }
        ]
      }]
    }"#;
    let system = load_str(json).unwrap_or_else(|e| panic!("load failed: {e}"));
    let rows = run_pipeline(&system.pipelines[0], &[r#"{"ts":1700000000000,"body":"{\"k\":1}"}"#]);
    assert_eq!(rows[0]["yr"], 1_700_000_000_001i64, "{}", rows[0]);
}

/// Two parses of one record can discover the same name -- a JSON payload nested in a field of another. The later
/// parse's keys merge over the earlier's in the unclaimed fields, so the row carries the inner document's value.
#[test]
fn the_later_parse_wins_a_discovered_name() {
    let json = r#"{
      "version": 1,
      "pipelines": [{
        "id": "p", "name": "p",
        "steps": [
          { "id": "outer", "type": "parser", "input": "_raw", "kind": { "type": "json" },
            "declared": [ { "name": "body", "type": "str" } ] },
          { "id": "inner", "type": "parser", "input": "body", "kind": { "type": "json" } }
        ]
      }]
    }"#;
    let system = load_str(json).unwrap_or_else(|e| panic!("load failed: {e}"));
    let rows = run_pipeline(&system.pipelines[0], &[r#"{"k":"outer","body":"{\"k\":\"inner\"}"}"#]);
    assert_eq!(rows[0]["k"], "inner", "{}", rows[0]);
}

/// A record's keys are data, however many there are: two parses of one record, each with more keys than any column
/// count would hold, keep every key of both, each where the record had it.
#[test]
fn two_parses_of_many_keys_keep_every_key() {
    let json = r#"{
      "version": 1,
      "pipelines": [{
        "id": "p", "name": "p",
        "steps": [
          { "id": "outer", "type": "parser", "input": "_raw", "kind": { "type": "json" },
            "declared": [ { "name": "body", "type": "str" } ] },
          { "id": "inner", "type": "parser", "input": "body", "kind": { "type": "json" } }
        ]
      }]
    }"#;
    let system = load_str(json).unwrap_or_else(|e| panic!("load failed: {e}"));

    let per_side = 300;
    let wide = |prefix: &str| -> serde_json::Map<String, serde_json::Value> {
        (0..per_side)
            .map(|i| (format!("{prefix}{i}"), serde_json::json!(i)))
            .collect()
    };
    let inner = serde_json::Value::Object(wide("i"));
    let mut outer = wide("o");
    outer.insert("body".into(), serde_json::json!(inner.to_string()));
    let raw = serde_json::Value::Object(outer).to_string();

    let rows = run_pipeline(&system.pipelines[0], &[&raw]);
    let row = rows[0].as_object().expect("a row is an object");
    for prefix in ["o", "i"] {
        for i in 0..per_side {
            let key = format!("{prefix}{i}");
            assert_eq!(row.get(&key), Some(&serde_json::json!(i)), "{key}");
        }
    }
}

/// A record's keys cost time in their number, however many there are. Every place that finds one key among an
/// object's keys is here, 200,000 keys deep: the keys nothing claims at the top and in an object, an open declared
/// object's keys it does not name, a second parse's keys laid over the first's, a spread, and `fromEntries`. A scan per
/// key takes tens of minutes; one pass takes seconds.
#[test]
fn a_record_of_many_keys_parses_and_writes_in_time_linear_in_them() {
    let json = r#"{
      "version": 1,
      "pipelines": [{
        "id": "p", "name": "p",
        "steps": [
          { "id": "outer", "type": "parser", "input": "_raw", "kind": { "type": "json" },
            "declared": [
              { "name": "d", "type": { "type": "struct", "fields": [ { "name": "a", "type": "i64" } ], "open": true } },
              { "name": "ks", "type": { "type": "array", "of": "str" } },
              { "name": "body", "type": "str" },
              { "name": "h", "type": { "type": "struct", "fields": [], "open": true } }
            ] },
          { "id": "inner", "type": "parser", "input": "body", "kind": { "type": "json" } },
          { "id": "build", "type": "code",
            "body": "s = {...h}; m = fromEntries(ks.map(k => ({key: k, value: 1})));" }
        ]
      }]
    }"#;
    let system = load_str(json).unwrap_or_else(|e| panic!("load failed: {e}"));

    let n = 200_000;
    let wide = |prefix: &str| -> serde_json::Map<String, serde_json::Value> {
        (0..n).map(|i| (format!("{prefix}{i}"), serde_json::json!(i))).collect()
    };
    let mut record = wide("t");
    record.insert("o".into(), serde_json::Value::Object(wide("o")));
    let mut d = wide("d");
    d.insert("a".into(), serde_json::json!(1));
    record.insert("d".into(), serde_json::Value::Object(d));
    record.insert(
        "body".into(),
        serde_json::json!(serde_json::Value::Object(wide("i")).to_string()),
    );
    record.insert("h".into(), serde_json::Value::Object(wide("h")));
    record.insert(
        "ks".into(),
        serde_json::json!((0..n).map(|i| format!("k{i}")).collect::<Vec<_>>()),
    );
    let raw = serde_json::Value::Object(record).to_string();

    let start = std::time::Instant::now();
    let rows = run_pipeline(&system.pipelines[0], &[&raw]);
    let took = start.elapsed();

    let row = rows[0].as_object().expect("a row is an object");
    let len = |v: &serde_json::Value| v.as_object().map_or(0, |o| o.len());
    assert_eq!(row.keys().filter(|k| k.starts_with('t')).count(), n);
    assert_eq!(row.keys().filter(|k| k.starts_with('i')).count(), n);
    assert_eq!(len(&row["o"]), n);
    assert_eq!(len(&row["d"]), n + 1);
    assert_eq!(len(&row["s"]), n);
    assert_eq!(len(&row["m"]), n);
    assert!(took < std::time::Duration::from_secs(120), "{took:?}");
}

/// An unroll after a second parser emitted no rows at all: the parser had rebound `xs` to a null of its own, and
/// unrolling a null array produces nothing. The operator's symptom was a pipeline that stopped producing records.
#[test]
fn an_unroll_after_a_second_parser_still_emits_a_row_per_element() {
    let json = r#"{
      "version": 1,
      "pipelines": [{
        "id": "p", "name": "p",
        "steps": [
          { "id": "outer", "type": "parser", "input": "_raw", "kind": { "type": "json" },
            "declared": [
              { "name": "xs", "type": { "type": "array", "of": "i64" } },
              { "name": "body", "type": "str" }
            ] },
          { "id": "inner", "type": "parser", "input": "body", "kind": { "type": "json" } },
          { "id": "u", "type": "unroll", "field": "xs", "keepParentFields": true }
        ]
      }]
    }"#;
    let system = load_str(json).unwrap_or_else(|e| panic!("load failed: {e}"));
    let rows = run_pipeline(&system.pipelines[0], &[r#"{"xs":[1,2],"body":"{\"k\":1}"}"#]);
    assert_eq!(rows.len(), 2, "one row per element: {rows:?}");
    assert_eq!(rows[0]["xs"], 1);
    assert_eq!(rows[1]["xs"], 2);
    assert_eq!(rows[0]["k"], 1, "and the inner parse's field is on each: {}", rows[0]);
}

/// A bundle's own logs, routed like anything else. The level is the one thing an author picks, and it is
/// written down rather than numbered.
#[test]
fn a_logs_source_loads_with_the_level_it_names() {
    use smeltr::plan::topology::SourceKind;

    let json = r#"{
      "version": 1,
      "sources": [{ "id": "own-logs", "type": "logs", "minLevel": "warn" }]
    }"#;
    let system = load_str(json).unwrap_or_else(|e| panic!("load failed: {e}"));
    match &system.topology.sources[0].kind {
        SourceKind::Logs(cfg) => assert_eq!(cfg.min_level, tracing::Level::WARN),
        other => panic!("a logs source: {other:?}"),
    }

    // Absent is `info`, which is what a process collects unless `SMELTR_LOG_LEVEL` says otherwise.
    let json = r#"{ "version": 1, "sources": [{ "id": "own-logs", "type": "logs" }] }"#;
    let system = load_str(json).unwrap_or_else(|e| panic!("load failed: {e}"));
    match &system.topology.sources[0].kind {
        SourceKind::Logs(cfg) => assert_eq!(cfg.min_level, tracing::Level::INFO),
        other => panic!("a logs source: {other:?}"),
    }
}

#[test]
fn a_logs_source_naming_a_level_nothing_spells_that_way_is_refused() {
    let json = r#"{
      "version": 1,
      "sources": [{ "id": "own-logs", "type": "logs", "minLevel": "verbose" }]
    }"#;
    let err = load_str(json).unwrap_err().to_string();
    assert!(err.contains("\"verbose\""), "it names what was written: {err}");
    assert!(
        err.contains("error, warn, info, debug, trace"),
        "and what it could be: {err}"
    );
}

/// Each logs source reads the whole stream, so a bundle may declare more than one.
#[test]
fn two_logs_sources_in_one_bundle_load() {
    let json = r#"{
      "version": 1,
      "sources": [
        { "id": "a", "type": "logs" },
        { "id": "b", "type": "logs" }
      ]
    }"#;
    load_str(json).unwrap_or_else(|e| panic!("two logs sources load: {e}"));
}

/// A bucket is what paces a metrics source's reader, so a zero one would spin it.
#[test]
fn a_metrics_source_with_a_zero_bucket_is_refused() {
    let json = r#"{"version":1,"sources":[{"id":"m","type":"metrics","bucketSeconds":0}]}"#;
    let err = load_str(json).unwrap_err().to_string();
    assert!(err.contains("\"m\""), "it names the source: {err}");
    assert!(err.contains("bucketSeconds"), "and the field: {err}");

    let json = r#"{"version":1,"sources":[{"id":"m","type":"metrics","bucketSeconds":1}]}"#;
    load_str(json).unwrap_or_else(|e| panic!("a one-second bucket loads: {e}"));
}

/// `tests/fixtures/bundle/` is the multi-file form of the same deployment.
#[test]
fn the_reference_bundle_example_loads_compiles_and_runs() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/bundle");
    let system = load_bundle_dir(&root).unwrap_or_else(|e| panic!("bundle load failed: {e}"));
    assert_eq!(system.pipelines.len(), 2);
    assert_eq!(system.topology.sources.len(), 2);
    assert_eq!(system.topology.destinations.len(), 2);
    assert_eq!(system.topology.routes.len(), 3);
    for route in &system.topology.routes {
        let to_out = route.deliver.iter().filter(|rule| rule.to == vec![0]).count();
        assert_eq!(to_out, 1, "every route delivers to `kafka-out`: {:?}", route.deliver);
    }
    let main = system
        .pipelines
        .iter()
        .find(|p| p.name == "Main pipeline")
        .expect("the bundle's \"main\" pipeline");
    let rows = run_pipeline(main, &[r#"{"status":200}"#, r#"{"status":500}"#]);
    assert_severity_rows(&rows);
}

/// A regex route condition, a grok parser, and a two-statement `for` loop
/// whose body reads a field the parser declared.
fn grok_and_for_loop_config_json() -> &'static str {
    r#"{
      "version": 1,
      "sources": [
        { "id": "src-main", "type": "kafka", "brokers": "b1:9092", "topics": ["logs"], "groupId": "smeltr" }
      ],
      "destinations": [
        { "id": "dst-main", "type": "kafka", "brokers": "b1:9092", "defaultTopic": "logs-out" }
      ],
      "pipelines": [
        {
          "id": "main",
          "name": "Main pipeline",
          "steps": [
            { "id": "parse", "type": "parser", "input": "_raw",
              "kind": { "type": "grok", "patterns": ["%{WORD:level} %{GREEDYDATA:msg}"] },
              "declared": [ { "name": "level", "type": "str" }, { "name": "msg", "type": "str" } ] },
            { "id": "retries", "type": "eval",
              "assignments": [ { "target": "retries", "expr": "[1, 2, 5]" } ] },
            { "id": "loop", "type": "code",
              "body": "let total = 0; let tag = \"\"; for (let r of retries) { total = total + r; tag = level; } totalDelay = total; taggedLevel = tag;" }
          ]
        }
      ],
      "routes": [
        { "id": "errors-only", "source": "src-main", "condition": "test(_raw, /^ERROR/)", "pipeline": "main",
          "deliver": [ { "to": ["dst-main"] } ] }
      ]
    }"#
}

#[test]
fn a_route_regex_condition_actually_matches_real_raw_bytes() {
    let system = load_str(grok_and_for_loop_config_json()).unwrap_or_else(|e| {
        panic!("load failed: {e}");
    });
    let topology = system
        .compile_topology()
        .unwrap_or_else(|e| panic!("topology: {}", join_errors(&e)));
    let source_id = system.topology.sources[0].id;

    let docs = ["ERROR database connection failed", "INFO server started"];
    let mut pool = BufferPool::new();
    let mut errors = ErrorSink::new();
    // A Kafka source's layout: the reserved prefix, then what it declares.
    let mut layout = raw_layout();
    layout.extend(
        smeltr::plan::topology::SourceMeta::KAFKA
            .source_fields()
            .iter()
            .map(|f| f.1.clone()),
    );
    let mut batch = Batch::new(&layout, docs.len(), 0);

    let mut live = Column::alloc(&mut pool, &DataType::Bool, docs.len());
    bits::fill_valid(live.bool_mut().unwrap().values, docs.len());
    batch.put(SLOT_LIVE, live, &mut pool);

    let mut offset = Column::alloc(&mut pool, &DataType::I64, docs.len());
    for (i, v) in offset.i64_mut().unwrap().values.iter_mut().enumerate() {
        *v = i as i64;
    }
    batch.put(SLOT_OFFSET, offset, &mut pool);

    let mut b = StrBuilder::new(&mut pool, VarType::Bytes, docs.len());
    b.reserve(&mut pool, docs.iter().map(|d| d.len()).sum());
    for d in &docs {
        b.push(d.as_bytes());
    }
    batch.put(SLOT_RAW, b.finish(), &mut pool);

    let mut ctx = Ctx::new(&mut pool, &mut errors);
    let masks = route_masks(&topology.routes, source_id, &mut batch, &mut ctx, None);
    assert_eq!(masks.len(), 1);
    let (_, m0) = &masks[0];
    assert!(
        bits::get_row(m0.bool().unwrap().values, 0),
        "the ERROR-prefixed line must match `test(_raw, /^ERROR/)`"
    );
    assert!(
        !bits::get_row(m0.bool().unwrap().values, 1),
        "the INFO-prefixed line must not match `test(_raw, /^ERROR/)`"
    );

    batch.reclaim(&mut pool);
    masks.into_iter().for_each(|(_, c)| c.reclaim(&mut pool));
}

#[test]
fn a_grok_step_and_a_for_loop_reading_an_outer_field_produce_correct_output() {
    let system = load_str(grok_and_for_loop_config_json()).unwrap_or_else(|e| {
        panic!("load failed: {e}");
    });
    let rows = run_pipeline(
        &system.pipelines[0],
        &["ERROR database connection failed", "INFO server started"],
    );
    assert_eq!(rows[0]["level"], "ERROR");
    assert_eq!(rows[0]["msg"], "database connection failed");
    assert_eq!(rows[0]["totalDelay"], 8, "1 + 2 + 5");
    assert_eq!(rows[0]["taggedLevel"], "ERROR", "the body reads the outer field");
    assert_eq!(rows[1]["level"], "INFO");
    assert_eq!(rows[1]["taggedLevel"], "INFO");
}

#[test]
fn unroll_keep_parent_fields_round_trips_through_the_config_file() {
    use smeltr::config_file::StepDto;
    let keep = |dto: &StepDto| match dto {
        StepDto::Unroll { keep_parent_fields, .. } => *keep_parent_fields,
        _ => panic!("expected Unroll"),
    };
    let omitted: StepDto = serde_json::from_str(r#"{"type":"unroll","id":"u1","field":"events"}"#).unwrap();
    assert!(!keep(&omitted));
    let explicit: StepDto =
        serde_json::from_str(r#"{"type":"unroll","id":"u1","field":"events","keepParentFields":true}"#).unwrap();
    assert!(keep(&explicit));
    let text = serde_json::to_string(&explicit).unwrap();
    assert!(text.contains("keepParentFields"), "got {text}");
    assert!(keep(&serde_json::from_str(&text).unwrap()));
}

/// The GUI's "array of objects" declared type is `Array(Struct)` here, by
/// agreement, in both directions.
#[test]
fn the_guis_array_of_objects_declared_type_loads_compiles_and_survives_a_save() {
    let json = r#"{
      "version": 1,
      "pipelines": [{
        "id": "p1", "name": "p1",
        "steps": [
          {"type":"parser","id":"s1","input":"_raw","kind":{"type":"json"},
           "declared":[{"name":"tags","type":{"type":"array","of":{"type":"struct",
             "fields":[{"name":"name","type":"str"},{"name":"n","type":"i64"}]}}}]},
          {"type":"eval","id":"s2","assignments":[{"target":"first","expr":"tags[0].name"}]}
        ]
      }]
    }"#;
    let system = load_str(json).expect("the GUI's shape must load");
    system
        .compile_topology()
        .unwrap_or_else(|e| panic!("must compile: {}", e[0].message));
}

/// A readable file somewhere on the host, as a JSON string literal ready to substitute into a config. It exists so
/// the refusal is about the path being absolute rather than about the file being absent, which would fail earlier and
/// say so.
fn a_readable_file_outside_every_root(tag: &str) -> (std::path::PathBuf, String) {
    let dir = std::env::temp_dir().join(format!("smeltr-cfg-{}-{tag}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let file = dir.join("secret");
    std::fs::write(&file, "root:x\n").unwrap();
    let literal = serde_json::Value::String(file.to_string_lossy().into_owned()).to_string();
    (file, literal)
}

/// An absolute lookup path would let anyone who can edit a pipeline read any file the process can, and a table's
/// file is committed beside the config that names it, so there is no honest use for one. Refused where a step's
/// table is compiled, which every save runs.
#[test]
fn an_absolute_lookup_table_path_is_refused_at_load() {
    let (file, path) = a_readable_file_outside_every_root("csv");
    let json = r#"{
      "version": 1,
      "lookupTables": [
        {"id":"t1","type":"csv","path":"@PATH@","delimiter":":"}
      ],
      "pipelines": [{
        "id":"p1","name":"p1",
        "steps":[{"type":"lookup","id":"s1","input":"_raw","table":"t1",
                  "matchKind":"exact","multiplicity":"first","keyColumn":"root",
                  "outputs":[{"source":"root","field":"shell"}]}]
      }]
    }"#
    .replace("\"@PATH@\"", &path);
    let err = refusal(&json);
    std::fs::remove_file(&file).ok();
    assert!(err.contains("is an absolute path"), "got: {err}");
}

#[test]
fn an_absolute_mmdb_lookup_path_is_refused_at_load() {
    let (file, path) = a_readable_file_outside_every_root("mmdb");
    let json = r#"{
      "version": 1,
      "lookupTables": [{"id":"t1","type":"mmdb","path":"@PATH@"}],
      "pipelines": [{
        "id":"p1","name":"p1",
        "steps":[{"type":"lookup","id":"s1","input":"_raw","table":"t1",
                  "matchKind":"cidr","multiplicity":"best",
                  "outputs":[{"source":"country","field":"c"}]}]
      }]
    }"#
    .replace("\"@PATH@\"", &path);
    let err = refusal(&json);
    std::fs::remove_file(&file).ok();
    assert!(err.contains("is an absolute path"), "got: {err}");
}

/// A bundle keeps its lookup files in `lookups/`, so a table naming a file anywhere else is refused -- at the root,
/// in `samples/`, or reached out of `lookups/` by `..`.
#[test]
fn a_lookup_file_outside_lookups_is_refused_at_load() {
    let json = |path: &str| {
        r#"{
          "version": 1,
          "lookupTables": [
            {"id":"t1","type":"csv","path":"@PATH@","delimiter":","}
          ],
          "pipelines": [{
            "id":"p1","name":"p1",
            "steps":[{"type":"lookup","id":"s1","input":"_raw","table":"t1",
                      "matchKind":"exact","multiplicity":"first","keyColumn":"name",
                      "outputs":[{"source":"shell","field":"shell"}]}]
          }]
        }"#
        .replace("@PATH@", path)
    };
    for path in [
        "users.csv",
        "samples/users.csv",
        "lookups/../samples/users.csv",
        "./lookups/users.csv",
        "lookups",
    ] {
        let err = refusal(&json(path));
        assert!(err.contains("is not in `lookups/`"), "{path}: {err}");
    }
    // In the folder, the path is fine; what stops this one is that a string came from no bundle to find it in.
    for path in ["lookups/users.csv", "lookups/geo/cities.csv"] {
        let err = refusal(&json(path));
        assert!(!err.is_empty(), "{path}: no bundle to resolve against");
        assert!(!err.contains("is not in `lookups/`"), "{path}: {err}");
    }
}

/// A configuration parsed from a string came from no bundle, so a relative lookup path has nothing to resolve
/// against. The message says that rather than reporting a file it looked for in the process's working directory.
#[test]
fn a_relative_lookup_path_with_no_bundle_says_so() {
    let json = r#"{
      "version": 1,
      "lookupTables": [{"id":"t1","type":"csv","path":"lookups/geo.csv","delimiter":","}],
      "pipelines": [{
        "id":"p1","name":"p1",
        "steps":[{"type":"lookup","id":"s1","input":"_raw","table":"t1",
                  "matchKind":"exact","multiplicity":"first","keyColumn":"root",
                  "outputs":[{"source":"root","field":"shell"}]}]
      }]
    }"#;
    let err = refusal(json);
    assert!(err.contains("came from no bundle"), "got: {err}");
}

const ROUTE: &str = r#"{"id":"r","source":"s","pipeline":"p"}"#;

fn route_with(extra: &str) -> String {
    format!(r#"{{"id":"r","source":"s","pipeline":"p"{extra}}}"#)
}

/// `json` with `exceptions` laid at the group, the top of the file.
fn with_group(json: &str, exceptions: &str) -> String {
    json.replacen('{', &format!(r#"{{"exceptions":{exceptions},"#), 1)
}

/// A route's `exceptions` loads under each answer, with its tag field; a route that names none gets the defaults,
/// and a pipeline carries neither.
#[test]
fn a_routes_failure_setting_loads() {
    for (json, answer) in [
        ("", Answer::Tag),
        (r#","exceptions":{"categories":{"math":"ignore"}}"#, Answer::Ignore),
        (r#","exceptions":{"categories":{"math":"block"}}"#, Answer::Block),
        (r#","exceptions":{"categories":{"math":"drop"}}"#, Answer::Drop),
    ] {
        let system =
            load_str(&cfg(KAFKA_OUT, EMPTY_PIPELINE, &route_with(json))).unwrap_or_else(|e| panic!("{json}: {e}"));
        let on_exception = &system.topology.routes[0].on_exception;
        assert_eq!(on_exception.answer(&ExceptionKind::Overflow), &answer, "{json}");
    }

    let named = route_with(r#","exceptions":{"tagField":"detail.why"}"#);
    let system = load_str(&cfg(KAFKA_OUT, EMPTY_PIPELINE, &named)).expect("must load");
    assert_eq!(system.topology.routes[0].on_exception.tag_field, "detail.why");
    let group = with_group(&cfg(KAFKA_OUT, EMPTY_PIPELINE, ROUTE), r#"{"tagField":"err"}"#);
    let system = load_str(&group).expect("must load");
    assert_eq!(system.topology.routes[0].on_exception.tag_field, "err", "the group's");
    let both = with_group(&cfg(KAFKA_OUT, EMPTY_PIPELINE, &named), r#"{"tagField":"err"}"#);
    let system = load_str(&both).expect("must load");
    assert_eq!(
        system.topology.routes[0].on_exception.tag_field, "detail.why",
        "the route's over the group's"
    );

    let plain = load_str(&cfg(KAFKA_OUT, EMPTY_PIPELINE, ROUTE)).expect("must load");
    assert_eq!(plain.topology.routes[0].on_exception.tag_field, "smeltr.exception");
    let written = serde_json::to_string(&smeltr::config_file::PipelineDto {
        id: "p".into(),
        name: "p".into(),
        steps: Vec::new(),
        produces: vec![],
    })
    .unwrap();
    assert_eq!(written, r#"{"id":"p","name":"p","steps":[]}"#);
}

/// A route's `exceptions` lays over the group's, which lays over the built-in answers: encoding is ignored, every
/// other category tagged. In each layer a kind beats its category.
#[test]
fn a_routes_exceptions_lay_over_the_groups() {
    use ExceptionKind::*;
    use smeltr::plan::config::Answer::*;
    let group = r#"{"categories":{"math":"drop","encoding":"tag"},"kinds":{"NaN":"ignore"}}"#;
    let both = |route: &str| {
        let json = with_group(&cfg(KAFKA_OUT, EMPTY_PIPELINE, &route_with(route)), group);
        load_str(&json).unwrap_or_else(|e| panic!("{route}: {e}"))
    };
    let kinds = [Overflow, NaN, InvalidUtf8, DivideByZero];
    let answers = |system: &smeltr::config_file::LoadedSystem| -> Vec<Answer> {
        kinds
            .iter()
            .map(|k: &ExceptionKind| system.topology.routes[0].on_exception.answer(k).clone())
            .collect()
    };
    let plain = load_str(&cfg(KAFKA_OUT, EMPTY_PIPELINE, ROUTE)).unwrap();
    assert_eq!(answers(&plain), [Tag, Tag, Ignore, Tag], "built in");
    assert_eq!(answers(&both("")), [Drop, Ignore, Tag, Drop], "the group's");
    let each = r#","exceptions":{"categories":{"math":"block"},"kinds":{"Overflow":"tag"}}"#;
    assert_eq!(
        answers(&both(each)),
        [Tag, Block, Tag, Block],
        "a category, then a kind"
    );

    for (layer, want) in [
        (r#"{"kinds":{"Nan":"drop"}}"#, r#"names "Nan", which is no kind"#),
        (
            r#"{"categories":{"Math":"drop"}}"#,
            r#"names "Math", which is no category"#,
        ),
    ] {
        let err = load_str(&cfg(
            KAFKA_OUT,
            EMPTY_PIPELINE,
            &route_with(&format!(r#","exceptions":{layer}"#)),
        ))
        .expect_err(layer);
        assert!(err.to_string().contains(want), "{err}");
    }
}

/// `default` and a bare answer are no layer: every category is answered by name.
#[test]
fn an_answer_to_every_kind_at_once_is_refused() {
    for layer in [r#""drop""#, r#"{"default":"drop"}"#] {
        load_str(&with_group(&cfg(KAFKA_OUT, EMPTY_PIPELINE, ROUTE), layer)).expect_err(layer);
    }
}

/// A route answers what happens to its pipeline's rows, Unmatched among them for the rows its rules decline.
/// Unprocessable never reaches one, and Refused is a destination's. A destination answers Refused alone.
#[test]
fn a_route_and_a_destination_answer_only_their_own_kinds() {
    let on_route = |layer: &str| {
        cfg(
            KAFKA_OUT,
            EMPTY_PIPELINE,
            &route_with(&format!(r#","exceptions":{layer}"#)),
        )
    };
    for layer in [
        r#"{"kinds":{"Unprocessable":"drop"}}"#,
        r#"{"kinds":{"Refused":"drop"}}"#,
    ] {
        let err = load_str(&on_route(layer)).expect_err(layer).to_string();
        assert!(err.contains("a route does not answer"), "{layer}: {err}");
    }
    // A route answers Unmatched for the rows its rules decline, so the delivery category reaches that kind.
    for layer in [
        r#"{"kinds":{"Unmatched":"drop"}}"#,
        r#"{"categories":{"delivery":"drop"}}"#,
    ] {
        load_str(&on_route(layer)).expect(layer);
    }
    for layer in [
        r#"{"kinds":{"Unmatched":"drop"}}"#,
        r#"{"kinds":{"Overflow":"drop"}}"#,
        r#"{"categories":{"math":"drop"}}"#,
    ] {
        let dest = format!(r#"{{"id":"d","type":"kafka","brokers":"b:9092","defaultTopic":"o","exceptions":{layer}}}"#);
        let err = load_str(&cfg(&dest, EMPTY_PIPELINE, ROUTE))
            .expect_err(layer)
            .to_string();
        assert!(err.contains("a destination answers only Refused"), "{layer}: {err}");
    }
    let dest = r#"{"id":"d","type":"kafka","brokers":"b:9092","defaultTopic":"o","exceptions":{"tagField":"t"}}"#;
    let err = load_str(&cfg(dest, EMPTY_PIPELINE, ROUTE)).expect_err("a destination's tag field");
    assert!(err.to_string().contains("a destination tags no row"), "{err}");
}

/// The tag field is a field a row can carry: not empty, not a control field the serializer hides.
#[test]
fn a_tag_field_must_be_a_writable_name() {
    for name in ["", "__failed", "a..b", "a."] {
        let json = route_with(&format!(r#","exceptions":{{"tagField":"{name}"}}"#));
        let err = load_str(&cfg(KAFKA_OUT, EMPTY_PIPELINE, &json)).expect_err("must be rejected");
        assert!(err.to_string().contains("not a field name"), "{name:?}: {err}");
    }
}

/// A pipeline may bind the name its route tags under: the tag is written over whatever the row holds there.
#[test]
fn a_pipeline_binding_its_tag_name_compiles() {
    let json = cfg(
        KAFKA_OUT,
        r#"{"id":"p","name":"p","steps":[
            {"id":"e","type":"eval","assignments":[{"target":"smeltr.exception","expr":"\"no\""}]}]}"#,
        ROUTE,
    );
    let system = load_str(&json).expect("must load");
    system
        .compile_topology()
        .unwrap_or_else(|e| panic!("topology: {}", join_errors(&e)));
}

/// `"exceptions"` answering Refused with `answer`.
fn refused(answer: &str) -> String {
    format!(r#""exceptions":{{"kinds":{{"Refused":{answer}}}}}"#)
}

/// A destination delivers what it refuses to other destinations that exist: at least one, none twice, never itself.
/// Refusals chain, and a chain that leads back to where it started is refused.
#[test]
fn a_destination_delivers_its_refusals_to_other_destinations() {
    let dests = |to: &str, q_answer: &str| {
        let (d, q) = (refused(&format!(r#"{{"deliver":{to}}}"#)), refused(q_answer));
        format!(
            r#"{{"id":"d","type":"kafka","brokers":"b:9092","defaultTopic":"o",{d}}},
               {{"id":"q","type":"kafka","brokers":"b:9092","defaultTopic":"q",{q}}},
               {{"id":"r","type":"kafka","brokers":"b:9092","defaultTopic":"r"}}"#
        )
    };
    let chained = load_str(&cfg(&dests(r#"["q"]"#, r#"{"deliver":["r"]}"#), EMPTY_PIPELINE, ROUTE));
    chained.expect("a chain that ends loads");
    let system = load_str(&cfg(&dests(r#"["q","r"]"#, r#""drop""#), EMPTY_PIPELINE, ROUTE)).expect("must load");
    let (q, r) = (system.topology.destinations[1].id, system.topology.destinations[2].id);
    assert_eq!(system.topology.destinations[0].on_reject, Answer::Deliver(vec![q, r]));
    assert_eq!(system.topology.destinations[1].on_reject, Answer::Drop);

    for (to, q_answer, says) in [
        (r#"["d"]"#, r#""drop""#, "delivers what it refuses to itself"),
        (r#"["nope"]"#, r#""drop""#, "unknown destination \"nope\""),
        (r#"[]"#, r#""drop""#, "delivers to no destination"),
        (r#"["q","q"]"#, r#""drop""#, "names destination \"q\" twice"),
        (r#"["q"]"#, r#"{"deliver":["d"]}"#, "lead back to it (d → q → d)"),
    ] {
        let err = load_str(&cfg(&dests(to, q_answer), EMPTY_PIPELINE, ROUTE)).expect_err(says);
        assert!(err.to_string().contains(says), "{says}: {err}");
    }
}

/// The group's answer to Refused is every destination's that gives none of its own. A group that delivers refusals
/// to a destination has that destination deliver its own to itself, unless it answers Refused for itself.
#[test]
fn the_groups_refused_answer_lays_under_each_destinations() {
    let dests = |alerts: &str| {
        format!(
            r#"{{"id":"main","type":"kafka","brokers":"b:9092","defaultTopic":"o"}},
               {{"id":"alerts","type":"kafka","brokers":"b:9092","defaultTopic":"a"{alerts}}}"#
        )
    };
    let json = with_group(
        &cfg(
            &dests(r#","exceptions":{"kinds":{"Refused":"block"}}"#),
            EMPTY_PIPELINE,
            ROUTE,
        ),
        r#"{"kinds":{"Refused":"drop"}}"#,
    );
    let system = load_str(&json).unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(system.topology.destinations[0].on_reject, Answer::Drop);
    assert_eq!(system.topology.destinations[1].on_reject, Answer::Block);

    let group = r#"{"kinds":{"Refused":{"deliver":["alerts"]}}}"#;
    let err = load_str(&with_group(&cfg(&dests(""), EMPTY_PIPELINE, ROUTE), group))
        .expect_err("alerts would deliver to itself")
        .to_string();
    assert!(
        err.contains(r#"destination "alerts" delivers what it refuses to itself"#),
        "{err}"
    );
    let own = load_str(&with_group(
        &cfg(
            &dests(r#","exceptions":{"kinds":{"Refused":"drop"}}"#),
            EMPTY_PIPELINE,
            ROUTE,
        ),
        group,
    ))
    .unwrap_or_else(|e| panic!("{e}"));
    let alerts = own.topology.destinations[1].id;
    assert_eq!(own.topology.destinations[0].on_reject, Answer::Deliver(vec![alerts]));
    assert_eq!(own.topology.destinations[1].on_reject, Answer::Drop);
}

/// An answer that needs what its kind's row does not have is refused, naming what is missing: an Unmatched row has
/// no route to go on down or tag under, an Unprocessable row no record to go on with, a Refused
/// row no destination to go on to.
#[test]
fn an_answer_a_delivery_kind_cannot_carry_out_is_refused() {
    let base = cfg(TWO_DESTS, EMPTY_PIPELINE, ROUTE);
    for (kind, answer, says) in [
        (
            "Unmatched",
            r#""ignore""#,
            "the group answers Unmatched with ignore, but an Unmatched row has no route",
        ),
        (
            "Unmatched",
            r#""tag""#,
            "the group answers Unmatched with tag, but an Unmatched row has no route",
        ),
        (
            "Unprocessable",
            r#""ignore""#,
            "answers Unprocessable with ignore, but an Unprocessable row has no record",
        ),
        (
            "Unprocessable",
            r#""tag""#,
            "answers Unprocessable with tag, but an Unprocessable row has no record",
        ),
        (
            "Refused",
            r#""ignore""#,
            "answers Refused with ignore, but a Refused row has no destination",
        ),
        (
            "Refused",
            r#""tag""#,
            "answers Refused with tag, but a Refused row has no destination",
        ),
    ] {
        let json = with_group(&base, &format!(r#"{{"kinds":{{"{kind}":{answer}}}}}"#));
        let err = load_str(&json).expect_err(says).to_string();
        assert!(err.contains(says), "{says}: {err}");
    }

    let own = cfg(
        &format!(
            r#"{{"id":"main","type":"kafka","brokers":"b:9092","defaultTopic":"o",{}}}"#,
            refused(r#""tag""#)
        ),
        EMPTY_PIPELINE,
        ROUTE,
    );
    let err = load_str(&own).expect_err("a destination tagging").to_string();
    assert!(
        err.contains(r#"destination "main" answers Refused with tag, but a Refused row has no destination"#),
        "{err}"
    );

    // A route is named as the Routes page names it, not by its minted id.
    let err = load_str(&cfg_with_route(r#","exceptions":{"kinds":{"Unmatched":"tag"}}"#))
        .expect_err("a route tagging")
        .to_string();
    assert!(
        err.contains("route s \u{2192} p answers Unmatched with tag, but an Unmatched row has no route"),
        "{err}"
    );

    // A category's answer is each of its kinds'.
    let err = load_str(&with_group(&base, r#"{"categories":{"delivery":"tag"}}"#))
        .expect_err("tags the delivery kinds")
        .to_string();
    for kind in ["Unmatched", "Unprocessable", "Refused"] {
        assert!(err.contains(&format!("answers {kind} with tag")), "{kind}: {err}");
    }
    load_str(&with_group(&base, r#"{"categories":{"delivery":"drop"}}"#)).expect("drop answers every kind");
}

const TWO_DESTS: &str = r#"{"id":"main","type":"kafka","brokers":"b:9092","defaultTopic":"o"},
        {"id":"alerts","type":"kafka","brokers":"b:9092","defaultTopic":"a"}"#;

fn cfg_with_route(route_extra: &str) -> String {
    cfg(
        TWO_DESTS,
        EMPTY_PIPELINE,
        &format!(r#"{{"id":"r","source":"s","pipeline":"p"{route_extra}}}"#),
    )
}

/// `deliver` resolves its destinations and round-trips; a route without
/// one does not write the key.
#[test]
fn a_routes_deliver_block_loads_and_round_trips() {
    let json = cfg_with_route(r#","deliver":[{"to":["alerts","main"]}]"#);
    let system = load_str(&json).expect("must load");
    let rules = &system.topology.routes[0].deliver;
    assert_eq!(rules.len(), 1);
    assert_eq!(rules[0].to.len(), 2);
    assert!(rules[0].when.is_none());

    let system = load_str(&cfg_with_route("")).unwrap();
    assert!(system.topology.routes[0].deliver.is_empty());
}

/// A `when` compiles against the pipeline's output: a computed field
/// resolves, an unknown one is a compile error, and so is a non-Bool. A
/// variant takes its type with `as`, since a rule binds nothing.
#[test]
fn a_deliver_condition_is_compiled_against_the_pipelines_output() {
    let alerts = r#"{"id":"alerts","type":"kafka","brokers":"b:9092","defaultTopic":"a"}"#;
    let sets_severity = r#"{"id":"p","name":"p","steps":[
        {"type":"eval","id":"e","assignments":[{"target":"severity","expr":"4"}]}]}"#;
    let route = |when: &str| {
        format!(r#"{{"id":"r","source":"s","pipeline":"p","deliver":[{{"when":"{when}","to":["alerts"]}}]}}"#)
    };
    let compile = |pipelines: &str, when: &str| {
        load_str(&cfg(alerts, pipelines, &route(when)))
            .expect("must load")
            .compile_topology()
    };
    compile(sets_severity, "severity > 3").expect("a computed field resolves");
    let Err(errs) = compile(EMPTY_PIPELINE, "nosuch > 3") else {
        panic!("an unknown field must not compile")
    };
    assert_eq!((errs[0].route, errs[0].rule), (Some(0), Some(0)), "{}", errs[0].message);
    let Err(errs) = compile(sets_severity, "severity") else {
        panic!("a non-Bool must not compile")
    };
    assert!(errs[0].message.contains("must be bool"), "{}", errs[0].message);
    let parses = r#"{"id":"p","name":"p","steps":[
        {"type":"code","id":"c","body":"Object.assign($, json(_raw));"}]}"#;
    let Err(errs) = compile(parses, "level > 3") else {
        panic!("a variant must not compile")
    };
    assert!(
        errs[0].message.contains("`level as type`"),
        "a rule binds nothing: {}",
        errs[0].message
    );
}

/// A credential sent without TLS is the admin's choice; the form warns, load does not refuse.
#[test]
fn a_credential_without_tls_loads() {
    let json = r#"{
      "version": 1,
      "sources": [{"id":"s","type":"kafka","brokers":"kafka.internal:9092","topics":["t"],"groupId":"g",
        "properties": {"security.protocol": "SASL_PLAINTEXT", "sasl.mechanism": "PLAIN", "sasl.username": "u"},
        "credentials": {"sasl.password": {"secret": "k", "version": 1}}}],
      "destinations": [
        {"id":"es","type":"elasticsearch","url":"http://es.internal:9200",
         "defaultIndex":"logs","auth":{"secret":"es-auth","version":2}}
      ],
      "pipelines": [{"id":"p","name":"p","steps":[]}],
      "routes": [{"id":"r","source":"s","pipeline":"p","deliver":[{"to":["es"]}]}]
    }"#;
    assert!(refusal(json).is_empty(), "{}", refusal(json));
}

/// The bundle names every connector's credentials, sources before destinations, each by the field naming it.
#[test]
fn a_bundle_names_its_sources_credentials_then_its_destinations() {
    let json = r#"{
      "version": 1,
      "sources": [{"id":"s","type":"kafka","brokers":"kafka.internal:9092","topics":["t"],"groupId":"g",
        "properties": {"security.protocol": "SASL_SSL", "sasl.mechanism": "PLAIN", "sasl.username": "u"},
        "credentials": {"sasl.password": {"secret": "k", "version": 1}}}],
      "destinations": [
        {"id":"es","type":"elasticsearch","url":"https://es.internal:9200",
         "defaultIndex":"logs","auth":{"secret":"es-auth","version":2}}
      ],
      "pipelines": [{"id":"p","name":"p","steps":[]}],
      "routes": [{"id":"r","source":"s","pipeline":"p","deliver":[{"to":["es"]}]}]
    }"#;
    let system = load_str(json).expect("must load");
    let named: Vec<(&str, &smeltr::plan::topology::SecretRef)> = (system.topology.named_secrets.iter())
        .map(|n| (n.what.as_str(), &n.reference))
        .collect();
    let stored = |name: &str, version| smeltr::plan::topology::SecretRef::LeaderStored {
        name: name.into(),
        version,
    };
    assert_eq!(
        named,
        [
            (r#"source "s": credentials"#, &stored("k", 1)),
            (r#"destination "es": auth"#, &stored("es-auth", 2)),
        ]
    );
}

/// Every field naming an address over HTTP takes a whole http:// or https:// URL, and is refused once otherwise.
#[test]
fn every_url_field_is_a_whole_http_or_https_url() {
    let destination = |d: &str| format!(r#"{{"version":1,"destinations":[{d}]}}"#);
    for (json, field) in [
        (
            destination(r#"{"id":"d","type":"elasticsearch","url":"es.internal:9200","defaultIndex":"i"}"#),
            "url",
        ),
        (
            destination(r#"{"id":"d","type":"splunkHec","url":"splunk.internal:8088","token":"T"}"#),
            "url",
        ),
        (http_config("intake.example", ""), "url"),
        (s3_config(r#","endpoint":"minio:9000""#), "endpoint"),
        (
            s3_config(r#","roleArn":"arn:aws:iam::1:role/w","authEndpoint":"sts.internal""#),
            "authEndpoint",
        ),
        (gcs_config(r#","endpoint":"gcs.internal""#), "endpoint"),
        (http_api_config("api.example/events", ""), "url"),
        (okta_config("acme.okta.com", ""), "org"),
    ] {
        let e = refusal(&json);
        assert_eq!(
            e.matches("give a whole http:// or https:// URL").count(),
            1,
            "{field}: {e}"
        );
        assert!(e.contains(&format!("has {field} ")), "{field}: {e}");
    }
}

/// The credential is named, never valued, and the name survives a save.
#[test]
fn an_elasticsearch_destination_round_trips_with_its_credential_by_reference() {
    let json = r#"{
      "version": 1,
      "sources": [{"id":"s","type":"kafka","brokers":"b:9092","topics":["t"],"groupId":"g"}],
      "destinations": [
        {"id":"es","type":"elasticsearch","url":"https://es.internal:9200",
         "defaultIndex":"logs","auth":"SMELTR_ES_AUTH"},
        {"id":"plain","type":"elasticsearch","url":"http://localhost:9200",
         "defaultIndex":"dev"}
      ],
      "pipelines": [{"id":"p","name":"p","steps":[]}],
      "routes": [{"id":"r","source":"s","pipeline":"p",
        "deliver":[{"to":["es"]}]}]
    }"#;
    let system = load_str(json).expect("must load");
    system.compile_topology().expect("must compile");
    assert_eq!(
        elastic(&system, 0).auth,
        Some(smeltr::plan::topology::SecretRef::WorkerLocal("SMELTR_ES_AUTH".into()))
    );
    assert_eq!(elastic(&system, 1).auth, None);
}

/// The two shapes a credential can take, told apart by whether it is a bare
/// string. Both name a place; neither carries a value.
#[test]
fn a_credential_names_either_a_variable_on_the_host_or_a_secret_the_leader_holds() {
    use smeltr::plan::topology::SecretRef;

    let json = r#"{
      "version": 1,
      "sources": [{"id":"s","type":"kafka","brokers":"b:9092","topics":["t"],"groupId":"g",
        "properties": {"security.protocol": "SASL_SSL"}, "credentials": {"sasl.password": {"secret": "kafka-west-sasl", "version": 7}}}],
      "destinations": [
        {"id":"es","type":"elasticsearch","url":"https://es.internal:9200",
         "defaultIndex":"logs","auth":{"secret":"es-auth","version":2}}
      ],
      "pipelines": [{"id":"p","name":"p","steps":[]}],
      "routes": [{"id":"r","source":"s","pipeline":"p",
        "deliver":[{"to":["es"]}]}]
    }"#;
    let system = load_str(json).expect("must load");
    system.compile_topology().expect("must compile");

    let stored = |name: &str, version: i64| SecretRef::LeaderStored {
        name: name.into(),
        version,
    };
    assert_eq!(elastic(&system, 0).auth, Some(stored("es-auth", 2)));
    match &system.topology.sources[0].kind {
        smeltr::plan::topology::SourceKind::Kafka(k) => assert_eq!(
            k.props.credentials,
            vec![("sasl.password".to_string(), stored("kafka-west-sasl", 7))]
        ),
        other => panic!("expected a Kafka source, got {other:?}"),
    }
}

/// A version is what makes rotating a credential a content change, so a
/// reference that leaves it out is refused rather than defaulted.
#[test]
fn a_leader_stored_credential_without_a_version_is_refused() {
    let json = r#"{
      "version": 1,
      "sources": [{"id":"s","type":"kafka","brokers":"b:9092","topics":["t"],"groupId":"g"}],
      "destinations": [
        {"id":"es","type":"elasticsearch","url":"https://es.internal:9200",
         "defaultIndex":"logs","auth":{"secret":"es-auth"}}
      ],
      "pipelines": [{"id":"p","name":"p","steps":[]}],
      "routes": [{"id":"r","source":"s","pipeline":"p",
        "deliver":[{"to":["es"]}]}]
    }"#;
    assert!(load_str(json).is_err());
}

fn elastic(system: &smeltr::config_file::LoadedSystem, i: usize) -> &smeltr::plan::topology::ElasticDestinationConfig {
    match &system.topology.destinations[i].kind {
        smeltr::plan::topology::DestinationKind::Elasticsearch(e) => e,
        other => panic!("expected an Elasticsearch destination, got {other:?}"),
    }
}

/// `docId` round-trips and the default is not written.
#[test]
fn a_document_id_policy_round_trips_and_the_default_stays_absent() {
    let json = r#"{
      "version": 1,
      "sources": [{"id":"s","type":"kafka","brokers":"b:9092","topics":["t"],"groupId":"g"}],
      "destinations": [
        {"id":"dedupe","type":"elasticsearch","url":"http://es:9200",
         "defaultIndex":"logs","docId":"dedupe"},
        {"id":"overwrite","type":"elasticsearch","url":"http://es:9200",
         "defaultIndex":"logs","docId":"overwrite"},
        {"id":"plain","type":"elasticsearch","url":"http://es:9200","defaultIndex":"logs"}
      ],
      "pipelines": [{"id":"p","name":"p","steps":[
        {"id":"e","type":"eval","assignments":[{"target":"__destination.id","expr":"_raw"}]}
      ]}],
      "routes": [{"id":"r","source":"s","pipeline":"p",
        "deliver":[{"to":["dedupe","overwrite","plain"]}]}]
    }"#;
    let system = load_str(json).expect("must load");
    system.compile_topology().expect("must compile");
    assert_eq!(elastic(&system, 0).doc_id, DocIdPolicy::Dedupe);
    assert_eq!(elastic(&system, 1).doc_id, DocIdPolicy::Overwrite);
    assert_eq!(elastic(&system, 2).doc_id, DocIdPolicy::Auto);

    let reloaded = load_str(json).expect("re-load");
    assert_eq!(elastic(&reloaded, 0).doc_id, DocIdPolicy::Dedupe);
}

/// A `docId` destination with no `__destination.id` upstream looks like it dedupes
/// and does not.
#[test]
fn a_doc_id_policy_needs_a_pipeline_that_sets_id_out() {
    let json = |steps: &str| {
        format!(
            r#"{{
              "version": 1,
              "sources": [{{"id":"s","type":"kafka","brokers":"b:9092","topics":["t"],"groupId":"g"}}],
              "destinations": [
                {{"id":"es","type":"elasticsearch","url":"http://es:9200",
                 "defaultIndex":"logs","docId":"dedupe"}}
              ],
              "pipelines": [{{"id":"p","name":"p","steps":[{steps}]}}],
              "routes": [{{"id":"r","source":"s","pipeline":"p",
                "deliver":[{{"to":["es"]}}]}}]
            }}"#
        )
    };

    let system = load_str(&json("")).expect("loads -- this is structural");
    let Err(errs) = system.compile_topology() else {
        panic!("a dedupe destination with no `__destination.id` upstream must not compile");
    };
    assert!(errs.iter().any(|e| e.message.contains("__destination.id")), "{errs:?}");

    let ok = load_str(&json(
        r#"{"id":"e","type":"eval","assignments":[{"target":"__destination.id","expr":"_raw"}]}"#,
    ))
    .expect("must load");
    ok.compile_topology().expect("must compile");
}

/// A destination naming its own `idField` needs nothing from the
/// pipeline, and the field survives a save.
#[test]
fn a_destination_that_names_its_own_id_field_needs_nothing_from_the_pipeline() {
    let json = r#"{
      "version": 1,
      "sources": [{"id":"s","type":"kafka","brokers":"b:9092","topics":["t"],"groupId":"g"}],
      "destinations": [
        {"id":"es","type":"elasticsearch","url":"http://es:9200",
         "defaultIndex":"logs","docId":"dedupe","idField":"event.id"}
      ],
      "pipelines": [{"id":"p","name":"p","steps":[]}],
      "routes": [{"id":"r","source":"s","pipeline":"p",
        "deliver":[{"to":["es"]}]}]
    }"#;
    let system = load_str(json).expect("must load");
    system.compile_topology().expect("must compile");
    assert_eq!(elastic(&system, 0).id_field.as_deref(), Some("event.id"));
    load_str(json)
        .expect("re-load")
        .compile_topology()
        .expect("still compiles after a round trip");

    let plain = json.replace(r#","docId":"dedupe","idField":"event.id""#, "");
    let plain = load_str(&plain).expect("loads without a docId policy");
    assert_eq!(elastic(&plain, 0).id_field, None);
    assert_eq!(elastic(&plain, 0).doc_id, DocIdPolicy::Auto);
}

/// A destination that a delivered destination's refusals go to takes the same document, so it is held to the same
/// requirement.
#[test]
fn where_refusals_go_is_checked_too() {
    let json = r#"{
      "version": 1,
      "sources": [{"id":"s","type":"kafka","brokers":"b:9092","topics":["t"],"groupId":"g"}],
      "destinations": [
        {"id":"main","type":"kafka","brokers":"b:9092","defaultTopic":"out",
         "exceptions":{"kinds":{"Refused":{"deliver":["review"]}}}},
        {"id":"review","type":"elasticsearch","url":"http://es:9200",
         "defaultIndex":"failed","docId":"dedupe"}
      ],
      "pipelines": [{"id":"p","name":"p","steps":[]}],
      "routes": [{"id":"r","source":"s","pipeline":"p",
        "deliver":[{"to":["main"]}]}]
    }"#;
    let system = load_str(json).expect("must load");
    let Err(errs) = system.compile_topology() else {
        panic!("review keys on `_id` and no pipeline sets `__destination.id`");
    };
    assert!(errs.iter().any(|e| e.message.contains("__destination.id")), "{errs:?}");
}

/// The check is against the tree's leaves, so a `__destination.id` bound inside a
/// `Code` block counts.
#[test]
fn an_id_out_set_inside_a_code_block_satisfies_the_check() {
    let json = r#"{
      "version": 1,
      "sources": [{"id":"s","type":"kafka","brokers":"b:9092","topics":["t"],"groupId":"g"}],
      "destinations": [
        {"id":"es","type":"elasticsearch","url":"http://es:9200",
         "defaultIndex":"logs","docId":"overwrite"}
      ],
      "pipelines": [{"id":"p","name":"p","steps":[
        {"id":"c","type":"code","body":"__destination.id = _raw;"}
      ]}],
      "routes": [{"id":"r","source":"s","pipeline":"p",
        "deliver":[{"to":["es"]}]}]
    }"#;
    let system = load_str(json).expect("must load");
    system.compile_topology().expect("must compile");
}

/// An unset credential variable is refused at construction, not a silent
/// unauthenticated connection.
#[test]
fn a_missing_credential_environment_variable_is_refused_at_construction() {
    let cfg =
        smeltr::plan::topology::DestinationKind::Elasticsearch(smeltr::plan::topology::ElasticDestinationConfig {
            url: "http://localhost:9200".into(),
            zone_urls: Default::default(),
            sniff: None,
            default_index: "logs".into(),
            tls: None,
            auth: Some(smeltr::plan::topology::SecretRef::WorkerLocal(
                "SMELTR_ES_AUTH_DEFINITELY_NOT_SET".into(),
            )),
            basic: None,
            doc_id: Default::default(),
            id_field: None,
            in_flight: 4,
            max_bulk_bytes: 5 * 1024 * 1024,
            buffer_bytes: 64 * 1024 * 1024,
            stall_after: std::time::Duration::from_secs(30),
        });
    let metrics = smeltr::metrics::MetricsHandle::new(std::sync::Arc::new(smeltr::metrics::Metrics::new()), "test");
    let Err(e) = smeltr::io::build_destination(&cfg, 0, "d", metrics, &Default::default(), None) else {
        panic!("an unset credential variable must not construct one");
    };
    assert!(e.to_string().contains("SMELTR_ES_AUTH_DEFINITELY_NOT_SET"), "{e}");
}

/// A bundle may name as many destinations as it likes, and a route delivers a row to every one it names: a row's
/// delivery is a list, not a word of bits.
#[test]
fn a_route_delivers_to_every_one_of_many_destinations() {
    let n = 300;
    let dests = (0..n)
        .map(|i| format!(r#"{{"id":"d{i}","type":"kafka","brokers":"b:9092","defaultTopic":"t{i}"}}"#))
        .collect::<Vec<_>>()
        .join(",");
    let to = (0..n).map(|i| format!(r#""d{i}""#)).collect::<Vec<_>>().join(",");
    let route = format!(r#"{{"id":"r","source":"s","pipeline":"p","deliver":[{{"to":[{to}]}}]}}"#);
    let json = cfg(&dests, EMPTY_PIPELINE, &route);
    let system = load_str(&json).expect("three hundred destinations load");
    assert_eq!(system.topology.destinations.len(), n);
    let compiled = system
        .compile_topology()
        .unwrap_or_else(|e| panic!("compile: {}", join_errors(&e)));
    let source = &system.topology.sources[0];

    let records = sample_records(&[("t", "{}")]);
    let rows = smeltr::runtime::preview_routes(&compiled, source, &records, budget());
    let mut every: Vec<u32> = system.topology.destinations.iter().map(|d| d.id).collect();
    every.sort_unstable();
    assert_eq!(rows[0].through[0].documents[0].destinations, every);
}

/// The group's answers to Unmatched and Unprocessable reach the compiled topology and the worker's reachable set,
/// and must name real destinations, each once.
#[test]
fn a_delivery_answer_round_trips_and_must_name_real_destinations() {
    for kind in [ExceptionKind::Unmatched, ExceptionKind::Unprocessable] {
        let label = format!("{kind:?}");
        let json = format!(
            r#"{{
              "version": 1,
              "exceptions": {{"kinds": {{"{label}": {{"deliver": ["leftovers", "spare"]}}}}}},
              "sources": [{{"id":"s","type":"kafka","brokers":"b:9092","topics":["t"],"groupId":"g"}}],
              "destinations": [
                {{"id":"main","type":"kafka","brokers":"b:9092","defaultTopic":"o"}},
                {{"id":"leftovers","type":"kafka","brokers":"b:9092","defaultTopic":"l"}},
                {{"id":"spare","type":"kafka","brokers":"b:9092","defaultTopic":"s"}}
              ],
              "pipelines": [{{"id":"p","name":"p","steps":[]}}],
              "routes": [{{"id":"r","source":"s","pipeline":"p","deliver":[{{"to":["main"]}}]}}]
            }}"#
        );
        let system = load_str(&json).unwrap_or_else(|e| panic!("{label}: {e}"));
        let (leftovers, spare) = (system.topology.destinations[1].id, system.topology.destinations[2].id);
        let source = system.topology.sources[0].id;
        let want = Answer::Deliver(vec![leftovers, spare]);
        assert_eq!(system.topology.on_exception.answer(&kind), &want, "{label}");

        let compiled = system.compile_topology().expect("must compile");
        // Or the worker never builds them, and the rows fall through to being dropped exactly as if none were named.
        let reached = compiled.destinations_reachable_from(source);
        assert!(
            reached.contains(&leftovers) && reached.contains(&spare),
            "{label}: {reached:?}"
        );

        for (bad, says) in [
            (
                json.replace(r#""spare"]"#, r#""nowhere"]"#),
                "unknown destination \"nowhere\"",
            ),
            (json.replace(r#""spare"]"#, r#""leftovers"]"#), "twice"),
            (json.replace(r#"["leftovers", "spare"]"#, "[]"), "no destination"),
        ] {
            let err = load_str(&bad).unwrap_err().to_string();
            assert!(err.contains("the group's") && err.contains(says), "{label}: {err}");
        }
    }
}

/// A destination's format says what it writes of each row, and each kind offers only the formats it can write: an
/// event's text is UTF-8, so only Splunk's raw endpoint takes the source's bytes. Only `text` reads a field.
#[test]
fn a_destination_writes_the_format_its_kind_offers() {
    let kafka = r#""type":"kafka","brokers":"b:9092","defaultTopic":"o""#;
    let event = r#""type":"splunkHec","url":"https://hec:8088","token":{"secret":"t","version":1}"#;
    let raw = r#""type":"splunkHec","url":"https://hec:8088","token":{"secret":"t","version":1},"endpoint":"raw""#;
    let text = |field: &str| Payload::Text { field: field.into() };
    for (destination, format, writes) in [
        (kafka, "", Ok(Payload::Record)),
        (kafka, r#","format":"text""#, Ok(text("_raw"))),
        (kafka, r#","format":"text","field":"msg""#, Ok(text("msg"))),
        (kafka, r#","format":"bytes""#, Ok(Payload::source_bytes())),
        (kafka, r#","format":"sourceBytes""#, Ok(Payload::source_bytes())),
        (
            kafka,
            r#","field":"msg""#,
            Err(r#"names a field, which only the "text" and "bytes" formats read"#),
        ),
        (event, r#","format":"text""#, Ok(text("_raw"))),
        (
            event,
            r#","format":"bytes""#,
            Err(r#"writes format "bytes", which only the raw endpoint takes"#),
        ),
        (raw, r#","format":"sourceBytes""#, Ok(Payload::source_bytes())),
        (raw, "", Ok(Payload::Record)),
    ] {
        let json = cfg(
            &format!(r#"{{"id":"main",{destination}{format}}}"#),
            EMPTY_PIPELINE,
            ROUTE,
        );
        match (load_str(&json), writes) {
            (Ok(system), Ok(payload)) => {
                assert_eq!(system.topology.destinations[0].kind.payload(), payload, "{format}")
            }
            (Err(e), Err(says)) => assert!(e.to_string().contains(says), "{format}: {e}"),
            (got, want) => panic!("{destination}{format}: {:?}, wanted {want:?}", got.map(|_| ())),
        }
    }
}

/// A manifest that says nothing blocks the delivery kinds, and `"drop"` is the one way to lose these rows.
#[test]
fn a_delivery_kind_defaults_to_block_and_drop_is_asked_for() {
    let json = |exceptions: &str| {
        let entry = if exceptions.is_empty() {
            String::new()
        } else {
            format!(r#""exceptions": {exceptions},"#)
        };
        format!(
            r#"{{
              "version": 1,
              {entry}
              "sources": [{{"id":"s","type":"kafka","brokers":"b:9092","topics":["t"],"groupId":"g"}}],
              "destinations": [{{"id":"main","type":"kafka","brokers":"b:9092","defaultTopic":"o"}}],
              "pipelines": [{{"id":"p","name":"p","steps":[]}}],
              "routes": [{{"id":"r","source":"s","pipeline":"p","deliver":[{{"to":["main"]}}]}}]
            }}"#
        )
    };
    for kind in [ExceptionKind::Unmatched, ExceptionKind::Unprocessable] {
        let loaded = |exceptions: &str| {
            let t = load_str(&json(exceptions)).expect("must load").topology;
            t.on_exception.answer(&kind).clone()
        };
        let label = format!("{kind:?}");
        let of = |answer: &str| format!(r#"{{"kinds": {{"{label}": {answer}}}}}"#);
        assert_eq!(loaded(""), Answer::Block, "{label}");
        assert_eq!(loaded(&of(r#""block""#)), Answer::Block, "{label}");
        assert_eq!(loaded(&of(r#""drop""#)), Answer::Drop, "{label}");
        assert_eq!(
            loaded(r#"{"categories": {"delivery": "drop"}}"#),
            Answer::Drop,
            "{label}"
        );
    }
}

fn bounded_cfg(source_extra: &str, destination: &str) -> String {
    format!(
        r#"{{"version":1,
            "sources":[{{"id":"in","type":"kafka","brokers":"b:9092","topics":["t"],"groupId":"g"{source_extra}}}],
            "destinations":[{destination}],
            "pipelines":[{{"id":"p","name":"P","steps":[]}}],
            "routes":[{{"id":"r","source":"in","pipeline":"p","deliver":[{{"to":["out"]}}]}}]}}"#
    )
}

fn kafka_out(extra: &str) -> String {
    format!(r#"{{"id":"out","type":"kafka","brokers":"b:9092","defaultTopic":"t"{extra}}}"#)
}

fn elastic_out(extra: &str) -> String {
    format!(r#"{{"id":"out","type":"elasticsearch","url":"http://es:9200","defaultIndex":"i"{extra}}}"#)
}

/// A start and a stop read as many topics as a source names.
#[test]
fn a_start_and_a_stop_take_many_topics() {
    let two = r#","topics":["a","b"]"#;
    for start_or_stop in [
        "",
        r#","start":{"type":"timestamp","millis":1700000000000}"#,
        r#","stop":{"type":"timestamp","millis":1700000000000}"#,
    ] {
        let json = bounded_cfg(&format!("{two}{start_or_stop}"), &kafka_out("")).replace(r#""topics":["t"],"#, "");
        load_str(&json).expect("a start or a stop reads every topic named");
    }
}

/// An offset is a position within one partition, so it is neither a start nor a stop for a source that reads many.
#[test]
fn an_offset_is_no_start_or_stop() {
    for position in ["start", "stop"] {
        let json = bounded_cfg(
            &format!(r#","{position}":{{"type":"offset","value":400}}"#),
            &kafka_out(""),
        );
        load_str(&json).expect_err("an offset is refused");
    }
}

/// Every bounded setting refuses a value outside its range: `inFlight`
/// (zero can never deliver), `bufferBytes`, `fetchBytes`.
#[test]
fn out_of_range_bounds_are_load_errors() {
    let big = 4usize * 1024 * 1024 * 1024;
    let cases: Vec<(String, &str)> = vec![
        (bounded_cfg("", &elastic_out(r#","inFlight":0"#)), "concurrent requests"),
        (
            bounded_cfg("", &elastic_out(r#","inFlight":1000"#)),
            "concurrent requests",
        ),
        (
            bounded_cfg("", &kafka_out(r#","bufferBytes":1024"#)),
            "lets egress and its producer each hold",
        ),
        (
            bounded_cfg("", &kafka_out(&format!(r#","bufferBytes":{big}"#))),
            "lets egress and its producer each hold",
        ),
        (bounded_cfg(r#","fetchBytes":1024"#, &kafka_out("")), "fetched records"),
        (
            bounded_cfg(&format!(r#","fetchBytes":{big}"#), &kafka_out("")),
            "fetched records",
        ),
    ];
    for (json, expected) in cases {
        let file: smeltr::config_file::FileConfig = serde_json::from_str(&json).expect("parses");
        let errors = smeltr::config_file::validation_errors(file);
        assert!(errors.iter().any(|e| e.contains(expected)), "{json}: {errors:?}");
    }
}

/// A non-default bounded value round-trips; the default is not written.
#[test]
fn bounded_values_round_trip_and_defaults_stay_out_of_the_file() {
    use smeltr::plan::topology::DestinationKind;
    use smeltr::plan::topology::SourceKind;
    let mib = 1024 * 1024;

    let json = bounded_cfg("", &elastic_out(r#","inFlight":9"#));
    let system = load_str(&json).expect("loads");
    assert_eq!(elastic(&system, 0).in_flight, 9);

    // 16 MiB is a set value; 64 MiB is the default, and loads to the same number either way.
    for value in [16 * mib, 64 * mib] {
        let json = bounded_cfg("", &kafka_out(&format!(r#","bufferBytes":{value}"#)));
        let system = load_str(&json).expect("loads");
        match &system.topology.destinations[0].kind {
            DestinationKind::Kafka(k) => assert_eq!(k.buffer_bytes, value),
            other => panic!("expected a Kafka destination, got {other:?}"),
        }

        let json = bounded_cfg(&format!(r#","fetchBytes":{value}"#), &kafka_out(""));
        let system = load_str(&json).expect("loads");
        match &system.topology.sources[0].kind {
            SourceKind::Kafka(k) => assert_eq!(k.fetch_bytes, value),
            other => panic!("expected a Kafka source, got {other:?}"),
        }
    }
}

/// A byte figure is stated as a size or a number and comes back as the exact
/// binary size, so `"4 GB"` loads and `67108864` is written `"64 MiB"`.
#[test]
fn byte_figures_read_in_units_and_are_written_in_exact_binary_units() {
    use smeltr::plan::topology::DestinationKind;
    let json = bounded_cfg("", &kafka_out(r#","bufferBytes":"16 MiB""#));
    let system = load_str(&json).expect("loads");
    match &system.topology.destinations[0].kind {
        DestinationKind::Kafka(k) => assert_eq!(k.buffer_bytes, 16 * 1024 * 1024),
        other => panic!("expected a Kafka destination, got {other:?}"),
    }

    let json = bounded_cfg("", &kafka_out(r#","bufferBytes":"1 GB""#));
    let file: smeltr::config_file::FileConfig = serde_json::from_str(&json).expect("parses");
    let written = serde_json::to_string(&file).unwrap();
    assert!(
        written.contains(r#""bufferBytes":"1000000000 B""#),
        "a decimal gigabyte divides no binary unit, so it is written in bytes: {written}"
    );

    let json = bounded_cfg("", &kafka_out(r#","bufferBytes":16777216"#));
    let file: smeltr::config_file::FileConfig = serde_json::from_str(&json).expect("parses");
    let written = serde_json::to_string(&file).unwrap();
    assert!(written.contains(r#""bufferBytes":"16 MiB""#), "{written}");

    let json = bounded_cfg("", &kafka_out(r#","bufferBytes":"16 parsecs""#));
    let err = serde_json::from_str::<smeltr::config_file::FileConfig>(&json).unwrap_err();
    assert!(err.to_string().contains("unit must be"), "{err}");

    let json = bounded_cfg("", &kafka_out(r#","bufferBytes":"1 KiB""#));
    let file: smeltr::config_file::FileConfig = serde_json::from_str(&json).expect("parses");
    let errors = smeltr::config_file::validation_errors(file);
    assert!(
        errors.iter().any(|e| e.contains("hold 1 KiB") && e.contains("4 MiB")),
        "the error speaks in units: {errors:?}"
    );
}

/// A source's `encoding`: an alias resolves to its canonical name, `auto`
/// stays out of the file, and a name encoding_rs does not know is a load
/// error naming it.
#[test]
fn source_encoding_resolves_and_round_trips() {
    use smeltr::format::encoding::Encoding;
    use smeltr::plan::topology::SourceKind;

    let encoding_of = |json: &str| match &load_str(json).expect("loads").topology.sources[0].kind {
        SourceKind::Kafka(k) => k.encoding,
        other => panic!("expected a Kafka source, got {other:?}"),
    };

    assert_eq!(encoding_of(&bounded_cfg("", &kafka_out(""))), Encoding::Auto);
    assert_eq!(
        encoding_of(&bounded_cfg(r#","encoding":"auto""#, &kafka_out(""))),
        Encoding::Auto
    );

    let latin = bounded_cfg(r#","encoding":"latin1""#, &kafka_out(""));
    assert_eq!(encoding_of(&latin).label(), "windows-1252");

    let none = bounded_cfg(r#","encoding":"none""#, &kafka_out(""));
    assert_eq!(encoding_of(&none), Encoding::None);

    // A label WHATWG maps to `replacement`, which would decode every record to one U+FFFD.
    let refused = bounded_cfg(r#","encoding":"hz-gb-2312""#, &kafka_out(""));
    let file: smeltr::config_file::FileConfig = serde_json::from_str(&refused).expect("parses");
    let errors = smeltr::config_file::validation_errors(file);
    assert!(errors.iter().any(|e| e.contains("hz-gb-2312")), "{errors:?}");

    let bad = bounded_cfg(r#","encoding":"klingon""#, &kafka_out(""));
    let file: smeltr::config_file::FileConfig = serde_json::from_str(&bad).expect("parses");
    let errors = smeltr::config_file::validation_errors(file);
    assert!(
        errors.iter().any(|e| e.contains("klingon") && e.contains("utf-16le")),
        "{errors:?}"
    );
}

/// A source that declares `encoding: none` has no `_raw` for a pipeline to read; `__source.bytes` decoded is the text.
#[test]
fn a_source_that_decodes_none_has_no_raw_to_read() {
    let with_body = |body: &str| {
        format!(
            r#"{{"version":1,
                "sources":[{{"id":"in","type":"kafka","brokers":"b:9092","topics":["t"],"groupId":"g","encoding":"none"}}],
                "destinations":[{}],
                "pipelines":[{{"id":"p","name":"P","steps":[{{"id":"c","type":"code","body":{}}}]}}],
                "routes":[{{"id":"r","source":"in","pipeline":"p","deliver":[{{"to":["out"]}}]}}]}}"#,
            kafka_out(""),
            serde_json::Value::from(body)
        )
    };
    let refused = refusal(&with_body("msg = _raw;"));
    assert!(refused.contains("_raw"), "{refused}");
    let decoded = refusal(&with_body(r#"msg = decode(__source.bytes, "utf-8");"#));
    assert!(decoded.is_empty(), "{decoded}");
    let parsed = refusal(&with_body("Object.assign($, json(__source.bytes));"));
    assert!(parsed.contains("parses text") && parsed.contains("decode"), "{parsed}");
    let parsed = refusal(&with_body(
        r#"Object.assign($, json(decode(__source.bytes, "utf-8")));"#,
    ));
    assert!(parsed.is_empty(), "{parsed}");
}

/// Every destination states its memory ceiling in bytes, whatever
/// enforces it, so a worker's totals add up.
#[test]
fn every_destination_states_its_buffer_ceiling_in_bytes() {
    let mib = 1024 * 1024;
    let json = format!(
        r#"{{"version":1,
            "sources":[{{"id":"in","type":"kafka","brokers":"b:9092","topics":["t"],"groupId":"g"}}],
            "destinations":[
              {{"id":"k","type":"kafka","brokers":"b:9092","defaultTopic":"t","bufferBytes":{}}},
              {{"id":"e","type":"elasticsearch","url":"http://es:9200","defaultIndex":"i","inFlight":3,"maxBulkBytes":{},"bufferBytes":{}}}],
            "pipelines":[{{"id":"p","name":"P","steps":[]}}],
            "routes":[{{"id":"r","source":"in","pipeline":"p","deliver":[{{"to":["k","e"]}}]}}]}}"#,
        8 * mib,
        2 * mib,
        32 * mib
    );
    let loaded = load_str(&json).expect("loads");
    let ceilings: Vec<usize> = loaded
        .topology
        .destinations
        .iter()
        .map(|d| d.kind.queue_bytes())
        .collect();
    // Kafka's `bufferBytes` is the whole a worker holds, half of it in egress; Elasticsearch's is egress's alone.
    assert_eq!(ceilings, vec![4 * mib, 32 * mib]);
}

/// An HTTP destination loads its URL as the save parsed it, percent-encoded, a user or password in it included.
#[test]
fn an_http_destination_url_loads_encoded() {
    let json = |url: &str| format!(r#"{{"version":1,"destinations":[{{"id":"h","type":"http","url":"{url}"}}]}}"#);
    for (url, loaded) in [
        (
            "https://intake.example.test/a b/ünï?q=a b",
            "https://intake.example.test/a%20b/%C3%BCn%C3%AF?q=a%20b",
        ),
        (
            "https://user:pass@intake.example.test/",
            "https://user:pass@intake.example.test/",
        ),
        ("https://user@intake.example.test/", "https://user@intake.example.test/"),
    ] {
        let json = json(url);
        assert!(refusal(&json).is_empty(), "{url}: {}", refusal(&json));
        match &load_str(&json).expect("loads").topology.destinations[0].kind {
            smeltr::plan::topology::DestinationKind::Http(h) => assert_eq!(h.url, loaded),
            _ => panic!("http"),
        }
    }
}

/// Raising the request size alone loads: the default start comes down to what the buffer affords, while a stated
/// start above it is refused.
#[test]
fn the_default_start_fits_the_buffer_a_larger_request_leaves() {
    let mib = 1024 * 1024;
    let json = |in_flight: &str| {
        format!(
            r#"{{"version":1,
                "destinations":[{{"id":"h","type":"http","url":"https://intake.example.test",
                  "maxRequestBytes":{}{in_flight}}}]}}"#,
            32 * mib
        )
    };
    let unstated = json("");
    assert!(refusal(&unstated).is_empty(), "{}", refusal(&unstated));
    let loaded = load_str(&unstated).expect("loads");
    match &loaded.topology.destinations[0].kind {
        smeltr::plan::topology::DestinationKind::Http(h) => assert_eq!(h.in_flight, 2),
        _ => panic!("http"),
    }
    let stated = json(r#","inFlight":3"#);
    assert!(
        refused_at(
            &stated,
            "inFlight",
            "starts at 3 concurrent requests but its buffer affords 2"
        ),
        "{}",
        refusal(&stated)
    );
}

/// `inFlight` is where the loop starts; what the buffer affords is where it stops. An operator who wants more
/// concurrency raises the buffer, and never tunes a request count.
#[test]
fn the_buffer_decides_how_many_requests_divide_it() {
    let mib = 1024 * 1024;
    let ceiling = |buffer: usize, bulk: usize| {
        let json = format!(
            r#"{{"version":1,
                "sources":[{{"id":"in","type":"kafka","brokers":"b:9092","topics":["t"],"groupId":"g"}}],
                "destinations":[
                  {{"id":"e","type":"elasticsearch","url":"http://es:9200","defaultIndex":"i","inFlight":1,
                    "maxBulkBytes":{bulk},"bufferBytes":{buffer}}}],
                "pipelines":[{{"id":"p","name":"P","steps":[]}}],
                "routes":[{{"id":"r","source":"in","pipeline":"p","deliver":[{{"to":["e"]}}]}}]}}"#
        );
        let loaded = load_str(&json).expect("loads");
        match &loaded.topology.destinations[0].kind {
            smeltr::plan::topology::DestinationKind::Elasticsearch(e) => e.max_in_flight(),
            _ => panic!("elasticsearch"),
        }
    };

    assert_eq!(ceiling(64 * mib, 5 * mib), 12, "whole requests only");
    assert_eq!(ceiling(32 * mib, 5 * mib), 6, "a smaller buffer affords fewer");
    assert_eq!(
        ceiling(64 * mib, 64 * mib),
        1,
        "a buffer holding one request affords one"
    );
    assert_eq!(
        ceiling(1024 * mib, 64 * 1024),
        smeltr::plan::topology::MAX_SENDERS,
        "a buffer divided by a small enough request is capped by the senders one destination may own"
    );
}

/// A destination that states no buffer gets the default rather than one derived from its other settings: the figure
/// the memory formula adds up has to be the same one the operator can read off the config.
#[test]
fn a_destination_that_states_no_buffer_takes_the_default() {
    let json = r#"{"version":1,
            "sources":[{"id":"in","type":"kafka","brokers":"b:9092","topics":["t"],"groupId":"g"}],
            "destinations":[
              {"id":"e","type":"elasticsearch","url":"http://es:9200","defaultIndex":"i","inFlight":1}],
            "pipelines":[{"id":"p","name":"P","steps":[]}],
            "routes":[{"id":"r","source":"in","pipeline":"p","deliver":[{"to":["e"]}]}]}"#;
    let loaded = load_str(json).expect("loads");
    assert_eq!(
        loaded.topology.destinations[0].kind.queue_bytes(),
        64 * 1024 * 1024,
        "one request in flight does not shrink the buffer"
    );
}

// -- route preview -------------------------------------------------------
//
// `runtime::preview_routes` answers where a sample's records would go without a destination existing. It runs the
// transform's own routing and serialization and stops where a worker would dispatch, so these assertions are about
// the same code that moves production data.
//
// A route's condition is evaluated before any pipeline has run, so it reads `_raw` and what the source declares --
// never a parsed field. That is why these conditions select on `__source.topic`.

fn two_route_config_json() -> &'static str {
    r#"{
      "version": 1,
      "sources": [
        { "id": "orders", "type": "kafka", "brokers": "b1:9092", "topics": ["orders", "other"], "groupId": "smeltr" }
      ],
      "destinations": [
        { "id": "es-prod", "type": "elasticsearch", "url": "http://es:9200", "defaultIndex": "orders" },
        { "id": "kafka-audit", "type": "kafka", "brokers": "b1:9092", "defaultTopic": "audit" }
      ],
      "pipelines": [
        {
          "id": "big",
          "name": "big orders",
          "steps": [
            { "id": "parse", "type": "parser", "input": "_raw", "kind": { "type": "json" },
              "declared": [ { "name": "total", "type": "i64" } ] }
          ]
        },
        {
          "id": "audit",
          "name": "audit everything",
          "steps": [
            { "id": "parse", "type": "parser", "input": "_raw", "kind": { "type": "json" },
              "declared": [ { "name": "total", "type": "i64" } ] },
            { "id": "drop-small", "type": "drop", "condition": "total < 100" }
          ]
        }
      ],
      "routes": [
        { "id": "orders-topic", "source": "orders", "condition": "__source.topic == \"orders\"", "pipeline": "big",
          "deliver": [ { "to": ["es-prod"] } ] },
        { "id": "catch-all", "source": "orders", "condition": null, "pipeline": "audit",
          "deliver": [ { "to": ["kafka-audit"] } ] }
      ]
    }"#
}

fn sample_records(rows: &[(&str, &str)]) -> Vec<smeltr::io::SourceRecord> {
    rows.iter()
        .map(|(topic, raw)| smeltr::io::SourceRecord {
            headers: None,
            chunk: smeltr::io::RecordChunk::of_bytes(raw.as_bytes()),
            row: 0,
            offset: 0,
            partition: 0,
            topic: std::sync::Arc::from(*topic),
        })
        .collect()
}

/// What a preview may hold. The leader's own ceiling in production; these tests name it so one that asks for more
/// can be written.
fn budget() -> smeltr::core::buffer::MemoryBudget {
    smeltr::runtime::preview_memory_budget()
}

/// A record matching a non-final route reaches that route's destinations *and* stays eligible for later ones. Which
/// destinations it would reach comes out of the delivery rules, with nothing standing in for a destination.
#[test]
fn a_route_preview_says_where_each_record_would_go() {
    let system = load_str(two_route_config_json()).unwrap_or_else(|e| panic!("load failed: {e}"));
    let compiled = system
        .compile_topology()
        .unwrap_or_else(|e| panic!("compile: {}", join_errors(&e)));
    let source = &system.topology.sources[0];

    let records = sample_records(&[("orders", r#"{"total":250}"#), ("other", r#"{"total":40}"#)]);
    let rows = smeltr::runtime::preview_routes(&compiled, source, &records, budget());
    assert_eq!(rows.len(), 2);

    // On `orders`, so the first route matches -- and it is not final, so the catch-all sees the row too.
    assert_eq!(rows[0].matched, vec![0, 1], "a non-final route leaves the row eligible");
    let reached: Vec<Vec<u32>> = rows[0]
        .through
        .iter()
        .flat_map(|t| t.documents.iter().map(|d| d.destinations.clone()))
        .collect();
    assert_eq!(reached.len(), 2, "both routes kept it");
    assert_eq!(reached[0], vec![0], "the orders route delivers to es-prod");
    assert_eq!(reached[1], vec![1], "and the catch-all to kafka-audit");
    assert!(rows[0].through[0].documents[0].document.contains("\"total\":250"));

    // On `other`, so only the catch-all matches -- and its pipeline drops a total under 100.
    assert_eq!(rows[1].matched, vec![1], "only the catch-all");
    assert!(
        rows[1].through[0].documents.is_empty(),
        "a route that matched and then dropped the row reports no document"
    );
}

/// Each route a record went through reports the exceptions its pipeline raised and the answer it took, so a row its
/// answer dropped is told apart from one the pipeline filtered, and a delivered row shows its record.
#[test]
fn a_route_preview_reports_each_routes_exceptions_and_answer() {
    use smeltr::plan::config::Answer;
    let preview = |exceptions: &str| {
        let json = two_route_config_json().replacen(
            r#""pipeline": "audit","#,
            &format!(r#""pipeline": "audit", "exceptions": {exceptions},"#),
            1,
        );
        let system = load_str(&json).unwrap_or_else(|e| panic!("load failed: {e}"));
        let compiled = system
            .compile_topology()
            .unwrap_or_else(|e| panic!("compile: {}", join_errors(&e)));
        let records = sample_records(&[("other", r#"{"total":"many"}"#)]);
        let audit = system
            .topology
            .destinations
            .iter()
            .find(|d| d.name == "kafka-audit")
            .unwrap()
            .id;
        let rows = smeltr::runtime::preview_routes(&compiled, &system.topology.sources[0], &records, budget());
        (rows, audit)
    };

    let (rows, _) = preview(r#"{"categories":{"parse":"drop"}}"#);
    let through = &rows[0].through[0];
    assert_eq!(through.answer, Answer::Drop);
    assert!(through.documents.is_empty(), "the answer dropped it");
    assert_eq!(through.exceptions.len(), 1, "got {:?}", through.exceptions);
    assert_eq!(through.exceptions[0].0, "UnparsableField");

    let (rows, audit) = preview(r#"{"categories":{"parse":{"deliver":["kafka-audit"]}}}"#);
    let through = &rows[0].through[0];
    assert!(matches!(through.answer, Answer::Deliver(_)));
    let doc = &through.documents[0];
    assert_eq!(doc.destinations, vec![audit]);
    assert_eq!(
        doc.document,
        r#"{"smeltr":{"exception":{"route":"catch-all","pipeline":"audit everything","exceptions":[{"step":"parse","kind":"UnparsableField","message":"_raw.total is \"many\", which could not parse as i64 in total"}]}},"_raw":"{\"total\":\"many\"}"}"#,
        "the record, saying where it failed"
    );
}

/// A preview runs in the leader, which also serves the GUI and every worker's heartbeat. A record asking for more
/// memory than a preview may hold is refused on that record; unbudgeted the allocation would end the process.
#[test]
fn a_record_that_asks_past_the_preview_budget_is_refused_and_the_rest_still_previews() {
    let json = r#"{
      "version": 1,
      "sources": [
        { "id": "orders", "type": "kafka", "brokers": "b1:9092", "topics": ["orders"], "groupId": "smeltr" }
      ],
      "destinations": [
        { "id": "es-prod", "type": "elasticsearch", "url": "http://es:9200", "defaultIndex": "orders" }
      ],
      "pipelines": [
        { "id": "fan", "name": "fan", "steps": [
          { "id": "parse", "type": "parser", "input": "_raw", "kind": { "type": "json" },
            "declared": [ { "name": "items", "type": {"type":"array","of":"i64"} } ] },
          { "id": "u", "type": "unroll", "field": "items" }
        ] }
      ],
      "routes": [
        { "id": "all", "source": "orders", "condition": null, "pipeline": "fan",
          "deliver": [ { "to": ["es-prod"] } ] }
      ]
    }"#;
    let system = load_str(json).unwrap_or_else(|e| panic!("load: {e}"));
    let compiled = system
        .compile_topology()
        .unwrap_or_else(|e| panic!("compile: {}", join_errors(&e)));
    let source = &system.topology.sources[0];

    // One record carrying an array that unrolls to a column far larger than the ceiling it is previewed under.
    let big = format!(
        r#"{{"items":[{}]}}"#,
        (0..40_000).map(|i| i.to_string()).collect::<Vec<_>>().join(",")
    );
    let records = sample_records(&[("orders", big.as_str()), ("orders", r#"{"items":[9]}"#)]);
    let tight = smeltr::core::buffer::MemoryBudget::new(64 * 1024);
    let rows = smeltr::runtime::preview_routes(&compiled, source, &records, tight);

    assert_eq!(rows.len(), 2, "one entry per input record");
    let refused = rows[0].refused.as_deref().expect("the big record was refused");
    assert!(refused.contains("bytes") || refused.contains("budget"), "{refused}");
    assert!(
        rows[1].refused.is_none() && rows[1].through[0].documents.len() == 1,
        "the budget the refused record held was released with its pool"
    );
}

/// A pipeline that unrolls an array produces more documents than it was given records. Nothing may attribute those
/// back by row position: `run_and_serialize` rebinds its row count to the batch's *after* the pipeline has run, so a
/// preview that indexed its input by that number read past the end of a one-record sample.
#[test]
fn a_pipeline_that_unrolls_attributes_every_document_to_the_record_it_came_from() {
    let json = r#"{
      "version": 1,
      "sources": [
        { "id": "orders", "type": "kafka", "brokers": "b1:9092", "topics": ["orders"], "groupId": "smeltr" }
      ],
      "destinations": [
        { "id": "es-prod", "type": "elasticsearch", "url": "http://es:9200", "defaultIndex": "orders" }
      ],
      "pipelines": [
        { "id": "fan", "name": "fan", "steps": [
          { "id": "parse", "type": "parser", "input": "_raw", "kind": { "type": "json" },
            "declared": [ { "name": "items", "type": {"type":"array","of":"i64"} } ] },
          { "id": "u", "type": "unroll", "field": "items" }
        ] }
      ],
      "routes": [
        { "id": "all", "source": "orders", "condition": null, "pipeline": "fan",
          "deliver": [ { "to": ["es-prod"] } ] }
      ]
    }"#;
    let system = load_str(json).unwrap_or_else(|e| panic!("load: {e}"));
    let compiled = system
        .compile_topology()
        .unwrap_or_else(|e| panic!("compile: {}", join_errors(&e)));
    let source = &system.topology.sources[0];

    // Two records with different fan-out, so a positional mapping would both overrun and misattribute.
    let records = sample_records(&[("orders", r#"{"items":[1,2,3]}"#), ("orders", r#"{"items":[9]}"#)]);
    let rows = smeltr::runtime::preview_routes(&compiled, source, &records, budget());

    assert_eq!(rows.len(), 2, "one entry per input record, whatever the pipeline did");
    assert_eq!(rows[0].through.len(), 1, "one route kept it");
    assert_eq!(rows[0].through[0].documents.len(), 3, "three elements, three documents");
    assert_eq!(rows[1].through[0].documents.len(), 1);
    // Each record's documents are its own, not the next record's.
    assert!(rows[1].through[0].documents[0].document.contains("9"));
    for doc in &rows[0].through[0].documents {
        assert!(
            !doc.document.contains("9"),
            "a document leaked across records: {}",
            doc.document
        );
    }
}

/// A record no route's condition selects is reported as having matched nothing, which is what tells "unrouted"
/// apart from "a pipeline dropped it".
#[test]
fn a_record_no_route_matches_is_distinguishable_from_a_dropped_one() {
    let system = load_str(two_route_config_json()).unwrap_or_else(|e| panic!("load failed: {e}"));
    let compiled = system
        .compile_topology()
        .unwrap_or_else(|e| panic!("compile: {}", join_errors(&e)));
    // A source no route names: nothing can match.
    let unrouted = smeltr::plan::topology::SourceConfig {
        id: smeltr::core::id::SourceId::MAX,
        ..system.topology.sources[0].clone()
    };
    let records = sample_records(&[("orders", r#"{"total":250}"#)]);

    let rows = smeltr::runtime::preview_routes(&compiled, &unrouted, &records, budget());
    assert_eq!(rows.len(), 1);
    assert!(rows[0].matched.is_empty(), "no route selected it");
    assert!(rows[0].through.is_empty());
}

/// A CSV step's `declared` list is its column table: position is the column, and a `"skip"` row holds one without
/// producing a field. `zone` is column 2 only because `unwanted` occupies column 1.
#[test]
fn a_skip_column_holds_its_position_without_producing_a_field() {
    let json = r#"{
      "version": 1,
      "pipelines": [{
        "id": "p", "name": "p",
        "steps": [
          { "id": "fields", "type": "parser", "input": "_raw",
            "kind": { "type": "csv", "delimiter": "," },
            "declared": [
              { "name": "host", "type": "str" },
              { "name": "unwanted", "type": "skip" },
              { "name": "zone", "type": "str" }
            ] }
        ]
      }]
    }"#;
    let system = load_str(json).unwrap_or_else(|e| panic!("load failed: {e}"));
    let rows = run_pipeline(&system.pipelines[0], &["web01,secret,dmz"]);
    assert_eq!(rows[0]["host"], "web01", "{}", rows[0]);
    assert_eq!(rows[0]["zone"], "dmz", "{}", rows[0]);
    assert!(
        rows[0].get("unwanted").is_none(),
        "the skipped value is dropped: {}",
        rows[0]
    );
}

/// A step that rebuilds its input writes it back without what it skips: a CSV column left empty, a key-value pair left
/// out. A skipped key makes no field.
#[test]
fn a_rebuilding_step_writes_its_input_back_without_what_it_skips() {
    let json = r#"{
      "version": 1,
      "pipelines": [
        { "id": "c", "name": "c", "steps": [
          { "id": "fields", "type": "parser", "input": "_raw",
            "kind": { "type": "csv", "delimiter": ",", "rebuild": true },
            "declared": [
              { "name": "host", "type": "str" },
              { "name": "unwanted", "type": "skip" },
              { "name": "zone", "type": "str" }
            ] } ] },
        { "id": "k", "name": "k", "steps": [
          { "id": "pairs", "type": "parser", "input": "_raw",
            "kind": { "type": "keyValue", "pairSep": " ", "kvSep": "=", "rebuild": true },
            "declared": [
              { "name": "host", "type": "str" },
              { "name": "unwanted", "type": "skip" }
            ] } ] }
      ]
    }"#;
    let system = load_str(json).unwrap_or_else(|e| panic!("load failed: {e}"));
    let rows = run_pipeline(&system.pipelines[0], &["web01,secret,dmz"]);
    assert_eq!(rows[0]["_raw"], "web01,,dmz", "{}", rows[0]);
    let rows = run_pipeline(&system.pipelines[1], &["host=web01 unwanted=secret zone=dmz"]);
    assert_eq!(rows[0]["_raw"], "host=web01 zone=dmz", "{}", rows[0]);
    assert!(rows[0].get("unwanted").is_none(), "{}", rows[0]);
    assert_eq!(rows[0]["zone"], "dmz", "{}", rows[0]);
}

/// `"skip"` names a column or a key to leave out, so it means nothing on a parser without either, nor nested inside a
/// struct.
#[test]
fn skip_is_only_a_csv_or_key_value_type() {
    let on_grok = r#"{
      "version": 1,
      "pipelines": [{ "id": "p", "name": "p", "steps": [
        { "id": "line", "type": "parser", "input": "_raw",
          "kind": { "type": "grok", "patterns": ["%{WORD:level}"] },
          "declared": [ { "name": "level", "type": "skip" } ] }
      ]}]
    }"#;
    let e = load_str(on_grok).expect_err("skip is not a grok type").to_string();
    assert!(e.contains("skip"), "{e}");

    let nested = r#"{
      "version": 1,
      "pipelines": [{ "id": "p", "name": "p", "steps": [
        { "id": "fields", "type": "parser", "input": "_raw",
          "kind": { "type": "csv", "delimiter": "," },
          "declared": [ { "name": "o", "type": { "type": "struct",
            "fields": [ { "name": "inner", "type": "skip" } ] } } ] }
      ]}]
    }"#;
    let e = load_str(nested).expect_err("skip is not a nested type").to_string();
    assert!(e.contains("skip"), "{e}");
}

fn splunk(system: &smeltr::config_file::LoadedSystem) -> &smeltr::plan::topology::SplunkHecDestinationConfig {
    match &system.topology.destinations[0].kind {
        smeltr::plan::topology::DestinationKind::SplunkHec(h) => h,
        other => panic!("expected a Splunk HEC destination, got {other:?}"),
    }
}

/// One Splunk HEC destination, with `extra` spliced into its object.
fn hec_config(extra: &str) -> String {
    cfg(
        &format!(
            r#"{{"id":"hec","type":"splunkHec","url":"https://splunk.internal:8088",
                "token":{{"secret":"hec-token","version":3}}{extra}}}"#
        ),
        EMPTY_PIPELINE,
        r#"{"id":"r","source":"s","pipeline":"p","deliver":[{"to":["hec"]}]}"#,
    )
}

/// A HEC destination needs only its address and its token. Everything else defaults, and the default waits for the
/// indexer: a 200 from HEC means queued, not written.
#[test]
fn a_minimal_splunk_hec_destination_loads_with_the_defaults() {
    use smeltr::plan::topology::Acknowledgement;

    let json = hec_config("");
    let system = load_str(&json).expect("must load");
    system.compile_topology().expect("must compile");
    let h = splunk(&system);
    assert_eq!(h.url, "https://splunk.internal:8088");
    assert_eq!(h.acknowledgement, Acknowledgement::Indexer);
    assert_eq!(h.endpoint, smeltr::plan::topology::Endpoint::Event);
    assert_eq!(h.index.as_deref(), None);
    assert_eq!(h.max_request_bytes, 1024 * 1024);
    assert_eq!(h.buffer_bytes, 64 * 1024 * 1024);
    assert_eq!(h.in_flight, 4);
    assert!(refusal(&json).is_empty(), "{}", refusal(&json));
}

/// The token is named, never valued, and it is one of the secrets the bundle needs.
#[test]
fn a_splunk_hec_token_is_listed_as_a_secret() {
    use smeltr::plan::topology::SecretRef;

    let system = load_str(&hec_config("")).expect("must load");
    let token = SecretRef::LeaderStored {
        name: "hec-token".into(),
        version: 3,
    };
    assert_eq!(splunk(&system).token, token);
    let named = &system.topology.named_secrets;
    let named: Vec<_> = named.iter().map(|n| (&n.reference, n.what.as_str())).collect();
    assert_eq!(named, vec![(&token, r#"destination "hec": token"#)]);
}

/// `receipt` and the raw endpoint have to be chosen by name, and when they are, they load.
#[test]
fn a_splunk_hec_destination_may_count_a_batch_on_receipt() {
    let json = hec_config(r#","acknowledgement":"receipt","endpoint":"raw","index":"main""#);
    let system = load_str(&json).expect("must load");
    let h = splunk(&system);
    assert_eq!(h.acknowledgement, smeltr::plan::topology::Acknowledgement::Receipt);
    assert_eq!(h.endpoint, smeltr::plan::topology::Endpoint::Raw);
    assert_eq!(h.index.as_deref(), Some("main"));
}

/// An acknowledgement HEC does not have is refused by name, and so are a pasted endpoint and an empty field.
#[test]
fn a_splunk_hec_destination_with_bad_settings_is_refused() {
    let e = load_str(&hec_config(r#","acknowledgement":"never""#))
        .expect_err("not an acknowledgement")
        .to_string();
    assert!(
        e.contains("never") && e.contains("indexer") && e.contains("receipt"),
        "{e}"
    );

    let endpoint = cfg(
        r#"{"id":"hec","type":"splunkHec","url":"https://splunk.internal:8088/services/collector/event",
            "token":"SPLUNK_HEC_TOKEN","sourcetype":""}"#,
        EMPTY_PIPELINE,
        r#"{"id":"r","source":"s","pipeline":"p","deliver":[{"to":["hec"]}]}"#,
    );
    let e = refusal(&endpoint);
    assert!(
        e.contains(r#"Give the base address, "https://splunk.internal:8088""#),
        "{e}"
    );
    assert!(e.contains("has an empty sourcetype"), "{e}");

    let e = refusal(&hec_config(r#","maxRequestBytes":"200 MiB""#));
    assert!(e.contains("Splunk refuses a body over"), "{e}");
}

fn http(system: &smeltr::config_file::LoadedSystem) -> &smeltr::plan::topology::HttpDestinationConfig {
    match &system.topology.destinations[0].kind {
        smeltr::plan::topology::DestinationKind::Http(h) => h,
        other => panic!("expected an HTTP destination, got {other:?}"),
    }
}

/// One HTTP destination at `url`, with `extra` spliced into its object.
fn http_config(url: &str, extra: &str) -> String {
    cfg(
        &format!(r#"{{"id":"intake","type":"http","url":"{url}"{extra}}}"#),
        EMPTY_PIPELINE,
        r#"{"id":"r","source":"s","pipeline":"p","deliver":[{"to":["intake"]}]}"#,
    )
}

/// An HTTP destination needs only its URL: the rows go as NDJSON records, uncompressed, with no headers of its own.
#[test]
fn a_minimal_http_destination_loads_with_the_defaults() {
    use smeltr::plan::topology::{Compression, RequestBody};

    let json = http_config("https://intake.example/v1/logs", "");
    let system = load_str(&json).expect("must load");
    system.compile_topology().expect("must compile");
    let h = http(&system);
    assert_eq!(h.url, "https://intake.example/v1/logs");
    assert_eq!(h.body, RequestBody::Lines);
    assert_eq!(h.payload, smeltr::plan::config::Payload::Record);
    assert_eq!(h.compression, Compression::None);
    assert!(h.headers.is_empty() && h.credentials.is_empty());
    assert_eq!(h.max_request_bytes, 4 * 1024 * 1024);
    assert!(refusal(&json).is_empty(), "{}", refusal(&json));
    assert_eq!(
        system.topology.destinations[0].kind.placement(),
        None,
        "every row goes to the one URL"
    );
}

/// A header's value is committed or named, and a named one is among the secrets the bundle needs.
#[test]
fn an_http_destination_names_its_credential_headers_as_secrets() {
    use smeltr::plan::topology::SecretRef;

    let json = http_config(
        "https://intake.example/v1/logs",
        r#","body":"jsonArray","compression":"gzip","headers":{"X-Source":"smeltr"},
           "credentials":{"DD-API-KEY":{"secret":"dd","version":2}}"#,
    );
    let system = load_str(&json).expect("must load");
    assert!(refusal(&json).is_empty(), "{}", refusal(&json));
    let h = http(&system);
    assert_eq!(h.body, smeltr::plan::topology::RequestBody::JsonArray);
    assert_eq!(h.headers, vec![("X-Source".to_string(), "smeltr".to_string())]);
    let key = SecretRef::LeaderStored {
        name: "dd".into(),
        version: 2,
    };
    assert_eq!(h.credentials, vec![("DD-API-KEY".to_string(), key.clone())]);
    let named = &system.topology.named_secrets;
    let named: Vec<_> = named.iter().map(|n| (&n.reference, n.what.as_str())).collect();
    assert_eq!(named, vec![(&key, r#"destination "intake": credentials"#)]);
}

/// One Prometheus destination at `url`, with `extra` spliced into its object.
fn prometheus_config(url: &str, extra: &str) -> String {
    cfg(
        &format!(r#"{{"id":"store","type":"prometheus","url":"{url}"{extra}}}"#),
        EMPTY_PIPELINE,
        r#"{"id":"r","source":"s","pipeline":"p","deliver":[{"to":["store"]}]}"#,
    )
}

/// A Prometheus destination needs only its URL, sends the record, and names its credential headers as secrets.
#[test]
fn a_prometheus_destination_loads_and_names_its_credentials() {
    use smeltr::plan::topology::{DestinationKind, SecretRef};

    let json = prometheus_config(
        "https://mimir.example/api/v1/push",
        r#","headers":{"X-Scope-OrgID":"t1"},"credentials":{"Authorization":{"secret":"mimir","version":1}}"#,
    );
    assert!(refusal(&json).is_empty(), "{}", refusal(&json));
    let system = load_str(&json).expect("must load");
    system.compile_topology().expect("must compile");
    let kind = &system.topology.destinations[0].kind;
    let DestinationKind::Prometheus(p) = kind else {
        panic!("expected a Prometheus destination, got {kind:?}")
    };
    assert_eq!(p.url, "https://mimir.example/api/v1/push");
    assert_eq!(p.headers, vec![("X-Scope-OrgID".to_string(), "t1".to_string())]);
    assert_eq!(p.max_request_bytes, 1024 * 1024);
    assert_eq!(kind.payload(), smeltr::plan::config::Payload::Record);
    assert_eq!(kind.placement(), None);
    let key = SecretRef::LeaderStored {
        name: "mimir".into(),
        version: 1,
    };
    assert_eq!(p.credentials, vec![("Authorization".to_string(), key)]);
    assert_eq!(
        p.version,
        smeltr::plan::topology::RemoteWriteVersion::V1,
        "1.0 when unsaid"
    );
}

/// A Prometheus destination's `version` picks remote_write 2.0; anything but `1.0` or `2.0` is refused.
#[test]
fn a_prometheus_destination_names_its_remote_write_version() {
    use smeltr::plan::topology::{DestinationKind, RemoteWriteVersion};

    let at = "https://prometheus.example/api/v1/write";
    let system = load_str(&prometheus_config(at, r#","version":"2.0""#)).expect("must load");
    let DestinationKind::Prometheus(p) = &system.topology.destinations[0].kind else {
        panic!("a Prometheus destination")
    };
    assert_eq!(p.version, RemoteWriteVersion::V2);
    assert!(load_str(&prometheus_config(at, r#","version":"3.0""#)).is_err());
}

#[test]
fn a_prometheus_destination_with_bad_settings_is_refused() {
    let at = "https://prometheus.example/api/v1/write";
    for (url, extra, says) in [
        ("prometheus.example", "", "give a whole http:// or https:// URL"),
        (at, r#","headers":{"Content-Encoding":"gzip"}"#, "smeltr sets itself"),
        (
            at,
            r#","headers":{"X-Prometheus-Remote-Write-Version":"2.0.0"}"#,
            "smeltr sets itself",
        ),
    ] {
        let e = refusal(&prometheus_config(url, extra));
        assert!(e.contains(says), "{url} {extra}: {e}");
    }
}

#[test]
fn an_http_destination_with_bad_settings_is_refused() {
    let at = "https://intake.example/";
    for (url, extra, says) in [
        ("intake.example", "", "give a whole http:// or https:// URL"),
        ("https://", "", "give a whole http:// or https:// URL"),
        ("https://bad host/hook", "", "give a whole http:// or https:// URL"),
        (at, r#","compression":"zstd""#, "none or gzip"),
        (at, r#","headers":{"Content-Type":"text/csv"}"#, "smeltr sets itself"),
        (at, r#","headers":{"bad name":"v"}"#, "not a header name"),
        (at, r#","headers":{"X-A":"one\ntwo"}"#, "line break"),
        (
            at,
            r#","headers":{"x-team":"a"},"credentials":{"X-Team":{"secret":"t","version":1}}"#,
            "twice",
        ),
    ] {
        let e = refusal(&http_config(url, extra));
        assert!(e.contains(says), "{url} {extra}: {e}");
    }
    // Text and bytes go in a JSON array as strings, bytes as base64.
    for format in ["text", "bytes"] {
        for body in ["lines", "jsonArray", "single"] {
            let extra = format!(r#","format":"{format}","body":"{body}""#);
            let e = refusal(&http_config(at, &extra));
            assert!(e.is_empty(), "{extra}: {e}");
        }
    }
    // Plain HTTP with a credential is the admin's choice, on this host or off it.
    for url in ["http://127.0.0.1:8080/in", "http://intake.example/in"] {
        let plain = http_config(url, r#","credentials":{"Authorization":{"secret":"t","version":1}}"#);
        assert!(refusal(&plain).is_empty(), "{url}: {}", refusal(&plain));
    }
    // A credential in `headers` is the writer's choice to commit it.
    let committed = http_config(at, r#","headers":{"X-Api-Key":"abc"}"#);
    assert!(refusal(&committed).is_empty(), "{}", refusal(&committed));
}

fn iceberg(system: &smeltr::config_file::LoadedSystem) -> &smeltr::plan::topology::IcebergDestinationConfig {
    match &system.topology.destinations[0].kind {
        smeltr::plan::topology::DestinationKind::Iceberg(c) => c,
        other => panic!("expected an Iceberg destination, got {other:?}"),
    }
}

/// One Iceberg destination writing `logs.web.access` through a catalog on this host, with `extra` spliced in.
fn iceberg_config(extra: &str) -> String {
    cfg(
        &format!(
            r#"{{"id":"table","type":"iceberg","catalog":"https://catalog.example/api",
                "namespace":"logs.web","table":"access"{extra}}}"#
        ),
        EMPTY_PIPELINE,
        r#"{"id":"r","source":"s","pipeline":"p","deliver":[{"to":["table"]}]}"#,
    )
}

/// An Iceberg destination needs its catalog, namespace and table: it signs in as no one, writes with what the catalog
/// vends, and compresses its Parquet with zstd.
#[test]
fn a_minimal_iceberg_destination_loads_with_the_defaults() {
    use smeltr::plan::topology::{CatalogSignIn, Compression};
    use std::time::Duration;

    let json = iceberg_config("");
    assert!(refusal(&json).is_empty(), "{}", refusal(&json));
    let system = load_str(&json).expect("must load");
    let compiled = system
        .compile_topology()
        .unwrap_or_else(|e| panic!("{}", join_errors(&e)));
    let c = iceberg(&system);
    assert_eq!(c.catalog, "https://catalog.example/api");
    assert_eq!(c.warehouse, None);
    assert!(matches!(c.sign_in, CatalogSignIn::None));
    assert_eq!(c.namespace, vec!["logs".to_string(), "web".to_string()]);
    assert_eq!(c.table, "access");
    assert!(c.partition_by.is_empty() && c.storage.is_none() && c.tls.is_none());
    assert_eq!(c.compression, Compression::Zstd);
    assert_eq!(c.in_flight, 4);
    assert_eq!(c.max_object_bytes, 64 * 1024 * 1024);
    assert_eq!(c.buffer_bytes, 256 * 1024 * 1024);
    assert_eq!(c.stall_after, Duration::from_secs(30));
    assert_eq!(c.file_interval, Duration::from_secs(60));
    let dest = &system.topology.destinations[0];
    assert_eq!(dest.kind.payload(), Payload::Record);
    assert!(
        compiled.records.contains_key(&dest.id),
        "its records are compiled for Parquet"
    );
    assert!(system.topology.named_secrets.is_empty());
}

/// Every field loads into the topology: client credentials, storage of its own, a partition spec and the limits.
#[test]
fn an_iceberg_destination_loads_with_every_field() {
    use smeltr::plan::topology::{CatalogSignIn, Compression, SecretRef};
    use std::time::Duration;

    let json = iceberg_config(
        r#","warehouse":"lake","partitionBy":["day(ts)","bucket(16, host)"],
           "clientId":"smeltr","clientSecret":{"secret":"catalog","version":2},
           "tokenUrl":"https://idp.example/token","scope":"catalog",
           "region":"eu-west-1","accessKeyId":"AWS_KEY","secretAccessKey":"AWS_SECRET",
           "compression":"snappy","inFlight":2,"maxObjectBytes":"128 MiB","bufferBytes":"512 MiB",
           "stallAfterSecs":60,"fileIntervalSecs":15"#,
    );
    assert!(refusal(&json).is_empty(), "{}", refusal(&json));
    let system = load_str(&json).expect("must load");
    let c = iceberg(&system);
    assert_eq!(c.warehouse.as_deref(), Some("lake"));
    assert_eq!(
        c.partition_by,
        vec!["day(ts)".to_string(), "bucket(16, host)".to_string()]
    );
    let secret = SecretRef::LeaderStored {
        name: "catalog".into(),
        version: 2,
    };
    match &c.sign_in {
        CatalogSignIn::ClientCredentials {
            token_url,
            client_id,
            client_secret,
            scope,
        } => {
            assert_eq!(token_url.as_deref(), Some("https://idp.example/token"));
            assert_eq!(client_id, "smeltr");
            assert_eq!(client_secret, &secret);
            assert_eq!(scope.as_deref(), Some("catalog"));
        }
        other => panic!("expected client credentials, got {other:?}"),
    }
    let storage = c.storage.as_ref().expect("storage of its own");
    assert_eq!(storage.region.as_deref(), Some("eu-west-1"));
    let key = storage.access_key.as_ref().expect("an access key");
    assert_eq!(key.id, SecretRef::WorkerLocal("AWS_KEY".into()));
    assert_eq!(c.compression, Compression::Snappy);
    assert_eq!(c.in_flight, 2);
    assert_eq!(c.max_object_bytes, 128 * 1024 * 1024);
    assert_eq!(c.buffer_bytes, 512 * 1024 * 1024);
    assert_eq!(c.stall_after, Duration::from_secs(60));
    assert_eq!(c.file_interval, Duration::from_secs(15));
    let named = &system.topology.named_secrets;
    assert!(
        named.iter().any(|n| n.reference == secret),
        "the client secret is among the bundle's secrets"
    );

    let bearer = iceberg_config(r#","token":{"secret":"catalog","version":2}"#);
    let system = load_str(&bearer).expect("must load");
    assert!(matches!(&iceberg(&system).sign_in, CatalogSignIn::Bearer(r) if *r == secret));

    let signed = iceberg_config(r#","signingName":"glue","region":"us-gov-west-1""#);
    let system = load_str(&signed).expect("must load");
    assert!(matches!(&iceberg(&system).sign_in, CatalogSignIn::SigV4 { signing_name } if signing_name == "glue"));
}

/// A save writes back what was read, and leaves the defaults out.
#[test]
fn an_iceberg_destination_round_trips_and_its_defaults_stay_out_of_the_file() {
    let json = iceberg_config(r#","partitionBy":["host"],"token":"CATALOG_TOKEN","fileIntervalSecs":10"#);
    let file: smeltr::config_file::FileConfig = serde_json::from_str(&json).expect("parses");
    let written = serde_json::to_string(&file).unwrap();
    for absent in [
        "compression",
        "inFlight",
        "maxObjectBytes",
        "bufferBytes",
        "stallAfterSecs",
        "region",
    ] {
        assert!(!written.contains(absent), "{absent}: {written}");
    }
    let again: smeltr::config_file::FileConfig = serde_json::from_str(&written).expect("parses");
    assert_eq!(serde_json::to_string(&again).unwrap(), written);
    let system = load_str(&written).expect("must load");
    assert_eq!(iceberg(&system).partition_by, vec!["host".to_string()]);
    assert_eq!(iceberg(&system).file_interval, std::time::Duration::from_secs(10));
}

/// Each setting that cannot reach or write the table is refused at the field that holds it.
#[test]
fn an_iceberg_destination_with_bad_settings_is_refused() {
    let secret = r#"{"secret":"c","version":1}"#;
    for (extra, field, says) in [
        (
            r#","partitionBy":["week(ts)"]"#.to_string(),
            "partitionBy",
            "is no partition term",
        ),
        (
            format!(r#","token":{secret},"clientId":"me","clientSecret":{secret}"#),
            "token",
            "not both",
        ),
        (
            r#","clientId":"me""#.to_string(),
            "clientSecret",
            "half of its client credentials",
        ),
        (
            format!(r#","clientSecret":{secret}"#),
            "clientId",
            "half of its client credentials",
        ),
        (
            format!(r#","clientId":"me","clientSecret":{secret},"tokenUrl":"idp.example""#),
            "tokenUrl",
            "give a whole http:// or https:// URL",
        ),
        (
            r#","fileIntervalSecs":4"#.to_string(),
            "fileIntervalSecs",
            "between 5 and 240",
        ),
        (
            r#","fileIntervalSecs":241"#.to_string(),
            "fileIntervalSecs",
            "between 5 and 240",
        ),
        (
            r#","accessKeyId":"AWS_KEY""#.to_string(),
            "secretAccessKey",
            "half an access key",
        ),
        (
            format!(r#","signingName":"glue","token":{secret}"#),
            "signingName",
            "takes neither",
        ),
        (r#","signingName":" ""#.to_string(), "signingName", "empty signingName"),
        (
            r#","signingName":"Glue""#.to_string(),
            "signingName",
            "glue or s3tables",
        ),
        (r#","signingName":"glue""#.to_string(), "region", "names no AWS region"),
    ] {
        let json = iceberg_config(&extra);
        assert!(refused_at(&json, field, says), "{extra}: {}", refusal(&json));
    }

    let place = |catalog: &str, namespace: &str, table: &str, extra: &str| {
        cfg(
            &format!(
                r#"{{"id":"table","type":"iceberg","catalog":"{catalog}","namespace":"{namespace}",
                    "table":"{table}"{extra}}}"#
            ),
            EMPTY_PIPELINE,
            r#"{"id":"r","source":"s","pipeline":"p","deliver":[{"to":["table"]}]}"#,
        )
    };
    let at = "https://catalog.example";
    for (json, field, says) in [
        (
            place("catalog.example", "logs", "t", ""),
            "catalog",
            "give a whole http:// or https:// URL",
        ),
        (place(at, "logs..web", "t", ""), "namespace", "has an empty level"),
        (place(at, "", "t", ""), "namespace", "names no namespace"),
        (place(at, "logs", " ", ""), "table", "names no table"),
    ] {
        assert!(refused_at(&json, field, says), "{json}: {}", refusal(&json));
    }
    // A catalog at its AWS address names the region its signatures are for.
    let json = place(
        "https://glue.us-gov-west-1.amazonaws.com/iceberg",
        "logs",
        "t",
        r#","signingName":"glue""#,
    );
    assert!(refusal(&json).is_empty(), "{json}: {}", refusal(&json));
    // On this host, plain HTTP shows a credential to no one.
    let json = place("http://localhost:8181", "logs", "t", r#","token":"CATALOG_TOKEN""#);
    assert!(refusal(&json).is_empty(), "{json}: {}", refusal(&json));
    // Every codec writes Parquet.
    for codec in ["none", "gzip", "snappy", "lz4", "zstd"] {
        let json = iceberg_config(&format!(r#","compression":"{codec}""#));
        assert!(refusal(&json).is_empty(), "{codec}: {}", refusal(&json));
    }
}

fn objects(system: &smeltr::config_file::LoadedSystem) -> &smeltr::plan::topology::ObjectsDestinationConfig {
    match &system.topology.destinations[0].kind {
        smeltr::plan::topology::DestinationKind::Objects(c) => c,
        other => panic!("expected an object store destination, got {other:?}"),
    }
}

fn s3(system: &smeltr::config_file::LoadedSystem) -> &smeltr::plan::topology::S3Store {
    match &objects(system).cloud {
        smeltr::plan::topology::Cloud::S3(c) => c,
        other => panic!("expected S3, got {other:?}"),
    }
}

/// One object store destination, `fields` its own, with `extra` spliced into its object.
fn store_config(fields: &str, extra: &str) -> String {
    cfg(
        &format!(r#"{{"id":"lake",{fields}{extra}}}"#),
        EMPTY_PIPELINE,
        r#"{"id":"r","source":"s","pipeline":"p","deliver":[{"to":["lake"]}]}"#,
    )
}

/// One S3 destination, with `extra` spliced into its object.
fn s3_config(extra: &str) -> String {
    store_config(r#""type":"s3","bucket":"logs""#, extra)
}

fn gcs_config(extra: &str) -> String {
    store_config(r#""type":"gcs","bucket":"logs""#, extra)
}

fn azure_config(extra: &str) -> String {
    store_config(r#""type":"azureBlob","account":"acme","container":"logs""#, extra)
}

/// An S3 destination needs only its bucket: it signs with the host's credentials, and writes gzipped NDJSON under
/// hourly folders.
#[test]
fn a_minimal_s3_destination_loads_with_the_defaults() {
    use smeltr::plan::topology::{Compression, ObjectFormat};

    let json = s3_config("");
    let system = load_str(&json).expect("must load");
    system.compile_topology().expect("must compile");
    let c = objects(&system);
    assert_eq!(c.prefix, "%Y/%m/%d/%H/");
    assert_eq!(c.format, ObjectFormat::Ndjson);
    assert_eq!(c.compression, Compression::Gzip);
    let s3 = s3(&system);
    assert_eq!(s3.bucket, "logs");
    assert!(s3.access.access_key.is_none() && s3.access.role.is_none());
    assert!(system.topology.named_secrets.is_empty());
    assert!(refusal(&json).is_empty(), "{}", refusal(&json));
}

/// Both halves of an access key are named, never valued, and a role is assumed with its external ID.
#[test]
fn an_s3_access_key_is_two_secrets_and_a_role_is_assumed_with_it() {
    let json = s3_config(
        r#","accessKeyId":"AWS_KEY_ID","secretAccessKey":{"secret":"aws-secret","version":2},
           "roleArn":"arn:aws:iam::123456789012:role/writer","externalId":"x-1",
           "format":"text","field":"message","compression":"zstd",
           "storageClass":"STANDARD_IA","kmsKeyId":"alias/logs","prefix":"fw/%Y-%m-%d/""#,
    );
    let system = load_str(&json).expect("must load");
    assert!(refusal(&json).is_empty(), "{}", refusal(&json));
    let role = s3(&system).access.role.as_ref().unwrap();
    assert_eq!(role.external_id.as_deref(), Some("x-1"));
    assert_eq!(
        objects(&system).format,
        smeltr::plan::topology::ObjectFormat::Text {
            field: "message".into()
        }
    );
    let named = &system.topology.named_secrets;
    let named: Vec<&str> = named.iter().map(|n| n.what.as_str()).collect();
    assert_eq!(
        named,
        [
            r#"destination "lake": accessKeyId"#,
            r#"destination "lake": secretAccessKey"#
        ]
    );
}

/// A destination writing a field's bytes takes that field from every source that reaches it, by a route or down a
/// DLQ chain, so loading refuses one a source does not declare as bytes.
#[test]
fn a_bytes_destination_writes_a_bytes_field_its_sources_declare() {
    let kafka = |id: &str, format: &str| {
        format!(r#"{{"id":"{id}","type":"kafka","brokers":"b:9092","defaultTopic":"o"{format}}}"#)
    };
    let delivered = route_with(r#","deliver":[{"to":["main"]}]"#);
    let one = |format: &str| refusal(&cfg(&kafka("main", format), EMPTY_PIPELINE, &delivered));

    assert_eq!(one(r#","format":"bytes""#), "");
    assert_eq!(one(r#","format":"bytes","field":"__source.bytes""#), "");
    let e = one(r#","format":"bytes","field":"_raw""#);
    assert!(
        e.contains("source `s` reaches destination `main`, which writes the bytes of `_raw`, which `s` declares as str, not bytes"),
        "{e}"
    );
    let e = one(r#","format":"bytes","field":"payload""#);
    assert!(
        e.contains("the bytes of `payload`, a field `s` does not declare"),
        "{e}"
    );

    let main = kafka("main", &format!(",{}", refused(r#"{"deliver":["dlq"]}"#)));
    let dlq = kafka("dlq", r#","format":"bytes","field":"payload""#);
    let e = refusal(&cfg(&format!("{main},{dlq}"), EMPTY_PIPELINE, &delivered));
    assert!(
        e.contains("reaches destination `dlq`, which writes the bytes of `payload`"),
        "{e}"
    );
}

#[test]
fn an_s3_destination_with_bad_settings_is_refused() {
    let e = refusal(&s3_config(r#","accessKeyId":"AWS_KEY_ID""#));
    assert!(e.contains("half an access key"), "{e}");
    let e = refusal(&s3_config(r#","externalId":"x""#));
    assert!(e.contains("externalId or authEndpoint and no roleArn"), "{e}");
    let e = refusal(&s3_config(r#","authEndpoint":"https://sts.internal""#));
    assert!(e.contains("externalId or authEndpoint and no roleArn"), "{e}");
    let e = refusal(&s3_config(r#","prefix":"%Y/%j/""#));
    assert!(e.contains("%Y %m %d %H %M"), "{e}");
    let e = refusal(&s3_config(r#","prefix":"/abs/""#));
    assert!(e.contains("does not start with /"), "{e}");
    let e = refusal(&s3_config(r#","field":"msg""#));
    assert!(e.contains("only the \"text\" and \"bytes\" formats read"), "{e}");
    let e = refusal(&s3_config(r#","newlines":"refuse""#));
    assert!(e.contains("only the \"bytes\" format reads"), "{e}");
    let e = refusal(&s3_config(r#","storageClass":"COLD""#));
    assert!(e.contains("COLD"), "{e}");
    let e = refusal(&s3_config(r#","storageClass":"GLACIER_IR""#));
    assert!(e.is_empty(), "{e}");
    let blank = cfg(
        r#"{"id":"lake","type":"s3","bucket":" "}"#,
        EMPTY_PIPELINE,
        r#"{"id":"r","source":"s","pipeline":"p","deliver":[{"to":["lake"]}]}"#,
    );
    let e = refusal(&blank);
    assert!(e.contains("names no bucket"), "{e}");
}

/// An object file stays open for a minute unless `fileIntervalSecs` says otherwise, within its bounds.
#[test]
fn an_object_destinations_file_interval_defaults_and_is_bounded() {
    use std::time::Duration;
    let system = load_str(&s3_config(r#","format":"parquet""#)).expect("must load");
    assert_eq!(objects(&system).file_interval, Duration::from_secs(60));
    let json = s3_config(r#","format":"parquet","fileIntervalSecs":5"#);
    assert!(refusal(&json).is_empty(), "{}", refusal(&json));
    assert_eq!(objects(&load_str(&json).unwrap()).file_interval, Duration::from_secs(5));
    let json = s3_config(r#","format":"parquet","fileIntervalSecs":3600"#);
    assert!(refusal(&json).is_empty(), "{}", refusal(&json));
    for out in [4, 3601] {
        let json = s3_config(&format!(r#","format":"parquet","fileIntervalSecs":{out}"#));
        assert!(
            refused_at(&json, "fileIntervalSecs", "it must be between 5 and 3600"),
            "{out}: {}",
            refusal(&json)
        );
    }
    // Every format keeps its files open across batches.
    let json = s3_config(r#","format":"ndjson","fileIntervalSecs":30"#);
    assert!(refusal(&json).is_empty(), "{}", refusal(&json));
    assert_eq!(
        objects(&load_str(&json).unwrap()).file_interval,
        Duration::from_secs(30)
    );
}

/// An object destination's files default to 64 MiB and its memory to 256 MiB, which holds such a file's records before
/// compression; memory reaches as far as the largest machine holds, and need not hold a whole file, which uploads in parts.
#[test]
fn an_object_destinations_buffer_and_file_size_defaults() {
    let (mib, tib) = (1024 * 1024, 1024usize.pow(4));
    let system = load_str(&s3_config("")).expect("must load");
    assert_eq!(objects(&system).buffer_bytes, 256 * mib);
    assert_eq!(objects(&system).max_object_bytes, 64 * mib);
    let json = s3_config(r#","bufferBytes":"32 TiB","maxObjectBytes":"64 GiB""#);
    assert!(refusal(&json).is_empty(), "{}", refusal(&json));
    assert_eq!(objects(&load_str(&json).unwrap()).buffer_bytes, 32 * tib);
    let json = s3_config(r#","bufferBytes":"64 MiB","maxObjectBytes":"1 GiB""#);
    assert!(refusal(&json).is_empty(), "{}", refusal(&json));
    let json = s3_config(r#","bufferBytes":"33 TiB""#);
    assert!(
        refused_at(&json, "bufferBytes", "it must be between"),
        "{}",
        refusal(&json)
    );
    let json = s3_config(r#","maxObjectBytes":"65 GiB""#);
    assert!(
        refused_at(&json, "maxObjectBytes", "it must be between"),
        "{}",
        refusal(&json)
    );
}

/// An S3 endpoint over plain HTTP loads, on this host or off it.
#[test]
fn an_s3_endpoint_over_plain_http_loads() {
    let e = refusal(&s3_config(r#","endpoint":"http://minio.internal:9000""#));
    assert!(e.is_empty(), "{e}");
    let e = refusal(&s3_config(r#","endpoint":"http://localhost:4566""#));
    assert!(e.is_empty(), "{e}");
    let e = refusal(&s3_config(
        r#","roleArn":"arn:aws:iam::1:role/w","authEndpoint":"http://sts.internal""#,
    ));
    assert!(e.is_empty(), "{e}");
    let e = refusal(&s3_config(
        r#","roleArn":"arn:aws:iam::1:role/w","authEndpoint":"HTTP://127.0.0.1:4566""#,
    ));
    assert!(e.is_empty(), "{e}");
}

/// A route or a destination that inherits the group's impossible answer leaves it to the group's one error.
#[test]
fn an_inherited_impossible_answer_is_reported_once() {
    let json = r#"{
      "version": 1,
      "exceptions": {"categories": {"delivery": "tag"}},
      "sources": [{"id":"s","type":"kafka","brokers":"b:9092","topics":["t"],"groupId":"g"}],
      "destinations": [
        {"id":"main","type":"kafka","brokers":"b:9092","defaultTopic":"o"},
        {"id":"es","type":"elasticsearch","url":"http://es:9200","defaultIndex":"i"}
      ],
      "pipelines": [{"id":"p","name":"p","steps":[]}],
      "routes": [
        {"id":"a","source":"s","pipeline":"p","deliver":[{"to":["main"]}]},
        {"id":"b","source":"s","pipeline":"p","deliver":[{"to":["main"]}]}
      ]
    }"#;
    let err = load_str(json).unwrap_err().to_string();
    assert_eq!(
        err.matches("with tag").count(),
        3,
        "once per delivery kind, the group's: {err}"
    );
    assert!(!err.contains("route \"") && !err.contains("destination \""), "{err}");
}

/// GCS and Azure take the settings S3 does, and the host's credentials when the bundle names none.
#[test]
fn gcs_and_azure_destinations_load_with_the_shared_settings() {
    use smeltr::plan::topology::{AzureCredential, Cloud};

    for json in [
        gcs_config(r#","prefix":"fw/","storageClass":"NEARLINE""#),
        azure_config(r#","prefix":"fw/","storageClass":"Cool""#),
    ] {
        let system = load_str(&json).expect("must load");
        system.compile_topology().expect("must compile");
        assert!(refusal(&json).is_empty(), "{}", refusal(&json));
        assert_eq!(objects(&system).prefix, "fw/");
        assert!(system.topology.named_secrets.is_empty());
    }

    let json = gcs_config(r#","serviceAccountKey":{"secret":"gcs-key","version":1}"#);
    let system = load_str(&json).expect("must load");
    assert!(matches!(&objects(&system).cloud, Cloud::Gcs(g) if g.access.service_account_key.is_some()));
    let named: Vec<String> = system.topology.named_secrets.iter().map(|n| n.what.clone()).collect();
    assert_eq!(named, [r#"destination "lake": serviceAccountKey"#]);

    let json = azure_config(r#","tenantId":"t","clientId":"c","clientSecret":"AZURE_SECRET""#);
    let system = load_str(&json).expect("must load");
    assert!(refusal(&json).is_empty(), "{}", refusal(&json));
    assert!(matches!(
        &objects(&system).cloud,
        Cloud::Azure(a) if matches!(&a.access.credential, Some(AzureCredential::ClientSecret { tenant_id, .. }) if tenant_id == "t")
    ));
}

#[test]
fn gcs_and_azure_destinations_with_bad_settings_are_refused() {
    let e = refusal(&gcs_config(r#","storageClass":"GLACIER""#));
    assert!(e.contains("none of STANDARD, NEARLINE, COLDLINE, ARCHIVE"), "{e}");
    let e = refusal(&gcs_config(r#","prefix":"%j/""#));
    assert!(e.contains("%Y %m %d %H %M"), "{e}");
    let e = refusal(&store_config(r#""type":"gcs","bucket":"""#, ""));
    assert!(e.contains("names no bucket"), "{e}");

    let e = refusal(&azure_config(r#","storageClass":"COOL""#));
    assert!(e.contains("none of Hot, Cool, Cold, Archive"), "{e}");
    let e = refusal(&azure_config(r#","accessKey":"KEY","sasToken":"SAS""#));
    assert!(e.contains("more than one credential"), "{e}");
    let e = refusal(&azure_config(r#","tenantId":"t","clientSecret":"S""#));
    assert!(e.contains("part of a service principal"), "{e}");
    let e = refusal(&azure_config(
        r#","accessKey":"KEY","managedIdentityClientId":"72f988bf-86f1-41af-91ab-2d7cd011db47""#,
    ));
    assert!(e.contains("more than one credential"), "{e}");
    let e = refusal(&azure_config(r#","managedIdentityClientId":"writer""#));
    assert!(e.contains("a GUID"), "{e}");
    let e = refusal(&azure_config(r#","authEndpoint":"https://login.microsoftonline.us""#));
    assert!(e.contains("authEndpoint and no clientSecret"), "{e}");
    let e = refusal(&store_config(
        r#""type":"azureBlob","account":"","container":"logs""#,
        "",
    ));
    assert!(e.contains("names no account"), "{e}");
    let e = serde_json::from_str::<smeltr::config_file::FileConfig>(&azure_config(r#","bucket":"logs""#)).unwrap_err();
    assert!(e.to_string().contains("unknown field `bucket`"), "{e}");
}

/// GCS and Azure endpoints over plain HTTP load, on this host or off it.
#[test]
fn gcs_and_azure_endpoints_over_plain_http_load() {
    let e = refusal(&gcs_config(r#","endpoint":"http://gcs.internal""#));
    assert!(e.is_empty(), "{e}");
    let e = refusal(&gcs_config(r#","endpoint":"http://localhost:4443""#));
    assert!(e.is_empty(), "{e}");
    let e = refusal(&azure_config(r#","endpoint":"http://blob.internal/acme""#));
    assert!(e.is_empty(), "{e}");
    let e = refusal(&azure_config(
        r#","tenantId":"t","clientId":"c","clientSecret":"S","authEndpoint":"http://login.internal""#,
    ));
    assert!(e.is_empty(), "{e}");
    let e = refusal(&azure_config(
        r#","endpoint":"http://127.0.0.1:10000/devstoreaccount1""#,
    ));
    assert!(e.is_empty(), "{e}");
}

/// One SQS/S3 source, with `extra` spliced into its object.
fn s3_sqs_config(extra: &str) -> String {
    format!(
        r#"{{"version":1,
            "sources":[{{"id":"q","type":"s3Sqs","queueUrl":"https://sqs.us-east-1.amazonaws.com/123456789012/logs"{extra}}}]}}"#
    )
}

fn object_source(system: &smeltr::config_file::LoadedSystem) -> &smeltr::plan::topology::ObjectSourceConfig {
    match &system.topology.sources[0].kind {
        smeltr::plan::topology::SourceKind::Objects(o) => o,
        other => panic!("expected an object source, got {other:?}"),
    }
}

/// An SQS/S3 source's queue URL and access block.
fn sqs_of(o: &smeltr::plan::topology::ObjectSourceConfig) -> (&str, &smeltr::plan::topology::S3Access) {
    match &o.queue {
        smeltr::plan::topology::ObjectQueue::Sqs { queue_url, access } => (queue_url, access),
        other => panic!("expected SQS, got {other:?}"),
    }
}

/// An SQS/S3 source needs only its queue: it signs with the host's credentials and reads each object a line at a
/// time.
#[test]
fn a_minimal_s3_sqs_source_loads_with_the_defaults() {
    use smeltr::plan::topology::Framing;

    let json = s3_sqs_config("");
    assert!(refusal(&json).is_empty(), "{}", refusal(&json));
    let system = load_str(&json).expect("must load");
    let o = object_source(&system);
    let (queue_url, access) = sqs_of(o);
    assert_eq!(queue_url, "https://sqs.us-east-1.amazonaws.com/123456789012/logs");
    assert_eq!(o.framing, Framing::Lines);
    assert_eq!(o.visibility, std::time::Duration::from_secs(60));
    assert!(access.access_key.is_none() && access.role.is_none() && o.tls.is_none());
    assert!(system.topology.named_secrets.is_empty());
}

#[test]
fn an_s3_sqs_source_loads_with_every_field() {
    use smeltr::plan::topology::Framing;

    let json = s3_sqs_config(
        r#","region":"eu-west-1","endpoint":"https://s3.internal","authEndpoint":"https://sts.internal",
           "accessKeyId":"AWS_KEY_ID","secretAccessKey":{"secret":"aws-secret","version":2},
           "roleArn":"arn:aws:iam::123456789012:role/reader","externalId":"x-1",
           "certificate":"acme","framing":"object","encoding":"windows-1252","visibilitySecs":300"#,
    );
    assert!(refusal(&json).is_empty(), "{}", refusal(&json));
    let system = load_str(&json).expect("must load");
    let o = object_source(&system);
    let (_, access) = sqs_of(o);
    assert_eq!(access.region.as_deref(), Some("eu-west-1"));
    assert_eq!(access.endpoint.as_deref(), Some("https://s3.internal"));
    assert_eq!(access.auth_endpoint.as_deref(), Some("https://sts.internal"));
    assert_eq!(access.role.as_ref().unwrap().external_id.as_deref(), Some("x-1"));
    assert_eq!(o.tls.as_ref().unwrap().certificate.as_deref(), Some("acme"));
    assert_eq!(o.framing, Framing::Object);
    assert_eq!(o.visibility, std::time::Duration::from_secs(300));
    let named: Vec<String> = system.topology.named_secrets.iter().map(|n| n.what.clone()).collect();
    assert_eq!(named.len(), 2, "{named:?}");
}

/// A queue URL pasted with blanks around it loads without them, so the queue's client is built from the URL itself.
#[test]
fn an_s3_sqs_queue_url_loads_trimmed() {
    let json = r#"{"version":1,
        "sources":[{"id":"q","type":"s3Sqs","queueUrl":" https://sqs.us-east-1.amazonaws.com/123456789012/logs "}]}"#;
    assert!(refusal(json).is_empty(), "{}", refusal(json));
    let system = load_str(json).expect("must load");
    let (queue_url, _) = sqs_of(object_source(&system));
    assert_eq!(queue_url, "https://sqs.us-east-1.amazonaws.com/123456789012/logs");
}

/// A certificate is refused on an object source whose every address is plain HTTP, where nothing it holds is used,
/// and kept while any one of them negotiates TLS.
#[test]
fn a_certificate_on_an_object_source_reached_over_plain_http_only_is_refused() {
    let plain = r#"{"version":1,"sources":[{"id":"q","type":"s3Sqs","queueUrl":"http://localhost:4566/000000000000/logs",
            "endpoint":"http://localhost:4566","certificate":"acme"}]}"#;
    let why = r#"source "q" sets TLS and is reached over "http://localhost:4566/000000000000/logs""#;
    assert!(refused_at(plain, "queueUrl", why), "{}", refusal(plain));
    let one_tls = s3_sqs_config(r#","endpoint":"http://localhost:4566","certificate":"acme""#);
    assert!(refusal(&one_tls).is_empty(), "{}", refusal(&one_tls));
}

/// A line breaker cuts an object into events at its first capture group, which is dropped.
#[test]
fn a_line_breaker_loads_and_writes_back_as_it_was_given() {
    use smeltr::plan::topology::Framing;

    let json = s3_sqs_config(r#","lineBreaker":"([\\r\\n]+)\\d{4}-""#);
    assert!(refusal(&json).is_empty(), "{}", refusal(&json));
    let system = load_str(&json).expect("must load");
    let Framing::LineBreaker(breaker) = &object_source(&system).framing else {
        panic!("a line breaker")
    };
    assert_eq!(breaker.0.as_str(), r"([\r\n]+)\d{4}-");
    let given: serde_json::Value = serde_json::from_str::<serde_json::Value>(&json).unwrap()["sources"][0].clone();
    let file: smeltr::config_file::FileConfig = serde_json::from_str(&json).unwrap();
    let written = &serde_json::to_value(&file).unwrap()["sources"][0];
    assert_eq!(written, &given, "lines framing, the default, stays out of the file");
    assert_eq!(written.get("framing"), None);
}

/// A line breaker is refused when it cannot say where an event ends: blank, not a pattern, without the group that is
/// the break, or matching no bytes at all and so breaking between every byte; and on a framing that does not cut lines.
#[test]
fn a_bad_line_breaker_is_refused() {
    for (breaker, why) in [
        (r#"" ""#, r#"source "q" names a blank lineBreaker pattern"#),
        (
            r#""(""#,
            r#"source "q" ends an event where "(" matches, which is not a pattern"#,
        ),
        (r#""\\n""#, "which has no capture group to mark the break"),
        (
            r#""(\\n*)""#,
            "which can match no bytes at all, and so would break between every byte",
        ),
    ] {
        let json = s3_sqs_config(&format!(r#","lineBreaker":{breaker}"#));
        assert!(refused_at(&json, "lineBreaker", why), "{breaker}: {}", refusal(&json));
    }
    for framing in ["object", "escapedLines"] {
        let json = s3_sqs_config(&format!(r#","framing":"{framing}","lineBreaker":"(\\n)""#));
        let why = r#"source "q" sets lineBreaker, which only lines framing reads"#;
        assert!(refused_at(&json, "lineBreaker", why), "{framing}: {}", refusal(&json));
    }
    for unread in [r#","framing":"multiline""#, r#","eventStart":"^x""#, r#","maxLines":5"#] {
        let e = serde_json::from_str::<smeltr::config_file::FileConfig>(&s3_sqs_config(unread)).unwrap_err();
        assert!(e.to_string().contains("unknown"), "{unread}: {e}");
    }
}

#[test]
fn an_s3_sqs_source_with_bad_settings_is_refused() {
    let e = refusal(&s3_sqs_config(r#","accessKeyId":"AWS_KEY_ID""#));
    assert!(e.contains(r#"source "q" names half an access key"#), "{e}");
    let e = refusal(&s3_sqs_config(r#","externalId":"x""#));
    assert!(e.contains("externalId or authEndpoint and no roleArn"), "{e}");
    let e = refusal(&s3_sqs_config(r#","authEndpoint":"https://sts.internal""#));
    assert!(e.contains("externalId or authEndpoint and no roleArn"), "{e}");
    let e = refusal(&s3_sqs_config(r#","certificate":" ""#));
    assert!(e.contains("blank name"), "{e}");
    let e = refusal(&s3_sqs_config(r#","encoding":"klingon""#));
    assert!(e.contains("not a name smeltr knows"), "{e}");
    for secs in ["0", "43201"] {
        let e = refusal(&s3_sqs_config(&format!(r#","visibilitySecs":{secs}"#)));
        assert!(e.contains("between 1 and 43200"), "{e}");
    }
    let e = refusal(&s3_sqs_config(r#","visibilitySecs":43200"#));
    assert!(e.is_empty(), "{e}");
    let e = refusal(&s3_sqs_config("").replace("https://sqs.us-east-1.amazonaws.com/123456789012/logs", " "));
    assert!(e.contains("names no queueUrl"), "{e}");
    let e = refusal(&s3_sqs_config("").replace("https://sqs.us-east-1.amazonaws.com/123456789012/logs", "logs"));
    assert!(
        e.contains(r#"has queueUrl "logs"; give a whole http:// or https:// URL"#),
        "{e}"
    );
    for unknown in [r#","bucket":"logs""#, r#","kmsKeyId":"k""#] {
        let e = serde_json::from_str::<smeltr::config_file::FileConfig>(&s3_sqs_config(unknown)).unwrap_err();
        assert!(e.to_string().contains("unknown field"), "{e}");
    }
}

/// Two flattened blocks still leave no field to fall through unread.
#[test]
fn an_s3_destination_refuses_a_field_neither_of_its_blocks_knows() {
    let e =
        serde_json::from_str::<smeltr::config_file::FileConfig>(&s3_config(r#","queueUrl":"https://q""#)).unwrap_err();
    assert!(e.to_string().contains("unknown field `queueUrl`"), "{e}");
}

/// The queue, the store and the token service each load over plain HTTP, on this host or off it.
#[test]
fn an_s3_sqs_source_over_plain_http_loads() {
    let e = refusal(&s3_sqs_config("").replace("https://sqs", "http://sqs"));
    assert!(e.is_empty(), "{e}");
    let e = refusal(&s3_sqs_config("").replace(
        "https://sqs.us-east-1.amazonaws.com/123456789012/logs",
        "http://localhost:4566/000000000000/logs",
    ));
    assert!(e.is_empty(), "{e}");
    let e = refusal(&s3_sqs_config(r#","endpoint":"http://minio.internal:9000""#));
    assert!(e.is_empty(), "{e}");
    let e = refusal(&s3_sqs_config(
        r#","roleArn":"arn:aws:iam::1:role/r","authEndpoint":"http://sts.internal""#,
    ));
    assert!(e.is_empty(), "{e}");
    let e = refusal(&s3_sqs_config(r#","endpoint":"http://127.0.0.1:4566""#));
    assert!(e.is_empty(), "{e}");
}

/// One Listing/S3 source, with `extra` spliced into its object.
fn s3_list_config(extra: &str) -> String {
    format!(
        r#"{{"version":1,
            "sources":[{{"id":"l","type":"s3List","bucket":"acme-logs"{extra}}}]}}"#
    )
}

/// A listing source's queue: where it lists and the settings that make it a listing.
fn listing_of(o: &smeltr::plan::topology::ObjectSourceConfig) -> &smeltr::plan::topology::ObjectQueue {
    match &o.queue {
        q @ smeltr::plan::topology::ObjectQueue::List { .. } => q,
        other => panic!("expected a listing, got {other:?}"),
    }
}

/// A Listing/S3 source needs only its bucket: it lists all of it every minute, reads everything, and keeps what it
/// read.
#[test]
fn a_minimal_s3_list_source_loads_with_the_defaults() {
    use smeltr::plan::topology::{AfterCommit, Framing, ListPlace, ListStart, ObjectQueue};

    let json = s3_list_config("");
    assert!(refusal(&json).is_empty(), "{}", refusal(&json));
    let system = load_str(&json).expect("must load");
    let o = object_source(&system);
    let ObjectQueue::List {
        place: ListPlace::S3 { bucket, access },
        prefix,
        poll_interval,
        start,
        after_commit,
    } = listing_of(o)
    else {
        unreachable!()
    };
    assert_eq!(bucket, "acme-logs");
    assert_eq!(prefix, "");
    assert_eq!(*poll_interval, std::time::Duration::from_secs(60));
    assert_eq!(*start, ListStart::Everything);
    assert_eq!(*after_commit, AfterCommit::Keep);
    assert_eq!(o.framing, Framing::Lines);
    assert!(access.access_key.is_none() && access.role.is_none() && o.tls.is_none());
    assert!(system.topology.named_secrets.is_empty());
    assert_eq!(
        system.topology.sources[0].kind.meta(),
        smeltr::plan::topology::SourceMeta::S3_LIST
    );
    assert_eq!(
        system.topology.sources[0].kind.single_worker().as_deref(),
        Some("s3://acme-logs/")
    );
}

#[test]
fn an_s3_list_source_loads_with_every_field() {
    use smeltr::plan::topology::{AfterCommit, Framing, ListPlace, ListStart, ObjectQueue};

    let json = s3_list_config(
        r#","prefix":"app/","region":"eu-west-1","endpoint":"https://s3.internal","authEndpoint":"https://sts.internal",
           "accessKeyId":"AWS_KEY_ID","secretAccessKey":{"secret":"aws-secret","version":2},
           "roleArn":"arn:aws:iam::123456789012:role/reader","externalId":"x-1",
           "certificate":"acme","framing":"object","encoding":"windows-1252","pollIntervalSecs":300,
           "afterCommit":"move","movePrefix":"done/""#,
    );
    assert!(refusal(&json).is_empty(), "{}", refusal(&json));
    let system = load_str(&json).expect("must load");
    let o = object_source(&system);
    let ObjectQueue::List {
        place: ListPlace::S3 { access, .. },
        prefix,
        poll_interval,
        start,
        after_commit,
        ..
    } = listing_of(o)
    else {
        unreachable!()
    };
    assert_eq!(prefix, "app/");
    assert_eq!(access.region.as_deref(), Some("eu-west-1"));
    assert_eq!(access.endpoint.as_deref(), Some("https://s3.internal"));
    assert_eq!(access.role.as_ref().unwrap().external_id.as_deref(), Some("x-1"));
    assert_eq!(*poll_interval, std::time::Duration::from_secs(300));
    assert_eq!(*start, ListStart::Everything);
    assert_eq!(
        *after_commit,
        AfterCommit::Move {
            prefix: "done/".to_string()
        }
    );
    assert_eq!(o.tls.as_ref().unwrap().certificate.as_deref(), Some("acme"));
    assert_eq!(o.framing, Framing::Object);
    let named: Vec<String> = system.topology.named_secrets.iter().map(|n| n.what.clone()).collect();
    assert_eq!(named.len(), 2, "{named:?}");

    let system = load_str(&s3_list_config(r#","start":"new""#)).expect("must load");
    let ObjectQueue::List { start, .. } = listing_of(object_source(&system)) else {
        unreachable!()
    };
    assert_eq!(*start, ListStart::New);
    let system = load_str(&s3_list_config(r#","afterCommit":"delete""#)).expect("must load");
    let ObjectQueue::List { after_commit, .. } = listing_of(object_source(&system)) else {
        unreachable!()
    };
    assert_eq!(*after_commit, AfterCommit::Delete);
}

#[test]
fn an_s3_list_source_with_no_bucket_is_refused() {
    let e = refusal(&s3_list_config("").replace("acme-logs", " "));
    assert!(e.contains(r#"source "l" names no bucket"#), "{e}");
    let e = serde_json::from_str::<smeltr::config_file::FileConfig>(
        r#"{"version":1,"sources":[{"id":"l","type":"s3List"}]}"#,
    )
    .unwrap_err();
    assert!(e.to_string().contains("missing field `bucket`"), "{e}");
}

#[test]
fn an_s3_list_source_with_a_bucket_name_s3_would_not_give_is_refused() {
    for bad in [
        "s3://acme-logs",
        "acme-logs/app",
        "Acme-Logs",
        "ab",
        "-acme",
        "acme.",
        "acme_logs",
    ] {
        let e = refusal(&s3_list_config("").replace("acme-logs", bad));
        assert!(e.contains("which is not a bucket's name"), "{bad}: {e}");
    }
    for good in ["abc", "acme.logs-2", &"a".repeat(63)] {
        let e = refusal(&s3_list_config("").replace("acme-logs", good));
        assert!(e.is_empty(), "{good}: {e}");
    }
    let e = refusal(&s3_list_config("").replace("acme-logs", &"a".repeat(64)));
    assert!(e.contains("which is not a bucket's name"), "{e}");
}

#[test]
fn an_s3_list_source_polling_every_0_seconds_is_refused() {
    let e = refusal(&s3_list_config(r#","pollIntervalSecs":0"#));
    assert!(e.contains("lists its bucket every 0 seconds"), "{e}");
    let e = refusal(&s3_list_config(r#","pollIntervalSecs":1"#));
    assert!(e.is_empty(), "{e}");
}

/// Only the ledger records the first listing as read, so a source that keeps none would read it all again.
#[test]
fn an_s3_list_source_starting_at_new_objects_must_keep_them() {
    for after in [
        r#","afterCommit":"delete""#,
        r#","afterCommit":"move","movePrefix":"done/""#,
    ] {
        let e = refusal(&s3_list_config(&format!(r#","prefix":"app/","start":"new"{after}"#)));
        assert!(e.contains("starts at new objects and does not keep"), "{after}: {e}");
    }
    let e = refusal(&s3_list_config(r#","start":"new","afterCommit":"keep""#));
    assert!(e.is_empty(), "{e}");
}

#[test]
fn an_s3_list_source_moving_with_no_move_prefix_is_refused() {
    let e = refusal(&s3_list_config(r#","prefix":"app/","afterCommit":"move""#));
    assert!(e.contains("names no movePrefix"), "{e}");
    let e = refusal(&s3_list_config(
        r#","prefix":"app/","afterCommit":"move","movePrefix":" ""#,
    ));
    assert!(e.contains("has an empty movePrefix"), "{e}");
}

#[test]
fn an_s3_list_source_naming_a_move_prefix_without_moving_is_refused() {
    for after in ["", r#","afterCommit":"keep""#, r#","afterCommit":"delete""#] {
        let e = refusal(&s3_list_config(&format!(
            r#","prefix":"app/","movePrefix":"done/"{after}"#
        )));
        assert!(e.contains("names a movePrefix and does not move"), "{after}: {e}");
    }
}

/// A moved object under the prefix the source lists would be listed, and read, again.
#[test]
fn an_s3_list_source_moving_into_what_it_lists_is_refused() {
    for (prefix, to) in [
        ("app/", "app/done/"),
        ("app/", "app/"),
        ("app/done/", "app/"),
        ("app", "app-done"),
    ] {
        let e = refusal(&s3_list_config(&format!(
            r#","prefix":"{prefix}","afterCommit":"move","movePrefix":"{to}""#
        )));
        assert!(
            e.contains("so it would list every moved object again"),
            "{prefix} -> {to}: {e}"
        );
    }
    let e = refusal(&s3_list_config(r#","afterCommit":"move","movePrefix":"done/""#));
    assert!(e.contains("overlaps the whole bucket"), "{e}");
    let e = refusal(&s3_list_config(
        r#","prefix":"app/","afterCommit":"move","movePrefix":"done/app/""#,
    ));
    assert!(e.is_empty(), "{e}");
}

/// A moved key is `movePrefix` + what follows `prefix` in it, so the two agree on a trailing `/`.
#[test]
fn an_s3_list_source_moving_across_a_trailing_slash_is_refused() {
    for (prefix, to) in [("incoming", "processed/"), ("in/", "done"), ("", "done")] {
        let e = refusal(&s3_list_config(&format!(
            r#","prefix":"{prefix}","afterCommit":"move","movePrefix":"{to}""#
        )));
        assert!(e.contains("End both in \"/\" or neither"), "{prefix} -> {to}: {e}");
    }
    let e = refusal(&s3_list_config(
        r#","prefix":"incoming","afterCommit":"move","movePrefix":"processed""#,
    ));
    assert!(e.is_empty(), "{e}");
}

#[test]
fn an_s3_list_source_moving_under_no_object_path_is_refused() {
    for to in ["done//", "a//b/", "../done/", "done/./"] {
        let e = refusal(&s3_list_config(&format!(
            r#","prefix":"app/","afterCommit":"move","movePrefix":"{to}""#
        )));
        assert!(e.contains("which is not an object path"), "{to}: {e}");
    }
}

/// A key never starts with `/`, so such a prefix matches nothing.
#[test]
fn an_s3_list_source_with_a_leading_slash_is_refused() {
    let e = refusal(&s3_list_config(r#","prefix":"/app/""#));
    assert!(e.contains(r#"has the prefix "/app/", which starts with "/""#), "{e}");
    let e = refusal(&s3_list_config(
        r#","prefix":"app/","afterCommit":"move","movePrefix":"/done/""#,
    ));
    assert!(e.contains(r#"under "/done/", which starts with "/""#), "{e}");
}

#[test]
fn an_s3_list_source_with_bad_access_identity_or_encoding_is_refused() {
    let e = refusal(&s3_list_config(r#","accessKeyId":"AWS_KEY_ID""#));
    assert!(e.contains(r#"source "l" names half an access key"#), "{e}");
    let e = refusal(&s3_list_config(r#","externalId":"x""#));
    assert!(e.contains("externalId or authEndpoint and no roleArn"), "{e}");
    let e = refusal(&s3_list_config(r#","region":" ""#));
    assert!(e.contains("has an empty region"), "{e}");
    let e = refusal(&s3_list_config(r#","certificate":" ""#));
    assert!(e.contains("blank name"), "{e}");
    let e = refusal(&s3_list_config(r#","encoding":"klingon""#));
    assert!(e.contains("not a name smeltr knows"), "{e}");
    let e = refusal(&s3_list_config(r#","endpoint":"http://minio.internal:9000""#));
    assert!(e.is_empty(), "{e}");
    let e = refusal(&s3_list_config(r#","endpoint":"http://127.0.0.1:9000""#));
    assert!(e.is_empty(), "{e}");
    for unknown in [
        r#","queueUrl":"https://q""#,
        r#","visibilitySecs":60"#,
        r#","afterCommit":"archive""#,
    ] {
        let e = serde_json::from_str::<smeltr::config_file::FileConfig>(&s3_list_config(unknown)).unwrap_err();
        assert!(e.to_string().contains("unknown"), "{unknown}: {e}");
    }
}

/// One Listing/GCS source, with `extra` spliced into its object.
fn gcs_list_config(extra: &str) -> String {
    format!(
        r#"{{"version":1,
            "sources":[{{"id":"l","type":"gcsList","bucket":"acme-logs"{extra}}}]}}"#
    )
}

/// One Listing/Azure Blob source, with `extra` spliced into its object.
fn azure_list_config(extra: &str) -> String {
    format!(
        r#"{{"version":1,
            "sources":[{{"id":"l","type":"azureBlobList","account":"acme","container":"logs"{extra}}}]}}"#
    )
}

/// Whether validating `json` refuses `field` with a message holding `message`.
fn refused_at(json: &str, field: &str, message: &str) -> bool {
    let file: smeltr::config_file::FileConfig = serde_json::from_str(json).expect("parses");
    smeltr::config_file::validation_errors(file)
        .iter()
        .any(|e| e.field == Some(field) && e.message.contains(message))
}

/// A Listing/GCS source needs only its bucket, and takes the listing defaults an `s3List` takes.
#[test]
fn a_minimal_gcs_list_source_loads_with_the_defaults() {
    use smeltr::plan::topology::{AfterCommit, Framing, GcsStore, ListPlace, ListStart, ObjectQueue, SourceMeta};

    let json = gcs_list_config("");
    assert!(refusal(&json).is_empty(), "{}", refusal(&json));
    let system = load_str(&json).expect("must load");
    let o = object_source(&system);
    let ObjectQueue::List {
        place: ListPlace::Gcs(GcsStore { bucket, access }),
        prefix,
        poll_interval,
        start,
        after_commit,
    } = listing_of(o)
    else {
        panic!("expected a GCS listing, got {:?}", o.queue)
    };
    assert_eq!(bucket, "acme-logs");
    assert_eq!(prefix, "");
    assert_eq!(*poll_interval, std::time::Duration::from_secs(60));
    assert_eq!(*start, ListStart::Everything);
    assert_eq!(*after_commit, AfterCommit::Keep);
    assert_eq!(o.framing, Framing::Lines);
    assert!(access.endpoint.is_none() && access.service_account_key.is_none() && o.tls.is_none());
    assert!(system.topology.named_secrets.is_empty());
    assert_eq!(system.topology.sources[0].kind.meta(), SourceMeta::GCS_LIST);
    assert_eq!(
        system.topology.sources[0].kind.single_worker().as_deref(),
        Some("gs://acme-logs/")
    );
}

#[test]
fn a_gcs_list_source_loads_with_every_field() {
    use smeltr::plan::topology::{AfterCommit, Framing, GcsStore, ListPlace, ListStart, ObjectQueue};

    let json = gcs_list_config(
        r#","prefix":"app/","endpoint":"https://storage.example.com",
           "serviceAccountKey":{"secret":"gcs-key","version":2},
           "certificate":"acme","framing":"object","encoding":"windows-1252","pollIntervalSecs":300,
           "afterCommit":"move","movePrefix":"done/""#,
    );
    assert!(refusal(&json).is_empty(), "{}", refusal(&json));
    let system = load_str(&json).expect("must load");
    let o = object_source(&system);
    let ObjectQueue::List {
        place: ListPlace::Gcs(GcsStore { access, .. }),
        prefix,
        poll_interval,
        start,
        after_commit,
    } = listing_of(o)
    else {
        panic!("expected a GCS listing, got {:?}", o.queue)
    };
    assert_eq!(prefix, "app/");
    assert_eq!(access.endpoint.as_deref(), Some("https://storage.example.com"));
    assert!(access.service_account_key.is_some());
    assert_eq!(*poll_interval, std::time::Duration::from_secs(300));
    assert_eq!(*start, ListStart::Everything);
    assert_eq!(
        *after_commit,
        AfterCommit::Move {
            prefix: "done/".to_string()
        }
    );
    assert_eq!(o.tls.as_ref().unwrap().certificate.as_deref(), Some("acme"));
    assert_eq!(o.framing, Framing::Object);
    assert_eq!(
        system.topology.sources[0].kind.single_worker().as_deref(),
        Some("gs://acme-logs/app/")
    );
    let named: Vec<String> = system.topology.named_secrets.iter().map(|n| n.what.clone()).collect();
    assert_eq!(named.len(), 1, "{named:?}");
}

#[test]
fn a_gcs_list_source_with_no_bucket_is_refused() {
    let json = gcs_list_config("").replace("acme-logs", " ");
    assert!(
        refused_at(&json, "bucket", r#"source "l" names no bucket"#),
        "{}",
        refusal(&json)
    );
    let e = serde_json::from_str::<smeltr::config_file::FileConfig>(
        r#"{"version":1,"sources":[{"id":"l","type":"gcsList"}]}"#,
    )
    .unwrap_err();
    assert!(e.to_string().contains("missing field `bucket`"), "{e}");
}

/// A Listing/Azure Blob source needs only its account and container, and takes the listing defaults an `s3List`
/// takes.
#[test]
fn a_minimal_azure_blob_list_source_loads_with_the_defaults() {
    use smeltr::plan::topology::{AfterCommit, AzureStore, Framing, ListPlace, ListStart, ObjectQueue, SourceMeta};

    let json = azure_list_config("");
    assert!(refusal(&json).is_empty(), "{}", refusal(&json));
    let system = load_str(&json).expect("must load");
    let o = object_source(&system);
    let ObjectQueue::List {
        place: ListPlace::Azure(AzureStore { container, access }),
        prefix,
        poll_interval,
        start,
        after_commit,
    } = listing_of(o)
    else {
        panic!("expected an Azure listing, got {:?}", o.queue)
    };
    assert_eq!(container, "logs");
    assert_eq!(access.account, "acme");
    assert_eq!(prefix, "");
    assert_eq!(*poll_interval, std::time::Duration::from_secs(60));
    assert_eq!(*start, ListStart::Everything);
    assert_eq!(*after_commit, AfterCommit::Keep);
    assert_eq!(o.framing, Framing::Lines);
    assert!(access.credential.is_none() && access.endpoint.is_none() && o.tls.is_none());
    assert!(system.topology.named_secrets.is_empty());
    assert_eq!(system.topology.sources[0].kind.meta(), SourceMeta::AZURE_BLOB_LIST);
    assert_eq!(
        system.topology.sources[0].kind.single_worker().as_deref(),
        Some("azure://acme/logs/")
    );
}

#[test]
fn an_azure_blob_list_source_loads_with_every_field() {
    use smeltr::plan::topology::{
        AfterCommit, AzureCredential, AzureStore, Framing, ListPlace, ListStart, ObjectQueue,
    };

    let json = azure_list_config(
        r#","prefix":"app/","endpoint":"https://acme.blob.core.usgovcloudapi.net",
           "authEndpoint":"https://login.microsoftonline.us",
           "tenantId":"t","clientId":"c","clientSecret":{"secret":"azure-secret","version":2},
           "certificate":"acme","framing":"escapedLines","encoding":"windows-1252","pollIntervalSecs":300,
           "start":"new""#,
    );
    assert!(refusal(&json).is_empty(), "{}", refusal(&json));
    let system = load_str(&json).expect("must load");
    let o = object_source(&system);
    let ObjectQueue::List {
        place: ListPlace::Azure(AzureStore { access, .. }),
        prefix,
        poll_interval,
        start,
        after_commit,
    } = listing_of(o)
    else {
        panic!("expected an Azure listing, got {:?}", o.queue)
    };
    assert_eq!(prefix, "app/");
    assert_eq!(
        access.endpoint.as_deref(),
        Some("https://acme.blob.core.usgovcloudapi.net")
    );
    assert_eq!(
        access.auth_endpoint.as_deref(),
        Some("https://login.microsoftonline.us")
    );
    assert!(matches!(&access.credential, Some(AzureCredential::ClientSecret { client_id, .. }) if client_id == "c"));
    assert_eq!(*poll_interval, std::time::Duration::from_secs(300));
    assert_eq!(*start, ListStart::New);
    assert_eq!(*after_commit, AfterCommit::Keep);
    assert_eq!(o.tls.as_ref().unwrap().certificate.as_deref(), Some("acme"));
    assert_eq!(o.framing, Framing::EscapedLines);
    assert_eq!(
        system.topology.sources[0].kind.single_worker().as_deref(),
        Some("azure://acme/logs/app/")
    );
    let named: Vec<String> = system.topology.named_secrets.iter().map(|n| n.what.clone()).collect();
    assert_eq!(named.len(), 1, "{named:?}");

    let system = load_str(&azure_list_config(r#","accessKey":"AZURE_KEY","afterCommit":"delete""#)).expect("must load");
    let o = object_source(&system);
    let ObjectQueue::List {
        place: ListPlace::Azure(AzureStore { access, .. }),
        after_commit,
        ..
    } = listing_of(o)
    else {
        panic!("expected an Azure listing, got {:?}", o.queue)
    };
    assert!(matches!(&access.credential, Some(AzureCredential::AccessKey(_))));
    assert_eq!(*after_commit, AfterCommit::Delete);
    assert_eq!(system.topology.named_secrets.len(), 1);
}

#[test]
fn an_azure_blob_list_source_with_no_container_is_refused() {
    let json = azure_list_config("").replace(r#""container":"logs""#, r#""container":" ""#);
    assert!(
        refused_at(&json, "container", r#"source "l" names no container"#),
        "{}",
        refusal(&json)
    );
    let e = serde_json::from_str::<smeltr::config_file::FileConfig>(
        r#"{"version":1,"sources":[{"id":"l","type":"azureBlobList","account":"acme"}]}"#,
    )
    .unwrap_err();
    assert!(e.to_string().contains("missing field `container`"), "{e}");
}

/// A server's ed25519 host key as OpenSSH writes it.
const SFTP_HOST_KEY: &str = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAID3o82ivVxzYDbjz/HQTXSvCvoEb7fBpGzXPVbqK+q63";

/// One SFTP source, with `extra` spliced into its object after its host and username.
fn sftp_config(extra: &str) -> String {
    format!(
        r#"{{"version":1,
            "sources":[{{"id":"l","type":"sftp","host":"sftp.example.com","username":"smeltr",
                         "hostKey":"{SFTP_HOST_KEY}"{extra}}}]}}"#
    )
}

/// An SFTP source needs its host, username, host key and one credential, and takes the listing defaults an `s3List`
/// takes.
#[test]
fn a_minimal_sftp_source_loads_with_the_defaults() {
    use smeltr::plan::topology::{
        AfterCommit, Framing, ListPlace, ListStart, ObjectQueue, SftpCredential, SftpStore, SourceMeta,
    };

    let json = sftp_config(r#","password":"SFTP_PASSWORD""#);
    assert!(refusal(&json).is_empty(), "{}", refusal(&json));
    let system = load_str(&json).expect("must load");
    let o = object_source(&system);
    let ObjectQueue::List {
        place: ListPlace::Sftp(SftpStore { directory, access }),
        prefix,
        poll_interval,
        start,
        after_commit,
    } = listing_of(o)
    else {
        panic!("expected an SFTP listing, got {:?}", o.queue)
    };
    assert_eq!(access.host, "sftp.example.com");
    assert_eq!(access.port, 22);
    assert_eq!(access.username, "smeltr");
    assert_eq!(access.host_key, SFTP_HOST_KEY);
    assert!(matches!(access.credential, SftpCredential::Password(_)));
    assert_eq!(directory, "");
    assert_eq!(prefix, "");
    assert_eq!(*poll_interval, std::time::Duration::from_secs(60));
    assert_eq!(*start, ListStart::Everything);
    assert_eq!(*after_commit, AfterCommit::Keep);
    assert_eq!(o.framing, Framing::Lines);
    assert!(o.tls.is_none());
    let named: Vec<String> = system.topology.named_secrets.iter().map(|n| n.what.clone()).collect();
    assert_eq!(named, [r#"source "l": password"#]);
    assert_eq!(system.topology.sources[0].kind.meta(), SourceMeta::SFTP);
    assert_eq!(
        system.topology.sources[0].kind.single_worker().as_deref(),
        Some("sftp://smeltr@sftp.example.com:22/~/")
    );
}

/// A directory reads as one path however its slashes are written, and its place names it apart from the home directory.
#[test]
fn an_sftp_directory_reads_as_one_path() {
    use smeltr::plan::topology::{ListPlace, ObjectQueue, SftpStore};

    for (written, directory, place) in [
        (
            "//srv//export/",
            "/srv/export",
            "sftp://smeltr@sftp.example.com:22/srv/export/",
        ),
        ("/", "/", "sftp://smeltr@sftp.example.com:22/"),
        (
            "incoming//",
            "incoming",
            "sftp://smeltr@sftp.example.com:22/~/incoming/",
        ),
    ] {
        let json = sftp_config(&format!(r#","password":"SFTP_PASSWORD","directory":"{written}""#));
        let system = load_str(&json).expect("must load");
        let ObjectQueue::List {
            place: ListPlace::Sftp(SftpStore { directory: loaded, .. }),
            ..
        } = listing_of(object_source(&system))
        else {
            panic!("expected an SFTP listing")
        };
        assert_eq!(loaded, directory, "{written}");
        assert_eq!(
            system.topology.sources[0].kind.single_worker().as_deref(),
            Some(place),
            "{written}"
        );
    }
}

#[test]
fn an_sftp_source_loads_with_every_field() {
    use smeltr::plan::topology::{AfterCommit, Framing, ListPlace, ListStart, ObjectQueue, SftpCredential, SftpStore};

    let json = sftp_config(
        r#","port":2222,"privateKey":{"secret":"sftp-key","version":2},
           "directory":"/srv/export","prefix":"app/","framing":"object","encoding":"windows-1252",
           "pollIntervalSecs":300,"afterCommit":"move","movePrefix":"done/""#,
    );
    assert!(refusal(&json).is_empty(), "{}", refusal(&json));
    let system = load_str(&json).expect("must load");
    let o = object_source(&system);
    let ObjectQueue::List {
        place: ListPlace::Sftp(SftpStore { directory, access }),
        prefix,
        poll_interval,
        start,
        after_commit,
    } = listing_of(o)
    else {
        panic!("expected an SFTP listing, got {:?}", o.queue)
    };
    assert_eq!(access.port, 2222);
    assert!(matches!(access.credential, SftpCredential::PrivateKey(_)));
    assert_eq!(directory, "/srv/export");
    assert_eq!(prefix, "app/");
    assert_eq!(*poll_interval, std::time::Duration::from_secs(300));
    assert_eq!(*start, ListStart::Everything);
    assert_eq!(
        *after_commit,
        AfterCommit::Move {
            prefix: "done/".to_string()
        }
    );
    assert_eq!(o.framing, Framing::Object);
    assert_eq!(
        system.topology.sources[0].kind.single_worker().as_deref(),
        Some("sftp://smeltr@sftp.example.com:2222/srv/export/app/")
    );
    let named: Vec<String> = system.topology.named_secrets.iter().map(|n| n.what.clone()).collect();
    assert_eq!(named, [r#"source "l": privateKey"#]);
}

#[test]
fn an_sftp_source_with_bad_access_is_refused() {
    let password = r#","password":"SFTP_PASSWORD""#;
    let json = sftp_config(password).replace(r#""host":"sftp.example.com""#, r#""host":" ""#);
    assert!(
        refused_at(&json, "host", r#"source "l" names no host"#),
        "{}",
        refusal(&json)
    );
    let json = sftp_config(password).replace(r#""username":"smeltr""#, r#""username":"""#);
    assert!(
        refused_at(&json, "username", r#"source "l" names no username"#),
        "{}",
        refusal(&json)
    );
    for credentials in ["", r#","password":"SFTP_PASSWORD","privateKey":"SFTP_KEY""#] {
        let json = sftp_config(credentials);
        assert!(
            refused_at(&json, "password", "exactly one of password and privateKey"),
            "{credentials}: {}",
            refusal(&json)
        );
    }
    for host_key in [
        "",
        "not-a-key",
        "ssh-ed25519 AAAA",
        // A known_hosts line, host name and all.
        &format!("sftp.example.com {SFTP_HOST_KEY}"),
        // An ed25519 key named as another type.
        &SFTP_HOST_KEY.replace("ssh-ed25519", "ssh-rsa"),
    ] {
        let json = sftp_config(password).replace(SFTP_HOST_KEY, host_key);
        assert!(
            refused_at(&json, "hostKey", "not a public key as OpenSSH writes it"),
            "{host_key}: {}",
            refusal(&json)
        );
    }
    let json = sftp_config(&format!(r#"{password},"port":0"#));
    assert!(refused_at(&json, "port", "has port 0"), "{}", refusal(&json));
    let json = sftp_config(&format!(r#"{password},"port":70000"#));
    assert!(
        refused_at(&json, "port", "has port 70000; name the server's port, 1 to 65535"),
        "{}",
        refusal(&json)
    );
    let e = serde_json::from_str::<smeltr::config_file::FileConfig>(
        r#"{"version":1,"sources":[{"id":"l","type":"sftp","host":"h","username":"u","password":"P"}]}"#,
    )
    .unwrap_err();
    assert!(e.to_string().contains("missing field `hostKey`"), "{e}");
}

fn sftp_destination_config(extra: &str) -> String {
    store_config(
        &format!(
            r#""type":"sftp","host":"sftp.example.com","username":"smeltr","hostKey":"{SFTP_HOST_KEY}",
               "privateKey":"SFTP_KEY""#
        ),
        extra,
    )
}

/// An SFTP destination writes the object formats into one directory, signing in as the SFTP source does.
#[test]
fn an_sftp_destination_loads_as_an_object_store() {
    use smeltr::plan::topology::{Cloud, SftpCredential};

    let json = sftp_destination_config(r#","directory":"/srv//inbox/","prefix":"fw/%Y/","format":"json""#);
    let system = load_str(&json).expect("must load");
    system.compile_topology().expect("must compile");
    assert!(refusal(&json).is_empty(), "{}", refusal(&json));
    let c = objects(&system);
    assert_eq!(c.prefix, "fw/%Y/");
    let Cloud::Sftp(sftp) = &c.cloud else {
        panic!("expected SFTP, got {:?}", c.cloud);
    };
    assert_eq!(sftp.directory, "/srv/inbox");
    assert_eq!(sftp.access.host, "sftp.example.com");
    assert!(matches!(&sftp.access.credential, SftpCredential::PrivateKey(r) if r.describe().contains("SFTP_KEY")));
    let named = &system.topology.named_secrets;
    let named: Vec<&str> = named.iter().map(|n| n.what.as_str()).collect();
    assert_eq!(named, [r#"destination "lake": privateKey"#]);
}

/// An SFTP server has no storage classes, is trusted by its host key, not a TLS certificate, and needs time to sign in.
#[test]
fn an_sftp_destination_takes_no_storage_class_or_certificate_or_short_stall() {
    for extra in [r#","storageClass":"STANDARD""#, r#","certificate":"client""#] {
        let e = serde_json::from_str::<smeltr::config_file::FileConfig>(&sftp_destination_config(extra)).unwrap_err();
        assert!(e.to_string().contains("unknown field"), "{extra}: {e}");
    }
    let json = sftp_destination_config(r#","stallAfterSecs":10"#);
    assert!(
        refused_at(&json, "stallAfterSecs", "needs at least 15s"),
        "{}",
        refusal(&json)
    );
    assert!(refusal(&sftp_destination_config(r#","stallAfterSecs":15"#)).is_empty());
    let json = sftp_destination_config("").replace(r#""username":"smeltr""#, r#""username":"""#);
    assert!(
        refused_at(&json, "username", r#"destination "lake" names no username"#),
        "{}",
        refusal(&json)
    );
}

/// One Queue/Azure Blob source, with `extra` spliced into its object.
fn azure_queue_config(extra: &str) -> String {
    format!(
        r#"{{"version":1,
            "sources":[{{"id":"q","type":"azureBlobQueue","account":"acme",
                         "queueUrl":"https://acme.queue.core.windows.net/blob-events"{extra}}}]}}"#
    )
}

/// A Queue/Azure Blob source's queue URL, account and how many receives it allows a message.
fn storage_queue_of(
    o: &smeltr::plan::topology::ObjectSourceConfig,
) -> (&str, &smeltr::plan::topology::AzureAccess, u32) {
    match &o.queue {
        smeltr::plan::topology::ObjectQueue::StorageQueue {
            queue_url,
            access,
            max_deliveries,
        } => (queue_url, access, *max_deliveries),
        other => panic!("expected a Storage queue, got {other:?}"),
    }
}

/// A Queue/Azure Blob source needs only its account and queue: it signs with the host's identity, reads each blob a
/// line at a time, and moves a message to the poison queue on its sixth receive.
#[test]
fn a_minimal_azure_blob_queue_source_loads_with_the_defaults() {
    use smeltr::plan::topology::{Framing, SourceMeta};

    let json = azure_queue_config("");
    assert!(refusal(&json).is_empty(), "{}", refusal(&json));
    let system = load_str(&json).expect("must load");
    let o = object_source(&system);
    let (queue_url, access, max_deliveries) = storage_queue_of(o);
    assert_eq!(queue_url, "https://acme.queue.core.windows.net/blob-events");
    assert_eq!(access.account, "acme");
    assert!(access.credential.is_none() && access.endpoint.is_none() && o.tls.is_none());
    assert_eq!(max_deliveries, 5);
    assert_eq!(o.framing, Framing::Lines);
    assert_eq!(o.visibility, std::time::Duration::from_secs(60));
    assert_eq!(system.topology.sources[0].kind.meta(), SourceMeta::AZURE_BLOB_QUEUE);
    assert!(system.topology.named_secrets.is_empty());
}

#[test]
fn an_azure_blob_queue_source_loads_with_every_field() {
    use smeltr::plan::topology::{AzureCredential, Framing};

    let json = azure_queue_config(
        r#","endpoint":"https://acme.blob.core.usgovcloudapi.net","authEndpoint":"https://login.microsoftonline.us",
           "tenantId":"t","clientId":"c","clientSecret":{"secret":"azure-secret","version":2},
           "certificate":"acme","framing":"escapedLines","encoding":"windows-1252","visibilitySecs":300,"maxDeliveries":2"#,
    );
    assert!(refusal(&json).is_empty(), "{}", refusal(&json));
    let system = load_str(&json).expect("must load");
    let o = object_source(&system);
    let (_, access, max_deliveries) = storage_queue_of(o);
    assert_eq!(
        access.endpoint.as_deref(),
        Some("https://acme.blob.core.usgovcloudapi.net")
    );
    assert_eq!(
        access.auth_endpoint.as_deref(),
        Some("https://login.microsoftonline.us")
    );
    assert!(matches!(&access.credential, Some(AzureCredential::ClientSecret { client_id, .. }) if client_id == "c"));
    assert_eq!(max_deliveries, 2);
    assert_eq!(o.tls.as_ref().unwrap().certificate.as_deref(), Some("acme"));
    assert_eq!(o.framing, Framing::EscapedLines);
    assert_eq!(o.visibility, std::time::Duration::from_secs(300));
    let named: Vec<String> = system.topology.named_secrets.iter().map(|n| n.what.clone()).collect();
    assert_eq!(named.len(), 1, "{named:?}");
}

#[test]
fn an_azure_blob_queue_source_with_bad_settings_is_refused() {
    let e = refusal(&azure_queue_config("").replace(r#""account":"acme","#, r#""account":" ","#));
    assert!(e.contains(r#"source "q" names no account"#), "{e}");
    let e = refusal(&azure_queue_config(r#","accessKey":"KEY","sasToken":"SAS""#));
    assert!(e.contains("more than one credential"), "{e}");
    let e = refusal(&azure_queue_config(r#","tenantId":"t","clientSecret":"S""#));
    assert!(e.contains("part of a service principal"), "{e}");
    let e = refusal(&azure_queue_config(
        r#","authEndpoint":"https://login.microsoftonline.us""#,
    ));
    assert!(e.contains("authEndpoint and no clientSecret"), "{e}");
    let e = refusal(&azure_queue_config(r#","certificate":" ""#));
    assert!(e.contains("blank name"), "{e}");
    for n in ["0", "1001"] {
        let e = refusal(&azure_queue_config(&format!(r#","maxDeliveries":{n}"#)));
        assert!(e.contains("maxDeliveries must be between 1 and 1000"), "{e}");
    }
    let e = refusal(&azure_queue_config(r#","visibilitySecs":0"#));
    assert!(e.contains("between 1 and 43200"), "{e}");
    let e = refusal(&azure_queue_config("").replace("https://acme.queue.core.windows.net/blob-events", "logs"));
    assert!(
        e.contains(r#"has queueUrl "logs"; give a whole http:// or https:// URL"#),
        "{e}"
    );
    let long = "q".repeat(57);
    let e = refusal(&azure_queue_config("").replace("blob-events", &long));
    assert!(e.contains("leaves no room for its poison queue"), "{e}");
    let e = refusal(&azure_queue_config("").replace("blob-events", &"q".repeat(56)));
    assert!(e.is_empty(), "{e}");
    for unknown in [r#","container":"logs""#, r#","region":"eu-west-1""#] {
        let e = serde_json::from_str::<smeltr::config_file::FileConfig>(&azure_queue_config(unknown)).unwrap_err();
        assert!(e.to_string().contains("unknown field"), "{e}");
    }
}

/// The queue, the blob service and the token service each load over plain HTTP, on this host or off it.
#[test]
fn an_azure_blob_queue_source_over_plain_http_loads() {
    let e = refusal(&azure_queue_config("").replace("https://acme.queue", "http://acme.queue"));
    assert!(e.is_empty(), "{e}");
    let e = refusal(&azure_queue_config(r#","endpoint":"http://blob.internal""#));
    assert!(e.is_empty(), "{e}");
    let e = refusal(
        &azure_queue_config(r#","endpoint":"http://127.0.0.1:10000/devstoreaccount1""#).replace(
            "https://acme.queue.core.windows.net",
            "http://127.0.0.1:10001/devstoreaccount1",
        ),
    );
    assert!(e.is_empty(), "{e}");
}

/// One Pub/Sub/GCS source, with `extra` spliced into its object.
fn gcs_pubsub_config(extra: &str) -> String {
    format!(
        r#"{{"version":1,
            "sources":[{{"id":"g","type":"gcsPubSub",
                         "subscription":"projects/acme/subscriptions/gcs-events"{extra}}}]}}"#
    )
}

/// A Pub/Sub/GCS source's subscription, Pub/Sub address and Google Cloud access.
fn pubsub_of(
    o: &smeltr::plan::topology::ObjectSourceConfig,
) -> (&str, Option<&str>, &smeltr::plan::topology::GcsAccess) {
    match &o.queue {
        smeltr::plan::topology::ObjectQueue::PubSub {
            subscription,
            endpoint,
            access,
        } => (subscription, endpoint.as_deref(), access),
        other => panic!("expected a Pub/Sub subscription, got {other:?}"),
    }
}

/// A Pub/Sub/GCS source needs only its subscription: it signs with the host's credentials, at Google's addresses,
/// and reads each object a line at a time.
#[test]
fn a_minimal_gcs_pubsub_source_loads_with_the_defaults() {
    use smeltr::plan::topology::{Framing, SourceMeta};

    let json = gcs_pubsub_config("");
    assert!(refusal(&json).is_empty(), "{}", refusal(&json));
    let system = load_str(&json).expect("must load");
    let o = object_source(&system);
    let (subscription, endpoint, access) = pubsub_of(o);
    assert_eq!(subscription, "projects/acme/subscriptions/gcs-events");
    assert!(endpoint.is_none() && access.endpoint.is_none() && access.service_account_key.is_none());
    assert!(o.tls.is_none());
    assert_eq!(o.framing, Framing::Lines);
    assert_eq!(o.visibility, std::time::Duration::from_secs(60));
    assert_eq!(system.topology.sources[0].kind.meta(), SourceMeta::GCS_PUB_SUB);
    assert!(system.topology.named_secrets.is_empty());
}

#[test]
fn a_gcs_pubsub_source_loads_with_every_field() {
    use smeltr::plan::topology::Framing;

    let json = gcs_pubsub_config(
        r#","pubsubEndpoint":"https://pubsub.example.com","endpoint":"https://storage.example.com",
           "serviceAccountKey":{"secret":"gcs-key","version":2},
           "certificate":"acme","framing":"object","encoding":"windows-1252","visibilitySecs":600"#,
    );
    assert!(refusal(&json).is_empty(), "{}", refusal(&json));
    let system = load_str(&json).expect("must load");
    let o = object_source(&system);
    let (_, endpoint, access) = pubsub_of(o);
    assert_eq!(endpoint, Some("https://pubsub.example.com"));
    assert_eq!(access.endpoint.as_deref(), Some("https://storage.example.com"));
    assert!(access.service_account_key.is_some());
    assert_eq!(o.tls.as_ref().unwrap().certificate.as_deref(), Some("acme"));
    assert_eq!(o.framing, Framing::Object);
    assert_eq!(o.visibility, std::time::Duration::from_secs(600));
    let named: Vec<String> = system.topology.named_secrets.iter().map(|n| n.what.clone()).collect();
    assert_eq!(named.len(), 1, "{named:?}");
}

#[test]
fn a_gcs_pubsub_source_with_bad_settings_is_refused() {
    let named = "projects/acme/subscriptions/gcs-events";
    let e = refusal(&gcs_pubsub_config("").replace(named, " "));
    assert!(e.contains(r#"source "g" names no subscription"#), "{e}");
    for short in [
        "gcs-events",
        "projects/acme/topics/gcs-events",
        "projects//subscriptions/gcs-events",
    ] {
        let e = refusal(&gcs_pubsub_config("").replace(named, short));
        assert!(e.contains("Name it in full"), "{short}: {e}");
    }
    let e = refusal(&gcs_pubsub_config(r#","visibilitySecs":601"#));
    assert!(e.contains("between 1 and 600"), "{e}");
    let e = refusal(&gcs_pubsub_config(r#","pubsubEndpoint":" ""#));
    assert!(e.contains("empty pubsubEndpoint"), "{e}");
    let e = refusal(&gcs_pubsub_config(r#","endpoint":"""#));
    assert!(e.contains("empty endpoint"), "{e}");
    let e = refusal(&gcs_pubsub_config(r#","certificate":" ""#));
    assert!(e.contains("blank name"), "{e}");
    for unknown in [r#","bucket":"logs""#, r#","maxDeliveries":3"#] {
        let e = serde_json::from_str::<smeltr::config_file::FileConfig>(&gcs_pubsub_config(unknown)).unwrap_err();
        assert!(e.to_string().contains("unknown field"), "{e}");
    }
}

/// Pub/Sub and Cloud Storage each load over plain HTTP, on this host or off it.
#[test]
fn a_gcs_pubsub_source_over_plain_http_loads() {
    let e = refusal(&gcs_pubsub_config(r#","pubsubEndpoint":"http://pubsub.internal""#));
    assert!(e.is_empty(), "{e}");
    let e = refusal(&gcs_pubsub_config(r#","endpoint":"http://storage.internal""#));
    assert!(e.is_empty(), "{e}");
    let e = refusal(&gcs_pubsub_config(
        r#","pubsubEndpoint":"http://127.0.0.1:8085","endpoint":"http://localhost:4443""#,
    ));
    assert!(e.is_empty(), "{e}");
}

/// A Parquet destination writes each route's rows under that route's own record: one route's integer is not
/// another's float.
#[test]
fn a_parquet_destination_writes_each_routes_own_record() {
    use smeltr::core::column::DataType;
    let s3 = r#"{"id":"lake","type":"s3","bucket":"b","format":"parquet"}"#;
    let pipeline = |id: &str, field: &str, expr: &str| {
        format!(
            r#"{{"id":"{id}","name":"{id}","steps":[{{"id":"e","type":"eval","assignments":[{{"target":"{field}","expr":"{expr}"}}]}}]}}"#
        )
    };
    let route = |id: &str, pipeline: &str| {
        format!(r#"{{"id":"{id}","source":"s","pipeline":"{pipeline}","deliver":[{{"to":["lake"]}}]}}"#)
    };
    let json = cfg(
        s3,
        &[pipeline("p", "n", "1"), pipeline("q", "n", "1.5")].join(","),
        &[route("r", "p"), route("t", "q")].join(","),
    );
    let system = load_str(&json).expect("must load");
    let compiled = system
        .compile_topology()
        .unwrap_or_else(|e| panic!("{}", join_errors(&e)));
    let lake = system
        .topology
        .destinations
        .iter()
        .find(|d| d.name == "lake")
        .unwrap()
        .id;
    let n = |route: u32| {
        let DataType::Struct(fields, _) = &compiled.records[&lake][&Some(route)] else {
            panic!("{:?}", compiled.records[&lake]);
        };
        fields.iter().find(|(name, _)| name == "n").map(|(_, t)| t.clone())
    };
    assert_eq!(n(0), Some(DataType::I64));
    assert_eq!(n(1), Some(DataType::F64));
}

/// An open object's unclaimed fields are its `rest` column, so it cannot also name a field `rest`.
#[test]
fn a_parquet_record_naming_rest_beside_unclaimed_fields_does_not_compile() {
    let s3 = r#"{"id":"lake","type":"s3","bucket":"b","format":"parquet"}"#;
    let pipeline = r#"{"id":"p","name":"p","steps":[
        {"id":"e","type":"eval","assignments":[{"target":"rest","expr":"1"}]},
        {"id":"j","type":"code","body":"Object.assign($, json(_raw));"}]}"#;
    let json = cfg(
        s3,
        pipeline,
        r#"{"id":"r","source":"s","pipeline":"p","deliver":[{"to":["lake"]}]}"#,
    );
    let Err(errors) = load_str(&json).expect("must load").compile_topology() else {
        panic!("a record naming `rest` beside unclaimed fields compiled");
    };
    assert!(join_errors(&errors).contains("rest"), "{}", join_errors(&errors));
}

/// A healthy row keeps what its pipeline wrote at the tag field, so a Parquet column there takes that and the tag.
#[test]
fn a_parquet_tag_column_takes_what_the_pipeline_writes_there_too() {
    use smeltr::core::column::DataType;
    let s3 = r#"{"id":"lake","type":"s3","bucket":"b","format":"parquet"}"#;
    let pipeline = r#"{"id":"p","name":"p","steps":[
        {"id":"e","type":"eval","assignments":[{"target":"smeltr.exception","expr":"1"}]}]}"#;
    let json = cfg(
        s3,
        pipeline,
        r#"{"id":"r","source":"s","pipeline":"p","deliver":[{"to":["lake"]}]}"#,
    );
    let system = load_str(&json).expect("must load");
    let compiled = system
        .compile_topology()
        .unwrap_or_else(|e| panic!("{}", join_errors(&e)));
    let lake = system
        .topology
        .destinations
        .iter()
        .find(|d| d.name == "lake")
        .unwrap()
        .id;
    let expected = DataType::Struct(vec![("exception".into(), DataType::Variant)], true);
    let DataType::Struct(fields, _) = &compiled.records[&lake][&Some(0)] else {
        panic!("{:?}", compiled.records[&lake]);
    };
    assert_eq!(
        fields.iter().find(|(n, _)| n == "smeltr").map(|(_, t)| t),
        Some(&expected)
    );
}

/// A row no route takes reaches a Parquet destination through the group's answer as `{_raw: str}`.
#[test]
fn a_parquet_destination_only_the_group_answer_reaches_writes_raw() {
    use smeltr::core::column::DataType;
    let json = r#"{"version":1,
        "sources":[{"id":"s","type":"kafka","brokers":"b:9092","topics":["t"],"groupId":"g"}],
        "destinations":[{"id":"lake","type":"s3","bucket":"b","format":"parquet"}],
        "pipelines":[],
        "routes":[],
        "exceptions":{"kinds":{"Unmatched":{"deliver":["lake"]}}}}"#;
    let system = load_str(json).expect("must load");
    let compiled = system
        .compile_topology()
        .unwrap_or_else(|e| panic!("{}", join_errors(&e)));
    let lake = system
        .topology
        .destinations
        .iter()
        .find(|d| d.name == "lake")
        .unwrap()
        .id;
    assert_eq!(
        compiled.records[&lake][&None],
        DataType::Struct(vec![("_raw".into(), DataType::Str)], false)
    );
}

/// One Event Hubs source, with `extra` spliced into its object.
fn event_hubs_config(namespace: &str, extra: &str) -> String {
    format!(
        r#"{{"version":1,
            "sources":[{{"id":"e","type":"eventHubs","namespace":"{namespace}","eventHubs":["insights-logs"],
                         "consumerGroup":"smeltr","connectionString":"EVENTHUBS_CONNECTION_STRING"{extra}}}]}}"#
    )
}

/// The Kafka source an Event Hubs source loads as.
fn event_hubs_kafka(json: &str) -> smeltr::plan::topology::KafkaSourceConfig {
    let system = load_str(json).expect("must load");
    match &system.topology.sources[0].kind {
        smeltr::plan::topology::SourceKind::Kafka(k) => k.clone(),
        other => panic!("expected a Kafka source, got {other:?}"),
    }
}

/// An Event Hubs source is a Kafka source at the namespace's Kafka endpoint, over TLS, signed in with its connection
/// string, and called Event Hubs in its records and on the map.
#[test]
fn an_event_hubs_source_loads_as_a_kafka_source_signed_in_with_its_connection_string() {
    use smeltr::plan::topology::{KafkaService, SecretRef, SourceMeta, StartPosition};

    let json = event_hubs_config("acme", "");
    assert!(refusal(&json).is_empty(), "{}", refusal(&json));
    let k = event_hubs_kafka(&json);
    assert_eq!(k.brokers, "acme.servicebus.windows.net:9093");
    assert_eq!(k.topics, ["insights-logs"]);
    assert_eq!(k.group_id, "smeltr");
    assert_eq!(k.start, StartPosition::Earliest);
    assert!(k.stop.is_none() && k.tls.is_none());
    assert_eq!(k.service, KafkaService::EventHubs);
    let mut literal = k.props.literal.clone();
    literal.sort();
    assert_eq!(
        literal,
        [
            ("sasl.mechanism".to_string(), "PLAIN".to_string()),
            ("sasl.username".to_string(), "$ConnectionString".to_string()),
            ("security.protocol".to_string(), "SASL_SSL".to_string()),
        ]
    );
    assert_eq!(
        k.props.credentials,
        [(
            "sasl.password".to_string(),
            SecretRef::WorkerLocal("EVENTHUBS_CONNECTION_STRING".to_string())
        )]
    );
    let system = load_str(&json).unwrap();
    assert_eq!(system.topology.sources[0].kind.meta(), SourceMeta::EVENT_HUBS);
    system.compile_topology().expect("must compile");
}

/// A namespace is a name in Azure's public cloud, a host name in another cloud, or a `host:port`; only one on this
/// host, the emulator, is reached without TLS.
#[test]
fn an_event_hubs_namespace_names_its_kafka_endpoint() {
    for (namespace, brokers, protocol) in [
        (
            "acme.servicebus.usgovcloudapi.net",
            "acme.servicebus.usgovcloudapi.net:9093",
            "SASL_SSL",
        ),
        ("eh.internal:9093", "eh.internal:9093", "SASL_SSL"),
        ("localhost:9092", "localhost:9092", "SASL_PLAINTEXT"),
    ] {
        let k = event_hubs_kafka(&event_hubs_config(namespace, ""));
        assert_eq!(k.brokers, brokers, "{namespace}");
        let used = k.props.literal.iter().find(|(key, _)| key == "security.protocol");
        assert_eq!(used.map(|(_, v)| v.as_str()), Some(protocol), "{namespace}");
    }
}

#[test]
fn an_event_hubs_source_without_its_settings_is_refused() {
    let e = refusal(&event_hubs_config("", ""));
    assert!(e.contains("names no namespace"), "{e}");
    let e = refusal(&event_hubs_config("sb://acme.servicebus.windows.net/", ""));
    assert!(e.contains("without a scheme or a path"), "{e}");
    let e = refusal(&event_hubs_config("acme", "").replace(r#"["insights-logs"]"#, "[]"));
    assert!(e.contains("reads no event hubs"), "{e}");
    let e = refusal(&event_hubs_config("acme", "").replace(r#""consumerGroup":"smeltr""#, r#""consumerGroup":" ""#));
    assert!(e.contains("no consumer group"), "{e}");
    let e = refusal(&event_hubs_config("acme", "").replace("EVENTHUBS_CONNECTION_STRING", " "));
    assert!(e.contains("credential by a blank name"), "{e}");
}

/// One Event Hubs destination, with `extra` spliced into its object.
fn event_hubs_destination_config(namespace: &str, extra: &str) -> String {
    format!(
        r#"{{"version":1,
            "destinations":[{{"id":"e","type":"eventHubs","namespace":"{namespace}","defaultEventHub":"alerts",
                              "connectionString":"EVENTHUBS_CONNECTION_STRING"{extra}}}]}}"#
    )
}

/// An Event Hubs destination is a Kafka destination at the namespace's Kafka endpoint, signed in with its
/// connection string as the source is, and called Event Hubs on the map.
#[test]
fn an_event_hubs_destination_loads_as_a_kafka_destination_signed_in_with_its_connection_string() {
    use smeltr::plan::topology::{Compression, DestinationKind, KafkaService, SecretRef};

    for (namespace, brokers, protocol) in [
        ("acme", "acme.servicebus.windows.net:9093", "SASL_SSL"),
        ("localhost:9092", "localhost:9092", "SASL_PLAINTEXT"),
    ] {
        let json = event_hubs_destination_config(namespace, r#","compression":"gzip""#);
        assert!(refusal(&json).is_empty(), "{}", refusal(&json));
        let system = load_str(&json).expect("must load");
        let kind = &system.topology.destinations[0].kind;
        let DestinationKind::Kafka(k) = kind else {
            panic!("expected a Kafka destination, got {kind:?}")
        };
        assert_eq!(k.brokers, brokers);
        assert_eq!(k.default_topic, "alerts");
        assert_eq!(k.compression, Compression::Gzip);
        assert_eq!(k.service, KafkaService::EventHubs);
        assert!(k.tls.is_none());
        let source = event_hubs_kafka(&event_hubs_config(namespace, ""));
        assert_eq!(k.props.literal, source.props.literal, "{namespace}");
        let used = k.props.literal.iter().find(|(key, _)| key == "security.protocol");
        assert_eq!(used.map(|(_, v)| v.as_str()), Some(protocol), "{namespace}");
        assert_eq!(
            k.props.credentials,
            [(
                "sasl.password".to_string(),
                SecretRef::WorkerLocal("EVENTHUBS_CONNECTION_STRING".to_string())
            )]
        );
        assert_eq!(
            smeltr::leader::map::System::from(kind),
            smeltr::leader::map::System::EventHubs
        );
    }
}

#[test]
fn an_event_hubs_destination_without_its_settings_or_with_a_codec_it_refuses_is_refused() {
    let e = refusal(&event_hubs_destination_config("", ""));
    assert!(e.contains("names no namespace"), "{e}");
    let e = refusal(&event_hubs_destination_config("sb://acme.servicebus.windows.net/", ""));
    assert!(e.contains("without a scheme or a path"), "{e}");
    let e = refusal(&event_hubs_destination_config("acme", "").replace(r#""alerts""#, r#"" ""#));
    assert!(e.contains("no default event hub"), "{e}");
    let e = refusal(&event_hubs_destination_config("acme", "").replace("EVENTHUBS_CONNECTION_STRING", " "));
    assert!(e.contains("credential by a blank name"), "{e}");
    for codec in ["snappy", "lz4", "zstd"] {
        let e = refusal(&event_hubs_destination_config(
            "acme",
            &format!(r#","compression":"{codec}""#),
        ));
        assert!(
            e.contains(r#"an Event Hubs destination takes only "gzip" or "none""#),
            "{codec}: {e}"
        );
    }
}

/// The Kafka properties an Event Hubs source signed in with `sign_in` loads with, sorted.
fn event_hubs_signed_in(namespace: &str, sign_in: &str) -> smeltr::plan::topology::KafkaProps {
    let json =
        event_hubs_config(namespace, sign_in).replace(r#","connectionString":"EVENTHUBS_CONNECTION_STRING""#, "");
    assert!(refusal(&json).is_empty(), "{}", refusal(&json));
    let mut props = event_hubs_kafka(&json).props;
    props.literal.sort();
    props
}

/// A service principal signs in with an Entra token for the namespace, obtained from its tenant with its secret: in
/// Azure's public cloud by default, or wherever `authEndpoint` says.
#[test]
fn an_event_hubs_source_signs_in_as_a_service_principal() {
    use smeltr::plan::topology::SecretRef;

    let principal = r#","tenantId":"t-1","clientId":"c-1","clientSecret":"EVENTHUBS_SECRET""#;
    let pairs = |pairs: &[(&str, &str)]| -> Vec<(String, String)> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    };
    let props = event_hubs_signed_in("acme", principal);
    assert_eq!(
        props.literal,
        pairs(&[
            ("sasl.mechanism", "OAUTHBEARER"),
            ("sasl.oauthbearer.client.id", "c-1"),
            ("sasl.oauthbearer.method", "oidc"),
            ("sasl.oauthbearer.scope", "https://acme.servicebus.windows.net/.default"),
            (
                "sasl.oauthbearer.token.endpoint.url",
                "https://login.microsoftonline.com/t-1/oauth2/v2.0/token"
            ),
            ("security.protocol", "SASL_SSL"),
        ])
    );
    assert_eq!(
        props.credentials,
        [(
            "sasl.oauthbearer.client.secret".to_string(),
            SecretRef::WorkerLocal("EVENTHUBS_SECRET".to_string())
        )]
    );

    let props = event_hubs_signed_in(
        "acme.servicebus.usgovcloudapi.net",
        &format!(r#"{principal},"authEndpoint":"https://login.microsoftonline.us/""#),
    );
    let value = |key: &str| props.literal.iter().find(|(k, _)| k == key).map(|(_, v)| v.clone());
    assert_eq!(
        value("sasl.oauthbearer.token.endpoint.url").as_deref(),
        Some("https://login.microsoftonline.us/t-1/oauth2/v2.0/token")
    );
    assert_eq!(
        value("sasl.oauthbearer.scope").as_deref(),
        Some("https://acme.servicebus.usgovcloudapi.net/.default")
    );
}

/// Naming no credential signs in as the host's managed identity, through Azure's instance metadata service.
#[test]
fn an_event_hubs_source_naming_no_credential_signs_in_as_the_hosts_managed_identity() {
    let props = event_hubs_signed_in("acme", "");
    let pairs: Vec<(String, String)> = [
        ("sasl.mechanism", "OAUTHBEARER"),
        (
            "sasl.oauthbearer.config",
            "query=api-version=2018-02-01&resource=https://acme.servicebus.windows.net",
        ),
        ("sasl.oauthbearer.metadata.authentication.type", "azure_imds"),
        ("sasl.oauthbearer.method", "oidc"),
        ("security.protocol", "SASL_SSL"),
    ]
    .iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect();
    assert_eq!(props.literal, pairs);
    assert!(props.credentials.is_empty());
}

/// Naming a managed identity's client id asks the instance metadata service for that identity's token.
#[test]
fn an_event_hubs_source_naming_a_managed_identity_signs_in_as_it() {
    let id = "72f988bf-86f1-41af-91ab-2d7cd011db47";
    let props = event_hubs_signed_in("acme", &format!(r#","managedIdentityClientId":" {id} ""#));
    let value = |key: &str| props.literal.iter().find(|(k, _)| k == key).map(|(_, v)| v.clone());
    assert_eq!(
        value("sasl.oauthbearer.config").as_deref(),
        Some(
            format!("query=api-version=2018-02-01&resource=https://acme.servicebus.windows.net&client_id={id}")
                .as_str()
        )
    );
    assert_eq!(
        value("sasl.oauthbearer.metadata.authentication.type").as_deref(),
        Some("azure_imds")
    );
    assert!(props.credentials.is_empty());
}

/// An Event Hubs destination signs in as its source does, from the same fields.
#[test]
fn an_event_hubs_destination_signs_in_as_its_source_does() {
    use smeltr::plan::topology::DestinationKind;

    for sign_in in [
        r#","connectionString":"EVENTHUBS_CONNECTION_STRING""#,
        r#","tenantId":"t-1","clientId":"c-1","clientSecret":"EVENTHUBS_SECRET""#,
        r#","managedIdentityClientId":"72f988bf-86f1-41af-91ab-2d7cd011db47""#,
        "",
    ] {
        let json = format!(
            r#"{{"version":1,
                "destinations":[{{"id":"e","type":"eventHubs","namespace":"acme","defaultEventHub":"alerts"{sign_in}}}]}}"#
        );
        let system = load_str(&json).expect("must load");
        let DestinationKind::Kafka(k) = &system.topology.destinations[0].kind else {
            panic!("expected a Kafka destination")
        };
        let source = event_hubs_kafka(
            &event_hubs_config("acme", "").replace(r#","connectionString":"EVENTHUBS_CONNECTION_STRING""#, sign_in),
        );
        assert_eq!(k.props, source.props, "{sign_in}");
    }
}

#[test]
fn an_event_hubs_sign_in_naming_two_credentials_or_part_of_a_principal_is_refused() {
    let both = refusal(&event_hubs_config(
        "acme",
        r#","tenantId":"t-1","clientId":"c-1","clientSecret":"EVENTHUBS_SECRET""#,
    ));
    assert!(both.contains("more than one credential"), "{both}");
    let without = |extra: &str| {
        refusal(&event_hubs_config("acme", extra).replace(r#","connectionString":"EVENTHUBS_CONNECTION_STRING""#, ""))
    };
    let e = without(r#","tenantId":"t-1","clientId":"c-1""#);
    assert!(e.contains("part of a service principal"), "{e}");
    let e = without(r#","authEndpoint":"https://login.microsoftonline.us""#);
    assert!(e.contains("no clientSecret"), "{e}");
    let e = without(
        r#","tenantId":"t-1","clientId":"c-1","clientSecret":"EVENTHUBS_SECRET","authEndpoint":"https://login.microsoftonline.com/t-1""#,
    );
    assert!(e.contains("the sign-in host alone"), "{e}");
    let e = without(r#","tenantId":" ","clientId":"c-1","clientSecret":"EVENTHUBS_SECRET""#);
    assert!(e.contains("empty tenantId"), "{e}");
    let e = refusal(&event_hubs_destination_config("acme", r#","clientId":"c-1""#));
    assert!(e.contains("part of a service principal"), "{e}");
    let e = refusal(&event_hubs_config(
        "acme",
        r#","managedIdentityClientId":"72f988bf-86f1-41af-91ab-2d7cd011db47""#,
    ));
    assert!(e.contains("more than one credential"), "{e}");
    let e = without(r#","managedIdentityClientId":"writer""#);
    assert!(e.contains("a GUID"), "{e}");
    let e = without(r#","managedIdentityClientId":" ""#);
    assert!(e.contains("empty managedIdentityClientId"), "{e}");
}

/// One HTTP API source asking `url`, with `extra` spliced into its object.
fn http_api_config(url: &str, extra: &str) -> String {
    format!(
        r#"{{"version":1,
            "sources":[{{"id":"g","type":"httpApi","url":"{url}"{extra}}}]}}"#
    )
}

fn http_api(system: &smeltr::config_file::LoadedSystem) -> &smeltr::plan::topology::HttpApiConfig {
    match &system.topology.sources[0].kind {
        smeltr::plan::topology::SourceKind::Api(smeltr::plan::topology::ApiSourceConfig {
            api: smeltr::plan::topology::Api::HttpApi(h),
            ..
        }) => h,
        other => panic!("expected an HTTP API source, got {other:?}"),
    }
}

/// An HTTP API source needs only its URL: the answer is one record, read again every minute, with no headers.
#[test]
fn a_minimal_http_api_source_loads_with_the_defaults() {
    use smeltr::plan::topology::{ApiRecords, SourceKind, SourceMeta};

    let json = http_api_config("https://api.example/v1/status", "");
    assert!(refusal(&json).is_empty(), "{}", refusal(&json));
    let system = load_str(&json).expect("must load");
    system.compile_topology().expect("must compile");
    let h = http_api(&system);
    assert_eq!(h.url, "https://api.example/v1/status");
    assert_eq!(h.records, ApiRecords::Body);
    assert!(!h.follow_links && h.since.is_none());
    assert!(h.headers.is_empty() && h.credentials.is_empty());
    let kind = &system.topology.sources[0].kind;
    let SourceKind::Api(a) = kind else { unreachable!() };
    assert_eq!(a.poll_interval, std::time::Duration::from_secs(60));
    assert!(a.tls.is_none());
    assert_eq!(kind.meta(), SourceMeta::HTTP_API.with_encoding(kind.encoding()));
    assert_eq!(
        kind.single_worker().as_deref(),
        Some("https://api.example/v1/status"),
        "one worker asks it"
    );
}

/// A POST carries its body as written; a body on a GET is refused.
#[test]
fn an_http_api_source_posts_its_body() {
    use smeltr::plan::topology::ApiMethod;

    let json = http_api_config(
        "https://telemetry.example.com/api/v2/stream",
        r#","method":"POST","body":{"batchSize":1000,"maxLingerMs":15000}"#,
    );
    assert!(refusal(&json).is_empty(), "{}", refusal(&json));
    let system = load_str(&json).expect("must load");
    let h = http_api(&system);
    assert_eq!(h.method, ApiMethod::Post);
    let body: serde_json::Value = serde_json::from_str(h.body.as_deref().expect("a body")).unwrap();
    assert_eq!(body, serde_json::json!({"batchSize": 1000, "maxLingerMs": 15000}));

    let empty = load_str(&http_api_config("https://api.example/v1/run", r#","method":"POST""#)).expect("must load");
    assert_eq!(
        (http_api(&empty).method, http_api(&empty).body.as_deref()),
        (ApiMethod::Post, None)
    );
    assert_eq!(
        http_api(&load_str(&http_api_config("https://a.example/", "")).unwrap()).method,
        ApiMethod::Get
    );

    let on_get = refusal(&http_api_config("https://api.example/v1/events", r#","body":{"a":1}"#));
    assert!(on_get.contains("only with a POST"), "{on_get}");
    let put = http_api_config("https://api.example/v1/events", r#","method":"PUT""#);
    assert!(load_str(&put).is_err(), "a PUT is refused");
}

#[test]
fn an_http_api_source_loads_with_every_field() {
    use smeltr::plan::topology::{ApiRecords, ListStart, SecretRef};

    let json = http_api_config(
        "https://api.example/v1/events?limit=500",
        r#","headers":{"Accept":"application/json"},
           "credentials":{"Authorization":{"secret":"api","version":3}},
           "records":"array","recordsAt":"/data","followLinks":true,
           "sinceParam":"since","sinceOverlapSecs":30,"start":"new","pollIntervalSecs":15"#,
    );
    assert!(refusal(&json).is_empty(), "{}", refusal(&json));
    let system = load_str(&json).expect("must load");
    let h = http_api(&system);
    assert_eq!(h.records, ApiRecords::Array("/data".into()));
    assert!(h.follow_links);
    let since = h.since.as_ref().expect("since");
    assert_eq!(since.param, "since");
    assert_eq!(since.overlap, std::time::Duration::from_secs(30));
    assert_eq!(since.start, ListStart::New);
    assert_eq!(h.headers, vec![("Accept".to_string(), "application/json".to_string())]);
    let key = SecretRef::LeaderStored {
        name: "api".into(),
        version: 3,
    };
    assert_eq!(h.credentials, vec![("Authorization".to_string(), key.clone())]);
    let named = &system.topology.named_secrets;
    let named: Vec<_> = named.iter().map(|n| (&n.reference, n.what.as_str())).collect();
    assert_eq!(named, vec![(&key, r#"source "g": credentials"#)]);
}

#[test]
fn an_http_api_source_with_bad_settings_is_refused() {
    let at = "https://api.example/v1/events";
    for (url, extra, field, says) in [
        ("", "", "url", "names no url"),
        ("api.example/events", "", "url", "give a whole http:// or https:// URL"),
        (
            "ftp://api.example/events",
            "",
            "url",
            "give a whole http:// or https:// URL",
        ),
        (
            at,
            r#","headers":{"Host":"b.example"}"#,
            "headers",
            "smeltr sets itself",
        ),
        (at, r#","headers":{"X-A":"one\ntwo"}"#, "headers", "line break"),
        (
            at,
            r#","records":"array","recordsAt":"data""#,
            "recordsAt",
            "not a JSON pointer",
        ),
        (at, r#","recordsAt":"/data""#, "recordsAt", "does not read an array"),
        (at, r#","pollIntervalSecs":0"#, "pollIntervalSecs", "at least 1"),
        (at, r#","sinceParam":"since date""#, "sinceParam", "letters, digits"),
        (
            "https://api.example/v1/events?since=2020-01-01T00:00:00Z",
            r#","sinceParam":"since""#,
            "sinceParam",
            "take it out of the URL",
        ),
        (at, r#","sinceOverlapSecs":10"#, "sinceOverlapSecs", "no sinceParam"),
        (at, r#","start":"new""#, "start", "no sinceParam"),
        (at, r#","certificate":" ""#, "certificate", "blank name"),
        (
            "http://127.0.0.1/events",
            r#","certificate":"acme""#,
            "url",
            "does not negotiate TLS",
        ),
    ] {
        let json = http_api_config(url, extra);
        assert!(
            refused_at(&json, field, says),
            "{url} {extra}: wanted {field}: {says}, got {}",
            refusal(&json)
        );
    }
}

/// A key in the URL or in `headers` is the writer's choice to commit it, and a cookie is theirs to send.
#[test]
fn an_http_api_source_may_carry_a_committed_key_and_a_cookie() {
    let json = http_api_config(
        "https://api.example/v1/events?api_key=abc",
        r#","headers":{"X-Api-Key":"abc","Cookie":"session=1"}"#,
    );
    assert!(refusal(&json).is_empty(), "{}", refusal(&json));
}

/// Plain HTTP is the admin's choice, with a credential or without; the form warns, load does not refuse.
#[test]
fn an_http_api_source_may_ask_plain_http() {
    let credential = r#","credentials":{"Authorization":{"secret":"t","version":1}}"#;
    for json in [
        http_api_config("http://api.example/events", ""),
        http_api_config("http://api.example/events", credential),
        http_api_config("http://127.0.0.1:8080/events", credential),
    ] {
        assert!(refusal(&json).is_empty(), "{}", refusal(&json));
    }
}

/// Plain HTTP to Okta is the admin's choice; the form warns, load does not refuse.
#[test]
fn an_okta_source_over_plain_http_loads() {
    let json = okta_config("http://okta.internal", "");
    assert!(refusal(&json).is_empty(), "{}", refusal(&json));
}

/// One Okta System Log source of `org`, its token in the leader's store, with `extra` spliced into its object.
fn okta_config(org: &str, extra: &str) -> String {
    format!(
        r#"{{"version":1,
            "sources":[{{"id":"o","type":"oktaLogs","org":"{org}",
                         "token":{{"secret":"okta","version":2}}{extra}}}]}}"#
    )
}

/// An Okta source needs its org and its token; one worker reads the org's System Log, from what Okta lists first.
#[test]
fn an_okta_source_loads_with_the_defaults() {
    use smeltr::plan::topology::{Api, ApiSourceConfig, ListStart, SecretRef, SourceKind, SourceMeta};

    let json = okta_config("https://acme.okta.com/", "");
    assert!(refusal(&json).is_empty(), "{}", refusal(&json));
    let system = load_str(&json).expect("must load");
    system.compile_topology().expect("must compile");
    let kind = &system.topology.sources[0].kind;
    let SourceKind::Api(ApiSourceConfig {
        api: Api::OktaLogs(o),
        poll_interval,
        ..
    }) = kind
    else {
        panic!("expected an Okta source, got {kind:?}")
    };
    assert_eq!(o.org, "https://acme.okta.com");
    assert_eq!(o.start, ListStart::Everything);
    assert_eq!(*poll_interval, std::time::Duration::from_secs(60));
    assert_eq!(kind.meta(), SourceMeta::OKTA_LOGS.with_encoding(kind.encoding()));
    assert_eq!(
        kind.single_worker().as_deref(),
        Some("okta:https://acme.okta.com/api/v1/logs")
    );
    let key = SecretRef::LeaderStored {
        name: "okta".into(),
        version: 2,
    };
    let named = &system.topology.named_secrets;
    let named: Vec<_> = named.iter().map(|n| (&n.reference, n.what.as_str())).collect();
    assert_eq!(named, vec![(&key, r#"source "o": token"#)]);
}

#[test]
fn an_okta_source_with_bad_settings_is_refused() {
    let at = "https://acme.okta.com";
    for (org, extra, field, says) in [
        ("", "", "org", "names no org"),
        ("acme.okta.com", "", "org", "give a whole http:// or https:// URL"),
        ("https://acme.okta.com/api/v1/logs", "", "org", "give its address alone"),
        ("https://acme.okta.com?x=1", "", "org", "give its address alone"),
        (at, r#","pollIntervalSecs":0"#, "pollIntervalSecs", "at least 1"),
        (at, r#","certificate":" ""#, "certificate", "blank name"),
        (
            "http://127.0.0.1:8080",
            r#","certificate":"acme""#,
            "org",
            "does not negotiate TLS",
        ),
    ] {
        let json = okta_config(org, extra);
        assert!(
            refused_at(&json, field, says),
            "{org} {extra}: wanted {field}: {says}, got {}",
            refusal(&json)
        );
    }
    let json = r#"{"version":1,"sources":[{"id":"o","type":"oktaLogs","org":"https://acme.okta.com"}]}"#;
    assert!(load_str(json).is_err(), "a token is required");
}

/// One Okta System Log source signing in with `sign_in`, the source's credential fields spliced into its object.
fn okta_sign_in_config(sign_in: &str) -> String {
    format!(
        r#"{{"version":1,
            "sources":[{{"id":"o","type":"oktaLogs","org":"https://acme.okta.com"{sign_in}}}]}}"#
    )
}

/// A service app signs in with its client id and its private key in place of a token; its key id is optional.
#[test]
fn an_okta_source_signs_in_as_a_service_app() {
    use smeltr::plan::topology::{Api, ApiSourceConfig, OktaSignIn, SecretRef, SourceKind};

    let key = SecretRef::LeaderStored {
        name: "okta-key".into(),
        version: 3,
    };
    for (key_id, want) in [
        ("", None),
        (r#","keyId":" k1 ""#, Some("k1")),
        (r#","keyId":" ""#, None),
    ] {
        let json = okta_sign_in_config(&format!(
            r#","clientId":" 0oa1 ","privateKey":{{"secret":"okta-key","version":3}}{key_id}"#
        ));
        assert!(refusal(&json).is_empty(), "{}", refusal(&json));
        let system = load_str(&json).expect("must load");
        system.compile_topology().expect("must compile");
        let kind = &system.topology.sources[0].kind;
        let SourceKind::Api(ApiSourceConfig {
            api: Api::OktaLogs(o), ..
        }) = kind
        else {
            panic!("expected an Okta source, got {kind:?}")
        };
        let OktaSignIn::ServiceApp {
            client_id,
            private_key,
            key_id,
        } = &o.sign_in
        else {
            panic!("expected a service app, got {:?}", o.sign_in)
        };
        assert_eq!(client_id, "0oa1");
        assert_eq!(private_key, &key);
        assert_eq!(key_id.as_deref(), want);
        let named = &system.topology.named_secrets;
        let named: Vec<_> = named.iter().map(|n| (&n.reference, n.what.as_str())).collect();
        assert_eq!(named, vec![(&key, r#"source "o": privateKey"#)]);
    }
}

/// A source names a token or a whole service app, never both and never neither.
#[test]
fn an_okta_source_with_a_muddled_sign_in_is_refused() {
    let token = r#","token":{"secret":"okta","version":2}"#;
    let client = r#","clientId":"0oa1""#;
    let key = r#","privateKey":{"secret":"okta-key","version":3}"#;
    let key_id = r#","keyId":"k1""#;
    for (sign_in, field, says) in [
        (String::new(), "token", "names no sign-in"),
        (
            format!("{token}{client}{key}"),
            "token",
            "both a token and a service app",
        ),
        (format!("{token}{key}"), "token", "both a token and a service app"),
        (key.to_string(), "clientId", "give its clientId"),
        (format!(r#","clientId":" "{key}"#), "clientId", "give its clientId"),
        (client.to_string(), "privateKey", "as privateKey"),
        (format!("{token}{key_id}"), "keyId", "with no service app"),
    ] {
        let json = okta_sign_in_config(&sign_in);
        assert!(
            refused_at(&json, field, says),
            "{sign_in}: wanted {field}: {says}, got {}",
            refusal(&json)
        );
    }
}

/// One HTTP receiver on `port`, with `extra` spliced into its object.
fn http_receiver_config(port: u16, extra: &str) -> String {
    format!(
        r#"{{"version":1,
            "sources":[{{"id":"hook","type":"httpReceiver","port":{port}{extra}}}]}}"#
    )
}

const RECEIVER_TOKEN: &str = r#","tokens":{"ci":{"secret":"hook","version":1}}"#;

/// A receiver needs a port and a token: it listens on every address, takes POSTs at `/`, and reads each body whole.
#[test]
fn a_minimal_http_receiver_loads_with_the_defaults() {
    use smeltr::plan::topology::{ApiRecords, Receiver, SourceKind, SourceMeta};

    let json = http_receiver_config(8088, RECEIVER_TOKEN);
    assert!(refusal(&json).is_empty(), "{}", refusal(&json));
    let system = load_str(&json).expect("must load");
    system.compile_topology().expect("must compile");
    let kind = &system.topology.sources[0].kind;
    let SourceKind::Receiver(r) = kind else {
        panic!("expected a receiver, got {kind:?}")
    };
    let Receiver::Http(h) = &r.receiver else {
        panic!("expected an HTTP receiver, got {:?}", r.receiver)
    };
    assert_eq!(h.path, "/");
    assert_eq!(h.records, ApiRecords::Body);
    assert_eq!(r.listen, "[::]:8088".parse().unwrap());
    assert_eq!(r.tokens.len(), 1);
    assert_eq!(r.max_request_bytes, 10 * 1024 * 1024);
    assert_eq!(kind.meta(), SourceMeta::HTTP_RECEIVER.with_encoding(kind.encoding()));
    assert_eq!(kind.single_worker(), None, "every worker of the group listens");
    assert_eq!(
        kind.listens().map(|l| (l.transport, l.address)),
        Some((smeltr::plan::topology::Transport::Tcp, r.listen))
    );
}

#[test]
fn an_http_receiver_loads_with_every_field() {
    use smeltr::plan::topology::{ApiRecords, Receiver, SourceKind};

    let json = http_receiver_config(
        9000,
        &format!(
            r#"{RECEIVER_TOKEN},"path":"/hooks/ci","records":"array",
               "recordsAt":"/events","maxRequestBytes":2048"#
        ),
    );
    assert!(refusal(&json).is_empty(), "{}", refusal(&json));
    let system = load_str(&json).expect("must load");
    let SourceKind::Receiver(r) = &system.topology.sources[0].kind else {
        unreachable!()
    };
    let Receiver::Http(h) = &r.receiver else {
        panic!("expected an HTTP receiver, got {:?}", r.receiver)
    };
    assert_eq!(h.path, "/hooks/ci");
    assert_eq!(h.records, ApiRecords::Array("/events".into()));
    assert_eq!(r.listen, "[::]:9000".parse().unwrap());
    assert_eq!(r.max_request_bytes, 2048);
}

/// With neither a token nor authorities to vouch for a sender, anyone who reaches the port could send.
#[test]
fn an_http_receiver_that_admits_anyone_is_refused() {
    let e = refusal(&http_receiver_config(8088, ""));
    assert!(e.contains("admits anyone"), "{e}");
}

#[test]
fn an_http_receiver_with_a_bad_address_path_or_size_is_refused() {
    let e = refusal(&http_receiver_config(0, RECEIVER_TOKEN));
    assert!(e.contains("port 0"), "{e}");
    let e = refusal(&http_receiver_config(
        8088,
        &format!(r#"{RECEIVER_TOKEN},"path":"hooks""#),
    ));
    assert!(e.contains("a path starts with /"), "{e}");
    let e = refusal(&http_receiver_config(
        8088,
        &format!(r#"{RECEIVER_TOKEN},"path":"/health""#),
    ));
    assert!(e.contains("health checks"), "{e}");
    let e = refusal(&http_receiver_config(
        8088,
        &format!(r#"{RECEIVER_TOKEN},"maxRequestBytes":1"#),
    ));
    assert!(e.contains("maxRequestBytes is between"), "{e}");
    let e = refusal(&http_receiver_config(
        8088,
        &format!(r#"{RECEIVER_TOKEN},"recordsAt":"/events""#),
    ));
    assert!(e.contains("does not read an array"), "{e}");
    let e = refusal(&http_receiver_config(
        8088,
        &format!(r#"{RECEIVER_TOKEN},"path":"/in/*rest""#),
    ));
    assert!(e.contains("a path starts with /"), "a route pattern is not a path: {e}");
}

/// A receiver presents its certificate as a server's, which is what a worker holds that certificate to.
#[test]
fn an_http_receiver_presents_its_certificate_as_a_server() {
    let json = format!(
        r#"{{"version":1,
            "sources":[{{"id":"hook","type":"httpReceiver","port":8088,"certificate":"served"{RECEIVER_TOKEN}}}]}}"#
    );
    assert_eq!(refusal(&json), "");
    let system = load_str(&json).expect("must load");
    let tls = system.topology.sources[0].kind.tls().expect("a certificate");
    assert_eq!(tls.side, smeltr::io::trust::Side::Server, "{tls:?}");
    assert!(
        !tls.trust_public,
        "a receiver verifies senders against its authorities alone"
    );
}

/// Every worker binds every receiver, so two on one port would fail on all of them.
#[test]
fn two_receivers_on_one_port_are_refused() {
    let json = format!(
        r#"{{"version":1,"sources":[
            {{"id":"a","type":"httpReceiver","port":8088{RECEIVER_TOKEN}}},
            {{"id":"b","type":"hecReceiver","port":8088{HEC_TOKEN}}}]}}"#
    );
    let e = refusal(&json);
    assert!(
        e.contains(r#"source "b" listens on port 8088, which source "a" already does"#),
        "{e}"
    );
}

/// An exporter sends every signal to one port, so OTLP receivers of different signals share one, served alike.
#[test]
fn otlp_receivers_of_different_signals_share_a_port() {
    use smeltr::plan::topology::{Receiver, Signal, SourceKind, SourceMeta};

    let otlp = |id: &str, signal: &str, extra: &str| {
        format!(r#"{{"id":"{id}","type":"otlpReceiver","port":4318{signal}{extra}{RECEIVER_TOKEN}}}"#)
    };
    let sources = |list: &[String]| format!(r#"{{"version":1,"sources":[{}]}}"#, list.join(","));
    let shared = sources(&[
        otlp("logs", "", ""),
        otlp("metrics", r#","signal":"metrics""#, ""),
        otlp("traces", r#","signal":"traces""#, ""),
    ]);
    assert!(refusal(&shared).is_empty(), "{}", refusal(&shared));
    let system = load_str(&shared).expect("must load");
    system.compile_topology().expect("must compile");
    let signals: Vec<_> = (system.topology.sources.iter())
        .map(|s| match &s.kind {
            SourceKind::Receiver(r) => match r.receiver {
                Receiver::Otlp(signal) => (
                    signal,
                    s.kind.meta() == SourceMeta::otlp(signal).with_encoding(s.kind.encoding()),
                ),
                _ => panic!("{:?}", r.receiver),
            },
            other => panic!("{other:?}"),
        })
        .collect();
    assert_eq!(
        signals,
        [(Signal::Logs, true), (Signal::Metrics, true), (Signal::Traces, true)],
        "each takes its signal's records"
    );

    let e = refusal(&sources(&[otlp("a", "", ""), otlp("b", r#","signal":"logs""#, "")]));
    assert!(
        e.contains(r#"source "b" takes logs on port 4318, which source "a" already does"#),
        "{e}"
    );
    let e = refusal(&sources(&[
        otlp("a", "", ""),
        otlp("b", r#","signal":"traces""#, r#","certificate":"elsewhere""#),
    ]));
    assert!(
        e.contains(
            r#"source "b" shares port 4318 with source "a", which serves it; give both the same certificate and trusted authorities"#
        ),
        "{e}"
    );
    let e = refusal(&sources(&[
        otlp("a", "", ""),
        format!(r#"{{"id":"b","type":"httpReceiver","port":4318{RECEIVER_TOKEN}}}"#),
    ]));
    assert!(
        e.contains(r#"source "b" listens on port 4318, which source "a" already does"#),
        "{e}"
    );
    let unknown = r#"{"version":1,"sources":[{"id":"a","type":"otlpReceiver","port":4318,"signal":"profiles"}]}"#;
    let e = serde_json::from_str::<smeltr::config_file::FileConfig>(unknown).unwrap_err();
    assert!(e.to_string().contains("unknown variant `profiles`"), "{e}");
}

const HEC_TOKEN: &str = r#","tokens":{"ci":{"token":{"secret":"hook","version":1}}}"#;

/// A HEC receiver takes a port and a token, and serves Splunk's paths.
#[test]
fn a_minimal_hec_receiver_loads() {
    use smeltr::plan::topology::{Receiver, SourceKind, SourceMeta};

    let json = format!(r#"{{"version":1,"sources":[{{"id":"hec","type":"hecReceiver","port":8088{HEC_TOKEN}}}]}}"#);
    assert!(refusal(&json).is_empty(), "{}", refusal(&json));
    let system = load_str(&json).expect("must load");
    system.compile_topology().expect("must compile");
    let kind = &system.topology.sources[0].kind;
    let SourceKind::Receiver(r) = kind else {
        panic!("expected a receiver, got {kind:?}")
    };
    let Receiver::Hec(hec) = &r.receiver else {
        panic!("expected HEC, got {:?}", r.receiver)
    };
    assert!(
        hec.acknowledging.is_empty(),
        "a token holds its senders unless it says otherwise"
    );
    assert_eq!(r.listen, "[::]:8088".parse().unwrap());
    assert_eq!(kind.meta(), SourceMeta::HEC_RECEIVER.with_encoding(kind.encoding()));
    assert_eq!(
        kind.listens().map(|l| (l.transport, l.address)),
        Some((smeltr::plan::topology::Transport::Tcp, r.listen))
    );
}

/// A token with acknowledgement gives its senders `ackId`s, as Splunk's `useACK` does.
#[test]
fn a_hec_token_with_acknowledgement_loads() {
    use smeltr::plan::topology::{Receiver, SourceKind};

    let json = r#"{"version":1,"sources":[{"id":"hec","type":"hecReceiver","port":8088,"tokens":{
        "fluent":{"token":"FLUENT_TOKEN"},
        "vector":{"token":"VECTOR_TOKEN","acknowledgement":true}}}]}"#;
    assert!(refusal(json).is_empty(), "{}", refusal(json));
    let system = load_str(json).expect("must load");
    let SourceKind::Receiver(r) = &system.topology.sources[0].kind else {
        panic!("expected a receiver")
    };
    let Receiver::Hec(hec) = &r.receiver else {
        panic!("expected HEC")
    };
    assert_eq!(hec.acknowledging, ["vector".to_string()].into());
    assert_eq!(r.tokens.len(), 2);
}

/// What every receiver takes is flattened into each, and still leaves no field to fall through unread.
#[test]
fn a_receiver_refuses_a_field_it_does_not_know() {
    let json = format!(
        r#"{{"version":1,"sources":[{{"id":"hec","type":"hecReceiver","port":8088,
        "maxRequestBytes":1048576,"path":"/events"{HEC_TOKEN}}}]}}"#
    );
    let e = serde_json::from_str::<smeltr::config_file::FileConfig>(&json).unwrap_err();
    assert!(e.to_string().contains("unknown field `path`"), "{e}");
    let loaded = serde_json::from_str::<smeltr::config_file::FileConfig>(&json.replace(r#","path":"/events""#, ""));
    assert!(loaded.is_ok(), "{loaded:?}");
    // Where a receiver listens is its worker's to say.
    let e = serde_json::from_str::<smeltr::config_file::FileConfig>(
        &json.replace(r#","path":"/events""#, r#","bindAddress":"127.0.0.1""#),
    )
    .unwrap_err();
    assert!(e.to_string().contains("unknown field `bindAddress`"), "{e}");
}

/// An OTLP receiver takes a port and a bearer token, and is held to what every receiver is.
#[test]
fn a_minimal_otlp_receiver_loads() {
    use smeltr::plan::topology::{Receiver, Signal, SourceKind, SourceMeta};

    let json =
        format!(r#"{{"version":1,"sources":[{{"id":"otel","type":"otlpReceiver","port":4318{RECEIVER_TOKEN}}}]}}"#);
    assert!(refusal(&json).is_empty(), "{}", refusal(&json));
    let system = load_str(&json).expect("must load");
    system.compile_topology().expect("must compile");
    let kind = &system.topology.sources[0].kind;
    let SourceKind::Receiver(r) = kind else {
        panic!("expected a receiver, got {kind:?}")
    };
    assert!(matches!(r.receiver, Receiver::Otlp(Signal::Logs)), "{:?}", r.receiver);
    assert_eq!(r.listen, "[::]:4318".parse().unwrap());
    assert_eq!(kind.meta(), SourceMeta::OTLP_LOGS.with_encoding(kind.encoding()));
    let e = refusal(r#"{"version":1,"sources":[{"id":"otel","type":"otlpReceiver","port":0}]}"#);
    assert!(e.contains("admits anyone who can reach its port"), "{e}");
    assert!(e.contains("listens on port 0"), "{e}");
}

/// A Prometheus receiver takes a port and a bearer token, and is held to what every receiver is.
#[test]
fn a_minimal_prometheus_receiver_loads() {
    use smeltr::plan::topology::{Receiver, SourceKind, SourceMeta};

    let json = format!(
        r#"{{"version":1,"sources":[{{"id":"prom","type":"prometheusReceiver","port":9090{RECEIVER_TOKEN}}}]}}"#
    );
    assert!(refusal(&json).is_empty(), "{}", refusal(&json));
    let system = load_str(&json).expect("must load");
    system.compile_topology().expect("must compile");
    let kind = &system.topology.sources[0].kind;
    let SourceKind::Receiver(r) = kind else {
        panic!("expected a receiver, got {kind:?}")
    };
    assert!(matches!(r.receiver, Receiver::Prometheus), "{:?}", r.receiver);
    assert_eq!(r.listen, "[::]:9090".parse().unwrap());
    assert_eq!(
        kind.meta(),
        SourceMeta::prometheus_receiver().with_encoding(kind.encoding())
    );
    let e = refusal(r#"{"version":1,"sources":[{"id":"prom","type":"prometheusReceiver","port":0}]}"#);
    assert!(e.contains("admits anyone who can reach its port"), "{e}");
    assert!(e.contains("listens on port 0"), "{e}");
}

/// An Elasticsearch receiver takes a port and a token, and is held to what every receiver is.
#[test]
fn a_minimal_elastic_receiver_loads() {
    use smeltr::plan::topology::{Receiver, SourceKind, SourceMeta};

    let json =
        format!(r#"{{"version":1,"sources":[{{"id":"bulk","type":"elasticReceiver","port":9200{RECEIVER_TOKEN}}}]}}"#);
    assert!(refusal(&json).is_empty(), "{}", refusal(&json));
    let system = load_str(&json).expect("must load");
    system.compile_topology().expect("must compile");
    let kind = &system.topology.sources[0].kind;
    let SourceKind::Receiver(r) = kind else {
        panic!("expected a receiver, got {kind:?}")
    };
    assert!(matches!(r.receiver, Receiver::Elastic), "{:?}", r.receiver);
    assert_eq!(r.listen, "[::]:9200".parse().unwrap());
    assert_eq!(kind.meta(), SourceMeta::ELASTIC_RECEIVER.with_encoding(kind.encoding()));
    let e = refusal(r#"{"version":1,"sources":[{"id":"bulk","type":"elasticReceiver","port":0}]}"#);
    assert!(e.contains("admits anyone who can reach its port"), "{e}");
    assert!(e.contains("listens on port 0"), "{e}");
}

/// A HEC receiver is held to what every receiver is: someone it admits, and a port.
#[test]
fn a_hec_receiver_that_admits_anyone_is_refused() {
    let e = refusal(r#"{"version":1,"sources":[{"id":"hec","type":"hecReceiver","port":0}]}"#);
    assert!(e.contains("admits anyone who can reach its port"), "{e}");
    assert!(e.contains("listens on port 0"), "{e}");
}

/// A receiver presents its certificate, so one with authorities and no certificate has nothing to serve.
#[test]
fn an_http_receiver_with_authorities_and_no_certificate_is_refused() {
    let json = r#"{"version":1,
        "sources":[{"id":"hook","type":"httpReceiver","port":8088,
            "trustedAuthorities":"-----BEGIN CERTIFICATE-----\nAA==\n-----END CERTIFICATE-----\n"}]}"#;
    assert!(
        refused_at(json, "certificate", "names no certificate"),
        "{}",
        refusal(json)
    );
}

/// The TLS settings of an S2S receiver that presents the certificate `s2s-tls` and admits a forwarder whose
/// certificate chains to its authorities, which `s2s_receiver_config` fills in.
const S2S_TLS: &str = r#","certificate":"s2s-tls","trustedAuthorities":"@authorities""#;

/// An S2S receiver of `extra` on port 9997, with [`S2S_TLS`]'s authorities filled in.
fn s2s_receiver_config(extra: &str) -> String {
    let key = rcgen::KeyPair::generate().unwrap();
    let params = rcgen::CertificateParams::new(vec!["indexer".to_string()]).unwrap();
    let pem = serde_json::to_string(&params.self_signed(&key).unwrap().pem()).unwrap();
    format!(
        r#"{{"version":1,
            "sources":[{{"id":"forwarders","type":"s2sReceiver","port":9997{extra}}}]}}"#
    )
    .replace(r#""@authorities""#, &pem)
}

const S2S_TOKEN: &str = r#","tokens":{"uf-fleet":"S2S_TOKEN"}"#;

/// An S2S receiver takes a port and a token, and breaks every sourcetype into an event per line.
#[test]
fn a_minimal_s2s_receiver_loads() {
    use smeltr::plan::topology::{Receiver, SecretRef, SourceKind, SourceMeta};

    let json =
        format!(r#"{{"version":1,"sources":[{{"id":"forwarders","type":"s2sReceiver","port":9997{S2S_TOKEN}}}]}}"#);
    assert!(refusal(&json).is_empty(), "{}", refusal(&json));
    let system = load_str(&json).expect("must load");
    system.compile_topology().expect("must compile");
    let kind = &system.topology.sources[0].kind;
    let SourceKind::Receiver(r) = kind else {
        panic!("expected a receiver, got {kind:?}")
    };
    let Receiver::S2s(s2s) = &r.receiver else {
        panic!("expected S2S, got {:?}", r.receiver)
    };
    assert!(s2s.sourcetypes.is_empty());
    assert_eq!(r.listen, "[::]:9997".parse().unwrap());
    assert_eq!(
        r.tokens,
        [("uf-fleet".to_string(), SecretRef::WorkerLocal("S2S_TOKEN".into()))]
    );
    assert!(r.tls.is_none());
    assert_eq!(kind.meta(), SourceMeta::S2S_RECEIVER.with_encoding(kind.encoding()));
    assert_eq!(
        kind.listens().map(|l| (l.transport, l.address)),
        Some((smeltr::plan::topology::Transport::Tcp, r.listen))
    );
}

/// Each sourcetype named breaks into events where its line breaker breaks; the file is written back as it was given.
#[test]
fn a_full_s2s_receiver_loads_and_writes_back_as_it_was_given() {
    use smeltr::plan::topology::{Receiver, SourceKind};

    let json = s2s_receiver_config(&format!(
        r#"{S2S_TLS},"maxRequestBytes":2048,
           "tokens":{{"uf-fleet":"S2S_TOKEN","hf":{{"secret":"heavy","version":2}}}},
           "sourcetypes":{{"java:app":{{"lineBreaker":"([\\r\\n]+)\\d{{4}}-"}},"xml":{{"lineBreaker":"</event>(\\s*)"}}}}"#
    ));
    assert!(refusal(&json).is_empty(), "{}", refusal(&json));
    let system = load_str(&json).expect("must load");
    let SourceKind::Receiver(r) = &system.topology.sources[0].kind else {
        panic!("expected a receiver")
    };
    let Receiver::S2s(s2s) = &r.receiver else {
        panic!("expected S2S")
    };
    let rules: Vec<(&str, &str)> = (s2s.sourcetypes.iter())
        .map(|(name, breaker)| (name.as_str(), breaker.0.as_str()))
        .collect();
    assert_eq!(rules, [("java:app", r"([\r\n]+)\d{4}-"), ("xml", r"</event>(\s*)")]);
    assert_eq!(r.listen, "[::]:9997".parse().unwrap());
    assert_eq!(r.max_request_bytes, 2048);
    assert_eq!(r.tokens.len(), 2);
    assert_eq!(r.tls.as_ref().unwrap().certificate.as_deref(), Some("s2s-tls"));

    let file: smeltr::config_file::FileConfig = serde_json::from_str(&json).unwrap();
    let written = &serde_json::to_value(&file).unwrap()["sources"][0];
    let given: serde_json::Value = serde_json::from_str::<serde_json::Value>(&json).unwrap()["sources"][0].clone();
    assert_eq!(written, &given);
}

/// A sourcetype with no line breaker, or Splunk's default `([\r\n]+)`, is an event per line, as one not named: it
/// takes no rule, and the file is written back as it was given.
#[test]
fn an_s2s_sourcetype_without_a_line_breaker_or_with_the_default_is_an_event_per_line() {
    use smeltr::plan::topology::{Receiver, SourceKind};

    let json = s2s_receiver_config(&format!(
        r#"{S2S_TOKEN},"sourcetypes":{{"plain":{{}},"default":{{"lineBreaker":"([\\r\\n]+)"}},"xml":{{"lineBreaker":"</event>(\\s*)"}}}}"#
    ));
    assert!(refusal(&json).is_empty(), "{}", refusal(&json));
    let system = load_str(&json).expect("must load");
    let SourceKind::Receiver(r) = &system.topology.sources[0].kind else {
        panic!("expected a receiver")
    };
    let Receiver::S2s(s2s) = &r.receiver else {
        panic!("expected S2S")
    };
    let named: Vec<&str> = s2s.sourcetypes.keys().map(String::as_str).collect();
    assert_eq!(named, ["xml"]);

    let file: smeltr::config_file::FileConfig = serde_json::from_str(&json).unwrap();
    let written = &serde_json::to_value(&file).unwrap()["sources"][0];
    let given: serde_json::Value = serde_json::from_str::<serde_json::Value>(&json).unwrap()["sources"][0].clone();
    assert_eq!(written, &given);
}

/// An S2S receiver is held to what every receiver is, and admits a forwarder whose certificate its authorities
/// vouch for without a token.
#[test]
fn an_s2s_receiver_is_held_to_what_every_receiver_is() {
    let e = refusal(r#"{"version":1,"sources":[{"id":"forwarders","type":"s2sReceiver","port":0}]}"#);
    assert!(e.contains("admits anyone who can reach its port"), "{e}");
    assert!(e.contains("listens on port 0"), "{e}");
    assert!(refused_at(
        &s2s_receiver_config(&format!(r#"{S2S_TOKEN},"maxRequestBytes":1"#)),
        "maxRequestBytes",
        "maxRequestBytes is between"
    ));
    assert!(refused_at(
        &s2s_receiver_config(r#","tokens":{" ":"S2S_TOKEN"}"#),
        "tokens",
        "blank name"
    ));
    assert!(refused_at(
        &s2s_receiver_config(&format!(r#"{S2S_TOKEN},"certificate":" ""#)),
        "certificate",
        "blank name"
    ));
    assert_eq!(refusal(&s2s_receiver_config(S2S_TLS)), "");
    let json = format!(
        r#"{{"version":1,"sources":[
            {{"id":"hec","type":"hecReceiver","port":9997{HEC_TOKEN}}},
            {{"id":"forwarders","type":"s2sReceiver","port":9997{S2S_TOKEN}}}]}}"#
    );
    let e = refusal(&json);
    assert!(
        e.contains(r#"source "forwarders" listens on port 9997, which source "hec" already does"#),
        "{e}"
    );
}

/// A sourcetype's line breaker is held to what an object source's is, and its refusal names the sourcetype.
#[test]
fn an_s2s_sourcetype_with_a_bad_line_breaker_or_name_is_refused() {
    let with = |sourcetypes: &str| s2s_receiver_config(&format!(r#"{S2S_TOKEN},"sourcetypes":{sourcetypes}"#));
    for (breaker, why) in [
        (r#"" ""#, "names a blank lineBreaker pattern"),
        (r#""(""#, r#"ends an event where "(" matches, which is not a pattern"#),
        (
            r#""\\n""#,
            r#"ends an event where "\n" matches, which has no capture group"#,
        ),
        (
            r#""(\\n*)""#,
            r#"ends an event where "(\n*)" matches, which can match no bytes at all"#,
        ),
    ] {
        let json = with(&format!(r#"{{"java:app":{{"lineBreaker":{breaker}}}}}"#));
        let why = format!(r#"source forwarders's sourcetype "java:app" {why}"#);
        assert!(refused_at(&json, "sourcetypes", &why), "{breaker}: {}", refusal(&json));
    }
    let json = with(r#"{" ":{"lineBreaker":"(\\n)"}}"#);
    assert!(refused_at(&json, "sourcetypes", "names a sourcetype by a blank name"));
    for unread in [
        r#"{"java:app":{"lineBreaker":"(\\n)","eventStart":"^x"}}"#,
        r#"{"java:app":{"lineBreaker":"(\\n)","maxLines":5}}"#,
    ] {
        let e = serde_json::from_str::<smeltr::config_file::FileConfig>(&with(unread)).unwrap_err();
        assert!(
            e.to_string().contains("lineBreaker") || e.to_string().contains("unknown field"),
            "{unread}: {e}"
        );
    }
}

/// A HEC receiver breaks a raw request's body by its sourcetype's line breaker, and refuses one as an S2S receiver
/// does.
#[test]
fn a_hec_receiver_takes_a_line_breaker_by_sourcetype() {
    use smeltr::plan::topology::{Receiver, SourceKind};

    let with = |sourcetypes: &str| {
        format!(
            r#"{{"version":1,"sources":[{{"id":"hec","type":"hecReceiver","port":8088{HEC_TOKEN},
            "sourcetypes":{sourcetypes}}}]}}"#
        )
    };
    let json = with(r#"{"app:xml":{"lineBreaker":"</event>(\\s*)"}}"#);
    assert!(refusal(&json).is_empty(), "{}", refusal(&json));
    let system = load_str(&json).expect("must load");
    let SourceKind::Receiver(r) = &system.topology.sources[0].kind else {
        panic!("expected a receiver")
    };
    let Receiver::Hec(hec) = &r.receiver else {
        panic!("expected HEC")
    };
    let rules: Vec<(&str, &str)> = (hec.sourcetypes.iter())
        .map(|(name, b)| (name.as_str(), b.0.as_str()))
        .collect();
    assert_eq!(rules, [("app:xml", r"</event>(\s*)")]);
    let json = with(r#"{"app:xml":{"lineBreaker":"\\n"}}"#);
    let why = r#"source hec's sourcetype "app:xml" ends an event where "\n" matches, which has no capture group"#;
    assert!(refused_at(&json, "sourcetypes", why), "{}", refusal(&json));
}

/// A syslog receiver takes a port and the networks its senders connect from, and serves plain TCP.
#[test]
fn a_minimal_syslog_receiver_loads() {
    use smeltr::plan::topology::{Receiver, SourceKind, SourceMeta};

    let json = r#"{"version":1,"sources":[{"id":"syslog","type":"syslogReceiver","port":1514,
        "allowFrom":["10.0.0.0/8","192.0.2.7"]}]}"#;
    assert!(refusal(json).is_empty(), "{}", refusal(json));
    let system = load_str(json).expect("must load");
    system.compile_topology().expect("must compile");
    let kind = &system.topology.sources[0].kind;
    let SourceKind::Receiver(r) = kind else {
        panic!("expected a receiver, got {kind:?}")
    };
    let Receiver::Syslog(syslog) = &r.receiver else {
        panic!("expected syslog, got {:?}", r.receiver)
    };
    let networks: Vec<String> = syslog.allow_from.iter().map(|n| n.to_string()).collect();
    assert_eq!(networks, ["10.0.0.0/8", "192.0.2.7/32"]);
    assert_eq!(r.listen, "[::]:1514".parse().unwrap());
    assert!(r.tokens.is_empty() && r.tls.is_none());
    assert_eq!(kind.meta(), SourceMeta::SYSLOG_RECEIVER.with_encoding(kind.encoding()));
    assert_eq!(
        kind.listens().map(|l| (l.transport, l.address)),
        Some((smeltr::plan::topology::Transport::Tcp, r.listen))
    );

    let file: smeltr::config_file::FileConfig = serde_json::from_str(json).unwrap();
    let written = &serde_json::to_value(&file).unwrap()["sources"][0];
    let given: serde_json::Value = serde_json::from_str::<serde_json::Value>(json).unwrap()["sources"][0].clone();
    assert_eq!(written, &given, "a syslog receiver writes back as it was given");
}

/// A syslog receiver carries no token, so it admits senders by their network or by a certificate its authorities
/// vouch for, and is held to what every receiver's listener is.
#[test]
fn a_syslog_receiver_admits_by_network_or_certificate() {
    let syslog = |extra: &str| {
        format!(r#"{{"version":1,"sources":[{{"id":"syslog","type":"syslogReceiver","port":1514{extra}}}]}}"#)
    };
    assert!(refused_at(
        &syslog(""),
        "allowFrom",
        "admits anyone who can reach its port"
    ));
    assert!(refused_at(
        &syslog(r#","allowFrom":["10.0.0.0/33"]"#),
        "allowFrom",
        "which is not a network"
    ));
    assert!(refused_at(
        &syslog(r#","allowFrom":["anywhere"]"#),
        "allowFrom",
        "which is not a network"
    ));
    let e = serde_json::from_str::<smeltr::config_file::FileConfig>(&syslog(r#","tokens":{"t":"T"}"#)).unwrap_err();
    assert!(
        e.to_string().contains("unknown field `tokens`"),
        "a syslog receiver takes no token: {e}"
    );
    let vouched = s2s_receiver_config(S2S_TLS).replace(
        r#""type":"s2sReceiver","port":9997"#,
        r#""type":"syslogReceiver","port":6514"#,
    );
    assert_eq!(refusal(&vouched), "", "mutual TLS admits without allowFrom");
    let json = format!(
        r#"{{"version":1,"sources":[
            {{"id":"forwarders","type":"s2sReceiver","port":1514{S2S_TOKEN}}},
            {{"id":"syslog","type":"syslogReceiver","port":1514,"allowFrom":["10.0.0.0/8"]}}]}}"#
    );
    let e = refusal(&json);
    assert!(
        e.contains(r#"source "syslog" listens on port 1514, which source "forwarders" already does"#),
        "{e}"
    );
}

/// A syslog receiver's line breaker ends a message over TCP where it breaks, and is refused where it could not: over
/// UDP, or where any line breaker is.
#[test]
fn a_syslog_receiver_takes_a_line_breaker_over_tcp() {
    use smeltr::plan::topology::{Receiver, SourceKind};

    let syslog = |extra: &str| {
        format!(
            r#"{{"version":1,"sources":[{{"id":"syslog","type":"syslogReceiver","port":1514,
            "allowFrom":["10.0.0.0/8"]{extra}}}]}}"#
        )
    };
    let json = syslog(r#","lineBreaker":"(\\x00)""#);
    assert!(refusal(&json).is_empty(), "{}", refusal(&json));
    let system = load_str(&json).expect("must load");
    let SourceKind::Receiver(r) = &system.topology.sources[0].kind else {
        panic!("a receiver")
    };
    let Receiver::Syslog(syslog_config) = &r.receiver else {
        panic!("a syslog receiver")
    };
    assert_eq!(
        syslog_config.line_breaker.as_ref().map(|b| b.0.as_str()),
        Some(r"(\x00)")
    );
    let file: smeltr::config_file::FileConfig = serde_json::from_str(&json).unwrap();
    let written = &serde_json::to_value(&file).unwrap()["sources"][0];
    assert_eq!(written["lineBreaker"], r"(\x00)", "it writes back as it was given");

    assert!(refused_at(
        &syslog(r#","transport":"udp","lineBreaker":"(\\x00)""#),
        "lineBreaker",
        r#"source "syslog" sets lineBreaker, which only TCP reads"#
    ));
    assert!(refused_at(
        &syslog(r#","lineBreaker":"\\x00""#),
        "lineBreaker",
        "which has no capture group to mark the break"
    ));
}

/// A syslog receiver detects each message's encoding unless it declares one, since syslog over TCP names no charset.
#[test]
fn a_syslog_receiver_declares_its_encoding() {
    use smeltr::format::encoding::Encoding;
    let syslog = |extra: &str| {
        format!(
            r#"{{"version":1,"sources":[{{"id":"syslog","type":"syslogReceiver","port":1514,
            "allowFrom":["10.0.0.0/8"]{extra}}}]}}"#
        )
    };
    let encoding = |json: &str| load_str(json).expect("must load").topology.sources[0].kind.encoding();
    assert_eq!(encoding(&syslog("")), Encoding::Auto);
    assert_eq!(
        encoding(&syslog(r#","encoding":"windows-1252""#)),
        Encoding::for_label("windows-1252").unwrap()
    );
    assert!(refused_at(
        &syslog(r#","encoding":"klingon""#),
        "encoding",
        "not a name smeltr knows"
    ));
}

/// A syslog receiver may take UDP, and writes it back as given; it writes no transport for TCP, as absent is TCP.
#[test]
fn a_syslog_receiver_takes_udp() {
    use smeltr::plan::topology::{Receiver, SourceKind, Transport};

    let json = r#"{"version":1,"sources":[{"id":"syslog","type":"syslogReceiver","port":514,
        "allowFrom":["10.0.0.0/8"],"transport":"udp"}]}"#;
    assert!(refusal(json).is_empty(), "{}", refusal(json));
    let system = load_str(json).expect("must load");
    system.compile_topology().expect("must compile");
    let kind = &system.topology.sources[0].kind;
    let SourceKind::Receiver(r) = kind else {
        panic!("expected a receiver, got {kind:?}")
    };
    let Receiver::Syslog(syslog) = &r.receiver else {
        panic!("expected syslog, got {:?}", r.receiver)
    };
    assert_eq!(syslog.transport, Transport::Udp);
    assert_eq!(r.listens().transport, Transport::Udp);
    assert_eq!(
        kind.listens().map(|l| (l.transport, l.address)),
        Some((Transport::Udp, "[::]:514".parse().unwrap()))
    );

    let written = |json: &str| {
        let file: smeltr::config_file::FileConfig = serde_json::from_str(json).unwrap();
        serde_json::to_value(&file).unwrap()["sources"][0].clone()
    };
    let given: serde_json::Value = serde_json::from_str::<serde_json::Value>(json).unwrap()["sources"][0].clone();
    assert_eq!(
        written(json),
        given,
        "a UDP syslog receiver writes back as it was given"
    );
    let tcp = written(&json.replace(r#""transport":"udp""#, r#""transport":"tcp""#));
    assert!(tcp.get("transport").is_none(), "TCP is written as absent: {tcp}");
}

/// Over UDP a syslog receiver serves no TLS, so it takes no certificate, and only its networks can admit a sender.
#[test]
fn a_udp_syslog_receiver_admits_only_by_network() {
    let vouched = |extra: &str| {
        s2s_receiver_config(&format!(r#"{S2S_TLS},"transport":"udp"{extra}"#)).replace(
            r#""type":"s2sReceiver","port":9997"#,
            r#""type":"syslogReceiver","port":6514"#,
        )
    };
    assert!(refused_at(&vouched(""), "certificate", "takes UDP"));
    assert!(
        refused_at(&vouched(""), "allowFrom", "admits anyone who can reach its port"),
        "a receiver's authorities admit no one over UDP: {}",
        refusal(&vouched(""))
    );
    let with_networks = vouched(r#","allowFrom":["10.0.0.0/8"]"#);
    assert!(refused_at(&with_networks, "certificate", "takes UDP"));
    assert!(
        !refused_at(&with_networks, "allowFrom", ""),
        "{}",
        refusal(&with_networks)
    );
}

/// A TCP port and a UDP port are two ports whatever their number, so a syslog receiver of each may share one; two
/// over UDP may not.
#[test]
fn syslog_receivers_share_a_port_only_across_transports() {
    let two = |first: &str, second: &str| {
        format!(
            r#"{{"version":1,"sources":[
            {{"id":"a","type":"syslogReceiver","port":514,"allowFrom":["10.0.0.0/8"]{first}}},
            {{"id":"b","type":"syslogReceiver","port":514,"allowFrom":["10.0.0.0/8"]{second}}}]}}"#
        )
    };
    let across = two("", r#","transport":"udp""#);
    assert!(refusal(&across).is_empty(), "{}", refusal(&across));
    load_str(&across).expect("must load");
    let both_udp = two(r#","transport":"udp""#, r#","transport":"udp""#);
    assert!(
        refused_at(
            &both_udp,
            "port",
            r#"listens on port 514, which source "a" already does"#
        ),
        "{}",
        refusal(&both_udp)
    );
}

/// One PostgreSQL source reading `auth.events` by `mode`, its password in the leader's store, with `extra` spliced
/// into its object.
fn postgres_config(query: &str, mode: &str, extra: &str) -> String {
    format!(
        r#"{{"version":1,
            "sources":[{{"id":"pg","type":"postgres","host":" db.internal ","database":"auth","user":"reader",
                         "password":{{"secret":"pg","version":2}},
                         "query":{query:?},"mode":{mode},
                         "fields":[{{"name":"event_id","type":"i64"}},{{"name":"occurred_at","type":"datetime"}},
                                   {{"name":"user_name","type":"str"}},{{"name":"tags","type":"array<str>"}}]{extra}}}]}}"#
    )
}

const EVENTS: &str = "SELECT event_id, occurred_at, user_name, tags FROM auth.events";
const BY_TIME: &str = r#"{"type":"incrementing","position":["occurred_at","event_id"]}"#;

/// A PostgreSQL source declares its record, waits 30 seconds behind a time position by default, and is read by one
/// worker under a place that names its database.
#[test]
fn a_postgres_source_loads_with_the_defaults() {
    use smeltr::core::column::DataType;
    use smeltr::io::sql_source::{Mode, Position, Start};
    use smeltr::plan::topology::{SecretRef, SourceKind, SqlDatabase};

    let json = postgres_config(EVENTS, BY_TIME, "");
    assert!(refusal(&json).is_empty(), "{}", refusal(&json));
    let system = load_str(&json).expect("must load");
    system.compile_topology().expect("must compile");
    let kind = &system.topology.sources[0].kind;
    let SourceKind::Sql(sql) = kind else {
        panic!("expected an SQL source, got {kind:?}")
    };
    let SqlDatabase::Postgres(connection) = &sql.database else {
        panic!("expected a PostgreSQL database, got {:?}", sql.database)
    };
    assert_eq!(connection.host, "db.internal");
    assert_eq!(connection.port, 5432);
    assert!(matches!(
        connection.encrypted,
        smeltr::io::sql_source::Encrypted::Require
    ));
    assert_eq!(
        sql.source.mode,
        Mode::Incrementing(Position {
            columns: vec!["occurred_at".into(), "event_id".into()],
            settle_secs: Some(30),
            start: Start::Now,
        })
    );
    assert_eq!(sql.source.batch, 5000);
    assert_eq!(sql.source.poll_interval, std::time::Duration::from_secs(60));
    assert_eq!(&*sql.source.place, "db.internal:5432/auth");
    assert_eq!(
        kind.meta().record_fields(),
        [
            ("event_id", DataType::I64),
            ("occurred_at", DataType::DateTime),
            ("user_name", DataType::Str),
            ("tags", DataType::Array(Box::new(DataType::Str))),
        ]
    );
    let place = kind.single_worker().expect("one worker reads it");
    assert!(place.starts_with("postgres://db.internal:5432/auth#"), "{place}");
    let key = SecretRef::LeaderStored {
        name: "pg".into(),
        version: 2,
    };
    let named = &system.topology.named_secrets;
    let named: Vec<_> = named.iter().map(|n| (&n.reference, n.what.as_str())).collect();
    assert_eq!(named, vec![(&key, r#"source "pg": password"#)]);
}

/// A position mode begins now unless told otherwise, at the beginning, or from a value of its first column's type.
#[test]
fn a_postgres_source_begins_where_its_start_says() {
    use smeltr::io::sql_source::{Mode, Start};
    use smeltr::io::values::Value;

    let start = |mode: &str| {
        let system = load_str(&postgres_config(EVENTS, mode, "")).expect("must load");
        let smeltr::plan::topology::SourceKind::Sql(sql) = &system.topology.sources[0].kind else {
            panic!("expected an SQL source")
        };
        let (Mode::Incrementing(p) | Mode::UpdatedAt(p)) = &sql.source.mode else {
            panic!("expected a position mode")
        };
        p.start.clone()
    };
    assert_eq!(
        start(r#"{"type":"incrementing","position":["occurred_at"],"start":"beginning"}"#),
        Start::Beginning
    );
    assert_eq!(
        start(
            r#"{"type":"updatedAt","position":["occurred_at"],"start":"from","startFrom":"2026-01-31T09:00:00+01:00"}"#
        ),
        Start::From(Value::Time(1_769_846_400_000_000))
    );
    assert_eq!(
        start(
            r#"{"type":"incrementing","position":["occurred_at"],"start":"from","startFrom":"2026-01-31T08:00:00.5Z"}"#
        ),
        Start::From(Value::Time(1_769_846_400_500_000))
    );
    assert_eq!(
        start(r#"{"type":"incrementing","position":["event_id"],"start":"from","startFrom":7}"#),
        Start::From(Value::Int(7))
    );
}

/// Its progress is kept under the query and what its mode remembers by, so changing either starts afresh, and
/// changing how often it polls does not.
#[test]
fn a_postgres_source_starts_afresh_only_when_what_it_reads_changes() {
    let place = |query: &str, mode: &str, extra: &str| {
        let system = load_str(&postgres_config(query, mode, extra)).expect("must load");
        system.topology.sources[0].kind.single_worker().unwrap()
    };
    let first = place(EVENTS, BY_TIME, "");
    assert_eq!(place(EVENTS, BY_TIME, r#","pollIntervalSecs":5,"batch":10"#), first);
    assert_ne!(place(&format!("{EVENTS} WHERE outcome = 'x'"), BY_TIME, ""), first);
    assert_ne!(
        place(
            EVENTS,
            r#"{"type":"updatedAt","position":["occurred_at","event_id"]}"#,
            ""
        ),
        first
    );
    assert_ne!(
        place(EVENTS, r#"{"type":"incrementing","position":["event_id"]}"#, ""),
        first
    );
    assert_ne!(place(EVENTS, BY_TIME, r#","port":6432"#), first);
}

/// A pipeline reads the source's fields by name, as typed.
#[test]
fn a_pipeline_reads_a_postgres_source_s_fields() {
    let json = format!(
        r#"{{"version":1,
            "sources":[{{"id":"pg","type":"postgres","host":"db","database":"auth","user":"reader",
                         "query":{EVENTS:?},"mode":{BY_TIME},
                         "fields":[{{"name":"event_id","type":"i64"}},{{"name":"occurred_at","type":"datetime"}},
                                   {{"name":"user_name","type":"str"}},{{"name":"tags","type":"array<str>"}}]}}],
            "destinations":[{KAFKA_OUT}],
            "pipelines":[{{"id":"p","name":"p","steps":[
              {{"id":"e","type":"eval","assignments":[{{"target":"next","expr":"event_id + 1"}}]}}]}}],
            "routes":[{{"id":"r","source":"pg","condition":"user_name != \"svc\"","pipeline":"p",
                        "deliver":[{{"to":["d"]}}]}}]}}"#
    );
    assert!(refusal(&json).is_empty(), "{}", refusal(&json));
    let system = load_str(&json).expect("must load");
    system.compile_topology().expect("a pipeline reads the declared fields");
}

#[test]
fn a_postgres_source_with_bad_settings_is_refused() {
    let by = |position: &str| format!(r#"{{"type":"incrementing","position":{position}}}"#);
    let start = |start: &str| format!(r#"{{"type":"incrementing","position":["occurred_at","event_id"],{start}}}"#);
    let refused = [
        (
            postgres_config("UPDATE auth.events SET x = 1", BY_TIME, ""),
            "query",
            "UPDATE",
        ),
        (
            postgres_config(&format!("{EVENTS} ORDER BY event_id"), BY_TIME, ""),
            "query",
            "ORDER BY",
        ),
        (postgres_config(EVENTS, &by("[]"), ""), "mode", "names no position"),
        (
            postgres_config(EVENTS, &by(r#"["seen_at"]"#), ""),
            "mode",
            "not one of its fields",
        ),
        (
            postgres_config(EVENTS, &by(r#"["user_name"]"#), ""),
            "mode",
            "neither an i64 nor a datetime",
        ),
        (
            postgres_config(EVENTS, &by(r#"["event_id","event_id"]"#), ""),
            "mode",
            "twice",
        ),
        (
            postgres_config(EVENTS, &by(r#"["event_id","tags"]"#), ""),
            "mode",
            "tells rows apart by `tags`",
        ),
        (
            postgres_config(EVENTS, r#"{"type":"updatedAt","position":["event_id"]}"#, ""),
            "mode",
            "not a datetime",
        ),
        (
            postgres_config(
                EVENTS,
                r#"{"type":"incrementing","position":["event_id"],"settleSecs":10}"#,
                "",
            ),
            "mode",
            "only a time can wait",
        ),
        (
            postgres_config(EVENTS, &start(r#""start":"from""#), ""),
            "mode",
            "give startFrom",
        ),
        (
            postgres_config(EVENTS, &start(r#""startFrom":"2026-01-31T09:00:00Z""#), ""),
            "mode",
            "does not start from it",
        ),
        (
            postgres_config(EVENTS, &start(r#""start":"from","startFrom":"yesterday""#), ""),
            "mode",
            "not a time",
        ),
        (
            postgres_config(
                EVENTS,
                &start(r#""start":"from","startFrom":"2026-01-31T09:00:00""#),
                "",
            ),
            "mode",
            "not a time",
        ),
        (
            postgres_config(
                EVENTS,
                r#"{"type":"incrementing","position":["event_id"],"start":"from","startFrom":"7"}"#,
                "",
            ),
            "mode",
            "not an integer",
        ),
        (
            postgres_config(EVENTS, r#"{"type":"comparing","key":[]}"#, ""),
            "mode",
            "by no key",
        ),
        (
            postgres_config(EVENTS, r#"{"type":"comparing","key":["sku"]}"#, ""),
            "mode",
            "`sku`",
        ),
        (
            postgres_config(
                &format!("{EVENTS} WHERE event_id > :last.event_id"),
                r#"{"type":"everyPoll"}"#,
                "",
            ),
            "query",
            "only a mode that reads past",
        ),
        (
            postgres_config(&format!("{EVENTS} WHERE x > :last.x"), BY_TIME, ""),
            "query",
            ":last.x",
        ),
        (
            postgres_config(EVENTS, BY_TIME, "").replace(r#"" db.internal ""#, r#"" ""#),
            "host",
            "names no host",
        ),
        (postgres_config(EVENTS, BY_TIME, r#","port":0"#), "port", "port 0"),
        (
            postgres_config(EVENTS, BY_TIME, r#","batch":0"#),
            "batch",
            "between 1 and",
        ),
        (
            postgres_config(EVENTS, BY_TIME, r#","batch":100001"#),
            "batch",
            "between 1 and",
        ),
        (
            postgres_config(EVENTS, BY_TIME, r#","pollIntervalSecs":0"#),
            "pollIntervalSecs",
            "at least 1",
        ),
        (
            postgres_config(EVENTS, BY_TIME, r#","certificate":" ""#),
            "certificate",
            "blank name",
        ),
        (
            postgres_config(EVENTS, BY_TIME, r#","certificate":"acme","tls":"disable""#),
            "certificate",
            "turns it off",
        ),
    ];
    for (json, field, says) in refused {
        assert!(
            refused_at(&json, field, says),
            "wanted {field}: {says}, got {}",
            refusal(&json)
        );
    }
    let bad_fields = [
        (r#"[{"name":"a","type":"number"}]"#, "not a type"),
        (r#"[{"name":"__source.x","type":"str"}]"#, "not a name"),
        (
            r#"[{"name":"a","type":"str"},{"name":"a.b","type":"str"}]"#,
            "one holds the other",
        ),
        ("[]", "at least one field"),
    ];
    for (fields, says) in bad_fields {
        let json = format!(
            r#"{{"version":1,"sources":[{{"id":"pg","type":"postgres","host":"db","database":"auth","user":"r",
                "query":"SELECT a FROM t","mode":{{"type":"everyPoll"}},"fields":{fields}}}]}}"#
        );
        assert!(
            refused_at(&json, "fields", says),
            "{fields}: wanted {says}, got {}",
            refusal(&json)
        );
    }
}

/// An HTTP API source signs in as an app with a secret at a token endpoint, or with a key through an issuer, and
/// the worker is handed the credential it signs in with.
#[test]
fn an_http_api_source_signs_in_as_an_app() {
    use smeltr::plan::topology::{AppProof, AppSignIn, SecretRef, TokenAt};

    let json = http_api_config(
        "https://api.example/v1/events",
        r#","clientId":" app ","clientSecret":{"secret":"app","version":2},
           "tokenUrl":"https://idp.example/connect/token","scope":"read""#,
    );
    assert!(refusal(&json).is_empty(), "{}", refusal(&json));
    let system = load_str(&json).expect("must load");
    let secret = SecretRef::LeaderStored {
        name: "app".into(),
        version: 2,
    };
    let Some(AppSignIn {
        client_id,
        token_at: TokenAt::Url(url),
        scope,
        proof: AppProof::Secret(held),
    }) = &http_api(&system).sign_in
    else {
        panic!("a secret sign-in at a token URL: {:?}", http_api(&system).sign_in);
    };
    assert_eq!(
        (client_id.as_str(), url.as_str()),
        ("app", "https://idp.example/connect/token")
    );
    assert_eq!(scope.as_deref(), Some("read"));
    assert_eq!(held, &secret);
    let named = &system.topology.named_secrets;
    let named: Vec<_> = named.iter().map(|n| (&n.reference, n.what.as_str())).collect();
    assert_eq!(named, vec![(&secret, r#"source "g": clientSecret"#)]);

    let json = http_api_config(
        "https://api.example/v1/events",
        r#","clientId":"app","privateKey":{"secret":"key","version":1},"keyId":"k1",
           "issuer":"https://idp.example/realms/a""#,
    );
    assert!(refusal(&json).is_empty(), "{}", refusal(&json));
    let system = load_str(&json).expect("must load");
    let Some(AppSignIn {
        token_at: TokenAt::Issuer(issuer),
        scope: None,
        proof: AppProof::Key { key_id, .. },
        ..
    }) = &http_api(&system).sign_in
    else {
        panic!("a key sign-in through an issuer: {:?}", http_api(&system).sign_in);
    };
    assert_eq!(issuer, "https://idp.example/realms/a");
    assert_eq!(key_id.as_deref(), Some("k1"));
}

/// An app's sign-in names one app, one proof and one place to sign in, over TLS, and nothing else sets the header
/// its token goes in.
#[test]
fn an_http_api_source_with_a_muddled_sign_in_is_refused() {
    let at = "https://api.example/v1/events";
    let client = r#","clientId":"app""#;
    let secret = r#","clientSecret":{"secret":"app","version":1}"#;
    let key = r#","privateKey":{"secret":"key","version":1}"#;
    let url = r#","tokenUrl":"https://idp.example/token""#;
    let issuer = r#","issuer":"https://idp.example""#;
    for (sign_in, field, says) in [
        (format!("{secret}{url}"), "clientId", "no clientId"),
        (format!(r#","clientId":" "{secret}{url}"#), "clientId", "no clientId"),
        (
            format!("{client}{url}"),
            "clientSecret",
            "no clientSecret or privateKey",
        ),
        (
            format!("{client}{secret}{key}{url}"),
            "clientSecret",
            "both a clientSecret and a privateKey",
        ),
        (
            format!(r#"{client}{secret}{url},"keyId":"k1""#),
            "keyId",
            "with no privateKey",
        ),
        (format!("{client}{secret}"), "tokenUrl", "no tokenUrl or issuer"),
        (
            format!("{client}{secret}{url}{issuer}"),
            "tokenUrl",
            "both a tokenUrl and an issuer",
        ),
        (
            format!(r#"{client}{secret},"issuer":"http://idp.example""#),
            "issuer",
            "is plain HTTP",
        ),
        (
            format!(r#"{client}{secret},"issuer":"https://idp.example?x=1""#),
            "issuer",
            "has a query",
        ),
        (
            format!(r#"{client}{secret}{url},"credentials":{{"authorization":{{"secret":"t","version":1}}}}"#),
            "credentials",
            "signs in as an app",
        ),
    ] {
        let json = http_api_config(at, &sign_in);
        assert!(
            refused_at(&json, field, says),
            "{sign_in}: wanted {field}: {says}, got {}",
            refusal(&json)
        );
    }
    // Plain HTTP to a token endpoint or to the API loads, as it does for every credential; the GUI warns of it.
    for (at, sign_in) in [
        ("http://api.example/v1/events", format!("{client}{secret}{url}")),
        (at, format!(r#"{client}{secret},"tokenUrl":"http://idp.example/token""#)),
    ] {
        let json = http_api_config(at, &sign_in);
        assert!(refusal(&json).is_empty(), "{sign_in}: {}", refusal(&json));
    }
}
