//! The published function reference (`website/src/lib/reference.json`) checked
//! against the engine: every `StdFunc`, map, filter and object function is documented exactly once, and every
//! example on the page is evaluated and must produce the result it claims.
//!
//! A reference nobody runs drifts: an example is a promise about what a
//! function *does*. The GUI's autocomplete reads its functions from this
//! reference too (`gui-frontend/src/stdlib.ts`).

use smeltr::core::batch::Batch;
use smeltr::core::buffer::BufferPool;
use smeltr::core::ctx::Ctx;
use smeltr::core::error::ErrorSink;
use smeltr::expr::inline::PRELUDE_FUNCTIONS;
use smeltr::ops::objectops::ObjectOp;
use smeltr::ops::stdfuncs;
use smeltr::ops::{BinOp, UnOp};
use smeltr::plan::config::PipelineConfig;
use smeltr::plan::output::{run_to_json, run_to_json_marked};
use smeltr::plan::plan::compile;
use smeltr::plan::topology::{Placement, SourceMeta};

use crate::harness;

#[derive(serde::Deserialize)]
struct Reference {
    groups: Vec<Group>,
    exceptions: Vec<ExceptionRow>,
    #[serde(rename = "recordFields")]
    record_fields: Vec<RecordField>,
    types: Vec<TypeRow>,
    /// The examples How it works shows, by topic.
    topics: Vec<Topic>,
}

/// One row of the types table: the type as a declaration spells it, what it holds, and a value of it.
#[derive(serde::Deserialize)]
struct TypeRow {
    name: String,
    holds: String,
    /// The type the example is declared as, where `name` is generic.
    #[serde(default)]
    declare: Option<String>,
    #[serde(default)]
    example: Option<Example>,
}

#[derive(serde::Deserialize)]
struct Topic {
    #[serde(default)]
    examples: Vec<Example>,
    #[serde(default)]
    rejected: Vec<Rejected>,
}

/// One row of the record fields table.
#[derive(serde::Deserialize)]
struct RecordField {
    name: String,
    #[serde(rename = "type")]
    ty: String,
    #[serde(rename = "setBy")]
    set_by: String,
}

/// One row of the exception kinds table: a kind, its category, and when it happens.
#[derive(serde::Deserialize)]
struct ExceptionRow {
    kind: String,
    category: String,
}

/// One line of the `parseTime` format table: the specifier, what it matches, and a parse that shows it.
#[derive(serde::Deserialize)]
struct SpecifierRow {
    specifier: String,
    matches: String,
    examples: Vec<Example>,
    /// The label over the run of rows this one belongs to, drawn once above
    /// the run.
    #[serde(default)]
    group: Option<String>,
}

/// One line of an operator table: the symbol, what it does under these operand types, and an example or two.
#[derive(serde::Deserialize)]
struct OperatorRow {
    symbol: String,
    gloss: String,
    examples: Vec<Example>,
}

#[derive(serde::Deserialize)]
struct Rejected {
    expr: String,
    why: String,
}

/// One type category: the operators that produce it, then the functions over it.
#[derive(serde::Deserialize)]
struct Group {
    id: String,
    title: String,
    blurb: String,
    #[serde(default)]
    rows: Vec<OperatorRow>,
    #[serde(default)]
    notes: Vec<String>,
    /// Expressions the page shows as compile errors.
    #[serde(default)]
    rejected: Vec<Rejected>,
    functions: Vec<Function>,
}

#[derive(serde::Deserialize)]
struct Function {
    name: String,
    signature: String,
    summary: String,
    /// The JSON is read by TypeScript too, where the key is camelCase.
    #[serde(default, rename = "specifierBlurb")]
    specifier_blurb: Option<String>,
    #[serde(default)]
    specifiers: Vec<SpecifierRow>,
    #[serde(default)]
    notes: Vec<String>,
    examples: Vec<Example>,
    /// The label over the run of functions this one belongs to.
    #[serde(default)]
    group: Option<String>,
}

#[derive(serde::Deserialize)]
struct Example {
    expr: String,
    /// A JSON literal the expression must produce, as text. Absent when the
    /// value is not the same twice -- anything reading the clock.
    #[serde(default)]
    result: Option<String>,
    /// `name = expr` bindings evaluated before `expr`, so an example can read
    /// like pipeline code instead of a wall of literals.
    #[serde(default)]
    given: Vec<String>,
    /// What this one example is here to show, rendered beside it as a comment.
    #[serde(default)]
    note: Option<String>,
    /// The exception kinds it throws, in kind order. Empty when it throws none.
    #[serde(default)]
    throws: Vec<String>,
    /// The label over the run of examples this one belongs to, drawn once above
    /// the run.
    #[serde(default)]
    group: Option<String>,
}

fn reference() -> Reference {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("website/src/lib/reference.json");
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("{} is not the expected shape: {e}", path.display()))
}

/// The typed rows of every function, as `website/src/lib/signatures.json` holds them: name to a list of
/// `{params, result}`. The page prints them under each heading.
fn engine_signatures() -> serde_json::Value {
    let mut map = serde_json::Map::new();
    for row in stdfuncs::ROWS {
        if map.contains_key(row.name) {
            continue;
        }
        let rows: Vec<serde_json::Value> = row
            .func
            .typed_rows()
            .into_iter()
            .map(|(params, result)| serde_json::json!({ "params": params, "result": result }))
            .collect();
        map.insert(row.name.to_string(), serde_json::Value::Array(rows));
    }
    serde_json::Value::Object(map)
}

/// The page lists every exception kind once, under the category the engine gives it.
#[test]
fn the_exception_kinds_table_matches_the_engine() {
    use smeltr::core::error::{Category, ExceptionKind};
    let label = |c: Category| Category::ALL.iter().find(|(_, k)| *k == c).unwrap().0;
    let mut want: Vec<(String, String)> = (ExceptionKind::KINDS.iter())
        .map(|(kind, c)| (kind.to_string(), label(*c).to_string()))
        .collect();
    let mut have: Vec<(String, String)> = (reference().exceptions.into_iter())
        .map(|r| (r.kind, r.category))
        .collect();
    want.sort();
    have.sort();
    assert_eq!(
        have, want,
        "website/src/lib/reference.json's `exceptions` differ from ExceptionKind::KINDS"
    );
}

/// The record fields table names each field a source gives, with its type; the record's raw text; the default tag
/// field; and every field a pipeline sets for its destinations, each of which the compiled plan must read as such.
#[test]
fn the_record_fields_table_matches_the_engine() {
    let r = reference();
    let row = |name: &str| {
        (r.record_fields.iter().find(|f| f.name == name))
            .unwrap_or_else(|| panic!("the record fields table has no `{name}`"))
    };
    for meta in [
        SourceMeta::KAFKA,
        SourceMeta::METRICS,
        SourceMeta::LOGS,
        SourceMeta::S3_SQS,
        SourceMeta::S3_LIST,
        SourceMeta::GCS_LIST,
        SourceMeta::AZURE_BLOB_LIST,
        SourceMeta::SFTP,
        SourceMeta::AZURE_BLOB_QUEUE,
        SourceMeta::GCS_PUB_SUB,
        SourceMeta::EVENT_HUBS,
        SourceMeta::HTTP_API,
        SourceMeta::HTTP_RECEIVER,
        SourceMeta::OKTA_LOGS,
        SourceMeta::POSTGRES,
    ] {
        for (name, ty) in meta.source_fields() {
            let f = row(name);
            assert_eq!(f.set_by, "source", "{name}");
            assert_eq!(f.ty, ty.to_string(), "{name}'s type");
        }
    }
    assert_eq!(row("_raw").set_by, "source");
    assert_eq!(row(smeltr::plan::config::DEFAULT_TAG_FIELD).set_by, "Smeltr");
    for field in Placement::ALL
        .map(Placement::field)
        .into_iter()
        .chain(["__destination.id"])
    {
        assert_eq!(row(field).set_by, "pipeline", "{field}");
    }
    for f in r.record_fields.iter().filter(|f| f.set_by == "pipeline") {
        let plan = compile(&out_is(&format!("{} = \"x\";\n", f.name), "1"), &SourceMeta::KAFKA)
            .unwrap_or_else(|_| panic!("`{}` cannot be written", f.name));
        let slot = plan
            .slot_names()
            .into_iter()
            .find(|(_, n)| *n == f.name)
            .map(|(s, _)| s);
        assert!(
            slot.is_some()
                && plan
                    .placement_slots
                    .into_iter()
                    .chain([plan.id_out_slot])
                    .any(|s| s == slot),
            "no destination reads `{}`",
            f.name
        );
    }
}

/// The page's type lines come from the engine's own table, so they cannot drift from it. `just signatures`
/// rewrites the file.
#[test]
fn the_signatures_file_matches_the_engine() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("website/src/lib/signatures.json");
    let want = serde_json::to_string_pretty(&engine_signatures()).unwrap() + "\n";
    if std::env::var_os("SMELTR_WRITE_SIGNATURES").is_some() {
        std::fs::write(&path, &want).unwrap_or_else(|e| panic!("write {}: {e}", path.display()));
        return;
    }
    let have = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    // Values, not bytes: a Windows checkout has CRLF line endings.
    let have: serde_json::Value = serde_json::from_str(&have).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    assert_eq!(
        have,
        engine_signatures(),
        "{} is stale; run `just signatures`",
        path.display()
    );
}

fn functions(r: &Reference) -> impl Iterator<Item = &Function> {
    r.groups.iter().flat_map(|g| g.functions.iter())
}

fn operator_rows(r: &Reference) -> impl Iterator<Item = &OperatorRow> {
    r.groups.iter().flat_map(|g| g.rows.iter())
}

fn specifier_rows(r: &Reference) -> impl Iterator<Item = &SpecifierRow> {
    functions(r).flat_map(|f| f.specifiers.iter())
}

/// Syntax the page documents alongside the operators that has no `BinOp`/`UnOp` of its own.
/// `.` and `$[]` spell a field name. `[]` indexes a value or spells a named field path.
const NOT_IN_THE_OPERATOR_ENUMS: [&str; 4] = [".", "?:", "[]", "$[]"];

/// One expression over a one-row batch, as the value of `out`, after any
/// `given` bindings, each a `name = expr` statement, and the kinds of the
/// exceptions they throw, once each in kind order. `None` is a null result:
/// the serializer omits a null field.
fn evaluate(expr: &str, given: &[String]) -> (Option<serde_json::Value>, Vec<&'static str>) {
    let body: String = given.iter().map(|g| format!("{g};\n")).collect();
    let plan = compile(&out_is(&body, expr), &SourceMeta::KAFKA).unwrap_or_else(|errs| {
        panic!(
            "`{expr}` does not compile: {}",
            errs.iter().map(|e| e.to_string()).collect::<Vec<_>>().join("; ")
        )
    });

    let mut pool = BufferPool::new();
    let mut errors = ErrorSink::new();
    let mut batch = Batch::new(&plan.slot_layout, 1, 0);
    let rows: Vec<serde_json::Value> = {
        let mut ctx = Ctx::new(&mut pool, &mut errors);
        run_to_json(&plan, &mut batch, &mut ctx)
            .iter()
            .map(|row| serde_json::from_slice(row).unwrap())
            .collect()
    };
    batch.reclaim(&mut pool);
    assert_eq!(rows.len(), 1, "`{expr}`: expected one row");
    let mut kinds: Vec<_> = errors.entries().iter().map(|a| a.kind.clone()).collect();
    kinds.sort_by_key(|k| k.index());
    kinds.dedup_by_key(|k| k.index());
    (rows[0].get("out").cloned(), kinds.iter().map(|k| k.label()).collect())
}

/// Whether the expression compiles at all. The page claims some do not.
fn compiles(expr: &str) -> bool {
    compile(&out_is("", expr), &SourceMeta::KAFKA).is_ok()
}

/// A pipeline that runs `body`, then `out = expr`.
fn out_is(body: &str, expr: &str) -> PipelineConfig {
    harness::pipeline(vec![harness::code(0, &format!("{body}out = {expr};"))])
}

#[test]
fn the_reference_documents_every_function_exactly_once() {
    let r = reference();
    let mut documented: Vec<&str> = functions(&r).map(|f| f.name.as_str()).collect();
    documented.sort_unstable();
    let before = documented.len();
    documented.dedup();
    assert_eq!(before, documented.len(), "a function is documented twice");

    // map/filter are expression syntax lowered to array instructions, and the object functions are object
    // instructions, not StdFunc variants.
    let mut defined: Vec<&str> = stdfuncs::ROWS
        .iter()
        .map(|row| row.name)
        .chain(PRELUDE_FUNCTIONS)
        .chain(ObjectOp::NAMED.iter().filter_map(|op| op.name()))
        .collect();
    defined.sort_unstable();
    defined.dedup();

    assert_eq!(
        documented, defined,
        "website/src/lib/reference.json and ops::stdfuncs have drifted apart"
    );
}

/// Group ids are HTML `id` attributes, the targets of the page's own table of
/// contents, and the keys the page renders the groups under.
#[test]
fn every_group_id_is_unique() {
    let r = reference();
    let mut ids: Vec<&str> = r.groups.iter().map(|g| g.id.as_str()).collect();
    ids.sort_unstable();
    let before = ids.len();
    ids.dedup();
    assert_eq!(before, ids.len(), "two groups share an id");
}

#[test]
fn the_reference_documents_every_operator() {
    let r = reference();
    // A symbol takes one row per operand type it serves, so rows repeat; the set of symbols is what must match.
    let mut documented: Vec<&str> = operator_rows(&r).map(|o| o.symbol.as_str()).collect();
    documented.sort_unstable();
    documented.dedup();

    // `-` is both `Sub` and `Neg`; one symbol covers both.
    let mut defined: Vec<&str> = BinOp::ALL
        .iter()
        .map(|op| op.symbol())
        .chain(UnOp::ALL.iter().map(|op| op.symbol()))
        .chain(NOT_IN_THE_OPERATOR_ENUMS)
        .collect();
    defined.sort_unstable();
    defined.dedup();

    assert_eq!(
        documented, defined,
        "website/src/lib/reference.json and ops::{{BinOp, UnOp}} have drifted apart"
    );
}

/// Someone writing a custom format reads `parseTime`'s table and nothing else, so it has to be the whole set:
/// a specifier missing from it is one nobody knows they have, and one listed that the parser does not take is a
/// format that silently fails.
#[test]
fn the_reference_documents_every_format_specifier() {
    let r = reference();
    let documented: Vec<&str> = specifier_rows(&r).map(|s| s.specifier.as_str()).collect();
    let mut unique = documented.clone();
    unique.sort_unstable();
    let before = unique.len();
    unique.dedup();
    assert_eq!(before, unique.len(), "a specifier is documented twice");

    assert_eq!(
        documented,
        smeltr::ops::timeparse::FORMAT_SPECIFIERS,
        "website/src/lib/reference.json and ops::timeparse::FORMAT_SPECIFIERS have drifted apart"
    );
}

/// Every example is a claim the engine has to honor. A `result` is the JSON
/// the output would carry; the comparison is over rendered JSON so a float
/// documented as an integer fails.
/// What is wrong with an example: the value or the exceptions it documents that the engine does not give.
fn check(ex: &Example) -> Vec<String> {
    let expr = ex.expr.as_str();
    let (actual, thrown) = evaluate(expr, &ex.given);
    let mut wrong = Vec::new();
    if thrown != ex.throws {
        wrong.push(format!("`{expr}` throws {thrown:?}, documented as {:?}", ex.throws));
    }
    let Some(expected) = ex.result.as_ref() else {
        return wrong;
    };
    let actual = actual.map(|v| serde_json::to_string(&v).unwrap());
    let want = (expected != "null").then(|| {
        let v: serde_json::Value =
            serde_json::from_str(expected).unwrap_or_else(|e| panic!("`{expected}` is not JSON: {e}"));
        serde_json::to_string(&v).unwrap()
    });
    if actual != want {
        let show = |v: &Option<String>| v.clone().unwrap_or_else(|| "null".into());
        wrong.push(format!(
            "`{expr}` produced {}, documented as {}",
            show(&actual),
            show(&want)
        ));
    }
    wrong
}

/// Panics listing every wrong example, so one run shows them all.
fn check_all<'a>(examples: impl Iterator<Item = &'a Example>) {
    let wrong: Vec<String> = examples.flat_map(check).collect();
    assert!(
        wrong.is_empty(),
        "{} wrong examples:\n{}",
        wrong.len(),
        wrong.join("\n")
    );
}

#[test]
fn every_documented_example_produces_its_documented_result() {
    let r = reference();
    let examples = functions(&r)
        .flat_map(|f| f.examples.iter())
        .chain(operator_rows(&r).flat_map(|row| row.examples.iter()))
        .chain(specifier_rows(&r).flat_map(|row| row.examples.iter()))
        .chain(r.topics.iter().flat_map(|t| t.examples.iter()));
    check_all(examples);
}

/// The types table names every type a declaration accepts, as the compiler's refusal of an unknown type lists them,
/// and each example is a value of its type: declared as it, it gives its documented result.
#[test]
fn the_types_table_matches_the_engine() {
    let r = reference();
    let errs = compile(&out_is("let v: nosuch = 1;\n", "v"), &SourceMeta::KAFKA).expect_err("an unknown type compiles");
    let message = errs.iter().map(|e| e.to_string()).collect::<String>();
    let list = message
        .split_once("(expected ")
        .and_then(|(_, rest)| rest.rsplit_once(')'))
        .unwrap_or_else(|| panic!("the unknown-type message lists no types: {message}"))
        .0;
    let (head, last) = list.rsplit_once(" or ").expect("the list ends in `or`");
    let mut engine: Vec<&str> = head.split(", ").chain([last]).collect();
    let mut documented: Vec<&str> = r.types.iter().map(|t| t.name.as_str()).collect();
    engine.sort_unstable();
    documented.sort_unstable();
    assert_eq!(
        documented, engine,
        "website/src/lib/reference.json's `types` differ from the type parser"
    );
    let mut declared = Vec::new();
    for t in &r.types {
        assert!(!t.holds.trim().is_empty(), "{} holds nothing", t.name);
        let Some(ex) = &t.example else { continue };
        let ty = t.declare.as_deref().unwrap_or(&t.name);
        let mut given = ex.given.clone();
        given.push(format!("let v: {ty} = {}", ex.expr));
        declared.push(Example {
            expr: "v".into(),
            result: ex.result.clone(),
            given,
            note: None,
            throws: ex.throws.clone(),
            group: None,
        });
    }
    check_all(declared.iter());
}

/// The page's point in showing these is that they are refused when the pipeline compiles, rather than being null when
/// it runs. An expression that started compiling would make the page teach the opposite.
#[test]
fn every_expression_documented_as_rejected_fails_to_compile() {
    let r = reference();
    let topics = r.topics.iter().flat_map(|t| t.rejected.iter());
    for rejected in r.groups.iter().flat_map(|g| g.rejected.iter()).chain(topics) {
        assert!(
            !compiles(&rejected.expr),
            "`{}` is documented as a compile error but compiles",
            rejected.expr
        );
        assert!(!rejected.why.trim().is_empty(), "`{}` has no reason", rejected.expr);
    }
}

/// The prose fields the page renders. Empty text would publish a blank cell.
/// The page draws a label once, above the run it heads, so a label's items must
/// be contiguous: one that came back after another would head two runs and read
/// as two subjects.
fn labels_run_once<'a>(what: &str, labels: impl Iterator<Item = Option<&'a str>>) {
    let mut seen: Vec<&str> = Vec::new();
    let mut previous: Option<&str> = None;
    for label in labels {
        assert!(label.is_none_or(|l| !l.trim().is_empty()), "{what} has an empty group");
        if label != previous {
            if let Some(l) = label {
                assert!(!seen.contains(&l), "{what}'s group {l:?} is split in two");
                seen.push(l);
            }
            previous = label;
        }
    }
}

#[test]
fn every_entry_carries_the_prose_the_page_renders() {
    let r = reference();
    assert!(!r.groups.is_empty(), "no groups");
    for g in &r.groups {
        assert!(
            !g.id.trim().is_empty() && !g.title.trim().is_empty(),
            "group {:?}",
            g.title
        );
        assert!(!g.blurb.trim().is_empty(), "group {:?} has no blurb", g.title);
        assert!(
            !g.rows.is_empty() || !g.functions.is_empty(),
            "group {:?} documents nothing",
            g.title
        );
        assert!(
            g.notes.iter().all(|n| !n.trim().is_empty()),
            "group {:?} has an empty note",
            g.title
        );
        // Notes and rejections raise an operator table; without one they have nothing to raise.
        assert!(
            g.rows.is_empty() == (g.notes.is_empty() && g.rejected.is_empty()),
            "group {:?} has notes or rejections but no operator table, or the reverse",
            g.title
        );
        labels_run_once(&g.title, g.functions.iter().map(|f| f.group.as_deref()));
    }
    for example in functions(&r)
        .flat_map(|f| f.examples.iter())
        .chain(operator_rows(&r).flat_map(|row| row.examples.iter()))
    {
        assert!(
            example.note.iter().all(|n| !n.trim().is_empty()),
            "`{}` has an empty note",
            example.expr
        );
    }
    for row in specifier_rows(&r) {
        assert!(
            row.specifier.starts_with('%') && row.specifier.len() == 2,
            "{:?} is not a specifier",
            row.specifier
        );
        assert!(!row.matches.trim().is_empty(), "{} has no gloss", row.specifier);
        // One example each: the table is read by someone picking a specifier, and it is evaluated against the
        // engine, so it is the part of the row that cannot go stale.
        assert_eq!(row.examples.len(), 1, "{} needs exactly one example", row.specifier);
    }
    for row in operator_rows(&r) {
        assert!(!row.gloss.trim().is_empty(), "{} has no gloss", row.symbol);
        // One example, or as many as three when each teaches a distinct rule. Capped because the table is meant to be
        // scanned -- but an example is evaluated against the engine where a note is unchecked prose, so a rule worth
        // stating belongs here rather than squeezed into a note to fit.
        assert!(
            (1..=3).contains(&row.examples.len()),
            "{} has {} examples",
            row.symbol,
            row.examples.len()
        );
    }
    for f in functions(&r) {
        assert!(!f.summary.trim().is_empty(), "{} has no summary", f.name);
        // Method form, `value.f(args)`, except the one function with no receiver to hang it on.
        let call = format!("{}(", f.name);
        assert!(
            f.signature.contains(&format!(".{call}")) || f.signature.starts_with(&call),
            "{}'s signature does not name it: {:?}",
            f.name,
            f.signature
        );
        assert!(!f.examples.is_empty(), "{} has no examples", f.name);
        // The blurb introduces the specifier table; neither is any use alone.
        assert!(
            f.specifiers.is_empty() == f.specifier_blurb.is_none(),
            "{} has a specifier table without its blurb, or the reverse",
            f.name
        );
        assert!(
            f.specifier_blurb.iter().all(|b| !b.trim().is_empty()),
            "{} has an empty specifier blurb",
            f.name
        );
        assert!(
            f.notes.iter().all(|n| !n.trim().is_empty()),
            "{} has an empty note",
            f.name
        );
        labels_run_once(&f.name, f.examples.iter().map(|e| e.group.as_deref()));
        labels_run_once(&f.name, f.specifiers.iter().map(|r| r.group.as_deref()));
    }
}

/// Code steps' examples (`website/src/lib/code-steps-examples.json`): each block's code, run after a JSON parser
/// over its input, gives the output the page shows; each error's code fails to compile with an error naming each fix
/// the page shows.
#[derive(serde::Deserialize)]
struct Examples {
    /// Drawn with the input record beside the output.
    blocks: std::collections::BTreeMap<String, Block>,
    /// Lists of code that does not compile, by page section.
    errors: std::collections::BTreeMap<String, Vec<Refused>>,
}

#[derive(serde::Deserialize)]
struct Refused {
    code: String,
    #[serde(default)]
    declared: std::collections::BTreeMap<String, String>,
    /// Text the compile error must hold, each one a way to fix the code.
    fixes: Vec<String>,
}

#[derive(serde::Deserialize)]
struct Block {
    code: String,
    /// The JSON parser's declared fields, name to type, as the text above the block tells the reader to declare.
    #[serde(default)]
    declared: std::collections::BTreeMap<String, String>,
    /// One record, or an array of records when the code changes how many there are.
    input: serde_json::Value,
    /// The record, or the array of records, the code gives.
    output: serde_json::Value,
    /// Inline lookup tables the code's `lookup` calls name, as a group configuration holds them.
    #[serde(default, rename = "lookupTables")]
    lookup_tables: Vec<serde_json::Value>,
}

/// The records `code` gives, run by a Code step after a JSON parser declaring `declared` (name to type, as the page
/// spells it), over `inputs`, each less `_raw`; or the compile errors.
fn run_code(
    inputs: &[String],
    declared: &std::collections::BTreeMap<String, String>,
    code: &str,
    lookup_tables: &[serde_json::Value],
) -> Result<Vec<serde_json::Value>, String> {
    let parse: String = declared.iter().map(|(field, ty)| format!("{field}: {ty}; ")).collect();
    let parse = parse + "Object.assign($, json(_raw));";
    let steps = serde_json::json!([
        { "id": "parse", "type": "code", "body": parse },
        { "id": "code", "type": "code", "body": code },
    ]);
    let example = serde_json::json!({ "id": "example", "name": "example", "steps": steps });
    let file = serde_json::json!({ "version": 1, "pipelines": [example], "lookupTables": lookup_tables });
    let system = smeltr::config_file::load_str(&file.to_string()).map_err(|e| e.to_string())?;
    let plan = compile(&system.pipelines[0], &SourceMeta::KAFKA)
        .map_err(|errs| errs.iter().map(|e| e.to_string()).collect::<Vec<_>>().join("; "))?;
    let texts: Vec<&str> = inputs.iter().map(String::as_str).collect();
    let mut pool = BufferPool::new();
    let mut errors = ErrorSink::new();
    let mut batch = harness::raw_batch(&plan, &texts, &mut pool);
    let out: Vec<serde_json::Value> = {
        let mut ctx = Ctx::new(&mut pool, &mut errors);
        (run_to_json(&plan, &mut batch, &mut ctx).iter())
            .map(|row| {
                let mut out: serde_json::Value = serde_json::from_slice(row).unwrap();
                out.as_object_mut().unwrap().remove("_raw");
                out
            })
            .collect()
    };
    batch.reclaim(&mut pool);
    Ok(out)
}

#[test]
fn every_code_steps_example_gives_what_the_page_shows() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("website/src/lib/code-steps-examples.json");
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    let examples: Examples = serde_json::from_str(&text).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    for refused in examples.errors.values().flatten() {
        let code = &refused.code;
        let got = run_code(&["{}".to_string()], &refused.declared, code, &[]).expect_err(&format!("`{code}` compiles"));
        for fix in &refused.fixes {
            assert!(
                got.contains(fix.as_str()),
                "`{code}`: the error does not name `{fix}`: {got}"
            );
        }
    }
    for (name, block) in &examples.blocks {
        let inputs: Vec<String> = match &block.input {
            serde_json::Value::Array(records) => records.iter().map(|r| r.to_string()).collect(),
            record => vec![record.to_string()],
        };
        let out = run_code(&inputs, &block.declared, &block.code, &block.lookup_tables)
            .unwrap_or_else(|e| panic!("block `{name}` does not compile: {e}"));
        let out = match &block.input {
            serde_json::Value::Array(_) => serde_json::Value::Array(out),
            _ => out.into_iter().next().expect("one record"),
        };
        // Rendered JSON, so a float documented as an integer fails; keys compare in any order.
        assert_eq!(
            serde_json::to_string(&out).unwrap(),
            serde_json::to_string(&block.output).unwrap(),
            "block `{name}`"
        );
    }
}

/// The Code step page's tables of statements and parse functions (`website/src/lib/code-language.json`).
#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct CodeLanguage {
    statements: Vec<Statement>,
    parse_functions: Vec<ParseFunction>,
}

#[derive(serde::Deserialize)]
struct Statement {
    form: String,
    rule: String,
}

#[derive(serde::Deserialize)]
struct ParseFunction {
    name: String,
    signature: String,
    summary: String,
    options: Vec<ParseOption>,
}

#[derive(serde::Deserialize)]
struct ParseOption {
    name: String,
    holds: String,
}

fn code_language() -> CodeLanguage {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("website/src/lib/code-language.json");
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("{} is not the expected shape: {e}", path.display()))
}

/// Every word that begins or shapes a statement has a row: the reserved words but the values and `as` (the
/// expression reference's), the statements spelled as calls, and the words the parser reads only where a statement
/// starts.
#[test]
fn the_statement_table_names_every_statement() {
    use smeltr::expr::lex::{KEYWORDS, Keyword};
    let table = code_language();
    let reserved = (KEYWORDS.iter())
        .filter(|(_, k)| !matches!(k, Keyword::True | Keyword::False | Keyword::Null | Keyword::As))
        .map(|(word, _)| *word);
    let contextual = ["switch", "case", "default", "try", "catch", "finally", "throw"];
    for word in reserved.chain(smeltr::expr::parse::STATEMENT_CALLS).chain(contextual) {
        let named = table
            .statements
            .iter()
            .any(|s| s.form.split(|c: char| !c.is_ascii_alphanumeric()).any(|w| w == word));
        assert!(named, "no statement row names `{word}`");
    }
    for s in &table.statements {
        assert!(!s.rule.trim().is_empty(), "`{}` has no rule", s.form);
    }
}

/// The parse functions and their options are the engine's.
#[test]
fn the_parse_function_table_matches_the_engine() {
    use smeltr::plan::parse_call::OPTIONS;
    let table = code_language();
    let have: Vec<(&str, Vec<&str>)> = (table.parse_functions.iter())
        .map(|f| (f.name.as_str(), f.options.iter().map(|o| o.name.as_str()).collect()))
        .collect();
    let want: Vec<(&str, Vec<&str>)> = OPTIONS.iter().map(|(name, known)| (*name, known.to_vec())).collect();
    assert_eq!(
        have, want,
        "website/src/lib/code-language.json's parse functions differ from parse_call::OPTIONS"
    );
    for f in &table.parse_functions {
        assert!(
            f.signature.starts_with(&format!("{}(", f.name)),
            "{}'s signature",
            f.name
        );
        assert!(!f.summary.trim().is_empty(), "{} has no summary", f.name);
        assert!(
            f.options.iter().all(|o| !o.holds.trim().is_empty()),
            "{} has an empty option",
            f.name
        );
    }
}

/// Step reference examples (`website/src/lib/steps/*.json`): each example's steps, loaded as a saved pipeline and
/// run over its input, give the records the page shows. A record that is a string is the raw text; any other value
/// is sent as its JSON. Steps that do not open with a parser are preceded by a JSON parser of `_raw` that declares
/// nothing. `_raw` is left out of the output. An example's records must not fail.
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct StepExample {
    steps: Vec<serde_json::Value>,
    /// Other pipelines the steps call.
    #[serde(default)]
    pipelines: Vec<serde_json::Value>,
    #[serde(default, rename = "lookupTables")]
    lookup_tables: Vec<serde_json::Value>,
    input: Vec<serde_json::Value>,
    output: Vec<serde_json::Value>,
    /// The group's `exceptions`. An example that names it shows failed records, tagged or dropped as it says; one
    /// that does not must fail no record.
    #[serde(default, rename = "exceptions")]
    on_exception: Option<String>,
}

fn run_step_example(ex: &StepExample) -> Result<Vec<serde_json::Value>, String> {
    let mut steps = ex.steps.clone();
    if steps.first().is_none_or(|s| s["type"] != "parser") {
        let parse = serde_json::json!({ "id": "parse", "type": "parser", "input": "_raw", "kind": { "type": "json" } });
        steps.insert(0, parse);
    }
    let example = serde_json::json!({ "id": "example", "name": "example", "steps": steps });
    let mut pipelines = vec![example];
    pipelines.extend(ex.pipelines.iter().cloned());
    let mut file = serde_json::json!({ "version": 1, "pipelines": pipelines, "lookupTables": ex.lookup_tables });
    if let Some(answer) = &ex.on_exception {
        // Delivery stays at its default: a row in the preview is neither unmatched, unprocessable nor refused.
        let categories: serde_json::Map<_, _> = ["parse", "math", "limits", "encoding"]
            .into_iter()
            .map(|c| (c.to_string(), serde_json::json!(answer)))
            .collect();
        file["exceptions"] = serde_json::json!({ "categories": categories });
    }
    let system = smeltr::config_file::load_str(&file.to_string()).map_err(|e| e.to_string())?;
    let plan = compile(&system.pipelines[0], &SourceMeta::KAFKA)
        .map_err(|errs| errs.iter().map(|e| e.to_string()).collect::<Vec<_>>().join("; "))?;
    let texts: Vec<String> = ex
        .input
        .iter()
        .map(|v| v.as_str().map_or_else(|| v.to_string(), str::to_string))
        .collect();
    let texts: Vec<&str> = texts.iter().map(String::as_str).collect();
    let mut pool = BufferPool::new();
    let mut errors = ErrorSink::new();
    let mut batch = harness::raw_batch(&plan, &texts, &mut pool);
    let rows = {
        let mut ctx = Ctx::new(&mut pool, &mut errors);
        // The GUI preview's run: a failed record tagged or dropped as the group's answers say.
        run_to_json_marked(&plan, &system.topology.on_exception, &mut batch, &mut ctx, false, None)
            .iter()
            .filter_map(|row| row.json.as_deref())
            .map(|row| {
                let mut out: serde_json::Value = serde_json::from_slice(row).unwrap();
                out.as_object_mut().unwrap().remove("_raw");
                out
            })
            .collect()
    };
    batch.reclaim(&mut pool);
    match errors.entries() {
        [] => Ok(rows),
        _ if ex.on_exception.is_some() => Ok(rows),
        failed => Err(format!("records failed: {failed:?}")),
    }
}

#[test]
fn every_step_example_gives_what_the_page_shows() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("website/src/lib/steps");
    let mut files: Vec<_> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("read {}: {e}", dir.display()))
        .map(|f| f.unwrap().path())
        .collect();
    files.sort();
    for path in files {
        let text = std::fs::read_to_string(&path).unwrap();
        let examples: std::collections::BTreeMap<String, StepExample> =
            serde_json::from_str(&text).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        for (name, ex) in &examples {
            let out = run_step_example(ex).unwrap_or_else(|e| panic!("{} `{name}`: {e}", path.display()));
            // Rendered JSON, so a float documented as an integer fails; keys compare in any order.
            assert_eq!(
                serde_json::to_string(&out).unwrap(),
                serde_json::to_string(&ex.output).unwrap(),
                "{} `{name}`",
                path.display()
            );
        }
    }
}

/// One row of the Parser page's built-in Grok patterns table.
#[derive(serde::Deserialize)]
struct GrokPattern {
    name: String,
    matches: String,
    example: String,
}

/// The Parser page lists every built-in Grok pattern, in the engine's order, and each row's example is matched whole
/// by its pattern.
#[test]
fn the_grok_pattern_table_matches_the_engine() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("website/src/lib/grok-patterns.json");
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    let rows: Vec<GrokPattern> = serde_json::from_str(&text).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    let have: Vec<&str> = rows.iter().map(|r| r.name.as_str()).collect();
    let want: Vec<&str> = smeltr::format::grok_pattern::BUILTINS.iter().map(|(n, _)| *n).collect();
    assert_eq!(have, want, "{} differs from grok_pattern::BUILTINS", path.display());
    for row in &rows {
        assert!(!row.matches.trim().is_empty(), "{} has no gloss", row.name);
        let given = [format!("t = {}", serde_json::to_string(&row.example).unwrap())];
        // A whole-line pattern also writes the fields it names; `v` is the whole match either way.
        let (got, _) = evaluate(&format!("grok(t, [\"^%{{{}:v}}$\"])", row.name), &given);
        assert_eq!(
            got.as_ref().and_then(|g| g.get("v")),
            Some(&serde_json::json!(row.example)),
            "%{{{}}} does not match {:?} whole",
            row.name,
            row.example
        );
    }
}

/// One row of the Sources page's metrics table.
#[derive(serde::Deserialize)]
struct MetricRow {
    group: String,
    name: String,
    kind: String,
    labels: Vec<String>,
    what: String,
}

/// `smeltr_` names in `src/` that a metrics source never emits.
const NOT_METRICS: [&str; 5] = [
    // The session cookie.
    "smeltr_session",
    // The cookie binding an OIDC sign-in to the browser that started it.
    "smeltr_oidc",
    // Facts the heartbeat carries as text; they never enter the registry.
    "smeltr_source_blocked_reason",
    "smeltr_source_connection_error",
    "smeltr_destination_last_refusal",
];

/// The Sources page lists every metric the engine writes, under the kind it writes it as. There is no list of metrics
/// in the engine, so this reads `src/`: every `"smeltr_…"` literal outside a test module is a metric or on
/// `NOT_METRICS`, and each write form says whether its name is a counter or a gauge.
#[test]
fn the_metrics_table_matches_the_engine() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let path = root.join("website/src/lib/metrics.json");
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    let rows: Vec<MetricRow> = serde_json::from_str(&text).unwrap_or_else(|e| panic!("{}: {e}", path.display()));

    let literal = regex::Regex::new(r#""(smeltr_[a-z0-9_]*)""#).unwrap();
    let writes = [
        (r#"\.incr\(\s*"(smeltr_[a-z0-9_]+)""#, "counter"),
        (r#"\.incr_seconds\(\s*"(smeltr_[a-z0-9_]+)""#, "counter"),
        (r#"\.count_to\(\s*"(smeltr_[a-z0-9_]+)""#, "counter"),
        (r#"\.gauge\(\s*"(smeltr_[a-z0-9_]+)""#, "gauge"),
        (r#"incr_per_function\([^;]*?"(smeltr_[a-z0-9_]+)""#, "counter"),
        (r#"registry_gauge\(\s*&?\w+,\s*"(smeltr_[a-z0-9_]+)""#, "gauge"),
        // A name chosen from a list of pairs, then passed to `incr`.
        (r#"\(\s*"(smeltr_[a-z0-9_]+)",\s*o\.\w+\)"#, "counter"),
    ]
    .map(|(re, kind)| (regex::Regex::new(re).unwrap(), kind));

    let mut named = std::collections::BTreeSet::new();
    let mut kinds = std::collections::BTreeMap::<String, &str>::new();
    let mut dirs = vec![root.join("src")];
    while let Some(dir) = dirs.pop() {
        for entry in std::fs::read_dir(&dir).unwrap() {
            let p = entry.unwrap().path();
            if p.is_dir() {
                dirs.push(p);
                continue;
            }
            if p.extension().is_none_or(|e| e != "rs") {
                continue;
            }
            let file = std::fs::read_to_string(&p).unwrap();
            // Test modules sit at the end of each file.
            let code = file.split("#[cfg(test)]").next().unwrap();
            named.extend(literal.captures_iter(code).map(|c| c[1].to_string()));
            for (re, kind) in &writes {
                for c in re.captures_iter(code) {
                    let old = kinds.insert(c[1].to_string(), kind);
                    assert!(
                        old.is_none_or(|k| k == *kind),
                        "{} is written as a counter and a gauge",
                        &c[1]
                    );
                }
            }
        }
    }
    let want: std::collections::BTreeSet<&str> = named
        .iter()
        .map(String::as_str)
        .filter(|n| !NOT_METRICS.contains(n))
        .collect();
    let have: std::collections::BTreeSet<&str> = rows.iter().map(|r| r.name.as_str()).collect();
    assert_eq!(have, want, "{} differs from the metrics src/ writes", path.display());

    let mut seen = std::collections::BTreeSet::new();
    for row in &rows {
        assert_eq!(
            kinds.get(&row.name).copied(),
            Some(row.kind.as_str()),
            "{}'s kind",
            row.name
        );
        assert!(
            seen.insert((&row.name, &row.labels)),
            "{} {:?} is listed twice",
            row.name,
            row.labels
        );
        assert!(
            !row.group.is_empty() && !row.what.trim().is_empty(),
            "{} has no group or text",
            row.name
        );
    }
    let groups: Vec<&str> = rows.iter().map(|r| r.group.as_str()).collect();
    let mut runs = groups.clone();
    runs.dedup();
    let distinct: std::collections::BTreeSet<&str> = groups.into_iter().collect();
    assert_eq!(runs.len(), distinct.len(), "each group's rows must be contiguous");
}

/// One row of a connector's settings table. Exactly one of `required`, `default` and `absent` says what leaving it
/// out means.
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct SettingRow {
    name: String,
    #[serde(rename = "type")]
    _ty: String,
    #[serde(default)]
    required: bool,
    default: Option<serde_json::Value>,
    absent: Option<String>,
    /// Inputs the setting takes. The first stands in for the setting when the others are checked.
    examples: Vec<serde_json::Value>,
    what: String,
    #[serde(default)]
    values: Vec<SettingValue>,
}

/// One value a setting takes, and what it does.
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct SettingValue {
    value: serde_json::Value,
    what: String,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ConnectorKind {
    #[serde(rename = "type")]
    ty: String,
    name: String,
    summary: String,
    #[serde(default)]
    shared: bool,
    settings: Vec<SettingRow>,
    #[serde(default)]
    fields: Vec<String>,
    #[serde(default)]
    reads: Vec<String>,
    /// Its settings, `shared` ones included, in its GUI form's order: the form's own, then each Advanced section.
    #[serde(default)]
    form: Vec<FormGroup>,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct FormGroup {
    #[serde(default)]
    section: Option<String>,
    settings: Vec<String>,
}

/// Each kind's form lists each of its settings but the common ones once, and only the first group is outside an
/// Advanced section.
fn check_forms(what: &str, group: &ConnectorGroup) {
    for kind in &group.kinds {
        let mut listed: Vec<&str> = kind
            .form
            .iter()
            .flat_map(|g| g.settings.iter().map(String::as_str))
            .collect();
        let mut have: Vec<&str> = (kind.settings.iter())
            .chain(group.shared.iter().filter(|_| kind.shared))
            .map(|s| s.name.as_str())
            .collect();
        listed.sort();
        have.sort();
        assert_eq!(listed, have, "{what} {}: its form lists each setting once", kind.ty);
        for (i, g) in kind.form.iter().enumerate() {
            assert_eq!(g.section.is_some(), i > 0, "{what} {}: form group {i}", kind.ty);
        }
    }
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ConnectorGroup {
    common: Vec<SettingRow>,
    #[serde(default)]
    shared: Vec<SettingRow>,
    kinds: Vec<ConnectorKind>,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ConnectorReference {
    sources: ConnectorGroup,
    destinations: ConnectorGroup,
}

/// Each variant of `enum_name` in `src/config_file/dto.rs`, by its `type` tag, to the keys it takes, a flattened
/// struct's keys included. Read from the source so that a setting added to the engine is a setting missing here.
fn dto_keys(enum_name: &str) -> std::collections::BTreeMap<String, std::collections::BTreeSet<String>> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/config_file/dto.rs");
    let text = std::fs::read_to_string(&path).unwrap();
    let body = |head: &str| -> Vec<&str> {
        let start = text.find(head).unwrap_or_else(|| panic!("no `{head}` in dto.rs"));
        text[start..].lines().skip(1).take_while(|l| *l != "}").collect()
    };
    let camel = |snake: &str| -> String {
        let mut out = String::new();
        let mut up = false;
        for c in snake.chars() {
            match c {
                '_' => up = true,
                c if up => {
                    out.extend(c.to_uppercase());
                    up = false;
                }
                c => out.push(c),
            }
        }
        out
    };
    let rename = regex::Regex::new(r#"rename = "([^"]+)""#).unwrap();
    let field = regex::Regex::new(r"^(\s+)(?:pub )?(?:r#)?(\w+): ([\w<>:]+),").unwrap();
    // The keys of fields indented `indent`, with each flattened field's struct read in its place.
    fn keys(
        lines: &[&str],
        indent: usize,
        field: &regex::Regex,
        rename: &regex::Regex,
        camel: &dyn Fn(&str) -> String,
        body: &dyn Fn(&str) -> Vec<String>,
    ) -> std::collections::BTreeSet<String> {
        let mut out = std::collections::BTreeSet::new();
        let mut attrs = String::new();
        for line in lines {
            if line.trim_start().starts_with("#[") {
                attrs.push_str(line);
                continue;
            }
            if let Some(c) = field.captures(line).filter(|c| c[1].len() == indent) {
                if attrs.contains("flatten") {
                    let lines = body(&format!("pub struct {} {{", &c[3]));
                    let lines: Vec<&str> = lines.iter().map(String::as_str).collect();
                    out.extend(keys(&lines, 4, field, rename, camel, body));
                } else if let Some(r) = rename.captures(&attrs) {
                    out.insert(r[1].to_string());
                } else {
                    out.insert(camel(&c[2]));
                }
            }
            if !line.trim_start().starts_with("///") {
                attrs.clear();
            }
        }
        out
    }
    let owned = |head: &str| body(head).into_iter().map(str::to_string).collect::<Vec<_>>();
    let lines = body(&format!("pub enum {enum_name} {{"));
    let variant = regex::Regex::new(r"^    (\w+) \{(.*)$").unwrap();
    // `Postgres(SqlSourceDto),`: the struct's own fields, under the variant's tag.
    let newtype = regex::Regex::new(r"^    (\w+)\((\w+)\),$").unwrap();
    let mut out = std::collections::BTreeMap::new();
    let mut i = 0;
    while i < lines.len() {
        if let Some(c) = newtype.captures(lines[i]) {
            let attrs = lines[..i]
                .iter()
                .rev()
                .take_while(|l| l.trim_start().starts_with("#["))
                .copied();
            let tag = (attrs.filter_map(|l| rename.captures(l)).next())
                .map(|r| r[1].to_string())
                .unwrap_or_else(|| c[1][..1].to_lowercase() + &c[1][1..]);
            let lines = owned(&format!("pub struct {} {{", &c[2]));
            let lines: Vec<&str> = lines.iter().map(String::as_str).collect();
            out.insert(tag, keys(&lines, 4, &field, &rename, &camel, &owned));
            i += 1;
            continue;
        }
        let Some(c) = variant.captures(lines[i]) else {
            i += 1;
            continue;
        };
        let name = &c[1];
        let tag = name[..1].to_lowercase() + &name[1..];
        let inline = c[2].trim().trim_end_matches(',').trim_end_matches('}').trim();
        let fields = if inline.is_empty() {
            let end = (i..lines.len()).find(|&j| lines[j].starts_with("    },")).unwrap();
            let fields = keys(&lines[i + 1..end], 8, &field, &rename, &camel, &owned);
            i = end;
            fields
        } else {
            // `Metrics { bucket_seconds: u64 },`
            (inline.split(',').filter(|f| !f.trim().is_empty()))
                .map(|f| camel(f.split(':').next().unwrap().trim()))
                .collect()
        };
        out.insert(tag, fields);
        i += 1;
    }
    out
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct StepReference {
    steps: ConnectorGroup,
    #[serde(rename = "parserFormats")]
    parser_formats: ConnectorGroup,
}

#[test]
fn the_step_reference_matches_the_engine() {
    use smeltr::config_file::{ParserKindDto, StepDto};
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("website/src/lib/step-reference.json");
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    let r: StepReference =
        serde_json::from_str(&text).unwrap_or_else(|e| panic!("{} is not the expected shape: {e}", path.display()));
    // The pipeline a step's examples compile in: after a JSON parse that gives the fields they read a type, beside the
    // lookup table and the pipeline they name.
    let parse = serde_json::json!({
        "id": "parse",
        "type": "parser",
        "input": "_raw",
        "kind": { "type": "json" },
        "declared": [
            { "name": "status", "type": "i64" },
            { "name": "payload", "type": "str" },
            { "name": "user", "type": "str" },
            { "name": "items", "type": { "type": "array", "of": "variant" } },
            { "name": "order.lines", "type": { "type": "array", "of": "variant" } },
            { "name": "level", "type": "str" },
            { "name": "path", "type": "str" },
            { "name": "user_agent", "type": "str" },
            { "name": "action", "type": "str" },
            { "name": "source.ip", "type": "str" },
        ],
    });
    let refusals = |steps: Vec<serde_json::Value>| -> Option<Vec<String>> {
        let file = serde_json::json!({
            "version": 1,
            "pipelines": [
                { "id": "example", "name": "example", "steps": steps },
                { "id": "normalize", "name": "normalize", "steps": [] },
            ],
            "lookupTables": [{
                "id": "users",
                "type": "inline",
                "headers": ["user", "team", "risk_score"],
                "rows": [["alice", "payments", "3"]],
            }],
        });
        let file = serde_json::from_value(file).ok()?;
        Some(
            (smeltr::config_file::validation_errors(file).iter())
                .map(|e| e.to_string())
                .collect(),
        )
    };
    let step = |entry: &serde_json::Map<String, serde_json::Value>| {
        // A disabled step is not compiled, so its examples are checked enabled.
        let mut entry = entry.clone();
        entry.remove("disabled");
        let entry = serde_json::Value::Object(entry);
        refusals(vec![parse.clone(), entry])
    };
    let format = |entry: &serde_json::Map<String, serde_json::Value>| {
        let parser = serde_json::json!({ "id": "parse", "type": "parser", "input": "_raw", "kind": entry });
        refusals(vec![parser])
    };
    // Each example is checked against the refusals of the first ones, so a kind's own first ones must compile clean.
    // The shared ones are left out: Unroll refuses a filter.
    for kind in &r.steps.kinds {
        let mut first = serde_json::Map::new();
        first.insert("type".into(), kind.ty.clone().into());
        for row in r.steps.common.iter().chain(&kind.settings) {
            first.insert(row.name.clone(), row.examples[0].clone());
        }
        assert_eq!(
            step(&first),
            Some(vec![]),
            "step {}: its first examples are refused",
            kind.ty
        );
    }
    check_connectors::<StepDto>("step", &r.steps, dto_keys("StepDto"), &["id"], Some(&step));
    check_connectors::<ParserKindDto>(
        "parser format",
        &r.parser_formats,
        dto_keys("ParserKindDto"),
        &[],
        Some(&format),
    );
}

/// The variants serde names when it refuses an unknown one, or `None` for any other refusal.
fn variants(error: &str) -> Option<std::collections::BTreeSet<String>> {
    let (_, expected) = error.split_once("unknown variant `notAValue`, expected ")?;
    let names = regex::Regex::new(r"`([^`]+)`").unwrap();
    Some(
        names
            .captures_iter(expected)
            .map(|c| c[1].to_string())
            .filter(|v| !OLD_NAMES.contains(&v.as_str()))
            .collect(),
    )
}

/// Values the engine still reads under a name it no longer writes, which the page does not list: `sourceBytes` is
/// `bytes`.
const OLD_NAMES: &[&str] = &["sourceBytes"];

/// What the leader refuses of a bundle holding `entry` alone under `section`, or `None` when the bundle does not
/// read at all.
fn refusals(section: &str, entry: &serde_json::Map<String, serde_json::Value>) -> Option<Vec<String>> {
    let file = serde_json::from_value(serde_json::json!({ "version": 1, section: [entry] })).ok()?;
    Some(
        (smeltr::config_file::validation_errors(file).iter())
            .map(|e| e.to_string())
            .collect(),
    )
}

fn connector_reference() -> ConnectorReference {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("website/src/lib/connector-reference.json");
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("{} is not the expected shape: {e}", path.display()))
}

/// The leader's refusals of a connector entry.
type Refusals<'a> = &'a dyn Fn(&serde_json::Map<String, serde_json::Value>) -> Option<Vec<String>>;

/// Every setting of every connector, checked against the bundle's own reading of it: the page names each key the
/// engine takes and no other, a required one cannot be left out, an optional one can, a stated default is what
/// the engine takes when the setting is left out, and a setting with a closed set of values lists each one the
/// leader accepts. `validate` gives the leader's refusals of an entry, where it has more than the bundle's reading.
/// A listed value is checked to read, not to pass validation: some values pass only beside others, such as an HTTP
/// destination's `format` `bytes` beside `body` `single`.
fn check_connectors<T: serde::de::DeserializeOwned + serde::Serialize>(
    what: &str,
    group: &ConnectorGroup,
    engine: std::collections::BTreeMap<String, std::collections::BTreeSet<String>>,
    engine_common: &[&str],
    validate: Option<Refusals>,
) {
    let documented: Vec<&str> = group.kinds.iter().map(|k| k.ty.as_str()).collect();
    let engine_kinds: Vec<&str> = engine.keys().map(String::as_str).collect();
    let mut sorted = documented.clone();
    sorted.sort();
    assert_eq!(sorted, engine_kinds, "the {what} documented differ from the engine's");
    for kind in &group.kinds {
        let rows: Vec<&SettingRow> = (group.common.iter())
            .chain(&kind.settings)
            .chain(group.shared.iter().filter(|_| kind.shared))
            .collect();
        let names: std::collections::BTreeSet<String> = rows.iter().map(|r| r.name.clone()).collect();
        assert_eq!(names.len(), rows.len(), "{what} {}: a setting is listed twice", kind.ty);
        let want: std::collections::BTreeSet<String> = (engine_common.iter().map(|s| s.to_string()))
            .chain(engine[&kind.ty].iter().cloned())
            .collect();
        assert_eq!(
            names, want,
            "{what} {}: the settings documented differ from the engine's",
            kind.ty
        );
        assert!(!kind.name.is_empty() && !kind.summary.is_empty());

        let mut full = serde_json::Map::new();
        full.insert("type".into(), kind.ty.clone().into());
        for r in &rows {
            assert!(
                !r.examples.is_empty(),
                "{what} {} setting {}: no example",
                kind.ty,
                r.name
            );
            full.insert(r.name.clone(), r.examples[0].clone());
        }
        let load = |entry: &serde_json::Map<String, serde_json::Value>| -> Result<serde_json::Value, String> {
            let dto: T = serde_json::from_value(serde_json::Value::Object(entry.clone())).map_err(|e| e.to_string())?;
            Ok(serde_json::to_value(dto).unwrap())
        };
        let saved = load(&full).unwrap_or_else(|e| panic!("{what} {}: its examples do not load: {e}", kind.ty));
        let known = validate.and_then(|validate| validate(&full)).unwrap_or_default();
        for r in &rows {
            let ctx = format!("{what} {} setting {}", kind.ty, r.name);
            assert_eq!(
                usize::from(r.required) + usize::from(r.default.is_some()) + usize::from(r.absent.is_some()),
                1,
                "{ctx}: exactly one of required, default and absent"
            );
            assert!(!r.what.is_empty(), "{ctx}: no description");
            // Each example reads, and the leader refuses nothing of it that it does not already refuse of the
            // first. A refusal that quotes the example, such as of an identity the bundle does not declare, is
            // compared as if it quoted the first.
            for e in &r.examples {
                let mut with = full.clone();
                with.insert(r.name.clone(), e.clone());
                load(&with).unwrap_or_else(|err| panic!("{ctx}: its example {e} does not load: {err}"));
                if let Some(validate) = validate {
                    let as_first = |err: &String| match (e.as_str(), r.examples[0].as_str()) {
                        (Some(e), Some(first)) => err.replace(&format!("{e:?}"), &format!("{first:?}")),
                        _ => err.clone(),
                    };
                    let refused: Vec<String> = (validate(&with).unwrap_or_default().into_iter())
                        .filter(|err| !known.contains(&as_first(err)))
                        .collect();
                    assert!(refused.is_empty(), "{ctx}: its example {e} is refused: {refused:?}");
                }
            }
            for v in &r.values {
                assert!(!v.what.is_empty(), "{ctx}: value {} has no description", v.value);
                let mut with = full.clone();
                with.insert(r.name.clone(), v.value.clone());
                load(&with).unwrap_or_else(|e| panic!("{ctx}: its value {} does not load: {e}", v.value));
            }
            // A value the engine does not know: refused with the engine's own list when the set is closed.
            let unknown = match &r.examples[0] {
                serde_json::Value::String(_) => Some(serde_json::json!("notAValue")),
                serde_json::Value::Object(o) if o.contains_key("type") => {
                    Some(serde_json::json!({"type": "notAValue"}))
                }
                _ => None,
            };
            if let Some(unknown) = unknown {
                let mut with = full.clone();
                with.insert(r.name.clone(), unknown);
                if let Some(engine) = load(&with).err().and_then(|e| variants(&e)) {
                    let name = |v: &serde_json::Value| -> String {
                        v.get("type").unwrap_or(v).as_str().unwrap_or_default().to_string()
                    };
                    let documented: std::collections::BTreeSet<String> =
                        r.values.iter().map(|v| name(&v.value)).collect();
                    assert!(
                        !documented.is_empty(),
                        "{ctx}: takes a fixed set of values, and lists none"
                    );
                    assert_eq!(documented.len(), r.values.len(), "{ctx}: a value is listed twice");
                    assert!(
                        documented.is_subset(&engine),
                        "{ctx}: documents a value the engine does not know"
                    );
                    // A value left out is one the leader refuses here, past what it refuses of the documented ones.
                    for left_out in engine.difference(&documented) {
                        let refused = validate.is_some_and(|validate| {
                            let mut with = full.clone();
                            let value = match r.examples[0].is_object() {
                                true => serde_json::json!({ "type": left_out }),
                                false => serde_json::json!(left_out),
                            };
                            with.insert(r.name.clone(), value);
                            // A value that does not read, such as a position without its fields, is not one the
                            // leader refuses: it is unchecked, so it must be listed.
                            let Some(shown) = validate(&with) else { return false };
                            r.values.iter().all(|v| {
                                let mut documented = full.clone();
                                documented.insert(r.name.clone(), v.value.clone());
                                let known = validate(&documented).unwrap_or_default();
                                shown.iter().any(|e| !known.contains(e))
                            })
                        });
                        assert!(refused, "{ctx}: the engine takes {left_out}, which is not documented");
                    }
                }
            }
            assert!(
                saved.get(&r.name).is_some(),
                "{ctx}: its example is lost on load, so the name is wrong"
            );
            let mut without = full.clone();
            without.remove(&r.name);
            match (&r.default, r.required) {
                (_, true) => assert!(load(&without).is_err(), "{ctx}: documented required, loads without it"),
                (Some(default), _) => {
                    let mut with = full.clone();
                    with.insert(r.name.clone(), default.clone());
                    let saved = load(&with).unwrap_or_else(|e| panic!("{ctx}: its default does not load: {e}"));
                    assert!(
                        saved.get(&r.name).is_none(),
                        "{ctx}: its documented default is not the engine's"
                    );
                }
                (None, false) => {
                    load(&without).unwrap_or_else(|e| panic!("{ctx}: documented optional, refused without it: {e}"));
                }
            }
        }
    }
}

#[test]
fn the_source_reference_matches_the_engine() {
    use smeltr::config_file::SourceDto;
    let r = connector_reference();
    check_connectors::<SourceDto>(
        "source",
        &r.sources,
        dto_keys("SourceKindDto"),
        &["id"],
        Some(&|entry| refusals("sources", entry)),
    );
    check_forms("source", &r.sources);
    let logs = r.sources.kinds.iter().find(|k| k.ty == "logs").expect("a logs source");
    let min_level = logs
        .settings
        .iter()
        .find(|s| s.name == "minLevel")
        .expect("logs takes minLevel");
    check_levels("source logs setting minLevel", &min_level.values);
    let fields = reference().record_fields;
    for kind in &r.sources.kinds {
        let meta = SourceMeta::of_kind(&kind.ty).unwrap_or_else(|| panic!("no source kind {}", kind.ty));
        let want: Vec<&str> = meta.source_fields().iter().map(|(name, _)| *name).collect();
        assert_eq!(kind.fields, want, "source {}: the fields it gives", kind.ty);
        for f in &kind.fields {
            assert!(
                fields.iter().any(|row| &row.name == f),
                "{f} is not in the record fields table"
            );
        }
    }
}

#[test]
fn the_destination_reference_matches_the_engine() {
    use smeltr::config_file::DestinationDto;
    let r = connector_reference();
    check_connectors::<DestinationDto>(
        "destination",
        &r.destinations,
        dto_keys("DestinationKindDto"),
        &["id", "exceptions"],
        Some(&|entry| refusals("destinations", entry)),
    );
    check_forms("destination", &r.destinations);
    let fields = reference().record_fields;
    let mut read = std::collections::BTreeSet::new();
    for kind in &r.destinations.kinds {
        for f in &kind.reads {
            let row = (fields.iter().find(|row| &row.name == f))
                .unwrap_or_else(|| panic!("{f} is not in the record fields table"));
            assert_eq!(row.set_by, "pipeline", "{f}");
            read.insert(f.as_str());
        }
    }
    for p in Placement::ALL {
        assert!(
            read.contains(p.field()),
            "no destination is documented reading {}",
            p.field()
        );
    }
}

/// Every source and destination example on the Sources and Destinations pages is one the leader accepts on its own.
#[test]
fn every_connector_example_loads() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("website/src/lib/connector-examples.json");
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    let examples: serde_json::Map<String, serde_json::Value> = serde_json::from_str(&text).unwrap();
    for (name, example) in examples {
        let section = if name.ends_with("Source") {
            "sources"
        } else if name.ends_with("Destination") {
            "destinations"
        } else {
            continue;
        };
        // An example naming an identity is shown beside one the page declares, here one naming a certificate from
        // the list.
        let identities: Vec<_> = (example.get("identity").and_then(|i| i.as_str()))
            .map(|id| serde_json::json!({ "id": id, "certificate": { "secret": id, "version": 1 } }))
            .into_iter()
            .collect();
        let file =
            serde_json::from_value(serde_json::json!({ "version": 1, "identities": identities, section: [example] }))
                .unwrap_or_else(|e| panic!("{name} does not load: {e}"));
        let errors = smeltr::config_file::validation_errors(file);
        assert!(errors.is_empty(), "{name} is refused: {errors:?}");
    }
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct StartupSetting {
    key: String,
    env: String,
    default: String,
    /// Inputs the setting takes, as its environment variable spells them. A setting that lists its `values` has none.
    #[serde(default)]
    examples: Vec<serde_json::Value>,
    what: String,
    #[serde(default)]
    values: Vec<SettingValue>,
}

/// A severity setting lists each of tracing's levels once, by labels the engine reads.
fn check_levels(ctx: &str, values: &[SettingValue]) {
    let levels: std::collections::BTreeSet<tracing::Level> = (values.iter())
        .map(|v| {
            let label = v.value.as_str().unwrap_or_default();
            smeltr::logs::level_for_label(label).unwrap_or_else(|| panic!("{ctx}: {label} is not a level"))
        })
        .collect();
    assert_eq!(levels.len(), values.len(), "{ctx}: a level is listed twice");
    assert_eq!(levels.len(), 5, "{ctx}: the levels documented differ from the engine's");
}

/// Every startup example is an environment value the process reads without refusing it.
#[test]
fn every_startup_example_is_read() {
    #[derive(serde::Deserialize)]
    struct Table {
        settings: Vec<StartupSetting>,
    }
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let table: Table =
        serde_json::from_str(&std::fs::read_to_string(root.join("website/src/lib/startup-settings.json")).unwrap())
            .unwrap();
    use smeltr::startup_config as c;
    // A refused value ends the process, naming the variable and the value.
    let readers: std::collections::BTreeMap<&str, fn() -> String> = [
        ("leader.http.port", (|| c::gui_port().to_string()) as fn() -> String),
        ("leader.http.bind", || c::gui_bind()),
        ("leader.http.public_origin", || format!("{:?}", c::public_origin())),
        ("leader.enabled", || c::leader_enabled().to_string()),
        ("leader.storage.data_dir", || c::leader_data_dir().display().to_string()),
        ("leader.secrets.key", || {
            c::leader_secret_key_path().display().to_string()
        }),
        ("leader.secrets.versions", || c::leader_secret_versions().to_string()),
        ("leader.lookups.versions", || c::leader_lookup_versions().to_string()),
        ("leader.tls.enabled", || c::leader_tls_enabled().to_string()),
        ("leader.tls.extra_san", || c::leader_tls_extra_sans().join(",")),
        ("leader.tls.ca_validity", || format!("{:?}", c::leader_ca_validity())),
        ("leader.tls.acme_http_port", || c::acme_http_port().to_string()),
        ("leader.recover_admin", || c::recover_admin().to_string()),
        ("worker.enabled", || c::worker_enabled().to_string()),
        ("worker.group", || c::worker_group()),
        ("worker.zone", || format!("{:?}", c::worker_zone())),
        ("worker.listen_address", || c::worker_listen_address().to_string()),
        ("worker.leader.url", || format!("{:?}", c::leader_url())),
        ("worker.leader.ca", || format!("{:?}", c::leader_ca_path())),
        ("worker.leader.trust_public", || c::leader_trust_public().to_string()),
        ("worker.storage.state_dir", || {
            c::worker_state_dir().display().to_string()
        }),
        ("worker.runtime.thread_limit", || c::worker_thread_limit().to_string()),
        ("worker.runtime.memory", || format!("{:?}", c::worker_memory_bytes())),
        ("worker.heartbeat.interval_ms", || {
            format!("{:?}", c::worker_heartbeat_interval())
        }),
        ("worker.heartbeat.stop_after_unreachable_secs", || {
            format!("{:?}", c::worker_stop_after_unreachable_secs())
        }),
        ("worker.enrollment_code", || {
            format!("{:?}", c::worker_enrollment_code())
        }),
        ("worker.metrics_endpoint.enabled", || {
            c::worker_metrics_endpoint_enabled().to_string()
        }),
        ("worker.metrics_endpoint.port", || {
            c::worker_metrics_endpoint_port().to_string()
        }),
        ("worker.metrics_endpoint.tls_cert", || {
            format!("{:?}", c::worker_metrics_endpoint_tls_cert())
        }),
        ("worker.metrics_endpoint.tls_key", || {
            format!("{:?}", c::worker_metrics_endpoint_tls_key())
        }),
        ("worker.metrics_endpoint.client_ca", || {
            format!("{:?}", c::worker_metrics_endpoint_client_ca())
        }),
        ("worker.metrics_endpoint.user", || {
            format!("{:?}", c::worker_metrics_endpoint_user())
        }),
        ("worker.metrics_endpoint.password", || {
            format!("{:?}", c::worker_metrics_endpoint_password())
        }),
        ("log_level", || c::log_level().to_string()),
    ]
    .into_iter()
    .collect();
    for s in &table.settings {
        let read = readers
            .get(s.key.as_str())
            .unwrap_or_else(|| panic!("{}: no reader", s.key));
        let unset = read();
        for e in &s.examples {
            let value = e
                .as_str()
                .unwrap_or_else(|| panic!("{}: example {e} is not an environment value", s.key));
            // SAFETY: nextest runs each test in its own process, and no other thread here reads the environment.
            unsafe { std::env::set_var(&s.env, value) };
            let got = read();
            unsafe { std::env::remove_var(&s.env) };
            // An example the reader ignores would read as the default; one that names the default is no example. The
            // thread limit's default is this machine's core count, which an example may name.
            if s.key != "worker.runtime.thread_limit" {
                assert_ne!(got, unset, "{}: example {value} reads as unset", s.key);
            }
        }
    }
}

/// The example startup file the reference shows starts a leader. Its paths, port and bind are overridden by the
/// environment, which wins over the file, so the leader starts in a scratch directory; the file is still read whole,
/// and an unknown key or a value of the wrong type refuses to start.
#[test]
fn the_startup_file_example_starts_a_leader() {
    use std::io::BufRead;
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let dir = std::env::temp_dir().join(format!("smeltr-startup-example-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let mut leader = std::process::Command::new(env!("CARGO_BIN_EXE_smeltr"))
        .env("SMELTR_CONFIG_FILE", root.join("website/src/lib/startup-example.toml"))
        .env("SMELTR_LEADER_DATA_DIR", dir.join("data"))
        .env("SMELTR_LEADER_SECRET_KEY", dir.join("sealing.key"))
        .env("SMELTR_GUI_PORT", port.to_string())
        .env("SMELTR_GUI_BIND", "127.0.0.1")
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let mut said = String::new();
    let started = std::io::BufReader::new(leader.stderr.take().unwrap())
        .lines()
        .map_while(Result::ok)
        .any(|line| {
            said.push_str(&line);
            said.push('\n');
            line.contains("leader.origin")
        });
    let _ = leader.kill();
    let _ = leader.wait();
    let _ = std::fs::remove_dir_all(&dir);
    assert!(started, "the example startup file does not start a leader:\n{said}");
}

/// The startup settings table names every file key the binary reads and the environment variable it fills, and each
/// default is what the engine takes when the setting is unset.
#[test]
fn the_startup_settings_match_the_engine() {
    #[derive(serde::Deserialize)]
    struct Table {
        settings: Vec<StartupSetting>,
    }
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let table: Table =
        serde_json::from_str(&std::fs::read_to_string(root.join("website/src/lib/startup-settings.json")).unwrap())
            .unwrap();
    let main = std::fs::read_to_string(root.join("src/bin/smeltr/main.rs")).unwrap();
    let backfill = regex::Regex::new(r#"backfill_env\(\s*"(\w+)",\s*(?:Some\()?file\.([\w.]+)"#).unwrap();
    let engine: std::collections::BTreeSet<(String, String)> = backfill
        .captures_iter(&main)
        // `extra_san.join(",")`: the list is joined into one value.
        .map(|c| (c[2].trim_end_matches(".join").to_string(), c[1].to_string()))
        .collect();
    let documented: std::collections::BTreeSet<(String, String)> =
        table.settings.iter().map(|s| (s.key.clone(), s.env.clone())).collect();
    assert_eq!(
        documented, engine,
        "the startup settings documented differ from the binary's"
    );

    // What the engine takes with its environment unset, as the table spells it: `None` for a setting that is unset
    // or derived, whose default the table words as prose.
    use smeltr::startup_config as c;
    let some = |v: String| Some(v);
    let path = |p: std::path::PathBuf| Some(p.display().to_string());
    let cores = std::thread::available_parallelism().unwrap().get();
    let engine: std::collections::BTreeMap<&str, Option<String>> = [
        ("leader.http.port", some(c::gui_port().to_string())),
        ("leader.http.bind", some(c::gui_bind())),
        ("leader.http.public_origin", c::public_origin()),
        ("leader.enabled", some(c::leader_enabled().to_string())),
        ("leader.storage.data_dir", path(c::leader_data_dir())),
        ("leader.secrets.key", path(c::leader_secret_key_path())),
        ("leader.secrets.versions", some(c::leader_secret_versions().to_string())),
        ("leader.lookups.versions", some(c::leader_lookup_versions().to_string())),
        ("leader.tls.enabled", some(c::leader_tls_enabled().to_string())),
        (
            "leader.tls.extra_san",
            Some(c::leader_tls_extra_sans().join(",")).filter(|s| !s.is_empty()),
        ),
        (
            "leader.tls.ca_validity",
            some(format!("{}d", c::leader_ca_validity().as_secs() / 86_400)),
        ),
        ("leader.tls.acme_http_port", some(c::acme_http_port().to_string())),
        ("leader.recover_admin", some(c::recover_admin().to_string())),
        ("worker.enabled", some(c::worker_enabled().to_string())),
        ("worker.group", some(c::worker_group())),
        ("worker.zone", c::worker_zone()),
        ("worker.listen_address", some(c::worker_listen_address().to_string())),
        ("worker.leader.url", c::leader_url()),
        ("worker.leader.ca", c::leader_ca_path()),
        ("worker.leader.trust_public", some(c::leader_trust_public().to_string())),
        ("worker.storage.state_dir", path(c::worker_state_dir())),
        // The table words it as the available cores.
        (
            "worker.runtime.thread_limit",
            Some(c::worker_thread_limit())
                .filter(|&n| n != cores)
                .map(|n| n.to_string()),
        ),
        ("worker.runtime.memory", c::worker_memory_bytes().map(|n| n.to_string())),
        (
            "worker.heartbeat.interval_ms",
            some(c::worker_heartbeat_interval().as_millis().to_string()),
        ),
        (
            "worker.heartbeat.stop_after_unreachable_secs",
            c::worker_stop_after_unreachable_secs().map(|n| n.to_string()),
        ),
        ("worker.enrollment_code", c::worker_enrollment_code()),
        (
            "worker.metrics_endpoint.enabled",
            some(c::worker_metrics_endpoint_enabled().to_string()),
        ),
        (
            "worker.metrics_endpoint.port",
            some(c::worker_metrics_endpoint_port().to_string()),
        ),
        (
            "worker.metrics_endpoint.tls_cert",
            c::worker_metrics_endpoint_tls_cert().and_then(path),
        ),
        (
            "worker.metrics_endpoint.tls_key",
            c::worker_metrics_endpoint_tls_key().and_then(path),
        ),
        (
            "worker.metrics_endpoint.client_ca",
            c::worker_metrics_endpoint_client_ca().and_then(path),
        ),
        ("worker.metrics_endpoint.user", c::worker_metrics_endpoint_user()),
        (
            "worker.metrics_endpoint.password",
            c::worker_metrics_endpoint_password(),
        ),
        ("log_level", some(c::log_level().to_string().to_lowercase())),
    ]
    .into_iter()
    .collect();
    for s in &table.settings {
        assert!(!s.what.is_empty(), "{}: no description", s.key);
        assert!(
            s.values.is_empty() != s.examples.is_empty(),
            "{}: lists its values or gives examples, not both",
            s.key
        );
        let got = engine
            .get(s.key.as_str())
            .unwrap_or_else(|| panic!("{}: no engine default to check", s.key));
        let literal = s.default.strip_prefix('`').and_then(|d| d.strip_suffix('`'));
        assert_eq!(literal, got.as_deref(), "{}: documented default {}", s.key, s.default);
        if s.key == "log_level" {
            check_levels(&s.key, &s.values);
        }
    }
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkerSizing {
    parts: Vec<SizingPart>,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct SizingPart {
    id: String,
    part: String,
    #[serde(default)]
    kinds: Vec<String>,
    bytes: String,
    each: String,
    what: String,
}

/// The worker sizing table gives each part of a deployment the ceiling `worker::memory` counts for it, and names every
/// connector kind that holds memory of its own, so a kind added to the engine is a kind missing here. A connector's
/// default is read from the connector reference, which is itself checked against the engine.
#[test]
fn the_worker_sizing_table_matches_the_engine() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("website/src/lib/worker-sizing.json");
    let table: WorkerSizing = serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
    let reference = connector_reference();
    let size = |text: &str| smeltr::config_file::bytes::parse(text).unwrap_or_else(|e| panic!("{e}"));
    let setting = |kinds: &[ConnectorKind], shared: &[SettingRow], ty: &str, name: &str| -> usize {
        let kind = kinds
            .iter()
            .find(|k| k.ty == ty)
            .unwrap_or_else(|| panic!("no kind {ty}"));
        let row = kind
            .settings
            .iter()
            .chain(shared.iter().filter(|_| kind.shared))
            .find(|s| s.name == name)
            .unwrap_or_else(|| panic!("{ty} has no {name}"));
        match (&row.default, &row.absent) {
            (Some(serde_json::Value::String(d)), _) => size(d),
            (_, Some(a)) => size(a),
            _ => panic!("{ty}'s {name} states no size"),
        }
    };
    let sources = &reference.sources;
    let destinations = &reference.destinations;
    let object_buffer = setting(&destinations.kinds, &destinations.shared, "s3", "bufferBytes");
    let in_flight = destinations
        .kinds
        .iter()
        .find(|k| k.ty == "s3")
        .and_then(|k| {
            k.settings
                .iter()
                .chain(&destinations.shared)
                .find(|s| s.name == "inFlight")
        })
        .and_then(|s| s.default.as_ref()?.as_u64())
        .expect("s3 states its inFlight") as usize;
    let engine: std::collections::BTreeMap<&str, usize> = [
        ("transform", smeltr::startup_config::memory_budget_bytes()),
        ("lanes", smeltr::worker::TICK_MAX_BYTES),
        ("kafka", setting(&sources.kinds, &sources.shared, "kafka", "fetchBytes")),
        ("receiver", 2 * smeltr::io::receiver::INBOX_BYTES),
        ("objectRecord", smeltr::io::object_source::MAX_RECORD_BYTES as usize),
        ("apiAnswer", smeltr::io::api::MAX_ANSWER_BYTES as usize),
        ("sqlResult", smeltr::io::sql_source::MAX_RESULT_BYTES),
        (
            "buffer",
            setting(&destinations.kinds, &destinations.shared, "kafka", "bufferBytes"),
        ),
        (
            "objectBuffer",
            object_buffer
                + (smeltr::io::files::MAX_OPEN_FILES * smeltr::io::files::open_file_bytes(in_flight))
                    .min(object_buffer),
        ),
    ]
    .into_iter()
    .collect();
    let documented: std::collections::BTreeMap<&str, usize> =
        table.parts.iter().map(|p| (p.id.as_str(), size(&p.bytes))).collect();
    assert_eq!(
        documented, engine,
        "the worker sizing table differs from the engine's ceilings"
    );

    for p in &table.parts {
        assert!(
            !p.part.is_empty() && !p.each.is_empty() && !p.what.is_empty(),
            "{}: a column is empty",
            p.id
        );
    }
    // Every source kind but the worker's own metrics and logs holds memory beyond its lane; every destination holds
    // its buffer.
    let listed = |group: &[ConnectorKind]| -> std::collections::BTreeSet<String> {
        group.iter().map(|k| k.ty.clone()).collect()
    };
    let kinds_of = |ids: &[&str]| -> std::collections::BTreeSet<String> {
        table
            .parts
            .iter()
            .filter(|p| ids.contains(&p.id.as_str()))
            .flat_map(|p| p.kinds.iter().cloned())
            .collect()
    };
    let mut source_kinds = listed(&sources.kinds);
    source_kinds.remove("metrics");
    source_kinds.remove("logs");
    assert_eq!(
        kinds_of(&["kafka", "receiver", "objectRecord", "apiAnswer", "sqlResult"]),
        source_kinds
    );
    assert_eq!(kinds_of(&["buffer", "objectBuffer"]), listed(&destinations.kinds));
}
