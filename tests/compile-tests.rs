//! Compiler tests: config in, `Plan` out.
//!
//! The headline case is `rename_emits_no_instruction` — the design doc's own
//! phase-3 acceptance criterion.

use smeltr::core::column::DataType;
use smeltr::core::id::FunctionId;
use smeltr::plan::config::{DeclaredField, FunctionKind, ParserKind, PipelineConfig, Step};
use smeltr::plan::plan::{Op, Plan, compile};

use crate::harness;
use harness::*;

/// Step 0: `decls`, then a JSON parse of `_raw`.
fn json(decls: &str) -> Step {
    code(0, &format!("{decls} Object.assign($, json(_raw));"))
}

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

/// The errors a config that must not compile produces.
fn err(body: Vec<Step>) -> Vec<smeltr::plan::config::CompileError> {
    compile(&pipeline(body), &smeltr::plan::topology::SourceMeta::KAFKA)
        .err()
        .unwrap_or_else(|| panic!("expected a compile error"))
}

/// Count instructions attributed to a given function.
fn instrs_for(p: &Plan, f: FunctionId) -> usize {
    p.instrs.iter().filter(|i| i.function == f).count()
}

// ---------------------------------------------------------------------------
// The acceptance test
// ---------------------------------------------------------------------------

#[test]
fn rename_emits_no_instruction() {
    // Phase-3 acceptance criterion from the design doc. Rename is pure
    // compile-time symbol-table surgery: no kernel, no data movement, no cost.
    let p = ok(vec![
        code(0, "src = 1;"),
        code(1, "move(src, dst);"),
        code(2, "out = dst + 1;"),
    ]);
    assert_eq!(instrs_for(&p, 1), 0, "Rename must emit nothing");
}

/// After a dynamic parser a rename still emits nothing. A name any step binds claims the record's key of that name, so
/// the serializer skips the keys `dst` and `out`: the rename's new name and the eval's target.
#[test]
fn rename_after_a_dynamic_parser_moves_the_records_keys() {
    let p = ok(vec![
        json("src: i64;"),
        code(1, "move(src, dst);"),
        code(2, "out = dst + 1;"),
    ]);
    assert_eq!(instrs_for(&p, 1), 0, "Rename must emit nothing");
    assert!(p.tree.get("dst").is_some() && p.tree.get("out").is_some());
}

#[test]
fn rename_does_not_allocate_a_slot() {
    let with = ok(vec![code(0, "src = 1;"), code(1, "move(src, dst);")]);
    let without = ok(vec![code(0, "src = 1;")]);
    assert_eq!(
        with.slot_layout.len(),
        without.slot_layout.len(),
        "renaming moves a name, it does not create a column"
    );
}

/// The serializer walks the tree's leaves, so the new name must point at the column the parser wrote under the old one.
#[test]
fn renamed_field_resolves_to_the_original_slot() {
    let p = ok(vec![json("src: i64;"), code(1, "move(src, dst);")]);
    let parsed = ok(vec![json("src: i64;")]);
    let written = parsed.tree.leaf("src").expect("the parser binds `src`");
    let bound = |name: &str| p.tree.leaf(name);
    assert_eq!(bound("dst"), Some(written));
    assert_eq!(bound("src"), None);
    assert_eq!(instrs_for(&p, 1), 0, "Rename must emit nothing");
}

// ---------------------------------------------------------------------------
// SSA
// ---------------------------------------------------------------------------

/// Authors rely on assignment order: `y` keeps the `x` it read, however often `x` is reassigned after. The parser
/// supplies `x`, so the compiler cannot fold it away.
#[test]
fn a_reassignment_never_changes_what_an_earlier_step_read() {
    let plan = ok(vec![
        json("x: i64;"),
        code(1, "y = x;"),
        code(2, "x = x + 1;"),
        code(3, "x = x + 1;"),
    ]);
    let mut env = Env::new();
    let mut b = raw_batch(&plan, &[r#"{"x":1}"#], &mut env.pool);
    let rows = env.run_json(&plan, &mut b);
    assert_eq!(rows[0]["y"], 1, "{}", rows[0]);
    assert_eq!(rows[0]["x"], 3, "{}", rows[0]);
    b.reclaim(&mut env.pool);
}

#[test]
fn comment_emits_nothing() {
    let p = ok(vec![
        json("a: i64;"),
        func(
            1,
            FunctionKind::Comment {
                text: "explanation".into(),
            },
        ),
    ]);
    assert_eq!(instrs_for(&p, 1), 0);
}

#[test]
fn disabled_function_emits_nothing() {
    let Step::Function(mut f) = code(1, "x = 1;") else {
        unreachable!("a function step")
    };
    f.disabled = true;
    let p = ok(vec![json(""), Step::Function(f)]);
    assert_eq!(instrs_for(&p, 1), 0);
}

// ---------------------------------------------------------------------------
// Predication
// ---------------------------------------------------------------------------

#[test]
fn if_else_produces_two_guards() {
    let p = ok(vec![
        json("flag: bool; a: i64;"),
        code(1, "out: i64 = null; if (flag) { out = a + 1; } else { out = a + 2; }"),
    ]);
    let guards = p
        .provenance
        .iter()
        .filter(|o| o.origin == smeltr::plan::resolve::Origin::Guard)
        .count();
    assert_eq!(guards, 2, "then and else each get one");
}

#[test]
fn guarded_assignment_desugars_to_ternary_retain() {
    use smeltr::expr::ir::IrKind;
    let p = ok(vec![
        json("flag: bool; x: i64;"),
        Step::If {
            id: 2,
            cond: "flag".into(),
            then: vec![code(1, "x = 999;")],
            else_: vec![],
        },
    ]);
    let assign = p.instrs.iter().find(|i| i.function == 1).unwrap();
    match &assign.op {
        // `x = flag ? 999 : x` — the blend the ternary already required.
        Op::Eval { ir } => assert!(
            matches!(ir.kind, IrKind::Ternary { .. }),
            "guarded assignment must retain the prior value"
        ),
        other => panic!("got {other:?}"),
    }
}

/// A step's own `filter` is a guard, and a static parser may run under a guard.
#[test]
fn a_parser_carrying_a_filter_compiles() {
    let mut parser = csv(1, &["extracted"], &[("extracted", DataType::Str)]);
    if let Step::Function(f) = &mut parser {
        f.filter = Some("flag".into());
    }
    let plan = compile(
        &pipeline(vec![json("flag: bool; body: str;"), parser]),
        &smeltr::plan::topology::SourceMeta::KAFKA,
    )
    .unwrap();
    assert!(plan.instrs.iter().any(|i| matches!(i.op, Op::Call { .. })));
}

/// A second parser that declares a bound field assigns it by presence over the first one's value (plan rule 3).
#[test]
fn a_second_parser_declaring_a_bound_field_blends_it_by_presence() {
    let p = ok(vec![
        csv(0, &["a", "b"], &[("a", DataType::Str), ("b", DataType::Str)]),
        csv(1, &["a", "c"], &[("a", DataType::Str), ("c", DataType::Str)]),
    ]);
    let a = p.tree.leaf("a");
    let blend = p.instrs.iter().find(|i| Some(i.out) == a).expect("`a` is written");
    assert_eq!(blend.function, 1);
    assert!(matches!(&blend.op, Op::Eval { .. }), "{:?}", blend.op);
}

/// A bare `null` assigned inside a guarded region used to silently retype the field: `resolve_typed` has to
/// concretize an untyped null literal to *some* `DataType` before it reaches here, and it defaults to `I64`,
/// regardless of what the field is everywhere else the guard does not run. A field
/// reassigned to `null` from inside an `if` -- exactly the "clear this out" pattern a guarded region is for -- kept
/// its real type only by accident of whichever slot a later read happened to resolve against. `guarded_value` now
/// recognizes the literal null token and adopts the field's prior type instead of unifying the placeholder.
#[test]
fn a_bare_null_inside_a_guard_keeps_the_fields_prior_type() {
    let p = ok(vec![json("flag: bool; s: str;"), code(1, "if (flag) { s = null; }")]);
    let s = p.provenance.iter().rfind(|o| o.name == "s").unwrap();
    assert_eq!(s.ty, DataType::Str);
}

/// The other side of the same fix: a guarded reassignment to a genuinely incompatible type is a compile error
/// naming the field, not a silent retype discovered later at some unrelated read.
#[test]
fn a_type_mismatch_inside_a_guard_is_a_compile_error() {
    let e = err(vec![
        json("flag: bool; n: i64;"),
        code(1, r#"if (flag) { n = "not a number"; }"#),
    ]);
    assert!(e[0].message.contains("`n`"), "{:?}", e);
}

/// A guarded rename is `move(a, b)` on the rows the guard selects: those rows get `b` from `a` and lose `a`, and the
/// rest keep both as they were.
#[test]
fn rename_inside_a_conditional_moves_only_the_selected_rows() {
    let plan = ok(vec![json("flag: bool; a: i64;"), code(1, "if (flag) { move(a, b); }")]);
    let mut env = Env::new();
    let mut b = raw_batch(
        &plan,
        &[r#"{"flag":true,"a":1}"#, r#"{"flag":false,"a":2,"b":3}"#],
        &mut env.pool,
    );
    let rows = env.run_json(&plan, &mut b);
    assert_eq!(rows[0]["b"], 1, "{}", rows[0]);
    assert!(rows[0].get("a").is_none_or(|a| a.is_null()), "{}", rows[0]);
    assert_eq!(rows[1]["a"], 2, "{}", rows[1]);
    assert_eq!(rows[1]["b"], 3, "{}", rows[1]);
    b.reclaim(&mut env.pool);
}

/// An unroll changes how many rows there are, so it cannot sit under a guard: not in an `if`, not in an `else`, and
/// not behind its own filter.
#[test]
fn unroll_inside_a_conditional_is_a_compile_error() {
    let parser = || json("flag: bool; xs: array<i64>;");
    let filtered = {
        let Step::Function(mut f) = code(1, "unroll(xs);") else {
            unreachable!("a function step")
        };
        f.filter = Some("flag".into());
        Step::Function(f)
    };
    for guarded in [
        code(1, "if (flag) { unroll(xs); }"),
        code(1, "if (flag) { y = 1; } else { unroll(xs); }"),
        filtered,
    ] {
        let e = err(vec![parser(), guarded]);
        assert!(
            e.iter()
                .any(|e| e.function == Some(1) && e.message.contains("Unroll cannot appear inside a conditional")),
            "{e:?}"
        );
    }
    ok(vec![parser(), code(1, "unroll(xs);")]);
}

// ---------------------------------------------------------------------------
// Resolution errors
// ---------------------------------------------------------------------------

/// Where the root is closed, a read of a name nothing bound is a compile error on the step that reads it. A JSON parse
/// opens the root: after one, the same read is a variant read of the root's unclaimed fields, null where the row has
/// no such key.
#[test]
fn an_unknown_field_is_a_compile_error_unless_the_root_is_open() {
    let e = err(vec![code(1, "x = whatever.key;")]);
    assert!(
        e.iter()
            .any(|e| e.function == Some(1) && e.message.contains("`whatever.key` is not a field")),
        "{e:?}"
    );

    let plan = ok(vec![json(""), code(1, "x = whatever.key;")]);
    let x = plan.tree.leaf("x").expect("`x` is a leaf");
    assert_eq!(plan.provenance[x].ty, DataType::Variant);
    let mut env = Env::new();
    let mut b = raw_batch(&plan, &[r#"{"a":1}"#], &mut env.pool);
    let rows = env.run_json(&plan, &mut b);
    assert!(rows[0]["x"].is_null(), "{}", rows[0]);
    b.reclaim(&mut env.pool);
}

#[test]
fn type_error_reports_the_function() {
    let e = err(vec![json("s: str; n: i64;"), code(1, "x = s + n;")]);
    assert_eq!(e[0].function, Some(1));
}

/// A rename is `b = a`, then `a` is gone: it replaces what `b` held.
#[test]
fn renaming_onto_a_bound_name_replaces_it() {
    let p = ok(vec![json("a: i64; b: i64;"), code(1, "move(a, b);")]);
    let leaves = p.tree.leaves();
    let live: Vec<&str> = leaves.iter().map(|(n, _)| n.as_str()).collect();
    assert!(live.contains(&"b") && !live.contains(&"a"), "{live:?}");
    let b = p.tree.leaf("b").unwrap();
    assert_eq!(p.provenance[b].slot, b);
    let parsed = ok(vec![json("a: i64; b: i64;")]);
    let a_parsed = parsed.tree.leaf("a").expect("the parser binds `a`");
    assert_eq!(b, a_parsed, "`b` is `a`'s column");
    assert_eq!(instrs_for(&p, 1), 0, "Rename must emit nothing");
}

/// A name is a leaf or an object, never both: once `source` is a scalar, a write to `source.ip` is a compile error on
/// the step that writes it.
#[test]
fn a_write_under_a_scalar_is_a_compile_error() {
    let e = err(vec![code(0, r#"source = "x"; source.ip = "192.168.1.1";"#)]);
    assert!(
        e.iter()
            .any(|e| e.function == Some(0) && e.message.contains("`source` is str and has no fields")),
        "{e:?}"
    );
}

/// An object keeps its type: a scalar written after its dotted child is a compile error naming the typed assignment.
#[test]
fn a_scalar_written_over_its_dotted_child_is_a_compile_error() {
    let e = err(vec![code(0, r#"source.ip = "192.168.1.1"; source = "x";"#)]);
    assert!(
        e.iter().any(|e| e
            .message
            .contains("is str, not {ip: str}; to change the type of `source`, write `source: str = ")),
        "{e:?}"
    );
}

#[test]
fn sibling_dotted_fields_do_not_warn() {
    // "source.ip" and "source.port" are both nested under "source" but
    // neither is a prefix of the other — this must keep working, silently.
    let p = ok(vec![code(0, r#"source.ip = "1.2.3.4"; source.port = 443;"#)]);
    assert!(p.warnings.is_empty(), "{:?}", p.warnings);
}

/// A rename writes its target, so a rename to a path under a scalar is a compile error on the rename's step.
#[test]
fn a_rename_under_a_scalar_is_a_compile_error() {
    let e = err(vec![code(0, r#"source = "x"; y = 1;"#), code(1, "move(y, source.ip);")]);
    assert!(
        e.iter()
            .any(|e| e.function == Some(1) && e.message.contains("`source` is str and has no fields")),
        "{e:?}"
    );
}

/// The editor puts a compile error on the step its `function` names.
#[test]
fn parse_error_is_attributed() {
    let e = err(vec![json("a: i64;"), code(1, "x = a +;")]);
    assert!(
        e.iter()
            .any(|x| x.message.contains("parse error") && x.function == Some(1)),
        "{e:?}"
    );
}

// ---------------------------------------------------------------------------
// Slots and paths
// ---------------------------------------------------------------------------

#[test]
fn declared_types_survive_into_the_slot_layout() {
    let p = ok(vec![json("port: i64;"), code(1, "out = port + 1;")]);
    let port = p.provenance.iter().find(|o| o.name == "port").unwrap();
    assert_eq!(port.ty, DataType::I64);
}

/// Ingest writes the reserved slots and the source's fields by position, so the plan must bind each name where ingest
/// writes it: source field `i` at `RAW_SLOTS + i`.
#[test]
fn reserved_slots_come_first_then_the_sources_own_fields() {
    use smeltr::core::batch::{RAW_SLOTS, SLOT_LIVE, SLOT_RAW, raw_layout};
    let p = ok(vec![json("")]);
    assert_eq!(p.provenance[SLOT_LIVE].name, "__live");
    assert_eq!(p.provenance[SLOT_RAW].name, "_raw");
    assert_eq!(p.slot_layout[..RAW_SLOTS], raw_layout()[..]);
    for (i, (name, ty)) in smeltr::plan::topology::SourceMeta::KAFKA
        .source_fields()
        .iter()
        .enumerate()
    {
        assert_eq!(p.provenance[RAW_SLOTS + i].name, *name, "source field {i}");
        assert_eq!(p.slot_layout[RAW_SLOTS + i], *ty, "source field {i}");
    }
}

/// A declared struct is its leaves: `geo.city` is a leaf of the tree, and a dotted read of it names that leaf's slot.
#[test]
fn a_dotted_read_of_a_declared_structs_field_shares_its_leaf() {
    let p = ok(vec![
        json("geo: {city: str, country: str};"),
        code(1, "out = geo.city;"),
    ]);
    assert!(p.warnings.is_empty(), "{:?}", p.warnings);
    let city = p.tree.leaf("geo.city").expect("`geo.city` is a leaf");
    assert_eq!(p.tree.leaf("out"), Some(city), "`out` is `geo.city`'s slot");
}

/// A dotted path walks through nested declared structs to the leaf, and the read has the leaf's type.
#[test]
fn a_dotted_path_through_nested_declared_structs_reads_the_leaf() {
    let p = ok(vec![json("n: {inner: {deep: i64}};"), code(1, "out = n.inner.deep;")]);
    assert!(p.warnings.is_empty(), "{:?}", p.warnings);
    let out = p.tree.leaf("out").unwrap();
    assert_eq!(p.provenance[out].ty, DataType::I64);
}

/// A closed object has no unclaimed fields: a dotted read of a key it does not have is a compile error on the step that
/// reads it, as `geo["nosuch"]` is. An object a JSON parser declares open has unclaimed fields, and the same read of
/// one is a variant read of them.
#[test]
fn a_key_the_object_does_not_have_is_a_compile_error_unless_the_object_is_open() {
    for expr in ["geo.nosuch", "geo[\"nosuch\"]"] {
        let read = code(1, &format!("out = {expr};"));
        let e = err(vec![code(0, r#"geo = {city: "x", country: "y"};"#), read.clone()]);
        assert!(
            e.iter()
                .any(|e| e.function == Some(1) && e.message.contains("`geo.nosuch` is not a field")),
            "{expr}: {e:?}"
        );

        let p = ok(vec![json("geo: {city: str, country: str, ...};"), read]);
        let out = p.tree.leaf("out").expect("`out` is a leaf");
        assert_eq!(p.provenance[out].ty, DataType::Variant, "{expr}");
    }
}

/// A rename moves the object whole, its leaves with it, so a dotted read of the new name reads the old one's leaf.
#[test]
fn a_renamed_declared_struct_carries_its_dotted_reads_with_it() {
    let p = ok(vec![
        json("geo: {city: str, country: str};"),
        code(1, "move(geo, location);"),
        code(2, "out = location.city;"),
    ]);
    assert!(p.warnings.is_empty(), "{:?}", p.warnings);
    let out = p.tree.leaf("out").unwrap();
    assert_eq!(p.provenance[out].ty, DataType::Str);
}

/// An object literal bound in an earlier step is its paths: `obj.k` is a column the step produced, and there is no
/// `obj` slot beside it.
#[test]
fn a_dotted_read_of_a_locally_built_object_reads_its_path() {
    let p = ok(vec![code(1, "obj = {k: 1};"), code(2, "out = obj.k;")]);
    assert!(p.warnings.is_empty(), "{:?}", p.warnings);
    assert!(p.tree.leaves().iter().any(|(n, _)| n == "obj.k"));
    assert!(!p.tree.leaves().iter().any(|(n, _)| n == "obj"));
}

/// A parser's declared `source.ip` makes `source` an object, and an object keeps its type: a scalar written over it is
/// a compile error on the step that writes it.
#[test]
fn assigning_a_scalar_over_a_declared_object_is_a_compile_error() {
    let e = err(vec![json("source.ip: str;"), code(1, r#"source = "x";"#)]);
    assert!(
        e.iter()
            .any(|e| e.function == Some(1) && e.message.contains("is str, not {ip: str, ...}")),
        "{e:?}"
    );
}

/// A scalar has no path under it: a read of `host.name`, where a parser declared `host` a `str`, is a compile error on
/// the step that reads it. A dynamic parser before it binds nothing there.
#[test]
fn a_path_under_a_declared_scalar_is_a_compile_error() {
    let e = err(vec![
        json(""),
        code(1, r#"host: str; Object.assign($, grok(_raw, ["%{GREEDYDATA:host}"]));"#),
        code(2, "out = host.name;"),
    ]);
    assert!(
        e.iter()
            .any(|e| e.function == Some(2) && e.message.contains("`host` is str and has no fields")),
        "{e:?}"
    );
}

/// A rename moves its source whole, the paths under it with the rest. After `foo` becomes `source`, `source.ip` is
/// read from what `foo` held, whatever the record holds under `source` itself.
#[test]
fn a_rename_moves_the_paths_under_its_source() {
    let plan = ok(vec![
        json("foo.ip: str; source.ip: str;"),
        code(2, "move(foo, source);"),
        code(3, "out = source.ip;"),
    ]);
    let mut env = Env::new();
    let mut b = raw_batch(
        &plan,
        &[r#"{"foo":{"ip":"from foo"},"source":{"ip":"from source"}}"#],
        &mut env.pool,
    );
    let rows = env.run_json(&plan, &mut b);
    assert_eq!(rows[0]["out"], "from foo", "{}", rows[0]);
    b.reclaim(&mut env.pool);
}

/// One move's target is a dotted path of another's. A later move replaces what is under its target, so after
/// `move(x, p)`, `p.q` is `x.q`, not the `y` the move before put there.
#[test]
fn a_later_rename_pair_replaces_an_earlier_pairs_dotted_target() {
    let plan = ok(vec![
        json("x.q: i64; y: i64;"),
        code(1, "move(y, p.q); move(x, p);"),
        code(2, "out = p.q;"),
    ]);
    let mut env = Env::new();
    let mut b = raw_batch(&plan, &[r#"{"x":{"q":1},"y":2}"#], &mut env.pool);
    let rows = env.run_json(&plan, &mut b);
    assert_eq!(rows[0]["out"], 1, "{}", rows[0]);
    b.reclaim(&mut env.pool);
}

// ---------------------------------------------------------------------------
// Liveness
// ---------------------------------------------------------------------------

#[test]
fn a_live_output_field_is_never_offered_for_reuse() {
    // "a" is read once (by the eval below) and never reassigned, so it
    // remains a live output field all the way to serialization --
    // liveness must not treat "read once" as "dead", or a same-shape
    // instruction later in the pipeline (here, nothing does, but see
    // `json-parser-tests.rs`'s `a_live_field_survives_a_send_step_with_a_
    // matching_reuse_shape` for a case where one does) could silently
    // steal its buffer before the row is ever serialized.
    let p = ok(vec![json("a: i64;"), code(1, "b = a + 1;"), code(2, "c = b + 1;")]);
    let a_slot = p.tree.leaf("a").expect("\"a\" is a leaf");
    assert!(
        p.instrs.iter().all(|i| i.reuse != Some(a_slot)),
        "\"a\" is live all the way to output and must never be reused"
    );
}

// ---------------------------------------------------------------------------
// Observability
// ---------------------------------------------------------------------------

#[test]
fn a_realistic_pipeline_compiles() {
    let p = ok(vec![
        json("http.status: i64; srcip: str;"),
        code(1, "move(srcip, source.ip);"),
        code(
            2,
            r#"
            http.is_error = http.status >= 400;
            severity: str = null;
            if (http.is_error) { severity = "high"; } else { severity = "low"; }
            if (http.status == 200) drop()
            "#,
        ),
    ]);

    // The rename is the write `source.ip = srcip`. `source` is a key of the record's unclaimed fields, so the write
    // splits it into an object that keeps the key's other keys (rule 3): that split is the rename's one instruction.
    // The name it leaves costs nothing (rule 6).
    let source = p.tree.object("source").and_then(|o| o.unclaimed);
    let split: Vec<_> = p.instrs.iter().filter(|i| i.function == 1).map(|i| i.out).collect();
    assert_eq!(
        split,
        source.into_iter().collect::<Vec<_>>(),
        "the rename emits only the split of `source`"
    );
    assert!(!p.instrs.is_empty());
}

// ---------------------------------------------------------------------------
// Dynamic typing
// ---------------------------------------------------------------------------

/// After a JSON parse a field no declaration names is a variant read of the root's unclaimed fields. An operator, a
/// comparison or a condition that reads one is a compile error on its step that names the type to give it.
#[test]
fn an_undeclared_field_is_refused_where_it_is_read() {
    for (step, read, want) in [
        (1, "is_error = http.status >= 400;", "`http.status` is a variant"),
        (1, r#"is_web = host == "web01";"#, "`host` is a variant"),
        (2, "if (flag) { y = 1; }", "condition is a variant"),
    ] {
        let e = err(vec![json(""), code(step, read)]);
        assert_eq!(e[0].function, Some(step), "{e:?}");
        assert!(e[0].message.contains(want), "{e:?}");
        assert!(e[0].message.contains("give it a type first"), "{e:?}");
    }
}

/// A key under a variant is a variant, and an operator, a comparison or a condition given one is a compile error
/// that names the type to give it: a declaration of the key. `m` is the variant an open `{...}` holds under a key
/// only the row knows.
#[test]
fn a_variant_is_refused_where_it_is_read() {
    for (step, read, name) in [
        (2, "is_error = m.status >= 400;", "`m.status` is a variant"),
        (2, r#"is_web = m.host == "web01";"#, "`m.host` is a variant"),
        (3, "if (m.flag) { y = 1; }", "condition is a variant"),
    ] {
        let e = err(vec![
            json("pairs: array<{key: str, value: str}>;"),
            code(
                1,
                r#"let w: {...} = {}; let k = "v"; w[k] = fromEntries(pairs); m = w.v;"#,
            ),
            code(step, read),
        ]);
        assert_eq!(e[0].function, Some(step), "{e:?}");
        assert!(e[0].message.contains(name), "{e:?}");
        assert!(e[0].message.contains("give it a type first"), "{e:?}");
    }
}

#[test]
fn a_declared_type_is_still_enforced() {
    // Declaring a type is a promise the parser coerces to, so the checker can
    // rely on it — and reject misuse.
    let e = err(vec![json("name: str;"), code(1, "x = name + 1;")]);
    assert!(e[0].message.contains("str"), "{:?}", e);
}

/// A declared path and a declared field read alike, whichever declared them.
#[test]
fn dynamic_and_declared_fields_mix() {
    ok(vec![
        json("port: i64; whatever.count: i64;"),
        code(1, "both = port + whatever.count;"),
    ]);
}

// ---------------------------------------------------------------------------
// Unroll
// ---------------------------------------------------------------------------

#[test]
fn unroll_rebinds_the_field_to_its_element_type() {
    ok(vec![
        json("tags: array<i64>;"),
        code(1, "unroll(tags);"),
        // Type-checks only if "tags" now resolves to I64, not Array(I64).
        code(2, "doubled = tags * 2;"),
    ]);
}

#[test]
fn unroll_on_an_unbound_field_is_a_compile_error() {
    let e = err(vec![code(0, "unroll(ghost);")]);
    assert!(e[0].message.contains("not bound"), "{:?}", e);
}

#[test]
fn unroll_on_a_non_array_field_is_a_compile_error() {
    let e = err(vec![json("status: i64;"), code(1, "unroll(status);")]);
    assert!(e[0].message.contains("Array"), "{:?}", e);
}

// ---------------------------------------------------------------------------
// CSV
// ---------------------------------------------------------------------------

/// A csv parser step, for the tests of the step itself.
fn csv(id: FunctionId, headers: &[&str], declared: &[(&str, DataType)]) -> Step {
    func(
        id,
        FunctionKind::Parser {
            input: "_raw".into(),
            kind: ParserKind::Csv(smeltr::plan::config::CsvConfig {
                split_dots: false,
                delimiter: b',',
                headers: headers.iter().map(|h| Some(h.to_string())).collect(),
                ..Default::default()
            }),
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

/// Step 0: `host: str`, then a csv parse of `_raw` with columns `host` and `code`.
const HOST_CODE: &str = r#"host: str; Object.assign($, csv(_raw, ["host", "code"], {delimiter: ","}));"#;

// CSV binds exactly its headers, so a name the config did not write down is an
// unknown field after one.

#[test]
fn a_csv_rejects_an_unknown_name_downstream() {
    let errs = err(vec![code(0, HOST_CODE), code(1, "out = hsot;")]);
    assert!(
        errs.iter().any(|e| e.message.contains("`hsot` is not a field")),
        "{errs:?}"
    );
}

/// A csv step's column is a header: one key, or under `splitDots` a path split at its dots.
#[test]
fn a_csv_step_column_is_one_key_unless_it_splits_dots() {
    for (split_dots, want) in [
        (false, serde_json::json!({"src.ip": "1.2.3.4"})),
        (true, serde_json::json!({"src": {"ip": "1.2.3.4"}})),
    ] {
        let step = func(
            0,
            FunctionKind::Parser {
                input: "_raw".into(),
                kind: ParserKind::Csv(smeltr::plan::config::CsvConfig {
                    split_dots,
                    delimiter: b',',
                    headers: vec![Some("src.ip".into())],
                    ..Default::default()
                }),
                declared: vec![DeclaredField {
                    name: "src.ip".into(),
                    ty: DataType::Str,
                    time_source: None,
                }],
            },
        );
        let plan = ok(vec![step]);
        let mut env = Env::new();
        let mut b = raw_batch(&plan, &["1.2.3.4"], &mut env.pool);
        let mut rows = env.run_json(&plan, &mut b);
        rows[0].as_object_mut().expect("an object").remove("_raw");
        assert_eq!(rows[0], want);
        b.reclaim(&mut env.pool);
    }
}

/// A header nothing declares is still a column the parse writes: it is a `str` field, read like any other.
#[test]
fn a_csv_column_nobody_declared_is_a_str_field() {
    let plan = ok(vec![code(0, HOST_CODE), code(1, "out = code;")]);
    let out = plan.tree.leaf("out").expect("`out` is a leaf");
    assert_eq!(plan.provenance[out].ty, DataType::Str);
    let mut env = Env::new();
    let mut b = raw_batch(&plan, &["web1,200"], &mut env.pool);
    let rows = env.run_json(&plan, &mut b);
    assert_eq!(rows[0]["out"], "200", "{}", rows[0]);
    b.reclaim(&mut env.pool);
}

/// JSON and key-value take a field's name from the record itself, so the producible set is unknowable at compile time
/// and a name this author's sample never contained is an ordinary declaration. CSV is not in this list: its names come
/// from `headers` or a column position, never from the data, so it is checked like Grok.
#[test]
fn a_parser_that_names_fields_from_the_record_declares_whatever_it_likes() {
    for kind in [
        ParserKind::Json { split_dots: false },
        ParserKind::KeyValue(smeltr::plan::config::KvConfig::default()),
    ] {
        let step = func(
            0,
            FunctionKind::Parser {
                input: "_raw".into(),
                kind: kind.clone(),
                declared: vec![DeclaredField {
                    name: "nothing_here_names_me".into(),
                    ty: DataType::Str,
                    time_source: None,
                }],
            },
        );
        assert!(
            compile(&pipeline(vec![step]), &smeltr::plan::topology::SourceMeta::KAFKA).is_ok(),
            "{kind:?} names fields from the record, so it must accept this"
        );
    }
}
