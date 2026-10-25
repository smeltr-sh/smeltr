//! A parse of `ParserKind::Grok`. Pattern compilation is `grok_scan`; the staging is `spread`.
//!
//! Every Grok field is declared, so nothing lands in the unclaimed fields. A row matching no candidate pattern is a
//! `Unparsable`; a row that matches but lacks one capture leaves that field null.

use crate::core::buffer::BufferPool;
use crate::core::column::{Column, DataType};
use crate::format::grok_scan::{Group, Matcher};
use crate::format::spread::{self, Scanner, Spread};
use crate::plan::plan::Output;
use crate::plan::tree;

/// A parse of `ParserKind::Grok`: a row parses where a candidate pattern matches it. The matcher is compiled once at
/// plan-compile time (`Call::Parse::matcher`).
pub struct Grok<'m> {
    matcher: &'m Matcher,
    /// Which capture group each output reads, per candidate pattern, by the output's index.
    groups: Vec<Vec<Option<Group>>>,
    /// A `Str` column is text already; only a `Bytes` input has to be checked.
    trusted_utf8: bool,
}

impl<'m> Grok<'m> {
    /// Each output's capture group is resolved once here, so the row loop indexes captures and stages by output index,
    /// hashing no name per row. A capture's name is the output's name, its key split at dots where `split_dots` says.
    pub fn new(matcher: &'m Matcher, split_dots: bool, outputs: &[Output], input: &Column) -> Self {
        let captures = matcher.capture_names();
        let capture = |name: &str| captures.iter().find(|c| tree::name_of(c, split_dots) == name).copied();
        let groups = (0..matcher.pattern_count())
            .map(|p| {
                outputs
                    .iter()
                    .map(|o| capture(&o.name).and_then(|c| matcher.group_index(p, c)))
                    .collect()
            })
            .collect();
        Grok {
            matcher,
            groups,
            trusted_utf8: matches!(input.data_type(), DataType::Str),
        }
    }
}

impl Scanner for Grok<'_> {
    const NAME: &'static str = "grok";
    type Scratch<'d> = ();

    fn scan<'d>(
        &self,
        input: &'d Column,
        row: usize,
        _: &mut (),
        spread: &mut Spread<'d>,
        pool: &mut BufferPool,
    ) -> Option<bool> {
        let bytes = spread::row_bytes(input, row)?;
        let text = match self.trusted_utf8 {
            // SAFETY: a `Str` column holds valid UTF-8 by the columnar model's invariant.
            true => Some(unsafe { std::str::from_utf8_unchecked(bytes) }),
            false => std::str::from_utf8(bytes).ok(),
        };
        let Some(matched) = text.and_then(|text| self.matcher.match_row(text)) else {
            return Some(false);
        };
        spread.row(row, pool);
        for (index, group) in self.groups[matched.pattern].iter().enumerate() {
            if let Some(text) = group.and_then(|g| matched.get(g)) {
                spread.set_text_at(index, text, false, pool);
            }
        }
        Some(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::batch::{Batch, SLOT_RAW};
    use crate::core::buffer::BufferPool;
    use crate::core::column::{DataType, StrBuilder, VarType};
    use crate::core::ctx::Ctx;
    use crate::core::error::ErrorSink;
    use crate::core::error::ExceptionKind;
    use crate::plan::config::{
        DeclaredField, FunctionConfig, FunctionKind, GrokConfig, ParserKind, PipelineConfig, Step,
    };
    use crate::plan::plan::{Call, Op, Plan, compile};

    fn compile_pipeline(config: GrokConfig, declared: &[(&str, DataType)]) -> Plan {
        let p = PipelineConfig {
            id: 0,
            name: "test".into(),
            body: vec![Step::Function(FunctionConfig::new(
                0,
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
            ))],
            step_ids: vec!["p".into()],
            lookup_tables: Default::default(),
            grok_patterns: Default::default(),
        };
        compile(&p, &crate::plan::topology::SourceMeta::KAFKA).unwrap_or_else(|e| {
            panic!(
                "compile failed: {}",
                e.iter().map(|x| x.to_string()).collect::<Vec<_>>().join("; ")
            )
        })
    }

    fn parse_fields(plan: &Plan) -> Vec<Output> {
        plan.instrs
            .iter()
            .find_map(|i| match &i.op {
                Op::Call { outputs, .. } => Some(outputs.clone()),
                _ => None,
            })
            .unwrap()
    }

    /// The matcher `compile` cached on the plan's `Call::Parse`, which is what `parse_batch` receives at runtime.
    fn parse_matcher(plan: &Plan) -> std::sync::Arc<Matcher> {
        plan.instrs
            .iter()
            .find_map(|i| match &i.op {
                Op::Call {
                    call: Call::Parse { matcher, .. },
                    ..
                } => matcher.clone(),
                _ => None,
            })
            .unwrap()
    }

    /// `inspect` sees the parsed batch before it is reclaimed, so a test can read what landed in a declared field.
    fn run_with(
        config: GrokConfig,
        declared: &[(&str, DataType)],
        line: &[u8],
        inspect: impl FnOnce(&Plan, &Batch),
    ) -> ErrorSink {
        let plan = compile_pipeline(config, declared);
        let fields = parse_fields(&plan);
        let matcher = parse_matcher(&plan);
        let mut pool = BufferPool::new();
        let mut errors = ErrorSink::new();
        let mut batch = Batch::new(&plan.slot_layout, 1, 0);
        let mut b = StrBuilder::new(&mut pool, VarType::Str, 1);
        b.reserve(&mut pool, line.len());
        b.push(line);
        batch.put(SLOT_RAW, b.finish(), &mut pool);

        let mut ctx = Ctx::new(&mut pool, &mut errors);
        let grok = Grok::new(&matcher, true, &fields, batch.peek(SLOT_RAW));
        spread::parse_batch(grok, SLOT_RAW, &fields, &[], None, &mut batch, &mut ctx);
        inspect(&plan, &batch);
        batch.reclaim(&mut pool);
        errors
    }

    fn run(config: GrokConfig, declared: &[(&str, DataType)], line: &[u8]) -> ErrorSink {
        run_with(config, declared, line, |_, _| {})
    }

    /// The slot the parse writes `name` to. The tree's leaf is a later instruction's output, which `run_with` never
    /// runs: it drives the parse alone.
    fn slot_of(plan: &Plan, name: &str) -> crate::core::id::SlotId {
        parse_fields(plan)
            .iter()
            .find(|f| f.name == name)
            .unwrap_or_else(|| panic!("the parse writes no `{name}`"))
            .slot
    }

    #[test]
    fn a_matching_row_populates_declared_fields() {
        let config = GrokConfig {
            split_dots: true,
            patterns: vec!["%{WORD:action} %{INT:status}".into()],
            custom_patterns: vec![],
        };
        let errors = run_with(
            config,
            &[("action", DataType::Str), ("status", DataType::I64)],
            b"login 200",
            |plan, batch| {
                let action = batch.get(slot_of(plan, "action"));
                assert_eq!(action.var().unwrap().get(0), b"login");
                let status = batch.get(slot_of(plan, "status"));
                assert_eq!(status.i64().unwrap().values[0], 200, "coerced to its declared type");
            },
        );
        assert!(errors.is_empty(), "{:?}", errors.entries());
    }

    /// A whole-line pattern with nothing declared, as onboarding proposes one, writes every field it names.
    #[test]
    fn a_whole_line_pattern_writes_the_fields_it_names() {
        let config = GrokConfig {
            split_dots: true,
            patterns: vec!["^%{SYSLOGLINE}$".into()],
            custom_patterns: vec![],
        };
        let plan = compile_pipeline(config, &[]);
        let mut names: Vec<String> = parse_fields(&plan).into_iter().map(|f| f.name).collect();
        names.sort();
        assert_eq!(
            names,
            [
                "host.hostname",
                "log.syslog.priority",
                "message",
                "process.name",
                "process.pid",
                "timestamp"
            ]
        );
    }

    #[test]
    fn a_non_matching_row_is_a_parse_failure() {
        let config = GrokConfig {
            split_dots: true,
            patterns: vec!["^%{INT:n}$".into()],
            custom_patterns: vec![],
        };
        let errors = run(config, &[("n", DataType::I64)], b"nope!");
        assert!(
            errors
                .entries()
                .iter()
                .any(|a| a.row == 0 && matches!(a.kind, ExceptionKind::Unparsable))
        );
    }
}
