//! A parse of `ParserKind::KeyValue` or `ParserKind::Cef`. Pair splitting is `kv_scan` or `cef_scan`; the walk of the
//! name tree is `spread`. A pair's key, split at its dots unless the call says not to, names a declared field, or is a
//! key nothing claims, laid into the unclaimed fields.

use std::borrow::Cow;

use crate::core::buffer::BufferPool;
use crate::core::column::Column;
use crate::format::spread::{self, Scanner, Spread, Step};
use crate::format::tag::{self, Tag};
use crate::format::{cef_scan, kv_scan};
use crate::plan::config::KvConfig;

/// A parse of `ParserKind::KeyValue`: a row parses where it splits whole into pairs.
pub struct Kv<'c>(pub &'c KvConfig);

impl Scanner for Kv<'_> {
    const NAME: &'static str = "kv";
    type Scratch<'d> = Vec<kv_scan::Pair<'d>>;

    fn split_dots(&self) -> bool {
        self.0.split_dots
    }

    fn scan<'d>(
        &self,
        input: &'d Column,
        row: usize,
        pairs: &mut Vec<kv_scan::Pair<'d>>,
        spread: &mut Spread<'d>,
        pool: &mut BufferPool,
    ) -> Option<bool> {
        let bytes = spread::row_bytes(input, row)?;
        // A record that is not whole keeps the pairs read before it broke off.
        let whole = kv_scan::split_pairs(bytes, self.0, pairs);
        if !self.0.skip.is_empty() {
            pairs.retain(|p| !self.0.skip.iter().any(|k| k == p.key));
        }
        lay(row, pairs, spread, pool);
        Some(whole)
    }
}

/// A parse of `ParserKind::Cef`, whose bool is `split_dots`: a row parses where it reads whole as a CEF record.
pub struct Cef(pub bool);

impl Scanner for Cef {
    const NAME: &'static str = "cef";
    type Scratch<'d> = Vec<kv_scan::Pair<'d>>;

    fn split_dots(&self) -> bool {
        self.0
    }

    fn scan<'d>(
        &self,
        input: &'d Column,
        row: usize,
        pairs: &mut Vec<kv_scan::Pair<'d>>,
        spread: &mut Spread<'d>,
        pool: &mut BufferPool,
    ) -> Option<bool> {
        let bytes = spread::row_bytes(input, row)?;
        let whole = cef_scan::split_record(bytes, pairs);
        lay(row, pairs, spread, pool);
        Some(whole)
    }
}

/// Lays `row`'s pairs, each under its key. Inlined into each scanner's row loop, which it is most of.
#[inline(always)]
fn lay<'d>(row: usize, pairs: &mut Vec<kv_scan::Pair<'d>>, spread: &mut Spread<'d>, pool: &mut BufferPool) {
    spread.row(row, pool);
    for pair in pairs.drain(..) {
        match spread.enter(Cow::Borrowed(pair.key), false) {
            Step::Whole => spread.text(&pair.value, pair.quoted, pool),
            Step::Copy => spread.copy_text(&pair.value, pair.quoted),
            Step::Refused => spread.refused(|out| tag::encode(&Tag::Str(pair.value.as_ref().into()), out)),
            Step::Into | Step::Skip => {}
        }
        spread.leave();
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
    use crate::plan::compile::{Call, Op, Output, ParseObject, Plan, compile};
    use crate::plan::config::{DeclaredField, FunctionConfig, FunctionKind, ParserKind, PipelineConfig, Step};

    fn compile_pipeline(config: KvConfig, declared: &[(&str, DataType)]) -> Plan {
        let p = PipelineConfig {
            id: 0,
            name: "test".into(),
            body: vec![Step::Function(FunctionConfig::new(
                0,
                FunctionKind::Parser {
                    input: "_raw".into(),
                    into: crate::plan::tree::ROOT.to_string(),
                    kind: ParserKind::KeyValue(config),
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
        compile(&p, &crate::plan::topology::SourceMeta::KAFKA).unwrap_or_else(|e| panic!("compile failed: {e:?}"))
    }

    fn parse_fields(plan: &Plan) -> (Vec<Output>, Vec<ParseObject>) {
        plan.instrs
            .iter()
            .find_map(|i| match &i.op {
                Op::Call {
                    outputs,
                    call: Call::Parse { objects, .. },
                    ..
                } => Some((outputs.clone(), objects.clone())),
                _ => None,
            })
            .unwrap()
    }

    #[test]
    fn discovered_keys_land_in_the_unclaimed_fields() {
        let plan = compile_pipeline(KvConfig::default(), &[]);
        let (fields, objects) = parse_fields(&plan);
        let mut pool = BufferPool::new();
        let mut errors = ErrorSink::new();
        let mut batch = Batch::new(&plan.slot_layout, 1, 0);
        let mut b = StrBuilder::new(&mut pool, VarType::Str, 1);
        b.reserve(&mut pool, 21);
        b.push(b"status=200 host=web01");
        batch.put(SLOT_RAW, b.finish(), &mut pool);

        let mut ctx = Ctx::new(&mut pool, &mut errors);
        let config = KvConfig::default();
        spread::parse_batch(Kv(&config), SLOT_RAW, &fields, &objects, None, &mut batch, &mut ctx);
        assert!(errors.is_empty());
        let unclaimed_fields = batch.peek(objects[0].out).variant().unwrap();
        let row = crate::format::tag::decode(unclaimed_fields.get(0));
        assert_eq!(
            row,
            Some(crate::format::tag::Tag::Object(vec![
                // An undeclared value is text, whatever it looks like.
                ("status".into(), crate::format::tag::Tag::Str("200".into())),
                ("host".into(), crate::format::tag::Tag::Str("web01".into())),
            ]))
        );
        batch.reclaim(&mut pool);
    }

    /// The proposal a preview makes is what the parser saw: the counts are the values as it staged them, and the
    /// suggestion is the declaration those values would take.
    #[test]
    fn a_listening_run_records_what_each_field_held() {
        let plan = compile_pipeline(KvConfig::default(), &[]);
        let (fields, objects) = parse_fields(&plan);
        let mut pool = BufferPool::new();
        let mut errors = ErrorSink::new();
        let rows = [
            b"status=200 host=web01".as_slice(),
            b"status=nope host=web02".as_slice(),
            b"host=web03".as_slice(),
        ];
        let mut batch = Batch::new(&plan.slot_layout, rows.len(), 0);
        let mut b = StrBuilder::new(&mut pool, VarType::Str, rows.len());
        b.reserve(&mut pool, rows.iter().map(|r| r.len()).sum());
        for r in rows {
            b.push(r);
        }
        batch.put(SLOT_RAW, b.finish(), &mut pool);

        let mut tally = crate::core::tally::FieldTally::new();
        let mut ctx = Ctx::new(&mut pool, &mut errors).tallying(&mut tally);
        let config = KvConfig::default();
        spread::parse_batch(Kv(&config), SLOT_RAW, &fields, &objects, None, &mut batch, &mut ctx);

        let parses = tally.parses();
        assert_eq!(parses.len(), 1, "one parser step ran");
        let seen = &parses[0].fields;
        let status = seen.iter().find(|s| s.name == "status").expect("status was parsed");
        assert_eq!(status.rows, 3);
        assert_eq!(status.counts.present, 2, "one record never mentioned it");
        assert_eq!(
            (status.counts.ints, status.counts.strings),
            (1, 1),
            "the rows disagreed"
        );
        assert_eq!(status.suggested, DataType::Str, "which is what widens the suggestion");

        let host = seen.iter().find(|s| s.name == "host").expect("host was parsed");
        assert_eq!((host.counts.present, host.counts.strings), (3, 3));
        assert_eq!(host.suggested, DataType::Str);

        batch.reclaim(&mut pool);
    }

    /// A parse that bound no field still read its rows, and says how many.
    #[test]
    fn a_parse_that_finds_no_field_still_counts_its_rows() {
        let plan = compile_pipeline(KvConfig::default(), &[]);
        let (fields, objects) = parse_fields(&plan);
        let mut pool = BufferPool::new();
        let mut errors = ErrorSink::new();
        let mut batch = Batch::new(&plan.slot_layout, 3, 0);
        let mut b = StrBuilder::new(&mut pool, VarType::Str, 3);
        for _ in 0..3 {
            b.push(b"");
        }
        batch.put(SLOT_RAW, b.finish(), &mut pool);

        let mut tally = crate::core::tally::FieldTally::new();
        let mut ctx = Ctx::new(&mut pool, &mut errors).tallying(&mut tally);
        let config = KvConfig::default();
        spread::parse_batch(Kv(&config), SLOT_RAW, &fields, &objects, None, &mut batch, &mut ctx);

        let [parse] = tally.parses() else {
            panic!("one parser step ran")
        };
        assert!(parse.fields.is_empty(), "no row held a pair");
        assert_eq!(parse.rows, 3);

        batch.reclaim(&mut pool);
    }
}
