//! A parse of `ParserKind::Csv`. Field splitting is `csv_scan`; the staging is `spread`. This
//! module names columns (from `CsvConfig::headers`, applied positionally, since one `_raw` record has no header row of
//! its own).

use crate::core::buffer::BufferPool;
use crate::core::column::{Column, DataType};
use crate::format::csv_scan;
use crate::format::spread::{self, Scanner, Spread};
use crate::plan::config::CsvConfig;
use crate::plan::plan::Output;
use crate::plan::tree;

/// A parse of `ParserKind::Csv`: a row parses where it splits whole into fields.
pub struct Csv<'c> {
    config: &'c CsvConfig,
    /// The field each position of the column table names, by its index in the outputs.
    columns: Vec<Option<usize>>,
    /// A `Str` column is text already; only a `Bytes` input has to be checked field by field.
    trusted_utf8: bool,
}

impl<'c> Csv<'c> {
    /// The column table is positional, so each position resolves to its field once here, and the row loop indexes.
    pub fn new(config: &'c CsvConfig, outputs: &[Output], input: &Column) -> Self {
        let columns = config
            .headers
            .iter()
            .map(|h| {
                let name = tree::name_of(h.as_deref()?, config.split_dots);
                outputs.iter().position(|o| o.name == name)
            })
            .collect();
        Csv {
            config,
            columns,
            trusted_utf8: matches!(input.data_type(), DataType::Str),
        }
    }
}

impl Scanner for Csv<'_> {
    const NAME: &'static str = "csv";
    type Scratch<'d> = Vec<csv_scan::Field<'d>>;

    fn scan<'d>(
        &self,
        input: &'d Column,
        row: usize,
        fields: &mut Vec<csv_scan::Field<'d>>,
        spread: &mut Spread<'d>,
        pool: &mut BufferPool,
    ) -> Option<bool> {
        let bytes = spread::row_bytes(input, row)?;
        // A record that is not whole keeps the fields read before it broke off.
        let whole = csv_scan::split_line_into(bytes, self.config, self.trusted_utf8, fields);
        spread.row(row, pool);
        // Past the column table nothing names the column -- a record carries no header row -- so the row's tail is
        // dropped.
        for (column, field) in self.columns.iter().zip(fields.iter()) {
            if let Some(index) = column {
                spread.set_text_at(*index, &field.text, field.quoted, pool);
            }
        }
        Some(whole)
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
    use crate::plan::config::{DeclaredField, FunctionConfig, FunctionKind, ParserKind, PipelineConfig, Step};
    use crate::plan::plan::{Plan, compile};

    fn compile_pipeline(config: CsvConfig, declared: &[(&str, DataType)]) -> Plan {
        let p = PipelineConfig {
            id: 0,
            name: "test".into(),
            body: vec![Step::Function(FunctionConfig::new(
                0,
                FunctionKind::Parser {
                    input: "_raw".into(),
                    into: String::new(),
                    kind: ParserKind::Csv(config),
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

    fn parse_fields(plan: &Plan) -> Vec<Output> {
        plan.instrs
            .iter()
            .find_map(|i| match &i.op {
                crate::plan::plan::Op::Call { outputs, .. } => Some(outputs.clone()),
                _ => None,
            })
            .unwrap()
    }

    fn seed(pool: &mut BufferPool, batch: &mut Batch, rows: &[&str]) {
        let mut b = StrBuilder::new(pool, VarType::Str, rows.len());
        b.reserve(pool, rows.iter().map(|r| r.len()).sum());
        for r in rows {
            b.push(r.as_bytes());
        }
        batch.put(SLOT_RAW, b.finish(), pool);
    }

    /// A record carries no header row, so a column past the table has nothing to name it and the row's tail is dropped
    /// rather than named by position. Naming those columns is the author's move, in the table.
    #[test]
    fn a_column_past_the_table_is_dropped() {
        let config = CsvConfig {
            split_dots: false,
            delimiter: b',',
            headers: vec![Some("a".into()), Some("b".into())],
            ..CsvConfig::default()
        };
        let plan = compile_pipeline(config.clone(), &[("a", DataType::I64), ("b", DataType::I64)]);
        let fields = parse_fields(&plan);
        let mut pool = BufferPool::new();
        let mut errors = ErrorSink::new();
        let mut batch = Batch::new(&plan.slot_layout, 1, 0);
        seed(&mut pool, &mut batch, &["10,20,thirty"]);

        let mut ctx = Ctx::new(&mut pool, &mut errors);
        let csv = Csv::new(&config, &fields, batch.peek(SLOT_RAW));
        spread::parse_batch(csv, SLOT_RAW, &fields, &[], None, &mut batch, &mut ctx);
        assert!(errors.is_empty(), "a wide row still parses");
        let written = [SLOT_RAW, fields[0].slot, fields[1].slot];
        assert!(
            (0..plan.slot_layout.len())
                .filter(|s| !written.contains(s))
                .all(|s| batch.peek(s).is_unbound()),
            "nothing else is written"
        );

        batch.reclaim(&mut pool);
    }

    /// A row narrower than the table leaves the columns it lacks null; a wider one loses only its tail.
    #[test]
    fn varying_row_widths_fill_what_each_row_reaches() {
        let config = CsvConfig {
            split_dots: false,
            delimiter: b',',
            headers: vec![Some("a".into()), Some("b".into()), Some("c".into())],
            ..CsvConfig::default()
        };
        let declared = [("a", DataType::I64), ("b", DataType::I64), ("c", DataType::I64)];
        let plan = compile_pipeline(config.clone(), &declared);
        let fields = parse_fields(&plan);
        let mut pool = BufferPool::new();
        let mut errors = ErrorSink::new();
        let rows = ["1,2,3", "4,5", "6,7,8,9"];
        let mut batch = Batch::new(&plan.slot_layout, rows.len(), 0);
        seed(&mut pool, &mut batch, &rows);

        let mut ctx = Ctx::new(&mut pool, &mut errors);
        let csv = Csv::new(&config, &fields, batch.peek(SLOT_RAW));
        spread::parse_batch(csv, SLOT_RAW, &fields, &[], None, &mut batch, &mut ctx);
        assert!(errors.is_empty(), "every row parses");

        let slot = |name: &str| fields.iter().find(|f| f.name == name).unwrap().slot;
        let c = batch.peek(slot("c")).i64().unwrap();
        assert_eq!(c.values[0], 3);
        assert!(!c.validity.is_valid(1), "the narrow row never reached `c`");
        assert_eq!(c.values[2], 8);

        let a = batch.peek(slot("a")).i64().unwrap();
        assert_eq!([a.values[0], a.values[1], a.values[2]], [1, 4, 6]);

        batch.reclaim(&mut pool);
    }
}
