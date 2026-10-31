//! `csvRebuild` and `kvRebuild`: a record's text again, as the parser of its format read it, with the columns or pairs a
//! parse skips taken out. The rest of the text is kept byte for byte, its quoting and spacing included, so what reads
//! the line downstream reads it as it read the original. A row that does not split whole is kept as it came.

use super::helpers::short_circuit_empty;
use crate::core::bits;
use crate::core::column::{Column, DataType, List, StrBuilder, VarType};
use crate::core::ctx::Ctx;
use crate::format::{csv_scan, kv_scan};
use crate::plan::config::{CsvConfig, KvConfig};

/// `csvRebuild(text, headers, delimiter, quote, escape, squeeze, trim)`: `text` with each column whose header is `null`
/// left empty. `escape` is `""` where a quote is doubled.
pub fn csv_rebuild(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    rebuild(ctx, ins, |args, row, text| {
        let config = CsvConfig {
            delimiter: args.byte(2, row)?,
            quote: args.byte(3, row)?,
            escape: args.maybe_byte(4, row)?,
            squeeze: args.flag(5, row)?,
            trim: args.flag(6, row)?,
            ..CsvConfig::default()
        };
        let skipped = args.elements(1, row, |valid, _| !valid)?;
        Some(
            csv_scan::blank_columns(text, &config, |c| skipped.get(c).copied().unwrap_or(false))
                .unwrap_or_else(|| text.to_vec()),
        )
    })
}

/// `kvRebuild(text, skip, pairSep, kvSep, quote, escapes, nextKey)`: `text` without the pairs whose key `skip` names.
/// `quote` is `""` where no value is quoted.
pub fn kv_rebuild(ctx: &mut Ctx, ins: &[&Column]) -> Column {
    rebuild(ctx, ins, |args, row, text| {
        let config = KvConfig {
            pair_sep: args.byte(2, row)?,
            kv_sep: args.byte(3, row)?,
            quote: args.maybe_byte(4, row)?,
            escapes: args.flag(5, row)?,
            next_key: args.flag(6, row)?,
            ..KvConfig::default()
        };
        let skip = args.elements(1, row, |valid, key| valid.then(|| key.to_vec()))?;
        let skip: Vec<&[u8]> = skip.iter().flatten().map(Vec::as_slice).collect();
        Some(kv_scan::drop_pairs(text, &config, |k| skip.contains(&k.as_bytes())).unwrap_or_else(|| text.to_vec()))
    })
}

/// Runs `each` on every row whose arguments are all there; a null one makes the row null.
fn rebuild(ctx: &mut Ctx, ins: &[&Column], each: impl Fn(&Args, usize, &[u8]) -> Option<Vec<u8>>) -> Column {
    let len = ins[0].len();
    if let Some(e) = short_circuit_empty(ins, &DataType::Str, len) {
        return e;
    }
    let filled: Vec<Column> = ins
        .iter()
        .map(|c| match c.constant_value() {
            Some(v) => Column::filled(ctx.pool, v, len),
            None => Column::empty(c.data_type(), 0),
        })
        .collect();
    let cols: Vec<&Column> = ins
        .iter()
        .zip(&filled)
        .map(|(c, f)| if c.constant_value().is_some() { f } else { *c })
        .collect();
    let args = Args(&cols);
    let text = cols[0].var().expect("a Str text");
    let mut b = StrBuilder::new(ctx.pool, VarType::Str, len);
    for row in 0..len {
        match cols
            .iter()
            .all(|c| c.is_valid(row))
            .then(|| each(&args, row, text.get(row)))
            .flatten()
        {
            Some(out) => b.push_grow(ctx.pool, &out),
            None => b.push_null(),
        }
    }
    b.finish()
}

/// The arguments of one call, every one as rows.
struct Args<'a>(&'a [&'a Column]);

impl Args<'_> {
    fn text(&self, i: usize, row: usize) -> Option<&[u8]> {
        Some(self.0[i].var().ok()?.get(row))
    }

    /// One byte, as a one-character string.
    fn byte(&self, i: usize, row: usize) -> Option<u8> {
        match self.text(i, row)? {
            [b] => Some(*b),
            _ => None,
        }
    }

    /// One byte, or none for `""`.
    fn maybe_byte(&self, i: usize, row: usize) -> Option<Option<u8>> {
        match self.text(i, row)? {
            [] => Some(None),
            [b] => Some(Some(*b)),
            _ => None,
        }
    }

    fn flag(&self, i: usize, row: usize) -> Option<bool> {
        let b = self.0[i].bool().ok()?;
        Some(bits::get_row(b.values, row))
    }

    /// `each` of the array's elements in `row`: whether it is there, and its text where it is a string.
    fn elements<T>(&self, i: usize, row: usize, each: impl Fn(bool, &[u8]) -> T) -> Option<Vec<T>> {
        let List { offsets, child, .. } = self.0[i].list().ok()?;
        let text = child.var().ok();
        Some(
            (offsets[row] as usize..offsets[row + 1] as usize)
                .map(|e| {
                    let valid = child.is_valid(e);
                    each(valid, text.as_ref().filter(|_| valid).map_or(&[][..], |t| t.get(e)))
                })
                .collect(),
        )
    }
}
