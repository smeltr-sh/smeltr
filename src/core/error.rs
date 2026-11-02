//! The per-row exception channel.
//!
//! Functions raise, never abort: a kernel that hits a bad row writes a null and records an `Exception`. The
//! `exceptions` answer to each kind decides at pipeline exit what becomes of an marked row.
//!
//! The sink lives in `Ctx`, not `Batch`: a kernel holds `&batch` for its inputs while it marks, so the channel must
//! be parallel to the data.

use super::id::{FunctionId, SlotId};
use crate::core::column::{Column, DataType};
use std::collections::HashMap;
use std::ops::Range;

/// The group a kind belongs to: an `exceptions` setting can answer a category as one.
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum Category {
    Parse,
    Math,
    Limits,
    /// Text that did not decode as declared, repaired or guessed. The value is kept.
    Encoding,
    /// A row no pipeline answers for on its own: no route took it, no pipeline finished it, or a destination refused
    /// it.
    Delivery,
}

impl Category {
    /// Every category and its label, in declaration order.
    pub const ALL: [(&'static str, Category); 5] = [
        ("parse", Category::Parse),
        ("math", Category::Math),
        ("limits", Category::Limits),
        ("encoding", Category::Encoding),
        ("delivery", Category::Delivery),
    ];

    /// Whether a `try` catches this category's kinds (Language): an encoding exception kept its value, and a
    /// delivery exception happens outside every pipeline.
    pub fn caught(self) -> bool {
        !matches!(self, Category::Encoding | Category::Delivery)
    }
}

/// What went wrong. An enum, not a `String`: bad rows are not always rare (a misconfigured parser makes every row bad),
/// and metrics count by kind.
#[derive(Clone, PartialEq, Debug)]
pub enum ExceptionKind {
    /// The parser got nothing from this row: malformed JSON, an unterminated CSV quote, a Grok pattern that matched
    /// nothing. Its field is the *input* slot.
    Unparsable,
    /// One declared field could not be produced as its declared type `to`; every sibling parsed fine. That field is null.
    UnparsableField { to: DataType },
    /// A value a declaration met could not be coerced to the declared type: the value is null.
    CoercionRefused { to: DataType },
    /// I64 arithmetic overflowed: the result is null.
    Overflow,
    /// Division by zero. Result is null, not Infinity.
    DivideByZero,
    /// Bytes that did not decode under the source's encoding were replaced with U+FFFD.
    InvalidUtf8,
    /// `auto` recognized neither UTF-8 nor UTF-16 and decoded the record as `encoding`. A guess, not an exception: the
    /// text may well be right. Marked because a source whose every record lands here should declare its encoding.
    GuessedEncoding { encoding: &'static str },
    /// A `while` loop hit its time budget with this row's condition still true; `trips` is how many the batch
    /// completed. The row's accumulators are not what the author asked for.
    LoopBudgetExceeded { trips: u32 },
    /// Two keys of one record contradict after flattening: the same key twice, or a scalar at a name beside a path
    /// under it. One was kept. `key` is the later of the two as the record spelled it.
    KeyContradiction { key: String },
    /// An array read or write, or a string read, named a position the value does not have: the read is null, the write
    /// does nothing.
    IndexOutOfRange,
    /// A parse met `NaN`, `Infinity` or a number past f64's range, which no f64 holds: that key has no value.
    NaN,
    /// No route on the row's source took it.
    Unmatched,
    /// No pipeline could finish the row: one panicked on its batch, or the record does not fit the memory budget alone.
    Unprocessable,
    /// A destination refused the record.
    Refused,
}

impl ExceptionKind {
    pub fn category(&self) -> Category {
        Self::KINDS[self.index()].1
    }

    /// Whether a `try` catches this kind: `Category::caught`.
    pub fn caught(&self) -> bool {
        self.category().caught()
    }

    /// Every kind's `label` and category, in `index` order: what an `exceptions` setting answers by.
    pub const KINDS: [(&'static str, Category); 14] = [
        ("Unparsable", Category::Parse),
        ("UnparsableField", Category::Parse),
        ("CoercionRefused", Category::Parse),
        ("Overflow", Category::Math),
        ("DivideByZero", Category::Math),
        ("InvalidUtf8", Category::Encoding),
        ("GuessedEncoding", Category::Encoding),
        ("LoopBudgetExceeded", Category::Limits),
        ("KeyContradiction", Category::Parse),
        ("IndexOutOfRange", Category::Limits),
        ("NaN", Category::Math),
        ("Unmatched", Category::Delivery),
        ("Unprocessable", Category::Delivery),
        ("Refused", Category::Delivery),
    ];

    /// This kind's position in `KINDS`.
    pub fn index(&self) -> usize {
        match self {
            ExceptionKind::Unparsable => 0,
            ExceptionKind::UnparsableField { .. } => 1,
            ExceptionKind::CoercionRefused { .. } => 2,
            ExceptionKind::Overflow => 3,
            ExceptionKind::DivideByZero => 4,
            ExceptionKind::InvalidUtf8 => 5,
            ExceptionKind::GuessedEncoding { .. } => 6,
            ExceptionKind::LoopBudgetExceeded { .. } => 7,
            ExceptionKind::KeyContradiction { .. } => 8,
            ExceptionKind::IndexOutOfRange => 9,
            ExceptionKind::NaN => 10,
            ExceptionKind::Unmatched => 11,
            ExceptionKind::Unprocessable => 12,
            ExceptionKind::Refused => 13,
        }
    }

    /// The `kind` label of `smeltr_rows_errored_total`. Its own method rather than a sliced `Debug` so a variant gaining a
    /// field cannot change the label.
    pub fn label(&self) -> &'static str {
        Self::KINDS[self.index()].0
    }
}

/// Stamped on an exception from ingest, which no pipeline function can hold: decoding a record happens before any
/// function runs. Visible as the `function` label of `smeltr_rows_errored_total`.
pub const INGEST_FUNCTION: FunctionId = u32::MAX;

#[derive(Clone, PartialEq, Debug)]
pub struct Exception {
    pub row: u32,
    /// Which pipeline function produced this.
    pub function: FunctionId,
    /// The slot it is about, where it is about one.
    pub field: Option<SlotId>,
    pub kind: ExceptionKind,
    /// What the exception read: one per operand of the call that raised it, in order. Empty until its producer fills it.
    pub read: Vec<Read>,
    /// The operator, function or parser that raised it, as the pipeline spells it.
    pub call: &'static str,
}

/// One value an exception read.
#[derive(Clone, PartialEq, Debug)]
pub struct Read {
    /// The slot read and the key path under its record, empty for the slot itself. `None` for a literal or computed
    /// value, which a message names by its value alone.
    pub source: Option<(SlotId, String)>,
    /// The value as the pipeline language spells it, cut at 64 bytes with `…`.
    pub value: Option<String>,
}

impl Read {
    pub fn new(source: Option<(SlotId, String)>, value: Option<String>) -> Read {
        let value = value.map(|mut v| {
            if v.len() > 64 {
                v.truncate(v.floor_char_boundary(64));
                v.push('…');
                // Truncating keeps the input-sized buffer; an exception must not hold it.
                v.shrink_to_fit();
            }
            v
        });
        Read { source, value }
    }

    /// Row `row` of `col`, which `source` holds where it is a slot.
    pub fn of(source: Option<SlotId>, col: &Column, row: usize) -> Read {
        let mut out = Vec::new();
        let value = match crate::format::json::write_value(col, &col.data_type(), row, &mut out) {
            true => String::from_utf8_lossy(&out).into_owned(),
            false => "null".to_string(),
        };
        Read::new(source.map(|s| (s, String::new())), Some(value))
    }
}

impl Exception {
    /// The sentence `e.message`, the preview and a blocked batch's reason show: `<source> is <value>, which <what
    /// failed> in <slot>`, each part the exception holds. `names` names the slots, as `Plan::slot_names` does.
    ///
    /// Distinct facts must give distinct sentences: the preview and a catch dedup a row's exceptions on this text.
    pub fn message(&self, names: &HashMap<SlotId, String>) -> String {
        let named = |r: &Read| {
            let (slot, path) = r.source.as_ref()?;
            let name = names.get(slot)?;
            Some(crate::plan::tree::join(name, path))
        };
        let name = |r: &Read| {
            named(r)
                .or_else(|| r.value.clone())
                .unwrap_or_else(|| "a value".to_string())
        };
        let said = |r: &Read| match (named(r), &r.value) {
            (Some(n), Some(v)) => format!("{n} is {v}"),
            _ => name(r),
        };
        let read = &self.read;
        // The call with its operands by name: `a + b`, `-a`, `abs(a)`.
        let call = match (read.as_slice(), self.call.starts_with(char::is_alphabetic)) {
            ([a, b], false) => format!("{} {} {}", name(a), self.call, name(b)),
            ([a], false) => format!("{}{}", self.call, name(a)),
            _ => format!(
                "{}({})",
                self.call,
                read.iter().map(name).collect::<Vec<_>>().join(", ")
            ),
        };
        let [first, second] = [0, 1].map(|i| read.get(i).map_or("a value".to_string(), said));
        // The slot written, unless the subject already names it.
        let slot = (self.field)
            .filter(|f| read.first().and_then(|r| r.source.as_ref()).is_none_or(|(s, _)| s != f))
            .and_then(|f| names.get(&f))
            .map_or(String::new(), |n| format!(" in {n}"));
        match &self.kind {
            ExceptionKind::Unparsable => format!("{first}, which {} could not parse{slot}", self.call),
            ExceptionKind::UnparsableField { to } => {
                format!("{first}, which could not parse as {}{slot}", to.spell())
            }
            ExceptionKind::CoercionRefused { to } => {
                format!("{first}, which could not convert to {}{slot}", to.spell())
            }
            ExceptionKind::Overflow => {
                let operands = read.iter().map(said).collect::<Vec<_>>().join(" and ");
                format!("{operands}, whose {call} overflowed i64{slot}")
            }
            ExceptionKind::DivideByZero => format!("{second}, which {call} divided by{slot}"),
            ExceptionKind::IndexOutOfRange => {
                format!(
                    "{second}, which is outside {}{slot}",
                    read.first().map_or(String::new(), name)
                )
            }
            ExceptionKind::NaN => format!("{first} held a number no f64 holds{slot}"),
            ExceptionKind::KeyContradiction { key } => {
                let at = read.first().and_then(named).unwrap_or_else(|| key.clone());
                format!("{at} contradicts a key before it; the later was kept")
            }
            ExceptionKind::InvalidUtf8 => format!("{first} held bytes that are not UTF-8, replaced with U+FFFD"),
            ExceptionKind::GuessedEncoding { encoding } => format!("{first} decoded as {encoding}, a guess"),
            ExceptionKind::LoopBudgetExceeded { trips } => {
                let trip = if *trips == 1 { "trip" } else { "trips" };
                format!("a while loop hit its time budget after {trips} {trip}")
            }
            ExceptionKind::Unmatched => "no route took the row".into(),
            ExceptionKind::Unprocessable => "no pipeline could finish the row".into(),
            ExceptionKind::Refused => "a destination refused the row".into(),
        }
    }
}

/// Sparse exception storage: nothing is allocated for a batch with no exceptions. Entries are in insertion order, not
/// sorted by row.
pub struct ErrorSink {
    entries: Vec<Exception>,
}

impl ErrorSink {
    pub fn new() -> Self {
        ErrorSink { entries: Vec::new() }
    }

    /// The entry, for a producer to fill what it read.
    pub fn raise(
        &mut self,
        row: usize,
        function: FunctionId,
        field: Option<SlotId>,
        kind: ExceptionKind,
    ) -> &mut Exception {
        self.entries.push(Exception {
            row: row as u32,
            function,
            field,
            kind,
            read: Vec::new(),
            call: "",
        });
        self.entries.last_mut().expect("just pushed")
    }

    /// Give each entry from `start` on that has read nothing `call` and what `read` says its row read.
    pub fn fill_from(&mut self, start: usize, call: &'static str, read: impl Fn(usize) -> Vec<Read>) {
        for a in self.entries[start..].iter_mut().filter(|a| a.read.is_empty()) {
            (a.call, a.read) = (call, read(a.row as usize));
        }
    }

    /// Put back entries taken out of this sink, after every entry it holds.
    pub fn extend(&mut self, entries: impl IntoIterator<Item = Exception>) {
        self.entries.extend(entries);
    }

    /// Raise an exception on every row in `rows`, for column-level events.
    pub fn raise_range(
        &mut self,
        rows: Range<usize>,
        function: FunctionId,
        field: Option<SlotId>,
        kind: ExceptionKind,
    ) {
        self.entries.reserve(rows.len());
        for row in rows {
            self.raise(row, function, field, kind.clone());
        }
    }

    /// Replace every entry from `start` on: an entry at row `R` becomes one entry per row `expand(R)` returns. How
    /// `Op::Unroll` keeps this run's exceptions attributed after it renumbers rows; `start` is
    /// `Ctx::pipeline_error_start`.
    pub fn remap_rows_from(&mut self, start: usize, expand: impl Fn(u32) -> Vec<u32>) {
        let tail = self.entries.split_off(start.min(self.entries.len()));
        for a in tail {
            for new_row in expand(a.row) {
                self.entries.push(Exception {
                    row: new_row,
                    ..a.clone()
                });
            }
        }
    }

    /// Drop every entry from `start` on whose row `taken` refuses. An exception belongs to a row only when the row
    /// takes the value it came from: a selection computes every branch on every row (design doc, Columnar data model),
    /// so the branch a row did not take is retracted here, after the fact, and never fails the row.
    pub fn retract_from(&mut self, start: usize, taken: impl Fn(u32) -> bool) {
        let end = self.entries.len();
        self.retract_range(start, end, taken);
    }

    /// `retract_from` over the entries in `start..end` only; the rest stay where they are. Indices at or above `end`
    /// shift down by what was dropped, so a caller retracting two ranges takes the upper one first.
    pub fn retract_range(&mut self, start: usize, end: usize, taken: impl Fn(u32) -> bool) {
        let end = end.min(self.entries.len());
        if start >= end {
            return;
        }
        let mut i = 0;
        self.entries.retain(|a| {
            let keep = i < start || i >= end || taken(a.row);
            i += 1;
            keep
        });
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn entries(&self) -> &[Exception] {
        &self.entries
    }

    /// Rows with at least one exception, ascending and distinct: the DLQ candidates.
    pub fn error_rows(&self) -> Vec<u32> {
        let mut v: Vec<u32> = self.entries.iter().filter(|a| a.kind.caught()).map(|a| a.row).collect();
        v.sort_unstable();
        v.dedup();
        v
    }
}

impl Default for ErrorSink {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raise_records_row_and_function() {
        let mut s = ErrorSink::new();
        s.raise(5, 2, None, ExceptionKind::DivideByZero);
        assert_eq!(s.len(), 1);
        assert_eq!(s.entries()[0].row, 5);
        assert_eq!(s.entries()[0].function, 2);
    }

    #[test]
    fn a_try_catches_every_kind_but_encoding_and_delivery() {
        for (kind, caught) in [
            (ExceptionKind::UnparsableField { to: DataType::I64 }, true),
            (ExceptionKind::NaN, true),
            (ExceptionKind::IndexOutOfRange, true),
            (ExceptionKind::InvalidUtf8, false),
            (
                ExceptionKind::GuessedEncoding {
                    encoding: "windows-1252",
                },
                false,
            ),
            (ExceptionKind::Refused, false),
        ] {
            assert_eq!(kind.caught(), caught, "{kind:?}");
        }
    }

    /// `kind` on row 0 writing slot 9, which read `read`, in a message naming slots 1 `a`, 2 `b` and 9 `out`.
    fn message(kind: ExceptionKind, call: &'static str, read: Vec<Read>) -> String {
        let names = HashMap::from([(1, "a".to_string()), (2, "b".to_string()), (9, "out".to_string())]);
        let mut s = ErrorSink::new();
        s.raise(0, 0, Some(9), kind);
        s.fill_from(0, call, |_| read.clone());
        s.entries()[0].message(&names)
    }

    fn field(slot: SlotId, value: &str) -> Read {
        Read::new(Some((slot, String::new())), Some(value.to_string()))
    }

    #[test]
    fn a_message_names_the_source_the_value_what_failed_and_the_slot() {
        let max = field(1, &i64::MAX.to_string());
        let overflow = message(ExceptionKind::Overflow, "+", vec![max, field(2, "1")]);
        assert_eq!(
            overflow,
            "a is 9223372036854775807 and b is 1, whose a + b overflowed i64 in out"
        );
        let zero = vec![Read::new(None, Some("10".into())), field(2, "0")];
        assert_eq!(
            message(ExceptionKind::DivideByZero, "/", zero),
            "b is 0, which 10 / b divided by in out"
        );
        // A literal has no name: its value alone.
        let refused = message(
            ExceptionKind::CoercionRefused { to: DataType::I64 },
            "",
            vec![field(3, "\"many\"")],
        );
        assert_eq!(refused, "\"many\", which could not convert to i64 in out");
        // A bracketed key joins its slot with no dot; a read with neither name nor value is `a value`.
        let bracketed = Read::new(Some((1, "[\"a.b\"]".into())), Some("\"x\"".into()));
        let parse = ExceptionKind::UnparsableField { to: DataType::I64 };
        assert_eq!(
            message(parse, "", vec![bracketed]),
            "a[\"a.b\"] is \"x\", which could not parse as i64 in out"
        );
        let unnamed = Read::new(Some((7, String::new())), None);
        assert_eq!(
            message(ExceptionKind::InvalidUtf8, "", vec![unnamed]),
            "a value held bytes that are not UTF-8, replaced with U+FFFD"
        );
        let long = Read::new(None, Some("x".repeat(100)));
        assert_eq!(long.value.unwrap(), format!("{}…", "x".repeat(64)));
    }

    #[test]
    fn error_rows_excludes_note_only_rows_and_dedups() {
        let mut s = ErrorSink::new();
        s.raise(1, 0, None, ExceptionKind::InvalidUtf8);
        s.raise(9, 0, None, ExceptionKind::DivideByZero);
        s.raise(3, 0, None, ExceptionKind::DivideByZero);
        s.raise(9, 1, None, ExceptionKind::DivideByZero);
        assert_eq!(s.error_rows(), vec![3, 9]);
    }

    #[test]
    fn raise_range_covers_every_row() {
        let mut s = ErrorSink::new();
        s.raise_range(0..4, 7, None, ExceptionKind::GuessedEncoding { encoding: "utf-8" });
        let rows: Vec<u32> = s.entries().iter().map(|a| a.row).collect();
        assert_eq!(rows, vec![0, 1, 2, 3]);
    }

    #[test]
    fn retract_from_leaves_entries_before_the_mark_alone() {
        let mut s = ErrorSink::new();
        s.raise(0, 1, None, ExceptionKind::DivideByZero);
        s.raise(1, 1, None, ExceptionKind::DivideByZero);
        let mark = s.len();
        s.raise(0, 2, None, ExceptionKind::DivideByZero);
        s.raise(1, 2, None, ExceptionKind::DivideByZero);
        s.retract_from(mark, |row| row == 1);
        let kept: Vec<(u32, FunctionId)> = s.entries().iter().map(|a| (a.row, a.function)).collect();
        assert_eq!(kept, vec![(0, 1), (1, 1), (1, 2)]);
    }

    #[test]
    fn retract_range_leaves_entries_past_the_end_alone() {
        let mut s = ErrorSink::new();
        s.raise(0, 1, None, ExceptionKind::DivideByZero);
        s.raise(1, 1, None, ExceptionKind::DivideByZero);
        s.raise(0, 2, None, ExceptionKind::DivideByZero);
        s.retract_range(0, 2, |row| row == 1);
        let kept: Vec<(u32, FunctionId)> = s.entries().iter().map(|a| (a.row, a.function)).collect();
        assert_eq!(kept, vec![(1, 1), (0, 2)]);
    }

    // The preview dedups a row's exceptions on `message`. A payload the text drops would fold two facts into one.

    #[test]
    fn message_tells_types_values_and_trip_counts_apart() {
        let refused = |to| message(ExceptionKind::CoercionRefused { to }, "", vec![field(1, "\"x\"")]);
        assert_ne!(refused(DataType::I64), refused(DataType::F64));
        let parsed = |v: &str| {
            message(
                ExceptionKind::UnparsableField { to: DataType::I64 },
                "",
                vec![field(1, v)],
            )
        };
        assert_ne!(parsed("\"x\""), parsed("\"y\""));
        let one = message(ExceptionKind::LoopBudgetExceeded { trips: 1 }, "", vec![]);
        assert_eq!(one, "a while loop hit its time budget after 1 trip");
        assert_ne!(one, message(ExceptionKind::LoopBudgetExceeded { trips: 2 }, "", vec![]));
    }

    #[test]
    fn remap_rows_from_touches_only_the_tail() {
        let mut s = ErrorSink::new();
        s.raise(0, 0, None, ExceptionKind::DivideByZero); // an earlier route's
        s.raise(1, 1, None, ExceptionKind::DivideByZero);
        s.raise(2, 1, None, ExceptionKind::DivideByZero);
        s.remap_rows_from(1, |r| if r == 1 { vec![10, 11] } else { vec![] });
        let rows: Vec<u32> = s.entries().iter().map(|a| a.row).collect();
        assert_eq!(rows, vec![0, 10, 11]);
    }

    /// The GUI's exceptions editor (`gui-frontend/src/exceptions.ts`) is a hand-written copy of `Category::ALL` and
    /// `ExceptionKind::KINDS`; this keeps the two equal, order included, since the editor lists them in that order.
    #[test]
    fn the_gui_lists_the_kinds_and_categories() {
        let source: Vec<&str> = include_str!("../../gui-frontend/src/exceptions.ts").lines().collect();
        let label = |category: Category| Category::ALL.iter().find(|(_, c)| *c == category).unwrap().0;
        let categories = Category::ALL.map(|(label, _)| format!("'{label}'")).join(", ");
        let categories = format!("export const CATEGORIES = [{categories}] as const;");
        assert!(
            source.contains(&categories.as_str()),
            "exceptions.ts must hold the line:\n{categories}"
        );
        let kinds: Vec<_> = ExceptionKind::KINDS
            .iter()
            .map(|(kind, category)| format!("  ['{kind}', '{}'],", label(*category)))
            .collect();
        assert!(
            source.windows(kinds.len()).any(|run| run.iter().eq(&kinds)),
            "exceptions.ts must hold these lines, in order:\n{}",
            kinds.join("\n")
        );
    }
}
