//! What a parser's fields looked like in one run, so a preview can propose a declaration from a sample.
//!
//! The counts here are the parser's own, taken from the values as `format::spread::Spread::collapse` staged them, so
//! a proposal is what the parser saw rather than a second reading of the records. An undeclared key goes to the
//! unclaimed fields as it arrived, whatever it held; the proposal is the declaration that would make it a typed field.

use crate::core::column::DataType;
use crate::core::id::FunctionId;

/// What the rows of one field held. Null, missing and never-visited are one
/// thing, and so is a blank under a declared number, bool or datetime: it is not `present`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TagCounts {
    pub present: u32,
    pub bools: u32,
    pub ints: u32,
    pub floats: u32,
    pub strings: u32,
    pub arrays: u32,
    pub objects: u32,
    /// Values of an undeclared field that read as a time (`timeparse::reads_as_time`), counted beside their kind: a
    /// field whose every value did is one to declare a `datetime`. Zero for a declared field.
    pub times: u32,
}

impl TagCounts {
    /// How many kinds appeared. One is a field every row agreed on; more is what widens a suggestion.
    pub fn kinds(&self) -> usize {
        [
            self.bools,
            self.ints,
            self.floats,
            self.strings,
            self.arrays,
            self.objects,
        ]
        .iter()
        .filter(|n| **n > 0)
        .count()
    }
}

/// One field a parser produced, and what the sample said about it.
#[derive(Clone, Debug)]
pub struct FieldSeen {
    /// The path the parser knows it by: dotted for JSON, the key for a key/value record, a header or a position for
    /// CSV, a capture name for grok.
    pub name: String,
    /// Whether a declaration already named it. A declared field's type is the declaration, not a finding.
    pub declared: bool,
    /// A declared field's type, or the declaration an undeclared field's values suggest (`format::tag::Suggestion`).
    pub suggested: DataType,
    /// Rows the parser read, which is `present`'s denominator.
    pub rows: u32,
    pub counts: TagCounts,
    /// Rows whose value would not coerce to the declared type, each of which is also a `UnparsableField`
    /// exception. Zero unless `declared`.
    pub failed: u32,
}

/// One parser's fields, in the order they were bound: declared first, then discovered in the order the rows
/// introduced them.
#[derive(Clone, Debug)]
pub struct ParseSeen {
    /// The pipeline function it ran in. Two parser steps of one pipeline share it, which is why a parse is its own
    /// entry rather than a key into a map.
    pub function: FunctionId,
    /// The rows it read, which a parse that bound no field still read.
    pub rows: u32,
    pub fields: Vec<FieldSeen>,
}

/// Every parse in one run, in the order they ran.
///
/// A run collects one only when something asked for it: the runtime parses every batch and passes none.
#[derive(Debug, Default)]
pub struct FieldTally {
    parses: Vec<ParseSeen>,
}

impl FieldTally {
    pub fn new() -> Self {
        FieldTally::default()
    }

    /// Begin a parse of `rows` rows. Every `saw` until the next `parsed` belongs to it.
    pub fn parsed(&mut self, function: FunctionId, rows: u32) {
        self.parses.push(ParseSeen {
            function,
            rows,
            fields: Vec::new(),
        });
    }

    /// Record a field of the parse in progress. A `saw` before any `parsed` is dropped rather than guessed at.
    pub fn saw(&mut self, seen: FieldSeen) {
        if let Some(current) = self.parses.last_mut() {
            current.fields.push(seen);
        }
    }

    pub fn parses(&self) -> &[ParseSeen] {
        &self.parses
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seen(name: &str) -> FieldSeen {
        FieldSeen {
            name: name.to_string(),
            declared: false,
            suggested: DataType::Str,
            rows: 3,
            counts: TagCounts::default(),
            failed: 0,
        }
    }

    #[test]
    fn kinds_counts_what_appeared_not_how_often() {
        let mut c = TagCounts::default();
        assert_eq!(c.kinds(), 0, "a field no row held agrees about nothing");
        c.ints = 312;
        assert_eq!(c.kinds(), 1);
        c.strings = 188;
        assert_eq!(c.kinds(), 2, "two kinds is what makes a type widen, however lopsided");
    }

    /// Two parser steps in one pipeline are two field sets, and they share a function -- so what separates them is
    /// that each parse says where it starts.
    #[test]
    fn a_tally_keeps_each_parse_apart() {
        let mut tally = FieldTally::new();
        tally.parsed(7, 3);
        tally.saw(seen("order_id"));
        tally.saw(seen("total"));
        tally.parsed(7, 3);
        tally.saw(seen("payload.city"));

        let parses = tally.parses();
        assert_eq!(parses.len(), 2, "one function, two parses");
        let first: Vec<&str> = parses[0].fields.iter().map(|s| s.name.as_str()).collect();
        assert_eq!(first, vec!["order_id", "total"]);
        let second: Vec<&str> = parses[1].fields.iter().map(|s| s.name.as_str()).collect();
        assert_eq!(second, vec!["payload.city"]);
    }

    /// A field with no parse to belong to is dropped: attaching it to the previous parse would put one step's
    /// fields under another's name.
    #[test]
    fn a_field_before_any_parse_is_not_guessed_at() {
        let mut tally = FieldTally::new();
        tally.saw(seen("orphan"));
        assert!(tally.parses().is_empty());
    }
}
