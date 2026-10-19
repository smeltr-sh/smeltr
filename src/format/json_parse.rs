//! A parse of `ParserKind::Json`: bytes in, columns out. Scanning is `json_scan`; the walk of the name tree is
//! `spread`.
//!
//! A key is one key unless the call splits keys at their dots. A declared name receives its value whole, an object or
//! an array as one value, so a mismatch against a declared scalar is a `UnparsableField`. A declared `Struct` is its
//! fields, each a declared name, and an object (`ParseObject`) that takes the keys under it no field takes. A key
//! nothing claims is laid into the unclaimed fields of the deepest object at its name, its value copied whole.

use std::borrow::Cow;

use crate::core::buffer::BufferPool;
use crate::core::column::Column;
use crate::format::json_scan::{self, Sink};
use crate::format::spread::{self, Scanner, Spread, Step};
use crate::format::tag::{self, Tag};
use crate::format::typed::Slot;

/// Adapts `Spread` to `json_scan::Sink` for one row's scan.
struct RowSink<'s, 'd> {
    spread: &'s mut Spread<'d>,
    pool: &'s mut BufferPool,
}

impl<'d> Sink<'d> for RowSink<'_, 'd> {
    fn enter(&mut self, key: Cow<'d, str>, object: bool) -> Step {
        self.spread.enter(key, object)
    }

    type Leaf<'a>
        = Slot<'a>
    where
        Self: 'a;

    fn leaf(&mut self) -> Slot<'_> {
        self.spread.whole(self.pool)
    }

    // Straight into the column: an emitter for one value costs the hottest declared case a tenth of its instructions.
    fn scalar(&mut self, tag: &Tag<'_>) {
        self.spread.whole_scalar(tag, self.pool);
    }

    fn repeated(&mut self, path: &[Cow<'d, str>]) {
        self.spread.repeated(path);
    }

    fn copy(&mut self) -> (&mut Vec<u8>, usize, bool) {
        self.spread.copy()
    }

    fn copied(&mut self) {
        self.spread.copied();
    }

    fn uncopy(&mut self) {
        self.spread.uncopy();
    }

    fn refused(&mut self, tag: &Tag<'_>) {
        self.spread.refused(|out| tag::encode(tag, out));
    }

    fn leave(&mut self) {
        self.spread.leave();
    }
}

/// A parse of `ParserKind::Json`: a row parses where it is an object. Each key splits at its dots into a path where
/// the flag says so.
pub struct Json(pub bool);

impl Scanner for Json {
    const NAME: &'static str = "json";
    type Scratch<'d> = json_scan::Scratch<'d>;

    fn split_dots(&self) -> bool {
        self.0
    }

    fn scan<'d>(
        &self,
        input: &'d Column,
        row: usize,
        scratch: &mut json_scan::Scratch<'d>,
        spread: &mut Spread<'d>,
        pool: &mut BufferPool,
    ) -> Option<bool> {
        let bytes = spread::row_bytes(input, row)?;
        spread.row(row, pool);
        let mut sink = RowSink { spread, pool };
        // False where malformed, or valid JSON whose top level is not an object.
        let parsed = json_scan::parse_object(bytes, scratch, &mut sink);
        spread.lost = scratch.lost;
        Some(parsed)
    }
}
