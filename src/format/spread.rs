//! The spread kernel: one walk of the name tree for every source of an object's keys. A source, the JSON scanner or a
//! text parser, hands it one key at a time (`enter`, then the value, then `leave`), and the kernel keeps a cursor on
//! the tree the parse declares. A declared leaf takes the value whole into its builder (`typed::TypedBuilder`). A key
//! no node names goes to the unclaimed fields of the object it belongs to, the deepest object at or above the cursor,
//! as the value's encoded bytes (`core::variant`); a JSON object nothing under it names is encoded straight from the
//! scanner, and no key under it is walked. Each object's unclaimed fields are one variant object per row.
//!
//! A key is one key; under `splitDots` the kernel splits it at its dots into a path, once, as it enters it.
//!
//! Keys contradict where a row lays a value at a name twice, or a value at a name and a path under it: the kernel keeps
//! one and marks the row (`KeyContradiction`). A row's unclaimed fields are staged as a small nest of byte runs, so
//! keys of one object that arrive apart (a split key beside a nested object, a repeated key) are one object.

use std::borrow::Cow;
use std::ops::Range;

use crate::core::batch::Batch;
use crate::core::bits;
use crate::core::buffer::{BufferPool, check};
use crate::core::column::{Column, DataType, StrBuilder, VarType};
use crate::core::ctx::Ctx;
use crate::core::error::{ExceptionKind, Read};
use crate::core::id::SlotId;
use crate::core::keys::Keys;
use crate::core::tally::FieldSeen;
use crate::core::variant::{self, Value};
use crate::format::json::MAX_NESTING;
use crate::format::tag::{self, Suggestion, Tag};
use crate::format::typed::{Cell, Slot, TypedBuilder};
use crate::plan::compile::{Output, ParseObject};
use crate::plan::tree;

/// What a source does with the value of the key it entered.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Step {
    /// The value is an object the walk goes into: each of its keys is entered in turn.
    Into,
    /// A declared leaf takes the value whole: `whole` or `text`.
    Whole,
    /// The value goes to unclaimed fields as its encoding: written into `copy`'s buffer, then `copied`.
    Copy,
    /// Nothing keeps the value.
    Skip,
    /// A value that is no object arrived at a declared object's name. Nothing keeps it; `refused` names it.
    Refused,
}

/// A node of the name tree the parse declares, by decoded keys.
#[derive(Default)]
struct Node {
    children: Keys<String, usize>,
    /// The declared field at this name.
    leaf: Option<usize>,
    /// The object at this name that takes the keys under it no node names.
    object: Option<usize>,
    /// The row, plus one, a value last landed at this name, and under it.
    set_row: usize,
    under_row: usize,
}

/// One key of the walk: the node it reached, if the tree holds it, and the object a key under it nothing names goes
/// to, with how many keys of the walk lead to that object.
#[derive(Clone, Copy)]
struct Frame {
    node: Option<usize>,
    home: Option<usize>,
    home_depth: usize,
    /// The value is copied only for a preview's tally: its object is closed and drops it.
    tally_only: bool,
}

/// An object the parse lays keys into.
struct Unclaimed<'d> {
    out: SlotId,
    closed: bool,
    /// Its unclaimed fields before the parse, which this row's keys are laid over.
    prior: Option<&'d Column>,
    /// This row's keys, the prior's among them once a key lands here.
    stage: Stage<'d>,
    seeded: bool,
    builder: StrBuilder,
}

/// The keys a parse laid into the unclaimed fields, as the record spelled them, in the order the rows introduced them.
#[derive(Default)]
struct Discovered {
    seen: Keys<String, Suggestion>,
}

impl Discovered {
    fn saw(&mut self, name: String, tag: &Tag<'_>) {
        let i = match self.seen.find(&name) {
            Some(i) => i,
            None => {
                check();
                self.seen.push(name, Suggestion::default())
            }
        };
        self.seen.at_mut(i).expect("a found key is held").1.saw(tag);
    }

    /// Each leaf of the encoded value `b`, at `name` and the names under it.
    fn saw_all(&mut self, name: String, b: &[u8]) {
        match variant::read(b) {
            Value::Object(entries) => {
                for (k, v) in entries {
                    self.saw_all(tree::child(&name, k), v);
                }
            }
            _ => {
                if let Some(tag) = tag::decode(b) {
                    self.saw(name, &tag);
                }
            }
        }
    }
}

/// One batch's walk. `'d` is the batch's lifetime: a staged key or value may borrow straight from a row's bytes or
/// from an object's prior unclaimed fields, so `collapse` must run before that borrow ends.
pub struct Spread<'d> {
    nodes: Vec<Node>,
    fields: Vec<(SlotId, DataType, String, TypedBuilder)>,
    objects: Vec<Unclaimed<'d>>,
    split_dots: bool,
    rows: usize,
    /// The rows a guard selected, which are the rows the parse read: what a tally counts its fields against.
    read: usize,
    /// The row being walked.
    row: usize,
    keys: Vec<Cow<'d, str>>,
    /// One per key in `keys`, after the root's.
    frames: Vec<Frame>,
    /// How many keys each `enter` pushed: more than one where it split a key.
    groups: Vec<usize>,
    /// This row's copied values; a staged value is a range of it.
    arena: Vec<u8>,
    copy_start: usize,
    /// Scratch for encoding one row's unclaimed fields.
    out: Vec<u8>,
    /// Rows where a value that is no object arrived at an object's name, with that object's node, the key and the
    /// value. Marked at `collapse`.
    not_objects: Vec<(usize, usize, String, Option<String>)>,
    /// Rows whose keys contradict, with the later key of each pair. Marked at `collapse`.
    contradictions: Vec<(usize, String)>,
    /// Kept only for a preview.
    discovered: Option<Discovered>,
    /// Whether a value its field's type does not take is a refused conversion, as a variant's is, rather than a
    /// parse's exception.
    converts: bool,
    /// What the row just scanned lost, which `parse_batch` marks.
    pub lost: Lost,
}

/// What a row's read lost. Each is a warning on the row.
#[derive(Copy, Clone, Default, Debug, PartialEq)]
pub struct Lost {
    /// Text that names no character became U+FFFD.
    pub replaced: bool,
    /// A number an f64 cannot hold became no value.
    pub non_finite: bool,
}

impl<'d> Spread<'d> {
    /// `objects` take the keys `fields` does not name; each one's `under` is read from `batch`. Where `ctx` tallies
    /// fields, for a preview, it also keeps what each unclaimed key held.
    pub fn new(
        rows: usize,
        fields: &[Output],
        objects: &[ParseObject],
        split_dots: bool,
        batch: &'d Batch,
        ctx: &mut Ctx,
    ) -> Self {
        let mut s = Spread {
            nodes: vec![Node::default()],
            fields: Vec::with_capacity(fields.len()),
            objects: Vec::with_capacity(objects.len()),
            split_dots,
            rows,
            read: rows,
            row: 0,
            keys: Vec::new(),
            frames: Vec::new(),
            groups: Vec::new(),
            arena: Vec::new(),
            copy_start: 0,
            out: Vec::new(),
            not_objects: Vec::new(),
            contradictions: Vec::new(),
            discovered: ctx.fields.is_some().then(Discovered::default),
            converts: false,
            lost: Lost::default(),
        };
        for (i, f) in fields.iter().enumerate() {
            let n = s.node_at(&f.name);
            s.nodes[n].leaf = Some(i);
            let builder = TypedBuilder::new(&f.ty, f.time_source.as_ref(), ctx.now_micros);
            s.fields.push((f.slot, f.ty.clone(), f.name.clone(), builder));
        }
        for (i, o) in objects.iter().enumerate() {
            let n = s.node_at(&o.name);
            s.nodes[n].object = Some(i);
            let prior = o.under.map(|u| batch.peek(u)).filter(|c| c.variant().is_ok());
            s.objects.push(Unclaimed {
                out: o.out,
                closed: o.closed,
                prior,
                stage: Stage::default(),
                seeded: false,
                builder: StrBuilder::new(ctx.pool, VarType::Variant, rows),
            });
        }
        s.reset();
        s
    }

    /// The node at `name`, made where the tree does not hold it yet.
    fn node_at(&mut self, name: &str) -> usize {
        let mut n = 0;
        if name.is_empty() {
            return n;
        }
        for key in tree::segments(name) {
            n = match self.child(n, &key) {
                Some(c) => c,
                None => {
                    let c = self.nodes.len();
                    self.nodes.push(Node::default());
                    self.nodes[n].children.push(key.into_owned(), c);
                    c
                }
            };
        }
        n
    }

    fn child(&self, n: usize, key: &str) -> Option<usize> {
        self.nodes[n].children.get(key).copied()
    }

    /// The walk at the root, with no key entered.
    fn reset(&mut self) {
        self.keys.clear();
        self.groups.clear();
        self.frames.clear();
        let home = self.nodes[0].object;
        self.frames.push(Frame {
            node: Some(0),
            home,
            home_depth: 0,
            tally_only: false,
        });
    }

    /// Start walking row `row`, a row after the last one walked. A source calls it before the row's first key.
    pub fn row(&mut self, row: usize, pool: &mut BufferPool) {
        while self.row < row {
            self.flush(pool);
            self.row += 1;
            check();
        }
        self.reset();
    }

    /// Enter `key` of the object the walk is at. `object` says the value is an object, which the walk can go into.
    /// Each `enter` is followed by the value the step asks for and then `leave`.
    pub fn enter(&mut self, key: Cow<'d, str>, object: bool) -> Step {
        let mut n = 1;
        if self.split_dots && key.contains('.') {
            n = 0;
            match key {
                Cow::Borrowed(k) => k.split('.').for_each(|p| {
                    self.push(Cow::Borrowed(p));
                    n += 1;
                }),
                Cow::Owned(k) => k.split('.').for_each(|p| {
                    self.push(Cow::Owned(p.to_string()));
                    n += 1;
                }),
            }
        } else {
            self.push(key);
        }
        self.groups.push(n);
        self.decide(object)
    }

    /// Leave the key `enter` entered last.
    pub fn leave(&mut self) {
        let n = self.groups.pop().expect("a key was entered");
        self.keys.truncate(self.keys.len() - n);
        self.frames.truncate(self.frames.len() - n);
    }

    fn push(&mut self, key: Cow<'d, str>) {
        let parent = *self.frames.last().expect("the root's frame");
        let depth = self.keys.len();
        // Past the nesting bound the rest of a name is one key, which no node names.
        let node = match depth + 1 < MAX_NESTING {
            true => parent.node.and_then(|n| self.child(n, &key)),
            false => None,
        };
        let (home, home_depth) = match node.and_then(|n| self.nodes[n].object) {
            Some(o) => (Some(o), depth + 1),
            None => (parent.home, parent.home_depth),
        };
        self.frames.push(Frame {
            node,
            home,
            home_depth,
            tally_only: parent.tally_only,
        });
        self.keys.push(key);
        check();
    }

    fn decide(&mut self, object: bool) -> Step {
        let f = *self.frames.last().expect("a key was entered");
        if let Some(n) = f.node {
            let node = &self.nodes[n];
            if node.leaf.is_some() {
                return Step::Whole;
            }
            if object {
                return Step::Into;
            }
            if node.object.is_some() {
                if self.stamp() {
                    self.contradicted();
                }
                self.not_objects.push((self.row, n, spelled(&self.keys), None));
                return Step::Refused;
            }
        }
        let Some(home) = f.home else {
            return Step::Skip;
        };
        if self.objects[home].closed && !f.tally_only {
            if self.discovered.is_none() {
                return Step::Skip;
            }
            self.frames.last_mut().expect("a key was entered").tally_only = true;
        }
        if object && !f.tally_only && !self.objects[home].closed && self.holds(home, f.home_depth) {
            return Step::Into;
        }
        Step::Copy
    }

    /// Where a declared leaf's value is written, whole.
    pub fn whole<'s>(&'s mut self, pool: &'s mut BufferPool) -> Slot<'s> {
        let i = self.stamped();
        Slot::new(&mut self.fields[i].3, self.row, pool)
    }

    /// Write a declared leaf's scalar value.
    pub fn whole_scalar(&mut self, tag: &Tag<'_>, pool: &mut BufferPool) {
        let i = self.stamped();
        self.fields[i].3.set(self.row, Cell::Tag(tag), pool);
    }

    /// The declared leaf at the walk's key, stamped as written on this row.
    fn stamped(&mut self) -> usize {
        let i = self.leaf();
        if self.stamp() {
            self.contradicted();
        }
        i
    }

    /// A key repeats at `path` inside the value at the walk's key: a contradiction, where the parse keeps that key.
    pub fn repeated(&mut self, path: &[Cow<'_, str>]) {
        let f = *self.frames.last().expect("a key was entered");
        let leaf = f.node.and_then(|n| self.nodes[n].leaf);
        if f.tally_only || leaf.is_some_and(|i| !keeps(&self.fields[i].1, path)) {
            return;
        }
        let mut name = spelled(&self.keys);
        path.iter().for_each(|k| tree::push_key(&mut name, k));
        self.contradictions.push((self.row, name));
        check();
    }

    /// A declared leaf's value, as text: straight into its builder.
    pub fn text(&mut self, text: &str, quoted: bool, pool: &mut BufferPool) {
        let i = self.leaf();
        if self.stamp() {
            self.contradicted();
        }
        self.fields[i].3.set(self.row, Cell::Text { text, quoted }, pool);
    }

    fn leaf(&self) -> usize {
        let n = self.frames.last().and_then(|f| f.node).expect("a leaf was entered");
        self.nodes[n].leaf.expect("a leaf was entered")
    }

    /// Stage text for field `index` of the `fields` `new` was given, on the row being walked: for a parser whose
    /// names its configuration fixes, which resolves each once.
    pub fn set_text_at(&mut self, index: usize, text: &str, quoted: bool, pool: &mut BufferPool) {
        self.fields[index].3.set(self.row, Cell::Text { text, quoted }, pool);
    }

    /// The buffer a `Copy` value's encoding is written into, how many keys deep it sits (an object the value holds
    /// has its keys at that depth), and whether a key under it splits at its dots.
    pub fn copy(&mut self) -> (&mut Vec<u8>, usize, bool) {
        self.copy_start = self.arena.len();
        (&mut self.arena, self.keys.len(), self.split_dots)
    }

    /// The value written since `copy` is not kept: it was null, or the source walks it instead.
    pub fn uncopy(&mut self) {
        self.arena.truncate(self.copy_start);
    }

    /// Lay the value written since `copy`.
    pub fn copied(&mut self) {
        self.lay(None);
    }

    /// A `Copy` value that is text.
    pub fn copy_text(&mut self, text: &str, quoted: bool) {
        let guess = self.discovered.is_some().then(|| text_tag(text, quoted));
        variant::put_str(self.copy().0, text);
        self.lay(guess);
    }

    /// Lay the value written since `copy` into its object's unclaimed fields. A preview tallies `guess`, else what the
    /// value holds.
    fn lay(&mut self, guess: Option<Tag<'_>>) {
        let range = self.copy_start..self.arena.len();
        let f = *self.frames.last().expect("a key was entered");
        if let Some(discovered) = &mut self.discovered {
            let name = spelled(&self.keys);
            match &guess {
                Some(g) => discovered.saw(name, g),
                None => discovered.saw_all(name, &self.arena[range.clone()]),
            }
        }
        if f.tally_only {
            self.arena.truncate(range.start);
            return;
        }
        let home = f.home.expect("a copied value has an object");
        let mut hit = self.stamp();
        self.seed(home);
        let path = stage_path(&self.keys, f.home_depth);
        hit |= self.objects[home].stage.insert(&path, Bytes::Arena(range), &self.arena);
        if hit {
            self.contradicted();
        }
    }

    /// The type the tree declares at node `n`.
    fn type_at(&self, n: usize) -> DataType {
        let node = &self.nodes[n];
        if let Some(l) = node.leaf {
            return self.fields[l].1.clone();
        }
        let fields = node
            .children
            .iter()
            .map(|(k, c)| (k.clone(), self.type_at(*c)))
            .collect();
        DataType::Struct(fields, node.object.is_some_and(|o| !self.objects[o].closed))
    }

    /// The type of the object a source's row is spread into: the first that holds more than one name.
    fn top_type(&self) -> DataType {
        let mut n = 0;
        while let Some(&(_, c)) = self.nodes[n].children.iter().next()
            && self.nodes[n].children.len() == 1
            && self.nodes[n].object.is_none()
        {
            n = c;
        }
        self.type_at(n)
    }

    /// Walk `entries`, a variant object's, as the row's keys.
    fn walk(&mut self, entries: variant::Entries<'d>, pool: &mut BufferPool) {
        for (k, v) in entries {
            match self.enter(Cow::Borrowed(k), variant::is_object(v)) {
                Step::Into => {
                    if let Value::Object(inner) = variant::read(v) {
                        self.walk(inner, pool);
                    }
                }
                Step::Whole => tag::emit(v, &mut self.whole(pool)),
                Step::Copy => {
                    self.copy().0.extend_from_slice(v);
                    self.copied();
                }
                Step::Refused => self.refused(|out| out.extend_from_slice(v)),
                Step::Skip => {}
            }
            self.leave();
        }
    }

    /// Note a value landing at the walk's key: under every name above it, and at the key's own node. Whether that
    /// contradicts what the row laid there before.
    fn stamp(&mut self) -> bool {
        let r = self.row + 1;
        let mut hit = false;
        let top = self.frames.len() - 1;
        for f in &self.frames[1..top] {
            if let Some(n) = f.node {
                let node = &mut self.nodes[n];
                hit |= node.set_row == r;
                node.under_row = r;
            }
        }
        if let Some(n) = self.frames[top].node {
            let node = &mut self.nodes[n];
            hit |= node.set_row == r || node.under_row == r;
            node.set_row = r;
        }
        hit
    }

    /// The value of the key `enter` just refused, which `encode` writes encoded.
    pub fn refused(&mut self, encode: impl FnOnce(&mut Vec<u8>)) {
        let mut bytes = Vec::new();
        encode(&mut bytes);
        self.not_objects.last_mut().expect("a refused key").3 = Some(tag::json_text(&bytes));
    }

    fn contradicted(&mut self) {
        self.contradictions.push((self.row, spelled(&self.keys)));
        check();
    }

    /// Whether the unclaimed fields of `home` hold a value at the walk's key.
    fn holds(&mut self, home: usize, home_depth: usize) -> bool {
        self.seed(home);
        let path = stage_path(&self.keys, home_depth);
        self.objects[home].stage.holds(&path, &self.arena)
    }

    /// Lay the prior unclaimed fields of `home`, on this row, beneath the keys the row lays there.
    fn seed(&mut self, home: usize) {
        let o = &mut self.objects[home];
        if o.seeded {
            return;
        }
        o.seeded = true;
        if let Some(prior) = o.prior
            && let Ok(v) = prior.variant()
            && v.validity.is_valid(self.row)
            && let Value::Object(entries) = variant::read(v.get(self.row))
        {
            for (k, b) in entries {
                o.stage.push(Cow::Borrowed(k), false, Held::Bytes(Bytes::Ref(b)));
            }
        }
    }

    /// Write the row's unclaimed fields, one object per row, and clear them for the next.
    fn flush(&mut self, pool: &mut BufferPool) {
        for o in &mut self.objects {
            if o.seeded && o.stage.keys.len() > 0 {
                self.out.clear();
                if o.stage.write(&self.arena, &mut self.out) > 0 {
                    o.builder.push_grow(pool, &self.out);
                } else {
                    o.builder.push_null();
                }
            } else {
                match o.prior.and_then(|p| p.variant().ok()) {
                    Some(v) if v.validity.is_valid(self.row) => o.builder.push_grow(pool, v.get(self.row)),
                    _ => o.builder.push_null(),
                }
            }
            o.stage.clear();
            o.seeded = false;
        }
        self.arena.clear();
    }

    /// Every staged field as its final `Column`, with its slot, marking each declared-type exception as a
    /// `UnparsableField` on that row, and each row whose keys contradict as a `KeyContradiction`. Each object's
    /// unclaimed fields are last: one object per row of the keys laid there, null where a row has none.
    /// Each exception names what it read under `input`.
    pub fn collapse(mut self, ctx: &mut Ctx, input: SlotId) -> Vec<(SlotId, Column)> {
        while self.row < self.rows {
            self.flush(ctx.pool);
            self.row += 1;
        }
        for (row, key) in std::mem::take(&mut self.contradictions) {
            let read = vec![Read::new(Some((input, key.clone())), None)];
            ctx.errors
                .raise(row, ctx.function, None, ExceptionKind::KeyContradiction { key })
                .read = read;
        }
        for (row, n, key, value) in std::mem::take(&mut self.not_objects) {
            let field = self.objects[self.nodes[n].object.expect("an object's node")].out;
            let kind = match self.converts {
                true => ExceptionKind::CoercionRefused { to: self.type_at(n) },
                false => ExceptionKind::UnparsableField { to: self.type_at(n) },
            };
            ctx.raise_at(row, field, kind).read = vec![Read::new(Some((input, key)), value)];
        }
        if let Some(tally) = ctx.fields.as_deref_mut() {
            tally.parsed(ctx.function, self.read as u32);
        }
        let (rows, read) = (self.rows, self.read as u32);
        let mut out: Vec<(SlotId, Column)> = Vec::with_capacity(self.fields.len() + self.objects.len());
        for (slot, pinned, name, stage) in self.fields {
            let counts = stage.counts();
            let (col, failed) = stage.finish(rows, ctx.pool);
            let n = failed.len() as u32;
            for (row, value) in failed {
                let kind = match self.converts {
                    true => ExceptionKind::CoercionRefused { to: pinned.clone() },
                    false => ExceptionKind::UnparsableField { to: pinned.clone() },
                };
                ctx.raise_at(row, slot, kind).read = vec![Read::new(Some((input, name.clone())), value)];
            }
            if let Some(tally) = ctx.fields.as_deref_mut() {
                tally.saw(FieldSeen {
                    name,
                    declared: true,
                    suggested: pinned,
                    rows: read,
                    counts,
                    failed: n,
                });
            }
            out.push((slot, col));
        }
        if let (Some(discovered), Some(tally)) = (self.discovered, ctx.fields.as_deref_mut()) {
            for (name, s) in discovered.seen {
                tally.saw(FieldSeen {
                    name,
                    declared: false,
                    suggested: s.suggest(),
                    rows: read,
                    counts: s.counts(),
                    failed: 0,
                });
            }
        }
        for o in self.objects {
            out.push((o.out, o.builder.finish()));
        }
        out
    }
}

/// Whether each row is one `guard` selects; every row without one. A parser visits only these: a row it skips lays
/// nothing, so its unclaimed fields stay as they were and its fields are null, which the guard's blend discards.
pub fn selected(batch: &Batch, guard: Option<SlotId>) -> Vec<bool> {
    let rows = batch.rows();
    match guard.map(|g| batch.peek(g).bool().expect("a guard is Bool")) {
        Some(live) => (0..rows)
            .map(|r| live.validity.is_valid(r) && bits::get_row(live.values, r))
            .collect(),
        None => vec![true; rows],
    }
}

/// One format's reading of a row into a `Spread`. A scanner is made once per batch, which is where its format's
/// per-batch setup goes.
pub trait Scanner {
    /// The parser's name, as the pipeline calls it.
    const NAME: &'static str;
    /// What a row's scan reuses from the last row's, borrowing the row's bytes.
    type Scratch<'d>: Default;
    /// Whether a value its field's type does not take is a refused conversion, as a variant's is, rather than a
    /// parse's exception.
    const CONVERTS: bool = false;

    /// Whether a key splits at its dots into a path.
    fn split_dots(&self) -> bool {
        false
    }

    /// Lay row `row` of `input` into `spread`, starting the row with `Spread::row` before its first key. `None` where
    /// the row holds nothing to parse, and otherwise whether it parsed.
    fn scan<'d>(
        &self,
        input: &'d Column,
        row: usize,
        scratch: &mut Self::Scratch<'d>,
        spread: &mut Spread<'d>,
        pool: &mut BufferPool,
    ) -> Option<bool>;
}

/// Scan every row `guard` selects into `outputs` and the unclaimed fields of `objects`. A row that does not parse is
/// marked on `input_slot`: a parse exception or, for a variant, a refused conversion.
pub fn parse_batch<S: Scanner>(
    scanner: S,
    input_slot: SlotId,
    outputs: &[Output],
    objects: &[ParseObject],
    guard: Option<SlotId>,
    batch: &mut Batch,
    ctx: &mut Ctx,
) {
    let rows = batch.rows();
    let live = selected(batch, guard);
    // A scanner reads rows, and a `__source` field every record shares is one value.
    batch.get_mut(input_slot).expand(ctx.pool);
    let collapsed = {
        let mut spread = Spread::new(rows, outputs, objects, scanner.split_dots(), batch, ctx);
        spread.converts = S::CONVERTS;
        spread.read = live.iter().filter(|&&l| l).count();
        let input = batch.peek(input_slot);
        let mut scratch = S::Scratch::default();
        let start = ctx.errors.len();
        for row in (0..rows).filter(|&r| live[r]) {
            if scanner.scan(input, row, &mut scratch, &mut spread, ctx.pool) == Some(false) {
                let kind = match S::CONVERTS {
                    true => ExceptionKind::CoercionRefused { to: spread.top_type() },
                    false => ExceptionKind::Unparsable,
                };
                // The record is spelled only where it did not parse: a repair names its source alone.
                let a = ctx.raise_at(row, input_slot, kind);
                (a.call, a.read) = (S::NAME, vec![Read::of(Some(input_slot), input, row)]);
            }
            let lost = std::mem::take(&mut spread.lost);
            if lost.replaced {
                ctx.raise_at(row, input_slot, ExceptionKind::InvalidUtf8);
            }
            if lost.non_finite {
                ctx.raise_at(row, input_slot, ExceptionKind::NaN);
            }
        }
        ctx.errors.fill_from(start, S::NAME, |_| {
            vec![Read::new(Some((input_slot, String::new())), None)]
        });
        spread.collapse(ctx, input_slot)
    };
    for (slot, col) in collapsed {
        batch.put(slot, col, ctx.pool);
    }
}

/// A parse of `ParserKind::Variant`: each row's variant laid into the outputs and the unclaimed fields as a parse lays
/// its keys. A value that does not fit the type its name declares is a refused conversion, and so is a row that holds
/// a value that is not an object. A null row lays nothing.
pub struct Variant;

impl Scanner for Variant {
    const NAME: &'static str = "";
    type Scratch<'d> = ();
    const CONVERTS: bool = true;

    fn scan<'d>(
        &self,
        input: &'d Column,
        row: usize,
        _: &mut (),
        spread: &mut Spread<'d>,
        pool: &mut BufferPool,
    ) -> Option<bool> {
        let v = input.variant().ok()?;
        match v.validity.is_valid(row).then(|| variant::read(v.get(row)))? {
            Value::Object(entries) => {
                spread.row(row, pool);
                spread.walk(entries, pool);
                Some(true)
            }
            Value::Null => None,
            _ => Some(false),
        }
    }
}

/// `keys` as one name.
fn spelled(keys: &[Cow<'_, str>]) -> String {
    let mut name = String::new();
    for k in keys {
        tree::push_key(&mut name, k);
    }
    name
}

/// The path to the walk's key from the object `home_depth` keys down: a key at each level, the rest of the name one key
/// past the nesting bound.
fn stage_path<'k, 'd>(keys: &'k [Cow<'d, str>], home_depth: usize) -> Cow<'k, [Cow<'d, str>]> {
    let cut = MAX_NESTING - 1;
    if keys.len() <= cut {
        return Cow::Borrowed(&keys[home_depth..]);
    }
    let rest = tree::rest_key(&spelled(&keys[cut..])).into_owned();
    let mut path = keys[home_depth..cut].to_vec();
    path.push(Cow::Owned(rest));
    Cow::Owned(path)
}

/// A staged value's encoding: in the row's arena, or in an object's prior unclaimed fields.
#[derive(Clone)]
enum Bytes<'d> {
    Arena(Range<usize>),
    Ref(&'d [u8]),
}

impl<'d> Bytes<'d> {
    fn get<'a>(&'a self, arena: &'a [u8]) -> &'a [u8] {
        match self {
            Bytes::Arena(r) => &arena[r.clone()],
            Bytes::Ref(b) => b,
        }
    }
}

enum Held<'d> {
    Bytes(Bytes<'d>),
    Object(Stage<'d>),
}

/// One row's keys of an object, laid at their names, in time linear in their number. A key is `fresh` where this row
/// laid it, not the prior; a key written again leaves a hole where it was and lands at the end.
#[derive(Default)]
struct Stage<'d> {
    keys: Keys<Cow<'d, str>, (bool, Held<'d>)>,
}

impl<'d> Stage<'d> {
    /// The object `b` holds, as a level to lay more into; nothing where it holds no object.
    fn of(b: &Bytes<'d>, fresh: bool, arena: &[u8]) -> Stage<'d> {
        let mut stage = Stage::default();
        match b {
            Bytes::Ref(r) => {
                if let Value::Object(entries) = variant::read(r) {
                    for (k, v) in entries {
                        stage.push(Cow::Borrowed(k), fresh, Held::Bytes(Bytes::Ref(v)));
                    }
                }
            }
            Bytes::Arena(range) => {
                if let Value::Object(entries) = variant::read(&arena[range.clone()]) {
                    for (k, v) in entries {
                        let at = v.as_ptr() as usize - arena.as_ptr() as usize;
                        let held = Held::Bytes(Bytes::Arena(at..at + v.len()));
                        stage.push(Cow::Owned(k.to_string()), fresh, held);
                    }
                }
            }
        }
        stage
    }

    fn clear(&mut self) {
        self.keys.clear();
    }

    /// Lay `value` at `path`: inside the object at its first key, made where nothing is there, replacing what is at the
    /// rest. Whether that contradicts what this row laid there: a value at a name this row set, or under a value.
    fn insert(&mut self, path: &[Cow<'d, str>], value: Bytes<'d>, arena: &[u8]) -> bool {
        let (key, rest) = path.split_first().expect("a key");
        let at = self.keys.find(key);
        if rest.is_empty() {
            let hit = at.is_some_and(|i| self.keys.at(i).is_some_and(|(_, (fresh, _))| *fresh));
            if let Some(i) = at {
                self.keys.take(i);
            }
            self.push(key.clone(), true, Held::Bytes(value));
            return hit;
        }
        let (i, hit) = match at {
            None => (self.push(key.clone(), true, Held::Object(Stage::default())), false),
            Some(i) => {
                let (_, (fresh, held)) = self.keys.at_mut(i).expect("a found key is held");
                let mut hit = false;
                if let Held::Bytes(b) = held {
                    hit = *fresh && !variant::is_object(b.get(arena));
                    *held = Held::Object(Stage::of(b, *fresh, arena));
                }
                *fresh = true;
                (i, hit)
            }
        };
        let Some((_, (_, Held::Object(inner)))) = self.keys.at_mut(i) else {
            unreachable!("made an object above")
        };
        inner.insert(rest, value, arena) | hit
    }

    /// Whether a value sits at `path`.
    fn holds(&self, path: &[Cow<'d, str>], arena: &[u8]) -> bool {
        let (key, rest) = path.split_first().expect("a key");
        let Some((_, (_, held))) = self.keys.find(key).and_then(|i| self.keys.at(i)) else {
            return false;
        };
        match held {
            _ if rest.is_empty() => true,
            Held::Object(inner) => inner.holds(rest, arena),
            Held::Bytes(b) => {
                let mut b = b.get(arena);
                for k in rest {
                    match variant::member(b, k) {
                        Some(m) => b = m,
                        None => return false,
                    }
                }
                true
            }
        }
    }

    /// Append this level as an object, leaving out an object under it that holds nothing. How many keys it holds.
    fn write(&self, arena: &[u8], out: &mut Vec<u8>) -> usize {
        let open = variant::begin_object(out);
        let mut count = 0;
        for (key, (_, held)) in self.keys.iter() {
            let mark = out.len();
            variant::put_key(out, key);
            match held {
                Held::Bytes(b) => {
                    out.extend_from_slice(b.get(arena));
                    count += 1;
                }
                Held::Object(inner) if inner.write(arena, out) > 0 => count += 1,
                Held::Object(_) => out.truncate(mark),
            }
        }
        variant::end(out, open, count);
        count
    }

    fn push(&mut self, key: Cow<'d, str>, fresh: bool, held: Held<'d>) -> usize {
        check();
        self.keys.push(key, (fresh, held))
    }
}

/// The `Tag` a preview guesses for one text field: verbatim `Str` if explicitly quoted (quoting is the author's signal
/// "this is text"), else sniffed.
fn text_tag(text: &str, quoted: bool) -> Tag<'_> {
    match quoted {
        true => Tag::Str(Cow::Borrowed(text)),
        false => sniff_scalar(text.as_bytes()),
    }
}

/// Whether a value of type `ty` keeps the key at `path`: a closed object keeps only the keys its fields are at.
fn keeps(mut ty: &DataType, path: &[Cow<'_, str>]) -> bool {
    for key in path {
        while let DataType::Array(t) = ty {
            ty = t;
        }
        match ty {
            DataType::Struct(fields, open) => match fields.iter().find(|(name, _)| name == key) {
                Some((_, t)) => ty = t,
                None => return *open,
            },
            _ => return true,
        }
    }
    true
}

/// This row's raw bytes, or `None` when there is nothing to parse: a null row, a variant row that holds no
/// string, or an input column that is not text-shaped at all.
pub fn row_bytes(col: &Column, row: usize) -> Option<&[u8]> {
    if let Ok(v) = col.variant() {
        return match v.validity.is_valid(row).then(|| variant::read(v.get(row)))? {
            Value::Str(s) => Some(s.as_bytes()),
            Value::Bytes(b) => Some(b),
            _ => None,
        };
    }
    let v = col.var().ok()?;
    if v.validity.is_valid(row) {
        Some(v.get(row))
    } else {
        None
    }
}

// -- sniffing ------------------------------------------------------------------
//
// CSV, KeyValue and Grok have no syntax that types a value; every value is text. A declared field parses as exactly its
// type, by the one table (`ops::convert`); an undeclared one stays text, and a preview sniffs it to suggest a type,
// more strictly (`sniff_scalar`).

/// Type inference for an undeclared textual value. A number is recognized only in JSON's own number grammar (no leading
/// zero, no `+`, no whitespace), so a ZIP code (`00501`) or a version (`1.2.3`) stays text; inference is a guess, and a
/// declaration is not.
pub fn sniff_scalar(text: &[u8]) -> Tag<'_> {
    match text {
        b"true" => return Tag::Bool(true),
        b"false" => return Tag::Bool(false),
        _ => {}
    }
    let Ok(s) = std::str::from_utf8(text) else {
        return Tag::Str(Cow::Owned(String::from_utf8_lossy(text).into_owned()));
    };
    if json_shaped_number(s) {
        if let Ok(i) = s.parse::<i64>() {
            return Tag::I64(i);
        }
        if let Ok(f) = s.parse::<f64>()
            && f.is_finite()
        {
            return Tag::F64(f);
        }
    }
    Tag::Str(Cow::Borrowed(s))
}

/// Whether `s` is exactly a JSON number: optional `-`, no leading zero unless the integer part is `0`, optional
/// `.digits`, optional exponent.
fn json_shaped_number(s: &str) -> bool {
    let b = s.as_bytes();
    let mut i = 0;
    if b.first() == Some(&b'-') {
        i += 1;
    }
    match b.get(i) {
        Some(b'0') => i += 1,
        Some(c) if c.is_ascii_digit() => {
            i += 1;
            while matches!(b.get(i), Some(c) if c.is_ascii_digit()) {
                i += 1;
            }
        }
        _ => return false,
    }
    if b.get(i) == Some(&b'.') {
        i += 1;
        let start = i;
        while matches!(b.get(i), Some(c) if c.is_ascii_digit()) {
            i += 1;
        }
        if i == start {
            return false;
        }
    }
    if matches!(b.get(i), Some(b'e' | b'E')) {
        i += 1;
        if matches!(b.get(i), Some(b'+' | b'-')) {
            i += 1;
        }
        let start = i;
        while matches!(b.get(i), Some(c) if c.is_ascii_digit()) {
            i += 1;
        }
        if i == start {
            return false;
        }
    }
    i == b.len() && !b.is_empty()
}

#[cfg(test)]
mod scalar_tests {
    use super::*;

    #[test]
    fn sniff_scalar_recognizes_bool_and_number() {
        assert!(matches!(sniff_scalar(b"true"), Tag::Bool(true)));
        assert!(matches!(sniff_scalar(b"42"), Tag::I64(42)));
        assert!(matches!(sniff_scalar(b"3.14"), Tag::F64(_)));
        assert!(matches!(sniff_scalar(b"-3"), Tag::I64(-3)));
    }

    #[test]
    fn sniff_scalar_keeps_zip_codes_and_versions_as_text() {
        assert!(matches!(sniff_scalar(b"00501"), Tag::Str(s) if s == "00501"));
        assert!(matches!(sniff_scalar(b"1.2.3"), Tag::Str(s) if s == "1.2.3"));
        assert!(matches!(sniff_scalar(b"hello world"), Tag::Str(s) if s == "hello world"));
    }
}
