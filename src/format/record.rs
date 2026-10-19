//! The record as one batch holds it: the plan's name tree (`plan::tree`) resolved against the batch's columns once,
//! for every row. Each object has its fields, and where its unclaimed fields come from on a row. Both writers read
//! it: `format::json` writes one document per row, `io::parquet` cuts Arrow columns from it.

use std::borrow::Cow;

use crate::core::column::{Column, DataType, Fixed, VarLen};
use crate::core::id::SlotId;
use crate::core::keys::Keys;
use crate::core::variant;
use crate::format::json::write_json_string;
use crate::plan::tree::{self, Node};

/// The record's tree over one batch. `format::json` writes a row of it as one document; `io::parquet` cuts it into Arrow
/// columns.
pub struct RecordView<'a> {
    pub(crate) root: Object<'a>,
    /// One entry per `Field::Leaf`, indexed by the `usize` it carries.
    pub(crate) leaves: Vec<Leaf<'a>>,
}

/// One object of the tree: its fields, and where its unclaimed fields come from.
pub(crate) struct Object<'a> {
    pub(crate) fields: Vec<(Key, Field<'a>)>,
    pub(crate) unclaimed: Unclaimed<'a>,
    /// Keys of the unclaimed fields that are gone, though their column still holds them (`plan::tree::Object::removed`).
    pub(crate) removed: Vec<String>,
    /// Whether this is the record's own object, whose `__` keys are the engine's and never written.
    pub(crate) hide_internal: bool,
    /// The fields' keys, made at the first `claims`.
    pub(crate) keys: std::cell::OnceCell<Keys<String, ()>>,
}

pub(crate) enum Field<'a> {
    /// The `usize` indexes `RecordView::leaves`. `Column::data_type` allocates for `Array`/`Struct`, and the tree
    /// is walked once per row.
    Leaf(&'a Column, usize),
    Object(Object<'a>),
}

/// Where an object's unclaimed fields come from on a row.
pub(crate) enum Unclaimed<'a> {
    None,
    /// A slot's column: a variant object per row.
    Slot(VarLen<'a>),
    /// The value the parent's unclaimed fields hold at this object's key: an object `with_field` made on the way to
    /// its name, over a key the record may hold already.
    Inherit,
}

/// One leaf of the tree: its type, and its rows viewed once per batch rather than once per cell.
pub(crate) struct Leaf<'a> {
    pub(crate) ty: DataType,
    pub(crate) view: View<'a>,
}

/// A column's rows, resolved when the tree is built. The two common scalar shapes are written from here; anything else
/// goes through `write_value`, which resolves per cell.
pub(crate) enum View<'a> {
    Str(VarLen<'a>),
    I64(Fixed<'a, i64>),
    Other,
}

impl<'a> Leaf<'a> {
    fn of(col: &'a Column) -> Leaf<'a> {
        let ty = col.data_type();
        let view = if col.is_unbound() || col.constant_value().is_some() {
            View::Other
        } else {
            match ty {
                DataType::Str => col.var().map(View::Str).unwrap_or(View::Other),
                DataType::I64 => col.i64().map(View::I64).unwrap_or(View::Other),
                _ => View::Other,
            }
        };
        Leaf { ty, view }
    }
}

impl<'a> Object<'a> {
    fn new(unclaimed: Unclaimed<'a>) -> Object<'a> {
        Object {
            fields: Vec::new(),
            unclaimed,
            removed: Vec::new(),
            hide_internal: false,
            keys: std::cell::OnceCell::new(),
        }
    }

    pub(crate) fn field(&self, key: &str) -> Option<&Field<'a>> {
        self.fields.iter().find(|(k, _)| k.raw == key).map(|(_, f)| f)
    }

    /// Whether a field of this object has `key`, or `key` was taken out of the unclaimed fields: their own key there is
    /// not written. The record's own `__` keys are the engine's, wherever they came from.
    pub(crate) fn claims(&self, key: &str) -> bool {
        if self.removed.iter().any(|k| k == key) || (self.hide_internal && tree::is_internal(key)) {
            return true;
        }
        let keys = self
            .keys
            .get_or_init(|| self.fields.iter().map(|(k, _)| (k.raw.clone(), ())).collect());
        keys.find(key).is_some()
    }

    /// What `own`, this object's unclaimed fields on a row, hold at `key`: the value the object under `key` inherits.
    /// None where a rename took the key away.
    pub(crate) fn member<'r>(&self, own: Option<&'r [u8]>, key: &str) -> Option<&'r [u8]> {
        own.filter(|_| !self.removed.iter().any(|k| k == key))
            .and_then(|v| variant::member(v, key))
    }

    /// The unclaimed fields' value on `row`, where `inherited` is what the parent's hold at this object's key.
    pub(crate) fn unclaimed_at<'r>(&'r self, row: usize, inherited: Option<&'r [u8]>) -> Option<&'r [u8]> {
        match &self.unclaimed {
            Unclaimed::None => None,
            Unclaimed::Slot(v) => v.validity.is_valid(row).then(|| v.get(row)),
            Unclaimed::Inherit => inherited,
        }
    }

    /// Put `col` at the path `segs`, in place of what is there. A new field goes first among its object's fields
    /// where `first`, else last. An object made on the way inherits its key of the parent's unclaimed fields.
    fn put(&mut self, segs: &[&str], col: &'a Column, leaves: &mut Vec<Leaf<'a>>, first: bool) {
        let (head, rest) = segs.split_first().expect("a name has a segment");
        self.keys = std::cell::OnceCell::new();
        let at = match self.fields.iter().position(|(k, _)| k.raw == *head) {
            Some(i) => i,
            None => {
                let at = if first { 0 } else { self.fields.len() };
                let placeholder = Field::Object(Object::new(Unclaimed::Inherit));
                self.fields.insert(at, (Key::new(head), placeholder));
                at
            }
        };
        if rest.is_empty() {
            leaves.push(Leaf::of(col));
            self.fields[at].1 = Field::Leaf(col, leaves.len() - 1);
            return;
        }
        if let Field::Leaf(..) = self.fields[at].1 {
            self.fields[at].1 = Field::Object(Object::new(Unclaimed::Inherit));
        }
        let Field::Object(o) = &mut self.fields[at].1 else {
            unreachable!("made an object above")
        };
        o.put(rest, col, leaves, first);
    }
}

impl<'a> RecordView<'a> {
    /// The record `tree` names, each slot's rows read through `column`. A `__`-prefixed field of the record is a
    /// pipeline-set control variable, kept live for egress to read but not event data, and is not written.
    pub fn new(tree: &tree::Object, column: impl Fn(SlotId) -> &'a Column) -> Self {
        Self::build(tree, &column, false)
    }

    /// Like `new`, but also serializes `__`-prefixed control variables. For the GUI preview only; a real destination
    /// must never see one.
    pub fn new_with_internal(tree: &tree::Object, column: impl Fn(SlotId) -> &'a Column) -> Self {
        Self::build(tree, &column, true)
    }

    /// A record of `fields`, each at its dotted name, its unclaimed fields the one named `tree::UNCLAIMED`, if any.
    pub fn from_named(fields: impl IntoIterator<Item = (&'a str, &'a Column)>) -> Self {
        let mut s = RecordView {
            root: Object::new(Unclaimed::None),
            leaves: Vec::new(),
        };
        for (name, col) in fields {
            if name == tree::UNCLAIMED {
                if let Ok(v) = col.variant() {
                    s.root.unclaimed = Unclaimed::Slot(v);
                }
                continue;
            }
            let segs: Vec<Cow<str>> = tree::segments(name).collect();
            let segs: Vec<&str> = segs.iter().map(AsRef::as_ref).collect();
            s.root.put(&segs, col, &mut s.leaves, false);
        }
        s
    }

    fn build(tree: &tree::Object, column: &dyn Fn(SlotId) -> &'a Column, show_internal: bool) -> Self {
        /// `hide_internal` holds at the record's own level alone.
        fn object<'a>(
            o: &tree::Object,
            column: &dyn Fn(SlotId) -> &'a Column,
            leaves: &mut Vec<Leaf<'a>>,
            hide_internal: bool,
        ) -> Object<'a> {
            let unclaimed = match o.unclaimed.map(|s| column(s).variant()) {
                Some(Ok(v)) => Unclaimed::Slot(v),
                _ => Unclaimed::None,
            };
            let mut out = Object::new(unclaimed);
            out.removed = o.removed.clone();
            out.hide_internal = hide_internal;
            for (key, node) in &o.fields {
                if hide_internal && tree::is_internal(key) {
                    continue;
                }
                let field = match node {
                    Node::Leaf(slot) => {
                        let col = column(*slot);
                        leaves.push(Leaf::of(col));
                        Field::Leaf(col, leaves.len() - 1)
                    }
                    Node::Object(o) => Field::Object(object(o, column, leaves, false)),
                };
                out.fields.push((Key::new(key), field));
            }
            out
        }
        let mut leaves = Vec::new();
        let root = object(tree, column, &mut leaves, !show_internal);
        RecordView { root, leaves }
    }

    /// `self` with `col` written at `name` as well, ahead of the record's own fields at its level: the exception tag.
    /// The name claims its key, and an object made on the way to it holds the record's own key there beneath it.
    pub fn with_field(mut self, name: &str, col: &'a Column) -> Self {
        let segs: Vec<Cow<str>> = tree::segments(name).collect();
        let segs: Vec<&str> = segs.iter().map(AsRef::as_ref).collect();
        self.root.put(&segs, col, &mut self.leaves, true);
        self
    }

    /// What is at `name` on `row`: a field's column, or the value a key of the unclaimed fields holds.
    pub(crate) fn at(&self, name: &str, row: usize) -> Option<At<'_>> {
        let segs: Vec<Cow<str>> = tree::segments(name).collect();
        let segs: Vec<&str> = segs.iter().map(AsRef::as_ref).collect();
        let mut at = &self.root;
        let mut inherited: Option<&[u8]> = None;
        for (i, seg) in segs.iter().enumerate() {
            let own = at.unclaimed_at(row, inherited);
            match at.field(seg) {
                Some(Field::Leaf(col, _)) => return (i + 1 == segs.len()).then_some(At::Leaf(col)),
                Some(Field::Object(o)) => {
                    inherited = at.member(own, seg);
                    at = o;
                }
                // A key a rename took away is gone, though the column still holds it.
                None if at.claims(seg) => return None,
                None => {
                    let mut value = own?;
                    for key in &segs[i..] {
                        value = variant::member(value, key)?;
                    }
                    return Some(At::Unclaimed(value));
                }
            }
        }
        None
    }
}

/// What `RecordView::at` found.
pub(crate) enum At<'r> {
    Leaf(&'r Column),
    Unclaimed(&'r [u8]),
}

// ---------------------------------------------------------------------------
// The tree
// ---------------------------------------------------------------------------

/// One key of the tree, quoted once when the tree is built rather than once per row.
pub(crate) struct Key {
    /// The key as it was given.
    pub(crate) raw: String,
    /// The key as it is written: quoted, escaped, and followed by the colon.
    pub(crate) quoted: Vec<u8>,
}

impl Key {
    fn new(raw: &str) -> Key {
        let mut quoted = Vec::with_capacity(raw.len() + 3);
        write_json_string(raw.as_bytes(), &mut quoted);
        quoted.push(b':');
        Key {
            raw: raw.to_string(),
            quoted,
        }
    }
}
