//! What an object literal lays down, and what an assignment binds, over the name tree (`plan::tree`) as it stands
//! before them. An object literal is one object wherever it is written: bound to a name, it is that name's slots; as a
//! value, `plan::resolve` builds it on each row from the same parts.

use std::borrow::Cow;
use std::collections::BTreeMap;

use crate::core::column::DataType;
use crate::core::id::SlotId;
use crate::expr::ast::{Expr, ExprKind, Span, Value};
use crate::plan::resolve::SymbolTable;
use crate::plan::tree::{self, Node, Object, Place};

/// What a slot of an assignment's value takes.
#[derive(Clone, Debug)]
pub enum Source<'e> {
    /// The value an expression computes.
    Expr(Cow<'e, Expr>),
    /// The value a slot already holds: a copy.
    Slot(SlotId),
    /// An object's unclaimed fields: the slot, less the keys taken out of it (`tree::Object::removed`).
    Unclaimed(SlotId, Vec<String>),
    /// The first where it holds a value, the second elsewhere: a spread's key laid over the leaf already there.
    Over(Box<Source<'e>>, Box<Source<'e>>),
    /// The key of its name in what the source holds on each row, where that is an object: a member of a variant or of
    /// unclaimed fields.
    Member(Box<Source<'e>>, String),
    /// Unclaimed fields laid key by key, first to last, a later key winning. Each layer is a slot of unclaimed
    /// fields, or a variant whose value on a row, where it is an object, lays its keys.
    Layers(Vec<Source<'e>>),
}

/// An object as a literal or an assignment lays it down: each field and what it takes, and the layers of its unclaimed
/// fields.
#[derive(Debug, Default)]
pub struct Draft<'e> {
    pub fields: BTreeMap<String, Part<'e>>,
    pub unclaimed: Vec<Source<'e>>,
    /// Whether a literal builds it: bound, it is open, its unclaimed fields holding no key where nothing lays any.
    pub built: bool,
}

#[derive(Debug)]
pub enum Part<'e> {
    Leaf(Source<'e>),
    Object(Draft<'e>),
}

/// What binding `target` to `value` writes: each slot by its place under `target`, and what it takes. An object
/// literal is a new object, its entries applied in order: a key replaces what is under it, whole, and a spread lays its
/// source over what is there. A name copies: a leaf's slot, or an object's leaves and unclaimed fields. Anything else
/// is one leaf. `Err` says why the value cannot bind.
pub fn writes<'e>(target: &str, value: &'e Expr, syms: &SymbolTable) -> Result<Vec<(Place, Source<'e>)>, String> {
    let mut out = Vec::new();
    // A literal's object takes the shape of a closed one it is written over.
    let closed = |name: &str| syms.object(name).is_some_and(|o| o.unclaimed.is_none());
    flatten(target, part(value, syms)?, &closed, &mut out);
    Ok(out)
}

/// The places `part` lays down at `name`, each with what fills it. An object a literal builds holds no key where
/// nothing lays any, unless it is written over one `closed` names.
pub fn flatten<'e>(name: &str, part: Part<'e>, closed: &impl Fn(&str) -> bool, out: &mut Vec<(Place, Source<'e>)>) {
    match part {
        Part::Leaf(source) => out.push((Place::Leaf(name.to_string()), source)),
        Part::Object(draft) => {
            // An object with no field lays no place: `{}` under a key leaves nothing.
            let built = draft.built && !draft.fields.is_empty() && !closed(name);
            for (key, part) in draft.fields {
                flatten(&tree::child(name, &key), part, closed, out);
            }
            match layered(draft.unclaimed) {
                Some(source) => out.push((Place::Unclaimed(name.to_string()), source)),
                None if built => {
                    let none = Expr::new(Span::default(), ExprKind::Literal(Value::Null));
                    out.push((Place::Unclaimed(name.to_string()), Source::Expr(Cow::Owned(none))));
                }
                None => {}
            }
        }
    }
}

/// What unclaimed fields laid in `layers` take: nothing where none were laid, and the lone layer itself where it is a
/// slot's.
pub fn layered(mut layers: Vec<Source<'_>>) -> Option<Source<'_>> {
    match layers.as_slice() {
        [] => None,
        [Source::Slot(_) | Source::Unclaimed(..)] => layers.pop(),
        _ => Some(Source::Layers(layers)),
    }
}

/// What `value` lays down: an object literal's object, a named object's copy, or one leaf. `Err` says why it cannot.
pub fn part<'e>(value: &'e Expr, syms: &SymbolTable) -> Result<Part<'e>, String> {
    match &value.kind {
        ExprKind::Object(entries) => {
            let mut draft = Draft {
                built: true,
                ..Draft::default()
            };
            lay_entries(&mut draft, entries, syms)?;
            Ok(Part::Object(draft))
        }
        ExprKind::Field(name) => Ok(match syms.named(name).1 {
            Some(Node::Object(o)) => Part::Object(copied(&o)),
            Some(Node::Leaf(slot)) => Part::Leaf(Source::Slot(slot)),
            // A member of a variant, or a name nothing binds, which resolving it reports.
            None => Part::Leaf(Source::Expr(Cow::Borrowed(value))),
        }),
        ExprKind::Call { name, args } if name == "rest" => rest(args, syms),
        _ => Ok(Part::Leaf(Source::Expr(Cow::Borrowed(value)))),
    }
}

/// `rest(o)`: the unclaimed fields of the object `o`, as an open object with no field of its own.
fn rest<'e>(args: &'e [Expr], syms: &SymbolTable) -> Result<Part<'e>, String> {
    let [
        Expr {
            kind: ExprKind::Field(name),
            ..
        },
    ] = args
    else {
        return Err("`rest` takes the name of an object, `rest(o)`".to_string());
    };
    match syms.named(name) {
        (_, Some(Node::Object(o))) => match o.unclaimed {
            Some(u) if !syms.no_keys(&o) => Ok(Part::Object(Draft {
                fields: BTreeMap::new(),
                unclaimed: vec![Source::Unclaimed(u, o.removed.clone())],
                built: false,
            })),
            _ => Err(format!("`{name}` holds no unclaimed fields for `rest` to give")),
        },
        _ => Err(format!("`rest` takes an object, and `{name}` is not one")),
    }
}

/// An object literal's entries, laid in order.
fn lay_entries<'e>(
    draft: &mut Draft<'e>,
    entries: &'e [(Option<String>, Expr)],
    syms: &SymbolTable,
) -> Result<(), String> {
    // A key the literal names twice, itself or through a spread of a literal, is known to conflict before any row
    // runs. A spread of a name may override a key on some rows, which is no conflict.
    let mut seen: Vec<&str> = Vec::new();
    for (key, value) in entries {
        let named = match key {
            Some(k) => vec![k.as_str()],
            None => literal_keys(value),
        };
        for k in named {
            if seen.contains(&k) {
                return Err(format!("object literal has a duplicate key `{k}`"));
            }
            seen.push(k);
        }
        match key {
            Some(k) => draft.put(k, part(value, syms)?)?,
            None => spread(draft, value, syms)?,
        }
    }
    Ok(())
}

/// The keys an object literal lays down, its spreads of literals included. None for anything else.
fn literal_keys(value: &Expr) -> Vec<&str> {
    let ExprKind::Object(fields) = &value.kind else {
        return Vec::new();
    };
    fields
        .iter()
        .flat_map(|(k, v)| match k {
            Some(k) => vec![k.as_str()],
            None => literal_keys(v),
        })
        .collect()
}

/// `...value` laid over `draft`. A literal's entries apply as if written in place. An object lays each leaf over the
/// leaf of its name where it holds a value, each object over the object of its name, and its unclaimed fields over
/// the draft's, a key at a time. A variant does the same with the keys its value has on each row. A name nothing
/// binds is an error, and so is any other value.
fn spread<'e>(draft: &mut Draft<'e>, value: &'e Expr, syms: &SymbolTable) -> Result<(), String> {
    let name = match &value.kind {
        ExprKind::Object(entries) => return lay_entries(draft, entries, syms),
        ExprKind::Field(name) => name,
        _ => {
            return Err(
                "a spread takes a name or an object literal; name the value first, `let v = …; {...v}`".to_string(),
            );
        }
    };
    let slot = match syms.named(name) {
        (name, Some(Node::Object(o))) => return draft.lay_object(name, &o, syms),
        (_, Some(Node::Leaf(slot))) => slot,
        (_, None) => match syms.leaf_above(name) {
            Some((_, slot)) => slot,
            None => return Err(format!("`{name}` is not a field")),
        },
    };
    match syms.type_of(slot) {
        DataType::Variant => {
            draft.lay_variant(Source::Expr(Cow::Borrowed(value)));
            Ok(())
        }
        // Nothing to lay.
        DataType::Null => Ok(()),
        t => Err(format!("a spread takes an object, not {}", t.spell())),
    }
}

/// The object `o`, copied: its leaves' slots, and its unclaimed fields' slot, at every depth.
pub fn copied<'e>(o: &Object) -> Draft<'e> {
    Draft {
        fields: o
            .fields
            .iter()
            .map(|(key, node)| {
                let part = match node {
                    Node::Leaf(slot) => Part::Leaf(Source::Slot(*slot)),
                    Node::Object(o) => Part::Object(copied(o)),
                };
                (key.clone(), part)
            })
            .collect(),
        unclaimed: o
            .unclaimed
            .map(|u| Source::Unclaimed(u, o.removed.clone()))
            .into_iter()
            .collect(),
        built: false,
    }
}

impl<'e> Draft<'e> {
    /// Put `part` at `key`, a path under this object, in place of what was there. An object made on the way keeps the
    /// rest of what the unclaimed fields hold at its key, as a write under a key does (rule 3). A value on the way is
    /// no place to write under, as it is not for a write; one that holds nothing, `null`, gives way.
    fn put(&mut self, key: &str, part: Part<'e>) -> Result<(), String> {
        let segs: Vec<Cow<str>> = tree::segments(key).collect();
        let (last, path) = segs.split_last().expect("a key has a segment");
        let mut at = self;
        let mut above = String::new();
        for seg in path {
            let child = at.child(seg);
            tree::push_key(&mut above, seg);
            let entry = at.fields.entry(seg.to_string()).or_insert(Part::Object(child));
            match entry {
                Part::Leaf(Source::Expr(e)) if matches!(e.kind, ExprKind::Literal(Value::Null)) => {
                    *entry = Part::Object(Draft::default());
                }
                Part::Leaf(_) => {
                    return Err(format!(
                        "`{key}` is a path under `{above}`, which the literal lays as a value, and nothing can be \
                         written under it"
                    ));
                }
                Part::Object(_) => {}
            }
            let Part::Object(d) = entry else {
                unreachable!("made an object above")
            };
            at = d;
        }
        at.fields.insert(last.to_string(), part);
        Ok(())
    }

    /// Lay the object `o`, bound at `name`, over this one, leaf by leaf. Where a leaf meets an object, a null gives way
    /// and a variant leaf lays its keys into the object. Any other value does not meet an object, and neither does an
    /// object laid over a variant, which would be a write under it.
    fn lay_object(&mut self, name: &str, o: &Object, syms: &SymbolTable) -> Result<(), String> {
        for (key, node) in &o.fields {
            let at = tree::child(name, key);
            let laid = match (self.fields.remove(key), node) {
                // An earlier spread's unclaimed fields may hold the key, which the leaf lays over (rule 3).
                (None, Node::Leaf(slot)) => {
                    Part::Leaf(self.unclaimed.iter().rev().fold(Source::Slot(*slot), |top, u| {
                        let key = Source::Member(Box::new(u.clone()), key.clone());
                        Source::Over(Box::new(top), Box::new(key))
                    }))
                }
                (None, Node::Object(o)) => {
                    let mut child = self.child(key);
                    child.lay_object(&at, o, syms)?;
                    Part::Object(child)
                }
                (Some(Part::Leaf(beneath)), Node::Leaf(slot)) => {
                    Part::Leaf(Source::Over(Box::new(Source::Slot(*slot)), Box::new(beneath)))
                }
                (Some(Part::Object(mut draft)), Node::Object(o)) => {
                    draft.lay_object(&at, o, syms)?;
                    Part::Object(draft)
                }
                (Some(Part::Object(mut draft)), Node::Leaf(slot)) => match syms.type_of(*slot) {
                    DataType::Null => Part::Object(draft),
                    DataType::Variant => {
                        let value = Expr::new(Span::default(), ExprKind::Field(at));
                        draft.lay_variant(Source::Expr(Cow::Owned(value)));
                        Part::Object(draft)
                    }
                    t => return Err(unmet(key, &t.spell(), "an object")),
                },
                (Some(Part::Leaf(beneath)), Node::Object(o)) => match &beneath {
                    Source::Slot(s) if syms.type_of(*s) == &DataType::Null => Part::Object(copied(o)),
                    Source::Expr(e) if matches!(e.kind, ExprKind::Literal(Value::Null)) => Part::Object(copied(o)),
                    Source::Slot(s) => {
                        return Err(unmet(key, "an object", &syms.type_of(*s).spell()));
                    }
                    _ => return Err(unmet(key, "an object", "a value")),
                },
            };
            self.fields.insert(key.clone(), laid);
        }
        // `o`'s unclaimed fields may hold a key this object has as a field, which they replace on the rows they do.
        if let Some(u) = o.unclaimed {
            let laid = |k: &String| !o.fields.contains_key(k) && !o.removed.contains(k);
            for (key, part) in self.fields.iter_mut().filter(|(k, _)| laid(k)) {
                part.lay(Source::Member(Box::new(Source::Slot(u)), key.clone()));
            }
            self.unclaimed.push(Source::Unclaimed(u, o.removed.clone()));
        }
        Ok(())
    }

    /// A new object at `key`: what the unclaimed fields laid so far hold there, as its own unclaimed fields.
    fn child(&self, key: &str) -> Draft<'e> {
        Draft {
            fields: BTreeMap::new(),
            unclaimed: self
                .unclaimed
                .iter()
                .map(|u| Source::Member(Box::new(u.clone()), key.to_string()))
                .collect(),
            built: self.built,
        }
    }

    /// Lay `value`, a variant, over this object: each field takes the value's key of its name on the rows the value
    /// holds one, and the value's keys are laid over the unclaimed fields.
    fn lay_variant(&mut self, value: Source<'e>) {
        for (key, part) in self.fields.iter_mut() {
            part.lay(Source::Member(Box::new(value.clone()), key.clone()));
        }
        self.unclaimed.push(value);
    }
}

impl<'e> Part<'e> {
    /// Lay `value`, a variant, over this part: a leaf takes it where it holds a value, and an object its keys.
    fn lay(&mut self, value: Source<'e>) {
        match self {
            Part::Leaf(beneath) => {
                let under = std::mem::replace(beneath, Source::Slot(0));
                *beneath = Source::Over(Box::new(value), Box::new(under));
            }
            Part::Object(draft) => draft.lay_variant(value),
        }
    }
}

/// Why a spread cannot lay `key` as `is` over `was`.
fn unmet(key: &str, is: &str, was: &str) -> String {
    format!("a spread lays `{key}` as {is} over {was}, and the two do not meet")
}

/// Whether `name` is a path under `prefix`: `prefix`, a dot, and more.
pub fn is_under(name: &str, prefix: &str) -> bool {
    name.strip_prefix(prefix)
        .is_some_and(|rest| rest.starts_with('.') || rest.starts_with('['))
}

/// Whether `name` is `prefix` or a path under it.
pub fn at_or_under(name: &str, prefix: &str) -> bool {
    name.strip_prefix(prefix)
        .is_some_and(|rest| rest.is_empty() || rest.starts_with('.') || rest.starts_with('['))
}
