//! Name resolution and lowering: `Expr` → `Ir`.
//!
//! One pass doing three things at once, which is why it is one pass and not three:
//!
//! 1. `Field(name)` → `SlotId`, via the symbol table.
//! 2. `Regex(PatternSrc)` → `PatternId`, interned into the resource builder.
//! 3. Bottom-up type computation, cached on every `Ir` node.
//!
//! They cannot be cleanly separated because each depends on the last: a field's type comes from its slot, and an
//! operator's type comes from its operands'.

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};

use crate::core::column::DataType;
use crate::core::id::{FunctionId, PatternId, SlotId};
use crate::expr::ast::{Expr, ExprKind, PatternSrc, Site, Span, Value};
use crate::expr::inline::PRELUDE_FUNCTIONS;
use crate::expr::ir::{Ir, IrKind, Layer};
use crate::expr::parse::{STATEMENT_CALLS, index_of};
use crate::ops::objectops::ObjectOp;
use crate::ops::stdfuncs::{Arg, Param, Ret, StdFunc};
use crate::ops::{BinOp, UnOp, table};
use crate::plan::binding::{self, Part, Source};
use crate::plan::config::CompileError;
use crate::plan::parse_call::PARSE_FNS;
use crate::plan::tree::{self, Node, Object, Place};

/// Where a slot came from.
#[derive(Clone, PartialEq, Debug)]
pub struct SlotOrigin {
    pub slot: SlotId,
    pub name: String,
    pub ty: DataType,
    pub origin: Origin,
}

#[derive(Clone, PartialEq, Debug)]
pub enum Origin {
    Ingest,
    Function(FunctionId),
    /// A guard column synthesized for a conditional.
    Guard,
    Temp,
}

/// Interned compile-time constants.
#[derive(Clone, Default, Debug)]
pub struct ResourceBuilder {
    pub patterns: Vec<PatternSrc>,
}

impl ResourceBuilder {
    /// Intern a pattern, deduplicating identical ones.
    ///
    /// The grammar guarantees every regex is a compile-time constant, which is what makes this possible — a pattern
    /// built from a runtime value could not be compiled once and shared across every batch.
    pub fn intern(&mut self, p: &PatternSrc) -> PatternId {
        if let Some(i) = self.patterns.iter().position(|q| q == p) {
            return i;
        }
        self.patterns.push(p.clone());
        self.patterns.len() - 1
    }
}

/// The name tree (`plan::tree`) at one point of the pipeline, and every slot made so far with its type and origin.
/// The heart of resolution.
#[derive(Clone)]
pub struct SymbolTable {
    root: Object,
    pub layout: Vec<DataType>,
    pub origins: Vec<SlotOrigin>,
    pub warnings: Vec<CompileError>,
    /// The slots bound to a local, rather than to the record's field.
    pub local_slots: HashSet<SlotId>,
    /// The slots `delete` took out of the record.
    pub deleted_slots: HashSet<SlotId>,
    /// Each copy that shares a slot rather than fill one: the slot, the name it was put at, and how many slots there
    /// were then, so a later write of that name is told from an earlier one.
    pub aliases: Vec<(SlotId, String, usize)>,
    /// Each open local, innermost last.
    open: Vec<Scope>,
    /// How many locals have been opened: the next one's serial.
    opened: usize,
    /// Slots null on every row: bound to the null literal where nothing, or only null, was before.
    pub nulls: HashSet<SlotId>,
    /// Names whose read is a slot's value where it holds one, else what the tree reads there (`over_plan_output`).
    laid: Vec<(String, SlotId)>,
    /// The tree without the laid names: what a laid name's own read falls back to. A read above one sees it in `root`.
    under: Object,
    /// Nothing may be bound here (`over_plan_output`), so a variant takes a type with `as`, never a declaration.
    reads_only: bool,
}

/// A local's binding, opened before the local is bound and closed when its block ends. Opening hides an object bound
/// at the name outside the block; closing puts back what the name was.
#[derive(Clone, Debug)]
pub struct Scope {
    name: String,
    saved: Option<Node>,
    /// Which local this is: two of one name, nested or one after the other, have two serials.
    serial: usize,
}

impl Scope {
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl SymbolTable {
    /// A table over a *compiled pipeline's output*, for resolving an expression that runs after the pipeline rather
    /// than inside it — a route's `deliver` rule's `when` (design doc, Runtime stages: "Delivery belongs to routes").
    ///
    /// Every name maps to the slot the plan already put it in, so resolution yields `IrKind::Field(slot)` pointing
    /// straight at the finished batch. Nothing is bound here and nothing may be: this table describes a batch that
    /// already exists, so `layout` is the plan's own and allocating into it would name a slot the batch has no column
    /// for. A field reads with the type it had inside the pipeline.
    pub fn over_plan_output(
        layout: &[DataType],
        tree: &Object,
        ingest: &[(&str, DataType)],
        // Names laid over the tree at slots past the plan's own -- the route's tag field, written over whatever the row
        // holds there. A read is the slot's value where it holds one, else the row's own (`SymbolTable::laid`).
        laid: &[(&str, SlotId)],
    ) -> SymbolTable {
        let mut root = tree.clone();
        // The ingest names sit at their fixed slots, the same ones the batch lays them in -- `_raw` is readable from a
        // `when` for the same reason a route condition can read it.
        for (i, (name, _)) in ingest.iter().enumerate() {
            if *name != tree::UNCLAIMED && root.get(name).is_none() {
                root.put(name, Node::Leaf(i));
            }
        }
        let under = root.clone();
        for (name, slot) in laid {
            root.put(name, Node::Leaf(*slot));
        }
        SymbolTable {
            root,
            laid: laid.iter().map(|(name, slot)| (name.to_string(), *slot)).collect(),
            under,
            reads_only: true,
            layout: layout.to_vec(),
            // Origins are logs only (`logs`'s slot listing); a `when` never binds, so nothing here is ever read
            // back.
            origins: layout
                .iter()
                .enumerate()
                .map(|(slot, ty)| SlotOrigin {
                    slot,
                    name: String::new(),
                    ty: ty.clone(),
                    origin: Origin::Ingest,
                })
                .collect(),
            warnings: Vec::new(),
            local_slots: HashSet::new(),
            deleted_slots: HashSet::new(),
            aliases: Vec::new(),
            open: Vec::new(),
            opened: 0,
            nulls: HashSet::new(),
        }
    }

    pub fn new(ingest: &[(&str, DataType)]) -> SymbolTable {
        let mut t = SymbolTable {
            root: Object::default(),
            layout: Vec::new(),
            origins: Vec::new(),
            warnings: Vec::new(),
            local_slots: HashSet::new(),
            deleted_slots: HashSet::new(),
            aliases: Vec::new(),
            open: Vec::new(),
            opened: 0,
            nulls: HashSet::new(),
            laid: Vec::new(),
            under: Object::default(),
            reads_only: false,
        };
        for (name, ty) in ingest {
            let slot = t.layout.len();
            t.layout.push(ty.clone());
            match *name {
                tree::UNCLAIMED => t.root.unclaimed = Some(slot),
                name => {
                    t.root.put(name, Node::Leaf(slot));
                }
            }
            t.origins.push(SlotOrigin {
                slot,
                name: name.to_string(),
                ty: ty.clone(),
                origin: Origin::Ingest,
            });
        }
        t
    }

    /// The record: every name bound right now.
    pub fn root(&self) -> &Object {
        &self.root
    }

    /// The record as `$` names it: the root, less the engine's own fields (`tree::is_internal`) and the open locals.
    pub fn record(&self) -> Object {
        let mut o = self.root.clone();
        o.fields.retain(|k, _| !tree::is_internal(k) && !self.is_local(k));
        o
    }

    /// The slot of the leaf `name`.
    pub fn lookup(&self, name: &str) -> Option<SlotId> {
        self.root.leaf(name)
    }

    /// The slot of the record's own leaf `name`, a top-level name, where a local hides it too.
    pub fn field_lookup(&self, name: &str) -> Option<SlotId> {
        match self.open.iter().find(|s| s.name == name) {
            Some(scope) => match &scope.saved {
                Some(Node::Leaf(s)) => Some(*s),
                _ => None,
            },
            None => self.lookup(name),
        }
    }

    /// The node `name` is: a leaf or an object.
    pub fn node(&self, name: &str) -> Option<&Node> {
        self.root.get(name)
    }

    /// The node `name` names, with the tree's name for it: `$` is the record, which the tree names `""`.
    pub fn named<'n>(&self, name: &'n str) -> (&'n str, Option<Node>) {
        match name {
            tree::ROOT => ("", Some(Node::Object(self.record()))),
            name => (name, self.node(name).cloned()),
        }
    }

    /// The object `name` is. `""` is the record.
    pub fn object(&self, name: &str) -> Option<&Object> {
        self.root.object(name)
    }

    /// The deepest object on `name`'s path, with its name and the rest of `name` below it.
    pub fn deepest<'n>(&self, name: &'n str) -> (&Object, &'n str, &'n str) {
        self.root.deepest(name)
    }

    /// The leaf a path meets on its way to `name`, with its slot.
    pub fn leaf_above<'n>(&self, name: &'n str) -> Option<(&'n str, SlotId)> {
        self.root.leaf_above(name)
    }

    /// The slot at `place`.
    pub fn slot_of(&self, place: &Place) -> Option<SlotId> {
        self.root.slot(place)
    }

    pub fn open_scope(&mut self, name: &str) -> Scope {
        let saved = self.root.get(name).cloned();
        if let Some(Node::Object(_)) = saved {
            self.root.take(name);
        }
        let scope = Scope {
            name: name.to_string(),
            saved,
            serial: self.opened,
        };
        self.opened += 1;
        self.open.push(scope.clone());
        scope
    }

    /// Close `scope`, putting back what its name was: what the scope saved, or what `field_bind` bound there since.
    pub fn close_scope(&mut self, scope: &Scope) {
        let saved = match self.open.iter().rposition(|s| s.name == scope.name) {
            Some(i) => self.open.remove(i).saved,
            None => scope.saved.clone(),
        };
        self.root.take(&scope.name);
        if let Some(node) = saved {
            self.root.put(&scope.name, node);
        }
    }

    /// Whether `name` is an open local's name or a path under it.
    pub fn is_local(&self, name: &str) -> bool {
        self.local_index(name).is_some()
    }

    /// Where the open local `name` names, or is a path under, stands among the open locals: the `local_count` when it
    /// was opened.
    pub fn local_index(&self, name: &str) -> Option<usize> {
        let head = tree::head(name);
        self.open.iter().rposition(|s| s.name == head)
    }

    /// Which binding `name`, or a path under it, means here: the open local's serial, or `None` for the record's field.
    pub fn binding(&self, name: &str) -> Option<usize> {
        self.local_index(name).map(|i| self.open[i].serial)
    }

    /// Note that `slot` is null on every row.
    pub fn note_null(&mut self, slot: SlotId) {
        self.nulls.insert(slot);
    }

    /// Whether `slot` is null on every row.
    pub fn is_null(&self, slot: SlotId) -> bool {
        self.nulls.contains(&slot)
    }

    /// Whether `o` is known to hold no key besides its fields, so a read of another is a compile error.
    pub fn no_keys(&self, o: &Object) -> bool {
        o.unclaimed.is_none_or(|u| self.is_null(u))
    }

    /// Every slot an open local hides, which its block's end puts back.
    pub fn hidden_slots(&self) -> Vec<SlotId> {
        let mut out = Vec::new();
        for scope in &self.open {
            match &scope.saved {
                Some(Node::Leaf(s)) => out.push(*s),
                Some(Node::Object(o)) => out.extend(o.slots()),
                None => {}
            }
        }
        out
    }

    /// The slots of the record's fields an open local hides, less smeltr's own `__` fields. A local that hides a
    /// local outside it hides no field.
    pub fn hidden_field_slots(&self) -> Vec<SlotId> {
        let mut out = Vec::new();
        for (i, scope) in self.open.iter().enumerate() {
            if scope.name.starts_with("__") || self.open[..i].iter().any(|outer| outer.name == scope.name) {
                continue;
            }
            match &scope.saved {
                Some(Node::Leaf(s)) => out.push(*s),
                Some(Node::Object(o)) => out.extend(o.slots()),
                None => {}
            }
        }
        out
    }

    /// How many locals are open.
    pub fn local_count(&self) -> usize {
        self.open.len()
    }

    /// Take `name` out of the tree, with everything under it: a read of it after this is a compile error
    /// (`unresolved`) until a step binds it again.
    pub fn unbind(&mut self, name: &str) -> Option<Node> {
        self.root.take(name)
    }

    /// Take the slot at `place` out of the tree: the leaf, or the object's unclaimed fields.
    pub fn unbind_place(&mut self, place: &Place) {
        match place {
            Place::Leaf(name) => {
                if self.lookup(name).is_some() {
                    self.root.take(name);
                }
            }
            Place::Unclaimed(name) => self.set_unclaimed(name, None),
        }
    }

    /// Bind `place` to a fresh slot of type `ty`: a leaf, or an object's unclaimed fields, which are a variant.
    pub fn bind_place(&mut self, place: &Place, ty: DataType, origin: Origin) -> SlotId {
        match place {
            Place::Leaf(name) => self.bind(name, ty, origin),
            Place::Unclaimed(name) => self.bind_unclaimed(name, origin),
        }
    }

    /// Put the slot `slot` at `place`, in place of whatever was there: a copy, which moves no data.
    pub fn put_place(&mut self, place: &Place, slot: SlotId) {
        match place {
            Place::Leaf(name) => {
                self.root.put(name, Node::Leaf(slot));
            }
            Place::Unclaimed(name) => self.set_unclaimed(name, Some(slot)),
        }
    }

    /// Put `node` at `name`, in place of whatever was there.
    pub fn put(&mut self, name: &str, node: Node) {
        self.root.put(name, node);
    }

    pub fn type_of(&self, slot: SlotId) -> &DataType {
        &self.layout[slot]
    }

    /// Whether `name` is a leaf bound to a variant.
    pub fn is_dynamic(&self, name: &str) -> bool {
        self.lookup(name)
            .is_some_and(|slot| self.layout[slot] == DataType::Variant)
    }

    pub fn slot_count(&self) -> usize {
        self.layout.len()
    }

    /// Every slot the tree names right now, by its place. What `rebound_since` compares against.
    pub fn bindings(&self) -> HashMap<Place, SlotId> {
        self.root.places().into_iter().collect()
    }

    /// Every place bound by an assignment to a slot made since `before` was taken, when `first` was the next slot, with
    /// its slot then and now: what a loop body compiled in between assigned. In the order the places first bound,
    /// which is the order the body assigns them.
    pub fn rebound_since(
        &self,
        before: &HashMap<Place, SlotId>,
        first: SlotId,
    ) -> Vec<(Place, Option<SlotId>, SlotId)> {
        let assigned = |o: &SlotOrigin| matches!(o.origin, Origin::Function(_));
        let mut rebound: Vec<(SlotId, Place, Option<SlotId>, SlotId)> = self
            .bindings()
            .into_iter()
            .filter(|&(_, slot)| slot >= first && assigned(&self.origins[slot]))
            .map(|(place, slot)| {
                let name = place.to_string();
                let first_bound = self.origins[first..]
                    .iter()
                    .find(|o| o.name == name && assigned(o))
                    .map_or(slot, |o| o.slot);
                let then = before.get(&place).copied();
                (first_bound, place, then, slot)
            })
            .collect();
        rebound.sort();
        rebound
            .into_iter()
            .map(|(_, place, then, now)| (place, then, now))
            .collect()
    }

    /// Bind a name to a **fresh** slot.
    ///
    /// SSA: `x = x + 1` does not mutate `x`'s column, it allocates a new one and rebinds the name. The old slot becomes
    /// dead — which is what makes "mutate in place when a slot is provably dead" a *derivable* optimization rather than
    /// a hand-audited one. Whatever was at `name` goes: a leaf replaces an object whole.
    pub fn bind(&mut self, name: &str, ty: DataType, origin: Origin) -> SlotId {
        let slot = self.alloc(name.to_string(), ty, origin);
        if self.is_local(name) {
            self.local_slots.insert(slot);
        }
        self.root.put(name, Node::Leaf(slot));
        slot
    }

    /// Bind the record's own leaf `name`, a top-level name, to a fresh slot, where a local hides it too: the block's
    /// end puts that slot back.
    pub fn field_bind(&mut self, name: &str, ty: DataType, origin: Origin) -> SlotId {
        let Some(i) = self.open.iter().position(|s| s.name == name) else {
            return self.bind(name, ty, origin);
        };
        let slot = self.alloc(name.to_string(), ty, origin);
        self.open[i].saved = Some(Node::Leaf(slot));
        slot
    }

    /// Give the object `name`, made where it is not one yet, a fresh slot for its unclaimed fields.
    pub fn bind_unclaimed(&mut self, name: &str, origin: Origin) -> SlotId {
        let slot = self.alloc(
            Place::Unclaimed(name.to_string()).to_string(),
            DataType::Variant,
            origin,
        );
        self.set_unclaimed(name, Some(slot));
        slot
    }

    /// Make `name` an empty object where nothing is bound there: it claims its key though it holds nothing.
    pub fn make_object(&mut self, name: &str) {
        if !name.is_empty() && self.root.get(name).is_none() {
            self.root.put(name, Node::Object(Object::default()));
        }
    }

    /// Set the slot of the object `name`'s unclaimed fields, making the object where it is not one yet. `None` takes
    /// them away. The new column has no key removed.
    pub fn set_unclaimed(&mut self, name: &str, slot: Option<SlotId>) {
        if self.root.object(name).is_none() {
            if slot.is_none() {
                return;
            }
            self.root.put(name, Node::Object(Object::default()));
        }
        let o = self.root.object_mut(name).expect("made above");
        o.unclaimed = slot;
        o.removed.clear();
    }

    /// Take `key` out of the object `name`'s unclaimed fields, where it has some, without touching their column.
    pub fn remove_key(&mut self, name: &str, key: &str) {
        if let Some(o) = self.root.object_mut(name)
            && o.unclaimed.is_some()
            && !o.removed.iter().any(|k| k == key)
        {
            o.removed.push(key.to_string());
        }
    }

    /// The keys taken out of the object `name`'s unclaimed fields (`tree::Object::removed`).
    pub fn removed(&self, name: &str) -> &[String] {
        self.root.object(name).map_or(&[], |o| &o.removed)
    }

    /// Give the object `name` the unclaimed fields `slot` less `removed`: a copy, which moves no data.
    pub fn put_unclaimed(&mut self, name: &str, slot: SlotId, removed: &[String]) {
        self.set_unclaimed(name, Some(slot));
        self.root.object_mut(name).expect("made above").removed = removed.to_vec();
    }

    /// A fresh slot of type `ty`, named `name` in provenance and bound to nothing: what an instruction writes before
    /// a write binds it.
    pub fn alloc(&mut self, name: String, ty: DataType, origin: Origin) -> SlotId {
        let slot = self.layout.len();
        self.layout.push(ty.clone());
        self.origins.push(SlotOrigin { slot, name, ty, origin });
        slot
    }

    /// An unnamed slot — a guard column, or a blend's output.
    pub fn temp(&mut self, ty: DataType, origin: Origin) -> SlotId {
        let slot = self.layout.len();
        self.alloc(format!("<t{slot}>"), ty, origin)
    }

    /// Rename: pure symbol-table surgery. The node at `from` moves to `to`, whole, and whatever was at `to` goes.
    ///
    /// **Emits no instruction and moves no data.** The columns stay exactly where they are; only the names pointing at
    /// them change.
    pub fn rebind(&mut self, from: &str, to: &str) -> Result<(), CompileError> {
        let Some(node) = self.root.take(from) else {
            return Err(CompileError::new(
                None,
                format!("cannot rename `{from}`: no such field"),
            ));
        };
        self.root.take(to);
        let moved: Vec<(Place, SlotId)> = match &node {
            Node::Leaf(slot) => vec![(Place::Leaf(to.to_string()), *slot)],
            Node::Object(o) => o.places().into_iter().map(|(p, slot)| (p.under(to), slot)).collect(),
        };
        for (place, slot) in moved {
            self.origins[slot].name = place.to_string();
        }
        self.root.put(to, node);
        Ok(())
    }

    /// A read of `name`, which nothing bound: a compile error, wherever it appears. A record key a step reads is a
    /// field its parser declares.
    pub fn unresolved(&self, name: &str) -> CompileError {
        CompileError::new(None, format!("`{name}` is not a field"))
    }

    /// Why a write at `name` is refused, where a leaf stands above it: a path under a variant or a scalar is no place
    /// to write. A leaf that holds nothing, `null`, gives way to the object.
    pub fn write_refused(&self, name: &str) -> Option<CompileError> {
        let (above, slot) = self.leaf_above(name)?;
        let ty = self.type_of(slot);
        let message = match ty {
            DataType::Null => return None,
            DataType::Variant => format!(
                "`{name}` is a path under `{above}`, a variant, and a write under a variant is refused: \
                 `{above}: {{...}} = {above}` makes it an object first"
            ),
            ty => format!("`{above}` is {} and has no fields", ty.spell()),
        };
        Some(CompileError::new(None, message))
    }
}

/// Lower one expression. The node carries the type the checker judged (`expr::ir::Ir::ty`).
pub fn resolve(e: &Expr, syms: &mut SymbolTable, res: &mut ResourceBuilder) -> Result<Ir, CompileError> {
    Ok(match &e.kind {
        ExprKind::Literal(v) => Ir::new(
            v.data_type().unwrap_or(DataType::Null),
            e.span,
            IrKind::Literal(v.clone()),
        ),

        ExprKind::Field(name) => read_ir(name, e.span, syms, res)?,

        ExprKind::Regex(_) => {
            // A regex literal has no value of its own to evaluate — it's meaningful only as `match`/`replace`'s pattern
            // argument, which `resolve_call` intercepts and interns *before* recursing here (see its doc). Reaching
            // this arm means a regex literal turned up somewhere else — `x == /foo/i`, a bare `/foo/` assignment.
            // Reporting it here makes it an ordinary compile error rather than an `expr::eval` panic at runtime.
            return Err(CompileError::new(
                None,
                "a regex literal is only valid as an argument to a regex-consuming \
                 function (match, replace, test, split), not as a value on its own"
                    .to_string(),
            ));
        }

        ExprKind::Coerce { ty, value } => coerce_ir(resolve(value, syms, res)?, ty, e.span)?,

        ExprKind::Check { ty, value, site } => check_ir(resolve(value, syms, res)?, value, ty, site, e.span, syms)?,

        ExprKind::SetElement { array, index, value } => resolve_set_element(array, index, value, e.span, syms, res)?,

        ExprKind::Binary { op, lhs, rhs } => {
            let l = resolve(lhs, syms, res)?;
            let r = resolve(rhs, syms, res)?;

            // `x == null` / `x != null` (either operand order) is `isNull(x)`, negated for `!=`. A comparison already
            // settles a null row this way, but `isNull` also reads a name bound as paths, which is null only where
            // every leaf is (`is_null_ir`).
            //
            // Keyed on the *literal* `null` appearing syntactically on one side, not on either side's checking type
            // being `null`: a comparison of two all-null values is still a comparison. This is also the one
            // test that reads a variant, since it reads presence, not content.
            let lhs_is_null_literal = matches!(lhs.kind, ExprKind::Literal(Value::Null));
            let rhs_is_null_literal = matches!(rhs.kind, ExprKind::Literal(Value::Null));
            if matches!(op, BinOp::Eq | BinOp::Ne) && (lhs_is_null_literal || rhs_is_null_literal) {
                let (operand, operand_expr) = if rhs_is_null_literal { (l, lhs) } else { (r, rhs) };
                let is_null_ir = is_null_ir(operand, operand_expr, e.span, syms);
                if *op == BinOp::Ne {
                    Ir::new(
                        DataType::Bool,
                        e.span,
                        IrKind::Unary {
                            op: UnOp::Not,
                            operand: Box::new(is_null_ir),
                        },
                    )
                } else {
                    is_null_ir
                }
            } else {
                // `??` reads presence, not content, so a variant takes it and its result is a variant.
                if *op != BinOp::Coalesce {
                    refuse_variant(&l, lhs, syms)?;
                    refuse_variant(&r, rhs, syms)?;
                }
                if *op == BinOp::Coalesce {
                    return blend(None, l, r).map(|ir| Ir { span: e.span, ..ir });
                }
                let t = binop_result(*op, &l.ty, &r.ty).map_err(|err| CompileError::new(None, err))?;
                let (lt, rt) = (l.ty.clone(), r.ty.clone());
                let (l, r) = (widen_number(l, &rt, e.span)?, widen_number(r, &lt, e.span)?);
                Ir::new(
                    t,
                    e.span,
                    IrKind::Binary {
                        op: *op,
                        lhs: Box::new(l),
                        rhs: Box::new(r),
                    },
                )
            }
        }

        ExprKind::Unary { op, operand } => {
            let o = resolve(operand, syms, res)?;
            refuse_variant(&o, operand, syms)?;
            let t = unop_result(*op, &o.ty).map_err(|err| CompileError::new(None, err))?;
            Ir::new(
                t,
                e.span,
                IrKind::Unary {
                    op: *op,
                    operand: Box::new(o),
                },
            )
        }

        ExprKind::Ternary { cond, then, else_ } => {
            let c = resolve(cond, syms, res)?;
            refuse_variant(&c, cond, syms)?;
            let (t, f) = (resolve(then, syms, res)?, resolve(else_, syms, res)?);
            ternary_result(&c.ty, &t.ty, &f.ty).map_err(|err| CompileError::new(None, err))?;
            Ir {
                span: e.span,
                ..blend(Some(c), t, f)?
            }
        }

        ExprKind::Call { name, args } => resolve_call(name, args, e.span, syms, res)?,
        ExprKind::Index { base, index } => resolve_element_access(base, index, e.span, syms, res)?,
        ExprKind::Array(items) => resolve_array(items, e.span, syms, res)?,
        ExprKind::Object(_) => resolve_object(e, syms, res)?,
        ExprKind::Arrow(_) => {
            return Err(CompileError::new(
                None,
                "a function is a value only where it is called, passed to a function, or bound with `let`",
            ));
        }
    })
}

/// Whether the language gives `name` a meaning as a call: a statement's, a parser's or a function's. A pipeline's own
/// function needs a name of its own, since there is one namespace.
pub fn is_builtin(name: &str) -> bool {
    STATEMENT_CALLS.contains(&name)
        || PARSE_FNS.contains(&name)
        || PRELUDE_FUNCTIONS.contains(&name)
        || name == "rest"
        || ObjectOp::by_name(name).is_some()
        || StdFunc::by_name(name).is_some()
}

/// Lower `keys`, `entries`, `fromEntries` and `omit`. A value read from a key is one of the types the object's values
/// meet as, and a value in unclaimed fields is one a variant holds.
fn resolve_object_op(
    op: ObjectOp,
    args: &[Expr],
    span: Span,
    syms: &mut SymbolTable,
    res: &mut ResourceBuilder,
) -> Result<Ir, CompileError> {
    let word = op.name().expect("a pipeline calls it by its name");
    let fail = |message: String| Err(CompileError::new(None, message));
    if op == ObjectOp::FromEntries {
        let [pairs] = args else {
            return fail("`fromEntries` takes one array of `{key, value}` objects".to_string());
        };
        let ir = resolve(pairs, syms, res)?;
        refuse_variant(&ir, pairs, syms)?;
        let entry = |e: &DataType| match e {
            DataType::Null => true,
            DataType::Struct(fields, _) => {
                let of = |n: &str| fields.iter().find(|(k, _)| k == n).map(|(_, t)| t);
                matches!(
                    (of("key"), of("value")),
                    (Some(DataType::Str | DataType::Null), Some(_))
                )
            }
            _ => false,
        };
        match &ir.ty {
            DataType::Null => {}
            DataType::Array(e) if entry(e) => {}
            DataType::Array(e) => {
                return fail(format!(
                    "`fromEntries` takes `{{key: str, value}}` objects, found {}",
                    e.spell()
                ));
            }
            t => {
                return fail(format!(
                    "`fromEntries` takes an array of `{{key, value}}` objects, found {}",
                    t.spell()
                ));
            }
        }
        let ty = DataType::Struct(Vec::new(), true);
        return Ok(Ir::new(ty, span, IrKind::ObjectOp { op, args: vec![ir] }));
    }
    let Some((object, keys)) = args.split_first() else {
        return fail(format!("`{word}` takes an object"));
    };
    let ir = resolve(object, syms, res)?;
    refuse_variant(&ir, object, syms)?;
    let DataType::Struct(fields, open) = ir.ty.clone() else {
        return fail(format!("`{word}` takes an object, found {}", ir.ty.spell()));
    };
    match op {
        ObjectOp::Keys | ObjectOp::Entries if !keys.is_empty() => fail(format!("`{word}` takes one object")),
        ObjectOp::Keys => Ok(Ir::new(
            DataType::Array(Box::new(DataType::Str)),
            span,
            IrKind::ObjectOp { op, args: vec![ir] },
        )),
        ObjectOp::Entries => {
            let mut value = if open { DataType::Variant } else { DataType::Null };
            for (n, t) in &fields {
                value = value.unify(t).ok_or_else(|| {
                    CompileError::new(
                        None,
                        format!(
                            "`entries` gives every value as one type, and `{n}`, {}, does not meet the others, {}",
                            t.spell(),
                            value.spell()
                        ),
                    )
                })?;
            }
            let pair = DataType::Struct(
                vec![("key".to_string(), DataType::Str), ("value".to_string(), value)],
                false,
            );
            Ok(Ir::new(
                DataType::Array(Box::new(pair)),
                span,
                IrKind::ObjectOp { op, args: vec![ir] },
            ))
        }
        ObjectOp::Omit => {
            let mut key_irs = Vec::with_capacity(keys.len());
            for k in keys {
                let key = resolve(k, syms, res)?;
                object_key(&key, k, syms)?;
                key_irs.push(key);
            }
            Ok(omit_ir(ir, &key_irs, span))
        }
        ObjectOp::FromEntries | ObjectOp::Get => unreachable!("not called by a name here"),
    }
}

/// `o` less `keys`: `o[k] = null` for each, as `lay_key` writes it.
fn omit_ir(o: Ir, keys: &[Ir], span: Span) -> Ir {
    let null = Ir::new(DataType::Null, span, IrKind::Literal(Value::Null));
    let mut fit = |_: &str, ty: &DataType, _: &Ir| coerce_ir(null.clone(), ty, span);
    keys.iter().fold(o, |o, k| {
        lay_key(o, k, &null, span, &mut fit).expect("null fits every type")
    })
}

/// `o` with `value` laid at `key`, one key, the way `o[key] = value` writes it (Language): the field `key`
/// names takes `fit(name, type, named)`, the value as the field's type where `named` holds, and a null value takes
/// the field out; else the unclaimed fields of `o`, where it is open, take it. A literal key reaches only its own
/// field, and a null value there drops the field from the type. The record's internal fields stay as they are.
pub fn lay_key(
    o: Ir,
    key: &Ir,
    value: &Ir,
    span: Span,
    fit: &mut dyn FnMut(&str, &DataType, &Ir) -> Result<Ir, CompileError>,
) -> Result<Ir, CompileError> {
    let DataType::Struct(_, open) = o.ty.clone() else {
        return Ok(o);
    };
    let literal = match &key.kind {
        IrKind::Literal(Value::Str(k)) => Some(k.as_str()),
        _ => None,
    };
    let takes_out = matches!(value.kind, IrKind::Literal(Value::Null));
    let (fields, unclaimed) = object_parts(o);
    let (mut laid, mut taken) = (Vec::with_capacity(fields.len()), Vec::new());
    for (name, field) in fields {
        if tree::is_internal(&name) || literal.is_some_and(|k| k != name) {
            laid.push((name, field));
            continue;
        }
        if takes_out && literal.is_some() {
            continue;
        }
        let named = key_names(key, &name, span);
        taken.push(named.clone());
        let fitted = fit(&tree::spell(&name), &field.ty, &named)?;
        laid.push((name, choose(&named, fitted, field, span)));
    }
    let unclaimed = unclaimed.map(|u| {
        let unclaimed_key = unclaimed_key(key, taken, span);
        Box::new(Ir::new(
            DataType::Variant,
            span,
            IrKind::Merge(vec![Layer::Spread(u), Layer::Key(unclaimed_key, value.clone())]),
        ))
    });
    let ty = DataType::Struct(laid.iter().map(|(n, ir)| (n.clone(), ir.ty.clone())).collect(), open);
    Ok(Ir::new(
        ty,
        span,
        IrKind::Object {
            fields: laid,
            unclaimed,
        },
    ))
}

/// `key` where no field takes it, `taken` holding where one does: the key the unclaimed fields have, null where a
/// field has it.
fn unclaimed_key(key: &Ir, taken: Vec<Ir>, span: Span) -> Ir {
    if taken.is_empty() {
        return key.clone();
    }
    let null = Ir::new(DataType::Str, span, IrKind::Literal(Value::Null));
    choose(
        &Ir::new(DataType::Bool, span, IrKind::OrN(taken)),
        null,
        key.clone(),
        span,
    )
}

/// `then` where `cond` holds and `else_` elsewhere. Two objects choose leaf by leaf, so each leaf stays its own
/// column; a null `then` blanks each of `else_`'s leaves and its unclaimed fields.
fn choose(cond: &Ir, then: Ir, else_: Ir, span: Span) -> Ir {
    if !matches!(else_.ty, DataType::Struct(..)) {
        let ty = match then.ty {
            DataType::Null => else_.ty.clone(),
            _ => then.ty.clone(),
        };
        let kind = IrKind::Ternary {
            cond: Box::new(cond.clone()),
            then: Box::new(then),
            else_: Box::new(else_),
        };
        return Ir::new(ty, span, kind);
    }
    let null = |ty: &DataType| Ir::new(ty.clone(), span, IrKind::Literal(Value::Null));
    let (mut then_fields, then_unclaimed) = match then.kind {
        IrKind::Literal(Value::Null) => (Vec::new(), None),
        _ => object_parts(then),
    };
    let DataType::Struct(_, open) = else_.ty.clone() else {
        unreachable!("an object, matched above")
    };
    let (fields, unclaimed) = object_parts(else_);
    let fields: Vec<(String, Ir)> = fields
        .into_iter()
        .map(|(n, e)| {
            let t = match then_fields.iter().position(|(m, _)| *m == n) {
                Some(i) => then_fields.swap_remove(i).1,
                None => null(&e.ty),
            };
            let c = choose(cond, t, e, span);
            (n, c)
        })
        .collect();
    let unclaimed = unclaimed.map(|e| {
        let t = then_unclaimed.unwrap_or_else(|| null(&DataType::Variant));
        Box::new(choose(cond, t, e, span))
    });
    let ty = DataType::Struct(fields.iter().map(|(n, ir)| (n.clone(), ir.ty.clone())).collect(), open);
    Ir::new(ty, span, IrKind::Object { fields, unclaimed })
}

/// Lower a `Call`. Both call syntaxes land here as the same node shape — `expr::parse::Parser::postfix` desugars
/// `['x'].length()` into `Call{name: "length", args: [['x']]}` directly, since it can see the `.` — but `x.length()`
/// for a bare identifier chain arrives as `Call{name: "x.length", args: []}` instead, because the lexer's
/// dotted-identifier collapse (`expr::lex`'s doc) already merged `x` and `length` into one token before the parser ever
/// got a `.` to react to. The `_` arm below undoes that: split the collapsed name at its last `.`, and if the tail
/// names a real `StdFunc`, treat the head as the receiver — see `StdFunc::by_name`'s doc for the full picture.
///
/// A regex argument is interned before the general per-argument recursion reaches it: `ExprKind::Regex` resolving to a
/// hard error (see that arm's doc, just above) is what keeps a pattern from ever being a value. Which positions those
/// are comes from the table — a row declares them `Param::Pattern` (`takes_pattern_at`) — so `match`, `replace`, `test`
/// and `split` need no arm of their own here. `split` is the one position that accepts either, and picks by whether the
/// argument is written as a regex literal.
fn resolve_call(
    name: &str,
    args: &[Expr],
    span: Span,
    syms: &mut SymbolTable,
    res: &mut ResourceBuilder,
) -> Result<Ir, CompileError> {
    // A method call is a call of its method with the receiver first: `o.keys()` is `keys(o)`. No function's name
    // holds a dot, so a dotted name is always one.
    let (method, arg_exprs): (&str, Cow<[Expr]>) = match name.rsplit_once('.') {
        Some((recv, method)) => {
            let recv = Expr::new(span, ExprKind::Field(recv.to_string()));
            (method, std::iter::once(recv).chain(args.iter().cloned()).collect())
        }
        None => (name, Cow::Borrowed(args)),
    };
    if method == "isNull" && arg_exprs.len() == 1 {
        let operand = resolve(&arg_exprs[0], syms, res)?;
        return Ok(is_null_ir(operand, &arg_exprs[0], span, syms));
    }
    // `toString` of something already a `Str` is that value. Elided here, where the argument's type is known, rather
    // than handled in the kernel: `Column` is deliberately not `Clone` (it owns pooled buffers), so a kernel could only
    // satisfy this by copying the string data column every batch. Template literals wrap every `${...}` in `toString`
    // so a hole can hold any scalar, which would otherwise make interpolating a plain string the most expensive kind.
    if method == "toString" && arg_exprs.len() == 1 {
        let ir = resolve(&arg_exprs[0], syms, res)?;
        if matches!(ir.ty, DataType::Str) {
            return Ok(ir);
        }
        // Not a string -- fall through to the ordinary path, which re-resolves the argument. Cheap: resolution is
        // compile-time.
    }
    if let Some(op) = ObjectOp::by_name(method) {
        return resolve_object_op(op, &arg_exprs, span, syms, res);
    }
    let Some(func) = StdFunc::by_name(method) else {
        if PARSE_FNS.contains(&name) || name == "rest" {
            return Err(CompileError::new(
                None,
                format!("`{name}(…)` is the whole value of a write: write `x = {name}(…);`, then read `x`"),
            ));
        }
        if PRELUDE_FUNCTIONS.contains(&method) {
            return Err(CompileError::new(
                None,
                format!("`{method}` runs only in a code block, which expands the functions it calls"),
            ));
        }
        if name == "Object.assign" {
            return Err(CompileError::new(
                None,
                "`Object.assign` is a statement that writes its first argument; it has no value to read".to_string(),
            ));
        }
        return Err(CompileError::new(
            None,
            format!("function `{name}` is not implemented (stdlib pending)"),
        ));
    };
    // `x[i]` and `at(x, i)` are the same call, so they are the same resolution: a struct field by literal key and an
    // element of an array of objects are typed from the key, which no table row can express.
    if func == StdFunc::At && arg_exprs.len() == 2 {
        let access = index_of(arg_exprs[0].clone(), arg_exprs[1].clone(), span);
        return resolve(&access, syms, res);
    }
    // A unit is written as a literal in almost every use, so one this table will never convert is a mistake the
    // compiler can name -- rather than a column of nulls that looks like missing data. A unit computed per row is
    // still a runtime null: there is nothing to check before the rows arrive.
    if matches!(func, StdFunc::Duration | StdFunc::CountIn)
        && let Some(ExprKind::Literal(Value::Str(unit))) = arg_exprs.get(1).map(|a| &a.kind)
        && !crate::ops::datetime::is_known_unit(unit)
    {
        return Err(CompileError::new(
            None,
            format!(
                "`{unit}` is not a unit of time: use `ns`, `us`, `ms`, `s`, `m`, `h`, `d` or `w`, or a long name. \
                 A month and a year vary in length, so a span cannot be counted in either."
            ),
        ));
    }
    let mut arg_irs = Vec::with_capacity(arg_exprs.len());
    let mut arg_kinds = Vec::with_capacity(arg_exprs.len());
    for (i, a) in arg_exprs.iter().enumerate() {
        // A position some row declares as a pattern takes a regex literal -- unless another row takes a value there
        // and this argument is not one (`split(s, ",")`), which is the ordinary path.
        let is_regex = matches!(a.kind, ExprKind::Regex(_));
        if func.takes_pattern_at(i) && (is_regex || !func.takes_value_at(i)) {
            arg_irs.push(intern_regex_arg(func.name(), a, res)?);
            arg_kinds.push(Arg::Pattern);
        } else {
            let ir = resolve(a, syms, res)?;
            refuse_variant(&ir, a, syms)?;
            arg_kinds.push(Arg::Ty(ir.ty.clone()));
            arg_irs.push(ir);
        }
    }
    let Some(result_ty) = func.result_type(&arg_kinds) else {
        return Err(CompileError::new(None, call_type_error(func, name, &arg_kinds)));
    };
    // An argument widens to the type the row gives it: the `f64` its parameter names, the type every argument met as
    // one, or the array the push builds.
    if let Some(row) = func.row_for(&arg_kinds) {
        for (i, ir) in arg_irs.iter_mut().enumerate() {
            let param = row.params.get(i).or(row.params.last());
            let to = match (param, &row.ret) {
                (Some(Param::F64), _) => DataType::F64,
                (_, Ret::NumericJoin | Ret::SameArray) => result_ty.clone(),
                (_, Ret::Pushed(_)) if i == 0 => result_ty.clone(),
                (_, Ret::Pushed(first)) if i >= *first => match &result_ty {
                    DataType::Array(elem) => (**elem).clone(),
                    _ => continue,
                },
                _ => continue,
            };
            let taken = std::mem::replace(ir, Ir::new(DataType::Null, span, IrKind::Literal(Value::Null)));
            *ir = widen_to(taken, &to, span)?;
        }
    }
    Ok(Ir::new(result_ty, span, IrKind::Call { func, args: arg_irs }))
}

/// The error a use of a variant gets, naming the value where the expression does.
fn refuse_variant(ir: &Ir, e: &Expr, syms: &SymbolTable) -> Result<(), CompileError> {
    if ir.ty != DataType::Variant {
        return Ok(());
    }
    Err(CompileError::new(
        None,
        match named_expr(e) {
            Some(name) => format!("`{name}` {}", variant_use(Some(&conversion_hint(&name, syms)))),
            None => format!("this value {}", variant_use(None)),
        },
    ))
}

/// The conversion that gives `name`, a variant, a type: `name: type = name;`, or, for a key under a variant, the
/// variant's conversion to an open object holding it. Where nothing may be bound, `name as type`.
pub fn conversion_hint(name: &str, syms: &SymbolTable) -> String {
    if syms.reads_only {
        return format!("`{name} as type`");
    }
    let Some((above, _)) = syms.leaf_above(name) else {
        return format!("`{name}: type = {name};`");
    };
    let keys: Vec<Cow<str>> = tree::segments(tree::key_under(above, name)).collect();
    let ty = keys
        .iter()
        .rev()
        .fold("type".to_string(), |inner, key| format!("{{{key}: {inner}, ...}}"));
    format!("`{above}: {ty} = {above};`")
}

/// The name `e` reads. `None` for any other expression.
fn named_expr(e: &Expr) -> Option<String> {
    match &e.kind {
        ExprKind::Field(name) => Some(name.clone()),
        _ => None,
    }
}

/// How `e` reads in an error: its name where it has one (`named_expr`), a literal as written, a call as `f(...)`,
/// and "the value" for anything else.
pub fn describe_expr(e: &Expr) -> String {
    if let Some(name) = named_expr(e) {
        return name;
    }
    match &e.kind {
        ExprKind::Literal(Value::Str(s)) => format!("{s:?}"),
        ExprKind::Literal(Value::I64(n)) => n.to_string(),
        ExprKind::Literal(Value::F64(x)) => format!("{x:?}"),
        ExprKind::Literal(Value::Bool(b)) => b.to_string(),
        ExprKind::Literal(Value::Null) => "null".to_string(),
        ExprKind::Call { name, args } => {
            let args: Vec<String> = args.iter().map(describe_expr).collect();
            format!("{name}({})", args.join(", "))
        }
        ExprKind::Coerce { ty, value } => format!("{} as {}", describe_expr(value), ty.spell()),
        ExprKind::Array(items) => {
            let items: Vec<String> = items.iter().map(describe_expr).collect();
            format!("[{}]", items.join(", "))
        }
        ExprKind::Object(entries) => {
            let entries: Vec<String> = entries
                .iter()
                .map(|(k, v)| match k {
                    Some(k) => format!("{k}: {}", describe_expr(v)),
                    None => format!("...{}", describe_expr(v)),
                })
                .collect();
            format!("{{{}}}", entries.join(", "))
        }
        _ => "the value".to_string(),
    }
}

/// `ir` coerced to `to` by the coercion table, or `ir` itself where it is that type already. A pair the table has
/// no row for is a compile error.
pub fn coerce_ir(ir: Ir, to: &DataType, span: Span) -> Result<Ir, CompileError> {
    let ty = match &ir.ty {
        from if from == to => return Ok(ir),
        DataType::Null => return Ok(Ir::new(to.clone(), span, IrKind::Literal(Value::Null))),
        DataType::Variant => to.clone(),
        from => {
            if !coercible(from, to) {
                let (from, to) = (from.spell(), to.spell());
                return Err(CompileError::new(None, format!("cannot coerce {from} to {to}")));
            }
            to.clone()
        }
    };
    Ok(Ir::new(
        ty,
        span,
        IrKind::Coerce {
            to: to.clone(),
            value: Box::new(ir),
        },
    ))
}

/// `ir` widened to `ty` where its type meets `ty` as the narrower: the `i64` side of a blend, an `i64` element
/// beside an `f64`, an argument the result widened. A null reads as any type as it is.
pub fn widen_to(ir: Ir, ty: &DataType, span: Span) -> Result<Ir, CompileError> {
    // A blend into a variant keeps each side as it is; the select holds them.
    if ir.ty == *ty || ir.ty == DataType::Null || *ty == DataType::Variant || !fits(&ir.ty, ty) {
        return Ok(ir);
    }
    coerce_ir(ir, ty, span)
}

/// `ir` where it already has `to`, the type declared at `site`: `null` takes the type at any depth, an `i64` widens to
/// `f64`, a variant holds any value, and an object fits an object type that names each
/// of its fields and takes each field's type, a field it lacks being null, where the type is open or the object
/// closed. Any other type is a compile error naming `site`, or the argument's name where it is one.
fn check_ir(ir: Ir, e: &Expr, to: &DataType, site: &Site, span: Span, syms: &SymbolTable) -> Result<Ir, CompileError> {
    let from = match &ir.ty {
        DataType::Variant if matches!(site, Site::Declaration(_)) => return coerce_ir(ir, to, span),
        DataType::Variant => match refuse_variant(&ir, e, syms) {
            Err(err) => return Err(CompileError::new(None, format!("{site}: {}", err.message))),
            Ok(()) => unreachable!("a variant is refused"),
        },
        from if fits(from, to) => return coerce_ir(ir, to, span),
        from => from,
    };
    let named = named_expr(e).filter(|n| !crate::expr::inline::is_temporary(n));
    let (from_s, to_s) = (from.spell(), to.spell());
    let message = match (site, named) {
        (Site::Argument { function, .. }, Some(name)) => {
            // An open object fits a closed type once it drops its other keys.
            let drops =
                matches!(from, DataType::Struct(fields, true) if fits(&DataType::Struct(fields.clone(), false), to));
            let hint = match drops {
                true => format!(". To drop `{name}`'s other keys, write `{function}({name} as {to_s})`"),
                false => String::new(),
            };
            format!("`{name}` is {from_s}, but `{function}` takes {to_s}{hint}")
        }
        (Site::Declaration(name), _) => {
            let shown = describe_expr(e);
            match coercible(from, to) {
                true => format!(
                    "`{shown}` is {from_s}, not {to_s}; to convert the value, write `{name}: {to_s} = {shown} as {to_s}`"
                ),
                false => format!("`{shown}` is {from_s}, not {to_s}"),
            }
        }
        _ => format!("{site} is {from_s}, not {to_s}"),
    };
    Err(CompileError::new(None, message))
}

/// Whether a value of `from` may stand where `to` is declared, by `check_ir`'s rule. A closed value has no unclaimed
/// fields, so it fits an open object, which may have some; an open one does not fit a closed object.
pub fn fits(from: &DataType, to: &DataType) -> bool {
    use DataType::*;
    match (from, to) {
        _ if from == to => true,
        (Null, _) => true,
        // A variant holds any value as it is.
        (_, Variant) => true,
        (I64, F64) => true,
        (Array(f), Array(t)) => fits(f, t),
        (Struct(have, open), Struct(want, wanted)) => {
            (!*open || *wanted)
                && have.iter().all(|(n, f)| match want.iter().find(|(m, _)| m == n) {
                    Some((_, t)) => fits(f, t),
                    None => false,
                })
        }
        _ => false,
    }
}

/// The coercion table's rows (design doc, Language), as a question about types: whether any value of
/// `from` may coerce to `to`. A `null` holds no value to refuse.
pub fn coercible(from: &DataType, to: &DataType) -> bool {
    use DataType::*;
    // A variant is decoded row by row: a row it does not fit is refused, never the pair.
    if matches!(from, Null | Variant) {
        return true;
    }
    match to {
        Null => false,
        Bool => matches!(from, Str | Bool),
        I64 | F64 => matches!(from, Str | I64 | F64),
        Str => true,
        DateTime => matches!(from, Str | I64 | F64 | DateTime),
        Duration => matches!(from, Str | I64 | F64 | Duration),
        Bytes => matches!(from, Str | Bytes),
        // Every value is one; nothing is refused.
        Variant => true,
        Array(t) => matches!(from, Array(f) if coercible(f, t)),
        // The unclaimed fields coerce value by value, each as a variant's would, so they never refuse the pair.
        Struct(fields, _) => match from {
            Struct(have, _) => fields.iter().all(|(name, t)| {
                have.iter()
                    .find(|(n, _)| n == name)
                    .is_none_or(|(_, f)| coercible(f, t))
            }),
            _ => false,
        },
    }
}

/// `array[index] = value`, as the array with that position set. The value meets the element type as a push's does: a
/// `null` array or element takes the value's type, and `i64` beside `f64` widens to `f64`. The index is `i64`.
fn resolve_set_element(
    array: &Expr,
    index: &Expr,
    value: &Expr,
    span: Span,
    syms: &mut SymbolTable,
    res: &mut ResourceBuilder,
) -> Result<Ir, CompileError> {
    let a = resolve(array, syms, res)?;
    refuse_variant(&a, array, syms)?;
    let i = resolve(index, syms, res)?;
    refuse_variant(&i, index, syms)?;
    let v = resolve(value, syms, res)?;
    refuse_variant(&v, value, syms)?;
    let i = match &i.ty {
        DataType::Null | DataType::I64 => i,
        other => {
            return Err(CompileError::new(
                None,
                format!("an index must be i64, found {}", other.spell()),
            ));
        }
    };
    let (held, written) = (&a.ty, &v.ty);
    let ty = held
        .unify(&DataType::Array(Box::new(written.clone())))
        .filter(|t| matches!(t, DataType::Array(_)))
        .ok_or_else(|| {
            let name = named_expr(array).unwrap_or_else(|| "the array".to_string());
            CompileError::new(
                None,
                format!("`{name}[...] = value` writes {} into {}", written.spell(), held.spell()),
            )
        })?;
    let elem = match &ty {
        DataType::Array(elem) => (**elem).clone(),
        _ => unreachable!("an array, filtered above"),
    };
    let a = widen_to(a, &ty, span)?;
    let v = widen_to(v, &elem, span)?;
    Ok(Ir::new(
        ty,
        span,
        IrKind::SetElement {
            array: Box::new(a),
            index: Box::new(i),
            value: Box::new(v),
        },
    ))
}

/// Arity first: a wrong count is the likelier mistake, and a list of types would only obscure it.
fn call_type_error(func: StdFunc, name: &str, args: &[Arg]) -> String {
    let arities = func.arities();
    // A variadic function has a minimum, not a set: `push(xs)` is short, `push(xs, a, b, c)` never is.
    if func.is_variadic() {
        let min = arities.first().copied().unwrap_or(0);
        if args.len() < min {
            return format!("`{name}` takes {min} or more arguments, found {}", args.len());
        }
    } else if !arities.contains(&args.len()) {
        let wanted = match arities.as_slice() {
            [n] => n.to_string(),
            [init @ .., last] => {
                let init: Vec<String> = init.iter().map(usize::to_string).collect();
                format!("{} or {last}", init.join(", "))
            }
            [] => unreachable!("every function has a row"),
        };
        let noun = if arities == [1] { "argument" } else { "arguments" };
        return format!("`{name}` takes {wanted} {noun}, found {}", args.len());
    }
    let found: Vec<String> = args
        .iter()
        .map(|a| match a {
            Arg::Ty(t) => t.spell(),
            Arg::Pattern => "/pattern/".to_string(),
        })
        .collect();
    format!(
        "`{name}` is not defined for ({}); it takes {}",
        found.join(", "),
        func.signatures_text()
    )
}

/// Intern `arg` as `func`'s regex argument — `arg` must *syntactically* be a regex literal (not an expression that
/// merely evaluates to one; there is no such thing, see `ExprKind::Regex`'s doc), matching the design doc's "grammar
/// enforces that patterns are compile-time constants."
fn intern_regex_arg(func: &str, arg: &Expr, res: &mut ResourceBuilder) -> Result<Ir, CompileError> {
    let ExprKind::Regex(p) = &arg.kind else {
        return Err(CompileError::new(
            None,
            format!("`{func}`'s pattern argument must be a literal /regex/, not an expression"),
        ));
    };
    let id = res.intern(p);
    Ok(Ir::new(DataType::Bool, arg.span, IrKind::Regex(id)))
}

/// Lower an array literal `[e1, e2, ...]`. Fixed arity: every row gets exactly `items.len()` elements, one per source
/// expression — there is no way to write a variable-length array literal in the grammar, so `expr::eval::eval_array`
/// can build the child column by interleaving `items.len()` whole-column evaluations rather than needing a general
/// variable-length list builder.
///
/// The items meet as a ternary's branches do: `i64` beside `f64` as `f64`, two objects field by field, `null` as the
/// other's type. An item may itself be an array or an object, so `[[1]]` is `array<array<i64>>`. `[]` has no item to
/// take a type from: it is `array<null>`.
fn resolve_array(
    items: &[Expr],
    span: Span,
    syms: &mut SymbolTable,
    res: &mut ResourceBuilder,
) -> Result<Ir, CompileError> {
    let mut elem_irs = Vec::with_capacity(items.len());
    let mut elem_ty = DataType::Null;
    for it in items {
        let ir = resolve(it, syms, res)?;
        refuse_variant(&ir, it, syms)?;
        elem_ty = unify_element_types(elem_ty, ir.ty.clone(), it.span)?;
        elem_irs.push(ir);
    }
    let elem_irs = elem_irs
        .into_iter()
        .map(|ir| widen_to(ir, &elem_ty, span))
        .collect::<Result<Vec<Ir>, CompileError>>()?;
    let result_ty = DataType::Array(Box::new(elem_ty));
    Ok(Ir::new(result_ty, span, IrKind::Array(elem_irs)))
}

/// Fold element types by `DataType::unify`, as two ternary branches meet.
fn unify_element_types(acc: DataType, next: DataType, span: Span) -> Result<DataType, CompileError> {
    acc.unify(&next).ok_or_else(|| {
        CompileError::new(
            None,
            format!(
                "array elements must all have the same type; found {} and {} at {span:?}",
                acc.spell(),
                next.spell()
            ),
        )
    })
}

/// An object literal as a value: the object its entries lay down (`binding::part`), as a name bound to it would be,
/// built on each row.
fn resolve_object(value: &Expr, syms: &mut SymbolTable, res: &mut ResourceBuilder) -> Result<Ir, CompileError> {
    let part = binding::part(value, syms).map_err(|message| CompileError::new(None, message))?;
    part_ir(part, "", value.span, syms, res)
}

/// `part`, the part at `key` under an object literal, as a value: a leaf's source, or the struct an object's fields
/// make, carrying its unclaimed fields.
fn part_ir(
    part: Part,
    key: &str,
    span: Span,
    syms: &mut SymbolTable,
    res: &mut ResourceBuilder,
) -> Result<Ir, CompileError> {
    let draft = match part {
        Part::Leaf(source) => return source_ir(&source, syms, res),
        Part::Object(draft) => draft,
    };
    let mut fields = Vec::with_capacity(draft.fields.len());
    for (k, part) in draft.fields {
        let path = tree::child(key, &k);
        match part {
            // Past the nesting bound the rest of a path is one key (`tree::segments`), so an object there lays its
            // places down as keys of this one, as a name bound to it would.
            Part::Object(_) if tree::segments(&path).count() == crate::format::json::MAX_NESTING => {
                let mut places = Vec::new();
                binding::flatten(&tree::spell(&k), part, &|_| false, &mut places);
                for (place, source) in places {
                    let ir = source_ir(&source, syms, res)?;
                    let ir = match place {
                        Place::Leaf(_) => ir,
                        Place::Unclaimed(_) => Ir::new(
                            DataType::Struct(Vec::new(), true),
                            span,
                            IrKind::Object {
                                fields: Vec::new(),
                                unclaimed: Some(Box::new(ir)),
                            },
                        ),
                    };
                    fields.push((tree::rest_key(place.name()).into_owned(), ir));
                }
            }
            part => fields.push((k, part_ir(part, &path, span, syms, res)?)),
        }
    }
    // Unclaimed fields known to hold no key leave the type closed: its keys are all known.
    let (unclaimed, open) = match binding::layered(draft.unclaimed) {
        Some(source) => {
            let open = !matches!(source, Source::Unclaimed(u, _) if syms.is_null(u));
            (Some(Box::new(source_ir(&source, syms, res)?)), open)
        }
        None => (None, false),
    };
    let ty = DataType::Struct(fields.iter().map(|(n, ir)| (n.clone(), ir.ty.clone())).collect(), open);
    Ok(Ir::new(ty, span, IrKind::Object { fields, unclaimed }))
}

/// What `source` computes, the value of a place under what lays it down.
pub fn source_ir(source: &Source, syms: &mut SymbolTable, res: &mut ResourceBuilder) -> Result<Ir, CompileError> {
    let span = Span::default();
    Ok(match source {
        Source::Expr(e) => resolve(e, syms, res)?,
        Source::Slot(s) => field_ir(*s, span, syms),
        Source::Unclaimed(s, removed) => unclaimed_ir(*s, removed, span),
        // `top ?? beneath`, as an author would write it.
        Source::Over(top, beneath) => blend(None, source_ir(top, syms, res)?, source_ir(beneath, syms, res)?)?,
        Source::Member(value, member) => {
            let base = source_ir(value, syms, res)?;
            let index = Ir::new(DataType::Str, base.span, IrKind::Literal(Value::Str(member.clone())));
            Ir::new(
                DataType::Variant,
                base.span,
                IrKind::Index {
                    base: Box::new(base),
                    index: Box::new(index),
                },
            )
        }
        Source::Layers(layers) => {
            let mut spreads = Vec::with_capacity(layers.len());
            for layer in layers {
                spreads.push(Layer::Spread(source_ir(layer, syms, res)?));
            }
            Ir::new(DataType::Variant, span, IrKind::Merge(spreads))
        }
    })
}

/// `isNull(operand)`. An object is null where every leaf is and its unclaimed fields are, so it is the conjunction
/// over them; the struct a whole read builds carries no validity of its own.
fn is_null_ir(operand: Ir, operand_expr: &Expr, span: Span, syms: &SymbolTable) -> Ir {
    if let ExprKind::Field(name) = &operand_expr.kind
        && let Some(Node::Object(o)) = syms.node(name)
    {
        let tests: Vec<Ir> = o
            .places()
            .into_iter()
            .map(|(place, slot)| {
                let value = match &place {
                    Place::Leaf(_) => field_ir(slot, span, syms),
                    Place::Unclaimed(n) => {
                        let removed = o.object(n).map_or(&[][..], |u| &u.removed);
                        unclaimed_ir(slot, removed, span)
                    }
                };
                Ir::new(
                    DataType::Bool,
                    span,
                    IrKind::Call {
                        func: StdFunc::IsNull,
                        args: vec![value],
                    },
                )
            })
            .collect();
        return match tests.len() {
            1 => tests.into_iter().next().expect("one test"),
            _ => Ir::new(DataType::Bool, span, IrKind::AndN(tests)),
        };
    }
    Ir::new(
        DataType::Bool,
        span,
        IrKind::Call {
            func: StdFunc::IsNull,
            args: vec![operand],
        },
    )
}

/// A read of `name`. A name laid over the tree (`SymbolTable::laid`) reads its slot where that holds a value, else what
/// the tree reads there, or its slot alone where the tree has nothing there.
fn read_ir(name: &str, span: Span, syms: &mut SymbolTable, res: &mut ResourceBuilder) -> Result<Ir, CompileError> {
    if let Some(&(_, slot)) = syms.laid.iter().find(|(n, _)| n == name) {
        let laid = field_ir(slot, span, syms);
        std::mem::swap(&mut syms.root, &mut syms.under);
        let own = tree_read_ir(name, span, syms, res);
        std::mem::swap(&mut syms.root, &mut syms.under);
        return match own {
            Ok(own) => blend(None, laid, own),
            Err(_) => Ok(laid),
        };
    }
    tree_read_ir(name, span, syms, res)
}

/// `name` read from the tree: a leaf's slot, an object whole, as a copy of it would lay it down, or a member of the leaf
/// a path meets on its way to `name` (`member_ir`), or of an open object's unclaimed fields (`open_member_ir`). A name
/// nothing binds is a compile error, and so is a key a closed object does not have.
fn tree_read_ir(name: &str, span: Span, syms: &mut SymbolTable, res: &mut ResourceBuilder) -> Result<Ir, CompileError> {
    let (at, node) = syms.named(name);
    match node {
        Some(Node::Leaf(slot)) => Ok(field_ir(slot, span, syms)),
        Some(Node::Object(o)) => object_ir(&o, at, span, syms, res),
        None => match open_member_ir(name, span, syms)? {
            Some(ir) => Ok(ir),
            None => member_ir(name, span, syms)?.ok_or_else(|| syms.unresolved(name)),
        },
    }
}

/// `name` read through the deepest object on its way to it, where that object is open and has no field of the next
/// key: the key's value in its unclaimed fields, a variant, and on down the path from there. `None` where that object
/// holds no key besides its fields (`SymbolTable::no_keys`) or has the field.
fn open_member_ir(name: &str, span: Span, syms: &SymbolTable) -> Result<Option<Ir>, CompileError> {
    let (o, at, rest) = syms.root().deepest(name);
    if syms.no_keys(o) {
        return Ok(None);
    }
    let mut keys = tree::segments(rest);
    let key = keys.next().expect("a name has a segment");
    if o.fields.contains_key(key.as_ref()) {
        return Ok(None);
    }
    let unclaimed = unclaimed_or_null(o, span);
    let ir = unclaimed_member(unclaimed, str_ir(&key, span), span);
    path_under(ir, &tree::child(at, &key), keys, span).map(Some)
}

/// The unclaimed fields of `o`, or null where it has none.
fn unclaimed_or_null(o: &Object, span: Span) -> Ir {
    match o.unclaimed {
        Some(u) => unclaimed_ir(u, &o.removed, span),
        None => Ir::new(DataType::Variant, span, IrKind::Literal(Value::Null)),
    }
}

/// The value at `key` in `unclaimed`, an open object's unclaimed fields: a variant.
fn unclaimed_member(unclaimed: Ir, key: Ir, span: Span) -> Ir {
    Ir::new(
        DataType::Variant,
        span,
        IrKind::ObjectOp {
            op: ObjectOp::Get,
            args: vec![unclaimed, key],
        },
    )
}

/// An object's value as its fields, each with what reads it, and its unclaimed fields if it is open.
fn object_parts(ir: Ir) -> (Vec<(String, Ir)>, Option<Ir>) {
    if let IrKind::Object { fields, unclaimed } = ir.kind {
        return (fields, unclaimed.map(|u| *u));
    }
    let DataType::Struct(types, open) = &ir.ty else {
        unreachable!("only an object has parts")
    };
    let (span, open) = (ir.span, *open);
    let fields = types
        .iter()
        .map(|(n, t)| {
            let read = IrKind::Index {
                base: Box::new(ir.clone()),
                index: Box::new(str_ir(n, span)),
            };
            (n.clone(), Ir::new(t.clone(), span, read))
        })
        .collect();
    let unclaimed = open.then(|| Ir::new(DataType::Variant, span, IrKind::Unclaimed(Box::new(ir))));
    (fields, unclaimed)
}

/// `o[key]` for a key only the row knows: the field it names, or its key in the unclaimed fields, a variant either
/// way; null where it names nothing.
///
/// An object whose fields are each their own column is read as `lay_key` writes it: a compare per field, and `Get`
/// only on the unclaimed fields. Any other object is read whole, which encodes every field on every row.
fn computed_key_read(object: Ir, key: Ir, key_expr: &Expr, syms: &SymbolTable, span: Span) -> Result<Ir, CompileError> {
    object_key(&key, key_expr, syms)?;
    Ok(match object.kind {
        IrKind::Object { .. } | IrKind::Field(_) => read_key(object, &key, span),
        _ => get_ir(object, key, span),
    })
}

/// `key` read under `o`: a ternary chain, each field in turn, the field `key` names taken as a variant, and the
/// unclaimed fields read where no field has it.
fn read_key(o: Ir, key: &Ir, span: Span) -> Ir {
    let (fields, unclaimed) = object_parts(o);
    let variant = |ir: Ir| coerce_ir(ir, &DataType::Variant, span).expect("every value is a variant");
    let named: Vec<Ir> = fields.iter().map(|(name, _)| key_names(key, name, span)).collect();
    let mut out = match unclaimed {
        None => Ir::new(DataType::Variant, span, IrKind::Literal(Value::Null)),
        Some(u) => get_ir(u, unclaimed_key(key, named.clone(), span), span),
    };
    for ((_, field), named) in fields.into_iter().zip(named).rev() {
        out = choose(&named, variant(field), out, span);
    }
    out
}

/// The value at `key` in `objects`, a variant object on each row: a variant, null where the row's object has no
/// such key.
fn get_ir(objects: Ir, key: Ir, span: Span) -> Ir {
    Ir::new(
        DataType::Variant,
        span,
        IrKind::ObjectOp {
            op: ObjectOp::Get,
            args: vec![objects, key],
        },
    )
}

/// Whether `key`, a key only the row knows, is `name` on each row.
fn key_names(key: &Ir, name: &str, span: Span) -> Ir {
    Ir::new(
        DataType::Bool,
        span,
        IrKind::Binary {
            op: BinOp::Eq,
            lhs: Box::new(key.clone()),
            rhs: Box::new(str_ir(name, span)),
        },
    )
}

/// Whether `key` may name an object's key: text, or null, which names none.
pub fn object_key(key: &Ir, key_expr: &Expr, syms: &SymbolTable) -> Result<(), CompileError> {
    refuse_variant(key, key_expr, syms)?;
    match &key.ty {
        DataType::Str | DataType::Null => Ok(()),
        other => Err(CompileError::new(
            None,
            format!("an object's key is str, found {}", other.spell()),
        )),
    }
}

/// A text literal.
fn str_ir(s: &str, span: Span) -> Ir {
    Ir::new(DataType::Str, span, IrKind::Literal(Value::Str(s.to_string())))
}

/// The object `o` at `name` read whole, as a copy of it would lay it down.
fn object_ir(
    o: &Object,
    name: &str,
    span: Span,
    syms: &mut SymbolTable,
    res: &mut ResourceBuilder,
) -> Result<Ir, CompileError> {
    part_ir(Part::Object(binding::copied(o)), name, span, syms, res)
}

/// The unclaimed fields in `slot`, less the keys `removed` takes out of them (`tree::Object::removed`).
pub fn unclaimed_ir(slot: SlotId, removed: &[String], span: Span) -> Ir {
    let column = Ir::new(DataType::Variant, span, IrKind::Field(slot));
    if removed.is_empty() {
        return column;
    }
    unclaimed_less(column, removed.iter().map(|k| str_ir(k, span)), span)
}

/// Unclaimed fields `unclaimed` with each of `keys` laid over them as null, which takes it out.
fn unclaimed_less(unclaimed: Ir, keys: impl Iterator<Item = Ir>, span: Span) -> Ir {
    let null = Ir::new(DataType::Null, span, IrKind::Literal(Value::Null));
    let layers = std::iter::once(Layer::Spread(unclaimed))
        .chain(keys.map(|k| Layer::Key(k, null.clone())))
        .collect();
    Ir::new(DataType::Variant, span, IrKind::Merge(layers))
}

/// A plain slot reference, typed as the slot is.
fn field_ir(slot: SlotId, span: Span, syms: &SymbolTable) -> Ir {
    Ir::new(syms.type_of(slot).clone(), span, IrKind::Field(slot))
}

/// `name` read through the leaf a path meets on its way to it: a variant gives a member, a variant, where the row's
/// value has it; a struct gives its field, with the field's type. A path under a scalar is a compile error. `None`
/// where no leaf stands above `name`, or where the leaf is an array or null, which have no member to read.
pub fn member_ir(name: &str, span: Span, syms: &SymbolTable) -> Result<Option<Ir>, CompileError> {
    let Some((above, slot)) = syms.leaf_above(name) else {
        return Ok(None);
    };
    if matches!(syms.type_of(slot), DataType::Array(_) | DataType::Null) {
        return Ok(None);
    }
    let ir = field_ir(slot, span, syms);
    path_under(
        ir,
        above,
        tree::segments(name).skip(tree::segments(above).count()),
        span,
    )
    .map(Some)
}

/// `keys` read in turn under `ir`, the value at `receiver`: a variant gives a member, a variant; a struct its field,
/// or where it is open and has no such field, the key's value in its unclaimed fields. A path under a scalar is a
/// compile error, and so is a key a closed struct lacks.
fn path_under<'k>(
    mut ir: Ir,
    receiver: &str,
    keys: impl Iterator<Item = Cow<'k, str>>,
    span: Span,
) -> Result<Ir, CompileError> {
    let mut receiver = receiver.to_string();
    for key in keys {
        let key = key.as_ref();
        let index = str_ir(key, span);
        ir = match &ir.ty {
            DataType::Variant => Ir::new(
                DataType::Variant,
                span,
                IrKind::Index {
                    base: Box::new(ir),
                    index: Box::new(index),
                },
            ),
            DataType::Struct(fields, open) => match (fields.iter().find(|(n, _)| n == key), open) {
                (Some((_, ty)), _) => Ir::new(
                    ty.clone(),
                    span,
                    IrKind::Index {
                        base: Box::new(ir),
                        index: Box::new(index),
                    },
                ),
                (None, true) => {
                    let unclaimed = Ir::new(DataType::Variant, span, IrKind::Unclaimed(Box::new(ir)));
                    unclaimed_member(unclaimed, index, span)
                }
                (None, false) => {
                    let name = tree::child(&receiver, key);
                    return Err(CompileError::new(None, format!("`{name}` is not a field")));
                }
            },
            t => {
                return Err(CompileError::new(
                    None,
                    format!("`{receiver}` is {} and has no fields", t.spell()),
                ));
            }
        };
        tree::push_key(&mut receiver, key);
    }
    Ok(ir)
}

/// The pieces an object bound to `name` is: each field of `value`, a struct, a leaf read from it, a struct field an
/// object, and its unclaimed fields the object's where its type is open. Each by its place, with what fills it. An
/// object bound to a name is its leaves, wherever it came from: an array's element, a coercion, a parse.
pub fn split_struct(name: &str, value: &Ir) -> Vec<(Place, Ir)> {
    let mut out = Vec::new();
    split_into(name, value, &mut out);
    out
}

fn split_into(name: &str, value: &Ir, out: &mut Vec<(Place, Ir)>) {
    let DataType::Struct(fields, open) = &value.ty else {
        unreachable!("only a struct splits into leaves")
    };
    let span = value.span;
    for (key, ty) in fields {
        let index = Ir::new(DataType::Str, span, IrKind::Literal(Value::Str(key.clone())));
        let read = Ir::new(
            ty.clone(),
            span,
            IrKind::Index {
                base: Box::new(value.clone()),
                index: Box::new(index),
            },
        );
        let path = tree::child(name, key);
        match ty {
            DataType::Struct(..) => split_into(&path, &read, out),
            _ => out.push((Place::Leaf(path), read)),
        }
    }
    if *open {
        out.push((
            Place::Unclaimed(name.to_string()),
            Ir::new(DataType::Variant, span, IrKind::Unclaimed(Box::new(value.clone()))),
        ));
    }
}

/// Bind `name` to an array's element, the slot `element`: the slot itself, or, for an element that is an object, the
/// leaves `split_struct` reads from it, each a fresh slot. The leaves go to `out`, for what visits the elements to fill.
pub fn bind_element(syms: &mut SymbolTable, name: &str, element: SlotId, origin: &Origin, out: &mut Vec<(SlotId, Ir)>) {
    let ty = syms.type_of(element).clone();
    if !matches!(ty, DataType::Struct(..)) {
        syms.put(name, Node::Leaf(element));
        return;
    }
    syms.unbind(name);
    let value = Ir::new(ty.clone(), Span::default(), IrKind::Field(element));
    for (place, ir) in split_struct(name, &value) {
        let slot = syms.bind_place(&place, ir.ty.clone(), origin.clone());
        out.push((slot, ir));
    }
}

/// Lower an element read: `base[index]`, and `at(base, index)`, which are the same call. Three shapes, chosen by
/// `base`'s resolved type:
///
/// - `Array(T)[I64]` and `Text[I64]` — an element read, lowered to `at`. One implementation of element access, so
///   `xs[-1]` is the last element exactly as `xs.at(-1)` is, and an out-of-range index is null in both.
/// - `Struct[...]` — a field read. Unlike an element, the field set is static and heterogeneously typed, so the result
///   type can only be known if the key is a compile-time constant: `index` must *syntactically* be a string literal (the
///   same "consumed structurally" treatment `resolve_call` gives `match`/`replace`'s regex argument), and an unknown
///   field name is a compile error rather than a runtime null — the field set is fully known, so a bad key is a real
///   mistake, not a legitimate miss the way an out-of-range element index is.
/// - `Variant[Str | I64]` — what each row's value holds at the key, a variant: a member at a string, an element at a
///   number, null where the row's value holds nothing there.
///
/// `base` of type `null` (an all-null expression) is a compile error: there is no way
/// to know which of the shapes above applies without a concrete type.
///
/// The array and object shapes are here, rather than as `at` rows, because each is typed from the *value* of its key -- the field a
/// literal names, or the element type of an array the `Array` parameter kind does not cover -- which a `Ret` rule
/// cannot express. `resolve_call` routes `at` here so the two spellings cannot answer differently.
fn resolve_element_access(
    base: &Expr,
    index: &Expr,
    span: Span,
    syms: &mut SymbolTable,
    res: &mut ResourceBuilder,
) -> Result<Ir, CompileError> {
    // A key only the row knows, on an object bound to a name, `$` the root among them.
    if let ExprKind::Field(name) = &base.kind
        && let (at, Some(Node::Object(o))) = syms.named(name)
    {
        let key = resolve(index, syms, res)?;
        return computed_key_read(object_ir(&o, at, span, syms, res)?, key, index, syms, span);
    }
    let base_ir = resolve(base, syms, res)?;
    // Cloned because each arm below moves `base_ir` into the node it builds while still needing the type to pick and
    // check that shape.
    let base_ty = base_ir.ty.clone();
    match base_ty {
        // A key under a value whose shape only the row knows: the member where the row's value has it.
        DataType::Variant if let ExprKind::Literal(Value::Str(key)) = &index.kind => Ok(Ir::new(
            DataType::Variant,
            span,
            IrKind::Index {
                base: Box::new(base_ir),
                index: Box::new(str_ir(key, span)),
            },
        )),
        // A key only the row knows: a member's name or an element's position.
        DataType::Variant => {
            let index_ir = resolve(index, syms, res)?;
            refuse_variant(&index_ir, index, syms)?;
            match &index_ir.ty {
                DataType::Null | DataType::I64 | DataType::Str => Ok(Ir::new(
                    DataType::Variant,
                    span,
                    IrKind::Index {
                        base: Box::new(base_ir),
                        index: Box::new(index_ir),
                    },
                )),
                other => Err(CompileError::new(
                    None,
                    format!("a key under a variant is a str or an i64, found {}", other.spell()),
                )),
            }
        }
        DataType::Null => Err(CompileError::new(None, "cannot index null")),
        // `x["trim"]` on a scalar is a method call that forgot its parentheses, not an index; it belongs in the hint
        // below, so only a non-string key reaches `at` here.
        DataType::Array(_) | DataType::Str | DataType::Bytes
            if !matches!(&index.kind, ExprKind::Literal(Value::Str(_))) =>
        {
            let index_ir = resolve(index, syms, res)?;
            refuse_variant(&index_ir, index, syms)?;
            let index_ir = match &index_ir.ty {
                DataType::Null | DataType::I64 => index_ir,
                other => {
                    return Err(CompileError::new(
                        None,
                        format!("an index must be i64, found {}", other.spell()),
                    ));
                }
            };
            let args = vec![Arg::Ty(base_ty.clone()), Arg::Ty(index_ir.ty.clone())];
            // `at`'s `Array` parameter is an array of scalars, so a declared array of objects has no row and keeps its
            // own element read. `expr::eval::array_index` answers the same way `at` does, negative index included.
            match StdFunc::At.result_type(&args) {
                Some(result_ty) => Ok(Ir::new(
                    result_ty,
                    span,
                    IrKind::Call {
                        func: StdFunc::At,
                        args: vec![base_ir, index_ir],
                    },
                )),
                None => {
                    let DataType::Array(elem_ty) = base_ty else {
                        return Err(CompileError::new(None, "cannot index this value".to_string()));
                    };
                    Ok(Ir::new(
                        *elem_ty,
                        span,
                        IrKind::Index {
                            base: Box::new(base_ir),
                            index: Box::new(index_ir),
                        },
                    ))
                }
            }
        }
        // A key is one key, whether the pipeline or the row knows it.
        DataType::Struct(..) => match &index.kind {
            ExprKind::Literal(Value::Str(key)) => path_under(
                base_ir,
                "this object",
                std::iter::once(Cow::Borrowed(key.as_str())),
                span,
            ),
            _ => {
                let key = resolve(index, syms, res)?;
                computed_key_read(base_ir, key, index, syms, span)
            }
        },
        other => {
            // `x.trim` parses as member access now (dot on an expression is sugar for `x["trim"]`), so a method call
            // that forgot its parentheses lands here instead of failing in the parser. Without the hint the author gets
            // a type error about indexing, which is true but says nothing about the actual mistake.
            let hint = match &index.kind {
                ExprKind::Literal(Value::Str(key)) => {
                    format!(" -- if you meant to call a method, add parentheses: `.{key}()`")
                }
                _ => String::new(),
            };
            Err(CompileError::new(
                None,
                format!(
                    "cannot index a value of type {} (expected an array, string or object){hint}",
                    other.spell()
                ),
            ))
        }
    }
}

// The type judgment is `ops::table`'s, plus the two things the table cannot express: `null` and `variant`. `null` belongs
// to every type (`DataType::unify`). A variant is assigned, renamed, copied, spread, serialized and tested for null,
// and nothing else: an operator, a call or a condition given one is a compile error that names the type to give it.
// Each rule's `Err` is the message.

/// What a use of a variant says, after the value's name, with `hint`, the conversion the caller knows, or the one
/// that fits any name.
pub fn variant_use(hint: Option<&str>) -> String {
    let hint = hint.unwrap_or("`name: type = name;`");
    format!("is a variant: give it a type first -- {hint}")
}

/// Whether `t` is `null` or holds it: an array's element, a struct's field.
pub fn holds_null(t: &DataType) -> bool {
    match t {
        DataType::Null => true,
        DataType::Array(e) => holds_null(e),
        DataType::Struct(fields, _) => fields.iter().any(|(_, t)| holds_null(t)),
        _ => false,
    }
}

/// The types an operator takes its operands as: where one holds `null`, the null part takes the other side's type, so
/// `null < 1` is typed as `1 < 1` and `[] + xs` as `xs + xs`.
fn operand_types(a: &DataType, b: &DataType) -> (DataType, DataType) {
    match a.unify(b) {
        Some(t) if holds_null(a) || holds_null(b) => (t.clone(), t),
        _ => (a.clone(), b.clone()),
    }
}

/// Whether `from` holds an `i64` where `to` holds an `f64`, at any depth: a value that widens to meet it.
pub fn widens(from: &DataType, to: &DataType) -> bool {
    use DataType::*;
    match (from, to) {
        (I64, F64) => true,
        (Array(f), Array(t)) => widens(f, t),
        (Struct(have, _), Struct(want, _)) => have
            .iter()
            .any(|(n, f)| want.iter().any(|(m, t)| m == n && widens(f, t))),
        _ => false,
    }
}

/// `ir` widened where it holds an `i64` that `to` holds as an `f64`; otherwise `ir`. A run-time kernel never meets an
/// `i64` beside an `f64`.
pub fn widen_number(ir: Ir, to: &DataType, span: Span) -> Result<Ir, CompileError> {
    if !widens(&ir.ty, to) {
        return Ok(ir);
    }
    let to = ir.ty.unify(to).unwrap_or_else(|| to.clone());
    coerce_ir(ir, &to, span)
}

/// A binary operator's type, its operands widened as `widen_number` widens them.
pub fn binop_result(op: BinOp, a: &DataType, b: &DataType) -> Result<DataType, String> {
    // `??` treats a null operand as the subject of the test, not a hole, and reads a variant's presence. Its sides
    // meet as a ternary's branches do, and a pair that does not meet is a variant.
    if op == BinOp::Coalesce {
        return Ok(a.unify(b).unwrap_or(DataType::Variant));
    }
    if *a == DataType::Variant || *b == DataType::Variant {
        return Err(variant_use(None));
    }
    match (a, b) {
        // A comparison, `&&` and `||` are true or false on every row, whatever their operands hold.
        (DataType::Null, DataType::Null) if op.is_comparison() || matches!(op, BinOp::And | BinOp::Or) => {
            Ok(DataType::Bool)
        }
        (DataType::Null, DataType::Null) => Ok(DataType::Null),
        _ => {
            let (l, r) = operand_types(a, b);
            let widened = |x: &DataType, y: &DataType| match widens(x, y) {
                true => x.unify(y).unwrap_or_else(|| y.clone()),
                false => x.clone(),
            };
            let (l, r) = (widened(&l, &r), widened(&r, &l));
            table::result_type(op, &l, &r).ok_or_else(|| {
                format!(
                    "operator {} is not defined for {} and {}",
                    op.symbol(),
                    a.spell(),
                    b.spell()
                )
            })
        }
    }
}

/// A unary operator's type.
pub fn unop_result(op: UnOp, t: &DataType) -> Result<DataType, String> {
    match t {
        DataType::Variant => Err(variant_use(None)),
        // `!null` is true.
        DataType::Null if op == UnOp::Not => Ok(DataType::Bool),
        DataType::Null => Ok(DataType::Null),
        t => table::unop_result(op, t).ok_or_else(|| format!("unary {} is not defined for {}", op.symbol(), t.spell())),
    }
}

/// Whether `t` is what a condition must be: `Bool`, or `null`, which reads false.
pub fn is_condition(t: &DataType) -> bool {
    matches!(t, DataType::Null | DataType::Bool)
}

/// `cond ? then : else_`, or `then ?? else_` with no condition: the one blend, at `then`'s span. Its parts meet as
/// `DataType::unify` says, `f64` for `i64` with `f64`; a pair that does not meet is a variant, which moves each row's
/// value as it is (Language).
pub fn blend(cond: Option<Ir>, then: Ir, else_: Ir) -> Result<Ir, CompileError> {
    let (ty, span) = (then.ty.unify(&else_.ty).unwrap_or(DataType::Variant), then.span);
    let (then, else_) = (
        Box::new(widen_to(then, &ty, span)?),
        Box::new(widen_to(else_, &ty, span)?),
    );
    let kind = match cond {
        Some(c) => IrKind::Ternary {
            cond: Box::new(c),
            then,
            else_,
        },
        None => IrKind::Binary {
            op: BinOp::Coalesce,
            lhs: then,
            rhs: else_,
        },
    };
    Ok(Ir::new(ty, span, kind))
}

/// `cond ? a : b`. The condition must be `Bool` (no truthiness). The branches meet as `DataType::unify` says; a pair
/// that does not meet, or a variant branch, makes the whole a variant: the blend moves both values and reads neither.
pub fn ternary_result(cond: &DataType, then: &DataType, else_: &DataType) -> Result<DataType, String> {
    if !is_condition(cond) {
        return Err(format!(
            "ternary condition must be bool, found {} (no truthiness coercion)",
            cond.spell()
        ));
    }
    Ok(then.unify(else_).unwrap_or(DataType::Variant))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::batch::Batch;
    use crate::core::bits;
    use crate::core::buffer::BufferPool;
    use crate::core::column::Column;
    use crate::core::ctx::{CompiledPattern, Ctx};
    use crate::core::error::ErrorSink;
    use crate::expr::eval::eval;
    use crate::expr::parse::parse_expr;
    use crate::ops::BinOp;
    use crate::ops::stdfuncs::{Arg, ROWS, StdFunc};
    use crate::ops::testing::rows_of;

    fn table() -> SymbolTable {
        SymbolTable::new(&[
            ("__live", DataType::Bool),
            ("__source.offset", DataType::I64),
            ("_raw", DataType::Str),
            ("__source.topic", DataType::Str),
        ])
    }

    fn with_fields(fields: &[(&str, DataType)]) -> SymbolTable {
        let mut t = table();
        for (n, ty) in fields {
            t.bind(n, ty.clone(), Origin::Function(0));
        }
        t
    }

    fn lower(src: &str, t: &mut SymbolTable) -> Result<Ir, CompileError> {
        let e = parse_expr(src).unwrap();
        resolve(&e, t, &mut ResourceBuilder::default())
    }

    /// `ir` over four rows, its result handed to `read`. Each of `fields` holds `rows_of` its type: a value, a null, a
    /// zero or empty, and an edge. A regex literal is `/x/`, the one pattern `lower` interns.
    fn eval_rows<R>(ir: &Ir, t: &SymbolTable, fields: &[(&str, DataType)], read: impl FnOnce(&Column) -> R) -> R {
        let mut pool = BufferPool::new();
        let mut batch = Batch::new(&t.layout, 4, 0);
        for (name, ty) in fields {
            let col = rows_of(&mut pool, ty);
            batch.put(t.lookup(name).unwrap(), col, &mut pool);
        }
        let patterns = [CompiledPattern {
            regex: regex::Regex::new("x").unwrap(),
        }];
        let mut errors = ErrorSink::new();
        let out = {
            let mut ctx = Ctx::new(&mut pool, &mut errors);
            ctx.patterns = &patterns;
            eval(ir, &batch, &mut ctx)
        };
        let r = read(&out);
        out.reclaim(&mut pool);
        batch.reclaim(&mut pool);
        r
    }

    /// Each row of a `Bool` column, `None` where it is null.
    fn bools(c: &Column) -> Vec<Option<bool>> {
        let b = c.bool().unwrap();
        (0..c.len())
            .map(|i| b.validity.is_valid(i).then(|| bits::get_row(b.values, i)))
            .collect()
    }

    #[test]
    fn field_resolves_to_a_slot_with_its_type() {
        let mut t = with_fields(&[("a", DataType::I64)]);
        let ir = lower("a", &mut t).unwrap();
        assert_eq!(ir.ty, DataType::I64);
        assert!(matches!(ir.kind, IrKind::Field(_)));
    }

    #[test]
    fn types_propagate_bottom_up() {
        let mut t = with_fields(&[("a", DataType::I64), ("b", DataType::F64)]);
        assert_eq!(lower("a + a", &mut t).unwrap().ty, DataType::I64);
        assert_eq!(lower("a + b", &mut t).unwrap().ty, DataType::F64);
        assert_eq!(lower("a / a", &mut t).unwrap().ty, DataType::F64);
        assert_eq!(lower("a < b", &mut t).unwrap().ty, DataType::Bool);
    }

    #[test]
    fn type_errors_are_caught_at_compile_time() {
        let mut t = with_fields(&[("s", DataType::Str), ("n", DataType::I64)]);
        let e = lower("s + n", &mut t).unwrap_err();
        assert!(e.message.contains("str"), "{}", e.message);
    }

    #[test]
    fn bind_allocates_a_fresh_slot_each_time() {
        // SSA. `x = x + 1` must not reuse x's slot.
        let mut t = table();
        let first = t.bind("x", DataType::I64, Origin::Function(0));
        let second = t.bind("x", DataType::I64, Origin::Function(1));
        assert_ne!(first, second);
        assert_eq!(t.lookup("x"), Some(second), "the name follows the new slot");
    }

    #[test]
    fn rename_moves_the_name_not_the_column() {
        let mut t = with_fields(&[("src", DataType::I64)]);
        let slot = t.lookup("src").unwrap();
        let before = t.slot_count();

        t.rebind("src", "dst").unwrap();

        assert_eq!(t.lookup("dst"), Some(slot), "same slot, new name");
        assert_eq!(t.lookup("src"), None, "old name is gone");
        assert_eq!(t.slot_count(), before, "no new slot allocated");
    }

    #[test]
    fn rename_updates_provenance() {
        let mut t = with_fields(&[("src", DataType::I64)]);
        let slot = t.lookup("src").unwrap();
        t.rebind("src", "dst").unwrap();
        let o = t.origins.iter().find(|o| o.slot == slot).unwrap();
        assert_eq!(o.name, "dst");
    }

    #[test]
    fn renaming_onto_a_bound_name_replaces_it() {
        let mut t = with_fields(&[("a", DataType::I64), ("b", DataType::I64), ("b.k", DataType::I64)]);
        let a = t.lookup("a").unwrap();
        t.rebind("a", "b").unwrap();
        assert_eq!(t.lookup("b"), Some(a));
        assert_eq!(t.lookup("a"), None);
        assert_eq!(t.lookup("b.k"), None);
    }

    /// A scalar has no path under it: a write to `source.ip`, where `source` is a `str`, is refused, and the refusal
    /// names the scalar.
    #[test]
    fn a_write_under_a_scalar_is_refused() {
        let t = with_fields(&[("source", DataType::Str)]);
        let e = t.write_refused("source.ip").expect("a path under a scalar");
        assert!(e.message.contains("`source` is str and has no fields"), "{}", e.message);
    }

    /// A bind of a value over an object replaces the object: its leaves and its unclaimed fields leave the tree with it.
    #[test]
    fn binding_a_scalar_over_an_object_replaces_it() {
        let mut t = with_fields(&[("source.ip", DataType::Str), ("source.port", DataType::I64)]);
        t.bind_unclaimed("source", Origin::Function(0));
        let slot = t.bind("source", DataType::Str, Origin::Function(1));
        assert_eq!(t.node("source"), Some(&Node::Leaf(slot)));
        let under: Vec<Place> = t
            .root()
            .places()
            .into_iter()
            .map(|(p, _)| p)
            .filter(|p| binding::at_or_under(p.name(), "source"))
            .collect();
        assert_eq!(under, vec![Place::Leaf("source".into())]);
    }

    #[test]
    fn sibling_dotted_fields_do_not_conflict() {
        // "source.ip" and "source.port" share a prefix but neither is a dotted-prefix OF the other — both are ordinary
        // nested fields under the same parent object, which is exactly what the data model wants to allow. No warning
        // either.
        let mut t = with_fields(&[("source.ip", DataType::Str)]);
        let before = t.warnings.len();
        t.bind("source.port", DataType::I64, Origin::Function(0));
        assert_eq!(t.warnings.len(), before, "siblings aren't a conflict");
    }

    #[test]
    fn reassigning_the_same_name_is_not_a_conflict() {
        // SSA: `x = x + 1` binds "x" again on top of an existing "x". That must keep working, with no warning — only
        // binding a DIFFERENT, dot-nested name is flagged.
        let mut t = with_fields(&[("x", DataType::I64)]);
        let before = t.warnings.len();
        t.bind("x", DataType::I64, Origin::Function(1));
        assert_eq!(t.warnings.len(), before);
    }

    #[test]
    fn a_name_that_prefixes_itself_via_substring_is_not_a_false_positive() {
        // "sourceX" is not a dotted child of "source" — the check must be boundary-aware, not a bare string-prefix
        // test.
        let mut t = with_fields(&[("source", DataType::Str)]);
        let before = t.warnings.len();
        t.bind("sourceX", DataType::Str, Origin::Function(0));
        assert_eq!(t.warnings.len(), before);
    }

    /// A rename writes its target, so a target under a scalar is refused like any write: at any depth, the refusal
    /// names the scalar the path meets first.
    #[test]
    fn a_rename_target_under_a_scalar_is_refused_at_the_scalar() {
        let t = with_fields(&[("source", DataType::Str)]);
        let e = t.write_refused("source.geo.ip").expect("a path under a scalar");
        assert!(e.message.contains("`source` is str and has no fields"), "{}", e.message);
    }

    #[test]
    fn renaming_an_unknown_field_is_an_error() {
        let mut t = table();
        assert!(t.rebind("ghost", "x").is_err());
    }

    #[test]
    fn an_unknown_field_is_an_error() {
        let mut t = table();
        let e = lower("ghost", &mut t).unwrap_err();
        assert!(e.message.contains("`ghost` is not a field"), "{}", e.message);
    }

    #[test]
    fn patterns_are_interned_and_deduplicated() {
        let mut t = with_fields(&[("x", DataType::Str)]);
        let mut res = ResourceBuilder::default();
        for src in ["match(x, /foo/i)", "match(x, /foo/i)", "match(x, /bar/)"] {
            let e = parse_expr(src).unwrap();
            resolve(&e, &mut t, &mut res).unwrap();
        }
        assert_eq!(res.patterns.len(), 2, "identical patterns share one id");
    }

    #[test]
    fn flags_distinguish_otherwise_identical_patterns() {
        let mut t = with_fields(&[("x", DataType::Str)]);
        let mut res = ResourceBuilder::default();
        resolve(&parse_expr("match(x, /foo/)").unwrap(), &mut t, &mut res).unwrap();
        resolve(&parse_expr("match(x, /foo/i)").unwrap(), &mut t, &mut res).unwrap();
        assert_eq!(res.patterns.len(), 2);
    }

    #[test]
    fn nested_expression_lowers_completely() {
        let mut t = with_fields(&[("a", DataType::I64), ("b", DataType::I64)]);
        let ir = lower("a + b * 2", &mut t).unwrap();
        match &ir.kind {
            IrKind::Binary {
                op: BinOp::Add, rhs, ..
            } => {
                assert!(matches!(rhs.kind, IrKind::Binary { op: BinOp::Mul, .. }));
            }
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn ternary_requires_a_bool_condition() {
        let mut t = with_fields(&[("s", DataType::Str), ("n", DataType::I64)]);
        let e = lower("s ? n : n", &mut t).unwrap_err();
        assert!(e.message.contains("truthiness"), "{}", e.message);
    }

    #[test]
    fn a_temp_takes_no_name_and_provenance_is_indexed_by_slot() {
        let mut t = with_fields(&[("a", DataType::I64)]);
        let names = t.root().leaves();
        let s = t.temp(DataType::Bool, Origin::Guard);
        t.temp(DataType::Variant, Origin::Temp);
        // Config reads a field by name, and the serializer writes the live names. Neither may reach a temp.
        assert_eq!(t.root().leaves(), names);
        assert_eq!(
            t.lookup(&t.origins[s].name),
            None,
            "not even by the name provenance shows"
        );
        t.bind("b", DataType::I64, Origin::Function(0));
        // The runtime reads a slot's provenance at the slot's index.
        assert_eq!(t.origins.len(), t.slot_count());
        assert!(t.origins.iter().enumerate().all(|(i, o)| o.slot == i));
        assert_eq!(t.origins[s].origin, Origin::Guard);
    }

    #[test]
    fn indexing_a_scalar_is_a_compile_error() {
        // Indexing only makes sense on Array/Struct; a scalar is a clear compile error, not a panic.
        let mut t = with_fields(&[("a", DataType::I64)]);
        assert!(lower("a[0]", &mut t).unwrap_err().message.contains("cannot index"));
    }

    /// An array literal nests, and its items meet at every depth.
    #[test]
    fn an_array_literal_nests() {
        let mut t = table();
        let arr = |e: DataType| DataType::Array(Box::new(e));
        assert_eq!(lower("[[1]]", &mut t).unwrap().ty, arr(arr(DataType::I64)));
        assert_eq!(lower("[[1], [2.5]]", &mut t).unwrap().ty, arr(arr(DataType::F64)));
        let s = DataType::Struct(vec![("a".into(), arr(DataType::I64))], false);
        assert_eq!(lower("[{a: [1]}]", &mut t).unwrap().ty, arr(s));
        let e = lower("[[1], [\"x\"]]", &mut t).unwrap_err();
        assert!(e.message.contains("same type"), "{}", e.message);
    }

    // ------------------------------------------------------------------
    // Stdlib `Call` resolution
    // ------------------------------------------------------------------

    #[test]
    fn comparing_with_the_null_literal_is_true_exactly_where_the_value_is_null() {
        // Under the null propagation every other operator gets, `x != null` would be null on every row and guard
        // nothing. `d` is a variant. Row 1 of each field is its null.
        let mut t = with_fields(&[("s", DataType::Str)]);
        t.bind("d", DataType::Variant, Origin::Function(0));
        let fields = [("s", DataType::Str), ("d", DataType::I64)];
        let is_null = [Some(false), Some(true), Some(false), Some(false)];
        let not_null = [Some(true), Some(false), Some(true), Some(true)];
        for x in ["s", "d"] {
            for (src, want) in [
                (format!("{x} == null"), is_null),
                (format!("null == {x}"), is_null),
                (format!("{x} != null"), not_null),
                (format!("null != {x}"), not_null),
            ] {
                let ir = lower(&src, &mut t).unwrap();
                assert_eq!(eval_rows(&ir, &t, &fields, bools), want, "{src}");
            }
        }
    }

    /// A variant only moves: comparing it with a value is a compile error that names the type to give it. A
    /// nullness test and `??` still read it.
    #[test]
    fn a_variant_is_refused_by_an_operator_and_read_by_a_nullness_test() {
        let mut t = table();
        t.bind("kind", DataType::Variant, Origin::Function(0));
        let e = lower("kind == \"a\"", &mut t).unwrap_err();
        assert!(e.message.contains("`kind` is a variant"), "{}", e.message);
        assert!(e.message.contains("`kind: type = kind;`"), "{}", e.message);
        for src in ["kind + 1", "!kind", "kind ? 1 : 2", "length(kind)", "[kind]"] {
            let e = lower(src, &mut t).unwrap_err();
            assert!(e.message.contains("variant"), "{src}: {}", e.message);
        }
        for src in ["kind == null", "kind != null", "isNull(kind)", "kind.isNull()"] {
            assert_eq!(lower(src, &mut t).unwrap().ty, DataType::Bool, "{src}");
        }
        assert_eq!(lower("kind ?? 1", &mut t).unwrap().ty, DataType::Variant);
    }

    /// A call or an index that compiles runs on every row its arguments can hold. The kernels read their arguments
    /// unchecked, so a type the compiler let through by mistake would panic on every batch. A refusal of the memory
    /// budget -- `resize` to `i64::MAX` -- unwinds by design, and the runtime takes it.
    #[test]
    fn a_call_or_index_that_compiles_does_not_panic_at_run_time() {
        use DataType::*;
        let scalars = [Bool, I64, F64, Str, Bytes, DateTime, Duration];
        let arrays = [I64, F64, Str, Bool].map(|t| Array(Box::new(t)));
        let tys: Vec<DataType> = scalars.into_iter().chain(arrays).collect();
        let names: Vec<String> = (0..tys.len()).map(|i| format!("v{i}")).collect();
        let fields: Vec<(&str, DataType)> = names.iter().map(String::as_str).zip(tys).collect();
        let mut t = with_fields(&fields);
        let mut args: Vec<&str> = names.iter().map(String::as_str).collect();
        args.push("/x/");

        // Every arity the table has. A variadic function is tried with one argument more.
        let mut shapes = std::collections::BTreeSet::new();
        for row in ROWS {
            shapes.insert((row.name, row.params.len()));
            if row.func.is_variadic() {
                shapes.insert((row.name, row.params.len() + 1));
            }
        }
        let mut srcs: Vec<String> = args
            .iter()
            .flat_map(|b| args.iter().map(move |i| format!("{b}[{i}]")))
            .collect();
        // A call of three or more arguments compiles only where a row of its function takes every one, each by its
        // own parameter: `resolve_call` answers before the table only for one or two. So a longer call is tried over
        // the arguments each parameter takes, not every argument -- `communityId`'s six would be millions of calls.
        let kind = |func: StdFunc, j: usize, a: usize| match fields.get(a) {
            Some((_, ty)) => Some(Arg::Ty(ty.clone())),
            None => func.takes_pattern_at(j).then_some(Arg::Pattern),
        };
        let mut calls = std::collections::BTreeSet::new();
        for (name, n) in shapes {
            let func = StdFunc::by_name(name).unwrap();
            let every: Vec<usize> = (0..args.len()).collect();
            let choices: Vec<Vec<Vec<usize>>> = if n < 3 {
                vec![vec![every; n]]
            } else {
                func.rows()
                    .filter(|row| func.row_fits(row, n))
                    .map(|row| {
                        (0..n)
                            .map(|j| {
                                let param = row.params.get(j).or(row.params.last()).unwrap();
                                (0..args.len())
                                    .filter(|&a| kind(func, j, a).is_some_and(|k| param.accepts(&k)))
                                    .collect()
                            })
                            .collect()
                    })
                    .collect()
            };
            for per_position in choices {
                let count: usize = per_position.iter().map(Vec::len).product();
                for k in 0..count {
                    let mut rest = k;
                    let picked: Vec<&str> = per_position
                        .iter()
                        .map(|c| {
                            let a = c[rest % c.len()];
                            rest /= c.len();
                            args[a]
                        })
                        .collect();
                    calls.insert(format!("{name}({})", picked.join(", ")));
                }
            }
        }
        srcs.extend(calls);

        let panicked: Vec<String> = srcs
            .into_iter()
            .filter(|src| {
                let Ok(e) = parse_expr(src) else { return false };
                let Ok(ir) = resolve(&e, &mut t, &mut ResourceBuilder::default()) else {
                    return false;
                };
                let run = std::panic::AssertUnwindSafe(|| eval_rows(&ir, &t, &fields, |_| ()));
                std::panic::catch_unwind(run).is_err_and(|p| !p.is::<crate::core::buffer::OverBudget>())
            })
            .collect();
        assert!(
            panicked.is_empty(),
            "these compile, then panic at run time: {panicked:#?}"
        );
    }

    #[test]
    fn length_of_a_number_is_a_compile_error() {
        let mut t = with_fields(&[("n", DataType::I64)]);
        let e = lower("length(n)", &mut t).unwrap_err();
        assert!(e.message.contains("length"), "{}", e.message);
    }

    #[test]
    fn slice_rejects_a_non_i64_bound() {
        let mut t = with_fields(&[("s", DataType::Str)]);
        assert!(lower(r#"slice(s, "a")"#, &mut t).is_err());
        assert!(lower("slice(s, 1, 2.0)", &mut t).is_err());
    }

    #[test]
    fn string_predicates_reject_a_non_text_operand() {
        let mut t = with_fields(&[("s", DataType::Str), ("n", DataType::I64)]);
        assert!(lower("indexOf(s, n)", &mut t).is_err());
        assert!(lower("startsWith(n, s)", &mut t).is_err());
        assert!(lower("includes(s, n)", &mut t).is_err());
    }

    #[test]
    fn parse_time_takes_no_text_as_its_reference() {
        let mut t = with_fields(&[("s", DataType::Str), ("reference", DataType::DateTime)]);
        assert!(lower("parseTime(s, s, reference)", &mut t).is_ok());
        assert!(lower("parseTime(s, s, s)", &mut t).is_err());
    }

    #[test]
    fn parse_time_rejects_a_non_text_argument() {
        let mut t = with_fields(&[("n", DataType::I64)]);
        assert!(lower("parseTime(n)", &mut t).is_err());
    }

    #[test]
    fn abs_rejects_a_non_numeric_argument() {
        let mut t = with_fields(&[("s", DataType::Str)]);
        assert!(lower("abs(s)", &mut t).is_err());
    }

    #[test]
    fn min_rejects_a_non_numeric_argument() {
        let mut t = with_fields(&[("i", DataType::I64), ("s", DataType::Str)]);
        assert!(lower("min(i, s)", &mut t).is_err());
    }

    #[test]
    fn a_function_with_no_parameters_resolves_on_no_arguments() {
        let mut t = table();
        assert_eq!(lower("now()", &mut t).unwrap().ty, DataType::DateTime);
        let e = lower("now(1)", &mut t).unwrap_err();
        assert!(e.message.contains("takes 0 arguments, found 1"), "{}", e.message);
    }

    /// The error names every signature, in the words the table uses.
    #[test]
    fn a_type_error_lists_the_signatures() {
        let mut t = with_fields(&[("n", DataType::I64)]);
        let e = lower("parseInt(n)", &mut t).unwrap_err();
        assert_eq!(
            e.message,
            "`parseInt` is not defined for (i64); it takes (str) or (str, i64)"
        );
        let e = lower("parseInt(n, n, n)", &mut t).unwrap_err();
        assert_eq!(e.message, "`parseInt` takes 1 or 2 arguments, found 3");
    }

    #[test]
    fn split_takes_a_string_or_a_regex_in_the_same_position() {
        let mut t = with_fields(&[("s", DataType::Str)]);
        let arr = DataType::Array(Box::new(DataType::Str));
        assert_eq!(lower("split(s, \",\")", &mut t).unwrap().ty, arr);
        assert_eq!(lower("split(s, /,\\s*/)", &mut t).unwrap().ty, arr);
        assert_eq!(lower("split(s, s)", &mut t).unwrap().ty, arr);
    }

    #[test]
    fn match_rejects_a_non_text_value() {
        let mut t = with_fields(&[("n", DataType::I64)]);
        let e = lower("match(n, /x/)", &mut t).unwrap_err();
        assert!(e.message.contains("(str, /pattern/)"), "{}", e.message);
    }

    #[test]
    fn a_text_function_refuses_bytes() {
        let mut t = with_fields(&[("b", DataType::Bytes)]);
        let e = lower("trim(b)", &mut t).unwrap_err();
        assert!(e.message.contains("(str)"), "{}", e.message);
        assert_eq!(lower("trim(decode(b, \"utf-8\"))", &mut t).unwrap().ty, DataType::Str);
    }

    #[test]
    fn matchs_pattern_argument_must_be_a_literal_regex() {
        let mut t = with_fields(&[("s", DataType::Str), ("p", DataType::Str)]);
        let e = lower("match(s, p)", &mut t).unwrap_err();
        assert!(e.message.contains("literal"), "{}", e.message);
    }

    // -- method-call syntax ---------------------------------------------------

    #[test]
    fn method_call_on_a_bare_field_desugars_via_the_dotted_name_split() {
        // `x.length()` collapses to one lexer token ("x.length") before the parser ever sees a `.` — resolve_call has
        // to split it back apart.
        let mut t = with_fields(&[("x", DataType::Str)]);
        let ir = lower("x.length()", &mut t).unwrap();
        assert_eq!(ir.ty, DataType::I64);
        let IrKind::Call { func, args } = &ir.kind else {
            panic!("expected a Call, got {:?}", ir.kind);
        };
        assert_eq!(*func, StdFunc::Length);
        assert_eq!(args.len(), 1, "the receiver becomes the sole argument");
    }

    #[test]
    fn method_call_on_a_nested_field_splits_at_the_last_dot() {
        let mut t = with_fields(&[("source.ip", DataType::Str)]);
        let ir = lower("source.ip.trim()", &mut t).unwrap();
        assert_eq!(ir.ty, DataType::Str);
        assert!(matches!(
            ir.kind,
            IrKind::Call {
                func: StdFunc::Trim,
                ..
            }
        ));
    }

    #[test]
    fn method_call_on_a_dotted_receiver_naming_an_unknown_method_is_an_error() {
        let mut t = with_fields(&[("x", DataType::Str)]);
        let e = lower("x.notAFunction()", &mut t).unwrap_err();
        assert!(e.message.contains("not implemented"), "{}", e.message);
    }

    /// Moved from the parser to the type checker: dot on an expression is member access now, so a forgotten `()` is no
    /// longer a syntax error. It must still be rejected, and the message has to name the real mistake rather than only
    /// complaining about indexing.
    #[test]
    fn a_method_call_missing_its_parens_is_a_type_error() {
        let mut t = with_fields(&[("x", DataType::Str)]);
        let e = lower(r#""hi".trim"#, &mut t).unwrap_err();
        assert!(e.message.contains("cannot index"), "{}", e.message);
        assert!(
            e.message.contains("add parentheses: `.trim()`"),
            "the error should suggest the parentheses, got: {}",
            e.message
        );
    }

    // -- DateTime part extraction ---------------------------------------------

    /// A route condition, a deliver rule's `when` and a capture filter expand no functions, so `map` has nowhere to run.
    #[test]
    fn map_and_filter_are_refused_outside_a_code_block() {
        let mut t = with_fields(&[("xs", DataType::Array(Box::new(DataType::I64)))]);
        let e = parse_expr("length(xs.filter(x => x > 1)) > 0").unwrap();
        let err = resolve(&e, &mut t, &mut ResourceBuilder::default()).unwrap_err();
        assert!(err.message.contains("code block"), "{}", err.message);
    }

    #[test]
    fn datetime_part_rejects_a_non_datetime_argument() {
        let mut t = with_fields(&[("n", DataType::I64)]);
        assert!(lower("year(n)", &mut t).is_err());
    }

    fn r(op: BinOp, a: DataType, b: DataType) -> Result<DataType, String> {
        binop_result(op, &a, &b)
    }

    /// An `i64` met by an `f64` widens to `f64`; `/` is `f64`. Text and time keep their operator's type.
    #[test]
    fn widening_rule() {
        use DataType::*;
        assert_eq!(r(BinOp::Add, I64, I64).unwrap(), I64);
        assert_eq!(r(BinOp::Add, I64, F64).unwrap(), F64);
        assert_eq!(r(BinOp::Add, F64, I64).unwrap(), F64);
        assert_eq!(r(BinOp::Div, I64, I64).unwrap(), F64);
        assert_eq!(r(BinOp::Lt, I64, F64).unwrap(), Bool);
        assert_eq!(r(BinOp::Mul, Duration, F64).unwrap(), Duration);
        assert_eq!(r(BinOp::Add, Str, Str).unwrap(), Str);
        assert_eq!(r(BinOp::Sub, DateTime, DateTime).unwrap(), Duration);
        assert_eq!(r(BinOp::Add, DateTime, Duration).unwrap(), DateTime);
        assert!(r(BinOp::Add, Str, I64).is_err());
    }

    /// An `i64` beside an `f64` widens to it, at any depth; an `f64` never narrows to `i64`.
    #[test]
    fn the_i64_side_widens_to_f64() {
        use DataType::*;
        assert!(widens(&I64, &F64));
        assert!(!widens(&F64, &I64));
        assert!(widens(&Array(Box::new(I64)), &Array(Box::new(F64))));
        assert!(binop_result(BinOp::BitAnd, &F64, &I64).is_err());
        assert!(unop_result(UnOp::BitNot, &F64).is_err());
        assert_eq!(unop_result(UnOp::Neg, &I64), Ok(I64));
    }

    #[test]
    fn null_unifies_with_anything_and_only_null_plus_null_stays_null() {
        use DataType::*;
        assert_eq!(r(BinOp::Add, Null, I64).unwrap(), I64);
        assert_eq!(r(BinOp::Add, Str, Null).unwrap(), Str);
        assert_eq!(r(BinOp::Add, Null, Null).unwrap(), Null);
        assert_eq!(r(BinOp::Coalesce, Null, Null).unwrap(), Null);
        // A comparison and a logical operator are Bool whatever their operands are.
        assert_eq!(r(BinOp::Eq, I64, Null).unwrap(), Bool);
        assert_eq!(r(BinOp::Lt, Null, Str).unwrap(), Bool);
        for op in [BinOp::Eq, BinOp::Ne, BinOp::Lt, BinOp::Ge, BinOp::And, BinOp::Or] {
            assert_eq!(r(op, Null, Null).unwrap(), Bool, "{op:?}");
        }
        assert_eq!(unop_result(UnOp::Not, &Null), Ok(Bool));
        assert_eq!(unop_result(UnOp::Neg, &Null), Ok(Null));
    }

    /// A variant only moves. An operator refuses it but `??`, which reads presence; a blend carries it and stays a
    /// variant.
    #[test]
    fn a_variant_moves_and_nothing_else() {
        use DataType::*;
        for op in [BinOp::Add, BinOp::Eq, BinOp::And] {
            let e = r(op, Variant, I64).unwrap_err();
            assert!(e.contains("give it a type first"), "{op:?}: {e}");
        }
        assert_eq!(r(BinOp::Coalesce, Variant, Str).unwrap(), Variant);
        assert!(unop_result(UnOp::Not, &Variant).is_err());
        assert_eq!(ternary_result(&Bool, &Variant, &I64).unwrap(), Variant);
        assert!(ternary_result(&Variant, &I64, &I64).is_err());
    }

    /// `[]` is `array<null>`: an operator takes it as the array it meets, and it meets nothing but an array.
    #[test]
    fn an_empty_array_takes_the_array_type_it_meets() {
        use DataType::*;
        let e = Array(Box::new(Null));
        let strs = Array(Box::new(Str));
        assert_eq!(r(BinOp::Add, e.clone(), strs.clone()).unwrap(), strs);
        assert_eq!(r(BinOp::Add, e.clone(), Null).unwrap(), e);
        assert!(r(BinOp::Add, e.clone(), I64).is_err());
        assert!(r(BinOp::Eq, e.clone(), e.clone()).is_err());
        assert!(unop_result(UnOp::Not, &e).is_err());
        assert_eq!(
            r(BinOp::Add, e.clone(), Array(Box::new(F64))).unwrap(),
            Array(Box::new(F64))
        );
    }

    /// A blend's branches meet as `unify` says, and a pair that does not meet is a variant.
    #[test]
    fn ternary_needs_a_bool_condition_and_its_branches_meet_or_blend_as_a_variant() {
        use DataType::*;
        assert!(ternary_result(&Str, &I64, &I64).is_err());
        assert_eq!(ternary_result(&Bool, &I64, &F64).unwrap(), F64);
        assert_eq!(ternary_result(&Bool, &Str, &I64).unwrap(), Variant);
        assert_eq!(r(BinOp::Coalesce, Str, I64).unwrap(), Variant);
    }

    /// The widening is in the IR: the `i64` side of a blend is coerced to `f64`, and a blend of types that do not
    /// meet is a variant with both sides as they are.
    #[test]
    fn a_blend_widens_its_i64_side_in_the_ir() {
        let mut t = with_fields(&[
            ("c", DataType::Bool),
            ("s", DataType::Str),
            ("n", DataType::I64),
            ("f", DataType::F64),
        ]);
        let is_widened = |ir: &Ir| matches!(&ir.kind, IrKind::Coerce { to, value } if *to == DataType::F64 && value.ty == DataType::I64);
        let ir = lower("n ?? f", &mut t).unwrap();
        assert_eq!(ir.ty, DataType::F64);
        let IrKind::Binary { lhs, rhs, .. } = &ir.kind else {
            panic!("{ir:?}")
        };
        assert!(is_widened(lhs) && rhs.ty == DataType::F64, "{ir:?}");
        let ir = lower("c ? f : n", &mut t).unwrap();
        assert_eq!(ir.ty, DataType::F64);
        let IrKind::Ternary { then, else_, .. } = &ir.kind else {
            panic!("{ir:?}")
        };
        assert!(then.ty == DataType::F64 && is_widened(else_), "{ir:?}");
        let ir = lower("c ? s : n", &mut t).unwrap();
        assert_eq!(ir.ty, DataType::Variant);
        let IrKind::Ternary { then, else_, .. } = &ir.kind else {
            panic!("{ir:?}")
        };
        assert!(then.ty == DataType::Str && else_.ty == DataType::I64, "{ir:?}");
        let ir = lower("[n, f]", &mut t).unwrap();
        let IrKind::Array(items) = &ir.kind else {
            panic!("{ir:?}")
        };
        assert!(is_widened(&items[0]), "{ir:?}");
        let ir = lower("min(n, f)", &mut t).unwrap();
        assert_eq!(ir.ty, DataType::F64);
        let IrKind::Call { args, .. } = &ir.kind else {
            panic!("{ir:?}")
        };
        assert!(is_widened(&args[0]), "{ir:?}");
    }

    /// `idiv` of two `i64` is an `i64`; beside an `f64`, the `i64` widens and the quotient is an `f64`.
    #[test]
    fn idiv_widens_as_percent_does() {
        let mut t = with_fields(&[("n", DataType::I64), ("f", DataType::F64)]);
        assert_eq!(lower("idiv(n, n)", &mut t).unwrap().ty, DataType::I64);
        assert_eq!(lower("idiv(n, f)", &mut t).unwrap().ty, DataType::F64);
    }

    #[test]
    fn every_binop_variant_has_a_table_row() {
        use DataType::*;
        let probes = [I64, F64, Bool, Str, DateTime, Duration];
        for op in BinOp::ALL {
            let ok = probes
                .iter()
                .any(|a| probes.iter().any(|b| r(op, a.clone(), b.clone()).is_ok()));
            assert!(ok, "{op:?} has no valid operand pair");
        }
    }
}
