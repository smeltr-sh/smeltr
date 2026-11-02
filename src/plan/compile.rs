//! The compiler's one pass, which binds each name to a slot and emits the instructions over those slots, and the
//! `Plan` type.
//!
//! Conditionals are predication, not control flow. A guard is a Bool column; an `Eval` of `x` under it compiles to `x'
//! = guard ? f(y) : x`, a `Drop` to `live' = live && !(c && guard)`, and a `Parser`'s declared fields the same way an
//! `Eval` assignment's are -- parsed unconditionally into fresh slots, then blended into each name's prior value.
//! `Unroll` is a compile error inside a conditional: it changes how many rows there are, and a row the guard does not
//! take has no element.

use std::collections::{HashMap, HashSet};

use crate::core::column::DataType;
use crate::core::error::ExceptionKind;
use crate::core::id::{FunctionId, SlotId};
use crate::expr::ast::{Catch, Expr, ExprKind, Region, Site, Span, Stmt, Value};
use crate::expr::ir::{Ir, IrKind, Layer, flatten};
use crate::expr::parse::parse_block_shaped;
use crate::ops::BinOp;
use crate::plan::binding::{self, Source};
use crate::plan::config::{
    CompileError, DeclaredField, FunctionKind, GrokPatterns, LookupOutput, LookupSource, MatchKind, Multiplicity,
    ParserKind, PipelineConfig, Step, TimeSource,
};
use crate::plan::lower;
use crate::plan::parse_call::{ParseCall, matcher_keys};
use crate::plan::resolve::{
    Origin, ResourceBuilder, Scope, SlotOrigin, SymbolTable, bind_element, binop_result, blend, coerce_ir, coercible,
    describe_expr, fits, holds_null, is_condition, lay_key, member_ir, object_key, resolve, source_ir, split_struct,
    unclaimed_ir, variant_use, widen_to,
};
use crate::plan::topology::{Placement, SourceMeta};
use crate::plan::tree::{self, Node, Object, Place};

/// The error for a declaration of `name`, a local.
fn local_declared(name: &str) -> String {
    format!("`{name}` is a local; its type is its value's")
}

/// Parse an expression's source, the error naming the step it belongs to.
pub fn parse(src: &str, fid: Option<FunctionId>) -> Result<Expr, CompileError> {
    crate::expr::parse::parse_expr(src)
        .map_err(|e| CompileError::new(fid, format!("parse error in `{src}`: {}", e.message)))
}

/// The names the runtime fills, in slot order: `core::batch`'s prefix, the record's fields, then the source's
/// `__source` fields.
pub fn ingest_fields(meta: &SourceMeta) -> Vec<(&str, DataType)> {
    let mut fields: Vec<(&str, DataType)> = prefix_names().collect();
    fields.extend(meta.record_fields());
    fields.extend(meta.source_fields().iter().map(|(n, t)| (*n, t.clone())));
    fields
}

/// `core::batch`'s prefix names, in slot order. The array length is `PREFIX_SLOTS`, so the prefix and its layout
/// cannot drift.
fn prefix_names() -> impl Iterator<Item = (&'static str, DataType)> {
    const NAMES: [&str; crate::core::batch::PREFIX_SLOTS] = ["__live"];
    NAMES.into_iter().zip(crate::core::batch::prefix_layout())
}

/// The reserved names, each with its layout's type. The runtime reads each back after the run at that type, so each
/// stays bound, and at that type, to the end of a pipeline.
fn reserved_names() -> impl Iterator<Item = (&'static str, DataType)> {
    prefix_names().take(crate::core::batch::RESERVED_SLOTS)
}

#[derive(Clone, Debug)]
pub struct Instr {
    pub op: Op,
    pub out: SlotId,
    pub out_ty: DataType,
    /// The rows this instruction's result is for. A row barrier's row loop skips the rest; a vectorized kernel runs
    /// on every row and has its guard compiled into a blend, and what it notes on a masked row is retracted after
    /// it (`plan::exec::retract_masked`). Set on a row barrier and on an instruction lifted ahead of the guarded
    /// expression that reads it; only that expression reads it, so where the guard takes no row it is not computed.
    pub guard: Option<SlotId>,
    /// Filled by liveness: this slot is dead, so reuse its buffers.
    pub reuse: Option<SlotId>,
    /// Filled by liveness: no instruction reads this instruction's output, so a bare literal may stay a constant
    /// column here -- nothing between this slot and the serializer asks for its rows. Every accessor refuses an
    /// unexpanded constant, so this is what keeps one out of a reader that would be refused. It rests on the same
    /// reader set `reuse` rests on; a slot liveness did not see read is already a slot it would hand away.
    pub constant_ok: bool,
    pub function: FunctionId,
}

impl Instr {
    /// Every slot this instruction reads from the batch it runs on, its guard included, a slot possibly more than
    /// once. A loop's body runs on the loop's wave, so what it reads counts here, less the slots the loop fills itself.
    pub fn reads(&self, f: &mut impl FnMut(SlotId)) {
        if let Some(g) = self.guard {
            f(g);
        }
        match &self.op {
            Op::Eval { ir } => walk_slots(ir, f),
            Op::Call { input, guard, call, .. } => {
                f(*input);
                guard.iter().copied().for_each(&mut *f);
                if let Call::Parse { objects, .. } = call {
                    objects.iter().filter_map(|o| o.under).for_each(&mut *f);
                }
            }
            Op::Loop(l) => {
                l.array.iter().for_each(|&(array, _)| f(array));
                l.accumulators.iter().filter_map(|(r, _)| *r).for_each(&mut *f);
                l.wave_reads().into_iter().for_each(f);
            }
            Op::Begin { mask, .. } => mask.iter().for_each(|m| walk_slots(m, f)),
            // The region's mask lives until its end: a row may leave it until then.
            Op::End { mask } => f(*mask),
            Op::Leave { rows, .. } => f(*rows),
            Op::ArrayFold(l) => {
                f(l.array);
                l.folds.iter().for_each(|fold| f(fold.seed));
                l.element_reads().into_iter().for_each(f);
            }
            // `clear` slots are released here, not read.
            Op::Unroll { array, live, .. } => {
                f(*array);
                f(*live);
            }
            Op::Try(TryStep::Throw { caught, rows, .. }) => {
                f(*caught);
                rows.iter().copied().for_each(f);
            }
            Op::Try(_) => {}
        }
    }
}

impl Loop {
    /// What a trip's wave takes from the loop's batch besides the element and the accumulators.
    pub fn wave_reads(&self) -> Vec<SlotId> {
        let own = |s: SlotId| {
            self.array.is_some_and(|(_, element)| s == element) || self.accumulators.iter().any(|(r, _)| *r == Some(s))
        };
        reads_of(&self.body, own)
    }
}

/// Every slot `instrs` read, once each, less those `own` claims.
fn reads_of(instrs: &[Instr], own: impl Fn(SlotId) -> bool) -> Vec<SlotId> {
    let mut out = Vec::new();
    for instr in instrs {
        instr.reads(&mut |s| {
            if !own(s) && !out.contains(&s) {
                out.push(s)
            }
        });
    }
    out
}

/// One output of an `Op::Call`, with its slot.
#[derive(Clone, Debug)]
pub struct Output {
    /// What the call knows it by: a parse's key under its target, a lookup's column or record path.
    pub name: String,
    pub slot: SlotId,
    /// The declared type. A lookup under `Multiplicity::All` holds an array of it.
    pub ty: DataType,
    /// `Some` only for a declared `datetime`: how its text reads.
    pub time_source: Option<TimeSource>,
}

/// An object a dynamic parser lays the keys it does not declare into. Each key goes to the deepest object at its
/// name, and the rest of its name is its key there.
#[derive(Clone, Debug)]
pub struct ParseObject {
    /// The object's name, `""` for the record.
    pub name: String,
    /// Its unclaimed fields before the parse, which the keys it finds are laid over. `None` where the object has none,
    /// or the parse declares it new and so replaces it.
    pub under: Option<SlotId>,
    /// Where its unclaimed fields after the parse go.
    pub out: SlotId,
    /// Whether it is declared closed: a key under it that no field takes is dropped.
    pub closed: bool,
}

/// What an `Op::Call` runs.
#[derive(Clone, Debug)]
pub enum Call {
    Parse {
        kind: ParserKind,
        /// A Grok matcher compiled once at plan-compile time.
        matcher: Option<std::sync::Arc<crate::format::grok_scan::Matcher>>,
        /// Where a dynamic parser lays the keys it does not declare, the record first. Empty for a static parser.
        objects: Vec<ParseObject>,
    },
    Lookup(crate::lookup::Lookup),
}

#[derive(Clone, Debug)]
pub enum Op {
    Eval {
        ir: Ir,
    },
    /// A call that reads `input` and writes each of `outputs` to its own slot; the instruction's own `out` is a
    /// throwaway marker. It skips a row `guard` masks: its outputs are null there, and the blend that follows
    /// keeps the prior. Not `Instr::guard`, which marks a row barrier.
    Call {
        input: SlotId,
        guard: Option<SlotId>,
        outputs: Vec<Output>,
        call: Call,
    },
    /// `for` over a real array whose trips depend on one another (`independent`), or a `while`. A row barrier, run as
    /// a wavefront (design doc, Compiler).
    Loop(Loop),
    /// A `for` whose trips do not depend on one another, vectorized.
    ArrayFold(ArrayFold),
    /// Replace the batch with one row per element of `array`. `live` is the `__live` binding at this point; `clear`
    /// names the user-visible slots released instead of duplicated (`keep_parent_fields: false`).
    Unroll {
        array: SlotId,
        live: SlotId,
        clear: Vec<SlotId>,
    },
    /// Opens a region on the rows `mask` selects, every row without one, and writes its mask: the guard of what the
    /// region holds, whose bits clear as rows leave it.
    Begin {
        region: Region,
        mask: Option<Ir>,
    },
    /// Closes the innermost region, `mask` its mask's slot.
    End {
        mask: SlotId,
    },
    /// The rows `rows` selects leave every region out to the nearest `to`.
    Leave {
        to: Region,
        rows: SlotId,
    },
    /// One step of a `try`, before its region ends.
    Try(TryStep),
}

/// Each `(read_slot, write_slot)` is one accumulator: a name the body binds outside the loop variable. `read_slot` is
/// its slot before the loop (`None` if never bound); `write_slot` is where the body's last assignment lands, fed back as
/// the next trip's `read_slot` and holding the row's final value.
pub type Accumulator = (Option<SlotId>, SlotId);

#[derive(Clone, Debug)]
pub struct Loop {
    /// The array a `for` walks and its element's slot. A `while` has none: its body leaves at its condition.
    pub array: Option<(SlotId, SlotId)>,
    pub accumulators: Vec<Accumulator>,
    pub body: Vec<Instr>,
    /// The mask of the loop's region, in the wave, where its body can leave it: a row whose bit a trip cleared is
    /// done.
    pub stay: Option<SlotId>,
    /// How long a loop with no array may run. One with an array ends with it.
    pub budget: Option<std::time::Duration>,
}

/// A `for` whose trips do not depend on one another (`independent`): `body` runs once over every element of every row,
/// in one batch of them, then each of `folds` folds its row's elements.
#[derive(Clone, Debug)]
pub struct ArrayFold {
    pub array: SlotId,
    pub element_slot: SlotId,
    pub body: Vec<Instr>,
    pub folds: Vec<Fold>,
}

impl ArrayFold {
    /// What the elements' batch takes from the loop's besides the element.
    pub fn element_reads(&self) -> Vec<SlotId> {
        reads_of(&self.body, |s| s == self.element_slot)
    }
}

/// One accumulator `Op::ArrayFold` writes: the values `value` holds on the elements `ran` selects, every element
/// without it, folded with `op` onto `seed`, its slot before the loop, into `out`.
#[derive(Clone, Debug)]
pub struct Fold {
    pub op: FoldOp,
    pub seed: SlotId,
    pub value: SlotId,
    pub ran: Option<SlotId>,
    pub out: SlotId,
}

/// The steps a `try` with a `catch` runs after its body, inside its region.
#[derive(Clone, Debug)]
pub enum TryStep {
    /// Writes each row's first exception since the `try` began of one of `kinds`, by position in `ExceptionKind::KINDS`,
    /// by its name: null on a row the `catch` does not take. Both this and `Caught` pass over the rows `rows` no longer
    /// selects: they left before the `catch` (an earlier `catch`'s `continue`), and keep their exceptions.
    Kind { kinds: Vec<usize>, rows: Option<SlotId> },
    /// Clears the exceptions since the `try` began on every marked row, keeping them for a `Throw` where `keep`, and
    /// writes the text of each row's exceptions of `kinds`. `names` names the fields a text cites; filled once the plan
    /// is complete.
    Caught {
        names: std::sync::Arc<HashMap<SlotId, String>>,
        kinds: Vec<usize>,
        keep: bool,
        rows: Option<SlotId>,
    },
    /// The rows `rows` selects take back the exceptions the `Caught` writing `caught` cleared, but those of `handled`.
    /// After its `try`'s end.
    Throw {
        caught: SlotId,
        rows: Option<SlotId>,
        handled: Vec<usize>,
    },
}

/// The operators `Op::ArrayFold` accepts: associative and commutative, so the fold order is free. `Last` is a plain
/// overwrite, and `Push` appends each element's value, in order.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum FoldOp {
    Add,
    Mul,
    And,
    Or,
    Last,
    Push,
}

#[derive(Debug)]
pub struct Plan {
    /// `PipelineConfig::id`: this pipeline's position in the bundle, and the `pipeline` label on every metric about
    /// it. Positional here and resolved to the author's own id at the wire, the same as a source or a destination --
    /// the executor works in integers, and the position does not survive a config edit that reorders or deletes an
    /// unrelated pipeline (`worker::resolve_subject_labels`).
    pub id: usize,
    /// `PipelineConfig::name`: what a person reading a block reason knows this pipeline by.
    pub name: String,
    /// `PipelineConfig::step_ids`, indexed by `FunctionId`: the step an exception came from, as its author named it.
    pub step_ids: Vec<String>,
    /// `id` rendered, because it is written as a metric label on every batch and building it there would allocate
    /// per batch on every transform thread. Compiled once, like everything else here.
    pub id_label: String,
    pub slot_layout: Vec<DataType>,
    pub instrs: Vec<Instr>,
    /// Where each slot came from, indexed by slot: entry `s` describes slot `s`, one entry for every slot in
    /// `slot_layout`. Keeps every binding the compiler made, superseded ones and compiler temporaries alike.
    ///
    /// A compiler temporary is bound to no name. Config cannot reach it, and it is never a leaf of `tree`, so the
    /// serializer never writes it. The name it shows here is for reading the plan.
    pub provenance: Vec<SlotOrigin>,
    /// Each copy that shares its value's slot under another name, which `provenance` does not see: the slot, the name,
    /// how many slots there were when it was made, and the step that made it.
    pub aliases: Vec<(SlotId, String, usize, FunctionId)>,
    pub patterns: Vec<crate::expr::ast::PatternSrc>,
    pub warnings: Vec<CompileError>,
    /// Instruction indices that are row barriers. Declared by the function, never discovered from the data: finding
    /// one at runtime would cost roughly a thousandfold over the vectorized path while looking identical in the config.
    pub barriers: Vec<usize>,
    /// The name tree at the end of compilation (`plan::tree`): the record the serializer writes. Ingest names and
    /// `__`-prefixed control fields are in it; the serializer hides them.
    pub tree: Object,
    /// `__live`'s final slot.
    pub live_slot: SlotId,
    /// Each `Placement`'s final slot, in `Placement::ALL` order, if the pipeline binds it.
    pub placement_slots: [Option<SlotId>; Placement::ALL.len()],
    /// `__destination.id`'s final slot, if the pipeline binds it.
    pub id_out_slot: Option<SlotId>,
    /// Whether any instruction reads the source's metadata field `i`; the runtime skips building that column otherwise.
    /// Indices line up with the `SourceMeta` this plan was compiled against.
    pub reads_meta: Vec<bool>,
    /// How many slots the runtime fills before the first instruction (`ingest_fields`): the prefix, the record and
    /// the `__source` fields.
    pub ingest_slots: usize,
    /// `patterns`, compiled. Indices line up with `patterns`.
    pub compiled_patterns: Vec<crate::core::ctx::CompiledPattern>,
}

impl Plan {
    /// `slot_layout` with the tag's column and the origin's appended: the layout the runtime builds its batch with.
    /// No instruction reads or writes them.
    pub fn delivery_layout(&self) -> Vec<DataType> {
        let mut layout = self.slot_layout.clone();
        layout.extend([DataType::Variant, DataType::I64]);
        layout
    }

    /// Each row's record, by its index in the batch the run began with. Written before the first step, so an
    /// `Unroll` carries it to every row it makes.
    pub fn origin_slot(&self) -> SlotId {
        self.slot_layout.len() + 1
    }

    /// Each slot a field holds, by the name a row's exception cites. A copy shares its source's slot, so a slot may
    /// have several names: the one it was made for wins while it still holds the slot.
    pub fn slot_names(&self) -> HashMap<SlotId, String> {
        let mut names: HashMap<SlotId, String> = HashMap::new();
        for (name, slot) in self.tree.leaves() {
            let own = self.provenance.get(slot).is_some_and(|o| o.name == name);
            if own || !names.contains_key(&slot) {
                names.insert(slot, name);
            }
        }
        // A value computed whole before its pieces are bound is named for the name it was computed for.
        for o in &self.provenance {
            if !names.contains_key(&o.slot) && !o.name.starts_with('<') {
                names.insert(o.slot, o.name.clone());
            }
        }
        // A local the inliner renamed is named as the user wrote it. An inlined function's temporaries carry no name a
        // user wrote.
        for n in names.values_mut() {
            *n = crate::expr::inline::user_names(n).into_owned();
        }
        names.retain(|_, n| !crate::expr::inline::RESERVED_PREFIXES.iter().any(|p| n.starts_with(p)));
        names
    }

    /// The tag field's column: what a route's `when` reads and the serializer writes, under the route's tag field,
    /// beside the pipeline's own fields.
    pub fn tag_slot(&self) -> SlotId {
        self.slot_layout.len()
    }
}

/// Where a parse or a lookup writes: `target`, `""` for the record, its declared `ty` where it has one, and whether the
/// write merges into it or replaces it (`Compiler::parse_into`).
#[derive(Clone, Copy)]
struct Dest<'a> {
    target: &'a str,
    ty: Option<&'a DataType>,
    merge: bool,
}

/// Where the compiler is emitting: the rows what it emits runs on, as `Instr::guard`, and the function it belongs to,
/// as `Instr::function`. A function that opens a region of its own takes the guard around it apart, as `outer`.
#[derive(Clone, Copy)]
struct Here {
    guard: Option<SlotId>,
    id: FunctionId,
}

#[derive(Clone)]
struct Compiler<'p> {
    /// The bundle's lookup tables, by id (`PipelineConfig::lookup_tables`).
    tables: &'p HashMap<String, Result<LookupSource, String>>,
    /// The bundle's grok patterns (`PipelineConfig::grok_patterns`).
    grok_patterns: &'p GrokPatterns,
    syms: SymbolTable,
    res: ResourceBuilder,
    instrs: Vec<Instr>,
    errors: Vec<CompileError>,
    /// The instruction lists the loop bodies being compiled are apart from, outermost first: `outer[i]` is the list
    /// the loop at depth `i` goes into. Empty outside a loop. A body's assignment leaves the old value standing where
    /// the loop ran no trips, so what it replaces is blended to null rather than unbound.
    outer: Vec<Vec<Instr>>,
    /// The first slot each loop being compiled made, outermost first. A slot before `loop_firsts[i]` was bound before
    /// the loop at depth `i`.
    loop_firsts: Vec<SlotId>,
    /// How many locals were open when each code block being compiled began, outermost first: a local at or past the
    /// last is the block's own.
    scope_marks: Vec<usize>,
    /// Where each open `catch` variable stands among the open locals (`local_index`). A name whose own local is one is
    /// a caught exception; a local opened inside the `catch` with its name is another binding.
    catch_scopes: Vec<usize>,
    /// Each slot `open_path` split ahead of a loop, with the depth of the outermost loop it runs ahead of: the record's
    /// before that loop, not an accumulator of it or of any loop inside it.
    hoisted: HashMap<SlotId, usize>,
    pass: Pass,
    /// The names the runtime fills (`ingest_fields`): no spread writes them.
    ingest: Vec<String>,
    /// How many `try` bodies are being compiled: an `unroll` inside one would renumber the rows its exceptions are on.
    tries: usize,
    /// The regions open where the compiler is, innermost last: what a `break`, a `continue` or a `return` can leave.
    regions: Vec<Region>,
    /// The step's names first written `null` or `[]`, while `settle` compiles it.
    untyped: Option<Untyped>,
    /// The vectorized loops being compiled, innermost last (`array_fold`).
    folding: Vec<Folding>,
    /// What `type_at` asks: the type of an expression where a step begins.
    probe: Option<std::rc::Rc<Probe>>,
}

/// A vectorized loop being compiled: its accumulators, each one's write with the type it binds once compiled, and
/// whether a value had no fold kernel.
#[derive(Clone, Default)]
struct Folding {
    accs: Vec<String>,
    writes: Vec<(String, Fold, DataType)>,
    refused: bool,
}

/// What `settle` knows of the names a step first writes `null` or `[]`, which have no type of their own: each takes
/// the one the writes after it blend to, a variant where they do not meet, as a construct's writes do.
#[derive(Clone, Default)]
struct Untyped {
    /// What each first write's later writes blended to in the rounds before, by its place among the round's first
    /// writes of `null` or `[]`: two blocks may each bind a name of one spelling.
    settled: HashMap<usize, DataType>,
    /// Each first write this round bound with no type, by its place, name and binding (`SymbolTable::binding`), and
    /// what the writes after it blend to so far.
    found: Vec<(usize, String, Option<usize>, DataType)>,
    /// How many first writes of `null` or `[]` this round has met.
    seen: usize,
    /// The last round: a name with no type here is an error.
    last: bool,
}

/// What a lookup reads, and how: a lookup step's settings, or a `lookup` call's.
struct LookupSpec<'s> {
    source: &'s LookupSource,
    match_kind: MatchKind,
    multiplicity: Multiplicity,
    key_column: &'s str,
    outputs: &'s [LookupOutput],
    /// Whether it writes into the record, where a row it finds nothing for, with no default, keeps what each output's
    /// name held (Language).
    merge: bool,
}

/// Where the compiler is with the outermost construct, an `if` or a loop, being compiled (`construct`): in its typing
/// pass, noting each name the construct first writes, in the order met, with the type its writes blend to; or
/// compiling it, with the names it declared ahead of itself, so a declaration of one inside is the declaration the
/// typing pass found, not a redeclaration of a name bound outside.
#[derive(Clone)]
enum Pass {
    Outside,
    Typing(Vec<(String, FirstWrite)>),
    Compiling(Vec<String>),
}

/// What a construct's typing pass learned of a name it first writes: the type every write to it blends to, or the
/// one its declaration states, and how many declarations state one.
#[derive(Clone)]
struct FirstWrite {
    ty: DataType,
    declared: usize,
}

/// A slot's value, resolved: a slot that holds it already, or an expression to evaluate.
enum Resolved {
    Slot(SlotId),
    /// An object's unclaimed fields: the slot, less the keys taken out of it (`tree::Object::removed`).
    Unclaimed(SlotId, Vec<String>),
    Ir(Ir),
}

/// The type `expr` has where step `step` of `p` begins: what the names in it read there, every step before it run.
/// Given a `cut` of a code step, the type of the statement it puts at `cut.at`, read where the cut body has it, its
/// `let`s bound; where the cut body does not compile that far, where the step begins. `None` where it does not
/// resolve, or `step` is not one the pipeline reaches. The pipeline need not compile: an error elsewhere, the
/// half-typed expression the question is about among them, leaves the names before `step` bound.
pub fn type_at(
    p: &PipelineConfig,
    meta: &SourceMeta,
    step: FunctionId,
    expr: &Expr,
    cut: Option<crate::plan::dot::Cut>,
) -> Option<DataType> {
    let ingest = ingest_fields(meta);
    let probe = std::rc::Rc::new(Probe {
        step,
        expr: expr.clone(),
        cut,
        found: std::cell::RefCell::new(None),
    });
    let mut c = Compiler::new(p, &ingest, Some(probe.clone()));
    c.block(&p.body, None);
    probe.found.take()
}

/// What `type_at` asks the compiler, and its answer once the compiler reaches the step. Shared, since a step can be
/// compiled on a copy of the compiler that is thrown away: a later round's answer replaces an earlier one's.
struct Probe {
    step: FunctionId,
    expr: Expr,
    /// Compiled in place of the step's body, where the step is a code step.
    cut: Option<crate::plan::dot::Cut>,
    found: std::cell::RefCell<Option<DataType>>,
}

impl<'p> Compiler<'p> {
    fn new(p: &'p PipelineConfig, ingest: &[(&str, DataType)], probe: Option<std::rc::Rc<Probe>>) -> Compiler<'p> {
        Compiler {
            tables: &p.lookup_tables,
            grok_patterns: &p.grok_patterns,
            ingest: ingest.iter().map(|(n, _)| n.to_string()).collect(),
            tries: 0,
            regions: Vec::new(),
            untyped: None,
            folding: Vec::new(),
            syms: SymbolTable::new(ingest),
            res: ResourceBuilder::default(),
            instrs: Vec::new(),
            errors: Vec::new(),
            outer: Vec::new(),
            loop_firsts: Vec::new(),
            scope_marks: Vec::new(),
            catch_scopes: Vec::new(),
            hoisted: HashMap::new(),
            pass: Pass::Outside,
            probe,
        }
    }

    /// Answer the probe, where `step` is the one it asks about.
    fn answer_probe(&self, step: FunctionId) {
        if let Some(probe) = self.probe.as_ref().filter(|p| p.step == step) {
            self.answer(probe);
        }
    }

    /// The cut body to compile for code step `step`, where the probe asks about it.
    fn probe_cut(&self, step: FunctionId) -> Option<String> {
        let probe = self.probe.as_ref().filter(|p| p.step == step)?;
        probe.cut.as_ref().map(|c| c.body.clone())
    }

    /// Answer the probe again, where `e`, in step `step`, is the statement its cut put there.
    fn answer_cut(&self, e: &Expr, step: FunctionId) {
        let probe = self.probe.as_ref().filter(|p| p.step == step);
        if let Some(probe) = probe.filter(|p| p.cut.as_ref().is_some_and(|c| c.at == e.span.start)) {
            self.answer(probe);
        }
    }

    fn answer(&self, probe: &Probe) {
        let mut syms = self.syms.clone();
        let ty = resolve(&probe.expr, &mut syms, &mut ResourceBuilder::default())
            .ok()
            .map(|ir| ir.ty);
        *probe.found.borrow_mut() = ty;
    }
}

/// Compile one pipeline against the fields `meta` declares: the instructions, in one pass, then slot liveness. Every
/// error is collected rather than the first one returned, so one compile reports everything wrong with a pipeline.
pub fn compile(p: &PipelineConfig, meta: &SourceMeta) -> Result<Plan, Vec<CompileError>> {
    let ingest = ingest_fields(meta);
    let mut c = Compiler::new(p, &ingest, None);

    c.block(&p.body, None);

    // Whatever removed or rebound one on the way -- a `delete`, a Rename, a `move` -- the runtime reads it here.
    for (name, held) in reserved_names() {
        let kept = c.syms.lookup(name).is_some_and(|slot| *c.syms.type_of(slot) == held);
        if !kept {
            c.errors.push(CompileError::new(
                None,
                format!(
                    "`{name}` is smeltr's own and must stay {} to the end of the pipeline",
                    held.spell()
                ),
            ));
        }
    }
    if !c.errors.is_empty() {
        return Err(c.errors);
    }

    let tree = c.syms.root().clone();
    let leaves: Vec<(String, SlotId)> = tree.leaves();
    let live_slot = c.syms.lookup("__live").expect("a reserved name is bound to the end");
    let compiled_patterns = match compile_patterns(&c.res.patterns) {
        Ok(p) => p,
        Err(e) => return Err(vec![e]),
    };

    // Top-level instructions only; a nested loop has no top-level index.
    let barriers = c
        .instrs
        .iter()
        .enumerate()
        .filter(|(_, i)| matches!(i.op, Op::Loop(_)))
        .map(|(idx, _)| idx)
        .collect();

    // A copy shares the source's slot, so a field the serializer writes can be one of these under another name.
    let reads_meta = meta
        .source_fields()
        .iter()
        .enumerate()
        .map(|(i, (own, _))| {
            let slot = meta.source_slot(i);
            any_instr_reads(&c.instrs, slot) || leaves.iter().any(|(name, s)| *s == slot && name != own)
        })
        .collect();
    let mut plan = Plan {
        id: p.id,
        name: p.name.clone(),
        step_ids: p.step_ids.clone(),
        id_label: p.id.to_string(),
        placement_slots: Placement::ALL.map(|p| c.syms.lookup(p.field())),
        id_out_slot: c.syms.lookup("__destination.id"),
        slot_layout: c.syms.layout,
        instrs: c.instrs,
        provenance: c.syms.origins,
        aliases: c.syms.aliases,
        patterns: c.res.patterns,
        warnings: c.syms.warnings,
        barriers,
        tree,
        live_slot,
        reads_meta,
        ingest_slots: ingest.len(),
        compiled_patterns,
    };
    let mut instrs = std::mem::take(&mut plan.instrs);
    let mut names = None;
    name_tries(&mut instrs, &mut || {
        names
            .get_or_insert_with(|| std::sync::Arc::new(plan.slot_names()))
            .clone()
    });
    plan.instrs = instrs;
    let unread = unread_writes(
        &plan,
        &c.syms.local_slots,
        &c.syms.deleted_slots,
        &c.syms.nulls,
        &c.syms.blends,
        &plan.aliases,
    );
    plan.warnings.extend(unread);
    // A case that runs on into the next has the next one's statements copied into its branch, so a warning about
    // them comes twice.
    let mut seen = HashSet::new();
    plan.warnings.retain(|w| seen.insert((w.function, w.message.clone())));
    liveness(&mut plan);
    Ok(plan)
}

/// What a `while`'s condition is called where it is not a `bool`.
const WHILE_CONDITION: &str = "a `while` condition";

/// Whether a condition holds on every row, or on none, whatever the row: a `bool` literal, or a comparison of a side
/// with itself. A side that calls a function is never the same twice, nor is one that may hold an `f64`, which NaN
/// keeps from equalling itself.
fn always(ir: &Ir) -> Option<bool> {
    match &ir.kind {
        IrKind::Literal(Value::Bool(b)) => Some(*b),
        IrKind::Binary { op, lhs, rhs } if same(lhs, rhs) && !may_hold_nan(&lhs.ty) => match op {
            // An ordering with a null side is false, so `x <= x` is not always true.
            BinOp::Eq => Some(true),
            BinOp::Ne | BinOp::Lt | BinOp::Gt => Some(false),
            _ => None,
        },
        _ => None,
    }
}

fn may_hold_nan(ty: &DataType) -> bool {
    !matches!(
        ty,
        DataType::Bool | DataType::I64 | DataType::Str | DataType::Bytes | DataType::DateTime | DataType::Duration
    )
}

/// A condition as a message quotes it: a comparison by its sides and operator.
fn condition_text(e: &Expr) -> String {
    match &e.kind {
        ExprKind::Binary { op, lhs, rhs } => {
            format!("{} {} {}", describe_expr(lhs), op.symbol(), describe_expr(rhs))
        }
        _ => describe_expr(e),
    }
}

/// Whether `a` and `b` read the same value on every row: the same fields and literals, put together the same way.
fn same(a: &Ir, b: &Ir) -> bool {
    match (&a.kind, &b.kind) {
        (IrKind::Field(x), IrKind::Field(y)) => x == y,
        (IrKind::Literal(x), IrKind::Literal(y)) => x == y,
        (IrKind::Binary { op, lhs, rhs }, IrKind::Binary { op: o, lhs: l, rhs: r }) => {
            op == o && same(lhs, l) && same(rhs, r)
        }
        (IrKind::Unary { op, operand }, IrKind::Unary { op: o, operand: x }) => op == o && same(operand, x),
        (IrKind::Index { base, index }, IrKind::Index { base: b, index: i }) => same(base, b) && same(index, i),
        _ => false,
    }
}

/// A warning for each value a statement writes that nothing reads: no instruction reads it and the record does not
/// hold it at the end. A local is never read; a field is written again, or deleted, first. Only an expression's own
/// write counts, under a name code can spell, so a parse's outputs, its blends over a prior included (`blends`), and
/// the engine's own slots are never named. A copy shares its value's slot (`aliases`), so the name a warning gives is
/// the last one to let the value go. Null written where nothing was, as a declaration writes it, loses no value.
fn unread_writes(
    plan: &Plan,
    locals: &HashSet<SlotId>,
    deleted: &HashSet<SlotId>,
    nulls: &HashSet<SlotId>,
    blends: &HashSet<SlotId>,
    aliases: &[(SlotId, String, usize, FunctionId)],
) -> Vec<CompileError> {
    let mut read = HashSet::new();
    every_read(&plan.instrs, &mut |s| {
        read.insert(s);
    });
    let held: HashSet<SlotId> = plan
        .tree
        .leaves()
        .into_iter()
        .chain(plan.tree.unclaimed_slots())
        .map(|(_, s)| s)
        .collect();
    let spelled = |n: &str| !n.is_empty() && !n.starts_with(['<', '#']) && !tree::is_internal(n) && !n.contains('*');
    let mut written = Vec::new();
    every_eval(&plan.instrs, &mut |s| written.push(s));
    let mut out: Vec<CompileError> = Vec::new();
    for slot in written {
        let o = &plan.provenance[slot];
        let Origin::Function(f) = o.origin else { continue };
        if !spelled(&o.name)
            || read.contains(&slot)
            || held.contains(&slot)
            || nulls.contains(&slot)
            || blends.contains(&slot)
        {
            continue;
        }
        // Each name the value was put at, from the slot after which a write of that name replaces it.
        let names: Vec<(&str, usize)> = std::iter::once((o.name.as_str(), slot + 1))
            .chain(
                aliases
                    .iter()
                    .filter(|(s, n, ..)| *s == slot && spelled(n))
                    .map(|(_, n, at, _)| (n.as_str(), *at)),
            )
            .collect();
        let rewritten = names
            .iter()
            .filter_map(|&(name, from)| {
                let again = plan
                    .provenance
                    .get(from..)?
                    .iter()
                    .position(|later| later.name == name)?;
                Some((from + again, name))
            })
            .max();
        let last = names.last().map_or(o.name.as_str(), |&(n, _)| n);
        // A field `$ = {…}` or `unroll` cleared is neither written again nor deleted: nothing is said of it.
        let message = if let Some((_, name)) = rewritten {
            format!("`{name}` is written again before anything reads it")
        } else if locals.contains(&slot) {
            format!("the local `{last}` is never read")
        } else if deleted.contains(&slot) {
            format!("`{last}` is deleted before anything reads it")
        } else {
            continue;
        };
        out.push(CompileError::new(Some(f), message));
    }
    out
}

/// Every slot `instrs` read, those inside a loop's body and those a loop reads back between its trips included.
fn every_read(instrs: &[Instr], f: &mut impl FnMut(SlotId)) {
    for instr in instrs {
        instr.reads(f);
        match &instr.op {
            Op::Loop(l) => {
                every_read(&l.body, f);
                l.accumulators.iter().for_each(|&(_, written)| f(written));
                l.stay.iter().copied().for_each(&mut *f);
            }
            Op::ArrayFold(l) => {
                every_read(&l.body, f);
                for fold in &l.folds {
                    f(fold.value);
                    fold.ran.iter().copied().for_each(&mut *f);
                }
            }
            _ => {}
        }
    }
}

/// The slot each expression in `instrs` writes, inside loops too.
fn every_eval(instrs: &[Instr], f: &mut impl FnMut(SlotId)) {
    for instr in instrs {
        match &instr.op {
            Op::Eval { .. } => f(instr.out),
            Op::Loop(Loop { body, .. }) | Op::ArrayFold(ArrayFold { body, .. }) => every_eval(body, f),
            _ => {}
        }
    }
}

/// How a field of type `ty` reads time text: a `datetime` reads any format the automatic reading knows.
fn auto_time(ty: &DataType) -> Option<TimeSource> {
    (*ty == DataType::DateTime).then_some(TimeSource::Auto)
}

/// Read each of `fields` (name, type, time source) whose key under `target` `formats` names in that format. `Err` says
/// which format names no `datetime` field.
fn read_formats<'f>(
    target: &str,
    mut formats: Vec<(String, String)>,
    fields: impl Iterator<Item = (&'f str, &'f DataType, &'f mut Option<TimeSource>)>,
) -> Result<(), String> {
    for (name, ty, time_source) in fields {
        let Some(at) = formats.iter().position(|(key, _)| tree::join(target, key) == name) else {
            continue;
        };
        let (key, format) = formats.swap_remove(at);
        if *ty != DataType::DateTime {
            return Err(format!(
                "`{key}` is {}; only a `datetime` is read in a format",
                ty.spell()
            ));
        }
        *time_source = Some(TimeSource::Format(format));
    }
    match formats.first() {
        Some((key, _)) => Err(format!(
            "`formats` names `{key}`, which is no field this call writes; declare `{key}: datetime` before it"
        )),
        None => Ok(()),
    }
}

/// Give each `try`'s end the names the text of its exceptions cites, which `names` builds the first time it is asked.
fn name_tries(instrs: &mut [Instr], names: &mut impl FnMut() -> std::sync::Arc<HashMap<SlotId, String>>) {
    for instr in instrs {
        match &mut instr.op {
            Op::Try(TryStep::Caught { names: n, .. }) => *n = names(),
            Op::Loop(Loop { body, .. }) => name_tries(body, names),
            _ => {}
        }
    }
}

/// Compile every interned regex once. `i`/`m`/`s` map onto the `regex` crate; `g` is carried on `CompiledPattern` for
/// `replace`; the other JS flags are accepted and ignored.
pub(crate) fn compile_patterns(
    patterns: &[crate::expr::ast::PatternSrc],
) -> Result<Vec<crate::core::ctx::CompiledPattern>, CompileError> {
    patterns
        .iter()
        .map(|p| {
            let regex = regex::RegexBuilder::new(&p.source)
                .case_insensitive(p.flags.case_insensitive)
                .multi_line(p.flags.multi_line)
                .dot_matches_new_line(p.flags.dot_matches_newline)
                .build()
                .map_err(|e| CompileError::new(None, format!("invalid regex `/{}/`: {e}", p.source)))?;
            Ok(crate::core::ctx::CompiledPattern { regex })
        })
        .collect()
}

/// The accumulators of a `for` body whose trips do not depend on one another, which runs once over every element
/// (`Op::ArrayFold`). Every name the body writes outside its own locals is one: written once, at the body's top level
/// or under `if`s, as `acc = acc ⊕ e` for `+`, `*`, `&&` or `||`, `acc = push(acc, e)` or `acc = e`, and read
/// nowhere else in the body. Nothing in the body may leave the loop, and a loop inside it writes only the body's
/// locals. Any other body is `None`, a wavefront.
fn independent<'a>(body: &'a [Stmt], var: &'a str, syms: &SymbolTable) -> Option<Vec<&'a str>> {
    #[derive(Default)]
    struct Walk<'a> {
        accs: Vec<&'a str>,
        reads: Vec<String>,
        locals: Vec<&'a str>,
    }
    fn read<'a>(e: &'a Expr, w: &mut Walk<'a>) {
        e.find_map(&mut |e| {
            match &e.kind {
                ExprKind::Field(n) => w.reads.push(n.clone()),
                // `a.includes(x)` reads `a`, which the call's name holds.
                ExprKind::Call { name, .. } => w.reads.extend(name.rsplit_once('.').map(|(a, _)| a.to_string())),
                _ => {}
            }
            None::<()>
        });
    }
    // `nested`: inside a loop in the body, which a `break` or `continue` leaves. `in_body`: inside an inlined
    // function's body, which its `return` leaves.
    fn walk<'a>(
        stmts: &'a [Stmt],
        scope: &mut Vec<&'a str>,
        nested: bool,
        in_body: bool,
        w: &mut Walk<'a>,
    ) -> Option<()> {
        let mark = scope.len();
        for stmt in stmts {
            let local = |n: &str, scope: &[&str]| scope.contains(&tree::head(n));
            match stmt {
                Stmt::Let { name, value } => {
                    read(value, w);
                    scope.push(name);
                    w.locals.push(name);
                }
                Stmt::Assign { target, value, .. } if local(target, scope) => read(value, w),
                Stmt::Assign {
                    target,
                    value,
                    typed: false,
                } if !nested => {
                    read(fold_shape(target, value)?.1, w);
                    w.accs.push(target);
                }
                Stmt::IndexAssign { target, index, value } if local(target, scope) => {
                    read(index, w);
                    read(value, w);
                }
                Stmt::Expr(e) => read(e, w),
                Stmt::If { cond, then, else_ } => {
                    read(cond, w);
                    walk(then, scope, nested, in_body, w)?;
                    walk(else_, scope, nested, in_body, w)?;
                }
                Stmt::Body(b) => walk(b, scope, nested, true, w)?,
                Stmt::Leave { to: Region::Body, .. } if in_body => {}
                Stmt::Leave {
                    to: Region::Loop | Region::Trip,
                    ..
                } if nested => {}
                Stmt::For { var, iterable, body } => {
                    read(iterable, w);
                    scope.push(var);
                    w.locals.push(var);
                    walk(body, scope, true, in_body, w)?;
                    scope.pop();
                }
                Stmt::While { cond, body, next } => {
                    read(cond, w);
                    walk(body, scope, true, in_body, w)?;
                    walk(next, scope, true, in_body, w)?;
                }
                _ => return None,
            }
        }
        scope.truncate(mark);
        Some(())
    }
    let mut w = Walk::default();
    walk(body, &mut vec![var], false, false, &mut w)?;
    // A read of the record, of an object above an accumulator or of a path under one reads the accumulator.
    let overlaps =
        |a: &str, b: &str| a == b || tree::ancestors(a).any(|x| x == b) || tree::ancestors(b).any(|x| x == a);
    let is_object = |name: &str| matches!(syms.node(name), Some(Node::Object(_)));
    for (i, acc) in w.accs.iter().enumerate() {
        if w.accs[..i].contains(acc)
            || is_object(acc)
            || syms.write_refused(acc).is_some()
            || w.locals.iter().any(|l| overlaps(l, acc))
            || w.reads.iter().any(|r| r == tree::ROOT || overlaps(r, acc))
        {
            return None;
        }
    }
    Some(w.accs)
}

/// How `target = value` folds, and the value it folds: `None` where it reads `target` other than as the
/// accumulator of `+`, `*`, `&&`, `||` or `push`, or writes an object.
fn fold_shape<'a>(target: &str, value: &'a Expr) -> Option<(FoldOp, &'a Expr)> {
    let is_acc = |e: &Expr| matches!(&e.kind, ExprKind::Field(n) if n == target);
    let (op, e) = match &value.kind {
        ExprKind::Object(_) => return None,
        ExprKind::Call { name, args } if name == "push" && args.len() == 2 && is_acc(&args[0]) => {
            (FoldOp::Push, &args[1])
        }
        ExprKind::Binary { op, lhs, rhs } if is_acc(lhs) != is_acc(rhs) => {
            let e = if is_acc(lhs) { &**rhs } else { &**lhs };
            match op {
                // Text does not commute: `s = x + s` prepends.
                BinOp::Add if is_acc(lhs) => (FoldOp::Add, e),
                BinOp::Mul => (FoldOp::Mul, e),
                // `acc && e` runs `e` only while `acc` is true, which a fold cannot know.
                BinOp::And if is_acc(rhs) => (FoldOp::And, e),
                BinOp::Or if is_acc(rhs) => (FoldOp::Or, e),
                _ => (FoldOp::Last, value),
            }
        }
        _ => (FoldOp::Last, value),
    };
    Some((op, e))
}

/// Whether `plan::exec::run_array_fold` has a kernel for this fold: `+` and `*` on numbers or, for `+`, on text or on
/// bytes, never the two mixed; `&&` and `||` on Bools; an overwrite of anything but an object; a push of a value the
/// array's elements meet. A dynamic seed (`None`) has no static type to ask, and the kernels read it at runtime, null
/// as false or as null.
fn fold_has_kernel(op: FoldOp, seed: &Option<DataType>, map: &DataType) -> bool {
    let numeric = |t: &DataType| t.is_numeric();
    let text = |t: &DataType| matches!(t, DataType::Str | DataType::Bytes);
    let boolean = |t: &DataType| matches!(t, DataType::Bool);
    match op {
        FoldOp::Last => !matches!(map, DataType::Struct(..)),
        FoldOp::Add => match seed {
            None => numeric(map) || text(map),
            Some(s) => (numeric(s) && numeric(map)) || (text(s) && s == map),
        },
        FoldOp::Mul => seed.as_ref().is_none_or(numeric) && numeric(map),
        FoldOp::And | FoldOp::Or => seed.as_ref().is_none_or(boolean) && boolean(map),
        FoldOp::Push => matches!(seed, Some(DataType::Array(e)) if e.unify(map).is_some()),
    }
}

/// Why a write of `value`, of type `from`, to `target`, which keeps `to`, is refused: `as` converts the value where
/// the coercion table has the pair, and the typed assignment changes the name's type.
fn type_kept(target: &str, value: &Expr, from: &DataType, to: &DataType) -> String {
    // `c[i] = e` names the element, not the array it rebuilds.
    if let (ExprKind::SetElement { index, value, .. }, DataType::Array(f), DataType::Array(t)) = (&value.kind, from, to)
    {
        let (shown, index) = (describe_expr(value), describe_expr(index));
        let (f, t) = (f.spell(), t.spell());
        return format!("`{shown}` is {f}, not {t}; to convert the value, write `{target}[{index}] = {shown} as {t}`");
    }
    let shown = describe_expr(value);
    // A variant has no type of its own to change the name to: `as` is its one fix.
    if *from == DataType::Variant {
        let to = to.spell();
        return format!("`{shown}` is variant, but `{target}` is {to}; to convert the value, write `{shown} as {to}`");
    }
    let converts = match &value.kind {
        ExprKind::Object(_) | ExprKind::Coerce { .. } => String::new(),
        _ if coercible(from, to) => format!("to convert the value, write `{shown} as {}`; ", to.spell()),
        _ => String::new(),
    };
    // The typed assignment converts, so a value already written `e as T` is spelled `e` there.
    let written = match &value.kind {
        ExprKind::Coerce { value, .. } => describe_expr(value),
        _ => shown.clone(),
    };
    let (from, to) = (from.spell(), to.spell());
    format!(
        "`{shown}` is {from}, not {to}; {converts}to change the type of `{target}`, write `{target}: {from} = {written}`"
    )
}

/// `ir`, written by `value` to `target`, kept at `to`, the type the name keeps (Language): `null` takes
/// the type at any depth, an `i64` widens to `f64`, and nothing else converts, a variant included. `Err` is the
/// message.
fn keep_ir(target: &str, value: &Expr, ir: Ir, to: &DataType) -> Result<Ir, String> {
    let from = ir.ty.clone();
    if from == *to {
        return Ok(ir);
    }
    if !keeps(&from, to) {
        return Err(type_kept(target, value, &from, to));
    }
    coerce_ir(ir, to, value.span).map_err(|_| type_kept(target, value, &from, to))
}

/// Whether a value of `from` may be written to a name that keeps `to`: as `fits` checks a declaration, except that a
/// field an open object has no name for joins its unclaimed fields.
fn keeps(from: &DataType, to: &DataType) -> bool {
    match (from, to) {
        (DataType::Array(f), DataType::Array(t)) => keeps(f, t),
        (DataType::Struct(have, open), DataType::Struct(want, wanted)) => {
            (!*open || *wanted)
                && have.iter().all(|(n, f)| match want.iter().find(|(m, _)| m == n) {
                    Some((_, t)) => keeps(f, t),
                    None => *wanted,
                })
        }
        _ => fits(from, to),
    }
}

/// The places `laid`, an object `lay_key` built, writes under `name`, each with what fills it.
fn laid_pieces(name: &str, laid: Ir, out: &mut Vec<(Place, Ir)>) {
    match laid.kind {
        IrKind::Object { fields, unclaimed } => {
            for (key, field) in fields {
                laid_pieces(&tree::child(name, &key), field, out);
            }
            if let Some(u) = unclaimed {
                out.push((Place::Unclaimed(name.to_string()), *u));
            }
        }
        _ => out.push((Place::Leaf(name.to_string()), laid)),
    }
}

/// The type `node` holds: a leaf's, or an object of its fields' types, open where it has unclaimed fields that may
/// hold a key.
fn node_type(syms: &SymbolTable, node: &Node) -> DataType {
    shape(syms, node, false)
}

/// `node_type`, or where `laid`, the type it holds once a write lays keys in it: open wherever it has unclaimed fields.
fn shape(syms: &SymbolTable, node: &Node, laid: bool) -> DataType {
    match node {
        Node::Leaf(slot) => syms.type_of(*slot).clone(),
        Node::Object(o) => DataType::Struct(
            o.fields
                .iter()
                .map(|(k, n)| (k.clone(), shape(syms, n, laid)))
                .collect(),
            if laid { o.unclaimed.is_some() } else { !syms.no_keys(o) },
        ),
    }
}

/// Why a loop refuses an accumulator whose type the body changes. One message for the general loop and the fold.
fn accumulator_type_change(name: &str, kind: &str, before: &DataType, after: &DataType) -> String {
    let (before, after) = (before.spell(), after.spell());
    format!(
        "`{name}` is assigned inside a `{kind}` loop and must keep one type across it, but starts as {before} and \
         the body makes it {after} -- a row that runs no trips would keep the first and a row that runs would have \
         the second, and there is no column that holds both."
    )
}

/// `inner` on the rows `outer` selects: false where either is false or null.
fn guard_ir(outer: Option<SlotId>, inner: Ir) -> Ir {
    let span = inner.span;
    let ir = match outer {
        None => inner,
        Some(o) => Ir::new(
            DataType::Bool,
            span,
            IrKind::Binary {
                op: BinOp::And,
                lhs: Box::new(Ir::new(DataType::Bool, span, IrKind::Field(o))),
                rhs: Box::new(inner),
            },
        ),
    };
    flatten(is_true(ir))
}

fn negate(ir: Ir) -> Ir {
    let span = ir.span;
    Ir::new(
        DataType::Bool,
        span,
        IrKind::Unary {
            op: crate::ops::UnOp::Not,
            operand: Box::new(ir),
        },
    )
}

/// `true && ir`: true where `ir` is a true Bool, false everywhere else. `&&` reads null and any other value as false,
/// so a guard built on this is a Bool on every row whatever the condition holds.
pub(crate) fn is_true(ir: Ir) -> Ir {
    let span = ir.span;
    Ir::new(
        DataType::Bool,
        span,
        IrKind::Binary {
            op: BinOp::And,
            lhs: Box::new(Ir::new(DataType::Bool, span, IrKind::Literal(Value::Bool(true)))),
            rhs: Box::new(ir),
        },
    )
}

impl Compiler<'_> {
    fn err(&mut self, f: Option<FunctionId>, msg: impl Into<String>) {
        self.errors.push(CompileError::new(f, msg));
    }

    /// Records what compiles in function `id` but is likely a mistake (`Plan::warnings`).
    fn warn(&mut self, id: FunctionId, msg: impl Into<String>) {
        self.syms.warnings.push(CompileError::new(Some(id), msg));
    }

    /// Names every error recorded since `before` -- usually one, from the assignment currently being compiled -- as
    /// belonging to the assignment at `index`. `self.expr` and friends push straight onto `self.errors` several
    /// calls down, with no assignment in view by the time they do; catching it here, where the eval loop still
    /// has its own position in the list, is simpler than threading it through every path that can raise one.
    fn attribute_to_assignment(&mut self, before: usize, index: usize) {
        for e in &mut self.errors[before..] {
            e.assignment_index = Some(index as u32);
        }
    }

    /// Bind the leaf `name` to a fresh slot, as `SymbolTable::bind` does, making each object on the way to it.
    fn bind(&mut self, name: &str, ty: DataType, origin: Origin, id: FunctionId) -> SlotId {
        self.open_path(name, id);
        self.syms.bind(name, ty, origin)
    }

    /// Whether a loop body is being compiled.
    fn in_loop(&self) -> bool {
        !self.outer.is_empty()
    }

    /// Make each object above `name` that the tree does not hold yet. Where the object it sits in has unclaimed fields,
    /// the new object takes the key of its name from them, where it holds an object, as its own unclaimed fields: a
    /// write under a key keeps the rest of it (Language). Elsewhere it is open, holding no key yet. That split
    /// is no write of the author's, so it runs on every row, ahead of every loop the unclaimed fields were bound before.
    /// A leaf on the way gives way: a write under one that holds a value is refused before this. A guarded write finds
    /// its name bound (Language), so no guard reaches here.
    fn open_path(&mut self, name: &str, id: FunctionId) {
        for at in tree::ancestors(name) {
            let (parent, seg) = tree::parent(at);
            if self.syms.node(at).is_some() || self.syms.is_local(at) {
                continue;
            }
            // A key taken out of the unclaimed fields holds nothing to keep.
            let u = self.syms.object(parent).and_then(|o| o.unclaimed);
            let Some(u) = u.filter(|_| !self.syms.removed(parent).iter().any(|k| *k == seg)) else {
                self.open_new(at, id);
                continue;
            };
            let span = Span::default();
            let member = member(u, &tree::spell(&seg), span);
            // A split hoisted ahead of a loop is bound before it too, so a split of it goes where it went.
            let depth = self.ahead(u);
            // Ahead of a loop, a row may run no trip, so the key's value stays whole beneath the fields a trip
            // writes.
            let ir = match depth {
                Some(_) => member,
                None => Ir::new(DataType::Variant, span, IrKind::Unclaimed(Box::new(member))),
            };
            let slot = self.syms.bind_unclaimed(at, Origin::Function(id));
            match depth {
                Some(depth) => self.emit_ahead(depth, slot, ir, id),
                None => self.emit(Op::Eval { ir }, slot, DataType::Variant, id),
            }
        }
    }

    /// Make `name` an object with unclaimed fields that hold nothing: an object a write makes is open. Ahead of every
    /// loop, as a split is.
    fn open_new(&mut self, name: &str, id: FunctionId) {
        let slot = self.syms.bind_unclaimed(name, Origin::Function(id));
        self.syms.note_null(slot);
        let null = Ir::new(DataType::Variant, Span::default(), IrKind::Literal(Value::Null));
        match self.in_loop() {
            true => self.emit_ahead(0, slot, null, id),
            false => self.emit(Op::Eval { ir: null }, slot, DataType::Variant, id),
        }
    }

    /// The outermost loop `u` is bound ahead of, where it is: a slot bound before the loop began, or hoisted ahead of
    /// it.
    fn ahead(&self, u: SlotId) -> Option<usize> {
        let depth = self.hoisted.get(&u).copied();
        depth.or_else(|| self.loop_firsts.iter().position(|&first| u < first))
    }

    /// Evaluate `ir` into `out` ahead of the loop at `depth`, on every row.
    fn emit_ahead(&mut self, depth: usize, out: SlotId, ir: Ir, id: FunctionId) {
        self.outer[depth].push(Instr {
            op: Op::Eval { ir },
            out,
            out_ty: DataType::Variant,
            guard: None,
            reuse: None,
            constant_ok: false,
            function: id,
        });
        self.hoisted.insert(out, depth);
    }

    /// Give each place at or under a name `body` assigns a slot of its own where another name shares it. A loop feeds
    /// an accumulator's slot trip by trip, which would change the other name with it. `var`'s own names are the
    /// loop's.
    fn unshare_assigned(&mut self, body: &[Stmt], var: Option<&str>, id: FunctionId) {
        let mut names = Vec::new();
        assigned(body, &mut names);
        for name in names {
            if var.is_some_and(|v| tree::head(&name) == v) {
                continue;
            }
            for place in self.places_at(&name) {
                self.unshare(&place, id);
            }
        }
    }

    /// Give `place` a copy of its slot where another place shares the slot, and unclaimed fields a column of their
    /// own where keys were taken out of them: an accumulator's first value is the column.
    fn unshare(&mut self, place: &Place, id: FunctionId) {
        if let Place::Unclaimed(name) = place
            && !self.syms.removed(name).is_empty()
        {
            self.settle_unclaimed(name, id);
            return;
        }
        let Some(slot) = self.syms.slot_of(place) else {
            return;
        };
        // A slot a local hides is shared too: the block's end puts it back.
        if !self.syms.root().places().iter().any(|(p, s)| *s == slot && p != place)
            && !self.syms.hidden_slots().contains(&slot)
        {
            return;
        }
        let ty = self.syms.type_of(slot).clone();
        let copy = self.syms.alloc(place.to_string(), ty.clone(), Origin::Function(id));
        // A copy of a local's value is the local's, so an unread one is said to be the local's.
        if self.syms.is_local(place.name()) {
            self.syms.local_slots.insert(copy);
        }
        let ir = Ir::new(ty.clone(), Span::default(), IrKind::Field(slot));
        self.emit(Op::Eval { ir }, copy, ty, id);
        self.syms.put_place(place, copy);
    }

    /// Give the object `name`'s unclaimed fields a column of their own without the keys taken out of them, where any
    /// were: what a step that reads them as a column, and not through `unclaimed_ir`, needs first.
    fn settle_unclaimed(&mut self, name: &str, id: FunctionId) {
        let Some(u) = self.syms.object(name).and_then(|o| o.unclaimed) else {
            return;
        };
        if self.syms.removed(name).is_empty() {
            return;
        }
        let ir = unclaimed_ir(u, self.syms.removed(name), Span::default());
        let out = self.syms.bind_unclaimed(name, Origin::Function(id));
        self.emit(Op::Eval { ir }, out, DataType::Variant, id);
    }

    /// The objects a dynamic parse lays the keys it does not declare into, the record first: each object the tree
    /// holds, less those at or under a declared leaf or a new declared object, which the parse replaces; each new
    /// declared object; and each object above a declared name. A declared object, or one above a declared name, that
    /// is still a key of its parent's unclaimed fields is split from them first (`open_path`), so it keeps the keys an
    /// earlier step laid there, and the parse lays its own over them. A declared object the tree then does not hold is
    /// new, and closed where its type is; one the tree holds is closed where it has no unclaimed fields.
    fn parse_objects(
        &mut self,
        target: &str,
        leaves: &[DeclaredField],
        declared: &[(String, bool)],
        id: FunctionId,
    ) -> Vec<ParseObject> {
        let names: Vec<String> = leaves
            .iter()
            .map(|d| d.name.clone())
            .chain(declared.iter().map(|(o, _)| o.clone()))
            .collect();
        // The path to a declared object's unclaimed fields opens the object itself.
        let places = leaves
            .iter()
            .map(|d| Place::Leaf(d.name.clone()))
            .chain(declared.iter().map(|(o, _)| Place::Unclaimed(o.clone())));
        for place in places {
            self.open_path(&place.to_string(), id);
        }
        let new: Vec<&String> = declared
            .iter()
            .map(|(o, _)| o)
            .filter(|o| !matches!(self.syms.node(o), Some(Node::Object(_))))
            .collect();
        let closed = |n: &str| new.iter().any(|o| *o == n) && declared.iter().any(|(o, closed)| o == n && *closed);
        let replaced = |n: &str| {
            leaves.iter().any(|d| binding::at_or_under(n, &d.name)) || new.iter().any(|o| binding::at_or_under(n, o))
        };
        // The parse lays its keys over each object's column, so a key taken out of one is gone from it first.
        let within = |n: &str| target.is_empty() || binding::at_or_under(n, target);
        let kept: Vec<String> = std::iter::once(String::new())
            .chain(self.syms.root().objects().into_iter().map(|(name, _)| name))
            .filter(|name| within(name) && (!target.is_empty() || !self.syms.is_local(name)) && !replaced(name))
            .collect();
        for name in &kept {
            self.settle_unclaimed(name, id);
        }
        // An object the tree holds with no unclaimed fields is closed, and stays so. The record is not: a parse opens
        // it.
        let mut objects: Vec<(String, Option<SlotId>, bool)> = kept
            .into_iter()
            .map(|name| {
                let under = self.syms.object(&name).and_then(|o| o.unclaimed);
                let closed = under.is_none() && !name.is_empty();
                (name, under, closed)
            })
            .collect();
        // A variant leaf the parse makes an object of lays its keys under the parse's.
        let held = |n: &str| match self.syms.node(n) {
            Some(Node::Leaf(s)) if *self.syms.type_of(*s) == DataType::Variant => Some(*s),
            _ => None,
        };
        objects.extend(new.iter().map(|name| (name.to_string(), held(name), closed(name))));
        for at in names
            .iter()
            .flat_map(|name| tree::ancestors(name))
            .filter(|at| within(at))
        {
            // A leaf that holds nothing gives way to the object, as it does to any write under it.
            let open = match self.syms.node(at) {
                None => true,
                Some(Node::Leaf(s)) => *self.syms.type_of(*s) == DataType::Null,
                Some(Node::Object(_)) => false,
            };
            if open && !objects.iter().any(|(n, ..)| n == at) {
                objects.push((at.to_string(), None, false));
            }
        }
        objects
            .into_iter()
            .map(|(name, under, closed)| ParseObject {
                out: self.syms.alloc(
                    Place::Unclaimed(name.clone()).to_string(),
                    DataType::Variant,
                    Origin::Function(id),
                ),
                name: tree::key_under(target, &name).to_string(),
                under,
                closed,
            })
            .collect()
    }

    /// What a loop body at `depth` assigned, as the loop's accumulators, each with its name: every place bound to a
    /// slot made since `first`, outside the loop variable `var` and less what `open_path` split ahead of the loop.
    fn accumulators(
        &self,
        before: &HashMap<Place, SlotId>,
        first: SlotId,
        depth: usize,
        var: Option<&str>,
    ) -> (Vec<String>, Vec<(Option<SlotId>, SlotId)>) {
        self.syms
            .rebound_since(before, first)
            .into_iter()
            .filter(|(place, _, _)| var.is_none_or(|v| tree::head(place.name()) != v))
            .filter(|(_, _, now)| !matches!(self.hoisted.get(now), Some(&d) if d <= depth))
            .map(|(place, then, now)| (place.to_string(), (then, now)))
            .unzip()
    }
}

/// The member of the unclaimed fields in `u` at `path`: a key, and a key under it at each dot.
fn member(u: SlotId, path: &str, span: Span) -> Ir {
    let base = Ir::new(DataType::Variant, span, IrKind::Field(u));
    tree::segments(path).fold(base, |base, key| {
        let index = Ir::new(DataType::Str, span, IrKind::Literal(Value::Str(key.to_string())));
        Ir::new(
            DataType::Variant,
            span,
            IrKind::Index {
                base: Box::new(base),
                index: Box::new(index),
            },
        )
    })
}

/// `declared` with each object type lowered to its leaves, and the objects, parents before children, each with whether
/// it is closed.
fn lower_objects(declared: &[DeclaredField]) -> (Vec<DeclaredField>, Vec<(String, bool)>) {
    fn lower(d: DeclaredField, leaves: &mut Vec<DeclaredField>, objects: &mut Vec<(String, bool)>) {
        let DataType::Struct(fields, open) = &d.ty else {
            leaves.push(d);
            return;
        };
        objects.push((d.name.clone(), !open));
        for (key, ty) in fields {
            let leaf = DeclaredField {
                name: tree::child(&d.name, key),
                ty: ty.clone(),
                time_source: auto_time(ty),
            };
            lower(leaf, leaves, objects);
        }
    }
    let (mut leaves, mut objects) = (Vec::new(), Vec::new());
    for d in declared {
        lower(d.clone(), &mut leaves, &mut objects);
    }
    (leaves, objects)
}

/// The names `body`'s statements assign, nested blocks included.
fn assigned(body: &[Stmt], out: &mut Vec<String>) {
    Stmt::find_map::<()>(body, &|s| !matches!(s, Stmt::Function { .. }), &mut |s| {
        match s {
            Stmt::Assign { target: name, .. } | Stmt::IndexAssign { target: name, .. } | Stmt::Declare { name, .. } => {
                out.push(name.clone())
            }
            Stmt::Delete { names, .. } => out.extend(names.iter().cloned()),
            _ => {}
        }
        None
    });
}

/// The candidate names an error offers, bounded: a wide CSV has dozens of headers and the whole list buries the one
/// word the reader came for.
fn name_hint<'a>(names: impl Iterator<Item = &'a str>) -> String {
    const SHOWN: usize = 8;
    let all: Vec<&str> = names.collect();
    match all.len().checked_sub(SHOWN) {
        Some(rest) if rest > 0 => format!("{}, and {rest} more", all[..SHOWN].join(", ")),
        _ => all.join(", "),
    }
}

impl Compiler<'_> {
    fn emit(&mut self, op: Op, out: SlotId, out_ty: DataType, function: FunctionId) {
        self.instrs.push(Instr {
            op,
            out,
            out_ty,
            guard: None,
            reuse: None,
            constant_ok: false,
            function,
        });
    }

    fn expr(&mut self, src: &str, f: FunctionId) -> Option<Ir> {
        let ast = match parse(src, Some(f)) {
            Ok(a) => a,
            Err(e) => {
                self.errors.push(e);
                return None;
            }
        };
        self.expr_ast(&ast, f)
    }

    /// `None` means an error was recorded.
    fn expr_ast(&mut self, ast: &Expr, f: FunctionId) -> Option<Ir> {
        match resolve(ast, &mut self.syms, &mut self.res) {
            Ok(ir) => Some(flatten(ir)),
            Err(mut e) => {
                e.function = Some(f);
                self.errors.push(e);
                None
            }
        }
    }

    /// Evaluate a `for`'s iterable into a temp slot; it must be statically `Array(_)`. `None` means an error was
    /// recorded.
    fn resolve_array_iterable(&mut self, iterable: &Expr, here: Here) -> Option<(SlotId, DataType)> {
        let Here { guard, id } = here;
        let iterable_ir = self.expr_ast(iterable, id)?;
        let DataType::Array(element_ty) = iterable_ir.ty.clone() else {
            self.err(
                Some(id),
                "`for...of` needs a literal array (e.g. `[1, 2, 3]`) or a field \
                 of a declared `Array` type — this is neither (a dynamically-typed \
                 field included: declare its type on the parser that produces it)",
            );
            return None;
        };
        let array_slot = self.syms.temp(iterable_ir.ty.clone(), Origin::Temp);
        self.emit_under(
            guard,
            Op::Eval { ir: iterable_ir },
            array_slot,
            DataType::Array(element_ty.clone()),
            id,
        );
        Some((array_slot, *element_ty))
    }

    /// `emit`, for an instruction computed ahead of the guarded expression that reads it: it runs on every row, and
    /// what it notes on a row outside `reach` is retracted after it (`plan::exec::retract_masked`).
    fn emit_under(&mut self, reach: Option<SlotId>, op: Op, out: SlotId, out_ty: DataType, f: FunctionId) {
        self.instrs.push(Instr {
            op,
            out,
            out_ty,
            guard: reach,
            reuse: None,
            constant_ok: false,
            function: f,
        });
    }

    /// What the record holds at `name`, which no step bound: the unclaimed fields of the deepest object on its path,
    /// and the member of them at the rest of `name`. `None` where there is none. A row a guard leaves keeps it.
    fn record_key(&self, name: &str, span: Span) -> Option<(SlotId, Ir)> {
        let (object, at, rest) = self.syms.deepest(name);
        let key = tree::segments(rest).next().unwrap_or_default();
        let u = object.unclaimed.filter(|u| !self.syms.is_null(*u))?;
        // A local's own object holds keys; the record's unclaimed fields hold none of a local's.
        let held = !name.starts_with("__")
            && (!at.is_empty() || !self.syms.is_local(name))
            && self.syms.leaf_above(name).is_none()
            && !object.removed.iter().any(|k| *k == key);
        held.then(|| (u, member(u, rest, span)))
    }

    /// Whether `name` holds an array, bound itself or read as a declared struct's field: where `name[i] = x` writes a
    /// position.
    fn holds_array(&self, name: &str) -> bool {
        let ty = match self.syms.lookup(name) {
            Some(s) => self.syms.type_of(s).clone(),
            None => match member_ir(name, Span::default(), &self.syms) {
                Ok(Some(ir)) => ir.ty,
                _ => return false,
            },
        };
        matches!(ty, DataType::Array(_))
    }

    /// `name: type;` states a name's type once, before anything binds it. A name already bound has its value's type,
    /// and so does a path under it and a key an open object on its way may hold: `name: type = name` converts it. Any
    /// other name is a record key, which its parser declares. So a declaration in code is a compile error until a
    /// spread in code fills what it declares.
    fn declare(&mut self, name: &str, to: &DataType, id: FunctionId) {
        // `path` under an object, as the object type that holds it.
        let nest = |path: &str, open: bool| match path {
            "" => to.clone(),
            _ => tree::segments(path)
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
                .fold(to.clone(), |t, k| DataType::Struct(vec![(k.to_string(), t)], open)),
        };
        let message = if self.syms.is_local(name) {
            local_declared(name)
        } else if name == tree::ROOT {
            "`$` is the record, which its parsers type".to_string()
        } else if let Some(node) = self.syms.node(name) {
            self.already(name, node_type(&self.syms, node), to)
        } else if let Some((above, slot)) = self.syms.leaf_above(name) {
            match self.syms.type_of(slot) {
                // `q.a: str` names `q: {a: str, ...}`.
                DataType::Variant => self.already(above, DataType::Variant, &nest(&name[above.len() + 1..], true)),
                t => format!("`{above}` is {} and has no fields", t.spell()),
            }
        } else {
            match self.syms.deepest(name) {
                (o, at, _) if !self.syms.no_keys(o) => {
                    let whose = if at.is_empty() {
                        "the record".to_string()
                    } else {
                        format!("`{at}`")
                    };
                    let to = to.spell();
                    format!("`{name}` may be a key of {whose}; to give it a type, write `{name}: {to} = {name}`")
                }
                (_, "", _) => {
                    // Nothing holds the name yet: it is null, of its type, until a value is assigned.
                    let null = Expr {
                        span: Span::default(),
                        kind: ExprKind::Coerce {
                            ty: to.clone(),
                            value: Box::new(Expr {
                                span: Span::default(),
                                kind: ExprKind::Literal(Value::Null),
                            }),
                        },
                    };
                    self.assign(name, &null, false, true, Here { guard: None, id });
                    return;
                }
                // `o.b: str` under `o: {a: i64}` names `o: {a: i64, b: str}`.
                (o, at, rest) => {
                    let held = node_type(&self.syms, &Node::Object(o.clone()));
                    let DataType::Struct(mut fields, open) = held.clone() else {
                        unreachable!("an object's type")
                    };
                    let key = tree::segments(rest).next().unwrap_or_default();
                    let below = tree::key_under(tree::head(rest), rest);
                    fields.push((key.to_string(), nest(below, false)));
                    self.already(at, held, &DataType::Struct(fields, open))
                }
            }
        };
        self.err(Some(id), message);
    }

    /// Why `name`, bound and `held`, takes no declaration: `name: to = name as to` changes its type to `to`.
    fn already(&self, name: &str, held: DataType, to: &DataType) -> String {
        let (held_s, to_s) = (held.spell(), to.spell());
        if held == *to {
            return format!("`{name}` is already {held_s}");
        }
        format!(
            "`{name}` is already {held_s}; to change the type of `{name}` to {to_s}, write `{name}: {to_s} = {name} as {to_s}`"
        )
    }

    /// `target[key] = value`, with a key only the row knows, on an open object: on each row, the write `lay_key` says
    /// the key's path takes. A name nothing binds is an open object with nothing in it first.
    fn keyed_write(&mut self, target: &str, index: &Expr, value: &Expr, here: Here) {
        let Here { guard, id } = here;
        let (name, node) = self.syms.named(target);
        if let Some(e) = self.syms.write_refused(name) {
            self.err(Some(id), e.message);
            return;
        }
        if (guard.is_some() || self.in_loop()) && !self.syms.is_local(target) {
            // The record may hold the key, which the write would hide on the rows it skips (Language).
            if name.is_empty() {
                return self.err(
                    Some(id),
                    "`$[key] = value` writes a key the record may hold, which a write inside an `if` or a loop would \
                     hide on the rows it skips; write it outside the `if` or the loop"
                        .to_string(),
                );
            }
            // A first write by key makes an open object with nothing in it: what a typing pass notes for the name.
            if node.is_none()
                && let Pass::Typing(typing) = &mut self.pass
                && !typing.iter().any(|(n, _)| n == name)
            {
                typing.push((
                    name.to_string(),
                    FirstWrite {
                        ty: DataType::Struct(Vec::new(), true),
                        declared: 0,
                    },
                ));
            }
        }
        match node {
            Some(Node::Leaf(slot)) => match self.syms.type_of(slot) {
                DataType::Variant => {
                    self.err(
                        Some(id),
                        format!(
                            "`{target}` {}",
                            variant_use(Some(&format!("`{target}: {{...}} = {target};`")))
                        ),
                    );
                    return;
                }
                t => {
                    let t = t.spell();
                    self.err(
                        Some(id),
                        format!("`{target}[key] = value` writes a key of an object or a position of an array, and `{target}` is {t}"),
                    );
                    return;
                }
            },
            None if !name.is_empty() => {
                // A first write names a type (Language), and `null` has none.
                if matches!(value.kind, ExprKind::Literal(Value::Null)) {
                    return self.err(
                        Some(id),
                        format!("`{target}` has no type: `null` is null; write `{target}: {{...}} = null` before it"),
                    );
                }
                self.open_empty(name, id)
            }
            _ => {}
        }
        let held = self.syms.object(name).expect("an object, made above where it was none");
        if held.unclaimed.is_none() {
            let (whose, way) = match name {
                "" => ("the record".to_string(), "a `json` parser opens it".to_string()),
                _ => (
                    format!("`{target}`"),
                    format!("change its type, `{target}: {{...}} = {target} as {{...}}`"),
                ),
            };
            self.err(
                Some(id),
                format!(
                    "`{target}[key] = value` writes a key only the row knows, and {whose} is closed: write a field by \
                     name, or {way}"
                ),
            );
            return;
        }
        let Some(key) = self.expr_ast(index, id) else {
            return;
        };
        if let Err(e) = object_key(&key, index, &self.syms) {
            self.err(Some(id), e.message);
            return;
        }
        let Some(value) = self.expr_ast(value, id) else {
            return;
        };
        let span = value.span;
        let (key, value) = (self.once(key, here), self.once(value, here));
        let object = match resolve(
            &Expr::new(span, ExprKind::Field(target.to_string())),
            &mut self.syms,
            &mut self.res,
        ) {
            Ok(ir) => ir,
            Err(e) => return self.err(Some(id), e.message),
        };
        // The value fits each field the key may name, as a declaration's does: coerced by the table, a row it does not
        // fit refused. A field holding nothing takes the value's type. An object takes it once, on the rows that name
        // it, into a slot named for it, and its leaves read from there.
        let mut fit = |path: &str, ty: &DataType, named: &Ir| {
            if value.ty == DataType::Null || *ty == DataType::Null {
                return Ok(value.clone());
            }
            // A pair the table lacks goes through a variant, which refuses each row the key names.
            let coerced = coerce_ir(value.clone(), ty, span)
                .or_else(|_| coerce_ir(coerce_ir(value.clone(), &DataType::Variant, span)?, ty, span))?;
            if !matches!(ty, DataType::Struct(..)) {
                return Ok(coerced);
            }
            let named = Ir::new(
                ty.clone(),
                span,
                IrKind::Ternary {
                    cond: Box::new(named.clone()),
                    then: Box::new(coerced),
                    else_: Box::new(Ir::new(DataType::Null, span, IrKind::Literal(Value::Null))),
                },
            );
            let slot = self.syms.alloc(tree::join(name, path), ty.clone(), Origin::Temp);
            self.emit_under(guard, Op::Eval { ir: named }, slot, ty.clone(), id);
            Ok(Ir::new(ty.clone(), span, IrKind::Field(slot)))
        };
        let laid = match lay_key(object, &key, &value, span, &mut fit) {
            Ok(laid) => laid,
            Err(e) => return self.err(Some(id), e.message),
        };
        let mut pieces = Vec::new();
        laid_pieces(name, laid, &mut pieces);
        for (place, ir) in pieces {
            // The record's own fields are the engine's, and no key names them.
            if name.is_empty() && tree::is_internal(tree::head(place.name())) {
                continue;
            }
            match place {
                Place::Leaf(leaf) => {
                    self.bind_writes(&leaf.clone(), vec![(Place::Leaf(leaf), Resolved::Ir(ir))], false, here)
                }
                place => self.bind_resolved(&place, Resolved::Ir(flatten(ir)), here),
            }
        }
    }

    /// Make `name` an open object with nothing in it: unclaimed fields bound to a column that holds none, so a loop
    /// has them to add to.
    fn open_empty(&mut self, name: &str, id: FunctionId) {
        self.open_path(name, id);
        self.syms.unbind(name);
        self.open_new(name, id);
    }

    /// `ir`, computed once into a slot of its own where it computes anything, for a caller that reads it more than
    /// once.
    fn once(&mut self, ir: Ir, here: Here) -> Ir {
        let Here { guard, id } = here;
        if matches!(ir.kind, IrKind::Field(_) | IrKind::Literal(_)) {
            return ir;
        }
        let (ty, span) = (ir.ty.clone(), ir.span);
        let slot = self.syms.temp(ty.clone(), Origin::Temp);
        self.emit_under(guard, Op::Eval { ir }, slot, ty.clone(), id);
        Ir::new(ty, span, IrKind::Field(slot))
    }

    /// Warns of a condition, written `text`, that holds on every row or on none.
    fn note_constant(&mut self, ir: &Ir, text: &str, id: FunctionId) {
        if let Some(holds) = always(ir) {
            self.warn(id, format!("`{}` is always {holds}", text.trim()));
        }
    }

    /// `ir` as a condition: `bool`, or null, which reads false. Anything else is a compile error naming `what`.
    fn condition(&mut self, ir: Ir, what: &str, id: FunctionId) -> Option<Ir> {
        if is_condition(&ir.ty) {
            return Some(ir);
        }
        self.err(
            Some(id),
            match &ir.ty {
                DataType::Variant => format!("{what} {}", variant_use(None)),
                other => format!("{what} must be bool, found {} (no truthiness)", other.spell()),
            },
        );
        None
    }

    /// Bind `target` to `value`: every `let`, assignment and `Eval` step lands here, and binds the places
    /// `plan::binding::writes` gives. Every place resolves before anything binds, so a place may read what the same
    /// assignment replaces.
    fn assign(&mut self, target: &str, value: &Expr, declares: bool, typed: bool, here: Here) {
        let Here { guard, id } = here;
        if let ExprKind::Call { name, args } = &value.kind
            && name == "Object.assign"
            && matches!(args.first().map(|a| &a.kind), Some(ExprKind::Field(p)) if p == target)
        {
            return self.object_assign(target, &args[1..], declares, typed, here);
        }
        if target == tree::ROOT {
            return self.assign_root(value, here);
        }
        // `move`'s write replaces what `to` held, whatever its type, as a Rename does.
        if let ExprKind::Field(from) = &value.kind
            && from.starts_with(crate::expr::parse::MOVE_TEMP)
            && guard.is_none()
            && !self.in_loop()
            && self.syms.node(target).is_some()
        {
            self.delete(target, true, Here { guard: None, ..here });
        }
        // A `let` and `name: type = e` are the writes that give a name a type of their own.
        let typed = declares || typed;
        let (stated, written) = match &value.kind {
            ExprKind::Check { ty, value: v, .. } if typed => (Some(ty.clone()), &**v),
            _ => (None, value),
        };
        // A declaration of an object type converts a variant by spreading its keys into the object's fields.
        let parse = ParseCall::of(written).or_else(|| match (&stated, declares) {
            (Some(DataType::Struct(..)), false) => self.spread_of(written),
            _ => None,
        });
        // In a construct's typing pass, a first write of a name inside it, and every write to that name after, bind
        // the name afresh and note the type the writes blend to, or the one a declaration states (Language).
        // A local, a path under one and the engine's own names are not the construct's to type.
        if let Pass::Typing(typing) = &self.pass
            && !declares
            && (guard.is_some() || self.in_loop())
            && !self.syms.is_local(target)
            && !tree::is_internal(target)
        {
            let tracked = typing.iter().position(|(n, _)| n == target);
            if tracked.is_some() || self.syms.node(target).is_none() {
                match &parse {
                    Some(call) => self.parse_write(target, call, stated.as_ref(), Here { guard: None, ..here }),
                    None => {
                        let Some(writes) = self.resolve_writes(target, value, id) else {
                            return;
                        };
                        self.bind_writes(target, writes, true, Here { guard: None, ..here });
                    }
                }
                let now = self
                    .syms
                    .node(target)
                    .map(|n| node_type(&self.syms, n))
                    .unwrap_or(DataType::Null);
                let Pass::Typing(typing) = &mut self.pass else {
                    unreachable!("a typing pass")
                };
                let first = match tracked {
                    Some(i) => &mut typing[i].1,
                    None => {
                        typing.push((
                            target.to_string(),
                            FirstWrite {
                                ty: DataType::Null,
                                declared: 0,
                            },
                        ));
                        &mut typing.last_mut().expect("just pushed").1
                    }
                };
                match stated {
                    Some(ty) => {
                        first.ty = ty;
                        first.declared += 1;
                    }
                    None if first.declared == 0 => first.ty = first.ty.unify(&now).unwrap_or(DataType::Variant),
                    None => {}
                }
                return;
            }
        }
        // A declaration types every row (Language): inside an `if` or a loop it declares a name of the
        // construct's own, never one bound outside.
        if typed && !declares && (guard.is_some() || self.in_loop()) && self.bound_outside(target) {
            let to = match &value.kind {
                ExprKind::Check { ty, .. } => ty.spell(),
                _ => "type".to_string(),
            };
            return self.err(
                Some(id),
                format!(
                    "`{target}: {to}` is inside an `if` or a loop, and a declaration types every row; write \
                     `{target}: {to} = {target} as {to};` before it, and `{target} = …` here"
                ),
            );
        }
        // A `let` binds a local, which leaves the reserved slot as it was.
        if typed
            && !declares
            && let Some((_, held)) = reserved_names().find(|(n, _)| *n == target)
            && let Some(to) = stated.as_ref().filter(|to| **to != held)
        {
            return self.err(
                Some(id),
                format!(
                    "`{target}` is smeltr's own and stays {}; it cannot be declared {}",
                    held.spell(),
                    to.spell()
                ),
            );
        }
        if let Some(call) = &parse {
            return self.parse_write(target, call, stated.as_ref(), here);
        }
        let Some(writes) = self.resolve_writes(target, value, id) else {
            return;
        };
        // A first write binds the name to its value's type (Language); `null` and `[]` have none. A `null`
        // over a key the record may hold takes that key out. A `let` is a first write, whatever it shadows.
        if (declares || self.syms.node(target).is_none())
            && let Some((place, ty)) = writes
                .iter()
                .filter(|(p, _)| matches!(p, Place::Leaf(_)))
                .map(|(p, r)| (p, self.resolved_ty(r)))
                .find(|(_, t)| holds_null(t))
            && !(ty == DataType::Null && self.record_key(target, Span::default()).is_some())
        {
            let whole = place.name() == target;
            if whole && let Some(untyped) = &mut self.untyped {
                let at = untyped.seen;
                untyped.seen += 1;
                match untyped.settled.get(&at).filter(|t| !holds_null(t)) {
                    // A round before found the type: the write declares it.
                    Some(ty) => {
                        let decl = Expr::new(
                            value.span,
                            ExprKind::Check {
                                ty: ty.clone(),
                                value: Box::new(value.clone()),
                                site: Site::Declaration(target.to_string()),
                            },
                        );
                        return self.assign(target, &decl, declares, true, here);
                    }
                    None if !untyped.last => {
                        untyped
                            .found
                            .push((at, target.to_string(), self.syms.binding(target), ty));
                        return self.bind_writes(target, writes, declares, here);
                    }
                    None => {}
                }
            }
            let shown = describe_expr(value);
            // An expression with no short spelling is "the value".
            let (what, written) = match shown.as_str() {
                "the value" => (shown.clone(), "…".to_string()),
                _ => (format!("`{shown}`"), shown.clone()),
            };
            let message = if whole {
                format!(
                    "`{target}` has no type: {what} is {}; write `{target}: type = {written}`",
                    ty.spell()
                )
            } else {
                format!("`{place}` has no type in {what}; write `{target}: type = {written}`, naming its type")
            };
            self.err(Some(id), message);
            // Bound as null, which takes any write without a second report, so each line after reports only its own.
            if whole {
                self.syms
                    .bind_place(&Place::Leaf(target.to_string()), DataType::Null, Origin::Function(id));
            }
            return;
        }
        // A write to a name `settle` has not typed yet is taken as it is, and its type noted. A local of the same
        // name, opened inside or after, is another binding and types itself.
        if let Some(i) = self.untyped_found(target) {
            self.bind_writes(target, writes, declares, here);
            let now = self
                .syms
                .node(target)
                .map(|n| node_type(&self.syms, n))
                .unwrap_or(DataType::Null);
            let found = &mut self.untyped.as_mut().expect("checked above").found[i].3;
            *found = found.unify(&now).unwrap_or(DataType::Variant);
            return;
        }
        let writes = match (typed, self.syms.node(target)) {
            (false, Some(node)) => {
                let held = node_type(&self.syms, node);
                match self.keep_type(target, &held, writes, value, id) {
                    Some(writes) => writes,
                    None => return,
                }
            }
            _ => writes,
        };
        self.bind_writes(target, writes, declares, here);
    }

    /// `target = call` or `target: ty = call`, `call` a parse function's: the object it gives replaces `target`. A name
    /// bound outside the construct takes it only by a declaration, which binds the name afresh.
    fn parse_write(&mut self, target: &str, call: &ParseCall, ty: Option<&DataType>, here: Here) {
        let Here { id, .. } = here;
        if let Some(ty) = ty
            && !matches!(ty, DataType::Struct(..))
        {
            return self.err(
                Some(id),
                format!(
                    "`{}` gives an object, and `{target}` is declared {}",
                    call.name,
                    ty.spell()
                ),
            );
        }
        let mut input = None;
        if self.bound_outside(target) {
            if ty.is_none() {
                return self.err(
                    Some(id),
                    format!(
                        "`{target}` is bound, and `{}` writes an object its declaration types: write \
                         `{target}: {{...}} = …` to bind it afresh, or `Object.assign($, {}(…))` to write the record",
                        call.name, call.name
                    ),
                );
            }
            // The text is read before the name it may be is bound afresh: `q: {name: str} = q`.
            let Some(slot) = self.input_slot(call.input, here) else {
                return;
            };
            input = Some(slot);
            self.syms.unbind(target);
        }
        self.parse_call(
            call,
            input,
            Dest {
                target,
                ty,
                merge: target.is_empty(),
            },
            here,
        );
    }

    /// `e` as a spread of a variant, where it names one outside a loop: its keys on each row lay into an object's
    /// fields as a parse lays them.
    fn spread_of<'e>(&mut self, e: &'e Expr) -> Option<ParseCall<'e>> {
        if self.in_loop() || !matches!(e.kind, ExprKind::Field(_)) {
            return None;
        }
        let ir = resolve(e, &mut self.syms, &mut self.res).ok()?;
        (ir.ty == DataType::Variant).then(|| ParseCall::variant(e))
    }

    /// `Object.assign(p, a, …)`, `p` a name or `$`: each source writes `p` in turn. A parse or a variant lays its keys
    /// over what `p` holds, typed by it, as a parse into the record does; any other source is `p = {...p, ...a} as T`,
    /// `T` the type `p` holds.
    fn object_assign(&mut self, target: &str, sources: &[Expr], declares: bool, typed: bool, here: Here) {
        let Here { guard, id } = here;
        let root = target == tree::ROOT;
        for source in sources {
            let call = ParseCall::of(source).or_else(|| self.spread_of(source));
            match (call, self.syms.node(target)) {
                (Some(call), _) if root => self.parse_call(
                    &call,
                    None,
                    Dest {
                        target: "",
                        ty: None,
                        merge: true,
                    },
                    here,
                ),
                (None, _) if root && (guard.is_some() || self.in_loop()) => self.assign_keys(source, here),
                (_, None) if !root => return self.err(Some(id), format!("`{target}` is not a field")),
                (_, Some(Node::Leaf(s))) if !root && *self.syms.type_of(*s) != DataType::Variant => {
                    let held = self.syms.type_of(*s).spell();
                    return self.err(Some(id), format!("a spread takes an object, not {held}"));
                }
                (Some(call), _) => {
                    // Typed by what `p` holds, open: it may hold keys after it.
                    let ty = self.syms.node(target).map(|n| shape(&self.syms, n, true));
                    let ty = ty.filter(|t| matches!(t, DataType::Struct(..)));
                    self.parse_call(
                        &call,
                        None,
                        Dest {
                            target,
                            ty: ty.as_ref(),
                            merge: true,
                        },
                        here,
                    );
                }
                (None, _) => {
                    let at = Expr::new(source.span, ExprKind::Field(target.to_string()));
                    let mut spread = Expr::new(source.span, ExprKind::Object(vec![(None, at), (None, source.clone())]));
                    // `p` may hold keys after it, on every row: types are the batch's.
                    if !root {
                        let ty = self.syms.node(target).map(|n| shape(&self.syms, n, true));
                        let value = Box::new(spread);
                        spread = Expr::new(
                            source.span,
                            ExprKind::Coerce {
                                ty: ty.expect("bound"),
                                value,
                            },
                        );
                    }
                    self.assign(target, &spread, declares, typed, here);
                }
            }
        }
    }

    /// `Object.assign($, source)` under a guard or in a loop: each key `source` is known to hold is a write of that
    /// field, so the rows the guard skips keep theirs. A source that may hold other keys is refused, since they would
    /// reach those rows.
    fn assign_keys(&mut self, source: &Expr, here: Here) {
        let Here { id, .. } = here;
        let field = |name: String| Expr::new(source.span, ExprKind::Field(name));
        let keys: Option<Vec<(String, Expr)>> = match &source.kind {
            ExprKind::Object(entries) => entries.iter().map(|(k, v)| Some((k.clone()?, v.clone()))).collect(),
            ExprKind::Field(name) => self.syms.object(name).filter(|o| self.syms.no_keys(o)).map(|o| {
                o.fields
                    .keys()
                    .map(|k| (tree::spell(k).into_owned(), field(tree::child(name, k))))
                    .collect()
            }),
            _ => None,
        };
        let Some(keys) = keys else {
            return self.err(
                Some(id),
                format!(
                    "`{}` may hold keys nothing names, which would reach the rows the `if` or loop skips; write each \
                     key, `key = …`, or merge it outside the `if` or loop",
                    describe_expr(source)
                ),
            );
        };
        for (key, value) in keys {
            self.assign(&key, &value, false, false, here);
        }
    }

    /// `$ = value`: `value` replaces the record. The engine's own `__` fields and the locals stay.
    fn assign_root(&mut self, value: &Expr, here: Here) {
        let Here { guard, id } = here;
        if guard.is_some() || self.in_loop() {
            return self.err(
                Some(id),
                "`$ = …` replaces the record on every row; write it outside the `if` or the loop".to_string(),
            );
        }
        let kept = |c: &Self, p: &Place| tree::is_internal(tree::head(p.name())) || c.syms.is_local(p.name());
        let held = |c: &Self| -> Vec<Place> {
            let places = c.syms.root().places().into_iter().map(|(p, _)| p);
            places.filter(|p| !kept(c, p)).collect()
        };
        // `$ = f(text)`: the text is read, the record goes, and the parse writes a new one.
        if let Some(call) = ParseCall::of(value) {
            let Some(input) = self.input_slot(call.input, Here { guard: None, ..here }) else {
                return;
            };
            self.replace(held(self), Here { guard: None, ..here });
            return self.parse_call(
                &call,
                Some(input),
                Dest {
                    target: "",
                    ty: None,
                    merge: true,
                },
                Here { guard: None, ..here },
            );
        }
        let Some(writes) = self.resolve_writes("", value, id) else {
            return;
        };
        if let [(Place::Leaf(n), r)] = writes.as_slice()
            && n.is_empty()
        {
            let (e, ty) = (describe_expr(value), self.resolved_ty(r).spell());
            return self.err(
                Some(id),
                format!("`$` is the record, an object, and `{e}` is {ty}; write `r: {{...}} = {e}; $ = r;`"),
            );
        }
        // A local and a field of the record cannot share a name.
        if let Some((p, _)) = writes.iter().find(|(p, _)| self.syms.is_local(p.name())) {
            let local = tree::head(p.name());
            return self.err(
                Some(id),
                format!(
                    "`$ = …` writes the field `{local}`, and `{local}` is a local here; give the local another name"
                ),
            );
        }
        let writes: Vec<(Place, Resolved)> = writes
            .into_iter()
            .filter(|(p, _)| !tree::is_internal(tree::head(p.name())))
            .collect();
        let old: Vec<Place> = held(self)
            .into_iter()
            .filter(|p| writes.iter().all(|(w, _)| w != p))
            .collect();
        self.replace(old, Here { guard: None, ..here });
        for (place, r) in writes {
            self.bind_resolved(&place, r, Here { guard: None, ..here });
        }
    }

    /// Whether `name` is bound outside the construct being compiled: a bound name is, unless it is a local the block
    /// opened or a name the construct declared ahead of itself.
    fn bound_outside(&self, name: &str) -> bool {
        let mark = self.scope_marks.last().copied().unwrap_or(0);
        self.syms.node(name).is_some()
            && self.syms.local_index(name).is_none_or(|i| i < mark)
            && !matches!(&self.pass, Pass::Compiling(names) if names.iter().any(|n| n == name))
    }

    /// `writes` to `target`, a bound name that `held` types, each coerced to the type it keeps (Language).
    /// One write of the whole is coerced as `keep_ir` says; a literal or a copied object writes pieces, each a place
    /// `held` has, coerced to its leaf's type, and no place of `held` is left out. Anything else is a compile error
    /// naming the way out. `None` means an error was recorded.
    fn keep_type(
        &mut self,
        target: &str,
        held: &DataType,
        writes: Vec<(Place, Resolved)>,
        value: &Expr,
        id: FunctionId,
    ) -> Option<Vec<(Place, Resolved)>> {
        // A name an earlier error left untyped takes the write without a second report.
        if *held == DataType::Null {
            return Some(writes);
        }
        if let [(Place::Leaf(n), _)] = writes.as_slice()
            && n == target
        {
            let (place, r) = writes.into_iter().next()?;
            // Null over an object takes its leaves out (Language); a guarded one nulls them on its rows.
            if matches!(held, DataType::Struct(..)) && self.resolved_ty(&r) == DataType::Null {
                return Some(vec![(place, r)]);
            }
            return match keep_ir(target, value, self.resolved_ir(r), held) {
                Ok(ir) => Some(vec![(place, Resolved::Ir(ir))]),
                Err(message) => {
                    self.err(Some(id), message);
                    None
                }
            };
        }
        let fits = writes.iter().all(|(place, _)| match place {
            Place::Leaf(p) => matches!(self.syms.node(p), Some(Node::Leaf(_))),
            Place::Unclaimed(p) => self.syms.object(p).is_some_and(|o| o.unclaimed.is_some()),
        }) && self
            .places_at(target)
            .iter()
            .all(|p| writes.iter().any(|(w, _)| w == p));
        if !fits {
            // The pieces are not the shape held: the value, whole, is converted to it, as any value is.
            let ir = match resolve(value, &mut self.syms, &mut self.res) {
                Ok(ir) => ir,
                Err(e) => {
                    self.errors.push(e);
                    return None;
                }
            };
            return match keep_ir(target, value, ir, held) {
                Ok(ir) => Some(vec![(Place::Leaf(target.to_string()), Resolved::Ir(ir))]),
                Err(message) => {
                    self.err(Some(id), message);
                    None
                }
            };
        }
        let mut out = Vec::with_capacity(writes.len());
        for (place, r) in writes {
            let Place::Leaf(p) = &place else {
                out.push((place, r));
                continue;
            };
            let Some(Node::Leaf(leaf)) = self.syms.node(p) else {
                unreachable!("a leaf, checked above")
            };
            let leaf_ty = self.syms.type_of(*leaf).clone();
            let Ok(ir) = keep_ir(p, value, self.resolved_ir(r), &leaf_ty) else {
                // The whole value, named, is the way out.
                let from = match resolve(value, &mut self.syms, &mut self.res) {
                    Ok(ir) => ir.ty,
                    Err(_) => DataType::Struct(vec![], false),
                };
                self.err(Some(id), type_kept(target, value, &from, held));
                return None;
            };
            out.push((place, Resolved::Ir(ir)));
        }
        Some(out)
    }

    /// `r` as an `Ir`.
    fn resolved_ir(&self, r: Resolved) -> Ir {
        match r {
            Resolved::Slot(s) => Ir::new(self.syms.type_of(s).clone(), Span::default(), IrKind::Field(s)),
            Resolved::Unclaimed(s, removed) => unclaimed_ir(s, &removed, Span::default()),
            Resolved::Ir(ir) => ir,
        }
    }

    /// What binding `target` to `value` writes, each place with what fills it, resolved against the tree as it
    /// stands. `None` means an error was recorded, and each place is bound already so a later read does not report
    /// again.
    fn resolve_writes(&mut self, target: &str, value: &Expr, id: FunctionId) -> Option<Vec<(Place, Resolved)>> {
        let writes = match binding::writes(target, value, &self.syms) {
            Ok(writes) => writes,
            Err(message) => {
                self.err(Some(id), message);
                return None;
            }
        };
        let mut resolved: Vec<(Place, Resolved)> = Vec::with_capacity(writes.len());
        for (place, source) in &writes {
            let Some(r) = self.resolve_source(source, id) else {
                // Reported already. Each place binds as its declared type, or null where it has none, so a later read
                // does not report again.
                for (place, source) in &writes {
                    let ty = match (place, source) {
                        (Place::Unclaimed(_), _) => DataType::Variant,
                        (_, Source::Expr(e)) => match &e.kind {
                            ExprKind::Check { ty, .. } | ExprKind::Coerce { ty, .. } => ty.clone(),
                            _ => DataType::Null,
                        },
                        _ => DataType::Null,
                    };
                    self.syms.bind_place(place, ty, Origin::Function(id));
                }
                return None;
            };
            resolved.push((place.clone(), r));
        }
        Some(resolved)
    }

    /// What `source` fills a slot with: a copy is the slot it copies, and anything else what `resolve::source_ir`
    /// computes. `None` means an error was recorded.
    fn resolve_source(&mut self, source: &Source, id: FunctionId) -> Option<Resolved> {
        Some(match source {
            Source::Slot(s) => Resolved::Slot(*s),
            Source::Unclaimed(s, removed) if removed.is_empty() => Resolved::Slot(*s),
            Source::Unclaimed(s, removed) => Resolved::Unclaimed(*s, removed.clone()),
            _ => match source_ir(source, &mut self.syms, &mut self.res) {
                Ok(ir) => Resolved::Ir(flatten(ir)),
                Err(mut e) => {
                    e.function = Some(id);
                    self.errors.push(e);
                    return None;
                }
            },
        })
    }

    fn resolved_ty(&self, r: &Resolved) -> DataType {
        match r {
            Resolved::Slot(s) => self.syms.type_of(*s).clone(),
            Resolved::Unclaimed(..) => DataType::Variant,
            Resolved::Ir(ir) => ir.ty.clone(),
        }
    }

    /// Bind `target` to `writes`, each a place at or under it with what fills it. Everything else at or under `target`
    /// is replaced. A `let` (`declares`) puts the name's own binding from before it out of reach instead, for its
    /// scope to put back when the block ends.
    fn bind_writes(&mut self, target: &str, writes: Vec<(Place, Resolved)>, declares: bool, here: Here) {
        let Here { guard, id } = here;
        let blended = guard.is_some() || self.in_loop();
        let mut writes = self.split_objects(writes, here);
        // `null` over an object nulls its leaves and unclaimed fields, and the name stays the object it was, each leaf
        // at its type, so a later read of the whole object still has one.
        let nulled = !declares
            && matches!(writes.as_slice(), [(Place::Leaf(n), r)] if n == target && self.resolved_ty(r) == DataType::Null);
        if let (true, Some(Node::Object(o))) = (nulled, self.syms.node(target)) {
            let null = |ty: &DataType| Resolved::Ir(Ir::new(ty.clone(), Span::default(), IrKind::Literal(Value::Null)));
            writes = (o.places().into_iter())
                .map(|(p, slot)| (p.under(target), null(self.syms.type_of(slot))))
                .collect();
        }
        // In a `try`, a value that fails leaves its row before anything is written there.
        if self.tries > 0 {
            writes = (writes.into_iter())
                .map(|(place, r)| match r {
                    Resolved::Ir(ir) if !matches!(ir.kind, IrKind::Field(_) | IrKind::Literal(_)) => {
                        let (span, ty) = (ir.span, ir.ty.clone());
                        let slot = self.syms.alloc(place.name().to_string(), ty.clone(), Origin::Temp);
                        self.emit_under(guard, Op::Eval { ir }, slot, ty.clone(), id);
                        (place, Resolved::Ir(Ir::new(ty, span, IrKind::Field(slot))))
                    }
                    r => (place, r),
                })
                .collect();
        }
        if declares {
            self.syms.unbind(target);
        } else if !self.check_shape(target, &writes, blended, id) {
            return;
        }
        self.open_path(target, id);
        let replaced: Vec<Place> = self
            .places_at(target)
            .into_iter()
            .filter(|p| writes.iter().all(|(w, _)| w != p))
            .collect();
        self.replace(replaced, here);
        let empty = writes.is_empty();
        // Each prior is read before anything binds: the places are distinct, and `replace` leaves the written ones.
        for (place, r) in writes {
            self.bind_resolved(&place, r, here);
        }
        // `{}` is an object with nothing in it, open where a write builds it on every row.
        match empty {
            true if guard.is_none() && !self.in_loop() => self.open_new(target, id),
            true => self.syms.make_object(target),
            false => {}
        }
    }

    /// `writes` with every leaf that would hold an object split into that object's leaves and unclaimed fields: no
    /// leaf holds an object (Language). A value its pieces would each compute is computed once, into a slot of
    /// its own.
    fn split_objects(&mut self, writes: Vec<(Place, Resolved)>, here: Here) -> Vec<(Place, Resolved)> {
        let Here { guard, id } = here;
        let mut out = Vec::with_capacity(writes.len());
        for (place, r) in writes {
            let ty = self.resolved_ty(&r);
            let (Place::Leaf(name), DataType::Struct(..)) = (&place, &ty) else {
                out.push((place, r));
                continue;
            };
            let pieces = match r {
                Resolved::Slot(s) => split_struct(name, &Ir::new(ty, Span::default(), IrKind::Field(s))),
                Resolved::Ir(ir) if matches!(ir.kind, IrKind::Field(_)) => split_struct(name, &ir),
                Resolved::Unclaimed(..) => unreachable!("unclaimed fields are a variant, matched above"),
                Resolved::Ir(ir) => {
                    // The pieces the value itself makes: what it is built from says whether it carries unclaimed
                    // fields.
                    let shape: HashSet<Place> = split_struct(name, &ir).into_iter().map(|(p, _)| p).collect();
                    let span = ir.span;
                    let slot = self.syms.alloc(name.clone(), ty.clone(), Origin::Temp);
                    self.emit_under(guard, Op::Eval { ir }, slot, ty.clone(), id);
                    split_struct(name, &Ir::new(ty, span, IrKind::Field(slot)))
                        .into_iter()
                        .filter(|(p, _)| shape.contains(p))
                        .collect()
                }
            };
            out.extend(pieces.into_iter().map(|(p, ir)| (p, Resolved::Ir(ir))));
        }
        out
    }

    /// Every place at or under `name` the tree holds.
    fn places_at(&self, name: &str) -> Vec<Place> {
        match self.syms.node(name) {
            Some(Node::Leaf(_)) => vec![Place::Leaf(name.to_string())],
            Some(Node::Object(o)) => o.places().into_iter().map(|(p, _)| p.under(name)).collect(),
            None => Vec::new(),
        }
    }

    /// Whether the tree lets `target` take `writes`. A leaf above it that holds a value refuses it. So does a write
    /// that makes a leaf an object, or an object a leaf, where the old one stands on the rows a guard or a loop trip
    /// leaves (`blended`): a name is a value or an object on every row.
    fn check_shape(&mut self, target: &str, writes: &[(Place, Resolved)], blended: bool, id: FunctionId) -> bool {
        if let Some(e) = self.syms.write_refused(target) {
            self.err(Some(id), e.message);
            return false;
        }
        if !blended {
            return true;
        }
        // A place that is an object now, or one under a value, at any depth under the target.
        let holds_value = |s: SlotId| *self.syms.type_of(s) != DataType::Null;
        let changed = writes.iter().find_map(|(place, _)| {
            if let (Place::Leaf(n), Some(Node::Object(_))) = (place, self.syms.node(place.name())) {
                return Some((n.clone(), "an object"));
            }
            if let (Place::Unclaimed(n), Some(Node::Leaf(s))) = (place, self.syms.node(place.name())) {
                return holds_value(*s).then(|| (n.clone(), "a value"));
            }
            let (above, s) = self.syms.leaf_above(place.name())?;
            (binding::at_or_under(above, target) && holds_value(s)).then(|| (above.to_string(), "a value"))
        });
        let Some((name, now)) = changed else {
            return true;
        };
        self.err(
            Some(id),
            format!(
                "`{name}` is {now} here, and this write under a guard or in a loop would change that only on the \
                 rows it runs on: a name is a value or an object on every row. Assign `{name}` outside the guard or \
                 loop, or write the fields under it."
            ),
        );
        false
    }

    /// Bind `place` to what `r` fills it with. An unguarded copy outside a loop shares the slot.
    fn bind_resolved(&mut self, place: &Place, r: Resolved, here: Here) {
        let Here { guard, id } = here;
        let ir = match r {
            Resolved::Slot(s) if guard.is_none() && !self.in_loop() => {
                self.syms.put_place(place, s);
                let made = self.syms.origins.len();
                self.syms.aliases.push((s, place.name().to_string(), made, id));
                return;
            }
            Resolved::Unclaimed(s, removed)
                if guard.is_none() && !self.in_loop() && matches!(place, Place::Unclaimed(_)) =>
            {
                self.syms.put_unclaimed(place.name(), s, &removed);
                return;
            }
            Resolved::Slot(s) => Ir::new(self.syms.type_of(s).clone(), Span::default(), IrKind::Field(s)),
            Resolved::Unclaimed(s, removed) => unclaimed_ir(s, &removed, Span::default()),
            Resolved::Ir(ir) => ir,
        };
        // Null written where nothing, or only null, was: still null on every row, under any guard.
        let null = matches!(ir.kind, IrKind::Literal(Value::Null))
            && self.syms.slot_of(place).is_none_or(|s| self.syms.is_null(s))
            && !self.in_loop();
        // Under a guard the write is `guard ? value : prior`, an unbound prior being null.
        let ir = match guard {
            None => ir,
            Some(g) => {
                let prior = match (place, self.syms.slot_of(place)) {
                    (Place::Unclaimed(name), Some(s)) => unclaimed_ir(s, self.syms.removed(name), ir.span),
                    (_, Some(s)) => Ir::new(self.syms.type_of(s).clone(), ir.span, IrKind::Field(s)),
                    (_, None) => Ir::new(DataType::Null, ir.span, IrKind::Literal(Value::Null)),
                };
                let cond = Ir::new(DataType::Bool, ir.span, IrKind::Field(g));
                let Some(ir) = self.blended(blend(Some(cond), ir, prior)) else {
                    return;
                };
                ir
            }
        };
        let ty = match place {
            Place::Unclaimed(_) => DataType::Variant,
            Place::Leaf(_) => ir.ty.clone(),
        };
        let out = self.syms.bind_place(place, ty.clone(), Origin::Function(id));
        if null {
            self.syms.note_null(out);
        }
        self.emit(Op::Eval { ir }, out, ty, id);
    }

    /// Replace each of `places`, bound before a write that does not bind it again: unbound, or null where a guard or a
    /// loop trip leaves the old value standing elsewhere.
    fn replace(&mut self, places: Vec<Place>, here: Here) {
        let Here { guard, .. } = here;
        for place in places {
            if guard.is_none() && !self.in_loop() {
                self.syms.unbind_place(&place);
                continue;
            }
            let ty = match place {
                Place::Unclaimed(_) => DataType::Variant,
                Place::Leaf(_) => DataType::Null,
            };
            let null = Ir::new(ty, Span::default(), IrKind::Literal(Value::Null));
            self.bind_resolved(&place, Resolved::Ir(null), here);
        }
    }

    /// Parse `in_slot` as `call` says into `dest`, once the call is checked against its input and the target: each
    /// field the parser declares, a name under the target, is a write of its name under `here`'s guard, and a dynamic
    /// parse lays every other key into the unclaimed fields of the deepest object at its name. A parse that merges,
    /// into the record or by `Object.assign` into a name, leaves a key the row lacks alone: what was bound there, or
    /// else the record's key, converted to the declared type. Any other parse into a name replaces it, and the name is
    /// unbound before.
    fn parse_into(&mut self, call: &ParseCall, in_slot: SlotId, dest: Dest, here: Here) {
        let Dest { target, ty, merge } = dest;
        let Here { guard, id } = here;
        let (kind, matcher) = match call.kind(self.grok_patterns) {
            Ok(k) => k,
            Err(message) => return self.err(Some(id), message),
        };
        match self.syms.type_of(in_slot) {
            DataType::Variant if kind == ParserKind::Variant => {}
            // Bytes are not text until a `decode` says how to read them.
            DataType::Str | DataType::Null => {}
            DataType::Variant => {
                return self.err(
                    Some(id),
                    format!("`{}` {}", describe_expr(call.input), variant_use(None)),
                );
            }
            t => {
                let mut message = format!(
                    "`{}` parses text, and `{}` is {}",
                    call.name,
                    describe_expr(call.input),
                    t.spell()
                );
                if *t == DataType::Bytes {
                    message.push_str(", which `decode(value, encoding)` reads as text");
                }
                return self.err(Some(id), message);
            }
        }
        let keys = matcher_keys(&kind, &matcher);
        if let Some(k) = keys.iter().flatten().find(|k| tree::is_internal(tree::head(k))) {
            return self.err(
                Some(id),
                format!("`{k}` is the engine's own field; give the capture or column another name"),
            );
        }
        if self.writes_a_local(target, keys.iter().flatten(), id) {
            return;
        }
        let formats = match call.formats() {
            Ok(f) => f,
            Err(message) => return self.err(Some(id), message),
        };
        let declared = self.parse_fields(target, &kind, keys, ty);
        // A name is a value or an object, so a parser declares one or the other.
        for d in &declared {
            if let Some(e) = declared.iter().find(|e| binding::is_under(&e.name, &d.name)) {
                self.err(
                    Some(id),
                    format!(
                        "`{}` and `{}` are both declared, and a name is a value or an object, not both: \
                         declare `{}` as an object type, or only the fields under it",
                        d.name, e.name, d.name
                    ),
                );
                return;
            }
        }

        // A dynamic parser's declared object is its leaves and its unclaimed fields: each leaf is a field the
        // parse writes, and the object takes the keys under its name that no leaf takes.
        let (mut leaves, declared_objects) = match kind.is_dynamic() {
            true => lower_objects(&declared),
            false => (declared, Vec::new()),
        };
        let fields = leaves.iter_mut().map(|d| (d.name.as_str(), &d.ty, &mut d.time_source));
        if let Err(message) = read_formats(target, formats, fields) {
            return self.err(Some(id), message);
        }

        // Every declared field gets a slot of its declared type, whether or not anything reads it.
        //
        // A key the parse does not find leaves the name's value alone (Language): what an earlier step
        // bound there, or else the record's key, coerced to the declared type. So a field with a prior stages
        // into its own slot, and its name takes `parsed ?? prior`. A parser skips a row its guard masks
        // (`Op::Call::guard`), so there the blend keeps the prior.
        if merge {
            self.note_first_writes(leaves.iter().map(|d| (d.name.clone(), d.ty.clone())), guard);
        }
        let mut fields: Vec<Output> = Vec::new();
        let mut priors: Vec<Option<Ir>> = Vec::new();
        for d in &leaves {
            let prior = match merge {
                true => self.prior(&d.name, &d.ty),
                false => None,
            };
            // Named for the field, so an exception it records cites the field.
            let slot = self.syms.alloc(d.name.clone(), d.ty.clone(), Origin::Function(id));
            fields.push(Output {
                name: tree::key_under(target, &d.name).to_string(),
                slot,
                ty: d.ty.clone(),
                time_source: d.time_source.clone(),
            });
            priors.push(prior);
        }

        let objects = match (kind.is_dynamic(), merge) {
            (false, _) => Vec::new(),
            (true, true) => self.parse_objects(target, &leaves, &declared_objects, id),
            // The target and each object under it are new.
            (true, false) => declared_objects
                .iter()
                .map(|(o, closed)| ParseObject {
                    name: tree::key_under(target, o).to_string(),
                    under: None,
                    out: self.syms.alloc(
                        Place::Unclaimed(o.clone()).to_string(),
                        DataType::Variant,
                        Origin::Function(id),
                    ),
                    closed: *closed,
                })
                .collect(),
        };
        // A declared object the tree does not hold is new, and replaces what held its name, as each object a parse
        // into a name writes does.
        let new: Vec<&String> = declared_objects
            .iter()
            .map(|(o, _)| o)
            .filter(|o| !merge || !matches!(self.syms.node(o), Some(Node::Object(_))))
            .collect();
        let parsed: Vec<(String, SlotId)> = leaves
            .iter()
            .zip(&fields)
            .map(|(d, f)| (d.name.clone(), f.slot))
            .collect();
        let out = self.syms.temp(DataType::Bool, Origin::Temp);
        self.emit(
            Op::Call {
                input: in_slot,
                guard,
                outputs: fields,
                call: Call::Parse {
                    kind,
                    matcher,
                    objects: objects.clone(),
                },
            },
            out,
            DataType::Bool,
            id,
        );

        // Each object the parse laid keys into holds them now. A new one is a write of the object: its
        // unclaimed fields and those of each object under it.
        let named: Vec<(String, &ParseObject)> = objects.iter().map(|o| (tree::join(target, &o.name), o)).collect();
        for (name, o) in named.iter().filter(|(n, _)| !new.contains(&n)) {
            self.syms.set_unclaimed(name, Some(o.out));
        }
        for name in new.iter().filter(|o| !new.iter().any(|p| binding::is_under(o, p))) {
            let write = named
                .iter()
                .filter(|(n, _)| binding::at_or_under(n, name))
                .map(|(n, o)| (Place::Unclaimed(n.clone()), Resolved::Slot(o.out)))
                .collect();
            self.bind_writes(name, write, false, here);
        }
        // Each field is a write of its name, under the guard.
        for ((name, slot), prior) in parsed.into_iter().zip(priors) {
            if !self.write_over(&name, slot, prior, here) {
                return;
            }
        }
        // A closed declared object has no unclaimed fields: the parse dropped each key it had there.
        for (name, _) in named.iter().filter(|(_, o)| o.closed) {
            self.syms.set_unclaimed(name, None);
        }
    }

    /// In a construct's typing pass, note each of `names` a parse or a lookup into the record writes first inside the
    /// construct, at its type, so the construct declares it ahead of itself from the record's key (Language).
    fn note_first_writes(&mut self, names: impl Iterator<Item = (String, DataType)>, guard: Option<SlotId>) {
        if guard.is_none() && !self.in_loop() {
            return;
        }
        let unbound: Vec<(String, DataType)> = names.filter(|(n, _)| self.syms.node(n).is_none()).collect();
        let Pass::Typing(typing) = &mut self.pass else {
            return;
        };
        for (name, ty) in unbound {
            match typing.iter_mut().find(|(n, _)| *n == name) {
                Some((_, first)) if first.declared == 0 => first.ty = first.ty.unify(&ty).unwrap_or(DataType::Variant),
                Some(_) => {}
                None => typing.push((name, FirstWrite { ty, declared: 0 })),
            }
        }
    }

    /// What the record's `name` holds before a parse or a lookup writes it, for a row the write finds no key on: what
    /// was bound there, or else the record's key, converted to `ty`.
    fn prior(&self, name: &str, ty: &DataType) -> Option<Ir> {
        let span = Span::default();
        match self.syms.lookup(name) {
            Some(s) if self.syms.is_null(s) => None,
            Some(s) => Some(Ir::new(self.syms.type_of(s).clone(), span, IrKind::Field(s))),
            None if self.syms.node(name).is_none() => self
                .record_key(name, span)
                .map(|(_, key)| crate::plan::resolve::coerce_ir(key, ty, span).expect("a variant coerces")),
            None => None,
        }
    }

    /// `try { body } catch (var: kinds) { catch } ... finally { finally }` under `here`'s guard. The body is a region a
    /// exception leaves (Language); each `catch` runs on the rows holding one of its `kinds`, every kind a `try`
    /// catches where none is named, after their exceptions are cleared, with `var.kind` the first such kind and
    /// `var.message` their text. A row's other exceptions go back on it after the `catch`, in a `try` of their own that
    /// the next `catch` takes, and past the last as if no `catch` had taken them. The `finally` runs on every row that
    /// entered the `try`, whatever left it.
    fn try_catch(&mut self, body: &[Stmt], catches: &[Catch], finally: &[Stmt], here: Here) {
        let Here { guard, id } = here;
        let span = Span::default();
        let entered = guard.filter(|_| !finally.is_empty()).map(|g| self.copy_guard(g, id));
        let mut mask = self.tried(!catches.is_empty(), guard, body, id);
        if catches.is_empty() {
            self.end(mask, id);
        }
        // A typed `catch` gives back what it did not clear: inside the next `catch`'s `try`, or after the last.
        let mut rest = None;
        for (var, kinds, catch) in catches {
            if let Some(op) = rest.take() {
                mask = self.tried(true, guard, &[], id);
                let out = self.syms.temp(DataType::Bool, Origin::Temp);
                self.emit(op, out, DataType::Bool, id);
            }
            let typed = !kinds.is_empty();
            let kinds: Vec<usize> = match typed {
                true => kinds.to_vec(),
                false => (0..ExceptionKind::KINDS.len())
                    .filter(|&i| ExceptionKind::KINDS[i].1.caught())
                    .collect(),
            };
            let kind = self.syms.temp(DataType::Str, Origin::Temp);
            self.emit(
                Op::Try(TryStep::Kind {
                    kinds: kinds.clone(),
                    rows: guard,
                }),
                kind,
                DataType::Str,
                id,
            );
            let text = self.syms.temp(DataType::Str, Origin::Temp);
            let keep = typed || Stmt::any(catch, &|_| true, &mut |s| matches!(s, Stmt::Throw(_)));
            let (names, caught) = (Default::default(), kinds.clone());
            self.emit(
                Op::Try(TryStep::Caught {
                    names,
                    kinds: caught,
                    keep,
                    rows: guard,
                }),
                text,
                DataType::Str,
                id,
            );
            self.end(mask, id);
            let kind_ir = Ir::new(DataType::Str, span, IrKind::Field(kind));
            let func = crate::ops::stdfuncs::StdFunc::IsNull;
            let taken = negate(Ir::new(
                DataType::Bool,
                span,
                IrKind::Call {
                    func,
                    args: vec![kind_ir],
                },
            ));
            // The rows the `Throw` gives back to, though the `catch` may clear `guard` with a `continue`, a `return` or
            // an exception.
            let took = guard.filter(|_| typed).map(|g| self.copy_guard(g, id));
            let mask = self.begin(Region::Block, guard, Some(taken), id);
            let scope = var.as_deref().map(|v| {
                self.catch_scopes.push(self.syms.local_count());
                let scope = self.syms.open_scope(v);
                self.syms.put_place(&Place::Leaf(format!("{v}.kind")), kind);
                self.syms.put_place(&Place::Leaf(format!("{v}.message")), text);
                scope
            });
            self.code_block(
                catch,
                Here {
                    guard: Some(mask),
                    ..here
                },
            );
            if let Some(scope) = scope {
                self.syms.close_scope(&scope);
                self.catch_scopes.pop();
            }
            self.end(mask, id);
            rest = typed.then_some(Op::Try(TryStep::Throw {
                caught: text,
                rows: took,
                handled: kinds,
            }));
        }
        if let Some(op) = rest {
            let out = self.syms.temp(DataType::Bool, Origin::Temp);
            self.emit(op, out, DataType::Bool, id);
        }
        // An exception stops a `finally` where it happens, as it stops a body.
        if !finally.is_empty() {
            let mask = self.tried(false, entered, finally, id);
            self.end(mask, id);
        }
    }

    /// A copy of `guard`, whose own bits clear as rows leave.
    fn copy_guard(&mut self, guard: SlotId, id: FunctionId) -> SlotId {
        let copy = self.syms.temp(DataType::Bool, Origin::Guard);
        let ir = Ir::new(DataType::Bool, Span::default(), IrKind::Field(guard));
        self.emit(Op::Eval { ir }, copy, DataType::Bool, id);
        copy
    }

    /// `throw e` on the rows `here`'s guard selects. The exceptions go back on the rows they were cleared from, so a
    /// loop opened inside the `catch`, whose trips number their rows afresh, cannot hold one.
    fn throw(&mut self, e: &Expr, here: Here) {
        let Here { guard, id } = here;
        let ExprKind::Field(name) = &e.kind else {
            unreachable!("the parser reads a name after `throw`")
        };
        let Some(caught) = self.syms.lookup(&format!("{name}.message")) else {
            let why = format!("`throw {name}`: `{name}` no longer holds what its `catch` took");
            return self.err(Some(id), why);
        };
        if self.loop_firsts.last().is_some_and(|&first| caught < first) {
            let why = format!(
                "`throw {name}` cannot appear inside a loop in its `catch`: set a flag in the loop and \
                 `if (flag) throw {name}` after it"
            );
            return self.err(Some(id), why);
        }
        let out = self.syms.temp(DataType::Bool, Origin::Temp);
        let op = Op::Try(TryStep::Throw {
            caught,
            rows: guard,
            handled: Vec::new(),
        });
        self.emit(op, out, DataType::Bool, id);
    }

    /// `stmts` in a `try` region on the rows `outer` selects, which an exception leaves. Returns its mask; the caller ends
    /// it.
    fn tried(&mut self, catches: bool, outer: Option<SlotId>, stmts: &[Stmt], id: FunctionId) -> SlotId {
        let mask = self.begin(Region::Try { catches }, outer, None, id);
        self.tries += 1;
        self.code_block(stmts, Here { guard: Some(mask), id });
        self.tries -= 1;
        mask
    }

    /// Open `region` on the rows `outer` selects where `cond` holds: its mask, the guard of what it holds.
    fn begin(&mut self, region: Region, outer: Option<SlotId>, cond: Option<Ir>, id: FunctionId) -> SlotId {
        let field = |s| Ir::new(DataType::Bool, Span::default(), IrKind::Field(s));
        let mask = match cond {
            Some(cond) => Some(guard_ir(outer, cond)),
            None => outer.map(field),
        };
        let slot = self.syms.temp(DataType::Bool, Origin::Guard);
        self.emit(Op::Begin { region, mask }, slot, DataType::Bool, id);
        self.regions.push(region);
        slot
    }

    /// Close the region `begin` opened with `mask`.
    fn end(&mut self, mask: SlotId, id: FunctionId) {
        self.regions.pop();
        let out = self.syms.temp(DataType::Bool, Origin::Temp);
        self.emit(Op::End { mask }, out, DataType::Bool, id);
    }

    /// `stmts` in a region of their own (`begin`).
    fn region(&mut self, region: Region, outer: Option<SlotId>, cond: Option<Ir>, stmts: &[Stmt], id: FunctionId) {
        let mask = self.begin(region, outer, cond, id);
        self.code_block(stmts, Here { guard: Some(mask), id });
        self.end(mask, id);
    }

    /// `break`, `continue` or an inlined `return`: the rows `here`'s guard selects leave every region out to the
    /// nearest `to`.
    fn leave(&mut self, to: Region, here: Here) {
        let Here { guard, id } = here;
        match guard {
            Some(rows) if self.regions.contains(&to) => {
                let out = self.syms.temp(DataType::Bool, Origin::Temp);
                self.emit(Op::Leave { to, rows }, out, DataType::Bool, id);
            }
            _ => {
                let word = if to == Region::Trip { "continue" } else { "break" };
                self.err(Some(id), format!("`{word}` is only valid inside a loop"));
            }
        }
    }

    /// A loop's body: where a row can leave it, at a `while`'s test, inside a `try` or by a `Leave`, it runs in a
    /// region for the loop and one for the trip inside it. A `while`'s `next` and test follow the trip, in the loop's
    /// region. Returns the loop region's mask: a row whose bit is clear is done.
    fn trip(&mut self, test: Option<(&Expr, &[Stmt])>, body: &[Stmt], id: FunctionId) -> Option<SlotId> {
        let enter = |s: &Stmt| !matches!(s, Stmt::Function { .. });
        if test.is_none() && self.tries == 0 && !Stmt::any(body, &enter, &mut |s| matches!(s, Stmt::Leave { .. })) {
            self.code_block(body, Here { guard: None, id });
            return None;
        }
        let stay = self.begin(Region::Loop, None, None, id);
        self.region(Region::Trip, Some(stay), None, body, id);
        if let Some((cond, next)) = test {
            self.code_block(next, Here { guard: Some(stay), id });
            let leave = [Stmt::Leave {
                to: Region::Loop,
                span: cond.span,
            }];
            self.if_stmt(cond, [&[], &leave], WHILE_CONDITION, Here { guard: Some(stay), id });
        }
        self.end(stay, id);
        Some(stay)
    }

    /// `delete(name)`: the reverse of a declaration (Language). At the top level the name leaves the tree,
    /// and its key leaves the unclaimed fields of the object it is in. Inside an `if` or a loop a bound name is a
    /// compile error, as its declaration would be there, unless `clears`: a step's delete, `move`'s or a Remove's,
    /// clears it on the rows selected; a key an open object may hold is taken out on the rows selected.
    fn delete(&mut self, name: &str, clears: bool, here: Here) {
        let Here { guard, id } = here;
        if name == tree::ROOT {
            return self.err(
                Some(id),
                "`$` is the record and cannot be deleted; write `$ = {}`".to_string(),
            );
        }
        if let Some(e) = self.syms.write_refused(name) {
            return self.err(Some(id), e.message);
        }
        let bound = self.syms.node(name).is_some();
        let held = bound || self.record_key(name, Span::default()).is_some();
        if !held {
            return self.err(Some(id), format!("`{name}` is not a field"));
        }
        if guard.is_some() || self.in_loop() {
            if bound && self.bound_outside(name) && !clears {
                return self.err(
                    Some(id),
                    format!(
                        "`{name}` is bound outside this `if`, loop or `try`, and `delete` takes a name out on every row; \
                         write `{name} = null` to clear it on these rows"
                    ),
                );
            }
            let null = Expr::new(Span::default(), ExprKind::Literal(Value::Null));
            return self.assign(name, &null, false, false, here);
        }
        // A local is no key of the record.
        if self.syms.is_local(name) {
            self.syms.unbind(name);
            return;
        }
        self.open_path(name, id);
        let gone: Vec<SlotId> = self
            .places_at(name)
            .iter()
            .filter_map(|p| self.syms.slot_of(p))
            .collect();
        self.syms.deleted_slots.extend(gone);
        self.syms.unbind(name);
        let (parent, key) = tree::parent(name);
        self.syms.remove_key(parent, &key);
    }

    /// Write what the parse function `call` gives to `dest`, as `parse_into` does. Without a declared type, an object
    /// one, a field the target holds keeps its type, and a new one is open.
    fn parse_call(&mut self, call: &ParseCall, input: Option<SlotId>, dest: Dest, here: Here) {
        let Some(in_slot) = input.or_else(|| self.input_slot(call.input, here)) else {
            return;
        };
        if call.name == "lookup" {
            return self.lookup_call(call, in_slot, dest, here);
        }
        self.parse_into(call, in_slot, dest, here);
    }

    /// The slot holding `e`'s value: a field's own, or one `e` is computed into under `here`'s guard.
    fn input_slot(&mut self, e: &Expr, here: Here) -> Option<SlotId> {
        let Here { guard, id } = here;
        let input = self.expr_ast(e, id)?;
        Some(match input.kind {
            IrKind::Field(slot) => slot,
            _ => {
                let ty = input.ty.clone();
                let slot = self.syms.temp(ty.clone(), Origin::Temp);
                self.emit_under(guard, Op::Eval { ir: input }, slot, ty, id);
                slot
            }
        })
    }

    /// `lookup(table, key, {…})` into `dest`, as `parse_call` writes a parse: each output a field of the target, at its
    /// declared type or the type the field holds, else `str`.
    fn lookup_call(&mut self, call: &ParseCall, in_slot: SlotId, dest: Dest, here: Here) {
        let Here { id, .. } = here;
        let Dest { target, ty, merge } = dest;
        let args = match call.lookup() {
            Ok(args) => args,
            Err(message) => return self.err(Some(id), message),
        };
        let tables = self.tables;
        let source = match tables.get(&args.table) {
            Some(Ok(source)) => source,
            Some(Err(e)) => return self.err(Some(id), format!("lookup table `{}`: {e}", args.table)),
            None => {
                let mut names: Vec<&str> = tables.keys().map(String::as_str).collect();
                names.sort_unstable();
                let has = match names.is_empty() {
                    true => "none".to_string(),
                    false => name_hint(names.into_iter()),
                };
                return self.err(
                    Some(id),
                    format!("no lookup table is named `{}`; the bundle has {has}", args.table),
                );
            }
        };
        if self.writes_a_local(target, args.fields.iter().map(|(out, ..)| out), id) {
            return;
        }
        let mut outputs: Vec<LookupOutput> = args
            .fields
            .into_iter()
            .map(|(out, column, default)| {
                let field = tree::join(target, &out);
                let ty = match self.held_type(target, &field, ty) {
                    Some(DataType::Array(t)) => *t,
                    Some(DataType::Null | DataType::Variant) | None => DataType::Str,
                    Some(t) => t,
                };
                LookupOutput {
                    source: column,
                    field,
                    default,
                    time_source: auto_time(&ty),
                    ty,
                }
            })
            .collect();
        let fields = outputs
            .iter_mut()
            .map(|o| (o.field.as_str(), &o.ty, &mut o.time_source));
        if let Err(message) = call.formats().and_then(|f| read_formats(target, f, fields)) {
            return self.err(Some(id), message);
        }
        let spec = LookupSpec {
            source,
            match_kind: args.match_kind,
            multiplicity: args.multiplicity,
            key_column: &args.key_column,
            outputs: &outputs,
            merge,
        };
        self.lookup_into(&describe_expr(call.input), in_slot, &spec, here);
    }

    /// Whether a parse into `target`, the record, writes a key a local names: a second declaration of the local, so a
    /// compile error.
    fn writes_a_local<'k>(&mut self, target: &str, keys: impl IntoIterator<Item = &'k String>, id: FunctionId) -> bool {
        if !target.is_empty() {
            return false;
        }
        let local = keys.into_iter().find(|k| self.syms.is_local(k));
        if let Some(name) = &local {
            self.err(Some(id), local_declared(name));
        }
        local.is_some()
    }

    /// The type of `name`, a field of `target`: what `ty`, the target's declared type, gives it, else what the tree
    /// holds.
    fn held_type(&self, target: &str, name: &str, ty: Option<&DataType>) -> Option<DataType> {
        match ty {
            Some(ty) => tree::segments(tree::key_under(target, name))
                .try_fold(ty, |t, key| match t {
                    DataType::Struct(fields, _) => fields.iter().find(|(k, _)| *k == key).map(|(_, t)| t),
                    _ => None,
                })
                .cloned(),
            None => self.syms.lookup(name).map(|s| self.syms.type_of(s).clone()),
        }
    }

    /// What a parse into `target` declares: every field the target holds, at its type, where the parse may find any
    /// key; the keys it can find, where it finds only those (`keys`), each at the type the target or `ty` holds, else
    /// `str`. A new target with no `ty` is an open object.
    fn parse_fields(
        &self,
        target: &str,
        kind: &ParserKind,
        keys: Option<Vec<String>>,
        ty: Option<&DataType>,
    ) -> Vec<DeclaredField> {
        let field = |name: String, ty: DataType| DeclaredField {
            time_source: auto_time(&ty),
            name,
            ty,
        };
        // A declared object type names fields the parse may not find, which are null; a closed one takes no other key.
        if let Some(keys) = keys {
            let (mut declared, open) = match ty {
                Some(t @ DataType::Struct(_, open)) => {
                    (lower_objects(&[field(target.to_string(), t.clone())]).0, *open)
                }
                _ => (Vec::new(), true),
            };
            for k in keys {
                let name = tree::join(target, &k);
                if open && declared.iter().all(|d| d.name != name) {
                    let ty = self.held_type(target, &name, ty).unwrap_or(DataType::Str);
                    declared.push(field(name, ty));
                }
            }
            return declared;
        }
        debug_assert!(kind.is_dynamic());
        if let Some(ty) = ty {
            return vec![field(target.to_string(), ty.clone())];
        }
        // A name the parse writes is the object it held, which a guard's typing pass declared: its type, unclaimed
        // fields and all.
        if !target.is_empty() {
            let ty = match self.syms.node(target) {
                Some(n @ Node::Object(_)) => node_type(&self.syms, n),
                _ => DataType::Struct(Vec::new(), true),
            };
            return vec![field(target.to_string(), ty)];
        }
        self.syms
            .root()
            .leaves()
            .into_iter()
            .filter(|(name, _)| {
                let head = tree::head(name);
                !tree::is_internal(head) && !self.ingest.iter().any(|n| n == head) && !self.syms.is_local(name)
            })
            .map(|(name, slot)| (name, self.syms.type_of(slot).clone()))
            .filter(|(_, ty)| *ty != DataType::Null)
            .map(|(name, ty)| field(name, ty))
            .collect()
    }

    /// Look `in_slot`, the key `input` names, up in a table as `spec` says, each output a write of its name under
    /// `here`'s guard.
    fn lookup_into(&mut self, input: &str, in_slot: SlotId, spec: &LookupSpec, here: Here) {
        let Here { guard, id } = here;
        // A key is read, so a variant one needs its declaration. A `null` one belongs to every type, and
        // misses on every row.
        let in_ty = match self.syms.type_of(in_slot).clone() {
            DataType::Variant => {
                self.err(
                    Some(id),
                    format!(
                        "lookup input `{input}` {}",
                        variant_use(Some(&format!("`{input}: str = {input};`")))
                    ),
                );
                return;
            }
            t => t,
        };
        if !matches!(
            in_ty,
            DataType::Null | DataType::Bool | DataType::I64 | DataType::F64 | DataType::Str | DataType::Bytes
        ) {
            self.err(
                Some(id),
                format!(
                    "lookup input `{input}` has type {}, which can't be used as a lookup key",
                    in_ty.spell()
                ),
            );
            return;
        }
        if spec.outputs.is_empty() {
            self.err(Some(id), "lookup has no output columns".to_string());
            return;
        }

        let table = match spec.source {
            LookupSource::Csv { file, delimiter } => {
                let loaded = file.map_stored().and_then(|(map, stored)| {
                    let (shown, key) = (file.shown(), spec.key_column);
                    crate::lookup::RowTable::from_csv(&shown, map, stored.as_deref(), *delimiter, key, spec.match_kind)
                });
                match loaded {
                    Ok(t) => crate::lookup::Table::Row(t),
                    Err(e) => {
                        self.err(Some(id), format!("lookup: {e}"));
                        return;
                    }
                }
            }
            LookupSource::Inline { headers, rows } => {
                match crate::lookup::RowTable::build(headers.clone(), rows.clone(), spec.key_column, spec.match_kind) {
                    Ok(t) => crate::lookup::Table::Row(t),
                    Err(e) => {
                        self.err(Some(id), format!("lookup: {e}"));
                        return;
                    }
                }
            }
            LookupSource::Mmdb { file } => {
                if spec.match_kind != crate::plan::config::MatchKind::Cidr {
                    self.err(
                        Some(id),
                        "an MMDB lookup source must use CIDR matching (it has no other \
                         notion of a key column) -- its own IP trie is the match"
                            .to_string(),
                    );
                    return;
                }
                if spec.multiplicity == crate::plan::config::Multiplicity::All {
                    self.err(
                        Some(id),
                        "an MMDB lookup finds at most one record by construction (its \
                         trie descent), so \"all matches\" has nothing to return more \
                         than \"best match\" already would"
                            .to_string(),
                    );
                    return;
                }
                match file
                    .map()
                    .and_then(|map| crate::lookup::mmdb::MmdbTable::from_map(&file.shown(), map))
                {
                    Ok(m) => crate::lookup::Table::Mmdb(m),
                    Err(e) => {
                        self.err(Some(id), format!("lookup: {e}"));
                        return;
                    }
                }
            }
        };

        // A row table's columns are known; an MMDB record's shape is not, so a bad path there is a runtime
        // miss.
        if let crate::lookup::Table::Row(t) = &table {
            for o in spec.outputs.iter() {
                if t.column(&o.source).is_none() {
                    self.err(
                        Some(id),
                        format!(
                            "lookup output `{}` reads column `{}`, which is not one of \
                             this table's columns",
                            o.field, o.source
                        ),
                    );
                    return;
                }
            }
        }

        // The loader admits only these, but a `PipelineConfig` is built by hand too, and the executor's
        // conversion has no arm for anything else.
        for o in spec.outputs.iter() {
            let scalar = matches!(
                o.ty,
                DataType::Str | DataType::I64 | DataType::F64 | DataType::Bool | DataType::DateTime
            );
            if !scalar || (o.time_source.is_some() != (o.ty == DataType::DateTime)) {
                self.err(
                    Some(id),
                    format!(
                        "lookup output `{}` is {}, but an output is one value: str, i64, f64, bool or datetime",
                        o.field, o.ty
                    ),
                );
                return;
            }
        }

        // A typed output over a row table is checked cell by cell here, so the worker never meets text
        // that does not fit. An MMDB value is met at run time.
        if let crate::lookup::Table::Row(t) = &table {
            for o in spec.outputs.iter().filter(|o| o.ty != DataType::Str) {
                let col = t.column(&o.source).expect("checked above");
                if t.all_fit(col, &o.ty) {
                    continue;
                }
                if let Some((n, cell)) = t
                    .cells(col)
                    .enumerate()
                    .find(|(_, cell)| !crate::lookup::fits(cell, &o.ty, o.time_source.as_ref()))
                {
                    self.err(
                        Some(id),
                        format!(
                            "lookup output `{}` is {}, but column `{}` of this table holds {cell:?} at row {}",
                            o.field,
                            o.ty,
                            o.source,
                            n + 1
                        ),
                    );
                    return;
                }
            }
        }

        // Each output writes into a slot of its own, and its name takes it, over the name's prior where the lookup
        // merges, blended under the guard. The lookup skips a row the guard does not take.
        let all = spec.multiplicity == crate::plan::config::Multiplicity::All;
        let outputs: Vec<Output> = spec
            .outputs
            .iter()
            .map(|o| {
                let out_ty = if all {
                    DataType::Array(Box::new(o.ty.clone()))
                } else {
                    o.ty.clone()
                };
                Output {
                    name: o.source.clone(),
                    // Named for the field, so an exception it records cites the field.
                    slot: self.syms.alloc(o.field.clone(), out_ty, Origin::Function(id)),
                    ty: o.ty.clone(),
                    time_source: o.time_source.clone(),
                }
            })
            .collect();
        let looked_up: Vec<(String, SlotId)> = spec
            .outputs
            .iter()
            .zip(&outputs)
            .map(|(o, out)| (o.field.clone(), out.slot))
            .collect();

        if spec.merge {
            self.note_first_writes(spec.outputs.iter().map(|o| (o.field.clone(), o.ty.clone())), guard);
        }
        let marker = self.syms.temp(DataType::Bool, Origin::Temp);
        self.emit(
            Op::Call {
                input: in_slot,
                guard,
                outputs,
                call: Call::Lookup(crate::lookup::Lookup {
                    table: std::sync::Arc::new(table),
                    multiplicity: spec.multiplicity,
                    defaults: spec.outputs.iter().map(|o| o.default.clone()).collect(),
                }),
            },
            marker,
            DataType::Bool,
            id,
        );
        for (name, slot) in looked_up {
            let prior = match spec.merge {
                true => self.prior(&name, self.syms.type_of(slot)),
                false => None,
            };
            if !self.write_over(&name, slot, prior, here) {
                return;
            }
        }
    }

    /// `slot`, a call's output, written for `name` under `here`'s guard, over `prior` on the rows it holds null:
    /// `slot ?? prior`. The call leaves a row the guard masks null, so over a prior the blend keeps the prior there
    /// already and the guard is not needed. `false` means an error was recorded.
    fn write_over(&mut self, name: &str, slot: SlotId, prior: Option<Ir>, here: Here) -> bool {
        let Here { guard, .. } = here;
        let blends = prior.is_some();
        let (value, guard) = match prior {
            None => (Resolved::Slot(slot), guard),
            Some(prior) => {
                let raw = Ir::new(self.syms.type_of(slot).clone(), prior.span, IrKind::Field(slot));
                let Some(ir) = self.blended(blend(None, raw, prior)) else {
                    return false;
                };
                (Resolved::Ir(ir), None)
            }
        };
        self.bind_writes(
            name,
            vec![(Place::Leaf(name.to_string()), value)],
            false,
            Here { guard, ..here },
        );
        if blends {
            self.syms.blends.extend(self.syms.lookup(name));
        }
        true
    }

    /// `ir` flattened, or `None` with its error recorded.
    fn blended(&mut self, ir: Result<Ir, CompileError>) -> Option<Ir> {
        ir.map(flatten).map_err(|e| self.errors.push(e)).ok()
    }

    /// `__live` false on the rows `dropped` is true on.
    fn drop_where(&mut self, dropped: Ir, id: FunctionId) {
        let span = dropped.span;
        let live = self.syms.field_lookup("__live").unwrap();
        let ir = Ir::new(
            DataType::Bool,
            span,
            IrKind::Binary {
                op: BinOp::And,
                lhs: Box::new(Ir::new(DataType::Bool, span, IrKind::Field(live))),
                rhs: Box::new(negate(dropped)),
            },
        );
        let out = self.syms.field_bind("__live", DataType::Bool, Origin::Function(id));
        self.emit(Op::Eval { ir: flatten(ir) }, out, DataType::Bool, id);
    }

    /// `unroll(field, keep_parent_fields)`: one row per element of `field`, which is rebound to the element.
    fn unroll(&mut self, field: &str, keep_parent_fields: bool, here: Here) {
        let Here { guard, id } = here;
        if self.tries > 0 {
            return self.err(
                Some(id),
                "`unroll` cannot appear inside a `try`: it changes how many rows there are, and the exceptions the \
                 `try` catches are counted by row. Unroll before the `try`",
            );
        }
        if guard.is_some() || self.in_loop() {
            self.err(
                Some(id),
                "Unroll cannot appear inside a conditional or a loop: it changes how many rows there are, and a \
                 row the condition does not take would have no value for the field it unrolls. Unroll first, \
                 then filter or drop the rows",
            );
            return;
        }
        let Some(array_slot) = self.syms.lookup(field) else {
            self.err(Some(id), format!("unroll field `{field}` is not bound"));
            return;
        };
        let array_ty = self.syms.layout[array_slot].clone();
        let DataType::Array(element_ty) = array_ty else {
            self.err(
                Some(id),
                format!("unroll field `{field}` has type {array_ty:?}, expected Array(_)"),
            );
            return;
        };
        let element_ty = *element_ty;

        let out_slot = self
            .syms
            .alloc(field.to_string(), element_ty.clone(), Origin::Function(id));
        let live_slot = self.syms.field_lookup("__live").unwrap();

        // `__`-prefixed names are control state, and a local is the step's own: neither is a parent field. A field a
        // local hides is one, though the block's end puts the name back.
        let syms = &self.syms;
        let kept = |p: &Place| keep_parent_fields || p.name().starts_with("__") || syms.is_local(p.name());
        let places = syms.root().places();
        let parent: Vec<SlotId> = match keep_parent_fields {
            true => Vec::new(),
            false => places
                .iter()
                .filter(|(p, _)| !kept(p))
                .map(|(_, s)| *s)
                .chain(syms.hidden_field_slots())
                .collect(),
        };
        let kept: Vec<Place> = places.iter().filter(|(p, _)| kept(p)).map(|(p, _)| p.clone()).collect();
        // A name the unroll keeps takes a slot of its own where it shares one the unroll empties: the array
        // it consumes, or a parent field it clears.
        let emptied: HashSet<SlotId> = parent.iter().copied().chain([array_slot]).collect();
        for (place, slot) in &places {
            if kept.contains(place) && emptied.contains(slot) && *place != Place::Leaf(field.to_string()) {
                self.unshare(place, id);
            }
        }
        let mut clear: Vec<SlotId> = Vec::new();
        for slot in parent {
            if !clear.contains(&slot) {
                clear.push(slot);
            }
        }
        // A slot no name holds, nor a local hides, is dead, so no row copies it: a staging slot a parse wrote, a
        // temporary.
        let live: HashSet<SlotId> = places
            .iter()
            .map(|(_, s)| *s)
            .chain(self.syms.hidden_slots())
            .chain(clear.iter().copied())
            .collect();
        clear.extend((0..out_slot).filter(|s| !live.contains(s)));
        self.emit(
            Op::Unroll {
                array: array_slot,
                live: live_slot,
                clear,
            },
            out_slot,
            element_ty,
            id,
        );
        // `field` is rebound to the element.
        let write = vec![(Place::Leaf(field.to_string()), Resolved::Slot(out_slot))];
        self.bind_writes(field, write, false, Here { guard: None, ..here });
    }

    /// Compile one statement list under `here`'s guard. A name this list `let`s is a local from its `let` to the end of
    /// the list: bound inside a scope that hides every `name.*` from outside and takes every `name.*` bound inside with
    /// it when the block ends. A write to the name before its `let` is a write to the name outside.
    fn code_block(&mut self, stmts: &[Stmt], here: Here) {
        let Here { id, .. } = here;
        // By the name the user wrote: two `let`s of one name are two locals once the inliner renames them.
        let mut names: Vec<std::borrow::Cow<str>> = Vec::new();
        for stmt in stmts {
            if let Stmt::Let { name, .. } = stmt {
                let name = crate::expr::inline::user_names(name);
                if names.contains(&name) {
                    self.err(Some(id), format!("`{name}` is already declared in this code block"));
                    return;
                }
                names.push(name);
            }
        }
        self.scope_marks.push(self.syms.local_count());
        let mut scopes: Vec<Scope> = Vec::new();
        for stmt in stmts {
            if let Stmt::Let { name, .. } = stmt {
                if !self.syms.is_local(name) && self.syms.node(name).is_some() {
                    self.warn(id, format!("the local `{name}` hides the field `{name}`"));
                }
                scopes.push(self.syms.open_scope(name));
            }
            self.statement(stmt, here);
        }
        for scope in scopes.iter().rev() {
            self.syms.close_scope(scope);
        }
        self.scope_marks.pop();
    }

    /// Compile one statement under `here`'s guard.
    fn statement(&mut self, stmt: &Stmt, here: Here) {
        let Here { guard, id } = here;
        // A caught exception is read, never written: `throw` gives back what its `catch` took.
        let written: Vec<&String> = match stmt {
            Stmt::Assign { target, .. } | Stmt::IndexAssign { target, .. } | Stmt::Unroll { field: target, .. } => {
                vec![target]
            }
            Stmt::Delete { names, .. } => names.iter().collect(),
            _ => Vec::new(),
        };
        let caught = |n: &&String| (self.syms.local_index(n)).is_some_and(|i| self.catch_scopes.contains(&i));
        if let Some(n) = written.into_iter().find(caught) {
            return self.err(
                Some(id),
                format!("`{n}` is a caught exception, which is read and never written"),
            );
        }
        {
            match stmt {
                // `expr::inline::expand` removes these before compilation.
                Stmt::Function { name, .. } => {
                    self.err(
                        Some(id),
                        format!("internal: `function {name}` reached the compiler un-inlined"),
                    );
                }
                Stmt::Return(_) => {
                    self.err(
                        Some(id),
                        "internal: `return` reached the compiler un-inlined".to_string(),
                    );
                }
                Stmt::Leave { to, .. } => self.leave(*to, here),
                // An `if` or a loop that writes a name nothing binds is a construct: typed in a pass of its own, then
                // compiled (`construct`). One that writes only bound names has nothing for that pass to find.
                Stmt::If { .. } | Stmt::For { .. } | Stmt::While { .. } | Stmt::Try { .. } | Stmt::Body(_) => {
                    let mut names = Vec::new();
                    assigned(std::slice::from_ref(stmt), &mut names);
                    // A write of the record, `$`, is one, and so is `Object.assign` into a name: each writes names only
                    // its value knows.
                    let spreads = Stmt::find_map(std::slice::from_ref(stmt), &|_| true, &mut |s| match s {
                        Stmt::Assign { value, .. } => {
                            matches!(&value.kind, ExprKind::Call { name, .. } if name == "Object.assign").then_some(())
                        }
                        _ => None,
                    });
                    let first_writes = spreads.is_some()
                        || names
                            .iter()
                            .any(|n| self.syms.node(n).is_none() && !self.syms.is_local(n) && !tree::is_internal(n));
                    match first_writes {
                        true => self.construct(here, |c| c.construct_stmt(stmt, here)),
                        false => self.construct_stmt(stmt, here),
                    }
                }
                // A `let` puts the name's own binding from before it out of reach, whatever its value binds. The scope
                // puts it back when the block ends.
                Stmt::Let { name, value } => self.assign(name, value, true, true, here),
                Stmt::Assign { target, value, .. } if self.folding.last().is_some_and(|f| f.accs.contains(target)) => {
                    self.fold_write(target, value, here)
                }
                Stmt::Assign { target, value, typed } => self.assign(target, value, false, *typed, here),
                Stmt::Declare { name, ty } => self.declare(name, ty, id),
                // Checked, then discarded: there are no side effects.
                Stmt::Expr(e) => {
                    self.answer_cut(e, id);
                    self.expr_ast(e, id);
                }
                // A computed key writes a position of an array, or a key of an open object. A literal key is a name the
                // parser folds into the target.
                Stmt::IndexAssign { target, index, value } => {
                    if self.holds_array(target) {
                        let write = ExprKind::SetElement {
                            array: Box::new(Expr::new(index.span, ExprKind::Field(target.clone()))),
                            index: Box::new(index.clone()),
                            value: Box::new(value.clone()),
                        };
                        self.assign(target, &Expr::new(value.span, write), false, false, here);
                    } else {
                        self.keyed_write(target, index, value, here);
                    }
                }
                Stmt::Delete { names, clears, .. } => {
                    for name in names {
                        self.delete(name, *clears, here);
                    }
                }
                Stmt::Drop(span) => {
                    if self.in_loop() {
                        return self.err(
                            Some(id),
                            "`drop()` cannot appear inside a loop: it drops the row, which no trip owns; set a flag \
                             in the loop and `if (flag) drop()` after it",
                        );
                    }
                    let dropped = match guard {
                        Some(g) => Ir::new(DataType::Bool, *span, IrKind::Field(g)),
                        None => Ir::new(DataType::Bool, *span, IrKind::Literal(Value::Bool(true))),
                    };
                    self.drop_where(dropped, id);
                }
                Stmt::Unroll { field, keep, .. } => self.unroll(field, *keep, here),
                Stmt::Throw(e) => self.throw(e, here),
            }
        }
    }

    /// Compile a construct, an `if` or a loop, whose body `f` compiles. A typing pass finds each name the construct
    /// first writes and the type every write to it in the construct blends to, or the one a declaration there
    /// states, and the construct is compiled again with each such name declared before it (Language): from
    /// the record's key where the record may hold one, else null on every row. A construct inside one compiles once,
    /// for the pass that is on: the outermost's typing pass sees every write in it, and declares what it first writes
    /// ahead of the outermost.
    fn construct(&mut self, here: Here, f: impl Fn(&mut Self)) {
        let Here { id, .. } = here;
        if !matches!(self.pass, Pass::Outside) {
            return f(self);
        }
        let saved = self.clone();
        self.pass = Pass::Typing(Vec::new());
        f(self);
        let Pass::Typing(found) = std::mem::replace(&mut self.pass, Pass::Outside) else {
            unreachable!("the typing pass just ran")
        };
        *self = saved;
        let mut names = Vec::new();
        for (name, first) in found {
            // A value with no type binds nothing (Language): the write itself says so. Where the record may
            // hold a key of the name, the rows no write runs on keep it, so the name binds, as a variant.
            let keyed = self.record_key(&name, Span::default()).is_some();
            if holds_null(&first.ty) && !keyed {
                continue;
            }
            // A refused name is declared all the same, so the construct compiles against the type it was going to
            // have and reports nothing more.
            if first.declared > 1 {
                self.err(
                    Some(id),
                    format!("`{name}` is declared twice in one `if` or loop; a type is stated once"),
                );
            }
            let span = Span::default();
            let ty = match first.declared > 1 || holds_null(&first.ty) {
                true => DataType::Variant,
                false => first.ty,
            };
            // Where the record may hold a key of the name, a row no write runs on keeps it: the name is declared from
            // the key, `name: T = name`, which converts it.
            let prior = match self.record_key(&name, span) {
                Some(_) => ExprKind::Field(name.clone()),
                None => ExprKind::Literal(Value::Null),
            };
            let decl = Expr::new(
                span,
                ExprKind::Check {
                    ty,
                    value: Box::new(Expr::new(span, prior)),
                    site: Site::Declaration(name.clone()),
                },
            );
            self.assign(&name, &decl, false, true, here);
            names.push(name);
        }
        self.pass = Pass::Compiling(names);
        f(self);
        self.pass = Pass::Outside;
    }

    /// A `while`'s loop, on the rows its condition held on before it.
    fn while_loop(&mut self, cond: &Expr, body: &[Stmt], next: &[Stmt], here: Here) {
        let Here { guard, id } = here;
        self.unshare_assigned(body, None, id);
        self.unshare_assigned(next, None, id);
        let before = self.syms.bindings();
        let first = self.syms.slot_count();
        let mut stay = None;
        let body_instrs = self.compile_isolated(|c| stay = c.trip(Some((cond, next)), body, id));
        let (acc_names, accumulators) = self.accumulators(&before, first, self.outer.len(), None);
        let mut computed = Vec::new();
        assigned(next, &mut computed);
        if acc_names.iter().all(|n| computed.contains(n)) {
            self.err(
                Some(id),
                "a `while` body that assigns nothing can never change its own \
                         condition, so the loop either runs zero times or forever -- \
                         assign something the condition reads",
            );
            return;
        }

        if !self.check_accumulator_types(&acc_names, &accumulators, "while", id) {
            return;
        }

        let (out, out_ty) = match accumulators.first() {
            Some(&(_, w)) => (w, self.syms.type_of(w).clone()),
            None => (self.syms.temp(DataType::Bool, Origin::Temp), DataType::Bool),
        };
        self.instrs.push(Instr {
            op: Op::Loop(Loop {
                array: None,
                accumulators,
                body: body_instrs,
                stay,
                budget: Some(crate::startup_config::loop_budget()),
            }),
            out,
            out_ty,
            guard,
            reuse: None,
            constant_ok: false,
            function: id,
        });
    }

    /// Compile an `if` or a loop statement under `here`'s guard.
    fn construct_stmt(&mut self, stmt: &Stmt, here: Here) {
        let Here { guard, id } = here;
        match stmt {
            // A row the condition fails before the loop takes no trip; after that `trip` tests it at each trip's end.
            Stmt::While { cond, body, next } => {
                let Some(cond_ir) = self.expr_ast(cond, id) else {
                    return;
                };
                let Some(cond_ir) = self.condition(cond_ir, WHILE_CONDITION, id) else {
                    return;
                };
                self.branches(guard, cond_ir, [true, false], id, |c, _, mask| {
                    c.while_loop(
                        cond,
                        body,
                        next,
                        Here {
                            guard: Some(mask),
                            ..here
                        },
                    )
                });
            }
            Stmt::If { cond, then, else_ } => self.if_stmt(cond, [then, else_], "an `if` condition", here),
            Stmt::Body(body) => self.region(Region::Body, guard, None, body, id),
            Stmt::For { var, iterable, body } => {
                let Some((array_slot, element_ty)) = self.resolve_array_iterable(iterable, here) else {
                    return;
                };

                let scope = self.syms.open_scope(var);
                let element_slot = self.syms.alloc(var.clone(), element_ty, Origin::Function(id));
                let mut leaves = Vec::new();
                bind_element(&mut self.syms, var, element_slot, &Origin::Function(id), &mut leaves);

                // A fold cannot stop part-way, which an exception inside a `try` must. Nor does it note the type it
                // writes, which a name `settle` has not typed yet needs.
                if self.tries == 0
                    && let Some(accs) = independent(body, var, &self.syms)
                    && accs
                        .iter()
                        .all(|a| self.syms.lookup(a).is_some() && self.untyped_found(a).is_none())
                    && self.array_fold(&accs, (array_slot, element_slot), &leaves, body, here)
                {
                    self.syms.close_scope(&scope);
                    return;
                }

                // Every place the body binds outside the loop variable is an accumulator. Each trip reads the
                // element's leaves from it first.
                self.unshare_assigned(body, Some(var), id);
                let before = self.syms.bindings();
                let first = self.syms.slot_count();
                let mut stay = None;
                let body_instrs = self.compile_isolated(|c| {
                    for (slot, ir) in leaves {
                        let ty = ir.ty.clone();
                        c.emit(Op::Eval { ir }, slot, ty, id);
                    }
                    stay = c.trip(None, body, id);
                });
                let (acc_names, accumulators) = self.accumulators(&before, first, self.outer.len(), Some(var));

                self.syms.close_scope(&scope);

                if !self.check_accumulator_types(&acc_names, &accumulators, "for", id) {
                    return;
                }

                let (out, out_ty) = match accumulators.first() {
                    Some(&(_, w)) => (w, self.syms.type_of(w).clone()),
                    None => (self.syms.temp(DataType::Bool, Origin::Temp), DataType::Bool),
                };
                self.instrs.push(Instr {
                    op: Op::Loop(Loop {
                        array: Some((array_slot, element_slot)),
                        accumulators,
                        body: body_instrs,
                        stay,
                        budget: None,
                    }),
                    out,
                    out_ty,
                    guard,
                    reuse: None,
                    constant_ok: false,
                    function: id,
                });
            }
            Stmt::Try { body, catches, finally } => self.try_catch(body, catches, finally, here),
            _ => unreachable!("only an `if`, a `try` or a loop is a construct"),
        }
    }

    /// Run `f` with emission redirected into a fresh instruction list: a loop body is a template the executor re-runs.
    /// Compile `body`, whose trips do not depend on one another (`independent`), to one `Op::ArrayFold` over every
    /// element of `array`: each of `accs` folds the values its write gave on the elements where it ran. False, with
    /// nothing compiled, where a value has no fold kernel: the caller compiles the general loop.
    fn array_fold(
        &mut self,
        accs: &[&str],
        (array, element_slot): (SlotId, SlotId),
        leaves: &[(SlotId, Ir)],
        body: &[Stmt],
        here: Here,
    ) -> bool {
        let Here { guard, id } = here;
        let errors = self.errors.len();
        self.folding.push(Folding {
            accs: accs.iter().map(|a| a.to_string()).collect(),
            ..Folding::default()
        });
        let body = self.compile_isolated(|c| {
            for (slot, ir) in leaves {
                c.emit(Op::Eval { ir: ir.clone() }, *slot, ir.ty.clone(), id);
            }
            c.code_block(body, Here { guard: None, ..here });
        });
        let folding = self.folding.pop().expect("pushed above");
        if folding.refused {
            self.errors.truncate(errors);
            return false;
        }
        let mut folds = Vec::new();
        let mut first = None;
        for (name, mut fold, ty) in folding.writes {
            fold.out = self.bind(&name, ty.clone(), Origin::Function(id), id);
            first.get_or_insert((fold.out, ty));
            folds.push(fold);
        }
        let (out, out_ty) = first.unwrap_or_else(|| (self.syms.temp(DataType::Bool, Origin::Temp), DataType::Bool));
        self.instrs.push(Instr {
            op: Op::ArrayFold(ArrayFold {
                array,
                element_slot,
                body,
                folds,
            }),
            out,
            out_ty,
            guard,
            reuse: None,
            constant_ok: false,
            function: id,
        });
        true
    }

    /// The write `target = value` of an accumulator of the vectorized loop being compiled: its value into a slot of its
    /// own, with the elements `here`'s guard selects, where it ran. The value takes the accumulator's type as the
    /// general loop's write does; one with no fold kernel refuses the fold.
    fn fold_write(&mut self, target: &str, value: &Expr, here: Here) {
        let Here { guard, id } = here;
        let (op, e) = fold_shape(target, value).expect("`independent` shaped every accumulator's write");
        let Some(ir) = self.expr_ast(e, id) else {
            return;
        };
        let seed = self
            .syms
            .lookup(target)
            .expect("`independent` takes only a bound accumulator");
        let seed_ty =
            Some(self.syms.type_of(seed).clone()).filter(|t| !matches!(t, DataType::Variant | DataType::Null));
        // An overwrite keeps the seed's type (rule 5): the value is coerced to it, as the general loop's write is.
        let ir = match (&seed_ty, op) {
            (Some(st), FoldOp::Last) => match keep_ir(target, e, ir, st) {
                Ok(ir) => ir,
                Err(message) => return self.err(Some(id), message),
            },
            _ => ir,
        };
        // A fold has kernels for numbers and text, and for Bool under `&&` and `||`. Anything else the checker
        // accepts, a date and a span for one, is the general loop's, which dispatches through the arithmetic kernels.
        // An operator given a variant is a compile error, which the general loop reports.
        let variant = *self.syms.type_of(seed) == DataType::Variant;
        if (variant && op != FoldOp::Last) || !fold_has_kernel(op, &seed_ty, &ir.ty) {
            self.folding.last_mut().expect("folding").refused = true;
            return;
        }
        // The accumulator's type is the assignment's, as the checker types it: the seed combined with the value for
        // `+` and `*`, Bool for `&&` and `||`, the value itself for an overwrite. `total = 0.0` over integers is F64,
        // as it is in the loop.
        let result_ty = match (op, &seed_ty) {
            (FoldOp::Add, Some(st)) => binop_result(BinOp::Add, st, &ir.ty),
            (FoldOp::Mul, Some(st)) => binop_result(BinOp::Mul, st, &ir.ty),
            (FoldOp::And | FoldOp::Or, _) => Ok(DataType::Bool),
            (FoldOp::Push, Some(DataType::Array(el))) => {
                Ok(DataType::Array(Box::new(el.unify(&ir.ty).expect("fold_has_kernel"))))
            }
            _ => Ok(ir.ty.clone()),
        };
        let result_ty = match result_ty {
            Ok(t) => t,
            Err(e) => return self.err(Some(id), e),
        };
        // A value widens to the seed or the element at any depth, as the general loop's write does.
        let widened = match (op, &result_ty) {
            (FoldOp::Push, DataType::Array(el)) => Some(&**el),
            (FoldOp::Add | FoldOp::Mul, _) => seed_ty.as_ref(),
            _ => None,
        };
        let ir = match widened {
            Some(t) => match widen_to(ir, t, e.span) {
                Ok(ir) => ir,
                Err(e) => return self.errors.push(e),
            },
            None => ir,
        };
        // The general loop refuses an accumulator that changes type across the body; a fold is that loop run at once,
        // so it refuses the same way, with the same words. A `null` meets any seed, and a variant is compatible with
        // one, as the loop's dynamic write slot is. A row that runs no trip keeps a variant seed, so the result is one.
        let ty = match (&seed_ty, &result_ty) {
            _ if variant => DataType::Variant,
            (Some(before), after) if before != after => match before.unify(after) {
                Some(t) if t == *before || matches!(after, DataType::Variant) => t,
                _ => return self.err(Some(id), accumulator_type_change(target, "for", before, &result_ty)),
            },
            _ => result_ty,
        };
        // Named for the accumulator, as the general loop's write is, so what its value notes names it.
        let value = self.syms.alloc(target.to_string(), ir.ty.clone(), Origin::Function(id));
        let value_ty = ir.ty.clone();
        self.emit_under(guard, Op::Eval { ir }, value, value_ty, id);
        // A copy: a row that leaves the region later clears its mask.
        let ran = guard.map(|g| {
            let ran = self.syms.temp(DataType::Bool, Origin::Temp);
            let ir = Ir::new(DataType::Bool, e.span, IrKind::Field(g));
            self.emit(Op::Eval { ir }, ran, DataType::Bool, id);
            ran
        });
        let fold = Fold {
            op,
            seed,
            value,
            ran,
            out: seed,
        };
        self.folding
            .last_mut()
            .expect("folding")
            .writes
            .push((target.to_string(), fold, ty));
    }

    fn compile_isolated(&mut self, f: impl FnOnce(&mut Self)) -> Vec<Instr> {
        self.outer.push(std::mem::take(&mut self.instrs));
        self.loop_firsts.push(self.syms.slot_count());
        f(self);
        self.loop_firsts.pop();
        let saved = self.outer.pop().expect("pushed above");
        std::mem::replace(&mut self.instrs, saved)
    }

    /// A loop accumulator must keep one type across the loop: a row that ran zero trips holds the old type and there is
    /// no mixed-type column. A variant or `null` slot is not checked for consistency.
    fn check_accumulator_types(
        &mut self,
        names: &[String],
        accumulators: &[(Option<SlotId>, SlotId)],
        kind: &str,
        id: FunctionId,
    ) -> bool {
        for (name, (read_slot, write_slot)) in names.iter().zip(accumulators) {
            let after = self.syms.type_of(*write_slot).clone();
            let Some(r) = read_slot else { continue };
            // A variant is compatible with any type, and a `null` meets any. A row that runs no trip keeps a variant
            // it held before, so the result is a variant.
            if *self.syms.type_of(*r) == DataType::Variant {
                self.syms.layout[*write_slot] = DataType::Variant;
                continue;
            }
            if *self.syms.type_of(*write_slot) == DataType::Variant {
                continue;
            }
            let before = self.syms.type_of(*r).clone();
            if before != after {
                self.err(Some(id), accumulator_type_change(name, kind, &before, &after));
                return false;
            }
        }
        true
    }

    /// A fresh guard slot: true where `inner` is a true Bool and `outer`, if there is one, is true; false everywhere
    /// else. So a guarded assignment keeps the prior value on a row where `inner` is null or not a Bool.
    fn and_guard(&mut self, outer: Option<SlotId>, inner: Ir, f: FunctionId) -> SlotId {
        let slot = self.syms.temp(DataType::Bool, Origin::Guard);
        self.emit(
            Op::Eval {
                ir: guard_ir(outer, inner),
            },
            slot,
            DataType::Bool,
            f,
        );
        slot
    }

    /// An `if`'s branches under `outer`, each in a region of its own that `block` compiles: arm 0 is `then`, arm 1
    /// `else`. An arm `arms` leaves out has no region. The condition is false where it is null or not a Bool, so the
    /// `else` takes that row.
    fn branches(
        &mut self,
        outer: Option<SlotId>,
        cond: Ir,
        arms: [bool; 2],
        f: FunctionId,
        mut block: impl FnMut(&mut Self, usize, SlotId),
    ) {
        let mut cond = flatten(is_true(cond));
        if arms == [true, true] {
            // Run once a row into one slot both branches read, so a row it fails on counts once. Under `outer`, so what
            // it notes on a row outside it is not the row's.
            let slot = self.syms.temp(DataType::Bool, Origin::Temp);
            let span = cond.span;
            self.emit_under(outer, Op::Eval { ir: cond }, slot, DataType::Bool, f);
            cond = Ir::new(DataType::Bool, span, IrKind::Field(slot));
        }
        for (arm, cond) in [(0, cond.clone()), (1, negate(cond))] {
            if arms[arm] {
                let mask = self.begin(Region::Block, outer, Some(cond), f);
                block(self, arm, mask);
                self.end(mask, f);
            }
        }
    }

    /// `if (cond) { then } else { else_ }` under `here`'s guard, `what` naming the condition where it is not a Bool. A
    /// condition with no branch still runs, for what it notes.
    fn if_stmt(&mut self, cond: &Expr, arms: [&[Stmt]; 2], what: &str, here: Here) {
        let Here { guard, id } = here;
        let Some(cond_ir) = self.expr_ast(cond, id) else {
            return;
        };
        let Some(cond_ir) = self.condition(cond_ir, what, id) else {
            return;
        };
        // `while (true)` is a loop that ends at a `break`.
        if what != WHILE_CONDITION || !matches!(cond_ir.kind, IrKind::Literal(Value::Bool(true))) {
            self.note_constant(&cond_ir, &condition_text(cond), id);
        }
        let present = [!arms[0].is_empty() || arms[1].is_empty(), !arms[1].is_empty()];
        self.branches(guard, cond_ir, present, id, |c, arm, mask| {
            c.code_block(
                arms[arm],
                Here {
                    guard: Some(mask),
                    ..here
                },
            )
        });
    }

    fn block(&mut self, body: &[Step], guard: Option<SlotId>) {
        for step in body {
            self.answer_probe(match step {
                Step::If { id, .. } => *id,
                Step::Function(f) => f.id,
            });
            match step {
                Step::If { id, cond, then, else_ } => {
                    let Some(cond_ir) = self.expr(cond, *id) else {
                        continue;
                    };
                    let Some(cond_ir) = self.condition(cond_ir, "an `if` condition", *id) else {
                        continue;
                    };
                    self.note_constant(&cond_ir, cond, *id);
                    let arms = [then, else_];
                    self.construct(Here { guard, id: *id }, |c| {
                        c.branches(
                            guard,
                            cond_ir.clone(),
                            [true, !else_.is_empty()],
                            *id,
                            |c, arm, mask| c.block(arms[arm], Some(mask)),
                        );
                    });
                }
                Step::Function(f) => {
                    if f.disabled {
                        continue;
                    }
                    let mut every_row = f.filter.is_none();
                    let effective = match &f.filter {
                        None => guard,
                        Some(src) => match self.expr(src, f.id).and_then(|ir| self.condition(ir, "a filter", f.id)) {
                            // A filter of `true` is the field's own placeholder: every row, as no filter is.
                            Some(ir) if matches!(ir.kind, IrKind::Literal(Value::Bool(true))) => {
                                every_row = true;
                                guard
                            }
                            Some(ir) => {
                                self.note_constant(&ir, src, f.id);
                                Some(self.and_guard(guard, ir, f.id))
                            }
                            None => guard,
                        },
                    };
                    // A filter is an `if` around the step, so a name the step first writes is declared ahead of it
                    // from the record's key, and the rows the filter skips keep the key.
                    if every_row {
                        self.function(
                            &f.kind,
                            Here {
                                guard: effective,
                                id: f.id,
                            },
                            guard,
                        );
                    } else {
                        self.construct(Here { guard, id: f.id }, |c| {
                            c.function(
                                &f.kind,
                                Here {
                                    guard: effective,
                                    id: f.id,
                                },
                                guard,
                            )
                        });
                    }
                }
            }
        }
    }

    /// Compile the step `kind` as the code it lowers to: its own statements under `here`'s guard, its filter and the
    /// guard around it, and what comes after it under `outer`, the guard around it alone.
    fn function(&mut self, kind: &FunctionKind, here: Here, outer: Option<SlotId>) {
        let Here { id, .. } = here;
        let kind = match lower::names(kind) {
            Ok(kind) => kind,
            Err(message) => return self.err(Some(id), message),
        };
        let lowered = match &kind {
            FunctionKind::Comment { .. } => return,
            // Each assignment is a one-line code block, and compiles as one.
            FunctionKind::Eval { assignments } => {
                for (index, (target, src)) in assignments.iter().enumerate() {
                    let before = self.errors.len();
                    match parse(src, Some(id)) {
                        Ok(value) => {
                            let assign = Stmt::Assign {
                                target: target.clone(),
                                value,
                                typed: false,
                            };
                            self.code(&[assign], here);
                        }
                        Err(e) => self.errors.push(e),
                    }
                    self.attribute_to_assignment(before, index);
                }
                return;
            }
            FunctionKind::Code { body } => {
                let cut = self.probe_cut(id);
                let body = cut.as_deref().unwrap_or(body);
                match parse_block_shaped(body) {
                    Ok((stmts, shape)) => {
                        for ender in shape.unreachable {
                            self.warn(id, format!("nothing after `{ender}` in this block runs"));
                        }
                        for label in shape.fall_throughs {
                            let label = label.slice(body).trim();
                            self.warn(id, format!("{label} has no `break`, so the next case runs too"));
                        }
                        self.code(&stmts, here)
                    }
                    Err(e) => self.err(Some(id), format!("parse error in code block: {}", e.message)),
                }
                return;
            }
            FunctionKind::Drop { cond } => {
                let cond = match cond.trim() {
                    "" => None,
                    src => match parse(src, Some(id)) {
                        Ok(e) => Some(e),
                        Err(e) => return self.errors.push(e),
                    },
                };
                lower::Lowered::from(lower::drop(cond))
            }
            FunctionKind::Rename { pairs } => lower::Lowered::from(lower::rename(pairs)),
            FunctionKind::Remove { fields } => lower::Lowered::from(lower::remove(fields)),
            FunctionKind::Parser {
                input,
                into,
                kind,
                declared,
            } => lower::parser(input, into, kind, declared, |n| self.held(n)),
            FunctionKind::Lookup {
                input,
                table,
                match_kind,
                multiplicity,
                key_column,
                outputs,
            } => lower::lookup(table, input, *match_kind, *multiplicity, key_column, outputs, |n| {
                self.held(n)
            }),
            FunctionKind::Unroll {
                field,
                keep_parent_fields,
            } => lower::Lowered::from(vec![Stmt::Unroll {
                field: field.clone(),
                keep: *keep_parent_fields,
                span: Span::default(),
            }]),
        };
        self.settle(|c| {
            c.code_block(&lowered.body, here);
            c.code_block(&lowered.after, Here { guard: outer, ..here });
        });
    }

    /// Compile the code `stmts` under `here`'s guard, each function it calls expanded.
    fn code(&mut self, stmts: &[Stmt], here: Here) {
        let Here { id, .. } = here;
        match crate::expr::inline::expand(stmts) {
            Ok(stmts) => self.settle(|c| c.code_block(&stmts, here)),
            Err(e) => self.err(Some(id), e.message),
        }
    }

    /// Compile a step, `f`, so each name it first writes `null` or `[]` has the type the writes after it blend to.
    /// A round that meets such a name binds it with no type and notes what its writes blend to; the step is compiled
    /// again with each first write declaring what was found. Each round can type what the one before left typeless,
    /// as a `map` over a `map`'s result. The rounds end when one finds nothing new, and a last one reports each name
    /// still typeless, or blended to a variant.
    fn settle(&mut self, f: impl Fn(&mut Self)) {
        let outer = self.untyped.take();
        let mut untyped = Untyped::default();
        for round in 1.. {
            let saved = self.clone();
            self.untyped = Some(untyped.clone());
            f(self);
            let found = std::mem::take(&mut self.untyped.as_mut().expect("set above").found);
            if found.is_empty() || untyped.last {
                break;
            }
            *self = saved;
            let changed = found.iter().any(|(at, _, _, t)| untyped.settled.get(at) != Some(t));
            untyped.settled.extend(found.into_iter().map(|(at, _, _, t)| (at, t)));
            // A round types at least one more first write, or changes nothing.
            untyped.last = !changed || round > untyped.settled.len();
        }
        self.untyped = outer;
    }

    /// Where `name`'s binding here is among the first writes `settle` has not typed yet.
    fn untyped_found(&self, name: &str) -> Option<usize> {
        let binding = self.syms.binding(name);
        let untyped = self.untyped.as_ref()?;
        untyped
            .found
            .iter()
            .rposition(|(_, n, b, _)| n == name && *b == binding)
    }

    /// What the tree holds at `name`, for a step's declarations.
    fn held(&self, name: &str) -> lower::Held {
        match self.syms.node(name) {
            Some(node) => lower::Held::Bound(node_type(&self.syms, node)),
            // `$` is the record, which the tree names `""`.
            None if name == tree::ROOT => lower::Held::Bound(node_type(&self.syms, &Node::Object(self.syms.record()))),
            None if self.record_key(name, Span::default()).is_some() => lower::Held::Key,
            None => lower::Held::Free,
        }
    }
}

/// A slot is dead after its last read. One forward scan records each slot's last reading instruction; `Instr.reuse`
/// then offers a dead slot of the same storage shape (I64/F64/DateTime/Duration share one; `Str` never backs an I64).
///
/// Ingest slots and every slot of `tree` are read after the last instruction, by the serializer, and are never
/// offered.
fn liveness(plan: &mut Plan) {
    let n = plan.instrs.len();
    let mut last_read = vec![usize::MAX; plan.slot_layout.len()];

    // A loop body's own slots live on the wave's batch, not this one. What the body reads from the outer batch is
    // gathered into the wave, so it is read here and is marked here.
    for (i, instr) in plan.instrs.iter().enumerate() {
        instr.reads(&mut |s| last_read[s] = i);
    }

    // A literal may stay one value only where the serializer is the last reader, because the serializer is the one
    // reader that takes the value instead of the rows. Two things have to hold.
    //
    // No instruction reads the slot, a loop body included: `usize::MAX` still means that here, before the forced
    // entries below. `reuse` never offers an unread slot, so it survives a reader it did not see. This does not: an
    // unread slot is the permission. Every reader of the outer batch must mark here.
    //
    // And the slot is not `__live`, the one the runtime reads on its own, outside the instruction stream and outside
    // `tree`: it becomes a bitmap walked a word at a time, through an accessor that refuses an unexpanded constant.
    // The text slots it reads, each `Placement` and `__destination.id`, take a constant as the value in every row.
    for i in 0..n {
        let out = plan.instrs[i].out;
        plan.instrs[i].constant_ok = last_read[out] == usize::MAX && out != plan.live_slot;
    }

    // `__source.bytes` is the record as read, as large as the record. A destination reads it from the record a row
    // came from, so it is held only while a step reads it: an unroll copies none of it into the rows it makes, and a
    // preview or a capture shows none on them.
    let payload = plan.tree.leaf(crate::plan::topology::SOURCE_BYTES);
    let runtime_filled = plan.ingest_slots;
    for slot in plan.tree.slots().into_iter().chain(0..runtime_filled) {
        if Some(slot) != payload {
            last_read[slot] = n;
        }
    }

    // An unroll copies each slot it keeps to every row it makes, so it releases one nothing reads after it: a local the
    // rest of the step leaves alone is not copied.
    for i in 0..n {
        let out = plan.instrs[i].out;
        if let Op::Unroll { clear, live, .. } = &mut plan.instrs[i].op {
            let live = *live;
            for (s, &lr) in last_read.iter().enumerate().take(out) {
                if (lr <= i || lr == usize::MAX) && s != live && !clear.contains(&s) {
                    clear.push(s);
                }
            }
        }
    }

    let mut available: Vec<SlotId> = Vec::new();
    for i in 0..n {
        let out_ty = plan.instrs[i].out_ty.clone();
        if let Some(pos) = available
            .iter()
            .position(|&d| plan.slot_layout[d].same_storage_shape(&out_ty) && last_read[d] < i)
        {
            let dead = available.remove(pos);
            plan.instrs[i].reuse = Some(dead);
        }
        for (s, &lr) in last_read.iter().enumerate() {
            if lr == i && !available.contains(&s) && s >= runtime_filled {
                available.push(s);
            }
        }
    }
}

/// Does any instruction read `slot`? A loop's body reads outer slots through its wave.
fn any_instr_reads(instrs: &[Instr], slot: SlotId) -> bool {
    instrs.iter().any(|i| {
        let mut found = false;
        i.reads(&mut |s| found |= s == slot);
        found
    })
}

pub(crate) fn walk_slots(ir: &Ir, f: &mut impl FnMut(SlotId)) {
    match &ir.kind {
        IrKind::Field(s) => f(*s),
        IrKind::Literal(_) | IrKind::Regex(_) => {}
        IrKind::Binary { lhs, rhs, .. } => {
            walk_slots(lhs, f);
            walk_slots(rhs, f);
        }
        IrKind::Unary { operand, .. } => walk_slots(operand, f),
        IrKind::Ternary { cond, then, else_ } => {
            walk_slots(cond, f);
            walk_slots(then, f);
            walk_slots(else_, f);
        }
        IrKind::Call { args, .. } | IrKind::ObjectOp { args, .. } => args.iter().for_each(|a| walk_slots(a, f)),
        IrKind::Index { base, index } => {
            walk_slots(base, f);
            walk_slots(index, f);
        }
        IrKind::Coerce { value, .. } | IrKind::Unclaimed(value) => walk_slots(value, f),
        IrKind::SetElement { array, index, value } => {
            walk_slots(array, f);
            walk_slots(index, f);
            walk_slots(value, f);
        }
        IrKind::Array(v) | IrKind::ConcatN(v) | IrKind::AndN(v) | IrKind::OrN(v) | IrKind::CoalesceN(v) => {
            v.iter().for_each(|x| walk_slots(x, f))
        }
        IrKind::Object { fields, unclaimed } => {
            fields.iter().for_each(|(_, x)| walk_slots(x, f));
            unclaimed.iter().for_each(|x| walk_slots(x, f));
        }
        IrKind::Merge(layers) => layers.iter().flat_map(Layer::children).for_each(|x| walk_slots(x, f)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::batch::Batch;
    use crate::core::buffer::BufferPool;
    use crate::core::ctx::Ctx;
    use crate::core::error::ErrorSink;
    use crate::plan::config::{FunctionConfig, PipelineConfig};
    use crate::plan::topology::SourceMeta;

    fn eval_pipeline(assignments: &[(&str, &str)]) -> PipelineConfig {
        PipelineConfig {
            id: 0,
            name: "p".into(),
            body: vec![Step::Function(FunctionConfig::new(
                0,
                FunctionKind::Eval {
                    assignments: assignments
                        .iter()
                        .map(|(t, e)| (t.to_string(), e.to_string()))
                        .collect(),
                },
            ))],
            step_ids: vec!["eval-1".into()],
            lookup_tables: Default::default(),
            grok_patterns: Default::default(),
        }
    }

    /// `plan` over `rows` records: each output row as JSON, and the batch after, to read a slot's column from.
    fn run_rows(plan: &Plan, rows: usize, pool: &mut BufferPool) -> (Vec<serde_json::Value>, Batch) {
        let mut errors = ErrorSink::new();
        let mut batch = Batch::new(&plan.slot_layout, rows, 0);
        let mut ctx = Ctx::new(pool, &mut errors);
        let out = crate::plan::output::run_to_json(plan, &mut batch, &mut ctx);
        (out.iter().map(|r| serde_json::from_slice(r).unwrap()).collect(), batch)
    }

    /// A literal only the serializer reads stays one value for the batch. Read by anything else -- the very next
    /// assignment will do -- and it is rows, because every accessor refuses an unexpanded constant.
    #[test]
    fn only_an_unread_literal_slot_keeps_its_constant() {
        let p = eval_pipeline(&[("kept", r#""x""#), ("read", r#""y""#), ("uses_read", r#"read + "z""#)]);
        let plan = compile(&p, &SourceMeta::KAFKA).unwrap();
        let mut pool = BufferPool::new();
        let (rows, batch) = run_rows(&plan, 2, &mut pool);
        let column = |name: &str| batch.peek(plan.tree.leaf(name).unwrap());
        assert!(
            column("kept").constant_value().is_some(),
            "only the serializer reads it"
        );
        assert!(
            column("read").constant_value().is_none(),
            "`uses_read` reads it, so it has to be rows"
        );
        assert!(
            rows.iter().all(|r| r["kept"] == "x"),
            "the value is written on every row: {rows:?}"
        );
        batch.reclaim(&mut pool);
    }

    fn code_pipeline(body: &str) -> PipelineConfig {
        let mut p = eval_pipeline(&[]);
        p.body = vec![Step::Function(FunctionConfig::new(
            0,
            FunctionKind::Code { body: body.into() },
        ))];
        p
    }

    /// A filter of `true` runs the step on every row, as no filter does: what it deletes leaves the record, and it
    /// computes no guard.
    #[test]
    fn a_filter_of_true_is_no_filter() {
        let compiled = |filter: Option<&str>| {
            let mut p = code_pipeline("Object.assign($, json(_raw)); x = when; delete(when)");
            let Step::Function(f) = &mut p.body[0] else {
                unreachable!("one step")
            };
            f.filter = filter.map(Into::into);
            let plan = compile(&p, &SourceMeta::NONE).unwrap();
            let record = crate::plan::dictionary::record_type(&plan, "t");
            (crate::plan::dictionary::fields(&record), plan.slot_layout.len())
        };
        let (fields, slots) = compiled(None);
        assert!(!fields.iter().any(|(n, _)| n == "when"), "{fields:?}");
        assert_eq!(compiled(Some("true")), (fields, slots));
        // Any other filter leaves the rows it skips as they were.
        let (filtered, _) = compiled(Some("_raw != \"\""));
        assert!(filtered.iter().any(|(n, _)| n == "when"), "{filtered:?}");
    }

    /// A Remove step takes its fields out of the record, as `delete` does; under a filter it clears them on the rows
    /// selected and the record keeps them.
    #[test]
    fn a_remove_step_takes_its_fields_out_of_the_record() {
        let compiled = |fields: &[&str], filter: Option<&str>| {
            let mut p = code_pipeline("Object.assign($, json(_raw)); x = when; y = 1; o = {a: 1, b: 2}");
            let mut remove = FunctionConfig::new(
                1,
                FunctionKind::Remove {
                    fields: fields.iter().map(|f| f.to_string()).collect(),
                },
            );
            remove.filter = filter.map(Into::into);
            p.body.push(Step::Function(remove));
            p.step_ids.push("remove-1".into());
            compile(&p, &SourceMeta::NONE).map(|plan| {
                let record = crate::plan::dictionary::record_type(&plan, "t");
                crate::plan::dictionary::fields(&record)
                    .into_iter()
                    .map(|(n, _)| n)
                    .collect::<Vec<_>>()
            })
        };
        let names = compiled(&["when", "y"], None).unwrap();
        assert!(names.contains(&"x".to_string()), "{names:?}");
        assert!(!names.iter().any(|n| n == "when" || n == "y"), "{names:?}");
        let filtered = compiled(&["when", "y"], Some("_raw != \"\"")).unwrap();
        assert!(
            filtered.iter().any(|n| n == "when") && filtered.iter().any(|n| n == "y"),
            "{filtered:?}"
        );
        assert_eq!(compiled(&["", " "], None).unwrap(), compiled(&[], None).unwrap());
        // A name twice, or under an object also named, goes with the first or the object.
        assert_eq!(compiled(&["y", "y"], None).unwrap(), compiled(&["y"], None).unwrap());
        assert_eq!(compiled(&["o", "o.a"], None).unwrap(), compiled(&["o"], None).unwrap());
    }

    fn warnings(body: &str) -> Vec<String> {
        let plan = compile(&code_pipeline(body), &SourceMeta::KAFKA).unwrap_or_else(|e| panic!("{body}: {e:?}"));
        plan.warnings.into_iter().map(|w| w.message).collect()
    }

    /// What compiles but is likely a mistake is a warning; what is read, or what the record holds at the end, is not.
    #[test]
    fn an_unread_write_is_a_warning() {
        assert_eq!(warnings("let n = 1; x = 2"), ["the local `n` is never read"]);
        assert_eq!(
            warnings("x = 1; x = 2"),
            ["`x` is written again before anything reads it"]
        );
        // Beside a parse into the record, which writes `x` again with what it finds there.
        assert_eq!(
            warnings("x = 1; x = 2; Object.assign($, json(_raw))"),
            ["`x` is written again before anything reads it"]
        );
        assert_eq!(
            warnings("x = 1; delete(x)"),
            ["`x` is deleted before anything reads it"]
        );
        // The copy of `g` the unroll makes is the local's.
        assert_eq!(
            warnings("a = [1, 2]; let g = a; unroll(a, false); g = [3];"),
            [
                "`g` is written again before anything reads it",
                "the local `g` is never read"
            ]
        );
        // A local written again is read after, but not its first value.
        assert_eq!(
            warnings("let n = 0; n = parseInt(_raw); x = n"),
            ["`n` is written again before anything reads it"]
        );
        // A copy shares its value: the name that lets it go is the one said.
        assert_eq!(
            warnings("let n = parseInt(_raw); x = n; x = 2"),
            ["`x` is written again before anything reads it"]
        );
        assert_eq!(
            warnings("a = 1; b = a; a = 2; b = 3"),
            ["`b` is written again before anything reads it"]
        );
        assert_eq!(
            warnings("a = 1; b = 5; b = a; a = 2; b = 3"),
            ["`b` is written again before anything reads it"]
        );
        assert_eq!(
            warnings("a = 1; move(a, b); b = 2"),
            ["`b` is written again before anything reads it"]
        );
        for body in [
            "let n = 1; x = n",
            "x = 1; x = x + 1",
            "x = 1; if (_raw == \"a\") { x = 2 }",
            "let n = 0; for (let v of [1, 2]) { n = n + v } x = n",
            "x = [1, 2].map(v => v * 2)",
            "_raw = \"a\"",
            "x: str; x = \"a\"",
            "x = null; x = 2",
            // Replaced with the record, not deleted.
            "x = 1; $ = {y: 2}",
            // A parse into the record writes each field it holds, its own input too, with what it finds there.
            "body = _raw; Object.assign($, json(body)); body = null",
        ] {
            assert_eq!(warnings(body), Vec::<String>::new(), "{body}");
        }
    }

    /// A parser's declared field holds null until the parse writes it, which loses no value.
    #[test]
    fn a_parsed_field_is_no_unread_write() {
        let mut p = eval_pipeline(&[]);
        let field = |name: &str| DeclaredField {
            name: name.into(),
            ty: DataType::Str,
            time_source: None,
        };
        p.body = vec![
            Step::Function(FunctionConfig::new(
                0,
                FunctionKind::Parser {
                    input: "_raw".into(),
                    into: tree::ROOT.to_string(),
                    kind: ParserKind::Json { split_dots: false },
                    declared: vec![field("srcip"), field("service")],
                },
            )),
            Step::Function(FunctionConfig::new(
                1,
                FunctionKind::Drop {
                    cond: r#"service == "NTP""#.into(),
                },
            )),
        ];
        p.step_ids = vec!["parse-1".into(), "drop-1".into()];
        let plan = compile(&p, &SourceMeta::KAFKA).unwrap();
        assert_eq!(plan.warnings, Vec::new());
    }

    /// What follows `drop()`, `return`, `break` or `continue` in its block never runs.
    #[test]
    fn code_that_never_runs_is_a_warning() {
        assert_eq!(warnings("drop(); x = 1"), ["nothing after `drop()` in this block runs"]);
        assert_eq!(
            warnings("for (let v of [1]) { continue; x = v }"),
            ["nothing after `continue` in this block runs"]
        );
        assert_eq!(warnings("if (_raw == \"a\") { drop() } x = 1"), Vec::<String>::new());
    }

    /// A condition that holds on every row, or on none, is a warning; one a row decides is not.
    #[test]
    fn a_condition_always_the_same_is_a_warning() {
        let n = "let n = parseInt(_raw);";
        assert_eq!(
            warnings(&format!("{n} if (n == n) {{ x = 1 }}")),
            ["`n == n` is always true"]
        );
        assert_eq!(
            warnings(&format!("{n} if (n < n) {{ x = 1 }}")),
            ["`n < n` is always false"]
        );
        assert_eq!(warnings("if (true) { x = 1 }"), ["`true` is always true"]);
        let fine =
            format!("{n} if (n <= n) {{ x = 1 }} let i = 0; while (true) {{ i += 1; if (i > 2) {{ break }} }} y = i");
        assert_eq!(warnings(&fine), Vec::<String>::new());
        let continued = "for (let v of [1]) { switch (v) { case 1: y = 1; continue; default: y = 2 } }";
        assert_eq!(warnings(continued), Vec::<String>::new());
        for body in [
            "let f = 0.5; if (f == f) { x = 1 }",
            "if (_raw == \"a\") { x = 1 }",
            "if (now() == now()) { x = 1 }",
        ] {
            assert_eq!(warnings(body), Vec::<String>::new(), "{body}");
        }
    }

    /// A local named as a field hides it from the code after it.
    #[test]
    fn a_local_hiding_a_field_is_a_warning() {
        assert_eq!(
            warnings("x = 1; let x = 2; y = x + 1"),
            ["the local `x` hides the field `x`"]
        );
        assert_eq!(warnings("let x = 2; y = x + 1"), Vec::<String>::new());
    }

    /// A case with statements of its own and no `break` runs the next case too; labels that share a body do not.
    #[test]
    fn a_case_running_on_is_a_warning() {
        let body = |cases: &str| format!("let c = _raw; switch (c) {{ {cases} }}");
        assert_eq!(
            warnings(&body(r#"case "a": x = 1; case "b": x = 2; break; default: x = 3;"#)),
            [r#"case "a" has no `break`, so the next case runs too"#]
        );
        // A branch that drops does not end the case while the other runs on.
        assert_eq!(
            warnings(&body(r#"case "a": if (c == "z") { drop(); } default: x = 3;"#)),
            [r#"case "a" has no `break`, so the next case runs too"#]
        );
        for cases in [
            r#"case "a": case "b": x = 2; break; default: x = 3;"#,
            r#"case "a": x = 1; break; default: x = 3;"#,
            r#"case "a": drop(); default: x = 3;"#,
            r#"case "a": if (c == "z") { drop(); } else { drop(); } default: x = 3;"#,
        ] {
            assert_eq!(warnings(&body(cases)), Vec::<String>::new(), "{cases}");
        }
    }

    /// The runtime reads each reserved slot back at its layout's type, so a pipeline that ends with one removed or
    /// at another type is refused, however it got there, for every reserved slot there is.
    #[test]
    fn a_reserved_slot_stays_at_its_type() {
        for (name, held) in reserved_names() {
            let (other, value) = match held {
                DataType::I64 => ("str", r#""x""#),
                _ => ("i64", "1"),
            };
            for body in [
                format!("{name}: {other} = {value}"),
                format!("delete({name})"),
                format!("delete({name}); {name} = {value}"),
                format!("move({name}, x)"),
                format!("x = {value}; move(x, {name})"),
            ] {
                let errors = compile(&code_pipeline(&body), &SourceMeta::KAFKA).unwrap_err();
                assert!(
                    errors
                        .iter()
                        .any(|e| e.message.contains(&format!("`{name}` is smeltr's own"))),
                    "{body}: {errors:?}"
                );
            }
            for body in [
                format!("{name}: {} = {name}", held.spell()),
                format!("let {name}: {other} = {value}"),
            ] {
                assert!(compile(&code_pipeline(&body), &SourceMeta::KAFKA).is_ok(), "{body}");
            }
        }
    }

    /// An unroll copies what it keeps to every row it makes, so a local nothing reads after it is released, not
    /// copied; one read after it stays.
    #[test]
    fn an_unroll_releases_a_local_nothing_reads_after_it() {
        let body = r#"a = [1, 2]; let big = duration(5, "s"); let g = sha512("x"); unroll(a, false); w = g;"#;
        let plan = compile(&code_pipeline(body), &SourceMeta::KAFKA).unwrap();
        let (out, clear) = plan
            .instrs
            .iter()
            .find_map(|i| match &i.op {
                Op::Unroll { clear, .. } => Some((i.out, clear)),
                _ => None,
            })
            .unwrap();
        let below = |ty: &DataType| (0..out).filter(|&s| plan.slot_layout[s] == *ty).collect::<Vec<_>>();
        assert!(!below(&DataType::Duration).is_empty());
        assert!(
            below(&DataType::Duration).iter().all(|s| clear.contains(s)),
            "{clear:?}"
        );
        let payload = plan.tree.leaf(crate::plan::topology::SOURCE_BYTES).unwrap();
        assert!(clear.contains(&payload), "{clear:?}");
    }

    /// `__source.bytes` is held for a step that reads it after an unroll, as any field is.
    #[test]
    fn an_unroll_keeps_the_payload_a_step_reads_after_it() {
        let body = r#"a = [1, 2]; unroll(a, false); n = length(__source.bytes);"#;
        let plan = compile(&code_pipeline(body), &SourceMeta::KAFKA).unwrap();
        let clear = plan
            .instrs
            .iter()
            .find_map(|i| match &i.op {
                Op::Unroll { clear, .. } => Some(clear),
                _ => None,
            })
            .unwrap();
        let payload = plan.tree.leaf(crate::plan::topology::SOURCE_BYTES).unwrap();
        assert!(!clear.contains(&payload), "{clear:?}");
    }

    /// `_raw` is the record's text, which a step may drop or replace: the runtime does not read it back.
    #[test]
    fn raw_is_not_reserved() {
        for body in ["delete(_raw)", "_raw: i64 = 1", "delete(_raw); _raw = 1"] {
            assert!(compile(&code_pipeline(body), &SourceMeta::KAFKA).is_ok(), "{body}");
        }
    }

    /// `__live` is read by the runtime itself as a bitmap, through an accessor that refuses an unexpanded constant. No
    /// instruction reads it, so a literal assigned to it must still be rows. The serializer reads `__live` as a bitmap
    /// too, so `run_rows` itself fails if `__live` kept its constant.
    #[test]
    fn the_live_slot_never_keeps_a_constant() {
        let p = eval_pipeline(&[("__live", "true"), ("kept", r#""x""#)]);
        let plan = compile(&p, &SourceMeta::KAFKA).unwrap();
        let mut pool = BufferPool::new();
        let (_, batch) = run_rows(&plan, 2, &mut pool);
        assert!(
            batch.peek(plan.live_slot).bool().is_ok(),
            "`__live` is read as a bitmap"
        );
        let kept = plan.tree.leaf("kept").unwrap();
        assert!(
            batch.peek(kept).constant_value().is_some(),
            "an ordinary output field still keeps its value"
        );
        batch.reclaim(&mut pool);
    }

    /// A source that declares its record's fields puts them in scope, typed, right after the prefix, and the
    /// `__source` fields after them. It has no `_raw`.
    #[test]
    fn a_declared_record_is_in_scope_and_raw_is_not() {
        let meta = SourceMeta::KAFKA
            .declaring(vec![("id".into(), DataType::I64), ("name".into(), DataType::Str)])
            .unwrap();
        let plan = compile(&code_pipeline("next = id + 1;"), &meta).unwrap();
        let first = crate::core::batch::PREFIX_SLOTS;
        assert_eq!(plan.slot_layout[first..first + 2], [DataType::I64, DataType::Str]);
        assert_eq!(plan.provenance[first].name, "id");
        assert_eq!(plan.provenance[meta.source_slot(0)].name, "__source.name");

        let errs = compile(&code_pipeline("t = _raw;"), &meta).unwrap_err();
        assert!(errs.iter().any(|e| e.message.contains("_raw")), "{errs:?}");
    }

    /// A type error partway through an eval step's assignments names the one assignment it happened in, not the
    /// whole step -- the editor puts the message on that row rather than making the reader guess which of several
    /// is wrong.
    #[test]
    fn a_type_error_is_attributed_to_its_own_assignment() {
        let p = eval_pipeline(&[("good", "1 + 1"), ("bad", "\"x\" == 1"), ("also_good", "2 + 2")]);
        let errs = compile(&p, &SourceMeta::NONE).unwrap_err();
        assert_eq!(errs.len(), 1, "{errs:?}");
        assert_eq!(errs[0].assignment_index, Some(1));
    }

    /// Two assignments can share a target -- `Eval` clobbers-always, so a reassignment is ordinary -- and only one
    /// of them fails. By index, not by name, or every row naming the shared target would show the one error.
    #[test]
    fn a_repeated_target_does_not_blur_which_assignment_failed() {
        let p = eval_pipeline(&[("x", "1"), ("x", "\"x\" == 1"), ("x", "2")]);
        let errs = compile(&p, &SourceMeta::NONE).unwrap_err();
        assert_eq!(errs.len(), 1, "{errs:?}");
        assert_eq!(errs[0].assignment_index, Some(1));
    }

    /// An error raised elsewhere in an eval step -- not while compiling any one assignment's expression -- names
    /// no assignment, since there is not one to name.
    #[test]
    fn a_step_level_error_names_no_assignment() {
        let mut p = eval_pipeline(&[("good", "1 + 1")]);
        let Step::Function(f) = &mut p.body[0] else {
            unreachable!()
        };
        f.filter = Some("\"x\" == 1".into());
        let errs = compile(&p, &SourceMeta::NONE).unwrap_err();
        assert_eq!(errs.len(), 1, "{errs:?}");
        assert_eq!(errs[0].assignment_index, None);
    }
}
