//! Kernel execution context: the pool, the exception channel, and the per-run state a kernel cannot be handed as a
//! parameter.

use crate::core::buffer::BufferPool;
use crate::core::error::{ErrorSink, Exception, ExceptionKind};
use crate::core::id::{FunctionId, SlotId};
use crate::core::tally::FieldTally;

/// One regex literal, compiled once at plan-compile time. Whether a replacement takes the first match or every one
/// is the function called -- `replaceFirst` or `replaceAll` -- not a flag on the pattern.
#[derive(Debug, Clone)]
pub struct CompiledPattern {
    pub regex: regex::Regex,
}

/// A kernel takes the context and its input columns and returns the column it produced, allocating its output from
/// the pool `Ctx` carries. `&mut` because a kernel producing `Str` grows its data buffer from that pool, and because
/// every kernel must be able to raise.
pub struct Ctx<'a> {
    pub pool: &'a mut BufferPool,
    pub errors: &'a mut ErrorSink,
    /// Which pipeline function is executing. Stamped onto every exception.
    pub function: FunctionId,
    /// The slot the executing instruction fills, for an exception to name the field it is about.
    pub field: Option<SlotId>,
    /// The `Code` block whose `while` loops `loop_spent` is accounting for.
    ///
    /// One `Code` block is the unit of the loop budget: a `while` is the only unbounded construct, and every loop in a
    /// block charges the same total, so a `while` nested in a `for` cannot multiply the budget by an array length the
    /// data picks. `run_loop` resets the total when it sees a different owner.
    pub loop_owner: Option<FunctionId>,
    pub loop_spent: std::time::Duration,
    /// Regex literals for whatever is executing, indexed by `PatternId`. Set by `plan::exec::run_scoped` from the plan,
    /// and by `plan::topology::route_masks` from each route's own table.
    pub patterns: &'a [CompiledPattern],
    /// `errors.len()` when this `Ctx` was made, so `Op::Unroll` can remap only this batch's exceptions when it
    /// renumbers rows — never an earlier route's, which live in an unrelated row space. Set here rather than at the
    /// start of the run so an exception the caller records for the same batch before running it (ingest's decode
    /// notes) is remapped too.
    pub pipeline_error_start: usize,
    /// Rows in the batch being evaluated, for a kernel with no column to read it from (`now()`). Set by
    /// `expr::eval::eval_call` before each call.
    pub rows: usize,
    /// What `now()` reads. The records being run build several `Ctx`s, so it is read once and passed to [`Ctx::at`] by
    /// all of them: two routes over one record must not disagree, or a yearless timestamp could infer two different
    /// years.
    pub now_micros: i64,
    /// Where each parser records the fields it produced: for a preview proposing a declaration from a sample, and for
    /// the one batch per interval a deployment describes to its leader (`streams`). `None` for every other batch: a
    /// worker parses every batch and must not pay to describe each.
    pub fields: Option<&'a mut FieldTally>,
    /// The regions being run, innermost last. `None` marks where a loop's wave begins: the regions before it are the
    /// loop's batch's, and their masks are slots of that batch.
    pub regions: Vec<Option<OpenRegion>>,
    /// Rows of this batch that leave a region beyond it, under a wave's start, and where to: the loop takes them out
    /// to its own batch.
    pub leaving: Vec<(u32, crate::expr::ast::Region)>,
    /// What each `catch` took, by the slot of its text, for a `throw` to give back.
    pub caught: Vec<(SlotId, Vec<crate::core::error::Exception>)>,
}

/// A region being run (`plan::exec`).
#[derive(Clone, Copy, Debug)]
pub struct OpenRegion {
    pub region: crate::expr::ast::Region,
    /// The slot of its mask, the rows still in it.
    pub mask: SlotId,
    /// `errors.len()` when it began: what a `try`'s exceptions are counted from.
    pub mark: usize,
}

/// Epoch microseconds now. The one place the process reads the wall clock for `now()`.
pub fn wall_clock_micros() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0)
}

impl<'a> Ctx<'a> {
    /// A `Ctx` reading the clock now. For one batch evaluated in several of them, read the instant once and use
    /// [`Ctx::at`].
    pub fn new(pool: &'a mut BufferPool, errors: &'a mut ErrorSink) -> Self {
        Ctx::at(pool, errors, wall_clock_micros())
    }

    /// A `Ctx` whose `now()` is `now_micros`.
    pub fn at(pool: &'a mut BufferPool, errors: &'a mut ErrorSink, now_micros: i64) -> Self {
        let pipeline_error_start = errors.len();
        Ctx {
            pool,
            errors,
            function: 0,
            field: None,
            loop_owner: None,
            loop_spent: std::time::Duration::ZERO,
            patterns: &[],
            pipeline_error_start,
            rows: 0,
            now_micros,
            fields: None,
            regions: Vec::new(),
            leaving: Vec::new(),
            caught: Vec::new(),
        }
    }

    /// Record what each parser's fields looked like. Only a preview asks: the proposal it builds has to be what the
    /// parser did, so it rides the parse rather than reading the records a second time.
    pub fn tallying(mut self, fields: &'a mut FieldTally) -> Self {
        self.fields = Some(fields);
        self
    }

    /// Raise an exception on `row`, about the slot the executing instruction fills.
    pub fn raise(&mut self, row: usize, kind: ExceptionKind) -> &mut Exception {
        self.errors.raise(row, self.function, self.field, kind)
    }

    /// Raise an exception on `row`, about `field` rather than the slot the executing instruction fills.
    pub fn raise_at(&mut self, row: usize, field: SlotId, kind: ExceptionKind) -> &mut Exception {
        self.errors.raise(row, self.function, Some(field), kind)
    }

    pub fn raise_range(&mut self, rows: std::ops::Range<usize>, kind: ExceptionKind) {
        self.errors.raise_range(rows, self.function, self.field, kind);
    }
}
