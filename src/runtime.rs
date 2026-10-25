//! One source's runtime: ingest (`Reader`) and transform (`Transform`), meeting at a shared `Lane` -- the source's
//! queue, its report channel and its parked batches. Egress lives in `egress`; commit is `commit::CommitTracker`,
//! driven by the reader.
//!
//! `Worker::tick` drives both halves on one thread; `worker::deploy` runs readers and a transform pool in their own
//! loops over the same lanes.
//! Both call the same `step`, so a test that ticks and a deployment that threads run the same code.

use std::borrow::Cow;
use std::collections::{HashMap, HashSet, VecDeque};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::commit::{CommitTracker, TRANSFORM};
use crate::core::batch::{Batch, SLOT_LIVE, SLOT_RAW};
use crate::core::bits;
use crate::core::buffer::{self, BufferPool, MemoryBudget};
use crate::core::column::{Column, DataType};
use crate::core::ctx::Ctx;
use crate::core::error::{ErrorSink, ExceptionKind, INGEST_FUNCTION, Read};
use crate::core::id::{DestId, SlotId, SourceId};
use crate::format::encoding::Encoding;
use crate::format::json::scalar_text;
use crate::format::record::RecordView;
use crate::handoff::{Acks, BlockCause, Handed, Handoff, Ready, Report, Reports, Taken};
use crate::heartbeat::MAX_TEXT_LEN;
use crate::io::parquet::{self, Chunk, Columns};
use crate::io::{
    BatchId, CommitMap, Destination, Dispatched, HeldRefusal, IoError, OwnedRecord, Rejection, SalvageTarget, Source,
    SourceRecord, TopicPartition,
};
use crate::metrics::MetricsHandle;
use crate::plan::config::{Answer, Payload};
use crate::plan::exec;
use crate::plan::output::{RaisedIn, answer_rows, tag_column, tag_or_not};
use crate::plan::plan::Plan;
use crate::plan::topology::{CompiledRoute, CompiledTopology, Placement, SourceMeta, route_masks};

/// The block reason for a destination that stopped taking records. The cause is quoted: a missing topic, an unreachable
/// broker and a full queue are three different fixes.
fn undeliverable(cause: &dyn std::fmt::Display) -> String {
    format!(
        "a destination is not accepting records ({cause}); the batch in hand is kept \
         and retried, and the source resumes once delivery succeeds"
    )
}

/// The block reason for a destination that examined a record and refused it, answering `Refused` with block. Names `dest`
/// and quotes `reason` as the destination worded it, rather than a line true of any destination: an operator reading
/// only this string can tell which destination and what it said. The fix comes before the destination's words, since a
/// reason is cut to `MAX_TEXT_LEN` on its way to the heartbeat and a destination's own wording can be long.
fn block_reason(dest: &str, reason: &str) -> String {
    format!(
        "destination {dest} refused a record and is set to block: fix the destination's schema or the \
         pipeline that shapes the record, and this source resumes on its own. Nothing is committed until \
         then. It said: {reason}"
    )
}

/// `dest`'s name among `targets`, or its id when this thread holds no target for it.
fn dest_name(targets: &[DestinationTarget], dest: DestId) -> String {
    targets
        .iter()
        .find(|t| t.id == dest)
        .map_or_else(|| dest.to_string(), |t| t.name.clone())
}

/// The block reason for rows whose exceptions a route answered with block. Retrying cannot clear it: the same rows
/// fail the same way until the pipeline or the answer changes. The fix comes before the detail, since a reason is cut
/// to `MAX_TEXT_LEN` on its way to the heartbeat and the first failure's text can be long.
fn failed_block_reason(blocked: &Blocked) -> String {
    match &blocked.cause {
        Cause::Marked { pipeline, first } => format!(
            "pipeline {pipeline} marked {} record(s) with a kind its route's exceptions block: fix it, or answer \
             that kind another way. The first {first}",
            blocked.rows
        ),
        Cause::Declined { route } => format!(
            "No delivery rule of route {route} took {} record(s), and Unmatched is answered with block. Nothing is \
             committed and this source is blocked until the config changes -- add a rule that takes them, drop \
             them in the pipeline, or answer Unmatched with drop or deliver",
            blocked.rows
        ),
    }
}

/// Why a batch no pipeline could process is held, when a pipeline panicked on it.
const PANICKED: &str = "its pipelines panicked";

/// What a capture says of a record that blocked its source because no route took it and `Unmatched` is answered with
/// block.
const UNMATCHED_MESSAGE: &str = "matched no route";

/// What a capture says of a record that blocked its source because no delivery rule of its route took it and
/// `Unmatched` is answered with block.
const DECLINED_MESSAGE: &str = "no delivery rule took it";

/// The block reason for rows no route matched, `Unmatched` answered with block. Retrying cannot clear it, so it names
/// the only two fixes.
fn unmatched_block_reason(rows: usize) -> String {
    format!(
        "{rows} record(s) matched no route and Unmatched is answered with block. Nothing is committed \
         and this source is blocked until the config changes -- add a route that takes them, or answer \
         Unmatched with drop or deliver"
    )
}

/// The block reason for records no pipeline could process, `Unprocessable` answered with block. Retrying cannot
/// clear it: the same records panic the same pipelines, or ask the same memory budget for too much. The fix comes
/// before the cause, which can be long, for the same reason as [`block_reason`].
fn unprocessable_block_reason(rows: usize, why: &str) -> String {
    format!(
        "{rows} record(s) could not be processed and Unprocessable is answered with block: fix the pipeline, or \
         answer Unprocessable with drop or deliver. Nothing is committed and this source is blocked until the \
         config changes. The cause: {why}"
    )
}

/// The fewest batches a reader may have outstanding for the acknowledgment round trip, on top of what the transform
/// pool could be working on for it (`Reader::max_in_flight`). A deployed reader takes more when its destinations'
/// queues hold more (`worker::reader_depth`).
///
/// A pipelining depth, not a memory bound: an outstanding batch costs the reader its offsets and nothing more, and the
/// rows are bounded by the destination's egress queue.
pub const MAX_IN_FLIGHT: usize = 4;

/// The most batches a reader may have outstanding, whatever they hold: a batch of empty records counts no bytes against
/// the budget, and each costs the tracker its own entry. Over eight minutes of half-second polls, past any file
/// interval.
pub const MAX_TRACKED: usize = 1024;

/// A destination a route's `deliver` rules (bit `id` of a row's mask) can send rows to. Carries the per-destination
/// config `place_rows` needs, because it has the target in hand and not the topology.
pub struct DestinationTarget {
    pub id: DestId,
    /// The author's name for it. Free text such as a block reason is never resolved to names on its way to the
    /// leader, so this is what the runtime writes there.
    pub name: String,
    pub destination: Box<dyn Destination>,
    /// `DestinationConfig::on_reject`.
    pub on_reject: Answer,
    /// Whether this destination keys documents on the row's `__destination.id`, so a row without one is counted
    /// (`smeltr_rows_without_doc_id_total`).
    pub wants_doc_id: bool,
    /// The row field that supplies the document identity when the pipeline set no `__destination.id`.
    pub id_field: Option<String>,
    /// The `__destination` field that places a row in it, if it reads one.
    pub placement: Option<Placement>,
}

/// The `reason` label on `smeltr_rows_dropped_total`: no route matched the row, or no delivery rule of its route took
/// it, and `Unmatched` is answered with drop (design doc, Exception taxonomy). A source loses rows three ways and only this label tells them apart.
pub const DROPPED_UNMATCHED: &str = "unmatched";
/// A destination refused the row for good, and its answer to `Refused` is drop. One a refused row was delivered to
/// counts here too, and the row it drops is counted lost as well.
pub const DROPPED_REJECTED: &str = "rejected";
/// The pipeline marked the row, and its route answered with drop.
pub const DROPPED_EXCEPTION: &str = "exception";
/// No pipeline could process the row -- a pipeline panicked on its batch, or the memory budget will not fit it
/// alone -- and `Unprocessable` is answered with drop.
pub const DROPPED_UNPROCESSABLE: &str = "unprocessable";

/// How long a reader goes without taking work before it keeps its source's membership in place of waiting (design doc,
/// Runtime stages: ingest keeps whatever membership its source protocol requires). Short spells of backpressure are
/// ordinary, and pausing throws away what the client has prefetched; ten seconds is well inside Kafka's poll interval,
/// and a group's rebalance takes seconds anyway.
pub const MEMBERSHIP_AFTER: Duration = Duration::from_secs(10);

/// Ingest and commit: polls the source, tracks each batch at poll time, hands it to transform, and commits what comes
/// back acknowledged. One per source, and the only thing that touches it -- a Kafka consumer refuses a commit from
/// anyone who does not hold the partition.
pub struct Reader {
    pub source_id: SourceId,
    /// Batches this reader may have outstanding before it stops polling, unless [`Reader::with_byte_budget`] lets it
    /// read on: every transform thread that could be working on this lane has one, and [`MAX_IN_FLIGHT`] more for the
    /// acknowledgment round trip.
    pub max_in_flight: usize,
    /// Bytes past which a reader at `max_in_flight` stops polling: its destinations' largest queue, so every queue binds
    /// first however small its batches are. 0 is none: the batch count alone binds.
    pub byte_budget: usize,
    /// Every destination reachable from this source: what a batch waits on from the moment it is polled. Kept in step
    /// with `Transform::destinations` by [`Worker::add_destination`].
    pub destinations: Vec<DestId>,
    pub metrics: MetricsHandle,
    pub source: Box<dyn Source>,
    /// At-least-once bookkeeping. Carries offsets, never rows: egress keeps whatever may have to be re-sent.
    pub commits: CommitTracker,
    /// Whether the source is blocked. The transition is what gets logged.
    pub blocked: bool,
    /// The last outstanding count written to `smeltr_source_in_flight`. Written on change only, so an idle reader
    /// writes nothing and the series is absent until the reader first has a batch out.
    reported_in_flight: usize,
    /// Why, while `blocked`: the leader cannot see a block at all without this (design doc, Control plane). `None` exactly
    /// when `blocked` is false.
    pub block_reason: Option<String>,
    /// What it is blocked on, while `blocked`, which tells the leader what clears it. `None` exactly when
    /// `block_reason` is.
    pub block_cause: Option<BlockCause>,
    /// Whether the last offset commit was refused, so the log fires on the transition rather than every turn.
    pub commit_failing: bool,
    /// When the failing source was last logged, while its polls fail. `None` once a poll lands.
    poll_failing: Option<std::time::Instant>,
    /// A tracked batch the handoff would not take. Offered again before the next poll: it is already the oldest thing
    /// the watermark waits on, so dropping it would block every commit behind it.
    pub pending: Option<Handed>,
    /// How long the reader goes without taking work before it keeps its source's membership: [`MEMBERSHIP_AFTER`].
    pub membership_after: Duration,
    /// Since when the reader has been unable to take work. `None` while it is taking it.
    full_since: Option<std::time::Instant>,
    /// Recent refusals every destination this reader hears from gave, for the heartbeat. Shared with the transform
    /// threads serving the same destinations; a fresh, private ring until [`Reader::with_refusals`] injects a
    /// deployment-wide one.
    refusals: Arc<RefusalRing>,
    /// Asked before each poll: whether the source should copy its records' headers. Never, until
    /// [`Reader::with_headers`] says otherwise.
    wants_headers: Box<dyn Fn() -> bool + Send>,
}

/// Batches parked for a re-run, ahead of anything the handoff holds: their offsets are older than anything queued.
#[derive(Default)]
struct Parked {
    /// Batches a destination would not take, oldest first. Each keeps the id it was tracked under.
    retry: VecDeque<Undelivered>,
}

/// A batch a destination would not take, kept for a re-run.
struct Undelivered {
    batch: BatchId,
    records: Vec<SourceRecord>,
    /// The full queue this batch waits on, or `None` for a delivery failure, which is re-run at once: the hold for
    /// that has already been reported, and egress owns the retry of what it accepted.
    waiting: Option<Waiting>,
    /// Kept until the config changes, and never re-run: the same rows meet the same routes and fail the same way, so a
    /// re-run is work thrown away. The lane is passed over while it holds one; a new config replaces the runtime.
    held: bool,
    /// Already the answer to `Unprocessable`'s: a pipeline panicked on it. Its re-run sends it there again without
    /// running a pipeline.
    unprocessable: bool,
    /// While `held`: the records that blocked, by index into `records`, and why. Empty otherwise.
    blocking: Vec<(usize, String)>,
    /// The capture admissions already shown `blocking`. Each is shown it once.
    shown: HashSet<u64>,
    /// While `held`: that this worker holds records for a capture of the source, until the batch is gone. Kept for its
    /// drop.
    _hold: Option<crate::worker::capture::Holding>,
}

/// A batch waiting for room on one destination's queue. The lane is skipped by the pool until the queue has room for
/// `bytes`, so no thread runs a pipeline whose rows have nowhere to go.
#[derive(Clone, Copy)]
struct Waiting {
    dest: DestId,
    bytes: usize,
    /// Since the first refusal: a re-run that is refused again keeps waiting from then, so a queue that is nearly
    /// full cannot keep a batch from ever counting as stalled.
    since: std::time::Instant,
    /// How long a wait is a stall, as the destination states it.
    stall_after: Duration,
    /// Whether the reader has been told. Once: `enter_block` is idempotent, and the release is the batch going through.
    hold_reported: bool,
}

/// What a lane had for the pool.
enum ParkedTake {
    /// A batch to run, whether it is already unprocessable, and since when it has been waiting for room, if it was.
    Batch(BatchId, Vec<SourceRecord>, bool, Option<std::time::Instant>),
    /// The oldest parked batch waits on a queue with no room, so nothing on this lane may run: it is older than
    /// everything behind it, and a batch behind it would meet the same queue.
    Waiting(Waiting),
    /// The oldest parked batch is held until the config changes, so nothing on this lane may run.
    Held,
    Nothing,
}

/// How a batch left `hand_off`.
enum HandOff {
    /// Every destination took its rows; the acknowledgments are owed.
    Delivered,
    /// A destination's queue had no room. The batch is parked on its lane and nothing was reported: a wait is not a
    /// hold until it has lasted.
    Parked,
}

/// A dispatched batch, until every destination that took it has reported: the units each still owes. A destination can
/// owe more than one -- its own rows, and rows another destination refused and salvaged to it -- and the reader hears
/// it done with the batch only once the last has come back.
struct Owed {
    lane: usize,
    outstanding: HashMap<DestId, usize>,
}

/// Rows a destination refused, on their way to its DLQs.
struct Salvage {
    lane: usize,
    batch: BatchId,
    /// The destination that refused them. Its acknowledgment of `batch` is sent once every DLQ has them.
    dest: DestId,
    rows: Vec<OwnedRecord>,
    /// The DLQs that have not taken them yet.
    left: Vec<DestId>,
}

/// One source's side of transform: the facts every batch of its records is run against, and what is parked waiting to
/// be re-run. Shared, so more than one transform thread can serve the same source; the exclusive half -- the egress
/// handles a batch's rows are handed to -- stays on [`Transform`].
pub struct Lane {
    pub source_id: SourceId,
    /// The id the author gave this source, which is what a capture names. `None` on the single-source tick driver:
    /// that is not a deployment, and a capture only ever reaches one.
    pub source_name: Option<String>,
    /// What this source's records carry; the ingest batch is built from it.
    pub meta: SourceMeta,
    /// How this source's record bytes become `_raw`'s text (`format::encoding`).
    pub encoding: Encoding,
    /// Batches this source's reader has polled and no transform thread has taken.
    pub batches: Handoff,
    /// Every destination reachable from this source: what its batches are dispatched to, and what its reader waits on.
    /// Behind a lock only because `Worker::add_destination` builds a worker in steps -- it does not change while a
    /// deployment runs.
    destinations: Mutex<Vec<DestId>>,
    /// Where a batch's outcome goes: this source's reader, and no other.
    pub reports: Reports,
    /// Woken when this lane has work, so a thread watching several of them need not poll each.
    ready: Arc<Ready>,
    parked: Mutex<Parked>,
}

impl Lane {
    /// A lane whose arrivals are announced on `ready`. Every lane one transform thread drains shares one.
    ///
    /// One argument per fact the lane holds, and every one of them is a different thing a batch of this source's
    /// records is run against; grouping them into a struct would name a thing that only exists to be unpacked here.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        source_id: SourceId,
        source_name: Option<String>,
        meta: SourceMeta,
        encoding: Encoding,
        destinations: Vec<DestId>,
        reports: Reports,
        ready: Arc<Ready>,
        capacity: usize,
    ) -> Lane {
        Lane {
            source_id,
            source_name,
            meta,
            encoding,
            batches: Handoff::watched(ready.clone(), capacity),
            destinations: Mutex::new(destinations),
            reports,
            ready,
            parked: Mutex::new(Parked::default()),
        }
    }

    /// Every destination a batch of this source's records is dispatched to.
    fn destinations(&self) -> Vec<DestId> {
        self.destinations.lock().unwrap().clone()
    }

    fn add_destination(&self, id: DestId) {
        self.destinations.lock().unwrap().push(id);
    }

    /// The next parked batch. Taken under the lock, so two threads serving this source cannot run the same batch
    /// twice. `has_room` answers for the destination an undelivered batch waits on; a batch it says no to stays, and
    /// so does everything behind it.
    fn take_parked(&self, has_room: impl Fn(DestId, usize) -> bool) -> ParkedTake {
        let mut parked = self.parked.lock().unwrap();
        let Some(front) = parked.retry.front() else {
            return ParkedTake::Nothing;
        };
        if front.held {
            return ParkedTake::Held;
        }
        if let Some(w) = front.waiting
            && !has_room(w.dest, w.bytes)
        {
            return ParkedTake::Waiting(w);
        }
        let u = parked.retry.pop_front().expect("front was just seen");
        ParkedTake::Batch(u.batch, u.records, u.unprocessable, u.waiting.map(|w| w.since))
    }

    /// Record that the reader has been told, if the oldest parked batch is still the one waiting on `dest` since
    /// `since`. A batch that went through and was parked again is a new wait, told anew.
    fn note_hold_reported(&self, dest: DestId, since: std::time::Instant) {
        let mut parked = self.parked.lock().unwrap();
        if let Some(w) = parked.retry.front_mut().and_then(|u| u.waiting.as_mut())
            && w.dest == dest
            && w.since == since
        {
            w.hold_reported = true;
        }
    }

    /// Kept and re-run, never dropped: the offsets are uncommitted, and a running worker must not move past data it
    /// has not delivered.
    fn park_retry(&self, batch: BatchId, mut records: Vec<SourceRecord>, unprocessable: bool) {
        crate::io::own_rows(&mut records);
        self.parked.lock().unwrap().retry.push_front(Undelivered {
            batch,
            records,
            waiting: None,
            held: false,
            unprocessable,
            blocking: Vec::new(),
            shown: HashSet::new(),
            _hold: None,
        });
        self.ready.signal();
    }

    /// Show the records the held batch blocked on to each capture of `name` that has not been shown them. Under the
    /// lock, so two threads serving this source cannot show a capture the same records twice.
    fn show_held(&self, captures: &crate::worker::capture::Captures, name: &str) {
        let mut parked = self.parked.lock().unwrap();
        let Some(held) = parked.retry.front_mut().filter(|u| u.held) else {
            return;
        };
        // Every turn while a capture is open: the copy and decode below only for a capture still owed them.
        if !captures.owed_held(name, &held.shown) {
            return;
        }
        let records: Vec<SourceRecord> = held.blocking.iter().map(|(i, _)| held.records[*i].clone()).collect();
        let why: Vec<String> = held.blocking.iter().map(|(_, why)| why.clone()).collect();
        let raw = decode_records(&records, &self.meta, self.encoding);
        let text: Vec<&str> = raw.text.iter().map(|t| t.as_ref()).collect();
        captures.tap_held(name, &mut held.shown, &records, &text, &why);
    }

    /// Kept until the config changes, with `blocking`, the records that blocked and why, for a capture of the source
    /// to be shown. No signal: nothing on this lane may run again under this config.
    fn park_held(
        &self,
        batch: BatchId,
        mut records: Vec<SourceRecord>,
        blocking: Vec<(usize, String)>,
        captures: Option<&Arc<crate::worker::capture::Captures>>,
    ) {
        crate::io::own_rows(&mut records);
        let hold = captures
            .zip(self.source_name.as_ref())
            .map(|(c, name)| c.holding(crate::worker::capture::HeldAt::Source(name.clone())));
        self.parked.lock().unwrap().retry.push_front(Undelivered {
            batch,
            records,
            waiting: None,
            held: true,
            unprocessable: false,
            blocking,
            shown: HashSet::new(),
            _hold: hold,
        });
    }

    /// Kept until `waiting.dest` has room. No signal: the queue wakes the pool when it frees room, and waking it now
    /// would only have it find the same full queue.
    fn park_waiting(&self, batch: BatchId, mut records: Vec<SourceRecord>, waiting: Waiting, unprocessable: bool) {
        crate::io::own_rows(&mut records);
        self.parked.lock().unwrap().retry.push_front(Undelivered {
            batch,
            records,
            waiting: Some(waiting),
            held: false,
            unprocessable,
            blocking: Vec::new(),
            shown: HashSet::new(),
            _hold: None,
        });
    }
}

/// A pool for one of `threads` threads sharing `memory`. Idle buffers stay charged where a refused thread cannot free
/// them, so together the threads' pools keep at most half the budget idle.
fn idle_capped_pool(memory: &MemoryBudget, threads: usize) -> BufferPool {
    let cap = memory.limit().map_or(usize::MAX, |limit| limit / (2 * threads.max(1)));
    BufferPool::with_budget(memory.clone()).with_idle_cap(cap)
}

/// Transform: routes a batch, runs its pipelines, serializes, and hands rows to destinations. Holds no source and no
/// offsets, which is what lets it run on a thread that never touches a consumer.
pub struct Transform {
    /// Every source this thread serves. A batch is taken from whichever has one; the pool is these threads over these
    /// lanes.
    pub lanes: Vec<Arc<Lane>>,
    /// Where the next scan starts, so one busy lane does not always win.
    next_lane: usize,
    /// Signaled by every lane above. What this thread waits on when none of them had work.
    ready: Arc<Ready>,
    /// Which lane a dispatched batch belongs to, and how many destinations still owe a completion for it. A completion
    /// names only its batch, and this thread dispatches batches from every lane it serves.
    owed: HashMap<BatchId, Owed>,
    /// Refused rows a DLQ's queue had no room for yet. The refusing destination's acknowledgment waits with them.
    salvage: Vec<Salvage>,
    pub metrics: MetricsHandle,
    pub topology: Arc<CompiledTopology>,
    /// `topology`'s records as Parquet columns.
    columnar: Columnar,
    pub destinations: Vec<DestinationTarget>,
    /// The process-wide ceiling on columnar memory. A refusal splits the range that asked for too much -- see
    /// `process_batch`.
    pub memory: MemoryBudget,
    /// Kept across attempts, so a batch reuses the buffers the last one gave back.
    pool: BufferPool,
    /// Capture sessions this worker is running, if any. `None` outside a deployment.
    captures: Option<Arc<crate::worker::capture::Captures>>,
    /// Each capture at a destination's filter, compiled against each route feeding it, by session, admission and route
    /// index. Compiled once per session rather than per batch: a parser has no place on the hot path.
    capture_filters: HashMap<(String, u64, usize), RouteFilter>,
    /// Recent refusals every destination this thread hands batches to gave, for the heartbeat. Shared with the reader
    /// threads of the same lanes; a fresh, private ring until [`Transform::with_refusals`] injects a deployment-wide
    /// one.
    refusals: Arc<RefusalRing>,
    /// What this deployment sees of its streams (design doc, Observability), shared by every thread of it. `None`
    /// outside a deployment: a preview and the tick driver report to no leader.
    streams: Option<Arc<crate::streams::Streams>>,
    /// Raw bytes of a batch run through the pipelines at once (`startup_config::slice_bytes`), so what a step writes is
    /// still in cache when the next reads it.
    pub slice_bytes: usize,
    /// Run at the end of every attempt, under the pipelines' panic guard, with the turn's rows in hand. A pipeline
    /// never panics by design, so a test reaches the unprocessable path through this. `None` in a deployment.
    pub fault: Option<fn()>,
}

/// A `Reader` and a `Transform` over one lane: the whole runtime for one source, on one thread. A deployment runs the
/// two halves in their own loops instead, over the same `Lane`.
pub struct Worker {
    pub reader: Reader,
    pub transform: Transform,
    /// The reader's end of the report channel. Transform's end is on the lane.
    pub acks: Acks,
}

impl Worker {
    /// The one lane both halves share.
    pub fn lane(&self) -> &Arc<Lane> {
        &self.transform.lanes[0]
    }
}

/// What the pool took for one turn.
struct Turn {
    lane: usize,
    batch: BatchId,
    records: Vec<SourceRecord>,
    /// Already the answer to `Unprocessable`'s, back for room or after a failed dispatch.
    unprocessable: bool,
    /// When the batch first found a queue full, if it has been waiting for room.
    waiting_since: Option<std::time::Instant>,
}

/// One range of a batch's records and the route it resumes at. Routes before `resume_from` have their rows in the
/// turn's `Output` for the whole range already.
struct Attempt {
    start: usize,
    end: usize,
    resume_from: usize,
    /// What `now()` reads, once the range has first run: a split keeps it, so every route a record takes agrees.
    now_micros: Option<i64>,
}

/// `records` cut into ranges in order, each closed by the record that takes it to `bytes`, so none is empty and a
/// record larger than `bytes` runs alone.
fn slices(records: &[SourceRecord], bytes: usize) -> Vec<(usize, usize)> {
    let mut ends = Vec::new();
    let mut held = 0usize;
    for (i, record) in records.iter().enumerate() {
        held += record.size();
        if held >= bytes {
            ends.push(i + 1);
            held = 0;
        }
    }
    if ends.last() != Some(&records.len()) {
        ends.push(records.len());
    }
    let starts = std::iter::once(0).chain(ends.iter().copied());
    starts.zip(ends.iter().copied()).collect()
}

/// What one turn of ingest did.
pub enum Polled {
    /// A batch was polled and handed to transform.
    Handed,
    /// The source had nothing ready, or is blocked.
    Idle,
    /// Too many batches are outstanding, or transform has not taken the last one. The step has already waited its
    /// timeout.
    Full,
}

/// What one turn produced, held until the batch is handed off: each destination's rows, and where the rest went. A turn
/// that ends any other way drops it and runs its batch again or holds it, so nothing in it has left or been counted.
#[derive(Default)]
struct Output {
    /// Each destination's rows, in the order the routes produced them.
    rows: HashMap<DestId, Vec<OwnedRecord>>,
    outcomes: Outcomes,
    /// For a turn that ends in a block: each record of the batch that blocked, by its index, and why. What a capture
    /// of the blocked source is shown (`Lane::park_held`).
    blocking: Vec<(usize, String)>,
}

impl Output {
    fn push(&mut self, dest: DestId, row: OwnedRecord) {
        self.rows.entry(dest).or_default().push(row);
        *self.outcomes.sent.entry(dest).or_insert(0) += 1;
    }
}

/// Where one turn's rows went, counted once the batch is handed off. What a destination did -- a hand-over that
/// failed, a row it refused -- happened, and is counted when it happens.
#[derive(Default)]
struct Outcomes {
    sent: HashMap<DestId, u64>,
    without_doc_id: HashMap<DestId, u64>,
    no_rule: u64,
    unmatched: u64,
    over_budget: u64,
    /// By reason.
    dropped: HashMap<&'static str, u64>,
}

impl Outcomes {
    fn drop_rows(&mut self, reason: &'static str, rows: u64) {
        *self.dropped.entry(reason).or_insert(0) += rows;
    }
}

/// An attempt the memory budget stopped, and where a retry picks up.
struct Refused {
    resume_from: usize,
    over: buffer::OverBudget,
}

impl Worker {
    /// A worker over `source`, delivering to `destinations`. Both halves share `metrics`, so the worker reports into
    /// one registry. The unprocessable answer is the topology's.
    pub fn new(
        source_id: SourceId,
        source: Box<dyn Source>,
        encoding: Encoding,
        topology: CompiledTopology,
        destinations: Vec<DestinationTarget>,
        memory: MemoryBudget,
        metrics: MetricsHandle,
    ) -> Worker {
        let (reports, acks) = crate::handoff::reports();
        let lane = Arc::new(Lane::new(
            source_id,
            None,
            topology.meta_for(source_id),
            encoding,
            destinations.iter().map(|d| d.id).collect(),
            reports,
            Arc::new(Ready::new()),
            crate::handoff::HANDOFF_BYTES,
        ));
        let ready = lane.ready.clone();
        Worker {
            reader: Reader::new(
                source_id,
                source,
                destinations.iter().map(|d| d.id).collect(),
                MAX_IN_FLIGHT,
                metrics.clone(),
            ),
            transform: Transform::new(vec![lane], ready, 0, Arc::new(topology), destinations, memory, metrics),
            acks,
        }
    }

    /// One turn of each half on this thread. Returns the number of records transform ran; `0` is an idle turn or a
    /// poison retry.
    ///
    /// Nothing else runs to acknowledge batches, so this forces the destinations to complete whenever the depth limit
    /// is reached.
    pub fn tick(&mut self, max_rows: usize, max_bytes: usize, timeout: Duration) -> Result<usize, IoError> {
        self.reader.step(
            &self.transform.lanes[0].batches,
            &self.acks,
            max_rows,
            max_bytes,
            timeout,
        )?;
        let stepped = self.transform.step(Duration::ZERO);
        // Even on failure: a block transform reported on its way out is the reason the source must stop.
        self.reader.apply(self.acks.drain());
        let rows = stepped?;
        self.reader.retire_and_commit()?;

        while self.reader.over_depth() {
            self.transform.flush_destinations()?;
            self.transform.collect_completions()?;
            self.reader.apply(self.acks.drain());
            self.reader.retire_and_commit()?;
            if self.reader.over_depth() {
                return Err(IoError::transient(
                    "destinations acknowledged a flush without completing their batches",
                ));
            }
        }
        Ok(rows)
    }

    /// Add a destination to both halves. A reader waiting on a destination nothing dispatches to never commits; one
    /// dispatching to a destination the reader is not waiting on commits past it.
    pub fn add_destination(&mut self, target: DestinationTarget) {
        self.reader.destinations.push(target.id);
        self.transform.lanes[0].add_destination(target.id);
        self.transform.destinations.push(target);
    }
}

impl Reader {
    /// Ingest for one source. `destinations` is everything reachable from it: a batch waits on all of them.
    pub fn new(
        source_id: SourceId,
        source: Box<dyn Source>,
        destinations: Vec<DestId>,
        max_in_flight: usize,
        metrics: MetricsHandle,
    ) -> Reader {
        // Started at 0, so a source that has read nothing yet reads idle rather than unreported.
        let source_label = source_id.to_string();
        metrics.start("smeltr_rows_polled_total", &[("source", source_label.as_str())]);
        metrics.start("smeltr_bytes_polled_total", &[("source", source_label.as_str())]);
        // Before the first message is finished too: the leader judges this source's lag in messages once it sees it.
        if source.counts_messages() {
            metrics.start(
                "smeltr_source_messages_finished_total",
                &[("source", source_label.as_str())],
            );
        }
        Reader {
            source_id,
            max_in_flight,
            byte_budget: 0,
            destinations,
            metrics,
            source,
            commits: CommitTracker::new(),
            blocked: false,
            reported_in_flight: 0,
            block_reason: None,
            block_cause: None,
            commit_failing: false,
            poll_failing: None,
            pending: None,
            membership_after: MEMBERSHIP_AFTER,
            full_since: None,
            refusals: Arc::new(RefusalRing::default()),
            wants_headers: Box::new(|| false),
        }
    }

    /// Share `refusals` with the rest of this deployment, in place of the private ring `new` gave it -- one
    /// destination's history in one place, however many readers and transform threads dispatch to it.
    pub fn with_refusals(mut self, refusals: Arc<RefusalRing>) -> Reader {
        self.refusals = refusals;
        self
    }

    /// Read on past `max_in_flight` batches while the outstanding ones hold under `bytes`.
    pub fn with_byte_budget(mut self, bytes: usize) -> Reader {
        self.byte_budget = bytes;
        self
    }

    /// Whether the outstanding batches stop the reader: past `max_in_flight` of them and past the byte budget, or past
    /// [`MAX_TRACKED`].
    pub fn over_depth(&self) -> bool {
        let depth = self.commits.depth();
        depth > MAX_TRACKED || (depth > self.max_in_flight && self.commits.bytes() >= self.byte_budget)
    }

    /// Have the source copy its records' headers while `wants` answers true: something reads them, or a capture
    /// is taking its records.
    pub fn with_headers(mut self, wants: impl Fn() -> bool + Send + 'static) -> Reader {
        self.wants_headers = Box::new(wants);
        self
    }

    /// This reader's block as a heartbeat fact, while it is blocked. `None` once it resumes: a fact that stops is a fact
    /// that clears, the same as [`Reader::enter_block`]'s gauge.
    pub fn blocked_fact(&self) -> Option<crate::heartbeat::MetricSampleDto> {
        let reason = self.block_reason.clone()?;
        let cause = self.block_cause.unwrap_or(BlockCause::Held);
        Some(crate::heartbeat::MetricSampleDto {
            metric: "smeltr_source_blocked_reason".to_string(),
            labels: vec![
                ("source".to_string(), self.source_id.to_string()),
                ("cause".to_string(), cause.name().to_string()),
            ],
            value: crate::heartbeat::MetricValueDto::Text(reason),
        })
    }

    /// This reader's source's most recent client-connectivity error as a heartbeat fact (E4), read fresh from the
    /// source every call -- unlike `block_reason`, nothing on `Reader` remembers it, since the source kind is
    /// already the thing keeping it (`Source::connection_error`). `None` once a fetch or an assignment proves the
    /// source reachable again, the same as `blocked_fact` clearing once a block ends.
    pub fn connection_error_fact(&self) -> Option<crate::heartbeat::MetricSampleDto> {
        let reason = self.source.connection_error()?;
        Some(crate::heartbeat::MetricSampleDto {
            metric: "smeltr_source_connection_error".to_string(),
            labels: vec![("source".to_string(), self.source_id.to_string())],
            value: crate::heartbeat::MetricValueDto::Text(reason),
        })
    }
}

impl Transform {
    /// One thread of the transform pool, draining `lanes`. `start_at` staggers where each thread begins its scan, so
    /// they do not all contend on the same lane.
    pub fn new(
        lanes: Vec<Arc<Lane>>,
        ready: Arc<Ready>,
        start_at: usize,
        topology: Arc<CompiledTopology>,
        destinations: Vec<DestinationTarget>,
        memory: MemoryBudget,
        metrics: MetricsHandle,
    ) -> Transform {
        Transform {
            next_lane: if lanes.is_empty() { 0 } else { start_at % lanes.len() },
            lanes,
            ready,
            owed: HashMap::new(),
            salvage: Vec::new(),
            metrics,
            columnar: Columnar::of(&topology),
            topology,
            destinations,
            pool: idle_capped_pool(&memory, 1),
            memory,
            captures: None,
            capture_filters: HashMap::new(),
            refusals: Arc::new(RefusalRing::default()),
            streams: None,
            slice_bytes: crate::startup_config::slice_bytes(),
            fault: None,
        }
    }

    /// Account what this thread's Parsers produce and what its sources receive unmatched to `streams`, which the
    /// deployment's report drains. Attached like `captures`, since only a deployment has a leader to report to.
    pub fn with_streams(mut self, streams: Arc<crate::streams::Streams>) -> Transform {
        self.streams = Some(streams);
        self
    }

    /// Watch `captures` for sessions naming any of this thread's lanes. Attached rather than taken by `new`, since
    /// only a deployment ever has one -- the tick driver has no leader to be invited by.
    pub fn with_captures(mut self, captures: Arc<crate::worker::capture::Captures>) -> Transform {
        self.captures = Some(captures);
        self
    }

    /// Share `refusals` with the rest of this deployment, in place of the private ring `new` gave it -- see
    /// [`Reader::with_refusals`].
    pub fn with_refusals(mut self, refusals: Arc<RefusalRing>) -> Transform {
        self.refusals = refusals;
        self
    }

    /// One of `threads` transform threads sharing the budget: its pool keeps idle at most its share of half of it.
    pub fn with_threads(mut self, threads: usize) -> Transform {
        self.pool = idle_capped_pool(&self.memory, threads);
        self
    }
}

impl Transform {
    /// The next batch to run and the lane it came from: a parked batch before anything queued, since its offsets are
    /// older. A lane whose parked batch waits on a full queue is passed over whole, and its reader is told once the
    /// wait has become a stall. Scans every lane once, then waits up to `pop_timeout` for any of them to have work,
    /// or for a queue to free room.
    fn take_next(&mut self, pop_timeout: Duration) -> Option<Turn> {
        // Read before the scan: a batch that arrives while it runs must not be waited past.
        let seen = self.ready.seen();
        for _ in 0..self.lanes.len() {
            let i = self.next_lane;
            self.next_lane = (self.next_lane + 1) % self.lanes.len();
            let destinations = &self.destinations;
            let has_room = |dest: DestId, bytes: usize| {
                destinations
                    .iter()
                    .find(|d| d.id == dest)
                    .is_none_or(|d| d.destination.has_room(bytes))
            };
            match self.lanes[i].take_parked(has_room) {
                ParkedTake::Batch(batch, records, unprocessable, waiting_since) => {
                    return Some(Turn {
                        lane: i,
                        batch,
                        records,
                        unprocessable,
                        waiting_since,
                    });
                }
                ParkedTake::Waiting(w) => {
                    if !w.hold_reported && w.since.elapsed() >= w.stall_after {
                        self.lanes[i].note_hold_reported(w.dest, w.since);
                        self.lanes[i].reports.send(Report::Block(
                            BlockCause::Waiting,
                            undeliverable(&format!(
                                "destination {} has had no room for {:?}",
                                dest_name(&self.destinations, w.dest),
                                w.stall_after
                            )),
                        ));
                    }
                    continue;
                }
                ParkedTake::Held => {
                    if let (Some(captures), Some(name)) = (&self.captures, &self.lanes[i].source_name)
                        && captures.watching(name)
                    {
                        self.lanes[i].show_held(captures, name);
                    }
                    continue;
                }
                ParkedTake::Nothing => {}
            }
            // No wait per lane: another may already have work, and the wait below covers all of them at once.
            if let Taken::Batch(b) = self.lanes[i].batches.pop(Duration::ZERO) {
                return Some(Turn {
                    lane: i,
                    batch: b.batch,
                    records: b.records,
                    unprocessable: false,
                    waiting_since: None,
                });
            }
        }
        self.ready.wait_past(seen, pop_timeout);
        None
    }

    /// One turn: take a batch -- an undelivered one, or the next off some lane's queue -- run it, hand the rows on,
    /// and report to the lane it came from.
    pub fn step(&mut self, pop_timeout: Duration) -> Result<usize, IoError> {
        let Some(Turn {
            lane: lane_idx,
            batch,
            records,
            unprocessable,
            waiting_since,
        }) = self.take_next(pop_timeout)
        else {
            // An idle turn is what collects completions.
            self.collect()?;
            return Ok(0);
        };
        if unprocessable {
            // Back for room or after a failed hand-over: no pipeline runs it again.
            return self.divert_unprocessable(lane_idx, batch, records, waiting_since, true);
        }
        let lane = self.lanes[lane_idx].clone();
        let reports = lane.reports.clone();
        let source_label = lane.source_id.to_string();

        let process_start = std::time::Instant::now();
        let mut out = Output::default();
        let outcome = catch_unwind(AssertUnwindSafe(|| self.process_batch(&lane, &records, &mut out)));
        self.metrics.incr_seconds(
            "smeltr_tick_process_seconds_total",
            &[("source", source_label.as_str())],
            process_start.elapsed().as_secs_f64(),
        );
        self.metrics
            .incr("smeltr_tick_batches_total", &[("source", source_label.as_str())], 1);
        if let Some(limit) = self.memory.limit() {
            self.metrics
                .gauge("smeltr_memory_peak_bytes", &[], self.memory.peak() as f64);
            self.metrics.gauge("smeltr_memory_budget_bytes", &[], limit as f64);
        }
        let row_count = records.len();
        match outcome {
            Ok(Ok(())) => {
                let t = std::time::Instant::now();
                let dests = lane.destinations();
                match self.hand_off(lane_idx, batch, records, waiting_since, false, &dests, out.rows) {
                    Err(e) => {
                        reports.send(Report::Block(BlockCause::Waiting, undeliverable(&e)));
                        return Err(e);
                    }
                    // Parked, not blocked: the reader learns of a wait only once it has become a stall.
                    Ok(HandOff::Parked) => {
                        self.metrics.incr(
                            "smeltr_batches_waiting_for_room_total",
                            &[("source", source_label.as_str())],
                            1,
                        );
                        return Ok(0);
                    }
                    Ok(HandOff::Delivered) => {}
                }
                if dests.is_empty() {
                    reports.send(Report::Acked { batch, dest: TRANSFORM });
                }
                self.count_outcomes(&source_label, out.outcomes);
                self.metrics.incr_seconds(
                    "smeltr_tick_flush_seconds_total",
                    &[("source", source_label.as_str())],
                    t.elapsed().as_secs_f64(),
                );
                reports.send(Report::Release);
            }
            // Only a new config clears it: the same rows meet the same routes and pipelines, and block the same way.
            Ok(Err(held)) => {
                lane.park_held(
                    batch,
                    records,
                    std::mem::take(&mut out.blocking),
                    self.captures.as_ref(),
                );
                reports.send(Report::Block(BlockCause::Held, held.message.clone()));
                return Err(held);
            }
            Err(_panic) => {
                // The same records meet the same pipelines, so a second attempt would panic the same way. What the
                // routes produced before the panic goes with `out`.
                self.metrics
                    .incr("smeltr_batches_poisoned_total", &[("source", source_label.as_str())], 1);
                return self.divert_unprocessable(lane_idx, batch, records, None, false);
            }
        }

        self.collect()?;
        Ok(row_count)
    }

    /// Collect completions, holding the source if a destination cannot be asked: the rows are its own, but the worker
    /// cannot learn they landed and must stop reading until it can.
    pub fn collect(&mut self) -> Result<(), IoError> {
        if let Err(e) = self.collect_completions() {
            // Every lane this thread serves: the destination it could not ask is one they all deliver through.
            for lane in &self.lanes {
                lane.reports.send(Report::Block(BlockCause::Waiting, undeliverable(&e)));
            }
            return Err(e);
        }
        Ok(())
    }

    /// Hand each of `dests` its rows of this batch without waiting for the acknowledgment: once, in one call, the turn
    /// having ended clean. A routed batch asks every destination its lane reaches, including ones it has nothing for:
    /// `track` put all of them on the batch's waiting list at poll time, and one handed nothing completes at once. An
    /// unprocessable one asks only where its answer sends it: the reader has stopped waiting on the rest.
    #[allow(clippy::too_many_arguments)]
    fn hand_off(
        &mut self,
        lane: usize,
        id: BatchId,
        records: Vec<SourceRecord>,
        waiting_since: Option<std::time::Instant>,
        unprocessable: bool,
        dests: &[DestId],
        mut rows: HashMap<DestId, Vec<OwnedRecord>>,
    ) -> Result<HandOff, IoError> {
        for &dest_id in dests {
            let Some(i) = self.destinations.iter().position(|d| d.id == dest_id) else {
                // `deploy` gives every thread a handle for every destination, so this is a worker built by hand with a
                // short list. Parked like any other failed hand-over: the batch is tracked, and dropping it here
                // would leave its reader waiting forever.
                self.lanes[lane].park_retry(id, records, unprocessable);
                return Err(IoError::transient(format!(
                    "destination {dest_id} is reachable from this source but this thread has no handle for it"
                )));
            };
            let these = rows.remove(&dest_id).unwrap_or_default();
            let count = these.len() as u64;
            match self.destinations[i].destination.deliver(id, Cow::Owned(these)) {
                // No room: keep the batch for when there is. A destination earlier in this loop may already hold it,
                // and the re-run duplicates there -- at-least-once, as specified.
                Ok((Dispatched::NoRoom { bytes, stall_after }, _)) => {
                    self.lanes[lane].park_waiting(
                        id,
                        records,
                        Waiting {
                            dest: dest_id,
                            bytes,
                            since: waiting_since.unwrap_or_else(std::time::Instant::now),
                            stall_after,
                            hold_reported: false,
                        },
                        unprocessable,
                    );
                    return Ok(HandOff::Parked);
                }
                Ok((dispatched, refused)) => self.handed(lane, id, dest_id, dispatched, refused),
                Err(e) => {
                    let dest_label = dest_id.to_string();
                    self.metrics.incr(
                        "smeltr_rows_send_failed_total",
                        &[("destination", dest_label.as_str())],
                        count,
                    );
                    // Only this batch: the ones already handed over are egress's to re-send. A destination earlier in
                    // this loop may already hold it, and the re-run duplicates there -- at-least-once, as specified.
                    self.lanes[lane].park_retry(id, records, unprocessable);
                    return Err(e);
                }
            }
        }
        Ok(HandOff::Delivered)
    }

    /// `dest` took its unit of `batch`. In flight, it is owed until its completion. Refusals it answered with at once
    /// are a unit of their own, answered now by its answer to `Refused`; a destination that finished inside `deliver`
    /// answers for all of them there, and nothing will report it later.
    fn handed(&mut self, lane: usize, batch: BatchId, dest: DestId, dispatched: Dispatched, refused: Vec<Rejection>) {
        // Recorded before anything can report it: `collect_completions` has only the batch id to go on.
        if dispatched == Dispatched::InFlight {
            self.owe(lane, batch, dest);
        } else {
            let dest_label = dest.to_string();
            self.metrics
                .incr("smeltr_flushes_total", &[("destination", dest_label.as_str())], 1);
        }
        if dispatched == Dispatched::Complete || !refused.is_empty() {
            self.owe(lane, batch, dest);
            self.answer_refusals(lane, batch, dest, refused);
        }
    }

    /// Ask every destination what it has finished with, answer for what each refused, and tell the reader.
    fn collect_completions(&mut self) -> Result<(), IoError> {
        for s in std::mem::take(&mut self.salvage) {
            if let Some(s) = self.offer_salvage(s) {
                self.salvage.push(s);
            }
        }
        for i in 0..self.destinations.len() {
            let dest = self.destinations[i].id;
            for held in self.destinations[i].destination.take_held() {
                self.note_held(dest, held);
            }
            for c in self.destinations[i].destination.poll_completions()? {
                // A completion names only its batch, so the lane comes from what this thread dispatched.
                let owed = self.owed.get(&c.batch).filter(|o| o.outstanding.contains_key(&dest));
                let Some(lane) = owed.map(|o| o.lane) else {
                    // Unreachable: nothing else takes an entry out. Dropping the report would leave the batch
                    // outstanding forever, so say so rather than lose it silently.
                    tracing::error!(
                        target: "runtime.completion_unclaimed",
                        subject = %dest,
                        batch = c.batch,
                        "a destination completed a batch this thread never dispatched, so its reader will wait for it"
                    );
                    continue;
                };
                self.answer_refusals(lane, c.batch, dest, c.rejections);
            }
        }
        Ok(())
    }

    /// `dest` owes one more unit of `batch`. Recorded before anything can report it: a completion carries only the
    /// batch id.
    fn owe(&mut self, lane: usize, batch: BatchId, dest: DestId) {
        let owed = self.owed.entry(batch).or_insert_with(|| Owed {
            lane,
            outstanding: HashMap::new(),
        });
        *owed.outstanding.entry(dest).or_insert(0) += 1;
    }

    /// One of `dest`'s units of `batch` is answered for. The last tells the reader `dest` is done with the batch.
    fn settle(&mut self, batch: BatchId, dest: DestId) {
        let Some(owed) = self.owed.get_mut(&batch) else {
            return;
        };
        let lane = owed.lane;
        let Some(left) = owed.outstanding.get_mut(&dest) else {
            return;
        };
        *left -= 1;
        if *left > 0 {
            return;
        }
        owed.outstanding.remove(&dest);
        if owed.outstanding.is_empty() {
            self.owed.remove(&batch);
        }
        self.lanes[lane].reports.send(Report::Acked { batch, dest });
    }

    /// `dest` refused rows of `batch` and keeps them to send again, since it is set to block: the source blocks until
    /// they land. Counted each time, as each is a refusal.
    fn note_held(&mut self, dest: DestId, held: HeldRefusal) {
        let Some(lane) = self.owed.get(&held.batch).map(|o| o.lane) else {
            return;
        };
        let Some(first) = held.rejections.first() else {
            return;
        };
        let why = block_reason(&dest_name(&self.destinations, dest), &first.reason);
        let rows: Vec<(usize, String)> = held.rejections.into_iter().map(|r| (r.doc, r.reason)).collect();
        report_rejections(&self.metrics, &self.refusals, self.lanes[lane].source_id, dest, &rows);
        self.lanes[lane].reports.send(Report::Block(BlockCause::Refused, why));
    }

    /// One of `dest`'s units of `batch` came back with `rejections`: its answer to `Refused` answers for them, and then
    /// the unit is settled. A DLQ answers for rows it was sent in salvage the same way, so DLQs chain.
    fn answer_refusals(&mut self, lane: usize, batch: BatchId, dest: DestId, rejections: Vec<Rejection>) {
        if rejections.is_empty() {
            self.settle(batch, dest);
            return;
        }
        let reports = self.lanes[lane].reports.clone();
        let rows: Vec<(usize, String)> = rejections.iter().map(|r| (r.doc, r.reason.clone())).collect();
        report_rejections(&self.metrics, &self.refusals, self.lanes[lane].source_id, dest, &rows);
        let own = rejections;
        let policy = self
            .destinations
            .iter()
            .find(|d| d.id == dest)
            .map_or(Answer::Block, |d| d.on_reject.clone());
        match policy {
            Answer::Drop => {
                let source_label = self.lanes[lane].source_id.to_string();
                self.metrics.incr(
                    "smeltr_rows_dropped_total",
                    &[("source", source_label.as_str()), ("reason", DROPPED_REJECTED)],
                    own.iter().map(Rejection::rows).sum(),
                );
                // A row a DLQ drops had been refused once already: it is lost, not just refused.
                let lost = own
                    .iter()
                    .filter(|r| r.record.as_ref().is_some_and(|rec| rec.salvaged))
                    .map(Rejection::rows)
                    .sum();
                self.note_lost(dest, lost);
                self.settle(batch, dest);
            }
            Answer::Deliver(dlqs) => {
                let mut rows = Vec::with_capacity(own.len());
                for r in own {
                    // Without its document -- a transport that keeps none -- the row has nothing to send.
                    match r.record {
                        // It may wait on a full DLQ long after its batch is done, so it holds none of the batch.
                        Some(mut rec) => {
                            let riders = std::mem::take(&mut rec.riders);
                            let value = rec.value.clone();
                            rows.push(OwnedRecord {
                                columns: rec.columns.as_ref().map(Columns::alone),
                                ..rec
                            });
                            // Riders go on as themselves, each with the value they shared.
                            for rider in riders {
                                rows.push(OwnedRecord {
                                    value: value.clone(),
                                    columns: rider.columns.as_ref().map(Columns::alone),
                                    ..rider
                                });
                            }
                        }
                        None => {
                            for &dlq in &dlqs {
                                self.note_lost(dlq, 1);
                            }
                        }
                    }
                }
                let s = Salvage {
                    lane,
                    batch,
                    dest,
                    rows,
                    left: dlqs,
                };
                if let Some(s) = self.offer_salvage(s) {
                    self.salvage.push(s);
                }
            }
            // A destination that holds its own refusals reports them held, not done. One that does not cannot send
            // them again, so the unit is never settled and the source stays blocked until a redeploy. Load refuses
            // ignore and tag: no destination is left to carry the row.
            Answer::Block | Answer::Ignore | Answer::Tag => {
                reports.send(Report::Block(
                    BlockCause::Refused,
                    block_reason(&dest_name(&self.destinations, dest), &own[0].reason),
                ));
            }
        }
    }

    /// Hand `s`'s rows to each DLQ still left, and settle the refusing destination's unit once every one has them. Each
    /// DLQ that takes them is reported owed first, on the same channel, so the reader never sees the batch done while a
    /// DLQ still has them. A DLQ answers for what it refuses with its own policy, so DLQs chain. Answers what is left to
    /// try, if a DLQ's queue had no room.
    fn offer_salvage(&mut self, mut s: Salvage) -> Option<Salvage> {
        let reports = self.lanes[s.lane].reports.clone();
        let mut still = Vec::new();
        for dlq in std::mem::take(&mut s.left) {
            let payload = self.topology.payload(dlq);
            // Each row as this DLQ is to be sent it; a row a rule already sent here is not sent again.
            let mut rows: Vec<OwnedRecord> = Vec::new();
            // A source record's bytes, by address, to the row that carries them: its rows share one copy.
            let mut carriers: HashMap<*const u8, usize> = HashMap::new();
            for r in &s.rows {
                let Some(out) = r.salvage_for(dlq, &payload) else {
                    continue;
                };
                let at = out.value.as_ptr();
                let mut row = OwnedRecord::from(&out);
                // A DLQ further down may write the form the row was sent in, which only its own value holds.
                if r.unfit.is_none() && !row.kept.iter().any(|(p, _)| *p == r.payload) {
                    row.kept.push((r.payload.clone(), r.value.as_slice().into()));
                }
                // A DLQ that writes source bytes gets each source record once; its other rows ride along.
                if matches!(payload, Payload::Bytes { .. }) && !row.value.is_empty() {
                    if let Some(&carrier) = carriers.get(&at) {
                        row.value = Vec::new();
                        rows[carrier].riders.push(row);
                        continue;
                    }
                    carriers.insert(at, rows.len());
                }
                rows.push(row);
            }
            if rows.is_empty() {
                continue;
            }
            let bytes = crate::io::held_bytes(&rows);
            let offered: u64 = rows.iter().map(OwnedRecord::rows).sum();
            // No handle for a DLQ is a worker built by hand with a short list; a deployed one holds every destination
            // its source reaches, DLQs included.
            let Some(slot) = self.destinations.iter().position(|d| d.id == dlq) else {
                self.note_lost(dlq, offered);
                continue;
            };
            let handle = &mut self.destinations[slot].destination;
            if !handle.has_room(bytes) {
                still.push(dlq);
                continue;
            }
            match handle.deliver(s.batch, Cow::Owned(rows)) {
                Ok((dispatched @ (Dispatched::InFlight | Dispatched::Complete), refused)) => {
                    reports.send(Report::Owes {
                        batch: s.batch,
                        dest: dlq,
                    });
                    let refused_rows: u64 = refused.iter().map(Rejection::rows).sum();
                    self.note_salvaged(dlq, offered - refused_rows);
                    self.handed(s.lane, s.batch, dlq, dispatched, refused);
                }
                Ok((Dispatched::NoRoom { .. }, _)) | Err(_) => still.push(dlq),
            }
        }
        if still.is_empty() {
            self.settle(s.batch, s.dest);
            return None;
        }
        s.left = still;
        Some(s)
    }

    /// Route `records`, run every matched pipeline, and put each route's rows in `out`. The records run in slices of
    /// about `slice_bytes` each, one after another, so a step's columns stay in cache for the next. An `IoError` here is a block
    /// only a new config clears -- an exception answered with block -- and names it. Nothing is handed over.
    ///
    /// A memory refusal is resolved here rather than reported: the range that refused is halved and re-run, down to a
    /// single record, and only a record that will not fit alone leaves the pipeline. Backing off would retry the same
    /// allocation forever; diverting the batch would punish a whole poll for one oversized record.
    fn process_batch(&mut self, lane: &Lane, records: &[SourceRecord], out: &mut Output) -> Result<(), IoError> {
        let id_fields: Vec<(DestId, String)> = self
            .destinations
            .iter()
            .filter_map(|d| d.id_field.clone().map(|f| (d.id, f)))
            .collect();

        // Each split strictly shrinks its range, so this reaches single records at worst. Later ranges pushed first, so
        // delivery follows source order.
        let mut pending: Vec<Attempt> = slices(records, self.slice_bytes)
            .into_iter()
            .rev()
            .map(|(start, end)| Attempt {
                start,
                end,
                resume_from: 0,
                now_micros: None,
            })
            .collect();
        let mut diverted = 0usize;
        let mut refusal = None;
        while let Some(mut attempt) = pending.pop() {
            attempt
                .now_micros
                .get_or_insert_with(crate::core::ctx::wall_clock_micros);
            let Refused { resume_from, over } = match self.run_attempt(lane, records, &attempt, &id_fields, out)? {
                None => continue,
                Some(refused) => refused,
            };
            if attempt.end - attempt.start <= 1 {
                self.divert_over_budget(lane, attempt.start, &records[attempt.start..attempt.end], &over, out)?;
                diverted += records[attempt.start..attempt.end].len();
                refusal.get_or_insert(over);
                continue;
            }
            self.metrics.incr(
                "smeltr_memory_splits_total",
                &[("source", lane.source_id.to_string().as_str())],
                1,
            );
            let mid = attempt.start + (attempt.end - attempt.start) / 2;
            pending.push(Attempt {
                start: mid,
                end: attempt.end,
                resume_from,
                now_micros: attempt.now_micros,
            });
            pending.push(Attempt {
                start: attempt.start,
                end: mid,
                resume_from,
                now_micros: attempt.now_micros,
            });
        }
        if let Some(over) = refusal {
            tracing::error!(
                target: "runtime.records_over_budget",
                subject = %lane.source_id,
                records = diverted,
                over = %over,
                "records do not fit the memory budget on their own, so they are diverted rather than retried forever"
            );
        }
        if let Some(fault) = self.fault {
            fault();
        }
        Ok(())
    }

    /// One range of records through routing and every route it still owes, or the refusal that stopped it.
    ///
    /// A route allocates everything inside `run_and_serialize` and puts nothing in `out` until that returns, so a
    /// refusal there has produced nothing for that route. Routes before it have their rows in `out` for the whole
    /// range; `resume_from` keeps the retry from producing them twice.
    fn run_attempt(
        &mut self,
        lane: &Lane,
        records: &[SourceRecord],
        attempt: &Attempt,
        id_fields: &[(DestId, String)],
        out: &mut Output,
    ) -> Result<Option<Refused>, IoError> {
        let held_idle = self.pool.holds_idle();
        let empty = self.pool.empty_like();
        let mut pool = std::mem::replace(&mut self.pool, empty);
        match self.run_attempt_in(&mut pool, lane, records, attempt, id_fields, out) {
            // A refusal drops the pool instead: what it holds is charged, and the retry needs the room. When idle
            // buffers took part, the same range runs once more without them, as `BufferPool::fresh` asks again.
            Ok(Some(refused)) if held_idle => {
                drop(pool);
                let again = Attempt {
                    resume_from: refused.resume_from,
                    ..*attempt
                };
                self.run_attempt(lane, records, &again, id_fields, out)
            }
            Ok(Some(refused)) => Ok(Some(refused)),
            result => {
                pool.trim();
                self.pool = pool;
                result
            }
        }
    }

    /// `run_attempt`, allocating from `pool`.
    fn run_attempt_in(
        &mut self,
        pool: &mut BufferPool,
        lane: &Lane,
        records: &[SourceRecord],
        attempt: &Attempt,
        id_fields: &[(DestId, String)],
        out: &mut Output,
    ) -> Result<Option<Refused>, IoError> {
        let slice = &records[attempt.start..attempt.end];
        let mut errors = ErrorSink::new();
        let began = std::time::Instant::now();

        let raw = decode_records(slice, &lane.meta, lane.encoding);

        // A capture sees what this batch is about to be run over: after decode, before any pipeline, so what it
        // keeps is the record as it arrived. A worker nobody is debugging answers `watching` from one atomic load
        // and takes no lock. A full session drops rather than blocking, so watching a flow can never slow it.
        if let (Some(captures), Some(name)) = (&self.captures, &lane.source_name)
            && captures.watching(name)
        {
            let text: Vec<&str> = raw.text.iter().map(|t| t.as_ref()).collect();
            captures.tap(name, slice, &text);
        }

        // Routing, every route's pipeline and every delivery rule read the same `now()`, the one a split carries over.
        let now_micros = attempt.now_micros.unwrap_or_else(crate::core::ctx::wall_clock_micros);
        let routed = buffer::catch_over_budget(|| {
            let mut ingest = seed_ingest_batch(pool, slice, &raw, &lane.meta, lane.source_name.as_deref());
            let masks = {
                let mut ctx = Ctx::at(pool, &mut errors, now_micros);
                route_masks(
                    &self.topology.routes,
                    lane.source_id,
                    &mut ingest,
                    &mut ctx,
                    Some(&self.metrics),
                )
            };
            ingest.reclaim(pool);
            masks
        });
        let masks = match routed {
            Ok(masks) => masks,
            Err(over) => {
                return Ok(Some(self.abandon(lane, began, attempt.resume_from, over)));
            }
        };

        // Which rows a route took: the union of every mask, since routing leaves `SLOT_LIVE` as it came. Over every
        // route, including the ones a resumed attempt skips -- those matched on the attempt that was refused.
        let mut taken = vec![0u64; bits::words_for(slice.len())];
        for (_, mask) in &masks {
            bits::or_assign(
                &mut taken,
                mask.bool().expect("a route mask is Bool").values,
                slice.len(),
            );
        }

        // Before any pipeline runs: the retry meets the same rows and the same routes, so running them now is work
        // the block throws away on every attempt until a redeploy.
        if *self.topology.on_exception.answer(&ExceptionKind::Unmatched) == Answer::Block {
            let unmatched = slice.len() - bits::popcount(&taken);
            if unmatched > 0 {
                self.key_unmatched(lane, slice, &taken);
                for (_, mask) in masks {
                    mask.reclaim(pool);
                }
                out.blocking = (0..slice.len())
                    .filter(|&i| !bits::get_row(&taken, i))
                    .map(|i| (attempt.start + i, UNMATCHED_MESSAGE.to_string()))
                    .collect();
                let msg = unmatched_block_reason(unmatched);
                return Err(IoError::transient(msg));
            }
        }

        // Every mask reaches `reclaim`, including the ones this attempt skips and the ones a refusal leaves behind:
        // `Column`'s `Drop` asserts against a column dropped still holding buffers.
        let mut masks = masks.into_iter();
        // Each record and source bytes destination it was written to, over every route of the batch: a record's bytes go once.
        let mut delivered: HashSet<(u32, DestId)> = HashSet::new();
        let fields = self.topology.text_fields();
        let at_destinations = self.captures.as_ref().map_or_else(Vec::new, |c| c.at_destinations());
        self.capture_filters.retain(|(session, admission, _), _| {
            at_destinations
                .iter()
                .any(|at| at.session == *session && at.admission == *admission)
        });
        while let Some((route_idx, mask)) = masks.next() {
            if route_idx < attempt.resume_from {
                mask.reclaim(pool);
                continue;
            }
            let pipeline_id = self.topology.routes[route_idx].pipeline;
            let plan = self
                .topology
                .plan_for(lane.source_id, pipeline_id)
                .expect("compile_topology validated every route's pipeline id");
            let taps = Self::route_taps(
                &mut self.capture_filters,
                self.captures.as_deref(),
                &self.destinations,
                &at_destinations,
                &self.topology,
                route_idx,
            );
            // The one batch this interval describes for the leader (`streams::Streams::due`): its Parsers tally
            // what they produce, which every other batch skips. A batch the route took no row of is not it.
            let took_rows = bits::any_set(mask.bool().expect("a route mask is Bool").values, slice.len());
            let observed = match (&self.streams, &lane.source_name) {
                (Some(streams), Some(name)) if took_rows && streams.due(route_idx) => Some((streams, name)),
                _ => None,
            };
            let mut tally = observed.map(|_| crate::core::tally::FieldTally::new());
            let docs = buffer::catch_over_budget(|| {
                run_and_serialize(
                    plan,
                    Ingest {
                        records: slice,
                        raw: &raw,
                        meta: &lane.meta,
                        source: lane.source_name.as_deref(),
                        now_micros,
                        tally: tally.as_mut(),
                    },
                    mask,
                    pool,
                    &mut errors,
                    &self.metrics,
                    Egress {
                        route: &self.topology.routes[route_idx],
                        id_fields,
                        dlq_of: &self.topology.dlq_of,
                        columns: Some(self.columnar.route(Some(route_idx as u32), &self.topology, &fields)),
                        taps: &taps,
                    },
                )
            });
            let mut docs = match docs {
                Ok(docs) => docs,
                Err(over) => {
                    // The range is run again, split; one of those runs tallies in its place.
                    if let Some((streams, _)) = observed {
                        streams.not_run(route_idx);
                    }
                    for (_, unrun) in masks {
                        unrun.reclaim(pool);
                    }
                    return Ok(Some(self.abandon(lane, began, route_idx, over)));
                }
            };
            if let (Some((streams, name)), Some(tally)) = (observed, &tally) {
                streams.saw_parse(name, &self.topology.routes[route_idx].id, plan, tally);
            }
            if let Some(blocked) = &mut docs.blocking {
                for (_, unrun) in masks {
                    unrun.reclaim(pool);
                }
                out.blocking = std::mem::take(&mut blocked.records)
                    .into_iter()
                    .map(|(i, why)| (attempt.start + i, why))
                    .collect();
                let msg = failed_block_reason(blocked);
                return Err(IoError::transient(msg));
            }
            let (dropped, no_rule, no_rule_dropped) = (docs.dropped, docs.no_rule, docs.no_rule_dropped);
            if let Some(captures) = &self.captures {
                for (tap, rows) in taps.iter().zip(std::mem::take(&mut docs.captured)) {
                    captures.offer_sent(&at_destinations[tap.at], rows);
                }
            }
            self.place_rows(out, docs, route_idx, &lane.meta, slice, &mut delivered);
            out.outcomes.drop_rows(DROPPED_EXCEPTION, dropped);
            out.outcomes.no_rule += no_rule;
            out.outcomes.drop_rows(DROPPED_UNMATCHED, no_rule_dropped);
        }
        // Past the loop, so a refusal above returns first and the retry that resumes it places these instead: once.
        self.place_unmatched(lane, slice, &raw.text, &taken, out);
        Ok(None)
    }

    /// What route `route_idx` copies for each capture in `at`: those at a destination of this thread's the route
    /// delivers to, each with its filter compiled against the route's output. A filter runs on each route delivering
    /// there that it compiles on, since those routes may give the destination rows of different shapes; one that
    /// compiles on none of them ends its capture, saying why for each. `filters` keeps what each route makes of it.
    fn route_taps(
        filters: &mut HashMap<(String, u64, usize), RouteFilter>,
        captures: Option<&crate::worker::capture::Captures>,
        destinations: &[DestinationTarget],
        at: &[crate::worker::capture::AtDestination],
        topology: &CompiledTopology,
        route_idx: usize,
    ) -> Vec<RouteTap> {
        if at.is_empty() {
            return Vec::new();
        }
        let mut taps = Vec::new();
        for (i, at) in at.iter().enumerate() {
            let Some(target) = destinations.iter().find(|t| t.name == at.destination) else {
                continue;
            };
            if !topology.routes[route_idx].delivers_to(target.id) {
                continue;
            }
            let key = |idx: usize| (at.session.clone(), at.admission, idx);
            if !filters.contains_key(&key(route_idx)) {
                let (mut errors, mut compiles) = (Vec::new(), false);
                let compiled: Vec<_> = match &at.filter {
                    None => topology.delivering(target.id).map(|(idx, _)| (idx, Ok(None))).collect(),
                    Some(src) => (topology.capture_filters(target.id, src).into_iter())
                        .map(|(idx, c)| {
                            (
                                idx,
                                c.map(|(ir, patterns)| Some(Arc::new(OutputFilter { ir, patterns }))),
                            )
                        })
                        .collect(),
                };
                for (idx, compiled) in compiled {
                    let filter = compiled.map_or_else(
                        |e| {
                            errors.push(format!("on route {:?}: {e}", topology.routes[idx].id));
                            RouteFilter::Skips
                        },
                        RouteFilter::Keeps,
                    );
                    compiles |= matches!(filter, RouteFilter::Keeps(_));
                    filters.insert(key(idx), filter);
                }
                if let (false, Some(captures)) = (compiles, captures) {
                    captures.fail_at(at, errors.join("; "));
                }
            }
            let RouteFilter::Keeps(filter) = &filters[&key(route_idx)] else {
                continue;
            };
            taps.push(RouteTap {
                at: i,
                dest: target.id,
                placement: target.placement,
                filter: filter.clone(),
                wanted: at.wanted,
                room: at.room,
            });
        }
        taps
    }

    /// The stream each row of `slice` that `taken` leaves out was filed under, to the leader. On every batch, dropped,
    /// delivered or blocked: a stream nothing routes is what these rows are, and the leader learns of it from here or
    /// not at all. A blocked batch is held until a new config, so its rows are keyed once.
    fn key_unmatched(&self, lane: &Lane, slice: &[SourceRecord], taken: &[u64]) {
        if let (Some(streams), Some(name)) = (&self.streams, &lane.source_name) {
            let keys = slice
                .iter()
                .enumerate()
                .filter(|(i, _)| !bits::get_row(taken, *i))
                .map(|(_, r)| lane.meta.stream_key(r));
            streams.saw_unmatched(name, keys);
        }
    }

    /// The rows of `slice` that `taken` leaves out, answered as `Unmatched` is: put in `out` for every destination it
    /// delivers to, or dropped (design doc, Exception taxonomy). Counted either way, because a source silently matching
    /// nothing is what this exists to make visible. Block never gets here: `run_attempt` stops the batch before any
    /// pipeline runs. `text` is the batch's decoded text, for a `record` payload.
    fn place_unmatched(
        &self,
        lane: &Lane,
        slice: &[SourceRecord],
        text: &[Cow<'_, str>],
        taken: &[u64],
        out: &mut Output,
    ) {
        let unmatched = (slice.len() - bits::popcount(taken)) as u64;
        if unmatched == 0 {
            return;
        }
        out.outcomes.unmatched += unmatched;
        self.key_unmatched(lane, slice, taken);
        let targets = self.topology.on_exception.answer(&ExceptionKind::Unmatched).targets();
        if targets.is_empty() {
            out.outcomes.drop_rows(DROPPED_UNMATCHED, unmatched);
            return;
        }
        let left: Vec<_> = slice
            .iter()
            .enumerate()
            .filter(|(i, _)| !bits::get_row(taken, *i))
            .collect();
        self.place_delivered(out, targets, &left, lane, Some(text), DROPPED_UNMATCHED);
    }

    /// Put every row in `docs`, which route `route` made, in `out`, once for each destination its set names, in that
    /// destination's form, and keeping the forms of the DLQs down its chain. `slice` is what the rows were made from: a
    /// destination that writes their bytes is sent each record's once, which `delivered` tracks across routes.
    fn place_rows(
        &self,
        out: &mut Output,
        docs: Serialized,
        route: usize,
        meta: &SourceMeta,
        slice: &[SourceRecord],
        delivered: &mut HashSet<(u32, DestId)>,
    ) {
        // Destination id -> position in `destinations`, so the per-row loop visits only the destinations a row goes
        // to. `id == index` is not assumed: the caller may hold any subset, in any order. Ids are allocated densely at
        // load, so a vector indexed by id is small.
        let width = self.destinations.iter().map(|t| t.id as usize + 1).max().unwrap_or(0);
        let mut slot_of: Vec<Option<usize>> = vec![None; width];
        for (idx, t) in self.destinations.iter().enumerate() {
            slot_of[t.id as usize] = Some(idx);
        }
        // What each destination writes, and the forms a row keeps for the DLQs down its chain, by position in
        // `destinations`.
        let payload_at: Vec<Payload> = self.destinations.iter().map(|t| self.topology.payload(t.id)).collect();
        let kept_at: Vec<Vec<Payload>> = self.destinations.iter().map(|t| self.kept(t.id)).collect();
        // Each record's bytes once, shared by every row and destination that keeps them: a record unrolled into many
        // rows would otherwise copy its whole source once for each.
        let mut source_bytes_of: HashMap<(usize, &str), Option<Arc<[u8]>>> = HashMap::new();
        let fields = &self.topology.text_fields();
        fn text_of<'t>(fields: &[String], texts: &'t [Option<String>], field: &str) -> Option<&'t str> {
            let at = fields.iter().position(|f| f == field)?;
            texts.get(at)?.as_deref()
        }

        let Serialized { rows, sets, .. } = docs;
        for mut row in rows {
            let dests = &sets[row.set as usize];
            debug_assert!(
                !dests.is_empty(),
                "a row no rule took met Unmatched before it was placed"
            );
            for (i, &dest) in dests.iter().enumerate() {
                // A row naming a destination this worker does not hold goes nowhere, and is not counted as Unmatched either.
                let Some(slot) = slot_of.get(dest as usize).copied().flatten() else {
                    continue;
                };
                let target = &self.destinations[slot];
                let origin = row.origin as usize;
                let payload = &payload_at[slot];
                if matches!(payload, Payload::Bytes { .. }) && !delivered.insert((row.origin, target.id)) {
                    continue;
                }
                // The pipeline's `__destination.id` wins over the destination's configured field, as its placement wins over
                // its default.
                let doc_id = row.doc_id.as_deref().or_else(|| {
                    row.doc_id_by_dest
                        .iter()
                        .find(|(d, _)| *d == target.id)
                        .map(|(_, v)| v.as_str())
                });
                // Delivered without the dedupe it was configured for, and counted so that is not silent.
                if target.wants_doc_id && doc_id.is_none() {
                    *out.outcomes.without_doc_id.entry(target.id).or_insert(0) += 1;
                }
                // Where a refusal of this row goes, worked out while the row's identities are still here: a refusal
                // comes back with the acknowledgment, after the columns are gone. Every DLQ down the chain, since a
                // DLQ that refuses sends it on to its own; one a rule already sent the row to has it, and is left out.
                let chain = self.topology.dlq_of.get(&target.id);
                let salvage_to: Option<Vec<SalvageTarget>> = chain.map(|chain| {
                    chain
                        .iter()
                        .map(|&dlq| SalvageTarget {
                            dlq,
                            doc_id: row.doc_id.clone().or_else(|| {
                                row.doc_id_by_dest
                                    .iter()
                                    .find(|(d, _)| *d == dlq)
                                    .map(|(_, v)| v.clone())
                            }),
                            reached: dests.contains(&dlq),
                        })
                        .collect()
                });
                let mut field_bytes = |field| {
                    source_bytes_of
                        .entry((origin, field))
                        .or_insert_with(|| meta.field_bytes(&slice[origin], field).map(Into::into))
                        .clone()
                };
                let kept: Vec<(Payload, Arc<[u8]>)> = kept_at[slot]
                    .iter()
                    .filter_map(|p| {
                        let form = match p {
                            Payload::Record => row.bytes.as_slice().into(),
                            Payload::Text { field } => text_of(fields, &row.texts, field)?.as_bytes().into(),
                            Payload::Bytes { field } => field_bytes(&**field)?,
                        };
                        Some((p.clone(), form))
                    })
                    .collect();
                let (value, unfit) = match payload {
                    // The last destination takes the document; only a row bound for several is copied.
                    Payload::Record if i + 1 == dests.len() => (std::mem::take(&mut row.bytes), None),
                    Payload::Record => (row.bytes.clone(), None),
                    Payload::Text { field } => match text_of(fields, &row.texts, field) {
                        Some(text) => (text.as_bytes().to_vec(), None),
                        None => (Vec::new(), Some(crate::io::no_form(payload))),
                    },
                    Payload::Bytes { field } => match field_bytes(&**field) {
                        Some(bytes) => (bytes.to_vec(), None),
                        None => (Vec::new(), Some(crate::io::no_form(payload))),
                    },
                };
                out.push(
                    target.id,
                    OwnedRecord {
                        value,
                        payload: payload.clone(),
                        unfit,
                        placement: target.placement.and_then(|p| row.placements[p as usize].clone()),
                        doc_id: doc_id.map(str::to_string),
                        key: None,
                        salvaged: false,
                        salvage_to,
                        kept,
                        route: Some(route as u32),
                        columns: row.columns.clone(),
                        riders: Vec::new(),
                    },
                );
            }
        }
    }

    /// Record what an abandoned attempt cost: the difference between "short of memory, coping" and "spending the tick
    /// redoing itself".
    fn abandon(&self, lane: &Lane, began: std::time::Instant, resume_from: usize, over: buffer::OverBudget) -> Refused {
        self.metrics.incr_seconds(
            "smeltr_memory_abandoned_seconds_total",
            &[("source", lane.source_id.to_string().as_str())],
            began.elapsed().as_secs_f64(),
        );
        Refused { resume_from, over }
    }

    /// One record the budget refuses on its own. Nothing is left to split, so it leaves the pipeline alone, counted
    /// under its own name because it is the signal that data has left, and is answered as `Unprocessable` is.
    /// Delivered, its rows go in `out` with the rest of the turn's, so they leave with this batch and
    /// its commit waits for them like any other row's. Blocked, the whole batch is held until the config changes.
    /// `start` is where `records` begin in the batch.
    fn divert_over_budget(
        &mut self,
        lane: &Lane,
        start: usize,
        records: &[SourceRecord],
        over: &buffer::OverBudget,
        out: &mut Output,
    ) -> Result<(), IoError> {
        match self.topology.on_exception.answer(&ExceptionKind::Unprocessable) {
            Answer::Deliver(targets) => {
                out.outcomes.over_budget += records.len() as u64;
                let records: Vec<_> = records.iter().enumerate().collect();
                self.place_delivered(out, targets, &records, lane, None, DROPPED_UNPROCESSABLE);
                Ok(())
            }
            Answer::Drop => {
                out.outcomes.over_budget += records.len() as u64;
                out.outcomes.drop_rows(DROPPED_UNPROCESSABLE, records.len() as u64);
                Ok(())
            }
            // Load refuses ignore and tag: no record is left to carry on.
            Answer::Block | Answer::Ignore | Answer::Tag => {
                let why = format!("over the memory budget: {over}");
                out.blocking = (start..start + records.len()).map(|i| (i, why.clone())).collect();
                Err(IoError::transient(unprocessable_block_reason(records.len(), &why)))
            }
        }
    }

    /// The barrier: wait for every destination to acknowledge everything outstanding. Bounded by each destination's
    /// stall threshold, so a destination that has stopped answering fails the caller rather than holding it.
    pub fn flush_destinations(&mut self) -> Result<(), IoError> {
        for d in &mut self.destinations {
            d.destination.flush()?;
            let label = d.id.to_string();
            self.metrics
                .incr("smeltr_flushes_total", &[("destination", label.as_str())], 1);
        }
        Ok(())
    }

    /// A batch a pipeline panicked on, answered as `Unprocessable` is: held, dropped, or sent where it names, each record
    /// as it arrived. Delivered, it is handed over like a routed
    /// batch -- parked for room, re-run on a failed hand-over -- to every destination it names, and its commit waits
    /// for each, and for every DLQ one of them sends a refusal to. `again` is a batch parked here before: the reader
    /// already waits on these alone.
    fn divert_unprocessable(
        &mut self,
        lane_idx: usize,
        batch: BatchId,
        records: Vec<SourceRecord>,
        waiting_since: Option<std::time::Instant>,
        again: bool,
    ) -> Result<usize, IoError> {
        let lane = self.lanes[lane_idx].clone();
        let source_label = lane.source_id.to_string();
        let rows = records.len();
        match self.topology.on_exception.answer(&ExceptionKind::Unprocessable).clone() {
            // Load refuses ignore and tag: no record is left to carry on.
            Answer::Block | Answer::Ignore | Answer::Tag => {
                // Which record panicked is not known, so every one of them is shown.
                let blocking = (0..rows).map(|i| (i, PANICKED.to_string())).collect();
                lane.park_held(batch, records, blocking, self.captures.as_ref());
                lane.reports.send(Report::Block(
                    BlockCause::Held,
                    unprocessable_block_reason(rows, PANICKED),
                ));
                return Ok(0);
            }
            Answer::Drop => {
                self.metrics.incr(
                    "smeltr_rows_dropped_total",
                    &[("source", source_label.as_str()), ("reason", DROPPED_UNPROCESSABLE)],
                    rows as u64,
                );
                lane.reports.send(Report::Abandoned {
                    batch,
                    waits_on: Vec::new(),
                });
            }
            Answer::Deliver(targets) => {
                let mut to = targets.clone();
                to.sort_unstable();
                to.dedup();
                // Before the first hand-over: from here the batch waits on these alone. An acknowledgment from one of
                // them that comes first is kept either way, and a DLQ a refusal reaches is reported owed as it is
                // handed the row. Once only: narrowed again, the list would forget a DLQ a refusal of the first
                // attempt's rows added.
                if !again {
                    lane.reports.send(Report::Abandoned {
                        batch,
                        waits_on: to.clone(),
                    });
                }
                let mut out = Output::default();
                let these: Vec<_> = records.iter().enumerate().collect();
                self.place_delivered(&mut out, &targets, &these, &lane, None, DROPPED_UNPROCESSABLE);
                drop(these);
                match self.hand_off(lane_idx, batch, records, waiting_since, true, &to, out.rows) {
                    Err(e) => {
                        lane.reports.send(Report::Block(BlockCause::Waiting, undeliverable(&e)));
                        return Err(e);
                    }
                    Ok(HandOff::Parked) => {
                        self.metrics.incr(
                            "smeltr_batches_waiting_for_room_total",
                            &[("source", source_label.as_str())],
                            1,
                        );
                        return Ok(0);
                    }
                    Ok(HandOff::Delivered) => {}
                }
                // Labeled with where it went, not just where it came from: the leader draws this edge from the metric
                // alone, so joining it back to whatever the config currently names would let a redeploy re-attribute a
                // batch to a destination it never reached.
                for dest in &to {
                    let dest_label = dest.to_string();
                    self.metrics.incr(
                        "smeltr_batches_dlq_total",
                        &[("source", source_label.as_str()), ("destination", dest_label.as_str())],
                        1,
                    );
                }
                self.count_outcomes(&source_label, out.outcomes);
            }
        }
        // Nothing of this batch is left here: a source blocked by an earlier failed hand-over of it may resume.
        lane.reports.send(Report::Release);
        self.collect()?;
        Ok(rows)
    }

    /// Put `records` in `out` for each of `targets`, each in that destination's form (`source_form`): rows no route
    /// or pipeline answered for, unmatched or unprocessable. `text` is the batch's decoded text, where the caller has
    /// it. Counted sent at hand-off like any row; a destination that refuses one answers as it does for any row.
    ///
    /// No handle for a target is a worker built by hand with a short list; `destinations_reachable_from` gives a
    /// deployed one every destination its source can write to. Its rows are counted dropped under `dropped`.
    fn place_delivered(
        &self,
        out: &mut Output,
        targets: &[DestId],
        records: &[(usize, &SourceRecord)],
        lane: &Lane,
        text: Option<&[Cow<'_, str>]>,
        dropped: &'static str,
    ) {
        let text: Vec<Cow<'_, str>> = records
            .iter()
            .map(|&(i, r)| match (text, lane.meta.read_bytes(r)) {
                (Some(text), _) => Cow::Borrowed(text[i].as_ref()),
                (None, Some(bytes)) => lane.encoding.decode(bytes).text,
                (None, None) => Cow::Borrowed(""),
            })
            .collect();
        // The record no pipeline ran on, `{_raw: str}`, as columns: cut once, for every target that reads them. Only a
        // payload source's record has that type; a typed record gets no columns, so a Parquet target refuses it.
        let mut raw: Option<Arc<Chunk>> = None;
        let raw_record = lane.meta.decodes();
        for &to in targets {
            let Some(target) = self.destinations.iter().find(|d| d.id == to) else {
                out.outcomes.drop_rows(dropped, records.len() as u64);
                continue;
            };
            // No pipeline ran, so there is no `__destination.id` and no field to read one from: a destination keying on
            // an identity gets none and generates its own, which is the dedupe it was configured for silently not
            // happening. Counted under the name that already exists for it.
            if target.wants_doc_id {
                *out.outcomes.without_doc_id.entry(to).or_insert(0) += records.len() as u64;
            }
            let payload = self.topology.payload(to);
            let kept = self.kept(to);
            let fields = self.topology.text_fields();
            let columns = self.columnar.route(None, &self.topology, &fields);
            let chunk = match (columns.reads(&[to], &self.topology.dlq_of).columns, columns.schema) {
                (true, Some(schema)) if raw_record => Some(
                    raw.get_or_insert_with(|| Arc::new(Chunk::raw(schema, text.iter().map(AsRef::as_ref))))
                        .clone(),
                ),
                _ => None,
            };
            for (i, (&(_, record), text)) in records.iter().zip(&text).enumerate() {
                let (value, unfit) = match source_form(&payload, &lane.meta, record, text) {
                    Some(value) => (value, None),
                    None => (Vec::new(), Some(crate::io::no_form(&payload))),
                };
                out.push(
                    to,
                    OwnedRecord {
                        value,
                        payload: payload.clone(),
                        unfit,
                        kept: kept
                            .iter()
                            .filter_map(|p| Some((p.clone(), source_form(p, &lane.meta, record, text)?.into())))
                            .collect(),
                        columns: chunk.clone().map(|chunk| Columns { chunk, row: i as u32 }),
                        ..OwnedRecord::default()
                    },
                );
            }
        }
    }

    /// The forms a row bound for `dest` keeps for the DLQs down its chain.
    fn kept(&self, dest: DestId) -> Vec<Payload> {
        self.topology
            .kept(dest, self.topology.dlq_of.get(&dest).into_iter().flatten())
    }

    /// Rows handed to `dlq` in salvage and taken. Counted at once: they are handed over under their own unit, which
    /// nothing will run again.
    fn note_salvaged(&self, dlq: DestId, saved: u64) {
        if saved == 0 {
            return;
        }
        let label = dlq.to_string();
        self.metrics
            .incr("smeltr_rows_salvaged_total", &[("destination", label.as_str())], saved);
    }

    /// Rows `dest` refused with nowhere left to send them.
    fn note_lost(&self, dest: DestId, rows: u64) {
        let label = dest.to_string();
        self.metrics
            .incr("smeltr_rows_lost_total", &[("destination", label.as_str())], rows);
    }

    /// Count `o` for the batch just handed off.
    fn count_outcomes(&self, source_label: &str, o: Outcomes) {
        // One count of what met `Unmatched`, by why: no route took the row, or no delivery rule of its route did.
        for (cause, n) in [("route", o.unmatched), ("rule", o.no_rule)] {
            self.metrics.incr(
                "smeltr_rows_unmatched_total",
                &[("source", source_label), ("cause", cause)],
                n,
            );
        }
        self.metrics.incr(
            "smeltr_rows_dlq_over_budget_total",
            &[("source", source_label)],
            o.over_budget,
        );
        for (reason, n) in o.dropped {
            self.metrics.incr(
                "smeltr_rows_dropped_total",
                &[("source", source_label), ("reason", reason)],
                n,
            );
        }
        for (name, counts) in [
            ("smeltr_rows_sent_total", o.sent),
            ("smeltr_rows_without_doc_id_total", o.without_doc_id),
        ] {
            for (dest, n) in counts {
                let dest_label = dest.to_string();
                self.metrics.incr(name, &[("destination", dest_label.as_str())], n);
            }
        }
    }
}

/// Refusals kept per destination, oldest dropped first. Twenty is enough to show a pattern (a destination's detail page,
/// design doc, Control plane) without paying for a destination that refuses continuously.
const REFUSAL_RING_CAP: usize = 20;

/// Cut `s` to at most `max` characters -- characters, not bytes, since this is log text a human reads -- and
/// mark that it was cut.
pub(crate) fn truncate(s: &str, max: usize) -> String {
    match s.char_indices().nth(max) {
        None => s.to_string(),
        Some((cut, _)) => format!("{}…", &s[..cut]),
    }
}

fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// One destination's refusal, kept as it worded it -- an operator's diagnosis, not smeltr's paraphrase.
#[derive(Debug, Clone, PartialEq)]
pub struct Refusal {
    /// Milliseconds since the epoch, matching every other timestamp this process reports.
    pub at_millis: i64,
    pub message: String,
}

/// The most recent refusals every destination gave, across every reader and transform thread that met one.
///
/// Shared (`Arc`) across a deployment: a destination is one thing regardless of how many threads dispatch to it, and
/// its recent history belongs in one place rather than fragmented per thread (design doc, Control plane: "an egress
/// thread ... reports its destination").
///
/// One entry per call to [`RefusalRing::record`], not per row: `smeltr_rows_rejected_total` already counts volume, so a
/// call refusing a hundred thousand rows at once costs this ring exactly one push, and every entry is truncated to
/// [`MAX_TEXT_LEN`]. Both bounds exist because this is meant to ride every heartbeat.
#[derive(Default)]
pub struct RefusalRing {
    by_dest: Mutex<HashMap<DestId, VecDeque<Refusal>>>,
}

impl RefusalRing {
    /// Record one refusal event for `dest`, worded as `exemplar` -- the one row a caller with many to choose from
    /// picked to stand for the call. Oldest dropped past [`REFUSAL_RING_CAP`].
    pub fn record(&self, dest: DestId, exemplar: &str) {
        let entry = Refusal {
            at_millis: now_millis(),
            message: truncate(exemplar, MAX_TEXT_LEN),
        };
        let mut by_dest = self.by_dest.lock().unwrap();
        let ring = by_dest.entry(dest).or_default();
        ring.push_back(entry);
        while ring.len() > REFUSAL_RING_CAP {
            ring.pop_front();
        }
    }

    /// `dest`'s refusals, oldest first. Empty for a destination that has refused nothing.
    pub fn snapshot(&self, dest: DestId) -> Vec<Refusal> {
        self.by_dest
            .lock()
            .unwrap()
            .get(&dest)
            .map(|ring| ring.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// `dest`'s refusals as heartbeat facts, ready to append to that destination's egress thread report.
    pub fn heartbeat_facts(&self, dest: DestId) -> Vec<crate::heartbeat::MetricSampleDto> {
        self.snapshot(dest)
            .into_iter()
            .map(|r| crate::heartbeat::MetricSampleDto {
                metric: "smeltr_destination_last_refusal".to_string(),
                labels: vec![
                    ("destination".to_string(), dest.to_string()),
                    ("at_millis".to_string(), r.at_millis.to_string()),
                ],
                value: crate::heartbeat::MetricValueDto::Text(r.message),
            })
            .collect()
    }
}

/// Account for rows `dest` refused for good: one log for the call, one count, and one exemplar kept in
/// `ring`. Free: it needs the registry and the ring, not the thread.
fn report_rejections(
    metrics: &MetricsHandle,
    ring: &RefusalRing,
    source: SourceId,
    dest: DestId,
    rows: &[(usize, String)],
) {
    if rows.is_empty() {
        return;
    }
    // One event for the call. Not one per row -- a row index is this batch's own numbering of a batch the line
    // never names -- and not one per distinct reason either: a reason is not a bounded set. `io::elastic` builds
    // one per refused document, so a wholly refused batch has about as many reasons as rows, and keying on them
    // would be the per-element flood wearing a different name.
    //
    // `reasons` is what says whether this is one cause or thousands; `reason` is an example, which is all
    // `RefusalRing` below keeps for the same reason.
    let reasons: HashSet<&str> = rows.iter().map(|(_, reason)| reason.as_str()).collect();
    tracing::error!(
        target: "runtime.rows_rejected",
        subject = %dest,
        rows = rows.len(),
        reasons = reasons.len(),
        reason = %rows[0].1,
        "a destination refused rows it will refuse again as sent, so its answer to Refused answers for them"
    );
    ring.record(dest, &rows[0].1);
    let (source_label, dest_label) = (source.to_string(), dest.to_string());
    metrics.incr(
        "smeltr_rows_rejected_total",
        &[("source", source_label.as_str()), ("destination", dest_label.as_str())],
        rows.len() as u64,
    );
}

impl Reader {
    /// Write `smeltr_source_in_flight` if the outstanding count moved since it was last written. Called on every
    /// step, and once more when the reader stops, so the series a redeployed reader inherits under the same thread
    /// label says what its predecessor left behind.
    pub fn report_in_flight(&mut self) {
        if self.commits.depth() != self.reported_in_flight {
            self.reported_in_flight = self.commits.depth();
            let source_label = self.source_id.to_string();
            self.metrics.gauge(
                "smeltr_source_in_flight",
                &[("source", source_label.as_str())],
                self.reported_in_flight as f64,
            );
        }
    }

    /// One turn of ingest: apply what transform reported, commit what that completes, and poll -- or, past the depth
    /// limit, wait up to `timeout` for an acknowledgment instead.
    pub fn step(
        &mut self,
        batches: &Handoff,
        acks: &Acks,
        max_rows: usize,
        max_bytes: usize,
        timeout: Duration,
    ) -> Result<Polled, IoError> {
        let source_label = self.source_id.to_string();
        // Every step, ahead of anything that can return early: a blocked or backpressured reader stops polling, which
        // is exactly when lag is worth watching grow (E6). The source measures on its own cadence, off this thread,
        // so most calls answer empty and this costs one iteration over an already-small `Vec`.
        for sample in self.source.lag() {
            let partition_label = sample.partition.partition.to_string();
            self.metrics.gauge(
                "smeltr_source_lag",
                &[
                    ("source", source_label.as_str()),
                    ("topic", sample.partition.topic.as_ref()),
                    ("partition", partition_label.as_str()),
                ],
                sample.lag as f64,
            );
        }
        // Ahead of the early returns too: a full or blocked reader is when a sender is let go or refused.
        let dropped = self.source.take_dropped();
        if dropped > 0 {
            self.metrics.incr(
                "smeltr_source_dropped_total",
                &[("source", source_label.as_str())],
                dropped,
            );
        }
        for (reason, refused) in self.source.take_refused() {
            self.metrics.incr(
                "smeltr_source_refused_total",
                &[("source", source_label.as_str()), ("reason", reason)],
                refused,
            );
        }
        let full = self.source.take_full();
        if full > 0 {
            self.metrics
                .incr("smeltr_source_full_total", &[("source", source_label.as_str())], full);
        }
        // What waits for transform on this lane: the queue, and the batch it refused last.
        let pending = self.pending.iter().flat_map(|b| &b.records).map(SourceRecord::size);
        self.metrics.gauge(
            "smeltr_source_lane_bytes",
            &[("source", source_label.as_str())],
            (batches.bytes() + pending.sum::<usize>()) as f64,
        );
        if let Some(held) = self.source.held_bytes() {
            self.metrics.gauge(
                "smeltr_source_held_bytes",
                &[("source", source_label.as_str())],
                held as f64,
            );
        }
        let finished = self.source.take_finished();
        if finished > 0 {
            self.metrics.incr(
                "smeltr_source_messages_finished_total",
                &[("source", source_label.as_str())],
                finished,
            );
        }
        // On every step, not only an empty poll: a busy source never polls empty, and acknowledged batches stay
        // outstanding until something retires them.
        self.apply(acks.drain());
        self.retire_and_commit()?;
        // Before the wait below, so a reader parked at its depth reports the depth it is parked at.
        self.report_in_flight();
        if self.over_depth() {
            self.apply(acks.wait(timeout));
            self.retire_and_commit()?;
            if self.over_depth() {
                return self.full();
            }
        }
        // What the queue refused last time goes first: it is already the oldest thing the watermark waits on.
        if let Some(batch) = self.pending.take() {
            return match batches.push(batch, timeout) {
                Ok(()) => {
                    self.full_since = None;
                    Ok(Polled::Handed)
                }
                Err(unsent) => {
                    self.pending = Some(unsent.batch);
                    self.full()
                }
            };
        }
        self.full_since = None;
        let t = std::time::Instant::now();
        self.source.keep_headers((self.wants_headers)());
        let records = match self.source.poll_batch(max_rows, max_bytes, timeout) {
            Ok(records) => {
                self.poll_landed();
                records
            }
            Err(e) => {
                self.poll_failed(&e);
                return Err(e);
            }
        };
        self.metrics.incr_seconds(
            "smeltr_tick_poll_seconds_total",
            &[("source", source_label.as_str())],
            t.elapsed().as_secs_f64(),
        );
        if records.is_empty() {
            self.metrics
                .incr("smeltr_tick_idle_polls_total", &[("source", source_label.as_str())], 1);
            // A blocked source polls empty because it is blocked, so this is the turn that has to notice there is nothing
            // left to wait for.
            self.release_if_idle();
            return Ok(Polled::Idle);
        }
        // Tracked at poll time, in poll order: what makes the tracker's prefix rule a watermark.
        let id = self.commits.next_id();
        let mut tokens = CommitMap::new();
        for r in &records {
            tokens
                .entry(TopicPartition::new(r.topic.clone(), r.partition))
                .and_modify(|o| *o = (*o).max(r.offset))
                .or_insert(r.offset);
        }
        let waiting = self.destinations.iter().copied().collect();
        let bytes = records.iter().map(|r| r.read_size()).sum();
        self.commits.track(id, tokens, waiting, bytes);
        self.metrics.incr(
            "smeltr_rows_polled_total",
            &[("source", source_label.as_str())],
            records.len() as u64,
        );
        self.metrics.incr(
            "smeltr_bytes_polled_total",
            &[("source", source_label.as_str())],
            bytes as u64,
        );
        // A full queue is transform running behind: keep the batch, which the tracker already counts as outstanding.
        match batches.push(Handed { batch: id, records }, timeout) {
            Ok(()) => Ok(Polled::Handed),
            Err(unsent) => {
                self.pending = Some(unsent.batch);
                Ok(Polled::Full)
            }
        }
    }

    /// Commit the longest run of batches, from the oldest, that every destination has finished with.
    ///
    /// A refused commit is reported, never propagated: propagating it reaches the destination-block path, which answers
    /// for the wrong side of the worker, and the tokens are already gone from the tracker. Safe because a commit is a
    /// watermark and the next one covers what this one would have; a dead broker surfaces through `poll_batch`.
    pub fn retire_and_commit(&mut self) -> Result<(), IoError> {
        // Before anything retires, whoever calls: a batch read from a partition the group has since moved must not
        // commit there, even if the partition has come back.
        for revoked in self.source.take_revoked() {
            self.commits.forget(&revoked);
        }
        let Some(retired) = self.commits.retire() else {
            return Ok(());
        };
        let source_label = self.source_id.to_string();
        if let Err(e) = self.source.commit(&retired.tokens) {
            self.metrics
                .incr("smeltr_commits_dropped_total", &[("source", source_label.as_str())], 1);
            if !self.commit_failing {
                self.commit_failing = true;
                tracing::warn!(
                    target: "runtime.commit_dropped",
                    subject = %self.source_id,
                    error = %e,
                    "could not commit offsets, so the commit is dropped and reading continues. These records are \
                     already delivered; whatever reads these partitions next resumes from the last commit that \
                     landed, so they are redelivered rather than lost. If this persists the source itself is \
                     unreachable and its polls will say so."
                );
            }
            return Ok(());
        }
        if self.commit_failing {
            self.commit_failing = false;
            tracing::info!(
                target: "runtime.commit_resumed",
                subject = %self.source_id,
                "a source is committing offsets again"
            );
        }
        self.metrics
            .incr("smeltr_commits_total", &[("source", source_label.as_str())], 1);
        self.release_if_idle();
        Ok(())
    }

    /// Count a failed poll and log it, again every [`crate::logs::RELOG_EVERY`] while polls keep failing, so its
    /// warning stands as long as the failure does. The caller backs off and polls again.
    fn poll_failed(&mut self, e: &IoError) {
        let source_label = self.source_id.to_string();
        self.metrics.incr(
            "smeltr_source_poll_failures_total",
            &[("source", source_label.as_str())],
            1,
        );
        if self
            .poll_failing
            .is_none_or(|at| at.elapsed() >= crate::logs::RELOG_EVERY)
        {
            self.poll_failing = Some(std::time::Instant::now());
            tracing::warn!(
                target: "runtime.source_poll_failed",
                subject = %self.source_id,
                error = %e,
                "a source could not be read, and is read again after a wait"
            );
        }
    }

    /// A poll landed: say so once if polls had been failing.
    fn poll_landed(&mut self) {
        if self.poll_failing.take().is_some() {
            tracing::info!(
                target: "runtime.source_poll_recovered",
                subject = %self.source_id,
                "a source is being read again"
            );
        }
    }

    /// Resume a blocked source once nothing is outstanding. Every batch transform still holds was tracked when polled, so
    /// an empty tracker means nothing is left to re-send.
    fn release_if_idle(&mut self) {
        if self.blocked && self.commits.is_empty() {
            self.leave_block();
        }
    }

    /// The reader can take no more work this turn, and has already waited for room. Past `membership_after` of this it
    /// keeps its source's membership: a source that stops being polled holds up its group's rebalances, and is evicted
    /// once its poll interval runs out.
    fn full(&mut self) -> Result<Polled, IoError> {
        let since = *self.full_since.get_or_insert_with(std::time::Instant::now);
        if since.elapsed() >= self.membership_after {
            self.source.keep_membership(Duration::ZERO)?;
        }
        Ok(Polled::Full)
    }

    /// Act on what transform reported.
    pub fn apply(&mut self, reports: Vec<Report>) {
        for r in reports {
            match r {
                Report::Acked { batch, dest } => self.commits.acknowledge(batch, dest),
                Report::Owes { batch, dest } => self.commits.owe(batch, dest),
                Report::Abandoned { batch, waits_on } => self.commits.abandon(batch, &waits_on),
                Report::Block(cause, why) => self.enter_block(cause, &why),
                // Transform having nothing left is necessary, not sufficient: only the tracker knows what is
                // outstanding.
                Report::Release => self.release_if_idle(),
            }
        }
    }

    /// Block the source and say so, once per transition. `why` names the cause and the fix, and is kept as
    /// [`Reader::block_reason`] on every call -- not only the first -- since a source can stay blocked past the
    /// condition that first blocked it.
    fn enter_block(&mut self, cause: BlockCause, why: &str) {
        // A lesser cause does not replace a greater one: the greater outlasts it, and is reported only once.
        if self.block_cause.is_some_and(|c| c > cause) {
            return;
        }
        let source_label = self.source_id.to_string();
        self.metrics
            .gauge("smeltr_source_blocked", &[("source", source_label.as_str())], 1.0);
        self.block_reason = Some(truncate(why, MAX_TEXT_LEN));
        self.block_cause = Some(cause);
        if !self.blocked {
            self.blocked = true;
            if let Err(e) = self.source.block() {
                tracing::warn!(
                    target: "runtime.source_block_failed",
                    subject = %self.source_id,
                    error = %e,
                    "a source could not be blocked"
                );
            }
            tracing::warn!(
                target: "runtime.source_blocked",
                subject = %self.source_id,
                why,
                "a source is blocked: no offsets are being committed and nothing is being consumed on this thread, \
                 so lag will build until this clears"
            );
        }
    }

    fn leave_block(&mut self) {
        let source_label = self.source_id.to_string();
        self.metrics
            .gauge("smeltr_source_blocked", &[("source", source_label.as_str())], 0.0);
        self.block_reason = None;
        self.block_cause = None;
        if self.blocked {
            self.blocked = false;
            if let Err(e) = self.source.release() {
                tracing::warn!(
                    target: "runtime.source_release_failed",
                    subject = %self.source_id,
                    error = %e,
                    "a source could not be resumed"
                );
            }
            tracing::info!(
                target: "runtime.source_resumed",
                subject = %self.source_id,
                "a source resumed: the destination is accepting records again"
            );
        }
    }
}

/// One route's output: its rows, and the distinct destination sets they go to. A row names its set by index, so the
/// common route -- one rule, one destination -- costs every row a copy of one integer and the table one entry.
struct Serialized {
    rows: Vec<SerializedRow>,
    /// Each sorted and deduplicated, possibly empty.
    sets: Vec<Vec<DestId>>,
    /// Set when the route answered a row with block: nothing was serialized and the source blocks.
    blocking: Option<Blocked>,
    /// Rows the route answered with drop and let go. Counted by the caller once the batch is handed
    /// off: an attempt that fails is run again, and would count the same rows again.
    dropped: u64,
    /// Rows no delivery rule took, which met the route's `Unmatched` answer, and of those the ones it dropped. Counted
    /// by the caller, as `dropped` is.
    no_rule: u64,
    no_rule_dropped: u64,
    /// Each row's answer, by its row in the run: the route preview reports what was decided rather than deciding again.
    answers: Vec<Answer>,
    /// The `ErrorSink` entries the answers were made from: the pipeline's, not what a delivery rule's condition adds.
    raised: std::ops::Range<usize>,
    /// What each of `Egress::taps` keeps of this run, in that order: each row under its columns.
    captured: Vec<Vec<(Vec<crate::heartbeat::CaptureColumn>, crate::heartbeat::CapturedRow)>>,
}

impl Serialized {
    /// A run that blocked its source: nothing of it is sent.
    fn blocked(blocked: Blocked, answers: Vec<Answer>, raised: std::ops::Range<usize>) -> Serialized {
        Serialized {
            rows: Vec::new(),
            sets: Vec::new(),
            blocking: Some(blocked),
            dropped: 0,
            no_rule: 0,
            no_rule_dropped: 0,
            answers,
            raised,
            captured: Vec::new(),
        }
    }
}

/// What a block on a route's rows says about them.
struct Blocked {
    rows: usize,
    cause: Cause,
    /// Each record that blocked, by its index in the run's records, with why.
    records: Vec<(usize, String)>,
}

/// Why a route's rows blocked its source.
enum Cause {
    /// The pipeline marked them with a kind the route's exceptions block. `pipeline` is `Plan::name`, and `first` the
    /// first marked row's exception, as the end of a clause: "at step parse: <what went wrong>". A record's own reason
    /// is worded as `first`.
    Marked { pipeline: String, first: String },
    /// No delivery rule of route `route` took them, and its `Unmatched` answer is block.
    Declined { route: String },
}

struct SerializedRow {
    /// The row's JSON document, where a destination it is bound for, or a DLQ down a chain from one, writes it.
    bytes: Vec<u8>,
    /// Each of `Columnar::fields` on this row, where a destination it is bound for, or a DLQ down a chain from one,
    /// writes text. `None` is a field the row does not have.
    texts: Vec<Option<String>>,
    /// Index into `Serialized::sets`.
    set: u32,
    /// The row's value of each `Placement`, in `Placement::ALL` order.
    placements: [Option<String>; Placement::ALL.len()],
    /// The pipeline's `__destination.id`, if set. Wins over `doc_id_by_dest`.
    doc_id: Option<String>,
    /// Each interested destination's configured `id_field` value on this row. Per destination because the field name is
    /// per-destination config.
    doc_id_by_dest: Vec<(DestId, String)>,
    /// The record it was made from, by its index in the batch: an `Unroll` makes several rows of one.
    origin: u32,
    /// The row as columns, where a destination it is bound for, or a DLQ down a chain from one, writes them.
    columns: Option<Columns>,
}

/// One batch as it arrived: the records, their text, what their source says they carry, and the instant it is
/// processed at.
struct Ingest<'a> {
    records: &'a [SourceRecord],
    raw: &'a RawText<'a>,
    meta: &'a SourceMeta,
    /// The source's name in the deployment, what `__source.name` reads.
    source: Option<&'a str>,
    /// What `now()` reads for every row and every route of these records. See [`Ctx::now_micros`].
    now_micros: i64,
    /// Where each Parser tallies the fields it produces, for the one batch an interval describes to the leader
    /// (`streams::Streams::due`). `None` for every other batch.
    tally: Option<&'a mut crate::core::tally::FieldTally>,
}

/// A batch's records as text, decoded once per attempt and rebuilt as a column for routing and for each route.
struct RawText<'a> {
    text: Vec<Cow<'a, str>>,
    /// What a decode had to do, per row. Replayed into every route's exception window, so a repaired or guessed-at
    /// row is marked wherever it is delivered.
    notes: Vec<(u32, ExceptionKind)>,
}

/// Decode every record's `__source.bytes` with the source's encoding into its `_raw`; a row, which has none, has no
/// text. One pass per batch:
/// `_raw` is rebuilt per route, but from this text, so nothing decodes twice and a note is recorded once.
fn decode_records<'a>(records: &'a [SourceRecord], meta: &SourceMeta, encoding: Encoding) -> RawText<'a> {
    let mut text = Vec::with_capacity(records.len());
    let mut notes = Vec::new();
    for (row, read) in meta.read_bytes_each(records).enumerate() {
        let Some(bytes) = read else {
            text.push(Cow::Borrowed(""));
            continue;
        };
        let decoded = encoding.decode(bytes);
        if decoded.replaced {
            notes.push((row as u32, ExceptionKind::InvalidUtf8));
        }
        if let Some(guess) = decoded.guessed {
            notes.push((row as u32, ExceptionKind::GuessedEncoding { encoding: guess.name() }));
        }
        text.push(decoded.text);
    }
    RawText { text, notes }
}

/// A record in the form `payload` names, for a row no pipeline made: what it read as it arrived, the document a
/// pipeline with no steps would write for it, or one of that document's fields. `text` is a payload record's
/// `_raw`. `None` for a field the record does not have.
fn source_form(payload: &Payload, meta: &SourceMeta, record: &SourceRecord, text: &str) -> Option<Vec<u8>> {
    match payload {
        Payload::Bytes { field } => meta.field_bytes(record, field),
        Payload::Record => Some(meta.record_json(record, text)),
        Payload::Text { field } => meta.record_text(record, text, field),
    }
}

/// Where a route's output goes: the route itself, and the destination config its rules can reach.
struct Egress<'a> {
    route: &'a CompiledRoute,
    /// `(destination, field name)` for every destination that reads the document identity off a field of the row.
    id_fields: &'a [(DestId, String)],
    /// `CompiledTopology::dlq_of`: a row's identity is read for the DLQs its destination salvages to as well.
    dlq_of: &'a HashMap<DestId, Vec<DestId>>,
    /// How this route's rows reach the destinations that write columns. `None` writes every row as JSON and cuts no
    /// columns: the preview, which shows each row's document.
    columns: Option<RouteColumns<'a>>,
    /// The captures at a destination this route's rows are copied for, as the pipeline finished them.
    taps: &'a [RouteTap],
}

/// A capture at a destination's filter, compiled against one route's output (`topology::output_condition`).
struct OutputFilter {
    ir: crate::expr::ir::Ir,
    patterns: Vec<crate::core::ctx::CompiledPattern>,
}

/// A capture at a destination's filter, as one route delivering there makes of it.
enum RouteFilter {
    /// `None` keeps every row bound there.
    Keeps(Option<Arc<OutputFilter>>),
    /// It does not compile against the route's output: the route copies nothing for it.
    Skips,
}

/// One capture at a destination, as one route run copies rows for it.
struct RouteTap {
    /// Index into the captures the attempt was served.
    at: usize,
    dest: DestId,
    /// `DestinationTarget::placement`.
    placement: Option<Placement>,
    /// `None` keeps every row bound there.
    filter: Option<Arc<OutputFilter>>,
    /// The most rows the capture still takes.
    wanted: usize,
    /// The most bytes of rows it still takes.
    room: usize,
}

/// What each destination writes of a row, and, for those that write Parquet, each route's record as their columns
/// (`CompiledTopology::records`).
#[derive(Default)]
pub struct Columnar {
    dests: HashSet<DestId>,
    /// By route, `None` for the rows no pipeline ran on.
    schemas: HashMap<Option<u32>, parquet::Schema>,
}

/// `Columnar` for one route.
#[derive(Clone, Copy)]
struct RouteColumns<'a> {
    dests: &'a HashSet<DestId>,
    /// `None` where no record was made for the route: no destination it reaches writes columns.
    schema: Option<&'a parquet::Schema>,
    /// What each destination writes.
    topology: &'a CompiledTopology,
    /// `CompiledTopology::text_fields`: what a row's `texts` hold, in this order.
    fields: &'a [String],
}

impl Columnar {
    pub fn of(topology: &CompiledTopology) -> Columnar {
        let mut schemas = HashMap::new();
        let mut records = HashMap::new();
        for (&route, record) in topology.records.values().flatten() {
            // A route has one record, the pipeline's, and every destination on it takes the chunk cut to that record.
            // A table destination checks each row against a schema built from its own record (`io::files`), which
            // only holds if the two are one.
            debug_assert!(
                records.insert(route, record).is_none_or(|first| first == record),
                "route {route:?} writes two records"
            );
            // A record Parquet cannot hold fails the bundle's compile. One built by hand cuts no columns, and its rows
            // are refused where they are delivered.
            if let Ok(schema) = parquet::Schema::new(record) {
                schemas.entry(route).or_insert(schema);
            }
        }
        Columnar {
            dests: topology.records.keys().copied().collect(),
            schemas,
        }
    }

    /// For `route`, with what `topology` says each destination writes and `fields`, its `text_fields`.
    fn route<'a>(
        &'a self,
        route: Option<u32>,
        topology: &'a CompiledTopology,
        fields: &'a [String],
    ) -> RouteColumns<'a> {
        RouteColumns {
            dests: &self.dests,
            schema: self.schemas.get(&route),
            topology,
            fields,
        }
    }
}

/// What a row bound for a set of destinations is read as there, or at a DLQ down a chain from there.
#[derive(Clone, Copy)]
struct Reads {
    json: bool,
    columns: bool,
    /// One of `Columnar::fields`.
    text: bool,
}

impl RouteColumns<'_> {
    fn reads(&self, set: &[DestId], dlq_of: &HashMap<DestId, Vec<DestId>>) -> Reads {
        let chains = set.iter().flat_map(|d| dlq_of.get(d).into_iter().flatten());
        let (mut json, mut columns, mut text) = (false, false, false);
        for d in set.iter().chain(chains) {
            match self.topology.payload(*d) {
                _ if self.dests.contains(d) => columns = self.schema.is_some(),
                Payload::Record => json = true,
                Payload::Text { .. } => text = true,
                Payload::Bytes { .. } => {}
            }
        }
        Reads { json, columns, text }
    }
}

/// Run `plan` over the rows `mask` selects, then serialize every row still live, pairing each with the destination mask
/// the route's `deliver` rules chose and its placements and `__destination.id`.
///
/// A row that picked up exceptions during this call takes the route's answer to them (`answer_rows`): ignored, it
/// goes on as it is; tagged, it goes on with its tag field set and is placed by the delivery rules like any other row;
/// dropped, it is gone before the rules run; delivered, it goes to the answer's targets instead of the rules'; blocked,
/// nothing is serialized and `Serialized::blocking` says why. Only exceptions from this call count, not an earlier
/// route's.
fn run_and_serialize(
    plan: &Plan,
    ingest: Ingest<'_>,
    mask: Column,
    pool: &mut BufferPool,
    errors: &mut ErrorSink,
    metrics: &MetricsHandle,
    egress: Egress<'_>,
) -> Serialized {
    let Ingest {
        records,
        raw,
        meta,
        source,
        now_micros,
        tally,
    } = ingest;
    let route = egress.route;
    let id_fields = egress.id_fields;
    let deliver = &route.deliver;
    let rows = records.len();
    let on_exception = &route.on_exception;
    let mut batch = Batch::new(&plan.delivery_layout(), rows, 0);
    let mut origin = Column::alloc(pool, &DataType::I64, rows);
    for (row, at) in origin.i64_mut().unwrap().values.iter_mut().enumerate() {
        *at = row as i64;
    }
    batch.put(plan.origin_slot(), origin, pool);
    // `reads_meta` covers the pipeline and its route's delivery rules. A destination's `id_field` reads the same batch
    // and is configured on the worker, not in the plan, so it is unioned in here.
    let fields = meta.source_fields();
    // A capture shows every `__source` field, and its filter, compiled after the plan, may read any of them: neither
    // is in `reads_meta`, so while one is open here every field is filled.
    let watched = !egress.taps.is_empty();
    let wanted =
        |i: usize| watched || plan.reads_meta[i] || egress.id_fields.iter().any(|(_, field)| field == fields[i].0);
    let noted = meta.ingest(&mut batch, records, &raw.text, source, wanted, pool);

    let err_start = errors.len();
    {
        let mut ctx = Ctx::at(pool, errors, now_micros);
        ctx.fields = tally;
        // Ingest's repairs, inside this route's window so a repaired row is marked wherever it goes and an `Unroll`
        // remaps it with the rest.
        for (row, kind) in &raw.notes {
            let raw = ctx
                .errors
                .raise(*row as usize, INGEST_FUNCTION, Some(SLOT_RAW), kind.clone());
            raw.read = vec![Read::new(Some((SLOT_RAW, String::new())), None)];
        }
        for (row, slot, kind, value) in noted {
            let at = ctx.errors.raise(row as usize, INGEST_FUNCTION, Some(slot), kind);
            if value.is_some() {
                at.read = vec![Read::new(Some((slot, String::new())), value)];
            }
        }
        exec::run_scoped(plan, &mut batch, mask, &mut ctx, Some(metrics));
    }
    // `Op::Unroll` may have replaced the batch with a larger one.
    let rows = batch.rows();
    let slot_names = plan.slot_names();
    let entries = &errors.entries()[err_start..];
    // A row the pipeline itself dropped is not this setting's to answer for.
    let (answers, marked) = {
        let live = batch.peek(plan.live_slot).bool().unwrap();
        answer_rows(entries, on_exception, &live, rows, &slot_names, &plan.step_ids)
    };
    let raised = err_start..errors.len();
    let rows_answered = |want: fn(&Answer) -> bool| answers.iter().filter(move |a| want(a)).count();

    let blocked = rows_answered(|a| *a == Answer::Block);
    if blocked > 0 {
        let exception = |a: &crate::core::error::Exception| {
            let what = a.message(&slot_names);
            match plan.step_ids.get(a.function as usize) {
                Some(step) => format!("at step {step}: {what}"),
                None => format!("with: {what}"),
            }
        };
        let blocking = entries.iter().filter(|a| {
            answers.get(a.row as usize) == Some(&Answer::Block) && *on_exception.answer(&a.kind) == Answer::Block
        });
        let first = blocking.clone().next().map(exception).unwrap_or_default();
        // By record, not row: an `Unroll` may have made several rows of one record, and a capture shows records.
        let origin = batch.peek(plan.origin_slot()).i64().unwrap().values;
        let mut by_record: std::collections::BTreeMap<usize, String> = std::collections::BTreeMap::new();
        for a in blocking {
            by_record
                .entry(origin[a.row as usize] as usize)
                .or_insert_with(|| exception(a));
        }
        let records = by_record.into_iter().collect();
        let blocked = Blocked {
            rows: blocked,
            cause: Cause::Marked {
                pipeline: plan.name.clone(),
                first,
            },
            records,
        };
        batch.reclaim(pool);
        return Serialized::blocked(blocked, answers, raised);
    }
    let tag_slot = plan.tag_slot();
    let at = RaisedIn {
        route: Some(&route.id),
        pipeline: &plan.name,
    };
    batch.put(tag_slot, tag_column(&marked, at, rows, pool), pool);

    // One vectorized evaluation per rule, over the finished batch.
    let rule_masks: Vec<Option<Column>> = deliver
        .iter()
        .map(|rule| {
            rule.when.as_ref().map(|ir| {
                let mut ctx = Ctx::at(pool, errors, now_micros);
                ctx.patterns = &rule.compiled_patterns;
                crate::expr::eval::eval(ir, &batch, &mut ctx)
            })
        })
        .collect();
    // Rules are additive; a null condition does not match.
    let rules_take = |row: usize, into: &mut Vec<DestId>| {
        for (rule, matched) in deliver.iter().zip(&rule_masks) {
            let hit = match matched {
                Some(m) => match m.bool() {
                    Ok(b) => b.validity.is_valid(row) && bits::get_row(b.values, row),
                    Err(_) => false,
                },
                None => true,
            };
            if hit {
                into.extend_from_slice(&rule.to);
            }
        }
    };
    let live = batch.peek(plan.live_slot).bool().unwrap();
    let is_live = |row: usize| live.validity.is_valid(row) && bits::get_row(live.values, row);
    // A row no rule takes meets the route's `Unmatched` answer. Under block nothing of the run is sent, so that is
    // decided before any row is written.
    let unmatched = on_exception.answer(&ExceptionKind::Unmatched);
    if *unmatched == Answer::Block {
        let mut taken = Vec::new();
        let declined: Vec<usize> = (0..rows)
            .filter(|&row| is_live(row) && matches!(answers[row], Answer::Ignore | Answer::Tag))
            .filter(|&row| {
                taken.clear();
                rules_take(row, &mut taken);
                taken.is_empty()
            })
            .collect();
        if !declined.is_empty() {
            let origin = batch.peek(plan.origin_slot()).i64().unwrap().values;
            // By record, as a marked row's block is.
            let records: std::collections::BTreeSet<usize> = declined.iter().map(|&row| origin[row] as usize).collect();
            let records = records.into_iter().map(|r| (r, DECLINED_MESSAGE.to_string())).collect();
            let blocked = Blocked {
                rows: declined.len(),
                cause: Cause::Declined {
                    route: route.id.clone(),
                },
                records,
            };
            for m in rule_masks.into_iter().flatten() {
                m.reclaim(pool);
            }
            batch.reclaim(pool);
            return Serialized::blocked(blocked, answers, raised);
        }
    }

    // On a row with exceptions, the tag first, as the record's own field at its level: its name claims its key there.
    // Every other row is written as the pipeline left it.
    let tagged =
        RecordView::new(&plan.tree, |slot| batch.peek(slot)).with_field(&on_exception.tag_field, batch.peek(tag_slot));
    let plain = RecordView::new(&plan.tree, |slot| batch.peek(slot));

    // A destination whose `id_field` names nothing this pipeline produces is not an error: the same destination may be
    // fed by several routes.
    let doc_id_slot = plan.id_out_slot;
    let origin = batch.peek(plan.origin_slot()).i64().unwrap().values;

    let mut out = Vec::new();
    let mut sets: Vec<Vec<DestId>> = Vec::new();
    // Interned: rows of one route share a handful of sets, usually one. The last set is tried first because
    // consecutive rows almost always match the same rules.
    let mut intern = |set: &[DestId]| {
        let i = match sets.iter().rposition(|s| *s == set) {
            Some(i) => i,
            None => {
                sets.push(set.to_vec());
                sets.len() - 1
            }
        };
        u32::try_from(i).expect("distinct destination sets in one route run fit a u32")
    };
    // What each interned set reads a row as, by its index in `sets`.
    let mut reads: Vec<Reads> = Vec::new();
    let mut reads_of = |set: u32, dests: &[DestId]| {
        if reads.len() == set as usize {
            reads.push(match egress.columns {
                Some(c) => c.reads(dests, egress.dlq_of),
                None => Reads {
                    json: true,
                    columns: false,
                    text: false,
                },
            });
        }
        reads[set as usize]
    };
    let mut scratch: Vec<DestId> = Vec::new();
    let (mut no_rule, mut no_rule_dropped) = (0u64, 0u64);
    // Every document is written here first, then copied out at its own length. The buffer keeps its capacity across
    // the batch, so the growth series is paid once. The copies live on in `Serialized`, so each is sized to the row
    // it holds; a buffer sized from a neighbor would make every row carry the largest row's size.
    let mut doc: Vec<u8> = Vec::new();
    // Each row bound for columns, by its place in `out`: cut into one chunk once every row is placed.
    let mut cut: Vec<(usize, usize)> = Vec::new();
    // Each row of `out`'s row in the batch, while a capture is copying them.
    let mut batch_row: Vec<usize> = Vec::new();
    for row in 0..rows {
        if !is_live(row) || answers[row] == Answer::Drop {
            continue;
        }
        let serializer = tag_or_not(&tagged, &plain, &marked, row);
        // Written once, at the first set that reads it as JSON.
        let mut written = false;
        let mut document = |reads: Reads, doc: &mut Vec<u8>| {
            if !reads.json {
                return Vec::new();
            }
            if !written {
                serializer.write_row(row, doc);
                written = true;
            }
            doc.as_slice().to_vec()
        };
        let origin = origin[row] as u32;
        let fields = egress.columns.map_or(&[][..], |c| c.fields);
        let texts = |reads: Reads| match reads.text {
            true => fields.iter().map(|f| serializer.text(f, row)).collect(),
            false => Vec::new(),
        };
        // A delivered row goes where its answer says, and to none of the rules'.
        if let Answer::Deliver(targets) = &answers[row] {
            scratch.clear();
            scratch.extend_from_slice(targets);
            scratch.sort_unstable();
            let set = intern(&scratch);
            let reads = reads_of(set, &scratch);
            if reads.columns {
                cut.push((out.len(), row));
            }
            out.push(SerializedRow {
                bytes: document(reads, &mut doc),
                texts: texts(reads),
                set,
                placements: Default::default(),
                doc_id: None,
                doc_id_by_dest: Vec::new(),
                origin,
                columns: None,
            });
            if !egress.taps.is_empty() {
                batch_row.push(row);
            }
            continue;
        }
        scratch.clear();
        rules_take(row, &mut scratch);
        if scratch.is_empty() {
            no_rule += 1;
            // Block returned above, and `Unmatched` is never ignored or tagged (`config_file::load`).
            let Answer::Deliver(targets) = unmatched else {
                no_rule_dropped += 1;
                continue;
            };
            scratch.extend_from_slice(targets);
        }
        scratch.sort_unstable();
        scratch.dedup();
        let set = intern(&scratch);
        let placements = plan
            .placement_slots
            .map(|slot| slot.and_then(|slot| read_text(&batch, slot, row)));
        let doc_id = doc_id_slot.and_then(|slot| read_text(&batch, slot, row));
        // A chosen destination's DLQ counts as reachable: `offer_salvage` sends there after the columns are gone, so this
        // is the only moment its identity can be read.
        let chosen = &scratch;
        let doc_id_by_dest: Vec<(DestId, String)> = if doc_id.is_some() {
            Vec::new()
        } else {
            id_fields
                .iter()
                .filter(|(dest, _)| {
                    chosen.contains(dest)
                        || chosen
                            .iter()
                            .any(|c| egress.dlq_of.get(c).is_some_and(|dlqs| dlqs.contains(dest)))
                })
                .filter_map(|(dest, name)| serializer.text(name, row).map(|v| (*dest, v)))
                .collect()
        };
        let reads = reads_of(set, &scratch);
        if reads.columns {
            cut.push((out.len(), row));
        }
        out.push(SerializedRow {
            bytes: document(reads, &mut doc),
            texts: texts(reads),
            set,
            placements,
            doc_id,
            doc_id_by_dest,
            origin,
            columns: None,
        });
        if !egress.taps.is_empty() {
            batch_row.push(row);
        }
    }
    if let Some(schema) = egress.columns.and_then(|c| c.schema)
        && !cut.is_empty()
    {
        // The tagged rows first, then the rest, each part read through its own view.
        let (on_tagged, on_plain): (Vec<_>, Vec<_>) =
            cut.iter().partition(|(_, row)| marked.contains_key(&(*row as u32)));
        let tagged_rows: Vec<u32> = on_tagged.iter().map(|&&(_, row)| row as u32).collect();
        let plain_rows: Vec<u32> = on_plain.iter().map(|&&(_, row)| row as u32).collect();
        let chunk = Arc::new(Chunk::cut(
            schema,
            &[(&tagged, &tagged_rows), (&plain, &plain_rows)],
            now_micros,
            pool,
        ));
        for (place, &(at, _)) in on_tagged.into_iter().chain(on_plain).enumerate() {
            out[at].columns = Some(Columns {
                chunk: chunk.clone(),
                row: place as u32,
            });
        }
    }

    let captured = copy_for_captures(
        plan,
        route,
        egress.taps,
        &batch,
        Rows {
            out: &out,
            sets: &sets,
            batch_row: &batch_row,
        },
        |row, doc| tag_or_not(&tagged, &plain, &marked, row).write_row(row, doc),
        now_micros,
    );

    for m in rule_masks.into_iter().flatten() {
        m.reclaim(pool);
    }
    batch.reclaim(pool);
    Serialized {
        rows: out,
        sets,
        blocking: None,
        dropped: rows_answered(|a| *a == Answer::Drop) as u64,
        no_rule,
        no_rule_dropped,
        answers,
        raised,
        captured,
    }
}

/// A route run's rows, as `run_and_serialize` placed them.
struct Rows<'a> {
    out: &'a [SerializedRow],
    sets: &'a [Vec<DestId>],
    /// Each of `out`'s row in the batch.
    batch_row: &'a [usize],
}

/// What each of `taps` keeps of a route run: the rows bound for its destination that its filter matches, up to what it
/// still takes, each read from `batch` as the pipeline finished it. `write` writes a row's document, as it is sent. Each
/// row also carries every `__` field the pipeline holds for it (`worker::capture::internal_columns`), which the document
/// leaves out: a capture is how a person sees what steered the row.
///
/// A filter's own exceptions are not the pipeline's, so they go to a sink of their own and mark no row: a row the
/// filter cannot decide on is not kept. Its memory is its own too, from a pool outside the memory budget: watching a
/// flow must not be what pushes a batch over it and changes how the batch is delivered.
fn copy_for_captures(
    plan: &Plan,
    route: &CompiledRoute,
    taps: &[RouteTap],
    batch: &Batch,
    rows: Rows<'_>,
    write: impl Fn(usize, &mut Vec<u8>),
    now_micros: i64,
) -> Vec<Vec<(Vec<crate::heartbeat::CaptureColumn>, crate::heartbeat::CapturedRow)>> {
    if taps.is_empty() {
        return Vec::new();
    }
    let record = crate::plan::dictionary::record_type(plan, &route.on_exception.tag_field);
    let written = crate::worker::capture::written_columns(&record);
    let internal = crate::worker::capture::internal_columns(plan);
    let mut doc = Vec::new();
    let mut errors = ErrorSink::new();
    let mut pool = BufferPool::new();
    taps.iter()
        .map(|tap| {
            let keep = tap.filter.as_ref().map(|f| {
                let mut ctx = Ctx::at(&mut pool, &mut errors, now_micros);
                ctx.patterns = &f.patterns;
                crate::expr::eval::eval(&f.ir, batch, &mut ctx)
            });
            let kept = |row: usize| match &keep {
                None => true,
                Some(mask) => mask
                    .bool()
                    .is_ok_and(|b| b.validity.is_valid(row) && bits::get_row(b.values, row)),
            };
            let mut copied = Vec::new();
            let mut bytes = 0;
            for (i, r) in rows.out.iter().enumerate() {
                if copied.len() >= tap.wanted || bytes >= tap.room {
                    break;
                }
                let row = rows.batch_row[i];
                if !rows.sets[r.set as usize].contains(&tap.dest) || !kept(row) {
                    continue;
                }
                doc.clear();
                write(row, &mut doc);
                let object = crate::worker::capture::tapped_object(&doc);
                let doc_id = r.doc_id.clone().or_else(|| {
                    r.doc_id_by_dest
                        .iter()
                        .find(|(d, _)| *d == tap.dest)
                        .map(|(_, v)| v.clone())
                });
                let placement = tap.placement.and_then(|p| r.placements[p as usize].clone());
                let fields = internal
                    .iter()
                    .map(|(_, slot)| {
                        doc.clear();
                        let ty = &plan.slot_layout[*slot];
                        let written = crate::format::json::write_value(batch.peek(*slot), ty, row, &mut doc);
                        crate::worker::capture::tapped_value(written.then_some(&doc[..]))
                    })
                    .collect();
                let sent = crate::worker::capture::sent_row(
                    object,
                    &written,
                    &internal,
                    fields,
                    [placement, doc_id, Some(route.id.clone())],
                );
                bytes += sent.1.size();
                copied.push(sent);
            }
            if let Some(mask) = keep {
                mask.reclaim(&mut pool);
            }
            copied
        })
        .collect()
}

/// One row's text from a `Str`/`Bytes` slot or a variant, or `None` where null. Bytes read as their base64.
fn read_text(batch: &Batch, slot: SlotId, row: usize) -> Option<String> {
    let col = batch.peek(slot);
    match col.data_type() {
        DataType::Str | DataType::Bytes | DataType::Variant => scalar_text(col, row),
        _ => None,
    }
}

/// A batch for route evaluation: the prefix, the record, and every `__source` field the source declares, `__live`
/// primed all-true.
fn seed_ingest_batch(
    pool: &mut BufferPool,
    records: &[SourceRecord],
    raw: &RawText<'_>,
    meta: &SourceMeta,
    source: Option<&str>,
) -> Batch {
    let rows = records.len();
    let layout: Vec<DataType> = crate::plan::plan::ingest_fields(meta)
        .into_iter()
        .map(|(_, t)| t)
        .collect();
    let mut batch = Batch::new(&layout, rows, 0);

    let mut live = Column::alloc(pool, &DataType::Bool, rows);
    bits::fill_valid(live.bool_mut().unwrap().values, rows);
    batch.put(SLOT_LIVE, live, pool);

    // Every declared field, not just what one plan reads: a route condition may test any of them before a pipeline is
    // chosen. What ingest notes is not a row's to carry from here: the route's own run builds and notes it.
    meta.ingest(&mut batch, records, &raw.text, source, |_| true, pool);

    batch
}

// -- route preview ------------------------------------------------------
//
// The leader answers "where would these records go" by running the same three steps a transform thread runs --
// seed the batch, mask the routes, run and serialize each matched route -- and stopping where it would dispatch.
// `SerializedRow` already carries the document, the destinations the delivery rules chose and the identity each
// would file it under, so nothing stands in for a destination: there is no preview destination to keep in step
// with a real one, and none of this can reach a broker.

/// One document a route produced, and where it would go.
pub struct PreviewedDocument {
    /// The document as it would leave, the pipeline's exception tag included.
    pub document: String,
    /// Every destination this route's `deliver` rules chose. Empty is a row every rule declined.
    pub destinations: Vec<DestId>,
    /// Every `__destination` value that steers this row, by its name under `__destination`. The identity is the one
    /// a destination asking for one would file it under.
    pub destination: std::collections::BTreeMap<&'static str, String>,
}

/// One route a record went through, and everything that came out of it.
pub struct PreviewedRoute {
    /// Index into `CompiledTopology::routes`.
    pub route: usize,
    /// What this route made of the record. More than one document when the pipeline unrolls an array; none when it
    /// or its answer dropped the record, or its answer blocks.
    pub documents: Vec<PreviewedDocument>,
    /// Each exception this route's pipeline raised on the record, as its kind's label and its message, deduplicated.
    /// One on a row the pipeline then filtered stays listed, though nothing answered it.
    pub exceptions: Vec<(&'static str, String)>,
    /// The record's answer on this route: its rows' answers joined, with the route's `Unmatched` answer for a row no
    /// delivery rule took. `Ignore` when nothing answered it.
    pub answer: Answer,
}

/// One record's whole trip.
pub struct PreviewedRow {
    pub row: usize,
    /// Every route whose condition selected this record, in topology order.
    pub matched: Vec<usize>,
    /// What each matched route made of it. Empty when another record of the sample blocks: a block sends nothing
    /// from any route, for any record polled with it. The record that blocks keeps only the route that blocked.
    pub through: Vec<PreviewedRoute>,
    /// Why this record stopped short, where it did: it asked for more memory than a preview may hold, it blocks its
    /// source, or another record of the sample does. `through` carries whatever completed before a memory refusal.
    pub refused: Option<String>,
}

/// The ceiling on columnar memory one previewed record may hold, unless `SMELTR_PREVIEW_BUDGET_BYTES` says
/// otherwise.
///
/// A preview runs in the leader, which is also what serves the GUI, the config API and every worker's heartbeat.
/// `Op::Unroll` gathers `_raw` into each row it produces, so a record holding a large array asks for orders of
/// magnitude more than it weighs; unbudgeted, that allocation ends the process the fleet reports to.
pub(crate) const PREVIEW_BUDGET_BYTES: usize = 256 * 1024 * 1024;

/// The one ceiling every preview in this process shares. What is protected is the leader's memory, which is one
/// number however many people are previewing.
pub fn preview_memory_budget() -> MemoryBudget {
    static BUDGET: std::sync::OnceLock<MemoryBudget> = std::sync::OnceLock::new();
    BUDGET
        .get_or_init(|| MemoryBudget::new(crate::startup_config::preview_budget_bytes()))
        .clone()
}

/// [`preview_routes_at`], with `now()` read from the clock rather than pinned, and each record's text its bytes read
/// as UTF-8.
pub fn preview_routes(
    topology: &CompiledTopology,
    destinations: &[crate::plan::topology::DestinationConfig],
    source: SourceId,
    source_name: &str,
    records: &[SourceRecord],
    budget: MemoryBudget,
) -> Vec<PreviewedRow> {
    let meta = topology.meta_for(source);
    let texts: Vec<String> = records
        .iter()
        .map(|r| String::from_utf8_lossy(meta.read_bytes(r).unwrap_or_default()).into_owned())
        .collect();
    preview_routes_at(
        topology,
        destinations,
        source,
        source_name,
        records,
        &texts,
        budget,
        crate::core::ctx::wall_clock_micros(),
    )
}

/// Route `records` as if they had arrived through `source`, named `source_name` in the deployment, and say where each
/// would go.
///
/// `texts` are the records' `_raw`, one per record, as a sample holds it (`config_file::sample`): a paste arrives as
/// text and a capture decodes with its source's encoding, so nothing decodes here and no row can pick up a decode
/// exception the real ingest would not have given it. A record's bytes are its payload as read.
///
/// **One record at a time.** A pipeline does not preserve row identity -- `Op::Unroll` replaces the batch with one
/// row per array element -- so a batch's output rows cannot be attributed back to the records that produced them.
/// Running each record alone makes that attribution true by construction rather than by an assumption the compiler
/// is free to break. Routing is unaffected: eligibility is per row, so a record's masks do not depend on what it
/// shares a batch with.
///
/// `now_micros` is read once for the whole call and given to every record and every route, as a live worker gives each
/// record one: `now()` and a reference-less `parseTime` put the clock into the output, so a run can be
/// repeated to the same answer only against the instant it was given, and a condition on time cannot answer
/// differently part way down a sample.
///
/// Each record is budgeted ([`preview_memory_budget`]) and gets a pool of its own, so one record asking for more
/// than a preview may hold is reported on that record and the rest of the sample still previews. A live batch
/// splits on a refusal and retries; a preview cannot, because halving a record's output would answer a question
/// nobody asked.
#[allow(clippy::too_many_arguments)]
pub fn preview_routes_at(
    topology: &CompiledTopology,
    destinations: &[crate::plan::topology::DestinationConfig],
    source: SourceId,
    source_name: &str,
    records: &[SourceRecord],
    texts: &[String],
    budget: MemoryBudget,
    now_micros: i64,
) -> Vec<PreviewedRow> {
    let meta = topology.meta_for(source);
    let id_fields: Vec<(DestId, String)> = destinations
        .iter()
        .filter_map(|d| d.kind.id_field().map(|f| (d.id, f.to_string())))
        .collect();

    // Counters this run produces are thrown away with it: a preview is not traffic, and adding it to the process
    // registry would put it in the leader's own metrics and in anything reading them.
    let metrics = MetricsHandle::new(Arc::new(crate::metrics::Metrics::new()), "preview");

    // The first record that blocks its source, and why. A live block holds the whole poll, so once the sample is
    // run every other record is shown held with it: the preview cannot know which records a poll would put together,
    // and showing the others delivered would promise deliveries a block may stop.
    let mut blocker: Option<usize> = None;
    let mut rows: Vec<PreviewedRow> = (0..records.len())
        .map(|row| {
            let slice = &records[row..row + 1];
            let raw = RawText {
                text: vec![Cow::Borrowed(texts[row].as_str())],
                notes: Vec::new(),
            };
            let mut entry = PreviewedRow {
                row,
                matched: Vec::new(),
                through: Vec::new(),
                refused: None,
            };

            // Per record, as a live attempt does per attempt: a refusal unwinds past every buffer it was holding,
            // and the pool it allocated from is discarded with it.
            let mut pool = BufferPool::with_budget(budget.clone());
            let mut errors = ErrorSink::new();

            let masks = buffer::catch_over_budget(|| {
                let mut ingest = seed_ingest_batch(&mut pool, slice, &raw, &meta, Some(source_name));
                let masks = {
                    let mut ctx = Ctx::at(&mut pool, &mut errors, now_micros);
                    route_masks(&topology.routes, source, &mut ingest, &mut ctx, None)
                };
                ingest.reclaim(&mut pool);
                masks
            });
            let masks = match masks {
                Ok(masks) => masks,
                Err(over) => {
                    entry.refused = Some(over.to_string());
                    return entry;
                }
            };

            // Every mask reaches `reclaim`, the ones a refusal leaves behind included: `Column`'s `Drop` asserts
            // against a column dropped still holding buffers.
            let mut masks = masks.into_iter();
            while let Some((route_idx, mask)) = masks.next() {
                // Read the mask before it is handed on: it is what says a route *matched*, which a route whose
                // pipeline then dropped the record would otherwise have no way to report.
                let matched = mask
                    .bool()
                    .map(|b| b.validity.is_valid(0) && bits::get_row(b.values, 0))
                    .unwrap_or(false);
                if matched {
                    entry.matched.push(route_idx);
                }

                let route = &topology.routes[route_idx];
                let Some(plan) = topology.plan_for(source, route.pipeline).filter(|_| matched) else {
                    mask.reclaim(&mut pool);
                    continue;
                };
                let docs = buffer::catch_over_budget(|| {
                    run_and_serialize(
                        plan,
                        Ingest {
                            records: slice,
                            raw: &raw,
                            meta: &meta,
                            source: Some(source_name),
                            now_micros,
                            tally: None,
                        },
                        mask,
                        &mut pool,
                        &mut errors,
                        &metrics,
                        Egress {
                            route,
                            id_fields: &id_fields,
                            dlq_of: &topology.dlq_of,
                            columns: None,
                            taps: &[],
                        },
                    )
                });
                let docs = match docs {
                    Ok(docs) => docs,
                    Err(over) => {
                        entry.refused = Some(over.to_string());
                        for (_, unrun) in masks {
                            unrun.reclaim(&mut pool);
                        }
                        return entry;
                    }
                };
                let slot_names = plan.slot_names();
                let mut exceptions: Vec<(&'static str, String)> = Vec::new();
                for a in &errors.entries()[docs.raised.clone()] {
                    let exception = (a.kind.label(), a.message(&slot_names));
                    if !exceptions.contains(&exception) {
                        exceptions.push(exception);
                    }
                }
                let mut answer = Answer::Ignore;
                for a in &docs.answers {
                    answer.join(a);
                }
                // A row no rule took was answered too, as its route answers `Unmatched`.
                let declined = docs.no_rule > 0
                    || docs
                        .blocking
                        .as_ref()
                        .is_some_and(|b| matches!(b.cause, Cause::Declined { .. }));
                if declined {
                    answer.join(
                        topology.routes[route_idx]
                            .on_exception
                            .answer(&ExceptionKind::Unmatched),
                    );
                }
                if let Some(blocked) = &docs.blocking {
                    // A block stops the whole batch, so no route's documents leave: not this one's, nor an earlier
                    // one's.
                    blocker.get_or_insert(row);
                    entry.through = vec![PreviewedRoute {
                        route: route_idx,
                        documents: Vec::new(),
                        exceptions,
                        answer,
                    }];
                    entry.refused = Some(match &blocked.cause {
                        Cause::Marked { pipeline, first } => format!(
                            "pipeline {pipeline} marked it with a kind its route's exceptions block. The first {first}"
                        ),
                        Cause::Declined { .. } => {
                            "no delivery rule of its route took it, and Unmatched is answered with block".to_string()
                        }
                    });
                    for (_, unrun) in masks {
                        unrun.reclaim(&mut pool);
                    }
                    return entry;
                }
                let sets = docs.sets;
                entry.through.push(PreviewedRoute {
                    route: route_idx,
                    exceptions,
                    answer,
                    documents: docs
                        .rows
                        .into_iter()
                        .map(|doc| PreviewedDocument {
                            document: String::from_utf8_lossy(&doc.bytes).into_owned(),
                            destinations: sets[doc.set as usize].clone(),
                            destination: Placement::ALL
                                .map(Placement::name)
                                .into_iter()
                                .zip(doc.placements)
                                .chain([(
                                    "id",
                                    doc.doc_id
                                        .or_else(|| doc.doc_id_by_dest.first().map(|(_, v)| v.clone())),
                                )])
                                .filter_map(|(name, value)| Some((name, value?)))
                                .collect(),
                        })
                        .collect(),
                });
            }
            let unmatched = topology.on_exception.answer(&ExceptionKind::Unmatched);
            if entry.matched.is_empty() && entry.refused.is_none() && *unmatched == Answer::Block {
                blocker.get_or_insert(row);
                entry.refused = Some("no route matched it, and Unmatched is answered with block".into());
            }
            entry
        })
        .collect();
    if let Some(b) = blocker {
        for entry in rows.iter_mut().filter(|e| e.row != b) {
            entry.through.clear();
            entry.refused.get_or_insert_with(|| {
                format!("held: record {b} blocks the source, and a block holds every record polled with it")
            });
        }
    }
    rows
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config_file;
    use crate::core::column::Scalar;
    use crate::core::error::{Exception, ExceptionKind};
    use crate::plan::output::{RowException, row_exceptions};

    #[test]
    fn a_batch_is_sliced_in_order_and_an_oversized_record_runs_alone() {
        let mut chunk = crate::io::RecordChunkBuilder::new();
        for (i, raw) in ["0123456789", "0123456789", "0123456789", "0123456789", "0123456789"]
            .iter()
            .enumerate()
        {
            chunk.push(raw.as_bytes(), i as i64, 0, Arc::from("t"), None);
        }
        let records = chunk.finish();
        assert_eq!(slices(&records, 25), [(0, 3), (3, 5)]);
        assert_eq!(slices(&records, 50), [(0, 5)]);
        assert_eq!(slices(&records, 5), [(0, 1), (1, 2), (2, 3), (3, 4), (4, 5)]);
        assert_eq!(slices(&[], 5), [(0, 0)]);

        let mut chunk = crate::io::RecordChunkBuilder::new();
        for (i, raw) in ["0123456789", ""].iter().enumerate() {
            chunk.push(raw.as_bytes(), i as i64, 0, Arc::from("t"), None);
        }
        assert_eq!(
            slices(&chunk.finish(), 5),
            [(0, 1), (1, 2)],
            "an empty record after the last cut still runs"
        );
    }

    fn ann(row: u32, field: Option<SlotId>, kind: ExceptionKind) -> Exception {
        Exception {
            row,
            function: 0,
            field,
            kind,
            read: Vec::new(),
            call: "",
        }
    }

    /// A placement or document id slot may hold one value for the whole batch: its text is the value on every row, and
    /// what a row of that type would give where the value is not text.
    #[test]
    fn read_text_reads_a_constant_as_its_text_in_every_row() {
        let mut pool = BufferPool::new();
        let layout = [
            DataType::Bool,
            DataType::Str,
            DataType::Str,
            DataType::Bytes,
            DataType::I64,
        ];
        let mut batch = Batch::new(&layout, 2, 0);
        batch.put(2, Column::constant(Scalar::Str("events".into()), 2), &mut pool);
        batch.put(3, Column::constant(Scalar::Bytes(b"raw".to_vec()), 2), &mut pool);
        batch.put(4, Column::constant(Scalar::I64(7), 2), &mut pool);
        for row in 0..2 {
            assert_eq!(read_text(&batch, 2, row).as_deref(), Some("events"));
            assert_eq!(
                read_text(&batch, 3, row).as_deref(),
                Some("cmF3"),
                "bytes read as their base64"
            );
            assert_eq!(read_text(&batch, 4, row), None, "a number is not text");
        }
        batch.reclaim(&mut pool);
    }

    fn names() -> HashMap<SlotId, String> {
        HashMap::from([(0, "status".to_string())])
    }

    fn raised(entries: &[Exception]) -> HashMap<u32, Vec<RowException>> {
        row_exceptions(entries, &names(), &["parse".to_string()])
    }

    fn exception(kind: &'static str, message: &str) -> RowException {
        RowException {
            step: Some("parse".to_string()),
            kind,
            message: message.to_string(),
        }
    }

    #[test]
    fn a_failure_is_listed_for_a_failure_and_a_note_alike() {
        let entries = vec![
            ann(0, Some(0), ExceptionKind::UnparsableField { to: DataType::I64 }),
            ann(1, None, ExceptionKind::GuessedEncoding { encoding: "utf-8" }),
        ];
        let raised = raised(&entries);
        assert!(raised.contains_key(&0));
        assert!(raised.contains_key(&1));
    }

    #[test]
    fn a_failure_names_its_step_and_its_field() {
        let entries = vec![ann(0, Some(0), ExceptionKind::UnparsableField { to: DataType::I64 })];
        assert_eq!(
            raised(&entries)[&0],
            vec![exception(
                "UnparsableField",
                "a value, which could not parse as i64 in status"
            )]
        );
    }

    #[test]
    fn a_failure_read_before_any_step_names_none() {
        let mut read = ann(0, Some(0), ExceptionKind::InvalidUtf8);
        read.function = crate::core::error::INGEST_FUNCTION;
        assert_eq!(raised(&[read])[&0][0].step, None);
    }

    #[test]
    fn row_failures_dedupes_and_keeps_distinct_failures() {
        let entries = vec![
            ann(0, None, ExceptionKind::DivideByZero),
            ann(0, None, ExceptionKind::DivideByZero),
            ann(0, Some(0), ExceptionKind::Unparsable),
        ];
        assert_eq!(
            raised(&entries)[&0],
            vec![
                exception("DivideByZero", "a value, which () divided by"),
                exception("Unparsable", "a value, which  could not parse in status"),
            ]
        );
    }

    /// `now()` puts the clock into a pipeline's output, so two runs agree only if both are given the same instant.
    /// `preview_routes_at` takes it as an argument rather than reading the clock, so pinning it makes a run
    /// repeatable.
    #[test]
    fn preview_routes_at_pinned_to_one_instant_repeats_the_same_answer() {
        let json = r#"{
          "version": 1,
          "sources": [
            { "id": "orders", "type": "kafka", "brokers": "b1:9092", "topics": ["orders"], "groupId": "smeltr" }
          ],
          "destinations": [
            { "id": "es-prod", "type": "elasticsearch", "url": "http://es:9200", "defaultIndex": "orders" }
          ],
          "pipelines": [
            { "id": "stamp", "name": "stamp", "steps": [
              { "id": "ts", "type": "eval", "assignments": [ { "target": "seenAt", "expr": "now()" } ] }
            ] }
          ],
          "routes": [
            { "id": "all", "source": "orders", "condition": null, "pipeline": "stamp",
              "deliver": [ { "to": ["es-prod"] } ] }
          ]
        }"#;
        let system = config_file::load_str(json).unwrap_or_else(|e| panic!("load: {e}"));
        let compiled = system.compile_topology().unwrap_or_else(|e| panic!("compile: {e:?}"));
        let source = system.topology.sources[0].id;
        let records = [SourceRecord {
            chunk: crate::io::RecordChunk::of_bytes(b"{}"),
            row: 0,
            offset: 0,
            partition: 0,
            topic: Arc::from("orders"),
            headers: None,
        }];

        let pinned = 1_700_000_000_000_000i64;
        let run = || {
            preview_routes_at(
                &compiled,
                &system.topology.destinations,
                source,
                "orders",
                &records,
                &["{}".to_string()],
                preview_memory_budget(),
                pinned,
            )
        };
        let doc = |rows: Vec<PreviewedRow>| -> String {
            let row = rows.into_iter().next().expect("one record in, one row out");
            let route = row.through.into_iter().next().expect("the route kept the record");
            route
                .documents
                .into_iter()
                .next()
                .expect("one document, no unroll")
                .document
        };
        let first = doc(run());
        let second = doc(run());

        assert!(first.contains("seenAt"), "{first}");
        assert_eq!(first, second, "one instant, given twice, produces the same document");
    }

    /// A block stops the whole batch, so a preview whose later route blocks lists no route's documents, the earlier
    /// route's included, and says why.
    #[test]
    fn a_preview_that_blocks_lists_no_documents() {
        let json = r#"{
          "version": 1,
          "sources": [
            { "id": "orders", "type": "kafka", "brokers": "b1:9092", "topics": ["orders"], "groupId": "smeltr" }
          ],
          "destinations": [
            { "id": "es-prod", "type": "elasticsearch", "url": "http://es:9200", "defaultIndex": "orders" }
          ],
          "pipelines": [
            { "id": "pass", "name": "pass", "steps": [] },
            { "id": "ratio", "name": "ratio", "steps": [
              { "id": "parse", "type": "parser", "input": "_raw", "kind": { "type": "json" },
                "declared": [ { "name": "status", "type": "i64" } ] },
              { "id": "div", "type": "eval", "assignments": [ { "target": "ratio", "expr": "100 / status" } ] }
            ] }
          ],
          "routes": [
            { "id": "first", "source": "orders", "condition": null, "pipeline": "pass",
              "deliver": [ { "to": ["es-prod"] } ] },
            { "id": "second", "source": "orders", "condition": null, "pipeline": "ratio",
              "deliver": [ { "to": ["es-prod"] } ], "exceptions": { "categories": { "math": "block" } } }
          ]
        }"#;
        let system = config_file::load_str(json).unwrap_or_else(|e| panic!("load: {e}"));
        let compiled = system.compile_topology().unwrap_or_else(|e| panic!("compile: {e:?}"));
        let source = system.topology.sources[0].id;
        let record = |raw: &[u8], offset: i64| SourceRecord {
            chunk: crate::io::RecordChunk::of_bytes(raw),
            row: 0,
            offset,
            partition: 0,
            topic: Arc::from("orders"),
            headers: None,
        };
        let records = [record(br#"{"status":0}"#, 0), record(br#"{"status":2}"#, 1)];

        let rows = preview_routes(
            &compiled,
            &system.topology.destinations,
            source,
            "orders",
            &records,
            preview_memory_budget(),
        );

        let row = &rows[0];
        assert_eq!(row.matched.len(), 2, "both routes took it");
        assert_eq!(row.through.len(), 1, "only the route that blocked");
        assert!(row.through[0].documents.is_empty(), "nothing leaves a blocked batch");
        assert_eq!(row.through[0].answer, Answer::Block);
        assert_eq!(row.through[0].exceptions[0].0, "DivideByZero");
        let why = row.refused.as_deref().expect("a blocked record says why");
        assert!(why.contains("exceptions block"), "{why}");
        assert!(why.contains("pipeline ratio"), "names the pipeline: {why}");
        assert!(why.contains("at step div"), "names the step: {why}");

        let healthy = &rows[1];
        assert!(healthy.through.is_empty(), "held with the record that blocks");
        let why = healthy.refused.as_deref().expect("a held record says why");
        assert!(why.contains("record 0 blocks the source"), "{why}");
    }

    /// A record no delivery rule takes shows its route's `Unmatched` answer: drop sends nothing, deliver sends the
    /// pipeline's output to the answer's targets, and block holds it and says why.
    #[test]
    fn a_record_no_rule_takes_shows_its_routes_unmatched_answer() {
        for (answer, unmatched) in [
            (Answer::Drop, r#""drop""#),
            (Answer::Deliver(vec![1]), r#"{ "deliver": ["dlq"] }"#),
            (Answer::Block, r#""block""#),
        ] {
            let json = format!(
                r#"{{
                  "version": 1,
                  "sources": [
                    {{ "id": "orders", "type": "kafka", "brokers": "b1:9092", "topics": ["orders"], "groupId": "smeltr" }}
                  ],
                  "destinations": [
                    {{ "id": "es-prod", "type": "elasticsearch", "url": "http://es:9200", "defaultIndex": "orders" }},
                    {{ "id": "dlq", "type": "elasticsearch", "url": "http://es:9200", "defaultIndex": "dlq" }}
                  ],
                  "pipelines": [ {{ "id": "pass", "name": "pass", "steps": [] }} ],
                  "routes": [
                    {{ "id": "errors", "source": "orders", "condition": null, "pipeline": "pass",
                      "deliver": [ {{ "when": "_raw == \"x\"", "to": ["es-prod"] }} ],
                      "exceptions": {{ "kinds": {{ "Unmatched": {unmatched} }} }} }}
                  ]
                }}"#
            );
            let system = config_file::load_str(&json).unwrap_or_else(|e| panic!("load: {e}"));
            let compiled = system.compile_topology().unwrap_or_else(|e| panic!("compile: {e:?}"));
            let source = system.topology.sources[0].id;
            let records = [SourceRecord {
                chunk: crate::io::RecordChunk::of_bytes(b"y"),
                row: 0,
                offset: 0,
                partition: 0,
                topic: Arc::from("orders"),
                headers: None,
            }];
            let rows = preview_routes(
                &compiled,
                &system.topology.destinations,
                source,
                "orders",
                &records,
                preview_memory_budget(),
            );
            let route = &rows[0].through[0];
            assert_eq!(route.answer, answer, "{unmatched}");
            match answer {
                Answer::Drop => assert!(route.documents.is_empty() && rows[0].refused.is_none()),
                Answer::Deliver(_) => assert_eq!(route.documents[0].destinations, vec![1]),
                _ => {
                    let why = rows[0].refused.as_deref().expect("a blocked record says why");
                    assert!(why.contains("no delivery rule"), "{why}");
                }
            }
        }
    }

    /// A source that declares its record's fields hands typed rows: the pipeline reads them by name, at their types,
    /// with no `_raw` and nothing parsed, and a value that was not its field's type is null and names the field.
    #[test]
    fn a_typed_record_runs_by_its_declared_fields() {
        let json = r#"{
          "version": 1,
          "sources": [
            { "id": "orders", "type": "kafka", "brokers": "b1:9092", "topics": ["orders"], "groupId": "smeltr" }
          ],
          "destinations": [
            { "id": "es-prod", "type": "elasticsearch", "url": "http://es:9200", "defaultIndex": "orders" }
          ],
          "pipelines": [
            { "id": "next", "name": "next", "steps": [
              { "id": "add", "type": "eval", "assignments": [ { "target": "next", "expr": "id + 1" } ] }
            ] }
          ],
          "routes": [
            { "id": "all", "source": "orders", "condition": "name != \"skip\"", "pipeline": "next",
              "deliver": [ { "to": ["es-prod"] } ] },
            { "id": "skipped", "source": "orders", "condition": "name == \"skip\"", "pipeline": "next",
              "deliver": [ { "to": ["es-prod"] } ] }
          ]
        }"#;
        let system = config_file::load_str(json).unwrap_or_else(|e| panic!("load: {e}"));
        let source = system.topology.sources[0].id;
        let meta = SourceMeta::KAFKA
            .declaring(vec![("id".into(), DataType::I64), ("name".into(), DataType::Str)])
            .unwrap();
        let metas = crate::plan::topology::SourceMetas::from([(source, meta.clone())]);
        let compiled = crate::plan::topology::compile_topology_with(
            &system.topology.routes,
            &system.pipelines,
            system.topology.on_exception.clone(),
            &metas,
        )
        .unwrap_or_else(|e| panic!("compile: {e:?}"));
        let objects = [
            serde_json::json!({"id": 41, "name": "a"}),
            serde_json::json!({"id": "forty", "name": "b"}),
            serde_json::json!({"id": 1, "name": "skip"}),
        ];
        let objects: Vec<crate::io::RecordObject> = objects.iter().map(|o| o.as_object().unwrap().clone()).collect();
        let fields = crate::io::SourceFields::new();
        let stored: Vec<_> = objects.iter().map(|o| (o, &fields)).collect();
        let records = meta.read_stored(&stored).unwrap();

        let rows = preview_routes(
            &compiled,
            &system.topology.destinations,
            source,
            "orders",
            &records,
            preview_memory_budget(),
        );

        let doc = |row: &PreviewedRow| -> serde_json::Value {
            serde_json::from_str(&row.through[0].documents[0].document).unwrap()
        };
        assert_eq!(doc(&rows[0]), serde_json::json!({"id": 41, "name": "a", "next": 42}));
        assert_eq!(
            doc(&rows[1]),
            serde_json::json!({"name": "b", "smeltr": {"exception": {
                "route": "all",
                "pipeline": "next",
                "exceptions": [{"kind": "UnparsableField", "message": "id is \"forty\", which could not parse as i64"}],
            }}}),
            "a value not its type is null and names the field"
        );
        assert_eq!(rows[2].matched, [1], "a route condition reads a declared field");
    }

    /// A row no pipeline made goes to a destination as its record object, and one field's text is that field's. A
    /// typed record has no `__source.bytes`, so it has no form to write as bytes.
    #[test]
    fn a_typed_record_no_pipeline_made_is_its_record_object() {
        let meta = SourceMeta::KAFKA
            .declaring(vec![("id".into(), DataType::I64), ("name".into(), DataType::Str)])
            .unwrap();
        let object = serde_json::json!({"id": 7, "name": "a"}).as_object().unwrap().clone();
        let fields = crate::io::SourceFields::new();
        let records = meta.read_stored(&[(&object, &fields)]).unwrap();
        let form =
            |payload: Payload| source_form(&payload, &meta, &records[0], "").map(|v| String::from_utf8(v).unwrap());

        assert_eq!(form(Payload::Record).as_deref(), Some(r#"{"id":7,"name":"a"}"#));
        assert_eq!(form(Payload::source_bytes()), None);
        assert_eq!(form(Payload::Text { field: "name".into() }).as_deref(), Some("a"));
        assert_eq!(
            form(Payload::Text { field: "_raw".into() }),
            None,
            "a typed record has no `_raw`"
        );

        let bytes = crate::io::SourceRecord::read(crate::io::RecordChunk::of_bytes(b"line"), 0, &fields);
        let raw = |payload: Payload| {
            source_form(&payload, &SourceMeta::KAFKA, &bytes, "line").map(|v| String::from_utf8(v).unwrap())
        };
        assert_eq!(raw(Payload::Record).as_deref(), Some(r#"{"_raw":"line"}"#));
        assert_eq!(raw(Payload::source_bytes()).as_deref(), Some("line"));
        assert_eq!(raw(Payload::Text { field: "_raw".into() }).as_deref(), Some("line"));
    }

    /// An unmatched record under block holds the rest of the sample as a pipeline block does.
    #[test]
    fn a_preview_with_an_unmatched_record_under_block_holds_every_record() {
        let json = r#"{
          "version": 1,
          "sources": [
            { "id": "orders", "type": "kafka", "brokers": "b1:9092", "topics": ["orders"], "groupId": "smeltr" }
          ],
          "destinations": [
            { "id": "es-prod", "type": "elasticsearch", "url": "http://es:9200", "defaultIndex": "orders" }
          ],
          "pipelines": [ { "id": "pass", "name": "pass", "steps": [] } ],
          "routes": [
            { "id": "first", "source": "orders", "condition": "__source.offset == 0", "pipeline": "pass",
              "deliver": [ { "to": ["es-prod"] } ] }
          ]
        }"#;
        let system = config_file::load_str(json).unwrap_or_else(|e| panic!("load: {e}"));
        let compiled = system.compile_topology().unwrap_or_else(|e| panic!("compile: {e:?}"));
        let source = system.topology.sources[0].id;
        let record = |offset: i64| SourceRecord {
            chunk: crate::io::RecordChunk::of_bytes(b"{}"),
            row: 0,
            offset,
            partition: 0,
            topic: Arc::from("orders"),
            headers: None,
        };

        let rows = preview_routes(
            &compiled,
            &system.topology.destinations,
            source,
            "orders",
            &[record(0), record(1)],
            preview_memory_budget(),
        );

        assert_eq!(rows[0].matched.len(), 1);
        assert!(rows[0].through.is_empty(), "held with the unmatched record");
        assert!(
            rows[0]
                .refused
                .as_deref()
                .unwrap()
                .contains("record 1 blocks the source")
        );
        assert!(rows[1].matched.is_empty());
        assert!(
            rows[1]
                .refused
                .as_deref()
                .unwrap()
                .contains("Unmatched is answered with block")
        );
    }

    /// A source that never has anything: enough of `Source` to build a bare `Reader`.
    struct NullSource;
    impl Source for NullSource {
        fn poll_batch(&mut self, _: usize, _: usize, _: Duration) -> Result<Vec<SourceRecord>, IoError> {
            Ok(Vec::new())
        }
        fn commit(&mut self, _: &CommitMap) -> Result<(), IoError> {
            Ok(())
        }
    }

    fn bare_reader(metrics: MetricsHandle) -> Reader {
        Reader::new(7, Box::new(NullSource), vec![], MAX_IN_FLIGHT, metrics)
    }

    /// A source whose `connection_error` is fixed at construction (E4), to exercise `Reader::connection_error_fact`
    /// without a real Kafka client -- the client-specific mechanics (`error()`, recovery) are `io::kafka`'s own
    /// tests; this one is only about the fact reaching a `Reader`, then a heartbeat, unchanged.
    struct FlakySource {
        error: Option<String>,
    }
    impl Source for FlakySource {
        fn poll_batch(&mut self, _: usize, _: usize, _: Duration) -> Result<Vec<SourceRecord>, IoError> {
            Ok(Vec::new())
        }
        fn commit(&mut self, _: &CommitMap) -> Result<(), IoError> {
            Ok(())
        }
        fn connection_error(&self) -> Option<String> {
            self.error.clone()
        }
    }

    /// A reader reports its row count from the start, so a source that has read nothing yet reads idle.
    #[test]
    fn a_new_reader_reports_its_rows_polled_at_zero() {
        let registry = Arc::new(crate::metrics::Metrics::new());
        let _reader = bare_reader(MetricsHandle::new(registry.clone(), "reader-7"));
        let read = registry.take_delta(&mut crate::metrics::Cursor::default());
        let polled = read
            .iter()
            .find(|s| s.key.name == "smeltr_rows_polled_total")
            .expect("rows_polled reported");
        assert!(polled.key.labels.contains(&("source", "7".to_string())));
        assert!(matches!(polled.value, crate::metrics::MetricValue::Counter(0)));
    }

    #[test]
    fn the_block_reason_names_the_destination_and_quotes_the_cluster() {
        let msg = block_reason("es-prod", "mapper_parsing_exception: failed to parse field [x]");
        assert!(msg.contains("destination es-prod"), "{msg}");
        assert!(msg.contains("mapper_parsing_exception"), "{msg}");
    }

    /// Cut to what a heartbeat carries, a block reason keeps both its fix and the whole of a long destination error,
    /// and still names its fix however long the cause it quotes.
    #[test]
    fn a_block_reason_keeps_its_fix_and_cause_when_cut_to_a_heartbeat_fact() {
        let mapping = format!(
            "mapper_parsing_exception: failed to parse field [event.payload.attributes.duration] of type [long] in \
             document with id 'Xk2-9a'. Preview of field's value: '{}'; caused by: illegal_argument_exception: For \
             input string",
            "1".repeat(150)
        );
        let refused = truncate(&block_reason("es-prod-cluster-west", &mapping), MAX_TEXT_LEN);
        assert!(refused.ends_with("input string"), "{refused}");
        let long = "x".repeat(MAX_TEXT_LEN * 2);
        let refused = truncate(&block_reason("es-prod-cluster-west", &long), MAX_TEXT_LEN);
        assert!(
            refused.contains("fix the destination's schema or the pipeline"),
            "{refused}"
        );
        let unprocessable = truncate(&unprocessable_block_reason(100_000, &long), MAX_TEXT_LEN);
        assert!(
            unprocessable.contains("answer Unprocessable with drop or deliver"),
            "{unprocessable}"
        );
    }

    /// A block reason rides every heartbeat once a source stays blocked, so it is bounded.
    #[test]
    fn a_holds_reason_is_truncated_like_any_other_heartbeat_fact() {
        let registry = Arc::new(crate::metrics::Metrics::new());
        let mut reader = bare_reader(MetricsHandle::new(registry, "reader-7"));
        reader.apply(vec![Report::Block(BlockCause::Held, "x".repeat(MAX_TEXT_LEN + 50))]);
        assert_eq!(reader.block_reason.as_ref().unwrap().chars().count(), MAX_TEXT_LEN + 1);
    }

    /// A held source stays held through a late refusal or a destination's wait: each clears sooner than the hold,
    /// and the reader hears only when the whole block lifts.
    #[test]
    fn a_lesser_cause_does_not_replace_a_held_readers_cause() {
        let registry = Arc::new(crate::metrics::Metrics::new());
        let mut reader = bare_reader(MetricsHandle::new(registry, "reader-7"));
        reader.apply(vec![Report::Block(BlockCause::Held, "matched no route".into())]);
        reader.apply(vec![Report::Block(
            BlockCause::Refused,
            "destination 3 refused a record".into(),
        )]);
        reader.apply(vec![Report::Block(
            BlockCause::Waiting,
            "destination 3 is unreachable".into(),
        )]);
        assert_eq!(reader.block_cause, Some(BlockCause::Held));
        assert_eq!(reader.block_reason.as_deref(), Some("matched no route"));
    }

    /// `enter_block` keeps the specific reason a destination gave, not a boilerplate line true of any destination:
    /// ordinary text a `HeartbeatRequest` already carries and round-trips, and clears once the source resumes.
    #[test]
    fn a_held_readers_reason_reaches_a_heartbeat_request() {
        let registry = Arc::new(crate::metrics::Metrics::new());
        let mut reader = bare_reader(MetricsHandle::new(registry, "reader-7"));
        assert!(reader.blocked_fact().is_none(), "not blocked yet");

        let why = "destination 3 refused a record: mapper_parsing_exception: failed to parse field [x]".to_string();
        reader.apply(vec![Report::Block(BlockCause::Refused, why.clone())]);
        assert_eq!(reader.block_reason.as_deref(), Some(why.as_str()));

        let fact = reader.blocked_fact().expect("blocked");
        assert!(
            fact.labels.contains(&("cause".to_string(), "refused".to_string())),
            "what clears it travels with why: {:?}",
            fact.labels
        );
        let heartbeat = crate::heartbeat::HeartbeatRequest {
            heartbeat_interval_ms: None,
            single_worker: Vec::new(),
            progress: Vec::new(),
            streams: None,
            streams_window: None,
            stopping: false,
            authority_fingerprints: Vec::new(),
            takes_authority: true,
            logs: Vec::new(),
            boot_id: 0,
            group_id: "prod".to_string(),
            secret_versions: Vec::new(),
            supervising: None,
            resources: None,
            worker_id: "w1".to_string(),
            stop_after_unreachable_secs: None,
            commit: None,
            smeltr_version: String::new(),
            release: None,
            authority_expires_millis: None,
            threads: vec![crate::heartbeat::ThreadHeartbeat {
                thread_id: "reader-7".to_string(),
                group_id: "g".to_string(),
                kind: Some(crate::heartbeat::ThreadKind::Reader),
                partitions_owned: vec![],
                metrics: vec![fact],
            }],
            accepts: Vec::new(),
            took: None,
        };

        let json = serde_json::to_string(&heartbeat).unwrap();
        assert!(json.contains("mapper_parsing_exception"), "{json}");
        let back: crate::heartbeat::HeartbeatRequest = serde_json::from_str(&json).unwrap();
        assert!(matches!(
            &back.threads[0].metrics[0].value,
            crate::heartbeat::MetricValueDto::Text(t) if t.contains("mapper_parsing_exception")
        ));

        reader.apply(vec![Report::Release]);
        assert!(reader.blocked_fact().is_none(), "cleared once the source resumed");
    }

    /// A source's own connectivity error (E4) reaches a heartbeat fact under its own metric name, distinct from
    /// `blocked_fact`: a source can be unreachable without any destination ever having refused it.
    #[test]
    fn a_sources_connection_error_reaches_a_heartbeat_fact() {
        let registry = Arc::new(crate::metrics::Metrics::new());
        let healthy = Reader::new(
            7,
            Box::new(FlakySource { error: None }),
            vec![],
            MAX_IN_FLIGHT,
            MetricsHandle::new(registry.clone(), "reader-7"),
        );
        assert!(
            healthy.connection_error_fact().is_none(),
            "a healthy source reports nothing"
        );

        let reason = "kafka-nowhere.invalid:9092/bootstrap: Failed to resolve".to_string();
        let unreachable = Reader::new(
            7,
            Box::new(FlakySource {
                error: Some(reason.clone()),
            }),
            vec![],
            MAX_IN_FLIGHT,
            MetricsHandle::new(registry, "reader-7"),
        );
        let fact = unreachable
            .connection_error_fact()
            .expect("unreachable source reports a fact");
        assert_eq!(fact.metric, "smeltr_source_connection_error");
        assert_eq!(fact.labels, vec![("source".to_string(), "7".to_string())]);
        assert!(matches!(
            &fact.value,
            crate::heartbeat::MetricValueDto::Text(t) if t == &reason
        ));
    }

    /// A source that cannot be read, until it can.
    struct DownSource {
        down: bool,
    }
    impl Source for DownSource {
        fn poll_batch(&mut self, _: usize, _: usize, _: Duration) -> Result<Vec<SourceRecord>, IoError> {
            if self.down {
                return Err(IoError::transient("broker unreachable"));
            }
            Ok(Vec::new())
        }
        fn commit(&mut self, _: &CommitMap) -> Result<(), IoError> {
            Ok(())
        }
    }

    /// Every failed poll counts, and a landed one ends the failure, so the next is logged at once.
    #[test]
    fn a_failed_poll_is_counted_and_a_landed_one_ends_it() {
        let registry = Arc::new(crate::metrics::Metrics::new());
        let mut reader = Reader::new(
            7,
            Box::new(DownSource { down: true }),
            vec![],
            MAX_IN_FLIGHT,
            MetricsHandle::new(registry.clone(), "reader-7"),
        );
        let (_reports, acks) = crate::handoff::reports();
        let batches = Handoff::new();
        let step = |reader: &mut Reader| reader.step(&batches, &acks, 10, 1 << 20, Duration::ZERO);
        assert!(step(&mut reader).is_err());
        assert!(step(&mut reader).is_err());
        assert!(
            reader.poll_failing.is_some(),
            "a failing source is remembered as failing"
        );
        let failures = |registry: &crate::metrics::Metrics| {
            registry
                .snapshot()
                .into_iter()
                .find(|s| s.key.name == "smeltr_source_poll_failures_total")
                .map(|s| match s.value {
                    crate::metrics::MetricValue::Counter(n) => n,
                    other => panic!("a counter, not {other:?}"),
                })
        };
        assert_eq!(failures(&registry), Some(2));

        reader.source = Box::new(DownSource { down: false });
        assert!(step(&mut reader).is_ok());
        assert!(reader.poll_failing.is_none(), "a landed poll ends the failure");
        assert_eq!(failures(&registry), Some(2));
    }

    /// The lane's gauge counts what its queue holds and the batch the queue refused, which waits on the reader.
    #[test]
    fn a_lane_reports_its_queue_and_the_batch_it_refused() {
        let record = |offset: i64| SourceRecord {
            chunk: crate::io::RecordChunk::of_bytes(b"{\"a\":1}"),
            row: 0,
            offset,
            partition: 0,
            topic: Arc::from("orders"),
            headers: None,
        };
        let size = record(0).size();
        let registry = Arc::new(crate::metrics::Metrics::new());
        let mut reader = Reader::new(
            7,
            Box::new(crate::io::fake::FakeSource::new((0..3).map(record))),
            vec![],
            MAX_IN_FLIGHT,
            MetricsHandle::new(registry.clone(), "reader-7"),
        );
        let lane = |registry: &crate::metrics::Metrics| {
            registry
                .snapshot()
                .into_iter()
                .find(|s| s.key.name == "smeltr_source_lane_bytes")
                .map(|s| match s.value {
                    crate::metrics::MetricValue::Gauge(g) => g as usize,
                    other => panic!("a gauge, not {other:?}"),
                })
        };
        let (_reports, acks) = crate::handoff::reports();
        // Room for one batch: the second is refused and kept.
        let batches = Handoff::watched(Arc::new(crate::handoff::Ready::new()), size);
        let step = |reader: &mut Reader| reader.step(&batches, &acks, 1, 1 << 20, Duration::ZERO).unwrap();
        step(&mut reader);
        assert_eq!(
            lane(&registry),
            Some(0),
            "written before the first batch is handed over"
        );
        assert!(matches!(step(&mut reader), Polled::Full));
        step(&mut reader);
        assert_eq!(lane(&registry), Some(2 * size));
        let crate::handoff::Taken::Batch(_) = batches.pop(Duration::ZERO) else {
            panic!("the first batch is queued")
        };
        step(&mut reader);
        assert_eq!(lane(&registry), Some(size), "the queue gave up one batch");
    }

    /// A source that finishes three messages on every step, and reads nothing.
    struct FinishingSource;
    impl Source for FinishingSource {
        fn poll_batch(&mut self, _: usize, _: usize, _: Duration) -> Result<Vec<SourceRecord>, IoError> {
            Ok(Vec::new())
        }
        fn commit(&mut self, _: &CommitMap) -> Result<(), IoError> {
            Ok(())
        }
        fn counts_messages(&self) -> bool {
            true
        }
        fn take_finished(&mut self) -> u64 {
            3
        }
    }

    #[test]
    fn messages_finished_are_counted_for_the_source() {
        let registry = Arc::new(crate::metrics::Metrics::new());
        let mut reader = Reader::new(
            7,
            Box::new(FinishingSource),
            vec![],
            MAX_IN_FLIGHT,
            MetricsHandle::new(registry.clone(), "reader-7"),
        );
        let finished = |registry: &crate::metrics::Metrics| {
            registry
                .snapshot()
                .into_iter()
                .find(|s| s.key.name == "smeltr_source_messages_finished_total")
                .expect("counted")
        };
        assert!(
            matches!(finished(&registry).value, crate::metrics::MetricValue::Counter(0)),
            "reported before the first message is finished"
        );
        let (_reports, acks) = crate::handoff::reports();
        let batches = Handoff::new();
        for _ in 0..2 {
            reader.step(&batches, &acks, 10, 1 << 20, Duration::ZERO).unwrap();
        }
        let finished = finished(&registry);
        assert!(matches!(finished.value, crate::metrics::MetricValue::Counter(6)));
        assert!(finished.key.labels.iter().any(|(k, v)| &**k == "source" && &**v == "7"));
    }

    #[test]
    fn the_refusal_ring_drops_the_oldest_entry_past_its_cap() {
        let ring = RefusalRing::default();
        for i in 0..REFUSAL_RING_CAP + 5 {
            ring.record(1, &format!("refusal {i}"));
        }
        let kept = ring.snapshot(1);
        assert_eq!(kept.len(), REFUSAL_RING_CAP, "capped, not one entry per call");
        assert_eq!(
            kept.first().unwrap().message,
            "refusal 5",
            "the oldest five were dropped"
        );
        assert_eq!(
            kept.last().unwrap().message,
            format!("refusal {}", REFUSAL_RING_CAP + 4)
        );
    }

    #[test]
    fn the_refusal_ring_truncates_an_overlong_message() {
        let ring = RefusalRing::default();
        ring.record(1, &"x".repeat(MAX_TEXT_LEN + 50));
        let kept = ring.snapshot(1);
        assert_eq!(
            kept[0].message.chars().count(),
            MAX_TEXT_LEN + 1,
            "the cap plus the mark that it was cut"
        );
        assert!(kept[0].message.ends_with('…'), "{}", kept[0].message);
    }

    #[test]
    fn a_destination_with_no_refusals_has_an_empty_ring() {
        let ring = RefusalRing::default();
        assert!(ring.snapshot(9).is_empty());
        assert!(ring.heartbeat_facts(9).is_empty());
    }

    /// A batch can refuse thousands of rows at once for the same underlying cause; `smeltr_rows_rejected_total` already
    /// counts every one of them, so the ring keeps one exemplar per call instead of paying for each row.
    #[test]
    fn report_rejections_keeps_one_ring_entry_per_call_however_many_rows() {
        let registry = Arc::new(crate::metrics::Metrics::new());
        let metrics = MetricsHandle::new(registry, "t0");
        let ring = RefusalRing::default();
        let rows: Vec<(usize, String)> = (0..500).map(|i| (i, "same cause every time".to_string())).collect();

        report_rejections(&metrics, &ring, 0, 2, &rows);

        let kept = ring.snapshot(2);
        assert_eq!(kept.len(), 1, "one call, one exemplar");
        assert_eq!(kept[0].message, "same cause every time");
    }
}
