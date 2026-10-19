//! The egress stage: one thread per destination, owning that destination's client and draining a bounded queue every
//! reader pushes to. Readers reach it through [`EgressHandle`], itself a [`Destination`], so transform's deliver/poll
//! cycle does not know where the work runs.
//!
//! Retry belongs here. A unit is kept until its batch is acknowledged and re-sent from here on a delivery failure; a
//! reader carries offsets and nothing else, and feels an outage only through the queue filling.
//!
//! A full queue never holds the thread offering to it. The offer is refused at once, the batch stays parked on its
//! lane, and the transform pool goes on serving every other lane; the queue wakes the pool when room returns. A
//! destination that stops draining therefore stops the sources feeding it and nothing else.
//!
//! This thread keeps its own id space for the client it owns and maps each completion back to the `(reader, batch)` it
//! came from: the reader half is which return channel answers, the batch half is what that reader tracked.
//!
//! A destination set to block keeps what it refuses here too, and sends it again on a clock of its own: the fix for a
//! refusal is usually on the destination's side, which tells nobody when it is done. Each refusal goes back with its
//! document, so a reader whose destination sends refusals to a DLQ has what to send.

use std::borrow::Cow;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, channel};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::core::id::DestId;
use crate::handoff::Ready;
use crate::io::{BatchCompletion, BatchId, Destination, Dispatched, HeldRefusal, IoError, OwnedRecord, Rejection};
use crate::metrics::MetricsHandle;
use crate::worker::capture::{Captures, HeldAt};

/// Which reader a unit came from, and where its completion goes.
type ReaderId = u64;

/// A batch sent and not yet acknowledged: refusals seen so far, and the rows, kept for a re-send.
struct Owed {
    rejections: Vec<Rejection>,
    unit: Unit,
}

/// One reader's batch, as far as one destination is concerned. Owned rows: they cross a thread boundary.
struct Unit {
    /// Acceptance order across every reader; a re-send keeps it.
    seq: u64,
    reader: ReaderId,
    batch: BatchId,
    rows: Vec<OwnedRecord>,
    bytes: usize,
    /// Set on the rows of a batch this destination refused and keeps, being sent again. Boxed: a unit is offered by
    /// value, and most units are never held.
    held: Option<Box<Held>>,
}

/// What is known of a batch whose refusals are being sent again.
struct Held {
    /// The wait before this attempt; the next one waits twice as long, up to `HELD_RETRY_MAX`.
    backoff: Duration,
    /// Refusals that are not held -- ones whose document the transport kept none of -- reported with the batch once it
    /// completes.
    carried: Vec<Rejection>,
    /// Why the destination refused each of the unit's rows, in row order.
    why: Vec<String>,
    /// The capture admissions the rows have been shown to: a capture at this destination sees them once.
    shown: HashSet<u64>,
}

/// The first wait before refused rows are sent again, and the longest. A refusal is fixed by a person, so this is paced
/// for one: a minute between attempts once it has been refused a while.
const HELD_RETRY_MIN: Duration = Duration::from_secs(1);
const HELD_RETRY_MAX: Duration = Duration::from_secs(60);

/// What the egress thread tells a reader.
enum Report {
    Done(BatchCompletion),
    /// The destination refused some of the batch and keeps those rows to send again; the batch is not done.
    Held(HeldRefusal),
    /// Nothing outstanding will be delivered: the stage is stopping.
    Failed(String),
}

#[derive(Default)]
struct QueueState {
    units: VecDeque<Unit>,
    /// Queued plus sent-but-unacknowledged. Taking a unit off the queue does not free it; acknowledging the batch does.
    bytes: usize,
    /// What of `bytes` is above the reserve, and so borrowed from the [`QueuePool`].
    borrowed: usize,
    next_seq: u64,
    closed: bool,
}

struct Queue {
    state: Mutex<QueueState>,
    /// Bytes this destination may hold -- queued, and sent but unacknowledged -- before an offer is refused. Shared
    /// across readers.
    budget: usize,
    /// Signaled when a unit arrives, or on close.
    work: Condvar,
    /// The transform pool's wake-up, signaled when bytes are freed: a lane parked on this queue is the pool's to run
    /// again, and nothing else tells it room came back.
    pool: Mutex<Option<Arc<Ready>>>,
    /// The destination is set to block: it keeps what it refuses. See [`Egress::holding_refusals`].
    holds_refusals: AtomicBool,
    /// Where bytes above a reserve come from, when this queue borrows. See [`Egress::borrowing`].
    lend: OnceLock<Lend>,
    /// The captures told this destination is sent to here. See [`Egress::with_captures`].
    captures: OnceLock<Arc<crate::worker::capture::Captures>>,
}

/// The bytes every borrowing queue of one deployment may hold above its reserve, together. A deployment has two: one
/// its destination queues borrow from, and one its receivers do (`io::receiver::Holdings`).
///
/// A queue holds its reserve whatever the others do, so each can always move. Above it, it takes from here what its
/// own budget still allows, and gives back as the destination acknowledges. A stalled destination holds at most its
/// budget, so the rest keep what remains.
pub struct QueuePool {
    limit: usize,
    lent: AtomicUsize,
}

impl QueuePool {
    pub fn new(limit: usize) -> Arc<QueuePool> {
        Arc::new(QueuePool {
            limit,
            lent: AtomicUsize::new(0),
        })
    }

    /// The most it lends.
    pub fn limit(&self) -> usize {
        self.limit
    }

    /// Bytes lent now.
    pub fn lent(&self) -> usize {
        self.lent.load(Ordering::Relaxed)
    }

    pub(crate) fn fits(&self, bytes: usize) -> bool {
        self.lent() + bytes <= self.limit
    }

    /// Lends `bytes` if they fit, or anyway when `forced`.
    pub(crate) fn take(&self, bytes: usize, forced: bool) -> bool {
        let mut lent = self.lent();
        loop {
            if !forced && lent + bytes > self.limit {
                return false;
            }
            match self
                .lent
                .compare_exchange_weak(lent, lent + bytes, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => return true,
                Err(now) => lent = now,
            }
        }
    }

    pub(crate) fn give(&self, bytes: usize) {
        self.lent.fetch_sub(bytes, Ordering::Relaxed);
    }
}

/// How one queue borrows: from which pool, above how many bytes of its own, and how many bytes of memory each byte it
/// holds stands for.
struct Lend {
    pool: Arc<QueuePool>,
    reserve: usize,
    charge: usize,
}

impl Lend {
    /// The pool bytes a queue holding `bytes` borrows.
    fn borrowed(&self, bytes: usize) -> usize {
        bytes.saturating_sub(self.reserve) * self.charge
    }
}

/// Why an offer was not taken. A refused unit comes back with its rows, which are the caller's to drop.
enum Refused {
    NoRoom(Unit),
    Closed,
}

impl Queue {
    fn new(budget: usize) -> Self {
        Queue {
            state: Mutex::new(QueueState::default()),
            budget,
            work: Condvar::new(),
            pool: Mutex::new(None),
            holds_refusals: AtomicBool::new(false),
            lend: OnceLock::new(),
            captures: OnceLock::new(),
        }
    }

    /// Whether `bytes` more would fit. An empty queue takes anything: a unit larger than the whole budget goes in
    /// alone, borrowing what it must, and the cap stops a second joining it. A closed queue answers yes so the offer
    /// reaches the refusal that says so.
    fn has_room(&self, bytes: usize) -> bool {
        let st = self.state.lock().unwrap();
        st.closed
            || st.bytes == 0
            || (st.bytes + bytes <= self.budget
                && self
                    .lend
                    .get()
                    .is_none_or(|l| l.pool.fits(l.borrowed(st.bytes + bytes) - st.borrowed)))
    }

    /// What this destination is holding against [`Queue::budget`]: queued, plus sent and not yet acknowledged. The
    /// number `has_room` decides against, which is what makes it the one worth reporting.
    fn held(&self) -> usize {
        self.state.lock().unwrap().bytes
    }

    /// Queue `unit` if it fits, now. Never waits: the caller holds a transform thread, and a queue that is full is
    /// full for as long as the destination is slow, which is not this thread's to wait out.
    fn offer(&self, unit: Unit) -> Result<(), Refused> {
        let mut st = self.state.lock().unwrap();
        if st.closed {
            return Err(Refused::Closed);
        }
        if st.bytes > 0 && st.bytes + unit.bytes > self.budget {
            return Err(Refused::NoRoom(unit));
        }
        if let Some(lend) = self.lend.get() {
            let borrowed = lend.borrowed(st.bytes + unit.bytes);
            if !lend.pool.take(borrowed - st.borrowed, st.bytes == 0) {
                return Err(Refused::NoRoom(unit));
            }
            st.borrowed = borrowed;
        }
        st.bytes += unit.bytes;
        let mut unit = unit;
        unit.seq = st.next_seq;
        st.next_seq += 1;
        st.units.push_back(unit);
        drop(st);
        self.work.notify_one();
        Ok(())
    }

    /// Next unit, waiting at most `timeout` for one.
    fn pop(&self, timeout: Duration) -> Popped {
        let mut st = self.state.lock().unwrap();
        if let Some(u) = st.units.pop_front() {
            return Popped::Work(u);
        }
        if st.closed {
            return Popped::Closed;
        }
        let (mut st, _) = self.work.wait_timeout(st, timeout).unwrap();
        if let Some(u) = st.units.pop_front() {
            return Popped::Work(u);
        }
        if st.closed {
            return Popped::Closed;
        }
        Popped::Idle
    }

    /// A retry backoff, cut short by shutdown.
    fn sleep_unless_closed(&self, d: Duration) {
        let st = self.state.lock().unwrap();
        if st.closed {
            return;
        }
        let _ = self.work.wait_timeout(st, d).unwrap();
    }

    /// Give budget back: on acknowledgment, never merely on send. Every waiting pool thread is woken, since more than
    /// one lane may be parked on this queue and each has to look for itself.
    fn release(&self, bytes: usize) {
        let mut st = self.state.lock().unwrap();
        st.bytes = st.bytes.saturating_sub(bytes);
        if let Some(lend) = self.lend.get() {
            let borrowed = lend.borrowed(st.bytes);
            lend.pool.give(st.borrowed - borrowed);
            st.borrowed = borrowed;
        }
        drop(st);
        if let Some(pool) = self.pool.lock().unwrap().as_ref() {
            pool.signal_all();
        }
    }

    fn is_closed(&self) -> bool {
        self.state.lock().unwrap().closed
    }

    fn close(&self) {
        self.state.lock().unwrap().closed = true;
        self.work.notify_all();
        if let Some(pool) = self.pool.lock().unwrap().as_ref() {
            pool.signal_all();
        }
    }
}

enum Popped {
    Work(Unit),
    Idle,
    Closed,
}

/// How long the thread waits for work before polling the destination anyway. `poll_completions` is what drives an
/// out-of-band acknowledgment (librdkafka's delivery callbacks fire from `producer.poll()`), so an idle destination
/// still has to be polled or its last batches never commit.
const POLL_INTERVAL: Duration = Duration::from_millis(5);

/// Return channels, one per reader.
type Returns = Mutex<HashMap<ReaderId, Sender<Report>>>;

/// One destination's egress thread and the queue feeding it.
pub struct Egress {
    id: DestId,
    queue: Arc<Queue>,
    returns: Arc<Returns>,
    next_reader: ReaderId,
    stall_after: Duration,
    /// The author's name for the destination.
    name: String,
    thread: Option<JoinHandle<()>>,
}

impl Egress {
    /// Start the thread that owns `destination`.
    /// `name` is the author's name for it, which its logs carry: `id` is an index no reader of a log knows.
    pub fn start(
        id: DestId,
        name: &str,
        destination: Box<dyn Destination>,
        stall_after: Duration,
        queue_bytes: usize,
        metrics: MetricsHandle,
    ) -> Egress {
        let queue = Arc::new(Queue::new(queue_bytes));
        let returns: Arc<Returns> = Arc::new(Mutex::new(HashMap::new()));
        // Here rather than on the thread: the bound is known before anything is spawned, so a destination whose
        // thread has not been scheduled yet still states what it may hold.
        note_queue_budget(&metrics, id, &queue);
        // Started at 0, so a destination that has delivered nothing yet reads idle rather than unreported.
        let label = id.to_string();
        metrics.start("smeltr_rows_delivered_total", &[("destination", label.as_str())]);
        metrics.start("smeltr_bytes_delivered_total", &[("destination", label.as_str())]);
        let thread = {
            let queue = queue.clone();
            let returns = returns.clone();
            let name = name.to_string();
            std::thread::Builder::new()
                .name(format!("egress-{id}"))
                .spawn(move || run(destination, &queue, &returns, &metrics, id, &name))
                .expect("spawning an egress thread")
        };
        Egress {
            id,
            queue,
            returns,
            next_reader: 0,
            stall_after,
            name: name.to_string(),
            thread: Some(thread),
        }
    }

    /// Tell `captures` this destination is sent to here, until this is dropped: the transform threads copy what
    /// they finish for it into any capture opened there, and the egress thread the rows it refused and keeps.
    pub fn with_captures(self, captures: Arc<crate::worker::capture::Captures>) -> Egress {
        captures.send_to(&self.name);
        let _ = self.queue.captures.set(captures);
        self
    }

    /// Wake `pool` whenever this queue frees room, so a lane parked on it is run again without polling.
    pub fn with_pool(self, pool: Arc<Ready>) -> Egress {
        *self.queue.pool.lock().unwrap() = Some(pool);
        self
    }

    /// Hold `reserve` bytes of this queue's own and borrow above them from `pool`, each byte held counting as `charge`
    /// bytes of it: what the destination holds again beside the queue, such as a Kafka producer's copy. Every queue
    /// borrowing from one pool must wake the same transform pool ([`Egress::with_pool`]): room one gives back may be
    /// what another's lane waits on.
    pub fn borrowing(self, pool: Arc<QueuePool>, reserve: usize, charge: usize) -> Egress {
        let _ = self.queue.lend.set(Lend {
            pool,
            reserve,
            charge: charge.max(1),
        });
        self
    }

    /// Keep what the destination refuses and send it again, completing a batch only once it lands: the destination is
    /// set to block. Otherwise a refusal goes back with the batch's completion, for the reader's side to answer.
    pub fn holding_refusals(self) -> Egress {
        self.queue.holds_refusals.store(true, Ordering::Relaxed);
        self
    }

    /// A handle for one reader, with its own return channel.
    pub fn handle(&mut self) -> EgressHandle {
        let reader = self.next_reader;
        self.next_reader += 1;
        let (tx, rx) = channel();
        self.returns.lock().unwrap().insert(reader, tx);
        EgressHandle {
            reader,
            queue: self.queue.clone(),
            reports: rx,
            ready: Vec::new(),
            held: Vec::new(),
            outstanding: 0,
            stall_after: self.stall_after,
        }
    }

    pub fn id(&self) -> DestId {
        self.id
    }
}

impl Drop for Egress {
    fn drop(&mut self) {
        if let Some(captures) = self.queue.captures.get() {
            captures.stop_sending_to(&self.name);
        }
        self.queue.close();
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

/// Backoff bounds between re-sends of a unit the destination would not take: short enough to clear a blip, capped so an
/// outage settles into a slow retry.
const RETRY_MIN: Duration = Duration::from_millis(50);
const RETRY_MAX: Duration = Duration::from_secs(5);

/// Whether a stall is due its log: it has none yet, or its last is [`crate::logs::RELOG_EVERY`] old, so the error
/// the leader tells on Destination unreachable stands for the whole outage.
fn stall_log_due(stalled: Option<Instant>) -> bool {
    stalled.is_none_or(|at| at.elapsed() >= crate::logs::RELOG_EVERY)
}

/// The egress thread. Owns the client for its whole life.
fn run(
    mut destination: Box<dyn Destination>,
    queue: &Queue,
    returns: &Returns,
    metrics: &MetricsHandle,
    id: DestId,
    name: &str,
) {
    // The destination's own batch ids, mapped back to the reader's.
    let mut owner: HashMap<BatchId, Owed> = HashMap::new();
    let mut next_local: BatchId = 0;
    // Units to send again, oldest first.
    let mut redo: VecDeque<Unit> = VecDeque::new();
    // Refused rows kept to send again, each with when.
    let mut waiting: Vec<(Instant, Unit)> = Vec::new();
    let mut backoff = RETRY_MIN;
    // When the stall was last logged; `None` while the destination is healthy.
    let mut stalled: Option<Instant> = None;
    // Whether the client's own report of no reachable broker is what last marked this destination down. A failed send
    // takes the stall over, so only a landed send clears it, not a broker coming back.
    let mut cut_off = false;
    // Written on change, not every pass: the loop turns on POLL_INTERVAL whether or not anything moved.
    let mut reported: Option<usize> = None;
    // The same discipline for the queue's fill, which moves far more often than anything reads it.
    let mut held_reported: Option<usize> = None;
    let mut held_reported_at = Instant::now();
    // The rows refused and kept, written on change.
    let mut kept_reported: Option<u64> = None;
    let mut open_reported: Option<usize> = None;
    // Held while any are kept, so a capture at this destination waits to be shown them.
    let mut holding: Option<crate::worker::capture::Holding> = None;

    loop {
        // Here, not only where the queue is polled: a unit that keeps failing is re-sent forever without asking for new
        // work.
        if queue.is_closed() {
            break;
        }

        // At the top, so the passes that `continue` on a delivery failure report it too. Those are the passes a full
        // queue is most worth seeing.
        note_queue(metrics, id, queue, &mut held_reported, &mut held_reported_at);
        // A re-send before anything new: it is older.
        let now = Instant::now();
        let next = match redo.pop_front() {
            Some(u) => Some(u),
            None => match waiting.iter().position(|(at, _)| *at <= now) {
                Some(i) => Some(waiting.swap_remove(i).1),
                None => match queue.pop(POLL_INTERVAL) {
                    Popped::Closed => break,
                    Popped::Idle => None,
                    Popped::Work(u) => Some(u),
                },
            },
        };

        if let Some(unit) = next {
            let local = next_local;
            next_local += 1;
            // Borrowed: the unit is kept until acknowledged, to send again or to hand a refused row back.
            match destination.deliver(local, Cow::Borrowed(&unit.rows)) {
                Err(e) => {
                    note_stall(metrics, id, name, &e, &mut stalled);
                    cut_off = false;
                    // Everything sent and unacknowledged is suspect too.
                    take_back(&mut owner, &mut redo, unit);
                    queue.sleep_unless_closed(backoff);
                    backoff = (backoff * 2).min(RETRY_MAX);
                    continue;
                }
                // Nothing will report it later, so report it now.
                Ok((Dispatched::Complete, rejections)) => {
                    clear_stall(metrics, id, name, &mut stalled, &mut backoff);
                    finish(queue, returns, metrics, id, unit, rejections, &mut waiting);
                }
                // Refusals seen at hand-over wait for the acknowledgment, so one completion carries every refusal
                // against a batch. Handed to the transport is not delivered: `Dispatched::InFlight` means only that, so
                // healthy is not reported here. A transport that will not talk to its destination at all still hands
                // every batch off this way -- `poll_completions` is what tells the two apart, once the batch either
                // lands or times out.
                Ok((Dispatched::InFlight, rejections)) => {
                    owner.insert(local, Owed { rejections, unit });
                }
                // A transport's own queue is not this queue: a client that will not take a batch is a delivery
                // failure here, retried like any other.
                Ok((Dispatched::NoRoom { .. }, _)) => {
                    let e = IoError::transient("destination client would not take the batch");
                    note_stall(metrics, id, name, &e, &mut stalled);
                    cut_off = false;
                    take_back(&mut owner, &mut redo, unit);
                    queue.sleep_unless_closed(backoff);
                    backoff = (backoff * 2).min(RETRY_MAX);
                    continue;
                }
            }
        }

        // Every pass, idle or not -- see `POLL_INTERVAL`.
        match destination.poll_completions() {
            Ok(done) => {
                for c in done {
                    let Some(mut owed) = owner.remove(&c.batch) else {
                        continue;
                    };
                    owed.rejections.extend(c.rejections);
                    clear_stall(metrics, id, name, &mut stalled, &mut backoff);
                    finish(queue, returns, metrics, id, owed.unit, owed.rejections, &mut waiting);
                }
            }
            Err(e) => {
                note_stall(metrics, id, name, &e, &mut stalled);
                cut_off = false;
                take_back_all(&mut owner, &mut redo);
                queue.sleep_unless_closed(backoff);
                backoff = (backoff * 2).min(RETRY_MAX);
            }
        }

        note_cut_off(
            metrics,
            id,
            name,
            destination.connection_error(),
            &mut cut_off,
            &mut stalled,
            &mut backoff,
        );
        note_concurrency(metrics, id, destination.concurrency(), &mut reported);
        note_open(metrics, id, destination.open_bytes(), &mut open_reported);
        if queue.holds_refusals.load(Ordering::Relaxed) {
            note_kept(metrics, id, &owner, &redo, &waiting, &mut kept_reported);
        }
        if let Some(captures) = queue.captures.get() {
            let keeps = kept_reported.is_some_and(|kept| kept > 0);
            // Taken before the rows are shown, so a capture admitted meanwhile waits for them rather than reporting.
            if keeps && holding.is_none() {
                holding = Some(captures.holding(HeldAt::Destination(name.to_string())));
            }
            if keeps {
                let units = waiting
                    .iter_mut()
                    .map(|(_, u)| u)
                    .chain(owner.values_mut().map(|o| &mut o.unit))
                    .chain(redo.iter_mut());
                show_held(captures, name, units);
            } else {
                holding = None;
            }
        }
    }

    // Rows nobody will deliver. None of it was committed, so it is redelivered when the deployment comes back.
    redo.extend(waiting.into_iter().map(|(_, u)| u));
    abandon(returns, &mut owner, &mut redo);
}

/// A unit the destination has answered for. Each refusal gets its document back. A destination set to block keeps the
/// rows it refused and sends them again after a wait, reporting the batch only once they land; any other refusal goes
/// back with the batch's completion.
fn finish(
    queue: &Queue,
    returns: &Returns,
    metrics: &MetricsHandle,
    id: DestId,
    mut unit: Unit,
    mut rejections: Vec<Rejection>,
    waiting: &mut Vec<(Instant, Unit)>,
) {
    // Refused, not delivered: a rejection is the destination answering, not the row arriving.
    let mut refused = vec![false; unit.rows.len()];
    for r in &rejections {
        if let Some(at) = refused.get_mut(r.doc) {
            *at = true;
        }
    }
    let bytes: usize = unit
        .rows
        .iter()
        .zip(&refused)
        .filter(|(_, r)| !**r)
        .map(|(row, _)| row.sent_bytes())
        .sum();
    note_delivered(
        metrics,
        id,
        unit.rows.len().saturating_sub(rejections.len()) as u64,
        bytes as u64,
    );
    for r in &mut rejections {
        if r.record.is_none() {
            r.record = unit.rows.get_mut(r.doc).map(std::mem::take);
        }
    }
    let (backoff, mut carried, shown) = match unit.held.take() {
        Some(h) => ((h.backoff * 2).min(HELD_RETRY_MAX), h.carried, h.shown),
        None => (HELD_RETRY_MIN, Vec::new(), HashSet::new()),
    };
    let holds = queue.holds_refusals.load(Ordering::Relaxed);
    let (kept, passed): (Vec<Rejection>, Vec<Rejection>) =
        rejections.into_iter().partition(|r| holds && r.record.is_some());
    carried.extend(passed);
    if kept.is_empty() {
        report(
            returns,
            unit.reader,
            Report::Done(BatchCompletion {
                batch: unit.batch,
                rejections: carried,
            }),
        );
        queue.release(unit.bytes);
        return;
    }
    let mut rows = Vec::with_capacity(kept.len());
    let mut refusals = Vec::with_capacity(kept.len());
    let mut why = Vec::with_capacity(kept.len());
    for mut r in kept {
        rows.extend(r.record.take());
        why.push(r.reason.clone());
        refusals.push(r);
    }
    let bytes = crate::io::held_bytes(&rows);
    // The rest of the batch landed; what is kept still counts against the queue until it does too.
    queue.release(unit.bytes.saturating_sub(bytes));
    report(
        returns,
        unit.reader,
        Report::Held(HeldRefusal {
            batch: unit.batch,
            rejections: refusals,
        }),
    );
    waiting.push((
        Instant::now() + backoff,
        Unit {
            seq: unit.seq,
            reader: unit.reader,
            batch: unit.batch,
            rows,
            bytes,
            held: Some(Box::new(Held {
                backoff,
                carried,
                why,
                shown,
            })),
        },
    ));
}

/// Show each capture at this destination the rows it refused and keeps, once per capture: waiting, or being sent
/// again.
fn show_held<'a>(captures: &Captures, name: &str, units: impl Iterator<Item = &'a mut Unit>) {
    for unit in units {
        if let Some(held) = &mut unit.held {
            captures.offer_held_at(name, &mut held.shown, &unit.rows, &held.why);
        }
    }
}

/// Take every sent-but-unacknowledged batch back for re-sending, plus the one that just failed, in acceptance order.
fn take_back(owner: &mut HashMap<BatchId, Owed>, redo: &mut VecDeque<Unit>, failed: Unit) {
    redo.push_front(failed);
    take_back_all(owner, redo);
}

fn take_back_all(owner: &mut HashMap<BatchId, Owed>, redo: &mut VecDeque<Unit>) {
    let mut all: Vec<Unit> = owner.drain().map(|(_, o)| o.unit).chain(redo.drain(..)).collect();
    all.sort_by_key(|u| u.seq);
    redo.extend(all);
}

/// Tell each reader with work outstanding that it will not be delivered.
fn abandon(returns: &Returns, owner: &mut HashMap<BatchId, Owed>, redo: &mut VecDeque<Unit>) {
    let mut readers: Vec<ReaderId> = owner
        .values()
        .map(|o| o.unit.reader)
        .chain(redo.iter().map(|u| u.reader))
        .collect();
    readers.sort_unstable();
    readers.dedup();
    let map = returns.lock().unwrap();
    for reader in readers {
        if let Some(tx) = map.get(&reader) {
            let _ = tx.send(Report::Failed(
                "destination stopped with this batch undelivered".to_string(),
            ));
        }
    }
}

/// Count a failure, mark the destination unhealthy, and log the stall, again every [`crate::logs::RELOG_EVERY`]
/// while it lasts. This thread owns the client, so it is the only writer of `smeltr_destination_healthy` and
/// `smeltr_destination_stalls_total`.
fn note_stall(metrics: &MetricsHandle, id: DestId, name: &str, e: &IoError, stalled: &mut Option<Instant>) {
    let label = id.to_string();
    metrics.incr("smeltr_destination_stalls_total", &[("destination", label.as_str())], 1);
    metrics.gauge("smeltr_destination_healthy", &[("destination", label.as_str())], 0.0);
    if stall_log_due(*stalled) {
        *stalled = Some(Instant::now());
        tracing::warn!(
            target: "egress.destination_stalled",
            subject = name,
            error = %e,
            "a destination is stalled: its rows are held here and re-sent until it recovers. Nothing is committed \
             past them, and the sources feeding it stop once this destination's buffer is full."
        );
    }
}

/// Mark the destination down as soon as its client says it reaches nothing, without waiting for a send to fail, and
/// up again once the client says it reaches a broker, unless a failed send has marked it down since. Batches in flight
/// stay with the client: it is still sending them, and only a failed send takes them back.
fn note_cut_off(
    metrics: &MetricsHandle,
    id: DestId,
    name: &str,
    error: Option<String>,
    cut_off: &mut bool,
    stalled: &mut Option<Instant>,
    backoff: &mut Duration,
) {
    match error {
        // On `stalled`, not `cut_off`: a landed send clears the stall while the client may still say it reaches
        // nothing. A cut-off stall is told again once its log is due; a failed send's is told by its retries.
        Some(e) if stalled.is_none() || (*cut_off && stall_log_due(*stalled)) => {
            *cut_off = true;
            note_stall(metrics, id, name, &IoError::transient(e), stalled);
        }
        None if *cut_off => {
            *cut_off = false;
            clear_stall(metrics, id, name, stalled, backoff);
        }
        _ => {}
    }
}

/// Report what the destination's own loop currently allows outstanding. This thread owns the client, so it is the only
/// writer of `smeltr_destination_in_flight`. A destination that reports no figure leaves the series absent rather than
/// publishing a zero, which would read as a stalled destination.
fn note_concurrency(metrics: &MetricsHandle, id: DestId, now: Option<usize>, reported: &mut Option<usize>) {
    let Some(level) = now else { return };
    if *reported == Some(level) {
        return;
    }
    *reported = Some(level);
    let label = id.to_string();
    metrics.gauge(
        "smeltr_destination_in_flight",
        &[("destination", label.as_str())],
        level as f64,
    );
}

/// Report what the destination's open files buffer, written on change.
fn note_open(metrics: &MetricsHandle, id: DestId, now: Option<usize>, reported: &mut Option<usize>) {
    let Some(bytes) = now else { return };
    if *reported == Some(bytes) {
        return;
    }
    *reported = Some(bytes);
    let label = id.to_string();
    metrics.gauge(
        "smeltr_destination_open_bytes",
        &[("destination", label.as_str())],
        bytes as f64,
    );
}

/// Publish how many rows the destination refused and keeps, wherever each is: waiting, or being sent again. The leader
/// reads it to know which workers a capture at this destination has held rows to wait for.
fn note_kept(
    metrics: &MetricsHandle,
    id: DestId,
    owner: &HashMap<BatchId, Owed>,
    redo: &VecDeque<Unit>,
    waiting: &[(Instant, Unit)],
    reported: &mut Option<u64>,
) {
    // A DLQ's carrier stands for its riders too: each is a row of its own.
    let held = |u: &Unit| match u.held {
        Some(_) => u.rows.iter().map(OwnedRecord::rows).sum(),
        None => 0,
    };
    let kept = waiting.iter().map(|(_, u)| held(u)).sum::<u64>()
        + owner.values().map(|o| held(&o.unit)).sum::<u64>()
        + redo.iter().map(held).sum::<u64>();
    if *reported == Some(kept) {
        return;
    }
    *reported = Some(kept);
    let label = id.to_string();
    metrics.gauge(
        "smeltr_destination_held_rows",
        &[("destination", label.as_str())],
        kept as f64,
    );
}

/// The shortest gap between two writes of the queue's fill. The loop turns every [`POLL_INTERVAL`], far oftener than
/// any heartbeat reads: without a floor a busy destination would take the registry's lock two hundred times a second
/// to publish a number nobody has asked for yet. Well under the heartbeat interval, so what a beat reads is current.
const QUEUE_SAMPLE_FLOOR: Duration = Duration::from_millis(25);

/// Publish what the queue holds. A level, not a count: the leader reads it off the freshest heartbeat, because a level
/// summed over a window is not a number anything means.
///
/// Read at most once per [`QUEUE_SAMPLE_FLOOR`], and written only where it moved since the last write -- so an idle
/// destination costs one lock every floor and no write at all. `last` is what went out, so the floor delays a
/// change by at most its own length and never drops one.
fn note_queue(metrics: &MetricsHandle, id: DestId, queue: &Queue, last: &mut Option<usize>, at: &mut Instant) {
    if at.elapsed() < QUEUE_SAMPLE_FLOOR {
        return;
    }
    *at = Instant::now();
    let held = queue.held();
    if *last == Some(held) {
        return;
    }
    *last = Some(held);
    let label = id.to_string();
    metrics.gauge(
        "smeltr_destination_queue_bytes",
        &[("destination", label.as_str())],
        held as f64,
    );
}

/// State what this queue may hold. Reported beside the fill because a fill alone says nothing: 6 MB is most of a
/// Kafka queue and a rounding error against a destination that states a larger one.
///
/// Written once, at [`Egress::start`]. The bound never moves, and every beat carries it on regardless: a gauge
/// reports its current value whether or not it was written since (`Metrics::peek_delta`).
fn note_queue_budget(metrics: &MetricsHandle, id: DestId, queue: &Queue) {
    let label = id.to_string();
    metrics.gauge(
        "smeltr_destination_queue_budget_bytes",
        &[("destination", label.as_str())],
        queue.budget as f64,
    );
}

/// Count a batch as delivered: the destination has acknowledged it, not merely accepted it for later delivery.
/// `Dispatched::Complete` and a completion out of `poll_completions` are the only two places that is true --
/// `Dispatched::InFlight` is a hand-off to the transport, not a delivery, and is not counted here (see the comment on
/// that match arm). A row rejected within an otherwise-delivered batch still counts: the destination took the batch
/// and then refused the row, which `smeltr_rows_rejected_total` tracks on its own. `bytes` is the whole batch as the
/// queue measured it, refused rows included: they crossed the wire.
fn note_delivered(metrics: &MetricsHandle, id: DestId, rows: u64, bytes: u64) {
    let label = id.to_string();
    if rows > 0 {
        metrics.incr("smeltr_rows_delivered_total", &[("destination", label.as_str())], rows);
    }
    if bytes > 0 {
        metrics.incr(
            "smeltr_bytes_delivered_total",
            &[("destination", label.as_str())],
            bytes,
        );
    }
}

/// Mark the destination healthy again. Called before the batch is reported: a reader released by that report may read
/// the gauge, and it must not see the stalled value of a send that has already landed.
fn clear_stall(metrics: &MetricsHandle, id: DestId, name: &str, stalled: &mut Option<Instant>, backoff: &mut Duration) {
    *backoff = RETRY_MIN;
    let label = id.to_string();
    metrics.gauge("smeltr_destination_healthy", &[("destination", label.as_str())], 1.0);
    if stalled.take().is_some() {
        tracing::info!(
            target: "egress.destination_recovered",
            subject = name,
            "a destination recovered"
        );
    }
}

fn report(returns: &Returns, reader: ReaderId, r: Report) {
    if let Some(tx) = returns.lock().unwrap().get(&reader) {
        // A reader that has gone away is not an error.
        let _ = tx.send(r);
    }
}

/// One reader's view of one destination: `deliver` hands a batch to the egress thread as one unit, `poll_completions`
/// reads what came back. It holds no rows between calls.
pub struct EgressHandle {
    reader: ReaderId,
    queue: Arc<Queue>,
    reports: Receiver<Report>,
    /// Completions taken off the channel but not yet handed to the caller. A completion is a fact about a batch egress
    /// has released; dropping one leaves that batch uncommitted forever.
    ready: Vec<BatchCompletion>,
    /// Held refusals taken off the channel, for `take_held`.
    held: Vec<HeldRefusal>,
    outstanding: usize,
    stall_after: Duration,
}

impl EgressHandle {
    /// Drain whatever has arrived without waiting. Completions collected alongside a failure are kept for the next
    /// call.
    fn collect(&mut self) -> Result<Vec<BatchCompletion>, IoError> {
        let mut out = std::mem::take(&mut self.ready);
        let mut failure = None;
        while let Ok(r) = self.reports.try_recv() {
            match r {
                Report::Done(c) => out.push(c),
                Report::Held(h) => self.held.push(h),
                Report::Failed(msg) => failure = Some(msg),
            }
        }
        if let Some(msg) = failure {
            self.outstanding = 0;
            self.ready = out;
            return Err(IoError::transient(msg));
        }
        self.outstanding = self.outstanding.saturating_sub(out.len());
        Ok(out)
    }
}

impl Destination for EgressHandle {
    /// Queues the rows as one unit. Refuses nothing itself: the destination answers on the acknowledgment.
    fn deliver(
        &mut self,
        batch: BatchId,
        rows: Cow<'_, [OwnedRecord]>,
    ) -> Result<(Dispatched, Vec<Rejection>), IoError> {
        // A batch with nothing for this destination stays off the queue.
        if rows.is_empty() {
            return Ok((Dispatched::Complete, Vec::new()));
        }
        let rows = rows.into_owned();
        let unit = Unit {
            // Assigned under the queue's lock.
            seq: 0,
            reader: self.reader,
            batch,
            bytes: crate::io::held_bytes(&rows),
            rows,
            held: None,
        };
        let dispatched = match self.queue.offer(unit) {
            Ok(()) => {
                self.outstanding += 1;
                Dispatched::InFlight
            }
            // The rows go with the unit: the batch is re-run whole when room returns.
            Err(Refused::NoRoom(unit)) => Dispatched::NoRoom {
                bytes: unit.bytes,
                stall_after: self.stall_after,
            },
            Err(Refused::Closed) => return Err(IoError::transient("destination is shutting down")),
        };
        Ok((dispatched, Vec::new()))
    }

    fn has_room(&self, bytes: usize) -> bool {
        self.queue.has_room(bytes)
    }

    fn poll_completions(&mut self) -> Result<Vec<BatchCompletion>, IoError> {
        self.collect()
    }

    fn take_held(&mut self) -> Vec<HeldRefusal> {
        std::mem::take(&mut self.held)
    }

    /// Wait until every batch this reader handed over has been reported.
    fn flush(&mut self) -> Result<(), IoError> {
        let deadline = Instant::now() + self.stall_after;
        while self.outstanding > 0 {
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return Err(IoError::transient(format!(
                    "destination did not acknowledge {} batch(es) within {:?}",
                    self.outstanding, self.stall_after
                )));
            }
            match self.reports.recv_timeout(left) {
                Ok(Report::Done(c)) => {
                    self.ready.push(c);
                    self.outstanding -= 1;
                }
                Ok(Report::Held(h)) => self.held.push(h),
                Ok(Report::Failed(msg)) => {
                    self.outstanding = 0;
                    return Err(IoError::transient(msg));
                }
                Err(RecvTimeoutError::Timeout) => continue,
                Err(RecvTimeoutError::Disconnected) => {
                    self.outstanding = 0;
                    return Err(IoError::transient("destination thread has stopped"));
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A stall is logged when it begins and again once its last log is [`crate::logs::RELOG_EVERY`] old, so the
    /// leader's warning for it never fades while it lasts.
    #[test]
    fn a_stall_is_logged_again_once_its_last_log_is_old() {
        assert!(stall_log_due(None), "a new stall is logged");
        assert!(!stall_log_due(Some(Instant::now())), "one just logged is not");
        let old = Instant::now()
            .checked_sub(crate::logs::RELOG_EVERY)
            .expect("the clock reaches back that far");
        assert!(stall_log_due(Some(old)), "one logged a relog ago is again");
    }

    fn unit(bytes: usize) -> Unit {
        Unit {
            seq: 0,
            reader: 0,
            batch: 0,
            rows: Vec::new(),
            bytes,
            held: None,
        }
    }

    fn borrowing(budget: usize, pool: &Arc<QueuePool>, reserve: usize, charge: usize) -> Queue {
        let queue = Queue::new(budget);
        let _ = queue.lend.set(Lend {
            pool: pool.clone(),
            reserve,
            charge,
        });
        queue
    }

    #[test]
    fn a_queue_borrows_above_its_reserve_and_gives_back_on_acknowledgment() {
        let pool = QueuePool::new(10);
        let queue = borrowing(100, &pool, 4, 1);
        assert!(queue.offer(unit(4)).is_ok());
        assert_eq!(pool.lent(), 0, "the reserve is the queue's own");
        assert!(queue.offer(unit(6)).is_ok());
        assert_eq!(pool.lent(), 6);
        assert!(!queue.has_room(5), "the pool has 4 left");
        assert!(matches!(queue.offer(unit(5)), Err(Refused::NoRoom(_))));
        queue.release(7);
        assert_eq!(pool.lent(), 0, "3 held are within the reserve");
    }

    #[test]
    fn a_stalled_queue_borrows_no_more_than_its_budget_and_leaves_the_rest_to_others() {
        let pool = QueuePool::new(20);
        let stalled = borrowing(12, &pool, 2, 1);
        let other = borrowing(100, &pool, 2, 1);
        assert!(stalled.offer(unit(6)).is_ok());
        assert!(stalled.offer(unit(6)).is_ok());
        assert!(!stalled.has_room(1), "at its budget");
        assert_eq!(pool.lent(), 10);
        assert!(other.offer(unit(12)).is_ok());
        assert_eq!(pool.lent(), 20);
    }

    #[test]
    fn an_empty_queue_takes_a_unit_even_from_an_empty_pool() {
        let pool = QueuePool::new(0);
        let queue = borrowing(100, &pool, 0, 2);
        assert!(queue.offer(unit(3)).is_ok(), "a queue always moves");
        assert_eq!(pool.lent(), 6, "each byte held counts twice");
        assert!(!queue.has_room(1));
        queue.release(3);
        assert_eq!(pool.lent(), 0);
    }
}
