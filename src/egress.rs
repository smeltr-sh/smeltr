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

pub mod pacing;

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
use pacing::{Completion, Pacer};

/// Which reader a unit came from, and where its completion goes.
type ReaderId = u64;

/// A batch sent and not yet acknowledged: refusals seen so far, and the rows, kept for a re-send.
struct Owed {
    rejections: Vec<Rejection>,
    unit: Unit,
    /// What the pacing model is told when it is acknowledged, once the queue is paced.
    paced: Option<Completion>,
}

/// One reader's batch, as far as one destination is concerned. Owned rows: they cross a thread boundary.
struct Unit {
    reader: ReaderId,
    batch: BatchId,
    rows: Vec<OwnedRecord>,
    bytes: usize,
    /// Set on the rows of a batch this destination refused and keeps, being sent again. Boxed: a unit is offered by
    /// value, and most units are never held.
    held: Option<Box<Held>>,
    /// Refusals of the batch's other rows, which are not sent again, reported with the batch once it completes: the
    /// ones a held unit's transport kept no document for, and the ones answered before part of the batch failed.
    carried: Vec<Rejection>,
    /// Set once the unit failed to land: the wait before this attempt, doubling up to [`RETRY_MAX`]. The destination
    /// is not recovered while any unit has one.
    retry: Option<Duration>,
}

/// What is known of a batch whose refusals are being sent again.
struct Held {
    /// The wait before this attempt; the next one waits twice as long, up to `HELD_RETRY_MAX`.
    backoff: Duration,
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
    /// Borrowed besides, for the senders the destination spawned past those it started with
    /// ([`Egress::charging_senders`]). Held while the destination lives: a spawned sender is kept.
    senders: usize,
    closed: bool,
}

struct Queue {
    state: Mutex<QueueState>,
    /// Bytes this destination may hold -- queued, and sent but unacknowledged -- before an offer is refused. Shared
    /// across readers. The pacing model's target and one batch ready behind it once the queue is paced
    /// ([`Egress::paced`], `Pacing::queue_bytes`).
    budget: AtomicUsize,
    /// The budget it was started with: the most a paced queue that borrows from no pool may hold.
    most: usize,
    /// The least a paced queue's budget is, one batch, once the queue is paced.
    paced: OnceLock<usize>,
    /// A source asked for room this queue did not have, since the last pop. The pacing model's mark that the link,
    /// not the source, bounded what the next unit's sample measures: at steady state the queue is often empty with
    /// everything in flight, so a backlog alone would read a full link as a quiet source.
    pressed: AtomicBool,
    /// Signaled when a unit arrives, or on close.
    work: Condvar,
    /// The transform pool's wake-up, signaled when bytes are freed: a lane parked on this queue is the pool's to run
    /// again, and nothing else tells it room came back.
    pool: Mutex<Option<Arc<Ready>>>,
    /// The destination is set to block: it keeps what it refuses. See [`Egress::holding_refusals`].
    holds_refusals: AtomicBool,
    /// Where bytes above a reserve come from, when this queue borrows. See [`Egress::borrowing`].
    lend: OnceLock<Lend>,
    /// What one sender the destination spawns holds, when the queue's pool pays for them. See
    /// [`Egress::charging_senders`].
    sender_bytes: OnceLock<usize>,
    /// The captures told this destination is sent to here. See [`Egress::with_captures`].
    captures: OnceLock<Arc<crate::worker::capture::Captures>>,
}

/// The bytes every borrowing queue of one deployment may hold above its reserve, together. A deployment has two: one
/// its destination queues borrow from, and one its receivers do (`io::receiver::Holdings`).
///
/// A queue holds its reserve whatever the others do, so each can always move. Above it, it takes from here what its
/// own budget still allows, and gives back as the destination acknowledges. A stalled destination holds at most its
/// budget, so the rest keep what remains.
///
/// Lending is first come, first served while the pool has room. Once a queue is refused, the pool is contended, and
/// for the next [`WANTING_FOR`] each queue may borrow only up to an equal share: the limit over the queues holding or
/// wanting. Nothing is taken back -- a queue above its share stops borrowing and drains as its destination
/// acknowledges -- so the shares settle in about a round trip.
pub struct QueuePool {
    limit: usize,
    lent: AtomicUsize,
    fair: Mutex<Fair>,
}

/// Who holds and who was refused, for the share rule.
#[derive(Default)]
struct Fair {
    next: u64,
    holding: HashSet<u64>,
    wanting: HashMap<u64, Instant>,
}

/// How long a refusal keeps the pool contended.
const WANTING_FOR: Duration = Duration::from_secs(1);

impl Fair {
    fn prune(&mut self, now: Instant) {
        self.wanting
            .retain(|_, at| now.saturating_duration_since(*at) < WANTING_FOR);
    }

    /// The pool bytes `who` may hold while others want room: an equal share among everyone holding or wanting.
    fn share(&self, who: u64, limit: usize) -> usize {
        let contenders = self
            .holding
            .iter()
            .chain(self.wanting.keys())
            .collect::<HashSet<_>>()
            .len();
        let contenders = contenders + usize::from(!self.holding.contains(&who) && !self.wanting.contains_key(&who));
        limit / contenders.max(1)
    }

    fn others_wanting(&self, who: u64) -> bool {
        self.wanting.keys().any(|k| *k != who)
    }
}

impl QueuePool {
    pub fn new(limit: usize) -> Arc<QueuePool> {
        Arc::new(QueuePool {
            limit,
            lent: AtomicUsize::new(0),
            fair: Mutex::new(Fair::default()),
        })
    }

    /// A name for one queue that borrows here.
    pub(crate) fn lender(&self) -> u64 {
        let mut f = self.fair.lock().unwrap();
        f.next += 1;
        f.next
    }

    /// The pool bytes `who` may hold right now: the whole pool while nobody else wants room, its share otherwise.
    pub(crate) fn share(&self, who: u64) -> usize {
        let mut f = self.fair.lock().unwrap();
        f.prune(Instant::now());
        if f.others_wanting(who) {
            f.share(who, self.limit)
        } else {
            self.limit
        }
    }

    /// The most it lends.
    pub fn limit(&self) -> usize {
        self.limit
    }

    /// Bytes lent now.
    pub fn lent(&self) -> usize {
        self.lent.load(Ordering::Relaxed)
    }

    /// Whether `who`, holding `held` pool bytes, could take `bytes` more now.
    pub(crate) fn fits(&self, who: u64, held: usize, bytes: usize) -> bool {
        let mut f = self.fair.lock().unwrap();
        f.prune(Instant::now());
        self.allowed(&f, who, held, bytes)
    }

    fn allowed(&self, f: &Fair, who: u64, held: usize, bytes: usize) -> bool {
        self.lent() + bytes <= self.limit && (!f.others_wanting(who) || held + bytes <= f.share(who, self.limit))
    }

    /// Lends `bytes` more to `who`, holding `held`, if they fit, or anyway when `forced`. A refusal marks `who` as
    /// wanting, which holds every queue to its share for a while.
    pub(crate) fn take(&self, who: u64, held: usize, bytes: usize, forced: bool) -> bool {
        let now = Instant::now();
        let mut f = self.fair.lock().unwrap();
        f.prune(now);
        if !forced && !self.allowed(&f, who, held, bytes) {
            f.wanting.insert(who, now);
            return false;
        }
        self.lent.fetch_add(bytes, Ordering::Relaxed);
        if held + bytes > 0 {
            f.holding.insert(who);
        }
        true
    }

    /// `who` gives `bytes` back, holding `held` after.
    pub(crate) fn give(&self, who: u64, held: usize, bytes: usize) {
        self.lent.fetch_sub(bytes, Ordering::Relaxed);
        if held == 0 {
            self.fair.lock().unwrap().holding.remove(&who);
        }
    }
}

/// How one queue borrows: from which pool, above how many bytes of its own, and how many bytes of memory each byte it
/// holds stands for.
struct Lend {
    pool: Arc<QueuePool>,
    /// This queue's name in the pool.
    id: u64,
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
            budget: AtomicUsize::new(budget),
            most: budget,
            paced: OnceLock::new(),
            pressed: AtomicBool::new(false),
            work: Condvar::new(),
            pool: Mutex::new(None),
            holds_refusals: AtomicBool::new(false),
            lend: OnceLock::new(),
            sender_bytes: OnceLock::new(),
            captures: OnceLock::new(),
        }
    }

    /// Whether `bytes` more would fit. An empty queue takes anything: a unit larger than the whole budget goes in
    /// alone, borrowing what it must, and the cap stops a second joining it. A closed queue answers yes so the offer
    /// reaches the refusal that says so.
    fn has_room(&self, bytes: usize) -> bool {
        let st = self.state.lock().unwrap();
        let room = st.closed
            || st.bytes == 0
            || (st.bytes + bytes <= self.budget()
                && self.lend.get().is_none_or(|l| {
                    l.pool.fits(
                        l.id,
                        st.borrowed + st.senders,
                        l.borrowed(st.bytes + bytes) - st.borrowed,
                    )
                }));
        if !room {
            self.pressed.store(true, Ordering::Relaxed);
        }
        room
    }

    /// Whether the source is pressing on this queue: it was refused room since the last pop. Units waiting behind
    /// the one popped are not enough: they wait on this thread, with the budget not yet full, and a sample taken
    /// then says how fast this thread hands batches over, not how fast the link carries them. Consumed: the mark is
    /// the next unit's.
    fn pressing(&self) -> bool {
        self.pressed.swap(false, Ordering::Relaxed)
    }

    /// The most a paced budget may be: the queue bytes whose borrowing fits this queue's share of the pool, less
    /// what its senders borrow, plus the reserve, which is its own. Without a pool, the budget it was started with.
    fn share(&self) -> usize {
        match self.lend.get() {
            Some(l) => {
                let senders = self.state.lock().unwrap().senders;
                l.reserve
                    .saturating_add(l.pool.share(l.id).saturating_sub(senders) / l.charge)
            }
            None => self.most,
        }
    }

    /// Borrow what one more sender holds, if the pool lends it: the destination asks before it spawns one past
    /// those it started with. Refused, the sender is not spawned and the pool is contended, as for a unit.
    fn take_sender(&self) -> bool {
        let (Some(lend), Some(&bytes)) = (self.lend.get(), self.sender_bytes.get()) else {
            return true;
        };
        let mut st = self.state.lock().unwrap();
        if !lend.pool.take(lend.id, st.borrowed + st.senders, bytes, false) {
            return false;
        }
        st.senders += bytes;
        true
    }

    /// Bytes this queue may hold now.
    fn budget(&self) -> usize {
        self.budget.load(Ordering::Relaxed)
    }

    /// The pacing model moved: the queue holds no more than `bytes` from here, draining to it if above.
    fn set_budget(&self, bytes: usize) {
        self.budget.store(bytes, Ordering::Relaxed);
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
        if st.bytes > 0 && st.bytes + unit.bytes > self.budget() {
            self.pressed.store(true, Ordering::Relaxed);
            return Err(Refused::NoRoom(unit));
        }
        if let Some(lend) = self.lend.get() {
            let borrowed = lend.borrowed(st.bytes + unit.bytes);
            if !lend
                .pool
                .take(lend.id, st.borrowed + st.senders, borrowed - st.borrowed, st.bytes == 0)
            {
                self.pressed.store(true, Ordering::Relaxed);
                return Err(Refused::NoRoom(unit));
            }
            st.borrowed = borrowed;
        }
        st.bytes += unit.bytes;
        st.units.push_back(unit);
        drop(st);
        self.work.notify_one();
        Ok(())
    }

    /// Next unit, waiting at most `timeout` for one.
    fn pop(&self, timeout: Duration) -> Popped {
        let mut st = self.state.lock().unwrap();
        if let Some(u) = st.units.pop_front() {
            return Popped::Work(u, self.pressing());
        }
        if st.closed {
            return Popped::Closed;
        }
        let (mut st, _) = self.work.wait_timeout(st, timeout).unwrap();
        if let Some(u) = st.units.pop_front() {
            return Popped::Work(u, self.pressing());
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
            lend.pool.give(lend.id, borrowed + st.senders, st.borrowed - borrowed);
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
        let mut st = self.state.lock().unwrap();
        st.closed = true;
        // The destination and its senders end with the queue.
        if let Some(lend) = self.lend.get() {
            lend.pool.give(lend.id, st.borrowed, st.senders);
            st.senders = 0;
        }
        drop(st);
        self.work.notify_all();
        if let Some(pool) = self.pool.lock().unwrap().as_ref() {
            pool.signal_all();
        }
    }
}

enum Popped {
    /// A unit, and whether the source was pressing on the queue: refused room since the last.
    Work(Unit, bool),
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
        let id = pool.lender();
        let _ = self.queue.lend.set(Lend {
            pool,
            id,
            reserve,
            charge: charge.max(1),
        });
        self
    }

    /// Let the pacing model ([`pacing`]) set this queue's budget, from `floor` bytes, one batch, up to the queue's
    /// share of the pool it borrows from, or without a pool up to the budget it was started with. The budget is
    /// released only as batches are acknowledged, so it bounds what the destination has in flight too.
    pub fn paced(self, floor: usize) -> Egress {
        self.queue.set_budget(floor.max(1));
        let _ = self.queue.paced.set(floor.max(1));
        self
    }

    /// Have each sender the destination spawns past those it started with borrow `bytes` from the pool this queue
    /// borrows from, held for as long as the destination lives. The pacing target is then held to what the pool
    /// lends besides, so the senders and the bytes they carry share one share. Without a pool, nothing is charged.
    pub fn charging_senders(self, bytes: usize) -> Egress {
        let _ = self.queue.sender_bytes.set(bytes);
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
    queue: &Arc<Queue>,
    returns: &Returns,
    metrics: &MetricsHandle,
    id: DestId,
    name: &str,
) {
    // The destination's own batch ids, mapped back to the reader's.
    let mut owner: HashMap<BatchId, Owed> = HashMap::new();
    let mut next_local: BatchId = 0;
    // Units to send again, each with when: ones that failed to land, and refused rows kept.
    let mut again: Vec<(Instant, Unit)> = Vec::new();
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
    // The pacing model, once the queue is paced. Its target is the queue's budget, and so what is in flight.
    let mut pacer: Option<Pacer> = None;

    loop {
        if pacer.is_none()
            && let Some(&floor) = queue.paced.get()
        {
            if queue.sender_bytes.get().is_some() {
                let queue = queue.clone();
                destination.lend_senders(Box::new(move || queue.take_sender()));
            }
            pacer = Some(Pacer::new(floor, queue, metrics, id));
        }
        // Here, not only where the queue is polled: a unit that keeps failing is re-sent forever without asking for new
        // work.
        if queue.is_closed() {
            break;
        }

        // At the top, so the passes that `continue` on a delivery failure report it too. Those are the passes a full
        // queue is most worth seeing.
        note_queue(metrics, id, queue, &mut held_reported, &mut held_reported_at);
        // A re-send that is due before anything new: it is older.
        let now = Instant::now();
        let mut pressing = true;
        let next = match again.iter().position(|(at, _)| *at <= now) {
            Some(i) => Some(again.swap_remove(i).1),
            None => match queue.pop(POLL_INTERVAL) {
                Popped::Closed => break,
                Popped::Idle => None,
                Popped::Work(u, pressed) => {
                    pressing = pressed;
                    Some(u)
                }
            },
        };

        if let Some(unit) = next {
            let local = next_local;
            next_local += 1;
            // A re-send is never the source's doing: it counts as the link's.
            let app_limited = !pressing;
            // Borrowed: the unit is kept until acknowledged, to send again or to hand a refused row back.
            let delivered = destination.deliver(local, Cow::Borrowed(&unit.rows));
            // Once the destination has taken it: a Kafka producer at its bound holds `deliver` until it has room,
            // and that wait is this side's, not the link's round trip. A batch it did not take was never in flight.
            let taken = matches!(delivered, Ok((Dispatched::Complete | Dispatched::InFlight, _)));
            let paced = pacer
                .as_mut()
                .filter(|_| taken)
                .map(|p| p.dispatched(unit.bytes, app_limited));
            match delivered {
                Err(e) => {
                    note_stall(metrics, id, name, &e, &mut stalled);
                    cut_off = false;
                    // This unit alone: the ones in flight are answered for by `poll_completions`.
                    again.push(failed(unit));
                    queue.sleep_unless_closed(backoff);
                    backoff = (backoff * 2).min(RETRY_MAX);
                    continue;
                }
                // Nothing will report it later, so report it now.
                Ok((Dispatched::Complete, rejections)) => {
                    // The destination is up, if not yet for every unit.
                    if failing(&owner, &again) {
                        backoff = RETRY_MIN;
                    } else {
                        clear_stall(metrics, id, name, &mut stalled, &mut backoff);
                    }
                    if let (Some(p), Some(c)) = (&mut pacer, paced) {
                        p.completed(queue, c);
                    }
                    finish(queue, returns, metrics, id, unit, rejections, &mut again);
                }
                // Refusals seen at hand-over wait for the acknowledgment, so one completion carries every refusal
                // against a batch. Handed to the transport is not delivered: `Dispatched::InFlight` means only that, so
                // healthy is not reported here. A transport that will not talk to its destination at all still hands
                // every batch off this way -- `poll_completions` is what tells the two apart, once the batch either
                // lands or times out.
                Ok((Dispatched::InFlight, rejections)) => {
                    owner.insert(
                        local,
                        Owed {
                            rejections,
                            unit,
                            paced,
                        },
                    );
                }
                // A transport's own queue is not this queue: a client that will not take a batch is a delivery
                // failure here, retried like any other.
                Ok((Dispatched::NoRoom { .. }, _)) => {
                    let e = IoError::transient("destination client would not take the batch");
                    note_stall(metrics, id, name, &e, &mut stalled);
                    cut_off = false;
                    again.push(failed(unit));
                    queue.sleep_unless_closed(backoff);
                    backoff = (backoff * 2).min(RETRY_MAX);
                    continue;
                }
            }
        }

        // Every pass, idle or not -- see `POLL_INTERVAL`.
        match destination.poll_completions() {
            Ok(done) => {
                // A batch that landed beside one that failed does not clear the stall, nor does it wait on the failed
                // one: that waits its own `retry`. The destination recovers once every unit that failed has landed.
                let (mut failures, mut landed) = (false, false);
                for c in done {
                    let Some(mut owed) = owner.remove(&c.batch) else {
                        continue;
                    };
                    owed.rejections.extend(c.rejections);
                    if let Some(undelivered) = c.undelivered.filter(|u| !u.rows.is_empty()) {
                        note_stall(metrics, id, name, &undelivered.error, &mut stalled);
                        failures = true;
                        let rest = remainder(queue, metrics, id, owed.unit, owed.rejections, &undelivered.rows);
                        if let (Some(p), Some(paced)) = (&mut pacer, owed.paced) {
                            p.abandoned(paced, paced.bytes.saturating_sub(rest.bytes));
                        }
                        again.push(failed(rest));
                        continue;
                    }
                    landed = true;
                    if let (Some(p), Some(paced)) = (&mut pacer, owed.paced) {
                        p.completed(
                            queue,
                            Completion {
                                throttled: c.throttled,
                                ..paced
                            },
                        );
                    }
                    finish(queue, returns, metrics, id, owed.unit, owed.rejections, &mut again);
                }
                if failures {
                    cut_off = false;
                }
                if landed {
                    // The destination is up, if not yet for every unit.
                    if failures || failing(&owner, &again) {
                        backoff = RETRY_MIN;
                    } else {
                        clear_stall(metrics, id, name, &mut stalled, &mut backoff);
                    }
                } else if failures {
                    // Nothing landed: the destination is down as a whole, so it backs off as a whole.
                    queue.sleep_unless_closed(backoff);
                    backoff = (backoff * 2).min(RETRY_MAX);
                }
            }
            Err(e) => {
                note_stall(metrics, id, name, &e, &mut stalled);
                cut_off = false;
                take_back_all(&mut owner, &mut again, &mut pacer);
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
            note_kept(metrics, id, &owner, &again, &mut kept_reported);
        }
        if let Some(captures) = queue.captures.get() {
            let keeps = kept_reported.is_some_and(|kept| kept > 0);
            // Taken before the rows are shown, so a capture admitted meanwhile waits for them rather than reporting.
            if keeps && holding.is_none() {
                holding = Some(captures.holding(HeldAt::Destination(name.to_string())));
            }
            if keeps {
                let units = again
                    .iter_mut()
                    .map(|(_, u)| u)
                    .chain(owner.values_mut().map(|o| &mut o.unit));
                show_held(captures, name, units);
            } else {
                holding = None;
            }
        }
    }

    // Rows nobody will deliver. None of it was committed, so it is redelivered when the deployment comes back.
    abandon(returns, &owner, &again);
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
    again: &mut Vec<(Instant, Unit)>,
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
    let mut carried = std::mem::take(&mut unit.carried);
    let (backoff, shown) = match unit.held.take() {
        Some(h) => ((h.backoff * 2).min(HELD_RETRY_MAX), h.shown),
        None => (HELD_RETRY_MIN, HashSet::new()),
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
                throttled: false,
                undelivered: None,
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
    again.push((
        Instant::now() + backoff,
        Unit {
            reader: unit.reader,
            batch: unit.batch,
            rows,
            bytes,
            held: Some(Box::new(Held { backoff, why, shown })),
            carried,
            retry: None,
        },
    ));
}

/// Show each capture at this destination the rows it refused and keeps, once per capture: waiting to be sent again, or
/// sent.
fn show_held<'a>(captures: &Captures, name: &str, units: impl Iterator<Item = &'a mut Unit>) {
    for unit in units {
        if let Some(held) = &mut unit.held {
            captures.offer_held_at(name, &mut held.shown, &unit.rows, &held.why);
        }
    }
}

/// What of `unit` did not land, as a unit of its own to send again: the rows `undelivered` names, in the batch's order.
/// The rows that landed are counted delivered and their bytes go back to the queue; the refusals answered so far are
/// carried, with their documents, to the batch's completion.
fn remainder(
    queue: &Queue,
    metrics: &MetricsHandle,
    id: DestId,
    mut unit: Unit,
    rejections: Vec<Rejection>,
    undelivered: &[usize],
) -> Unit {
    // In the batch's order: each of its requests named its own.
    let mut kept: Vec<usize> = undelivered.to_vec();
    kept.sort_unstable();
    let mut landed = vec![true; unit.rows.len()];
    for &i in kept.iter().chain(rejections.iter().map(|r| &r.doc)) {
        if let Some(at) = landed.get_mut(i) {
            *at = false;
        }
    }
    let (count, bytes) = unit
        .rows
        .iter()
        .zip(&landed)
        .filter(|(_, landed)| **landed)
        .fold((0u64, 0u64), |(n, b), (row, _)| (n + 1, b + row.sent_bytes() as u64));
    note_delivered(metrics, id, count, bytes);
    for mut r in rejections {
        if r.record.is_none() {
            r.record = unit.rows.get_mut(r.doc).map(std::mem::take);
        }
        unit.carried.push(r);
    }
    let rows: Vec<OwnedRecord> = kept
        .iter()
        .filter_map(|&i| unit.rows.get_mut(i).map(std::mem::take))
        .collect();
    if let Some(held) = &mut unit.held {
        held.why = kept.iter().filter_map(|&i| held.why.get(i).cloned()).collect();
    }
    let bytes = crate::io::held_bytes(&rows);
    queue.release(unit.bytes.saturating_sub(bytes));
    Unit { rows, bytes, ..unit }
}

/// `unit`, which failed to land, to be sent again once its own wait is over. Each failure doubles the wait.
fn failed(mut unit: Unit) -> (Instant, Unit) {
    let retry = unit.retry.map_or(RETRY_MIN, |r| (r * 2).min(RETRY_MAX));
    unit.retry = Some(retry);
    (Instant::now() + retry, unit)
}

/// Whether a unit that failed to land is still out or waiting to be sent again.
fn failing(owner: &HashMap<BatchId, Owed>, again: &[(Instant, Unit)]) -> bool {
    owner.values().any(|o| o.unit.retry.is_some()) || again.iter().any(|(_, u)| u.retry.is_some())
}

/// Take every sent-but-unacknowledged batch back to be sent again. The pacing model is told of each, so it no longer
/// counts it in flight.
fn take_back_all(owner: &mut HashMap<BatchId, Owed>, again: &mut Vec<(Instant, Unit)>, pacer: &mut Option<Pacer>) {
    for (_, o) in owner.drain() {
        if let (Some(p), Some(paced)) = (pacer.as_mut(), o.paced) {
            p.abandoned(paced, 0);
        }
        again.push(failed(o.unit));
    }
}

/// Tell each reader with work outstanding that it will not be delivered.
fn abandon(returns: &Returns, owner: &HashMap<BatchId, Owed>, again: &[(Instant, Unit)]) {
    let mut readers: Vec<ReaderId> = owner
        .values()
        .map(|o| o.unit.reader)
        .chain(again.iter().map(|(_, u)| u.reader))
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

/// Report the requests the destination has open. This thread owns the client, so it is the only
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

/// Publish how many rows the destination refused and keeps, wherever each is: waiting to be sent again, or sent. The
/// leader reads it to know which workers a capture at this destination has held rows to wait for.
fn note_kept(
    metrics: &MetricsHandle,
    id: DestId,
    owner: &HashMap<BatchId, Owed>,
    again: &[(Instant, Unit)],
    reported: &mut Option<u64>,
) {
    // A DLQ's carrier stands for its riders too: each is a row of its own.
    let held = |u: &Unit| match u.held {
        Some(_) => u.rows.iter().map(OwnedRecord::rows).sum(),
        None => 0,
    };
    let kept = again.iter().map(|(_, u)| held(u)).sum::<u64>() + owner.values().map(|o| held(&o.unit)).sum::<u64>();
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
/// Written at [`Egress::start`], and again each time the pacing model moves a paced queue's budget. Every beat
/// carries it on between: a gauge reports its current value whether or not it was written since
/// (`Metrics::peek_delta`).
fn note_queue_budget(metrics: &MetricsHandle, id: DestId, queue: &Queue) {
    let label = id.to_string();
    metrics.gauge(
        "smeltr_destination_queue_budget_bytes",
        &[("destination", label.as_str())],
        queue.budget() as f64,
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
            reader: self.reader,
            batch,
            bytes: crate::io::held_bytes(&rows),
            rows,
            held: None,
            carried: Vec::new(),
            retry: None,
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
            reader: 0,
            batch: 0,
            rows: Vec::new(),
            bytes,
            held: None,
            carried: Vec::new(),
            retry: None,
        }
    }

    fn borrowing(budget: usize, pool: &Arc<QueuePool>, reserve: usize, charge: usize) -> Queue {
        let queue = Queue::new(budget);
        let _ = queue.lend.set(Lend {
            pool: pool.clone(),
            id: pool.lender(),
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

    /// Once a queue is refused, each may borrow only up to an equal share; a queue above its share stops borrowing
    /// and the one refused gets room as the other drains.
    #[test]
    fn a_contended_pool_holds_each_queue_to_an_equal_share() {
        let pool = QueuePool::new(20);
        let greedy = borrowing(100, &pool, 0, 1);
        let other = borrowing(100, &pool, 0, 1);
        assert!(greedy.offer(unit(16)).is_ok(), "nobody else wants it yet");
        assert!(other.offer(unit(1)).is_ok());
        assert!(
            matches!(other.offer(unit(6)), Err(Refused::NoRoom(_))),
            "the pool has 3 left"
        );
        assert!(!greedy.has_room(1), "contended: greedy is above its share of 10");
        assert!(matches!(greedy.offer(unit(1)), Err(Refused::NoRoom(_))));
        assert_eq!(pool.share(greedy.lend.get().unwrap().id), 10);
        greedy.release(10);
        assert_eq!(pool.lent(), 7);
        assert!(other.offer(unit(6)).is_ok(), "the other takes what was given back");
        assert!(
            greedy.offer(unit(4)).is_ok(),
            "the other got its room, so the pool is uncontended again"
        );
        assert!(
            matches!(other.offer(unit(10)), Err(Refused::NoRoom(_))),
            "the other wants more than is left"
        );
        assert!(
            matches!(greedy.offer(unit(1)), Err(Refused::NoRoom(_))),
            "so greedy, at its share, gets no more"
        );
    }

    #[test]
    fn a_paced_queue_holds_what_its_budget_says_and_moves_with_it() {
        let queue = Queue::new(100);
        let _ = queue.paced.set(4);
        queue.set_budget(4);
        assert!(queue.offer(unit(4)).is_ok());
        assert!(!queue.has_room(1), "one batch is the budget");
        queue.set_budget(12);
        assert!(queue.has_room(8));
        assert!(queue.offer(unit(8)).is_ok());
        queue.set_budget(4);
        assert!(!queue.has_room(1), "above the budget: it drains to it");
        queue.release(8);
        assert!(!queue.has_room(1), "4 held is the budget");
        queue.release(4);
        assert!(queue.has_room(4));
    }

    #[test]
    fn a_paced_queue_that_borrows_from_no_pool_grows_up_to_the_budget_it_started_with() {
        let queue = Queue::new(100);
        let _ = queue.paced.set(4);
        queue.set_budget(4);
        assert_eq!(queue.share(), 100, "its stated buffer, or its kind's default");
    }

    #[test]
    fn a_refusal_marks_the_next_unit_as_pressed_and_a_quiet_source_does_not() {
        let queue = Queue::new(4);
        let pressed = |p| match p {
            Popped::Work(_, pressed) => pressed,
            _ => panic!("a unit was queued"),
        };
        // A quiet source: one unit, nothing refused, nothing behind it.
        assert!(queue.offer(unit(4)).is_ok());
        assert!(!pressed(queue.pop(Duration::ZERO)));
        // Everything in flight, the queue empty, and the source asks for room it does not get: the link is full.
        assert!(!queue.has_room(1));
        assert!(queue.offer(unit(4)).is_err());
        queue.release(4);
        assert!(queue.offer(unit(4)).is_ok());
        assert!(pressed(queue.pop(Duration::ZERO)), "a refusal marks the next unit");
        // The mark is consumed: the one after is quiet again.
        queue.release(4);
        assert!(queue.offer(unit(4)).is_ok());
        assert!(!pressed(queue.pop(Duration::ZERO)));
        // A backlog under the budget does not: the queue had room.
        queue.release(4);
        assert!(queue.offer(unit(2)).is_ok());
        assert!(queue.offer(unit(2)).is_ok());
        assert!(!pressed(queue.pop(Duration::ZERO)));
    }

    #[test]
    fn a_spawned_sender_borrows_from_the_pool_and_narrows_the_share() {
        let pool = QueuePool::new(16);
        let queue = borrowing(100, &pool, 4, 1);
        let _ = queue.sender_bytes.set(6);
        assert_eq!(queue.share(), 20);
        assert!(queue.take_sender());
        assert!(queue.take_sender());
        assert_eq!(pool.lent(), 12);
        assert_eq!(queue.share(), 4 + 4, "what the senders hold is not the queue's to hold");
        assert!(!queue.take_sender(), "the pool lends no further");
        assert!(queue.offer(unit(4)).is_ok(), "the reserve is the queue's own");
        assert!(queue.offer(unit(4)).is_ok(), "the pool's rest is the queue's");
        assert!(!queue.has_room(1));
        queue.close();
        assert_eq!(pool.lent(), 4, "the senders end with the queue");
        queue.release(8);
        assert_eq!(pool.lent(), 0);
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
