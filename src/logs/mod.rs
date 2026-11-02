//! Logs (design doc, Observability): what a process has to say about itself in sentences, beside the
//! counters [`crate::metrics`] keeps about itself in numbers. Same scope, same lifetime: one buffer per process,
//! written wherever a call site is and whether or not a deployment exists.
//!
//! ```text
//!   call sites ──┐
//!     tracing    ├──► layer ──► bounded channel ──► drain thread ──► console
//!   log facade ──┘   (filter)     (drops, counts)
//! ```
//!
//! Three properties the shape exists for:
//!
//! - **A write never blocks.** Reader, transform and egress threads hand an event off and continue. The console's
//!   latency is unbounded and not ours -- a pipe to a slow reader, a terminal over a slow link -- and a thread
//!   moving data must never wait on it. A buffer with no room drops and counts. This is the one bounded queue in
//!   smeltr that refuses to backpressure (design doc, Runtime stages), because a report of a stall must not be
//!   stalled by it.
//! - **A call site reports once per event, not once per element.** A loop that refuses a thousand rows says so
//!   once, with a thousand in a field. That is what keeps output proportional to what happened rather than to how
//!   much data it happened to.
//! - **A log is data.** A timestamp, a severity, a site, a sentence and typed fields. The console
//!   rendering is one view of it; what a SIEM reads is the fields, not a parse of the sentence.
//!
//! A path that is about to exit does not write here. It prints and goes: a buffer has nothing to offer a process
//! that will not outlive the handoff. An orderly exit calls [`stop`] first.

mod console;
mod event;
mod layer;
mod noted;

pub(crate) use console::{render_fields, render_value, stamp};
#[cfg(test)]
pub(crate) use layer::LogLayer;

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, TrySendError, sync_channel};
use std::sync::{Condvar, LazyLock, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime};

pub use event::{Event, Value};
pub use noted::Noted;
pub use tracing::Level;

/// One event as a test reads it: its level, its target, and its fields with the sentence under `message`.
#[cfg(test)]
pub(crate) type Said = (Level, String, Vec<(String, String)>);

/// Every event raised on this thread while `f` runs.
#[cfg(test)]
pub(crate) fn said(f: impl FnOnce()) -> Vec<Said> {
    use tracing_subscriber::layer::SubscriberExt;

    #[derive(Default)]
    struct Catch(std::sync::Arc<Mutex<Vec<Said>>>);
    struct Fields(Vec<(String, String)>);
    impl tracing::field::Visit for Fields {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            self.0.push((
                field.name().to_string(),
                format!("{value:?}").trim_matches('"').to_string(),
            ));
        }
    }
    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Catch {
        fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
            let mut fields = Fields(Vec::new());
            event.record(&mut fields);
            self.0.lock().unwrap().push((
                *event.metadata().level(),
                event.metadata().target().to_string(),
                fields.0,
            ));
        }
    }
    let catch = Catch::default();
    let seen = catch.0.clone();
    tracing::subscriber::with_default(tracing_subscriber::registry().with(catch), f);
    seen.lock().unwrap().clone()
}

/// Events held between drains. Reached only when the drain thread is behind, so it buys time rather than storage.
///
/// `just measure` empties a full one in about 27 ms. That is what this buys against a burst, and what
/// [`STOP_GRACE`] has to cover.
pub const CAPACITY: usize = 4096;

/// Events held for a logs source. Smaller than what is held for the leader, and for a different reason:
/// this one is drained every poll by a reader thread, so a backlog here means the pipeline carrying it has
/// stalled -- in which case its own logs matter more than the ones it was carrying.
pub const LOGS_CAPACITY: usize = 1024;

/// Events held for the leader while it is out of contact, on a worker, or until a leader writes its own into its
/// store. A worker that cannot reach one keeps the oldest and
/// refuses the rest, the same way the buffer in front of it refuses rather than evicts: the start of an outage is
/// what explains it, and the symptoms after are the part that repeats.
pub const PENDING_CAPACITY: usize = 4096;

/// How long [`stop`] waits for the drain thread to finish what it holds. Long enough for a full buffer to reach a
/// slow pipe, and short enough that a ctrl-c still feels immediate.
///
/// Some thirty times what an idle console needs, which is the point: the one an operator is watching may be a
/// terminal over a slow link rather than a pipe to nothing.
pub const STOP_GRACE: Duration = Duration::from_secs(1);

/// How long the leader shows a warning a worker logged after its last log of it.
pub const WARNING_FADES_AFTER: Duration = Duration::from_secs(5 * 60);

/// How often a worker logs a condition that lasts again: inside [`WARNING_FADES_AFTER`], so its warning stands
/// while the condition does and fades once it stops.
pub const RELOG_EVERY: Duration = Duration::from_secs(4 * 60);

/// When a thing was last said, so a site says it once, then again every [`RELOG_EVERY`] while it goes on, and
/// once more as soon as it starts again after [`Repeat::clear`].
#[derive(Debug, Default)]
pub struct Repeat(Option<std::time::Instant>);

impl Repeat {
    /// Not said yet. `const`, so a static can hold one.
    pub const fn new() -> Repeat {
        Repeat(None)
    }

    /// Whether the thing is due to be said now, and counts it said.
    pub fn due(&mut self) -> bool {
        if self.0.is_none_or(|at| at.elapsed() >= RELOG_EVERY) {
            self.0 = Some(std::time::Instant::now());
            return true;
        }
        false
    }

    /// The thing stopped: it is said again as soon as it starts.
    pub fn clear(&mut self) {
        self.0 = None;
    }
}

/// A fault that lasts, and when to log it: when it begins, when its message changes, and again every
/// [`RELOG_EVERY`] while it lasts, so its warning stands as long as the fault does. The log line itself stays with
/// the caller, which knows what to say.
#[derive(Debug, Default)]
pub struct Relog {
    why: Option<String>,
    /// When `why` was last logged.
    logged: Repeat,
}

impl Relog {
    /// No fault under way. `const`, so a static can hold one.
    pub const fn new() -> Relog {
        Relog {
            why: None,
            logged: Repeat::new(),
        }
    }

    /// Keeps `why` as the fault under way. A changed message is due its log at once; one that differs only in what
    /// [`steady`] drops is the same fault.
    pub fn fail(&mut self, why: String) {
        if self.why.as_deref().is_none_or(|old| steady(old) != steady(&why)) {
            self.logged.clear();
        }
        self.why = Some(why);
    }

    /// [`Relog::fail`] and [`Relog::due`] in one, for a site with nothing to do between them.
    pub fn fail_due(&mut self, why: String) -> Option<&str> {
        self.fail(why);
        self.due()
    }

    /// The fault under way, when it is due its log now, and counts it logged.
    pub fn due(&mut self) -> Option<&str> {
        if self.why.is_none() || !self.logged.due() {
            return None;
        }
        self.why.as_deref()
    }

    /// The fault under way.
    pub fn why(&self) -> Option<&str> {
        self.why.as_deref()
    }

    /// The fault is over. Whether one was under way, for a caller that says so.
    pub fn clear(&mut self) -> bool {
        self.logged.clear();
        self.why.take().is_some()
    }
}

/// `text` without the parts of an error that change from one report of the same fault to the next: how long curl or
/// librdkafka waited ("after 3005ms", "after 30 milliseconds with 0 bytes received"), and how many repeats librdkafka
/// counted (", 571 identical error(s) suppressed"). For telling two reports apart; the line keeps the text as it was.
pub fn steady(text: &str) -> std::borrow::Cow<'_, str> {
    static VARYING: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
        regex::Regex::new(
            r"(, \d+ identical error\(s\) suppressed)|( ?after \d+ ?(milliseconds|ms)\b( with \d+ (out of \d+ )?bytes received)?)",
        )
        .unwrap()
    });
    VARYING.replace_all(text, "")
}

thread_local! {
    /// Whether this thread is inside [`take`], where a panic must not be logged through it again.
    static WRITING: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Every panic in the process is logged before the default hook prints it, so a thread or task that dies says so
/// here and not only on a console nobody reads. A panic that budget handling expects is quiet.
fn log_panics() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            let writing = WRITING.with(std::cell::Cell::get);
            if !writing
                && info
                    .payload()
                    .downcast_ref::<crate::core::buffer::OverBudget>()
                    .is_none()
            {
                let panic = panic_text(info.payload());
                let thread = std::thread::current();
                tracing::error!(
                    target: "process.panicked",
                    thread = thread.name().unwrap_or_default(),
                    location = info.location().map(|l| l.to_string()).unwrap_or_default(),
                    %panic,
                    "a thread panicked"
                );
            }
            previous(info);
        }));
    });
}

/// What the channel carries. `Stop` rides it so that it arrives behind every event already sent.
enum Message {
    /// An event, and whether a thread of this process's leader raised it.
    Event(Event, bool),
    Stop,
}

static SENDER: OnceLock<SyncSender<Message>> = OnceLock::new();
static DROPPED: AtomicU64 = AtomicU64::new(0);
/// Written to a console that would not take them. Counted apart from `DROPPED`, which is the buffer being behind:
/// this is the buffer keeping up and there being nobody to tell.
static UNWRITABLE: AtomicU64 = AtomicU64::new(0);
/// Set by [`stop`]. Only redirects new logs to the console; what ends the drain thread is `Message::Stop`.
static STOPPING: AtomicBool = AtomicBool::new(false);
/// Raised by the drain thread once it has written what it held, and waited on by [`stop`].
static FLUSHED: (Mutex<bool>, Condvar) = (Mutex::new(false), Condvar::new());
/// The severity threshold, as `Level`'s discriminant. `Level` is not an atomic-sized type of its own.
static MAX_LEVEL: AtomicUsize = AtomicUsize::new(level_ord(Level::INFO));
/// This process's worker id, once it has one. `start` runs before either role begins, so early events have none.
static WORKER: OnceLock<String> = OnceLock::new();
/// Whether this process runs a leader and no worker, so that what no thread of the leader's raised is the leader's
/// all the same: a thread it spawned itself, or one before it started.
static NO_WORKER: AtomicBool = AtomicBool::new(false);
/// What the drain thread has written, by site. Whoever holds a registry publishes it; `logs` stays underneath
/// `metrics` rather than depending on it. This is what says whether one site is drowning the rest.
static WRITTEN: LazyLock<Mutex<HashMap<String, u64>>> = LazyLock::new(Default::default);
/// Written, and not yet acknowledged by the leader. Drained by the heartbeat, which drops only what landed.
static PENDING: LazyLock<Mutex<VecDeque<Event>>> = LazyLock::new(Default::default);
/// What this process's leader raised and has not yet written into its own store. Apart from [`PENDING`], so a
/// worker in the same process never carries the leader's logs under its own name.
static LEADER_PENDING: LazyLock<Mutex<VecDeque<Event>>> = LazyLock::new(Default::default);
/// Where logs sources read: one queue per open [`Tap`], named by its number. Empty while none is open: nothing
/// accumulates for a consumer that does not exist, which is the same reason a leader queues nothing.
static LOGS: LazyLock<Mutex<Vec<Queue>>> = LazyLock::new(Default::default);
/// One reader's queue.
struct Queue {
    /// The number of the [`Tap`] that owns it.
    tap: u64,
    /// When its reader opened it. What the worker's other process said before then is not this reader's.
    opened: SystemTime,
    events: VecDeque<Event>,
}
/// The number the next [`Tap`] takes.
static NEXT_TAP: AtomicU64 = AtomicU64::new(0);
/// Events a full queue in [`LOGS`] refused, for the life of the process.
static UNREAD: AtomicU64 = AtomicU64::new(0);

/// This process's logs, numbered. With [`boot_id`] it is the key the leader dedupes on: `send` retries a
/// heartbeat once, and unlike the metrics it rides beside, these rows accumulate rather than overwrite.
static NEXT_SEQ: AtomicU64 = AtomicU64::new(0);
/// This process, told apart from its own previous lives. A sequence restarts at zero on a restart, so without
/// this a fresh event would collide with an old row and be dropped as a duplicate.
static BOOT_ID: LazyLock<u64> = LazyLock::new(|| {
    // Not randomness for its own sake: two workers restarting in the same millisecond must not share a boot id,
    // and the address of a fresh allocation is the cheapest thing to hand that differs between them.
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or_default();
    now ^ (Box::into_raw(Box::new(0u8)) as u64).rotate_left(32)
});

pub(crate) fn next_seq() -> u64 {
    NEXT_SEQ.fetch_add(1, Ordering::Relaxed)
}

/// This process's boot id, which with an event's sequence number names it uniquely for the life of the fleet.
pub fn boot_id() -> u64 {
    *BOOT_ID
}

/// Events a full [`PENDING`] or [`LEADER_PENDING`] refused, for the life of the process. A worker out of contact for long enough loses
/// logs whatever it does; this says how many.
static UNDELIVERED: AtomicU64 = AtomicU64::new(0);

/// `Level` orders most severe first, so a smaller ordinal is a more severe event.
const fn level_ord(level: Level) -> usize {
    match level {
        Level::ERROR => 0,
        Level::WARN => 1,
        Level::INFO => 2,
        Level::DEBUG => 3,
        Level::TRACE => 4,
    }
}

fn level_of(ord: usize) -> Level {
    match ord {
        0 => Level::ERROR,
        1 => Level::WARN,
        2 => Level::INFO,
        3 => Level::DEBUG,
        _ => Level::TRACE,
    }
}

/// The threshold every call site is measured against.
pub fn max_level() -> Level {
    level_of(MAX_LEVEL.load(Ordering::Relaxed))
}

/// The severities anything written down may name, most severe first.
pub const LEVEL_LABELS: &[&str] = &["error", "warn", "info", "debug", "trace"];

/// One of [`LEVEL_LABELS`], in any case and with surrounding space. Nothing else, the numbers `tracing`'s own
/// parser accepts included: `3` tells whoever reads the config back nothing.
pub fn level_for_label(label: &str) -> Option<Level> {
    match label.trim().to_ascii_lowercase().as_str() {
        "error" => Some(Level::ERROR),
        "warn" => Some(Level::WARN),
        "info" => Some(Level::INFO),
        "debug" => Some(Level::DEBUG),
        "trace" => Some(Level::TRACE),
        _ => None,
    }
}

/// Logs dropped for want of room, for the life of the process. A running total: whoever holds a registry
/// reports it as a gauge, so `logs` stays underneath `metrics` rather than depending on it.
pub fn dropped() -> u64 {
    DROPPED.load(Ordering::Relaxed)
}

/// Up to `limit` of what the leader has not acknowledged, oldest first, left in place and mapped where they lie.
/// The heartbeat carries what this returns and calls [`delivered`] with what landed, so a dropped connection
/// costs a round trip rather than the events.
///
/// Mapped rather than handed over: what rides the wire is a smaller thing than an `Event`, and materializing a
/// batch of events only to convert and drop them would copy every message twice a beat.
pub fn pending_map<T>(limit: usize, f: impl FnMut(&Event) -> T) -> Vec<T> {
    map_queue(&PENDING, limit, f)
}

/// [`pending_map`] for what this process's leader raised, which the leader writes into its own store.
pub fn leader_pending_map<T>(limit: usize, f: impl FnMut(&Event) -> T) -> Vec<T> {
    map_queue(&LEADER_PENDING, limit, f)
}

fn map_queue<T>(queue: &Mutex<VecDeque<Event>>, limit: usize, f: impl FnMut(&Event) -> T) -> Vec<T> {
    queue
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .iter()
        .take(limit)
        .map(f)
        .collect()
}

/// How many the leader has not acknowledged.
pub fn pending_len() -> usize {
    PENDING.lock().unwrap_or_else(|e| e.into_inner()).len()
}

/// Drop the `count` oldest: the leader has them. Called only on a heartbeat the leader answered.
pub fn delivered(count: usize) {
    drop_oldest(&PENDING, count);
}

/// [`delivered`] for what the leader wrote into its own store.
pub fn leader_delivered(count: usize) {
    drop_oldest(&LEADER_PENDING, count);
}

fn drop_oldest(queue: &Mutex<VecDeque<Event>>, count: usize) {
    let mut pending = queue.lock().unwrap_or_else(|e| e.into_inner());
    let take = count.min(pending.len());
    pending.drain(..take);
}

/// Open a queue of this process's logs for one reader, kept open for as long as the tap lives.
///
/// Every reader gets every event from its opening on, the way every metrics reader holds a cursor of its own: a
/// deployed logs source and a capture read side by side, and neither takes from the other. Dropping the tap -- a
/// redeploy, a bundle that no longer declares the source, a capture that ended -- closes its queue, so nothing
/// accumulates for a reader that has gone.
pub fn tap() -> Tap {
    let id = NEXT_TAP.fetch_add(1, Ordering::Relaxed);
    LOGS.lock().unwrap_or_else(|e| e.into_inner()).push(Queue {
        tap: id,
        opened: SystemTime::now(),
        events: VecDeque::new(),
    });
    Tap(id)
}

/// One reader's open queue, closed when dropped. Only [`tap`] makes one.
pub struct Tap(u64);

impl Tap {
    /// Up to `limit`, oldest first, taken. Unlike the leader's queue these are gone once read: the pipeline that
    /// reads them is the one that decides what at-least-once means from here.
    pub fn drain(&self, limit: usize) -> Vec<Event> {
        let mut logs = LOGS.lock().unwrap_or_else(|e| e.into_inner());
        let Some(queue) = logs.iter_mut().find(|q| q.tap == self.0) else {
            return Vec::new();
        };
        let take = limit.min(queue.events.len());
        queue.events.drain(..take).collect()
    }

    /// Return what [`drain`](Self::drain) handed over and a caller could not use, oldest first. They go back in
    /// front of everything recorded since, which is where they came from, so a reader that takes more rows than
    /// its batch has room for loses nothing rather than losing it silently.
    ///
    /// The queue is still bounded: what no longer fits is refused here exactly as it would have been on the way
    /// in, and counted the same way.
    pub fn put_back(&self, events: Vec<Event>) {
        let mut logs = LOGS.lock().unwrap_or_else(|e| e.into_inner());
        let Some(queue) = logs.iter_mut().find(|q| q.tap == self.0) else {
            return;
        };
        for event in events.into_iter().rev() {
            if queue.events.len() < LOGS_CAPACITY {
                queue.events.push_front(event);
            } else {
                UNREAD.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

impl Drop for Tap {
    fn drop(&mut self) {
        LOGS.lock()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|q| q.tap != self.0);
    }
}

/// Hand every open reader events the worker's other process recorded and sent here. They reach the readers only:
/// the process that recorded them has written them to its console and queued them for the leader, so the leader
/// hears each event once, from the process that owns it.
pub fn mirror(events: impl IntoIterator<Item = Event>) {
    let worker = worker();
    // Collected before the lock: whatever produces them may log, and a log here with no drain thread takes it.
    let events: Vec<Event> = events.into_iter().collect();
    let mut logs = LOGS.lock().unwrap_or_else(|e| e.into_inner());
    for mut event in events {
        event.worker = worker;
        // A mirror arrives up to a heartbeat after what it carries was said, so a reader opened in between would
        // otherwise be handed what was said before it asked.
        let timestamp = event.timestamp;
        offer(logs.iter_mut().filter(|q| q.opened <= timestamp), &event);
    }
}

/// `event` to each of `queues`, each refusing it on its own when full.
fn offer<'a>(queues: impl IntoIterator<Item = &'a mut Queue>, event: &Event) {
    for queue in queues {
        if queue.events.len() < LOGS_CAPACITY {
            queue.events.push_back(event.clone());
        } else {
            UNREAD.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Events a full logs queue refused, for the life of the process. One count per reader that refused one.
pub fn unread() -> u64 {
    UNREAD.load(Ordering::Relaxed)
}

/// Events refused by a full queue while the leader was out of contact, for the life of the process.
pub fn undelivered() -> u64 {
    UNDELIVERED.load(Ordering::Relaxed)
}

/// Logs that reached a console which would not take them, for the life of the process. A closed pipe makes
/// every write fail while nothing is dropped, so without this the pair below would report a healthy buffer at the
/// moment nothing is reaching anybody.
pub fn unwritable() -> u64 {
    UNWRITABLE.load(Ordering::Relaxed)
}

/// What has been written, by site, for the life of the process. The answer to "is one site drowning the rest",
/// which is the question that decides whether these events ever need folding.
pub fn written_by_site() -> Vec<(String, u64)> {
    WRITTEN
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .iter()
        .map(|(site, n)| (site.clone(), *n))
        .collect()
}

thread_local! {
    /// Set while this thread is delivering logs. Read by the layer, which runs on the thread that records
    /// -- the drain thread is too late, because by then the thread that raised it is gone.
    static DELIVERING: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    /// Set on every thread of this process's leader. Read by [`hand_off`] on the thread that records, for the same
    /// reason as `DELIVERING`.
    static LEADER_THREAD: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Mark this thread as the leader's, so what it logs is stored under the leader rather than under a worker running
/// in the same process. Called on the leader's main thread and on each thread its runtime starts.
pub fn mark_leader_thread() {
    LEADER_THREAD.with(|t| t.set(true));
}

/// Say what the buffers lost since the last call, once per outage and on the interval: the counts a scrape reads,
/// for a reader of the log alone. Console-only, since what carries logs onward must not queue a report of its own
/// failing to.
pub fn report_losses() {
    static LAST: Mutex<([u64; 4], Relog)> = Mutex::new(([0; 4], Relog::new()));
    let now = [dropped(), undelivered(), unread(), unwritable()];
    let mut last = LAST.lock().unwrap_or_else(|e| e.into_inner());
    let since: Vec<u64> = now.iter().zip(last.0).map(|(n, l)| n.saturating_sub(l)).collect();
    let (dropped, undelivered, unread, unwritable) = (since[0], since[1], since[2], since[3]);
    if since.iter().any(|n| *n > 0) {
        if last.1.fail_due("losing".to_string()).is_some() {
            // Counted from the last line, not the last call: a beat that says nothing leaves its losses for the next.
            last.0 = now;
            let count = |n: u64| (n > 0).then_some(n);
            while_delivering(|| {
                tracing::warn!(
                    target: "logs.events_lost",
                    dropped = count(dropped),
                    undelivered = count(undelivered),
                    unread = count(unread),
                    unwritable = count(unwritable),
                    "log events were lost since the last report"
                );
            });
        }
    } else if last.1.clear() {
        while_delivering(|| {
            tracing::info!(target: "logs.events_kept_again", "no log event has been lost since the last report");
        });
    }
}

/// What a panic said, for a line or an error: its message when it has one, as `panic!` and `expect` leave it,
/// and nothing otherwise.
pub fn panic_text(payload: &dyn std::any::Any) -> String {
    payload
        .downcast_ref::<&str>()
        .map(|s| s.to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_default()
}

/// Run `f` with everything this thread reports marked console-only.
///
/// Whatever carries logs onward raises logs of its own when it fails, and those must not join the
/// queue they were failing to empty: a leader that cannot write a batch would report so, queue the report, fail
/// on the next beat, and report again, one event per beat forever.
pub fn while_delivering<T>(f: impl FnOnce() -> T) -> T {
    struct Clear;
    impl Drop for Clear {
        fn drop(&mut self) {
            DELIVERING.with(|d| d.set(false));
        }
    }
    DELIVERING.with(|d| d.set(true));
    let _clear = Clear;
    f()
}

/// Whether this thread is delivering logs. Unwinding through [`while_delivering`] still clears it.
pub(crate) fn delivering() -> bool {
    DELIVERING.with(std::cell::Cell::get)
}

/// Name this process's worker, once it has a name. Called once, by the worker, after it has loaded or minted its
/// id -- which is after [`start`], because a process does not know its role until its configuration is read.
pub fn set_worker(id: &str) {
    let _ = WORKER.set(id.to_string());
}

fn worker() -> Option<&'static str> {
    WORKER.get().map(String::as_str)
}

/// Queue what no thread of the leader's raised for the leader too, as it queues what they raised. Called once, by a
/// leader running no worker of its own; in one that runs one, those are the worker's.
pub fn runs_no_worker() {
    NO_WORKER.store(true, Ordering::Relaxed);
}

/// Hand an event to the drain thread, or write it here where there is no thread to hand it to: before [`start`]
/// -- a test, or anything before startup finished -- and after [`stop`], when the channel has no reader left. A
/// log raised while shutting down is the last one to swallow.
fn hand_off(event: Event) {
    let leader = LEADER_THREAD.with(std::cell::Cell::get);
    match SENDER.get() {
        Some(sender) if !STOPPING.load(Ordering::Relaxed) => {
            if sender.try_send(Message::Event(event, leader)).is_err() {
                DROPPED.fetch_add(1, Ordering::Relaxed);
            }
        }
        _ => take(event, leader),
    }
}

/// Install the buffer, start its drain thread, and point both `tracing` and the `log` facade at it. Called once,
/// at startup, before either role begins.
///
/// The facade is the point for everything smeltr did not write: `rdkafka`, `ureq` and `rustls` all report through
/// it, and with no backend installed everything they have ever reported was discarded. `tracing` is the point for
/// everything else -- axum, tower and h2 instrument with it, and with no subscriber installed all of that went
/// nowhere too.
pub fn start() {
    log_panics();
    let (sender, receiver) = sync_channel::<Message>(CAPACITY);
    if SENDER.set(sender).is_err() {
        return;
    }
    std::thread::Builder::new()
        .name("smeltr-logs".into())
        .spawn(move || drain(receiver))
        .expect("start the logs drain thread (called once, at startup)");

    let level = crate::startup_config::log_level();
    MAX_LEVEL.store(level_ord(level), Ordering::Relaxed);

    // A second installation is somebody else's subscriber, not a failure of ours: leave it alone and keep our own
    // call sites working.
    use tracing_subscriber::layer::SubscriberExt;
    let subscriber = tracing_subscriber::registry().with(layer::LogLayer);
    if tracing::subscriber::set_global_default(subscriber).is_ok() {
        // The facade's own ceiling, checked inside the library before it formats anything. Set below what is kept
        // it would hide records we asked for; set above it, libraries render text we then discard.
        let _ = tracing_log::LogTracer::builder()
            .with_max_level(as_log_filter(layer::facade_ceiling(level)))
            .init();
    }
}

fn as_log_filter(level: Level) -> tracing_log::log::LevelFilter {
    use tracing_log::log::LevelFilter;
    match level {
        Level::ERROR => LevelFilter::Error,
        Level::WARN => LevelFilter::Warn,
        Level::INFO => LevelFilter::Info,
        Level::DEBUG => LevelFilter::Debug,
        Level::TRACE => LevelFilter::Trace,
    }
}

/// Write what the buffer still holds, and stop the drain thread. Called immediately before `std::process::exit`
/// on a path that means to exit: what is still in the channel lives in the drain thread, and nothing else will
/// write it.
///
/// Bounded by [`STOP_GRACE`] and never fails. The rule that a write never blocks applies at this end too -- an
/// exit waits for logs a little, never indefinitely.
pub fn stop() {
    let Some(sender) = SENDER.get() else { return };
    // Before the handoff, so a log raised while the drain thread finishes is written here rather than
    // landing in a channel that is about to go unread.
    STOPPING.store(true, Ordering::Relaxed);

    // A full channel means the drain thread is busy rather than stuck, so retrying lands it. Past the deadline we
    // are exiting regardless, which is the whole bargain.
    let deadline = Instant::now() + STOP_GRACE;
    let mut stop = Message::Stop;
    while let Err(TrySendError::Full(unsent)) = sender.try_send(stop) {
        if Instant::now() >= deadline {
            return;
        }
        stop = unsent;
        std::thread::yield_now();
    }

    let (written, wake) = &FLUSHED;
    let guard = written.lock().unwrap_or_else(|e| e.into_inner());
    let remaining = deadline.saturating_duration_since(Instant::now());
    let _ = wake.wait_timeout_while(guard, remaining, |written| !*written);
}

fn drain(receiver: Receiver<Message>) {
    loop {
        match receiver.recv() {
            Ok(Message::Event(event, leader)) => take(event, leader),
            // Behind every event already sent, so nothing recorded before a stop is left unwritten.
            Ok(Message::Stop) => break,
            // `SENDER` is a static and outlives every thread, so this cannot fire: `Stop` is what ends this
            // thread. A real exit loses at most what is still in the channel.
            Err(_) => break,
        }
    }

    let (written, wake) = &FLUSHED;
    *written.lock().unwrap_or_else(|e| e.into_inner()) = true;
    wake.notify_all();
}

/// Write one event, raised by a thread of the leader's where `leader`, and count it against its site. The drain thread's job, and also the caller's own on the paths
/// that have no drain thread to hand to.
///
/// Counted after the write and only when it landed: a count of what was said is worth nothing if it also counts
/// what nobody could hear.
fn take(event: Event, leader: bool) {
    // A panic in here would re-enter through the hook and take a lock this thread holds.
    WRITING.with(|w| w.set(true));
    struct Done;
    impl Drop for Done {
        fn drop(&mut self) {
            WRITING.with(|w| w.set(false));
        }
    }
    let _done = Done;
    // Counted first, and against the console alone: whether the leader hears about this is a separate question
    // from whether anybody here did, and gating one on the other means a daemon with a closed stderr tells the
    // leader nothing -- the one shape where its console was already useless.
    if console::write(&event).is_ok() {
        let mut written = WRITTEN.lock().unwrap_or_else(|e| e.into_inner());
        // A site is almost always one this map already holds, and its key is a `&'static str` for everything
        // smeltr writes itself, so the common path neither allocates nor copies.
        match written.get_mut(event.site.as_ref()) {
            Some(count) => *count += 1,
            None => {
                written.insert(event.site.to_string(), 1);
            }
        }
    } else {
        UNWRITABLE.fetch_add(1, Ordering::Relaxed);
    }

    // Never what delivery itself raised. Whatever carries these onward reports its own failures, and putting
    // those in the next batch makes a failure that repeats itself for the life of the process.
    if event.local_only {
        return;
    }

    // Every open reader. Taken first because a reader is the consumer that can be told to stop.
    offer(LOGS.lock().unwrap_or_else(|e| e.into_inner()).iter_mut(), &event);

    // A worker queues for the leader, whose heartbeat is the one path to one, and a leader queues for its own
    // store. A process running both roles keeps the two apart, so each is stored under its own name.
    let queue = if leader || (worker().is_none() && NO_WORKER.load(Ordering::Relaxed)) {
        &LEADER_PENDING
    } else if worker().is_some() {
        &PENDING
    } else {
        return;
    };
    let mut pending = queue.lock().unwrap_or_else(|e| e.into_inner());
    if pending.len() < PENDING_CAPACITY {
        pending.push_back(event);
    } else {
        UNDELIVERED.fetch_add(1, Ordering::Relaxed);
    }
}

/// The open taps are one table in a process, so every test that opens one, in any module, holds this.
#[cfg(test)]
pub(crate) static TAPS_LOCK: Mutex<()> = Mutex::new(());

/// How many readers have a queue open.
#[cfg(test)]
pub(crate) fn taps_open() -> usize {
    LOGS.lock().unwrap_or_else(|e| e.into_inner()).len()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn queue_len(tap: &Tap) -> usize {
        LOGS.lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .find(|q| q.tap == tap.0)
            .map_or(0, |q| q.events.len())
    }

    fn push(seq: u64) {
        offer(LOGS.lock().unwrap_or_else(|e| e.into_inner()).iter_mut(), &event(seq));
    }

    fn event(seq: u64) -> Event {
        Event {
            timestamp: std::time::SystemTime::UNIX_EPOCH,
            level: Level::ERROR,
            site: "put_back.test".into(),
            message: String::new(),
            fields: Vec::new(),
            worker: None,
            thread: None,
            local_only: false,
            seq,
        }
    }

    fn queued(queue: &Mutex<VecDeque<Event>>, site: &str) -> bool {
        queue
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .any(|e| e.site == site)
    }

    #[test]
    fn a_thing_is_said_once_until_it_stops_and_starts_again() {
        let mut said = Repeat::new();
        assert!(said.due(), "said the first time");
        assert!(!said.due(), "not again inside the interval");
        said.clear();
        assert!(said.due(), "said again once it starts again");
    }

    /// What a thread of the leader's raises is kept for the leader's own store and never rides a worker's beat,
    /// though a worker runs in the same process; what any other thread raises is never the leader's.
    #[test]
    fn a_leader_thread_s_logs_are_kept_apart_from_a_worker_s() {
        let leader_site = "leader_thread.marked.test";
        let other_site = "leader_thread.unmarked.test";
        std::thread::spawn(move || {
            mark_leader_thread();
            hand_off(Event {
                site: leader_site.into(),
                ..event(1)
            });
        })
        .join()
        .unwrap();
        std::thread::spawn(move || {
            hand_off(Event {
                site: other_site.into(),
                ..event(2)
            });
        })
        .join()
        .unwrap();

        assert!(
            queued(&LEADER_PENDING, leader_site),
            "a leader thread's log was not kept for the leader"
        );
        assert!(
            !queued(&PENDING, leader_site),
            "a leader thread's log was queued for a worker's beat"
        );
        assert!(
            !queued(&LEADER_PENDING, other_site),
            "another thread's log was kept as the leader's"
        );
    }

    /// A reader that takes more rows than its batch has room for hands the rest back, and they come out of the
    /// next drain in the order they were recorded -- not after whatever arrived while it was encoding. The queue
    /// stays bounded across that return: it refuses on the way back exactly as it would on the way in.
    #[test]
    fn a_return_goes_in_front_of_what_came_since_and_is_refused_when_full() {
        let _taps = TAPS_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let tap = tap();
        for seq in 0..4 {
            push(seq);
        }

        let taken = tap.drain(4);
        assert_eq!(taken.iter().map(|e| e.seq).collect::<Vec<_>>(), vec![0, 1, 2, 3]);

        // Two shipped, two over budget, and one more recorded while that was happening.
        push(4);
        tap.put_back(taken.into_iter().skip(2).collect());

        let next = tap.drain(8);
        assert_eq!(
            next.iter().map(|e| e.seq).collect::<Vec<_>>(),
            vec![2, 3, 4],
            "the returned pair is older than what arrived while they were held"
        );

        // And with the queue full, a return is refused like any other write.
        let taken: Vec<Event> = (0..2).map(event).collect();
        while queue_len(&tap) < LOGS_CAPACITY {
            push(9);
        }

        let before = unread();
        tap.put_back(taken);
        assert_eq!(
            unread() - before,
            2,
            "a full queue refuses a return like any other write"
        );
        assert_eq!(queue_len(&tap), LOGS_CAPACITY, "and does not grow past its bound");
    }

    /// Two readers each get every event, and one reading does not take from the other. A reader's drop closes its
    /// own queue and leaves the other's.
    #[test]
    fn every_reader_gets_every_event_and_closes_only_its_own_queue() {
        let _taps = TAPS_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let first = tap();
        let second = tap();
        push(1);

        assert_eq!(first.drain(8).iter().map(|e| e.seq).collect::<Vec<_>>(), vec![1]);
        assert_eq!(
            second.drain(8).iter().map(|e| e.seq).collect::<Vec<_>>(),
            vec![1],
            "the first reader's drain took from the second"
        );

        drop(first);
        assert_eq!(taps_open(), 1, "the first reader's drop closed the wrong queue");
        push(2);
        assert_eq!(second.drain(8).iter().map(|e| e.seq).collect::<Vec<_>>(), vec![2]);
        drop(second);
        assert_eq!(taps_open(), 0, "a dropped reader left its queue open");
    }

    /// What the worker's other process sent reaches every reader here, named as this process's worker, and
    /// nowhere else: that process has already written it to its console and queued it for the leader.
    #[test]
    fn a_mirrored_event_reaches_the_readers_only() {
        let _taps = TAPS_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let site = "mirror.readers_only.test";
        let mirrored = || Event {
            timestamp: SystemTime::now(),
            site: site.into(),
            ..event(7)
        };

        // With no reader open, nothing is kept for one.
        mirror([mirrored()]);
        let reader = tap();
        assert!(reader.drain(8).is_empty(), "a mirror with no reader open was kept");

        mirror([mirrored()]);
        let got = reader.drain(8);
        assert_eq!(got.len(), 1, "{got:?}");
        assert_eq!(got[0].site, site);
        assert_eq!(got[0].worker, worker());
        assert!(
            written_by_site().iter().all(|(s, _)| s != site),
            "a mirrored event was written to this process's console"
        );
    }

    /// A mirror arrives up to a heartbeat after what it carries was said. A reader is handed only what was said
    /// once it was open, as it would have been had the other process been this one.
    #[test]
    fn a_mirrored_event_said_before_a_reader_opened_is_not_that_readers() {
        let _taps = TAPS_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let said_before = Event {
            timestamp: SystemTime::now() - Duration::from_secs(5),
            site: "mirror.before_open.test".into(),
            ..event(1)
        };
        let reader = tap();
        mirror([said_before]);
        assert!(
            reader.drain(8).iter().all(|e| e.site != "mirror.before_open.test"),
            "a reader was handed what was said before it opened"
        );
    }

    /// A report that differs only in how long it waited or how many repeats it counted is the same fault, said once.
    #[test]
    fn a_relog_reads_past_a_duration_and_a_repeat_count() {
        let refused = |took: u32, repeats: u32| {
            format!(
                "Connect to ipv4#127.0.0.1:9 failed: Connection refused (after {took}ms in state CONNECT, {repeats} \
                 identical error(s) suppressed)"
            )
        };
        let mut relog = Relog::new();
        assert!(relog.fail_due(refused(0, 1)).is_some());
        assert_eq!(
            relog.fail_due(refused(3005, 571)),
            None,
            "only the wait and the count changed"
        );
        assert_eq!(
            relog.fail_due("unable to access: Could not resolve host after 30 ms".into()),
            Some("unable to access: Could not resolve host after 30 ms"),
            "a changed fault is said at once, with its text as it was"
        );
        assert_eq!(
            relog.fail_due(
                "unable to access: Could not resolve host after 2041 milliseconds with 0 bytes received".into()
            ),
            None
        );
    }

    /// A fault is said when it begins, when its message changes, and again each [`RELOG_EVERY`] while it lasts;
    /// its end is said once, and a fault after that is a new one.
    #[test]
    fn a_relog_says_a_fault_when_it_begins_changes_or_lasts_and_its_end_once() {
        let mut relog = Relog::new();
        assert_eq!(relog.fail_due("away".into()), Some("away"));
        assert_eq!(
            relog.fail_due("away".into()),
            None,
            "the same fault is not said again at once"
        );
        assert_eq!(relog.why(), Some("away"), "the fault stands while it is not said");
        relog.logged = Repeat(std::time::Instant::now().checked_sub(RELOG_EVERY));
        assert_eq!(
            relog.fail_due("away".into()),
            Some("away"),
            "a fault that lasts is said again on the interval"
        );
        assert_eq!(
            relog.fail_due("gone".into()),
            Some("gone"),
            "a changed message is said at once"
        );
        assert!(relog.clear(), "a fault under way ends");
        assert!(!relog.clear(), "its end is said once");
        assert_eq!(relog.due(), None, "nothing is due after the fault ends");
        assert_eq!(
            relog.fail_due("away".into()),
            Some("away"),
            "a fault after a recovery is new"
        );
    }
}
