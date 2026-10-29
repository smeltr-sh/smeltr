//! The reader→transform boundary: a byte-bounded queue of batches one way, and a channel of reports back.
//!
//! The queue is the only backpressure this pair has: when it fills, ingest waits. Reports are small, bounded by the
//! batches in flight, and never block the thread that produced them -- a transform stalled on reporting an
//! acknowledgment would hold up the commit it reports.

use std::collections::VecDeque;
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, TryRecvError, channel};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use crate::core::id::DestId;
use crate::io::{BatchId, IoError, SourceRecord};

/// Bytes of raw records one queue may hold before ingest waits, per batch of depth. A lane's depth is how many
/// transform threads could be working on it at once, plus one of slack so the reader can poll while they do
/// (`worker::lane_depth`).
pub const HANDOFF_BYTES: usize = 4 * 1024 * 1024;

/// One batch, numbered by the reader that polled it.
#[derive(Debug)]
pub struct Handed {
    pub batch: BatchId,
    pub records: Vec<SourceRecord>,
}

/// What transform tells the reader about a batch it was given.
pub enum Report {
    /// `dest` has finished with `batch`. What it refused was answered for before this was sent.
    Acked { batch: BatchId, dest: DestId },
    /// `batch` waits on `dest` again: rows another destination refused were sealed there under the same batch.
    /// Sent before the refusing destination's `Acked`, on the same channel, so the batch is never left waiting on
    /// nothing while the DLQ still has them.
    Owes { batch: BatchId, dest: DestId },
    /// Stop fetching, for `cause`, saying why. Only the reader can act on it: it owns the source.
    Block(BlockCause, String),
    /// Transform has delivered and holds nothing. The reader resumes only if its tracker is empty too.
    Release,
    /// No pipeline will finish this batch (a pipeline panicked on it). Stop waiting on every destination but
    /// `waits_on`, the ones the deployment's `unprocessable` disposition sends it to: those still owe their
    /// acknowledgment, and the watermark moves once they give it.
    Abandoned { batch: BatchId, waits_on: Vec<DestId> },
}

/// What a source is blocked on, which says what clears it (design doc, Exception taxonomy). Ordered by what outlasts
/// what: a reader blocked on two causes names the greater, since it hears only when the whole block lifts.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum BlockCause {
    /// A destination with no room, or one that cannot be reached or asked: it clears on its own once delivery
    /// succeeds.
    Waiting,
    /// Rows a destination refused and is set to block on: it clears once the destination takes them.
    Refused,
    /// Records a route answered with block: only a new config clears it.
    Held,
}

impl BlockCause {
    /// What a heartbeat calls it, as the `cause` label of `smeltr_source_blocked_reason`.
    pub fn name(self) -> &'static str {
        match self {
            BlockCause::Held => "held",
            BlockCause::Refused => "refused",
            BlockCause::Waiting => "waiting",
        }
    }

    /// The cause a heartbeat calls `name`, if this build knows it.
    pub fn named(name: &str) -> Option<BlockCause> {
        [BlockCause::Held, BlockCause::Refused, BlockCause::Waiting]
            .into_iter()
            .find(|c| c.name() == name)
    }
}

struct State {
    queued: VecDeque<Handed>,
    bytes: usize,
    closed: bool,
}

/// A batch the queue would not take. It is already tracked, so it goes back to the reader rather than being dropped: a
/// tracked batch nobody holds blocks every commit behind it forever.
#[derive(Debug)]
pub struct Unsent {
    pub batch: Handed,
    pub error: IoError,
}

/// One signal across every queue a transform thread watches. A thread that found nothing anywhere waits here rather
/// than on one queue, so a batch arriving on any of them wakes it.
#[derive(Default)]
pub struct Ready {
    /// Bumped whenever a batch becomes available; the value itself means nothing beyond "something changed".
    tick: Mutex<u64>,
    arrived: Condvar,
}

impl Ready {
    pub fn new() -> Ready {
        Ready::default()
    }

    /// Say a batch is available. Wakes one waiter: one batch is one thread's work.
    pub fn signal(&self) {
        *self.tick.lock().unwrap() += 1;
        self.arrived.notify_one();
    }

    /// Wakes every waiter, for a change they all have to see -- a close.
    pub fn signal_all(&self) {
        *self.tick.lock().unwrap() += 1;
        self.arrived.notify_all();
    }

    /// Read before scanning, so a batch that arrives during the scan is not waited past.
    pub fn seen(&self) -> u64 {
        *self.tick.lock().unwrap()
    }

    /// Wait until something has been signaled since `seen`, or `timeout` elapses.
    pub fn wait_past(&self, seen: u64, timeout: Duration) {
        let tick = self.tick.lock().unwrap();
        if *tick != seen {
            return;
        }
        let _ = self.arrived.wait_timeout(tick, timeout).unwrap();
    }
}

pub struct Handoff {
    /// Bytes it may hold before a reader pushing to it waits.
    capacity: usize,
    state: Mutex<State>,
    /// Signaled when bytes are freed.
    room: Condvar,
    /// Signaled when a batch arrives, or on close.
    work: Condvar,
    /// Shared with every other queue one transform thread watches, so it can wait across all of them.
    ready: Arc<Ready>,
}

/// What a `pop` found.
pub enum Taken {
    Batch(Handed),
    /// Nothing arrived within the timeout.
    Idle,
    Closed,
}

impl Default for Handoff {
    fn default() -> Self {
        Self::new()
    }
}

impl Handoff {
    pub fn new() -> Handoff {
        Handoff::watched(Arc::new(Ready::new()), HANDOFF_BYTES)
    }

    /// A queue holding `capacity` bytes, whose arrivals are announced on `ready` -- shared with every other queue the
    /// same transform threads watch.
    pub fn watched(ready: Arc<Ready>, capacity: usize) -> Handoff {
        Handoff {
            capacity,
            state: Mutex::new(State {
                queued: VecDeque::new(),
                bytes: 0,
                closed: false,
            }),
            room: Condvar::new(),
            work: Condvar::new(),
            ready,
        }
    }

    /// What this queue's arrivals are announced on.
    pub fn ready(&self) -> &Arc<Ready> {
        &self.ready
    }

    /// Hand a batch over, waiting up to `timeout` for room. A batch larger than the whole budget goes in alone: the cap
    /// stops a second joining it, the same rule `Source::poll_batch` follows for an oversized record.
    pub fn push(&self, batch: Handed, timeout: Duration) -> Result<(), Unsent> {
        let bytes: usize = batch.records.iter().map(SourceRecord::size).sum();
        let deadline = Instant::now() + timeout;
        let mut st = self.state.lock().unwrap();
        while !st.closed && st.bytes + bytes > self.capacity && st.bytes > 0 {
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return Err(Unsent {
                    batch,
                    error: IoError::transient(format!("transform did not take a batch within {timeout:?}")),
                });
            }
            let (guard, _) = self.room.wait_timeout(st, left).unwrap();
            st = guard;
        }
        if st.closed {
            return Err(Unsent {
                batch,
                error: IoError::transient("worker is shutting down"),
            });
        }
        st.bytes += bytes;
        st.queued.push_back(batch);
        self.work.notify_one();
        drop(st);
        self.ready.signal();
        Ok(())
    }

    /// Take the oldest batch, waiting up to `timeout` for one.
    pub fn pop(&self, timeout: Duration) -> Taken {
        let deadline = Instant::now() + timeout;
        let mut st = self.state.lock().unwrap();
        loop {
            if st.closed && st.queued.is_empty() {
                return Taken::Closed;
            }
            if let Some(b) = st.queued.pop_front() {
                let bytes: usize = b.records.iter().map(SourceRecord::size).sum();
                st.bytes = st.bytes.saturating_sub(bytes);
                self.room.notify_all();
                return Taken::Batch(b);
            }
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return Taken::Idle;
            }
            let (guard, _) = self.work.wait_timeout(st, left).unwrap();
            st = guard;
        }
    }

    /// A waiting reader fails; transform drains what is queued, then ends.
    pub fn close(&self) {
        let mut st = self.state.lock().unwrap();
        st.closed = true;
        self.room.notify_all();
        self.work.notify_all();
        drop(st);
        self.ready.signal_all();
    }

    pub fn is_empty(&self) -> bool {
        self.state.lock().unwrap().queued.is_empty()
    }

    /// Bytes of raw records it holds now, as its capacity counts them.
    pub fn bytes(&self) -> usize {
        self.state.lock().unwrap().bytes
    }
}

/// Transform's end of the report channel. Cloneable: every thread in the pool may report to the same reader.
#[derive(Clone)]
pub struct Reports(Sender<Report>);

/// The reader's end.
pub struct Acks(Receiver<Report>);

pub fn reports() -> (Reports, Acks) {
    let (tx, rx) = channel();
    (Reports(tx), Acks(rx))
}

impl Reports {
    /// Never blocks. A reader that has gone away is not an error: its uncommitted batches are redelivered.
    pub fn send(&self, r: Report) {
        let _ = self.0.send(r);
    }
}

impl Acks {
    /// Everything that has arrived, without waiting.
    pub fn drain(&self) -> Vec<Report> {
        let mut out = Vec::new();
        loop {
            match self.0.try_recv() {
                Ok(r) => out.push(r),
                Err(TryRecvError::Empty | TryRecvError::Disconnected) => return out,
            }
        }
    }

    /// Wait up to `timeout` for a report, then everything behind it.
    pub fn wait(&self, timeout: Duration) -> Vec<Report> {
        match self.0.recv_timeout(timeout) {
            Ok(first) => {
                let mut out = vec![first];
                out.extend(self.drain());
                out
            }
            Err(RecvTimeoutError::Timeout | RecvTimeoutError::Disconnected) => Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn rec(offset: i64, bytes: usize) -> SourceRecord {
        SourceRecord {
            chunk: crate::io::RecordChunk::of_bytes(vec![b'x'; bytes]),
            row: 0,
            offset,
            partition: 0,
            topic: "t".into(),
            headers: None,
        }
    }

    fn handed(batch: BatchId, bytes: usize) -> Handed {
        Handed {
            batch,
            records: vec![rec(batch as i64, bytes)],
        }
    }

    fn take(h: &Handoff) -> Handed {
        match h.pop(Duration::from_secs(1)) {
            Taken::Batch(b) => b,
            _ => panic!("expected a batch"),
        }
    }

    /// Poll order is what the commit watermark is a prefix of.
    #[test]
    fn batches_come_out_in_the_order_they_went_in() {
        let h = Handoff::new();
        for i in 0..3 {
            h.push(handed(i, 8), Duration::from_secs(1)).unwrap();
        }
        assert_eq!([take(&h).batch, take(&h).batch, take(&h).batch], [0, 1, 2]);
    }

    /// The batch comes back to the caller: it is tracked, and a tracked batch nobody holds is a commit that never
    /// happens.
    #[test]
    fn a_full_queue_makes_the_reader_wait_and_then_hands_the_batch_back() {
        let h = Handoff::new();
        h.push(handed(0, HANDOFF_BYTES), Duration::from_secs(1)).unwrap();
        let unsent = h
            .push(handed(1, HANDOFF_BYTES), Duration::from_millis(20))
            .expect_err("a full queue must refuse rather than grow");
        assert_eq!(unsent.batch.batch, 1);
    }

    /// A record's headers are memory it holds, so they fill the queue as its text does.
    #[test]
    fn a_records_headers_count_against_the_queue() {
        let h = Handoff::new();
        let mut heavy = handed(0, 1);
        heavy.records[0].headers = Some(vec![("k".to_string(), vec![0; HANDOFF_BYTES])]);
        h.push(heavy, Duration::from_secs(1)).unwrap();
        h.push(handed(1, 1), Duration::from_millis(20))
            .expect_err("the headers filled the queue");
    }

    #[test]
    fn an_oversized_batch_goes_in_alone_rather_than_deadlocking() {
        let h = Handoff::new();
        h.push(handed(0, HANDOFF_BYTES * 3), Duration::from_millis(20))
            .expect("nothing outstanding, so it goes alone");
        assert_eq!(take(&h).batch, 0);
    }

    #[test]
    fn taking_a_batch_frees_its_bytes_for_the_next() {
        let h = Handoff::new();
        h.push(handed(0, HANDOFF_BYTES), Duration::from_secs(1)).unwrap();
        assert_eq!(take(&h).batch, 0);
        h.push(handed(1, HANDOFF_BYTES), Duration::from_millis(20))
            .expect("the first batch's bytes were released when it was taken");
    }

    #[test]
    fn an_idle_source_is_not_an_error() {
        let h = Handoff::new();
        assert!(matches!(h.pop(Duration::from_millis(5)), Taken::Idle));
    }

    /// What is queued is still transform's to deliver and the reader's to commit.
    #[test]
    fn closing_drains_what_is_queued_before_it_ends() {
        let h = Handoff::new();
        h.push(handed(7, 8), Duration::from_secs(1)).unwrap();
        h.close();
        assert_eq!(take(&h).batch, 7);
        assert!(matches!(h.pop(Duration::from_millis(5)), Taken::Closed));
    }

    #[test]
    fn closing_wakes_a_reader_waiting_for_room() {
        let h = Arc::new(Handoff::new());
        h.push(handed(0, HANDOFF_BYTES), Duration::from_secs(1)).unwrap();
        let closer = {
            let h = h.clone();
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(20));
                h.close();
            })
        };
        let started = Instant::now();
        h.push(handed(1, HANDOFF_BYTES), Duration::from_secs(30))
            .expect_err("shutting down");
        assert!(started.elapsed() < Duration::from_secs(5));
        closer.join().unwrap();
    }

    #[test]
    fn reports_arrive_in_order_and_draining_is_not_blocking() {
        let (tx, rx) = reports();
        assert!(rx.drain().is_empty());
        tx.send(Report::Block(BlockCause::Held, "stop".into()));
        tx.send(Report::Acked { batch: 4, dest: 1 });
        let got = rx.drain();
        assert_eq!(got.len(), 2);
        assert!(matches!(&got[0], Report::Block(_, w) if w == "stop"));
        assert!(matches!(got[1], Report::Acked { batch: 4, dest: 1, .. }));
    }

    #[test]
    fn waiting_returns_as_soon_as_a_report_arrives() {
        let (tx, rx) = reports();
        let sender = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(20));
            tx.send(Report::Release);
            tx.send(Report::Release);
        });
        let started = Instant::now();
        let got = rx.wait(Duration::from_secs(10));
        assert!(!got.is_empty(), "wait must return once a report is available");
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "wait must not block for the full timeout once data has arrived"
        );
        sender.join().unwrap();
        // Both sends have completed, so both reports are queued by now, but the first `wait` may have raced the second
        // `send` and drained only one of them -- it is not required to catch both at once, only to lose neither.
        let total = got.len() + rx.drain().len();
        assert_eq!(total, 2, "no report is lost across a wait and a following drain");
    }

    #[test]
    fn a_reader_that_went_away_does_not_fail_the_sender() {
        let (tx, rx) = reports();
        drop(rx);
        tx.send(Report::Block(BlockCause::Held, "nobody is listening".into()));
    }
}
