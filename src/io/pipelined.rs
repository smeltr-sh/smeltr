//! Turning a blocking one-shot delivery into a pipelined one: a destination implements one blocking
//! [`Sender::send_one`] and [`Pool`] supplies `deliver`/`poll_completions` around it.
//!
//! A pool, not one background thread: a single thread makes the call non-blocking but still serializes the round trips,
//! so throughput stays at `1/round-trip`. Overlapping round trips is what moves it.
//!
//! The pool sets no limit of its own. What is in flight is bounded by the egress queue in front of it, which holds
//! the pacing target (`egress::pacing`) until each batch is acknowledged; the pool sends every request it is handed,
//! a batch's side by side, and reports the batch once its last request is answered. It spawns a sender when every one
//! it has is busy, up to the sender count; the configured figure is how many it starts with. A destination that asks
//! to be sent less (429) is obeyed by the sender that was asked, which waits out the backoff, and its completion says
//! it was throttled, so the model caps what it holds below what was in flight then. Not for Kafka: librdkafka
//! pipelines on its own threads.

use std::collections::{HashMap, VecDeque};
use std::sync::mpsc::{Receiver, SyncSender, TryRecvError, sync_channel};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use crate::io::{BatchCompletion, BatchId, IoError, Rejection, Undelivered};

/// What one delivery attempt produced.
pub struct Delivered {
    /// Documents the destination examined and refused. Not an error: see `Destination::deliver`.
    pub rejections: Vec<Rejection>,
    /// The destination asked to be sent less (429, 503, a per-item `es_rejected_execution_exception`). Reported rather
    /// than inferred from timing: a slow cluster and a throttled one call for opposite responses.
    pub throttled: bool,
}

/// One blocking delivery. Implementors do a single round trip and say what the destination refused; retrying a
/// *transient* failure inside this call is the implementor's business, since only it knows what is safe to repeat.
pub trait Sender: Send + 'static {
    /// Everything one batch needs to be delivered, owned -- it crosses a thread boundary.
    type Job: Send + 'static;

    /// A failure names the batch's rows the job did not land, so only they are sent again, and carries the refusals
    /// it had already seen, so none is lost.
    fn send_one(&mut self, job: Self::Job) -> Result<Delivered, Failed>;
}

/// A job that did not land whole.
pub struct Failed {
    pub undelivered: Undelivered,
    /// Refusals the job saw before it failed.
    pub rejections: Vec<Rejection>,
}

/// How long a sender waits for a job before handing its free pages back, so what the last request freed has had its
/// chance to be reused.
const IDLE: Duration = Duration::from_millis(100);

/// Jobs waiting for a sender. A queue under a condvar rather than a channel, so every idle sender waits at once and each
/// notices on its own that it has gone idle; a shared channel receiver lets one sender wait while the rest queue on its
/// lock.
struct Jobs<J> {
    waiting: Mutex<(VecDeque<(BatchId, J)>, bool)>,
    arrived: Condvar,
}

impl<J> Jobs<J> {
    fn push(&self, job: (BatchId, J)) {
        self.waiting.lock().unwrap().0.push_back(job);
        self.arrived.notify_one();
    }

    fn close(&self) {
        self.waiting.lock().unwrap().1 = true;
        self.arrived.notify_all();
    }

    /// The next job, or `None` once closed. Hands this thread's free pages back once per idle stretch.
    fn take(&self) -> Option<(BatchId, J)> {
        let mut waiting = self.waiting.lock().unwrap();
        let mut released = false;
        loop {
            if let Some(job) = waiting.0.pop_front() {
                return Some(job);
            }
            if waiting.1 {
                return None;
            }
            if released {
                waiting = self.arrived.wait(waiting).unwrap();
                continue;
            }
            let (held, timeout) = self.arrived.wait_timeout(waiting, IDLE).unwrap();
            waiting = held;
            if timeout.timed_out() && waiting.0.is_empty() {
                drop(waiting);
                crate::core::buffer::release_idle();
                released = true;
                waiting = self.waiting.lock().unwrap();
            }
        }
    }
}

/// Runs several [`Sender`]s concurrently, keyed by batch.
pub struct Pool<S: Sender> {
    jobs: Arc<Jobs<S::Job>>,
    done: Receiver<(BatchId, Result<Delivered, Failed>)>,
    /// What a sender spawned later reports on. Dropped once every sender is spawned, so the channel closes when they
    /// have all stopped.
    done_tx: Option<SyncSender<(BatchId, Result<Delivered, Failed>)>>,
    handles: Vec<JoinHandle<()>>,
    /// Senders not yet given a thread: none has been busy enough to need them.
    parked: Vec<S>,
    /// Asked before a sender past those spawned at the start gets its thread; a refusal leaves it parked, and a batch
    /// waits for a spawned sender to come free. `None` spawns freely.
    lend: Option<crate::io::Lend>,
    /// Senders spawned at the start, which nothing is asked for.
    started: usize,
    /// Requests handed to a sender and not yet answered: never more than the senders spawned.
    outstanding: usize,
    /// Each batch with a request not yet answered: what its answered requests said so far.
    batches: HashMap<BatchId, Answers>,
    /// Completions collected while `submit` was waiting for room, held for the next `poll`.
    stashed: Vec<BatchCompletion>,
}

impl<S: Sender> Pool<S> {
    /// One thread per sender, each owning its own connection, spawned when every spawned one is busy and kept from
    /// then on. A sender idle for longer than `IDLE` hands back the pages its last request freed.
    ///
    /// `start` senders are spawned now, clamped into the pool.
    pub fn new(senders: Vec<S>, start: usize) -> Pool<S> {
        assert!(!senders.is_empty(), "a pool needs at least one sender");
        let jobs = Arc::new(Jobs {
            waiting: Mutex::new((VecDeque::new(), false)),
            arrived: Condvar::new(),
        });
        let (done_tx, done_rx) = sync_channel(senders.len() * 4);
        let mut parked = senders;
        // Spawned last first: the first sender is the first spawned.
        parked.reverse();
        let mut pool = Pool {
            jobs,
            done: done_rx,
            done_tx: Some(done_tx),
            handles: Vec::new(),
            parked,
            lend: None,
            started: 0,
            outstanding: 0,
            batches: HashMap::new(),
            stashed: Vec::new(),
        };
        for _ in 0..start.max(1) {
            pool.spawn_one();
        }
        pool.started = pool.handles.len();
        pool
    }

    /// Ask `lend` before each sender spawned past those spawned at the start.
    pub fn lend_senders(&mut self, lend: crate::io::Lend) {
        self.lend = Some(lend);
    }

    /// Give one more sender its thread, if one is parked and, past those spawned at the start, `lend` allows it.
    fn spawn_one(&mut self) {
        if self.parked.is_empty()
            || (self.handles.len() >= self.started && self.lend.as_mut().is_some_and(|lend| !lend()))
        {
            return;
        }
        if let (Some(mut s), Some(done_tx)) = (self.parked.pop(), self.done_tx.clone()) {
            let jobs = self.jobs.clone();
            self.handles.push(
                std::thread::Builder::new()
                    .name("smeltr-destination-sender".into())
                    .spawn(move || {
                        while let Some((batch, job)) = jobs.take() {
                            let outcome = s.send_one(job);
                            if done_tx.send((batch, outcome)).is_err() {
                                break;
                            }
                        }
                    })
                    .expect("spawn destination sender"),
            );
        }
        if self.parked.is_empty() {
            self.done_tx = None;
        }
    }

    /// Senders given a thread so far.
    pub fn spawned(&self) -> usize {
        self.handles.len()
    }

    /// Take one completion off the channel, blocking.
    fn recv_one(&mut self) -> Result<(), IoError> {
        match self.done.recv() {
            Ok((batch, outcome)) => {
                self.answered(batch, outcome);
                Ok(())
            }
            Err(_) => Err(IoError::transient("every destination sender thread has stopped")),
        }
    }

    /// Hand `jobs` over as `batch`, each to a sender of its own. The batch completes once, when the last of them is
    /// answered: with every refusal they report, and the rows of any that failed as undelivered. Blocks while every
    /// sender is busy and no more may be spawned. `jobs` is never empty: no answer would complete a batch of none.
    pub fn submit(&mut self, batch: BatchId, jobs: Vec<S::Job>) -> Result<(), IoError> {
        debug_assert!(!jobs.is_empty(), "a batch of no requests is never answered");
        // Counted before any is sent: an answer that comes while a later one waits for a sender must not complete it.
        let count = jobs.len();
        self.batches.entry(batch).or_default().open += count;
        for (sent, job) in jobs.into_iter().enumerate() {
            if self.outstanding >= self.handles.len() {
                // A sender whose answer has arrived is free: spawn only when none is.
                self.take_arrived();
            }
            if self.outstanding >= self.handles.len() {
                self.spawn_one();
            }
            while self.outstanding >= self.handles.len() {
                if let Err(e) = self.recv_one() {
                    // The rest are dropped: the batch completes on those already sent, or is forgotten if none was.
                    if let Some(answers) = self.batches.get_mut(&batch) {
                        answers.open -= count - sent;
                        if answers.open == 0 {
                            self.batches.remove(&batch);
                        }
                    }
                    return Err(e);
                }
            }
            // Senders that have all stopped are found out when their completions channel closes.
            self.jobs.push((batch, job));
            self.outstanding += 1;
        }
        Ok(())
    }

    /// One request of `batch` answered. Its batch completes into `stashed` once none is left open.
    fn answered(&mut self, batch: BatchId, outcome: Result<Delivered, Failed>) {
        self.outstanding -= 1;
        let answers = self.batches.entry(batch).or_default();
        answers.open = answers.open.saturating_sub(1);
        match outcome {
            Ok(d) => {
                answers.rejections.extend(d.rejections);
                answers.throttled |= d.throttled;
            }
            Err(f) => {
                answers.rejections.extend(f.rejections);
                match &mut answers.undelivered {
                    Some(u) => u.rows.extend(f.undelivered.rows),
                    None => answers.undelivered = Some(f.undelivered),
                }
            }
        }
        if answers.open > 0 {
            return;
        }
        let answers = self.batches.remove(&batch).unwrap_or_default();
        self.stashed.push(BatchCompletion {
            batch,
            rejections: answers.rejections,
            throttled: answers.throttled,
            undelivered: answers.undelivered,
        });
    }

    /// Completions that have arrived, without waiting. `Err` only once every sender has stopped: a failed request is
    /// its batch's completion, naming what did not land.
    pub fn poll(&mut self) -> Result<Vec<BatchCompletion>, IoError> {
        let open = self.take_arrived();
        let out = std::mem::take(&mut self.stashed);
        if !open {
            self.stashed = out;
            return Err(IoError::transient("every destination sender thread has stopped"));
        }
        Ok(out)
    }

    /// Take every answer that has arrived, without waiting, into `stashed`. False once every sender has stopped.
    fn take_arrived(&mut self) -> bool {
        loop {
            match self.done.try_recv() {
                Ok((batch, outcome)) => self.answered(batch, outcome),
                Err(TryRecvError::Empty) => return true,
                Err(TryRecvError::Disconnected) => return false,
            }
        }
    }

    /// Wait for everything outstanding: the barrier, for `Destination::flush`.
    pub fn drain(&mut self) -> Result<Vec<BatchCompletion>, IoError> {
        while self.outstanding > 0 {
            match self.done.recv() {
                Ok((batch, outcome)) => self.answered(batch, outcome),
                Err(_) => return Err(IoError::transient("every destination sender thread has stopped")),
            }
        }
        Ok(std::mem::take(&mut self.stashed))
    }

    pub fn outstanding(&self) -> usize {
        self.outstanding
    }
}

/// What the answered requests of one batch said, while any of its requests is open.
#[derive(Default)]
struct Answers {
    open: usize,
    rejections: Vec<Rejection>,
    throttled: bool,
    /// Every failed request's rows, under the first one's error.
    undelivered: Option<Undelivered>,
}

impl<S: Sender> Drop for Pool<S> {
    fn drop(&mut self) {
        // Closing the job queue ends the sender loops.
        self.jobs.close();
        for h in self.handles.drain(..) {
            let _ = h.join();
        }
    }
}
