//! Turning a blocking one-shot delivery into a pipelined one: a destination implements one blocking
//! [`Sender::send_one`] and [`Pool`] supplies `deliver`/`poll_completions` around it.
//!
//! A pool, not one background thread: a single thread makes the call non-blocking but still serializes the round trips,
//! so throughput stays at `1/round-trip`. Overlapping round trips is what moves it.
//!
//! How many may be outstanding is the pacing model's to say (`egress::pacing`): its target in bytes, over the request
//! size, is the limit, never above the sender count. The configured figure is where the limit starts, until the
//! model's first word. A destination that asks to be sent less (429) is obeyed by the sender that was asked, which
//! waits out the backoff, and its completion says it was throttled, so the model caps what it holds below what was in
//! flight then. Not for Kafka: librdkafka pipelines on its own threads.

use std::collections::VecDeque;
use std::sync::mpsc::{Receiver, SyncSender, TryRecvError, sync_channel};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use crate::io::{BatchCompletion, BatchId, IoError, Rejection};

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

    fn send_one(&mut self, job: Self::Job) -> Result<Delivered, IoError>;
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
    done: Receiver<(BatchId, Result<Delivered, IoError>)>,
    /// What a sender spawned later reports on. Dropped once every sender is spawned, so the channel closes when they
    /// have all stopped.
    done_tx: Option<SyncSender<(BatchId, Result<Delivered, IoError>)>>,
    handles: Vec<JoinHandle<()>>,
    /// Senders not yet given a thread: the limit has not reached them.
    parked: Vec<S>,
    /// Asked before a sender past those spawned at the start gets its thread; a refusal leaves it parked and the
    /// limit at the senders spawned. `None` spawns freely.
    lend: Option<crate::io::Lend>,
    /// Senders spawned at the start, which nothing is asked for.
    started: usize,
    outstanding: usize,
    /// How many requests may be outstanding right now -- the pacing target over the request size, never above the
    /// number of senders.
    limit: usize,
    max_limit: usize,
    /// Bytes one request carries, which turns the target into a count.
    request_bytes: usize,
    /// Completions collected while `submit` was waiting for room, held for the next `poll`.
    stashed: Vec<BatchCompletion>,
    /// A failure `poll` took off the channel beside completions it returned first, held for the next call.
    failed: Option<IoError>,
}

impl<S: Sender> Pool<S> {
    /// One thread per sender, each owning its own connection, spawned as the limit reaches it and kept from then on. A
    /// sender idle for longer than `IDLE` hands back the pages its last request freed.
    ///
    /// `start` is where the limit begins, clamped into the pool, until the pacing model's first word
    /// ([`Pool::pace`]); `request_bytes` is what one request carries.
    pub fn new(senders: Vec<S>, start: usize, request_bytes: usize) -> Pool<S> {
        assert!(!senders.is_empty(), "a pool needs at least one sender");
        let jobs = Arc::new(Jobs {
            waiting: Mutex::new((VecDeque::new(), false)),
            arrived: Condvar::new(),
        });
        let (done_tx, done_rx) = sync_channel(senders.len() * 4);
        let max_limit = senders.len();
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
            limit: start.clamp(1, max_limit),
            max_limit,
            request_bytes: request_bytes.max(1),
            stashed: Vec::new(),
            failed: None,
        };
        pool.spawn_to_limit();
        pool.started = pool.handles.len();
        pool
    }

    /// Ask `lend` before each sender spawned past those spawned at the start.
    pub fn lend_senders(&mut self, lend: crate::io::Lend) {
        self.lend = Some(lend);
    }

    /// Give each sender under the limit its thread.
    fn spawn_to_limit(&mut self) {
        while self.handles.len() < self.limit && !self.parked.is_empty() {
            if self.handles.len() >= self.started
                && let Some(lend) = &mut self.lend
                && !lend()
            {
                break;
            }
            let (Some(mut s), Some(done_tx)) = (self.parked.pop(), self.done_tx.clone()) else {
                break;
            };
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
        // No more than the senders that have a thread may be outstanding.
        self.limit = self.limit.min(self.handles.len());
    }

    /// Senders given a thread so far.
    pub fn spawned(&self) -> usize {
        self.handles.len()
    }

    /// The most requests this pool will ever allow outstanding: its sender count.
    pub fn max_limit(&self) -> usize {
        self.max_limit
    }

    /// The pacing target moved: as many requests as `target_bytes` holds of them, at least one, at most the senders.
    pub fn pace(&mut self, target_bytes: usize) {
        self.limit = (target_bytes / self.request_bytes).clamp(1, self.max_limit);
        self.spawn_to_limit();
    }

    /// Requests currently allowed outstanding.
    pub fn limit(&self) -> usize {
        self.limit
    }

    /// Take one completion off the channel, blocking.
    fn recv_one(&mut self) -> Result<(), IoError> {
        match self.done.recv() {
            Ok((batch, Ok(d))) => {
                self.outstanding -= 1;
                self.stashed.push(BatchCompletion {
                    batch,
                    rejections: d.rejections,
                    throttled: d.throttled,
                });
                Ok(())
            }
            Ok((_, Err(e))) => {
                self.outstanding -= 1;
                Err(e)
            }
            Err(_) => Err(IoError::transient("every destination sender thread has stopped")),
        }
    }

    /// Hand `job` over as `batch`. Blocks while the limit is reached.
    pub fn submit(&mut self, batch: BatchId, job: S::Job) -> Result<(), IoError> {
        if let Some(e) = self.failed.take() {
            return Err(e);
        }
        while self.outstanding >= self.limit {
            self.recv_one()?;
        }
        // Senders that have all stopped are found out when their completions channel closes.
        self.jobs.push((batch, job));
        self.outstanding += 1;
        Ok(())
    }

    /// Completions that have arrived, without waiting. A failure is returned as `Err` once the completions that arrived
    /// before it have been returned: a batch that landed is reported whatever became of the others.
    pub fn poll(&mut self) -> Result<Vec<BatchCompletion>, IoError> {
        if self.stashed.is_empty()
            && let Some(e) = self.failed.take()
        {
            return Err(e);
        }
        let mut out = std::mem::take(&mut self.stashed);
        loop {
            match self.done.try_recv() {
                Ok((batch, Ok(d))) => {
                    self.outstanding -= 1;
                    out.push(BatchCompletion {
                        batch,
                        rejections: d.rejections,
                        throttled: d.throttled,
                    });
                }
                Ok((_, Err(e))) => {
                    self.outstanding -= 1;
                    // Keep reading: a batch that landed behind the failure is reported now too.
                    self.failed.get_or_insert(e);
                }
                Err(TryRecvError::Empty) => {
                    return match self.failed.take() {
                        Some(e) if out.is_empty() => Err(e),
                        held => {
                            self.failed = held;
                            Ok(out)
                        }
                    };
                }
                Err(TryRecvError::Disconnected) => {
                    self.stashed = out;
                    return Err(IoError::transient("every destination sender thread has stopped"));
                }
            }
        }
    }

    /// Wait for everything outstanding: the barrier, for `Destination::flush`.
    pub fn drain(&mut self) -> Result<Vec<BatchCompletion>, IoError> {
        let mut out = std::mem::take(&mut self.stashed);
        let mut failure = self.failed.take();
        while self.outstanding > 0 {
            match self.done.recv() {
                Ok((batch, Ok(d))) => {
                    self.outstanding -= 1;
                    out.push(BatchCompletion {
                        batch,
                        rejections: d.rejections,
                        throttled: d.throttled,
                    });
                }
                Ok((_, Err(e))) => {
                    self.outstanding -= 1;
                    // Keep draining: results left in the channel would be reported against the next batch.
                    failure.get_or_insert(e);
                }
                Err(_) => {
                    self.stashed = out;
                    return Err(IoError::transient("every destination sender thread has stopped"));
                }
            }
        }
        match failure {
            // Kept for the next `poll`.
            Some(e) => {
                self.stashed = out;
                Err(e)
            }
            None => Ok(out),
        }
    }

    pub fn outstanding(&self) -> usize {
        self.outstanding
    }
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
