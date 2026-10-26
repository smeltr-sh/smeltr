//! `io::pipelined::Pool` — the shared machinery a blocking destination
//! gets `dispatch`/`poll_completions` from.

use smeltr::io::pipelined::{Delivered, Pool, Sender};
use smeltr::io::{IoError, Rejection};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

/// A sender that sleeps, so overlap is observable as wall time.
struct Slow {
    delay: Duration,
    concurrent: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
}

impl Sender for Slow {
    type Job = u32;
    fn send_one(&mut self, _job: u32) -> Result<Delivered, IoError> {
        let now = self.concurrent.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(now, Ordering::SeqCst);
        std::thread::sleep(self.delay);
        self.concurrent.fetch_sub(1, Ordering::SeqCst);
        Ok(Delivered {
            rejections: Vec::new(),
            throttled: false,
        })
    }
}

fn slow_pool(n: usize, delay: Duration) -> (Pool<Slow>, Arc<AtomicUsize>) {
    let concurrent = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let senders = (0..n)
        .map(|_| Slow {
            delay,
            concurrent: concurrent.clone(),
            peak: peak.clone(),
        })
        .collect();
    (Pool::new(senders, n, MIB), peak)
}

/// A sender that holds each delivery until `n` are in flight at once, or
/// until a deadline, so overlap is observed directly rather than as wall time.
struct Meeting {
    n: usize,
    concurrent: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
}

impl Sender for Meeting {
    type Job = u32;
    fn send_one(&mut self, _job: u32) -> Result<Delivered, IoError> {
        let now = self.concurrent.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(now, Ordering::SeqCst);
        // Generous, so a loaded machine's late thread still arrives; a pool
        // that serializes waits it out once per delivery and fails below.
        let deadline = Instant::now() + Duration::from_secs(5);
        while self.peak.load(Ordering::SeqCst) < self.n && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(1));
        }
        self.concurrent.fetch_sub(1, Ordering::SeqCst);
        Ok(Delivered {
            rejections: Vec::new(),
            throttled: false,
        })
    }
}

#[test]
fn round_trips_actually_overlap() {
    // The property the whole module exists for. Four deliveries through four
    // senders must be in flight at once: if they are not, the pool is a
    // single background thread with extra steps, which buys nothing against
    // a destination whose cost is one round trip.
    let concurrent = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let senders = (0..4)
        .map(|_| Meeting {
            n: 4,
            concurrent: concurrent.clone(),
            peak: peak.clone(),
        })
        .collect();
    let mut pool = Pool::new(senders, 4, MIB);
    for b in 0..4u64 {
        pool.submit(b, b as u32).unwrap();
    }
    let done = pool.drain().unwrap();

    assert_eq!(done.len(), 4);
    assert_eq!(peak.load(Ordering::SeqCst), 4, "all four ran at once");
}

#[test]
fn one_sender_serializes() {
    // The contrast, so the test above is measuring the pool and not the
    // machine being fast.
    let (mut pool, peak) = slow_pool(1, Duration::from_millis(60));
    let t = Instant::now();
    for b in 0..3u64 {
        pool.submit(b, b as u32).unwrap();
    }
    pool.drain().unwrap();
    assert_eq!(peak.load(Ordering::SeqCst), 1);
    assert!(t.elapsed() >= Duration::from_millis(150), "one sender cannot overlap");
}

/// Held until `release` is set. Deliberately not a `Barrier`: a barrier
/// is re-armed after it trips, so a sender that took a second job would
/// block on it again with nobody left to release it -- which deadlocked
/// this test the first time.
struct Blocked(Arc<AtomicBool>);
impl Sender for Blocked {
    type Job = ();
    fn send_one(&mut self, _job: ()) -> Result<Delivered, IoError> {
        while !self.0.load(Ordering::SeqCst) {
            std::thread::sleep(Duration::from_millis(2));
        }
        Ok(Delivered {
            rejections: Vec::new(),
            throttled: false,
        })
    }
}

#[test]
fn submit_blocks_once_every_sender_is_busy() {
    // Backpressure. Without it a worker outrunning its destination queues
    // batches until it runs out of memory -- the same stall, wearing a
    // different hat.
    let gate = Arc::new(AtomicBool::new(false));
    let mut pool = Pool::new(vec![Blocked(gate.clone()), Blocked(gate.clone())], 2, MIB);
    // The bound is the concurrency limit, which starts at the sender
    // count -- so two fill it and the third has to wait for one to finish.
    for b in 0..2u64 {
        pool.submit(b, ()).unwrap();
    }

    let submitted = Arc::new(AtomicUsize::new(0));
    let s2 = submitted.clone();
    let handle = std::thread::spawn(move || {
        pool.submit(2, ()).unwrap();
        s2.store(1, Ordering::SeqCst);
        pool
    });
    std::thread::sleep(Duration::from_millis(150));
    assert_eq!(
        submitted.load(Ordering::SeqCst),
        0,
        "submit returned while every sender was busy and the queue full"
    );
    gate.store(true, Ordering::SeqCst);
    let mut pool = handle.join().unwrap();
    pool.drain().unwrap();
}

struct Failing(bool);
impl Sender for Failing {
    type Job = ();
    fn send_one(&mut self, _job: ()) -> Result<Delivered, IoError> {
        if self.0 {
            Err(IoError::transient("cluster said no"))
        } else {
            Ok(Delivered {
                rejections: vec![Rejection {
                    doc: 2,
                    reason: "mapping conflict".into(),
                    record: None,
                }],
                throttled: false,
            })
        }
    }
}

#[test]
fn a_failure_surfaces_and_rejections_come_back_with_their_batch() {
    let mut pool = Pool::new(vec![Failing(true)], 1, MIB);
    pool.submit(7, ()).unwrap();
    let err = pool.drain().expect_err("the failure must surface");
    assert!(err.to_string().contains("cluster said no"));

    let mut pool = Pool::new(vec![Failing(false)], 1, MIB);
    pool.submit(9, ()).unwrap();
    let done = pool.drain().unwrap();
    assert_eq!(done.len(), 1);
    assert_eq!(done[0].batch, 9, "attributed to the batch that produced it");
    assert_eq!(done[0].rejections.len(), 1);
    assert_eq!(done[0].rejections[0].doc, 2);
}

#[test]
fn dropping_the_pool_does_not_hang() {
    // The sender loops end when the job channel closes; without that the
    // join in `Drop` waits on a `recv` that never returns.
    let (pool, _) = slow_pool(2, Duration::from_millis(1));
    drop(pool);
}

/// Lands at once.
struct Instant0;
impl Sender for Instant0 {
    type Job = ();
    fn send_one(&mut self, _job: ()) -> Result<Delivered, IoError> {
        Ok(Delivered {
            rejections: Vec::new(),
            throttled: false,
        })
    }
}

/// The configured figure is where the limit starts; the pacing target is what moves it.
#[test]
fn the_limit_starts_where_it_was_told_and_follows_the_pacing_target() {
    let mut pool = Pool::new((0..8).map(|_| Instant0).collect(), 2, MIB);
    assert_eq!(pool.limit(), 2, "starts where it was told to, not at the sender count");
    assert_eq!(pool.max_limit(), 8, "and may climb to the senders it owns");
    pool.pace(5 * MIB);
    assert_eq!(pool.limit(), 5, "the target over the request size");
    pool.pace(5 * MIB + MIB / 2);
    assert_eq!(pool.limit(), 5, "whole requests only");
    pool.pace(0);
    assert_eq!(pool.limit(), 1, "never below one: a destination always moves");
    pool.pace(1024 * MIB);
    assert_eq!(pool.limit(), 8, "never past the senders it owns");
}

/// A start above the senders is the pool's to refuse, not to honor: `limit` is what `submit` gates on, and a limit
/// past the sender count would admit work no sender can take.
#[test]
fn a_start_above_the_sender_count_is_clamped_to_it() {
    let pool = Pool::new((0..2).map(|_| Instant0).collect(), 99, MIB);
    assert_eq!(pool.limit(), 2);
    assert_eq!(pool.max_limit(), 2);
}

const MIB: usize = 1024 * 1024;

/// Answers after the job's wait in milliseconds, failing an odd one.
struct FailsLate;
impl Sender for FailsLate {
    type Job = u64;
    fn send_one(&mut self, wait: u64) -> Result<Delivered, IoError> {
        std::thread::sleep(Duration::from_millis(wait));
        if wait % 2 == 1 {
            return Err(IoError::transient("cluster said no"));
        }
        Ok(Delivered {
            rejections: Vec::new(),
            throttled: false,
        })
    }
}

#[test]
fn a_batch_that_landed_is_reported_before_a_failure_beside_it() {
    // Landed, failed, landed, in that order on the channel.
    let mut pool = Pool::new(vec![FailsLate, FailsLate, FailsLate], 3, MIB);
    pool.submit(1, 0).unwrap();
    pool.submit(2, 51).unwrap();
    pool.submit(3, 100).unwrap();
    std::thread::sleep(Duration::from_millis(300));
    let done = pool.poll().expect("the landed batches come first");
    assert_eq!(done.iter().map(|c| c.batch).collect::<Vec<_>>(), vec![1, 3]);
    pool.poll().expect_err("then the failure");

    let mut pool = Pool::new(vec![FailsLate, FailsLate], 2, MIB);
    pool.submit(1, 0).unwrap();
    pool.submit(2, 51).unwrap();
    pool.drain().expect_err("the barrier reports the failure");
    let done = pool.poll().expect("and keeps the landed batch for the next poll");
    assert_eq!(done.iter().map(|c| c.batch).collect::<Vec<_>>(), vec![1]);
}
