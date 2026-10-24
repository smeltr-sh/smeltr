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
    (Pool::new(senders, n), peak)
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
    let mut pool = Pool::new(senders, 4);
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
    let mut pool = Pool::new(vec![Blocked(gate.clone()), Blocked(gate.clone())], 2);
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
    let mut pool = Pool::new(vec![Failing(true)], 1);
    pool.submit(7, ()).unwrap();
    let err = pool.drain().expect_err("the failure must surface");
    assert!(err.to_string().contains("cluster said no"));

    let mut pool = Pool::new(vec![Failing(false)], 1);
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

/// Reports a throttle for the first `n` deliveries, then stops.
struct Throttling(Arc<AtomicUsize>);
impl Sender for Throttling {
    type Job = ();
    fn send_one(&mut self, _job: ()) -> Result<Delivered, IoError> {
        let left = self.0.load(Ordering::SeqCst);
        let throttled = left > 0;
        if throttled {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
        Ok(Delivered {
            rejections: Vec::new(),
            throttled,
        })
    }
}

/// Eight senders, starting at eight, that refuse the first `refusals` deliveries between them.
fn refusing_pool(refusals: usize) -> Pool<Throttling> {
    let budget = Arc::new(AtomicUsize::new(refusals));
    Pool::new((0..8).map(|_| Throttling(budget.clone())).collect(), 8)
}

/// Where the limit stands after a single refusal: what one decrease costs.
fn after_one_refusal() -> usize {
    let mut pool = refusing_pool(1);
    pool.submit(0, ()).unwrap();
    pool.drain().unwrap();
    pool.limit()
}

/// One decrease per round of congestion, not one per refusal. Requests already in flight when the limit was lowered
/// met the same overload, so their refusals are that signal arriving again.
#[test]
fn a_burst_of_refusals_lowers_the_limit_once() {
    // Every delivery in the burst is refused: eight requests, one congestion event.
    let mut pool = refusing_pool(8);

    // All eight outstanding before any completes, so every refusal lands against the same limit.
    for batch in 0..8u64 {
        pool.submit(batch, ()).unwrap();
    }
    pool.drain().unwrap();

    let once = after_one_refusal();
    assert!(once < 8, "one refusal lowers the limit: {once}");
    assert_eq!(pool.limit(), once, "eight refusals in one round cost what one does");
}

/// A decrease still answers the next round: settling ends when the requests that predate it have drained.
#[test]
fn congestion_that_outlives_the_round_lowers_it_again() {
    let mut pool = refusing_pool(100);

    for batch in 0..8u64 {
        pool.submit(batch, ()).unwrap();
    }
    pool.drain().unwrap();
    let first = pool.limit();
    assert!(first < 8, "the first round lowers it: {first}");

    // One full round at the lowered limit, every request of it refused.
    for batch in 8..8 + first as u64 {
        pool.submit(batch, ()).unwrap();
    }
    pool.drain().unwrap();
    assert!(
        pool.limit() < first,
        "a fresh round of refusals is answered: {} after {first}",
        pool.limit()
    );
}

#[test]
fn a_throttle_lowers_the_limit_and_recovery_is_slower_than_backoff() {
    // AIMD, and the asymmetry is the point: a throttle is a load signal,
    // so back off fast and recover slowly. Backing off slowly keeps the
    // destination overloaded; recovering fast walks straight back into it.
    let mut pool = refusing_pool(1);
    assert_eq!(pool.limit(), 8, "starts at the sender count");

    pool.submit(0, ()).unwrap();
    pool.drain().unwrap();
    let lowered = pool.limit();
    assert!(lowered < 8, "one throttle lowers it: {lowered}");

    // As many clean completions as the throttle cost must not win it all back.
    for b in 1..=(8 - lowered) as u64 {
        pool.submit(b, ()).unwrap();
        pool.drain().unwrap();
    }
    assert!(
        pool.limit() < 8,
        "recovered as fast as it backed off: {lowered} to {}",
        pool.limit()
    );
}

/// The configured figure is where the loop starts, not where it stops: a destination that never refuses is found out
/// rather than guessed at.
#[test]
fn a_destination_that_never_refuses_is_climbed_to_past_its_starting_point() {
    // A budget nothing exhausts: every completion is clean.
    let budget = Arc::new(AtomicUsize::new(0));
    let mut pool = Pool::new((0..8).map(|_| Throttling(budget.clone())).collect(), 2);
    assert_eq!(pool.limit(), 2, "starts where it was told to, not at the sender count");
    assert_eq!(pool.max_limit(), 8, "and may climb to the senders it owns");

    // Each round of `limit` clean completions buys one more: 2 -> 3 takes two, 3 -> 4 takes three, and so on.
    for batch in 0..40u64 {
        pool.submit(batch, ()).unwrap();
        pool.drain().unwrap();
    }
    assert_eq!(pool.limit(), 8, "and it gets all the way there");
}

/// A start above the senders is the pool's to refuse, not to honor: `limit` is what `submit` gates on, and a limit
/// past the sender count would admit work no sender can take.
#[test]
fn a_start_above_the_sender_count_is_clamped_to_it() {
    let budget = Arc::new(AtomicUsize::new(0));
    let pool = Pool::new((0..2).map(|_| Throttling(budget.clone())).collect(), 99);
    assert_eq!(pool.limit(), 2);
    assert_eq!(pool.max_limit(), 2);
}

#[test]
fn the_limit_never_falls_below_one_or_climbs_past_the_senders() {
    let budget = Arc::new(AtomicUsize::new(100));
    let mut pool = Pool::new((0..4).map(|_| Throttling(budget.clone())).collect(), 4);
    for b in 0..8u64 {
        pool.submit(b, ()).unwrap();
        pool.drain().unwrap();
    }
    assert_eq!(pool.limit(), 1, "sustained throttling floors at one, not zero");

    budget.store(0, Ordering::SeqCst);
    for b in 100..140u64 {
        pool.submit(b, ()).unwrap();
        pool.drain().unwrap();
    }
    assert_eq!(pool.limit(), 4, "and recovery stops at the sender count");
}

// -- convergence ---------------------------------------------------------
//
// The tests above drive the control law a round or two at a time. These drive
// it as a loop instead: hundreds of completions against a destination whose
// capacity is known, asserting on where the limit ends up rather than on what
// one completion does to it. A control loop's failures are shapes -- parking
// at the floor, running away, never coming down -- and a single step cannot
// show any of them.

/// Accepts `capacity` concurrent deliveries and reports a throttle beyond it -- a cluster past its write capacity
/// answers the request and says "less", rather than refusing it.
struct Capacity {
    capacity: Arc<AtomicUsize>,
    concurrent: Arc<AtomicUsize>,
    delay: Duration,
}

impl Sender for Capacity {
    type Job = ();
    fn send_one(&mut self, _job: ()) -> Result<Delivered, IoError> {
        let now = self.concurrent.fetch_add(1, Ordering::SeqCst) + 1;
        let over = now > self.capacity.load(Ordering::SeqCst);
        std::thread::sleep(self.delay);
        self.concurrent.fetch_sub(1, Ordering::SeqCst);
        Ok(Delivered {
            rejections: Vec::new(),
            throttled: over,
        })
    }
}

/// Keep `pool` fed for `rounds` submissions, sampling the limit after each. `submit` blocks at the limit, so this is a
/// pipelined workload rather than a serial one: completions land while later batches are still being handed over.
fn drive(pool: &mut Pool<Capacity>, rounds: u64) -> Vec<usize> {
    let mut samples = Vec::with_capacity(rounds as usize);
    for batch in 0..rounds {
        pool.submit(batch, ()).unwrap();
        let _ = pool.poll().unwrap();
        samples.push(pool.limit());
    }
    let _ = pool.drain().unwrap();
    samples.push(pool.limit());
    samples
}

fn capacity_pool(senders: usize, capacity: &Arc<AtomicUsize>, start: usize) -> Pool<Capacity> {
    let concurrent = Arc::new(AtomicUsize::new(0));
    Pool::new(
        (0..senders)
            .map(|_| Capacity {
                capacity: capacity.clone(),
                concurrent: concurrent.clone(),
                delay: Duration::from_millis(1),
            })
            .collect(),
        start,
    )
}

/// The point of the whole mechanism: a destination that can take more than it was configured for is found out.
#[test]
fn the_loop_climbs_to_a_destinations_real_capacity() {
    let capacity = Arc::new(AtomicUsize::new(8));
    let mut pool = capacity_pool(16, &capacity, 2);

    let samples = drive(&mut pool, 600);
    let settled = &samples[samples.len() / 2..];
    let peak = *settled.iter().max().unwrap();

    assert!(
        peak >= 8,
        "a capacity of 8 was never reached from a start of 2: peaked at {peak}"
    );
    assert!(
        samples.iter().all(|&l| l <= 16),
        "the sender count is the ceiling: {:?}",
        samples.iter().max()
    );
    // AIMD probes upward by design, so it oscillates around capacity rather than resting on it. What must not happen
    // is settling far below -- that is throughput given away for pressure that is not there.
    let mean = settled.iter().sum::<usize>() as f64 / settled.len() as f64;
    assert!(
        mean >= 4.0,
        "the sawtooth should center near a capacity of 8, not collapse below half of it: mean {mean:.1}"
    );
}

/// Reports a throttle every `period`-th delivery regardless of concurrency: a destination under steady mild pressure
/// rather than a burst. Deterministic -- no thread timing decides the outcome.
struct Trickle {
    seen: Arc<AtomicUsize>,
    period: usize,
}

impl Sender for Trickle {
    type Job = ();
    fn send_one(&mut self, _job: ()) -> Result<Delivered, IoError> {
        let n = self.seen.fetch_add(1, Ordering::SeqCst) + 1;
        Ok(Delivered {
            rejections: Vec::new(),
            throttled: n.is_multiple_of(self.period),
        })
    }
}

/// Multiplicative decrease against additive increase: a steady trickle of refusals must reach an equilibrium, not
/// ratchet the limit down to the floor. At a limit of L, one refusal per `period` completions buys `period / L`
/// increases and costs one halving, so the two balance near `sqrt(2 * period)` -- well above 1.
#[test]
fn a_steady_trickle_of_refusals_settles_rather_than_collapsing() {
    let seen = Arc::new(AtomicUsize::new(0));
    let mut pool = Pool::new(
        (0..16)
            .map(|_| Trickle {
                seen: seen.clone(),
                period: 32,
            })
            .collect(),
        16,
    );

    let mut samples = Vec::new();
    for batch in 0..1_500u64 {
        pool.submit(batch, ()).unwrap();
        let _ = pool.poll().unwrap();
        samples.push(pool.limit());
    }
    let _ = pool.drain().unwrap();

    let settled = &samples[samples.len() / 2..];
    let floor = *settled.iter().min().unwrap();
    let mean = settled.iter().sum::<usize>() as f64 / settled.len() as f64;
    assert!(
        floor > 1,
        "5% refusals drove the limit to the floor and left it there: min {floor}, mean {mean:.1}"
    );
    assert!(
        mean >= 3.0,
        "equilibrium against a 1-in-32 refusal rate should sit several requests up, not just above the floor: \
         mean {mean:.1}"
    );
}

/// A destination that loses capacity is followed down. The loop's whole worth is that it answers a cluster getting
/// slower without an operator editing a figure.
#[test]
fn capacity_that_drops_is_followed_down() {
    let capacity = Arc::new(AtomicUsize::new(12));
    let mut pool = capacity_pool(16, &capacity, 12);

    let before = drive(&mut pool, 300);
    let high = before[before.len() / 2..].iter().sum::<usize>() as f64 / (before.len() / 2) as f64;

    capacity.store(2, Ordering::SeqCst);
    let after = drive(&mut pool, 400);
    let low = after[after.len() / 2..].iter().sum::<usize>() as f64 / (after.len() / 2) as f64;

    assert!(
        low < high,
        "capacity fell from 12 to 2 and the limit did not follow: {high:.1} before, {low:.1} after"
    );
    assert!(
        low <= 6.0,
        "a capacity of 2 should hold the limit low, not merely lower: mean {low:.1}"
    );
}

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
    let mut pool = Pool::new(vec![FailsLate, FailsLate, FailsLate], 3);
    pool.submit(1, 0).unwrap();
    pool.submit(2, 51).unwrap();
    pool.submit(3, 100).unwrap();
    std::thread::sleep(Duration::from_millis(300));
    let done = pool.poll().expect("the landed batches come first");
    assert_eq!(done.iter().map(|c| c.batch).collect::<Vec<_>>(), vec![1, 3]);
    pool.poll().expect_err("then the failure");

    let mut pool = Pool::new(vec![FailsLate, FailsLate], 2);
    pool.submit(1, 0).unwrap();
    pool.submit(2, 51).unwrap();
    pool.drain().expect_err("the barrier reports the failure");
    let done = pool.poll().expect("and keeps the landed batch for the next poll");
    assert_eq!(done.iter().map(|c| c.batch).collect::<Vec<_>>(), vec![1]);
}

/// Throttles a job of 1, fails a job of 2 after a short wait, lands anything else.
struct Mixed;
impl Sender for Mixed {
    type Job = u8;
    fn send_one(&mut self, job: u8) -> Result<Delivered, IoError> {
        if job == 2 {
            std::thread::sleep(Duration::from_millis(50));
            return Err(IoError::transient("connection reset"));
        }
        Ok(Delivered {
            rejections: Vec::new(),
            throttled: job == 1,
        })
    }
}

#[test]
fn a_failed_request_ends_its_place_in_the_settling_round() {
    let mut pool = Pool::new(vec![Mixed, Mixed, Mixed, Mixed], 4);
    for job in [1, 2, 2, 2] {
        pool.submit(0, job).unwrap();
    }
    let _ = pool.drain();
    let _ = pool.poll();
    assert_eq!(pool.limit(), 2, "the throttle halved it once");
    // Every request of that round has answered, so a throttle now is new pressure.
    pool.submit(0, 1).unwrap();
    pool.drain().unwrap();
    assert_eq!(pool.limit(), 1);
}
