//! main's model (`old`) and the BBRv3 port, run over the same simulated paths, each path built so that one of the
//! port's mechanisms has its stated reason to act. Run with
//! `cargo nextest run --lib egress::pacing::compare --run-ignored only --no-capture`.

use std::time::{Duration, Instant};

use super::old;
use super::{Completion, Gate, Pacing, Sent};

const MIB: usize = 1024 * 1024;
const GIB: f64 = 1024.0 * 1024.0 * 1024.0;
const BATCH: usize = 4 * MIB;
const CEILING: usize = 1024 * MIB;

enum G {
    Send,
    Wait(Duration),
    Full,
}

trait Model {
    type Token: Copy;
    fn gate(&mut self, held: u64, now: Instant) -> G;
    fn dispatched(&mut self, now: Instant, pressed: bool) -> Self::Token;
    fn completed(&mut self, t: Self::Token, throttled: bool, now: Instant, nothing_to_send: bool);
    /// The batch came back undelivered, to be sent again as a new one: `throttled` when the destination was asked
    /// to send less, else a failure that says nothing of the path.
    fn failed(&mut self, t: Self::Token, throttled: bool, now: Instant);
    fn idle(&mut self);
    fn budget(&self) -> usize;
    fn rate(&self) -> f64;
}

struct New(Pacing);

impl Model for New {
    type Token = Sent;
    fn gate(&mut self, _held: u64, now: Instant) -> G {
        match self.0.gate(BATCH, now) {
            Gate::Send => G::Send,
            Gate::Wait(d) => G::Wait(d),
            Gate::Full => G::Full,
        }
    }
    fn dispatched(&mut self, now: Instant, _pressed: bool) -> Sent {
        self.0.dispatched(BATCH, now, now)
    }
    fn completed(&mut self, sent: Sent, throttled: bool, now: Instant, nothing_to_send: bool) {
        self.0.check_app_limited(nothing_to_send);
        self.0.completed(
            Completion {
                bytes: BATCH,
                sent,
                throttled,
            },
            now,
        );
    }
    fn failed(&mut self, sent: Sent, throttled: bool, now: Instant) {
        self.0.abandoned(
            Completion {
                bytes: BATCH,
                sent,
                throttled,
            },
            0,
            now,
        );
    }
    fn idle(&mut self) {
        self.0.check_app_limited(true);
    }
    fn budget(&self) -> usize {
        self.0.queue_bytes()
    }
    fn rate(&self) -> f64 {
        self.0.rate()
    }
}

/// main's model as main's egress drives it: a batch goes while what is in flight and it fit the target, and a
/// sample is app-limited when the source was not refused room before its batch went.
struct Old(old::Pacing);

impl Model for Old {
    type Token = (old::Sent, bool);
    fn gate(&mut self, held: u64, _now: Instant) -> G {
        if held == 0 || held + BATCH as u64 <= self.0.target() as u64 {
            G::Send
        } else {
            G::Full
        }
    }
    fn dispatched(&mut self, now: Instant, pressed: bool) -> (old::Sent, bool) {
        (self.0.dispatched(BATCH, now), !pressed)
    }
    fn completed(&mut self, (sent, app_limited): (old::Sent, bool), throttled: bool, now: Instant, _: bool) {
        self.0.completed(
            old::Completion {
                bytes: BATCH,
                sent,
                app_limited,
                throttled,
            },
            now,
        );
    }
    fn failed(&mut self, (sent, app_limited): (old::Sent, bool), throttled: bool, _now: Instant) {
        self.0.abandoned(
            old::Completion {
                bytes: BATCH,
                sent,
                app_limited,
                throttled,
            },
            0,
        );
    }
    fn idle(&mut self) {}
    fn budget(&self) -> usize {
        self.0.queue_bytes()
    }
    fn rate(&self) -> f64 {
        self.0.rate()
    }
}

/// When a flow's source has a batch.
#[derive(Clone, Copy)]
enum Source {
    /// Always.
    Backlogged,
    /// `rate` bytes a second.
    Rate(f64),
    /// Backlogged for `on` seconds, then nothing for `off`, over and over, starting on.
    OnOff { on: f64, off: f64 },
}

/// The path every flow shares: a bottleneck whose capacity follows `capacity` (seconds from the start, bytes a
/// second; the last step at or before now holds), a round trip of `rtt` besides, a destination that refuses a batch
/// while more than `limit` (by the same kind of schedule) is out across flows or while its own queue holds more than
/// `buffer`, and completions seen only at the next multiple of `slot`. A refused batch is sent again after
/// `retry`, and goes through the bottleneck then. One in `fail_every` batches fails outright after a round trip.
#[derive(Clone)]
struct Path {
    capacity: Vec<(f64, f64)>,
    rtt: Duration,
    limit: Vec<(f64, Option<u64>)>,
    buffer: Option<f64>,
    slot: Option<Duration>,
    retry: Duration,
    fail_every: Option<u64>,
    /// A refusal comes back to egress as a failed batch, to be sent again as a new one, instead of the destination
    /// retrying it and reporting it throttled once it lands.
    refusal_fails: bool,
}

impl Path {
    fn new(capacity: f64, rtt_ms: u64) -> Path {
        Path {
            capacity: vec![(0.0, capacity)],
            rtt: Duration::from_millis(rtt_ms),
            limit: vec![(0.0, None)],
            buffer: None,
            slot: None,
            retry: Duration::from_millis(50),
            fail_every: None,
            refusal_fails: false,
        }
    }

    fn at<T: Copy>(steps: &[(f64, T)], t: f64) -> T {
        steps
            .iter()
            .rev()
            .find(|(at, _)| *at <= t)
            .map_or(steps[0].1, |(_, v)| *v)
    }
}

/// A batch on its way to the bottleneck: it enters it at `enters`, or is refused there.
struct Waiting<T> {
    enters: Instant,
    token: T,
    sent_at: Instant,
    throttled: bool,
}

struct Out<T> {
    at: Instant,
    seen: Instant,
    token: T,
    sent_at: Instant,
    throttled: bool,
    failed: bool,
}

struct Flow<M: Model> {
    model: M,
    source: Source,
    source_next: Instant,
    /// Whether the source was refused room since the last batch went.
    pressed: bool,
    held: u64,
    waiting: Vec<Waiting<M::Token>>,
    out: Vec<Out<M::Token>>,
    acks: Vec<(Instant, u64)>,
    refused: Vec<Instant>,
    latencies: Vec<(Instant, Duration)>,
    budget: Vec<(Instant, usize)>,
}

impl<M: Model> Flow<M> {
    fn has_batch(&self, start: Instant, now: Instant) -> bool {
        match self.source {
            Source::Backlogged => true,
            Source::Rate(_) => now >= self.source_next,
            Source::OnOff { on, off } => (now - start).as_secs_f64() % (on + off) < on,
        }
    }
}

struct Sim<M: Model> {
    path: Path,
    start: Instant,
    now: Instant,
    link_free: Instant,
    sent: u64,
    flows: Vec<Flow<M>>,
    /// (when, bytes queued at the bottleneck) at each send.
    queue: Vec<(Instant, f64)>,
}

impl<M: Model> Sim<M> {
    fn new(path: Path, flows: Vec<(M, Source)>) -> Sim<M> {
        let start = Instant::now();
        Sim {
            path,
            start,
            now: start,
            link_free: start,
            sent: 0,
            queue: Vec::new(),
            flows: flows
                .into_iter()
                .map(|(model, source)| Flow {
                    model,
                    source,
                    source_next: start,
                    pressed: false,
                    held: 0,
                    waiting: Vec::new(),
                    out: Vec::new(),
                    acks: Vec::new(),
                    refused: Vec::new(),
                    latencies: Vec::new(),
                    budget: Vec::new(),
                })
                .collect(),
        }
    }

    fn secs(&self, t: Instant) -> f64 {
        (t - self.start).as_secs_f64()
    }

    fn run_to(&mut self, secs: f64) {
        let end = self.start + Duration::from_secs_f64(secs);
        while self.now < end {
            let mut next = end;
            let mut sent = false;
            for i in 0..self.flows.len() {
                let (start, now) = (self.start, self.now);
                let f = &mut self.flows[i];
                if f.has_batch(start, now) {
                    match f.model.gate(f.held, now) {
                        G::Send => {
                            self.send(i);
                            sent = true;
                        }
                        G::Wait(d) => {
                            f.pressed = true;
                            next = next.min(now + d);
                        }
                        G::Full => f.pressed = true,
                    }
                } else {
                    f.model.idle();
                    next = next.min(match f.source {
                        Source::Rate(_) => f.source_next,
                        Source::OnOff { on, off } => {
                            let t = (now - start).as_secs_f64();
                            let cycle = on + off;
                            start + Duration::from_secs_f64((t / cycle).floor() * cycle + cycle)
                        }
                        Source::Backlogged => now,
                    });
                }
            }
            if sent {
                continue;
            }
            for f in &self.flows {
                if let Some(o) = f.out.iter().map(|o| o.seen).min() {
                    next = next.min(o);
                }
                if let Some(w) = f.waiting.iter().map(|w| w.enters).min() {
                    next = next.min(w);
                }
            }
            self.now = next.max(self.now + Duration::from_micros(1)).min(end.max(self.now));
            self.admit();
            self.acknowledge();
        }
    }

    /// Hands a batch to the path. A destination over its limit refuses it at once; otherwise it reaches the
    /// bottleneck half a round trip later, where [`Sim::admit`] queues or refuses it.
    fn send(&mut self, i: usize) {
        let now = self.now;
        let t = self.secs(now);
        // What the destination holds: a batch it refused, or one that failed, holds none of its room.
        let out_total: u64 = self
            .flows
            .iter()
            .map(|f| (f.out.iter().filter(|o| !o.failed).count() + f.waiting.len()) as u64 * BATCH as u64)
            .sum();
        let over_limit = Path::at(&self.path.limit, t).is_some_and(|l| out_total + BATCH as u64 > l);
        let failed = self.path.fail_every.is_some_and(|n| (self.sent * 7919 + 13) % n == 0);
        self.sent += 1;
        let half = self.path.rtt / 2;
        let f = &mut self.flows[i];
        let token = f.model.dispatched(now, std::mem::take(&mut f.pressed));
        f.held += BATCH as u64;
        if failed {
            let at = now + self.path.rtt;
            f.out.push(Out {
                at,
                seen: at,
                token,
                sent_at: now,
                throttled: false,
                failed,
            });
        } else {
            // A refused batch is answered at once, then sent again after the wait the destination names.
            if over_limit {
                f.refused.push(now);
                if self.path.refusal_fails {
                    let at = now + self.path.rtt;
                    f.out.push(Out {
                        at,
                        seen: at,
                        token,
                        sent_at: now,
                        throttled: true,
                        failed: true,
                    });
                    return;
                }
            }
            f.waiting.push(Waiting {
                enters: if over_limit {
                    now + self.path.rtt + self.path.retry + half
                } else {
                    now + half
                },
                token,
                sent_at: now,
                throttled: over_limit,
            });
        }
        if let Source::Rate(rate) = f.source {
            f.source_next = f.source_next.max(now) + Duration::from_secs_f64(BATCH as f64 / rate);
        }
    }

    /// Lets each batch that has reached the bottleneck in, in the order they reached it. One that finds more than
    /// the buffer queued ahead of it is refused: the answer takes half a round trip back, and it is sent again after
    /// the destination's wait.
    fn admit(&mut self) {
        let now = self.now;
        loop {
            let Some((i, k)) = self
                .flows
                .iter()
                .enumerate()
                .flat_map(|(i, f)| f.waiting.iter().enumerate().map(move |(k, w)| (i, k, w.enters)))
                .filter(|(_, _, e)| *e <= now)
                .min_by_key(|(_, _, e)| *e)
                .map(|(i, k, _)| (i, k))
            else {
                return;
            };
            let w = self.flows[i].waiting.swap_remove(k);
            let capacity = Path::at(&self.path.capacity, self.secs(w.enters));
            let queued = self.link_free.saturating_duration_since(w.enters).as_secs_f64() * capacity;
            self.queue.push((w.enters, queued));
            let half = self.path.rtt / 2;
            if self.path.buffer.is_some_and(|b| queued > b) {
                self.flows[i].refused.push(w.enters);
                if self.path.refusal_fails {
                    let at = w.enters + half;
                    self.flows[i].out.push(Out {
                        at,
                        seen: at,
                        token: w.token,
                        sent_at: w.sent_at,
                        throttled: true,
                        failed: true,
                    });
                    continue;
                }
                self.flows[i].waiting.push(Waiting {
                    enters: w.enters + self.path.rtt + self.path.retry,
                    throttled: true,
                    ..w
                });
                continue;
            }
            self.link_free = self.link_free.max(w.enters) + Duration::from_secs_f64(BATCH as f64 / capacity);
            let at = self.link_free + half;
            let seen = match self.path.slot {
                Some(slot) => {
                    let since = (at - self.start).as_nanos();
                    let s = slot.as_nanos();
                    self.start + Duration::from_nanos(since.div_ceil(s) as u64 * s as u64)
                }
                None => at,
            };
            self.flows[i].out.push(Out {
                at,
                seen,
                token: w.token,
                sent_at: w.sent_at,
                throttled: w.throttled,
                failed: false,
            });
        }
    }

    fn acknowledge(&mut self) {
        let (start, now) = (self.start, self.now);
        for f in &mut self.flows {
            f.out.sort_by_key(|o| std::cmp::Reverse(o.seen));
            while f.out.last().is_some_and(|o| o.seen <= now) {
                let o = f.out.pop().expect("one is due");
                f.held -= BATCH as u64;
                let nothing = !f.has_batch(start, now);
                if o.failed {
                    f.model.failed(o.token, o.throttled, o.seen);
                } else {
                    f.model.completed(o.token, o.throttled, o.seen, nothing);
                    f.acks.push((o.at, BATCH as u64));
                    f.latencies.push((o.sent_at, o.seen - o.sent_at));
                }
                f.budget.push((now, f.model.budget()));
            }
        }
    }

    /// Bytes a second acknowledged between `from` and `to` seconds, all flows or flow `i`.
    fn throughput(&self, i: Option<usize>, from: f64, to: f64) -> f64 {
        let (a, b) = (
            self.start + Duration::from_secs_f64(from),
            self.start + Duration::from_secs_f64(to),
        );
        let bytes: u64 = self
            .flows
            .iter()
            .enumerate()
            .filter(|(j, _)| i.is_none_or(|i| i == *j))
            .flat_map(|(_, f)| f.acks.iter())
            .filter(|(t, _)| *t >= a && *t < b)
            .map(|(_, n)| n)
            .sum();
        bytes as f64 / (to - from)
    }

    fn refused(&self, from: f64, to: f64) -> usize {
        self.flows
            .iter()
            .flat_map(|f| f.refused.iter())
            .filter(|t| (from..to).contains(&self.secs(**t)))
            .count()
    }

    /// Latency from hand-off to completion seen, p50 and p99, over batches sent between `from` and `to`.
    fn latency(&self, from: f64, to: f64) -> (f64, f64) {
        let mut l: Vec<f64> = self
            .flows
            .iter()
            .flat_map(|f| f.latencies.iter())
            .filter(|(at, _)| (from..to).contains(&self.secs(*at)))
            .map(|(_, d)| d.as_secs_f64() * 1e3)
            .collect();
        if l.is_empty() {
            return (0.0, 0.0);
        }
        l.sort_by(f64::total_cmp);
        (l[l.len() / 2], l[(l.len() * 99 / 100).min(l.len() - 1)])
    }

    /// Mean bytes queued at the bottleneck ahead of each batch as it reaches it, between `from` and `to`.
    fn queued(&self, from: f64, to: f64) -> f64 {
        let q: Vec<f64> = self
            .queue
            .iter()
            .filter(|(at, _)| (from..to).contains(&self.secs(*at)))
            .map(|(_, b)| *b)
            .collect();
        if q.is_empty() {
            0.0
        } else {
            q.iter().sum::<f64>() / q.len() as f64
        }
    }

    /// Mean of every flow's queue budget, summed across flows, sampled at completions between `from` and `to`.
    fn budget(&self, from: f64, to: f64) -> f64 {
        self.flows
            .iter()
            .map(|f| {
                let b: Vec<f64> = f
                    .budget
                    .iter()
                    .filter(|(at, _)| (from..to).contains(&self.secs(*at)))
                    .map(|(_, b)| *b as f64)
                    .collect();
                if b.is_empty() {
                    0.0
                } else {
                    b.iter().sum::<f64>() / b.len() as f64
                }
            })
            .sum()
    }

    /// The first time after `after` from which throughput over each following `window` stays at least `share` of
    /// `target`, in seconds after `after`; `None` if never by `until`.
    fn recovery(&self, after: f64, until: f64, target: f64, share: f64) -> Option<f64> {
        let window = 0.5;
        let mut t = after;
        while t + window <= until {
            if self.throughput(None, t, t + window) >= share * target {
                return Some(t - after);
            }
            t += 0.1;
        }
        None
    }

    /// Jain's fairness index of the flows' throughputs between `from` and `to`.
    fn fairness(&self, from: f64, to: f64) -> f64 {
        let x: Vec<f64> = (0..self.flows.len())
            .map(|i| self.throughput(Some(i), from, to))
            .collect();
        let sum: f64 = x.iter().sum();
        let squares: f64 = x.iter().map(|v| v * v).sum();
        if squares == 0.0 {
            0.0
        } else {
            sum * sum / (x.len() as f64 * squares)
        }
    }
}

fn new_model() -> New {
    New(Pacing::seeded(BATCH, CEILING, Instant::now(), 7))
}

fn old_model() -> Old {
    Old(old::Pacing::new(BATCH, CEILING))
}

fn mib(b: f64) -> f64 {
    b / MIB as f64
}

macro_rules! scenario {
    ($title:expr, $path:expr, $sources:expr, $secs:expr, |$s:ident| $line:expr) => {{
        println!("\n## {}", $title);
        {
            let flows = $sources.iter().map(|src| (new_model(), *src)).collect();
            let mut $s = Sim::new($path.clone(), flows);
            $s.run_to($secs);
            println!("bbr  | {}", $line);
        }
        {
            let flows = $sources.iter().map(|src| (old_model(), *src)).collect();
            let mut $s = Sim::new($path.clone(), flows);
            $s.run_to($secs);
            println!("main | {}", $line);
        }
    }};
}

#[test]
#[ignore = "a comparison report, not a check"]
fn compare_the_two_models() {
    let one = [Source::Backlogged];

    // Steady state, and what each holds for it (rows 1, 30).
    for (cap, rtt) in [(GIB, 1), (GIB, 20), (GIB, 200), (100.0 * MIB as f64, 20)] {
        let path = Path::new(cap, rtt);
        scenario!(
            format!("steady {} MiB/s, {rtt} ms", mib(cap)),
            path,
            one,
            30.0,
            |s| format!(
                "thru {:.0} MiB/s  queued {:.1} MiB  budget {:.1} MiB  lat p50/p99 {:.1}/{:.1} ms",
                mib(s.throughput(None, 10.0, 30.0)),
                mib(s.queued(10.0, 30.0)),
                mib(s.budget(10.0, 30.0)),
                s.latency(10.0, 30.0).0,
                s.latency(10.0, 30.0).1
            )
        );
    }

    // Startup: time to fill a long fat pipe, and what it queues (rows 3, 6).
    let path = Path::new(GIB, 100);
    scenario!("startup 1 GiB/s, 100 ms", path, one, 10.0, |s| format!(
        "90% after {:?} s  queued 0-3 s {:.1} MiB  lat p99 0-3 s {:.0} ms",
        s.recovery(0.0, 10.0, GIB, 0.9),
        mib(s.queued(0.0, 3.0)),
        s.latency(0.0, 3.0).1
    ));

    // Bunched completions (rows 22, 23).
    for slot in [5, 12, 30] {
        let path = Path {
            slot: Some(Duration::from_millis(slot)),
            ..Path::new(1.2e9, 4)
        };
        scenario!(
            format!("acks in {slot} ms slots, 1.2 GB/s, 4 ms"),
            path,
            one,
            20.0,
            |s| format!(
                "thru {:.0} MiB/s  rate est {:.0} MiB/s  budget {:.1} MiB",
                mib(s.throughput(None, 5.0, 20.0)),
                mib(s.flows[0].model.rate()),
                mib(s.budget(5.0, 20.0))
            )
        );
    }

    // A destination's hard limit (rows 9, 11, 13, 14).
    for limit in [3u64, 8, 32] {
        let path = Path {
            limit: vec![(0.0, Some(limit * BATCH as u64))],
            ..Path::new(GIB, 20)
        };
        scenario!(
            format!("limit {limit} batches, 1 GiB/s, 20 ms"),
            path,
            one,
            60.0,
            |s| format!(
                "thru {:.0} MiB/s  refused/min {:.0}  refused in first 2 s {}",
                mib(s.throughput(None, 10.0, 60.0)),
                s.refused(10.0, 60.0) as f64 * 60.0 / 50.0,
                s.refused(0.0, 2.0)
            )
        );
    }

    // A fresh flow against a small limit: the initial window and Startup's loss exit (rows 2, 5).
    let path = Path {
        limit: vec![(0.0, Some(4 * BATCH as u64))],
        ..Path::new(GIB, 5)
    };
    scenario!("fresh flow, limit 4 batches, 5 ms", path, one, 5.0, |s| format!(
        "refused first 1 s {}  first 5 s {}",
        s.refused(0.0, 1.0),
        s.refused(0.0, 5.0)
    ));

    // A shallow buffer: refuses when its own queue holds over 2 batches (rows 1, 9).
    let path = Path {
        buffer: Some(2.0 * BATCH as f64),
        ..Path::new(GIB, 20)
    };
    scenario!(
        "shallow buffer 2 batches, 1 GiB/s, 20 ms",
        path,
        one,
        60.0,
        |s| format!(
            "thru {:.0} MiB/s  refused/min {:.0}  queued {:.1} MiB",
            mib(s.throughput(None, 10.0, 60.0)),
            s.refused(10.0, 60.0) as f64 * 60.0 / 50.0,
            mib(s.queued(10.0, 60.0))
        )
    );

    // A limit that drops and comes back (row 16).
    let path = Path {
        limit: vec![
            (0.0, Some(16 * BATCH as u64)),
            (30.0, Some(6 * BATCH as u64)),
            (60.0, Some(16 * BATCH as u64)),
        ],
        ..Path::new(GIB, 20)
    };
    scenario!("limit 16 -> 6 at 30 s -> 16 at 60 s", path, one, 90.0, |s| format!(
        "refused 30-35 s {}  refused 35-60 s {}  thru 40-60 {:.0}  thru 75-90 {:.0} MiB/s",
        s.refused(30.0, 35.0),
        s.refused(35.0, 60.0),
        mib(s.throughput(None, 40.0, 60.0)),
        mib(s.throughput(None, 75.0, 90.0))
    ));

    // Capacity drops, then comes back, no limit (rows 10, 20).
    let path = Path {
        capacity: vec![(0.0, GIB), (30.0, 0.1 * GIB), (60.0, GIB)],
        ..Path::new(GIB, 20)
    };
    scenario!(
        "capacity 1 -> 0.1 GiB/s at 30 s -> 1 at 60 s, 20 ms",
        path,
        one,
        90.0,
        |s| format!(
            "queued 31-60 s {:.0} MiB  lat p50/p99 31-60 s {:.0}/{:.0} ms  back to 80% after {:?} s",
            mib(s.queued(31.0, 60.0)),
            s.latency(31.0, 60.0).0,
            s.latency(31.0, 60.0).1,
            s.recovery(60.0, 90.0, GIB, 0.8)
        )
    );

    // Random failures, no congestion (row 17).
    for every in [200u64, 50] {
        let path = Path {
            fail_every: Some(every),
            ..Path::new(GIB, 20)
        };
        scenario!(
            format!("1 in {every} batches fails, 1 GiB/s, 20 ms"),
            path,
            one,
            30.0,
            |s| format!("thru {:.0} MiB/s", mib(s.throughput(None, 10.0, 30.0)))
        );
    }

    // A slow destination (row 27).
    let path = Path::new(MIB as f64, 10);
    scenario!("slow destination 1 MiB/s, 10 ms", path, one, 120.0, |s| format!(
        "thru {:.2} MiB/s  budget {:.0} MiB  lat p50 {:.0} ms",
        mib(s.throughput(None, 30.0, 120.0)),
        mib(s.budget(30.0, 120.0)),
        s.latency(30.0, 120.0).0
    ));

    // Bursts with idle between (rows 21, 26): 2 s on, 12 s off, against a limit of 8.
    let path = Path {
        limit: vec![(0.0, Some(8 * BATCH as u64))],
        ..Path::new(GIB, 20)
    };
    let onoff = [Source::OnOff { on: 2.0, off: 12.0 }];
    scenario!(
        "2 s on, 12 s off, limit 8, 1 GiB/s, 20 ms",
        path,
        onoff,
        84.0,
        |s| format!(
            "refused/burst {:.1}  lat p50/p99 {:.0}/{:.0} ms  thru in bursts {:.0} MiB/s",
            s.refused(14.0, 84.0) as f64 / 5.0,
            s.latency(14.0, 84.0).0,
            s.latency(14.0, 84.0).1,
            mib((1..6)
                .map(|k| s.throughput(None, k as f64 * 14.0, k as f64 * 14.0 + 2.0))
                .sum::<f64>()
                / 5.0)
        )
    );

    // A source slower than the link (row 21).
    let path = Path::new(GIB, 20);
    let slow = [Source::Rate(0.2 * GIB)];
    scenario!("source 0.2 GiB/s on a 1 GiB/s link", path, slow, 30.0, |s| format!(
        "thru {:.0} MiB/s  rate est {:.0} MiB/s  budget {:.0} MiB",
        mib(s.throughput(None, 10.0, 30.0)),
        mib(s.flows[0].model.rate()),
        mib(s.budget(10.0, 30.0))
    ));

    // Flows sharing one destination (rows 7, 12).
    for n in [2usize, 4] {
        let path = Path {
            limit: vec![(0.0, Some(16 * BATCH as u64))],
            ..Path::new(GIB, 20)
        };
        let flows = vec![Source::Backlogged; n];
        scenario!(
            format!("{n} flows, limit 16, 1 GiB/s, 20 ms"),
            path,
            flows,
            60.0,
            |s| format!(
                "thru {:.0} MiB/s  fairness {:.3}  refused/min {:.0}",
                mib(s.throughput(None, 10.0, 60.0)),
                s.fairness(10.0, 60.0),
                s.refused(10.0, 60.0) as f64 * 60.0 / 50.0
            )
        );
        let path = Path::new(GIB, 20);
        let flows = vec![Source::Backlogged; n];
        scenario!(
            format!("{n} flows, no limit, 1 GiB/s, 20 ms"),
            path,
            flows,
            60.0,
            |s| format!(
                "thru {:.0} MiB/s  fairness {:.3}  queued {:.0} MiB  lat p99 {:.0} ms",
                mib(s.throughput(None, 10.0, 60.0)),
                s.fairness(10.0, 60.0),
                mib(s.queued(10.0, 60.0)),
                s.latency(10.0, 60.0).1
            )
        );
    }
}

#[test]
#[ignore = "a trace, not a check"]
fn trace_a_capacity_drop() {
    let path = Path {
        capacity: vec![(0.0, GIB), (30.0, 0.1 * GIB)],
        ..Path::new(GIB, 20)
    };
    let mut s = Sim::new(path, vec![(new_model(), Source::Backlogged)]);
    let mut t = 28.0;
    while t < 60.0 {
        s.run_to(t);
        let p = &s.flows[0].model.0;
        println!(
            "t {t:>5.1}  state {:?}  max_bw {:.0} MiB/s  bw {:.0}  min_rtt {:?}  cwnd {:.0} MiB  inflight {:.0} MiB  extra_acked {:.0} MiB  cycle {}",
            p.state,
            mib(p.max_bw),
            mib(p.bw),
            p.min_rtt,
            mib(p.cwnd),
            mib(p.inflight as f64),
            mib(p.extra_acked),
            p.cycle_count
        );
        t += 1.0;
    }
}

/// Sweeps how far capacity drops, and whether the bottleneck's buffer is deep enough never to drop, to find where the
/// segment-reading model's queue stops draining after a drop with no loss.
#[test]
#[ignore = "a sweep for a person to read: cargo nextest run --lib egress::pacing::compare --run-ignored only --no-capture"]
fn sweep_a_capacity_drop() {
    for (ceiling, buffer) in [
        (CEILING, None),
        (64 * MIB, None),
        (CEILING, Some(64.0 * MIB as f64)),
        (CEILING, Some(16.0 * MIB as f64)),
    ] {
        for after in [0.5, 0.33, 0.2, 0.1] {
            let path = || Path {
                capacity: vec![(0.0, GIB), (30.0, after * GIB)],
                buffer,
                ..Path::new(GIB, 20)
            };
            let mut new = Sim::new(
                path(),
                vec![(
                    New(Pacing::seeded(BATCH, ceiling, Instant::now(), 7)),
                    Source::Backlogged,
                )],
            );
            let mut old = Sim::new(
                path(),
                vec![(Old(old::Pacing::new(BATCH, ceiling)), Source::Backlogged)],
            );
            new.run_to(60.0);
            old.run_to(60.0);
            let p = &new.flows[0].model.0;
            println!(
                "ceiling {:>4} MiB buffer {:>8}  drop to {after:.2}  before bbr {:>5.0} main {:>5.0}  bbr: {:>5.0} MiB/s queued {:>5.0} MiB p99 {:>6.0} ms max_bw {:>5.0} cycle {:>3}   main: {:>5.0} MiB/s queued {:>4.0} MiB p99 {:>5.0} ms",
                ceiling / MIB,
                buffer.map_or("none".to_string(), |b| format!("{:.0} MiB", b / MIB as f64)),
                mib(new.throughput(None, 10.0, 30.0)),
                mib(old.throughput(None, 10.0, 30.0)),
                mib(new.throughput(None, 45.0, 60.0)),
                mib(new.queued(45.0, 60.0)),
                new.latency(45.0, 60.0).1,
                mib(p.max_bw),
                p.cycle_count,
                mib(old.throughput(None, 45.0, 60.0)),
                mib(old.queued(45.0, 60.0)),
                old.latency(45.0, 60.0).1,
            );
        }
    }
}

/// Prints the segment-reading model's state each half second on a 1 GiB/s, 20 ms path whose bottleneck holds 16 MiB,
/// less than one round trip's worth.
#[test]
#[ignore = "a trace for a person to read: cargo nextest run --lib egress::pacing::compare --run-ignored only --no-capture"]
fn trace_a_shallow_buffer() {
    let path = Path {
        buffer: Some(16.0 * MIB as f64),
        ..Path::new(GIB, 20)
    };
    let mut s = Sim::new(path, vec![(new_model(), Source::Backlogged)]);
    let mut t = 0.0;
    while t < 12.0 {
        t += 0.25;
        let before = s.refused(0.0, t);
        s.run_to(t);
        let p = &s.flows[0].model.0;
        println!(
            "t {t:>5.2}  {:<14} {:>5.0} MiB/s lost {:>3}  max_bw {:>5.0} bw_short {:>6.0} bw {:>5.0}  in_long {:>6.1} in_short {:>6.1} cwnd {:>5.1} inflight {:>5.1} MiB  min_rtt {:?}",
            format!("{:?}", p.state),
            mib(s.throughput(None, t - 0.25, t)),
            s.refused(0.0, t) - before,
            mib(p.max_bw),
            mib(p.bw_shortterm),
            mib(p.bw),
            mib(p.inflight_longterm),
            mib(p.inflight_shortterm),
            mib(p.cwnd),
            mib(p.inflight as f64),
            p.min_rtt,
        );
    }
}

/// Prints the segment-reading model's state for flow 0 every `step` seconds from `from` to `to`.
fn trace(name: &str, path: Path, flows: usize, from: f64, to: f64, step: f64) {
    println!("## {name}");
    let mut s = Sim::new(
        path,
        (0..flows)
            .map(|i| {
                (
                    New(Pacing::seeded(BATCH, CEILING, Instant::now(), 7 + i as u64)),
                    Source::Backlogged,
                )
            })
            .collect(),
    );
    s.run_to(from);
    let mut t = from;
    while t < to {
        let before = s.refused(0.0, t);
        t += step;
        s.run_to(t);
        let p = &s.flows[0].model.0;
        let shares: Vec<String> = (0..flows)
            .map(|i| format!("{:.0}", mib(s.throughput(Some(i), t - step, t))))
            .collect();
        println!(
            "t {t:>5.2} {:<14} thru {:>11} refused {:>3}  max_bw {:>5.0} bw_lo {:>6.0} bw {:>5.0} | in_hi {:>6.1} in_lo {:>6.1} cwnd {:>5.1} inflight {:>5.1} MiB | rtt {:>5.1} ms cycle {} loss_rounds? {}",
            format!("{:?}", p.state),
            shares.join("/"),
            s.refused(0.0, t) - before,
            mib(p.max_bw),
            mib(p.bw_shortterm),
            mib(p.bw),
            mib(p.inflight_longterm),
            mib(p.inflight_shortterm),
            mib(p.cwnd),
            mib(p.inflight as f64),
            p.min_rtt.map_or(0.0, |d| d.as_secs_f64() * 1e3),
            p.cycle_count,
            p.loss_events_in_round,
        );
    }
}

#[test]
#[ignore = "traces for a person to read: cargo nextest run --lib egress::pacing::compare --run-ignored only --no-capture"]
fn trace_the_losses() {
    trace(
        "1 in 50 batches fails",
        Path {
            fail_every: Some(50),
            ..Path::new(GIB, 20)
        },
        1,
        0.0,
        8.0,
        0.25,
    );
    trace(
        "limit 16 -> 6 at 30 s -> 16 at 60 s",
        Path {
            limit: vec![
                (0.0, Some(16 * BATCH as u64)),
                (30.0, Some(6 * BATCH as u64)),
                (60.0, Some(16 * BATCH as u64)),
            ],
            ..Path::new(GIB, 20)
        },
        1,
        55.0,
        80.0,
        0.5,
    );
    trace(
        "shallow buffer 2 batches",
        Path {
            buffer: Some(2.0 * BATCH as f64),
            ..Path::new(GIB, 20)
        },
        1,
        5.0,
        12.0,
        0.25,
    );
    trace(
        "4 flows, limit 16",
        Path {
            limit: vec![(0.0, Some(16 * BATCH as u64))],
            ..Path::new(GIB, 20)
        },
        4,
        0.0,
        30.0,
        1.0,
    );
}

/// Runs each scenario that refuses batches twice: once as destinations report a refusal now, throttled when the
/// retried batch lands; once with the refusal coming back as a failed batch at once, sent again as a new one.
#[test]
#[ignore = "a comparison for a person to read: cargo nextest run --lib egress::pacing::compare --run-ignored only --no-capture"]
fn refusals_reported_late_or_at_once() {
    let limit = |steps: &[(f64, u64)]| {
        steps
            .iter()
            .map(|(t, n)| (*t, Some(n * BATCH as u64)))
            .collect::<Vec<_>>()
    };
    let cases: Vec<(&str, Path, usize, f64, f64, f64)> = vec![
        (
            "limit 3",
            Path {
                limit: limit(&[(0.0, 3)]),
                ..Path::new(GIB, 20)
            },
            1,
            60.0,
            10.0,
            60.0,
        ),
        (
            "limit 8",
            Path {
                limit: limit(&[(0.0, 8)]),
                ..Path::new(GIB, 20)
            },
            1,
            60.0,
            10.0,
            60.0,
        ),
        (
            "shallow buffer 2 batches",
            Path {
                buffer: Some(2.0 * BATCH as f64),
                ..Path::new(GIB, 20)
            },
            1,
            60.0,
            10.0,
            60.0,
        ),
        (
            "limit 16 -> 6 -> 16, after",
            Path {
                limit: limit(&[(0.0, 16), (30.0, 6), (60.0, 16)]),
                ..Path::new(GIB, 20)
            },
            1,
            90.0,
            75.0,
            90.0,
        ),
        (
            "limit 16 -> 6 -> 16, during",
            Path {
                limit: limit(&[(0.0, 16), (30.0, 6), (60.0, 16)]),
                ..Path::new(GIB, 20)
            },
            1,
            90.0,
            35.0,
            60.0,
        ),
        (
            "4 flows, limit 16",
            Path {
                limit: limit(&[(0.0, 16)]),
                ..Path::new(GIB, 20)
            },
            4,
            60.0,
            10.0,
            60.0,
        ),
    ];
    for (name, path, flows, until, from, to) in cases {
        for fails in [false, true] {
            let path = Path {
                refusal_fails: fails,
                ..path.clone()
            };
            let mut new = Sim::new(
                path.clone(),
                (0..flows)
                    .map(|i| {
                        (
                            New(Pacing::seeded(BATCH, CEILING, Instant::now(), 7 + i as u64)),
                            Source::Backlogged,
                        )
                    })
                    .collect(),
            );
            let mut old = Sim::new(path, (0..flows).map(|_| (old_model(), Source::Backlogged)).collect());
            new.run_to(until);
            old.run_to(until);
            let per_min = |n: usize| n as f64 * 60.0 / (to - from);
            println!(
                "{name:<28} {:<8} bbr {:>5.0} MiB/s refused/min {:>5.0} fair {:.2} p99 {:>4.0} ms | main {:>5.0} MiB/s refused/min {:>5.0} fair {:.2} p99 {:>4.0} ms",
                if fails { "at once" } else { "late" },
                mib(new.throughput(None, from, to)),
                per_min(new.refused(from, to)),
                new.fairness(from, to),
                new.latency(from, to).1,
                mib(old.throughput(None, from, to)),
                per_min(old.refused(from, to)),
                old.fairness(from, to),
                old.latency(from, to).1,
            );
        }
    }
}

/// Prints, each round trip, how the segment-reading model probes after a destination's limit comes back from 6
/// batches to 16.
#[test]
#[ignore = "a trace for a person to read: cargo nextest run --lib egress::pacing::compare --run-ignored only --no-capture"]
fn trace_a_limit_coming_back() {
    let path = Path {
        limit: vec![
            (0.0, Some(16 * BATCH as u64)),
            (30.0, Some(6 * BATCH as u64)),
            (60.0, Some(16 * BATCH as u64)),
        ],
        ..Path::new(GIB, 20)
    };
    let mut s = Sim::new(path, vec![(new_model(), Source::Backlogged)]);
    s.run_to(62.0);
    let mut t = 62.0;
    let mut last = None;
    while t < 66.0 {
        t += 0.006;
        s.run_to(t);
        let p = &s.flows[0].model.0;
        let line = format!(
            "{:<14} in_hi {:>8.3} cwnd {:>7.3} inflight {:>5.1} MiB  limited {} full_bw_count {} now {} up_rounds {} per_inc {:>9.0} B acked {:>9.0} B too_high {} check_loss {}",
            format!("{:?}", p.state),
            mib(p.inflight_longterm),
            mib(p.cwnd),
            mib(p.inflight as f64),
            p.is_cwnd_limited(),
            p.full_bw_count,
            p.full_bw_now,
            p.bw_probe_up_rounds,
            p.probe_up_acked_per_inc,
            p.bw_probe_up_acked,
            p.prev_probe_too_high,
            p.check_probe_loss,
        );
        if last.as_ref() != Some(&line) {
            println!("t {t:>6.3} {line}");
            last = Some(line);
        }
    }
}
