//! How much a destination may hold: what raises its throughput, and nothing more.
//!
//! A destination left for the worker to size has no fixed buffer. Its egress thread measures the link -- how fast
//! the destination acknowledges, and how long one batch takes to come back -- and from the two derives a target,
//! the bytes worth having queued and in flight: a bandwidth-delay product with headroom to probe for more. The egress
//! queue's budget is that target, so a slow destination's queue stays small and its source feels backpressure early,
//! while a fast one grows for as long as each step still raises the rate. The destination's own in-flight bound reads
//! the same number, so one figure governs the queue, a Kafka producer's copy and an HTTP pool's senders alike.
//!
//! The model is BBR's, cut to what a batch pipeline needs. A rate sample taken while the source had nothing more to
//! send says how fast the source produces, not how fast the link carries, and may raise the estimate but never lower
//! it: a slow source never makes its destination look slow. Startup doubles the target each round trip until the rate
//! stops growing; after that an eight-step cycle probes at 1.25 and drains at 0.75, so the round-trip filter sees an
//! unqueued sample each cycle and a link that gets faster or slower is followed.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

/// How long the least round trip is kept: a link whose round trip grew is believed after this long.
const RTT_WINDOW: Duration = Duration::from_secs(10);

/// How many round trips the greatest rate is kept for, and the least and most time that is. Samples lapse only as
/// batches come back with others still waiting: a destination the source left idle keeps its estimate, one that
/// crawls loses it within [`RTT_WINDOW`], whatever its round trip now reads.
const RATE_WINDOW_ROUND_TRIPS: u32 = 10;
const RATE_WINDOW_LEAST: Duration = Duration::from_secs(1);
const RATE_WINDOW_MOST: Duration = RTT_WINDOW;

/// Startup ends once the rate has grown less than this over [`FULL_PIPE_ROUNDS`] round trips in a row.
const FULL_PIPE_GROWTH: f64 = 1.25;
const FULL_PIPE_ROUNDS: u32 = 3;

/// The gain while the pipe is still filling.
const STARTUP_GAIN: f64 = 2.0;

/// The steady-state cycle, one step per round trip: probe, drain, cruise.
const PROBE_CYCLE: [f64; 8] = [1.25, 0.75, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0];

/// One delivery, as the model is told of it.
#[derive(Clone, Copy, Debug)]
pub struct Completion {
    /// The bytes acknowledged.
    pub bytes: usize,
    /// When the batch was handed to the destination.
    pub dispatched_at: Instant,
    /// Nothing else was waiting in the queue when it was dispatched, so the link was not the bound on this sample:
    /// the source was.
    pub app_limited: bool,
}

/// The pacing model for one destination.
#[derive(Debug)]
pub struct Pacing {
    /// The least the target is: one batch, so the destination always moves.
    floor: usize,
    /// The most the target may be right now: the destination's fair share of the pool, or a stated buffer.
    ceiling: usize,
    target: usize,
    /// Bytes handed to the destination so far, and bytes it acknowledged.
    dispatched: u64,
    delivered: u64,
    /// `delivered` as it stood at each completion, oldest first, for the rate over a window.
    history: VecDeque<(Instant, u64)>,
    /// Round-trip samples within [`RTT_WINDOW`], oldest first. The least is the estimate.
    rtts: VecDeque<(Instant, Duration)>,
    /// Rate samples, in bytes a second, within the rate window, oldest first. The greatest is the estimate.
    rates: VecDeque<(Instant, f64)>,
    /// A round trip ends when `delivered` passes what was `dispatched` at its start.
    round_ends_at: u64,
    rounds: u32,
    /// Whether startup is over, and what the rate was at the last round where it grew enough, with how many rounds
    /// have passed since.
    full_pipe: bool,
    rate_at_last_growth: f64,
    rounds_without_growth: u32,
}

impl Pacing {
    /// A model whose target starts at `floor`, one batch, and never passes `ceiling`.
    pub fn new(floor: usize, ceiling: usize) -> Pacing {
        let floor = floor.max(1);
        Pacing {
            floor,
            ceiling: ceiling.max(floor),
            target: floor,
            dispatched: 0,
            delivered: 0,
            history: VecDeque::new(),
            rtts: VecDeque::new(),
            rates: VecDeque::new(),
            round_ends_at: 0,
            rounds: 0,
            full_pipe: false,
            rate_at_last_growth: 0.0,
            rounds_without_growth: 0,
        }
    }

    /// The bytes the destination may hold now: queued and in flight together.
    pub fn target(&self) -> usize {
        self.target
    }

    /// The most the target may be. Moves as the pool's share does; the target follows at the next completion.
    pub fn set_ceiling(&mut self, ceiling: usize) {
        self.ceiling = ceiling.max(self.floor);
        self.target = self.target.clamp(self.floor, self.ceiling);
    }

    /// The least round trip seen lately, once there is one.
    pub fn round_trip(&self) -> Option<Duration> {
        self.rtts.iter().map(|(_, d)| *d).min()
    }

    /// The greatest delivery rate seen lately, in bytes a second.
    pub fn rate(&self) -> f64 {
        self.rates.iter().map(|(_, r)| *r).fold(0.0, f64::max)
    }

    /// A batch of `bytes` was handed to the destination.
    pub fn dispatched(&mut self, bytes: usize) {
        self.dispatched += bytes as u64;
    }

    /// A batch came back acknowledged. Updates the estimates and the target.
    pub fn completed(&mut self, c: Completion, now: Instant) {
        self.delivered += c.bytes as u64;
        let rtt = now.saturating_duration_since(c.dispatched_at);
        self.rtts.push_back((now, rtt));
        while self
            .rtts
            .front()
            .is_some_and(|(at, _)| now.saturating_duration_since(*at) > RTT_WINDOW)
        {
            self.rtts.pop_front();
        }
        let least_rtt = self.round_trip().unwrap_or(rtt).max(Duration::from_micros(1));

        // The rate over the last round trip: what was acknowledged since the completion one round trip back, over
        // the time since it. One batch over its own time would read as a burst; a window reads as a rate.
        let since = now - least_rtt;
        let base = self
            .history
            .iter()
            .rev()
            .find(|(at, _)| *at <= since)
            .copied()
            .or_else(|| self.history.front().copied());
        self.history.push_back((now, self.delivered));
        while self
            .history
            .front()
            .is_some_and(|(at, _)| now.saturating_duration_since(*at) > RTT_WINDOW)
        {
            self.history.pop_front();
        }
        let rate = match base {
            Some((at, delivered)) if now > at => {
                (self.delivered - delivered) as f64 / now.saturating_duration_since(at).as_secs_f64()
            }
            // The first sample: this batch alone, over the least it could have taken.
            _ => c.bytes as f64 / least_rtt.as_secs_f64(),
        };

        let window = (least_rtt * RATE_WINDOW_ROUND_TRIPS).clamp(RATE_WINDOW_LEAST, RATE_WINDOW_MOST);
        if c.app_limited {
            // The source was the bound, so this says only that the link carries at least this much.
            if rate > self.rate() {
                self.rates.push_back((now, rate));
            }
        } else {
            while self
                .rates
                .front()
                .is_some_and(|(at, _)| now.saturating_duration_since(*at) > window)
            {
                self.rates.pop_front();
            }
            self.rates.push_back((now, rate));
        }

        if self.delivered >= self.round_ends_at {
            self.round_ends_at = self.dispatched;
            self.rounds += 1;
            self.end_round();
        }
        self.retarget(least_rtt);
    }

    /// A round trip ended: judge whether the pipe is full yet.
    fn end_round(&mut self) {
        if self.full_pipe {
            return;
        }
        let rate = self.rate();
        if rate >= self.rate_at_last_growth * FULL_PIPE_GROWTH {
            self.rate_at_last_growth = rate;
            self.rounds_without_growth = 0;
        } else {
            self.rounds_without_growth += 1;
            if self.rounds_without_growth >= FULL_PIPE_ROUNDS {
                self.full_pipe = true;
            }
        }
    }

    fn gain(&self) -> f64 {
        if self.full_pipe {
            PROBE_CYCLE[(self.rounds as usize) % PROBE_CYCLE.len()]
        } else {
            STARTUP_GAIN
        }
    }

    fn retarget(&mut self, least_rtt: Duration) {
        let bdp = self.rate() * least_rtt.as_secs_f64();
        let wanted = (self.gain() * bdp).round();
        let wanted = if wanted.is_finite() && wanted > 0.0 {
            wanted.min(usize::MAX as f64) as usize
        } else {
            0
        };
        self.target = wanted.clamp(self.floor, self.ceiling);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIB: usize = 1024 * 1024;
    const BATCH: usize = 4 * MIB;

    /// A link `rtt` away that carries `capacity` bytes a second, fed by a source with plenty to send, for `rounds`
    /// round trips. The destination holds batches up to the target, as the queue does: a new batch goes out the
    /// moment the target has room for it, and the link lands them one transmission time apart, the first a round
    /// trip after it was sent. Answers the target at each round trip's end.
    fn drive(p: &mut Pacing, rtt: Duration, capacity: f64, rounds: usize, app_limited: bool) -> Vec<usize> {
        let per_batch = Duration::from_secs_f64(BATCH as f64 / capacity);
        let start = Instant::now();
        let mut now = start;
        // (lands at, sent at), in landing order.
        let mut in_flight: VecDeque<(Instant, Instant)> = VecDeque::new();
        let mut link_free_at = start;
        let mut targets = Vec::new();
        let mut next_round = start + rtt;
        while targets.len() < rounds {
            let held = in_flight.len() * BATCH;
            if held == 0 || held + BATCH <= p.target() {
                p.dispatched(BATCH);
                let lands = (now + rtt).max(link_free_at + per_batch);
                link_free_at = lands;
                in_flight.push_back((lands, now));
                continue;
            }
            let (lands, sent) = in_flight.pop_front().expect("something in flight");
            now = lands;
            p.completed(
                Completion {
                    bytes: BATCH,
                    dispatched_at: sent,
                    app_limited,
                },
                now,
            );
            if now >= next_round {
                targets.push(p.target());
                next_round = now + rtt;
            }
        }
        targets
    }

    #[test]
    fn the_target_starts_at_one_batch_and_doubles_while_the_pipe_fills() {
        let mut p = Pacing::new(BATCH, 1024 * MIB);
        assert_eq!(p.target(), BATCH);
        // 200ms away, a link that carries 100 MiB/s: a bandwidth-delay product of 20 MiB.
        let targets = drive(&mut p, Duration::from_millis(200), 100.0 * MIB as f64, 4, false);
        assert!(targets[0] >= 2 * BATCH, "doubled after the first round: {targets:?}");
        assert!(targets[2] > targets[0], "and on: {targets:?}");
        assert!(targets[3] > targets[2], "{targets:?}");
    }

    #[test]
    fn the_target_settles_near_the_bandwidth_delay_product_and_within_the_ceiling() {
        let rtt = Duration::from_millis(200);
        let capacity = 100.0 * MIB as f64;
        let bdp = (capacity * rtt.as_secs_f64()) as usize;
        let mut p = Pacing::new(BATCH, 1024 * MIB);
        let targets = drive(&mut p, rtt, capacity, 40, false);
        let settled = &targets[20..];
        for t in settled {
            assert!(
                *t >= bdp * 3 / 4,
                "never drains below three quarters of the product: {t} vs {bdp}"
            );
            assert!(*t <= bdp * 3, "never far above it: {t} vs {bdp}");
        }
        assert!(p.full_pipe, "startup ended");

        let mut capped = Pacing::new(BATCH, 3 * BATCH);
        let targets = drive(&mut capped, rtt, capacity, 10, false);
        assert!(targets.iter().all(|t| *t <= 3 * BATCH), "{targets:?}");
    }

    #[test]
    fn a_slow_link_keeps_a_small_target() {
        // 10ms away, carrying 1 MiB/s: a product of 10 KiB, far below one batch.
        let mut p = Pacing::new(BATCH, 1024 * MIB);
        let targets = drive(&mut p, Duration::from_millis(10), MIB as f64, 20, false);
        assert!(targets.iter().all(|t| *t <= 2 * BATCH), "{targets:?}");
    }

    #[test]
    fn a_slow_source_never_lowers_the_estimate() {
        let rtt = Duration::from_millis(200);
        let capacity = 100.0 * MIB as f64;
        let mut p = Pacing::new(BATCH, 1024 * MIB);
        drive(&mut p, rtt, capacity, 30, false);
        let rate = p.rate();
        let target = p.target();
        // The source goes quiet: one batch a round trip, with nothing waiting behind it.
        let mut now = Instant::now() + Duration::from_secs(60);
        for _ in 0..50 {
            p.dispatched(BATCH);
            p.completed(
                Completion {
                    bytes: BATCH,
                    dispatched_at: now,
                    app_limited: true,
                },
                now + rtt,
            );
            now += Duration::from_secs(1);
        }
        assert!(p.rate() >= rate * 0.99, "the estimate held: {} vs {rate}", p.rate());
        assert!(
            p.target() >= target * 3 / 4,
            "and so did the target: {} vs {target}",
            p.target()
        );
    }

    #[test]
    fn a_stall_lets_the_rate_lapse_and_the_target_fall_to_the_floor() {
        let rtt = Duration::from_millis(200);
        let mut p = Pacing::new(BATCH, 1024 * MIB);
        drive(&mut p, rtt, 100.0 * MIB as f64, 30, false);
        assert!(p.target() > 4 * BATCH);
        // Then one batch comes back after a very long time, with others still waiting.
        let late = Instant::now() + Duration::from_secs(120);
        p.dispatched(BATCH);
        p.completed(
            Completion {
                bytes: BATCH,
                dispatched_at: late - Duration::from_secs(30),
                app_limited: false,
            },
            late,
        );
        assert!(
            p.target() <= 2 * BATCH,
            "one batch over thirty seconds is no rate at all: {}",
            p.target()
        );
    }

    #[test]
    fn the_ceiling_moves_the_target_at_once() {
        let mut p = Pacing::new(BATCH, 1024 * MIB);
        drive(&mut p, Duration::from_millis(200), 100.0 * MIB as f64, 30, false);
        assert!(p.target() > 2 * BATCH);
        p.set_ceiling(2 * BATCH);
        assert_eq!(p.target(), 2 * BATCH);
        p.set_ceiling(0);
        assert_eq!(p.target(), BATCH, "never below the floor");
    }
}
