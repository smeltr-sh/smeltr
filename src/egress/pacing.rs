//! How much a destination may hold: what raises its throughput, and nothing more.
//!
//! A destination left for the worker to size has no fixed buffer. Its egress thread measures the link -- how fast
//! the destination acknowledges, and how long one batch takes to come back -- and from the two derives a target,
//! the bytes worth having queued and in flight: a bandwidth-delay product with headroom to probe for more. The egress
//! queue's budget is that target, so a slow destination's queue stays small and its source feels backpressure early,
//! while a fast one grows for as long as each step still raises the rate. The destination's own in-flight bound reads
//! the same number, so one figure governs the queue, a Kafka producer's copy and an HTTP pool's senders alike.
//!
//! The model is BBR's, cut to what a batch pipeline needs. The target is counted in whole batches, rounded up, since
//! the queue holds whole batches and a target short of one more holds none of it. It is never less than two batches,
//! as BBR's window is never less than four packets: with one batch on the wire the link idles for each acknowledgement,
//! however short the round trip. A rate sample taken while the source had nothing more to send says how fast the
//! source produces, not how fast the link carries, and may raise the estimate but never lower it: a slow source never
//! makes its destination look slow. Startup doubles the target each round trip until the rate stops growing; after
//! that an eight-step cycle probes at 1.25 and drains at 0.75, so a link that gets faster or slower is followed. A
//! least round trip ten seconds old is measured again, as BBR's ProbeRTT does: the target drops to one batch until a
//! batch sent alone comes back.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

/// How long the least round trip is kept before it is measured again.
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

/// What the model knew when a batch was handed to the destination, kept with the batch until it comes back.
#[derive(Clone, Copy, Debug)]
pub struct Sent {
    /// When the batch was handed over, and the bytes handed over before it.
    at: Instant,
    dispatched: u64,
    /// Bytes acknowledged by then, and when the last of them was.
    delivered: u64,
    delivered_at: Instant,
}

/// One delivery, as the model is told of it.
#[derive(Clone, Copy, Debug)]
pub struct Completion {
    /// The bytes acknowledged.
    pub bytes: usize,
    /// What [`Pacing::dispatched`] answered for the batch.
    pub sent: Sent,
    /// The source was not refused room in the queue before this batch went, so the budget, and with it the link,
    /// was not the bound on this sample: the source was.
    pub app_limited: bool,
}

/// The pacing model for one destination.
#[derive(Debug)]
pub struct Pacing {
    /// One batch: the target is counted in these.
    batch: usize,
    /// The most the target may be right now: the destination's fair share of the pool, or a stated buffer.
    ceiling: usize,
    target: usize,
    /// Bytes handed to the destination so far, and bytes it acknowledged, with when the last of them was.
    dispatched: u64,
    delivered: u64,
    delivered_at: Option<Instant>,
    /// The least round trip, and when it was taken: it stands for [`RTT_WINDOW`].
    least_rtt: Option<(Duration, Instant)>,
    /// While the round trip is being measured again: `dispatched` when that began. It ends once a batch handed over
    /// after that, and so sent alone, comes back.
    probing_rtt_from: Option<u64>,
    /// Rate samples, in bytes a second, within the rate window, oldest first. The greatest is the estimate.
    rates: VecDeque<(Instant, f64)>,
    /// `delivered` when this round trip began: it ends once a batch sent after that comes back.
    round_starts_at: u64,
    rounds: u32,
    /// Whether a sample this round was the link's, not the source's: only such a round can say the rate stopped
    /// growing.
    round_link_bound: bool,
    /// Whether startup is over, and what the rate was at the last round where it grew enough, with how many rounds
    /// have passed since.
    full_pipe: bool,
    rate_at_last_growth: f64,
    rounds_without_growth: u32,
}

impl Pacing {
    /// A model whose target is counted in batches of `batch` bytes, starts at two of them, and never passes
    /// `ceiling`.
    pub fn new(batch: usize, ceiling: usize) -> Pacing {
        let batch = batch.max(1);
        let ceiling = ceiling.max(batch);
        Pacing {
            batch,
            ceiling,
            target: (2 * batch).min(ceiling),
            dispatched: 0,
            delivered: 0,
            delivered_at: None,
            least_rtt: None,
            probing_rtt_from: None,
            rates: VecDeque::new(),
            round_starts_at: 0,
            rounds: 0,
            round_link_bound: false,
            full_pipe: false,
            rate_at_last_growth: 0.0,
            rounds_without_growth: 0,
        }
    }

    /// The bytes the destination may hold now: queued and in flight together.
    pub fn target(&self) -> usize {
        self.target
    }

    /// What the egress queue may hold: the target, and one batch ready behind it, so the next is made while the
    /// last is on the wire. Never past the ceiling.
    pub fn queue_bytes(&self) -> usize {
        (self.target + self.batch).min(self.ceiling).max(self.target)
    }

    /// The most the target may be. Moves as the pool's share does; the target follows at the next completion.
    pub fn set_ceiling(&mut self, ceiling: usize) {
        self.ceiling = ceiling.max(self.batch);
        self.target = self.target.clamp(self.batch, self.ceiling);
    }

    /// The least round trip seen lately, once there is one.
    pub fn round_trip(&self) -> Option<Duration> {
        self.least_rtt.map(|(rtt, _)| rtt)
    }

    /// How far a probe above the link's measured capacity would stand below the ceiling, in bytes: 0 once the link
    /// carries at least the ceiling over the probe's gain. A model held at its ceiling measures no more than the
    /// ceiling lets through, so this is as near as it can come to saying the ceiling, not the link, is the bound.
    pub fn headroom(&self) -> usize {
        let wanted = self
            .round_trip()
            .map_or(0.0, |rtt| PROBE_CYCLE[0] * self.rate() * rtt.as_secs_f64());
        if wanted >= self.ceiling as f64 {
            0
        } else {
            self.ceiling - wanted.max(0.0) as usize
        }
    }

    /// The greatest delivery rate seen lately, in bytes a second.
    pub fn rate(&self) -> f64 {
        self.rates.iter().map(|(_, r)| *r).fold(0.0, f64::max)
    }

    /// A batch of `bytes` was handed to the destination at `now`. The answer goes back with its [`Completion`].
    pub fn dispatched(&mut self, bytes: usize, now: Instant) -> Sent {
        let sent = Sent {
            at: now,
            dispatched: self.dispatched,
            delivered: self.delivered,
            delivered_at: *self.delivered_at.get_or_insert(now),
        };
        self.dispatched += bytes as u64;
        sent
    }

    /// A batch came back acknowledged. Updates the estimates and the target.
    pub fn completed(&mut self, c: Completion, now: Instant) {
        self.delivered += c.bytes as u64;
        let rtt = now.saturating_duration_since(c.sent.at);
        let expired = self
            .least_rtt
            .is_some_and(|(_, at)| now.saturating_duration_since(at) > RTT_WINDOW);
        if expired || self.least_rtt.is_none_or(|(least, _)| rtt <= least) {
            self.least_rtt = Some((rtt, now));
        }
        // BBR's ProbeRTT. Every batch on the wire has waited behind another, so the least round trip lately is
        // queueing as much as distance; one sent alone says what the link's own round trip is now.
        let probe_ended = self.probing_rtt_from.is_some_and(|from| c.sent.dispatched >= from);
        if probe_ended {
            self.probing_rtt_from = None;
            if let Some((_, at)) = &mut self.least_rtt {
                *at = now;
            }
        } else if expired && self.probing_rtt_from.is_none() {
            self.probing_rtt_from = Some(self.dispatched);
        }
        let least_rtt = self.round_trip().unwrap_or(rtt).max(Duration::from_micros(1));
        // While the target is held to one batch, the link is not what bounds the rate.
        let app_limited = c.app_limited || self.probing_rtt_from.is_some() || probe_ended;

        // BBR's sample: what was acknowledged while this batch was out, over at least its own round trip. Acks come
        // in bursts -- a Kafka producer reports a batch only once its last record lands -- and a rate taken between
        // two acks microseconds apart would read as a link far faster than any.
        self.delivered_at = Some(now);
        let interval = now.saturating_duration_since(c.sent.delivered_at).max(rtt);
        let rate = if interval.is_zero() {
            0.0
        } else {
            (self.delivered - c.sent.delivered) as f64 / interval.as_secs_f64()
        };

        let window = (least_rtt * RATE_WINDOW_ROUND_TRIPS).clamp(RATE_WINDOW_LEAST, RATE_WINDOW_MOST);
        if app_limited {
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
            self.round_link_bound = true;
        }

        // BBR's round: it ends once a batch sent after it began comes back, which is a round trip whatever the
        // batches' sizes.
        if c.sent.delivered >= self.round_starts_at {
            self.round_starts_at = self.delivered;
            self.rounds += 1;
            self.end_round();
        }
        self.retarget(least_rtt);
    }

    /// A round trip ended: judge whether the pipe is full yet.
    fn end_round(&mut self) {
        let link_bound = std::mem::take(&mut self.round_link_bound);
        // A round the source set the pace of says nothing of whether the link has more.
        if self.full_pipe || !link_bound {
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
        if self.probing_rtt_from.is_some() {
            self.target = self.batch.min(self.ceiling);
            return;
        }
        let bdp = self.rate() * least_rtt.as_secs_f64();
        // In whole batches, rounded up: the queue holds whole batches, so a target a little under three would hold
        // two and the estimate, measured from what is held, would fall to two; and a probe a quarter over two
        // batches must still admit a third, or a link held to two is never measured carrying three.
        let batch = self.batch as f64;
        let wanted = (self.gain() * bdp / batch - 0.01).ceil().max(2.0) * batch;
        let wanted = if wanted.is_finite() && wanted > 0.0 {
            wanted.min(usize::MAX as f64) as usize
        } else {
            0
        };
        self.target = wanted.clamp(self.batch, self.ceiling);
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
        // (lands at, as sent), in landing order.
        let mut in_flight: VecDeque<(Instant, Sent)> = VecDeque::new();
        let mut link_free_at = start;
        let mut targets = Vec::new();
        let mut next_round = start + rtt;
        while targets.len() < rounds {
            let held = in_flight.len() * BATCH;
            if held == 0 || held + BATCH <= p.target() {
                let sent = p.dispatched(BATCH, now);
                let lands = (now + rtt).max(link_free_at + per_batch);
                link_free_at = lands;
                in_flight.push_back((lands, sent));
                continue;
            }
            let (lands, sent) = in_flight.pop_front().expect("something in flight");
            now = lands;
            p.completed(
                Completion {
                    bytes: BATCH,
                    sent,
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
    fn the_target_starts_at_two_batches_and_doubles_while_the_pipe_fills() {
        let mut p = Pacing::new(BATCH, 1024 * MIB);
        assert_eq!(p.target(), 2 * BATCH);
        // 200ms away, a link that carries 100 MiB/s: a bandwidth-delay product of 20 MiB.
        let targets = drive(&mut p, Duration::from_millis(200), 100.0 * MIB as f64, 4, false);
        assert!(
            targets[1] >= 4 * BATCH,
            "doubled once a round's samples are in: {targets:?}"
        );
        assert!(targets[2] > targets[1], "and on: {targets:?}");
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
        // Two batches, and a third on a probe: one batch takes four seconds to send.
        assert!(targets[5..].iter().all(|t| *t <= 3 * BATCH), "{targets:?}");
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
            let sent = p.dispatched(BATCH, now);
            p.completed(
                Completion {
                    bytes: BATCH,
                    sent,
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
    fn a_stall_lets_the_rate_lapse_and_the_target_fall() {
        let rtt = Duration::from_millis(200);
        let mut p = Pacing::new(BATCH, 1024 * MIB);
        drive(&mut p, rtt, 100.0 * MIB as f64, 30, false);
        assert!(p.target() > 3 * BATCH);
        // Then one batch comes back after a very long time, with others still waiting.
        let late = Instant::now() + Duration::from_secs(120);
        let sent = p.dispatched(BATCH, late - Duration::from_secs(30));
        p.completed(
            Completion {
                bytes: BATCH,
                sent,
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
    fn acks_that_land_together_read_as_what_was_in_flight_over_the_round_trip() {
        let rtt = Duration::from_millis(200);
        let mut p = Pacing::new(BATCH, 1024 * MIB);
        let start = Instant::now();
        let sent: Vec<Sent> = (0..8).map(|_| p.dispatched(BATCH, start)).collect();
        for s in sent {
            p.completed(
                Completion {
                    bytes: BATCH,
                    sent: s,
                    app_limited: false,
                },
                start + rtt,
            );
        }
        let most = 8.0 * BATCH as f64 / rtt.as_secs_f64();
        assert!(p.rate() <= most * 1.01, "{} vs {most}", p.rate());
        assert!(p.target() <= 2 * 8 * BATCH + BATCH, "{}", p.target());
    }

    #[test]
    fn a_quiet_source_does_not_end_startup() {
        let rtt = Duration::from_millis(200);
        let mut p = Pacing::new(BATCH, 1024 * MIB);
        drive(&mut p, rtt, 100.0 * MIB as f64, 20, true);
        assert!(!p.full_pipe, "every round was the source's");
        let targets = drive(&mut p, rtt, 100.0 * MIB as f64, 16, false);
        assert!(targets[8..].iter().max().unwrap() >= &(5 * BATCH), "{targets:?}");
    }

    #[test]
    fn a_target_held_to_one_batch_climbs_once_the_link_has_more() {
        let rtt = Duration::from_millis(200);
        let mut p = Pacing::new(BATCH, 1024 * MIB);
        // Startup over, with the estimate at a single batch: the way a burst of quiet rounds could leave it.
        p.full_pipe = true;
        // 100 MiB/s, 200 ms: 20 MiB, five batches, on the wire.
        let targets = drive(&mut p, rtt, 100.0 * MIB as f64, 80, false);
        assert!(*targets.last().unwrap() >= 5 * BATCH, "{targets:?}");
    }

    #[test]
    fn a_link_wider_than_the_ceiling_leaves_no_headroom_and_a_narrow_one_does() {
        let rtt = Duration::from_millis(100);
        // 64 batches a second over 100 ms: 6.4 batches on the wire, with a ceiling of 4.
        let mut wide = Pacing::new(BATCH, 4 * BATCH);
        drive(&mut wide, rtt, 64.0 * BATCH as f64, 200, false);
        assert_eq!(wide.headroom(), 0);
        // 16 a second: 1.6 on the wire, well under the ceiling of 8.
        let mut narrow = Pacing::new(BATCH, 8 * BATCH);
        drive(&mut narrow, rtt, 16.0 * BATCH as f64, 200, false);
        assert!(narrow.headroom() > 4 * BATCH, "{}", narrow.headroom());
    }

    #[test]
    fn the_ceiling_moves_the_target_at_once() {
        let mut p = Pacing::new(BATCH, 1024 * MIB);
        drive(&mut p, Duration::from_millis(200), 100.0 * MIB as f64, 30, false);
        assert!(p.target() > 2 * BATCH);
        p.set_ceiling(2 * BATCH);
        assert_eq!(p.target(), 2 * BATCH);
        p.set_ceiling(0);
        assert_eq!(p.target(), BATCH, "never below one batch");
    }

    #[test]
    fn a_product_under_one_batch_keeps_two_on_the_wire() {
        // 1ms away, carrying 100 MiB/s: a product of 100 KiB, but one batch takes 40ms to send.
        let mut p = Pacing::new(BATCH, 1024 * MIB);
        let targets = drive(&mut p, Duration::from_millis(1), 100.0 * MIB as f64, 60, false);
        assert!(p.full_pipe, "startup ended");
        assert!(targets.iter().all(|t| *t >= 2 * BATCH), "{targets:?}");
        assert!(targets[20..].iter().all(|t| *t <= 3 * BATCH), "{targets:?}");
    }

    #[test]
    fn a_least_round_trip_ten_seconds_old_is_measured_again_with_one_batch_alone() {
        let rtt = Duration::from_millis(200);
        let mut p = Pacing::new(BATCH, 1024 * MIB);
        drive(&mut p, rtt, 100.0 * MIB as f64, 30, false);
        let rate = p.rate();
        let target = p.target();
        assert!(target > 3 * BATCH);

        // Well past ten seconds on, a batch that waited behind others comes back.
        let later = Instant::now() + Duration::from_secs(30);
        let queued = p.dispatched(BATCH, later - Duration::from_millis(600));
        p.completed(
            Completion {
                bytes: BATCH,
                sent: queued,
                app_limited: false,
            },
            later,
        );
        assert_eq!(p.target(), BATCH, "held to one batch while it is measured");
        assert_eq!(p.queue_bytes(), 2 * BATCH, "with one ready behind it");

        // One sent alone comes back a round trip later: that is the round trip, and the target is back.
        let alone = p.dispatched(BATCH, later);
        p.completed(
            Completion {
                bytes: BATCH,
                sent: alone,
                app_limited: false,
            },
            later + rtt,
        );
        assert_eq!(p.round_trip(), Some(rtt));
        assert!(p.rate() >= rate * 0.99, "the probe's samples never lowered the rate");
        assert!(p.target() >= target * 3 / 4, "{} vs {target}", p.target());
    }
}
