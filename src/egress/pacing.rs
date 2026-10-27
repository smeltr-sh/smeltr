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
//!
//! A destination that asks to be sent less -- a 429, a 503, a bulk item refused for a full queue -- is answered as
//! BBRv2 answers loss: once a round trip, the most it may hold is cut to seven tenths of what it held. Each probe it
//! goes through without asking, held to that, grows it back by a batch, then two, then four, until it no longer
//! binds, or is past the ceiling and forgotten. The rate alone would not see it: a throttled destination answers fast.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use super::{Queue, note_queue_budget};
use crate::core::id::DestId;
use crate::metrics::MetricsHandle;

/// How long the least round trip is kept before it is measured again.
const RTT_WINDOW: Duration = Duration::from_secs(10);

/// How many round trips the greatest rate is kept for. Samples lapse only as batches come back with others still
/// waiting: a destination the source left idle keeps its estimate, one that crawls loses it within [`RTT_WINDOW`],
/// whatever its round trip now reads.
const RATE_WINDOW_ROUND_TRIPS: u32 = 10;
/// The least time the greatest rate is kept, however short the round trip.
const RATE_WINDOW_LEAST: Duration = Duration::from_secs(1);
/// The most time the greatest rate is kept, however long the round trip.
const RATE_WINDOW_MOST: Duration = RTT_WINDOW;

/// Startup ends once the rate has grown less than this over [`FULL_PIPE_ROUNDS`] round trips in a row.
const FULL_PIPE_GROWTH: f64 = 1.25;
/// The round trips in a row the rate must stop growing for before startup ends.
const FULL_PIPE_ROUNDS: u32 = 3;

/// The gain while the pipe is still filling.
const STARTUP_GAIN: f64 = 2.0;

/// The steady-state cycle, one step per round trip: probe, drain, cruise.
const PROBE_CYCLE: [f64; 8] = [1.25, 0.75, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0];

/// What a destination that asked to be sent less is cut to: BBRv2's beta.
const THROTTLE_CUT: f64 = 0.7;

/// What the model knew when a batch was handed to the destination, kept with the batch until it comes back.
#[derive(Clone, Copy, Debug)]
pub struct Sent {
    /// When the batch was handed over.
    at: Instant,
    /// The bytes handed over before it.
    dispatched: u64,
    /// The bytes acknowledged by then.
    delivered: u64,
    /// When the last of those was acknowledged.
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
    /// The destination asked to be sent less while delivering it.
    pub throttled: bool,
}

/// The pacing model for one destination.
#[derive(Debug)]
pub struct Pacing {
    /// One batch: the target is counted in these.
    batch: usize,
    /// The most the target may be right now: the destination's fair share of the pool, or a stated buffer.
    ceiling: usize,
    /// The bytes worth having queued and in flight: the rate times the least round trip, times this round's gain, in whole
    /// batches.
    target: usize,
    /// Bytes handed to the destination so far.
    dispatched: u64,
    /// Bytes it acknowledged so far.
    delivered: u64,
    /// When the last of those was acknowledged.
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
    /// Round trips ended so far: past startup, which step of [`PROBE_CYCLE`] this one is.
    rounds: u32,
    /// Whether a sample this round was the link's, not the source's: only such a round can say the rate stopped
    /// growing.
    round_link_bound: bool,
    /// Whether startup is over.
    full_pipe: bool,
    /// The rate at the last round where it grew by [`FULL_PIPE_GROWTH`].
    rate_at_last_growth: f64,
    /// Rounds since then, toward [`FULL_PIPE_ROUNDS`].
    rounds_without_growth: u32,
    /// The most the target may be since the destination asked to be sent less, in whole batches: BBRv2's
    /// `inflight_hi`. Cut at most once a round trip, and grown at the end of a probe it did not ask in.
    throttled_at: Option<usize>,
    /// Whether the destination asked to be sent less this round: `throttled_at` is cut once a round.
    round_throttled: bool,
    /// Batches `throttled_at` grows by after the next probe without a throttle. Doubles each time.
    regrowth: usize,
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
            throttled_at: None,
            round_throttled: false,
            regrowth: 1,
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
        if c.throttled && !self.round_throttled {
            let held = self.dispatched.saturating_sub(self.delivered) as f64;
            let batches = (THROTTLE_CUT * held / self.batch as f64).floor().max(1.0);
            self.throttled_at = Some((batches as usize).saturating_mul(self.batch));
            self.round_throttled = true;
            self.regrowth = 1;
            // The destination said where its limit is: startup has found it.
            self.full_pipe = true;
        }
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
        // A probe the destination took everything in, held at its last limit: try it with more. Once a cycle, so a
        // destination that asks for less is asked again only as often as BBR probes.
        let probed = self.full_pipe && PROBE_CYCLE[(self.rounds as usize).wrapping_sub(1) % PROBE_CYCLE.len()] > 1.0;
        if !std::mem::take(&mut self.round_throttled)
            && probed
            && let Some(at) = self.throttled_at
            && self.target >= at
        {
            let grown = at.saturating_add(self.regrowth.saturating_mul(self.batch));
            self.throttled_at = (grown < self.ceiling).then_some(grown);
            self.regrowth = self.regrowth.saturating_mul(2);
        }
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
        let wanted = self.throttled_at.map_or(wanted, |at| wanted.min(at));
        self.target = wanted.clamp(self.batch, self.ceiling);
    }
}

/// The pacing model for one destination's queue, and what it reports.
pub(super) struct Pacer {
    /// The model whose target is the queue's budget.
    model: Pacing,
    /// How far the link stood below the share, averaged over each stretch and written at its end.
    headroom: Headroom,
    /// Where the budget and headroom are reported, under this destination's id.
    metrics: MetricsHandle,
    id: DestId,
}

impl Pacer {
    /// Starts the model at `floor` bytes, one batch, and sets the queue's budget to it.
    pub(super) fn new(floor: usize, queue: &Queue, metrics: &MetricsHandle, id: DestId) -> Pacer {
        let model = Pacing::new(floor, queue.share());
        queue.set_budget(model.queue_bytes());
        note_queue_budget(metrics, id, queue);
        Pacer {
            model,
            headroom: Headroom::default(),
            metrics: metrics.clone(),
            id,
        }
    }

    /// A batch of `bytes` handed to the destination: what the model is told when it is acknowledged.
    pub(super) fn dispatched(&mut self, bytes: usize, app_limited: bool) -> Completion {
        Completion {
            bytes,
            sent: self.model.dispatched(bytes, Instant::now()),
            app_limited,
            throttled: false,
        }
    }

    /// Tell the model a batch came back, and carry its target to the queue and the metrics when it moves. The ceiling
    /// is read each time: the pool's share moves as other queues come and go. The model's headroom under the ceiling
    /// is averaged over time and reported every [`HEADROOM_EVERY`]: none for long is a destination memory holds back.
    pub(super) fn completed(&mut self, queue: &Queue, c: Completion) {
        let before = self.model.target();
        let ceiling = queue.share();
        self.model.set_ceiling(ceiling);
        self.model.completed(c, Instant::now());
        if self.model.target() != before {
            queue.set_budget(self.model.queue_bytes());
            note_queue_budget(&self.metrics, self.id, queue);
        }
        let share = self.model.headroom() as f64 / ceiling.max(1) as f64;
        if let Some(mean) = self.headroom.add(share, Instant::now()) {
            let label = self.id.to_string();
            self.metrics.gauge(
                "smeltr_destination_pace_headroom_ratio",
                &[("destination", label.as_str())],
                mean,
            );
        }
    }
}

/// How often the averaged headroom is reported: the leader's finest bucket, so each bucket reads one stretch. The
/// estimate dips for a moment as old rate samples lapse and the round trip is measured again; a mean over a stretch
/// is not moved by that, where the bucket's greatest reading, which a gauge keeps, would be.
const HEADROOM_EVERY: Duration = Duration::from_secs(10);

/// The headroom, weighted by how long each reading stood, since the last report.
#[derive(Default)]
struct Headroom {
    /// When this stretch began, and the last reading with when it was taken.
    since: Option<Instant>,
    last: Option<(Instant, f64)>,
    /// The sum of each reading times the seconds it stood.
    weighted: f64,
}

impl Headroom {
    /// Takes the reading `share` at `now`, and answers the mean over the stretch once it is [`HEADROOM_EVERY`] long.
    fn add(&mut self, share: f64, now: Instant) -> Option<f64> {
        let since = *self.since.get_or_insert(now);
        if let Some((at, last)) = self.last {
            self.weighted += last * now.saturating_duration_since(at).as_secs_f64();
        }
        self.last = Some((now, share));
        let span = now.saturating_duration_since(since);
        if span < HEADROOM_EVERY {
            return None;
        }
        let mean = self.weighted / span.as_secs_f64();
        self.since = Some(now);
        self.weighted = 0.0;
        Some(mean)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIB: usize = 1024 * 1024;
    const BATCH: usize = 4 * MIB;

    #[test]
    fn headroom_is_reported_as_its_mean_over_time_so_a_moment_s_dip_barely_moves_it() {
        let mut h = Headroom::default();
        let start = Instant::now();
        let at = |s: f64| start + Duration::from_secs_f64(s);
        assert_eq!(h.add(0.0, at(0.0)), None);
        assert_eq!(h.add(0.8, at(4.0)), None, "the estimate dips");
        assert_eq!(h.add(0.0, at(4.5)), None);
        let mean = h.add(0.0, at(10.0)).expect("a stretch is ten seconds");
        assert!((mean - 0.04).abs() < 1e-9, "half a second at 0.8 over ten: {mean}");
        assert_eq!(h.add(0.0, at(15.0)), None, "the next stretch starts afresh");
        assert_eq!(h.add(0.0, at(20.0)), Some(0.0));
    }

    /// A link `rtt` away that carries `capacity` bytes a second, fed by a source with plenty to send, for `rounds`
    /// round trips. The destination holds batches up to the target, as the queue does: a new batch goes out the
    /// moment the target has room for it, and the link lands them one transmission time apart, the first a round
    /// trip after it was sent. Answers the target at each round trip's end.
    fn drive(p: &mut Pacing, rtt: Duration, capacity: f64, rounds: usize, app_limited: bool) -> Vec<usize> {
        drive_throttling(p, rtt, capacity, rounds, app_limited, usize::MAX)
    }

    /// As [`drive`], to a destination that asks to be sent less for every batch sent while it held more than
    /// `limit` bytes.
    fn drive_throttling(
        p: &mut Pacing,
        rtt: Duration,
        capacity: f64,
        rounds: usize,
        app_limited: bool,
        limit: usize,
    ) -> Vec<usize> {
        let per_batch = Duration::from_secs_f64(BATCH as f64 / capacity);
        let start = Instant::now();
        let mut now = start;
        // (lands at, as sent, throttled), in landing order.
        let mut in_flight: VecDeque<(Instant, Sent, bool)> = VecDeque::new();
        let mut link_free_at = start;
        let mut targets = Vec::new();
        let mut next_round = start + rtt;
        while targets.len() < rounds {
            let held = in_flight.len() * BATCH;
            if held == 0 || held + BATCH <= p.target() {
                let sent = p.dispatched(BATCH, now);
                let lands = (now + rtt).max(link_free_at + per_batch);
                link_free_at = lands;
                in_flight.push_back((lands, sent, held + BATCH > limit));
                continue;
            }
            let (lands, sent, throttled) = in_flight.pop_front().expect("something in flight");
            now = lands;
            p.completed(
                Completion {
                    bytes: BATCH,
                    sent,
                    app_limited,
                    throttled,
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
                    throttled: false,
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
                throttled: false,
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
                    throttled: false,
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
                throttled: false,
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
                throttled: false,
            },
            later + rtt,
        );
        assert_eq!(p.round_trip(), Some(rtt));
        assert!(p.rate() >= rate * 0.99, "the probe's samples never lowered the rate");
        assert!(p.target() >= target * 3 / 4, "{} vs {target}", p.target());
    }

    #[test]
    fn a_destination_that_asks_for_less_is_held_under_its_limit_and_tried_with_more_after() {
        let rtt = Duration::from_millis(200);
        // 100 MiB/s, 200 ms: five batches on the wire, but the destination asks for less past three.
        let mut p = Pacing::new(BATCH, 1024 * MIB);
        let targets = drive_throttling(&mut p, rtt, 100.0 * MIB as f64, 200, false, 3 * BATCH);
        assert!(p.full_pipe, "a throttle ends startup");
        let settled = &targets[40..];
        assert!(
            settled.iter().all(|t| *t <= 5 * BATCH),
            "never runs on to the link's product for long: {targets:?}"
        );
        assert!(
            settled.iter().filter(|t| **t <= 3 * BATCH).count() > settled.len() / 2,
            "held at the limit most of the time: {targets:?}"
        );
        assert!(
            settled.iter().any(|t| *t > 3 * BATCH),
            "and tried past it now and then: {targets:?}"
        );

        // It stops asking: the limit grows back until the link, not the limit, sets the target.
        let targets = drive(&mut p, rtt, 100.0 * MIB as f64, 80, false);
        assert!(p.throttled_at.is_none_or(|at| at > 5 * BATCH), "{:?}", p.throttled_at);
        assert!(targets[60..].iter().any(|t| *t >= 5 * BATCH), "{targets:?}");
    }
}
