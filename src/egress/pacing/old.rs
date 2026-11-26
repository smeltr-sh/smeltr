//! main's pacing model, kept beside the BBRv3 port only so the two can be compared on the same simulated paths.
#![allow(dead_code)]
use std::collections::VecDeque;
use std::time::{Duration, Instant};

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
    /// The bytes worth having queued and in flight: the rate times the least round trip, times this round's gain,
    /// in whole batches.
    target: usize,
    /// Bytes handed to the destination so far.
    dispatched: u64,
    /// Bytes it acknowledged so far.
    delivered: u64,
    /// Bytes handed over whose batch failed, and so went back to be sent again: in flight no longer, and never
    /// acknowledged.
    abandoned: u64,
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
    /// inflight_hi. Cut at most once a round trip, and grown at the end of a probe it did not ask in.
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
            abandoned: 0,
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

    /// A batch handed over failed but for `landed` of its bytes, and what failed goes back to be sent again: none of
    /// it is in flight any more, and what landed counts as delivered. No sample: its round trip is the failure's, not
    /// the link's.
    pub fn abandoned(&mut self, c: Completion, landed: usize) {
        let landed = landed.min(c.bytes);
        self.delivered += landed as u64;
        self.abandoned += (c.bytes - landed) as u64;
    }

    /// A batch came back acknowledged. Updates the estimates and the target.
    pub fn completed(&mut self, c: Completion, now: Instant) {
        if c.throttled && !self.round_throttled {
            let held = self.dispatched.saturating_sub(self.delivered + self.abandoned) as f64;
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
