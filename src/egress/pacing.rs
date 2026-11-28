//! How much a destination may hold, and how fast it is sent: BBR, as the IETF specifies it
//! ([draft-ietf-ccwg-bbr](https://datatracker.ietf.org/doc/draft-ietf-ccwg-bbr/), version 3 of the algorithm).
//!
//! Each paced destination is one BBR flow. Its egress thread is the sender: it hands a batch over only while what is
//! in flight leaves room in the congestion window (cwnd) and the pacing rate says the batch is due, and each
//! acknowledgment is a rate sample that drives the model. The model and its state machine are the draft's, in its
//! names, so a line here can be read against the draft's pseudocode:
//!
//! ```mermaid
//! stateDiagram-v2
//!     [*] --> Startup
//!     Startup --> Drain: pipe full (rate plateau or loss)
//!     Drain --> ProbeBW_DOWN: inflight at BDP, or 3 rounds
//!     ProbeBW_DOWN --> ProbeBW_CRUISE: inflight under headroom and BDP
//!     ProbeBW_DOWN --> ProbeBW_REFILL: time to probe
//!     ProbeBW_CRUISE --> ProbeBW_REFILL: time to probe
//!     ProbeBW_REFILL --> ProbeBW_UP: one round
//!     ProbeBW_UP --> ProbeBW_DOWN: rate plateau, or too much loss
//!     ProbeBW_DOWN --> ProbeRTT: min_rtt 10 s old
//!     ProbeRTT --> ProbeBW_CRUISE: 200 ms and a round
//!     ProbeRTT --> Startup: pipe not yet full
//! ```
//!
//! What the draft says of packets, this says of batches, and where that forces a reading it is this:
//!
//! - A packet is a batch: egress hands over and hears back about nothing smaller. So C.SMSS and the send quantum are
//!   both the batch size the worker cut, and every count the draft makes in packets is a count of batches. The
//!   offload budget is TCP's, three quanta.
//! - Each batch is acknowledged on its own, as with selective acknowledgments, so C.has_selective_acks holds and
//!   each lost batch is one loss event.
//! - Loss is the bytes the destination was asked to send less for (a 429, a 503, a bulk item refused for a full
//!   queue). Those that go back to be sent again are lost; those delivered all the same are delivered and lost both,
//!   as an ECN CE mark is congestion on data that arrived ("MUST treat any CE marks as congestion"). Any other failure,
//!   a reset or an error, says nothing of how full the path is: its bytes leave flight and signal nothing. What was
//!   delivered of a batch that failed in part is acknowledged. A loss recovery lasts until a batch sent after the loss is
//!   acknowledged. Nothing here tells a loss to be spurious, so the draft's undo has no counterpart.
//! - The destination's share of the worker's memory pool, or the buffer it states, bounds what it may hold as a
//!   receiver's window does: the sender holds to the lesser of it and cwnd, and a link the share holds back reads as
//!   the bottleneck, not as a source with nothing to send (the draft's receiver flow control limits).
//! - The initial window is ten batches, within the share.
//! - A ProbeBW cycle that runs [`MAX_BW_CYCLE_TIMER`] without ending ends the max_bw filter's slot all the same. The
//!   draft counts the filter's window in cycles and a cycle mostly in round trips, and leaves a fall in capacity to
//!   loss. Under TCP a slower network is never loss here, only longer round trips, so without this a link that
//!   narrows behind a deep enough queue keeps its old max_bw for good: the round trips it stretches keep the cycle
//!   from ending.
//!
//! The egress queue holds the window and one batch ready behind it, so the next batch is made while the last is on
//! the wire.

use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::time::{Duration, Instant};

use super::{Queue, note_queue_budget};
use crate::core::id::DestId;
use crate::metrics::MetricsHandle;

/// BBR.LossThresh: the most loss a round trip may see while probing before inflight is judged too high.
const LOSS_THRESH: f64 = 0.02;
/// BBR.Beta: the multiplicative decrease on a round trip with loss.
const BETA: f64 = 0.7;
/// BBR.Headroom: the share of inflight_longterm left free for other flows while cruising.
const HEADROOM: f64 = 0.15;
/// BBR.MinPipeCwnd, in packets.
const MIN_PIPE_CWND: f64 = 4.0;
/// C.InitialCwnd, in packets.
const INITIAL_CWND: f64 = 10.0;
/// The offload budget, in send quanta: TCP's three.
const OFFLOAD_BUDGET: f64 = 3.0;

/// BBR.StartupPacingGain: 4 ln 2, the least gain that doubles the sending rate each round.
const STARTUP_PACING_GAIN: f64 = 2.77;
/// BBR.DrainPacingGain.
const DRAIN_PACING_GAIN: f64 = 0.5;
/// The pacing gains of ProbeBW_DOWN and ProbeBW_UP; CRUISE and REFILL pace at 1.
const PROBE_DOWN_PACING_GAIN: f64 = 0.9;
const PROBE_UP_PACING_GAIN: f64 = 1.25;
/// BBR.DefaultCwndGain, and the cwnd gain of ProbeBW_UP.
const DEFAULT_CWND_GAIN: f64 = 2.0;
const PROBE_UP_CWND_GAIN: f64 = 2.25;
/// BBR.PacingMarginPercent.
const PACING_MARGIN_PERCENT: f64 = 1.0;

/// BBR.MaxBwFilterLen, in ProbeBW cycles.
const MAX_BW_FILTER_LEN: u64 = 2;
/// The longest a ProbeBW cycle runs before the max_bw filter moves to its next slot regardless. A cycle on a healthy
/// path ends well inside it, so it acts only once round trips have stretched past what the draft's cycle allows.
const MAX_BW_CYCLE_TIMER: Duration = Duration::from_secs(10);
/// BBR.ExtraAckedFilterLen, in round trips.
const EXTRA_ACKED_FILTER_LEN: u64 = 10;
/// BBR.MinRTTFilterLen.
const MIN_RTT_FILTER_LEN: Duration = Duration::from_secs(10);
/// BBR.ProbeRTTCwndGain.
const PROBE_RTT_CWND_GAIN: f64 = 0.5;
/// BBR.ProbeRTTDuration.
const PROBE_RTT_DURATION: Duration = Duration::from_millis(200);
/// BBR.StartupFullLossCnt.
const STARTUP_FULL_LOSS_CNT: u32 = 6;
/// The growth, and the rounds without it, that say the pipe is full (CheckFullBWReached).
const FULL_BW_GROWTH: f64 = 1.25;
const FULL_BW_COUNT: u32 = 3;

/// What the model knew when a batch was handed to the destination, kept with the batch until it comes back: the
/// draft's per-packet state.
#[derive(Clone, Copy, Debug)]
pub struct Sent {
    /// P.send_time.
    send_time: Instant,
    /// P.delivered.
    delivered: u64,
    /// P.delivered_time.
    delivered_time: Instant,
    /// P.first_send_time.
    first_send_time: Instant,
    /// P.is_app_limited.
    is_app_limited: bool,
    /// P.tx_in_flight: C.inflight just after this batch was sent.
    tx_in_flight: u64,
    /// P.lost.
    lost: u64,
    /// The bytes sent before this batch: its place in the send order, as a packet number is.
    sent_before: u64,
}

/// One delivery, as the model is told of it.
#[derive(Clone, Copy, Debug)]
pub struct Completion {
    /// The bytes acknowledged.
    pub bytes: usize,
    /// What [`Pacing::dispatched`] answered for the batch.
    pub sent: Sent,
    /// The destination asked to be sent less while delivering it.
    pub throttled: bool,
}

/// Whether a batch of some size may be sent now.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Gate {
    Send,
    /// The pacing rate has it due this much later.
    Wait(Duration),
    /// What is in flight leaves it no room: it waits for an acknowledgment.
    Full,
}

/// BBR.state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum State {
    Startup,
    Drain,
    ProbeBwDown,
    ProbeBwCruise,
    ProbeBwRefill,
    ProbeBwUp,
    ProbeRtt,
}

impl State {
    fn is_probe_bw(self) -> bool {
        matches!(
            self,
            State::ProbeBwDown | State::ProbeBwCruise | State::ProbeBwRefill | State::ProbeBwUp
        )
    }
}

/// A windowed max filter, Kathleen Nichols' algorithm as Linux's `lib/minmax.c` has it: the best, second best and
/// third best samples over the window, so the max can fall back as the best ages out. A window `win` long holds the
/// samples of `win` slots of virtual time, the current one and the `win - 1` before it, as the draft's BBR.max_bw
/// filter holds two ProbeBW cycles: a sample lapses once it is `win` old, where `lib/minmax.c` keeps it one longer.
#[derive(Clone, Copy, Debug, Default)]
struct MaxFilter {
    s: [(u64, f64); 3],
}

impl MaxFilter {
    fn reset(&mut self, t: u64, v: f64) -> f64 {
        self.s = [(t, v); 3];
        v
    }

    /// UpdateWindowedMaxFilter: take `v` at virtual time `t`, over a window `win` long.
    fn update(&mut self, win: u64, t: u64, v: f64) -> f64 {
        let val = (t, v);
        if v >= self.s[0].1 || t.saturating_sub(self.s[2].0) >= win {
            return self.reset(t, v);
        }
        if v >= self.s[1].1 {
            self.s[2] = val;
            self.s[1] = val;
        } else if v >= self.s[2].1 {
            self.s[2] = val;
        }
        let dt = t.saturating_sub(self.s[0].0);
        if dt >= win {
            self.s[0] = self.s[1];
            self.s[1] = self.s[2];
            self.s[2] = val;
            if t.saturating_sub(self.s[0].0) >= win {
                self.s[0] = self.s[1];
                self.s[1] = self.s[2];
                self.s[2] = val;
            }
        } else if self.s[1].0 == self.s[0].0 && dt > win / 4 {
            self.s[2] = val;
            self.s[1] = val;
        } else if self.s[2].0 == self.s[1].0 && dt > win / 2 {
            self.s[2] = val;
        }
        self.s[0].1
    }
}

/// RS: the rate sample taken on one acknowledgment.
#[derive(Clone, Copy, Debug, Default)]
struct RateSample {
    delivery_rate: f64,
    is_app_limited: bool,
    delivered: u64,
    prior_delivered: u64,
    newly_acked: u64,
    tx_in_flight: u64,
    lost: u64,
}

/// The BBR model and sender state for one destination.
#[derive(Debug)]
pub struct Pacing {
    /// C.SMSS: one batch.
    smss: f64,
    /// C.send_quantum: one batch.
    send_quantum: f64,
    /// The receiver's window: the destination's share of the pool, or its stated buffer.
    ceiling: f64,

    // Delivery rate sampling (draft §4.1).
    /// C.delivered, C.delivered_time, C.first_send_time.
    delivered: u64,
    delivered_time: Instant,
    first_send_time: Instant,
    /// C.lost: bytes lost, or marked by a destination that asked to be sent less.
    lost: u64,
    /// C.inflight.
    inflight: u64,
    /// The bytes sent so far.
    sent: u64,
    /// C.app_limited: 0, or C.delivered + C.inflight when the source last left a gap.
    app_limited: u64,
    /// C.min_rtt: the least round trip over the flow's life.
    lifetime_min_rtt: Option<Duration>,
    /// Whether cwnd was full at some point in this round trip, and in the last.
    cwnd_limited_now: bool,
    cwnd_limited_last: bool,
    /// While in loss recovery: the bytes sent when it began. It ends once a batch sent after that is acknowledged.
    recovery: Option<u64>,

    // Control parameters.
    /// C.cwnd, C.pacing_rate (bytes a second), C.next_send_time.
    cwnd: f64,
    pacing_rate: f64,
    next_send_time: Instant,
    prior_cwnd: f64,

    // The model.
    state: State,
    pacing_gain: f64,
    cwnd_gain: f64,
    round_count: u64,
    round_start: bool,
    next_round_delivered: u64,
    max_bw_filter: MaxFilter,
    cycle_count: u64,
    /// When the max_bw filter last moved to its next slot.
    cycle_ended_at: Instant,
    max_bw: f64,
    bw_shortterm: f64,
    bw: f64,
    min_rtt: Option<Duration>,
    min_rtt_stamp: Instant,
    min_rtt_expired: bool,
    extra_acked_interval_start: Instant,
    extra_acked_delivered: u64,
    extra_acked_filter: MaxFilter,
    extra_acked: f64,
    max_inflight: f64,
    inflight_longterm: f64,
    inflight_shortterm: f64,
    bw_latest: f64,
    inflight_latest: u64,
    loss_round_delivered: u64,
    loss_round_start: bool,
    is_loss_in_round: bool,
    loss_events_in_round: u32,
    full_bw: f64,
    full_bw_count: u32,
    full_bw_now: bool,
    full_bw_reached: bool,
    drain_start_round: u64,
    is_bw_probe_stopping: bool,
    check_probe_loss: bool,
    bw_probe_up_acked: f64,
    probe_up_acked_per_inc: f64,
    bw_probe_up_rounds: u32,
    rounds_since_probe_up: u64,
    bw_probe_wait: Duration,
    cycle_stamp: Instant,
    prev_probe_too_high: bool,
    prev_probe_precautionary: bool,
    probe_rtt_done_stamp: Option<Instant>,
    probe_rtt_round_done: bool,
    probe_rtt_cwnd: f64,
    idle_restart: bool,
    /// PickProbeWait's randomness: splitmix64.
    rng: u64,
}

impl Pacing {
    /// A flow whose send quantum is a batch of `batch` bytes, held to `ceiling`: OnInit.
    pub fn new(batch: usize, ceiling: usize, now: Instant) -> Pacing {
        let mut seed = RandomState::new().build_hasher();
        seed.write_usize(batch);
        Pacing::seeded(batch, ceiling, now, seed.finish())
    }

    fn seeded(batch: usize, ceiling: usize, now: Instant, rng: u64) -> Pacing {
        let (smss, send_quantum) = (batch.max(1) as f64, batch.max(1) as f64);
        let mut p = Pacing {
            smss,
            send_quantum,
            ceiling: (ceiling as f64).max(send_quantum),
            delivered: 0,
            delivered_time: now,
            first_send_time: now,
            lost: 0,
            inflight: 0,
            sent: 0,
            app_limited: 0,
            lifetime_min_rtt: None,
            cwnd_limited_now: false,
            cwnd_limited_last: false,
            recovery: None,
            cwnd: INITIAL_CWND * smss,
            pacing_rate: 0.0,
            next_send_time: now,
            prior_cwnd: 0.0,
            state: State::Startup,
            pacing_gain: STARTUP_PACING_GAIN,
            cwnd_gain: DEFAULT_CWND_GAIN,
            round_count: 0,
            round_start: false,
            next_round_delivered: 0,
            max_bw_filter: MaxFilter::default(),
            cycle_count: 0,
            cycle_ended_at: now,
            max_bw: 0.0,
            bw_shortterm: f64::INFINITY,
            bw: 0.0,
            min_rtt: None,
            min_rtt_stamp: now,
            min_rtt_expired: false,
            extra_acked_interval_start: now,
            extra_acked_delivered: 0,
            extra_acked_filter: MaxFilter::default(),
            extra_acked: 0.0,
            max_inflight: 0.0,
            inflight_longterm: f64::INFINITY,
            inflight_shortterm: f64::INFINITY,
            bw_latest: 0.0,
            inflight_latest: 0,
            loss_round_delivered: 0,
            loss_round_start: false,
            is_loss_in_round: false,
            loss_events_in_round: 0,
            full_bw: 0.0,
            full_bw_count: 0,
            full_bw_now: false,
            full_bw_reached: false,
            drain_start_round: 0,
            is_bw_probe_stopping: false,
            check_probe_loss: false,
            bw_probe_up_acked: 0.0,
            probe_up_acked_per_inc: f64::INFINITY,
            bw_probe_up_rounds: 0,
            rounds_since_probe_up: 0,
            bw_probe_wait: Duration::ZERO,
            cycle_stamp: now,
            prev_probe_too_high: false,
            prev_probe_precautionary: false,
            probe_rtt_done_stamp: None,
            probe_rtt_round_done: false,
            probe_rtt_cwnd: f64::INFINITY,
            idle_restart: false,
            rng,
        };
        // InitPacingRate, with no round trip yet: one millisecond stands for it.
        p.pacing_rate = STARTUP_PACING_GAIN * INITIAL_CWND * smss / 0.001;
        p.enter_startup();
        p
    }

    /// What the destination may have in flight: the lesser of cwnd and the receiver's window.
    pub fn target(&self) -> usize {
        self.window() as usize
    }

    fn window(&self) -> f64 {
        self.cwnd.min(self.ceiling)
    }

    /// What the egress queue may hold: the window, and one batch ready behind it. Never past the ceiling.
    pub fn queue_bytes(&self) -> usize {
        (self.window() + self.send_quantum).min(self.ceiling).max(self.window()) as usize
    }

    /// The receiver's window. Moves as the pool's share does.
    pub fn set_ceiling(&mut self, ceiling: usize) {
        self.ceiling = (ceiling as f64).max(self.send_quantum);
    }

    /// BBR.min_rtt, once there is one.
    pub fn round_trip(&self) -> Option<Duration> {
        self.min_rtt
    }

    /// BBR.max_bw, in bytes a second.
    pub fn rate(&self) -> f64 {
        self.max_bw
    }

    /// How far a probe above the link's measured capacity would stand below the ceiling, in bytes: 0 once the link
    /// carries at least the ceiling over the probe's gain. A model held at its ceiling measures no more than the
    /// ceiling lets through, so this is as near as it can come to saying the ceiling, not the link, is the bound.
    pub fn headroom(&self) -> usize {
        let wanted = self
            .round_trip()
            .map_or(0.0, |rtt| PROBE_UP_PACING_GAIN * self.max_bw * rtt.as_secs_f64());
        (self.ceiling - wanted).max(0.0) as usize
    }

    /// Whether a batch of `bytes` may go now: cwnd and the receiver's window must have room for it, unless nothing is
    /// in flight, and the pacing rate must have it due.
    pub fn gate(&mut self, bytes: usize, now: Instant) -> Gate {
        // OnTransmit, before the batch goes: a flow restarting from idle paces it at the restarted rate.
        self.handle_restart_from_idle(now);
        if self.inflight > 0 && (self.inflight + bytes as u64) as f64 > self.window() {
            if self.cwnd <= self.ceiling {
                self.cwnd_limited_now = true;
            }
            return Gate::Full;
        }
        match self.next_send_time.checked_duration_since(now) {
            Some(wait) if !wait.is_zero() => Gate::Wait(wait),
            _ => Gate::Send,
        }
    }

    /// CheckIfApplicationLimited: the source has `nothing_to_send` while the window has room for a batch, so the
    /// samples of the next round trip say how fast it produces, not how fast the link carries.
    pub fn check_app_limited(&mut self, nothing_to_send: bool) {
        if nothing_to_send && self.inflight as f64 + self.send_quantum <= self.window() {
            self.mark_app_limited();
        }
    }

    fn mark_app_limited(&mut self) {
        self.app_limited = (self.delivered + self.inflight).max(1);
    }

    /// A batch of `bytes` whose handoff to the destination began at `began` and ended at `now`: OnPacketSent.
    /// The answer goes back with its [`Completion`].
    pub fn dispatched(&mut self, bytes: usize, began: Instant, now: Instant) -> Sent {
        let bytes = bytes as u64;
        // The batch departs when its handoff begins, so the handoff is spent from its own pacing delay.
        let departed = self.next_send_time.max(began);
        // A thread that waits for the pacing rate wakes late, and a late send is never made up: so the lateness comes
        // off the next gap, up to half of it, as Linux's TCP pacing takes OS jitter into account
        // (tcp_update_skb_after_send, in net/ipv4/tcp_output.c).
        let gap = Duration::from_secs_f64(bytes as f64 / self.pacing_rate.max(1.0));
        let late = began.saturating_duration_since(self.next_send_time);
        self.next_send_time = departed + gap - (gap / 2).min(late);
        let send_time = departed.max(now);
        if self.inflight == 0 {
            self.first_send_time = send_time;
            self.delivered_time = send_time;
        }
        let sent_before = self.sent;
        self.sent += bytes;
        self.inflight += bytes;
        if self.inflight as f64 + self.send_quantum > self.cwnd && self.cwnd <= self.ceiling {
            self.cwnd_limited_now = true;
        }
        Sent {
            send_time,
            delivered: self.delivered,
            delivered_time: self.delivered_time,
            first_send_time: self.first_send_time,
            is_app_limited: self.app_limited != 0,
            tx_in_flight: self.inflight,
            lost: self.lost,
            sent_before,
        }
    }

    /// A batch handed over failed but for `delivered` of its bytes, and what failed goes back to be sent again. What
    /// was delivered is acknowledged. What failed is lost only when the destination was asked to send less (`c.throttled`):
    /// any other failure, a reset or an error, says nothing of how full the path is, so it is in flight no longer and
    /// signals nothing.
    pub fn abandoned(&mut self, c: Completion, delivered: usize, now: Instant) {
        let bytes = c.bytes as u64;
        let delivered = (delivered as u64).min(bytes);
        let lost = if c.throttled { bytes - delivered } else { 0 };
        self.answered(c.sent, bytes, delivered, lost, now);
    }

    /// A batch came back acknowledged whole. One the destination was asked to send less for while delivering it is
    /// also congestion, as an ECN CE mark is: its bytes are delivered and count as lost.
    pub fn completed(&mut self, c: Completion, now: Instant) {
        let bytes = c.bytes as u64;
        self.answered(c.sent, bytes, bytes, if c.throttled { bytes } else { 0 }, now);
    }

    /// The answer for a batch of `size` bytes sent as `p`: `acked` of them acknowledged, and `lost` marked lost.
    /// The transport's loss marking, then UpdateOnACK for what was acknowledged.
    fn answered(&mut self, p: Sent, size: u64, acked: u64, lost: u64, now: Instant) {
        self.inflight = self.inflight.saturating_sub(size);
        // Delivered before marked, as a transport counts what an ACK acknowledges before the losses it implies, so
        // the loss round that the marking starts begins after this answer.
        self.delivered += acked;
        self.lost_packet(&p, size, lost, now);
        if acked == 0 {
            return;
        }
        if self.recovery.is_some_and(|at| p.sent_before >= at) {
            self.exit_recovery();
        }

        // UpdateRateSample and GenerateRateSample, for one batch: it is the newest acknowledged.
        self.delivered_time = now;
        let send_elapsed = p.send_time.saturating_duration_since(p.first_send_time);
        let ack_elapsed = now.saturating_duration_since(p.delivered_time);
        self.first_send_time = p.send_time;
        let rtt = now.saturating_duration_since(p.send_time);
        if self.lifetime_min_rtt.is_none() && !rtt.is_zero() {
            // InitPacingRate, now that there is a round trip.
            self.pacing_rate = STARTUP_PACING_GAIN * INITIAL_CWND * self.smss / rtt.as_secs_f64();
        }
        if self.lifetime_min_rtt.is_none_or(|m| rtt < m) {
            self.lifetime_min_rtt = Some(rtt);
        }
        if self.app_limited != 0 && self.delivered > self.app_limited {
            self.app_limited = 0;
        }
        let mut rs = RateSample {
            delivery_rate: 0.0,
            is_app_limited: p.is_app_limited,
            delivered: self.delivered - p.delivered,
            prior_delivered: p.delivered,
            newly_acked: acked,
            tx_in_flight: p.tx_in_flight,
            lost: self.lost - p.lost,
        };
        let interval = send_elapsed.max(ack_elapsed);
        if self.lifetime_min_rtt.is_none_or(|m| interval >= m) && !interval.is_zero() {
            rs.delivery_rate = rs.delivered as f64 / interval.as_secs_f64();
        }

        if self.full_bw_reached && now.saturating_duration_since(self.cycle_ended_at) >= MAX_BW_CYCLE_TIMER {
            self.advance_max_bw_filter(now);
        }

        // UpdateModelAndState.
        self.update_latest_delivery_signals(&rs);
        self.update_congestion_signals(&rs);
        self.update_ack_aggregation(&rs, now);
        self.check_full_bw_reached(&rs);
        self.check_startup_done(&rs);
        self.check_drain_done(now);
        self.update_probe_bw_cycle_phase(&rs, now);
        self.update_min_rtt(rtt, now);
        self.check_probe_rtt(&rs, now);
        self.advance_latest_delivery_signals(&rs);
        self.bw = self.max_bw.min(self.bw_shortterm);

        // UpdateControlParameters.
        self.set_pacing_rate();
        self.set_cwnd(&rs);
    }

    // Loss (draft §5.5.10).

    /// `lost` of a batch's `bytes` were lost, or marked: the transport's loss detection, then HandleLostPacket.
    fn lost_packet(&mut self, p: &Sent, bytes: u64, lost: u64, now: Instant) {
        if lost == 0 {
            return;
        }
        if self.recovery.is_none() {
            // OnEnterFastRecovery.
            self.save_cwnd();
            self.recovery = Some(self.sent);
        }
        self.lost += lost;
        self.note_loss();
        if !self.check_probe_loss {
            return;
        }
        let rs = RateSample {
            tx_in_flight: p.tx_in_flight,
            lost: self.lost - p.lost,
            is_app_limited: p.is_app_limited,
            ..RateSample::default()
        };
        if self.is_inflight_too_high(&rs) {
            let rs = RateSample {
                tx_in_flight: self.inflight_at_loss(&rs, bytes, lost),
                ..rs
            };
            self.handle_inflight_too_high(&rs, now);
        }
    }

    fn exit_recovery(&mut self) {
        if self.recovery.take().is_some() {
            self.cwnd = self.cwnd.max(self.prior_cwnd);
        }
    }

    fn note_loss(&mut self) {
        if !self.is_loss_in_round {
            self.loss_round_delivered = self.delivered;
            self.save_cwnd();
        }
        self.is_loss_in_round = true;
        self.loss_events_in_round += 1;
    }

    fn is_inflight_too_high(&self, rs: &RateSample) -> bool {
        rs.lost as f64 > rs.tx_in_flight as f64 * LOSS_THRESH
    }

    /// InflightAtLoss: at what part of the batch losses passed BBR.LossThresh. `lost` of its `size` bytes were lost:
    /// the draft's packet is lost whole, and a batch may be lost in part.
    fn inflight_at_loss(&self, rs: &RateSample, size: u64, lost: u64) -> u64 {
        let inflight_prev = rs.tx_in_flight as f64 - size as f64;
        let lost_prev = rs.lost as f64 - lost as f64;
        let lost_prefix = (LOSS_THRESH * inflight_prev - lost_prev) / (1.0 - LOSS_THRESH);
        (inflight_prev + lost_prefix).max(0.0) as u64
    }

    fn handle_inflight_too_high(&mut self, rs: &RateSample, now: Instant) {
        self.prev_probe_too_high = true;
        self.check_probe_loss = false;
        if !rs.is_app_limited {
            self.inflight_longterm = (rs.tx_in_flight as f64).max(self.target_inflight() * BETA);
        }
        if self.state == State::ProbeBwUp {
            self.start_probe_bw_down(now);
        }
    }

    fn update_latest_delivery_signals(&mut self, rs: &RateSample) {
        self.loss_round_start = false;
        self.bw_latest = self.bw_latest.max(rs.delivery_rate);
        self.inflight_latest = self.inflight_latest.max(rs.delivered);
        if rs.prior_delivered >= self.loss_round_delivered {
            self.loss_round_delivered = self.delivered;
            self.loss_round_start = true;
        }
    }

    fn advance_latest_delivery_signals(&mut self, rs: &RateSample) {
        if self.loss_round_start {
            self.bw_latest = rs.delivery_rate;
            self.inflight_latest = rs.delivered;
            self.loss_events_in_round = 0;
        }
    }

    fn reset_congestion_signals(&mut self) {
        self.is_loss_in_round = false;
        self.loss_events_in_round = 0;
        self.bw_latest = 0.0;
        self.inflight_latest = 0;
    }

    fn update_congestion_signals(&mut self, rs: &RateSample) {
        self.update_max_bw(rs);
        if !self.loss_round_start {
            return;
        }
        // AdaptLowerBoundsFromCongestion.
        if !self.is_probing_bw() && self.is_loss_in_round {
            if self.bw_shortterm.is_infinite() {
                self.bw_shortterm = self.max_bw;
            }
            if self.inflight_shortterm.is_infinite() {
                self.inflight_shortterm = self.cwnd;
            }
            self.bw_shortterm = self.bw_latest.max(BETA * self.bw_shortterm);
            self.inflight_shortterm = (self.inflight_latest as f64).max(BETA * self.inflight_shortterm);
        }
        self.is_loss_in_round = false;
    }

    fn reset_short_term_model(&mut self) {
        self.bw_shortterm = f64::INFINITY;
        self.inflight_shortterm = f64::INFINITY;
    }

    // The path model (draft §5.5).

    fn update_round(&mut self, rs: &RateSample) {
        if rs.prior_delivered >= self.next_round_delivered {
            self.start_round();
            self.round_count += 1;
            self.rounds_since_probe_up += 1;
            self.round_start = true;
            self.cwnd_limited_last = std::mem::take(&mut self.cwnd_limited_now);
        } else {
            self.round_start = false;
        }
    }

    fn start_round(&mut self) {
        self.next_round_delivered = self.delivered;
    }

    /// C.is_cwnd_limited: cwnd was full at some point in the last round trip.
    fn is_cwnd_limited(&self) -> bool {
        self.cwnd_limited_now || self.cwnd_limited_last
    }

    fn update_max_bw(&mut self, rs: &RateSample) {
        self.update_round(rs);
        if rs.delivery_rate > 0.0 && (rs.delivery_rate >= self.max_bw || !rs.is_app_limited) {
            self.max_bw = self
                .max_bw_filter
                .update(MAX_BW_FILTER_LEN, self.cycle_count, rs.delivery_rate);
        }
    }

    fn update_ack_aggregation(&mut self, rs: &RateSample, now: Instant) {
        let interval = now.saturating_duration_since(self.extra_acked_interval_start);
        let mut expected = self.bw * interval.as_secs_f64();
        if self.extra_acked_delivered as f64 <= expected {
            self.extra_acked_delivered = 0;
            self.extra_acked_interval_start = now;
            expected = 0.0;
        }
        self.extra_acked_delivered += rs.newly_acked;
        let extra = (self.extra_acked_delivered as f64 - expected).min(self.cwnd);
        let len = if self.full_bw_reached {
            EXTRA_ACKED_FILTER_LEN
        } else {
            1
        };
        self.extra_acked = self.extra_acked_filter.update(len, self.round_count, extra);
    }

    fn update_min_rtt(&mut self, rtt: Duration, now: Instant) {
        self.min_rtt_expired = now.saturating_duration_since(self.min_rtt_stamp) > MIN_RTT_FILTER_LEN;
        if self.min_rtt_expired {
            self.probe_rtt_cwnd = self.bdp_multiple(self.bw, PROBE_RTT_CWND_GAIN);
        }
        if self.min_rtt.is_none_or(|m| rtt < m) || self.min_rtt_expired {
            self.min_rtt = Some(rtt);
            self.min_rtt_stamp = now;
        }
    }

    fn bdp(&self) -> f64 {
        self.min_rtt.map_or(0.0, |rtt| self.bw * rtt.as_secs_f64())
    }

    /// BDPMultiple(gain), at `bw`: the draft takes BBR.bw, but for IsTimeToCruise, BBR.max_bw.
    fn bdp_multiple(&self, bw: f64, gain: f64) -> f64 {
        self.min_rtt
            .map_or(INITIAL_CWND * self.smss, |rtt| gain * bw * rtt.as_secs_f64())
    }

    fn quantization_budget(&self, cap: f64) -> f64 {
        let mut cap = cap
            .max(OFFLOAD_BUDGET * self.send_quantum)
            .max(MIN_PIPE_CWND * self.smss);
        if self.state == State::ProbeBwUp {
            cap += 2.0 * self.smss;
        }
        cap
    }

    /// Inflight(bw, gain).
    fn inflight(&self, bw: f64, gain: f64) -> f64 {
        self.quantization_budget(self.bdp_multiple(bw, gain))
    }

    fn update_max_inflight(&mut self) {
        let cap = self.bdp_multiple(self.bw, self.cwnd_gain) + self.extra_acked;
        self.max_inflight = self.quantization_budget(cap);
    }

    /// TargetInflight.
    fn target_inflight(&self) -> f64 {
        self.bdp().min(self.cwnd)
    }

    // The state machine (draft §5.3).

    fn enter_startup(&mut self) {
        self.state = State::Startup;
        self.pacing_gain = STARTUP_PACING_GAIN;
        self.cwnd_gain = DEFAULT_CWND_GAIN;
    }

    fn reset_full_bw(&mut self) {
        self.full_bw = 0.0;
        self.full_bw_count = 0;
        self.full_bw_now = false;
    }

    fn check_full_bw_reached(&mut self, rs: &RateSample) {
        if self.full_bw_now || !self.round_start || rs.is_app_limited {
            return;
        }
        if rs.delivery_rate >= self.full_bw * FULL_BW_GROWTH {
            self.reset_full_bw();
            self.full_bw = rs.delivery_rate;
            return;
        }
        self.full_bw_count += 1;
        self.full_bw_now = self.full_bw_count >= FULL_BW_COUNT;
        if self.full_bw_now {
            self.full_bw_reached = true;
        }
    }

    fn check_startup_done(&mut self, rs: &RateSample) {
        self.check_startup_high_loss(rs);
        if self.state == State::Startup && self.full_bw_reached {
            self.state = State::Drain;
            self.pacing_gain = DRAIN_PACING_GAIN;
            self.cwnd_gain = DEFAULT_CWND_GAIN;
            self.drain_start_round = self.round_count;
        }
    }

    fn check_startup_high_loss(&mut self, rs: &RateSample) {
        if self.full_bw_reached || !self.loss_round_start || self.recovery.is_none() {
            return;
        }
        if self.loss_events_in_round < STARTUP_FULL_LOSS_CNT {
            return;
        }
        if !self.is_inflight_too_high(rs) {
            return;
        }
        self.full_bw_reached = true;
        self.inflight_longterm = self.bdp().max(self.inflight_latest as f64);
    }

    fn check_drain_done(&mut self, now: Instant) {
        if self.state == State::Drain
            && ((self.inflight as f64) <= self.inflight(self.bw, 1.0) || self.round_count > self.drain_start_round + 3)
        {
            self.cwnd_gain = DEFAULT_CWND_GAIN;
            self.start_probe_bw_down(now);
        }
    }

    fn start_probe_bw_down(&mut self, now: Instant) {
        self.reset_congestion_signals();
        self.probe_up_acked_per_inc = f64::INFINITY;
        self.pick_probe_wait();
        self.cycle_stamp = now;
        self.is_bw_probe_stopping = true;
        self.start_round();
        self.state = State::ProbeBwDown;
        self.pacing_gain = PROBE_DOWN_PACING_GAIN;
        self.cwnd_gain = DEFAULT_CWND_GAIN;
    }

    fn start_probe_bw_cruise(&mut self) {
        self.state = State::ProbeBwCruise;
        self.pacing_gain = 1.0;
        self.cwnd_gain = DEFAULT_CWND_GAIN;
    }

    fn start_probe_bw_refill(&mut self) {
        self.reset_short_term_model();
        self.bw_probe_up_rounds = 0;
        self.bw_probe_up_acked = 0.0;
        self.prev_probe_precautionary = false;
        self.is_bw_probe_stopping = false;
        self.start_round();
        self.state = State::ProbeBwRefill;
        self.pacing_gain = 1.0;
        self.cwnd_gain = DEFAULT_CWND_GAIN;
    }

    fn start_probe_bw_up(&mut self, rs: &RateSample) {
        self.start_round();
        self.reset_full_bw();
        self.full_bw = rs.delivery_rate;
        self.state = State::ProbeBwUp;
        self.pacing_gain = PROBE_UP_PACING_GAIN;
        self.cwnd_gain = PROBE_UP_CWND_GAIN;
        self.raise_inflight_longterm_slope();
    }

    fn update_probe_bw_cycle_phase(&mut self, rs: &RateSample, now: Instant) {
        if !self.full_bw_reached {
            return;
        }
        if self.adapt_long_term_model(rs, now) {
            return;
        }
        match self.state {
            State::ProbeBwDown => {
                if self.is_time_to_probe_bw(now) {
                    return;
                }
                if self.is_time_to_cruise() {
                    self.start_probe_bw_cruise();
                }
            }
            State::ProbeBwCruise => {
                self.is_time_to_probe_bw(now);
            }
            State::ProbeBwRefill => {
                if self.round_start {
                    self.check_probe_loss = true;
                    self.start_probe_bw_up(rs);
                }
            }
            State::ProbeBwUp => {
                if self.is_time_to_go_down(rs) {
                    self.prev_probe_too_high = false;
                    self.start_probe_bw_down(now);
                }
            }
            State::Startup | State::Drain | State::ProbeRtt => {}
        }
    }

    fn is_time_to_cruise(&self) -> bool {
        let inflight = self.inflight as f64;
        inflight <= self.inflight_with_headroom() && inflight <= self.inflight(self.max_bw, 1.0)
    }

    fn is_time_to_go_down(&mut self, rs: &RateSample) -> bool {
        if self.prev_probe_too_high && self.inflight as f64 >= self.inflight_longterm {
            self.prev_probe_precautionary = true;
            return true;
        }
        if self.is_cwnd_limited() && self.cwnd >= self.inflight_longterm {
            self.reset_full_bw();
            self.full_bw = rs.delivery_rate;
        } else if self.full_bw_now {
            return true;
        }
        false
    }

    fn is_probing_bw(&self) -> bool {
        matches!(self.state, State::Startup | State::ProbeBwRefill | State::ProbeBwUp)
    }

    fn inflight_with_headroom(&self) -> f64 {
        if self.inflight_longterm.is_infinite() {
            return f64::INFINITY;
        }
        let headroom = self.smss.max(HEADROOM * self.inflight_longterm);
        (self.inflight_longterm - headroom).max(MIN_PIPE_CWND * self.smss)
    }

    fn raise_inflight_longterm_slope(&mut self) {
        let growth_this_round = f64::from(1u32 << self.bw_probe_up_rounds);
        self.bw_probe_up_rounds = (self.bw_probe_up_rounds + 1).min(30);
        self.probe_up_acked_per_inc = (self.cwnd / growth_this_round).max(self.smss);
    }

    fn probe_inflight_longterm_upward(&mut self, rs: &RateSample) {
        if !self.is_cwnd_limited() || self.cwnd < self.inflight_longterm {
            return;
        }
        self.bw_probe_up_acked += rs.newly_acked as f64;
        if self.bw_probe_up_acked >= self.probe_up_acked_per_inc {
            let delta = (self.bw_probe_up_acked / self.probe_up_acked_per_inc).floor();
            self.bw_probe_up_acked -= delta * self.probe_up_acked_per_inc;
            self.inflight_longterm += delta * self.smss;
        }
        if self.round_start {
            self.raise_inflight_longterm_slope();
        }
    }

    /// AdaptLongTermModel: whether it decided a state transition.
    fn adapt_long_term_model(&mut self, rs: &RateSample, now: Instant) -> bool {
        if self.is_bw_probe_stopping && self.round_start {
            self.check_probe_loss = false;
            self.is_bw_probe_stopping = false;
            if self.state.is_probe_bw() && !rs.is_app_limited {
                self.advance_max_bw_filter(now);
            }
            if self.state.is_probe_bw() && self.prev_probe_precautionary && !self.prev_probe_too_high {
                self.start_probe_bw_refill();
                return true;
            }
        }
        if !self.is_inflight_too_high(rs) {
            if self.inflight_longterm.is_infinite() {
                return false;
            }
            if rs.tx_in_flight as f64 > self.inflight_longterm {
                self.inflight_longterm = rs.tx_in_flight as f64;
            }
            if self.state == State::ProbeBwUp {
                self.probe_inflight_longterm_upward(rs);
            }
        }
        false
    }

    fn advance_max_bw_filter(&mut self, now: Instant) {
        self.cycle_count += 1;
        self.cycle_ended_at = now;
    }

    fn is_time_to_probe_bw(&mut self, now: Instant) -> bool {
        if now.saturating_duration_since(self.cycle_stamp) > self.bw_probe_wait || self.is_reno_coexistence_probe_time()
        {
            self.start_probe_bw_refill();
            return true;
        }
        false
    }

    fn is_reno_coexistence_probe_time(&self) -> bool {
        // TargetInflight in packets, which are batches here.
        let reno_rounds = (self.target_inflight() / self.smss).min(63.0);
        self.rounds_since_probe_up as f64 >= reno_rounds
    }

    fn pick_probe_wait(&mut self) {
        self.rounds_since_probe_up = self.random() & 1;
        let fraction = (self.random() >> 11) as f64 / (1u64 << 53) as f64;
        self.bw_probe_wait = Duration::from_secs_f64(2.0 + fraction);
    }

    fn random(&mut self) -> u64 {
        self.rng = self.rng.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.rng;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    // ProbeRTT (draft §5.3.4).

    fn check_probe_rtt(&mut self, rs: &RateSample, now: Instant) {
        if self.state != State::ProbeRtt && self.min_rtt_expired && !self.idle_restart {
            self.save_cwnd();
            self.state = State::ProbeRtt;
            self.pacing_gain = 1.0;
            self.cwnd_gain = PROBE_RTT_CWND_GAIN;
            self.probe_rtt_done_stamp = None;
            self.is_bw_probe_stopping = true;
            self.start_round();
        }
        if self.state == State::ProbeRtt {
            self.handle_probe_rtt(now);
        }
        if rs.delivered > 0 {
            self.idle_restart = false;
        }
    }

    fn handle_probe_rtt(&mut self, now: Instant) {
        self.mark_app_limited();
        if self.probe_rtt_done_stamp.is_none() && self.inflight as f64 <= self.probe_rtt_cwnd() {
            self.probe_rtt_done_stamp = Some(now + PROBE_RTT_DURATION);
            self.probe_rtt_round_done = false;
            self.start_round();
        } else if self.probe_rtt_done_stamp.is_some() {
            if self.round_start {
                self.probe_rtt_round_done = true;
            }
            if self.probe_rtt_round_done {
                self.check_probe_rtt_done(now);
            }
        }
    }

    fn check_probe_rtt_done(&mut self, now: Instant) {
        if self.probe_rtt_done_stamp.is_some_and(|done| now > done) {
            self.min_rtt_stamp = now;
            self.restore_cwnd();
            // ExitProbeRTT.
            self.reset_short_term_model();
            if self.full_bw_reached {
                self.start_probe_bw_down(now);
                self.start_probe_bw_cruise();
            } else {
                self.enter_startup();
            }
        }
    }

    fn probe_rtt_cwnd(&self) -> f64 {
        self.bdp_multiple(self.bw, PROBE_RTT_CWND_GAIN)
            .min(self.probe_rtt_cwnd)
            .max(MIN_PIPE_CWND * self.smss)
    }

    fn handle_restart_from_idle(&mut self, now: Instant) {
        if self.inflight == 0 && self.app_limited != 0 {
            self.idle_restart = true;
            self.extra_acked_interval_start = now;
            self.extra_acked_delivered = 0;
            if self.state.is_probe_bw() {
                self.set_pacing_rate_with_gain(1.0);
            } else if self.state == State::ProbeRtt {
                self.check_probe_rtt_done(now);
            }
        }
    }

    // Control parameters (draft §5.6).

    fn set_pacing_rate_with_gain(&mut self, gain: f64) {
        let rate = gain * self.bw * (100.0 - PACING_MARGIN_PERCENT) / 100.0;
        if self.full_bw_reached || rate > self.pacing_rate {
            self.pacing_rate = rate;
        }
    }

    fn set_pacing_rate(&mut self) {
        self.set_pacing_rate_with_gain(self.pacing_gain);
    }

    fn save_cwnd(&mut self) {
        if self.recovery.is_none() && self.state != State::ProbeRtt {
            self.prior_cwnd = self.cwnd;
        } else {
            self.prior_cwnd = self.prior_cwnd.max(self.cwnd);
        }
    }

    fn restore_cwnd(&mut self) {
        self.cwnd = self.cwnd.max(self.prior_cwnd);
    }

    fn set_cwnd(&mut self, rs: &RateSample) {
        self.update_max_inflight();
        let acked = rs.newly_acked as f64;
        if self.full_bw_reached {
            self.cwnd = (self.cwnd + acked).min(self.max_inflight);
        } else if self.cwnd < self.max_inflight || (self.delivered as f64) < INITIAL_CWND * self.smss {
            self.cwnd += acked;
        }
        self.cwnd = self.cwnd.max(MIN_PIPE_CWND * self.smss);
        if self.state == State::ProbeRtt {
            self.cwnd = self.cwnd.min(self.probe_rtt_cwnd());
        }
        // BoundCwndForModel.
        let mut cap = f64::INFINITY;
        if self.state.is_probe_bw() && self.state != State::ProbeBwCruise {
            cap = self.inflight_longterm;
        } else if matches!(self.state, State::ProbeRtt | State::ProbeBwCruise) {
            cap = self.inflight_with_headroom();
        }
        cap = cap.min(self.inflight_shortterm).max(MIN_PIPE_CWND * self.smss);
        self.cwnd = self.cwnd.min(cap);
    }
}

/// The pacing model for one destination's queue, and what it reports.
pub(super) struct Pacer {
    /// The model whose window is the queue's budget.
    model: Pacing,
    /// How far the link stood below the share, averaged over each stretch and written at its end.
    headroom: Headroom,
    /// Where the budget and headroom are reported, under this destination's id.
    metrics: MetricsHandle,
    id: DestId,
}

impl Pacer {
    /// Starts a flow whose send quantum is a batch of `batch` bytes, and sets the queue's budget to its window.
    pub(super) fn new(batch: usize, queue: &Queue, metrics: &MetricsHandle, id: DestId) -> Pacer {
        let model = Pacing::new(batch, queue.share(), Instant::now());
        queue.set_budget(model.queue_bytes());
        note_queue_budget(metrics, id, queue);
        Pacer {
            model,
            headroom: Headroom::default(),
            metrics: metrics.clone(),
            id,
        }
    }

    /// Whether a batch of `bytes` may be handed over now.
    pub(super) fn gate(&mut self, bytes: usize) -> Gate {
        self.model.gate(bytes, Instant::now())
    }

    /// The source has `nothing_to_send`: see [`Pacing::check_app_limited`].
    pub(super) fn check_app_limited(&mut self, nothing_to_send: bool) {
        self.model.check_app_limited(nothing_to_send);
    }

    /// A batch of `bytes` handed to the destination in a handoff that began at `began`: what the model is told when it
    /// is acknowledged.
    pub(super) fn dispatched(&mut self, bytes: usize, began: Instant) -> Completion {
        Completion {
            bytes,
            sent: self.model.dispatched(bytes, began, Instant::now()),
            throttled: false,
        }
    }

    /// Tell the model a batch failed but for `delivered` of its bytes, and goes back to be sent again: throttled, if
    /// `c.throttled`. Carries its window to the queue as [`Pacer::completed`] does.
    pub(super) fn abandoned(&mut self, queue: &Queue, c: Completion, delivered: usize) {
        self.answered(queue, |model, now| model.abandoned(c, delivered, now));
    }

    /// Tell the model a batch came back, and carry its window to the queue and the metrics when it moves. The ceiling
    /// is read each time: the pool's share moves as other queues come and go. The model's headroom under the ceiling
    /// is averaged over time and reported every [`HEADROOM_EVERY`]: none for long is a destination memory holds back.
    pub(super) fn completed(&mut self, queue: &Queue, c: Completion) {
        self.answered(queue, |model, now| model.completed(c, now));
    }

    fn answered(&mut self, queue: &Queue, answer: impl FnOnce(&mut Pacing, Instant)) {
        let before = self.model.queue_bytes();
        let ceiling = queue.share();
        self.model.set_ceiling(ceiling);
        answer(&mut self.model, Instant::now());
        if self.model.queue_bytes() != before {
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

    /// A path, and the source feeding it: a bottleneck that carries `capacity` bytes a second, a round trip of `rtt`
    /// besides, and each acknowledgment seen up to `late` after it arrives (cycling through that span), as an egress
    /// thread that polls between pops sees it. The source has a batch whenever asked, or offers `offered` bytes a
    /// second. A destination with a `limit` asks to be sent less for each batch sent while more than that was out.
    /// Handing a batch over takes `handoff`, which the egress thread spends before it can send the next. A wait for
    /// the pacing rate ends `wake_late` after it was due, as a timed wait on a condition variable does.
    #[derive(Clone, Copy)]
    struct Path {
        capacity: f64,
        rtt: Duration,
        late: Duration,
        offered: Option<f64>,
        limit: Option<u64>,
        handoff: Duration,
        wake_late: Duration,
    }

    impl Path {
        fn new(capacity: f64, rtt: Duration) -> Path {
            Path {
                capacity,
                rtt,
                late: Duration::ZERO,
                offered: None,
                limit: None,
                handoff: Duration::ZERO,
                wake_late: Duration::ZERO,
            }
        }
    }

    struct Sim {
        p: Pacing,
        path: Path,
        start: Instant,
        now: Instant,
        link_free: Instant,
        /// (acknowledged at, completion), soonest last.
        out: Vec<(Instant, Completion)>,
        source_next: Instant,
        sent: u64,
        /// (when, bytes) of each acknowledgment, and how many were throttled.
        acks: Vec<(Instant, u64)>,
        throttled: usize,
        states: Vec<State>,
    }

    impl Sim {
        fn new(batch: usize, ceiling: usize, path: Path) -> Sim {
            let start = Instant::now();
            Sim {
                p: Pacing::seeded(batch, ceiling, start, 7),
                path,
                start,
                now: start,
                link_free: start,
                out: Vec::new(),
                source_next: start,
                sent: 0,
                acks: Vec::new(),
                throttled: 0,
                states: vec![State::Startup],
            }
        }

        fn has_batch(&self) -> bool {
            self.path.offered.is_none() || self.now >= self.source_next
        }

        /// Runs the flow until `secs` from its start.
        fn run_to(&mut self, secs: f64) {
            let end = self.start + Duration::from_secs_f64(secs);
            while self.now < end {
                let mut next = end;
                if self.has_batch() {
                    match self.p.gate(BATCH, self.now) {
                        Gate::Send => {
                            self.send();
                            continue;
                        }
                        Gate::Wait(d) => next = next.min(self.now + d + self.path.wake_late),
                        Gate::Full => {}
                    }
                } else {
                    self.p.check_app_limited(true);
                    next = next.min(self.source_next);
                }
                if let Some((at, _)) = self.out.last() {
                    next = next.min(*at);
                }
                self.now = next.max(self.now);
                self.acknowledge();
            }
        }

        fn send(&mut self) {
            let held = self.p.inflight;
            let began = self.now;
            self.now += self.path.handoff;
            let sent = self.p.dispatched(BATCH, began, self.now);
            let throttled = self.path.limit.is_some_and(|l| held + BATCH as u64 > l);
            self.link_free = self.link_free.max(self.now) + Duration::from_secs_f64(BATCH as f64 / self.path.capacity);
            let span = self.path.late.as_micros() as u64;
            let late = if span == 0 { 0 } else { (self.sent * 7919) % span };
            self.sent += 1;
            let at = self.link_free + self.path.rtt + Duration::from_micros(late);
            self.out.push((
                at,
                Completion {
                    bytes: BATCH,
                    sent,
                    throttled,
                },
            ));
            self.out.sort_by_key(|(at, _)| std::cmp::Reverse(*at));
            if let Some(rate) = self.path.offered {
                self.source_next = self.source_next.max(self.now) + Duration::from_secs_f64(BATCH as f64 / rate);
            }
        }

        fn acknowledge(&mut self) {
            while self.out.last().is_some_and(|(at, _)| *at <= self.now) {
                let (at, c) = self.out.pop().expect("one is due");
                self.p.check_app_limited(!self.has_batch());
                self.p.completed(c, at);
                self.acks.push((at, c.bytes as u64));
                self.throttled += usize::from(c.throttled);
                if self.states.last() != Some(&self.p.state) {
                    self.states.push(self.p.state);
                }
            }
        }

        /// Bytes acknowledged a second between `from` and `to` seconds from the start.
        fn throughput(&self, from: f64, to: f64) -> f64 {
            let (a, b) = (
                self.start + Duration::from_secs_f64(from),
                self.start + Duration::from_secs_f64(to),
            );
            let bytes: u64 = self
                .acks
                .iter()
                .filter(|(t, _)| *t >= a && *t < b)
                .map(|(_, n)| n)
                .sum();
            bytes as f64 / (to - from)
        }
    }

    const GIB: f64 = 1024.0 * 1024.0 * 1024.0;

    #[test]
    fn startup_ends_once_the_rate_plateaus_and_measures_the_link() {
        // A 20 ms path at 1 GiB/s: a product of about 20 batches.
        let mut s = Sim::new(BATCH, 1024 * MIB, Path::new(GIB, Duration::from_millis(20)));
        s.run_to(2.0);
        assert!(s.p.full_bw_reached, "the pipe filled");
        assert_eq!(&s.states[..2], &[State::Startup, State::Drain], "{:?}", s.states);
        assert!(
            s.states[2..].iter().any(|s| s.is_probe_bw()),
            "drained into ProbeBW: {:?}",
            s.states
        );
        let error = (s.p.rate() - GIB).abs() / GIB;
        assert!(error < 0.02, "max_bw within 2% of the link: {} vs {GIB}", s.p.rate());
    }

    #[test]
    fn probe_bw_keeps_the_link_full_without_holding_far_past_the_product() {
        let mut s = Sim::new(BATCH, 1024 * MIB, Path::new(GIB, Duration::from_millis(20)));
        s.run_to(8.0);
        let carried = s.throughput(4.0, 8.0);
        assert!(carried > 0.9 * GIB, "carries the link: {carried} of {GIB}");
        let bdp = GIB * 0.02;
        assert!(
            (s.p.target() as f64) < 3.0 * bdp,
            "the window stays near the product: {} vs {bdp}",
            s.p.target()
        );
        let cycles: Vec<_> = s.states.iter().filter(|s| **s == State::ProbeBwUp).collect();
        assert!(!cycles.is_empty(), "it probes for more: {:?}", s.states);
    }

    /// Acknowledgments seen late -- an egress thread polls for them between pops -- stretch the round trip a batch
    /// reads well past the least one. The window still carries the link: cwnd is twice the product, plus the
    /// aggregation the late acknowledgments show.
    #[test]
    fn acknowledgments_seen_late_still_keep_the_link_full() {
        let capacity = 1.2e9;
        let path = Path {
            late: Duration::from_millis(12),
            ..Path::new(capacity, Duration::from_millis(4))
        };
        let mut s = Sim::new(4 * MIB, 825 * MIB, path);
        s.run_to(10.0);
        let carried = s.throughput(5.0, 10.0);
        assert!(carried > 0.85 * capacity, "carries the link: {carried} of {capacity}");
        assert!(s.p.extra_acked > 0.0, "the late acknowledgments read as aggregation");
    }

    #[test]
    fn a_slow_source_never_lowers_the_estimate_or_ends_startup() {
        let path = Path {
            offered: Some(0.1 * GIB),
            ..Path::new(GIB, Duration::from_millis(20))
        };
        let mut s = Sim::new(BATCH, 1024 * MIB, path);
        s.run_to(3.0);
        assert_eq!(
            s.p.state,
            State::Startup,
            "app-limited rounds without loss stay in Startup"
        );

        let mut s = Sim::new(BATCH, 1024 * MIB, Path::new(GIB, Duration::from_millis(20)));
        s.run_to(3.0);
        let measured = s.p.rate();
        s.path.offered = Some(0.1 * GIB);
        s.source_next = s.now;
        s.run_to(8.0);
        assert!(
            s.p.rate() >= 0.95 * measured,
            "a source sending a tenth of the link leaves the estimate: {} vs {measured}",
            s.p.rate()
        );
    }

    #[test]
    fn a_min_rtt_ten_seconds_old_is_measured_again_in_probe_rtt() {
        let mut s = Sim::new(BATCH, 1024 * MIB, Path::new(GIB, Duration::from_millis(20)));
        let mut least = usize::MAX;
        let mut t = 0.0;
        while t < 13.0 {
            t += 0.001;
            s.run_to(t);
            if s.p.state == State::ProbeRtt {
                least = least.min(s.p.target());
            }
        }
        let i = s
            .states
            .iter()
            .position(|s| *s == State::ProbeRtt)
            .expect("entered ProbeRTT");
        assert!(
            s.states[i + 1..].iter().any(|s| s.is_probe_bw()),
            "and came back to ProbeBW: {:?}",
            s.states
        );
        assert!(
            least as f64 <= (0.5 * GIB * 0.02).max(MIN_PIPE_CWND * BATCH as f64) + 1.0,
            "cwnd held to ProbeRTTCwnd: {least}"
        );
        assert_eq!(s.p.cwnd_gain, DEFAULT_CWND_GAIN, "and its gain restored");
    }

    #[test]
    fn a_source_idle_past_the_min_rtt_window_skips_probe_rtt() {
        let mut s = Sim::new(BATCH, 1024 * MIB, Path::new(GIB, Duration::from_millis(20)));
        s.run_to(3.0);
        s.path.offered = Some(1.0);
        s.source_next = s.now + Duration::from_secs(11);
        s.run_to(14.0);
        s.path.offered = None;
        let before = s.states.len();
        s.run_to(14.2);
        assert!(s.p.idle_restart || s.states.len() >= before, "restarted from idle");
        assert!(
            !s.states[before..].contains(&State::ProbeRtt),
            "idle drained the queue: no ProbeRTT {:?}",
            &s.states[before..]
        );
    }

    #[test]
    fn a_product_under_one_batch_keeps_the_least_window_on_the_wire() {
        // 1 ms at 100 MiB/s: a tenth of a batch.
        let mut s = Sim::new(
            BATCH,
            1024 * MIB,
            Path::new(100.0 * MIB as f64, Duration::from_millis(1)),
        );
        s.run_to(5.0);
        assert!(
            s.p.target() >= 4 * BATCH,
            "BBR.MinPipeCwnd is four packets, and a packet is a batch: {}",
            s.p.target()
        );
        let carried = s.throughput(3.0, 5.0);
        assert!(carried > 0.9 * 100.0 * MIB as f64, "and the link stays full: {carried}");
    }

    /// A destination that asks to be sent less past 8 batches in flight, on a link whose product is about 5:
    /// inflight_longterm learns the limit, and each probe above it costs the batches sent over it in the round trip
    /// before the answer comes back. A batch is a large packet, so that is a larger share than BBR.LossThresh.
    #[test]
    fn a_destination_that_asks_for_less_is_held_near_its_limit() {
        let limit = 8 * BATCH as u64;
        let path = Path {
            limit: Some(limit),
            ..Path::new(GIB, Duration::from_millis(20))
        };
        let mut s = Sim::new(BATCH, 1024 * MIB, path);
        s.run_to(4.0);
        let early = s.throttled;
        let early_acks = s.acks.len();
        s.run_to(14.0);
        let longterm = s.p.inflight_longterm / limit as f64;
        assert!(
            (0.8..1.3).contains(&longterm),
            "inflight_longterm holds the limit: {longterm} of it"
        );
        let over = (s.throttled - early) as f64 / (s.acks.len() - early_acks) as f64;
        assert!(over < 0.2, "only probes go over the limit: {over}");
        let carried = s.throughput(4.0, 14.0);
        assert!(carried > 0.9 * GIB, "and the link stays full: {carried}");
    }

    #[test]
    fn the_window_never_passes_the_ceiling_and_headroom_says_which_bound() {
        let ceiling = 5 * BATCH;
        let mut s = Sim::new(BATCH, ceiling, Path::new(GIB, Duration::from_millis(20)));
        s.run_to(3.0);
        assert!(s.p.target() <= ceiling, "{}", s.p.target());
        assert!(s.p.queue_bytes() <= ceiling);
        assert_eq!(s.p.headroom(), 0, "the link is wider than the ceiling");

        let mut s = Sim::new(
            BATCH,
            1024 * MIB,
            Path::new(100.0 * MIB as f64, Duration::from_millis(20)),
        );
        s.run_to(3.0);
        assert!(s.p.headroom() > 0, "a narrow link leaves headroom");
    }

    #[test]
    fn a_link_ten_times_wider_is_found_within_a_few_cycles() {
        let mut s = Sim::new(BATCH, 2048 * MIB, Path::new(0.1 * GIB, Duration::from_millis(20)));
        s.run_to(5.0);
        s.path.capacity = GIB;
        s.run_to(20.0);
        let carried = s.throughput(16.0, 20.0);
        assert!(carried > 0.8 * GIB, "found the wider link: {carried}");
    }

    /// The pacing delay runs from when a handoff begins, so a handoff shorter than a batch's time on the link costs
    /// nothing.
    #[test]
    fn a_handoff_shorter_than_the_pacing_delay_does_not_slow_the_flow() {
        let path = Path {
            handoff: Duration::from_millis(2),
            ..Path::new(GIB, Duration::from_millis(20))
        };
        let mut s = Sim::new(BATCH, 1024 * MIB, path);
        s.run_to(8.0);
        let carried = s.throughput(4.0, 8.0) / GIB;
        assert!(carried > 0.9, "{carried:.2} GiB/s");
    }

    /// When the link narrows, the batches a destination asks to be sent less are the loss that brings the model down
    /// to it: the measured rate falls to the new link and the window back toward its product.
    #[test]
    fn a_narrowed_link_brings_the_model_down_through_its_destination_s_limit() {
        let path = Path {
            limit: Some(32 * BATCH as u64),
            ..Path::new(GIB, Duration::from_millis(20))
        };
        let mut s = Sim::new(BATCH, 2048 * MIB, path);
        s.run_to(5.0);
        s.path.capacity = 0.1 * GIB;
        s.run_to(120.0);
        let rate = s.p.rate() / GIB;
        assert!(rate < 0.15, "the rate falls to the link: {rate:.2} GiB/s");
        assert!(
            s.p.target() <= 16 * BATCH,
            "the window comes back down: {} MiB",
            s.p.target() / MIB
        );
        let carried = s.throughput(60.0, 120.0) / GIB;
        assert!(carried > 0.09, "the link stays full: {carried:.3} GiB/s");
    }

    /// A link that narrows behind a deep queue, with nothing ever refused: no loss reaches the model, and the queue
    /// stretches each round trip until a cycle cannot end by them. The cycle timer still ages the old rate out, and
    /// the window comes back down to the narrower link.
    #[test]
    fn a_link_that_narrows_without_loss_brings_the_model_down_within_a_few_cycle_timers() {
        let mut s = Sim::new(BATCH, 1024 * MIB, Path::new(GIB, Duration::from_millis(20)));
        s.run_to(30.0);
        s.path.capacity = 0.1 * GIB;
        s.run_to(120.0);
        let rate = s.p.rate() / GIB;
        assert!(rate < 0.15, "the rate falls to the link: {rate:.2} GiB/s");
        assert!(
            s.p.target() <= 16 * BATCH,
            "the window comes back down: {} MiB",
            s.p.target() / MIB
        );
        let carried = s.throughput(90.0, 120.0) / GIB;
        assert!(carried > 0.09, "the link stays full: {carried:.3} GiB/s");
    }

    #[test]
    fn sends_are_spaced_at_the_pacing_rate() {
        let mut s = Sim::new(BATCH, 1024 * MIB, Path::new(GIB, Duration::from_millis(20)));
        s.run_to(4.0);
        let before = s.p.next_send_time;
        let rate = s.p.pacing_rate;
        s.p.dispatched(BATCH, before, before);
        let gap = s.p.next_send_time.duration_since(before).as_secs_f64();
        assert!(
            (gap - BATCH as f64 / rate).abs() < 1e-6,
            "one batch's time at the rate: {gap}"
        );
        assert!(
            rate < 1.3 * GIB && rate > 0.8 * GIB,
            "the rate follows the link: {rate}"
        );
    }

    /// A batch due every 0.92 ms on an 80 ms path, and each wait for it ending 200 µs late: without the late send's
    /// credit the flow carries 84% of the link.
    #[test]
    fn waking_late_for_the_pacing_rate_costs_no_throughput() {
        let capacity = BATCH as f64 / 0.00092;
        let mut carried = [0.0; 2];
        for (i, late) in [Duration::ZERO, Duration::from_micros(200)].into_iter().enumerate() {
            let path = Path {
                wake_late: late,
                ..Path::new(capacity, Duration::from_millis(80))
            };
            let mut s = Sim::new(BATCH, 1024 * MIB, path);
            s.run_to(20.0);
            carried[i] = s.throughput(5.0, 20.0) / capacity;
        }
        assert!(
            carried[1] > carried[0] - 0.01,
            "waking late carries what waking on time does: {:.3} vs {:.3}",
            carried[1],
            carried[0]
        );
    }

    #[test]
    fn a_failed_batch_leaves_flight_signalling_nothing_and_what_was_delivered_is_acknowledged() {
        let now = Instant::now();
        let mut p = Pacing::seeded(BATCH, 1024 * MIB, now, 7);
        let sent = p.dispatched(BATCH, now, now);
        assert_eq!(p.inflight, BATCH as u64);
        let c = Completion {
            bytes: BATCH,
            sent,
            throttled: false,
        };
        p.abandoned(c, BATCH / 4, now + Duration::from_millis(10));
        assert_eq!(p.inflight, 0, "nothing in flight");
        assert_eq!(p.delivered, (BATCH / 4) as u64, "a quarter was delivered");
        assert_eq!(p.lost, 0, "a failure is no congestion");
        assert!(p.recovery.is_none(), "and starts no recovery");
        assert!(
            p.min_rtt.is_some(),
            "what was delivered was acknowledged: a round trip was sampled"
        );
    }

    #[test]
    fn a_throttled_batch_is_lost_for_what_goes_again_and_acknowledged_for_what_was_delivered() {
        let now = Instant::now();
        let mut p = Pacing::seeded(BATCH, 1024 * MIB, now, 7);
        let sent = p.dispatched(BATCH, now, now);
        let c = Completion {
            bytes: BATCH,
            sent,
            throttled: true,
        };
        p.abandoned(c, BATCH / 4, now + Duration::from_millis(10));
        assert_eq!(p.delivered, (BATCH / 4) as u64, "a quarter was delivered");
        assert_eq!(p.lost, (BATCH - BATCH / 4) as u64, "the rest was asked for less: lost");
        assert!(p.recovery.is_some(), "a loss starts recovery");
    }

    #[test]
    fn a_batch_delivered_throttled_is_acknowledged_then_marked() {
        let now = Instant::now();
        let mut p = Pacing::seeded(BATCH, 1024 * MIB, now, 7);
        let sent = p.dispatched(BATCH, now, now);
        p.completed(
            Completion {
                bytes: BATCH,
                sent,
                throttled: true,
            },
            now + Duration::from_millis(10),
        );
        assert_eq!(p.delivered, BATCH as u64, "it was delivered");
        assert_eq!(p.lost, BATCH as u64, "and marks congestion");
        assert_eq!(
            p.loss_round_delivered, BATCH as u64,
            "the loss round it starts begins after this answer"
        );
        assert!(!p.loss_round_start, "so it does not end on the same answer");
    }

    #[test]
    fn the_max_filter_keeps_the_best_within_its_window() {
        let mut f = MaxFilter::default();
        assert_eq!(f.update(2, 0, 10.0), 10.0);
        assert_eq!(
            f.update(2, 1, 5.0),
            10.0,
            "the best stands within the window: its slot and the next"
        );
        assert_eq!(f.update(2, 2, 4.0), 5.0, "and lapses past it to the next best");
        assert_eq!(f.update(2, 3, 3.0), 4.0);
    }

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
}
