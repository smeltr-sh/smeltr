//! One object's counts over a window (design doc, Control plane): what the map's focus view draws when a node's
//! stats control is used.
//!
//! [`build`] answers for one object in one group, from that group's compiled topology and the registry's durable
//! series. The topology says what the object is wired to; the series say how much went each way. A subject with no
//! samples in the window is a zero here rather than an absence: the question is "how much", and nothing having
//! happened is an answer to it -- unlike the map, where an unmeasured edge is drawn differently from an idle one.
//!
//! **Only one ratio here is exact.** `smeltr_rows_unmatched_total` under cause `route` over `smeltr_rows_polled_total`
//! is two counts of the same rows at the same place. Under cause `rule` it counts each route's copy that no delivery
//! rule took, so two routes declining one row count it twice. A route's share is not: `smeltr_route_matched_total` counts rows a route's condition matched,
//! which is an upper bound on rows delivered -- the pipeline can drop a row, and a delivery rule's `when` can
//! decline it afterward. Every route sees every row, so the shares can sum past the rows polled. That is a fact
//! about routing, not an error to reconcile, and a reader is told rather than shown a total that looks wrong.

use std::collections::BTreeMap;
use std::time::Duration;

use serde::Serialize;

use crate::config_file::LoadedSystem;
use crate::core::id::SourceId;
use crate::heartbeat::{MetricSampleDto, MetricValueDto};
use crate::plan::config::{FunctionKind, Step};

use super::envelope::{Envelope, Refusal};
use super::map::NodeKind;
use super::registry::ThreadHealth;
use super::series::{RegistryStore, Tier};
use super::throughput::{self, ExpectedPoint};
use super::warnings::{self, SubjectKind, WarningSpan, label, stale_after_ms};
use super::{
    BYTES_DELIVERED_METRIC, BYTES_POLLED_METRIC, ROWS_DELIVERED_METRIC, ROWS_IN_METRIC, ROWS_OUT_METRIC,
    ROWS_UNMATCHED_METRIC,
};

/// `route_masks`' count of rows each route's own condition matched (`plan::topology`), labeled with the route's id.
const ROUTE_MATCHED_METRIC: &str = "smeltr_route_matched_total";
/// `Transform::poll`'s time inside the source's own read, labeled with the source (`runtime.rs`).
const TICK_POLL_SECONDS_METRIC: &str = "smeltr_tick_poll_seconds_total";
/// `plan::exec`'s per-step counts, labeled with the pipeline and the step within it.
const FUNCTION_SECONDS_METRIC: &str = "smeltr_function_seconds_total";
const ROWS_DROPPED_METRIC: &str = "smeltr_rows_dropped_total";
const ROWS_ERRORED_METRIC: &str = "smeltr_rows_errored_total";
/// `Transform::hand_off`'s per-destination counts of rows in a hand-over that failed (`runtime.rs`).
const ROWS_SEND_FAILED_METRIC: &str = "smeltr_rows_send_failed_total";
const ROWS_REJECTED_METRIC: &str = "smeltr_rows_rejected_total";
/// `egress::note_stall`'s count of times a destination stopped taking writes.
const DESTINATION_STALLS_METRIC: &str = "smeltr_destination_stalls_total";
/// `egress::note_queue`'s level: what one worker's queue for a destination is holding.
pub(super) const DESTINATION_QUEUE_BYTES_METRIC: &str = "smeltr_destination_queue_bytes";
/// `egress::note_queue_budget`'s bound on it.
pub(super) const DESTINATION_QUEUE_BUDGET_METRIC: &str = "smeltr_destination_queue_budget_bytes";
/// A Kafka producer's mean round trip to its brokers, off its freshest statistics (`io::kafka::publish_stats`).
const DESTINATION_RTT_METRIC: &str = "smeltr_destination_rtt_seconds";
/// Seconds a destination was held back because the far end asked it to slow down: a Kafka producer's broker
/// throttle time, an HTTP destination's backoff after a throttled answer. Summed over concurrent requests.
const DESTINATION_THROTTLED_METRIC: &str = "smeltr_destination_throttled_seconds_total";

/// What the fleet is saying now, beside the series store's record of what it said.
///
/// Two clocks, and which one a figure is on decides where it is read from. Every count in a [`Stats`] is an
/// amount summed over the window asked for, and the buckets hold it. A queue's fill is a level, true only at the
/// instant the worker read it, and summing a level over a window is not a number anything means -- so it comes off
/// the freshest heartbeat instead.
pub struct Live<'a> {
    /// Every thread the leader has heard from (`RegistryStore::snapshot`).
    pub snapshot: &'a [ThreadHealth],
    /// What the fleet was told to beat at, which is what says when a report is too old to count.
    pub heartbeat_interval: Duration,
    /// The share of past windows a source's expected band holds, in (0, 1) (`throughput::band_percent`).
    pub band_share: f64,
}

/// One bucket of a [`Spread`]: when it started and what it measured.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SpreadPoint {
    pub bucket_millis: i64,
    pub value: f64,
}

/// How one figure moved across the window's buckets: its least, its most, its mean, and the buckets themselves,
/// oldest first. The bucket holding "now" is left out: it is still filling, and a rate read off a part of a bucket
/// is low by however much of the bucket is left. A bucket nothing wrote is absent here as it is in the store, so the
/// mean is over the buckets present.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Spread {
    pub min: f64,
    pub max: f64,
    pub mean: f64,
    pub buckets: Vec<SpreadPoint>,
}

impl Spread {
    /// `points` oldest first, or `None` for none: a spread over nothing is not a figure.
    fn over(points: Vec<SpreadPoint>) -> Option<Spread> {
        if points.is_empty() {
            return None;
        }
        let values = points.iter().map(|p| p.value);
        Some(Spread {
            min: values.clone().fold(f64::INFINITY, f64::min),
            max: values.clone().fold(f64::NEG_INFINITY, f64::max),
            mean: values.sum::<f64>() / points.len() as f64,
            buckets: points,
        })
    }
}

/// One route out of a source, and how much of that source it took.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RouteShare {
    /// `RouteConfig::id`: minted, so it is a key rather than something to show.
    pub route: String,
    /// The pipeline this route feeds, by its author's id -- what names the share to a reader, since nobody names a
    /// route.
    pub pipeline: String,
    /// Routes are unordered and each sees every row, which is why the shares can overlap.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub condition: Option<String>,
    /// Rows this route's condition matched over the window. An upper bound on what it delivered (module doc).
    pub matched: f64,
}

/// A source's window: everything it read, and where that went.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SourceStats {
    /// Rows read from the source over the window -- the denominator of the one exact ratio.
    pub polled: f64,
    /// Rows that met `Unmatched`: no route took them, or no delivery rule of the route that did.
    pub unmatched: f64,
    /// The answer to `Unmatched` (design doc, Exception taxonomy).
    pub unmatched_policy: crate::config_file::AnswerDto,
    /// This source's routes in the order they run, each with its share. A route that matched nothing over the
    /// window is still listed: that it took nothing is the answer to "where did this source's rows go".
    pub routes: Vec<RouteShare>,
    /// Rows read per second, bucket by bucket.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rate: Option<Spread>,
    /// Payload bytes read per second, bucket by bucket.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bytes_rate: Option<Spread>,
    /// Microseconds inside the source's own read per row read, bucket by bucket. Only buckets that read a row count.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub poll_per_row: Option<Spread>,
    /// What the source was expected to read per second, bucket by bucket (`throughput::expected_over`). Empty while
    /// it has too little history.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub expected: Vec<ExpectedPoint>,
}

/// One step of a pipeline, and what it did to the rows that reached it.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StepShare {
    /// The step's own id, as its author wrote it (`PipelineConfig::step_ids`).
    pub step: String,
    /// Rows this step took out of the batch.
    pub dropped: f64,
    /// Rows this step marked with an error, across every kind of error -- which kind is not broken out here.
    pub errored: f64,
    /// Microseconds spent in this step over the window.
    pub micros: f64,
}

/// A pipeline's window: what reached it, what left, and where the work went.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PipelineStats {
    /// Rows handed to this pipeline: every route naming it, summed. Exact -- a route's matched mask is the batch
    /// the pipeline is run over (`plan::exec::run_scoped`'s `initial_live`).
    pub rows_in: f64,
    /// Rows still live when it finished: `rows_in` less what every step dropped. Exact for the same reason -- a row
    /// leaves a pipeline only through a step that redefines `__live`, which is what a step's `dropped` counts.
    pub rows_out: f64,
    /// Microseconds spent per row over the window, which is what compares between pipelines and between windows. A
    /// total only says how much traffic there was. `None` when no row reached it, where a rate would be a division
    /// by zero dressed as an answer.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub micros_per_row: Option<f64>,
    /// Its steps in the order they run, whether or not each did anything.
    pub steps: Vec<StepShare>,
    /// Rows handed to it per second, bucket by bucket.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rate: Option<Spread>,
    /// Microseconds its steps spent per row, bucket by bucket. Only buckets that took a row count.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub per_row: Option<Spread>,
}

/// Rows one source had refused by this destination. The only count carrying both ends: `smeltr_rows_sent_total` names the
/// destination alone, so what *arrived* cannot be split by source, and refusals can.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SourceRefusals {
    pub source: String,
    pub rejected: f64,
}

/// One worker's queue for a destination, as of that worker's freshest heartbeat.
///
/// Not a total over the window. Every other figure here is an amount that accumulated; this is a level, and a level
/// has no meaning added up. A worker that has gone quiet is still listed at its last known fill, marked `stale`: a
/// reader must not take it as a queue somebody is holding now, nor lose sight of a worker that stopped reporting.
///
/// Carried by the freshest heartbeat, but read before it: the egress thread samples its own queue
/// (`egress::note_queue`), and it stops sampling while it sits out a retry backoff. A queue nobody is draining only
/// grows, so a figure from a stalling destination is the least it was holding, not the most.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QueueFill {
    /// The worker holding it. A destination has one queue per worker running it, each with its own budget.
    pub worker: String,
    /// Bytes held: queued, plus sent and not yet acknowledged (`egress::Queue::held`).
    pub bytes: f64,
    /// What that worker may hold before it refuses an offer (`plan::topology::DestinationKind::queue_bytes`).
    pub budget: f64,
    /// The worker's last heartbeat is older than [`stale_after_ms`]: this is the last fill it reported, not what it
    /// holds now.
    pub stale: bool,
}

/// A destination's window: what it took, what it would not, and what it cost to hand over.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DestinationStats {
    /// Rows handed to this destination's buffer. Counted before dispatch and before any acknowledgment -- not
    /// evidence anything actually left this process.
    pub received: f64,
    /// Rows this destination has acknowledged. The only count here that means a row actually arrived.
    pub delivered: f64,
    /// Sends that failed for a reason that may pass: retried, not lost.
    pub send_failed: f64,
    /// Rows refused for good, which no retry would change.
    pub rejected: f64,
    /// Times the destination stopped taking writes altogether.
    pub stalls: f64,
    /// Which sources the refusals came from, worst first. Empty when nothing was refused.
    pub rejected_by_source: Vec<SourceRefusals>,
    /// What each worker's queue for this destination is holding: fresh workers fullest first, then stale ones. Empty
    /// where no worker running it has ever reported one.
    pub queues: Vec<QueueFill>,
    /// The mean round trip to the brokers, in milliseconds, from the freshest worker reporting one. Bytes in flight
    /// over it are what the buffer reaches. `None` for a destination that does not measure it, or that no worker is
    /// running.
    pub round_trip_ms: Option<f64>,
    /// Milliseconds the far end held requests back over the window, summed over concurrent requests. Above zero, the
    /// far end is what binds, and a bigger buffer buys nothing.
    pub throttled_ms: f64,
    /// Rows acknowledged per second, bucket by bucket.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rate: Option<Spread>,
    /// Bytes acknowledged per second, bucket by bucket, as the queue measured them.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bytes_rate: Option<Spread>,
    /// The round trip in milliseconds, bucket by bucket. A gauge's bucket keeps its highest reading, so each point
    /// is that bucket's peak, and the least of them is the lowest peak, not the lowest sample.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub round_trip: Option<Spread>,
}

/// One object's counts. Its envelope is not `ok` for a subject this cannot answer for -- one the group does not
/// declare, or a kind whose stats are not built yet -- and says which, rather than answering with zeroes that read
/// as "nothing happened".
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Stats {
    pub group: String,
    pub kind: NodeKind,
    /// The author's own id for the object asked about.
    pub id: String,
    /// The window asked for, which every total covers.
    pub window_millis: i64,
    /// The bucket width these totals were summed from (`Tier::for_window`): how coarse the answer is, and how
    /// close to now its newest bucket can be.
    pub bucket_millis: i64,
    /// The leader's clock when it answered, which is the clock the buckets are on. A spread's grid ends at the
    /// bucket before the one holding this instant; the browser's own clock may sit a bucket off.
    pub now_millis: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<SourceStats>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pipeline: Option<PipelineStats>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub destination: Option<DestinationStats>,
    /// The warnings about a source or destination that held at any time in the window, oldest first.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<WarningSpan>,
}

impl Stats {
    /// No counts, and why -- the object could not be read, or is a kind this cannot answer for yet.
    pub fn failed(group: &str, kind: NodeKind, id: &str, refusal: impl Into<Refusal>) -> Envelope<Stats> {
        // Not `ok`, and still names what was asked about, so the focus view can say whose counts are missing.
        let empty = Stats {
            group: group.to_string(),
            kind,
            id: id.to_string(),
            window_millis: 0,
            bucket_millis: 0,
            now_millis: 0,
            source: None,
            pipeline: None,
            destination: None,
            warnings: Vec::new(),
        };
        let Refusal { status, errors } = refusal.into();
        Envelope::with(empty, status, errors)
    }
}

/// `id`'s counts in `group` over `window`. `system` is that group's own bundle with `shared/` merged, loaded and
/// validated (`tree::load_group_at`); `series_store` is the registry's durable series, absent on a leader that has
/// none, which answers with zeroes rather than an error -- there is simply no history yet.
pub fn build(
    group: &str,
    system: &LoadedSystem,
    kind: NodeKind,
    id: &str,
    window: Duration,
    series_store: Option<&RegistryStore>,
    live: Live<'_>,
) -> Envelope<Stats> {
    let tier = Tier::for_window(window);
    let now_ms = super::warnings::now_millis();
    let since_ms = now_ms - window.as_millis() as i64;
    let totals = |metric: &str| match series_store {
        None => Vec::new(),
        Some(store) => totals_by_subject(store, metric, tier, since_ms),
    };
    let shape = Shape {
        bucket_ms: tier.bucket_ms(),
        now_bucket_ms: tier.align(now_ms),
    };

    let mut answer = Envelope::ok(Stats {
        group: group.to_string(),
        kind,
        id: id.to_string(),
        window_millis: window.as_millis() as i64,
        bucket_millis: tier.bucket_ms(),
        now_millis: now_ms,
        source: None,
        pipeline: None,
        destination: None,
        warnings: Vec::new(),
    });

    match kind {
        NodeKind::Source => {
            let Some(source) = system.topology.sources.iter().find(|s| s.name == id) else {
                return Stats::failed(group, kind, id, Refusal::not_found(undeclared(group, "source", id)));
            };
            let at = [("source", id)];
            let polled = totals(ROWS_IN_METRIC);
            let rows = buckets(&polled, group, &at);
            answer.source = Some(SourceStats {
                rate: shape.rate(&rows),
                bytes_rate: shape.rate(&buckets(&totals(BYTES_POLLED_METRIC), group, &at)),
                poll_per_row: shape.per_row(
                    &scaled(buckets(&totals(TICK_POLL_SECONDS_METRIC), group, &at), 1e6),
                    &rows,
                ),
                polled: total(&polled, group, &at),
                unmatched: total(&totals(ROWS_UNMATCHED_METRIC), group, &[("source", id)]),
                unmatched_policy: system.answer_by_name(
                    system
                        .topology
                        .on_exception
                        .answer(&crate::core::error::ExceptionKind::Unmatched),
                ),
                routes: route_shares(system, source.id, group, &totals(ROUTE_MATCHED_METRIC)),
                expected: series_store
                    .map(|store| throughput::expected_over(store, group, id, since_ms, now_ms, live.band_share))
                    .transpose()
                    .unwrap_or_else(|e| {
                        tracing::warn!(
                            target: "stats.expected_unreadable",
                            error = %e,
                            "could not read this source's history, so its chart draws no expected band"
                        );
                        None
                    })
                    .unwrap_or_default(),
            });
        }
        NodeKind::Pipeline => {
            let Some(index) = system.pipeline_ids.iter().position(|p| p == id) else {
                return Stats::failed(group, kind, id, Refusal::not_found(undeclared(group, "pipeline", id)));
            };
            answer.pipeline = Some(pipeline_stats(system, index, group, &totals, &shape));
        }
        NodeKind::Destination => {
            if !system.topology.destinations.iter().any(|d| d.name == id) {
                return Stats::failed(
                    group,
                    kind,
                    id,
                    Refusal::not_found(undeclared(group, "destination", id)),
                );
            }
            answer.destination = Some(destination_stats(group, id, &totals, &live, &shape));
        }
    }
    let subject = match kind {
        NodeKind::Source => Some(SubjectKind::Source),
        NodeKind::Destination => Some(SubjectKind::Destination),
        NodeKind::Pipeline => None,
    };
    if let (Some(subject), Some(store)) = (subject, series_store) {
        match warnings::spans(store, subject, group, id, since_ms) {
            Ok(spans) => answer.warnings = spans,
            // The counts stand without them: a chart with no marks still says what was read.
            Err(e) => tracing::warn!(
                target: "stats.warnings_unreadable",
                error = %e,
                "could not read when this object's warnings held, so its chart marks none"
            ),
        }
    }
    answer
}

fn undeclared(group: &str, kind: &str, id: &str) -> String {
    format!("group {group:?} declares no {kind} {id:?}")
}

/// The pipeline at `index`, its steps in the order they run.
fn pipeline_stats(
    system: &LoadedSystem,
    index: usize,
    group: &str,
    totals: &impl Fn(&str) -> Vec<SubjectTotal>,
    shape: &Shape,
) -> PipelineStats {
    let id = &system.pipeline_ids[index];
    let matched = totals(ROUTE_MATCHED_METRIC);
    // See `total`'s comment: summing zero routes is `-0.0`, not `0.0`, unless cleared.
    let routes: Vec<&str> = system
        .topology
        .routes
        .iter()
        .filter(|r| r.pipeline == index)
        .map(|r| r.id.as_str())
        .collect();
    let rows_in: f64 = routes
        .iter()
        .map(|route| total(&matched, group, &[("route", route)]))
        .sum::<f64>()
        + 0.0;
    let mut rows_by_bucket = Buckets::new();
    for route in &routes {
        for (bucket, rows) in buckets(&matched, group, &[("route", route)]) {
            *rows_by_bucket.entry(bucket).or_default() += rows;
        }
    }

    let (dropped, errored, seconds) = (
        totals(ROWS_DROPPED_METRIC),
        totals(ROWS_ERRORED_METRIC),
        totals(FUNCTION_SECONDS_METRIC),
    );
    let steps: Vec<StepShare> = running_steps(system, index)
        .iter()
        .map(|step| {
            let at = [("pipeline", id.as_str()), ("function", step.as_str())];
            StepShare {
                dropped: total(&dropped, group, &at),
                errored: total(&errored, group, &at),
                micros: total(&seconds, group, &at) * 1e6,
                step: step.clone(),
            }
        })
        .collect();

    let spent: f64 = steps.iter().map(|s| s.micros).sum();
    let spent_by_bucket = scaled(buckets(&seconds, group, &[("pipeline", id.as_str())]), 1e6);
    PipelineStats {
        rows_out: (rows_in - steps.iter().map(|s| s.dropped).sum::<f64>()).max(0.0),
        rows_in,
        micros_per_row: (rows_in > 0.0).then(|| spent / rows_in),
        steps,
        rate: shape.rate(&rows_by_bucket),
        per_row: shape.per_row(&spent_by_bucket, &rows_by_bucket),
    }
}

/// The steps of the pipeline at `index` that actually run, in order. A comment emits nothing and a disabled step is
/// skipped at compile (`plan::compile`), but both take a `FunctionId` and a place in `step_ids`, so listing that
/// directly would show a reader steps that cost nothing because they do not exist at runtime.
fn running_steps(system: &LoadedSystem, index: usize) -> Vec<String> {
    fn walk(steps: &[Step], step_ids: &[String], out: &mut Vec<String>) {
        for step in steps {
            match step {
                Step::Function(f) => {
                    if !f.disabled
                        && !matches!(f.kind, FunctionKind::Comment { .. })
                        && let Some(id) = step_ids.get(f.id as usize)
                    {
                        out.push(id.clone());
                    }
                }
                Step::If { id, then, else_, .. } => {
                    if let Some(step) = step_ids.get(*id as usize) {
                        out.push(step.clone());
                    }
                    walk(then, step_ids, out);
                    walk(else_, step_ids, out);
                }
            }
        }
    }
    let Some(pipeline) = system.pipelines.iter().find(|p| p.id == index) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    walk(&pipeline.body, &pipeline.step_ids, &mut out);
    out
}

fn destination_stats(
    group: &str,
    id: &str,
    totals: &impl Fn(&str) -> Vec<SubjectTotal>,
    live: &Live<'_>,
    shape: &Shape,
) -> DestinationStats {
    let at = [("destination", id)];
    let rejected = totals(ROWS_REJECTED_METRIC);
    let mut by_source: Vec<SourceRefusals> = rejected
        .iter()
        .filter(|s| s.label("group") == Some(group) && s.label("destination") == Some(id))
        .filter_map(|s| {
            Some(SourceRefusals {
                source: s.label("source")?.to_string(),
                rejected: s.total,
            })
        })
        .collect();
    by_source.sort_by(|a, b| b.rejected.total_cmp(&a.rejected).then_with(|| a.source.cmp(&b.source)));

    let delivered = totals(ROWS_DELIVERED_METRIC);
    DestinationStats {
        rate: shape.rate(&buckets(&delivered, group, &at)),
        bytes_rate: shape.rate(&buckets(&totals(BYTES_DELIVERED_METRIC), group, &at)),
        round_trip: shape.levels(&scaled(peaks(&totals(DESTINATION_RTT_METRIC), group, &at), 1e3)),
        received: total(&totals(ROWS_OUT_METRIC), group, &at),
        delivered: total(&delivered, group, &at),
        send_failed: total(&totals(ROWS_SEND_FAILED_METRIC), group, &at),
        rejected: total(&rejected, group, &at),
        stalls: total(&totals(DESTINATION_STALLS_METRIC), group, &at),
        rejected_by_source: by_source,
        queues: queue_fills(group, id, live),
        round_trip_ms: freshest_gauge(group, id, live, DESTINATION_RTT_METRIC).map(|seconds| seconds * 1e3),
        throttled_ms: total(&totals(DESTINATION_THROTTLED_METRIC), group, &at) * 1e3,
    }
}

/// One gauge for this destination off the freshest worker still counted as live, or `None` where nobody reports it.
fn freshest_gauge(group: &str, id: &str, live: &Live<'_>, metric: &str) -> Option<f64> {
    let stale_after = stale_after_ms(live.heartbeat_interval);
    live.snapshot
        .iter()
        .filter(|thread| thread.report.group_id == group && thread.last_seen_millis_ago as i64 <= stale_after)
        .filter_map(|thread| Some((thread.last_seen_millis_ago, gauge(&thread.report.metrics, metric, id)?)))
        .min_by_key(|(seen_ago, _)| *seen_ago)
        .map(|(_, value)| value)
}

/// Every worker's queue for this destination: fresh workers fullest first, then stale workers fullest first.
///
/// Off the live snapshot rather than the series, for the reason [`Live`] gives. A worker quiet for longer than
/// [`stale_after_ms`] keeps its last known fill, marked stale and sorted after every fresh one, so it can never stand
/// in for a queue that is still filling.
///
/// One row per worker. A worker runs one egress thread per destination, so a second report naming the same pair is a
/// thread the leader has not yet forgotten; the freshest wins, since it is the one describing what is running.
pub(super) fn queue_fills(group: &str, id: &str, live: &Live<'_>) -> Vec<QueueFill> {
    let stale_after = stale_after_ms(live.heartbeat_interval);
    let mut freshest: Vec<(u64, QueueFill)> = Vec::new();
    for thread in live.snapshot {
        if thread.report.group_id != group {
            continue;
        }
        let (Some(bytes), Some(budget)) = (
            gauge(&thread.report.metrics, DESTINATION_QUEUE_BYTES_METRIC, id),
            gauge(&thread.report.metrics, DESTINATION_QUEUE_BUDGET_METRIC, id),
        ) else {
            continue;
        };
        let fill = QueueFill {
            worker: thread.worker_id.clone(),
            bytes,
            budget,
            stale: thread.last_seen_millis_ago as i64 > stale_after,
        };
        match freshest.iter_mut().find(|(_, held)| held.worker == fill.worker) {
            Some((seen_ago, held)) if thread.last_seen_millis_ago < *seen_ago => {
                *seen_ago = thread.last_seen_millis_ago;
                *held = fill;
            }
            Some(_) => {}
            None => freshest.push((thread.last_seen_millis_ago, fill)),
        }
    }

    let mut out: Vec<QueueFill> = freshest.into_iter().map(|(_, fill)| fill).collect();
    // Fullest first, so the panel draws the answer before the detail. By share rather than bytes: two workers may
    // hold the same destination under different budgets, and the one closer to refusing is the one to show first.
    out.sort_by(|a, b| {
        a.stale
            .cmp(&b.stale)
            .then_with(|| share(b).total_cmp(&share(a)))
            .then_with(|| a.worker.cmp(&b.worker))
    });
    out
}

/// How full a queue is, 0 to 1. Zero where the budget is missing or nonsense rather than a division that yields an
/// infinity for something to draw.
pub(super) fn share(fill: &QueueFill) -> f64 {
    if fill.budget <= 0.0 {
        return 0.0;
    }
    fill.bytes / fill.budget
}

/// One gauge off a thread's freshest report, where it names this destination. `None` from a thread that never
/// reported it -- a reader or a transform, or a worker older than the metric.
fn gauge(metrics: &[MetricSampleDto], metric: &str, destination: &str) -> Option<f64> {
    metrics.iter().find_map(|sample| {
        if sample.metric != metric || label(&sample.labels, "destination") != Some(destination) {
            return None;
        }
        match sample.value {
            MetricValueDto::Gauge(v) => Some(v),
            _ => None,
        }
    })
}

/// `source`'s routes in the order they run, each with what its condition matched.
fn route_shares(system: &LoadedSystem, source: SourceId, group: &str, matched: &[SubjectTotal]) -> Vec<RouteShare> {
    system
        .topology
        .routes
        .iter()
        .filter(|r| r.source == source)
        .map(|r| RouteShare {
            pipeline: system
                .pipeline_ids
                .get(r.pipeline)
                .cloned()
                .expect("a validated system's routes only name pipelines it declares"),
            condition: r.cond.clone(),
            matched: total(matched, group, &[("route", &r.id)]),
            route: r.id.clone(),
        })
        .collect()
}

/// One subject's labels, decoded once, what it totaled over the window, and the buckets that total came from.
struct SubjectTotal {
    labels: Vec<(String, String)>,
    total: f64,
    /// Bucket start to value, oldest first (`RegistryStore::series_since`).
    points: Vec<(i64, f64)>,
}

/// A figure per bucket start, for the buckets that have one.
type Buckets = BTreeMap<i64, f64>;

/// The window's grain: how wide a bucket is, and which bucket is still filling.
struct Shape {
    bucket_ms: i64,
    now_bucket_ms: i64,
}

impl Shape {
    /// Each finished bucket's count as a per-second rate.
    fn rate(&self, counts: &Buckets) -> Option<Spread> {
        let per_second = 1000.0 / self.bucket_ms as f64;
        self.spread(counts.iter().map(|(&bucket, &count)| (bucket, count * per_second)))
    }

    /// Each finished bucket's `spent` over its `rows`, for the buckets that took a row. A bucket with time and no rows
    /// is left out rather than divided by zero, and a bucket with rows and no time reads as zero, which is what it
    /// cost.
    fn per_row(&self, spent: &Buckets, rows: &Buckets) -> Option<Spread> {
        self.spread(
            rows.iter()
                .filter(|(_, rows)| **rows > 0.0)
                .map(|(&bucket, &rows)| (bucket, spent.get(&bucket).copied().unwrap_or(0.0) / rows)),
        )
    }

    /// Each finished bucket's level, as the store kept it.
    fn levels(&self, levels: &Buckets) -> Option<Spread> {
        self.spread(levels.iter().map(|(&bucket, &level)| (bucket, level)))
    }

    fn spread(&self, points: impl Iterator<Item = (i64, f64)>) -> Option<Spread> {
        Spread::over(
            points
                .filter(|(bucket, _)| *bucket != self.now_bucket_ms)
                .map(|(bucket_millis, value)| SpreadPoint { bucket_millis, value })
                .collect(),
        )
    }
}

/// Like [`total`], bucket by bucket: every matching subject's counters added within each bucket.
fn buckets(totals: &[SubjectTotal], group: &str, labels: &[(&str, &str)]) -> Buckets {
    let mut out = Buckets::new();
    for (bucket, value) in matching_points(totals, group, labels) {
        *out.entry(bucket).or_default() += value;
    }
    out
}

/// `buckets` in another unit: the store keeps seconds, and the answer gives the unit its field names.
fn scaled(mut buckets: Buckets, factor: f64) -> Buckets {
    for value in buckets.values_mut() {
        *value *= factor;
    }
    buckets
}

/// Like [`buckets`], for a gauge: the highest reading within each bucket, which is what the store keeps of a gauge
/// and so the only rule that gives the same answer for one subject as for several.
fn peaks(totals: &[SubjectTotal], group: &str, labels: &[(&str, &str)]) -> Buckets {
    let mut out = Buckets::new();
    for (bucket, value) in matching_points(totals, group, labels) {
        out.entry(bucket)
            .and_modify(|peak| *peak = peak.max(value))
            .or_insert(value);
    }
    out
}

fn matching_points<'a>(
    totals: &'a [SubjectTotal],
    group: &'a str,
    labels: &'a [(&str, &str)],
) -> impl Iterator<Item = (i64, f64)> + 'a {
    totals
        .iter()
        .filter(move |subject| {
            subject.label("group") == Some(group)
                && labels.iter().all(|(name, value)| subject.label(name) == Some(*value))
        })
        .flat_map(|subject| subject.points.iter().copied())
}

impl SubjectTotal {
    fn label(&self, name: &str) -> Option<&str> {
        self.labels
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    }
}

/// Every subject in `group` carrying all of `labels`, summed. One pair usually names one subject; more than one
/// matches where a metric splits the same thing further -- `smeltr_rows_errored_total` carries a `kind` this does not
/// break out, and summing over it is what "errored here" means. Zero when nothing was recorded: see the module doc
/// on why an absence is a zero here.
fn total(totals: &[SubjectTotal], group: &str, labels: &[(&str, &str)]) -> f64 {
    // `Iterator::sum` on an empty `f64` iterator is `-0.0`, not the `0.0` this doc promises: a client
    // that formats `-0.0` shows a minus sign no count should ever carry. `+ 0.0` clears the sign.
    totals
        .iter()
        .filter(|subject| {
            subject.label("group") == Some(group)
                && labels.iter().all(|(name, value)| subject.label(name) == Some(*value))
        })
        .map(|subject| subject.total)
        .sum::<f64>()
        + 0.0
}

/// Every subject's total for `metric` over the window, its key decoded here rather than at each lookup -- one
/// answer asks after a step's three metrics and a route's one, and a key parsed per question would be parsed
/// hundreds of times. Summing is what a total means: a bucket holds a counter's increase over it, so the buckets in
/// a window add up to the window's count.
///
/// `maintain` moves a bucket to the next tier only once it ages out of its own, so the window's newest buckets are
/// still in the finer tiers. Their points are read too and keyed by `tier`'s bucket they fall in; [`buckets`] adds
/// a bucket's points and [`peaks`] takes their highest, as `maintain` will.
fn totals_by_subject(store: &RegistryStore, metric: &str, tier: Tier, since_ms: i64) -> Vec<SubjectTotal> {
    match store.series_since_at(metric, tier.and_finer(), since_ms) {
        Ok(read) => {
            let mut by_subject: BTreeMap<String, Vec<(i64, f64)>> = BTreeMap::new();
            for (subject_key, points) in read.into_iter().flatten() {
                let of = by_subject.entry(subject_key).or_default();
                of.extend(points.into_iter().map(|(at, value)| (tier.align(at), value)));
            }
            by_subject
                .into_iter()
                .filter_map(|(subject_key, mut points)| {
                    points.sort_by_key(|(at, _)| *at);
                    Some(SubjectTotal {
                        labels: serde_json::from_str(&subject_key).ok()?,
                        total: points.iter().map(|(_, v)| v).sum(),
                        points,
                    })
                })
                .collect()
        }
        Err(e) => {
            tracing::warn!(
                target: "stats.metric_unreadable",
                subject = %metric,
                error = %e,
                "could not read a metric for an object's stats, so its total reads as zero"
            );
            Vec::new()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config_file::load_str;

    /// Two routes out of one source that both match every row, plus `unmatched` as the manifest says it.
    fn system(unmatched: &str) -> LoadedSystem {
        let json = format!(
            r#"{{
              "version": 1,
              {unmatched}
              "sources": [{{ "id": "orders-west", "type": "metrics", "bucketSeconds": 10 }}],
              "destinations": [
                {{ "id": "es-prod", "type": "kafka", "brokers": "k:9092", "defaultTopic": "out" }},
                {{ "id": "leftovers", "type": "kafka", "brokers": "k:9092", "defaultTopic": "dlq" }}
              ],
              "pipelines": [
                {{ "id": "audit", "name": "Audit", "steps": [] }},
                {{ "id": "normalize", "name": "Normalize", "steps": [
                  {{ "id": "why", "type": "comment", "text": "emits nothing" }},
                  {{ "id": "keep-orders", "type": "drop", "condition": "false" }},
                  {{ "id": "off", "type": "eval", "disabled": true,
                     "assignments": [{{ "target": "x", "expr": "1" }}] }},
                  {{ "id": "late-only", "type": "if", "condition": "true", "then": [
                    {{ "id": "tag-late", "type": "eval", "assignments": [{{ "target": "late", "expr": "true" }}] }}
                  ] }}
                ] }}
              ],
              "routes": [
                {{ "id": "route-aaa", "source": "orders-west", "condition": "true", "pipeline": "audit",
                   "deliver": [{{ "to": ["es-prod"] }}] }},
                {{ "id": "route-bbb", "source": "orders-west", "condition": "true", "pipeline": "normalize",
                   "deliver": [{{ "to": ["es-prod"] }}] }}
              ]
            }}"#
        );
        load_str(&json).expect("the fixture loads")
    }

    fn with_dlq() -> LoadedSystem {
        system(r#""exceptions": { "kinds": { "Unmatched": { "deliver": ["leftovers"] } } },"#)
    }

    fn seeded() -> RegistryStore {
        let store = RegistryStore::open(std::path::Path::new(":memory:")).unwrap();
        let now = super::super::warnings::now_millis();
        let put = |metric: &str, key: &str, value: f64| store.record_delta(metric, key, now, value).unwrap();
        put(ROWS_IN_METRIC, r#"[["group","g"],["source","orders-west"]]"#, 1000.0);
        put(
            ROWS_UNMATCHED_METRIC,
            r#"[["group","g"],["source","orders-west"]]"#,
            164.0,
        );
        put(
            ROUTE_MATCHED_METRIC,
            r#"[["group","g"],["route","route-aaa"],["source","orders-west"]]"#,
            600.0,
        );
        put(
            ROUTE_MATCHED_METRIC,
            r#"[["group","g"],["route","route-bbb"],["source","orders-west"]]"#,
            700.0,
        );
        put(
            ROWS_DROPPED_METRIC,
            r#"[["function","keep-orders"],["group","g"],["pipeline","normalize"]]"#,
            40.0,
        );
        // Two kinds of error on one step: what the view shows is that the step errored, not which way.
        for kind in ["Unparsable", "TypeMismatch"] {
            put(
                ROWS_ERRORED_METRIC,
                &format!(r#"[["function","tag-late"],["group","g"],["kind","{kind}"],["pipeline","normalize"]]"#),
                3.5,
            );
        }
        put(
            FUNCTION_SECONDS_METRIC,
            r#"[["function","keep-orders"],["group","g"],["pipeline","normalize"]]"#,
            2100e-6,
        );
        put(
            FUNCTION_SECONDS_METRIC,
            r#"[["function","tag-late"],["group","g"],["pipeline","normalize"]]"#,
            350e-6,
        );
        put(ROWS_OUT_METRIC, r#"[["destination","es-prod"],["group","g"]]"#, 900.0);
        put(
            ROWS_DELIVERED_METRIC,
            r#"[["destination","es-prod"],["group","g"]]"#,
            880.0,
        );
        put(
            ROWS_SEND_FAILED_METRIC,
            r#"[["destination","es-prod"],["group","g"]]"#,
            12.0,
        );
        put(
            ROWS_REJECTED_METRIC,
            r#"[["destination","es-prod"],["group","g"],["source","orders-west"]]"#,
            5.0,
        );
        put(
            ROWS_REJECTED_METRIC,
            r#"[["destination","es-prod"],["group","g"],["source","clicks-west"]]"#,
            3.0,
        );
        store
    }

    /// A fleet that has said nothing. Every queue reads as absent, which is the honest answer where nobody has
    /// reported holding one.
    fn quiet() -> Live<'static> {
        Live {
            snapshot: &[],
            heartbeat_interval: BEAT,
            band_share: 0.99,
        }
    }

    const BEAT: Duration = Duration::from_secs(1);

    /// One egress thread reporting a queue for `destination`, `seen_ago` milliseconds since its last beat.
    fn holding(worker: &str, destination: &str, bytes: f64, budget: f64, seen_ago: u64) -> ThreadHealth {
        let at = |metric: &str, value: f64| MetricSampleDto {
            metric: metric.to_string(),
            labels: vec![("destination".to_string(), destination.to_string())],
            value: MetricValueDto::Gauge(value),
        };
        ThreadHealth {
            worker_id: worker.to_string(),
            last_seen_millis_ago: seen_ago,
            stop_after_unreachable_secs: None,
            commit: None,
            smeltr_version: String::new(),
            release: None,
            supervising: None,
            resources: None,
            authority_expires_millis: None,
            stopped_at_millis: None,
            report: crate::heartbeat::ThreadHeartbeat {
                thread_id: format!("egress-{destination}"),
                group_id: "g".to_string(),
                kind: Some(crate::heartbeat::ThreadKind::Egress),
                partitions_owned: vec![],
                metrics: vec![
                    at(DESTINATION_QUEUE_BYTES_METRIC, bytes),
                    at(DESTINATION_QUEUE_BUDGET_METRIC, budget),
                ],
                metrics_held: false,
            },
        }
    }

    fn built(system: &LoadedSystem, store: Option<&RegistryStore>) -> Envelope<Stats> {
        build(
            "g",
            system,
            NodeKind::Source,
            "orders-west",
            Duration::from_secs(600),
            store,
            quiet(),
        )
    }

    /// A share is named by the pipeline it feeds, because nobody names a route -- and it is listed whether or not
    /// the route matched anything. Shares are reported as they happened, not normalized: every route sees every
    /// row, so here they sum past what the source polled.
    #[test]
    fn a_sources_routes_come_back_with_their_shares() {
        let store = seeded();
        let answer = built(&with_dlq(), Some(&store));

        assert!(answer.succeeded(), "{:?}", answer.errors);
        let source = answer.body.source.expect("a source answers with source stats");
        assert_eq!(source.polled, 1000.0);
        assert_eq!(source.unmatched, 164.0);
        assert_eq!(
            source.unmatched_policy,
            crate::config_file::AnswerDto::Deliver {
                to: vec!["leftovers".into()],
            }
        );

        let shares: Vec<(&str, f64)> = source.routes.iter().map(|r| (r.pipeline.as_str(), r.matched)).collect();
        assert_eq!(shares, vec![("audit", 600.0), ("normalize", 700.0)]);
    }

    /// A manifest that says nothing blocks, and the answer says so rather than leaving the reader to know the default.
    #[test]
    fn a_deployment_naming_no_unmatched_policy_reports_block() {
        let source = built(&system(""), Some(&seeded())).body.source.unwrap();
        assert_eq!(source.unmatched, 164.0);
        assert_eq!(source.unmatched_policy, crate::config_file::AnswerDto::Block);
    }

    /// A leader with no durable registry has no history to serve, which is zero rather than an error.
    #[test]
    fn no_series_store_answers_with_zeroes_rather_than_failing() {
        let answer = built(&with_dlq(), None);
        assert!(answer.succeeded(), "{:?}", answer.errors);
        let source = answer.body.source.unwrap();
        assert_eq!(source.polled, 0.0);
        assert_eq!(source.routes.len(), 2);
    }

    fn built_for(kind: NodeKind, id: &str, store: Option<&RegistryStore>) -> Envelope<Stats> {
        build("g", &with_dlq(), kind, id, Duration::from_secs(600), store, quiet())
    }

    /// Like [`built_for`], but with a fleet that is saying something about its queues.
    fn built_live(id: &str, snapshot: &[ThreadHealth]) -> DestinationStats {
        build(
            "g",
            &with_dlq(),
            NodeKind::Destination,
            id,
            Duration::from_secs(600),
            None,
            Live {
                snapshot,
                heartbeat_interval: BEAT,
                band_share: 0.99,
            },
        )
        .body
        .destination
        .expect("a destination answers with destination stats")
    }

    /// Three finished buckets and the one still filling. The rate is each bucket's count per second over the ones
    /// that are done; the bucket holding now is left out, since a part of a bucket reads low.
    #[test]
    fn a_spread_is_over_the_finished_buckets_and_leaves_out_the_one_still_filling() {
        let store = RegistryStore::open(std::path::Path::new(":memory:")).unwrap();
        let now = super::super::warnings::now_millis();
        let bucket = Tier::TenSeconds.bucket_ms();
        let key = r#"[["group","g"],["source","orders-west"]]"#;
        for (ago, rows) in [(3, 1000.0), (2, 3000.0), (1, 2000.0), (0, 50.0)] {
            store
                .record_delta(ROWS_IN_METRIC, key, now - ago * bucket, rows)
                .unwrap();
        }
        let source = built_for(NodeKind::Source, "orders-west", Some(&store))
            .body
            .source
            .unwrap();
        let rate = source.rate.expect("three finished buckets make a spread");
        assert_eq!((rate.min, rate.max, rate.mean), (100.0, 300.0, 200.0));
        assert_eq!(rate.buckets.len(), 3, "the bucket holding now is not one of them");
        assert!(
            rate.buckets.windows(2).all(|w| w[0].bucket_millis < w[1].bucket_millis),
            "oldest first"
        );
        assert_eq!(
            source.polled, 6050.0,
            "the total still counts every bucket, the filling one too"
        );
    }

    /// A day's chart reads minute buckets, and the last hour is still in ten-second ones until `maintain` moves it.
    #[test]
    fn a_long_window_counts_its_newest_buckets_from_the_finer_tiers() {
        let store = RegistryStore::open(std::path::Path::new(":memory:")).unwrap();
        let now = super::super::warnings::now_millis();
        let minute = Tier::OneMinute.bucket_ms();
        let key = r#"[["group","g"],["source","orders-west"]]"#;
        store
            .record_delta(ROWS_IN_METRIC, key, now - 120 * minute, 600.0)
            .unwrap();
        let recent = Tier::OneMinute.align(now) - 5 * minute;
        store.record_delta(ROWS_IN_METRIC, key, recent, 60.0).unwrap();
        store.record_delta(ROWS_IN_METRIC, key, recent + 10_000, 60.0).unwrap();
        store.maintain(now).unwrap();
        let source = build(
            "g",
            &with_dlq(),
            NodeKind::Source,
            "orders-west",
            Duration::from_secs(24 * 60 * 60),
            Some(&store),
            quiet(),
        )
        .body
        .source
        .unwrap();
        assert_eq!(source.polled, 720.0);
        let rate = source.rate.unwrap();
        assert!(
            rate.buckets.iter().all(|b| b.bucket_millis % minute == 0),
            "on the minute grid"
        );
        assert_eq!(
            rate.buckets.iter().map(|b| b.value).collect::<Vec<_>>(),
            vec![10.0, 2.0],
            "the old bucket from the minute tier, the two ten-second ones added into theirs"
        );
    }

    /// Per-row time is each bucket's time over that bucket's rows. A bucket with time and no rows has no per-row
    /// figure; a bucket with rows and no time cost nothing.
    #[test]
    fn per_row_time_is_bucket_by_bucket_and_skips_buckets_that_took_no_row() {
        let store = RegistryStore::open(std::path::Path::new(":memory:")).unwrap();
        let now = super::super::warnings::now_millis();
        let bucket = Tier::TenSeconds.bucket_ms();
        let rows = r#"[["group","g"],["source","orders-west"]]"#;
        store
            .record_delta(ROWS_IN_METRIC, rows, now - 3 * bucket, 100.0)
            .unwrap();
        store
            .record_delta(TICK_POLL_SECONDS_METRIC, rows, now - 3 * bucket, 400e-6)
            .unwrap();
        store
            .record_delta(TICK_POLL_SECONDS_METRIC, rows, now - 2 * bucket, 900e-6)
            .unwrap();
        store.record_delta(ROWS_IN_METRIC, rows, now - bucket, 50.0).unwrap();
        let source = built_for(NodeKind::Source, "orders-west", Some(&store))
            .body
            .source
            .unwrap();
        let per_row = source.poll_per_row.unwrap();
        assert_eq!(
            per_row.buckets.iter().map(|p| p.value).collect::<Vec<_>>(),
            vec![4.0, 0.0]
        );
        assert_eq!((per_row.min, per_row.max, per_row.mean), (0.0, 4.0, 2.0));
    }

    /// A pipeline's rows per bucket are its routes' matches added within the bucket; its per-row time is every
    /// step's time over them. A destination's round trip comes off a gauge, so a bucket is its peak.
    #[test]
    fn a_pipeline_adds_its_routes_per_bucket_and_a_destinations_round_trip_is_each_buckets_peak() {
        let store = RegistryStore::open(std::path::Path::new(":memory:")).unwrap();
        let now = super::super::warnings::now_millis();
        let then = now - 2 * Tier::TenSeconds.bucket_ms();
        let route = |id: &str| format!(r#"[["group","g"],["route","{id}"],["source","orders-west"]]"#);
        store
            .record_delta(ROUTE_MATCHED_METRIC, &route("route-bbb"), then, 60.0)
            .unwrap();
        // `route-aaa` feeds `audit`, not `normalize`: it must not count here.
        store
            .record_delta(ROUTE_MATCHED_METRIC, &route("route-aaa"), then, 999.0)
            .unwrap();
        // `late-only` is an `if`. It is listed among the steps, so the time under its label shows on its own share.
        for (step, seconds) in [("keep-orders", 90e-6), ("late-only", 60e-6), ("tag-late", 30e-6)] {
            store
                .record_delta(
                    FUNCTION_SECONDS_METRIC,
                    &format!(r#"[["function","{step}"],["group","g"],["pipeline","normalize"]]"#),
                    then,
                    seconds,
                )
                .unwrap();
        }
        let pipeline = built_for(NodeKind::Pipeline, "normalize", Some(&store))
            .body
            .pipeline
            .unwrap();
        assert_eq!(pipeline.rate.unwrap().mean, 6.0);
        assert_eq!(pipeline.per_row.unwrap().mean, 3.0);
        assert_eq!(
            pipeline.steps.iter().find(|s| s.step == "late-only").map(|s| s.micros),
            Some(60.0)
        );

        let rtt = r#"[["destination","es-prod"],["group","g"]]"#;
        store.record_gauge_max(DESTINATION_RTT_METRIC, rtt, then, 0.12).unwrap();
        store
            .record_gauge_max(DESTINATION_RTT_METRIC, rtt, then, 0.095)
            .unwrap();
        let destination = built_for(NodeKind::Destination, "es-prod", Some(&store))
            .body
            .destination
            .unwrap();
        let trip = destination.round_trip.unwrap();
        assert_eq!(
            (trip.min, trip.max),
            (120.0, 120.0),
            "the bucket kept its peak, not its last reading"
        );
        assert!(
            destination.rate.is_none(),
            "nothing was delivered, so there is no rate to spread"
        );

        store.record_delta(BYTES_DELIVERED_METRIC, rtt, then, 20_480.0).unwrap();
        let destination = built_for(NodeKind::Destination, "es-prod", Some(&store))
            .body
            .destination
            .unwrap();
        assert_eq!(
            destination.bytes_rate.unwrap().mean,
            2048.0,
            "bytes per second over a ten-second bucket"
        );
    }

    /// Rows in is what the routes handed it; rows out is that less what its steps took, which is exact because a
    /// row leaves a pipeline only through a step that drops it.
    #[test]
    fn a_pipelines_rows_out_is_what_reached_it_less_what_its_steps_dropped() {
        let store = seeded();
        let answer = built_for(NodeKind::Pipeline, "normalize", Some(&store));

        assert!(answer.succeeded(), "{:?}", answer.errors);
        let p = answer.body.pipeline.expect("a pipeline answers with pipeline stats");
        assert_eq!(p.rows_in, 700.0, "route-bbb is the only route to it");
        assert_eq!(p.rows_out, 700.0 - 40.0);
        assert_eq!(
            p.steps.iter().map(|s| s.step.as_str()).collect::<Vec<_>>(),
            vec!["keep-orders", "late-only", "tag-late"],
            "the steps that run, in order, an `if` before its branch: a comment emits nothing and a disabled step \
             is skipped, so neither is a step that cost nothing -- it is not a step"
        );
        assert_eq!(p.steps[0].dropped, 40.0);
        assert_eq!(p.steps[2].errored, 7.0, "summed across every kind of error");
    }

    /// Time is served as a rate, so it compares between pipelines and between windows rather than tracking how busy
    /// the last ten minutes were.
    #[test]
    fn a_pipelines_time_is_per_row_and_absent_when_no_row_reached_it() {
        let store = seeded();
        let busy = built_for(NodeKind::Pipeline, "normalize", Some(&store))
            .body
            .pipeline
            .unwrap();
        assert_eq!(busy.micros_per_row, Some((2100.0 + 350.0) / 700.0));

        let idle = built_for(NodeKind::Pipeline, "audit", None).body.pipeline.unwrap();
        assert_eq!(idle.rows_in, 0.0);
        assert!(!idle.rows_in.is_sign_negative(), "rows_in is -0.0, not 0.0");
        assert!(idle.micros_per_row.is_none(), "a rate over no rows is not an answer");
    }

    /// What arrived cannot be split by source -- `smeltr_rows_sent_total` names only the destination -- but refusals can,
    /// and that is the one question a destination can answer about where its trouble comes from.
    #[test]
    fn a_destinations_refusals_are_broken_out_by_the_source_they_came_from() {
        let store = seeded();
        let answer = built_for(NodeKind::Destination, "es-prod", Some(&store));

        assert!(answer.succeeded(), "{:?}", answer.errors);
        let d = answer
            .body
            .destination
            .expect("a destination answers with destination stats");
        assert_eq!(d.received, 900.0);
        assert_eq!(d.delivered, 880.0);
        assert_eq!(d.send_failed, 12.0);
        assert_eq!(d.rejected, 5.0 + 3.0, "both sources' refusals, together");
        assert_eq!(
            d.rejected_by_source
                .iter()
                .map(|r| (r.source.as_str(), r.rejected))
                .collect::<Vec<_>>(),
            vec![("orders-west", 5.0), ("clicks-west", 3.0)],
            "worst first"
        );
    }

    /// A count with nothing behind it is zero, not `-0.0`: `Iterator::sum` on an empty set returns the latter, and
    /// a client that formats it renders a minus sign no count should carry.
    #[test]
    fn an_untouched_destinations_counts_carry_no_negative_sign() {
        let answer = built_for(NodeKind::Destination, "es-prod", None);
        let d = answer
            .body
            .destination
            .expect("a destination answers with destination stats");
        for (name, value) in [
            ("received", d.received),
            ("delivered", d.delivered),
            ("send_failed", d.send_failed),
            ("rejected", d.rejected),
            ("stalls", d.stalls),
        ] {
            assert_eq!(value, 0.0, "{name}");
            assert!(!value.is_sign_negative(), "{name} is -0.0, not 0.0");
        }
    }

    #[test]
    fn a_pipeline_the_group_does_not_declare_refuses() {
        let answer = built_for(NodeKind::Pipeline, "ghost", None);
        assert!(!answer.succeeded());
        assert!(answer.errors[0].message.contains("ghost"), "{:?}", answer.errors);
    }

    #[test]
    fn a_source_the_group_does_not_declare_refuses() {
        let system = with_dlq();
        let answer = build(
            "g",
            &system,
            NodeKind::Source,
            "ghost",
            Duration::from_secs(600),
            None,
            quiet(),
        );
        assert!(!answer.succeeded());
        assert!(answer.errors[0].message.contains("ghost"), "{:?}", answer.errors);
    }

    /// Fullest first, so the panel draws the worst queue before the rest of them.
    #[test]
    fn queues_come_back_fullest_first() {
        let fleet = [
            holding("w-quiet", "es-prod", 1_000.0, 16_000_000.0, 0),
            holding("w-jammed", "es-prod", 15_000_000.0, 16_000_000.0, 0),
            holding("w-middling", "es-prod", 8_000_000.0, 16_000_000.0, 0),
        ];
        let queues = built_live("es-prod", &fleet).queues;

        let order: Vec<&str> = queues.iter().map(|q| q.worker.as_str()).collect();
        assert_eq!(order, vec!["w-jammed", "w-middling", "w-quiet"]);
        assert_eq!(queues[0].bytes, 15_000_000.0);
        assert_eq!(queues[0].budget, 16_000_000.0);
    }

    /// By share, not by bytes. Two workers can hold the same destination under different budgets, and the one
    /// nearer refusing an offer is the one worth seeing first.
    #[test]
    fn a_smaller_queue_nearer_its_budget_sorts_above_a_bigger_one() {
        let fleet = [
            holding("w-big", "es-prod", 8_000_000.0, 16_000_000.0, 0),
            holding("w-small", "es-prod", 900_000.0, 1_000_000.0, 0),
        ];
        let queues = built_live("es-prod", &fleet).queues;
        let order: Vec<&str> = queues.iter().map(|q| q.worker.as_str()).collect();
        assert_eq!(
            order,
            vec!["w-small", "w-big"],
            "90% of a small budget beats 50% of a large one"
        );
    }

    /// A worker that has stopped beating keeps its last known fill, marked stale and listed after every fresh
    /// worker however full it was: it is not a queue anybody is holding now, and it is not a worker to lose sight of.
    #[test]
    fn a_worker_quiet_longer_than_the_stale_window_is_listed_last_and_marked_stale() {
        let stale = (BEAT * 3).as_millis() as u64 + 1;
        let fleet = [
            holding("w-gone", "es-prod", 15_000_000.0, 16_000_000.0, stale),
            holding("w-here", "es-prod", 2_000_000.0, 16_000_000.0, 0),
        ];
        let queues = built_live("es-prod", &fleet).queues;

        let order: Vec<(&str, bool)> = queues.iter().map(|q| (q.worker.as_str(), q.stale)).collect();
        assert_eq!(order, vec![("w-here", false), ("w-gone", true)]);
    }

    /// A worker reporting a queue for one destination says nothing about another's.
    #[test]
    fn a_queue_is_only_reported_for_the_destination_it_names() {
        let fleet = [holding("w1", "leftovers", 15_000_000.0, 16_000_000.0, 0)];
        assert!(built_live("es-prod", &fleet).queues.is_empty());
    }

    /// A worker heartbeating an empty queue is present at zero, which is what says the destination is keeping up.
    #[test]
    fn an_empty_queue_is_reported_rather_than_left_out() {
        let fleet = [holding("w1", "es-prod", 0.0, 16_000_000.0, 0)];
        let queues = built_live("es-prod", &fleet).queues;
        assert_eq!(queues.len(), 1);
        assert_eq!(queues[0].bytes, 0.0);
        assert!(
            !queues[0].bytes.is_sign_negative(),
            "a fill of zero carries no minus sign"
        );
    }

    /// One row per worker. A thread the leader has not yet forgotten must not draw its worker twice, and the
    /// freshest report is the one describing what is running.
    #[test]
    fn two_reports_from_one_worker_collapse_to_the_freshest() {
        let mut older = holding("w1", "es-prod", 15_000_000.0, 16_000_000.0, 400);
        older.report.thread_id = "egress-es-prod-old".to_string();
        let fleet = [older, holding("w1", "es-prod", 2_000_000.0, 16_000_000.0, 10)];
        let queues = built_live("es-prod", &fleet).queues;

        assert_eq!(queues.len(), 1, "one worker, one queue");
        assert_eq!(queues[0].bytes, 2_000_000.0, "the freshest beat wins");
    }

    /// A destination in another group with the same id is a different destination (`subject_key`'s reason).
    #[test]
    fn another_groups_worker_does_not_report_into_this_groups_queue() {
        let mut elsewhere = holding("w1", "es-prod", 15_000_000.0, 16_000_000.0, 0);
        elsewhere.report.group_id = "other".to_string();
        assert!(built_live("es-prod", &[elsewhere]).queues.is_empty());
    }

    /// The round trip is a level, like the queue: the freshest live worker's, and absent where no worker reports
    /// one -- an Elasticsearch destination, or nobody running it.
    #[test]
    fn the_round_trip_is_the_freshest_workers_and_absent_where_nobody_measures_one() {
        let with_rtt = |worker: &str, seconds: f64, seen_ago: u64| {
            let mut thread = holding(worker, "es-prod", 1.0, 16_000_000.0, seen_ago);
            thread.report.metrics.push(MetricSampleDto {
                metric: DESTINATION_RTT_METRIC.to_string(),
                labels: vec![("destination".to_string(), "es-prod".to_string())],
                value: MetricValueDto::Gauge(seconds),
            });
            thread
        };
        let stale = stale_after_ms(BEAT) as u64 + 1;
        let fleet = [
            with_rtt("w-older", 0.25, 900),
            with_rtt("w-fresh", 0.125, 100),
            with_rtt("w-gone", 0.005, stale),
        ];
        let stats = built_live("es-prod", &fleet);
        assert_eq!(stats.round_trip_ms, Some(125.0));
        assert_eq!(stats.throttled_ms, 0.0, "nothing throttled in a store with no series");

        let quiet = [holding("w1", "leftovers", 1.0, 16_000_000.0, 0)];
        assert_eq!(built_live("leftovers", &quiet).round_trip_ms, None);
    }
}
