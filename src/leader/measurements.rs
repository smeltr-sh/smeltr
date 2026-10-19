//! What the map paints on its nodes right now (design doc "The map"): each source's and destination's rate, how
//! full its busiest worker's queue is, a source's lag, and the warnings naming it -- or naming its group, when a
//! warning is about something in the group the map draws no node for, such as a worker.
//!
//! Read apart from [`super::map`] so the page can poll it. `map::build` compiles every group on every call; this
//! reads the registry and the warning catalog and compiles nothing, whatever the branch holds. The page joins the two on (group, kind,
//! id), and polls this one alone until `commit` moves, which is when the topology is worth compiling again.
//!
//! A node with nothing to report is absent, never zeroed: no reading is not the same as an idle one.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use serde::Serialize;

use super::access::{Access, Level};
use super::holds::Seen;
use super::map::NodeKind;
use super::series::{RegistryStore, SubjectPoints, Tier};
use super::stats::{self, Live};
use super::store::HeldSource;
use super::throughput;
use super::warnings::{self, SubjectKind};
use super::{ROWS_DELIVERED_METRIC, ROWS_IN_METRIC, ROWS_UNMATCHED_METRIC, subject_label};
use crate::heartbeat::SourceKey;

/// Kafka lag per partition (`runtime.rs`), labeled with the source, topic and partition.
const SOURCE_LAG_METRIC: &str = "smeltr_source_lag";

/// How many closed buckets back a rate still counts as current. One poll can land just after a bucket closes, before
/// any worker has written into the next; two keeps that from reading as a stop.
const CURRENT_BUCKETS: i64 = 2;

/// How far back a reading is still worth saying how old it is. Past this a node simply has no reading.
const LAST_SEEN_WINDOW_MS: i64 = 15 * 60 * 1000;

/// A queue at or past this share of its budget is near capacity. Not backpressure: a worker refusing offers is what
/// says that, and the warning catalog reports it.
const NEAR_CAPACITY: f64 = 0.9;

/// What the fleet reports about each node now.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Measurements {
    /// The commit the branch asked about is at. The topology the page drew was compiled from some commit
    /// (`map::Map::commit`); while the two agree it is still the topology.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub commit: Option<String>,
    /// The leader's clock when it answered, which every age here is measured from.
    pub now_millis: i64,
    /// How many milliseconds of traffic a rate is read over.
    pub bucket_millis: i64,
    pub nodes: Vec<NodeReading>,
    /// The catalog's warnings that name a group but no source or destination in it: a worker's, an identity's. The
    /// map lists them under the group they name.
    pub group_warnings: Vec<warnings::Warning>,
}

impl Measurements {
    /// Each source's normal, from `readings`, on the node it is about.
    pub fn with_expected(mut self, readings: &[throughput::Reading]) -> Self {
        let mut nodes: Nodes = self
            .nodes
            .into_iter()
            .map(|n| ((n.group.clone(), n.kind, n.id.clone()), n))
            .collect();
        let seconds = throughput::WINDOW.as_secs_f64();
        for r in readings {
            node(&mut nodes, &r.group, NodeKind::Source, &r.source).expected = Some(Expected {
                low: r.band.low / seconds,
                high: r.band.high / seconds,
            });
        }
        self.nodes = nodes.into_values().collect();
        self
    }

    /// Only what `access` holds viewer in: the fleet's readings are read whole and then narrowed, so the map
    /// draws the caller's groups and nothing of the others.
    pub fn visible_to(mut self, access: &Access) -> Self {
        self.nodes.retain(|n| access.allows(&n.group, Level::Viewer));
        self.group_warnings.retain(|w| w.visible_to(access));
        self
    }
}

/// One source or destination in one group.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeReading {
    pub group: String,
    pub kind: NodeKind,
    /// The author's id, as `map::MapNode::author_id` names it.
    pub id: String,
    /// Rows per second over the newest closed bucket: read for a source, acknowledged for a destination. Absent when
    /// that bucket is older than the last [`CURRENT_BUCKETS`].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rate: Option<f64>,
    /// When the newest bucket holding any report for this node closed. Present with `rate` absent, it says how
    /// stale the last reading is.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_seen_millis: Option<i64>,
    /// A destination's queues, across the workers of its group.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub queue: Option<QueueSummary>,
    /// A source's Kafka lag in records, summed over its partitions' current readings.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lag: Option<f64>,
    /// The worker holding a single-worker source at the place it reads now.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub holder: Option<String>,
    /// When this leader last heard from `holder`. Absent with `holder` present: not heard since the leader started.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub holder_heard_millis: Option<i64>,
    /// How many items that hold's ledger keeps. Present with `holder`; 0 for a source that keeps a cursor instead.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ledger_entries: Option<u64>,
    /// A source's rows per second that no route claimed, over the same current bucket as `rate`. Absent when that
    /// bucket is not current.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unmatched: Option<f64>,
    /// A source's normal rows per second at this time of day (`throughput`). Absent while it has too little
    /// history to have one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected: Option<Expected>,
    /// The warning catalog's warnings naming this node in this group, one per condition.
    pub warnings: Vec<warnings::Warning>,
}

/// The rates a source's normal holds, in rows per second.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct Expected {
    pub low: f64,
    pub high: f64,
}

/// One destination's queues, summed up the way a card draws them: by its busiest worker, never by an average that
/// would hide one full worker among idle ones.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QueueSummary {
    /// The fullest current worker's held bytes over its budget. Can pass 1. Absent when no current worker reports.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub busiest: Option<f64>,
    /// Current workers at or past [`NEAR_CAPACITY`].
    pub near_capacity: u32,
    /// Workers whose current heartbeat reports this queue.
    pub fresh: u32,
    /// Workers that reported this queue but whose last heartbeat is stale.
    pub stale: u32,
    /// Every worker the leader knows in this group, silent ones included: each runs every destination its group
    /// deploys, so this is how many queues there should be.
    pub workers: u32,
}

/// Who holds the single-worker sources: every hold the store keeps, and the holds that are at the place their
/// source reads today ([`reading_now`]), each with when its holder was last heard. A hold at any other place is left
/// over from where the source used to read, and is not its holder.
pub struct Holding<'a> {
    pub held: &'a [HeldSource],
    pub reading_now: &'a BTreeMap<(String, SourceKey), Option<i64>>,
}

/// The holds in `held` at the place their source reads now, each with when its holder was last heard, in epoch
/// milliseconds, or `None` when this leader has not heard from it yet. `seen` says what the leader has heard
/// ([`Holds::seen`](super::holds::Holds::seen)).
///
/// The leader compiles nothing here. A holder is answered with the key its deployed topology lists, place and all,
/// so a hold its holder was last answered with is at the place its source reads now. A holder not heard since this
/// leader started has been answered nothing yet, so its hold is taken as current only when it is the one hold its
/// source has in its group: with two, either may be at a place the source has left.
pub fn reading_now(
    held: &[HeldSource],
    seen: impl Fn(&HeldSource) -> Option<Seen>,
) -> BTreeMap<(String, SourceKey), Option<i64>> {
    let now = warnings::now_millis();
    let mut holds_of: HashMap<(&str, &str), usize> = HashMap::new();
    for h in held {
        *holds_of.entry((&h.group, &h.key.source)).or_default() += 1;
    }
    held.iter()
        .filter_map(|h| {
            let heard = match seen(h)? {
                Seen::Answered(ago) => Some(now - i64::try_from(ago.as_millis()).unwrap_or(i64::MAX).min(now)),
                Seen::Unheard if holds_of[&(h.group.as_str(), h.key.source.as_str())] == 1 => None,
                Seen::Unheard => return None,
            };
            Some(((h.group.clone(), h.key.clone()), heard))
        })
        .collect()
}

/// The fleet's readings at `now_ms`, the leader's clock, which the buckets are aligned to. `commit` is left for the
/// caller, which knows what branch was asked about.
pub fn build(
    warnings: &[warnings::Warning],
    series: Option<&RegistryStore>,
    holding: Holding<'_>,
    live: Live<'_>,
    now_ms: i64,
) -> Measurements {
    let tier = Tier::TenSeconds;
    let mut nodes: Nodes = BTreeMap::new();
    if let Some(store) = series {
        for (metric, label, kind) in [
            (ROWS_IN_METRIC, "source", NodeKind::Source),
            (ROWS_DELIVERED_METRIC, "destination", NodeKind::Destination),
        ] {
            for ((group, id), (bucket, count)) in newest_closed(store, metric, label, now_ms) {
                let closed_at = bucket + tier.bucket_ms();
                let reading = node(&mut nodes, &group, kind, &id);
                reading.last_seen_millis = Some(closed_at);
                if bucket >= tier.align(now_ms) - CURRENT_BUCKETS * tier.bucket_ms() {
                    reading.rate = Some(count * 1000.0 / tier.bucket_ms() as f64);
                }
            }
        }
        // Off the rate's own bucket only: a bucket where every row matched records no unmatched point at all.
        for ((group, id), (bucket, count)) in newest_closed(store, ROWS_UNMATCHED_METRIC, "source", now_ms) {
            if let Some(r) = nodes.get_mut(&(group, NodeKind::Source, id))
                && r.rate.is_some()
                && r.last_seen_millis == Some(bucket + tier.bucket_ms())
            {
                r.unmatched = Some(count * 1000.0 / tier.bucket_ms() as f64);
            }
        }
        for ((group, id), lag) in current_lag(store, now_ms) {
            node(&mut nodes, &group, NodeKind::Source, &id).lag = Some(lag);
        }
    }

    for held in holding.held {
        if let Some(&heard) = holding.reading_now.get(&(held.group.clone(), held.key.clone())) {
            let reading = node(&mut nodes, &held.group, NodeKind::Source, &held.key.source);
            reading.holder = Some(held.holder.clone());
            reading.holder_heard_millis = heard;
            reading.ledger_entries = Some(held.ledger_entries);
        }
    }

    for (group, id) in queued_destinations(&live) {
        let summary = queue_summary(&group, &id, &live);
        node(&mut nodes, &group, NodeKind::Destination, &id).queue = Some(summary);
    }

    let mut group_warnings = Vec::new();
    for w in warnings {
        let Some(group) = &w.group else { continue };
        let kind = match w.subject_kind {
            SubjectKind::Source => NodeKind::Source,
            SubjectKind::Destination => NodeKind::Destination,
            SubjectKind::Pipeline => NodeKind::Pipeline,
            _ => {
                group_warnings.push(w.clone());
                continue;
            }
        };
        let on_node = &mut node(&mut nodes, group, kind, &w.subject_id).warnings;
        if !on_node.iter().any(|seen| seen.condition == w.condition) {
            on_node.push(w.clone());
        }
    }

    Measurements {
        commit: None,
        now_millis: now_ms,
        bucket_millis: tier.bucket_ms(),
        nodes: nodes.into_values().collect(),
        group_warnings,
    }
}

type Nodes = BTreeMap<(String, NodeKind, String), NodeReading>;

/// The reading for one node, started empty the first time anything reports on it.
fn node<'a>(nodes: &'a mut Nodes, group: &str, kind: NodeKind, id: &str) -> &'a mut NodeReading {
    nodes
        .entry((group.to_string(), kind, id.to_string()))
        .or_insert_with(|| NodeReading {
            group: group.to_string(),
            kind,
            id: id.to_string(),
            rate: None,
            last_seen_millis: None,
            queue: None,
            lag: None,
            holder: None,
            holder_heard_millis: None,
            ledger_entries: None,
            unmatched: None,
            expected: None,
            warnings: Vec::new(),
        })
}

/// `metric`'s newest closed bucket per (group, `label`) in the last [`LAST_SEEN_WINDOW_MS`], summed across every
/// subject naming that pair. The bucket holding `now_ms` is still filling, and a rate off part of it reads low.
fn newest_closed(
    store: &RegistryStore,
    metric: &str,
    label: &str,
    now_ms: i64,
) -> BTreeMap<(String, String), (i64, f64)> {
    let tier = Tier::TenSeconds;
    let open = tier.align(now_ms);
    let mut by_bucket: BTreeMap<(String, String), BTreeMap<i64, f64>> = BTreeMap::new();
    for (subject_key, points) in read(store, metric, now_ms - LAST_SEEN_WINDOW_MS) {
        let (Some(group), Some(id)) = (subject_label(&subject_key, "group"), subject_label(&subject_key, label)) else {
            continue;
        };
        let buckets = by_bucket.entry((group, id)).or_default();
        for (bucket, value) in points.into_iter().filter(|(bucket, _)| *bucket < open) {
            *buckets.entry(bucket).or_default() += value;
        }
    }
    by_bucket
        .into_iter()
        .filter_map(|(key, buckets)| Some((key, buckets.into_iter().next_back()?)))
        .collect()
}

/// Each source's lag now: every partition's newest reading in the open bucket or the [`CURRENT_BUCKETS`] closed ones
/// before it, summed. Lag is a level, so the open bucket's reading is the newest one rather than a partial count. A
/// partition gone quiet longer than that is left out rather than counted at a level nobody is reporting.
fn current_lag(store: &RegistryStore, now_ms: i64) -> BTreeMap<(String, String), f64> {
    let tier = Tier::TenSeconds;
    let since = tier.align(now_ms) - CURRENT_BUCKETS * tier.bucket_ms();
    let mut lag: BTreeMap<(String, String), f64> = BTreeMap::new();
    for (subject_key, points) in read(store, SOURCE_LAG_METRIC, since) {
        let (Some(group), Some(source), Some(&(_, value))) = (
            subject_label(&subject_key, "group"),
            subject_label(&subject_key, "source"),
            points.last(),
        ) else {
            continue;
        };
        *lag.entry((group, source)).or_default() += value;
    }
    lag
}

fn read(store: &RegistryStore, metric: &str, since_ms: i64) -> Vec<(String, SubjectPoints)> {
    store
        .series_since(metric, Tier::TenSeconds, since_ms)
        .unwrap_or_else(|e| {
            tracing::warn!(
                target: "map.metric_unreadable",
                subject = %metric,
                error = %e,
                "could not read a metric for the map's measurements, so its nodes show no reading"
            );
            Vec::new()
        })
}

/// Every (group, destination) some worker has reported a queue for.
fn queued_destinations(live: &Live<'_>) -> BTreeSet<(String, String)> {
    live.snapshot
        .iter()
        .flat_map(|thread| {
            thread.report.metrics.iter().filter_map(move |sample| {
                if sample.metric != stats::DESTINATION_QUEUE_BYTES_METRIC {
                    return None;
                }
                let id = warnings::label(&sample.labels, "destination")?;
                Some((thread.report.group_id.clone(), id.to_string()))
            })
        })
        .collect()
}

/// Each running worker's group: the group of its freshest row. A worker restarted into another group keeps its old
/// threads' rows under the old one, and is not that group's any more.
fn current_groups<'a>(live: &Live<'a>) -> BTreeMap<&'a str, &'a str> {
    let mut freshest: BTreeMap<&str, (u64, &str)> = BTreeMap::new();
    for t in live.snapshot.iter().filter(|t| t.stopped_at_millis.is_none()) {
        let row = (t.last_seen_millis_ago, t.report.group_id.as_str());
        freshest
            .entry(t.worker_id.as_str())
            .and_modify(|held| *held = (*held).min(row))
            .or_insert(row);
    }
    freshest
        .into_iter()
        .map(|(worker, (_, group))| (worker, group))
        .collect()
}

fn queue_summary(group: &str, id: &str, live: &Live<'_>) -> QueueSummary {
    let current = current_groups(live);
    let fills: Vec<_> = stats::queue_fills(group, id, live)
        .into_iter()
        .filter(|q| current.get(q.worker.as_str()) == Some(&group))
        .collect();
    let fresh: Vec<f64> = fills.iter().filter(|q| !q.stale).map(stats::share).collect();
    let workers = current.values().filter(|g| **g == group).count();
    QueueSummary {
        busiest: fresh.iter().copied().reduce(f64::max),
        near_capacity: fresh.iter().filter(|s| **s >= NEAR_CAPACITY).count() as u32,
        fresh: fresh.len() as u32,
        stale: (fills.len() - fresh.len()) as u32,
        workers: workers as u32,
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::heartbeat::{MetricSampleDto, MetricValueDto};
    use crate::leader::registry::ThreadHealth;
    use crate::leader::warnings::{Condition, Severity, Warning};

    const BEAT: Duration = Duration::from_secs(1);

    fn store() -> RegistryStore {
        RegistryStore::open(std::path::Path::new(":memory:")).unwrap()
    }

    fn live(snapshot: &[ThreadHealth]) -> Live<'_> {
        Live {
            snapshot,
            heartbeat_interval: BEAT,
            band_share: 0.99,
        }
    }

    /// One egress thread of `worker` in group `g`, reporting a queue for `destination`, `seen_ago` ms since its beat.
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
                kind: None,
                partitions_owned: vec![],
                metrics: vec![
                    at(stats::DESTINATION_QUEUE_BYTES_METRIC, bytes),
                    at(stats::DESTINATION_QUEUE_BUDGET_METRIC, budget),
                ],
            },
        }
    }

    fn no_holds() -> Holding<'static> {
        static NONE: BTreeMap<(String, SourceKey), Option<i64>> = BTreeMap::new();
        Holding {
            held: &[],
            reading_now: &NONE,
        }
    }

    fn reading<'a>(answer: &'a Measurements, kind: NodeKind, id: &str) -> &'a NodeReading {
        answer
            .nodes
            .iter()
            .find(|n| n.kind == kind && n.id == id)
            .unwrap_or_else(|| panic!("no reading for {id}: {:?}", answer.nodes))
    }

    /// A rate is rows per second over the newest closed bucket. The bucket still filling would read low.
    #[test]
    fn a_rate_is_the_newest_closed_bucket_per_second() {
        let store = store();
        let now = warnings::now_millis();
        let open = Tier::TenSeconds.align(now);
        let key = r#"[["group","g"],["source","orders"]]"#;
        store.record_delta(ROWS_IN_METRIC, key, open - 10_000, 1200.0).unwrap();
        store.record_delta(ROWS_IN_METRIC, key, open, 7.0).unwrap();

        let answer = build(&[], Some(&store), no_holds(), live(&[]), now);
        let node = reading(&answer, NodeKind::Source, "orders");
        assert_eq!(node.rate, Some(120.0));
        assert_eq!(node.last_seen_millis, Some(open));
    }

    /// Unmatched rows read off the rate's own bucket. One from an earlier bucket is not current, though no point says
    /// the newer bucket had none.
    #[test]
    fn unmatched_rows_are_a_rate_off_the_rate_bucket() {
        let store = store();
        let now = warnings::now_millis();
        let open = Tier::TenSeconds.align(now);
        let metrics = r#"[["group","g"],["source","metrics"]]"#;
        let logs = r#"[["group","g"],["source","logs"]]"#;
        for (metric, key, at, rows) in [
            (ROWS_IN_METRIC, metrics, open - 10_000, 66.0),
            (ROWS_UNMATCHED_METRIC, metrics, open - 10_000, 66.0),
            (ROWS_UNMATCHED_METRIC, logs, open - 20_000, 50.0),
            (ROWS_IN_METRIC, logs, open - 10_000, 30.0),
        ] {
            store.record_delta(metric, key, at, rows).unwrap();
        }

        let answer = build(&[], Some(&store), no_holds(), live(&[]), now);
        assert_eq!(reading(&answer, NodeKind::Source, "metrics").unmatched, Some(6.6));
        assert_eq!(reading(&answer, NodeKind::Source, "logs").unmatched, None);
    }

    /// A node that went quiet keeps when it was last heard from, and has no rate: a stale number is not current.
    #[test]
    fn a_quiet_node_has_an_age_and_no_rate() {
        let store = store();
        let now = warnings::now_millis();
        let open = Tier::TenSeconds.align(now);
        let key = r#"[["destination","es-prod"],["group","g"]]"#;
        store
            .record_delta(ROWS_DELIVERED_METRIC, key, open - 120_000, 500.0)
            .unwrap();

        let answer = build(&[], Some(&store), no_holds(), live(&[]), now);
        let node = reading(&answer, NodeKind::Destination, "es-prod");
        assert_eq!(node.rate, None);
        assert_eq!(node.last_seen_millis, Some(open - 110_000));
    }

    /// A bucket counts as current while it is one of the last `CURRENT_BUCKETS` closed ones, so a poll landing just
    /// after a bucket closes still has a rate; one older than that is an age, not a rate.
    #[test]
    fn a_rate_is_current_for_the_last_closed_buckets_only() {
        let store = store();
        let now = warnings::now_millis();
        let open = Tier::TenSeconds.align(now);
        store
            .record_delta(
                ROWS_IN_METRIC,
                r#"[["group","g"],["source","recent"]]"#,
                open - 20_000,
                100.0,
            )
            .unwrap();
        store
            .record_delta(
                ROWS_IN_METRIC,
                r#"[["group","g"],["source","older"]]"#,
                open - 30_000,
                100.0,
            )
            .unwrap();

        let answer = build(&[], Some(&store), no_holds(), live(&[]), now);
        assert_eq!(reading(&answer, NodeKind::Source, "recent").rate, Some(10.0));
        let older = reading(&answer, NodeKind::Source, "older");
        assert_eq!(older.rate, None);
        assert_eq!(older.last_seen_millis, Some(open - 20_000));
    }

    /// Two groups running a source with one id are two sources (`registry::subject_key`'s reason).
    #[test]
    fn the_same_id_in_two_groups_is_two_readings() {
        let store = store();
        let now = warnings::now_millis();
        let open = Tier::TenSeconds.align(now);
        store
            .record_delta(
                ROWS_IN_METRIC,
                r#"[["group","a"],["source","orders"]]"#,
                open - 10_000,
                10.0,
            )
            .unwrap();
        store
            .record_delta(
                ROWS_IN_METRIC,
                r#"[["group","b"],["source","orders"]]"#,
                open - 10_000,
                30.0,
            )
            .unwrap();

        let answer = build(&[], Some(&store), no_holds(), live(&[]), now);
        let rates: Vec<(&str, Option<f64>)> = answer.nodes.iter().map(|n| (n.group.as_str(), n.rate)).collect();
        assert_eq!(rates, vec![("a", Some(1.0)), ("b", Some(3.0))]);
    }

    /// Lag is summed over a source's partitions, from the ones reporting now only.
    #[test]
    fn lag_sums_current_partitions_only() {
        let store = store();
        let now = warnings::now_millis();
        let open = Tier::TenSeconds.align(now);
        let key = |p: &str| format!(r#"[["group","g"],["partition","{p}"],["source","orders"],["topic","t"]]"#);
        store
            .record_gauge_max(SOURCE_LAG_METRIC, &key("0"), open, 100.0)
            .unwrap();
        store
            .record_gauge_max(SOURCE_LAG_METRIC, &key("1"), open - 10_000, 50.0)
            .unwrap();
        store
            .record_gauge_max(SOURCE_LAG_METRIC, &key("2"), open - 600_000, 9_000.0)
            .unwrap();

        let answer = build(&[], Some(&store), no_holds(), live(&[]), now);
        assert_eq!(reading(&answer, NodeKind::Source, "orders").lag, Some(150.0));
    }

    /// The busiest current worker is what a card shows, so one full worker is never averaged away. A stale worker
    /// is counted but never sets the figure.
    #[test]
    fn a_queue_summary_is_its_busiest_current_worker() {
        let stale = (BEAT * 3).as_millis() as u64 + 1;
        let mut silent = holding("w-silent", "other", 0.0, 1.0, stale);
        silent.report.metrics.clear();
        let fleet = [
            holding("w-hot", "es-prod", 95.0, 100.0, 0),
            holding("w-idle", "es-prod", 5.0, 100.0, 0),
            holding("w-gone", "es-prod", 150.0, 100.0, stale),
            silent,
        ];

        let answer = build(&[], None, no_holds(), live(&fleet), warnings::now_millis());
        let queue = reading(&answer, NodeKind::Destination, "es-prod")
            .queue
            .clone()
            .expect("a reported queue");
        assert_eq!(
            queue,
            QueueSummary {
                busiest: Some(0.95),
                near_capacity: 1,
                fresh: 2,
                stale: 1,
                workers: 4,
            }
        );
    }

    /// A worker restarted into another group leaves its old threads' rows behind; the old group neither counts it
    /// nor reads its queue.
    #[test]
    fn a_worker_moved_to_another_group_counts_only_there() {
        let mut left = holding("w-moved", "es-prod", 90.0, 100.0, 5_000);
        left.report.thread_id = "egress-old".to_string();
        let mut now_in = holding("w-moved", "es-prod", 10.0, 100.0, 0);
        now_in.report.group_id = "h".to_string();
        let fleet = [holding("w-stays", "es-prod", 20.0, 100.0, 0), left, now_in];

        let answer = build(&[], None, no_holds(), live(&fleet), warnings::now_millis());
        let in_g = answer
            .nodes
            .iter()
            .find(|n| n.group == "g" && n.id == "es-prod")
            .and_then(|n| n.queue.clone())
            .expect("a reported queue");
        assert_eq!(
            (in_g.workers, in_g.fresh, in_g.stale, in_g.busiest),
            (1, 1, 0, Some(0.2))
        );
    }

    /// The catalog's warnings ride on the node they name, in the group they name; one naming the group but no node
    /// rides on the group, and one naming no group is left out.
    #[test]
    fn warnings_attach_to_their_node() {
        let warning = |condition, subject_kind, id: &str| Warning {
            scope: None,
            condition,
            subject_kind,
            site: None,
            subject_id: id.to_string(),
            group: Some("g".to_string()),
            severity: Severity::Warning,
            since_millis: None,
            message: String::new(),
        };
        let warnings = [
            warning(Condition::SourceBlocked, SubjectKind::Source, "orders"),
            warning(Condition::LagClimbing, SubjectKind::Source, "orders"),
            warning(Condition::DestinationRefusing, SubjectKind::Destination, "es-prod"),
            warning(Condition::WorkerSilent, SubjectKind::Worker, "w1"),
            Warning {
                group: None,
                ..warning(Condition::WorkerNeverHeartbeated, SubjectKind::Worker, "w2")
            },
        ];

        let answer = build(&warnings, None, no_holds(), live(&[]), warnings::now_millis());
        assert_eq!(answer.nodes.len(), 2, "{:?}", answer.nodes);
        let conditions = |kind, id| -> Vec<Condition> {
            reading(&answer, kind, id)
                .warnings
                .iter()
                .map(|w| w.condition)
                .collect()
        };
        assert_eq!(
            conditions(NodeKind::Source, "orders"),
            vec![Condition::SourceBlocked, Condition::LagClimbing]
        );
        assert_eq!(
            conditions(NodeKind::Destination, "es-prod"),
            vec![Condition::DestinationRefusing]
        );
        assert_eq!(answer.group_warnings, vec![warnings[3].clone()]);
    }

    /// A viewer reads the nodes and group warnings of the groups they hold, and nothing of the others.
    #[test]
    fn a_viewer_reads_only_their_groups() {
        let warning = |group: &str| Warning {
            scope: None,
            condition: Condition::WorkerSilent,
            subject_kind: SubjectKind::Worker,
            site: None,
            subject_id: format!("w-{group}"),
            group: Some(group.to_string()),
            severity: Severity::Warning,
            since_millis: None,
            message: String::new(),
        };
        let blocked = |group: &str| Warning {
            condition: Condition::SourceBlocked,
            subject_kind: SubjectKind::Source,
            subject_id: "orders".to_string(),
            ..warning(group)
        };
        let warnings = [
            blocked("payments"),
            blocked("search"),
            warning("payments"),
            warning("search"),
        ];
        let payments = Access::Areas(BTreeMap::from([("payments".to_string(), Level::Viewer)]));

        let answer = build(&warnings, None, no_holds(), live(&[]), warnings::now_millis()).visible_to(&payments);
        let groups: BTreeSet<&str> = answer.nodes.iter().map(|n| n.group.as_str()).collect();
        assert_eq!(groups, BTreeSet::from(["payments"]));
        assert_eq!(answer.group_warnings, vec![warnings[2].clone()]);
    }

    /// A source held at the place it reads now names its holder and its ledger's size. A hold left at a place the
    /// source no longer reads names nobody, and a source with no hold has neither.
    #[test]
    fn a_hold_rides_on_its_source_at_the_place_it_reads_now() {
        let key = |source: &str, place: &str| SourceKey {
            source: source.to_string(),
            place: place.to_string(),
        };
        let held = |source: &str, place: &str, holder: &str, ledger_entries| HeldSource {
            group: "g".to_string(),
            key: key(source, place),
            holder: holder.to_string(),
            ledger_entries,
        };
        let holds = [
            held("logs", "s3://b/new", "w1", 42),
            held("logs", "s3://b/old", "w0", 7),
            held("moved", "s3://b/old", "w2", 3),
            held("tables", "sftp://h/t", "w3", 0),
        ];
        let reading_now: BTreeMap<_, _> = [
            (("g".to_string(), key("logs", "s3://b/new")), Some(1_000)),
            (("g".to_string(), key("moved", "s3://b/new")), Some(1_000)),
            (("g".to_string(), key("tables", "sftp://h/t")), None),
        ]
        .into();
        let warnings = [Warning {
            scope: None,
            condition: Condition::SourceBlocked,
            subject_kind: SubjectKind::Source,
            site: None,
            subject_id: "unheld".to_string(),
            group: Some("g".to_string()),
            severity: Severity::Warning,
            since_millis: None,
            message: String::new(),
        }];

        let holding = Holding {
            held: &holds,
            reading_now: &reading_now,
        };
        let answer = build(&warnings, None, holding, live(&[]), warnings::now_millis());
        let logs = reading(&answer, NodeKind::Source, "logs");
        assert_eq!((logs.holder.as_deref(), logs.ledger_entries), (Some("w1"), Some(42)));
        assert_eq!(logs.holder_heard_millis, Some(1_000));
        let tables = reading(&answer, NodeKind::Source, "tables");
        assert_eq!((tables.holder.as_deref(), tables.ledger_entries), (Some("w3"), Some(0)));
        assert_eq!(tables.holder_heard_millis, None, "not heard since the leader started");
        assert!(answer.nodes.iter().all(|n| n.id != "moved"), "{:?}", answer.nodes);
        let unheld = reading(&answer, NodeKind::Source, "unheld");
        assert_eq!((&unheld.holder, unheld.ledger_entries), (&None, None));
        let json = serde_json::to_value(logs).unwrap();
        assert_eq!(
            (json["holder"].clone(), json["ledgerEntries"].clone()),
            ("w1".into(), 42.into())
        );
        assert!(serde_json::to_value(unheld).unwrap().get("holder").is_none());
    }

    /// A hold its holder was answered with is current, dated by when the holder was heard. A holder not heard since
    /// the leader started is taken at its word only where its source has one hold in the group.
    #[test]
    fn a_source_s_normal_rides_on_its_node_in_rows_per_second() {
        let reading = throughput::Reading {
            group: "g".to_string(),
            source: "s".to_string(),
            rows: 0.0,
            ended_ms: 0,
            band: throughput::Band {
                low: 1800.0,
                high: 3600.0,
            },
        };
        let answer = build(&[], None, no_holds(), live(&[]), warnings::now_millis()).with_expected(&[reading]);
        assert_eq!(answer.nodes.len(), 1);
        assert_eq!(
            (answer.nodes[0].kind, answer.nodes[0].id.as_str()),
            (NodeKind::Source, "s")
        );
        assert_eq!(answer.nodes[0].expected, Some(Expected { low: 1.0, high: 2.0 }));
    }

    #[test]
    fn a_hold_is_current_when_answered_or_alone_and_not_yet_heard() {
        let held = |source: &str, place: &str, holder: &str| HeldSource {
            group: "g".to_string(),
            key: SourceKey {
                source: source.to_string(),
                place: place.to_string(),
            },
            holder: holder.to_string(),
            ledger_entries: 0,
        };
        let holds = [
            held("answered", "p", "w1"),
            held("alone", "p", "w2"),
            held("moved", "old", "w3"),
            held("moved", "new", "w3"),
            held("gone", "p", "w4"),
        ];
        let current = reading_now(&holds, |h| match h.holder.as_str() {
            "w1" => Some(Seen::Answered(std::time::Duration::from_secs(5))),
            "w2" | "w3" => Some(Seen::Unheard),
            _ => None,
        });
        let sources: Vec<&str> = current.keys().map(|(_, k)| k.source.as_str()).collect();
        assert_eq!(sources, ["alone", "answered"]);
        let answered = current.iter().find(|((_, k), _)| k.source == "answered").unwrap().1;
        let ago = warnings::now_millis() - answered.expect("heard");
        assert!((5_000..6_000).contains(&ago), "{ago}");
        assert_eq!(current.iter().find(|((_, k), _)| k.source == "alone").unwrap().1, &None);
    }
}
