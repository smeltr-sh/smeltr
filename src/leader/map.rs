//! The topology map (design doc "The map"): the whole fleet's compiled topology, one band per worker group, painted
//! with what the registry currently knows.
//!
//! `build` compiles every group the branch holds, `shared/` merged into each -- the same merge `load_group_from`
//! does for a real deployment, restated here so one group's problems do not blank the others. A source running in
//! two groups is therefore two nodes, one per group, each marked that it came from `shared/`, never deduplicated
//! into a band of its own: the map draws the COMPILED topology, and a shared source running in two groups is two
//! running things (design doc "One fleet, many groups" -- "a group is a filter on that view rather than a mode that
//! hides the rest"). A group that cannot load reports its own problems in `group_errors` instead of a graph for that
//! group; every group that does compile still draws. "Every group" is every group the caller holds viewer in
//! ([`visible_groups`]): a group they do not hold is not drawn, nor read.
//!
//! A call is never a node or an edge here -- `expand_calls` (`config_file::load`) has already spliced it away before
//! anything compiles, so the compiled topology has no calls to draw. A pipeline nothing routes to directly is, for
//! the same reason, not a node: it is a callee, spliced into whichever pipeline calls it, and its blast radius is
//! `leader::connections`, served separately for its editor.
//!
//! **Color is a departure from the design.** The design paints an edge by how far its throughput is from normal,
//! but learning normal volume was deliberately deferred -- it moves by hour and by day and wants retained history
//! to learn from, which does not exist yet. So a node instead carries whether the warning catalog
//! (`leader::warnings`) names it: the same held-source, refusing-destination conditions the design's "thin red
//! link" wants spotted, without inventing a baseline this leader cannot back up.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use serde::Serialize;

use crate::config_file::{self, LoadError};
use crate::core::error::{Category, ExceptionKind};
use crate::core::id::{DestId, SourceId};
use crate::plan::config::OnException;
use crate::plan::topology::{DestinationConfig, SourceConfig};

use super::access::{Access, Level};
use super::series::{RegistryStore, Tier};
use super::warnings::{self, SubjectKind};
use super::{BATCHES_DLQ_METRIC, subject_label};

/// How many finest-tier buckets back a poisoned batch still draws its edge -- long enough that landing between two
/// polls does not drop it, short enough that a source that has stopped poisoning batches loses the edge.
const POISON_WINDOW_BUCKETS: i64 = 2;

/// The column a node draws in (design doc "The map"), and what a request naming one object says it is
/// (`leader::stats`) -- an author id is unique only within its own kind, so the kind is part of naming the object.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum NodeKind {
    Source,
    Pipeline,
    Destination,
}

/// The system behind a source or destination -- what its glyph on the map says. A pipeline has none.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum System {
    Kafka,
    EventHubs,
    Elasticsearch,
    SplunkHec,
    S3,
    S3Sqs,
    AzureBlobQueue,
    GcsPubSub,
    Gcs,
    AzureBlob,
    Sftp,
    Http,
    Postgres,
    #[serde(rename = "mysql")]
    MySql,
    Metrics,
    Logs,
    Iceberg,
    Delta,
}

impl From<&crate::plan::topology::SourceKind> for System {
    fn from(kind: &crate::plan::topology::SourceKind) -> Self {
        use crate::plan::topology::SourceKind;
        match kind {
            SourceKind::Kafka(k) => match k.service {
                crate::plan::topology::KafkaService::Kafka => System::Kafka,
                crate::plan::topology::KafkaService::EventHubs => System::EventHubs,
            },
            SourceKind::Metrics(_) => System::Metrics,
            SourceKind::Logs(_) => System::Logs,
            SourceKind::Objects(o) => match o.queue {
                crate::plan::topology::ObjectQueue::Sqs { .. } => System::S3Sqs,
                crate::plan::topology::ObjectQueue::List { ref place, .. } => match place {
                    crate::plan::topology::ListPlace::S3 { .. } => System::S3,
                    crate::plan::topology::ListPlace::Gcs(_) => System::Gcs,
                    crate::plan::topology::ListPlace::Azure(_) => System::AzureBlob,
                    crate::plan::topology::ListPlace::Sftp(_) => System::Sftp,
                },
                crate::plan::topology::ObjectQueue::StorageQueue { .. } => System::AzureBlobQueue,
                crate::plan::topology::ObjectQueue::PubSub { .. } => System::GcsPubSub,
            },
            SourceKind::Api(_) | SourceKind::Receiver(_) => System::Http,
            SourceKind::Sql(s) => match s.database {
                crate::plan::topology::SqlDatabase::Postgres(_) => System::Postgres,
                crate::plan::topology::SqlDatabase::MySql(_) => System::MySql,
            },
        }
    }
}

impl From<&crate::plan::topology::DestinationKind> for System {
    fn from(kind: &crate::plan::topology::DestinationKind) -> Self {
        use crate::plan::topology::DestinationKind;
        match kind {
            DestinationKind::Kafka(k) => match k.service {
                crate::plan::topology::KafkaService::Kafka => System::Kafka,
                crate::plan::topology::KafkaService::EventHubs => System::EventHubs,
            },
            DestinationKind::Elasticsearch(_) => System::Elasticsearch,
            DestinationKind::SplunkHec(_) => System::SplunkHec,
            DestinationKind::Objects(o) => match o.cloud {
                crate::plan::topology::Cloud::S3(_) => System::S3,
                crate::plan::topology::Cloud::Gcs(_) => System::Gcs,
                crate::plan::topology::Cloud::Azure(_) => System::AzureBlob,
                crate::plan::topology::Cloud::Sftp(_) => System::Sftp,
            },
            DestinationKind::Http(_) | DestinationKind::Prometheus(_) | DestinationKind::LogAnalytics(_) => {
                System::Http
            }
            DestinationKind::Iceberg(_) => System::Iceberg,
            DestinationKind::Delta(_) => System::Delta,
        }
    }
}

/// One source, pipeline or destination, compiled inside one worker group. `author_id` and `name` coincide for a
/// source or destination -- neither has a display name apart from the id its author gave it
/// (`config_file::load::to_source`'s doc); a pipeline's differ.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MapNode {
    /// Which group this instance runs in. An author id is unique only within one group plus `shared/`, never across
    /// the fleet, so this is part of the node's identity, not a label on top of it.
    pub group: String,
    pub author_id: String,
    pub name: String,
    pub kind: NodeKind,
    /// The system behind a source or destination; `None` for a pipeline.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub system: Option<System>,
    /// Declared in `shared/` rather than in this group's own subtree -- still one running thing per group that
    /// merges it in, just not authored here.
    pub shared: bool,
    /// The warning catalog names this object in this group. Always `false` for a pipeline: no catalog entry has
    /// one as its subject.
    pub warning: bool,
}

/// A source→pipeline edge: one `RouteConfig`, inside one group.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RouteEdge {
    pub group: String,
    /// `RouteConfig::id`.
    pub route: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub condition: Option<String>,
    pub source: String,
    pub pipeline: String,
}

/// A pipeline→destination edge: one destination out of one `DeliverRule` on one route, inside one group. A rule
/// naming several destinations is several edges, since a graph edge has one destination; all share the rule's own
/// `when`.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DeliverEdge {
    pub group: String,
    /// The route this rule belongs to -- a rule has no id of its own.
    pub route: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub when: Option<String>,
    pub pipeline: String,
    pub destination: String,
}

/// A source→destination edge: one of the deployment's unmatched destinations, for a source a row can fall through
/// (`TopologyConfig::falls_through`). No route and no pipeline sit on this path -- the row arrives as it was read.
///
/// Drawn from configuration: an unmatched row is an ongoing answer, so the path exists whether or not anything
/// is on it right now.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UnmatchedEdge {
    pub group: String,
    pub source: String,
    pub destination: String,
}

/// A node→destination edge: an exception answer delivering there. From a pipeline, for the kinds its route answers
/// with deliver; from a destination, for what it refuses.
///
/// Drawn from configuration, like [`UnmatchedEdge`]: the path exists whether or not anything is on it right now.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExceptionEdge {
    pub group: String,
    /// `pipeline` or `destination`: what kind of node the edge leaves.
    pub from_kind: &'static str,
    pub from: String,
    /// The route whose answer this is; absent for a destination's.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub route: Option<String>,
    pub destination: String,
    /// The kinds answered with deliver to `destination`, in `ExceptionKind::KINDS` order.
    pub kinds: Vec<&'static str>,
}

/// A source→destination edge: a batch nothing could process, and a destination the deployment's `unprocessable`
/// answer sent it to.
///
/// Drawn from traffic, unlike [`UnmatchedEdge`], and from traffic at both ends: every source can panic on a batch,
/// so configuration says "all of them", and every source in a deployment lined up against one node says nothing.
/// A poisoned batch is an event, and this edge says it happened -- to the destination that was running when it did,
/// which a branch edited since need not still name.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PoisonEdge {
    pub group: String,
    pub source: String,
    pub destination: String,
    /// Batches, not rows: nothing processed a poisoned batch, so it has none to count. Always present -- a reading
    /// is what draws this edge.
    pub throughput: f64,
}

/// Why one group has no graph: it names the group and every problem `validate_at` or an unreadable file raised for
/// it, the same wording an all-in-one-scope bundle would have raised on its own (`config_file` load errors).
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupError {
    pub group: String,
    pub errors: Vec<String>,
}

/// The fleet's map. Its envelope is not `ok` only for a problem that stopped every group from being read at all --
/// the repository, the branch, or `shared/` itself; one group failing to compile does not clear `nodes`, it is
/// reported in `group_errors` while every other group still draws (module doc).
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Map {
    pub nodes: Vec<MapNode>,
    pub routes: Vec<RouteEdge>,
    pub delivers: Vec<DeliverEdge>,
    pub unmatched: Vec<UnmatchedEdge>,
    pub exceptions: Vec<ExceptionEdge>,
    pub poison: Vec<PoisonEdge>,
    /// One entry per group that did not compile. Empty when every group the branch holds drew.
    pub group_errors: Vec<GroupError>,
    /// The commit this was compiled from. `measurements::Measurements::commit` moving away from it is what says the
    /// topology has changed since.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub commit: Option<String>,
}

/// Recently poisoned batches, keyed by (group, source, destination): a poison edge is drawn from this alone, so the
/// batch's own destination has to come with it ([`latest_by_group_pair`]).
type Poisoned = HashMap<(String, String, String), f64>;

/// The fleet's graph, accumulated one group at a time by [`build_group_graph`].
#[derive(Default)]
struct Graph {
    nodes: Vec<MapNode>,
    routes: Vec<RouteEdge>,
    delivers: Vec<DeliverEdge>,
    unmatched: Vec<UnmatchedEdge>,
    exceptions: Vec<ExceptionEdge>,
    poison: Vec<PoisonEdge>,
}

/// One worker group's own bundle, as `config_api::Reading` reads it: `id` is the group, `raw` its own subtree
/// (never merged with `shared/` -- `build` does that), `at` is where it was read from, for a relative lookup path.
pub struct GroupBundle {
    pub id: String,
    pub raw: config_file::RawBundle,
    pub at: config_file::BundleAt,
}

/// Of `groups`, the ones `access` holds viewer in, in the order given: what the map reads and draws for them.
pub fn visible_groups(groups: Vec<String>, access: &Access) -> Vec<String> {
    groups
        .into_iter()
        .filter(|group| access.allows(group, Level::Viewer))
        .collect()
}

/// The fleet's map for one branch: `shared` is that branch's `shared/` bundle exactly as `config_api::Reading::raw`
/// reads it, `shared_at` is where it was read from; `groups` is every worker group the branch holds
/// (`leader::tree::groups_at`), each with its own subtree read the same way. `warnings` is
/// `leader::warnings_snapshot`'s answer for the same moment, and `series_store` is the registry's durable series,
/// when this leader has one (`Registry::series`).
pub fn build(
    shared: config_file::RawBundle,
    shared_at: &config_file::BundleAt,
    groups: Vec<GroupBundle>,
    warnings: &[warnings::Warning],
    series_store: Option<&RegistryStore>,
) -> Map {
    let shared_unreadable: Vec<String> = shared
        .unreadable
        .iter()
        .map(|(path, message)| format!("{}/{path}: {message}", config_file::SHARED_DIR))
        .collect();
    let mut graph = Graph::default();
    let mut group_errors = Vec::new();

    for group in groups {
        let mut errors = shared_unreadable.clone();
        errors.extend(
            group
                .raw
                .unreadable
                .iter()
                .map(|(path, message)| format!("{}/{}/{path}: {message}", config_file::GROUPS_DIR, group.id)),
        );

        let file = merged_file(&shared, &group.raw, &group.id);
        let declared_at = merged_declared_at(&shared, shared_at, &group.raw, &group.at);

        match config_file::validate_at(file, &declared_at) {
            Ok(system) if errors.is_empty() => build_group_graph(&system, &group.id, &shared.file, &mut graph),
            Ok(_) => group_errors.push(GroupError {
                group: group.id,
                errors,
            }),
            Err(e) => {
                errors.extend(match e {
                    LoadError::Validation(errs) => errs.into_iter().map(|e| e.to_string()).collect(),
                    other => vec![other.to_string()],
                });
                group_errors.push(GroupError {
                    group: group.id,
                    errors,
                });
            }
        }
    }

    mark_warnings(&mut graph.nodes, warnings);
    if let Some(store) = series_store {
        let since_ms = warnings::now_millis() - POISON_WINDOW_BUCKETS * Tier::TenSeconds.bucket_ms();
        draw_poison(&mut graph, &latest_by_group_pair(store, BATCHES_DLQ_METRIC, since_ms));
    }

    Map {
        nodes: graph.nodes,
        routes: graph.routes,
        delivers: graph.delivers,
        unmatched: graph.unmatched,
        exceptions: graph.exceptions,
        poison: graph.poison,
        group_errors,
        commit: None,
    }
}

/// `shared`'s bundle merged with one group's own: shared's objects first, then the group's own, the order
/// `load_group_from` merges in. The group's own manifest (version, unmatched, unprocessable, name) is kept as-is: `shared/` has none of its own to contribute (`load_group_dir`'s doc, "system.json is the group's alone").
/// A Kafka source with no group id reads as `group_id`'s own (`config_file::fill_default_group_ids`).
pub fn merged_file(
    shared: &config_file::RawBundle,
    group: &config_file::RawBundle,
    group_id: &str,
) -> config_file::FileConfig {
    let mut file = group.file.clone();
    file.sources = cat(&shared.file.sources, &group.file.sources);
    config_file::fill_default_group_ids(&mut file.sources, group_id);
    file.destinations = cat(&shared.file.destinations, &group.file.destinations);
    file.routes = cat(&shared.file.routes, &group.file.routes);
    file.lookup_tables = cat(&shared.file.lookup_tables, &group.file.lookup_tables);
    file.grok_patterns = cat(&shared.file.grok_patterns, &group.file.grok_patterns);
    file.schemas = cat(&shared.file.schemas, &group.file.schemas);
    file.pipelines = cat(&shared.file.pipelines, &group.file.pipelines);
    file
}

fn cat<T: Clone>(a: &[T], b: &[T]) -> Vec<T> {
    let mut v = Vec::with_capacity(a.len() + b.len());
    v.extend_from_slice(a);
    v.extend_from_slice(b);
    v
}

/// Every lookup table declared in either half, mapped to where it was actually declared -- a shared table resolves
/// a relative path against `shared/`, a group's own against its own subtree, the same split `load_group_from`'s
/// `declared_in` keeps.
pub fn merged_declared_at(
    shared: &config_file::RawBundle,
    shared_at: &config_file::BundleAt,
    group: &config_file::RawBundle,
    group_at: &config_file::BundleAt,
) -> config_file::DeclaredAt {
    let mut declared_at: config_file::DeclaredAt = shared
        .file
        .lookup_tables
        .iter()
        .map(|t| (t.id.clone(), shared_at.clone()))
        .collect();
    declared_at.extend(
        group
            .file
            .lookup_tables
            .iter()
            .map(|t| (t.id.clone(), group_at.clone())),
    );
    declared_at
}

/// Flags each node the catalog currently names in its group. A warning's subject is the id its author typed, not the
/// positional one the compiled topology numbers nodes by, and it is unique only together with the group
/// (`registry::subject_key`'s own doc: "an id unique only within a group is not fleet-unique") -- so both have to
/// match.
fn mark_warnings(nodes: &mut [MapNode], warnings: &[warnings::Warning]) {
    for node in nodes {
        let kind = match node.kind {
            NodeKind::Source => SubjectKind::Source,
            NodeKind::Pipeline => SubjectKind::Pipeline,
            NodeKind::Destination => SubjectKind::Destination,
        };
        node.warning = warnings.iter().any(|w| {
            w.subject_kind == kind && w.subject_id == node.author_id && w.group.as_deref() == Some(&node.group)
        });
    }
}

/// One group's nodes and edges as its config declares them, appended onto the fleet's. `shared` is the `shared/`
/// bundle the group merges in, which says which of its nodes are declared there.
fn build_group_graph(
    system: &config_file::LoadedSystem,
    group: &str,
    shared: &config_file::FileConfig,
    graph: &mut Graph,
) {
    let topo = &system.topology;
    let pipeline_author = |id: usize| -> String {
        system
            .pipeline_ids
            .get(id)
            .cloned()
            .expect("a validated system's pipeline ids are assigned 0..len in file order")
    };

    for s in &topo.sources {
        graph.nodes.push(MapNode {
            group: group.to_string(),
            author_id: s.name.clone(),
            name: s.name.clone(),
            kind: NodeKind::Source,
            system: Some(System::from(&s.kind)),
            shared: shared.sources.iter().any(|shared| shared.id == s.name),
            warning: false,
        });
    }
    for d in &topo.destinations {
        graph.nodes.push(MapNode {
            group: group.to_string(),
            author_id: d.name.clone(),
            name: d.name.clone(),
            kind: NodeKind::Destination,
            system: Some(System::from(&d.kind)),
            shared: shared.destinations.iter().any(|shared| shared.id == d.name),
            warning: false,
        });
    }
    // A pipeline is a node only when a route names it directly (module doc): a callee spliced in by `expand_calls`
    // has no route of its own to draw one from.
    let routed_pipelines: BTreeSet<usize> = topo.routes.iter().map(|r| r.pipeline).collect();
    for id in routed_pipelines {
        let pipeline = system
            .pipelines
            .iter()
            .find(|p| p.id == id)
            .expect("a validated system's routes only name pipelines that exist");
        let author_id = pipeline_author(id);
        graph.nodes.push(MapNode {
            group: group.to_string(),
            shared: shared.pipelines.iter().any(|shared| shared.id == author_id),
            author_id,
            name: pipeline.name.clone(),
            kind: NodeKind::Pipeline,
            system: None,
            warning: false,
        });
    }

    let sources_by_id: HashMap<SourceId, &SourceConfig> = topo.sources.iter().map(|s| (s.id, s)).collect();
    let destinations_by_id: HashMap<DestId, &DestinationConfig> = topo.destinations.iter().map(|d| (d.id, d)).collect();

    for r in &topo.routes {
        let source = sources_by_id
            .get(&r.source)
            .expect("a validated system's routes only name sources that exist");
        let pipeline = pipeline_author(r.pipeline);

        graph.routes.push(RouteEdge {
            group: group.to_string(),
            route: r.id.clone(),
            condition: r.cond.clone(),
            source: source.name.clone(),
            pipeline: pipeline.clone(),
        });

        for rule in &r.deliver {
            for dest_id in &rule.to {
                let destination = destinations_by_id
                    .get(dest_id)
                    .expect("a validated system's deliver rules only name destinations that exist");
                graph.delivers.push(DeliverEdge {
                    group: group.to_string(),
                    route: r.id.clone(),
                    when: rule.when.clone(),
                    pipeline: pipeline.clone(),
                    destination: destination.name.clone(),
                });
            }
        }
        // Of the delivery kinds a route answers only `Unmatched`, for the rows its rules decline: the others its
        // answers hold are the group's, drawn below.
        for (to, kinds) in delivered_by(&r.on_exception, |kind, category| {
            category != Category::Delivery || kind == "Unmatched"
        }) {
            graph.exceptions.push(ExceptionEdge {
                group: group.to_string(),
                from_kind: "pipeline",
                from: pipeline.clone(),
                route: Some(r.id.clone()),
                destination: destinations_by_id[&to].name.clone(),
                kinds,
            });
        }
    }

    for t in topo.on_exception.answer(&ExceptionKind::Unmatched).targets() {
        let destination = destinations_by_id
            .get(t)
            .expect("a validated system's answers name destinations that exist");
        for s in topo.sources.iter().filter(|s| topo.falls_through(s.id)) {
            graph.unmatched.push(UnmatchedEdge {
                group: group.to_string(),
                source: s.name.clone(),
                destination: destination.name.clone(),
            });
        }
    }

    for d in &topo.destinations {
        for t in d.on_reject.targets() {
            graph.exceptions.push(ExceptionEdge {
                group: group.to_string(),
                from_kind: "destination",
                from: d.name.clone(),
                route: None,
                destination: destinations_by_id[t].name.clone(),
                kinds: vec!["Refused"],
            });
        }
    }
}

/// The poison edges: each batch diverted recently, from its source to its destination. Both ends come from the series,
/// never from the answer to `Unprocessable`: a batch was diverted to the destination the worker was running, which a
/// branch since edited need not still name. An end its group did not draw has no node to draw to, so that edge is
/// passed over rather than drawn to nothing.
fn draw_poison(graph: &mut Graph, poisoned: &Poisoned) {
    let drawn: HashSet<(&str, &str)> = graph
        .nodes
        .iter()
        .filter(|n| n.kind != NodeKind::Pipeline)
        .map(|n| (n.group.as_str(), n.name.as_str()))
        .collect();
    let mut poison: Vec<PoisonEdge> = poisoned
        .iter()
        .filter(|((group, source, destination), _)| {
            drawn.contains(&(group.as_str(), source.as_str()))
                && drawn.contains(&(group.as_str(), destination.as_str()))
        })
        .map(|((group, source, destination), batches)| PoisonEdge {
            group: group.clone(),
            source: source.clone(),
            destination: destination.clone(),
            throughput: *batches,
        })
        .collect();
    // The map is served from a `HashMap`, so an order has to be imposed for one branch to draw the same picture twice.
    poison.sort_by(|a, b| (&a.group, &a.source, &a.destination).cmp(&(&b.group, &b.source, &b.destination)));
    graph.poison = poison;
}

/// The most recent value of `metric`'s series for each (group, source, destination), among points at or after
/// `since_ms`. `registry::subject_key` folds the reporting worker's group into every series, since an id is unique
/// only together with the group running it. A subject with nothing that recent is absent, not zero. A sample missing
/// either end is passed over -- an edge needs two of them.
fn latest_by_group_pair(store: &RegistryStore, metric: &str, since_ms: i64) -> HashMap<(String, String, String), f64> {
    let series = match store.series_since(metric, Tier::TenSeconds, since_ms) {
        Ok(series) => series,
        Err(e) => {
            tracing::warn!(
                target: "map.metric_unreadable",
                subject = %metric,
                error = %e,
                "could not read a metric for the topology map, so it draws no poison edges"
            );
            return HashMap::new();
        }
    };
    let mut latest = HashMap::new();
    for (subject_key, points) in series {
        if let Some((key, value)) = (|| {
            let group = subject_label(&subject_key, "group")?;
            let source = subject_label(&subject_key, "source")?;
            let destination = subject_label(&subject_key, "destination")?;
            let &(_, value) = points.last()?;
            Some(((group, source, destination), value))
        })() {
            *latest.entry(key).or_insert(0.0) += value;
        }
    }
    latest
}

/// Each destination `on_exception` delivers to, with the kinds of the categories `keep` takes that it delivers there,
/// in destination order.
fn delivered_by(
    on_exception: &OnException,
    keep: impl Fn(&str, Category) -> bool,
) -> BTreeMap<DestId, Vec<&'static str>> {
    let mut by: BTreeMap<DestId, Vec<&'static str>> = BTreeMap::new();
    for ((kind, category), answer) in ExceptionKind::KINDS.iter().zip(&on_exception.answers) {
        if keep(kind, *category) {
            for t in answer.targets() {
                let kinds = by.entry(*t).or_default();
                if !kinds.contains(kind) {
                    kinds.push(kind);
                }
            }
        }
    }
    by
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config_file::{
        CURRENT_VERSION, DeliverRuleDto, DestinationDto, DestinationKindDto, FileConfig, PipelineDto, RawBundle,
        RouteDto, SourceDto, SourceKindDto, StepDto,
    };

    fn source(id: &str, topic: &str) -> SourceDto {
        SourceDto {
            id: id.to_string(),
            kind: SourceKindDto::Kafka {
                schema_registry: None,
                read: Default::default(),
                brokers: "kafka:9092".to_string(),
                topics: vec![topic.to_string()],
                group_id: format!("g-{id}"),
                tls: Default::default(),
                start: Default::default(),
                stop: None,
                fetch_bytes: Default::default(),
                encoding: Default::default(),
                props: Default::default(),
            },
        }
    }

    fn destination(id: &str) -> DestinationDto {
        DestinationDto {
            id: id.to_string(),
            on_exception: None,
            kind: DestinationKindDto::Kafka {
                schema_registry: None,
                brokers: "kafka:9092".to_string(),
                default_topic: "out".to_string(),
                tls: Default::default(),
                format: Default::default(),
                field: None,
                compression: Default::default(),
                acks: Default::default(),
                stall_after_secs: Default::default(),
                buffer_bytes: Default::default(),
                props: Default::default(),
            },
            accepts: vec![],
        }
    }

    fn pipeline(id: &str, name: &str, steps: Vec<StepDto>) -> PipelineDto {
        PipelineDto {
            id: id.to_string(),
            name: name.to_string(),
            steps,
            produces: vec![],
        }
    }

    fn route(id: &str, from: &str, through: &str, to: &[&str]) -> RouteDto {
        RouteDto {
            id: id.to_string(),
            source: from.to_string(),
            condition: Some("true".to_string()),
            pipeline: through.to_string(),
            deliver: vec![DeliverRuleDto {
                when: Some("true".to_string()),
                to: to.iter().map(|d| d.to_string()).collect(),
            }],
            on_exception: None,
        }
    }

    /// `route`, with its condition taken off: it then matches every row, so nothing on its source falls through.
    fn unconditional(id: &str, from: &str, through: &str, to: &[&str]) -> RouteDto {
        RouteDto {
            condition: None,
            ..route(id, from, through, to)
        }
    }

    fn call(target: &str) -> StepDto {
        StepDto::Call {
            id: format!("call-{target}"),
            filter: None,
            disabled: false,
            pipeline: target.to_string(),
        }
    }

    fn config(
        sources: Vec<SourceDto>,
        destinations: Vec<DestinationDto>,
        routes: Vec<RouteDto>,
        pipelines: Vec<PipelineDto>,
    ) -> FileConfig {
        FileConfig {
            version: CURRENT_VERSION,
            sources,
            destinations,
            pipelines,
            routes,
            lookup_tables: vec![],
            grok_patterns: vec![],
            on_exception: None,
            name: None,
            schemas: vec![],
        }
    }

    fn raw(file: FileConfig) -> RawBundle {
        RawBundle {
            file,
            unreadable: Vec::new(),
        }
    }

    fn empty_shared() -> (RawBundle, config_file::BundleAt) {
        (
            raw(config(vec![], vec![], vec![], vec![])),
            config_file::BundleAt::Dir(std::env::temp_dir()),
        )
    }

    fn group_bundle(id: &str, file: FileConfig) -> GroupBundle {
        GroupBundle {
            id: id.to_string(),
            raw: raw(file),
            at: config_file::BundleAt::Dir(std::env::temp_dir()),
        }
    }

    /// `build` with an empty `shared/` and no warnings or series -- what most of these tests only need one group
    /// (or a small few) to exercise.
    fn build_fleet(groups: Vec<GroupBundle>) -> Map {
        let (shared, shared_at) = empty_shared();
        build(shared, &shared_at, groups, &[], None)
    }

    #[test]
    fn a_delta_destination_is_drawn_as_delta() {
        let delta: DestinationDto =
            serde_json::from_value(serde_json::json!({"id": "lake", "type": "delta", "location": "s3://lake/web"}))
                .unwrap();
        let file = config(
            vec![source("orders-west", "orders")],
            vec![delta],
            vec![route("r1", "orders-west", "orders-normalize", &["lake"])],
            vec![pipeline("orders-normalize", "Normalize orders", vec![])],
        );
        let map = build_fleet(vec![group_bundle("ingest-1", file)]);

        let lake = map.nodes.iter().find(|n| n.author_id == "lake").unwrap();
        assert_eq!(lake.system, Some(System::Delta));
        assert_eq!(serde_json::to_value(System::Delta).unwrap(), "delta");
    }

    /// A small bundle's edges, checked by the author ids on their two ends -- not by any positional id, since that
    /// is exactly what the map must not leak.
    #[test]
    fn the_edge_list_for_a_small_bundle_is_keyed_by_author_ids() {
        let file = config(
            vec![source("orders-west", "orders")],
            vec![destination("es-prod")],
            vec![route("r1", "orders-west", "orders-normalize", &["es-prod"])],
            vec![pipeline("orders-normalize", "Normalize orders", vec![])],
        );
        let map = build_fleet(vec![group_bundle("ingest-1", file)]);

        assert_eq!(map.nodes.len(), 3);
        assert!(map.nodes.contains(&MapNode {
            group: "ingest-1".to_string(),
            author_id: "orders-west".to_string(),
            name: "orders-west".to_string(),
            kind: NodeKind::Source,
            system: Some(System::Kafka),
            shared: false,
            warning: false,
        }));
        assert!(map.nodes.contains(&MapNode {
            group: "ingest-1".to_string(),
            author_id: "orders-normalize".to_string(),
            name: "Normalize orders".to_string(),
            kind: NodeKind::Pipeline,
            system: None,
            shared: false,
            warning: false,
        }));
        assert!(map.nodes.contains(&MapNode {
            group: "ingest-1".to_string(),
            author_id: "es-prod".to_string(),
            name: "es-prod".to_string(),
            kind: NodeKind::Destination,
            system: Some(System::Kafka),
            shared: false,
            warning: false,
        }));

        assert_eq!(map.routes.len(), 1);
        let edge = &map.routes[0];
        assert_eq!(edge.group, "ingest-1");
        assert_eq!(edge.route, "r1");
        assert_eq!(edge.condition.as_deref(), Some("true"));
        assert_eq!(edge.source, "orders-west");
        assert_eq!(edge.pipeline, "orders-normalize");

        assert_eq!(map.delivers.len(), 1);
        let deliver = &map.delivers[0];
        assert_eq!(deliver.group, "ingest-1");
        assert_eq!(deliver.route, "r1");
        assert_eq!(deliver.when.as_deref(), Some("true"));
        assert_eq!(deliver.pipeline, "orders-normalize");
        assert_eq!(deliver.destination, "es-prod");
    }

    /// A group that cannot load draws no graph for itself: the errors it would refuse to deploy with, not an empty
    /// topology that looks like a group with nothing in it. It is still reported by name.
    #[test]
    fn a_group_that_does_not_compile_is_answered_with_its_problems() {
        let file = config(
            vec![source("orders-west", "orders")],
            vec![],
            vec![RouteDto {
                condition: Some("not a valid expression (".to_string()),
                ..route("r1", "orders-west", "orders-normalize", &[])
            }],
            vec![pipeline("orders-normalize", "Normalize orders", vec![])],
        );
        let map = build_fleet(vec![group_bundle("ingest-1", file)]);

        assert!(map.nodes.is_empty());
        assert!(map.routes.is_empty());
        assert!(map.delivers.is_empty());
        assert_eq!(map.group_errors.len(), 1);
        assert_eq!(map.group_errors[0].group, "ingest-1");
        assert!(!map.group_errors[0].errors.is_empty());
    }

    /// A route naming a pipeline that does not exist is exactly this: there is no topology to draw for that group
    /// until the dangling reference is fixed.
    #[test]
    fn a_route_to_a_missing_pipeline_is_a_problem_not_a_graph() {
        let file = config(
            vec![source("orders-west", "orders")],
            vec![],
            vec![route("r1", "orders-west", "does-not-exist", &[])],
            vec![],
        );
        let map = build_fleet(vec![group_bundle("ingest-1", file)]);

        assert!(map.nodes.is_empty());
        assert_eq!(map.group_errors.len(), 1);
        assert!(
            map.group_errors[0].errors.iter().any(|e| e.contains("does-not-exist")),
            "{:?}",
            map.group_errors
        );
    }

    /// A callee spliced into its caller is not on the map at all: not as a node, and not as any edge naming it --
    /// the compiled topology has no calls, so there is nothing here that would draw one.
    #[test]
    fn no_call_appears_anywhere_in_the_maps_output() {
        let file = config(
            vec![source("orders-west", "orders")],
            vec![destination("es-prod")],
            vec![route("r1", "orders-west", "orders-normalize", &["es-prod"])],
            vec![
                pipeline("orders-normalize", "Normalize orders", vec![call("enrich-geo")]),
                pipeline("enrich-geo", "Enrich geo", vec![]),
            ],
        );
        let map = build_fleet(vec![group_bundle("ingest-1", file)]);

        assert_eq!(map.nodes.len(), 3, "enrich-geo is not a fourth node");
        assert!(map.nodes.iter().all(|n| n.author_id != "enrich-geo"), "{:?}", map.nodes);
        assert!(map.routes.iter().all(|r| r.pipeline != "enrich-geo"));
        assert!(map.delivers.iter().all(|d| d.pipeline != "enrich-geo"));
    }

    /// Two groups each holding a source of the same author id are two nodes: an id is unique only within one group
    /// plus `shared/`, never across the fleet.
    #[test]
    fn two_groups_with_a_source_of_the_same_id_are_two_nodes() {
        let one = config(vec![source("orders-bad", "orders")], vec![], vec![], vec![]);
        let other = config(vec![source("orders-bad", "orders")], vec![], vec![], vec![]);
        let map = build_fleet(vec![group_bundle("ingest-1", one), group_bundle("ingest-2", other)]);

        assert_eq!(map.nodes.len(), 2, "one per group, not deduplicated");
        assert!(map.nodes.iter().all(|n| n.author_id == "orders-bad"));
        let groups: BTreeSet<&str> = map.nodes.iter().map(|n| n.group.as_str()).collect();
        assert_eq!(groups, BTreeSet::from(["ingest-1", "ingest-2"]));
    }

    /// A viewer of one group is drawn that group and none of the others.
    #[test]
    fn a_viewer_is_drawn_only_the_groups_they_hold() {
        let payments = Access::Areas(BTreeMap::from([("payments".to_string(), Level::Viewer)]));
        let held = visible_groups(vec!["payments".to_string(), "search".to_string()], &payments);
        assert_eq!(held, vec!["payments".to_string()]);

        let file = || config(vec![source("orders", "orders")], vec![], vec![], vec![]);
        let map = build_fleet(held.iter().map(|id| group_bundle(id, file())).collect());
        assert!(!map.nodes.is_empty());
        assert!(map.nodes.iter().all(|n| n.group == "payments"), "{:?}", map.nodes);

        let everyone = visible_groups(
            vec!["payments".to_string(), "search".to_string()],
            &Access::Administrator,
        );
        assert_eq!(everyone.len(), 2);
    }

    /// One group failing to compile does not blank the fleet: every group that does compile still draws, and the
    /// broken one is reported by name.
    #[test]
    fn a_group_that_does_not_compile_is_reported_while_others_still_draw() {
        let healthy = config(vec![source("orders-west", "orders")], vec![], vec![], vec![]);
        let broken = config(
            vec![source("orders-east", "orders")],
            vec![],
            vec![route("r1", "orders-east", "does-not-exist", &[])],
            vec![],
        );
        let map = build_fleet(vec![
            group_bundle("ingest-1", healthy),
            group_bundle("ingest-2", broken),
        ]);

        assert_eq!(map.nodes.len(), 1);
        assert_eq!(map.nodes[0].group, "ingest-1");
        assert_eq!(map.nodes[0].author_id, "orders-west");
        assert_eq!(map.group_errors.len(), 1);
        assert_eq!(map.group_errors[0].group, "ingest-2");
        assert!(!map.group_errors[0].errors.is_empty());
    }

    /// A source declared in `shared/` is drawn inside every group that runs it, each its own node marked `shared`
    /// -- not merged into one band of its own.
    #[test]
    fn a_shared_object_appears_in_each_group_that_runs_it() {
        let shared = raw(config(vec![source("orders-west", "orders")], vec![], vec![], vec![]));
        let shared_at = config_file::BundleAt::Dir(std::env::temp_dir());
        let map = build(
            shared,
            &shared_at,
            vec![
                group_bundle("ingest-1", config(vec![], vec![], vec![], vec![])),
                group_bundle("ingest-2", config(vec![], vec![], vec![], vec![])),
            ],
            &[],
            None,
        );

        assert_eq!(map.nodes.len(), 2);
        for n in &map.nodes {
            assert_eq!(n.author_id, "orders-west");
            assert!(n.shared, "it came from shared/, not from this group's own subtree");
        }
        let groups: BTreeSet<&str> = map.nodes.iter().map(|n| n.group.as_str()).collect();
        assert_eq!(groups, BTreeSet::from(["ingest-1", "ingest-2"]));

        // A source the group declares itself is not marked shared.
        let own = build_fleet(vec![group_bundle(
            "ingest-1",
            config(vec![source("orders-west", "orders")], vec![], vec![], vec![]),
        )]);
        assert!(!own.nodes[0].shared);
    }

    /// A warning names one group's instance of a source; the same author id running in another group is unaffected.
    #[test]
    fn a_warning_paints_only_the_node_in_the_group_it_names() {
        let held = warnings::Warning {
            area: None,
            condition: warnings::Condition::SourceBlocked,
            subject_kind: SubjectKind::Source,
            site: None,
            subject_id: "orders-west".to_string(),
            group: Some("ingest-1".to_string()),
            severity: warnings::Severity::Failed,
            since_millis: None,
            message: "held".to_string(),
        };
        let (shared, shared_at) = empty_shared();
        let map = build(
            shared,
            &shared_at,
            vec![
                group_bundle(
                    "ingest-1",
                    config(vec![source("orders-west", "orders")], vec![], vec![], vec![]),
                ),
                group_bundle(
                    "ingest-2",
                    config(vec![source("orders-west", "orders")], vec![], vec![], vec![]),
                ),
            ],
            &[held],
            None,
        );

        let in_1 = map.nodes.iter().find(|n| n.group == "ingest-1").unwrap();
        let in_2 = map.nodes.iter().find(|n| n.group == "ingest-2").unwrap();
        assert!(in_1.warning, "the warning names ingest-1");
        assert!(
            !in_2.warning,
            "the same source id failing in another group is another group's fact"
        );
    }

    /// A route's deliver answers draw from its pipeline, one edge per destination naming every kind sent there; a
    /// destination's answer to Refused draws from the destination. The group's delivery kinds draw from neither.
    #[test]
    fn an_exception_edge_is_drawn_for_every_deliver_answer() {
        let answers = |json: &str| Some(serde_json::from_str(json).unwrap());
        let file = FileConfig {
            on_exception: answers(r#"{"categories":{"math":{"deliver":["dlq"]}},"kinds":{"Unmatched":"drop"}}"#),
            ..config(
                vec![source("orders-west", "orders")],
                vec![
                    DestinationDto {
                        on_exception: answers(r#"{"kinds":{"Refused":{"deliver":["dlq"]}}}"#),
                        ..destination("out")
                    },
                    destination("dlq"),
                ],
                vec![unconditional("r1", "orders-west", "normalize", &["out"])],
                vec![pipeline("normalize", "Normalize", vec![])],
            )
        };
        let map = build_fleet(vec![group_bundle("ingest-1", file)]);

        let edge = |from_kind, from: &str, route: Option<&str>, kinds| ExceptionEdge {
            group: "ingest-1".into(),
            from_kind,
            from: from.into(),
            route: route.map(Into::into),
            destination: "dlq".into(),
            kinds,
        };
        assert_eq!(
            map.exceptions,
            vec![
                edge(
                    "pipeline",
                    "normalize",
                    Some("r1"),
                    vec!["Overflow", "DivideByZero", "NaN"]
                ),
                edge("destination", "out", None, vec!["Refused"]),
            ]
        );
    }

    /// One edge per source a row can still fall through, and none for a source whose routing is already total.
    /// A source with no routes at all falls through everything, which is how the runtime treats it.
    #[test]
    fn an_unmatched_edge_is_drawn_for_every_source_a_row_can_still_fall_through() {
        let file = FileConfig {
            on_exception: Some(serde_json::from_str(r#"{"kinds":{"Unmatched":{"deliver":["leftovers"]}}}"#).unwrap()),
            ..config(
                vec![
                    source("orders-west", "orders"),
                    source("orders-east", "orders"),
                    source("clicks", "clicks"),
                ],
                vec![destination("leftovers")],
                vec![
                    route("r1", "orders-west", "normalize", &[]),
                    unconditional("r2", "orders-east", "normalize", &[]),
                ],
                vec![pipeline("normalize", "Normalize", vec![])],
            )
        };
        let map = build_fleet(vec![group_bundle("ingest-1", file)]);

        let mut drawn: Vec<&str> = map.unmatched.iter().map(|e| e.source.as_str()).collect();
        drawn.sort();
        assert_eq!(drawn, vec!["clicks", "orders-west"]);
        assert!(map.unmatched.iter().all(|e| e.destination == "leftovers"));
    }

    /// Unmatched rows that block or drop have no destination, so there is no edge. That case
    /// is what the warning catalog is for, not the graph.
    #[test]
    fn no_unmatched_destination_draws_no_unmatched_edges() {
        let file = config(
            vec![source("orders-west", "orders")],
            vec![destination("leftovers")],
            vec![route("r1", "orders-west", "normalize", &[])],
            vec![pipeline("normalize", "Normalize", vec![])],
        );
        let map = build_fleet(vec![group_bundle("ingest-1", file)]);

        assert!(map.unmatched.is_empty());
    }

    /// Every source can panic on a batch, so configuration cannot say which ones do. A poison edge is drawn from
    /// the batches actually diverted -- both ends off the sample, so an edge whose destination this branch no longer
    /// declares is passed over rather than re-attributed to whatever `unprocessable` now names.
    #[test]
    fn a_poison_edge_is_drawn_only_where_a_batch_has_been_diverted() {
        let store = RegistryStore::open(std::path::Path::new(":memory:")).unwrap();
        let now = warnings::now_millis();
        store
            .record_delta(
                BATCHES_DLQ_METRIC,
                r#"[["destination","quarantine"],["group","ingest-1"],["source","orders-east"]]"#,
                now,
                2.0,
            )
            .unwrap();
        // The deployment that diverted this one named a destination the branch no longer declares, so it has no node
        // to draw to.
        store
            .record_delta(
                BATCHES_DLQ_METRIC,
                r#"[["destination","retired"],["group","ingest-1"],["source","orders-west"]]"#,
                now,
                9.0,
            )
            .unwrap();

        let file = FileConfig {
            on_exception: Some(
                serde_json::from_str(r#"{"kinds":{"Unprocessable":{"deliver":["quarantine"]}}}"#).unwrap(),
            ),
            ..config(
                vec![source("orders-west", "orders"), source("orders-east", "orders")],
                vec![destination("quarantine")],
                vec![],
                vec![],
            )
        };
        let (shared, shared_at) = empty_shared();
        let map = build(
            shared,
            &shared_at,
            vec![group_bundle("ingest-1", file)],
            &[],
            Some(&store),
        );

        assert_eq!(map.poison.len(), 1);
        let edge = &map.poison[0];
        assert_eq!(edge.source, "orders-east");
        assert_eq!(edge.destination, "quarantine");
        assert_eq!(edge.throughput, 2.0);
    }
}
