//! What a change reaches: every pipeline and route whose behavior a set of edits can alter, and why.
//!
//! A pipeline is reached by its own edit, by a lookup table it reads, by a shared grok pattern it names, or by a
//! pipeline it calls -- a call splices
//! the callee's steps in, so the caller's output moves too. A route is reached by its own edit, by the source it
//! reads from, or by a destination it writes to: routing and delivery are the route's concern, not the pipeline's,
//! so a pipeline tested in isolation is unaffected by either.
//!
//! This is answered from two `FileConfig`s rather than from a diff of files: `sources.json` holds every source, so
//! "that file changed" cannot say which. Comparing the objects says which.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use crate::core::error::{Category, ExceptionKind};

use super::dto::{
    DestinationDto, FileConfig, GrokPatternDto, IdentityDto, LookupTableDto, PipelineDto, RouteDto, SourceDto, StepDto,
};
use super::links::Links;

/// Everything `after` reaches, and why.
#[derive(Clone, Debug, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Impact {
    /// Sorted by id, so two callers comparing the same change agree on the order.
    pub pipelines: Vec<Reached>,
    /// Sorted by id. `RouteDto.id` and `PipelineDto.id` are separate namespaces, so this is never merged with
    /// `pipelines`.
    pub routes: Vec<Reached>,
    /// The name of every pipeline in `before` or `after`, by id, `after`'s where both hold it. A person sees a
    /// pipeline by name, and a `Reason::Calls` may name one the change deleted, which only `before` still holds.
    pub pipeline_names: BTreeMap<String, String>,
}

/// One pipeline or route a change reaches, and every reason it was: reached by two paths at once, it keeps both.
#[derive(Clone, Debug, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Reached {
    pub id: String,
    /// Deduplicated, ordered.
    pub reasons: Vec<Reason>,
}

/// Why a change reaches a pipeline or route.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Serialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum Reason {
    /// The object's own definition changed.
    Edited,
    /// A lookup table the pipeline reads changed.
    ReadsTable { table: String },
    /// A shared grok pattern the pipeline names changed, or one it names does.
    NamesGrokPattern { pattern: String },
    /// A pipeline this one calls was itself reached; the call splices its steps in.
    Calls { pipeline: String },
    /// The source the route reads from changed.
    SourceChanged { source: String },
    /// A destination the route writes to, on success or on error, changed.
    DestinationChanged { destination: String },
}

/// Every pipeline `after` behaves differently in, or might, and every route whose routing or delivery differs,
/// given what changed from `before`.
pub fn pipelines_touched(before: &FileConfig, after: &FileConfig) -> Impact {
    let links = Links::of(after);
    let changed = Changed::between(before, after, &links);

    let mut pipelines: BTreeMap<String, BTreeSet<Reason>> = BTreeMap::new();
    for pipeline in &after.pipelines {
        let mut reasons = BTreeSet::new();
        // A pipeline is tested under the group's exceptions.
        if changed.pipelines.contains(&pipeline.id) || pipeline_answers_differ(before, after) {
            reasons.insert(Reason::Edited);
        }
        for table in changed_among(links.tables.get(&pipeline.id), &changed.lookup_tables) {
            reasons.insert(Reason::ReadsTable { table });
        }
        for pattern in changed_among(links.patterns.get(&pipeline.id), &changed.grok_patterns) {
            reasons.insert(Reason::NamesGrokPattern { pattern });
        }
        if !reasons.is_empty() {
            pipelines.insert(pipeline.id.clone(), reasons);
        }
    }

    // A pipeline is reached by whoever calls it: a call splices its steps in, so the caller's output moves with
    // it. `id` is the callee just found reached, which is exactly why `caller` is reached too. Seeded from every
    // pipeline whose steps changed or whose tables did, not from an edit to the rest of it: a call inlines steps and
    // nothing else. And from every deleted pipeline id: a caller of a pipeline the change deleted is reached too,
    // even though the deleted callee itself is gone from `after.pipelines` and so reports nothing of its own.
    let after_pipeline_ids: HashSet<&str> = after.pipelines.iter().map(|p| p.id.as_str()).collect();
    let deleted_pipelines = changed
        .pipelines
        .iter()
        .filter(|id| !after_pipeline_ids.contains(id.as_str()))
        .cloned();
    let mut queue: Vec<String> = pipelines
        .iter()
        .filter(|(id, reasons)| {
            changed.pipeline_steps.contains(*id)
                || reasons
                    .iter()
                    .any(|r| matches!(r, Reason::ReadsTable { .. } | Reason::NamesGrokPattern { .. }))
        })
        .map(|(id, _)| id.clone())
        .chain(deleted_pipelines)
        .collect();
    while let Some(id) = queue.pop() {
        for caller in links.callers.get(&id).into_iter().flatten() {
            let new = pipelines
                .entry(caller.clone())
                .or_default()
                .insert(Reason::Calls { pipeline: id.clone() });
            if new {
                queue.push(caller.clone());
            }
        }
    }

    let mut routes: BTreeMap<String, BTreeSet<Reason>> = BTreeMap::new();
    for (route, written) in after.routes.iter().zip(&links.routes) {
        let mut reasons = BTreeSet::new();
        // The group's exceptions are the base the route's own lay over.
        if changed.routes.contains(&route.id) || before.on_exception != after.on_exception {
            reasons.insert(Reason::Edited);
        }
        if changed.sources.contains(&route.source) {
            reasons.insert(Reason::SourceChanged {
                source: route.source.clone(),
            });
        }
        for destination in changed_among(Some(&written.destinations), &changed.destinations) {
            reasons.insert(Reason::DestinationChanged { destination });
        }
        if !reasons.is_empty() {
            routes.insert(route.id.clone(), reasons);
        }
    }

    // An object the change deleted reaches nothing: it is gone, and nothing left runs it. Each kind is retained
    // against its own collection in `after` -- route ids and pipeline ids are separate namespaces, so checking one
    // against the other would drop live routes or let a deleted route survive under a live pipeline's id.
    pipelines.retain(|id, _| after.pipelines.iter().any(|p| &p.id == id));
    routes.retain(|id, _| after.routes.iter().any(|r| &r.id == id));

    Impact {
        pipelines: into_reached(pipelines),
        routes: into_reached(routes),
        pipeline_names: before
            .pipelines
            .iter()
            .chain(&after.pipelines)
            .map(|p| (p.id.clone(), p.name.clone()))
            .collect(),
    }
}

fn into_reached(reached: BTreeMap<String, BTreeSet<Reason>>) -> Vec<Reached> {
    reached
        .into_iter()
        .map(|(id, reasons)| Reached {
            id,
            reasons: reasons.into_iter().collect(),
        })
        .collect()
}

/// Which objects differ, by id. A deleted object counts as changed: what read it now reads nothing.
struct Changed {
    sources: HashSet<String>,
    destinations: HashSet<String>,
    routes: HashSet<String>,
    lookup_tables: HashSet<String>,
    /// Each shared grok pattern that changed, and each that names one that did: it expands differently.
    grok_patterns: HashSet<String>,
    pipelines: HashSet<String>,
    /// The pipelines whose steps changed: what a `call` inlines, so what reaches its callers. The rest of a pipeline
    /// -- its name -- is its own.
    pipeline_steps: HashSet<String>,
}

impl Changed {
    fn between(before: &FileConfig, after: &FileConfig, links: &Links) -> Changed {
        let identities = differing(&before.identities, &after.identities, |i: &IdentityDto| i.id.clone());
        let mut sources = differing(&before.sources, &after.sources, |s: &SourceDto| s.id.clone());
        let mut destinations = differing(&before.destinations, &after.destinations, |d: &DestinationDto| {
            d.id.clone()
        });
        // A source or destination naming a changed identity behaves differently even though its own DTO did not:
        // the identity is what it connects out under.
        sources.extend(
            after
                .sources
                .iter()
                .filter(|s| s.identity().is_some_and(|id| identities.contains(id)))
                .map(|s| s.id.clone()),
        );
        destinations.extend(
            after
                .destinations
                .iter()
                .filter(|d| d.identity().is_some_and(|id| identities.contains(id)))
                .map(|d| d.id.clone()),
        );
        Changed {
            sources,
            destinations,
            routes: differing(&before.routes, &after.routes, |r: &RouteDto| r.id.clone()),
            lookup_tables: differing(&before.lookup_tables, &after.lookup_tables, |t: &LookupTableDto| {
                t.id.clone()
            }),
            grok_patterns: patterns_changed(before, after, links),
            pipelines: differing(&before.pipelines, &after.pipelines, |p: &PipelineDto| p.id.clone()),
            pipeline_steps: differing(&steps_of(before), &steps_of(after), |(id, _)| id.to_string()),
        }
    }
}

/// Each pipeline's id and steps, for comparing what a `call` inlines and nothing else.
fn steps_of(file: &FileConfig) -> Vec<(&str, &[StepDto])> {
    file.pipelines
        .iter()
        .map(|p| (p.id.as_str(), p.steps.as_slice()))
        .collect()
}

/// Ids present on one side only, or present on both with different content.
///
/// Content is compared as serialized JSON: a DTO is plain data, and this needs no per-kind equality to maintain
/// alongside the shapes themselves.
fn differing<T: serde::Serialize>(before: &[T], after: &[T], id_of: fn(&T) -> String) -> HashSet<String> {
    let json = |item: &T| serde_json::to_string(item).unwrap_or_default();
    let old: HashMap<String, String> = before.iter().map(|i| (id_of(i), json(i))).collect();
    let new: HashMap<String, String> = after.iter().map(|i| (id_of(i), json(i))).collect();

    let mut changed = HashSet::new();
    for (id, body) in &new {
        if old.get(id) != Some(body) {
            changed.insert(id.clone());
        }
    }
    for id in old.keys() {
        if !new.contains_key(id) {
            changed.insert(id.clone());
        }
    }
    changed
}

/// Whether the group's answers a pipeline runs under differ: every kind but the delivery ones, which never reach a
/// pipeline, and the tag field.
pub fn pipeline_answers_differ(before: &FileConfig, after: &FileConfig) -> bool {
    let reaching = |f: &FileConfig| {
        let mut layer = f.on_exception.clone().unwrap_or_default();
        layer
            .categories
            .retain(|k, _| Category::ALL.iter().any(|(n, c)| n == k && *c != Category::Delivery));
        layer.kinds.retain(|k, _| {
            ExceptionKind::KINDS
                .iter()
                .any(|(n, c)| n == k && *c != Category::Delivery)
        });
        layer
    };
    reaching(before) != reaching(after)
}

/// Of `ids`, the ones in `changed`, in order.
fn changed_among(ids: Option<&BTreeSet<String>>, changed: &HashSet<String>) -> BTreeSet<String> {
    ids.into_iter()
        .flatten()
        .filter(|id| changed.contains(*id))
        .cloned()
        .collect()
}

/// The shared patterns that changed, and every one that names one of them, however deep.
fn patterns_changed(before: &FileConfig, after: &FileConfig, links: &Links) -> HashSet<String> {
    let changed = differing(&before.grok_patterns, &after.grok_patterns, |p: &GrokPatternDto| {
        p.id.clone()
    });
    links.with_namers(changed).into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config_file::CURRENT_VERSION;
    use crate::config_file::dto::{
        AnswerDto, DeliverRuleDto, LookupSourceDto, MatchKindDto, OnExceptionDto, SourceKindDto,
    };

    fn pipeline(id: &str, steps: Vec<StepDto>) -> PipelineDto {
        PipelineDto {
            id: id.to_string(),
            name: id.to_string(),
            steps,
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

    fn lookup(table: &str) -> StepDto {
        StepDto::Lookup {
            id: format!("lookup-{table}"),
            filter: None,
            disabled: false,
            input: "city_id".to_string(),
            table: table.to_string(),
            match_kind: MatchKindDto::Exact,
            multiplicity: Default::default(),
            key_column: "city_id".to_string(),
            outputs: vec![],
        }
    }

    fn source(id: &str, topic: &str) -> SourceDto {
        SourceDto {
            id: id.to_string(),
            kind: SourceKindDto::Kafka {
                schema_registry: None,
                read: Default::default(),
                brokers: "kafka:9092".to_string(),
                topics: vec![topic.to_string()],
                group_id: "g".to_string(),
                identity: None,
                start: Default::default(),
                stop: None,
                fetch_bytes: Default::default(),
                encoding: Default::default(),
                props: Default::default(),
            },
        }
    }

    fn route(id: &str, from: &str, through: &str, to: &[&str]) -> RouteDto {
        route_with_when(id, from, through, None, to)
    }

    fn route_with_when(id: &str, from: &str, through: &str, when: Option<&str>, to: &[&str]) -> RouteDto {
        RouteDto {
            id: id.to_string(),
            source: from.to_string(),
            condition: None,
            pipeline: through.to_string(),
            deliver: vec![DeliverRuleDto {
                when: when.map(str::to_string),
                to: to.iter().map(|d| d.to_string()).collect(),
            }],
            on_exception: None,
        }
    }

    fn config(sources: Vec<SourceDto>, routes: Vec<RouteDto>, pipelines: Vec<PipelineDto>) -> FileConfig {
        FileConfig {
            version: CURRENT_VERSION,
            sources,
            destinations: vec![],
            pipelines,
            routes,
            lookup_tables: vec![],
            grok_patterns: vec![],
            identities: vec![],
            on_exception: None,
            name: None,
        }
    }

    fn reached(id: &str, reasons: Vec<Reason>) -> Reached {
        Reached {
            id: id.to_string(),
            reasons,
        }
    }

    /// A delivery kind never reaches a pipeline, so answering one differently reaches none; a kind that does reaches
    /// every pipeline.
    #[test]
    fn only_an_answer_a_pipeline_runs_under_reaches_it() {
        let with = |kind: &str, answer: AnswerDto| FileConfig {
            on_exception: Some(OnExceptionDto {
                kinds: [(kind.to_string(), answer)].into(),
                ..Default::default()
            }),
            ..config(vec![], vec![], vec![pipeline("orders", vec![])])
        };
        let before = config(vec![], vec![], vec![pipeline("orders", vec![])]);
        assert!(
            pipelines_touched(&before, &with("Refused", AnswerDto::Drop))
                .pipelines
                .is_empty()
        );
        assert_eq!(
            pipelines_touched(&before, &with("NaN", AnswerDto::Drop)).pipelines,
            vec![reached("orders", vec![Reason::Edited])]
        );
    }

    /// The ordinary case: one pipeline edited, one pipeline reached, for the reason that it was edited.
    #[test]
    fn an_edited_pipeline_reaches_itself() {
        let before = config(
            vec![],
            vec![],
            vec![pipeline("orders", vec![]), pipeline("clicks", vec![])],
        );
        let after = config(
            vec![],
            vec![],
            vec![pipeline("orders", vec![call("enrich")]), pipeline("clicks", vec![])],
        );
        let impact = pipelines_touched(&before, &after);
        assert_eq!(impact.pipelines, vec![reached("orders", vec![Reason::Edited])]);
        assert!(impact.routes.is_empty());
    }

    /// A call splices the called pipeline's steps in, so editing it moves its callers' output too -- transitively,
    /// and each hop names the callee that reached it.
    #[test]
    fn an_edited_pipeline_reaches_everything_that_calls_it() {
        let pipelines = |enrich_steps: Vec<StepDto>| {
            vec![
                pipeline("orders", vec![call("enrich")]),
                pipeline("audit", vec![call("orders")]),
                pipeline("clicks", vec![]),
                pipeline("enrich", enrich_steps),
            ]
        };
        let before = config(vec![], vec![], pipelines(vec![]));
        let after = config(vec![], vec![], pipelines(vec![lookup("geo")]));

        assert_eq!(
            pipelines_touched(&before, &after).pipelines,
            vec![
                reached(
                    "audit",
                    vec![Reason::Calls {
                        pipeline: "orders".to_string()
                    }]
                ),
                reached("enrich", vec![Reason::Edited]),
                reached(
                    "orders",
                    vec![Reason::Calls {
                        pipeline: "enrich".to_string()
                    }]
                ),
            ],
            "clicks calls none of them"
        );
    }

    /// A grok naming `pattern`.
    fn grok(pattern: &str) -> StepDto {
        StepDto::Parser {
            id: "g".to_string(),
            filter: None,
            disabled: false,
            input: "_raw".to_string(),
            into: crate::plan::tree::ROOT.to_string(),
            kind: crate::config_file::ParserKindDto::Grok {
                patterns: vec![pattern.to_string()],
                custom_patterns: vec![],
                split_dots: None,
            },
            declared: vec![],
        }
    }

    /// `OUTER` names `INNER`, so editing `INNER` changes what `%{OUTER}` expands to. A pipeline naming neither, or
    /// naming a pattern whose name only starts the same, is not reached.
    #[test]
    fn a_changed_grok_pattern_reaches_every_pipeline_naming_it_however_deep() {
        let with = |inner: &str| FileConfig {
            grok_patterns: vec![
                GrokPatternDto {
                    id: "INNER".to_string(),
                    pattern: inner.to_string(),
                },
                GrokPatternDto {
                    id: "OUTER".to_string(),
                    pattern: "<%{INNER}>".to_string(),
                },
                GrokPatternDto {
                    id: "INNERMOST".to_string(),
                    pattern: "x".to_string(),
                },
            ],
            ..config(
                vec![],
                vec![],
                vec![
                    pipeline("direct", vec![grok("%{INNER:i}")]),
                    pipeline("nested", vec![grok("%{OUTER}")]),
                    pipeline("prefix", vec![grok("%{INNERMOST}")]),
                    pipeline("caller", vec![call("nested")]),
                ],
            )
        };
        let reason = |p: &str| Reason::NamesGrokPattern { pattern: p.to_string() };
        assert_eq!(
            pipelines_touched(&with("a"), &with("b")).pipelines,
            vec![
                reached(
                    "caller",
                    vec![Reason::Calls {
                        pipeline: "nested".to_string()
                    }]
                ),
                reached("direct", vec![reason("INNER")]),
                reached("nested", vec![reason("OUTER")]),
            ]
        );
    }

    /// Deleting a pipeline reaches its caller too, even though the deleted callee itself is gone from `after` and
    /// so is never a key `into_reached` sees a reason attached to.
    #[test]
    fn a_deleted_pipeline_reaches_everything_that_called_it() {
        let before = config(
            vec![],
            vec![],
            vec![pipeline("orders", vec![call("enrich")]), pipeline("enrich", vec![])],
        );
        let after = config(vec![], vec![], vec![pipeline("orders", vec![call("enrich")])]);

        assert_eq!(
            pipelines_touched(&before, &after).pipelines,
            vec![reached(
                "orders",
                vec![Reason::Calls {
                    pipeline: "enrich".to_string()
                }]
            )],
            "enrich is gone from `after`, so it reports nothing of its own, but orders still calls it"
        );
    }

    /// A pipeline reached by two paths at once -- edited, and reading a lookup table that also changed -- keeps
    /// both reasons rather than the second silently no-oping.
    #[test]
    fn a_pipeline_reached_two_ways_reports_both_reasons() {
        let table = |delimiter: &str| LookupTableDto {
            id: "geo".to_string(),
            source: LookupSourceDto::Csv {
                path: "lookups/geo.csv".to_string(),
                delimiter: delimiter.to_string(),
                headers: vec![],
            },
        };
        let with = |steps: Vec<StepDto>, delimiter: &str| FileConfig {
            lookup_tables: vec![table(delimiter)],
            ..config(vec![], vec![], vec![pipeline("clicks", steps)])
        };
        let before = with(vec![lookup("geo")], ",");
        let after = with(
            vec![
                lookup("geo"),
                StepDto::Comment {
                    id: "c".to_string(),
                    text: "note".to_string(),
                },
            ],
            ";",
        );

        assert_eq!(
            pipelines_touched(&before, &after).pipelines,
            vec![reached(
                "clicks",
                vec![
                    Reason::Edited,
                    Reason::ReadsTable {
                        table: "geo".to_string()
                    }
                ]
            )]
        );
    }

    /// A three-hop call chain, only the leaf changed: each hop's reason names the pipeline it calls, so the chain
    /// traces back to what actually changed.
    #[test]
    fn a_multi_hop_caller_chain_names_the_pipeline_that_changed() {
        let pipelines = |enrich_steps: Vec<StepDto>| {
            vec![
                pipeline("audit", vec![call("orders")]),
                pipeline("orders", vec![call("enrich")]),
                pipeline("enrich", enrich_steps),
            ]
        };
        let before = config(vec![], vec![], pipelines(vec![]));
        let after = config(vec![], vec![], pipelines(vec![lookup("geo")]));

        assert_eq!(
            pipelines_touched(&before, &after).pipelines,
            vec![
                reached(
                    "audit",
                    vec![Reason::Calls {
                        pipeline: "orders".to_string()
                    }]
                ),
                reached("enrich", vec![Reason::Edited]),
                reached(
                    "orders",
                    vec![Reason::Calls {
                        pipeline: "enrich".to_string()
                    }]
                ),
            ]
        );
    }

    /// A source reaches the routes that read it, not the pipelines behind them: a pipeline tested in isolation is
    /// unaffected by which source feeds it.
    #[test]
    fn an_edited_source_reaches_the_routes_that_read_it() {
        let routes = vec![
            route("r1", "orders-west", "orders-normalize", &["es"]),
            route("r2", "orders-west", "orders-audit", &["es"]),
            route("r3", "clicks-west", "clicks-sessionize", &["es"]),
        ];
        let pipelines = vec![
            pipeline("orders-normalize", vec![]),
            pipeline("orders-audit", vec![]),
            pipeline("clicks-sessionize", vec![]),
        ];
        let before = config(
            vec![source("orders-west", "orders"), source("clicks-west", "clicks")],
            routes.clone(),
            pipelines.clone(),
        );
        let after = config(
            vec![source("orders-west", "orders-v2"), source("clicks-west", "clicks")],
            routes,
            pipelines,
        );

        let impact = pipelines_touched(&before, &after);
        assert_eq!(
            impact.routes,
            vec![
                reached(
                    "r1",
                    vec![Reason::SourceChanged {
                        source: "orders-west".to_string()
                    }]
                ),
                reached(
                    "r2",
                    vec![Reason::SourceChanged {
                        source: "orders-west".to_string()
                    }]
                ),
            ],
            "both routes reading the changed source, and not the one reading a different one"
        );
        assert!(impact.pipelines.is_empty(), "no pipeline's own definition changed");
    }

    /// A destination salvages what it refuses to its DLQ, so a route delivering there writes to the DLQ too: a
    /// change to the DLQ reaches the route, and a route delivering elsewhere is not reached.
    #[test]
    fn a_route_is_reached_by_the_dlq_of_a_destination_it_delivers_to() {
        let kafka = |id: &str, topic: &str, dlq: Option<&str>| DestinationDto {
            id: id.to_string(),
            on_exception: dlq.map(|d| OnExceptionDto {
                kinds: [(
                    "Refused".to_string(),
                    AnswerDto::Deliver {
                        to: vec![d.to_string()],
                    },
                )]
                .into(),
                ..Default::default()
            }),
            kind: crate::config_file::dto::DestinationKindDto::Kafka {
                schema_registry: None,
                brokers: "kafka:9092".to_string(),
                default_topic: topic.to_string(),
                identity: None,
                format: Default::default(),
                field: None,
                compression: Default::default(),
                acks: crate::config_file::dto::AcksDto::All,
                stall_after_secs: Default::default(),
                buffer_bytes: Default::default(),
                props: Default::default(),
            },
        };
        let routes = vec![
            route("r1", "orders-west", "orders-normalize", &["es"]),
            route("r2", "orders-west", "orders-normalize", &["other"]),
        ];
        let pipelines = vec![pipeline("orders-normalize", vec![])];
        let with = |dlq_topic: &str| FileConfig {
            destinations: vec![
                kafka("es", "out", Some("dlq")),
                kafka("other", "out", None),
                kafka("dlq", dlq_topic, None),
            ],
            ..config(vec![source("orders-west", "orders")], routes.clone(), pipelines.clone())
        };

        let impact = pipelines_touched(&with("dlq-v1"), &with("dlq-v2"));
        assert_eq!(
            impact.routes,
            vec![reached(
                "r1",
                vec![Reason::DestinationChanged {
                    destination: "dlq".to_string()
                }]
            )]
        );
    }

    /// A source's own DTO does not change when the identity it names does -- the change lands in `identities`, not
    /// in `sources`. The route reading it still has to be reported: it connects differently now.
    #[test]
    fn a_route_is_reached_by_an_identity_its_source_names() {
        let routes = vec![route("r1", "orders-west", "orders-normalize", &["es"])];
        let pipelines = vec![pipeline("orders-normalize", vec![])];
        let mut named = source("orders-west", "orders");
        if let SourceKindDto::Kafka { identity, .. } = &mut named.kind {
            *identity = Some("acme".to_string());
        }
        let before = config(vec![named.clone()], routes.clone(), pipelines.clone());
        let mut after = before.clone();
        after.identities = vec![IdentityDto {
            id: "acme".to_string(),
            description: None,
            certificate: None,
            key: None,
            authorities: Some("-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----".to_string()),
            trust_public: None,
        }];

        let impact = pipelines_touched(&before, &after);
        assert_eq!(
            impact.routes,
            vec![reached(
                "r1",
                vec![Reason::SourceChanged {
                    source: "orders-west".to_string()
                }]
            )],
            "the identity a route's source connects out under changed, so the route did too"
        );
    }

    /// A lookup table is read inside a pipeline rather than named by a route, so it is found by walking steps --
    /// including the branches of an `if`.
    #[test]
    fn an_edited_lookup_table_reaches_the_pipelines_that_read_it_however_deep() {
        let nested = pipeline(
            "clicks",
            vec![StepDto::If {
                id: "gate".to_string(),
                condition: "true".to_string(),
                then: vec![lookup("geo")],
                r#else: vec![],
            }],
        );
        let table = |delimiter: &str| LookupTableDto {
            id: "geo".to_string(),
            source: LookupSourceDto::Csv {
                path: "lookups/geo.csv".to_string(),
                delimiter: delimiter.to_string(),
                headers: vec![],
            },
        };
        let with = |t: LookupTableDto| FileConfig {
            lookup_tables: vec![t],
            ..config(vec![], vec![], vec![nested.clone(), pipeline("orders", vec![])])
        };

        assert_eq!(
            pipelines_touched(&with(table(",")), &with(table(";"))).pipelines,
            vec![reached(
                "clicks",
                vec![Reason::ReadsTable {
                    table: "geo".to_string()
                }]
            )]
        );
    }

    /// A changed route is reported as a reached route by its own id, not folded into its pipeline: editing
    /// `deliver.when` does not move the pipeline's output.
    #[test]
    fn a_changed_route_is_reached_as_a_route_not_its_pipeline() {
        let pipelines = vec![pipeline("orders-normalize", vec![])];
        let before = config(
            vec![source("orders-west", "orders")],
            vec![route_with_when("r1", "orders-west", "orders-normalize", None, &["es"])],
            pipelines.clone(),
        );
        let after = config(
            vec![source("orders-west", "orders")],
            vec![route_with_when(
                "r1",
                "orders-west",
                "orders-normalize",
                Some("status == 'ok'"),
                &["es"],
            )],
            pipelines,
        );

        let impact = pipelines_touched(&before, &after);
        assert_eq!(impact.routes, vec![reached("r1", vec![Reason::Edited])]);
        assert!(
            impact.pipelines.is_empty(),
            "the pipeline's own behavior did not change"
        );
    }

    /// Route ids and pipeline ids are separate namespaces: a route deleted under an id is not reported just
    /// because a live pipeline happens to share that id.
    #[test]
    fn a_deleted_route_is_not_reported_under_a_live_pipelines_id() {
        let before = config(
            vec![source("orders-west", "orders")],
            vec![route("shared-id", "orders-west", "orders-normalize", &["es"])],
            vec![pipeline("shared-id", vec![])],
        );
        let after = config(
            vec![source("orders-west", "orders")],
            vec![],
            vec![pipeline("shared-id", vec![call("enrich")])],
        );

        let impact = pipelines_touched(&before, &after);
        assert_eq!(impact.pipelines, vec![reached("shared-id", vec![Reason::Edited])]);
        assert!(impact.routes.is_empty(), "the route under that id is gone");
    }

    /// A deleted callee is named from `before`; a renamed pipeline goes by its name in `after`.
    #[test]
    fn pipeline_names_cover_both_sides_and_after_wins() {
        let named = |id: &str, name: &str| PipelineDto {
            name: name.to_string(),
            ..pipeline(id, vec![])
        };
        let before = config(vec![], vec![], vec![named("p-1", "Orders"), named("p-2", "Enrich")]);
        let after = config(vec![], vec![], vec![named("p-1", "Orders v2")]);
        assert_eq!(
            pipelines_touched(&before, &after).pipeline_names,
            BTreeMap::from([
                ("p-1".to_string(), "Orders v2".to_string()),
                ("p-2".to_string(), "Enrich".to_string()),
            ])
        );
    }

    /// A pipeline the change deleted is not a pipeline to run tests against.
    #[test]
    fn a_deleted_pipeline_is_not_reached() {
        let before = config(
            vec![],
            vec![],
            vec![pipeline("orders", vec![]), pipeline("clicks", vec![])],
        );
        let after = config(vec![], vec![], vec![pipeline("clicks", vec![])]);
        assert!(pipelines_touched(&before, &after).pipelines.is_empty());
    }

    #[test]
    fn a_change_that_changed_nothing_reaches_nothing() {
        let c = config(
            vec![source("orders-west", "orders")],
            vec![route("r1", "orders-west", "orders", &["es"])],
            vec![pipeline("orders", vec![])],
        );
        let impact = pipelines_touched(&c, &c);
        assert!(impact.pipelines.is_empty());
        assert!(impact.routes.is_empty());
    }
}
