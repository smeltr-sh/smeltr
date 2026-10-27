//! Every link between a bundle's objects, from one walk over a `FileConfig`: which source a route reads, which
//! pipeline it runs and where it delivers, which tables and grok patterns a pipeline reads, which pipelines it calls,
//! where a destination's refusals go.
//!
//! `impact` answers from these what a change reaches. [`Links::connections`] answers what a change to one object
//! reaches before anything changes: the blast radius an editor shows (user story A5).
//!
//! A route is where data's path is decided, so the path through a pipeline is a route's: a pipeline two routes run
//! joins each route's source to that route's destinations alone, as the map draws it.

use std::collections::{BTreeMap, BTreeSet};

use super::dto::{AnswerDto, FileConfig, OnExceptionDto, StepDto};

/// The kinds of object a bundle declares. An id is unique only within its kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Kind {
    Source,
    Route,
    Pipeline,
    Destination,
    LookupTable,
    GrokPattern,
}

/// One object of a bundle.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize)]
pub struct Object {
    pub kind: Kind,
    pub id: String,
}

impl Object {
    pub fn new(kind: Kind, id: &str) -> Object {
        Object {
            kind,
            id: id.to_string(),
        }
    }
}

/// What a change to one object reaches, never holding the object itself.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Connections {
    /// Everything downstream of it: what reads, runs, calls or delivers on from it.
    pub downstream: BTreeSet<Object>,
    /// What it holds an implicit contract with on fields, though upstream of it: the sources whose rows a pipeline
    /// processes, and the pipelines whose rows a destination writes. Empty for every other kind.
    pub upstream: BTreeSet<Object>,
}

impl Connections {
    pub fn is_empty(&self) -> bool {
        self.downstream.is_empty() && self.upstream.is_empty()
    }
}

/// One route's path: what it reads, what it runs, and every destination it writes to.
#[derive(Clone, Debug)]
pub struct RouteLinks {
    pub id: String,
    pub source: String,
    pub pipeline: String,
    /// Delivered to by a rule, by the route's or the group's exception answers, and down each chain of refusals:
    /// a destination's refusals go where its own answer says.
    pub destinations: BTreeSet<String>,
}

/// Every link in one bundle.
#[derive(Clone, Debug, Default)]
pub struct Links {
    pub routes: Vec<RouteLinks>,
    /// Pipeline id -> the pipelines it calls, directly.
    pub calls: BTreeMap<String, BTreeSet<String>>,
    /// Pipeline id -> the pipelines that call it, directly.
    pub callers: BTreeMap<String, BTreeSet<String>>,
    /// Pipeline id -> the lookup tables its steps read, however deep.
    pub tables: BTreeMap<String, BTreeSet<String>>,
    /// Pipeline id -> every `%{NAME}` its steps hold. Read off the steps' text, so a grok in code counts, and a name
    /// nothing declares (a built-in, or a pattern a change deleted) is kept too.
    pub patterns: BTreeMap<String, BTreeSet<String>>,
    /// Shared grok pattern id -> every `%{NAME}` its own pattern holds.
    pub pattern_names: BTreeMap<String, BTreeSet<String>>,
    /// Destination id -> where its refusals go, directly.
    pub refusals: BTreeMap<String, BTreeSet<String>>,
    /// Where the group's own exception answers deliver: a row any source reads can end there.
    pub group_deliveries: BTreeSet<String>,
}

impl Links {
    pub fn of(file: &FileConfig) -> Links {
        let mut links = Links {
            group_deliveries: delivered_to(&file.on_exception).collect(),
            ..Links::default()
        };
        for d in &file.destinations {
            links
                .refusals
                .insert(d.id.clone(), delivered_to(&d.on_exception).collect());
        }
        for p in &file.pipelines {
            let (mut calls, mut tables) = (BTreeSet::new(), BTreeSet::new());
            walk(&p.steps, &mut |step| match step {
                StepDto::Call { pipeline, .. } => {
                    calls.insert(pipeline.clone());
                }
                StepDto::Lookup { table, .. } => {
                    tables.insert(table.clone());
                }
                _ => {}
            });
            for called in &calls {
                links.callers.entry(called.clone()).or_default().insert(p.id.clone());
            }
            links.calls.insert(p.id.clone(), calls);
            links.tables.insert(p.id.clone(), tables);
            let text = serde_json::to_string(&p.steps).unwrap_or_default();
            links.patterns.insert(p.id.clone(), pattern_names(&text));
        }
        for g in &file.grok_patterns {
            links.pattern_names.insert(g.id.clone(), pattern_names(&g.pattern));
        }
        for r in &file.routes {
            let first: Vec<String> = (r.deliver.iter().flat_map(|rule| rule.to.iter().cloned()))
                .chain(delivered_to(&r.on_exception))
                .chain(links.group_deliveries.iter().cloned())
                .collect();
            let destinations = links.refusal_chain(first);
            links.routes.push(RouteLinks {
                id: r.id.clone(),
                source: r.source.clone(),
                pipeline: r.pipeline.clone(),
                destinations,
            });
        }
        links
    }

    /// `from`, and every destination down each one's chain of refusals.
    fn refusal_chain(&self, from: impl IntoIterator<Item = String>) -> BTreeSet<String> {
        closure(from, |d| self.refusals.get(d).into_iter().flatten().cloned().collect())
    }

    /// `id`, and every pipeline that splices it in, however far up.
    fn with_callers(&self, id: &str) -> BTreeSet<String> {
        closure([id.to_string()], |p| {
            self.callers.get(p).into_iter().flatten().cloned().collect()
        })
    }

    /// `id`, and every pipeline it splices in, however far down.
    fn with_callees(&self, id: &str) -> BTreeSet<String> {
        closure([id.to_string()], |p| {
            self.calls.get(p).into_iter().flatten().cloned().collect()
        })
    }

    /// `patterns`, and every shared pattern that names one of them, directly or through another, however deep.
    pub(super) fn with_namers(&self, patterns: impl IntoIterator<Item = String>) -> BTreeSet<String> {
        closure(patterns, |p| {
            (self.pattern_names.iter())
                .filter(|(_, names)| names.contains(p))
                .map(|(id, _)| id.clone())
                .collect()
        })
    }

    /// The routes that run `pipeline`, or a pipeline that splices it in.
    fn routes_running(&self, pipeline: &str) -> impl Iterator<Item = &RouteLinks> {
        let running = self.with_callers(pipeline);
        self.routes.iter().filter(move |r| running.contains(&r.pipeline))
    }

    /// What a change to `object` reaches, in this bundle. An object the bundle does not link to anything answers
    /// empty.
    pub fn connections(&self, object: &Object) -> Connections {
        let mut c = Connections::default();
        let id = object.id.as_str();
        let down = &mut c.downstream;
        match object.kind {
            Kind::Source => {
                for r in self.routes.iter().filter(|r| r.source == id) {
                    down.extend(self.route_downstream(r));
                }
                // A row no route matches, or that nothing could process, takes the group's own answer.
                down.extend(destinations(&self.refusal_chain(self.group_deliveries.clone())));
            }
            Kind::Route => {
                if let Some(r) = self.routes.iter().find(|r| r.id == id) {
                    down.extend(self.route_downstream(r));
                }
            }
            Kind::Pipeline => {
                self.pipelines_downstream([id.to_string()], down);
                c.upstream = (self.routes_running(id))
                    .map(|r| Object::new(Kind::Source, &r.source))
                    .collect();
            }
            Kind::Destination => {
                down.extend(destinations(&self.refusal_chain([id.to_string()])));
                for r in self.routes.iter().filter(|r| r.destinations.contains(id)) {
                    c.upstream.extend(
                        self.with_callees(&r.pipeline)
                            .iter()
                            .map(|p| Object::new(Kind::Pipeline, p)),
                    );
                }
            }
            Kind::LookupTable => {
                let readers = (self.tables.iter())
                    .filter(|(_, tables)| tables.contains(id))
                    .map(|(p, _)| p.clone());
                self.pipelines_downstream(readers, down);
            }
            Kind::GrokPattern => {
                let naming = self.with_namers([id.to_string()]);
                down.extend(naming.iter().map(|p| Object::new(Kind::GrokPattern, p)));
                // The pipelines whose steps name it, directly or through a pattern that does.
                let pipelines = (self.patterns.iter())
                    .filter(|(_, names)| !names.is_disjoint(&naming))
                    .map(|(p, _)| p.clone());
                self.pipelines_downstream(pipelines.collect::<Vec<_>>(), down);
            }
        }
        c.downstream.remove(object);
        c
    }

    /// What a route runs and writes to.
    fn route_downstream(&self, r: &RouteLinks) -> impl Iterator<Item = Object> {
        let pipelines: Vec<Object> = (self.with_callees(&r.pipeline).iter())
            .map(|p| Object::new(Kind::Pipeline, p))
            .collect();
        std::iter::once(Object::new(Kind::Route, &r.id))
            .chain(pipelines)
            .chain(destinations(&r.destinations))
    }

    /// `pipelines`, every pipeline that splices one in, and the routes running any of them with what those write to.
    fn pipelines_downstream(&self, pipelines: impl IntoIterator<Item = String>, into: &mut BTreeSet<Object>) {
        for p in pipelines {
            into.extend(self.with_callers(&p).iter().map(|c| Object::new(Kind::Pipeline, c)));
            for r in self.routes_running(&p) {
                into.insert(Object::new(Kind::Route, &r.id));
                into.extend(destinations(&r.destinations));
            }
        }
    }
}

/// `start`, and everything `next` reaches from it, however far.
fn closure(start: impl IntoIterator<Item = String>, next: impl Fn(&str) -> Vec<String>) -> BTreeSet<String> {
    let mut seen = BTreeSet::new();
    let mut queue: Vec<String> = start.into_iter().collect();
    while let Some(id) = queue.pop() {
        if seen.insert(id.clone()) {
            queue.extend(next(&id));
        }
    }
    seen
}

fn destinations(ids: &BTreeSet<String>) -> impl Iterator<Item = Object> + '_ {
    ids.iter().map(|d| Object::new(Kind::Destination, d))
}

/// Every destination a `deliver` answer in `layer` names.
fn delivered_to(layer: &Option<OnExceptionDto>) -> impl Iterator<Item = String> + '_ {
    (layer.iter())
        .flat_map(|each| each.categories.values().chain(each.kinds.values()))
        .flat_map(|a| match a {
            AnswerDto::Deliver { to, .. } => to.clone(),
            _ => Vec::new(),
        })
}

/// Every `NAME` in a `%{NAME}` or `%{NAME:field}` in `text`.
fn pattern_names(text: &str) -> BTreeSet<String> {
    (text.match_indices("%{"))
        .filter_map(|(i, _)| {
            let rest = &text[i + 2..];
            let end = rest.find(['}', ':'])?;
            Some(rest[..end].to_string())
        })
        .filter(|name| !name.is_empty())
        .collect()
}

/// Every step, an `if` step's branches included.
fn walk(steps: &[StepDto], f: &mut impl FnMut(&StepDto)) {
    for step in steps {
        f(step);
        if let StepDto::If { then, r#else, .. } = step {
            walk(then, f);
            walk(r#else, f);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Two firewalls through one shared normalizer to one index, a web log to another, with refused rows salvaged to
    /// a DLQ and rows nothing could route sent to `leftovers`.
    ///
    /// ```text
    ///   fw-a ─ r-a ─┐                         ┌─ es-net ─ refused ─ dlq
    ///               ├─ fw-normalize ─ calls ─ common (reads sites, names %{FW_LINE} -> %{IP})
    ///   fw-b ─ r-b ─┘                         └─ es-net-b
    ///   web ── r-web ── web-normalize ── es-web
    /// ```
    fn bundle() -> FileConfig {
        let kafka_source = |id: &str| {
            json!({"id": id, "type": "kafka", "brokers": "b:9092", "topics": [id],
                                             "groupId": "g"})
        };
        let kafka_dest = |id: &str, dlq: Option<&str>| {
            let mut d = json!({"id": id, "type": "kafka", "brokers": "b:9092", "defaultTopic": id});
            if let Some(dlq) = dlq {
                d["exceptions"] = json!({"kinds": {"Refused": {"deliver": [dlq]}}});
            }
            d
        };
        let route = |id: &str, from: &str, through: &str, to: &str| json!({"id": id, "source": from, "pipeline": through, "deliver": [{"to": [to]}]});
        serde_json::from_value(json!({
            "version": 1,
            "sources": [kafka_source("fw-a"), kafka_source("fw-b"),
                        {"id": "web", "type": "kafka", "brokers": "b:9092", "topics": ["web"], "groupId": "g"}],
            "destinations": [kafka_dest("es-net", Some("dlq")), kafka_dest("es-net-b", None),
                             kafka_dest("es-web", None), kafka_dest("dlq", None), kafka_dest("leftovers", None)],
            "pipelines": [
                {"id": "fw-normalize", "name": "fw", "steps": [{"type": "call", "id": "c", "pipeline": "common"}]},
                {"id": "common", "name": "common", "steps": [
                    {"type": "lookup", "id": "site", "input": "ip", "table": "sites", "matchKind": "exact",
                     "multiplicity": "first", "keyColumn": "ip", "outputs": []},
                    {"id": "line", "type": "parser", "input": "_raw",
                     "kind": {"type": "grok", "patterns": ["%{FW_LINE}"]}}]},
                {"id": "web-normalize", "name": "web", "steps": []}
            ],
            "routes": [route("r-a", "fw-a", "fw-normalize", "es-net"), route("r-b", "fw-b", "fw-normalize", "es-net-b"),
                       route("r-web", "web", "web-normalize", "es-web")],
            "grokPatterns": [{"id": "FW_LINE", "pattern": "%{IP:src} %{WORD:action}"}, {"id": "IP", "pattern": "[0-9.]+"}],
            "exceptions": {"kinds": {"Unmatched": {"deliver": ["leftovers"]}}}
        }))
        .unwrap()
    }

    fn objects(of: &[(Kind, &str)]) -> BTreeSet<Object> {
        of.iter().map(|(k, id)| Object::new(*k, id)).collect()
    }

    fn connections(kind: Kind, id: &str) -> Connections {
        Links::of(&bundle()).connections(&Object::new(kind, id))
    }

    use Kind::*;

    /// A destination reaches the destinations its refusals go to, and holds a contract with every pipeline whose rows
    /// it writes, down a chain of refusals too.
    #[test]
    fn a_destination_reaches_its_refusals_and_names_the_pipelines_it_writes() {
        let c = connections(Destination, "es-net");
        let writers = objects(&[(Pipeline, "fw-normalize"), (Pipeline, "common")]);
        assert_eq!(c.upstream, writers);
        assert_eq!(c.downstream, objects(&[(Destination, "dlq")]));
        assert_eq!(connections(Destination, "dlq").upstream, writers);
    }

    /// A pipeline two routes run joins each source to its own route's destinations alone.
    #[test]
    fn a_source_feeds_only_its_own_routes_destinations() {
        let c = connections(Source, "fw-a");
        assert!(c.upstream.is_empty());
        assert_eq!(
            c.downstream,
            objects(&[
                (Route, "r-a"),
                (Pipeline, "fw-normalize"),
                (Pipeline, "common"),
                (Destination, "es-net"),
                (Destination, "dlq"),
                (Destination, "leftovers"),
            ])
        );
    }

    /// A pipeline spliced into another runs wherever its caller does, on its caller's sources.
    #[test]
    fn a_called_pipeline_reaches_through_its_callers() {
        let c = connections(Pipeline, "common");
        assert_eq!(c.upstream, objects(&[(Source, "fw-a"), (Source, "fw-b")]));
        assert_eq!(
            c.downstream,
            objects(&[
                (Pipeline, "fw-normalize"),
                (Route, "r-a"),
                (Route, "r-b"),
                (Destination, "es-net"),
                (Destination, "es-net-b"),
                (Destination, "dlq"),
                (Destination, "leftovers"),
            ])
        );
    }

    /// A table and a grok pattern reach whatever reads them, however deep the read is.
    #[test]
    fn a_table_and_a_pattern_reach_what_reads_them() {
        let readers = objects(&[
            (Pipeline, "common"),
            (Pipeline, "fw-normalize"),
            (Route, "r-a"),
            (Route, "r-b"),
            (Destination, "es-net"),
            (Destination, "es-net-b"),
            (Destination, "dlq"),
            (Destination, "leftovers"),
        ]);
        assert_eq!(connections(LookupTable, "sites").downstream, readers);
        let ip = connections(GrokPattern, "IP");
        let mut through_fw_line = readers.clone();
        through_fw_line.insert(Object::new(GrokPattern, "FW_LINE"));
        assert_eq!(ip.downstream, through_fw_line);
        assert!(ip.upstream.is_empty());
    }

    /// Something nothing links to answers empty.
    #[test]
    fn an_unlinked_object_answers_empty() {
        assert!(connections(LookupTable, "unused").is_empty());
        assert!(connections(Route, "gone").is_empty());
    }
}
