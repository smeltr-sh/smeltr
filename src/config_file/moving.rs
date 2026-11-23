//! Moving objects from one area to another: which of an area's objects must go with the one asked for, and what a
//! chosen set leaves broken.
//!
//! An object sees what its own area holds and, from a group, what `shared/` holds. So a move breaks a reference
//! when the two ends land where one cannot see the other: an object staying behind that names one moving into a
//! group, or a moving object that names one staying in a group. A test and its sample see only their own area.
//!
//! ```text
//!   staying ──names──▶ moving    broken unless the move is into shared
//!   moving  ──names──▶ staying   broken unless the move is out of shared, or the area moved to holds the same one
//! ```
//!
//! The objects the one asked for needs, followed until nothing more breaks, are the least a move takes.

use std::collections::BTreeSet;

use super::SHARED_DIR;
use super::dto::StepDto;
use super::import::{Listed, Objects, Part, Standing, describe};
use super::links::{delivered_to, pattern_name_spans, walk};

type Id = (Part, String);

/// One object naming another, and how, as a sentence says it. A test naming what it runs is loose: the move goes ahead, and the test's run says it
/// cannot run.
#[derive(Clone, Debug)]
struct Names {
    by: Id,
    of: Id,
    how: &'static str,
    loose: bool,
}

/// One object a move lists: how it stands where it goes, whether it is chosen, and whether the move needs it.
#[derive(Clone, Debug, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MoveRow {
    #[serde(flatten)]
    pub listed: Listed,
    pub ticked: bool,
    /// In the least the move takes.
    pub needed: bool,
}

/// A move of `asked` out of the area `from`, whose objects are `here`, into the area `to`, which sees `there`.
pub struct Moving<'a> {
    here: &'a Objects,
    there: &'a Objects,
    asked: Id,
    /// Further objects asked for beside `asked`, each with the least it takes, which the move takes along with
    /// `asked`'s: a card's other routes.
    also: BTreeSet<Id>,
    from: &'a str,
    to: &'a str,
    /// What `there` holds identically, by kind and id.
    same: BTreeSet<Id>,
}

impl<'a> Moving<'a> {
    pub fn new(here: &'a Objects, there: &'a Objects, asked: Id, from: &'a str, to: &'a str) -> Moving<'a> {
        let same = (here.list(there).into_iter())
            .filter(|l| l.standing == Standing::Same)
            .map(|l| (l.part, l.id))
            .collect();
        Moving {
            here,
            there,
            asked,
            also: BTreeSet::new(),
            from,
            to,
            same,
        }
    }

    /// A move that asks for `also` as well as the one object, with their least.
    pub fn also(self, also: BTreeSet<Id>) -> Moving<'a> {
        Moving { also, ..self }
    }

    /// Every reference between `here`'s objects.
    fn names(&self) -> Vec<Names> {
        let (f, mut all) = (&self.here.file, Vec::new());
        let mut add = |by: Id, part: Part, id: &str, how: &'static str| {
            all.push(Names {
                loose: by.0 == Part::Test && how != "runs over",
                by,
                of: (part, id.to_string()),
                how,
            })
        };
        let patterns: BTreeSet<&str> = f.grok_patterns.iter().map(|g| g.id.as_str()).collect();
        let named_patterns = |text: &str| -> BTreeSet<String> {
            (pattern_name_spans(text).map(|span| &text[span]))
                .filter(|n| patterns.contains(n))
                .map(str::to_string)
                .collect()
        };
        for r in &f.routes {
            let by = || (Part::Route, r.id.clone());
            add(by(), Part::Source, &r.source, "reads");
            add(by(), Part::Pipeline, &r.pipeline, "runs");
            let to: BTreeSet<String> = (r.deliver.iter().flat_map(|rule| rule.to.iter().cloned()))
                .chain(delivered_to(&r.on_exception))
                .collect();
            to.iter().for_each(|d| add(by(), Part::Destination, d, "delivers to"));
        }
        for p in &f.pipelines {
            let by = || (Part::Pipeline, p.id.clone());
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
            calls.iter().for_each(|c| add(by(), Part::Pipeline, c, "calls"));
            tables.iter().for_each(|t| add(by(), Part::LookupTable, t, "looks up"));
            let text = serde_json::to_string(&p.steps).unwrap_or_default();
            named_patterns(&text)
                .iter()
                .for_each(|g| add(by(), Part::GrokPattern, g, "uses"));
            p.produces.iter().for_each(|s| add(by(), Part::Schema, s, "produces"));
        }
        for d in &f.destinations {
            let by = || (Part::Destination, d.id.clone());
            let refused: BTreeSet<String> = delivered_to(&d.on_exception).collect();
            refused
                .iter()
                .for_each(|r| add(by(), Part::Destination, r, "delivers to"));
            d.accepts.iter().for_each(|s| add(by(), Part::Schema, s, "accepts"));
        }
        for g in &f.grok_patterns {
            (named_patterns(&g.pattern).iter())
                .filter(|n| **n != g.id)
                .for_each(|n| add((Part::GrokPattern, g.id.clone()), Part::GrokPattern, n, "uses"));
        }
        for t in &self.here.tests {
            let by = || (Part::Test, t.id.clone());
            add(by(), Part::Sample, &t.sample, "runs over");
            t.pipeline.iter().for_each(|p| add(by(), Part::Pipeline, p, "runs"));
            let routes: BTreeSet<&String> = (t.rows.iter())
                .filter_map(|o| o.sent.as_ref().map(|s| &s.route))
                .collect();
            routes.into_iter().for_each(|r| add(by(), Part::Route, r, "runs"));
        }
        let held = self.here.ids();
        all.retain(|n| held.contains(&n.of) && n.by != n.of);
        all
    }

    /// Whether `n` still holds once the objects in `going` have moved.
    fn holds(&self, n: &Names, going: &BTreeSet<Id>) -> bool {
        let own_area_only = matches!(n.of.0, Part::Sample | Part::Test);
        match (going.contains(&n.by), going.contains(&n.of)) {
            (true, true) | (false, false) => true,
            (false, true) => self.to == SHARED_DIR && !own_area_only,
            (true, false) => (self.from == SHARED_DIR && !own_area_only) || self.same.contains(&n.of),
        }
    }

    /// `ticked` and every object it takes with it, however far: the least a move of them takes.
    fn needed(&self, names: &[Names], ticked: &BTreeSet<Id>) -> BTreeSet<Id> {
        let mut going = ticked.clone();
        loop {
            let more: Vec<Id> = (names.iter())
                .filter(|n| !n.loose && !self.holds(n, &going))
                .map(|n| {
                    if going.contains(&n.by) {
                        n.of.clone()
                    } else {
                        n.by.clone()
                    }
                })
                .collect();
            if more.is_empty() {
                return going;
            }
            going.extend(more);
        }
    }

    /// The objects this move lists, with `ticked` chosen, or the least the move takes when `ticked` is `None`, and
    /// a sentence for each reference the chosen ones leave broken.
    pub fn list(&self, ticked: Option<&BTreeSet<Id>>) -> (Vec<MoveRow>, Vec<String>) {
        let names = self.names();
        let mut asked = self.also.clone();
        asked.insert(self.asked.clone());
        let least = self.needed(&names, &asked);
        let mut ticked = ticked.cloned().unwrap_or_else(|| least.clone());
        ticked.insert(self.asked.clone());
        let held = self.here.ids();
        ticked.retain(|id| held.contains(id));
        let needed = self.needed(&names, &ticked);
        // A sample has no tick of its own: it goes whenever what goes needs it, as an export takes it with its tests.
        ticked.extend(needed.iter().filter(|(part, _)| *part == Part::Sample).cloned());

        // What is chosen, what it takes, and what names or is named by what is chosen.
        let mut shown = needed.clone();
        for n in &names {
            if ticked.contains(&n.by) || ticked.contains(&n.of) {
                shown.insert(n.by.clone());
                shown.insert(n.of.clone());
            }
        }
        let left_out: BTreeSet<Id> = held.difference(&shown).cloned().collect();
        let (pipelines, routes) = (self.here.file.pipeline_names(), self.here.file.route_names());
        let name = |(part, id): &Id| -> String {
            let named = match part {
                Part::Pipeline => pipelines.get(id),
                Part::Route => routes.get(id),
                _ => None,
            };
            named.cloned().unwrap_or_else(|| id.clone())
        };
        let mut rows: Vec<MoveRow> = (self.here.without(&left_out).list(self.there).into_iter())
            .map(|mut listed| {
                let id = (listed.part, listed.id.clone());
                listed.name = Some(name(&id));
                MoveRow {
                    ticked: ticked.contains(&id),
                    needed: least.contains(&id) || needed.contains(&id),
                    listed,
                }
            })
            .collect();
        rows.sort_by_key(|r| (flow(r.listed.part), r.listed.id.clone()));
        rows.sort_by_key(|r| (r.listed.part, r.listed.id.clone()) != self.asked);

        // One sentence for each object left unticked that has to go, by the reference to the one asked for where it
        // has one: ticking it mends the rest of its references too.
        let mut unmet: Vec<&Names> = names.iter().filter(|n| !n.loose && !self.holds(n, &ticked)).collect();
        unmet.sort_by_key(|n| n.by != self.asked && n.of != self.asked);
        let mut said = BTreeSet::new();
        let mut broken = Vec::new();
        for n in unmet {
            let moving = ticked.contains(&n.by);
            if !said.insert(if moving { &n.of } else { &n.by }) {
                continue;
            }
            let (by, of) = (describe(n.by.0), describe(n.of.0));
            let (by_name, of_name) = (name(&n.by), name(&n.of));
            let (verb, from, to, cap) = (n.how, self.from, self.to, capitalized(by));
            broken.push(match moving {
                false => format!(
                    "{cap} {by_name} stays in {from} and {verb} {of} {of_name}, which {from} would no longer have. \
                     Tick the {by} to move it with the {of}."
                ),
                true => format!(
                    "{cap} {by_name} moves to {to} and {verb} {of} {of_name}, which {to} does not have. Tick the \
                     {of} to move it with the {by}."
                ),
            });
        }
        (rows, broken)
    }

    /// `here`'s objects that `ticked` names.
    pub fn going(&self, ticked: &BTreeSet<Id>) -> Objects {
        let staying: BTreeSet<Id> = self.here.ids().difference(ticked).cloned().collect();
        self.here.without(&staying)
    }
}

/// Where a kind sits along the way a row goes, which is the order a move lists them in: what reads, what runs, what
/// that reads, where it goes, and the tests over it.
fn flow(part: Part) -> u8 {
    match part {
        Part::Source => 0,
        Part::Route => 1,
        Part::Pipeline => 2,
        Part::LookupTable => 3,
        Part::GrokPattern => 4,
        Part::Schema => 5,
        Part::Destination => 6,
        Part::Test => 7,
        Part::Sample => 8,
    }
}

fn capitalized(word: &str) -> String {
    let mut chars = word.chars();
    chars
        .next()
        .map(|c| c.to_uppercase().chain(chars).collect())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::BTreeMap;

    /// Two firewalls' worth of objects in one area: `fw` read by `r-fw`, which runs `fw-normalize` (calling `common`,
    /// which looks up in `sites` and names `%{FW_LINE}`, which names `%{FW_IP}`) and delivers to `es-net`, whose
    /// refusals go to `dlq`; and `web` beside it, alone. Test `t-fw` runs `fw-normalize` over sample `s-fw`.
    fn objects() -> Objects {
        let dest = |id: &str, refused: Option<&str>| {
            let mut d = json!({"id": id, "type": "kafka", "brokers": "b:9092", "defaultTopic": id});
            if let Some(to) = refused {
                d["exceptions"] = json!({"categories": {"Refused": {"deliver": [to]}}});
            }
            d
        };
        let file = serde_json::from_value(json!({
            "version": 1,
            "sources": [{"id": "fw", "type": "metrics", "bucketSeconds": 10},
                        {"id": "web", "type": "metrics", "bucketSeconds": 10}],
            "destinations": [dest("es-net", Some("dlq")), dest("dlq", None), dest("es-web", None)],
            "pipelines": [
                {"id": "fw-normalize", "name": "fw", "produces": ["network"],
                 "steps": [{"type": "call", "id": "c", "pipeline": "common"}]},
                {"id": "common", "name": "common", "steps": [
                    {"type": "lookup", "id": "site", "input": "_raw", "table": "sites", "matchKind": "exact",
                     "multiplicity": "first", "keyColumn": "ip", "outputs": [{"source": "site", "field": "site"}]},
                    {"id": "line", "type": "parser", "input": "_raw",
                     "kind": {"type": "grok", "patterns": ["%{FW_LINE} %{WORD}"]}}]},
                {"id": "web-normalize", "name": "web", "steps": []}
            ],
            "routes": [
                {"id": "r-fw", "source": "fw", "pipeline": "fw-normalize", "deliver": [{"to": ["es-net"]}]},
                {"id": "r-web", "source": "web", "pipeline": "web-normalize", "deliver": [{"to": ["es-web"]}]}],
            "lookupTables": [{"id": "sites", "type": "inline", "headers": ["ip", "site"], "rows": []}],
            "grokPatterns": [{"id": "FW_LINE", "pattern": "%{FW_IP:src} %{WORD:action}"},
                             {"id": "FW_IP", "pattern": "[0-9.]+"}],
            "schemas": [{"id": "network"}]
        }))
        .unwrap();
        let sample = serde_json::from_value(json!({"id": "s-fw", "name": "s-fw", "columns": [], "rows": []})).unwrap();
        let test = serde_json::from_value(json!({
            "id": "t-fw", "name": "t-fw", "pipeline": "fw-normalize", "sample": "s-fw",
            "lockedAt": 0, "rowsIn": 0, "fields": [], "rows": []
        }))
        .unwrap();
        Objects {
            file,
            samples: vec![sample],
            tests: vec![test],
            contents: BTreeMap::new(),
        }
    }

    fn empty() -> Objects {
        Objects {
            file: serde_json::from_value(json!({"version": 1})).unwrap(),
            samples: Vec::new(),
            tests: Vec::new(),
            contents: BTreeMap::new(),
        }
    }

    fn ids(rows: &[MoveRow], which: impl Fn(&MoveRow) -> bool) -> Vec<(Part, &str)> {
        let mut ids: Vec<_> = (rows.iter().filter(|r| which(r)))
            .map(|r| (r.listed.part, r.listed.id.as_str()))
            .collect();
        ids.sort();
        ids
    }

    #[test]
    fn between_groups_a_source_takes_everything_its_route_needs_and_its_test_stays_a_choice() {
        let (here, there) = (objects(), empty());
        let moving = Moving::new(&here, &there, (Part::Source, "fw".into()), "west", "east");
        let (rows, broken) = moving.list(None);
        assert_eq!(broken, Vec::<String>::new());
        assert_eq!(
            ids(&rows, |r| r.ticked),
            [
                (Part::Pipeline, "common"),
                (Part::Pipeline, "fw-normalize"),
                (Part::LookupTable, "sites"),
                (Part::GrokPattern, "FW_IP"),
                (Part::GrokPattern, "FW_LINE"),
                (Part::Schema, "network"),
                (Part::Route, "r-fw"),
                (Part::Source, "fw"),
                (Part::Destination, "dlq"),
                (Part::Destination, "es-net"),
            ]
        );
        assert_eq!(ids(&rows, |r| !r.ticked), [(Part::Test, "t-fw")], "listed, unticked");
        assert_eq!(rows[0].listed.id, "fw", "the one asked for first");
    }

    #[test]
    fn into_shared_a_source_goes_alone() {
        let (here, there) = (objects(), empty());
        let moving = Moving::new(&here, &there, (Part::Source, "fw".into()), "west", "shared");
        let (rows, broken) = moving.list(None);
        assert_eq!(broken, Vec::<String>::new());
        assert_eq!(ids(&rows, |r| r.ticked), [(Part::Source, "fw")]);
        assert_eq!(
            ids(&rows, |r| !r.ticked),
            [(Part::Route, "r-fw")],
            "what names it is listed"
        );
    }

    #[test]
    fn out_of_shared_what_stays_and_names_it_is_needed_and_said_when_unticked() {
        let (here, there) = (objects(), empty());
        let moving = Moving::new(&here, &there, (Part::Destination, "es-net".into()), "shared", "west");
        let only = BTreeSet::from([(Part::Destination, "es-net".to_string())]);
        let (rows, broken) = moving.list(Some(&only));
        assert_eq!(
            broken,
            [
                "Route fw → fw stays in shared and delivers to destination es-net, which shared would no \
              longer have. Tick the route to move it with the destination."
            ]
        );
        assert_eq!(
            ids(&rows, |r| r.needed),
            [(Part::Route, "r-fw"), (Part::Destination, "es-net")]
        );
    }

    #[test]
    fn a_sample_takes_its_tests_and_a_test_its_sample() {
        let (here, there) = (objects(), empty());
        for asked in [(Part::Sample, "s-fw"), (Part::Test, "t-fw")] {
            let moving = Moving::new(&here, &there, (asked.0, asked.1.into()), "west", "shared");
            let (rows, _) = moving.list(None);
            assert_eq!(
                ids(&rows, |r| r.ticked),
                [(Part::Sample, "s-fw"), (Part::Test, "t-fw")],
                "{asked:?}"
            );
        }
    }

    #[test]
    fn what_the_area_moved_to_holds_the_same_is_not_needed() {
        let here = objects();
        let mut there = empty();
        there.file.destinations = here.file.destinations.clone();
        let moving = Moving::new(&here, &there, (Part::Route, "r-fw".into()), "west", "east");
        let (rows, _) = moving.list(None);
        let es_net = rows.iter().find(|r| r.listed.id == "es-net").unwrap();
        assert_eq!((es_net.listed.standing, es_net.ticked), (Standing::Same, false));
    }

    #[test]
    fn what_is_asked_for_beside_takes_its_own_least_too() {
        let (here, there) = (objects(), empty());
        let moving = Moving::new(&here, &there, (Part::Route, "r-fw".into()), "west", "east")
            .also(BTreeSet::from([(Part::Route, "r-web".to_string())]));
        let (rows, broken) = moving.list(None);
        let ticked = ids(&rows, |r| r.ticked);
        for id in ["r-fw", "r-web", "fw-normalize", "web-normalize"] {
            assert!(ticked.iter().any(|(_, t)| *t == id), "{id} goes: {ticked:?}");
        }
        assert!(broken.is_empty(), "{broken:?}");
    }
}
