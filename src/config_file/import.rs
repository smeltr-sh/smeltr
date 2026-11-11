//! Importing an export: what each object it holds is beside what the importing area already has, and the importer's
//! answers applied. An object identical to one here is reused; one whose id holds something else here is taken, and
//! the importer keeps the imported one, keeps the one here, or renames the imported one, which rewrites every
//! reference to it inside the import. Each secret and certificate an imported source or destination names is matched
//! to one here.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::dto::{AnswerDto, FileConfig, OnExceptionDto, SecretRefDto, StepDto};
use super::links::{pattern_name_spans, walk};
use super::sample::SampleDto;
use super::test::TestDto;

/// The kinds of object an export holds. An id is unique only within its kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Part {
    Pipeline,
    LookupTable,
    GrokPattern,
    Schema,
    Sample,
    Test,
    Route,
    Source,
    Destination,
}

/// One object, by its kind and id, that an import or an export leaves out.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PartId {
    pub part: Part,
    pub id: String,
}

/// The objects of a bundle and the samples and tests beside it: what an export holds, or what an area has.
#[derive(Clone, Debug)]
pub struct Objects {
    pub file: FileConfig,
    pub samples: Vec<SampleDto>,
    pub tests: Vec<TestDto>,
    /// Each lookup file's content hash, by the path a table names it at. A table is compared by its file's contents,
    /// not by the name the file is stored under.
    pub contents: BTreeMap<String, String>,
}

/// How an imported object stands beside the importing area.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum Standing {
    /// No object of its kind here has its id.
    New,
    /// One here is identical, and is reused.
    Same,
    /// One here has its id and is different.
    Taken,
}

/// One imported object, how it stands, and, when taken, both versions as JSON text.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Listed {
    pub part: Part,
    pub id: String,
    pub standing: Standing,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub imported: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub here: Option<String>,
    /// The one here is in `shared/`, and the import is into a group: it can be kept or renamed beside, not replaced.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub in_shared: bool,
}

/// The importer's answer for a taken id.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", tag = "keep", content = "as")]
pub enum Keep {
    /// The imported object replaces the one here.
    Imported,
    /// The one here stays, and the imported one is not written.
    Here,
    /// Both: the imported one takes the new id given.
    Renamed(String),
}

/// A secret or certificate the import names, and what names it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Credential {
    pub name: String,
    pub certificate: bool,
    /// Each source or destination that names it, as "Source fw" or "Destination out".
    pub named_by: Vec<String>,
}

impl Objects {
    /// Every object, by kind and id, as the JSON its file holds.
    fn values(&self) -> BTreeMap<(Part, String), serde_json::Value> {
        fn add<T: Serialize>(
            into: &mut BTreeMap<(Part, String), serde_json::Value>,
            part: Part,
            items: &[T],
            id: impl Fn(&T) -> &str,
        ) {
            for item in items {
                let value = serde_json::to_value(item).expect("a config_file DTO serializes");
                into.insert((part, id(item).to_string()), value);
            }
        }
        let (f, mut all) = (&self.file, BTreeMap::new());
        add(&mut all, Part::Pipeline, &f.pipelines, |p| &p.id);
        add(&mut all, Part::LookupTable, &f.lookup_tables, |t| &t.id);
        add(&mut all, Part::GrokPattern, &f.grok_patterns, |g| &g.id);
        add(&mut all, Part::Schema, &f.schemas, |s| &s.id);
        add(&mut all, Part::Sample, &self.samples, |s| &s.id);
        add(&mut all, Part::Test, &self.tests, |t| &t.id);
        add(&mut all, Part::Route, &f.routes, |r| &r.id);
        add(&mut all, Part::Source, &f.sources, |s| &s.id);
        add(&mut all, Part::Destination, &f.destinations, |d| &d.id);
        all
    }

    /// The pipeline that was exported: the one no other pipeline here calls. Listed first.
    pub fn root(&self) -> Option<&str> {
        let mut called = BTreeSet::new();
        for p in &self.file.pipelines {
            walk(&p.steps, &mut |step| {
                if let StepDto::Call { pipeline, .. } = step {
                    called.insert(pipeline.clone());
                }
            });
        }
        let root = self.file.pipelines.iter().find(|p| !called.contains(&p.id));
        root.map(|p| p.id.as_str())
    }

    /// Every object's kind and id.
    pub fn ids(&self) -> BTreeSet<(Part, String)> {
        self.values().into_keys().collect()
    }

    /// `values`, with each lookup table's file named by its content hash, which is what makes two tables the same.
    fn compared(&self) -> BTreeMap<(Part, String), serde_json::Value> {
        // A secret or certificate name belongs to the leader it was made on, and an import matches it to one on this
        // leader, so two sources that differ only in those names are the same source.
        let mut unnamed = self.clone();
        for c in self.credentials() {
            match c.certificate {
                false => unnamed.match_secret(&c.name, &SecretRefDto::WorkerLocal(String::new())),
                true => unnamed.match_certificate(&c.name, ""),
            }
        }
        let mut values = unnamed.values();
        for ((part, _), value) in &mut values {
            let path = value.get("path").and_then(|p| p.as_str());
            if let (Part::LookupTable, Some(hash)) = (part, path.and_then(|p| self.contents.get(p))) {
                value["path"] = serde_json::Value::String(format!("sha256:{hash}"));
            }
        }
        values
    }

    /// Each object of this import, in the order an export lists them, and how it stands beside `here`.
    pub fn list(&self, here: &Objects) -> Vec<Listed> {
        let (here_shown, here) = (here.values(), here.compared());
        let shown = self.values();
        let pretty = |v: &serde_json::Value| serde_json::to_string_pretty(v).expect("a JSON value prints");
        let mut listed: Vec<Listed> = (self.compared().into_iter())
            .map(|((part, id), imported)| {
                let key = (part, id.clone());
                let standing = match here.get(&key) {
                    None => Standing::New,
                    Some(there) if *there == imported => Standing::Same,
                    Some(_) => Standing::Taken,
                };
                let taken = standing == Standing::Taken;
                Listed {
                    here: taken.then(|| pretty(&here_shown[&key])),
                    imported: taken.then(|| pretty(&shown[&key])),
                    part,
                    id,
                    standing,
                    in_shared: false,
                }
            })
            .collect();
        let root = self.root().map(str::to_string);
        listed.sort_by_key(|l| (l.part, Some(&l.id) != root.as_ref(), l.id.clone()));
        listed
    }

    /// What to write: this import with `answers` applied against `here`. Renames are made first, so a rename is
    /// rewritten everywhere the import names it; then what `here` already holds identically, and what the importer
    /// keeps here, are left out. `Err` with every taken id `answers` does not answer, or with a rename to an id that is
    /// taken too.
    pub fn resolve(mut self, here: &Objects, answers: &BTreeMap<(Part, String), Keep>) -> Result<Objects, Vec<String>> {
        let listed = self.list(here);
        let unanswered: Vec<String> = (listed.iter())
            .filter(|l| l.standing == Standing::Taken && !answers.contains_key(&(l.part, l.id.clone())))
            .map(|l| format!("{} {:?} needs an answer: its id is taken here", describe(l.part), l.id))
            .collect();
        if !unanswered.is_empty() {
            return Err(unanswered);
        }
        let (held_here, held) = (here.ids(), self.ids());
        let mut refused = Vec::new();
        for ((part, id), keep) in answers {
            if let Keep::Renamed(to) = keep {
                if to.trim().is_empty()
                    || held_here.contains(&(*part, to.clone()))
                    || held.contains(&(*part, to.clone()))
                {
                    refused.push(format!(
                        "{} {id:?} can't be renamed to {to:?}: that id is taken",
                        describe(*part)
                    ));
                    continue;
                }
                self.rename(*part, id, to);
            }
        }
        if !refused.is_empty() {
            return Err(refused);
        }
        let left_out: BTreeSet<(Part, String)> = (listed.into_iter())
            .filter(|l| match l.standing {
                Standing::Same => true,
                Standing::Taken => answers.get(&(l.part, l.id.clone())) == Some(&Keep::Here),
                Standing::New => false,
            })
            .map(|l| (l.part, l.id))
            .collect();
        self.retain(|part, id| !left_out.contains(&(part, id.to_string())));
        Ok(self)
    }

    /// This import without what `here` already holds identically: what an answer can still write.
    pub fn unlike(&self, here: &Objects) -> Objects {
        let same: BTreeSet<(Part, String)> = (self.list(here).into_iter())
            .filter(|l| l.standing == Standing::Same)
            .map(|l| (l.part, l.id))
            .collect();
        let mut unlike = self.clone();
        unlike.retain(|part, id| !same.contains(&(part, id.to_string())));
        unlike
    }

    /// This import without the objects `left_out` names, which the importer chose not to import.
    pub fn without(&self, left_out: &BTreeSet<(Part, String)>) -> Objects {
        let mut kept = self.clone();
        kept.retain(|part, id| !left_out.contains(&(part, id.to_string())));
        kept
    }

    /// Keeps only the objects `keep` answers true for.
    fn retain(&mut self, keep: impl Fn(Part, &str) -> bool) {
        retain_file(&mut self.file, &keep);
        self.samples.retain(|o| keep(Part::Sample, &o.id));
        self.tests.retain(|o| keep(Part::Test, &o.id));
    }

    /// Gives the `part` with id `from` the id `to`, and rewrites every reference to it in this import.
    pub fn rename(&mut self, part: Part, from: &str, to: &str) {
        let swap = |s: &mut String| {
            if s == from {
                *s = to.to_string();
            }
        };
        let f = &mut self.file;
        match part {
            Part::Pipeline => {
                f.pipelines
                    .iter_mut()
                    .filter(|p| p.id == from)
                    .for_each(|p| p.id = to.to_string());
                for p in &mut f.pipelines {
                    walk_mut(&mut p.steps, &mut |step| {
                        if let StepDto::Call { pipeline, .. } = step {
                            swap(pipeline);
                        }
                    });
                }
                f.routes.iter_mut().for_each(|r| swap(&mut r.pipeline));
                self.tests.iter_mut().filter_map(|t| t.pipeline.as_mut()).for_each(swap);
                self.outcomes_renamed(|s| &mut s.pipeline, from, to);
            }
            Part::LookupTable => {
                f.lookup_tables.iter_mut().for_each(|t| swap(&mut t.id));
                for p in &mut f.pipelines {
                    walk_mut(&mut p.steps, &mut |step| {
                        if let StepDto::Lookup { table, .. } = step {
                            swap(table);
                        }
                    });
                }
            }
            Part::GrokPattern => {
                f.grok_patterns.iter_mut().for_each(|g| swap(&mut g.id));
                for g in &mut f.grok_patterns {
                    g.pattern = renamed_pattern(&g.pattern, from, to);
                }
                for p in &mut f.pipelines {
                    let text = serde_json::to_string(&p.steps).expect("a config_file DTO serializes");
                    let text = renamed_pattern(&text, from, to);
                    p.steps = serde_json::from_str(&text).expect("a pattern name is rewritten inside a JSON string");
                }
            }
            Part::Schema => {
                f.schemas.iter_mut().for_each(|s| swap(&mut s.id));
                f.pipelines.iter_mut().flat_map(|p| &mut p.produces).for_each(swap);
                f.destinations.iter_mut().flat_map(|d| &mut d.accepts).for_each(swap);
            }
            Part::Sample => {
                self.samples.iter_mut().for_each(|s| swap(&mut s.id));
                self.tests.iter_mut().for_each(|t| swap(&mut t.sample));
            }
            Part::Test => self.tests.iter_mut().for_each(|t| swap(&mut t.id)),
            Part::Route => {
                f.routes.iter_mut().for_each(|r| swap(&mut r.id));
                self.outcomes_renamed(|s| &mut s.route, from, to);
            }
            Part::Source => {
                f.sources.iter_mut().for_each(|s| swap(&mut s.id));
                f.routes.iter_mut().for_each(|r| swap(&mut r.source));
                self.samples.iter_mut().filter_map(|s| s.source.as_mut()).for_each(swap);
            }
            Part::Destination => {
                f.destinations.iter_mut().for_each(|d| swap(&mut d.id));
                for r in &mut f.routes {
                    r.deliver.iter_mut().flat_map(|rule| &mut rule.to).for_each(swap);
                    deliveries_mut(&mut r.on_exception).for_each(swap);
                }
                f.destinations
                    .iter_mut()
                    .flat_map(|d| deliveries_mut(&mut d.on_exception))
                    .for_each(swap);
            }
        }
    }

    /// Each end-to-end test's outcomes name the route and pipeline that made them: the one `name` picks naming
    /// `from` is made to name `to`.
    fn outcomes_renamed(&mut self, name: fn(&mut super::Sent) -> &mut String, from: &str, to: &str) {
        let sent = self
            .tests
            .iter_mut()
            .flat_map(|t| t.rows.iter_mut().filter_map(|o| o.sent.as_mut()));
        for named in sent.map(name).filter(|n| *n == from) {
            *named = to.to_string();
        }
    }

    /// Every secret and certificate this import's sources and destinations name, secrets first, each by name.
    pub fn credentials(&self) -> Vec<Credential> {
        let mut found: BTreeMap<(bool, String), Vec<String>> = BTreeMap::new();
        let mut add = |certificate: bool, name: &str, by: String| {
            let named_by = found.entry((certificate, name.to_string())).or_default();
            if !named_by.contains(&by) {
                named_by.push(by);
            }
        };
        for s in &self.file.sources {
            s.secrets()
                .into_iter()
                .for_each(|(_, r)| add(false, r.name(), format!("Source {}", s.id)));
            if let Some(name) = s.tls().and_then(|t| t.certificate) {
                add(true, &name, format!("Source {}", s.id));
            }
        }
        for d in &self.file.destinations {
            d.secrets()
                .into_iter()
                .for_each(|(_, r)| add(false, r.name(), format!("Destination {}", d.id)));
            if let Some(name) = d.tls().and_then(|t| t.certificate) {
                add(true, &name, format!("Destination {}", d.id));
            }
        }
        (found.into_iter())
            .map(|((certificate, name), named_by)| Credential {
                name,
                certificate,
                named_by,
            })
            .collect()
    }

    /// Points every reference to secret `from` at `to`.
    pub fn match_secret(&mut self, from: &str, to: &SecretRefDto) {
        let refs = (self.file.sources.iter_mut().flat_map(|s| s.secrets_mut()))
            .chain(self.file.destinations.iter_mut().flat_map(|d| d.secrets_mut()));
        refs.filter(|r| r.name() == from).for_each(|r| *r = to.clone());
    }

    /// Points every reference to certificate `from` at `to`.
    pub fn match_certificate(&mut self, from: &str, to: &str) {
        fn rewritten<T: Serialize + serde::de::DeserializeOwned>(item: &T, from: &str, to: &str) -> T {
            let mut value = serde_json::to_value(item).expect("a config_file DTO serializes");
            certificate_renamed(&mut value, from, to);
            serde_json::from_value(value).expect("a renamed certificate is the same shape")
        }
        for s in &mut self.file.sources {
            *s = rewritten(s, from, to);
        }
        for d in &mut self.file.destinations {
            *d = rewritten(d, from, to);
        }
    }
}

/// Keeps only the objects of `f` that `keep` answers true for.
pub(super) fn retain_file(f: &mut FileConfig, keep: &impl Fn(Part, &str) -> bool) {
    f.pipelines.retain(|o| keep(Part::Pipeline, &o.id));
    f.lookup_tables.retain(|o| keep(Part::LookupTable, &o.id));
    f.grok_patterns.retain(|o| keep(Part::GrokPattern, &o.id));
    f.schemas.retain(|o| keep(Part::Schema, &o.id));
    f.routes.retain(|o| keep(Part::Route, &o.id));
    f.sources.retain(|o| keep(Part::Source, &o.id));
    f.destinations.retain(|o| keep(Part::Destination, &o.id));
}

/// What a person calls `part`, in a sentence.
pub fn describe(part: Part) -> &'static str {
    match part {
        Part::Pipeline => "pipeline",
        Part::LookupTable => "lookup table",
        Part::GrokPattern => "grok pattern",
        Part::Schema => "schema",
        Part::Sample => "sample",
        Part::Test => "test",
        Part::Route => "route",
        Part::Source => "source",
        Part::Destination => "destination",
    }
}

/// `text` with every `%{from}` and `%{from:` naming `to` instead.
fn renamed_pattern(text: &str, from: &str, to: &str) -> String {
    let (mut renamed, mut copied) = (String::new(), 0);
    for span in pattern_name_spans(text).filter(|span| &text[span.clone()] == from) {
        renamed.push_str(&text[copied..span.start]);
        renamed.push_str(to);
        copied = span.end;
    }
    renamed.push_str(&text[copied..]);
    renamed
}

/// Every `"certificate"` in `value` naming `from`, made to name `to`.
fn certificate_renamed(value: &mut serde_json::Value, from: &str, to: &str) {
    match value {
        serde_json::Value::Object(map) => {
            for (key, v) in map.iter_mut() {
                if key == "certificate" && v.as_str() == Some(from) {
                    *v = serde_json::Value::String(to.to_string());
                } else {
                    certificate_renamed(v, from, to);
                }
            }
        }
        serde_json::Value::Array(items) => items.iter_mut().for_each(|v| certificate_renamed(v, from, to)),
        _ => {}
    }
}

/// Every destination an exception layer delivers to, to rewrite.
fn deliveries_mut(layer: &mut Option<OnExceptionDto>) -> impl Iterator<Item = &mut String> {
    (layer.iter_mut())
        .flat_map(|each| each.categories.values_mut().chain(each.kinds.values_mut()))
        .flat_map(|a| match a {
            AnswerDto::Deliver { to, .. } => to.iter_mut().collect(),
            _ => Vec::new(),
        })
}

/// Every step, an `if` step's branches included, to rewrite.
fn walk_mut(steps: &mut [StepDto], f: &mut impl FnMut(&mut StepDto)) {
    for step in steps {
        f(step);
        if let StepDto::If { then, r#else, .. } = step {
            walk_mut(then, f);
            walk_mut(r#else, f);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// An export of `fw-normalize` with its route: it calls `common`, which reads `sites` and names `%{FW_LINE}`.
    fn imported() -> Objects {
        let file = serde_json::from_value(json!({
            "version": 1,
            "sources": [{"id": "fw", "type": "kafka", "brokers": "b:9092", "topics": ["fw"],
                         "certificate": "kafka-client", "properties": {"security.protocol": "SASL_SSL"},
                         "credentials": {"sasl.password": {"secret": "kafka-password", "version": 3}}}],
            "destinations": [
                {"id": "es-net", "type": "kafka", "brokers": "b:9092", "defaultTopic": "n",
                 "exceptions": {"kinds": {"Refused": {"deliver": ["dlq"]}}}},
                {"id": "dlq", "type": "kafka", "brokers": "b:9092", "defaultTopic": "d"}],
            "pipelines": [
                {"id": "fw-normalize", "name": "fw", "produces": ["network"],
                 "steps": [{"type": "call", "id": "c", "pipeline": "common"}]},
                {"id": "common", "name": "common", "steps": [
                    {"type": "lookup", "id": "site", "input": "_raw", "table": "sites", "matchKind": "exact",
                     "multiplicity": "first", "keyColumn": "ip", "outputs": [{"source": "site", "field": "site"}]},
                    {"id": "line", "type": "parser", "input": "_raw",
                     "kind": {"type": "grok", "patterns": ["%{FW_LINE} %{WORD}"]}}]}],
            "routes": [{"id": "r-fw", "source": "fw", "pipeline": "fw-normalize", "deliver": [{"to": ["es-net"]}]}],
            "lookupTables": [{"id": "sites", "type": "csv", "path": "lookups/sites.csv", "delimiter": ","}],
            "grokPatterns": [{"id": "FW_LINE", "pattern": "%{FW_IP:src} %{WORD:action}"},
                             {"id": "FW_IP", "pattern": "[0-9.]+"}],
            "schemas": [{"id": "network"}]
        }))
        .unwrap();
        Objects {
            file,
            samples: vec![
                serde_json::from_value(json!({"id": "s-fw", "name": "fw", "source": "fw", "records": []})).unwrap(),
            ],
            tests: vec![
                serde_json::from_value(
                    json!({"id": "t-common", "name": "common", "pipeline": "common", "sample": "s-fw",
                                              "lockedAt": 0, "rowsIn": 0, "fields": [], "rows": []}),
                )
                .unwrap(),
                serde_json::from_value(
                    json!({"id": "t-end", "name": "end", "sample": "s-fw", "lockedAt": 0, "rowsIn": 1,
                                              "fields": [], "rows": [{"row": 0, "values": {},
                                                  "sent": {"route": "r-fw", "pipeline": "fw-normalize", "destinations": []}}]}),
                )
                .unwrap(),
            ],
            contents: BTreeMap::new(),
        }
    }

    /// An area that holds `common` as imported, and a different `sites`.
    fn here() -> Objects {
        let mut here = imported();
        here.file.pipelines.retain(|p| p.id == "common");
        here.file.lookup_tables[0].source =
            serde_json::from_value(json!({"type": "csv", "path": "lookups/other-sites.csv", "delimiter": ","}))
                .unwrap();
        here.file.lookup_tables[0].id = "sites".into();
        here.file.grok_patterns.clear();
        here.file.schemas.clear();
        here.file.routes.clear();
        here.file.sources.clear();
        here.file.destinations.clear();
        here.samples.clear();
        here.tests.clear();
        here
    }

    fn standing(listed: &[Listed], part: Part, id: &str) -> Standing {
        listed.iter().find(|l| l.part == part && l.id == id).unwrap().standing
    }

    #[test]
    fn each_object_is_new_the_same_as_one_here_or_taken() {
        let listed = imported().list(&here());
        assert_eq!(standing(&listed, Part::Pipeline, "fw-normalize"), Standing::New);
        assert_eq!(standing(&listed, Part::Pipeline, "common"), Standing::Same);
        assert_eq!(standing(&listed, Part::LookupTable, "sites"), Standing::Taken);
        let sites = listed.iter().find(|l| l.id == "sites").unwrap();
        assert!(sites.imported.as_deref().unwrap().contains("lookups/sites.csv"));
        assert!(sites.here.as_deref().unwrap().contains("lookups/other-sites.csv"));
        // The exported pipeline leads its kind.
        assert_eq!(listed[0].id, "fw-normalize");
    }

    #[test]
    fn nothing_resolves_until_every_taken_id_has_an_answer() {
        let refused = imported().resolve(&here(), &BTreeMap::new()).unwrap_err();
        assert_eq!(
            refused,
            [r#"lookup table "sites" needs an answer: its id is taken here"#]
        );
    }

    #[test]
    fn what_is_the_same_here_or_kept_here_is_not_written() {
        let answers = BTreeMap::from([((Part::LookupTable, "sites".to_string()), Keep::Here)]);
        let written = imported().resolve(&here(), &answers).unwrap();
        let ids: Vec<&str> = written.file.pipelines.iter().map(|p| p.id.as_str()).collect();
        assert_eq!(ids, ["fw-normalize"]);
        assert!(written.file.lookup_tables.is_empty());
    }

    #[test]
    fn a_rename_rewrites_every_reference_in_the_import() {
        let mut objects = imported();
        objects.rename(Part::Pipeline, "fw-normalize", "fw-2");
        objects.rename(Part::LookupTable, "sites", "sites-2");
        objects.rename(Part::GrokPattern, "FW_IP", "FW_ADDR");
        objects.rename(Part::Destination, "dlq", "dlq-2");
        objects.rename(Part::Sample, "s-fw", "s-2");
        objects.rename(Part::Source, "fw", "fw-in");
        let file = serde_json::to_string(&objects.file).unwrap();
        let tests = serde_json::to_string(&objects.tests).unwrap();
        let samples = serde_json::to_string(&objects.samples).unwrap();
        for gone in ["\"fw-normalize\"", "\"sites\"", "FW_IP", "\"dlq\""] {
            assert!(!file.contains(gone), "{gone} in {file}");
        }
        assert!(file.contains("%{FW_ADDR:src}"));
        assert_eq!(objects.file.routes[0].source, "fw-in");
        let sent = objects.tests[1].rows[0].sent.as_ref().unwrap();
        assert_eq!((sent.route.as_str(), sent.pipeline.as_str()), ("r-fw", "fw-2"));
        assert!(!tests.contains("s-fw") && !samples.contains("s-fw") && samples.contains("\"source\":\"fw-in\""));
    }

    #[test]
    fn a_rename_to_a_taken_id_is_refused() {
        let answers = BTreeMap::from([((Part::LookupTable, "sites".to_string()), Keep::Renamed("sites".into()))]);
        let refused = imported().resolve(&here(), &answers).unwrap_err();
        assert_eq!(
            refused,
            [r#"lookup table "sites" can't be renamed to "sites": that id is taken"#]
        );
    }

    #[test]
    fn a_source_that_differs_only_in_the_secrets_and_certificates_it_names_is_the_same() {
        let mut here = here();
        here.file.sources = imported().file.sources;
        here.match_secret("kafka-password", &SecretRefDto::WorkerLocal("KAFKA_FW".into()));
        here.match_certificate("kafka-client", "fw-client");
        assert_eq!(standing(&imported().list(&here), Part::Source, "fw"), Standing::Same);
        here.file.sources[0].id = "other".into();
        here.file
            .sources
            .extend(imported().file.sources.into_iter().map(|mut s| {
                s.id = "fw".into();
                serde_json::from_value(json!({"id": s.id, "type": "kafka", "brokers": "c:9092", "topics": ["fw"]}))
                    .unwrap()
            }));
        assert_eq!(standing(&imported().list(&here), Part::Source, "fw"), Standing::Taken);
    }

    #[test]
    fn credentials_are_listed_by_what_names_them_and_matched_to_ones_here() {
        let mut objects = imported();
        assert_eq!(
            objects.credentials(),
            [
                Credential {
                    name: "kafka-password".into(),
                    certificate: false,
                    named_by: vec!["Source fw".into()]
                },
                Credential {
                    name: "kafka-client".into(),
                    certificate: true,
                    named_by: vec!["Source fw".into()]
                },
            ]
        );
        objects.match_secret("kafka-password", &SecretRefDto::WorkerLocal("KAFKA_PASSWORD".into()));
        objects.match_certificate("kafka-client", "fw-client");
        let source = serde_json::to_value(&objects.file.sources[0]).unwrap();
        assert_eq!(source["credentials"]["sasl.password"], json!("KAFKA_PASSWORD"));
        assert_eq!(source["certificate"], json!("fw-client"));
    }
}
