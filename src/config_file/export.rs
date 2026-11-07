//! What one pipeline's export holds: the pipeline and everything it reads, chosen from the bundle a group runs, and
//! with its routes included, what those read and deliver to.
//!
//! The export is a bundle, so what is chosen here is a `FileConfig` that `bundle_files` writes and `load_bundle_from`
//! reads back. Its manifest carries the format version and nothing of the group's own: the importing group's
//! exception answers and name are its own.

use std::collections::BTreeSet;

use super::dto::FileConfig;
use super::links::{Links, closure, delivered_to};
use super::test::TestSummary;

/// The objects an export holds, by kind, and the files beside them.
#[derive(Debug)]
pub struct Export {
    pub file: FileConfig,
    /// Tests, by id: those that lock a pipeline the export holds and, with routes included, end-to-end tests whose
    /// routes the export holds all of.
    pub tests: Vec<String>,
    /// The samples those tests read, by id.
    pub samples: Vec<String>,
    /// Every secret an exported source or destination names, by name. Only names: a bundle never holds a value.
    pub secrets: BTreeSet<String>,
    /// Every certificate an exported source or destination names, by name.
    pub certificates: BTreeSet<String>,
}

/// What exporting `pipeline` from `merged`, the bundle a group runs, holds. `None` when `merged` has no such
/// pipeline.
pub fn export(merged: &FileConfig, tests: &[TestSummary], pipeline: &str, include_routes: bool) -> Option<Export> {
    merged.pipelines.iter().find(|p| p.id == pipeline)?;
    let links = Links::of(merged);

    let pipelines = links.with_callees(pipeline);
    let tables: BTreeSet<&String> = pipelines
        .iter()
        .flat_map(|p| links.tables.get(p).into_iter().flatten())
        .collect();
    // A built-in pattern is named too, and is left out with every other name the bundle does not declare.
    let named = pipelines
        .iter()
        .flat_map(|p| links.patterns.get(p).into_iter().flatten().cloned());
    let patterns = closure(named, |g| {
        links.pattern_names.get(g).into_iter().flatten().cloned().collect()
    });

    let routes: Vec<_> = match include_routes {
        true => merged.routes.iter().filter(|r| r.pipeline == pipeline).collect(),
        false => Vec::new(),
    };
    let sources: BTreeSet<&String> = routes.iter().map(|r| &r.source).collect();
    // The route's own deliveries and the chains of refusals after them. The group's exception answers stay behind,
    // so what only they deliver to does too.
    let destinations = links.refusal_chain((routes.iter()).flat_map(|r| {
        r.deliver
            .iter()
            .flat_map(|rule| rule.to.iter().cloned())
            .chain(delivered_to(&r.on_exception))
    }));

    let mut file = FileConfig {
        version: merged.version,
        sources: (merged.sources.iter())
            .filter(|s| sources.contains(&s.id))
            .cloned()
            .collect(),
        destinations: (merged.destinations.iter())
            .filter(|d| destinations.contains(&d.id))
            .cloned()
            .collect(),
        pipelines: (merged.pipelines.iter())
            .filter(|p| pipelines.contains(&p.id))
            .cloned()
            .collect(),
        routes: routes.into_iter().cloned().collect(),
        lookup_tables: (merged.lookup_tables.iter())
            .filter(|t| tables.contains(&t.id))
            .cloned()
            .collect(),
        grok_patterns: (merged.grok_patterns.iter())
            .filter(|g| patterns.contains(&g.id))
            .cloned()
            .collect(),
        schemas: Vec::new(),
        on_exception: None,
        name: None,
    };
    // The pipeline asked for first: it is what the export is of, and the rest are what it calls.
    file.pipelines.sort_by_key(|p| p.id != pipeline);
    let schemas: BTreeSet<&String> = (file.pipelines.iter().flat_map(|p| &p.produces))
        .chain(file.destinations.iter().flat_map(|d| &d.accepts))
        .collect();
    file.schemas = (merged.schemas.iter())
        .filter(|s| schemas.contains(&s.id))
        .cloned()
        .collect();

    let route_ids: BTreeSet<&String> = file.routes.iter().map(|r| &r.id).collect();
    let tests: Vec<&TestSummary> = (tests.iter())
        .filter(|t| match &t.pipeline {
            Some(p) => pipelines.contains(p),
            None => !t.routes.is_empty() && t.routes.iter().all(|r| route_ids.contains(r)),
        })
        .collect();
    let samples: BTreeSet<String> = tests.iter().map(|t| t.sample.clone()).collect();

    let secrets = (file.sources.iter().flat_map(|s| s.secrets()))
        .chain(file.destinations.iter().flat_map(|d| d.secrets()))
        .map(|(_, r)| r.name().to_string())
        .collect();
    let certificates = (file.sources.iter().filter_map(|s| s.tls()?.certificate))
        .chain(file.destinations.iter().filter_map(|d| d.tls()?.certificate))
        .collect();

    Some(Export {
        file,
        tests: tests.into_iter().map(|t| t.id.clone()).collect(),
        samples: samples.into_iter().collect(),
        secrets,
        certificates,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Two firewalls through one normalizer that calls a common one, and a web log beside them.
    ///
    /// ```text
    ///   fw ── r-fw ── fw-normalize ─ calls ─ common (reads sites, names %{FW_LINE} -> %{FW_IP})
    ///                     └─ es-net ─ refused ─ dlq
    ///   web ─ r-web ─ web-normalize ── es-web
    /// ```
    fn bundle() -> FileConfig {
        let kafka_dest = |id: &str, dlq: Option<&str>| {
            let mut d = json!({"id": id, "type": "kafka", "brokers": "b:9092", "defaultTopic": id});
            if let Some(dlq) = dlq {
                d["exceptions"] = json!({"kinds": {"Refused": {"deliver": [dlq]}}});
            }
            d
        };
        serde_json::from_value(json!({
            "version": 1,
            "sources": [
                {"id": "fw", "type": "kafka", "brokers": "b:9092", "topics": ["fw"], "certificate": "kafka-client", "properties": {"security.protocol": "SASL_SSL"},
                 "credentials": {"sasl.password": {"secret": "kafka-password", "version": 3}}},
                {"id": "web", "type": "kafka", "brokers": "b:9092", "topics": ["web"],
                 "credentials": {"sasl.password": "WEB_PASSWORD"}}],
            "destinations": [kafka_dest("es-net", Some("dlq")), kafka_dest("es-web", None), kafka_dest("dlq", None),
                             kafka_dest("leftovers", None)],
            "pipelines": [
                {"id": "fw-normalize", "name": "fw", "produces": ["network"],
                 "steps": [{"type": "call", "id": "c", "pipeline": "common"}]},
                {"id": "common", "name": "common", "steps": [
                    {"type": "lookup", "id": "site", "input": "_raw", "table": "sites", "matchKind": "exact",
                     "multiplicity": "first", "keyColumn": "ip", "outputs": [{"source": "site", "field": "site"}]},
                    {"id": "line", "type": "parser", "input": "_raw",
                     "kind": {"type": "grok", "patterns": ["%{FW_LINE} %{WORD}"]}}]},
                {"id": "web-normalize", "name": "web", "produces": ["web"], "steps": []}
            ],
            "routes": [
                {"id": "r-fw", "source": "fw", "pipeline": "fw-normalize", "deliver": [{"to": ["es-net"]}]},
                {"id": "r-web", "source": "web", "pipeline": "web-normalize", "deliver": [{"to": ["es-web"]}]}],
            "lookupTables": [{"id": "sites", "type": "csv", "path": "lookups/sites.csv", "delimiter": ",", "headers": ["ip", "site"]},
                             {"id": "users", "type": "csv", "path": "lookups/users.csv", "delimiter": ","}],
            "grokPatterns": [{"id": "FW_LINE", "pattern": "%{FW_IP:src} %{WORD:action}"}, {"id": "FW_IP", "pattern": "[0-9.]+"},
                             {"id": "UNUSED", "pattern": "x"}],
            "schemas": [{"id": "network"}, {"id": "web"}],
            "exceptions": {"kinds": {"Unmatched": {"deliver": ["leftovers"]}}}
        }))
        .unwrap()
    }

    fn test(id: &str, pipeline: Option<&str>, sample: &str, routes: &[&str]) -> TestSummary {
        TestSummary {
            id: id.into(),
            name: id.into(),
            pipeline: pipeline.map(Into::into),
            sample: sample.into(),
            rows_in: 1,
            rows_out: 1,
            pipelines: Vec::new(),
            routes: routes.iter().map(|r| r.to_string()).collect(),
        }
    }

    fn tests() -> Vec<TestSummary> {
        vec![
            test("t-common", Some("common"), "s-fw", &[]),
            test("t-web", Some("web-normalize"), "s-web", &[]),
            test("t-fw-end", None, "s-fw-end", &["r-fw"]),
            test("t-both-end", None, "s-mixed", &["r-fw", "r-web"]),
        ]
    }

    fn ids<T>(items: &[T], id: impl Fn(&T) -> &str) -> Vec<String> {
        items.iter().map(|i| id(i).to_string()).collect()
    }

    #[test]
    fn a_pipeline_alone_includes_what_it_reads_however_deep() {
        let e = export(&bundle(), &tests(), "fw-normalize", false).unwrap();
        assert_eq!(ids(&e.file.pipelines, |p| &p.id), ["fw-normalize", "common"]);
        assert_eq!(ids(&e.file.lookup_tables, |t| &t.id), ["sites"]);
        // WORD is a built-in, so the export includes only the declared patterns, through FW_LINE down to FW_IP.
        assert_eq!(ids(&e.file.grok_patterns, |g| &g.id), ["FW_LINE", "FW_IP"]);
        assert_eq!(ids(&e.file.schemas, |s| &s.id), ["network"]);
        assert!(e.file.routes.is_empty() && e.file.sources.is_empty() && e.file.destinations.is_empty());
        assert_eq!(e.tests, ["t-common"]);
        assert_eq!(e.samples, ["s-fw"]);
        assert!(e.secrets.is_empty() && e.certificates.is_empty());
    }

    #[test]
    fn the_group_s_own_settings_stay_behind() {
        let e = export(&bundle(), &tests(), "fw-normalize", true).unwrap();
        assert!(e.file.on_exception.is_none() && e.file.name.is_none());
        assert_eq!(e.file.version, bundle().version);
    }

    #[test]
    fn routes_include_their_source_destinations_refusals_and_credential_names() {
        let e = export(&bundle(), &tests(), "fw-normalize", true).unwrap();
        assert_eq!(ids(&e.file.routes, |r| &r.id), ["r-fw"]);
        assert_eq!(ids(&e.file.sources, |s| &s.id), ["fw"]);
        // `leftovers` is delivered to only by the group's own answers, which stay behind.
        assert_eq!(ids(&e.file.destinations, |d| &d.id), ["es-net", "dlq"]);
        // The export holds an end-to-end test only when it holds every route the test covers.
        assert_eq!(e.tests, ["t-common", "t-fw-end"]);
        assert_eq!(e.samples, ["s-fw", "s-fw-end"]);
        assert_eq!(e.secrets.into_iter().collect::<Vec<_>>(), ["kafka-password"]);
        assert_eq!(e.certificates.into_iter().collect::<Vec<_>>(), ["kafka-client"]);
    }

    #[test]
    fn a_pipeline_the_bundle_lacks_exports_nothing() {
        assert!(export(&bundle(), &tests(), "missing", true).is_none());
    }

    #[test]
    fn what_is_chosen_loads_as_a_bundle_of_its_own() {
        for include_routes in [false, true] {
            let e = export(&bundle(), &tests(), "fw-normalize", include_routes).unwrap();
            let text = serde_json::to_string(&e.file).unwrap();
            // The lookup files are not here, so only what is wrong with the rest counts.
            let errors: Vec<String> = (crate::config_file::validation_errors(serde_json::from_str(&text).unwrap()))
                .into_iter()
                .map(|e| e.to_string())
                .filter(|e| !e.contains("lookups/"))
                .collect();
            assert!(errors.is_empty(), "{errors:?}");
        }
    }
}
