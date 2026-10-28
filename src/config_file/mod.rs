//! The config file: JSON in, a runnable topology out.
//!
//! `plan::config`/`plan::topology` key everything by small integer ids, which is what the compiler and the per-row
//! destination bitmask need and what a file must not use: an integer id shifts when a source is inserted mid-list.
//! `dto` is the file's own shape, keyed by string ids the GUI assigns and keeps stable; `load` converts it to the
//! compiler's shape, allocating integer ids deterministically. That direction is the only one: a save edits the DTOs
//! it read and writes them back, so the file's shape is never reconstructed from the compiler's.
//!
//! Every source, destination, pipeline, route and step has a string `id`, unique within its scope. Array order is
//! cosmetic for sources, destinations and pipelines; it is semantic within one pipeline's `steps` and one source's
//! `routes`.
//!
//! `"version"` is required and must be `1`.
//!
//! Every write path calls [`validate`], so what is stored is always runnable: the GUI save through `validation_errors`,
//! the bundle upload and standalone seeding through the bundle loaders.

mod bundle;
pub mod bytes;
mod dto;
#[cfg(test)]
mod fuzz_delta;
#[cfg(test)]
mod fuzz_event_hubs;
#[cfg(test)]
mod fuzz_hec_receiver;
#[cfg(test)]
mod fuzz_iceberg;
#[cfg(test)]
mod fuzz_loader;
mod impact;
mod links;
mod load;
mod merge;
mod sample;
mod test;

use std::collections::HashMap;
use std::sync::Arc;

use crate::core::column::DataType;
use crate::core::error::ExceptionKind;
use crate::core::id::{DestId, FunctionId};
use crate::plan::config::{Answer, CompileError, PipelineConfig};
use crate::plan::topology::{CompiledTopology, DestinationKind, Records, TopologyConfig};

pub use bundle::{
    BundleError, DirFileSet, FileSet, GROK_PATTERNS_FILE, GROUPS_DIR, LOOKUP_TABLES_FILE, LOOKUPS_DIR, PIPELINES_DIR,
    RawBundle, SCHEMAS_DIR, SECTION_FILES, SHARED_DIR, bundle_files, group_subtree, is_safe_group_id,
    is_safe_group_name, is_safe_secret_name, load_bundle_dir, load_bundle_from, load_group_dir, load_group_from,
    manifest_bytes, read_raw_from, slugged_pipelines, slugged_schemas, slugify, unsafe_group_id, unsafe_group_name,
    unsafe_secret_name,
};
pub use dto::{
    AnswerDto, AssignmentDto, BufferBytes, CompoundTypeDto, CustomPatternDto, DataTypeDto, DeliverRuleDto,
    DestinationDto, DestinationKindDto, FetchBytes, FieldDto, FileConfig, GrokPatternDto, LeaderSecretDto,
    LookupOutputDto, LookupSourceDto, LookupTableDto, MatchKindDto, MultiplicityDto, OnExceptionDto, PUBSUB_ENDPOINT,
    ParserKindDto, PipelineDto, RenamePairDto, RouteDto, SchemaDto, SchemaFieldDto, SecretRefDto, SourceDto,
    SourceKindDto, SqlKind, SqlSourceDto, StartPositionDto, StepDto,
};
pub use impact::{Impact, Reached, Reason, pipeline_answers_differ, pipelines_touched};
pub use links::{Connections, Kind, Links, Object};
pub use load::{
    BundleAt, DeclaredAt, ItemRef, LoadError, ValidationError, check_shared_group_starts, fill_default_group_ids,
    line_breaker_refusal, load_str, load_str_at, on_loopback, plain_off_this_host, refuse_outside_lookups,
    sql_connection, to_byte, validation_errors, validation_errors_at, web_url,
};
pub use merge::{ObjectConflict, SectionMerge, Side, is_section_file, merge_section, text_merge_keeps_each_side};
pub use sample::{
    MAX_SAMPLE_RECORDS, SAMPLES_DIR, SampleDto, SampleSummary, list_samples, read_sample, record_source,
    refuse_sample_size, sample_bytes, sample_path,
};
pub use test::{
    Difference, RowsIn, TESTS_DIR, TestDto, TestSummary, Verdict, compare, list_tests, read_test, test_bytes, test_path,
};

/// The `"version"` this build writes and reads. A change to the format bumps it and adds a converter from the
/// version before it, so a file nobody has saved since still loads (`docs/architecture/control-plane.md`, Upgrades).
pub const CURRENT_VERSION: u32 = 1;

/// Structural resolution, then compilation. The one check every write path runs.
pub fn validate(file: FileConfig) -> Result<LoadedSystem, LoadError> {
    validate_at(file, &load::DeclaredAt::new())
}

/// `validate` for a bundle read from a directory: `declared_at` says where each lookup table was declared, which is
/// what a relative lookup path resolves against.
pub fn validate_at(file: FileConfig, declared_at: &load::DeclaredAt) -> Result<LoadedSystem, LoadError> {
    let system = load::from_file_config_at(file, declared_at)?;
    system
        .compile_topology()
        .map_err(|errs| LoadError::Validation(errs.into_iter().map(|e| system.to_validation_error(e)).collect()))?;
    Ok(system)
}

/// A pipeline's rows breaking a schema it produces, or a destination it delivers to accepting none it does.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaFinding {
    /// The pipeline's id.
    pub pipeline: String,
    /// The step that wrote the field, by its id. `None` for a field no step writes, and for a destination's.
    pub step: Option<String>,
    pub message: String,
}

/// The compiler's inputs, with every string id resolved to an integer id. Not yet compiled: `load_str` stops here so
/// the preview endpoint can list pipelines whose expressions do not compile.
#[derive(Debug, Default)]
pub struct LoadedSystem {
    pub topology: TopologyConfig,
    pub pipelines: Vec<PipelineConfig>,
    /// `PipelineConfig::id` (a compiler-local index) back to the DTO's string id, so a `CompileError::pipeline`
    /// can be reported the way the file that declared it names it.
    pub pipeline_ids: Vec<String>,
    /// The schemas each pipeline produces, by `PipelineConfig::id`.
    pub produces: Vec<Vec<Arc<crate::schema::Schema>>>,
    /// The schemas each destination accepts, for the destinations that name any.
    pub accepts: HashMap<DestId, Vec<Arc<crate::schema::Schema>>>,
}

impl LoadedSystem {
    /// Compile every pipeline and route. Expression errors surface here, as a `CompileError` naming the function.
    pub fn compile_topology(&self) -> Result<CompiledTopology, Vec<CompileError>> {
        let metas = self.topology.sources.iter().map(|s| (s.id, s.kind.meta())).collect();
        let compiled = crate::plan::topology::compile_topology_with(
            &self.topology.routes,
            &self.pipelines,
            self.topology.on_exception.clone(),
            &metas,
        )?;
        let mut compiled = compiled;
        let named: std::collections::HashMap<DestId, Vec<DestId>> = self
            .topology
            .destinations
            .iter()
            .filter(|d| !d.on_reject.targets().is_empty())
            .map(|d| (d.id, d.on_reject.targets().to_vec()))
            .collect();
        compiled.dlq_of = named
            .keys()
            .map(|&d| (d, crate::plan::topology::dlq_chain(&named, d)))
            .collect();
        let mut errors = self.check_doc_id_sources(&compiled);
        errors.extend(self.check_bytes_fields(&compiled));
        let (records, refused) = self.parquet_records(&compiled);
        compiled.records = records;
        compiled.payloads = self
            .topology
            .destinations
            .iter()
            .map(|d| (d.id, d.kind.payload()))
            .collect();
        errors.extend(refused);
        if errors.is_empty() { Ok(compiled) } else { Err(errors) }
    }

    /// A `CompileError`'s `pipeline` and `function` are the compiler's indices; this is what a caller reports, keyed
    /// by the ids the file itself uses.
    fn to_validation_error(&self, e: CompileError) -> load::ValidationError {
        // A route's error is about its condition, or about one of its rules' `when`.
        let route = e.route.and_then(|i| self.topology.routes.get(i));
        load::ValidationError {
            pipeline: e.pipeline.and_then(|id| self.pipeline_ids.get(id).cloned()),
            step: self.step_id_for(e.pipeline, e.function),
            function: e.function,
            assignment_index: e.assignment_index,
            item: route.map(|r| load::ItemRef {
                section: "routes",
                id: r.id.clone(),
            }),
            field: route.map(|_| if e.rule.is_some() { "deliver.when" } else { "condition" }),
            rule: route.and(e.rule),
            ..load::ValidationError::from(e.message)
        }
    }

    /// `answer` as the file says it, with each destination by its author's id.
    pub fn answer_by_name(&self, answer: &Answer) -> AnswerDto {
        match answer {
            Answer::Ignore => AnswerDto::Ignore,
            Answer::Tag => AnswerDto::Tag,
            Answer::Drop => AnswerDto::Drop,
            Answer::Block => AnswerDto::Block,
            Answer::Deliver(targets) => AnswerDto::Deliver {
                to: targets
                    .iter()
                    .map(|&to| {
                        self.topology
                            .destinations
                            .iter()
                            .find(|d| d.id == to)
                            .expect("a loaded system's answer names destinations it has")
                            .name
                            .clone()
                    })
                    .collect(),
            },
        }
    }

    /// The pipeline the file names `id`.
    pub fn pipeline(&self, id: &str) -> Option<&PipelineConfig> {
        let index = self.pipeline_ids.iter().position(|p| p == id)?;
        self.pipelines.iter().find(|p| p.id == index)
    }

    /// The schemas the pipeline `PipelineConfig::id` names produces.
    pub fn produced_by(&self, pipeline: usize) -> &[Arc<crate::schema::Schema>] {
        self.produces.get(pipeline).map_or(&[], Vec::as_slice)
    }

    /// The step a `CompileError`'s `pipeline`/`function` indices name, by its own string id. `function` is a
    /// position local to one pipeline and, once a `call` step has inlined another pipeline's steps into it, no
    /// longer lines up with document order the way it did before expansion; the id does not move. `None` when
    /// either index is absent or does not resolve -- an unattributed error, or an index out of range.
    pub fn step_id_for(&self, pipeline: Option<usize>, function: Option<FunctionId>) -> Option<String> {
        let pipeline_id = pipeline?;
        let function_id = function?;
        self.pipelines
            .iter()
            .find(|p| p.id == pipeline_id)?
            .step_ids
            .get(function_id as usize)
            .cloned()
    }

    /// Where a pipeline's rows break the schemas it produces, and where it delivers to a destination that accepts
    /// none of them, over every route `compiled` runs it on. A finding never refuses the system.
    pub fn schema_findings(&self, compiled: &CompiledTopology) -> Vec<SchemaFinding> {
        let mut out: Vec<SchemaFinding> = Vec::new();
        let mut push = |f: SchemaFinding| {
            if !out.contains(&f) {
                out.push(f);
            }
        };
        for route in &compiled.routes {
            let pipeline = self.pipeline_ids.get(route.pipeline).cloned().unwrap_or_default();
            let produces = self.produced_by(route.pipeline);
            if let Some(plan) = compiled.plan_for(route.source, route.pipeline)
                && !produces.is_empty()
            {
                let record = crate::plan::dictionary::record_type(plan, &route.on_exception.tag_field);
                // A field the source gave is the first step's to fix, which the record enters through.
                let first = (self.pipelines.iter().find(|p| p.id == route.pipeline))
                    .and_then(|p| p.body.first())
                    .map(|step| match step {
                        crate::plan::config::Step::Function(f) => f.id,
                        crate::plan::config::Step::If { id, .. } => *id,
                    });
                let findings = crate::schema::check(produces, &record);
                for (keys, message) in crate::schema::told(produces, &findings) {
                    let at = keys.and_then(|keys| crate::plan::dictionary::writer(plan, &keys).or(first));
                    push(SchemaFinding {
                        pipeline: pipeline.clone(),
                        step: self.step_id_for(Some(route.pipeline), at),
                        message,
                    });
                }
            }
            for dest in route.deliver.iter().flat_map(|rule| &rule.to) {
                let Some(accepts) = self.accepts.get(dest) else {
                    continue;
                };
                if accepts.iter().any(|a| produces.iter().any(|p| p.id == a.id)) {
                    continue;
                }
                let name = (self.topology.destinations.iter().find(|d| d.id == *dest)).map_or("", |d| d.name.as_str());
                let titles: Vec<&str> = accepts.iter().map(|a| a.title.as_str()).collect();
                push(SchemaFinding {
                    pipeline: pipeline.clone(),
                    step: None,
                    message: format!(
                        "destination {name:?} accepts {}, but pipeline {pipeline:?} produces no schema it accepts",
                        titles.join(", ")
                    ),
                });
            }
        }
        out
    }

    /// What each destination that writes columns writes, route by route, and an error for each record it cannot hold.
    /// An Iceberg or Delta destination writes Parquet, and a destination writing Avro or CSV writes Parquet's columns
    /// as Avro (`io::avro`) or CSV (`io::csv`).
    fn parquet_records(&self, compiled: &CompiledTopology) -> (HashMap<DestId, Records>, Vec<CompileError>) {
        let parquet: Vec<_> = self
            .topology
            .destinations
            .iter()
            .filter(|d| match &d.kind {
                DestinationKind::Objects(o) => o.format.writes_columns(),
                DestinationKind::Iceberg(_) | DestinationKind::Delta(_) => true,
                DestinationKind::Kafka(k) => k.avro.is_some(),
                _ => false,
            })
            .collect();
        let mut records: HashMap<DestId, Records> = HashMap::new();
        let mut write = |dests: Vec<DestId>, route: Option<u32>, record: &DataType| {
            for dest in dests.into_iter().filter(|d| parquet.iter().any(|p| p.id == *d)) {
                records.entry(dest).or_default().insert(route, record.clone());
            }
        };
        for (r, route) in compiled.routes.iter().enumerate() {
            let Some(plan) = compiled.plan_for(route.source, route.pipeline) else {
                continue; // `compile_topology` already reported this
            };
            let record = crate::plan::dictionary::record_type(plan, &route.on_exception.tag_field);
            write(compiled.written_by(route), Some(r as u32), &record);
        }
        // The group's answers deliver a row no pipeline ran on as `{_raw: str}` (`runtime::source_form`).
        let unrouted = [ExceptionKind::Unmatched, ExceptionKind::Unprocessable]
            .iter()
            .flat_map(|k| compiled.on_exception.answer(k).targets())
            .copied()
            .collect();
        write(
            compiled.with_dlqs(unrouted),
            None,
            &DataType::Struct(vec![("_raw".to_string(), DataType::Str)], false),
        );
        let errors = parquet
            .iter()
            .flat_map(|d| {
                records
                    .get(&d.id)
                    .into_iter()
                    .flat_map(|r| r.values())
                    .map(move |r| (d, r))
            })
            .filter_map(|(d, record)| {
                let format = match &d.kind {
                    DestinationKind::Kafka(_) => "Avro",
                    DestinationKind::Objects(o) => o.format.column_format().unwrap_or("Parquet"),
                    _ => "Parquet",
                };
                let why = match crate::io::parquet::Schema::new(record) {
                    Ok(schema) if format == "Avro" => crate::io::avro::schema_of(schema.arrow()).err()?,
                    Ok(_) => return None,
                    Err(why) => why,
                };
                Some(CompileError::new(
                    None,
                    format!("destination `{}` cannot write its rows as {format}: {why}", d.name),
                ))
            })
            .collect();
        (records, errors)
    }

    /// Every destination writing a field's bytes, and every DLQ down its chain, takes that field from each source
    /// that reaches it, so each such source declares it, as `bytes`.
    fn check_bytes_fields(&self, compiled: &CompiledTopology) -> Vec<CompileError> {
        let mut errors = Vec::new();
        for source in &self.topology.sources {
            let meta = source.kind.meta();
            for to in compiled.destinations_reachable_from(source.id) {
                let Some(dest) = self.topology.destinations.iter().find(|d| d.id == to) else {
                    continue;
                };
                let crate::plan::config::Payload::Bytes { field } = dest.kind.payload() else {
                    continue;
                };
                let (name, dest_name) = (&source.name, &dest.name);
                let why = match meta.field_type(&field) {
                    Some(crate::core::column::DataType::Bytes) => continue,
                    Some(ty) => format!("which `{name}` declares as {ty}, not bytes"),
                    None => format!("a field `{name}` does not declare"),
                };
                errors.push(CompileError::new(
                    None,
                    format!(
                        "source `{name}` reaches destination `{dest_name}`, which writes the bytes of `{field}`, {why}"
                    ),
                ));
            }
        }
        errors
    }

    /// A destination that keys documents on `__destination.id` must be handed one: every route delivering to it needs a pipeline
    /// that binds the field. Otherwise every document gets a generated `_id`, which is the at-least-once behavior
    /// `docId` was set to avoid, silently.
    ///
    /// Checked against the leaves of `Plan::tree`, so a `__destination.id` bound inside a `Code` block counts, and over the
    /// compiled routes. Where an exception delivers a row is exempt: that row is a failure on its way to be looked at,
    /// not a document the destination dedupes.
    fn check_doc_id_sources(&self, compiled: &CompiledTopology) -> Vec<CompileError> {
        // A destination naming its own `idField` needs nothing from any pipeline.
        let wants: Vec<_> = self
            .topology
            .destinations
            .iter()
            .filter(|d| d.kind.wants_doc_id() && d.kind.id_field().is_none())
            .map(|d| (d.id, d.name.clone()))
            .collect();
        if wants.is_empty() {
            return Vec::new();
        }

        let mut errors = Vec::new();
        for (r, route) in compiled.routes.iter().enumerate() {
            let Some(plan) = compiled.plan_for(route.source, route.pipeline) else {
                continue; // `compile_topology` already reported this
            };
            if plan.tree.leaf("__destination.id").is_some() {
                continue;
            }
            // A delivered destination's DLQ takes the same document, identity and all.
            let mut written: Vec<DestId> = route.deliver.iter().flat_map(|rule| rule.to.iter().copied()).collect();
            let salvage: Vec<DestId> = written
                .iter()
                .filter_map(|d| compiled.dlq_of.get(d))
                .flatten()
                .copied()
                .collect();
            written.extend(salvage);
            written.sort_unstable();
            written.dedup();
            {
                for dest in &written {
                    let Some((_, name)) = wants.iter().find(|(id, _)| id == dest) else {
                        continue;
                    };
                    errors.push(CompileError::new(
                        None,
                        format!(
                            "route {r} writes to destination `{name}`, which is configured to \
                             key documents on a stable id, but names no `idField` and its \
                             pipeline never sets `__destination.id`. Every document would get a generated \
                             id instead, which is the plain at-least-once behavior `docId` was \
                             set to avoid -- give that destination an `idField` naming the field \
                             the identity lives in, or set `__destination.id` in the pipeline if it has \
                             to be computed, or set `docId` back to `auto`"
                        ),
                    ));
                }
            }
        }
        errors
    }
}

#[cfg(test)]
pub(crate) mod test_fixtures {
    /// One of everything: both source kinds, two destinations, a two-pipeline topology with a conditional route and a
    /// DLQ policy, and every step kind including a nested `if`.
    pub(crate) fn example_json() -> &'static str {
        include_str!("../../tests/fixtures/pipeline.json")
    }
}

#[cfg(test)]
mod tests {
    use super::test_fixtures::example_json;
    use super::*;

    #[test]
    fn an_unsupported_version_is_rejected() {
        let json = example_json().replacen("\"version\": 1", "\"version\": 2", 1);
        let err = load_str(&json).unwrap_err();
        assert!(matches!(err, LoadError::UnsupportedVersion(2)));
    }

    #[test]
    fn malformed_json_reports_a_position() {
        let err = load_str("{ not json").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("line"), "{msg}");
    }

    // -- validation errors: pipeline/function attribution -------------------

    /// Two functions per pipeline, the second an unparseable expression -- what a save compiles, since it validates
    /// every pipeline together.
    fn two_pipeline_json(second_expr: &str) -> String {
        format!(
            r#"{{
              "version": 1,
              "pipelines": [
                {{
                  "id": "main", "name": "main",
                  "steps": [
                    {{ "id": "c1", "type": "comment", "text": "noop" }},
                    {{ "id": "e1", "type": "eval", "assignments": [{{ "target": "x", "expr": "1" }}] }}
                  ]
                }},
                {{
                  "id": "second", "name": "second",
                  "steps": [
                    {{ "id": "c2", "type": "comment", "text": "noop" }},
                    {{ "id": "e2", "type": "eval", "assignments": [{{ "target": "x", "expr": {second_expr:?} }}] }}
                  ]
                }}
              ]
            }}"#
        )
    }

    fn validation_of(json: &str) -> Vec<ValidationError> {
        let file: FileConfig = serde_json::from_str(json).unwrap();
        let LoadError::Validation(errors) = validate(file).unwrap_err() else {
            panic!("expected a Validation error");
        };
        errors
    }

    /// One route from `k` through `p` to `out`, its condition `condition` and its one rule's `when` `when`.
    fn route_json(condition: &str, when: &str) -> String {
        format!(
            r#"{{
              "version": 1,
              "sources": [{{ "id": "k", "type": "kafka", "brokers": "b:9092", "topics": ["t"], "groupId": "g" }}],
              "destinations": [{{ "id": "out", "type": "kafka", "brokers": "b:9092", "defaultTopic": "t" }}],
              "pipelines": [{{ "id": "p", "name": "p", "steps": [] }}],
              "routes": [{{ "id": "r", "source": "k", "condition": {condition:?}, "pipeline": "p",
                          "deliver": [{{ "to": [] }}, {{ "when": {when:?}, "to": ["out"] }}] }}]
            }}"#
        )
    }

    #[test]
    fn a_pipelines_schema_findings_are_at_its_steps_and_its_destinations() {
        let json = r#"{
          "version": 1,
          "sources": [{ "id": "k", "type": "kafka", "brokers": "b:9092", "topics": ["t"], "groupId": "g" }],
          "schemas": [
            { "id": "stamped", "title": "Stamped", "fields": [{ "field": "@timestamp", "type": "datetime" }] },
            { "id": "login", "title": "Login" }
          ],
          "destinations": [{ "id": "out", "type": "kafka", "brokers": "b:9092", "defaultTopic": "t",
                             "accepts": ["login"] }],
          "pipelines": [{ "id": "p", "name": "p", "produces": ["stamped"],
                          "steps": [{ "id": "s", "type": "code", "body": "$[\"@timestamp\"] = \"now\";" }] }],
          "routes": [{ "id": "r", "source": "k", "pipeline": "p", "deliver": [{ "to": ["out"] }] }]
        }"#;
        let system = load_str(json).unwrap();
        let compiled = system.compile_topology().unwrap();
        let found: Vec<_> = (system.schema_findings(&compiled).into_iter())
            .map(|f| (f.pipeline, f.step, f.message))
            .collect();
        assert_eq!(
            found,
            [
                (
                    "p".to_string(),
                    Some("s".to_string()),
                    "`@timestamp` is str, but Stamped has it as datetime. Write it as datetime.".to_string()
                ),
                (
                    "p".to_string(),
                    None,
                    "destination \"out\" accepts Login, but pipeline \"p\" produces no schema it accepts".to_string()
                ),
            ]
        );
    }

    #[test]
    fn a_route_compile_error_names_its_route_and_its_condition_or_rule() {
        let route = Some(ItemRef {
            section: "routes",
            id: "r".into(),
        });
        let errors = validation_of(&route_json("_raw", "true"));
        assert!(
            errors
                .iter()
                .any(|e| e.item == route && e.field == Some("condition") && e.rule.is_none()),
            "{errors:?}"
        );
        let errors = validation_of(&route_json("true", "_raw"));
        assert!(
            errors
                .iter()
                .any(|e| e.item == route && e.field == Some("deliver.when") && e.rule == Some(1)),
            "{errors:?}"
        );
    }

    #[test]
    fn a_pipeline_compile_error_names_its_pipeline_function_and_step() {
        let errors = validation_of(&two_pipeline_json("1 +"));
        assert!(
            errors.iter().any(|e| e.pipeline.as_deref() == Some("second")
                && e.function == Some(1)
                && e.step.as_deref() == Some("e2")),
            "{errors:?}"
        );
    }

    #[test]
    fn an_error_in_one_pipeline_is_not_attributed_to_another() {
        // Both pipelines assign FunctionId 1 to their second step -- a bare function id would be
        // ambiguous between them without the pipeline it came from.
        let errors = validation_of(&two_pipeline_json("1 +"));
        assert!(
            errors.iter().all(|e| e.pipeline.as_deref() != Some("main")),
            "an error from `second` landed on `main`: {errors:?}"
        );
    }

    /// `main` calls `callee`, whose second step is the broken one. `expand_calls` splices `callee`'s steps into
    /// `main` before numbering, so the broken step's `FunctionId` (2: comment, then `callee`'s two steps) is a
    /// position in `main`'s expanded body, not in `callee` alone -- and it is not the position of any step `main`
    /// itself declared. Only the string id survives that expansion able to name the step correctly.
    fn call_expansion_json() -> &'static str {
        r#"{
          "version": 1,
          "pipelines": [
            {
              "id": "main", "name": "main",
              "steps": [
                { "id": "before", "type": "comment", "text": "noop" },
                { "id": "call1", "type": "call", "pipeline": "callee" },
                { "id": "after", "type": "eval", "assignments": [{ "target": "z", "expr": "1" }] }
              ]
            },
            {
              "id": "callee", "name": "callee",
              "steps": [
                { "id": "c-good", "type": "eval", "assignments": [{ "target": "a", "expr": "1" }] },
                { "id": "c-bad", "type": "eval", "assignments": [{ "target": "b", "expr": "1 +" }] }
              ]
            }
          ]
        }"#
    }

    #[test]
    fn a_compile_error_inside_an_inlined_call_names_the_callees_own_step_id() {
        let errors = validation_of(call_expansion_json());
        assert!(
            errors.iter().any(|e| e.pipeline.as_deref() == Some("main")
                && e.function == Some(2)
                && e.step.as_deref() == Some("c-bad")),
            "{errors:?}"
        );
        // Never the caller's own call step, or any other step near it -- the old positional index, read against
        // the GUI's one-id-per-`call` numbering, would have named whatever sits at position 2 there instead.
        assert!(
            errors
                .iter()
                .all(|e| e.step.as_deref() != Some("call1") && e.step.as_deref() != Some("after")),
            "{errors:?}"
        );
    }

    /// `main`: a step, then an `if` by the id `if_id` whose condition does not compile, holding one step of its own.
    fn broken_if_json(if_id: &str) -> String {
        format!(
            r#"{{
              "version": 1,
              "pipelines": [
                {{
                  "id": "main", "name": "main",
                  "steps": [
                    {{ "id": "first", "type": "eval", "assignments": [{{ "target": "a", "expr": "1" }}] }},
                    {{ "id": "{if_id}", "type": "if", "condition": "1 +",
                      "then": [{{ "id": "inner", "type": "eval", "assignments": [{{ "target": "b", "expr": "2" }}] }}] }}
                  ]
                }}
              ]
            }}"#
        )
    }

    /// A condition that does not parse, and one that parses but does not type.
    const BROKEN_CONDITIONS: [&str; 2] = ["1 +", "nosuch(a) > 1"];

    #[test]
    fn a_condition_that_does_not_compile_names_its_if() {
        for broken in BROKEN_CONDITIONS {
            let errors = validation_of(&broken_if_json("check").replace("1 +", broken));
            assert!(
                errors.iter().any(|e| e.step.as_deref() == Some("check")),
                "{broken}: {errors:?}"
            );
            assert!(
                errors.iter().all(|e| e.step.as_deref() != Some("first")),
                "{broken}: {errors:?}"
            );
        }
    }

    #[test]
    fn an_if_without_an_id_is_refused() {
        let json = broken_if_json("check")
            .replace("1 +", "a > 0")
            .replace(r#""id": "check", "#, "");
        let err = load_str(&json).unwrap_err().to_string();
        assert!(err.contains("missing field `id`"), "{err}");
    }

    /// `call_expansion_json` with every step compiling, and the call by the id `call_id` carrying `members`.
    fn call_json(call_id: &str, members: &str) -> String {
        call_expansion_json()
            .replace(
                r#""id": "call1", "type": "call","#,
                &format!(r#""id": "{call_id}", "type": "call", {members}"#),
            )
            .replace(r#""expr": "1 +""#, r#""expr": "1""#)
    }

    /// A call's filter is the call's own: an error in it names the call.
    #[test]
    fn a_filter_on_a_call_that_does_not_compile_names_the_call() {
        for broken in BROKEN_CONDITIONS {
            let errors = validation_of(&call_json("call1", &format!(r#""filter": "{broken}","#)));
            assert!(
                errors.iter().any(|e| e.step.as_deref() == Some("call1")),
                "{broken}: {errors:?}"
            );
            assert!(
                errors.iter().all(|e| e.step.as_deref() != Some("before")),
                "{broken}: {errors:?}"
            );
        }
    }

    #[test]
    fn a_call_whose_id_a_callee_step_holds_is_refused_filtered_or_not() {
        for members in ["", r#""filter": "true","#] {
            let err = load_str(&call_json("c-good", members)).unwrap_err().to_string();
            assert!(err.contains(r#"duplicate step id "c-good""#), "{members}: {err}");
        }
    }

    /// A disabled call leaves no step, and its id is a step id all the same, filtered or not.
    #[test]
    fn a_disabled_call_whose_id_another_step_holds_is_refused() {
        for members in [r#""disabled": true,"#, r#""disabled": true, "filter": "true","#] {
            let err = load_str(&call_json("after", members)).unwrap_err().to_string();
            assert!(err.contains(r#"duplicate step id "after""#), "{members}: {err}");
        }
    }

    /// Two calls are two steps: one id between them is refused, though neither leaves a step to hold it.
    #[test]
    fn two_calls_with_one_id_are_refused() {
        let json = r#"{
          "version": 1,
          "pipelines": [
            {
              "id": "main", "name": "main",
              "steps": [
                { "id": "call1", "type": "call", "pipeline": "left" },
                { "id": "call1", "type": "call", "pipeline": "right" }
              ]
            },
            {
              "id": "left", "name": "left",
              "steps": [{ "id": "l", "type": "eval", "assignments": [{ "target": "a", "expr": "1" }] }]
            },
            {
              "id": "right", "name": "right",
              "steps": [{ "id": "r", "type": "eval", "assignments": [{ "target": "b", "expr": "1" }] }]
            }
          ]
        }"#;
        let err = load_str(json).unwrap_err().to_string();
        assert!(err.contains(r#"duplicate step id "call1""#), "{err}");
    }

    /// Pipelines calling the next twice over double at every level. The repeated step ids are refused before the
    /// copies are made, so thirty levels cost thirty refusal checks and not a billion steps.
    #[test]
    fn a_doubling_call_chain_is_refused_before_it_is_copied() {
        let last = 30;
        let pipelines: Vec<String> = (0..=last)
            .map(|n| {
                let steps = if n == last {
                    r#"[{ "id": "leaf", "type": "eval", "assignments": [{ "target": "a", "expr": "1" }] }]"#.to_string()
                } else {
                    format!(
                        r#"[{{ "id": "c{n}a", "type": "call", "pipeline": "p{next}" }},
                            {{ "id": "c{n}b", "type": "call", "pipeline": "p{next}" }}]"#,
                        next = n + 1
                    )
                };
                format!(r#"{{ "id": "p{n}", "name": "p{n}", "steps": {steps} }}"#)
            })
            .collect();
        let json = format!(r#"{{ "version": 1, "pipelines": [{}] }}"#, pipelines.join(","));
        let started = std::time::Instant::now();
        let err = load_str(&json).unwrap_err().to_string();
        assert!(err.contains("duplicate step id"), "{err}");
        assert!(
            started.elapsed() < std::time::Duration::from_secs(5),
            "took {:?}",
            started.elapsed()
        );
    }

    /// A filtered call's guard is the call's own step: it takes the call's id in document order, before the callee's
    /// steps. An unfiltered call leaves no step, so its id takes no place.
    #[test]
    fn a_filtered_calls_guard_takes_the_calls_place_among_the_step_ids() {
        let filtered = load_str(&call_json("call1", r#""filter": "true","#)).unwrap();
        assert_eq!(
            filtered.pipelines[0].step_ids,
            ["before", "call1", "c-good", "c-bad", "after"]
        );
        let plain = load_str(&call_json("call1", "")).unwrap();
        assert_eq!(plain.pipelines[0].step_ids, ["before", "c-good", "c-bad", "after"]);
    }

    /// An `if`'s id is a step id like any other: it takes its place in document order, before its branches, and
    /// must not repeat one.
    #[test]
    fn an_if_id_is_a_step_id() {
        let good = broken_if_json("check").replace("1 +", "a > 0");
        let system = load_str(&good).unwrap();
        assert_eq!(system.pipelines[0].step_ids, ["first", "check", "inner"]);

        let repeated = broken_if_json("first").replace("1 +", "a > 0");
        let err = load_str(&repeated).unwrap_err().to_string();
        assert!(err.contains("duplicate step id"), "{err}");
    }

    #[test]
    fn step_id_for_resolves_none_rather_than_panicking_out_of_range() {
        let system = load_str(example_json()).unwrap();
        assert_eq!(system.step_id_for(None, Some(0)), None);
        assert_eq!(system.step_id_for(Some(0), None), None);
        assert_eq!(system.step_id_for(Some(0), Some(u32::MAX)), None);
        assert_eq!(system.step_id_for(Some(usize::MAX), Some(0)), None);
    }

    /// A blank assignment is a structural error -- caught before compilation, unlike the call-inlining case above --
    /// but it names a real step directly, so it is attributed too. Not every structural problem can be: a duplicate
    /// id or an unknown reference has no single step to point at, and those still carry none.
    #[test]
    fn a_blank_assignment_is_attributed_though_it_is_a_structural_error() {
        let json = r#"{
          "version": 1,
          "pipelines": [{
            "id": "p", "name": "p",
            "steps": [
              { "id": "e", "type": "eval", "assignments": [{ "target": "", "expr": "1" }] }
            ]
          }]
        }"#;
        let file: FileConfig = serde_json::from_str(json).unwrap();
        let errors = validation_errors(file);
        assert!(
            errors.iter().any(|e| e.pipeline.as_deref() == Some("p")
                && e.function.is_none()
                && e.step.as_deref() == Some("e")
                && e.contains("no target field")),
            "{errors:?}"
        );
    }
}
