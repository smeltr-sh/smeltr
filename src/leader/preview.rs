//! Previews: one pipeline over a sample (`run_preview`), a sample routed through a whole bundle
//! (`run_route_preview`), and a locked test run as the preview it locked and compared (`run_test`). Each takes
//! everything it reads as arguments -- the config, the sample and where its lookup tables resolve -- so the leader's
//! routes, its test runner and the demo's preview service give one answer.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use crate::config_file::{self, Verdict};
use crate::core::batch::Batch;
use crate::core::bits;
use crate::core::buffer::{self, BufferPool};
use crate::core::column::{Column, DataType, Validity};
use crate::core::ctx::Ctx;
use crate::core::error::{ErrorSink, ExceptionKind};
use crate::core::id::{FunctionId, SlotId};
use crate::format::record::RecordView;
use crate::leader::envelope::{ApiError, Envelope, Refusal};
use crate::plan;
use crate::plan::compile::Plan;
use crate::plan::config::CompileError;
use crate::plan::output::{RowFilter, run_to_json_marked};
use crate::plan::resolve::Origin;
use crate::plan::topology::SourceMeta;
use crate::plan::tree;
use crate::schema::Schema;
use axum::http::StatusCode;

#[derive(serde::Deserialize)]
pub struct PreviewRequest {
    /// A single-file `config_file` JSON document.
    pub config: String,
    /// By `PipelineDto::id`, which a rename keeps; defaults to the first pipeline.
    pub pipeline: Option<String>,
    /// The saved sample to run, by id. `None` is an editor with none chosen yet, or a caller handing the records
    /// over directly.
    #[serde(default)]
    pub sample: Option<String>,
    /// Show `__`-prefixed control variables in the output.
    #[serde(default)]
    pub show_internal: bool,
    /// What `now()` reads, in unix microseconds. A test re-runs against the instant it was locked at, because
    /// `now()` and a reference-less `parseTime` put the clock into the output -- two runs of one pipeline over one
    /// sample agree only if they are given one instant. An ordinary preview sends none and reads the clock.
    #[serde(default)]
    pub at_micros: Option<i64>,
    /// A Bool over the pipeline's output, typed as a filter on a destination capture is: only the rows it is true
    /// for are returned. One that does not compile fails the preview at the `filter` stage.
    #[serde(default)]
    pub filter: Option<String>,
    /// Say, on each row, which steps changed each of its fields and which step dropped it (`touched`). It keeps every
    /// field's every value for the whole run and compares them, which costs about one more run.
    #[serde(default)]
    pub attribution: bool,
    /// What may follow a `.` in an expression of the config. Asked, the preview answers it instead of running the
    /// sample, and `Preview::after_dot` holds the answer.
    #[serde(default)]
    pub after_dot: Option<AfterDotRequest>,
    /// A capture filter at a destination, checked against each route delivering there. Asked, the preview answers it
    /// instead of running the sample, and `Preview::capture_filter` holds the answer.
    #[serde(default)]
    pub capture_filter: Option<CaptureFilterRequest>,
}

/// A capture filter's text, and the destination, by its id, it would run at.
#[derive(serde::Deserialize)]
pub struct CaptureFilterRequest {
    pub text: String,
    pub destination: String,
}

/// The routes delivering to a destination a capture filter there passes over, each with why: it does not compile
/// against that route's output (`runtime`). `reads` is how many it runs on.
#[derive(Default, serde::Serialize)]
pub struct CaptureFilterCheck {
    pub reads: usize,
    pub skips: Vec<RouteSkip>,
}

#[derive(serde::Serialize)]
pub struct RouteSkip {
    pub route: String,
    pub error: String,
}

/// A `.` an author has just typed: the expression's text, the dot's byte offset in it, and where the expression sits,
/// which decides what its names read.
#[derive(serde::Deserialize)]
pub struct AfterDotRequest {
    pub text: String,
    pub dot: usize,
    pub at: DotAt,
}

/// Where an expression sits: its names read the record as it is there.
#[derive(serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DotAt {
    /// At the start of a step of the request's pipeline, by the step's id.
    Step(String),
    /// A source's record as it arrives, by the source's id: a route's condition, or a capture filter at the source.
    Source(String),
    /// A `when` of a route's `deliver` rules, by the route's id: its pipeline's output.
    When(String),
    /// A capture filter at a destination, by its id: the output of every route that delivers there.
    Destination(String),
}

/// What may follow a `.`: the type of what it follows where it sits, as the engine spells it, an object's fields, and
/// each function a call there may name. `ty` is absent where it does not resolve.
#[derive(serde::Serialize)]
pub struct AfterDot {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ty: Option<String>,
    pub fields: Vec<(String, String)>,
    pub methods: Vec<plan::dot::Method>,
}

#[derive(Default, serde::Serialize)]
pub struct Preview {
    pub stage: Option<&'static str>,
    /// Every pipeline name in the config, even on a compile failure.
    pub pipeline_names: Vec<String>,
    pub rows: Vec<RowOut>,
    /// How many rows there were before the request's filter. Absent with no filter.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unfiltered: Option<u32>,
    /// What each parser step produced, in the order they ran, for an editor proposing a declaration. Recorded as
    /// the parse decided, so it is what the parser did rather than a second reading of the records.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub parsers: Vec<ParserOut>,
    /// The record the pipeline writes (`plan::dictionary`). Absent when it did not compile.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dictionary: Option<DictionaryOut>,
    /// What compiles but is likely a mistake (`Plan::warnings`), each placed at its step as an error is.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<ApiError>,
    /// How many of `warnings` are where the rows break the schemas the pipeline produces.
    #[serde(skip_serializing_if = "is_zero")]
    pub schema_findings: u32,
    /// The fields the schemas the pipeline produces require and its rows lack, which the editor shows as columns.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub missing: Vec<crate::schema::Missing>,
    /// The answer to `PreviewRequest::after_dot`. Absent where the dot follows no value: it is in a string, a comment
    /// or a number.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub after_dot: Option<AfterDot>,
    /// The answer to `PreviewRequest::capture_filter`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub capture_filter: Option<CaptureFilterCheck>,
}

/// The record by its compiled types alone, and with the sample's output filling what only a row knows. `sampled` is
/// absent with no sample.
#[derive(serde::Serialize)]
pub struct DictionaryOut {
    pub compiled: Dictionary,
    pub sampled: Option<Dictionary>,
}

/// One record type, as its fields with their types spelled, and as an Elasticsearch mapping.
#[derive(serde::Serialize)]
pub struct Dictionary {
    pub fields: Vec<(String, String)>,
    pub elasticsearch: serde_json::Value,
}

impl Dictionary {
    pub fn of(record: &crate::core::column::DataType) -> Self {
        Dictionary {
            fields: plan::dictionary::fields(record),
            elasticsearch: crate::io::elastic_mapping::mapping(record),
        }
    }
}

/// One parser step's fields, in the order they were bound: declared first, then discovered in the order the rows
/// introduced them.
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ParserOut {
    pub fields: Vec<FieldOut>,
}

/// One field the parse produced. `present` of `rows` is coverage; `kinds` is what the rows disagreed about, and is
/// why `suggested` is what it is.
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FieldOut {
    pub name: String,
    /// Whether a declaration already named it.
    pub declared: bool,
    /// A declared field's type, or the declaration an undeclared field's values suggest, spelled as a declaration
    /// spells it.
    pub suggested: String,
    pub rows: u32,
    pub present: u32,
    /// Every kind the rows held, largest first, as (type, rows). One entry is a field they agreed on.
    pub kinds: Vec<(String, u32)>,
    /// Rows that would not coerce to a declared type. Zero unless `declared`.
    #[serde(skip_serializing_if = "is_zero")]
    pub failed: u32,
    /// Rows whose value read as a time (`TagCounts::times`): every present row is a field to declare a `datetime`.
    #[serde(skip_serializing_if = "is_zero")]
    pub times: u32,
}

fn is_zero(n: &u32) -> bool {
    *n == 0
}

/// A grok pattern's captures, which are the fields the step can produce. The editor fills a step's field names in from
/// this rather than asking the author to retype what the pattern already says.
#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GrokCapturesRequest {
    pub patterns: Vec<String>,
    #[serde(default)]
    pub custom_patterns: Vec<config_file::CustomPatternDto>,
    /// The bundle's patterns, which the step may name. The editor has them already, and a step being edited may
    /// name one that is not saved yet.
    #[serde(default)]
    pub grok_patterns: Vec<config_file::GrokPatternDto>,
}

/// The editor keeps the fields it has when a half-typed pattern cannot compile, so a failure carries no captures.
#[derive(Default, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GrokCaptures {
    /// Every capture across the patterns, in the order they appear, deduped.
    pub captures: Vec<String>,
}

/// A line breaker as it is typed into a sourcetype's row.
#[derive(serde::Deserialize)]
pub struct LineBreakerRequest {
    pub pattern: String,
}

/// Pipeline code as it is typed into a step's field.
#[derive(serde::Deserialize)]
pub struct FormatRequest {
    pub code: String,
    pub kind: crate::expr::pretty::Kind,
}

/// The code laid out in the style (`expr::pretty`), or null where it does not parse yet.
#[derive(Default, serde::Serialize)]
pub struct Formatted {
    pub code: Option<String>,
}

/// How a declaration spells a type, which is what the editor's own type list offers.
fn kinds_of(counts: &crate::core::tally::TagCounts) -> Vec<(String, u32)> {
    let mut kinds: Vec<(String, u32)> = [
        ("bool", counts.bools),
        ("i64", counts.ints),
        ("f64", counts.floats),
        ("str", counts.strings),
        ("array", counts.arrays),
        ("struct", counts.objects),
    ]
    .into_iter()
    .filter(|(_, n)| *n > 0)
    .map(|(name, n)| (name.to_string(), n))
    .collect();
    kinds.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    kinds
}

impl Preview {
    /// Which stage stopped the preview, and why, at 422: what was sent to preview is what is refused. A compile
    /// failure still names every pipeline, so the editor's list stands.
    pub fn failed(stage: &'static str, errors: Vec<ApiError>, pipeline_names: Vec<String>) -> Envelope<Preview> {
        let body = Preview {
            stage: Some(stage),
            pipeline_names,
            ..Default::default()
        };
        Envelope::with(body, StatusCode::UNPROCESSABLE_ENTITY, errors)
    }
}

#[derive(serde::Serialize)]
pub struct RowOut {
    pub row: u32,
    /// `None` when the row did not survive the pipeline.
    pub json: Option<serde_json::Value>,
    pub exceptions: Vec<ExceptionOut>,
    /// With `attribution`: each field a step changed or added, by its name as the editor writes it, and those steps by
    /// id in the order they ran. Absent on a row no step changed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub steps: Option<BTreeMap<String, Vec<String>>>,
    /// With `attribution`: the step after which the row was gone.
    #[serde(rename = "droppedBy", skip_serializing_if = "Option::is_none")]
    pub dropped_by: Option<String>,
}

#[derive(serde::Serialize)]
pub struct ExceptionOut {
    /// The answer the exception's kind takes: `ignore`, `tag`, `drop`, `deliver` or `block`.
    pub answer: &'static str,
    pub message: String,
    /// The step that raised it, by id. Absent for one raised reading the record, before any step.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub step: Option<String>,
}

// -- route preview ------------------------------------------------------
//
// Where a sample's records would go: which routes match, what each matched pipeline produces, and which
// destinations the delivery rules choose. `runtime::preview_routes` runs the transform's own routing and
// serialization and stops where it would dispatch, so nothing here opens a connection or stands in for a
// destination.
//
// Separate from the pipeline preview above, which answers a different question -- one pipeline's rows in and rows
// out, with no source identity and no delivery. Routing selects on a source, so this one needs the sample to name
// the source its records stand for and that one must not have it.

#[derive(serde::Deserialize)]
pub struct RoutePreviewRequest {
    /// A whole-bundle `config_file` JSON document: sources, destinations, routes and pipelines.
    pub config: String,
    /// The saved sample to route, by id.
    pub sample: String,
    /// What `now()` reads, in unix microseconds. A run re-run against the instant it was locked at agrees with the
    /// first, because `now()` and a reference-less `parseTime` put the clock into the output -- two runs over one
    /// sample agree only if they are given one instant. An ordinary preview sends none and reads the clock.
    #[serde(default)]
    pub at_micros: Option<i64>,
}

#[derive(Default, serde::Serialize)]
pub struct RoutePreview {
    /// The pipeline each route dispatches to, by route index. A `RouteConfig` has no name of its own, and what a
    /// reader needs of a matched route is where it sent the row.
    pub route_pipelines: Vec<String>,
    /// The id of the pipeline each route dispatches to, by route index -- position-parallel to `route_pipelines`. A
    /// locked test records this, so a rename keeps it and a change to the pipeline finds it.
    pub route_pipeline_ids: Vec<String>,
    /// `RouteConfig::id`, by route index -- position-parallel to `route_pipelines`. Resolves an index in `matched`
    /// or `through` to the id the config gave that route, which an edit keeps and a delete-and-insert does not.
    pub route_ids: Vec<String>,
    /// Destination names by id.
    pub destination_names: HashMap<u32, String>,
    /// The answer to `Unmatched`, with each destination by its author's id. Absent only on failure.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unmatched: Option<config_file::AnswerDto>,
    pub rows: Vec<RoutePreviewRow>,
    /// How the columns the sample was captured under differ from what its source declares now: the rows are read as
    /// the source declares.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

#[derive(serde::Serialize)]
pub struct RoutePreviewRow {
    pub row: usize,
    /// Every route whose condition selected this record.
    pub matched: Vec<usize>,
    /// What each matched route made of it.
    pub through: Vec<RoutePreviewThrough>,
    /// Why this record stopped short, where it did: `PreviewedRow::refused`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refused: Option<String>,
}

#[derive(serde::Serialize)]
pub struct RoutePreviewThrough {
    pub route: usize,
    /// What this route made of the record. More than one when the pipeline unrolls an array; none when it was
    /// dropped or blocked.
    pub documents: Vec<RoutePreviewDocument>,
    pub exceptions: Vec<RoutePreviewException>,
    /// The joined answer to them: `ignore`, `tag`, `drop`, `deliver` or `block`.
    pub answer: &'static str,
}

#[derive(serde::Serialize)]
pub struct RoutePreviewException {
    pub kind: &'static str,
    pub message: String,
}

#[derive(serde::Serialize)]
pub struct RoutePreviewDocument {
    pub document: String,
    pub destinations: Vec<u32>,
    #[serde(skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub destination: std::collections::BTreeMap<&'static str, String>,
}

/// A sample's records as a source's meta reads them.
pub struct SampleRead {
    pub records: Vec<crate::io::SourceRecord>,
    /// Each record's `_raw` as the sample holds it, already decoded; a record with none, its bytes read as UTF-8.
    pub texts: Vec<String>,
    /// A sentence for each way the sample's columns differ from the meta's record.
    pub warnings: Vec<String>,
}

/// `records`, a sample's, as `meta` reads them, and a sentence for each way `columns`, what they were captured as,
/// differ from `meta`'s record. A column `meta` no longer declares is left out, one it declares and the sample lacks
/// reads null, and a column whose type changed reads under the new type, its failed marks dropped: a value that
/// failed one type is judged afresh by another. A sample with no columns is read as it stands.
///
/// Only a record's own fields are compared: an internal one (`plan::tree::is_internal`) is the engine's, not its
/// source's. Where `meta` is a source's, the source sets its own internal fields, so the sample's are not read; where
/// it is the sample's own (`SourceMeta::of_columns`), they are columns of the record like any other.
pub fn read_sample(
    meta: &SourceMeta,
    columns: &[crate::heartbeat::CaptureColumn],
    records: &[crate::io::RecordObject],
) -> Result<SampleRead, String> {
    let from_source = !meta.source_fields().is_empty();
    let head = |name: &str| {
        crate::plan::tree::segments(name)
            .next()
            .map(std::borrow::Cow::into_owned)
    };
    let own = |name: &str| !head(name).is_some_and(|h| crate::plan::tree::is_internal(&h));
    let captured: Vec<_> = columns.iter().filter(|c| own(&c.name)).collect();
    let declared = meta.record_fields();
    // An open record keeps a top-level field no declared field claims.
    let kept = |name: &str| meta.is_open() && !declared.iter().any(|(d, _)| head(d) == head(name));
    let mut warnings = Vec::new();
    let mut gone = Vec::new();
    let mut retyped = Vec::new();
    if !captured.is_empty() {
        for c in &captured {
            match declared.iter().find(|(name, _)| *name == c.name) {
                None if c.name == crate::plan::tree::UNCLAIMED || kept(&c.name) => {}
                None => {
                    warnings.push(format!(
                        "`{}` was captured, and its source no longer declares it, so it is left out",
                        c.name
                    ));
                    gone.push(c.name.as_str());
                }
                Some((_, now)) if now.spell() != c.ty && c.name != crate::plan::tree::UNCLAIMED => {
                    warnings.push(format!(
                        "`{}` was captured as {} and its source now declares it {}",
                        c.name,
                        c.ty,
                        now.spell()
                    ));
                    retyped.push(c.name.as_str());
                }
                Some(_) => {}
            }
        }
        for (name, _) in declared
            .iter()
            .filter(|(n, _)| *n != crate::plan::tree::UNCLAIMED && own(n))
        {
            if !captured.iter().any(|c| c.name == *name) {
                warnings.push(format!("`{name}` is declared and was not captured, so it reads null"));
            }
        }
    }
    let objects: Vec<crate::io::RecordObject> = records
        .iter()
        .map(|r| {
            let mut object = r.clone();
            if from_source {
                object.retain(|key, _| !crate::plan::tree::is_internal(key));
            }
            for name in &gone {
                remove_at(&mut object, name);
            }
            let mut failed = crate::io::take_mistyped(&mut object);
            failed.retain(|f| !gone.contains(&f.as_str()) && !retyped.contains(&f.as_str()));
            if !failed.is_empty() {
                let (outer, inner) = crate::io::MISTYPED_FIELDS
                    .split_once('.')
                    .expect("the failed list nests one level");
                if let serde_json::Value::Object(tag) = object
                    .entry(outer)
                    .or_insert_with(|| serde_json::Value::Object(Default::default()))
                {
                    tag.insert(inner.to_string(), serde_json::json!(failed));
                }
            }
            object
        })
        .collect();
    let sources: Vec<_> = records.iter().map(config_file::record_source).collect();
    let stored: Vec<_> = objects.iter().zip(&sources).collect();
    let read = meta.read_stored(&stored)?;
    let texts = records
        .iter()
        .zip(&read)
        .map(|(r, read)| match r.get("_raw") {
            Some(serde_json::Value::String(text)) => text.clone(),
            _ => String::from_utf8_lossy(meta.read_bytes(read).unwrap_or_default()).into_owned(),
        })
        .collect();
    Ok(SampleRead {
        records: read,
        texts,
        warnings,
    })
}

/// Remove the value at `path`, a record field's name, from `object`, and each object it leaves empty.
fn remove_at(object: &mut crate::io::RecordObject, path: &str) {
    fn remove(object: &mut crate::io::RecordObject, keys: &[std::borrow::Cow<'_, str>]) {
        match keys {
            [] => {}
            [last] => {
                object.remove(last.as_ref());
            }
            [key, rest @ ..] => {
                if let Some(serde_json::Value::Object(inner)) = object.get_mut(key.as_ref()) {
                    remove(inner, rest);
                    if inner.is_empty() {
                        object.remove(key.as_ref());
                    }
                }
            }
        }
    }
    let keys: Vec<_> = crate::plan::tree::segments(path).collect();
    remove(object, &keys);
}

/// Every lookup table `config` declares, mapped to `at` -- the ordinary case, a bundle read from one place with no
/// merge behind it. A caller previewing a merge of two bundles (a group's test run, with `shared/` merged in) builds its own
/// `DeclaredAt` instead, one origin per table, rather than pointing every table at the same place.
pub fn single_origin_declared_at(config: &str, at: &config_file::BundleAt) -> config_file::DeclaredAt {
    serde_json::from_str::<config_file::FileConfig>(config)
        .map(|file| file.lookup_tables.iter().map(|t| (t.id.clone(), at.clone())).collect())
        .unwrap_or_default()
}

pub fn run_route_preview(
    config: &str,
    sample: config_file::SampleDto,
    declared_at: config_file::DeclaredAt,
    at_micros: Option<i64>,
) -> Envelope<RoutePreview> {
    let Some(source_name) = sample.source.clone() else {
        return Envelope::refused(Refusal::invalid(
            "this sample does not say which source it stands for, and routing selects on a source -- \
             name one on the sample to route it",
        ));
    };

    let system = match config_file::load_str_at(config, &declared_at) {
        Ok(system) => system,
        Err(e) => return Envelope::refused(Refusal::invalid(e.to_string())),
    };
    let Some(source) = system.topology.sources.iter().find(|s| s.name == source_name) else {
        return Envelope::refused(Refusal::invalid(format!(
            "this sample stands for source {source_name:?}, which this config does not declare"
        )));
    };

    let compiled = match system.compile_topology() {
        Ok(compiled) => compiled,
        Err(errs) => {
            return Envelope::refused(Refusal::all(
                StatusCode::UNPROCESSABLE_ENTITY,
                errs.into_iter()
                    .map(|e| ApiError::from_validation(&system.to_validation_error(e)))
                    .collect(),
            ));
        }
    };
    let route_ids: Vec<String> = compiled.routes.iter().map(|r| r.id.clone()).collect();

    // A sample's byte records hold their text already decoded, so nothing decodes. A record with no coordinates gets
    // the empty ones a pasted record has; the source's own `SourceMeta` decides whether a route may read them at all.
    let SampleRead {
        records,
        texts,
        warnings,
    } = match read_sample(&compiled.meta_for(source.id), &sample.columns, &sample.records) {
        Ok(read) => read,
        Err(e) => return Envelope::refused(Refusal::invalid(e)),
    };

    let previewed = crate::runtime::preview_routes_at(
        &compiled,
        &crate::runtime::Sample {
            source,
            records: &records,
            texts: &texts,
        },
        crate::runtime::preview_memory_budget(),
        at_micros.unwrap_or_else(crate::core::ctx::wall_clock_micros),
    );

    // By id, not by position: `load` assigns pipeline ids from its own map, and reading the list positionally
    // would name the wrong pipeline the day those stop agreeing.
    let pipeline_name = |id: usize| {
        system
            .pipelines
            .iter()
            .find(|p| p.id == id)
            .map(|p| p.name.clone())
            .unwrap_or_else(|| format!("pipeline {id}"))
    };
    Envelope::ok(RoutePreview {
        route_pipelines: compiled.routes.iter().map(|r| pipeline_name(r.pipeline)).collect(),
        route_pipeline_ids: compiled
            .routes
            .iter()
            .map(|r| system.pipeline_ids.get(r.pipeline).cloned().unwrap_or_default())
            .collect(),
        route_ids,
        destination_names: system
            .topology
            .destinations
            .iter()
            .map(|d| (d.id, d.name.clone()))
            .collect(),
        unmatched: Some(system.answer_by_name(system.topology.on_exception.answer(&ExceptionKind::Unmatched))),
        warnings,
        rows: previewed
            .into_iter()
            .map(|row| RoutePreviewRow {
                row: row.row,
                matched: row.matched,
                through: row
                    .through
                    .into_iter()
                    .map(|t| RoutePreviewThrough {
                        route: t.route,
                        exceptions: t
                            .exceptions
                            .into_iter()
                            .map(|(kind, message)| RoutePreviewException { kind, message })
                            .collect(),
                        answer: t.answer.label(),
                        documents: t
                            .documents
                            .into_iter()
                            .map(|d| RoutePreviewDocument {
                                document: d.document,
                                destinations: d.destinations,
                                destination: d.destination,
                            })
                            .collect(),
                    })
                    .collect(),
                refused: row.refused,
            })
            .collect(),
    })
}

/// Compiling a pattern touches nothing outside itself, so this is the whole answer and a route only wraps it.
pub fn captures_of(req: GrokCapturesRequest) -> Envelope<GrokCaptures> {
    let config = crate::plan::config::GrokConfig {
        split_dots: true,
        patterns: req.patterns,
        custom_patterns: req.custom_patterns.into_iter().map(|p| (p.name, p.pattern)).collect(),
    };
    let shared = req.grok_patterns.into_iter().map(|p| (p.id, p.pattern)).collect();
    match crate::format::grok_scan::Matcher::compile_with(&config, &shared) {
        Ok(m) => Envelope::ok(GrokCaptures {
            captures: m.capture_names().into_iter().map(str::to_string).collect(),
        }),
        Err(e) => Envelope::refused(Refusal::invalid(e.to_string())),
    }
}

/// Not `ok` when a save would refuse the pattern, with why, word for word as the save says it.
pub fn line_breaker_verdict(req: LineBreakerRequest) -> Envelope {
    let refusal = config_file::line_breaker_refusal("this line breaker", &req.pattern);
    match refusal {
        Some(refusal) => Envelope::refused(Refusal::invalid(refusal)),
        None => Envelope::done(),
    }
}

pub fn format_code(req: FormatRequest) -> Envelope<Formatted> {
    Envelope::ok(Formatted {
        code: crate::expr::pretty::format(&req.code, req.kind),
    })
}

/// `system` compiled, and the id of its destination `name`: what a capture filter there is compiled over, as a worker
/// compiles it (`runtime`). The whole topology compiles, so an error anywhere in `system` leaves a destination no answer.
fn at_destination(
    system: &config_file::LoadedSystem,
    name: &str,
) -> Result<(plan::topology::CompiledTopology, crate::core::id::DestId), String> {
    let dest = (system.topology.destinations.iter())
        .find(|d| d.name == *name)
        .ok_or_else(|| format!("this config has no destination {name:?}"))?
        .id;
    let compiled = system.compile_topology().map_err(|errors| {
        (errors.into_iter())
            .map(|e| system.describe(e))
            .collect::<Vec<_>>()
            .join("; ")
    })?;
    Ok((compiled, dest))
}

/// Route `r`'s pipeline, compiled over what its source gives, and that metadata.
fn route_plan(
    system: &config_file::LoadedSystem,
    r: &plan::topology::RouteConfig,
) -> Result<(plan::compile::Plan, SourceMeta), String> {
    let meta = (system.topology.sources.iter())
        .find(|s| s.id == r.source)
        .map(|s| s.kind.meta())
        .unwrap_or(SourceMeta::NONE);
    let p = (system.pipelines.iter())
        .find(|p| p.id == r.pipeline)
        .ok_or_else(|| format!("this config has no pipeline {:?}", r.pipeline))?;
    let plan = plan::compile::compile(p, &meta)
        .map_err(|errors| errors.iter().map(ToString::to_string).collect::<Vec<_>>().join("; "))?;
    Ok((plan, meta))
}

/// What a capture filter at a destination makes of each route delivering there, as a worker compiles it.
fn capture_filter(
    asked: &CaptureFilterRequest,
    system: &config_file::LoadedSystem,
) -> Result<CaptureFilterCheck, String> {
    let (compiled, dest) = at_destination(system, &asked.destination)?;
    let mut check = CaptureFilterCheck::default();
    for (idx, filter) in compiled.capture_filters(dest, &asked.text) {
        match filter {
            Ok(_) => check.reads += 1,
            Err(e) => check.skips.push(RouteSkip {
                route: compiled.routes[idx].id.clone(),
                error: e.to_string(),
            }),
        }
    }
    Ok(check)
}

/// The answer to `asked` against `system`. A step's names read the record its pipeline's sample was captured under, as
/// a preview of that pipeline does; a source's, the record the source gives; a `when`'s, the route's pipeline's output
/// for that source. `Err` names an id `system` does not hold.
fn after_dot(
    asked: &AfterDotRequest,
    system: &config_file::LoadedSystem,
    pipeline: Option<&str>,
    columns: &[crate::heartbeat::CaptureColumn],
) -> Result<Option<AfterDot>, String> {
    use plan::dot::{Before, before_dot, fields_of, methods_for, type_in};
    use plan::resolve::SymbolTable;
    let expr = match before_dot(&asked.text, asked.dot) {
        Before::Nothing => return Ok(None),
        Before::Unread => None,
        Before::Value(e) => Some(e),
    };
    // What `e` is over a route's pipeline's output. The pipeline compiles on its own: a half-typed expression after it
    // is not in it.
    let over_output = |r: &plan::topology::RouteConfig, e: &crate::expr::ast::Expr| {
        let (plan, meta) = route_plan(system, r).ok()?;
        type_in(
            e,
            &plan::topology::output_symbols(&plan, &r.on_exception.tag_field, &meta),
        )
    };
    let ty = match &asked.at {
        DotAt::Step(step) => {
            let p = pipeline
                .and_then(|id| system.pipeline(id))
                .ok_or("this config has no such pipeline")?;
            let step = p
                .function_id(step)
                .ok_or_else(|| format!("this pipeline has no step {step:?}"))?;
            let meta = SourceMeta::of_columns(columns)?;
            let cut = plan::dot::cut_at(&asked.text, asked.dot);
            expr.and_then(|e| plan::compile::type_at(p, &meta, step, &e, cut))
        }
        DotAt::Source(id) => {
            let source = (system.topology.sources.iter())
                .find(|s| s.name == *id)
                .ok_or_else(|| format!("this config has no source {id:?}"))?;
            let syms = SymbolTable::new(&plan::compile::ingest_fields(&source.kind.meta()));
            expr.and_then(|e| type_in(&e, &syms))
        }
        DotAt::When(route) => {
            let r = (system.topology.routes.iter())
                .find(|r| r.id == *route)
                .ok_or_else(|| format!("this config has no route {route:?}"))?;
            expr.and_then(|e| over_output(r, &e))
        }
        DotAt::Destination(id) => {
            // A filter there runs on each route delivering to it that it compiles on (`runtime`), so a name has
            // what any of them gives it.
            let (compiled, dest) = at_destination(system, id)?;
            (compiled.delivering(dest))
                .filter_map(|(_, r)| type_in(expr.as_ref()?, &compiled.output_of(r)))
                .reduce(|a, b| plan::dictionary::meet(a, &b))
        }
    };
    Ok(Some(match ty {
        None => AfterDot {
            ty: None,
            fields: Vec::new(),
            methods: Vec::new(),
        },
        Some(ty) => AfterDot {
            fields: fields_of(&ty).iter().map(|(k, t)| (k.clone(), t.spell())).collect(),
            methods: methods_for(&ty),
            ty: Some(ty.spell()),
        },
    }))
}

pub fn run_preview(
    req: PreviewRequest,
    declared_at: config_file::DeclaredAt,
    columns: &[crate::heartbeat::CaptureColumn],
    records: Vec<crate::io::RecordObject>,
) -> Envelope<Preview> {
    let system = match config_file::load_str_at(&req.config, &declared_at) {
        Ok(system) => system,
        // `Validation` is one error per problem found, some of them attributed to a step; folding them into one
        // joined string here would lose both the attribution and the count.
        Err(config_file::LoadError::Validation(errs)) => {
            let errors = errs.iter().map(ApiError::from_validation).collect();
            return Preview::failed("load", errors, vec![]);
        }
        Err(e) => {
            return Preview::failed("load", vec![ApiError::new(e.to_string())], vec![]);
        }
    };

    let pipeline_names: Vec<String> = system.pipelines.iter().map(|p| p.name.clone()).collect();

    if let Some(asked) = &req.capture_filter {
        return match capture_filter(asked, &system) {
            Ok(check) => Envelope::ok(Preview {
                pipeline_names,
                capture_filter: Some(check),
                ..Preview::default()
            }),
            Err(e) => Preview::failed("config", vec![ApiError::new(e)], pipeline_names),
        };
    }

    if let Some(asked) = &req.after_dot {
        return match after_dot(asked, &system, req.pipeline.as_deref(), columns) {
            Ok(after_dot) => Envelope::ok(Preview {
                pipeline_names,
                after_dot,
                ..Preview::default()
            }),
            Err(e) => Preview::failed("config", vec![ApiError::new(e)], pipeline_names),
        };
    }

    let pipeline_cfg = match &req.pipeline {
        Some(id) => system.pipeline(id),
        None => system.pipelines.first(),
    };
    let Some(pipeline_cfg) = pipeline_cfg else {
        return Preview::failed(
            "config",
            vec![ApiError::new("this config has no pipelines to run")],
            pipeline_names,
        );
    };

    // A pipeline preview is of the pipeline alone, over the columns its sample was captured under, each read from the
    // record as it was stored, `__source` fields included. Not the metadata of a source that routes to this pipeline,
    // which would make `__source.topic` compile here and read null against records that never carried one.
    let meta = match SourceMeta::of_columns(columns) {
        Ok(meta) => meta,
        Err(e) => return Preview::failed("sample", vec![ApiError::new(e)], pipeline_names),
    };
    let SampleRead {
        records: sourced,
        texts,
        ..
    } = match read_sample(&meta, columns, &records) {
        Ok(read) => read,
        Err(e) => return Preview::failed("sample", vec![ApiError::new(e)], pipeline_names),
    };
    let mut plan = match plan::compile::compile(pipeline_cfg, &meta) {
        Ok(plan) => plan,
        Err(errs) => {
            // `compile` runs one pipeline in isolation and so never sets `CompileError::pipeline`; the pipeline
            // being compiled is `pipeline_cfg` regardless, so it is named directly here rather than through `e`.
            let messages = errs
                .iter()
                .map(|e| ApiError::from_compile(e, system.step_id_for(Some(pipeline_cfg.id), e.function)))
                .collect();
            return Preview::failed("compile", messages, pipeline_names);
        }
    };
    if req.attribution {
        keep_bindings(&mut plan);
    }

    let warn = |e: &CompileError| ApiError::from_compile(e, system.step_id_for(Some(pipeline_cfg.id), e.function));
    let mut warnings: Vec<ApiError> = plan.warnings.iter().map(warn).collect();
    // A field the source gave is warned of at the first step, which the record enters through.
    let first = pipeline_cfg.body.first().map(|step| match step {
        plan::config::Step::Function(f) => f.id,
        plan::config::Step::If { id, .. } => *id,
    });

    // A pipeline has no answers of its own: it is previewed under the group's.
    let on_exception = &system.topology.on_exception;
    let record = plan::dictionary::record_type(&plan, &on_exception.tag_field);
    let filter = match &req.filter {
        None => None,
        Some(src) => {
            match plan::topology::output_condition(src, "a preview filter", &plan, &on_exception.tag_field, &meta) {
                Ok((ir, patterns)) => Some(RowFilter { ir, patterns }),
                Err(e) => return Preview::failed("filter", vec![ApiError::new(e.to_string())], pipeline_names),
            }
        }
    };
    if records.is_empty() {
        let (found, missing) = schema_warnings(system.produced_by(pipeline_cfg.id), &plan, &record, first);
        warnings.extend(found.iter().map(warn));
        return Envelope::ok(Preview {
            stage: None,
            pipeline_names,
            after_dot: None,
            capture_filter: None,
            rows: vec![],
            unfiltered: filter.as_ref().map(|_| 0),
            parsers: vec![],
            dictionary: Some(DictionaryOut {
                compiled: Dictionary::of(&record),
                sampled: None,
            }),
            warnings,
            schema_findings: found.len() as u32,
            missing,
        });
    }

    let mut pool = BufferPool::with_budget(crate::runtime::preview_memory_budget());
    let mut errors = ErrorSink::new();
    // Outside the budget guard, like the errors: a refusal still leaves whatever the parse got to say.
    let mut tally = crate::core::tally::FieldTally::new();
    let at_micros = req.at_micros;

    // Budgeted: the leader is also what serves the GUI, the config API and every worker's heartbeat, and a pipeline
    // that unrolls an array asks for far more than the sample weighs.
    let previewed = buffer::catch_over_budget(|| {
        let mut batch = Batch::new(&plan.delivery_layout(), records.len(), 0);

        // A byte record's `_raw` is text (`core::batch`), and a sample's records are text by the time they are
        // stored -- a paste arrives as text and a capture decodes with its source's encoding, so nothing decodes here.
        let text: Vec<std::borrow::Cow<'_, str>> =
            texts.iter().map(|t| std::borrow::Cow::Borrowed(t.as_str())).collect();
        // Each record's `__source` fields are its own columns, so no source names the batch.
        meta.ingest(&mut batch, &sourced, &text, None, |_| true, &mut pool);

        let mut ctx = match at_micros {
            Some(at) => Ctx::at(&mut pool, &mut errors, at),
            None => Ctx::new(&mut pool, &mut errors),
        }
        .tallying(&mut tally);
        let rows = run_to_json_marked(
            &plan,
            on_exception,
            &mut batch,
            &mut ctx,
            req.show_internal,
            filter.as_ref(),
        );
        // An unroll makes rows of its own, which no longer line up with the sample's.
        let touched = (req.attribution && batch.rows() == records.len()).then(|| touched(&plan, &batch));
        batch.reclaim(&mut pool);
        (rows, touched)
    });
    let (preview_rows, touched) = match previewed {
        Ok(previewed) => previewed,
        Err(over) => {
            let mut refused = Preview::failed(
                "memory",
                vec![ApiError::new(format!(
                    "this sample asked for more memory than a preview may hold: {over}"
                ))],
                pipeline_names,
            );
            refused.status = StatusCode::PAYLOAD_TOO_LARGE;
            return refused;
        }
    };

    // A called pipeline's steps are the call's: the call is the step this pipeline's author sees.
    let calls = serde_json::from_str::<serde_json::Value>(&req.config)
        .ok()
        .and_then(|config| calls_of(&config, req.pipeline.as_deref()))
        .unwrap_or_default();
    let mut rows: Vec<(RowOut, bool)> = preview_rows
        .into_iter()
        .map(|r| {
            let row = RowOut {
                row: r.row,
                json: r
                    .json
                    .map(|bytes| serde_json::from_slice(&bytes).expect("serializer produced valid JSON")),
                exceptions: r
                    .exceptions
                    .into_iter()
                    .map(|(answer, message, step)| ExceptionOut {
                        answer,
                        message,
                        step: step.map(|id| calls.get(&id).cloned().unwrap_or(id)),
                    })
                    .collect(),
                steps: None,
                dropped_by: None,
            };
            (row, r.matched)
        })
        .collect();
    if let Some(touched) = touched {
        attribute(&calls, &plan, touched, &mut rows);
    }

    let parsers = tally
        .parses()
        .iter()
        .map(|parse| ParserOut {
            fields: parse
                .fields
                .iter()
                .map(|f| FieldOut {
                    name: f.name.clone(),
                    declared: f.declared,
                    suggested: f.suggested.to_string(),
                    rows: f.rows,
                    present: f.counts.present,
                    kinds: kinds_of(&f.counts),
                    failed: f.failed,
                    times: f.counts.times,
                })
                .collect(),
        })
        .collect();

    // The dictionary is of every row: a filter chooses what is shown, not what the pipeline writes.
    let sampled = plan::dictionary::with_sample(&record, rows.iter().filter_map(|(r, _)| r.json.as_ref()));
    let (found, missing) = schema_warnings(system.produced_by(pipeline_cfg.id), &plan, &sampled, first);
    warnings.extend(found.iter().map(warn));
    let unfiltered = filter.as_ref().map(|_| rows.len() as u32);
    let rows = rows
        .into_iter()
        .filter(|(_, matched)| *matched)
        .map(|(r, _)| r)
        .collect();
    Envelope::ok(Preview {
        stage: None,
        pipeline_names,
        after_dot: None,
        capture_filter: None,
        dictionary: Some(DictionaryOut {
            compiled: Dictionary::of(&record),
            sampled: Some(Dictionary::of(&sampled)),
        }),
        rows,
        unfiltered,
        parsers,
        warnings,
        schema_findings: found.len() as u32,
        missing,
    })
}

/// A warning for each field of `record`, what `plan` writes, that breaks the schemas the pipeline produces
/// (`schema::check`): at the step that wrote the field, or at `first` where the source gave it. A field the schemas
/// require and the record lacks has no step that wrote it, and is warned of with none.
fn schema_warnings(
    produces: &[Arc<Schema>],
    plan: &Plan,
    record: &DataType,
    first: Option<FunctionId>,
) -> (Vec<CompileError>, Vec<crate::schema::Missing>) {
    let findings = crate::schema::check(produces, record);
    let warned = crate::schema::told(produces, &findings)
        .into_iter()
        .map(|(keys, message)| {
            let at = keys.and_then(|keys| plan::dictionary::writer(plan, &keys).or(first));
            CompileError::new(at, message)
        })
        .collect();
    (warned, crate::schema::missing(produces, &findings))
}

/// Clear every `reuse` that would hand away a binding of a name, so `touched` can read each binding after the run.
/// Compiler temporaries are still reused.
fn keep_bindings(plan: &mut Plan) {
    let named: HashSet<SlotId> = plan
        .provenance
        .iter()
        .filter(|o| !o.name.starts_with('<'))
        .map(|o| o.slot)
        .chain(plan.aliases.iter().map(|(slot, ..)| *slot))
        .collect();
    for instr in &mut plan.instrs {
        if instr.reuse.is_some_and(|s| named.contains(&s)) {
            instr.reuse = None;
        }
    }
}

/// What each step did to each row, read from a run of a plan `keep_bindings` kept: a step that binds a name changed it
/// on a row where the new value differs from the one bound before, and a step that binds `__live` dropped a row it
/// turns false. A value is compared whole, so a step that changes one field of an object changed the object.
struct Touched<'p> {
    /// Per row, each name a step changed, with the step, in the order they ran.
    changed: Vec<Vec<(&'p str, FunctionId)>>,
    dropped_by: Vec<Option<FunctionId>>,
}

fn touched<'p>(plan: &'p Plan, batch: &Batch) -> Touched<'p> {
    let rows = batch.rows();
    let mut changed = vec![Vec::new(); rows];
    let mut dropped_by = vec![None; rows];
    // Every binding of a name in the order the compiler made it: a copy, made when there were `at` slots, comes
    // before the slot numbered `at`.
    let mut bindings: Vec<(usize, &'p str, SlotId, Option<FunctionId>)> = plan
        .provenance
        .iter()
        .filter(|o| !o.name.starts_with('<'))
        .map(|o| {
            let step = match o.origin {
                Origin::Function(f) => Some(f),
                _ => None,
            };
            (2 * o.slot + 1, o.name.as_str(), o.slot, step)
        })
        .chain(
            plan.aliases
                .iter()
                .map(|(slot, name, at, f)| (2 * at, name.as_str(), *slot, Some(*f))),
        )
        .collect();
    bindings.sort_by_key(|(order, ..)| *order);
    let mut bound: HashMap<&str, SlotId> = HashMap::new();
    for (_, name, slot, step) in bindings {
        let before = bound.insert(name, slot);
        let Some(step) = step else {
            continue;
        };
        if name == "__live" {
            let live = |slot: Option<SlotId>, row: usize| {
                slot.is_none_or(|s| {
                    batch
                        .peek(s)
                        .bool()
                        .is_ok_and(|b| b.validity.is_valid(row) && bits::get_row(b.values, row))
                })
            };
            for (row, dropped) in dropped_by.iter_mut().enumerate() {
                if dropped.is_none() && live(before, row) && !live(Some(slot), row) {
                    *dropped = Some(step);
                }
            }
            continue;
        }
        let now = batch.peek(slot);
        let differs = match before.map(|s| batch.peek(s)) {
            // A first binding changed the rows it gives a value. An object's every row is one.
            None if !matches!(now.data_type(), DataType::Struct(..)) => {
                (0..rows).map(|row| now.is_valid(row)).collect()
            }
            None => vec![true; rows],
            Some(was) => differs(was, now).unwrap_or_else(|| {
                // Unclaimed fields serialize as an object's own keys, under no name.
                let field = if name.ends_with('*') { tree::UNCLAIMED } else { "v" };
                let (was, now) = (
                    RecordView::from_named([(field, was)]),
                    RecordView::from_named([(field, now)]),
                );
                (0..rows).map(|row| was.row(row) != now.row(row)).collect()
            }),
        };
        for (changed, _) in changed.iter_mut().zip(differs).filter(|(_, differs)| *differs) {
            changed.push((name, step));
        }
    }
    Touched { changed, dropped_by }
}

/// Per row, whether `was` and `now` hold different values, read straight from two columns of one type held row by
/// row. `None` for any other pair, which only their JSON can compare.
fn differs(was: &Column, now: &Column) -> Option<Vec<bool>> {
    fn by_row(a: &Validity, b: &Validity, len: usize, same: impl Fn(usize) -> bool) -> Vec<bool> {
        (0..len)
            .map(|row| match (a.is_valid(row), b.is_valid(row)) {
                (true, true) => !same(row),
                (a, b) => a != b,
            })
            .collect()
    }
    let len = now.len();
    if was.data_type() != now.data_type() {
        return None;
    }
    Some(match now.data_type() {
        DataType::Bool => {
            let (a, b) = (was.bool().ok()?, now.bool().ok()?);
            by_row(a.validity, b.validity, len, |r| {
                bits::get_row(a.values, r) == bits::get_row(b.values, r)
            })
        }
        DataType::I64 => {
            let (a, b) = (was.i64().ok()?, now.i64().ok()?);
            by_row(a.validity, b.validity, len, |r| a.values[r] == b.values[r])
        }
        DataType::DateTime => {
            let (a, b) = (was.datetime().ok()?, now.datetime().ok()?);
            by_row(a.validity, b.validity, len, |r| a.values[r] == b.values[r])
        }
        DataType::Duration => {
            let (a, b) = (was.duration().ok()?, now.duration().ok()?);
            by_row(a.validity, b.validity, len, |r| a.values[r] == b.values[r])
        }
        DataType::F64 => {
            let (a, b) = (was.f64().ok()?, now.f64().ok()?);
            by_row(a.validity, b.validity, len, |r| {
                a.values[r].to_bits() == b.values[r].to_bits()
            })
        }
        DataType::Str | DataType::Bytes => {
            let (a, b) = (was.var().ok()?, now.var().ok()?);
            by_row(a.validity, b.validity, len, |r| a.get(r) == b.get(r))
        }
        _ => return None,
    })
}

/// `touched` written onto `rows`, each name spread over the leaves of the row at it as the editor names them, and each
/// step named by the step of the pipeline it is in: a called pipeline's steps by the call (`calls_of`).
fn attribute(calls: &HashMap<String, String>, plan: &Plan, touched: Touched<'_>, rows: &mut [(RowOut, bool)]) {
    let step = |f: FunctionId| {
        let id = plan.step_ids.get(f as usize)?;
        Some(calls.get(id).unwrap_or(id).clone())
    };
    let claimed: HashSet<String> = plan.tree.leaves().into_iter().map(|(name, _)| name).collect();
    for ((row, _), (changed, dropped)) in rows.iter_mut().zip(touched.changed.into_iter().zip(touched.dropped_by)) {
        row.dropped_by = dropped.and_then(step);
        let Some(json) = &row.json else {
            continue;
        };
        let leaves = leaves(json);
        let mut steps: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for (name, f) in changed {
            let Some(id) = step(f) else {
                continue;
            };
            let (prefix, unclaimed) = match name.strip_suffix('*') {
                Some(object) => (object.trim_end_matches('.'), true),
                None => (name, false),
            };
            let at = leaves
                .range::<str, _>((std::ops::Bound::Included(prefix), std::ops::Bound::Unbounded))
                .take_while(|leaf| leaf.starts_with(prefix))
                .filter(|leaf| under(leaf, prefix) && !(unclaimed && claimed.contains(*leaf)));
            for leaf in at {
                let ids = steps.entry(leaf.clone()).or_default();
                if ids.last() != Some(&id) {
                    ids.push(id.clone());
                }
            }
        }
        row.steps = (!steps.is_empty()).then_some(steps);
    }
}

/// Whether `leaf` is `name` or a leaf of the object at it. `""` is the record.
fn under(leaf: &str, name: &str) -> bool {
    name.is_empty()
        || leaf
            .strip_prefix(name)
            .is_some_and(|rest| rest.is_empty() || rest.starts_with('.') || rest.starts_with('['))
}

/// Each step a `call` of the pipeline with id `pipeline` (or of the first) brought in, by the call's id.
fn calls_of(config: &serde_json::Value, pipeline: Option<&str>) -> Option<HashMap<String, String>> {
    let pipelines = config["pipelines"].as_array()?;
    let own = match pipeline {
        Some(id) => pipelines.iter().find(|p| p["id"] == id)?,
        None => pipelines.first()?,
    };
    let mut calls = HashMap::new();
    let mut walk = vec![(own["steps"].as_array()?.clone(), None::<String>)];
    while let Some((steps, call)) = walk.pop() {
        for step in steps {
            let id = step["id"].as_str().unwrap_or_default().to_string();
            if let Some(call) = &call {
                calls.insert(id.clone(), call.clone());
            }
            let by = call.clone().unwrap_or(id);
            if step["type"] == CALL {
                let callee = pipelines.iter().find(|p| p["id"] == step["pipeline"]);
                if let Some(steps) = callee.and_then(|p| p["steps"].as_array()) {
                    walk.push((steps.clone(), Some(by)));
                }
            } else {
                for branch in ["then", "else"] {
                    if let Some(steps) = step[branch].as_array() {
                        walk.push((steps.clone(), call.clone()));
                    }
                }
            }
        }
    }
    Some(calls)
}

const CALL: &str = "call";

/// A record's leaves by name, as the editor names them. An empty object or an array is one leaf, as the editor draws
/// it.
fn leaves(record: &serde_json::Value) -> BTreeSet<String> {
    fn walk(value: &serde_json::Value, path: &mut String, into: &mut BTreeSet<String>) {
        match value {
            serde_json::Value::Object(object) if !object.is_empty() => {
                for (key, value) in object {
                    let len = path.len();
                    plan::tree::push_key(path, key);
                    walk(value, path, into);
                    path.truncate(len);
                }
            }
            _ if !path.is_empty() => {
                into.insert(path.clone());
            }
            _ => {}
        }
    }
    let mut into = BTreeSet::new();
    walk(record, &mut String::new(), &mut into);
    into
}

/// What one test made of the config on a branch.
#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TestResult {
    pub id: String,
    pub name: String,
    /// `None` for an end-to-end test, which names no pipeline.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pipeline: Option<String>,
    /// Which area this ran in -- ids are unique only within an area, the same collision
    /// `AreaTestSummary`'s doc names.
    pub area: String,
    /// Absent when the run could not happen at all -- a sample that is gone, a pipeline that does not compile.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub verdict: Option<Verdict>,
    /// Why there is no verdict. A test that could not run is not a test that failed, and is not reported as one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Run `test` and compare it against what it locked. `pipeline` is the discriminant: `Some` runs the pipeline
/// preview over the sample, `None` routes the sample by its source (the route preview) instead.
/// `sample` is the test's sample as the branch holds it, `None` when the branch has none by that id.
pub fn run_test(
    test: &config_file::TestDto,
    scope: &str,
    config: &str,
    sample: Result<Option<config_file::SampleDto>, String>,
    declared_at: &config_file::DeclaredAt,
) -> TestResult {
    let mut result = TestResult {
        id: test.id.clone(),
        name: test.name.clone(),
        pipeline: test.pipeline.clone(),
        area: scope.to_string(),
        verdict: None,
        error: None,
    };
    let sample = match sample {
        Ok(Some(sample)) => sample,
        Ok(None) => {
            result.error = Some(format!(
                "this test runs over the sample {:?}, which is not on this branch",
                test.sample
            ));
            return result;
        }
        Err(e) => {
            result.error = Some(e);
            return result;
        }
    };

    let rows_in = sample.records.len();
    let actual = match &test.pipeline {
        Some(pipeline) => run_pipeline_actual(
            pipeline,
            config,
            test.locked_at,
            declared_at.clone(),
            &sample.columns,
            sample.records,
        ),
        None => run_route_actual(config, test.locked_at, declared_at.clone(), sample),
    };
    match actual {
        Ok(actual) => result.verdict = Some(config_file::compare(test, rows_in, &actual)),
        Err(message) => result.error = Some(message),
    }
    result
}

/// A pipeline test's run: the pipeline preview over the sample's records, pinned to the instant the test locked.
pub fn run_pipeline_actual(
    pipeline: &str,
    config: &str,
    locked_at: i64,
    declared_at: config_file::DeclaredAt,
    columns: &[crate::heartbeat::CaptureColumn],
    records: Vec<crate::io::RecordObject>,
) -> Result<Vec<Option<serde_json::Value>>, String> {
    let previewed = run_preview(
        PreviewRequest {
            config: config.to_string(),
            pipeline: Some(pipeline.to_string()),
            sample: None,
            show_internal: false,
            at_micros: Some(locked_at),
            filter: None,
            attribution: false,
            after_dot: None,
            capture_filter: None,
        },
        declared_at,
        columns,
        records,
    );
    if !previewed.succeeded() {
        return Err(previewed
            .errors
            .iter()
            .map(|e| e.message.clone())
            .collect::<Vec<_>>()
            .join("; "));
    }
    Ok(previewed.body.rows.into_iter().map(|r| r.json).collect())
}

/// An end-to-end test's run: the route preview over the sample, flattened -- per record, per matched route, per
/// document -- into one list position-aligned with `TestDto.expected`, each document packed with where it went
/// (`config_file::TestDto::end_to_end_document`) the same way locking packs it.
pub fn run_route_actual(
    config: &str,
    locked_at: i64,
    declared_at: config_file::DeclaredAt,
    sample: config_file::SampleDto,
) -> Result<Vec<Option<serde_json::Value>>, String> {
    let resp = run_route_preview(config, sample, declared_at, Some(locked_at));
    if !resp.succeeded() {
        return Err(resp
            .errors
            .iter()
            .map(|e| e.message.clone())
            .collect::<Vec<_>>()
            .join("; "));
    }
    if let Some(row) = resp.rows.iter().find(|r| r.refused.is_some()) {
        return Err(format!(
            "record {}: {}",
            row.row,
            row.refused
                .as_deref()
                .unwrap_or("asked for more memory than a preview may hold")
        ));
    }

    let mut actual = Vec::new();
    for row in &resp.rows {
        for through in &row.through {
            let route = resp
                .route_ids
                .get(through.route)
                .map(String::as_str)
                .unwrap_or_default();
            let pipeline = resp
                .route_pipeline_ids
                .get(through.route)
                .map(String::as_str)
                .unwrap_or_default();
            for doc in &through.documents {
                let destinations: Vec<String> = doc
                    .destinations
                    .iter()
                    .map(|d| resp.destination_names.get(d).cloned().unwrap_or_else(|| d.to_string()))
                    .collect();
                let document = serde_json::from_str(&doc.document).expect("route preview produced valid JSON");
                actual.push(Some(config_file::TestDto::end_to_end_document(
                    document,
                    pipeline,
                    route,
                    destinations,
                )));
            }
        }
    }
    Ok(actual)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A bundle whose pipeline sets `u` and `n` and then has a step `s`, routed from a Kafka source with a condition and
    /// a `when` being typed. A second pipeline sets `u` and `n` of other types, and both routes deliver to `both`.
    const BUNDLE: &str = r#"{"version":1,
        "sources":[{"id":"in","type":"kafka","brokers":"b:9092","topics":["t"],"groupId":"g"}],
        "destinations":[{"id":"out","type":"kafka","brokers":"b:9092","defaultTopic":"t"},
            {"id":"both","type":"kafka","brokers":"b:9092","defaultTopic":"t"}],
        "pipelines":[{"id":"p","name":"P","steps":[
            {"id":"u","type":"code","body":"u = parseUrl(_raw); n = 1;"},
            {"id":"s","type":"code","body":"x = 1;"}]},
            {"id":"q","name":"Q","steps":[{"id":"v","type":"code","body":"u = {path: \"/\", seen: true}; n = 1.5;"}]}],
        "routes":[{"id":"r","source":"in","pipeline":"p","condition":"_raw.","deliver":[{"when":"n.","to":["out","both"]}]},
            {"id":"r2","source":"in","pipeline":"q","deliver":[{"to":["both"]}]}]}"#;

    /// `BUNDLE` without the condition and the `when` being typed: a destination's filter compiles over the whole
    /// topology, which compiles only once they are written.
    fn compiling() -> String {
        BUNDLE
            .replace(r#""condition":"_raw.","#, "")
            .replace(r#""when":"n.","#, "")
    }

    /// The schemas `producing` declares: `stamped`, of a `@timestamp`, and `login`, which requires `time` and `user`.
    const SCHEMAS: &str = r#""schemas":[
        {"id":"stamped","title":"Stamped","fields":[{"field":"@timestamp","type":"datetime"}]},
        {"id":"login","title":"Login","fields":[
            {"field":"time","type":"i64","required":true},
            {"field":"user","type":"{...}","required":true},
            {"field":"user.name","type":"str"}]}],"#;

    /// `compiling`, declaring `SCHEMAS`, its pipeline producing `schemas`.
    fn producing(schemas: &str) -> String {
        compiling()
            .replacen(r#"{"version":1,"#, &format!(r#"{{"version":1,{SCHEMAS}"#), 1)
            .replace(
                r#""id":"p","name":"P","#,
                &format!(r#""id":"p","name":"P","produces":{schemas},"#),
            )
    }

    fn warnings_of(config: String, columns: &[crate::heartbeat::CaptureColumn]) -> Vec<(Option<String>, String)> {
        (previewed(config, columns).warnings.iter())
            .map(|w| (w.step.clone(), w.message.clone()))
            .collect()
    }

    fn previewed(config: String, columns: &[crate::heartbeat::CaptureColumn]) -> Preview {
        run_preview(
            PreviewRequest {
                config,
                pipeline: Some("p".into()),
                sample: None,
                show_internal: false,
                at_micros: None,
                filter: None,
                attribution: false,
                after_dot: None,
                capture_filter: None,
            },
            Default::default(),
            columns,
            Vec::new(),
        )
        .body
    }

    #[test]
    fn the_fields_a_schema_requires_and_the_rows_lack_are_listed_with_their_types() {
        let missing = previewed(producing(r#"["login"]"#), &[]).missing;
        let time = missing.iter().find(|m| m.path == "time").unwrap();
        assert_eq!((time.written_as.as_str(), time.schema.as_str()), ("i64", "Login"));
        assert_eq!(missing.iter().find(|m| m.path == "user").unwrap().written_as, "{...}");
        assert_eq!(missing.len(), 2, "{missing:?}");
    }

    #[test]
    fn a_field_a_schema_types_otherwise_is_warned_at_the_step_that_wrote_it() {
        let body = r#""body":"$[\"@timestamp\"] = \"now\"; source = {ip: \"::1\"};""#;
        let config = producing(r#"["stamped"]"#).replace(r#""body":"x = 1;""#, body);
        assert_eq!(
            warnings_of(config, &[]),
            [(
                Some("s".to_string()),
                "`@timestamp` is str, but Stamped has it as datetime. Write it as datetime.".to_string()
            )]
        );
        // A pipeline that produces no schema is held to none.
        let config = compiling().replace(r#""body":"x = 1;""#, body);
        assert_eq!(warnings_of(config, &[]), []);
    }

    #[test]
    fn a_field_the_source_gave_is_warned_at_the_first_step() {
        let column = |name: &str| crate::heartbeat::CaptureColumn {
            name: name.into(),
            ty: "str".into(),
        };
        let columns = [column("_raw"), column("@timestamp")];
        assert_eq!(
            warnings_of(producing(r#"["stamped"]"#), &columns),
            [(
                Some("u".to_string()),
                "`@timestamp` is str, but Stamped has it as datetime. Write it as datetime.".to_string()
            )]
        );
    }

    #[test]
    fn a_field_the_schema_requires_and_no_step_writes_is_warned_at_no_step() {
        let warned = warnings_of(producing(r#"["login"]"#), &[]);
        assert_eq!(
            warned,
            [(None, "Login requires `time`, `user`; the rows lack them.".to_string())]
        );
    }

    fn after(text: &str, at: DotAt) -> Option<AfterDot> {
        let config = match at {
            DotAt::Destination(_) => compiling(),
            _ => BUNDLE.into(),
        };
        let previewed = run_preview(
            PreviewRequest {
                config,
                pipeline: Some("p".into()),
                sample: None,
                show_internal: false,
                at_micros: None,
                filter: None,
                attribution: false,
                after_dot: Some(AfterDotRequest {
                    dot: text.len() - 1,
                    text: text.into(),
                    at,
                }),
                capture_filter: None,
            },
            Default::default(),
            &[],
            Vec::new(),
        );
        assert!(
            previewed.succeeded(),
            "{:?}",
            previewed.errors.iter().map(|e| &e.message).collect::<Vec<_>>()
        );
        previewed.body.after_dot
    }

    fn method<'a>(answer: &'a AfterDot, name: &str) -> &'a plan::dot::Method {
        answer.methods.iter().find(|m| m.name == name).unwrap()
    }

    /// A step reads what the steps before it wrote, though the step itself does not compile yet.
    #[test]
    fn a_step_reads_the_record_the_steps_before_it_wrote() {
        let answer = after("x = u.", DotAt::Step("s".into())).unwrap();
        assert!(
            answer.ty.as_deref().is_some_and(|t| t.starts_with('{')),
            "{:?}",
            answer.ty
        );
        assert!(
            answer.fields.contains(&("path".into(), "str".into())),
            "{:?}",
            answer.fields
        );
        assert!(method(&answer, "keys").fits);
        assert!(!method(&answer, "sign").fits);
    }

    /// A code step reads the names its own statements before the dot bind, `let`s among them.
    #[test]
    fn a_step_reads_what_its_own_statements_before_the_dot_bind() {
        let local = after("let v = parseUrl(_raw); w = v.path.", DotAt::Step("s".into())).unwrap();
        assert_eq!(local.ty.as_deref(), Some("str"));
        let inner = after("if (n == 1) { let v = [1]; x = v.", DotAt::Step("s".into())).unwrap();
        assert_eq!(inner.ty.as_deref(), Some("array<i64>"));
    }

    /// A source's record is what it gives as it arrives; a `when`, and a destination's filter, the pipeline's output.
    #[test]
    fn a_source_and_a_when_read_their_own_record() {
        let raw = after("_raw.", DotAt::Source("in".into())).unwrap();
        assert_eq!(raw.ty.as_deref(), Some("str"));
        assert!(method(&raw, "toLowerCase").fits);
        assert_eq!(method(&raw, "sign").takes, "i64/f64");
        assert!(!method(&raw, "sign").fits);

        let n = after("n.", DotAt::When("r".into())).unwrap();
        assert_eq!(n.ty.as_deref(), Some("i64"));
        // A capture filter at a destination reads what the routes delivering there give it.
        assert_eq!(
            after("n.", DotAt::Destination("out".into())).unwrap().ty.as_deref(),
            Some("i64")
        );
        assert!(after("n.", DotAt::Source("in".into())).unwrap().ty.is_none());
    }

    /// A destination fed by routes of different types reads what any of them gives.
    #[test]
    fn a_destination_reads_what_any_route_delivering_there_gives() {
        let at = || DotAt::Destination("both".into());
        assert_eq!(after("n.", at()).unwrap().ty.as_deref(), Some("f64"));
        let u = after("u.", at()).unwrap();
        assert!(u.fields.contains(&("path".into(), "str".into())), "{:?}", u.fields);
        assert!(u.fields.contains(&("domain".into(), "str".into())), "{:?}", u.fields);
        assert!(u.fields.contains(&("seen".into(), "bool".into())), "{:?}", u.fields);
        assert_eq!(after("x.", at()).unwrap().ty.as_deref(), Some("i64"));
        assert_eq!(after("nope.", at()).unwrap().ty, None);
    }

    /// A capture filter at a destination runs on the routes it compiles on and names each it passes over.
    #[test]
    fn a_capture_filter_at_a_destination_names_the_routes_it_skips() {
        let check = |text: &str| {
            let previewed = run_preview(
                PreviewRequest {
                    config: compiling(),
                    pipeline: None,
                    sample: None,
                    show_internal: false,
                    at_micros: None,
                    filter: None,
                    attribution: false,
                    after_dot: None,
                    capture_filter: Some(CaptureFilterRequest {
                        text: text.into(),
                        destination: "both".into(),
                    }),
                },
                Default::default(),
                &[],
                Vec::new(),
            );
            assert!(previewed.succeeded());
            previewed.body.capture_filter.unwrap()
        };
        let both = check("n > 1");
        assert_eq!((both.reads, both.skips.len()), (2, 0));
        let one = check("x > 1");
        assert_eq!(one.reads, 1);
        assert_eq!(one.skips.iter().map(|s| s.route.as_str()).collect::<Vec<_>>(), ["r2"]);
        assert!(one.skips[0].error.contains('x'), "{}", one.skips[0].error);
        assert_eq!(check("nope > 1").reads, 0);
    }

    /// A dot in a string follows no value; an id the bundle does not hold fails.
    #[test]
    fn a_dot_that_follows_no_value_has_no_answer() {
        assert!(after(r#"x == "a."#, DotAt::Source("in".into())).is_none());
        let previewed = run_preview(
            PreviewRequest {
                config: BUNDLE.into(),
                pipeline: None,
                sample: None,
                show_internal: false,
                at_micros: None,
                filter: None,
                attribution: false,
                after_dot: Some(AfterDotRequest {
                    text: "_raw.".into(),
                    dot: 4,
                    at: DotAt::Source("nope".into()),
                }),
                capture_filter: None,
            },
            Default::default(),
            &[],
            Vec::new(),
        );
        assert!(!previewed.succeeded());
    }

    /// An unroll copies none of a record's payload into the rows it makes: a large record, its bulk deleted, unrolls
    /// into many rows within the budget, and they show no `__source.bytes`.
    #[test]
    fn an_unroll_of_a_large_record_stays_within_the_budget() {
        let items: Vec<serde_json::Value> = (0..40).map(|i| serde_json::json!({"i": i})).collect();
        let raw = serde_json::json!({"pad": "x".repeat(4_000_000), "items": items}).to_string();
        let mut record = crate::io::RecordObject::new();
        record.insert("_raw".into(), serde_json::Value::String(raw));
        let body =
            "let s = json(_raw); items: array<{i: i64}> = s.items; delete(_raw); unroll(items, false); $ = items;";
        let config = serde_json::json!({"version": 1,
            "sources": [{"id": "in", "type": "kafka", "brokers": "b:9092", "topics": ["t"], "groupId": "g"}],
            "destinations": [{"id": "out", "type": "kafka", "brokers": "b:9092", "defaultTopic": "t"}],
            "pipelines": [{"id": "p", "name": "P", "steps": [{"id": "u", "type": "code", "body": body}]}],
            "routes": [{"id": "r", "source": "in", "pipeline": "p", "deliver": [{"to": ["out"]}]}]})
        .to_string();
        let previewed = run_preview(
            PreviewRequest {
                config,
                pipeline: Some("p".into()),
                sample: None,
                show_internal: true,
                at_micros: None,
                filter: None,
                attribution: false,
                after_dot: None,
                capture_filter: None,
            },
            Default::default(),
            &[],
            vec![record],
        );
        assert!(previewed.succeeded(), "{:?}", previewed.errors);
        assert_eq!(previewed.body.rows.len(), 40);
        for row in &previewed.body.rows {
            let json = row.json.as_ref().unwrap();
            assert_eq!(json.pointer("/__source/bytes"), None, "{json}");
        }
    }
}
