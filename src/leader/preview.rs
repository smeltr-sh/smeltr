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
    /// A Bool over the pipeline's output, typed as a filter on a destination capture is: only the rows it is true
    /// for are returned. One that does not compile fails the preview at the `filter` stage.
    #[serde(default)]
    pub filter: Option<String>,
    /// Say, on each row, which steps changed each of its fields and which step dropped it (`touched`). It keeps every
    /// field's every value for the whole run and compares them, which costs about one more run.
    #[serde(default)]
    pub attribution: bool,
    /// Also return these rows and fields of the output as a sample holds them (`Preview::as_sample`), to be saved as
    /// one.
    #[serde(default)]
    pub as_sample: Option<ShownCut>,
    /// Also return the dictionary of what the pipeline writes (`Preview::dictionary`): its fields and an Elasticsearch
    /// mapping.
    #[serde(default)]
    pub dictionary: bool,
    /// What may follow a `.` in an expression of the config. Asked, the preview answers it instead of running the
    /// sample, and `Preview::after_dot` holds the answer.
    #[serde(default)]
    pub after_dot: Option<AfterDotRequest>,
    /// A capture filter at a destination, checked against each route delivering there. Asked, the preview answers it
    /// instead of running the sample, and `Preview::capture_filter` holds the answer.
    #[serde(default)]
    pub capture_filter: Option<CaptureFilterRequest>,
    /// A test of this pipeline over this sample, by id: the preview compares its rows against what the test locked
    /// (`Preview::verdict`).
    #[serde(default)]
    pub test: Option<String>,
    /// The test `test` names, as the handler read it. Absent when the preview runs over another sample than the
    /// test's.
    #[serde(skip)]
    pub locked: Option<config_file::TestDto>,
}

/// The fields and rows on screen: what a saved sample keeps of the output.
#[derive(serde::Deserialize)]
pub struct ShownCut {
    /// Leaf names, as the records grid names its columns. `None` keeps every field the rows hold.
    #[serde(default)]
    pub fields: Option<Vec<String>>,
    /// By `RowOut::row`.
    pub rows: Vec<u32>,
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
    /// The route's name (`LoadedSystem::route_names`).
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

/// A field a schema the pipeline produces types otherwise than the rows hold it: what its warning says, the field's
/// path, and the type the schemas agree on, which a declaration of it can take.
#[derive(Debug, serde::Serialize)]
pub struct SchemaFix {
    pub message: String,
    pub path: String,
    #[serde(rename = "type")]
    pub ty: config_file::DataTypeDto,
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
    /// Each grok step's fields, as the pipeline types them, by step id. Empty when the pipeline did not compile.
    pub parsed_fields: BTreeMap<String, Vec<config_file::FieldDto>>,
    /// The dictionary of what the pipeline writes (`plan::dictionary`), when asked. Absent when it did not compile.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dictionary: Option<DictionaryOut>,
    /// What compiles but is likely a mistake (`Plan::warnings`), each placed at its step as an error is.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<ApiError>,
    /// How many of `warnings` are where the rows break the schemas the pipeline produces.
    #[serde(skip_serializing_if = "is_zero")]
    pub schema_findings: u32,
    /// The warnings a field's declared type answers, each with that type.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub fixes: Vec<SchemaFix>,
    /// The fields the schemas the pipeline produces require or recommend and its rows lack, which the editor shows
    /// as columns.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub missing: Vec<crate::schema::Missing>,
    /// The answer to `PreviewRequest::after_dot`. Absent where the dot follows no value: it is in a string, a comment
    /// or a number.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub after_dot: Option<AfterDot>,
    /// The answer to `PreviewRequest::capture_filter`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub capture_filter: Option<CaptureFilterCheck>,
    /// Answers `PreviewRequest::as_sample`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub as_sample: Option<crate::heartbeat::CaptureBatch>,
    /// What these rows make of `PreviewRequest::test`: the run on screen is the test's run.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub verdict: Option<config_file::Verdict>,
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
            elasticsearch: crate::io::elastic_mapping::mapping(record, &Default::default()),
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
// out, with no source identity and no delivery. Routing selects on a source, so this one needs the source the
// records stand for, named by the sample or else by the request, and that one must not have it.

#[derive(serde::Deserialize)]
pub struct RoutePreviewRequest {
    /// A whole-bundle `config_file` JSON document: sources, destinations, routes and pipelines.
    pub config: String,
    /// The saved sample to route, by id. None where the caller hands it a held capture instead.
    #[serde(default)]
    pub sample: Option<String>,
    /// The source the records stand for where the sample or capture names none, such as a paste with none or a
    /// capture at a destination. A sample that names one keeps its own.
    #[serde(default)]
    pub source: Option<String>,
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
    /// Each route's name (`LoadedSystem::route_names`), by route index -- position-parallel to
    /// `route_pipelines`.
    pub route_names: Vec<String>,
    /// Destination names by id.
    pub destination_names: HashMap<u32, String>,
    /// The answer to `Unmatched`, with each destination by its author's id. Absent only on failure.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unmatched: Option<config_file::AnswerDto>,
    pub rows: Vec<RoutePreviewRow>,
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
}

/// `table`, a sample's, as `meta` reads it (`SourceMeta::read_table`): each field `meta` declares is read from the
/// row under `meta`'s type for it, whatever the sample was captured under.
pub fn read_sample(meta: &SourceMeta, table: &crate::heartbeat::CaptureBatch) -> Result<SampleRead, String> {
    let read = meta.read_table(table)?;
    let raw = plan::topology::Entry::Field("_raw".into());
    let texts = (table.rows.iter().zip(&read))
        .map(|(row, read)| {
            let cells = crate::io::Cells {
                columns: &table.columns,
                row,
            };
            match cells.of(&raw, &[]) {
                Ok(Some(text)) if text.is_string() => text.as_str().unwrap_or_default().to_string(),
                _ => String::from_utf8_lossy(meta.read_bytes(read).unwrap_or_default()).into_owned(),
            }
        })
        .collect();
    Ok(SampleRead { records: read, texts })
}

/// Every lookup table `config` declares, mapped to `at` -- the ordinary case, a bundle read from one place with no
/// merge behind it. A caller previewing a merge of two bundles (a group's test run, with `shared/` merged in) builds its own
/// `DeclaredAt` instead, one origin per table, rather than pointing every table at the same place.
pub fn single_origin_declared_at(config: &str, at: &config_file::BundleAt) -> config_file::DeclaredAt {
    serde_json::from_str::<config_file::FileConfig>(config)
        .map(|file| file.lookup_tables.iter().map(|t| (t.id.clone(), at.clone())).collect())
        .unwrap_or_default()
}

/// What `now()` reads in a run over `table`: the instant its rows were taken, so a preview and a test of them read one
/// instant. Rows that say none, saved before samples carried it, read the clock.
fn taken_at(table: &crate::heartbeat::CaptureBatch) -> i64 {
    table.instant().unwrap_or_else(crate::core::ctx::wall_clock_micros)
}

pub fn run_route_preview(
    config: &str,
    sample: config_file::SampleDto,
    declared_at: config_file::DeclaredAt,
) -> Envelope<RoutePreview> {
    let Some(source_name) = sample.source.clone() else {
        return Envelope::refused(Refusal::invalid(
            "these records do not say which source they stand for, and routing selects on a source -- \
             send the source they stand for in `source` to route them",
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
    let SampleRead { records, texts } = match read_sample(&compiled.meta_for(source.id), &sample.table) {
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
        taken_at(&sample.table),
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
        route_names: system.route_names(),
        destination_names: system
            .topology
            .destinations
            .iter()
            .map(|d| (d.id, d.name.clone()))
            .collect(),
        unmatched: Some(system.answer_by_name(system.topology.on_exception.answer(&ExceptionKind::Unmatched))),
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
    let names = system.route_names();
    for (idx, filter) in compiled.capture_filters(dest, &asked.text) {
        match filter {
            Ok(_) => check.reads += 1,
            Err(e) => check.skips.push(RouteSkip {
                route: names[idx].clone(),
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
    table: &crate::heartbeat::CaptureBatch,
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
        return match after_dot(asked, &system, req.pipeline.as_deref(), &table.columns) {
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
    let pipeline_id = match &req.pipeline {
        Some(id) => id.clone(),
        None => system.pipeline_ids.first().cloned().unwrap_or_default(),
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
    let meta = match SourceMeta::of_columns(&table.columns) {
        Ok(meta) => meta,
        Err(e) => return Preview::failed("sample", vec![ApiError::new(e)], pipeline_names),
    };
    let SampleRead {
        records: sourced,
        texts,
    } = match read_sample(&meta, table) {
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
                Err(e) => {
                    // Text that does not parse is no expression, so the find box searches for it as written.
                    let stage = if plan::compile::parse(src, None).is_ok() {
                        "filter"
                    } else {
                        "filterParse"
                    };
                    return Preview::failed(stage, vec![ApiError::new(e.to_string())], pipeline_names);
                }
            }
        }
    };
    if table.rows.is_empty() {
        let (found, fixes, missing) = schema_warnings(system.produced_by(pipeline_cfg.id), &plan, &record, first);
        warnings.extend(found.iter().map(warn));
        return Envelope::ok(Preview {
            stage: None,
            pipeline_names,
            after_dot: None,
            capture_filter: None,
            as_sample: None,
            verdict: None,
            rows: vec![],
            unfiltered: filter.as_ref().map(|_| 0),
            parsers: vec![],
            parsed_fields: parsed_fields(&plan),
            dictionary: req.dictionary.then(|| DictionaryOut {
                compiled: Dictionary::of(&record),
                sampled: None,
            }),
            warnings,
            schema_findings: found.len() as u32,
            fixes,
            missing,
        });
    }

    let mut pool = BufferPool::with_budget(crate::runtime::preview_memory_budget());
    let mut errors = ErrorSink::new();
    // Outside the budget guard, like the errors: a refusal still leaves whatever the parse got to say.
    let mut tally = crate::core::tally::FieldTally::new();
    let at_micros = taken_at(table);

    // Budgeted: the leader is also what serves the GUI, the config API and every worker's heartbeat, and a pipeline
    // that unrolls an array asks for far more than the sample weighs.
    let previewed = buffer::catch_over_budget(|| {
        let mut batch = Batch::new(&plan.delivery_layout(), table.rows.len(), 0);

        // A byte record's `_raw` is text (`core::batch`), and a sample's records are text by the time they are
        // stored -- a paste arrives as text and a capture decodes with its source's encoding, so nothing decodes here.
        let text: Vec<std::borrow::Cow<'_, str>> =
            texts.iter().map(|t| std::borrow::Cow::Borrowed(t.as_str())).collect();
        // Each record's `__source` fields are its own columns, so no source names the batch.
        meta.ingest(&mut batch, &sourced, &text, None, |_| true, &mut pool);

        let mut ctx = Ctx::at(&mut pool, &mut errors, at_micros).tallying(&mut tally);
        let rows = run_to_json_marked(
            &plan,
            on_exception,
            &mut batch,
            &mut ctx,
            req.show_internal || req.locked.is_some(),
            filter.as_ref(),
        );
        // An unroll makes rows of its own, which no longer line up with the sample's.
        let touched = (req.attribution && batch.rows() == table.rows.len()).then(|| touched(&plan, &batch));
        let sample = req.as_sample.as_ref().map(|cut| {
            let made = crate::leader::capture::output_fields(at_micros, &pipeline_id, req.sample.as_deref());
            as_sample(&plan, &on_exception.tag_field, &batch, &rows, cut, &made)
        });
        batch.reclaim(&mut pool);
        (rows, touched, sample)
    });
    let (preview_rows, touched, as_sample) = match previewed {
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
    // Every row, as a test runs, whatever the filter keeps on screen.
    let verdict = req.locked.as_ref().map(|test| {
        let run = config_file::Run {
            made: (rows.iter())
                .map(|(r, _)| config_file::Made {
                    row: r.row as usize,
                    document: r.json.clone(),
                    sent: None,
                })
                .collect(),
            types: plan::dictionary::fields(&sampled).into_iter().collect(),
        };
        config_file::compare(test, table.rows.len(), &run)
    });
    if req.locked.is_some() && !req.show_internal {
        for object in rows.iter_mut().filter_map(|(r, _)| r.json.as_mut()?.as_object_mut()) {
            object.retain(|key, _| !crate::plan::tree::is_internal(key));
        }
    }
    let (found, fixes, missing) = schema_warnings(system.produced_by(pipeline_cfg.id), &plan, &sampled, first);
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
        dictionary: req.dictionary.then(|| DictionaryOut {
            compiled: Dictionary::of(&record),
            sampled: Some(Dictionary::of(&sampled)),
        }),
        rows,
        unfiltered,
        parsers,
        parsed_fields: parsed_fields(&plan),
        warnings,
        schema_findings: found.len() as u32,
        fixes,
        missing,
        as_sample,
        verdict,
    })
}

/// Each grok step's fields in `plan`, by step id, spelled as a declaration spells them.
fn parsed_fields(plan: &Plan) -> BTreeMap<String, Vec<config_file::FieldDto>> {
    let mut out = BTreeMap::new();
    for i in &plan.instrs {
        if let plan::compile::Op::Call {
            outputs,
            call: plan::compile::Call::Parse { kind, .. },
            ..
        } = &i.op
            && matches!(kind, plan::config::ParserKind::Grok(_))
            && let Some(step) = plan.step_ids.get(i.function as usize)
        {
            let field = |o: &plan::compile::Output| config_file::FieldDto {
                name: o.name.clone(),
                ty: type_dto(&o.ty, o.time_source.as_ref()),
            };
            out.insert(step.clone(), outputs.iter().map(field).collect());
        }
    }
    out
}

/// `ty` spelled as a declaration spells it, a `datetime` with the format it is read by.
fn type_dto(ty: &DataType, time_source: Option<&plan::config::TimeSource>) -> config_file::DataTypeDto {
    match time_source {
        Some(plan::config::TimeSource::Format(format)) => {
            config_file::DataTypeDto::Compound(config_file::CompoundTypeDto::DateTime { format: format.clone() })
        }
        _ => config_file::DataTypeDto::Scalar(ty.to_string()),
    }
}

/// The rows and fields of `batch` that `cut` names, as a sample holds them: each row as a capture takes it, typed as
/// `plan` finished it. In row order.
fn as_sample(
    plan: &Plan,
    tag_field: &str,
    batch: &Batch,
    rows: &[plan::output::PreviewRow],
    cut: &ShownCut,
    made: &[(crate::heartbeat::CaptureColumn, serde_json::Value)],
) -> crate::heartbeat::CaptureBatch {
    let finished = crate::worker::capture::Finished::of(plan, tag_field);
    let mut taken = Vec::new();
    for shown in rows.iter().filter(|r| cut.rows.contains(&r.row)) {
        let Some(doc) = &shown.json else {
            continue;
        };
        let document: serde_json::Value = serde_json::from_slice(doc).expect("serializer produced valid JSON");
        let mut object = match &cut.fields {
            Some(fields) => config_file::kept(&document, fields),
            None => document.as_object().cloned().unwrap_or_default(),
        };
        // A `__` field is the plan's, which `finished` reads from the batch.
        object.retain(|key, _| !tree::is_internal(key));
        let (columns, row) = finished.row(batch, shown.row as usize, object);
        taken.push(crate::heartbeat::CaptureBatch {
            columns,
            rows: vec![row],
        });
    }
    let mut table = config_file::sample_table(&taken);
    // The fields on screen, and no others: a field the pipeline writes but no row shown holds is not one. How the
    // output was made stands in for how its input was captured.
    let holds = |i: usize| table.rows.iter().any(|row| !row.values[i].is_null());
    let keep: Vec<bool> = (table.columns.iter().enumerate())
        .map(|(i, c)| {
            !c.name.starts_with("__capture.")
                && (cut.fields.as_ref().is_none_or(|f| f.contains(&c.name))
                    || (!tree::is_internal(&c.name) && holds(i)))
        })
        .collect();
    table.keep_columns(&keep);
    for (column, value) in made {
        table.columns.push(column.clone());
        table.rows.iter_mut().for_each(|row| row.values.push(value.clone()));
    }
    table
}

/// The schemas the pipeline produces (`schema::check`) as `record`, the fields `plan` writes, breaks them, in three
/// lists. The first warns of each field the record holds at a type a schema gives otherwise, at the step that wrote
/// it, or at `first` where the source gave it, and of each field a schema requires and the record lacks, at no step.
/// The second holds the fix for each of those warnings that a declaration of its field answers. The third holds every
/// field the schemas require or recommend and the record lacks; a field only recommended is in this list and has no
/// warning in the first.
fn schema_warnings(
    produces: &[Arc<Schema>],
    plan: &Plan,
    record: &DataType,
    first: Option<FunctionId>,
) -> (Vec<CompileError>, Vec<SchemaFix>, Vec<crate::schema::Missing>) {
    let findings = crate::schema::check(produces, record);
    let mut fixes = Vec::new();
    let warned = crate::schema::told(produces, &findings)
        .into_iter()
        .filter(|t| t.breaks)
        .map(|t| {
            let at = (t.keys.as_ref()).and_then(|keys| plan::dictionary::writer(plan, keys).or(first));
            let warning = CompileError::new(at, t.message);
            if let Some(path) = t.keys.map(|keys| keys.join("."))
                && let Some(ty) = crate::schema::read_as(produces, &path)
            {
                fixes.push(SchemaFix {
                    message: warning.message.clone(),
                    path,
                    ty: type_dto(&ty, None),
                });
            }
            warning
        })
        .collect();
    (warned, fixes, crate::schema::missing(produces, &findings))
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
                let place = match name.ends_with('*') {
                    true => tree::Place::Unclaimed(String::new()),
                    false => tree::Place::Leaf("v".to_string()),
                };
                let (was, now) = (
                    RecordView::from_places([(place.clone(), was)]),
                    RecordView::from_places([(place, now)]),
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
    /// The sample it runs over; `None` when the test itself could not be read.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sample: Option<String>,
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
        sample: Some(test.sample.clone()),
        area: scope.to_string(),
        verdict: None,
        error: None,
    };
    match run_made(test.pipeline.as_deref(), &test.sample, config, sample, declared_at) {
        Ok((rows_in, run)) => result.verdict = Some(config_file::compare(test, rows_in, &run)),
        Err(message) => result.error = Some(message),
    }
    result
}

/// What locking `lock` holds: the same run [`run_test`] compares, so a test passes the moment it is locked. `sample`
/// is as [`run_test`] takes it.
pub fn lock_test(
    lock: &config_file::Lock,
    config: &str,
    sample: Result<Option<config_file::SampleDto>, String>,
    declared_at: &config_file::DeclaredAt,
) -> Result<config_file::Locked, String> {
    let (rows_in, run) = run_made(lock.pipeline.as_deref(), &lock.sample, config, sample, declared_at)?;
    Ok(lock.locked(rows_in, &run))
}

/// How many records the sample `sample_id` holds, and what a test's run over it makes: `pipeline`'s preview, or with
/// none the route preview.
fn run_made(
    pipeline: Option<&str>,
    sample_id: &str,
    config: &str,
    sample: Result<Option<config_file::SampleDto>, String>,
    declared_at: &config_file::DeclaredAt,
) -> Result<(usize, config_file::Run), String> {
    let sample =
        sample?.ok_or_else(|| format!("this test runs over the sample {sample_id:?}, which is not on this branch"))?;
    let rows_in = sample.table.rows.len();
    let run = match pipeline {
        Some(pipeline) => run_pipeline_actual(pipeline, config, declared_at.clone(), &sample.table),
        None => run_route_actual(config, declared_at.clone(), sample),
    }?;
    Ok((rows_in, run))
}

/// A pipeline test's run: the pipeline preview over the sample's records, as the editor shows it, its `__` fields too,
/// each field typed as its dictionary types it.
pub fn run_pipeline_actual(
    pipeline: &str,
    config: &str,
    declared_at: config_file::DeclaredAt,
    table: &crate::heartbeat::CaptureBatch,
) -> Result<config_file::Run, String> {
    let previewed = run_preview(
        PreviewRequest {
            config: config.to_string(),
            pipeline: Some(pipeline.to_string()),
            sample: None,
            show_internal: true,
            filter: None,
            attribution: false,
            as_sample: None,
            dictionary: true,
            after_dot: None,
            capture_filter: None,
            test: None,
            locked: None,
        },
        declared_at,
        table,
    );
    if let Some(why) = previewed.why_failed() {
        return Err(why);
    }
    let types = (previewed.body.dictionary.and_then(|d| d.sampled))
        .map(|d| d.fields.into_iter().collect())
        .unwrap_or_default();
    let made = (previewed.body.rows.into_iter())
        .map(|r| config_file::Made {
            row: r.row as usize,
            document: r.json,
            sent: None,
        })
        .collect();
    Ok(config_file::Run { made, types })
}

/// An end-to-end test's run: the route preview over the sample, flattened per record, per route that carried it, per
/// document, each document with where it went, and each field typed as its JSON is. A record that made no document is one entry making none, so a test
/// can check that too.
pub fn run_route_actual(
    config: &str,
    declared_at: config_file::DeclaredAt,
    sample: config_file::SampleDto,
) -> Result<config_file::Run, String> {
    let resp = run_route_preview(config, sample, declared_at);
    if let Some(why) = resp.why_failed() {
        return Err(why);
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

    let mut made = Vec::new();
    for row in &resp.rows {
        let before = made.len();
        for through in &row.through {
            let name = |names: &[String]| names.get(through.route).cloned().unwrap_or_default();
            for doc in &through.documents {
                let destinations: Vec<String> = doc
                    .destinations
                    .iter()
                    .map(|d| resp.destination_names.get(d).cloned().unwrap_or_else(|| d.to_string()))
                    .collect();
                made.push(config_file::Made {
                    row: row.row,
                    document: Some(serde_json::from_str(&doc.document).expect("route preview produced valid JSON")),
                    sent: Some(config_file::Sent {
                        pipeline: name(&resp.route_pipeline_ids),
                        route: name(&resp.route_ids),
                        destinations,
                    }),
                });
            }
        }
        if made.len() == before {
            made.push(config_file::Made {
                row: row.row,
                document: None,
                sent: None,
            });
        }
    }
    // A document is sent as JSON, so its types are what its JSON says, as a destination reads it.
    let open = DataType::Struct(Vec::new(), true);
    let sent = plan::dictionary::with_sample(&open, made.iter().filter_map(|m| m.document.as_ref()));
    let types = plan::dictionary::fields(&sent).into_iter().collect();
    Ok(config_file::Run { made, types })
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
            {"field":"time","type":"i64","requirement":"required"},
            {"field":"user","type":"{...}","requirement":"required"},
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

    fn cells(table: &crate::heartbeat::CaptureBatch, row: usize) -> crate::io::Cells<'_> {
        crate::io::Cells {
            columns: &table.columns,
            row: &table.rows[row],
        }
    }

    /// What row `row` of `table` holds at `path`, null where it holds nothing.
    fn cell(table: &crate::heartbeat::CaptureBatch, row: usize, path: &str) -> serde_json::Value {
        let held = cells(table, row).of(&plan::topology::Entry::Field(path.to_string().into()), &[]);
        held.unwrap().map(|v| v.into_owned()).unwrap_or_default()
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
                filter: None,
                attribution: false,
                as_sample: None,
                dictionary: false,
                after_dot: None,
                capture_filter: None,
                test: None,
                locked: None,
            },
            Default::default(),
            &crate::heartbeat::CaptureBatch {
                columns: columns.to_vec(),
                rows: Vec::new(),
            },
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
    fn the_fields_a_schema_recommends_and_the_rows_lack_are_told_apart_from_its_warnings() {
        let config = producing(r#"["login"]"#).replace(
            r#"{"field":"user.name","type":"str"}"#,
            r#"{"field":"user.name","type":"str"},{"field":"message","type":"str","requirement":"recommended"}"#,
        );
        let preview = previewed(config, &[]);
        assert_eq!(preview.schema_findings, 1, "{:?}", preview.warnings);
        let message = preview.missing.iter().find(|m| m.path == "message").unwrap();
        assert_eq!(message.requirement, crate::config_file::Requirement::Recommended);
    }

    #[test]
    fn a_field_a_schema_types_otherwise_is_warned_at_the_step_that_wrote_it() {
        let body = r#""body":"$[\"@timestamp\"] = \"now\"; source = {ip: \"::1\"};""#;
        let config = producing(r#"["stamped"]"#).replace(r#""body":"x = 1;""#, body);
        assert_eq!(
            warnings_of(config, &[]),
            [(
                Some("s".to_string()),
                "`@timestamp` is str, but Stamped has it as datetime.".to_string()
            )]
        );
        // A pipeline that produces no schema is held to none.
        let config = compiling().replace(r#""body":"x = 1;""#, body);
        assert_eq!(warnings_of(config, &[]), []);
    }

    /// A grok step's fields come typed as the pipeline types them: a chosen type, else a produced schema's, else the
    /// pattern's.
    #[test]
    fn a_grok_steps_fields_come_as_the_pipeline_types_them() {
        let step = r#"{"id":"s","type":"parser","input":"_raw","kind":{"type":"grok",
            "patterns":["%{WORD:time} %{INT:n2} %{HTTPDATE:at} %{INT:took}"]},"declared":[{"name":"took","type":"f64"}]}"#;
        let config = producing(r#"["login"]"#).replace(r#"{"id":"s","type":"code","body":"x = 1;"}"#, step);
        let fields = &previewed(config, &[]).parsed_fields["s"];
        let at = serde_json::json!({"type": "datetime", "format": "%d/%b/%Y:%H:%M:%S %z"});
        assert_eq!(
            serde_json::to_value(fields).unwrap(),
            serde_json::json!([
                {"name": "time", "type": "i64"},
                {"name": "n2", "type": "i64"},
                {"name": "at", "type": at},
                {"name": "took", "type": "f64"}
            ])
        );
    }

    /// A field declared otherwise than a produced schema types it is warned of with the schema's type, which the
    /// declaration can take.
    #[test]
    fn a_warning_a_declared_type_answers_comes_with_that_type() {
        let step = r#"{"id":"s","type":"parser","input":"_raw","kind":{"type":"csv","delimiter":","},
            "declared":[{"name":"time","type":"str"},{"name":"host","type":"str"}]}"#;
        let config = producing(r#"["login"]"#).replace(r#"{"id":"s","type":"code","body":"x = 1;"}"#, step);
        let preview = previewed(config, &[]);
        let fixes: Vec<[String; 3]> = (preview.fixes.iter())
            .map(|f| [&f.message, &f.path, &serde_json::to_string(&f.ty).unwrap()].map(String::clone))
            .collect();
        assert_eq!(
            fixes,
            [["`time` is str, but Login has it as i64.", "time", r#""i64""#].map(String::from)]
        );
        assert!(
            preview.warnings.iter().any(|w| w.message == fixes[0][0]),
            "{:?}",
            preview.warnings
        );
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
                "`@timestamp` is str, but Stamped has it as datetime.".to_string()
            )]
        );
    }

    #[test]
    fn a_field_the_schema_requires_and_no_step_writes_is_warned_at_no_step() {
        let warned = warnings_of(producing(r#"["login"]"#), &[]);
        assert_eq!(
            warned,
            [(
                None,
                "Login requires `time` and `user`; the rows lack them.".to_string()
            )]
        );
    }

    /// What a sample keeps of the output: `fields` on `rows`.
    fn shown(fields: &[&str], rows: &[u32]) -> Option<ShownCut> {
        Some(ShownCut {
            fields: Some(fields.iter().map(|f| f.to_string()).collect()),
            rows: rows.to_vec(),
        })
    }

    /// The rows and fields on screen save as a sample, each typed as the pipeline writes it: a `__` field shown is
    /// kept, and a field not shown is not. Another pipeline's preview reads it under those types.
    #[test]
    fn the_rows_a_preview_shows_save_as_a_sample_typed_as_the_pipeline_writes_them() {
        let records = ["http://a/x", "http://b/y"]
            .map(|raw| serde_json::json!({ "_raw": raw }).as_object().unwrap().clone())
            .to_vec();
        let previewed = run_preview(
            PreviewRequest {
                config: compiling(),
                pipeline: Some("p".into()),
                sample: None,
                show_internal: true,
                filter: None,
                attribution: false,
                as_sample: shown(&["x", "u.path", "__source.bytes"], &[0]),
                dictionary: false,
                after_dot: None,
                capture_filter: None,
                test: None,
                locked: None,
            },
            Default::default(),
            &crate::heartbeat::CaptureBatch::pasted(records),
        );
        assert!(
            previewed.succeeded(),
            "{:?}",
            previewed.errors.iter().map(|e| &e.message).collect::<Vec<_>>()
        );
        let sample = previewed.body.as_sample.unwrap();
        assert_eq!(sample.rows.len(), 1, "only the row shown");
        assert_eq!(cell(&sample, 0, "x"), serde_json::json!(1));
        assert_eq!(cell(&sample, 0, "u.path"), serde_json::json!("/x"));
        let x = sample.columns.iter().find(|c| c.name == "x").unwrap();
        assert_eq!(x.ty, "i64", "typed as the pipeline writes it");
        let named: Vec<_> = (sample.columns.iter())
            .map(|c| c.name.as_str())
            .filter(|name| !name.starts_with("__capture."))
            .collect();
        assert_eq!(named, ["u.path", "x", "__source.bytes"], "the fields shown");

        let meta = SourceMeta::of_columns(&sample.columns).unwrap();
        read_sample(&meta, &sample).expect("the sample reads back by its own columns");
    }

    /// A JSON or key-value parse converts a key a produced schema types as it lays it: a value that does not fit is
    /// null, and an exception on its row. A key no schema types keeps what it arrived as.
    #[test]
    fn a_dynamic_parse_converts_the_keys_a_produced_schema_types() {
        for (body, raws) in [
            (
                "Object.assign($, json(_raw));",
                [r#"{"time":"42","user":{"name":7},"other":"5"}"#, r#"{"time":"soon"}"#],
            ),
            (
                "Object.assign($, kv(_raw));",
                ["time=42 user.name=7 other=5", "time=soon"],
            ),
        ] {
            let step = format!(r#"{{"id":"s","type":"code","body":{}}}"#, serde_json::json!(body));
            let config = producing(r#"["login"]"#).replace(r#"{"id":"s","type":"code","body":"x = 1;"}"#, &step);
            let records = raws
                .map(|raw| serde_json::json!({ "_raw": raw }).as_object().unwrap().clone())
                .to_vec();
            let previewed = run_preview(
                PreviewRequest {
                    config,
                    pipeline: Some("p".into()),
                    sample: None,
                    show_internal: false,
                    filter: None,
                    attribution: false,
                    as_sample: shown(&["time", "user.name", "other"], &[0, 1]),
                    dictionary: false,
                    after_dot: None,
                    capture_filter: None,
                    test: None,
                    locked: None,
                },
                Default::default(),
                &crate::heartbeat::CaptureBatch::pasted(records),
            );
            let errors: Vec<_> = previewed.errors.iter().map(|e| &e.message).collect();
            assert!(previewed.succeeded(), "{body}: {errors:?}");
            let rows = serde_json::to_value(&previewed.body.rows).unwrap();
            let refused = rows[1]["exceptions"].as_array().unwrap().iter().any(|e| {
                e["message"]
                    .as_str()
                    .unwrap()
                    .contains(r#"time is "soon", which could not parse as i64"#)
            });
            assert!(refused, "{body}: {}", rows[1]["exceptions"]);
            let sample = previewed.body.as_sample.unwrap();
            let first = |path: &str| cell(&sample, 0, path);
            assert_eq!(first("time"), serde_json::json!(42), "{body}");
            assert_eq!(first("user.name"), serde_json::json!("7"), "{body}");
            assert_eq!(first("other"), serde_json::json!("5"), "{body}: no schema types it");
            assert!(cell(&sample, 1, "time").is_null(), "{body}");
        }
    }

    /// A sample of a captured sample's output says how the output was made, in place of how its input was captured:
    /// it was taken from no source.
    #[test]
    fn a_sample_of_the_output_says_how_it_was_made_not_how_its_input_was_captured() {
        let column = |name: &str, ty: &str| crate::heartbeat::CaptureColumn {
            name: name.into(),
            ty: ty.into(),
        };
        let captured = crate::heartbeat::CaptureBatch {
            columns: vec![
                column("_raw", "str"),
                column("__capture.timestamp", "datetime"),
                column("__capture.source", "str"),
            ],
            rows: vec![crate::heartbeat::CapturedRow {
                values: vec![
                    "http://a/x".into(),
                    "2026-10-25T13:57:12.000000Z".into(),
                    "orders-west".into(),
                ],
                ..Default::default()
            }],
        };
        let previewed = run_preview(
            PreviewRequest {
                config: compiling(),
                pipeline: Some("p".into()),
                sample: Some("orders".into()),
                show_internal: false,
                filter: None,
                attribution: false,
                as_sample: shown(&["x"], &[0]),
                dictionary: false,
                after_dot: None,
                capture_filter: None,
                test: None,
                locked: None,
            },
            Default::default(),
            &captured,
        );
        let sample = previewed.body.as_sample.unwrap();
        assert_eq!(
            serde_json::Value::Object(cells(&sample, 0).under("__capture")),
            serde_json::json!({"timestamp": "2026-10-25T13:57:12.000000Z", "pipeline": "p", "sample": "orders"}),
            "the output was made at the instant its input was taken",
        );
        let named: Vec<_> = sample.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(!named.contains(&"__capture.source"), "{named:?}");
        assert!(named.contains(&"__capture.pipeline"), "{named:?}");

        let meta = SourceMeta::of_columns(&sample.columns).unwrap();
        read_sample(&meta, &sample).expect("the sample reads back by its own columns");
    }

    /// A pipeline over a saved sample of another pipeline's output writes under an object that output holds: a
    /// lookup adds `destination.geo.country_iso_code`, a write changes one field, a delete removes one, and the
    /// object's other fields stay.
    #[test]
    fn a_pipeline_on_a_saved_output_sample_writes_under_its_objects() {
        let config = r#"{"version":1,
            "lookupTables":[{"id":"countries","type":"inline","headers":["ip","country"],"rows":[["8.8.8.8","US"]]}],
            "pipelines":[
                {"id":"ecs","name":"ECS","steps":[{"id":"e","type":"code","body":
                    "destination = {ip: _raw, port: 443, geo: {city_name: \"Ashburn\"}}; event = {kind: \"event\"};"}]},
                {"id":"geo","name":"Geo","steps":[
                    {"id":"l","type":"lookup","input":"destination.ip","table":"countries","matchKind":"exact",
                     "keyColumn":"ip","outputs":[{"source":"country","field":"destination.geo.country_iso_code"}]},
                    {"id":"w","type":"code","body":"destination.port = 8443; event.kind = null; event.category = \"network\";"}]}]}"#;
        let request = |pipeline: &str, as_sample: Option<ShownCut>| PreviewRequest {
            config: config.into(),
            pipeline: Some(pipeline.into()),
            sample: None,
            show_internal: false,
            filter: None,
            attribution: false,
            as_sample,
            dictionary: false,
            after_dot: None,
            capture_filter: None,
            test: None,
            locked: None,
        };
        let messages = |p: &Envelope<Preview>| p.errors.iter().map(|e| e.message.clone()).collect::<Vec<_>>();
        let raw = serde_json::json!({ "_raw": "8.8.8.8" }).as_object().unwrap().clone();
        let saved = run_preview(
            request(
                "ecs",
                shown(
                    &[
                        "destination.ip",
                        "destination.port",
                        "destination.geo.city_name",
                        "event.kind",
                    ],
                    &[0],
                ),
            ),
            Default::default(),
            &crate::heartbeat::CaptureBatch::pasted(vec![raw]),
        );
        assert!(saved.succeeded(), "{:?}", messages(&saved));
        let sample = saved.body.as_sample.unwrap();
        let meta = SourceMeta::of_columns(&sample.columns).unwrap();
        let declared: Vec<_> = meta.fields().map(|(name, _)| name).collect();
        for leaf in [
            "destination.ip",
            "destination.port",
            "destination.geo.city_name",
            "event.kind",
        ] {
            assert!(declared.contains(&leaf), "{leaf}: {declared:?}");
        }
        assert!(
            !declared.contains(&"destination"),
            "an object is its leaves: {declared:?}"
        );

        let previewed = run_preview(request("geo", None), Default::default(), &sample);
        assert!(previewed.succeeded(), "{:?}", messages(&previewed));
        assert!(previewed.body.warnings.is_empty(), "{:?}", messages(&previewed));
        let row = previewed.body.rows[0].json.as_ref().unwrap();
        assert_eq!(
            row["destination"],
            serde_json::json!({
                "ip": "8.8.8.8",
                "port": 8443,
                "geo": {"city_name": "Ashburn", "country_iso_code": "US"},
            })
        );
        assert_eq!(row["event"], serde_json::json!({"category": "network"}));
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
                filter: None,
                attribution: false,
                as_sample: None,
                dictionary: false,
                after_dot: Some(AfterDotRequest {
                    dot: text.len() - 1,
                    text: text.into(),
                    at,
                }),
                capture_filter: None,
                test: None,
                locked: None,
            },
            Default::default(),
            &Default::default(),
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
                    filter: None,
                    attribution: false,
                    as_sample: None,
                    dictionary: false,
                    after_dot: None,
                    capture_filter: Some(CaptureFilterRequest {
                        text: text.into(),
                        destination: "both".into(),
                    }),
                    test: None,
                    locked: None,
                },
                Default::default(),
                &Default::default(),
            );
            assert!(previewed.succeeded());
            previewed.body.capture_filter.unwrap()
        };
        let both = check("n > 1");
        assert_eq!((both.reads, both.skips.len()), (2, 0));
        let one = check("x > 1");
        assert_eq!(one.reads, 1);
        assert_eq!(
            one.skips.iter().map(|s| s.route.as_str()).collect::<Vec<_>>(),
            ["in \u{2192} Q"]
        );
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
                filter: None,
                attribution: false,
                as_sample: None,
                dictionary: false,
                after_dot: Some(AfterDotRequest {
                    text: "_raw.".into(),
                    dot: 4,
                    at: DotAt::Source("nope".into()),
                }),
                capture_filter: None,
                test: None,
                locked: None,
            },
            Default::default(),
            &Default::default(),
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
                filter: None,
                attribution: false,
                as_sample: None,
                dictionary: false,
                after_dot: None,
                capture_filter: None,
                test: None,
                locked: None,
            },
            Default::default(),
            &crate::heartbeat::CaptureBatch::pasted(vec![record]),
        );
        assert!(previewed.succeeded(), "{:?}", previewed.errors);
        assert_eq!(previewed.body.rows.len(), 40);
        for row in &previewed.body.rows {
            let json = row.json.as_ref().unwrap();
            assert_eq!(json.pointer("/__source/bytes"), None, "{json}");
        }
    }
}
