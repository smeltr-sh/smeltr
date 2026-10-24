//! Previews: one pipeline over a sample (`run_preview`), a sample routed through a whole bundle
//! (`run_route_preview`), and a locked test run as the preview it locked and compared (`run_test`). Each takes
//! everything it reads as arguments -- the config, the sample and where its lookup tables resolve -- so the leader's
//! routes, its test runner and the demo's preview service give one answer.

use std::collections::HashMap;

use crate::config_file::{self, Verdict};
use crate::core::batch::Batch;
use crate::core::buffer::{self, BufferPool};
use crate::core::ctx::Ctx;
use crate::core::error::{ErrorSink, ExceptionKind};
use crate::leader::envelope::{ApiError, Empty, Envelope};
use crate::plan;
use crate::plan::output::{RowFilter, run_to_json_marked};
use crate::plan::topology::SourceMeta;

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
    /// Which stage stopped the preview, and why. A compile failure still names every pipeline, so the editor's
    /// list stands.
    pub fn failed(stage: &'static str, errors: Vec<ApiError>, pipeline_names: Vec<String>) -> Envelope<Preview> {
        Envelope {
            ok: false,
            errors,
            body: Preview {
                stage: Some(stage),
                pipeline_names,
                ..Default::default()
            },
        }
    }
}

#[derive(serde::Serialize)]
pub struct RowOut {
    pub row: u32,
    /// `None` when the row did not survive the pipeline.
    pub json: Option<serde_json::Value>,
    pub exceptions: Vec<ExceptionOut>,
}

#[derive(serde::Serialize)]
pub struct ExceptionOut {
    /// The answer the exception's kind takes: `ignore`, `tag`, `drop`, `deliver` or `block`.
    pub answer: &'static str,
    pub message: String,
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

/// What a record a block held says about why, beside its fields (`worker::capture::EXCEPTION_MESSAGE`).
const EXCEPTION: &str = "__exception";

/// The columns of a sample that are its records' own: not why a record blocked, which is about the record and no
/// pipeline reads.
pub fn replayed(columns: &[crate::heartbeat::CaptureColumn]) -> impl Iterator<Item = &crate::heartbeat::CaptureColumn> {
    columns.iter().filter(|c| c.name.split('.').next() != Some(EXCEPTION))
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
/// Where `meta` is a source's, that source sets the `__source` fields, from each record's `__source`; where it is the
/// sample's own (`SourceMeta::of_columns`), they are columns of the record like any other.
pub fn read_sample(
    meta: &SourceMeta,
    columns: &[crate::heartbeat::CaptureColumn],
    records: &[crate::io::RecordObject],
) -> Result<SampleRead, String> {
    let from_source = !meta.source_fields().is_empty();
    let captured: Vec<_> = replayed(columns)
        .filter(|c| !(from_source && c.name.starts_with("__source.")))
        .collect();
    let declared = meta.record_fields();
    let head = |name: &str| {
        crate::plan::tree::segments(name)
            .next()
            .map(std::borrow::Cow::into_owned)
    };
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
        for (name, _) in declared.iter().filter(|(n, _)| *n != crate::plan::tree::UNCLAIMED) {
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
                object.remove("__source");
            }
            object.remove(EXCEPTION);
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
        return Envelope::failed_one(
            "this sample does not say which source it stands for, and routing selects on a source -- \
             name one on the sample to route it",
        );
    };

    let system = match config_file::load_str_at(config, &declared_at) {
        Ok(system) => system,
        Err(e) => return Envelope::failed_one(e.to_string()),
    };
    let Some(source) = system.topology.sources.iter().find(|s| s.name == source_name) else {
        return Envelope::failed_one(format!(
            "this sample stands for source {source_name:?}, which this config does not declare"
        ));
    };
    let source_id = source.id;

    let compiled = match system.compile_topology() {
        Ok(compiled) => compiled,
        Err(errs) => {
            return Envelope::failed(
                errs.iter()
                    .map(|e| ApiError::from_compile(e, system.step_id_for(e.pipeline, e.function)))
                    .collect(),
            );
        }
    };
    let route_ids: Vec<String> = compiled.routes.iter().map(|r| r.id.clone()).collect();

    // A sample's byte records hold their text already decoded, so nothing decodes. A record with no coordinates gets
    // the empty ones a pasted record has; the source's own `SourceMeta` decides whether a route may read them at all.
    let SampleRead {
        records,
        texts,
        warnings,
    } = match read_sample(&compiled.meta_for(source_id), &sample.columns, &sample.records) {
        Ok(read) => read,
        Err(e) => return Envelope::failed_one(e),
    };

    let previewed = crate::runtime::preview_routes_at(
        &compiled,
        &system.topology.destinations,
        source_id,
        &source_name,
        &records,
        &texts,
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
        Err(e) => Envelope::failed_one(e.to_string()),
    }
}

/// Not `ok` when a save would refuse the pattern, with why, word for word as the save says it.
pub fn line_breaker_verdict(req: LineBreakerRequest) -> Envelope {
    let refusal = config_file::line_breaker_refusal("this line breaker", &req.pattern);
    Envelope::with(Empty, refusal.into_iter().map(ApiError::new).collect())
}

pub fn format_code(req: FormatRequest) -> Envelope<Formatted> {
    Envelope::with(
        Formatted {
            code: crate::expr::pretty::format(&req.code, req.kind),
        },
        Vec::new(),
    )
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

    let pipeline_cfg = match &req.pipeline {
        Some(id) => system
            .pipeline_ids
            .iter()
            .position(|p| p == id)
            .and_then(|i| system.pipelines.iter().find(|p| p.id == i)),
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
    let meta = match SourceMeta::of_columns(&replayed(columns).cloned().collect::<Vec<_>>()) {
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
    let plan = match plan::plan::compile(pipeline_cfg, &meta) {
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

    let warnings: Vec<ApiError> = plan
        .warnings
        .iter()
        .map(|e| ApiError::from_compile(e, system.step_id_for(Some(pipeline_cfg.id), e.function)))
        .collect();

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
        return Envelope::ok(Preview {
            stage: None,
            pipeline_names,
            rows: vec![],
            unfiltered: filter.as_ref().map(|_| 0),
            parsers: vec![],
            dictionary: Some(DictionaryOut {
                compiled: Dictionary::of(&record),
                sampled: None,
            }),
            warnings,
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
        batch.reclaim(&mut pool);
        rows
    });
    let preview_rows = match previewed {
        Ok(rows) => rows,
        Err(over) => {
            return Preview::failed(
                "memory",
                vec![ApiError::new(format!(
                    "this sample asked for more memory than a preview may hold: {over}"
                ))],
                pipeline_names,
            );
        }
    };

    let rows: Vec<(RowOut, bool)> = preview_rows
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
                    .map(|(answer, message)| ExceptionOut { answer, message })
                    .collect(),
            };
            (row, r.matched)
        })
        .collect();

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
    let unfiltered = filter.as_ref().map(|_| rows.len() as u32);
    let rows = rows
        .into_iter()
        .filter(|(_, matched)| *matched)
        .map(|(r, _)| r)
        .collect();
    Envelope::ok(Preview {
        stage: None,
        pipeline_names,
        dictionary: Some(DictionaryOut {
            compiled: Dictionary::of(&record),
            sampled: Some(Dictionary::of(&sampled)),
        }),
        rows,
        unfiltered,
        parsers,
        warnings,
    })
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
    /// Which scope this ran in -- ids are unique only within a scope, the same collision
    /// `ScopedTestSummary`'s doc names.
    pub scope: String,
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
        scope: scope.to_string(),
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
        },
        declared_at,
        columns,
        records,
    );
    if !previewed.ok {
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
    if !resp.ok {
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
