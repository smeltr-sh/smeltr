//! DTO -> `plan::config`/`plan::topology`: id resolution and structural validation (every reference resolves, every id
//! is unique in its scope, every field that must not be blank is not, every bound holds). Expression text passes
//! through verbatim; `LoadedSystem::compile_topology` checks it with the compiler's own errors.
//!
//! Every top-level item that fails to convert contributes one error and is otherwise skipped, so one bad route does not
//! hide an unrelated bad pipeline.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use std::path::PathBuf;

use crate::core::column::DataType;
use crate::core::error::{Category, ExceptionKind};
use crate::core::id::{ById, DestId, FunctionId, SourceId};
use crate::ops::timeparse;
use crate::plan::config::{
    Answer, CsvConfig, DeclaredField, FunctionConfig, FunctionKind, GrokConfig, KvConfig, LookupAt, LookupFile,
    LookupOutput, LookupSource, MatchKind, Multiplicity, OnException, ParserKind, PipelineConfig, Step, TimeSource,
};
use crate::plan::parse_call::split_dots_default;
use crate::plan::topology::{
    DeliverRule, DestinationConfig, KafkaSourceConfig, NamedCertificate, NamedSecret, RouteConfig, SourceConfig,
    SourceKind, TopologyConfig,
};
use crate::schema::Schema;

use super::CURRENT_VERSION;
use super::dto::*;

mod clouds;
pub(super) mod destination;
mod shared;
pub(super) mod source;

pub use shared::*;

/// Structural resolution only: every reference resolves, every id is unique, every bound holds. Expression text
/// passes through verbatim and is never compiled here, so a pipeline whose expressions don't compile still loads —
/// on purpose, so `/api/areas/:area/preview/pipeline` can list which pipeline that is rather than refuse the whole
/// file. Reach for [`super::validate`] instead when the question is "does this bundle actually run": that is what
/// compiles every pipeline and is what every save path goes through.
pub fn load_str(json: &str) -> Result<super::LoadedSystem, LoadError> {
    load_str_at(json, &DeclaredAt::new())
}

/// `load_str` for a configuration that came from a bundle directory: `declared_at` says where each lookup table was
/// declared, which is what a relative lookup path resolves against.
pub fn load_str_at(json: &str, declared_at: &DeclaredAt) -> Result<super::LoadedSystem, LoadError> {
    let file: FileConfig = serde_json::from_str(json)?;
    from_file_config_at(file, declared_at)
}

/// Every problem `validate` would report for `file`, structural first and compile errors only once the structure
/// resolves. Empty means `file` is runnable.
pub fn validation_errors(file: FileConfig) -> Vec<ValidationError> {
    validation_errors_at(file, &DeclaredAt::new())
}

/// `validation_errors` for a bundle read from a directory, so a relative lookup path resolves.
pub fn validation_errors_at(file: FileConfig, declared_at: &DeclaredAt) -> Vec<ValidationError> {
    match super::validate_at(file, declared_at) {
        Ok(_) => Vec::new(),
        Err(LoadError::Validation(errs)) => errs,
        Err(e) => vec![e.to_string().into()],
    }
}

#[derive(Debug)]
pub enum LoadError {
    /// Malformed JSON, or JSON that does not fit the DTO shape.
    Json(serde_json::Error),
    UnsupportedVersion(u32),
    /// Every problem found, together.
    Validation(Vec<ValidationError>),
}

impl From<serde_json::Error> for LoadError {
    fn from(e: serde_json::Error) -> Self {
        LoadError::Json(e)
    }
}

impl std::fmt::Display for LoadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LoadError::Json(e) => write!(f, "invalid config JSON: {e}"),
            LoadError::UnsupportedVersion(v) => write!(
                f,
                "unsupported config version {v} (this build supports version {CURRENT_VERSION})"
            ),
            LoadError::Validation(errs) => {
                write!(
                    f,
                    "{}",
                    errs.iter().map(ToString::to_string).collect::<Vec<_>>().join("; ")
                )
            }
        }
    }
}

impl std::error::Error for LoadError {}

struct IdMaps {
    sources: HashMap<String, SourceId>,
    destinations: HashMap<String, DestId>,
    pipelines: HashMap<String, usize>,
    /// Raw DTOs, each with where the bundle that declared it was read from: resolving one can fail, and that error
    /// belongs to the step that referenced it.
    lookup_tables: HashMap<String, (LookupSourceDto, Option<BundleAt>)>,
    /// Raw DTOs, so `expand_calls` can inline a `Call` target.
    pipeline_dtos: HashMap<String, PipelineDto>,
    grok_patterns: crate::plan::config::GrokPatterns,
}

/// Where a bundle was read from, and so what a lookup path in it resolves against.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum BundleAt {
    /// A directory on disk. What a checkout gives.
    Dir(PathBuf),
    /// A directory inside a commit: the repository holding it, the commit, and the path within it, without a
    /// trailing slash. What a reader with no checkout gives.
    Commit {
        repo: PathBuf,
        commit: String,
        prefix: String,
    },
}

/// What a log line says of where a bundle was read from: its directory, or its path in a commit.
impl std::fmt::Display for BundleAt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BundleAt::Dir(dir) => write!(f, "{}", dir.display()),
            BundleAt::Commit { commit, prefix, .. } => write!(f, "{prefix} at commit {commit}"),
        }
    }
}

impl BundleAt {
    /// Where `path`, named relative to this bundle, actually is.
    ///
    /// A commit needs no confinement check beyond the relative one: a tree path is looked up, never walked, so
    /// `../` names an entry no tree holds rather than reaching a parent.
    pub fn lookup(&self, path: &str) -> Result<LookupAt, crate::lookup::LookupError> {
        match self {
            BundleAt::Dir(dir) => crate::lookup::resolve_lookup_path(path, dir).map(LookupAt::Path),
            BundleAt::Commit { repo, commit, prefix } => {
                crate::lookup::check_relative(path)?;
                Ok(LookupAt::InCommit {
                    repo: repo.clone(),
                    commit: commit.clone(),
                    path: if prefix.is_empty() {
                        path.to_string()
                    } else {
                        format!("{prefix}/{path}")
                    },
                })
            }
        }
    }
}

/// Every lookup table id mapped to where the `lookup-tables.json` declaring it was read from. A bundle builds one; a
/// configuration parsed from a string has none, and so can name no lookup file.
pub type DeclaredAt = HashMap<String, BundleAt>;

pub(crate) fn from_file_config_at(
    file: FileConfig,
    declared_at: &DeclaredAt,
) -> Result<super::LoadedSystem, LoadError> {
    if file.version != CURRENT_VERSION {
        return Err(LoadError::UnsupportedVersion(file.version));
    }

    let mut id_errors = Vec::new();
    // A check on one item or one assignment says which, so the editor puts the message on its control. A check across
    // items has no one place to put it, and stays a plain string.
    let mut attributed_errors: Vec<ValidationError> = Vec::new();
    let maps = IdMaps {
        sources: index_by_id(&file.sources, |s| &s.id, "source", &mut id_errors),
        destinations: index_by_id(&file.destinations, |d| &d.id, "destination", &mut id_errors),
        pipelines: index_by_id(&file.pipelines, |p| &p.id, "pipeline", &mut id_errors),
        lookup_tables: {
            let mut map = HashMap::new();
            for t in &file.lookup_tables {
                let at = declared_at.get(&t.id).cloned();
                if map.insert(t.id.clone(), (t.source.clone(), at)).is_some() {
                    id_errors.push(format!("duplicate lookup table id {:?}", t.id));
                }
            }
            map
        },
        pipeline_dtos: file.pipelines.iter().map(|p| (p.id.clone(), p.clone())).collect(),
        grok_patterns: {
            let mut map = crate::plan::config::GrokPatterns::new();
            for p in &file.grok_patterns {
                if map.insert(p.id.clone(), p.pattern.clone()).is_some() {
                    id_errors.push(format!("duplicate grok pattern id {:?}", p.id));
                }
            }
            map
        },
    };
    // A route has no numeric id, so it takes no place in `maps` -- but its id still has to be unique, because it is
    // what a diff tells an edit from a delete-and-insert by, and what a metric about the route is labeled with.
    check_unique_ids(file.routes.iter().map(|r| &r.id), "route", &mut id_errors);
    required_fields(&file, &mut attributed_errors);
    // Checked here rather than where a grok names one: a pattern no step uses yet is still the bundle's, and the
    // first step to name it should not be the one to find it broken.
    for (id, e) in crate::format::grok_scan::Matcher::check_shared(&maps.grok_patterns) {
        ItemErrors::new(&mut attributed_errors, "grokPatterns", &id)
            .at("pattern")
            .push(e.to_string());
    }
    let (produces, accepts) = resolve_schemas(&file, &mut attributed_errors);

    // References resolved against a map built from duplicate ids would only add secondary errors on top of the real
    // one.
    if !id_errors.is_empty() || !attributed_errors.is_empty() {
        return Err(LoadError::Validation(
            id_errors.into_iter().map(Into::into).chain(attributed_errors).collect(),
        ));
    }

    let sources = file.sources.iter().map(|s| to_source(s, maps.sources[&s.id])).collect();
    let mut errors = Vec::new();
    let mut group = OnException::default();
    if let Some(layer) = &file.on_exception {
        lay_answers(&mut group, layer, Layer::Group, &maps)
            .unwrap_or_else(|e| errors.push(format!("the worker group's {e}")));
    }
    let mut destinations: Vec<DestinationConfig> = file
        .destinations
        .iter()
        .map(|d| to_destination(d, maps.destinations[&d.id]))
        .collect();
    for (d, dto) in destinations.iter_mut().zip(&file.destinations) {
        let mut own = group.clone();
        if let Some(layer) = &dto.on_exception {
            lay_answers(&mut own, layer, Layer::Destination, &maps)
                .unwrap_or_else(|e| errors.push(format!("destination {:?}'s {e}", dto.id)));
        }
        d.on_reject = own.answer(&ExceptionKind::Refused).clone();
    }

    let names = file.route_names();
    let (routes, route_errors) = collect(file.routes.iter().map(|r| to_route(r, &names[&r.id], &maps, &group)));
    errors.extend(route_errors);
    let (pipelines, pipeline_errors) = collect(file.pipelines.iter().zip(&produces).map(|(p, produces)| {
        let mut call_ids = Vec::new();
        let expanded = expand_calls(
            &p.steps,
            &maps.pipeline_dtos,
            &mut vec![p.id.clone()],
            &mut call_ids,
            &mut HashSet::new(),
        )
        .map_err(|e| format!("pipeline {:?}: {e}", p.name))?;
        to_pipeline(p, &expanded, &call_ids, maps.pipelines[&p.id], produces, &maps)
    }));
    errors.extend(pipeline_errors);

    if !errors.is_empty() {
        return Err(LoadError::Validation(errors.into_iter().map(Into::into).collect()));
    }

    let (named_secrets, named_certificates) = named_by(&file);
    let system = super::LoadedSystem {
        topology: TopologyConfig {
            named_secrets,
            named_certificates,
            fitted: crate::plan::topology::Fitted {
                sources: file
                    .sources
                    .iter()
                    .filter(|s| s.kind.leaves_fetch())
                    .map(|s| maps.sources[&s.id])
                    .collect(),
                destinations: file
                    .destinations
                    .iter()
                    .filter(|d| !d.kind.states_buffer())
                    .map(|d| maps.destinations[&d.id])
                    .collect(),
            },
            sources,
            destinations,
            routes,
            on_exception: group,
        },
        pipelines,
        pipeline_ids: file.pipelines.iter().map(|p| p.id.clone()).collect(),
        produces,
        accepts: file
            .destinations
            .iter()
            .zip(accepts)
            .map(|(d, a)| (maps.destinations[&d.id], a))
            .filter(|(_, a)| !a.is_empty())
            .collect(),
        compiled: None,
    };
    // Only once every answer resolves.
    let errors = impossible_answers_of(&system);
    if errors.is_empty() {
        Ok(system)
    } else {
        Err(LoadError::Validation(errors.into_iter().map(Into::into).collect()))
    }
}

/// Every answer `system` gives that its kind's row cannot carry out, and every refusal that leads back to where it
/// started.
fn impossible_answers_of(system: &super::LoadedSystem) -> Vec<String> {
    let topology = &system.topology;
    let group = &topology.on_exception;
    let mut errors = impossible_answers("the worker group", group, None, Layer::Group);
    for r in &topology.routes {
        let what = format!("route {}", r.name);
        errors.extend(impossible_answers(&what, &r.on_exception, Some(group), Layer::Route));
    }
    for d in &topology.destinations {
        let mut own = OnException::default();
        own.answers[ExceptionKind::Refused.index()] = d.on_reject.clone();
        let what = format!("destination {:?}", d.name);
        errors.extend(impossible_answers(&what, &own, Some(group), Layer::Destination));
        if d.on_reject.targets().contains(&d.id) {
            errors.push(format!(
                "destination {:?} delivers what it refuses to itself, where it would be refused again; give it \
                 its own answer to Refused",
                d.name
            ));
        }
    }
    errors.extend(refusal_cycles(&topology.destinations));
    errors
}

/// Each object's schemas, in the file's order.
type SchemasOf = Vec<Vec<Arc<Schema>>>;

/// The schemas each pipeline produces and each destination accepts, in the file's order, each id resolved against
/// the ones the bundle declares. A declared schema that does not read, or whose id is another's, and an id that names
/// no schema, are errors on the item that has it.
fn resolve_schemas(file: &FileConfig, attributed: &mut Vec<ValidationError>) -> (SchemasOf, SchemasOf) {
    let mut declared: HashMap<&str, Arc<Schema>> = HashMap::new();
    let mut seen = HashSet::new();
    for dto in &file.schemas {
        let errors = &mut ItemErrors::new(attributed, "schemas", &dto.id);
        if !seen.insert(dto.id.as_str()) {
            errors.push(format!("duplicate schema id {:?}", dto.id));
            continue;
        }
        match crate::schema::read(dto) {
            Ok(schema) => {
                declared.insert(&dto.id, Arc::new(schema));
            }
            Err(e) => errors.at("fields").push(format!("schema {}'s fields: {e}", dto.id)),
        }
    }
    // `names` is what the object does with a schema: "produces" or "accepts".
    let mut resolve =
        |section: &'static str, id: &str, field: &'static str, names: &str, ids: &[String]| -> Vec<Arc<Schema>> {
            let mut errors = ItemErrors::new(attributed, section, id);
            ids.iter()
                .filter_map(|s| {
                    let found = declared.get(s.as_str()).cloned();
                    if found.is_none() {
                        errors.at(field).push(format!(
                            "{} {id:?} {names} schema {s:?}, which does not exist",
                            &section[..section.len() - 1]
                        ));
                    }
                    found
                })
                .collect()
        };
    let produces = file
        .pipelines
        .iter()
        .map(|p| resolve("pipelines", &p.id, "produces", "produces", &p.produces))
        .collect();
    let accepts = file
        .destinations
        .iter()
        .map(|d| resolve("destinations", &d.id, "accepts", "accepts", &d.accepts))
        .collect();
    (produces, accepts)
}

/// Every credential and every certificate the bundle's connectors name, from the file's own walks: the ones the
/// save-time checks read, so a kind added to the file cannot name a secret the deploy does not look for.
fn named_by(file: &FileConfig) -> (Vec<NamedSecret>, Vec<NamedCertificate>) {
    let sources = file
        .sources
        .iter()
        .map(|s| (format!("source {:?}", s.id), s.secrets(), s.tls()));
    let destinations = file
        .destinations
        .iter()
        .map(|d| (format!("destination {:?}", d.id), d.secrets(), d.tls()));
    let mut named = Vec::new();
    let mut certificates = Vec::new();
    for (owner, secrets, tls) in sources.chain(destinations) {
        for (field, reference) in secrets {
            named.push(NamedSecret {
                reference: to_secret_ref(reference),
                what: format!("{owner}: {field}"),
            });
        }
        if let Some(name) = tls.as_ref().and_then(|t| t.certificate.as_deref()) {
            certificates.push(NamedCertificate {
                name: name.trim().to_string(),
                what: owner.clone(),
            });
        }
    }
    (named, certificates)
}

/// What a layer of `exceptions` lays its answers over.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Layer {
    /// Every kind.
    Group,
    /// Every kind but `Unprocessable`, which never reaches a route, and `Refused`, which is a destination's. A route
    /// answers `Unmatched` for the rows its delivery rules decline.
    Route,
    /// `Refused` alone.
    Destination,
}

impl Layer {
    fn answers(self, kind: &str, category: Category) -> bool {
        match self {
            Layer::Group => true,
            Layer::Route => category != Category::Delivery || kind == "Unmatched",
            Layer::Destination => kind == "Refused",
        }
    }

    /// Why this layer names none of `name`'s kinds.
    fn refuses(self, name: &str) -> String {
        match self {
            Layer::Group => unreachable!("the group answers every kind"),
            Layer::Route => format!(
                "exceptions names {name:?}, which a route does not answer: Unprocessable never reaches a route, and \
                 Refused is a destination's"
            ),
            Layer::Destination => format!("exceptions names {name:?}; a destination answers only Refused"),
        }
    }
}

/// Every answer `on_exception` gives a kind `layer` answers that needs what the kind does not have. `what` names whose
/// answers they are. An answer the same as `under`'s, the group's that `on_exception` lies over, is the group's to report.
fn impossible_answers(
    what: &str,
    on_exception: &OnException,
    under: Option<&OnException>,
    layer: Layer,
) -> Vec<String> {
    let mut errors = Vec::new();
    for (i, ((kind, category), answer)) in ExceptionKind::KINDS.iter().zip(&on_exception.answers).enumerate() {
        if !layer.answers(kind, *category) || under.is_some_and(|u| u.answers[i] == *answer) {
            continue;
        }
        let lacks = match (*kind, answer) {
            ("Unmatched", Answer::Ignore | Answer::Tag) => Some("an Unmatched row has no route"),
            ("Unprocessable", Answer::Ignore | Answer::Tag) => Some("an Unprocessable row has no record"),
            ("Refused", Answer::Ignore | Answer::Tag) => Some("a Refused row has no destination"),
            _ => None,
        };
        if let Some(lacks) = lacks {
            errors.push(format!("{what} answers {kind} with {}, but {lacks}", answer.label()));
        }
    }
    errors
}

/// Assigns `0..len` as integer ids in file order; one error per repeated id.
fn index_by_id<T, I: TryFrom<usize>>(
    items: &[T],
    id_of: impl Fn(&T) -> &String,
    kind: &str,
    errors: &mut Vec<String>,
) -> HashMap<String, I> {
    let mut map = HashMap::with_capacity(items.len());
    for (i, item) in items.iter().enumerate() {
        let id = id_of(item);
        let numeric_id = I::try_from(i).unwrap_or_else(|_| panic!("more than {kind}s than an id can hold ({i})"));
        if map.insert(id.clone(), numeric_id).is_some() {
            errors.push(format!("duplicate {kind} id {id:?}"));
        }
    }
    map
}

/// One error per repeated id, worded as [`index_by_id`] words it -- for a kind that needs the check without the
/// numbering.
fn check_unique_ids<'a>(ids: impl Iterator<Item = &'a String>, kind: &str, errors: &mut Vec<String>) {
    let mut seen = std::collections::HashSet::new();
    for id in ids {
        if !seen.insert(id) {
            errors.push(format!("duplicate {kind} id {id:?}"));
        }
    }
}

/// A single ASCII byte, written as a 1-character JSON string.
pub fn to_byte(s: &str, field: &str) -> Result<u8, String> {
    let bytes = s.as_bytes();
    if bytes.len() == 1 {
        Ok(bytes[0])
    } else {
        Err(format!("{field} must be exactly one ASCII character, got {s:?}"))
    }
}

/// A plain `DataType`: every scalar but the three no parser can ever produce (`format::typed::TypedBuilder` has no
/// case for `DateTime`, `Duration` or `Bytes` -- declaring one bare is
/// always a column of nulls), plus `Array`/`Struct` recursion. A `datetime` in either spelling is refused here: it is
/// legal only as a parser's own top-level declared field (`to_declared_fields`), because the conversion it names
/// replaces one batch slot in place -- nested one level down, there is no slot of its own to convert.
fn to_data_type(dto: &DataTypeDto) -> Result<DataType, String> {
    match dto {
        DataTypeDto::Scalar(s) => match s.as_str() {
            "bool" => Ok(DataType::Bool),
            "i64" => Ok(DataType::I64),
            "f64" => Ok(DataType::F64),
            "str" => Ok(DataType::Str),
            "variant" => Ok(DataType::Variant),
            "datetime" => Err(NESTED_DATETIME.to_string()),
            "duration" | "bytes" => Err(format!(
                "\"{s}\" is not a type a parser can produce -- declaring it is always a column of nulls"
            )),
            "skip" => Err(
                "\"skip\" marks a CSV column or a key-value key to leave out of the record, so it is only a type \
                 there -- and never on a field nested inside an array or struct"
                    .to_string(),
            ),
            other => Err(format!(
                "unknown type {other:?} (expected bool, i64, f64, str, variant, datetime, or an array/struct object)"
            )),
        },
        DataTypeDto::Compound(CompoundTypeDto::Array { of }) => Ok(DataType::Array(Box::new(to_data_type(of)?))),
        DataTypeDto::Compound(CompoundTypeDto::Struct { fields, open }) => {
            Ok(DataType::Struct(to_fields(fields)?, *open))
        }
        DataTypeDto::Compound(CompoundTypeDto::DateTime { .. }) => Err(NESTED_DATETIME.to_string()),
    }
}

/// Why a `datetime` nested inside an array or struct is refused.
const NESTED_DATETIME: &str = "a datetime can't be declared nested inside an array or struct -- only as a parser's \
                               own top-level field. Declare the text as str here and convert it afterward with \
                               parseTime";

/// Plain type pairs for a nested position (a declared `Struct`'s fields, recursed into through `to_data_type`) --
/// never a parser's own top-level `declared` list, which is `to_declared_fields` and carries a time source.
fn to_fields(fields: &[FieldDto]) -> Result<Vec<(String, DataType)>, String> {
    fields
        .iter()
        .map(|f| Ok((f.name.clone(), to_data_type(&f.ty)?)))
        .collect()
}

/// Reads a top-level type, with the time source its text is read by: auto for the bare `"datetime"`, the checked format
/// for a `datetime` object, and none for any other type, which `to_data_type` reads and refuses as usual.
fn to_timed_type(ty: &DataTypeDto) -> Result<(DataType, Option<TimeSource>), String> {
    let time = match ty {
        DataTypeDto::Scalar(s) if s == "datetime" => TimeSource::Auto,
        DataTypeDto::Compound(CompoundTypeDto::DateTime { format }) => {
            timeparse::check_format(format).map_err(|e| format!("datetime format {format:?}: {e}"))?;
            TimeSource::Format(format.clone())
        }
        other => return Ok((to_data_type(other)?, None)),
    };
    Ok((DataType::DateTime, Some(time)))
}

/// A parser step's own `declared` list: like `to_fields`, but a top-level `datetime` is legal here (`to_timed_type`):
/// the parser pins the field to `Str` and the convert pass after the parse runs `parseTime` over it.
fn to_declared_fields(fields: &[FieldDto]) -> Result<Vec<DeclaredField>, String> {
    fields
        .iter()
        .map(|f| {
            let build = || -> Result<DeclaredField, String> {
                let (ty, time_source) = to_timed_type(&f.ty)?;
                Ok(DeclaredField {
                    name: f.name.clone(),
                    ty,
                    time_source,
                })
            };
            build().map_err(|e| format!("field {:?}: {e}", f.name))
        })
        .collect()
}

/// A lookup output's type is a scalar or a top-level `datetime`, spelled as a declared field's is; absent is `str`.
/// Array and struct are refused: a table cell is one value. The default, when there is one, must fit the type, so a
/// miss cannot write what the field cannot hold.
fn to_lookup_output(o: &LookupOutputDto) -> Result<LookupOutput, String> {
    let build = || -> Result<LookupOutput, String> {
        let (ty, time_source) = match &o.ty {
            None => (DataType::Str, None),
            Some(DataTypeDto::Scalar(s)) if s == "skip" => {
                return Err("\"skip\" is not a type: leave the column out of the outputs instead".to_string());
            }
            Some(DataTypeDto::Compound(c)) if !matches!(c, CompoundTypeDto::DateTime { .. }) => {
                return Err("a lookup output is one value: bool, i64, f64, str or datetime".to_string());
            }
            Some(other) => to_timed_type(other)?,
        };
        let default = o.default.clone().filter(|d| !d.is_empty());
        if let Some(d) = &default
            && !crate::lookup::fits(d, &ty, time_source.as_ref())
        {
            return Err(format!("its default {d:?} does not fit {ty}"));
        }
        Ok(LookupOutput {
            source: o.source.clone(),
            field: o.field.clone(),
            default,
            ty,
            time_source,
        })
    };
    build().map_err(|e| format!("output {:?}: {e}", o.field))
}

/// A CSV step's `declared` list is its column table: entry `i` describes column `i`, left to right. An entry typed
/// `"skip"` holds its position and produces no field, which is how a column the author does not want keeps every column
/// after it aligned.
///
/// Returns each column's name, `None` where it is skipped, and the fields the kept ones declare -- `CsvConfig::headers`
/// and the `Parser`'s own `declared`, which is why one authored list becomes two.
fn to_csv_columns(columns: &[FieldDto]) -> Result<(Vec<Option<String>>, Vec<DeclaredField>), String> {
    let mut headers = Vec::with_capacity(columns.len());
    let mut kept = Vec::new();
    for c in columns {
        if is_skip(&c.ty) {
            headers.push(None);
            continue;
        }
        headers.push(Some(c.name.clone()));
        kept.extend(to_declared_fields(std::slice::from_ref(c))?);
    }
    Ok((headers, kept))
}

/// Whether a parser's declared field is typed `"skip"`: a CSV column or a key-value key that makes no field.
fn is_skip(ty: &DataTypeDto) -> bool {
    matches!(ty, DataTypeDto::Scalar(s) if s == "skip")
}

/// Gives every Kafka source in `sources` that names no consumer group its worker group's own, `smeltr.<worker
/// group>`. Sources of one worker group then share a group, and its rebalances involve only that group's workers.
/// Called wherever a worker group's bundle is merged with `shared/`, so a shared source reads the whole topic in
/// every worker group that runs it.
pub fn fill_default_group_ids(sources: &mut [SourceDto], worker_group: &str) {
    for s in sources {
        if let SourceKindDto::Kafka(KafkaSourceDto { group_id, .. }) = &mut s.kind
            && group_id.trim().is_empty()
        {
            *group_id = format!("{DEFAULT_GROUP_ID}.{worker_group}");
        }
    }
}

/// The database `source` connects to: what Run reaches before the source declares the fields a load needs. `None`
/// for a source that is not an SQL one.
pub fn sql_connection(source: &SourceDto) -> Option<crate::plan::topology::SqlDatabase> {
    let (_, sql) = source.kind.sql()?;
    Some(sql_database(sql, &format!("source {:?}", source.id)))
}

/// `name` is the DTO's `id`: v1 has no separate display label.
fn to_source(dto: &SourceDto, id: SourceId) -> SourceConfig {
    let of = format!("source {:?}", dto.id);
    let mut kind = dto.kind.spec().to_kind(&of);
    if let Some(avro) = to_avro_read(&dto.kind) {
        match &mut kind {
            SourceKind::Kafka(k) => k.avro = Some(avro),
            SourceKind::Objects(o) => o.avro = Some(avro),
            _ => unreachable!("only a source that reads payloads reads Avro"),
        }
    }
    SourceConfig {
        id,
        name: dto.id.clone(),
        kind,
    }
}

/// How `kind` reads its payloads as Avro, where its `format` is `"avro"`.
fn to_avro_read(kind: &SourceKindDto) -> Option<crate::plan::topology::AvroRead> {
    kind.read()
        .filter(|r| r.format == Some(crate::config_file::dto::SourceFormatDto::Avro))?;
    Some(crate::plan::topology::AvroRead {
        registry: kind.schema_registry().map(to_registry),
        meta: kind.meta(),
        writers: Default::default(),
    })
}

/// Sources in two worker groups that read one topic in one consumer group on the same brokers share its committed
/// offsets, and each commit carries the start it descends from (`io::kafka`). Given different starts, each deploy of
/// one moves the partitions to its own start, away from the other's, so the pair is refused on the later group's
/// source. `groups` are the merged bundles of every worker group, by name, with blank group ids filled with their own.
pub fn check_shared_group_starts(groups: &[(String, &TopologyConfig)]) -> Vec<ValidationError> {
    let readers: Vec<(&str, &str, &KafkaSourceConfig)> = groups
        .iter()
        .flat_map(|(group, topology)| {
            topology.sources.iter().filter_map(move |s| match &s.kind {
                SourceKind::Kafka(k) => Some((group.as_str(), s.name.as_str(), k)),
                _ => None,
            })
        })
        .collect();
    let mut errors = Vec::new();
    for (i, (group, id, kafka)) in readers.iter().enumerate() {
        let cluster = cluster_of(&kafka.brokers);
        let earlier = readers[..i].iter().find_map(|(other_group, other_id, other)| {
            (other_group != group
                && kafka.topics.iter().any(|t| other.topics.contains(t))
                && other.group_id == kafka.group_id
                && other.start != kafka.start
                && !cluster_of(&other.brokers).is_disjoint(&cluster))
            .then_some((*other_group, *other_id))
        });
        if let Some((other_group, other_id)) = earlier {
            ItemErrors::new(&mut errors, "sources", id).at("start").push(format!(
                "Sources {id:?} in {group} and {other_id:?} in {other_group} share consumer group {:?} but have \
                 different starts. Give them the same start or separate consumer groups.",
                kafka.group_id
            ));
        }
    }
    errors
}

/// `dto` with the built-in answer to `Refused`, which the caller lays the group's and its own over.
fn to_destination(dto: &DestinationDto, id: DestId) -> DestinationConfig {
    let of = format!("destination {:?}", dto.id);
    let kind = dto.kind.spec().to_kind(&of);
    DestinationConfig {
        id,
        name: dto.id.clone(),
        kind,
        on_reject: Answer::Block,
    }
}

/// Each chain of refusals that leads back to where it started, once, named by the destinations it passes through. A
/// destination delivering to itself is reported apart, and not here.
fn refusal_cycles(destinations: &[DestinationConfig]) -> Vec<String> {
    let name = |id: DestId| destinations.iter().find(|d| d.id == id).map_or("", |d| d.name.as_str());
    let next = |id: DestId| {
        destinations
            .iter()
            .find(|d| d.id == id)
            .map_or(&[][..], |d| d.on_reject.targets())
    };
    let mut errors = Vec::new();
    for start in destinations {
        // Depth first from `start`, reporting a path back to it only when `start` is the least id on it, so each
        // cycle is named once.
        let mut stack: Vec<Vec<DestId>> = vec![vec![start.id]];
        let mut seen: HashSet<DestId> = HashSet::new();
        while let Some(path) = stack.pop() {
            let last = *path.last().expect("a path is never empty");
            for &to in next(last) {
                if to == start.id && path.len() > 1 {
                    if path.iter().all(|id| *id >= start.id) {
                        let named: Vec<&str> = path.iter().chain([&to]).map(|&id| name(id)).collect();
                        errors.push(format!(
                            "destination {:?}'s refusals lead back to it ({}); a record refused along the way would \
                             go round forever",
                            start.name,
                            named.join(" → ")
                        ));
                    }
                } else if to != last && seen.insert(to) {
                    let mut longer = path.clone();
                    longer.push(to);
                    stack.push(longer);
                }
            }
        }
    }
    errors
}

/// Looks up the `kind` (source or pipeline) that `route` names, `id`, in `known`.
fn route_names<T: Copy>(route: &str, kind: &str, id: &str, known: &HashMap<String, T>) -> Result<T, String> {
    known
        .get(id)
        .copied()
        .ok_or_else(|| format!("route {route} references unknown {kind} {id:?}"))
}

fn to_route(dto: &RouteDto, name: &str, maps: &IdMaps, group: &OnException) -> Result<RouteConfig, String> {
    // The editor leaves a new route's ends empty until they are chosen, and its name has no such end to show, so
    // these read as the routes page says them (`unchosen`).
    if dto.source.is_empty() {
        return Err("a route names no source".into());
    }
    if dto.pipeline.is_empty() {
        return Err(format!("a route from {} names no pipeline", dto.source));
    }
    let source = route_names(name, "source", &dto.source, &maps.sources)?;
    let pipeline = route_names(name, "pipeline", &dto.pipeline, &maps.pipelines)?;
    let deliver = dto
        .deliver
        .iter()
        .map(|rule| {
            let to =
                rule.to
                    .iter()
                    .map(|d| {
                        maps.destinations.get(d).copied().ok_or_else(|| {
                            format!("the deliver rule of route {name} references unknown destination {d:?}")
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
            Ok(DeliverRule {
                when: rule.when.clone(),
                to,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;

    let mut on_exception = group.clone();
    if let Some(layer) = &dto.on_exception {
        lay_answers(&mut on_exception, layer, Layer::Route, maps).map_err(|e| format!("route {name}'s {e}"))?;
    }

    Ok(RouteConfig {
        id: dto.id.clone(),
        name: name.to_string(),
        deliver,
        source,
        cond: dto.condition.clone(),
        pipeline,
        on_exception,
    })
}

/// A parser step's kind, and its declared fields. A CSV step's columns are its `declared` list: each names a header.
fn to_parser_kind(dto: &ParserKindDto, declared: &[FieldDto]) -> Result<(ParserKind, Vec<DeclaredField>), String> {
    let kind = match dto {
        ParserKindDto::Json { split_dots } => ParserKind::Json {
            split_dots: split_dots.unwrap_or(split_dots_default("json")),
        },
        ParserKindDto::Csv {
            delimiter,
            split_dots,
            rebuild,
        } => {
            let (headers, kept) = to_csv_columns(declared)?;
            let config = CsvConfig {
                split_dots: split_dots.unwrap_or(split_dots_default("csv")),
                delimiter: to_byte(delimiter, "csv delimiter")?,
                headers,
                rebuild: *rebuild,
                ..CsvConfig::default()
            };
            return Ok((ParserKind::Csv(config), kept));
        }
        ParserKindDto::KeyValue {
            pair_sep,
            kv_sep,
            quote,
            next_key,
            split_dots,
            rebuild,
        } => {
            let (skip, kept): (Vec<&FieldDto>, Vec<&FieldDto>) = declared.iter().partition(|f| is_skip(&f.ty));
            let config = KvConfig {
                split_dots: split_dots.unwrap_or(split_dots_default("kv")),
                next_key: next_key.unwrap_or(KvConfig::default().next_key),
                pair_sep: to_byte(pair_sep, "keyValue pairSep")?,
                kv_sep: to_byte(kv_sep, "keyValue kvSep")?,
                quote: quote.as_deref().map(|q| to_byte(q, "keyValue quote")).transpose()?,
                skip: skip.into_iter().map(|f| f.name.clone()).collect(),
                rebuild: *rebuild,
                ..KvConfig::default()
            };
            let kept: Vec<FieldDto> = kept.into_iter().cloned().collect();
            return Ok((ParserKind::KeyValue(config), to_declared_fields(&kept)?));
        }
        ParserKindDto::Grok {
            patterns,
            custom_patterns,
            split_dots,
        } => ParserKind::Grok(GrokConfig {
            split_dots: split_dots.unwrap_or(split_dots_default("grok")),
            patterns: patterns.clone(),
            custom_patterns: custom_patterns
                .iter()
                .map(|p| (p.name.clone(), p.pattern.clone()))
                .collect(),
        }),
    };
    Ok((kind, to_declared_fields(declared)?))
}

/// Resolve a lookup path and confine it: a table's contents come back out through its output fields, so an unchecked
/// path turns "can edit a pipeline" into "can read any file this process can". Checked here because this is where
/// config enters; a `PipelineConfig` built in Rust is trusted.
///
/// `at` is the directory the table's own `lookup-tables.json` was read from, which is what a relative path resolves
/// against. `None` is a configuration that came from no directory.
fn resolve_lookup_file(path: &str, at: Option<&BundleAt>, what: &str) -> Result<LookupFile, String> {
    // Ahead of the bundle, because an absolute path is wrong whether or not there is one to resolve against, and
    // saying so is more use than saying there is no bundle.
    crate::lookup::check_relative(path).map_err(|e| format!("{what}: {e}"))?;
    refuse_outside_lookups(path).map_err(|e| format!("{what}: {e}"))?;
    let Some(at) = at else {
        return Err(format!(
            "{what}: lookup file `{path}` is named relative to the bundle that declared the table, and this \
             configuration came from no bundle"
        ));
    };
    Ok(LookupFile {
        text: path.to_string(),
        at: at.lookup(path).map_err(|e| format!("{what}: {e}"))?,
    })
}

/// A lookup file sits in its bundle's `lookups/` folder, where an upload puts one. One folder holds them all, so none
/// sits among the files only the leader reads, `samples/` and `tests/`.
pub fn refuse_outside_lookups(path: &str) -> Result<(), String> {
    use std::path::Component;
    let mut parts = std::path::Path::new(path).components();
    let in_lookups = matches!(parts.next(), Some(Component::Normal(first)) if first == std::ffi::OsStr::new(super::LOOKUPS_DIR))
        && parts.clone().next().is_some()
        && parts.all(|c| matches!(c, Component::Normal(_)));
    if in_lookups {
        return Ok(());
    }
    Err(format!(
        "lookup file `{path}` is not in `{dir}/`, where a bundle keeps its lookup files -- name it `{dir}/<name>`",
        dir = super::LOOKUPS_DIR
    ))
}

fn to_lookup_source(dto: &LookupSourceDto, at: Option<&BundleAt>) -> Result<LookupSource, String> {
    Ok(match dto {
        // `headers` is a GUI hint; the compiler reads the file.
        LookupSourceDto::Csv {
            path,
            delimiter,
            headers: _,
        } => LookupSource::Csv {
            file: resolve_lookup_file(path, at, "lookup csv source")?,
            delimiter: to_byte(delimiter, "lookup csv delimiter")?,
        },
        LookupSourceDto::Inline { headers, rows } => LookupSource::Inline {
            headers: headers.clone(),
            rows: rows.clone(),
        },
        LookupSourceDto::Mmdb { path } => LookupSource::Mmdb {
            file: resolve_lookup_file(path, at, "lookup mmdb source")?,
        },
    })
}

fn to_match_kind(dto: MatchKindDto) -> MatchKind {
    match dto {
        MatchKindDto::Exact => MatchKind::Exact,
        MatchKindDto::Cidr => MatchKind::Cidr,
        MatchKindDto::Regex => MatchKind::Regex,
    }
}

fn to_multiplicity(dto: MultiplicityDto) -> Multiplicity {
    match dto {
        MultiplicityDto::Best => Multiplicity::Best,
        MultiplicityDto::First => Multiplicity::First,
        MultiplicityDto::All => Multiplicity::All,
    }
}

/// `call_ids` are the ids of the calls `expand_calls` removed without an `If`: a disabled call, and a call with no
/// filter. No step of `steps` holds one, but they are step ids all the same, so they are held first, and a step that
/// names one is refused.
fn to_pipeline(
    dto: &PipelineDto,
    steps: &[StepDto],
    call_ids: &[String],
    id: usize,
    produces: &[Arc<Schema>],
    maps: &IdMaps,
) -> Result<PipelineConfig, String> {
    let in_pipeline = |e: String| format!("pipeline {:?}: {e}", dto.name);
    let mut held_step_ids = HashSet::new();
    let mut step_ids = ById::new();
    for call_id in call_ids {
        hold_step_id(call_id, &mut held_step_ids).map_err(in_pipeline)?;
    }
    let body = to_steps(steps, &mut step_ids, &mut held_step_ids, maps).map_err(in_pipeline)?;
    Ok(PipelineConfig {
        id,
        name: dto.name.clone(),
        body,
        step_ids,
        lookup_tables: maps
            .lookup_tables
            .iter()
            .map(|(table, (source, at))| (table.clone(), to_lookup_source(source, at.as_ref())))
            .collect(),
        grok_patterns: maps.grok_patterns.clone(),
        produces: produces.to_vec(),
    })
}

/// Lays `layer` over the kinds of `on_exception` that `scope` answers: `categories` each category it names, then `kinds`
/// each kind it names; and its tag field over `on_exception`'s.
fn lay_answers(
    on_exception: &mut OnException,
    each: &OnExceptionDto,
    scope: Layer,
    maps: &IdMaps,
) -> Result<(), String> {
    if let Some(field) = &each.tag_field {
        if scope == Layer::Destination {
            return Err("exceptions names a tag field; a destination tags no row".to_string());
        }
        // A `__` name is a control field, and the serializer never writes one.
        match crate::plan::tree::parse_name(field) {
            Ok(spelled) if !field.starts_with("__") => on_exception.tag_field = spelled,
            _ => return Err(format!("tag field {field:?} is not a field name a row can carry")),
        }
    }
    let mut lay = |answer: &AnswerDto, of: &dyn Fn(&str, Category) -> bool| -> Result<(), String> {
        let answer = to_answer(answer, maps)?;
        for (slot, (kind, category)) in on_exception.answers.iter_mut().zip(ExceptionKind::KINDS) {
            if of(kind, category) && scope.answers(kind, category) {
                *slot = answer.clone();
            }
        }
        Ok(())
    };
    for (name, answer) in &each.categories {
        let Some(&(_, named)) = Category::ALL.iter().find(|(label, _)| label == name) else {
            return Err(no_such(
                "category",
                "categories",
                name,
                Category::ALL.map(|(label, _)| label),
            ));
        };
        if !ExceptionKind::KINDS
            .iter()
            .any(|(kind, c)| *c == named && scope.answers(kind, *c))
        {
            return Err(scope.refuses(name));
        }
        lay(answer, &|_, category| category == named)?;
    }
    for (name, answer) in &each.kinds {
        let Some(&(_, category)) = ExceptionKind::KINDS.iter().find(|(label, _)| label == name) else {
            return Err(no_such(
                "kind",
                "kinds",
                name,
                ExceptionKind::KINDS.map(|(label, _)| label),
            ));
        };
        if !scope.answers(name, category) {
            return Err(scope.refuses(name));
        }
        lay(answer, &|kind, _| kind == name)?;
    }
    Ok(())
}

fn no_such<const N: usize>(what: &str, plural: &str, name: &str, labels: [&str; N]) -> String {
    format!(
        "exceptions names {name:?}, which is no {what}; the {plural} are {}",
        labels.join(", ")
    )
}

/// `dto`, each destination it delivers to by id.
fn to_answer(dto: &AnswerDto, maps: &IdMaps) -> Result<Answer, String> {
    let to = match dto {
        AnswerDto::Ignore => return Ok(Answer::Ignore),
        AnswerDto::Tag => return Ok(Answer::Tag),
        AnswerDto::Drop => return Ok(Answer::Drop),
        AnswerDto::Block => return Ok(Answer::Block),
        AnswerDto::Deliver { to } => to,
    };
    if to.is_empty() {
        return Err("exceptions delivers to no destination; name one, or answer another way".into());
    }
    let mut targets = Vec::with_capacity(to.len());
    for (i, name) in to.iter().enumerate() {
        if to[..i].contains(name) {
            return Err(format!("exceptions names destination {name:?} twice"));
        }
        let Some(&to) = maps.destinations.get(name) else {
            return Err(format!("exceptions names unknown destination {name:?}"));
        };
        targets.push(to);
    }
    Ok(Answer::Deliver(targets))
}

/// Replaces every `Call` step, recursively, with the callee's own steps. `visiting` is the chain of pipelines being
/// expanded, so a cycle is a named error. A disabled `Call` contributes no step, and its target is never looked up;
/// a `Call` with a `filter` wraps the inlined steps in an `If` that holds the call's id. Every other call's id goes on
/// `call_ids`, since no step is left to hold it. The compiler never sees a `Call`.
fn expand_calls(
    steps: &[StepDto],
    pipelines: &HashMap<String, PipelineDto>,
    visiting: &mut Vec<String>,
    call_ids: &mut Vec<String>,
    seen: &mut HashSet<String>,
) -> Result<Vec<StepDto>, String> {
    let mut out = Vec::with_capacity(steps.len());
    for step in steps {
        // `to_steps` refuses a repeated id once everything is expanded; refusing it here, at the first step of a
        // callee reached a second time, keeps the expansion from being copied out before the refusal. Pipelines
        // calling one another twice over multiply, and the copies would exist before the rule could see them.
        if !seen.insert(step_id(step).to_string()) {
            return Err(format!("duplicate step id {:?}", step_id(step)));
        }
        match step {
            StepDto::If {
                id,
                condition,
                then,
                r#else,
            } => out.push(StepDto::If {
                id: id.clone(),
                condition: condition.clone(),
                then: expand_calls(then, pipelines, visiting, call_ids, seen)?,
                r#else: expand_calls(r#else, pipelines, visiting, call_ids, seen)?,
            }),
            StepDto::Call {
                id,
                filter,
                disabled,
                pipeline,
            } => {
                if *disabled {
                    call_ids.push(id.clone());
                    continue;
                }
                let callee = pipelines
                    .get(pipeline)
                    .ok_or_else(|| format!("step {id:?} calls unknown pipeline {pipeline:?}"))?;
                if visiting.contains(&callee.id) {
                    // Every pipeline on the chain exists, so each has a name to show.
                    let chain: Vec<&str> = visiting
                        .iter()
                        .chain([&callee.id])
                        .map(|p| pipelines.get(p).map_or(p.as_str(), |dto| dto.name.as_str()))
                        .collect();
                    return Err(format!("step {id:?}: pipeline call cycle: {}", chain.join(" -> ")));
                }
                visiting.push(callee.id.clone());
                let expanded = expand_calls(&callee.steps, pipelines, visiting, call_ids, seen)?;
                visiting.pop();

                match filter {
                    // The call's own step: its filter is what this guard checks.
                    Some(cond) => out.push(StepDto::If {
                        id: id.clone(),
                        condition: cond.clone(),
                        then: expanded,
                        r#else: Vec::new(),
                    }),
                    None => {
                        call_ids.push(id.clone());
                        out.extend(expanded);
                    }
                }
            }
            other => out.push(other.clone()),
        }
    }
    Ok(out)
}

fn step_id(step: &StepDto) -> &str {
    match step {
        StepDto::If { id, .. }
        | StepDto::Comment { id, .. }
        | StepDto::Eval { id, .. }
        | StepDto::Code { id, .. }
        | StepDto::Drop { id, .. }
        | StepDto::Rename { id, .. }
        | StepDto::Remove { id, .. }
        | StepDto::Parser { id, .. }
        | StepDto::Lookup { id, .. }
        | StepDto::Unroll { id, .. }
        | StepDto::Call { id, .. } => id,
    }
}

fn to_steps(
    steps: &[StepDto],
    step_ids: &mut ById<FunctionId, String>,
    held_step_ids: &mut HashSet<String>,
    maps: &IdMaps,
) -> Result<Vec<Step>, String> {
    steps
        .iter()
        .map(|s| to_step(s, step_ids, held_step_ids, maps))
        .collect()
}

/// One `FunctionId` per step, in document order, local to this pipeline. An `if` takes its id before its branches.
/// `step_ids` grows in step: its index after each push is the `FunctionId` just assigned, so `step_ids[f]` is the
/// step that owns `FunctionId f`.
fn to_step(
    dto: &StepDto,
    step_ids: &mut ById<FunctionId, String>,
    held_step_ids: &mut HashSet<String>,
    maps: &IdMaps,
) -> Result<Step, String> {
    let (step_id, filter, disabled, kind) = match dto {
        StepDto::If {
            id,
            condition,
            then,
            r#else,
        } => {
            return Ok(Step::If {
                id: claim_step_id(id, step_ids, held_step_ids)?,
                cond: condition.clone(),
                then: to_steps(then, step_ids, held_step_ids, maps)?,
                else_: to_steps(r#else, step_ids, held_step_ids, maps)?,
            });
        }
        StepDto::Comment { id, text } => (id, None, false, FunctionKind::Comment { text: text.clone() }),
        StepDto::Eval {
            id,
            filter,
            disabled,
            assignments,
        } => {
            let assignments = assignments.iter().map(|a| (a.target.clone(), a.expr.clone())).collect();
            (id, filter.clone(), *disabled, FunctionKind::Eval { assignments })
        }
        StepDto::Code {
            id,
            filter,
            disabled,
            body,
        } => (id, filter.clone(), *disabled, FunctionKind::Code { body: body.clone() }),
        StepDto::Drop {
            id,
            filter,
            disabled,
            condition,
        } => (
            id,
            filter.clone(),
            *disabled,
            FunctionKind::Drop {
                cond: condition.clone(),
            },
        ),
        StepDto::Rename {
            id,
            filter,
            disabled,
            pairs,
        } => {
            let pairs = pairs.iter().map(|p| (p.from.clone(), p.to.clone())).collect();
            (id, filter.clone(), *disabled, FunctionKind::Rename { pairs })
        }
        StepDto::Remove {
            id,
            filter,
            disabled,
            fields,
        } => (
            id,
            filter.clone(),
            *disabled,
            FunctionKind::Remove { fields: fields.clone() },
        ),
        StepDto::Parser {
            id,
            filter,
            disabled,
            input,
            into,
            kind,
            declared,
        } => {
            let step_err = |e: String| format!("step {id:?}: {e}");
            let (parser_kind, declared) = to_parser_kind(kind, declared).map_err(step_err)?;
            (
                id,
                filter.clone(),
                *disabled,
                FunctionKind::Parser {
                    input: input.clone(),
                    into: into.clone(),
                    kind: parser_kind,
                    declared,
                },
            )
        }
        StepDto::Lookup {
            id,
            filter,
            disabled,
            input,
            table,
            match_kind,
            multiplicity,
            key_column,
            outputs,
        } => {
            if !maps.lookup_tables.contains_key(table) {
                return Err(format!("step {id:?} references unknown lookup table {table:?}"));
            }
            let outputs = outputs
                .iter()
                .map(|o| to_lookup_output(o).map_err(|e| format!("step {id:?}: {e}")))
                .collect::<Result<Vec<_>, _>>()?;
            (
                id,
                filter.clone(),
                *disabled,
                FunctionKind::Lookup {
                    input: input.clone(),
                    table: table.clone(),
                    match_kind: to_match_kind(*match_kind),
                    multiplicity: to_multiplicity(*multiplicity),
                    key_column: key_column.clone(),
                    outputs,
                },
            )
        }
        StepDto::Unroll {
            id,
            filter,
            disabled,
            field,
            keep_parent_fields,
        } => (
            id,
            filter.clone(),
            *disabled,
            FunctionKind::Unroll {
                field: field.clone(),
                keep_parent_fields: *keep_parent_fields,
            },
        ),
        StepDto::Call { id, .. } => {
            unreachable!("step {id:?}: Call should have been expanded by expand_calls")
        }
    };

    let id = claim_step_id(step_id, step_ids, held_step_ids)?;
    Ok(Step::Function(FunctionConfig {
        id,
        filter,
        disabled,
        kind,
    }))
}

/// The next `FunctionId`, for the step `step_id` names. Refused when `step_id` is already held.
fn claim_step_id(
    step_id: &str,
    step_ids: &mut ById<FunctionId, String>,
    held_step_ids: &mut HashSet<String>,
) -> Result<FunctionId, String> {
    hold_step_id(step_id, held_step_ids)?;
    Ok(step_ids.push(step_id.to_string()))
}

/// Marks `step_id` as held. Refused when it already is.
fn hold_step_id(step_id: &str, held_step_ids: &mut HashSet<String>) -> Result<(), String> {
    if !held_step_ids.insert(step_id.to_string()) {
        return Err(format!("duplicate step id {step_id:?}"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config_file::test_fixtures::example_json;

    #[test]
    fn this_host_is_read_from_the_url_s_host() {
        for url in [
            "http://127.0.0.1:9000",
            "http://[::1]/",
            "HTTP://Localhost",
            "http://user@localhost:8181/x",
            "http://localhost?x=1",
            "http://localhost#f",
            "localhost:9092",
        ] {
            assert!(on_loopback(url), "{url}");
            assert!(!plain_off_this_host(url), "{url}");
        }
        for url in ["http://minio:9000", "http://localhost@minio/", "http://minio?localhost"] {
            assert!(!on_loopback(url), "{url}");
            assert!(plain_off_this_host(url), "{url}");
        }
        assert!(!plain_off_this_host("https://minio"));
    }

    /// One SQL source of `kind`, reading every poll; `extra` adds fields to it.
    fn sql_config(kind: &str, extra: &str) -> String {
        format!(
            r#"{{"version":1,
                "sources":[{{"id":"db","type":"{kind}","host":" db.internal ","database":"auth","user":"reader",
                             "password":{{"secret":"db","version":2}},
                             "query":"SELECT event_id FROM events","mode":{{"type":"everyPoll"}},
                             "fields":[{{"name":"event_id","type":"i64"}}]{extra}}}]}}"#
        )
    }

    fn sql_refusal(json: &str) -> String {
        let file: FileConfig = serde_json::from_str(json).expect("parses");
        validation_errors(file)
            .iter()
            .map(|e| e.to_string())
            .collect::<Vec<_>>()
            .join("; ")
    }

    /// A mysql source loads as a postgres source does, read through a MySQL connection, in MySQL's SQL, on MySQL's
    /// port, under a place that names it.
    #[test]
    fn a_mysql_source_loads_as_a_postgres_source_does() {
        use crate::plan::topology::{SecretRef, SqlDatabase};
        let json = sql_config("mysql", "");
        assert!(sql_refusal(&json).is_empty(), "{}", sql_refusal(&json));
        let system = load_str(&json).expect("must load");
        system.compile_topology().expect("must compile");
        let kind = &system.topology.sources[0].kind;
        let SourceKind::Sql(sql) = kind else {
            panic!("expected an SQL source, got {kind:?}")
        };
        let SqlDatabase::MySql(connection) = &sql.database else {
            panic!("expected a MySQL database, got {:?}", sql.database)
        };
        assert_eq!(connection.host, "db.internal");
        assert_eq!(connection.port, 3306);
        assert!(matches!(
            connection.encrypted,
            crate::io::sql_source::Encrypted::Require
        ));
        assert_eq!(
            sql.source.query,
            crate::io::sql::Query::parse("SELECT event_id FROM events", crate::io::sql::Dialect::MySql).unwrap()
        );
        assert_eq!(&*sql.source.place, "db.internal:3306/auth");
        let place = kind.single_worker().expect("one worker reads it");
        assert!(place.starts_with("mysql://db.internal:3306/auth#"), "{place}");
        assert_eq!(
            sql.meta,
            crate::plan::topology::SourceMeta::MYSQL
                .declaring(vec![("event_id".into(), DataType::I64)])
                .unwrap()
        );
        let named = &system.topology.named_secrets;
        let named: Vec<_> = named.iter().map(|n| (&n.reference, n.what.as_str())).collect();
        let key = SecretRef::LeaderStored {
            name: "db".into(),
            version: 2,
        };
        assert_eq!(named, vec![(&key, r#"source "db": password"#)]);
    }

    /// A source on another port reads there, and one naming its kind's own port reads where one naming none does.
    #[test]
    fn an_sql_source_reads_on_its_port() {
        let place = |kind: &str, extra: &str| {
            let system = load_str(&sql_config(kind, extra)).expect("must load");
            system.topology.sources[0].kind.single_worker().unwrap()
        };
        for (kind, own, other) in [("postgres", 5432, 3306), ("mysql", 3306, 5432)] {
            assert_eq!(place(kind, &format!(r#","port":{own}"#)), place(kind, ""), "{kind}");
            let elsewhere = place(kind, &format!(r#","port":{other}"#));
            assert!(
                elsewhere.starts_with(&format!("{kind}://db.internal:{other}/auth#")),
                "{elsewhere}"
            );
        }
    }

    /// A mysql source is refused what a postgres source is.
    #[test]
    fn a_mysql_source_with_bad_settings_is_refused() {
        for (extra, field, why) in [
            (r#","port":0"#, "port", "port 0"),
            (
                r#","tls":"disable","certificate":"ghost""#,
                "certificate",
                "turns it off",
            ),
            (r#","host":" ""#, "host", "names no host"),
        ] {
            for kind in ["postgres", "mysql"] {
                let json =
                    sql_config(kind, extra).replacen(r#""host":" db.internal ","#, "", usize::from(field == "host"));
                let refused = sql_refusal(&json);
                assert!(refused.contains(why), "{kind} {field}: {refused}");
            }
        }
        let fields =
            sql_config("mysql", "").replace(r#""fields":[{"name":"event_id","type":"i64"}]"#, r#""fields":[]"#);
        assert!(
            sql_refusal(&fields).contains("at least one field"),
            "{}",
            sql_refusal(&fields)
        );
    }

    #[test]
    fn duplicate_source_ids_are_reported() {
        let json = example_json().replacen("\"self-metrics\"", "\"kafka-main\"", 1);
        let err = load_str(&json).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("duplicate source id"), "{msg}");
    }

    /// A route id is what a diff tells an edit from a delete-and-insert by, and what a metric about the route is
    /// labeled with, so two routes sharing one is refused the same as two sources would be.
    #[test]
    fn duplicate_route_ids_are_reported() {
        let json = example_json().replacen("\"catch-all\"", "\"errors\"", 1);
        let err = load_str(&json).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("duplicate route id"), "{msg}");
    }

    #[test]
    fn a_route_to_an_unknown_pipeline_is_reported() {
        let json = example_json().replacen("\"pipeline\": \"main\"", "\"pipeline\": \"ghost\"", 1);
        let err = load_str(&json).unwrap_err();
        assert!(err.to_string().contains("unknown pipeline"), "{err}");
    }

    #[test]
    fn a_route_that_names_no_source_is_reported() {
        let json = example_json().replacen("\"source\": \"kafka-main\"", "\"source\": \"\"", 1);
        let err = load_str(&json).unwrap_err();
        assert!(err.to_string().contains("names no source"), "{err}");
    }

    #[test]
    fn a_route_that_names_no_pipeline_is_reported() {
        let json = example_json().replacen("\"pipeline\": \"main\"", "\"pipeline\": \"\"", 1);
        let err = load_str(&json).unwrap_err();
        assert!(err.to_string().contains("names no pipeline"), "{err}");
    }

    #[test]
    fn a_deliver_rule_to_an_unknown_destination_is_reported() {
        let json = example_json().replacen("\"kafka-out\"", "\"kafka-ghost\"", 1);
        let err = load_str(&json).unwrap_err();
        assert!(err.to_string().contains("unknown destination"), "{err}");
    }

    // -- Call: inlining another pipeline -----------------------------------

    fn two_pipeline_json(caller_call_step: &str) -> String {
        format!(
            r#"{{
              "version": 1,
              "pipelines": [
                {{
                  "id": "main", "name": "main",
                  "steps": [{caller_call_step}]
                }},
                {{
                  "id": "helper", "name": "helper",
                  "steps": [
                    {{ "id": "h-eval", "type": "eval", "assignments": [
                      {{ "target": "enriched", "expr": "1" }}
                    ] }}
                  ]
                }}
              ]
            }}"#
        )
    }

    #[test]
    fn a_pipeline_with_a_call_step_compiles_end_to_end() {
        let json = two_pipeline_json(r#"{ "id": "c", "type": "call", "pipeline": "helper" }"#);
        let system = load_str(&json).unwrap();
        crate::plan::compile::compile(&system.pipelines[0], &crate::plan::topology::SourceMeta::KAFKA)
            .unwrap_or_else(|e| panic!("expected the inlined pipeline to compile: {e:?}"));
    }

    /// A called pipeline declares the field it reads, so it compiles on its own; inlined after the caller's parser,
    /// the declaration restates the caller's type.
    #[test]
    fn a_called_pipeline_declares_what_its_caller_writes() {
        let json = r#"{
          "version": 1,
          "pipelines": [
            { "id": "main", "name": "main", "steps": [
              { "id": "parse", "type": "parser", "input": "_raw", "kind": { "type": "json" },
                "declared": [ { "name": "source.ip", "type": "str" } ] },
              { "id": "c", "type": "call", "pipeline": "helper" }
            ] },
            { "id": "helper", "name": "helper", "steps": [
              { "id": "reads", "type": "code", "body": "source.ip: str;" },
              { "id": "h-eval", "type": "eval", "assignments": [ { "target": "seen", "expr": "source.ip" } ] }
            ] }
          ]
        }"#;
        let system = load_str(json).unwrap();
        for p in &system.pipelines {
            crate::plan::compile::compile(p, &crate::plan::topology::SourceMeta::KAFKA)
                .unwrap_or_else(|e| panic!("{} should compile: {e:?}", p.name));
        }
    }

    #[test]
    fn a_call_step_inlines_the_callee_pipelines_steps() {
        let json = two_pipeline_json(r#"{ "id": "c", "type": "call", "pipeline": "helper" }"#);
        let system = load_str(&json).unwrap();
        let main = &system.pipelines[0];
        assert_eq!(
            main.body.len(),
            1,
            "the callee's one step should be spliced in directly"
        );
        let Step::Function(f) = &main.body[0] else {
            panic!("expected a Function step");
        };
        assert!(matches!(&f.kind, FunctionKind::Eval { .. }));
    }

    #[test]
    fn a_call_with_a_filter_wraps_the_inlined_steps_in_an_if() {
        let json =
            two_pipeline_json(r#"{ "id": "c", "type": "call", "filter": "status >= 400", "pipeline": "helper" }"#);
        let system = load_str(&json).unwrap();
        let main = &system.pipelines[0];
        assert_eq!(main.body.len(), 1);
        let Step::If { cond, then, else_, .. } = &main.body[0] else {
            panic!("expected an If step wrapping the inlined call, got {:?}", main.body[0]);
        };
        assert_eq!(cond, "status >= 400");
        assert_eq!(then.len(), 1);
        assert!(else_.is_empty());
    }

    #[test]
    fn a_disabled_call_contributes_no_step() {
        let json = two_pipeline_json(r#"{ "id": "c", "type": "call", "disabled": true, "pipeline": "helper" }"#);
        let system = load_str(&json).unwrap();
        assert!(system.pipelines[0].body.is_empty());
    }

    #[test]
    fn a_disabled_call_to_an_unknown_pipeline_is_not_an_error() {
        let json = two_pipeline_json(r#"{ "id": "c", "type": "call", "disabled": true, "pipeline": "ghost" }"#);
        assert!(load_str(&json).is_ok());
    }

    #[test]
    fn a_call_to_an_unknown_pipeline_is_reported() {
        let json = two_pipeline_json(r#"{ "id": "c", "type": "call", "pipeline": "ghost" }"#);
        let err = load_str(&json).unwrap_err();
        assert!(err.to_string().contains("unknown pipeline"), "{err}");
    }

    #[test]
    fn a_call_cycle_is_reported() {
        let json = r#"{
          "version": 1,
          "pipelines": [
            {
              "id": "a", "name": "Alpha",
              "steps": [{ "id": "c1", "type": "call", "pipeline": "b" }]
            },
            {
              "id": "b", "name": "Beta",
              "steps": [{ "id": "c2", "type": "call", "pipeline": "a" }]
            }
          ]
        }"#;
        let err = load_str(json).unwrap_err();
        assert!(err.to_string().contains("cycle"), "{err}");
        // A person sees pipeline names, never ids.
        assert!(err.to_string().contains("Alpha -> Beta -> Alpha"), "{err}");
    }

    #[test]
    fn a_pipeline_calling_itself_is_a_cycle() {
        let json = r#"{
          "version": 1,
          "pipelines": [{
            "id": "a", "name": "a",
            "steps": [{ "id": "c1", "type": "call", "pipeline": "a" }]
          }]
        }"#;
        let err = load_str(json).unwrap_err();
        assert!(err.to_string().contains("cycle"), "{err}");
    }

    #[test]
    fn a_call_inside_an_if_branch_is_expanded_too() {
        let json = r#"{
          "version": 1,
          "pipelines": [
            {
              "id": "main", "name": "main",
              "steps": [{
                "id": "always", "type": "if", "condition": "true",
                "then": [{ "id": "c", "type": "call", "pipeline": "helper" }],
                "else": []
              }]
            },
            {
              "id": "helper", "name": "helper",
              "steps": [
                { "id": "h-eval", "type": "eval", "assignments": [
                  { "target": "enriched", "expr": "1" }
                ] }
              ]
            }
          ]
        }"#;
        let system = load_str(json).unwrap();
        let Step::If { then, .. } = &system.pipelines[0].body[0] else {
            panic!("expected the top-level If to survive");
        };
        assert_eq!(
            then.len(),
            1,
            "the callee's step should be inlined inside the then branch"
        );
        assert!(matches!(&then[0], Step::Function(_)));
    }

    // -- shared grok patterns -----------------------------------------------

    /// A grok pipeline naming `%{VENDOR:v}`, with `patterns` as the bundle's `grokPatterns`.
    fn naming_vendor(patterns: &str) -> String {
        format!(
            r#"{{
              "version": 1,
              "grokPatterns": {patterns},
              "pipelines": [{{
                "id": "p", "name": "p",
                "steps": [{{
                  "id": "g", "type": "parser", "input": "_raw",
                  "kind": {{ "type": "grok", "patterns": ["%{{VENDOR:v}}"] }},
                  "declared": [{{ "name": "v", "type": "str" }}]
                }}]
              }}]
            }}"#
        )
    }

    #[test]
    fn a_grok_step_names_a_bundle_pattern() {
        let system = load_str(&naming_vendor(r#"[{ "id": "VENDOR", "pattern": "acme-%{INT}" }]"#)).unwrap();
        assert_eq!(
            system.pipelines[0].grok_patterns.get("VENDOR").map(String::as_str),
            Some("acme-%{INT}")
        );
    }

    #[test]
    fn a_bundle_pattern_with_a_built_ins_name_is_refused_on_its_item() {
        let errors =
            validation_errors(serde_json::from_str(&naming_vendor(r#"[{ "id": "WORD", "pattern": "x" }]"#)).unwrap());
        let on_item = errors
            .iter()
            .find(|e| {
                e.item
                    .as_ref()
                    .is_some_and(|i| i.section == "grokPatterns" && i.id == "WORD")
            })
            .unwrap_or_else(|| panic!("{errors:?}"));
        assert_eq!(on_item.field, Some("pattern"));
    }

    #[test]
    fn a_duplicate_grok_pattern_id_is_reported() {
        let patterns = r#"[{ "id": "VENDOR", "pattern": "a" }, { "id": "VENDOR", "pattern": "b" }]"#;
        let err = load_str(&naming_vendor(patterns)).unwrap_err();
        assert!(err.to_string().contains("duplicate grok pattern id"), "{err}");
    }

    // -- schemas ------------------------------------------------------------

    /// A pipeline producing `produces` and a destination accepting `accepts`, with `schemas` as the bundle's own.
    fn with_schemas(schemas: &str, produces: &str, accepts: &str) -> FileConfig {
        serde_json::from_str(&format!(
            r#"{{
              "version": 1,
              "schemas": {schemas},
              "destinations": [{{ "id": "d", "type": "kafka", "brokers": "b:9092", "defaultTopic": "t",
                                  "accepts": {accepts} }}],
              "pipelines": [{{ "id": "p", "name": "p", "produces": {produces}, "steps": [] }}]
            }}"#
        ))
        .unwrap()
    }

    const LOGIN: &str = r#"[
        { "id": "acme-login", "fields": [{ "field": "user", "type": "str", "requirement": "required" }] },
        { "id": "acme-audit" }
    ]"#;

    #[test]
    fn a_pipeline_and_a_destination_name_declared_schemas() {
        let system = from_file_config_at(
            with_schemas(LOGIN, r#"["acme-audit", "acme-login"]"#, r#"["acme-login"]"#),
            &DeclaredAt::new(),
        )
        .unwrap();
        let ids = |s: &[Arc<Schema>]| s.iter().map(|s| s.id.clone()).collect::<Vec<_>>();
        assert_eq!(ids(system.produced_by(0)), ["acme-audit", "acme-login"]);
        assert_eq!(
            system.accepts.values().map(|a| ids(a)).collect::<Vec<_>>(),
            [["acme-login"]]
        );
    }

    /// The one error `file` has, on the item and setting it names.
    fn the_error(file: FileConfig) -> (String, String, Option<&'static str>, String) {
        let errors = validation_errors(file);
        let [e] = errors.as_slice() else { panic!("{errors:?}") };
        let item = e.item.as_ref().unwrap_or_else(|| panic!("{e:?}"));
        (item.section.to_string(), item.id.clone(), e.field, e.message.clone())
    }

    #[test]
    fn a_schema_id_that_names_no_schema_is_refused_on_what_names_it() {
        let (section, id, field, message) = the_error(with_schemas("[]", r#"["acme-login"]"#, "[]"));
        assert_eq!(
            (section.as_str(), id.as_str(), field),
            ("pipelines", "p", Some("produces"))
        );
        assert!(message.contains("\"acme-login\""), "{message}");
        let (section, _, field, _) = the_error(with_schemas("[]", "[]", r#"["nothing"]"#));
        assert_eq!((section.as_str(), field), ("destinations", Some("accepts")));
    }

    #[test]
    fn a_declared_schema_that_does_not_read_or_whose_id_is_taken_is_refused_on_its_item() {
        let broken = r#"[{ "id": "mine", "fields": [{ "field": "a", "type": "string" }] }]"#;
        let (section, id, field, _) = the_error(with_schemas(broken, "[]", "[]"));
        assert_eq!(
            (section.as_str(), id.as_str(), field),
            ("schemas", "mine", Some("fields"))
        );
        let twice = r#"[{ "id": "mine" }, { "id": "mine" }]"#;
        let (_, _, _, message) = the_error(with_schemas(twice, "[]", "[]"));
        assert!(message.contains("duplicate schema id"), "{message}");
    }

    // -- shared lookup tables -----------------------------------------------

    #[test]
    fn a_lookup_referencing_an_unknown_table_is_reported() {
        let json = r#"{
          "version": 1,
          "pipelines": [{
            "id": "p", "name": "p",
            "steps": [{
              "id": "l", "type": "lookup",
              "input": "ip", "table": "ghost", "matchKind": "exact",
              "outputs": []
            }]
          }]
        }"#;
        let err = load_str(json).unwrap_err();
        assert!(err.to_string().contains("unknown lookup table"), "{err}");
    }

    #[test]
    fn a_duplicate_lookup_table_id_is_reported() {
        let json = r#"{
          "version": 1,
          "lookupTables": [
            { "id": "t", "type": "inline", "headers": [], "rows": [] },
            { "id": "t", "type": "mmdb", "path": "/tmp/x.mmdb" }
          ]
        }"#;
        let err = load_str(json).unwrap_err();
        assert!(err.to_string().contains("duplicate lookup table id"), "{err}");
    }

    #[test]
    fn two_steps_can_share_the_same_lookup_table() {
        let json = r#"{
          "version": 1,
          "lookupTables": [
            { "id": "geo", "type": "inline", "headers": ["ip", "city"], "rows": [] }
          ],
          "pipelines": [{
            "id": "p", "name": "p",
            "steps": [
              {
                "id": "l1", "type": "lookup",
                "input": "src_ip", "table": "geo", "matchKind": "exact",
                "keyColumn": "ip",
                "outputs": [{ "source": "city", "field": "src_city" }]
              },
              {
                "id": "l2", "type": "lookup",
                "input": "dst_ip", "table": "geo", "matchKind": "exact",
                "keyColumn": "ip",
                "outputs": [{ "source": "city", "field": "dst_city" }]
              }
            ]
          }]
        }"#;
        let system = load_str(json).unwrap();
        assert_eq!(system.pipelines[0].body.len(), 2);
        for step in &system.pipelines[0].body {
            let Step::Function(f) = step else { panic!() };
            assert!(matches!(
                &f.kind,
                FunctionKind::Lookup { table, .. } if table == "geo"
            ));
        }
    }

    #[test]
    fn a_lookup_output_type_loads_and_a_default_that_does_not_fit_it_is_refused() {
        let json = |default: &str, ty: &str| {
            format!(
                r#"{{
              "version": 1,
              "sources": [], "destinations": [], "routes": [],
              "lookupTables": [
                {{ "id": "apps", "type": "inline", "headers": ["name", "risk"], "rows": [] }}
              ],
              "pipelines": [{{
                "id": "p", "name": "p",
                "steps": [{{
                  "id": "l", "type": "lookup",
                  "input": "app", "table": "apps", "matchKind": "exact", "keyColumn": "name",
                  "outputs": [{{ "source": "risk", "field": "risk", "default": "{default}", "type": {ty} }}]
                }}]
              }}]
            }}"#
            )
        };
        let system = load_str(&json("0", "\"i64\"")).unwrap();
        let Step::Function(f) = &system.pipelines[0].body[0] else {
            panic!()
        };
        let FunctionKind::Lookup { outputs, .. } = &f.kind else {
            panic!()
        };
        assert_eq!(outputs[0].ty, DataType::I64);
        assert_eq!(outputs[0].time_source, None);

        let system = load_str(&json("", "{ \"type\": \"datetime\", \"format\": \"%Y-%m-%d\" }")).unwrap();
        let Step::Function(f) = &system.pipelines[0].body[0] else {
            panic!()
        };
        let FunctionKind::Lookup { outputs, .. } = &f.kind else {
            panic!()
        };
        assert_eq!(outputs[0].ty, DataType::DateTime);
        assert_eq!(outputs[0].time_source, Some(TimeSource::Format("%Y-%m-%d".into())));

        let err = load_str(&json("none", "\"i64\"")).expect_err("a default that is not an i64");
        let text = format!("{err:?}");
        assert!(
            text.contains(r#"output \"risk\": its default \"none\" does not fit i64"#),
            "{text}"
        );

        let err = load_str(&json("", "{ \"type\": \"array\", \"of\": \"str\" }")).expect_err("an array");
        assert!(format!("{err:?}").contains("one value"), "{err:?}");
    }

    #[test]
    fn a_multi_character_delimiter_is_rejected() {
        let json = example_json().replacen(
            "{ \"type\": \"json\" }",
            "{ \"type\": \"csv\", \"delimiter\": \"::\" }",
            1,
        );
        let err = load_str(&json).unwrap_err();
        assert!(err.to_string().contains("one ASCII character"), "{err}");
    }

    #[test]
    fn duplicate_step_ids_within_one_pipeline_are_reported() {
        let json = example_json().replacen("\"tag-high\"", "\"parse\"", 1);
        let err = load_str(&json).unwrap_err();
        assert!(err.to_string().contains("duplicate step id"), "{err}");
    }

    #[test]
    fn multiple_errors_are_all_reported_together() {
        let json = example_json()
            .replacen("\"pipeline\": \"main\"", "\"pipeline\": \"ghost\"", 1)
            .replacen("\"kafka-out\"", "\"kafka-ghost\"", 1);
        let LoadError::Validation(errors) = load_str(&json).unwrap_err() else {
            panic!("expected a Validation error");
        };
        assert!(errors.len() >= 2, "{errors:?}");
    }

    #[test]
    fn an_unknown_step_type_is_a_clear_parse_error() {
        let json = example_json().replacen("\"type\": \"eval\"", "\"type\": \"eval2\"", 1);
        let err = load_str(&json).unwrap_err();
        assert!(matches!(err, LoadError::Json(_)));
    }

    #[test]
    fn a_lookup_outputs_empty_string_default_loads_as_null_not_empty_string() {
        let json = r#"{
          "version": 1,
          "lookupTables": [
            { "id": "t", "type": "inline", "headers": ["ip", "city"], "rows": [] }
          ],
          "pipelines": [{
            "id": "p", "name": "p",
            "steps": [{
              "id": "l", "type": "lookup",
              "input": "ip",
              "table": "t",
              "matchKind": "exact",
              "keyColumn": "ip",
              "outputs": [
                { "source": "city", "field": "geo.city", "default": "" },
                { "source": "city", "field": "geo.city2", "default": "unknown" }
              ]
            }]
          }]
        }"#;
        let system = load_str(json).unwrap();
        let Step::Function(f) = &system.pipelines[0].body[0] else {
            unreachable!()
        };
        let FunctionKind::Lookup { outputs, .. } = &f.kind else {
            unreachable!()
        };
        assert_eq!(outputs[0].default, None, "an empty string loads as null");
        assert_eq!(outputs[1].default, Some("unknown".to_string()));
    }

    #[test]
    fn a_code_step_loads_its_body_verbatim() {
        let json = r#"{
          "version": 1,
          "pipelines": [{
            "id": "p", "name": "p",
            "steps": [{
              "id": "c", "type": "code",
              "body": "let x = status + 1; severity = x > 400 ? \"high\" : \"low\";"
            }]
          }]
        }"#;
        let system = load_str(json).unwrap();
        let Step::Function(f) = &system.pipelines[0].body[0] else {
            unreachable!()
        };
        let FunctionKind::Code { body } = &f.kind else {
            unreachable!()
        };
        assert_eq!(body, "let x = status + 1; severity = x > 400 ? \"high\" : \"low\";");
    }

    #[test]
    fn struct_and_array_declared_types_round_trip() {
        let json = r#"{
          "version": 1,
          "pipelines": [{
            "id": "p", "name": "p",
            "steps": [{
              "id": "parse", "type": "parser", "input": "_raw", "kind": { "type": "json" },
              "declared": [
                { "name": "tags", "type": { "type": "array", "of": "str" } },
                { "name": "geo", "type": { "type": "struct", "fields": [
                  { "name": "lat", "type": "f64" }, { "name": "lon", "type": "f64" }
                ] } }
              ]
            }]
          }]
        }"#;
        let system = load_str(json).unwrap();
        let Step::Function(f) = &system.pipelines[0].body[0] else {
            unreachable!()
        };
        let FunctionKind::Parser { declared, .. } = &f.kind else {
            unreachable!()
        };
        assert_eq!(
            declared[0],
            DeclaredField {
                name: "tags".into(),
                ty: DataType::Array(Box::new(DataType::Str)),
                time_source: None,
            }
        );
        assert_eq!(
            declared[1],
            DeclaredField {
                name: "geo".into(),
                ty: DataType::Struct(
                    vec![("lat".into(), DataType::F64), ("lon".into(), DataType::F64)],
                    false
                ),
                time_source: None,
            }
        );
    }

    /// A JSON config with one parser step declaring a single field of `ty`, for the refusal tests below.
    fn declaring(ty: &str) -> String {
        format!(
            r#"{{
              "version": 1,
              "pipelines": [{{
                "id": "p", "name": "p",
                "steps": [{{
                  "id": "parse", "type": "parser", "input": "_raw", "kind": {{ "type": "json" }},
                  "declared": [{{ "name": "ts", "type": {ty} }}]
                }}]
              }}]
            }}"#
        )
    }

    #[test]
    fn a_bare_datetime_declaration_is_auto_detected() {
        let system = load_str(&declaring(r#""datetime""#)).unwrap();
        let Step::Function(f) = &system.pipelines[0].body[0] else {
            unreachable!()
        };
        let FunctionKind::Parser { declared, .. } = &f.kind else {
            unreachable!()
        };
        assert_eq!(declared[0].ty, DataType::DateTime);
        assert_eq!(declared[0].time_source, Some(TimeSource::Auto));
    }

    #[test]
    fn a_datetime_with_a_format_carries_it() {
        let system = load_str(&declaring(r#"{ "type": "datetime", "format": "%Y/%m/%d %H:%M:%S" }"#)).unwrap();
        let Step::Function(f) = &system.pipelines[0].body[0] else {
            unreachable!()
        };
        let FunctionKind::Parser { declared, .. } = &f.kind else {
            unreachable!()
        };
        assert_eq!(declared[0].ty, DataType::DateTime);
        assert_eq!(
            declared[0].time_source,
            Some(TimeSource::Format("%Y/%m/%d %H:%M:%S".into()))
        );
    }

    #[test]
    fn a_datetime_format_with_a_specifier_parsetime_lacks_is_refused() {
        let err = load_str(&declaring(r#"{ "type": "datetime", "format": "%Y-%m-%d %J" }"#)).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("%J"), "{msg}");
        assert!(msg.contains("\"ts\""), "{msg}");
    }

    #[test]
    fn a_bare_duration_declaration_is_refused() {
        let err = load_str(&declaring(r#""duration""#)).unwrap_err();
        assert!(err.to_string().contains("column of nulls"), "{err}");
    }

    #[test]
    fn a_bare_bytes_declaration_is_refused() {
        let err = load_str(&declaring(r#""bytes""#)).unwrap_err();
        assert!(err.to_string().contains("column of nulls"), "{err}");
    }

    #[test]
    fn a_datetime_nested_in_an_array_is_refused() {
        for ty in [r#""datetime""#, r#"{ "type": "datetime", "format": "%Y" }"#] {
            let err = load_str(&declaring(&format!(r#"{{ "type": "array", "of": {ty} }}"#))).unwrap_err();
            let msg = err.to_string();
            assert!(msg.contains("nested"), "{msg}");
        }
    }

    #[test]
    fn a_datetime_nested_in_a_struct_is_refused() {
        let err = load_str(&declaring(
            r#"{ "type": "struct", "fields": [{ "name": "at", "type": "datetime" }] }"#,
        ))
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("nested"), "{msg}");
    }

    #[test]
    fn a_struct_is_open_where_it_says_so() {
        for (open, want) in [(r#", "open": true"#, true), ("", false)] {
            let system = load_str(&declaring(&format!(
                r#"{{ "type": "struct", "fields": [{{ "name": "a", "type": "i64" }}]{open} }}"#
            )))
            .unwrap();
            let Step::Function(f) = &system.pipelines[0].body[0] else {
                unreachable!()
            };
            let FunctionKind::Parser { declared, .. } = &f.kind else {
                unreachable!()
            };
            assert_eq!(
                declared[0].ty,
                DataType::Struct(vec![("a".into(), DataType::I64)], want)
            );
        }
    }

    /// `variant` is the type of a value whose shape only the row knows, and a declaration names it as it names
    /// any other type: bare, as an array's element and as a struct's field.
    #[test]
    fn a_variant_is_a_declared_type() {
        for (ty, want) in [
            (r#""variant""#, DataType::Variant),
            (
                r#"{ "type": "array", "of": "variant" }"#,
                DataType::Array(Box::new(DataType::Variant)),
            ),
            (
                r#"{ "type": "struct", "fields": [{ "name": "a", "type": "variant" }] }"#,
                DataType::Struct(vec![("a".into(), DataType::Variant)], false),
            ),
        ] {
            let system = load_str(&declaring(ty)).unwrap();
            let Step::Function(f) = &system.pipelines[0].body[0] else {
                unreachable!()
            };
            let FunctionKind::Parser { declared, .. } = &f.kind else {
                unreachable!()
            };
            assert_eq!(declared[0].ty, want, "{ty}");
        }
    }

    /// Mirrors how `eval_assignments`'s own messages are checked: the refusal names where it came from.
    #[test]
    fn a_refused_declared_field_names_the_pipeline_step_and_field() {
        let err = load_str(&declaring(r#""duration""#)).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("\"p\""), "{msg}");
        assert!(msg.contains("\"parse\""), "{msg}");
        assert!(msg.contains("\"ts\""), "{msg}");
    }

    #[test]
    fn validation_errors_is_empty_for_a_clean_file() {
        let file: FileConfig = serde_json::from_str(example_json()).unwrap();
        assert!(validation_errors(file).is_empty());
    }

    #[test]
    fn a_reserved_kafka_property_is_refused() {
        let json = example_json().replacen(
            r#""groupId": "smeltr""#,
            r#""groupId": "smeltr", "properties": {"enable.auto.commit": "true"}"#,
            1,
        );
        let file: FileConfig = serde_json::from_str(&json).unwrap();
        let errors = validation_errors(file);
        assert!(
            errors
                .iter()
                .any(|e| e.contains("enable.auto.commit") && e.contains("smeltr owns")),
            "{errors:?}"
        );
    }

    /// A blank name is caught at load, whether it names a variable or a leader secret, not when a worker looks it up.
    #[test]
    fn a_credential_with_a_blank_name_is_refused() {
        for credential in [r#""""#, r#"{"secret": " ", "version": 1}"#] {
            let json = example_json().replacen(
                r#""groupId": "smeltr""#,
                &format!(r#""groupId": "smeltr", "credentials": {{"sasl.password": {credential}}}"#),
                1,
            );
            let file: FileConfig = serde_json::from_str(&json).unwrap();
            let errors = validation_errors(file);
            assert!(errors.iter().any(|e| e.contains("blank name")), "{errors:?}");
        }
    }

    /// A password in `properties` would be committed and read in review.
    #[test]
    fn a_secret_kafka_property_must_go_through_the_env_map() {
        let json = example_json().replacen(
            r#""groupId": "smeltr""#,
            r#""groupId": "smeltr", "properties": {"sasl.password": "hunter2"}"#,
            1,
        );
        let file: FileConfig = serde_json::from_str(&json).unwrap();
        let errors = validation_errors(file);
        assert!(
            errors
                .iter()
                .any(|e| e.contains("sasl.password") && e.contains("credentials")),
            "{errors:?}"
        );
    }

    /// The mechanism decides which provider the build must carry, and a
    /// value named through the environment cannot be asked about at save.
    #[test]
    fn the_sasl_mechanism_may_not_be_named_through_the_environment() {
        for key in ["sasl.mechanism", "sasl.mechanisms"] {
            let json = example_json().replacen(
                r#""groupId": "smeltr""#,
                &format!(r#""groupId": "smeltr", "credentials": {{"{key}": "KAFKA_MECH"}}"#),
                1,
            );
            let file: FileConfig = serde_json::from_str(&json).unwrap();
            let errors = validation_errors(file);
            assert!(
                errors.iter().any(|e| e.contains(key) && e.contains("credentials")),
                "{errors:?}"
            );
        }
    }

    /// Every credential-valued property librdkafka documents.
    #[test]
    fn every_credential_valued_property_is_refused_in_the_literal_map() {
        for key in [
            "sasl.password",
            "ssl.key.password",
            "ssl.keystore.password",
            "ssl.key.pem",
            "sasl.oauthbearer.client.secret",
            "sasl.oauthbearer.client.credentials.client.secret",
            "sasl.oauthbearer.assertion.private.key.pem",
            "sasl.oauthbearer.assertion.private.key.passphrase",
        ] {
            let json = example_json().replacen(
                r#""groupId": "smeltr""#,
                &format!(r#""groupId": "smeltr", "properties": {{"{key}": "x"}}"#),
                1,
            );
            let file: FileConfig = serde_json::from_str(&json).unwrap();
            let errors = validation_errors(file);
            assert!(
                errors.iter().any(|e| e.contains(key) && e.contains("credentials")),
                "{key} was accepted into the literal map: {errors:?}"
            );
        }
    }

    /// The value is checked as well as the name.
    #[test]
    fn pem_key_material_is_refused_whatever_the_property_is_called() {
        let json = example_json().replacen(
            r#""groupId": "smeltr""#,
            r#""groupId": "smeltr", "properties": {"some.future.credential": "-----BEGIN PRIVATE KEY-----\nMIIEvQ\n-----END PRIVATE KEY-----"}"#,
            1,
        );
        let file: FileConfig = serde_json::from_str(&json).unwrap();
        let errors = validation_errors(file);
        assert!(errors.iter().any(|e| e.contains("key material")), "{errors:?}");
    }

    /// A path to key material is not a credential.
    #[test]
    fn paths_to_key_material_are_not_credentials() {
        for key in [
            "ssl.key.location",
            "ssl.ca.location",
            "ssl.certificate.location",
            "sasl.oauthbearer.assertion.private.key.file",
        ] {
            assert!(!is_credential_valued(key), "{key} is a path, not a credential");
        }
    }

    /// Both spellings of a reserved property are reserved.
    #[test]
    fn an_alias_of_a_reserved_property_is_reserved_too() {
        for (alias, canonical) in [
            ("metadata.broker.list", "bootstrap.servers"),
            ("compression.codec", "compression.type"),
            ("request.required.acks", "acks"),
        ] {
            let json = example_json().replacen(
                r#""groupId": "smeltr""#,
                &format!(r#""groupId": "smeltr", "properties": {{"{alias}": "true"}}"#),
                1,
            );
            let file: FileConfig = serde_json::from_str(&json).unwrap();
            let errors = validation_errors(file);
            assert!(
                errors.iter().any(|e| e.contains(alias) && e.contains("smeltr owns")),
                "{alias} (an alias of {canonical}) was accepted: {errors:?}"
            );
        }
    }

    /// Linking Cyrus made this property live: librdkafka hands it to
    /// `system()` on every worker. Write access to a bundle must not be
    /// shell access to the fleet.
    #[test]
    fn the_kinit_command_is_refused_as_a_shell_command() {
        let json = example_json().replacen(
            r#""groupId": "smeltr""#,
            r#""groupId": "smeltr", "properties": {"sasl.kerberos.kinit.cmd": "curl x | sh"}"#,
            1,
        );
        let file: FileConfig = serde_json::from_str(&json).unwrap();
        let errors = validation_errors(file);
        assert!(
            errors
                .iter()
                .any(|e| e.contains("sasl.kerberos.kinit.cmd") && e.contains("shell command")),
            "{errors:?}"
        );
    }

    /// The legacy topic-level spelling does nothing on the high-level consumer; it is refused rather than silently
    /// ignored.
    #[test]
    fn the_legacy_auto_commit_property_is_refused_as_the_wrong_knob() {
        let json = example_json().replacen(
            r#""groupId": "smeltr""#,
            r#""groupId": "smeltr", "properties": {"auto.commit.enable": "true"}"#,
            1,
        );
        let file: FileConfig = serde_json::from_str(&json).unwrap();
        let errors = validation_errors(file);
        assert!(
            errors
                .iter()
                .any(|e| e.contains("auto.commit.enable") && e.contains("keeps auto-commit off")),
            "{errors:?}"
        );
    }

    /// The GUI's `LIMITS` (`gui-frontend/src/api.ts`) offers each field's bounds and default without asking the
    /// leader; these are the ones a save enforces, so a form never offers a value the save refuses.
    #[test]
    fn the_gui_offers_the_bounds_a_save_enforces() {
        use crate::config_file::dto::{
            BufferBytes as Buffer, BulkBytes as Bulk, FetchBytes as Fetch, FileInterval as File, HttpRequestBytes,
            InFlight, MaxDeliveries as Deliveries, MaxRequestBytes as Receiver, ObjectBytes as Object,
            PollIntervalSecs, RequestBytes, StallAfter as Stall, VisibilitySecs as Visibility,
        };
        use crate::plan::topology::MAX_SENDERS;
        type Request = RequestBytes;
        // Name, min, max (none where the loader sets none), default (none where absent is not a number).
        use crate::config_file::dto::CompressionDto::{self as Codec, Gzip, Lz4, Zstd};
        let level = |name, codec: Codec, kafka| {
            let (levels, default) = codec.levels(kafka).expect("a codec with levels");
            (
                name,
                u64::from(*levels.start()),
                Some(u64::from(*levels.end())),
                u64::from(default),
            )
        };
        let want: Vec<(&str, u64, Option<u64>, Option<u64>)> = [
            ("inFlight", 1, Some(MAX_SENDERS as u64), InFlight::DEFAULT as u64),
            level("gzipLevel", Gzip, false),
            level("zstdLevel", Zstd, false),
            level("kafkaZstdLevel", Zstd, true),
            level("kafkaLz4Level", Lz4, true),
            (
                "maxBulkBytes",
                Bulk::MIN as u64,
                Some(Bulk::MAX as u64),
                Bulk::DEFAULT as u64,
            ),
            (
                "maxRequestBytes",
                Request::MIN as u64,
                Some(Request::MAX as u64),
                Request::DEFAULT as u64,
            ),
            (
                "httpRequestBytes",
                Request::MIN as u64,
                Some(Request::MAX as u64),
                HttpRequestBytes::DEFAULT as u64,
            ),
            (
                "logAnalyticsRequestBytes",
                Request::MIN as u64,
                Some(LOG_ANALYTICS_MAX_REQUEST_BYTES as u64),
                Request::DEFAULT as u64,
            ),
            (
                "receiverRequestBytes",
                Receiver::MIN,
                Some(Receiver::MAX),
                Receiver::DEFAULT,
            ),
            (
                "maxObjectBytes",
                Object::MIN as u64,
                Some(Object::MAX as u64),
                Object::DEFAULT as u64,
            ),
            ("stallAfterSecs", Stall::MIN, Some(Stall::MAX), Stall::DEFAULT),
            (
                "bufferBytes",
                Buffer::MIN as u64,
                Some(Buffer::MAX as u64),
                Buffer::DEFAULT as u64,
            ),
            ("fileIntervalSecs", File::MIN, Some(File::MAX), File::DEFAULT),
            (
                "objectBufferBytes",
                Buffer::MIN as u64,
                Some(Buffer::OBJECT_MAX as u64),
                Buffer::OBJECT_DEFAULT as u64,
            ),
            (
                "fetchBytes",
                Fetch::MIN as u64,
                Some(Fetch::MAX as u64),
                Fetch::DEFAULT as u64,
            ),
            (
                "visibilitySecs",
                Visibility::MIN,
                Some(Visibility::MAX),
                Visibility::DEFAULT,
            ),
            (
                "ackDeadlineSecs",
                Visibility::MIN,
                Some(ACK_DEADLINE_MAX),
                Visibility::DEFAULT,
            ),
            (
                "maxDeliveries",
                Deliveries::MIN.into(),
                Some(Deliveries::MAX.into()),
                Deliveries::DEFAULT.into(),
            ),
            // The loader refuses only 0.
            ("pollIntervalSecs", 1, None, PollIntervalSecs::DEFAULT),
        ]
        .into_iter()
        .map(|(name, min, max, default)| (name, min, max, Some(default)))
        .flat_map(|entry| {
            let after = (entry.0 == "receiverRequestBytes").then_some((
                "maxConnections",
                1,
                Some(ConnectionLimits::MOST as u64),
                None,
            ));
            std::iter::once(entry).chain(after)
        })
        .collect();

        let source = include_str!("../../gui-frontend/src/api.ts");
        let table = &source[source.find("export const LIMITS = {").expect("LIMITS")..];
        let table = &table[..table.find("} as const;").expect("the end of LIMITS")];
        // `64 * 1024 * 1024` or `43_200`; `Number.MAX_SAFE_INTEGER` is no bound.
        let number = |text: &str| -> Option<u64> {
            let text = text.trim();
            (text != "Number.MAX_SAFE_INTEGER").then(|| {
                text.split('*')
                    .map(|n| n.trim().replace('_', "").parse::<u64>().expect(text))
                    .product()
            })
        };
        let offered: Vec<(&str, u64, Option<u64>, Option<u64>)> = table
            .split("},")
            .filter(|entry| entry.contains("min:"))
            .map(|entry| {
                let head = &entry[..entry.find(": {").expect("an entry")];
                let name = head.rsplit(|c: char| !c.is_alphanumeric()).next().expect("its name");
                let field = |key: &str| {
                    let rest = &entry[entry.find(&format!("{key}:")).expect(key) + key.len() + 1..];
                    number(&rest[..rest.find([',', '}']).unwrap_or(rest.len())])
                };
                (
                    name,
                    field("min").expect("a min"),
                    field("max"),
                    entry.contains("default:").then(|| field("default").expect("a default")),
                )
            })
            .collect();
        assert_eq!(offered, want);
    }

    /// The GUI's Kafka form (`gui-frontend/src/kafka.ts`, `FIELD_LABELS`) shows each reason with its label for every
    /// field the reason names in backticks; one it has no label for would show as the JSON key.
    #[test]
    fn the_gui_labels_every_field_a_reserved_reason_names() {
        let source = include_str!("../../gui-frontend/src/kafka.ts");
        let labels = &source[source.find("const FIELD_LABELS").expect("FIELD_LABELS")..];
        let labels = &labels[..labels.find("]);").expect("the end of FIELD_LABELS")];
        for (key, why) in RESERVED_KAFKA_PROPS {
            for field in why.split('`').skip(1).step_by(2) {
                assert!(
                    labels.contains(&format!("['{field}', '")),
                    "{key} names `{field}`, which the form has no label for"
                );
            }
        }
    }

    /// Every property smeltr sets on a client is reserved.
    #[test]
    fn properties_smeltr_sets_itself_are_reserved() {
        for key in [
            "partition.assignment.strategy",
            "queue.buffering.max.kbytes",
            "queued.max.messages.kbytes",
            "max.poll.interval.ms",
            "enable.idempotence",
            "statistics.interval.ms",
            "fetch.max.bytes",
            "fetch.message.max.bytes",
            "receive.message.max.bytes",
            "acks",
            "queue.buffering.max.messages",
        ] {
            let json = example_json().replacen(
                r#""groupId": "smeltr""#,
                &format!(r#""groupId": "smeltr", "properties": {{"{key}": "1"}}"#),
                1,
            );
            let file: FileConfig = serde_json::from_str(&json).unwrap();
            let errors = validation_errors(file);
            assert!(
                errors.iter().any(|e| e.contains(key) && e.contains("smeltr owns")),
                "{key} was accepted: {errors:?}"
            );
        }
    }

    /// A transactional producer refuses every record sent outside a transaction, and smeltr begins none.
    #[test]
    fn a_transactional_id_is_reserved() {
        let json = example_json().replacen(
            r#""groupId": "smeltr""#,
            r#""groupId": "smeltr", "properties": {"transactional.id": "smeltr"}"#,
            1,
        );
        let file: FileConfig = serde_json::from_str(&json).unwrap();
        let errors = validation_errors(file);
        assert!(
            errors
                .iter()
                .any(|e| e.contains("transactional.id") && e.contains("smeltr owns")),
            "{errors:?}"
        );
    }

    #[test]
    fn a_property_in_both_maps_is_refused() {
        let json = example_json().replacen(
            r#""groupId": "smeltr""#,
            r#""groupId": "smeltr", "properties": {"sasl.username": "a"}, "credentials": {"sasl.username": "SOME_VAR"}"#,
            1,
        );
        let file: FileConfig = serde_json::from_str(&json).unwrap();
        let errors = validation_errors(file);
        assert!(errors.iter().any(|e| e.contains("both")), "{errors:?}");
    }

    #[test]
    fn ordinary_kafka_properties_are_accepted() {
        let json = example_json().replacen(
            r#""groupId": "smeltr""#,
            r#""groupId": "smeltr", "properties": {"security.protocol": "SASL_SSL", "sasl.mechanism": "SCRAM-SHA-512", "sasl.username": "smeltr"}, "credentials": {"sasl.password": "KAFKA_PASSWORD"}"#,
            1,
        );
        let file: FileConfig = serde_json::from_str(&json).unwrap();
        assert!(validation_errors(file).is_empty());
    }

    /// OIDC needs libcurl, which `curl-static` links in. The config surface offers `sasl.oauthbearer.*`, so the build
    /// has to accept it.
    #[test]
    fn the_oidc_properties_this_build_offers_are_accepted_at_save_time() {
        let json = example_json().replacen(
            r#""groupId": "smeltr""#,
            r#""groupId": "smeltr", "properties": {"sasl.oauthbearer.method": "oidc"}"#,
            1,
        );
        let file: FileConfig = serde_json::from_str(&json).unwrap();
        let errors = validation_errors(file);
        assert!(
            errors.is_empty(),
            "{errors:?}\nA librdkafka built without the OIDC macros fails here. On Windows, `cargo clean -p rdkafka-sys` and build again to test."
        );
    }

    // -- TLS ----------------------------------------------------------------

    /// A self-signed certificate, in PEM. Minted rather than pasted: a fixture certificate expires, and a test that
    /// starts failing on a date is worse than no test.
    fn a_certificate() -> String {
        let key = rcgen::KeyPair::generate().unwrap();
        rcgen::CertificateParams::new(vec!["smeltr-test".to_string()])
            .unwrap()
            .self_signed(&key)
            .unwrap()
            .pem()
    }

    /// `example_json` with `on_source` added to the first Kafka source.
    fn with_tls(on_source: &str) -> FileConfig {
        let json = example_json().replacen(
            r#""groupId": "smeltr""#,
            &format!(r#""groupId": "smeltr"{on_source}"#),
            1,
        );
        serde_json::from_str(&json).unwrap()
    }

    /// The Kafka source speaking TLS, with `fields` of its own TLS settings.
    fn tls_source(fields: &str) -> String {
        format!(r#", "properties": {{"security.protocol": "SSL"}}{fields}"#)
    }

    /// The source's authorities, as a JSON field.
    fn trusting(pem: &str) -> String {
        format!(r#", "trustedAuthorities": {}"#, serde_json::to_string(pem).unwrap())
    }

    #[test]
    fn a_connector_naming_a_listed_certificate_is_accepted() {
        let errors = validation_errors(with_tls(&tls_source(r#", "certificate": "acme""#)));
        assert!(errors.is_empty(), "{errors:?}");
    }

    #[test]
    fn a_certificate_named_by_a_blank_name_is_refused() {
        let errors = validation_errors(with_tls(&tls_source(r#", "certificate": " ""#)));
        assert!(
            errors
                .iter()
                .any(|e| e.field == Some("certificate") && e.contains("blank name")),
            "{errors:?}"
        );
    }

    /// A trust setting with neither half is the one combination refused: every other says something about a
    /// connection.
    #[test]
    fn a_trust_setting_beside_neither_half_is_refused() {
        let errors = validation_errors(with_tls(&tls_source(r#", "trustPublic": true"#)));
        assert!(errors.iter().any(|e| e.contains("changes nothing")), "{errors:?}");
    }

    /// `CertificateDer`'s own PEM reader selects certificate sections rather than proving there is nothing else,
    /// so a certificate beside a CSR would pass silently through an iterator that only extracts certificates.
    #[test]
    fn a_certificate_beside_a_csr_is_refused() {
        let csr = "-----BEGIN CERTIFICATE REQUEST-----\nMIIBCjCBtAIBADAA\n-----END CERTIFICATE REQUEST-----\n";
        let errors = validation_errors(with_tls(&tls_source(&trusting(&format!("{}{csr}", a_certificate())))));
        assert!(errors.iter().any(|e| e.contains("Csr")), "{errors:?}");
    }

    #[test]
    fn trusting_no_public_root_and_no_authority_is_refused() {
        let errors = validation_errors(with_tls(&tls_source(
            r#", "trustPublic": false, "certificate": "acme""#,
        )));
        assert!(errors.iter().any(|e| e.contains("trusts no public root")), "{errors:?}");
    }

    #[test]
    fn trusting_what_is_not_a_certificate_is_refused() {
        let errors = validation_errors(with_tls(&tls_source(&trusting("not a certificate"))));
        assert!(
            errors.iter().any(|e| e.contains("not a certificate authority")),
            "{errors:?}"
        );
    }

    /// A `CERTIFICATE` section whose framing is valid PEM but whose DER is not a certificate: the label alone
    /// would pass this, and only the TLS handshake would catch it.
    #[test]
    fn trusting_a_certificate_labeled_section_of_unparseable_der_is_refused() {
        // base64 of "not actually DER" -- valid PEM framing around bytes that are not a certificate.
        let bogus = "-----BEGIN CERTIFICATE-----\nbm90IGFjdHVhbGx5IERFUg==\n-----END CERTIFICATE-----\n";
        let errors = validation_errors(with_tls(&tls_source(&trusting(bogus))));
        assert!(
            errors.iter().any(|e| e.contains("not a certificate authority")),
            "{errors:?}"
        );
    }

    /// A `CERTIFICATE` section that parses as a valid certificate followed by bytes that are not one: the DER
    /// parse alone would pass this, since it stops at the certificate's own end and never inspects what follows.
    #[test]
    fn trusting_a_certificate_followed_by_trailing_bytes_is_refused() {
        let key = rcgen::KeyPair::generate().unwrap();
        let cert = rcgen::CertificateParams::new(vec!["smeltr-test".to_string()])
            .unwrap()
            .self_signed(&key)
            .unwrap();
        let mut der = cert.der().to_vec();
        der.extend_from_slice(b"trailing");
        let bogus = format!(
            "-----BEGIN CERTIFICATE-----\n{}\n-----END CERTIFICATE-----\n",
            crate::format::bytes_text::BytesForm::Base64.text(&der)
        );
        let errors = validation_errors(with_tls(&tls_source(&trusting(&bogus))));
        assert!(
            errors.iter().any(|e| e.contains("not a certificate authority")),
            "{errors:?}"
        );
    }

    /// One place each answer can come from: the connector's own TLS settings carry the material, and a path names
    /// the same material another way.
    #[test]
    fn tls_settings_beside_a_path_to_the_same_material_are_refused() {
        for key in [
            "ssl.ca.location",
            "ssl.certificate.location",
            "ssl.key.location",
            "ssl.keystore.location",
        ] {
            let on_source = format!(
                r#", "properties": {{"security.protocol": "SSL", "{key}": "/etc/pki/acme.pem"}}, "certificate": "acme""#
            );
            let errors = validation_errors(with_tls(&on_source));
            assert!(
                errors.iter().any(|e| e.contains("one place it can come from")),
                "{key}: {errors:?}"
            );
        }
    }

    /// A passphrase on the key is what would make a certificate's key mean different things on two connectors.
    /// It is credential-valued, so it is written where a credential goes.
    #[test]
    fn tls_settings_beside_a_passphrase_for_the_key_are_refused() {
        let on_source = concat!(
            r#", "properties": {"security.protocol": "SSL"}"#,
            r#", "credentials": {"ssl.key.password": "KAFKA_KEY_PASSWORD"}"#,
            r#", "certificate": "acme""#
        );
        let errors = validation_errors(with_tls(on_source));
        assert!(
            errors.iter().any(|e| e.contains("one place it can come from")),
            "{errors:?}"
        );
    }

    /// Naming authorities names what a far end is verified against; a connector that also turns verification off
    /// is not doing that, on Kafka alone -- Elasticsearch has no such knob, so the authorities would verify on one
    /// connector and not the other.
    #[test]
    fn tls_settings_beside_disabled_certificate_verification_are_refused() {
        let on_source = format!(
            r#", "properties": {{"security.protocol": "SSL", "enable.ssl.certificate.verification": "false"}}{}"#,
            trusting(&a_certificate())
        );
        let errors = validation_errors(with_tls(&on_source));
        assert!(
            errors.iter().any(|e| e.contains("one place it can come from")),
            "{errors:?}"
        );
    }

    /// The chain can still check out under the connector's own authorities while this turns off whether the
    /// certificate is also checked against the broker's own name -- a different half of the same question
    /// `enable.ssl.certificate.verification` answers the other half of.
    #[test]
    fn tls_settings_beside_disabled_hostname_verification_are_refused() {
        let on_source = format!(
            r#", "properties": {{"security.protocol": "SSL", "ssl.endpoint.identification.algorithm": "none"}}{}"#,
            trusting(&a_certificate())
        );
        let errors = validation_errors(with_tls(&on_source));
        assert!(
            errors.iter().any(|e| e.contains("one place it can come from")),
            "{errors:?}"
        );
    }

    /// librdkafka defaults `security.protocol` to plaintext, so an unset one leaves the TLS settings unused.
    #[test]
    fn tls_settings_under_a_protocol_that_negotiates_no_tls_are_refused() {
        for on_source in [
            r#", "certificate": "acme""#,
            r#", "properties": {"security.protocol": "SASL_PLAINTEXT"}, "certificate": "acme""#,
        ] {
            let errors = validation_errors(with_tls(on_source));
            assert!(
                errors
                    .iter()
                    .any(|e| e.contains("does not negotiate TLS") || e.contains("defaults to plaintext")),
                "{on_source}: {errors:?}"
            );
        }
    }

    #[test]
    fn tls_settings_on_a_destination_reached_over_plain_http_are_refused() {
        let mut file: FileConfig = serde_json::from_str(example_json()).unwrap();
        file.destinations.push(serde_json::from_str(
            r#"{"id": "es", "type": "elasticsearch", "url": "http://es.internal:9200", "defaultIndex": "rows", "certificate": "acme"}"#,
        ).unwrap());
        let errors = validation_errors(file);
        assert!(
            errors.iter().any(|e| e.contains("does not negotiate TLS")),
            "{errors:?}"
        );
    }

    #[test]
    fn a_zone_url_is_checked_like_the_url() {
        for (zone_urls, expect) in [
            (r#"{" ": "https://es-a:9200"}"#, "names a zone by a blank name"),
            (r#"{"us-east-1a": " "}"#, "has no URL for the zone"),
            (r#"{"us-east-1a": "ftp://es-a"}"#, "has zoneUrls \"ftp://es-a\""),
        ] {
            let mut file: FileConfig = serde_json::from_str(example_json()).unwrap();
            file.destinations.push(
                serde_json::from_str(&format!(
                    r#"{{"id": "es", "type": "elasticsearch", "url": "https://es:9200", "defaultIndex": "rows",
                        "zoneUrls": {zone_urls}}}"#
                ))
                .unwrap(),
            );
            let errors = validation_errors(file);
            assert!(errors.iter().any(|e| e.contains(expect)), "{zone_urls}: {errors:?}");
        }
    }

    #[test]
    fn a_zone_attribute_wants_sniffing_and_a_name() {
        for (extra, expect) in [
            (r#""zoneAttribute": "rack""#, "does not sniff"),
            (r#""sniff": true, "zoneAttribute": " ""#, "blank zone attribute"),
        ] {
            let mut file: FileConfig = serde_json::from_str(example_json()).unwrap();
            file.destinations.push(
                serde_json::from_str(&format!(
                    r#"{{"id": "es", "type": "elasticsearch", "url": "https://es:9200", "defaultIndex": "rows", {extra}}}"#
                ))
                .unwrap(),
            );
            let errors = validation_errors(file);
            assert!(errors.iter().any(|e| e.contains(expect)), "{extra}: {errors:?}");
        }
    }

    /// Sniffing prefers the zone the worker is in, and names it by `zone` unless told otherwise.
    #[test]
    fn a_sniffing_worker_takes_its_zone() {
        let mut file: FileConfig = serde_json::from_str(example_json()).unwrap();
        file.destinations.push(
            serde_json::from_str(
                r#"{"id": "es", "type": "elasticsearch", "url": "https://es:9200", "defaultIndex": "rows", "sniff": true}"#,
            )
            .unwrap(),
        );
        let loaded = load_str(&serde_json::to_string(&file).unwrap()).expect("sniffing loads");
        let mut kind = loaded
            .topology
            .destinations
            .iter()
            .find(|d| d.name == "es")
            .unwrap()
            .kind
            .clone();
        kind.take_zone(Some("us-east-1a"));
        let crate::plan::topology::DestinationKind::Elasticsearch(c) = kind else {
            unreachable!()
        };
        assert_eq!(
            c.sniff,
            Some(crate::plan::topology::Sniff {
                zone_attribute: "zone".into(),
                zone: Some("us-east-1a".into())
            })
        );
    }

    /// A worker sends to its own zone's address, and to `url` when its zone has none or it has no zone.
    #[test]
    fn a_worker_takes_its_own_zones_url() {
        let mut file: FileConfig = serde_json::from_str(example_json()).unwrap();
        file.destinations.push(
            serde_json::from_str(
                r#"{"id": "es", "type": "elasticsearch", "url": "https://es:9200", "defaultIndex": "rows",
                    "zoneUrls": {"us-east-1a": "https://es-a:9200", " us-east-1b ": "https://es-b:9200"}}"#,
            )
            .unwrap(),
        );
        let loaded = load_str(&serde_json::to_string(&file).unwrap()).expect("zone URLs load");
        let es = loaded.topology.destinations.iter().find(|d| d.name == "es").unwrap();
        for (zone, expect) in [
            (Some("us-east-1b"), "https://es-b:9200"),
            (Some("eu-west-1a"), "https://es:9200"),
            (None, "https://es:9200"),
        ] {
            let mut kind = es.kind.clone();
            kind.take_zone(zone);
            let crate::plan::topology::DestinationKind::Elasticsearch(c) = kind else {
                unreachable!()
            };
            assert_eq!((c.url.as_str(), c.zone_urls.len()), (expect, 0), "{zone:?}");
        }
    }

    /// A connector carries its TLS settings, which is what lets a client be built from the topology alone.
    #[test]
    fn a_connector_carries_its_tls_settings() {
        let certificate = a_certificate();
        let on_source = tls_source(&format!(r#", "certificate": "acme"{}"#, trusting(&certificate)));
        let loaded = load_str(&serde_json::to_string(&with_tls(&on_source)).unwrap()).expect("a source speaking TLS");
        let kafka = loaded
            .topology
            .sources
            .iter()
            .find_map(|s| match &s.kind {
                crate::plan::topology::SourceKind::Kafka(k) => Some(k),
                _ => None,
            })
            .expect("a kafka source");
        let carried = kafka.tls.as_ref().expect("the TLS it speaks");
        assert_eq!(carried.of, "source \"kafka-main\"");
        assert_eq!(carried.certificate.as_deref(), Some("acme"));
        assert_eq!(carried.authorities.as_deref(), Some(certificate.as_str()));
        assert!(carried.trust_public, "absent is trusting the public roots");
        assert_eq!(
            carried.side,
            crate::io::trust::Side::Client,
            "a Kafka source connects out"
        );
    }

    /// A Kafka source without `start` reads from the earliest. Every partition's commit carries its start, so a
    /// release that changed this default would move every such source on its next deploy: changing it needs a
    /// converter that writes the old default into each file (`CLAUDE.md`, Stored formats).
    #[test]
    fn a_kafka_source_without_a_start_reads_from_the_earliest() {
        let loaded = load_str(example_json()).expect("the example loads");
        let kafka = loaded
            .topology
            .sources
            .iter()
            .find_map(|s| match &s.kind {
                crate::plan::topology::SourceKind::Kafka(k) => Some(k),
                _ => None,
            })
            .expect("a kafka source");
        assert_eq!(kafka.start, StartPosition::Earliest);
    }

    /// A certificate a worker cannot obtain has to refuse the deploy before the running one is stopped, which is
    /// what `named_certificates` is asked, as `named_secrets` is for a SASL password.
    #[test]
    fn a_certificate_is_one_the_bundle_names() {
        let on_source = tls_source(r#", "certificate": "acme""#);
        let loaded = load_str(&serde_json::to_string(&with_tls(&on_source)).unwrap()).expect("a source naming one");
        assert_eq!(
            loaded.topology.named_certificates,
            vec![crate::plan::topology::NamedCertificate {
                name: "acme".to_string(),
                what: "source \"kafka-main\"".to_string(),
            }]
        );
    }

    /// TLS, SCRAM, and the password by name: what `ssl-vendored` buys.
    #[test]
    fn a_secured_broker_configuration_is_accepted() {
        let json = example_json().replacen(
            r#""groupId": "smeltr""#,
            r#""groupId": "smeltr", "properties": {"security.protocol": "SASL_SSL", "sasl.mechanism": "SCRAM-SHA-512", "sasl.username": "smeltr", "ssl.ca.location": "/etc/ssl/certs/ca.pem"}, "credentials": {"sasl.password": "KAFKA_PASSWORD"}"#,
            1,
        );
        let file: FileConfig = serde_json::from_str(&json).unwrap();
        assert!(validation_errors(file).is_empty());
    }

    /// librdkafka refuses to build a PLAIN or SCRAM client missing either half of its login, so the save does.
    #[test]
    fn a_plain_or_scram_login_needs_its_username_and_password() {
        let errors = |properties: &str, credentials: &str| {
            let json = example_json().replacen(
                r#""groupId": "smeltr""#,
                &format!(r#""groupId": "smeltr", "properties": {properties}, "credentials": {credentials}"#),
                1,
            );
            validation_errors(serde_json::from_str(&json).unwrap())
                .into_iter()
                .map(|e| e.message)
                .collect::<Vec<_>>()
        };
        let plain = r#"{"security.protocol": "SASL_SSL", "sasl.mechanism": "PLAIN", "sasl.username": "u"}"#;
        let missing = errors(plain, "{}");
        assert!(
            missing.len() == 1 && missing[0].contains("\"sasl.password\""),
            "{missing:?}"
        );
        assert_eq!(
            errors(plain, r#"{"sasl.password": "KAFKA_PASSWORD"}"#),
            Vec::<String>::new()
        );
        let scram = r#"{"security.protocol": "SASL_SSL", "sasl.mechanisms": "SCRAM-SHA-256"}"#;
        assert_eq!(errors(scram, r#"{"sasl.password": "KAFKA_PASSWORD"}"#).len(), 1);
        // Outside SASL, and for a mechanism with no password, there is nothing to ask for.
        assert_eq!(errors(r#"{"sasl.mechanism": "PLAIN"}"#, "{}"), Vec::<String>::new());
        let oauth = r#"{"security.protocol": "SASL_SSL", "sasl.mechanism": "OAUTHBEARER"}"#;
        assert_eq!(errors(oauth, "{}"), Vec::<String>::new());
        let lower = r#"{"security.protocol": "SASL_SSL", "sasl.mechanism": "scram-sha-256", "sasl.username": "u"}"#;
        let refused = errors(lower, r#"{"sasl.password": "KAFKA_PASSWORD"}"#);
        assert!(
            refused.len() == 1 && refused[0].contains("\"SCRAM-SHA-256\""),
            "{refused:?}"
        );
        let both = r#"{"security.protocol": "SASL_SSL", "sasl.mechanism": "OAUTHBEARER", "sasl.mechanisms": "PLAIN"}"#;
        assert_eq!(errors(both, "{}").len(), 2);
        let hidden = r#"{"sasl.mechanism": "PLAIN"}"#;
        let login = |e: Vec<String>| e.iter().filter(|m| m.contains("signs in with SASL")).count();
        assert_eq!(login(errors(hidden, r#"{"security.protocol": "KAFKA_PROTOCOL"}"#)), 2);
    }

    /// Every codec the GUI offers is one this build produces.
    #[test]
    fn every_offered_compression_codec_is_supported_by_this_build() {
        for codec in [
            CompressionDto::None,
            CompressionDto::Gzip,
            CompressionDto::Snappy,
            CompressionDto::Lz4,
            CompressionDto::Zstd,
        ] {
            let props = crate::plan::topology::KafkaProps {
                literal: vec![("compression.type".into(), codec.to_plan(None).as_property().into())],
                credentials: Vec::new(),
            };
            let mut errors = Vec::new();
            crate::io::kafka::validate_literal_props(&props, "test", &mut errors);
            assert!(
                errors.is_empty(),
                "{} is offered but this build refuses it: {errors:?}",
                codec.to_plan(None).as_property()
            );
        }
    }

    #[test]
    fn a_misspelled_property_is_refused() {
        let json = example_json().replacen(
            r#""groupId": "smeltr""#,
            r#""groupId": "smeltr", "properties": {"sasl.mechanismm": "PLAIN"}"#,
            1,
        );
        let file: FileConfig = serde_json::from_str(&json).unwrap();
        let errors = validation_errors(file);
        assert!(errors.iter().any(|e| e.contains("sasl.mechanismm")), "{errors:?}");
    }

    #[test]
    fn an_out_of_range_property_value_is_refused() {
        let json = example_json().replacen(
            r#""groupId": "smeltr""#,
            r#""groupId": "smeltr", "properties": {"socket.timeout.ms": "3"}"#,
            1,
        );
        let file: FileConfig = serde_json::from_str(&json).unwrap();
        let errors = validation_errors(file);
        assert!(errors.iter().any(|e| e.contains("socket.timeout.ms")), "{errors:?}");
    }

    #[test]
    fn a_destination_with_no_address_is_a_load_error() {
        // The first `"brokers": "b1:9092"` in the fixture is a destination's.
        let json = example_json().replacen(r#""brokers": "b1:9092""#, r#""brokers": """#, 1);
        let file: FileConfig = serde_json::from_str(&json).unwrap();
        let errors = validation_errors(file);
        assert!(errors.iter().any(|e| e.contains("has no brokers")), "{errors:?}");
    }

    /// The fixture's Kafka source, again under `id`, with `edit` applied to the copy's JSON.
    fn with_second_reader(id: &str, edit: impl Fn(&mut serde_json::Value)) -> Vec<String> {
        let mut json: serde_json::Value = serde_json::from_str(example_json()).unwrap();
        let sources = json["sources"].as_array_mut().unwrap();
        let mut copy = sources.iter().find(|s| s["type"] == "kafka").unwrap().clone();
        copy["id"] = id.into();
        edit(&mut copy);
        sources.push(copy);
        let file: FileConfig = serde_json::from_value(json).unwrap();
        validation_errors(file).into_iter().map(|e| e.to_string()).collect()
    }

    #[test]
    fn two_sources_reading_one_topic_in_one_group_are_refused() {
        // The same cluster listed another way is still the same cluster.
        // Another list of the same cluster, sharing one broker, is still the same cluster.
        let errors = with_second_reader("again", |s| s["brokers"] = " B2:9092, b3:9092".into());
        assert!(
            errors
                .iter()
                .any(|e| e.contains("source \"again\" reads \"logs-raw\" in consumer group")),
            "{errors:?}"
        );
    }

    #[test]
    fn a_second_reader_of_a_topic_under_its_own_group_is_accepted() {
        let errors = with_second_reader("again", |s| s["groupId"] = "smeltr.replay".into());
        assert!(!errors.iter().any(|e| e.contains("would split")), "{errors:?}");
        let errors = with_second_reader("again", |s| s["brokers"] = "other:9092".into());
        assert!(!errors.iter().any(|e| e.contains("would split")), "{errors:?}");
    }

    #[test]
    fn a_whitespace_only_required_field_counts_as_empty() {
        let json = example_json().replacen(r#""brokers": "b1:9092,b2:9092""#, r#""brokers": " ""#, 1);
        let file: FileConfig = serde_json::from_str(&json).unwrap();
        let errors = validation_errors(file);
        assert!(errors.iter().any(|e| e.contains("has no brokers")), "{errors:?}");
    }

    #[test]
    fn an_eval_assignment_needs_a_target() {
        let json = example_json().replacen(r#""target": "severity""#, r#""target": """#, 1);
        let file: FileConfig = serde_json::from_str(&json).unwrap();
        let errors = validation_errors(file);
        assert!(errors.iter().any(|e| e.contains("has no target field")), "{errors:?}");
        assert!(
            errors.iter().any(|e| e.contains("severity")),
            "the message names the step it is in: {errors:?}"
        );
        // Attributed, not just named in the text: the editor puts this on the step's own card
        // rather than a list nothing on the page points back to.
        let attributed = errors.iter().find(|e| e.contains("has no target field")).unwrap();
        assert_eq!(attributed.pipeline.as_deref(), Some("main"));
        assert_eq!(attributed.step.as_deref(), Some("severity"));
        assert_eq!(attributed.assignment_index, Some(0));
        assert_eq!(attributed.field, Some("target"));
    }

    #[test]
    fn an_eval_assignment_needs_an_expression() {
        let json = example_json().replacen(r#""expr": "status >= 400 ? \"high\" : \"low\"""#, r#""expr": """#, 1);
        let file: FileConfig = serde_json::from_str(&json).unwrap();
        let errors = validation_errors(file);
        assert!(errors.iter().any(|e| e.contains("has no expression")), "{errors:?}");
        // The parser would otherwise report a bare `parse error in ``', which names neither the
        // step it came from nor the field that was left blank.
        assert!(
            !errors.iter().any(|e| e.contains("parse error in")),
            "the blank expression is caught before the parser sees it: {errors:?}"
        );
        let attributed = errors.iter().find(|e| e.contains("has no expression")).unwrap();
        assert_eq!(attributed.pipeline.as_deref(), Some("main"));
        assert_eq!(attributed.step.as_deref(), Some("severity"));
        assert_eq!(attributed.assignment_index, Some(0));
        assert_eq!(attributed.field, Some("expr"));
    }

    /// A topic pattern that does not compile is refused at load, on its source's topics; an event hub is named in full.
    #[test]
    fn a_topic_pattern_must_compile_and_an_event_hub_is_no_pattern() {
        let mut file: FileConfig = serde_json::from_str(example_json()).unwrap();
        file.sources.push(
            serde_json::from_value(serde_json::json!({
                "id": "bad", "type": "kafka", "brokers": "b:9092", "topics": ["^orders-("], "groupId": "g"
            }))
            .unwrap(),
        );
        file.sources.push(
            serde_json::from_value(serde_json::json!({
                "id": "good", "type": "kafka", "brokers": "b:9092", "topics": ["^orders-.*"], "groupId": "g"
            }))
            .unwrap(),
        );
        file.sources.push(
            serde_json::from_value(serde_json::json!({
                "id": "hubs", "type": "eventHubs", "namespace": "acme", "eventHubs": ["^logs-.*"],
                "consumerGroup": "g", "connectionString": "EVENTHUBS_CONNECTION_STRING"
            }))
            .unwrap(),
        );
        let errors = validation_errors(file);
        let on = |id: &str, field: &str| {
            errors
                .iter()
                .filter(|e| e.item.as_ref().is_some_and(|i| i.id == id) && e.field == Some(field))
                .map(|e| e.message.clone())
                .collect::<Vec<_>>()
        };
        assert!(
            on("bad", "topics").iter().any(|m| m.contains("does not compile")),
            "{errors:?}"
        );
        assert!(on("good", "topics").is_empty(), "{errors:?}");
        assert!(
            on("hubs", "eventHubs").iter().any(|m| m.contains("named in full")),
            "{errors:?}"
        );
    }

    /// The item and field an error names are what the editor places it by, so each check names the setting the
    /// fix goes in, by the name the file gives it.
    #[test]
    fn a_connector_check_names_its_item_and_field() {
        let mut file: FileConfig = serde_json::from_str(example_json()).unwrap();
        file.sources.push(
            serde_json::from_value(serde_json::json!({
                "id": "blank", "type": "kafka", "brokers": "", "topics": [], "groupId": ""
            }))
            .unwrap(),
        );
        file.destinations.push(
            serde_json::from_value(serde_json::json!({
                "id": "blank", "type": "kafka", "brokers": "b:9092", "defaultTopic": "",
                "properties": { "bootstrap.servers": "x" }
            }))
            .unwrap(),
        );
        let errors = validation_errors(file);
        let placed = |section: &str, field: &str| {
            errors.iter().any(|e| {
                e.item.as_ref().is_some_and(|i| i.section == section && i.id == "blank") && e.field == Some(field)
            })
        };
        for field in ["brokers", "topics"] {
            assert!(placed("sources", field), "{field}: {errors:?}");
        }
        for field in ["defaultTopic", "properties"] {
            assert!(placed("destinations", field), "{field}: {errors:?}");
        }
    }

    /// A credential check lands on the field holding the credential.
    #[test]
    fn a_credential_check_names_its_field() {
        let mut file: FileConfig = serde_json::from_str(example_json()).unwrap();
        file.destinations.push(
            serde_json::from_value(serde_json::json!({
                "id": "es", "type": "elasticsearch", "url": "http://es.example:9200",
                "defaultIndex": "logs", "auth": " "
            }))
            .unwrap(),
        );
        let errors = validation_errors(file);
        let on = |field: &str, says: &str| {
            errors.iter().any(|e| {
                e.item
                    .as_ref()
                    .is_some_and(|i| i.section == "destinations" && i.id == "es")
                    && e.field == Some(field)
                    && e.message.contains(says)
            })
        };
        assert!(on("auth", "blank name"), "{errors:?}");
    }

    /// The example bundle with one Delta destination, `lake`, holding `fields` beside its id and type.
    fn with_delta(fields: serde_json::Value) -> FileConfig {
        let mut file: FileConfig = serde_json::from_str(example_json()).unwrap();
        let mut dest = serde_json::json!({ "id": "lake", "type": "delta" });
        dest.as_object_mut()
            .unwrap()
            .extend(fields.as_object().unwrap().clone());
        file.destinations.push(serde_json::from_value(dest).unwrap());
        file
    }

    /// The refusals at `field` of the Delta destination `lake`.
    fn delta_refusals(fields: serde_json::Value, field: &str) -> Vec<String> {
        validation_errors(with_delta(fields))
            .into_iter()
            .filter(|e| {
                e.item
                    .as_ref()
                    .is_some_and(|i| i.section == "destinations" && i.id == "lake")
                    && e.field == Some(field)
            })
            .map(|e| e.message)
            .collect()
    }

    /// A Delta destination needs only its location: it signs with the host's credentials, partitions a created table
    /// by nothing, and compresses its Parquet with zstd.
    #[test]
    fn a_minimal_delta_destination_loads_with_the_defaults() {
        let json =
            serde_json::to_string(&with_delta(serde_json::json!({ "location": "s3://lake/tables/web" }))).unwrap();
        let system = load_str(&json).unwrap_or_else(|e| panic!("{e}"));
        let dest = system
            .topology
            .destinations
            .iter()
            .find(|d| d.name == "lake")
            .expect("the destination loads");
        let DestinationKind::Delta(c) = &dest.kind else {
            panic!("expected a Delta destination, got {:?}", dest.kind);
        };
        assert_eq!(c.location, "s3://lake/tables/web");
        assert!(c.partition_by.is_empty() && c.tls.is_none());
        assert!(c.storage.region.is_none() && c.storage.endpoint.is_none());
        assert!(c.storage.access_key.is_none() && c.storage.role.is_none());
        assert_eq!(c.compression, crate::plan::topology::Compression::Zstd(3));
        assert_eq!(c.in_flight, InFlight::default().0);
        assert_eq!(c.max_object_bytes, ObjectBytes::default().0);
        assert_eq!(c.buffer_bytes, BufferBytes::OBJECT_DEFAULT);
        assert_eq!(c.stall_after, std::time::Duration::from_secs(StallAfter::default().0));
        assert_eq!(
            c.file_interval,
            std::time::Duration::from_secs(FileInterval::default().0)
        );
    }

    #[test]
    fn a_delta_location_outside_s3_is_refused_at_location() {
        for bad in [
            "",
            "lake/tables/web",
            "https://lake/tables/web",
            "s3://",
            "s3:///tables/web",
        ] {
            let refused = delta_refusals(serde_json::json!({ "location": bad }), "location");
            assert!(
                refused.iter().any(|m| m.contains("s3://bucket/path")),
                "{bad:?}: {refused:?}"
            );
        }
        for fine in ["s3://lake/tables/web", "s3a://lake/tables/web", "s3://lake"] {
            let refused = delta_refusals(serde_json::json!({ "location": fine }), "location");
            assert!(refused.is_empty(), "{fine:?}: {refused:?}");
        }
    }

    /// Delta partitions by top-level columns, by their values: a nested field or a transform is no column.
    #[test]
    fn a_delta_partition_that_is_not_a_column_name_is_refused() {
        for bad in ["http.host", "day(ts)", ""] {
            let refused = delta_refusals(
                serde_json::json!({ "location": "s3://lake/web", "partitionBy": [bad] }),
                "partitionBy",
            );
            assert!(
                refused.iter().any(|m| m.contains(&format!("{bad:?}"))),
                "{bad:?}: {refused:?}"
            );
        }
    }

    #[test]
    fn a_delta_partition_named_twice_is_refused() {
        let refused = delta_refusals(
            serde_json::json!({ "location": "s3://lake/web", "partitionBy": ["host", "day", "host"] }),
            "partitionBy",
        );
        assert_eq!(refused.len(), 1, "{refused:?}");
        assert!(refused[0].contains("\"host\" twice"), "{refused:?}");
    }

    /// A Unity Catalog table's fields as one that loads, with `change` laid over them.
    fn unity_catalog(change: serde_json::Value) -> serde_json::Value {
        let mut uc = serde_json::json!({
            "workspace": "https://dbc-1234.cloud.databricks.com",
            "table": "main.logs.web",
            "clientId": "6f1c2a3b",
            "clientSecret": "DATABRICKS_CLIENT_SECRET",
        });
        uc.as_object_mut().unwrap().extend(change.as_object().unwrap().clone());
        uc
    }

    #[test]
    fn a_delta_table_named_both_ways_or_neither_is_refused() {
        let both = delta_refusals(
            serde_json::json!({ "location": "s3://lake/web", "unityCatalog": unity_catalog(serde_json::json!({})) }),
            "location",
        );
        assert!(both.iter().any(|m| m.contains("not both")), "{both:?}");
        let neither = delta_refusals(serde_json::json!({}), "location");
        assert!(neither.iter().any(|m| m.contains("names no table")), "{neither:?}");
    }

    #[test]
    fn a_unity_catalog_workspace_that_is_no_root_address_is_refused() {
        for (bad, says) in [
            ("dbc-1234.cloud.databricks.com", "nothing after its host"),
            ("", "nothing after its host"),
            (
                "https://dbc-1234.cloud.databricks.com/?o=1234567890",
                "nothing after its host",
            ),
            ("https://dbc-1234.cloud.databricks.com/browse", "nothing after its host"),
        ] {
            let refused = delta_refusals(
                serde_json::json!({ "unityCatalog": unity_catalog(serde_json::json!({ "workspace": bad })) }),
                "unityCatalog.workspace",
            );
            assert!(refused.iter().any(|m| m.contains(says)), "{bad:?}: {refused:?}");
        }
        let local = delta_refusals(
            serde_json::json!({ "unityCatalog": unity_catalog(serde_json::json!({ "workspace": "http://localhost:8080" })) }),
            "unityCatalog.workspace",
        );
        assert!(local.is_empty(), "{local:?}");
    }

    #[test]
    fn a_unity_catalog_table_needs_the_region_unity_catalog_does_not_give() {
        let refused = delta_refusals(
            serde_json::json!({ "unityCatalog": unity_catalog(serde_json::json!({})) }),
            "region",
        );
        assert!(
            refused.iter().any(|m| m.contains("name the bucket's region")),
            "{refused:?}"
        );
    }

    #[test]
    fn a_unity_catalog_table_not_in_three_parts_is_refused() {
        for bad in ["web", "logs.web", "main.logs.web.v2", "main..web", "", "main.logs."] {
            let refused = delta_refusals(
                serde_json::json!({ "unityCatalog": unity_catalog(serde_json::json!({ "table": bad })) }),
                "unityCatalog.table",
            );
            assert!(
                refused.iter().any(|m| m.contains("catalog.schema.table")),
                "{bad:?}: {refused:?}"
            );
        }
        let blank_id = delta_refusals(
            serde_json::json!({ "unityCatalog": unity_catalog(serde_json::json!({ "clientId": " " })) }),
            "unityCatalog.clientId",
        );
        assert!(blank_id.iter().any(|m| m.contains("names no clientId")), "{blank_id:?}");
    }

    /// Unity Catalog vends the storage credentials, so an access key or role beside it would go unused. Where the
    /// bucket is stays the operator's to say.
    #[test]
    fn storage_credentials_beside_a_unity_catalog_table_are_refused() {
        let uc = unity_catalog(serde_json::json!({}));
        for (field, value) in [
            ("accessKeyId", "AWS_ACCESS_KEY_ID"),
            ("secretAccessKey", "AWS_SECRET_ACCESS_KEY"),
            ("roleArn", "arn:aws:iam::111122223333:role/smeltr"),
            ("authEndpoint", "https://sts.us-west-2.amazonaws.com"),
        ] {
            let refused = delta_refusals(serde_json::json!({ "unityCatalog": uc, field: value }), field);
            assert!(
                refused.iter().any(|m| m.contains("beside a unityCatalog table")),
                "{field}: {refused:?}"
            );
        }
        let errors = validation_errors(with_delta(serde_json::json!({
            "unityCatalog": uc, "region": "us-west-2", "endpoint": "https://s3.us-west-2.amazonaws.com",
        })));
        assert!(
            !errors.iter().any(|e| e
                .item
                .as_ref()
                .is_some_and(|i| i.section == "destinations" && i.id == "lake")),
            "{errors:?}"
        );
    }

    #[test]
    fn a_unity_catalog_table_loads_with_its_secret() {
        let json = serde_json::to_string(&with_delta(serde_json::json!({
            "unityCatalog": unity_catalog(serde_json::json!({ "table": " main.logs.web " })),
            "region": "us-west-2",
        })))
        .unwrap();
        let system = load_str(&json).unwrap_or_else(|e| panic!("{e}"));
        let dest = (system.topology.destinations.iter())
            .find(|d| d.name == "lake")
            .expect("the destination loads");
        let DestinationKind::Delta(c) = &dest.kind else {
            panic!("expected a Delta destination, got {:?}", dest.kind);
        };
        assert_eq!(c.location, "");
        let uc = c.unity_catalog.as_ref().expect("a Unity Catalog table");
        assert_eq!(uc.workspace, "https://dbc-1234.cloud.databricks.com");
        assert_eq!(uc.table, "main.logs.web");
        assert_eq!(uc.client_id, "6f1c2a3b");
        let secret = crate::plan::topology::SecretRef::WorkerLocal("DATABRICKS_CLIENT_SECRET".to_string());
        assert_eq!(uc.client_secret, secret);
        assert_eq!(c.storage.region.as_deref(), Some("us-west-2"));
        let named: Vec<_> = (system.topology.named_secrets.iter())
            .map(|n| (n.reference.clone(), n.what.as_str()))
            .collect();
        assert_eq!(
            named,
            vec![(secret, r#"destination "lake": unityCatalog.clientSecret"#)]
        );
    }

    /// Steps nest, and a blank field inside an `if` is as broken as one outside it.
    #[test]
    fn a_blank_assignment_is_found_inside_an_if() {
        let json = example_json().replacen(r#""target": "alert""#, r#""target": " ""#, 1);
        let file: FileConfig = serde_json::from_str(&json).unwrap();
        let errors = validation_errors(file);
        assert!(
            errors
                .iter()
                .any(|e| e.contains("tag-high") && e.contains("has no target field")),
            "{errors:?}"
        );
        let attributed = errors.iter().find(|e| e.contains("has no target field")).unwrap();
        assert_eq!(
            attributed.step.as_deref(),
            Some("tag-high"),
            "attribution reaches inside the if, not just the message text: {attributed:?}"
        );
    }

    /// Emptiness, not reachability.
    #[test]
    fn an_unreachable_address_is_not_a_load_error() {
        let json = example_json().replacen(
            r#""brokers": "b1:9092,b2:9092""#,
            r#""brokers": "nothing-is-listening-here:9092""#,
            1,
        );
        let file: FileConfig = serde_json::from_str(&json).unwrap();
        assert!(validation_errors(file).is_empty());
    }
}
