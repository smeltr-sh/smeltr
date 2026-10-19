//! DTO -> `plan::config`/`plan::topology`: id resolution and structural validation (every reference resolves, every id
//! is unique in its scope, every field that must not be blank is not, every bound holds). Expression text passes
//! through verbatim; `LoadedSystem::compile_topology` checks it with the compiler's own errors.
//!
//! Every top-level item that fails to convert contributes one error and is otherwise skipped, so one bad route does not
//! hide an unrelated bad pipeline.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::Duration;

use std::path::PathBuf;

use crate::core::column::DataType;
use crate::core::error::{Category, ExceptionKind};
use crate::core::id::{DestId, FunctionId, SourceId};
use crate::format::encoding::Encoding;
use crate::io::entra::IMDS_API_VERSION;
use crate::io::line_breaker::{LineBreaker, PER_LINE};
use crate::io::logs::LogsSourceConfig;
use crate::metrics::MetricsSourceConfig;
use crate::ops::timeparse;
use crate::plan::config::{
    Answer, CsvConfig, DeclaredField, FunctionConfig, FunctionKind, GrokConfig, KvConfig, LookupAt, LookupFile,
    LookupOutput, LookupSource, MatchKind, Multiplicity, OnException, ParserKind, Payload, PipelineConfig, Step,
    TimeSource,
};
use crate::plan::parse_call::split_dots_default;
use crate::plan::topology::{
    AccessKey, Acknowledgement, AfterCommit, ApiRecords, AssumedRole, AzureAccess, AzureCredential, AzureStore,
    CatalogSignIn, Cloud, DeliverRule, DeltaDestinationConfig, DestinationConfig, DestinationKind, DocIdPolicy,
    Endpoint, EntraSignIn, Framing, GcsAccess, GcsStore, HttpDestinationConfig, IcebergDestinationConfig, Identity,
    KafkaDestinationConfig, KafkaProps, KafkaService, KafkaSourceConfig, ListPlace, ListStart,
    LogAnalyticsDestinationConfig, NamedSecret, Newlines, ObjectFormat, ObjectQueue, ObjectSourceConfig,
    ObjectsDestinationConfig, OktaSignIn, PrometheusDestinationConfig, RemoteWriteVersion, RequestBody, RouteConfig,
    S3Access, S3Store, SftpAccess, SftpCredential, SftpStore, SourceConfig, SourceKind, SplunkHecDestinationConfig,
    StartPosition, StopPosition, TopologyConfig, Transport, UnityCatalogTable, identity_secret,
};

use super::CURRENT_VERSION;
use super::bytes;
use super::dto::*;

/// Structural resolution only: every reference resolves, every id is unique, every bound holds. Expression text
/// passes through verbatim and is never compiled here, so a pipeline whose expressions don't compile still loads —
/// on purpose, so `/api/preview` can list which pipeline that is rather than refuse the whole file. Reach for
/// [`super::validate`] instead when the question is "does this bundle actually run": that is what compiles every
/// pipeline and is what every save path goes through.
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

/// One problem `validate` found, attributed to where it came from when it is known. A compile error carries the
/// pipeline and function step whose expression failed. A check on one source, destination or identity carries that
/// item and, where one setting is wrong, its field. A problem between items carries no attribution.
#[derive(Clone, Debug, PartialEq)]
pub struct ValidationError {
    pub pipeline: Option<String>,
    pub function: Option<u32>,
    /// `function`, resolved to the step's own string id through that pipeline's `step_ids`. Stable across a `call`
    /// step's inlining, where `function` alone is not.
    pub step: Option<String>,
    /// The eval assignment, by its position in the step's own list. See `CompileError::assignment_index`.
    pub assignment_index: Option<u32>,
    /// The source, destination or identity this is about.
    pub item: Option<ItemRef>,
    /// The setting this is about, by its name in the file: a key of `item`, or of the assignment. A key inside a map
    /// follows the map's name after a dot, as in `properties.security.protocol`.
    pub field: Option<&'static str>,
    /// The route's delivery rule this is about, by its position in the route's `deliver`. See `CompileError::rule`.
    pub rule: Option<u32>,
    pub message: String,
}

impl From<String> for ValidationError {
    fn from(message: String) -> Self {
        ValidationError {
            pipeline: None,
            function: None,
            step: None,
            assignment_index: None,
            item: None,
            field: None,
            rule: None,
            message,
        }
    }
}

/// One item of a bundle section that is a list: the section's name in the file, and the item's id.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct ItemRef {
    pub section: &'static str,
    pub id: String,
}

/// Where the checks on one item put what they find, so each error names the item and, through `at`, the field.
struct ItemErrors<'a> {
    errors: &'a mut Vec<ValidationError>,
    item: ItemRef,
    field: Option<&'static str>,
}

impl<'a> ItemErrors<'a> {
    fn new(errors: &'a mut Vec<ValidationError>, section: &'static str, id: &str) -> Self {
        ItemErrors {
            errors,
            item: ItemRef {
                section,
                id: id.to_string(),
            },
            field: None,
        }
    }

    /// A problem with the item as a whole, or after `at`, with one setting.
    fn push(&mut self, message: String) {
        self.errors.push(ValidationError {
            item: Some(self.item.clone()),
            field: self.field,
            ..message.into()
        });
    }

    /// The same item's errors, about one of its settings.
    fn at(&mut self, field: &'static str) -> ItemErrors<'_> {
        ItemErrors {
            errors: self.errors,
            item: self.item.clone(),
            field: Some(field),
        }
    }
}

impl std::fmt::Display for ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl ValidationError {
    /// Reads the message text alone, so callers that only ever cared what the problem said do not have to learn
    /// about attribution to keep asking.
    pub fn contains(&self, needle: &str) -> bool {
        self.message.contains(needle)
    }
}

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
    check_unique_ids(file.identities.iter().map(|i| &i.id), "identity", &mut id_errors);
    required_fields(&file, &mut attributed_errors);
    // Checked here rather than where a grok names one: a pattern no step uses yet is still the bundle's, and the
    // first step to name it should not be the one to find it broken.
    for (id, e) in crate::format::grok_scan::Matcher::check_shared(&maps.grok_patterns) {
        ItemErrors::new(&mut attributed_errors, "grokPatterns", &id)
            .at("pattern")
            .push(e.to_string());
    }

    // References resolved against a map built from duplicate ids would only add secondary errors on top of the real
    // one.
    if !id_errors.is_empty() || !attributed_errors.is_empty() {
        return Err(LoadError::Validation(
            id_errors.into_iter().map(Into::into).chain(attributed_errors).collect(),
        ));
    }

    // Every name resolves and every id is unique: `required_fields` refused the bundle otherwise, so a connector's
    // identity is a lookup that cannot miss.
    let identities: Identities = file.identities.iter().map(|i| (i.id.clone(), to_identity(i))).collect();
    let sources = file
        .sources
        .iter()
        .map(|s| to_source(s, maps.sources[&s.id], &identities))
        .collect();
    let mut errors = Vec::new();
    let mut group = OnException::default();
    if let Some(layer) = &file.on_exception {
        lay_answers(&mut group, layer, Layer::Group, &maps).unwrap_or_else(|e| errors.push(format!("the group's {e}")));
    }
    let mut destinations: Vec<DestinationConfig> = file
        .destinations
        .iter()
        .map(|d| to_destination(d, maps.destinations[&d.id], &identities))
        .collect();
    for (d, dto) in destinations.iter_mut().zip(&file.destinations) {
        let mut own = group.clone();
        if let Some(layer) = &dto.on_exception {
            lay_answers(&mut own, layer, Layer::Destination, &maps)
                .unwrap_or_else(|e| errors.push(format!("destination {:?}'s {e}", dto.id)));
        }
        d.on_reject = own.answer(&ExceptionKind::Refused).clone();
    }

    let (routes, route_errors) = collect(file.routes.iter().map(|r| to_route(r, &maps, &group)));
    errors.extend(route_errors);
    let (pipelines, pipeline_errors) = collect(file.pipelines.iter().map(|p| {
        let mut call_ids = Vec::new();
        let expanded = expand_calls(
            &p.steps,
            &maps.pipeline_dtos,
            &mut vec![p.id.clone()],
            &mut call_ids,
            &mut HashSet::new(),
        )
        .map_err(|e| format!("pipeline {:?}: {e}", p.name))?;
        to_pipeline(p, &expanded, &call_ids, maps.pipelines[&p.id], &maps)
    }));
    errors.extend(pipeline_errors);

    // Only once every answer resolves.
    if errors.is_empty() {
        errors.extend(impossible_answers("the group", &group, None, Layer::Group));
        for r in &routes {
            let what = format!("route {:?}", r.id);
            errors.extend(impossible_answers(&what, &r.on_exception, Some(&group), Layer::Route));
        }
        for d in &destinations {
            let mut own = OnException::default();
            own.answers[ExceptionKind::Refused.index()] = d.on_reject.clone();
            let what = format!("destination {:?}", d.name);
            errors.extend(impossible_answers(&what, &own, Some(&group), Layer::Destination));
            if d.on_reject.targets().contains(&d.id) {
                errors.push(format!(
                    "destination {:?} delivers what it refuses to itself, where it would be refused again; give it \
                     its own answer to Refused",
                    d.name
                ));
            }
        }
        errors.extend(refusal_cycles(&destinations));
    }

    if !errors.is_empty() {
        return Err(LoadError::Validation(errors.into_iter().map(Into::into).collect()));
    }

    let (named_secrets, named_identities) = named_by(&file, &identities);
    Ok(super::LoadedSystem {
        topology: TopologyConfig {
            named_secrets,
            identities: named_identities,
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
    })
}

/// Every credential the bundle names and every identity its connectors name, from the file's own walks: the ones
/// the save-time checks read, so a kind added to the file cannot name a secret the deploy does not look for. An
/// identity named by several connectors is listed once, in the place of the first; its key is a named credential
/// like any other, once per connector naming it.
fn named_by(file: &FileConfig, known: &Identities) -> (Vec<NamedSecret>, Vec<std::sync::Arc<Identity>>) {
    let sources = file
        .sources
        .iter()
        .map(|s| (format!("source {:?}", s.id), s.secrets(), s.identity()));
    let destinations = file
        .destinations
        .iter()
        .map(|d| (format!("destination {:?}", d.id), d.secrets(), d.identity()));
    let mut named = Vec::new();
    let mut identities: Vec<std::sync::Arc<Identity>> = Vec::new();
    for (owner, secrets, identity) in sources.chain(destinations) {
        for (field, reference) in secrets {
            named.push(NamedSecret {
                reference: to_secret_ref(reference),
                what: format!("{owner}: {field}"),
            });
        }
        let identity = identity.and_then(|id| known.get(id));
        named.extend(identity_secret(identity.map(|i| &**i), &owner));
        if let Some(identity) = identity
            && !identities.iter().any(|i| i.id == identity.id)
        {
            identities.push(identity.clone());
        }
    }
    (named, identities)
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
            ("Unmatched", Answer::Ignore | Answer::Tag) => Some("route"),
            ("Unprocessable", Answer::Ignore | Answer::Tag) => Some("record"),
            ("Refused", Answer::Ignore | Answer::Tag) => Some("destination"),
            _ => None,
        };
        if let Some(lacks) = lacks {
            errors.push(format!(
                "{what} answers {kind} with {}, but a {kind} row has no {lacks}",
                answer.label()
            ));
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

fn collect<T>(items: impl Iterator<Item = Result<T, String>>) -> (Vec<T>, Vec<String>) {
    let mut oks = Vec::new();
    let mut errs = Vec::new();
    for r in items {
        match r {
            Ok(v) => oks.push(v),
            Err(e) => errs.push(e),
        }
    }
    (oks, errs)
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
pub(crate) fn to_data_type(dto: &DataTypeDto) -> Result<DataType, String> {
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
                "\"skip\" marks a CSV column to leave out of the record, so it is only a type there -- and never on a \
                 field nested inside an array or struct"
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

/// A parser step's own `declared` list: like `to_fields`, but a top-level `datetime` is legal here -- the parser
/// pins the field to `Str` and the convert pass after the parse runs `parseTime` over it, auto-detected for the bare string and
/// with the format for the object. A format is checked here, so a specifier `parseTime` lacks is refused where it is
/// written. Every other type is exactly `to_data_type`'s, refusals included.
fn to_declared_fields(fields: &[FieldDto]) -> Result<Vec<DeclaredField>, String> {
    fields
        .iter()
        .map(|f| {
            let build = || -> Result<DeclaredField, String> {
                let (ty, time_source) = match &f.ty {
                    DataTypeDto::Scalar(s) if s == "datetime" => (DataType::DateTime, Some(TimeSource::Auto)),
                    DataTypeDto::Compound(CompoundTypeDto::DateTime { format }) => {
                        timeparse::check_format(format).map_err(|e| format!("datetime format {format:?}: {e}"))?;
                        (DataType::DateTime, Some(TimeSource::Format(format.clone())))
                    }
                    other => (to_data_type(other)?, None),
                };
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
            Some(DataTypeDto::Scalar(s)) if s == "datetime" => (DataType::DateTime, Some(TimeSource::Auto)),
            Some(DataTypeDto::Compound(CompoundTypeDto::DateTime { format })) => {
                timeparse::check_format(format).map_err(|e| format!("datetime format {format:?}: {e}"))?;
                (DataType::DateTime, Some(TimeSource::Format(format.clone())))
            }
            Some(DataTypeDto::Scalar(s)) if s == "skip" => {
                return Err("\"skip\" is not a type: leave the column out of the outputs instead".to_string());
            }
            Some(DataTypeDto::Compound(_)) => {
                return Err("a lookup output is one value: bool, i64, f64, str or datetime".to_string());
            }
            Some(other) => (to_data_type(other)?, None),
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
        if matches!(&c.ty, DataTypeDto::Scalar(s) if s == "skip") {
            headers.push(None);
            continue;
        }
        headers.push(Some(c.name.clone()));
        kept.extend(to_declared_fields(std::slice::from_ref(c))?);
    }
    Ok((headers, kept))
}

/// The identities a bundle declares, by the id a connector names one with. Shared rather than copied: several
/// connectors usually name one, and each carries a certificate and a set of authorities.
type Identities = HashMap<String, std::sync::Arc<Identity>>;

fn to_identity(dto: &IdentityDto) -> std::sync::Arc<Identity> {
    std::sync::Arc::new(Identity {
        id: dto.id.clone(),
        certificate: dto.certificate.clone(),
        key: dto.key.as_ref().map(to_secret_ref),
        authorities: dto.authorities.clone(),
        trust_public: dto.trust_public != Some(false),
    })
}

fn to_start(start: StartPositionDto) -> StartPosition {
    match start {
        StartPositionDto::Earliest => StartPosition::Earliest,
        StartPositionDto::Latest => StartPosition::Latest,
        StartPositionDto::Timestamp { millis } => StartPosition::Timestamp(millis),
    }
}

fn to_stop(stop: StopPositionDto) -> StopPosition {
    match stop {
        StopPositionDto::Timestamp { millis } => StopPosition::Timestamp(millis),
    }
}

/// The consumer group a Kafka source that names none reads as outside any worker group.
const DEFAULT_GROUP_ID: &str = "smeltr";

/// Gives every Kafka source in `sources` that names no consumer group its worker group's own, `smeltr.<worker
/// group>`. Sources of one worker group then share a group, and its rebalances involve only that group's workers.
/// Called wherever a worker group's bundle is merged with `shared/`, so a shared source reads the whole topic in
/// every worker group that runs it.
pub fn fill_default_group_ids(sources: &mut [SourceDto], worker_group: &str) {
    for s in sources {
        if let SourceKindDto::Kafka { group_id, .. } = &mut s.kind
            && group_id.trim().is_empty()
        {
            *group_id = format!("{DEFAULT_GROUP_ID}.{worker_group}");
        }
    }
}

/// The database an SQL source connects to, from its connection settings alone. `None` for a kind that is not one.
fn sql_database(kind: &SourceKindDto, identities: &Identities) -> Option<crate::plan::topology::SqlDatabase> {
    let (kind, sql) = kind.sql()?;
    let (host, port, database, user) = (
        sql.host.trim().to_string(),
        sql.port(kind),
        sql.database.trim().to_string(),
        sql.user.trim().to_string(),
    );
    let password = sql.password.as_ref().map(to_secret_ref);
    let tls = match sql.tls {
        SqlTlsDto::Require => crate::io::sql_source::Tls::Require,
        SqlTlsDto::Disable => crate::io::sql_source::Tls::Disable,
    };
    let identity = named_identity(&sql.identity, identities);
    Some(match kind {
        SqlKind::Postgres => crate::plan::topology::SqlDatabase::Postgres(crate::io::postgres::PostgresConnection {
            host,
            port,
            database,
            user,
            password,
            tls,
            identity,
        }),
        SqlKind::MySql => crate::plan::topology::SqlDatabase::MySql(crate::io::mysql::MySqlConnection {
            host,
            port,
            database,
            user,
            password,
            tls,
            identity,
        }),
    })
}

/// The database `source` connects to, under `identity` where it names one: what Run reaches before the source
/// declares the fields a load needs. `None` for a source that is not an SQL one.
pub fn sql_connection(
    source: &SourceDto,
    identity: Option<&IdentityDto>,
) -> Option<crate::plan::topology::SqlDatabase> {
    let identities: Identities = identity.iter().map(|i| (i.id.clone(), to_identity(i))).collect();
    sql_database(&source.kind, &identities)
}

/// `name` is the DTO's `id`: v1 has no separate display label.
fn to_source(dto: &SourceDto, id: SourceId, identities: &Identities) -> SourceConfig {
    let mut kind = match &dto.kind {
        SourceKindDto::Kafka {
            brokers,
            topics,
            group_id,
            identity,
            start,
            stop,
            fetch_bytes,
            encoding,
            schema_registry: _,
            read: _,
            props,
        } => SourceKind::Kafka(KafkaSourceConfig {
            identity: named_identity(identity, identities),
            fetch_bytes: fetch_bytes.map_or(FetchBytes::DEFAULT, |f| f.0),
            // Validation refused an unknown label before any of this ran, so the fallback is unreachable.
            encoding: Encoding::for_label(&encoding.0).unwrap_or_default(),
            props: to_kafka_props(props),
            brokers: brokers.clone(),
            topics: topics.clone(),
            // A worker group's load fills a blank first, so a blank here is a bundle read outside one.
            group_id: if group_id.trim().is_empty() {
                DEFAULT_GROUP_ID.to_string()
            } else {
                group_id.clone()
            },
            start: to_start(*start),
            stop: stop.map(to_stop),
            deployed_at: 0,
            member: None,
            rack: None,
            service: KafkaService::Kafka,
            avro: None,
        }),
        SourceKindDto::EventHubs {
            namespace,
            event_hubs,
            consumer_group,
            connection_string,
            sign_in,
            start,
            stop,
            fetch_bytes,
            encoding,
            schema_registry: _,
            read: _,
        } => {
            let brokers = event_hubs_brokers(namespace);
            SourceKind::Kafka(KafkaSourceConfig {
                identity: None,
                fetch_bytes: fetch_bytes.map_or(FetchBytes::DEFAULT, |f| f.0),
                // Validation refused an unknown label before any of this ran, so the fallback is unreachable.
                encoding: Encoding::for_label(&encoding.0).unwrap_or_default(),
                props: event_hubs_props(&brokers, connection_string.as_ref(), sign_in),
                brokers,
                topics: event_hubs.clone(),
                group_id: consumer_group.clone(),
                start: to_start(*start),
                stop: stop.map(to_stop),
                deployed_at: 0,
                member: None,
                rack: None,
                service: KafkaService::EventHubs,
                avro: None,
            })
        }
        SourceKindDto::Metrics { bucket_seconds } => SourceKind::Metrics(MetricsSourceConfig {
            bucket: Duration::from_secs(*bucket_seconds),
        }),
        // Validation refused an unknown label before any of this ran, so the fallback is unreachable.
        SourceKindDto::Logs { min_level } => SourceKind::Logs(LogsSourceConfig {
            min_level: crate::logs::level_for_label(&min_level.0).unwrap_or(tracing::Level::INFO),
        }),
        SourceKindDto::S3Sqs {
            queue_url,
            access,
            identity,
            framing,
            encoding,
            read: _,
            visibility_secs,
        } => SourceKind::Objects(ObjectSourceConfig {
            queue: ObjectQueue::Sqs {
                queue_url: queue_url.trim().to_string(),
                access: to_s3_access(access),
            },
            identity: named_identity(identity, identities),
            framing: to_framing(framing),
            // Validation refused an unknown label before any of this ran, so the fallback is unreachable.
            encoding: Encoding::for_label(&encoding.0).unwrap_or_default(),
            avro: None,
            visibility: Duration::from_secs(visibility_secs.0),
        }),
        SourceKindDto::S3List {
            bucket,
            prefix,
            access,
            identity,
            framing,
            encoding,
            read: _,
            poll_interval_secs,
            start,
            after_commit,
            move_prefix,
        } => listed(
            to_list(
                ListPlace::S3 {
                    bucket: bucket.trim().to_string(),
                    access: to_s3_access(access),
                },
                prefix,
                *poll_interval_secs,
                *start,
                *after_commit,
                move_prefix,
            ),
            named_identity(identity, identities),
            framing,
            encoding,
        ),
        SourceKindDto::GcsList {
            bucket,
            prefix,
            access,
            identity,
            framing,
            encoding,
            read: _,
            poll_interval_secs,
            start,
            after_commit,
            move_prefix,
        } => listed(
            to_list(
                ListPlace::Gcs(GcsStore {
                    bucket: bucket.trim().to_string(),
                    access: to_gcs_access(access),
                }),
                prefix,
                *poll_interval_secs,
                *start,
                *after_commit,
                move_prefix,
            ),
            named_identity(identity, identities),
            framing,
            encoding,
        ),
        SourceKindDto::AzureBlobList {
            container,
            prefix,
            access,
            identity,
            framing,
            encoding,
            read: _,
            poll_interval_secs,
            start,
            after_commit,
            move_prefix,
        } => listed(
            to_list(
                ListPlace::Azure(AzureStore {
                    container: container.trim().to_string(),
                    access: to_azure_access(access),
                }),
                prefix,
                *poll_interval_secs,
                *start,
                *after_commit,
                move_prefix,
            ),
            named_identity(identity, identities),
            framing,
            encoding,
        ),
        SourceKindDto::Sftp {
            directory,
            prefix,
            access,
            framing,
            encoding,
            read: _,
            poll_interval_secs,
            start,
            after_commit,
            move_prefix,
        } => listed(
            to_list(
                ListPlace::Sftp(SftpStore {
                    directory: sftp_directory(directory),
                    access: to_sftp_access(access),
                }),
                prefix,
                *poll_interval_secs,
                *start,
                *after_commit,
                move_prefix,
            ),
            None,
            framing,
            encoding,
        ),
        SourceKindDto::HttpApi {
            url,
            method,
            body,
            headers,
            credentials,
            identity,
            records,
            records_at,
            follow_links,
            since_param,
            since_overlap_secs,
            poll_interval_secs,
            start,
            sign_in,
        } => SourceKind::Api(crate::plan::topology::ApiSourceConfig {
            api: crate::plan::topology::Api::HttpApi(crate::plan::topology::HttpApiConfig {
                url: url.trim().to_string(),
                method: match method {
                    ApiMethodDto::Get => crate::plan::topology::ApiMethod::Get,
                    ApiMethodDto::Post => crate::plan::topology::ApiMethod::Post,
                },
                body: body.as_ref().map(|b| b.to_string()),
                headers: headers.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
                credentials: credentials.iter().map(|(k, r)| (k.clone(), to_secret_ref(r))).collect(),
                records: match records {
                    ApiRecordsDto::Body => ApiRecords::Body,
                    ApiRecordsDto::Lines => ApiRecords::Lines,
                    ApiRecordsDto::Array => ApiRecords::Array(records_at.clone().unwrap_or_default()),
                },
                follow_links: *follow_links,
                since: since_param.as_ref().map(|param| crate::plan::topology::Since {
                    param: param.clone(),
                    overlap: Duration::from_secs(*since_overlap_secs),
                    start: match start {
                        ListStartDto::Everything => ListStart::Everything,
                        ListStartDto::New => ListStart::New,
                    },
                }),
                sign_in: to_app_sign_in(sign_in),
            }),
            identity: named_identity(identity, identities),
            poll_interval: Duration::from_secs(poll_interval_secs.0),
        }),
        SourceKindDto::OktaLogs {
            org,
            token,
            client_id,
            private_key,
            key_id,
            identity,
            poll_interval_secs,
            start,
        } => SourceKind::Api(crate::plan::topology::ApiSourceConfig {
            api: crate::plan::topology::Api::OktaLogs(crate::plan::topology::OktaLogsConfig {
                org: org.trim().trim_end_matches('/').to_string(),
                sign_in: match (token, client_id, private_key) {
                    (Some(token), _, _) => OktaSignIn::Token(to_secret_ref(token)),
                    (None, Some(client_id), Some(private_key)) => OktaSignIn::ServiceApp {
                        client_id: client_id.trim().to_string(),
                        private_key: to_secret_ref(private_key),
                        key_id: key_id
                            .as_deref()
                            .map(str::trim)
                            .filter(|k| !k.is_empty())
                            .map(String::from),
                    },
                    _ => unreachable!("required_fields refuses an Okta source with neither a token nor a service app"),
                },
                start: match start {
                    ListStartDto::Everything => ListStart::Everything,
                    ListStartDto::New => ListStart::New,
                },
            }),
            identity: named_identity(identity, identities),
            poll_interval: Duration::from_secs(poll_interval_secs.0),
        }),
        SourceKindDto::Postgres(_) | SourceKindDto::MySql(_) => {
            let (kind, sql) = dto.kind.sql().expect("an SQL source");
            let database = sql_database(&dto.kind, identities).expect("an SQL source has a database");
            let topic = database.address();
            let read = SqlRead {
                query: &sql.query,
                mode: &sql.mode,
                fields: &sql.fields,
                poll_interval_secs: sql.poll_interval_secs,
                batch: sql.batch,
            };
            SourceKind::Sql(read.config(
                kind.dialect(),
                kind.meta(),
                &format!("{}://{topic}", kind.scheme()),
                topic.clone(),
                database,
            ))
        }
        SourceKindDto::HttpReceiver {
            listener,
            path,
            tokens,
            records,
            records_at,
        } => receiver_config(
            crate::plan::topology::Receiver::Http(crate::plan::topology::HttpReceiverConfig {
                path: path.clone(),
                records: match records {
                    ApiRecordsDto::Body => ApiRecords::Body,
                    ApiRecordsDto::Lines => ApiRecords::Lines,
                    ApiRecordsDto::Array => ApiRecords::Array(records_at.clone().unwrap_or_default()),
                },
            }),
            listener,
            tokens.iter(),
            identities,
        ),
        SourceKindDto::OtlpReceiver {
            listener,
            signal,
            tokens,
        } => receiver_config(
            crate::plan::topology::Receiver::Otlp(*signal),
            listener,
            tokens.iter(),
            identities,
        ),
        SourceKindDto::PrometheusReceiver { listener, tokens } => receiver_config(
            crate::plan::topology::Receiver::Prometheus,
            listener,
            tokens.iter(),
            identities,
        ),
        SourceKindDto::ElasticReceiver { listener, tokens } => receiver_config(
            crate::plan::topology::Receiver::Elastic,
            listener,
            tokens.iter(),
            identities,
        ),
        SourceKindDto::HecReceiver {
            listener,
            tokens,
            sourcetypes,
        } => receiver_config(
            crate::plan::topology::Receiver::Hec(crate::plan::topology::HecReceiverConfig {
                acknowledging: tokens
                    .iter()
                    .filter(|(_, t)| t.acknowledgement)
                    .map(|(name, _)| name.clone())
                    .collect(),
                sourcetypes: to_line_breakers(sourcetypes),
            }),
            listener,
            tokens.iter().map(|(name, t)| (name, &t.token)),
            identities,
        ),
        SourceKindDto::S2sReceiver {
            listener,
            tokens,
            sourcetypes,
        } => receiver_config(
            crate::plan::topology::Receiver::S2s(crate::plan::topology::S2sReceiverConfig {
                sourcetypes: to_line_breakers(sourcetypes),
            }),
            listener,
            tokens.iter(),
            identities,
        ),
        SourceKindDto::SyslogReceiver {
            listener,
            allow_from,
            encoding,
            transport,
            line_breaker,
        } => receiver_config(
            crate::plan::topology::Receiver::Syslog(crate::plan::topology::SyslogReceiverConfig {
                // Validation refused a block that does not parse, or a label that names no encoding, before any of
                // this ran.
                allow_from: allow_from.iter().filter_map(|b| b.trim().parse().ok()).collect(),
                encoding: Encoding::for_label(&encoding.0).unwrap_or_default(),
                transport: *transport,
                line_breaker: line_breaker.as_deref().and_then(to_line_breaker),
            }),
            listener,
            std::iter::empty(),
            identities,
        ),
        SourceKindDto::AzureBlobQueue {
            queue_url,
            access,
            identity,
            framing,
            encoding,
            read: _,
            visibility_secs,
            max_deliveries,
        } => SourceKind::Objects(ObjectSourceConfig {
            queue: ObjectQueue::StorageQueue {
                queue_url: queue_url.clone(),
                access: to_azure_access(access),
                max_deliveries: max_deliveries.0,
            },
            identity: named_identity(identity, identities),
            framing: to_framing(framing),
            // As above.
            encoding: Encoding::for_label(&encoding.0).unwrap_or_default(),
            avro: None,
            visibility: Duration::from_secs(visibility_secs.0),
        }),
        SourceKindDto::GcsPubSub {
            subscription,
            pubsub_endpoint,
            access,
            identity,
            framing,
            encoding,
            read: _,
            visibility_secs,
        } => SourceKind::Objects(ObjectSourceConfig {
            queue: ObjectQueue::PubSub {
                subscription: subscription.trim().to_string(),
                endpoint: pubsub_endpoint.clone(),
                access: to_gcs_access(access),
            },
            identity: named_identity(identity, identities),
            framing: to_framing(framing),
            // As above.
            encoding: Encoding::for_label(&encoding.0).unwrap_or_default(),
            avro: None,
            visibility: Duration::from_secs(visibility_secs.0),
        }),
    };
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

/// A flush that outlasts the consumer's `max.poll.interval.ms` gets the consumer evicted, which turns one stalled
/// destination into a group-wide rebalance.
fn check_stall_after(t: StallAfter, id: &str, errors: &mut ItemErrors<'_>) {
    if t.0 < StallAfter::MIN || t.0 > StallAfter::MAX {
        errors.at("stallAfterSecs").push(format!(
            "destination {id:?} sets a stall threshold of {}s; it must be between {} and {}. \
             A flush that outlasts the consumer's poll interval gets the consumer evicted, \
             which turns one stalled destination into a group-wide rebalance",
            t.0,
            StallAfter::MIN,
            StallAfter::MAX
        ));
    }
}

/// The shortest stall threshold an SFTP destination takes. A write is cut off at the threshold, so a shorter one
/// would cut off a sign-in before it finishes, and a request on a dead connection before the SFTP client's own 10 s
/// timeout drops that connection.
const SFTP_MIN_STALL_AFTER: u64 = 15;

/// Properties smeltr sets itself, with the reason it owns each. Both spellings of an aliased property appear:
/// librdkafka resolves either to the same setting, and operator properties are applied after smeltr's.
const RESERVED_KAFKA_PROPS: &[(&str, &str)] = &[
    (
        "bootstrap.servers",
        "set from this source or destination's own `brokers` field",
    ),
    (
        "sasl.kerberos.kinit.cmd",
        "librdkafka runs it through system() on every worker, at client start and on each ticket \
         refresh; its default already uses the keytab and principal a bundle names, and a bundle \
         may not carry a shell command",
    ),
    (
        "metadata.broker.list",
        "librdkafka's other spelling of `bootstrap.servers`, which is set from this source or destination's own `brokers` field",
    ),
    ("group.id", "set from this source's own `groupId` field"),
    ("client.rack", "each worker sets it from its zone setting"),
    ("group.instance.id", "each worker sets it from its own id"),
    (
        "enable.auto.commit",
        "smeltr commits explicitly once a batch's destinations have flushed; letting librdkafka auto-commit would acknowledge offsets for rows that have not been delivered, silently breaking at-least-once",
    ),
    (
        "auto.commit.enable",
        "a deprecated topic-level property for librdkafka's legacy simple consumer, which the high-level consumer smeltr runs ignores -- setting it reads as a way to control auto-commit and does nothing at all, where `enable.auto.commit` is the global property smeltr pins off and reserves",
    ),
    ("auto.offset.reset", "set from this source's own `start` field"),
    (
        "max.poll.interval.ms",
        "chosen together with the destination stall threshold so a failing destination cannot trigger a group-wide rebalance",
    ),
    (
        "compression.type",
        "set from this destination's own `compression` field, which offers exactly the codecs this build can produce -- two ways to set one thing is one way too many",
    ),
    (
        "compression.codec",
        "librdkafka's other spelling of `compression.type`, which is set from this destination's own `compression` field",
    ),
    (
        "queued.max.messages.kbytes",
        "set from this source's own `fetchBytes` field, which is what makes a reader's memory footprint a number this deployment chose",
    ),
    (
        "queue.buffering.max.kbytes",
        "set from this destination's own `bufferBytes` field: the producer holds half of it and the egress queue in front of it the other half",
    ),
    (
        "statistics.interval.ms",
        "smeltr reports librdkafka's own statistics as `smeltr_destination_throttled_seconds_total` and \
         `smeltr_destination_buffered_bytes`, which is the only way broker-enforced quota throttling is visible at \
         all; setting it to 0 would switch those series off with nothing to say they had stopped",
    ),
    (
        "queue.buffering.max.messages",
        "left at librdkafka's own default, so it cannot undercut the bound `bufferBytes` sets on \
         `queue.buffering.max.kbytes`; set to 0 it would remove that bound entirely",
    ),
    (
        "batch.size",
        "derived at deployment from this destination's own `bufferBytes`, the topic's partitions and the measured \
         round trip to the broker, so one buffer says what the link carries",
    ),
    (
        "message.max.bytes",
        "set with the derived batch, capped at what the broker takes, which the worker asks it at deployment",
    ),
    (
        "batch.num.messages",
        "raised with the derived batch, so a batch of small records can fill to its bytes",
    ),
    (
        "linger.ms",
        "derived at deployment from the measured round trip to the broker, so a batch fills while the one before it \
         is in flight",
    ),
    (
        "fetch.max.bytes",
        "set from this source's own `fetchBytes` field: one fetch may carry half the queue, so a fetch always lands with room behind it",
    ),
    (
        "receive.message.max.bytes",
        "raised with the fetch derived from this source's own `fetchBytes` field, since librdkafka refuses a fetch \
         larger than the message that carries it",
    ),
    (
        "fetch.message.max.bytes",
        "set from this source's own `fetchBytes` field, the same as `fetch.max.bytes`, so one hot partition may fill a fetch",
    ),
    (
        "enable.idempotence",
        "required for the producer not to duplicate or reorder records on retry, which at-least-once delivery and the Elasticsearch dedupe story both rely on",
    ),
    (
        "transactional.id",
        "smeltr never begins a transaction, and a transactional producer refuses every record sent outside one",
    ),
    (
        "partition.assignment.strategy",
        "smeltr runs cooperative rebalancing, and cooperative and eager members must not share a consumer group -- a source that set this to an eager strategy would break rebalancing for every worker in its group, not only itself",
    ),
];

/// Whether a librdkafka property's value is a credential, and so may only be named through `credentials`.
/// Classified by shape rather than listed: librdkafka gains properties with each release, and an unrecognized key that
/// says `password` must still be refused. A path to key material is not a credential.
fn is_credential_valued(key: &str) -> bool {
    const CREDENTIAL_WORDS: &[&str] = &["password", "passphrase", "secret"];
    /// Keys carrying a private key *inline*, as opposed to a path to one.
    const INLINE_PRIVATE_KEYS: &[&str] = &["ssl.key.pem", "sasl.oauthbearer.assertion.private.key.pem"];
    let lower = key.to_ascii_lowercase();
    CREDENTIAL_WORDS.iter().any(|w| lower.contains(w)) || INLINE_PRIVATE_KEYS.contains(&lower.as_str())
}

/// PEM key material, whatever the property is called.
fn looks_like_key_material(value: &str) -> bool {
    value.contains("-----BEGIN")
}

/// Infallible; [`validate_kafka_props`] has already run.
fn to_kafka_props(dto: &crate::config_file::dto::KafkaPropsDto) -> crate::plan::topology::KafkaProps {
    crate::plan::topology::KafkaProps {
        literal: dto.properties.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
        credentials: dto
            .credentials
            .iter()
            .map(|(k, r)| (k.clone(), to_secret_ref(r)))
            .collect(),
    }
}

/// The identity a connector names, which `required_fields` has already refused the bundle for not declaring.
/// A miss is read as absent rather than panicked on: the guarantee belongs to a caller, not to this lookup.
fn named_identity(named: &Option<String>, identities: &Identities) -> Option<std::sync::Arc<Identity>> {
    identities.get(named.as_deref()?).cloned()
}

fn to_framing(dto: &ObjectFramingDto) -> Framing {
    match dto.framing {
        // Validation refused a pattern that does not compile before any of this ran, so the fallback is unreachable.
        FramingDto::Lines => dto
            .line_breaker
            .as_deref()
            .and_then(to_line_breaker)
            .map_or(Framing::Lines, Framing::LineBreaker),
        FramingDto::Object => Framing::Object,
        FramingDto::EscapedLines => Framing::EscapedLines,
    }
}

/// Each sourcetype's line breaker. One with none, or `PER_LINE`, is left out, as an event per line. Validation refused a
/// pattern that does not compile before any of this ran.
fn to_line_breakers(sourcetypes: &BTreeMap<String, SourcetypeDto>) -> BTreeMap<String, LineBreaker> {
    sourcetypes
        .iter()
        .filter_map(|(name, t)| {
            let pattern = t.line_breaker.as_deref().filter(|p| *p != PER_LINE)?;
            Some((name.clone(), to_line_breaker(pattern)?))
        })
        .collect()
}

/// The line breaker of `pattern`, or `None` when it does not compile.
fn to_line_breaker(pattern: &str) -> Option<LineBreaker> {
    regex::bytes::Regex::new(pattern).ok().map(LineBreaker)
}

/// A listing source of `queue`. Nothing hides a listed object, so its visibility is the queue kinds' default.
fn listed(
    queue: ObjectQueue,
    identity: Option<std::sync::Arc<Identity>>,
    framing: &ObjectFramingDto,
    encoding: &EncodingLabel,
) -> SourceKind {
    SourceKind::Objects(ObjectSourceConfig {
        queue,
        identity,
        framing: to_framing(framing),
        // Validation refused an unknown label before any of this ran, so the fallback is unreachable.
        encoding: Encoding::for_label(&encoding.0).unwrap_or_default(),
        avro: None,
        visibility: Duration::from_secs(VisibilitySecs::DEFAULT),
    })
}

/// The listing of `prefix` in `place`.
fn to_list(
    place: ListPlace,
    prefix: &str,
    poll_interval: PollIntervalSecs,
    start: ListStartDto,
    after_commit: AfterCommitDto,
    move_prefix: &Option<String>,
) -> ObjectQueue {
    ObjectQueue::List {
        place,
        prefix: prefix.to_string(),
        poll_interval: Duration::from_secs(poll_interval.0),
        start: match start {
            ListStartDto::Everything => ListStart::Everything,
            ListStartDto::New => ListStart::New,
        },
        after_commit: match after_commit {
            AfterCommitDto::Keep => AfterCommit::Keep,
            AfterCommitDto::Delete => AfterCommit::Delete,
            // Validation refused a move with no prefix before any of this ran.
            AfterCommitDto::Move => AfterCommit::Move {
                prefix: move_prefix.clone().unwrap_or_default(),
            },
        },
    }
}

fn to_gcs_access(dto: &GcsAccessDto) -> GcsAccess {
    GcsAccess {
        endpoint: dto.endpoint.clone(),
        service_account_key: dto.service_account_key.as_ref().map(to_secret_ref),
    }
}

fn to_azure_access(dto: &AzureAccessDto) -> AzureAccess {
    AzureAccess {
        account: dto.account.clone(),
        endpoint: dto.endpoint.clone(),
        auth_endpoint: dto.auth_endpoint.clone(),
        // More than one, or part of a principal, is refused where the fields are checked.
        credential: match (
            &dto.access_key,
            &dto.sas_token,
            &dto.tenant_id,
            &dto.client_id,
            &dto.client_secret,
        ) {
            (Some(key), ..) => Some(AzureCredential::AccessKey(to_secret_ref(key))),
            (_, Some(token), ..) => Some(AzureCredential::SasToken(to_secret_ref(token))),
            (_, _, Some(tenant_id), Some(client_id), Some(secret)) => Some(AzureCredential::ClientSecret {
                tenant_id: tenant_id.clone(),
                client_id: client_id.clone(),
                secret: to_secret_ref(secret),
            }),
            _ => dto
                .managed_identity_client_id
                .as_ref()
                .map(|client_id| AzureCredential::ManagedIdentity {
                    client_id: client_id.trim().to_string(),
                }),
        },
    }
}

/// `directory` as one path: repeated slashes read as one, and no trailing slash but on the root.
fn sftp_directory(directory: &str) -> String {
    let parts: Vec<&str> = directory.trim().split('/').filter(|p| !p.is_empty()).collect();
    match (directory.trim().starts_with('/'), parts.join("/")) {
        (true, d) => format!("/{d}"),
        (false, d) => d,
    }
}

fn to_sftp_access(dto: &SftpAccessDto) -> SftpAccess {
    SftpAccess {
        host: dto.host.trim().to_string(),
        // `check_sftp_access` refused a port out of range.
        port: u16::try_from(dto.port).unwrap_or_default(),
        username: dto.username.trim().to_string(),
        // Validation refused naming both or neither before any of this ran, so the empty password is unreachable.
        credential: match (&dto.private_key, &dto.password) {
            (Some(key), _) => SftpCredential::PrivateKey(to_secret_ref(key)),
            (None, password) => SftpCredential::Password(to_secret_ref(
                password.as_ref().unwrap_or(&SecretRefDto::WorkerLocal(String::new())),
            )),
        },
        host_key: dto.host_key.trim().to_string(),
    }
}

fn to_s3_access(dto: &S3AccessDto) -> S3Access {
    S3Access {
        region: dto.region.clone(),
        endpoint: dto.endpoint.clone(),
        auth_endpoint: dto.auth_endpoint.clone(),
        access_key: match (&dto.access_key_id, &dto.secret_access_key) {
            (Some(id), Some(secret)) => Some(AccessKey {
                id: to_secret_ref(id),
                secret: to_secret_ref(secret),
            }),
            // Half a key is refused where the fields are checked.
            _ => None,
        },
        role: dto.role_arn.as_ref().map(|arn| AssumedRole {
            arn: arn.clone(),
            external_id: dto.external_id.clone(),
        }),
    }
}

/// The longest name an Azure Storage queue takes.
const QUEUE_NAME_MAX: usize = 63;

/// Refuses an object source's empty queue URL, and a visibility the queue would refuse.
fn check_queue(queue_url: &str, visibility: VisibilitySecs, what: &str, errors: &mut ItemErrors<'_>) {
    if queue_url.trim().is_empty() {
        errors.at("queueUrl").push(format!("{what} names no queueUrl"));
    }
    check_visibility(visibility, VisibilitySecs::MAX, what, errors);
}

/// Refuses a visibility the queue would refuse: above `max`, its longest, or below the shortest any queue takes.
fn check_visibility(visibility: VisibilitySecs, max: u64, what: &str, errors: &mut ItemErrors<'_>) {
    if !(VisibilitySecs::MIN..=max).contains(&visibility.0) {
        errors.at("visibilitySecs").push(format!(
            "{what} hides a received message for {}s; visibilitySecs must be between {} and {max}",
            visibility.0,
            VisibilitySecs::MIN,
        ));
    }
}

/// Refuses a name S3 would not give a bucket: 3 to 63 lowercase letters, digits, dots and hyphens, starting and
/// ending with a letter or digit. The usual mistake it catches is a URL or an `s3://` path in place of the name.
fn check_bucket_name(bucket: &str, what: &str, errors: &mut ItemErrors<'_>) {
    let edge = |c: Option<char>| c.is_some_and(|c| c.is_ascii_lowercase() || c.is_ascii_digit());
    let plausible = (3..=63).contains(&bucket.len())
        && bucket
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '.' || c == '-')
        && edge(bucket.chars().next())
        && edge(bucket.chars().last());
    if bucket.trim().is_empty() {
        errors.at("bucket").push(format!("{what} names no bucket"));
    } else if !plausible {
        errors.at("bucket").push(format!(
            "{what} has the bucket {bucket:?}, which is not a bucket's name. A name is 3 to 63 lowercase letters, \
             digits, dots and hyphens, starting and ending with a letter or digit; name the bucket alone, with no \
             s3:// or path"
        ));
    }
}

/// Refuses a listing source's poll and after-commit settings that would stall it or read an object twice.
fn check_listing(
    prefix: &str,
    poll_interval: PollIntervalSecs,
    start: ListStartDto,
    after_commit: AfterCommitDto,
    move_prefix: &Option<String>,
    what: &str,
    errors: &mut ItemErrors<'_>,
) {
    if poll_interval.0 == 0 {
        errors.at("pollIntervalSecs").push(format!(
            "{what} lists its bucket every 0 seconds; pollIntervalSecs is at least 1"
        ));
    }
    if start == ListStartDto::New && after_commit != AfterCommitDto::Keep {
        errors.at("start").push(format!(
            "{what} starts at new objects and does not keep what it reads. Only the ledger that afterCommit \
             \"keep\" writes records the first listing as read; with no ledger every object it found would be read \
             again"
        ));
    }
    if prefix.starts_with('/') {
        errors.at("prefix").push(format!(
            "{what} has the prefix {prefix:?}, which starts with \"/\" and so matches no key; drop the leading \"/\""
        ));
    }
    match (after_commit, move_prefix) {
        (AfterCommitDto::Move, None) => {
            errors.at("movePrefix").push(format!(
                "{what} moves what it reads and names no movePrefix to move it under"
            ));
        }
        (AfterCommitDto::Move, Some(to)) if to.trim().is_empty() => {
            errors
                .at("movePrefix")
                .push(format!("{what} has an empty movePrefix; name where a read object goes"));
        }
        (AfterCommitDto::Move, Some(to)) if to.starts_with('/') => {
            errors.at("movePrefix").push(format!(
                "{what} moves what it reads under {to:?}, which starts with \"/\"; drop the leading \"/\""
            ));
        }
        // A moved key is `movePrefix` + what follows `prefix` in it, so a `/` on one and not the other doubles or
        // drops the `/` between them.
        (AfterCommitDto::Move, Some(to)) if (prefix.is_empty() || prefix.ends_with('/')) != to.ends_with('/') => {
            let listed = if prefix.is_empty() {
                "the whole bucket".to_string()
            } else {
                format!("the prefix {prefix:?}")
            };
            errors.at("movePrefix").push(format!(
                "{what} lists {listed} and moves what it reads under {to:?}, so a moved key gains or loses a \"/\". \
                 End both in \"/\" or neither; an empty prefix ends in \"/\""
            ));
        }
        // `Path::parse` takes `done//` by dropping both `/`s, yet refuses the key `done//x`, so a stem it rewrites
        // is refused.
        (AfterCommitDto::Move, Some(to))
            if {
                let stem = to.strip_suffix('/').unwrap_or(to);
                !object_store::path::Path::parse(stem).is_ok_and(|p| p.as_ref() == stem)
            } =>
        {
            errors.at("movePrefix").push(format!(
                "{what} moves what it reads under {to:?}, which is not an object path. A path is segments \
                 joined by single \"/\", none empty, \".\" or \"..\", \
                 and no control character"
            ));
        }
        (AfterCommitDto::Move, Some(to)) if to.starts_with(prefix) || prefix.starts_with(to.as_str()) => {
            let listed = if prefix.is_empty() {
                "the whole bucket, which it lists".to_string()
            } else {
                format!("the prefix {prefix:?} it lists")
            };
            errors.at("movePrefix").push(format!(
                "{what} moves what it reads under {to:?}, which overlaps {listed}, so it would list every moved \
                 object again. Name a movePrefix outside it"
            ));
        }
        (AfterCommitDto::Keep | AfterCommitDto::Delete, Some(_)) => {
            errors.at("movePrefix").push(format!(
                "{what} names a movePrefix and does not move what it reads; drop it, or set afterCommit to \"move\""
            ));
        }
        _ => {}
    }
}

/// The longest ack deadline Pub/Sub takes, in seconds.
const ACK_DEADLINE_MAX: u64 = 600;

/// Refuses a Pub/Sub subscription that is not named in full, as `projects/<project>/subscriptions/<subscription>`.
fn check_subscription(subscription: &str, what: &str, errors: &mut ItemErrors<'_>) {
    let parts: Vec<&str> = subscription.trim().split('/').collect();
    let named = matches!(parts.as_slice(), ["projects", project, "subscriptions", name]
        if !project.is_empty() && !name.is_empty());
    if subscription.trim().is_empty() {
        errors.at("subscription").push(format!("{what} names no subscription"));
    } else if !named {
        errors.at("subscription").push(format!(
            "{what} has the subscription {subscription:?}. Name it in full, as \
             projects/<project>/subscriptions/<subscription>"
        ));
    }
}

/// `url` as a whole http:// or https:// URL, the one shape every URL field takes; `None` when it is not one.
pub fn web_url(url: &str) -> Option<reqwest::Url> {
    reqwest::Url::parse(url.trim())
        .ok()
        .filter(|u| matches!(u.scheme(), "http" | "https") && u.has_host())
}

/// Refuses `url` at `field` unless it is a whole http:// or https:// URL. A blank one is left to the check that
/// says what the field needs. `what` names the connector.
fn check_web_url(url: &str, field: &'static str, what: &str, errors: &mut ItemErrors<'_>) {
    if !url.trim().is_empty() && web_url(url).is_none() {
        errors.at(field).push(format!(
            "{what} has {field} {url:?}; give a whole http:// or https:// URL"
        ));
    }
}

/// Refuses a GCS access block with an endpoint that is named but empty. `what` names the connector, and `field`
/// the endpoint's.
fn check_endpoint(endpoint: Option<&str>, field: &'static str, what: &str, errors: &mut ItemErrors<'_>) {
    if endpoint.is_some_and(|e| e.trim().is_empty()) {
        errors
            .at(field)
            .push(format!("{what} has an empty {field}; name one, or leave it out"));
    }
}

/// Refuses an Azure access block that cannot sign a request as written. `what` names the connector.
fn check_azure_access(a: &AzureAccessDto, what: &str, errors: &mut ItemErrors<'_>) {
    let blank = |v: &str| v.trim().is_empty();
    if blank(&a.account) {
        errors.at("account").push(format!("{what} names no account"));
    }
    let others: Vec<&'static str> = [
        (a.access_key.is_some(), "accessKey"),
        (a.sas_token.is_some(), "sasToken"),
    ]
    .into_iter()
    .filter_map(|(named, field)| named.then_some(field))
    .collect();
    check_entra_sign_in(&a.entra(), &others, &["accessKey", "sasToken"], what, errors);
    if a.endpoint.as_deref().is_some_and(blank) {
        errors
            .at("endpoint")
            .push(format!("{what} has an empty endpoint; name one, or leave it out"));
    }
}

/// Refuses an SFTP access block that cannot reach or sign in to its server as written. `what` names the connector.
fn check_sftp_access(a: &SftpAccessDto, what: &str, errors: &mut ItemErrors<'_>) {
    if a.host.trim().is_empty() {
        errors.at("host").push(format!("{what} names no host"));
    }
    if a.port == 0 || a.port > u32::from(u16::MAX) {
        errors.at("port").push(format!(
            "{what} has port {}; name the server's port, 1 to 65535, 22 by default",
            a.port
        ));
    }
    if a.username.trim().is_empty() {
        errors.at("username").push(format!("{what} names no username"));
    }
    if a.password.is_some() == a.private_key.is_some() {
        errors.at("password").push(format!(
            "{what} must name exactly one of password and privateKey to sign in with"
        ));
    }
    if let Err(e) = russh::keys::PublicKey::from_openssh(a.host_key.trim()) {
        errors.at("hostKey").push(format!(
            "{what} has a hostKey that is not a public key as OpenSSH writes it (\"ssh-ed25519 AAAA…\"): {e}. \
             Copy the server's line from its known_hosts entry or from ssh-keyscan, without the host name"
        ));
    }
}

/// Refuses an S3 access block that cannot sign a request as written. `what` names the connector.
fn check_s3_access(a: &S3AccessDto, what: &str, errors: &mut ItemErrors<'_>) {
    if a.access_key_id.is_some() != a.secret_access_key.is_some() {
        let missing = if a.access_key_id.is_none() {
            "accessKeyId"
        } else {
            "secretAccessKey"
        };
        errors.at(missing).push(format!(
            "{what} names half an access key. Name both accessKeyId and secretAccessKey, or neither to use the \
             host's credentials"
        ));
    }
    if (a.external_id.is_some() || a.auth_endpoint.is_some()) && a.role_arn.is_none() {
        errors.at("roleArn").push(format!(
            "{what} has an externalId or authEndpoint and no roleArn; both serve assuming a role"
        ));
    }
    for (field, value) in [
        ("region", &a.region),
        ("endpoint", &a.endpoint),
        ("authEndpoint", &a.auth_endpoint),
        ("roleArn", &a.role_arn),
        ("externalId", &a.external_id),
    ] {
        if value.as_deref().is_some_and(|v| v.trim().is_empty()) {
            errors
                .at(field)
                .push(format!("{what} has an empty {field}; name one, or leave it out"));
        }
    }
}

/// How `kind` reads its payloads as Avro, where its `format` is `"avro"`.
fn to_avro_read(kind: &SourceKindDto) -> Option<crate::plan::topology::AvroRead> {
    kind.read()
        .filter(|r| r.format == Some(crate::config_file::dto::SourceFormatDto::Avro))?;
    Some(crate::plan::topology::AvroRead {
        registry: kind.schema_registry().map(to_registry),
        meta: kind.meta(),
    })
}

/// The registry a Kafka or Event Hubs destination registers its Avro schemas in, where its format is Avro.
fn avro_write(
    format: FormatDto,
    registry: &Option<crate::config_file::dto::SchemaRegistryDto>,
) -> Option<crate::plan::topology::SchemaRegistry> {
    registry.as_ref().filter(|_| format == FormatDto::Avro).map(to_registry)
}

fn to_registry(dto: &crate::config_file::dto::SchemaRegistryDto) -> crate::plan::topology::SchemaRegistry {
    crate::plan::topology::SchemaRegistry {
        url: dto.url.clone(),
        // Validation refused a username without a password, and a password without a username.
        basic: dto.username.clone().zip(dto.password.as_ref().map(to_secret_ref)),
    }
}

fn to_secret_ref(dto: &crate::config_file::dto::SecretRefDto) -> crate::plan::topology::SecretRef {
    use crate::config_file::dto::SecretRefDto;
    match dto {
        SecretRefDto::WorkerLocal(var) => crate::plan::topology::SecretRef::WorkerLocal(var.clone()),
        SecretRefDto::LeaderStored(l) => crate::plan::topology::SecretRef::LeaderStored {
            name: l.secret.clone(),
            version: l.version,
        },
    }
}

/// Every key a Kafka client sets, with the map that sets it.
fn kafka_keys(props: &KafkaPropsDto) -> impl Iterator<Item = (&'static str, &String)> {
    (props.properties.keys().map(|k| ("properties", k))).chain(props.credentials.keys().map(|k| ("credentials", k)))
}

/// Checks one Kafka client's operator-supplied properties.
fn validate_kafka_props(dto: &KafkaPropsDto, what: &str, errors: &mut ItemErrors<'_>) {
    for (map, key) in kafka_keys(dto) {
        if let Some((_, why)) = RESERVED_KAFKA_PROPS.iter().find(|(k, _)| k == key) {
            errors.at(map).push(format!(
                "{what} sets the librdkafka property {key:?}, which smeltr owns: {why}"
            ));
        }
    }
    for (key, value) in dto.properties.iter() {
        if is_credential_valued(key) {
            errors.at("properties").push(format!(
                "{what} puts {key:?} in `properties`, whose values are committed to the \
                 config repository and read in review. Move it to `credentials` and name \
                 where the value lives instead"
            ));
        } else if looks_like_key_material(value) {
            errors.at("properties").push(format!(
                "{what} puts what looks like PEM key material in `properties` under {key:?}, \
                 and those values are committed to the config repository and read in review. \
                 Move it to `credentials` and name where the value lives, or point at it \
                 with a `*.location` path instead"
            ));
        }
    }
    // Which mechanism is selected decides which provider librdkafka must have linked, and `validate_literal_props` can
    // only ask about a value it can see. It is never a credential, so naming it here would hide the one question that
    // has to be answered before a deploy rather than after.
    for key in dto.credentials.keys() {
        if matches!(key.as_str(), "sasl.mechanism" | "sasl.mechanisms") {
            errors.at("credentials").push(format!(
                "{what} names {key:?} in `credentials`, whose value is not knowable at \
                 save time. It holds no secret, so put it in `properties` and let the build \
                 be asked whether it carries that mechanism"
            ));
        }
    }

    // A property in both maps has two answers and no rule for picking one.
    for key in dto.credentials.keys() {
        if dto.properties.contains_key(key) {
            errors.at("credentials").push(format!(
                "{what} sets {key:?} in both `properties` and `credentials`; \
                 give it one value or the other"
            ));
        }
    }

    // librdkafka checks the rest: a misspelled name or an out-of-range value is refused at save, not at deploy. Keys
    // already refused above are left out so one mistake gets one error.
    let remaining = crate::plan::topology::KafkaProps {
        literal: dto
            .properties
            .iter()
            .filter(|(k, _)| !RESERVED_KAFKA_PROPS.iter().any(|(r, _)| r == k) && !is_credential_valued(k))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
        credentials: Vec::new(),
    };
    let mut refused = Vec::new();
    crate::io::kafka::validate_literal_props(&remaining, what, &mut refused);
    for message in refused {
        errors.at("properties").push(message);
    }
    check_sasl_mechanism_case(dto, what, errors);
    check_sasl_login(dto, what, errors);
}

/// Refuses a mechanism librdkafka knows written in another case: it matches names case-sensitively, so it would refuse
/// the mechanism only once a worker builds the client.
fn check_sasl_mechanism_case(dto: &KafkaPropsDto, what: &str, errors: &mut ItemErrors<'_>) {
    const MECHANISMS: [&str; 5] = ["GSSAPI", "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512", "OAUTHBEARER"];
    for key in ["sasl.mechanism", "sasl.mechanisms"] {
        let Some(mechanism) = dto.properties.get(key) else {
            continue;
        };
        if let Some(name) = MECHANISMS
            .iter()
            .find(|m| m.eq_ignore_ascii_case(mechanism) && *m != mechanism)
        {
            errors.at("properties").push(format!(
                "{what} sets {key:?} to {mechanism:?}, which librdkafka does not know: it names mechanisms \
                 case-sensitively. Write {name:?}"
            ));
        }
    }
}

/// Refuses a PLAIN or SCRAM login without both its username and its password, which librdkafka would refuse only once
/// a worker builds the client.
fn check_sasl_login(dto: &KafkaPropsDto, what: &str, errors: &mut ItemErrors<'_>) {
    // A protocol named in `credentials` is not knowable at save time, so it may be SASL.
    let sasl = dto.credentials.contains_key("security.protocol")
        || (dto.properties.get("security.protocol")).is_some_and(|p| p.to_ascii_uppercase().starts_with("SASL_"));
    // Either spelling may be the one librdkafka takes, and it names mechanisms case-sensitively.
    let Some(mechanism) = ["sasl.mechanism", "sasl.mechanisms"]
        .into_iter()
        .filter_map(|k| dto.properties.get(k))
        .find(|m| matches!(m.as_str(), "PLAIN" | "SCRAM-SHA-256" | "SCRAM-SHA-512"))
    else {
        return;
    };
    if !sasl {
        return;
    }
    let keys = [
        ("sasl.username", "`properties`, or in `credentials` if it is secret"),
        ("sasl.password", "`credentials`"),
    ];
    for (key, where_) in keys {
        if !dto.properties.contains_key(key) && !dto.credentials.contains_key(key) {
            errors.at("properties").push(format!(
                "{what} signs in with SASL {mechanism} and does not set {key:?}, which it needs. Set it in {where_}"
            ));
        }
    }
}

// -- identities -------------------------------------------------------------

/// librdkafka property names that answer the same question an identity answers: what this client presents, and what
/// it trusts. A connector naming an identity and one of these has two answers to one question, and librdkafka would
/// silently take whichever was set last.
const IDENTITY_KAFKA_PROPS: &[&str] = &[
    "ssl.certificate.pem",
    "ssl.certificate.location",
    "ssl.key.pem",
    "ssl.key.location",
    "ssl.ca.pem",
    "ssl.ca.location",
    // PKCS#12: the same certificate and key, in one file, under other names.
    "ssl.keystore.location",
    "ssl.keystore.password",
    // Not material, but the one thing that would make an identity's key mean different things to the two
    // connectors: librdkafka decrypts a passphrase-protected key and ureq cannot, so a connector holding this
    // beside an identity is a deployment that works on Kafka and fails on Elasticsearch. The value is a credential
    // and may be worker-local, so save time cannot look at the key and answer this any other way.
    "ssl.key.password",
    // Naming an identity is naming which authorities a far end is verified against; a connector that also turns
    // verification off is not doing that at all. Elasticsearch has no equivalent knob, so the identity would
    // verify on one connector and not the other -- the same asymmetry `ssl.key.password` is refused for.
    "enable.ssl.certificate.verification",
    // A different half of the same question: the chain can check out under the identity's own authorities while
    // this decides whether the certificate is also checked against the broker's own name. "none" is what turns
    // that off -- any certificate the identity's authorities ever issued, for any host, then answers for this one.
    "ssl.endpoint.identification.algorithm",
];

/// The librdkafka security protocols that negotiate TLS. Under any other, nothing an identity holds is used, and
/// librdkafka's own default is `plaintext`.
const TLS_PROTOCOLS: &[&str] = &["SSL", "SASL_SSL"];

/// Refuses `pem` unless it holds certificates and nothing else. Read with the same library that will present
/// them, so a save cannot accept PEM the connector would go on to refuse.
///
/// Every PEM section, not `CertificateDer`'s own iterator: that one selects certificate sections rather than
/// proving there is nothing else, so a certificate beside a CSR or a public key would pass it silently.
///
/// Each section's DER is parsed as X.509, not just labeled `CERTIFICATE`: a `CERTIFICATE` section wrapping
/// malformed or truncated DER passes the label check and only surfaces at the TLS handshake, in a chain member
/// `refuse_unless_usable` never reads (it looks at the leaf alone) or in an authorities bundle
/// nothing else here reads at all.
fn refuse_unless_certificates(pem: &str) -> Result<(), String> {
    use rustls::pki_types::pem::{PemObject, SectionKind};

    if pem.contains("PRIVATE KEY") {
        return Err("it holds a private key".to_string());
    }
    let mut certificates = 0;
    for section in <(SectionKind, Vec<u8>)>::pem_slice_iter(pem.as_bytes()) {
        let (kind, der) = section.map_err(|e| format!("{e}"))?;
        if kind != SectionKind::Certificate {
            return Err(format!("it holds a {kind:?} section, not a certificate"));
        }
        let (remainder, _) = x509_parser::parse_x509_certificate(&der)
            .map_err(|e| format!("it holds a certificate that is not one this can read: {e}"))?;
        if !remainder.is_empty() {
            return Err("it holds a certificate followed by bytes that are not one".to_string());
        }
        certificates += 1;
    }
    if certificates == 0 {
        return Err("it holds no certificate".to_string());
    }
    Ok(())
}

/// Which end of a TLS handshake a certificate is presented from.
#[derive(Clone, Copy)]
enum Presented {
    /// By a connector connecting out.
    AsClient,
    /// By a receiver, to its senders.
    AsServer,
}

/// Refuses a presented certificate that cannot authenticate as `presented`: Extended Key Usage naming purposes
/// that leave out that end's, or Key Usage naming permissions that do not include the handshake signature either
/// end makes. Read with the same library the connectors build their TLS configs against, for the same reason
/// `refuse_unless_certificates` is: a save must not accept what a handshake would go on to refuse. Either
/// extension absent entirely rules nothing out -- RFC 5280's own default -- so only one naming purposes or
/// permissions and excluding what this needs is refused.
fn refuse_unless_usable(pem: &str, presented: Presented) -> Result<(), String> {
    use rustls::pki_types::pem::PemObject;

    let leaf = rustls::pki_types::CertificateDer::pem_slice_iter(pem.as_bytes())
        .next()
        .ok_or_else(|| "it holds no certificate".to_string())?
        .map_err(|e| format!("{e}"))?;
    let (_, cert) = x509_parser::parse_x509_certificate(leaf.as_ref())
        .map_err(|e| format!("that is not a certificate this can read: {e}"))?;
    match (cert.extended_key_usage(), presented) {
        (Ok(Some(eku)), Presented::AsClient) if !eku.value.client_auth => {
            return Err(
                "whose Extended Key Usage does not permit client authentication, so a far end enforcing it \
                 refuses this certificate at the handshake -- a connector connecting out presents it as a client \
                 certificate"
                    .to_string(),
            );
        }
        (Ok(Some(eku)), Presented::AsServer) if !eku.value.server_auth => {
            return Err(
                "whose Extended Key Usage does not permit server authentication, so a sender refuses this \
                 certificate at the handshake -- a receiver presents it as a server certificate"
                    .to_string(),
            );
        }
        (Ok(_), _) => {}
        (Err(e), _) => return Err(format!("whose Extended Key Usage could not be read: {e}")),
    }
    // Either end signs the handshake, which is what digitalSignature names permission for: rustls offers only
    // ephemeral key exchange, so no end encrypts with its certificate's key. Present but false is a certificate
    // meant for key exchange, not signing -- the Extended Key Usage names the purpose, this the means, and a far
    // end enforcing KeyUsage checks both.
    match cert.key_usage() {
        Ok(Some(usage)) if !usage.value.digital_signature() => Err(
            "whose Key Usage does not permit a digital signature, so it cannot sign the handshake a far end \
             enforcing it requires"
                .to_string(),
        ),
        Ok(_) => Ok(()),
        Err(e) => Err(format!("whose Key Usage could not be read: {e}")),
    }
}

/// A receiver speaking `receiver`, with what every receiver takes. It listens on `port` at every address of the host,
/// IPv4 and IPv6: `[::]`, which the receiver opens dual-stack. The worker that runs it narrows that to the address its
/// own startup settings name (`startup_config::worker_listen_address`), since an address is a fact about one host.
fn receiver_config<'a>(
    receiver: crate::plan::topology::Receiver,
    listener: &ReceiverSettingsDto,
    tokens: impl Iterator<Item = (&'a String, &'a SecretRefDto)>,
    identities: &Identities,
) -> SourceKind {
    SourceKind::Receiver(crate::plan::topology::ReceiverConfig {
        receiver,
        listen: std::net::SocketAddr::new(std::net::Ipv6Addr::UNSPECIFIED.into(), listener.port),
        tokens: tokens.map(|(k, r)| (k.clone(), to_secret_ref(r))).collect(),
        identity: named_identity(&listener.identity, identities),
        max_request_bytes: usize::try_from(listener.max_request_bytes.0).unwrap_or(usize::MAX),
    })
}

/// Whether a connector connecting out presents `identity`'s certificate. One no connector names yet is held to that
/// too, as what it is most often named for; one only receivers name is checked where they name it.
fn presented_as_client(file: &FileConfig, identity: &str) -> bool {
    let named = |by: Option<&str>| by == Some(identity);
    let served = |s: &SourceDto| s.kind.receiver_port().is_some();
    let receivers = file.sources.iter().filter(|s| served(s) && named(s.identity())).count();
    receivers == 0
        || file.sources.iter().any(|s| !served(s) && named(s.identity()))
        || file.destinations.iter().any(|d| named(d.identity()))
}

/// Checks what an identity says about itself, before anything names it.
fn validate_identities(file: &FileConfig, attributed: &mut Vec<ValidationError>) {
    for i in &file.identities {
        let errors = &mut ItemErrors::new(attributed, "identities", &i.id);
        let what = format!("identity {:?}", i.id);
        if i.id.trim().is_empty() {
            errors.at("id").push("an identity has no id".to_string());
        }

        // A certificate is presented by signing with its key, so one without the other is configuration that
        // cannot act.
        match (&i.certificate, &i.key) {
            (Some(_), None) => errors.at("key").push(format!(
                "{what} has a certificate and no private key. A certificate is presented by signing \
                 with its key, so name the credential holding it"
            )),
            (None, Some(_)) => errors.at("certificate").push(format!(
                "{what} names a private key and has no certificate. A key alone identifies nothing -- \
                 paste the certificate it was issued for, or take the key out"
            )),
            _ => {}
        }

        if i.certificate.is_none() && i.authorities.is_none() {
            errors.at("certificate").push(format!(
                "{what} declares neither a certificate to present nor an authority to trust, so naming \
                 it changes nothing. Fill one of them, or take the identity out"
            ));
        }

        if let Some(certificate) = &i.certificate {
            if let Err(why) = refuse_unless_certificates(certificate) {
                errors.at("certificate").push(format!(
                    "{what} has a certificate that is not one: {why}. It is committed to the config \
                     repository and read in review, so a certificate belongs here and a private key is \
                     named under `key` instead"
                ));
            } else if presented_as_client(file, &i.id)
                && let Err(why) = refuse_unless_usable(certificate, Presented::AsClient)
            {
                errors
                    .at("certificate")
                    .push(format!("{what} presents a certificate {why}"));
            }
        }

        if i.trust_public == Some(false) && i.authorities.is_none() {
            errors.at("trustPublic").push(format!(
                "{what} trusts no public root and names no authority, so nothing it connects to could be \
                 verified. Paste the authorities it trusts, or trust public roots"
            ));
        }

        if let Some(authorities) = &i.authorities
            && let Err(why) = refuse_unless_certificates(authorities)
        {
            errors.at("authorities").push(format!(
                "{what} trusts what is not a certificate authority: {why}. Leave it out to verify a \
                 far end against the platform's own roots"
            ));
        }
    }
}

/// Refuses a connector naming an identity this bundle does not declare. `what` names the connector.
fn check_identity_declared(named: &Option<String>, file: &FileConfig, what: &str, errors: &mut ItemErrors<'_>) {
    let Some(name) = named else { return };
    if !file.identities.iter().any(|i| &i.id == name) {
        errors.at("identity").push(format!(
            "{what} names the identity {name:?}, which this bundle does not declare. Declare it under \
             Identities, or in `shared/`"
        ));
    }
}

/// Refuses an object source naming an identity this bundle does not declare, or one reached at `urls` of which none
/// negotiates TLS: nothing the identity holds would be used. The source's one client reaches every one of them over
/// the identity's TLS. A blank address is refused on its own.
fn check_object_identity(
    named: &Option<String>,
    urls: &[(&'static str, &str)],
    file: &FileConfig,
    what: &str,
    errors: &mut ItemErrors<'_>,
) {
    check_identity_declared(named, file, what, errors);
    let Some(name) = named else { return };
    let tls = |url: &str| url.trim().to_ascii_lowercase().starts_with("https://");
    if let Some((field, url)) = urls.first()
        && !url.trim().is_empty()
        && !urls.iter().any(|(_, url)| tls(url))
    {
        errors.at(field).push(format!(
            "{what} names the identity {name:?} and is reached over {url:?}, which does not negotiate TLS -- \
             nothing the identity holds would be used. Give it an https:// URL, or take the identity off it"
        ));
    }
}

/// Refuses a Kafka client whose own properties disagree with the identity it names.
fn check_kafka_identity(named: &Option<String>, props: &KafkaPropsDto, what: &str, errors: &mut ItemErrors<'_>) {
    let Some(name) = named else { return };

    for (map, key) in kafka_keys(props) {
        if IDENTITY_KAFKA_PROPS.contains(&key.to_ascii_lowercase().as_str()) {
            errors.at(map).push(format!(
                "{what} names the identity {name:?} and also sets {key:?}, which names the same \
                 material the identity carries. Take one of them out, so there is one place it can \
                 come from"
            ));
        }
    }

    // librdkafka defaults this to plaintext, so absent is as wrong as a plaintext value: the identity would be
    // configuration nothing reads.
    match props.properties.get("security.protocol") {
        Some(protocol) if TLS_PROTOCOLS.contains(&protocol.to_ascii_uppercase().as_str()) => {}
        Some(protocol) => errors.at("properties.security.protocol").push(format!(
            "{what} names the identity {name:?} and sets \"security.protocol\" to {protocol:?}, which \
             does not negotiate TLS -- nothing the identity holds would be used. Set {} or {}, or take \
             the identity off it",
            TLS_PROTOCOLS[0], TLS_PROTOCOLS[1]
        )),
        None => errors.at("properties.security.protocol").push(format!(
            "{what} names the identity {name:?} and does not set \"security.protocol\", which \
             librdkafka defaults to plaintext -- nothing the identity holds would be used. Set it to \
             {} or {} in `properties`, or take the identity off it",
            TLS_PROTOCOLS[0], TLS_PROTOCOLS[1]
        )),
    }
}

/// Two sources reading one topic under one consumer group on one cluster are one group member each, so the group
/// splits the topic's partitions between them and each pipeline sees part of it. Refused on the later source.
fn check_group_topics(file: &FileConfig, attributed: &mut Vec<ValidationError>) {
    /// A source reading a topic: its brokers, and its id.
    type Reader<'a> = (HashSet<String>, &'a str);
    // Every source reading each (group, topic) so far.
    let mut readers: HashMap<(&str, &str), Vec<Reader<'_>>> = HashMap::new();
    for s in &file.sources {
        let (brokers, group, topics, field) = match &s.kind {
            SourceKindDto::Kafka {
                brokers,
                group_id,
                topics,
                ..
            } => (brokers.clone(), group_id, topics, "topics"),
            SourceKindDto::EventHubs {
                namespace,
                consumer_group,
                event_hubs,
                ..
            } => (event_hubs_brokers(namespace), consumer_group, event_hubs, "eventHubs"),
            _ => continue,
        };
        let cluster = cluster_of(&brokers);
        // Blank is each worker group's own, filled at the merge, so `shared/` checked alone cannot say which.
        if group.trim().is_empty() {
            continue;
        }
        let errors = &mut ItemErrors::new(attributed, "sources", &s.id);
        for topic in topics.iter().filter(|t| !t.trim().is_empty()) {
            let seen = readers.entry((group.as_str(), topic.trim())).or_default();
            let first = seen
                .iter()
                .find(|(other, id)| *id != s.id && !other.is_disjoint(&cluster))
                .map(|(_, id)| *id);
            seen.push((cluster.clone(), &s.id));
            if let Some(first) = first {
                errors.at(field).push(format!(
                    "source {:?} reads {topic:?} in consumer group {group:?} on the same brokers as source {first:?}, \
                     so the group would split its partitions between them and each would see part of it. Read it \
                     from one source, or give one of them its own group",
                    s.id
                ));
            }
        }
    }
}

/// The brokers a list names. Two lists that share a broker name one cluster, however much else each lists: a broker
/// belongs to one.
fn cluster_of(brokers: &str) -> HashSet<String> {
    brokers
        .split(',')
        .map(|b| b.trim().to_ascii_lowercase())
        .filter(|b| !b.is_empty())
        .collect()
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

/// Refuses a registry with no address, and a username or a password without the other.
fn check_registry(dto: &crate::config_file::dto::SchemaRegistryDto, what: &str, errors: &mut ItemErrors<'_>) {
    if dto.url.trim().is_empty() {
        errors
            .at("schemaRegistry.url")
            .push(format!("{what} names a schema registry but not its address"));
    }
    match (&dto.username, &dto.password) {
        (Some(_), None) => errors.at("schemaRegistry.password").push(format!(
            "{what} signs in to its schema registry with a username but no password"
        )),
        (None, Some(_)) => errors.at("schemaRegistry.username").push(format!(
            "{what} signs in to its schema registry with a password but no username"
        )),
        _ => {}
    }
}

/// Refuses how a source reads its payloads where it does not hold: a stream's Avro read without a registry, an
/// encoding or a framing beside Avro, fields that declare no record, and a registry or fields where the source reads
/// no Avro.
fn check_read(kind: &SourceKindDto, id: &str, errors: &mut ItemErrors<'_>) {
    use crate::config_file::dto::SourceFormatDto;
    let Some(read) = kind.read() else { return };
    let what = format!("source {id:?}");
    let stream = matches!(kind, SourceKindDto::Kafka { .. } | SourceKindDto::EventHubs { .. });
    let framing = match kind {
        SourceKindDto::S3Sqs { framing, .. }
        | SourceKindDto::S3List { framing, .. }
        | SourceKindDto::GcsList { framing, .. }
        | SourceKindDto::AzureBlobList { framing, .. }
        | SourceKindDto::Sftp { framing, .. }
        | SourceKindDto::AzureBlobQueue { framing, .. }
        | SourceKindDto::GcsPubSub { framing, .. } => Some(framing),
        _ => None,
    };
    if let Some(registry) = kind.schema_registry() {
        check_registry(registry, &what, errors);
    }
    let Some(SourceFormatDto::Avro) = read.format else {
        if kind.schema_registry().is_some() {
            errors.at("schemaRegistry").push(format!(
                "{what} names a schema registry but reads no Avro; set format to \"avro\""
            ));
        }
        if !read.fields.is_empty() {
            errors.at("fields").push(format!(
                "{what} declares fields but reads no Avro; set format to \"avro\""
            ));
        }
        return;
    };
    if stream && kind.schema_registry().is_none() {
        errors.at("schemaRegistry.url").push(format!(
            "{what} reads Avro, which a stream frames with its schema's id in a schema registry, but names none"
        ));
    }
    if kind.encoding() != Encoding::default() {
        errors.at("encoding").push(format!(
            "{what} reads Avro, whose strings are UTF-8; leave encoding at {:?}",
            EncodingLabel::DEFAULT
        ));
    }
    if framing.is_some_and(|f| !f.framing.is_default() || f.line_breaker.is_some()) {
        errors.at("framing").push(format!(
            "{what} reads Avro container files, each its own records; remove framing and lineBreaker"
        ));
    }
    if let Err(why) = SqlFieldDto::open_meta(crate::plan::topology::SourceMeta::NONE, &read.fields) {
        errors.at("fields").push(format!("{what}: {why}"));
    }
}

/// Refuses a source's encoding label that names no encoding smeltr knows.
fn check_encoding(encoding: &EncodingLabel, id: &str, errors: &mut ItemErrors<'_>) {
    if Encoding::for_label(&encoding.0).is_none() {
        errors.at("encoding").push(format!(
            "source {id:?} declares encoding {:?}, which is not a name smeltr knows. Use \
             {:?} to detect per record, \"none\" for bytes that are not text, or an encoding label from the \
             WHATWG list (\"utf-8\", \"utf-16le\", \"windows-1252\", \"shift_jis\")",
            encoding.0,
            EncodingLabel::DEFAULT
        ));
    }
}

/// Refuses a line breaker on any framing but lines, and one `check_line_breaker` refuses.
fn check_framing(dto: &ObjectFramingDto, id: &str, errors: &mut ItemErrors<'_>) {
    let Some(line_breaker) = &dto.line_breaker else {
        return;
    };
    let what = format!("source {id:?}");
    if dto.framing == FramingDto::Lines {
        check_line_breaker(&what, line_breaker, errors.at("lineBreaker"));
    } else {
        errors
            .at("lineBreaker")
            .push(format!("{what} sets lineBreaker, which only lines framing reads"));
    }
}

/// Refuses a sourcetype named by a blank name, and a line breaker it gives that `check_line_breaker` refuses.
fn check_sourcetypes(what: &str, sourcetypes: &BTreeMap<String, SourcetypeDto>, errors: &mut ItemErrors<'_>) {
    for (name, t) in sourcetypes {
        // A field is named by a static path, so a refusal names the sourcetype in its message.
        if name.trim().is_empty() {
            errors
                .at("sourcetypes")
                .push(format!("{what} names a sourcetype by a blank name"));
        }
        let what = format!("{what}'s sourcetype {name:?}");
        if let Some(line_breaker) = &t.line_breaker {
            check_line_breaker(&what, line_breaker, errors.at("sourcetypes"));
        }
    }
}

/// Refuses a line breaker that is blank, not a pattern, has no capture group to mark the break, or matches no bytes
/// at all.
fn check_line_breaker(what: &str, pattern: &str, mut errors: ItemErrors<'_>) {
    if let Some(why) = line_breaker_refusal(what, pattern) {
        errors.push(why);
    }
}

/// Why a save refuses `pattern` as a line breaker, said of `what`, or `None` when it takes it. The editor asks this as
/// a line breaker is typed, so its verdict is the save's.
pub fn line_breaker_refusal(what: &str, pattern: &str) -> Option<String> {
    if pattern.trim().is_empty() {
        return Some(format!("{what} names a blank lineBreaker pattern"));
    }
    match regex::bytes::Regex::new(pattern) {
        Err(e) => Some(format!(
            "{what} ends an event where \"{pattern}\" matches, which is not a pattern: {e}"
        )),
        // `LineBreaker` drops the first group as the break.
        Ok(re) if re.captures_len() < 2 => Some(format!(
            "{what} ends an event where \"{pattern}\" matches, which has no capture group to mark the break"
        )),
        Ok(re) if re.is_match(b"") => Some(format!(
            "{what} ends an event where \"{pattern}\" matches, which can match no bytes at all, and so would break \
             between every byte"
        )),
        Ok(_) => None,
    }
}

/// Refuses a Kafka consumer's fetch size outside what one reader may hold.
fn check_fetch_bytes(fetch_bytes: Option<FetchBytes>, id: &str, errors: &mut ItemErrors<'_>) {
    if let Some(fetch_bytes) = fetch_bytes.filter(|f| f.0 < FetchBytes::MIN || f.0 > FetchBytes::MAX) {
        errors.at("fetchBytes").push(format!(
            "source {id:?} lets its consumer hold {} of fetched records; it must \
             be between {} and {}. Below the floor a reader waits on fetches rather \
             than working; above the ceiling one reader's queue outweighs every other \
             buffer the worker holds put together",
            bytes::exact(fetch_bytes.0),
            bytes::exact(FetchBytes::MIN),
            bytes::exact(FetchBytes::MAX)
        ));
    }
}

/// The bootstrap address of an Event Hubs namespace: a bare name is in Azure's public cloud, and a host without a port
/// is reached on 9093, Event Hubs' Kafka port.
pub fn event_hubs_brokers(namespace: &str) -> String {
    let namespace = namespace.trim();
    if !namespace.contains('.') && !namespace.contains(':') {
        format!("{namespace}.servicebus.windows.net:9093")
    } else if namespace.contains(':') {
        namespace.to_string()
    } else {
        format!("{namespace}:9093")
    }
}

/// What signs a Kafka client in to an Event Hubs namespace at `brokers`, as `sign_in` says: its connection string as
/// the password, else an Entra token for the namespace, obtained with a service principal's secret or, naming
/// neither, from the host's managed identity: the one `managedIdentityClientId` names, else the host's own
/// (`io::kafka::apply_client_config` completes that one on the worker).
fn event_hubs_props(brokers: &str, connection_string: Option<&SecretRefDto>, sign_in: &EntraSignInDto) -> KafkaProps {
    // Event Hubs always negotiates TLS; only the emulator, on this host, does not.
    let protocol = if brokers.split(',').all(on_loopback) {
        "SASL_PLAINTEXT"
    } else {
        "SASL_SSL"
    };
    // An Entra token is for the namespace, named by its host.
    let namespace = brokers.split(',').next().unwrap_or_default();
    let host = match crate::startup_config::split_authority(namespace) {
        // An IPv6 literal keeps its brackets in a URL.
        Some((h, _)) if h.contains(':') => format!("[{h}]"),
        Some((h, _)) => h,
        None => namespace.to_string(),
    };
    let literal = |pairs: &[(&str, String)]| -> Vec<(String, String)> {
        (pairs.iter().map(|(k, v)| (k.to_string(), v.clone()))).collect()
    };
    // More than one credential, or part of a principal, is refused where the fields are checked.
    if let Some(connection_string) = connection_string {
        return KafkaProps {
            literal: literal(&[
                ("sasl.mechanism", "PLAIN".into()),
                ("sasl.username", "$ConnectionString".into()),
                ("security.protocol", protocol.into()),
            ]),
            credentials: vec![("sasl.password".to_string(), to_secret_ref(connection_string))],
        };
    }
    match to_entra_sign_in(sign_in) {
        EntraSignIn::ServicePrincipal {
            authority,
            tenant_id,
            client_id,
            secret,
        } => {
            let authority = authority.as_deref().unwrap_or(crate::io::entra_authority(&host));
            KafkaProps {
                literal: literal(&[
                    ("sasl.mechanism", "OAUTHBEARER".into()),
                    ("sasl.oauthbearer.method", "oidc".into()),
                    ("sasl.oauthbearer.client.id", client_id),
                    (
                        "sasl.oauthbearer.token.endpoint.url",
                        format!("{authority}/{tenant_id}/oauth2/v2.0/token"),
                    ),
                    ("sasl.oauthbearer.scope", format!("https://{host}/.default")),
                    ("security.protocol", protocol.into()),
                ]),
                credentials: vec![("sasl.oauthbearer.client.secret".to_string(), secret)],
            }
        }
        EntraSignIn::ManagedIdentity { client_id } => KafkaProps {
            literal: literal(&[
                ("sasl.mechanism", "OAUTHBEARER".into()),
                ("sasl.oauthbearer.method", "oidc".into()),
                ("sasl.oauthbearer.metadata.authentication.type", "azure_imds".into()),
                (
                    "sasl.oauthbearer.config",
                    match client_id {
                        Some(client_id) => format!(
                            "query=api-version={IMDS_API_VERSION}&resource=https://{host}&client_id={client_id}"
                        ),
                        None => format!("query=api-version={IMDS_API_VERSION}&resource=https://{host}"),
                    },
                ),
                ("security.protocol", protocol.into()),
            ]),
            credentials: Vec::new(),
        },
    }
}

/// Whether `url` names anything after its host: a path, a query or a fragment.
fn names_a_path(url: &str) -> bool {
    let url = url.trim();
    let rest = url.split_once("://").map_or(url, |(_, r)| r);
    rest.find(['/', '?', '#']).is_some_and(|i| rest[i..] != *"/")
}

/// Refuses an Event Hubs namespace that is blank or is not a name, a host name or `host:port`.
fn check_event_hubs_namespace(namespace: &str, what: &str, errors: &mut ItemErrors<'_>) {
    if namespace.trim().is_empty() {
        errors.at("namespace").push(format!("{what} names no namespace"));
    } else if namespace.contains("://") || namespace.contains([',', '/']) {
        errors.at("namespace").push(format!(
            "{what} names the namespace {namespace:?}; give its name, such as \"acme\", or its host name, such as \
             \"acme.servicebus.usgovcloudapi.net\", without a scheme or a path"
        ));
    }
}

/// Refuses a managed identity's client id that is not one: Azure names an identity by a GUID, and the id is sent
/// in the metadata service's query.
fn check_managed_identity_client_id(client_id: Option<&str>, what: &str, errors: &mut ItemErrors<'_>) {
    let Some(client_id) = client_id.map(str::trim).filter(|c| !c.is_empty()) else {
        return;
    };
    if !client_id.chars().all(|c| c.is_ascii_hexdigit() || c == '-') {
        errors.at("managedIdentityClientId").push(format!(
            "{what} has managedIdentityClientId {client_id:?}, which is not a client id. Give the managed identity's \
             client id, a GUID, not its name or resource id"
        ));
    }
}

/// The most a Log Analytics destination's request may carry, before compression: the most the Logs Ingestion API takes
/// in one call.
const LOG_ANALYTICS_MAX_REQUEST_BYTES: usize = 1024 * 1024;

/// The Logs Ingestion API version a Log Analytics destination's requests name.
const LOGS_INGESTION_API_VERSION: &str = "2023-01-01";

/// Where rows for `stream` of the data collection rule `rule_id` are POSTed, under the rule's `endpoint`.
fn log_analytics_stream_url(endpoint: &str, rule_id: &str, stream: &str) -> String {
    format!(
        "{}/dataCollectionRules/{}/streams/{}?api-version={LOGS_INGESTION_API_VERSION}",
        endpoint.trim().trim_end_matches('/'),
        crate::io::http::percent_encode(rule_id.trim()),
        crate::io::http::percent_encode(stream.trim()),
    )
}

/// Refuses a Log Analytics destination's rule or stream that the Logs Ingestion API could not name, and an endpoint that
/// names a path: the rule and stream make the path.
fn check_log_analytics_rule(id: &str, endpoint: &str, rule_id: &str, stream: &str, errors: &mut ItemErrors<'_>) {
    let rule = rule_id.trim();
    let hex = rule.strip_prefix("dcr-").unwrap_or_default();
    if hex.len() != 32 || !hex.chars().all(|c| c.is_ascii_hexdigit()) {
        errors.at("ruleId").push(format!(
            "destination {id:?} has ruleId {rule_id:?}; give the data collection rule's immutable id, \"dcr-\" and \
             32 hex digits, from the rule's JSON view"
        ));
    }
    if stream.trim().is_empty() {
        errors.at("stream").push(format!(
            "destination {id:?} names no stream; give the rule's stream, such as \"Custom-Events_CL\""
        ));
    }
    if names_a_path(endpoint) {
        errors.at("endpoint").push(format!(
            "destination {id:?} has endpoint {endpoint:?}; give the endpoint alone, such as \
             \"https://dce-1a2b.eastus-1.ingest.monitor.azure.com\": the rule and the stream follow it"
        ));
    }
}

/// Refuses an Event Hubs sign-in that names more than one credential, part of a service principal, or an empty or
/// unused field.
fn check_event_hubs_sign_in(
    connection_string: Option<&SecretRefDto>,
    a: &EntraSignInDto,
    what: &str,
    errors: &mut ItemErrors<'_>,
) {
    let named: &[&str] = if connection_string.is_some() {
        &["connectionString"]
    } else {
        &[]
    };
    check_entra_sign_in(a, named, &["connectionString"], what, errors);
}

/// Refuses an Entra sign-in that names more than one credential, part of a service principal, or an empty or unused
/// field. `takes` are the kind's own credentials it may name in place of Entra's, and `named` those of them it does.
fn check_entra_sign_in(
    a: &EntraSignInDto,
    named: &[&'static str],
    takes: &[&'static str],
    what: &str,
    errors: &mut ItemErrors<'_>,
) {
    let principal = [a.tenant_id.is_some(), a.client_id.is_some(), a.client_secret.is_some()];
    if principal.contains(&true) && principal.contains(&false) {
        let missing = match (&a.tenant_id, &a.client_id) {
            (None, _) => "tenantId",
            (_, None) => "clientId",
            _ => "clientSecret",
        };
        errors.at(missing).push(format!(
            "{what} names part of a service principal. Name tenantId, clientId and clientSecret together, or none \
             of them"
        ));
    }
    let managed = a.managed_identity_client_id.is_some();
    if named.len() + usize::from(principal.contains(&true)) + usize::from(managed) > 1 {
        // Two or more named: the managed identity, or else one of the kind's own credentials, is one of them.
        let field = if managed {
            "managedIdentityClientId"
        } else {
            named.first().copied().unwrap_or("clientSecret")
        };
        let others: String = takes.iter().map(|o| format!("{o}, ")).collect();
        errors.at(field).push(format!(
            "{what} names more than one credential. Name one of {others}a service principal and \
             managedIdentityClientId, or none to use the host's managed identity"
        ));
    }
    if a.auth_endpoint.is_some() && a.client_secret.is_none() {
        errors.at("clientSecret").push(format!(
            "{what} has an authEndpoint and no clientSecret; only a service principal obtains a token there"
        ));
    }
    if let Some(endpoint) = a.auth_endpoint.as_deref().filter(|e| names_a_path(e)) {
        errors.at("authEndpoint").push(format!(
            "{what} has authEndpoint {endpoint:?}; give the sign-in host alone, such as \
             \"https://login.microsoftonline.us\": the tenant follows it from tenantId"
        ));
    }
    check_managed_identity_client_id(a.managed_identity_client_id.as_deref(), what, errors);
    for (field, value) in [
        ("authEndpoint", &a.auth_endpoint),
        ("tenantId", &a.tenant_id),
        ("clientId", &a.client_id),
        ("managedIdentityClientId", &a.managed_identity_client_id),
    ] {
        if value.as_deref().is_some_and(|v| v.trim().is_empty()) {
            errors
                .at(field)
                .push(format!("{what} has an empty {field}; name one, or leave it out"));
        }
    }
}

/// How `sign_in` signs in, as a worker does it. The fields are checked first, so a partial service principal is
/// never seen.
fn to_entra_sign_in(sign_in: &EntraSignInDto) -> EntraSignIn {
    match (&sign_in.tenant_id, &sign_in.client_id, &sign_in.client_secret) {
        (Some(tenant_id), Some(client_id), Some(secret)) => EntraSignIn::ServicePrincipal {
            authority: (sign_in.auth_endpoint.as_deref()).map(|a| a.trim().trim_end_matches('/').to_string()),
            tenant_id: tenant_id.trim().to_string(),
            client_id: client_id.trim().to_string(),
            secret: to_secret_ref(secret),
        },
        _ => EntraSignIn::ManagedIdentity {
            client_id: (sign_in.managed_identity_client_id.as_deref()).map(|c| c.trim().to_string()),
        },
    }
}

/// Refuses a Kafka producer's `bufferBytes` outside the range a worker can hold.
fn check_kafka_buffer(buffer_bytes: Option<BufferBytes>, id: &str, errors: &mut ItemErrors<'_>) {
    if let Some(buffer_bytes) = buffer_bytes.filter(|b| b.0 < BufferBytes::MIN || b.0 > BufferBytes::MAX) {
        errors.at("bufferBytes").push(format!(
            "destination {id:?} lets egress and its producer each hold {}; it must be between {} \
             and {}. Below the floor an ordinary batch fills the queue and reports a \
             stall it is not in; above the ceiling a destination that stops draining \
             costs more memory than the rest of the worker put together",
            bytes::exact(buffer_bytes.0),
            bytes::exact(BufferBytes::MIN),
            bytes::exact(BufferBytes::MAX)
        ));
    }
}

/// Whether `url` names this host: `localhost` or a loopback address. `url` may be a bare `host:port`.
pub fn on_loopback(url: &str) -> bool {
    let url = url.trim();
    let rest = url.split_once("://").map_or(url, |(_, r)| r);
    crate::startup_config::split_authority(rest.split(['/', '?', '#']).next().unwrap_or_default())
        .is_some_and(|(host, _)| crate::startup_config::is_loopback(&host))
}

/// Whether `url` is plain http:// to a host other than this one: what a credential or a payload may never cross.
pub fn plain_off_this_host(url: &str) -> bool {
    let url = url.trim();
    url.get(..7)
        .is_some_and(|scheme| scheme.eq_ignore_ascii_case("http://"))
        && !on_loopback(url)
}

/// Every field with no workable default, and every bounded value. About emptiness and range, not reachability: a broker
/// that is down is a runtime concern, or a restart would depend on everything else being up. Whitespace counts as
/// empty.
fn required_fields(file: &FileConfig, attributed: &mut Vec<ValidationError>) {
    let blank = |v: &str| v.trim().is_empty();

    validate_identities(file, attributed);
    check_receiver_ports(file, attributed);

    // A credential that names nothing is refused here, or it surfaces only when a worker fails to look it up.
    let named = (file.sources.iter().map(|s| ("sources", "source", &s.id, s.secrets())))
        .chain(
            file.destinations
                .iter()
                .map(|d| ("destinations", "destination", &d.id, d.secrets())),
        )
        .chain(
            file.identities
                .iter()
                .map(|i| ("identities", "identity", &i.id, i.secrets())),
        );
    for (section, what, id, secrets) in named {
        let errors = &mut ItemErrors::new(attributed, section, id);
        let mut fields: Vec<_> = secrets
            .iter()
            .filter(|(_, r)| blank(r.name()))
            .map(|(f, _)| *f)
            .collect();
        fields.dedup();
        for field in fields {
            errors.at(field).push(format!(
                "{what} {id:?} names a credential by a blank name. Name the environment variable or leader secret \
                 holding it"
            ));
        }
    }

    check_group_topics(file, attributed);

    let addressed = (file.sources.iter().map(|s| ("sources", "source", &s.id, s.urls()))).chain(
        file.destinations
            .iter()
            .map(|d| ("destinations", "destination", &d.id, d.urls())),
    );
    for (section, what, id, urls) in addressed {
        let errors = &mut ItemErrors::new(attributed, section, id);
        for (field, url) in urls {
            check_web_url(url, field, &format!("{what} {id:?}"), errors);
        }
    }

    for s in &file.sources {
        let errors = &mut ItemErrors::new(attributed, "sources", &s.id);
        check_read(&s.kind, &s.id, errors);
        match &s.kind {
            SourceKindDto::Kafka {
                brokers,
                topics,
                identity,
                fetch_bytes,
                encoding,
                props,
                ..
            } => {
                check_identity_declared(identity, file, &format!("source {:?}", s.id), errors);
                check_kafka_identity(identity, props, &format!("source {:?}", s.id), errors);
                check_encoding(encoding, &s.id, errors);
                validate_kafka_props(props, &format!("source {:?}", s.id), errors);
                check_fetch_bytes(*fetch_bytes, &s.id, errors);
                if blank(brokers) {
                    errors.at("brokers").push(format!("source {:?} has no brokers", s.id));
                }
                if topics.iter().all(|t| blank(t)) {
                    errors
                        .at("topics")
                        .push(format!("source {:?} subscribes to no topics", s.id));
                }
                for topic in topics {
                    if let Some(Err(e)) = crate::io::kafka::topic_pattern(topic) {
                        errors.at("topics").push(format!(
                            "source {:?} names the topic pattern {topic:?}, which does not compile: {e}",
                            s.id
                        ));
                    }
                }
            }
            SourceKindDto::EventHubs {
                namespace,
                event_hubs,
                consumer_group,
                connection_string,
                sign_in,
                fetch_bytes,
                encoding,
                ..
            } => {
                check_encoding(encoding, &s.id, errors);
                check_fetch_bytes(*fetch_bytes, &s.id, errors);
                check_event_hubs_namespace(namespace, &format!("source {:?}", s.id), errors);
                check_event_hubs_sign_in(
                    connection_string.as_ref(),
                    sign_in,
                    &format!("source {:?}", s.id),
                    errors,
                );
                if event_hubs.iter().all(|h| blank(h)) {
                    errors
                        .at("eventHubs")
                        .push(format!("source {:?} reads no event hubs", s.id));
                }
                for hub in event_hubs.iter().filter(|h| h.starts_with('^')) {
                    errors.at("eventHubs").push(format!(
                        "source {:?} names {hub:?}; an event hub is named in full, not by a pattern",
                        s.id
                    ));
                }
                if blank(consumer_group) {
                    errors.at("consumerGroup").push(format!(
                        "source {:?} has no consumer group -- it is the group every event hub above is read \
                         under",
                        s.id
                    ));
                }
            }
            SourceKindDto::Metrics { bucket_seconds } => {
                if *bucket_seconds == 0 {
                    errors.at("bucketSeconds").push(format!(
                        "source {:?} drains its bucket every 0 seconds; bucketSeconds is at least 1, since \
                         waiting out the bucket is what paces its reader",
                        s.id
                    ));
                }
            }
            SourceKindDto::Logs { min_level } => {
                if crate::logs::level_for_label(&min_level.0).is_none() {
                    errors.at("minLevel").push(format!(
                        "source {:?} carries logs from {:?} down; it must be one of {}",
                        s.id,
                        min_level.0,
                        crate::logs::LEVEL_LABELS.join(", ")
                    ));
                }
            }
            SourceKindDto::S3Sqs {
                queue_url,
                access,
                identity,
                encoding,
                framing,
                visibility_secs,
                ..
            } => {
                let what = format!("source {:?}", s.id);
                check_object_identity(identity, &s.urls(), file, &what, errors);
                check_s3_access(access, &what, errors);
                check_encoding(encoding, &s.id, errors);
                check_framing(framing, &s.id, errors);
                check_queue(queue_url, *visibility_secs, &what, errors);
            }
            SourceKindDto::S3List {
                bucket,
                prefix,
                access,
                identity,
                encoding,
                framing,
                poll_interval_secs,
                start,
                after_commit,
                move_prefix,
                ..
            } => {
                let what = format!("source {:?}", s.id);
                check_object_identity(identity, &s.urls(), file, &what, errors);
                check_s3_access(access, &what, errors);
                check_encoding(encoding, &s.id, errors);
                check_framing(framing, &s.id, errors);
                check_bucket_name(bucket, &what, errors);
                check_listing(
                    prefix,
                    *poll_interval_secs,
                    *start,
                    *after_commit,
                    move_prefix,
                    &what,
                    errors,
                );
            }
            SourceKindDto::GcsList {
                bucket,
                prefix,
                access,
                identity,
                encoding,
                framing,
                poll_interval_secs,
                start,
                after_commit,
                move_prefix,
                ..
            } => {
                let what = format!("source {:?}", s.id);
                check_object_identity(identity, &s.urls(), file, &what, errors);
                check_endpoint(access.endpoint.as_deref(), "endpoint", &what, errors);
                check_encoding(encoding, &s.id, errors);
                check_framing(framing, &s.id, errors);
                if bucket.trim().is_empty() {
                    errors.at("bucket").push(format!("{what} names no bucket"));
                }
                check_listing(
                    prefix,
                    *poll_interval_secs,
                    *start,
                    *after_commit,
                    move_prefix,
                    &what,
                    errors,
                );
            }
            SourceKindDto::AzureBlobList {
                container,
                prefix,
                access,
                identity,
                encoding,
                framing,
                poll_interval_secs,
                start,
                after_commit,
                move_prefix,
                ..
            } => {
                let what = format!("source {:?}", s.id);
                check_object_identity(identity, &s.urls(), file, &what, errors);
                check_azure_access(access, &what, errors);
                check_encoding(encoding, &s.id, errors);
                check_framing(framing, &s.id, errors);
                if container.trim().is_empty() {
                    errors.at("container").push(format!("{what} names no container"));
                }
                check_listing(
                    prefix,
                    *poll_interval_secs,
                    *start,
                    *after_commit,
                    move_prefix,
                    &what,
                    errors,
                );
            }
            SourceKindDto::Sftp {
                prefix,
                access,
                encoding,
                framing,
                poll_interval_secs,
                start,
                after_commit,
                move_prefix,
                ..
            } => {
                let what = format!("source {:?}", s.id);
                check_sftp_access(access, &what, errors);
                check_encoding(encoding, &s.id, errors);
                check_framing(framing, &s.id, errors);
                check_listing(
                    prefix,
                    *poll_interval_secs,
                    *start,
                    *after_commit,
                    move_prefix,
                    &what,
                    errors,
                );
            }
            SourceKindDto::HttpApi {
                url,
                method,
                body,
                headers,
                credentials,
                identity,
                records,
                records_at,
                since_param,
                since_overlap_secs,
                poll_interval_secs,
                start,
                sign_in,
                ..
            } => {
                let what = format!("source {:?}", s.id);
                check_api_source(&what, ("url", url), identity, *poll_interval_secs, file, errors);
                check_http_headers(&what, headers, credentials, SOURCE_HEADERS, "for every request", errors);
                check_app_sign_in(&what, sign_in, headers, credentials, errors);
                check_records_at(&what, *records, records_at, errors);
                if body.is_some() && *method != ApiMethodDto::Post {
                    errors
                        .at("body")
                        .push(format!("{what} sends a body only with a POST; set method to \"POST\""));
                }
                match since_param {
                    Some(param) if param.is_empty() || !param.bytes().all(is_query_name_byte) => {
                        errors.at("sinceParam").push(format!(
                            "{what} names the query parameter {param:?}; a name is letters, digits and -._~[] only"
                        ))
                    }
                    Some(param)
                        if reqwest::Url::parse(url.trim())
                            .is_ok_and(|u| u.query_pairs().any(|(name, _)| name == param.as_str())) =>
                    {
                        errors.at("sinceParam").push(format!(
                            "{what} asks for {param:?} in its URL already, and sinceParam adds it on every poll; \
                             take it out of the URL"
                        ))
                    }
                    Some(_) => {}
                    None => {
                        if *since_overlap_secs != 0 {
                            errors.at("sinceOverlapSecs").push(format!(
                                "{what} sets an overlap and no sinceParam; the overlap is how far back sinceParam \
                                 asks from"
                            ));
                        }
                        if *start != ListStartDto::Everything {
                            errors.at("start").push(format!(
                                "{what} starts at new records and has no sinceParam to ask for them with; with \
                                 none, every poll reads everything"
                            ));
                        }
                    }
                }
            }
            SourceKindDto::HttpReceiver {
                listener,
                path,
                tokens,
                records,
                records_at,
            } => {
                let what = format!("source {:?}", s.id);
                check_receiver(&what, listener, tokens, file, errors);
                check_records_at(&what, *records, records_at, errors);
                if !path.starts_with('/')
                    || path.contains(['?', '#', ':', '*'])
                    || path.chars().any(char::is_whitespace)
                {
                    errors.at("path").push(format!(
                        "{what} takes requests at {path:?}; a path starts with / and holds no ?, #, :, * or space"
                    ));
                } else if path == crate::io::receiver::HEALTH_PATH {
                    errors.at("path").push(format!(
                        "{what} takes requests at {path:?}, which is where it answers health checks; give it another \
                         path"
                    ));
                }
            }
            SourceKindDto::HecReceiver {
                listener,
                tokens,
                sourcetypes,
            } => {
                let what = format!("source {:?}", s.id);
                check_receiver(&what, listener, tokens, file, errors);
                check_sourcetypes(&what, sourcetypes, errors);
            }
            SourceKindDto::S2sReceiver {
                listener,
                tokens,
                sourcetypes,
            } => {
                let what = format!("source {:?}", s.id);
                check_receiver(&what, listener, tokens, file, errors);
                check_sourcetypes(&what, sourcetypes, errors);
            }
            SourceKindDto::OtlpReceiver { listener, tokens, .. }
            | SourceKindDto::PrometheusReceiver { listener, tokens }
            | SourceKindDto::ElasticReceiver { listener, tokens } => {
                check_receiver(&format!("source {:?}", s.id), listener, tokens, file, errors)
            }
            SourceKindDto::SyslogReceiver {
                listener,
                allow_from,
                encoding,
                transport,
                line_breaker,
            } => {
                let what = format!("source {:?}", s.id);
                check_syslog_receiver(&what, listener, allow_from, *transport, file, errors);
                check_encoding(encoding, &s.id, errors);
                if let Some(line_breaker) = line_breaker {
                    match transport {
                        Transport::Udp => errors.at("lineBreaker").push(format!(
                            "{what} sets lineBreaker, which only TCP reads: over UDP a datagram is a message"
                        )),
                        Transport::Tcp => check_line_breaker(&what, line_breaker, errors.at("lineBreaker")),
                    }
                }
            }
            SourceKindDto::Postgres(_) | SourceKindDto::MySql(_) => {
                let (kind, sql) = s.kind.sql().expect("an SQL source");
                let SqlSourceDto {
                    host,
                    port,
                    database,
                    user,
                    tls,
                    identity,
                    query,
                    mode,
                    fields,
                    poll_interval_secs,
                    batch,
                    ..
                } = sql;
                let what = format!("source {:?}", s.id);
                check_identity_declared(identity, file, &what, errors);
                if let (Some(name), SqlTlsDto::Disable) = (identity, tls) {
                    errors.at("identity").push(format!(
                        "{what} names the identity {name:?} and turns TLS off, so nothing the identity holds would be \
                         used. Set tls to \"require\", or take the identity off it"
                    ));
                }
                for (field, value, names) in [
                    ("host", host, "the database server's host name or address"),
                    ("database", database, "the database the query runs in"),
                    ("user", user, "the user it connects as"),
                ] {
                    if value.trim().is_empty() {
                        errors.at(field).push(format!("{what} names no {field}; give {names}"));
                    }
                }
                if *port == Some(0) {
                    errors
                        .at("port")
                        .push(format!("{what} connects to port 0; give the server's port"));
                }
                let read = SqlRead {
                    query,
                    mode,
                    fields,
                    poll_interval_secs: *poll_interval_secs,
                    batch: *batch,
                };
                read.check(&what, kind.dialect(), kind.meta(), errors);
            }
            SourceKindDto::OktaLogs {
                org,
                token,
                client_id,
                private_key,
                key_id,
                identity,
                poll_interval_secs,
                ..
            } => {
                let what = format!("source {:?}", s.id);
                let parsed = check_api_source(&what, ("org", org), identity, *poll_interval_secs, file, errors);
                if parsed.is_some_and(|u| u.path() != "/" || u.query().is_some() || u.fragment().is_some()) {
                    errors.at("org").push(format!(
                        "{what} names the org {org:?}; give its address alone, as https://acme.okta.com"
                    ));
                }
                let client_id = client_id.as_deref().filter(|c| !blank(c));
                let service_app = client_id.is_some() || private_key.is_some();
                match (token, service_app) {
                    (Some(_), true) => errors.at("token").push(format!(
                        "{what} names both a token and a service app; give the token, or clientId and privateKey"
                    )),
                    (None, false) => errors.at("token").push(format!(
                        "{what} names no sign-in; give a token, or a service app's clientId and privateKey"
                    )),
                    (None, true) => {
                        if client_id.is_none() {
                            errors.at("clientId").push(format!(
                                "{what} names a service app's private key and no client id; give its clientId"
                            ));
                        }
                        if private_key.is_none() {
                            errors.at("privateKey").push(format!(
                                "{what} names a service app's client id and no private key; give the credential \
                                 holding it as privateKey"
                            ));
                        }
                    }
                    (Some(_), false) => {}
                }
                if key_id.is_some() && !service_app {
                    errors.at("keyId").push(format!(
                        "{what} names a keyId with no service app; give it beside clientId and privateKey, or drop it"
                    ));
                }
            }
            SourceKindDto::AzureBlobQueue {
                queue_url,
                access,
                identity,
                encoding,
                framing,
                visibility_secs,
                max_deliveries,
                ..
            } => {
                let what = format!("source {:?}", s.id);
                check_object_identity(identity, &s.urls(), file, &what, errors);
                check_azure_access(access, &what, errors);
                check_encoding(encoding, &s.id, errors);
                check_framing(framing, &s.id, errors);
                check_queue(queue_url, *visibility_secs, &what, errors);
                let poison = crate::io::storage_queue::POISON_SUFFIX;
                let name = queue_url
                    .trim()
                    .trim_end_matches('/')
                    .rsplit('/')
                    .next()
                    .unwrap_or_default();
                if name.len() + poison.len() > QUEUE_NAME_MAX {
                    errors.at("queueUrl").push(format!(
                        "{what} reads the queue {name:?}, whose name leaves no room for its poison queue \
                         {name}{poison}. A queue's name is at most {QUEUE_NAME_MAX} characters, so this one may have \
                         at most {}",
                        QUEUE_NAME_MAX - poison.len()
                    ));
                }
                if !(MaxDeliveries::MIN..=MaxDeliveries::MAX).contains(&max_deliveries.0) {
                    errors.at("maxDeliveries").push(format!(
                        "{what} moves a message to the poison queue after {} deliveries; maxDeliveries must be \
                         between {} and {}",
                        max_deliveries.0,
                        MaxDeliveries::MIN,
                        MaxDeliveries::MAX
                    ));
                }
            }
            SourceKindDto::GcsPubSub {
                subscription,
                pubsub_endpoint,
                access,
                identity,
                encoding,
                framing,
                visibility_secs,
                ..
            } => {
                let what = format!("source {:?}", s.id);
                check_object_identity(identity, &s.urls(), file, &what, errors);
                check_encoding(encoding, &s.id, errors);
                check_framing(framing, &s.id, errors);
                check_subscription(subscription, &what, errors);
                check_endpoint(pubsub_endpoint.as_deref(), "pubsubEndpoint", &what, errors);
                check_endpoint(access.endpoint.as_deref(), "endpoint", &what, errors);
                check_visibility(*visibility_secs, ACK_DEADLINE_MAX, &what, errors);
            }
        }
    }

    for d in &file.destinations {
        let errors = &mut ItemErrors::new(attributed, "destinations", &d.id);
        match &d.kind {
            DestinationKindDto::Kafka {
                brokers,
                default_topic,
                identity,
                props,
                stall_after_secs,
                buffer_bytes,
                format,
                field,
                schema_registry,
                ..
            } => {
                check_field(
                    &d.id,
                    matches!(format, FormatDto::Text | FormatDto::Bytes),
                    field,
                    errors,
                );
                check_stream_avro(*format, schema_registry, &d.id, errors);
                check_stall_after(*stall_after_secs, &d.id, errors);
                validate_kafka_props(props, &format!("destination {:?}", d.id), errors);
                check_identity_declared(identity, file, &format!("destination {:?}", d.id), errors);
                check_kafka_identity(identity, props, &format!("destination {:?}", d.id), errors);
                check_kafka_buffer(*buffer_bytes, &d.id, errors);
                if blank(brokers) {
                    errors
                        .at("brokers")
                        .push(format!("destination {:?} has no brokers", d.id));
                }
                if blank(default_topic) {
                    errors
                        .at("defaultTopic")
                        .push(format!("destination {:?} has no default topic", d.id));
                }
            }
            DestinationKindDto::EventHubs {
                namespace,
                default_event_hub,
                format,
                field,
                compression,
                stall_after_secs,
                buffer_bytes,
                connection_string,
                sign_in,
                schema_registry,
                ..
            } => {
                check_field(
                    &d.id,
                    matches!(format, FormatDto::Text | FormatDto::Bytes),
                    field,
                    errors,
                );
                check_stream_avro(*format, schema_registry, &d.id, errors);
                check_stall_after(*stall_after_secs, &d.id, errors);
                check_kafka_buffer(*buffer_bytes, &d.id, errors);
                check_event_hubs_namespace(namespace, &format!("destination {:?}", d.id), errors);
                check_event_hubs_sign_in(
                    connection_string.as_ref(),
                    sign_in,
                    &format!("destination {:?}", d.id),
                    errors,
                );
                if blank(default_event_hub) {
                    errors
                        .at("defaultEventHub")
                        .push(format!("destination {:?} has no default event hub", d.id));
                }
                if !matches!(compression, CompressionDto::None | CompressionDto::Gzip) {
                    errors.at("compression").push(format!(
                        "destination {:?} compresses with {compression:?}; an Event Hubs destination takes only \
                         \"gzip\" or \"none\"",
                        d.id
                    ));
                }
            }
            DestinationKindDto::Elasticsearch {
                url,
                zone_urls,
                default_index,
                identity,
                in_flight,
                max_bulk_bytes,
                buffer_bytes,
                stall_after_secs,
                sniff,
                zone_attribute,
                ..
            } => {
                match zone_attribute {
                    Some(_) if !sniff => errors.at("zoneAttribute").push(format!(
                        "destination {:?} names a zone attribute but does not sniff; turn sniffing on, or take \
                         the zone attribute off",
                        d.id
                    )),
                    Some(a) if blank(a) => errors
                        .at("zoneAttribute")
                        .push(format!("destination {:?} names a blank zone attribute", d.id)),
                    _ => {}
                }
                check_http_delivery(
                    &HttpDelivery {
                        id: &d.id,
                        url,
                        url_field: "url",
                        zone_urls: Some(zone_urls),
                        request_field: "maxBulkBytes",
                        identity,
                        in_flight: Some(*in_flight),
                        request_bytes: max_bulk_bytes.0,
                        request_limits: (BulkBytes::MIN, BulkBytes::MAX),
                        request_what: "a bulk size",
                        limit_why: "Elasticsearch refuses a body over its own http.max_content_length, which \
                                    defaults to 100 MB",
                        buffer_bytes: *buffer_bytes,
                        stall_after: *stall_after_secs,
                    },
                    file,
                    errors,
                );
                if blank(default_index) {
                    errors
                        .at("defaultIndex")
                        .push(format!("destination {:?} has no default index", d.id));
                }
            }
            DestinationKindDto::SplunkHec {
                url,
                zone_urls,
                index,
                sourcetype,
                source,
                host,
                endpoint,
                format,
                field,
                identity,
                in_flight,
                max_request_bytes,
                buffer_bytes,
                stall_after_secs,
                ..
            } => {
                check_field(
                    &d.id,
                    matches!(format, FormatDto::Text | FormatDto::Bytes),
                    field,
                    errors,
                );
                refuse_avro(*format, &d.id, errors);
                // An event is a JSON string, so its text is UTF-8; only the raw endpoint takes bytes that may not be.
                if *format == FormatDto::Bytes && *endpoint == EndpointDto::Event {
                    errors.at("format").push(format!(
                        "destination {:?} writes format \"bytes\", which only the raw endpoint takes",
                        d.id
                    ));
                }
                check_http_delivery(
                    &HttpDelivery {
                        id: &d.id,
                        url,
                        url_field: "url",
                        zone_urls: Some(zone_urls),
                        request_field: "maxRequestBytes",
                        identity,
                        in_flight: Some(*in_flight),
                        request_bytes: max_request_bytes.0,
                        request_limits: (RequestBytes::MIN, RequestBytes::MAX),
                        request_what: "a request size",
                        limit_why: "Splunk refuses a body over its own max_content_length, and Splunk Cloud over \
                                    a megabyte",
                        buffer_bytes: *buffer_bytes,
                        stall_after: *stall_after_secs,
                    },
                    file,
                    errors,
                );
                // The event and ack paths are joined to the base address, so a pasted endpoint would get its path
                // twice and every request a 404.
                if let Some(at) = url.find("/services/collector") {
                    errors.at("url").push(format!(
                        "destination {:?} is reached at {url:?}, which names a collector endpoint. Give the base \
                         address, {:?}: the destination adds the endpoint itself",
                        d.id,
                        &url[..at]
                    ));
                }
                // Unset is the way to say "none"; an empty one is a field left half-filled.
                for (what, value) in [
                    ("index", index),
                    ("sourcetype", sourcetype),
                    ("source", source),
                    ("host", host),
                ] {
                    if value.as_deref().is_some_and(blank) {
                        errors.at(what).push(format!(
                            "destination {:?} has an empty {what}; name one, or leave it out",
                            d.id
                        ));
                    }
                }
            }
            DestinationKindDto::S3 {
                bucket,
                kms_key_id,
                access,
                objects,
                cloud,
            } => {
                let url = access.endpoint.as_deref().unwrap_or(S3_ENDPOINT);
                check_objects(&d.id, objects, url, &cloud.identity, file, errors);
                check_storage_class(&d.id, cloud, crate::io::objects::S3_STORAGE_CLASSES, errors);
                if blank(bucket) {
                    errors
                        .at("bucket")
                        .push(format!("destination {:?} names no bucket", d.id));
                }
                check_s3_access(access, &format!("destination {:?}", d.id), errors);
                if kms_key_id.as_deref().is_some_and(blank) {
                    errors.at("kmsKeyId").push(format!(
                        "destination {:?} has an empty kmsKeyId; name one, or leave it out",
                        d.id
                    ));
                }
            }
            DestinationKindDto::Gcs {
                bucket,
                access,
                objects,
                cloud,
            } => {
                let url = access.endpoint.as_deref().unwrap_or(GCS_ENDPOINT);
                check_objects(&d.id, objects, url, &cloud.identity, file, errors);
                check_storage_class(&d.id, cloud, crate::io::objects::GCS_STORAGE_CLASSES, errors);
                if blank(bucket) {
                    errors
                        .at("bucket")
                        .push(format!("destination {:?} names no bucket", d.id));
                }
                check_endpoint(
                    access.endpoint.as_deref(),
                    "endpoint",
                    &format!("destination {:?}", d.id),
                    errors,
                );
            }
            DestinationKindDto::AzureBlob {
                container,
                access,
                objects,
                cloud,
            } => {
                let url = access.endpoint.as_deref().unwrap_or(AZURE_ENDPOINT);
                check_objects(&d.id, objects, url, &cloud.identity, file, errors);
                check_storage_class(&d.id, cloud, crate::io::objects::AZURE_ACCESS_TIERS, errors);
                if blank(container) {
                    errors
                        .at("container")
                        .push(format!("destination {:?} names no container", d.id));
                }
                check_azure_access(access, &format!("destination {:?}", d.id), errors);
            }
            DestinationKindDto::Http {
                url,
                zone_urls,
                body: _,
                format,
                field,
                compression,
                headers,
                credentials,
                identity,
                in_flight,
                max_request_bytes,
                buffer_bytes,
                stall_after_secs,
            } => {
                check_http_delivery(
                    &HttpDelivery {
                        id: &d.id,
                        url,
                        url_field: "url",
                        zone_urls: Some(zone_urls),
                        request_field: "maxRequestBytes",
                        identity,
                        in_flight: Some(*in_flight),
                        request_bytes: max_request_bytes.0,
                        request_limits: (RequestBytes::MIN, RequestBytes::MAX),
                        request_what: "a request size",
                        limit_why: "a request is held whole in memory until the service answers it",
                        buffer_bytes: *buffer_bytes,
                        stall_after: *stall_after_secs,
                    },
                    file,
                    errors,
                );
                check_field(
                    &d.id,
                    matches!(format, FormatDto::Text | FormatDto::Bytes),
                    field,
                    errors,
                );
                refuse_avro(*format, &d.id, errors);
                if !matches!(compression, CompressionDto::None | CompressionDto::Gzip) {
                    errors.at("compression").push(format!(
                        "destination {:?} compresses with {compression:?}; an HTTP request is sent as none or gzip",
                        d.id
                    ));
                }
                check_http_headers(
                    &format!("destination {:?}", d.id),
                    headers,
                    credentials,
                    DESTINATION_HEADERS,
                    "from the body and compression",
                    errors,
                );
            }
            DestinationKindDto::Prometheus {
                url,
                version: _,
                headers,
                credentials,
                identity,
                max_request_bytes,
                buffer_bytes,
                stall_after_secs,
            } => {
                check_http_delivery(
                    &HttpDelivery {
                        id: &d.id,
                        url,
                        url_field: "url",
                        zone_urls: None,
                        request_field: "maxRequestBytes",
                        identity,
                        in_flight: None,
                        request_bytes: max_request_bytes.0,
                        request_limits: (RequestBytes::MIN, RequestBytes::MAX),
                        request_what: "a request size",
                        limit_why: "a request is held whole in memory until the store answers it",
                        buffer_bytes: *buffer_bytes,
                        stall_after: *stall_after_secs,
                    },
                    file,
                    errors,
                );
                check_http_headers(
                    &format!("destination {:?}", d.id),
                    headers,
                    credentials,
                    PROMETHEUS_HEADERS,
                    "as remote_write asks",
                    errors,
                );
            }
            DestinationKindDto::LogAnalytics {
                endpoint,
                rule_id,
                stream,
                sign_in,
                identity,
                in_flight,
                max_request_bytes,
                buffer_bytes,
                stall_after_secs,
            } => {
                check_http_delivery(
                    &HttpDelivery {
                        id: &d.id,
                        url: endpoint,
                        url_field: "endpoint",
                        zone_urls: None,
                        request_field: "maxRequestBytes",
                        identity,
                        in_flight: Some(*in_flight),
                        request_bytes: max_request_bytes.0,
                        request_limits: (RequestBytes::MIN, LOG_ANALYTICS_MAX_REQUEST_BYTES),
                        request_what: "a request size",
                        limit_why: "the Logs Ingestion API takes at most 1 MiB in one call",
                        buffer_bytes: *buffer_bytes,
                        stall_after: *stall_after_secs,
                    },
                    file,
                    errors,
                );
                check_log_analytics_rule(&d.id, endpoint, rule_id, stream, errors);
                check_entra_sign_in(sign_in, &[], &[], &format!("destination {:?}", d.id), errors);
            }
            DestinationKindDto::Sftp { access, objects, .. } => {
                check_objects(&d.id, objects, "sftp", &None, file, errors);
                if objects.stall_after_secs.0 < SFTP_MIN_STALL_AFTER {
                    errors.at("stallAfterSecs").push(format!(
                        "destination {:?} sets a stall threshold of {}s; an SFTP destination needs at least \
                         {SFTP_MIN_STALL_AFTER}s, or a slow sign-in or a dead connection outlasts every write",
                        d.id, objects.stall_after_secs.0
                    ));
                }
                check_sftp_access(access, &format!("destination {:?}", d.id), errors);
            }
            DestinationKindDto::Iceberg { .. } => check_iceberg(d, file, errors),
            DestinationKindDto::Delta { .. } => check_delta(d, file, errors),
        }
    }
    for p in &file.pipelines {
        eval_assignments(&p.steps, p, &blank, attributed);
    }
}

/// An eval assignment writes `target` from `expr`, so neither is optional: a blank target names a
/// column nothing can read back, and a blank expression has nothing to compute. Steps nest, so this
/// walks `if` branches too.
///
/// Attributed to the assignment's row and field, so the editor puts the message on the box that is blank.
fn eval_assignments(
    steps: &[StepDto],
    pipeline: &PipelineDto,
    blank: &impl Fn(&str) -> bool,
    errors: &mut Vec<ValidationError>,
) {
    for step in steps {
        match step {
            StepDto::If { then, r#else, .. } => {
                eval_assignments(then, pipeline, blank, errors);
                eval_assignments(r#else, pipeline, blank, errors);
            }
            StepDto::Eval { id, assignments, .. } => {
                for (index, a) in assignments.iter().enumerate() {
                    let at = |field, message| ValidationError {
                        pipeline: Some(pipeline.id.clone()),
                        step: Some(id.clone()),
                        assignment_index: Some(index as u32),
                        field: Some(field),
                        ..ValidationError::from(message)
                    };
                    if blank(&a.target) {
                        errors.push(at(
                            "target",
                            format!(
                                "pipeline {:?}, step {id:?}: an assignment has no target field. \
                                 Name the field it writes, or remove the assignment",
                                pipeline.name
                            ),
                        ));
                    }
                    if blank(&a.expr) {
                        // Naming a target that is itself blank would read as `assignment to ""`.
                        let which = match blank(&a.target) {
                            true => "an assignment".to_string(),
                            false => format!("the assignment to {:?}", a.target),
                        };
                        errors.push(at(
                            "expr",
                            format!(
                                "pipeline {:?}, step {id:?}: {which} has no expression. Give it \
                                 one, or remove the assignment",
                                pipeline.name
                            ),
                        ));
                    }
                }
            }
            _ => {}
        }
    }
}

/// The checks every object store destination shares: its delivery limits, prefix and format.
fn check_objects(
    id: &str,
    o: &ObjectSettingsDto,
    url: &str,
    identity: &Option<String>,
    file: &FileConfig,
    errors: &mut ItemErrors<'_>,
) {
    check_http_delivery(
        &HttpDelivery {
            id,
            url,
            url_field: "endpoint",
            zone_urls: None,
            request_field: "maxObjectBytes",
            identity,
            // A file's parts, not requests: checked below.
            in_flight: None,
            request_bytes: o.max_object_bytes.0,
            request_limits: (ObjectBytes::MIN, ObjectBytes::MAX),
            request_what: "an object size",
            limit_why: "a file is finished early at this size",
            // Bounded below, against the object's own limits: files upload in parts, so it need not hold one.
            buffer_bytes: None,
            stall_after: o.stall_after_secs,
        },
        file,
        errors,
    );
    check_file_limits(id, o.in_flight, o.buffer_bytes, errors);
    if let Err(why) = crate::io::objects::check_prefix(&o.prefix.0) {
        errors
            .at("prefix")
            .push(format!("destination {id:?} has the prefix {:?}: {why}", o.prefix.0));
    }
    check_field(
        id,
        matches!(o.format, ObjectFormatDto::Text | ObjectFormatDto::Bytes),
        &o.field,
        errors,
    );
    if o.newlines.is_some() && o.format != ObjectFormatDto::Bytes {
        errors.at("newlines").push(format!(
            "destination {id:?} names newlines, which only the \"bytes\" format reads"
        ));
    }
    if o.format == ObjectFormatDto::Avro && o.compression == CompressionDto::Lz4 {
        errors.at("compression").push(format!(
            "destination {id:?} writes Avro compressed with lz4, which Avro has no codec for; use \"gzip\", \"snappy\", \"zstd\" or \"none\""
        ));
    }
    check_file_interval(o.file_interval_secs, id, errors);
}

/// The buffer and upload concurrency of a destination that writes files: one that uploads them in parts, so its
/// buffer need not hold a whole file.
fn check_file_limits(id: &str, in_flight: InFlight, buffer_bytes: Option<BufferBytes>, errors: &mut ItemErrors<'_>) {
    if let Some(stated) = buffer_bytes
        && (stated.0 < BufferBytes::MIN || stated.0 > BufferBytes::OBJECT_MAX)
    {
        errors.at("bufferBytes").push(format!(
            "destination {id:?} lets egress hold {} for it; it must be between {} and {}. A file is finished once \
             the rows it holds reach 3/4 of this, so it sizes the files, and egress holds those rows until then",
            bytes::exact(stated.0),
            bytes::exact(BufferBytes::MIN),
            bytes::exact(BufferBytes::OBJECT_MAX)
        ));
    }
    if in_flight.0 == 0 || in_flight.0 > crate::plan::topology::MAX_SENDERS {
        errors.at("inFlight").push(format!(
            "destination {id:?} uploads {} parts of a file at once; it must be between 1 and {}",
            in_flight.0,
            crate::plan::topology::MAX_SENDERS
        ));
    }
}

/// The longest a table destination keeps its files open: the source waits that long for its batches'
/// acknowledgement, and a Kafka reader that waits past its `max.poll.interval.ms` (300 s) is evicted.
const TABLE_FILE_INTERVAL_MAX: u64 = 240;

/// Everything an Iceberg destination is checked for: its delivery limits, where its catalog is, how it signs in,
/// which table it writes and how a created one is partitioned.
fn check_iceberg(d: &DestinationDto, file: &FileConfig, errors: &mut ItemErrors<'_>) {
    let DestinationKindDto::Iceberg {
        catalog,
        warehouse,
        namespace,
        table,
        partition_by,
        token,
        client_id,
        client_secret,
        token_url,
        scope,
        signing_name,
        access,
        identity,
        compression: _,
        in_flight,
        max_object_bytes,
        buffer_bytes,
        stall_after_secs,
        file_interval_secs,
    } = &d.kind
    else {
        return;
    };
    let id = d.id.as_str();
    check_http_delivery(
        &HttpDelivery {
            id,
            url: catalog,
            url_field: "catalog",
            zone_urls: None,
            request_field: "maxObjectBytes",
            identity,
            // A file's parts, not requests: checked below.
            in_flight: None,
            request_bytes: max_object_bytes.0,
            request_limits: (ObjectBytes::MIN, ObjectBytes::MAX),
            request_what: "a data file size",
            limit_why: "a file is finished early at this size",
            // Bounded below, against a file's own limits: files upload in parts, so it need not hold one.
            buffer_bytes: None,
            stall_after: *stall_after_secs,
        },
        file,
        errors,
    );
    check_file_limits(id, *in_flight, *buffer_bytes, errors);
    let what = format!("destination {id:?}");
    let blank = |v: &str| v.trim().is_empty();
    if token_url.as_deref().is_some_and(blank) {
        errors
            .at("tokenUrl")
            .push(format!("{what} has an empty tokenUrl; name one, or leave it out"));
    }
    if token.is_some() && client_id.is_some() {
        errors.at("token").push(format!(
            "{what} names both a token and a clientId. Name a token, or clientId and clientSecret, not both"
        ));
    }
    if let Some(name) = signing_name
        && !blank(name)
        && !SIGNING_NAMES.contains(&name.trim())
    {
        errors.at("signingName").push(format!(
            "{what} signs its catalog calls for {name:?}; the signing service is {}",
            SIGNING_NAMES.join(" or ")
        ));
    }
    if signing_name.is_some() && access.region.is_none() && crate::io::iceberg::table::signing_region(catalog).is_none()
    {
        errors.at("region").push(format!(
            "{what} signs its catalog calls with AWS signatures, and its catalog's address {catalog:?} names no AWS \
             region. Name the region the catalog is in"
        ));
    }
    if signing_name.is_some() && (token.is_some() || client_id.is_some()) {
        errors.at("signingName").push(format!(
            "{what} names a signingName beside a token or clientId. A catalog signed in to with AWS signatures takes \
             neither"
        ));
    }
    if client_id.is_some() != client_secret.is_some() {
        let missing = if client_id.is_none() {
            "clientId"
        } else {
            "clientSecret"
        };
        errors.at(missing).push(format!(
            "{what} names half of its client credentials. Name both clientId and clientSecret, or neither"
        ));
    }
    if (token_url.is_some() || scope.is_some()) && client_id.is_none() {
        let field = if token_url.is_some() { "tokenUrl" } else { "scope" };
        errors.at(field).push(format!(
            "{what} has a tokenUrl or scope and no clientId; both serve signing in with client credentials"
        ));
    }
    for (field, value) in [
        ("warehouse", warehouse),
        ("clientId", client_id),
        ("scope", scope),
        ("signingName", signing_name),
    ] {
        if value.as_deref().is_some_and(blank) {
            errors
                .at(field)
                .push(format!("{what} has an empty {field}; name one, or leave it out"));
        }
    }
    if blank(namespace) {
        errors.at("namespace").push(format!("{what} names no namespace"));
    } else if namespace.trim().split('.').any(blank) {
        errors.at("namespace").push(format!(
            "{what} names the namespace {namespace:?}, which has an empty level. Join its levels with single dots, \
             as logs.web"
        ));
    }
    if blank(table) {
        errors.at("table").push(format!("{what} names no table"));
    }
    let mut names = std::collections::HashSet::new();
    for term in partition_by {
        match crate::io::iceberg::transform::partition_term(term) {
            Err(why) => errors
                .at("partitionBy")
                .push(format!("{what} has a partitionBy term it cannot read: {why}")),
            // Iceberg names each partition field once: a catalog refuses to create a table with a second of a name.
            Ok((transform, path)) => {
                let name = crate::io::iceberg::transform::partition_name(&transform, &path);
                if names.contains(&name) {
                    errors.at("partitionBy").push(format!(
                        "{what} has two partitionBy terms naming the partition field {name:?}; keep one"
                    ));
                }
                names.insert(name);
            }
        }
    }
    if !access.is_empty() {
        check_s3_access(access, &what, errors);
    }
    check_table_file_interval(*file_interval_secs, &what, errors);
}

/// A table destination's files stay open no longer than a Kafka reader may wait for their acknowledgement.
fn check_table_file_interval(file_interval: FileInterval, what: &str, errors: &mut ItemErrors<'_>) {
    if file_interval.0 < FileInterval::MIN || file_interval.0 > TABLE_FILE_INTERVAL_MAX {
        errors.at("fileIntervalSecs").push(format!(
            "{what} keeps a file open for {}s; a table destination's must be between {} and \
             {TABLE_FILE_INTERVAL_MAX}. Its batches are acknowledged when the files are committed, and a Kafka \
             reader that waits past its 300s poll interval is evicted from its group",
            file_interval.0,
            FileInterval::MIN,
        ));
    }
}

/// Everything a Delta destination is checked for: its delivery limits, how it reaches its bucket, where its table is
/// and how a created one is partitioned.
fn check_delta(d: &DestinationDto, file: &FileConfig, errors: &mut ItemErrors<'_>) {
    let DestinationKindDto::Delta {
        location,
        unity_catalog,
        partition_by,
        access,
        identity,
        compression: _,
        in_flight,
        max_object_bytes,
        buffer_bytes,
        stall_after_secs,
        file_interval_secs,
    } = &d.kind
    else {
        return;
    };
    let id = d.id.as_str();
    check_http_delivery(
        &HttpDelivery {
            id,
            url: access.endpoint.as_deref().unwrap_or(S3_ENDPOINT),
            url_field: "endpoint",
            zone_urls: None,
            request_field: "maxObjectBytes",
            identity,
            // A file's parts, not requests: checked below.
            in_flight: None,
            request_bytes: max_object_bytes.0,
            request_limits: (ObjectBytes::MIN, ObjectBytes::MAX),
            request_what: "a data file size",
            limit_why: "a file is finished early at this size",
            // Bounded below, against a file's own limits: files upload in parts, so it need not hold one.
            buffer_bytes: None,
            stall_after: *stall_after_secs,
        },
        file,
        errors,
    );
    check_file_limits(id, *in_flight, *buffer_bytes, errors);
    let what = format!("destination {id:?}");
    check_s3_access(access, &what, errors);
    match (location, unity_catalog) {
        (Some(_), Some(_)) => errors.at("location").push(format!(
            "{what} names both a location and a unityCatalog table. Name the table's root in S3 as location, or its \
             Unity Catalog table as unityCatalog, not both"
        )),
        (None, None) => errors.at("location").push(format!(
            "{what} names no table. Name its root in S3 as location, as s3://bucket/path, or its Unity Catalog table \
             as unityCatalog"
        )),
        (Some(location), None) => {
            let bucket = ["s3://", "s3a://"]
                .iter()
                .find_map(|scheme| location.trim().strip_prefix(scheme))
                .map(|rest| rest.split('/').next().unwrap_or_default());
            if bucket.is_none_or(str::is_empty) {
                errors.at("location").push(format!(
                    "{what} keeps its table at {location:?}; give the table's root in S3, as s3://bucket/path"
                ));
            }
        }
        (None, Some(uc)) => check_unity_catalog(uc, access, &what, errors),
    }
    // Delta partitions by top-level columns, by their values, so a term is a column's name and nothing more.
    let mut seen = HashSet::new();
    for column in partition_by {
        if column.trim().is_empty() || column.contains(['.', '(', ')', ',']) {
            errors.at("partitionBy").push(format!(
                "{what} partitions by {column:?}; a Delta table is partitioned by top-level columns, so name one \
                 column, as host"
            ));
        } else if !seen.insert(column) {
            errors
                .at("partitionBy")
                .push(format!("{what} partitions by {column:?} twice; name each column once"));
        }
    }
    check_table_file_interval(*file_interval_secs, &what, errors);
}

/// Refuses a Unity Catalog table that cannot be found or signed in to as written.
fn check_unity_catalog(uc: &UnityCatalogDto, access: &S3AccessDto, what: &str, errors: &mut ItemErrors<'_>) {
    let blank = |v: &str| v.trim().is_empty();
    let workspace = &uc.workspace;
    // Every API is asked under the workspace's root, so a URL copied from a browser, with a path or `?o=`, is no base.
    let root = |u: &reqwest::Url| u.path() == "/" && u.query().is_none() && u.fragment().is_none();
    if !reqwest::Url::parse(workspace.trim())
        .is_ok_and(|u| matches!(u.scheme(), "http" | "https") && u.has_host() && root(&u))
    {
        errors.at("unityCatalog.workspace").push(format!(
            "{what} reaches its Unity Catalog workspace at {workspace:?}; give the workspace's https:// URL and \
             nothing after its host, as https://dbc-1234.cloud.databricks.com"
        ));
    }
    if access.region.as_deref().is_none_or(blank) {
        errors.at("region").push(format!(
            "{what} names no region beside a unityCatalog table; name the bucket's region, as us-west-2"
        ));
    }
    let table = &uc.table;
    let parts: Vec<&str> = table.trim().split('.').collect();
    if parts.len() != 3 || parts.iter().copied().any(blank) {
        errors.at("unityCatalog.table").push(format!(
            "{what} names the Unity Catalog table {table:?}; name it by its three parts, as catalog.schema.table"
        ));
    }
    if blank(&uc.client_id) {
        errors.at("unityCatalog.clientId").push(format!(
            "{what} names no clientId; name the OAuth client id of the service principal it signs in as"
        ));
    }
    // Unity Catalog vends the credentials for the table's storage, so storage credentials of its own go unused.
    let own = [
        ("accessKeyId", access.access_key_id.is_some()),
        ("secretAccessKey", access.secret_access_key.is_some()),
        ("roleArn", access.role_arn.is_some()),
        ("authEndpoint", access.auth_endpoint.is_some()),
    ];
    for (field, _) in own.into_iter().filter(|(_, set)| *set) {
        errors.at(field).push(format!(
            "{what} names {field} beside a unityCatalog table. Unity Catalog gives the credentials for the table's \
             storage; take {field} out"
        ));
    }
}

/// The services an Iceberg catalog signed with AWS signatures answers as: Glue's Iceberg endpoint, and S3 Tables.
/// Another name signs for a service that is no catalog, and AWS signs `s3` unlike the rest.
const SIGNING_NAMES: [&str; 2] = ["glue", "s3tables"];

/// A source waits up to the interval for its batches' acknowledgement, and after a crash reads that much again.
fn check_file_interval(t: FileInterval, id: &str, errors: &mut ItemErrors<'_>) {
    if t.0 < FileInterval::MIN || t.0 > FileInterval::MAX {
        errors.at("fileIntervalSecs").push(format!(
            "destination {id:?} keeps a file open for {}s; it must be between {} and {}. \
             Its batches are acknowledged when the file is finished, so a source waits up to this long for \
             them, and after a crash reads that much again",
            t.0,
            FileInterval::MIN,
            FileInterval::MAX
        ));
    }
}

/// A cloud destination's storage class is one of `classes`, the cloud's own.
fn check_storage_class(id: &str, cloud: &CloudSettingsDto, classes: &[&str], errors: &mut ItemErrors<'_>) {
    match &cloud.storage_class {
        Some(class) if class.trim().is_empty() => errors.at("storageClass").push(format!(
            "destination {id:?} has an empty storageClass; name one, or leave it out"
        )),
        Some(class) if !classes.contains(&class.as_str()) => errors.at("storageClass").push(format!(
            "destination {id:?} has the storage class {class:?}, which is none of {}",
            classes.join(", ")
        )),
        _ => {}
    }
}

/// A destination's `field` is read only by formats `"text"` and `"bytes"`, and names a field when it is there.
fn check_field(id: &str, reads_field: bool, field: &Option<String>, errors: &mut ItemErrors<'_>) {
    match field {
        Some(_) if !reads_field => errors.at("field").push(format!(
            "destination {id:?} names a field, which only the \"text\" and \"bytes\" formats read"
        )),
        Some(field) if field.trim().is_empty() => errors.at("field").push(format!(
            "destination {id:?} has an empty field; name one, or leave it out"
        )),
        _ => {}
    }
}

/// Refuses a stream destination's Avro without a registry, and a registry where it writes no Avro.
fn check_stream_avro(
    format: FormatDto,
    registry: &Option<crate::config_file::dto::SchemaRegistryDto>,
    id: &str,
    errors: &mut ItemErrors<'_>,
) {
    let what = format!("destination {id:?}");
    match (format == FormatDto::Avro, registry) {
        (true, None) => errors.at("schemaRegistry.url").push(format!(
            "{what} writes Avro, which a stream frames with its schema's id in a schema registry, but names none"
        )),
        (false, Some(_)) => errors.at("schemaRegistry").push(format!(
            "{what} names a schema registry but writes no Avro; set format to \"avro\""
        )),
        (true, Some(registry)) => check_registry(registry, &what, errors),
        (false, None) => {}
    }
}

/// Refuses Avro from a destination whose requests carry text, not a stream's records or an object's file.
fn refuse_avro(format: FormatDto, id: &str, errors: &mut ItemErrors<'_>) {
    if format == FormatDto::Avro {
        errors.at("format").push(format!(
            "destination {id:?} writes format \"avro\", which only a stream or an object destination writes"
        ));
    }
}

/// What a Kafka, Splunk or HTTP destination's `format` writes of each row.
fn to_payload(format: FormatDto, field: &Option<String>) -> Payload {
    match format {
        // An Avro destination writes its record by its own encoder, which `KafkaDestinationConfig::avro` holds.
        FormatDto::Json | FormatDto::Avro => Payload::Record,
        FormatDto::Text => Payload::Text {
            field: text_field(field).into(),
        },
        FormatDto::Bytes => Payload::Bytes {
            field: bytes_field(field).into(),
        },
    }
}

/// The field format `"text"` writes: `_raw` unless one is named.
fn text_field(field: &Option<String>) -> String {
    field.clone().unwrap_or_else(|| "_raw".to_string())
}

/// The field format `"bytes"` writes: `__source.bytes` unless one is named.
fn bytes_field(field: &Option<String>) -> String {
    field
        .clone()
        .unwrap_or_else(|| crate::plan::topology::SOURCE_BYTES.to_string())
}

/// `url` as parsed, so what goes on the wire is percent-encoded as the save checked it.
fn as_parsed(url: &str) -> String {
    reqwest::Url::parse(url.trim()).map_or_else(|_| url.trim().to_string(), String::from)
}

/// The delivery settings every destination over `io::http` shares, and the words its own limits are stated in.
struct HttpDelivery<'a> {
    id: &'a str,
    url: &'a str,
    /// The name the file gives `url`.
    url_field: &'static str,
    /// Zone -> the address a worker there sends to instead, checked as `url` is.
    zone_urls: Option<&'a BTreeMap<String, String>>,
    /// The name the file gives `request_bytes`.
    request_field: &'static str,
    identity: &'a Option<String>,
    /// Requests the destination starts with outstanding; `None` checks none.
    in_flight: Option<InFlight>,
    request_bytes: usize,
    request_limits: (usize, usize),
    request_what: &'a str,
    limit_why: &'a str,
    buffer_bytes: Option<BufferBytes>,
    stall_after: StallAfter,
}

fn check_http_delivery(d: &HttpDelivery<'_>, file: &FileConfig, errors: &mut ItemErrors<'_>) {
    let id = d.id;
    if d.url.trim().is_empty() {
        errors.at(d.url_field).push(format!("destination {id:?} has no URL"));
    }
    check_identity_declared(d.identity, file, &format!("destination {id:?}"), errors);
    let zone_urls = d.zone_urls.into_iter().flatten();
    for (zone, url) in zone_urls.clone() {
        if zone.trim().is_empty() {
            errors
                .at("zoneUrls")
                .push(format!("destination {id:?} names a zone by a blank name"));
        } else if url.trim().is_empty() {
            errors
                .at("zoneUrls")
                .push(format!("destination {id:?} has no URL for the zone {zone:?}"));
        }
    }
    // http:// never negotiates TLS, so an identity on one is configuration nothing reads.
    let addresses = std::iter::once((d.url_field, d.url)).chain(zone_urls.map(|(_, url)| ("zoneUrls", url.as_str())));
    for (field, url) in addresses {
        if let Some(name) = d.identity
            && !url.trim().to_ascii_lowercase().starts_with("https://")
        {
            errors.at(field).push(format!(
                "destination {id:?} names the identity {name:?} and is reached over {url:?}, \
                 which does not negotiate TLS -- nothing the identity holds would be used. \
                 Give it an https:// URL, or take the identity off it"
            ));
        }
    }
    check_stall_after(d.stall_after, id, errors);
    let (min, max) = d.request_limits;
    if d.request_bytes < min || d.request_bytes > max {
        errors.at(d.request_field).push(format!(
            "destination {id:?} sets {} of {}; it must be between {} and {}. {}",
            d.request_what,
            bytes::exact(d.request_bytes),
            bytes::exact(min),
            bytes::exact(max),
            d.limit_why
        ));
    }
    if let Some(stated) = d.buffer_bytes {
        if stated.0 < BufferBytes::MIN || stated.0 > BufferBytes::MAX {
            errors.at("bufferBytes").push(format!(
                "destination {id:?} lets egress hold {} for it; it must be between \
                 {} and {}. Egress holds a batch from acceptance until the service \
                 answers for it, so this bounds both the queue and the requests \
                 outstanding against it",
                bytes::exact(stated.0),
                bytes::exact(BufferBytes::MIN),
                bytes::exact(BufferBytes::MAX)
            ));
        }
        if stated.0 < d.request_bytes {
            errors.at("bufferBytes").push(format!(
                "destination {id:?} lets egress hold {} but sends requests of up to \
                 {}. The buffer must fit a whole request, or there is never room to \
                 hand one over",
                bytes::exact(stated.0),
                bytes::exact(d.request_bytes)
            ));
        }
    }
    let Some(in_flight) = d.in_flight else {
        return;
    };
    let affords = affords(d.buffer_bytes, d.request_bytes);
    if in_flight.0 == 0 || in_flight.0 > crate::plan::topology::MAX_SENDERS {
        errors.at("inFlight").push(format!(
            "destination {id:?} starts at {} concurrent requests; it must be between 1 and \
             {}. Each one is a sender thread holding its own connection, and egress runs \
             one thread per destination, so this is what one worker shows the service",
            in_flight.0,
            crate::plan::topology::MAX_SENDERS
        ));
    } else if in_flight.start(affords) > affords {
        errors.at("inFlight").push(format!(
            "destination {id:?} starts at {} concurrent requests but its buffer affords {}. \
             The buffer holds a request from acceptance until the service answers for it, \
             so a start above what it affords can never be reached",
            in_flight.0, affords
        ));
    }
}

/// Refuses a `recordsAt` that is not a JSON pointer, or that names an array a source set to `body` or `lines` never
/// reads.
fn check_records_at(what: &str, records: ApiRecordsDto, records_at: &Option<String>, errors: &mut ItemErrors<'_>) {
    match (records, records_at) {
        (ApiRecordsDto::Array, Some(at)) if !at.is_empty() && !at.starts_with('/') => errors.at("recordsAt").push(
            format!("{what} reads its records at {at:?}, which is not a JSON pointer; one starts with /, as /data"),
        ),
        (ApiRecordsDto::Body | ApiRecordsDto::Lines, Some(_)) => errors.at("recordsAt").push(format!(
            "{what} says where an array of records is, and does not read an array; set records to \"array\" or \
             leave recordsAt out"
        )),
        _ => {}
    }
}

/// What a receiver must be before it can listen: a port, an address that is one, an identity that holds a
/// certificate to serve, and someone it admits. With neither a token nor an identity whose authorities vouch for a
/// sender, anyone who can reach the port could send.
fn check_receiver<T>(
    what: &str,
    listener: &ReceiverSettingsDto,
    tokens: &BTreeMap<String, T>,
    file: &FileConfig,
    errors: &mut ItemErrors<'_>,
) {
    let vouches = check_listener(what, listener, file, errors);
    if tokens.is_empty() && !vouches {
        errors.at("tokens").push(format!(
            "{what} admits anyone who can reach its port; give it a token, or an identity whose authorities a \
             sender's certificate must chain to"
        ));
    }
    for name in tokens.keys() {
        if name.trim().is_empty() {
            errors
                .at("tokens")
                .push(format!("{what} names a token by a blank name"));
        }
    }
}

/// A syslog receiver as [`check_receiver`] holds any, with the networks it admits senders from in place of tokens,
/// as syslog carries none. Over UDP it serves no TLS, so only its networks can admit a sender.
fn check_syslog_receiver(
    what: &str,
    listener: &ReceiverSettingsDto,
    allow_from: &[String],
    transport: Transport,
    file: &FileConfig,
    errors: &mut ItemErrors<'_>,
) {
    let vouches = check_listener(what, listener, file, errors);
    if transport == Transport::Udp && listener.identity.is_some() {
        errors.at("identity").push(format!(
            "{what} takes UDP, which it serves without TLS; leave out its identity, or take TCP to serve TLS"
        ));
    }
    let vouches = vouches && transport == Transport::Tcp;
    if allow_from.is_empty() && !vouches {
        errors.at("allowFrom").push(format!(
            "{what} admits anyone who can reach its port; give it the networks its senders connect from, or an \
             identity whose authorities a sender's certificate must chain to"
        ));
    }
    for block in allow_from {
        if block.trim().parse::<ipnetwork::IpNetwork>().is_err() {
            errors.at("allowFrom").push(format!(
                "{what} admits senders from {block:?}, which is not a network; give an address or a CIDR block, as \
                 10.0.0.0/8"
            ));
        }
    }
}

/// What every receiver's listener must be: a port, and an identity that holds a certificate to serve. Whether that
/// identity's authorities vouch for a sender, which then needs nothing else to be admitted.
fn check_listener(what: &str, listener: &ReceiverSettingsDto, file: &FileConfig, errors: &mut ItemErrors<'_>) -> bool {
    let ReceiverSettingsDto {
        port,
        identity,
        max_request_bytes,
    } = listener;
    let blank = |v: &str| v.trim().is_empty();
    if *port == 0 {
        errors
            .at("port")
            .push(format!("{what} listens on port 0; give the port senders connect to"));
    }
    check_identity_declared(identity, file, what, errors);
    let declared = identity
        .as_ref()
        .and_then(|name| file.identities.iter().find(|i| &i.id == name));
    match declared.and_then(|i| i.certificate.as_deref()).filter(|c| !blank(c)) {
        None if declared.is_some() => errors.at("identity").push(format!(
            "{what} serves TLS under the identity {identity:?}, which holds no certificate; a receiver presents one \
             to every sender"
        )),
        Some(certificate) => {
            // One that is not a certificate is reported where the identity is declared.
            if refuse_unless_certificates(certificate).is_ok()
                && let Err(why) = refuse_unless_usable(certificate, Presented::AsServer)
            {
                errors.at("identity").push(format!(
                    "{what} serves TLS under the identity {identity:?}, which presents a certificate {why}"
                ));
            }
        }
        None => {}
    }
    if !(MaxRequestBytes::MIN..=MaxRequestBytes::MAX).contains(&max_request_bytes.0) {
        errors.at("maxRequestBytes").push(format!(
            "{what} reads requests of up to {} bytes; maxRequestBytes is between {} and {}",
            max_request_bytes.0,
            MaxRequestBytes::MIN,
            MaxRequestBytes::MAX
        ));
    }
    declared.is_some_and(|i| i.authorities.as_deref().is_some_and(|a| !blank(a)))
}

/// Refuses two receivers on one port: every worker of the group binds each, and the second bind would fail on every
/// one of them. OTLP receivers of different signals share one, as an exporter sends every signal to one port, so they
/// serve it alike: on one address, under one identity. A TCP port and a UDP port are two ports, whatever their number.
fn check_receiver_ports(file: &FileConfig, attributed: &mut Vec<ValidationError>) {
    let mut by_port: std::collections::BTreeMap<(Transport, u16), Vec<&SourceDto>> = std::collections::BTreeMap::new();
    for s in &file.sources {
        let Some(port) = s.kind.receiver_port().filter(|p| *p != 0) else {
            continue;
        };
        let on_port = by_port.entry((s.kind.receiver_transport(), port)).or_default();
        let why = match (on_port.first(), &s.kind) {
            (None, _) => None,
            (
                Some(
                    first @ SourceDto {
                        kind: SourceKindDto::OtlpReceiver { listener: theirs, .. },
                        ..
                    },
                ),
                SourceKindDto::OtlpReceiver { listener, signal, .. },
            ) => {
                let same = |o: &SourceDto| matches!(&o.kind, SourceKindDto::OtlpReceiver { signal: other, .. } if other == signal);
                if let Some(taken) = on_port.iter().find(|o| same(o)) {
                    Some(format!(
                        "source {:?} takes {} on port {port}, which source {:?} already does; an export of \
                         one signal reaches one receiver, so give it another port",
                        s.id,
                        signal.name(),
                        taken.id
                    ))
                } else if listener.identity != theirs.identity {
                    Some(format!(
                        "source {:?} shares port {port} with source {:?}, which serves it; give it the same \
                         identity certificate, or another port",
                        s.id, first.id
                    ))
                } else {
                    None
                }
            }
            (Some(first), _) => Some(format!(
                "source {:?} listens on port {port}, which source {:?} already does; every worker binds both, so \
                 give each its own port",
                s.id, first.id
            )),
        };
        if let Some(why) = why {
            ItemErrors::new(attributed, "sources", &s.id).at("port").push(why);
        }
        on_port.push(s);
    }
}

/// What an SQL source reads, whatever its database: the settings every SQL kind shares.
struct SqlRead<'a> {
    query: &'a str,
    mode: &'a SqlModeDto,
    fields: &'a [SqlFieldDto],
    poll_interval_secs: PollIntervalSecs,
    batch: SqlBatch,
}

/// The value `startFrom` names, in a position whose first column is `ty`: an integer for an i64, an ISO 8601 time
/// with its offset for a datetime.
fn sql_start_value(value: &serde_json::Value, ty: &DataType) -> Result<crate::io::values::Value, String> {
    use crate::io::values::Value;
    match ty {
        DataType::I64 => value
            .as_i64()
            .map(Value::Int)
            .ok_or_else(|| format!("{value} is not an integer, and the position's first column is an i64")),
        DataType::DateTime => value
            .as_str()
            .and_then(|text| {
                ["%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z"]
                    .iter()
                    .find_map(|fmt| crate::ops::timeparse::parse_with_format(text, fmt))
            })
            .map(Value::Time)
            .ok_or_else(|| {
                format!(
                    "{value} is not a time like \"2026-01-31T09:00:00Z\", and the position's first column is a datetime"
                )
            }),
        other => Err(format!("the position's first column is {other:?}, which has no start")),
    }
}

/// How long a position whose first column is a time waits for late commits when its source names no wait.
const SQL_SETTLE_SECS: u32 = 30;

impl SqlRead<'_> {
    /// Refuses a query `dialect` would not run as a source's, fields that declare no record, and a mode the fields
    /// cannot hold.
    fn check(
        &self,
        what: &str,
        dialect: crate::io::sql::Dialect,
        kind: crate::plan::topology::SourceMeta,
        errors: &mut ItemErrors<'_>,
    ) {
        let query = match crate::io::sql::Query::parse(self.query, dialect) {
            Ok(query) => Some(query),
            Err(why) => {
                errors.at("query").push(format!("{what}'s query: {why}"));
                None
            }
        };
        if let Err(why) = SqlFieldDto::meta(kind, self.fields) {
            errors.at("fields").push(format!("{what}: {why}"));
        }
        let types: HashMap<&str, Option<DataType>> = self
            .fields
            .iter()
            .map(|f| (f.name.as_str(), crate::expr::parse::parse_type(&f.ty).ok()))
            .collect();
        if self.poll_interval_secs.0 == 0 {
            errors
                .at("pollIntervalSecs")
                .push(format!("{what} polls every 0 seconds; pollIntervalSecs is at least 1"));
        }
        if !(1..=SqlBatch::MAX).contains(&self.batch.0) {
            errors.at("batch").push(format!(
                "{what} reads {} rows a poll; batch is between 1 and {}",
                self.batch.0,
                SqlBatch::MAX
            ));
        }
        // A mode reads fields, so it is checked once there are some. Run reads a query that declares none yet.
        if self.fields.is_empty() {
            return;
        }
        let named = |column: &str| types.contains_key(column);
        match self.mode {
            SqlModeDto::Incrementing {
                position,
                settle_secs,
                start,
                start_from,
            }
            | SqlModeDto::UpdatedAt {
                position,
                settle_secs,
                start,
                start_from,
            } => {
                let updated_at = matches!(self.mode, SqlModeDto::UpdatedAt { .. });
                let Some(first) = position.first() else {
                    errors.at("mode").push(format!(
                        "{what} names no position; give the column rows are read in the order of"
                    ));
                    return;
                };
                for (i, column) in position.iter().enumerate() {
                    if position[..i].contains(column) {
                        errors
                            .at("mode")
                            .push(format!("{what} names `{column}` twice in its position"));
                    } else if !named(column) {
                        errors.at("mode").push(format!(
                            "{what}'s position names `{column}`, which is not one of its fields; a position is read \
                             from the record"
                        ));
                    }
                }
                let first_type = types.get(first.as_str()).cloned().flatten();
                match (&first_type, updated_at) {
                    (Some(DataType::DateTime), _) | (Some(DataType::I64), false) | (None, _) => {}
                    (Some(_), false) => errors.at("mode").push(format!(
                        "{what} reads by `{first}`, which is neither an i64 nor a datetime; a position is a number \
                         or a time that only grows"
                    )),
                    (Some(_), true) => errors.at("mode").push(format!(
                        "{what} reads by updated at `{first}`, which is not a datetime; give the time every insert \
                         and update sets"
                    )),
                }
                for column in &position[1..] {
                    if let Some(Some(ty)) = types.get(column.as_str())
                        && !matches!(ty, DataType::I64 | DataType::DateTime | DataType::Str)
                    {
                        errors.at("mode").push(format!(
                            "{what} tells rows apart by `{column}`, which is not an i64, a datetime or a str"
                        ));
                    }
                }
                if settle_secs.is_some() && first_type.as_ref().is_some_and(|t| *t != DataType::DateTime) {
                    errors.at("mode").push(format!(
                        "{what} waits for late commits by `{first}`, which is not a time; only a time can wait. \
                         Take settleSecs off"
                    ));
                }
                match (start, start_from) {
                    (SqlStartDto::From, None) => errors
                        .at("mode")
                        .push(format!("{what} starts from a value it does not give; give startFrom")),
                    (SqlStartDto::From, Some(value)) => {
                        if let Some(ty) = &first_type
                            && let Err(why) = sql_start_value(value, ty)
                        {
                            errors.at("mode").push(format!("{what}'s startFrom: {why}"));
                        }
                    }
                    (SqlStartDto::Now | SqlStartDto::Beginning, Some(_)) => errors.at("mode").push(format!(
                        "{what} gives startFrom but does not start from it; take startFrom off, or set start to from"
                    )),
                    (SqlStartDto::Now | SqlStartDto::Beginning, None) => {}
                }
                if let Some(query) = &query {
                    for column in query.last_columns() {
                        if !named(column) {
                            errors.at("query").push(format!(
                                "{what}'s query names :last.{column}, and `{column}` is not one of its fields; :last \
                                 is the value a field held in the last row sent"
                            ));
                        }
                    }
                }
            }
            SqlModeDto::Comparing { key } => {
                if key.is_empty() {
                    errors.at("mode").push(format!(
                        "{what} compares values by no key; give the columns that tell each row apart"
                    ));
                }
                for column in key.iter().filter(|c| !named(c)) {
                    errors
                        .at("mode")
                        .push(format!("{what}'s key names `{column}`, which is not one of its fields"));
                }
            }
            SqlModeDto::EveryPoll => {}
        }
        if let (Some(query), SqlModeDto::Comparing { .. } | SqlModeDto::EveryPoll) = (&query, self.mode)
            && let Some(column) = query.last_columns().first()
        {
            errors.at("query").push(format!(
                "{what}'s query names :last.{column}; only a mode that reads past the last row sent has one"
            ));
        }
    }

    /// The source `check` passed. `database` names where it connects, as its place begins; `topic` is what each
    /// record's `__source.database` reads.
    fn config(
        &self,
        dialect: crate::io::sql::Dialect,
        kind: crate::plan::topology::SourceMeta,
        database: &str,
        topic: String,
        connection: crate::plan::topology::SqlDatabase,
    ) -> crate::plan::topology::SqlConfig {
        use crate::io::sql_source::{Mode, Position, Start};
        let query = crate::io::sql::Query::parse(self.query, dialect)
            .unwrap_or_else(|why| unreachable!("validation refuses a query that does not parse: {why}"));
        let fields: Vec<(String, DataType)> = self
            .fields
            .iter()
            .map(|f| {
                let ty = crate::expr::parse::parse_type(&f.ty)
                    .unwrap_or_else(|why| unreachable!("validation refuses a type that does not parse: {why:?}"));
                (f.name.clone(), ty)
            })
            .collect();
        let position =
            |columns: &[String], settle_secs: Option<u32>, start: SqlStartDto, from: Option<&serde_json::Value>| {
                let first = fields
                    .iter()
                    .find(|(name, _)| Some(name) == columns.first())
                    .map(|(_, ty)| ty);
                let time = first == Some(&DataType::DateTime);
                let start = match (start, from, first) {
                    (SqlStartDto::Now, ..) => Start::Now,
                    (SqlStartDto::Beginning, ..) => Start::Beginning,
                    (SqlStartDto::From, Some(value), Some(ty)) => {
                        Start::From(sql_start_value(value, ty).unwrap_or_else(|why| {
                            unreachable!("validation refuses a startFrom that does not read: {why}")
                        }))
                    }
                    (SqlStartDto::From, ..) => unreachable!("validation refuses a start from no value"),
                };
                Position {
                    start,
                    columns: columns.to_vec(),
                    settle_secs: if time {
                        Some(settle_secs.unwrap_or(SQL_SETTLE_SECS))
                    } else {
                        None
                    },
                }
            };
        let (mode, by) = match self.mode {
            SqlModeDto::Incrementing {
                position: p,
                settle_secs,
                start,
                start_from,
            } => (
                Mode::Incrementing(position(p, *settle_secs, *start, start_from.as_ref())),
                format!("incrementing:{}", p.join(",")),
            ),
            SqlModeDto::UpdatedAt {
                position: p,
                settle_secs,
                start,
                start_from,
            } => (
                Mode::UpdatedAt(position(p, *settle_secs, *start, start_from.as_ref())),
                format!("updatedAt:{}", p.join(",")),
            ),
            SqlModeDto::Comparing { key } => (
                Mode::Comparing { key: key.clone() },
                format!("comparing:{}", key.join(",")),
            ),
            SqlModeDto::EveryPoll => (Mode::EveryPoll, "everyPoll".to_string()),
        };
        // The query and the columns its mode remembers by decide what the stored progress means, so either changing
        // starts afresh. A digest keeps the place short whatever the query's length.
        let digest = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, format!("{by}\n{}", self.query).as_bytes());
        let digest: String = digest.as_ref()[..8].iter().map(|b| format!("{b:02x}")).collect();
        let meta = SqlFieldDto::meta(kind, self.fields)
            .unwrap_or_else(|why| unreachable!("validation refuses fields that declare no record: {why}"));
        crate::plan::topology::SqlConfig {
            database: connection,
            source: crate::io::sql_source::SqlSourceConfig {
                query,
                mode,
                fields: fields.into(),
                poll_interval: Duration::from_secs(self.poll_interval_secs.0),
                batch: self.batch.0,
                place: topic.into(),
            },
            meta,
            place: format!("{database}#{digest}"),
        }
    }
}

/// What every API source's address and schedule must be: a URL at `field`, an identity only where TLS would use it,
/// and a poll interval of at least a second. The URL, when it is a whole http:// or https:// one.
fn check_api_source(
    what: &str,
    (field, url): (&'static str, &str),
    identity: &Option<String>,
    poll_interval: PollIntervalSecs,
    file: &FileConfig,
    errors: &mut ItemErrors<'_>,
) -> Option<reqwest::Url> {
    check_identity_declared(identity, file, what, errors);
    // http:// never negotiates TLS, so an identity on one is configuration nothing reads.
    if let Some(name) = identity
        && !url.trim().to_ascii_lowercase().starts_with("https://")
    {
        errors.at(field).push(format!(
            "{what} names the identity {name:?} and asks {url:?}, which does not negotiate TLS -- nothing the \
             identity holds would be used. Give it an https:// URL, or take the identity off it"
        ));
    }
    if poll_interval.0 == 0 {
        errors
            .at("pollIntervalSecs")
            .push(format!("{what} asks every 0 seconds; pollIntervalSecs is at least 1"));
    }
    if url.trim().is_empty() {
        errors.at(field).push(format!("{what} names no {field}"));
        return None;
    }
    // The source's URLs are refused once, with every other URL field's.
    let parsed = web_url(url)?;
    Some(parsed)
}

/// An app's sign-in, once [`check_app_sign_in`] passed it. `None` from one naming nothing.
fn to_app_sign_in(dto: &AppSignInDto) -> Option<crate::plan::topology::AppSignIn> {
    use crate::plan::topology::{AppProof, AppSignIn, TokenAt};
    let given = |v: &Option<String>| v.as_deref().map(str::trim).filter(|v| !v.is_empty()).map(String::from);
    let token_at = match (given(&dto.token_url), given(&dto.issuer)) {
        (Some(url), _) => TokenAt::Url(url),
        (None, Some(issuer)) => TokenAt::Issuer(issuer),
        (None, None) => return None,
    };
    let proof = match (&dto.client_secret, &dto.private_key) {
        (Some(secret), _) => AppProof::Secret(to_secret_ref(secret)),
        (None, Some(key)) => AppProof::Key {
            private_key: to_secret_ref(key),
            key_id: given(&dto.key_id),
        },
        (None, None) => return None,
    };
    Some(AppSignIn {
        client_id: given(&dto.client_id)?,
        token_at,
        scope: given(&dto.scope),
        proof,
    })
}

/// That an app's sign-in names an app, one proof and one place to sign in, and that nothing else sets the header
/// its token goes in.
fn check_app_sign_in(
    what: &str,
    sign_in: &AppSignInDto,
    headers: &BTreeMap<String, String>,
    credentials: &BTreeMap<String, SecretRefDto>,
    errors: &mut ItemErrors<'_>,
) {
    if sign_in.is_empty() {
        return;
    }
    let given = |v: &Option<String>| v.as_deref().is_some_and(|v| !v.trim().is_empty());
    if !given(&sign_in.client_id) {
        errors.at("clientId").push(format!(
            "{what} names part of an app's sign-in and no clientId; give the app's clientId, or drop the rest"
        ));
    }
    match (&sign_in.client_secret, &sign_in.private_key) {
        (Some(_), Some(_)) => errors.at("clientSecret").push(format!(
            "{what} names both a clientSecret and a privateKey; give the one the app signs in with"
        )),
        (None, None) => errors.at("clientSecret").push(format!(
            "{what} signs in as an app and names no clientSecret or privateKey; give the one the app signs in with"
        )),
        _ => {}
    }
    if sign_in.key_id.is_some() && sign_in.private_key.is_none() {
        errors.at("keyId").push(format!(
            "{what} names a keyId with no privateKey; give it beside privateKey, or drop it"
        ));
    }
    match (given(&sign_in.token_url), given(&sign_in.issuer)) {
        (true, true) => errors.at("tokenUrl").push(format!(
            "{what} names both a tokenUrl and an issuer; give the token endpoint, or the issuer that names it"
        )),
        (false, false) => errors.at("tokenUrl").push(format!(
            "{what} signs in as an app and names no tokenUrl or issuer; give the token endpoint, or the issuer \
             that names it"
        )),
        _ => {}
    }
    // Plain HTTP to a token endpoint loads, as every address a credential goes to does. An issuer's discovery is
    // held to the leader's own login rule, which takes plain HTTP on this host alone.
    if let Some(url) = sign_in
        .token_url
        .as_deref()
        .map(str::trim)
        .filter(|u| !plain_off_this_host(u))
        && let Err(e) = crate::io::discovery::check_url(url, true)
    {
        errors.at("tokenUrl").push(format!("{what} signs in at {e}"));
    }
    if let Some(issuer) = sign_in.issuer.as_deref().map(str::trim)
        && let Err(e) = crate::io::discovery::check_issuer(issuer)
    {
        errors.at("issuer").push(format!("{what} names the issuer {e}"));
    }
    if let Some((map, name)) = (headers.keys().map(|k| ("headers", k)))
        .chain(credentials.keys().map(|k| ("credentials", k)))
        .find(|(_, name)| name.eq_ignore_ascii_case("authorization"))
    {
        errors.at(map).push(format!(
            "{what} sets the header {name:?} and signs in as an app, whose token goes there; give one"
        ));
    }
}

/// Headers smeltr sets on every request an HTTP destination sends, by their lowercase names: a second value would
/// contradict it.
const DESTINATION_HEADERS: &[&str] = &[
    "content-type",
    "content-encoding",
    "content-length",
    "transfer-encoding",
    "host",
    "cookie",
];

/// What a Prometheus destination sets itself: an HTTP destination's, and the remote_write version.
const PROMETHEUS_HEADERS: &[&str] = &{
    let mut owned = [""; DESTINATION_HEADERS.len() + 1];
    let mut at = 0;
    while at < DESTINATION_HEADERS.len() {
        owned[at] = DESTINATION_HEADERS[at];
        at += 1;
    }
    owned[at] = "x-prometheus-remote-write-version";
    owned
};

/// Headers smeltr sets on every request an API source sends, by their lowercase names.
const SOURCE_HEADERS: &[&str] = &["content-length", "transfer-encoding", "host"];

/// An HTTP item's headers: each a name HTTP allows, set once across both maps, none of the `owned` ones smeltr sets
/// itself `owned_why`, a committed value holding no line break.
fn check_http_headers(
    what: &str,
    headers: &BTreeMap<String, String>,
    credentials: &BTreeMap<String, SecretRefDto>,
    owned: &[&str],
    owned_why: &str,
    errors: &mut ItemErrors<'_>,
) {
    let names = (headers.keys().map(|k| ("headers", k))).chain(credentials.keys().map(|k| ("credentials", k)));
    let mut seen: HashSet<String> = HashSet::new();
    for (map, name) in names {
        let lower = name.to_ascii_lowercase();
        if name.is_empty() || !name.bytes().all(is_token_byte) {
            errors.at(map).push(format!(
                "{what} names the header {name:?}, which is not a header name: letters, digits and \
                 !#$%&'*+-.^_`|~ only"
            ));
        } else if owned.contains(&lower.as_str()) {
            errors.at(map).push(format!(
                "{what} sets the header {name:?}, which smeltr sets itself {owned_why}"
            ));
        } else if !seen.insert(lower) {
            // Header names are case-insensitive, so two spellings are one header with two values.
            errors
                .at(map)
                .push(format!("{what} sets the header {name:?} twice; give it one value"));
        }
    }
    for (name, value) in headers {
        if value.chars().any(|c| c.is_control() && c != '\t') {
            errors.at("headers").push(format!(
                "{what} gives the header {name:?} a value with a line break or control character"
            ));
        }
    }
}

/// A byte a query parameter's name may hold without being escaped.
fn is_query_name_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b"-._~[]".contains(&b)
}

/// A byte RFC 9110 allows in a header name.
fn is_token_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b)
}

/// How many requests of `request_bytes` the buffer holds at once.
fn affords(buffer_bytes: Option<BufferBytes>, request_bytes: usize) -> usize {
    effective_buffer_bytes(buffer_bytes, request_bytes) / request_bytes.max(1)
}

/// What egress may hold for an HTTP destination. Unset takes the default, or one whole request if that is larger:
/// raising the request size alone must never leave a destination unable to hand a request over.
fn effective_buffer_bytes(stated: Option<BufferBytes>, request_bytes: usize) -> usize {
    match stated {
        Some(b) => b.0,
        None => BufferBytes::DEFAULT.max(request_bytes),
    }
}

fn to_objects(
    cloud: Cloud,
    o: &ObjectSettingsDto,
    settings: &CloudSettingsDto,
    identities: &Identities,
) -> DestinationKind {
    DestinationKind::Objects(ObjectsDestinationConfig {
        cloud,
        prefix: o.prefix.0.clone(),
        format: match o.format {
            ObjectFormatDto::Ndjson => ObjectFormat::Ndjson,
            ObjectFormatDto::Json => ObjectFormat::Json,
            ObjectFormatDto::Parquet => ObjectFormat::Parquet,
            ObjectFormatDto::Avro => ObjectFormat::Avro,
            ObjectFormatDto::Csv => ObjectFormat::Csv,
            ObjectFormatDto::Bytes => ObjectFormat::Bytes {
                field: bytes_field(&o.field).into(),
                newlines: match o.newlines {
                    None | Some(NewlinesDto::Escape) => Newlines::Escape,
                    Some(NewlinesDto::Refuse) => Newlines::Refuse,
                },
            },
            ObjectFormatDto::Text => ObjectFormat::Text {
                field: text_field(&o.field).into(),
            },
        },
        compression: o.compression.to_plan(),
        storage_class: settings.storage_class.clone(),
        identity: named_identity(&settings.identity, identities),
        in_flight: o.in_flight.0,
        max_object_bytes: o.max_object_bytes.0,
        buffer_bytes: o.buffer_bytes.map_or(BufferBytes::OBJECT_DEFAULT, |b| b.0),
        stall_after: std::time::Duration::from_secs(o.stall_after_secs.0),
        file_interval: std::time::Duration::from_secs(o.file_interval_secs.0),
    })
}

/// `dto` with the built-in answer to `Refused`, which the caller lays the group's and its own over.
fn to_destination(dto: &DestinationDto, id: DestId, identities: &Identities) -> DestinationConfig {
    let kind = match &dto.kind {
        DestinationKindDto::Kafka {
            brokers,
            default_topic,
            identity,
            props,
            compression,
            stall_after_secs,
            buffer_bytes,
            format,
            field,
            schema_registry,
        } => DestinationKind::Kafka(KafkaDestinationConfig {
            payload: to_payload(*format, field),
            identity: named_identity(identity, identities),
            brokers: brokers.clone(),
            props: to_kafka_props(props),
            default_topic: default_topic.clone(),
            compression: compression.to_plan(),
            stall_after: std::time::Duration::from_secs(stall_after_secs.0),
            buffer_bytes: buffer_bytes.map_or(BufferBytes::DEFAULT, |b| b.0),
            service: KafkaService::Kafka,
            avro: avro_write(*format, schema_registry),
        }),
        DestinationKindDto::EventHubs {
            namespace,
            default_event_hub,
            connection_string,
            sign_in,
            format,
            field,
            compression,
            stall_after_secs,
            buffer_bytes,
            schema_registry,
        } => {
            let brokers = event_hubs_brokers(namespace);
            DestinationKind::Kafka(KafkaDestinationConfig {
                payload: to_payload(*format, field),
                identity: None,
                props: event_hubs_props(&brokers, connection_string.as_ref(), sign_in),
                brokers,
                default_topic: default_event_hub.clone(),
                compression: compression.to_plan(),
                stall_after: std::time::Duration::from_secs(stall_after_secs.0),
                buffer_bytes: buffer_bytes.map_or(BufferBytes::DEFAULT, |b| b.0),
                service: KafkaService::EventHubs,
                avro: avro_write(*format, schema_registry),
            })
        }
        DestinationKindDto::Elasticsearch {
            url,
            zone_urls,
            default_index,
            identity,
            auth,
            doc_id,
            id_field,
            in_flight,
            max_bulk_bytes,
            buffer_bytes,
            stall_after_secs,
            sniff,
            zone_attribute,
        } => DestinationKind::Elasticsearch(crate::plan::topology::ElasticDestinationConfig {
            sniff: sniff.then(|| crate::plan::topology::Sniff {
                zone_attribute: zone_attribute
                    .as_deref()
                    .unwrap_or(crate::plan::topology::Sniff::DEFAULT_ZONE_ATTRIBUTE)
                    .trim()
                    .to_string(),
                zone: None,
            }),
            identity: named_identity(identity, identities),
            url: url.clone(),
            zone_urls: zone_urls.clone(),
            default_index: default_index.clone(),
            auth: auth.as_ref().map(to_secret_ref),
            doc_id: match doc_id {
                crate::config_file::dto::DocIdPolicyDto::Auto => DocIdPolicy::Auto,
                crate::config_file::dto::DocIdPolicyDto::Dedupe => DocIdPolicy::Dedupe,
                crate::config_file::dto::DocIdPolicyDto::Overwrite => DocIdPolicy::Overwrite,
            },
            id_field: id_field.clone(),
            in_flight: in_flight.start(affords(*buffer_bytes, max_bulk_bytes.0)),
            max_bulk_bytes: max_bulk_bytes.0,
            buffer_bytes: effective_buffer_bytes(*buffer_bytes, max_bulk_bytes.0),
            stall_after: std::time::Duration::from_secs(stall_after_secs.0),
        }),
        DestinationKindDto::SplunkHec {
            url,
            zone_urls,
            token,
            index,
            sourcetype,
            source,
            host,
            endpoint,
            format,
            field,
            acknowledgement,
            identity,
            in_flight,
            max_request_bytes,
            buffer_bytes,
            stall_after_secs,
        } => DestinationKind::SplunkHec(SplunkHecDestinationConfig {
            payload: to_payload(*format, field),
            url: url.clone(),
            zone_urls: zone_urls.clone(),
            token: to_secret_ref(token),
            index: index.clone(),
            sourcetype: sourcetype.clone(),
            source: source.clone(),
            host: host.clone(),
            endpoint: match endpoint {
                EndpointDto::Event => Endpoint::Event,
                EndpointDto::Raw => Endpoint::Raw,
            },
            acknowledgement: match acknowledgement {
                AcknowledgementDto::Indexer => Acknowledgement::Indexer,
                AcknowledgementDto::Receipt => Acknowledgement::Receipt,
            },
            identity: named_identity(identity, identities),
            in_flight: in_flight.start(affords(*buffer_bytes, max_request_bytes.0)),
            max_request_bytes: max_request_bytes.0,
            buffer_bytes: effective_buffer_bytes(*buffer_bytes, max_request_bytes.0),
            stall_after: std::time::Duration::from_secs(stall_after_secs.0),
        }),
        DestinationKindDto::S3 {
            bucket,
            kms_key_id,
            access,
            objects,
            cloud,
        } => to_objects(
            Cloud::S3(S3Store {
                bucket: bucket.clone(),
                kms_key_id: kms_key_id.clone(),
                access: to_s3_access(access),
            }),
            objects,
            cloud,
            identities,
        ),
        DestinationKindDto::Gcs {
            bucket,
            access,
            objects,
            cloud,
        } => to_objects(
            Cloud::Gcs(GcsStore {
                bucket: bucket.clone(),
                access: to_gcs_access(access),
            }),
            objects,
            cloud,
            identities,
        ),
        DestinationKindDto::AzureBlob {
            container,
            access,
            objects,
            cloud,
        } => to_objects(
            Cloud::Azure(AzureStore {
                container: container.clone(),
                access: to_azure_access(access),
            }),
            objects,
            cloud,
            identities,
        ),
        DestinationKindDto::Http {
            url,
            zone_urls,
            body,
            format,
            field,
            compression,
            headers,
            credentials,
            identity,
            in_flight,
            max_request_bytes,
            buffer_bytes,
            stall_after_secs,
        } => DestinationKind::Http(HttpDestinationConfig {
            url: as_parsed(url),
            zone_urls: zone_urls
                .iter()
                .map(|(zone, url)| (zone.clone(), as_parsed(url)))
                .collect(),
            body: match body {
                RequestBodyDto::Lines => RequestBody::Lines,
                RequestBodyDto::JsonArray => RequestBody::JsonArray,
                RequestBodyDto::Single => RequestBody::Single,
            },
            payload: to_payload(*format, field),
            compression: compression.to_plan(),
            headers: headers.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
            credentials: credentials.iter().map(|(k, r)| (k.clone(), to_secret_ref(r))).collect(),
            identity: named_identity(identity, identities),
            in_flight: in_flight.start(affords(*buffer_bytes, max_request_bytes.0)),
            max_request_bytes: max_request_bytes.0,
            buffer_bytes: effective_buffer_bytes(*buffer_bytes, max_request_bytes.0),
            stall_after: std::time::Duration::from_secs(stall_after_secs.0),
        }),
        DestinationKindDto::Prometheus {
            url,
            version,
            headers,
            credentials,
            identity,
            max_request_bytes,
            buffer_bytes,
            stall_after_secs,
        } => DestinationKind::Prometheus(PrometheusDestinationConfig {
            url: url.trim().to_string(),
            version: match version {
                RemoteWriteVersionDto::V1 => RemoteWriteVersion::V1,
                RemoteWriteVersionDto::V2 => RemoteWriteVersion::V2,
            },
            headers: headers.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
            credentials: credentials.iter().map(|(k, r)| (k.clone(), to_secret_ref(r))).collect(),
            identity: named_identity(identity, identities),
            max_request_bytes: max_request_bytes.0,
            buffer_bytes: effective_buffer_bytes(*buffer_bytes, max_request_bytes.0),
            stall_after: std::time::Duration::from_secs(stall_after_secs.0),
        }),
        DestinationKindDto::LogAnalytics {
            endpoint,
            rule_id,
            stream,
            sign_in,
            identity,
            in_flight,
            max_request_bytes,
            buffer_bytes,
            stall_after_secs,
        } => DestinationKind::LogAnalytics(LogAnalyticsDestinationConfig {
            http: HttpDestinationConfig {
                url: log_analytics_stream_url(endpoint, rule_id, stream),
                zone_urls: BTreeMap::new(),
                body: RequestBody::JsonArray,
                payload: Payload::Record,
                compression: crate::plan::topology::Compression::Gzip,
                headers: Vec::new(),
                credentials: Vec::new(),
                identity: named_identity(identity, identities),
                in_flight: in_flight.start(affords(*buffer_bytes, max_request_bytes.0)),
                max_request_bytes: max_request_bytes.0,
                buffer_bytes: effective_buffer_bytes(*buffer_bytes, max_request_bytes.0),
                stall_after: std::time::Duration::from_secs(stall_after_secs.0),
            },
            sign_in: to_entra_sign_in(sign_in),
        }),
        DestinationKindDto::Sftp {
            directory,
            access,
            objects,
        } => to_objects(
            Cloud::Sftp(SftpStore {
                directory: sftp_directory(directory),
                access: to_sftp_access(access),
            }),
            objects,
            // An SFTP server has no storage classes, and is trusted by its host key.
            &CloudSettingsDto::default(),
            identities,
        ),
        DestinationKindDto::Iceberg {
            catalog,
            warehouse,
            namespace,
            table,
            partition_by,
            token,
            client_id,
            client_secret,
            token_url,
            scope,
            signing_name,
            access,
            identity,
            compression,
            in_flight,
            max_object_bytes,
            buffer_bytes,
            stall_after_secs,
            file_interval_secs,
        } => DestinationKind::Iceberg(IcebergDestinationConfig {
            catalog: catalog.trim().to_string(),
            warehouse: warehouse.as_ref().map(|w| w.trim().to_string()),
            sign_in: match (token, client_id, client_secret) {
                // Naming it beside a token or a client's credentials is refused where the fields are checked.
                _ if signing_name.is_some() => CatalogSignIn::SigV4 {
                    signing_name: signing_name.as_deref().unwrap_or_default().trim().to_string(),
                },
                (Some(token), _, _) => CatalogSignIn::Bearer(to_secret_ref(token)),
                (None, Some(client_id), Some(client_secret)) => CatalogSignIn::ClientCredentials {
                    token_url: token_url.as_ref().map(|u| u.trim().to_string()),
                    client_id: client_id.trim().to_string(),
                    client_secret: to_secret_ref(client_secret),
                    scope: scope.as_ref().map(|s| s.trim().to_string()),
                },
                // Half of a client's credentials is refused where the fields are checked.
                _ => CatalogSignIn::None,
            },
            namespace: namespace
                .trim()
                .split('.')
                .map(|level| level.trim().to_string())
                .collect(),
            table: table.trim().to_string(),
            partition_by: partition_by.clone(),
            storage: (!access.is_empty()).then(|| to_s3_access(access)),
            compression: compression.to_plan(),
            identity: named_identity(identity, identities),
            in_flight: in_flight.0,
            max_object_bytes: max_object_bytes.0,
            buffer_bytes: buffer_bytes.map_or(BufferBytes::OBJECT_DEFAULT, |b| b.0),
            stall_after: std::time::Duration::from_secs(stall_after_secs.0),
            file_interval: std::time::Duration::from_secs(file_interval_secs.0),
        }),
        DestinationKindDto::Delta {
            location,
            unity_catalog,
            partition_by,
            access,
            identity,
            compression,
            in_flight,
            max_object_bytes,
            buffer_bytes,
            stall_after_secs,
            file_interval_secs,
        } => DestinationKind::Delta(DeltaDestinationConfig {
            // Naming both, or neither, is refused where the fields are checked.
            location: location.as_deref().map(str::trim).unwrap_or_default().to_string(),
            unity_catalog: unity_catalog.as_ref().map(|u| UnityCatalogTable {
                workspace: u.workspace.trim().to_string(),
                table: u.table.trim().to_string(),
                client_id: u.client_id.trim().to_string(),
                client_secret: to_secret_ref(&u.client_secret),
            }),
            partition_by: partition_by.clone(),
            storage: to_s3_access(access),
            compression: compression.to_plan(),
            identity: named_identity(identity, identities),
            in_flight: in_flight.0,
            max_object_bytes: max_object_bytes.0,
            buffer_bytes: buffer_bytes.map_or(BufferBytes::OBJECT_DEFAULT, |b| b.0),
            stall_after: std::time::Duration::from_secs(stall_after_secs.0),
            file_interval: std::time::Duration::from_secs(file_interval_secs.0),
        }),
    };
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

fn to_route(dto: &RouteDto, maps: &IdMaps, group: &OnException) -> Result<RouteConfig, String> {
    let source = *maps
        .sources
        .get(&dto.source)
        .ok_or_else(|| format!("route {:?} references unknown source {:?}", dto.id, dto.source))?;
    let pipeline = *maps
        .pipelines
        .get(&dto.pipeline)
        .ok_or_else(|| format!("route {:?} references unknown pipeline {:?}", dto.id, dto.pipeline))?;
    let deliver = dto
        .deliver
        .iter()
        .map(|rule| {
            let to = rule
                .to
                .iter()
                .map(|d| {
                    maps.destinations.get(d).copied().ok_or_else(|| {
                        format!(
                            "route {:?}'s deliver rule references unknown destination {:?}",
                            dto.id, d
                        )
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
        lay_answers(&mut on_exception, layer, Layer::Route, maps).map_err(|e| format!("route {:?}'s {e}", dto.id))?;
    }

    Ok(RouteConfig {
        id: dto.id.clone(),
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
        ParserKindDto::Csv { delimiter, split_dots } => {
            let (headers, kept) = to_csv_columns(declared)?;
            let config = CsvConfig {
                split_dots: split_dots.unwrap_or(split_dots_default("csv")),
                delimiter: to_byte(delimiter, "csv delimiter")?,
                headers,
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
        } => ParserKind::KeyValue(KvConfig {
            split_dots: split_dots.unwrap_or(split_dots_default("kv")),
            next_key: next_key.unwrap_or(KvConfig::default().next_key),
            pair_sep: to_byte(pair_sep, "keyValue pairSep")?,
            kv_sep: to_byte(kv_sep, "keyValue kvSep")?,
            quote: quote.as_deref().map(|q| to_byte(q, "keyValue quote")).transpose()?,
            ..KvConfig::default()
        }),
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
    maps: &IdMaps,
) -> Result<PipelineConfig, String> {
    let in_pipeline = |e: String| format!("pipeline {:?}: {e}", dto.name);
    let mut next_function_id: FunctionId = 0;
    let mut held_step_ids = HashSet::new();
    let mut step_ids = Vec::new();
    for call_id in call_ids {
        hold_step_id(call_id, &mut held_step_ids).map_err(in_pipeline)?;
    }
    let body = to_steps(steps, &mut next_function_id, &mut step_ids, &mut held_step_ids, maps).map_err(in_pipeline)?;
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
        | StepDto::Parser { id, .. }
        | StepDto::Lookup { id, .. }
        | StepDto::Unroll { id, .. }
        | StepDto::Call { id, .. } => id,
    }
}

fn to_steps(
    steps: &[StepDto],
    next_id: &mut FunctionId,
    step_ids: &mut Vec<String>,
    held_step_ids: &mut HashSet<String>,
    maps: &IdMaps,
) -> Result<Vec<Step>, String> {
    steps
        .iter()
        .map(|s| to_step(s, next_id, step_ids, held_step_ids, maps))
        .collect()
}

/// One `FunctionId` per step, in document order, local to this pipeline. An `if` takes its id before its branches.
/// `step_ids` grows in step: its index after each push is the `FunctionId` just assigned, so `step_ids[f]` is the
/// step that owns `FunctionId f`.
fn to_step(
    dto: &StepDto,
    next_id: &mut FunctionId,
    step_ids: &mut Vec<String>,
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
                id: claim_step_id(id, next_id, step_ids, held_step_ids)?,
                cond: condition.clone(),
                then: to_steps(then, next_id, step_ids, held_step_ids, maps)?,
                else_: to_steps(r#else, next_id, step_ids, held_step_ids, maps)?,
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
        StepDto::Parser {
            id,
            filter,
            disabled,
            input,
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

    let id = claim_step_id(step_id, next_id, step_ids, held_step_ids)?;
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
    next_id: &mut FunctionId,
    step_ids: &mut Vec<String>,
    held_step_ids: &mut HashSet<String>,
) -> Result<FunctionId, String> {
    hold_step_id(step_id, held_step_ids)?;
    let id = *next_id;
    *next_id += 1;
    step_ids.push(step_id.to_string());
    Ok(id)
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
        assert!(matches!(connection.tls, crate::io::sql_source::Tls::Require));
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
                .with_encoding(Encoding::Fixed(encoding_rs::UTF_8))
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
            (r#","tls":"disable","identity":"ghost""#, "identity", "turns TLS off"),
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
        crate::plan::plan::compile(&system.pipelines[0], &crate::plan::topology::SourceMeta::KAFKA)
            .unwrap_or_else(|e| panic!("expected the inlined pipeline to compile: {e:?}"));
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
                .any(|e| e.contains("sasl.kerberos.kinit.cmd") && e.contains("system()")),
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
                .any(|e| e.contains("auto.commit.enable") && e.contains("enable.auto.commit")),
            "{errors:?}"
        );
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
            "batch.size",
            "message.max.bytes",
            "linger.ms",
            "fetch.max.bytes",
            "fetch.message.max.bytes",
            "receive.message.max.bytes",
            "batch.num.messages",
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

    /// The one property smeltr reserves without setting: leaving librdkafka's default in place is itself the
    /// decision, and an operator lowering it would bound the producer queue somewhere `bufferBytes` cannot see.
    #[test]
    fn the_message_count_bound_is_reserved_though_smeltr_leaves_it_alone() {
        let json = example_json().replacen(
            r#""groupId": "smeltr""#,
            r#""groupId": "smeltr", "properties": {"queue.buffering.max.messages": "1"}"#,
            1,
        );
        let file: FileConfig = serde_json::from_str(&json).unwrap();
        let errors = validation_errors(file);
        assert!(
            errors
                .iter()
                .any(|e| e.contains("queue.buffering.max.messages") && e.contains("smeltr owns")),
            "{errors:?}"
        );
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

    // -- identities ---------------------------------------------------------

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

    /// `example_json` with `identities` declared and the first Kafka source naming one.
    fn with_identity(identities: &str, on_source: &str) -> FileConfig {
        let json = example_json()
            .replacen(
                r#""groupId": "smeltr""#,
                &format!(r#""groupId": "smeltr"{on_source}"#),
                1,
            )
            .replacen(
                r#""version": 1,"#,
                &format!(r#""version": 1, "identities": [{identities}],"#),
                1,
            );
        serde_json::from_str(&json).unwrap()
    }

    const TLS: &str = r#", "properties": {"security.protocol": "SSL"}, "identity": "acme""#;

    #[test]
    fn an_identity_presenting_a_certificate_with_no_key_is_refused() {
        let identity = format!(
            r#"{{"id": "acme", "certificate": {}}}"#,
            serde_json::to_string(&a_certificate()).unwrap()
        );
        let errors = validation_errors(with_identity(&identity, TLS));
        assert!(errors.iter().any(|e| e.contains("no private key")), "{errors:?}");
    }

    #[test]
    fn an_identity_naming_a_key_with_no_certificate_is_refused() {
        let errors = validation_errors(with_identity(
            r#"{"id": "acme", "key": "ACME_KEY", "authorities": null}"#,
            TLS,
        ));
        assert!(errors.iter().any(|e| e.contains("no certificate")), "{errors:?}");
    }

    /// An identity with neither half is the one combination refused: every other says something about a
    /// connection.
    #[test]
    fn an_identity_that_declares_neither_half_is_refused() {
        let errors = validation_errors(with_identity(r#"{"id": "acme"}"#, TLS));
        assert!(errors.iter().any(|e| e.contains("changes nothing")), "{errors:?}");
    }

    /// The bundle is committed and read in review, so what goes in the certificate field is checked for being one.
    #[test]
    fn a_private_key_in_an_identitys_certificate_is_refused() {
        let identity = r#"{"id": "acme", "certificate": "-----BEGIN PRIVATE KEY-----\nMIIEvQ\n-----END PRIVATE KEY-----", "key": "ACME_KEY"}"#;
        let errors = validation_errors(with_identity(identity, TLS));
        assert!(errors.iter().any(|e| e.contains("holds a private key")), "{errors:?}");
    }

    /// A certificate good for server authentication only presents fine to a browser and refuses at the handshake
    /// with a far end that enforces Extended Key Usage -- a connector connecting out presents an identity's
    /// certificate as a client certificate.
    #[test]
    fn a_certificate_restricted_to_server_auth_is_refused() {
        let key = rcgen::KeyPair::generate().unwrap();
        let mut params = rcgen::CertificateParams::new(vec!["smeltr-test".to_string()]).unwrap();
        params.extended_key_usages = vec![rcgen::ExtendedKeyUsagePurpose::ServerAuth];
        let certificate = params.self_signed(&key).unwrap().pem();
        let identity = format!(
            r#"{{"id": "acme", "certificate": {}, "key": "ACME_KEY"}}"#,
            serde_json::to_string(&certificate).unwrap()
        );
        let errors = validation_errors(with_identity(&identity, TLS));
        assert!(errors.iter().any(|e| e.contains("client authentication")), "{errors:?}");
    }

    /// Extended Key Usage names the purpose, Key Usage the means: a leaf naming clientAuth but restricted to key
    /// encipherment cannot sign the handshake client authentication needs, whatever it claims its purpose is.
    #[test]
    fn a_certificate_that_cannot_sign_is_refused_though_its_purpose_names_client_auth() {
        let key = rcgen::KeyPair::generate().unwrap();
        let mut params = rcgen::CertificateParams::new(vec!["smeltr-test".to_string()]).unwrap();
        params.extended_key_usages = vec![rcgen::ExtendedKeyUsagePurpose::ClientAuth];
        params.key_usages = vec![rcgen::KeyUsagePurpose::KeyEncipherment];
        let certificate = params.self_signed(&key).unwrap().pem();
        let identity = format!(
            r#"{{"id": "acme", "certificate": {}, "key": "ACME_KEY"}}"#,
            serde_json::to_string(&certificate).unwrap()
        );
        let errors = validation_errors(with_identity(&identity, TLS));
        assert!(errors.iter().any(|e| e.contains("digital signature")), "{errors:?}");
    }

    /// The ordinary case: nothing restricts an identity's certificate, which is what an unremarkable leaf
    /// certificate carries -- Extended Key Usage absent entirely rules nothing out.
    #[test]
    fn a_certificate_naming_no_extended_key_usage_is_not_refused() {
        let identity = format!(
            r#"{{"id": "acme", "certificate": {}, "key": "ACME_KEY"}}"#,
            serde_json::to_string(&a_certificate()).unwrap()
        );
        let errors = validation_errors(with_identity(&identity, TLS));
        assert!(
            !errors.iter().any(|e| e.contains("client authentication")),
            "{errors:?}"
        );
    }

    /// `CertificateDer`'s own PEM reader selects certificate sections rather than proving there is nothing else,
    /// so a certificate beside a CSR would pass silently through an iterator that only extracts certificates.
    #[test]
    fn a_certificate_beside_a_csr_is_refused() {
        let csr = "-----BEGIN CERTIFICATE REQUEST-----\nMIIBCjCBtAIBADAA\n-----END CERTIFICATE REQUEST-----\n";
        let identity = format!(
            r#"{{"id": "acme", "authorities": {}}}"#,
            serde_json::to_string(&format!("{}{csr}", a_certificate())).unwrap()
        );
        let errors = validation_errors(with_identity(&identity, TLS));
        assert!(errors.iter().any(|e| e.contains("Csr")), "{errors:?}");
    }

    #[test]
    fn an_identity_trusting_no_public_root_and_no_authority_is_refused() {
        let identity = format!(
            r#"{{"id": "acme", "trustPublic": false, "certificate": {}, "key": "ACME_KEY"}}"#,
            serde_json::to_string(&a_certificate()).unwrap()
        );
        let errors = validation_errors(with_identity(&identity, TLS));
        assert!(errors.iter().any(|e| e.contains("trusts no public root")), "{errors:?}");
    }

    #[test]
    fn trusting_what_is_not_a_certificate_is_refused() {
        let identity = r#"{"id": "acme", "authorities": "not a certificate"}"#;
        let errors = validation_errors(with_identity(identity, TLS));
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
        let identity = format!(
            r#"{{"id": "acme", "authorities": {}}}"#,
            serde_json::to_string(bogus).unwrap()
        );
        let errors = validation_errors(with_identity(&identity, TLS));
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
        let identity = format!(
            r#"{{"id": "acme", "authorities": {}}}"#,
            serde_json::to_string(&bogus).unwrap()
        );
        let errors = validation_errors(with_identity(&identity, TLS));
        assert!(
            errors.iter().any(|e| e.contains("not a certificate authority")),
            "{errors:?}"
        );
    }

    #[test]
    fn a_connector_naming_an_identity_nothing_declares_is_refused() {
        let file: FileConfig = serde_json::from_str(&example_json().replacen(
            r#""groupId": "smeltr""#,
            r#""groupId": "smeltr", "properties": {"security.protocol": "SSL"}, "identity": "acme""#,
            1,
        ))
        .unwrap();
        let errors = validation_errors(file);
        assert!(errors.iter().any(|e| e.contains("does not declare")), "{errors:?}");
    }

    /// One place each answer can come from: an identity carries the material inline, and a path names the same
    /// material another way.
    #[test]
    fn an_identity_beside_a_path_to_the_same_material_is_refused() {
        for key in [
            "ssl.ca.location",
            "ssl.certificate.location",
            "ssl.key.location",
            "ssl.keystore.location",
        ] {
            let on_source = format!(
                r#", "properties": {{"security.protocol": "SSL", "{key}": "/etc/pki/acme.pem"}}, "identity": "acme""#
            );
            let errors = validation_errors(with_identity(
                r#"{"id": "acme", "authorities": "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----"}"#,
                &on_source,
            ));
            assert!(
                errors.iter().any(|e| e.contains("one place it can come from")),
                "{key}: {errors:?}"
            );
        }
    }

    /// A passphrase on the key is what would make an identity's key mean different things on the two connectors.
    /// It is credential-valued, so it is written where a credential goes.
    #[test]
    fn an_identity_beside_a_passphrase_for_its_key_is_refused() {
        let on_source = concat!(
            r#", "properties": {"security.protocol": "SSL"}"#,
            r#", "credentials": {"ssl.key.password": "KAFKA_KEY_PASSWORD"}"#,
            r#", "identity": "acme""#
        );
        let errors = validation_errors(with_identity(
            r#"{"id": "acme", "authorities": "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----"}"#,
            on_source,
        ));
        assert!(
            errors.iter().any(|e| e.contains("one place it can come from")),
            "{errors:?}"
        );
    }

    /// Naming an identity names which authorities a far end is verified against; a connector that also turns
    /// verification off is not doing that, on Kafka alone -- Elasticsearch has no such knob, so the identity
    /// would verify on one connector and not the other.
    #[test]
    fn an_identity_beside_disabled_certificate_verification_is_refused() {
        let on_source = concat!(
            r#", "properties": {"security.protocol": "SSL", "enable.ssl.certificate.verification": "false"}"#,
            r#", "identity": "acme""#
        );
        let errors = validation_errors(with_identity(
            r#"{"id": "acme", "authorities": "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----"}"#,
            on_source,
        ));
        assert!(
            errors.iter().any(|e| e.contains("one place it can come from")),
            "{errors:?}"
        );
    }

    /// The chain can still check out under the identity's own authorities while this turns off whether the
    /// certificate is also checked against the broker's own name -- a different half of the same question
    /// `enable.ssl.certificate.verification` answers the other half of.
    #[test]
    fn an_identity_beside_disabled_hostname_verification_is_refused() {
        let on_source = concat!(
            r#", "properties": {"security.protocol": "SSL", "ssl.endpoint.identification.algorithm": "none"}"#,
            r#", "identity": "acme""#
        );
        let errors = validation_errors(with_identity(
            r#"{"id": "acme", "authorities": "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----"}"#,
            on_source,
        ));
        assert!(
            errors.iter().any(|e| e.contains("one place it can come from")),
            "{errors:?}"
        );
    }

    /// librdkafka defaults `security.protocol` to plaintext, so an unset one leaves the identity unused.
    #[test]
    fn an_identity_under_a_protocol_that_negotiates_no_tls_is_refused() {
        let identity =
            r#"{"id": "acme", "authorities": "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----"}"#;
        for on_source in [
            r#", "identity": "acme""#,
            r#", "properties": {"security.protocol": "SASL_PLAINTEXT"}, "identity": "acme""#,
        ] {
            let errors = validation_errors(with_identity(identity, on_source));
            assert!(
                errors
                    .iter()
                    .any(|e| e.contains("does not negotiate TLS") || e.contains("defaults to plaintext")),
                "{on_source}: {errors:?}"
            );
        }
    }

    #[test]
    fn an_identity_on_a_destination_reached_over_plain_http_is_refused() {
        let json = example_json()
            .replacen(r#""version": 1,"#, r#""version": 1, "identities": [{"id": "acme", "authorities": "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----"}],"#, 1);
        let mut file: FileConfig = serde_json::from_str(&json).unwrap();
        file.destinations.push(serde_json::from_str(
            r#"{"id": "es", "type": "elasticsearch", "url": "http://es.internal:9200", "defaultIndex": "rows", "identity": "acme"}"#,
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

    /// A connector carries the identity it names, resolved, rather than the name -- which is what lets a client
    /// be built from the topology alone.
    #[test]
    fn a_connector_carries_the_identity_it_names() {
        let certificate = a_certificate();
        let identity = format!(
            r#"{{"id": "acme", "certificate": {}, "key": "ACME_KEY", "authorities": {}}}"#,
            serde_json::to_string(&certificate).unwrap(),
            serde_json::to_string(&certificate).unwrap()
        );
        let json = example_json()
            .replacen(r#""groupId": "smeltr""#, &format!(r#""groupId": "smeltr"{TLS}"#), 1)
            .replacen(
                r#""version": 1,"#,
                &format!(r#""version": 1, "identities": [{identity}],"#),
                1,
            );
        let loaded = load_str(&json).expect("an identity a source names");
        let kafka = loaded
            .topology
            .sources
            .iter()
            .find_map(|s| match &s.kind {
                crate::plan::topology::SourceKind::Kafka(k) => Some(k),
                _ => None,
            })
            .expect("a kafka source");
        let carried = kafka.identity.as_ref().expect("the identity it names");
        assert_eq!(carried.id, "acme");
        assert_eq!(carried.certificate.as_deref(), Some(certificate.as_str()));
        assert_eq!(
            carried.key,
            Some(crate::plan::topology::SecretRef::WorkerLocal("ACME_KEY".into()))
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

    /// An identity nothing names configures no connection, so nothing about it -- a key it wants, a certificate
    /// running out -- can stop anything working. Both questions asked of a bundle are asked of the same list.
    #[test]
    fn only_the_identities_a_connector_names_are_in_use() {
        let certificate = a_certificate();
        let identities = format!(
            r#"{{"id": "acme", "authorities": {0}}}, {{"id": "unused", "authorities": {0}}}"#,
            serde_json::to_string(&certificate).unwrap()
        );
        let json = example_json()
            .replacen(r#""groupId": "smeltr""#, &format!(r#""groupId": "smeltr"{TLS}"#), 1)
            .replacen(
                r#""version": 1,"#,
                &format!(r#""version": 1, "identities": [{identities}],"#),
                1,
            );
        let loaded = load_str(&json).expect("two identities, one of them named");
        let in_use: Vec<String> = loaded.topology.identities.iter().map(|i| i.id.clone()).collect();
        assert_eq!(in_use, vec!["acme".to_string()]);
    }

    /// A key a worker cannot obtain has to refuse the deploy before the running one is stopped, which is what
    /// `named_secrets` is asked. An identity's key is as much a named credential as a SASL password.
    #[test]
    fn an_identitys_key_is_a_credential_the_bundle_names() {
        let certificate = a_certificate();
        let identity = format!(
            r#"{{"id": "acme", "certificate": {}, "key": {{"secret": "acme-key", "version": 3}}}}"#,
            serde_json::to_string(&certificate).unwrap()
        );
        let json = example_json()
            .replacen(r#""groupId": "smeltr""#, &format!(r#""groupId": "smeltr"{TLS}"#), 1)
            .replacen(
                r#""version": 1,"#,
                &format!(r#""version": 1, "identities": [{identity}],"#),
                1,
            );
        let loaded = load_str(&json).expect("an identity a source names");
        let named = &loaded.topology.named_secrets;
        assert!(
            named.iter().any(|n| matches!(
                &n.reference,
                crate::plan::topology::SecretRef::LeaderStored { name, version }
                    if name == "acme-key" && *version == 3
            )),
            "{:?}",
            named.iter().map(|n| n.what.as_str()).collect::<Vec<_>>()
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
                literal: vec![("compression.type".into(), codec.to_plan().as_property().into())],
                credentials: Vec::new(),
            };
            let mut errors = Vec::new();
            crate::io::kafka::validate_literal_props(&props, "test", &mut errors);
            assert!(
                errors.is_empty(),
                "{} is offered but this build refuses it: {errors:?}",
                codec.to_plan().as_property()
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
        assert!(c.partition_by.is_empty() && c.identity.is_none());
        assert!(c.storage.region.is_none() && c.storage.endpoint.is_none());
        assert!(c.storage.access_key.is_none() && c.storage.role.is_none());
        assert_eq!(c.compression, crate::plan::topology::Compression::Zstd);
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

    /// Half of a pair names the half that is missing, since that is where the fix goes.
    #[test]
    fn an_identity_with_half_a_pair_names_the_missing_half() {
        let mut file: FileConfig = serde_json::from_str(example_json()).unwrap();
        file.identities.push(
            serde_json::from_value(serde_json::json!({
                "id": "half", "key": "TLS_KEY"
            }))
            .unwrap(),
        );
        let errors = validation_errors(file);
        let half = errors
            .iter()
            .find(|e| e.contains("names a private key and has no certificate"))
            .unwrap();
        assert_eq!(
            half.item,
            Some(ItemRef {
                section: "identities",
                id: "half".into()
            })
        );
        assert_eq!(half.field, Some("certificate"));
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
