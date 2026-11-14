//! DTO -> `plan::config`/`plan::topology`: id resolution and structural validation (every reference resolves, every id
//! is unique in its scope, every field that must not be blank is not, every bound holds). Expression text passes
//! through verbatim; `LoadedSystem::compile_topology` checks it with the compiler's own errors.
//!
//! Every top-level item that fails to convert contributes one error and is otherwise skipped, so one bad route does not
//! hide an unrelated bad pipeline.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use std::path::PathBuf;

use crate::core::column::DataType;
use crate::core::error::{Category, ExceptionKind};
use crate::core::id::{DestId, FunctionId, SourceId};
use crate::format::encoding::Encoding;
use crate::io::entra::IMDS_API_VERSION;
use crate::io::framing::{ByteOrder, Counts, LengthPrefix};
use crate::io::line_breaker::{LineBreaker, PER_LINE};
use crate::io::logs::LogsSourceConfig;
use crate::io::object_source::MAX_RECORD_BYTES;
use crate::io::trust::Side;
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
    CatalogSignIn, Cloud, ConnectionLimits, DeliverRule, DeltaDestinationConfig, DestinationConfig, DestinationKind,
    DocIdPolicy, Endpoint, EntraSignIn, Framing, GcsAccess, GcsStore, HttpDestinationConfig, IcebergDestinationConfig,
    KafkaDestinationConfig, KafkaProps, KafkaService, KafkaSourceConfig, ListPlace, ListStart,
    LogAnalyticsDestinationConfig, NamedCertificate, NamedSecret, Newlines, ObjectFormat, ObjectQueue,
    ObjectSourceConfig, ObjectsDestinationConfig, OktaSignIn, OtlpDestinationConfig, PrometheusDestinationConfig,
    RemoteWriteVersion, RequestBody, RouteConfig, S3Access, S3Store, SftpAccess, SftpCredential, SftpStore,
    SourceConfig, SourceKind, SplunkHecDestinationConfig, StartPosition, StopPosition, Tls, TopologyConfig, Transport,
    UnityCatalogTable,
};
use crate::plan::topology::{Signal, SourceMeta};
use crate::schema::Schema;

use super::CURRENT_VERSION;
use super::bytes;
use super::dto::*;

pub(super) mod destination;
pub(super) mod source;

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

/// One problem `validate` found, attributed to where it came from when it is known. A compile error carries the
/// pipeline and function step whose expression failed. A check on one source or destination carries that
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
    /// The source or destination this is about.
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

    /// A problem with the item as a whole, or after `at`, with one setting. `message` names the item, as the error
    /// bar shows it without its id.
    fn push(&mut self, message: String) {
        debug_assert!(
            message.contains(&self.item.id),
            "{message:?} does not name {}",
            self.item.id
        );
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
    for (at, r) in topology.routes.iter().enumerate() {
        let what = format!("route {}", system.route_name(at));
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

/// The TLS `of` speaks, or `None` for a connector that says nothing about it. A receiver trusts no public root:
/// whom it admits is its authorities' say alone.
fn to_tls(dto: &TlsDto, of: &str, side: Side) -> Option<Tls> {
    dto.is_set().then(|| Tls {
        of: of.to_string(),
        certificate: dto.certificate.as_deref().map(|c| c.trim().to_string()),
        authorities: dto.trusted_authorities.clone(),
        trust_public: side == Side::Client && dto.trust_public != Some(false),
        side,
        senders: Default::default(),
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
        if let SourceKindDto::Kafka(KafkaSourceDto { group_id, .. }) = &mut s.kind
            && group_id.trim().is_empty()
        {
            *group_id = format!("{DEFAULT_GROUP_ID}.{worker_group}");
        }
    }
}

/// The database an SQL source connects to, from its connection settings alone.
fn sql_database(sql: &SqlSourceDto, of: &str) -> crate::plan::topology::SqlDatabase {
    let kind = sql.kind;
    let (host, port, database, user) = (
        sql.host.trim().to_string(),
        sql.port(kind),
        sql.database.trim().to_string(),
        sql.user.trim().to_string(),
    );
    let password = sql.password.as_ref().map(to_secret_ref);
    let encrypted = match sql.encrypted {
        SqlTlsDto::Require => crate::io::sql_source::Encrypted::Require,
        SqlTlsDto::Disable => crate::io::sql_source::Encrypted::Disable,
    };
    let tls = to_tls(&sql.tls, of, Side::Client);
    match kind {
        SqlKind::Postgres => crate::plan::topology::SqlDatabase::Postgres(crate::io::postgres::PostgresConnection {
            host,
            port,
            database,
            user,
            password,
            encrypted,
            tls,
        }),
        SqlKind::MySql => crate::plan::topology::SqlDatabase::MySql(crate::io::mysql::MySqlConnection {
            host,
            port,
            database,
            user,
            password,
            encrypted,
            tls,
        }),
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

/// Properties smeltr sets itself, with the reason it owns each, which `/api/system` serves to the GUI. Both spellings
/// of an aliased property appear: librdkafka resolves either to the same setting, and operator properties are applied
/// after smeltr's.
pub const RESERVED_KAFKA_PROPS: &[(&str, &str)] = &[
    ("bootstrap.servers", "set from the `brokers` field"),
    // librdkafka runs it through system() on every worker, at client start and on each ticket refresh; its default
    // already uses the keytab and principal a bundle names, and a bundle may not carry a shell command.
    (
        "sasl.kerberos.kinit.cmd",
        "it runs a shell command on every worker, and a config may not carry one",
    ),
    (
        "metadata.broker.list",
        "another name for bootstrap.servers, which is set from the `brokers` field",
    ),
    ("group.id", "set from this source's `groupId` field"),
    ("client.rack", "each worker sets it from its zone"),
    ("group.instance.id", "each worker sets it from its own id"),
    // smeltr commits explicitly once a batch's destinations have flushed; letting librdkafka auto-commit would
    // acknowledge offsets for rows that have not been delivered, silently breaking at-least-once.
    (
        "enable.auto.commit",
        "smeltr commits only once a batch is delivered, and auto-commit would mark rows done before they are",
    ),
    // A deprecated topic-level property for librdkafka's legacy simple consumer, which the high-level consumer smeltr
    // runs ignores; setting it reads as a way to control auto-commit and does nothing at all, where
    // `enable.auto.commit` is the global property smeltr pins off and reserves.
    (
        "auto.commit.enable",
        "it does nothing on the consumer smeltr runs, and smeltr keeps auto-commit off",
    ),
    ("auto.offset.reset", "set from this source's `start` field"),
    // `io::kafka::MAX_POLL_INTERVAL`. A flush must not outlast it (`StallAfter::MAX`), or the group evicts the consumer.
    (
        "max.poll.interval.ms",
        "fixed at 5 minutes, which caps each destination's `stallAfterSecs` field, so a slow destination cannot rebalance the group",
    ),
    // That field offers exactly the codecs this build can produce.
    ("compression.type", "set from this destination's `compression` field"),
    (
        "compression.codec",
        "another name for compression.type, which is set from this destination's `compression` field",
    ),
    ("acks", "set from this destination's `acks` field"),
    (
        "request.required.acks",
        "another name for acks, which is set from this destination's `acks` field",
    ),
    // That field makes a reader's memory footprint a number this deployment chose.
    (
        "queued.max.messages.kbytes",
        "set from this source's `fetchBytes` field",
    ),
    // The producer holds half of that field, and the egress queue in front of it the other half.
    (
        "queue.buffering.max.kbytes",
        "set from this destination's `bufferBytes` field",
    ),
    // smeltr reports librdkafka's own statistics as `smeltr_destination_throttled_seconds_total` and
    // `smeltr_destination_buffered_bytes`, which is the only way broker-enforced quota throttling is visible at all;
    // setting it to 0 would switch those series off with nothing to say they had stopped.
    (
        "statistics.interval.ms",
        "smeltr needs it on to report destination throttling and buffer size",
    ),
    (
        "queue.buffering.max.messages",
        "set to its maximum, so this destination's `bufferBytes` field bounds the queue",
    ),
    // One fetch may carry half the queue, so a fetch always lands with room behind it.
    ("fetch.max.bytes", "set from this source's `fetchBytes` field"),
    // librdkafka refuses a fetch larger than the message that carries it.
    (
        "receive.message.max.bytes",
        "raised with the fetch size, which is set from this source's `fetchBytes` field",
    ),
    // The same as `fetch.max.bytes`, so one hot partition may fill a fetch.
    ("fetch.message.max.bytes", "set from this source's `fetchBytes` field"),
    // smeltr runs the producer with it off: an idempotent producer keeps at most five requests in flight, too few for a
    // far link.
    (
        "enable.idempotence",
        "smeltr keeps it off, because an idempotent producer allows too few requests in flight for a distant broker",
    ),
    (
        "transactional.id",
        "smeltr never begins a transaction, and a transactional producer refuses records sent outside one",
    ),
    // Cooperative and eager members must not share a consumer group.
    (
        "partition.assignment.strategy",
        "smeltr uses cooperative rebalancing, and one member on another strategy breaks the group for every worker",
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

fn to_framing(dto: &FramingSettingsDto) -> Framing {
    match dto.framing {
        // Validation refused a pattern that does not compile before any of this ran, so the fallback is unreachable.
        FramingDto::Lines => dto
            .line_breaker
            .as_deref()
            .and_then(to_line_breaker)
            .map_or(Framing::Lines, Framing::LineBreaker),
        FramingDto::Object => Framing::Object,
        FramingDto::EscapedLines => Framing::EscapedLines,
        // Validation refused a length prefix or a fixed size that is absent or out of range.
        FramingDto::LengthPrefix => {
            let prefix = dto.length_prefix.expect("validation required a lengthPrefix");
            Framing::LengthPrefix(LengthPrefix {
                offset: prefix.offset as usize,
                width: prefix.bytes as usize,
                order: match prefix.order {
                    ByteOrderDto::Big => ByteOrder::Big,
                    ByteOrderDto::Little => ByteOrder::Little,
                },
                counts: match prefix.counts {
                    CountsDto::Rest => Counts::Rest,
                    CountsDto::Message => Counts::Message,
                },
            })
        }
        FramingDto::Fixed => Framing::Fixed(dto.fixed_bytes.expect("validation required fixedBytes") as usize),
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
fn listed(queue: ObjectQueue, tls: Option<Tls>, framing: &FramingSettingsDto, encoding: &EncodingLabel) -> SourceKind {
    SourceKind::Objects(ObjectSourceConfig {
        queue,
        tls,
        framing: to_framing(framing),
        // Validation refused an unknown label before any of this ran, so the fallback is unreachable.
        encoding: Encoding::for_label(&encoding.0).unwrap_or_default(),
        avro: None,
        visibility: Duration::from_secs(VisibilitySecs::DEFAULT),
    })
}

/// The listing of `prefix` in `place`.
fn to_list(place: ListPlace, listing: &ListingDto) -> ObjectQueue {
    ObjectQueue::List {
        place,
        prefix: listing.prefix.to_string(),
        poll_interval: Duration::from_secs(listing.poll_interval_secs.0),
        start: match listing.start {
            ListStartDto::Everything => ListStart::Everything,
            ListStartDto::New => ListStart::New,
        },
        after_commit: match listing.after_commit {
            AfterCommitDto::Keep => AfterCommit::Keep,
            AfterCommitDto::Delete => AfterCommit::Delete,
            // Validation refused a move with no prefix before any of this ran.
            AfterCommitDto::Move => AfterCommit::Move {
                prefix: listing.move_prefix.clone().unwrap_or_default(),
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
fn check_listing(listing: &ListingDto, what: &str, errors: &mut ItemErrors<'_>) {
    let ListingDto {
        prefix,
        poll_interval_secs: poll_interval,
        start,
        after_commit,
        move_prefix,
    } = listing;
    let (poll_interval, start, after_commit) = (*poll_interval, *start, *after_commit);
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

// -- TLS --------------------------------------------------------------------

/// librdkafka property names that answer the same question a connector's TLS settings answer: what this client
/// presents, and what it trusts. A connector setting TLS and one of these has two answers to one question, and
/// librdkafka would silently take whichever was set last. `/api/system` serves them to the GUI.
pub const TLS_KAFKA_PROPS: &[&str] = &[
    "ssl.certificate.pem",
    "ssl.certificate.location",
    "ssl.key.pem",
    "ssl.key.location",
    "ssl.ca.pem",
    "ssl.ca.location",
    // PKCS#12: the same certificate and key, in one file, under other names.
    "ssl.keystore.location",
    "ssl.keystore.password",
    // Not material, but the one thing that would make a certificate's key mean different things to two
    // connectors: librdkafka decrypts a passphrase-protected key and ureq cannot, so a connector holding this
    // beside a certificate is a deployment that works on Kafka and fails on Elasticsearch. The value is a credential
    // and may be worker-local, so save time cannot look at the key and answer this any other way.
    "ssl.key.password",
    // Naming authorities is naming what a far end is verified against; a connector that also turns verification
    // off is not doing that at all. Elasticsearch has no equivalent knob, so the authorities would verify on one
    // connector and not the other -- the same asymmetry `ssl.key.password` is refused for.
    "enable.ssl.certificate.verification",
    // A different half of the same question: the chain can check out under the connector's own authorities while
    // this decides whether the certificate is also checked against the broker's own name. "none" is what turns
    // that off -- any certificate the authorities ever issued, for any host, then answers for this one.
    "ssl.endpoint.identification.algorithm",
];

/// The librdkafka security protocols that negotiate TLS. Under any other, nothing a connector's TLS settings say is
/// used, and librdkafka's own default is `plaintext`.
const TLS_PROTOCOLS: &[&str] = &["SSL", "SASL_SSL"];

/// Refuses `pem` unless it holds certificates and nothing else. Read with the same library that will present
/// them, so a save cannot accept PEM the connector would go on to refuse.
///
/// Every PEM section, not `CertificateDer`'s own iterator: that one selects certificate sections rather than
/// proving there is nothing else, so a certificate beside a CSR or a public key would pass it silently.
///
/// Each section's DER is parsed as X.509, not just labeled `CERTIFICATE`: a `CERTIFICATE` section wrapping
/// malformed or truncated DER passes the label check and only surfaces at the TLS handshake, in a chain member
/// `io::trust::refuse_unless_usable` never reads (it looks at the leaf alone) or in an authorities bundle
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

/// A receiver speaking `receiver`, with what every receiver takes. It listens on `port` at every address of the host,
/// IPv4 and IPv6: `[::]`, which the receiver opens dual-stack. The worker that runs it narrows that to the address its
/// own startup settings name (`startup_config::worker_listen_address`), since an address is a fact about one host.
fn receiver_config<'a>(
    receiver: crate::plan::topology::Receiver,
    answer_after_delivery: bool,
    listener: &ReceiverSettingsDto,
    tls: &ServedTlsDto,
    tokens: impl Iterator<Item = (&'a String, &'a SecretRefDto)>,
    connections: crate::plan::topology::ConnectionLimits,
    of: &str,
) -> SourceKind {
    SourceKind::Receiver(crate::plan::topology::ReceiverConfig {
        receiver,
        listen: std::net::SocketAddr::new(std::net::Ipv6Addr::UNSPECIFIED.into(), listener.port),
        tokens: tokens.map(|(k, r)| (k.clone(), to_secret_ref(r))).collect(),
        tls: to_tls(&tls.tls(), of, Side::Server).map(|served| Tls {
            senders: to_senders(tls),
            ..served
        }),
        max_request_bytes: usize::try_from(listener.max_request_bytes.0).unwrap_or(usize::MAX),
        answer_after_delivery,
        connections,
    })
}

/// Whom a receiver admits by a sender's certificate besides its authorities. What does not read was refused at save.
fn to_senders(tls: &ServedTlsDto) -> crate::plan::topology::Senders {
    crate::plan::topology::Senders {
        fingerprints: tls
            .sender_fingerprints
            .iter()
            .filter_map(|f| crate::io::senders::Fingerprint::parse(f).ok())
            .collect(),
        names: tls.sender_names.iter().map(|n| n.trim().to_string()).collect(),
    }
}

/// What `dto` says of a receiver's connections.
fn to_connection_limits(dto: &ConnectionLimitsDto) -> crate::plan::topology::ConnectionLimits {
    let size = |n: u64| usize::try_from(n).unwrap_or(usize::MAX);
    crate::plan::topology::ConnectionLimits {
        max: dto.max_connections.map(size),
        per_sender: dto.max_connections_per_sender.map(size),
        idle: dto.idle_timeout_secs.map(std::time::Duration::from_secs),
    }
}

/// Checks what a connector's TLS settings say, before anything reads them: a certificate named by name, authorities
/// that are certificates, and a trust setting that leaves something to verify a far end against.
fn check_tls(tls: &TlsDto, what: &str, errors: &mut ItemErrors<'_>) {
    if tls.certificate.as_deref().is_some_and(|c| c.trim().is_empty()) {
        errors.at("certificate").push(format!(
            "{what} names a certificate by a blank name. Choose one from the list"
        ));
    }
    if tls.certificate.is_none() && tls.trusted_authorities.is_none() && tls.trust_public == Some(true) {
        errors.at("trustPublic").push(format!(
            "{what} names neither a certificate nor trusted authorities, so trust public roots changes nothing. Choose a \
             certificate or paste trusted authorities, or leave trust public roots at its default"
        ));
    }
    if tls.trust_public == Some(false) && tls.trusted_authorities.is_none() {
        errors.at("trustPublic").push(format!(
            "{what} trusts no public root and names no trusted authority, so nothing it connects to could be verified. \
             Paste its trusted authorities, or trust public roots"
        ));
    }
    if let Some(authorities) = &tls.trusted_authorities
        && let Err(why) = refuse_unless_certificates(authorities)
    {
        errors.at("trustedAuthorities").push(format!(
            "{what} trusts what is not a certificate authority: {why}. Leave it out to verify a far end against \
             the public roots"
        ));
    }
}

/// Refuses TLS settings on a connector reached at `urls` of which none negotiates TLS: nothing they say would be
/// used. A blank address is refused on its own.
fn check_tls_reaches(tls: &TlsDto, urls: &[(&'static str, &str)], what: &str, errors: &mut ItemErrors<'_>) {
    let https = |url: &str| url.trim().to_ascii_lowercase().starts_with("https://");
    if tls.is_set()
        && let Some((field, url)) = urls.first()
        && !url.trim().is_empty()
        && !urls.iter().any(|(_, url)| https(url))
    {
        errors.at(field).push(format!(
            "{what} sets TLS and is reached over {url:?}, which does not negotiate TLS, so nothing its certificate or \
             trusted authorities say would be used. Give it an https:// URL, or take them off it"
        ));
    }
}

/// Refuses a Kafka client whose own properties disagree with its TLS settings.
fn check_kafka_tls(tls: &TlsDto, props: &KafkaPropsDto, what: &str, errors: &mut ItemErrors<'_>) {
    if !tls.is_set() {
        return;
    }

    for (map, key) in kafka_keys(props) {
        if TLS_KAFKA_PROPS.contains(&key.to_ascii_lowercase().as_str()) {
            errors.at(map).push(format!(
                "{what} sets TLS and also sets {key:?}, which names the same material. Take one of them out, so \
                 there is one place it can come from"
            ));
        }
    }

    // librdkafka defaults this to plaintext, so absent is as wrong as a plaintext value: the TLS settings would be
    // configuration nothing reads.
    match props.properties.get("security.protocol") {
        Some(protocol) if TLS_PROTOCOLS.contains(&protocol.to_ascii_uppercase().as_str()) => {}
        Some(protocol) => errors.at("properties.security.protocol").push(format!(
            "{what} sets TLS and sets \"security.protocol\" to {protocol:?}, which does not negotiate TLS, so \
             nothing its certificate or trusted authorities say would be used. Set {} or {}, or take them off it",
            TLS_PROTOCOLS[0], TLS_PROTOCOLS[1]
        )),
        None => errors.at("properties.security.protocol").push(format!(
            "{what} sets TLS and does not set \"security.protocol\", which librdkafka defaults to plaintext, so \
             nothing its certificate or trusted authorities say would be used. Set it to {} or {} in `properties`, or take \
             them off it",
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
            SourceKindDto::Kafka(KafkaSourceDto {
                brokers,
                group_id,
                topics,
                ..
            }) => (brokers.clone(), group_id, topics, "topics"),
            SourceKindDto::EventHubs(EventHubsSourceDto {
                namespace,
                consumer_group,
                event_hubs,
                ..
            }) => (event_hubs_brokers(namespace), consumer_group, event_hubs, "eventHubs"),
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

/// Refuses a Basic sign-in where it does not hold: a username without a password or the other way round, and a blank
/// username. `fields` names the username's and the password's; `whom` says what signs in, as the message's subject.
fn check_basic(
    username: &Option<String>,
    password: &Option<crate::config_file::dto::SecretRefDto>,
    fields: (&'static str, &'static str),
    whom: &str,
    errors: &mut ItemErrors<'_>,
) {
    match (username, password) {
        (Some(_), None) => errors
            .at(fields.1)
            .push(format!("{whom} with a username but no password")),
        (None, Some(_)) => errors
            .at(fields.0)
            .push(format!("{whom} with a password but no username")),
        (Some(u), Some(_)) if u.trim().is_empty() => errors.at(fields.0).push(format!("{whom} as a blank username")),
        _ => {}
    }
}

/// Refuses an Elasticsearch destination's sign-in where it does not hold: `check_basic`'s, and a password beside
/// `auth`.
fn check_elastic_sign_in(
    id: &str,
    auth: &Option<crate::config_file::dto::SecretRefDto>,
    username: &Option<String>,
    password: &Option<crate::config_file::dto::SecretRefDto>,
    errors: &mut ItemErrors<'_>,
) {
    let whom = format!("destination {id:?} signs in");
    check_basic(username, password, ("username", "password"), &whom, errors);
    if auth.is_some() && password.is_some() {
        errors.at("auth").push(format!(
            "destination {id:?} signs in with both an API key and a password; keep one"
        ));
    }
}

/// Refuses a registry with no address, and a sign-in `check_basic` refuses.
fn check_registry(dto: &crate::config_file::dto::SchemaRegistryDto, what: &str, errors: &mut ItemErrors<'_>) {
    if dto.url.trim().is_empty() {
        errors
            .at("schemaRegistry.url")
            .push(format!("{what} names a schema registry but not its address"));
    }
    check_basic(
        &dto.username,
        &dto.password,
        ("schemaRegistry.username", "schemaRegistry.password"),
        &format!("{what} signs in to its schema registry"),
        errors,
    );
}

/// Refuses how a source reads its payloads where it does not hold: a stream's Avro read without a registry, an
/// encoding or a framing beside Avro, fields that declare no record, and a registry or fields where the source reads
/// no Avro.
fn check_read(kind: &SourceKindDto, id: &str, errors: &mut ItemErrors<'_>) {
    use crate::config_file::dto::SourceFormatDto;
    let Some(read) = kind.read() else { return };
    let what = format!("source {id:?}");
    let stream = matches!(kind, SourceKindDto::Kafka(_) | SourceKindDto::EventHubs(_));
    let framing = kind.framing();
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

/// Refuses a line breaker on any framing but lines, and one `check_line_breaker` refuses; a length prefix and a fixed
/// size on any framing but their own, absent from their own, or that no message or record of `max` bytes could hold.
/// `unit` is the reader's word for it: a receiver's "message", an object's "record".
fn check_framing(dto: &FramingSettingsDto, id: &str, max: u64, unit: &str, errors: &mut ItemErrors<'_>) {
    let what = format!("source {id:?}");
    if let Some(line_breaker) = &dto.line_breaker {
        if dto.framing == FramingDto::Lines {
            check_line_breaker(&what, line_breaker, errors.at("lineBreaker"));
        } else {
            errors
                .at("lineBreaker")
                .push(format!("{what} sets lineBreaker, which only lines framing reads"));
        }
    }
    match (dto.framing, dto.length_prefix) {
        (FramingDto::LengthPrefix, None) => errors.at("lengthPrefix").push(format!(
            "{what} cuts {unit}s by a length prefix; give the length's size in bytes, and its offset if any, in lengthPrefix"
        )),
        (FramingDto::LengthPrefix, Some(prefix)) => {
            if !(1..=8).contains(&prefix.bytes) {
                errors.at("lengthPrefix").push(format!(
                    "{what} reads a length of {} bytes; it must be 1 to 8",
                    prefix.bytes
                ));
            } else if prefix.offset.saturating_add(prefix.bytes) > max {
                errors.at("lengthPrefix").push(format!(
                    "{what} reads a length that ends {} bytes into a {unit}, past the largest {unit} of {}",
                    prefix.offset.saturating_add(prefix.bytes),
                    bytes::exact(max as usize)
                ));
            }
        }
        (_, Some(_)) => errors.at("lengthPrefix").push(format!(
            "{what} sets lengthPrefix, which only lengthPrefix framing reads"
        )),
        (_, None) => {}
    }
    match (dto.framing, dto.fixed_bytes) {
        (FramingDto::Fixed, None) => errors.at("fixedBytes").push(format!(
            "{what} cuts {unit}s by a fixed size; give how many bytes in fixedBytes"
        )),
        (FramingDto::Fixed, Some(size)) if size == 0 || size > max => errors.at("fixedBytes").push(format!(
            "{what} cuts a {unit} every {size} bytes; it must be 1 to {}, the largest {unit}",
            bytes::exact(max as usize)
        )),
        (FramingDto::Fixed, Some(_)) | (_, None) => {}
        (_, Some(_)) => errors
            .at("fixedBytes")
            .push(format!("{what} sets fixedBytes, which only fixed framing reads")),
    }
}

/// Refuses a sourcetype named by a blank name, and a line breaker it gives that `check_line_breaker` refuses.
fn check_sourcetypes(id: &str, sourcetypes: &BTreeMap<String, SourcetypeDto>, errors: &mut ItemErrors<'_>) {
    let what = format!("source {id:?}");
    for (name, t) in sourcetypes {
        // A field is named by a static path, so a refusal names the sourcetype in its message.
        if name.trim().is_empty() {
            errors
                .at("sourcetypes")
                .push(format!("{what} names a sourcetype by a blank name"));
        }
        let what = format!("source {id}'s sourcetype {name:?}");
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
fn blank(v: &str) -> bool {
    v.trim().is_empty()
}

fn required_fields(file: &FileConfig, attributed: &mut Vec<ValidationError>) {
    check_receiver_ports(file, attributed);

    let with_tls = (file
        .sources
        .iter()
        .filter_map(|s| Some(("sources", "source", &s.id, s.tls()?))))
    .chain(
        file.destinations
            .iter()
            .filter_map(|d| Some(("destinations", "destination", &d.id, d.tls()?))),
    );
    for (section, what, id, tls) in with_tls {
        check_tls(
            &tls,
            &format!("{what} {id:?}"),
            &mut ItemErrors::new(attributed, section, id),
        );
    }

    // A credential that names nothing is refused here, or it surfaces only when a worker fails to look it up.
    let named = (file.sources.iter().map(|s| ("sources", "source", &s.id, s.secrets()))).chain(
        file.destinations
            .iter()
            .map(|d| ("destinations", "destination", &d.id, d.secrets())),
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
        s.kind.spec().check(s, errors);
    }

    for d in &file.destinations {
        let errors = &mut ItemErrors::new(attributed, "destinations", &d.id);
        d.kind.spec().check(d, errors);
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
fn check_objects(id: &str, o: &ObjectSettingsDto, url: &str, tls: &TlsDto, errors: &mut ItemErrors<'_>) {
    check_http_delivery(
        &HttpDelivery {
            id,
            url,
            url_field: "endpoint",
            zone_urls: None,
            request_field: "maxObjectBytes",
            tls,
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
    if o.variant.is_some() && o.format != ObjectFormatDto::Parquet {
        errors.at("variant").push(format!(
            "destination {id:?} names variant, which only the \"parquet\" format reads"
        ));
    }
    if o.format == ObjectFormatDto::Avro && o.compression == CompressionDto::Lz4 {
        errors.at("compression").push(format!(
            "destination {id:?} writes Avro compressed with lz4, which Avro has no codec for; use \"gzip\", \"snappy\", \"zstd\" or \"none\""
        ));
    }
    check_file_interval(o.file_interval_secs, &format!("destination {id:?}"), "written", errors);
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

/// A source waits up to the interval for its batches' acknowledgement, and after a crash reads that much again.
/// `finished` is what becomes of a file in its destination's words: an object store's is written, a table's committed.
fn check_file_interval(file_interval: FileInterval, what: &str, finished: &str, errors: &mut ItemErrors<'_>) {
    if file_interval.0 < FileInterval::MIN || file_interval.0 > FileInterval::MAX {
        errors.at("fileIntervalSecs").push(format!(
            "{what} has fileIntervalSecs {}; it must be between {} and {} seconds. A file stays open that long, \
             and its batches are acknowledged only when it is {finished}, so a source waits up to that long for \
             them and, after a crash, reads that much again.",
            file_interval.0,
            FileInterval::MIN,
            FileInterval::MAX,
        ));
    }
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
    tls: &'a TlsDto,
    /// Requests the destination starts with outstanding; `None` checks none.
    in_flight: Option<InFlight>,
    request_bytes: usize,
    request_limits: (usize, usize),
    request_what: &'a str,
    limit_why: &'a str,
    buffer_bytes: Option<BufferBytes>,
    stall_after: StallAfter,
}

fn check_http_delivery(d: &HttpDelivery<'_>, errors: &mut ItemErrors<'_>) {
    let id = d.id;
    if d.url.trim().is_empty() {
        errors.at(d.url_field).push(format!("destination {id:?} has no URL"));
    }
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
    // http:// never negotiates TLS, so TLS settings on one are configuration nothing reads.
    let addresses = std::iter::once((d.url_field, d.url)).chain(zone_urls.map(|(_, url)| ("zoneUrls", url.as_str())));
    for (field, url) in addresses {
        check_tls_reaches(d.tls, &[(field, url)], &format!("destination {id:?}"), errors);
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

/// What a receiver must be before it can listen: a port, an address that is one, a certificate to serve, and someone
/// it admits. With no token, authorities or sender fingerprints to admit a sender by, anyone who can reach the port
/// could send.
fn check_receiver<T>(
    what: &str,
    listener: &ReceiverSettingsDto,
    tls: &ServedTlsDto,
    tokens: &BTreeMap<String, T>,
    errors: &mut ItemErrors<'_>,
) {
    let vouches = check_listener(what, listener, tls, errors);
    if tokens.is_empty() && !vouches {
        errors.at("tokens").push(format!(
            "{what} admits anyone who can reach its port; give it a token, trusted authorities a sender's certificate \
             must chain to, or fingerprints of the certificates it admits"
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

/// A receiver's connection limits. Only a receiver whose transport is `tcp` reads them.
fn check_connection_limits(what: &str, connections: &ConnectionLimitsDto, tcp: bool, errors: &mut ItemErrors<'_>) {
    let ConnectionLimitsDto {
        max_connections,
        max_connections_per_sender,
        idle_timeout_secs,
    } = connections;
    let set = [
        ("maxConnections", max_connections.is_some()),
        ("maxConnectionsPerSender", max_connections_per_sender.is_some()),
        ("idleTimeoutSecs", idle_timeout_secs.is_some()),
    ];
    for (field, set) in set {
        if set && !tcp {
            errors.at(field).push(format!(
                "{what} sets {field}, which only TCP reads: over UDP no connection is held open"
            ));
        }
    }
    let most = ConnectionLimits::MOST as u64;
    if let Some(n) = max_connections
        && !(1..=most).contains(n)
    {
        errors.at("maxConnections").push(format!(
            "{what} holds up to {n} connections; maxConnections is between 1 and {most}"
        ));
    }
    if let Some(n) = max_connections_per_sender
        && !(1..=max_connections.unwrap_or(most)).contains(n)
    {
        errors.at("maxConnectionsPerSender").push(match max_connections {
            Some(max) => format!(
                "{what} holds up to {n} connections from one sender; maxConnectionsPerSender is between 1 and its \
                 maxConnections, {max}"
            ),
            None => format!(
                "{what} holds up to {n} connections from one sender; maxConnectionsPerSender is between 1 and {most}"
            ),
        });
    }
    if *idle_timeout_secs == Some(0) {
        errors.at("idleTimeoutSecs").push(format!(
            "{what} closes a connection idle for 0 seconds; idleTimeoutSecs is at least 1"
        ));
    }
}

/// A syslog receiver as [`check_receiver`] holds any, with the networks it admits senders from in place of tokens,
/// as syslog carries none. Over UDP it serves no TLS, so only its networks can admit a sender.
fn check_syslog_receiver(
    what: &str,
    listener: &ReceiverSettingsDto,
    tls: &ServedTlsDto,
    allow_from: &[String],
    transport: Transport,
    errors: &mut ItemErrors<'_>,
) {
    let vouches = check_listener(what, listener, tls, errors);
    if transport == Transport::Udp && tls.is_set() {
        errors.at("certificate").push(format!(
            "{what} takes UDP, which it serves without TLS; take its certificate and trusted authorities off, or switch it to \
             TCP to serve TLS"
        ));
    }
    let vouches = vouches && transport == Transport::Tcp;
    if allow_from.is_empty() && !vouches {
        errors.at("allowFrom").push(format!(
            "{what} admits anyone who can reach its port; give it the networks its senders connect from, trusted \
             authorities a sender's certificate must chain to, or fingerprints of the certificates it admits"
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

/// What every receiver's listener must be: a port, and a certificate to serve where it serves TLS. Whether its
/// authorities or sender fingerprints vouch for a sender, which then needs nothing else to be admitted.
fn check_listener(what: &str, listener: &ReceiverSettingsDto, tls: &ServedTlsDto, errors: &mut ItemErrors<'_>) -> bool {
    let ReceiverSettingsDto {
        port,
        max_request_bytes,
    } = listener;
    let blank = |v: &str| v.trim().is_empty();
    if *port == 0 {
        errors
            .at("port")
            .push(format!("{what} listens on port 0; give the port senders connect to"));
    }
    if tls.is_set() && tls.certificate.is_none() {
        errors.at("certificate").push(format!(
            "{what} serves TLS and names no certificate; a receiver serves one to every sender. Choose one from \
             the list"
        ));
    }
    if !(MaxRequestBytes::MIN..=MaxRequestBytes::MAX).contains(&max_request_bytes.0) {
        errors.at("maxRequestBytes").push(format!(
            "{what} reads requests of up to {} bytes; maxRequestBytes is between {} and {}",
            max_request_bytes.0,
            MaxRequestBytes::MIN,
            MaxRequestBytes::MAX
        ));
    }
    for fingerprint in &tls.sender_fingerprints {
        if let Err(why) = crate::io::senders::Fingerprint::parse(fingerprint) {
            errors.at("senderFingerprints").push(format!(
                "{what} admits a sender by {fingerprint:?}, which is not a fingerprint: {why}. Write one as \
                 sha-256:AB:CD:…"
            ));
        }
    }
    for name in &tls.sender_names {
        if !crate::io::senders::is_host(name.trim()) {
            errors.at("senderNames").push(format!(
                "{what} admits senders named {name:?}, which is not a host name; give one as host.example.com, or \
                 *.example.com for any host in it"
            ));
        }
    }
    let authorities = tls.trusted_authorities.as_deref().is_some_and(|a| !blank(a));
    if !tls.sender_names.is_empty() && !authorities {
        errors.at("senderNames").push(format!(
            "{what} names its senders' hosts and trusts no authority, so no certificate can match those names; paste \
             the trusted authorities their certificates chain to"
        ));
    }
    authorities || !tls.sender_fingerprints.is_empty()
}

/// Refuses two receivers on one port: every worker of the group binds each, and the second bind would fail on every
/// one of them. OTLP receivers of different signals share one, as an exporter sends every signal to one port, so they
/// serve it alike: on one address, under one certificate. A TCP port and a UDP port are two ports, whatever their number.
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
                        kind:
                            SourceKindDto::OtlpReceiver(OtlpReceiverSourceDto {
                                tls: theirs,
                                connections: their_limits,
                                ..
                            }),
                        ..
                    },
                ),
                SourceKindDto::OtlpReceiver(OtlpReceiverSourceDto {
                    tls,
                    signal,
                    connections,
                    ..
                }),
            ) => {
                let same = |o: &SourceDto| matches!(&o.kind, SourceKindDto::OtlpReceiver(OtlpReceiverSourceDto { signal: other, .. }) if other == signal);
                if let Some(taken) = on_port.iter().find(|o| same(o)) {
                    Some(format!(
                        "source {:?} takes {} on port {port}, which source {:?} already does; an export of \
                         one signal reaches one receiver, so give it another port",
                        s.id,
                        signal.name(),
                        taken.id
                    ))
                } else if tls != theirs {
                    Some(format!(
                        "source {:?} shares port {port} with source {:?}, which serves it; give both the same \
                         certificate and trusted authorities, or this one another port",
                        s.id, first.id
                    ))
                } else if to_connection_limits(connections) != to_connection_limits(their_limits) {
                    Some(format!(
                        "source {:?} shares port {port} with source {:?}, which serves it; give both the same \
                         maxConnections, maxConnectionsPerSender and idleTimeoutSecs, or this one another port",
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
        id: &str,
        dialect: crate::io::sql::Dialect,
        kind: crate::plan::topology::SourceMeta,
        errors: &mut ItemErrors<'_>,
    ) {
        let what = format!("source {id:?}");
        // The possessive leaves the id unquoted: `source "pg"'s query` reads as broken punctuation.
        let whose = format!("source {id}'s");
        let query = match crate::io::sql::Query::parse(self.query, dialect) {
            Ok(query) => Some(query),
            Err(why) => {
                errors.at("query").push(format!("{whose} query: {why}"));
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
                            "{whose} position names `{column}`, which is not one of its fields; a position is read \
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
                            errors.at("mode").push(format!("{whose} startFrom: {why}"));
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
                                "{whose} query names :last.{column}, and `{column}` is not one of its fields; :last \
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
                        .push(format!("{whose} key names `{column}`, which is not one of its fields"));
                }
            }
            SqlModeDto::EveryPoll => {}
        }
        if let (Some(query), SqlModeDto::Comparing { .. } | SqlModeDto::EveryPoll) = (&query, self.mode)
            && let Some(column) = query.last_columns().first()
        {
            errors.at("query").push(format!(
                "{whose} query names :last.{column}; only a mode that reads past the last row sent has one"
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

/// What every API source's address and schedule must be: a URL at `field`, TLS settings only where TLS would use
/// them, and a poll interval of at least a second. The URL, when it is a whole http:// or https:// one.
fn check_api_source(
    what: &str,
    (field, url): (&'static str, &str),
    tls: &TlsDto,
    poll_interval: PollIntervalSecs,
    errors: &mut ItemErrors<'_>,
) -> Option<reqwest::Url> {
    check_tls_reaches(tls, &[(field, url)], what, errors);
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

/// A destination over the HTTP core, sending records to `url` with no headers of its own, as its sizes and stall
/// threshold say. Each kind sets what it sends on top of it.
fn to_http_core(
    url: String,
    tls: Option<Tls>,
    in_flight: &InFlight,
    max_request_bytes: usize,
    buffer_bytes: Option<BufferBytes>,
    stall_after_secs: &StallAfter,
) -> HttpDestinationConfig {
    HttpDestinationConfig {
        url,
        zone_urls: BTreeMap::new(),
        body: RequestBody::default(),
        payload: Payload::Record,
        compression: crate::plan::topology::Compression::None,
        headers: Vec::new(),
        credentials: Vec::new(),
        tls,
        in_flight: in_flight.start(affords(buffer_bytes, max_request_bytes)),
        max_request_bytes,
        buffer_bytes: effective_buffer_bytes(buffer_bytes, max_request_bytes),
        stall_after: std::time::Duration::from_secs(stall_after_secs.0),
    }
}

/// An HTTP request is sent as none or gzip.
fn check_http_compression(id: &str, compression: &CompressionDto, errors: &mut ItemErrors<'_>) {
    if !matches!(compression, CompressionDto::None | CompressionDto::Gzip) {
        errors.at("compression").push(format!(
            "destination {id:?} compresses with {compression:?}; set compression to none or gzip"
        ));
    }
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
    tls: &TlsDto,
    of: &str,
) -> DestinationKind {
    DestinationKind::Objects(ObjectsDestinationConfig {
        cloud,
        prefix: o.prefix.0.clone(),
        format: match o.format {
            ObjectFormatDto::Ndjson => ObjectFormat::Ndjson,
            ObjectFormatDto::Json => ObjectFormat::Json,
            ObjectFormatDto::Parquet => ObjectFormat::Parquet {
                native_variants: o.variant == Some(VariantDto::Native),
            },
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
        tls: to_tls(tls, of, Side::Client),
        in_flight: o.in_flight.0,
        max_object_bytes: o.max_object_bytes.0,
        buffer_bytes: o.buffer_bytes.map_or(BufferBytes::OBJECT_DEFAULT, |b| b.0),
        stall_after: std::time::Duration::from_secs(o.stall_after_secs.0),
        file_interval: std::time::Duration::from_secs(o.file_interval_secs.0),
    })
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

/// Looks up the `kind` (source or pipeline) that `route` names, `id`, in `known`. The editor leaves a new route's source
/// and pipeline empty until they are chosen, so an empty one is reported as not chosen, not as naming something gone.
fn route_names<T: Copy>(route: &str, kind: &str, id: &str, known: &HashMap<String, T>) -> Result<T, String> {
    if id.is_empty() {
        return Err(format!("route {route:?} names no {kind}"));
    }
    known
        .get(id)
        .copied()
        .ok_or_else(|| format!("route {route:?} references unknown {kind} {id:?}"))
}

fn to_route(dto: &RouteDto, maps: &IdMaps, group: &OnException) -> Result<RouteConfig, String> {
    let source = route_names(&dto.id, "source", &dto.source, &maps.sources)?;
    let pipeline = route_names(&dto.id, "pipeline", &dto.pipeline, &maps.pipelines)?;
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
        | StepDto::Remove { id, .. }
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
        let want: Vec<(&str, u64, Option<u64>, Option<u64>)> = [
            ("inFlight", 1, Some(MAX_SENDERS as u64), InFlight::DEFAULT as u64),
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
        assert!(c.partition_by.is_empty() && c.tls.is_none());
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
