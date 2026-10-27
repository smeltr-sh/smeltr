//! A deployment as a directory of small JSON files. One file per pipeline, so editing
//! pipeline A never touches B's file and a diff on one file is the review for one pipeline's change. Sources,
//! destinations, routes and lookup tables stay single files.
//!
//! ```text
//! system.json            {"version": 1, "exceptions": ...?, "name": ...?}
//! sources.json           [SourceDto, ...]        (optional, default [])
//! destinations.json      [DestinationDto, ...]   (optional, default [])
//! routes.json            [RouteDto, ...]         (optional, default [])
//! lookup-tables.json     [LookupTableDto, ...]   (optional, default [])
//! grok-patterns.json     [GrokPatternDto, ...]   (optional, default [])
//! schemas.json           [SchemaDto, ...]        (optional, default [])
//! pipelines/<slug>.json  one PipelineDto per file; the filename is `slugify` of the pipeline's own `id`
//! lookups/<name>         a file a lookup table names, CSV or MMDB
//! ```
//!
//! A leader's repository holds one such bundle per worker group under `groups/<group-id>/`, plus a `shared/` bundle
//! every group draws on (design doc, Control plane). A group runs `shared/` merged with its own subtree, which is
//! what `load_group_dir` reads; `load_bundle_dir` reads a lone bundle at a root.
//!
//! The strict loaders assemble one `FileConfig` and hand it to `config_file::validate`, so a loaded bundle is always
//! runnable. `read_bundle_dir_raw` is the lenient read the GUI shows a broken bundle through.

use std::collections::BTreeMap;
use std::path::Path;

use serde::Serialize;
use serde::de::DeserializeOwned;

use super::dto::{DestinationDto, FileConfig, GrokPatternDto, LookupTableDto, PipelineDto, RouteDto, SchemaDto, SourceDto};
use super::load::{BundleAt, LoadError};
use super::{CURRENT_VERSION, LoadedSystem};

/// `system.json`. A key it does not know fails the load, as in every DTO, so a misspelled or retired key cannot run the
/// default in its place. The key is caught here rather than refused by serde, so the GUI's read keeps the fields it
/// does know, and an edit of the manifest repairs the file without losing them.
#[derive(serde::Deserialize, serde::Serialize)]
struct Manifest {
    version: u32,
    /// The group's answer to every exception kind: a property of the deployment.
    #[serde(default, rename = "exceptions", skip_serializing_if = "Option::is_none")]
    on_exception: Option<super::dto::OnExceptionDto>,
    /// A group's display name. Absent means show the id instead; `shared/` has no manifest and so never has one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    name: Option<String>,
    /// Every key the fields above do not name. Never written: `manifest_bytes` builds a manifest from a `FileConfig`.
    #[serde(flatten)]
    unknown: BTreeMap<String, serde_json::Value>,
}

impl Manifest {
    /// What the load says about the keys it does not know, if there are any.
    fn unknown_keys(&self) -> Option<String> {
        let keys: Vec<String> = self.unknown.keys().map(|k| format!("`{k}`")).collect();
        match keys.len() {
            0 => None,
            1 => Some(format!("unknown field {}", keys[0])),
            _ => Some(format!("unknown fields {}", keys.join(", "))),
        }
    }
}

/// A failure with the file it happened at.
#[derive(Debug)]
pub enum BundleError {
    /// A required file is missing, or a read failed.
    Io {
        path: String,
        message: String,
    },
    Json {
        path: String,
        source: serde_json::Error,
    },
    /// The filename and the file's own `id` disagree.
    PipelineIdMismatch {
        path: String,
        filename_id: String,
        content_id: String,
    },
    /// Two pipeline ids produce one filename.
    DuplicateSlug {
        slug: String,
        first_id: String,
        second_id: String,
    },
    /// One id defined in both `shared/` and a group's own subtree. `validate` would report the duplicate over the
    /// merged whole, without saying which file each came from.
    DuplicateAcrossSubtrees {
        kind: &'static str,
        id: String,
        first: String,
        second: String,
    },
    /// A `system.json` of a version this build does not read.
    UnsupportedVersion {
        path: String,
        version: u32,
    },
    /// What `config_file::validate` reported over the assembled whole.
    Load(LoadError),
}

impl std::fmt::Display for BundleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BundleError::Io { path, message } => write!(f, "{path}: {message}"),
            BundleError::Json { path, source } => write!(f, "{path}: invalid JSON: {source}"),
            BundleError::PipelineIdMismatch {
                path,
                filename_id,
                content_id,
            } => write!(
                f,
                "{path}: filename says pipeline id {filename_id:?}, but the file's own \
                 \"id\" is {content_id:?}"
            ),
            BundleError::DuplicateSlug {
                slug,
                first_id,
                second_id,
            } => write!(
                f,
                "pipeline ids {first_id:?} and {second_id:?} both produce the filename \
                 \"pipelines/{slug}.json\" -- rename one of them"
            ),
            BundleError::DuplicateAcrossSubtrees {
                kind,
                id,
                first,
                second,
            } => write!(
                f,
                "{kind} id {id:?} is defined in both {first} and {second} -- one of them has to \
                 change, since a group loads both"
            ),
            BundleError::UnsupportedVersion { path, version } => {
                write!(f, "{path}: {}", LoadError::UnsupportedVersion(*version))
            }
            BundleError::Load(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for BundleError {}

// -- reading ------------------------------------------------------------

/// A named-file tree: the seam a bundle is read through. A directory is one (`DirFileSet`); a commit is another,
/// supplied by whoever has a repository to read it from.
pub trait FileSet {
    /// Bytes at `path` (forward-slash-separated), or `None` if absent.
    fn read(&mut self, path: &str) -> std::io::Result<Option<Vec<u8>>>;
    /// Every `.json` file directly under `dir`, as paths relative to the bundle.
    fn json_paths_under(&mut self, dir: &str) -> std::io::Result<Vec<String>>;

    /// Every pipeline file. A deployment reads these and nothing else under a directory, which is what keeps
    /// `samples/` out of what reaches a worker.
    fn pipeline_paths(&mut self) -> std::io::Result<Vec<String>> {
        self.json_paths_under("pipelines")
    }
}

/// The directory half of the seam. `leader::tree::TreeFiles` is the other: a commit.
pub struct DirFileSet<'a> {
    pub root: &'a Path,
}

impl FileSet for DirFileSet<'_> {
    fn read(&mut self, path: &str) -> std::io::Result<Option<Vec<u8>>> {
        match std::fs::read(self.root.join(path)) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }

    fn json_paths_under(&mut self, dir: &str) -> std::io::Result<Vec<String>> {
        let root = self.root.join(dir);
        if !root.is_dir() {
            return Ok(Vec::new());
        }
        let mut out = Vec::new();
        for entry in std::fs::read_dir(&root)? {
            let name = entry?.file_name();
            let name = name.to_string_lossy();
            if name.ends_with(".json") {
                out.push(format!("{dir}/{name}"));
            }
        }
        Ok(out)
    }
}

fn io_err(path: &str, e: std::io::Error) -> BundleError {
    BundleError::Io {
        path: path.to_string(),
        message: e.to_string(),
    }
}

fn parse_json<T: DeserializeOwned>(path: &str, bytes: &[u8]) -> Result<T, BundleError> {
    serde_json::from_slice(bytes).map_err(|source| BundleError::Json {
        path: path.to_string(),
        source,
    })
}

fn require(files: &mut dyn FileSet, path: &str) -> Result<Vec<u8>, BundleError> {
    files
        .read(path)
        .map_err(|e| io_err(path, e))?
        .ok_or_else(|| io_err(path, std::io::ErrorKind::NotFound.into()))
}

fn read_optional_array<T: DeserializeOwned>(files: &mut dyn FileSet, path: &str) -> Result<Vec<T>, BundleError> {
    match files.read(path).map_err(|e| io_err(path, e))? {
        Some(bytes) => parse_json(path, &bytes),
        None => Ok(Vec::new()),
    }
}

/// One bundle's files, parsed and not yet validated. Split out from [`load_bundle_from`] so two of them can be merged
/// before anything checks the whole.
struct Parts {
    manifest: Option<Manifest>,
    sources: Vec<SourceDto>,
    destinations: Vec<DestinationDto>,
    routes: Vec<RouteDto>,
    lookup_tables: Vec<LookupTableDto>,
    grok_patterns: Vec<GrokPatternDto>,
    schemas: Vec<SchemaDto>,
    pipelines: Vec<PipelineDto>,
}

/// Every error names its file, so `at` prefixes the subtree a merged read is in. Empty for a lone bundle, whose
/// paths are already what a reader would type.
fn read_parts(files: &mut dyn FileSet, at: &str) -> Result<Parts, BundleError> {
    let named = |p: &str| format!("{at}{p}");
    let manifest = match files
        .read("system.json")
        .map_err(|e| io_err(&named("system.json"), e))?
    {
        Some(bytes) => Some(parse_json::<Manifest>(&named("system.json"), &bytes)?),
        None => None,
    };
    if let Some(m) = &manifest
        && m.version != CURRENT_VERSION
    {
        return Err(BundleError::UnsupportedVersion {
            path: named("system.json"),
            version: m.version,
        });
    }
    if let Some(unknown) = manifest.as_ref().and_then(Manifest::unknown_keys) {
        return Err(BundleError::Json {
            path: named("system.json"),
            source: serde::de::Error::custom(unknown),
        });
    }

    let mut pipelines = Vec::new();
    for path in files.pipeline_paths().map_err(|e| io_err(&named("pipelines/"), e))? {
        let bytes = require(files, &path)?;
        let pipeline: PipelineDto = parse_json(&named(&path), &bytes)?;
        let filename_id = Path::new(&path)
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or_default()
            .to_string();
        if filename_id != slugify(&pipeline.id) {
            return Err(BundleError::PipelineIdMismatch {
                path: named(&path),
                filename_id,
                content_id: pipeline.id,
            });
        }
        pipelines.push(pipeline);
    }

    Ok(Parts {
        manifest,
        sources: read_optional_array(files, "sources.json").map_err(|e| at_path(e, at))?,
        destinations: read_optional_array(files, "destinations.json").map_err(|e| at_path(e, at))?,
        routes: read_optional_array(files, "routes.json").map_err(|e| at_path(e, at))?,
        lookup_tables: read_optional_array(files, "lookup-tables.json").map_err(|e| at_path(e, at))?,
        grok_patterns: read_optional_array(files, GROK_PATTERNS_FILE).map_err(|e| at_path(e, at))?,
        schemas: read_optional_array(files, SCHEMAS_FILE).map_err(|e| at_path(e, at))?,
        pipelines,
    })
}

/// Move an error's path into the subtree it was read from.
fn at_path(e: BundleError, at: &str) -> BundleError {
    match e {
        BundleError::Io { path, message } => BundleError::Io {
            path: format!("{at}{path}"),
            message,
        },
        BundleError::Json { path, source } => BundleError::Json {
            path: format!("{at}{path}"),
            source,
        },
        other => other,
    }
}

/// Every lookup table declared in `parts`, mapped to `at` -- where the bundle whose `lookup-tables.json` declared it
/// was read from, and what a relative lookup path in it resolves against.
fn declared_in(parts: &Parts, at: &BundleAt) -> crate::config_file::DeclaredAt {
    parts.lookup_tables.iter().map(|t| (t.id.clone(), at.clone())).collect()
}

fn assemble_at(
    manifest: Manifest,
    parts: Parts,
    declared_at: &crate::config_file::DeclaredAt,
) -> Result<LoadedSystem, BundleError> {
    let file = FileConfig {
        version: manifest.version,
        on_exception: manifest.on_exception,
        name: manifest.name,
        sources: parts.sources,
        destinations: parts.destinations,
        pipelines: parts.pipelines,
        routes: parts.routes,
        lookup_tables: parts.lookup_tables,
        grok_patterns: parts.grok_patterns,
        schemas: parts.schemas,
    };
    super::validate_at(file, declared_at).map_err(BundleError::Load)
}

/// Read and validate a bundle directory. A relative lookup path in it resolves against `root`.
pub fn load_bundle_dir(root: &Path) -> Result<LoadedSystem, BundleError> {
    load_bundle_from(&mut DirFileSet { root }, &BundleAt::Dir(root.to_path_buf()))
}

/// Read and validate one bundle out of `files`. A relative lookup path in it resolves against `at`, which is where
/// `files` reads from -- a directory, or a commit.
pub fn load_bundle_from(files: &mut dyn FileSet, at: &BundleAt) -> Result<LoadedSystem, BundleError> {
    let mut parts = read_parts(files, "")?;
    let Some(manifest) = parts.manifest.take() else {
        return Err(io_err("system.json", std::io::ErrorKind::NotFound.into()));
    };
    let declared_at = declared_in(&parts, at);
    assemble_at(manifest, parts, &declared_at)
}

/// Where a leader's repository keeps each group's bundle, and the one every group draws on.
pub const GROUPS_DIR: &str = "groups";
pub const SHARED_DIR: &str = "shared";

/// A bundle's section files: each a JSON array of objects keyed by a unique `id`.
pub const SECTION_FILES: [&str; 6] = [
    "sources.json",
    "destinations.json",
    "routes.json",
    LOOKUP_TABLES_FILE,
    GROK_PATTERNS_FILE,
    SCHEMAS_FILE,
];
pub const LOOKUP_TABLES_FILE: &str = "lookup-tables.json";
pub const GROK_PATTERNS_FILE: &str = "grok-patterns.json";
pub const SCHEMAS_FILE: &str = "schemas.json";

/// Where a bundle keeps its lookup files, and where an upload puts one. A table names its file by a path under this,
/// relative to the bundle that declares the table.
pub const LOOKUPS_DIR: &str = "lookups";

/// Whether `group_id` is safe as a path component. A group id arrives from an admin's request body and from a
/// worker's own bootstrap config, and both end up joined into a path here, so this is a strict allowlist: every
/// encoding trick fails closed without being enumerated, and a leading dot can never produce a hidden directory
/// or `.git`.
pub fn is_safe_group_id(group_id: &str) -> bool {
    !group_id.is_empty()
        && group_id.len() <= 100
        && !group_id.starts_with('.')
        && group_id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.')
}

/// The message an unsafe id is refused with, written once so every caller says the same thing.
pub fn unsafe_group_id(group_id: &str) -> String {
    format!(
        "unsafe worker group id {group_id:?}: only letters, digits, '-', '_' and '.' are allowed, \
         it may not start with '.', and it must be 1-100 characters"
    )
}

/// Whether `name` is usable as a secret's name. A name is written into a bundle, listed in the GUI, and bound into
/// the sealing of its own value, so it has to be stable and unambiguous across all three: one spelling, no
/// whitespace to lose, and nothing that reads differently in JSON than on screen.
pub fn is_safe_secret_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 100
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.')
}

/// The message an unusable secret name is refused with, written once so every caller says the same thing.
pub fn unsafe_secret_name(name: &str) -> String {
    format!(
        "unusable secret name {name:?}: only letters, digits, '-', '_' and '.' are allowed, \
         and it must be 1-100 characters"
    )
}

/// Whether `name` is usable as a worker group's display name. Nothing reads it as a path and nothing requires it
/// to be unique, so anything with a character in it will do -- but a group is drawn by its name and falls back to
/// its id when it has none, so a blank one would draw as an id a reader would take for a name someone chose.
pub fn is_safe_group_name(name: &str) -> bool {
    !name.trim().is_empty()
}

/// The message an unusable group name is refused with, written once so every caller says the same thing.
pub fn unsafe_group_name(name: &str) -> String {
    format!("unusable worker group name {name:?}: it needs a character that is not whitespace")
}

/// A group's own subtree within a leader's repository. The single point where a group id becomes a path, so the
/// safety check cannot be skipped.
///
/// `SHARED_DIR` is reserved: a scope of that name reads the subtree every group draws on, so a group holding it
/// could never be read or written through one.
pub fn group_subtree(group_id: &str) -> Result<String, String> {
    if !is_safe_group_id(group_id) {
        return Err(unsafe_group_id(group_id));
    }
    if group_id == SHARED_DIR {
        return Err(format!(
            "worker group id {group_id:?} is reserved: it names the subtree every group draws on"
        ));
    }
    Ok(format!("{GROUPS_DIR}/{group_id}"))
}

/// Read and validate what `group_id` runs: `shared/` merged with its own subtree.
///
/// Shared comes first in every list. `system.json` is the group's alone: `version` describes the format and
/// `exceptions` is a deployment's own choice, so a shared copy would have nothing to say that the group's does not.
pub fn load_group_dir(root: &Path, group_id: &str) -> Result<LoadedSystem, BundleError> {
    let subtree = group_subtree(group_id).map_err(|message| BundleError::Io {
        path: format!("{GROUPS_DIR}/"),
        message,
    })?;
    let shared_root = root.join(SHARED_DIR);
    let group_root = root.join(&subtree);
    load_group_from(
        &mut DirFileSet { root: &shared_root },
        &BundleAt::Dir(shared_root.clone()),
        &mut DirFileSet { root: &group_root },
        &BundleAt::Dir(group_root.clone()),
        group_id,
    )
}

/// Read and validate what `group_id` runs, out of any two file sets: `shared/` and the group's own subtree. `*_at`
/// is where each was read from, which is what a relative lookup path declared in it resolves against.
///
/// The seam that lets the leader ask whether a commit -- or a tree it has merged but not committed -- would load,
/// which it cannot answer from a directory it never checks out.
pub fn load_group_from(
    shared_files: &mut dyn FileSet,
    shared_at: &BundleAt,
    group_files: &mut dyn FileSet,
    group_at: &BundleAt,
    group_id: &str,
) -> Result<LoadedSystem, BundleError> {
    let subtree = group_subtree(group_id).map_err(|message| BundleError::Io {
        path: format!("{GROUPS_DIR}/"),
        message,
    })?;
    // A `shared/` that is not there reads as one with nothing in it: every file is absent, which `read_parts`
    // already answers with empty parts.
    let shared = read_parts(shared_files, "shared/")?;
    let group_prefix = format!("{subtree}/");
    let mut group = read_parts(group_files, &group_prefix)?;

    let Some(manifest) = group.manifest.take() else {
        return Err(io_err(
            &format!("{group_prefix}system.json"),
            std::io::ErrorKind::NotFound.into(),
        ));
    };

    // Each side's tables resolve against the subtree that declared them, so a shared table names a file in `shared/`
    // and a group's names one in its own -- and moving a pipeline between them moves its files, not its paths.
    let mut declared_at = declared_in(&shared, shared_at);
    declared_at.extend(declared_in(&group, group_at));

    let mut parts = merge(shared, group, &group_prefix)?;
    super::fill_default_group_ids(&mut parts.sources, group_id);
    assemble_at(manifest, parts, &declared_at)
}

/// Shared first, then the group's own. An id in both is refused here rather than by `validate`, which sees one
/// merged list and cannot say which file either came from.
fn merge(shared: Parts, group: Parts, group_at: &str) -> Result<Parts, BundleError> {
    fn check<T>(
        kind: &'static str,
        shared: &[T],
        group: &[T],
        id: impl Fn(&T) -> &str,
        at: &str,
    ) -> Option<BundleError> {
        let clash = group.iter().find(|g| shared.iter().any(|s| id(s) == id(g)))?;
        Some(BundleError::DuplicateAcrossSubtrees {
            kind,
            id: id(clash).to_string(),
            first: format!("shared/{}", kind_file(kind)),
            second: format!("{at}{}", kind_file(kind)),
        })
    }
    let checks = [
        check("source", &shared.sources, &group.sources, |s| &s.id, group_at),
        check(
            "destination",
            &shared.destinations,
            &group.destinations,
            |d| &d.id,
            group_at,
        ),
        check("route", &shared.routes, &group.routes, |r| &r.id, group_at),
        check(
            "lookup table",
            &shared.lookup_tables,
            &group.lookup_tables,
            |t| &t.id,
            group_at,
        ),
        check(
            "grok pattern",
            &shared.grok_patterns,
            &group.grok_patterns,
            |p| &p.id,
            group_at,
        ),
        check("schema", &shared.schemas, &group.schemas, |s| &s.id, group_at),
        check("pipeline", &shared.pipelines, &group.pipelines, |p| &p.id, group_at),
    ];
    if let Some(e) = checks.into_iter().flatten().next() {
        return Err(e);
    }

    fn cat<T>(mut a: Vec<T>, b: Vec<T>) -> Vec<T> {
        a.extend(b);
        a
    }
    Ok(Parts {
        manifest: group.manifest,
        sources: cat(shared.sources, group.sources),
        destinations: cat(shared.destinations, group.destinations),
        routes: cat(shared.routes, group.routes),
        lookup_tables: cat(shared.lookup_tables, group.lookup_tables),
        grok_patterns: cat(shared.grok_patterns, group.grok_patterns),
        schemas: cat(shared.schemas, group.schemas),
        pipelines: cat(shared.pipelines, group.pipelines),
    })
}

/// The file an id of this kind is declared in, for an error that has to name both.
fn kind_file(kind: &str) -> &'static str {
    match kind {
        "source" => "sources.json",
        "destination" => "destinations.json",
        "route" => "routes.json",
        "lookup table" => "lookup-tables.json",
        "grok pattern" => GROK_PATTERNS_FILE,
        "schema" => SCHEMAS_FILE,
        _ => "pipelines/",
    }
}

/// A lenient read: whatever assembled from the files that parsed, plus `(path, message)` for each file that did not.
/// Says nothing about whether the whole validates; `validation_errors` answers that.
pub struct RawBundle {
    pub file: FileConfig,
    pub unreadable: Vec<(String, String)>,
}

fn read_array_lenient<T: DeserializeOwned>(
    files: &mut dyn FileSet,
    path: &str,
    unreadable: &mut Vec<(String, String)>,
) -> Vec<T> {
    match files.read(path) {
        Ok(Some(bytes)) => match serde_json::from_slice(&bytes) {
            Ok(v) => v,
            Err(e) => {
                unreadable.push((path.to_string(), format!("invalid JSON: {e}")));
                Vec::new()
            }
        },
        Ok(None) => Vec::new(),
        Err(e) => {
            unreadable.push((path.to_string(), e.to_string()));
            Vec::new()
        }
    }
}

/// Read a bundle without discarding a file because another one is broken. For the GUI's reads only; a deployment
/// loads through `load_group_dir`, which is all-or-nothing.
pub fn read_raw_from(files: &mut dyn FileSet) -> RawBundle {
    let mut unreadable = Vec::new();

    // An unreadable manifest is assumed to be the current version, so the rest of the bundle can still be shown --
    // and so can one of a version this build does not read, which is otherwise the one problem validation reports.
    let mut on_exception = None;
    let mut name = None;
    let version = match files.read("system.json") {
        Ok(Some(bytes)) => match serde_json::from_slice::<Manifest>(&bytes) {
            Ok(m) => {
                if m.version != CURRENT_VERSION {
                    let unsupported = LoadError::UnsupportedVersion(m.version).to_string();
                    unreadable.push(("system.json".to_string(), unsupported));
                }
                if let Some(unknown) = m.unknown_keys() {
                    unreadable.push(("system.json".to_string(), format!("invalid JSON: {unknown}")));
                }
                on_exception = m.on_exception.clone();
                name = m.name.clone();
                CURRENT_VERSION
            }
            Err(e) => {
                unreadable.push(("system.json".to_string(), format!("invalid JSON: {e}")));
                CURRENT_VERSION
            }
        },
        Ok(None) => CURRENT_VERSION,
        Err(e) => {
            unreadable.push(("system.json".to_string(), e.to_string()));
            CURRENT_VERSION
        }
    };

    let sources = read_array_lenient(files, "sources.json", &mut unreadable);
    let destinations = read_array_lenient(files, "destinations.json", &mut unreadable);
    let routes = read_array_lenient(files, "routes.json", &mut unreadable);
    let lookup_tables = read_array_lenient(files, "lookup-tables.json", &mut unreadable);
    let grok_patterns = read_array_lenient(files, GROK_PATTERNS_FILE, &mut unreadable);
    let schemas = read_array_lenient(files, SCHEMAS_FILE, &mut unreadable);

    let mut pipelines = Vec::new();
    match files.pipeline_paths() {
        Ok(paths) => {
            for path in paths {
                match files.read(&path) {
                    Ok(Some(bytes)) => match serde_json::from_slice::<PipelineDto>(&bytes) {
                        Ok(p) => pipelines.push(p),
                        Err(e) => unreadable.push((path, format!("invalid JSON: {e}"))),
                    },
                    // Listed, then removed before the read.
                    Ok(None) => {}
                    Err(e) => unreadable.push((path.clone(), e.to_string())),
                }
            }
        }
        Err(e) => unreadable.push(("pipelines/".to_string(), e.to_string())),
    }

    RawBundle {
        file: FileConfig {
            version,
            on_exception,
            name,
            sources,
            destinations,
            pipelines,
            routes,
            lookup_tables,
            grok_patterns,
            schemas,
        },
        unreadable,
    }
}

// -- writing --------------------------------------------------------------

/// A pipeline id as a filename stem: lowercased, every run outside `[a-z0-9]` collapsed to one `-`, ends trimmed.
/// Pipeline ids are free-text names, so this is what keeps the paths portable.
pub fn slugify(id: &str) -> String {
    let mut out = String::with_capacity(id.len());
    let mut last_was_dash = true; // suppresses a leading dash
    for c in id.chars() {
        if c.is_ascii_alphanumeric() {
            out.push(c.to_ascii_lowercase());
            last_was_dash = false;
        } else if !last_was_dash {
            out.push('-');
            last_was_dash = true;
        }
    }
    while out.ends_with('-') {
        out.pop();
    }
    if out.is_empty() { "pipeline".to_string() } else { out }
}

/// Each pipeline's `(slug, PipelineDto)`; two ids on one slug is an error, not a tiebreak.
///
/// The single place a pipeline's filename is decided, so every writer refuses the same collision. Writing both would
/// keep whichever went last and lose the other, which no reader could then tell from a pipeline nobody wrote.
pub fn slugged_pipelines(file: &FileConfig) -> Result<Vec<(String, &PipelineDto)>, BundleError> {
    let mut seen: std::collections::HashMap<String, &str> = std::collections::HashMap::new();
    let mut out = Vec::with_capacity(file.pipelines.len());
    for p in &file.pipelines {
        let slug = slugify(&p.id);
        if let Some(&first_id) = seen.get(&slug) {
            return Err(BundleError::DuplicateSlug {
                slug,
                first_id: first_id.to_string(),
                second_id: p.id.clone(),
            });
        }
        seen.insert(slug.clone(), &p.id);
        out.push((slug, p));
    }
    Ok(out)
}

/// A DTO is plain string-and-number data; serializing it cannot fail.
fn to_json_bytes<T: Serialize>(v: &T) -> Vec<u8> {
    serde_json::to_vec_pretty(v).expect("a config_file DTO always serializes")
}

/// `system.json` for `file`: everything a bundle holds outside its own sections. The GUI's per-section save writes
/// this too, so the manifest has one writer.
pub fn manifest_bytes(file: &FileConfig) -> Vec<u8> {
    to_json_bytes(&Manifest {
        version: file.version,
        on_exception: file.on_exception.clone(),
        name: file.name.clone(),
        unknown: BTreeMap::new(),
    })
}

/// A bundle as its named files, forward-slash-separated. What `load_bundle_dir` reads back, whether it is written to
/// a directory or committed into a tree. The inverse of `load_bundle_dir`'s split, over the config as authored.
pub fn bundle_files(file: &FileConfig) -> Result<Vec<(String, Vec<u8>)>, BundleError> {
    let mut files = vec![
        ("system.json".to_string(), manifest_bytes(file)),
        ("sources.json".to_string(), to_json_bytes(&file.sources)),
        ("destinations.json".to_string(), to_json_bytes(&file.destinations)),
        ("routes.json".to_string(), to_json_bytes(&file.routes)),
        ("lookup-tables.json".to_string(), to_json_bytes(&file.lookup_tables)),
        (GROK_PATTERNS_FILE.to_string(), to_json_bytes(&file.grok_patterns)),
        (SCHEMAS_FILE.to_string(), to_json_bytes(&file.schemas)),
    ];
    for (slug, p) in slugged_pipelines(file)? {
        files.push((format!("pipelines/{slug}.json"), to_json_bytes(p)));
    }
    Ok(files)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config_file::load_str;
    use crate::config_file::test_fixtures::example_json;

    fn temp_dir(label: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "smeltr-bundle-test-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// Write a bundle as a directory. Only tests do: the leader commits trees, and the worker only ever reads one.
    fn save_bundle_dir(file: &FileConfig, root: &std::path::Path) {
        std::fs::create_dir_all(root.join("pipelines")).unwrap();
        for (path, bytes) in bundle_files(file).unwrap() {
            std::fs::write(root.join(path), bytes).unwrap();
        }
    }

    fn example_file() -> FileConfig {
        serde_json::from_str(example_json()).unwrap()
    }

    /// Write `json` as a bundle at `root/sub`.
    fn write_subtree(root: &std::path::Path, sub: &str, json: &str) {
        load_str(json).expect("the fixture must be a valid bundle");
        let file: FileConfig = serde_json::from_str(json).unwrap();
        save_bundle_dir(&file, &root.join(sub));
    }

    fn metrics_source(id: &str) -> String {
        format!(r#"{{"id":{id:?},"type":"metrics","bucketSeconds":10}}"#)
    }

    #[test]
    fn a_group_runs_its_own_subtree_merged_with_shared() {
        let root = temp_dir("group-merge");
        write_subtree(
            &root,
            "shared",
            &format!(r#"{{"version":1,"sources":[{}]}}"#, metrics_source("everywhere")),
        );
        write_subtree(
            &root,
            "groups/west",
            &format!(r#"{{"version":1,"sources":[{}]}}"#, metrics_source("west-only")),
        );
        write_subtree(
            &root,
            "groups/east",
            &format!(r#"{{"version":1,"sources":[{}]}}"#, metrics_source("east-only")),
        );

        let west = load_group_dir(&root, "west").unwrap();
        let ids: Vec<&str> = west.topology.sources.iter().map(|s| s.name.as_str()).collect();
        assert_eq!(ids, ["everywhere", "west-only"], "shared first, then the group's own");

        let east = load_group_dir(&root, "east").unwrap();
        let ids: Vec<&str> = east.topology.sources.iter().map(|s| s.name.as_str()).collect();
        assert_eq!(ids, ["everywhere", "east-only"], "a group never sees another's");

        std::fs::remove_dir_all(&root).ok();
    }

    /// A Kafka source in `shared/` that names no group reads as each worker group's own, so every worker group that
    /// runs it reads the whole topic. One that names a group keeps it.
    #[test]
    fn a_kafka_source_with_no_group_reads_as_its_worker_group() {
        let root = temp_dir("group-default");
        let kafka = |id: &str, group: &str| {
            format!(r#"{{"id":{id:?},"type":"kafka","brokers":"b:9092","topics":[{id:?}]{group}}}"#)
        };
        write_subtree(
            &root,
            "shared",
            &format!(
                r#"{{"version":1,"sources":[{},{}]}}"#,
                kafka("blank", ""),
                kafka("named", r#","groupId":"replay""#)
            ),
        );
        for g in ["west", "east"] {
            write_subtree(&root, &format!("groups/{g}"), r#"{"version":1}"#);
            let system = load_group_dir(&root, g).unwrap();
            let groups: Vec<&str> = system
                .topology
                .sources
                .iter()
                .map(|s| match &s.kind {
                    crate::plan::topology::SourceKind::Kafka(k) => k.group_id.as_str(),
                    _ => unreachable!(),
                })
                .collect();
            assert_eq!(groups, [format!("smeltr.{g}").as_str(), "replay"]);
        }
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn ordinary_group_ids_are_accepted() {
        for id in ["ingest-1", "prod_orders", "standalone", "a", "v1.2", "A9"] {
            assert!(is_safe_group_id(id), "{id} should be allowed");
        }
    }

    #[test]
    fn traversal_and_separators_are_rejected() {
        for id in [
            "..",
            "../x",
            "../../ESCAPED",
            "a/../../b",
            "a/b",
            "a\\b",
            "/etc/passwd",
            "C:\\x",
        ] {
            assert!(!is_safe_group_id(id), "{id} must be rejected");
        }
    }

    #[test]
    fn a_leading_dot_is_rejected_so_an_id_cannot_become_a_hidden_dir() {
        assert!(!is_safe_group_id(".git"));
        assert!(!is_safe_group_id(".hidden"));
    }

    #[test]
    fn empty_and_overlong_ids_are_rejected() {
        assert!(!is_safe_group_id(""));
        assert!(!is_safe_group_id(&"a".repeat(101)));
        assert!(is_safe_group_id(&"a".repeat(100)));
    }

    #[test]
    fn non_ascii_and_control_characters_are_rejected() {
        for id in ["grοup", "a\0b", "a b", "a\nb", "a;rm -rf /", "a$(x)", "a%2fb"] {
            assert!(!is_safe_group_id(id), "{id:?} must be rejected");
        }
    }

    /// Nothing reads a display name as a path, so the only thing that would leave a group undrawable is a name
    /// with nothing in it to draw.
    #[test]
    fn a_group_name_needs_a_character_that_is_not_whitespace() {
        for blank in ["", " ", "\t", "  \n "] {
            assert!(!is_safe_group_name(blank), "{blank:?} must be refused");
        }
        for named in ["a", "West Coast \u{2014} Ingest", "  padded  ", "\u{65e5}\u{672c}"] {
            assert!(is_safe_group_name(named), "{named:?} must be allowed");
        }
    }

    /// A scope of this name reads `shared/` itself (`config_api::Scope::parse`), so a group holding it would
    /// be written into the shared bundle and never readable as its own.
    #[test]
    fn the_shared_subtree_s_own_name_is_not_a_group_id() {
        let err = group_subtree(SHARED_DIR).expect_err("reserved");
        assert!(err.contains(SHARED_DIR), "names what was wrong: {err}");
        assert_eq!(group_subtree("shared-west").unwrap(), "groups/shared-west");
    }

    #[test]
    fn a_group_id_that_would_traverse_never_becomes_a_path() {
        assert!(group_subtree("../../ESCAPED").is_err());
        assert!(group_subtree("a/b").is_err());
        assert!(group_subtree(".hidden").is_err());
        assert_eq!(group_subtree("prod-west").unwrap(), "groups/prod-west");

        let root = temp_dir("group-traversal");
        let err = load_group_dir(&root, "../../ESCAPED").unwrap_err().to_string();
        assert!(err.contains("unsafe worker group id"), "{err}");
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn a_group_with_no_shared_subtree_loads_on_its_own() {
        let root = temp_dir("group-no-shared");
        write_subtree(
            &root,
            "groups/west",
            &format!(r#"{{"version":1,"sources":[{}]}}"#, metrics_source("west-only")),
        );
        let west = load_group_dir(&root, "west").unwrap();
        assert_eq!(west.topology.sources.len(), 1);
        std::fs::remove_dir_all(&root).ok();
    }

    /// `validate` would report the duplicate over the merged whole without saying where either came from.
    #[test]
    fn one_id_in_both_subtrees_names_both_files() {
        let root = temp_dir("group-duplicate");
        let same = format!(r#"{{"version":1,"sources":[{}]}}"#, metrics_source("orders"));
        write_subtree(&root, "shared", &same);
        write_subtree(&root, "groups/west", &same);

        let err = load_group_dir(&root, "west").unwrap_err().to_string();
        assert!(err.contains("shared/sources.json"), "{err}");
        assert!(err.contains("groups/west/sources.json"), "{err}");
        assert!(err.contains("orders"), "{err}");

        std::fs::remove_dir_all(&root).ok();
    }

    /// A group's grok names a pattern `shared/` declares: the file round-trips, and the merge carries it.
    #[test]
    fn a_groups_grok_names_a_pattern_shared_declares() {
        let root = temp_dir("group-grok-pattern");
        write_subtree(
            &root,
            "shared",
            r#"{"version":1,"grokPatterns":[{"id":"VENDOR","pattern":"acme-%{INT}"}]}"#,
        );
        let file: FileConfig = serde_json::from_str(
            r#"{"version":1,"pipelines":[{"id":"p","name":"p","steps":[{"id":"g","type":"parser","input":"_raw",
              "kind":{"type":"grok","patterns":["%{VENDOR:v}"]},"declared":[{"name":"v","type":"str"}]}]}]}"#,
        )
        .unwrap();
        save_bundle_dir(&file, &root.join("groups/west"));

        let system = load_group_dir(&root, "west").unwrap();
        assert_eq!(system.pipelines[0].grok_patterns.len(), 1);
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn a_group_without_its_own_system_json_does_not_load() {
        let root = temp_dir("group-no-manifest");
        write_subtree(&root, "shared", r#"{"version":1}"#);
        std::fs::create_dir_all(root.join("groups/west")).unwrap();
        let err = load_group_dir(&root, "west").unwrap_err().to_string();
        assert!(err.contains("groups/west/system.json"), "{err}");
        std::fs::remove_dir_all(&root).ok();
    }

    /// An error from a merged read says which subtree the file is in.
    #[test]
    fn a_broken_file_in_shared_is_reported_under_shared() {
        let root = temp_dir("group-broken-shared");
        write_subtree(&root, "groups/west", r#"{"version":1}"#);
        std::fs::create_dir_all(root.join("shared")).unwrap();
        std::fs::write(root.join("shared/sources.json"), b"not json").unwrap();
        let err = load_group_dir(&root, "west").unwrap_err().to_string();
        assert!(err.starts_with("shared/sources.json"), "{err}");
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn slugify_handles_spaces_punctuation_and_case() {
        assert_eq!(slugify("Main pipeline"), "main-pipeline");
        assert_eq!(slugify("  leading/trailing  "), "leading-trailing");
        assert_eq!(slugify("a---b"), "a-b");
        assert_eq!(slugify(""), "pipeline");
        assert_eq!(slugify("!!!"), "pipeline");
    }

    /// Two ids that produce one filename would write one file and lose the other, so the bundle refuses to name
    /// its own files.
    #[test]
    fn two_pipelines_that_slugify_the_same_are_refused_their_filenames() {
        let mut file = example_file();
        file.pipelines[1].id = file.pipelines[0].id.clone();
        let err = bundle_files(&file).unwrap_err();
        assert!(matches!(err, BundleError::DuplicateSlug { .. }), "{err}");
    }

    #[test]
    fn a_missing_system_json_is_a_clear_error() {
        let dir = temp_dir("missing-manifest");
        let err = load_bundle_dir(&dir).unwrap_err();
        assert!(matches!(err, BundleError::Io { .. }), "{err}");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn sources_destinations_and_routes_default_to_empty_when_absent() {
        let dir = temp_dir("minimal");
        std::fs::write(dir.join("system.json"), br#"{"version":1}"#).unwrap();
        let system = load_bundle_dir(&dir).unwrap();
        assert!(system.topology.sources.is_empty());
        assert!(system.topology.destinations.is_empty());
        assert!(system.topology.routes.is_empty());
        assert!(system.pipelines.is_empty());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_pipeline_filename_that_disagrees_with_its_own_id_is_rejected() {
        let dir = temp_dir("id-mismatch");
        std::fs::write(dir.join("system.json"), br#"{"version":1}"#).unwrap();
        std::fs::create_dir_all(dir.join("pipelines")).unwrap();
        std::fs::write(
            dir.join("pipelines").join("wrong-name.json"),
            br#"{"id":"actual-id","name":"p","steps":[]}"#,
        )
        .unwrap();

        let err = load_bundle_dir(&dir).unwrap_err();
        assert!(matches!(err, BundleError::PipelineIdMismatch { .. }), "{err}");
        assert!(err.to_string().contains("wrong-name"));
        assert!(err.to_string().contains("actual-id"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn an_unsupported_bundle_version_is_rejected() {
        let dir = temp_dir("bad-version");
        std::fs::write(dir.join("system.json"), br#"{"version":99}"#).unwrap();
        let err = load_bundle_dir(&dir).unwrap_err();
        assert!(
            matches!(err, BundleError::UnsupportedVersion { version: 99, .. }),
            "{err}"
        );
        assert!(
            err.to_string()
                .starts_with("system.json: unsupported config version 99"),
            "{err}"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    /// The lenient read names the manifest as the file that does not load, and reads the rest as the current
    /// version, so the GUI shows the bundle beside the one file to fix.
    #[test]
    fn a_lenient_read_names_a_manifest_of_an_unsupported_version() {
        let dir = temp_dir("raw-bad-version");
        std::fs::write(dir.join("system.json"), br#"{"version":99,"name":"West"}"#).unwrap();
        let raw = read_raw_from(&mut DirFileSet { root: &dir });
        assert_eq!(raw.unreadable.len(), 1, "{:?}", raw.unreadable);
        assert_eq!(raw.unreadable[0].0, "system.json");
        assert_eq!(raw.unreadable[0].1, LoadError::UnsupportedVersion(99).to_string());
        assert_eq!(raw.file.version, CURRENT_VERSION);
        assert_eq!(raw.file.name.as_deref(), Some("West"));
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A key the manifest does not know fails the load, rather than load as the default where it meant to deliver.
    #[test]
    fn an_unknown_manifest_key_is_rejected() {
        for key in ["unmatched", "unprocessable"] {
            let dir = temp_dir(&format!("unknown-{key}"));
            std::fs::write(
                dir.join("system.json"),
                format!(r#"{{"version":1,"{key}":"leftovers"}}"#),
            )
            .unwrap();
            let err = load_bundle_dir(&dir).unwrap_err();
            assert!(
                matches!(&err, BundleError::Json { path, .. } if path == "system.json"),
                "{err}"
            );
            assert!(err.to_string().contains(key), "it names the key: {err}");
            std::fs::remove_dir_all(&dir).ok();
        }
    }

    #[test]
    fn malformed_json_in_one_file_names_that_file() {
        let dir = temp_dir("bad-json");
        std::fs::write(dir.join("system.json"), br#"{"version":1}"#).unwrap();
        std::fs::write(dir.join("routes.json"), b"not json").unwrap();
        let err = load_bundle_dir(&dir).unwrap_err();
        match &err {
            BundleError::Json { path, .. } => assert_eq!(path, "routes.json"),
            other => panic!("expected a Json error, got {other:?}"),
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_dangling_reference_across_files_is_still_caught() {
        let dir = temp_dir("dangling");
        std::fs::write(dir.join("system.json"), br#"{"version":1}"#).unwrap();
        std::fs::write(
            dir.join("routes.json"),
            br#"[{"id":"r","source":"nowhere","condition":null,"pipeline":"ghost"}]"#,
        )
        .unwrap();
        let err = load_bundle_dir(&dir).unwrap_err();
        assert!(err.to_string().contains("unknown"), "{err}");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_bundle_with_an_expression_that_does_not_compile_is_rejected() {
        let dir = temp_dir("bad-expression");
        std::fs::write(dir.join("system.json"), br#"{"version":1}"#).unwrap();
        std::fs::create_dir_all(dir.join("pipelines")).unwrap();
        std::fs::write(
            dir.join("pipelines").join("p.json"),
            br#"{"id":"p","name":"p","steps":[{"id":"e","type":"eval","assignments":[{"target":"x","expr":"1 +"}]}]}"#,
        )
        .unwrap();
        let err = load_bundle_dir(&dir).unwrap_err();
        assert!(matches!(err, BundleError::Load(LoadError::Validation(_))), "{err}");
        std::fs::remove_dir_all(&dir).ok();
    }

    // -- read_bundle_dir_raw ------------------------------------------------

    #[test]
    fn a_lenient_read_returns_every_pipeline_that_parses_even_if_one_does_not() {
        let dir = temp_dir("lenient-one-bad-pipeline");
        std::fs::write(dir.join("system.json"), br#"{"version":1}"#).unwrap();
        std::fs::create_dir_all(dir.join("pipelines")).unwrap();
        std::fs::write(
            dir.join("pipelines").join("good.json"),
            br#"{"id":"good","name":"good","steps":[]}"#,
        )
        .unwrap();
        std::fs::write(dir.join("pipelines").join("bad.json"), b"not json").unwrap();

        let raw = read_raw_from(&mut DirFileSet { root: &dir });
        assert_eq!(raw.file.pipelines.len(), 1);
        assert_eq!(raw.file.pipelines[0].id, "good");
        assert_eq!(raw.unreadable.len(), 1);
        assert_eq!(raw.unreadable[0].0, "pipelines/bad.json");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_lenient_read_returns_every_other_section_even_if_one_is_broken() {
        let dir = temp_dir("lenient-one-bad-section");
        std::fs::write(dir.join("system.json"), br#"{"version":1}"#).unwrap();
        std::fs::write(
            dir.join("sources.json"),
            br#"[{"id":"s1","type":"metrics","bucketSeconds":10}]"#,
        )
        .unwrap();
        std::fs::write(dir.join("destinations.json"), b"not json").unwrap();

        let raw = read_raw_from(&mut DirFileSet { root: &dir });
        assert_eq!(raw.file.sources.len(), 1);
        assert!(raw.file.destinations.is_empty());
        assert_eq!(raw.unreadable.len(), 1);
        assert_eq!(raw.unreadable[0].0, "destinations.json");
        assert!(
            raw.unreadable[0].1.starts_with("invalid JSON:"),
            "{:?}",
            raw.unreadable[0]
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_lenient_read_of_a_structurally_broken_but_all_valid_json_bundle_has_no_unreadable_entries() {
        let dir = temp_dir("lenient-dangling-ref");
        std::fs::write(dir.join("system.json"), br#"{"version":1}"#).unwrap();
        std::fs::write(
            dir.join("routes.json"),
            br#"[{"id":"r","source":"nowhere","condition":null,"pipeline":"ghost"}]"#,
        )
        .unwrap();

        let raw = read_raw_from(&mut DirFileSet { root: &dir });
        assert!(raw.unreadable.is_empty(), "{:?}", raw.unreadable);
        assert_eq!(raw.file.routes.len(), 1);
        let errors = super::super::validation_errors(raw.file);
        assert!(!errors.is_empty());
        assert!(errors[0].contains("unknown"), "{errors:?}");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// The group's exceptions, a deployment's own choice and belonging to no section.
    #[test]
    fn the_manifest_carries_the_exceptions_both_ways() {
        use crate::config_file::dto::{AnswerDto, OnExceptionDto};
        let dir = temp_dir("manifest-exceptions");
        let exceptions = OnExceptionDto {
            kinds: [(
                "Unmatched".to_string(),
                AnswerDto::Deliver {
                    to: vec!["leftovers".to_string()],
                },
            )]
            .into(),
            ..Default::default()
        };
        let file = FileConfig {
            on_exception: Some(exceptions.clone()),
            version: CURRENT_VERSION,
            name: None,
            sources: vec![],
            destinations: vec![],
            pipelines: vec![],
            routes: vec![],
            lookup_tables: vec![],
            grok_patterns: vec![],
            schemas: vec![],
        };
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("system.json"), manifest_bytes(&file)).unwrap();
        let raw = read_raw_from(&mut DirFileSet { root: &dir });
        assert!(raw.unreadable.is_empty());
        assert_eq!(raw.file.on_exception, Some(exceptions));
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A group's display name lives in its own manifest, and is absent rather than defaulted when nobody set one.
    #[test]
    fn the_manifest_carries_the_name_both_ways_and_is_absent_by_default() {
        let dir = temp_dir("manifest-name");
        let file = FileConfig {
            on_exception: None,
            version: CURRENT_VERSION,
            name: Some("Order Processing".to_string()),
            sources: vec![],
            destinations: vec![],
            pipelines: vec![],
            routes: vec![],
            lookup_tables: vec![],
            grok_patterns: vec![],
            schemas: vec![],
        };
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("system.json"), manifest_bytes(&file)).unwrap();
        let raw = read_raw_from(&mut DirFileSet { root: &dir });
        assert!(raw.unreadable.is_empty());
        assert_eq!(raw.file.name.as_deref(), Some("Order Processing"));

        let unnamed = FileConfig { name: None, ..file };
        std::fs::write(dir.join("system.json"), manifest_bytes(&unnamed)).unwrap();
        let raw = read_raw_from(&mut DirFileSet { root: &dir });
        assert_eq!(raw.file.name, None, "no name was set, so none is invented on load");
        assert!(
            !String::from_utf8_lossy(&manifest_bytes(&unnamed)).contains("name"),
            "an absent name is not written either"
        );
        std::fs::remove_dir_all(&dir).ok();
    }
}
