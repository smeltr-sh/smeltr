//! The REST API behind the GUI's sources, destinations, routes, lookup tables and pipelines pages, over a branch in
//! the leader's repository (`leader::git`, `config_file::bundle`).
//!
//! Nothing is checked out. A read is a commit (`Reading`, over `leader::tree::TreeFiles`); a save builds its tree
//! from that commit and swaps the branch onto it (`Writing`). The swap is a compare-and-swap on what was read, so a
//! save against a branch that moved underneath it is refused rather than overwriting whoever moved it -- which is
//! the reviewer committing a fix to an author's branch, as much as two authors on one -- and no writer holds a lock.
//!
//! Reads are lenient (`read_raw_from`): one broken file must not blank every other page. `errors` on a read is
//! advisory.
//!
//! Writes re-read the bundle, apply one change, and save only if the whole result validates
//! (`config_file::validation_errors`). A top-level file that failed to parse is overwritten only by the mutation that
//! replaces that section; a pipeline file that failed to parse is touched only by the PUT or DELETE naming its id.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use axum::Json;
use axum::extract::Extension;
use axum::extract::Path as AxPath;
use axum::extract::Query;
use axum::extract::State;
use axum::extract::{FromRequestParts, RawPathParams};
use axum::http::request::Parts;
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;

use smeltr::config_file::{
    self, DestinationDto, FileConfig, LookupSourceDto, LookupTableDto, PipelineDto, RawBundle, RouteDto, SourceDto,
};
use smeltr::leader::envelope::{ApiError, Envelope, Refusal};
use smeltr::leader::store::Actor;
use smeltr::leader::tree::TreeFiles;
use smeltr::leader::{blocking, blocking_answer, git};
use smeltr::plan::config::LookupFile;
use smeltr::startup_config::leader_data_dir;

/// What a request that names no branch acts on: `main`, so it reads what the fleet runs, and a write is refused by
/// [`Writing::open`] the same as one aimed at `main` by name.
pub const UNNAMED_BRANCH: &str = git::MAIN_BRANCH;

/// The header carrying the commit an editor read. Absent means "I did not look", which is how a first write and a
/// scripted caller both behave, and both skip the staleness check.
const BASE_COMMIT_HEADER: &str = "x-smeltr-base-commit";

#[derive(serde::Deserialize, Default)]
pub struct BranchQuery {
    branch: Option<String>,
}

impl BranchQuery {
    pub fn name(&self) -> &str {
        self.branch.as_deref().unwrap_or(UNNAMED_BRANCH)
    }
}

/// Which bundle a request names: the one every group draws on, or one group's own subtree (design doc, Control
/// plane). The single point the area in a request's path becomes a path in the repository, so an id
/// `git::group_subtree` would refuse can never reach one.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Scope(String);

impl Scope {
    /// `raw` as a request's path names the area: `"shared"`, or a group id. Refuses whatever `git::group_subtree`
    /// would, with the same message.
    fn parse(raw: &str) -> Result<Scope, Refusal> {
        if raw == smeltr::config_file::SHARED_DIR {
            return Ok(Scope(smeltr::config_file::SHARED_DIR.to_string()));
        }
        Ok(Scope(git::group_subtree(raw)?))
    }

    /// Where this scope's bundle sits within the repository: `shared` or `groups/<id>`.
    fn prefix(&self) -> &str {
        &self.0
    }
}

pub fn base_commit(headers: &HeaderMap) -> Option<String> {
    headers
        .get(BASE_COMMIT_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
}

/// Where a request writes: the area in its path, the branch its query names and the commit its headers say the
/// editor read. One extractor, so a handler takes the three together and opens its `Writing` from them.
pub struct Addressed {
    pub area: String,
    pub branch: String,
    pub base: Option<String>,
}

#[axum::async_trait]
impl<S: Send + Sync> FromRequestParts<S> for Addressed {
    type Rejection = axum::response::Response;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let params = RawPathParams::from_request_parts(parts, state)
            .await
            .map_err(IntoResponse::into_response)?;
        let area = params
            .iter()
            .find(|(name, _)| *name == "area")
            .map(|(_, area)| area.to_string());
        // A route under `/api/areas/:area` always has one; its absence is the router's mistake, not the caller's.
        let area = area
            .ok_or_else(|| Envelope::<Save>::refused(Refusal::internal("this route names no area")).into_response())?;
        let Query(q) = Query::<BranchQuery>::from_request_parts(parts, state)
            .await
            .map_err(IntoResponse::into_response)?;
        Ok(Addressed {
            area,
            branch: q.name().to_string(),
            base: base_commit(&parts.headers),
        })
    }
}

impl Addressed {
    /// The `Writing` this request's save goes through. Spawns git, so it runs under `blocking`.
    pub fn open(&self) -> Result<Writing, NotWritten> {
        Writing::open(&self.branch, self.base.as_deref(), &self.area)
    }

    /// One section's save on the branch and area this request names, as `Writing::save` makes it.
    async fn save(
        self,
        actor: Actor,
        touches: Touches,
        message: String,
        mutate: impl FnOnce(&mut FileConfig) + Send + 'static,
    ) -> Envelope<Save> {
        blocking_save(move || {
            self.open()
                .map_or_else(Save::refused, |writing| writing.save(touches, &message, &actor, mutate))
        })
        .await
    }
}

/// The bundle a request reads: a commit, and where in it the edited group's bundle sits. A commit is immutable, so
/// nothing can move under a reader and no lock is needed; nothing is checked out either, so a read never makes a
/// working tree.
///
/// `main` is readable -- it is what deploys move. A branch nobody has saved on reads as `main`, which is what it
/// would be branched from. The first edit is what starts a branch (design doc, Config plane), so a read must not:
/// otherwise a
/// mistyped `?branch=` becomes a branch on everybody's list, and the reader is shown an empty bundle rather than
/// what they would be editing.
pub struct Reading {
    repo: PathBuf,
    /// The branch this resolved to, which is `main` for one nobody has saved on.
    pub branch: String,
    pub commit: String,
    prefix: String,
}

impl Reading {
    /// The worker group this reads the subtree of. `None` for `shared/`.
    pub fn group_id(&self) -> Option<&str> {
        group_of(&self.prefix)
    }

    pub fn open(branch: &str, scope: &str) -> Result<Reading, Refusal> {
        Reading::open_in(&leader_data_dir(), branch, scope)
    }

    /// `data_dir` explicit rather than read from the environment -- what a test needs to run against a scratch
    /// repository.
    pub(crate) fn open_in(data_dir: &Path, branch: &str, scope: &str) -> Result<Reading, Refusal> {
        let prefix = Scope::parse(scope)?.prefix().to_string();
        let repo = git::repo_path(data_dir);
        if !git::repo_ready(&repo) {
            git::init_bare_repo(&repo)?;
            git::ensure_main_commit(&repo)?;
        }
        let started = branch == git::MAIN_BRANCH || git::branch_exists(&repo, branch)?;
        let branch = if started { branch } else { git::MAIN_BRANCH };
        let commit = git::branch_tip(&repo, branch)
            .ok_or_else(|| Refusal::not_found(format!("there is no branch {branch:?} to read")))?;
        Ok(Reading {
            repo,
            branch: branch.to_string(),
            commit,
            prefix,
        })
    }

    /// The scope as it was at `commit`, a full commit id in `branch`'s history or `main`'s: one save, or where the
    /// branch left `main`. A commit in neither is refused, so a reader sees only what was on the branch it names
    /// or what that branch could have started from. `data_dir` explicit, as for `open_in`.
    pub(crate) fn open_commit_in(data_dir: &Path, branch: &str, scope: &str, commit: &str) -> Result<Reading, Refusal> {
        git::check_commit_id(commit)?;
        if commit.len() != 40 {
            return Err(Refusal::invalid(format!(
                "{commit:?} is not a whole commit id: 40 hexadecimal characters"
            )));
        }
        let reading = Reading::open_in(data_dir, branch, scope)?;
        let commit = commit.to_ascii_lowercase();
        let in_history = |rev: &str| git::reachable_from(&reading.repo, &commit, rev);
        // The tip just read, not the branch's name: a deploy deletes the branch, and its commits outlive it.
        if !in_history(&reading.commit)? && !in_history(git::MAIN_BRANCH)? {
            return Err(Refusal::not_found(format!(
                "{commit} is not in the history of {branch:?} or of {}",
                git::MAIN_BRANCH
            )));
        }
        Ok(Reading {
            branch: reading.branch,
            ..Reading::at_commit_in(data_dir, scope, &commit)?
        })
    }

    /// The scope at `commit`, a whole commit id the leader itself names, such as the deployed one. Unlike
    /// [`Reading::open_commit_in`], which reads a commit a caller asked for, it is not checked against any history.
    pub(crate) fn at_commit_in(data_dir: &Path, scope: &str, commit: &str) -> Result<Reading, Refusal> {
        Ok(Reading {
            repo: git::repo_path(data_dir),
            branch: git::MAIN_BRANCH.to_string(),
            commit: commit.to_string(),
            prefix: Scope::parse(scope)?.prefix().to_string(),
        })
    }

    pub fn files(&self) -> TreeFiles<'_> {
        TreeFiles::new(&self.repo, &self.commit, self.prefix.clone())
    }

    /// A second scope at the exact commit this one already resolved to, not a fresh branch-tip lookup: a caller
    /// merging two scopes together (`load_group_from`'s merge, restated for a group's own read) must not have the
    /// branch move between the two reads and see a hybrid that never existed on either commit.
    pub(crate) fn sibling(&self, scope: &str) -> Result<Reading, Refusal> {
        Ok(Reading {
            repo: self.repo.clone(),
            branch: self.branch.clone(),
            commit: self.commit.clone(),
            prefix: Scope::parse(scope)?.prefix().to_string(),
        })
    }

    /// Where a lookup path in this bundle resolves.
    pub fn at(&self) -> config_file::BundleAt {
        config_file::BundleAt::Commit {
            repo: self.repo.clone(),
            commit: self.commit.clone(),
            prefix: self.prefix.clone(),
        }
    }

    pub fn raw(&self) -> RawBundle {
        config_file::read_raw_from(&mut self.files())
    }
}

/// A branch open for writing: the commit a save is based on, and what the branch must still be for it to land.
///
/// `main` is refused here rather than by asking callers not to: only a deploy moves it, and an administrator who
/// edits it anyway is reported rather than prevented (design doc, Config plane).
#[derive(Debug)]
pub struct Writing {
    repo: PathBuf,
    branch: String,
    prefix: String,
    /// The commit the save is applied to. A branch nobody has saved on starts at `main`.
    pub parent: String,
    /// What the branch must currently be. `None` is a first save, which must create the branch rather than move it.
    expected: Option<String>,
    /// Where a lookup file is read from: the parent, or the parent with the files this save adds (`stage`).
    seen: String,
    /// Files that land in the same commit as the config a `save` writes (`stage`).
    staged: Vec<git::FileEdit>,
}

/// A branch that is not where the writer read it, said as two commits rather than as a sentence. The editor holds
/// the only copy of the change, so it has to be able to offer to carry it somewhere -- which means knowing what it
/// has and what the branch became, without reading the message.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Conflict {
    /// The commit the editor read and saved against.
    pub base: String,
    /// Where the branch is now. Absent when it is gone.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub now: Option<String>,
}

/// A save that was not written: why, at its status, and the two commits when the branch moving is why.
#[derive(Debug)]
pub struct NotWritten {
    pub refusal: Refusal,
    pub conflict: Option<Conflict>,
}

impl From<Refusal> for NotWritten {
    fn from(refusal: Refusal) -> Self {
        NotWritten {
            refusal,
            conflict: None,
        }
    }
}

impl From<smeltr::leader::Panicked> for NotWritten {
    fn from(panicked: smeltr::leader::Panicked) -> Self {
        Refusal::from(panicked).into()
    }
}

impl From<git::GitError> for NotWritten {
    fn from(e: git::GitError) -> Self {
        Refusal::from(e).into()
    }
}

/// For a route that answers with no `conflict` of its own: an upload, a capture being saved.
impl From<NotWritten> for Refusal {
    fn from(not_written: NotWritten) -> Self {
        not_written.refusal
    }
}

impl Writing {
    /// `base` is the commit the editor read. It is checked here so the answer can name it, and again by the swap,
    /// which is what makes the answer true.
    pub fn open(branch: &str, base: Option<&str>, scope: &str) -> Result<Writing, NotWritten> {
        Writing::open_in(&leader_data_dir(), branch, base, scope)
    }

    /// `data_dir` explicit rather than read from the environment -- what a test needs to run against a scratch
    /// repository, and what `worker_groups_api` uses in production since it is handed `AppState`'s `data_dir`
    /// rather than reading the environment itself.
    pub(crate) fn open_in(
        data_dir: &Path,
        branch: &str,
        base: Option<&str>,
        scope: &str,
    ) -> Result<Writing, NotWritten> {
        // Before the branch: a write that names neither is answered for the scope it forgot.
        let prefix = Scope::parse(scope)?.prefix().to_string();
        if branch == git::MAIN_BRANCH {
            return Err(Refusal::invalid(format!(
                "{} is what deploys move and is not edited directly -- edit a branch and deploy it",
                git::MAIN_BRANCH
            ))
            .into());
        }
        let repo = git::repo_path(data_dir);
        if !git::repo_ready(&repo) {
            git::init_bare_repo(&repo)?;
            git::ensure_main_commit(&repo)?;
        }
        let expected = git::branch_tip(&repo, branch);
        let parent = expected
            .clone()
            .or_else(|| git::main_head(&repo))
            .ok_or_else(|| Refusal::internal(format!("{} has no commit to save onto", git::MAIN_BRANCH)))?;
        if let Some(base) = base
            && base != parent
        {
            return Err(moved_since_you_loaded_it(base, Some(&parent)));
        }
        Ok(Writing {
            prefix,
            repo,
            branch: branch.to_string(),
            seen: parent.clone(),
            parent,
            expected,
            staged: Vec::new(),
        })
    }

    pub fn read(&self) -> RawBundle {
        config_file::read_raw_from(&mut TreeFiles::new(&self.repo, &self.parent, self.prefix.clone()))
    }

    /// The commit a save is based on, as a `FileSet` -- what a check that must agree with the write itself reads
    /// from.
    pub fn files(&self) -> TreeFiles<'_> {
        TreeFiles::new(&self.repo, &self.parent, self.prefix.clone())
    }

    /// The repository saved into, and so the one whose lookup store an upload goes to.
    pub fn repo(&self) -> &Path {
        &self.repo
    }

    /// The bundle saved into, as a directory of the repository.
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    pub fn at(&self) -> config_file::BundleAt {
        config_file::BundleAt::Commit {
            repo: self.repo.clone(),
            commit: self.seen.clone(),
            prefix: self.prefix.clone(),
        }
    }

    /// Commit `edits`, each named relative to the bundle, and answer where the branch ends up. A save that changed
    /// nothing leaves it where it was -- and a first save that changed nothing does not start it.
    ///
    /// `author` is the account the gate resolved, which is what the commit records. Every caller takes it from an
    /// `Extension`, so no handler can name an author of its own.
    pub fn commit(&self, edits: Vec<git::FileEdit>, message: &str, author: &Actor) -> Result<String, NotWritten> {
        let edits = self.in_repo(edits);
        match git::commit_edits(
            &self.repo,
            &self.branch,
            &self.parent,
            self.expected.as_deref(),
            &edits,
            message,
            author,
        ) {
            Ok(git::Committed::At(commit)) => Ok(commit),
            Ok(git::Committed::Unchanged) => Ok(self.parent.clone()),
            Ok(git::Committed::Moved(now)) => Err(moved_since_you_loaded_it(&self.parent, now.as_deref())),
            Err(e) => Err(e.into()),
        }
    }

    /// Sets the files the next `save` commits beside the config. Lookup files are read from the parent with them laid
    /// over it, a commit no branch names, so a table is checked against a file this save adds.
    fn stage(&mut self, files: Vec<git::FileEdit>) -> Result<(), Refusal> {
        // Once per Writing: a second staging would lay its files over the parent, not over the first.
        debug_assert!(self.staged.is_empty(), "a Writing stages once");
        if files.is_empty() {
            return Ok(());
        }
        self.seen = git::stage_edits(&self.repo, &self.parent, &self.in_repo(files.clone()))?;
        self.staged = files;
        Ok(())
    }

    fn in_repo(&self, edits: Vec<git::FileEdit>) -> Vec<git::FileEdit> {
        edits
            .into_iter()
            .map(|e| git::FileEdit {
                path: format!("{}/{}", self.prefix, e.path),
                content: e.content,
            })
            .collect()
    }

    /// Removes this whole bundle in one commit -- every path under it, not named one by one. `git::commit_edits`
    /// removes a tree the same way it removes a blob: one path, gone, whatever it holds.
    pub fn remove_scope(&self, message: &str, author: &Actor) -> Result<String, NotWritten> {
        let edit = git::FileEdit {
            path: self.prefix.clone(),
            content: None,
        };
        match git::commit_edits(
            &self.repo,
            &self.branch,
            &self.parent,
            self.expected.as_deref(),
            &[edit],
            message,
            author,
        ) {
            Ok(git::Committed::At(commit)) => Ok(commit),
            Ok(git::Committed::Unchanged) => Ok(self.parent.clone()),
            Ok(git::Committed::Moved(now)) => Err(moved_since_you_loaded_it(&self.parent, now.as_deref())),
            Err(e) => Err(e.into()),
        }
    }
}

/// `config_file::validation_errors` for a bundle read from `at`. A lookup path in a bundle is relative to the
/// bundle, so validating one needs to say where it was read from.
pub fn errors_in(file: FileConfig, at: &config_file::BundleAt) -> Vec<config_file::ValidationError> {
    let declared_at = file.lookup_tables.iter().map(|t| (t.id.clone(), at.clone())).collect();
    config_file::validation_errors_at(file, &declared_at)
}

/// The problems a read reports, over the bundle that scope actually runs. For a group that is `shared/` merged
/// with its own subtree, the same thing a save is judged by: a route of its own naming a source `shared/`
/// declares is not a problem, and the subtree read alone calls it unknown.
///
/// Advisory either way. A read is lenient so one broken file cannot blank every page, and what this returns is
/// what the GUI shows beside the pages it could still draw.
fn errors_of_read(reading: &Reading, raw: &RawBundle) -> Vec<config_file::ValidationError> {
    if !reading.prefix.starts_with(&format!("{}/", config_file::GROUPS_DIR)) {
        return errors_in(raw.file.clone(), &reading.at());
    }

    // Leniently, like the read it reports on: a strict load stops at the first file it cannot parse, which would
    // take every other problem in the bundle with it. Each subtree keeps its own `at`, so a relative lookup path
    // resolves against the bundle that declared it.
    let shared = Reading {
        repo: reading.repo.clone(),
        branch: reading.branch.clone(),
        commit: reading.commit.clone(),
        prefix: config_file::SHARED_DIR.to_string(),
    };
    let group_id = reading.group_id().unwrap_or_default();
    errors_of_merge(&shared.raw(), shared.at(), raw, reading.at(), group_id)
}

/// The worker group whose subtree `prefix` is. `None` for `shared/`.
fn group_of(prefix: &str) -> Option<&str> {
    prefix.strip_prefix(&format!("{}/", config_file::GROUPS_DIR))
}

/// The problems of the bundle a group runs, from lenient reads of `shared/` and of the group's own subtree, each
/// with the `at` it was read from. A file `shared/` could not parse is among them, named by its path in the
/// repository; the group's own unreadable files are not, since the caller already reports those.
fn errors_of_merge(
    shared_raw: &RawBundle,
    shared_at: config_file::BundleAt,
    raw: &RawBundle,
    at: config_file::BundleAt,
    group_id: &str,
) -> Vec<config_file::ValidationError> {
    let mut declared_at: config_file::DeclaredAt = shared_raw
        .file
        .lookup_tables
        .iter()
        .map(|t| (t.id.clone(), shared_at.clone()))
        .collect();
    declared_at.extend(raw.file.lookup_tables.iter().map(|t| (t.id.clone(), at.clone())));

    // Shared first, as the merge that runs orders them. `system.json` is the group's alone.
    let mut merged = FileConfig {
        version: raw.file.version,
        on_exception: raw.file.on_exception.clone(),
        name: raw.file.name.clone(),
        sources: [&shared_raw.file.sources[..], &raw.file.sources[..]].concat(),
        destinations: [&shared_raw.file.destinations[..], &raw.file.destinations[..]].concat(),
        pipelines: [&shared_raw.file.pipelines[..], &raw.file.pipelines[..]].concat(),
        routes: [&shared_raw.file.routes[..], &raw.file.routes[..]].concat(),
        lookup_tables: [&shared_raw.file.lookup_tables[..], &raw.file.lookup_tables[..]].concat(),
        grok_patterns: [&shared_raw.file.grok_patterns[..], &raw.file.grok_patterns[..]].concat(),
    };
    config_file::fill_default_group_ids(&mut merged.sources, group_id);

    // A file `shared/` could not parse breaks this group too. Named with its subtree: a group has a `system.json`
    // of its own, and the two must not read as one file.
    let mut errors: Vec<config_file::ValidationError> = shared_raw
        .unreadable
        .iter()
        .map(|(path, message)| format!("{}/{path}: {message}", config_file::SHARED_DIR).into())
        .collect();
    errors.extend(config_file::validation_errors_at(merged, &declared_at));
    errors
}

/// Reports a broken config at startup without refusing to serve: the GUI is how a human fixes it.
pub fn startup_validate(reading: &Reading) {
    // A commit with nothing authored in it is not a broken config.
    if !matches!(
        config_file::FileSet::read(&mut reading.files(), "system.json"),
        Ok(Some(_))
    ) {
        return;
    }
    let raw = reading.raw();
    let shown = format!("{}/{}", reading.branch, reading.prefix);
    if !raw.unreadable.is_empty() {
        tracing::warn!(
            target: "config.files_unreadable",
            subject = %shown,
            files = raw.unreadable.len(),
            detail = %raw.unreadable.iter().map(|(p, m)| format!("{p}: {m}")).collect::<Vec<_>>().join("; "),
            "files in the branch the fleet runs could not be read"
        );
    }
    let errors = errors_in(raw.file, &reading.at());
    if !errors.is_empty() {
        tracing::warn!(
            target: "config.does_not_compile",
            subject = %shown,
            errors = errors.len(),
            detail = %errors.iter().map(|e| e.to_string()).collect::<Vec<_>>().join("; "),
            "the branch the fleet runs does not currently compile -- open the GUI to fix it"
        );
    }
}

/// What a save answers with.
#[derive(Debug, Default, serde::Serialize)]
pub struct Save {
    /// The commit this save produced, so the editor can send it as its base on the next one.
    #[serde(skip_serializing_if = "Option::is_none")]
    commit: Option<String>,
    /// Present when the branch moved: what the editor had and where the branch went.
    #[serde(skip_serializing_if = "Option::is_none")]
    conflict: Option<Conflict>,
}

impl Save {
    pub(crate) fn done(commit: String) -> Envelope<Save> {
        Envelope::ok(Save {
            commit: Some(commit),
            conflict: None,
        })
    }

    /// A save that was not written, with the two commits when the branch moving is why.
    pub(crate) fn refused(not_written: NotWritten) -> Envelope<Save> {
        let NotWritten { refusal, conflict } = not_written;
        Envelope::with(Save { commit: None, conflict }, refusal.status, refusal.errors)
    }
}

/// The section a mutation replaces, and so the only file `edits_for` writes.
#[derive(PartialEq, Eq)]
enum Touches {
    Sources,
    Destinations,
    Routes,
    LookupTables,
    GrokPatterns,
    OnlyPipelines,
    OnlyManifest,
}

fn json_bytes<T: serde::Serialize>(value: &T) -> Vec<u8> {
    serde_json::to_vec_pretty(value).expect("a config_file DTO always serializes")
}

/// The edits that turn `old` into `file`, each path relative to the bundle. A save writes only the file its section
/// owns, so every other file keeps its bytes and an absent one stays absent: rewriting what a save did not touch
/// would make two branches that edited different sections conflict on files neither of them changed.
///
/// A pipeline save writes the pipelines that changed and removes the ones that are gone. A pipeline file is removed
/// only if it was readable in `old`; one that was not is not in `old.file` to compare against.
///
/// `Err` when two pipeline ids name one file. Every writer asks `slugged_pipelines` the same question, so a save
/// that could not be read back is refused rather than keeping whichever pipeline was written last.
fn edits_for(file: &FileConfig, old: &RawBundle, touches: Touches) -> Result<Vec<git::FileEdit>, Refusal> {
    let section = |name: &str, bytes: Vec<u8>| {
        Ok(vec![git::FileEdit {
            path: name.to_string(),
            content: Some(bytes),
        }])
    };
    match touches {
        Touches::Sources => return section("sources.json", json_bytes(&file.sources)),
        Touches::Destinations => return section("destinations.json", json_bytes(&file.destinations)),
        Touches::Routes => return section("routes.json", json_bytes(&file.routes)),
        Touches::GrokPatterns => {
            return section(config_file::GROK_PATTERNS_FILE, json_bytes(&file.grok_patterns));
        }
        Touches::LookupTables => {
            let mut edits = section("lookup-tables.json", json_bytes(&file.lookup_tables))?;
            // Only a table names a lookup file, so one the old tables named and the new ones do not is named by
            // nothing. A commit that still names it keeps its own copy of the pointer, so a rollback is unharmed.
            let named: HashSet<&str> = file.lookup_tables.iter().filter_map(lookup_file_of).collect();
            let unnamed: HashSet<&str> = old.file.lookup_tables.iter().filter_map(lookup_file_of).collect();
            edits.extend(unnamed.difference(&named).map(|path| git::FileEdit {
                path: path.to_string(),
                content: None,
            }));
            return Ok(edits);
        }
        Touches::OnlyManifest => return section("system.json", config_file::manifest_bytes(file)),
        Touches::OnlyPipelines => {}
    }

    let pipelines = config_file::slugged_pipelines(file).map_err(|e| Refusal::invalid(e.to_string()))?;
    // Compared as JSON values: `PipelineDto` has no `PartialEq`, and its serialized form is what the file holds.
    let before: HashMap<&str, serde_json::Value> = old
        .file
        .pipelines
        .iter()
        .map(|p| {
            (
                p.id.as_str(),
                serde_json::to_value(p).expect("a pipeline DTO serializes"),
            )
        })
        .collect();
    let mut edits = Vec::new();
    for (slug, p) in &pipelines {
        let unchanged = before
            .get(p.id.as_str())
            .is_some_and(|was| *was == serde_json::to_value(p).expect("a pipeline DTO serializes"));
        if !unchanged {
            edits.push(git::FileEdit {
                path: format!("pipelines/{slug}.json"),
                content: Some(json_bytes(p)),
            });
        }
    }
    let new_slugs: HashSet<&str> = pipelines.iter().map(|(slug, _)| slug.as_str()).collect();
    for p in &old.file.pipelines {
        let slug = config_file::slugify(&p.id);
        if !new_slugs.contains(slug.as_str()) {
            edits.push(git::FileEdit {
                path: format!("pipelines/{slug}.json"),
                content: None,
            });
        }
    }
    Ok(edits)
}

/// The tree a save would leave behind, without building one: `edits` laid over what the parent commit holds. A
/// pending delete reads as absent, so a removed pipeline is gone from the listing as well as from the read.
struct Pending<'a> {
    under: TreeFiles<'a>,
    edits: &'a [git::FileEdit],
}

impl<'a> Pending<'a> {
    fn pending(&self, path: &str) -> Option<&'a git::FileEdit> {
        self.edits.iter().rev().find(|e| e.path == path)
    }
}

impl config_file::FileSet for Pending<'_> {
    fn read(&mut self, path: &str) -> std::io::Result<Option<Vec<u8>>> {
        match self.pending(path) {
            Some(edit) => Ok(edit.content.clone()),
            None => self.under.read(path),
        }
    }

    fn json_paths_under(&mut self, dir: &str) -> std::io::Result<Vec<String>> {
        let mut paths = self.under.json_paths_under(dir)?;
        let under = format!("{}/", dir.trim_end_matches('/'));
        for edit in self.edits {
            if !edit.path.starts_with(&under) || !edit.path.ends_with(".json") {
                continue;
            }
            match edit.content {
                Some(_) if !paths.contains(&edit.path) => paths.push(edit.path.clone()),
                None => paths.retain(|p| p != &edit.path),
                Some(_) => {}
            }
        }
        Ok(paths)
    }
}

/// A merged load's problems as a list `refuse_worse` can compare. A failure that is not validation -- an
/// unreadable file, an id declared in both subtrees -- stops the load before validation runs, so it is the one
/// thing there is to say about that bundle.
fn errors_of(loaded: Result<config_file::LoadedSystem, config_file::BundleError>) -> Vec<config_file::ValidationError> {
    match loaded {
        Ok(_) => Vec::new(),
        Err(config_file::BundleError::Load(config_file::LoadError::Validation(errs))) => errs,
        Err(e) => vec![e.to_string().into()],
    }
}

/// What every group would run once this save lands: `shared/` merged with the group's own subtree (design doc,
/// Config plane -- "a group is checked merged with `shared/` before it is committed").
///
/// A scope validated on its own cannot see this. An id is unique within a bundle, but a group runs two of them at
/// once, so `source-1` in `shared/` and `source-1` in a group are each valid alone and collide in the only bundle
/// that is ever executed. Writing `shared/` is checked against every group, since every group merges it; writing a
/// group is checked against that group alone. Then every group is checked against every other, for one consumer
/// group read from two starts (`config_file::check_shared_group_starts`).
///
/// Held to the same bar as the rest of a save: a group already broken still takes an unrelated fix, and only a
/// failure this save introduces is refused.
fn refuse_broken_merge(writing: &Writing, edits: &[git::FileEdit]) -> Result<(), Envelope<Save>> {
    let repo = writing.repo.clone();
    let parent = writing.parent.clone();
    let shared = config_file::SHARED_DIR;

    let writing_shared = writing.prefix == shared;
    let written = writing.prefix.strip_prefix(&format!("{}/", config_file::GROUPS_DIR));
    let mut groups = match smeltr::leader::tree::groups_at(&repo, &parent) {
        Ok(groups) => groups,
        // The group list is unreadable, so there is nothing to check against. The scope's own validation still
        // ran; refusing here would block every save on a repository this cannot enumerate.
        Err(_) if writing_shared => return Ok(()),
        Err(_) => written.map(|id| vec![id.to_string()]).unwrap_or_default(),
    };
    if !writing_shared && written.is_none() {
        return Ok(());
    }
    // A group this save creates is not on the branch yet, and is checked like the rest.
    if let Some(id) = written
        && !groups.iter().any(|g| g == id)
    {
        groups.push(id.to_string());
    }

    // Every group as it loads before and after this save, for the check across groups below.
    let mut befores = Vec::new();
    let mut afters = Vec::new();
    for group in groups {
        let loaded_before = smeltr::leader::tree::load_group_at(&repo, &parent, &group);
        if !writing_shared && written != Some(group.as_str()) {
            // This save does not reach the group: it loads the same after.
            if let Ok(loaded) = loaded_before {
                befores.push((group.clone(), loaded.topology.clone()));
                afters.push((group, loaded.topology));
            }
            continue;
        }

        let group_prefix = match config_file::group_subtree(&group) {
            Ok(prefix) => prefix,
            Err(_) => continue,
        };
        let at = |prefix: &str| config_file::BundleAt::Commit {
            repo: repo.clone(),
            commit: writing.seen.clone(),
            prefix: prefix.to_string(),
        };
        let overlay = |prefix: &str| Pending {
            under: TreeFiles::new(&repo, &parent, prefix.to_string()),
            edits,
        };
        let mut plain_shared = TreeFiles::new(&repo, &parent, shared.to_string());
        let mut plain_group = TreeFiles::new(&repo, &parent, group_prefix.clone());
        let mut pending_shared = overlay(shared);
        let mut pending_group = overlay(&group_prefix);

        let loaded_after = config_file::load_group_from(
            if writing_shared {
                &mut pending_shared
            } else {
                &mut plain_shared
            },
            &at(shared),
            if writing_shared {
                &mut plain_group
            } else {
                &mut pending_group
            },
            &at(&group_prefix),
            &group,
        );
        if let Ok(before) = &loaded_before {
            befores.push((group.clone(), before.topology.clone()));
        }
        if let Ok(after) = &loaded_after {
            afters.push((group.clone(), after.topology.clone()));
        }
        let before = errors_of(loaded_before);
        let after = errors_of(loaded_after);

        // Named, because on a `shared/` write the group that breaks is not the one being edited.
        refuse_worse(after, before).map_err(|mut response| {
            for e in &mut response.errors {
                e.message = format!("worker group {group:?}: {}", e.message);
            }
            response
        })?;
    }

    let starts = |groups: &[(String, smeltr::plan::topology::TopologyConfig)]| {
        let groups: Vec<_> = groups
            .iter()
            .map(|(group, topology)| (group.clone(), topology))
            .collect();
        config_file::check_shared_group_starts(&groups)
    };
    refuse_worse(starts(&afters), starts(&befores))
}

impl Writing {
    /// One save: read the branch's commit, apply `mutate`, validate, and swap the branch onto the result with the
    /// files staged. The swap is a compare-and-swap on the commit that was read, so a writer who lost a race is
    /// refused rather than overwriting the winner, and no lock is held. It spawns git, which is why every caller runs
    /// it through `blocking`.
    fn save(
        &self,
        touches: Touches,
        message: &str,
        author: &Actor,
        mutate: impl FnOnce(&mut FileConfig),
    ) -> Envelope<Save> {
        let at = self.at();
        let old = self.read();

        let mut file = old.file.clone();
        mutate(&mut file);

        // `shared/` has to stand on its own: every group draws on it, so it can reference nothing that only one of
        // them holds. A group is the other way round -- it runs `shared/` merged with its own subtree, and a route of
        // its own may name a source `shared/` declares, which the subtree read alone calls unknown. Judging a group by
        // its subtree refused every legitimate use of the thing `shared/` exists for; `refuse_broken_merge` below is
        // what holds a group to the bundle it actually runs.
        if self.prefix == config_file::SHARED_DIR {
            // The old config is judged where it was saved: a save that rewrites a lookup file in place has already
            // staged the new bytes at the path the old config names.
            let before = config_file::BundleAt::Commit {
                repo: self.repo.clone(),
                commit: self.parent.clone(),
                prefix: self.prefix.clone(),
            };
            let old_errors = errors_in(old.file.clone(), &before);
            if let Err(response) = refuse_worse(errors_in(file.clone(), &at), old_errors) {
                return response;
            }
        }

        let mut edits = match edits_for(&file, &old, touches) {
            Ok(edits) => edits,
            Err(e) => return Envelope::refused(e),
        };
        edits.extend(self.staged.iter().cloned());
        if let Err(response) = refuse_broken_merge(self, &edits) {
            return response;
        }
        match self.commit(edits, message, author) {
            Ok(commit) => Save::done(commit),
            Err(e) => Save::refused(e),
        }
    }
}

/// What a writer is told when the branch is not where they read it. Their change is the one thing here that is *not*
/// in git, so the message must not send them to reload before they have carried it over.
fn moved_since_you_loaded_it(base: &str, now: Option<&str>) -> NotWritten {
    let where_now = match now {
        Some(now) => format!("it is now at {}", git::short(now)),
        None => "it is gone".to_string(),
    };
    NotWritten {
        refusal: Refusal::conflict(format!(
            "this branch moved since you loaded it: you have {}, {where_now}. This save was not written, so your \
             change is in this editor and nowhere else -- carry it over before reloading past it. Whoever moved \
             the branch has theirs in a commit.",
            git::short(base)
        )),
        conflict: Some(Conflict {
            base: base.to_string(),
            now: now.map(str::to_string),
        }),
    }
}

/// A save lands unless it makes the error list *worse* (design doc, Config plane). A new mistake never lands; a
/// branch that is
/// already broken still takes fixes, one section at a time.
///
/// A branch is allowed to be broken, and has to be: bringing `main` in can leave a reference dangling, and the edit
/// that repairs it names what only `main` held, so a rule of "no errors at all" leaves the author unable to merge
/// and unable to fix. What must always run is `main`, and the deploy refuses a tree that does not load
/// (`leader::git::groups_that_do_not_load`), which is where that belongs.
fn refuse_worse(
    new_errors: Vec<config_file::ValidationError>,
    old_errors: Vec<config_file::ValidationError>,
) -> Result<(), Envelope<Save>> {
    let old_set: HashSet<&String> = old_errors.iter().map(|e| &e.message).collect();
    let introduced: Vec<&config_file::ValidationError> =
        new_errors.iter().filter(|e| !old_set.contains(&e.message)).collect();
    if introduced.is_empty() {
        return Ok(());
    }
    Err(Envelope::refused(Refusal::all(
        StatusCode::UNPROCESSABLE_ENTITY,
        introduced.into_iter().map(ApiError::from_validation).collect(),
    )))
}

// -- GET /api/areas/:area -------------------------------------------------

#[derive(Default, serde::Serialize)]
pub struct Config {
    config: Option<FileConfig>,
    /// The commit this was read at. An editor sends it back on the save, and a save is refused if the branch has
    /// moved past it.
    #[serde(skip_serializing_if = "Option::is_none")]
    commit: Option<String>,
}

fn unreadable_errors(raw: &RawBundle) -> Vec<ApiError> {
    raw.unreadable
        .iter()
        .map(|(path, message)| ApiError::new(format!("{path}: {message}")))
        .collect()
}

fn open_for_read(branch: &str, scope: &str) -> Result<Reading, Refusal> {
    Reading::open(branch, scope)
}

#[derive(serde::Deserialize)]
pub struct ConfigQuery {
    #[serde(flatten)]
    branch: BranchQuery,
    /// A commit to read at instead of the branch's tip (`Reading::open_commit_in`).
    at: Option<String>,
}

impl ConfigQuery {
    /// `area` at `at`, or at the branch's tip where none is named.
    pub(crate) fn open_in(&self, data_dir: &Path, area: &str) -> Result<Reading, Refusal> {
        let branch = self.branch.name();
        match &self.at {
            Some(at) => Reading::open_commit_in(data_dir, branch, area, at),
            None => Reading::open_in(data_dir, branch, area),
        }
    }
}

pub async fn get_config(AxPath(area): AxPath<String>, Query(q): Query<ConfigQuery>) -> Envelope<Config> {
    blocking_answer(move || config_in(&leader_data_dir(), &area, &q)).await
}

fn config_in(data_dir: &Path, area: &str, q: &ConfigQuery) -> Envelope<Config> {
    let reading = match q.open_in(data_dir, area) {
        Ok(reading) => reading,
        Err(e) => return Envelope::refused(e),
    };
    let raw = reading.raw();
    let mut errors = unreadable_errors(&raw);
    errors.extend(errors_of_read(&reading, &raw).iter().map(ApiError::from_validation));
    let status = read_status(&raw.unreadable);
    Envelope::with(
        Config {
            config: Some(raw.file),
            commit: Some(reading.commit),
        },
        status,
        errors,
    )
}

/// What a read answers at: a file it could not read is the leader's to fix, and the read still answers what it
/// read beside it. Validation errors alone are warnings, since a branch may be broken while it is worked on.
pub(crate) fn read_status<T>(unreadable: &[T]) -> StatusCode {
    match unreadable.is_empty() {
        true => StatusCode::OK,
        false => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

/// Every save runs off the async executor: it spawns git, and it holds a lock while it does.
pub(crate) async fn blocking_save(f: impl FnOnce() -> Envelope<Save> + Send + 'static) -> Envelope<Save> {
    blocking_answer(f).await
}

// -- sources / destinations / routes ---------------------------------------

pub async fn put_sources(
    State(state): State<smeltr::leader::AppState>,
    Extension(actor): Extension<Actor>,
    at: Addressed,
    Json(sources): Json<Vec<SourceDto>>,
) -> Envelope<Save> {
    let named = (sources.iter())
        .filter_map(|s| Some((format!("source {:?}", s.id), s.tls()?.certificate?)))
        .collect();
    if let Some(refused) = refuse_unlisted_certificates(&state, named).await {
        return refused;
    }
    at.save(actor, Touches::Sources, "edit sources".into(), |file| {
        file.sources = sources
    })
    .await
}

pub async fn put_destinations(
    State(state): State<smeltr::leader::AppState>,
    Extension(actor): Extension<Actor>,
    at: Addressed,
    Json(destinations): Json<Vec<DestinationDto>>,
) -> Envelope<Save> {
    let named = (destinations.iter())
        .filter_map(|d| Some((format!("destination {:?}", d.id), d.tls()?.certificate?)))
        .collect();
    if let Some(refused) = refuse_unlisted_certificates(&state, named).await {
        return refused;
    }
    at.save(actor, Touches::Destinations, "edit destinations".into(), |file| {
        file.destinations = destinations
    })
    .await
}

/// The refusal of a save whose connectors name a certificate the leader's list does not hold, each a `(what, name)`
/// pair. A worker would refuse the deploy anyway; refusing here says so to the person who typed the name.
async fn refuse_unlisted_certificates(
    state: &smeltr::leader::AppState,
    named: Vec<(String, String)>,
) -> Option<Envelope<Save>> {
    if named.is_empty() {
        return None;
    }
    let store = state.store.clone();
    let listed = blocking(move || {
        let records = store.list_secrets().map_err(|e| e.to_string())?;
        Ok::<_, String>(
            records
                .into_iter()
                .filter(|r| r.certificate)
                .map(|r| r.name)
                .collect::<Vec<_>>(),
        )
    })
    .await;
    let listed = match listed {
        Ok(listed) => listed,
        Err(e) => return Some(Envelope::refused(Refusal::internal(e))),
    };
    let unlisted: Vec<ApiError> = (named.into_iter())
        .filter(|(_, name)| !listed.contains(name))
        .map(|(what, name)| {
            ApiError::new(format!(
                "{what} names the certificate {name:?}, which is not in Admin → Certificates. Choose one from the list"
            ))
        })
        .collect();
    (!unlisted.is_empty()).then(|| Envelope::refused(Refusal::all(StatusCode::UNPROCESSABLE_ENTITY, unlisted)))
}

/// What a PEM says about itself, so the editor can show a subject and an expiry beside the field it was pasted
/// into. The leader reads it because `leader::tls` already does; a second implementation in the browser would
/// drift from the one that decides whether a certificate is usable.
#[derive(serde::Deserialize)]
pub struct DescribeRequest {
    pub pem: String,
}

#[derive(serde::Serialize, Default)]
pub struct DescribeResponse {
    pub certificates: Vec<smeltr::leader::tls::Described>,
}

/// Refused with why the PEM could not be read, in the words the save would use.
pub async fn describe_certificates(Json(req): Json<DescribeRequest>) -> Envelope<DescribeResponse> {
    smeltr::leader::tls::describe_each(req.pem.as_bytes(), "this")
        .map(|certificates| DescribeResponse { certificates })
        .map_err(Refusal::invalid)
        .into()
}

pub async fn put_routes(
    Extension(actor): Extension<Actor>,
    at: Addressed,
    Json(routes): Json<Vec<RouteDto>>,
) -> Envelope<Save> {
    at.save(actor, Touches::Routes, "edit routes".into(), |file| {
        file.routes = routes
    })
    .await
}

pub async fn put_grok_patterns(
    Extension(actor): Extension<Actor>,
    at: Addressed,
    Json(patterns): Json<Vec<config_file::GrokPatternDto>>,
) -> Envelope<Save> {
    at.save(actor, Touches::GrokPatterns, "edit grokPatterns".into(), |file| {
        file.grok_patterns = patterns
    })
    .await
}

/// The group's answer to every exception kind. It replaces what the manifest said; `null` or absent says nothing,
/// which is the default. An unknown key is refused: a misspelt answer would otherwise clear one.
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ManifestBody {
    exceptions: Option<config_file::OnExceptionDto>,
}

impl ManifestBody {
    fn apply(self, file: &mut FileConfig) {
        file.on_exception = self.exceptions;
    }
}

/// The group's answer to every exception kind. Its own manifest, so `shared/` has no say:
/// every group draws on `shared/` and each answers for its own rows.
pub async fn put_manifest(
    Extension(actor): Extension<Actor>,
    at: Addressed,
    Json(body): Json<ManifestBody>,
) -> Envelope<Save> {
    if at.area == config_file::SHARED_DIR {
        return Envelope::refused(Refusal::invalid(
            "shared/ has no manifest: each group answers for its own rows",
        ));
    }
    at.save(actor, Touches::OnlyManifest, "edit manifest".into(), |file| {
        body.apply(file)
    })
    .await
}

/// A table as the editor sends it. A CSV table may carry its file's new text in `contents`, which is committed under
/// `lookups/` in the same commit as the table and never stored in the table itself.
#[derive(serde::Deserialize)]
pub struct LookupTableEdit {
    #[serde(flatten)]
    table: LookupTableDto,
    contents: Option<String>,
}

pub async fn put_lookup_tables(
    Extension(actor): Extension<Actor>,
    at: Addressed,
    Json(tables): Json<Vec<LookupTableEdit>>,
) -> Envelope<Save> {
    blocking_save(move || {
        at.open()
            .map_or_else(Save::refused, |writing| save_lookup_tables(writing, tables, &actor))
    })
    .await
}

fn save_lookup_tables(mut writing: Writing, edits: Vec<LookupTableEdit>, author: &Actor) -> Envelope<Save> {
    let (repo, prefix) = (writing.repo().to_path_buf(), writing.prefix().to_string());
    let (mut tables, contents): (Vec<_>, Vec<_>) = edits.into_iter().map(|e| (e.table, e.contents)).unzip();
    let mut files = Vec::new();
    for (i, text) in contents.into_iter().enumerate() {
        let Some(text) = text else { continue };
        let naming = match own_file(&tables[i].id) {
            // Another table naming that path, or a file under it, would have its rows changed under it.
            Some(own)
                if !tables.iter().enumerate().any(|(j, t)| {
                    j != i && lookup_file_of(t).is_some_and(|p| p == own || p.starts_with(&format!("{own}/")))
                }) =>
            {
                Naming::AsGiven
            }
            _ => Naming::ByContent,
        };
        match with_contents(&repo, &prefix, &mut tables[i], text, naming) {
            Ok(file) => files.push(file),
            Err(e) => {
                let message = format!("lookup table {:?}: {}", tables[i].id, e.message());
                return Envelope::refused(Refusal::new(e.status, message));
            }
        }
    }
    if let Err(e) = writing.stage(files) {
        return Envelope::refused(e);
    }
    writing.save(Touches::LookupTables, "edit lookupTables", author, |file| {
        file.lookup_tables = tables
    })
}

/// The path a table's rows are saved at: one per table, so each save rewrites the same pointer and git's history of
/// that path is the table's. `None` for an id that is not a file name as it stands: cleaning it could give two ids
/// one path, and two branches could each add one.
fn own_file(id: &str) -> Option<String> {
    let name = format!("{id}.csv");
    (sanitize_filename(&name) == name).then(|| format!("{}/{name}", config_file::LOOKUPS_DIR))
}

/// Points `table` at `text`, put in `repo`'s store as the file of the bundle at `prefix`, and answers its pointer.
fn with_contents(
    repo: &Path,
    prefix: &str,
    table: &mut LookupTableDto,
    text: String,
    naming: Naming,
) -> Result<git::FileEdit, Refusal> {
    let LookupSourceDto::Csv {
        path,
        delimiter,
        headers,
    } = &mut table.source
    else {
        return Err(Refusal::invalid("only a csv table carries `contents`"));
    };
    let delimiter = config_file::to_byte(delimiter, "delimiter").map_err(Refusal::invalid)?;
    let stored_error = |e: std::io::Error| Refusal::internal(format!("couldn't store the file: {e}"));
    let mut incoming = smeltr::lookup::store::Incoming::new(repo).map_err(stored_error)?;
    std::io::Write::write_all(&mut incoming, text.as_bytes()).map_err(stored_error)?;
    let stored = store_lookup_file(repo, prefix, &format!("{}.csv", table.id), incoming, delimiter, naming)?;
    *path = stored.path;
    *headers = stored.headers.unwrap_or_default();
    Ok(stored.edit)
}

/// A lookup file named in the store, ready for the save that points a table at it.
pub struct StoredLookup {
    /// Where a table names it: `lookups/<name>`, relative to its bundle.
    pub path: String,
    /// The pointer to commit at `path`.
    pub edit: git::FileEdit,
    /// A CSV's header row; `None` for an MMDB.
    pub headers: Option<Vec<String>>,
}

/// How a stored lookup file's pointer is named in the bundle.
pub enum Naming {
    /// `filename` itself, replacing whatever pointer is there.
    AsGiven,
    /// `filename` behind the start of the file's hash, so it replaces nothing but the same bytes.
    ByContent,
}

/// Names `incoming` in `repo`'s store as the file `filename` of the bundle at `prefix`, reading a CSV's headers with
/// `delimiter`. Blocking.
pub fn store_lookup_file(
    repo: &Path,
    prefix: &str,
    filename: &str,
    incoming: smeltr::lookup::store::Incoming,
    delimiter: u8,
    naming: Naming,
) -> Result<StoredLookup, Refusal> {
    let pointer = incoming.finish(None).map_err(|e| Refusal::internal(e.to_string()))?;
    // Recorded before anything can refuse it, so that a file this leaves in the store is one retention counts.
    let name = format!("{prefix}/{}", sanitize_filename(filename));
    smeltr::leader::lookups::record_upload(repo, &pointer.oid, &name)
        .map_err(|e| Refusal::internal(format!("couldn't store the upload: {e}")))?;
    // An upload is named by content, not by a counter: a counter restarts at zero with the process, so the next
    // upload of a file called the same thing would silently overwrite a committed one that some other table still
    // names. Two uploads of the same bytes are the same file, which is the one collision worth having.
    let unique_name = match naming {
        Naming::AsGiven => sanitize_filename(filename),
        Naming::ByContent => format!("{}-{}", &pointer.oid[..16], sanitize_filename(filename)),
    };

    let headers = if unique_name.to_ascii_lowercase().ends_with(".mmdb") {
        None
    } else {
        let stored = smeltr::lookup::store::path_of(repo, &pointer.oid);
        let read = smeltr::lookup::store::map(&stored)
            .map_err(|e| smeltr::lookup::LookupError(e.to_string()))
            .and_then(|map| smeltr::lookup::csv_headers(&unique_name, &map, delimiter));
        Some(read.map_err(|e| Refusal::invalid(format!("couldn't read it as a lookup table: {e}")))?)
    };

    // Relative to the bundle that will name it: the same commit is checked out at a different absolute path on the
    // leader and on every worker, so an absolute path here would resolve nowhere on a worker.
    let path = format!("{}/{unique_name}", config_file::LOOKUPS_DIR);
    let edit = git::FileEdit {
        path: path.clone(),
        content: Some(pointer.text().into_bytes()),
    };
    Ok(StoredLookup { path, edit, headers })
}

/// Only characters safe in a filename everywhere; the rest become `_`.
fn sanitize_filename(name: &str) -> String {
    let base = name.rsplit(['/', '\\']).next().unwrap_or(name);
    let cleaned: String = base
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '.' || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    if cleaned.is_empty() || cleaned.starts_with('.') {
        format!("upload{cleaned}")
    } else {
        cleaned
    }
}

#[derive(serde::Deserialize)]
pub struct LookupFileQuery {
    #[serde(flatten)]
    branch: BranchQuery,
    path: String,
    ip: Option<String>,
    /// A commit to read at instead of the branch's tip, as `ConfigQuery`'s.
    at: Option<String>,
}

/// Runs `read` off the executor, since it spawns git, and answers its fields or why not.
async fn lookup_read(
    area: String,
    q: LookupFileQuery,
    read: fn(&Path, &str, &LookupFileQuery) -> Result<serde_json::Value, Refusal>,
) -> Envelope<serde_json::Value> {
    blocking_answer(move || read(&leader_data_dir(), &area, &q).into()).await
}

/// A CSV lookup file's text, for the editor to page, search and change, and the commit it was read at.
pub async fn get_lookup_file(
    AxPath(area): AxPath<String>,
    Query(q): Query<LookupFileQuery>,
) -> Envelope<serde_json::Value> {
    lookup_read(area, q, lookup_text_in).await
}

fn lookup_text_in(data_dir: &Path, area: &str, q: &LookupFileQuery) -> Result<serde_json::Value, Refusal> {
    let (file, commit) = read_lookup_file_in(data_dir, area, q)?;
    let text = String::from_utf8(file.map().map_err(|e| Refusal::internal(e.to_string()))?.to_vec()).map_err(|e| {
        Refusal::invalid(format!(
            "`{}` is not UTF-8 text, so it cannot be edited as a table: {e}",
            q.path
        ))
    })?;
    Ok(serde_json::json!({ "text": text, "commit": commit }))
}

/// A lookup file at the branch's current commit or at `at`, and that commit. Only a path in `lookups/` is read.
fn read_lookup_file_in(data_dir: &Path, area: &str, q: &LookupFileQuery) -> Result<(LookupFile, String), Refusal> {
    let path = q.path.as_str();
    config_file::refuse_outside_lookups(path).map_err(Refusal::invalid)?;
    let branch = q.branch.name();
    let reading = match &q.at {
        Some(at) => Reading::open_commit_in(data_dir, branch, area, at)?,
        None => Reading::open_in(data_dir, branch, area)?,
    };
    let at = reading
        .at()
        .lookup(path)
        .map_err(|e| Refusal::not_found(e.to_string()))?;
    Ok((
        LookupFile {
            text: path.to_string(),
            at,
        },
        reading.commit,
    ))
}

/// What an MMDB file says about itself, and the record for `ip` when one is named: null when no network holds it.
/// The file is read-only here.
pub async fn get_lookup_mmdb(
    AxPath(area): AxPath<String>,
    Query(q): Query<LookupFileQuery>,
) -> Envelope<serde_json::Value> {
    lookup_read(area, q, mmdb_info_in).await
}

/// Network counts by the stored file's hash. A count is a walk of the whole trie, so each file is walked once per
/// process.
static NETWORK_COUNTS: std::sync::LazyLock<std::sync::Mutex<std::collections::HashMap<String, u64>>> =
    std::sync::LazyLock::new(Default::default);

fn mmdb_info_in(data_dir: &Path, area: &str, q: &LookupFileQuery) -> Result<serde_json::Value, Refusal> {
    let ip: Option<std::net::IpAddr> = (q.ip.as_deref())
        .map(|ip| {
            ip.trim()
                .parse()
                .map_err(|_| Refusal::invalid(format!("{ip:?} is not an IP address")))
        })
        .transpose()?;
    let (file, _) = read_lookup_file_in(data_dir, area, q)?;
    let internal = |e: String| Refusal::internal(e);
    let oid = file
        .pointer()
        .map_err(|e| internal(e.to_string()))?
        .map(|p| p.oid)
        .unwrap_or_default();
    let map = file.map().map_err(|e| internal(e.to_string()))?;
    let table = smeltr::lookup::mmdb::MmdbTable::from_map(&q.path, map).map_err(|e| Refusal::invalid(e.to_string()))?;
    let cached = NETWORK_COUNTS.lock().unwrap().get(&oid).copied();
    let networks = cached
        .map_or_else(|| table.count_networks(), Ok)
        .map_err(|e| internal(e.to_string()))?;
    NETWORK_COUNTS.lock().unwrap().insert(oid, networks);
    let meta = table.metadata();
    let mut info = serde_json::json!({
        "databaseType": meta.database_type,
        "buildEpoch": meta.build_epoch,
        "ipVersion": meta.ip_version,
        "languages": meta.languages,
        "networks": networks,
    });
    if let Some(english) = meta.description.get("en") {
        info["description"] = english.as_str().into();
    }
    if let Some(ip) = ip {
        let record = table
            .find(ip)
            .and_then(|record| record.decode::<serde_json::Value>().ok().flatten());
        info["record"] = record.unwrap_or_default();
    }
    Ok(info)
}

// -- pipelines --------------------------------------------------------------

#[derive(Default, serde::Serialize)]
pub struct Pipelines {
    pipelines: Vec<PipelineDto>,
    #[serde(skip_serializing_if = "Option::is_none")]
    commit: Option<String>,
}

pub async fn list_pipelines(AxPath(area): AxPath<String>, Query(q): Query<BranchQuery>) -> Envelope<Pipelines> {
    let branch = q.name().to_string();
    let reading = match blocking(move || open_for_read(&branch, &area)).await {
        Ok(reading) => reading,
        Err(e) => return Envelope::refused(e),
    };
    let raw = reading.raw();
    let mut errors = unreadable_errors(&raw);
    errors.extend(errors_of_read(&reading, &raw).iter().map(ApiError::from_validation));
    let status = read_status(&raw.unreadable);
    Envelope::with(
        Pipelines {
            pipelines: raw.file.pipelines,
            commit: Some(reading.commit),
        },
        status,
        errors,
    )
}

#[derive(Default, serde::Serialize)]
pub struct Pipeline {
    pipeline: Option<PipelineDto>,
    #[serde(skip_serializing_if = "Option::is_none")]
    commit: Option<String>,
}

pub async fn get_pipeline(
    AxPath((area, id)): AxPath<(String, String)>,
    Query(q): Query<BranchQuery>,
) -> Envelope<Pipeline> {
    let branch = q.name().to_string();
    let reading = match blocking(move || open_for_read(&branch, &area)).await {
        Ok(reading) => reading,
        Err(e) => return Envelope::refused(e),
    };
    let raw = reading.raw();
    match raw.file.pipelines.iter().find(|p| p.id == id).cloned() {
        Some(pipeline) => Envelope::ok(Pipeline {
            pipeline: Some(pipeline),
            commit: Some(reading.commit),
        }),
        None => {
            let mut errors = vec![ApiError::new(format!("no pipeline {id:?}"))];
            errors.extend(unreadable_errors(&raw));
            let body = Pipeline {
                pipeline: None,
                commit: Some(reading.commit),
            };
            Envelope::with(body, StatusCode::NOT_FOUND, errors)
        }
    }
}

/// Create-or-replace by the stable `:id`. Display names may repeat or change; the path owns the identity.
pub async fn put_pipeline(
    Extension(actor): Extension<Actor>,
    AxPath((_, id)): AxPath<(String, String)>,
    at: Addressed,
    Json(pipeline): Json<PipelineDto>,
) -> Envelope<Save> {
    // The History page shows this; a person knows a pipeline by its name, not its id.
    let message = format!("edit pipeline {}", pipeline.name);
    at.save(actor, Touches::OnlyPipelines, message, move |file| {
        replace_pipeline(file, &id, pipeline)
    })
    .await
}

/// Normalize a client's body to the resource it addressed, preserving identity on rename.
fn replace_pipeline(file: &mut FileConfig, id: &str, mut pipeline: PipelineDto) {
    pipeline.id = id.to_string();
    match file.pipelines.iter().position(|p| p.id == id) {
        Some(i) => file.pipelines[i] = pipeline,
        None => file.pipelines.push(pipeline),
    }
}

pub async fn delete_pipeline(
    Extension(actor): Extension<Actor>,
    AxPath((_, id)): AxPath<(String, String)>,
    at: Addressed,
) -> Envelope<Save> {
    blocking_save(move || {
        // The history names what was deleted, and a pipeline's id is not shown to its author.
        let name = open_for_read(&at.branch, &at.area)
            .ok()
            .and_then(|r| {
                r.raw()
                    .file
                    .pipelines
                    .iter()
                    .find(|p| p.id == id)
                    .map(|p| p.name.clone())
            })
            .unwrap_or_else(|| id.clone());
        let message = format!("delete pipeline {name}");
        at.open().map_or_else(Save::refused, |writing| {
            writing.save(Touches::OnlyPipelines, &message, &actor, |file| {
                file.pipelines.retain(|p| p.id != id);
            })
        })
    })
    .await
}

// -- POST /api/areas/:area/undo ---------------------------------------------

/// A section an object is undone in, as the editor names it.
#[derive(Clone, Copy, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Section {
    Sources,
    Destinations,
    Routes,
    LookupTables,
    GrokPatterns,
    Pipelines,
}

impl Section {
    fn touches(self) -> Touches {
        match self {
            Section::Sources => Touches::Sources,
            Section::Destinations => Touches::Destinations,
            Section::Routes => Touches::Routes,
            Section::LookupTables => Touches::LookupTables,
            Section::GrokPatterns => Touches::GrokPatterns,
            Section::Pipelines => Touches::OnlyPipelines,
        }
    }

    fn noun(self) -> &'static str {
        match self {
            Section::Sources => "source",
            Section::Destinations => "destination",
            Section::Routes => "route",
            Section::LookupTables => "lookup table",
            Section::GrokPatterns => "grok pattern",
            Section::Pipelines => "pipeline",
        }
    }
}

#[derive(serde::Deserialize)]
#[serde(untagged)]
pub enum UndoRequest {
    Object {
        section: Section,
        id: String,
    },
    /// A file no section owns, by its path within the bundle.
    File {
        path: String,
    },
}

/// Puts one object, or one file no section owns, back the way it was where the branch left `main`, as one save on
/// the branch.
pub async fn undo(Extension(actor): Extension<Actor>, at: Addressed, Json(req): Json<UndoRequest>) -> Envelope<Save> {
    blocking_save(move || undo_in(&leader_data_dir(), &at.branch, at.base, &at.area, req, &actor)).await
}

/// `now` with the object `id` as `was` holds it, after the object it followed there, or removed when `was` has
/// none. `None` when neither holds it.
struct PutBack<T> {
    objects: Vec<T>,
    /// Whether the undo leaves the section as it is: the object is the same, and where it was.
    same: bool,
    /// Its `name`, else its id.
    name: String,
    /// The copy the undo restores. `None` when it removes the object.
    restored: Option<T>,
}

fn put_back<T: serde::Serialize + Clone>(was: &[T], now: &[T], id: &str, id_of: fn(&T) -> &str) -> Option<PutBack<T>> {
    let as_json = |object: &T| serde_json::to_value(object).expect("a config_file DTO always serializes");
    let at_base = was.iter().position(|o| id_of(o) == id);
    let at_tip = now.iter().position(|o| id_of(o) == id);
    let shown = at_base.map(|i| &was[i]).or(at_tip.map(|i| &now[i]))?;
    let name = match as_json(shown).get("name").and_then(|n| n.as_str()) {
        Some(name) => name.to_string(),
        None => id.to_string(),
    };
    let mut objects = now.to_vec();
    if let Some(t) = at_tip {
        objects.remove(t);
    }
    if let Some(b) = at_base {
        // Order is meaning in some sections -- routes are tried in it -- so it goes back after the nearest
        // object that preceded it and is still there, or first when none is.
        let after = was[..b]
            .iter()
            .rev()
            .find_map(|o| objects.iter().position(|p| id_of(p) == id_of(o)));
        objects.insert(after.map_or(0, |i| i + 1), was[b].clone());
    }
    let same = objects.iter().map(as_json).eq(now.iter().map(as_json));
    Some(PutBack {
        objects,
        same,
        name,
        restored: at_base.map(|b| was[b].clone()),
    })
}

/// The file at `path`, relative to the repository, in `commit`; `None` where the commit has none.
fn blob_at(repo: &Path, commit: &str, path: &str) -> Result<Option<Vec<u8>>, git::GitError> {
    git::read_blobs(repo, commit, &[path.to_string()]).map(|mut blobs| blobs.remove(0))
}

/// `put_back` written into `now`: whether that left it as it was, and the object's name.
fn put_back_into<T: serde::Serialize + Clone>(
    was: &[T],
    now: &mut Vec<T>,
    id: &str,
    id_of: fn(&T) -> &str,
) -> Option<(bool, String)> {
    let p = put_back(was, now, id, id_of)?;
    *now = p.objects;
    Some((p.same, p.name))
}

/// The file a lookup table names under `lookups/`, relative to its bundle.
fn lookup_file_of(table: &LookupTableDto) -> Option<&str> {
    match &table.source {
        LookupSourceDto::Csv { path, .. } | LookupSourceDto::Mmdb { path } => {
            config_file::refuse_outside_lookups(path).ok().map(|_| path.as_str())
        }
        LookupSourceDto::Inline { .. } => None,
    }
}

fn undo_in(
    data_dir: &Path,
    branch: &str,
    base: Option<String>,
    scope: &str,
    req: UndoRequest,
    author: &Actor,
) -> Envelope<Save> {
    let mut writing = match Writing::open_in(data_dir, branch, base.as_deref(), scope) {
        Ok(writing) => writing,
        Err(e) => return Save::refused(e),
    };
    let repo = writing.repo().to_path_buf();
    match git::branch_exists(&repo, branch) {
        Ok(true) => {}
        Ok(false) => {
            return Envelope::refused(Refusal::conflict(format!(
                "{branch:?} has no saves, so there is nothing to undo"
            )));
        }
        Err(e) => return Envelope::refused(e),
    }
    let left_at = match git::branch_base(&repo, branch) {
        Ok(left_at) => left_at,
        Err(e) => return Envelope::refused(e),
    };
    let (section, id) = match req {
        UndoRequest::Object { section, id } => (section, id),
        UndoRequest::File { path } => return undo_file_in(data_dir, branch, scope, &writing, &left_at, &path, author),
    };
    let id = id.as_str();
    let was = match Reading::open_commit_in(data_dir, branch, scope, &left_at) {
        Ok(reading) => reading.raw().file,
        Err(e) => return Envelope::refused(e),
    };
    let mut file = writing.read().file;

    let missing = || {
        Envelope::refused(Refusal::not_found(format!(
            "there is no {} {id:?} on {branch:?} or where it left {}",
            section.noun(),
            git::MAIN_BRANCH
        )))
    };
    let mut files = Vec::new();
    let found = match section {
        Section::Sources => put_back_into(&was.sources, &mut file.sources, id, |o| &o.id),
        Section::Destinations => put_back_into(&was.destinations, &mut file.destinations, id, |o| &o.id),
        Section::Routes => put_back_into(&was.routes, &mut file.routes, id, |o| &o.id),
        Section::GrokPatterns => put_back_into(&was.grok_patterns, &mut file.grok_patterns, id, |o| &o.id),
        Section::Pipelines => put_back_into(&was.pipelines, &mut file.pipelines, id, |o| &o.id),
        Section::LookupTables => match put_back(&was.lookup_tables, &file.lookup_tables, id, |o| &o.id) {
            Some(p) => {
                // The table names its file, and the branch may have changed the bytes under the same name.
                if let Some(path) = p.restored.as_ref().and_then(lookup_file_of) {
                    let in_repo = format!("{}/{path}", writing.prefix());
                    let read = |commit: &str| blob_at(&repo, commit, &in_repo);
                    match (read(&left_at), read(&writing.parent)) {
                        (Ok(Some(then)), Ok(now)) if now.as_ref() != Some(&then) => files.push(git::FileEdit {
                            path: path.to_string(),
                            content: Some(then),
                        }),
                        (Ok(_), Ok(_)) => {}
                        (Err(e), _) | (_, Err(e)) => return Envelope::refused(e),
                    }
                }
                file.lookup_tables = p.objects;
                Some((p.same && files.is_empty(), p.name))
            }
            None => None,
        },
    };
    let Some((same, name)) = found else {
        return missing();
    };
    if same {
        return Envelope::refused(Refusal::conflict(format!(
            "{} {name:?} is as it was where {branch:?} left {}: there is nothing to undo",
            section.noun(),
            git::MAIN_BRANCH
        )));
    }

    // On the commit `file` was read from, so the save differs from it in this one section and in nothing a save in
    // between could have brought.
    if let Err(e) = writing.stage(files) {
        return Envelope::refused(e);
    }
    let message = format!("undo changes to {} {name}", section.noun());
    writing.save(section.touches(), &message, author, |into| *into = file)
}

/// Puts `within`, a path in the scope's bundle, back as it was at `left_at`, or removes it where `left_at` has none.
///
/// Only a file no section owns -- a sample, a test, a lookup file -- or one that does not read at either end: a
/// section's objects go back one at a time, each judged as a save of its section is. The bytes it writes are the
/// ones `main` held there, so it is not refused for a file that does not read: putting such a file back is the point.
fn undo_file_in(
    data_dir: &Path,
    branch: &str,
    scope: &str,
    writing: &Writing,
    left_at: &str,
    within: &str,
    author: &Actor,
) -> Envelope<Save> {
    let path = format!("{}/{within}", writing.prefix);
    // A `..` would reach past the bundle the checks below read.
    if within
        .split('/')
        .any(|part| part.is_empty() || part == "." || part == "..")
    {
        return Envelope::refused(Refusal::invalid(format!("{path:?} is not a path within the bundle")));
    }
    let unowned = [
        config_file::SAMPLES_DIR,
        config_file::TESTS_DIR,
        config_file::LOOKUPS_DIR,
    ]
    .iter()
    .any(|dir| within.starts_with(&format!("{dir}/")));
    if !unowned && !does_not_load(&mut writing.files(), within) {
        let did_not_load = match Reading::open_commit_in(data_dir, branch, scope, left_at) {
            Ok(reading) => does_not_load(&mut reading.files(), within),
            Err(e) => return Envelope::refused(e),
        };
        if !did_not_load {
            return Envelope::refused(Refusal::invalid(format!(
                "{path} reads as objects: undo the changes to each of them instead"
            )));
        }
    }
    let read = |commit: &str| blob_at(&writing.repo, commit, &path);
    let (then, now) = match (read(left_at), read(&writing.parent)) {
        (Ok(then), Ok(now)) => (then, now),
        (Err(e), _) | (_, Err(e)) => return Envelope::refused(e),
    };
    if then == now {
        return Envelope::refused(Refusal::conflict(format!(
            "{path} is as it was where {branch:?} left {}: there is nothing to undo",
            git::MAIN_BRANCH
        )));
    }
    if then.is_none()
        && let Err(e) = refuse_removing_manifest(writing, within, &path)
    {
        return Envelope::refused(e);
    }
    let edits = vec![git::FileEdit {
        path: within.to_string(),
        content: then,
    }];
    // Named as the save that made the change names it: `delete sample keep`.
    let stem = |dir: &str| within.strip_prefix(&format!("{dir}/"))?.strip_suffix(".json");
    let named = match (stem(config_file::SAMPLES_DIR), stem(config_file::TESTS_DIR)) {
        (Some(id), _) => format!("sample {id}"),
        (_, Some(id)) => format!("test {id}"),
        _ => format!("file {path}"),
    };
    match writing.commit(edits, &format!("undo changes to {named}"), author) {
        Ok(commit) => Save::done(commit),
        Err(e) => Save::refused(e),
    }
}

// -- samples ------------------------------------------------------------
//
// Records under a name (`config_file::sample`). A sample is a file in the bundle like any other, so it is written by
// one commit on a branch and a rollback restores the samples the previous deploy ran with. It is not part of the
// configuration a deployment loads, so a write skips `Writing::save`'s whole-bundle validation: no sample can make
// a bundle invalid, and refusing to save records because an unrelated pipeline is mid-edit would be answering for
// the wrong thing.

#[derive(Debug, Default, serde::Serialize)]
pub struct Samples {
    samples: Vec<config_file::SampleSummary>,
    #[serde(skip_serializing_if = "Option::is_none")]
    commit: Option<String>,
}

#[derive(Debug, Default, serde::Serialize)]
pub struct Sample {
    #[serde(skip_serializing_if = "Option::is_none")]
    sample: Option<config_file::SampleDto>,
    #[serde(skip_serializing_if = "Option::is_none")]
    commit: Option<String>,
}

/// Fill in when each of `samples`, as `reading` listed them, was last written.
fn fill_written(reading: &Reading, samples: &mut [config_file::SampleSummary]) -> Result<(), Refusal> {
    let in_repo = |s: &config_file::SampleSummary| format!("{}/{}", reading.prefix, s.path);
    let paths: Vec<String> = samples.iter().map(in_repo).collect();
    let written = git::last_changed(&reading.repo, &reading.commit, &paths)?;
    for sample in samples {
        sample.written = written.get(&in_repo(sample)).copied();
    }
    Ok(())
}

pub async fn list_samples(AxPath(area): AxPath<String>, Query(q): Query<BranchQuery>) -> Envelope<Samples> {
    let branch = q.name().to_string();
    let read = blocking(move || {
        let reading = open_for_read(&branch, &area)?;
        let (mut samples, unreadable) = config_file::list_samples(&mut reading.files());
        fill_written(&reading, &mut samples)?;
        Ok::<_, Refusal>((samples, unreadable, reading.commit))
    })
    .await;
    match read {
        Ok((samples, unreadable, commit)) => {
            let status = read_status(&unreadable);
            let errors = (unreadable.into_iter())
                .map(|(path, message)| ApiError::new(format!("{path}: {message}")))
                .collect();
            let body = Samples {
                samples,
                commit: Some(commit),
            };
            Envelope::with(body, status, errors)
        }
        Err(e) => Envelope::refused(e),
    }
}

/// A sample at the branch's tip, or at the commit `at` names.
pub async fn get_sample(
    AxPath((area, id)): AxPath<(String, String)>,
    Query(q): Query<ConfigQuery>,
) -> Envelope<Sample> {
    let read = blocking(move || {
        let reading = q.open_in(&leader_data_dir(), &area)?;
        let sample =
            config_file::read_sample(&mut reading.files(), &id).map_err(|e| Refusal::internal(e.to_string()))?;
        Ok::<_, Refusal>((sample, reading.commit))
    })
    .await;
    match read {
        Ok((Some(sample), commit)) => Envelope::ok(Sample {
            sample: Some(sample),
            commit: Some(commit),
        }),
        Ok((None, commit)) => {
            let body = Sample {
                sample: None,
                commit: Some(commit),
            };
            let errors = vec![ApiError::new("there is no sample by that name on this branch")];
            Envelope::with(body, StatusCode::NOT_FOUND, errors)
        }
        Err(e) => Envelope::refused(e),
    }
}

/// Create-or-replace by the `:id` in the path, which is the sample's id whatever the body says.
pub async fn put_sample(
    Extension(actor): Extension<Actor>,
    AxPath((_, id)): AxPath<(String, String)>,
    at: Addressed,
    Json(mut sample): Json<config_file::SampleDto>,
) -> Envelope<Save> {
    sample.id = id;
    if let Err(message) = config_file::refuse_sample_size(sample.records.len()) {
        return Envelope::refused(Refusal::invalid(message));
    }
    blocking_save(move || {
        let message = format!("save sample {}", sample.name);
        at.open().map_or_else(Save::refused, |writing| {
            write_sample_file(
                writing,
                &sample.id,
                Some(config_file::sample_bytes(&sample)),
                &message,
                &actor,
            )
        })
    })
    .await
}

/// Removing it from the branch. The blob stays reachable in history, which is what makes a rollback restore it.
pub async fn delete_sample(
    Extension(actor): Extension<Actor>,
    AxPath((_, id)): AxPath<(String, String)>,
    at: Addressed,
) -> Envelope<Save> {
    blocking_save(move || {
        let message = format!("delete sample {id}");
        at.open().map_or_else(Save::refused, |writing| {
            write_sample_file(writing, &id, None, &message, &actor)
        })
    })
    .await
}

/// `Err` naming the existing sample when `path` already holds one whose id differs from `id`. `slugify` can put two
/// different ids at one path (`config_file::bundle::slugify`), and a commit overwrites unconditionally, so this is
/// the one place either save learns about the other.
///
/// `content` is what the caller is about to write: `None` is a delete, which never collides -- there is nothing left
/// to protect once the file is gone. `files` must read the same base commit the write itself lands on, so the check
/// and the write agree on what is there.
pub fn refuse_sample_collision(
    files: &mut dyn config_file::FileSet,
    path: &str,
    id: &str,
    content: &Option<Vec<u8>>,
) -> Result<(), String> {
    refuse_slug_collision(files, path, id, content, "sample", |bytes| {
        serde_json::from_slice::<config_file::SampleDto>(bytes)
            .ok()
            .map(|s| s.id)
    })
}

/// The same for a test, which shares the slug rule and everything under it.
pub fn refuse_test_collision(
    files: &mut dyn config_file::FileSet,
    path: &str,
    id: &str,
    content: &Option<Vec<u8>>,
) -> Result<(), String> {
    refuse_slug_collision(files, path, id, content, "test", |bytes| {
        serde_json::from_slice::<config_file::TestDto>(bytes).ok().map(|t| t.id)
    })
}

/// What refuses a write whose slug is already held by a different id, for one kind of bundle file.
type RefuseCollision = fn(&mut dyn config_file::FileSet, &str, &str, &Option<Vec<u8>>) -> Result<(), String>;

/// `id_of` reads back what a file of this kind calls itself. `None` is a file that does not parse as one, which
/// occupies the slug just the same.
fn refuse_slug_collision(
    files: &mut dyn config_file::FileSet,
    path: &str,
    id: &str,
    content: &Option<Vec<u8>>,
    kind: &str,
    id_of: fn(&[u8]) -> Option<String>,
) -> Result<(), String> {
    if content.is_none() {
        return Ok(());
    }
    let existing = match files.read(path) {
        Ok(Some(bytes)) => bytes,
        Ok(None) => return Ok(()),
        // The write itself will surface a read failure; this check only refuses a proven collision.
        Err(_) => return Ok(()),
    };
    match id_of(&existing) {
        // An unreadable file is not a license to destroy it: the slug is occupied either way.
        None => Err(format!(
            "{path} already holds a file that is not a readable {kind} -- rename this one"
        )),
        Some(other) if other != id => Err(format!("`{other}` and `{id}` both save as {path} -- rename one")),
        Some(_) => Ok(()),
    }
}

/// One commit setting `samples/<slug>.json` to `content`, or removing it when that is `None`.
fn write_sample_file(
    writing: Writing,
    id: &str,
    content: Option<Vec<u8>>,
    message: &str,
    author: &Actor,
) -> Envelope<Save> {
    match config_file::sample_path(id) {
        Ok(path) => write_one_file(writing, path, id, content, message, author, refuse_sample_collision),
        Err(e) => Envelope::refused(Refusal::invalid(e.to_string())),
    }
}

/// One commit setting `tests/<slug>.json` to `content`, or removing it when that is `None`.
pub fn write_test_file(
    writing: Writing,
    id: &str,
    content: Option<Vec<u8>>,
    message: &str,
    author: &Actor,
) -> Envelope<Save> {
    match config_file::test_path(id) {
        Ok(path) => write_one_file(writing, path, id, content, message, author, refuse_test_collision),
        Err(e) => Envelope::refused(Refusal::invalid(e.to_string())),
    }
}

/// One commit over one bundle file the deployment does not read, so no whole-bundle validation runs -- see the
/// section comment above.
fn write_one_file(
    writing: Writing,
    path: String,
    id: &str,
    content: Option<Vec<u8>>,
    message: &str,
    author: &Actor,
    refuse: RefuseCollision,
) -> Envelope<Save> {
    if let Err(e) = refuse(&mut writing.files(), &path, id, &content) {
        return Envelope::refused(Refusal::conflict(e));
    }
    match writing.commit(vec![git::FileEdit { path, content }], message, author) {
        Ok(commit) => Save::done(commit),
        Err(e) => Save::refused(e),
    }
}

// -- /api/areas/:area/unreadable, /api/areas/:area/files/*path -------------
//
// A file the lenient read could not parse has no page to edit it on, so it is edited as text. Only a file the read
// names is reachable here, which keeps these routes from opening any other file in the repository.

/// One file that does not load, by its path in the repository.
#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UnreadableFile {
    path: String,
    why: String,
    /// False for a group's `system.json`: it is what makes the subtree a group, so removing it breaks the group.
    deletable: bool,
}

#[derive(Default, serde::Serialize)]
pub struct Unreadable {
    files: Vec<UnreadableFile>,
    /// The commit this was read at, which a write of a listed file sends back. Absent when the read failed.
    #[serde(skip_serializing_if = "Option::is_none")]
    commit: Option<String>,
}

/// Every file of a bundle that does not load, by its path within the bundle, with why: what the lenient read
/// skipped, and the samples the library skipped. A directory that could not be listed is not among them, since it
/// is not a file there is anything to open.
fn unreadable_in_bundle(files: &mut dyn config_file::FileSet) -> Vec<(String, String)> {
    let skipped = config_file::read_raw_from(files).unreadable;
    let samples = config_file::list_samples(files).1;
    skipped
        .into_iter()
        .filter(|(path, _)| !path.ends_with('/'))
        .chain(samples.into_iter().filter(|(path, _)| path != config_file::SAMPLES_DIR))
        .collect()
}

/// The files of `prefix`'s bundle in `unreadable`. A file with two problems is listed once, with both.
fn unreadable_files_of(prefix: &str, unreadable: &[(String, String)]) -> Vec<UnreadableFile> {
    let mut files: Vec<UnreadableFile> = Vec::new();
    for (path, why) in unreadable {
        let full = format!("{prefix}/{path}");
        match files.iter_mut().find(|f| f.path == full) {
            Some(listed) => listed.why = format!("{}; {why}", listed.why),
            None => files.push(UnreadableFile {
                deletable: prefix == config_file::SHARED_DIR || path != "system.json",
                path: full,
                why: why.clone(),
            }),
        }
    }
    files
}

/// What `scope` runs that does not load: its own files, and for a group `shared/`'s as well. With the commit they
/// were read at.
#[cfg(test)]
fn unreadable_in(data_dir: &Path, branch: &str, scope: &str) -> Result<(Vec<UnreadableFile>, String), Refusal> {
    unreadable_of(Reading::open_in(data_dir, branch, scope)?)
}

/// `unreadable_in`, of a scope already opened: at a branch's tip, or at a commit a review compares.
fn unreadable_of(reading: Reading) -> Result<(Vec<UnreadableFile>, String), Refusal> {
    let mut files = unreadable_files_of(&reading.prefix, &unreadable_in_bundle(&mut reading.files()));
    if reading.prefix != config_file::SHARED_DIR {
        let shared = reading.sibling(config_file::SHARED_DIR)?;
        files.extend(unreadable_files_of(
            &shared.prefix,
            &unreadable_in_bundle(&mut shared.files()),
        ));
    }
    Ok((files, reading.commit))
}

/// What does not load at the branch's tip, or at the commit `at` names.
pub async fn get_unreadable(AxPath(area): AxPath<String>, Query(q): Query<ConfigQuery>) -> Envelope<Unreadable> {
    let read = move || {
        let (files, commit) = unreadable_of(q.open_in(&leader_data_dir(), &area)?)?;
        Ok::<_, Refusal>(Unreadable {
            files,
            commit: Some(commit),
        })
    };
    blocking(read).await.into()
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileBody {
    text: String,
}

#[derive(Default, serde::Serialize)]
pub struct File {
    path: String,
    text: String,
    /// The commit `text` was read at, which the write sends back. Absent when the read failed.
    #[serde(skip_serializing_if = "Option::is_none")]
    commit: Option<String>,
}

/// Whether `within` is a file of `files` that does not load.
fn does_not_load(files: &mut dyn config_file::FileSet, within: &str) -> bool {
    unreadable_in_bundle(files).iter().any(|(p, _)| p == within)
}

/// `Err` when removing `within` would remove a group's manifest, which is what makes its directory a group.
fn refuse_removing_manifest(writing: &Writing, within: &str, path: &str) -> Result<(), Refusal> {
    if writing.prefix != config_file::SHARED_DIR && within == "system.json" {
        return Err(Refusal::invalid(format!(
            "{path} is what makes its directory a group: deleting it breaks the group -- edit it instead"
        )));
    }
    Ok(())
}

/// Refuses `within` unless it is a file of `files` that does not load.
fn refuse_unlisted(
    files: &mut dyn config_file::FileSet,
    within: &str,
    path: &str,
    branch: &str,
) -> Result<(), Refusal> {
    if does_not_load(files, within) {
        return Ok(());
    }
    Err(Refusal::not_found(format!(
        "{path} is not a file that fails to load on {branch} -- only those are edited as text"
    )))
}

/// The text of `within`, a path in `area`'s bundle, and the commit it was read at.
fn read_listed_file(data_dir: &Path, branch: &str, area: &str, within: &str) -> Result<(String, String), Refusal> {
    let reading = Reading::open_in(data_dir, branch, area)?;
    let path = &format!("{}/{within}", reading.prefix);
    refuse_unlisted(&mut reading.files(), within, path, branch)?;
    match config_file::FileSet::read(&mut reading.files(), within) {
        Ok(Some(bytes)) => Ok((String::from_utf8_lossy(&bytes).into_owned(), reading.commit)),
        Ok(None) => Err(Refusal::not_found(format!("{path} is not on {branch}"))),
        Err(e) => Err(Refusal::internal(format!("{path}: {e}"))),
    }
}

pub async fn get_file(
    AxPath((area, within)): AxPath<(String, String)>,
    Query(q): Query<BranchQuery>,
) -> Envelope<File> {
    let path = within.clone();
    let read = blocking(move || read_listed_file(&leader_data_dir(), q.name(), &area, &within)).await;
    match read {
        Ok((text, commit)) => Envelope::ok(File {
            path,
            text,
            commit: Some(commit),
        }),
        Err(e) => {
            let Refusal { status, errors } = e;
            let body = File {
                path,
                ..File::default()
            };
            Envelope::with(body, status, errors)
        }
    }
}

pub async fn put_file(
    Extension(actor): Extension<Actor>,
    AxPath((_, within)): AxPath<(String, String)>,
    at: Addressed,
    Json(body): Json<FileBody>,
) -> Envelope<Save> {
    blocking_save(move || {
        let content = Some(body.text.into_bytes());
        at.open().map_or_else(Save::refused, |writing| {
            write_listed_file(writing, &within, content, &actor)
        })
    })
    .await
}

pub async fn delete_file(
    Extension(actor): Extension<Actor>,
    AxPath((_, within)): AxPath<(String, String)>,
    at: Addressed,
) -> Envelope<Save> {
    blocking_save(move || {
        at.open().map_or_else(Save::refused, |writing| {
            write_listed_file(writing, &within, None, &actor)
        })
    })
    .await
}

/// One commit setting `within`, a path in `writing`'s bundle, to `content`, or removing it when that is `None`, if
/// the branch's read lists it as a file that does not load.
fn write_listed_file(writing: Writing, within: &str, content: Option<Vec<u8>>, author: &Actor) -> Envelope<Save> {
    let path = &format!("{}/{within}", writing.prefix);
    if let Err(e) = refuse_unlisted(&mut writing.files(), within, path, &writing.branch) {
        return Envelope::refused(e);
    }
    if content.is_none()
        && let Err(e) = refuse_removing_manifest(&writing, within, path)
    {
        return Envelope::refused(e);
    }
    let message = match content {
        Some(_) => format!("edit {path}"),
        None => format!("delete {path}"),
    };
    let edits = vec![git::FileEdit {
        path: within.to_string(),
        content,
    }];
    if let Err(response) = refuse_worse_read(&writing, &edits) {
        return response;
    }
    match writing.commit(edits, &message, author) {
        Ok(commit) => Save::done(commit),
        Err(e) => Save::refused(e),
    }
}

/// `refuse_worse` over what a read reports, before and after `edits`, for every bundle they reach: `shared/` and
/// every group on a `shared/` write, the one group otherwise.
///
/// Not over a strict load, as `refuse_broken_merge` judges a section's save: a load stops at the first file it
/// cannot parse, so mending that file would bring the next file's problem into view as if this write made it. A read
/// reports every file at once, so only what the write itself introduces is new.
fn refuse_worse_read(writing: &Writing, edits: &[git::FileEdit]) -> Result<(), Envelope<Save>> {
    let (repo, parent) = (&writing.repo, writing.parent.as_str());
    let shared = config_file::SHARED_DIR;
    let at = |prefix: &str| config_file::BundleAt::Commit {
        repo: repo.clone(),
        commit: writing.seen.clone(),
        prefix: prefix.to_string(),
    };
    // A bundle as this write leaves it, or as it was when this write does not reach it.
    let read = |prefix: &str, after: bool| {
        let under = TreeFiles::new(repo, parent, prefix.to_string());
        if after && prefix == writing.prefix {
            config_file::read_raw_from(&mut Pending { under, edits })
        } else {
            config_file::read_raw_from(&mut { under })
        }
    };
    let named = |prefix: &str, raw: &RawBundle| -> Vec<config_file::ValidationError> {
        raw.unreadable
            .iter()
            .map(|(path, message)| format!("{prefix}/{path}: {message}").into())
            .collect()
    };
    let shared_alone = |after: bool| {
        let raw = read(shared, after);
        let mut errors = named(shared, &raw);
        errors.extend(errors_in(raw.file, &at(shared)));
        errors
    };
    let group = |prefix: &str, after: bool| {
        let raw = read(prefix, after);
        let mut errors = named(prefix, &raw);
        let group_id = group_of(prefix).unwrap_or_default();
        errors.extend(errors_of_merge(
            &read(shared, after),
            at(shared),
            &raw,
            at(prefix),
            group_id,
        ));
        errors
    };

    if writing.prefix != shared {
        return refuse_worse(group(&writing.prefix, true), group(&writing.prefix, false));
    }
    refuse_worse(shared_alone(true), shared_alone(false))?;
    // The group list is unreadable, so there is nothing more to check against, as for `refuse_broken_merge`.
    let groups = smeltr::leader::tree::groups_at(repo, parent).unwrap_or_default();
    for id in groups {
        let Ok(prefix) = config_file::group_subtree(&id) else {
            continue;
        };
        // Named, because on a `shared/` write the group that breaks is not the one being edited.
        refuse_worse(group(&prefix, true), group(&prefix, false)).map_err(|mut response| {
            for e in &mut response.errors {
                e.message = format!("worker group {id:?}: {}", e.message);
            }
            response
        })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    /// The shape the GUI places a save's error by (`api.ts`, `ApiError`).
    #[test]
    fn a_validation_error_reaches_the_editor_with_its_item_and_field() {
        let error = config_file::ValidationError {
            item: Some(config_file::ItemRef {
                section: "destinations",
                id: "archive".into(),
            }),
            field: Some("bucket"),
            ..config_file::ValidationError::from("destination \"archive\" names no bucket".to_string())
        };
        assert_eq!(
            serde_json::to_value(ApiError::from_validation(&error)).unwrap(),
            serde_json::json!({
                "pipeline": null, "function": null, "step": null, "assignmentIndex": null,
                "item": { "section": "destinations", "id": "archive" },
                "field": "bucket", "rule": null,
                "message": "destination \"archive\" names no bucket",
            })
        );
    }

    fn temp_dir(label: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "smeltr-config-api-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// A branch is allowed to be broken, and has to be: bringing `main` in can leave a reference dangling, and the
    /// edit that repairs it names what only `main` held. A save is refused for what it introduces, never for what
    /// it inherited.
    #[test]
    fn a_save_is_refused_only_for_the_errors_it_introduces() {
        let inherited: Vec<config_file::ValidationError> =
            vec![r#"route "r1" references unknown pipeline "orders""#.to_string().into()];

        refuse_worse(inherited.clone(), inherited.clone()).expect("an unrelated fix on a broken branch lands");
        refuse_worse(vec![], inherited.clone()).expect("and so does the one that repairs it");

        let mut worse = inherited.clone();
        worse.push(r#"route "r2" references unknown pipeline "gone""#.to_string().into());
        let refused = refuse_worse(worse, inherited).expect_err("a new mistake never lands");
        let said: Vec<&str> = refused.errors.iter().map(|e| e.message.as_str()).collect();
        assert_eq!(
            said,
            vec![r#"route "r2" references unknown pipeline "gone""#],
            "only the new one"
        );
    }

    /// A pipeline's file is named by slugifying its id, so two ids that slugify the same name one file. Writing
    /// both kept whichever went last and lost the other -- with `ok: true` and a real commit, so nothing anywhere
    /// said a pipeline had gone.
    #[test]
    fn two_pipeline_names_that_slugify_the_same_are_refused() {
        let pipeline = |id: &str| smeltr::config_file::PipelineDto {
            id: id.to_string(),
            name: id.to_string(),
            steps: vec![],
        };
        let file = FileConfig {
            version: 1,
            on_exception: None,
            name: None,
            sources: vec![],
            destinations: vec![],
            pipelines: vec![pipeline("Orders"), pipeline("orders")],
            routes: vec![],
            lookup_tables: vec![],
            grok_patterns: vec![],
        };
        let old = RawBundle {
            file: file.clone(),
            unreadable: vec![],
        };

        let err = edits_for(&file, &old, Touches::OnlyPipelines).expect_err("one file, two pipelines");
        assert!(
            err.message().contains("Orders") && err.message().contains("orders"),
            "it names both: {err}"
        );
        assert!(
            err.message().contains("pipelines/orders.json"),
            "and the file they collide on: {err}"
        );
    }

    /// A repository with `main`, and `work` started off it.
    fn a_repo(label: &str) -> PathBuf {
        let data = temp_dir(label);
        let repo = git::repo_path(&data);
        git::init_bare_repo(&repo).expect("a repository");
        git::ensure_main_commit(&repo).expect("a commit to branch from");
        data
    }

    /// A branch holding one of each kind of file that does not load -- a manifest `shared/` cannot parse, a group
    /// manifest of a version this build does not read, a pipeline that is not JSON -- beside a file that parses.
    fn a_repo_with_files_that_do_not_load(label: &str) -> PathBuf {
        let data = a_repo(label);
        let put = |scope: &str, path: &str, body: &[u8]| {
            Writing::open_in(&data, "work", None, scope)
                .unwrap()
                .commit(
                    vec![git::FileEdit {
                        path: path.to_string(),
                        content: Some(body.to_vec()),
                    }],
                    "seed",
                    &Actor::system(),
                )
                .expect("seeded");
        };
        put("shared", "system.json", b"{ not json");
        put("west", "system.json", br#"{"version":99}"#);
        put("west", "pipelines/x.json", b"{ broken");
        put("west", "sources.json", b"[]");
        data
    }

    /// A review lists what did not load at each end of a change, so a file a later commit fixed is still listed at
    /// the commit before it.
    #[test]
    fn a_file_a_later_commit_fixed_is_listed_at_the_commit_before() {
        let data = a_repo_with_files_that_do_not_load("unreadable-at");
        let broken = Reading::open_in(&data, "work", "west").unwrap().commit;
        let fix = git::FileEdit {
            path: "pipelines/x.json".to_string(),
            content: Some(br#"{"id": "x", "name": "x", "steps": []}"#.to_vec()),
        };
        let writing = Writing::open_in(&data, "work", None, "west").unwrap();
        writing.commit(vec![fix], "fix", &Actor::system()).unwrap();
        let at = |commit: Option<&str>| -> Vec<String> {
            let q: ConfigQuery = serde_json::from_value(serde_json::json!({ "branch": "work", "at": commit })).unwrap();
            unreadable_of(q.open_in(&data, "west").unwrap())
                .unwrap()
                .0
                .into_iter()
                .map(|f| f.path)
                .collect()
        };
        assert!(!at(None).contains(&"groups/west/pipelines/x.json".to_string()));
        assert!(at(Some(&broken)).contains(&"groups/west/pipelines/x.json".to_string()));
        std::fs::remove_dir_all(&data).ok();
    }

    fn listed(data: &Path, scope: &str) -> Vec<(String, bool)> {
        unreadable_in(data, "work", scope)
            .unwrap()
            .0
            .into_iter()
            .map(|f| (f.path, f.deletable))
            .collect()
    }

    #[test]
    fn a_group_lists_its_own_files_that_do_not_load_and_shared_s() {
        let data = a_repo_with_files_that_do_not_load("unreadable-list");

        let (files, commit) = unreadable_in(&data, "work", "west").unwrap();
        assert_eq!(commit, Reading::open_in(&data, "work", "west").unwrap().commit);
        let by_path: Vec<(&str, bool)> = files.iter().map(|f| (f.path.as_str(), f.deletable)).collect();
        assert_eq!(
            by_path,
            vec![
                ("groups/west/system.json", false),
                ("groups/west/pipelines/x.json", true),
                ("shared/system.json", true),
            ]
        );
        assert_eq!(files[0].why, config_file::LoadError::UnsupportedVersion(99).to_string());
        assert!(files[1].why.starts_with("invalid JSON:"), "{}", files[1].why);

        assert_eq!(listed(&data, "shared"), vec![("shared/system.json".to_string(), true)]);
        std::fs::remove_dir_all(&data).ok();
    }

    /// Each problem names the file it is in, so a group's `system.json` and `shared/`'s do not read as one.
    #[test]
    fn a_group_s_read_names_shared_s_files_with_their_subtree() {
        let data = a_repo_with_files_that_do_not_load("unreadable-named");
        let reading = Reading::open_in(&data, "work", "west").unwrap();
        let raw = reading.raw();
        let said: Vec<String> = unreadable_errors(&raw)
            .into_iter()
            .map(|e| e.message)
            .chain(errors_of_read(&reading, &raw).into_iter().map(|e| e.message))
            .collect();
        assert!(
            said.iter().any(|m| m.starts_with("shared/system.json: invalid JSON")),
            "{said:?}"
        );
        assert!(
            said.iter()
                .any(|m| m.starts_with("system.json: unsupported config version 99")),
            "{said:?}"
        );
        assert!(!said.iter().any(|m| m.starts_with("unsupported")), "{said:?}");
        std::fs::remove_dir_all(&data).ok();
    }

    #[test]
    fn a_listed_file_is_read_and_written_as_text_and_leaves_the_list_once_it_loads() {
        let data = a_repo_with_files_that_do_not_load("unreadable-edit");
        let (path, within) = ("groups/west/pipelines/x.json", "pipelines/x.json");
        let (text, commit) = read_listed_file(&data, "work", "west", within).unwrap();
        assert_eq!(text, "{ broken");

        let fixed = serde_json::to_string(&PipelineDto {
            id: "x".to_string(),
            name: "x".to_string(),
            steps: vec![],
        })
        .unwrap();
        let stale = Some("0".repeat(40));
        let response = write_listed(&data, stale, "west", within, Some(fixed.clone().into_bytes()));
        assert!(
            !response.succeeded(),
            "a write against a commit the branch is not at is refused"
        );
        let response = write_listed(&data, Some(commit), "west", within, Some(fixed.into_bytes()));
        assert!(response.succeeded(), "saved: {response:?}");

        let still: Vec<String> = listed(&data, "west").into_iter().map(|(p, _)| p).collect();
        assert!(!still.iter().any(|p| p == path), "{still:?}");
        assert!(
            read_listed_file(&data, "work", "west", within).is_err(),
            "it loads now, so it is not listed"
        );
        std::fs::remove_dir_all(&data).ok();
    }

    /// A sample the library skips is listed with the rest, so it can be opened as text and deleted.
    #[test]
    fn a_sample_that_does_not_load_is_listed_and_can_be_deleted() {
        let data = a_repo("unreadable-sample");
        let path = "groups/west/samples/old.json";
        let writing = Writing::open_in(&data, "work", None, "west").unwrap();
        let old = git::FileEdit {
            path: "samples/old.json".to_string(),
            content: Some(br#"{"id": "old", "name": "old", "records": ["x"]}"#.to_vec()),
        };
        writing.commit(vec![old], "seed", &Actor::system()).unwrap();

        assert!(listed(&data, "west").contains(&(path.to_string(), true)));
        let within = "samples/old.json";
        assert!(
            read_listed_file(&data, "work", "west", within)
                .unwrap()
                .0
                .contains(r#"["x"]"#)
        );
        let response = write_listed(&data, None, "west", within, None);
        assert!(response.succeeded(), "deleted: {response:?}");
        assert!(!listed(&data, "west").iter().any(|(p, _)| p == path));
        std::fs::remove_dir_all(&data).ok();
    }

    /// Mending `shared/` while the group's own manifest still does not load is not refused for what that brings
    /// into view: only what a write introduces counts against it.
    #[test]
    fn a_listed_file_other_than_a_group_s_manifest_can_be_deleted() {
        let data = a_repo_with_files_that_do_not_load("unreadable-delete");
        let response = write_listed(&data, None, "shared", "system.json", None);
        assert!(response.succeeded(), "deleted: {response:?}");
        assert_eq!(
            listed(&data, "shared"),
            Vec::<(String, bool)>::new(),
            "shared/ has no manifest left to fail"
        );

        let response = write_listed(&data, None, "west", "system.json", None);
        assert!(!response.succeeded(), "a group's manifest is what makes it a group");
        assert!(
            listed(&data, "west")
                .iter()
                .any(|(p, _)| p == "groups/west/system.json")
        );
        std::fs::remove_dir_all(&data).ok();
    }

    /// Only a file the read names is reachable, so these routes open nothing else in the repository.
    #[test]
    fn a_file_that_loads_is_neither_read_nor_written_nor_deleted_as_text() {
        let data = a_repo_with_files_that_do_not_load("unreadable-unlisted");
        for (area, within) in [
            ("west", "sources.json"),
            ("west", "../west/pipelines/x.json"),
            ("west", ""),
            ("..", "README.md"),
        ] {
            assert!(read_listed_file(&data, "work", area, within).is_err(), "{within} read");
            for content in [Some(b"[]".to_vec()), None] {
                let response = write_listed(&data, None, area, within, content);
                assert!(!response.succeeded(), "{within} written: {response:?}");
            }
        }
        let reading = Reading::open_in(&data, "work", "west").unwrap();
        assert_eq!(
            config_file::FileSet::read(&mut reading.files(), "sources.json").unwrap(),
            Some(b"[]".to_vec()),
            "untouched"
        );
        std::fs::remove_dir_all(&data).ok();
    }

    #[test]
    fn pipeline_saves_keep_unique_ids_when_names_repeat_or_change() {
        let data = a_repo("pipeline-stable-ids");
        let pipeline = |name: &str, steps| PipelineDto {
            // A stale or conflicting body ID cannot replace the resource named by the path.
            id: "New Pipeline".into(),
            name: name.into(),
            steps,
        };
        let save = |id: &str, pipeline| {
            let response = Writing::open_in(&data, "work", None, "west").unwrap().save(
                Touches::OnlyPipelines,
                "save pipeline",
                &Actor::system(),
                |file| replace_pipeline(file, id, pipeline),
            );
            assert!(response.succeeded(), "{response:?}");
        };
        let steps = vec![config_file::StepDto::Comment {
            id: "keep-me".into(),
            text: "Original pipeline".into(),
        }];
        save("pipeline-a", pipeline("New Pipeline", steps.clone()));
        save("pipeline-b", pipeline("New Pipeline", vec![]));
        let raw = Reading::open_in(&data, "work", "west").unwrap().raw();
        assert_eq!(raw.file.pipelines.len(), 2);
        let first = raw.file.pipelines.iter().find(|p| p.id == "pipeline-a").unwrap();
        assert_eq!(first.name, "New Pipeline");
        assert_eq!(
            serde_json::to_value(&first.steps).unwrap(),
            serde_json::to_value(&steps).unwrap()
        );
        assert!(
            raw.file
                .pipelines
                .iter()
                .find(|p| p.id == "pipeline-b")
                .unwrap()
                .steps
                .is_empty()
        );

        // A route continues addressing the first pipeline after its display name changes.
        let route = config_file::RouteDto {
            id: "route-a".into(),
            source: "source-a".into(),
            condition: None,
            pipeline: "pipeline-a".into(),
            deliver: vec![],
            on_exception: None,
        };
        let response = Writing::open_in(&data, "work", None, "west").unwrap().save(
            Touches::Routes,
            "add reference",
            &Actor::system(),
            |file| {
                file.sources.push(SourceDto {
                    id: "source-a".into(),
                    kind: config_file::SourceKindDto::Metrics { bucket_seconds: 10 },
                });
                file.routes.push(route);
            },
        );
        assert!(response.succeeded(), "{response:?}");
        save("pipeline-a", pipeline("Renamed", steps));
        let raw = Reading::open_in(&data, "work", "west").unwrap().raw();
        assert_eq!(raw.file.pipelines.len(), 2);
        assert_eq!(raw.file.routes[0].pipeline, "pipeline-a");
        assert_eq!(
            raw.file.pipelines.iter().find(|p| p.id == "pipeline-a").unwrap().name,
            "Renamed"
        );
        assert_eq!(
            raw.file.pipelines.iter().find(|p| p.id == "pipeline-b").unwrap().name,
            "New Pipeline"
        );
        assert!(
            !raw.file
                .pipelines
                .iter()
                .any(|p| p.id == "New Pipeline" || p.id == "Renamed")
        );
        std::fs::remove_dir_all(data).ok();
    }

    /// A save of one section must keep every manifest field it did not touch.
    #[test]
    fn a_sources_save_keeps_the_rest_of_the_manifest() {
        let data = a_repo("partial-save");
        let exceptions = r#"{"kinds":{"Unmatched":{"deliver":["leftovers"]}}}"#;
        let manifest = format!(r#"{{"version":1,"exceptions":{exceptions}}}"#).into_bytes();
        Writing::open_in(&data, "work", None, "west")
            .unwrap()
            .commit(
                vec![git::FileEdit {
                    path: "system.json".to_string(),
                    content: Some(manifest),
                }],
                "the group",
                &Actor::system(),
            )
            .expect("seeded");

        let response = Writing::open_in(&data, "work", None, "west").unwrap().save(
            Touches::Sources,
            "a sources save",
            &Actor::system(),
            |file| {
                file.sources = vec![SourceDto {
                    id: "source-1".to_string(),
                    kind: config_file::SourceKindDto::Metrics { bucket_seconds: 10 },
                }]
            },
        );
        assert!(response.succeeded(), "saved: {response:?}");

        let saved = Reading::open_in(&data, "work", "west").unwrap().raw().file;
        assert_eq!(saved.sources.len(), 1, "the save landed");
        assert_eq!(saved.on_exception, Some(serde_json::from_str(exceptions).unwrap()));
        std::fs::remove_dir_all(&data).ok();
    }

    /// A save of another section leaves an unreadable manifest as it was. Rewriting it would quietly drop the key the
    /// load refuses.
    #[test]
    fn a_sources_save_leaves_an_unreadable_manifest_alone() {
        let data = a_repo("unreadable-manifest");
        let manifest = br#"{"version":1,"name":"West","unmatchedDlq":"leftovers"}"#.to_vec();
        Writing::open_in(&data, "work", None, "west")
            .unwrap()
            .commit(
                vec![git::FileEdit {
                    path: "system.json".to_string(),
                    content: Some(manifest.clone()),
                }],
                "the group",
                &Actor::system(),
            )
            .expect("seeded");

        let response = Writing::open_in(&data, "work", None, "west").unwrap().save(
            Touches::Sources,
            "a sources save",
            &Actor::system(),
            |file| {
                file.sources = vec![SourceDto {
                    id: "source-1".to_string(),
                    kind: config_file::SourceKindDto::Metrics { bucket_seconds: 10 },
                }]
            },
        );
        assert!(response.succeeded(), "saved: {response:?}");

        let reading = Reading::open_in(&data, "work", "west").unwrap();
        assert_eq!(reading.raw().file.sources.len(), 1, "the save landed");
        assert_eq!(
            config_file::FileSet::read(&mut reading.files(), "system.json").unwrap(),
            Some(manifest),
            "the manifest is as it was"
        );
        std::fs::remove_dir_all(&data).ok();
    }

    /// A save writes its own section and nothing else: another section keeps its bytes, however they are laid out,
    /// and a section the bundle never had stays absent. Two branches that edit different sections then merge cleanly.
    #[test]
    fn a_save_writes_only_its_own_section() {
        let data = a_repo("owned-save");
        let untouched = [
            ("system.json", "{ \"version\": 1, \"name\": \"West\" }\n"),
            ("destinations.json", "[ ]\n"),
            ("lookup-tables.json", "[]"),
            (
                "pipelines/orders.json",
                "{\"id\":\"orders\",\"name\":\"orders\",\"steps\":[]}\n",
            ),
        ];
        Writing::open_in(&data, "work", None, "west")
            .unwrap()
            .commit(
                untouched
                    .iter()
                    .map(|(path, content)| git::FileEdit {
                        path: path.to_string(),
                        content: Some(content.as_bytes().to_vec()),
                    })
                    .collect(),
                "the group",
                &Actor::system(),
            )
            .expect("seeded");
        let save = |touches, mutate: &dyn Fn(&mut FileConfig)| {
            let response = Writing::open_in(&data, "work", None, "west").unwrap().save(
                touches,
                "a save",
                &Actor::system(),
                mutate,
            );
            assert!(response.succeeded(), "saved: {response:?}");
        };
        save(Touches::Sources, &|file| {
            file.sources = vec![SourceDto {
                id: "source-1".to_string(),
                kind: config_file::SourceKindDto::Metrics { bucket_seconds: 10 },
            }]
        });
        save(Touches::OnlyPipelines, &|file| {
            file.pipelines.push(config_file::PipelineDto {
                id: "new".to_string(),
                name: "new".to_string(),
                steps: vec![],
            })
        });

        let reading = Reading::open_in(&data, "work", "west").unwrap();
        let mut files = reading.files();
        for (path, content) in untouched {
            assert_eq!(
                config_file::FileSet::read(&mut files, path).unwrap(),
                Some(content.as_bytes().to_vec()),
                "{path} is as it was"
            );
        }
        assert_eq!(
            config_file::FileSet::read(&mut files, "routes.json").unwrap(),
            None,
            "routes.json stays absent"
        );
        let raw = reading.raw().file;
        assert_eq!((raw.sources.len(), raw.pipelines.len()), (1, 2), "both saves landed");
        std::fs::remove_dir_all(&data).ok();
    }

    /// A pipeline save writes the pipelines that changed and removes the ones that are gone.
    #[test]
    fn a_pipeline_save_writes_what_changed_and_removes_what_went() {
        let pipeline = |id: &str| config_file::PipelineDto {
            id: id.to_string(),
            name: id.to_string(),
            steps: vec![],
        };
        let file = FileConfig {
            version: 1,
            on_exception: None,
            name: None,
            sources: vec![],
            destinations: vec![],
            pipelines: vec![pipeline("keep"), pipeline("edit"), pipeline("remove")],
            routes: vec![],
            lookup_tables: vec![],
            grok_patterns: vec![],
        };
        let old = RawBundle {
            file: file.clone(),
            unreadable: vec![],
        };
        let mut changed = file;
        changed.pipelines.retain(|p| p.id != "remove");
        changed.pipelines[1].name = "edited".to_string();
        changed.pipelines.push(pipeline("add"));

        let mut edits: Vec<_> = edits_for(&changed, &old, Touches::OnlyPipelines)
            .unwrap()
            .into_iter()
            .map(|e| (e.path, e.content.is_some()))
            .collect();
        edits.sort();
        assert_eq!(
            edits,
            [
                ("pipelines/add.json".to_string(), true),
                ("pipelines/edit.json".to_string(), true),
                ("pipelines/remove.json".to_string(), false),
            ]
        );
    }

    /// An edit of the manifest is how the GUI repairs one with an unknown key: the save keeps every field the read
    /// recovered and drops the key.
    #[test]
    fn a_manifest_save_repairs_a_manifest_with_an_unknown_key() {
        let data = a_repo("repair-manifest");
        let manifest = br#"{"version":1,"name":"West","unmatchedDlq":"leftovers"}"#.to_vec();
        Writing::open_in(&data, "work", None, "west")
            .unwrap()
            .commit(
                vec![git::FileEdit {
                    path: "system.json".to_string(),
                    content: Some(manifest),
                }],
                "the group",
                &Actor::system(),
            )
            .expect("seeded");

        let body: ManifestBody =
            serde_json::from_str(r#"{"exceptions":{"categories":{"math":"drop"}}}"#).expect("a manifest body");
        let response = Writing::open_in(&data, "work", None, "west").unwrap().save(
            Touches::OnlyManifest,
            "edit manifest",
            &Actor::system(),
            |file| body.apply(file),
        );
        assert!(response.succeeded(), "saved: {response:?}");

        let raw = Reading::open_in(&data, "work", "west").unwrap().raw();
        assert!(raw.unreadable.is_empty(), "the key is gone: {:?}", raw.unreadable);
        assert_eq!(raw.file.name.as_deref(), Some("West"));
        assert_eq!(
            raw.file.on_exception,
            Some(config_file::OnExceptionDto {
                categories: [("math".to_string(), config_file::AnswerDto::Drop)].into(),
                ..Default::default()
            }),
            "the edit landed"
        );
        let system = config_file::FileSet::read(
            &mut Reading::open_in(&data, "work", "west").unwrap().files(),
            "system.json",
        )
        .unwrap()
        .expect("a manifest");
        let system: serde_json::Value = serde_json::from_slice(&system).unwrap();
        assert_eq!(
            system["exceptions"],
            serde_json::json!({"categories": {"math": "drop"}})
        );
        std::fs::remove_dir_all(&data).ok();
    }

    /// A read says what is wrong with the bundle that scope runs. For a group that is the merge, so a route of
    /// its own onto a shared source reads as fine. It is fine, and it runs.
    #[test]
    fn a_read_of_a_group_does_not_report_its_own_use_of_shared() {
        let data = a_repo("read-merged");
        let put = |scope: &str, path: &str, body: Vec<u8>| {
            Writing::open_in(&data, "work", None, scope)
                .unwrap()
                .commit(
                    vec![git::FileEdit {
                        path: path.to_string(),
                        content: Some(body),
                    }],
                    "seed",
                    &Actor::system(),
                )
                .expect("seeded");
        };

        put("west", "system.json", br#"{"version":1}"#.to_vec());
        put(
            "shared",
            "sources.json",
            serde_json::to_vec(&vec![SourceDto {
                id: "fleet-audit".to_string(),
                kind: config_file::SourceKindDto::Metrics { bucket_seconds: 10 },
            }])
            .unwrap(),
        );
        put(
            "west",
            "pipelines/passthrough.json",
            serde_json::to_vec(&PipelineDto {
                id: "passthrough".to_string(),
                name: "passthrough".to_string(),
                steps: vec![],
            })
            .unwrap(),
        );
        put(
            "west",
            "routes.json",
            serde_json::to_vec(&vec![config_file::RouteDto {
                id: "route-1".to_string(),
                source: "fleet-audit".to_string(),
                condition: None,
                pipeline: "passthrough".to_string(),
                deliver: vec![],
                on_exception: None,
            }])
            .unwrap(),
        );

        let reading = Reading::open_in(&data, "work", "west").unwrap();
        let raw = reading.raw();
        let said: Vec<String> = errors_of_read(&reading, &raw).into_iter().map(|e| e.message).collect();
        assert!(said.is_empty(), "the merged bundle resolves the source: {said:?}");
    }

    /// A read is lenient so one broken file cannot blank every page. Judging a group by a strict merged load
    /// gave that up: the load stops at the first file it cannot parse, so every other problem in the bundle --
    /// and every page that could still have been drawn from what did parse -- went with it.
    #[test]
    fn one_unparseable_file_does_not_hide_a_group_s_other_problems() {
        let data = a_repo("lenient-merge");
        let put = |scope: &str, path: &str, body: Vec<u8>| {
            Writing::open_in(&data, "work", None, scope)
                .unwrap()
                .commit(
                    vec![git::FileEdit {
                        path: path.to_string(),
                        content: Some(body),
                    }],
                    "seed",
                    &Actor::system(),
                )
                .expect("seeded");
        };

        put("west", "system.json", br#"{"version":1}"#.to_vec());
        // One file the reader cannot parse at all.
        put("west", "destinations.json", b"{ this is not json".to_vec());
        // And, separately, a route naming a pipeline nothing declares.
        put(
            "west",
            "routes.json",
            serde_json::to_vec(&vec![config_file::RouteDto {
                id: "route-1".to_string(),
                source: "nowhere".to_string(),
                condition: None,
                pipeline: "also-nowhere".to_string(),
                deliver: vec![],
                on_exception: None,
            }])
            .unwrap(),
        );

        let reading = Reading::open_in(&data, "work", "west").unwrap();
        let raw = reading.raw();
        let said: Vec<String> = errors_of_read(&reading, &raw).into_iter().map(|e| e.message).collect();

        assert!(
            said.iter().any(|m| m.contains("nowhere")),
            "the route's dangling reference still has to be reported: {said:?}"
        );
        // The caller reports the unparseable file itself, from `raw.unreadable`.
        assert!(
            !raw.unreadable.is_empty(),
            "the lenient read names the file it could not parse"
        );
    }

    /// What `shared/` exists for: a group wiring its own route to a source declared once, fleet-wide. A group's
    /// save is judged against the merge, where that source is known.
    #[test]
    fn a_group_may_route_from_a_source_shared_declares() {
        let data = a_repo("route-into-shared");
        let manifest = br#"{"version":1}"#.to_vec();

        let put = |scope: &str, path: &str, body: Vec<u8>, msg: &str| {
            Writing::open_in(&data, "work", None, scope)
                .unwrap()
                .commit(
                    vec![git::FileEdit {
                        path: path.to_string(),
                        content: Some(body),
                    }],
                    msg,
                    &Actor::system(),
                )
                .unwrap_or_else(|e| panic!("{msg}: {e:?}"));
        };

        put("west", "system.json", manifest.clone(), "the group");
        put(
            "shared",
            "sources.json",
            serde_json::to_vec(&vec![SourceDto {
                id: "fleet-audit".to_string(),
                kind: config_file::SourceKindDto::Metrics { bucket_seconds: 10 },
            }])
            .unwrap(),
            "a source every group draws on",
        );
        put(
            "west",
            "pipelines/passthrough.json",
            serde_json::to_vec(&PipelineDto {
                id: "passthrough".to_string(),
                name: "passthrough".to_string(),
                steps: vec![],
            })
            .unwrap(),
            "the group's own pipeline",
        );

        // The group's own route, onto the shared source. Nothing in `groups/west` declares it.
        let route = serde_json::to_vec(&vec![config_file::RouteDto {
            id: "r1".to_string(),
            source: "fleet-audit".to_string(),
            condition: None,
            pipeline: "passthrough".to_string(),
            deliver: vec![],
            on_exception: None,
        }])
        .unwrap();

        let writing = Writing::open_in(&data, "work", None, "west").unwrap();
        let edits = vec![git::FileEdit {
            path: "routes.json".to_string(),
            content: Some(route),
        }];
        refuse_broken_merge(&writing, &edits).expect("the merged bundle resolves the source, so the route lands");

        // And a route onto a source neither subtree has is still refused, so this is the merge and not a hole.
        let dangling = serde_json::to_vec(&vec![config_file::RouteDto {
            id: "r2".to_string(),
            source: "nowhere".to_string(),
            condition: None,
            pipeline: "passthrough".to_string(),
            deliver: vec![],
            on_exception: None,
        }])
        .unwrap();
        let refused = refuse_broken_merge(
            &writing,
            &[git::FileEdit {
                path: "routes.json".to_string(),
                content: Some(dangling),
            }],
        )
        .expect_err("a source no subtree declares is still unknown");
        assert!(
            refused.errors[0].message.contains("nowhere"),
            "it names what is missing: {}",
            refused.errors[0].message
        );
    }

    /// A group runs `shared/` merged with its own subtree, so an id that is free in each on its own is a
    /// collision in the only bundle that is ever executed -- and neither scope's own validation can see the
    /// other. Both ways in are held to it: the group first then `shared/`, and the reverse.
    #[test]
    fn an_id_the_other_subtree_already_holds_is_refused() {
        // Built rather than written out, so the fixture cannot drift from the shape the loader expects.
        let source = |id: &str| {
            serde_json::to_vec(&vec![SourceDto {
                id: id.to_string(),
                kind: config_file::SourceKindDto::Metrics { bucket_seconds: 10 },
            }])
            .unwrap()
        };
        let manifest = br#"{"version":1}"#.to_vec();

        // An area is a group's id, or `shared`, the subtree every group draws on.
        for (first, second) in [("west", "shared"), ("shared", "west")] {
            let data = a_repo(&format!("merge-{first}-first"));

            // The group has to exist for either side to merge into it.
            Writing::open_in(&data, "work", None, "west")
                .unwrap()
                .commit(
                    vec![git::FileEdit {
                        path: "system.json".to_string(),
                        content: Some(manifest.clone()),
                    }],
                    "the group",
                    &Actor::system(),
                )
                .expect("a group to merge into");

            Writing::open_in(&data, "work", None, first)
                .unwrap()
                .commit(
                    vec![git::FileEdit {
                        path: "sources.json".to_string(),
                        content: Some(source("source-1")),
                    }],
                    "the first source-1",
                    &Actor::system(),
                )
                .expect("the first one lands: nothing holds that id yet");

            let writing = Writing::open_in(&data, "work", None, second).unwrap();
            let edits = vec![git::FileEdit {
                path: "sources.json".to_string(),
                content: Some(source("source-1")),
            }];
            let refused = refuse_broken_merge(&writing, &edits)
                .expect_err(&format!("source-1 in {first} then {second} must be refused"));
            let said = &refused.errors[0].message;
            assert!(said.contains("source-1"), "it names the id: {said}");
            assert!(said.contains("west"), "and the group it breaks: {said}");

            // An id nothing else holds still lands, so the check refuses the collision and not the save.
            let free = vec![git::FileEdit {
                path: "sources.json".to_string(),
                content: Some(source("source-2")),
            }];
            refuse_broken_merge(&writing, &free).expect("an id free in both subtrees is not a collision");
        }
    }

    /// Two worker groups reading one topic in one consumer group share its offsets, so given different starts each
    /// deploy of one would move the other's partitions. Only that pair is refused: one start, or a group of its own,
    /// lands.
    #[test]
    fn one_consumer_group_read_from_two_starts_in_two_worker_groups_is_refused() {
        let source = |group: &str, start: &str| {
            format!(
                r#"[{{"id":"orders","type":"kafka","brokers":"127.0.0.1:9","topics":["orders"],"groupId":"{group}"{start}}}]"#
            )
            .into_bytes()
        };
        let latest = r#","start":{"type":"latest"}"#;
        let data = a_repo("shared-group-starts");
        for (group, sources) in [("west", Some(source("orders", latest))), ("east", None)] {
            let mut edits = vec![git::FileEdit {
                path: "system.json".to_string(),
                content: Some(br#"{"version":1}"#.to_vec()),
            }];
            edits.extend(sources.map(|content| git::FileEdit {
                path: "sources.json".to_string(),
                content: Some(content),
            }));
            Writing::open_in(&data, "work", None, group)
                .unwrap()
                .commit(edits, "a group", &Actor::system())
                .expect("each group lands on its own");
        }

        let writing = Writing::open_in(&data, "work", None, "east").unwrap();
        let save = |content: Vec<u8>| {
            refuse_broken_merge(
                &writing,
                &[git::FileEdit {
                    path: "sources.json".to_string(),
                    content: Some(content),
                }],
            )
        };
        let refused = save(source("orders", "")).expect_err("earliest against west's latest must be refused");
        let said = &refused.errors[0].message;
        assert!(
            said.contains("west") && said.contains("east"),
            "it names both groups: {said}"
        );
        save(source("orders", latest)).expect("one start moves nothing back");
        save(source("orders-east", "")).expect("a group of its own shares no offsets");

        // A group the branch does not hold yet is checked the same as one it does.
        let north = Writing::open_in(&data, "work", None, "north").unwrap();
        let refused = refuse_broken_merge(
            &north,
            &[
                git::FileEdit {
                    path: "system.json".to_string(),
                    content: Some(br#"{"version":1}"#.to_vec()),
                },
                git::FileEdit {
                    path: "sources.json".to_string(),
                    content: Some(source("orders", "")),
                },
            ],
        )
        .expect_err("a new group's earliest against west's latest must be refused");
        assert!(
            refused.errors[0].message.contains("north"),
            "{}",
            refused.errors[0].message
        );
    }

    /// A sample is written and read back through the same commit machinery as any other bundle file, and the
    /// records that come out are the ones that went in.
    #[test]
    fn a_sample_round_trips_through_a_branch() {
        let data = a_repo("sample-roundtrip");
        let sample = config_file::SampleDto {
            id: "orders-west, ordinary hour".to_string(),
            name: "orders-west, ordinary hour".to_string(),
            source: Some("orders-west".to_string()),
            records: vec![smeltr::io::raw_object(r#"{"order_id":"A-88120"}"#.to_string())],
            columns: Vec::new(),
        };

        let writing = Writing::open_in(&data, "work", None, "shared").unwrap();
        let path = config_file::sample_path(&sample.id).unwrap();
        writing
            .commit(
                vec![git::FileEdit {
                    path,
                    content: Some(config_file::sample_bytes(&sample)),
                }],
                "save sample",
                &Actor::system(),
            )
            .expect("the sample lands");

        let reading = Reading::open_in(&data, "work", "shared").unwrap();
        let read = config_file::read_sample(&mut reading.files(), &sample.id)
            .unwrap()
            .expect("the sample reads back");
        assert_eq!(
            read.records[0]["_raw"].as_str().unwrap(),
            sample.records[0]["_raw"].as_str().unwrap()
        );
        assert_eq!(read.source.as_deref(), Some("orders-west"));

        let (mut listed, unreadable) = config_file::list_samples(&mut reading.files());
        fill_written(&reading, &mut listed).unwrap();
        assert!(unreadable.is_empty());
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].records, 1);
        let saved = git::log(&reading.repo, &reading.commit, 1).unwrap();
        assert_eq!(
            listed[0].written,
            Some(saved[0].time),
            "written when the leader saved it"
        );
        std::fs::remove_dir_all(&data).ok();
    }

    /// A deployment reads the files it names and every pipeline. A sample is neither, so committing one leaves what
    /// a worker would load untouched.
    #[test]
    fn a_sample_does_not_reach_a_deployment() {
        let data = a_repo("sample-not-deployed");
        let writing = Writing::open_in(&data, "work", None, "shared").unwrap();
        writing
            .commit(
                vec![git::FileEdit {
                    path: config_file::sample_path("some records").unwrap(),
                    content: Some(br#"{"id":"some records","name":"some records","records":[{"_raw":"{}"}]}"#.to_vec()),
                }],
                "save sample",
                &Actor::system(),
            )
            .expect("the sample lands");

        let reading = Reading::open_in(&data, "work", "shared").unwrap();
        let raw = reading.raw();
        assert!(raw.unreadable.is_empty(), "a sample is not read as configuration");
        assert!(raw.file.pipelines.is_empty(), "and not as a pipeline");
        std::fs::remove_dir_all(&data).ok();
    }

    fn sample(id: &str) -> config_file::SampleDto {
        config_file::SampleDto {
            id: id.to_string(),
            name: id.to_string(),
            source: None,
            records: vec![],
            columns: Vec::new(),
        }
    }

    /// `slugify` collapses "Orders: West" and "orders_WEST!" onto one file. Writing the second over the first would
    /// keep whichever went last with `ok: true`, and nothing anywhere would say the first sample had gone.
    #[test]
    fn a_sample_whose_id_slugs_onto_another_is_refused() {
        let data = a_repo("sample-collision");
        let west = sample("Orders: West");
        let path = config_file::sample_path(&west.id).unwrap();
        Writing::open_in(&data, "work", None, "shared")
            .unwrap()
            .commit(
                vec![git::FileEdit {
                    path: path.clone(),
                    content: Some(config_file::sample_bytes(&west)),
                }],
                "save sample",
                &Actor::system(),
            )
            .expect("the first sample lands");

        let collide = sample("orders_WEST!");
        assert_eq!(
            config_file::sample_path(&collide.id).unwrap(),
            path,
            "both slug to one file"
        );
        let writing = Writing::open_in(&data, "work", None, "shared").unwrap();
        let content = Some(config_file::sample_bytes(&collide));
        let err = refuse_sample_collision(&mut writing.files(), &path, &collide.id, &content)
            .expect_err("a different id at the same path is refused");
        assert!(err.contains("Orders: West") && err.contains("orders_WEST!"), "{err}");
        assert!(err.contains(&path), "{err}");
        std::fs::remove_dir_all(&data).ok();
    }

    /// Saving the same sample again -- its id unchanged -- is an ordinary update, not a collision.
    #[test]
    fn a_same_id_resave_is_not_refused() {
        let data = a_repo("sample-same-id-resave");
        let original = sample("orders-west");
        let path = config_file::sample_path(&original.id).unwrap();
        Writing::open_in(&data, "work", None, "shared")
            .unwrap()
            .commit(
                vec![git::FileEdit {
                    path: path.clone(),
                    content: Some(config_file::sample_bytes(&original)),
                }],
                "save sample",
                &Actor::system(),
            )
            .expect("the sample lands");

        let mut updated = original;
        updated.records.push(smeltr::io::raw_object("{}".to_string()));
        let writing = Writing::open_in(&data, "work", None, "shared").unwrap();
        let content = Some(config_file::sample_bytes(&updated));
        refuse_sample_collision(&mut writing.files(), &path, &updated.id, &content)
            .expect("the same id re-saving over itself is not a collision");
        std::fs::remove_dir_all(&data).ok();
    }

    /// A delete never collides: there is nothing left at the path to protect once the content is `None`.
    #[test]
    fn a_delete_is_not_blocked_by_the_collision_check() {
        let data = a_repo("sample-delete-not-blocked");
        let original = sample("orders-west");
        let path = config_file::sample_path(&original.id).unwrap();
        Writing::open_in(&data, "work", None, "shared")
            .unwrap()
            .commit(
                vec![git::FileEdit {
                    path: path.clone(),
                    content: Some(config_file::sample_bytes(&original)),
                }],
                "save sample",
                &Actor::system(),
            )
            .expect("the sample lands");

        let writing = Writing::open_in(&data, "work", None, "shared").unwrap();
        refuse_sample_collision(&mut writing.files(), &path, "some other id", &None)
            .expect("deleting is never refused as a collision");
        std::fs::remove_dir_all(&data).ok();
    }

    /// An existing file that does not parse as a sample is treated as occupied: an unreadable file is not a license
    /// to destroy it.
    #[test]
    fn an_unparseable_existing_file_is_treated_as_occupied() {
        let data = a_repo("sample-unparseable");
        let path = config_file::sample_path("orders-west").unwrap();
        Writing::open_in(&data, "work", None, "shared")
            .unwrap()
            .commit(
                vec![git::FileEdit {
                    path: path.clone(),
                    content: Some(b"not json".to_vec()),
                }],
                "seed a broken file",
                &Actor::system(),
            )
            .expect("the broken file lands");

        let writing = Writing::open_in(&data, "work", None, "shared").unwrap();
        let content = Some(config_file::sample_bytes(&sample("orders-west")));
        let err = refuse_sample_collision(&mut writing.files(), &path, "orders-west", &content)
            .expect_err("an unreadable file is not a license to destroy it");
        assert!(err.contains(&path), "{err}");
        std::fs::remove_dir_all(&data).ok();
    }

    /// The second of two editors on one branch is refused rather than overwriting the first.
    #[test]
    fn a_save_against_a_commit_the_branch_has_moved_past_is_refused() {
        let data = a_repo("base-stale");
        let repo = git::repo_path(&data);
        let read_at = git::main_head(&repo).unwrap();

        let writing = Writing::open_in(&data, "work", Some(&read_at), "shared").unwrap();
        let moved = writing
            .commit(
                vec![git::FileEdit {
                    path: "sources.json".to_string(),
                    content: Some(b"[]".to_vec()),
                }],
                "somebody else's save",
                &Actor::system(),
            )
            .expect("the first save lands");
        assert_ne!(moved, read_at);

        let err = Writing::open_in(&data, "work", Some(&read_at), "shared").expect_err("the branch moved");
        assert!(err.refusal.message().contains(&read_at[..8]), "{err:?}");
        assert!(err.refusal.message().contains(&moved[..8]), "{err:?}");
        // The refused change is the one thing here that is not in git, so the message must say so rather than
        // reassuring the reader that every save is a commit -- this one was not.
        assert!(err.refusal.message().contains("was not written"), "{err:?}");
        assert!(err.refusal.message().contains("nowhere else"), "{err:?}");
        // And both commits without reading the sentence, so the editor can offer to carry the change onto a
        // branch of its own rather than parse English for the one it would start from.
        let conflict = err.conflict.expect("a branch that moved says which commits");
        assert_eq!(conflict.base, read_at);
        assert_eq!(conflict.now.as_deref(), Some(moved.as_str()));
        std::fs::remove_dir_all(&data).ok();
    }

    /// An editor that never read is a first write or a script, and both save without one.
    #[test]
    fn a_save_that_names_no_commit_is_not_checked() {
        let data = a_repo("base-absent");
        Writing::open_in(&data, "work", None, "shared")
            .unwrap()
            .commit(
                vec![git::FileEdit {
                    path: "sources.json".to_string(),
                    content: Some(b"[]".to_vec()),
                }],
                "somebody else's save",
                &Actor::system(),
            )
            .expect("the first save lands");
        assert!(Writing::open_in(&data, "work", None, "shared").is_ok());
        std::fs::remove_dir_all(&data).ok();
    }

    /// A read is a commit and nothing else: it never starts a branch. That is what lets a reader run with no lock
    /// at all, and what a mistyped `?branch=` must not cost.
    #[test]
    fn reading_a_branch_nobody_saved_on_reads_main_and_makes_nothing() {
        let data = temp_dir("read-unstarted");
        let repo = git::repo_path(&data);
        git::init_bare_repo(&repo).unwrap();
        git::ensure_main_commit(&repo).unwrap();

        let reading = Reading::open_in(&data, "no-such-branch", "shared").expect("an unstarted branch reads");
        assert_eq!(reading.branch, git::MAIN_BRANCH);
        assert_eq!(Some(reading.commit.clone()), git::main_head(&repo));
        assert!(
            !git::branch_exists(&repo, "no-such-branch").unwrap(),
            "a read started a branch"
        );
        std::fs::remove_dir_all(&data).ok();
    }

    /// `main` moves by deploy, never by an editor.
    #[test]
    fn main_is_not_editable() {
        let err = Writing::open(git::MAIN_BRANCH, None, "shared").expect_err("main is refused");
        assert!(err.refusal.message().contains("deploy"), "{err:?}");
        // Not a conflict: nothing moved, and there is nothing to carry anywhere.
        assert!(err.conflict.is_none(), "{err:?}");
    }

    // -- lookup files ----------------------------------------------------

    /// A repository whose `work` branch holds `files` in `shared/`, each in the lookup store and committed as its
    /// pointer, as an upload leaves it.
    fn a_repo_holding(label: &str, files: &[(&str, &[u8])]) -> PathBuf {
        use std::io::Write;
        let data = a_repo(label);
        let repo = git::repo_path(&data);
        let edits = files.iter().map(|(path, bytes)| {
            let mut incoming = smeltr::lookup::store::Incoming::new(&repo).unwrap();
            incoming.write_all(bytes).unwrap();
            git::FileEdit {
                path: path.to_string(),
                content: Some(incoming.finish(None).unwrap().text().into_bytes()),
            }
        });
        let writing = Writing::open_in(&data, "work", None, "shared").unwrap();
        writing.commit(edits.collect(), "seed", &Actor::system()).unwrap();
        data
    }

    /// A request that names no branch reads `main`, and a write is refused rather than landing on a branch nobody
    /// chose.
    #[test]
    fn a_write_that_names_no_branch_is_refused() {
        let data = a_repo("unnamed-branch");
        let unnamed = BranchQuery::default();
        assert_eq!(unnamed.name(), git::MAIN_BRANCH);
        assert!(Writing::open_in(&data, unnamed.name(), None, config_file::SHARED_DIR).is_err());
        std::fs::remove_dir_all(&data).ok();
    }

    fn query(path: &str, ip: Option<&str>) -> LookupFileQuery {
        let (path, ip) = (path.to_string(), ip.map(str::to_string));
        LookupFileQuery {
            // Where `a_repo_holding` seeds its files.
            branch: BranchQuery {
                branch: Some("work".to_string()),
            },
            path,
            ip,
            at: None,
        }
    }

    /// A review compares a file a change replaced with the one it replaced it with, so the old one is read at the
    /// commit that still names it.
    #[test]
    fn a_lookup_file_a_later_commit_removed_is_read_at_one_that_holds_it() {
        let data = a_repo_holding("lookup-at", &[("lookups/a.csv", b"ip,host\n")]);
        let held = Reading::open_in(&data, "work", "shared").unwrap().commit;
        let writing = Writing::open_in(&data, "work", None, "shared").unwrap();
        let removed = git::FileEdit {
            path: "lookups/a.csv".to_string(),
            content: None,
        };
        writing.commit(vec![removed], "remove", &Actor::system()).unwrap();
        let work = |at: Option<&str>| LookupFileQuery {
            at: at.map(str::to_string),
            ..query("lookups/a.csv", None)
        };
        lookup_text_in(&data, config_file::SHARED_DIR, &work(None)).expect_err("gone at the tip");
        assert_eq!(
            lookup_text_in(&data, config_file::SHARED_DIR, &work(Some(&held))).unwrap()["text"],
            "ip,host\n"
        );
        std::fs::remove_dir_all(&data).ok();
    }

    #[test]
    fn a_lookup_file_is_read_from_lookups_and_nowhere_else() {
        let data = a_repo_holding(
            "lookup-read",
            &[("lookups/a.csv", b"ip,host\n"), ("lookups/b.bin", b"\xff")],
        );
        assert_eq!(
            lookup_text_in(&data, config_file::SHARED_DIR, &query("lookups/a.csv", None)).unwrap()["text"],
            "ip,host\n"
        );
        for refused in ["system.json", "lookups/../system.json", "/etc/passwd", "lookups/b.bin"] {
            lookup_text_in(&data, config_file::SHARED_DIR, &query(refused, None)).expect_err(refused);
        }
        std::fs::remove_dir_all(&data).ok();
    }

    #[test]
    fn an_mmdb_shows_its_metadata_its_networks_and_a_probe() {
        let data = a_repo_holding(
            "lookup-mmdb",
            &[(
                "lookups/city.mmdb",
                include_bytes!("../../lookup/test-data/test-city.mmdb"),
            )],
        );
        let info = |ip| mmdb_info_in(&data, config_file::SHARED_DIR, &query("lookups/city.mmdb", ip));
        let started = std::time::Instant::now();
        let all = info(None).unwrap();
        eprintln!("walked {} networks in {:?}", all["networks"], started.elapsed());
        assert!(all["networks"].as_u64() > Some(0) && all["databaseType"].is_string() && all.get("record").is_none());
        assert!(info(Some("10.1.5.5")).unwrap()["record"].is_object(), "a hit");
        assert!(info(Some("8.8.8.8")).unwrap()["record"].is_null(), "a miss");
        assert!(
            info(Some("10.1.5"))
                .expect_err("not an IP")
                .message()
                .contains("10.1.5")
        );
        std::fs::remove_dir_all(&data).ok();
    }

    fn save_hosts(data: &Path, contents: &str) -> Envelope<Save> {
        let edit =
            serde_json::json!({ "id": "hosts", "type": "csv", "path": "", "delimiter": ";", "contents": contents });
        let edits = vec![serde_json::from_value(edit).unwrap()];
        save_lookup_tables(
            Writing::open_in(data, "work", None, "shared").unwrap(),
            edits,
            &Actor::system(),
        )
    }

    /// A lookup-table save to an area that is not one is refused as every save is, before its contents are read:
    /// no commit, the area's reason, and nothing moved to carry a change over from.
    #[test]
    fn a_lookup_table_save_to_an_area_that_is_not_one_is_refused_like_any_save() {
        let data = a_repo("lookup-no-scope");
        let edit = serde_json::json!({ "id": "hosts", "type": "csv", "path": "", "delimiter": ";", "contents": "x" });
        // As `put_lookup_tables` answers it.
        let refused = Writing::open_in(&data, "work", None, "../escaped").map_or_else(Save::refused, |writing| {
            save_lookup_tables(writing, vec![serde_json::from_value(edit).unwrap()], &Actor::system())
        });
        assert!(!refused.succeeded());
        assert!(
            refused.errors[0].message.contains("../escaped"),
            "{}",
            refused.errors[0].message
        );
        assert!(refused.body.commit.is_none() && refused.body.conflict.is_none());
        std::fs::remove_dir_all(&data).ok();
    }

    /// The file and the table that names it land in one commit, and a pipeline that looks the table up is compiled
    /// against the new file, which its parent does not hold.
    #[test]
    fn a_csv_saved_with_its_contents_is_one_commit_even_when_a_pipeline_reads_it() {
        let pipeline = br#"{"id": "p", "name": "p", "steps": [
            {"id": "e", "type": "eval", "assignments": [{"target": "ip", "expr": "\"10.0.0.1\""}]},
            {"id": "l", "type": "lookup", "input": "ip", "table": "hosts", "matchKind": "exact", "keyColumn": "ip",
             "outputs": [{"source": "host", "field": "h"}]}]}"#;
        let tables = br#"[{"id": "hosts", "type": "csv", "path": "lookups/old.csv", "delimiter": ";"}]"#;
        let data = a_repo("lookup-contents");
        commit_to(
            &data,
            "work",
            &[
                ("pipelines/p.json", pipeline.to_vec()),
                ("lookup-tables.json", tables.to_vec()),
                ("lookups/old.csv", b"ip;host\n".to_vec()),
            ],
        );
        let saved = save_hosts(&data, "ip;host\n10.0.0.1;web\n");
        assert!(saved.succeeded(), "{saved:?}");

        let log = git::log(&git::repo_path(&data), "work", 10).unwrap();
        assert_eq!(log.len(), 3, "main's first commit, the seed, and this save");
        let raw = Reading::open_in(&data, "work", "shared").unwrap().raw();
        let LookupSourceDto::Csv { path, headers, .. } = &raw.file.lookup_tables[0].source else {
            panic!()
        };
        assert_eq!(headers, &["ip", "host"]);
        assert_eq!(path, "lookups/hosts.csv");
        for file in [
            "shared/lookup-tables.json",
            "shared/lookups/hosts.csv",
            "shared/lookups/old.csv",
        ] {
            assert!(log[0].files.iter().any(|f| f == file), "{file} in {:?}", log[0].files);
        }
        let tree = git::read_blobs(
            &git::repo_path(&data),
            &log[0].commit,
            &["shared/lookups/old.csv".to_string()],
        )
        .unwrap();
        assert_eq!(tree, vec![None], "the file the table no longer names is gone");
        std::fs::remove_dir_all(&data).ok();
    }

    /// Each save rewrites the table's one pointer, so the branch holds no file that nothing names.
    #[test]
    fn saving_a_table_again_rewrites_its_own_file() {
        let data = a_repo("lookup-contents-again");
        assert!(save_hosts(&data, "ip;host\n10.0.0.1;web\n").succeeded());
        let saved = save_hosts(&data, "ip;host\n10.0.0.1;db\n");
        assert!(saved.succeeded(), "{saved:?}");

        let repo = git::repo_path(&data);
        let log = git::log(&repo, "work", 10).unwrap();
        assert_eq!(log[0].files, ["shared/lookups/hosts.csv"], "only the pointer changed");
        let files: Vec<String> = git::list_tree(&repo, &log[0].commit, "shared/lookups")
            .unwrap()
            .into_iter()
            .map(|e| e.name)
            .collect();
        assert_eq!(files, ["hosts.csv"]);
        std::fs::remove_dir_all(&data).ok();
    }

    /// The rows a save replaces in place are judged against the commit before it, so a pipeline the new rows break
    /// is refused rather than read as broken already.
    #[test]
    fn rows_that_break_a_lookup_are_refused_though_saved_in_place() {
        let pipeline = br#"{"id": "p", "name": "p", "steps": [
            {"id": "e", "type": "eval", "assignments": [{"target": "ip", "expr": "\"10.0.0.1\""}]},
            {"id": "l", "type": "lookup", "input": "ip", "table": "hosts", "matchKind": "exact", "keyColumn": "ip",
             "outputs": [{"source": "host", "field": "h"}]}]}"#;
        let data = a_repo("lookup-contents-break");
        commit_to(&data, "work", &[("pipelines/p.json", pipeline.to_vec())]);
        let first = save_hosts(&data, "ip;host\n10.0.0.1;web\n");
        assert!(first.succeeded(), "{first:?}");

        let refused = save_hosts(&data, "ip;owner\n10.0.0.1;web\n");
        assert!(!refused.succeeded(), "the lookup reads a column the new rows dropped");
        std::fs::remove_dir_all(&data).ok();
    }

    /// An id that is not a file name as it stands, or one whose path holds another table's file, is saved by
    /// content: two ids must never share a path.
    #[test]
    fn a_table_with_no_path_of_its_own_is_saved_by_content() {
        let data = a_repo("lookup-contents-odd");
        commit_to(
            &data,
            "work",
            &[(
                "lookup-tables.json",
                br#"[{"id": "under", "type": "csv", "path": "lookups/b.csv/rows.csv", "delimiter": ";"}]"#.to_vec(),
            )],
        );
        let table = |id: &str, contents: Option<&str>| {
            let mut t =
                serde_json::json!({ "id": id, "type": "csv", "path": "lookups/b.csv/rows.csv", "delimiter": ";" });
            if let Some(c) = contents {
                t["path"] = "".into();
                t["contents"] = c.into();
            }
            serde_json::from_value(t).unwrap()
        };
        let edits = vec![
            table("under", None),
            table("a b", Some("ip;host\n")),
            table("b", Some("ip;host\n")),
        ];
        let saved = save_lookup_tables(
            Writing::open_in(&data, "work", None, "shared").unwrap(),
            edits,
            &Actor::system(),
        );
        assert!(saved.succeeded(), "{saved:?}");

        let raw = Reading::open_in(&data, "work", "shared").unwrap().raw();
        let paths: Vec<&str> = raw.file.lookup_tables.iter().filter_map(lookup_file_of).collect();
        assert_eq!(paths[0], "lookups/b.csv/rows.csv");
        assert!(paths[1].ends_with("-a_b.csv"), "{paths:?}");
        assert!(paths[2].ends_with("-b.csv"), "{paths:?}");
        std::fs::remove_dir_all(&data).ok();
    }

    /// A table whose own file another table names leaves that table's rows alone.
    #[test]
    fn a_table_whose_file_another_names_is_saved_by_content() {
        let tables = br#"[{"id": "other", "type": "csv", "path": "lookups/hosts.csv", "delimiter": ";"}]"#;
        let data = a_repo("lookup-contents-shared");
        commit_to(
            &data,
            "work",
            &[
                ("lookup-tables.json", tables.to_vec()),
                ("lookups/hosts.csv", b"ip;host\n".to_vec()),
            ],
        );
        let other = serde_json::json!({ "id": "other", "type": "csv", "path": "lookups/hosts.csv", "delimiter": ";" });
        let hosts = serde_json::json!({ "id": "hosts", "type": "csv", "path": "", "delimiter": ";",
                                        "contents": "ip;host\n10.0.0.1;web\n" });
        let edits = vec![
            serde_json::from_value(other).unwrap(),
            serde_json::from_value(hosts).unwrap(),
        ];
        let saved = save_lookup_tables(
            Writing::open_in(&data, "work", None, "shared").unwrap(),
            edits,
            &Actor::system(),
        );
        assert!(saved.succeeded(), "{saved:?}");

        let raw = Reading::open_in(&data, "work", "shared").unwrap().raw();
        let paths: Vec<&str> = raw.file.lookup_tables.iter().filter_map(lookup_file_of).collect();
        assert_eq!(paths[0], "lookups/hosts.csv");
        assert!(paths[1].ends_with("-hosts.csv"), "{paths:?}");
        std::fs::remove_dir_all(&data).ok();
    }

    #[test]
    fn a_csv_whose_contents_do_not_parse_is_refused_by_name() {
        let data = a_repo("lookup-contents-bad");
        let refused = save_hosts(&data, "");
        assert!(
            !refused.succeeded() && refused.errors[0].message.contains("\"hosts\""),
            "{refused:?}"
        );
        assert!(!git::branch_exists(&git::repo_path(&data), "work").unwrap());
        std::fs::remove_dir_all(&data).ok();
    }

    // -- reading at a commit, and undo -------------------------------------

    /// Commits `files` into `shared/` on `branch`, which starts at `main` if it has no saves. `main` too, which no
    /// `Writing` moves: it is how a test has somebody deploy.
    fn commit_to(data: &Path, branch: &str, files: &[(&str, Vec<u8>)]) -> String {
        let repo = git::repo_path(data);
        let expected = git::branch_tip(&repo, branch);
        let parent = expected.clone().or_else(|| git::main_head(&repo)).unwrap();
        let edits: Vec<git::FileEdit> = files
            .iter()
            .map(|(path, content)| git::FileEdit {
                path: format!("shared/{path}"),
                content: Some(content.clone()),
            })
            .collect();
        match git::commit_edits(
            &repo,
            branch,
            &parent,
            expected.as_deref(),
            &edits,
            "seed",
            &Actor::system(),
        )
        .unwrap()
        {
            git::Committed::At(commit) => commit,
            _ => panic!("the seed did not land"),
        }
    }

    fn a_source(id: &str) -> SourceDto {
        SourceDto {
            id: id.to_string(),
            kind: config_file::SourceKindDto::Metrics { bucket_seconds: 10 },
        }
    }

    fn a_route(id: &str) -> RouteDto {
        RouteDto {
            id: id.to_string(),
            source: "s1".to_string(),
            condition: None,
            pipeline: "p".to_string(),
            deliver: vec![],
            on_exception: None,
        }
    }

    /// Pipeline `p`, with one eval step assigning `expr` when there is one.
    fn pipeline_p(expr: Option<&str>) -> PipelineDto {
        let steps = match expr {
            Some(expr) => {
                serde_json::json!([{"id": "e", "type": "eval", "assignments": [{"target": "x", "expr": expr}]}])
            }
            None => serde_json::json!([]),
        };
        serde_json::from_value(serde_json::json!({"id": "p", "name": "p", "steps": steps})).unwrap()
    }

    fn as_json<T: serde::Serialize>(value: &T) -> serde_json::Value {
        serde_json::to_value(value).unwrap()
    }

    /// `main` holding source `s1`, routes `r1`, `r2`, `r3` and pipeline `p`; no `work` yet.
    fn main_with_routes(label: &str) -> PathBuf {
        let data = a_repo(label);
        commit_to(
            &data,
            git::MAIN_BRANCH,
            &[
                ("sources.json", json_bytes(&vec![a_source("s1")])),
                (
                    "routes.json",
                    json_bytes(&vec![a_route("r1"), a_route("r2"), a_route("r3")]),
                ),
                ("pipelines/p.json", json_bytes(&pipeline_p(None))),
            ],
        );
        data
    }

    /// `write_listed_file` as the handlers call it: on `work` at `base`, in `area`.
    fn write_listed(
        data: &Path,
        base: Option<String>,
        area: &str,
        within: &str,
        content: Option<Vec<u8>>,
    ) -> Envelope<Save> {
        Writing::open_in(data, "work", base.as_deref(), area).map_or_else(Save::refused, |writing| {
            write_listed_file(writing, within, content, &Actor::system())
        })
    }

    fn save_work(data: &Path, touches: Touches, mutate: impl FnOnce(&mut FileConfig)) {
        let response =
            Writing::open_in(data, "work", None, "shared")
                .unwrap()
                .save(touches, "an edit", &Actor::system(), mutate);
        assert!(response.succeeded(), "saved: {response:?}");
    }

    fn undo_on_work(data: &Path, section: Section, id: &str) -> Envelope<Save> {
        let req = UndoRequest::Object {
            section,
            id: id.to_string(),
        };
        undo_in(data, "work", None, "shared", req, &Actor::system())
    }

    /// A save on a `Writing` whose branch moved since it was opened is not written: the swap on the commit that was
    /// read refuses it and names both commits. Undo relies on this, reading and saving on one `Writing`.
    #[test]
    fn a_save_on_a_writing_the_branch_moved_past_is_refused_with_both_commits() {
        let data = main_with_routes("undo-moved");
        save_work(&data, Touches::OnlyPipelines, |file| {
            file.pipelines = vec![pipeline_p(Some("1"))]
        });
        let writing = Writing::open_in(&data, "work", None, "shared").unwrap();
        let read_at = writing.parent.clone();
        save_work(&data, Touches::OnlyPipelines, |file| {
            file.pipelines = vec![pipeline_p(Some("2"))]
        });
        let moved = git::branch_tip(&git::repo_path(&data), "work").unwrap();

        let refused = writing.save(Touches::OnlyPipelines, "undo", &Actor::system(), |file| {
            file.pipelines = vec![pipeline_p(None)]
        });
        assert!(!refused.succeeded(), "{refused:?}");
        let conflict = refused.body.conflict.expect("a branch that moved says which commits");
        assert_eq!((conflict.base, conflict.now), (read_at, Some(moved)));
        assert_eq!(
            as_json(&work(&data).pipelines),
            as_json(&vec![pipeline_p(Some("2"))]),
            "the save in between stands"
        );
        std::fs::remove_dir_all(&data).ok();
    }

    fn work(data: &Path) -> FileConfig {
        Reading::open_in(data, "work", "shared").unwrap().raw().file
    }

    fn saves_on_work(data: &Path) -> Vec<git::CommitSummary> {
        git::log(&git::repo_path(data), "work", 20).unwrap()
    }

    #[test]
    fn an_edited_pipeline_goes_back_as_one_save() {
        let data = main_with_routes("undo-pipeline");
        save_work(&data, Touches::OnlyPipelines, |file| {
            file.pipelines = vec![pipeline_p(Some("1"))]
        });
        let before = saves_on_work(&data).len();

        let undone = undo_on_work(&data, Section::Pipelines, "p");
        assert!(undone.succeeded(), "{undone:?}");
        let saves = saves_on_work(&data);
        assert_eq!(saves.len(), before + 1, "one new save");
        assert_eq!(saves[0].subject, "undo changes to pipeline p");
        assert_eq!(saves[0].files, vec!["shared/pipelines/p.json".to_string()]);
        assert_eq!(as_json(&work(&data).pipelines), as_json(&vec![pipeline_p(None)]));
        std::fs::remove_dir_all(&data).ok();
    }

    #[test]
    fn a_source_the_branch_added_is_removed() {
        let data = main_with_routes("undo-added");
        save_work(&data, Touches::Sources, |file| file.sources.push(a_source("s2")));

        let undone = undo_on_work(&data, Section::Sources, "s2");
        assert!(undone.succeeded(), "{undone:?}");
        assert_eq!(as_json(&work(&data).sources), as_json(&vec![a_source("s1")]));
        assert_eq!(saves_on_work(&data)[0].subject, "undo changes to source s2");
        std::fs::remove_dir_all(&data).ok();
    }

    #[test]
    fn a_route_the_branch_removed_comes_back_at_its_place() {
        let data = main_with_routes("undo-removed");
        save_work(&data, Touches::Routes, |file| file.routes.retain(|r| r.id != "r2"));

        let undone = undo_on_work(&data, Section::Routes, "r2");
        assert!(undone.succeeded(), "{undone:?}");
        let ids: Vec<String> = work(&data).routes.into_iter().map(|r| r.id).collect();
        assert_eq!(ids, ["r1", "r2", "r3"]);
        std::fs::remove_dir_all(&data).ok();
    }

    #[test]
    fn a_route_the_branch_edited_and_moved_goes_back_as_and_where_it_was() {
        let data = main_with_routes("undo-moved");
        save_work(&data, Touches::Routes, |file| {
            let mut r1 = file.routes.remove(0);
            r1.condition = Some("true".to_string());
            file.routes.push(r1);
        });

        let undone = undo_on_work(&data, Section::Routes, "r1");
        assert!(undone.succeeded(), "{undone:?}");
        let routes = work(&data).routes;
        let ids: Vec<&str> = routes.iter().map(|r| r.id.as_str()).collect();
        assert_eq!(ids, ["r1", "r2", "r3"]);
        assert_eq!(routes[0].condition, None);
        std::fs::remove_dir_all(&data).ok();
    }

    #[test]
    fn a_route_the_branch_only_moved_has_something_to_undo() {
        let data = main_with_routes("undo-reordered");
        save_work(&data, Touches::Routes, |file| file.routes.rotate_left(1));

        let undone = undo_on_work(&data, Section::Routes, "r1");
        assert!(undone.succeeded(), "{undone:?}");
        let ids: Vec<String> = work(&data).routes.into_iter().map(|r| r.id).collect();
        assert_eq!(ids, ["r1", "r2", "r3"]);
        std::fs::remove_dir_all(&data).ok();
    }

    #[test]
    fn an_object_as_it_was_or_in_neither_has_nothing_to_undo() {
        let data = main_with_routes("undo-nothing");
        save_work(&data, Touches::Sources, |file| file.sources.push(a_source("s2")));
        let tip = git::branch_tip(&git::repo_path(&data), "work");

        let same = undo_on_work(&data, Section::Routes, "r1");
        assert!(
            !same.succeeded() && same.errors[0].message.contains("nothing to undo"),
            "{same:?}"
        );
        let neither = undo_on_work(&data, Section::Routes, "r9");
        assert!(
            !neither.succeeded() && neither.errors[0].message.contains("no route \"r9\""),
            "{neither:?}"
        );
        assert_eq!(
            git::branch_tip(&git::repo_path(&data), "work"),
            tip,
            "nothing was saved"
        );
        std::fs::remove_dir_all(&data).ok();
    }

    /// `path`, within `shared/`, set to `content` on `work`, or removed when that is `None`.
    fn edit_work(data: &Path, path: &str, content: Option<&[u8]>) {
        let writing = Writing::open_in(data, "work", None, "shared").unwrap();
        let edit = git::FileEdit {
            path: path.to_string(),
            content: content.map(<[u8]>::to_vec),
        };
        writing.commit(vec![edit], "an edit", &Actor::system()).unwrap();
    }

    fn undo_file_on_work(data: &Path, path: &str) -> Envelope<Save> {
        let req = UndoRequest::File { path: path.to_string() };
        undo_in(data, "work", None, "shared", req, &Actor::system())
    }

    fn on_work(data: &Path, path: &str) -> Option<Vec<u8>> {
        let repo = git::repo_path(data);
        let tip = git::branch_tip(&repo, "work").unwrap();
        git::read_blobs(&repo, &tip, &[format!("shared/{path}")])
            .unwrap()
            .remove(0)
    }

    #[test]
    fn a_sample_the_branch_deleted_comes_back_as_one_save() {
        let data = main_with_routes("undo-sample");
        let sample = br#"{"id": "s", "name": "s", "records": ["x"]}"#;
        commit_to(&data, git::MAIN_BRANCH, &[("samples/s.json", sample.to_vec())]);
        edit_work(&data, "samples/s.json", None);

        let undone = undo_file_on_work(&data, "samples/s.json");
        assert!(undone.succeeded(), "{undone:?}");
        assert_eq!(on_work(&data, "samples/s.json").as_deref(), Some(&sample[..]));
        let saves = saves_on_work(&data);
        assert_eq!(saves[0].subject, "undo changes to sample s");
        assert_eq!(saves[0].files, vec!["shared/samples/s.json".to_string()]);
        std::fs::remove_dir_all(&data).ok();
    }

    #[test]
    fn a_test_the_branch_added_is_removed() {
        let data = main_with_routes("undo-test");
        edit_work(&data, "tests/t.json", Some(b"{}"));

        let undone = undo_file_on_work(&data, "tests/t.json");
        assert!(undone.succeeded(), "{undone:?}");
        assert_eq!(on_work(&data, "tests/t.json"), None);
        std::fs::remove_dir_all(&data).ok();
    }

    /// The file is gone from the branch, so only the read where the branch left `main` lists it as not loading.
    #[test]
    fn a_file_that_does_not_read_and_was_deleted_comes_back() {
        let data = main_with_routes("undo-unreadable");
        commit_to(&data, git::MAIN_BRANCH, &[("destinations.json", b"{".to_vec())]);
        edit_work(&data, "destinations.json", None);

        let undone = undo_file_on_work(&data, "destinations.json");
        assert!(undone.succeeded(), "{undone:?}");
        assert_eq!(on_work(&data, "destinations.json").as_deref(), Some(&b"{"[..]));
        std::fs::remove_dir_all(&data).ok();
    }

    #[test]
    fn a_file_that_reads_as_objects_or_is_unchanged_or_outside_the_bundle_is_refused() {
        let data = main_with_routes("undo-file-refused");
        save_work(&data, Touches::Routes, |file| file.routes.truncate(2));
        edit_work(&data, "samples/s.json", Some(b"{}"));
        let tip = git::branch_tip(&git::repo_path(&data), "work");

        let objects = undo_file_on_work(&data, "routes.json");
        assert!(
            !objects.succeeded() && objects.errors[0].message.contains("reads as objects"),
            "{objects:?}"
        );
        let same = undo_file_on_work(&data, "samples/other.json");
        assert!(
            !same.succeeded() && same.errors[0].message.contains("nothing to undo"),
            "{same:?}"
        );
        let outside = undo_file_on_work(&data, "samples/../../groups/west/system.json");
        assert!(
            !outside.succeeded() && outside.errors[0].message.contains("not a path within the bundle"),
            "{outside:?}"
        );
        assert_eq!(
            git::branch_tip(&git::repo_path(&data), "work"),
            tip,
            "nothing was saved"
        );
        std::fs::remove_dir_all(&data).ok();
    }

    /// The copy put back is the one the branch started from, not what `main` became after it.
    #[test]
    fn an_undo_restores_where_the_branch_left_main_not_main_s_tip() {
        let data = main_with_routes("undo-base");
        let left_at = git::main_head(&git::repo_path(&data)).unwrap();
        save_work(&data, Touches::OnlyPipelines, |file| {
            file.pipelines = vec![pipeline_p(Some("1"))]
        });
        commit_to(
            &data,
            git::MAIN_BRANCH,
            &[("pipelines/p.json", json_bytes(&pipeline_p(Some("2"))))],
        );

        let undone = undo_on_work(&data, Section::Pipelines, "p");
        assert!(undone.succeeded(), "{undone:?}");
        assert_eq!(as_json(&work(&data).pipelines), as_json(&vec![pipeline_p(None)]));
        assert_eq!(git::branch_base(&git::repo_path(&data), "work").unwrap(), left_at);
        std::fs::remove_dir_all(&data).ok();
    }

    /// A table's file can change under the same name, and the undo puts its bytes back too.
    #[test]
    fn an_undone_lookup_table_gets_its_file_back() {
        use std::io::Write;
        let data = main_with_routes("undo-lookup");
        let repo = git::repo_path(&data);
        let pointer = |bytes: &[u8]| {
            let mut incoming = smeltr::lookup::store::Incoming::new(&repo).unwrap();
            incoming.write_all(bytes).unwrap();
            incoming.finish(None).unwrap().text().into_bytes()
        };
        let tables = br#"[{"id": "hosts", "type": "csv", "path": "lookups/hosts.csv", "delimiter": ";"}]"#.to_vec();
        let was = pointer(b"ip;host\n");
        commit_to(
            &data,
            git::MAIN_BRANCH,
            &[("lookup-tables.json", tables), ("lookups/hosts.csv", was.clone())],
        );
        commit_to(
            &data,
            "work",
            &[("lookups/hosts.csv", pointer(b"ip;host\n10.0.0.1;web\n"))],
        );

        let undone = undo_on_work(&data, Section::LookupTables, "hosts");
        assert!(undone.succeeded(), "{undone:?}");
        let tip = git::branch_tip(&repo, "work").unwrap();
        let file = git::read_blobs(&repo, &tip, &["shared/lookups/hosts.csv".to_string()]).unwrap();
        assert_eq!(file[0].as_ref(), Some(&was));
        assert_eq!(saves_on_work(&data)[0].subject, "undo changes to lookup table hosts");

        let again = undo_on_work(&data, Section::LookupTables, "hosts");
        assert!(
            !again.succeeded() && again.errors[0].message.contains("nothing to undo"),
            "{again:?}"
        );
        std::fs::remove_dir_all(&data).ok();
    }

    #[test]
    fn an_undone_added_lookup_table_takes_its_file_with_it() {
        use std::io::Write;
        let data = main_with_routes("undo-lookup-added");
        let repo = git::repo_path(&data);
        let mut incoming = smeltr::lookup::store::Incoming::new(&repo).unwrap();
        incoming.write_all(b"ip;host\n").unwrap();
        let pointer = incoming.finish(None).unwrap().text().into_bytes();
        let tables = br#"[{"id": "hosts", "type": "csv", "path": "lookups/hosts.csv", "delimiter": ";"}]"#.to_vec();
        commit_to(
            &data,
            "work",
            &[("lookup-tables.json", tables), ("lookups/hosts.csv", pointer)],
        );

        let undone = undo_on_work(&data, Section::LookupTables, "hosts");
        assert!(undone.succeeded(), "{undone:?}");
        let tip = git::branch_tip(&repo, "work").unwrap();
        let file = git::read_blobs(&repo, &tip, &["shared/lookups/hosts.csv".to_string()]).unwrap();
        assert_eq!(file[0], None);
        assert!(work(&data).lookup_tables.is_empty());
        std::fs::remove_dir_all(&data).ok();
    }

    fn config_at(data: &Path, branch: &str, at: Option<&str>) -> Envelope<Config> {
        let q = ConfigQuery {
            branch: BranchQuery {
                branch: Some(branch.to_string()),
            },
            at: at.map(str::to_string),
        };
        config_in(data, config_file::SHARED_DIR, &q)
    }

    /// A read at a commit is the scope as that commit holds it, for any commit of the branch or of `main`.
    #[test]
    fn a_config_read_at_a_commit_is_that_commit() {
        let data = main_with_routes("config-at");
        let repo = git::repo_path(&data);
        let left_at = git::main_head(&repo).unwrap();
        save_work(&data, Touches::Sources, |file| file.sources.push(a_source("s2")));
        let deployed = commit_to(&data, git::MAIN_BRANCH, &[("destinations.json", b"[]".to_vec())]);
        commit_to(&data, "other", &[("sources.json", b"[]".to_vec())]);
        let elsewhere = git::branch_tip(&repo, "other").unwrap();

        let tip = config_at(&data, "work", None);
        assert_eq!(tip.commit, git::branch_tip(&repo, "work"));
        assert_eq!(tip.body.config.unwrap().sources.len(), 2);

        let base = config_at(&data, "work", Some(&left_at));
        assert_eq!(base.commit.as_deref(), Some(left_at.as_str()));
        assert_eq!(base.body.config.unwrap().sources.len(), 1, "as the branch started");
        assert!(
            config_at(&data, "work", Some(&deployed)).config.is_some(),
            "main's history is readable"
        );

        for refused in [elsewhere.as_str(), &left_at[..12], "main"] {
            let read = config_at(&data, "work", Some(refused));
            assert!(
                !read.succeeded() && read.config.is_none() && read.commit.is_none(),
                "{refused}"
            );
        }
        std::fs::remove_dir_all(&data).ok();
    }

    // -- Scope ----------------------------------------------------------

    /// `Scope::parse` refuses exactly what `git::group_subtree` would -- the safety check has one place to live,
    /// and a scope cannot get around it by a different path in.
    #[test]
    fn an_unsafe_group_id_is_refused_as_a_scope() {
        for unsafe_id in ["../../escaped", "a/b", ".hidden"] {
            let err = Scope::parse(unsafe_id).expect_err("not a safe path component");
            assert!(err.message().contains(unsafe_id), "names what was wrong: {err}");
        }
    }
}
