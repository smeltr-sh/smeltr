//! Worker groups: what the GUI's group picker and Cluster page read, and where a group starts.
//!
//! A group exists on a commit exactly when `groups/<id>/` holds a file there (design doc, Control plane). Creating
//! one is a config write like any other -- `POST /api/worker-groups` commits the manifest onto a branch (`config_api`)
//! and does not open a change; an admin configures the group, then submits one change carrying it. `GET
//! /api/worker-groups` answers from the repository (`leader::git`, `leader::tree`). It lists every group the
//! caller's own branch holds now. Each one is marked deployed when `main` holds it too. It also names `shared`'s
//! hue. `shared` is not a group.
//!
//! A group's color lives in `Store`, not its manifest: `PUT /api/worker-groups/:id/hue` sets it directly, for any
//! id, unbranched and undeployed, alongside the git-backed identity and name every other route here manages.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use axum::Json;
use axum::extract::{Extension, Query};
use axum::http::HeaderMap;

use smeltr::config_file::{self, FileConfig, FileSet};
use smeltr::leader::access::{Access, Level};
use smeltr::leader::store::Actor;
use smeltr::leader::{AppState, blocking, git, tree};
use smeltr::startup_config::leader_data_dir;

use smeltr::leader::envelope::{Empty, Envelope};

use crate::config_api::{self, Save, UNNAMED_BRANCH, Writing, blocking_save};

#[derive(serde::Deserialize, Default)]
pub struct WorkerGroupQuery {
    branch: Option<String>,
}

impl WorkerGroupQuery {
    fn name(&self) -> &str {
        self.branch.as_deref().unwrap_or(UNNAMED_BRANCH)
    }
}

fn repo_at(data_dir: &Path) -> Result<PathBuf, String> {
    let repo = git::repo_path(data_dir);
    git::init_bare_repo(&repo)?;
    git::ensure_main_commit(&repo)?;
    Ok(repo)
}

// -- GET /api/worker-groups -------------------------------------------------

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerGroupOut {
    pub id: String,
    pub name: String,
    /// `true` once `main` holds this group's subtree -- what makes it real and live (design doc, Control plane).
    /// `false` is a
    /// group created on the caller's branch and not yet deployed.
    pub deployed: bool,
    /// The hue the group is drawn in, in degrees -- the store's answer (`Store::group_hues`), absent only for an id
    /// nothing has ever colored.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hue: Option<u16>,
}

#[derive(Default, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerGroups {
    groups: Vec<WorkerGroupOut>,
    /// `shared`'s hue: not a group, so it rides beside `groups` rather than pretending to be one of them.
    #[serde(skip_serializing_if = "Option::is_none")]
    shared_hue: Option<u16>,
    /// The commit this list read the branch at. A write that follows -- delete, in particular, since it has
    /// nothing to compare against otherwise -- bases itself on it, the same compare-and-swap every other config
    /// write already gets from its own read.
    #[serde(skip_serializing_if = "Option::is_none")]
    commit: Option<String>,
}

/// `id`'s manifest at `commit`. `None` for a group `commit` does not hold, an id it refuses, or a manifest that
/// does not parse -- every caller falls back the way an absent field does, rather than failing the listing.
fn group_manifest_at(repo: &Path, commit: &str, id: &str) -> Option<FileConfig> {
    let prefix = git::group_subtree(id).ok()?;
    let mut files = tree::TreeFiles::new(repo, commit, prefix);
    let bytes = files.read("system.json").ok().flatten()?;
    serde_json::from_slice::<FileConfig>(&bytes).ok()
}

/// The order a fleet's groups take their hues in, thirty degrees apart round the whole wheel: the three most
/// unlike each other first, then the three most unlike those, then the six between them. An admin editing a
/// group may still choose any hue.
const DEFAULT_HUES: [u16; 12] = [240, 330, 180, 120, 90, 30, 270, 300, 210, 60, 0, 150];

/// The first default no group already holds, so a new group is distinct from every existing one for as long
/// as there are defaults. Past twelve it starts round again, counting `taken` with its repeats so each new
/// group moves on: two groups sharing a hue is a nuisance, and refusing to create the thirteenth would be worse.
fn next_hue(taken: &[u16]) -> u16 {
    DEFAULT_HUES
        .into_iter()
        .find(|hue| !taken.contains(hue))
        .unwrap_or(DEFAULT_HUES[taken.len() % DEFAULT_HUES.len()])
}

/// The commit `branch` reads from: its own tip, or `main`'s head when the branch does not exist yet. Shared by
/// `list_groups_in` and the commit the listing hands back for a later write to base itself on.
fn resolve_branch_commit(repo: &Path, branch: &str) -> Result<String, String> {
    if git::branch_exists(repo, branch)? {
        git::branch_tip(repo, branch).ok_or_else(|| format!("there is no branch {branch:?} to read"))
    } else {
        git::main_head(repo).ok_or_else(|| format!("{} has no commit", git::MAIN_BRANCH))
    }
}

/// `list_branch_groups_at` for a branch by name, resolving its own commits -- what every test here wants; the
/// real endpoint resolves commits itself so it can also return the exact one it read from.
#[cfg(test)]
fn list_groups_in(
    data_dir: &Path,
    branch: &str,
    store: &dyn smeltr::leader::store::Store,
) -> Result<Vec<WorkerGroupOut>, String> {
    let repo = repo_at(data_dir)?;
    let deployed = git::deployed(&repo).ok_or("nothing has been deployed")?;
    let branch_commit = resolve_branch_commit(&repo, branch)?;
    list_branch_groups_at(&repo, &deployed, &branch_commit, store)
}

/// The branch listing for commits already resolved, so a caller that also hands the branch commit back to its
/// client reads it once and reuses the same value both places. Reading it twice would let a write land between
/// the two reads: the response would carry a commit newer than the rows it describes, and a client basing a
/// write on it could silently miss a change the second read already saw.
fn list_branch_groups_at(
    repo: &Path,
    deployed_commit: &str,
    branch_commit: &str,
    store: &dyn smeltr::leader::store::Store,
) -> Result<Vec<WorkerGroupOut>, String> {
    let hues = store.group_hues().map_err(|e| e.to_string())?;

    let deployed_ids: BTreeSet<String> = tree::groups_at(repo, deployed_commit)
        .map_err(|e| e.to_string())?
        .into_iter()
        .collect();
    // The branch's own tree. Nothing outside it. Union in `deployed_ids` and a group this branch deleted would keep
    // showing, through the fleet's still-live, unrelated copy. It would look just like an id the branch never touched.
    let branch_ids: BTreeSet<String> = tree::groups_at(repo, branch_commit)
        .map_err(|e| e.to_string())?
        .into_iter()
        .collect();

    Ok(branch_ids
        .into_iter()
        .map(|id| {
            let deployed = deployed_ids.contains(&id);
            let manifest = group_manifest_at(repo, branch_commit, &id);
            let name = manifest.and_then(|m| m.name).unwrap_or_else(|| id.clone());
            WorkerGroupOut {
                hue: hues.get(&id).copied(),
                id,
                name,
                deployed,
            }
        })
        .collect())
}

pub async fn list_worker_groups(
    axum::extract::State(state): axum::extract::State<AppState>,
    Extension(access): Extension<Access>,
    Query(q): Query<WorkerGroupQuery>,
) -> Json<Envelope<WorkerGroups>> {
    let branch = q.name().to_string();
    let store = state.store.clone();
    let listed = blocking(move || {
        let repo = repo_at(&leader_data_dir())?;
        let deployed = git::deployed(&repo).ok_or("nothing has been deployed")?;
        let branch_commit = resolve_branch_commit(&repo, &branch)?;
        let groups = list_branch_groups_at(&repo, &deployed, &branch_commit, store.as_ref())?;
        // Best-effort: `shared`'s hue is a courtesy on top of the listing, and a store hiccup here should not
        // fail a request the groups half of which already succeeded.
        let shared_hue = store
            .group_hues()
            .ok()
            .and_then(|m| m.get(config_file::SHARED_DIR).copied());
        Ok::<_, String>((groups, branch_commit, shared_hue))
    })
    .await;
    match listed {
        Ok((groups, commit, shared_hue)) => Json(Envelope::ok(WorkerGroups {
            groups: held(groups, &access),
            shared_hue,
            commit: Some(commit),
        })),
        Err(e) => Json(Envelope::failed_one(e)),
    }
}

/// The groups `access` holds, so a group the person cannot see is not named to them either.
fn held(groups: Vec<WorkerGroupOut>, access: &Access) -> Vec<WorkerGroupOut> {
    groups
        .into_iter()
        .filter(|g| access.allows(&g.id, Level::Viewer))
        .collect()
}

/// What the binary's in-process Cluster page shows: every group the fleet runs, live rather than pending.
pub async fn worker_groups_snapshot(state: &AppState) -> Result<Vec<WorkerGroupOut>, String> {
    let data_dir = state.data_dir.clone();
    let store = state.store.clone();
    blocking(move || {
        let repo = git::repo_path(&data_dir);
        if !git::repo_ready(&repo) {
            return Ok(Vec::new());
        }
        let Some(deployed) = git::deployed(&repo) else {
            return Ok(Vec::new());
        };
        list_groups_at(&repo, &deployed, store.as_ref())
    })
    .await
}

fn list_groups_at(
    repo: &Path,
    commit: &str,
    store: &dyn smeltr::leader::store::Store,
) -> Result<Vec<WorkerGroupOut>, String> {
    let hues = store.group_hues().map_err(|e| e.to_string())?;
    tree::groups_at(repo, commit).map_err(|e| e.to_string()).map(|ids| {
        ids.into_iter()
            .map(|id| {
                let manifest = group_manifest_at(repo, commit, &id);
                let name = manifest.and_then(|m| m.name).unwrap_or_else(|| id.clone());
                WorkerGroupOut {
                    hue: hues.get(&id).copied(),
                    id,
                    name,
                    deployed: true,
                }
            })
            .collect()
    })
}

// -- POST /api/worker-groups ------------------------------------------------

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateWorkerGroupRequest {
    id: String,
    name: String,
    /// The hue to draw the group in, in degrees. Absent takes the first default no group holds. Any hue is accepted --
    /// an admin choosing one has something specific in mind, and picking again is the whole cost of getting it
    /// wrong.
    #[serde(default)]
    hue: Option<u16>,
}

/// What a group's `system.json` holds: the bundle format's version, and a display name when one was given. A group
/// seeded on a fresh install gives none -- `group_manifest_at` already answers with the id when a manifest names
/// nothing, and an invented name would be a name nobody chose.
pub fn group_manifest(name: Option<String>) -> Vec<u8> {
    config_file::manifest_bytes(&new_manifest(name))
}

fn new_manifest(name: Option<String>) -> FileConfig {
    FileConfig {
        version: config_file::CURRENT_VERSION,
        sources: vec![],
        destinations: vec![],
        pipelines: vec![],
        routes: vec![],
        lookup_tables: vec![],
        grok_patterns: vec![],
        on_exception: None,
        name,
        schemas: vec![],
    }
}

/// Whether `id` is a group at the commit `writing` is based on: whether its subtree there holds a file. The listing
/// reads the same function, so what lists and what a write finds are one rule.
fn group_held(writing: &Writing, id: &str) -> Result<bool, String> {
    let config_file::BundleAt::Commit { repo, commit, .. } = writing.at() else {
        return Err("a group is read from a commit".to_string());
    };
    let ids = tree::groups_at(&repo, &commit).map_err(|e| e.to_string())?;
    Ok(ids.iter().any(|g| g == id))
}

/// The group a fresh install starts with, written into `main`'s first commit. Answers whether this call was the one
/// that seeded it.
///
/// A leader that has just started otherwise reports itself misconfigured: its own worker heartbeats for a group, and
/// `main` holds no configuration for any. Seeding the group the worker will name closes that without inventing a
/// second way for a group to exist -- it is still a file under `groups/<id>/` on `main`.
pub fn seed_default_group(
    data_dir: &Path,
    group_id: &str,
    store: &dyn smeltr::leader::store::Store,
) -> Result<bool, String> {
    let subtree = git::group_subtree(group_id).map_err(|e| e.to_string())?;
    let edit = git::FileEdit {
        path: format!("{subtree}/system.json"),
        content: Some(group_manifest(None)),
    };
    let seeded = git::seed_first_commit(&git::repo_path(data_dir), &[edit], &format!("worker group {group_id}"))
        .map_err(|e| e.to_string())?;
    // The manifest commit is what makes the group real; a color that fails to follow it is a group with no hue
    // yet, not a seed that did not happen -- retrying the whole seed on the next start would find `main` already
    // holds the manifest and stop here again.
    if seeded && let Err(e) = store.set_group_hue(group_id, DEFAULT_HUES[0]) {
        tracing::warn!(
            target: "leader.seeded_group_color_unrecorded",
            subject = %group_id,
            error = %e,
            "a worker group was seeded without a color"
        );
    }
    Ok(seeded)
}

/// Gives `shared` a hue the first time nothing else has, so the Build page's scope chip is never left
/// bare (`ScopePicker.svelte`). `shared` is not a group and holds no manifest, so seeding it does not wait
/// on `main`'s first commit the way a group's does -- it only needs the store, and runs every start,
/// stopping the moment a hue is already there. Answers whether this call was the one that set it.
pub fn seed_shared_hue(store: &dyn smeltr::leader::store::Store) -> Result<bool, String> {
    let hues = store.group_hues().map_err(|e| e.to_string())?;
    if hues.contains_key(config_file::SHARED_DIR) {
        return Ok(false);
    }
    let taken: Vec<u16> = hues.values().copied().collect();
    store
        .set_group_hue(config_file::SHARED_DIR, next_hue(&taken))
        .map_err(|e| e.to_string())?;
    Ok(true)
}

/// Writes `groups/<id>/system.json` on `branch` -- the group's manifest, carrying its display name. Refused if the
/// group already exists: an id is taken the moment its subtree holds a file, on `main` or on this branch, same as
/// any other create-once resource.
fn create_group_in(
    data_dir: &Path,
    branch: &str,
    base: Option<&str>,
    req: CreateWorkerGroupRequest,
    store: &dyn smeltr::leader::store::Store,
    author: &Actor,
) -> Envelope<Save> {
    // Read before the write: a hue is picked against the groups that already exist, so a new one is distinct from
    // every one of them rather than distinct from a guess.
    let taken: Vec<u16> = store
        .group_hues()
        .map(|m| m.values().copied().collect())
        .unwrap_or_default();

    if !config_file::is_safe_group_name(&req.name) {
        return Envelope::failed_one(config_file::unsafe_group_name(&req.name));
    }

    let writing = match Writing::open_in(data_dir, branch, base, &req.id) {
        Ok(w) => w,
        Err(e) => return Save::refused(e),
    };
    match group_held(&writing, &req.id) {
        Ok(false) => {}
        Ok(true) => return Envelope::failed_one(format!("worker group {:?} already exists", req.id)),
        Err(e) => return Envelope::failed_one(e),
    }

    let hue = req.hue.unwrap_or_else(|| next_hue(&taken));
    let edit = git::FileEdit {
        path: "system.json".to_string(),
        content: Some(group_manifest(Some(req.name))),
    };
    match writing.commit(vec![edit], &format!("create worker group {}", req.id), author) {
        Ok(commit) => {
            // Best-effort, same as the seeded group's own color: the group exists once the manifest commits, and
            // a store hiccup here is a color to pick again, not a failed create.
            if let Err(e) = store.set_group_hue(&req.id, hue) {
                tracing::warn!(
                    target: "leader.created_group_color_unrecorded",
                    subject = %req.id,
                    error = %e,
                    "a worker group has no color recorded"
                );
            }
            Save::done(commit)
        }
        Err(e) => Save::refused(e),
    }
}

#[derive(serde::Deserialize)]
pub struct UpdateWorkerGroupRequest {
    name: String,
}

/// Rewrites an existing group's manifest on `branch` -- its display name.
///
/// Refused when there is no group to edit: creating one is `create_group_in`'s job, and an id that is taken and an
/// id that is free need different answers. A group whose subtree holds no manifest gets one. The manifest is read and amended rather than rebuilt, so a
/// field this form does not carry -- `unprocessable` -- survives an edit it had nothing to do with.
fn update_group_in(
    data_dir: &Path,
    branch: &str,
    base: Option<&str>,
    id: &str,
    req: UpdateWorkerGroupRequest,
    author: &Actor,
) -> Envelope<Save> {
    if !config_file::is_safe_group_name(&req.name) {
        return Envelope::failed_one(config_file::unsafe_group_name(&req.name));
    }

    let writing = match Writing::open_in(data_dir, branch, base, id) {
        Ok(w) => w,
        Err(e) => return Save::refused(e),
    };
    let existing = match writing.files().read("system.json") {
        Ok(Some(bytes)) => match serde_json::from_slice::<FileConfig>(&bytes) {
            Ok(file) => file,
            Err(e) => {
                return Envelope::failed_one(format!("worker group {id:?} has an unreadable manifest: {e}"));
            }
        },
        Ok(None) if matches!(group_held(&writing, id), Ok(true)) => new_manifest(None),
        _ => return Envelope::failed_one(format!("there is no worker group {id:?} to edit")),
    };

    let amended = FileConfig {
        name: Some(req.name),
        ..existing
    };
    let edit = git::FileEdit {
        path: "system.json".to_string(),
        content: Some(config_file::manifest_bytes(&amended)),
    };
    match writing.commit(vec![edit], &format!("worker group {id}"), author) {
        Ok(commit) => Save::done(commit),
        Err(e) => Save::refused(e),
    }
}

// -- DELETE /api/worker-groups/:id ------------------------------------------

/// Removes `id`'s whole subtree on `branch`, in one commit -- every file it holds, not each one named. A config
/// write like create and rename: it goes live only once a change carrying it deploys, and reverting the commit
/// brings the group back. It does not touch workers. Once it deploys, one still heartbeating under `id` has nothing
/// to run, so it stops every pipeline and idles under a `groupNotDeployed` warning until someone moves it to another
/// group or shuts it down (docs/status.md, "Deleting a group does not look at its workers").
fn delete_group_in(data_dir: &Path, branch: &str, base: Option<&str>, id: &str, author: &Actor) -> Envelope<Save> {
    // `Writing::open_in` accepts any scope a config write may target, `shared` included -- it is not itself a
    // group, so `id` is checked against the narrower rule before anything opens, the same rule a group's own
    // subtree path is built from.
    if let Err(e) = config_file::group_subtree(id) {
        return Envelope::failed_one(e);
    }
    let writing = match Writing::open_in(data_dir, branch, base, id) {
        Ok(w) => w,
        Err(e) => return Save::refused(e),
    };
    match group_held(&writing, id) {
        Ok(true) => {}
        Ok(false) => return Envelope::failed_one(format!("there is no worker group {id:?} to delete")),
        Err(e) => return Envelope::failed_one(e),
    }

    match writing.remove_scope(&format!("delete worker group {id}"), author) {
        Ok(commit) => Save::done(commit),
        Err(e) => Save::refused(e),
    }
}

pub async fn delete_worker_group(
    Extension(actor): Extension<Actor>,
    axum::extract::Path(id): axum::extract::Path<String>,
    Query(q): Query<WorkerGroupQuery>,
    headers: HeaderMap,
) -> Json<Envelope<Save>> {
    let (branch, base) = (q.name().to_string(), config_api::base_commit(&headers));
    blocking_save(move || delete_group_in(&leader_data_dir(), &branch, base.as_deref(), &id, &actor)).await
}

// -- PUT /api/worker-groups/:id/hue -----------------------------------------

#[derive(serde::Deserialize)]
pub struct SetGroupHueRequest {
    hue: u16,
}

/// Sets `id`'s color directly in the store: immediate, unbranched, and never gated on `id` naming a real group --
/// `shared`'s hue is set the same way, and a color set for an id that never becomes a group, or one later
/// abandoned on an unmerged branch, is an orphan row that costs nothing.
pub async fn set_group_hue(
    axum::extract::State(state): axum::extract::State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
    Json(req): Json<SetGroupHueRequest>,
) -> Json<Envelope> {
    let store = state.store.clone();
    let set = blocking(move || store.set_group_hue(&id, req.hue)).await;
    Json(set.map(|()| Empty).into())
}

pub async fn update_worker_group(
    Extension(actor): Extension<Actor>,
    axum::extract::Path(id): axum::extract::Path<String>,
    Query(q): Query<WorkerGroupQuery>,
    headers: HeaderMap,
    Json(req): Json<UpdateWorkerGroupRequest>,
) -> Json<Envelope<Save>> {
    let (branch, base) = (q.name().to_string(), config_api::base_commit(&headers));
    blocking_save(move || update_group_in(&leader_data_dir(), &branch, base.as_deref(), &id, req, &actor)).await
}

pub async fn create_worker_group(
    axum::extract::State(state): axum::extract::State<AppState>,
    Extension(actor): Extension<Actor>,
    Query(q): Query<WorkerGroupQuery>,
    headers: HeaderMap,
    Json(req): Json<CreateWorkerGroupRequest>,
) -> Json<Envelope<Save>> {
    let (branch, base) = (q.name().to_string(), config_api::base_commit(&headers));
    let store = state.store.clone();
    blocking_save(move || {
        create_group_in(
            &leader_data_dir(),
            &branch,
            base.as_deref(),
            req,
            store.as_ref(),
            &actor,
        )
    })
    .await
}

#[cfg(test)]
mod tests {
    use smeltr::leader::sqlite_store::SqliteStore;
    use smeltr::leader::store::Store;

    use super::*;

    #[test]
    fn a_person_is_listed_only_the_groups_they_hold() {
        let group = |id: &str| WorkerGroupOut {
            id: id.to_string(),
            name: id.to_string(),
            deployed: true,
            hue: None,
        };
        let all = || vec![group("payments"), group("search")];
        let payments = Access::Areas([("payments".to_string(), Level::Viewer)].into());
        let ids = |groups: Vec<WorkerGroupOut>| groups.into_iter().map(|g| g.id).collect::<Vec<_>>();
        assert_eq!(ids(held(all(), &payments)), ["payments"]);
        assert_eq!(ids(held(all(), &Access::Administrator)), ["payments", "search"]);
    }

    fn temp_dir(label: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "smeltr-worker-groups-api-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// A store of its own per test, exactly as `sqlite_store`'s own tests build one.
    fn test_store(label: &str) -> SqliteStore {
        let key = std::env::temp_dir().join(format!(
            "smeltr-worker-groups-api-{label}-{}-{}.key",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        SqliteStore::open(Path::new(":memory:"), &key).unwrap()
    }

    #[test]
    fn a_fresh_install_starts_with_the_group_a_worker_will_name() {
        let data = temp_dir("seed-fresh");
        let store = test_store("seed-fresh");
        assert!(seed_default_group(&data, smeltr::startup_config::DEFAULT_WORKER_GROUP, &store).unwrap());

        let repo = git::repo_path(&data);
        let head = git::main_head(&repo).expect("main has a commit");
        assert_eq!(tree::groups_at(&repo, &head).unwrap(), vec!["default".to_string()]);
    }

    #[test]
    fn seeding_twice_seeds_once() {
        let data = temp_dir("seed-twice");
        let store = test_store("seed-twice");
        assert!(seed_default_group(&data, "default", &store).unwrap());
        let repo = git::repo_path(&data);
        let first = git::main_head(&repo).unwrap();

        assert!(!seed_default_group(&data, "default", &store).unwrap());
        assert_eq!(git::main_head(&repo).unwrap(), first, "main did not move");
    }

    /// A repository with any history is somebody's, and what their fleet runs is theirs to move.
    #[test]
    fn a_repository_that_already_has_history_is_not_seeded() {
        let data = temp_dir("seed-existing");
        let store = test_store("seed-existing");
        deploy_to_main(&data, "west", "West");
        let repo = git::repo_path(&data);
        let before = git::main_head(&repo).unwrap();

        assert!(!seed_default_group(&data, "default", &store).unwrap());
        assert_eq!(git::main_head(&repo).unwrap(), before, "main did not move");
        assert_eq!(tree::groups_at(&repo, &before).unwrap(), vec!["west".to_string()]);
    }

    /// The group id reaches a path, so the same allowlist every other way in is held to applies here.
    #[test]
    fn an_unsafe_group_id_is_refused_rather_than_seeded() {
        let data = temp_dir("seed-unsafe");
        let store = test_store("seed-unsafe");
        assert!(seed_default_group(&data, "../escape", &store).is_err());
        assert!(git::main_head(&git::repo_path(&data)).is_none(), "nothing was written");
    }

    fn request(id: &str, name: &str) -> CreateWorkerGroupRequest {
        CreateWorkerGroupRequest {
            id: id.to_string(),
            name: name.to_string(),
            hue: None,
        }
    }

    fn update(name: &str) -> UpdateWorkerGroupRequest {
        UpdateWorkerGroupRequest { name: name.to_string() }
    }

    /// A group created with the color an admin picked rather than the next default.
    fn request_hued(id: &str, name: &str, hue: u16) -> CreateWorkerGroupRequest {
        CreateWorkerGroupRequest {
            id: id.to_string(),
            name: name.to_string(),
            hue: Some(hue),
        }
    }

    /// The hue on `id` as the listing reports it on `branch`.
    fn hue_of(data: &Path, branch: &str, id: &str, store: &dyn Store) -> Option<u16> {
        list_groups_in(data, branch, store)
            .unwrap()
            .into_iter()
            .find(|g| g.id == id)
            .and_then(|g| g.hue)
    }

    #[test]
    fn a_new_group_is_given_the_first_default_hue() {
        let data = temp_dir("hue-first");
        let store = test_store("hue-first");
        create_group_in(&data, "work", None, request("west", "West"), &store, &Actor::system());
        assert_eq!(hue_of(&data, "work", "west", &store), Some(DEFAULT_HUES[0]));
    }

    /// The whole point of the color: two groups must not look the same.
    #[test]
    fn every_group_a_fleet_creates_gets_its_own_hue() {
        let data = temp_dir("hue-distinct");
        let store = test_store("hue-distinct");
        let ids: Vec<String> = (0..DEFAULT_HUES.len()).map(|i| format!("group-{i}")).collect();
        for id in &ids {
            create_group_in(&data, "work", None, request(id, id), &store, &Actor::system());
        }
        let hues: BTreeSet<u16> = ids.iter().filter_map(|id| hue_of(&data, "work", id, &store)).collect();
        assert_eq!(
            hues.len(),
            DEFAULT_HUES.len(),
            "the defaults were used up without repeating"
        );
    }

    /// Past the twelfth group the order starts round again and keeps going, rather than settling on one hue.
    #[test]
    fn groups_past_the_defaults_go_round_the_order_again() {
        let mut taken = DEFAULT_HUES.to_vec();
        taken.push(next_hue(&taken));
        assert_eq!(next_hue(&taken), DEFAULT_HUES[1]);
    }

    /// An admin changing a color has something specific in mind, so any hue is kept -- including one the
    /// leader would never assign.
    #[test]
    fn a_hue_an_admin_chose_is_kept_whatever_it_is() {
        let data = temp_dir("hue-chosen");
        let store = test_store("hue-chosen");
        create_group_in(
            &data,
            "work",
            None,
            request_hued("west", "West", 42),
            &store,
            &Actor::system(),
        );
        assert_eq!(hue_of(&data, "work", "west", &store), Some(42));
    }

    /// A color changes straight in the store: no branch, no review, no deploy -- and the listing reports it the
    /// moment it is set.
    #[test]
    fn a_groups_color_is_changed_directly_and_immediately() {
        let data = temp_dir("hue-change");
        let store = test_store("hue-change");
        deploy_to_main(&data, "west", "West");

        store.set_group_hue("west", 120).unwrap();
        assert_eq!(hue_of(&data, "work", "west", &store), Some(120));

        store.set_group_hue("west", 300).unwrap();
        assert_eq!(
            hue_of(&data, "work", "west", &store),
            Some(300),
            "replaced, not added to"
        );
    }

    #[test]
    fn shared_gets_a_hue_the_first_time_nothing_else_has() {
        let store = test_store("shared-hue-fresh");
        assert!(seed_shared_hue(&store).unwrap());
        assert_eq!(
            store.group_hues().unwrap().get(config_file::SHARED_DIR).copied(),
            Some(DEFAULT_HUES[0]),
        );
    }

    #[test]
    fn seeding_shareds_hue_twice_seeds_once() {
        let store = test_store("shared-hue-twice");
        assert!(seed_shared_hue(&store).unwrap());
        let first = store.group_hues().unwrap().get(config_file::SHARED_DIR).copied();

        assert!(!seed_shared_hue(&store).unwrap());
        assert_eq!(
            store.group_hues().unwrap().get(config_file::SHARED_DIR).copied(),
            first,
            "the color did not move"
        );
    }

    /// An admin's own choice for `shared` is not a starting point to seed past.
    #[test]
    fn shared_is_not_reseeded_once_an_admin_has_colored_it() {
        let store = test_store("shared-hue-chosen");
        store.set_group_hue(config_file::SHARED_DIR, 42).unwrap();
        assert!(!seed_shared_hue(&store).unwrap());
        assert_eq!(store.group_hues().unwrap().get(config_file::SHARED_DIR), Some(&42));
    }

    /// The ordinary case: one group seeded on a fresh install. `shared` takes the next default, the
    /// one most unlike the group's.
    #[test]
    fn shared_takes_the_next_default_after_the_seeded_group() {
        let data = temp_dir("shared-hue-spaced");
        let store = test_store("shared-hue-spaced");
        assert!(seed_default_group(&data, "default", &store).unwrap());

        assert!(seed_shared_hue(&store).unwrap());
        assert_eq!(
            store.group_hues().unwrap().get(config_file::SHARED_DIR).copied(),
            Some(DEFAULT_HUES[1]),
        );
    }

    /// A rename carries only the field the form has. `unprocessable` has nothing to do with a name and survives an
    /// edit that was never about it.
    #[test]
    fn renaming_a_group_leaves_the_rest_of_the_manifest_alone() {
        let data = temp_dir("rename-keeps");
        let repo = repo_at(&data).unwrap();
        let parent = git::main_head(&repo).unwrap();
        let file = FileConfig {
            version: config_file::CURRENT_VERSION,
            sources: vec![],
            destinations: vec![],
            pipelines: vec![],
            routes: vec![],
            lookup_tables: vec![],
            grok_patterns: vec![],
            on_exception: Some(config_file::OnExceptionDto {
                categories: [("math".to_string(), config_file::AnswerDto::Drop)].into(),
                ..Default::default()
            }),
            name: Some("West".to_string()),
            schemas: vec![],
        };
        let edit = git::FileEdit {
            path: "groups/west/system.json".to_string(),
            content: Some(config_file::manifest_bytes(&file)),
        };
        git::commit_edits(
            &repo,
            git::MAIN_BRANCH,
            &parent,
            Some(&parent),
            &[edit],
            "west",
            &Actor::system(),
        )
        .unwrap();

        update_group_in(&data, "work", None, "west", update("West Renamed"), &Actor::system());

        let head = git::branch_tip(&repo, "work").unwrap();
        let after = group_manifest_at(&repo, &head, "west").expect("the manifest is still readable");
        assert_eq!(after.name.as_deref(), Some("West Renamed"), "the name changed");
        assert_eq!(
            after.on_exception,
            Some(config_file::OnExceptionDto {
                categories: [("math".to_string(), config_file::AnswerDto::Drop)].into(),
                ..Default::default()
            }),
            "the exceptions did not"
        );
    }

    #[test]
    fn the_seeded_group_takes_the_first_default() {
        let data = temp_dir("hue-seed");
        let store = test_store("hue-seed");
        seed_default_group(&data, "default", &store).unwrap();
        assert_eq!(hue_of(&data, "work", "default", &store), Some(DEFAULT_HUES[0]));
    }

    /// A group deployed straight to `main`, the way an already-approved change lands one -- what a test needs
    /// without going through a branch and a change.
    fn deploy_to_main(data_dir: &Path, id: &str, name: &str) {
        let repo = repo_at(data_dir).unwrap();
        let parent = git::main_head(&repo).unwrap();
        let file = FileConfig {
            version: config_file::CURRENT_VERSION,
            sources: vec![],
            destinations: vec![],
            pipelines: vec![],
            routes: vec![],
            lookup_tables: vec![],
            grok_patterns: vec![],
            on_exception: None,
            name: Some(name.to_string()),
            schemas: vec![],
        };
        let edit = git::FileEdit {
            path: format!("groups/{id}/system.json"),
            content: Some(config_file::manifest_bytes(&file)),
        };
        git::commit_edits(
            &repo,
            git::MAIN_BRANCH,
            &parent,
            Some(&parent),
            &[edit],
            "deploy",
            &Actor::system(),
        )
        .unwrap();
        git::deploy_commit(data_dir, &git::main_head(&repo).unwrap(), &Actor::system()).unwrap();
    }

    #[test]
    fn creating_a_group_writes_its_manifest_and_it_lists_as_pending() {
        let data = temp_dir("create-lists-pending");
        let store = test_store("create-lists-pending");
        let response = create_group_in(&data, "work", None, request("west", "West"), &store, &Actor::system());
        assert!(response.ok, "{:?}", response.errors);

        let groups = list_groups_in(&data, "work", &store).unwrap();
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].id, "west");
        assert_eq!(groups[0].name, "West");
        assert!(!groups[0].deployed, "created on a branch, not on main");
    }

    #[test]
    fn a_second_create_with_the_same_id_is_refused() {
        let data = temp_dir("create-twice");
        let store = test_store("create-twice");
        let first = create_group_in(&data, "work", None, request("west", "West"), &store, &Actor::system());
        assert!(first.ok);

        let second = create_group_in(
            &data,
            "work",
            None,
            request("west", "West Again"),
            &store,
            &Actor::system(),
        );
        assert!(!second.ok);
        assert!(
            format!("{:?}", second.errors[0]).contains("west"),
            "{:?}",
            second.errors
        );
    }

    #[test]
    fn an_unsafe_group_id_is_refused_at_creation() {
        let data = temp_dir("create-unsafe-id");
        let store = test_store("create-unsafe-id");
        for id in ["../../ESCAPED", "a/b", ".hidden"] {
            let response = create_group_in(&data, "work", None, request(id, "Bad"), &store, &Actor::system());
            assert!(!response.ok, "group id {id:?} must be refused");
        }
    }

    /// A group is drawn by its name, so a blank one would draw as its id -- which a reader would take for a name
    /// someone chose. Both ways in are held to it; nothing else about a name is checked.
    #[test]
    fn a_blank_group_name_is_refused_at_creation_and_at_rename() {
        let data = temp_dir("blank-group-name");
        let store = test_store("blank-group-name");

        for blank in ["", "   "] {
            let response = create_group_in(&data, "work", None, request("west", blank), &store, &Actor::system());
            assert!(!response.ok, "name {blank:?} must be refused");
        }

        assert!(create_group_in(&data, "work", None, request("west", "West"), &store, &Actor::system()).ok);
        assert!(
            !update_group_in(&data, "work", None, "west", update("  "), &Actor::system()).ok,
            "a rename to blank is refused"
        );

        let repo = git::repo_path(&data);
        let head = git::branch_tip(&repo, "work").unwrap();
        let after = group_manifest_at(&repo, &head, "west").expect("readable");
        assert_eq!(
            after.name.as_deref(),
            Some("West"),
            "the name it had survived the refusal"
        );
    }

    #[test]
    fn a_group_main_already_holds_lists_as_deployed_even_unseen_on_the_branch() {
        let data = temp_dir("deployed-group-lists");
        let store = test_store("deployed-group-lists");
        deploy_to_main(&data, "east", "East");

        let groups = list_groups_in(&data, "work", &store).unwrap();
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].id, "east");
        assert!(groups[0].deployed);
    }

    #[test]
    fn a_deployed_group_being_renamed_on_the_branch_lists_its_pending_name() {
        let data = temp_dir("renamed-on-branch");
        let store = test_store("renamed-on-branch");
        deploy_to_main(&data, "east", "East");
        let renamed = create_group_in(
            &data,
            "work",
            None,
            request("east", "East Again"),
            &store,
            &Actor::system(),
        );
        assert!(
            !renamed.ok,
            "the id is taken -- renaming edits the manifest instead of recreating it"
        );

        // What the manifest actually looks like after an edit: `system.json` rewritten with a new name, the way a
        // real rename would leave it, rather than a second create the id already refuses.
        let repo = repo_at(&data).unwrap();
        let parent = git::branch_tip(&repo, "east-renamed")
            .or_else(|| git::main_head(&repo))
            .unwrap();
        let file = FileConfig {
            version: config_file::CURRENT_VERSION,
            sources: vec![],
            destinations: vec![],
            pipelines: vec![],
            routes: vec![],
            lookup_tables: vec![],
            grok_patterns: vec![],
            on_exception: None,
            name: Some("East, Renamed".to_string()),
            schemas: vec![],
        };
        let edit = git::FileEdit {
            path: "groups/east/system.json".to_string(),
            content: Some(config_file::manifest_bytes(&file)),
        };
        git::commit_edits(
            &repo,
            "east-renamed",
            &parent,
            None,
            &[edit],
            "rename east",
            &Actor::system(),
        )
        .unwrap();

        let groups = list_groups_in(&data, "east-renamed", &store).unwrap();
        assert_eq!(groups.len(), 1);
        assert_eq!(
            groups[0].name, "East, Renamed",
            "the branch's own manifest wins over main's"
        );
        assert!(
            groups[0].deployed,
            "main still holds east's subtree, under the old name"
        );
    }

    #[test]
    fn deleting_a_group_created_on_the_branch_removes_it_from_the_branchs_listing() {
        let data = temp_dir("delete-undeployed");
        let store = test_store("delete-undeployed");
        assert!(create_group_in(&data, "work", None, request("west", "West"), &store, &Actor::system()).ok);

        let deleted = delete_group_in(&data, "work", None, "west", &Actor::system());
        assert!(deleted.ok, "{:?}", deleted.errors);

        assert!(list_groups_in(&data, "work", &store).unwrap().is_empty());
    }

    #[test]
    fn deleting_a_group_removes_every_file_its_subtree_holds() {
        let data = temp_dir("delete-full-subtree");
        let store = test_store("delete-full-subtree");
        assert!(create_group_in(&data, "work", None, request("west", "West"), &store, &Actor::system()).ok);

        // A pipeline, next to the manifest `create_group_in` wrote. The part of a group's bundle a delete must
        // reach without being told each path by name.
        let repo = git::repo_path(&data);
        let parent = git::branch_tip(&repo, "work").unwrap();
        let pipeline = git::FileEdit {
            path: "groups/west/pipelines/p1.json".to_string(),
            content: Some(br#"{"id":"p1","steps":[]}"#.to_vec()),
        };
        git::commit_edits(
            &repo,
            "work",
            &parent,
            Some(&parent),
            &[pipeline],
            "add pipeline",
            &Actor::system(),
        )
        .unwrap();

        let deleted = delete_group_in(&data, "work", None, "west", &Actor::system());
        assert!(deleted.ok, "{:?}", deleted.errors);

        let head = git::branch_tip(&repo, "work").unwrap();
        assert!(
            git::list_tree(&repo, &head, "groups/west").unwrap().is_empty(),
            "nothing of the group's subtree remains"
        );
    }

    #[test]
    fn deleting_a_group_that_does_not_exist_is_refused() {
        let data = temp_dir("delete-missing");
        let deleted = delete_group_in(&data, "work", None, "nowhere", &Actor::system());
        assert!(!deleted.ok);
    }

    /// `Writing::open_in` accepts `shared` as a scope -- it is a real subtree writes target -- so a delete has to
    /// refuse it before opening, or it would remove `shared/` itself: every route, source and destination every
    /// group draws on (a Codex finding on this PR).
    #[test]
    fn deleting_the_shared_scope_is_refused() {
        let data = temp_dir("delete-shared");
        let deleted = delete_group_in(&data, "work", None, config_file::SHARED_DIR, &Actor::system());
        assert!(!deleted.ok);
    }

    /// `delete_group_in` and the branch-scoped listing together: a real delete of a deployed
    /// group must make it vanish from the branch's own listing, not just leave its files gone.
    #[test]
    fn deleting_a_deployed_group_removes_it_from_the_branchs_listing() {
        let data = temp_dir("delete-deployed");
        let store = test_store("delete-deployed");
        deploy_to_main(&data, "east", "East");

        let deleted = delete_group_in(&data, "work", None, "east", &Actor::system());
        assert!(deleted.ok, "{:?}", deleted.errors);

        assert!(list_groups_in(&data, "work", &store).unwrap().is_empty());
    }

    /// `east` deployed to `main` with a source, then a branch `stray` that removes its manifest and nothing else.
    fn east_without_its_manifest(data: &Path) -> &'static str {
        deploy_to_main(data, "east", "East");
        let repo = repo_at(data).unwrap();
        let parent = git::main_head(&repo).unwrap();
        let sources = git::FileEdit {
            path: "groups/east/sources.json".to_string(),
            content: Some(b"[]".to_vec()),
        };
        git::commit_edits(
            &repo,
            git::MAIN_BRANCH,
            &parent,
            Some(&parent),
            &[sources],
            "add sources",
            &Actor::system(),
        )
        .unwrap();
        let parent = git::main_head(&repo).unwrap();
        let manifest = git::FileEdit {
            path: "groups/east/system.json".to_string(),
            content: None,
        };
        git::commit_edits(
            &repo,
            "stray",
            &parent,
            None,
            &[manifest],
            "remove the manifest",
            &Actor::system(),
        )
        .unwrap();
        "stray"
    }

    /// A group exists on a commit exactly when `groups/<id>/` holds a file, manifest or not. Such a group lists, its
    /// id is taken, and a delete takes all of it.
    #[test]
    fn a_group_is_any_file_under_its_directory() {
        let data = temp_dir("manifestless");
        let store = test_store("manifestless");
        let branch = east_without_its_manifest(&data);

        let ids: Vec<String> = list_groups_in(&data, branch, &store)
            .unwrap()
            .into_iter()
            .map(|g| g.id)
            .collect();
        assert_eq!(ids, ["east"]);
        let again = create_group_in(&data, branch, None, request("east", "East"), &store, &Actor::system());
        assert!(!again.ok, "an id whose directory holds a file was created again");

        let deleted = delete_group_in(&data, branch, None, "east", &Actor::system());
        assert!(deleted.ok, "{:?}", deleted.errors);
        assert!(list_groups_in(&data, branch, &store).unwrap().is_empty());
    }

    /// Renaming a group that has no manifest gives it one.
    #[test]
    fn a_group_with_no_manifest_can_be_renamed() {
        let data = temp_dir("manifestless-rename");
        let store = test_store("manifestless-rename");
        let branch = east_without_its_manifest(&data);

        let rename = UpdateWorkerGroupRequest {
            name: "Eastern".to_string(),
        };
        let renamed = update_group_in(&data, branch, None, "east", rename, &Actor::system());
        assert!(renamed.ok, "{:?}", renamed.errors);
        let names: Vec<String> = list_groups_in(&data, branch, &store)
            .unwrap()
            .into_iter()
            .map(|g| g.name)
            .collect();
        assert_eq!(names, ["Eastern"]);
    }

    /// A branch descends straight from the commit it forked from. It already holds everything `main` did then.
    /// A group `main` gains after that is the one thing the branch's own tree cannot show. By design, the branch
    /// picks that up by merging `main` in. This listing does not guess at it.
    #[test]
    fn a_group_main_gains_after_a_branch_forks_does_not_show_on_it_until_merged_in() {
        let data = temp_dir("stale-branch");
        let store = test_store("stale-branch");
        deploy_to_main(&data, "west", "West");
        assert!(create_group_in(&data, "work", None, request("north", "North"), &store, &Actor::system()).ok);

        // `main` moves again, after `work` already forked from it.
        deploy_to_main(&data, "south", "South");

        let ids: BTreeSet<String> = list_groups_in(&data, "work", &store)
            .unwrap()
            .into_iter()
            .map(|g| g.id)
            .collect();
        assert_eq!(
            ids,
            BTreeSet::from(["west".to_string(), "north".to_string()]),
            "west (inherited) and north (the branch's own) show; south does not, until work merges main in"
        );
    }

    /// The commit a listing hands a later write, for that write to base itself on -- worth its own test since a
    /// delete has nothing else to read first and compare against.
    #[test]
    fn resolve_branch_commit_falls_back_to_main_for_a_branch_that_does_not_exist_yet() {
        let data = temp_dir("resolve-commit-fallback");
        deploy_to_main(&data, "west", "West");
        let repo = repo_at(&data).unwrap();
        let main_head = git::main_head(&repo).unwrap();
        assert_eq!(resolve_branch_commit(&repo, "work").unwrap(), main_head);
    }

    #[test]
    fn resolve_branch_commit_reads_the_branchs_own_tip_once_it_exists() {
        let data = temp_dir("resolve-commit-own-tip");
        let store = test_store("resolve-commit-own-tip");
        assert!(create_group_in(&data, "work", None, request("west", "West"), &store, &Actor::system()).ok);
        let repo = repo_at(&data).unwrap();
        let tip = git::branch_tip(&repo, "work").unwrap();
        assert_eq!(resolve_branch_commit(&repo, "work").unwrap(), tip);
    }
}
