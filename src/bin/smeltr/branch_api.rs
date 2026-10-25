//! Branches: what work is in flight, and what each branch changes.
//!
//! A branch is a ref in the leader's repository (`leader::git`), so listing one needs no checkout. What a branch
//! changes is measured against where it left `main`, which is what makes it a change rather than a name.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use axum::Json;
use axum::extract::Extension;
use axum::extract::Path as AxPath;
use axum::extract::Query;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use smeltr::config_file;
use smeltr::leader::access::{Access, Level};
use smeltr::leader::store::Actor;
use smeltr::leader::{blocking, git, tree};
use smeltr::startup_config::leader_data_dir;

use crate::gate::refuse;
use smeltr::leader::envelope::Envelope;

/// The repository, created if nobody has opened the GUI yet. `main` needs a commit before a branch can be measured
/// against it, and both calls are idempotent.
fn repo() -> Result<PathBuf, String> {
    repo_at(&leader_data_dir())
}

fn repo_at(data_dir: &Path) -> Result<PathBuf, String> {
    let repo = git::repo_path(data_dir);
    git::init_bare_repo(&repo)?;
    git::ensure_main_commit(&repo)?;
    Ok(repo)
}

/// Why a handler stopped short of what it set out to do: with an answer already made, a refusal carrying its
/// status, or with the message its failed mutation answers with.
#[derive(Debug)]
enum Stop {
    Answered(Box<Response>),
    Failed(String),
}

impl From<String> for Stop {
    fn from(message: String) -> Self {
        Stop::Failed(message)
    }
}

impl From<git::GitError> for Stop {
    fn from(e: git::GitError) -> Self {
        Stop::Failed(e.into())
    }
}

impl From<smeltr::leader::store::StoreError> for Stop {
    fn from(e: smeltr::leader::store::StoreError) -> Self {
        Stop::Failed(e.into())
    }
}

fn forbidden(why: &str) -> Stop {
    Stop::Answered(Box::new(refuse(StatusCode::FORBIDDEN, why)))
}

/// What a mutation answers with: what it did, or why it stopped.
fn answered(result: Result<Envelope<BranchMutation>, Stop>) -> Response {
    match result {
        Ok(done) => Json(done).into_response(),
        Err(Stop::Answered(response)) => *response,
        Err(Stop::Failed(message)) => Json(Envelope::<BranchMutation>::failed_one(message)).into_response(),
    }
}

/// Every scope this branch's change reaches: the id of every group whose own subtree (`groups/<id>/`) it
/// edits, plus -- since every group's deployment merges `shared/` (design doc, Control plane) -- every group that
/// currently exists on the branch, and `shared` itself, when `shared/` changes. What `test_api`'s review
/// path runs a change's tests against, in place of the reviewer's own editing scope: the branch under
/// review is not the reviewer's chip.
pub(crate) fn scopes_touched(data_dir: &Path, branch: &str) -> Result<Vec<String>, String> {
    let repo = repo_at(data_dir)?;
    let changed = git::changed_files(&repo, branch)?;
    let shared_prefix = format!("{}/", config_file::SHARED_DIR);
    let groups_prefix = format!("{}/", config_file::GROUPS_DIR);

    let mut scopes: BTreeSet<String> = BTreeSet::new();
    let mut shared_touched = false;
    for c in &changed {
        if c.path.starts_with(&shared_prefix) {
            shared_touched = true;
        } else if let Some(rest) = c.path.strip_prefix(&groups_prefix)
            && let Some(id) = rest.split('/').next().filter(|s| !s.is_empty())
        {
            scopes.insert(id.to_string());
        }
    }
    if shared_touched {
        scopes.insert(config_file::SHARED_DIR.to_string());
        scopes.extend(tree::groups_at(&repo, branch)?);
    }
    Ok(scopes.into_iter().collect())
}

/// What `access` lacks to act on `branch`'s change at `level`, as the sentence a refusal answers with; `None` when
/// it lacks nothing.
///
/// Each changed path is judged in its own area: a change to `shared/` is judged as shared, not as every group that
/// draws on it, since it is shared that the person changes. A path in no area needs an administrator. Every one,
/// because approving or deploying a change approves or deploys all of it.
pub(crate) fn lacking_in_change(
    access: &Access,
    data_dir: &Path,
    branch: &str,
    level: Level,
) -> Result<Option<String>, String> {
    if access.is_administrator() {
        return Ok(None);
    }
    let repo = repo_at(data_dir)?;
    let changed = git::changed_files(&repo, branch)?;
    Ok(lacking_in(access, &changed, level))
}

/// [`lacking_in_change`], of the files a change touches.
fn lacking_in(access: &Access, changed: &[git::FileChange], level: Level) -> Option<String> {
    if access.is_administrator() {
        return None;
    }
    let mut areas = BTreeSet::new();
    for c in changed {
        match crate::policy::area_of_path(&c.path) {
            Some(area) => areas.insert(area),
            None => return Some(format!("this change touches {}, which needs an administrator", c.path)),
        };
    }
    areas
        .into_iter()
        .find(|area| !access.allows(area, level))
        .map(|area| format!("this change touches {area}, which needs {} there", level.as_str()))
}

/// Whether `access` sees every one of `paths`: a change, or a commit, is shown only to someone holding viewer in
/// every area it touches, so an area not held stays one the person cannot see.
fn sees_all<'a>(access: &Access, mut paths: impl Iterator<Item = &'a str>) -> bool {
    access.is_administrator()
        || paths.all(|path| crate::policy::area_of_path(path).is_some_and(|area| access.allows(&area, Level::Viewer)))
}

/// That `access` sees `branch`'s change, else why not: the branch does not exist, or `access` does not see all of
/// its change. Both answer the same sentence, so a change the person cannot see is one they cannot tell from a
/// branch that is not there. Existence is asked first: git resolves names a branch is not -- `HEAD` -- and a change
/// read from one would look empty, and seen.
fn seen(access: &Access, data_dir: &Path, branch: &str) -> Result<(), String> {
    let missing = || format!("there is no branch {branch:?}");
    let repo = repo_at(data_dir)?;
    if !git::branch_exists(&repo, branch)? {
        return Err(missing());
    }
    match lacking_in_change(access, data_dir, branch, Level::Viewer)? {
        None => Ok(()),
        Some(_) => Err(missing()),
    }
}

/// [`lacking_in_change`], answered: a refusal when something is lacking, a failed mutation when the change could
/// not be read.
fn refused_in_change(access: &Access, data_dir: &Path, branch: &str, level: Level) -> Result<(), Stop> {
    match lacking_in_change(access, data_dir, branch, level)? {
        None => Ok(()),
        Some(why) => Err(forbidden(&why)),
    }
}

/// What stops `access` changing `branch`'s change at `level`: [`seen`] first, answered as a branch that is not
/// there, then [`refused_in_change`]. Changing a change needs `level` in all of it, as approving one does.
fn refused_to_change(access: &Access, data_dir: &Path, branch: &str, level: Level) -> Result<(), Stop> {
    seen(access, data_dir, branch)?;
    refused_in_change(access, data_dir, branch, level)
}

// -- GET /api/branches ----------------------------------------------------

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BranchOut {
    name: String,
    head: String,
    /// The commit the branch left `main` at, which `touches` is measured against.
    base: String,
    /// Committer date of the tip, unix seconds.
    last_save: i64,
    /// Commits the branch has that `main` does not.
    commits: u32,
    /// Commits `main` has that the branch does not, which is what makes a change stale.
    behind: u32,
    /// The files the branch changes, relative to the repository root.
    touches: Vec<String>,
    /// Those of `touches` that are samples no test names, which a deploy leaves out (`git::branch_scratch_samples`).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    scratch: Vec<String>,
    /// The review request on this branch, where one was opened.
    #[serde(skip_serializing_if = "Option::is_none")]
    change: Option<ChangeOut>,
    /// Whether the latest save is on the remote. Absent with no remote set.
    #[serde(skip_serializing_if = "Option::is_none")]
    push: Option<PushOut>,
}

/// Where a branch's latest save stands on the remote.
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PushOut {
    /// `pushed`; `waiting`, not yet out or failed and to be tried again; or `refused`, the remote's branch having
    /// commits this leader did not push.
    state: &'static str,
    /// Why it is not pushed. Empty when it is, or is merely next.
    message: String,
    /// The remote's tip, when refused.
    #[serde(skip_serializing_if = "Option::is_none")]
    host_tip: Option<String>,
}

/// A branch opened for review.
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChangeOut {
    /// `open` or `closed`.
    state: String,
    title: String,
    description: String,
    /// Unix seconds.
    opened: i64,
    updated: i64,
    /// Why it was closed. Empty while it is open.
    reason: String,
    /// Who opened it, by the name they had then.
    author: String,
    /// Their account. Absent for an author this leader has no account for.
    #[serde(skip_serializing_if = "Option::is_none")]
    author_id: Option<String>,
}

impl From<git::Change> for ChangeOut {
    fn from(change: git::Change) -> Self {
        ChangeOut {
            state: match change.state {
                git::ChangeState::Open => "open",
                git::ChangeState::Closed => "closed",
            }
            .to_string(),
            title: change.title,
            description: change.description,
            opened: change.opened,
            updated: change.updated,
            reason: change.reason,
            author: change.author.display_name,
            author_id: Some(change.author.id).filter(|id| !id.is_empty()),
        }
    }
}

#[derive(Default, serde::Serialize)]
pub struct Branches {
    branches: Vec<BranchOut>,
    /// The remote this leader follows and pushes to.
    #[serde(skip_serializing_if = "Option::is_none")]
    remote: Option<String>,
    /// Approve pushes to the remote's `main`.
    #[serde(rename = "approvePushes")]
    approve_pushes: bool,
}

/// Every branch but `main`. `main` is what deploys move rather than work in flight, and it is measured by the
/// history and the cluster view instead.
pub async fn list_branches(
    axum::extract::State(state): axum::extract::State<smeltr::leader::AppState>,
    Extension(access): Extension<Access>,
) -> Json<Envelope<Branches>> {
    let failures = state.remote.push_failures();
    let listed = blocking(move || {
        let repo = repo()?;
        let remote = smeltr::leader::remote::configured(state.store.as_ref())?;
        let approve_pushes = remote.as_ref().is_some_and(|r| r.approve_pushes);
        let remote = remote.map(|r| r.url);
        let host = git::host_branches(&repo)?;
        let mut branches = Vec::new();
        for summary in git::list_branches(&repo)?
            .into_iter()
            .filter(|b| b.name != git::MAIN_BRANCH)
        {
            let touches: Vec<String> = git::changed_files(&repo, &summary.name)?
                .into_iter()
                .map(|c| c.path)
                .collect();
            if !sees_all(&access, touches.iter().map(String::as_str)) {
                continue;
            }
            let base = git::branch_base(&repo, &summary.name)?;
            let scratch = git::branch_scratch_samples(&repo, &summary.name)?;
            let change = git::read_change(&repo, &summary.name)?.map(ChangeOut::from);
            let push = remote.as_ref().map(|_| {
                match failures
                    .get(&summary.name)
                    .filter(|f| f.tip.as_ref() == Some(&summary.head))
                {
                    Some(failed) => PushOut {
                        state: if failed.refused { "refused" } else { "waiting" },
                        message: failed.message.clone(),
                        host_tip: failed.host_tip.clone(),
                    },
                    None => PushOut {
                        state: if host.get(&summary.name) == Some(&summary.head) {
                            "pushed"
                        } else {
                            "waiting"
                        },
                        message: String::new(),
                        host_tip: None,
                    },
                }
            });
            branches.push(BranchOut {
                push,
                name: summary.name,
                head: summary.head,
                base,
                last_save: summary.last_save,
                commits: summary.ahead,
                behind: summary.behind,
                touches,
                scratch,
                change,
            });
        }
        branches.sort_by_key(|b| std::cmp::Reverse(b.last_save));
        Ok::<_, String>(Branches {
            branches,
            remote,
            approve_pushes,
        })
    })
    .await;
    Json(listed.into())
}

// -- GET /api/branches/:name/diff -----------------------------------------

#[derive(serde::Deserialize, Default)]
pub struct PathQuery {
    /// One file to narrow to. Absent means the whole change.
    path: Option<String>,
}

/// Two commits and, optionally, one file of what lies between them.
#[derive(serde::Deserialize)]
pub struct RangeQuery {
    from: String,
    to: String,
    path: Option<String>,
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FileChangeOut {
    path: String,
    /// `A`, `M` or `D`.
    status: String,
    added: u32,
    removed: u32,
}

#[derive(Default, serde::Serialize)]
pub struct Diff {
    files: Vec<FileChangeOut>,
    /// Unified diff, for the whole change or for `path` alone.
    diff: String,
}

pub async fn branch_diff(
    Extension(access): Extension<Access>,
    AxPath(name): AxPath<String>,
    Query(q): Query<PathQuery>,
) -> Json<Envelope<Diff>> {
    let answered = blocking(move || {
        seen(&access, &leader_data_dir(), &name)?;
        let repo = repo()?;
        let files = git::changed_files(&repo, &name)?
            .into_iter()
            .filter(|c| q.path.as_deref().is_none_or(|p| p == c.path))
            .map(|c| FileChangeOut {
                path: c.path,
                status: c.status,
                added: c.added,
                removed: c.removed,
            })
            .collect();
        let diff = git::diff_unified(&repo, &name, q.path.as_deref())?;
        Ok::<_, String>(Diff { files, diff })
    })
    .await;
    Json(answered.into())
}

// -- POST /api/branches, DELETE /api/branches/:name -----------------------

#[derive(serde::Deserialize)]
pub struct CreateBranchRequest {
    name: String,
}

#[derive(Default, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BranchMutation {
    /// Where the branch starts, so an editor can send it as its base on the first save.
    #[serde(skip_serializing_if = "Option::is_none")]
    head: Option<String>,
    /// What the call did leave for the caller. Separate from `errors`: the call succeeded, and this is about the
    /// state it succeeded into -- a merge that landed on a branch that now does not load.
    #[serde(skip_serializing_if = "Option::is_none")]
    warning: Option<String>,
    /// What bringing a branch current needs a side chosen for. Nothing was written.
    #[serde(skip_serializing_if = "Option::is_none")]
    conflicts: Option<git::Conflicts>,
    /// Each group that would not load at the commit a deploy was asked for. Nothing was deployed; the same request
    /// with `confirm` deploys it.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    does_not_load: Vec<NotLoading>,
}

#[derive(serde::Serialize)]
pub struct NotLoading {
    group: String,
    why: String,
}

impl BranchMutation {
    fn done(head: Option<String>) -> Envelope<BranchMutation> {
        BranchMutation::done_with(head, None)
    }

    fn done_with(head: Option<String>, warning: Option<String>) -> Envelope<BranchMutation> {
        Envelope::ok(BranchMutation {
            head,
            warning,
            ..Default::default()
        })
    }
}

/// Start a branch at `main` deliberately, rather than by saving onto a name nobody has used. Both are how a branch
/// comes to exist (design doc, Config plane); this is the one the GUI offers when an author parks one piece of work
/// and picks
/// up another.
pub async fn create_branch(Json(req): Json<CreateBranchRequest>) -> Json<Envelope<BranchMutation>> {
    let created = blocking(move || git::create_branch(&leader_data_dir(), &req.name).map_err(String::from)).await;
    Json(created.map_or_else(Envelope::failed_one, |head| BranchMutation::done(Some(head))))
}

/// Delete a branch. Work it carries goes with it: a branch is how nothing is lost
/// while it exists, and deleting one is the deliberate end of that (W10). `main` is refused.
pub async fn delete_branch(Extension(access): Extension<Access>, AxPath(name): AxPath<String>) -> Response {
    answered(
        blocking(move || {
            refused_to_change(&access, &leader_data_dir(), &name, Level::Editor)?;
            git::delete_branch(&leader_data_dir(), &name, None)?;
            Ok(BranchMutation::done(None))
        })
        .await,
    )
}

// -- POST /api/changes, POST /api/changes/:branch/close, PATCH /api/changes/:branch ---------------------

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OpenChangeRequest {
    branch: String,
    /// What the change is called. It becomes the commit message the deploy carries onto `main`.
    title: String,
    #[serde(default)]
    description: String,
}

#[derive(serde::Deserialize, Default)]
pub struct CloseChangeRequest {
    /// Offered and never required (W6).
    #[serde(default)]
    reason: String,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RetitleChangeRequest {
    /// What the change is called from here on. It becomes the commit message the deploy carries onto `main`.
    title: String,
    #[serde(default)]
    description: String,
}

/// Open a branch for review against `main`. A branch that changes nothing is refused: the review would have nothing
/// to show, and the deploy nothing to carry.
pub async fn open_change(
    Extension(actor): Extension<Actor>,
    Extension(access): Extension<Access>,
    Json(req): Json<OpenChangeRequest>,
) -> Response {
    answered(
        blocking(move || {
            refused_to_change(&access, &leader_data_dir(), &req.branch, Level::Editor)?;
            // No write lock: a marker is a commit on `refs/smeltr/changes/<branch>`, which nothing checks out and no
            // other write touches, and it lands by compare-and-swap on that ref.
            let repo = repo()?;
            if let (0, _) = git::ahead_behind(&repo, &req.branch)? {
                return Err(Stop::Failed(format!(
                    "{:?} changes nothing yet -- save something on it first",
                    req.branch
                )));
            }
            git::open_change(&repo, &req.branch, &req.title, &req.description, &actor)?;
            Ok(BranchMutation::done(None))
        })
        .await,
    )
}

/// Close a change without deploying it. The branch stays, so nothing is lost and it can be opened again.
pub async fn close_change(
    Extension(actor): Extension<Actor>,
    Extension(access): Extension<Access>,
    AxPath(branch): AxPath<String>,
    body: Option<Json<CloseChangeRequest>>,
) -> Response {
    let reason = body.map(|Json(b)| b.reason).unwrap_or_default();
    answered(
        blocking(move || {
            refused_to_change(&access, &leader_data_dir(), &branch, Level::Editor)?;
            // No write lock, for the reason `open_change` gives.
            git::close_change(&repo()?, &branch, &reason, &actor)?;
            Ok(BranchMutation::done(None))
        })
        .await,
    )
}

/// Rename a change: a new title and description, in place of the ones it was opened with. Only a change still
/// open for review takes a rename -- the same guard `open_change` and `close_change` share.
pub async fn retitle_change(
    Extension(actor): Extension<Actor>,
    Extension(access): Extension<Access>,
    AxPath(branch): AxPath<String>,
    Json(req): Json<RetitleChangeRequest>,
) -> Response {
    answered(
        blocking(move || {
            refused_to_change(&access, &leader_data_dir(), &branch, Level::Editor)?;
            // No write lock, for the reason `open_change` gives.
            git::retitle_change(&repo()?, &branch, &req.title, &req.description, &actor)?;
            Ok(BranchMutation::done(None))
        })
        .await,
    )
}

// -- GET /api/changes/:branch/timeline, POST /api/changes/:branch/comments ----------------

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommentRequest {
    body: String,
    anchor: Option<AnchorJson>,
    /// The comment that started the thread this answers.
    reply_to: Option<String>,
}

/// Where a comment is anchored, as the editor sends and is sent it.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum AnchorJson {
    Object {
        scope: String,
        section: String,
        #[serde(rename = "objectId")]
        object_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        step: Option<String>,
    },
    Line {
        path: String,
        line: u32,
    },
}

impl From<AnchorJson> for git::Anchor {
    fn from(anchor: AnchorJson) -> Self {
        match anchor {
            AnchorJson::Object {
                scope,
                section,
                object_id,
                step,
            } => git::Anchor::Object {
                scope,
                section,
                id: object_id,
                step,
            },
            AnchorJson::Line { path, line } => git::Anchor::Line { path, line },
        }
    }
}

impl From<git::Anchor> for AnchorJson {
    fn from(anchor: git::Anchor) -> Self {
        match anchor {
            git::Anchor::Object {
                scope,
                section,
                id,
                step,
            } => AnchorJson::Object {
                scope,
                section,
                object_id: id,
                step,
            },
            git::Anchor::Line { path, line } => AnchorJson::Line { path, line },
        }
    }
}

/// One thing that happened to a change. `kind` says which of the optional fields it carries.
#[derive(serde::Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct TimelineEventOut {
    /// `opened`, `renamed`, `reopened`, `closed`, `comment`, `approved`, `changes-requested`, `resolved`,
    /// `unresolved` or `saved`.
    kind: &'static str,
    /// The marker's commit, on everything but a save.
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<String>,
    /// Unix seconds.
    time: i64,
    author: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    author_id: Option<String>,
    /// What an opening, a rename or a reopening called the change.
    #[serde(skip_serializing_if = "Option::is_none")]
    title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    /// A close's reason, a comment's or a verdict's body, or a save's subject. A verdict always carries it, empty
    /// for an approval that said nothing.
    #[serde(skip_serializing_if = "Option::is_none")]
    text: Option<String>,
    /// The save's commit.
    #[serde(skip_serializing_if = "Option::is_none")]
    commit: Option<String>,
    /// The commit a save was made on.
    #[serde(skip_serializing_if = "Option::is_none")]
    parent: Option<String>,
    /// What a comment is about. A reply's is its thread's.
    #[serde(skip_serializing_if = "Option::is_none")]
    anchor: Option<AnchorJson>,
    /// The branch's head when a comment was written.
    #[serde(skip_serializing_if = "Option::is_none")]
    at: Option<String>,
    /// The thread a reply, a resolve or an unresolve belongs to, by the comment that started it.
    #[serde(skip_serializing_if = "Option::is_none")]
    reply_to: Option<String>,
    /// On a comment: the file holding its thread's anchor changed since the thread started.
    #[serde(skip_serializing_if = "Option::is_none")]
    outdated: Option<bool>,
}

impl From<git::TimelineEvent> for TimelineEventOut {
    fn from(event: git::TimelineEvent) -> Self {
        let named = |kind, title, description| TimelineEventOut {
            kind,
            title: Some(title),
            description: Some(description),
            ..Default::default()
        };
        let said = |kind, text| TimelineEventOut {
            kind,
            text: Some(text),
            ..Default::default()
        };
        let thread = |kind, reply_to| TimelineEventOut {
            kind,
            reply_to: Some(reply_to),
            ..Default::default()
        };
        let mut out = match event.kind {
            git::EventKind::Opened { title, description } => named("opened", title, description),
            git::EventKind::Renamed { title, description } => named("renamed", title, description),
            git::EventKind::Reopened { title, description } => named("reopened", title, description),
            git::EventKind::Closed { reason } => said("closed", reason),
            git::EventKind::Comment {
                body,
                anchor,
                at,
                reply_to,
                outdated,
            } => TimelineEventOut {
                anchor: anchor.map(AnchorJson::from),
                at,
                reply_to,
                outdated: Some(outdated),
                ..said("comment", body)
            },
            git::EventKind::Resolved { reply_to } => thread("resolved", reply_to),
            git::EventKind::Unresolved { reply_to } => thread("unresolved", reply_to),
            git::EventKind::Approved { body } => said("approved", body),
            git::EventKind::ChangesRequested { body } => said("changes-requested", body),
            git::EventKind::Saved {
                commit,
                parent,
                subject,
            } => TimelineEventOut {
                commit: Some(commit),
                parent,
                ..said("saved", subject)
            },
        };
        out.id = event.id;
        out.time = event.time;
        out.author = event.author.display_name;
        out.author_id = Some(event.author.id).filter(|id| !id.is_empty());
        out
    }
}

#[derive(Default, serde::Serialize)]
pub struct Timeline {
    events: Vec<TimelineEventOut>,
}

/// What happened to a change, oldest first: its opening, renames, comments, saves and closes.
pub async fn change_timeline(
    Extension(access): Extension<Access>,
    AxPath(branch): AxPath<String>,
) -> Json<Envelope<Timeline>> {
    let answered = blocking(move || {
        seen(&access, &leader_data_dir(), &branch)?;
        let events = git::change_timeline(&repo()?, &branch)?;
        Ok::<_, String>(Timeline {
            events: events.into_iter().map(TimelineEventOut::from).collect(),
        })
    })
    .await;
    Json(answered.into())
}

/// Comment on a change, open or closed.
pub async fn comment_on_change(
    Extension(actor): Extension<Actor>,
    Extension(access): Extension<Access>,
    AxPath(branch): AxPath<String>,
    Json(req): Json<CommentRequest>,
) -> Json<Envelope<BranchMutation>> {
    let answered = blocking(move || {
        seen(&access, &leader_data_dir(), &branch)?;
        // No write lock, for the reason `open_change` gives.
        let anchor = req.anchor.map(git::Anchor::from);
        git::comment_on_change(&repo()?, &branch, &req.body, anchor, req.reply_to.as_deref(), &actor)?;
        Ok::<_, String>(BranchMutation::done(None))
    })
    .await;
    Json(answered.unwrap_or_else(Envelope::failed_one))
}

// -- POST /api/changes/:branch/threads/:id --------------------------------

#[derive(serde::Deserialize)]
pub struct ThreadRequest {
    resolved: bool,
}

/// Resolve the thread the comment `:id` started, or open it again.
pub async fn resolve_thread(
    Extension(actor): Extension<Actor>,
    Extension(access): Extension<Access>,
    AxPath((branch, root)): AxPath<(String, String)>,
    Json(req): Json<ThreadRequest>,
) -> Json<Envelope<BranchMutation>> {
    let answered = blocking(move || {
        seen(&access, &leader_data_dir(), &branch)?;
        // No write lock, for the reason `open_change` gives.
        git::resolve_thread(&repo()?, &branch, &root, req.resolved, &actor)?;
        Ok::<_, String>(BranchMutation::done(None))
    })
    .await;
    Json(answered.unwrap_or_else(Envelope::failed_one))
}

// -- POST /api/changes/:branch/reviews -----------------------------------

#[derive(serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
enum VerdictIn {
    Approve,
    RequestChanges,
}

#[derive(serde::Deserialize)]
pub struct ReviewRequest {
    verdict: VerdictIn,
    #[serde(default)]
    body: String,
}

/// Approve an open change or ask for changes to it. Either is recorded and shown; neither holds a deploy.
pub async fn review_change(
    Extension(actor): Extension<Actor>,
    Extension(access): Extension<Access>,
    AxPath(branch): AxPath<String>,
    Json(req): Json<ReviewRequest>,
) -> Response {
    answered(
        blocking(move || {
            refused_to_change(&access, &leader_data_dir(), &branch, Level::Deployer)?;
            // No write lock, for the reason `open_change` gives.
            let verdict = match req.verdict {
                VerdictIn::Approve => git::Verdict::Approved,
                VerdictIn::RequestChanges => git::Verdict::ChangesRequested,
            };
            git::review_change(&repo()?, &branch, verdict, &req.body, &actor)?;
            Ok(BranchMutation::done(None))
        })
        .await,
    )
}

// -- POST /api/changes/:branch/deploy, .../bring-current ------------------

/// Approve and deploy: `main` takes the branch as one commit, the fleet is pointed at it and the branch goes. There
/// is no separate deploy step -- workers pick it up on their next fetch (W5). A group losing its subtree this way is
/// not refused for it, live workers or not: one still heartbeating there loads an empty group, so it stops every
/// pipeline and idles until someone moves it to another group or shuts it down.
///
/// With a remote set, the remote owns `main`: Approve pushes the change there first when the remote's settings allow
/// it, and is refused otherwise, the change being merged on the host.
pub async fn deploy_change(
    axum::extract::State(state): axum::extract::State<smeltr::leader::AppState>,
    Extension(actor): Extension<Actor>,
    Extension(access): Extension<Access>,
    AxPath(branch): AxPath<String>,
) -> Response {
    // Held until the deploy is done, so a remote saved meanwhile waits for it rather than following over it.
    let syncing = state.remote.clone();
    let _turn = syncing.turn.lock().await;
    answered(
        blocking(move || {
            let data_dir = leader_data_dir();
            // Here rather than before the turn, so what is judged is the change as it stands when it deploys.
            let tip = judged_tip(&access, &data_dir, &branch)?;
            let deployed = match smeltr::leader::remote::configured(state.store.as_ref())? {
                None => git::deploy_change_at(&data_dir, &branch, &tip, &actor)?,
                Some(remote) => {
                    let deployed = smeltr::leader::remote::approve_at(&data_dir, &remote, &branch, &tip, &actor)?;
                    state.remote.heard(&deployed);
                    deployed
                }
            };
            Ok(BranchMutation::done(Some(deployed)))
        })
        .await,
    )
}

/// The tip `branch` is at, once `access` is judged to hold deployer in all of the change it carries there: what a
/// deploy then squashes, and nothing a save lands meanwhile.
fn judged_tip(access: &Access, data_dir: &Path, branch: &str) -> Result<String, Stop> {
    seen(access, data_dir, branch)?;
    let repo = repo_at(data_dir)?;
    // `seen` found it, so a branch gone by here went in between, and answers as it would have then.
    let tip = git::branch_tip(&repo, branch).ok_or_else(|| format!("there is no branch {branch:?}"))?;
    let changed = git::changed_files_at(&repo, &tip)?;
    match lacking_in(access, &changed, Level::Deployer) {
        Some(why) => Err(forbidden(&why)),
        None => Ok(tip),
    }
}

/// What the GUI sends to bring a change current: nothing the first time, and its choices once it has shown the
/// conflicts.
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BringCurrentRequest {
    #[serde(default)]
    choices: Option<git::Choices>,
}

/// Bring a change current with `main` so it is reviewed against what approving it would deploy onto. A branch already current is
/// answered without a commit. A merge that needs a side chosen answers with the conflicts and writes nothing.
pub async fn bring_current(
    Extension(actor): Extension<Actor>,
    Extension(access): Extension<Access>,
    AxPath(branch): AxPath<String>,
    Json(request): Json<BringCurrentRequest>,
) -> Response {
    answered(
        blocking(move || {
            refused_to_change(&access, &leader_data_dir(), &branch, Level::Editor)?;
            Ok(brought_current(
                &leader_data_dir(),
                &branch,
                &actor,
                request.choices.as_ref(),
            ))
        })
        .await,
    )
}

fn brought_current(
    data_dir: &Path,
    branch: &str,
    actor: &Actor,
    choices: Option<&git::Choices>,
) -> Envelope<BranchMutation> {
    match git::bring_current(data_dir, branch, actor, choices) {
        Ok(git::BringCurrent::Done(brought)) => BranchMutation::done_with(brought.commit, brought.does_not_load),
        // Not `ok`, and the conflicts are the whole answer: there is no message to put beside them.
        Ok(git::BringCurrent::Conflicts(conflicts)) => Envelope {
            ok: false,
            errors: vec![],
            body: BranchMutation {
                conflicts: Some(conflicts),
                ..Default::default()
            },
        },
        Err(e) => Envelope::failed_one(e.to_string()),
    }
}

// -- GET /api/history, POST /api/deploy -----------------------------------

#[derive(serde::Deserialize)]
pub struct HistoryQuery {
    /// How many commits to answer with, of those the caller sees. Fifty unless asked otherwise.
    limit: Option<u32>,
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CommitOut {
    commit: String,
    /// A deployed change's title, which is what the merge carried onto `main`.
    subject: String,
    body: String,
    /// Unix seconds.
    time: i64,
    files: Vec<String>,
    /// Who wrote it, as they were called then.
    author: String,
    /// The account behind that name, where the leader wrote the commit itself.
    #[serde(skip_serializing_if = "Option::is_none")]
    author_id: Option<String>,
    /// Who deployed it. Present on a deploy commit, absent everywhere else; equal to the author when somebody
    /// deployed their own change.
    #[serde(skip_serializing_if = "Option::is_none")]
    deployer: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    deployer_id: Option<String>,
}

#[derive(Default, serde::Serialize)]
pub struct History {
    /// What the fleet is running.
    #[serde(skip_serializing_if = "Option::is_none")]
    deployed: Option<String>,
    /// Present while a rollback holds the fleet behind `main`'s head.
    #[serde(skip_serializing_if = "Option::is_none")]
    held: Option<HeldOut>,
    commits: Vec<CommitOut>,
}

/// A hold, as History shows it beside the commit the fleet is held at.
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HeldOut {
    /// `main`'s head, which the fleet does not run.
    head: String,
    /// Commits on `main` the fleet does not run.
    behind: u32,
    /// Who held it, and when, unix seconds.
    by: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    by_id: Option<String>,
    since: i64,
    /// A rollback holds it, which an approval does not end.
    rolled_back: bool,
}

/// `main`'s history, newest first, with the commit the fleet runs and the hold it is under, if any.
pub async fn history(Extension(access): Extension<Access>, Query(q): Query<HistoryQuery>) -> Json<Envelope<History>> {
    let limit = q.limit.unwrap_or(50).clamp(1, 500);
    let answered = blocking(move || {
        let repo = repo()?;
        let commits = seen_history(&repo, &access, limit)?;
        let held = git::hold(&repo)?.map(|h| HeldOut {
            head: h.head,
            behind: h.behind,
            by_id: (!h.by.id.is_empty()).then_some(h.by.id),
            by: h.by.display_name,
            since: h.since,
            rolled_back: h.rolled_back,
        });
        Ok::<_, String>(History {
            deployed: git::deployed(&repo),
            held,
            commits: commits
                .into_iter()
                .map(|c| CommitOut {
                    commit: c.commit,
                    subject: c.subject,
                    body: c.body,
                    time: c.time,
                    files: c.files,
                    author: c.author,
                    author_id: c.author_id,
                    deployer: c.deployer,
                    deployer_id: c.deployer_id,
                })
                .collect(),
        })
    })
    .await;
    Json(answered.into())
}

/// The newest `limit` commits on `main` that `access` sees all of, however far back they are.
fn seen_history(repo: &Path, access: &Access, limit: u32) -> Result<Vec<git::CommitSummary>, git::GitError> {
    git::log_where(repo, git::MAIN_BRANCH, limit, |files| {
        sees_all(access, files.iter().map(String::as_str))
    })
}

#[derive(serde::Deserialize)]
pub struct DeployCommitRequest {
    commit: String,
    /// Deploy it even if a group does not load there. The workers in that group refuse it and keep what they run.
    #[serde(default)]
    confirm: bool,
}

/// Deploy a commit already in `main`'s history. Rolling back is this and nothing else: a deploy that moves
/// backward, reviewed by whoever holds deploy like any other (W7). Deploying `main`'s head releases a hold.
pub async fn deploy_commit(
    axum::extract::State(state): axum::extract::State<smeltr::leader::AppState>,
    Extension(actor): Extension<Actor>,
    Extension(access): Extension<Access>,
    Json(req): Json<DeployCommitRequest>,
) -> Response {
    // After an Approve in flight, which moves the fleet once the host has taken it and must find it where it was.
    let _turn = state.remote.turn.lock().await;
    answered(
        blocking(move || {
            let data_dir = leader_data_dir();
            if let Some(why) = lacking_for_fleet(&access, &data_dir, &req.commit)? {
                return Err(forbidden(&why));
            }
            if !req.confirm {
                let broken = git::deploying_would_not_load(&data_dir, &req.commit)?;
                if !broken.is_empty() {
                    return Ok(would_not_load(&req.commit, broken));
                }
            }
            let deployed = git::deploy_commit(&data_dir, &req.commit, &actor)?;
            Ok(BranchMutation::done(Some(deployed)))
        })
        .await,
    )
}

/// What `access` lacks to deploy `commit` of `main`'s history, as a refusal's sentence. Moving the fleet to
/// another commit moves every group at once, whatever that commit changed, so it needs deployer in shared and in
/// every group on `main` or at `commit`: a group only `commit` has is one it starts again.
fn lacking_for_fleet(access: &Access, data_dir: &Path, commit: &str) -> Result<Option<String>, String> {
    if access.is_administrator() {
        return Ok(None);
    }
    git::check_commit_id(commit)?;
    let repo = repo_at(data_dir)?;
    let mut groups: BTreeSet<String> = tree::groups_at(&repo, git::MAIN_BRANCH)?.into_iter().collect();
    groups.extend(tree::groups_at(&repo, commit)?);
    Ok(std::iter::once(config_file::SHARED_DIR.to_string())
        .chain(groups)
        .find(|area| !access.allows(area, Level::Deployer))
        .map(|area| format!("deploying a commit moves every group, and this needs deployer in {area}")))
}

fn would_not_load(commit: &str, broken: Vec<(String, String)>) -> Envelope<BranchMutation> {
    let groups: Vec<String> = broken.iter().map(|(group, _)| format!("{group:?}")).collect();
    let message = format!(
        "{} {} {} not load at {}, so {} workers would not start it. Nothing was deployed",
        if groups.len() == 1 { "group" } else { "groups" },
        groups.join(", "),
        if groups.len() == 1 { "does" } else { "do" },
        git::short(commit),
        if groups.len() == 1 { "its" } else { "their" },
    );
    Envelope::with(
        BranchMutation {
            does_not_load: broken
                .into_iter()
                .map(|(group, why)| NotLoading { group, why })
                .collect(),
            ..Default::default()
        },
        vec![message.into()],
    )
}

// -- GET /api/diff --------------------------------------------------------

/// What landed between two commits. A save refused because the branch moved names both of them, and this is what
/// the writer holding the only copy of their change reads before deciding which of the two to keep.
pub async fn commit_diff(Extension(access): Extension<Access>, Query(q): Query<RangeQuery>) -> Json<Envelope<Diff>> {
    let answered = blocking(move || {
        let repo = repo()?;
        let files = git::changed_between(&repo, &q.from, &q.to)?;
        if !sees_all(&access, files.iter().map(|f| f.path.as_str())) {
            return Err("this range touches an area you cannot see".to_string());
        }
        let diff = git::diff_between(&repo, &q.from, &q.to, q.path.as_deref())?;
        Ok(Diff {
            files: files
                .into_iter()
                .map(|c| FileChangeOut {
                    path: c.path,
                    status: c.status,
                    added: c.added,
                    removed: c.removed,
                })
                .collect(),
            diff,
        })
    })
    .await;
    Json(answered.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The status a stopped mutation answers with.
    fn status_of<T>(stopped: Result<T, Stop>) -> StatusCode {
        answered(stopped.map(|_| BranchMutation::done(None))).status()
    }

    // -- scopes_touched -------------------------------------------------

    fn temp_dir(label: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "smeltr-branch-api-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// A commit on `branch`, built from the commit it is based on, exactly as a real save lands one
    /// (`leader::git`'s own `save` test fixture).
    fn save(data: &Path, branch: &str, files: &[(&str, &[u8])], message: &str) -> String {
        let repo = repo_at(data).unwrap();
        let expected = git::branch_tip(&repo, branch);
        let parent = expected.clone().or_else(|| git::main_head(&repo)).unwrap();
        let edits: Vec<git::FileEdit> = files
            .iter()
            .map(|(path, body)| git::FileEdit {
                path: path.to_string(),
                content: Some(body.to_vec()),
            })
            .collect();
        match git::commit_edits(
            &repo,
            branch,
            &parent,
            expected.as_deref(),
            &edits,
            message,
            &Actor::system(),
        )
        .unwrap()
        {
            git::Committed::At(commit) => commit,
            git::Committed::Unchanged => panic!("the fixture's save changed nothing"),
            git::Committed::Moved(now) => panic!("the fixture's branch was at {now:?}"),
        }
    }

    const MANIFEST: &[u8] = br#"{"version":1}"#;

    /// `main` holding `shared/system.json` plus a manifest for each of `groups`.
    fn seed_main(data: &Path, groups: &[&str]) {
        let paths: Vec<String> = groups.iter().map(|g| format!("groups/{g}/system.json")).collect();
        let mut files: Vec<(&str, &[u8])> = vec![("shared/system.json", MANIFEST)];
        files.extend(paths.iter().map(|p| (p.as_str(), MANIFEST)));
        save(data, git::MAIN_BRANCH, &files, "seed main");
    }

    /// The reported case: two branches off one `main` each add a source. Once one deploys, the other is brought
    /// current with both sources, and deploys them both.
    #[test]
    fn two_source_additions_both_survive_bringing_current() {
        let data = temp_dir("two-sources");
        seed_main(&data, &["west"]);
        let repo = repo_at(&data).unwrap();
        git::create_branch(&data, "work").unwrap();
        git::create_branch(&data, "work2").unwrap();
        let path = "groups/west/sources.json";
        let source = |id: &str| format!(r#"[{{"id":"{id}","type":"metrics","bucketSeconds":10}}]"#);
        save(&data, "work", &[(path, source("a").as_bytes())], "source a");
        save(&data, "work2", &[(path, source("b").as_bytes())], "source b");
        git::open_change(&repo, "work2", "Source b", "", &Actor::system()).unwrap();
        git::deploy_change(&data, "work2", &Actor::system()).unwrap();
        let main = git::main_head(&repo);

        let response = brought_current(&data, "work", &Actor::system(), None);
        assert!(response.ok && response.warning.is_none(), "{:?}", response.errors);
        assert_eq!(git::main_head(&repo), main, "main moves only on deploy");
        assert_eq!(
            tree::load_group_at(&repo, "work", "west")
                .unwrap()
                .topology
                .sources
                .len(),
            2
        );

        git::open_change(&repo, "work", "Source a", "", &Actor::system()).unwrap();
        git::deploy_change(&data, "work", &Actor::system()).unwrap();
        assert_eq!(
            tree::load_group_at(&repo, "main", "west")
                .unwrap()
                .topology
                .sources
                .len(),
            2
        );
    }

    /// A conflict answers with what needs a side and writes nothing; the answer sent back lands it.
    #[test]
    fn a_conflict_is_answered_with_the_choices_it_needs() {
        let data = temp_dir("conflict-choices");
        seed_main(&data, &["west"]);
        let path = "groups/west/sources.json";
        let source = |d: &str| format!(r#"[{{"id":"a","type":"metrics","bucketSeconds":{d}}}]"#);
        save(&data, "main", &[(path, source("10").as_bytes())], "base");
        git::create_branch(&data, "work").unwrap();
        let work = save(&data, "work", &[(path, source("20").as_bytes())], "work");
        save(&data, "main", &[(path, source("30").as_bytes())], "main");
        let repo = repo_at(&data).unwrap();

        let response = brought_current(&data, "work", &Actor::system(), None);
        assert!(!response.ok && response.errors.is_empty());
        let conflicts = response.body.conflicts.expect("the conflicts");
        assert_eq!(git::branch_tip(&repo, "work"), Some(work), "nothing was written");

        let body = serde_json::json!({
            "main": conflicts.main,
            "work": conflicts.work,
            "picks": [{"path": path, "id": "a", "side": "work"}],
        });
        let choices: git::Choices = serde_json::from_value(body).unwrap();
        let response = brought_current(&data, "work", &Actor::system(), Some(&choices));
        assert!(response.ok, "{:?}", response.errors);
        assert_eq!(git::ahead_behind(&repo, "work").unwrap().1, 0);
    }

    /// Each kind carries only the fields it has, and an account only where the author has one.
    #[test]
    fn a_timeline_event_carries_the_fields_its_kind_has() {
        let data = temp_dir("timeline-json");
        seed_main(&data, &["west"]);
        let repo = repo_at(&data).unwrap();
        let left_at = git::main_head(&repo);
        let commit = save(&data, "work", &[("groups/west/routes.json", b"[]")], "edit west");
        let ann = Actor {
            id: "u-ann".into(),
            display_name: "Ann".into(),
        };
        git::open_change(&repo, "work", "Edit west", "", &ann).unwrap();
        git::comment_on_change(&repo, "work", "fine by me", None, None, &ann).unwrap();

        let events: Vec<serde_json::Value> = git::change_timeline(&repo, "work")
            .unwrap()
            .into_iter()
            .map(|e| serde_json::to_value(TimelineEventOut::from(e)).unwrap())
            .collect();
        let time = |i: usize| events[i]["time"].clone();
        let id = |i: usize| events[i]["id"].clone();
        assert!(id(1).is_string() && id(2).is_string());
        assert_eq!(
            events,
            vec![
                serde_json::json!({"kind": "saved", "time": time(0), "author": "smeltr-leader", "text": "edit west", "commit": commit, "parent": left_at}),
                serde_json::json!({"kind": "opened", "id": id(1), "time": time(1), "author": "Ann", "authorId": "u-ann", "title": "Edit west", "description": ""}),
                serde_json::json!({"kind": "comment", "id": id(2), "time": time(2), "author": "Ann", "authorId": "u-ann", "text": "fine by me", "outdated": false}),
            ]
        );
    }

    /// An anchored comment, a reply to it and the thread's resolve, as the editor reads them.
    #[test]
    fn a_thread_reaches_the_timeline_with_its_anchor() {
        let data = temp_dir("timeline-thread-json");
        seed_main(&data, &["west"]);
        let repo = repo_at(&data).unwrap();
        let head = save(&data, "work", &[("groups/west/routes.json", b"[]")], "edit west");
        let ann = Actor {
            id: "u-ann".into(),
            display_name: "Ann".into(),
        };
        git::open_change(&repo, "work", "Edit west", "", &ann).unwrap();
        let req: CommentRequest = serde_json::from_value(serde_json::json!({
            "body": "why this one?",
            "anchor": {"kind": "object", "scope": "west", "section": "routes", "objectId": "r1"},
        }))
        .unwrap();
        let anchor = req.anchor.map(git::Anchor::from);
        git::comment_on_change(&repo, "work", &req.body, anchor, None, &ann).unwrap();
        let event = |i: usize| {
            let e = git::change_timeline(&repo, "work").unwrap().swap_remove(i);
            serde_json::to_value(TimelineEventOut::from(e)).unwrap()
        };
        let root = event(2)["id"].as_str().unwrap().to_string();
        git::comment_on_change(&repo, "work", "because", None, Some(&root), &ann).unwrap();
        git::resolve_thread(&repo, "work", &root, true, &ann).unwrap();
        let anchor = serde_json::json!({"kind": "object", "scope": "west", "section": "routes", "objectId": "r1"});

        let (root_event, reply, resolved) = (event(2), event(3), event(4));
        assert_eq!(
            root_event,
            serde_json::json!({"kind": "comment", "id": root, "time": root_event["time"], "author": "Ann", "authorId": "u-ann",
                "text": "why this one?", "anchor": anchor, "at": head, "outdated": false})
        );
        assert_eq!(
            reply,
            serde_json::json!({"kind": "comment", "id": reply["id"], "time": reply["time"], "author": "Ann", "authorId": "u-ann",
                "text": "because", "anchor": anchor, "at": head, "replyTo": root, "outdated": false})
        );
        assert_eq!(
            resolved,
            serde_json::json!({"kind": "resolved", "id": resolved["id"], "time": resolved["time"], "author": "Ann",
                "authorId": "u-ann", "replyTo": root})
        );

        let line: CommentRequest = serde_json::from_value(
            serde_json::json!({"body": "x", "anchor": {"kind": "line", "path": "shared/routes.json", "line": 2}}),
        )
        .unwrap();
        assert!(matches!(line.anchor, Some(AnchorJson::Line { line: 2, .. })));
        let step: CommentRequest = serde_json::from_value(serde_json::json!({"body": "x", "replyTo": root,
            "anchor": {"kind": "object", "scope": "shared", "section": "pipelines", "objectId": "p", "step": "e"}}))
        .unwrap();
        assert_eq!(step.reply_to.as_deref(), Some(root.as_str()));
        assert!(matches!(step.anchor, Some(AnchorJson::Object { step: Some(_), .. })));
    }

    /// A verdict's body is in `text` even when it is empty, so the timeline can tell an approval that said nothing
    /// from one whose body went missing.
    #[test]
    fn a_verdict_carries_its_body_even_when_empty() {
        let data = temp_dir("timeline-verdict-json");
        seed_main(&data, &["west"]);
        let repo = repo_at(&data).unwrap();
        save(&data, "work", &[("groups/west/routes.json", b"[]")], "edit west");
        let ann = Actor {
            id: "u-ann".into(),
            display_name: "Ann".into(),
        };
        let bob = Actor {
            id: "u-bob".into(),
            display_name: "Bob".into(),
        };
        git::open_change(&repo, "work", "Edit west", "", &ann).unwrap();
        git::review_change(&repo, "work", git::Verdict::ChangesRequested, "name the route", &bob).unwrap();
        git::review_change(&repo, "work", git::Verdict::Approved, "", &bob).unwrap();

        let events: Vec<serde_json::Value> = git::change_timeline(&repo, "work")
            .unwrap()
            .into_iter()
            .skip(2)
            .map(|e| serde_json::to_value(TimelineEventOut::from(e)).unwrap())
            .collect();
        let time = |i: usize| events[i]["time"].clone();
        let id = |i: usize| events[i]["id"].clone();
        assert_eq!(
            events,
            vec![
                serde_json::json!({"kind": "changes-requested", "id": id(0), "time": time(0), "author": "Bob", "authorId": "u-bob", "text": "name the route"}),
                serde_json::json!({"kind": "approved", "id": id(1), "time": time(1), "author": "Bob", "authorId": "u-bob", "text": ""}),
            ]
        );
    }

    #[test]
    fn a_review_request_names_its_verdict_in_kebab_case() {
        let req: ReviewRequest = serde_json::from_str(r#"{"verdict": "request-changes", "body": "x"}"#).unwrap();
        assert!(matches!(req.verdict, VerdictIn::RequestChanges));
        let req: ReviewRequest = serde_json::from_str(r#"{"verdict": "approve"}"#).unwrap();
        assert!(matches!(req.verdict, VerdictIn::Approve));
        assert_eq!(req.body, "");
        assert!(serde_json::from_str::<ReviewRequest>(r#"{"verdict": "reject"}"#).is_err());
    }

    #[test]
    fn editing_two_groups_own_subtrees_names_both() {
        let data = temp_dir("two-groups");
        seed_main(&data, &["west", "east", "south"]);
        save(
            &data,
            "work",
            &[("groups/west/routes.json", b"[]"), ("groups/east/routes.json", b"[]")],
            "edit west and east",
        );
        assert_eq!(
            scopes_touched(&data, "work").unwrap(),
            vec!["east".to_string(), "west".to_string()]
        );
    }

    /// Every group merges `shared/`, so an edit there reaches every group that exists, not just `shared`
    /// itself -- the case the design brief calls out by name.
    #[test]
    fn editing_shared_names_shared_and_every_existing_group() {
        let data = temp_dir("shared-edit");
        seed_main(&data, &["west", "east"]);
        save(&data, "work", &[("shared/routes.json", b"[]")], "edit shared");
        assert_eq!(
            scopes_touched(&data, "work").unwrap(),
            vec!["east".to_string(), "shared".to_string(), "west".to_string()]
        );
    }

    /// "Every group" is read off the branch's own tree, so a group the branch itself adds is named too --
    /// not just what `main` already held.
    #[test]
    fn editing_shared_names_a_group_the_branch_itself_adds() {
        let data = temp_dir("shared-plus-new-group");
        seed_main(&data, &["west"]);
        save(
            &data,
            "work",
            &[("shared/routes.json", b"[]"), ("groups/south/system.json", MANIFEST)],
            "edit shared and add south",
        );
        assert_eq!(
            scopes_touched(&data, "work").unwrap(),
            vec!["shared".to_string(), "south".to_string(), "west".to_string()]
        );
    }

    // -- lacking_in_change, lacking_for_fleet ---------------------------

    fn holding(areas: &[(&str, Level)]) -> Access {
        Access::Areas(areas.iter().map(|(a, l)| (a.to_string(), *l)).collect())
    }

    /// A change to `shared/` is judged as shared: deploying it needs deployer there, not in every group that
    /// draws on it -- the expansion a test run makes is about whose records it reads.
    #[test]
    fn a_shared_change_is_judged_as_shared() {
        let data = temp_dir("lacking-shared");
        seed_main(&data, &["west", "east"]);
        save(&data, "work", &[("shared/routes.json", b"[]")], "edit shared");
        let shared_deployer = holding(&[("shared", Level::Deployer)]);
        assert_eq!(
            lacking_in_change(&shared_deployer, &data, "work", Level::Deployer).unwrap(),
            None
        );
        let west_deployer = holding(&[("west", Level::Deployer), ("east", Level::Deployer)]);
        assert!(
            lacking_in_change(&west_deployer, &data, "work", Level::Deployer)
                .unwrap()
                .unwrap()
                .contains("shared")
        );
    }

    #[test]
    fn a_change_needs_the_level_in_every_area_it_touches() {
        let data = temp_dir("lacking-groups");
        seed_main(&data, &["west", "east"]);
        save(
            &data,
            "work",
            &[("groups/west/routes.json", b"[]"), ("groups/east/routes.json", b"[]")],
            "edit west and east",
        );
        let west_only = holding(&[("west", Level::Deployer), ("east", Level::Editor)]);
        let lacking = lacking_in_change(&west_only, &data, "work", Level::Deployer)
            .unwrap()
            .unwrap();
        assert!(lacking.contains("east"), "{lacking}");
        let both = holding(&[("west", Level::Deployer), ("east", Level::Deployer)]);
        assert_eq!(lacking_in_change(&both, &data, "work", Level::Deployer).unwrap(), None);
        assert_eq!(
            lacking_in_change(&Access::Administrator, &data, "work", Level::Deployer).unwrap(),
            None
        );
    }

    #[test]
    fn a_path_in_no_area_needs_an_administrator() {
        let data = temp_dir("lacking-outside");
        seed_main(&data, &["west"]);
        save(&data, "work", &[("README.md", b"hi")], "outside every area");
        let everywhere = holding(&[("shared", Level::Deployer), ("west", Level::Deployer)]);
        let lacking = lacking_in_change(&everywhere, &data, "work", Level::Deployer)
            .unwrap()
            .unwrap();
        assert!(lacking.contains("administrator"), "{lacking}");
    }

    /// A change is shown only to someone who sees every area it touches; shared is seen by anyone holding an area.
    #[test]
    fn a_change_is_seen_only_whole() {
        let payments = holding(&[("payments", Level::Viewer)]);
        let paths = |p: &'static [&'static str]| p.iter().copied();
        assert!(sees_all(
            &payments,
            paths(&["groups/payments/sources.json", "shared/routes.json"])
        ));
        assert!(!sees_all(
            &payments,
            paths(&["groups/payments/sources.json", "groups/search/routes.json"])
        ));
        assert!(!sees_all(&payments, paths(&["README.md"])));
        assert!(sees_all(&payments, paths(&[])));
        assert!(sees_all(&Access::Administrator, paths(&["README.md"])));
    }

    /// Deploying a commit from history moves every group, so it needs deployer in shared and in each of them.
    #[test]
    fn deploying_a_commit_needs_deployer_across_the_fleet() {
        let data = temp_dir("lacking-fleet");
        seed_main(&data, &["west", "east"]);
        let head = git::main_head(&repo_at(&data).unwrap()).unwrap();
        let most = holding(&[("shared", Level::Deployer), ("west", Level::Deployer)]);
        assert!(
            lacking_for_fleet(&most, &data, &head)
                .unwrap()
                .unwrap()
                .contains("east")
        );
        let all = holding(&[
            ("shared", Level::Deployer),
            ("west", Level::Deployer),
            ("east", Level::Deployer),
        ]);
        assert_eq!(lacking_for_fleet(&all, &data, &head).unwrap(), None);
        assert_eq!(lacking_for_fleet(&Access::Administrator, &data, &head).unwrap(), None);
    }

    /// Rolling back to a commit that still has a group `main` has since removed starts that group again, so it is
    /// judged too.
    #[test]
    fn rolling_back_needs_deployer_in_the_groups_the_target_has() {
        let data = temp_dir("lacking-rollback");
        seed_main(&data, &["west", "east"]);
        let repo = repo_at(&data).unwrap();
        let target = git::main_head(&repo).unwrap();
        let removed = git::FileEdit {
            path: "groups/east/system.json".into(),
            content: None,
        };
        git::commit_edits(
            &repo,
            git::MAIN_BRANCH,
            &target,
            Some(&target),
            &[removed],
            "remove east",
            &Actor::system(),
        )
        .unwrap();
        let head = git::main_head(&repo).unwrap();
        let west = holding(&[("shared", Level::Deployer), ("west", Level::Deployer)]);
        assert_eq!(lacking_for_fleet(&west, &data, &head).unwrap(), None);
        let lacking = lacking_for_fleet(&west, &data, &target).unwrap().unwrap();
        assert!(lacking.contains("east"), "{lacking}");
    }

    /// Opening, renaming, closing, bringing current and deleting change the change, so each needs editor in every
    /// area it touches; a change the person cannot see is answered as a branch that is not there.
    #[test]
    fn changing_a_change_needs_editor_in_all_of_it() {
        let data = temp_dir("editor-change");
        seed_main(&data, &["west", "east"]);
        save(
            &data,
            "work",
            &[("groups/west/routes.json", b"[]"), ("groups/east/routes.json", b"[]")],
            "edit west and east",
        );
        let viewer = holding(&[("west", Level::Viewer), ("east", Level::Viewer)]);
        let west_editor = holding(&[("west", Level::Editor), ("east", Level::Viewer)]);
        let editor = holding(&[("west", Level::Editor), ("east", Level::Editor)]);
        for lacking in [&viewer, &west_editor] {
            let refused = refused_to_change(lacking, &data, "work", Level::Editor);
            assert_eq!(status_of(refused), StatusCode::FORBIDDEN);
        }
        assert!(refused_to_change(&editor, &data, "work", Level::Editor).is_ok());

        let west_only = holding(&[("west", Level::Editor)]);
        let hidden = refused_to_change(&west_only, &data, "work", Level::Editor);
        assert_eq!(status_of(hidden), StatusCode::OK);
        let missing = refused_to_change(&Access::Administrator, &data, "gone", Level::Editor);
        assert_eq!(status_of(missing), StatusCode::OK);
        assert_eq!(
            seen(&west_only, &data, "work").unwrap_err(),
            "there is no branch \"work\""
        );
        assert_eq!(seen(&editor, &data, "gone").unwrap_err(), "there is no branch \"gone\"");
        assert_eq!(
            git::delete_branch(&data, "gone", None).unwrap_err().to_string(),
            seen(&editor, &data, "gone").unwrap_err(),
            "a missing branch reads the same at every step"
        );
    }

    /// History answers `limit` commits the caller sees, reading past the ones it does not.
    #[test]
    fn history_answers_the_limit_of_what_is_seen() {
        let data = temp_dir("history-seen");
        seed_main(&data, &["west", "east"]);
        save(
            &data,
            git::MAIN_BRANCH,
            &[("groups/west/routes.json", b"[]")],
            "west one",
        );
        save(
            &data,
            git::MAIN_BRANCH,
            &[("groups/west/routes.json", b"[1]")],
            "west two",
        );
        for n in 0..3 {
            let body = format!("[{n}]");
            save(
                &data,
                git::MAIN_BRANCH,
                &[("groups/east/routes.json", body.as_bytes())],
                "east",
            );
        }
        let repo = repo_at(&data).unwrap();
        let west = holding(&[("west", Level::Viewer)]);
        let subjects = |limit| -> Vec<String> {
            seen_history(&repo, &west, limit)
                .unwrap()
                .into_iter()
                .map(|c| c.subject)
                .collect()
        };
        assert_eq!(subjects(2), ["west two", "west one"]);
        assert_eq!(subjects(1), ["west two"]);
    }

    /// A deploy squashes the tip it judged: a save landing after the judgment refuses the deploy rather than
    /// deploying what nobody judged.
    #[test]
    fn a_deploy_squashes_the_tip_it_judged() {
        let data = temp_dir("deploy-tip");
        seed_main(&data, &["west", "east"]);
        let repo = repo_at(&data).unwrap();
        save(&data, "work", &[("groups/west/routes.json", b"[]")], "edit west");
        git::open_change(&repo, "work", "West", "", &Actor::system()).unwrap();
        let west = holding(&[("west", Level::Deployer), ("east", Level::Viewer)]);
        let tip = judged_tip(&west, &data, "work").unwrap();
        save(&data, "work", &[("groups/east/routes.json", b"[]")], "edit east");
        assert_eq!(status_of(judged_tip(&west, &data, "work")), StatusCode::FORBIDDEN);

        let main = git::main_head(&repo);
        let err = git::deploy_change_at(&data, "work", &tip, &Actor::system()).unwrap_err();
        assert!(err.to_string().contains("moved"), "{err}");
        assert_eq!(git::main_head(&repo), main, "nothing deployed");
        let now = git::branch_tip(&repo, "work").unwrap();
        git::deploy_change_at(&data, "work", &now, &Actor::system()).unwrap();
    }

    #[test]
    fn a_branch_touching_nothing_reaches_no_scope() {
        let data = temp_dir("no-touch");
        seed_main(&data, &["west"]);
        git::create_branch(&data, "work").unwrap();
        assert!(scopes_touched(&data, "work").unwrap().is_empty());
    }
}
