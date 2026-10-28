//! Bundle distribution over git: one bare repo at `<data-dir>/repos/config.git`, holding every worker group's bundle
//! under `groups/<group-id>/` and the `shared/` one they all draw on (`config_file::bundle`). What the fleet runs is
//! the commit [`DEPLOYED_REF`] names, a commit in `main`'s history, and what one group runs is its own subtree of that
//! commit merged with `shared/`.
//!
//! Served by `git http-backend`, so a worker's `git fetch` is an unmodified git client. Read-only: a push is refused
//! by the server itself.
//!
//! Nothing is ever checked out, and nothing spawns git: reads and writes go through the object database in this
//! process (`gix`), and serving the repository is the one thing that still does. A read is of a commit, a save
//! builds its tree in memory, and every ref moves by compare-and-swap on what the caller read -- so writers cannot
//! interleave and none of them holds a lock.

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use super::store::Actor;

#[derive(Debug)]
pub struct GitError(pub String);

impl std::fmt::Display for GitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for GitError {}

impl From<GitError> for String {
    fn from(e: GitError) -> String {
        e.0
    }
}

/// The repository, opened for this call. Opening reads config and nothing more -- refs and objects are found on
/// demand -- so a handle is not worth holding across threads, and `gix::Repository` is `!Sync` anyway.
fn open(repo_path: &Path) -> Result<gix::Repository, GitError> {
    gix::open(repo_path).map_err(|e| GitError(format!("opening {}: {e}", repo_path.display())))
}

/// What a rev names here: a branch, a commit id, or a tree id -- the same spec git would take.
fn resolve(repo: &gix::Repository, rev: &str) -> Result<gix::ObjectId, GitError> {
    repo.rev_parse_single(rev)
        .map(|id| id.detach())
        .map_err(|e| GitError(format!("{rev:?} names nothing here: {e}")))
}

/// The tree a rev holds. Readers are handed both kinds: a commit for what a branch is at, and a bare tree for a
/// merge that has not been committed yet.
fn tree_at(repo: &gix::Repository, rev: &str) -> Result<gix::ObjectId, GitError> {
    resolve(repo, &format!("{rev}^{{tree}}"))
}

/// Git's own mode text, as `ls-tree` writes it: six octal digits.
fn mode_text(mode: gix::objs::tree::EntryMode) -> String {
    let mut backing = [0u8; 6];
    let text = mode.as_bytes(&mut backing).to_string();
    format!("{:0>6}", text)
}

/// The leader's one repository. Every group's bundle is a subtree of it (`config_file::bundle`), so shared logic is
/// one file rather than a copy per group.
pub fn repo_path(data_dir: &Path) -> PathBuf {
    data_dir.join("repos").join("config.git")
}

/// A group's subtree within the repository, refusing an id that is not safe as a path component. The check itself
/// lives beside the loader that also joins one (`config_file::group_subtree`), so both ways in agree.
pub fn group_subtree(group_id: &str) -> Result<String, GitError> {
    crate::config_file::group_subtree(group_id).map_err(GitError)
}

/// A bare repository whose `HEAD` names `main`, idempotent. Pinning the branch here rather than taking the host's
/// `init.defaultBranch` is what makes a clone of it land on the branch a deploy moves.
///
/// Built whole beside `path` and renamed into place, so a leader stopped part way leaves nothing at `path`: `gix`
/// writes `HEAD` before `config`, and a `HEAD` alone reads as a repository that is there.
pub fn init_bare_repo(path: &Path) -> Result<(), GitError> {
    if repo_ready(path) {
        return Ok(());
    }
    // Two requests creating it at once would build in one directory, and one could move the other's half into place.
    static CREATING: Mutex<()> = Mutex::new(());
    let _creating = CREATING.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    if repo_ready(path) {
        return Ok(());
    }
    let mut partial = path.as_os_str().to_owned();
    partial.push(".partial");
    let partial = PathBuf::from(partial);
    // What a start stopped part way left.
    if partial.exists() {
        std::fs::remove_dir_all(&partial)
            .map_err(|e| GitError(format!("removing the half-made {}: {e}", partial.display())))?;
    }
    std::fs::create_dir_all(&partial).map_err(|e| GitError(e.to_string()))?;
    build_bare_repo(&partial).inspect_err(|_| {
        let _ = std::fs::remove_dir_all(&partial);
    })?;
    // An empty directory at `path` is not a repository; a rename onto a directory needs it gone first on Windows.
    if path.exists() {
        std::fs::remove_dir(path).map_err(|e| GitError(format!("{} is in the way: {e}", path.display())))?;
    }
    std::fs::rename(&partial, path).map_err(|e| GitError(format!("moving {} into place: {e}", partial.display())))
}

fn build_bare_repo(path: &Path) -> Result<(), GitError> {
    let repo = gix::init_bare(path).map_err(|e| GitError(format!("creating {}: {e}", path.display())))?;

    use gix::refs::transaction::{Change, LogChange, PreviousValue, RefEdit};
    repo.edit_reference(RefEdit {
        change: Change::Update {
            log: LogChange::default(),
            expected: PreviousValue::Any,
            new: gix::refs::Target::Symbolic(full_name(&format!("refs/heads/{MAIN_BRANCH}"))?),
        },
        name: full_name("HEAD")?,
        deref: false,
    })
    .map_err(|e| GitError(format!("pointing HEAD at {MAIN_BRANCH}: {e}")))?;
    // Unstamped with commits in it reads as pre-release on the next start, so it is stamped before it is in place.
    super::steps::stamp_new_repo(path).map_err(GitError)
}

/// Whether the repository has been created. `HEAD` is what `init_bare_repo` writes, so a directory that exists and
/// holds nothing does not count.
pub fn repo_ready(repo_path: &Path) -> bool {
    repo_path.join("HEAD").is_file()
}

/// Whether every ref reads, every commit in their history reads, and every object in each ref's tree reads, or
/// the first that does not. A repository not created yet passes.
///
/// For the start, before anything acts on the repository: a ref that does not read is otherwise taken for one that
/// is not there, so a damaged `DEPLOYED_REF` reads as nothing deployed. Each object is read whole and hashed, so a
/// damaged one fails its zlib checksum or its name. History is read to its commits and not their trees, which
/// rollback and the deploy log need and which would otherwise cost every tree ever saved.
pub fn check_repo(repo_path: &Path) -> Result<(), GitError> {
    if !repo_ready(repo_path) {
        // `init_bare_repo` moves a repository into place whole, so one with files and no `HEAD` lost it.
        if std::fs::read_dir(repo_path).is_ok_and(|mut entries| entries.next().is_some()) {
            return Err(GitError(format!(
                "{} does not read back whole: it has no HEAD",
                repo_path.display()
            )));
        }
        return Ok(());
    }
    let repo = open(repo_path)?;
    let damaged = |what: String| GitError(format!("{} does not read back whole: {what}", repo_path.display()));
    let references = repo.references().map_err(|e| damaged(e.to_string()))?;
    let mut tips = Vec::new();
    for reference in references.all().map_err(|e| damaged(e.to_string()))? {
        let reference = reference.map_err(|e| damaged(format!("a ref does not read: {e}")))?;
        let name = reference.name().as_bstr().to_string();
        let tip = reference
            .into_fully_peeled_id()
            .map_err(|e| damaged(format!("{name} does not read: {e}")))?
            .detach();
        tips.push((name, tip));
    }

    // Hashed as well as read: a damaged loose file can inflate cleanly into some other object.
    let read = |id: gix::ObjectId, what: &str| -> Result<gix::Object<'_>, GitError> {
        let object = repo
            .find_object(id)
            .map_err(|e| damaged(format!("{what} {id} does not read: {e}")))?;
        let hash = gix::objs::compute_hash(id.kind(), object.kind, &object.data)
            .map_err(|e| damaged(format!("{what} {id} does not hash: {e}")))?;
        if hash != id {
            return Err(damaged(format!("{what} {id} reads back as {hash}")));
        }
        Ok(object)
    };
    let mut seen = std::collections::HashSet::new();
    for (name, tip) in &tips {
        let what = format!("{name}'s commit");
        let tree = read(*tip, &what)?
            .try_into_commit()
            .map_err(|e| damaged(format!("{what} {tip} is not a commit: {e}")))?
            .tree_id()
            .map_err(|e| damaged(format!("{what} {tip} does not read: {e}")))?
            .detach();
        let what = format!("a tree in {name}");
        let mut trees = vec![tree];
        while let Some(id) = trees.pop() {
            if !seen.insert(id) {
                continue;
            }
            let tree = read(id, &what)?
                .try_into_tree()
                .map_err(|e| damaged(format!("{what} {id} is not a tree: {e}")))?;
            for entry in tree.iter() {
                let entry = entry.map_err(|e| damaged(format!("{what} {id} does not read: {e}")))?;
                let id = entry.oid().to_owned();
                if entry.mode().is_tree() {
                    trees.push(id);
                } else if entry.mode().is_blob() && seen.insert(id) {
                    read(id, &format!("a blob in {name}"))?;
                }
            }
        }
        // A walk of its own rather than `rev_walk`, so history two refs share is read once.
        let what = format!("a commit in {name}'s history");
        let mut commits = vec![*tip];
        while let Some(id) = commits.pop() {
            if !seen.insert(id) {
                continue;
            }
            let commit = read(id, &what)?
                .try_into_commit()
                .map_err(|e| damaged(format!("{what} {id} is not a commit: {e}")))?;
            commits.extend(commit.parent_ids().map(|parent| parent.detach()));
        }
    }
    Ok(())
}

/// The branch every deploy moves the fleet along, and the only one a deploy moves.
pub const MAIN_BRANCH: &str = "main";

/// Give `main` a commit if it has none, so there is something for a branch to start at and a save to build on.
///
/// A fresh repository has an unborn `HEAD`, and every write here is against a commit. This is the empty tree, a
/// commit of it, and a ref pointing there.
pub fn ensure_main_commit(repo_path: &Path) -> Result<(), GitError> {
    let repo = open(repo_path)?;
    let reference = format!("refs/heads/{MAIN_BRANCH}");
    if repo.find_reference(&reference).is_ok() {
        return Ok(());
    }
    // Written rather than named: the empty tree is well known, but a commit of one nothing has stored is a commit
    // pointing at an object that is not there.
    let tree = repo
        .write_object(gix::objs::Tree::empty())
        .map_err(|e| GitError(format!("writing the empty tree: {e}")))?
        .detach();
    let commit = commit_tree_in(
        &repo,
        tree,
        &[],
        &["empty configuration"],
        &Actor::system(),
        Ordered::Clock,
    )?;
    deploy_in(
        &repo,
        commit,
        None,
        Main::Created,
        "Deploy the empty configuration",
        false,
        &Actor::system(),
    )
}

/// Put `edits` in `main`'s first commit, and answer whether this was the install that did it.
///
/// The one write to `main` that is not a deploy, and it is the same write `ensure_main_commit` already makes -- a
/// repository with no history has nothing to deploy from. Doing nothing once `main` has a commit is what keeps it
/// that way: any history at all is somebody's, and what their fleet runs is theirs to move.
pub fn seed_first_commit(repo_path: &Path, edits: &[FileEdit], message: &str) -> Result<bool, GitError> {
    init_bare_repo(repo_path)?;
    let repo = open(repo_path)?;
    let reference = format!("refs/heads/{MAIN_BRANCH}");
    if repo.find_reference(&reference).is_ok() {
        return Ok(false);
    }

    let failed = |e: String| GitError(format!("seeding {MAIN_BRANCH}: {e}"));
    let empty = repo
        .write_object(gix::objs::Tree::empty())
        .map_err(|e| failed(e.to_string()))?
        .detach();
    let mut editor = repo
        .find_tree(empty)
        .map_err(|e| failed(e.to_string()))?
        .edit()
        .map_err(|e| failed(e.to_string()))?;
    for edit in edits {
        let Some(bytes) = &edit.content else {
            // Nothing to remove from a tree that holds nothing.
            continue;
        };
        let blob = repo.write_blob(bytes).map_err(|e| failed(e.to_string()))?.detach();
        editor
            .upsert(edit.path.as_str(), gix::object::tree::EntryKind::Blob, blob)
            .map_err(|e| failed(e.to_string()))?;
    }
    let tree = editor.write().map_err(|e| failed(e.to_string()))?.detach();

    let commit = commit_tree_in(&repo, tree, &[], &[message], &Actor::system(), Ordered::Clock)?;
    // `None`: the ref must still not exist. Another writer that got here first wins and this seeds nothing.
    match deploy_in(&repo, commit, None, Main::Created, message, false, &Actor::system()) {
        Ok(()) => Ok(true),
        Err(_) => Ok(false),
    }
}

/// A branch name becomes path components under `refs/heads/` and reaches git as an argument, so every segment is
/// held to the shape a group id is (`config_file::is_safe_group_id`) -- stricter than git's own ref rules, and the
/// strictness is the point.
///
/// A slash separates segments and nothing else: `owner/what-they-are-doing` is how a fleet with more than one
/// author keeps names apart, and it is what the storyboards draw. Every segment is checked on its own, so a
/// separator can never smuggle in a `..` or an empty component that a joined path would collapse.
pub fn check_branch_name(branch: &str) -> Result<(), GitError> {
    let refused = || {
        Err(GitError(format!(
            "unsafe branch name {branch:?}: only letters, digits, '-', '_', '.' and '/' as a separator are \
             allowed, no part may start with '.' or '-' or be empty, and it must be 1-100 characters"
        )))
    };
    if branch.is_empty() || branch.len() > 100 {
        return refused();
    }
    for segment in branch.split('/') {
        // A leading '-' is refused on top of the group-id shape: a name is a bare argument to `git branch -D`, and
        // one that starts with a dash would be read there as an option rather than a branch. `is_safe_group_id`
        // refuses an empty segment and a leading '.', which is what keeps `..` and `//` out.
        if !crate::config_file::is_safe_group_id(segment) || segment.starts_with('-') {
            return refused();
        }
    }
    // Git stores a ref as a file, and one ending in `.lock` collides with the lock it takes to write another.
    if branch.ends_with(".lock") {
        return refused();
    }
    Ok(())
}

/// Whether `branch` is a ref here. A branch nobody has saved on does not exist yet.
pub fn branch_exists(repo_path: &Path, branch: &str) -> Result<bool, GitError> {
    check_branch_name(branch)?;
    let ref_name = format!("refs/heads/{branch}");
    Ok(open(repo_path)?.find_reference(&ref_name).is_ok())
}

/// Start a branch at `main` and answer where it starts. A ref is the whole of a branch.
///
/// An empty old value means "only if it does not exist", so the check and the creation are one operation rather than
/// a look followed by a write that something else could get between.
pub fn create_branch(data_dir: &Path, branch: &str) -> Result<String, GitError> {
    check_branch_name(branch)?;
    if branch == MAIN_BRANCH {
        return Err(GitError(format!(
            "{MAIN_BRANCH} is what deploys move -- it is not a branch to start"
        )));
    }
    let repo = repo_path(data_dir);
    init_bare_repo(&repo)?;
    ensure_main_commit(&repo)?;
    let Some(head) = main_head(&repo) else {
        return Err(GitError(format!("{MAIN_BRANCH} has no commit to start a branch from")));
    };

    if let Some(other) = name_collision(&repo, branch)? {
        return Err(GitError(format!(
            "branch {branch:?} cannot be started while {other:?} exists: a name and a namespace are the same \
             path, so one of them has to be renamed"
        )));
    }

    let reference = format!("refs/heads/{branch}");
    if let Err(e) = swap_ref(&repo, &reference, &head, None) {
        return Err(if branch_exists(&repo, branch)? {
            GitError(format!("branch {branch:?} is already there"))
        } else {
            e
        });
    }
    Ok(head)
}

/// A branch whose name makes `branch` impossible. A ref is a file under `refs/heads/`, so `al` and `al/work`
/// cannot both exist: one wants a file where the other wants a directory. Git refuses the second either way; this
/// is so the refusal names the branch in the way.
fn name_collision(repo_path: &Path, branch: &str) -> Result<Option<String>, GitError> {
    let inside = format!("{branch}/");
    Ok(list_branches(repo_path)?
        .into_iter()
        .map(|b| b.name)
        .find(|name| name.starts_with(&inside) || branch.starts_with(&format!("{name}/"))))
}

/// Delete a branch, and the change that was about it.
/// Delete `branch`, and with `at`, only while it is still at that commit: a save since keeps it.
pub fn delete_branch(data_dir: &Path, branch: &str, at: Option<&str>) -> Result<(), GitError> {
    check_branch_name(branch)?;
    if branch == MAIN_BRANCH {
        return Err(GitError(format!(
            "{MAIN_BRANCH} is what deploys move and is not deleted"
        )));
    }
    let repo = repo_path(data_dir);
    if !branch_exists(&repo, branch)? {
        // `update-ref -d` is content to delete what is not there, so this is said here or not at all.
        return Err(GitError(format!("there is no branch {branch:?}")));
    }
    let git = open(&repo)?;
    let at = at.map(|at| resolve(&git, at)).transpose()?;
    delete_ref(&git, &format!("refs/heads/{branch}"), at)?;
    // The change was about this branch, so it goes with it rather than being left pointing at a name nothing holds.
    delete_change(&repo, branch)?;
    Ok(())
}

// -- writing a commit -------------------------------------------------------
//
// A save builds its tree from the commit it read and swaps the branch onto it in one operation, so two writers
// cannot interleave and neither needs a lock: the loser is told the branch moved, which is what it was going to be
// told anyway.

/// One path in a commit: the bytes to put there, or `None` to remove it.
#[derive(Clone, Debug)]
pub struct FileEdit {
    pub path: String,
    pub content: Option<Vec<u8>>,
}

/// What a compare-and-swap commit did.
pub enum Committed {
    /// The branch is at this new commit.
    At(String),
    /// The edits changed nothing, so nothing was written and the branch did not move.
    Unchanged,
    /// The branch was not where the caller expected, so nothing was written. It is at this instead, or at nothing.
    Moved(Option<String>),
}

/// Apply `edits` to `parent`'s tree and point `branch` at the result.
///
/// `expected` is what `branch` must currently be for this to land -- `None` for "it must not exist", which is a
/// first save. The check and the write are one ref update, so no writer can get between them.
pub fn commit_edits(
    repo_path: &Path,
    branch: &str,
    parent: &str,
    expected: Option<&str>,
    edits: &[FileEdit],
    message: &str,
    author: &Actor,
) -> Result<Committed, GitError> {
    check_branch_name(branch)?;
    let repo = open(repo_path)?;
    let parent_id = resolve(&repo, parent)?;
    let (parent_tree, tree) = tree_with(&repo, parent, edits)?;
    if tree == parent_tree {
        return Ok(Committed::Unchanged);
    }

    let commit = commit_tree_in(&repo, tree, &[parent_id], &[message], author, Ordered::Clock)?;
    let expected = expected.map(|expected| resolve(&repo, expected)).transpose()?;
    match swap_ref_in(&repo, &format!("refs/heads/{branch}"), commit, expected) {
        Ok(()) => Ok(Committed::At(commit.to_string())),
        Err(_) => Ok(Committed::Moved(branch_tip(repo_path, branch))),
    }
}

/// `edits` applied to `parent` as a commit no ref names, for a check that has to read a file before the save that
/// adds it has landed. Nothing reaches it, so it moves no branch and is collected with the rest of git's garbage.
pub fn stage_edits(repo_path: &Path, parent: &str, edits: &[FileEdit]) -> Result<String, GitError> {
    let repo = open(repo_path)?;
    let parent_id = resolve(&repo, parent)?;
    let (_, tree) = tree_with(&repo, parent, edits)?;
    Ok(commit_tree_in(
        &repo,
        tree,
        &[parent_id],
        &["staged for a check"],
        &Actor::system(),
        Ordered::Clock,
    )?
    .to_string())
}

/// `parent`'s tree, and that tree with `edits` applied.
fn tree_with(
    repo: &gix::Repository,
    parent: &str,
    edits: &[FileEdit],
) -> Result<(gix::ObjectId, gix::ObjectId), GitError> {
    let parent_tree = tree_at(repo, parent)?;
    let failed = |e: String| GitError(format!("building a commit on {parent}: {e}"));

    let base = repo.find_tree(parent_tree).map_err(|e| failed(e.to_string()))?;
    let mut editor = base.edit().map_err(|e| failed(e.to_string()))?;
    for edit in edits {
        match &edit.content {
            Some(bytes) => {
                let blob = repo.write_blob(bytes).map_err(|e| failed(e.to_string()))?.detach();
                editor
                    .upsert(edit.path.as_str(), gix::object::tree::EntryKind::Blob, blob)
                    .map_err(|e| failed(e.to_string()))?;
            }
            // A path the parent does not hold is already gone, which is what removing it asked for.
            None => {
                editor.remove(edit.path.as_str()).map_err(|e| failed(e.to_string()))?;
            }
        }
    }
    let tree = editor.write().map_err(|e| failed(e.to_string()))?.detach();
    Ok((parent_tree, tree))
}

/// The tree a commit holds.
fn tree_of(repo_path: &Path, commit: &str) -> Result<String, GitError> {
    Ok(tree_at(&open(repo_path)?, commit)?.to_string())
}

/// Whether `commit` is reachable from `rev`. A commit with no ancestry in common is not, which is the same answer
/// `merge-base --is-ancestor` gives by failing.
fn is_ancestor(repo_path: &Path, commit: &str, rev: &str) -> Result<bool, GitError> {
    let repo = open(repo_path)?;
    let commit = resolve(&repo, commit)?;
    let rev = resolve(&repo, rev)?;
    Ok(repo.merge_base(commit, rev).is_ok_and(|base| base.detach() == commit))
}

/// A commit's first line.
fn commit_subject(repo_path: &Path, commit: &str) -> Result<String, GitError> {
    let repo = open(repo_path)?;
    let id = resolve(&repo, commit)?;
    let commit = repo
        .find_commit(id)
        .map_err(|e| GitError(format!("reading {id}: {e}")))?;
    let message = commit
        .message_raw()
        .map_err(|e| GitError(format!("reading {id}: {e}")))?
        .to_string();
    Ok(message.lines().next().unwrap_or_default().trim().to_string())
}

/// A ref name, refused here rather than reaching the ref store as something else.
fn full_name(reference: &str) -> Result<gix::refs::FullName, GitError> {
    gix::refs::FullName::try_from(reference).map_err(|e| GitError(format!("{reference:?} is not a ref name: {e}")))
}

/// Now, as a commit carries it: unix seconds at UTC, and no earlier than `after`. The leader is the only committer its
/// commits have, so there is no local zone to record.
fn signature_time(after: i64) -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|since| since.as_secs() as i64)
        .unwrap_or(0);
    format!("{} +0000", now.max(after))
}

/// A second after the latest of `parents`' committer times, so a commit is always newer than what it follows, even
/// within one second or after the leader's clock went back. A Kafka source's `start` is ordered by it
/// (`io::kafka::client::resolve_starts`).
fn after_parents(repo: &gix::Repository, parents: &[gix::ObjectId]) -> Result<i64, GitError> {
    let mut latest = 0;
    for parent in parents {
        let unreadable = |e: &dyn std::fmt::Display| GitError(format!("reading commit {parent}: {e}"));
        let commit = repo.find_commit(*parent).map_err(|e| unreadable(&e))?;
        let time = commit
            .committer()
            .map_err(|e| unreadable(&e))?
            .time()
            .map_err(|e| unreadable(&e))?;
        latest = latest.max(time.seconds + 1);
    }
    Ok(latest)
}

/// The author of a commit, as git records one.
///
/// Two fields out of an [`Actor`]: the display name it had when the commit was written, and an address derived
/// from its immutable id. The address is synthetic and undeliverable on purpose -- `.invalid` can never be a real
/// domain (RFC 2606) -- because smeltr knows who somebody is without knowing how to write to them, and a name
/// that looks like an email address invites someone to use it as one.
///
/// The id is what survives: a person renamed tomorrow keeps every commit they wrote, under the name they had and
/// the address they still have.
fn signature_for(actor: &Actor) -> (String, String) {
    (
        sanitize_identity(&actor.display_name),
        format!("{}@smeltr.invalid", sanitize_identity(&actor.id)),
    )
}

/// What git allows in an identity. `<`, `>` and a newline end the author line and start whatever came next, so a
/// display name carrying one could forge the rest of a commit's headers.
///
/// Validation refuses these long before here (`users_api`). This is the second line, in the one place where
/// getting it wrong produces a commit object that lies.
fn sanitize_identity(raw: &str) -> String {
    // A space rather than nothing: dropping the character outright would run the words on either side of it
    // together, so a name arriving with a stray newline would read as one word nobody is called.
    let spaced: String = raw
        .chars()
        .map(|c| match c {
            '<' | '>' => ' ',
            c if c.is_control() => ' ',
            c => c,
        })
        .collect();
    let collapsed = spaced.split_whitespace().collect::<Vec<_>>().join(" ");
    match collapsed.is_empty() {
        true => "unknown".to_string(),
        false => collapsed,
    }
}

/// A commit written to the object database and pointed at by nothing.
///
/// `messages` are paragraphs of one message, separated by a blank line, as `commit-tree -m` joins them.
fn commit_tree(
    repo_path: &Path,
    tree: &str,
    parents: &[&str],
    messages: &[&str],
    author: &Actor,
    ordered: Ordered,
) -> Result<String, GitError> {
    let repo = open(repo_path)?;
    let tree = resolve(&repo, tree)?;
    let parents = parents
        .iter()
        .map(|parent| resolve(&repo, parent))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(commit_tree_in(&repo, tree, &parents, messages, author, ordered)?.to_string())
}

/// When a commit says it was made. A deployed commit's time orders a Kafka source's `start` against the one each
/// partition carries (`io::kafka::client::resolve_starts`), so a commit `main` takes is always newer than its parent.
/// Every other commit keeps the clock: a change's timeline orders its saves and its comments by it.
#[derive(Clone, Copy)]
enum Ordered {
    AfterParents,
    Clock,
}

/// The committer is always the leader; the author is whoever asked.
///
/// That is git's own distinction and it is the true one here: the leader wrote every object in this repository,
/// and a person decided what it should say. It also means an operator reading `git log` sees who to ask about a
/// change, without the leader claiming a person ran a command they never ran.
fn commit_tree_in(
    repo: &gix::Repository,
    tree: gix::ObjectId,
    parents: &[gix::ObjectId],
    messages: &[&str],
    author: &Actor,
    ordered: Ordered,
) -> Result<gix::ObjectId, GitError> {
    let when = signature_time(match ordered {
        Ordered::AfterParents => after_parents(repo, parents)?,
        Ordered::Clock => 0,
    });
    let (name, email) = signature_for(author);
    let who = gix::actor::SignatureRef {
        name: name.as_str().into(),
        email: email.as_str().into(),
        time: when.as_str(),
    };
    let who = who.to_owned().map_err(|e| GitError(format!("writing a commit: {e}")))?;
    let (leader_name, leader_email) = signature_for(&Actor::system());
    let leader = gix::actor::SignatureRef {
        name: leader_name.as_str().into(),
        email: leader_email.as_str().into(),
        time: when.as_str(),
    };
    let leader = leader
        .to_owned()
        .map_err(|e| GitError(format!("writing a commit: {e}")))?;
    let message = format!("{}\n", messages.join("\n\n").trim_end_matches('\n'));

    let commit = gix::objs::Commit {
        tree,
        parents: parents.iter().copied().collect(),
        author: who,
        committer: leader,
        encoding: None,
        message: message.into(),
        extra_headers: Vec::new(),
    };
    repo.write_object(&commit)
        .map(|id| id.detach())
        .map_err(|e| GitError(format!("writing a commit: {e}")))
}

/// Point `reference` at `new`, but only while it still holds `old` -- `None` for "only if it does not exist". The
/// check and the write are one operation, so nothing can get between them.
///
/// `Err` is the swap being refused: the ref moved, and what that means is the caller's to say.
fn swap_ref(repo_path: &Path, reference: &str, new: &str, old: Option<&str>) -> Result<(), GitError> {
    let repo = open(repo_path)?;
    let new = resolve(&repo, new)?;
    let old = old.map(|old| resolve(&repo, old)).transpose()?;
    swap_ref_in(&repo, reference, new, old)
}

fn swap_ref_in(
    repo: &gix::Repository,
    reference: &str,
    new: gix::ObjectId,
    old: Option<gix::ObjectId>,
) -> Result<(), GitError> {
    use gix::refs::transaction::{Change, LogChange, PreviousValue, RefEdit};

    // `MustNotExist` lets a ref that already holds `new` through, where git refuses whatever it holds. Asking
    // first restores that. What the check cannot close is two callers creating the same ref at the same object,
    // which is both of them getting what they asked for.
    if old.is_none() && repo.find_reference(reference).is_ok() {
        return Err(GitError(format!("{reference} is already there")));
    }
    let expected = match old {
        Some(old) => PreviousValue::MustExistAndMatch(gix::refs::Target::Object(old)),
        None => PreviousValue::MustNotExist,
    };
    repo.edit_reference(RefEdit {
        change: Change::Update {
            log: LogChange {
                message: "smeltr".into(),
                ..Default::default()
            },
            expected,
            new: gix::refs::Target::Object(new),
        },
        name: full_name(reference)?,
        deref: false,
    })
    .map_err(|e| GitError(format!("{reference} did not move: {e}")))?;
    forget_advertisement();
    Ok(())
}

/// Drop `reference`, whatever it holds and whether or not it is there.
fn delete_ref(repo: &gix::Repository, reference: &str, at: Option<gix::ObjectId>) -> Result<(), GitError> {
    use gix::refs::transaction::{Change, PreviousValue, RefEdit, RefLog};

    repo.edit_reference(RefEdit {
        change: Change::Delete {
            expected: at.map_or(PreviousValue::Any, |at| {
                PreviousValue::MustExistAndMatch(gix::refs::Target::Object(at))
            }),
            log: RefLog::AndReference,
        },
        name: full_name(reference)?,
        deref: false,
    })
    .map_err(|e| GitError(format!("{reference} was not deleted: {e}")))?;
    forget_advertisement();
    Ok(())
}

// -- reading a commit -------------------------------------------------------
//
// A commit is immutable, so what it answers cannot change under a reader and the id it was asked for describes what
// came back.

/// One entry of a tree listing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TreeEntry {
    /// Git's own mode: `100644` a file, `040000` a directory, `120000` a symlink.
    pub mode: String,
    pub name: String,
}

impl TreeEntry {
    /// Git stores a symlink as a blob holding its target text and never follows one resolving a path, so a symlink in
    /// a tree is not a way out of it -- but reading one hands back the target string as though it were the file, so it
    /// is refused where a file is wanted.
    pub fn is_symlink(&self) -> bool {
        self.mode == "120000"
    }

    pub fn is_dir(&self) -> bool {
        self.mode == "040000"
    }
}

/// Whether `commit` and its tree are in the repository. [`list_tree`] reads a commit or tree it does not have as
/// empty, which is not the same as a commit that holds nothing.
pub fn has_commit(repo_path: &Path, commit: &str) -> Result<bool, GitError> {
    let repo = open(repo_path)?;
    Ok(tree_at(&repo, commit).is_ok_and(|tree| repo.find_tree(tree).is_ok()))
}

/// List one directory of `commit`'s tree, or nothing where that directory is not in it. Not recursive: a bundle names
/// what it reads.
pub fn list_tree(repo_path: &Path, commit: &str, path: &str) -> Result<Vec<TreeEntry>, GitError> {
    let repo = open(repo_path)?;
    // A path that is not in the tree is "nothing here", which is how an absent `shared/` or `pipelines/` reads.
    let Ok(root) = tree_at(&repo, commit) else {
        return Ok(vec![]);
    };
    let Ok(mut tree) = repo.find_tree(root) else {
        return Ok(vec![]);
    };
    if !path.is_empty() {
        let Ok(Some(entry)) = tree.lookup_entry_by_path(path) else {
            return Ok(vec![]);
        };
        let Ok(object) = entry.object() else {
            return Ok(vec![]);
        };
        if object.kind != gix::object::Kind::Tree {
            return Ok(vec![]);
        }
        tree = object.into_tree();
    }
    let mut entries = Vec::new();
    for entry in tree.iter() {
        let entry = entry.map_err(|e| GitError(format!("listing {path:?} of {commit}: {e}")))?;
        entries.push(TreeEntry {
            mode: mode_text(entry.inner.mode),
            name: entry.inner.filename.to_string(),
        });
    }
    Ok(entries)
}

/// Read several blobs out of `commit`, one answer per path in the order asked and `None` for a path the commit does
/// not hold. `commit` may name a tree instead, which is how a merge is read before anything points at it.
pub fn read_blobs(repo_path: &Path, commit: &str, paths: &[String]) -> Result<Vec<Option<Vec<u8>>>, GitError> {
    if paths.is_empty() {
        return Ok(vec![]);
    }
    let repo = open(repo_path)?;
    let root = tree_at(&repo, commit)?;
    let tree = repo
        .find_tree(root)
        .map_err(|e| GitError(format!("reading {commit}: {e}")))?;

    let mut answers = Vec::with_capacity(paths.len());
    for path in paths {
        let found = tree
            .lookup_entry_by_path(path.as_str())
            .map_err(|e| GitError(format!("reading {path:?} of {commit}: {e}")))?;
        answers.push(match found {
            Some(entry) => Some(
                entry
                    .object()
                    .map_err(|e| GitError(format!("reading {path:?} of {commit}: {e}")))?
                    .detach()
                    .data,
            ),
            None => None,
        });
    }
    Ok(answers)
}

/// When each of `paths` last changed on `commit`'s first-parent line: the committer time, in unix seconds, of the
/// newest commit whose tree holds a different version of it than its first parent's does. A path `commit` does not
/// hold is left out.
///
/// Walks back only until every path is placed, one lookup per path per commit.
pub fn last_changed(
    repo_path: &Path,
    commit: &str,
    paths: &[String],
) -> Result<std::collections::HashMap<String, i64>, GitError> {
    let mut placed = std::collections::HashMap::new();
    if paths.is_empty() {
        return Ok(placed);
    }
    let repo = open(repo_path)?;
    let failed = |e: String| GitError(format!("reading the history of {commit}: {e}"));
    let version = |tree: &gix::Tree<'_>, path: &str| -> Result<Option<gix::ObjectId>, GitError> {
        Ok(tree
            .lookup_entry_by_path(path)
            .map_err(|e| failed(e.to_string()))?
            .map(|entry| entry.object_id()))
    };

    let mut at = repo
        .find_commit(resolve(&repo, commit)?)
        .map_err(|e| failed(e.to_string()))?;
    let tree = at.tree().map_err(|e| failed(e.to_string()))?;
    let mut pending: Vec<(&String, gix::ObjectId)> = Vec::new();
    for path in paths {
        if let Some(id) = version(&tree, path)? {
            pending.push((path, id));
        }
    }
    while !pending.is_empty() {
        let time = at.time().map_err(|e| failed(e.to_string()))?.seconds;
        let parent = match at.parent_ids().next() {
            Some(parent) => Some(repo.find_commit(parent.detach()).map_err(|e| failed(e.to_string()))?),
            None => None,
        };
        let Some(parent) = parent else {
            // The root commit wrote whatever is still unplaced.
            for (path, _) in pending.drain(..) {
                placed.insert(path.clone(), time);
            }
            break;
        };
        let parent_tree = parent.tree().map_err(|e| failed(e.to_string()))?;
        let mut still = Vec::with_capacity(pending.len());
        for (path, id) in pending {
            if version(&parent_tree, path)? == Some(id) {
                still.push((path, id));
            } else {
                placed.insert(path.clone(), time);
            }
        }
        pending = still;
        at = parent;
    }
    Ok(placed)
}

// -- changes ---------------------------------------------------------------
//
// A change is a branch opened for review. Its title, description and close reason are the only things about it with
// no home in git already, so they are commit messages too: `refs/smeltr/changes/<branch>` holds a chain of marker
// commits against the empty tree, newest last. The title is the message the squash-merge carries onto `main`, so it
// is written once rather than kept in a second store and reconciled.
//
// The markers sit off `refs/heads/` so they are never a branch and never checked out, and off the branch itself so
// opening a change does not move the head an editor is saving against.

/// The trailer naming a marker's kind, on its own last line.
const CHANGE_TRAILER: &str = "Smeltr-Change:";

/// What a comment is about: a line of a file or one object of a bundle (`Anchor`). Like every trailer below, it sits
/// in the marker's last paragraph, above the kind.
const ANCHOR_TRAILER: &str = "Smeltr-Anchor:";
/// The branch's head when a comment was written, which is what its anchor was read against.
const AT_TRAILER: &str = "Smeltr-At:";
/// The comment that started the thread a reply, a resolve or an unresolve belongs to.
const REPLY_TO_TRAILER: &str = "Smeltr-Reply-To:";

/// The sections an object anchor may name, as the editor names them, and the file each lives in. A pipeline has a
/// file of its own (`None`), named by its slugged id.
const ANCHOR_SECTIONS: [(&str, Option<&str>); 6] = [
    ("sources", Some("sources.json")),
    ("destinations", Some("destinations.json")),
    ("routes", Some("routes.json")),
    ("lookupTables", Some(crate::config_file::LOOKUP_TABLES_FILE)),
    ("grokPatterns", Some(crate::config_file::GROK_PATTERNS_FILE)),
    ("pipelines", None),
];

/// Where a comment is anchored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Anchor {
    /// One object of a scope's bundle -- `shared` or a group id -- and, for a pipeline, optionally one of its steps.
    Object {
        scope: String,
        section: String,
        id: String,
        step: Option<String>,
    },
    /// One line of a file, the path relative to the repository and the line counted from 1 in the branch's copy.
    Line { path: String, line: u32 },
}

impl Anchor {
    fn check(&self) -> Result<(), GitError> {
        match self {
            Anchor::Object {
                scope,
                section,
                id,
                step,
            } => {
                scope_prefix(scope)?;
                if !ANCHOR_SECTIONS.iter().any(|(name, _)| name == section) {
                    return Err(GitError(format!(
                        "{section:?} is not a section an object is anchored in"
                    )));
                }
                if id.is_empty() || step.as_deref() == Some("") {
                    return Err(GitError(
                        "an object anchor names its object, and its step when it has one".into(),
                    ));
                }
            }
            Anchor::Line { path, line } => {
                if path.is_empty() || path.starts_with('/') || *line == 0 {
                    return Err(GitError(
                        "a line anchor names a file in the repository and a line counted from 1".into(),
                    ));
                }
            }
        }
        Ok(())
    }

    /// The file in the repository that holds what this names.
    fn file(&self) -> Result<String, GitError> {
        match self {
            Anchor::Object { scope, section, id, .. } => {
                let prefix = scope_prefix(scope)?;
                match ANCHOR_SECTIONS.iter().find(|(name, _)| name == section) {
                    Some((_, Some(file))) => Ok(format!("{prefix}/{file}")),
                    Some((_, None)) => Ok(format!("{prefix}/pipelines/{}.json", crate::config_file::slugify(id))),
                    None => Err(GitError(format!(
                        "{section:?} is not a section an object is anchored in"
                    ))),
                }
            }
            Anchor::Line { path, .. } => Ok(path.clone()),
        }
    }

    /// The trailer's value: space-separated words, each percent-encoded so a name with a space in it stays one.
    fn trailer(&self) -> String {
        match self {
            Anchor::Object {
                scope,
                section,
                id,
                step,
            } => {
                let mut words = vec!["object".to_string(), encoded(scope), encoded(section), encoded(id)];
                words.extend(step.as_deref().map(encoded));
                words.join(" ")
            }
            Anchor::Line { path, line } => format!("line {} {line}", encoded(path)),
        }
    }

    /// The anchor a trailer's value names. `None` for one this build cannot read, which then anchors nothing.
    fn parse(value: &str) -> Option<Anchor> {
        let words: Vec<&str> = value.split(' ').collect();
        let anchor = match words[..] {
            ["object", scope, section, id] | ["object", scope, section, id, _] => Anchor::Object {
                scope: decoded(scope)?,
                section: decoded(section)?,
                id: decoded(id)?,
                step: match words.get(4) {
                    Some(step) => Some(decoded(step)?),
                    None => None,
                },
            },
            ["line", path, line] => Anchor::Line {
                path: decoded(path)?,
                line: line.parse().ok()?,
            },
            _ => return None,
        };
        anchor.check().ok().map(|_| anchor)
    }
}

/// Where a scope's bundle sits: `shared`, or a group's subtree.
fn scope_prefix(scope: &str) -> Result<String, GitError> {
    if scope == crate::config_file::SHARED_DIR {
        return Ok(scope.to_string());
    }
    group_subtree(scope)
}

/// `%`, whitespace and control characters as `%XX` of their UTF-8 bytes, so a word holds no space and no newline.
fn encoded(word: &str) -> String {
    let mut out = String::with_capacity(word.len());
    for c in word.chars() {
        if c == '%' || c.is_whitespace() || c.is_control() {
            let mut bytes = [0; 4];
            for b in c.encode_utf8(&mut bytes).bytes() {
                out.push_str(&format!("%{b:02X}"));
            }
        } else {
            out.push(c);
        }
    }
    out
}

fn decoded(word: &str) -> Option<String> {
    let bytes = word.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hex = std::str::from_utf8(bytes.get(i + 1..i + 3)?).ok()?;
            out.push(u8::from_str_radix(hex, 16).ok()?);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok().filter(|w| !w.is_empty())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeState {
    Open,
    Closed,
}

impl ChangeState {
    fn as_str(self) -> &'static str {
        match self {
            ChangeState::Open => "open",
            ChangeState::Closed => "closed",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value.trim() {
            "open" => Some(ChangeState::Open),
            "closed" => Some(ChangeState::Closed),
            _ => None,
        }
    }
}

/// A reviewer's answer to a change. It is recorded and shown; a deploy does not wait on it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    Approved,
    ChangesRequested,
}

impl Verdict {
    fn as_str(self) -> &'static str {
        match self {
            Verdict::Approved => "approved",
            Verdict::ChangesRequested => "changes-requested",
        }
    }
}

/// What one marker records: the change moving to a state, or something said about it.
///
/// A build that does not know a kind reads past it (`read_markers`), so a new kind needs no step to land.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MarkerKind {
    State(ChangeState),
    Comment,
    Verdict(Verdict),
    /// A thread marked settled, or opened again.
    Resolved,
    Unresolved,
}

impl MarkerKind {
    fn as_str(self) -> &'static str {
        match self {
            MarkerKind::State(state) => state.as_str(),
            MarkerKind::Comment => "comment",
            MarkerKind::Verdict(verdict) => verdict.as_str(),
            MarkerKind::Resolved => "resolved",
            MarkerKind::Unresolved => "unresolved",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value.trim() {
            "comment" => Some(MarkerKind::Comment),
            "approved" => Some(MarkerKind::Verdict(Verdict::Approved)),
            "changes-requested" => Some(MarkerKind::Verdict(Verdict::ChangesRequested)),
            "resolved" => Some(MarkerKind::Resolved),
            "unresolved" => Some(MarkerKind::Unresolved),
            value => ChangeState::parse(value).map(MarkerKind::State),
        }
    }
}

/// A branch opened for review, folded from its marker chain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Change {
    pub state: ChangeState,
    /// From the newest `open` marker: what a closed-and-reopened change is still called.
    pub title: String,
    pub description: String,
    /// When that marker was written, unix seconds.
    pub opened: i64,
    /// When the state last moved.
    pub updated: i64,
    /// Why it was closed. Empty while it is open, and empty when it was closed without one -- W6 encourages a
    /// reason and never requires it.
    pub reason: String,
    /// Who opened it, from the same marker the title comes from. A deploy squashes the branch away, so this is
    /// what carries the content's author onto `main`.
    pub author: Actor,
}

/// What a change without a reason is closed with: git has no empty commit message, and the absence is worth saying.
pub const NO_REASON: &str = "Closed without a reason";

/// Names the deployer on a deploy commit, since git's own two slots are spoken for: the author wrote the content
/// and the committer is the leader that wrote the object.
const DEPLOYED_BY: &str = "Deployed-by: ";

/// One identity as a commit trailer holds it: `Name <id@smeltr.invalid>`, the same form git writes.
fn signature_line(actor: &Actor) -> String {
    let (name, email) = signature_for(actor);
    format!("{name} <{email}>")
}

fn change_ref(branch: &str) -> String {
    format!("refs/smeltr/changes/{branch}")
}

/// One marker commit.
struct Marker {
    /// Its commit, which is how a reply names the comment it answers.
    id: String,
    time: i64,
    kind: MarkerKind,
    /// The message above the trailers.
    text: String,
    anchor: Option<Anchor>,
    at: Option<String>,
    reply_to: Option<String>,
    /// Who wrote it. A marker is written by the leader under `refs/smeltr/`, which nothing can push to, so this
    /// is the leader's own record of who wrote it rather than a claim the branch could make.
    author: Actor,
}

impl Marker {
    /// What a state marker is titled: the change's title on an open, the reason on a close.
    fn subject(&self) -> &str {
        self.text.lines().next().unwrap_or_default()
    }

    fn description(&self) -> String {
        self.text
            .lines()
            .skip(1)
            .collect::<Vec<&str>>()
            .join("\n")
            .trim()
            .to_string()
    }

    fn state(&self) -> Option<ChangeState> {
        match self.kind {
            MarkerKind::State(state) => Some(state),
            MarkerKind::Comment | MarkerKind::Verdict(_) | MarkerKind::Resolved | MarkerKind::Unresolved => None,
        }
    }
}

/// Split a marker's message into its kind, the text above it, and the other trailers. The kind is the last line,
/// which is how it is written; a message without one is not a marker.
///
/// The other trailers are the lines above the kind back to the previous blank line, and only when every one of
/// them is a `Smeltr-<Key>: value`. `write_marker` always writes that paragraph itself after the text, so a text
/// whose own last paragraph looks like trailers stays text.
fn parse_marker(message: &str) -> Option<(MarkerKind, String, Trailers)> {
    let mut lines: Vec<&str> = message.lines().collect();
    let trailer = lines.pop()?;
    let kind = MarkerKind::parse(trailer.strip_prefix(CHANGE_TRAILER)?)?;
    let start = lines.iter().rposition(|l| l.trim().is_empty()).map_or(0, |i| i + 1);
    let trailers: Option<Trailers> = lines[start..].iter().map(|l| parse_trailer(l)).collect();
    match trailers {
        Some(trailers) if !trailers.is_empty() => Some((kind, lines[..start].join("\n"), trailers)),
        _ => Some((kind, lines.join("\n"), vec![])),
    }
}

/// A marker's trailers other than its kind, each as its key, colon included, and its value.
type Trailers = Vec<(String, String)>;

/// `Smeltr-<Key>: value` as its key, colon included, and its value.
fn parse_trailer(line: &str) -> Option<(String, String)> {
    let (key, value) = line.split_once(": ")?;
    let name = key.strip_prefix("Smeltr-")?;
    if name.is_empty() || !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '-') {
        return None;
    }
    Some((format!("{key}:"), value.to_string()))
}

/// The commit `branch`'s marker chain is at, or `None` where nothing has been written to it.
fn change_tip(repo_path: &Path, branch: &str) -> Result<Option<String>, GitError> {
    let repo = open(repo_path)?;
    let Ok(reference) = repo.find_reference(&change_ref(branch)) else {
        return Ok(None);
    };
    let id = reference
        .into_fully_peeled_id()
        .map_err(|e| GitError(format!("reading {branch:?}'s change: {e}")))?;
    Ok(Some(id.detach().to_string()))
}

fn read_markers(repo_path: &Path, branch: &str) -> Result<Vec<Marker>, GitError> {
    let repo = open(repo_path)?;
    let Ok(reference) = repo.find_reference(&change_ref(branch)) else {
        return Ok(vec![]);
    };
    let failed = |e: String| GitError(format!("reading {branch:?}'s change: {e}"));
    let tip = reference.into_fully_peeled_id().map_err(|e| failed(e.to_string()))?;

    let mut markers = Vec::new();
    for info in repo.rev_walk([tip.detach()]).all().map_err(|e| failed(e.to_string()))? {
        let info = info.map_err(|e| failed(e.to_string()))?;
        let commit = repo.find_commit(info.id).map_err(|e| failed(e.to_string()))?;
        let message = commit.message_raw().map_err(|e| failed(e.to_string()))?.to_string();
        let Some((kind, text, trailers)) = parse_marker(&message) else {
            continue;
        };
        // A key this build does not know is read past, as a kind it does not know is.
        let trailer = |key: &str| trailers.iter().rev().find(|(k, _)| k == key).map(|(_, v)| v.clone());
        let author = commit.author().map_err(|e| failed(e.to_string()))?;
        markers.push(Marker {
            id: info.id.to_string(),
            time: commit.time().map_err(|e| failed(e.to_string()))?.seconds,
            kind,
            text,
            anchor: trailer(ANCHOR_TRAILER).and_then(|v| Anchor::parse(&v)),
            at: trailer(AT_TRAILER),
            reply_to: trailer(REPLY_TO_TRAILER),
            author: actor_of(author.name.to_string(), author.email.to_string()),
        });
    }
    Ok(markers)
}

/// The change on `branch`, or `None` where none was ever opened. Markers come back newest first.
pub fn read_change(repo_path: &Path, branch: &str) -> Result<Option<Change>, GitError> {
    check_branch_name(branch)?;
    // A comment or a verdict is said about a change and moves none of what this folds.
    let markers: Vec<(Marker, ChangeState)> = read_markers(repo_path, branch)?
        .into_iter()
        .filter_map(|m| m.state().map(|state| (m, state)))
        .collect();
    let Some((newest, newest_state)) = markers.first() else {
        return Ok(None);
    };
    // The title survives a close: a reopened change is the same change, and its reason is not its name.
    let opened_marker = markers.iter().find(|(_, s)| *s == ChangeState::Open).map(|(m, _)| m);
    // When it was opened, not when it was last renamed. A rename adds another open marker, so the
    // unbroken run of them back to the previous close is all one opening; only a close and a reopen
    // start a new one.
    let opened_at = markers
        .iter()
        .take_while(|(_, s)| *s == ChangeState::Open)
        .last()
        .map(|(m, _)| m)
        .or(opened_marker);
    Ok(Some(Change {
        state: *newest_state,
        title: opened_marker.map(|m| m.subject().to_string()).unwrap_or_default(),
        description: opened_marker.map(|m| m.description()).unwrap_or_default(),
        opened: opened_at.map(|m| m.time).unwrap_or(newest.time),
        updated: newest.time,
        reason: match newest_state {
            ChangeState::Closed => newest.subject().to_string(),
            ChangeState::Open => String::new(),
        },
        // Who opened it, not who last renamed it: a rename writes another open marker, and renaming somebody
        // else's change does not make it yours.
        author: opened_at.or(opened_marker).unwrap_or(newest).author.clone(),
    }))
}

/// A state marker's text: its subject, then its description where it has one.
fn titled(subject: &str, description: &str) -> String {
    match description.trim() {
        "" => subject.to_string(),
        description => format!("{subject}\n\n{description}"),
    }
}

/// Add a marker to `branch`'s chain, on top of `expected` -- the tip the caller decided against.
///
/// `expected` is the new marker's parent and the old value the ref must still hold, so deciding what to write and
/// writing it are one operation: a chain that moved in between is refused rather than written over. `None` is the
/// first marker, and requires the ref not to exist.
///
/// `trailers` go in the last paragraph, above the kind, each as `<key> <value>`.
fn write_marker(
    repo_path: &Path,
    branch: &str,
    text: &str,
    kind: MarkerKind,
    trailers: &[(&str, String)],
    expected: Option<&str>,
    author: &Actor,
) -> Result<(), GitError> {
    let reference = change_ref(branch);
    let mut closing = String::new();
    for (key, value) in trailers {
        if value.contains(['\n', '\r']) {
            return Err(GitError(format!("{key} cannot hold a line break")));
        }
        closing.push_str(&format!("{key} {value}\n"));
    }
    closing.push_str(&format!("{CHANGE_TRAILER} {}\n", kind.as_str()));
    // An approval may say nothing, and then the trailers are the whole message.
    let message = match text {
        "" => closing,
        text => format!("{text}\n\n{closing}"),
    };

    let repo = open(repo_path)?;
    let tree = repo
        .write_object(gix::objs::Tree::empty())
        .map_err(|e| GitError(format!("writing the empty tree: {e}")))?
        .detach()
        .to_string();
    let parents: Vec<&str> = expected.into_iter().collect();
    let commit = commit_tree(repo_path, &tree, &parents, &[&message], author, Ordered::Clock)?;
    swap_ref(repo_path, &reference, &commit, expected).map_err(|_| {
        GitError(format!(
            "{branch:?}'s change moved while this was being written -- read it again"
        ))
    })
}

/// Open `branch` for review. A change already open is refused: one branch is one change (design doc, Config plane).
pub fn open_change(
    repo_path: &Path,
    branch: &str,
    title: &str,
    description: &str,
    author: &Actor,
) -> Result<(), GitError> {
    check_branch_name(branch)?;
    if title.trim().is_empty() {
        return Err(GitError(
            "a change needs a title: it is what the deploy commit is called".into(),
        ));
    }
    let tip = change_tip(repo_path, branch)?;
    if let Some(change) = read_change(repo_path, branch)?
        && change.state == ChangeState::Open
    {
        return Err(GitError(format!("{branch:?} is already open for review")));
    }
    write_marker(
        repo_path,
        branch,
        &titled(title.trim(), description),
        MarkerKind::State(ChangeState::Open),
        &[],
        tip.as_deref(),
        author,
    )
}

/// Close `branch`'s change, keeping the branch: no work is lost, and it can be reopened (W6).
pub fn close_change(repo_path: &Path, branch: &str, reason: &str, author: &Actor) -> Result<(), GitError> {
    check_branch_name(branch)?;
    let tip = change_tip(repo_path, branch)?;
    match read_change(repo_path, branch)? {
        Some(change) if change.state == ChangeState::Open => {}
        Some(_) => return Err(GitError(format!("{branch:?} is not open for review"))),
        None => return Err(GitError(format!("{branch:?} was never opened for review"))),
    }
    let reason = match reason.trim() {
        "" => NO_REASON,
        reason => reason,
    };
    write_marker(
        repo_path,
        branch,
        reason,
        MarkerKind::State(ChangeState::Closed),
        &[],
        tip.as_deref(),
        author,
    )
}

/// Rename `branch`'s change: a new title and description, in place of the ones it was opened with. Only an
/// open change can be renamed -- the mirror image of `open_change`'s guard -- and the rename is just another
/// `Open` marker on top of the chain, which `read_change` already prefers.
pub fn retitle_change(
    repo_path: &Path,
    branch: &str,
    title: &str,
    description: &str,
    author: &Actor,
) -> Result<(), GitError> {
    check_branch_name(branch)?;
    if title.trim().is_empty() {
        return Err(GitError(
            "a change needs a title: it is what the deploy commit is called".into(),
        ));
    }
    let tip = change_tip(repo_path, branch)?;
    match read_change(repo_path, branch)? {
        Some(change) if change.state == ChangeState::Open => {}
        Some(_) => return Err(GitError(format!("{branch:?} is not open for review"))),
        None => return Err(GitError(format!("{branch:?} was never opened for review"))),
    }
    write_marker(
        repo_path,
        branch,
        &titled(title.trim(), description),
        MarkerKind::State(ChangeState::Open),
        &[],
        tip.as_deref(),
        author,
    )
}

/// Say something about `branch`'s change. A closed change takes comments too: a reason is often answered.
///
/// A comment may be anchored to what it is about, and records the branch's head it was written against. A reply
/// names the comment that started its thread, and is about whatever that one is about, so it carries no anchor of
/// its own.
pub fn comment_on_change(
    repo_path: &Path,
    branch: &str,
    body: &str,
    anchor: Option<Anchor>,
    reply_to: Option<&str>,
    author: &Actor,
) -> Result<(), GitError> {
    check_branch_name(branch)?;
    let body = body.trim();
    if body.is_empty() {
        return Err(GitError("a comment needs something said in it".into()));
    }
    let tip = change_tip(repo_path, branch)?;
    if read_change(repo_path, branch)?.is_none() {
        return Err(GitError(format!("{branch:?} was never opened for review")));
    }
    let mut trailers = Vec::new();
    if let Some(anchor) = &anchor {
        if reply_to.is_some() {
            return Err(GitError(
                "a reply is about what its thread is about, so it takes no anchor".into(),
            ));
        }
        anchor.check()?;
        trailers.push((ANCHOR_TRAILER, anchor.trailer()));
    }
    let root = match reply_to {
        Some(root) => Some(thread_root(&read_markers(repo_path, branch)?, branch, root)?.id.clone()),
        None => None,
    };
    // A plain comment is written as it always was.
    if (anchor.is_some() || root.is_some())
        && let Some(head) = branch_tip(repo_path, branch)
    {
        trailers.push((AT_TRAILER, head));
    }
    trailers.extend(root.map(|root| (REPLY_TO_TRAILER, root)));
    write_marker(
        repo_path,
        branch,
        body,
        MarkerKind::Comment,
        &trailers,
        tip.as_deref(),
        author,
    )
}

/// The comment `id` names on `branch`'s chain, when it started a thread.
fn thread_root<'a>(markers: &'a [Marker], branch: &str, id: &str) -> Result<&'a Marker, GitError> {
    check_commit_id(id)?;
    match markers.iter().find(|m| m.id == id.to_ascii_lowercase()) {
        Some(m) if m.kind == MarkerKind::Comment && m.reply_to.is_none() => Ok(m),
        Some(m) if m.kind == MarkerKind::Comment => Err(GitError(format!(
            "{id} is a reply: a thread is answered at the comment that started it"
        ))),
        Some(_) => Err(GitError(format!("{id} is not a comment"))),
        None => Err(GitError(format!("there is no comment {id} on {branch:?}'s change"))),
    }
}

/// Mark the thread `root` started settled, or open it again. Either is refused when the thread already is.
pub fn resolve_thread(
    repo_path: &Path,
    branch: &str,
    root: &str,
    resolved: bool,
    author: &Actor,
) -> Result<(), GitError> {
    check_branch_name(branch)?;
    let tip = change_tip(repo_path, branch)?;
    let markers = read_markers(repo_path, branch)?;
    let root = thread_root(&markers, branch, root)?.id.clone();
    if thread_resolved(&markers, &root) == resolved {
        return Err(GitError(match resolved {
            true => "this thread is already resolved".into(),
            false => "this thread is not resolved".into(),
        }));
    }
    let kind = if resolved {
        MarkerKind::Resolved
    } else {
        MarkerKind::Unresolved
    };
    write_marker(
        repo_path,
        branch,
        "",
        kind,
        &[(REPLY_TO_TRAILER, root)],
        tip.as_deref(),
        author,
    )
}

/// Whether the thread `root` started is resolved, by the newest of `markers` (newest first) that says.
fn thread_resolved(markers: &[Marker], root: &str) -> bool {
    markers
        .iter()
        .find(|m| {
            matches!(m.kind, MarkerKind::Resolved | MarkerKind::Unresolved) && m.reply_to.as_deref() == Some(root)
        })
        .is_some_and(|m| m.kind == MarkerKind::Resolved)
}

/// Approve `branch`'s change or ask for changes to it, as `author`. Only an open change is reviewed, and not by
/// whoever opened it. Asking for changes says what to change; an approval may say nothing.
pub fn review_change(
    repo_path: &Path,
    branch: &str,
    verdict: Verdict,
    body: &str,
    author: &Actor,
) -> Result<(), GitError> {
    check_branch_name(branch)?;
    let body = body.trim();
    if verdict == Verdict::ChangesRequested && body.is_empty() {
        return Err(GitError("asking for changes needs to say what to change".into()));
    }
    let tip = change_tip(repo_path, branch)?;
    let change = match read_change(repo_path, branch)? {
        Some(change) if change.state == ChangeState::Open => change,
        Some(_) => return Err(GitError(format!("{branch:?} is not open for review"))),
        None => return Err(GitError(format!("{branch:?} was never opened for review"))),
    };
    // An empty id is an identity no account owns -- the leader's own, or one a marker could not name -- and two of
    // those are not the same person.
    if !change.author.id.is_empty() && change.author.id == author.id {
        return Err(GitError("you opened this change, so somebody else reviews it".into()));
    }
    write_marker(
        repo_path,
        branch,
        body,
        MarkerKind::Verdict(verdict),
        &[],
        tip.as_deref(),
        author,
    )
}

/// One thing that happened to a change, as its timeline shows it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimelineEvent {
    /// The marker's commit. `None` for a save, whose commit is in `EventKind::Saved`.
    pub id: Option<String>,
    /// Unix seconds.
    pub time: i64,
    pub author: Actor,
    pub kind: EventKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EventKind {
    Opened {
        title: String,
        description: String,
    },
    /// An open marker on top of an open change.
    Renamed {
        title: String,
        description: String,
    },
    /// An open marker on top of a closed change.
    Reopened {
        title: String,
        description: String,
    },
    Closed {
        reason: String,
    },
    Comment {
        body: String,
        /// What it is about. A reply's is its thread's.
        anchor: Option<Anchor>,
        /// The branch's head when it was written.
        at: Option<String>,
        /// The comment that started its thread, on a reply.
        reply_to: Option<String>,
        /// Whether the file holding the thread's anchor changed between the thread's start and the branch's head.
        outdated: bool,
    },
    Resolved {
        reply_to: String,
    },
    Unresolved {
        reply_to: String,
    },
    Approved {
        body: String,
    },
    ChangesRequested {
        body: String,
    },
    /// A commit on the branch that `main` does not have.
    Saved {
        commit: String,
        /// The commit it was made on: what the save changed is the difference from it.
        parent: Option<String>,
        subject: String,
    },
}

/// Everything that happened to `branch`'s change, oldest first: its markers, and the saves on the branch, in the
/// order they happened. A branch never opened for review is refused.
///
/// Commit times are whole seconds, so a save and a marker can share one. The save goes first: a change is opened
/// on what was already saved, and a comment answers what it can see.
pub fn change_timeline(repo_path: &Path, branch: &str) -> Result<Vec<TimelineEvent>, GitError> {
    check_branch_name(branch)?;
    let mut markers = read_markers(repo_path, branch)?;
    if markers.iter().all(|m| m.state().is_none()) {
        return Err(GitError(format!("{branch:?} was never opened for review")));
    }
    markers.reverse();

    // A thread's anchor, and whether it is outdated, are its root's.
    let head = branch_tip(repo_path, branch);
    let mut threads: std::collections::HashMap<String, (Option<Anchor>, bool)> = std::collections::HashMap::new();
    for root in markers
        .iter()
        .filter(|m| m.kind == MarkerKind::Comment && m.reply_to.is_none())
    {
        let outdated = match (&root.anchor, &root.at, &head) {
            (Some(anchor), Some(at), Some(head)) => changed_since(repo_path, anchor, at, head),
            _ => false,
        };
        threads.insert(root.id.clone(), (root.anchor.clone(), outdated));
    }

    let mut last_state = None;
    let marked = markers.into_iter().filter_map(|marker| {
        let kind = match marker.kind {
            MarkerKind::State(ChangeState::Open) => {
                let (title, description) = (marker.subject().to_string(), marker.description());
                match last_state {
                    None => EventKind::Opened { title, description },
                    Some(ChangeState::Open) => EventKind::Renamed { title, description },
                    Some(ChangeState::Closed) => EventKind::Reopened { title, description },
                }
            }
            MarkerKind::State(ChangeState::Closed) => EventKind::Closed {
                reason: marker.subject().to_string(),
            },
            MarkerKind::Comment => {
                let thread = marker.reply_to.as_ref().unwrap_or(&marker.id);
                let (anchor, outdated) = threads.get(thread).cloned().unwrap_or((None, false));
                EventKind::Comment {
                    body: marker.text.trim().to_string(),
                    anchor,
                    at: marker.at.clone(),
                    reply_to: marker.reply_to.clone(),
                    outdated,
                }
            }
            // One that names no thread says nothing anybody can be shown.
            MarkerKind::Resolved => EventKind::Resolved {
                reply_to: marker.reply_to.clone()?,
            },
            MarkerKind::Unresolved => EventKind::Unresolved {
                reply_to: marker.reply_to.clone()?,
            },
            MarkerKind::Verdict(Verdict::Approved) => EventKind::Approved {
                body: marker.text.trim().to_string(),
            },
            MarkerKind::Verdict(Verdict::ChangesRequested) => EventKind::ChangesRequested {
                body: marker.text.trim().to_string(),
            },
        };
        last_state = marker.state().or(last_state);
        Some(TimelineEvent {
            id: Some(marker.id),
            time: marker.time,
            author: marker.author,
            kind,
        })
    });

    let mut saved = saves(repo_path, branch)?.into_iter().peekable();
    let mut events = Vec::new();
    for event in marked {
        while let Some(save) = saved.next_if(|s| s.time <= event.time) {
            events.push(save);
        }
        events.push(event);
    }
    events.extend(saved);
    Ok(events)
}

/// Whether the file holding `anchor` differs between `at` and `head`. A commit that cannot be read counts as a
/// change: the comment can no longer be shown against what it was written about.
fn changed_since(repo_path: &Path, anchor: &Anchor, at: &str, head: &str) -> bool {
    let Ok(file) = anchor.file() else {
        return true;
    };
    let paths = [file];
    match (read_blobs(repo_path, at, &paths), read_blobs(repo_path, head, &paths)) {
        (Ok(then), Ok(now)) => then != now,
        _ => true,
    }
}

/// The commits on `branch` that `main` does not have, oldest first.
fn saves(repo_path: &Path, branch: &str) -> Result<Vec<TimelineEvent>, GitError> {
    let repo = open(repo_path)?;
    let tip = resolve(&repo, branch)?;
    let head = resolve(&repo, MAIN_BRANCH)?;
    let failed = |e: String| GitError(format!("reading {branch:?}'s saves: {e}"));
    let mut saves = Vec::new();
    for info in repo
        .rev_walk([tip])
        .with_hidden([head])
        .all()
        .map_err(|e| failed(e.to_string()))?
    {
        let info = info.map_err(|e| failed(e.to_string()))?;
        let commit = repo.find_commit(info.id).map_err(|e| failed(e.to_string()))?;
        let message = commit.message_raw().map_err(|e| failed(e.to_string()))?.to_string();
        let author = commit.author().map_err(|e| failed(e.to_string()))?;
        saves.push(TimelineEvent {
            id: None,
            time: commit.time().map_err(|e| failed(e.to_string()))?.seconds,
            author: actor_of(author.name.to_string(), author.email.to_string()),
            kind: EventKind::Saved {
                commit: info.id.to_string(),
                parent: commit.parent_ids().next().map(|p| p.to_string()),
                subject: message.lines().next().unwrap_or_default().to_string(),
            },
        });
    }
    saves.reverse();
    Ok(saves)
}

/// Forget a change entirely, which is what deleting its branch means.
pub fn delete_change(repo_path: &Path, branch: &str) -> Result<(), GitError> {
    check_branch_name(branch)?;
    delete_ref(&open(repo_path)?, &change_ref(branch), None)
}

// -- deploying -------------------------------------------------------------

mod merge;
pub use merge::{BringCurrent, BroughtCurrent, Choices, Conflict, Conflicts, Pick, bring_current};

/// Every group in `tree` that does not load, and why, in the tree's order. Empty when every group it holds runs.
///
/// Every group, not only the one being edited: a merge or a deploy carries whatever else moved on `main`, and a
/// bundle that will not load is one no worker in that group can run. A caller that refuses names the first and says
/// what did not happen as a result, because that differs -- a merge leaves the branch alone, a deploy leaves `main`
/// alone.
pub fn groups_that_do_not_load(repo_path: &Path, tree: &str) -> Result<Vec<(String, String)>, GitError> {
    Ok(super::tree::groups_at(repo_path, tree)?
        .into_iter()
        .filter_map(|group| {
            let why = super::tree::load_group_at(repo_path, tree, &group).err()?.to_string();
            Some((group, why))
        })
        .collect())
}

/// A commit id cut to the length a person reads it at.
pub fn short(commit: &str) -> &str {
    &commit[..commit.len().min(8)]
}

/// The paths a merge could not settle. A merge reports what it resolved as well, so only what git itself would
/// leave for a person counts, and a path is named once however many stages it appears at.
fn conflicted_paths(merged: &gix::merge::tree::Outcome<'_>) -> Vec<String> {
    let unresolved = gix::merge::tree::TreatAsUnresolved::git();
    let mut paths: Vec<String> = Vec::new();
    for conflict in merged.conflicts.iter().filter(|c| c.is_unresolved(unresolved)) {
        for change in [&conflict.ours, &conflict.theirs] {
            let path = change.location().to_string();
            if !paths.contains(&path) {
                paths.push(path);
            }
        }
    }
    paths
}

/// Deploy a change: `main` takes the branch as one commit, the fleet is pointed at it, and the branch goes.
///
/// Returns the commit the fleet will pick up on its next fetch.
pub fn deploy_change(data_dir: &Path, branch: &str, author: &Actor) -> Result<String, GitError> {
    deploy_change_from(data_dir, branch, None, author)
}

/// [`deploy_change`], of `branch` while it is at `tip` and refused once it has moved: what a caller judged before
/// deploying is what deploys, though a save takes no lock and can land in between.
pub fn deploy_change_at(data_dir: &Path, branch: &str, tip: &str, author: &Actor) -> Result<String, GitError> {
    deploy_change_from(data_dir, branch, Some(tip), author)
}

fn deploy_change_from(data_dir: &Path, branch: &str, tip: Option<&str>, author: &Actor) -> Result<String, GitError> {
    let squash = squash_change_from(data_dir, branch, tip, author)?;
    deploy_squash(data_dir, &squash, author)?;
    Ok(squash.commit)
}

/// A change squashed onto `main`'s head, every check of an approval passed, and nothing moved yet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Squash {
    pub branch: String,
    pub title: String,
    /// The squash commit, whose one parent is `head`.
    pub commit: String,
    /// `main`'s head it was squashed onto.
    pub head: String,
    /// The branch's tip that was squashed. A save since is not part of the squash, and keeps the branch.
    pub tip: String,
}

/// Squash `branch` onto `main` as Approve would deploy it, refusing what Approve refuses, and move nothing: the commit
/// is written but no ref names it until [`deploy_squash`].
pub fn squash_change(data_dir: &Path, branch: &str, author: &Actor) -> Result<Squash, GitError> {
    squash_change_from(data_dir, branch, None, author)
}

/// [`squash_change`] of `branch` while it is at `tip`, refused once it has moved: what a caller judged is what is
/// squashed.
pub fn squash_change_at(data_dir: &Path, branch: &str, tip: &str, author: &Actor) -> Result<Squash, GitError> {
    squash_change_from(data_dir, branch, Some(tip), author)
}

/// [`squash_change`] of the tip `branch` is at, or with `expected` -- a whole commit id -- of that tip, refused when
/// `branch` has moved from it. Every check reads that one tip, so a save landing meanwhile cannot slip past them.
fn squash_change_from(
    data_dir: &Path,
    branch: &str,
    expected: Option<&str>,
    author: &Actor,
) -> Result<Squash, GitError> {
    check_branch_name(branch)?;
    if branch == MAIN_BRANCH {
        return Err(GitError(format!(
            "{MAIN_BRANCH} is what a deploy moves, not what it deploys"
        )));
    }
    let repo = repo_path(data_dir);
    let Some(tip) = branch_tip(&repo, branch) else {
        return Err(GitError(format!("there is no branch {branch:?}")));
    };
    if expected.is_some_and(|expected| expected != tip) {
        return Err(GitError(format!(
            "{branch:?} moved while this was being deployed -- look at it again"
        )));
    }

    let Some(change) = read_change(&repo, branch)? else {
        return Err(GitError(format!("{branch:?} was never opened for review")));
    };
    if change.state != ChangeState::Open {
        return Err(GitError(format!("{branch:?} is not open for review")));
    }

    let (ahead, behind) = ahead_behind_in(&open(&repo)?, &tip)?;
    if ahead == 0 {
        return Err(GitError(format!("{branch:?} changes nothing")));
    }
    if behind > 0 {
        // Merging is deploying, so a review against a `main` that has moved would put untested config in front of
        // live data (W4).
        return Err(GitError(format!(
            "{MAIN_BRANCH} has moved on by {behind} commit{} since this change was opened -- bring it current first",
            if behind == 1 { "" } else { "s" }
        )));
    }

    let head = main_head(&repo).ok_or_else(|| GitError(format!("{MAIN_BRANCH} has no commit")))?;
    // The branch is not behind `main`, so what a squash merge would produce is the tree the branch already holds,
    // less its left-out samples.
    let tree = deploying_tree(&repo, &head, &tip)?;
    if tree == tree_of(&repo, &head)? {
        return Err(GitError(format!(
            "{branch:?} makes no difference to {MAIN_BRANCH} -- there is nothing to deploy"
        )));
    }

    // What the fleet runs has to run. A branch can hold a commit smeltr did not make (design doc, Config plane), and
    // two changes that each loaded alone can still leave `main` naming what the other removed.
    if let Some(why) = worse_than_running(&repo, &tree)? {
        return Err(GitError(format!(
            "deploying {branch:?} would break what runs: {why}. Nothing was deployed"
        )));
    }

    // The squash is the only commit that survives: the branch and everything authored on it goes below. So it
    // carries both people -- the author in git's own author field, where `git log` and every tool that reads one
    // will find them, and the deployer in a trailer. An author who deploys their own change is both, and the
    // history says so rather than leaving a reader to infer it.
    let deployed_by = format!("{}{}", DEPLOYED_BY, signature_line(author));
    let mut messages = vec![change.title.as_str()];
    if !change.description.is_empty() {
        messages.push(&change.description);
    }
    messages.push(&deployed_by);
    let commit = commit_tree(&repo, &tree, &[&head], &messages, &change.author, Ordered::AfterParents)?;
    Ok(Squash {
        branch: branch.to_string(),
        title: change.title,
        commit,
        head,
        tip,
    })
}

/// Of `changed`, the files a branch at `tip` changes, each with whether it was removed: the samples no test beside
/// them names, by path. A deploy leaves them out. A sample removed is never left out, and a bundle holding a test
/// that does not read leaves none out, since what that test names cannot be known.
pub fn left_out_samples<'a>(
    repo_path: &Path,
    tip: &str,
    changed: impl IntoIterator<Item = (&'a str, bool)>,
) -> std::collections::BTreeSet<String> {
    let mut named: std::collections::HashMap<&str, Option<std::collections::HashSet<String>>> =
        std::collections::HashMap::new();
    let mut left_out = std::collections::BTreeSet::new();
    for (path, removed) in changed {
        // The bundle a sample is in: `groups/<id>/`, `shared/`, or the root's empty one.
        let bundle = path
            .rsplit_once('/')
            .and_then(|(dir, _)| dir.strip_suffix(crate::config_file::SAMPLES_DIR))
            .filter(|bundle| !removed && (bundle.is_empty() || bundle.ends_with('/')));
        let Some(bundle) = bundle else {
            continue;
        };
        let named = named.entry(bundle).or_insert_with(|| {
            let (tests, unreadable) =
                crate::config_file::list_tests(&mut super::tree::TreeFiles::new(repo_path, tip, bundle));
            unreadable.is_empty().then(|| {
                tests
                    .iter()
                    .filter_map(|test| crate::config_file::sample_path(&test.sample).ok())
                    .map(|within| format!("{bundle}{within}"))
                    .collect()
            })
        });
        if named.as_ref().is_some_and(|named| !named.contains(path)) {
            left_out.insert(path.to_string());
        }
    }
    left_out
}

/// `tip`'s tree with each of its left-out samples ([`left_out_samples`]) as `base` holds it: what deploying a branch
/// at `tip` onto `base` puts on `main`.
fn deploying_tree(repo_path: &Path, base: &str, tip: &str) -> Result<String, GitError> {
    let git = open(repo_path)?;
    let changed: Vec<(String, bool)> = tree_changes(&git, resolve(&git, base)?, resolve(&git, tip)?)?
        .iter()
        .map(|change| {
            let removed = matches!(change, gix::object::tree::diff::ChangeDetached::Deletion { .. });
            (change.location().to_string(), removed)
        })
        .collect();
    let left_out: Vec<String> = left_out_samples(
        repo_path,
        tip,
        changed.iter().map(|(path, removed)| (path.as_str(), *removed)),
    )
    .into_iter()
    .collect();
    if left_out.is_empty() {
        return Ok(tree_at(&git, tip)?.to_string());
    }
    let edits: Vec<FileEdit> = left_out
        .iter()
        .cloned()
        .zip(read_blobs(repo_path, base, &left_out)?)
        .map(|(path, content)| FileEdit { path, content })
        .collect();
    let (_, tree) = tree_with(&git, tip, &edits)?;
    Ok(tree.to_string())
}

/// Move `main` and the fleet to `squash` and delete its branch: the rest of an approval.
pub fn deploy_squash(data_dir: &Path, squash: &Squash, author: &Actor) -> Result<(), GitError> {
    let (repo, branch) = (repo_path(data_dir), squash.branch.as_str());
    // A rollback holds the fleet through an approval: only a release ends it.
    let git = open(&repo)?;
    let (new, old_head) = (resolve(&git, &squash.commit)?, resolve(&git, &squash.head)?);
    let running = deployed_in(&git);
    let moved = |_| {
        GitError(format!(
            "{MAIN_BRANCH} moved while this was being deployed -- bring {branch:?} current and try again"
        ))
    };
    match running.filter(|_| rolled_back(&git)) {
        Some(held) => move_main_under(&git, new, Some(old_head), held).map_err(moved)?,
        None => deploy_in(
            &git,
            new,
            running,
            Main::MovesFrom(old_head),
            &squash.title,
            false,
            author,
        )
        .map_err(moved)?,
    }

    // Approval merges and deletes the branch: what it held is on `main` now, under one commit (W5). The deploy has
    // already happened by here -- workers take it on their next heartbeat -- so a branch that will not go is
    // reported by it staying on the list rather than by an error that reads as "it did not deploy".
    let _ = delete_branch(data_dir, branch, Some(&squash.tip));
    Ok(())
}

/// A commit id as it may be given to us: hexadecimal, abbreviated or whole. Anything else is refused rather than
/// handed to git, where a rev can name far more than a commit.
pub fn check_commit_id(commit: &str) -> Result<(), GitError> {
    if (7..=40).contains(&commit.len()) && commit.chars().all(|c| c.is_ascii_hexdigit()) {
        return Ok(());
    }
    Err(GitError(format!(
        "{commit:?} is not a commit id: 7 to 40 hexadecimal characters"
    )))
}

/// Point the fleet at a commit already in `main`'s history. Behind `main`'s head this is a rollback, and the fleet
/// stays there -- held -- until `main`'s head is deployed again ([`hold`]); `main`'s head itself is the release.
///
/// Nothing is committed: the pointer moves, and the deploy log records who moved it, so `main`'s history stays the
/// history of what was written (W7).
///
/// Nothing is checked about what `commit` holds: a way back has to stay open when the way back is broken too.
/// [`deploying_would_not_load`] is the check a caller asks first.
pub fn deploy_commit(data_dir: &Path, commit: &str, author: &Actor) -> Result<String, GitError> {
    let repo = repo_path(data_dir);
    check_deployable(&repo, commit)?;

    let git = open(&repo)?;
    let target = resolve(&git, commit)?;
    let short = short(commit);
    let running = deployed_in(&git);
    if running == Some(target) {
        return Err(GitError(format!("{short} is already what is deployed")));
    }
    let release = Some(target.to_string()) == main_head(&repo);
    let subject = if release {
        format!("Deploy {short}, {MAIN_BRANCH}'s head")
    } else {
        format!("{ROLLED_BACK_SUBJECT}{short}: {}", commit_subject(&repo, commit)?)
    };
    deploy_in(&git, target, running, Main::Stays, &subject, !release, author)
        .map_err(|_| GitError(format!("the fleet moved while {short} was being deployed -- try again")))?;
    Ok(target.to_string())
}

/// Every group that would not load if `commit` were deployed with [`deploy_commit`], and why. A commit
/// [`deploy_commit`] would refuse anyway is refused here the same way.
pub fn deploying_would_not_load(data_dir: &Path, commit: &str) -> Result<Vec<(String, String)>, GitError> {
    let repo = repo_path(data_dir);
    check_deployable(&repo, commit)?;
    groups_that_do_not_load(&repo, commit)
}

fn check_deployable(repo: &Path, commit: &str) -> Result<(), GitError> {
    check_commit_id(commit)?;
    if !is_ancestor(repo, commit, MAIN_BRANCH)? {
        return Err(GitError(format!(
            "{commit} is not in {MAIN_BRANCH}'s history -- only something that has been deployed can be deployed again"
        )));
    }
    Ok(())
}

// -- the deployed pointer ---------------------------------------------------
//
// What workers fetch is [`DEPLOYED_REF`], not `main`: `main` is what was written, and the pointer is what runs. They
// differ only while a rollback holds the fleet behind `main`'s head. Every move of the pointer adds a marker to
// [`DEPLOY_LOG_REF`], the same kind of marker chain a change keeps, so who moved it and when is the leader's own
// record and never a claim a pushed commit could make.

/// Why the fleet does not run `commit` once `main` is there: the first group that does not load at `commit` but does
/// where the fleet runs, or is not there at all. `None` when deploying it would break nothing that runs now. The rule
/// Approve keeps, so a fix to one group lands while another is still broken.
pub fn worse_than_running(repo_path: &Path, commit: &str) -> Result<Option<String>, GitError> {
    let broken_now = match deployed(repo_path) {
        Some(running) => groups_that_do_not_load(repo_path, &running)?,
        None => Vec::new(),
    };
    Ok(groups_that_do_not_load(repo_path, commit)?
        .into_iter()
        .find(|(group, _)| !broken_now.iter().any(|(was, _)| was == group))
        .map(|(group, why)| format!("group {group:?} does not load at {}: {why}", short(commit))))
}

/// What the leader knows of each branch on the remote it follows, under this prefix: `main` as last fetched, and every
/// other branch as last pushed (`leader::remote`). Hidden from workers.
pub const HOST_REFS: &str = "refs/remotes/origin/";

/// Where the `main` of the remote this leader follows lands when fetched (`leader::remote`).
pub const FETCHED_MAIN_REF: &str = "refs/remotes/origin/main";

/// The commit the fleet runs. Served to workers, so it sits outside the namespace `http_backend` hides.
pub const DEPLOYED_REF: &str = "refs/smeltr/deployed";

/// One marker per move of [`DEPLOYED_REF`], newest last. Hidden from workers: it is the leader's bookkeeping.
const DEPLOY_LOG_REF: &str = "refs/smeltr/deploys";

/// The trailer naming the commit a deploy-log marker moved the fleet to.
const DEPLOYED_TRAILER: &str = "Smeltr-Deployed:";

/// The trailer on a deploy-log marker that holds the fleet where it put it: a rollback. Only a release ends it.
const HOLD_TRAILER: &str = "Smeltr-Hold: yes";

/// The subject a rollback's marker had before [`HOLD_TRAILER`], which marks the same thing in a log written then.
const ROLLED_BACK_SUBJECT: &str = "Roll back to ";

fn deployed_in(repo: &gix::Repository) -> Option<gix::ObjectId> {
    Some(
        repo.find_reference(DEPLOYED_REF)
            .ok()?
            .into_fully_peeled_id()
            .ok()?
            .detach(),
    )
}

/// The commit the fleet runs. `None` before anything has been deployed.
pub fn deployed(repo_path: &Path) -> Option<String> {
    deployed_in(&open(repo_path).ok()?).map(|id| id.to_string())
}

/// What a deploy does to `main` besides moving the fleet onto it.
enum Main {
    /// A rollback or a release: the fleet moves within `main`'s history.
    Stays,
    /// The first commit there is.
    Created,
    /// An approval: `main` moves from this commit to the one deployed.
    MovesFrom(gix::ObjectId),
}

/// Move the fleet to `new` while it still runs `running`, writing the marker that says who did it, and move `main`
/// as `main` says. One transaction: the fleet, its log and `main` move together or not at all, each by
/// compare-and-swap.
fn deploy_in(
    repo: &gix::Repository,
    new: gix::ObjectId,
    running: Option<gix::ObjectId>,
    main: Main,
    subject: &str,
    hold: bool,
    author: &Actor,
) -> Result<(), GitError> {
    let log = repo
        .find_reference(DEPLOY_LOG_REF)
        .ok()
        .and_then(|r| r.into_fully_peeled_id().ok())
        .map(|id| id.detach());
    let empty = repo
        .write_object(gix::objs::Tree::empty())
        .map_err(|e| GitError(format!("writing the empty tree: {e}")))?
        .detach();
    let mut message = format!("{subject}\n\n{DEPLOYED_TRAILER} {new}");
    if hold {
        message = format!("{message}\n{HOLD_TRAILER}");
    }
    let marker = commit_tree_in(
        repo,
        empty,
        &log.into_iter().collect::<Vec<_>>(),
        &[&message],
        author,
        Ordered::Clock,
    )?;

    let mut edits = vec![
        swap_edit(DEPLOYED_REF, new, running)?,
        swap_edit(DEPLOY_LOG_REF, marker, log)?,
    ];
    let main_ref = format!("refs/heads/{MAIN_BRANCH}");
    match main {
        Main::Stays => {}
        Main::Created => edits.push(swap_edit(&main_ref, new, None)?),
        Main::MovesFrom(old) => edits.push(swap_edit(&main_ref, new, Some(old))?),
    }
    repo.edit_references(edits)
        .map_err(|e| GitError(format!("the fleet did not move: {e}")))?;
    forget_advertisement();
    Ok(())
}

/// `name` moved from `old` to `new`, as one edit of a transaction: refused unless it still names `old`, or does not
/// exist when `old` is `None`.
fn swap_edit(
    name: &str,
    new: gix::ObjectId,
    old: Option<gix::ObjectId>,
) -> Result<gix::refs::transaction::RefEdit, GitError> {
    use gix::refs::transaction::{Change, LogChange, PreviousValue, RefEdit};
    Ok(RefEdit {
        change: Change::Update {
            log: LogChange {
                message: "smeltr".into(),
                ..Default::default()
            },
            expected: match old {
                Some(old) => PreviousValue::MustExistAndMatch(gix::refs::Target::Object(old)),
                None => PreviousValue::MustNotExist,
            },
            new: gix::refs::Target::Object(new),
        },
        name: full_name(name)?,
        deref: false,
    })
}

/// Move `main` to `commit`, the remote's `main` as just fetched. Any commit is followed, not only one that descends
/// from `main`: the remote owns `main`, and a force-push there is followed too, so that a fix can always be approved
/// on top of it. The fleet moves with `main` unless a rollback holds it, or `commit` is [`worse_than_running`]; then
/// it stays where it was.
///
/// Every lookup file `commit` names must already be in the store; the caller fetches them first.
pub fn follow(repo_path: &Path, commit: &str) -> Result<(), GitError> {
    let short = short(commit);
    let worse = worse_than_running(repo_path, commit)?.is_some();
    let git = open(repo_path)?;
    let new = resolve(&git, commit)?;
    let head = main_head(repo_path).map(|head| resolve(&git, &head)).transpose()?;
    let running = deployed_in(&git);
    let moved = |_| {
        GitError(format!(
            "{MAIN_BRANCH} moved while {short} was being followed -- the next sync tries again"
        ))
    };
    let main = match head {
        Some(head) => Main::MovesFrom(head),
        None => Main::Created,
    };
    // A fleet held only because the last `main` was worse moves on with the next one that is not.
    if !worse && (running.is_none() || running == head || !rolled_back(&git)) {
        let subject = format!("Follow the remote's {MAIN_BRANCH} to {short}");
        return deploy_in(&git, new, running, main, &subject, false, &Actor::system()).map_err(moved);
    }
    let Some(held) = running else {
        return Err(GitError(format!(
            "the remote's {MAIN_BRANCH} at {short} does not load, and nothing is deployed yet to hold the fleet on"
        )));
    };
    move_main_under(&git, new, head, held).map_err(moved)
}

/// Move `main` from `head` to `new` and leave the fleet on `held`. The pointer is part of the swap, unmoved, so a
/// Release landing meanwhile fails this rather than leaving the fleet held behind a `main` it was released onto.
fn move_main_under(
    git: &gix::Repository,
    new: gix::ObjectId,
    head: Option<gix::ObjectId>,
    held: gix::ObjectId,
) -> Result<(), GitError> {
    git.edit_references([
        swap_edit(&format!("refs/heads/{MAIN_BRANCH}"), new, head)?,
        swap_edit(DEPLOYED_REF, held, Some(held))?,
    ])
    .map_err(|e| GitError(e.to_string()))?;
    forget_advertisement();
    Ok(())
}

/// Whether the last move of the fleet was a rollback, which holds it until a release.
fn rolled_back(git: &gix::Repository) -> bool {
    let Some(marker) = git
        .find_reference(DEPLOY_LOG_REF)
        .ok()
        .and_then(|r| r.into_fully_peeled_id().ok())
        .and_then(|id| id.object().ok())
    else {
        return false;
    };
    let message = String::from_utf8_lossy(&marker.data);
    let message = message.split_once("\n\n").map_or("", |(_, message)| message);
    message.starts_with(ROLLED_BACK_SUBJECT) || message.lines().any(|line| line == HOLD_TRAILER)
}

/// The fleet held behind `main`'s head by a rollback.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hold {
    /// What the fleet runs.
    pub commit: String,
    /// `main`'s head, which it does not.
    pub head: String,
    /// Commits on `main` the fleet does not run.
    pub behind: u32,
    /// Who moved the fleet there, and when, unix seconds.
    pub by: Actor,
    pub since: i64,
    /// A rollback put it there, so only a release ends it. Otherwise `main`'s head would break what runs, and the
    /// fleet moves on with the first `main` that does not.
    pub rolled_back: bool,
}

/// The hold the fleet is under, or `None` when it runs `main`'s head.
pub fn hold(repo_path: &Path) -> Result<Option<Hold>, GitError> {
    let repo = open(repo_path)?;
    let failed = |e: String| GitError(format!("reading the deploy log: {e}"));
    let log_tip = || -> Option<gix::ObjectId> {
        Some(
            repo.find_reference(DEPLOY_LOG_REF)
                .ok()?
                .into_fully_peeled_id()
                .ok()?
                .detach(),
        )
    };
    let main = || -> Option<gix::ObjectId> {
        Some(
            repo.find_reference(&format!("refs/heads/{MAIN_BRANCH}"))
                .ok()?
                .into_fully_peeled_id()
                .ok()?
                .detach(),
        )
    };
    // Three refs read one after another. A deploy moves them in one transaction, so a read that spans one is read
    // again rather than answered with a mix of before and after.
    loop {
        let (Some(marker), Some(running), Some(head)) = (log_tip(), deployed_in(&repo), main()) else {
            return Ok(None);
        };
        let unmoved = || log_tip() == Some(marker) && deployed_in(&repo) == Some(running) && main() == Some(head);
        if running == head {
            if unmoved() {
                return Ok(None);
            }
            continue;
        }
        let behind = repo
            .rev_walk([head])
            .with_hidden([running])
            .all()
            .map_err(|e| failed(e.to_string()))?
            .count() as u32;
        let marker_commit = repo.find_commit(marker).map_err(|e| failed(e.to_string()))?;
        let author = marker_commit.author().map_err(|e| failed(e.to_string()))?;
        let hold = Hold {
            commit: running.to_string(),
            head: head.to_string(),
            behind,
            by: actor_of(author.name.to_string(), author.email.to_string()),
            since: marker_commit.time().map_err(|e| failed(e.to_string()))?.seconds,
            rolled_back: rolled_back(&repo),
        };
        if unmoved() {
            return Ok(Some(hold));
        }
    }
}

// -- reading branches and history -----------------------------------------
//
// Read-only, against the bare repository: a branch is a ref, and nothing here needs a checkout. Every one of these
// takes a branch name through `check_branch_name` first, because the name reaches git as an argument.

/// A branch as the GUI lists it. `ahead` and `behind` are counted against `main`, which is what makes a branch work
/// in flight rather than a name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BranchSummary {
    pub name: String,
    pub head: String,
    /// Committer date of the tip, in unix seconds: the last save.
    pub last_save: i64,
    /// Commits the branch has that `main` does not.
    pub ahead: u32,
    /// Commits `main` has that the branch does not.
    pub behind: u32,
}

/// One file's difference between two revisions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileChange {
    pub path: String,
    /// `A`, `M` or `D`. Rename detection is off, so a move is an add and a delete -- which is what a bundle's
    /// one-file-per-pipeline layout means anyway.
    pub status: String,
    pub added: u32,
    pub removed: u32,
}

/// One commit, as a history entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitSummary {
    pub commit: String,
    pub subject: String,
    pub body: String,
    /// Committer date, unix seconds.
    pub time: i64,
    pub files: Vec<String>,
    /// Who wrote it, as they were called then. `smeltr-leader` for a commit no person asked for, and for every
    /// commit written before smeltr recorded an author.
    pub author: String,
    /// The account behind that name, taken back out of the synthetic address. `None` for a commit whose author is
    /// not one, which is what makes "mine" answerable without matching on display names.
    pub author_id: Option<String>,
    /// Who deployed it, where the commit says. `None` for anything but a deploy, where there is only one person.
    pub deployer: Option<String>,
    pub deployer_id: Option<String>,
}

/// Every branch, `main` included. Callers filter: `main` is not editable, but it is listed where a diff needs it.
pub fn list_branches(repo_path: &Path) -> Result<Vec<BranchSummary>, GitError> {
    let repo = open(repo_path)?;
    let failed = |e: String| GitError(format!("listing branches: {e}"));
    let mut branches = Vec::new();
    for reference in repo
        .references()
        .map_err(|e| failed(e.to_string()))?
        .prefixed("refs/heads/")
        .map_err(|e| failed(e.to_string()))?
    {
        let mut reference = reference.map_err(|e| failed(e.to_string()))?;
        let name = reference.name().shorten().to_string();
        let head = reference.peel_to_id().map_err(|e| failed(e.to_string()))?.detach();
        let last_save = repo
            .find_commit(head)
            .map_err(|e| failed(e.to_string()))?
            .time()
            .map_err(|e| failed(e.to_string()))?
            .seconds;
        let (ahead, behind) = ahead_behind_in(&repo, &name)?;
        branches.push(BranchSummary {
            name,
            head: head.to_string(),
            last_save,
            ahead,
            behind,
        });
    }
    Ok(branches)
}

/// How far `branch` has moved from `main`, and `main` from it. `(0, 0)` for `main` itself.
pub fn ahead_behind(repo_path: &Path, branch: &str) -> Result<(u32, u32), GitError> {
    check_branch_name(branch)?;
    ahead_behind_in(&open(repo_path)?, branch)
}

fn ahead_behind_in(repo: &gix::Repository, branch: &str) -> Result<(u32, u32), GitError> {
    let tip = resolve(repo, branch)?;
    let head = resolve(repo, MAIN_BRANCH)?;
    // Each side is what the other cannot reach, which is `rev-list --left-right` counted one direction at a time.
    let reachable_only_from = |from: gix::ObjectId, hidden: gix::ObjectId| -> Result<u32, GitError> {
        let walk = repo
            .rev_walk([from])
            .with_hidden([hidden])
            .all()
            .map_err(|e| GitError(format!("counting {branch:?} against {MAIN_BRANCH}: {e}")))?;
        let mut count = 0;
        for info in walk {
            info.map_err(|e| GitError(format!("counting {branch:?} against {MAIN_BRANCH}: {e}")))?;
            count += 1;
        }
        Ok(count)
    };
    Ok((reachable_only_from(tip, head)?, reachable_only_from(head, tip)?))
}

/// What `branch` changes, against the `main` it branched from rather than against `main`'s tip: a commit that landed
/// on `main` afterward is not this branch's doing.
pub fn changed_files(repo_path: &Path, branch: &str) -> Result<Vec<FileChange>, GitError> {
    check_branch_name(branch)?;
    diff_files(repo_path, branch)
}

/// [`changed_files`] of `tip`, a commit a branch was at: what a caller judges and then acts on with that same tip,
/// since the branch can move between the two.
pub fn changed_files_at(repo_path: &Path, tip: &str) -> Result<Vec<FileChange>, GitError> {
    check_commit_id(tip)?;
    diff_files(repo_path, tip)
}

/// The commit `branch` left `main` at: what its changes are measured against ([`changed_files`]).
pub fn branch_base(repo_path: &Path, branch: &str) -> Result<String, GitError> {
    check_branch_name(branch)?;
    let (base, _) = since_main(&open(repo_path)?, branch)?;
    Ok(base.to_string())
}

/// Whether `commit` is in `branch`'s history, `branch`'s tip included.
pub fn reachable_from(repo_path: &Path, commit: &str, branch: &str) -> Result<bool, GitError> {
    check_commit_id(commit)?;
    check_branch_name(branch)?;
    is_ancestor(repo_path, commit, branch)
}

/// Where `branch` left `main`, and where it is now. A branch is read against the commit it started from rather
/// than against `main`'s tip: what somebody else deployed afterward is not this change's doing.
fn since_main(repo: &gix::Repository, branch: &str) -> Result<(gix::ObjectId, gix::ObjectId), GitError> {
    let tip = resolve(repo, branch)?;
    let head = resolve(repo, MAIN_BRANCH)?;
    let base = repo
        .merge_base(tip, head)
        .map_err(|e| GitError(format!("{branch:?} and {MAIN_BRANCH} share no commit: {e}")))?
        .detach();
    Ok((base, tip))
}

/// The file changes between two commits' trees, directories dropped.
fn tree_changes(
    repo: &gix::Repository,
    old: gix::ObjectId,
    new: gix::ObjectId,
) -> Result<Vec<gix::object::tree::diff::ChangeDetached>, GitError> {
    let failed = |e: String| GitError(format!("diffing {old} against {new}: {e}"));
    let old = repo
        .find_commit(old)
        .map_err(|e| failed(e.to_string()))?
        .tree()
        .map_err(|e| failed(e.to_string()))?;
    let new = repo
        .find_commit(new)
        .map_err(|e| failed(e.to_string()))?
        .tree()
        .map_err(|e| failed(e.to_string()))?;
    let mut changes = repo
        .diff_tree_to_tree(&old, &new, gix::diff::Options::default())
        .map_err(|e| failed(e.to_string()))?;
    changes.retain(|change| change.entry_mode().is_blob_or_symlink());
    Ok(changes)
}

/// What a path was and became: its mode and blob, or `None` on a side it is not on.
type Side = Option<(gix::objs::tree::EntryMode, gix::ObjectId)>;

fn sides(change: &gix::object::tree::diff::ChangeDetached) -> (Side, Side) {
    use gix::object::tree::diff::ChangeDetached as Change;
    match change {
        Change::Addition { entry_mode, id, .. } => (None, Some((*entry_mode, *id))),
        Change::Deletion { entry_mode, id, .. } => (Some((*entry_mode, *id)), None),
        Change::Modification {
            previous_entry_mode,
            previous_id,
            entry_mode,
            id,
            ..
        } => (Some((*previous_entry_mode, *previous_id)), Some((*entry_mode, *id))),
        Change::Rewrite {
            source_entry_mode,
            source_id,
            entry_mode,
            id,
            ..
        } => (Some((*source_entry_mode, *source_id)), Some((*entry_mode, *id))),
    }
}

/// The bytes on one side. An absent side is empty, which is what a diff against nothing is.
fn side_bytes(repo: &gix::Repository, side: &Side) -> Result<Vec<u8>, GitError> {
    let Some((_, id)) = side else {
        return Ok(Vec::new());
    };
    Ok(repo
        .find_object(*id)
        .map_err(|e| GitError(format!("reading {id}: {e}")))?
        .detach()
        .data)
}

/// Git's own test: a NUL anywhere in the first 8000 bytes.
fn is_binary(bytes: &[u8]) -> bool {
    bytes.iter().take(8000).any(|b| *b == 0)
}

/// The line diff of two blobs, with the slider heuristic git applies after Myers so hunks land where a reader
/// expects them.
fn line_diff<'a>(
    before: &'a [u8],
    after: &'a [u8],
) -> (gix::diff::blob::Diff, gix::diff::blob::InternedInput<&'a [u8]>) {
    let input = gix::diff::blob::InternedInput::new(before, after);
    let diff = gix::diff::blob::diff_with_slider_heuristics(gix::diff::blob::Algorithm::Myers, &input);
    (diff, input)
}

fn diff_files(repo_path: &Path, branch: &str) -> Result<Vec<FileChange>, GitError> {
    let repo = open(repo_path)?;
    let (base, tip) = since_main(&repo, branch)?;
    files_between(&repo, base, tip)
}

/// What two commits differ in, as one entry per file.
fn files_between(repo: &gix::Repository, old: gix::ObjectId, new: gix::ObjectId) -> Result<Vec<FileChange>, GitError> {
    let mut changes = Vec::new();
    for change in tree_changes(repo, old, new)? {
        let (before, after) = sides(&change);
        let status = match &change {
            gix::object::tree::diff::ChangeDetached::Addition { .. } => "A",
            gix::object::tree::diff::ChangeDetached::Deletion { .. } => "D",
            _ => "M",
        };
        let before_bytes = side_bytes(repo, &before)?;
        let after_bytes = side_bytes(repo, &after)?;
        // A binary file has no lines to count, which is the `-` git writes for both.
        let (added, removed) = if is_binary(&before_bytes) || is_binary(&after_bytes) {
            (0, 0)
        } else {
            let (diff, _) = line_diff(&before_bytes, &after_bytes);
            (diff.count_additions(), diff.count_removals())
        };
        changes.push(FileChange {
            path: change.location().to_string(),
            status: status.to_string(),
            added,
            removed,
        });
    }
    Ok(changes)
}

/// The unified diff of `branch` against where it left `main`, whole or for one path.
pub fn diff_unified(repo_path: &Path, branch: &str, path: Option<&str>) -> Result<String, GitError> {
    check_branch_name(branch)?;
    let repo = open(repo_path)?;
    let (base, tip) = since_main(&repo, branch)?;
    unified_between(&repo, base, tip, path)
}

fn unified_between(
    repo: &gix::Repository,
    old: gix::ObjectId,
    new: gix::ObjectId,
    path: Option<&str>,
) -> Result<String, GitError> {
    let mut out = String::new();
    for change in tree_changes(repo, old, new)? {
        let at = change.location().to_string();
        if path.is_some_and(|wanted| wanted != at) {
            continue;
        }
        out.push_str(&patch(repo, &change, &at)?);
    }
    Ok(out)
}

/// What landed between two commits, whichever branch they are on. A writer whose save was refused is looking at
/// somebody else's commit on the branch they share, which is neither a branch against `main` nor a deploy.
pub fn changed_between(repo_path: &Path, from: &str, to: &str) -> Result<Vec<FileChange>, GitError> {
    let repo = open(repo_path)?;
    let (old, new) = (resolve(&repo, from)?, resolve(&repo, to)?);
    files_between(&repo, old, new)
}

pub fn diff_between(repo_path: &Path, from: &str, to: &str, path: Option<&str>) -> Result<String, GitError> {
    let repo = open(repo_path)?;
    let (old, new) = (resolve(&repo, from)?, resolve(&repo, to)?);
    unified_between(&repo, old, new, path)
}

/// One file's hunks, under the line naming the file they are in.
///
/// What the file's presence, mode and blob ids did is not repeated here: the response carries a `files` list beside
/// this text, and the path, the status and the counts are already on it. The naming line stays, because it is what
/// separates one file's hunks from the next's.
fn patch(
    repo: &gix::Repository,
    change: &gix::object::tree::diff::ChangeDetached,
    at: &str,
) -> Result<String, GitError> {
    let (before, after) = sides(change);
    let before_bytes = side_bytes(repo, &before)?;
    let after_bytes = side_bytes(repo, &after)?;

    let mut out = format!("diff --git a/{at} b/{at}\n");
    if is_binary(&before_bytes) || is_binary(&after_bytes) {
        out.push_str(&format!("Binary files a/{at} and b/{at} differ\n"));
        return Ok(out);
    }

    let (diff, input) = line_diff(&before_bytes, &after_bytes);
    let body = gix::diff::blob::UnifiedDiff::new(
        &diff,
        &input,
        Hunks::default(),
        gix::diff::blob::unified_diff::ContextSize::default(),
    )
    .consume()
    .map_err(|e| GitError(format!("diffing {at}: {e}")))?;
    out.push_str(&body);
    Ok(out)
}

/// What a line whose file ends without one is marked with.
const NO_NEWLINE: &str = "\\ No newline at end of file\n";

/// Hunks as git writes them: a length of one is left off the header, and a last line without a newline is marked
/// rather than quietly given one -- which every bundle file has, since the JSON writer ends without one.
#[derive(Default)]
struct Hunks(String);

impl Hunks {
    /// One side of a hunk header. Git writes a bare line number for a single line, and counts an empty side from
    /// the line before it.
    fn range(start: u32, len: u32) -> String {
        match len {
            0 => format!("{},0", start.saturating_sub(1)),
            1 => format!("{start}"),
            _ => format!("{start},{len}"),
        }
    }
}

impl gix::diff::blob::unified_diff::ConsumeHunk for Hunks {
    type Out = String;

    fn consume_hunk(
        &mut self,
        header: gix::diff::blob::unified_diff::HunkHeader,
        lines: &[(gix::diff::blob::unified_diff::DiffLineKind, &[u8])],
    ) -> std::io::Result<()> {
        use gix::diff::blob::unified_diff::DiffLineKind;

        self.0.push_str(&format!(
            "@@ -{} +{} @@\n",
            Self::range(header.before_hunk_start, header.before_hunk_len),
            Self::range(header.after_hunk_start, header.after_hunk_len)
        ));
        for (kind, content) in lines {
            self.0.push(match kind {
                DiffLineKind::Context => ' ',
                DiffLineKind::Add => '+',
                DiffLineKind::Remove => '-',
            });
            self.0.push_str(&String::from_utf8_lossy(content));
            if !content.ends_with(b"\n") {
                self.0.push('\n');
                self.0.push_str(NO_NEWLINE);
            }
        }
        Ok(())
    }

    fn finish(self) -> String {
        self.0
    }
}

/// The newest `limit` commits reachable from `rev`, each with the files it touched.
///
/// One tree diff per commit read, and each answers for itself.
pub fn log(repo_path: &Path, rev: &str, limit: u32) -> Result<Vec<CommitSummary>, GitError> {
    log_where(repo_path, rev, limit, |_| true)
}

/// [`log`] of the commits whose files `keep` takes: the newest `limit` of those, read back as far as it takes to
/// find them.
pub fn log_where(
    repo_path: &Path,
    rev: &str,
    limit: u32,
    mut keep: impl FnMut(&[String]) -> bool,
) -> Result<Vec<CommitSummary>, GitError> {
    let repo = open(repo_path)?;
    let start = resolve(&repo, rev)?;
    let failed = |e: String| GitError(format!("reading the history of {rev}: {e}"));
    // A commit fetched from a remote was written there, by whoever could push to it, so what it says about an account
    // is a claim nobody here checked ([`account_of`]).
    // `None` when nothing was fetched. Walked only as far as the fetched history, so it costs what the leader wrote
    // since, not the whole history.
    let ours: Option<std::collections::HashSet<gix::ObjectId>> = match repo.find_reference(FETCHED_MAIN_REF) {
        Ok(tip) => {
            let tip = tip.into_fully_peeled_id().map_err(|e| failed(e.to_string()))?.detach();
            let walk = repo
                .rev_walk([start])
                .with_hidden([tip])
                .all()
                .map_err(|e| failed(e.to_string()))?;
            Some(
                walk.map(|info| info.map(|info| info.id))
                    .collect::<Result<_, _>>()
                    .map_err(|e| failed(e.to_string()))?,
            )
        }
        Err(_) => None,
    };

    let mut commits = Vec::new();
    for info in repo.rev_walk([start]).all().map_err(|e| failed(e.to_string()))? {
        if commits.len() >= limit as usize {
            break;
        }
        let info = info.map_err(|e| failed(e.to_string()))?;
        let files = commit_files(&repo, info.id)?;
        if !keep(&files) {
            continue;
        }
        let commit = repo.find_commit(info.id).map_err(|e| failed(e.to_string()))?;
        let message = commit.message_raw().map_err(|e| failed(e.to_string()))?.to_string();
        let mut lines = message.lines();
        let subject = lines.next().unwrap_or_default().to_string();
        let body = lines.collect::<Vec<&str>>().join("\n").trim().to_string();
        let author = commit.author().map_err(|e| failed(e.to_string()))?;
        let ours = ours.as_ref().is_none_or(|ours| ours.contains(&info.id));
        let deployer = deployer_of(&body).map(|a| if ours { a } else { Actor { id: String::new(), ..a } });
        commits.push(CommitSummary {
            commit: info.id.to_string(),
            subject,
            // The trailer is how the deployer is carried, not something to read twice.
            body: strip_trailer(&body, DEPLOYED_BY),
            time: commit.time().map_err(|e| failed(e.to_string()))?.seconds,
            files,
            author: author.name.to_string(),
            author_id: account_of(&author.email.to_string()).filter(|_| ours),
            deployer: deployer.as_ref().map(|a| a.display_name.clone()),
            deployer_id: deployer.filter(|a| !a.id.is_empty()).map(|a| a.id),
        });
    }
    Ok(commits)
}

/// The actor a leader-written signature names. The inverse of [`signature_for`] as far as one exists: a display
/// name went through [`sanitize_identity`] on the way in and is read back as it was written.
///
/// A signature this leader did not write has no account id, and the name is then whatever the commit says.
fn actor_of(name: String, email: String) -> Actor {
    match account_of(&email) {
        Some(id) => Actor { id, display_name: name },
        None => Actor {
            id: String::new(),
            display_name: name,
        },
    }
}

/// The account an author address names, if it names one.
///
/// The address is this leader's own construction, so reading it back is reading what was written. What that rests
/// on is the repository: `http.receivepack` is false, so nothing can push one in, and every commit under
/// `refs/heads/` was written here -- but for one a remote's `main` brought, which [`log`] reads no account from. An author line is a claim, and this is the claim being trustworthy because of
/// where it is kept rather than because of what it says -- somebody with the data directory can write any commit
/// they like, and can equally write the account store it would be checked against.
fn account_of(email: &str) -> Option<String> {
    let id = email.strip_suffix("@smeltr.invalid")?;
    let reserved = [Actor::SYSTEM_ID, Actor::HOST_ID, Actor::ANONYMOUS_ID, "smeltr-leader"];
    (!id.is_empty() && !reserved.contains(&id)).then(|| id.to_string())
}

/// The deployer a commit names, where it names one.
fn deployer_of(body: &str) -> Option<Actor> {
    let line = body.lines().rev().find_map(|l| l.strip_prefix(DEPLOYED_BY))?;
    let (name, rest) = line.rsplit_once(" <")?;
    Some(actor_of(name.trim().to_string(), rest.strip_suffix('>')?.to_string()))
}

/// A message without its `trailer` lines.
fn strip_trailer(body: &str, trailer: &str) -> String {
    body.lines()
        .filter(|l| !l.starts_with(trailer))
        .collect::<Vec<&str>>()
        .join("\n")
        .trim()
        .to_string()
}

/// The paths one commit touched, against its first parent. A root commit touches everything it holds.
fn commit_files(repo: &gix::Repository, commit: gix::ObjectId) -> Result<Vec<String>, GitError> {
    let failed = |e: String| GitError(format!("reading what {commit} touched: {e}"));
    let commit = repo.find_commit(commit).map_err(|e| failed(e.to_string()))?;
    let new = commit.tree().map_err(|e| failed(e.to_string()))?;
    let old = match commit.parent_ids().next() {
        Some(parent) => Some(
            repo.find_commit(parent.detach())
                .map_err(|e| failed(e.to_string()))?
                .tree()
                .map_err(|e| failed(e.to_string()))?,
        ),
        None => None,
    };
    let changes = repo
        .diff_tree_to_tree(old.as_ref(), &new, gix::diff::Options::default())
        .map_err(|e| failed(e.to_string()))?;
    Ok(blob_paths(&changes))
}

/// The file paths a tree diff touched. A tree diff reports the directories along the way too; every reader here
/// names files, so the trees are dropped.
fn blob_paths(changes: &[gix::object::tree::diff::ChangeDetached]) -> Vec<String> {
    changes
        .iter()
        .filter(|change| change.entry_mode().is_blob_or_symlink())
        .map(|change| change.location().to_string())
        .collect()
}

/// What `git http-backend` answered with: a CGI response, already parsed into the parts an HTTP response needs.
#[derive(Clone)]
pub struct CgiResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

/// Serve one git HTTP request through `git http-backend`, git's own server for the smart protocol.
///
/// Framing the protocol by hand -- a service announcement, pkt-lines, `upload-pack --stateless-rpc` -- is a second
/// implementation of something git already ships, and it only ever covered the shapes we happened to exercise.
/// `http-backend` is the one every other git host runs, so protocol v2 and every negotiation a real client attempts
/// are somebody else's to get right.
///
/// Read-only by construction: `http.receivepack` is set false, so a push is refused here rather than relying on a
/// route not existing.
/// How long a ref advertisement is served from memory before git is asked again.
///
/// Every worker asks on every heartbeat, and a fetch that changes nothing is *only* this request -- the client reads
/// the advertisement, sees it already holds that commit, and stops. Answering each one by spawning `http-backend`
/// and `upload-pack` costs the leader far more than the few hundred bytes it produces, and the bytes are the same
/// for every worker until a ref moves. A window shorter than the heartbeat keeps a deploy as prompt as it was while
/// making the cost of a fleet asking independent of its size.
const ADVERTISEMENT_TTL: Duration = Duration::from_secs(1);

/// The last ref advertisement: which repository it describes, when it was made, and the bytes. A leader serves one
/// repository, but the path is held so the answer for one can never be given for another.
static ADVERTISEMENT: Mutex<Option<(PathBuf, Instant, CgiResponse)>> = Mutex::new(None);

/// Whether this request is the ref advertisement, whose answer depends on the repository's refs and nothing about
/// the caller.
fn is_advertisement(path_info: &str, method: &str, query: &str) -> bool {
    method == "GET" && path_info.ends_with("info/refs") && query.contains("service=git-upload-pack")
}

/// Drop the cached advertisement, so the next asker is answered from the repository. Called where the leader moves
/// a ref itself; the TTL covers every other way one moves.
pub(crate) fn forget_advertisement() {
    *ADVERTISEMENT.lock().unwrap_or_else(|e| e.into_inner()) = None;
}

pub fn http_backend(
    repo_path: &Path,
    path_info: &str,
    method: &str,
    query: &str,
    content_type: Option<&str>,
    body: &[u8],
) -> Result<CgiResponse, GitError> {
    backend(repo_path, path_info, method, query, content_type, body, false)
}

/// [`http_backend`] taking pushes too: a git host, for the tests of pushing to one.
#[cfg(test)]
pub fn http_backend_receiving(
    repo_path: &Path,
    path_info: &str,
    method: &str,
    query: &str,
    content_type: Option<&str>,
    body: &[u8],
) -> Result<CgiResponse, GitError> {
    backend(repo_path, path_info, method, query, content_type, body, true)
}

fn backend(
    repo_path: &Path,
    path_info: &str,
    method: &str,
    query: &str,
    content_type: Option<&str>,
    body: &[u8],
    receive: bool,
) -> Result<CgiResponse, GitError> {
    let cacheable = !receive && is_advertisement(path_info, method, query);
    if cacheable {
        let cached = ADVERTISEMENT.lock().unwrap_or_else(|e| e.into_inner());
        if let Some((cached_repo, made, response)) = cached.as_ref()
            && cached_repo == repo_path
            && made.elapsed() < ADVERTISEMENT_TTL
        {
            return Ok(response.clone());
        }
    }

    let (project_root, repo_name) = match (repo_path.parent(), repo_path.file_name()) {
        (Some(root), Some(name)) => (root, name),
        _ => return Err(GitError(format!("{} is not a repository path", repo_path.display()))),
    };

    let mut cmd = Command::new("git");
    cmd.args([
        "-c",
        if receive {
            "http.receivepack=true"
        } else {
            "http.receivepack=false"
        },
        // `refs/smeltr/` is the leader's own bookkeeping, change markers and the deploy log among it. A worker
        // fetches `DEPLOYED_REF` and nothing else there, so it alone is served.
        "-c",
        "transfer.hideRefs=refs/smeltr",
        "-c",
        "transfer.hideRefs=!refs/smeltr/deployed",
        // What the leader fetched from a remote it follows: `main` is what came of it.
        "-c",
        "transfer.hideRefs=refs/remotes",
        "http-backend",
    ])
    .env("GIT_PROJECT_ROOT", project_root)
    // The one repository this leader serves is the only thing under the project root, so there is nothing an
    // export marker per repository would distinguish.
    .env("GIT_HTTP_EXPORT_ALL", "1")
    .env(
        "PATH_INFO",
        format!("/{}/{}", repo_name.to_string_lossy(), path_info.trim_start_matches('/')),
    )
    .env("REQUEST_METHOD", method)
    .env("QUERY_STRING", query)
    .env("CONTENT_LENGTH", body.len().to_string());
    if let Some(ct) = content_type {
        cmd.env("CONTENT_TYPE", ct);
    }

    let out = run(cmd, &["http-backend"], body)?;
    let response = parse_cgi(&out)?;
    if cacheable {
        *ADVERTISEMENT.lock().unwrap_or_else(|e| e.into_inner()) =
            Some((repo_path.to_path_buf(), Instant::now(), response.clone()));
    }
    Ok(response)
}

/// Whether `git` runs here: `Err` says what a leader without it cannot do and what failed. Checked once at startup,
/// because serving workers, stamping a new repository and pushing to a host all spawn it, and each would otherwise
/// fail later with an error that names none of that.
pub fn check_git_runs() -> Result<(), String> {
    git_runs("git")
}

fn git_runs(program: &str) -> Result<(), String> {
    let needed = |why: String| {
        format!(
            "the leader runs git to serve its configuration to workers, and `{program} --version` {why}. Install git \
             and put it on this process's PATH -- for a service, the PATH of the account it runs as"
        )
    };
    let mut cmd = Command::new(program);
    cmd.arg("--version");
    match crate::process::output_within(&mut cmd, Duration::from_secs(10)) {
        Ok(crate::process::Outcome::Exited(out)) if out.status.success() => Ok(()),
        Ok(crate::process::Outcome::Exited(out)) => Err(needed(format!(
            "failed ({}): {}",
            out.status,
            String::from_utf8_lossy(&out.stderr).trim()
        ))),
        Ok(crate::process::Outcome::Killed(_)) => Err(needed("did not answer within ten seconds".to_string())),
        Err(e) => Err(needed(format!("could not run: {e}"))),
    }
}

// -- pushing to the host ----------------------------------------------------
//
// `gix` cannot push, so a push runs the `git` binary, which the leader already needs to serve workers.

/// What the host made of a push.
#[derive(Debug, PartialEq, Eq)]
pub enum Pushed {
    Done,
    /// The host's branch was not where the lease said: someone else pushed to it.
    Refused,
}

/// `git` aimed at the host at `url` on behalf of this leader: no credential but `authorization`, no prompt, and no
/// redirect, which would carry the header to wherever the host pointed. The header travels in the environment, never
/// in argv, where every process on this host could read it.
fn git_to_host(repo_path: &Path, authorization: Option<&str>) -> Command {
    let mut cmd = Command::new("git");
    cmd.arg("--git-dir").arg(repo_path).args([
        "-c",
        "credential.helper=",
        "-c",
        "http.followRedirects=false",
        // A host that stops sending must not hold the push for good.
        "-c",
        "http.lowSpeedLimit=1024",
        "-c",
        "http.lowSpeedTime=60",
    ]);
    cmd.env("GIT_TERMINAL_PROMPT", "0");
    // `check_url` admits only http(s), but git would follow any transport it is handed, and libcurl carries a dozen
    // more protocols. This keeps both to the two a host is reached over.
    cmd.env("GIT_ALLOW_PROTOCOL", "http:https");
    if let Some(authorization) = authorization {
        cmd.env("GIT_CONFIG_COUNT", "1")
            .env("GIT_CONFIG_KEY_0", "http.extraHeader")
            .env("GIT_CONFIG_VALUE_0", format!("Authorization: {authorization}"));
    }
    cmd
}

/// Push `new` to `branch` on the host at `url`, or delete it there for `None`, but only while the host's branch is at
/// `lease` -- `None` for "only while it does not exist". The lease is what keeps a push from overwriting work someone
/// else pushed to the same branch.
pub fn push(
    repo_path: &Path,
    url: &str,
    authorization: Option<&str>,
    branch: &str,
    new: Option<&str>,
    lease: Option<&str>,
) -> Result<Pushed, GitError> {
    check_branch_name(branch)?;
    for commit in new.iter().chain(lease.iter()) {
        check_commit_id(commit)?;
    }
    let lease = format!("--force-with-lease=refs/heads/{branch}:{}", lease.unwrap_or(""));
    let refspec = format!("{}:refs/heads/{branch}", new.unwrap_or(""));
    let output = git_to_host(repo_path, authorization)
        .args(["push", "--porcelain", "--no-verify", &lease, url, &refspec])
        .stdin(Stdio::null())
        .output()
        .map_err(|e| GitError(format!("spawning git push: {e}")))?;
    if output.status.success() {
        return Ok(Pushed::Done);
    }
    // `--porcelain` says per ref what became of it; `!` is refused, and "stale info" is the lease.
    let said = String::from_utf8_lossy(&output.stdout);
    if said
        .lines()
        .any(|line| line.starts_with('!') && line.contains("(stale info)"))
    {
        return Ok(Pushed::Refused);
    }
    let why = said
        .lines()
        .find(|line| line.starts_with('!'))
        .map(str::to_string)
        .unwrap_or_else(|| String::from_utf8_lossy(&output.stderr).trim().to_string());
    // A host says why it refused on `remote:` lines -- a protected branch, say -- which the ref's line does not carry.
    let stderr = String::from_utf8_lossy(&output.stderr);
    let host_said: Vec<&str> = stderr
        .lines()
        .filter_map(|line| line.strip_prefix("remote:"))
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect();
    let why = if host_said.is_empty() {
        why
    } else {
        format!("{why}: {}", host_said.join(" "))
    };
    Err(GitError(format!("pushing {branch} to {url} did not work: {why}")))
}

/// Where `branch` is on the host at `url`, or `None` when the host has no such branch.
pub fn host_branch(
    repo_path: &Path,
    url: &str,
    authorization: Option<&str>,
    branch: &str,
) -> Result<Option<String>, GitError> {
    check_branch_name(branch)?;
    let output = git_to_host(repo_path, authorization)
        .args(["ls-remote", url, &format!("refs/heads/{branch}")])
        .stdin(Stdio::null())
        .output()
        .map_err(|e| GitError(format!("spawning git ls-remote: {e}")))?;
    if !output.status.success() {
        return Err(GitError(format!(
            "asking {url} where {branch} is did not work: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    Ok(String::from_utf8_lossy(&output.stdout)
        .split_whitespace()
        .next()
        .map(str::to_string))
}

/// Every branch the leader knows the host to have, `main` among them, and where.
pub fn host_branches(repo_path: &Path) -> Result<std::collections::HashMap<String, String>, GitError> {
    tips_under(repo_path, HOST_REFS)
}

/// Every branch here, `main` among them, and where: [`list_branches`] without the walks that count commits.
pub fn branch_tips(repo_path: &Path) -> Result<std::collections::HashMap<String, String>, GitError> {
    tips_under(repo_path, "refs/heads/")
}

/// Every ref under `prefix`, by the rest of its name, and the commit it names.
fn tips_under(repo_path: &Path, prefix: &str) -> Result<std::collections::HashMap<String, String>, GitError> {
    let repo = open(repo_path)?;
    let failed = |e: String| GitError(format!("listing {prefix}: {e}"));
    let mut out = std::collections::HashMap::new();
    for reference in repo
        .references()
        .map_err(|e| failed(e.to_string()))?
        .prefixed(prefix)
        .map_err(|e| failed(e.to_string()))?
    {
        let mut reference = reference.map_err(|e| failed(e.to_string()))?;
        let name = reference.name().as_bstr().to_string();
        let tip = reference.peel_to_id().map_err(|e| failed(e.to_string()))?;
        out.insert(name[prefix.len()..].to_string(), tip.to_string());
    }
    Ok(out)
}

/// Record that the host's `branch` is at `tip`, or that it has none. Only the leader's push and fetch write these.
pub fn record_host_branch(repo_path: &Path, branch: &str, tip: Option<&str>) -> Result<(), GitError> {
    check_branch_name(branch)?;
    let repo = open(repo_path)?;
    let name = format!("{HOST_REFS}{branch}");
    match tip {
        Some(tip) => {
            use gix::refs::transaction::{Change, LogChange, PreviousValue, RefEdit};
            repo.edit_reference(RefEdit {
                change: Change::Update {
                    log: LogChange {
                        message: "smeltr".into(),
                        ..Default::default()
                    },
                    expected: PreviousValue::Any,
                    new: gix::refs::Target::Object(resolve(&repo, tip)?),
                },
                name: full_name(&name)?,
                deref: false,
            })
            .map_err(|e| GitError(format!("{name} did not move: {e}")))?;
            Ok(())
        }
        None => delete_ref(&repo, &name, None),
    }
}

/// Forget everything the leader knew of a host: what it fetched and what it pushed. Called when the remote changes,
/// since none of it is about the new one.
pub fn forget_host(repo_path: &Path) -> Result<(), GitError> {
    for branch in host_branches(repo_path)?.into_keys() {
        record_host_branch(repo_path, &branch, None)?;
    }
    Ok(())
}

/// Split a CGI response into headers and body. `Status:` is the response code, defaulting to 200 when the script
/// does not set one, as CGI requires.
fn parse_cgi(out: &[u8]) -> Result<CgiResponse, GitError> {
    let split = out
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .map(|i| (i, i + 4))
        .or_else(|| out.windows(2).position(|w| w == b"\n\n").map(|i| (i, i + 2)));
    let Some((head_end, body_start)) = split else {
        return Err(GitError("git http-backend wrote no header block".into()));
    };

    let mut status = 200;
    let mut headers = Vec::new();
    for line in String::from_utf8_lossy(&out[..head_end]).lines() {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        let (name, value) = (name.trim(), value.trim());
        if name.eq_ignore_ascii_case("status") {
            status = value
                .split_whitespace()
                .next()
                .and_then(|c| c.parse().ok())
                .unwrap_or(200);
        } else {
            headers.push((name.to_string(), value.to_string()));
        }
    }
    Ok(CgiResponse {
        status,
        headers,
        body: out[body_start..].to_vec(),
    })
}

/// `args` as an error may repeat them. A worker authenticates its fetch by passing its bearer token as a `-c
/// http.extraHeader` argument, so the command line holds a credential -- and an error naming the command line
/// would carry it to the console, to the leader's store, onto the Logs screen and out through any logs source
/// the bundle declares. The design doc's secrets boundary is that a credential is named, never carried.
pub(crate) fn redacted(args: &[&str]) -> Vec<String> {
    args.iter()
        .map(|arg| match arg.split_once("Authorization:") {
            Some((before, _)) => format!("{before}Authorization: <redacted>"),
            None => (*arg).to_string(),
        })
        .collect()
}

fn run(mut cmd: Command, args: &[&str], stdin_body: &[u8]) -> Result<Vec<u8>, GitError> {
    let args = &redacted(args);
    let mut child = cmd
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| GitError(format!("spawning git {args:?}: {e}")))?;

    // stdin is written on its own thread: `upload-pack` can fill its stdout pipe before this process finishes writing
    // stdin.
    let mut stdin = child.stdin.take().expect("stdin was piped");
    let body = stdin_body.to_vec();
    let writer = std::thread::spawn(move || {
        use std::io::Write;
        let _ = stdin.write_all(&body);
    });

    let output = child
        .wait_with_output()
        .map_err(|e| GitError(format!("running git {args:?}: {e}")))?;
    let _ = writer.join();

    if !output.status.success() {
        return Err(GitError(format!(
            "git {args:?} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )));
    }
    Ok(output.stdout)
}

/// The commit `branch` points at. `None` when there is no such branch.
pub fn branch_tip(repo_path: &Path, branch: &str) -> Option<String> {
    check_branch_name(branch).ok()?;
    let repo = open(repo_path).ok()?;
    let id = repo
        .find_reference(&format!("refs/heads/{branch}"))
        .ok()?
        .into_fully_peeled_id()
        .ok()?;
    Some(id.detach().to_string())
}

/// `main`'s head: what a deploy moves the fleet to, and what a branch starts from. `None` when the repository has no
/// `main`, which is a leader nobody has authored anything on yet.
pub fn main_head(repo_path: &Path) -> Option<String> {
    branch_tip(repo_path, MAIN_BRANCH)
}

/// A commit holding one symlink, which nothing in smeltr writes: the API commits regular files, and this is how a
/// commit that arrived another way (an external host, a shell on the leader) reaches a reader. `120000` is git's
/// mode for one and the blob is its target text.
#[cfg(test)]
pub(crate) fn commit_a_symlink(repo_path: &Path, branch: &str, path: &str, target: &str) -> String {
    let repo = open(repo_path).unwrap();
    let parent = branch_tip(repo_path, branch).expect("a branch to add to");
    let parent_id = resolve(&repo, &parent).unwrap();

    // A symlink is a blob holding its target text, told apart by its mode alone.
    let blob = repo.write_blob(target.as_bytes()).unwrap().detach();
    let mut editor = repo
        .find_tree(tree_at(&repo, &parent).unwrap())
        .unwrap()
        .edit()
        .unwrap();
    editor.upsert(path, gix::object::tree::EntryKind::Link, blob).unwrap();
    let tree = editor.write().unwrap().detach();

    let commit = commit_tree_in(
        &repo,
        tree,
        &[parent_id],
        &["a symlink"],
        &Actor::system(),
        Ordered::Clock,
    )
    .unwrap();
    swap_ref_in(&repo, &format!("refs/heads/{branch}"), commit, Some(parent_id)).unwrap();
    commit.to_string()
}

#[cfg(test)]
mod tests {
    /// A worker's fetch carries its bearer token on the command line, so an error naming that command line would
    /// put a live credential on the Logs screen and in every store behind it.
    #[test]
    fn an_error_never_repeats_the_credential_the_command_line_carried() {
        let args = [
            "-c",
            "http.extraHeader=Authorization: Bearer 2fe9aa93f719235eaba13a06e5d5013170fdbb50",
            "-c",
            "http.sslCAInfo=/tmp/leader-ca.pem",
            "fetch",
        ];
        let said = format!("{:?}", super::redacted(&args));
        assert!(!said.contains("2fe9aa93"), "the token is gone: {said}");
        assert!(
            said.contains("Authorization: <redacted>"),
            "and it is visibly gone: {said}"
        );
        // Everything that is not a credential still reads, or the message stops being worth having.
        assert!(
            said.contains("http.sslCAInfo=/tmp/leader-ca.pem") && said.contains("fetch"),
            "{said}"
        );
    }

    #[test]
    fn git_runs_here() {
        super::git_runs("git").expect("the git these tests use runs");
    }

    /// What an operator reads when the leader refuses to start: the cause, and what to do about it.
    #[test]
    fn a_missing_git_says_what_the_leader_needs_it_for() {
        let e = super::git_runs("smeltr-no-such-git").unwrap_err();
        assert!(e.contains("serve its configuration to workers"), "{e}");
        assert!(e.contains("`smeltr-no-such-git --version` could not run"), "{e}");
        assert!(e.contains("PATH"), "{e}");
    }

    use super::*;

    fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("smeltr-git-test-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    fn git_ok(args: &[&str], cwd: &Path) {
        let status = Command::new("git").args(args).current_dir(cwd).status().unwrap();
        assert!(status.success(), "git {args:?} failed");
    }

    /// A leader stopped while it created the repository left `HEAD` and no `config` beside it. The next start builds
    /// a whole one in its place, and that one opens.
    #[test]
    fn a_repository_half_made_by_a_stopped_start_is_made_again() {
        let data = temp_dir("half-made");
        let repo = repo_path(&data);
        let partial = repo.with_extension("git.partial");
        std::fs::create_dir_all(partial.join("objects")).unwrap();
        std::fs::write(partial.join("HEAD"), "ref: refs/heads/master\n").unwrap();

        init_bare_repo(&repo).unwrap();
        assert!(!partial.exists());
        assert!(repo.join("config").is_file());
        super::super::steps::upgrade_repo(&repo).unwrap();
        ensure_main_commit(&repo).unwrap();
        std::fs::remove_dir_all(&data).ok();
    }

    /// A repository with a blob, a ref or its HEAD that does not read fails the check at start, rather than reading later as a
    /// file or a deploy that is not there.
    #[test]
    fn a_damaged_blob_or_ref_fails_the_check() {
        let seeded = || {
            let data = temp_dir("damaged");
            let repo = repo_path(&data);
            let edit = FileEdit {
                path: "groups/a/system.json".to_string(),
                content: Some(b"{\"damaged\": false}".to_vec()),
            };
            seed_first_commit(&repo, &[edit], "seed").unwrap();
            check_repo(&repo).unwrap();
            (data, repo)
        };

        let (data, repo) = seeded();
        let blob = open(&repo)
            .unwrap()
            .write_blob(b"{\"damaged\": false}")
            .unwrap()
            .to_string();
        std::fs::write(repo.join("objects").join(&blob[..2]).join(&blob[2..]), b"not zlib").unwrap();
        let damage = check_repo(&repo).unwrap_err().to_string();
        assert!(
            damage.contains("does not read back whole") && damage.contains(&blob),
            "{damage}"
        );
        std::fs::remove_dir_all(&data).ok();

        let (data, repo) = seeded();
        std::fs::write(repo.join("refs").join("heads").join(MAIN_BRANCH), b"").unwrap();
        let damage = check_repo(&repo).unwrap_err().to_string();
        assert!(damage.contains("does not read back whole"), "{damage}");
        std::fs::remove_dir_all(&data).ok();

        let (data, repo) = seeded();
        std::fs::remove_file(repo.join("HEAD")).unwrap();
        let damage = check_repo(&repo).unwrap_err().to_string();
        assert!(damage.contains("has no HEAD"), "{damage}");
        std::fs::remove_dir_all(&data).ok();
    }

    /// A directory with nothing in it is not a repository yet, and is no obstacle to making one there.
    #[test]
    fn a_repository_is_made_in_an_empty_directory() {
        let dir = temp_dir("into-empty");
        std::fs::create_dir_all(&dir).unwrap();
        init_bare_repo(&dir).unwrap();
        ensure_main_commit(&dir).unwrap();
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A bare repo with one commit on `main`, pushed from a working clone.
    fn repo_with_one_commit(name: &str) -> PathBuf {
        let root = temp_dir(name);
        let bare = root.join("bare.git");
        let work = root.join("work");
        std::fs::create_dir_all(&work).unwrap();
        init_bare_repo(&bare).unwrap();

        git_ok(&["init", "--quiet", "-b", "main"], &work);
        git_ok(&["config", "user.email", "test@example.com"], &work);
        git_ok(&["config", "user.name", "test"], &work);
        std::fs::write(work.join("sources.json"), b"[]").unwrap();
        git_ok(&["add", "sources.json"], &work);
        git_ok(&["commit", "--quiet", "-m", "initial"], &work);
        git_ok(
            &["push", "--quiet", bare.to_str().unwrap(), "HEAD:refs/heads/main"],
            &work,
        );
        bare
    }

    fn wrote(path: &str, body: &str) -> FileEdit {
        FileEdit {
            path: path.to_string(),
            content: Some(body.as_bytes().to_vec()),
        }
    }

    fn at(outcome: Committed) -> String {
        match outcome {
            Committed::At(c) => c,
            Committed::Unchanged => panic!("the edits changed nothing"),
            Committed::Moved(now) => panic!("the branch was at {now:?}"),
        }
    }

    /// A save needs no checkout: the tree is built from the commit it was based on, and the file lands in it.
    #[test]
    fn commit_edits_writes_a_file_without_a_working_tree() {
        let repo = repo_with_one_commit("edits-write");
        let main = main_head(&repo).unwrap();

        let commit = at(commit_edits(
            &repo,
            "work",
            &main,
            None,
            &[wrote("groups/standalone/sources.json", "[1]")],
            "a save",
            &Actor::system(),
        )
        .unwrap());

        assert_eq!(branch_tip(&repo, "work").as_deref(), Some(commit.as_str()));
        let read = read_blobs(&repo, &commit, &["groups/standalone/sources.json".to_string()]).unwrap();
        assert_eq!(read[0].as_deref(), Some(&b"[1]"[..]));
        std::fs::remove_dir_all(repo.parent().unwrap()).ok();
    }

    /// The check and the write are one operation, so the second of two writers who read the same commit is refused
    /// rather than overwriting the first.
    #[test]
    fn commit_edits_refuses_a_save_the_branch_has_moved_past() {
        let repo = repo_with_one_commit("edits-cas");
        let main = main_head(&repo).unwrap();
        let read_at = at(commit_edits(
            &repo,
            "work",
            &main,
            None,
            &[wrote("a.json", "1")],
            "first",
            &Actor::system(),
        )
        .unwrap());
        let moved = at(commit_edits(
            &repo,
            "work",
            &read_at,
            Some(&read_at),
            &[wrote("a.json", "2")],
            "second",
            &Actor::system(),
        )
        .unwrap());

        // A third writer still holding `read_at`.
        let outcome = commit_edits(
            &repo,
            "work",
            &read_at,
            Some(&read_at),
            &[wrote("a.json", "3")],
            "third",
            &Actor::system(),
        )
        .unwrap();
        match outcome {
            Committed::Moved(now) => assert_eq!(now.as_deref(), Some(moved.as_str())),
            Committed::At(c) => panic!("a stale save landed at {c}"),
            Committed::Unchanged => panic!("a stale save was read as a no-op"),
        }
        assert_eq!(branch_tip(&repo, "work").as_deref(), Some(moved.as_str()));
        std::fs::remove_dir_all(repo.parent().unwrap()).ok();
    }

    /// A first save is create-only, so two of them to one name is one branch and one refusal.
    #[test]
    fn commit_edits_refuses_a_first_save_onto_a_branch_that_now_exists() {
        let repo = repo_with_one_commit("edits-create");
        let main = main_head(&repo).unwrap();
        let first = at(commit_edits(
            &repo,
            "work",
            &main,
            None,
            &[wrote("a.json", "1")],
            "first",
            &Actor::system(),
        )
        .unwrap());

        let outcome = commit_edits(
            &repo,
            "work",
            &main,
            None,
            &[wrote("a.json", "2")],
            "also first",
            &Actor::system(),
        )
        .unwrap();
        assert!(matches!(outcome, Committed::Moved(Some(now)) if now == first));
        std::fs::remove_dir_all(repo.parent().unwrap()).ok();
    }

    /// A save that changes no bytes does not grow the branch.
    #[test]
    fn commit_edits_that_change_nothing_write_nothing() {
        let repo = repo_with_one_commit("edits-noop");
        let main = main_head(&repo).unwrap();

        let outcome = commit_edits(
            &repo,
            "work",
            &main,
            None,
            &[wrote("sources.json", "[]")],
            "no change",
            &Actor::system(),
        )
        .unwrap();
        assert!(matches!(outcome, Committed::Unchanged));
        assert_eq!(branch_tip(&repo, "work"), None, "a no-op save started a branch");
        std::fs::remove_dir_all(repo.parent().unwrap()).ok();
    }

    /// Removing a pipeline is an edit like any other, and removing what is not there is not an error.
    #[test]
    fn commit_edits_removes_a_file() {
        let repo = repo_with_one_commit("edits-remove");
        let main = main_head(&repo).unwrap();
        let gone = FileEdit {
            path: "sources.json".to_string(),
            content: None,
        };
        let absent = FileEdit {
            path: "pipelines/never.json".to_string(),
            content: None,
        };
        let commit = at(commit_edits(&repo, "work", &main, None, &[gone, absent], "remove", &Actor::system()).unwrap());

        let read = read_blobs(&repo, &commit, &["sources.json".to_string()]).unwrap();
        assert_eq!(read[0], None);
        std::fs::remove_dir_all(repo.parent().unwrap()).ok();
    }

    #[test]
    fn info_refs_advertises_the_pushed_branch() {
        let bare = repo_with_one_commit("advertise");
        let served = http_backend(&bare, "info/refs", "GET", "service=git-upload-pack", None, &[]).unwrap();
        assert_eq!(served.status, 200);
        assert!(
            served
                .headers
                .iter()
                .any(|(n, v)| n.eq_ignore_ascii_case("content-type")
                    && v == "application/x-git-upload-pack-advertisement"),
            "{:?}",
            served.headers
        );
        let text = String::from_utf8_lossy(&served.body);
        assert!(text.contains("# service=git-upload-pack"), "{text}");
        assert!(text.contains("refs/heads/main"), "{text}");
    }

    #[test]
    fn info_refs_on_an_empty_repo_still_responds_without_erroring() {
        let dir = temp_dir("advertise-empty");
        init_bare_repo(&dir).unwrap();
        let served = http_backend(&dir, "info/refs", "GET", "service=git-upload-pack", None, &[]).unwrap();
        assert_eq!(served.status, 200);
        assert!(String::from_utf8_lossy(&served.body).contains("# service=git-upload-pack"));
    }

    /// Read-only is a property of the server, not of which routes happen to exist.
    #[test]
    fn a_push_is_refused_by_the_server_itself() {
        let bare = repo_with_one_commit("no-push");
        let served = http_backend(&bare, "info/refs", "GET", "service=git-receive-pack", None, &[]).unwrap();
        assert_ne!(served.status, 200, "receive-pack must not be advertised");

        let served = http_backend(
            &bare,
            "git-receive-pack",
            "POST",
            "",
            Some("application/x-git-receive-pack-request"),
            b"0000",
        )
        .unwrap();
        assert_ne!(served.status, 200, "a push must not be served");
    }

    /// The leader hands every path under `/api/worker/repo/` to this and keeps no list of its own, so this is the whole guard
    /// on that route. A path a worker picks must never read a file git does not serve: not the repository's own
    /// config, and not what sits beside the repository in the data directory.
    #[test]
    fn a_path_a_worker_picks_never_reads_a_file_git_does_not_serve() {
        let data = temp_dir("not-git-to-serve");
        let repo = repo_path(&data);
        init_bare_repo(&repo).unwrap();
        ensure_main_commit(&repo).unwrap();

        let secret = "not-for-any-worker";
        git_ok(&["config", "--file", "config", "smeltr.marker", secret], &repo);
        std::fs::write(data.join("leader.db"), secret).unwrap();
        std::fs::create_dir_all(data.join("tls")).unwrap();
        std::fs::write(data.join("tls").join("ca-key.pem"), secret).unwrap();

        for path in ["config", "../../leader.db", "../../tls/ca-key.pem"] {
            // An error is a refusal too: the route answers it with a bare 500.
            if let Ok(served) = http_backend(&repo, path, "GET", "", None, &[]) {
                assert_ne!(served.status, 200, "{path} was served");
                assert!(!String::from_utf8_lossy(&served.body).contains(secret), "{path} leaked");
            }
        }
        std::fs::remove_dir_all(&data).ok();
    }

    // -- saving -------------------------------------------------------------

    /// A commit on `branch`, the way a save lands one: built from the commit it is based on, with nothing checked
    /// out anywhere. The repository is created on demand, as the API's first read or write creates it.
    fn save(data: &Path, branch: &str, files: &[(&str, &str)], message: &str) -> String {
        let repo = repo_path(data);
        init_bare_repo(&repo).unwrap();
        ensure_main_commit(&repo).unwrap();
        let expected = branch_tip(&repo, branch);
        let parent = expected.clone().or_else(|| main_head(&repo)).unwrap();
        let edits: Vec<FileEdit> = files
            .iter()
            .map(|(path, body)| FileEdit {
                path: path.to_string(),
                content: Some(body.as_bytes().to_vec()),
            })
            .collect();
        match commit_edits(
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
            Committed::At(commit) => commit,
            Committed::Unchanged => panic!("the fixture's save changed nothing"),
            Committed::Moved(now) => panic!("the fixture's branch was at {now:?}"),
        }
    }

    /// A branch name reaches git as an argument, so every segment is held to the same shape a group id is.
    #[test]
    fn a_branch_name_that_is_not_safe_is_refused() {
        for branch in [
            "../escape",
            ".hidden",
            "with space",
            "",
            "-force",
            // A separator cannot smuggle past the shape each segment is held to.
            "al/../escape",
            "al//two",
            "/leading",
            "trailing/",
            "al/.hidden",
            "al/-force",
            "al/with space",
            // Git stores a ref as a file, and `.lock` is the name it takes to write another.
            "al/work.lock",
        ] {
            assert!(check_branch_name(branch).is_err(), "{branch:?} must not reach git");
        }
    }

    /// A slash separates segments, which is how a fleet with more than one author keeps names apart.
    #[test]
    fn a_branch_name_may_be_namespaced() {
        for branch in ["al/vendor-discount", "work", "team.payments/rotate_key", "a/b/c"] {
            check_branch_name(branch).unwrap_or_else(|e| panic!("{branch:?}: {e}"));
        }
    }

    /// A ref is a file under `refs/heads/`, so a name and a namespace of that name cannot both exist. Git refuses
    /// the second; the refusal has to name the first, or there is nothing to act on.
    #[test]
    fn a_branch_and_a_namespace_of_its_name_cannot_both_exist() {
        let data = temp_dir("branch-namespace-clash");
        create_branch(&data, "al/vendor-discount").unwrap();
        let err = create_branch(&data, "al").expect_err("a file where a directory is");
        assert!(err.to_string().contains("al/vendor-discount"), "{err}");

        let other = temp_dir("branch-namespace-clash-other");
        create_branch(&other, "al").unwrap();
        let err = create_branch(&other, "al/vendor-discount").expect_err("a directory where a file is");
        assert!(err.to_string().contains("\"al\""), "{err}");
    }

    /// The whole point of allowing one: a namespaced branch is started, saved on and read like any other.
    #[test]
    fn a_namespaced_branch_saves_and_reads_like_any_other() {
        let data = temp_dir("namespaced-branch");
        let repo = repo_path(&data);
        let head = save(
            &data,
            "al/vendor-discount",
            &[("sources.json", "[{\"id\":\"one\"}]")],
            "edit sources",
        );
        assert!(branch_exists(&repo, "al/vendor-discount").unwrap());
        assert_eq!(branch_tip(&repo, "al/vendor-discount").as_deref(), Some(head.as_str()));
        assert_eq!(ahead_behind(&repo, "al/vendor-discount").unwrap(), (1, 0));
        assert_eq!(changed_files(&repo, "al/vendor-discount").unwrap().len(), 1);
    }

    /// Two branches are two refs off one history, so a save on one is invisible to the other until it lands on
    /// `main`.
    #[test]
    fn two_branches_do_not_see_each_other_s_saves() {
        let data = temp_dir("two-branches");
        let repo = repo_path(&data);
        let mine = save(&data, "mine", &[("sources.json", "[]")], "edit sources");
        let yours = create_branch(&data, "yours").unwrap();

        assert_ne!(mine, yours);
        assert_eq!(Some(yours), main_head(&repo), "the other branch starts where main is");
        let read = read_blobs(
            &repo,
            &branch_tip(&repo, "yours").unwrap(),
            &["sources.json".to_string()],
        )
        .unwrap();
        assert_eq!(read[0], None, "and does not hold the other's save");
    }

    // -- reading branches and history -------------------------------------

    /// `bring_current` where no conflict is expected.
    fn brought_current(data: &Path, branch: &str) -> Result<BroughtCurrent, GitError> {
        match bring_current(data, branch, &Actor::system(), None)? {
            BringCurrent::Done(done) => Ok(done),
            BringCurrent::Conflicts(c) => panic!("unexpected conflicts: {c:?}"),
        }
    }

    /// A data directory whose repository has `main`, plus `branch` carrying one save.
    fn data_with_branch(name: &str, branch: &str) -> PathBuf {
        let data = temp_dir(name);
        save(&data, branch, &[("sources.json", "[{\"id\":\"one\"}]")], "edit sources");
        data
    }

    #[test]
    fn every_branch_is_listed_with_its_distance_from_main() {
        let data = data_with_branch("list-branches", "work");
        let branches = list_branches(&repo_path(&data)).unwrap();

        let names: Vec<&str> = branches.iter().map(|b| b.name.as_str()).collect();
        assert!(names.contains(&MAIN_BRANCH) && names.contains(&"work"), "{names:?}");

        let work = branches.iter().find(|b| b.name == "work").unwrap();
        assert_eq!((work.ahead, work.behind), (1, 0), "one save, and `main` has not moved");
        assert!(work.last_save > 0, "the tip's committer date is the last save");
    }

    /// What makes a change stale: `main` moved after the branch left it.
    #[test]
    fn a_branch_main_has_moved_past_is_behind() {
        let data = data_with_branch("behind", "work");
        save(&data, MAIN_BRANCH, &[("destinations.json", "[]")], "somebody deployed");

        let (ahead, behind) = ahead_behind(&repo_path(&data), "work").unwrap();
        assert_eq!((ahead, behind), (1, 1));
    }

    #[test]
    fn changed_files_names_what_the_branch_changed_and_by_how_much() {
        let data = data_with_branch("changed-files", "work");
        let changed = changed_files(&repo_path(&data), "work").unwrap();

        assert_eq!(changed.len(), 1, "{changed:?}");
        assert_eq!(changed[0].path, "sources.json");
        assert_eq!(changed[0].status, "A", "`main` starts empty, so this is an add");
        assert_eq!((changed[0].added, changed[0].removed), (1, 0));
    }

    /// The counts `--numstat` gives for the same change, so a file's `+n -n` is git's arithmetic.
    #[test]
    fn changed_files_counts_what_git_numstat_counts() {
        let data = temp_dir("numstat-parity");
        save(
            &data,
            MAIN_BRANCH,
            &[("sources.json", "[\n  1,\n  2,\n  3\n]"), ("routes.json", "[]")],
            "main",
        );
        save(
            &data,
            "work",
            &[
                ("sources.json", "[\n  1,\n  9,\n  3,\n  4\n]"),
                ("destinations.json", "[\n  5\n]"),
            ],
            "work",
        );
        let repo = repo_path(&data);

        let out = std::process::Command::new("git")
            .args(["diff", "--no-renames", "--numstat", &format!("{MAIN_BRANCH}...work")])
            .current_dir(&repo)
            .output()
            .expect("git to run");
        let expected: Vec<(u32, u32, String)> = String::from_utf8_lossy(&out.stdout)
            .lines()
            .map(|line| {
                let mut f = line.splitn(3, '\t');
                (
                    f.next().unwrap().parse().unwrap(),
                    f.next().unwrap().parse().unwrap(),
                    f.next().unwrap().to_string(),
                )
            })
            .collect();
        assert!(!expected.is_empty(), "git found nothing to count");

        let ours: Vec<(u32, u32, String)> = changed_files(&repo, "work")
            .unwrap()
            .into_iter()
            .map(|c| (c.added, c.removed, c.path))
            .collect();
        assert_eq!(ours, expected);
    }

    /// A branch is diffed against where it left `main`, not against `main`'s tip: what somebody else deployed
    /// afterward is not this change's doing.
    #[test]
    fn a_branch_is_diffed_against_where_it_left_main() {
        let data = data_with_branch("merge-base-diff", "work");
        save(&data, MAIN_BRANCH, &[("destinations.json", "[]")], "somebody deployed");

        let changed = changed_files(&repo_path(&data), "work").unwrap();
        let paths: Vec<&str> = changed.iter().map(|c| c.path.as_str()).collect();
        assert_eq!(paths, vec!["sources.json"], "not destinations.json, which main gained");
    }

    /// A branch's base stays where it left `main` while `main` moves on, and is in the history of both.
    #[test]
    fn a_branch_s_base_is_where_it_left_main() {
        let data = temp_dir("branch-base");
        let repo = repo_path(&data);
        init_bare_repo(&repo).unwrap();
        ensure_main_commit(&repo).unwrap();
        let left_at = main_head(&repo).unwrap();
        let tip = save(&data, "work", &[("sources.json", "[{\"id\":\"one\"}]")], "edit sources");
        let deployed = save(&data, MAIN_BRANCH, &[("destinations.json", "[]")], "somebody deployed");

        assert_eq!(branch_base(&repo, "work").unwrap(), left_at);
        assert_eq!(
            branch_base(&repo, MAIN_BRANCH).unwrap(),
            deployed,
            "main's base is its own tip"
        );
        assert!(reachable_from(&repo, &left_at, "work").unwrap());
        assert!(reachable_from(&repo, &tip, "work").unwrap());
        assert!(
            !reachable_from(&repo, &deployed, "work").unwrap(),
            "main's later commit is not the branch's"
        );
        assert!(reachable_from(&repo, &deployed, MAIN_BRANCH).unwrap());
    }

    /// What `git diff` writes for the same change, so the text a viewer parses is git's own and not merely
    /// something shaped like it. Every bundle file ends without a newline, so this pins the marker for one too.
    fn git_diff(repo: &Path, range: &str, path: Option<&str>) -> String {
        let mut args = vec!["diff", "--no-renames", "--no-color", range];
        if let Some(path) = path {
            args.push("--");
            args.push(path);
        }
        let out = std::process::Command::new("git")
            .args(&args)
            .current_dir(repo)
            .output()
            .expect("git to run");
        assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
        without_headers(&String::from_utf8_lossy(&out.stdout))
    }

    /// git's patch text with the header lines we do not write removed -- the blob ids, the mode lines and the
    /// `---`/`+++` pair, all of which the `files` list beside the diff already answers.
    ///
    /// Only each file's header block is filtered. A removed line whose own text begins `--` renders as `---`, and
    /// inside a hunk it is content, not a header.
    fn without_headers(patch: &str) -> String {
        const HEADERS: [&str; 7] = [
            "index ",
            "--- ",
            "+++ ",
            "new file mode ",
            "deleted file mode ",
            "old mode ",
            "new mode ",
        ];
        let mut out = String::new();
        let mut in_header = false;
        for line in patch.lines() {
            if line.starts_with("diff --git ") {
                in_header = true;
            } else if line.starts_with("@@ ") {
                in_header = false;
            }
            if in_header && HEADERS.iter().any(|h| line.starts_with(h)) {
                continue;
            }
            out.push_str(line);
            out.push('\n');
        }
        out
    }

    #[test]
    fn a_unified_diff_is_what_git_writes_for_the_same_change() {
        let data = temp_dir("diff-parity");
        // An add, a modification whose last line moves, and a delete -- with no trailing newline anywhere, as the
        // bundle writer leaves them.
        save(
            &data,
            MAIN_BRANCH,
            &[("sources.json", "[\n  1,\n  2,\n  3\n]"), ("destinations.json", "[]")],
            "main",
        );
        save(
            &data,
            "work",
            &[
                ("sources.json", "[\n  1,\n  9,\n  3,\n  4\n]"),
                ("destinations.json", ""),
                ("routes.json", "[\n  7\n]"),
            ],
            "work",
        );
        // The delete is its own save: `save` writes files, it does not remove them.
        let repo = repo_path(&data);
        let tip = branch_tip(&repo, "work").unwrap();
        commit_edits(
            &repo,
            "work",
            &tip,
            Some(&tip),
            &[FileEdit {
                path: "destinations.json".into(),
                content: None,
            }],
            "drop destinations",
            &Actor::system(),
        )
        .unwrap();

        let range = format!("{MAIN_BRANCH}...work");
        assert_eq!(
            diff_unified(&repo, "work", None).unwrap(),
            git_diff(&repo, &range, None),
            "the whole diff"
        );
        for path in ["sources.json", "destinations.json", "routes.json"] {
            assert_eq!(
                diff_unified(&repo, "work", Some(path)).unwrap(),
                git_diff(&repo, &range, Some(path)),
                "{path}"
            );
        }
    }

    #[test]
    fn a_unified_diff_can_be_taken_for_one_path() {
        let data = data_with_branch("unified", "work");
        let repo = repo_path(&data);

        let whole = diff_unified(&repo, "work", None).unwrap();
        assert!(whole.contains("sources.json"), "{whole}");
        assert!(whole.contains("+[{\"id\":\"one\"}]"), "{whole}");

        assert_eq!(
            diff_unified(&repo, "work", Some("destinations.json")).unwrap(),
            "",
            "a path the branch did not touch has no diff"
        );
    }

    #[test]
    fn log_reads_a_commit_s_subject_body_and_files() {
        let data = temp_dir("log");
        save(
            &data,
            "work",
            &[("sources.json", "[]")],
            "edit sources\n\nwhy it changed",
        );

        let entries = log(&repo_path(&data), "work", 10).unwrap();
        assert_eq!(entries[0].subject, "edit sources");
        assert_eq!(entries[0].body, "why it changed");
        assert_eq!(entries[0].files, vec!["sources.json".to_string()]);
        assert_eq!(entries.len(), 2, "`main`'s own first commit is reachable too");
        assert!(entries[0].time >= entries[1].time, "newest first");
    }

    #[test]
    fn last_changed_is_the_newest_commit_that_wrote_each_file() {
        let data = temp_dir("last-changed");
        save(
            &data,
            "work",
            &[("samples/a.json", "1"), ("samples/b.json", "1")],
            "both",
        );
        save(&data, "work", &[("samples/a.json", "2")], "a again");
        save(&data, "work", &[("sources.json", "[]")], "unrelated");
        let repo = repo_path(&data);

        let times: std::collections::HashMap<String, i64> = log(&repo, "work", 10)
            .unwrap()
            .into_iter()
            .map(|c| (c.subject, c.time))
            .collect();
        let tip = branch_tip(&repo, "work").unwrap();
        let paths = ["samples/a.json", "samples/b.json", "samples/gone.json"].map(String::from);
        let placed = last_changed(&repo, &tip, &paths).unwrap();
        assert_eq!(placed.get("samples/a.json"), Some(&times["a again"]));
        assert_eq!(placed.get("samples/b.json"), Some(&times["both"]));
        assert_eq!(
            placed.get("samples/gone.json"),
            None,
            "a file the commit lacks has no time"
        );
    }

    #[test]
    fn log_stops_at_the_limit() {
        let data = data_with_branch("log-limit", "work");
        assert_eq!(log(&repo_path(&data), "work", 1).unwrap().len(), 1);
    }

    /// What a writer whose save was refused reads: not the branch against `main`, but the one commit that landed
    /// under them.
    #[test]
    fn two_commits_diff_to_what_landed_between_them() {
        let data = data_with_branch("diff-between", "work");
        let repo = repo_path(&data);
        let read_at = branch_tip(&repo, "work").expect("the branch has a tip");
        save(
            &data,
            "work",
            &[("sources.json", "[{\"id\":\"two\"}]")],
            "somebody else",
        );
        let moved = branch_tip(&repo, "work").expect("it moved");
        assert_ne!(read_at, moved);

        let files = changed_between(&repo, &read_at, &moved).unwrap();
        assert_eq!(files.len(), 1, "{files:?}");
        assert_eq!(files[0].path, "sources.json");
        assert_eq!(files[0].status, "M");

        let diff = diff_between(&repo, &read_at, &moved, None).unwrap();
        assert!(diff.contains("-[{\"id\":\"one\"}]"), "{diff}");
        assert!(diff.contains("+[{\"id\":\"two\"}]"), "{diff}");

        // The branch against `main` is a different question, and answers both commits at once.
        let whole = diff_unified(&repo, "work", None).unwrap();
        assert!(whole.contains("+[{\"id\":\"two\"}]"), "{whole}");
        assert!(
            !whole.contains("-[{\"id\":\"one\"}]"),
            "main never had the first save: {whole}"
        );
    }

    /// A branch name reaches git as an argument, so every reader holds it to the same shape a path component is.
    #[test]
    fn a_branch_name_that_is_not_safe_is_refused_by_every_reader() {
        let data = data_with_branch("unsafe-readers", "work");
        let repo = repo_path(&data);
        for branch in ["--upload-pack=touch", "../escape", "with space"] {
            assert!(ahead_behind(&repo, branch).is_err(), "{branch:?}");
            assert!(changed_files(&repo, branch).is_err(), "{branch:?}");
            assert!(diff_unified(&repo, branch, None).is_err(), "{branch:?}");
        }
    }

    // -- starting and deleting a branch -----------------------------------

    #[test]
    fn a_branch_is_started_at_main_and_refused_the_second_time() {
        let data = temp_dir("create-branch");
        let head = create_branch(&data, "al-vendor").unwrap();
        let repo = repo_path(&data);

        assert_eq!(
            Some(head.as_str()),
            main_head(&repo).as_deref(),
            "a new branch starts where main is"
        );
        assert!(branch_exists(&repo, "al-vendor").unwrap());
        assert!(create_branch(&data, "al-vendor").is_err(), "the name is taken");
    }

    /// Both sides are valid on their own and the merge is clean, yet the result does not run: the branch deleted a
    /// pipeline nothing referenced, `main` added a route to it, and the two touched different files so git had no
    /// reason to object.
    #[test]
    fn a_merge_that_leaves_a_group_unable_to_load_lands_and_says_so() {
        let data = temp_dir("merge-dangling");
        let repo = repo_path(&data);
        let route = |rs: &str| format!("[{rs}]");
        let to = |id: &str| format!(r#"{{"id":"r-{id}","source":"m","pipeline":"{id}"}}"#);
        let pipeline = |id: &str| format!(r#"{{"id":"{id}","name":"{id}","steps":[]}}"#);

        save(
            &data,
            MAIN_BRANCH,
            &[
                ("groups/west/system.json", r#"{"version":1}"#),
                (
                    "groups/west/sources.json",
                    r#"[{"id":"m","type":"metrics","bucketSeconds":10}]"#,
                ),
                ("groups/west/routes.json", &route(&to("shipping"))),
                ("groups/west/pipelines/shipping.json", &pipeline("shipping")),
                ("groups/west/pipelines/orders.json", &pipeline("orders")),
            ],
            "a bundle for west",
        );

        // The branch drops a pipeline nothing routes to. Valid on its own.
        let main = main_head(&repo).unwrap();
        let tip = match commit_edits(
            &repo,
            "work",
            &main,
            None,
            &[FileEdit {
                path: "groups/west/pipelines/orders.json".to_string(),
                content: None,
            }],
            "drop the unused orders pipeline",
            &Actor::system(),
        )
        .unwrap()
        {
            Committed::At(c) => c,
            _ => panic!("the deletion did not land"),
        };

        // `main` routes to it instead of dropping it, touching only routes.json. Also valid on its own.
        save(
            &data,
            MAIN_BRANCH,
            &[(
                "groups/west/routes.json",
                &route(&format!("{},{}", to("shipping"), to("orders"))),
            )],
            "route to orders as well",
        );

        // Git merged these two happily, which is the whole point: the failure is in the result, not the merge.
        let brought = brought_current(&data, "work").expect("the merge itself is clean");
        let said = brought.does_not_load.expect("and the result does not load");
        assert!(said.contains("west"), "it names the group: {said}");
        assert!(said.contains("orders"), "and what is wrong: {said}");
        assert_ne!(
            branch_tip(&repo, "work").as_deref(),
            Some(tip.as_str()),
            "and the merge landed -- an author cannot fix what they cannot see"
        );
        std::fs::remove_dir_all(&data).ok();
    }

    /// A bundle for `group` whose one route names `pipeline`, and the pipeline file to match.
    fn west(pipeline: &str) -> Vec<(String, String)> {
        vec![
            ("groups/west/system.json".into(), r#"{"version":1}"#.into()),
            (
                "groups/west/sources.json".into(),
                r#"[{"id":"m","type":"metrics","bucketSeconds":10}]"#.into(),
            ),
            (
                "groups/west/routes.json".into(),
                format!(r#"[{{"id":"r1","source":"m","pipeline":"{pipeline}"}}]"#),
            ),
            (
                format!("groups/west/pipelines/{pipeline}.json"),
                format!(r#"{{"id":"{pipeline}","name":"{pipeline}","steps":[]}}"#),
            ),
        ]
    }

    fn put(data: &Path, branch: &str, files: &[(String, String)], message: &str) -> String {
        let borrowed: Vec<(&str, &str)> = files.iter().map(|(a, b)| (a.as_str(), b.as_str())).collect();
        save(data, branch, &borrowed, message)
    }

    /// A commit can reach a branch some way smeltr did not write it (design doc, Config plane), so what a deploy
    /// puts in front of live
    /// data is checked at the deploy rather than trusted from the save.
    #[test]
    fn deploying_a_branch_that_does_not_load_is_refused() {
        let data = temp_dir("deploy-broken");
        let repo = repo_path(&data);
        put(&data, MAIN_BRANCH, &west("orders"), "a bundle for west");

        // The branch routes to a pipeline it does not carry.
        create_branch(&data, "work").unwrap();
        put(
            &data,
            "work",
            &[(
                "groups/west/routes.json".into(),
                r#"[{"id":"r1","source":"m","pipeline":"gone"}]"#.into(),
            )],
            "route to a pipeline that is not there",
        );
        open_change(&repo, "work", "Route to gone", "", &Actor::system()).unwrap();

        let before = main_head(&repo).unwrap();
        let err = deploy_change(&data, "work", &Actor::system()).expect_err("it would not run");
        assert!(err.to_string().contains("west"), "it names the group: {err}");
        assert!(err.to_string().contains("Nothing was deployed"), "{err}");
        assert_eq!(main_head(&repo).as_deref(), Some(before.as_str()), "main unmoved");
        assert!(branch_exists(&repo, "work").unwrap(), "and the branch is still there");
        std::fs::remove_dir_all(&data).ok();
    }

    /// A group already broken where the fleet runs is not made worse by a change that leaves it so: refusing would
    /// make the fix to every other group wait on it.
    #[test]
    fn a_fix_to_one_group_deploys_while_another_is_still_broken() {
        let data = temp_dir("deploy-beside-broken");
        let repo = repo_path(&data);
        put(&data, MAIN_BRANCH, &west("orders"), "a bundle for west");
        save(
            &data,
            MAIN_BRANCH,
            &[("groups/east/system.json", "not json")],
            "a broken east",
        );
        deploy_commit(&data, &main_head(&repo).unwrap(), &Actor::system()).unwrap();

        create_branch(&data, "work").unwrap();
        put(&data, "work", &west("shipping"), "west runs shipping");
        open_change(&repo, "work", "Shipping", "", &Actor::system()).unwrap();
        let deployed_now = deploy_change(&data, "work", &Actor::system()).expect("east was broken already");
        assert_eq!(deployed(&repo).as_deref(), Some(deployed_now.as_str()));

        // A group that is new, or loads where the fleet runs, is still refused when the change breaks it.
        create_branch(&data, "more").unwrap();
        save(
            &data,
            "more",
            &[("groups/north/system.json", "not json")],
            "a broken north",
        );
        open_change(&repo, "more", "North", "", &Actor::system()).unwrap();
        let err = deploy_change(&data, "more", &Actor::system()).expect_err("north is new and broken");
        assert!(err.to_string().contains("north"), "{err}");
        std::fs::remove_dir_all(&data).ok();
    }

    /// A rollback holds the fleet through an approval, which moves only `main`; a release ends it.
    #[test]
    fn a_rollback_holds_through_an_approval_until_a_release() {
        let data = temp_dir("hold-through-approve");
        let repo = repo_path(&data);
        put(&data, MAIN_BRANCH, &west("orders"), "a bundle for west");
        let first = main_head(&repo).unwrap();
        put(&data, MAIN_BRANCH, &west("shipping"), "west runs shipping");
        deploy_commit(&data, &main_head(&repo).unwrap(), &Actor::system()).unwrap();
        deploy_commit(&data, &first, &Actor::system()).unwrap();

        create_branch(&data, "work").unwrap();
        put(&data, "work", &west("billing"), "west runs billing");
        open_change(&repo, "work", "Billing", "", &Actor::system()).unwrap();
        let approved = deploy_change(&data, "work", &Actor::system()).unwrap();
        assert_eq!(main_head(&repo).as_deref(), Some(approved.as_str()));
        assert_eq!(deployed(&repo).as_deref(), Some(first.as_str()), "still held");

        deploy_commit(&data, &approved, &Actor::system()).unwrap();
        assert_eq!(hold(&repo).unwrap(), None, "released");
        std::fs::remove_dir_all(&data).ok();
    }

    /// A sample deploys only when a test beside it names it: the rest are left out, and stay on the branch.
    #[test]
    fn a_deploy_leaves_out_samples_no_test_names() {
        let data = temp_dir("left-out-samples");
        let repo = repo_path(&data);
        put(&data, MAIN_BRANCH, &west("orders"), "a bundle for west");
        save(
            &data,
            MAIN_BRANCH,
            &[
                ("groups/west/samples/old.json", "old"),
                ("groups/west/samples/gone.json", "gone"),
            ],
            "samples from before",
        );

        create_branch(&data, "work").unwrap();
        put(&data, "work", &west("billing"), "west runs billing");
        let test = r#"{"id":"t","name":"t","sample":"locked","lockedAt":0,"rowsIn":1,"expected":[]}"#;
        save(
            &data,
            "work",
            &[
                ("groups/west/samples/locked.json", "locked"),
                ("groups/west/tests/t.json", test),
                ("groups/west/samples/tried.json", "tried"),
                ("groups/west/samples/old.json", "old, edited"),
                ("shared/samples/pasted.json", "pasted"),
            ],
            "samples and a test",
        );
        let removing = [FileEdit {
            path: "groups/west/samples/gone.json".into(),
            content: None,
        }];
        let tip = branch_tip(&repo, "work").unwrap();
        commit_edits(
            &repo,
            "work",
            &tip,
            Some(&tip),
            &removing,
            "remove gone",
            &Actor::system(),
        )
        .unwrap();

        assert_eq!(
            {
                let changed = changed_files(&repo, "work").unwrap();
                let tip = branch_tip(&repo, "work").unwrap();
                let changed = changed.iter().map(|c| (c.path.as_str(), c.status == "D"));
                left_out_samples(&repo, &tip, changed).into_iter().collect::<Vec<_>>()
            },
            [
                "groups/west/samples/old.json",
                "groups/west/samples/tried.json",
                "shared/samples/pasted.json"
            ],
            "what the review marks as not deployed"
        );

        open_change(&repo, "work", "Billing", "", &Actor::system()).unwrap();
        let deployed = deploy_change(&data, "work", &Actor::system()).unwrap();
        let paths = [
            "groups/west/samples/locked.json",
            "groups/west/tests/t.json",
            "groups/west/samples/tried.json",
            "groups/west/samples/old.json",
            "groups/west/samples/gone.json",
            "shared/samples/pasted.json",
        ]
        .map(String::from);
        let landed: Vec<Option<String>> = read_blobs(&repo, &deployed, &paths)
            .unwrap()
            .into_iter()
            .map(|b| b.map(|b| String::from_utf8(b).unwrap()))
            .collect();
        assert_eq!(landed[0].as_deref(), Some("locked"), "a test names it");
        assert_eq!(landed[1].as_deref(), Some(test));
        assert_eq!(landed[2], None, "no test names it");
        assert_eq!(landed[3].as_deref(), Some("old"), "main keeps what it held");
        assert_eq!(landed[4], None, "a removal deploys");
        assert_eq!(landed[5], None, "nor in shared/");
        std::fs::remove_dir_all(&data).ok();
    }

    /// A branch holding only left-out samples has nothing to deploy.
    #[test]
    fn left_out_samples_alone_deploy_nothing() {
        let data = temp_dir("left-out-only");
        let repo = repo_path(&data);
        put(&data, MAIN_BRANCH, &west("orders"), "a bundle for west");
        create_branch(&data, "work").unwrap();
        save(
            &data,
            "work",
            &[("groups/west/samples/tried.json", "tried")],
            "a capture",
        );
        open_change(&repo, "work", "Tried", "", &Actor::system()).unwrap();
        let err = deploy_change(&data, "work", &Actor::system()).expect_err("nothing reaches main");
        assert!(err.0.contains("nothing to deploy"), "{}", err.0);
        std::fs::remove_dir_all(&data).ok();
    }

    /// A test that does not read could name any sample, so its bundle's samples all deploy.
    #[test]
    fn a_test_that_does_not_read_keeps_its_bundles_samples() {
        let data = temp_dir("left-out-unreadable-test");
        let repo = repo_path(&data);
        put(&data, MAIN_BRANCH, &west("orders"), "a bundle for west");
        create_branch(&data, "work").unwrap();
        save(
            &data,
            "work",
            &[
                ("groups/west/samples/tried.json", "tried"),
                ("groups/west/tests/broken.json", "{"),
            ],
            "a capture and a broken test",
        );
        open_change(&repo, "work", "Tried", "", &Actor::system()).unwrap();
        let deployed = deploy_change(&data, "work", &Actor::system()).unwrap();
        let landed = read_blobs(&repo, &deployed, &["groups/west/samples/tried.json".to_string()]).unwrap();
        assert_eq!(landed[0].as_deref(), Some(&b"tried"[..]));
        std::fs::remove_dir_all(&data).ok();
    }

    /// A log written before the hold trailer marks a rollback by its subject alone, and it holds the same.
    #[test]
    fn a_rollback_logged_before_the_hold_trailer_still_holds() {
        let data = temp_dir("hold-legacy");
        let repo = repo_path(&data);
        put(&data, MAIN_BRANCH, &west("orders"), "a bundle for west");
        let first = main_head(&repo).unwrap();
        put(&data, MAIN_BRANCH, &west("shipping"), "west runs shipping");
        let git = open(&repo).unwrap();
        let target = resolve(&git, &first).unwrap();
        let subject = format!("{ROLLED_BACK_SUBJECT}{}: a bundle for west", short(&first));
        deploy_in(
            &git,
            target,
            deployed_in(&git),
            Main::Stays,
            &subject,
            false,
            &Actor::system(),
        )
        .unwrap();
        assert!(rolled_back(&git));

        deploy_commit(&data, &main_head(&repo).unwrap(), &Actor::system()).unwrap();
        assert!(!rolled_back(&git), "a release is not a hold");
        std::fs::remove_dir_all(&data).ok();
    }

    /// The way back stays open when the way back does not load: the caller is told which groups, and deploys it
    /// anyway if they say so.
    #[test]
    fn a_commit_that_does_not_load_is_named_and_still_deployable() {
        let data = temp_dir("rollback-broken");
        let repo = repo_path(&data);
        save(
            &data,
            MAIN_BRANCH,
            &[("groups/east/system.json", "not json")],
            "a broken east",
        );
        let broken = main_head(&repo).unwrap();
        put(&data, MAIN_BRANCH, &west("orders"), "a bundle for west");
        deploy_commit(&data, &main_head(&repo).unwrap(), &Actor::system()).unwrap();

        let named = deploying_would_not_load(&data, &broken).unwrap();
        assert_eq!(named.len(), 1, "{named:?}");
        assert_eq!(named[0].0, "east");
        assert!(
            deploying_would_not_load(&data, "nonsense").is_err(),
            "refused as deploy_commit refuses it"
        );
        assert_eq!(deploy_commit(&data, &broken, &Actor::system()).unwrap(), broken);
        assert_eq!(deployed(&repo).as_deref(), Some(broken.as_str()));
        std::fs::remove_dir_all(&data).ok();
    }

    /// The point of letting the merge land: the author fixes the branch it left them and deploys. `main` never
    /// holds the broken state, because the deploy is where this is a refusal.
    #[test]
    fn a_merge_that_breaks_a_branch_can_be_fixed_on_it_and_deployed() {
        let data = temp_dir("refused-then-fixed");
        let repo = repo_path(&data);
        let pipeline = |id: &str| format!(r#"{{"id":"{id}","name":"{id}","steps":[]}}"#);
        let route = |id: &str| format!(r#"{{"id":"r-{id}","source":"m","pipeline":"{id}"}}"#);

        save(
            &data,
            MAIN_BRANCH,
            &[
                ("groups/west/system.json", r#"{"version":1}"#),
                (
                    "groups/west/sources.json",
                    r#"[{"id":"m","type":"metrics","bucketSeconds":10}]"#,
                ),
                ("groups/west/routes.json", &format!("[{}]", route("shipping"))),
                ("groups/west/pipelines/shipping.json", &pipeline("shipping")),
                ("groups/west/pipelines/orders.json", &pipeline("orders")),
            ],
            "a bundle for west",
        );

        // The author drops a pipeline nothing routes to.
        create_branch(&data, "work").unwrap();
        let main = main_head(&repo).unwrap();
        commit_edits(
            &repo,
            "work",
            &main,
            Some(&main),
            &[
                FileEdit {
                    path: "groups/west/pipelines/orders.json".to_string(),
                    content: None,
                },
                // ...and the change they actually came to make, which has to survive all of this.
                FileEdit {
                    path: "groups/west/pipelines/shipping.json".to_string(),
                    content: Some(
                        r#"{"id":"shipping","name":"shipping","steps":[{"type":"comment","id":"c1","text":"why"}]}"#
                            .as_bytes()
                            .to_vec(),
                    ),
                },
            ],
            "drop the unused orders pipeline, and raise shipping",
            &Actor::system(),
        )
        .unwrap();

        // Meanwhile `main` routes to it.
        save(
            &data,
            MAIN_BRANCH,
            &[(
                "groups/west/routes.json",
                &format!("[{},{}]", route("shipping"), route("orders")),
            )],
            "route to orders as well",
        );

        let said = brought_current(&data, "work")
            .expect("the merge lands")
            .does_not_load
            .expect("and reports what it left");
        assert!(said.contains("orders"), "the message names what is wrong: {said}");
        let merged = branch_tip(&repo, "work").unwrap();
        assert!(
            crate::leader::tree::load_group_at(&repo, &merged, "west").is_err(),
            "the branch is broken, and visibly so"
        );

        // Which is enough to act on: put the pipeline back.
        save(
            &data,
            "work",
            &[("groups/west/pipelines/orders.json", &pipeline("orders"))],
            "keep orders after all",
        );

        assert!(
            brought_current(&data, "work").unwrap().does_not_load.is_none(),
            "and loads once it is fixed"
        );
        open_change(&repo, "work", "Keep orders", "", &Actor::system()).unwrap();
        let deployed = deploy_change(&data, "work", &Actor::system()).expect("and it deploys");

        assert_eq!(main_head(&repo).as_deref(), Some(deployed.as_str()));
        let landed = crate::leader::tree::load_group_at(&repo, &deployed, "west").expect("what landed runs");
        assert!(
            landed.pipelines.iter().any(|p| p.name == "shipping"),
            "and still carries the change they came to make"
        );
        std::fs::remove_dir_all(&data).ok();
    }

    #[test]
    fn deleting_a_branch_that_is_not_there_is_refused() {
        let data = data_with_branch("delete-missing", "work");
        let err = delete_branch(&data, "never-existed", None).expect_err("there is no such branch");
        assert!(err.to_string().contains("no branch \"never-existed\""), "{err}");
    }

    #[test]
    fn main_is_neither_started_nor_deleted() {
        let data = temp_dir("main-branch-guard");
        init_bare_repo(&repo_path(&data)).unwrap();
        assert!(create_branch(&data, MAIN_BRANCH).is_err());
        assert!(delete_branch(&data, MAIN_BRANCH, None).is_err());
    }

    #[test]
    fn deleting_a_branch_frees_its_name() {
        let data = data_with_branch("delete-branch", "parked");
        delete_branch(&data, "parked", None).unwrap();

        assert!(!branch_exists(&repo_path(&data), "parked").unwrap());
        create_branch(&data, "parked").expect("and the name is free again");
    }

    /// A name that starts with a dash reads as an option wherever it reaches a command line.
    #[test]
    fn no_segment_of_a_branch_name_may_start_with_a_dash() {
        for branch in ["-D", "--force", "al/-D"] {
            assert!(check_branch_name(branch).is_err(), "{branch:?}");
        }
        assert!(check_branch_name("al-vendor").is_ok(), "a dash inside one is fine");
    }

    // -- changes ----------------------------------------------------------

    #[test]
    fn a_branch_with_no_marker_has_no_change() {
        let data = data_with_branch("no-change", "work");
        assert_eq!(read_change(&repo_path(&data), "work").unwrap(), None);
    }

    #[test]
    fn opening_a_change_records_its_title_and_description() {
        let data = data_with_branch("open-change", "work");
        let repo = repo_path(&data);
        open_change(
            &repo,
            "work",
            "Keep the vendor discount field",
            "it was dropped by a filter",
            &Actor::system(),
        )
        .unwrap();

        let change = read_change(&repo, "work").unwrap().expect("a change");
        assert_eq!(change.state, ChangeState::Open);
        assert_eq!(change.title, "Keep the vendor discount field");
        assert_eq!(change.description, "it was dropped by a filter");
        assert_eq!(change.reason, "");
        assert!(change.opened > 0);
    }

    #[test]
    fn a_change_needs_a_title_and_is_opened_once() {
        let data = data_with_branch("open-twice", "work");
        let repo = repo_path(&data);
        assert!(
            open_change(&repo, "work", "   ", "", &Actor::system()).is_err(),
            "the title names the deploy"
        );
        open_change(&repo, "work", "First", "", &Actor::system()).unwrap();
        assert!(
            open_change(&repo, "work", "Second", "", &Actor::system()).is_err(),
            "one branch is one change"
        );
    }

    /// Closing keeps the branch and the change: no work is lost, and the reason is what the author acts on (W6).
    #[test]
    fn closing_keeps_the_title_and_records_the_reason() {
        let data = data_with_branch("close-change", "work");
        let repo = repo_path(&data);
        open_change(&repo, "work", "Keep the vendor discount field", "why", &Actor::system()).unwrap();
        close_change(&repo, "work", "the filter is wanted after all", &Actor::system()).unwrap();

        let change = read_change(&repo, "work").unwrap().unwrap();
        assert_eq!(change.state, ChangeState::Closed);
        assert_eq!(change.reason, "the filter is wanted after all");
        assert_eq!(change.title, "Keep the vendor discount field", "a reason is not a name");
        assert!(branch_exists(&repo, "work").unwrap(), "the branch stays");
    }

    /// A reason is encouraged and never required, and git has no empty commit message.
    #[test]
    fn a_change_closed_without_a_reason_says_so() {
        let data = data_with_branch("close-no-reason", "work");
        let repo = repo_path(&data);
        open_change(&repo, "work", "Keep the vendor discount field", "", &Actor::system()).unwrap();
        close_change(&repo, "work", "  ", &Actor::system()).unwrap();
        assert_eq!(read_change(&repo, "work").unwrap().unwrap().reason, NO_REASON);
    }

    #[test]
    fn a_closed_change_reopens_under_the_same_title() {
        let data = data_with_branch("reopen", "work");
        let repo = repo_path(&data);
        open_change(&repo, "work", "Keep the vendor discount field", "why", &Actor::system()).unwrap();
        close_change(&repo, "work", "not yet", &Actor::system()).unwrap();
        open_change(&repo, "work", "Keep the vendor discount field", "why", &Actor::system()).unwrap();

        let change = read_change(&repo, "work").unwrap().unwrap();
        assert_eq!(change.state, ChangeState::Open);
        assert_eq!(change.reason, "", "reopening clears the reason");
    }

    #[test]
    fn closing_needs_something_open() {
        let data = data_with_branch("close-unopened", "work");
        assert!(close_change(&repo_path(&data), "work", "why", &Actor::system()).is_err());
    }

    /// A marker moves its own ref and nothing else: an editor saving on the branch is not interrupted by somebody
    /// opening the change.
    #[test]
    fn a_marker_does_not_move_the_branch() {
        let data = data_with_branch("marker-off-branch", "work");
        let repo = repo_path(&data);
        let before = main_head(&repo);
        let tip = branch_tip(&repo, "work");

        open_change(&repo, "work", "Keep the vendor discount field", "", &Actor::system()).unwrap();

        assert_eq!(branch_tip(&repo, "work"), tip);
        assert_eq!(main_head(&repo), before);
        let listed = list_branches(&repo).unwrap();
        assert!(
            listed.iter().all(|b| b.name != "refs/smeltr/changes/work"),
            "a marker is not a branch"
        );
    }

    #[test]
    fn deleting_a_branch_forgets_its_change() {
        let data = data_with_branch("delete-with-change", "parked");
        let repo = repo_path(&data);
        open_change(&repo, "parked", "Something", "", &Actor::system()).unwrap();
        delete_branch(&data, "parked", None).unwrap();

        create_branch(&data, "parked").unwrap();
        assert_eq!(
            read_change(&repo, "parked").unwrap(),
            None,
            "a new branch of the same name does not inherit the old change"
        );
    }

    #[test]
    fn retitling_changes_the_title_and_description() {
        let data = data_with_branch("retitle", "work");
        let repo = repo_path(&data);
        open_change(&repo, "work", "Keep the vendor discount field", "why", &Actor::system()).unwrap();

        retitle_change(
            &repo,
            "work",
            "Retain the vendor discount field",
            "renamed for clarity",
            &Actor::system(),
        )
        .unwrap();

        let change = read_change(&repo, "work").unwrap().unwrap();
        assert_eq!(change.state, ChangeState::Open);
        assert_eq!(change.title, "Retain the vendor discount field");
        assert_eq!(change.description, "renamed for clarity");
    }

    #[test]
    fn retitling_sets_and_clears_the_description() {
        let data = data_with_branch("retitle-description", "work");
        let repo = repo_path(&data);
        open_change(&repo, "work", "Keep the vendor discount field", "", &Actor::system()).unwrap();

        retitle_change(
            &repo,
            "work",
            "Keep the vendor discount field",
            "now it has one",
            &Actor::system(),
        )
        .unwrap();
        assert_eq!(
            read_change(&repo, "work").unwrap().unwrap().description,
            "now it has one"
        );

        retitle_change(&repo, "work", "Keep the vendor discount field", "", &Actor::system()).unwrap();
        assert_eq!(read_change(&repo, "work").unwrap().unwrap().description, "");
    }

    #[test]
    fn retitling_needs_an_open_change() {
        let data = data_with_branch("retitle-guard", "work");
        let repo = repo_path(&data);
        let err = retitle_change(&repo, "work", "New title", "", &Actor::system()).unwrap_err();
        assert!(err.to_string().contains("was never opened for review"), "{err}");

        open_change(&repo, "work", "First", "", &Actor::system()).unwrap();
        close_change(&repo, "work", "not yet", &Actor::system()).unwrap();
        let err = retitle_change(&repo, "work", "New title", "", &Actor::system()).unwrap_err();
        assert!(err.to_string().contains("is not open for review"), "{err}");
    }

    /// `opened` comes from the newest *open* marker (`read_change`), and a rename writes one: it is not pinned
    /// to when the change was first opened, it moves to when it was last (re)titled.
    #[test]
    fn renaming_does_not_reopen_the_change() {
        let data = data_with_branch("retitle-opened", "work");
        let repo = repo_path(&data);
        open_change(&repo, "work", "First title", "", &Actor::system()).unwrap();
        let opened_first = read_change(&repo, "work").unwrap().unwrap().opened;

        // Commit times are unix seconds: without the gap a rename lands in the same second, and a moved
        // `opened` would be indistinguishable from one that stayed put.
        std::thread::sleep(std::time::Duration::from_millis(1100));
        retitle_change(&repo, "work", "Second title", "", &Actor::system()).unwrap();

        let change = read_change(&repo, "work").unwrap().unwrap();
        assert_eq!(change.state, ChangeState::Open);
        assert_eq!(change.title, "Second title");
        assert_eq!(change.opened, opened_first, "a rename is not a new opening");
        assert!(change.updated > change.opened, "but it is an update");
    }

    /// A comment with no anchor and no thread, as the timeline shows one.
    fn plain(body: impl Into<String>) -> EventKind {
        EventKind::Comment {
            body: body.into(),
            anchor: None,
            at: None,
            reply_to: None,
            outdated: false,
        }
    }

    /// The marker commit's whole message, newest first.
    fn marker_messages(repo: &Path, branch: &str) -> Vec<String> {
        let repo = open(repo).unwrap();
        let tip = repo
            .find_reference(&change_ref(branch))
            .unwrap()
            .into_fully_peeled_id()
            .unwrap();
        repo.rev_walk([tip.detach()])
            .all()
            .unwrap()
            .map(|info| {
                let commit = repo.find_commit(info.unwrap().id).unwrap();
                commit.message_raw().unwrap().to_string()
            })
            .collect()
    }

    fn comments(repo: &Path, branch: &str) -> Vec<TimelineEvent> {
        change_timeline(repo, branch)
            .unwrap()
            .into_iter()
            .filter(|e| matches!(e.kind, EventKind::Comment { .. }))
            .collect()
    }

    fn pipeline_anchor(step: Option<&str>) -> Anchor {
        Anchor::Object {
            scope: "shared".into(),
            section: "pipelines".into(),
            id: "Orders West".into(),
            step: step.map(str::to_string),
        }
    }

    /// The message a plain comment is written with, byte for byte: older builds read it, and so must this one.
    #[test]
    fn a_plain_comment_is_written_as_it_always_was() {
        let data = data_with_branch("comment-bytes", "work");
        let repo = repo_path(&data);
        open_change(&repo, "work", "First", "", &Actor::system()).unwrap();
        comment_on_change(&repo, "work", "looks right", None, None, &Actor::system()).unwrap();
        assert_eq!(
            marker_messages(&repo, "work")[0],
            "looks right\n\nSmeltr-Change: comment\n"
        );
    }

    /// An anchor goes in the last paragraph, above the kind, and comes back as it went in -- a name with a space in
    /// it included.
    #[test]
    fn an_anchored_comment_round_trips() {
        let data = data_with_branch("comment-anchor", "work");
        let repo = repo_path(&data);
        open_change(&repo, "work", "First", "", &Actor::system()).unwrap();
        let head = branch_tip(&repo, "work").unwrap();
        let anchors = [
            pipeline_anchor(None),
            pipeline_anchor(Some("e 1")),
            Anchor::Line {
                path: "shared/sources.json".into(),
                line: 3,
            },
        ];
        for anchor in &anchors {
            comment_on_change(
                &repo,
                "work",
                "about this",
                Some(anchor.clone()),
                None,
                &Actor::system(),
            )
            .unwrap();
        }
        assert_eq!(
            marker_messages(&repo, "work")[1],
            format!(
                "about this\n\nSmeltr-Anchor: object shared pipelines Orders%20West e%201\nSmeltr-At: {head}\n\
                 Smeltr-Change: comment\n"
            )
        );
        let read: Vec<EventKind> = comments(&repo, "work").into_iter().map(|e| e.kind).collect();
        let expected: Vec<EventKind> = anchors
            .into_iter()
            .map(|anchor| EventKind::Comment {
                body: "about this".into(),
                anchor: Some(anchor),
                at: Some(head.clone()),
                reply_to: None,
                outdated: false,
            })
            .collect();
        assert_eq!(read, expected);
    }

    #[test]
    fn an_anchor_that_names_nothing_is_refused() {
        let data = data_with_branch("comment-anchor-bad", "work");
        let repo = repo_path(&data);
        open_change(&repo, "work", "First", "", &Actor::system()).unwrap();
        let refused = [
            Anchor::Object {
                scope: "../x".into(),
                section: "pipelines".into(),
                id: "p".into(),
                step: None,
            },
            Anchor::Object {
                scope: "shared".into(),
                section: "system".into(),
                id: "p".into(),
                step: None,
            },
            Anchor::Line {
                path: "shared/routes.json".into(),
                line: 0,
            },
        ];
        for anchor in refused {
            comment_on_change(&repo, "work", "x", Some(anchor.clone()), None, &Actor::system())
                .expect_err(&format!("{anchor:?}"));
        }
        assert!(comments(&repo, "work").is_empty());
    }

    /// The trailers are the paragraph the server writes after the body, so a body ending in one that looks like
    /// trailers is still body.
    #[test]
    fn a_trailer_in_the_body_stays_body() {
        let data = data_with_branch("comment-forged", "work");
        let repo = repo_path(&data);
        open_change(&repo, "work", "First", "", &Actor::system()).unwrap();
        let forged = "hello\n\nSmeltr-Anchor: line shared/routes.json 1\nSmeltr-Reply-To: 0123456789012345678901234567890123456789";
        comment_on_change(&repo, "work", forged, None, None, &Actor::system()).unwrap();
        comment_on_change(
            &repo,
            "work",
            forged,
            Some(pipeline_anchor(None)),
            None,
            &Actor::system(),
        )
        .unwrap();

        let read = comments(&repo, "work");
        assert_eq!(read[0].kind, plain(forged));
        let EventKind::Comment {
            body, anchor, reply_to, ..
        } = &read[1].kind
        else {
            panic!()
        };
        assert_eq!(
            (body.as_str(), anchor, reply_to),
            (forged, &Some(pipeline_anchor(None)), &None)
        );
    }

    /// A reply is about what its thread is about, and is outdated when its thread is.
    #[test]
    fn a_reply_inherits_its_thread() {
        let data = data_with_branch("comment-reply", "work");
        let repo = repo_path(&data);
        open_change(&repo, "work", "First", "", &Actor::system()).unwrap();
        let anchor = Anchor::Line {
            path: "sources.json".into(),
            line: 1,
        };
        comment_on_change(&repo, "work", "why?", Some(anchor.clone()), None, &Actor::system()).unwrap();
        let root = comments(&repo, "work")[0].id.clone().unwrap();
        comment_on_change(&repo, "work", "because", None, Some(&root), &Actor::system()).unwrap();
        save(&data, "work", &[("sources.json", "[]")], "drop the source");

        let read = comments(&repo, "work");
        let EventKind::Comment {
            anchor: got,
            reply_to,
            outdated,
            ..
        } = &read[1].kind
        else {
            panic!()
        };
        assert_eq!(
            (got, reply_to.as_deref(), *outdated),
            (&Some(anchor), Some(root.as_str()), true)
        );
        assert!(matches!(read[0].kind, EventKind::Comment { outdated: true, .. }));
    }

    #[test]
    fn a_reply_answers_a_comment_that_started_a_thread() {
        let data = data_with_branch("comment-reply-bad", "work");
        let repo = repo_path(&data);
        open_change(&repo, "work", "First", "", &Actor::system()).unwrap();
        comment_on_change(&repo, "work", "why?", None, None, &Actor::system()).unwrap();
        let root = comments(&repo, "work")[0].id.clone().unwrap();
        comment_on_change(&repo, "work", "because", None, Some(&root), &Actor::system()).unwrap();
        let reply = comments(&repo, "work")[1].id.clone().unwrap();
        let opened = change_timeline(&repo, "work")
            .unwrap()
            .into_iter()
            .find_map(|e| match e.kind {
                EventKind::Opened { .. } => e.id,
                _ => None,
            });

        let to_reply = comment_on_change(&repo, "work", "x", None, Some(&reply), &Actor::system()).unwrap_err();
        assert!(to_reply.to_string().contains("is a reply"), "{to_reply}");
        let to_open = comment_on_change(&repo, "work", "x", None, opened.as_deref(), &Actor::system()).unwrap_err();
        assert!(to_open.to_string().contains("is not a comment"), "{to_open}");
        let unknown = "0123456789012345678901234567890123456789";
        let to_nothing = comment_on_change(&repo, "work", "x", None, Some(unknown), &Actor::system()).unwrap_err();
        assert!(to_nothing.to_string().contains("there is no comment"), "{to_nothing}");
        let anchored = comment_on_change(
            &repo,
            "work",
            "x",
            Some(pipeline_anchor(None)),
            Some(&root),
            &Actor::system(),
        );
        assert!(anchored.unwrap_err().to_string().contains("takes no anchor"));
        assert_eq!(comments(&repo, "work").len(), 2, "nothing was written");
    }

    /// Outdated is the anchor's own file changing, not any save at all.
    #[test]
    fn a_comment_is_outdated_by_a_save_to_its_file_only() {
        let data = temp_dir("comment-outdated");
        save(
            &data,
            "work",
            &[
                ("shared/sources.json", "[]"),
                ("shared/pipelines/orders-west.json", "{}"),
            ],
            "seed",
        );
        let repo = repo_path(&data);
        open_change(&repo, "work", "First", "", &Actor::system()).unwrap();
        comment_on_change(
            &repo,
            "work",
            "this one",
            Some(pipeline_anchor(Some("e"))),
            None,
            &Actor::system(),
        )
        .unwrap();
        let outdated = || match &comments(&repo, "work")[0].kind {
            EventKind::Comment { outdated, .. } => *outdated,
            _ => unreachable!(),
        };
        assert!(!outdated());
        save(&data, "work", &[("shared/sources.json", "[ ]")], "another file");
        assert!(!outdated(), "a save to another file");
        save(
            &data,
            "work",
            &[("shared/pipelines/orders-west.json", "{ }")],
            "its file",
        );
        assert!(outdated(), "a save to the pipeline's own file");
    }

    #[test]
    fn a_thread_is_resolved_and_reopened() {
        let data = data_with_branch("thread-resolve", "work");
        let repo = repo_path(&data);
        open_change(&repo, "work", "First", "", &Actor::system()).unwrap();
        comment_on_change(&repo, "work", "why?", None, None, &Actor::system()).unwrap();
        let root = comments(&repo, "work")[0].id.clone().unwrap();
        comment_on_change(&repo, "work", "because", None, Some(&root), &Actor::system()).unwrap();
        let reply = comments(&repo, "work")[1].id.clone().unwrap();

        let not_yet = resolve_thread(&repo, "work", &root, false, &Actor::system()).unwrap_err();
        assert!(not_yet.to_string().contains("not resolved"), "{not_yet}");
        resolve_thread(&repo, "work", &root, true, &Actor::system()).unwrap();
        let again = resolve_thread(&repo, "work", &root, true, &Actor::system()).unwrap_err();
        assert!(again.to_string().contains("already resolved"), "{again}");
        resolve_thread(&repo, "work", &root, false, &Actor::system()).unwrap();
        assert!(
            resolve_thread(&repo, "work", &reply, true, &Actor::system()).is_err(),
            "a reply is no thread"
        );
        let unknown = "0123456789012345678901234567890123456789";
        assert!(resolve_thread(&repo, "work", unknown, true, &Actor::system()).is_err());

        assert_eq!(
            marker_messages(&repo, "work")[1],
            format!("Smeltr-Reply-To: {root}\nSmeltr-Change: resolved\n")
        );
        let kinds: Vec<EventKind> = change_timeline(&repo, "work")
            .unwrap()
            .into_iter()
            .map(|e| e.kind)
            .filter(|k| matches!(k, EventKind::Resolved { .. } | EventKind::Unresolved { .. }))
            .collect();
        assert_eq!(
            kinds,
            [
                EventKind::Resolved { reply_to: root.clone() },
                EventKind::Unresolved { reply_to: root.clone() }
            ]
        );
        assert_eq!(
            read_change(&repo, "work").unwrap().unwrap().state,
            ChangeState::Open,
            "a thread moves no state"
        );
    }

    #[test]
    fn a_comment_lands_on_an_open_or_a_closed_change() {
        let data = data_with_branch("comment", "work");
        let repo = repo_path(&data);
        open_change(&repo, "work", "Keep the vendor discount field", "", &Actor::system()).unwrap();
        comment_on_change(&repo, "work", "looks right", None, None, &Actor::system()).unwrap();
        close_change(&repo, "work", "not yet", &Actor::system()).unwrap();
        comment_on_change(&repo, "work", "why not?", None, None, &Actor::system()).unwrap();

        let comments: Vec<EventKind> = change_timeline(&repo, "work")
            .unwrap()
            .into_iter()
            .map(|e| e.kind)
            .filter(|k| matches!(k, EventKind::Comment { .. }))
            .collect();
        assert_eq!(comments, vec![plain("looks right"), plain("why not?"),]);
    }

    #[test]
    fn a_comment_needs_a_body_and_an_opened_change() {
        let data = data_with_branch("comment-guard", "work");
        let repo = repo_path(&data);
        let err = comment_on_change(&repo, "work", "hello", None, None, &Actor::system()).unwrap_err();
        assert!(err.to_string().contains("was never opened for review"), "{err}");
        assert_eq!(read_change(&repo, "work").unwrap(), None, "and nothing was written");

        open_change(&repo, "work", "First", "", &Actor::system()).unwrap();
        assert!(comment_on_change(&repo, "work", " \n ", None, None, &Actor::system()).is_err());
    }

    /// A comment is said about a change and moves nothing `read_change` folds.
    #[test]
    fn a_comment_leaves_the_change_as_it_was() {
        let data = data_with_branch("comment-fold", "work");
        let repo = repo_path(&data);
        let author = Actor {
            id: "u-ann".into(),
            display_name: "Ann".into(),
        };
        open_change(&repo, "work", "Keep the vendor discount field", "why", &author).unwrap();
        let before = read_change(&repo, "work").unwrap().unwrap();

        // A comment in the same second would leave `updated` alone even if it counted.
        std::thread::sleep(std::time::Duration::from_millis(1100));
        comment_on_change(&repo, "work", "first line\nsecond line", None, None, &Actor::system()).unwrap();
        assert_eq!(read_change(&repo, "work").unwrap().unwrap(), before);

        close_change(&repo, "work", "not yet", &author).unwrap();
        let closed = read_change(&repo, "work").unwrap().unwrap();
        comment_on_change(&repo, "work", "noted", None, None, &Actor::system()).unwrap();
        assert_eq!(read_change(&repo, "work").unwrap().unwrap(), closed);
    }

    #[test]
    fn a_comment_keeps_every_line_it_was_written_with() {
        let data = data_with_branch("comment-lines", "work");
        let repo = repo_path(&data);
        open_change(&repo, "work", "First", "", &Actor::system()).unwrap();
        let body = "Two things:\n\n- the filter\n- the field\nSmeltr-Change: open";
        comment_on_change(&repo, "work", body, None, None, &Actor::system()).unwrap();

        let timeline = change_timeline(&repo, "work").unwrap();
        assert_eq!(
            timeline.last().unwrap().kind,
            plain(body),
            "a trailer-shaped line inside the body is not the trailer"
        );
        assert_eq!(read_change(&repo, "work").unwrap().unwrap().title, "First");
    }

    fn ann() -> Actor {
        Actor {
            id: "u-ann".into(),
            display_name: "Ann".into(),
        }
    }

    fn bob() -> Actor {
        Actor {
            id: "u-bob".into(),
            display_name: "Bob".into(),
        }
    }

    fn verdicts(repo: &Path, branch: &str) -> Vec<EventKind> {
        change_timeline(repo, branch)
            .unwrap()
            .into_iter()
            .map(|e| e.kind)
            .filter(|k| matches!(k, EventKind::Approved { .. } | EventKind::ChangesRequested { .. }))
            .collect()
    }

    #[test]
    fn verdicts_land_on_an_open_change_in_order() {
        let data = data_with_branch("review", "work");
        let repo = repo_path(&data);
        open_change(&repo, "work", "Keep the vendor discount field", "", &ann()).unwrap();
        review_change(&repo, "work", Verdict::ChangesRequested, " rename the field \n", &bob()).unwrap();
        review_change(&repo, "work", Verdict::Approved, "good now", &bob()).unwrap();

        assert_eq!(
            verdicts(&repo, "work"),
            vec![
                EventKind::ChangesRequested {
                    body: "rename the field".into()
                },
                EventKind::Approved {
                    body: "good now".into()
                },
            ]
        );
        let last = change_timeline(&repo, "work").unwrap().pop().unwrap();
        assert_eq!(last.author, bob());
    }

    #[test]
    fn an_approval_with_nothing_said_round_trips() {
        let data = data_with_branch("review-empty", "work");
        let repo = repo_path(&data);
        open_change(&repo, "work", "First", "", &ann()).unwrap();
        review_change(&repo, "work", Verdict::Approved, "  ", &bob()).unwrap();
        assert_eq!(
            verdicts(&repo, "work"),
            vec![EventKind::Approved { body: String::new() }]
        );
    }

    #[test]
    fn a_verdict_needs_an_open_change() {
        let data = data_with_branch("review-guard", "work");
        let repo = repo_path(&data);
        let err = review_change(&repo, "work", Verdict::Approved, "", &bob()).unwrap_err();
        assert!(err.to_string().contains("was never opened for review"), "{err}");
        assert_eq!(read_change(&repo, "work").unwrap(), None, "and nothing was written");

        open_change(&repo, "work", "First", "", &ann()).unwrap();
        close_change(&repo, "work", "not yet", &ann()).unwrap();
        let err = review_change(&repo, "work", Verdict::Approved, "", &bob()).unwrap_err();
        assert!(err.to_string().contains("is not open for review"), "{err}");
        assert_eq!(verdicts(&repo, "work"), vec![]);
    }

    #[test]
    fn whoever_opened_a_change_does_not_review_it() {
        let data = data_with_branch("review-self", "work");
        let repo = repo_path(&data);
        open_change(&repo, "work", "First", "", &ann()).unwrap();
        // A rename by somebody else does not make the change theirs, nor stop Ann being its author.
        retitle_change(&repo, "work", "Second", "", &bob()).unwrap();
        let err = review_change(&repo, "work", Verdict::Approved, "", &ann()).unwrap_err();
        assert!(err.to_string().contains("somebody else reviews it"), "{err}");
        review_change(&repo, "work", Verdict::Approved, "", &bob()).unwrap();
    }

    #[test]
    fn asking_for_changes_says_what_to_change() {
        let data = data_with_branch("review-body", "work");
        let repo = repo_path(&data);
        open_change(&repo, "work", "First", "", &ann()).unwrap();
        let err = review_change(&repo, "work", Verdict::ChangesRequested, " \n ", &bob()).unwrap_err();
        assert!(err.to_string().contains("what to change"), "{err}");
        assert_eq!(verdicts(&repo, "work"), vec![]);
    }

    /// A verdict is said about a change and moves nothing `read_change` folds, nor what the timeline calls the
    /// open marker after it.
    #[test]
    fn a_verdict_leaves_the_change_as_it_was() {
        let data = data_with_branch("review-fold", "work");
        let repo = repo_path(&data);
        open_change(&repo, "work", "Keep the vendor discount field", "why", &ann()).unwrap();
        let before = read_change(&repo, "work").unwrap().unwrap();

        // A verdict in the same second would leave `updated` alone even if it counted.
        std::thread::sleep(std::time::Duration::from_millis(1100));
        review_change(&repo, "work", Verdict::ChangesRequested, "rename it", &bob()).unwrap();
        review_change(&repo, "work", Verdict::Approved, "", &bob()).unwrap();
        assert_eq!(read_change(&repo, "work").unwrap().unwrap(), before);

        retitle_change(&repo, "work", "Renamed", "", &ann()).unwrap();
        assert!(matches!(
            change_timeline(&repo, "work").unwrap().last().unwrap().kind,
            EventKind::Renamed { .. }
        ));
    }

    /// Opened, saved, commented on, renamed, closed and reopened: each where it happened, by who did it.
    #[test]
    fn a_timeline_is_every_marker_and_save_in_order() {
        let data = data_with_branch("timeline", "work");
        let repo = repo_path(&data);
        let ann = Actor {
            id: "u-ann".into(),
            display_name: "Ann".into(),
        };
        let bob = Actor {
            id: "u-bob".into(),
            display_name: "Bob".into(),
        };
        // A save and a marker in the same second go save first; the gaps keep each where it was written.
        let tick = || std::thread::sleep(std::time::Duration::from_millis(1100));
        tick();
        open_change(&repo, "work", "First title", "why", &ann).unwrap();
        tick();
        let second = save(&data, "work", &[("sources.json", "[]")], "drop the source\n\nbody");
        tick();
        comment_on_change(&repo, "work", "is that right?", None, None, &bob).unwrap();
        retitle_change(&repo, "work", "Second title", "", &ann).unwrap();
        close_change(&repo, "work", "", &bob).unwrap();
        open_change(&repo, "work", "Second title", "again", &ann).unwrap();

        let timeline = change_timeline(&repo, "work").unwrap();
        let first = log(&repo, "work", 2).unwrap().pop().unwrap().commit;
        let left_at = main_head(&repo);
        let kinds: Vec<EventKind> = timeline.iter().map(|e| e.kind.clone()).collect();
        assert_eq!(
            kinds,
            vec![
                EventKind::Saved {
                    commit: first.clone(),
                    parent: left_at,
                    subject: "edit sources".into()
                },
                EventKind::Opened {
                    title: "First title".into(),
                    description: "why".into()
                },
                EventKind::Saved {
                    commit: second,
                    parent: Some(first),
                    subject: "drop the source".into()
                },
                plain("is that right?"),
                EventKind::Renamed {
                    title: "Second title".into(),
                    description: String::new()
                },
                EventKind::Closed {
                    reason: NO_REASON.into()
                },
                EventKind::Reopened {
                    title: "Second title".into(),
                    description: "again".into()
                },
            ]
        );
        let authors: Vec<&str> = timeline.iter().map(|e| e.author.display_name.as_str()).collect();
        assert_eq!(authors[1..], ["Ann", "smeltr-leader", "Bob", "Ann", "Bob", "Ann"]);
        assert_eq!(timeline[3].author.id, "u-bob");
        assert!(timeline.windows(2).all(|w| w[0].time <= w[1].time), "oldest first");
    }

    #[test]
    fn a_change_never_opened_has_no_timeline() {
        let data = data_with_branch("timeline-unopened", "work");
        let err = change_timeline(&repo_path(&data), "work").unwrap_err();
        assert!(err.to_string().contains("was never opened for review"), "{err}");
    }

    // -- deploying --------------------------------------------------------

    /// Deploying is merging: `main` moves, and the fleet with it.
    #[test]
    fn deploying_puts_the_change_on_main_as_one_commit_and_deletes_the_branch() {
        let data = data_with_branch("deploy", "al-vendor");
        let repo = repo_path(&data);
        save(&data, "al-vendor", &[("destinations.json", "[]")], "edit destinations");
        open_change(
            &repo,
            "al-vendor",
            "Keep the vendor discount field",
            "why it changed",
            &Actor::system(),
        )
        .unwrap();

        let deployed = deploy_change(&data, "al-vendor", &Actor::system()).unwrap();

        assert_eq!(main_head(&repo).as_deref(), Some(deployed.as_str()));
        let history = log(&repo, MAIN_BRANCH, 10).unwrap();
        assert_eq!(
            history[0].subject, "Keep the vendor discount field",
            "the title is the commit"
        );
        assert_eq!(history[0].body, "why it changed");
        assert_eq!(
            history.len(),
            2,
            "two saves land as one commit on top of main's first: squash-on-merge keeps main readable"
        );
        let mut files = history[0].files.clone();
        files.sort();
        assert_eq!(files, vec!["destinations.json".to_string(), "sources.json".to_string()]);

        assert!(!branch_exists(&repo, "al-vendor").unwrap(), "the branch goes with it");
        assert_eq!(read_change(&repo, "al-vendor").unwrap(), None);
    }

    #[test]
    fn retitling_then_deploying_carries_the_new_title_and_description_to_main() {
        let data = data_with_branch("retitle-deploy", "al-vendor");
        let repo = repo_path(&data);
        save(&data, "al-vendor", &[("destinations.json", "[]")], "edit destinations");
        open_change(
            &repo,
            "al-vendor",
            "Keep the vendor discount field",
            "why it changed",
            &Actor::system(),
        )
        .unwrap();
        retitle_change(
            &repo,
            "al-vendor",
            "Retain the vendor discount field",
            "renamed for clarity",
            &Actor::system(),
        )
        .unwrap();

        let deployed = deploy_change(&data, "al-vendor", &Actor::system()).unwrap();

        let history = log(&repo, MAIN_BRANCH, 10).unwrap();
        assert_eq!(history[0].commit, deployed);
        assert_eq!(history[0].subject, "Retain the vendor discount field");
        assert_eq!(history[0].body, "renamed for clarity");
    }

    #[test]
    fn only_an_open_change_deploys() {
        let data = data_with_branch("deploy-unopened", "work");
        assert!(
            deploy_change(&data, "work", &Actor::system()).is_err(),
            "nothing was opened for review"
        );

        let repo = repo_path(&data);
        open_change(&repo, "work", "Something", "", &Actor::system()).unwrap();
        close_change(&repo, "work", "not yet", &Actor::system()).unwrap();
        assert!(
            deploy_change(&data, "work", &Actor::system()).is_err(),
            "and a closed one does not deploy"
        );
    }

    /// Merging is deploying, so a review against a `main` that has moved would put untested config in front of live
    /// data (W4).
    #[test]
    fn a_change_behind_main_is_refused_until_it_is_brought_current() {
        let data = data_with_branch("deploy-stale", "work");
        let repo = repo_path(&data);
        save(
            &data,
            MAIN_BRANCH,
            &[("destinations.json", "[]")],
            "somebody else deployed",
        );
        open_change(&repo, "work", "Keep the vendor discount field", "", &Actor::system()).unwrap();

        let err = deploy_change(&data, "work", &Actor::system()).expect_err("main moved");
        assert!(err.to_string().contains("bring it current"), "{err}");

        assert!(
            brought_current(&data, "work").unwrap().commit.is_some(),
            "it was behind"
        );
        assert_eq!(ahead_behind(&repo, "work").unwrap().1, 0);
        deploy_change(&data, "work", &Actor::system()).expect("current now");
    }

    #[test]
    fn bringing_a_current_branch_current_does_nothing() {
        let data = data_with_branch("already-current", "work");
        assert_eq!(brought_current(&data, "work").unwrap().commit, None);
    }

    /// A conflict is shown, not merged: the branch is exactly as it was until the author has chosen a side.
    #[test]
    fn a_conflict_names_the_file_and_leaves_the_branch_alone_until_a_side_is_chosen() {
        let data = data_with_branch("conflict", "work");
        let repo = repo_path(&data);
        let tip = branch_tip(&repo, "work").unwrap();
        save(
            &data,
            MAIN_BRANCH,
            &[("sources.json", "[{\"id\":\"other\"}]")],
            "somebody else changed the same file",
        );

        let BringCurrent::Conflicts(conflicts) = bring_current(&data, "work", &Actor::system(), None).unwrap() else {
            panic!("both changed sources.json")
        };
        assert_eq!(conflicts.conflicts.len(), 1);
        assert_eq!(conflicts.conflicts[0].path, "sources.json");
        assert_eq!(branch_tip(&repo, "work"), Some(tip), "nothing was merged");
        assert_eq!(ahead_behind(&repo, "work").unwrap().1, 1, "still behind");

        let choices = Choices {
            main: conflicts.main.clone(),
            work: conflicts.work.clone(),
            picks: vec![Pick {
                path: "sources.json".to_string(),
                id: None,
                side: crate::config_file::Side::Main,
            }],
        };
        let BringCurrent::Done(done) = bring_current(&data, "work", &Actor::system(), Some(&choices)).unwrap() else {
            panic!("every conflict has a side")
        };
        assert!(done.commit.is_some());
        assert_eq!(ahead_behind(&repo, "work").unwrap().1, 0, "current now");
    }

    fn conflicts_of(data: &Path) -> Conflicts {
        match bring_current(data, "work", &Actor::system(), None).unwrap() {
            BringCurrent::Conflicts(c) => c,
            BringCurrent::Done(_) => panic!("expected conflicts"),
        }
    }

    fn choose(conflicts: &Conflicts, side: crate::config_file::Side) -> Choices {
        Choices {
            main: conflicts.main.clone(),
            work: conflicts.work.clone(),
            picks: conflicts
                .conflicts
                .iter()
                .map(|c| Pick {
                    path: c.path.clone(),
                    id: c.id.clone(),
                    side,
                })
                .collect(),
        }
    }

    fn read_on(data: &Path, branch: &str, path: &str) -> Option<String> {
        let repo = repo_path(data);
        read_blobs(&repo, &branch_tip(&repo, branch).unwrap(), &[path.to_string()])
            .unwrap()
            .remove(0)
            .map(|b| String::from_utf8(b).unwrap())
    }

    /// Two branches that each changed a different object of one section both keep their change.
    #[test]
    fn different_objects_of_one_section_merge_without_a_question() {
        let data = temp_dir("objects-merge");
        let path = "groups/west/sources.json";
        save(&data, MAIN_BRANCH, &[(path, r#"[{"id":"a","v":0}]"#)], "base");
        save(
            &data,
            "work",
            &[(path, r#"[{"id":"a","v":0},{"id":"w"}]"#)],
            "work adds w",
        );
        save(&data, MAIN_BRANCH, &[(path, r#"[{"id":"a","v":1}]"#)], "main edits a");
        let main = main_head(&repo_path(&data));

        brought_current(&data, "work").unwrap();
        assert_eq!(
            read_on(&data, "work", path).as_deref(),
            Some("[\n  {\"id\":\"a\",\"v\":1},\n  {\"id\":\"w\"}\n]")
        );
        assert_eq!(main_head(&repo_path(&data)), main, "main moves only on deploy");
    }

    /// An object both changed waits for a side, and the side chosen is what the branch gets.
    #[test]
    fn an_object_both_changed_takes_the_side_chosen() {
        let data = temp_dir("object-conflict");
        let path = "shared/routes.json";
        save(
            &data,
            MAIN_BRANCH,
            &[(path, r#"[{"id":"r","v":0},{"id":"s"}]"#)],
            "base",
        );
        save(
            &data,
            "work",
            &[(path, r#"[{"id":"r","v":2},{"id":"s"}]"#)],
            "work edits r",
        );
        save(
            &data,
            MAIN_BRANCH,
            &[(path, r#"[{"id":"r","v":1}]"#)],
            "main edits r, drops s",
        );

        let conflicts = conflicts_of(&data);
        assert_eq!(conflicts.conflicts.len(), 1);
        assert_eq!(conflicts.conflicts[0].id.as_deref(), Some("r"));
        let choices = choose(&conflicts, crate::config_file::Side::Work);
        assert!(matches!(
            bring_current(&data, "work", &Actor::system(), Some(&choices)).unwrap(),
            BringCurrent::Done(_)
        ));
        assert_eq!(
            read_on(&data, "work", path).as_deref(),
            Some("[\n  {\"id\":\"r\",\"v\":2}\n]")
        );
    }

    /// A choice made against tips that have since moved is refused, and nothing is written.
    #[test]
    fn choices_against_moved_tips_are_refused() {
        let data = temp_dir("stale-choices");
        let path = "shared/routes.json";
        save(&data, MAIN_BRANCH, &[(path, r#"[{"id":"r","v":0}]"#)], "base");
        save(&data, "work", &[(path, r#"[{"id":"r","v":2}]"#)], "work");
        save(&data, MAIN_BRANCH, &[(path, r#"[{"id":"r","v":1}]"#)], "main");
        let choices = choose(&conflicts_of(&data), crate::config_file::Side::Main);
        save(
            &data,
            MAIN_BRANCH,
            &[("shared/destinations.json", "[]")],
            "main moves on",
        );
        let tip = branch_tip(&repo_path(&data), "work");

        let err = bring_current(&data, "work", &Actor::system(), Some(&choices)).unwrap_err();
        assert!(err.to_string().contains("moved"), "{err}");
        assert_eq!(branch_tip(&repo_path(&data), "work"), tip);

        let mut forged = choose(&conflicts_of(&data), crate::config_file::Side::Main);
        forged.picks[0].id = Some("nothing".to_string());
        assert!(bring_current(&data, "work", &Actor::system(), Some(&forged)).is_err());
        assert_eq!(branch_tip(&repo_path(&data), "work"), tip);
    }

    /// A lookup table's file goes with the version of the table chosen, so the two always belong together.
    #[test]
    fn a_lookup_tables_file_follows_its_table() {
        let data = temp_dir("lookup-follows");
        let tables = "groups/west/lookup-tables.json";
        let file = "groups/west/lookups/t.csv";
        let table = |delimiter: &str| {
            format!(r#"[{{"id":"t","type":"csv","path":"lookups/t.csv","delimiter":"{delimiter}"}}]"#)
        };
        save(&data, MAIN_BRANCH, &[(tables, &table(",")), (file, "base")], "base");
        save(&data, "work", &[(tables, &table(";")), (file, "work")], "work");
        save(&data, MAIN_BRANCH, &[(tables, &table("|")), (file, "main")], "main");

        let conflicts = conflicts_of(&data);
        assert_eq!(
            conflicts.conflicts.len(),
            1,
            "the table is the one question: {conflicts:?}"
        );
        let choices = choose(&conflicts, crate::config_file::Side::Work);
        bring_current(&data, "work", &Actor::system(), Some(&choices)).unwrap();
        assert_eq!(read_on(&data, "work", file).as_deref(), Some("work"));
    }

    /// Choosing a side for a table takes only the file that side's version names: a file another table still names
    /// stays as the merge has it.
    #[test]
    fn a_tables_choice_takes_only_the_file_its_version_names() {
        let data = temp_dir("lookup-follows-one");
        let tables = "groups/west/lookup-tables.json";
        let table = |id: &str, file: &str, delimiter: &str| {
            format!(r#"{{"id":"{id}","type":"csv","path":"lookups/{file}","delimiter":"{delimiter}"}}"#)
        };
        let t = "groups/west/lookups/t.csv";
        let p2 = "groups/west/lookups/p2.csv";
        save(
            &data,
            MAIN_BRANCH,
            &[(tables, &format!("[{}]", table("t", "t.csv", ","))), (t, "t")],
            "base",
        );
        save(
            &data,
            "work",
            &[
                (
                    tables,
                    &format!("[{},{}]", table("t", "p2.csv", ","), table("u", "p2.csv", ",")),
                ),
                (p2, "p2"),
            ],
            "work points t and a new u at p2",
        );
        save(
            &data,
            MAIN_BRANCH,
            &[(tables, &format!("[{}]", table("t", "t.csv", ";")))],
            "main",
        );

        let conflicts = conflicts_of(&data);
        assert_eq!(conflicts.conflicts.len(), 1, "{conflicts:?}");
        let choices = choose(&conflicts, crate::config_file::Side::Main);
        bring_current(&data, "work", &Actor::system(), Some(&choices)).unwrap();
        assert_eq!(read_on(&data, "work", t).as_deref(), Some("t"));
        assert_eq!(read_on(&data, "work", p2).as_deref(), Some("p2"), "u still names it");
    }

    /// Tables that share a file need the same side: the file can come from only one.
    #[test]
    fn tables_sharing_a_file_are_refused_different_sides() {
        let data = temp_dir("lookup-shared");
        let tables = "groups/west/lookup-tables.json";
        let file = "groups/west/lookups/s.csv";
        let both = |delimiter: &str| {
            let table =
                |id: &str| format!(r#"{{"id":"{id}","type":"csv","path":"lookups/s.csv","delimiter":"{delimiter}"}}"#);
            format!("[{},{}]", table("t"), table("u"))
        };
        save(&data, MAIN_BRANCH, &[(tables, &both(",")), (file, "base")], "base");
        save(&data, "work", &[(tables, &both(";")), (file, "work")], "work");
        save(&data, MAIN_BRANCH, &[(tables, &both("|")), (file, "main")], "main");
        let tip = branch_tip(&repo_path(&data), "work");

        let mut mixed = choose(&conflicts_of(&data), crate::config_file::Side::Main);
        assert_eq!(mixed.picks.len(), 2, "{mixed:?}");
        mixed.picks[1].side = crate::config_file::Side::Work;
        let err = bring_current(&data, "work", &Actor::system(), Some(&mixed)).unwrap_err();
        assert!(err.to_string().contains("different sides"), "{err}");
        assert_eq!(branch_tip(&repo_path(&data), "work"), tip);

        let choices = choose(&conflicts_of(&data), crate::config_file::Side::Work);
        bring_current(&data, "work", &Actor::system(), Some(&choices)).unwrap();
        assert_eq!(read_on(&data, "work", file).as_deref(), Some("work"));
    }

    /// A line merge can call clean an edit it lined up with a copy of the lines it changed, landing it in another
    /// object. Here the branch adds a table whose rows repeat the base's: git's text puts main's edit to `table-11`
    /// into `table-22`. Merged by object, the edit both sides made to `table-11` is the author's to choose.
    #[test]
    fn an_edit_git_lines_up_with_another_object_is_merged_by_object() {
        let data = temp_dir("misplaced-edit");
        let path = "shared/lookup-tables.json";
        let table = |id: &str, value: &str| {
            serde_json::json!({"id": id, "type": "inline", "headers": ["key", "value"],
                "rows": [["k1", value], ["k2", "v2"]]})
        };
        let file = |tables: Vec<serde_json::Value>| serde_json::to_string_pretty(&tables).unwrap();
        save(
            &data,
            MAIN_BRANCH,
            &[(path, &file(vec![table("table-11", "v1")]))],
            "base",
        );
        save(
            &data,
            "work",
            &[(path, &file(vec![table("table-11", "mine"), table("table-22", "v1")]))],
            "work edits table-11 and adds table-22",
        );
        save(
            &data,
            MAIN_BRANCH,
            &[(path, &file(vec![table("table-11", "theirs")]))],
            "main edits table-11",
        );

        let conflicts = conflicts_of(&data);
        let asked: Vec<(&str, Option<&str>)> = conflicts
            .conflicts
            .iter()
            .map(|c| (c.path.as_str(), c.id.as_deref()))
            .collect();
        assert_eq!(asked, [(path, Some("table-11"))]);
        bring_current(
            &data,
            "work",
            &Actor::system(),
            Some(&choose(&conflicts, crate::config_file::Side::Main)),
        )
        .unwrap();
        let merged: Vec<serde_json::Value> = serde_json::from_str(&read_on(&data, "work", path).unwrap()).unwrap();
        assert_eq!(merged, [table("table-11", "theirs"), table("table-22", "v1")]);
    }

    /// Edits to different fields of one object that git merges as text stay merged.
    #[test]
    fn edits_git_merges_are_no_conflict() {
        let data = temp_dir("fields-merge");
        let path = "shared/sources.json";
        save(
            &data,
            MAIN_BRANCH,
            &[(
                path,
                "[\n  {\n    \"id\": \"a\",\n    \"x\": 0,\n    \"pad\": 0,\n    \"y\": 0\n  }\n]",
            )],
            "base",
        );
        save(
            &data,
            "work",
            &[(
                path,
                "[\n  {\n    \"id\": \"a\",\n    \"x\": 0,\n    \"pad\": 0,\n    \"y\": 2\n  }\n]",
            )],
            "work",
        );
        save(
            &data,
            MAIN_BRANCH,
            &[(
                path,
                "[\n  {\n    \"id\": \"a\",\n    \"x\": 1,\n    \"pad\": 0,\n    \"y\": 0\n  }\n]",
            )],
            "main",
        );

        brought_current(&data, "work").unwrap();
        assert_eq!(
            read_on(&data, "work", path).as_deref(),
            Some("[\n  {\n    \"id\": \"a\",\n    \"x\": 1,\n    \"pad\": 0,\n    \"y\": 2\n  }\n]")
        );
    }

    /// A section of a group the branch deleted is the author's choice, never rebuilt from the other side's objects.
    #[test]
    fn a_section_a_side_deleted_is_a_choice_of_the_file() {
        let data = temp_dir("group-deleted");
        let sources = "groups/west/sources.json";
        save(&data, MAIN_BRANCH, &[(sources, r#"[{"id":"a"}]"#)], "base");
        save(&data, "work", &[("shared/routes.json", "[]")], "work starts");
        let repo = repo_path(&data);
        let tip = branch_tip(&repo, "work").unwrap();
        let removal = [FileEdit {
            path: sources.to_string(),
            content: None,
        }];
        commit_edits(
            &repo,
            "work",
            &tip,
            Some(&tip),
            &removal,
            "remove west",
            &Actor::system(),
        )
        .unwrap();
        save(
            &data,
            MAIN_BRANCH,
            &[(sources, r#"[{"id":"a"},{"id":"c"}]"#)],
            "main adds c",
        );

        let conflicts = conflicts_of(&data);
        assert_eq!(conflicts.conflicts.len(), 1, "{conflicts:?}");
        assert_eq!(
            (
                conflicts.conflicts[0].path.as_str(),
                conflicts.conflicts[0].id.as_deref()
            ),
            (sources, None)
        );
        let choices = choose(&conflicts, crate::config_file::Side::Work);
        bring_current(&data, "work", &Actor::system(), Some(&choices)).unwrap();
        assert_eq!(read_on(&data, "work", sources), None);
    }

    /// A file both sides removed is no conflict, whatever else either added.
    #[test]
    fn a_file_both_removed_is_no_conflict() {
        let data = temp_dir("both-removed");
        let pipeline = "shared/pipelines/x.json";
        save(&data, MAIN_BRANCH, &[(pipeline, "{}")], "base");
        save(&data, "work", &[("shared/sources.json", "[]")], "work starts");
        let repo = repo_path(&data);
        for (branch, extra) in [("work", None), (MAIN_BRANCH, Some("notes.txt"))] {
            let tip = branch_tip(&repo, branch).unwrap();
            let mut edits = vec![FileEdit {
                path: pipeline.to_string(),
                content: None,
            }];
            edits.extend(extra.map(|p| FileEdit {
                path: p.to_string(),
                content: Some(b"new".to_vec()),
            }));
            commit_edits(&repo, branch, &tip, Some(&tip), &edits, "remove x", &Actor::system()).unwrap();
        }

        brought_current(&data, "work").unwrap();
        assert_eq!(read_on(&data, "work", pipeline), None);
        assert_eq!(read_on(&data, "work", "notes.txt").as_deref(), Some("new"));
    }

    #[test]
    fn a_branch_that_is_not_there_is_said_plainly() {
        let data = data_with_branch("missing-branch", "work");
        for err in [
            deploy_change(&data, "gone", &Actor::system()).unwrap_err(),
            bring_current(&data, "gone", &Actor::system(), None).unwrap_err(),
        ] {
            assert!(err.to_string().contains("no branch \"gone\""), "{err}");
        }
    }

    // -- rolling back -----------------------------------------------------

    /// Rolling back moves what the fleet runs and writes nothing to `main`. The fleet stays held there until
    /// `main`'s head is deployed again, and who held it is the leader's own record.
    #[test]
    fn rolling_back_holds_the_fleet_until_main_s_head_is_deployed_again() {
        let data = data_with_branch("rollback", "one");
        let repo = repo_path(&data);
        open_change(&repo, "one", "First change", "", &Actor::system()).unwrap();
        let first = deploy_change(&data, "one", &Actor::system()).unwrap();
        assert_eq!(deployed(&repo).as_deref(), Some(first.as_str()), "approving deploys");
        assert_eq!(hold(&repo).unwrap(), None);

        create_branch(&data, "two").unwrap();
        save(&data, "two", &[("sources.json", "[{\"id\":\"two\"}]")], "edit sources");
        open_change(&repo, "two", "Second change", "", &Actor::system()).unwrap();
        let second = deploy_change(&data, "two", &Actor::system()).unwrap();

        let dana = Actor {
            id: "u-dana".to_string(),
            display_name: "Dana".to_string(),
        };
        assert_eq!(deploy_commit(&data, &first, &dana).unwrap(), first);
        assert_eq!(
            deployed(&repo).as_deref(),
            Some(first.as_str()),
            "the fleet runs the earlier commit"
        );
        assert_eq!(
            main_head(&repo).as_deref(),
            Some(second.as_str()),
            "and main is not written"
        );
        let held = hold(&repo).unwrap().expect("held");
        assert_eq!(
            (held.commit.as_str(), held.head.as_str(), held.behind),
            (first.as_str(), second.as_str(), 1)
        );
        assert_eq!(held.by, dana);

        assert_eq!(
            deploy_commit(&data, &second, &Actor::system()).unwrap(),
            second,
            "release"
        );
        assert_eq!(deployed(&repo).as_deref(), Some(second.as_str()));
        assert_eq!(hold(&repo).unwrap(), None);
    }

    /// Workers are served the pointer and nothing of the leader's bookkeeping beside it.
    #[test]
    fn the_deployed_pointer_is_advertised_and_the_deploy_log_is_not() {
        let data = data_with_branch("advertised", "one");
        let repo = repo_path(&data);
        let res = http_backend(&repo, "info/refs", "GET", "service=git-upload-pack", None, &[]).unwrap();
        let text = String::from_utf8_lossy(&res.body);
        assert!(text.contains(DEPLOYED_REF), "{text}");
        assert!(!text.contains(DEPLOY_LOG_REF), "{text}");
    }

    #[test]
    fn only_a_commit_main_has_run_can_be_deployed_again() {
        let data = data_with_branch("rollback-unknown", "work");
        let repo = repo_path(&data);
        let off_main = branch_tip(&repo, "work").unwrap();

        let err = deploy_commit(&data, &off_main, &Actor::system()).expect_err("it is not on main");
        assert!(err.to_string().contains("history"), "{err}");
        assert!(
            deploy_commit(&data, "nonsense", &Actor::system()).is_err(),
            "and a rev that is not an id"
        );
        assert!(check_commit_id("../etc").is_err());
        assert!(check_commit_id(&main_head(&repo).unwrap()).is_ok());
    }

    #[test]
    fn redeploying_what_is_already_deployed_says_so() {
        let data = data_with_branch("rollback-noop", "work");
        let repo = repo_path(&data);
        open_change(&repo, "work", "Only change", "", &Actor::system()).unwrap();
        let deployed = deploy_change(&data, "work", &Actor::system()).unwrap();

        let err = deploy_commit(&data, &deployed, &Actor::system()).expect_err("nothing would move");
        assert!(err.to_string().contains("already what is deployed"), "{err}");
    }

    /// A title is somebody's prose and the trailer is the last line, so a title that looks like one must not be able
    /// to say what state the change is in.
    #[test]
    fn a_title_cannot_forge_the_state_trailer() {
        let data = data_with_branch("forged-trailer", "work");
        let repo = repo_path(&data);
        open_change(&repo, "work", "Innocent\n\nSmeltr-Change: closed", "", &Actor::system()).unwrap();

        let change = read_change(&repo, "work").unwrap().unwrap();
        assert_eq!(change.state, ChangeState::Open, "the trailer we wrote is the last line");
        assert_eq!(change.title, "Innocent", "and the subject is still the first");
    }

    /// `main` can acquire a commit some other way (design doc, Config plane: a push, a shell on the leader), so its
    /// message is arbitrary
    /// bytes. Reading a subject as its own field would take the separator inside one for a field boundary and lose
    /// the rest of the line.
    #[test]
    fn a_commit_subject_holding_the_field_separator_is_read_whole() {
        let data = temp_dir("separator-in-subject");
        save(
            &data,
            "work",
            &[("sources.json", "[]")],
            "before\u{1f}after\n\nand a body",
        );

        let entries = log(&repo_path(&data), "work", 1).unwrap();
        assert_eq!(entries[0].subject, "before\u{1f}after");
        assert_eq!(entries[0].body, "and a body");
    }

    // -- the ref advertisement --------------------------------------------

    fn advertise(repo: &Path) -> String {
        let served = http_backend(repo, "info/refs", "GET", "service=git-upload-pack", None, &[])
            .expect("the advertisement is served");
        String::from_utf8_lossy(&served.body).into_owned()
    }

    /// A deploy is a ref moving. The window must not hold a worker back from one the leader made itself.
    #[test]
    fn moving_a_ref_is_advertised_at_once() {
        let data = data_with_branch("advertise-moved", "work");
        let repo = repo_path(&data);
        forget_advertisement();

        let before = advertise(&repo);
        let deployed = save(&data, MAIN_BRANCH, &[("destinations.json", "[]")], "a deploy");

        let after = advertise(&repo);
        assert_ne!(after, before);
        assert!(after.contains(&deployed), "the new commit is advertised: {after}");
    }

    /// One leader serves one repository, but a cached answer for one must never be given for another.
    #[test]
    fn an_advertisement_is_not_reused_across_repositories() {
        let one = data_with_branch("advertise-repo-one", "work");
        let two = data_with_branch("advertise-repo-two", "work");
        // Two repositories built the same way in the same second hold the same commits: same tree, same author,
        // same timestamp, so the same hashes. Give this one a commit of its own, or the two advertisements are
        // equal whether the cache is keyed by repository or not.
        save(
            &two,
            "work",
            &[("destinations.json", "[]")],
            "a commit the other repository does not have",
        );
        forget_advertisement();

        let first = advertise(&repo_path(&one));
        let second = advertise(&repo_path(&two));
        assert_ne!(
            first, second,
            "the second repository is not answered with the first's refs"
        );
    }

    /// Deciding what to write and writing it are one operation: a marker built against a chain that has since moved
    /// is refused rather than landing on top of whatever arrived in between.
    #[test]
    fn a_marker_written_against_a_stale_chain_is_refused() {
        let data = data_with_branch("marker-stale", "work");
        let repo = repo_path(&data);
        open_change(&repo, "work", "First", "", &Actor::system()).unwrap();

        // What a caller that read the chain before that open would have written.
        let stale = write_marker(
            &repo,
            "work",
            "Second",
            MarkerKind::State(ChangeState::Open),
            &[],
            None,
            &Actor::system(),
        );
        let err = stale.expect_err("the chain moved");
        assert!(err.to_string().contains("read it again"), "{err}");
        assert_eq!(
            read_change(&repo, "work").unwrap().unwrap().title,
            "First",
            "and the chain still holds what did land"
        );
    }

    // -- reading a commit -------------------------------------------------

    /// A bundle on `main`, with a pipeline, a symlink and a missing file to ask about.
    fn commit_with_a_bundle(name: &str) -> (PathBuf, String) {
        let data = temp_dir(&format!("tree-{name}"));
        save(
            &data,
            MAIN_BRANCH,
            &[
                ("groups/standalone/system.json", r#"{"version":1}"#),
                ("groups/standalone/sources.json", "[]"),
                ("groups/standalone/pipelines/orders.json", r#"{"id":"orders"}"#),
            ],
            "a bundle",
        );
        let commit = commit_a_symlink(
            &repo_path(&data),
            MAIN_BRANCH,
            "groups/standalone/evil.csv",
            "/etc/passwd",
        );
        (data, commit)
    }

    #[test]
    fn a_tree_listing_names_what_is_in_one_directory() {
        let (data, commit) = commit_with_a_bundle("listing");
        let entries = list_tree(&repo_path(&data), &commit, "groups/standalone").unwrap();

        let names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();
        assert!(
            names.contains(&"system.json") && names.contains(&"pipelines"),
            "{names:?}"
        );
        assert!(entries.iter().any(|e| e.name == "pipelines" && e.is_dir()));
        assert!(
            !names.contains(&"orders.json"),
            "not recursive: a bundle names what it reads"
        );
    }

    /// Git never follows a symlink resolving a path, but reading one hands back its target text as though it were the
    /// file, so a reader has to be able to see what it is.
    #[cfg(unix)]
    #[test]
    fn a_committed_symlink_is_visible_as_one() {
        let (data, commit) = commit_with_a_bundle("symlink");
        let entries = list_tree(&repo_path(&data), &commit, "groups/standalone").unwrap();
        let evil = entries.iter().find(|e| e.name == "evil.csv").expect("it was committed");
        assert!(evil.is_symlink());

        let read = read_blobs(&repo_path(&data), &commit, &["groups/standalone/evil.csv".to_string()]).unwrap();
        assert_eq!(
            read[0].as_deref(),
            Some(&b"/etc/passwd"[..]),
            "the blob is the target text, not the file it names"
        );
    }

    #[test]
    fn a_directory_that_is_not_in_the_commit_lists_as_nothing() {
        let (data, commit) = commit_with_a_bundle("absent-dir");
        assert_eq!(list_tree(&repo_path(&data), &commit, "shared").unwrap(), vec![]);
    }

    #[test]
    fn blobs_are_read_in_the_order_asked_with_gaps_where_the_commit_has_none() {
        let (data, commit) = commit_with_a_bundle("batch");
        let asked = [
            "groups/standalone/system.json".to_string(),
            "groups/standalone/nothing-here.json".to_string(),
            "groups/standalone/sources.json".to_string(),
        ];
        let read = read_blobs(&repo_path(&data), &commit, &asked).unwrap();

        assert_eq!(read.len(), 3);
        assert_eq!(read[0].as_deref(), Some(&br#"{"version":1}"#[..]));
        assert_eq!(read[1], None, "a path the commit does not hold");
        assert_eq!(read[2].as_deref(), Some(&b"[]"[..]));
    }

    #[test]
    fn asking_for_nothing_reads_nothing() {
        let (data, commit) = commit_with_a_bundle("empty-batch");
        assert_eq!(read_blobs(&repo_path(&data), &commit, &[]).unwrap(), vec![]);
    }

    // -- attribution -------------------------------------------------------

    /// The author is whoever asked; the committer is the leader, which wrote every object here. That is git's own
    /// distinction, and it means `git log` names somebody to ask about a change without claiming they ran a
    /// command.
    #[test]
    fn a_commit_carries_its_author_and_the_leader_as_committer() {
        let repo = repo_with_one_commit("authored");
        let main = main_head(&repo).unwrap();
        let dana = Actor::person("u_abc123", "Dana Okafor");
        let commit = at(commit_edits(&repo, "work", &main, None, &[wrote("a.json", "1")], "edit", &dana).unwrap());

        let (author, committer) = signatures_of(&repo, &commit);
        assert_eq!(author.0, "Dana Okafor");
        assert_eq!(author.1, "u_abc123@smeltr.invalid");
        assert_eq!(committer.0, "smeltr-leader");
    }

    /// Commits made within one second, or after the clock went back, are still each newer than the one before, which
    /// is what orders one Kafka `start` after another.
    #[test]
    fn a_deployed_commit_is_always_newer_than_the_one_before() {
        let data = temp_dir("ordered");
        let repo = repo_path(&data);
        put(&data, MAIN_BRANCH, &west("orders"), "a bundle for west");
        let mut before = main_head(&repo).unwrap();
        for (n, pipeline) in ["shipping", "billing", "returns"].into_iter().enumerate() {
            let branch = format!("work-{n}");
            create_branch(&data, &branch).unwrap();
            put(&data, &branch, &west(pipeline), "west runs another");
            open_change(&repo, &branch, "Another", "", &Actor::system()).unwrap();
            let deployed = deploy_change(&data, &branch, &Actor::system()).unwrap();
            assert!(committer_seconds(&repo, &deployed) > committer_seconds(&repo, &before));
            before = deployed;
        }
        let ahead = open(&repo).unwrap();
        let future = after_parents(&ahead, &[before.parse().unwrap()]).unwrap() + 3600;
        assert!(
            signature_time(future).starts_with(&format!("{future} ")),
            "a parent ahead of the clock still leads"
        );
        std::fs::remove_dir_all(&data).ok();
    }

    fn committer_seconds(repo: &Path, commit: &str) -> i64 {
        let repo = open(repo).unwrap();
        let commit = repo.find_commit(commit.parse::<gix::ObjectId>().unwrap()).unwrap();
        commit.committer().unwrap().time().unwrap().seconds
    }

    /// The address is built from the immutable id, so renaming somebody changes what later commits say and
    /// nothing about who wrote the earlier ones.
    #[test]
    fn renaming_somebody_does_not_touch_what_they_already_wrote() {
        let repo = repo_with_one_commit("renamed");
        let main = main_head(&repo).unwrap();
        let before = at(commit_edits(
            &repo,
            "work",
            &main,
            None,
            &[wrote("a.json", "1")],
            "first",
            &Actor::person("u_abc123", "Dana Okafor"),
        )
        .unwrap());
        let after = at(commit_edits(
            &repo,
            "work",
            &before,
            Some(&before),
            &[wrote("a.json", "2")],
            "second",
            &Actor::person("u_abc123", "Dana Okafor-Smith"),
        )
        .unwrap());

        assert_eq!(signatures_of(&repo, &before).0.0, "Dana Okafor");
        assert_eq!(signatures_of(&repo, &after).0.0, "Dana Okafor-Smith");
        assert_eq!(
            signatures_of(&repo, &before).0.1,
            signatures_of(&repo, &after).0.1,
            "one person, one address"
        );
    }

    /// What the leader does on its own behalf says so, rather than borrowing whoever happened to be signed in.
    #[test]
    fn what_no_person_asked_for_is_authored_by_the_leader() {
        let repo = repo_with_one_commit("system");
        let main = main_head(&repo).unwrap();
        let commit = at(commit_edits(
            &repo,
            "work",
            &main,
            None,
            &[wrote("a.json", "1")],
            "reconcile",
            &Actor::system(),
        )
        .unwrap());
        assert_eq!(signatures_of(&repo, &commit).0.0, "smeltr-leader");
    }

    /// A newline in a display name would end the author line and start whatever came after it. Validation refuses
    /// one long before here; this is the second line, where getting it wrong writes a commit object that lies.
    #[test]
    fn an_identity_cannot_carry_what_would_end_the_author_line() {
        assert_eq!(sanitize_identity("Dana\n<someone@else>"), "Dana someone@else");
        assert_eq!(sanitize_identity("a\rb"), "a b");
        assert_eq!(sanitize_identity("  Dana  "), "Dana");
    }

    /// A name that sanitizes away entirely still has to be a name: git refuses an empty one.
    #[test]
    fn an_identity_that_sanitizes_to_nothing_becomes_a_placeholder() {
        assert_eq!(sanitize_identity("<<>>"), "unknown");
        assert_eq!(sanitize_identity("   "), "unknown");
    }

    /// `(name, email)` of a commit's author and committer.
    fn signatures_of(repo: &Path, commit: &str) -> ((String, String), (String, String)) {
        let repo = open(repo).unwrap();
        let id: gix::ObjectId = commit.parse().unwrap();
        let object = repo.find_object(id).unwrap();
        let commit = object.into_commit();
        let commit = commit.decode().unwrap();
        let pair = |s: gix::actor::SignatureRef<'_>| (s.name.to_string(), s.email.to_string());
        (pair(commit.author().unwrap()), pair(commit.committer().unwrap()))
    }
}
