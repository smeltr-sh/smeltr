//! Bringing a branch current with `main`: git's tree merge, with each section file git could not merge merged by
//! object (`config_file::merge_section`). What neither can settle is a conflict the author chooses a side for.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::*;
use crate::config_file::{self, SectionMerge};

/// What bringing a branch current did.
#[derive(Debug)]
pub struct BroughtCurrent {
    /// The merge commit, or `None` when the branch was already current.
    pub commit: Option<String>,
    /// Set when the merged branch does not load. Not a refusal: the merge landed, and this says what to fix.
    pub does_not_load: Option<String>,
}

#[derive(Debug)]
pub enum BringCurrent {
    Done(BroughtCurrent),
    /// Nothing was written: these need a side chosen first.
    Conflicts(Conflicts),
}

/// What needs a choice, between the commits it was found at. A choice is only good against these same two tips.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Conflicts {
    pub main: String,
    pub work: String,
    pub conflicts: Vec<Conflict>,
}

/// One path both sides changed, or one object of a section file (`id`). Each version is its text, `None` on a side
/// that does not have it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Conflict {
    pub path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    pub base: Option<String>,
    pub main: Option<String>,
    pub work: Option<String>,
}

/// The author's answer to `Conflicts`: the two tips they were shown, and a side for every conflict.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Choices {
    pub main: String,
    pub work: String,
    pub picks: Vec<Pick>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Pick {
    pub path: String,
    #[serde(default)]
    pub id: Option<String>,
    pub side: config_file::Side,
}

/// Bring `branch` up to date with `main`, so its review is against what is running now.
///
/// Without `choices`, a merge that needs none lands, and one that does is returned as `Conflicts` with nothing
/// written. With them, the merge lands only if `main` and the branch are still where the choices were made and
/// every conflict has one. The conflict belongs to the author, who wrote the content (W4). A merge that lands but
/// does not load is `does_not_load` rather than an error -- see the comment at the check.
pub fn bring_current(
    data_dir: &Path,
    branch: &str,
    author: &Actor,
    choices: Option<&Choices>,
) -> Result<BringCurrent, GitError> {
    check_branch_name(branch)?;
    if branch == MAIN_BRANCH {
        return Err(GitError::invalid(format!(
            "{MAIN_BRANCH} is what everything else is brought current with"
        )));
    }
    let path = repo_path(data_dir);
    if !branch_exists(&path, branch)? {
        return Err(GitError::not_found(format!("there is no branch {branch:?}")));
    }
    let repo = open(&path)?;
    let work = resolve(&repo, &format!("refs/heads/{branch}"))?;
    let main = resolve(&repo, &format!("refs/heads/{MAIN_BRANCH}"))?;
    if let Some(choices) = choices
        && (choices.main != main || choices.work != work)
    {
        return Err(GitError::conflict(format!(
            "{MAIN_BRANCH} or this branch moved since the conflicts were shown -- bring it current again to see \
             them as they are now"
        )));
    }
    if ahead_behind_in(&repo, branch)?.1 == 0 {
        return Ok(BringCurrent::Done(BroughtCurrent {
            commit: None,
            does_not_load: None,
        }));
    }

    // The merge is computed into the object database, so a conflict produces nothing to clean up: the branch is as
    // it was. Renames are not followed: a bundle names its files by what they hold, so a moved file is a removal and
    // an addition, which is how a conflict over one reads to its author too.
    let failed = |e: String| GitError::failed(format!("merging {MAIN_BRANCH} into {branch:?}: {e}"));
    let options = repo
        .tree_merge_options()
        .map_err(|e| failed(e.to_string()))?
        .with_rewrites(None);
    let mut merged = repo
        .merge_commits(work, main, Default::default(), options.into())
        .map_err(|e| failed(e.to_string()))?;

    let picks: BTreeMap<(&str, Option<&str>), config_file::Side> = choices
        .map(|c| {
            c.picks
                .iter()
                .map(|p| ((p.path.as_str(), p.id.as_deref()), p.side))
                .collect()
        })
        .unwrap_or_default();
    let mut found = Found::default();
    let mut conflicted: BTreeSet<String> = conflicted_paths(&merged.tree_merge).into_iter().collect();
    let trees = Trees::of(&repo, work, main)?;

    // A section file git merged as text stands only where it kept what each side did to every object. Otherwise it
    // is merged like one git could not merge. Without one base there is nothing to check it against.
    for (file, blob) in text_merged(&merged.tree_merge) {
        if !config_file::is_section_file(&file) {
            continue;
        }
        let kept = match (trees.base, trees.bytes(&repo, &file)?) {
            (Some(_), [Some(b), Some(m), Some(w)]) => {
                let text = repo.find_blob(blob).map_err(|e| failed(e.to_string()))?;
                config_file::text_merge_keeps_each_side(&b, &m, &w, &text.data)
            }
            _ => false,
        };
        if !kept {
            conflicted.insert(file);
        }
    }

    // A section file git could not merge is merged by object, so two edits to different objects of it both stand.
    // Only against one base: criss-crossed history has several, and an object both sides match against the wrong one
    // would be taken silently -- so there the file is the author's choice.
    if trees.base.is_some() {
        for file in conflicted.iter().filter(|p| config_file::is_section_file(p)) {
            let [b, m, w] = trees.bytes(&repo, file)?;
            let (bundle, name) = file.rsplit_once('/').unwrap_or(("", file.as_str()));
            let pick = |id: &str| picks.get(&(file.as_str(), Some(id))).copied();
            let SectionMerge::Objects { conflicts, merged } =
                config_file::merge_section(name, b.as_deref(), m.as_deref(), w.as_deref(), pick)
            else {
                continue;
            };
            found.by_object.insert(file.clone());
            if let Some(bytes) = merged {
                found.write(file, Some(bytes));
            }
            for c in conflicts {
                // A lookup table's file goes with the version of the table chosen. Until there is a choice, both
                // versions' files wait on it.
                let chosen = pick(&c.id);
                let named = match chosen {
                    Some(side) => vec![c.names(side)],
                    None => vec![c.names(config_file::Side::Main), c.names(config_file::Side::Work)],
                };
                for named in named.into_iter().flatten() {
                    let named = format!("{bundle}/{named}");
                    // Tables can share a file, and the file can come from only one side.
                    let side = found.follows.entry(named.clone()).or_insert(chosen);
                    if chosen.is_some() && side.is_some() && *side != chosen {
                        return Err(GitError::invalid(format!(
                            "the lookup tables that use {named} were given different sides -- keep the same side for each"
                        )));
                    }
                    *side = side.or(chosen);
                }
                found.conflicts.push(Conflict {
                    path: file.clone(),
                    id: Some(c.id),
                    base: c.base,
                    main: c.main,
                    work: c.work,
                });
            }
        }
        for (file, side) in found.follows.clone() {
            if let Some(side) = side {
                found.take(&repo, &trees, &file, side)?;
            }
        }
    }

    let unsettled: Vec<&String> = conflicted.iter().filter(|p| !found.settled(p)).collect();
    for file in unsettled {
        let [b, m, w] = trees.bytes(&repo, file)?;
        let text = |bytes: Option<Vec<u8>>| bytes.map(|b| String::from_utf8_lossy(&b).into_owned());
        found.conflicts.push(Conflict {
            path: file.clone(),
            id: None,
            base: text(b),
            main: text(m),
            work: text(w),
        });
        if let Some(side) = picks.get(&(file.as_str(), None)) {
            found.take(&repo, &trees, file, *side)?;
        }
    }

    let listed: BTreeSet<(&str, Option<&str>)> = found
        .conflicts
        .iter()
        .map(|c| (c.path.as_str(), c.id.as_deref()))
        .collect();
    if let Some((path, id)) = picks.keys().find(|k| !listed.contains(*k)) {
        return Err(GitError::conflict(format!(
            "{path}{} is not a conflict here -- bring it current again to see the conflicts as they are now",
            id.map(|id| format!(" ({id})")).unwrap_or_default()
        )));
    }
    if listed.len() > picks.len() {
        return Ok(BringCurrent::Conflicts(Conflicts {
            main: main.to_string(),
            work: work.to_string(),
            conflicts: found.conflicts,
        }));
    }

    for (file, content) in found.writes {
        match content {
            Some(Content::Blob(mode, id)) => merged.tree_merge.tree.upsert(file.as_str(), mode.kind(), id),
            Some(Content::Bytes(bytes)) => {
                let id = repo.write_blob(&bytes).map_err(|e| failed(e.to_string()))?.detach();
                merged
                    .tree_merge
                    .tree
                    .upsert(file.as_str(), gix::objs::tree::EntryKind::Blob, id)
            }
            None => merged.tree_merge.tree.remove(file.as_str()),
        }
        .map_err(|e| failed(e.to_string()))?;
    }
    let tree = merged
        .tree_merge
        .tree
        .write()
        .map_err(|e| failed(e.to_string()))?
        .detach();

    // A clean merge is not a working one. Both sides can be individually valid and refer to each other through what
    // the other removed -- a route naming a pipeline the branch renamed -- and git has no reason to notice. Checked
    // before anything points at the tree, so a merge that would not run leaves the branch as it was.
    // Reported, not refused. A branch is work in progress, and a merge is how its author first sees what `main`
    // did to their change -- refusing would leave them unable to look at the thing they have to fix, and unable to
    // fix it beforehand, since the fix names what only `main` holds. Deploy is where this is a refusal.
    let broken = groups_that_do_not_load(&path, &tree.to_string())?
        .into_iter()
        .next()
        .map(|(group, why)| {
            format!("worker group {group:?} does not load on this branch now: {why}. Fix it here before deploying")
        });

    let commit = commit_tree_in(
        &repo,
        tree,
        &[work, main],
        &[&format!("Bring {branch} current with {MAIN_BRANCH}")],
        author,
        Ordered::Clock,
    )?;
    swap_ref_in(&repo, &format!("refs/heads/{branch}"), commit, Some(work)).map_err(|_| {
        GitError::conflict(format!(
            "{branch:?} moved while it was being brought current -- try again"
        ))
    })?;
    Ok(BringCurrent::Done(BroughtCurrent {
        commit: Some(commit.to_string()),
        does_not_load: broken,
    }))
}

/// The files both sides changed that git merged as text without a conflict, each with the blob it made.
fn text_merged(merged: &gix::merge::tree::Outcome<'_>) -> Vec<(String, gix::ObjectId)> {
    let unresolved = gix::merge::tree::TreatAsUnresolved::git();
    merged
        .conflicts
        .iter()
        .filter(|c| !c.is_unresolved(unresolved))
        .filter_map(|c| match &c.resolution {
            Ok(gix::merge::tree::Resolution::OursModifiedTheirsModifiedThenBlobContentMerge { merged_blob }) => {
                Some((c.ours.location().to_string(), merged_blob.merged_blob_id))
            }
            _ => None,
        })
        .collect()
}

enum Content {
    Blob(gix::objs::tree::EntryMode, gix::ObjectId),
    Bytes(Vec<u8>),
}

/// What the merge settled beyond git's own tree, and what is left for the author.
#[derive(Default)]
struct Found {
    /// A path's new content, or `None` to remove it.
    writes: BTreeMap<String, Option<Content>>,
    conflicts: Vec<Conflict>,
    /// Section files merged by object: their conflicts are the objects', never the file's.
    by_object: BTreeSet<String>,
    /// Files a conflicted lookup table's versions name, and the side chosen for the table, if it has one yet.
    follows: BTreeMap<String, Option<config_file::Side>>,
}

impl Found {
    fn write(&mut self, file: &str, bytes: Option<Vec<u8>>) {
        self.writes.insert(file.to_string(), bytes.map(Content::Bytes));
    }

    /// `file` as `side` has it.
    fn take(
        &mut self,
        repo: &gix::Repository,
        trees: &Trees,
        file: &str,
        side: config_file::Side,
    ) -> Result<(), GitError> {
        let tree = match side {
            config_file::Side::Main => trees.main,
            config_file::Side::Work => trees.work,
        };
        let content = entry(repo, tree, file)?.map(|(mode, id)| Content::Blob(mode, id));
        self.writes.insert(file.to_string(), content);
        Ok(())
    }

    /// Whether `file` is answered by something else: its objects, a choice already taken, or its lookup table.
    fn settled(&self, file: &str) -> bool {
        self.by_object.contains(file) || self.writes.contains_key(file) || self.follows.contains_key(file)
    }
}

struct Trees {
    /// The merge base's tree, when there is exactly one.
    base: Option<gix::ObjectId>,
    main: gix::ObjectId,
    work: gix::ObjectId,
}

impl Trees {
    /// The two tips' trees, and their merge base's when there is exactly one.
    fn of(repo: &gix::Repository, work: gix::ObjectId, main: gix::ObjectId) -> Result<Self, GitError> {
        let bases = repo
            .merge_bases_many(work, &[main])
            .map_err(|e| GitError::failed(format!("this branch and {MAIN_BRANCH} share no commit: {e}")))?;
        let base = match bases.as_slice() {
            [one] => Some(tree_at(repo, &one.to_string())?),
            _ => None,
        };
        Ok(Self {
            base,
            main: tree_at(repo, &main.to_string())?,
            work: tree_at(repo, &work.to_string())?,
        })
    }

    /// `file`'s bytes at the base, `main` and the branch. A side without it is `None`.
    fn bytes(&self, repo: &gix::Repository, file: &str) -> Result<[Option<Vec<u8>>; 3], GitError> {
        let read = |tree: Option<gix::ObjectId>| -> Result<Option<Vec<u8>>, GitError> {
            let Some(tree) = tree else { return Ok(None) };
            entry(repo, tree, file)?
                .map(|side| side_bytes(repo, &Some(side)))
                .transpose()
        };
        Ok([read(self.base)?, read(Some(self.main))?, read(Some(self.work))?])
    }
}

/// The file at `path` in `tree`. A directory there is refused: a choice is of one file's content.
fn entry(repo: &gix::Repository, tree: gix::ObjectId, path: &str) -> Result<Side, GitError> {
    let failed = |e: String| GitError::failed(format!("reading {path}: {e}"));
    let tree = repo.find_tree(tree).map_err(|e| failed(e.to_string()))?;
    match tree.lookup_entry_by_path(path).map_err(|e| failed(e.to_string()))? {
        Some(entry) if entry.mode().is_tree() => Err(GitError::invalid(format!(
            "{path} is a file on one side and a directory on the other; a side can be chosen only between files"
        ))),
        Some(entry) => Ok(Some((entry.mode(), entry.object_id()))),
        None => Ok(None),
    }
}
