//! Reading a bundle out of a commit.
//!
//! `config_file` defines the seam a bundle is read through ([`FileSet`]); this supplies the one backed by git, so the
//! leader can answer from a commit rather than from a checkout. A commit is immutable, so what it holds cannot change
//! under a reader and the id it was asked for describes what came back.

use std::path::Path;
#[cfg(test)]
use std::path::PathBuf;

use crate::config_file::FileSet;

use super::git;

/// One bundle inside one commit: the repository to ask, the commit to ask about, and the directory within it.
pub struct TreeFiles<'a> {
    repo: &'a Path,
    commit: &'a str,
    /// Where the bundle sits in the commit, without a trailing slash. Empty is the repository root.
    prefix: String,
}

impl<'a> TreeFiles<'a> {
    pub fn new(repo: &'a Path, commit: &'a str, prefix: impl Into<String>) -> TreeFiles<'a> {
        TreeFiles {
            repo,
            commit,
            prefix: prefix.into().trim_end_matches('/').to_string(),
        }
    }

    fn at(&self, path: &str) -> String {
        if self.prefix.is_empty() {
            path.to_string()
        } else {
            format!("{}/{path}", self.prefix)
        }
    }

    fn as_io(e: git::GitError) -> std::io::Error {
        std::io::Error::other(e.0)
    }
}

impl FileSet for TreeFiles<'_> {
    fn read(&mut self, path: &str) -> std::io::Result<Option<Vec<u8>>> {
        let asked = self.at(path);
        let mut read = git::read_blobs(self.repo, self.commit, &[asked]).map_err(TreeFiles::as_io)?;
        Ok(read.pop().flatten())
    }

    fn json_paths_under(&mut self, dir: &str) -> std::io::Result<Vec<String>> {
        let entries = git::list_tree(self.repo, self.commit, &self.at(dir)).map_err(TreeFiles::as_io)?;
        Ok(entries
            .into_iter()
            // A config file is a file. A directory named `x.json` is not one, and a symlink reads as its target
            // text rather than as what it names, which is never config either.
            .filter(|e| !e.is_dir() && !e.is_symlink() && e.name.ends_with(".json"))
            .map(|e| format!("{dir}/{}", e.name))
            .collect())
    }
}

/// What `group_id` would run at `rev`: `shared/` merged with its own subtree, loaded and validated.
///
/// `rev` is anything git resolves to a tree -- a commit, or a tree the leader has merged but not yet committed, so
/// a merge can be checked before anything points at it.
pub fn load_group_at(
    repo: &Path,
    rev: &str,
    group_id: &str,
) -> Result<crate::config_file::LoadedSystem, crate::config_file::BundleError> {
    let subtree =
        crate::config_file::group_subtree(group_id).map_err(|message| crate::config_file::BundleError::Io {
            path: format!("{}/", crate::config_file::GROUPS_DIR),
            message,
        })?;
    let at = |prefix: &str| crate::config_file::BundleAt::Commit {
        repo: repo.to_path_buf(),
        commit: rev.to_string(),
        prefix: prefix.to_string(),
    };
    crate::config_file::load_group_from(
        &mut TreeFiles::new(repo, rev, crate::config_file::SHARED_DIR),
        &at(crate::config_file::SHARED_DIR),
        &mut TreeFiles::new(repo, rev, &subtree),
        &at(&subtree),
        group_id,
    )
}

/// Every group `rev` holds a subtree for. A name `group_subtree` would refuse is not a group any worker can be in,
/// so it is passed over rather than reported.
pub fn groups_at(repo: &Path, rev: &str) -> Result<Vec<String>, git::GitError> {
    Ok(git::list_tree(repo, rev, crate::config_file::GROUPS_DIR)?
        .into_iter()
        .filter(|e| e.is_dir() && crate::config_file::group_subtree(&e.name).is_ok())
        .map(|e| e.name)
        .collect())
}

/// The pointers of the lookup files that the bundles under `prefixes` name at `rev`, each with the path it is committed
/// at -- the last one read, where two paths hold the same file. A table whose file is not there is
/// left out, and loading the bundle is what reports it; a bundle whose tables or files cannot be read, or a file
/// committed as its bytes (`store::pointer_in`), is an error, since what it names is then unknown.
pub fn lookup_pointers(
    repo: &Path,
    rev: &str,
    prefixes: &[String],
) -> Result<std::collections::HashMap<crate::lookup::store::Pointer, String>, String> {
    use crate::config_file::LookupSourceDto;
    let mut pointers = std::collections::HashMap::new();
    for prefix in prefixes {
        let at = crate::config_file::BundleAt::Commit {
            repo: repo.to_path_buf(),
            commit: rev.to_string(),
            prefix: prefix.clone(),
        };
        let raw = crate::config_file::read_raw_from(&mut TreeFiles::new(repo, rev, prefix.as_str()));
        if let Some((file, e)) = raw.unreadable.iter().find(|(file, _)| file == "lookup-tables.json") {
            return Err(format!("{prefix}/{file} at {rev}: {e}"));
        }
        for table in raw.file.lookup_tables {
            let (LookupSourceDto::Csv { path, .. } | LookupSourceDto::Mmdb { path }) = table.source else {
                continue;
            };
            let Ok(crate::plan::config::LookupAt::InCommit { path, .. }) = at.lookup(&path) else {
                continue;
            };
            let blob = git::read_blobs(repo, rev, std::slice::from_ref(&path)).map_err(|e| e.0)?;
            for blob in blob.into_iter().flatten() {
                pointers.insert(
                    crate::lookup::store::pointer_in(&blob, &path).map_err(|e| e.0)?,
                    path.clone(),
                );
            }
        }
    }
    Ok(pointers)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config_file::read_raw_from;

    fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("smeltr-tree-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    /// A bundle committed on `main`, plus the same bundle in a directory to read it against.
    fn a_committed_bundle(name: &str) -> (PathBuf, PathBuf, String) {
        let files: [(&str, &[u8]); 3] = [
            ("system.json", br#"{"version":1}"#),
            ("sources.json", br#"[{"id":"m","type":"metrics","bucketSeconds":10}]"#),
            (
                "pipelines/orders.json",
                br#"{"id":"orders","name":"orders","steps":[]}"#,
            ),
        ];
        let data = temp_dir(name);
        let bundle = data.join("bundle");
        std::fs::create_dir_all(bundle.join("pipelines")).unwrap();
        for (path, body) in files {
            std::fs::write(bundle.join(path), body).unwrap();
        }

        let repo = git::repo_path(&data);
        git::init_bare_repo(&repo).unwrap();
        git::ensure_main_commit(&repo).unwrap();
        let parent = git::main_head(&repo).unwrap();
        let edits: Vec<git::FileEdit> = files
            .iter()
            .map(|(path, body)| git::FileEdit {
                path: format!("groups/standalone/{path}"),
                content: Some(body.to_vec()),
            })
            .collect();
        let commit = match git::commit_edits(
            &repo,
            git::MAIN_BRANCH,
            &parent,
            Some(&parent),
            &edits,
            "a bundle",
            &crate::leader::store::Actor::system(),
        )
        .unwrap()
        {
            git::Committed::At(commit) => commit,
            _ => panic!("the bundle did not land"),
        };
        (data, bundle, commit)
    }

    /// The seam is the whole difference: the same bundle read from a commit and from the checkout of that commit
    /// answers the same thing.
    #[test]
    fn a_bundle_reads_the_same_from_a_commit_as_from_the_directory() {
        let (data, bundle, commit) = a_committed_bundle("same");
        let repo = git::repo_path(&data);

        let from_dir = read_raw_from(&mut crate::config_file::DirFileSet { root: &bundle });
        let from_tree = read_raw_from(&mut TreeFiles::new(&repo, &commit, "groups/standalone"));

        assert!(from_dir.unreadable.is_empty() && from_tree.unreadable.is_empty());
        assert_eq!(from_tree.file.sources.len(), 1);
        assert_eq!(from_tree.file.pipelines.len(), 1);
        assert_eq!(
            serde_json::to_value(&from_tree.file).unwrap(),
            serde_json::to_value(&from_dir.file).unwrap()
        );
    }

    #[test]
    fn a_bundle_the_commit_does_not_hold_reads_as_empty() {
        let (data, _, commit) = a_committed_bundle("absent");
        let repo = git::repo_path(&data);
        let raw = read_raw_from(&mut TreeFiles::new(&repo, &commit, "groups/never-authored"));

        assert!(raw.file.sources.is_empty() && raw.file.pipelines.is_empty());
        assert!(raw.unreadable.is_empty(), "nothing to run is a state, not a failure");
    }

    /// Git hands back a symlink's target text rather than the file it names, so one committed where a pipeline
    /// belongs is not read as a pipeline.
    #[test]
    fn a_symlink_is_not_read_as_a_pipeline() {
        let (data, _, _) = a_committed_bundle("symlink");
        let repo = git::repo_path(&data);
        let commit = git::commit_a_symlink(
            &repo,
            git::MAIN_BRANCH,
            "groups/standalone/pipelines/sneaky.json",
            "/etc/passwd",
        );

        let paths = TreeFiles::new(&repo, &commit, "groups/standalone")
            .pipeline_paths()
            .unwrap();
        assert_eq!(paths, vec!["pipelines/orders.json".to_string()]);
    }
}
