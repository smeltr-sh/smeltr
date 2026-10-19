//! The leader's lookup store, kept to what retention allows.
//!
//! Every upload is a version of the file name it was uploaded under. The newest `leader.lookups.versions` of each
//! name are kept, and so is any version a branch or a deployment names whatever the count says -- the deployed commit
//! and every one a worker runs -- the same two clauses secrets are kept by (`versions_to_forget`). A rollback reaches
//! as far back as that.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use crate::lookup::store;

use super::{git, tree};

/// Record `oid` as the newest version of `name`, the bundle and file name it was uploaded as (`groups/west/geo.mmdb`).
pub fn record_upload(repo: &Path, oid: &str, name: &str) -> std::io::Result<()> {
    std::fs::create_dir_all(repo.join(store::STORE_DIR))?;
    std::fs::write(name_path(repo, oid), name)
}

/// Where the name `oid` was last uploaded under is kept. Its modification time is when.
fn name_path(repo: &Path, oid: &str) -> PathBuf {
    store::path_of(repo, &format!("{oid}.name"))
}

/// Forget every stored version beyond the newest `keep` of its name that neither a branch nor one of `running`'s
/// `(group, commit)` deployments names. Nothing is forgotten when what they name cannot be read in full: a file one of
/// them needs would be gone for good.
pub fn prune(repo: &Path, keep: usize, running: &HashSet<(String, String)>) {
    let Ok(pinned) = pinned(repo, running) else {
        return;
    };
    let Ok(entries) = std::fs::read_dir(repo.join(store::STORE_DIR)) else {
        return;
    };
    let mut by_name: HashMap<String, Vec<(SystemTime, String)>> = HashMap::new();
    let ours = store::incoming_prefix(std::process::id());
    for entry in entries.flatten() {
        let file_name = entry.file_name();
        // An upload an earlier process was writing when it ended, which nothing will finish, or a file left behind
        // by one a delete was refused for.
        if file_name
            .to_str()
            .is_some_and(|n| n.starts_with(".incoming-") && !n.starts_with(&ours) || store::is_orphan(repo, n))
        {
            let _ = std::fs::remove_file(entry.path());
            continue;
        }
        let Some(oid) = file_name
            .to_str()
            .and_then(|n| n.strip_suffix(".name"))
            .filter(|o| store::is_oid(o))
        else {
            continue;
        };
        let (Ok(name), Ok(uploaded)) = (
            std::fs::read_to_string(entry.path()),
            entry.metadata().and_then(|m| m.modified()),
        ) else {
            continue;
        };
        by_name.entry(name).or_default().push((uploaded, oid.to_string()));
    }
    for held in by_name.into_values() {
        let pins: Vec<_> = held.iter().filter(|(_, oid)| pinned.contains(oid)).cloned().collect();
        for (_, oid) in super::versions_to_forget(&held, keep, &pins) {
            let _ = store::remove(repo, &oid);
        }
    }
}

/// The stored files every branch names, `main` included, in `shared/` and in every group, and those each running
/// deployment names.
fn pinned(repo: &Path, running: &HashSet<(String, String)>) -> Result<HashSet<String>, String> {
    let shared = || crate::config_file::SHARED_DIR.to_string();
    let mut named = HashSet::new();
    // What the fleet runs, which a rollback can leave behind every branch.
    let heads = git::list_branches(repo)
        .map_err(|e| e.0)?
        .into_iter()
        .map(|branch| branch.head);
    for head in heads.chain(git::deployed(repo)) {
        let mut prefixes = vec![shared()];
        for group in tree::groups_at(repo, &head).map_err(|e| e.0)? {
            prefixes.extend(crate::config_file::group_subtree(&group));
        }
        named.extend(tree::lookup_pointers(repo, &head, &prefixes)?.into_keys());
    }
    for (group, commit) in running {
        let mut prefixes = vec![shared()];
        prefixes.extend(crate::config_file::group_subtree(group));
        named.extend(tree::lookup_pointers(repo, commit, &prefixes)?.into_keys());
    }
    Ok(named.into_iter().map(|p| p.oid).collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::time::{Duration, UNIX_EPOCH};

    /// `bytes` stored as the version of `name` uploaded at `second`.
    fn upload(repo: &Path, name: &str, bytes: &[u8], second: u64) -> String {
        let mut incoming = store::Incoming::new(repo).unwrap();
        incoming.write_all(bytes).unwrap();
        let oid = incoming.finish(None).unwrap().oid;
        record_upload(repo, &oid, name).unwrap();
        let sidecar = std::fs::File::options()
            .write(true)
            .open(name_path(repo, &oid))
            .unwrap();
        sidecar.set_modified(UNIX_EPOCH + Duration::from_secs(second)).unwrap();
        oid
    }

    #[test]
    fn the_newest_versions_of_each_name_are_kept_and_so_is_what_a_branch_or_a_deployment_names() {
        let repo = std::env::temp_dir().join(format!("smeltr-lookup-retention-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&repo);
        git::init_bare_repo(&repo).unwrap();
        git::ensure_main_commit(&repo).unwrap();

        let geo: Vec<String> = (0..4).map(|n| upload(&repo, "geo.mmdb", &[n], u64::from(n))).collect();
        let asn = upload(&repo, "asn.mmdb", b"asn", 0);

        // A branch still names the oldest `geo.mmdb`.
        let pointer = store::Pointer {
            oid: geo[0].clone(),
            size: 1,
        };
        let edits = [
            git::FileEdit {
                path: "shared/lookup-tables.json".to_string(),
                content: Some(br#"[{"id":"geo","type":"mmdb","path":"lookups/geo.mmdb"}]"#.to_vec()),
            },
            git::FileEdit {
                path: "shared/lookups/geo.mmdb".to_string(),
                content: Some(pointer.text().into_bytes()),
            },
        ];
        let main = git::main_head(&repo).unwrap();
        let actor = crate::leader::store::Actor::system();
        git::commit_edits(&repo, "work", &main, None, &edits, "name the oldest", &actor).unwrap();

        // A worker still runs a commit naming the second oldest, which its branch has since moved past.
        let named = |oid: &str| {
            let pointer = store::Pointer {
                oid: oid.to_string(),
                size: 1,
            };
            [
                git::FileEdit {
                    path: "groups/west/lookup-tables.json".to_string(),
                    content: Some(br#"[{"id":"geo","type":"mmdb","path":"lookups/geo.mmdb"}]"#.to_vec()),
                },
                git::FileEdit {
                    path: "groups/west/lookups/geo.mmdb".to_string(),
                    content: Some(pointer.text().into_bytes()),
                },
            ]
        };
        let git::Committed::At(running) =
            git::commit_edits(&repo, "west", &main, None, &named(&geo[1]), "run it", &actor).unwrap()
        else {
            panic!("the commit landed")
        };
        git::commit_edits(
            &repo,
            "west",
            &running,
            Some(&running),
            &named(&geo[3]),
            "move on",
            &actor,
        )
        .unwrap();

        let abandoned = repo
            .join(store::STORE_DIR)
            .join(format!("{}0", store::incoming_prefix(0)));
        std::fs::write(&abandoned, b"half an upload").unwrap();
        let derived = |oid: &str| store::path_of(&repo, &format!("{oid}.index-0"));
        std::fs::write(derived(&geo[2]), b"an index").unwrap();
        std::fs::write(derived(&geo[3]), b"an index").unwrap();
        let orphan = derived(&"0".repeat(64));
        std::fs::write(&orphan, b"an index of nothing").unwrap();

        prune(&repo, 1, &[("west".to_string(), running)].into());
        assert!(!abandoned.exists(), "an earlier process's upload will never finish");
        assert!(!derived(&geo[2]).exists(), "a derived file goes with its file");
        assert!(derived(&geo[3]).exists(), "and stays with it");
        assert!(!orphan.exists(), "one whose file went without it goes too");
        let kept = |oid: &str| store::path_of(&repo, oid).exists();
        assert!(kept(&geo[0]), "a branch names it");
        assert!(kept(&geo[1]), "a running deployment names it");
        assert!(!kept(&geo[2]), "older than the newest, and nothing names it");
        assert!(kept(&geo[3]), "the newest");
        assert!(kept(&asn), "another name counts its own versions");
        std::fs::remove_dir_all(&repo).ok();
    }
}
