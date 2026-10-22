//! Following a git remote and pushing branches to it, against a *real* git host with Git LFS: Gitea, as
//! `just live-up` runs it.
//!
//! `leader::remote`'s own tests run against git's http-backend and a fake LFS server. This file is the contract with a
//! host: a token it issued is taken for fetch, push and LFS alike, an empty repository is seeded, a branch and its
//! lookup file arrive, a merge made on the host is followed with the file downloaded back, and a branch someone else
//! pushed to is refused.
//!
//! Point it at `GIT_TEST_HOST` with `GIT_TEST_AUTH`. Under `SMELTR_LIVE_TESTS=required` a missing one fails rather
//! than skips.

use crate::live;

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use smeltr::leader::git;
use smeltr::leader::remote::{self, Remote};
use smeltr::leader::store::Actor;
use smeltr::lookup::store::{self, Pointer};

const USER: &str = "smeltr";

/// A name no earlier run used.
fn unique(prefix: &str) -> String {
    format!(
        "{prefix}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

/// The host's API, as its admin.
struct Api {
    base: String,
    auth: String,
    agent: ureq::Agent,
}

impl Api {
    fn new(base: &str, user_password: &str) -> Api {
        Api {
            base: format!("{base}/api/v1"),
            auth: format!(
                "Basic {}",
                smeltr::format::bytes_text::BytesForm::Base64.text(user_password.as_bytes())
            ),
            agent: ureq::Agent::config_builder().proxy(None).build().into(),
        }
    }

    fn post(&self, path: &str, body: serde_json::Value) -> serde_json::Value {
        let mut response = self
            .agent
            .post(&format!("{}{path}", self.base))
            .header("Authorization", &self.auth)
            .send_json(body)
            .unwrap_or_else(|e| panic!("POST {path}: {e}"));
        response.body_mut().read_json().unwrap_or(serde_json::Value::Null)
    }

    fn get(&self, path: &str) -> ureq::http::Response<ureq::Body> {
        self.agent
            .get(&format!("{}{path}", self.base))
            .header("Authorization", &self.auth)
            .call()
            .unwrap_or_else(|e| panic!("GET {path}: {e}"))
    }

    /// Where `branch` of `repo` is once the host says it is at `want`, or wherever it is after a while. The host
    /// indexes what was pushed on its own time, so its API catches up a moment after git does.
    fn branch(&self, repo: &str, branch: &str, want: Option<&str>) -> Option<String> {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
        loop {
            let found = self.branches(repo).remove(branch);
            if found.as_deref() == want || std::time::Instant::now() > deadline {
                return found;
            }
            std::thread::sleep(std::time::Duration::from_millis(200));
        }
    }

    /// Where each branch of `repo` is.
    fn branches(&self, repo: &str) -> HashMap<String, String> {
        let listed: Option<Vec<serde_json::Value>> = self
            .get(&format!("/repos/{USER}/{repo}/branches"))
            .body_mut()
            .read_json()
            .unwrap();
        let listed = listed.unwrap_or_default();
        listed
            .iter()
            .map(|b| {
                (
                    b["name"].as_str().unwrap().to_string(),
                    b["commit"]["id"].as_str().unwrap().to_string(),
                )
            })
            .collect()
    }
}

/// A bundle that loads, and a CSV lookup table whose file is `pointer`.
fn bundle(pointer: &Pointer) -> Vec<(String, Vec<u8>)> {
    vec![
        ("groups/standalone/system.json".into(), br#"{"version":1}"#.to_vec()),
        (
            "groups/standalone/sources.json".into(),
            br#"[{"id":"m","type":"metrics","bucketSeconds":10}]"#.to_vec(),
        ),
        (
            "groups/standalone/pipelines/orders.json".into(),
            br#"{"id":"orders","name":"orders","steps":[]}"#.to_vec(),
        ),
        (
            "groups/standalone/lookup-tables.json".into(),
            br#"[{"id":"cities","type":"csv","delimiter":",","path":"lookups/cities.csv"}]"#.to_vec(),
        ),
        (
            "groups/standalone/lookups/cities.csv".into(),
            pointer.text().into_bytes(),
        ),
    ]
}

fn commit(repo: &Path, branch: &str, files: Vec<(String, Vec<u8>)>, title: &str) -> String {
    let expected = git::branch_tip(repo, branch);
    let parent = expected.clone().or_else(|| git::main_head(repo)).unwrap();
    let edits: Vec<git::FileEdit> = files
        .into_iter()
        .map(|(path, body)| git::FileEdit {
            path,
            content: Some(body),
        })
        .collect();
    match git::commit_edits(
        repo,
        branch,
        &parent,
        expected.as_deref(),
        &edits,
        title,
        &Actor::system(),
    )
    .unwrap()
    {
        git::Committed::At(commit) => commit,
        _ => panic!("{title} did not land"),
    }
}

/// A leader's repository, holding `bytes` in its lookup store as an upload would, and the roots its fetch trusts.
fn leader(bytes: &[u8]) -> (PathBuf, PathBuf, Pointer) {
    let data = std::env::temp_dir().join(unique("smeltr-git-live"));
    let repo = git::repo_path(&data);
    git::init_bare_repo(&repo).unwrap();
    let roots = remote::roots_path(&data);
    smeltr::io::trust::write_pem_file(None, &roots).unwrap();
    git::ensure_main_commit(&repo).unwrap();
    let digest = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, bytes);
    let pointer = Pointer {
        oid: digest.as_ref().iter().map(|b| format!("{b:02x}")).collect(),
        size: bytes.len() as u64,
    };
    let path = store::path_of(&repo, &pointer.oid);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, bytes).unwrap();
    (repo, roots, pointer)
}

#[test]
fn a_leader_seeds_pushes_to_and_follows_a_real_git_host() {
    let Some((host, auth)) = live::git_host("a_leader_seeds_pushes_to_and_follows_a_real_git_host") else {
        return;
    };
    let api = Api::new(&host, &auth);
    let token = api.post(
        &format!("/users/{USER}/tokens"),
        serde_json::json!({"name": unique("smeltr"), "scopes": ["write:repository", "write:user"]}),
    )["sha1"]
        .as_str()
        .expect("the host issued a token")
        .to_string();
    let name = unique("config");
    api.post("/user/repos", serde_json::json!({"name": name, "private": true}));
    let remote = Remote {
        url: format!("{host}/{USER}/{name}.git"),
        user: USER.to_string(),
        token: Some(token),
        poll: None,
        approve_pushes: false,
    };

    let bytes = b"ip,city\n10.0.0.1,Oslo\n";
    let (repo, roots, pointer) = leader(bytes);

    // An empty repository is given the leader's main.
    let main = git::main_head(&repo).unwrap();
    assert_eq!(remote::sync(&repo, &roots, &remote), Ok(Some(main.clone())));
    assert_eq!(api.branch(&name, "main", Some(&main)), Some(main));

    // A save on a branch arrives with its lookup file in the host's LFS.
    let tip = commit(&repo, "al/cities", bundle(&pointer), "cities");
    assert_eq!(
        remote::push_all(&repo, &remote, &HashMap::new(), 0, 0),
        Ok(HashMap::new())
    );
    assert_eq!(api.branch(&name, "al/cities", Some(&tip)), Some(tip));
    let mut media = api.get(&format!(
        "/repos/{USER}/{name}/media/groups/standalone/lookups/cities.csv?ref=al/cities"
    ));
    assert_eq!(
        media.body_mut().read_to_vec().unwrap(),
        bytes,
        "the host serves the file, not its pointer"
    );

    // Merged on the host, and followed: the file comes back from the host's LFS into a store that lost it.
    let pull = api.post(
        &format!("/repos/{USER}/{name}/pulls"),
        serde_json::json!({"head": "al/cities", "base": "main", "title": "cities"}),
    )["number"]
        .as_u64()
        .unwrap();
    api.post(
        &format!("/repos/{USER}/{name}/pulls/{pull}/merge"),
        serde_json::json!({"Do": "merge"}),
    );
    let merged = api.branches(&name)["main"].clone();
    assert_ne!(merged, git::main_head(&repo).unwrap(), "the merge made a commit");
    std::fs::remove_file(store::path_of(&repo, &pointer.oid)).unwrap();
    assert_eq!(remote::sync(&repo, &roots, &remote), Ok(Some(merged.clone())));
    assert_eq!(git::main_head(&repo), Some(merged.clone()));
    assert_eq!(git::deployed(&repo), Some(merged));
    assert_eq!(std::fs::read(store::path_of(&repo, &pointer.oid)).unwrap(), bytes);

    // Someone edits the branch on the host; the leader's next save there is refused, naming the host's tip.
    api.post(
        &format!("/repos/{USER}/{name}/contents/notes.txt"),
        serde_json::json!({"content": smeltr::format::bytes_text::BytesForm::Base64.text(b"theirs"), "message": "theirs", "branch": "al/cities"}),
    );
    let theirs = api.branches(&name)["al/cities"].clone();
    let ours = commit(
        &repo,
        "al/cities",
        vec![(
            "groups/standalone/system.json".into(),
            br#"{"version":1,"x":1}"#.to_vec(),
        )],
        "ours",
    );
    let refused = remote::push_all(&repo, &remote, &HashMap::new(), 0, 0).unwrap();
    let refused = &refused["al/cities"];
    assert!(refused.refused, "{refused:?}");
    assert_eq!(refused.tip.as_deref(), Some(ours.as_str()));
    assert_eq!(refused.host_tip.as_deref(), Some(theirs.as_str()));
    assert_eq!(api.branches(&name)["al/cities"], theirs, "their commit is still there");

    // With Approve pushing to main, an approval lands on the host's main before the fleet runs it.
    let data_dir = repo.parent().unwrap().parent().unwrap().to_path_buf();
    let remote = Remote {
        approve_pushes: true,
        ..remote
    };
    let more = |branch: &str| {
        let file = format!("groups/standalone/pipelines/{}.json", branch.replace('/', "-"));
        let body = format!(r#"{{"id":"{0}","name":"{0}","steps":[]}}"#, branch.replace('/', "-"));
        commit(&repo, branch, vec![(file, body.into_bytes())], branch);
        git::open_change(&repo, branch, branch, "", &Actor::system()).unwrap();
    };
    more("al/approved");
    let approved = remote::approve(&data_dir, &remote, "al/approved", &Actor::system()).unwrap();
    assert_eq!(api.branches(&name)["main"], approved);
    assert_eq!(git::deployed(&repo), Some(approved.clone()));

    // A host that protects main refuses the next one, and says so; nothing moves.
    api.post(
        &format!("/repos/{USER}/{name}/branch_protections"),
        serde_json::json!({"rule_name": "main", "enable_push": false}),
    );
    more("al/protected");
    let refused = remote::approve(&data_dir, &remote, "al/protected", &Actor::system()).unwrap_err();
    assert!(refused.contains("protected"), "{refused}");
    assert_eq!(api.branches(&name)["main"], approved);
    assert_eq!(git::main_head(&repo), Some(approved));
}
