//! What is wrong with the commit the fleet is deployed to: groups that do not load there, and tests that fail
//! there. Config that fails either way can reach the deployed pointer -- a rollback, a leader bug, a commit written
//! straight into the repository, a host merge -- and approving a change with a failing test is allowed. The fleet
//! keeps running what it had where it can, so the leader is the one that has to say so.
//!
//! Both answers are a function of the commit and of this build, so they are worked out once per deployed commit and
//! kept for as long as the process lives. One reader loads a new commit while the others answer with what was known
//! before it. Tests take longer, so one thread runs them, always for the newest deployed commit; until it has, the
//! last failures known stand, so a test still failing after a deploy is not reported as having started again.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

use super::git;
use crate::config_file::SchemaFinding;

/// A test that ran and did not produce what it locked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailingTest {
    /// `shared` or the group the test sits in.
    pub scope: String,
    pub id: String,
    pub name: String,
    /// The pipeline it runs. `None` for an end-to-end test, which routes its sample by its source.
    pub pipeline: Option<String>,
    /// What differs, worded to follow the test's name.
    pub why: String,
}

/// A group whose bundle does not load at the deployed commit, and the load error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotLoading {
    pub group: String,
    pub why: String,
}

/// What the checks found: groups that do not load at `commit`, what breaks a schema there, and tests that fail at
/// `tests_at`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Findings {
    pub commit: String,
    pub not_loading: Vec<NotLoading>,
    /// Each group's schema findings (`LoadedSystem::schema_findings`), with the group.
    pub schemas: Vec<(String, SchemaFinding)>,
    /// The commit `failing` was found at. Behind `commit` while that commit's tests are still running.
    pub tests_at: String,
    pub failing: Vec<FailingTest>,
}

impl Findings {
    /// Whether `failing` is not yet `commit`'s own answer.
    pub fn tests_pending(&self) -> bool {
        self.tests_at != self.commit
    }
}

/// Runs every test at a commit and answers the ones that fail. The test runner is the binary's, which owns the
/// preview a test runs.
pub type TestRunner = Arc<dyn Fn(&Path, &str) -> Vec<FailingTest> + Send + Sync>;

#[derive(Default)]
struct State {
    known: Findings,
    /// The commit one reader is loading, so the others do not load it too.
    loading: Option<String>,
    /// The commit whose tests are to run next.
    wanted: Option<String>,
    /// Whether the test thread is running.
    running: bool,
}

/// The checks of the deployed commit.
#[derive(Default)]
pub struct DeployChecks {
    runner: OnceLock<TestRunner>,
    state: Arc<Mutex<State>>,
}

impl DeployChecks {
    /// Hand over the test runner. Without one, no test is run and none is reported failing.
    pub fn set_runner(&self, runner: TestRunner) {
        let _ = self.runner.set(runner);
    }

    /// The findings about what is deployed now. The first reader to see a new commit loads it and asks for its
    /// tests; every other answers with what was known before, until that load lands.
    ///
    /// Blocking; the caller is inside `spawn_blocking`.
    pub fn findings(&self, data_dir: &Path) -> Findings {
        let repo = git::repo_path(data_dir);
        if !git::repo_ready(&repo) {
            return Findings::default();
        }
        let Some(commit) = git::deployed(&repo) else {
            return Findings::default();
        };
        {
            let mut state = self.state.lock().unwrap();
            if state.known.commit == commit || state.loading.as_deref() == Some(commit.as_str()) {
                return state.known.clone();
            }
            state.loading = Some(commit.clone());
        }

        let listed = checked(&repo, &commit);
        let mut state = self.state.lock().unwrap();
        // A reader that started on another commit since owns the next answer; this one's is already old.
        if state.loading.as_deref() != Some(commit.as_str()) {
            return state.known.clone();
        }
        state.loading = None;
        // A commit whose groups could not be listed is not known to be clean, so the next reader tries again.
        let Ok((not_loading, schemas)) = listed else {
            return state.known.clone();
        };
        state.known.commit = commit.clone();
        state.known.not_loading = not_loading;
        state.known.schemas = schemas;
        if let Some(runner) = self.runner.get() {
            state.wanted = Some(commit);
            if !state.running {
                state.running = true;
                run_tests(runner.clone(), self.state.clone(), data_dir.to_path_buf());
            }
        }
        state.known.clone()
    }
}

/// Each schema finding, with the group it is in.
type GroupFindings = Vec<(String, SchemaFinding)>;

/// The groups that do not load at `commit`, and the schema findings of each that does.
fn checked(repo: &Path, commit: &str) -> Result<(Vec<NotLoading>, GroupFindings), git::GitError> {
    let mut not_loading = Vec::new();
    let mut schemas = Vec::new();
    for group in super::tree::groups_at(repo, commit)? {
        match super::tree::load_group_at(repo, commit, &group) {
            Err(e) => not_loading.push(NotLoading {
                group,
                why: e.to_string(),
            }),
            // Loading compiled it, so this compile does not fail.
            Ok(system) => {
                if let Ok(compiled) = system.compile_topology() {
                    schemas.extend(
                        system
                            .schema_findings(&compiled)
                            .into_iter()
                            .map(|f| (group.clone(), f)),
                    );
                }
            }
        }
    }
    Ok((not_loading, schemas))
}

/// Run the wanted commit's tests until no commit is wanted, keeping each answer only while it is still the deployed
/// commit's.
fn run_tests(runner: TestRunner, state: Arc<Mutex<State>>, data_dir: PathBuf) {
    let worker = state.clone();
    let spawned = std::thread::Builder::new()
        .name("smeltr-deploy-tests".to_string())
        .spawn(move || {
            loop {
                let Some(commit) = ({
                    let mut state = worker.lock().unwrap();
                    let next = state.wanted.take();
                    state.running = next.is_some();
                    next
                }) else {
                    return;
                };
                // A panic in one run must not end the thread every later deploy relies on. The hook has logged it,
                // and the run answers nothing, so the failures last found stand.
                let Ok(failing) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| runner(&data_dir, &commit)))
                else {
                    continue;
                };
                let mut state = worker.lock().unwrap();
                if state.known.commit == commit {
                    state.known.tests_at = commit;
                    state.known.failing = failing;
                }
            }
        });
    if let Err(e) = spawned {
        state.lock().unwrap().running = false;
        tracing::warn!(
            target: "leader.deploy_tests_not_run",
            error = %e,
            "the deployed commit's tests could not be started, so the failures last found stand"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    fn seeded(name: &str, groups: &[(&str, &[u8])]) -> PathBuf {
        let data = std::env::temp_dir().join(format!("smeltr-deploy-checks-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&data);
        let edits: Vec<git::FileEdit> = groups
            .iter()
            .map(|(group, manifest)| git::FileEdit {
                path: format!("groups/{group}/system.json"),
                content: Some(manifest.to_vec()),
            })
            .collect();
        assert!(git::seed_first_commit(&git::repo_path(&data), &edits, "seed").unwrap());
        data
    }

    /// The findings once `commit`'s tests have landed.
    fn settled(checks: &DeployChecks, data: &Path, commit: &str) -> Findings {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let found = checks.findings(data);
            if !found.tests_pending() || Instant::now() > deadline {
                assert_eq!(found.commit, commit);
                return found;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    #[test]
    fn a_group_that_does_not_load_is_found_and_the_rest_are_not() {
        let data = seeded("not-loading", &[("east", br#"{"version":1}"#), ("west", b"not json")]);
        let found = DeployChecks::default().findings(&data);
        assert_eq!(found.commit, git::deployed(&git::repo_path(&data)).unwrap());
        assert_eq!(found.not_loading.len(), 1, "{found:?}");
        assert_eq!(found.not_loading[0].group, "west");
        assert!(found.failing.is_empty(), "no runner, no tests");
    }

    #[test]
    fn what_breaks_a_schema_at_the_deployed_commit_is_found_with_its_group() {
        let data = std::env::temp_dir().join(format!("smeltr-deploy-checks-schemas-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&data);
        let files: [(&str, &str); 5] = [
            ("system.json", r#"{"version":1}"#),
            (
                "schemas.json",
                r#"[{"id":"login","title":"Login","fields":[{"field":"user","type":"str","required":true}]}]"#,
            ),
            (
                "sources.json",
                r#"[{"id":"k","type":"kafka","brokers":"b:9092","topics":["t"],"groupId":"g"}]"#,
            ),
            (
                "routes.json",
                r#"[{"id":"r","source":"k","pipeline":"p","deliver":[]}]"#,
            ),
            (
                "pipelines/p.json",
                r#"{"id":"p","name":"p","produces":["login"],"steps":[]}"#,
            ),
        ];
        let edits: Vec<git::FileEdit> = (files.iter())
            .map(|(path, content)| git::FileEdit {
                path: format!("groups/east/{path}"),
                content: Some(content.as_bytes().to_vec()),
            })
            .collect();
        assert!(git::seed_first_commit(&git::repo_path(&data), &edits, "seed").unwrap());
        let found = DeployChecks::default().findings(&data);
        assert!(found.not_loading.is_empty(), "{found:?}");
        assert!(
            found.schemas.iter().any(|(group, f)| group == "east"
                && f.pipeline == "p"
                && f.message == "Login requires `user`; the rows lack it."),
            "{found:?}"
        );
    }

    #[test]
    fn tests_run_once_per_deployed_commit_and_their_failures_are_kept() {
        let data = seeded("runs-once", &[("east", br#"{"version":1}"#)]);
        let commit = git::deployed(&git::repo_path(&data)).unwrap();
        let runs = Arc::new(AtomicUsize::new(0));
        let checks = DeployChecks::default();
        let counted = runs.clone();
        checks.set_runner(Arc::new(move |_: &Path, at: &str| {
            counted.fetch_add(1, Ordering::SeqCst);
            vec![FailingTest {
                scope: "east".to_string(),
                id: "t".to_string(),
                name: "t".to_string(),
                pipeline: Some("p".to_string()),
                why: format!("ran at {at}"),
            }]
        }));

        let found = settled(&checks, &data, &commit);
        assert_eq!(found.failing.len(), 1);
        assert_eq!(found.failing[0].why, format!("ran at {commit}"));
        checks.findings(&data);
        assert_eq!(
            runs.load(Ordering::SeqCst),
            1,
            "a commit already checked is not run again"
        );
    }

    /// A deploy keeps the failures last found until its own tests land, so a test still failing is not reported as
    /// having started again.
    #[test]
    fn a_deploy_keeps_the_last_failures_until_its_own_tests_land() {
        let data = seeded("carried", &[("east", br#"{"version":1}"#)]);
        let repo = git::repo_path(&data);
        let first = git::deployed(&repo).unwrap();
        let checks = DeployChecks::default();
        checks.set_runner(Arc::new(|_: &Path, at: &str| {
            vec![FailingTest {
                scope: "east".to_string(),
                id: "t".to_string(),
                name: "t".to_string(),
                pipeline: None,
                why: format!("ran at {at}"),
            }]
        }));
        settled(&checks, &data, &first);

        let actor = crate::leader::store::Actor::system();
        let edit = git::FileEdit {
            path: "groups/east/system.json".to_string(),
            content: Some(br#"{ "version": 1 }"#.to_vec()),
        };
        git::commit_edits(&repo, "next", &first, None, &[edit], "next", &actor).unwrap();
        git::open_change(&repo, "next", "next", "", &actor).unwrap();
        let second = git::deploy_change(&data, "next", &actor).unwrap();

        let moved = checks.findings(&data);
        assert_eq!(moved.commit, second);
        if moved.tests_pending() {
            assert_eq!(moved.tests_at, first);
            assert_eq!(moved.failing[0].why, format!("ran at {first}"), "carried, not dropped");
        }
        let landed = settled(&checks, &data, &second);
        assert_eq!(landed.tests_at, second);
        assert_eq!(landed.failing[0].why, format!("ran at {second}"));
    }
}
