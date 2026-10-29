//! Locking a test, and running one.
//!
//! A test is a bundle file like a sample (`config_file::test`), saved by one commit on a branch, and no worker
//! reads either. What a test asserts is committed; whether it passes is not. A result is computed on every ask and
//! stored nowhere: `now()` puts the clock into a row, so one commit does not have one answer, and a cache keyed by
//! a commit would be a cache of something that is not a function of it.
//!
//! Running a test is a preview, given the sample the test names and the instant it was locked at: the pipeline
//! preview for a pipeline test, the route preview for an end-to-end test.

use std::collections::HashSet;
use std::path::Path;

use axum::Json;
use axum::extract::{Extension, Path as AxPath, Query};

use smeltr::config_file::{self, Impact, TestDto, TestSummary, Verdict};
use smeltr::leader::access::{Access, Level};
use smeltr::leader::deploy_checks::FailingTest;
use smeltr::leader::envelope::{ApiError, Envelope, Refusal};
use smeltr::leader::preview::{TestResult, run_test};
use smeltr::leader::store::Actor;
use smeltr::leader::{blocking, blocking_answer};
use smeltr::startup_config::leader_data_dir;

use crate::config_api::{self, Addressed, BranchQuery, ConfigQuery, Save, blocking_save};

/// Which tests a list or a run covers: one area's, the same as every other config call, or -- what a review asks
/// for -- every area the branch's own change reaches (`branch_api::scopes_touched`), since the branch under review
/// is not the reviewer's own editing area (the chip).
#[derive(Debug)]
enum Covers {
    Area(String),
    Change,
}

/// The scopes `covers` names: the branch's own (`branch_api::scopes_touched`) for a change, otherwise the one area.
///
/// The gate judged an area already. The branch's own it could not, so the caller is judged here, at `level`
/// over the change (`branch_api::lacking_in_change`): each changed path in its own area, a change to `shared/` as
/// shared. The scopes answered are wider than that -- a change to `shared/` reaches every group's tests -- since
/// what is run is what the change reaches, and what is judged is what the person changed.
fn resolve_scopes(
    data_dir: &Path,
    branch: &str,
    covers: &Covers,
    access: &Access,
    level: Level,
) -> Result<Vec<String>, Refusal> {
    match covers {
        Covers::Change => {
            if let Some(why) = crate::branch_api::lacking_in_change(access, data_dir, branch, level)? {
                return Err(Refusal::forbidden(why));
            }
            crate::branch_api::scopes_touched(data_dir, branch)
        }
        Covers::Area(area) => Ok(vec![area.clone()]),
    }
}

/// A test's summary, tagged with the area it is in: an id is unique only within an area, the same
/// collision a group-scoped `Warning::subject_id` or `MapNode::author_id` already carries a group to
/// resolve (`src/leader/warnings.rs`, `src/leader/map.rs`) -- necessary here once a review can run more
/// than one area's tests in a single answer.
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AreaTestSummary {
    pub area: String,
    #[serde(flatten)]
    pub summary: TestSummary,
}

#[derive(Default, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Tests {
    tests: Vec<AreaTestSummary>,
}

/// One area's tests.
pub async fn list_tests(
    Extension(access): Extension<Access>,
    AxPath(area): AxPath<String>,
    Query(q): Query<BranchQuery>,
) -> Envelope<Tests> {
    blocking_answer(move || list_in(&leader_data_dir(), q.name(), &Covers::Area(area), &access)).await
}

/// The tests of every area the change on branch `:name` reaches.
pub async fn list_change_tests(
    Extension(access): Extension<Access>,
    AxPath(branch): AxPath<String>,
) -> Envelope<Tests> {
    blocking_answer(move || list_in(&leader_data_dir(), &branch, &Covers::Change, &access)).await
}

/// [`list_tests`] over the leader at `data_dir`.
fn list_in(data_dir: &Path, branch: &str, covers: &Covers, access: &Access) -> Envelope<Tests> {
    let read = || {
        let scopes = resolve_scopes(data_dir, branch, covers, access, Level::Viewer)?;
        let mut tests = Vec::new();
        let mut errors = Vec::new();
        for scope in scopes {
            let reading = config_api::Reading::open_in(data_dir, branch, &scope)?;
            let (listed, unreadable) = config_file::list_tests(&mut reading.files());
            tests.extend(listed.into_iter().map(|summary| AreaTestSummary {
                area: scope.clone(),
                summary,
            }));
            errors.extend(
                unreadable
                    .into_iter()
                    .map(|(path, message)| format!("{scope}/{path}: {message}")),
            );
        }
        Ok::<_, Refusal>((tests, errors))
    };
    match read() {
        // The tests that could be read are listed, and each file that could not says why.
        Ok((tests, errors)) => Envelope::with(
            Tests { tests },
            config_api::read_status(&errors),
            errors.into_iter().map(ApiError::new).collect(),
        ),
        Err(e) => Envelope::refused(e),
    }
}

#[derive(Default, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Test {
    test: Option<TestDto>,
}

/// A test at the branch's tip, or at the commit `at` names.
pub async fn get_test(AxPath((area, id)): AxPath<(String, String)>, Query(q): Query<ConfigQuery>) -> Envelope<Test> {
    let read = blocking(move || {
        let reading = q.open_in(&leader_data_dir(), &area)?;
        config_file::read_test(&mut reading.files(), &id).map_err(|e| Refusal::internal(e.to_string()))
    })
    .await;
    match read {
        Ok(Some(test)) => Envelope::ok(Test { test: Some(test) }),
        Ok(None) => Envelope::refused(Refusal::not_found("there is no test by that name on this branch")),
        Err(e) => Envelope::refused(e),
    }
}

// -- locking ------------------------------------------------------------

/// Lock a test, or replace one under the same id. What it expects is committed, which is what makes "change the
/// config or change the test" a decision a reviewer sees rather than a silent edit.
pub async fn put_test(
    Extension(actor): Extension<Actor>,
    AxPath((_, id)): AxPath<(String, String)>,
    at: Addressed,
    Json(mut test): Json<TestDto>,
) -> Envelope<crate::config_api::Save> {
    test.id = id;
    let message = format!("lock test {}", test.name);
    blocking_save(move || {
        if test.pipeline.is_none() {
            let reading = match config_api::Reading::open(&at.branch, &at.area) {
                Ok(reading) => reading,
                Err(e) => return Envelope::refused(e),
            };
            if let Err(e) = check_lockable(&mut reading.files(), &test) {
                return Envelope::refused(e);
            }
        }
        at.open().map_or_else(Save::refused, |writing| {
            config_api::write_test_file(
                writing,
                &test.id,
                Some(config_file::test_bytes(&test)),
                &message,
                &actor,
            )
        })
    })
    .await
}

/// A test naming no pipeline routes its sample by its source, which `SampleDto.source` may not name. Refused here
/// rather than stored and failed on every later run: whether it is lockable is knowable now, from the sample alone.
fn check_lockable(files: &mut dyn config_file::FileSet, test: &TestDto) -> Result<(), Refusal> {
    match config_file::read_sample(files, &test.sample) {
        Ok(Some(sample)) if sample.source.is_some() => Ok(()),
        Ok(Some(_)) => Err(Refusal::invalid(format!(
            "this test names no pipeline and its sample {:?} names no source, so it cannot be routed -- \
             give the test a pipeline, or give the sample a source",
            test.sample
        ))),
        Ok(None) => Err(Refusal::invalid(format!(
            "this test runs over the sample {:?}, which is not on this branch",
            test.sample
        ))),
        Err(e) => Err(Refusal::internal(e.to_string())),
    }
}

pub async fn delete_test(
    Extension(actor): Extension<Actor>,
    AxPath((_, id)): AxPath<(String, String)>,
    at: Addressed,
) -> Envelope<crate::config_api::Save> {
    let message = format!("delete test {id}");
    blocking_save(move || {
        at.open().map_or_else(Save::refused, |writing| {
            config_api::write_test_file(writing, &id, None, &message, &actor)
        })
    })
    .await
}

// -- running ------------------------------------------------------------

#[derive(Default, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Run {
    /// One per test run, in the order they were named, across every scope this run covered.
    results: Vec<TestResult>,
    /// One entry per scope that reached anything, when `touched` was asked for: every pipeline and route
    /// that scope's change reaches, each with why. Something named here with no result covering it is
    /// reached and untested -- which is why the list is shown at all, not only the results. Empty --
    /// omitted entirely -- when `touched` was not asked for, or no scope run reached anything.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    touched: Vec<AreaImpact>,
}

/// One area's `Impact`, tagged with the area: `Reached.id` is unique only within an area, the same
/// collision `AreaTestSummary`'s doc names.
#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AreaImpact {
    pub area: String,
    #[serde(flatten)]
    pub impact: Impact,
}

/// `Impact::pipelines` and `Impact::routes` both empty -- nothing was asked for, or nothing was reached.
fn impact_is_empty(impact: &Impact) -> bool {
    impact.pipelines.is_empty() && impact.routes.is_empty()
}

#[derive(serde::Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct RunRequest {
    /// Which tests to run. Empty runs every test on the branch, which is what a review asks for.
    #[serde(default)]
    pub ids: Vec<String>,
    /// Narrow to the pipelines this branch's change reaches, and answer which those are. For a pipeline edit that
    /// is one pipeline; for a shared object it is every pipeline that reads it, because the author's tests only
    /// prove what the author thought of.
    #[serde(default)]
    pub touched: bool,
}

/// Run tests against a branch and answer what each made of it.
///
/// Nothing is written. A result is this answer and nothing else: the next caller runs them again, because the
/// config, the sample and the test can each have moved, and because an output is not a function of the commit
/// alone.
pub async fn run_tests(
    Extension(access): Extension<Access>,
    AxPath(area): AxPath<String>,
    Query(q): Query<BranchQuery>,
    Json(req): Json<RunRequest>,
) -> Envelope<Run> {
    let covers = Covers::Area(area);
    blocking_answer(move || run_as(&leader_data_dir(), q.name(), &covers, &access, req)).await
}

/// [`run_tests`] over every area the change on branch `:name` reaches.
pub async fn run_change_tests(
    Extension(access): Extension<Access>,
    AxPath(branch): AxPath<String>,
    Json(req): Json<RunRequest>,
) -> Envelope<Run> {
    blocking_answer(move || run_as(&leader_data_dir(), &branch, &Covers::Change, &access, req)).await
}

/// [`run_tests`] over the leader at `data_dir`.
fn run_as(data_dir: &Path, branch: &str, covers: &Covers, access: &Access, req: RunRequest) -> Envelope<Run> {
    resolve_scopes(data_dir, branch, covers, access, Level::Editor)
        .and_then(|scopes| run_scoped(data_dir, branch, &scopes, req.ids, req.touched))
        .map(|(results, touched)| Run { results, touched })
        .into()
}

/// `run_on_branch` for every scope in `scopes`, merged into one answer: every result carries the scope it
/// ran in, and `touched` holds one entry per scope that reached anything.
fn run_scoped(
    data_dir: &Path,
    branch: &str,
    scopes: &[String],
    ids: Vec<String>,
    touched_only: bool,
) -> Result<(Vec<TestResult>, Vec<AreaImpact>), Refusal> {
    let mut results = Vec::new();
    let mut touched = Vec::new();
    for scope in scopes {
        let (scope_results, impact) = run_on_branch(data_dir, branch, scope, ids.clone(), touched_only)?;
        results.extend(scope_results);
        if !impact_is_empty(&impact) {
            touched.push(AreaImpact {
                area: scope.clone(),
                impact,
            });
        }
    }
    Ok((results, touched))
}

/// Every named test against `branch` within `scope`, or every test on it when none are named.
///
/// The config is read once and handed to each run: a review runs several tests over one tree, and reading it per
/// test would answer from several.
fn run_on_branch(
    data_dir: &Path,
    branch: &str,
    scope: &str,
    ids: Vec<String>,
    touched_only: bool,
) -> Result<(Vec<TestResult>, Impact), Refusal> {
    let reading = config_api::Reading::open_in(data_dir, branch, scope)?;
    run_in(data_dir, reading, scope, ids, touched_only)
}

/// [`run_on_branch`] over a scope already read.
fn run_in(
    data_dir: &Path,
    reading: config_api::Reading,
    scope: &str,
    ids: Vec<String>,
    touched_only: bool,
) -> Result<(Vec<TestResult>, Impact), Refusal> {
    let raw = reading.raw();

    // The bundle this scope actually runs: `shared/` merged in for a group, the same merge `load_group_from` does
    // for a real deployment. A locked test's pipeline may call a source or read a lookup table declared only in
    // `shared/`, so running it against the group's bare subtree alone would report that source or table unknown
    // rather than running the test -- and a lookup table's own file must resolve against where it was actually
    // declared, `shared/` or the group's own subtree, or a shared table's file reads as missing under the group's
    // prefix. `sibling` reads the commit `reading` already resolved rather than looking up the branch tip again, so
    // this and `reading` never straddle a branch that moved between the two reads.
    let shared_reading = if scope == config_file::SHARED_DIR {
        None
    } else {
        Some(reading.sibling(config_file::SHARED_DIR)?)
    };
    let shared_raw = shared_reading.as_ref().map(|r| r.raw());
    let (exec_file, declared_at) = match (&shared_raw, &shared_reading) {
        (Some(shared), Some(shared_reading)) => (
            smeltr::leader::map::merged_file(shared, &raw, scope),
            smeltr::leader::map::merged_declared_at(shared, &shared_reading.at(), &raw, &reading.at()),
        ),
        _ => (
            raw.file.clone(),
            raw.file
                .lookup_tables
                .iter()
                .map(|t| (t.id.clone(), reading.at()))
                .collect(),
        ),
    };
    let config = serde_json::to_string(&exec_file).map_err(|e| Refusal::internal(e.to_string()))?;

    // What the change reaches, read against `main` -- the tree it would be deployed onto. Merged with `shared/` on
    // both sides for a group scope, the same way `exec_file` is: an edit landing only in `shared/` -- a source, a
    // lookup table -- still has to show up as touched.
    let deployed = if touched_only {
        Some(config_api::Reading::open_in(
            data_dir,
            smeltr::leader::git::MAIN_BRANCH,
            scope,
        )?)
    } else {
        None
    };
    let impact = if let Some(deployed) = &deployed {
        match &shared_raw {
            None => config_file::pipelines_touched(&deployed.raw().file, &exec_file),
            Some(shared) => {
                let shared_before = deployed.sibling(config_file::SHARED_DIR)?.raw();
                let deployed_file = smeltr::leader::map::merged_file(&shared_before, &deployed.raw(), scope);
                let mut impact = config_file::pipelines_touched(&deployed_file, &exec_file);
                // A shared-owned pipeline or route is concatenated into every group's merged file, so left as-is
                // it would be reported reached once per group on top of once under `shared` itself -- an N+1 count
                // for one change nobody locked N+1 tests against. `shared` already reports it; what a group adds
                // is only what the change propagates onto that group's own pipelines and routes (a caller, a
                // route reading a changed source), which a reason other than the object's own id still carries.
                let shared_pipeline_ids: HashSet<&str> = shared.file.pipelines.iter().map(|p| p.id.as_str()).collect();
                let shared_route_ids: HashSet<&str> = shared.file.routes.iter().map(|r| r.id.as_str()).collect();
                // The group's own failure setting reaches the shared pipelines it runs too.
                let group_policy_changed = config_file::pipeline_answers_differ(&deployed.raw().file, &exec_file);
                (impact.pipelines).retain(|r| group_policy_changed || !shared_pipeline_ids.contains(r.id.as_str()));
                impact.routes.retain(|r| !shared_route_ids.contains(r.id.as_str()));
                impact
            }
        }
    } else {
        Impact {
            pipelines: Vec::new(),
            routes: Vec::new(),
            pipeline_names: Default::default(),
        }
    };
    let touched_pipeline_ids: HashSet<&str> = impact.pipelines.iter().map(|r| r.id.as_str()).collect();
    let touched_route_ids: HashSet<&str> = impact.routes.iter().map(|r| r.id.as_str()).collect();

    let listed: Vec<config_file::TestSummary> = config_file::list_tests(&mut reading.files()).0;
    let wanted: Vec<String> = if !ids.is_empty() {
        ids
    } else if let Some(deployed) = &deployed {
        listed
            .into_iter()
            .filter(|t| {
                t.pipelines.iter().any(|p| touched_pipeline_ids.contains(p.as_str()))
                    || t.routes.iter().any(|r| touched_route_ids.contains(r.as_str()))
                    || test_or_sample_changed(deployed, &reading, t)
            })
            .map(|t| t.id)
            .collect()
    } else {
        listed.into_iter().map(|t| t.id).collect()
    };

    let mut results = Vec::with_capacity(wanted.len());
    for id in wanted {
        let test = match config_file::read_test(&mut reading.files(), &id) {
            Ok(Some(test)) => test,
            Ok(None) => {
                results.push(TestResult {
                    id: id.clone(),
                    name: id,
                    pipeline: None,
                    area: scope.to_string(),
                    verdict: None,
                    error: Some("there is no test by that name on this branch".to_string()),
                });
                continue;
            }
            Err(e) => {
                results.push(TestResult {
                    id: id.clone(),
                    name: id,
                    pipeline: None,
                    area: scope.to_string(),
                    verdict: None,
                    error: Some(e.to_string()),
                });
                continue;
            }
        };
        results.push(run_one(&test, scope, &config, &mut reading.files(), &declared_at));
    }
    Ok((results, impact))
}

/// Every test at `commit`, in `shared/` and in each group, that ran and did not produce what it locked: the leader's
/// check of what it deploys (`smeltr::leader::deploy_checks`). A scope whose tests cannot be read is passed over --
/// a group that does not load says so on its own.
pub fn failing_at(data_dir: &Path, commit: &str) -> Vec<FailingTest> {
    let repo = smeltr::leader::git::repo_path(data_dir);
    let groups = smeltr::leader::tree::groups_at(&repo, commit).unwrap_or_default();
    let scopes = std::iter::once(config_file::SHARED_DIR.to_string()).chain(groups);
    let mut out = Vec::new();
    for scope in scopes {
        let Ok(reading) = config_api::Reading::at_commit_in(data_dir, &scope, commit) else {
            continue;
        };
        let Ok((results, _)) = run_in(data_dir, reading, &scope, Vec::new(), false) else {
            continue;
        };
        out.extend(results.into_iter().filter_map(|r| {
            let verdict = r.verdict.filter(|v| !v.passed)?;
            Some(FailingTest {
                scope: r.area,
                id: r.id,
                name: r.name,
                pipeline: r.pipeline,
                why: failure_text(&verdict),
            })
        }));
    }
    out
}

/// What a failed verdict comes to, in a few words: that the counts differ, or where the output first does.
fn failure_text(verdict: &Verdict) -> String {
    if verdict.actual_rows != verdict.expected_rows {
        let documents = |n: usize| format!("{n} document{}", if n == 1 { "" } else { "s" });
        return format!(
            "it produced {} where it locked {}",
            documents(verdict.actual_rows),
            documents(verdict.expected_rows)
        );
    }
    match &verdict.first_difference {
        Some(d) => format!("document {} differs from what it locked", d.at + 1),
        None => "its output differs from what it locked".to_string(),
    }
}

/// Whether `test`'s own file or its sample differs on `branch` from `deployed`. A change that edits only a sample, or
/// only re-locks a test, reaches no pipeline, yet it is exactly what that test checks. The files compare as stored, so
/// one new on the branch counts, and so does an unreadable one whose bytes changed.
fn test_or_sample_changed(
    deployed: &config_api::Reading,
    branch: &config_api::Reading,
    test: &config_file::TestSummary,
) -> bool {
    use config_file::FileSet;
    let differs = |path: Result<String, config_file::BundleError>| match path {
        Ok(path) => deployed.files().read(&path).ok() != branch.files().read(&path).ok(),
        Err(_) => true,
    };
    differs(config_file::sample_path(&test.sample)) || differs(config_file::test_path(&test.id))
}

/// [`run_test`] over the sample `files` holds for it.
fn run_one(
    test: &TestDto,
    scope: &str,
    config: &str,
    files: &mut dyn config_file::FileSet,
    declared_at: &config_file::DeclaredAt,
) -> TestResult {
    let sample = config_file::read_sample(files, &test.sample).map_err(|e| e.to_string());
    run_test(test, scope, config, sample, declared_at)
}

#[cfg(test)]
mod tests {
    use super::*;
    use smeltr::config_file::{DirFileSet, Reached, Reason, SampleDto};
    use smeltr::leader::preview::{run_pipeline_actual, run_route_actual};

    /// The wire shape the GUI's TypeScript is written against: `Reason` is internally tagged on `type`, camelCase,
    /// with its payload fields un-renamed (`rename_all` on an enum renames variants, not a struct variant's own
    /// fields).
    #[test]
    fn touched_serializes_reasons_as_an_internally_tagged_camel_case_union() {
        let touched = Impact {
            pipelines: vec![Reached {
                id: "orders-normalize".to_string(),
                reasons: vec![
                    Reason::Edited,
                    Reason::ReadsTable {
                        table: "geo".to_string(),
                    },
                    Reason::Calls {
                        pipeline: "enrich".to_string(),
                    },
                ],
            }],
            routes: vec![Reached {
                id: "orders".to_string(),
                reasons: vec![
                    Reason::SourceChanged {
                        source: "orders-west".to_string(),
                    },
                    Reason::DestinationChanged {
                        destination: "es-prod".to_string(),
                    },
                ],
            }],
            pipeline_names: [("orders-normalize".to_string(), "Orders".to_string())].into(),
        };
        assert_eq!(
            serde_json::to_value(&touched).unwrap(),
            serde_json::json!({
                "pipelines": [{
                    "id": "orders-normalize",
                    "reasons": [
                        {"type": "edited"},
                        {"type": "readsTable", "table": "geo"},
                        {"type": "calls", "pipeline": "enrich"},
                    ],
                }],
                "routes": [{
                    "id": "orders",
                    "reasons": [
                        {"type": "sourceChanged", "source": "orders-west"},
                        {"type": "destinationChanged", "destination": "es-prod"},
                    ],
                }],
                "pipelineNames": {"orders-normalize": "Orders"},
            })
        );
    }

    /// An `AreaTestSummary`/`AreaImpact` flattens its area alongside the wrapped shape, rather than
    /// nesting it under a second key -- the GUI reads `t.area` next to `t.id` like any other field.
    #[test]
    fn an_area_test_summary_flattens_its_area_beside_the_summary() {
        let scoped = AreaTestSummary {
            area: "west".to_string(),
            summary: TestSummary {
                id: "t1".to_string(),
                name: "t1".to_string(),
                pipeline: Some("orders".to_string()),
                sample: "s1".to_string(),
                rows_in: 1,
                rows_out: 1,
                pipelines: vec!["orders".to_string()],
                routes: vec![],
            },
        };
        assert_eq!(
            serde_json::to_value(&scoped).unwrap(),
            serde_json::json!({
                "area": "west",
                "id": "t1",
                "name": "t1",
                "pipeline": "orders",
                "sample": "s1",
                "rowsIn": 1,
                "rowsOut": 1,
                "pipelines": ["orders"],
            })
        );
    }

    /// `bytes` put in `repo`'s lookup store, as the pointer a commit holds for them.
    fn stored(repo: &std::path::Path, bytes: &[u8]) -> Vec<u8> {
        let mut incoming = smeltr::lookup::store::Incoming::new(repo).unwrap();
        std::io::Write::write_all(&mut incoming, bytes).unwrap();
        incoming.finish(None).unwrap().text().into_bytes()
    }

    fn temp_dir(label: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("smeltr-test-api-{label}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join(config_file::SAMPLES_DIR)).unwrap();
        dir
    }

    fn write_sample(dir: &std::path::Path, sample: &SampleDto) {
        let path = config_file::sample_path(&sample.id).unwrap();
        std::fs::write(dir.join(path), config_file::sample_bytes(sample)).unwrap();
    }

    /// A pipeline that marks every record it sees, so its output is deterministic without a source of its own.
    const PIPELINE_CONFIG: &str = r#"{
      "version": 1,
      "pipelines": [
        { "id": "audit", "name": "audit", "steps": [
          { "id": "mark", "type": "eval", "assignments": [ { "target": "seen", "expr": "true" } ] }
        ] }
      ]
    }"#;

    /// The same pipeline, reached by one route off one source, delivering to one destination -- enough to route a
    /// sample end to end.
    const ROUTED_CONFIG: &str = r#"{
      "version": 1,
      "sources": [
        { "id": "orders", "type": "kafka", "brokers": "b1:9092", "topics": ["orders"], "groupId": "smeltr" }
      ],
      "destinations": [
        { "id": "es-prod", "type": "elasticsearch", "url": "http://es:9200", "defaultIndex": "orders" }
      ],
      "pipelines": [
        { "id": "audit", "name": "audit", "steps": [
          { "id": "mark", "type": "eval", "assignments": [ { "target": "seen", "expr": "true" } ] }
        ] }
      ],
      "routes": [
        { "id": "r1", "source": "orders", "condition": null, "pipeline": "audit",
          "deliver": [ { "to": ["es-prod"] } ] }
      ]
    }"#;

    fn one_record_sample(id: &str, source: Option<&str>) -> SampleDto {
        SampleDto {
            id: id.to_string(),
            name: id.to_string(),
            source: source.map(str::to_string),
            records: vec![smeltr::io::raw_object("{}".to_string())],
            columns: Vec::new(),
        }
    }

    /// A route preview locks, then a second run against that lock -- once matching, once not.
    #[test]
    fn an_end_to_end_test_passes_then_fails_when_the_lock_no_longer_matches() {
        let dir = temp_dir("end-to-end");
        let sample = one_record_sample("orders-sample", Some("orders"));
        write_sample(&dir, &sample);
        let at = config_file::BundleAt::Dir(dir.clone());
        let declared_at = crate::single_origin_declared_at(ROUTED_CONFIG, &at);

        let expected = run_route_actual(ROUTED_CONFIG, 0, declared_at.clone(), sample.clone())
            .expect("the route preview runs cleanly");
        assert_eq!(expected.len(), 1, "one record through one route producing one document");

        let mut test = TestDto {
            id: "routes-it".to_string(),
            name: "routes it".to_string(),
            pipeline: None,
            sample: sample.id.clone(),
            locked_at: 0,
            rows_in: 1,
            expected,
        };

        let passing = run_one(
            &test,
            "west",
            ROUTED_CONFIG,
            &mut DirFileSet { root: &dir },
            &declared_at,
        );
        let verdict = passing.verdict.expect("the route preview runs cleanly");
        assert!(verdict.passed, "{:?}", verdict.first_difference);
        assert_eq!(passing.pipeline, None, "an end-to-end test names no pipeline");
        assert_eq!(passing.area, "west", "the area it was run in travels with the result");

        // The sample held two records at lock and holds one now: a changed input fails the test.
        let shrunk = TestDto {
            rows_in: 2,
            ..test.clone()
        };
        let verdict = run_one(
            &shrunk,
            "west",
            ROUTED_CONFIG,
            &mut DirFileSet { root: &dir },
            &declared_at,
        )
        .verdict
        .expect("it still runs");
        assert!(!verdict.passed);
        assert_eq!(verdict.rows_in, Some(config_file::RowsIn { locked: 2, now: 1 }));

        // What was locked no longer describes what a run produces.
        test.expected[0] = Some(TestDto::end_to_end_document(
            serde_json::json!({"seen": false}),
            "audit",
            "r1",
            vec!["es-prod".to_string()],
        ));
        let failing = run_one(
            &test,
            "west",
            ROUTED_CONFIG,
            &mut DirFileSet { root: &dir },
            &declared_at,
        );
        let verdict = failing.verdict.expect("it still runs, it just no longer matches");
        assert!(!verdict.passed);
        assert_eq!(verdict.first_difference.unwrap().at, 0);
    }

    /// The shape every test on disk holds today: `pipeline` a bare string, with none of the new fields. It still
    /// deserializes, and running it still passes.
    #[test]
    fn a_narrow_test_file_written_before_the_split_still_loads_and_passes() {
        let dir = temp_dir("narrow");
        let sample = one_record_sample("orders-sample", None);
        write_sample(&dir, &sample);
        let at = config_file::BundleAt::Dir(dir.clone());
        let declared_at = crate::single_origin_declared_at(PIPELINE_CONFIG, &at);

        let expected = run_pipeline_actual(
            "audit",
            PIPELINE_CONFIG,
            0,
            declared_at.clone(),
            &[],
            sample.records.clone(),
        )
        .expect("the pipeline preview runs cleanly");
        let old = format!(
            r#"{{"id":"keeps-it","name":"keeps it","pipeline":"audit","sample":"{}","lockedAt":0,"rowsIn":1,"expected":{}}}"#,
            sample.id,
            serde_json::to_string(&expected).unwrap(),
        );

        let test: TestDto = serde_json::from_str(&old).expect("the old shape still deserializes");
        assert_eq!(test.pipeline.as_deref(), Some("audit"));

        let result = run_one(
            &test,
            "shared",
            PIPELINE_CONFIG,
            &mut DirFileSet { root: &dir },
            &declared_at,
        );
        let verdict = result.verdict.expect("the pipeline preview runs cleanly");
        assert!(verdict.passed, "{:?}", verdict.first_difference);
    }

    /// `SampleDto.source` is `Option`, so a test naming no pipeline and a sample naming no source cannot be routed
    /// -- refused at lock time, rather than stored and failing on every run.
    #[test]
    fn a_test_naming_no_pipeline_and_an_unroutable_sample_is_refused() {
        let dir = temp_dir("unroutable");
        let sample = one_record_sample("no-source", None);
        write_sample(&dir, &sample);

        let test = TestDto {
            id: "cant-lock".to_string(),
            name: "can't lock".to_string(),
            pipeline: None,
            sample: sample.id.clone(),
            locked_at: 0,
            rows_in: 1,
            expected: vec![],
        };
        let err = check_lockable(&mut DirFileSet { root: &dir }, &test).expect_err("the sample names no source");
        assert!(err.message().contains("names no source"), "{err}");
    }

    // -- run_scoped -------------------------------------------------------

    /// Same-id tests in two different scopes are not deduplicated or overwritten by a merged, multi-scope
    /// run: `run_scoped` answers with one result per scope, each naming which scope it ran in -- ids are
    /// unique only within a scope, so a plain id-keyed map on the far side would otherwise collide them.
    #[test]
    fn same_named_tests_in_two_scopes_stay_distinct_in_a_merged_run() {
        use smeltr::leader::git;

        let data = temp_dir("scoped-run");
        let repo = git::repo_path(&data);
        git::init_bare_repo(&repo).unwrap();
        git::ensure_main_commit(&repo).unwrap();

        let sample = one_record_sample("s", None);
        // Names a pipeline no scope here defines, so it runs and errors rather than panicking -- a
        // `TestResult` with `error: Some(..)` is still a result to merge and tag, which is all this test
        // is about.
        let test = TestDto {
            id: "dup-test".to_string(),
            name: "dup test".to_string(),
            pipeline: Some("audit".to_string()),
            sample: sample.id.clone(),
            locked_at: 0,
            rows_in: 1,
            expected: vec![],
        };
        let manifest = config_file::manifest_bytes(&config_file::FileConfig {
            version: config_file::CURRENT_VERSION,
            sources: vec![],
            destinations: vec![],
            pipelines: vec![],
            routes: vec![],
            lookup_tables: vec![],
            grok_patterns: vec![],
            on_exception: None,
            name: None,
        });

        let mut edits = Vec::new();
        for group in ["west", "east"] {
            edits.push(git::FileEdit {
                path: format!("groups/{group}/system.json"),
                content: Some(manifest.clone()),
            });
            edits.push(git::FileEdit {
                path: format!("groups/{group}/{}", config_file::sample_path(&sample.id).unwrap()),
                content: Some(config_file::sample_bytes(&sample)),
            });
            edits.push(git::FileEdit {
                path: format!("groups/{group}/{}", config_file::test_path(&test.id).unwrap()),
                content: Some(config_file::test_bytes(&test)),
            });
        }
        let parent = git::main_head(&repo).unwrap();
        // `work` does not exist yet, so this must be its first save (`expected: None`), not a move onto a
        // branch that is already there.
        match git::commit_edits(
            &repo,
            "work",
            &parent,
            None,
            &edits,
            "seed two scopes",
            &Actor::system(),
        )
        .unwrap()
        {
            git::Committed::At(_) => {}
            git::Committed::Unchanged => panic!("the fixture's save changed nothing"),
            git::Committed::Moved(now) => panic!("the fixture's branch was at {now:?}"),
        }

        let (results, touched) = run_scoped(&data, "work", &["west".to_string(), "east".to_string()], vec![], false)
            .expect("both scopes read");

        assert!(touched.is_empty(), "not asked for: {touched:?}");
        assert_eq!(results.len(), 2, "{results:?}");
        assert!(results.iter().all(|r| r.id == "dup-test"), "{results:?}");
        let mut scopes: Vec<&str> = results.iter().map(|r| r.area.as_str()).collect();
        scopes.sort();
        assert_eq!(scopes, vec!["east", "west"]);
    }

    /// A group's pipeline reads a `shared/` lookup table (`load_group_from`'s merge). A branch that edits only
    /// `shared/lookup-tables.json` -- no file under the group's own subtree -- still has to reach that pipeline:
    /// deployed and branch are each read with `shared/` merged in, not compared subtree-only.
    #[test]
    fn a_branch_editing_only_shared_reaches_a_group_pipeline_that_reads_it() {
        use smeltr::config_file::{LookupSourceDto, LookupTableDto, MatchKindDto};
        use smeltr::leader::git;

        let data = temp_dir("shared-impact");
        let repo = git::repo_path(&data);
        git::init_bare_repo(&repo).unwrap();
        git::ensure_main_commit(&repo).unwrap();

        let table = |delimiter: &str| config_file::FileConfig {
            lookup_tables: vec![LookupTableDto {
                id: "geo".to_string(),
                source: LookupSourceDto::Csv {
                    path: "lookups/geo.csv".to_string(),
                    delimiter: delimiter.to_string(),
                    headers: vec![],
                },
            }],
            ..empty_file_config()
        };
        let group_file = config_file::FileConfig {
            pipelines: vec![config_file::PipelineDto {
                id: "audit".to_string(),
                name: "audit".to_string(),
                steps: vec![
                    config_file::StepDto::Eval {
                        id: "bind-city-id".to_string(),
                        filter: None,
                        disabled: false,
                        assignments: vec![config_file::AssignmentDto {
                            target: "city_id".to_string(),
                            expr: "\"1\"".to_string(),
                        }],
                    },
                    config_file::StepDto::Lookup {
                        id: "lookup-geo".to_string(),
                        filter: None,
                        disabled: false,
                        input: "city_id".to_string(),
                        table: "geo".to_string(),
                        match_kind: MatchKindDto::Exact,
                        multiplicity: Default::default(),
                        key_column: "city_id".to_string(),
                        outputs: vec![config_file::LookupOutputDto {
                            source: "country".to_string(),
                            field: "country".to_string(),
                            default: None,
                            ty: None,
                        }],
                    },
                ],
            }],
            ..empty_file_config()
        };

        let geo_csv = git::FileEdit {
            path: format!("{}/lookups/geo.csv", config_file::SHARED_DIR),
            content: Some(stored(&repo, b"city_id,country\n1,US\n")),
        };

        // Seed `main` with a west group and a `shared/` table its pipeline reads, both loadable as deployed today.
        seed_main(&repo, &data, &table(","), &[("west", &group_file)], vec![geo_csv]);

        // `work` edits only `shared/lookup-tables.json` -- the group's own files are untouched.
        let parent = git::main_head(&repo).unwrap();
        let edits: Vec<git::FileEdit> = bundle_edits("shared", &table(";"));
        match git::commit_edits(
            &repo,
            "work",
            &parent,
            None,
            &edits,
            "widen the geo delimiter",
            &Actor::system(),
        )
        .unwrap()
        {
            git::Committed::At(_) => {}
            git::Committed::Unchanged => panic!("the fixture's save changed nothing"),
            git::Committed::Moved(now) => panic!("the fixture's branch was at {now:?}"),
        }

        let (_, impact) = run_on_branch(&data, "work", "west", vec![], true).expect("west reads");
        assert_eq!(
            impact.pipelines,
            vec![Reached {
                id: "audit".to_string(),
                reasons: vec![Reason::ReadsTable {
                    table: "geo".to_string()
                }],
            }],
            "west's own files didn't change, but the shared table its pipeline reads did: {impact:?}"
        );
    }

    /// A locked test on a group's pipeline that reads a `shared/` lookup table runs against the merged config, not
    /// the group's bare subtree: previewing it needs every source, pipeline and table the pipeline actually reads
    /// when deployed, `shared/`-declared ones included, or the preview reports them unknown rather than running.
    /// The table's own file has to resolve against `shared/`, where it was actually declared, too -- a group-scoped
    /// origin for every table (the group's own included) would 404 looking for it under the group's own subtree.
    #[test]
    fn a_locked_test_on_a_pipeline_reading_a_shared_table_runs_against_the_merged_config() {
        use smeltr::config_file::{LookupSourceDto, LookupTableDto, MatchKindDto};
        use smeltr::leader::git;

        let data = temp_dir("shared-exec");
        let repo = git::repo_path(&data);
        git::init_bare_repo(&repo).unwrap();
        git::ensure_main_commit(&repo).unwrap();

        let shared = config_file::FileConfig {
            lookup_tables: vec![LookupTableDto {
                id: "geo".to_string(),
                source: LookupSourceDto::Csv {
                    path: "lookups/geo.csv".to_string(),
                    delimiter: ",".to_string(),
                    headers: vec![],
                },
            }],
            ..empty_file_config()
        };
        let group_file = config_file::FileConfig {
            pipelines: vec![config_file::PipelineDto {
                id: "audit".to_string(),
                name: "audit".to_string(),
                steps: vec![
                    config_file::StepDto::Eval {
                        id: "bind-city-id".to_string(),
                        filter: None,
                        disabled: false,
                        assignments: vec![config_file::AssignmentDto {
                            target: "city_id".to_string(),
                            expr: "\"1\"".to_string(),
                        }],
                    },
                    config_file::StepDto::Lookup {
                        id: "lookup-geo".to_string(),
                        filter: None,
                        disabled: false,
                        input: "city_id".to_string(),
                        table: "geo".to_string(),
                        match_kind: MatchKindDto::Exact,
                        multiplicity: Default::default(),
                        key_column: "city_id".to_string(),
                        outputs: vec![config_file::LookupOutputDto {
                            source: "country".to_string(),
                            field: "country".to_string(),
                            default: None,
                            ty: None,
                        }],
                    },
                ],
            }],
            ..empty_file_config()
        };

        let sample = one_record_sample("s", None);
        let test = TestDto {
            id: "audit-test".to_string(),
            name: "audit test".to_string(),
            pipeline: Some("audit".to_string()),
            sample: sample.id.clone(),
            locked_at: 0,
            rows_in: 1,
            expected: vec![],
        };
        let fixture_files = vec![
            git::FileEdit {
                path: format!("groups/west/{}", config_file::sample_path(&sample.id).unwrap()),
                content: Some(config_file::sample_bytes(&sample)),
            },
            git::FileEdit {
                path: format!("groups/west/{}", config_file::test_path(&test.id).unwrap()),
                content: Some(config_file::test_bytes(&test)),
            },
            git::FileEdit {
                path: format!("{}/lookups/geo.csv", config_file::SHARED_DIR),
                content: Some(stored(&repo, b"city_id,country\n1,US\n")),
            },
        ];

        seed_main(&repo, &data, &shared, &[("west", &group_file)], fixture_files);

        let (results, _) =
            run_on_branch(&data, "main", "west", vec!["audit-test".to_string()], false).expect("west runs");
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].error, None,
            "the pipeline reads a table declared in shared/; the merged config should have found it: {results:?}"
        );
    }

    /// The deployed commit's check runs every scope's tests and keeps only the ones that ran and failed; one that
    /// could not run says so where it is run by hand.
    #[test]
    fn the_deployed_commit_reports_the_tests_that_ran_and_failed() {
        use smeltr::leader::git;

        let data = temp_dir("deployed-failing");
        let repo = git::repo_path(&data);
        git::init_bare_repo(&repo).unwrap();
        git::ensure_main_commit(&repo).unwrap();

        let group_file: config_file::FileConfig = serde_json::from_str(PIPELINE_CONFIG).unwrap();
        let sample = one_record_sample("s", None);
        let test = |id: &str, pipeline: &str| TestDto {
            id: id.to_string(),
            name: id.to_string(),
            pipeline: Some(pipeline.to_string()),
            sample: sample.id.clone(),
            locked_at: 0,
            rows_in: 1,
            // The pipeline keeps the one record, so locking none is a lock it no longer matches.
            expected: vec![],
        };
        let mut files = vec![git::FileEdit {
            path: format!("groups/west/{}", config_file::sample_path(&sample.id).unwrap()),
            content: Some(config_file::sample_bytes(&sample)),
        }];
        for t in [test("fails", "audit"), test("cannot-run", "gone")] {
            files.push(git::FileEdit {
                path: format!("groups/west/{}", config_file::test_path(&t.id).unwrap()),
                content: Some(config_file::test_bytes(&t)),
            });
        }
        seed_main(&repo, &data, &empty_file_config(), &[("west", &group_file)], files);

        let deployed = git::deployed(&repo).unwrap();
        assert_eq!(
            failing_at(&data, &deployed),
            vec![FailingTest {
                scope: "west".to_string(),
                id: "fails".to_string(),
                name: "fails".to_string(),
                pipeline: Some("audit".to_string()),
                why: "it produced 1 document where it locked 0 documents".to_string(),
            }]
        );
    }

    /// A `shared/`-owned pipeline reached by an edit is reported once, under `shared`'s own scope -- not once per
    /// group it is concatenated into as well. What a group scope adds on top of that is only what the change
    /// propagates onto the group's own objects: here, the group's own pipeline that calls the changed one.
    #[test]
    fn a_changed_shared_pipeline_is_reported_once_not_once_per_group() {
        use smeltr::leader::git;

        let data = temp_dir("shared-attribution");
        let repo = git::repo_path(&data);
        git::init_bare_repo(&repo).unwrap();
        git::ensure_main_commit(&repo).unwrap();

        let shared = |steps: Vec<config_file::StepDto>| config_file::FileConfig {
            pipelines: vec![config_file::PipelineDto {
                id: "enrich".to_string(),
                name: "enrich".to_string(),
                steps,
            }],
            ..empty_file_config()
        };
        let group_file = config_file::FileConfig {
            pipelines: vec![config_file::PipelineDto {
                id: "orders".to_string(),
                name: "orders".to_string(),
                steps: vec![config_file::StepDto::Call {
                    id: "call-enrich".to_string(),
                    filter: None,
                    disabled: false,
                    pipeline: "enrich".to_string(),
                }],
            }],
            ..empty_file_config()
        };

        seed_main(&repo, &data, &shared(vec![]), &[("west", &group_file)], vec![]);

        let parent = git::main_head(&repo).unwrap();
        let edits = bundle_edits(
            config_file::SHARED_DIR,
            &shared(vec![config_file::StepDto::Comment {
                id: "note".to_string(),
                text: "changed".to_string(),
            }]),
        );
        git::commit_edits(&repo, "work", &parent, None, &edits, "edit enrich", &Actor::system()).unwrap();

        let (_, impact) = run_on_branch(&data, "work", "west", vec![], true).expect("west reads");
        assert_eq!(
            impact.pipelines,
            vec![Reached {
                id: "orders".to_string(),
                reasons: vec![Reason::Calls {
                    pipeline: "enrich".to_string()
                }],
            }],
            "enrich is shared-owned; west adds only what the change propagates onto west's own pipelines: {impact:?}"
        );

        let (_, shared_impact) =
            run_on_branch(&data, "work", config_file::SHARED_DIR, vec![], true).expect("shared reads");
        assert_eq!(
            shared_impact.pipelines,
            vec![Reached {
                id: "enrich".to_string(),
                reasons: vec![Reason::Edited],
            }],
            "shared's own scope still reports the pipeline it owns: {shared_impact:?}"
        );
    }

    /// A branch that edits only a sample reaches no pipeline, yet a review still runs the test locked over that
    /// sample -- its record count is what changed. A test over a sample the branch left alone stays out.
    #[test]
    fn a_branch_editing_only_a_sample_runs_the_tests_locked_over_it() {
        use smeltr::leader::git;

        let data = temp_dir("sample-only");
        let repo = git::repo_path(&data);
        git::init_bare_repo(&repo).unwrap();
        git::ensure_main_commit(&repo).unwrap();

        let group_file: config_file::FileConfig = serde_json::from_str(PIPELINE_CONFIG).unwrap();
        let test_over = |sample: &SampleDto| TestDto {
            id: format!("{}-test", sample.id),
            name: format!("{} test", sample.id),
            pipeline: Some("audit".to_string()),
            sample: sample.id.clone(),
            locked_at: 0,
            rows_in: 1,
            expected: vec![],
        };
        let file_edit = |path: String, content: Vec<u8>| git::FileEdit {
            path: format!("groups/west/{path}"),
            content: Some(content),
        };
        let mut extra = Vec::new();
        for sample in [one_record_sample("edited", None), one_record_sample("kept", None)] {
            let test = test_over(&sample);
            extra.push(file_edit(
                config_file::sample_path(&sample.id).unwrap(),
                config_file::sample_bytes(&sample),
            ));
            extra.push(file_edit(
                config_file::test_path(&test.id).unwrap(),
                config_file::test_bytes(&test),
            ));
        }
        // `broken` does not parse on either side, and the branch still changes it.
        let broken = one_record_sample("broken", None);
        extra.push(file_edit(config_file::sample_path(&broken.id).unwrap(), b"{".to_vec()));
        extra.push(file_edit(
            config_file::test_path("broken-test").unwrap(),
            config_file::test_bytes(&test_over(&broken)),
        ));
        seed_main(&repo, &data, &empty_file_config(), &[("west", &group_file)], extra);

        // `work` adds a record to `edited`, rewrites `broken` and changes nothing else.
        let mut edited = one_record_sample("edited", None);
        edited.records.push(edited.records[0].clone());
        let edits = vec![
            file_edit(
                config_file::sample_path(&edited.id).unwrap(),
                config_file::sample_bytes(&edited),
            ),
            file_edit(config_file::sample_path("broken").unwrap(), b"{{".to_vec()),
        ];
        let parent = git::main_head(&repo).unwrap();
        match git::commit_edits(&repo, "work", &parent, None, &edits, "grow a sample", &Actor::system()).unwrap() {
            git::Committed::At(_) => {}
            git::Committed::Unchanged => panic!("the fixture's save changed nothing"),
            git::Committed::Moved(now) => panic!("the fixture's branch was at {now:?}"),
        }

        let (results, impact) = run_on_branch(&data, "work", "west", vec![], true).expect("west reads");
        assert!(impact_is_empty(&impact), "no pipeline changed: {impact:?}");
        let mut ids: Vec<&str> = results.iter().map(|r| r.id.as_str()).collect();
        ids.sort();
        assert_eq!(ids, vec!["broken-test", "edited-test"], "{results:?}");
        let broken = results.iter().find(|r| r.id == "broken-test").unwrap();
        assert!(broken.error.is_some(), "an unreadable sample is reported: {broken:?}");
        let edited = results.iter().find(|r| r.id == "edited-test").unwrap();
        let verdict = edited.verdict.as_ref().expect("it runs");
        assert_eq!(verdict.rows_in, Some(config_file::RowsIn { locked: 1, now: 2 }));
    }

    /// A change to `shared/` alone is judged as shared: someone holding shared and no group lists and runs its
    /// tests, every group's among them, and someone lacking the level in shared is answered 403.
    #[test]
    fn a_shared_only_change_is_judged_as_shared() {
        use axum::http::StatusCode;
        use smeltr::leader::git;
        use std::collections::BTreeMap;

        let data = temp_dir("shared-judged");
        let repo = git::repo_path(&data);
        git::init_bare_repo(&repo).unwrap();
        git::ensure_main_commit(&repo).unwrap();

        let group_file: config_file::FileConfig = serde_json::from_str(PIPELINE_CONFIG).unwrap();
        let sample = one_record_sample("s", None);
        let test = TestDto {
            id: "west-test".to_string(),
            name: "west test".to_string(),
            pipeline: Some("audit".to_string()),
            sample: sample.id.clone(),
            locked_at: 0,
            rows_in: 1,
            expected: vec![],
        };
        let files = vec![
            git::FileEdit {
                path: format!("groups/west/{}", config_file::sample_path(&sample.id).unwrap()),
                content: Some(config_file::sample_bytes(&sample)),
            },
            git::FileEdit {
                path: format!("groups/west/{}", config_file::test_path(&test.id).unwrap()),
                content: Some(config_file::test_bytes(&test)),
            },
        ];
        seed_main(&repo, &data, &empty_file_config(), &[("west", &group_file)], files);

        let parent = git::main_head(&repo).unwrap();
        let edits = bundle_edits(
            config_file::SHARED_DIR,
            &config_file::FileConfig {
                name: Some("renamed".to_string()),
                ..empty_file_config()
            },
        );
        git::commit_edits(&repo, "work", &parent, None, &edits, "edit shared", &Actor::system()).unwrap();

        let change = ("work", &Covers::Change);
        let holding = |level: Level| Access::Areas(BTreeMap::from([(config_file::SHARED_DIR.to_string(), level)]));

        let listed = list_in(&data, change.0, change.1, &holding(Level::Viewer));
        assert_eq!(listed.status, StatusCode::OK);
        let listed = listed.to_json();
        assert_eq!(listed["ok"], true, "{listed}");
        assert_eq!(
            listed["tests"][0]["id"], "west-test",
            "a shared change still lists west's tests: {listed}"
        );

        let ran = run_as(
            &data,
            change.0,
            change.1,
            &holding(Level::Deployer),
            RunRequest::default(),
        );
        assert_eq!(ran.status, StatusCode::OK);
        let ran = ran.to_json();
        assert_eq!(ran["ok"], true, "{ran}");
        assert_eq!(ran["results"][0]["id"], "west-test", "{ran}");
        assert_eq!(ran["results"][0]["area"], "west", "{ran}");

        let ran = run_as(
            &data,
            change.0,
            change.1,
            &holding(Level::Editor),
            RunRequest::default(),
        );
        assert_eq!(
            ran.status,
            StatusCode::OK,
            "an editor in shared runs a shared change's tests"
        );

        // West's editor reads shared but does not edit it.
        let west_editor = Access::Areas(BTreeMap::from([("west".to_string(), Level::Editor)]));
        let refused = run_as(&data, change.0, change.1, &west_editor, RunRequest::default());
        assert_eq!(refused.status, StatusCode::FORBIDDEN);
        assert_eq!(refused.to_json()["ok"], false);

        let unheld = Access::Areas(BTreeMap::new());
        assert_eq!(
            list_in(&data, change.0, change.1, &unheld).status,
            StatusCode::FORBIDDEN
        );
    }

    fn empty_file_config() -> config_file::FileConfig {
        config_file::FileConfig {
            version: config_file::CURRENT_VERSION,
            sources: vec![],
            destinations: vec![],
            pipelines: vec![],
            routes: vec![],
            lookup_tables: vec![],
            grok_patterns: vec![],
            on_exception: None,
            name: None,
        }
    }

    /// `file`'s bundle files, each prefixed under `groups/{group}/` or `shared/`.
    fn bundle_edits(prefix: &str, file: &config_file::FileConfig) -> Vec<smeltr::leader::git::FileEdit> {
        config_file::bundle_files(file)
            .expect("a well-formed bundle")
            .into_iter()
            .map(|(path, content)| smeltr::leader::git::FileEdit {
                path: format!("{prefix}/{path}"),
                content: Some(content),
            })
            .collect()
    }

    /// Deploys `shared` and every named group onto `main`, as one commit: what `run_on_branch` reads as
    /// `MAIN_BRANCH` before a branch is compared against it. `extra` is files a bundle doesn't cover on its own,
    /// such as a lookup table's CSV.
    fn seed_main(
        repo: &std::path::Path,
        data: &std::path::Path,
        shared: &config_file::FileConfig,
        groups: &[(&str, &config_file::FileConfig)],
        extra: Vec<smeltr::leader::git::FileEdit>,
    ) {
        use smeltr::leader::git;

        let mut edits = bundle_edits(config_file::SHARED_DIR, shared);
        for (group, file) in groups {
            edits.extend(bundle_edits(&format!("groups/{group}"), file));
        }
        edits.extend(extra);
        let parent = git::main_head(repo).unwrap();
        git::commit_edits(repo, "seed", &parent, None, &edits, "seed", &Actor::system()).unwrap();
        git::open_change(repo, "seed", "seed", "", &Actor::system()).unwrap();
        git::deploy_change(data, "seed", &Actor::system()).expect("the seed loads and deploys");
    }
}
