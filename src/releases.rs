//! What every supported release left behind, kept so this build proves it can carry it forward.
//!
//! `tests/fixtures/releases/<release>/` holds a leader data directory and the messages a worker and a leader of
//! that release send each other, all written by that release's own code (`just record-release`). This build has
//! to open the stores, load the deployment they hold, and read every message. `released-steps.txt` holds a hash of
//! every store step that has shipped, so a step edited after release fails here rather than on somebody's leader, and
//! `releases.txt` names every release recorded, so a fixture that goes missing fails here too.

use std::path::{Path, PathBuf};

use crate::heartbeat::{HeartbeatRequest, HeartbeatResponse};

fn releases_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/releases")
}

/// The group every fixture deploys, from `tests/fixtures/bundle/`.
const GROUP: &str = "default";

/// Every step this build knows, per store, as the line `released-steps.txt` keeps for it.
fn step_lines() -> Vec<String> {
    let sql = |store: &str, steps: &[&str]| -> Vec<String> {
        steps
            .iter()
            .enumerate()
            .map(|(i, step)| format!("{store} {} {}", i + 1, crate::leader::auth::hash_secret(step)))
            .collect()
    };
    let mut lines = sql("leader.db", crate::leader::sqlite_store::STEPS);
    lines.extend(sql("registry.db", crate::leader::series::STEPS));
    lines.extend(crate::leader::steps::REPO_STEPS.iter().enumerate().map(|(i, step)| {
        let what = format!("{} additive={}", step.source, step.additive);
        format!("config.git {} {}", i + 1, crate::leader::auth::hash_secret(&what))
    }));
    lines
}

/// A heartbeat as a worker sends one, carrying a value in every field.
fn sample_heartbeat() -> HeartbeatRequest {
    serde_json::from_value(serde_json::json!({
        "workerId": "w1",
        "bootId": 7,
        "groupId": GROUP,
        "stopAfterUnreachableSecs": 300,
        "commit": "0000000000000000000000000000000000000000",
        "smeltrVersion": crate::version::VERSION,
        "release": "1.0.0",
        "takesAuthority": true,
        "secretVersions": [["kafka-cred", 4]],
        "authorityFingerprints": ["ab12"],
        "authorityExpiresMillis": 1700000000000i64,
        "stopping": true,
        "singleWorker": [{"source": "listing", "place": "s3://logs/app/"}],
        "progress": [{
            "source": "listing",
            "place": "s3://logs/app/",
            "cursor": "c1",
            "added": [{"item": "app/1.log", "entry": "e1"}],
            "removed": ["app/0.log"]
        }],
        "streams": {
            "parsers": [{
                "source": "orders", "route": "orders-to-es", "pipeline": "normalize", "step": "parse", "rows": 12,
                "fields": [{
                    "name": "order.total", "declared": false, "suggested": "f64", "present": 12,
                    "bools": 0, "ints": 2, "floats": 10, "strings": 0, "arrays": 0, "objects": 0, "failed": 0
                }],
                "fieldsDropped": 1
            }],
            "unmatched": [{
                "source": "hec", "rows": 7, "keys": [{"key": "okta:system", "rows": 7}], "keysDropped": 0
            }]
        },
        "supervising": {
            "desiredCommit": "0000000000000000000000000000000000000000",
            "phase": "starting",
            "phaseSinceMs": 1700000000000i64,
            "discoveredMs": 1700000000000i64,
            "attemptedMs": 1700000000000i64,
            "incarnation": 2,
            "failure": "refused"
        },
        "resources": {
            "cpuPercent": 12.5,
            "cpuLimitCores": 2.0,
            "cpuLimited": true,
            "memoryUsedBytes": 1024,
            "memoryLimitBytes": 4096,
            "memoryLimited": true,
            "diskFreeBytes": 10,
            "diskTotalBytes": 100
        },
        "logs": [{
            "seq": 1,
            "timestampMs": 1700000000000i64,
            "level": "info",
            "site": "worker.started",
            "message": "started",
            "thread": "reader-0",
            "bootId": 7,
            "fields": [["n", {"kind": "uint", "value": 3}]]
        }],
        "threads": [{
            "threadId": "reader-0",
            "groupId": GROUP,
            "kind": "reader",
            "partitionsOwned": [{"topic": "orders", "partition": 3}],
            "metrics": [{"metric": "smeltr_rows_polled_total", "labels": [["source", "0"]], "value": {"kind": "counter", "value": 12}}]
        }]
    }))
    .expect("the sample is a heartbeat this build reads")
}

/// Every message a worker and a leader send each other, by the file a fixture keeps it in, with a value in every
/// field.
fn messages() -> Vec<(&'static str, serde_json::Value)> {
    vec![
        ("heartbeat.json", serde_json::to_value(sample_heartbeat()).unwrap()),
        (
            "heartbeat-response.json",
            serde_json::to_value(
                serde_json::from_value::<HeartbeatResponse>(serde_json::json!({
                    "captures": [{
                        "session": "s1",
                        "group": GROUP,
                        "source": {"id": "probe", "type": "metrics", "bucketSeconds": 5},
                        "filter": "true",
                        "maxMatches": 10,
                        "maxBytes": 1000,
                        "deadlineSecs": 60,
                        "secrets": [{"name": "kafka-cred", "version": 4, "value": "x"}]
                    }],
                    "secrets": [{"name": "kafka-cred", "version": 4, "value": "x"}],
                    "authority": "-----BEGIN CERTIFICATE-----",
                    "holds": [{"source": "listing", "place": "s3://logs/app/", "cursor": "c1"}]
                }))
                .expect("the sample is a response this build reads"),
            )
            .unwrap(),
        ),
        (
            "capture-report.json",
            serde_json::to_value(
                serde_json::from_value::<crate::heartbeat::CaptureReport>(serde_json::json!({
                    "session": "s1",
                    "workerId": "w1",
                    "batches": [{
                        "columns": [{"name": "_raw", "type": "str"}, {"name": "__source.file", "type": "str"}],
                        "rows": [{"values": ["a line", "app/1.log"], "failed": [0], "worker": "w1"}]
                    }],
                    "finished": {"failed": "refused"}
                }))
                .expect("the sample is a report this build reads"),
            )
            .unwrap(),
        ),
        (
            "enroll-request.json",
            serde_json::to_value(crate::leader::EnrollRequest {
                code: "smeltr-1-x-y".to_string(),
                worker_id: "w1".to_string(),
            })
            .unwrap(),
        ),
        (
            "enroll-result.json",
            serde_json::to_value(crate::leader::EnrollResult {
                ok: true,
                error: None,
                retry: true,
                token: Some("token".to_string()),
            })
            .unwrap(),
        ),
    ]
}

/// Read `message` as the type `name` holds, the way this build reads it off the wire.
fn read_message(name: &str, message: &[u8]) -> Result<(), String> {
    match name {
        "heartbeat.json" => serde_json::from_slice::<HeartbeatRequest>(message).map(drop),
        "heartbeat-response.json" => {
            // A capture this build cannot read still parses, as `Unreadable`, so the answer survives a newer leader.
            // What an older one sent must read whole.
            let response = serde_json::from_slice::<HeartbeatResponse>(message).map_err(|e| format!("{name}: {e}"))?;
            return match response
                .captures
                .iter()
                .find(|c| matches!(c, crate::heartbeat::Invited::Unreadable { .. }))
            {
                Some(c) => Err(format!("{name}: capture {} no longer reads", c.session())),
                None => Ok(()),
            };
        }
        "capture-report.json" => serde_json::from_slice::<crate::heartbeat::CaptureReport>(message).map(drop),
        "enroll-request.json" => serde_json::from_slice::<crate::leader::EnrollRequest>(message).map(drop),
        "enroll-result.json" => serde_json::from_slice::<crate::worker::EnrollResponse>(message).map(drop),
        _ => panic!("no reader for {name}"),
    }
    .map_err(|e| format!("{name}: {e}"))
}

/// Write `tests/fixtures/releases/$SMELTR_RECORD_RELEASE/` from this build, and record its steps as released.
#[test]
#[ignore = "run by `just record-release`, once per release"]
fn record_release() {
    let release = std::env::var("SMELTR_RECORD_RELEASE").expect("SMELTR_RECORD_RELEASE names the release");
    let dir = releases_dir().join(&release);
    assert!(!dir.exists(), "{} is already recorded", dir.display());
    // Every line already kept is still true of this build, so writing them all again at the end only adds new steps.
    let changed = changed_steps();
    assert!(changed.is_empty(), "released steps were changed: {changed:?}");
    if let Some(previous) = released().last()
        && let Err(why) = bump_allowed(previous, &release, &new_steps())
    {
        panic!("{why}");
    }
    let data = dir.join("data");
    std::fs::create_dir_all(&data).unwrap();

    let store = crate::leader::sqlite_store::SqliteStore::open(&data.join("leader.db"), &data.join("sealing.key"))
        .expect("leader.db opens");
    drop(store);

    let heartbeat = sample_heartbeat();
    let registry = crate::leader::registry::Registry::open(&data.join("registry.db")).expect("registry.db opens");
    registry.record_heartbeat(
        &heartbeat.worker_id,
        heartbeat.threads.clone(),
        crate::leader::registry::WorkerFacts {
            stop_after_unreachable_secs: heartbeat.stop_after_unreachable_secs,
            commit: heartbeat.commit.clone(),
            smeltr_version: heartbeat.smeltr_version.clone(),
            release: heartbeat.release.clone(),
            ..Default::default()
        },
        None,
        None,
        1,
    );
    drop(registry);

    let bundle = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/bundle");
    let edits: Vec<crate::leader::git::FileEdit> = walk(&bundle)
        .into_iter()
        .map(|file| crate::leader::git::FileEdit {
            path: format!(
                "groups/{GROUP}/{}",
                file.strip_prefix(&bundle).unwrap().to_string_lossy().replace('\\', "/")
            ),
            content: Some(std::fs::read(&file).unwrap()),
        })
        .collect();
    let repo = crate::leader::git::repo_path(&data);
    assert!(crate::leader::git::seed_first_commit(&repo, &edits, "the release fixture").unwrap());

    for (name, message) in messages() {
        std::fs::write(dir.join(name), serde_json::to_vec_pretty(&message).unwrap()).unwrap();
    }

    std::fs::write(
        releases_dir().join("released-steps.txt"),
        step_lines().join("\n") + "\n",
    )
    .unwrap();
    let mut releases = released();
    releases.push(release);
    std::fs::write(releases_dir().join("releases.txt"), releases.join("\n") + "\n").unwrap();
}

/// A step this build has and no release has shipped, by store, and whether a build without it can still use the
/// store.
#[derive(Debug, Clone, Copy, PartialEq)]
struct NewStep {
    store: &'static str,
    additive: bool,
}

/// Every step since the last release. An SQLite store's first step is breaking, since no build without it can use the store.
fn new_steps() -> Vec<NewStep> {
    let ledger = std::fs::read_to_string(releases_dir().join("released-steps.txt")).unwrap_or_default();
    let shipped = |store: &str| {
        ledger
            .lines()
            .filter(|line| line.split(' ').next() == Some(store))
            .count()
    };
    let sql = |store: &'static str, steps: &[&str]| -> Vec<NewStep> {
        steps
            .iter()
            .enumerate()
            .skip(shipped(store))
            .map(|(i, step)| NewStep {
                store,
                additive: i > 0 && crate::leader::steps::is_additive(step),
            })
            .collect()
    };
    let mut steps = sql("leader.db", crate::leader::sqlite_store::STEPS);
    steps.extend(sql("registry.db", crate::leader::series::STEPS));
    steps.extend(
        crate::leader::steps::REPO_STEPS
            .iter()
            .skip(shipped("config.git"))
            .map(|step| NewStep {
                store: "config.git",
                additive: step.additive,
            }),
    );
    steps
}

/// Whether `release` may follow `previous` given the steps it adds: a breaking step needs a new major, since builds
/// of one major take deployments from each other (`version::compatible`) and an older one could not open the
/// store; any step needs at least a new minor.
fn bump_allowed(previous: &str, release: &str, steps: &[NewStep]) -> Result<(), String> {
    let parse = |name: &str| -> Option<(u64, u64, u64)> {
        let mut parts = name.split('.').map(|n| n.parse::<u64>().ok());
        let parsed = (parts.next()??, parts.next()??, parts.next()??);
        parts.next().is_none().then_some(parsed)
    };
    let (Some(from), Some(to)) = (parse(previous), parse(release)) else {
        return Err(format!("{previous} and {release} are not both major.minor.patch"));
    };
    if to <= from {
        return Err(format!("{release} does not come after {previous}"));
    }
    let breaking: Vec<&str> = steps.iter().filter(|s| !s.additive).map(|s| s.store).collect();
    if !breaking.is_empty() && to.0 == from.0 {
        return Err(format!(
            "{release} breaks {} since {previous}, so it is a major release",
            breaking.join(", ")
        ));
    }
    if !steps.is_empty() && (to.0, to.1) == (from.0, from.1) {
        return Err(format!(
            "{release} adds store steps since {previous}, so it is at least a minor release"
        ));
    }
    Ok(())
}

#[test]
fn a_release_is_named_for_the_steps_it_adds() {
    let additive = NewStep {
        store: "leader.db",
        additive: true,
    };
    let breaking = NewStep {
        store: "registry.db",
        additive: false,
    };
    assert_eq!(bump_allowed("1.2.0", "1.2.1", &[]), Ok(()));
    assert!(bump_allowed("1.2.0", "1.2.1", &[additive]).is_err());
    assert_eq!(bump_allowed("1.2.0", "1.3.0", &[additive]), Ok(()));
    assert!(
        bump_allowed("1.2.0", "1.3.0", &[breaking])
            .unwrap_err()
            .contains("registry.db")
    );
    assert_eq!(bump_allowed("1.2.0", "2.0.0", &[breaking]), Ok(()));
    assert!(bump_allowed("1.2.0", "1.1.9", &[]).is_err());
    assert!(bump_allowed("1.2.0", "1.2.0", &[]).is_err());
}

/// The releases recorded so far, oldest first.
fn released() -> Vec<String> {
    std::fs::read_to_string(releases_dir().join("releases.txt"))
        .unwrap_or_default()
        .lines()
        .filter(|line| !line.is_empty())
        .map(str::to_string)
        .collect()
}

/// Every line of `released-steps.txt` this build no longer agrees with.
fn changed_steps() -> Vec<String> {
    let Ok(ledger) = std::fs::read_to_string(releases_dir().join("released-steps.txt")) else {
        return match released().is_empty() {
            true => Vec::new(),
            false => vec!["released-steps.txt is missing".to_string()],
        };
    };
    let now = step_lines();
    ledger
        .lines()
        .filter(|line| !line.is_empty() && !now.iter().any(|l| l == line))
        .map(str::to_string)
        .collect()
}

fn walk(dir: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            files.extend(walk(&path));
        } else {
            files.push(path);
        }
    }
    files
}

/// A step that has shipped is in some leader's store forever: changing it changes nothing there and leaves that
/// leader on a schema this build does not describe. A new step is appended instead.
#[test]
fn every_released_step_is_unchanged() {
    let changed = changed_steps();
    assert!(
        changed.is_empty(),
        "released steps were changed or removed, so append new ones instead: {changed:?}"
    );
}

/// Every supported release's data directory starts under this build and loads the deployment it holds, and
/// every message that release sends is one this build reads.
#[test]
fn every_release_still_starts_and_is_still_understood() {
    for release in released() {
        let fixture = releases_dir().join(&release);
        assert!(
            fixture.is_dir(),
            "{} is a supported release and its fixture is missing",
            fixture.display()
        );
        let named = |what: &str| format!("{}: {what}", fixture.display());

        let data = std::env::temp_dir().join(format!(
            "smeltr-release-{}-{}",
            fixture.file_name().unwrap().to_string_lossy(),
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&data);
        copy(&fixture.join("data"), &data);

        crate::leader::sqlite_store::SqliteStore::open(&data.join("leader.db"), &data.join("sealing.key"))
            .unwrap_or_else(|e| panic!("{}", named(&e.to_string())));
        let registry = crate::leader::registry::Registry::open(&data.join("registry.db"))
            .unwrap_or_else(|e| panic!("{}", named(&e.to_string())));
        assert!(
            registry
                .snapshot()
                .iter()
                .any(|thread| thread.worker_id == "w1" && thread.report.thread_id == "reader-0"),
            "{}",
            named("registry.db no longer holds the worker it recorded")
        );
        let repo = crate::leader::git::repo_path(&data);
        crate::leader::steps::upgrade_repo(&repo).unwrap_or_else(|e| panic!("{}", named(&e)));
        let deployed = crate::leader::git::deployed(&repo).unwrap_or_else(|| panic!("{}", named("nothing deployed")));
        crate::leader::tree::load_group_at(&repo, &deployed, GROUP)
            .unwrap_or_else(|e| panic!("{}", named(&format!("the deployed group does not load: {e}"))));

        let read =
            |name: &str| std::fs::read(fixture.join(name)).unwrap_or_else(|e| panic!("{}", named(&e.to_string())));
        for (name, _) in messages() {
            read_message(name, &read(name)).unwrap_or_else(|e| panic!("{}", named(&e)));
        }
        let _ = std::fs::remove_dir_all(&data);
    }
}

/// The release after this one adds fields to its messages, and a build one release apart still has to read them.
#[test]
fn every_message_reader_takes_a_field_it_does_not_know() {
    for (name, mut message) in messages() {
        message
            .as_object_mut()
            .unwrap()
            .insert("fieldFromANewerBuild".to_string(), serde_json::json!({"any": [1]}));
        read_message(name, &serde_json::to_vec(&message).unwrap()).unwrap();
    }
}

fn copy(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let path = entry.unwrap().path();
        let target = to.join(path.file_name().unwrap());
        if path.is_dir() {
            copy(&path, &target);
        } else {
            std::fs::copy(&path, &target).unwrap();
        }
    }
}
