//! The role rules against the built binary: each role has its own flag, and
//! what a role needs is required once it is on. Each rule is a startup
//! decision, so the exit code and the message are the behavior. No broker
//! or leader is needed.

use std::process::Command;

fn temp_dir(name: &str) -> std::path::PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("smeltr-role-test-{name}-{nanos:x}"));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Runs the binary to completion and returns `(exit_code, stderr)`. The
/// working directory is a fresh temp dir, so a developer's own
/// `smeltr.toml` is never picked up.
fn run_smeltr(dir: &std::path::Path, env: &[(&str, &str)]) -> (i32, String) {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_smeltr"));
    cmd.current_dir(dir);
    for var in [
        "SMELTR_LEADER_ENABLED",
        "SMELTR_WORKER_ENABLED",
        "SMELTR_LEADER_URL",
        "SMELTR_WORKER_GROUP",
        "SMELTR_WORKER_ENROLLMENT_CODE",
        "SMELTR_WORKER_STATE_DIR",
        "SMELTR_CONFIG_FILE",
    ] {
        cmd.env_remove(var);
    }
    for (k, v) in env {
        cmd.env(k, v);
    }
    let out = cmd.output().expect("spawn smeltr");
    (
        out.status.code().unwrap_or(-1),
        String::from_utf8_lossy(&out.stderr).to_string(),
    )
}

/// A missing enrollment code on a first run is an operator error, not a panic.
#[test]
fn a_first_run_worker_without_an_enrollment_code_refuses_to_start() {
    let dir = temp_dir("no-code");
    let state_dir = dir.join("state");
    let (code, stderr) = run_smeltr(
        &dir,
        &[
            ("SMELTR_WORKER_ENABLED", "1"),
            ("SMELTR_LEADER_URL", "http://127.0.0.1:4000"),
            ("SMELTR_WORKER_GROUP", "g1"),
            ("SMELTR_WORKER_STATE_DIR", state_dir.to_str().unwrap()),
        ],
    );
    assert_eq!(code, 1, "stderr: {stderr}");
    assert!(stderr.contains("SMELTR_WORKER_ENROLLMENT_CODE"), "stderr: {stderr}");
    assert!(!stderr.contains("panicked"), "stderr: {stderr}");
    assert!(!stderr.contains("RUST_BACKTRACE"), "stderr: {stderr}");
}

/// The enrollment code is spent on a first run only; a persisted credential is
/// what authenticates a worker from then on. A group need not be named either: a
/// worker that names none serves the default one.
#[test]
fn an_enrolled_worker_needs_neither_an_enrollment_code_nor_a_group() {
    let dir = temp_dir("enrolled");
    let state_dir = dir.join("state");
    std::fs::create_dir_all(&state_dir).unwrap();
    std::fs::write(state_dir.join("credential.token"), "a-persisted-token").unwrap();

    // Nothing listens on the port. A worker with no leader reachable keeps
    // retrying, so still running after startup is the proof.
    let mut child = Command::new(env!("CARGO_BIN_EXE_smeltr"))
        .current_dir(&dir)
        .env("SMELTR_WORKER_ENABLED", "1")
        .env("SMELTR_LEADER_URL", "http://127.0.0.1:1")
        .env("SMELTR_WORKER_STATE_DIR", state_dir.to_str().unwrap())
        .env_remove("SMELTR_WORKER_GROUP")
        .env_remove("SMELTR_WORKER_ENROLLMENT_CODE")
        .env_remove("SMELTR_CONFIG_FILE")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn smeltr in its worker role");

    // A worker that demands a code exits during startup, which the test above shows finishing well inside a second.
    // So this waits only long enough to outlast startup, and stops the moment the child settles it by exiting -- a
    // fixed sleep paid the whole wait even when the answer arrived immediately.
    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(750);
    let mut status = None;
    while status.is_none() && std::time::Instant::now() < deadline {
        status = child.try_wait().expect("poll smeltr");
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    let still_running = status.is_none();
    let _ = child.kill();
    let _ = child.wait();

    assert!(
        still_running,
        "an enrolled worker exited during startup ({status:?}) instead of running with an \
         unreachable leader -- it most likely demanded an enrollment code or a group it does not need"
    );
}

/// A role is chosen, never inferred, so a process told to be nothing says so
/// rather than picking one.
#[test]
fn a_process_with_no_role_refuses_to_start() {
    let dir = temp_dir("no-role");
    let (code, stderr) = run_smeltr(&dir, &[]);
    assert_eq!(code, 1, "stderr: {stderr}");
    assert!(stderr.contains("no role"), "stderr: {stderr}");
    assert!(stderr.contains("SMELTR_LEADER_ENABLED"), "stderr: {stderr}");
    assert!(stderr.contains("SMELTR_WORKER_ENABLED"), "stderr: {stderr}");
}

/// A worker with no leader of its own has nowhere to check in, so the URL is
/// required rather than guessed at.
#[test]
fn a_worker_without_a_leader_url_refuses_to_start() {
    let dir = temp_dir("no-leader-url");
    let (code, stderr) = run_smeltr(&dir, &[("SMELTR_WORKER_ENABLED", "1"), ("SMELTR_WORKER_GROUP", "g1")]);
    assert_eq!(code, 1, "stderr: {stderr}");
    assert!(stderr.contains("needs one to report to"), "stderr: {stderr}");
    assert!(stderr.contains("SMELTR_LEADER_URL"), "stderr: {stderr}");
}

#[test]
fn needs_an_enrollment_code_is_exactly_the_absence_of_a_credential() {
    let dir = temp_dir("predicate");
    assert!(
        smeltr::worker::needs_an_enrollment_code(&dir),
        "an empty state dir has never enrolled"
    );

    std::fs::write(dir.join("credential.token"), "a-persisted-token").unwrap();
    assert!(
        !smeltr::worker::needs_an_enrollment_code(&dir),
        "a persisted credential is what authenticates a worker from then on"
    );

    // A blank file is what a truncated write leaves behind, not a credential.
    std::fs::write(dir.join("credential.token"), "   \n").unwrap();
    assert!(
        smeltr::worker::needs_an_enrollment_code(&dir),
        "a blank credential file must not count as enrolled"
    );
}
