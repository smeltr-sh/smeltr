//! Bootstrap, recovery and the data-directory lock against the built binary.
//!
//! Its own binary because every property here is about what a *process* does at startup: a second leader refused,
//! a password printed to a console rather than raised as an event, a flag that acts once per start. None of it is
//! observable from inside one process.

use std::io::BufRead;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// A directory no other test in this binary can name. A counter rather than the clock: these run in parallel and
/// share a pid, and two leaders in one data directory is the thing under test rather than an accident to have.
fn scratch(label: &str) -> std::path::PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let n = NEXT.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("smeltr-accounts-{label}-{}-{n}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// A port the OS says is free. Nothing connects to it; the leader needs one to bind.
///
/// Asked for per leader rather than carved out of a range per process: nextest runs each test in a process of
/// its own, side by side, and a range wide enough for one is one the next has already started walking into.
fn port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("the OS names a free port")
        .local_addr()
        .expect("the port it named")
        .port()
}

/// A leader, its stderr accumulating on a thread of its own. Killed when it drops, so a test that fails an
/// assertion does not leave one holding a lock.
struct Leader {
    child: Child,
    stderr: Arc<Mutex<String>>,
    /// What it was started with, to start it again on another port ([`Leader::port_was_taken`]).
    data_dir: std::path::PathBuf,
    extra: Vec<(String, String)>,
}

impl Drop for Leader {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Leader {
    /// Everything read from stderr so far.
    fn said(&self) -> String {
        self.stderr.lock().unwrap().clone()
    }

    /// The port [`port`] named was taken by another process between asking and binding, so this leader refused
    /// to start before touching its data directory. Started again on a fresh port when it was; the caller goes on
    /// waiting on the new one.
    fn port_was_taken(&mut self) -> bool {
        let taken = self.said().contains("listen address") && self.said().contains("already in use by another process");
        if taken {
            let extra: Vec<(&str, &str)> = self.extra.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
            *self = start(&self.data_dir, &extra);
        }
        taken
    }

    /// Wait for `marker` to appear on stderr, or panic with everything that did. Long enough to cover a cold
    /// start: the first run creates a git repository and hashes an Argon2 password.
    ///
    /// The listening line is logged just before the socket is bound, so it says the leader got that far rather
    /// than that it is reachable. Nothing here connects, so that is all these tests need.
    fn wait_for(&mut self, marker: &str) -> String {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
        loop {
            let said = self.said();
            if said.contains(marker) {
                return said;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "waited for {marker:?} and never saw it. stderr:\n{said}"
            );
            self.port_was_taken();
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
    }

    /// Wait for the process to exit and return `(exit code, stderr)`.
    fn wait_for_exit(mut self) -> (i32, String) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
        loop {
            match self.child.try_wait().unwrap() {
                Some(status) => {
                    // The reader thread may still be draining the last of the pipe.
                    std::thread::sleep(std::time::Duration::from_millis(100));
                    if std::time::Instant::now() < deadline && self.port_was_taken() {
                        continue;
                    }
                    return (status.code().unwrap_or(-1), self.said());
                }
                None => assert!(
                    std::time::Instant::now() < deadline,
                    "the leader was still running. stderr:\n{}",
                    self.said()
                ),
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
    }
}

/// Start a leader on `data_dir`. TLS off, so a test does not wait on a generated certificate for a socket it
/// never opens.
fn start(data_dir: &std::path::Path, extra: &[(&str, &str)]) -> Leader {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_smeltr"));
    cmd.current_dir(data_dir);
    for var in [
        "SMELTR_LEADER_ENABLED",
        "SMELTR_WORKER_ENABLED",
        "SMELTR_LEADER_URL",
        "SMELTR_CONFIG_FILE",
        "SMELTR_RECOVER_ADMIN",
    ] {
        cmd.env_remove(var);
    }
    cmd.env("SMELTR_LEADER_ENABLED", "1")
        .env("SMELTR_WORKER_ENABLED", "0")
        .env("SMELTR_LEADER_TLS", "off")
        .env("SMELTR_GUI_PORT", port().to_string())
        .env("SMELTR_LEADER_DATA_DIR", data_dir.join("leader").to_str().unwrap())
        .env(
            "SMELTR_LEADER_SECRET_KEY",
            data_dir.join("keys").join("sealing.key").to_str().unwrap(),
        );
    for (k, v) in extra {
        cmd.env(k, v);
    }

    let mut child = cmd
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn smeltr");
    let stderr = Arc::new(Mutex::new(String::new()));
    let sink = stderr.clone();
    let pipe = child.stderr.take().expect("stderr is piped");
    std::thread::spawn(move || {
        for line in std::io::BufReader::new(pipe).lines().map_while(Result::ok) {
            let mut held = sink.lock().unwrap();
            held.push_str(&line);
            held.push('\n');
        }
    });
    Leader {
        child,
        stderr,
        data_dir: data_dir.to_path_buf(),
        extra: extra.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
    }
}

/// The password on the `one-time password:` line.
fn password_in(said: &str) -> String {
    said.lines()
        .find_map(|line| line.split_once("one-time password:"))
        .unwrap_or_else(|| panic!("no password was printed. stderr:\n{said}"))
        .1
        .trim()
        .to_string()
}

/// A fresh install is usable without a separate step: the leader has an administrator before it serves anything.
#[test]
fn a_fresh_leader_creates_an_administrator_and_prints_its_password() {
    let dir = scratch("bootstrap");
    let mut leader = start(&dir, &[]);
    let said = leader.wait_for("one-time password:");

    assert!(said.contains("had no accounts"), "stderr:\n{said}");
    assert!(said.contains("username: admin"), "stderr:\n{said}");
    let password = password_in(&said);
    assert!(
        password.len() >= 15,
        "a generated password clears the floor: {password:?}"
    );
}

/// Bootstrap is for an empty store and nothing else. A leader that printed a new password on every start would
/// leave one in the scrollback of every restart.
#[test]
fn a_restart_creates_nobody_and_prints_nothing() {
    let dir = scratch("restart");
    let mut first = start(&dir, &[]);
    first.wait_for("one-time password:");
    drop(first);

    let mut second = start(&dir, &[]);
    second.wait_for("smeltr is listening");
    assert!(
        !second.said().contains("one-time password:"),
        "stderr:\n{}",
        second.said()
    );
}

/// The way back in. The password it prints is a new one, so knowing the old one is worth nothing.
#[test]
fn recovery_prints_a_new_password_for_the_same_account() {
    let dir = scratch("recover");
    let mut first = start(&dir, &[]);
    let original = password_in(&first.wait_for("one-time password:"));
    drop(first);

    let mut second = start(&dir, &[("SMELTR_RECOVER_ADMIN", "1")]);
    let said = second.wait_for("one-time password:");
    assert!(said.contains("SMELTR_RECOVER_ADMIN reset"), "stderr:\n{said}");
    assert!(said.contains("username: admin"), "stderr:\n{said}");
    assert_ne!(password_in(&said), original);
}

/// Left set, it fires on every restart. The symptom is a password that stops working after a reboot, so the
/// leader names the cause while the operator is looking at the console.
#[test]
fn recovery_says_to_unset_itself() {
    let dir = scratch("recover-warn");
    drop(start(&dir, &[]).wait_for("one-time password:"));

    let mut second = start(&dir, &[("SMELTR_RECOVER_ADMIN", "1")]);
    let said = second.wait_for("unset SMELTR_RECOVER_ADMIN");
    assert!(said.contains("before the next restart"), "stderr:\n{said}");
}

/// Two leaders on one directory race on a database and a git repository and damage both. The second is refused
/// while the first still holds it.
#[test]
fn a_second_leader_on_one_data_directory_is_refused() {
    let dir = scratch("locked");
    let mut first = start(&dir, &[]);
    first.wait_for("smeltr is listening");

    let (code, said) = start(&dir, &[]).wait_for_exit();
    assert_eq!(code, 1, "stderr:\n{said}");
    assert!(said.contains("already using"), "stderr:\n{said}");
    assert!(!said.contains("panicked"), "stderr:\n{said}");
}

/// The lock is the operating system's, released when the process goes -- including when it is killed, which is
/// what a lock file holding a pid could not manage.
#[test]
fn the_directory_is_free_again_once_the_leader_stops() {
    let dir = scratch("unlocked");
    let mut first = start(&dir, &[]);
    first.wait_for("smeltr is listening");
    drop(first);

    let mut second = start(&dir, &[]);
    second.wait_for("smeltr is listening");
}

/// A config repository that does not read stops the leader at start with what is damaged, rather than letting it
/// serve or deploy from what still reads.
#[test]
fn a_damaged_config_repo_with_no_host_is_refused() {
    let dir = scratch("damaged-repo");
    let mut first = start(&dir, &[]);
    first.wait_for("smeltr is listening");
    drop(first);

    std::fs::write(dir.join("leader/repos/config.git/refs/heads/main"), b"").unwrap();
    let (code, said) = start(&dir, &[]).wait_for_exit();
    assert_eq!(code, 1, "stderr:\n{said}");
    assert!(said.contains("does not read back whole"), "stderr:\n{said}");
    assert!(said.contains("a copy you know is sound"), "stderr:\n{said}");
}
