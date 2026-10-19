//! What a call site puts in a field is what reaches whatever reads it.
//!
//! `Collect::record_debug` is the one funnel every field value passes through (`logs::event`): `%` arrives there
//! through `format_args!`, whose `Debug` is its `Display`, and `?` through the real `Debug`. By then the two are
//! indistinguishable, so a value spelled wrong at the call site is spelled wrong everywhere it goes.
//!
//! Read off the console, which is this tier's only consumer. A worker says its group before it has a leader to
//! reach, so no fixture beyond the binary is needed.

use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

/// A worker's first line, whatever else the process goes on to do about the leader it cannot reach.
#[test]
fn a_text_field_carries_the_text_and_not_rusts_rendering_of_it() {
    let dir = std::env::temp_dir().join(format!(
        "smeltr-logs-fields-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();

    let mut child = Command::new(env!("CARGO_BIN_EXE_smeltr"))
        .env("SMELTR_WORKER_ENABLED", "1")
        // Never answers, which is all this needs: the line under test is written on the way up.
        .env("SMELTR_LEADER_URL", "https://127.0.0.1:1")
        .env("SMELTR_WORKER_GROUP", "e2e")
        // Never spent: enrollment cannot reach the leader above. Present, and well formed, because a worker with
        // no credential and no enrollment code refuses to start before it says anything at all -- and so does one
        // whose enrollment code it cannot read.
        .env(
            "SMELTR_WORKER_ENROLLMENT_CODE",
            "smeltrenroll-1-system-unusedbythistest",
        )
        .env("SMELTR_WORKER_STATE_DIR", dir.to_str().unwrap())
        // Isolated from any `smeltr.toml` in the cwd this test inherited, as `tests/worker-tests.rs` explains.
        .current_dir(&dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn smeltr in its worker role");

    let stderr = BufReader::new(child.stderr.take().expect("piped"));
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut starting = None;
    for line in stderr.lines().map_while(Result::ok) {
        if line.contains("worker.starting") {
            starting = Some(line);
            break;
        }
        if Instant::now() > deadline {
            break;
        }
    }
    let _ = child.kill();
    let _ = child.wait();
    let _ = std::fs::remove_dir_all(&dir);

    let line = starting.expect("the worker says it is starting before it needs a leader");
    assert!(
        line.contains("group=e2e"),
        "a group is text, and `?` would store the quote characters with it -- \
         after which a search for `group:e2e` finds nothing: {line}"
    );
}
