//! A generated password never reaches the log pipeline.
//!
//! One log buffer serves a whole process, and a logs source reads it (`logs::tap`). A leader that also
//! runs a worker runs that worker's supervisor in its own process, and a capture of a logs source that supervisor
//! serves reads the leader's own events, so an event raised while bootstrapping could reach a sample. A one-time
//! password goes to the console and nowhere else.
//!
//! Its own binary because the buffer is process-wide: a test that starts it cannot share a process with tests that
//! do not expect their events carried.

use smeltr::leader::accounts;
use smeltr::leader::sqlite_store::SqliteStore;
use smeltr::leader::store::Store;

#[test]
fn bootstrapping_and_recovering_raise_no_event_carrying_the_password() {
    smeltr::logs::start();
    let tap = smeltr::logs::tap();

    let dir = std::env::temp_dir().join(format!("smeltr-logs-password-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let store = SqliteStore::open(std::path::Path::new(":memory:"), &dir.join("sealing.key")).unwrap();

    let bootstrapped = accounts::bootstrap_if_empty(&store)
        .unwrap()
        .expect("an empty store bootstraps");
    let recovered = accounts::recover_administrator(&store)
        .unwrap()
        .expect("the account is there to recover");

    // Everything the process has said since the tap opened, drained the way a pipeline's reader drains it.
    let said = drained(&tap);
    for password in [&bootstrapped.password, &recovered.password] {
        assert!(
            !said.contains(password.as_str()),
            "a one-time password reached the log pipeline:\n{said}"
        );
    }
    // The audit rows are written, and they carry the account rather than its password.
    let actions: Vec<String> = store
        .list_security_events(10)
        .unwrap()
        .into_iter()
        .map(|e| e.action)
        .collect();
    assert!(actions.contains(&"bootstrap".to_string()), "{actions:?}");
    assert!(actions.contains(&"recover".to_string()), "{actions:?}");
    let rendered = format!("{:?}", store.list_security_events(10).unwrap());
    assert!(
        !rendered.contains(&bootstrapped.password),
        "an audit row carried a password"
    );
}

/// Everything in the queue, rendered as a reader would see it. Drained until it stops yielding, because the drain
/// thread hands events over on its own schedule.
fn drained(tap: &smeltr::logs::Tap) -> String {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    let mut said = String::new();
    while std::time::Instant::now() < deadline {
        for event in tap.drain(1024) {
            said.push_str(&format!("{event:?}\n"));
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    said
}
