//! What a worker holds for its leader, and what it lets go of.
//!
//! One test, and a binary of its own: `start` is the process's and there is one of it.

use smeltr::logs;

/// Only this test's own events. `start` installs the `tracing` subscriber and the `log` bridge, and anything a
/// dependency raises while that happens is queued alongside -- asserting on the queue's total length made this
/// test depend on other people's silence.
fn mine() -> Vec<u64> {
    logs::pending_map(1024, |event| (event.site.to_string(), event.seq))
        .into_iter()
        .filter(|(site, _)| site == "delivery.test")
        .map(|(_, seq)| seq)
        .collect()
}

#[test]
fn a_worker_keeps_what_the_leader_has_not_acknowledged() {
    logs::start();
    // Only a worker queues -- `logs-leader-local` is where that is tested, because it turns on this process never
    // naming one, and a name is set once.
    logs::set_worker("w-1");

    for row in 0..5u64 {
        tracing::error!(target: "delivery.test", subject = "d1", row, "a destination refused a row");
    }
    // The drain thread is what queues them, so wait for it rather than race it.
    let began = std::time::Instant::now();
    while mine().len() < 5 {
        assert!(
            began.elapsed() < std::time::Duration::from_secs(5),
            "the drain thread queued them"
        );
        std::thread::yield_now();
    }

    // A peek leaves them where they are: a heartbeat that is never answered must cost nothing.
    let seen = mine();
    assert_eq!(mine(), seen, "peeking again finds the same five still waiting");
    assert!(
        seen.windows(2).all(|w| w[0] < w[1]),
        "oldest first, so the start of an outage goes first: {seen:?}"
    );

    // Acknowledged: only now are they gone, and only the ones that landed.
    let before = logs::pending_len();
    logs::delivered(before - 2);
    let left = mine();
    assert_eq!(left.len(), 2, "the two the leader has not seen are still held");
    assert_eq!(left, seen[3..], "and they are the two most recent, not any two");

    logs::delivered(logs::pending_len() + 99);
    assert_eq!(
        logs::pending_len(),
        0,
        "acknowledging more than is held is not an error"
    );
    assert_eq!(logs::undelivered(), 0, "nothing was refused for want of room");
}
