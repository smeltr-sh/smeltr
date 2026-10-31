//! A process that is no worker, and whose leader does not keep its own logs, queues none.
//!
//! Its own binary because the property is about what this process never does: name a worker. A name is set once,
//! so a test that sets one cannot also test the case where none is.

#[test]
fn a_process_with_no_worker_queues_nothing_for_a_leader() {
    smeltr::logs::start();

    for row in 0..20u64 {
        tracing::error!(target: "leader.local.test", subject = "d1", row, "a leader said something");
    }

    // Wait for the drain thread to have written them, which is where queuing would happen.
    let began = std::time::Instant::now();
    while smeltr::logs::written_by_site()
        .iter()
        .all(|(site, n)| site != "leader.local.test" || *n < 20)
    {
        assert!(
            began.elapsed() < std::time::Duration::from_secs(5),
            "the drain thread wrote them"
        );
        std::thread::yield_now();
    }

    assert!(
        smeltr::logs::pending_len() == 0,
        "a leader has no heartbeat, so holding these would fill to the cap and then count a backlog nothing was \
         ever going to drain"
    );
    assert_eq!(
        smeltr::logs::undelivered(),
        0,
        "and nothing is counted as refused either"
    );
}
