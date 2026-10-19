//! A log raised while delivering logs goes to the console and no further.
//!
//! Its own binary: the property is about what this process queues, and the queue is the process's.

#[test]
fn what_delivery_itself_says_does_not_join_the_queue_it_was_emptying() {
    smeltr::logs::start();
    smeltr::logs::set_worker("w-1");

    // What a leader's ingest does when the store refuses a batch: it reports, and that report must not become
    // part of the next batch -- which would fail, and report, one event per beat for the life of the process.
    smeltr::logs::while_delivering(|| {
        tracing::warn!(target: "guard.test.local", "the store would not take a batch");
    });
    tracing::warn!(target: "guard.test.queued", "something else entirely");

    let began = std::time::Instant::now();
    let queued = loop {
        let sites = smeltr::logs::pending_map(1024, |event| event.site.to_string());
        if sites.iter().any(|s| s == "guard.test.queued") {
            break sites;
        }
        assert!(
            began.elapsed() < std::time::Duration::from_secs(5),
            "the drain thread ran"
        );
        std::thread::yield_now();
    };

    assert!(
        !queued.iter().any(|s| s == "guard.test.local"),
        "what delivery said stayed on the console: {queued:?}"
    );
    assert!(
        smeltr::logs::written_by_site()
            .iter()
            .any(|(site, n)| site == "guard.test.local" && *n == 1),
        "and it did reach the console -- console-only is not silence"
    );
}
