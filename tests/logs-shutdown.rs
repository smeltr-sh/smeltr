//! The spine, end to end: a `tracing` call site reaches the console consumer, and an orderly exit writes what the
//! buffer still holds.
//!
//! One test, and a binary of its own: `start` and `stop` are the process's, and there is one of each.

#[test]
fn what_was_said_before_the_exit_is_written_by_it() {
    smeltr::logs::start();

    // Through `tracing`, not through any smeltr API: the call sites this replaces are ordinary `tracing` macros,
    // and the point of the spine is that nothing else is needed to reach the buffer.
    for row in 0..3u64 {
        tracing::error!(target: "shutdown.test", subject = "d1", row, "a destination refused a row");
    }

    let began = std::time::Instant::now();
    smeltr::logs::stop();

    let written = smeltr::logs::written_by_site();
    let mine = written.iter().find(|(site, _)| site == "shutdown.test");
    assert_eq!(
        mine.map(|(_, n)| *n),
        Some(3),
        "every event reached the drain thread and was written: {written:?}"
    );
    assert_eq!(smeltr::logs::dropped(), 0, "and none was dropped for want of room");
    assert_eq!(smeltr::logs::unwritable(), 0, "and the console took all of them");

    // The property the handshake exists for. Without it `stop` would still return, one whole grace later, on every
    // ctrl-c and every leader exit -- and every assertion above would still pass.
    assert!(
        began.elapsed() < smeltr::logs::STOP_GRACE,
        "stop returned on the drain thread's word rather than on the grace expiring"
    );

    // An exit path that stops twice, or one whose buffer was already empty, still exits.
    smeltr::logs::stop();
}
