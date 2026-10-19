//! What a log costs the thread that raises one.
//!
//! `tracing::error!` sits on reader, transform and egress threads, so the question is what a call site pays
//! before its thread carries on. Unlike the measurement this replaces, the field values and the message are built
//! inside the timed loop, because that is what a real site pays: the earlier figures timed the handoff alone and
//! were quoted as though they were the whole cost.
//!
//! Ignored by default; `just measure` runs it in release.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

/// `ops` events per thread, returning nanoseconds per op for the slowest thread.
fn hammer(threads: usize, ops: usize) -> f64 {
    let go = Arc::new(AtomicBool::new(false));
    let handles: Vec<_> = (0..threads)
        .map(|t| {
            let go = go.clone();
            std::thread::spawn(move || {
                let subject = format!("destination-{t}");
                while !go.load(Ordering::Acquire) {
                    std::hint::spin_loop();
                }
                let began = Instant::now();
                for row in 0..ops as u64 {
                    tracing::error!(
                        target: "bench.site",
                        subject = %subject,
                        row,
                        reason = "schema mismatch on field ts",
                        "a destination refused a row and always will"
                    );
                }
                began.elapsed().as_nanos() as f64 / ops as f64
            })
        })
        .collect();

    go.store(true, Ordering::Release);
    handles.into_iter().map(|h| h.join().unwrap()).fold(0.0, f64::max)
}

/// What a site below the threshold costs. `tracing` asks the layer once per callsite and caches the answer, so
/// the field values and the message are never built -- the property that makes a `debug!` on a hot path free
/// rather than merely quiet.
fn hammer_filtered(ops: usize) -> f64 {
    let began = Instant::now();
    for row in 0..ops as u64 {
        tracing::trace!(
            target: "bench.filtered",
            subject = %format!("destination-{row}"),
            row,
            "a message nobody asked for"
        );
    }
    began.elapsed().as_nanos() as f64 / ops as f64
}

#[test]
#[ignore = "measurement, not a check"]
fn what_a_call_site_pays() {
    // Default threshold is `info`, so `error!` is carried and `trace!` is refused at the callsite.
    smeltr::logs::start();
    println!("\n  threshold: {}\n", smeltr::logs::max_level());

    for threads in [1, 2, 4] {
        let ns = hammer(threads, 200_000);
        println!(
            "  CARRIED  threads={threads}: {ns:.1} ns/op, dropped {}",
            smeltr::logs::dropped()
        );
    }
    println!("  REFUSED  threads=1: {:.1} ns/op", hammer_filtered(1_000_000));

    let mut written = smeltr::logs::written_by_site();
    written.sort();
    println!("\n  written by site: {written:?}\n");
}

/// What the drain thread gets out of the buffer, and how long a full one takes to empty.
///
/// These are the two numbers `CAPACITY` and `STOP_GRACE` rest on. A buffer buys time only against a drain that
/// is faster than the burst; an exit grace is long enough only if a full buffer reaches the console inside it.
#[test]
#[ignore = "measurement, not a check"]
fn what_the_drain_thread_keeps_up_with() {
    smeltr::logs::start();
    const SITE: &str = "bench.drain";
    const N: u64 = 100_000;

    // Deltas, not totals: the measurement above this one has already put six hundred thousand through.
    let written = || -> u64 {
        smeltr::logs::written_by_site()
            .into_iter()
            .filter(|(s, _)| s == SITE)
            .map(|(_, n)| n)
            .sum()
    };
    let (dropped0, unwritable0) = (smeltr::logs::dropped(), smeltr::logs::unwritable());
    let accounted = || written() + (smeltr::logs::dropped() - dropped0) + (smeltr::logs::unwritable() - unwritable0);

    // An unthrottled burst: what a buffer this size absorbs before it starts refusing. The rate here is far
    // past anything the call-site discipline produces -- one event per pass, not per row -- so what this shows
    // is the shape of the cliff, not a workload.
    let began = Instant::now();
    for row in 0..N {
        tracing::error!(
            target: "bench.drain",
            subject = "destination-0",
            row,
            reason = "schema mismatch on field ts",
            "a destination refused a row and always will"
        );
    }
    let offered = began.elapsed();
    while accounted() < N {
        std::hint::spin_loop();
    }
    println!(
        "\n  BURST    {N} offered in {:.0} ms ({:.0}/s); {} written, {} dropped for want of room",
        offered.as_secs_f64() * 1000.0,
        N as f64 / offered.as_secs_f64(),
        written(),
        smeltr::logs::dropped() - dropped0
    );

    // A full buffer, then the exit that has to empty it inside `STOP_GRACE`.
    for row in 0..smeltr::logs::CAPACITY as u64 {
        tracing::error!(target: "bench.drain", row, "filling the buffer before an exit");
    }
    let began = Instant::now();
    smeltr::logs::stop();
    println!(
        "  STOP     a full buffer ({}) emptied in {:.0} ms, grace is {:.0} ms\n",
        smeltr::logs::CAPACITY,
        began.elapsed().as_secs_f64() * 1000.0,
        smeltr::logs::STOP_GRACE.as_secs_f64() * 1000.0
    );
}
