//! What the shared memory budget's accounting costs: one counter every
//! reader thread touches, so a shared cache line under contention.
//!
//! Two paths. Recycling: a buffer the pool already holds is already
//! charged, so `take` never consults the budget. Charging: a new
//! allocation pays the atomics, and this is where contention lives.
//!
//! What keeps that off the hot path is the columnar layout: a `take`
//! covers a whole column, so a batch makes tens of them, not thousands. A
//! `take` reached per row or per array element would change the answer,
//! and then sharded or thread-local reservations would be worth having.
//!
//! Ignored by default; `just measure` runs it in release.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use smeltr::core::buffer::{BufferPool, MemoryBudget};

/// `ops` take/give pairs per thread, returning nanoseconds per op for the
/// slowest thread.
///
/// `fresh` builds a new pool each iteration so nothing is recycled: every
/// take is a real charge and every drop a real release. Without it the
/// pool recycles and the budget is never consulted at all.
fn hammer(threads: usize, budget: Option<MemoryBudget>, fresh: bool, ops: usize) -> f64 {
    let start = Arc::new(AtomicBool::new(false));
    let handles: Vec<_> = (0..threads)
        .map(|_| {
            let start = start.clone();
            let budget = budget.clone();
            std::thread::spawn(move || {
                let make = || match &budget {
                    Some(b) => BufferPool::with_budget(b.clone()),
                    None => BufferPool::new(),
                };
                let mut pool = make();
                // Warm the bucket so the recycling variant never allocates.
                let warm: Vec<_> = (0..8).map(|_| pool.take(4096)).collect();
                for b in warm {
                    pool.give(b);
                }
                // Every thread starts together, or the early ones measure
                // an uncontended line and the whole point is lost.
                while !start.load(Ordering::Relaxed) {
                    std::hint::spin_loop();
                }
                let t = Instant::now();
                for _ in 0..ops {
                    if fresh {
                        let mut p = make();
                        std::hint::black_box(p.take(4096));
                    } else {
                        let b = pool.take(4096);
                        pool.give(b);
                    }
                }
                t.elapsed()
            })
        })
        .collect();
    std::thread::sleep(Duration::from_millis(50));
    start.store(true, Ordering::Relaxed);
    handles.into_iter().map(|h| h.join().unwrap()).max().unwrap().as_nanos() as f64 / ops as f64
}

/// Best of three, alternating: a single pass moved the *unbudgeted*
/// baseline by 4x between runs on a shared 4-core box, which is enough
/// noise to invent a regression out of nothing.
fn paired(threads: usize, fresh: bool, ops: usize) -> (f64, f64) {
    let (mut with, mut without) = (f64::MAX, f64::MAX);
    for _ in 0..3 {
        with = with.min(hammer(threads, Some(MemoryBudget::new(usize::MAX)), fresh, ops));
        without = without.min(hammer(threads, None, fresh, ops));
    }
    (with, without)
}

/// The path the pipeline mostly takes. A recycled buffer is already
/// charged, so the budget is not consulted and the two variants are the
/// same code.
#[test]
#[ignore = "a measurement, not a gate -- `just measure`"]
fn recycling_never_consults_the_budget() {
    for threads in [1usize, 2, 4] {
        let (with, without) = paired(threads, false, 2_000_000);
        println!("RECYCLE threads={threads}: budgeted {with:.1} ns/op, unbudgeted {without:.1} ns/op");
    }
}

/// The charging path, paired against the identical allocation pattern
/// with no budget, so the delta is the atomics rather than malloc.
#[test]
#[ignore = "a measurement, not a gate -- `just measure`"]
fn charging_costs_this_much_per_allocation() {
    for threads in [1usize, 2, 4] {
        let (with, without) = paired(threads, true, 500_000);
        println!(
            "CHARGE threads={threads}: budgeted {with:.1} ns/op, unbudgeted {without:.1} ns/op, \
             delta {:.1} ns",
            with - without
        );
    }
}
