//! How the transform's work scales with threads on this host, nothing else running: `BENCH_THREADS` (1,2,4 when
//! unset) copies of the `json_app` reference pipeline, each on a thread of its own between a generator and a counter,
//! side by side. A host whose hyperthreads help moves more rows at every hardware thread than at every core; one
//! whose siblings fight moves fewer.

mod panos;
mod reference;

use std::time::Duration;

/// Records each copy moves: enough that a copy runs for seconds, so the copies' unequal starts barely count.
const ROWS: u64 = 2_000_000;

#[test]
#[ignore = "a measurement, not an assertion: `just thread-scaling`"]
fn thread_scaling() {
    let counts: Vec<usize> = std::env::var("BENCH_THREADS").map_or(vec![1, 2, 4], |t| {
        t.split(',')
            .map(|n| n.trim().parse().expect("BENCH_THREADS is a list of numbers"))
            .collect()
    });
    let p = &reference::json_app::PIPELINE;
    println!(
        "\n{} hardware threads\n\n| threads | rows/s | per thread | slowest copy |\n|---:|---:|---:|---:|",
        std::thread::available_parallelism().map_or(0, |n| n.get())
    );
    for threads in counts {
        let copies: Vec<_> = (0..threads)
            .map(|_| std::thread::spawn(move || reference::run(&(p.bundle)(), p.line, ROWS, 4_000, 0).0))
            .collect();
        let runs: Vec<reference::Run> = copies.into_iter().map(|c| c.join().expect("a copy ran")).collect();
        let rows: u64 = runs.iter().map(|r| r.rows).sum();
        let slowest = runs.iter().map(|r| r.wall).max().unwrap_or(Duration::ZERO);
        let rate = rows as f64 / slowest.as_secs_f64();
        println!(
            "| {threads} | {rate:.0} | {:.0} | {:.2}s |",
            rate / threads as f64,
            slowest.as_secs_f64()
        );
    }
}
