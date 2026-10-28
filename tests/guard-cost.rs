//! What a guarded block costs by how many of a batch's rows its guard takes, over batches where 0%, 1%, 50% and 100%
//! of the rows take it: writes that hash and rewrite `_raw`, and a loop and a parse the block computes ahead of its
//! writes. Nothing a guard takes no row of is computed, so such a guard costs its condition and nothing of its body;
//! one that takes any row costs the whole body.
//! `cargo test --profile dist --test guard-cost -- --ignored --nocapture`.

// The harness mints certificates for the fuzzers it serves; this binary only sizes guards.
#[allow(dead_code)]
mod certs;
mod harness;

use std::time::{Duration, Instant};

use harness::{code, compile_ok, raw_batch};
use smeltr::core::bits;
use smeltr::core::buffer::BufferPool;
use smeltr::core::column::{Column, DataType};
use smeltr::core::ctx::Ctx;
use smeltr::core::error::ErrorSink;
use smeltr::plan::compile::Plan;
use smeltr::plan::exec::run_scoped;

const ROWS: usize = 4_096;
const BATCHES: usize = 64;
const RUNS: usize = 7;

/// `ROWS` lines, the first `percent` of every hundred starting `ERROR`, which the guard takes.
fn lines(percent: usize) -> Vec<String> {
    (0..ROWS)
        .map(|i| {
            let level = if i % 100 < percent { "ERROR" } else { "INFO" };
            format!(
                "{level} 2026-10-11T13:{:02}:{:02}Z host-{} request {} took {} ms",
                i % 60,
                i % 59,
                i % 32,
                i,
                i % 997
            )
        })
        .collect()
}

/// The median time to run `plan` over `BATCHES` batches of `lines`, of `RUNS` runs after one untimed one.
fn time(plan: &Plan, lines: &[String]) -> Duration {
    let texts: Vec<&str> = lines.iter().map(String::as_str).collect();
    let mut pool = BufferPool::new();
    let mut runs: Vec<Duration> = (0..=RUNS)
        .map(|_| {
            let mut spent = Duration::ZERO;
            for _ in 0..BATCHES {
                let mut errors = ErrorSink::new();
                let mut batch = raw_batch(plan, &texts, &mut pool);
                let mut live = Column::alloc(&mut pool, &DataType::Bool, ROWS);
                bits::fill_valid(live.bool_mut().unwrap().values, ROWS);
                let started = Instant::now();
                {
                    let mut ctx = Ctx::new(&mut pool, &mut errors);
                    run_scoped(plan, &mut batch, live, &mut ctx, None);
                }
                spent += started.elapsed();
                batch.reclaim(&mut pool);
            }
            spent
        })
        .skip(1)
        .collect();
    runs.sort();
    runs[RUNS / 2]
}

/// Times `body` at 0%, 1%, 50% and 100% taken, prints each, and holds 0% to under a quarter of 100%.
fn measure(what: &str, body: &str) {
    let plan = compile_ok(vec![code(0, body)]);
    println!("{what}: {ROWS} rows a batch, {BATCHES} batches, median of {RUNS} runs");
    let taken: Vec<Duration> = [0, 1, 50, 100]
        .into_iter()
        .map(|percent| {
            let t = time(&plan, &lines(percent));
            let per_row = t.as_nanos() as f64 / (ROWS * BATCHES) as f64;
            println!(
                "  {percent:>3}% taken  {:>8.2} ms  {per_row:>7.1} ns/row",
                t.as_secs_f64() * 1e3
            );
            t
        })
        .collect();
    let (none, all) = (taken[0], taken[3]);
    assert!(
        none * 4 < all,
        "{what}: a guard that takes no row costs {none:?}, at least a quarter of the {all:?} one that takes every row"
    );
}

#[test]
#[ignore = "a measurement: `cargo test --profile dist --test guard-cost -- --ignored --nocapture`"]
fn a_guard_that_takes_no_row_skips_its_body() {
    measure(
        "guarded writes",
        r#"out = ""; digest = "";
           if (startsWith(_raw, "ERROR")) {
             out = toUpperCase(replaceAll(_raw, /[0-9]+/, "N"));
             digest = sha256(_raw);
           }"#,
    );
}

/// What a guarded block computes ahead of its writes: a loop's array, and a parser's input and the parse.
#[test]
#[ignore = "a measurement: `cargo test --profile dist --test guard-cost -- --ignored --nocapture`"]
fn a_guard_that_takes_no_row_skips_what_its_body_computes_first() {
    measure(
        "a guarded loop and parse",
        r#"words = 0; took = "";
           if (startsWith(_raw, "ERROR")) {
             for (let w of split(toUpperCase(_raw), " ")) { words = words + 1; }
             let kvs = kv(replaceAll(_raw, /[0-9]+/, "N"), {pairSep: " ", kvSep: "="});
             took = (kvs.took ?? "") as str;
           }"#,
    );
}
