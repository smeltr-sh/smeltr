//! What a real normalization pipeline costs, in the unit an operator
//! sizes with. PAN-OS TRAFFIC syslog through Grok, a positional CSV
//! parse, three enrichment lookups and an ECS field mapping, driven
//! through `Worker::tick` with a generating source and a counting
//! destination.
//!
//! No broker: the workload is the pipeline, and a Kafka hop on both ends
//! would put the broker's throughput in the number. `tests/tick-profile`
//! measures that hop separately. What is left here is the CPU cost of the
//! transform, which is the thing the sizing claim is about.
//!
//! # What this does not measure
//!
//! `lookup::run` and `grok_parse` loop over rows inside their kernels. So
//! this workload does not exercise vectorization, and a result from it
//! must not be attributed to columnar batching. What it measures is the
//! absence of a per-event interpreter, a per-event allocation, a garbage
//! collector, and hash-map field access.
//!
//! The profiles are `#[ignore]`d: they time a thread against a whole
//! machine. `just measure` runs them in release.

use std::sync::Arc;

mod panos;
mod reference;
use panos::Stage;
use reference::{Counter, GB, Run};

/// One drive of `rows` records through the pipeline at `stage`.
fn run(stage: Stage, rows: u64, max_rows: usize, keep: usize) -> (Run, Counter) {
    reference::run(&panos::bundle(stage), panos::line, rows, max_rows, keep)
}

/// The fastest of `reps` runs. A throughput floor is set by whatever else
/// the machine did during the run, so the best run is the one least
/// polluted by it -- and without this the per-stage differences are
/// smaller than the noise between runs.
fn best_of(reps: usize, stage: Stage, rows: u64, max_rows: usize) -> Run {
    (0..reps)
        .map(|_| run(stage, rows, max_rows, 0).0)
        .max_by(|a, b| a.rows_per_sec().total_cmp(&b.rows_per_sec()))
        .expect("at least one rep")
}

/// The box, so a number is comparable to someone else's.
fn box_line() -> String {
    let model = std::fs::read_to_string("/proc/cpuinfo")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("model name"))
                .and_then(|l| l.split_once(':'))
                .map(|(_, v)| v.trim().to_string())
        })
        .unwrap_or_else(|| "unknown".into());
    let cores = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(0);
    let profile = if cfg!(debug_assertions) {
        "DEBUG -- not a measurement"
    } else {
        "release"
    };
    format!("{model}, {cores} logical cores, {profile}")
}

/// The mapping is right. A benchmark over a pipeline that produces the
/// wrong document measures nothing, so this runs with the ordinary tests.
#[test]
fn the_workload_maps_panos_to_ecs() {
    assert_eq!(
        panos::line(0).split(',').count(),
        panos::HEADERS.len(),
        "a generated record must have exactly one column per header, or every column after the gap is misparsed"
    );

    let (r, sink) = run(Stage::Ecs, 512, 256, 4);
    assert_eq!(r.docs, r.rows, "every row should reach the destination");

    let docs = sink.kept();
    assert!(!docs.is_empty(), "kept documents");
    let v: serde_json::Value = serde_json::from_slice(&docs[0]).expect("the destination gets JSON");

    // Grok, then CSV, then the rename: three stages have to have worked
    // for a dotted ECS name to hold a vendor column's value.
    let src = v.pointer("/source/ip").and_then(|x| x.as_str()).expect("source.ip");
    assert!(src.starts_with("10."), "source.ip came from the vendor column: {src}");
    assert!(
        v.pointer("/destination/ip").and_then(|x| x.as_str()).is_some(),
        "destination.ip"
    );

    // Typed by `eval`, not carried as the string the parser produced.
    assert!(
        v.pointer("/source/port").and_then(|x| x.as_i64()).is_some(),
        "source.port"
    );
    assert!(
        v.pointer("/network/bytes").and_then(|x| x.as_i64()).is_some(),
        "network.bytes"
    );

    // The three lookups hit.
    assert!(
        v.pointer("/source/site").and_then(|x| x.as_str()).is_some(),
        "cidr lookup"
    );
    assert!(
        v.pointer("/user/department").and_then(|x| x.as_str()).is_some(),
        "exact lookup on the user"
    );
    assert!(
        v.pointer("/panw/app_category").and_then(|x| x.as_str()).is_some(),
        "exact lookup on the application"
    );

    // Constants and derived fields.
    assert_eq!(
        v.pointer("/event/dataset").and_then(|x| x.as_str()),
        Some("panw.traffic")
    );
    assert!(v.get("@timestamp").is_some(), "@timestamp parsed");

    // The intermediates are gone: the document is ECS, not ECS plus the
    // vendor's 56 columns.
    for gone in ["_raw", "panos", "source_address", "bytes_sent", "syslog_time"] {
        assert!(
            v.get(gone).is_none(),
            "`{gone}` should have been nulled out of the document"
        );
    }
}

/// Where the time goes, stage by stage. Each row adds its own work to the
/// row above, so consecutive rows subtract to a per-stage cost.
#[test]
#[ignore = "a measurement, not an assertion: `just measure`"]
fn cost_by_stage() {
    const ROWS: u64 = 200_000;
    const BATCH: usize = 5_000;
    const REPS: usize = 5;

    println!("\nPAN-OS TRAFFIC -> ECS, {ROWS} records, batch {BATCH}, best of {REPS}");
    println!("{}", box_line());
    println!();
    println!(
        "{:>14}  {:>10}  {:>9}  {:>8}  {:>10}  {:>12}  {:>12}",
        "stage", "rows/s", "us/row", "step", "out B/row", "GB/day/vCPU", "in+out"
    );
    let mut prev = 0.0f64;
    for stage in Stage::ALL {
        let r = best_of(REPS, stage, ROWS, BATCH);
        let us_row = r.process as f64 / r.rows as f64;
        println!(
            "{:>14}  {:>10.0}  {:>9.2}  {:>8}  {:>10.0}  {:>12.0}  {:>12.0}",
            stage.label(),
            r.rows_per_sec(),
            us_row,
            if stage == Stage::Passthrough {
                "--".to_string()
            } else {
                format!("{:+.2}", us_row - prev)
            },
            r.bytes_out as f64 / r.docs as f64,
            r.gb_day_in(1),
            r.gb_day_both(1),
        );
        prev = us_row;
    }
    println!();
    println!("us/row is `smeltr_tick_process_seconds_total`: decode, route, pipeline, serialize.");
    println!("`step` is the change from the row above. It can be negative: every non-null field");
    println!("is serialized, so a stage that prunes fields buys back more than it spends.");
    println!("GB/day/vCPU is one thread: ingest bytes at this rate for 24h, GB = 10^9 bytes.");
    println!("`in+out` counts the serialized documents too, which is how vendor sizing is quoted.");
}

/// The per-core figure at saturation. One thread on an idle box is the
/// best case a core ever has; the honest number is what the Nth core
/// still adds.
#[test]
#[ignore = "a measurement, not an assertion: `just measure`"]
fn thread_scaling() {
    const ROWS: u64 = 200_000;
    const BATCH: usize = 5_000;
    let cores = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1);

    println!("\nPAN-OS TRAFFIC -> ECS, full pipeline, {ROWS} records per thread");
    println!("{}", box_line());
    println!();
    println!(
        "{:>8}  {:>12}  {:>12}  {:>12}  {:>12}  {:>9}",
        "threads", "aggregate", "per thread", "GB/day/vCPU", "in+out", "scaling"
    );

    let mut baseline = 0.0f64;
    let mut counts: Vec<usize> = vec![1, 2, 4, 8].into_iter().filter(|n| *n <= cores.max(1)).collect();
    if !counts.contains(&cores) {
        counts.push(cores);
    }
    for threads in counts {
        let gate = Arc::new(std::sync::Barrier::new(threads));
        let handles: Vec<_> = (0..threads)
            .map(|_| {
                let gate = gate.clone();
                std::thread::spawn(move || {
                    gate.wait();
                    run(Stage::Ecs, ROWS, BATCH, 0).0
                })
            })
            .collect();
        let runs: Vec<Run> = handles.into_iter().map(|h| h.join().unwrap()).collect();

        let rows: u64 = runs.iter().map(|r| r.rows).sum();
        let bytes_in: u64 = runs.iter().map(|r| r.bytes_in).sum();
        let bytes_out: u64 = runs.iter().map(|r| r.bytes_out).sum();
        let wall = runs.iter().map(|r| r.wall).max().unwrap();
        let aggregate = rows as f64 / wall.as_secs_f64();
        if threads == 1 {
            baseline = aggregate;
        }
        let per_core = |b: u64| b as f64 / wall.as_secs_f64() * 86_400.0 / GB / threads as f64;
        println!(
            "{:>8}  {:>6.0} r/s  {:>6.0} r/s  {:>12.0}  {:>12.0}  {:>8.2}x",
            threads,
            aggregate,
            aggregate / threads as f64,
            per_core(bytes_in),
            per_core(bytes_in + bytes_out),
            aggregate / baseline,
        );
    }
    println!();
    println!("(scaling is against the 1-thread aggregate; linear would be 1/2/4/8x)");
    println!("the per-vCPU figure to quote is the one at the highest thread count, not at one.");
}

/// Prints one line in and one document out, so the workload can be read
/// rather than trusted.
#[test]
#[ignore = "prints the workload: `cargo test --test panos-ecs-profile -- --ignored show_one`"]
fn show_one() {
    println!("\nin:\n{}", panos::line(0));
    let (_, sink) = run(Stage::Ecs, 16, 8, 1);
    let v: serde_json::Value = serde_json::from_slice(&sink.kept()[0]).unwrap();
    println!("\nout:\n{}", serde_json::to_string_pretty(&v).unwrap());
}
