//! Where a batch's wall clock goes: polling the broker, running the
//! pipeline, handing the result to the destination. Drives a real
//! `runtime::Worker` over a real Kafka source and destination through
//! `Worker::tick`, and reads the `smeltr_tick_*` phase counters the
//! runtime records.
//!
//! Needs a broker (`KAFKA_TEST_BROKERS`) and skips otherwise. It prints a
//! profile and asserts only that work happened: the number wanted is a
//! ratio to reason about, not a bound to defend. `just measure` runs it
//! in release.
//!
//! `tick` is the one-thread driver. A deployed worker runs the reader and
//! transform on separate threads over the same `step`s, so what this
//! measures is each stage's cost, not the pair's overlap.

use std::sync::Arc;
use std::time::{Duration, Instant};

use smeltr::core::buffer::MemoryBudget;
use smeltr::io::kafka::{KafkaDestination, KafkaSource};
use smeltr::io::{Destination, OutgoingRecord, OwnedRecord};
use smeltr::metrics::{MetricValue, Metrics, MetricsHandle};
use smeltr::plan::topology::KafkaDestinationConfig;
use smeltr::runtime::{DestinationTarget, Worker};
use std::borrow::Cow;

/// A destination reports librdkafka's statistics through a handle; these tests do not read them.
fn kafka_metrics() -> smeltr::metrics::MetricsHandle {
    smeltr::metrics::MetricsHandle::new(std::sync::Arc::new(smeltr::metrics::Metrics::new()), "test")
}

mod live;

fn live_brokers(test_name: &str) -> Option<String> {
    live::kafka(test_name)
}

fn unique(p: &str) -> String {
    let n = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{p}-{n}")
}

/// A row worth running a pipeline over: nested JSON with fields the
/// pipeline below actually reads and rewrites.
fn sample_row(i: usize) -> String {
    format!(
        r#"{{"ts":"2026-08-30T12:00:{:02}Z","status":{},"bytes":{},"host":"web-{:02}","path":"/api/v{}/thing","ua":"Mozilla/5.0 (compatible; smeltr-profile)","src":{{"ip":"10.0.{}.{}","port":{}}}}}"#,
        i % 60,
        if i.is_multiple_of(7) { 500 } else { 200 },
        1000 + (i % 9000),
        i % 32,
        1 + (i % 3),
        i % 256,
        (i * 7) % 256,
        30000 + (i % 30000),
    )
}

fn produce(brokers: &str, topic: &str, n: usize) {
    produce_with(brokers, topic, n, |i| sample_row(i).into_bytes());
}

/// `sample_row` with one byte no UTF-8 decoder accepts, so the record has
/// no UTF-8 reading at all and only a declared (or guessed) single-byte
/// encoding can read it.
fn latin1_row(i: usize) -> Vec<u8> {
    let row = sample_row(i);
    let (head, tail) = row.split_once("web-").expect("sample rows carry a host");
    let mut out = Vec::with_capacity(row.len() + 4);
    out.extend_from_slice(head.as_bytes());
    // `é` as the single byte windows-1252 spells it with.
    out.extend_from_slice(b"caf\xE9-");
    out.extend_from_slice(tail.as_bytes());
    out
}

fn produce_with(brokers: &str, topic: &str, n: usize, row: impl Fn(usize) -> Vec<u8>) {
    let mut dest = KafkaDestination::new(
        &KafkaDestinationConfig {
            payload: smeltr::plan::config::Payload::Record,
            compression: smeltr::plan::topology::Compression::None,
            acks: smeltr::plan::topology::Acks::All,
            stall_after: std::time::Duration::from_secs(30),
            brokers: brokers.to_string(),
            default_topic: topic.to_string(),
            service: smeltr::plan::topology::KafkaService::Kafka,
            tls: None,
            buffer_bytes: 64 * 1024 * 1024,
            props: Default::default(),
            avro: None,
        },
        0,
        kafka_metrics(),
        &Default::default(),
    )
    .expect("producer");
    let rows: Vec<OwnedRecord> = (0..n)
        .map(|i| OwnedRecord::from(&OutgoingRecord::new(&row(i))))
        .collect();
    dest.deliver(0, Cow::Owned(rows)).expect("deliver");
    dest.flush().expect("flush");
}

/// A pipeline that parses and does a little work, so this measures more
/// than serialization.
fn bundle_json(brokers: &str, inputs: &[String], output: &str, group: &str, encoding: &str) -> String {
    serde_json::json!({
        "version": 1,
        "sources": [{
            "id": "in", "type": "kafka", "brokers": brokers,
            "topics": inputs, "groupId": group, "encoding": encoding,
        }],
        "destinations": [{
            "id": "out", "type": "kafka", "brokers": brokers, "defaultTopic": output,
        }],
        "pipelines": [{
            "id": "p", "name": "profile",
            "steps": [
                { "id": "parse", "type": "parser", "input": "_raw", "kind": {"type": "json"},
                  "declared": [
                    {"name": "status", "type": "i64"},
                    {"name": "bytes", "type": "i64"},
                    {"name": "host", "type": "str"}
                  ]},
                { "id": "derive", "type": "eval", "assignments": [
                    {"target": "kb", "expr": "bytes / 1024"},
                    {"target": "severity", "expr": "status >= 500 ? \"high\" : \"low\""}
                ]},
            ],
        }],
        "routes": [{
            "id": "r", "source": "in", "condition": null, "pipeline": "p",
            "deliver": [{"to": ["out"]}],
        }],
    })
    .to_string()
}

fn counter(samples: &[smeltr::metrics::MetricSample], name: &str) -> u64 {
    samples
        .iter()
        .filter(|s| s.key.name == name)
        .map(|s| match s.value {
            MetricValue::Counter(c) => c,
            // A time, in the microseconds this profile reports.
            MetricValue::FloatCounter(seconds) => (seconds * 1e6) as u64,
            MetricValue::Gauge(g) => g as u64,
        })
        .sum()
}

/// One drive of the whole input through `tick` at a given batch size.
struct Profile {
    rows: usize,
    batches: u64,
    wall: Duration,
    /// Time to join the consumer group, kept out of the steady state.
    join: Duration,
    poll: u64,
    process: u64,
    flush: u64,
}

fn run_once(brokers: &str, inputs: &[String], output: &str, rows: usize, max_rows: usize) -> Profile {
    decoding(brokers, inputs, output, rows, max_rows, "auto")
}

/// `run_once` with the source declaring `encoding`.
fn decoding(brokers: &str, inputs: &[String], output: &str, rows: usize, max_rows: usize, encoding: &str) -> Profile {
    let group = unique("profile-group");
    run_bundle(
        &bundle_json(brokers, inputs, output, &group, encoding),
        rows,
        max_rows,
        None,
    )
}

/// Where the members of one consumer group wait for each other between
/// warm-up and the timed run. A member that blocks stops polling, and a
/// group cannot rebalance until every member polls: a thread parked at a
/// `Barrier` holds back the join of the threads behind it. So a waiting
/// member keeps its membership instead.
struct Gate {
    members: usize,
    warmed: std::sync::atomic::AtomicUsize,
}

impl Gate {
    fn new(members: usize) -> Gate {
        Gate {
            members,
            warmed: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    fn wait(&self, worker: &mut Worker) {
        use std::sync::atomic::Ordering;
        self.warmed.fetch_add(1, Ordering::SeqCst);
        while self.warmed.load(Ordering::SeqCst) < self.members {
            // The timeout is how late a member can see the last one arrive, and so how far apart the timed runs
            // can start. librdkafka counts whole milliseconds: less than one does not block, and the loop spins.
            worker
                .reader
                .source
                .keep_membership(Duration::from_millis(1))
                .expect("keep membership");
        }
    }
}

/// `run_once` on `bundle` (`bundle_json`), optionally behind a gate
/// between warm-up and the timed run.
fn run_bundle(bundle: &str, rows: usize, max_rows: usize, gate: Option<&Gate>) -> Profile {
    let system = smeltr::config_file::load_str(bundle).expect("bundle loads");
    let topology = system.compile_topology().expect("bundle compiles");

    let registry = Arc::new(Metrics::new());
    let source = KafkaSource::new(
        match &system.topology.sources[0].kind {
            smeltr::plan::topology::SourceKind::Kafka(k) => k,
            _ => unreachable!(),
        },
        &Default::default(),
    )
    .expect("consumer");

    let destinations = system
        .topology
        .destinations
        .iter()
        .map(|d| DestinationTarget {
            id: d.id,
            name: d.name.clone(),
            destination: smeltr::io::build_destination(
                &d.kind,
                d.id,
                &d.name,
                smeltr::metrics::MetricsHandle::new(std::sync::Arc::new(smeltr::metrics::Metrics::new()), "profile"),
                &Default::default(),
                None,
            )
            .expect("destination"),
            on_reject: d.on_reject.clone(),
            wants_doc_id: d.kind.wants_doc_id(),
            placement: d.kind.placement(),
        })
        .collect();

    let mut worker = Worker::new(
        system.topology.sources[0].id,
        Box::new(source),
        topology,
        destinations,
        MemoryBudget::unlimited(),
        MetricsHandle::new(registry.clone(), "profile-0"),
    );

    // Warm up before the clock starts: a fresh `group.id` spends seconds
    // in the group join, which is paid once per worker start, not per row.
    let mut empty_in_a_row = 0;
    let join = Instant::now();
    loop {
        match worker.tick(max_rows, MAX_BYTES, Duration::from_millis(500)) {
            Ok(0) => {
                empty_in_a_row += 1;
                assert!(empty_in_a_row < 40, "never joined the group");
            }
            Ok(_) => break,
            Err(e) => panic!("tick failed during warmup: {e}"),
        }
    }
    let join = join.elapsed();
    if let Some(g) = gate {
        g.wait(&mut worker);
    }
    let base = registry.snapshot();
    let (b_poll, b_proc, b_flush, b_batches) = (
        counter(&base, "smeltr_tick_poll_seconds_total"),
        counter(&base, "smeltr_tick_process_seconds_total"),
        counter(&base, "smeltr_tick_flush_seconds_total"),
        counter(&base, "smeltr_tick_batches_total"),
    );

    // Steady state, until drained.
    let mut processed = 0usize;
    let wall = Instant::now();
    let deadline = Instant::now() + Duration::from_secs(240);
    let mut empty_in_a_row = 0;
    while processed < rows && Instant::now() < deadline {
        match worker.tick(max_rows, MAX_BYTES, Duration::from_millis(500)) {
            Ok(0) => {
                empty_in_a_row += 1;
                if empty_in_a_row > 3 {
                    break;
                }
            }
            Ok(n) => {
                empty_in_a_row = 0;
                processed += n;
            }
            Err(e) => panic!("tick failed: {e}"),
        }
    }
    let wall = wall.elapsed();

    let s = registry.snapshot();
    Profile {
        rows: processed,
        batches: counter(&s, "smeltr_tick_batches_total") - b_batches,
        wall,
        join,
        poll: counter(&s, "smeltr_tick_poll_seconds_total") - b_poll,
        process: counter(&s, "smeltr_tick_process_seconds_total") - b_proc,
        flush: counter(&s, "smeltr_tick_flush_seconds_total") - b_flush,
    }
}

const MAX_BYTES: usize = 64_000_000;

#[test]
fn tick_phase_profile() {
    let Some(brokers) = live_brokers("tick_phase_profile") else {
        return;
    };

    const ROWS: usize = 100_000;
    let inputs = vec![unique("profile-in")];
    let output = unique("profile-out");

    let t = Instant::now();
    produce(&brokers, &inputs[0], ROWS);
    // The producer-only baseline: the same destination, flushed once at
    // the end instead of once per batch.
    println!(
        "\nproduced {ROWS} rows in {:?} ({:.1} us/row, one flush at the end)",
        t.elapsed(),
        t.elapsed().as_micros() as f64 / ROWS as f64,
    );

    println!();
    println!(
        "{:>8}  {:>7}  {:>9}  {:>11}  {:>10}  {:>9}  {:>9}  {:>8}",
        "batch", "batches", "rows/s", "poll us/row", "cpu us/row", "flush/row", "flush/batch", "join s"
    );

    // A sweep, because the question is whether the cost is per row or
    // per batch. `SMELTR_PROFILE_BATCHES` narrows it without a rebuild.
    let batches: Vec<usize> = match std::env::var("SMELTR_PROFILE_BATCHES") {
        Ok(v) => v
            .split(',')
            .map(|s| s.trim().parse().expect("SMELTR_PROFILE_BATCHES: integers"))
            .collect(),
        Err(_) => vec![250, 1_000, 5_000, 25_000],
    };
    for max_rows in batches {
        let p = run_once(&brokers, &inputs, &output, ROWS, max_rows);
        let per_row = |v: u64| v as f64 / p.rows.max(1) as f64;
        println!(
            "{:>8}  {:>7}  {:>9.0}  {:>11.2}  {:>10.2}  {:>9.1}  {:>9.1}  {:>8.2}",
            max_rows,
            p.batches,
            p.rows as f64 / p.wall.as_secs_f64(),
            per_row(p.poll),
            per_row(p.process),
            per_row(p.flush),
            p.flush as f64 / p.batches.max(1) as f64 / 1000.0,
            p.join.as_secs_f64(),
        );
        assert!(p.rows > 0, "expected to process what we produced");
        assert!(p.batches > 0, "expected the phase counters to be populated");
    }
    println!("(flush/batch in ms, join in s; everything else in us/row)");
    println!("join is the one-time consumer-group rebalance, excluded from the rest.");
}

/// What decoding `_raw` costs. The same rows through the same pipeline,
/// with only the source's `encoding` different: UTF-8 validates in place
/// and borrows, a declared single-byte encoding transcodes into a new
/// string per record, and `auto` adds a mark scan to whichever it picks.
///
/// The Latin-1 rows are the case worth declaring: `auto` reads them, but
/// it has to guess, and a guess marks every row.
#[test]
fn decode_cost_by_encoding() {
    let Some(brokers) = live_brokers("decode_cost_by_encoding") else {
        return;
    };

    const ROWS: usize = 100_000;
    const BATCH: usize = 5_000;
    let utf8_in = vec![unique("decode-utf8-in")];
    let latin1_in = vec![unique("decode-latin1-in")];
    let output = unique("decode-out");
    produce(&brokers, &utf8_in[0], ROWS);
    produce_with(&brokers, &latin1_in[0], ROWS, latin1_row);

    println!();
    println!(
        "{:>10}  {:>14}  {:>9}  {:>10}",
        "records", "encoding", "rows/s", "cpu us/row"
    );
    for (label, inputs, encoding) in [
        ("ascii", &utf8_in, "auto"),
        ("ascii", &utf8_in, "utf-8"),
        ("ascii", &utf8_in, "windows-1252"),
        ("latin-1", &latin1_in, "auto"),
        ("latin-1", &latin1_in, "windows-1252"),
    ] {
        let p = decoding(&brokers, inputs, &output, ROWS, BATCH, encoding);
        println!(
            "{:>10}  {:>14}  {:>9.0}  {:>10.2}",
            label,
            encoding,
            p.rows as f64 / p.wall.as_secs_f64(),
            p.process as f64 / p.rows.max(1) as f64,
        );
        assert!(p.rows > 0, "expected to process what we produced");
    }
    println!("(cpu us/row is the process phase: decode, route, pipeline, serialize.)");
    println!("latin-1 under `auto` is decoded by guess, so every row is marked and tagged.");
}

/// Throughput against reader count: `threads` workers in one consumer
/// group, each warmed up until it owns partitions, then timed over a
/// common window behind a gate.
#[test]
fn thread_scaling() {
    let Some(brokers) = live_brokers("thread_scaling") else {
        return;
    };
    const ROWS: usize = 400_000;
    // Eight auto-created single-partition topics rather than one topic of
    // eight: creating a multi-partition topic needs rdkafka's async admin
    // client.
    const TOPICS: usize = 8;

    let stem = unique("scale-in");
    let inputs: Vec<String> = (0..TOPICS).map(|i| format!("{stem}-{i}")).collect();
    let output = unique("scale-out");
    let t = Instant::now();
    for topic in &inputs {
        produce(&brokers, topic, ROWS / TOPICS);
    }
    println!(
        "\nproduced {ROWS} rows over {TOPICS} topic-partitions in {:?}",
        t.elapsed()
    );
    println!(
        "host has {} cores; the effective thread_limit is {}",
        std::thread::available_parallelism().map(|n| n.get()).unwrap_or(0),
        smeltr::startup_config::worker_thread_limit(),
    );

    println!();
    println!(
        "{:>8}  {:>9}  {:>12}  {:>10}  {:>11}  {:>10}",
        "threads", "rows", "aggregate", "per thread", "poll us/row", "scaling"
    );
    let mut baseline = 0.0f64;
    for threads in [1usize, 2, 4, 8] {
        let group = unique("scale-group");
        let gate = Arc::new(Gate::new(threads));
        let handles: Vec<_> = (0..threads)
            .map(|idx| {
                // A destination topic per thread: one auto-created
                // partition would serialize every thread's writes.
                let (b, i, o, g, gate) = (
                    brokers.clone(),
                    inputs.clone(),
                    format!("{output}-{idx}"),
                    group.clone(),
                    gate.clone(),
                );
                std::thread::spawn(move || run_bundle(&bundle_json(&b, &i, &o, &g, "auto"), ROWS, 4000, Some(&gate)))
            })
            .collect();
        let profiles: Vec<Profile> = handles.into_iter().map(|h| h.join().unwrap()).collect();

        let rows: usize = profiles.iter().map(|p| p.rows).sum();
        let wall = profiles.iter().map(|p| p.wall).max().unwrap();
        let poll: u64 = profiles.iter().map(|p| p.poll).sum();
        let aggregate = rows as f64 / wall.as_secs_f64();
        if threads == 1 {
            baseline = aggregate;
        }
        println!(
            "{:>8}  {:>9}  {:>7.0} r/s  {:>6.0} r/s  {:>11.1}  {:>9.2}x",
            threads,
            rows,
            aggregate,
            aggregate / threads as f64,
            poll as f64 / rows.max(1) as f64,
            aggregate / baseline,
        );
        assert!(rows > 0, "expected {threads} threads to process something");
    }
    println!("(scaling is against the 1-thread aggregate; linear would be 1/2/4/8x)");
}
