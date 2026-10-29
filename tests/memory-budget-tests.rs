//! The memory budget from the runtime's side (design doc, Runtime stages): a refusal splits the range that asked for
//! too much, down to a single record, and only a record that will not fit
//! alone leaves the pipeline.

use std::sync::Arc;
use std::time::Duration;

use smeltr::core::buffer::MemoryBudget;
use smeltr::core::error::ExceptionKind;
use smeltr::core::id::DestId;
use smeltr::io::SourceRecord;
use smeltr::io::fake::{FakeDestination, FakeSource};
use smeltr::metrics::{MetricValue, Metrics, MetricsHandle};
use smeltr::plan::config::{Answer, PipelineConfig, Step};
use smeltr::plan::topology::{DeliverRule, RouteConfig, compile_topology};
use smeltr::runtime::{DestinationTarget, Worker};

use crate::harness;
use harness::*;

const TEST_TOPIC: &str = "t";

/// A pipeline that fans `items` out, one output row per element.
fn unrolling_pipeline(id: usize) -> PipelineConfig {
    pipeline(
        id,
        vec![code(
            0,
            "items: array<str>; Object.assign($, json(_raw)); unroll(items);",
        )],
    )
}

fn pipeline(id: usize, body: Vec<Step>) -> PipelineConfig {
    PipelineConfig {
        id,
        name: format!("pipeline-{id}"),
        body,
        step_ids: Vec::new(),
        lookup_tables: Default::default(),
        grok_patterns: Default::default(),
    }
}

fn deliver_to(destinations: &[DestId]) -> Vec<DeliverRule> {
    vec![DeliverRule {
        when: None,
        to: destinations.to_vec(),
    }]
}

fn record(raw: &str, offset: i64) -> SourceRecord {
    SourceRecord {
        chunk: smeltr::io::RecordChunk::of_bytes(raw.as_bytes()),
        row: 0,
        offset,
        partition: 0,
        topic: Arc::from(TEST_TOPIC),
        headers: None,
    }
}

fn target(id: DestId, dest: &FakeDestination) -> DestinationTarget {
    DestinationTarget {
        id,
        name: id.to_string(),
        destination: Box::new(dest.clone()),
        on_reject: Answer::Drop,
        wants_doc_id: false,
        placement: Some(smeltr::plan::topology::Placement::Topic),
    }
}

fn worker(
    routes: Vec<RouteConfig>,
    pipelines: Vec<PipelineConfig>,
    source: &FakeSource,
    memory: MemoryBudget,
) -> (Worker, Arc<Metrics>) {
    let topology = compile_topology(&routes, &pipelines).unwrap_or_else(|e| {
        panic!(
            "topology compile failed: {}",
            e.iter().map(|x| x.to_string()).collect::<Vec<_>>().join("; ")
        )
    });
    let registry = Arc::new(Metrics::new());
    let w = Worker::new(
        0,
        Box::new(source.clone()),
        topology,
        Vec::new(),
        memory,
        MetricsHandle::new(registry.clone(), "t0"),
    );
    (w, registry)
}

fn counter(registry: &Metrics, name: &str) -> u64 {
    registry
        .snapshot()
        .into_iter()
        .filter(|s| s.key.name == name)
        .filter_map(|s| match s.value {
            MetricValue::Counter(c) => Some(c),
            MetricValue::FloatCounter(_) | MetricValue::Gauge(_) => None,
        })
        .sum()
}

/// A catch-all route through one pipeline into destination 0.
fn catch_all(pipeline_id: usize) -> RouteConfig {
    RouteConfig {
        id: format!("route-{pipeline_id}"),
        source: 0,
        cond: None,
        pipeline: pipeline_id,
        deliver: deliver_to(&[0]),
        on_exception: Default::default(),
    }
}

/// A poll big enough to hand the whole fixture over in one batch.
fn tick(w: &mut Worker) -> usize {
    w.tick(4000, 256 * 1024 * 1024, Duration::from_millis(50))
        .expect("the pipeline delivers")
}

/// One record whose array fans out far enough to exceed a small budget.
fn fanning_record(elements: usize, offset: i64) -> SourceRecord {
    let items: Vec<String> = (0..elements)
        .map(|i| format!("\"element-{i:06}-{}\"", "p".repeat(48)))
        .collect();
    record(&format!(r#"{{"items":[{}]}}"#, items.join(",")), offset)
}

/// Ordinary work runs nowhere near the default ceiling.
#[test]
fn an_ordinary_batch_runs_far_below_the_default_budget() {
    let budget = MemoryBudget::new(usize::MAX);
    let rows: Vec<SourceRecord> = (0..4000)
        .map(|i| {
            record(
                &format!(
                    r#"{{"host":"web-{i:04}","level":"info","msg":"{}","n":{i}}}"#,
                    "x".repeat(900)
                ),
                i as i64,
            )
        })
        .collect();
    let src = FakeSource::new(rows);
    let (mut w, _registry) = worker(
        vec![catch_all(0)],
        vec![pipeline(
            0,
            vec![code(
                0,
                "host: str; Object.assign($, json(_raw)); kept = host + \"-tagged\";",
            )],
        )],
        &src,
        budget.clone(),
    );
    let dest = FakeDestination::new();
    w.add_destination(target(0, &dest));

    assert_eq!(tick(&mut w), 4000);

    let peak = budget.peak();
    // The largest batch a reader polls; the default has to leave room for
    // every thread to hold one.
    println!("ordinary batch peak: {peak} bytes over 4000 rows of ~1 KB");
    let default = smeltr::worker::process_memory_budget()
        .limit()
        .expect("the default is a real ceiling");
    assert!(
        peak * 8 < default,
        "an ordinary batch peaked at {peak} bytes, too close to the {default}-byte default"
    );
}

/// A batch that cannot be materialized inside the budget is halved until
/// each piece fits, and every record is still delivered.
#[test]
fn a_batch_that_does_not_fit_is_split_until_it_does() {
    // Well under this workload's unbudgeted peak. The ceiling and the fan-out scale together: what is under test is
    // a batch that does not fit, which a quarter of each provokes exactly as well and four times faster.
    let limit = 8 * 1024 * 1024;
    let budget = MemoryBudget::new(limit);
    let src = FakeSource::new((0..8).map(|i| fanning_record(5_000, i)).collect::<Vec<_>>());
    let (mut w, registry) = worker(vec![catch_all(0)], vec![unrolling_pipeline(0)], &src, budget.clone());
    // One slice, so the whole batch asks for more than the budget holds.
    w.transform.slice_bytes = usize::MAX;
    let dest = FakeDestination::new();
    w.add_destination(target(0, &dest));

    assert_eq!(tick(&mut w), 8, "every record committed");
    assert_eq!(
        dest.sent().len(),
        8 * 5_000,
        "every element was delivered, not just the ones that fit"
    );
    assert!(
        counter(&registry, "smeltr_memory_splits_total") > 0,
        "it should have had to split at all -- otherwise this proves nothing"
    );
    assert_eq!(
        counter(&registry, "smeltr_rows_dlq_over_budget_total"),
        0,
        "splitting was enough; nothing should have been given up on"
    );
    assert!(budget.peak() <= limit, "the ceiling held: peaked at {}", budget.peak());
}

/// A record that will not fit alone leaves the pipeline alone, for where the deployment sends unprocessable rows.
#[test]
fn a_record_that_will_not_fit_alone_goes_where_unprocessable_rows_go_by_itself() {
    let budget = MemoryBudget::new(2 * 1024 * 1024);
    let mut records: Vec<SourceRecord> = (0..4).map(|i| record(r#"{"items":["a"]}"#, i)).collect();
    records.insert(2, fanning_record(50_000, 99));
    let src = FakeSource::new(records);
    let (mut w, registry) = worker(vec![catch_all(0)], vec![unrolling_pipeline(0)], &src, budget);
    let dest = FakeDestination::new();
    let dlq = FakeDestination::new();
    w.add_destination(target(0, &dest));
    w.add_destination(target(1, &dlq));
    Arc::get_mut(&mut w.transform.topology)
        .expect("a test worker's topology is its own")
        .on_exception
        .answers[ExceptionKind::Unprocessable.index()] = Answer::Deliver(vec![1]);

    assert_eq!(tick(&mut w), 5, "every record committed");

    assert_eq!(
        counter(&registry, "smeltr_rows_dlq_over_budget_total"),
        1,
        "exactly the one record that could not be split further"
    );
    assert_eq!(dlq.sent().len(), 1, "and it went where unprocessable rows go");
    assert_eq!(
        dest.sent().len(),
        4,
        "the four records that fit were delivered, not punished for it"
    );
}

/// Under block, a record that will not fit alone holds its whole batch until the config changes: nothing of it is
/// delivered, and nothing commits.
#[test]
fn a_record_that_will_not_fit_alone_under_block_holds_its_batch() {
    let budget = MemoryBudget::new(2 * 1024 * 1024);
    let mut records: Vec<SourceRecord> = (0..4).map(|i| record(r#"{"items":["a"]}"#, i)).collect();
    records.insert(2, fanning_record(50_000, 99));
    let src = FakeSource::new(records);
    let (mut w, registry) = worker(vec![catch_all(0)], vec![unrolling_pipeline(0)], &src, budget);
    let dest = FakeDestination::new();
    w.add_destination(target(0, &dest));

    for _ in 0..3 {
        let _ = w.tick(4000, 256 * 1024 * 1024, Duration::from_millis(50));
    }
    assert!(src.committed().is_empty(), "nothing commits past a held batch");
    assert!(src.is_blocked(), "and the source says why it stopped");
    // `FakeDestination` keeps what an attempt buffered and then discarded, so the count is what says it.
    assert_eq!(
        counter(&registry, "smeltr_rows_sent_total"),
        0,
        "nothing of the held batch was delivered"
    );
}

/// A route that has dispatched does not run again when a later route
/// refuses.
#[test]
fn a_later_routes_refusal_does_not_re_send_an_earlier_routes_rows() {
    let budget = MemoryBudget::new(8 * 1024 * 1024);
    let routes = vec![
        RouteConfig {
            id: "route-0".to_string(),
            source: 0,
            cond: None,
            pipeline: 0,
            deliver: deliver_to(&[0]),
            on_exception: Default::default(),
        },
        RouteConfig {
            id: "route-1".to_string(),
            source: 0,
            cond: None,
            pipeline: 1,
            deliver: deliver_to(&[1]),
            on_exception: Default::default(),
        },
    ];
    let pipelines = vec![
        pipeline(0, vec![code(0, "tag = \"first\"; t = toString(now());")]),
        pipeline(
            1,
            vec![code(
                0,
                "items: array<str>; Object.assign($, json(_raw)); unroll(items); t = toString(now());",
            )],
        ),
    ];
    let src = FakeSource::new((0..8).map(|i| fanning_record(5_000, i)).collect::<Vec<_>>());
    let (mut w, registry) = worker(routes, pipelines, &src, budget);
    // One slice, so the whole batch asks for more than the budget holds.
    w.transform.slice_bytes = usize::MAX;
    let first = FakeDestination::new();
    let second = FakeDestination::new();
    w.add_destination(target(0, &first));
    w.add_destination(target(1, &second));

    tick(&mut w);

    println!(
        "splits={} dlq={} dropped={} first={} second={}",
        counter(&registry, "smeltr_memory_splits_total"),
        counter(&registry, "smeltr_rows_dlq_over_budget_total"),
        counter(&registry, "smeltr_rows_dropped_total"),
        first.sent().len(),
        second.sent().len()
    );
    assert!(
        counter(&registry, "smeltr_memory_splits_total") > 0,
        "the second route has to have refused for this to test anything"
    );
    assert_eq!(
        first.sent().len(),
        8,
        "the first route delivered each record once, despite the second route splitting"
    );
    assert_eq!(second.sent().len(), 8 * 5_000, "the second route still landed");
    let stamps: std::collections::BTreeSet<String> = first
        .sent()
        .iter()
        .chain(second.sent().iter())
        .map(|r| serde_json::from_slice::<serde_json::Value>(&r.value).unwrap()["t"].to_string())
        .collect();
    assert_eq!(stamps.len(), 1, "a split keeps the records' one now(), got {stamps:?}");
}

/// A realistic message is never diverted for being merely large.
///
/// The slowest test in the suite, and deliberately so: two hundred thousand elements *is* the claim, so the count
/// cannot be scaled down the way the tests above scale theirs. What it pins -- that one record leaves three quarters
/// of the default ceiling free -- is broken by any change to how unroll allocates, and would otherwise be noticed in
/// production.
#[test]
fn the_largest_realistic_single_record_fits_the_default_budget() {
    let budget = MemoryBudget::new(usize::MAX);
    let src = FakeSource::new(vec![fanning_record(200_000, 0)]);
    let (mut w, registry) = worker(vec![catch_all(0)], vec![unrolling_pipeline(0)], &src, budget.clone());
    let dest = FakeDestination::new();
    w.add_destination(target(0, &dest));

    tick(&mut w);

    assert_eq!(dest.sent().len(), 200_000, "every element was delivered");
    assert_eq!(counter(&registry, "smeltr_rows_dlq_over_budget_total"), 0);
    let peak = budget.peak();
    let default = smeltr::worker::process_memory_budget()
        .limit()
        .expect("the default is a real ceiling");
    println!("one 200k-element record peaked at {peak} bytes against a {default}-byte default");
    assert!(
        peak * 4 < default,
        "one record peaked at {peak} bytes, leaving too little of the {default}-byte default"
    );
}

/// One record of `"a":1` repeated to about `len` bytes: its parse holds far more than its columns do.
fn repeated_key_record(len: usize, offset: i64) -> SourceRecord {
    let mut s = String::from("{");
    while s.len() < len {
        s.push_str("\"a\":1,");
    }
    s.push_str("\"a\":1}");
    record(&s, offset)
}

/// Idle buffers are charged though nothing uses them, so a record refused while the pool held them runs once more
/// without them, and one that fits the budget alone is delivered.
#[test]
fn a_record_refused_beside_idle_buffers_runs_again_without_them() {
    let limit = 14 << 20;
    let parse = || pipeline(0, vec![code(0, "msg: str; Object.assign($, json(_raw));")]);
    let big = || repeated_key_record(256 << 10, 1000);
    let alone = MemoryBudget::new(usize::MAX);
    let (mut w, _) = worker(
        vec![catch_all(0)],
        vec![parse()],
        &FakeSource::new(vec![big()]),
        alone.clone(),
    );
    w.add_destination(target(0, &FakeDestination::new()));
    tick(&mut w);

    let budget = MemoryBudget::new(limit);
    let wide = (0..1000).map(|i| record(&format!(r#"{{"msg":"{}"}}"#, "x".repeat(3500)), i));
    let src = FakeSource::new(wide);
    let (mut w, registry) = worker(vec![catch_all(0)], vec![parse()], &src, budget.clone());
    // One slice: what the pool keeps idle follows the slice, which otherwise follows this machine's L2.
    w.transform.slice_bytes = usize::MAX;
    let dest = FakeDestination::new();
    w.add_destination(target(0, &dest));
    tick(&mut w);
    let idle = budget.used();
    assert!(
        alone.peak() < limit && alone.peak() + idle > limit,
        "the record needs {} bytes alone and {idle} sit idle: it must fit only without them",
        alone.peak()
    );

    src.requeue(vec![big()]);
    tick(&mut w);
    assert_eq!(
        counter(&registry, "smeltr_rows_dlq_over_budget_total"),
        0,
        "the record was not diverted"
    );
    assert_eq!(dest.sent().len(), 1001, "it was delivered");
}
