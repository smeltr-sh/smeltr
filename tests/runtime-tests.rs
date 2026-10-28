//! The runtime over fakes: `FakeSource` in, through routing and a compiled
//! pipeline, out through a fake destination, driven by `Worker::tick`.

use std::sync::Arc;
use std::time::Duration;

use smeltr::core::buffer::MemoryBudget;
use smeltr::core::error::ExceptionKind;
use smeltr::core::id::{DestId, FunctionId};
use smeltr::egress::Egress;
use smeltr::format::encoding::Encoding;
use smeltr::handoff::{BlockCause, Handoff, reports};
use smeltr::io::fake::{AsyncDestination, FakeDestination, FakeSource};
use smeltr::io::{
    BatchId, CommitMap, Destination, IoError, OwnedRecord, PartitionLag, Rejection, Source, SourceRecord,
    TopicPartition,
};
use smeltr::metrics::{MetricValue, Metrics, MetricsHandle};
use smeltr::plan::compile::compile;
use smeltr::plan::config::{Answer, OnException, Payload, PipelineConfig, Step};
use smeltr::plan::topology::{
    DeliverRule, RouteConfig, SourceMeta, SourceMetas, compile_topology, compile_topology_with,
};
use smeltr::runtime::{
    DROPPED_REJECTED, DROPPED_UNPROCESSABLE, DestinationTarget, MAX_IN_FLIGHT, MAX_TRACKED, Polled, Reader, Worker,
};

use crate::harness;
use harness::*;

// -- fixtures ---------------------------------------------------------------

/// Make every attempt panic once its routes have produced their rows: the only way to reach the poison path, since a
/// pipeline never panics by design.
fn poison(w: &mut Worker) {
    w.transform.fault = Some(|| panic!("an injected fault -- poison-batch test fixture"));
}

const TEST_TOPIC: &str = "t";

fn if_(id: FunctionId, cond: &str, then: Vec<Step>) -> Step {
    Step::If {
        id,
        cond: cond.to_string(),
        then,
        else_: Vec::new(),
    }
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

/// Tagging every exception, encoding's included.
fn tagging_repairs() -> OnException {
    OnException::every(Answer::Tag)
}

/// The default answers, writing the tag at `error.message`, inside an object a record may hold too.
fn messaging_at_error() -> OnException {
    OnException {
        tag_field: "error.message".to_string(),
        ..OnException::default()
    }
}

/// `routes`, each answering its pipeline's exceptions with `on_exception`.
fn answering(on_exception: OnException, routes: Vec<RouteConfig>) -> Vec<RouteConfig> {
    routes
        .into_iter()
        .map(|r| RouteConfig {
            on_exception: on_exception.clone(),
            ..r
        })
        .collect()
}

/// `routes`, each dropping the rows its rules decline: a route that delivers only some rows on purpose says so.
fn dropping_declined(routes: Vec<RouteConfig>) -> Vec<RouteConfig> {
    let mut on_exception = OnException::default();
    on_exception.answers[ExceptionKind::Unmatched.index()] = Answer::Drop;
    answering(on_exception, routes)
}

fn rule(when: Option<&str>, to: &[DestId]) -> DeliverRule {
    DeliverRule {
        when: when.map(str::to_string),
        to: to.to_vec(),
    }
}

fn deliver_to(destinations: &[DestId]) -> Vec<DeliverRule> {
    vec![rule(None, destinations)]
}

/// A route from source 0.
fn route_cfg(cond: Option<&str>, pipeline: usize, deliver: Vec<DeliverRule>) -> RouteConfig {
    RouteConfig {
        id: "r".to_string(),
        source: 0,
        cond: cond.map(str::to_string),
        pipeline,
        deliver,
        on_exception: Default::default(),
    }
}

/// One catch-all route from source 0 through pipeline 0 to destination 0.
fn plain_route() -> Vec<RouteConfig> {
    vec![route_with_deliver(deliver_to(&[0]))]
}

fn route_with_deliver(deliver: Vec<DeliverRule>) -> RouteConfig {
    route_cfg(None, 0, deliver)
}

/// Tagged rows to destination 1 instead of destination 0: the tag field decides, like any other field.
fn diverting_route() -> Vec<RouteConfig> {
    vec![route_with_deliver(vec![
        rule(Some("smeltr.exception == null"), &[0]),
        rule(Some("smeltr.exception != null"), &[1]),
    ])]
}

/// `plain_route`, with its pipeline's failed rows answered by `answer`.
fn failing(answer: Answer) -> Vec<RouteConfig> {
    answering(OnException::every(answer), plain_route())
}

fn record(raw: &str, offset: i64, partition: i32) -> SourceRecord {
    record_on(TEST_TOPIC, raw, offset, partition)
}

fn record_bytes(raw: Vec<u8>, offset: i64) -> SourceRecord {
    SourceRecord {
        chunk: smeltr::io::RecordChunk::of_bytes(raw),
        row: 0,
        offset,
        partition: 0,
        topic: Arc::from(TEST_TOPIC),
        headers: None,
    }
}

fn record_on(topic: &str, raw: &str, offset: i64, partition: i32) -> SourceRecord {
    SourceRecord {
        chunk: smeltr::io::RecordChunk::of_bytes(raw.as_bytes()),
        row: 0,
        offset,
        partition,
        topic: Arc::from(topic),
        headers: None,
    }
}

fn tp(partition: i32) -> TopicPartition {
    TopicPartition::new(Arc::from(TEST_TOPIC), partition)
}

fn worker(routes: Vec<RouteConfig>, pipelines: Vec<PipelineConfig>, source: &FakeSource) -> Worker {
    worker_decoding(routes, pipelines, source, Encoding::Auto)
}

fn worker_decoding(
    routes: Vec<RouteConfig>,
    pipelines: Vec<PipelineConfig>,
    source: &FakeSource,
    encoding: Encoding,
) -> Worker {
    let topology = compile_topology(&routes, &pipelines).unwrap_or_else(|e| {
        panic!(
            "topology compile failed: {}",
            e.iter().map(|x| x.to_string()).collect::<Vec<_>>().join("; ")
        )
    });
    Worker::new(
        0,
        Box::new(source.clone()),
        encoding,
        topology,
        Vec::new(),
        MemoryBudget::unlimited(),
        MetricsHandle::new(Arc::new(Metrics::new()), "t0"),
    )
}

/// A worker whose group answers `Unmatched` with `unmatched`.
fn worker_with_unmatched(
    routes: Vec<RouteConfig>,
    pipelines: Vec<PipelineConfig>,
    source: &FakeSource,
    unmatched: Answer,
) -> Worker {
    let metas = SourceMetas::from([(0, SourceMeta::KAFKA)]);
    let mut on_exception = OnException::default();
    on_exception.answers[ExceptionKind::Unmatched.index()] = unmatched;
    let topology = compile_topology_with(&routes, &pipelines, on_exception, &metas).expect("topology compiles");
    Worker::new(
        0,
        Box::new(source.clone()),
        Encoding::Auto,
        topology,
        Vec::new(),
        MemoryBudget::unlimited(),
        MetricsHandle::new(Arc::new(Metrics::new()), "t0"),
    )
}

/// Delivered to each of `to`, in each one's own form.
fn deliver(to: &[DestId]) -> Answer {
    Answer::Deliver(to.to_vec())
}

/// A refusal delivered to `to`.
fn salvage_to(to: &[DestId]) -> Answer {
    deliver(to)
}

/// `w`'s destination `dest` writes `payload`. Set after the worker is built: a test worker's topology is its own.
fn writes(w: &mut Worker, dest: DestId, payload: Payload) {
    Arc::get_mut(&mut w.transform.topology)
        .expect("a test worker's topology is its own")
        .payloads
        .insert(dest, payload);
}

/// `_raw` as text.
fn raw_text() -> Payload {
    Payload::Text { field: "_raw".into() }
}

/// Answer unprocessable rows with `answer`. Set after the worker is built: a test worker's topology is its own.
fn set_unprocessable(w: &mut Worker, answer: Answer) {
    Arc::get_mut(&mut w.transform.topology)
        .expect("a test worker's topology is its own")
        .on_exception
        .answers[ExceptionKind::Unprocessable.index()] = answer;
}

/// A catch-all route through an empty pipeline to `dest` as destination 0.
fn async_worker(dest: &AsyncDestination, source: &FakeSource) -> Worker {
    let mut w = worker(plain_route(), vec![pipeline(0, vec![])], source);
    w.add_destination(target_with(0, dest.clone(), Answer::Drop));
    w
}

fn tick(w: &mut Worker) -> Result<usize, IoError> {
    w.tick(100, 1 << 20, Duration::from_millis(1))
}

fn target(id: DestId, dest: impl Destination + 'static) -> DestinationTarget {
    target_with(id, dest, Answer::Block)
}

fn target_with(id: DestId, dest: impl Destination + 'static, on_reject: Answer) -> DestinationTarget {
    DestinationTarget {
        id,
        name: format!("dest-{id}"),
        destination: Box::new(dest),
        on_reject,
        wants_doc_id: false,
        placement: Some(smeltr::plan::topology::Placement::Topic),
        id_field: None,
    }
}

/// A destination that keys documents on `__destination.id`, or on `id_field`.
fn keyed(id: DestId, dest: impl Destination + 'static, id_field: Option<&str>) -> DestinationTarget {
    DestinationTarget {
        id,
        name: format!("dest-{id}"),
        destination: Box::new(dest),
        on_reject: Answer::Block,
        wants_doc_id: true,
        placement: Some(smeltr::plan::topology::Placement::Index),
        id_field: id_field.map(str::to_string),
    }
}

fn text(bytes: &[u8]) -> &str {
    std::str::from_utf8(bytes).unwrap()
}

fn body(bytes: &[u8]) -> serde_json::Value {
    serde_json::from_slice(bytes).unwrap()
}

/// The exceptions a row's default tag lists, each as `kind: message`, joined by `; `. `None` on an untagged row.
fn exception_text(row: &serde_json::Value) -> Option<String> {
    let exceptions = row.pointer("/smeltr/exception/exceptions")?.as_array()?;
    let each = exceptions
        .iter()
        .map(|e| format!("{}: {}", e["kind"].as_str().unwrap(), e["message"].as_str().unwrap()));
    Some(each.collect::<Vec<_>>().join("; "))
}

/// The sample for `name{labels}` (labels order-insensitive, subset: samples
/// carry the writing thread's label too), or `None`.
fn metric(w: &Worker, name: &str, labels: &[(&str, &str)]) -> Option<MetricValue> {
    let want: std::collections::HashSet<(&str, &str)> = labels.iter().map(|(k, v)| (*k, *v)).collect();
    w.transform.metrics.registry().snapshot().into_iter().find_map(|s| {
        if s.key.name != name {
            return None;
        }
        let got: std::collections::HashSet<(&str, &str)> = s.key.labels.iter().map(|(k, v)| (*k, v.as_str())).collect();
        want.is_subset(&got).then_some(s.value)
    })
}

fn counter(w: &Worker, name: &str, labels: &[(&str, &str)]) -> Option<u64> {
    match metric(w, name, labels) {
        Some(MetricValue::Counter(v)) => Some(v),
        _ => None,
    }
}

fn counter_total(w: &Worker, name: &str) -> u64 {
    w.transform
        .metrics
        .registry()
        .snapshot()
        .iter()
        .filter(|s| s.key.name == name)
        .map(|s| match s.value {
            MetricValue::Counter(c) => c,
            MetricValue::FloatCounter(g) | MetricValue::Gauge(g) => g as u64,
        })
        .sum()
}

/// The `smeltr_source_blocked` gauge for source 0.
fn blocked(w: &Worker) -> Option<f64> {
    match metric(w, "smeltr_source_blocked", &[("source", "0")]) {
        Some(MetricValue::Gauge(v)) => Some(v),
        _ => None,
    }
}

/// `steps` after a JSON parser that declares nothing, run over two records whose `flag` is text. Returns the worker
/// and what it delivered.
/// `flag` is declared `bool`, and neither record's text is one: the field is null on both rows, and a null
/// condition reads false.
fn run_on_text_flags(steps: Vec<Step>) -> (Worker, Vec<serde_json::Value>) {
    let steps = std::iter::once(code(0, "flag: bool; Object.assign($, json(_raw));"))
        .chain(steps)
        .collect();
    let src = FakeSource::new([record(r#"{"flag":"yes"}"#, 0, 0), record(r#"{"flag":"no"}"#, 1, 0)]);
    let mut w = worker(plain_route(), vec![pipeline(0, steps)], &src);
    let dest = FakeDestination::new();
    w.add_destination(target(0, dest.clone()));
    tick(&mut w).unwrap();
    let sent = dest.sent().iter().map(|r| body(&r.value)).collect();
    (w, sent)
}

// -- lag ----------------------------------------------------------------

/// A `FakeSource` that also answers `lag`, so a test can check what `Reader::step` does with a lag sample without a
/// live broker: a real one's broker round trips are `KafkaSource`'s own concern (`kafka-tests.rs`), but turning a
/// sample into a gauge is generic over any source kind.
struct LagFakeSource {
    inner: FakeSource,
    samples: Vec<PartitionLag>,
}

impl Source for LagFakeSource {
    fn poll_batch(
        &mut self,
        max_rows: usize,
        max_bytes: usize,
        timeout: Duration,
    ) -> Result<Vec<SourceRecord>, IoError> {
        self.inner.poll_batch(max_rows, max_bytes, timeout)
    }

    fn commit(&mut self, offsets: &CommitMap) -> Result<(), IoError> {
        self.inner.commit(offsets)
    }

    fn lag(&mut self) -> Vec<PartitionLag> {
        std::mem::take(&mut self.samples)
    }
}

/// A source kind that never overrides `lag` -- `FakeSource`, the same as every other source but Kafka -- answers
/// the trait's default "cannot report" (E6), and `Reader::step` calling it every turn produces no
/// `smeltr_source_lag` sample.
#[test]
fn a_source_that_cannot_report_lag_emits_no_lag_metric() {
    let src = FakeSource::new([record("a", 0, 0)]);
    let mut w = worker(plain_route(), vec![pipeline(0, vec![])], &src);
    w.add_destination(target(0, FakeDestination::new()));

    tick(&mut w).unwrap();

    assert!(metric(&w, "smeltr_source_lag", &[("source", "0")]).is_none());
}

/// `Reader::step` turns a source's lag sample into a gauge labeled by source, topic and partition (E6) -- the
/// coordinates a per-partition series needs, and more than the two-label minimum: two topics under one streaming
/// source can share a partition number, and only the topic tells their lag apart.
#[test]
fn a_lag_sample_becomes_a_gauge_labeled_by_source_topic_and_partition() {
    let source = LagFakeSource {
        inner: FakeSource::default(),
        samples: vec![PartitionLag {
            partition: TopicPartition::new(Arc::from(TEST_TOPIC), 2),
            lag: 41,
        }],
    };
    let topology = compile_topology(&plain_route(), &[pipeline(0, vec![])]).unwrap();
    let mut w = Worker::new(
        0,
        Box::new(source),
        Encoding::Auto,
        topology,
        Vec::new(),
        MemoryBudget::unlimited(),
        MetricsHandle::new(Arc::new(Metrics::new()), "t0"),
    );

    tick(&mut w).unwrap();

    assert!(matches!(
        metric(
            &w,
            "smeltr_source_lag",
            &[("source", "0"), ("topic", TEST_TOPIC), ("partition", "2")]
        ),
        Some(MetricValue::Gauge(v)) if v == 41.0
    ));
}

// -- routing ----------------------------------------------------------------

/// A batch runs through its pipelines a slice at a time, and is still delivered whole, in source order, and committed
/// once.
#[test]
fn a_batch_run_in_slices_is_delivered_whole_and_in_order() {
    let pipelines = vec![pipeline(0, vec![code(0, r#"greeting = "hi";"#)])];
    let src = FakeSource::new((0..5).map(|i| record(&format!("r{i}"), 10 + i, 0)).collect::<Vec<_>>());
    let mut w = worker(plain_route(), pipelines, &src);
    w.transform.slice_bytes = 4;
    let dest = FakeDestination::new();
    w.add_destination(target(0, dest.clone()));

    assert_eq!(tick(&mut w).unwrap(), 5);
    let sent: Vec<String> = dest.sent().iter().map(|r| body(&r.value)["_raw"].to_string()).collect();
    assert_eq!(sent, [r#""r0""#, r#""r1""#, r#""r2""#, r#""r3""#, r#""r4""#]);
    assert_eq!(dest.flushes(), 1);
    assert_eq!(src.committed(), vec![[(tp(0), 14i64)].into_iter().collect()]);
    assert_eq!(
        counter(
            &w,
            "smeltr_function_calls_total",
            &[("pipeline", "0"), ("function", "0")]
        ),
        Some(3),
        "one call per slice: two records of two bytes fill one"
    );
}

/// Every row reaches the destination in one flush, the commit is the batch's high-water offset, and the metrics agree with the fakes.
#[test]
fn a_catch_all_route_delivers_every_row_and_commits_once() {
    let pipelines = vec![pipeline(0, vec![code(0, r#"greeting = "hi";"#)])];
    let src = FakeSource::new([record("a", 10, 0), record("b", 11, 0)]);
    let mut w = worker(plain_route(), pipelines, &src);
    let dest = FakeDestination::new();
    w.add_destination(target(0, dest.clone()));

    let n = tick(&mut w).unwrap();
    let sent = dest.sent();
    assert_eq!(n, 2);
    assert_eq!(sent.len(), 2);
    assert_eq!(text(&sent[0].value), r#"{"_raw":"a","greeting":"hi"}"#);
    assert_eq!(dest.flushes(), 1);
    assert_eq!(src.committed(), vec![[(tp(0), 11i64)].into_iter().collect()]);

    let rows = Some(n as u64);
    // Positional here: the worker resolves it to the author's own id on its way to the leader.
    let f0 = [("pipeline", "0"), ("function", "0")];
    assert_eq!(counter(&w, "smeltr_rows_polled_total", &[("source", "0")]), rows);
    // "a" and "b": one byte each of payload, which is what a byte rate is a rate of.
    assert_eq!(counter(&w, "smeltr_bytes_polled_total", &[("source", "0")]), Some(2));
    // The route's own id, not its position: reordering the list must not re-attribute this count.
    assert_eq!(
        counter(&w, "smeltr_route_matched_total", &[("source", "0"), ("route", "r")]),
        rows
    );
    assert_eq!(counter(&w, "smeltr_rows_sent_total", &[("destination", "0")]), rows);
    assert_eq!(
        counter(&w, "smeltr_function_calls_total", &f0),
        Some(1),
        "one call per batch, not per row"
    );
    assert_eq!(
        counter(&w, "smeltr_flushes_total", &[("destination", "0")]),
        Some(dest.flushes() as u64)
    );
    assert_eq!(
        counter(&w, "smeltr_commits_total", &[("source", "0")]),
        Some(src.committed().len() as u64)
    );
}

/// A route for another source takes none of source 0's rows, which meet the group's Unmatched answer. A route with no
/// `deliver` rules takes them, and each meets that route's Unmatched answer: counted unrouted either way it goes.
#[test]
fn a_route_that_delivers_nowhere_is_not_an_error() {
    let other_source = RouteConfig {
        source: 1,
        ..route_with_deliver(deliver_to(&[0]))
    };
    let no_rules = dropping_declined(vec![route_with_deliver(Vec::new())]).remove(0);
    for (label, route, unrouted) in [("another source", other_source, None), ("no rules", no_rules, Some(1))] {
        let src = FakeSource::new([record("a", 0, 0)]);
        let mut w = worker_with_unmatched(vec![route], vec![pipeline(0, vec![])], &src, Answer::Drop);
        let dest = FakeDestination::new();
        w.add_destination(target(0, dest.clone()));

        tick(&mut w).unwrap();
        assert!(dest.sent().is_empty(), "{label}");
        assert_eq!(
            counter(&w, "smeltr_rows_unmatched_total", &[("source", "0"), ("cause", "rule")]),
            unrouted,
            "{label}"
        );
        assert_eq!(
            counter(
                &w,
                "smeltr_rows_dropped_total",
                &[("source", "0"), ("reason", "unmatched")]
            ),
            Some(1),
            "{label}"
        );
        assert_eq!(src.committed().len(), 1, "{label}");
    }
}

/// A row the pipeline finished that no delivery rule takes meets its route's Unmatched answer: delivered as the
/// pipeline left it, dropped and counted, or blocking the source. Rows a rule takes go on as before.
#[test]
fn a_row_no_rule_takes_meets_its_routes_unmatched_answer() {
    let routes_answering = |answer: Answer| {
        let mut on_exception = OnException::default();
        on_exception.answers[ExceptionKind::Unmatched.index()] = answer;
        answering(
            on_exception,
            vec![route_with_deliver(vec![rule(Some(r#"_raw == "x""#), &[0])])],
        )
    };
    let run = |answer: Answer| {
        let src = FakeSource::new([record("x", 0, 0), record("y", 1, 0)]);
        // The group drops: what the route answers is what decides.
        let mut w = worker_with_unmatched(
            routes_answering(answer),
            vec![pipeline(0, vec![code(0, r#"via = "p";"#)])],
            &src,
            Answer::Drop,
        );
        let (taken, leftovers) = (FakeDestination::new(), FakeDestination::new());
        w.add_destination(target(0, taken.clone()));
        w.add_destination(target(1, leftovers.clone()));
        let ticked = tick(&mut w);
        (w, src, taken, leftovers, ticked)
    };

    let (w, src, taken, leftovers, ticked) = run(Answer::Deliver(vec![1]));
    ticked.unwrap();
    assert_eq!(text(&taken.sent()[0].value), r#"{"_raw":"x","via":"p"}"#);
    let left = leftovers.sent();
    assert_eq!(left.len(), 1);
    assert_eq!(
        text(&left[0].value),
        r#"{"_raw":"y","via":"p"}"#,
        "as the pipeline left it"
    );
    assert_eq!(
        counter(&w, "smeltr_rows_unmatched_total", &[("source", "0"), ("cause", "rule")]),
        Some(1)
    );
    assert_eq!(src.committed().len(), 1);

    let (w, src, taken, leftovers, ticked) = run(Answer::Drop);
    ticked.unwrap();
    assert_eq!(taken.sent().len(), 1);
    assert!(leftovers.sent().is_empty());
    assert_eq!(
        counter(
            &w,
            "smeltr_rows_dropped_total",
            &[("source", "0"), ("reason", "unmatched")]
        ),
        Some(1)
    );
    assert_eq!(src.committed().len(), 1);

    let (_, src, taken, leftovers, ticked) = run(Answer::Block);
    let e = ticked.expect_err("block fails the tick so nothing commits").to_string();
    assert!(e.contains("No delivery rule of route r took 1 record(s)"), "{e}");
    assert!(
        taken.sent().is_empty() && leftovers.sent().is_empty(),
        "nothing of the batch is sent"
    );
    assert!(src.committed().is_empty());
}

/// Routes are unordered: each sees every row, so a row two routes match runs both pipelines.
#[test]
fn a_row_two_routes_match_runs_both_pipelines() {
    let routes = vec![
        route_cfg(Some("__source.offset == 0"), 0, deliver_to(&[0])),
        route_cfg(None, 1, deliver_to(&[1])),
    ];
    let pipelines = vec![
        pipeline(0, vec![code(0, r#"via = "a";"#)]),
        pipeline(1, vec![code(0, r#"via = "b";"#)]),
    ];
    let src = FakeSource::new([record("x", 0, 0), record("y", 1, 0)]);
    let mut w = worker(routes, pipelines, &src);
    let (a, b) = (FakeDestination::new(), FakeDestination::new());
    w.add_destination(target(0, a.clone()));
    w.add_destination(target(1, b.clone()));

    tick(&mut w).unwrap();
    let a_sent = a.sent();
    assert_eq!(a_sent.len(), 1, "only offset 0 matches");
    assert_eq!(text(&a_sent[0].value), r#"{"_raw":"x","via":"a"}"#);
    assert_eq!(b.sent().len(), 2, "the route with no condition takes offset 0 too");
}

/// A route condition on `__source.topic` splits one poll; the column is readable but never serialized; offsets commit per topic-partition.
#[test]
fn a_route_condition_on_topic_splits_a_poll_and_offsets_commit_per_topic() {
    let routes = vec![
        route_cfg(Some("__source.topic == \"orders\""), 0, deliver_to(&[0])),
        route_cfg(Some("__source.topic != \"orders\""), 1, deliver_to(&[1])),
    ];
    let pipelines = vec![
        pipeline(0, vec![code(0, "came_from = __source.topic;")]),
        pipeline(1, vec![]),
    ];
    let src = FakeSource::new([
        record_on("orders", "a", 0, 0),
        record_on("clicks", "b", 42, 0),
        record_on("orders", "c", 1, 0),
    ]);
    let mut w = worker(routes, pipelines, &src);
    let (orders, other) = (FakeDestination::new(), FakeDestination::new());
    w.add_destination(target(0, orders.clone()));
    w.add_destination(target(1, other.clone()));

    tick(&mut w).unwrap();

    let orders_sent = orders.sent();
    assert_eq!(orders_sent.len(), 2);
    assert_eq!(other.sent().len(), 1);
    let out = text(&orders_sent[0].value);
    assert!(out.contains(r#""came_from":"orders""#), "{out}");
    assert!(!out.contains("__source.topic"), "{out}");

    let committed = src.committed();
    assert_eq!(committed.len(), 1);
    let m = &committed[0];
    assert_eq!(m.len(), 2, "one entry per topic-partition: {m:?}");
    assert_eq!(m[&TopicPartition::new(Arc::from("orders"), 0)], 1);
    assert_eq!(m[&TopicPartition::new(Arc::from("clicks"), 0)], 42);
}

/// A source that declares nothing puts no metadata name in scope, so
/// reading one is an ordinary unknown field. With nothing upstream to
/// supply it, that is a compile error.
#[test]
fn a_source_declaring_no_metadata_refuses_a_pipeline_that_reads_it() {
    use smeltr::plan::topology::SourceMeta;
    let plan = compile(&pipeline(0, vec![code(0, "t = __source.topic;")]), &SourceMeta::NONE);
    let errs = plan.expect_err("__source.topic is not a field this source carries");
    assert!(
        errs.iter().any(|e| e.to_string().contains("__source.topic")),
        "{errs:?}"
    );
}

// -- delivery rules ---------------------------------------------------------

/// Rules are additive from one pipeline run, a destination named twice gets one copy, and one this worker lacks is skipped.
#[test]
fn deliver_rules_fan_out_additively_once_per_held_destination() {
    let routes = vec![route_with_deliver(vec![rule(None, &[0]), rule(None, &[0, 2, 3])])];
    let pipelines = vec![pipeline(0, vec![code(0, "x = 1;")])];
    let src = FakeSource::new([record("a", 10, 0)]);
    let mut w = worker(routes, pipelines, &src);
    // Out of id order and missing id 3: nothing promises
    // `destinations[i].id == i`.
    let (a, c) = (FakeDestination::new(), FakeDestination::new());
    w.add_destination(target(2, c.clone()));
    w.add_destination(target(0, a.clone()));

    tick(&mut w).unwrap();

    assert_eq!(a.sent().len(), 1);
    assert_eq!(c.sent().len(), 1);
    assert_eq!(a.sent()[0].value, c.sent()[0].value, "one serialization");
    assert_eq!(
        counter(&w, "smeltr_rows_unmatched_total", &[("source", "0"), ("cause", "rule")]),
        None,
        "a destination this worker lacks does not make the row unrouted"
    );
}

/// A destination id is a position in the bundle, not a bit: one past any word width delivers like any other.
#[test]
fn a_destination_id_past_a_word_width_delivers() {
    let routes = vec![route_with_deliver(vec![rule(None, &[70]), rule(None, &[3])])];
    let pipelines = vec![pipeline(0, vec![code(0, "x = 1;")])];
    let src = FakeSource::new([record("a", 10, 0), record("b", 11, 0)]);
    let mut w = worker(routes, pipelines, &src);
    let (far, near) = (FakeDestination::new(), FakeDestination::new());
    w.add_destination(target(70, far.clone()));
    w.add_destination(target(3, near.clone()));

    tick(&mut w).unwrap();

    assert_eq!(far.sent().len(), 2);
    assert_eq!(near.sent().len(), 2);
    assert_eq!(
        counter(&w, "smeltr_rows_unmatched_total", &[("source", "0"), ("cause", "rule")]),
        None
    );
}

/// A `when` reads the pipeline's output; a row matching two rules goes to both, one matching none is counted unrouted, and a null condition never matches.
#[test]
fn a_deliver_condition_selects_rows_by_a_computed_field() {
    let routes = dropping_declined(vec![route_with_deliver(vec![
        rule(Some("sev > 2"), &[0]),
        rule(Some("sev > 6"), &[1]),
    ])]);
    let pipelines = vec![pipeline(
        0,
        vec![
            code(0, "n: i64; Object.assign($, json(_raw));"),
            code(1, "sev = n * 2;"),
        ],
    )];
    let src = FakeSource::new([
        record(r#"{"n":5}"#, 0, 0),     // both
        record(r#"{"n":2}"#, 1, 0),     // first only
        record(r#"{"n":0}"#, 2, 0),     // neither
        record(r#"{"other":1}"#, 3, 0), // `sev` is null: neither
    ]);
    let mut w = worker(routes, pipelines, &src);
    let (a, b) = (FakeDestination::new(), FakeDestination::new());
    w.add_destination(target(0, a.clone()));
    w.add_destination(target(1, b.clone()));

    tick(&mut w).unwrap();

    assert_eq!(a.sent().len(), 2);
    let b_sent = b.sent();
    assert_eq!(b_sent.len(), 1);
    assert!(text(&b_sent[0].value).contains(r#""sev":10"#));
    assert_eq!(
        counter(&w, "smeltr_rows_unmatched_total", &[("source", "0"), ("cause", "rule")]),
        Some(2)
    );
}

/// A dropped row is delivered nowhere, whatever the rules say, and is counted under the function that dropped it.
#[test]
fn a_dropped_row_is_delivered_nowhere_and_counted_under_its_function() {
    let pipelines = vec![pipeline(0, vec![code(0, "if (__source.offset == 0) drop();")])];
    let src = FakeSource::new([record("a", 0, 0), record("b", 1, 0)]);
    let mut w = worker(plain_route(), pipelines, &src);
    let dest = FakeDestination::new();
    w.add_destination(target(0, dest.clone()));

    tick(&mut w).unwrap();

    assert_eq!(dest.sent().len(), 1);
    let dropped = |f: &str| counter(&w, "smeltr_rows_dropped_total", &[("pipeline", "0"), ("function", f)]);
    assert_eq!(dropped("0"), Some(1));
    assert_eq!(dropped("1"), None);
}

/// An `if`'s condition is its own step's work: the calls that check it, and a row error it raises, are counted
/// under the `if`.
#[test]
fn an_ifs_condition_is_counted_under_the_if() {
    let pipelines = vec![pipeline(
        0,
        vec![
            code(0, "n = length(_raw);"),
            code(3, "one: bool = null;"),
            // Divides by zero on an empty `_raw`.
            if_(1, "1 / n == 1", vec![code(2, "one = true;")]),
        ],
    )];
    let src = FakeSource::new([record("a", 0, 0), record("", 1, 0)]);
    let mut w = worker(plain_route(), pipelines, &src);
    w.add_destination(target(0, FakeDestination::new()));

    tick(&mut w).unwrap();

    let errored = |f: &str| counter(&w, "smeltr_rows_errored_total", &[("pipeline", "0"), ("function", f)]);
    assert_eq!(errored("1"), Some(1));
    assert_eq!(errored("0"), None);
    // Counted beside the time. The time can round to zero and go unrecorded; the count cannot.
    let calls = |f: &str| counter(&w, "smeltr_function_calls_total", &[("pipeline", "0"), ("function", f)]);
    assert!(calls("1").is_some());
}

/// An `else` region is the `if`'s work too. Both regions read the condition from one slot, so it runs once a row. No
/// step here holds `FunctionId` 0, so a region billed to 0 would show there.
#[test]
fn an_ifs_else_guard_is_counted_under_the_if() {
    let pipelines = vec![pipeline(
        0,
        vec![
            code(1, "n = length(_raw);"),
            code(5, "one: bool = null;"),
            // Divides by zero on an empty `_raw`.
            Step::If {
                id: 2,
                cond: "1 / n == 1".to_string(),
                then: vec![code(3, "one = true;")],
                else_: vec![code(4, "one = false;")],
            },
        ],
    )];
    let src = FakeSource::new([record("a", 0, 0), record("", 1, 0)]);
    let mut w = worker(plain_route(), pipelines, &src);
    w.add_destination(target(0, FakeDestination::new()));

    tick(&mut w).unwrap();

    let calls = |f: &str| counter(&w, "smeltr_function_calls_total", &[("pipeline", "0"), ("function", f)]);
    assert_eq!(
        calls("2"),
        Some(5),
        "the condition, then each branch's region, opened and closed"
    );
    assert_eq!(calls("0"), None);
    let errored = |f: &str| counter(&w, "smeltr_rows_errored_total", &[("pipeline", "0"), ("function", f)]);
    assert_eq!(errored("2"), Some(1), "the condition runs once a row, and fails on one");
    assert_eq!(errored("0"), None);
}

/// A condition that is null is false, so the `else` runs. The condition itself records no error.
#[test]
fn a_text_condition_takes_the_else_and_records_no_error() {
    let (w, sent) = run_on_text_flags(vec![
        code(1, r#"y = "prior";"#),
        Step::If {
            id: 2,
            cond: "flag".to_string(),
            then: vec![code(3, r#"y = "then";"#)],
            else_: vec![code(4, r#"y = "else";"#)],
        },
    ]);

    assert_eq!(sent.len(), 2);
    for row in &sent {
        assert_eq!(row["y"], "else");
    }
    let errored = |f: &str| counter(&w, "smeltr_rows_errored_total", &[("pipeline", "0"), ("function", f)]);
    assert_eq!(errored("2"), None);
}

/// A step's filter that is null selects no row, and records no error.
#[test]
fn a_text_filter_selects_no_row_and_records_no_error() {
    let mut filtered = code(2, r#"y = "filtered";"#);
    if let Step::Function(f) = &mut filtered {
        f.filter = Some("flag".into());
    }
    let (w, sent) = run_on_text_flags(vec![code(1, r#"y = "prior";"#), filtered]);

    assert_eq!(sent.len(), 2);
    for row in &sent {
        assert_eq!(row["y"], "prior");
    }
    let errored = |f: &str| counter(&w, "smeltr_rows_errored_total", &[("pipeline", "0"), ("function", f)]);
    assert_eq!(errored("2"), None);
}

/// A loop under a null condition runs no row, and the batch is delivered.
#[test]
fn a_loop_under_a_text_condition_runs_no_row() {
    let (w, sent) = run_on_text_flags(vec![
        code(1, "xs = [1, 2]; n = 0;"),
        if_(2, "flag", vec![code(3, "for (let x of xs) { n = n + x; }")]),
    ]);

    assert_eq!(counter(&w, "smeltr_batches_poisoned_total", &[("source", "0")]), None);
    assert_eq!(sent.len(), 2);
    for row in &sent {
        assert_eq!(row["n"], 0);
    }
}

/// A drop whose condition is null drops no row.
#[test]
fn a_text_drop_condition_drops_no_row() {
    let (_, sent) = run_on_text_flags(vec![code(1, "if (flag) drop();")]);

    assert_eq!(sent.len(), 2);
}

// -- control variables and document identity --------------------------------

/// `__destination.id` and the destination's own placement are read back at egress per row and never serialized as
/// data. A placement another kind of destination reads stays behind.
#[test]
fn pipeline_set_placement_and_id_reach_the_destination_unserialized() {
    let pipelines = vec![pipeline(
        0,
        vec![if_(
            1,
            "__source.offset == 0",
            vec![code(
                0,
                r#"__destination.topic = "urgent"; __destination.id = "orders-" + toString(__source.offset);
                   __destination.index = "urgent-index"; __destination.prefix = "urgent/";"#,
            )],
        )],
    )];
    let src = FakeSource::new([record("a", 0, 0), record("b", 1, 0)]);
    let mut w = worker(plain_route(), pipelines, &src);
    let dest = FakeDestination::new();
    w.add_destination(keyed(0, dest.clone(), None));

    tick(&mut w).unwrap();

    let sent = dest.sent();
    assert_eq!(sent.len(), 2);
    assert_eq!(sent[0].placement.as_deref(), Some("urgent-index"));
    assert_eq!(sent[0].doc_id.as_deref(), Some("orders-0"));
    assert_eq!(sent[0].key, None, "a doc id never arrives as a key");
    assert_eq!(text(&sent[0].value), r#"{"_raw":"a"}"#);
    assert_eq!(
        (sent[1].placement.as_deref(), sent[1].doc_id.as_deref()),
        (None, None),
        "row 1 set neither, so the destination's own defaults apply"
    );
}

/// A placement or id assigned a bare literal is one value for the whole batch; every row still carries it.
#[test]
fn a_literal_placement_and_id_reach_the_destination_on_every_row() {
    let pipelines = vec![pipeline(
        0,
        vec![code(0, r#"__destination.id = "fixed"; __destination.index = "logs";"#)],
    )];
    let src = FakeSource::new([record("a", 0, 0), record("b", 1, 0)]);
    let mut w = worker(plain_route(), pipelines, &src);
    let dest = FakeDestination::new();
    w.add_destination(keyed(0, dest.clone(), None));

    tick(&mut w).unwrap();

    let sent = dest.sent();
    assert_eq!(sent.len(), 2);
    for row in &sent {
        assert_eq!(row.placement.as_deref(), Some("logs"));
        assert_eq!(row.doc_id.as_deref(), Some("fixed"));
    }
}

/// `__source.topic`, `__source.partition` and `__source.offset` build a unique single-hop identity, downstream of a parser.
#[test]
fn a_records_source_coordinates_make_a_unique_id() {
    let pipelines = vec![pipeline(
        0,
        vec![
            code(0, "Object.assign($, json(_raw));"),
            code(
                1,
                r#"__destination.id = __source.topic + "-" + toString(__source.partition) + "-" + toString(__source.offset);"#,
            ),
        ],
    )];
    let src = FakeSource::new([record(r#"{"a":1}"#, 7, 0), record(r#"{"a":1}"#, 7, 1)]);
    let mut w = worker(plain_route(), pipelines, &src);
    let dest = FakeDestination::new();
    w.add_destination(keyed(0, dest.clone(), None));

    tick(&mut w).unwrap();

    let sent = dest.sent();
    assert_eq!(sent[0].doc_id.as_deref(), Some("t-0-7"));
    assert_eq!(
        sent[1].doc_id.as_deref(),
        Some("t-1-7"),
        "the partition keeps these apart"
    );
}

/// A Kafka record's headers are an object of key to text: a repeated key holds its last value, a header with no value
/// is no entry, and a value that is not UTF-8 is repaired and noted.
#[test]
fn a_records_headers_are_an_object_of_text() {
    let pipelines = vec![pipeline(
        0,
        vec![code(
            0,
            r#"trace = __source.headers.trace; tenant = __source.headers.tenant; bad = __source.headers.bad;
               none = __source.headers.missing ?? "none"; kind = __source.type;"#,
        )],
    )];
    let mut r = record("{}", 0, 0);
    r.headers = Some(vec![
        ("trace".to_string(), b"t1".to_vec()),
        ("tenant".to_string(), b"a".to_vec()),
        ("tenant".to_string(), b"b".to_vec()),
        ("bad".to_string(), vec![b'x', 0xff]),
    ]);
    let src = FakeSource::new([r]);
    let mut w = worker(plain_route(), pipelines, &src);
    let dest = FakeDestination::new();
    w.add_destination(target(0, dest.clone()));

    tick(&mut w).unwrap();

    let row = body(&dest.sent()[0].value);
    assert_eq!(row["trace"], "t1", "{row}");
    assert_eq!(row["tenant"], "b", "the last value of a repeated key: {row}");
    assert_eq!(row["bad"], "x\u{fffd}", "{row}");
    assert_eq!(row["none"], "none", "{row}");
    assert_eq!(row["kind"], "kafka", "{row}");
    assert!(row.get("__source").is_none(), "never delivered: {row}");
}

/// A source copies its headers only when something its records reach reads them: a route's condition or its pipeline.
#[test]
fn headers_are_read_where_a_route_or_its_pipeline_names_them() {
    let reads = |cond: Option<&str>, body: &str| {
        let routes = vec![route_cfg(cond, 0, deliver_to(&[0]))];
        let topology = compile_topology(&routes, &[pipeline(0, vec![code(0, body)])]).expect("compiles");
        topology.reads(0, "__source.headers")
    };
    assert!(!reads(None, "a = 1;"));
    assert!(reads(None, "a = __source.headers.trace;"));
    assert!(reads(Some(r#"__source.headers.trace != null"#), "a = 1;"));
}

/// A route chose the row by a repaired header, so the row carries the repair though its pipeline never reads it.
#[test]
fn a_header_repaired_for_a_route_condition_marks_the_row() {
    let routes = answering(
        tagging_repairs(),
        vec![route_cfg(Some("__source.headers.bad != null"), 0, deliver_to(&[0]))],
    );
    let pipelines = vec![pipeline(0, vec![code(0, "a = 1;")])];
    let mut r = record("{}", 0, 0);
    r.headers = Some(vec![("bad".to_string(), vec![b'x', 0xff])]);
    let src = FakeSource::new([r]);
    let mut w = worker(routes, pipelines, &src);
    let dest = FakeDestination::new();
    w.add_destination(target(0, dest.clone()));

    tick(&mut w).unwrap();

    let row = body(&dest.sent()[0].value);
    assert!(exception_text(&row).unwrap().contains("InvalidUtf8"), "{row}");
}

/// A reserved field resolves to the runtime's column after a dynamic parser, not to a slot the parser binds.
#[test]
fn a_reserved_field_is_still_readable_after_a_dynamic_parser() {
    for (expr, want) in [
        ("__source.topic", serde_json::json!("t")),
        ("toString(__source.offset)", serde_json::json!("7")),
        ("toString(__source.partition)", serde_json::json!("3")),
        ("_raw", serde_json::json!(r#"{"a":1}"#)),
    ] {
        let pipelines = vec![pipeline(
            0,
            vec![
                code(0, "Object.assign($, json(_raw));"),
                code(1, &format!("out = {expr};")),
            ],
        )];
        let src = FakeSource::new([record(r#"{"a":1}"#, 7, 3)]);
        let mut w = worker(plain_route(), pipelines, &src);
        let dest = FakeDestination::new();
        w.add_destination(target(0, dest.clone()));

        tick(&mut w).unwrap();

        let row = body(&dest.sent()[0].value);
        assert_eq!(row["out"], want, "reading {expr} after a parser: {row}");
        assert_eq!(row["a"], 1, "the parsed document survives: {row}");
        assert_eq!(row["_raw"], r#"{"a":1}"#, "_raw still serializes: {row}");
    }
}

/// Identity is `__destination.id`, else the destination's own `idField` read as text, else generated; a missing one is counted only where the destination keys on it.
#[test]
fn document_identity_resolves_per_destination() {
    let routes = vec![route_with_deliver(deliver_to(&[0, 1, 2, 3]))];
    let pipelines = vec![pipeline(
        0,
        vec![
            code(0, "Object.assign($, json(_raw));"),
            if_(
                2,
                "__source.offset == 0",
                vec![code(1, r#"__destination.id = "computed";"#)],
            ),
        ],
    )];
    let src = FakeSource::new([
        record(r#"{"event":{"id":"e-0"},"seq":4242}"#, 0, 0),
        record(r#"{"event":{"id":"e-1"},"seq":7}"#, 1, 0),
        record(r#"{"other":1}"#, 2, 0),
    ]);
    let mut w = worker(routes, pipelines, &src);
    let dests: Vec<FakeDestination> = (0..4).map(|_| FakeDestination::new()).collect();
    w.add_destination(keyed(0, dests[0].clone(), Some("event.id")));
    w.add_destination(keyed(1, dests[1].clone(), Some("seq")));
    w.add_destination(keyed(2, dests[2].clone(), None));
    w.add_destination(target(3, dests[3].clone()));

    tick(&mut w).unwrap();

    let ids = |d: &FakeDestination| -> Vec<Option<String>> { d.sent().iter().map(|r| r.doc_id.clone()).collect() };
    let s = |v: &str| Some(v.to_string());
    assert_eq!(
        ids(&dests[0]),
        vec![s("computed"), s("e-1"), None],
        "__destination.id wins, then the configured field"
    );
    assert_eq!(
        ids(&dests[1]),
        vec![s("computed"), s("7"), None],
        "a non-string field is read as its text"
    );
    assert_eq!(ids(&dests[2]), vec![s("computed"), None, None]);
    assert_eq!(
        ids(&dests[3]),
        vec![s("computed"), None, None],
        "__destination.id travels whether or not the destination keys on it"
    );
    for (dest, missing) in [("0", Some(1)), ("1", Some(1)), ("2", Some(2)), ("3", None)] {
        assert_eq!(
            counter(&w, "smeltr_rows_without_doc_id_total", &[("destination", dest)]),
            missing,
            "destination {dest}"
        );
    }
    assert!(
        dests.iter().all(|d| d.sent().len() == 3),
        "a missing identity never blocks delivery"
    );
}

// -- failed rows --------------------------------------------------------------

/// A failed row is tagged and goes on: the delivery rules place it by its tag field like any other field, which is how an
/// operator writes "as well as" and "instead of". It carries the tag wherever it lands and is counted by kind.
#[test]
fn a_tagged_row_is_placed_by_the_delivery_rules() {
    let copying = || {
        vec![route_with_deliver(vec![
            rule(None, &[0]),
            rule(Some("smeltr.exception != null"), &[1]),
        ])]
    };
    for (label, routes, want_main, want_dlq) in [
        ("goes on", plain_route(), 2, 0),
        ("instead of", diverting_route(), 1, 1),
        ("as well as", copying(), 2, 1),
    ] {
        let pipelines = vec![pipeline(0, vec![code(0, "status: i64; Object.assign($, json(_raw));")])];
        let src = FakeSource::new([record(r#"{"status":200}"#, 0, 0), record("[1,2,3]", 1, 0)]);
        let mut w = worker(routes, pipelines, &src);
        let (main, dlq) = (FakeDestination::new(), FakeDestination::new());
        w.add_destination(target(0, main.clone()));
        w.add_destination(target(1, dlq.clone()));

        tick(&mut w).unwrap();

        let (main_sent, dlq_sent) = (main.sent(), dlq.sent());
        assert_eq!(main_sent.len(), want_main, "{label}");
        assert_eq!(dlq_sent.len(), want_dlq, "{label}");
        let clean = body(&main_sent[0].value);
        assert_eq!(clean["status"], 200, "{label}");
        assert!(
            clean.pointer("/smeltr/exception").is_none(),
            "{label}: a clean row carries no tag"
        );
        for r in main_sent.iter().skip(1).chain(&dlq_sent) {
            let row = body(&r.value);
            assert!(exception_text(&row).is_some(), "{label}: {row}");
            assert!(exception_text(&row).is_some(), "{label}: {row}");
        }
        assert_eq!(
            counter(
                &w,
                "smeltr_rows_errored_total",
                &[("pipeline", "0"), ("function", "0"), ("kind", "Unparsable"),]
            ),
            Some(1),
            "{label}"
        );
    }
}

/// An exception tags its row with its message, a field that failed its type as much as a parse that read
/// nothing or an overflow.
#[test]
fn an_exception_tags_its_row_with_its_message() {
    let parser = || code(0, "status: i64; code: i64; Object.assign($, json(_raw));");
    for (label, steps, raw, offset) in [
        ("Unparsable", vec![parser()], "[1,2,3]", 0),
        ("UnparsableField", vec![parser()], r#"{"status":"x","code":7}"#, 0),
        ("Overflow", vec![code(0, "big = __source.offset + 1;")], "x", i64::MAX),
    ] {
        let src = FakeSource::new([record(raw, offset, 0)]);
        let mut w = worker(diverting_route(), vec![pipeline(0, steps)], &src);
        let (main, dlq) = (FakeDestination::new(), FakeDestination::new());
        w.add_destination(target(0, main.clone()));
        w.add_destination(target(1, dlq.clone()));

        tick(&mut w).unwrap();

        let (main_sent, dlq_sent) = (main.sent(), dlq.sent());
        assert_eq!((main_sent.len(), dlq_sent.len()), (0, 1), "{label}");
        let row = body(&dlq_sent[0].value);
        assert!(exception_text(&row).is_some(), "{label}: {row}");
        assert!(exception_text(&row).unwrap().contains(label), "{label}: {row}");
        if label == "UnparsableField" {
            assert!(
                row.get("status").is_none() && row["code"] == 7,
                "the bad field is null, the rest survives: {row}"
            );
            assert!(
                exception_text(&row).unwrap().contains("status"),
                "the message names the field: {row}"
            );
        }
    }
}

/// The tag is a field like any other, so a rule can place a failed row by what went wrong. It is null on a healthy
/// row, and a null condition is false, so the negation of a test on it takes the healthy row.
#[test]
fn a_deliver_rule_can_route_on_what_went_wrong() {
    let routes = vec![route_with_deliver(vec![
        rule(Some("smeltr.exception == null"), &[0]),
        rule(
            Some(r#"startsWith(smeltr.exception.exceptions[0].kind as str, "Unparsable")"#),
            &[1],
        ),
        rule(
            Some(r#"startsWith(smeltr.exception.exceptions[0].kind as str, "DivideByZero")"#),
            &[2],
        ),
        rule(
            Some(r#"!startsWith(smeltr.exception.exceptions[0].kind as str, "Unparsable")"#),
            &[3],
        ),
    ])];
    let pipelines = vec![pipeline(
        0,
        vec![
            code(0, "status: i64; Object.assign($, json(_raw));"),
            code(1, "ratio = 100 / status;"),
        ],
    )];
    let src = FakeSource::new([
        record(r#"{"status":200}"#, 0, 0),
        record("[1,2,3]", 1, 0),
        record(r#"{"status":0}"#, 2, 0),
    ]);
    let mut w = worker(routes, pipelines, &src);
    let dests: Vec<FakeDestination> = (0..4).map(|_| FakeDestination::new()).collect();
    for (id, d) in dests.iter().enumerate() {
        w.add_destination(target(id as DestId, d.clone()));
    }

    tick(&mut w).unwrap();

    let statuses = |d: &FakeDestination| -> Vec<serde_json::Value> {
        d.sent().iter().map(|r| body(&r.value)["status"].clone()).collect()
    };
    assert_eq!(statuses(&dests[0]), vec![serde_json::json!(200)], "healthy only");
    assert_eq!(statuses(&dests[1]), vec![serde_json::Value::Null], "unparseable");
    assert_eq!(statuses(&dests[2]), vec![serde_json::json!(0)], "divide by zero");
    assert_eq!(
        statuses(&dests[3]),
        vec![serde_json::json!(200), serde_json::json!(0)],
        "the healthy row's null message fails the test, so its negation holds"
    );
}

/// Run `raw` records through `steps` into one destination and return the bodies sent, in order.
fn run_rows(steps: Vec<Step>, raw: &[&str]) -> Vec<serde_json::Value> {
    run_pipeline_rows(pipeline(0, steps), raw)
}

fn run_pipeline_rows(p: PipelineConfig, raw: &[&str]) -> Vec<serde_json::Value> {
    run_answered_rows(OnException::default(), p, raw)
}

/// `run_pipeline_rows`, its route answering exceptions with `on_exception`.
fn run_answered_rows(on_exception: OnException, p: PipelineConfig, raw: &[&str]) -> Vec<serde_json::Value> {
    let routes = answering(on_exception, vec![route_with_deliver(vec![rule(None, &[0])])]);
    let pipelines = vec![p];
    let src = FakeSource::new(raw.iter().enumerate().map(|(i, r)| record(r, i as i64, 0)));
    let mut w = worker(routes, pipelines, &src);
    let d = FakeDestination::new();
    w.add_destination(target(0, d.clone()));
    tick(&mut w).unwrap();
    d.sent().iter().map(|r| body(&r.value)).collect()
}

/// Every branch computes on every row, but a row answers only for the branch it took: the divide a ternary guards
/// never fails the row it guards.
#[test]
fn a_ternary_shields_the_branch_a_row_did_not_take() {
    let rows = run_rows(
        vec![
            code(0, "status: i64; Object.assign($, json(_raw));"),
            code(1, "ratio = status == 0 ? 0 : 100 / status;"),
        ],
        &[r#"{"status":200}"#, r#"{"status":0}"#],
    );
    assert_eq!(rows[0]["ratio"], 0.5);
    assert_eq!(rows[1]["ratio"], 0.0, "{}", rows[1]);
    assert!(rows[1].pointer("/smeltr/exception").is_none(), "{}", rows[1]);
}

/// Retraction reaches only what the branch itself noted: a failure an earlier step put on the row stays.
#[test]
fn a_ternary_leaves_an_earlier_failure_on_the_row() {
    let rows = run_rows(
        vec![
            code(0, "status: i64; Object.assign($, json(_raw));"),
            code(1, "bare = 100 / status;"),
            code(2, "ratio = status == 0 ? 0 : 100 / status;"),
        ],
        &[r#"{"status":0}"#, r#"{"status":200}"#],
    );
    assert!(exception_text(&rows[0]).is_some(), "{}", rows[0]);
    assert_eq!(
        exception_text(&rows[0]).unwrap(),
        "DivideByZero: status is 0, which 100 / status divided by in bare",
        "{}",
        rows[0]
    );
    assert!(rows[1].pointer("/smeltr/exception").is_none(), "{}", rows[1]);
}

/// The other side of the same rule: a divide in the branch a row did take still fails it, on either side.
#[test]
fn a_ternary_keeps_the_exception_of_the_branch_a_row_took() {
    for expr in ["status == 0 ? 100 / status : 1", "status != 0 ? 1 : 100 / status"] {
        let rows = run_rows(
            vec![
                code(0, "status: i64; Object.assign($, json(_raw));"),
                code(1, &format!("ratio = {expr};")),
            ],
            &[r#"{"status":0}"#],
        );
        assert!(exception_text(&rows[0]).is_some(), "{expr}: {}", rows[0]);
        assert_eq!(
            exception_text(&rows[0]).unwrap(),
            "DivideByZero: status is 0, which 100 / status divided by in ratio",
            "{expr}: {}",
            rows[0]
        );
    }
}

/// A guarded step is the same selection, so it shields the same way.
#[test]
fn a_guarded_step_shields_the_rows_it_masks() {
    let rows = run_rows(
        vec![
            code(0, "status: i64; Object.assign($, json(_raw));"),
            code(3, "ratio: f64 = null;"),
            if_(2, "status != 0", vec![code(1, "ratio = 100 / status;")]),
        ],
        &[r#"{"status":200}"#, r#"{"status":0}"#],
    );
    assert_eq!(rows[0]["ratio"], 0.5);
    assert!(rows[1].get("ratio").is_none(), "{}", rows[1]);
    assert!(rows[1].pointer("/smeltr/exception").is_none(), "{}", rows[1]);
}

/// `&&`, `||` and `??` stop reading where JavaScript would: the right side answers only on the rows the left left open.
#[test]
fn a_short_circuit_shields_the_operand_a_row_never_reached() {
    let rows = run_rows(
        vec![
            code(0, "status: i64; Object.assign($, json(_raw));"),
            code(
                1,
                "big = status != 0 && 100 / status > 1; any = status == 0 || 100 / status > 1;",
            ),
        ],
        &[r#"{"status":50}"#, r#"{"status":0}"#],
    );
    assert_eq!(rows[0]["big"], true);
    assert_eq!(rows[1]["big"], false, "{}", rows[1]);
    assert_eq!(rows[1]["any"], true, "{}", rows[1]);
    assert!(rows[1].pointer("/smeltr/exception").is_none(), "{}", rows[1]);
}

/// A parser under a guard parses every row, but a masked row does not answer for a declared field that did not fit.
#[test]
fn a_guarded_parser_shields_the_rows_it_masks() {
    let grok = code(
        1,
        r#"code: i64 = code; Object.assign($, grok(text, ["%{WORD:code}"]));"#,
    );
    let rows = run_rows(
        vec![
            code(0, "kind: str; text: str; Object.assign($, json(_raw));"),
            if_(2, r#"kind == "num""#, vec![grok]),
        ],
        &[r#"{"kind":"num","text":"x"}"#, r#"{"kind":"word","text":"x"}"#],
    );
    // A declared field that did not fit is tagged with a message naming it.
    assert_eq!(
        exception_text(&rows[0]).unwrap(),
        "UnparsableField: text.code is \"x\", which could not parse as i64 in code",
        "{}",
        rows[0]
    );
    assert!(rows[1].pointer("/smeltr/exception").is_none(), "{}", rows[1]);
}

/// The tag field is the group's or the route's to choose. A dotted name nests, beside the pipeline's own fields under the same
/// parent, and a rule reads it by that name.
#[test]
fn a_route_names_its_own_tag_field() {
    let p = pipeline(
        0,
        vec![
            code(0, "status: i64; Object.assign($, json(_raw));"),
            code(1, r#"siem.source = "smeltr"; ratio = 100 / status;"#),
        ],
    );
    let mut route = route_with_deliver(vec![
        rule(Some("siem.reason == null"), &[0]),
        rule(Some("siem.reason != null"), &[1]),
    ]);
    route.on_exception.tag_field = "siem.reason".to_string();
    let routes = vec![route];
    let src = FakeSource::new([record(r#"{"status":2}"#, 0, 0), record(r#"{"status":0}"#, 1, 0)]);
    let mut w = worker(routes, vec![p], &src);
    let (main, dlq) = (FakeDestination::new(), FakeDestination::new());
    w.add_destination(target(0, main.clone()));
    w.add_destination(target(1, dlq.clone()));

    tick(&mut w).unwrap();

    assert_eq!(main.sent().len(), 1);
    let failed = dlq.sent();
    assert_eq!(failed.len(), 1);
    let row = body(&failed[0].value);
    assert_eq!(
        row["siem"]["source"], "smeltr",
        "the pipeline's own field under the same parent: {row}"
    );
    assert_eq!(row["siem"]["reason"]["exceptions"][0]["kind"], "DivideByZero", "{row}");
    assert!(row.get("smeltr").is_none(), "{row}");
}

/// A rule reads the row as it will be sent: the tag on a tagged row, and on any other what the row holds at the tag
/// field's name, whether the pipeline wrote it or it came in among the record's unclaimed fields.
#[test]
fn a_rule_reads_the_tag_field_as_the_row_holds_it() {
    for (label, steps, clean) in [
        (
            "written",
            r#"n: i64; Object.assign($, json(_raw)); verdict = "ok"; ratio = 100 / n;"#,
            r#"{"n":2}"#,
        ),
        (
            "unclaimed",
            "n: i64; Object.assign($, json(_raw)); ratio = 100 / n;",
            r#"{"n":2,"verdict":"ok"}"#,
        ),
    ] {
        let mut route = route_with_deliver(vec![
            rule(Some(r#"verdict as str == "ok""#), &[0]),
            rule(Some(r#"verdict as str != "ok""#), &[1]),
        ]);
        route.on_exception.tag_field = "verdict".to_string();
        let src = FakeSource::new([record(clean, 0, 0), record(r#"{"n":0,"verdict":"ok"}"#, 1, 0)]);
        let mut w = worker(vec![route], vec![pipeline(0, vec![code(0, steps)])], &src);
        let (main, other) = (FakeDestination::new(), FakeDestination::new());
        w.add_destination(target(0, main.clone()));
        w.add_destination(target(1, other.clone()));

        tick(&mut w).unwrap();

        let (main_sent, other_sent) = (main.sent(), other.sent());
        assert_eq!(main_sent.len(), 1, "{label}");
        assert_eq!(body(&main_sent[0].value)["verdict"], "ok", "{label}");
        assert_eq!(other_sent.len(), 1, "{label}");
        let tagged = body(&other_sent[0].value);
        assert_eq!(
            tagged["verdict"]["exceptions"][0]["kind"], "DivideByZero",
            "{label}: {tagged}"
        );
    }
}

/// A rule reading the tag's parent whole sees the tag in it.
#[test]
fn a_rule_reading_the_tags_parent_sees_the_tag() {
    let p = pipeline(
        0,
        vec![code(
            0,
            r#"n: i64; Object.assign($, json(_raw)); siem.source = "smeltr"; ratio = 100 / n;"#,
        )],
    );
    let mut route = route_with_deliver(vec![
        rule(Some("length(keys(siem)) == 1"), &[0]),
        rule(Some("length(keys(siem)) == 2"), &[1]),
    ]);
    route.on_exception.tag_field = "siem.reason".to_string();
    let src = FakeSource::new([record(r#"{"n":2}"#, 0, 0), record(r#"{"n":0}"#, 1, 0)]);
    let mut w = worker(vec![route], vec![p], &src);
    let (main, dlq) = (FakeDestination::new(), FakeDestination::new());
    w.add_destination(target(0, main.clone()));
    w.add_destination(target(1, dlq.clone()));

    tick(&mut w).unwrap();

    assert_eq!(main.sent().len(), 1);
    assert_eq!(dlq.sent().len(), 1);
}

/// A key a rename took away stays away when the tag writes under it: the tag's object holds only the tag's fields.
#[test]
fn the_tag_under_a_renamed_away_key_does_not_bring_the_key_back() {
    let steps = vec![
        code(0, "status: i64; Object.assign($, json(_raw));"),
        code(1, "error.x = 1;"),
        code(2, "move(error, err);"),
        code(3, "ratio = 100 / status;"),
    ];
    let rows = run_answered_rows(
        messaging_at_error(),
        pipeline(0, steps),
        &[r#"{"status":0,"error":{"k":1}}"#],
    );
    assert_eq!(rows[0]["err"], serde_json::json!({"k": 1, "x": 1}), "{}", rows[0]);
    let error = rows[0]["error"].as_object().expect("the tag's object");
    assert_eq!(error.keys().collect::<Vec<_>>(), ["message"], "{}", rows[0]);
}

/// Under drop, a failed row is counted and let go; the healthy rows of the same batch are delivered and the batch
/// commits.
#[test]
fn a_pipeline_set_to_drop_lets_a_failed_row_go() {
    let p = pipeline(0, vec![code(0, "status: i64; Object.assign($, json(_raw));")]);
    let src = FakeSource::new([record(r#"{"status":200}"#, 0, 0), record("[1,2,3]", 1, 0)]);
    let mut w = worker(failing(Answer::Drop), vec![p], &src);
    let dest = FakeDestination::new();
    w.add_destination(target(0, dest.clone()));

    tick(&mut w).unwrap();

    let sent = dest.sent();
    assert_eq!(sent.len(), 1);
    assert_eq!(body(&sent[0].value)["status"], 200);
    assert_eq!(
        counter(
            &w,
            "smeltr_rows_dropped_total",
            &[("source", "0"), ("reason", "exception")]
        ),
        Some(1)
    );
    assert_eq!(src.committed(), vec![[(tp(0), 1i64)].into_iter().collect()]);
}

/// Each kind has its own answer, and a row takes the strongest of its kinds': an ignored kind leaves its row as it is,
/// but the same row is dropped when a dropped kind marks it too.
#[test]
fn a_row_takes_the_strongest_answer_of_its_kinds() {
    let p = pipeline(
        0,
        vec![code(
            0,
            "status: i64; Object.assign($, json(_raw)); ratio = status == null ? 0 : 100 / status;",
        )],
    );
    let mut on_exception = OnException::default();
    on_exception.answers[ExceptionKind::NaN.index()] = Answer::Ignore;
    on_exception.answers[ExceptionKind::DivideByZero.index()] = Answer::Drop;
    let src = FakeSource::new([
        record(r#"{"status":2,"v":NaN}"#, 0, 0),
        record(r#"{"status":0,"v":NaN}"#, 1, 0),
        record(r#"{"status":"x"}"#, 2, 0),
    ]);
    let mut w = worker(answering(on_exception, plain_route()), vec![p], &src);
    let dest = FakeDestination::new();
    w.add_destination(target(0, dest.clone()));

    tick(&mut w).unwrap();

    let rows: Vec<serde_json::Value> = dest.sent().iter().map(|r| body(&r.value)).collect();
    assert_eq!(rows.len(), 2, "the row a divide by zero marked is dropped: {rows:?}");
    assert!(rows[0].pointer("/smeltr/exception").is_none(), "{}", rows[0]);
    assert_eq!(
        exception_text(&rows[1]).unwrap(),
        "UnparsableField: _raw.status is \"x\", which could not parse as i64 in status",
        "the default tags: {}",
        rows[1]
    );
    assert_eq!(
        counter(&w, "smeltr_rows_errored_total", &[("kind", "NaN")]),
        Some(2),
        "an ignored kind still counts"
    );
}

/// A dropped row is counted once the batch is handed off, not on each attempt: a batch a destination would not take
/// is run again, and counting at the pipeline would count the same row twice.
#[test]
fn a_dropped_failed_row_is_counted_once_across_a_retry() {
    let p = pipeline(0, vec![code(0, "status: i64; Object.assign($, json(_raw));")]);
    let src = FakeSource::new([record(r#"{"status":200}"#, 0, 0), record("[1,2,3]", 1, 0)]);
    let mut w = worker(failing(Answer::Drop), vec![p], &src);
    let dest = FakeDestination::new();
    dest.fail_next_sends(1);
    w.add_destination(target(0, dest.clone()));
    let dropped = |w: &Worker| {
        counter(
            w,
            "smeltr_rows_dropped_total",
            &[("source", "0"), ("reason", "exception")],
        )
    };

    tick(&mut w).expect_err("the destination fails the first hand-over");
    assert_eq!(
        dropped(&w),
        None,
        "an attempt that did not go through drops nothing yet"
    );
    tick(&mut w).unwrap();
    assert_eq!(dropped(&w), Some(1), "counted once, on the attempt that went through");
    assert_eq!(dest.sent().len(), 1);
}

/// Under block, a failed row stops the source and retrying does not move it: the same rows fail the same way.
/// Nothing is sent, nothing commits, and the reason names the pipeline and the fixes. The batch is held, not re-run:
/// the pipeline runs once, so the error count is not inflated by retries.
#[test]
fn a_pipeline_set_to_block_blocks_the_source() {
    let mut p = pipeline(0, vec![code(0, "status: i64; Object.assign($, json(_raw));")]);
    p.step_ids = vec!["parse".to_string()];
    let src = FakeSource::new([record(r#"{"status":200}"#, 0, 0), record("[1,2,3]", 1, 0)]);
    let mut w = worker(failing(Answer::Block), vec![p], &src);
    let dest = FakeDestination::new();
    w.add_destination(target(0, dest.clone()));

    let e = tick(&mut w).expect_err("block fails the tick so nothing commits");
    let e = e.to_string();
    assert!(
        e.contains("pipeline pipeline-0 marked 1 record(s)"),
        "names the pipeline: {e}"
    );
    assert!(
        e.contains(r#"at step parse: _raw is "[1,2,3]", which json could not parse"#),
        "names the step and what went wrong: {e}"
    );
    for attempt in 0..3 {
        tick(&mut w).expect("a held batch is not run again");
        assert!(src.is_blocked(), "{attempt}");
        assert_eq!(blocked(&w), Some(1.0), "{attempt}");
        let why = w.reader.block_reason.as_deref().expect("a blocked source says why");
        assert!(
            why.contains("fix it") && why.contains("at step parse"),
            "{attempt}: {why}"
        );
    }
    assert!(dest.sent().is_empty(), "the healthy row is not sent either");
    assert!(src.committed().is_empty());
    assert_eq!(
        counter(
            &w,
            "smeltr_rows_errored_total",
            &[("pipeline", "0"), ("function", "0"), ("kind", "Unparsable")]
        ),
        Some(1),
        "the pipeline ran once"
    );
}

/// A route that blocks after another has run leaves nothing of that route's output sent: it was buffered, and the
/// block discards it. Nor is it counted sent.
#[test]
fn a_block_on_a_later_route_sends_and_counts_nothing_from_an_earlier_one() {
    let blocking = RouteConfig {
        on_exception: OnException::every(Answer::Block),
        ..route_cfg(None, 1, deliver_to(&[1]))
    };
    let routes = vec![route_cfg(None, 0, deliver_to(&[0])), blocking];
    let parsing = pipeline(1, vec![code(0, "status: i64; Object.assign($, json(_raw));")]);
    let src = FakeSource::new([record(r#"{"status":200}"#, 0, 0), record("[1,2,3]", 1, 0)]);
    let mut w = worker(routes, vec![pipeline(0, vec![]), parsing], &src);
    let (first, second) = (AsyncDestination::new(), AsyncDestination::new());
    w.add_destination(target(0, first.clone()));
    w.add_destination(target(1, second.clone()));

    tick(&mut w).expect_err("the second route blocks");
    tick(&mut w).expect("a held batch is not run again");

    assert!(first.units().is_empty(), "the first route's rows never left");
    assert_eq!(counter(&w, "smeltr_rows_sent_total", &[("destination", "0")]), None);
}

/// Under block, a batch with no failed row is delivered and committed as usual.
#[test]
fn a_pipeline_set_to_block_passes_a_healthy_batch() {
    let p = pipeline(0, vec![code(0, "status: i64; Object.assign($, json(_raw));")]);
    let src = FakeSource::new([record(r#"{"status":200}"#, 0, 0)]);
    let mut w = worker(failing(Answer::Block), vec![p], &src);
    let dest = FakeDestination::new();
    w.add_destination(target(0, dest.clone()));

    tick(&mut w).unwrap();

    assert_eq!(dest.sent().len(), 1);
    assert_eq!(src.committed(), vec![[(tp(0), 0i64)].into_iter().collect()]);
}

/// An unroll delivers every element from one source record, flushed and committed once.
#[test]
fn an_unroll_fan_out_delivers_every_element_and_commits_once() {
    let pipelines = vec![pipeline(
        0,
        vec![
            code(0, "tags: array<i64>; Object.assign($, json(_raw));"),
            code(1, "unroll(tags);"),
        ],
    )];
    let src = FakeSource::new([record(r#"{"tags":[1,2,3,4,5]}"#, 0, 0)]);
    let mut w = worker(plain_route(), pipelines, &src);
    let dest = FakeDestination::new();
    w.add_destination(target(0, dest.clone()));

    let n = tick(&mut w).unwrap();
    assert_eq!(n, 1, "one source record, whatever its fan-out");

    let values: Vec<i64> = dest
        .sent()
        .iter()
        .map(|r| body(&r.value)["tags"].as_i64().unwrap())
        .collect();
    assert_eq!(values, vec![1, 2, 3, 4, 5]);
    assert_eq!(dest.flushes(), 1);
    assert_eq!(src.committed(), vec![[(tp(0), 0i64)].into_iter().collect()]);
}

/// An error recorded before an unroll follows every row the source row fanned out into.
#[test]
fn a_failure_recorded_before_unroll_follows_every_row_it_fanned_out_into() {
    let pipelines = vec![pipeline(
        0,
        vec![
            code(0, "status: i64; tags: array<i64>; Object.assign($, json(_raw));"),
            code(1, "boom = status / 0;"),
            code(2, "unroll(tags);"),
        ],
    )];
    let src = FakeSource::new([record(r#"{"status":1,"tags":[1,2,3]}"#, 0, 0)]);
    let mut w = worker(diverting_route(), pipelines, &src);
    let (main, dlq) = (FakeDestination::new(), FakeDestination::new());
    w.add_destination(target(0, main.clone()));
    w.add_destination(target(1, dlq.clone()));

    tick(&mut w).unwrap();

    assert!(main.sent().is_empty(), "no unrolled row looks healthy");
    let dlq_sent = dlq.sent();
    assert_eq!(dlq_sent.len(), 3);
    for r in &dlq_sent {
        assert!(exception_text(&body(&r.value)).is_some());
    }
}

// -- delivered exceptions -----------------------------------------------------

/// `plain_route`, answering each kind in `kinds` as it says and every other kind by default.
fn answering_kind(kinds: &[(ExceptionKind, Answer)]) -> Vec<RouteConfig> {
    let mut on_exception = OnException::default();
    for (kind, answer) in kinds {
        on_exception.answers[kind.index()] = answer.clone();
    }
    answering(on_exception, plain_route())
}

/// A row whose kind is answered with deliver goes to its targets instead of where the deliver rules send it, in the
/// form its target writes. As the record, it is the row as serialized, its tag saying where it failed and why; as
/// source bytes, it is the source record's bytes exactly; text, its `_raw`.
#[test]
fn a_delivered_row_reaches_its_target_and_not_the_deliver_rules() {
    for payload in [Payload::Record, Payload::source_bytes(), raw_text()] {
        let mut p = pipeline(
            0,
            vec![
                code(0, "status: i64; Object.assign($, json(_raw));"),
                code(1, "ratio = 100 / status;"),
            ],
        );
        p.step_ids = vec!["parse".to_string(), "ratio".to_string()];
        let routes = answering_kind(&[(ExceptionKind::DivideByZero, deliver(&[1]))]);
        // The failed row first, so its target's set is interned before the healthy row's.
        let src = FakeSource::new([record(r#"{"status": 0}"#, 0, 0), record(r#"{"status":2}"#, 1, 0)]);
        let mut w = worker(routes, vec![p], &src);
        writes(&mut w, 1, payload.clone());
        let (main, failed) = (FakeDestination::new(), FakeDestination::new());
        w.add_destination(target(0, main.clone()));
        w.add_destination(target(1, failed.clone()));

        tick(&mut w).unwrap();

        let main = main.sent();
        assert_eq!(main.len(), 1, "{payload:?}: only the healthy row");
        assert_eq!(body(&main[0].value)["status"], 2, "{payload:?}");
        let failed = failed.sent();
        assert_eq!(failed.len(), 1, "{payload:?}");
        match payload {
            Payload::Record => {
                let row = body(&failed[0].value);
                assert_eq!(row["status"], 0, "the pipeline's row: {row}");
                assert_eq!(
                    row["smeltr"]["exception"],
                    serde_json::json!({
                        "route": "r",
                        "pipeline": "pipeline-0",
                        "exceptions": [{
                            "step": "ratio",
                            "kind": "DivideByZero",
                            "message": "status is 0, which 100 / status divided by in ratio",
                        }],
                    }),
                    "{row}"
                );
            }
            Payload::Bytes { .. } | Payload::Text { .. } => {
                assert_eq!(failed[0].value, br#"{"status": 0}"#, "verbatim")
            }
        }
        assert_eq!(failed[0].payload, payload, "{payload:?}");
        assert_eq!(src.committed().len(), 1, "{payload:?}");
    }
}

/// An unroll fans one record into many rows. To a destination that writes source bytes, the record goes once, whole,
/// however many of its rows are answered so; to one that writes the record, each row goes as itself.
#[test]
fn source_bytes_survive_an_unroll() {
    for (payload, copies) in [(Payload::source_bytes(), 1), (Payload::Record, 2)] {
        let p = pipeline(
            0,
            vec![
                code(0, "status: i64; tags: array<i64>; Object.assign($, json(_raw));"),
                code(1, "boom = status / 0;"),
                code(2, "unroll(tags);"),
            ],
        );
        let routes = answering_kind(&[(ExceptionKind::DivideByZero, deliver(&[1]))]);
        let raw = r#"{"status":1,"tags":[1,2]}"#;
        let src = FakeSource::new([record(raw, 0, 0)]);
        let mut w = worker(routes, vec![p], &src);
        writes(&mut w, 1, payload.clone());
        let (main, failed) = (FakeDestination::new(), FakeDestination::new());
        w.add_destination(target(0, main.clone()));
        w.add_destination(target(1, failed.clone()));

        tick(&mut w).unwrap();

        assert!(main.sent().is_empty());
        let sent = failed.sent();
        assert_eq!(sent.len(), copies, "{payload:?}");
        if payload == Payload::source_bytes() {
            assert_eq!(sent[0].value, raw.as_bytes(), "the whole record");
        }
    }
}

/// A destination that writes a field's text gets that field's value alone. A row without the field is refused, so its
/// destination's answer to refusals takes it.
#[test]
fn a_text_destination_writes_its_field_and_refuses_a_row_without_it() {
    let p = pipeline(0, vec![code(0, r#"msg: str; Object.assign($, json(_raw));"#)]);
    let src = FakeSource::new([record(r#"{"msg":"hello"}"#, 0, 0), record(r#"{"other":1}"#, 1, 0)]);
    let mut w = worker_with_dlqs(plain_route(), vec![p], &src, &[(0, vec![1])]);
    writes(&mut w, 0, Payload::Text { field: "msg".into() });
    let (main, dlq) = (FakeDestination::new(), FakeDestination::new());
    w.add_destination(target_with(0, main.clone(), salvage_to(&[1])));
    w.add_destination(target_with(1, dlq.clone(), Answer::Drop));

    tick(&mut w).unwrap();

    let sent = main.sent();
    assert_eq!(sent.len(), 1, "only the row with msg");
    assert_eq!(text(&sent[0].value), "hello");
    let salvaged = dlq.sent();
    assert_eq!(salvaged.len(), 1, "the row without msg");
    assert_eq!(body(&salvaged[0].value)["other"], 1, "{}", text(&salvaged[0].value));
    assert_eq!(src.committed().len(), 1);
}

/// A destination that writes source bytes gets each source record's bytes once, untouched by the pipeline.
#[test]
fn a_source_bytes_destination_gets_the_source_bytes() {
    let p = pipeline(0, vec![code(0, "added = 1;")]);
    let raw = r#"{"a": 1}"#;
    let src = FakeSource::new([record(raw, 0, 0)]);
    let mut w = worker(plain_route(), vec![p], &src);
    writes(&mut w, 0, Payload::source_bytes());
    let dest = FakeDestination::new();
    w.add_destination(target(0, dest.clone()));

    tick(&mut w).unwrap();

    let sent = dest.sent();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].value, raw.as_bytes());
}

/// A record unrolled into many rows keeps its source form once, shared by every row, not a copy per row.
#[test]
fn an_unrolled_record_keeps_one_copy_of_its_source() {
    let p = pipeline(
        0,
        vec![
            code(0, "tags: array<i64>; Object.assign($, json(_raw));"),
            code(1, "unroll(tags);"),
        ],
    );
    let src = FakeSource::new([record(r#"{"tags":[1,2,3]}"#, 0, 0)]);
    let mut w = worker_with_dlqs(plain_route(), vec![p], &src, &[(0, vec![1])]);
    writes(&mut w, 1, Payload::source_bytes());
    let main = FakeDestination::new();
    w.add_destination(target_with(0, main.clone(), deliver(&[1])));
    w.add_destination(target(1, FakeDestination::new()));

    tick(&mut w).unwrap();

    let sent = main.sent();
    assert_eq!(sent.len(), 3);
    let forms: Vec<_> = sent.iter().map(|r| Arc::clone(&r.kept[0].1)).collect();
    assert_eq!(&*forms[0], br#"{"tags":[1,2,3]}"#);
    assert!(forms.iter().all(|f| Arc::ptr_eq(f, &forms[0])), "one copy");
}

/// A row two kinds deliver to different targets reaches every target of both.
#[test]
fn a_row_two_kinds_deliver_reaches_both_targets() {
    let p = pipeline(
        0,
        vec![code(
            0,
            "status: i64; Object.assign($, json(_raw)); ratio = 100 / status;",
        )],
    );
    let routes = answering_kind(&[
        (ExceptionKind::NaN, deliver(&[1])),
        (ExceptionKind::DivideByZero, deliver(&[2])),
    ]);
    let raw = r#"{"status":0,"v":NaN}"#;
    let src = FakeSource::new([record(raw, 0, 0)]);
    let mut w = worker(routes, vec![p], &src);
    let dests: Vec<FakeDestination> = (0..3).map(|_| FakeDestination::new()).collect();
    for (id, d) in dests.iter().enumerate() {
        w.add_destination(target(id as DestId, d.clone()));
    }
    writes(&mut w, 1, Payload::source_bytes());
    writes(&mut w, 2, Payload::source_bytes());

    tick(&mut w).unwrap();

    assert!(dests[0].sent().is_empty(), "not where the deliver rules send it");
    for d in &dests[1..] {
        let sent = d.sent();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].value, raw.as_bytes());
    }
}

// -- unmatched rows -----------------------------------------------------------

/// Under deliver, a row no route took goes to the unmatched destination as it arrived, and is counted either way.
/// Routing leaves `__live` holding every row: what makes a row unmatched is the union of the routes' own masks.
#[test]
fn a_row_no_route_took_reaches_the_unmatched_destination_as_source_bytes() {
    let routes = vec![route_cfg(Some("__source.offset == 0"), 0, deliver_to(&[0]))];
    let pipelines = vec![pipeline(0, vec![code(0, r#"via = "a";"#)])];
    let src = FakeSource::new([record("x", 0, 0), record("y", 1, 0)]);
    let mut w = worker_with_unmatched(routes, pipelines, &src, deliver(&[1]));
    let (routed, leftover) = (FakeDestination::new(), FakeDestination::new());
    w.add_destination(target(0, routed.clone()));
    w.add_destination(target(1, leftover.clone()));
    writes(&mut w, 1, Payload::source_bytes());

    tick(&mut w).unwrap();

    let routed = routed.sent();
    assert_eq!(routed.len(), 1, "only offset 0 matched");
    assert_eq!(text(&routed[0].value), r#"{"_raw":"x","via":"a"}"#);

    let leftover = leftover.sent();
    assert_eq!(leftover.len(), 1, "offset 1 matched nothing");
    assert_eq!(leftover[0].value, b"y", "as it arrived: no pipeline ran over it");

    assert_eq!(
        counter(
            &w,
            "smeltr_rows_unmatched_total",
            &[("source", "0"), ("cause", "route")]
        ),
        Some(1)
    );
    assert_eq!(counter(&w, "smeltr_rows_sent_total", &[("destination", "1")]), Some(1));
    assert_eq!(counter(&w, "smeltr_rows_dropped_total", &[("source", "0")]), None);
    assert_eq!(
        src.committed().len(),
        1,
        "an unmatched row does not withhold the commit"
    );
}

/// To destinations that write the record, an unmatched row goes as the document a pipeline with no steps would write
/// for it: its text as `_raw`.
#[test]
fn an_unmatched_row_delivered_as_record_reaches_every_destination_as_its_raw() {
    let src = FakeSource::new([record(r#"say "hi""#, 0, 0)]);
    let mut w = worker_with_unmatched(Vec::new(), Vec::new(), &src, deliver(&[0, 1]));
    let (one, two) = (FakeDestination::new(), FakeDestination::new());
    w.add_destination(target(0, one.clone()));
    w.add_destination(target(1, two.clone()));

    tick(&mut w).unwrap();

    for (dest, d) in [("0", &one), ("1", &two)] {
        let sent = d.sent();
        assert_eq!(sent.len(), 1, "{dest}");
        assert_eq!(text(&sent[0].value), r#"{"_raw":"say \"hi\""}"#, "{dest}");
        assert_eq!(sent[0].payload, Payload::Record, "{dest}");
        assert_eq!(
            counter(&w, "smeltr_rows_sent_total", &[("destination", dest)]),
            Some(1),
            "{dest}"
        );
    }
    assert_eq!(src.committed().len(), 1);
}

/// To a destination that writes `_raw`'s text, an unmatched row goes as its decoded text; to one that writes
/// source bytes, as it arrived. A field the row has none of refuses it.
#[test]
fn an_unmatched_row_goes_as_its_destination_writes_it() {
    for payload in [raw_text(), Payload::source_bytes()] {
        let src = FakeSource::new([record(r#"say "hi""#, 0, 0)]);
        let mut w = worker_with_unmatched(Vec::new(), Vec::new(), &src, deliver(&[0]));
        writes(&mut w, 0, payload.clone());
        let dest = FakeDestination::new();
        w.add_destination(target(0, dest.clone()));
        tick(&mut w).unwrap();
        let sent = dest.sent();
        assert_eq!(
            (text(&sent[0].value), &sent[0].payload),
            (r#"say "hi""#, &payload),
            "{payload:?}"
        );
    }
}

/// Under drop the row is dropped, and counted.
#[test]
fn an_unmatched_row_under_drop_is_dropped_and_counted() {
    let routes = vec![route_cfg(Some("__source.offset == 0"), 0, deliver_to(&[0]))];
    let src = FakeSource::new([record("x", 0, 0), record("y", 1, 0)]);
    let mut w = worker_with_unmatched(routes, vec![pipeline(0, vec![])], &src, Answer::Drop);
    let dest = FakeDestination::new();
    w.add_destination(target(0, dest.clone()));

    tick(&mut w).unwrap();

    assert_eq!(dest.sent().len(), 1);
    assert_eq!(
        counter(
            &w,
            "smeltr_rows_unmatched_total",
            &[("source", "0"), ("cause", "route")]
        ),
        Some(1)
    );
    assert_eq!(counter(&w, "smeltr_rows_dropped_total", &[("source", "0")]), Some(1));
}

/// Under block, the default, an unmatched row stops the batch before any route runs. The batch is held, not re-run:
/// the same rows would meet the same routes. Nothing is sent, nothing commits, and the reason names both fixes.
#[test]
fn an_unmatched_row_under_block_blocks_the_source_before_any_route_runs() {
    let routes = vec![route_cfg(Some("__source.offset == 0"), 0, deliver_to(&[0]))];
    let src = FakeSource::new([record("x", 0, 0), record("y", 1, 0)]);
    let mut w = worker(routes, vec![pipeline(0, vec![])], &src);
    let dest = FakeDestination::new();
    w.add_destination(target(0, dest.clone()));

    let e = tick(&mut w).expect_err("block fails the tick so nothing commits");
    assert!(e.to_string().contains("matched no route"), "{e}");
    let routed = counter(&w, "smeltr_route_matched_total", &[("source", "0")]);
    assert!(routed.is_some(), "the first turn evaluated the routes");
    for attempt in 0..3 {
        tick(&mut w).expect("a held batch is not run again");
        assert!(src.is_blocked(), "{attempt}");
        assert_eq!(blocked(&w), Some(1.0), "{attempt}");
        let why = w.reader.block_reason.as_deref().expect("a blocked source says why");
        assert!(
            why.contains("add a route"),
            "{attempt}: the reason, not a guess over it: {why}"
        );
        assert_eq!(w.reader.block_cause, Some(BlockCause::Held), "{attempt}");
    }
    assert!(dest.sent().is_empty(), "the matched row is not sent either");
    assert!(src.committed().is_empty());
    assert_eq!(
        counter(&w, "smeltr_route_matched_total", &[("source", "0")]),
        routed,
        "the routes were not evaluated again"
    );
    assert_eq!(counter(&w, "smeltr_rows_dropped_total", &[("source", "0")]), None);
}

/// A source no route names at all, under block: its batch waits on no destination, and must still not commit or
/// release the block while transform holds it.
#[test]
fn a_source_with_no_routes_under_block_stays_blocked_and_commits_nothing() {
    let src = FakeSource::new([record("x", 0, 0), record("y", 1, 0)]);
    let mut w = worker_with_unmatched(vec![], vec![], &src, Answer::Block);

    let e = tick(&mut w).expect_err("block fails the tick");
    assert!(e.to_string().contains("matched no route"), "{e}");
    for attempt in 0..3 {
        tick(&mut w).expect("a held batch is not run again");
        assert!(src.is_blocked(), "{attempt}");
        assert_eq!(blocked(&w), Some(1.0), "{attempt}");
    }
    assert!(src.committed().is_empty(), "the held batch commits nothing");
}

/// A source no route names at all, under drop or deliver: every row is unmatched, and the reader is not blocked for
/// it. Nothing buffers waiting for a route that will never exist -- the batch commits on the tick it arrived.
#[test]
fn a_source_with_no_routes_commits_rather_than_blocking_its_reader() {
    for (label, dlq) in [("drop", None), ("deliver", Some(FakeDestination::new()))] {
        let src = FakeSource::new([record("x", 0, 0), record("y", 1, 0)]);
        let mut w = match &dlq {
            Some(d) => {
                let mut w = worker_with_unmatched(vec![], vec![], &src, deliver(&[0]));
                w.add_destination(target(0, d.clone()));
                writes(&mut w, 0, Payload::source_bytes());
                w
            }
            None => worker_with_unmatched(vec![], vec![], &src, Answer::Drop),
        };

        tick(&mut w).unwrap();

        assert_eq!(src.committed().len(), 1, "{label}: committed, not blocked");
        assert_eq!(
            counter(
                &w,
                "smeltr_rows_unmatched_total",
                &[("source", "0"), ("cause", "route")]
            ),
            Some(2),
            "{label}"
        );
        match &dlq {
            Some(d) => {
                let sent = d.sent();
                assert_eq!(sent.len(), 2, "{label}");
                assert_eq!(sent[0].value, b"x", "{label}: as it arrived");
                assert_eq!(
                    counter(&w, "smeltr_rows_dropped_total", &[("source", "0")]),
                    None,
                    "{label}"
                );
            }
            None => assert_eq!(
                counter(&w, "smeltr_rows_dropped_total", &[("source", "0")]),
                Some(2),
                "{label}"
            ),
        }
    }
}

/// An unconditional route takes everything still eligible, so nothing is unmatched and neither counter moves.
#[test]
fn an_unconditional_route_leaves_nothing_unmatched() {
    let src = FakeSource::new([record("x", 0, 0), record("y", 1, 0)]);
    let mut w = worker_with_unmatched(plain_route(), vec![pipeline(0, vec![])], &src, deliver(&[1]));
    let (routed, leftover) = (FakeDestination::new(), FakeDestination::new());
    w.add_destination(target(0, routed.clone()));
    w.add_destination(target(1, leftover.clone()));

    tick(&mut w).unwrap();

    assert_eq!(routed.sent().len(), 2);
    assert!(leftover.sent().is_empty());
    assert_eq!(
        counter(
            &w,
            "smeltr_rows_unmatched_total",
            &[("source", "0"), ("cause", "route")]
        ),
        None
    );
}

/// An unmatched row is ordinary traffic, so a destination that refuses one is resolved by that destination's
/// answer to `Refused` rather than counted lost. `Block` -- the default -- withholds the commit and blocks the source; the
/// operator asked not to lose data quietly. `Drop` is the setting that says otherwise.
#[test]
fn a_refused_unmatched_row_obeys_the_destination_s_reject_policy() {
    for (label, policy, want_commit) in [("block", Answer::Block, false), ("drop", Answer::Drop, true)] {
        let src = FakeSource::new([record("x", 0, 0)]);
        let mut w = worker_with_unmatched(vec![], vec![], &src, deliver(&[0]));
        let dlq = FakeDestination::new();
        dlq.reject_next_sends(usize::MAX);
        w.add_destination(target_with(0, dlq.clone(), policy));

        tick(&mut w).expect("the row was handed over; the refusal is the destination's answer");

        assert_eq!(
            counter(
                &w,
                "smeltr_rows_unmatched_total",
                &[("source", "0"), ("cause", "route")]
            ),
            Some(1),
            "{label}"
        );
        assert_eq!(
            counter(&w, "smeltr_rows_rejected_total", &[("destination", "0")]),
            Some(1),
            "{label}: refused, and said so"
        );
        if want_commit {
            assert_eq!(src.committed().len(), 1, "{label}: committed past");
            assert!(!src.is_blocked(), "{label}");
        } else {
            assert!(src.is_blocked(), "{label}: blocked rather than losing the row");
            let why = w.reader.block_reason.as_deref().expect("a blocked source says why");
            assert!(why.contains("set to block"), "{label}: {why}");
            assert!(src.committed().is_empty(), "{label}: nothing committed");
        }
    }
}

/// No pipeline ran, so an unmatched row carries no `__destination.id` and no field to read one from. A destination that keys
/// on an identity generates its own instead, which is its dedupe silently not happening -- counted under the name
/// that already exists for that.
#[test]
fn an_unmatched_row_to_a_keyed_destination_is_counted_as_having_no_identity() {
    let src = FakeSource::new([record("x", 0, 0), record("y", 1, 0)]);
    let mut w = worker_with_unmatched(vec![], vec![], &src, deliver(&[0]));
    let dlq = FakeDestination::new();
    w.add_destination(keyed(0, dlq.clone(), None));

    tick(&mut w).unwrap();

    assert_eq!(dlq.sent().len(), 2);
    assert!(dlq.sent().iter().all(|r| r.doc_id.is_none()));
    assert_eq!(
        counter(&w, "smeltr_rows_without_doc_id_total", &[("destination", "0")]),
        Some(2)
    );
}

// -- unprocessable batches ----------------------------------------------------

/// The attempt at a poison batch has its routes' rows in hand when it panics, and none of them may leave. The diversion
/// is handed over under the abandoned batch's own id.
#[test]
fn a_diverted_poison_batch_reaches_the_dlq_exactly_once_under_its_own_id() {
    let src = FakeSource::new([record(r#"{"a":1}"#, 0, 0)]);
    let mut w = worker(
        plain_route(),
        vec![pipeline(0, vec![code(0, "Object.assign($, json(_raw));")])],
        &src,
    );
    let dlq = AsyncDestination::new();
    w.add_destination(target(0, FakeDestination::new()));
    poison(&mut w);
    w.add_destination(target(1, dlq.clone()));
    set_unprocessable(&mut w, deliver(&[1]));
    writes(&mut w, 1, Payload::source_bytes());

    assert_eq!(tick(&mut w).unwrap(), 1);

    let units = dlq.units();
    assert_eq!(units.len(), 1, "one seal, for the diverted batch: {units:?}");
    let (_, rows) = &units[0];
    assert_eq!(rows.len(), 1, "the record once, not once per attempt: {rows:?}");
    assert_eq!(rows[0], br#"{"a":1}"#, "source bytes, verbatim");
    assert!(
        src.committed().is_empty(),
        "the commit waits for the DLQ to acknowledge"
    );
    assert_eq!(
        counter(&w, "smeltr_batches_poisoned_total", &[("source", "0")]),
        Some(1),
        "one attempt: the same records would panic the same pipelines again"
    );

    complete_all(&mut w, &dlq, units[0].0, 1);
    assert_eq!(
        src.committed(),
        vec![[(tp(0), 0i64)].into_iter().collect()],
        "the DLQ's acknowledgment is what commits it"
    );
}

/// A batch that panics is dropped or delivered on that turn, and committed past. Delivered, it goes to every
/// destination named, as it arrived or as its `_raw`.
#[test]
fn a_poison_batch_is_dropped_or_delivered_then_committed_past() {
    for (label, disposition, want_dropped, want_value) in [
        ("drop", Answer::Drop, Some(1), None),
        ("source bytes", deliver(&[1, 2]), None, Some(br#"{"a":1}"#.to_vec())),
        (
            "record",
            deliver(&[1, 2]),
            None,
            Some(br#"{"_raw":"{\"a\":1}"}"#.to_vec()),
        ),
    ] {
        let src = FakeSource::new([record(r#"{"a":1}"#, 0, 0)]);
        let mut w = worker(
            plain_route(),
            vec![pipeline(0, vec![code(0, "Object.assign($, json(_raw));")])],
            &src,
        );
        let (one, two) = (FakeDestination::new(), FakeDestination::new());
        w.add_destination(target(0, FakeDestination::new()));
        poison(&mut w);
        w.add_destination(target(1, one.clone()));
        w.add_destination(target(2, two.clone()));
        set_unprocessable(&mut w, disposition);
        if label == "source bytes" {
            writes(&mut w, 1, Payload::source_bytes());
            writes(&mut w, 2, Payload::source_bytes());
        }

        assert_eq!(tick(&mut w).unwrap(), 1, "{label}");
        // A batch given up on must not run again. A re-run would send, drop and commit it a second time, which the
        // checks below count.
        assert_eq!(tick(&mut w).unwrap(), 0, "{label}: given up, not re-run");
        assert_eq!(
            src.committed(),
            vec![[(tp(0), 0i64)].into_iter().collect()],
            "{label}: committed past"
        );
        for (dest, d) in [("1", &one), ("2", &two)] {
            let sent: Vec<Vec<u8>> = d.sent().into_iter().map(|r| r.value).collect();
            assert_eq!(
                sent,
                want_value.clone().into_iter().collect::<Vec<_>>(),
                "{label}: {dest}"
            );
            assert_eq!(
                counter(&w, "smeltr_rows_sent_total", &[("destination", dest)]),
                want_value.as_ref().map(|_| 1),
                "{label}: {dest}"
            );
        }
        assert_eq!(
            counter(
                &w,
                "smeltr_rows_dropped_total",
                &[("source", "0"), ("reason", DROPPED_UNPROCESSABLE)]
            ),
            want_dropped,
            "{label}"
        );
        assert!(!src.is_blocked(), "{label}");
    }
}

/// Under block, the default, a batch that panics is held until the config changes: nothing commits, the source says
/// why, and the pipelines do not run it again.
#[test]
fn a_poison_batch_under_block_is_held_and_blocks_the_source() {
    let src = FakeSource::new([record(r#"{"a":1}"#, 0, 0)]);
    let mut w = worker(
        plain_route(),
        vec![pipeline(0, vec![code(0, "Object.assign($, json(_raw));")])],
        &src,
    );
    w.add_destination(target(0, FakeDestination::new()));
    poison(&mut w);

    assert_eq!(tick(&mut w).unwrap(), 0);
    let panics = counter(&w, "smeltr_batches_poisoned_total", &[("source", "0")]);
    assert_eq!(panics, Some(1));
    for attempt in 0..3 {
        let _ = tick(&mut w);
        assert!(src.is_blocked(), "{attempt}");
        let why = w.reader.block_reason.as_deref().expect("a blocked source says why");
        assert!(why.contains("could not be processed"), "{attempt}: {why}");
        assert_eq!(w.reader.block_cause, Some(BlockCause::Held), "{attempt}");
    }
    assert!(src.committed().is_empty(), "nothing commits past a held batch");
    assert_eq!(
        counter(&w, "smeltr_batches_poisoned_total", &[("source", "0")]),
        panics,
        "held, not run again"
    );
}

/// An unprocessable destination that cannot take the batch holds the source, like any destination the flow needs: the
/// batch is sent again, with no pipeline run, until it lands. A row it took twice is at-least-once.
#[test]
fn an_unprocessable_destination_that_fails_holds_the_commit_until_it_takes_the_batch() {
    let src = FakeSource::new([record(r#"{"a":1}"#, 0, 0)]);
    let mut w = worker(
        plain_route(),
        vec![pipeline(0, vec![code(0, "Object.assign($, json(_raw));")])],
        &src,
    );
    // `FakeDestination::dispatch` flushes, so a flush that fails is a seal the destination would not take.
    let dlq = FakeDestination::new();
    dlq.fail_next_flushes(1);
    w.add_destination(target(0, FakeDestination::new()));
    poison(&mut w);
    w.add_destination(target(1, dlq.clone()));
    set_unprocessable(&mut w, deliver(&[1]));

    tick(&mut w).expect_err("the seal failed");
    assert!(
        src.committed().is_empty(),
        "nothing commits past what the DLQ has not taken"
    );
    assert!(src.is_blocked());
    assert_eq!(
        counter_total(&w, "smeltr_rows_sent_total"),
        0,
        "nothing reported sent yet"
    );

    assert_eq!(tick(&mut w).unwrap(), 1, "sent again, and taken");
    assert_eq!(
        src.committed(),
        vec![[(tp(0), 0i64)].into_iter().collect()],
        "committed once the DLQ has it"
    );
    assert!(!src.is_blocked(), "resumed on its own");
    assert_eq!(dlq.sent().len(), 2, "the refused seal's row, then the one that landed");
    assert_eq!(counter(&w, "smeltr_rows_sent_total", &[("destination", "1")]), Some(1));
    assert_eq!(
        counter(&w, "smeltr_batches_poisoned_total", &[("source", "0")]),
        Some(1),
        "no pipeline ran it again"
    );
    assert_eq!(counter_total(&w, "smeltr_rows_dropped_total"), 0, "nothing lost");
}

/// An unprocessable destination answers for what it refuses with its own reject policy, as any destination does. The
/// DLQ that takes the row is sealed under the batch, and the commit waits for it.
#[test]
fn an_unprocessable_row_refused_goes_to_that_destinations_dlq() {
    let src = FakeSource::new([record(r#"{"a":1}"#, 0, 0)]);
    let mut w = worker(
        plain_route(),
        vec![pipeline(0, vec![code(0, "Object.assign($, json(_raw));")])],
        &src,
    );
    let quarantine = FakeDestination::new();
    let dlq = AsyncDestination::new();
    quarantine.reject_next_sends(1);
    w.add_destination(target(0, FakeDestination::new()));
    poison(&mut w);
    w.add_destination(target_with(1, quarantine.clone(), salvage_to(&[2])));
    w.add_destination(target(2, dlq.clone()));
    let topology = Arc::get_mut(&mut w.transform.topology).expect("a test worker's topology is its own");
    topology.on_exception.answers[ExceptionKind::Unprocessable.index()] = deliver(&[1]);
    topology.dlq_of = std::collections::HashMap::from([(1, vec![2])]);
    topology.payloads.insert(2, Payload::source_bytes());

    tick(&mut w).unwrap();

    let units = dlq.units();
    assert_eq!(units.len(), 1, "sealed under the batch: {units:?}");
    assert_eq!(units[0].1, vec![br#"{"a":1}"#.to_vec()], "salvaged, not lost");
    assert!(src.committed().is_empty(), "the commit waits for the DLQ");

    complete_all(&mut w, &dlq, units[0].0, 1);
    assert_eq!(src.committed().len(), 1);
}

/// A DLQ a refusal reaches is added to the batch's waiting list as it is handed the row. An unprocessable batch run
/// again after a failed hand-over must not narrow the list a second time and forget it: the commit waits for the DLQ.
#[test]
fn an_unprocessable_batch_run_again_still_waits_for_the_dlq_a_refusal_reached() {
    let src = FakeSource::new([record(r#"{"a":1}"#, 0, 0)]);
    let mut w = worker(plain_route(), vec![pipeline(0, vec![])], &src);
    let (named, other, dlq) = (
        AsyncDestination::new(),
        AsyncDestination::new(),
        AsyncDestination::new(),
    );
    other.fail_next_deliver("no answer");
    w.add_destination(target(0, FakeDestination::new()));
    poison(&mut w);
    w.add_destination(target_with(1, named.clone(), salvage_to(&[3])));
    w.add_destination(target(2, other.clone()));
    w.add_destination(target(3, dlq.clone()));
    let topology = Arc::get_mut(&mut w.transform.topology).expect("a test worker's topology is its own");
    topology.on_exception.answers[ExceptionKind::Unprocessable.index()] = deliver(&[1, 2]);
    topology.dlq_of = std::collections::HashMap::from([(1, vec![3])]);

    tick(&mut w).expect_err("the second destination fails the hand-over");
    let (batch, _) = named.units()[0].clone();
    named.complete_with(
        batch,
        vec![Rejection {
            doc: 0,
            reason: "refused".into(),
            record: None,
        }],
    );
    // The refusal is answered before the batch is run again: the DLQ is handed the row, and the batch waits on it.
    w.transform.collect().unwrap();
    w.reader.apply(w.acks.drain());
    assert_eq!(dlq.units().len(), 1, "the DLQ has the refused row");

    tick(&mut w).unwrap();
    for d in [&named, &other] {
        for b in d.outstanding() {
            d.complete(b);
        }
    }
    tick(&mut w).unwrap();
    assert!(
        src.committed().is_empty(),
        "the named destinations acknowledged, but the DLQ has not"
    );

    complete_all(&mut w, &dlq, batch, 1);
    assert_eq!(src.committed().len(), 1, "the DLQ's acknowledgment is what commits it");
}

/// A reader that can take no more work -- nothing acknowledges what it handed over -- keeps its source's membership
/// once that has lasted `membership_after`, and not before: short backpressure is ordinary, and a Kafka source pauses
/// to keep it, which throws away what the client prefetched.
#[test]
fn a_reader_that_can_take_no_work_keeps_its_sources_membership() {
    for (after, keeps) in [(Duration::from_secs(3600), false), (Duration::ZERO, true)] {
        let src = FakeSource::new((0..5).map(|i| record("{}", i, 0)));
        let metrics = MetricsHandle::new(Arc::new(Metrics::new()), "test");
        let mut reader = Reader::new(0, Box::new(src.clone()), vec![0], 1, metrics);
        reader.membership_after = after;
        let handoff = Handoff::new();
        let (_reports, acks) = reports();

        let mut full = 0;
        for _ in 0..6 {
            if matches!(
                reader.step(&handoff, &acks, 1, 1 << 20, Duration::ZERO).unwrap(),
                Polled::Full
            ) {
                full += 1;
            }
        }
        assert!(full > 0, "{after:?}: the reader reached its limit");
        assert_eq!(src.membership_kept() > 0, keeps, "{after:?}");
    }
}

/// A partition the group moves away while a batch read from it is in flight commits nothing from that batch, however
/// it finishes: the new owner may already be past it. The batch's other partitions commit as before.
#[test]
fn a_revoked_partition_commits_nothing_from_a_batch_read_before_the_revoke() {
    let src = FakeSource::new([record("{}", 10, 0), record("{}", 20, 1)]);
    let dest = AsyncDestination::new();
    let mut w = async_worker(&dest, &src);

    tick(&mut w).unwrap();
    let (batch, _) = dest.units()[0].clone();
    src.revoke(tp(0));
    complete_all(&mut w, &dest, batch, 1);

    assert_eq!(
        src.committed(),
        vec![[(tp(1), 20i64)].into_iter().collect()],
        "partition 0 is someone else's now"
    );
}

/// A shutdown drain retires and commits without another step. A revoke the last step's poll saw is applied there too,
/// or a batch read before it would commit to a partition that has since come back.
#[test]
fn a_revoke_is_applied_by_a_commit_that_follows_no_step() {
    let src = FakeSource::new([record("{}", 10, 0), record("{}", 20, 1)]);
    let dest = AsyncDestination::new();
    let mut w = async_worker(&dest, &src);

    tick(&mut w).unwrap();
    let (batch, _) = dest.units()[0].clone();
    src.revoke(tp(0));
    dest.complete(batch);
    // What `worker::run_reader_loop` does once it is told to stop.
    w.transform.collect().unwrap();
    w.reader.apply(w.acks.drain());
    w.reader.retire_and_commit().unwrap();

    assert_eq!(
        src.committed(),
        vec![[(tp(1), 20i64)].into_iter().collect()],
        "partition 0 is someone else's now"
    );
}

// -- transient failures -----------------------------------------------------

/// A failed hand-over blocks the commit, holds the source, and the kept batch is handed over whole before any poll.
/// A row is counted sent once, by the attempt that goes through.
#[test]
fn an_undeliverable_batch_blocks_the_commit_holds_the_source_and_is_retried() {
    let routes = vec![route_with_deliver(deliver_to(&[0, 1]))];
    let src = FakeSource::new([record("a", 10, 0), record("b", 11, 0)]);
    let mut w = worker(routes, vec![pipeline(0, vec![])], &src);
    let (flaky, healthy) = (FakeDestination::new(), FakeDestination::new());
    flaky.fail_next_sends(1);
    w.add_destination(target(0, flaky.clone()));
    w.add_destination(target(1, healthy.clone()));

    tick(&mut w).expect_err("an undeliverable batch fails the tick");
    assert_eq!(flaky.sent().len(), 1, "the fake took the row it did not fail on");
    assert!(
        healthy.sent().is_empty(),
        "a failed hand-over stops the batch: the re-run hands it over whole"
    );
    assert!(src.committed().is_empty());
    assert!(src.is_blocked());
    assert_eq!(
        counter(&w, "smeltr_rows_send_failed_total", &[("destination", "0")]),
        Some(2),
        "every row of the hand-over that failed"
    );
    assert_eq!(
        counter(&w, "smeltr_rows_sent_total", &[("destination", "1")]),
        None,
        "nothing is counted sent before the batch is handed off"
    );

    // Not requeued: the batch the worker kept is what goes out.
    tick(&mut w).expect("the kept batch goes out once delivery works");
    assert_eq!(flaky.sent().len(), 3, "both rows, from the kept batch");
    assert_eq!(
        healthy.sent().len(),
        2,
        "handed over once, by the attempt that went through"
    );
    assert_eq!(src.committed().len(), 1);
    for dest in ["0", "1"] {
        assert_eq!(
            counter(&w, "smeltr_rows_sent_total", &[("destination", dest)]),
            Some(2),
            "{dest}: counted once, not once per attempt"
        );
    }
    assert!(!src.is_blocked(), "resumed on its own");
}

/// A destination that will not flush fails the tick, holds the source, commits nothing, and is never mistaken for a poison batch.
#[test]
fn a_stalled_destination_holds_the_source_and_is_never_poisoned() {
    let src = FakeSource::new([record("a", 10, 0), record("b", 11, 0)]);
    let mut w = worker(plain_route(), vec![pipeline(0, vec![code(0, "x = 1;")])], &src);
    w.add_destination(target(0, FakeDestination::stalled()));
    let poison = FakeDestination::new();
    w.add_destination(target(1, poison.clone()));
    set_unprocessable(&mut w, deliver(&[1]));

    tick(&mut w).expect_err("a destination that will not flush fails the tick");
    assert!(src.is_blocked());
    assert_eq!(blocked(&w), Some(1.0));
    assert_eq!(w.reader.block_cause, Some(BlockCause::Waiting));
    for _ in 0..10 {
        let _ = tick(&mut w);
    }
    assert!(
        src.committed().is_empty(),
        "nothing commits past a batch the destination never acked"
    );
    assert!(poison.sent().is_empty(), "a stall is not a poison batch");
}

// -- rejections ---------------------------------------------------------------

/// A destination's answer to `Refused` decides a refused record's fate: drop and count, send to the destination's DLQs, which answer with
/// their own policies, or block the source until it goes through. A destination with no egress in front of it cannot
/// send a refusal again, so under block its source stays blocked; egress's own tests cover the sending again.
#[test]
fn a_reject_policy_decides_what_happens_to_a_refused_record() {
    for (label, policy, dlq_refuses) in [
        ("drop", Answer::Drop, false),
        ("dlq", salvage_to(&[1]), false),
        ("dlq refuses too", salvage_to(&[1]), true),
        ("block", Answer::Block, false),
    ] {
        let src = FakeSource::new([record("a", 10, 0), record("b", 11, 0), record("c", 12, 0)]);
        let (main, dlq) = (FakeDestination::new(), FakeDestination::new());
        main.reject_next_sends(1);
        if dlq_refuses {
            dlq.reject_next_sends(1);
        }
        let mut w = worker(plain_route(), vec![pipeline(0, vec![])], &src);
        w.add_destination(target_with(0, main.clone(), policy.clone()));
        w.add_destination(target_with(1, dlq.clone(), Answer::Drop));

        tick(&mut w).expect("a refused record does not fail the tick");

        assert_eq!(
            counter(
                &w,
                "smeltr_rows_rejected_total",
                &[("source", "0"), ("destination", "0")]
            ),
            Some(1),
            "{label}: counted whatever the policy"
        );
        assert_eq!(main.sent().len(), 2, "{label}: the other two still went");
        let salvaged = counter(&w, "smeltr_rows_salvaged_total", &[("destination", "1")]);
        let lost = counter(&w, "smeltr_rows_lost_total", &[("destination", "1")]);
        let dropped = counter(
            &w,
            "smeltr_rows_dropped_total",
            &[("source", "0"), ("reason", DROPPED_REJECTED)],
        );
        match (policy, dlq_refuses) {
            (Answer::Drop, _) => {
                assert!(dlq.sent().is_empty(), "{label}");
                assert_eq!((salvaged, lost), (None, None), "{label}");
                assert_eq!(dropped, Some(1), "{label}: let go, and counted as dropped");
            }
            (Answer::Deliver(_), false) => {
                assert_eq!(dlq.sent().len(), 1, "{label}: the destination's DLQ took it");
                assert_eq!((salvaged, lost), (Some(1), None), "{label}");
                assert_eq!(dropped, None, "{label}: kept, not dropped");
            }
            (Answer::Deliver(_), true) => {
                assert!(dlq.sent().is_empty(), "{label}");
                assert_eq!((salvaged, lost), (None, Some(1)), "{label}: lost, said plainly");
                assert_eq!(dropped, Some(1), "{label}: the DLQ's own policy let it go");
            }
            (Answer::Block, _) => {
                assert!(src.committed().is_empty(), "{label}");
                assert!(src.is_blocked(), "{label}: blocked rather than losing the row");
                assert_eq!(blocked(&w), Some(1.0), "{label}");
                assert!(dlq.sent().is_empty(), "{label}: blocked, not diverted");
                continue;
            }
            (Answer::Ignore | Answer::Tag, _) => {
                unreachable!("a destination answers Refused with drop, deliver or block")
            }
        }
        assert_eq!(src.committed().len(), 1, "{label}: the batch commits past the refusal");
    }
}

/// A row refused down a chain of DLQs reaches the last one in that one's own form, though the DLQ between writes
/// another: the record goes through a DLQ that writes the source's bytes and on to one that writes the record.
#[test]
fn a_row_keeps_its_own_form_through_a_dlq_that_writes_another() {
    let src = FakeSource::new([record(r#"{"a": 1}"#, 0, 0)]);
    let (main, bytes, last) = (FakeDestination::new(), FakeDestination::new(), FakeDestination::new());
    main.reject_next_sends(1);
    bytes.reject_next_sends(1);
    let p = pipeline(0, vec![code(0, "a: i64; Object.assign($, json(_raw)); b = 2;")]);
    let mut w = worker_with_dlqs(plain_route(), vec![p], &src, &[(0, vec![1, 2]), (1, vec![2])]);
    writes(&mut w, 1, Payload::source_bytes());
    w.add_destination(target_with(0, main.clone(), salvage_to(&[1])));
    w.add_destination(target_with(1, bytes.clone(), salvage_to(&[2])));
    w.add_destination(target_with(2, last.clone(), Answer::Drop));

    tick(&mut w).expect("a refused record does not fail the tick");
    tick(&mut w).expect("a refused record does not fail the tick");

    let saved = last.sent();
    assert_eq!(saved.len(), 1);
    let row = body(&saved[0].value);
    assert_eq!(
        (row["a"].as_i64(), row["b"].as_i64()),
        (Some(1), Some(2)),
        "the record: {row}"
    );
}

/// A record unrolled into rows that are all refused reaches a DLQ writing source bytes once. Refused there too, every
/// row goes on to the next DLQ as itself.
#[test]
fn a_refused_unroll_reaches_a_source_bytes_dlq_once_and_goes_on_whole() {
    for bytes_refuses in [false, true] {
        let record_text = r#"{"tags":[1,2,3]}"#;
        let src = FakeSource::new([record(record_text, 0, 0)]);
        let (main, bytes, last) = (FakeDestination::new(), FakeDestination::new(), FakeDestination::new());
        main.reject_next_sends(3);
        if bytes_refuses {
            bytes.reject_next_sends(1);
        }
        let p = pipeline(
            0,
            vec![
                code(0, "tags: array<i64>; Object.assign($, json(_raw));"),
                code(1, "unroll(tags);"),
            ],
        );
        let mut w = worker_with_dlqs(plain_route(), vec![p], &src, &[(0, vec![1, 2]), (1, vec![2])]);
        writes(&mut w, 1, Payload::source_bytes());
        w.add_destination(target_with(0, main.clone(), salvage_to(&[1])));
        w.add_destination(target_with(1, bytes.clone(), salvage_to(&[2])));
        w.add_destination(target_with(2, last.clone(), Answer::Drop));

        tick(&mut w).expect("a refused record does not fail the tick");
        tick(&mut w).expect("a refused record does not fail the tick");

        let (bytes, last) = (bytes.sent(), last.sent());
        if bytes_refuses {
            assert!(bytes.is_empty());
            let tags: Vec<_> = last.iter().map(|r| body(&r.value)["tags"].as_i64()).collect();
            assert_eq!(tags, [Some(1), Some(2), Some(3)], "every row, as itself");
        } else {
            assert_eq!(bytes.len(), 1, "the record once");
            assert_eq!(bytes[0].value, record_text.as_bytes());
            assert!(last.is_empty());
        }
    }
}

/// A batch a destination refuses whole is salvaged in one hand-over, and one DLQ's tally splits between the rows it
/// took and the rows it would not, which its own policy drops.
#[test]
fn one_dlq_tallies_both_the_rows_it_took_and_the_rows_it_refused() {
    let src = FakeSource::new([record("a", 10, 0), record("b", 11, 0), record("c", 12, 0)]);
    let (main, dlq) = (FakeDestination::new(), FakeDestination::new());
    main.reject_next_sends(3);
    dlq.reject_next_sends(1);
    let mut w = worker(plain_route(), vec![pipeline(0, vec![])], &src);
    w.add_destination(target_with(0, main.clone(), salvage_to(&[1])));
    w.add_destination(target_with(1, dlq.clone(), Answer::Drop));

    tick(&mut w).expect("a refused record does not fail the tick");

    assert_eq!(dlq.sent().len(), 2, "the DLQ refused the first and took the rest");
    assert_eq!(
        counter(&w, "smeltr_rows_salvaged_total", &[("destination", "1")]),
        Some(2),
        "the two it took"
    );
    assert_eq!(counter(&w, "smeltr_rows_lost_total", &[("destination", "1")]), Some(1));
}

/// A rule that already sent the row to the refusing destination's DLQ has put it there: the DLQ gets one copy, counted
/// sent by the rule and not salvaged.
#[test]
fn a_row_already_sent_to_the_dlq_is_not_salvaged_twice() {
    let src = FakeSource::new([record("a", 10, 0)]);
    let (main, dlq) = (FakeDestination::new(), FakeDestination::new());
    main.reject_next_sends(1);
    let mut w = worker_with_dlqs(
        vec![route_with_deliver(deliver_to(&[0, 1]))],
        vec![pipeline(0, vec![])],
        &src,
        &[(0, vec![1])],
    );
    w.add_destination(target_with(0, main.clone(), salvage_to(&[1])));
    w.add_destination(target(1, dlq.clone()));

    tick(&mut w).expect("a refused record does not fail the tick");

    assert_eq!(dlq.sent().len(), 1, "one copy");
    assert_eq!(counter(&w, "smeltr_rows_sent_total", &[("destination", "1")]), Some(1));
    assert_eq!(counter(&w, "smeltr_rows_salvaged_total", &[("destination", "1")]), None);
    assert_eq!(src.committed().len(), 1);
}

/// A refusal delivered to a DLQ that writes source bytes or `_raw` sends it the source's bytes or its decoded text, not
/// what the pipeline made of them.
#[test]
fn a_refusal_delivered_as_source_bytes_sends_the_source_bytes() {
    for payload in [Payload::source_bytes(), raw_text()] {
        let src = FakeSource::new([record("x", 10, 0)]);
        let (main, dlq) = (FakeDestination::new(), FakeDestination::new());
        main.reject_next_sends(1);
        let pipelines = vec![pipeline(0, vec![code(0, r#"via = "a";"#)])];
        let mut w = worker_with_dlqs(plain_route(), pipelines, &src, &[(0, vec![1])]);
        writes(&mut w, 1, payload.clone());
        w.add_destination(target_with(0, main.clone(), deliver(&[1])));
        w.add_destination(target(1, dlq.clone()));

        tick(&mut w).expect("a refused record does not fail the tick");

        let saved = dlq.sent();
        assert_eq!(saved.len(), 1, "{payload:?}");
        assert_eq!(
            (saved[0].value.as_slice(), &saved[0].payload),
            (b"x".as_slice(), &payload),
            "the source's own"
        );
        assert_eq!(
            counter(&w, "smeltr_rows_salvaged_total", &[("destination", "1")]),
            Some(1)
        );
        assert_eq!(src.committed().len(), 1);
    }
}

/// The DLQ belongs to the destination, so a record the unmatched destination refuses is salvaged to it too, in the
/// DLQ's own form, whatever the unmatched destination writes.
#[test]
fn a_refusal_of_an_unmatched_record_is_salvaged_to_the_destinations_dlq() {
    for (sent, refused, saved) in [
        (Payload::source_bytes(), Payload::Record, &br#"{"_raw":"a"}"#[..]),
        (Payload::Record, Payload::Record, br#"{"_raw":"a"}"#),
        (Payload::Record, Payload::source_bytes(), b"a"),
    ] {
        let src = FakeSource::new([record("a", 10, 0)]);
        let (leftovers, dlq) = (FakeDestination::new(), FakeDestination::new());
        leftovers.reject_next_sends(1);
        let mut on_exception = OnException::default();
        on_exception.answers[ExceptionKind::Unmatched.index()] = deliver(&[0]);
        let metas = SourceMetas::from([(0, SourceMeta::KAFKA)]);
        let mut topology = compile_topology_with(&[], &[], on_exception, &metas).expect("topology compiles");
        topology.dlq_of = std::collections::HashMap::from([(0, vec![1])]);
        topology.payloads = std::collections::HashMap::from([(0, sent.clone()), (1, refused.clone())]);
        let metrics = MetricsHandle::new(Arc::new(Metrics::new()), "t0");
        let mut w = Worker::new(
            0,
            Box::new(src.clone()),
            Encoding::Auto,
            topology,
            Vec::new(),
            MemoryBudget::unlimited(),
            metrics,
        );
        w.add_destination(target_with(0, leftovers.clone(), deliver(&[1])));
        w.add_destination(target(1, dlq.clone()));

        tick(&mut w).expect("a refused record does not fail the tick");

        let got = dlq.sent();
        assert_eq!(got.len(), 1, "{sent:?} {refused:?}");
        assert_eq!(got[0].value, saved.to_vec(), "{sent:?} {refused:?}");
        assert_eq!(
            counter(&w, "smeltr_rows_salvaged_total", &[("destination", "1")]),
            Some(1)
        );
        assert_eq!(src.committed().len(), 1);
    }
}

/// A rejection a batched destination reports only at flush is answered by its policy like any other: dropped, it is
/// counted and committed past.
#[test]
fn a_rejection_reported_after_the_flush_is_counted_and_committed_past() {
    let src = FakeSource::new([record("a", 10, 0), record("b", 11, 0), record("c", 12, 0)]);
    let dest = FakeDestination::new();
    dest.reject_on_flush(&[1]);
    let mut w = worker(plain_route(), vec![pipeline(0, vec![])], &src);
    w.add_destination(target_with(0, dest.clone(), Answer::Drop));

    tick(&mut w).expect("a refused document does not fail the tick");

    assert_eq!(src.committed().len(), 1);
    assert_eq!(counter(&w, "smeltr_rows_rejected_total", &[("source", "0")]), Some(1));
}

/// A late rejection for an older batch, arriving while a newer one is in flight, is counted, and both commit.
#[test]
fn a_late_rejection_for_an_older_batch_is_counted_and_both_commit() {
    let routes = vec![
        route_cfg(None, 0, deliver_to(&[0])),
        route_cfg(None, 1, deliver_to(&[0])),
    ];
    let src = FakeSource::new([record("{}", 10, 0)]);
    let dest = AsyncDestination::new();
    let mut w = worker(routes, vec![pipeline(0, vec![]), pipeline(1, vec![])], &src);
    w.add_destination(target_with(0, dest.clone(), Answer::Drop));

    tick(&mut w).unwrap();
    src.requeue([record("{}", 11, 0), record("{}", 12, 0)]);
    tick(&mut w).unwrap();

    assert_eq!(dest.sent(), 6, "every row through both routes");
    let outstanding: Vec<_> = w.reader.commits.outstanding().collect();
    let [first, second] = outstanding[..] else {
        panic!("two batches in flight, got {outstanding:?}")
    };
    // Refuse a document of the first batch after the second is in flight.
    dest.complete_with(
        first,
        vec![Rejection {
            doc: 1,
            reason: "mapping conflict".into(),
            record: None,
        }],
    );
    dest.complete(second);
    tick(&mut w).unwrap();

    assert_eq!(counter_total(&w, "smeltr_rows_rejected_total"), 1);
    assert_eq!(
        src.committed(),
        vec![[(tp(0), 12i64)].into_iter().collect()],
        "dropped, not retried, so both batches commit"
    );
}

// -- in-flight batches and the commit watermark -----------------------------

/// Nothing commits until the destination acknowledges; an idle tick collects the acknowledgment.
#[test]
fn offsets_are_not_committed_until_the_destination_acknowledges() {
    let dest = AsyncDestination::new();
    let src = FakeSource::new([record("{}", 0, 0), record("{}", 1, 0)]);
    let mut w = async_worker(&dest, &src);

    assert_eq!(tick(&mut w).unwrap(), 2);
    assert_eq!(dest.sent(), 2);
    assert!(src.committed().is_empty(), "in flight, so uncommitted");

    dest.complete(dest.outstanding()[0]);
    tick(&mut w).unwrap();
    assert_eq!(src.committed(), vec![[(tp(0), 1i64)].into_iter().collect()]);
}

/// Commits retire in poll order: a younger batch acknowledged first cannot carry an older one's offsets.
#[test]
fn a_batch_completing_early_does_not_commit_past_an_older_one() {
    let dest = AsyncDestination::new();
    let src = FakeSource::new([record("{}", 0, 0)]);
    let mut w = async_worker(&dest, &src);

    tick(&mut w).unwrap();
    src.requeue([record("{}", 1, 0)]);
    tick(&mut w).unwrap();
    let outstanding = dest.outstanding();
    assert_eq!(outstanding.len(), 2);

    dest.complete(outstanding[1]);
    tick(&mut w).unwrap();
    assert!(src.committed().is_empty());

    dest.complete(outstanding[0]);
    tick(&mut w).unwrap();
    assert_eq!(
        src.committed(),
        vec![[(tp(0), 1i64)].into_iter().collect()],
        "one commit covers the pair at the higher offset"
    );
}

/// A reader never has more than `MAX_IN_FLIGHT` batches outstanding, and still makes progress at the bound.
#[test]
fn the_in_flight_queue_is_bounded() {
    let dest = AsyncDestination::new();
    let src = FakeSource::new([record("{}", 0, 0)]);
    let mut w = async_worker(&dest, &src);

    for i in 0..(MAX_IN_FLIGHT as i64 + 4) {
        src.requeue([record("{}", i, 0)]);
        tick(&mut w).unwrap();
        assert!(
            w.reader.commits.depth() <= MAX_IN_FLIGHT,
            "depth {} exceeded the bound at tick {i}",
            w.reader.commits.depth()
        );
    }
    assert!(!src.committed().is_empty(), "backpressure flushes");
}

/// A reader with a byte budget reads on past `MAX_IN_FLIGHT` small batches while they hold under it, and stops at it:
/// a destination that acknowledges late does not hold a source to a few batches.
#[test]
fn a_byte_budget_lets_small_batches_run_past_the_batch_floor() {
    let dest = AsyncDestination::new();
    let src = FakeSource::new([record("{}", 0, 0)]);
    let mut w = async_worker(&dest, &src);
    tick(&mut w).unwrap();
    let batch = w.reader.commits.bytes();
    assert!(batch > 0, "a batch counts what it read");
    let batches = MAX_IN_FLIGHT + 4;
    // Just over `batches` of them: the next one reaches it.
    w.reader.byte_budget = batch * batches + 1;

    for i in 1..batches as i64 {
        src.requeue([record("{}", i, 0)]);
        tick(&mut w).unwrap();
    }
    assert_eq!(w.reader.commits.depth(), batches, "every batch outstanding");
    assert!(src.committed().is_empty(), "nothing forced out under the budget");

    src.requeue([record("{}", batches as i64, 0)]);
    tick(&mut w).unwrap();
    assert!(!src.committed().is_empty(), "at the budget, backpressure flushes");
}

/// Batches that count no bytes still stop the reader, at `MAX_TRACKED`.
#[test]
fn batches_of_empty_records_stop_at_the_batch_ceiling() {
    let dest = AsyncDestination::new();
    let src = FakeSource::new([record("", 0, 0)]);
    let mut w = async_worker(&dest, &src);
    tick(&mut w).unwrap();
    assert_eq!(w.reader.commits.bytes(), 0, "an empty record reads nothing");
    w.reader.byte_budget = usize::MAX;

    for i in 1..=MAX_TRACKED as i64 {
        src.requeue([record("", i, 0)]);
        tick(&mut w).unwrap();
    }
    assert!(
        w.reader.commits.depth() <= MAX_TRACKED,
        "depth {} past the ceiling",
        w.reader.commits.depth()
    );
    assert!(!src.committed().is_empty(), "at the ceiling, backpressure flushes");
}

/// A refused dispatch keeps the batch, blocks the source, and re-runs it ahead of any poll; the block clears once it is acknowledged.
#[test]
fn a_destination_that_refuses_a_batch_holds_the_source_and_the_batch_is_retried() {
    let dest = AsyncDestination::new();
    let src = FakeSource::new([record("{}", 0, 0)]);
    let mut w = async_worker(&dest, &src);
    dest.fail_next_deliver("producer queue full");

    tick(&mut w).expect_err("a refused dispatch fails the tick");
    assert!(src.is_blocked());
    assert!(src.committed().is_empty());
    assert!(dest.outstanding().is_empty(), "nothing was handed over");

    assert_eq!(tick(&mut w).unwrap(), 1, "the same batch, re-run");
    assert_eq!(dest.sent(), 2, "sent once per attempt");
    assert_eq!(dest.outstanding().len(), 1);
    assert!(src.is_blocked(), "in flight, not yet acknowledged");

    dest.complete(dest.outstanding()[0]);
    tick(&mut w).unwrap();
    assert_eq!(src.committed(), vec![[(tp(0), 0i64)].into_iter().collect()]);
    assert!(!src.is_blocked(), "released, since nothing is outstanding");
}

/// A lost acknowledgment blocks the source with nothing in hand; the block clears only once every outstanding batch is acked, and the source reads again.
#[test]
fn a_hold_with_nothing_in_hand_clears_once_every_batch_is_acknowledged() {
    let dest = AsyncDestination::new();
    let src = FakeSource::new([record("{}", 0, 0)]);
    let mut w = async_worker(&dest, &src);

    tick(&mut w).unwrap();
    let batch = dest.outstanding()[0];

    dest.fail_next_poll("acknowledgment channel broke");
    tick(&mut w).expect_err("a delivery failure fails the tick");
    assert!(src.is_blocked());
    assert!(src.committed().is_empty());

    // Answering again is not acknowledging: an empty poll alone does not release.
    tick(&mut w).unwrap();
    assert!(src.is_blocked(), "a batch is still outstanding");

    dest.complete(batch);
    tick(&mut w).unwrap();
    assert!(
        !src.is_blocked(),
        "the empty-poll path must release: a blocked source cannot deliver its way out"
    );
    assert_eq!(blocked(&w), Some(0.0));
    assert_eq!(src.committed(), vec![[(tp(0), 0i64)].into_iter().collect()]);

    src.requeue([record("{}", 1, 0)]);
    assert_eq!(tick(&mut w).unwrap(), 1, "reading again without a redeploy");
}

/// An egress reports its delivered rows from the start, so a destination that has delivered nothing yet reads idle.
#[test]
fn a_new_egress_reports_its_rows_delivered_at_zero() {
    let registry = Arc::new(Metrics::new());
    let _stage = Egress::start(
        3,
        "d",
        Box::new(AsyncDestination::new()),
        Duration::from_secs(5),
        16_000_000,
        MetricsHandle::new(registry.clone(), "egress-3"),
    );
    let read = registry.take_delta(&mut smeltr::metrics::Cursor::default());
    let delivered = read
        .iter()
        .find(|s| s.key.name == "smeltr_rows_delivered_total")
        .expect("rows_delivered reported");
    assert!(delivered.key.labels.contains(&("destination", "3".to_string())));
    assert!(matches!(delivered.value, MetricValue::Counter(0)));
}

/// Through the egress stage: a failed delivery is re-sent by egress and only then committed.
#[test]
fn a_failed_delivery_is_re_sent_and_only_then_committed() {
    let dest = AsyncDestination::new();
    let src = FakeSource::new([record("{}", 7, 0)]);
    let mut stage = Egress::start(
        0,
        "d",
        Box::new(dest.clone()),
        Duration::from_secs(5),
        16_000_000,
        MetricsHandle::new(Arc::new(Metrics::new()), "test"),
    );
    let mut w = worker(plain_route(), vec![pipeline(0, vec![])], &src);
    w.add_destination(target_with(0, stage.handle(), Answer::Drop));

    tick(&mut w).unwrap();
    for _ in 0..200 {
        if !dest.outstanding().is_empty() {
            break;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(src.committed().is_empty(), "unacknowledged");

    // A second `send` of the same row is the proof of the re-send.
    let sent_before = dest.sent();
    dest.fail_next_poll("connection reset");
    for _ in 0..400 {
        if dest.sent() > sent_before && !dest.outstanding().is_empty() {
            break;
        }
        let _ = tick(&mut w);
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(dest.sent() > sent_before, "re-sent, not left to sit");
    assert!(src.committed().is_empty(), "still undelivered");

    for b in dest.outstanding() {
        dest.complete(b);
    }
    for _ in 0..200 {
        let _ = tick(&mut w);
        if !src.committed().is_empty() {
            break;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(
        src.committed().iter().flat_map(|m| m.values().copied()).max(),
        Some(7),
        "committed once it was really delivered"
    );
}

/// A refused source commit is dropped and counted, never blocked over or retried; the next watermark subsumes it.
#[test]
fn a_refused_source_commit_is_dropped_and_counted_not_held() {
    let dest = AsyncDestination::new();
    let src = FakeSource::new([record("{}", 0, 0)]);
    let mut w = async_worker(&dest, &src);

    tick(&mut w).unwrap();
    dest.complete(dest.outstanding()[0]);
    src.fail_commits(1);
    tick(&mut w).expect("a refused commit is the source's problem, not a destination block");
    assert!(!src.is_blocked());
    assert!(src.committed().is_empty());
    assert_eq!(counter(&w, "smeltr_commits_dropped_total", &[("source", "0")]), Some(1));

    for _ in 0..3 {
        let _ = tick(&mut w);
    }
    assert!(src.committed().is_empty(), "dropped, not retried");

    src.requeue([record("{}", 1, 0)]);
    tick(&mut w).unwrap();
    dest.complete(dest.outstanding()[0]);
    tick(&mut w).unwrap();
    assert_eq!(
        src.committed(),
        vec![[(tp(0), 1i64)].into_iter().collect()],
        "a later commit subsumes the dropped one"
    );
}

/// A `deliver` rule reads the batch its pipeline wrote, so a metadata
/// field only the rule names still has to be built. `Plan::reads_meta`
/// comes from the pipeline's instructions, which never see these rules:
/// derive it from those alone and the column stays empty, the condition
/// reads null, and every row is silently delivered nowhere.
#[test]
fn a_deliver_rule_reads_metadata_its_pipeline_never_touches() {
    let routes = dropping_declined(vec![route_with_deliver(vec![rule(Some("__source.offset == 0"), &[0])])]);
    let pipelines = vec![pipeline(0, vec![code(0, "x = 1;")])];
    let src = FakeSource::new([record("a", 0, 0), record("b", 1, 0)]);
    let mut w = worker(routes, pipelines, &src);
    let dest = FakeDestination::new();
    w.add_destination(target(0, dest.clone()));
    tick(&mut w).unwrap();
    assert_eq!(dest.sent().len(), 1, "only offset 0 should be delivered");
}

// -- ingest decoding ---------------------------------------------------------

/// `_raw` is text: ingest decodes each record with the source's encoding, so
/// a pipeline reads characters and never bytes. UTF-16 (with a mark or
/// without) arrives as the same string as its UTF-8 twin.
#[test]
fn ingest_decodes_utf16_records_into_raw() {
    let utf16le = |s: &str| -> Vec<u8> { s.encode_utf16().flat_map(|u| u.to_le_bytes()).collect() };
    let mut with_bom = vec![0xFF, 0xFE];
    with_bom.extend(utf16le("caf\u{e9} two"));

    let src = FakeSource::new([
        record_bytes(utf16le("caf\u{e9} one"), 0),
        record_bytes(with_bom, 1),
        record_bytes("caf\u{e9} three".as_bytes().to_vec(), 2),
    ]);
    let pipelines = vec![pipeline(0, vec![code(0, "out = _raw;")])];
    let mut w = worker(answering(tagging_repairs(), plain_route()), pipelines, &src);
    let dest = FakeDestination::new();
    w.add_destination(target(0, dest.clone()));

    tick(&mut w).unwrap();

    let sent = dest.sent();
    let out: Vec<String> = sent
        .iter()
        .map(|r| body(&r.value)["out"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(out, ["café one", "café two", "café three"]);
    // Nothing was repaired or guessed at, so no row has anything to say.
    assert!(
        sent.iter()
            .all(|r| body(&r.value).pointer("/smeltr/exception").is_none()),
        "{out:?}"
    );
}

/// Bytes `auto` cannot place are decoded as windows-1252, and the row is marked: the text is a guess, and a source
/// whose records all land here should declare its encoding. A guess is a note, which leaves the row as it is unless the
/// route tags notes.
#[test]
fn a_guessed_encoding_marks_the_row_with_a_note() {
    for (label, on_exception, says) in [
        ("by default", OnException::default(), false),
        ("tagging notes", tagging_repairs(), true),
    ] {
        let src = FakeSource::new([record_bytes(b"caf\xE9".to_vec(), 0), record_bytes(b"plain".to_vec(), 1)]);
        let p = pipeline(0, vec![code(0, "out = _raw;")]);
        let mut w = worker(answering(on_exception, plain_route()), vec![p], &src);
        let dest = FakeDestination::new();
        w.add_destination(target(0, dest.clone()));

        tick(&mut w).unwrap();

        let rows: Vec<serde_json::Value> = dest.sent().iter().map(|r| body(&r.value)).collect();
        assert_eq!(rows[0]["out"], "café", "{label}");
        let message = exception_text(&rows[0]).unwrap_or_default();
        assert_eq!(message.contains("GuessedEncoding"), says, "{label}: {}", rows[0]);
        assert!(
            rows[1].pointer("/smeltr/exception").is_none(),
            "valid UTF-8 is not a guess: {}",
            rows[1]
        );
    }
}

/// A declared encoding turns detection off: the same bytes decode as the
/// source says, and bytes that do not decode are replaced and the row says so.
#[test]
fn a_declared_encoding_decodes_without_detection() {
    let src = FakeSource::new([
        record_bytes("caf\u{e9}".as_bytes().to_vec(), 0),
        record_bytes(b"caf\xE9".to_vec(), 1),
    ]);
    let pipelines = vec![pipeline(0, vec![code(0, "out = _raw;")])];
    let routes = answering(tagging_repairs(), plain_route());
    let latin1 = Encoding::for_label("latin1").unwrap();
    let mut w = worker_decoding(routes.clone(), pipelines.clone(), &src, latin1);
    let dest = FakeDestination::new();
    w.add_destination(target(0, dest.clone()));
    tick(&mut w).unwrap();

    let rows: Vec<serde_json::Value> = dest.sent().iter().map(|r| body(&r.value)).collect();
    // Declared, so UTF-8 bytes are read as windows-1252 and nothing is a guess.
    assert_eq!(rows[0]["out"], "cafÃ©");
    assert_eq!(rows[1]["out"], "café");
    assert!(
        rows.iter().all(|r| r.pointer("/smeltr/exception").is_none()),
        "{rows:?}"
    );

    let src = FakeSource::new([record_bytes(b"caf\xE9".to_vec(), 0)]);
    let utf8 = Encoding::for_label("utf-8").unwrap();
    let mut w = worker_decoding(routes, pipelines, &src, utf8);
    let dest = FakeDestination::new();
    w.add_destination(target(0, dest.clone()));
    tick(&mut w).unwrap();

    let row = body(&dest.sent()[0].value);
    assert_eq!(row["out"], "caf\u{FFFD}");
    assert!(exception_text(&row).unwrap().contains("InvalidUtf8"), "{row}");
}

// -- one instant per record --------------------------------------------------

/// `now()` is the record's instant, not each `Ctx`'s. Records build one to
/// route, one per route to run and one per delivery rule, so two routes over
/// the same record must agree -- otherwise a yearless timestamp could infer
/// two different years across a new-year boundary.
#[test]
fn every_route_over_one_record_reads_the_same_now() {
    let pipelines = vec![
        pipeline(0, vec![code(0, "t = toString(now());")]),
        pipeline(1, vec![code(0, "t = toString(now());")]),
    ];
    let routes = vec![
        route_cfg(None, 0, deliver_to(&[0])),
        route_cfg(None, 1, deliver_to(&[1])),
    ];
    let src = FakeSource::new([record("{}", 0, 0), record("{}", 1, 0)]);
    let mut w = worker(routes, pipelines, &src);
    let (a, b) = (FakeDestination::new(), FakeDestination::new());
    w.add_destination(target(0, a.clone()));
    w.add_destination(target(1, b.clone()));

    tick(&mut w).unwrap();

    let stamps: Vec<String> = a
        .sent()
        .iter()
        .chain(b.sent().iter())
        .map(|r| body(&r.value)["t"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(stamps.len(), 4, "two rows through two routes");
    assert!(
        stamps.iter().all(|s| *s == stamps[0]),
        "one instant per record, got {stamps:?}"
    );
}

/// A decode note belongs to the record, not to each route that delivers it.
/// It is replayed into every route's exception window so the row says so
/// wherever it goes, which must not make `smeltr_rows_errored_total` scale with
/// route fan-out.
#[test]
fn a_decode_note_is_counted_once_however_many_routes_deliver_it() {
    let pipelines = vec![
        pipeline(0, vec![code(0, "a = _raw;")]),
        pipeline(1, vec![code(0, "b = _raw;")]),
    ];
    let routes = answering(
        tagging_repairs(),
        vec![
            route_cfg(None, 0, deliver_to(&[0])),
            route_cfg(None, 1, deliver_to(&[1])),
        ],
    );
    // One record `auto` has to guess at, so exactly one `GuessedEncoding`.
    let src = FakeSource::new([record_bytes(b"caf\xE9".to_vec(), 0)]);
    let mut w = worker(routes, pipelines, &src);
    let (a, b) = (FakeDestination::new(), FakeDestination::new());
    w.add_destination(target(0, a.clone()));
    w.add_destination(target(1, b.clone()));

    tick(&mut w).unwrap();

    assert_eq!(a.sent().len(), 1, "both routes delivered");
    assert_eq!(b.sent().len(), 1);
    for r in a.sent().iter().chain(b.sent().iter()) {
        let row = body(&r.value);
        assert!(
            exception_text(&row).unwrap().contains("GuessedEncoding"),
            "said on both routes: {row}"
        );
    }
    assert_eq!(
        counter(&w, "smeltr_rows_errored_total", &[("kind", "GuessedEncoding")]),
        None,
        "ingest's own note is not one of the pipeline's exceptions"
    );
}

// -- refusals that come back with the acknowledgment ------------------------

/// A worker whose one route delivers to a destination that sends refusals to DLQs 1 and 2, all three acknowledging out
/// of band.
fn salvaging_worker(src: &FakeSource) -> (Worker, AsyncDestination, AsyncDestination, AsyncDestination) {
    let mut w = worker(plain_route(), vec![pipeline(0, vec![])], src);
    let (main, dlq1, dlq2) = (
        AsyncDestination::new(),
        AsyncDestination::new(),
        AsyncDestination::new(),
    );
    w.add_destination(target_with(0, main.clone(), salvage_to(&[1, 2])));
    w.add_destination(target(1, dlq1.clone()));
    w.add_destination(target(2, dlq2.clone()));
    (w, main, dlq1, dlq2)
}

/// What `dlq` was sealed with, leaving out the empty unit `AsyncDestination` seals for a batch that sent it nothing
/// (egress answers that one at once).
fn salvaged_units(dlq: &AsyncDestination) -> Vec<(BatchId, Vec<OwnedRecord>)> {
    dlq.records().into_iter().filter(|(_, rows)| !rows.is_empty()).collect()
}

fn refusal(doc: usize) -> Rejection {
    Rejection {
        doc,
        reason: "mapping conflict".into(),
        record: None,
    }
}

/// A refusal that comes back with the acknowledgment goes to every DLQ, sealed under the same batch, and the batch
/// commits only once each DLQ has acknowledged it too.
#[test]
fn a_late_refusal_goes_to_every_dlq_and_the_commit_waits_for_them() {
    let src = FakeSource::new([record("a", 10, 0), record("b", 11, 0)]);
    let (mut w, main, dlq1, dlq2) = salvaging_worker(&src);
    tick(&mut w).unwrap();
    let [batch] = main.outstanding()[..] else {
        panic!("one batch in flight")
    };

    main.complete_with(batch, vec![refusal(1)]);
    tick(&mut w).unwrap();
    for dlq in [&dlq1, &dlq2] {
        let records = salvaged_units(dlq);
        let [(sealed, rows)] = &records[..] else {
            panic!("one unit on each DLQ, got {records:?}")
        };
        assert_eq!(*sealed, batch, "under the refused batch's own id");
        assert_eq!(rows.len(), 1);
        assert_eq!(
            text(&rows[0].value),
            r#"{"_raw":"b"}"#,
            "the document the destination refused"
        );
        assert!(rows[0].salvaged);
    }
    assert!(src.committed().is_empty(), "the DLQs have it and have not answered");

    // Every unit each DLQ has for the batch: `AsyncDestination` seals an empty one at hand-off too.
    complete_all(&mut w, &dlq1, batch, dlq1.records().len());
    assert!(src.committed().is_empty(), "one DLQ still owes it");
    complete_all(&mut w, &dlq2, batch, dlq2.records().len());
    assert_eq!(src.committed().len(), 1, "committed once every DLQ acknowledged");

    assert_eq!(
        counter(
            &w,
            "smeltr_rows_rejected_total",
            &[("source", "0"), ("destination", "0")]
        ),
        Some(1)
    );
    for dest in ["1", "2"] {
        assert_eq!(
            counter(&w, "smeltr_rows_salvaged_total", &[("destination", dest)]),
            Some(1),
            "{dest}"
        );
    }
}

/// A DLQ set to drop that refuses a row it was sent in salvage loses it, counted as lost: the row had been refused once
/// already.
#[test]
fn a_dlq_set_to_drop_loses_a_salvaged_row_it_refuses() {
    let src = FakeSource::new([record("a", 10, 0)]);
    let mut w = worker(plain_route(), vec![pipeline(0, vec![])], &src);
    let (main, dlq1, dlq2) = (
        AsyncDestination::new(),
        AsyncDestination::new(),
        AsyncDestination::new(),
    );
    w.add_destination(target_with(0, main.clone(), salvage_to(&[1, 2])));
    w.add_destination(target_with(1, dlq1.clone(), Answer::Drop));
    w.add_destination(target(2, dlq2.clone()));
    tick(&mut w).unwrap();
    let batch = main.outstanding()[0];
    main.complete_with(batch, vec![refusal(0)]);
    tick(&mut w).unwrap();

    // The salvaged unit is the latest under the batch, so the refusal names its row; the empty one from hand-off
    // completes as well.
    dlq1.complete_with(batch, vec![refusal(0)]);
    complete_all(&mut w, &dlq1, batch, dlq1.records().len() - 1);
    complete_all(&mut w, &dlq2, batch, dlq2.records().len());

    assert_eq!(counter(&w, "smeltr_rows_lost_total", &[("destination", "1")]), Some(1));
    assert!(!src.is_blocked(), "dropped, not blocked on");
    assert_eq!(src.committed().len(), 1);
}

/// DLQs chain: a DLQ that refuses a salvaged row sends it on to its own DLQ, keyed there on that DLQ's identity, and
/// the batch commits only once the last in the chain has it.
#[test]
fn a_dlq_that_refuses_sends_the_row_on_to_its_own_dlq() {
    let src = FakeSource::new([record("a", 10, 0)]);
    let mut w = worker_with_dlqs(
        plain_route(),
        vec![pipeline(0, vec![code(0, r#"k = "key-1";"#)])],
        &src,
        &[(0, vec![1, 2]), (1, vec![2])],
    );
    let (main, first, last) = (
        AsyncDestination::new(),
        AsyncDestination::new(),
        AsyncDestination::new(),
    );
    w.add_destination(target_with(0, main.clone(), salvage_to(&[1])));
    w.add_destination(target_with(1, first.clone(), salvage_to(&[2])));
    w.add_destination(keyed(2, last.clone(), Some("k")));
    tick(&mut w).unwrap();
    let batch = main.outstanding()[0];
    main.complete_with(batch, vec![refusal(0)]);
    tick(&mut w).unwrap();
    first.complete_with(batch, vec![refusal(0)]);
    complete_all(&mut w, &first, batch, first.records().len() - 1);

    let [(_, rows)] = &salvaged_units(&last)[..] else {
        panic!("the last DLQ in the chain has the row")
    };
    assert_eq!(rows[0].doc_id.as_deref(), Some("key-1"), "keyed on its own identity");
    assert!(src.committed().is_empty(), "the last DLQ has not acknowledged");
    complete_all(&mut w, &last, batch, last.records().len());
    assert_eq!(src.committed().len(), 1);
}

/// A DLQ whose queue will not take the rows yet keeps the refusing destination's acknowledgment back, so the batch
/// cannot commit past rows no DLQ has. The next collect offers them again, and a DLQ that took them is not sent them
/// twice.
#[test]
fn a_dlq_without_room_holds_the_refusing_destinations_acknowledgment() {
    let src = FakeSource::new([record("a", 10, 0)]);
    let (mut w, main, dlq1, dlq2) = salvaging_worker(&src);
    tick(&mut w).unwrap();
    let batch = main.outstanding()[0];
    dlq2.fail_next_deliver("no room");
    main.complete_with(batch, vec![refusal(0)]);
    tick(&mut w).unwrap();
    complete_all(&mut w, &dlq1, batch, dlq1.records().len());
    assert!(src.committed().is_empty(), "dlq 2 has not taken the rows");

    // The retry happens on this tick; its unit completes on the next.
    tick(&mut w).unwrap();
    complete_all(&mut w, &dlq2, batch, dlq2.records().len());
    assert_eq!(salvaged_units(&dlq1).len(), 1, "dlq 1 took them once, and only once");
    assert_eq!(salvaged_units(&dlq2).len(), 1, "dlq 2 took them on the second offer");
    assert_eq!(src.committed().len(), 1);
}

/// A destination set to block that keeps none of its own refusals cannot send them again: the batch stays owed and the
/// source blocked. (Egress keeps them; this is a destination built without it.)
#[test]
fn a_late_refusal_under_block_holds_the_batch() {
    let src = FakeSource::new([record("a", 10, 0)]);
    let mut w = worker(plain_route(), vec![pipeline(0, vec![])], &src);
    let main = AsyncDestination::new();
    w.add_destination(target(0, main.clone()));
    tick(&mut w).unwrap();
    main.complete_with(main.outstanding()[0], vec![refusal(0)]);
    let _ = tick(&mut w);
    let _ = tick(&mut w);

    assert!(src.is_blocked());
    assert!(src.committed().is_empty());
    let why = w.reader.block_reason.as_deref().expect("says why");
    assert!(why.contains("refused a record and is set to block"), "{why}");
    assert_eq!(w.reader.block_cause, Some(BlockCause::Refused));
}

/// Through the egress stage: a destination set to block keeps a refused row and sends it again. The source blocks while
/// it is refused, and resumes on its own once the row lands -- no redeploy, since the fix for a refusal is usually the
/// destination's own.
#[test]
fn a_blocking_destination_blocks_the_source_until_its_refused_row_lands() {
    let dest = FakeDestination::new();
    dest.reject_next_sends(1);
    let src = FakeSource::new([record("a", 7, 0)]);
    let mut stage = Egress::start(
        0,
        "d",
        Box::new(dest.clone()),
        Duration::from_secs(5),
        16_000_000,
        MetricsHandle::new(Arc::new(Metrics::new()), "test"),
    )
    .holding_refusals();
    let mut w = worker(plain_route(), vec![pipeline(0, vec![])], &src);
    w.add_destination(target(0, stage.handle()));

    let _ = tick(&mut w);
    let mut saw_blocked = false;
    for _ in 0..600 {
        let _ = tick(&mut w);
        saw_blocked |= src.is_blocked();
        if !src.committed().is_empty() {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(saw_blocked, "blocked while the row was refused");
    assert_eq!(src.committed().len(), 1, "committed once it landed");
    assert!(!src.is_blocked(), "resumed on its own");
    assert_eq!(dest.sent().len(), 1, "sent again, and taken");
    assert_eq!(
        counter(
            &w,
            "smeltr_rows_rejected_total",
            &[("source", "0"), ("destination", "0")]
        ),
        Some(1)
    );
}

/// A worker over `routes` whose topology knows each destination's DLQs, as a deployed one does: the serializer reads a
/// DLQ's identity field only for a DLQ it knows of.
fn worker_with_dlqs(
    routes: Vec<RouteConfig>,
    pipelines: Vec<PipelineConfig>,
    source: &FakeSource,
    dlq_of: &[(DestId, Vec<DestId>)],
) -> Worker {
    let mut topology = compile_topology(&routes, &pipelines).expect("compiles");
    topology.dlq_of = dlq_of.iter().cloned().collect();
    Worker::new(
        0,
        Box::new(source.clone()),
        Encoding::Auto,
        topology,
        Vec::new(),
        MemoryBudget::unlimited(),
        MetricsHandle::new(Arc::new(Metrics::new()), "t0"),
    )
}

/// Complete every unit `dest` has in flight, one completion each, and tick.
fn complete_all(w: &mut Worker, dest: &AsyncDestination, batch: BatchId, units: usize) {
    for _ in 0..units {
        dest.complete(batch);
    }
    tick(w).unwrap();
}

/// Two destinations that refuse into one DLQ send it two units of one batch. The batch waits for both: the first
/// acknowledgment must not stand for the second.
#[test]
fn a_dlq_owed_two_units_of_a_batch_acknowledges_it_after_both() {
    let src = FakeSource::new([record("a", 10, 0)]);
    let mut w = worker_with_dlqs(
        vec![route_with_deliver(deliver_to(&[0, 1]))],
        vec![pipeline(0, vec![])],
        &src,
        &[(0, vec![2]), (1, vec![2])],
    );
    let (a, b, dlq) = (
        AsyncDestination::new(),
        AsyncDestination::new(),
        AsyncDestination::new(),
    );
    w.add_destination(target_with(0, a.clone(), salvage_to(&[2])));
    w.add_destination(target_with(1, b.clone(), salvage_to(&[2])));
    w.add_destination(target(2, dlq.clone()));
    tick(&mut w).unwrap();
    let batch = a.outstanding()[0];
    a.complete_with(batch, vec![refusal(0)]);
    b.complete_with(batch, vec![refusal(0)]);
    tick(&mut w).unwrap();
    assert_eq!(salvaged_units(&dlq).len(), 2, "one unit from each");

    let units = dlq.records().len();
    complete_all(&mut w, &dlq, batch, units - 1);
    assert!(src.committed().is_empty(), "one unit on the DLQ is still out");
    complete_all(&mut w, &dlq, batch, 1);
    assert_eq!(src.committed().len(), 1);
}

/// Each DLQ is sent a refused row keyed on its own identity field, not the refusing destination's.
#[test]
fn each_dlq_keys_a_salvaged_row_on_its_own_identity() {
    let src = FakeSource::new([record("a", 10, 0)]);
    let mut w = worker_with_dlqs(
        plain_route(),
        vec![pipeline(0, vec![code(0, r#"k = "key-1";"#)])],
        &src,
        &[(0, vec![1, 2])],
    );
    let (main, keyed_dlq, plain_dlq) = (
        AsyncDestination::new(),
        AsyncDestination::new(),
        AsyncDestination::new(),
    );
    w.add_destination(target_with(0, main.clone(), salvage_to(&[1, 2])));
    w.add_destination(keyed(1, keyed_dlq.clone(), Some("k")));
    w.add_destination(target(2, plain_dlq.clone()));
    tick(&mut w).unwrap();
    main.complete_with(main.outstanding()[0], vec![refusal(0)]);
    tick(&mut w).unwrap();

    let keyed_rows = &salvaged_units(&keyed_dlq)[0].1;
    assert_eq!(keyed_rows[0].doc_id.as_deref(), Some("key-1"), "keyed on its own field");
    let plain_rows = &salvaged_units(&plain_dlq)[0].1;
    assert_eq!(plain_rows[0].doc_id, None, "a DLQ with no identity gets none");
}

/// A row a delivery rule already sent to the DLQ is not sent there again when its destination refuses it.
#[test]
fn a_refused_row_is_not_salvaged_to_a_dlq_that_already_has_it() {
    let src = FakeSource::new([record("a", 10, 0)]);
    let mut w = worker_with_dlqs(
        vec![route_with_deliver(deliver_to(&[0, 1]))],
        vec![pipeline(0, vec![])],
        &src,
        &[(0, vec![1])],
    );
    let (main, dlq) = (AsyncDestination::new(), AsyncDestination::new());
    w.add_destination(target_with(0, main.clone(), salvage_to(&[1])));
    w.add_destination(target(1, dlq.clone()));
    tick(&mut w).unwrap();
    let batch = main.outstanding()[0];
    main.complete_with(batch, vec![refusal(0)]);
    dlq.complete(batch);
    tick(&mut w).unwrap();

    assert_eq!(dlq.records().len(), 1, "only the rule's own unit");
    assert_eq!(src.committed().len(), 1);
}

/// A destination that writes Parquet is handed each row as columns cut from the batch. A row bound for it alone is
/// written as no JSON; one bound for a JSON destination too carries both. A tagged row's columns hold its tag.
#[test]
fn a_parquet_destination_is_handed_columns_and_no_json() {
    use arrow_array::Array;
    use arrow_array::cast::AsArray;
    use arrow_array::types::Int64Type;
    use smeltr::io::parquet::write;
    use smeltr::plan::config::DEFAULT_TAG_FIELD;
    use smeltr::plan::topology::Compression;

    let deliver = vec![rule(None, &[0]), rule(Some("_raw == \"1\""), &[1])];
    let routes = answering(tagging_repairs(), vec![route_with_deliver(deliver)]);
    let pipelines = vec![pipeline(0, vec![code(0, "n: i64 = _raw as i64;")])];
    let mut topology = compile_topology(&routes, &pipelines).expect("topology compiles");
    let written = smeltr::plan::dictionary::record_type(topology.plan_for(0, 0).unwrap(), DEFAULT_TAG_FIELD);
    topology.records.insert(0, [(Some(0), written)].into());
    let src = FakeSource::new([record("1", 10, 0), record("x", 11, 0)]);
    let mut w = Worker::new(
        0,
        Box::new(src.clone()),
        Encoding::Auto,
        topology,
        Vec::new(),
        MemoryBudget::unlimited(),
        MetricsHandle::new(Arc::new(Metrics::new()), "t0"),
    );
    let (columnar, json) = (FakeDestination::new(), FakeDestination::new());
    w.add_destination(target(0, columnar.clone()));
    w.add_destination(target(1, json.clone()));
    assert_eq!(tick(&mut w).unwrap(), 2);

    let sent = columnar.sent();
    assert_eq!(body(&sent[0].value)["n"], 1, "bound for the JSON destination too");
    assert!(sent[1].value.is_empty(), "no JSON where no destination reads it");
    let rows: Vec<_> = sent
        .iter()
        .map(|r| r.columns.clone().expect("cut into columns"))
        .collect();
    assert_eq!(json.sent().len(), 1);

    let path = std::env::temp_dir().join(format!("smeltr-runtime-{}.parquet", std::process::id()));
    std::fs::write(&path, write(&rows, Compression::None)).unwrap();
    let file = std::fs::File::open(&path).unwrap();
    let mut reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();
    std::fs::remove_file(&path).unwrap();
    let batch = reader.next().unwrap().unwrap();
    let n = batch.column_by_name("n").unwrap().as_primitive::<Int64Type>();
    assert_eq!(n.iter().collect::<Vec<_>>(), [Some(1), None]);
    let smeltr = batch.column_by_name("smeltr").unwrap().as_struct();
    let tag = smeltr.column_by_name("exception").unwrap().as_struct();
    let route = tag.column_by_name("route").unwrap().as_string::<i32>();
    assert!(route.is_null(0));
    assert_eq!(route.value(1), "r");
    let exceptions = tag.column_by_name("exceptions").unwrap().as_list::<i32>().value(1);
    let message = exceptions
        .as_struct()
        .column_by_name("message")
        .unwrap()
        .as_string::<i32>();
    assert!(message.value(0).contains("i64"), "{}", message.value(0));
}

/// A row no route takes reaches a Parquet destination through the group's answer as the `{_raw: str}` record's columns.
#[test]
fn an_unmatched_row_reaches_a_parquet_destination_as_its_raw_text() {
    use smeltr::core::column::DataType;
    let routes = vec![route_cfg(Some("false"), 0, deliver_to(&[0]))];
    let pipelines = vec![pipeline(0, vec![])];
    let metas = SourceMetas::from([(0, SourceMeta::KAFKA)]);
    let mut on_exception = OnException::default();
    on_exception.answers[ExceptionKind::Unmatched.index()] = deliver(&[0]);
    let mut topology = compile_topology_with(&routes, &pipelines, on_exception, &metas).expect("topology compiles");
    let raw = DataType::Struct(vec![("_raw".into(), DataType::Str)], false);
    topology.records.insert(0, [(None, raw)].into());
    let src = FakeSource::new([record("a", 10, 0), record("b", 11, 0)]);
    let mut w = Worker::new(
        0,
        Box::new(src.clone()),
        Encoding::Auto,
        topology,
        Vec::new(),
        MemoryBudget::unlimited(),
        MetricsHandle::new(Arc::new(Metrics::new()), "t0"),
    );
    let dest = FakeDestination::new();
    w.add_destination(target(0, dest.clone()));
    tick(&mut w).unwrap();
    let rows: Vec<_> = dest
        .sent()
        .iter()
        .map(|r| r.columns.clone().expect("cut into columns"))
        .collect();
    assert_eq!(rows.iter().map(|c| c.row).collect::<Vec<_>>(), [0, 1]);
    assert!(Arc::ptr_eq(&rows[0].chunk, &rows[1].chunk), "one chunk for the batch");
}
