//! What a deployment reports of its streams (design doc, Observability: Streams): the fields a Parser produced from
//! the one batch an interval tallies, and the keys of the rows no route took, every batch.

use std::sync::Arc;
use std::time::Duration;

use smeltr::core::buffer::MemoryBudget;
use smeltr::core::id::FunctionId;
use smeltr::format::encoding::Encoding;
use smeltr::handoff::{Handed, Ready, reports};
use smeltr::io::SourceRecord;
use smeltr::io::fake::AsyncDestination;
use smeltr::metrics::{Metrics, MetricsHandle};
use smeltr::plan::config::{Answer, FunctionConfig, FunctionKind, OnException, PipelineConfig, Step};
use smeltr::plan::topology::{DeliverRule, RouteConfig, compile_topology};
use smeltr::runtime::{DestinationTarget, Lane, Transform};
use smeltr::streams::Streams;

fn code(id: FunctionId, body: &str) -> Step {
    Step::Function(FunctionConfig::new(id, FunctionKind::Code { body: body.into() }))
}

/// One source, `orders`, with a route that takes only even offsets through a JSON Parser, drained by one transform
/// thread whose streams are accounted to `streams`.
fn transform(streams: &Arc<Streams>) -> (Transform, Arc<Lane>, AsyncDestination) {
    transform_answering(streams, Answer::Drop)
}

/// `transform`, with every exception answered with `answer`.
fn transform_answering(streams: &Arc<Streams>, answer: Answer) -> (Transform, Arc<Lane>, AsyncDestination) {
    let routes = vec![RouteConfig {
        id: "even".to_string(),
        source: 0,
        cond: Some("__source.offset % 2 == 0".to_string()),
        pipeline: 0,
        deliver: vec![DeliverRule {
            when: None,
            to: vec![0],
        }],
        on_exception: Default::default(),
    }];
    let pipelines = vec![PipelineConfig {
        id: 0,
        name: "normalize".into(),
        body: vec![code(0, "Object.assign($, json(_raw));")],
        step_ids: vec!["parse".to_string()],
        lookup_tables: Default::default(),
        grok_patterns: Default::default(),
    }];
    let mut topology = compile_topology(&routes, &pipelines).unwrap();
    topology.on_exception = OnException::every(answer);
    let topology = Arc::new(topology);
    let ready = Arc::new(Ready::new());
    let (tx, _rx) = reports();
    let lane = Arc::new(Lane::new(
        0,
        Some("orders".to_string()),
        smeltr::runtime::Reading {
            meta: topology.meta_for(0),
            encoding: Encoding::Auto,
        },
        vec![0],
        tx,
        ready.clone(),
        8_000_000,
    ));
    let dest = AsyncDestination::new();
    let target = DestinationTarget {
        id: 0,
        name: "es".to_string(),
        destination: Box::new(dest.clone()),
        on_reject: Answer::Drop,
        wants_doc_id: false,
        placement: Some(smeltr::plan::topology::Placement::Topic),
        id_field: None,
    };
    let transform = Transform::new(
        vec![lane.clone()],
        ready,
        0,
        topology,
        vec![target],
        MemoryBudget::unlimited(),
        MetricsHandle::new(Arc::new(Metrics::new()), "transform-0"),
    )
    .with_streams(streams.clone());
    (transform, lane, dest)
}

fn record(offset: i64, value: &[u8]) -> SourceRecord {
    SourceRecord {
        chunk: smeltr::io::RecordChunk::of_bytes(value),
        row: 0,
        offset,
        partition: 0,
        topic: "orders".into(),
        headers: None,
    }
}

fn push(lane: &Lane, batch: u64, records: Vec<SourceRecord>) {
    lane.batches
        .push(Handed { batch, records }, Duration::from_secs(1))
        .unwrap();
}

/// The first batch of an interval is tallied: its Parser's fields, with how many rows held each and as what, reach
/// the report under the source's, the route's, the pipeline's and the step's own ids. The second batch of the
/// interval is not.
#[test]
fn the_first_batch_of_an_interval_is_tallied_and_the_next_is_not() {
    let streams = Arc::new(Streams::new(
        1,
        vec!["normalize".to_string()],
        Duration::from_secs(3600),
    ));
    let (mut transform, lane, _dest) = transform(&streams);
    push(
        &lane,
        1,
        vec![
            record(0, br#"{"total": 1, "note": "x"}"#),
            record(2, br#"{"total": 2.5}"#),
            record(4, br#"{"total": "three"}"#),
        ],
    );
    transform.step(Duration::from_millis(5)).unwrap();
    push(&lane, 2, vec![record(6, br#"{"total": 4, "later": true}"#)]);
    transform.step(Duration::from_millis(5)).unwrap();

    let report = streams.drain().expect("the first batch was tallied");
    assert!(report.unmatched.is_empty(), "every record matched the route");
    let [shape] = report.parsers.as_slice() else {
        panic!("one Parser on one route: {report:?}")
    };
    assert_eq!(
        (
            shape.source.as_str(),
            shape.route.as_str(),
            shape.pipeline.as_str(),
            shape.step.as_str()
        ),
        ("orders", "even", "normalize", "parse")
    );
    assert_eq!(shape.rows, 3, "the first batch's rows, and not the second's");
    let total = shape
        .fields
        .iter()
        .find(|f| f.name == "total")
        .expect("seen in every row");
    assert_eq!((total.present, total.ints, total.floats, total.strings), (3, 1, 1, 1));
    assert!(!total.declared);
    let note = shape.fields.iter().find(|f| f.name == "note").expect("seen in one row");
    assert_eq!((note.present, note.strings), (1, 1));
    assert!(
        shape.fields.iter().all(|f| f.name != "later"),
        "the second batch was not tallied"
    );
    assert!(streams.drain().is_none(), "drained");
}

/// Every batch keys the rows no route took, so a stream that arrived while nothing selected it is reported from its
/// first row.
#[test]
fn rows_no_route_takes_are_keyed_on_every_batch() {
    let streams = Arc::new(Streams::new(
        1,
        vec!["normalize".to_string()],
        Duration::from_secs(3600),
    ));
    let (mut transform, lane, _dest) = transform(&streams);
    push(&lane, 1, vec![record(0, b"{}"), record(1, b"{}")]);
    transform.step(Duration::from_millis(5)).unwrap();
    push(&lane, 2, vec![record(3, b"{}")]);
    transform.step(Duration::from_millis(5)).unwrap();

    let report = streams.drain().expect("rows went unmatched");
    let [unmatched] = report.unmatched.as_slice() else {
        panic!("one source: {report:?}")
    };
    assert_eq!(unmatched.source, "orders");
    assert_eq!(unmatched.rows, 2, "one odd offset in each batch");
    assert_eq!(unmatched.keys.len(), 1, "a Kafka record's stream is its topic");
    assert_eq!((unmatched.keys[0].key.as_str(), unmatched.keys[0].rows), ("orders", 2));
}

/// A batch `Unmatched` blocks keys its rows no route took, once: it is held until a new config, and the turns that
/// meet it held key nothing more.
#[test]
fn a_blocked_batch_keys_its_unmatched_rows_once() {
    let streams = Arc::new(Streams::new(
        1,
        vec!["normalize".to_string()],
        Duration::from_secs(3600),
    ));
    let (mut transform, lane, _dest) = transform_answering(&streams, Answer::Block);
    push(&lane, 1, vec![record(0, b"{}"), record(1, b"{}")]);
    transform
        .step(Duration::from_millis(5))
        .expect_err("an odd offset matches no route, and Unmatched blocks");
    transform.step(Duration::from_millis(5)).ok();

    let report = streams.drain().expect("the blocked row was keyed");
    let [unmatched] = report.unmatched.as_slice() else {
        panic!("one source: {report:?}")
    };
    assert_eq!((unmatched.keys[0].key.as_str(), unmatched.keys[0].rows), ("orders", 1));
}

/// A batch the route took no row of is not the batch the interval tallies: the next one the route takes rows of is.
#[test]
fn a_batch_the_route_takes_nothing_of_does_not_spend_the_interval() {
    let streams = Arc::new(Streams::new(
        1,
        vec!["normalize".to_string()],
        Duration::from_secs(3600),
    ));
    let (mut transform, lane, _dest) = transform(&streams);
    push(&lane, 1, vec![record(1, b"{}"), record(3, b"{}")]);
    transform.step(Duration::from_millis(5)).unwrap();
    push(&lane, 2, vec![record(4, br#"{"later": true}"#)]);
    transform.step(Duration::from_millis(5)).unwrap();

    let report = streams.drain().expect("the second batch was tallied");
    let [shape] = report.parsers.as_slice() else {
        panic!("one Parser on one route: {report:?}")
    };
    assert_eq!(shape.rows, 1, "the second batch's one row");
    assert!(shape.fields.iter().any(|f| f.name == "later"));
    assert_eq!(report.unmatched.len(), 1, "the first batch's rows were keyed");
}
