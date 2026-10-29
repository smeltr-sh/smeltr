//! A `metrics::MetricsSource` is an ordinary `Source`: a worker built
//! around one runs through the same routes, pipelines and destinations.

use std::sync::Arc;
use std::time::Duration;

use smeltr::core::buffer::MemoryBudget;
use smeltr::core::id::FunctionId;
use smeltr::io::Source;
use smeltr::io::fake::FakeDestination;
use smeltr::metrics::{Metrics, MetricsHandle, MetricsSource, MetricsSourceConfig};
use smeltr::plan::config::{Answer, FunctionConfig, FunctionKind, PipelineConfig, Step};
use smeltr::plan::topology::{DeliverRule, RouteConfig, SourceMeta, SourceMetas, compile_topology_with};
use smeltr::runtime::{DestinationTarget, Worker};

fn code(id: FunctionId, body: &str) -> Step {
    Step::Function(FunctionConfig::new(id, FunctionKind::Code { body: body.into() }))
}

#[test]
fn a_worker_around_a_metrics_source_ships_real_counters_as_ordinary_json_rows() {
    // Some other thread's activity, in the process-wide registry.
    let metrics = Arc::new(Metrics::new());
    metrics.incr("smeltr_rows_polled_total", &[("source", "0")], 42);

    let routes = vec![RouteConfig {
        id: "r".to_string(),
        source: 9,
        cond: None,
        pipeline: 0,
        deliver: vec![DeliverRule {
            when: None,
            to: vec![0],
        }],
        on_exception: Default::default(),
    }];
    let pipelines = vec![PipelineConfig {
        id: 0,
        name: "metrics-out".into(),
        body: vec![code(0, "metric: str; value: i64; Object.assign($, json(_raw));")],
        step_ids: Vec::new(),
        lookup_tables: Default::default(),
        grok_patterns: Default::default(),
    }];
    // A metrics row carries the topic the source stamps it with, and no
    // partition or offset a pipeline could mean anything by.
    let metas = SourceMetas::from([(9, SourceMeta::METRICS)]);
    let topology = compile_topology_with(&routes, &pipelines, Default::default(), &metas).unwrap();

    let mut w = Worker::new(
        9,
        Box::new(MetricsSource::new(
            metrics.clone(),
            MetricsSourceConfig { bucket: Duration::ZERO },
        )),
        topology,
        Vec::new(),
        MemoryBudget::unlimited(),
        MetricsHandle::new(metrics.clone(), "t0"),
    );
    let dest = FakeDestination::new();
    w.add_destination(DestinationTarget {
        id: 0,
        name: "0".to_string(),
        destination: Box::new(dest.clone()),
        on_reject: Answer::Block,
        wants_doc_id: false,
        placement: Some(smeltr::plan::topology::Placement::Topic),
        id_field: None,
    });

    // Beside it ride the worker's own counters for source 9, started at 0.
    w.tick(100, 1 << 20, Duration::from_millis(1)).unwrap();
    let rows: Vec<serde_json::Value> = dest
        .sent()
        .iter()
        .map(|m| serde_json::from_slice::<serde_json::Value>(&m.value).unwrap())
        .collect();
    let parsed = rows
        .iter()
        .find(|row| row["source"] == "0")
        .unwrap_or_else(|| panic!("no row for source 0: {rows:?}"));
    assert_eq!(parsed["metric"], "smeltr_rows_polled_total");
    assert_eq!(parsed["source"], "0");
    assert_eq!(parsed["value"], 42);
    assert!(parsed["ts"].is_number());

    // The source has its own cursor: it does not report the same increment
    // twice, and neither another source nor the heartbeat loses it. The
    // worker counts its own polls under source 9, so only the first row is
    // source 0.
    w.tick(100, 1 << 20, Duration::from_millis(1)).unwrap();
    let source_0: Vec<serde_json::Value> = dest
        .sent()
        .iter()
        .map(|m| serde_json::from_slice::<serde_json::Value>(&m.value).unwrap())
        .filter(|row| row["metric"] == "smeltr_rows_polled_total" && row["source"] == "0")
        .collect();
    let total: i64 = source_0.iter().map(|row| row["value"].as_i64().unwrap()).sum();
    assert_eq!(total, 42, "the same increment was reported twice: {source_0:?}");

    let mut second = MetricsSource::new(metrics.clone(), MetricsSourceConfig { bucket: Duration::ZERO });
    let read: Vec<serde_json::Value> = second
        .poll_batch(100, 1 << 20, Duration::ZERO)
        .unwrap()
        .iter()
        .map(|r| serde_json::from_slice::<serde_json::Value>(r.bytes().unwrap()).unwrap())
        .collect();
    assert!(
        read.iter()
            .any(|row| row["metric"] == "smeltr_rows_polled_total" && row["source"] == "0" && row["value"] == 42),
        "a second source reads the increment too: {read:?}"
    );

    let heartbeat = metrics.peek_delta(smeltr::metrics::SUBSCRIBER_HEARTBEAT);
    assert!(
        heartbeat
            .iter()
            .any(|s| s.key.name == "smeltr_rows_polled_total" && s.key.labels.contains(&("source", "0".to_string()))),
        "the heartbeat still holds the increment: {heartbeat:?}"
    );
}

/// `MetricsSource` stamps every record with `metrics::METRICS_TOPIC` so a
/// route can pick metrics rows out like any other source's. Declaring the
/// field is what keeps that expressible.
#[test]
fn a_route_can_select_metrics_rows_by_their_topic() {
    use smeltr::plan::compile::compile;
    use smeltr::plan::topology::SourceMeta;

    let p = PipelineConfig {
        id: 0,
        name: "select".into(),
        body: vec![code(0, "from_metrics = __source.topic;")],
        step_ids: Vec::new(),
        lookup_tables: Default::default(),
        grok_patterns: Default::default(),
    };
    compile(&p, &SourceMeta::METRICS).expect("a metrics row carries __source.topic");
}
