//! A `io::logs::LogsSource` is an ordinary `Source`: a worker built around one runs through the same routes,
//! pipelines and destinations.
//!
//! A binary of its own: the buffer is the process's, and this test starts it.

use std::sync::Arc;
use std::time::{Duration, Instant};

use smeltr::core::buffer::MemoryBudget;
use smeltr::core::id::FunctionId;
use smeltr::io::fake::FakeDestination;
use smeltr::io::logs::{LogsSource, LogsSourceConfig};
use smeltr::metrics::{Metrics, MetricsHandle};
use smeltr::plan::config::{Answer, FunctionConfig, FunctionKind, PipelineConfig, Step};
use smeltr::plan::topology::{DeliverRule, RouteConfig, SourceMeta, SourceMetas, compile_topology_with};
use smeltr::runtime::{DestinationTarget, Worker};

fn code(id: FunctionId, body: &str) -> Step {
    Step::Function(FunctionConfig::new(id, FunctionKind::Code { body: body.into() }))
}

#[test]
fn a_worker_around_a_logs_source_ships_this_process_s_own_logs_as_json_rows() {
    smeltr::logs::start();

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
        name: "logs-out".into(),
        body: vec![code(
            0,
            "level: str; site: str; message: str; Object.assign($, json(_raw));",
        )],
        step_ids: Vec::new(),
        lookup_tables: Default::default(),
        grok_patterns: Default::default(),
    }];
    let metas = SourceMetas::from([(9, SourceMeta::LOGS)]);
    let topology = compile_topology_with(&routes, &pipelines, Default::default(), &metas).unwrap();

    // Built first: nothing is kept for a reader that is not there yet, which is what stops an unread queue
    // accumulating for the life of a process that declares no logs source.
    let source = LogsSource::new(LogsSourceConfig {
        min_level: tracing::Level::INFO,
    });
    tracing::error!(
        target: "logs_source.test",
        subject = "d1",
        rows = 5u64,
        "a destination refused a batch"
    );

    let registry = Arc::new(Metrics::new());
    let mut w = Worker::new(
        9,
        Box::new(source),
        topology,
        Vec::new(),
        MemoryBudget::unlimited(),
        MetricsHandle::new(registry.clone(), "t0"),
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

    // The drain thread is what fills the queue, so poll for it rather than race it.
    let began = Instant::now();
    let sent = loop {
        w.tick(100, 1 << 20, Duration::from_millis(20)).unwrap();
        let sent = dest.sent();
        if sent.iter().any(|r| {
            serde_json::from_slice::<serde_json::Value>(&r.value)
                .map(|v| v["site"] == "logs_source.test")
                .unwrap_or(false)
        }) {
            break sent;
        }
        assert!(
            began.elapsed() < Duration::from_secs(5),
            "the drain thread put it in the queue and the worker shipped it; got {} rows",
            sent.len()
        );
    };

    let mine: Vec<serde_json::Value> = sent
        .iter()
        .map(|r| serde_json::from_slice(&r.value).unwrap())
        .filter(|v: &serde_json::Value| v["site"] == "logs_source.test")
        .collect();
    assert_eq!(mine.len(), 1, "one call, one record: {mine:?}");
    let row = &mine[0];
    assert_eq!(row["level"], "error");
    assert_eq!(row["message"], "a destination refused a batch");
    assert!(row["timestamp"].is_number(), "{row}");
    // The whole point of shipping data rather than a sentence: what the call site passed is searchable without
    // anyone parsing the message.
    assert_eq!(row["fields"]["subject"], "d1");
    assert_eq!(row["fields"]["rows"], 5);
}

/// `LogsSource` stamps every record with `io::logs::LOGS_TOPIC` so a route can pick logs out like any
/// other source's. Declaring the field is what keeps that expressible.
#[test]
fn a_route_can_select_logs_rows_by_their_topic() {
    use smeltr::plan::compile::compile;

    let p = PipelineConfig {
        id: 0,
        name: "select".into(),
        body: vec![code(0, "from_logs = __source.topic;")],
        step_ids: Vec::new(),
        lookup_tables: Default::default(),
        grok_patterns: Default::default(),
    };
    compile(&p, &SourceMeta::LOGS).expect("a logs row carries __source.topic");
}
