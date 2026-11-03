//! The OTLP destination (`io::otlp::export` over `io::http`) between two of smeltr's own OTLP receivers: what one
//! receiver takes, carried through a pipeline that keeps its resource and scope, arrives at the other as it was.

use std::borrow::Cow;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use std::time::Duration;

use serde_json::{Value, json};
use smeltr::core::batch::Batch;
use smeltr::io::http::{HttpDestination, agent_config};
use smeltr::io::otlp::Encoding;
use smeltr::io::otlp::export::Otlp;
use smeltr::io::{Destination, Dispatched, OwnedRecord, Source, SourceRecord, build_source, http_post};
use smeltr::plan::config::Payload;
use smeltr::plan::topology::{
    Compression, DestinationKind, HeldSecrets, OtlpDestinationConfig, Receiver, ReceiverConfig, SecretRef, Signal,
    SourceKind, SourceMeta,
};

use crate::harness::{Env, code, compile_ok_for};

/// The step that carries what a receiver's record came under into the fields the destination sends it under.
pub const CARRY: &str = "resource = __source.resource; \
    scope = {name: __source.scope.name, version: __source.scope.version, attributes: __source.scope.attributes};";

pub fn free_port() -> SocketAddr {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}

/// A receiver of `signal` admitting `s3cret` as a bearer token.
pub fn receiver(listen: SocketAddr, signal: Signal) -> Box<dyn Source> {
    let mut secrets = HeldSecrets::default();
    secrets.set_stored([("hook".to_string(), 1, "s3cret".to_string())]);
    let config = ReceiverConfig {
        receiver: Receiver::Otlp(signal),
        listen,
        tokens: vec![(
            "sender".into(),
            SecretRef::LeaderStored {
                name: "hook".into(),
                version: 1,
            },
        )],
        tls: None,
        max_request_bytes: 1 << 20,
    };
    let registry = Arc::new(smeltr::metrics::Metrics::new());
    let subjects = Arc::new(smeltr::metrics::Subjects::default());
    build_source(&SourceKind::Receiver(config), &registry, &subjects, &secrets, None).expect("opens")
}

/// POSTs `body`, an OTLP export of `content_type`, to `at`, and answers the status.
pub fn post(at: SocketAddr, signal: Signal, content_type: &str, body: &[u8]) -> u16 {
    let mut stream = TcpStream::connect(at).unwrap();
    write!(
        stream,
        "POST {} HTTP/1.1\r\nHost: receiver\r\nAuthorization: Bearer s3cret\r\nContent-Type: {content_type}\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n",
        smeltr::io::otlp::http_path(signal),
        body.len()
    )
    .unwrap();
    stream.write_all(body).unwrap();
    let mut answer = String::new();
    let _ = stream.read_to_string(&mut answer);
    answer.get(9..12).and_then(|s| s.parse().ok()).unwrap_or(0)
}

/// `n` records of `source`, each batch committed once read, so its sender hears it answered.
fn take(source: &mut Box<dyn Source>, n: usize) -> Vec<SourceRecord> {
    let mut records = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while records.len() < n {
        assert!(
            std::time::Instant::now() < deadline,
            "only {} of {n} records arrived",
            records.len()
        );
        let batch = source.poll_batch(1000, 1 << 24, Duration::from_millis(100)).unwrap();
        crate::harness::receiver::commit(source, &batch);
        records.extend(batch);
    }
    records
}

/// Each record as its pipeline reads it: its record, and what it came under.
fn read(signal: Signal, records: &[SourceRecord]) -> Vec<Value> {
    let meta = SourceMeta::otlp(signal);
    let filed = meta.objects(records, Some("in"));
    (records.iter().zip(filed))
        .map(|(record, mut filed)| {
            let fields: Value = serde_json::from_slice(&meta.record_json(record, "")).unwrap();
            filed.retain(|key, _| key == "resource" || key.starts_with("scope"));
            json!({"record": fields, "filed": filed})
        })
        .collect()
}

/// The JSON rows a pipeline running [`CARRY`] makes of `records`, as egress hands them to a destination. A batch is
/// one chunk's records, as a poll ends where what they came under changes.
pub fn carried(signal: Signal, records: &[SourceRecord]) -> Vec<OwnedRecord> {
    let meta = SourceMeta::otlp(signal);
    let plan = compile_ok_for(meta.clone(), vec![code(0, CARRY)]);
    let mut env = Env::new();
    let mut rows = Vec::new();
    for batch in records.chunk_by(|a, b| Arc::ptr_eq(&a.chunk, &b.chunk)) {
        let mut columns = Batch::new(&plan.slot_layout, batch.len(), 0);
        let none: &[&str] = &[];
        meta.ingest(&mut columns, batch, none, Some("in"), |_| true, &mut env.pool);
        rows.extend(env.run(&plan, &mut columns).into_iter().map(|value| OwnedRecord {
            value,
            payload: Payload::Record,
            ..OwnedRecord::default()
        }));
        columns.reclaim(&mut env.pool);
    }
    rows
}

/// An OTLP destination of `signal` sending to `to`.
fn destination(to: SocketAddr, signal: Signal, compression: Compression) -> OtlpDestinationConfig {
    OtlpDestinationConfig {
        http: smeltr::plan::topology::HttpDestinationConfig {
            url: format!("http://{to}"),
            zone_urls: Default::default(),
            body: Default::default(),
            payload: Payload::Record,
            compression,
            headers: vec![("Authorization".into(), "Bearer s3cret".into())],
            credentials: Vec::new(),
            tls: None,
            in_flight: 1,
            max_request_bytes: 1 << 20,
            buffer_bytes: 1 << 22,
            stall_after: Duration::from_secs(20),
        },
        signal,
    }
}

/// Receives `export` at one receiver, carries its records through [`CARRY`] to an OTLP destination, and answers what
/// each receiver read.
fn round_trip(signal: Signal, export: &Value, items: usize, compression: Compression) -> (Vec<Value>, Vec<Value>) {
    let (from, to) = (free_port(), free_port());
    let (mut first, mut second) = (receiver(from, signal), receiver(to, signal));
    let body = export.to_string().into_bytes();
    let sent = std::thread::spawn(move || post(from, signal, "application/json", &body));
    let received = take(&mut first, items);
    assert_eq!(sent.join().unwrap(), 200);

    let config = destination(to, signal, compression);
    let agent = agent_config(None, &HeldSecrets::default()).unwrap();
    let transport = http_post::transport_as(&config.http, Encoding::Protobuf.content_type(), &[], &agent);
    let rows = carried(signal, &received);
    let delivered = std::thread::spawn(move || {
        let mut d = HttpDestination::new(transport, Otlp::new(&config));
        d.deliver(0, Cow::Owned(rows)).map(|(dispatched, refused)| {
            assert_eq!(dispatched, Dispatched::Complete);
            refused.into_iter().map(|r| r.reason).collect::<Vec<_>>()
        })
    });
    let arrived = take(&mut second, items);
    let refused = delivered.join().unwrap().expect("delivers");
    assert!(refused.is_empty(), "refused: {refused:?}");
    (read(signal, &received), read(signal, &arrived))
}

/// Attributes of every type OTLP has but bytes, nested.
fn attributes(prefix: &str) -> Value {
    json!([
        {"key": format!("{prefix}.text"), "value": {"stringValue": "checkout"}},
        {"key": format!("{prefix}.flag"), "value": {"boolValue": true}},
        {"key": format!("{prefix}.count"), "value": {"intValue": "42"}},
        {"key": format!("{prefix}.whole"), "value": {"doubleValue": 2.0}},
        {"key": format!("{prefix}.ratio"), "value": {"doubleValue": 0.25}},
        {"key": format!("{prefix}.list"), "value": {"arrayValue": {"values": [{"intValue": "1"}, {"stringValue": "a"}]}}},
        {"key": format!("{prefix}.map"), "value": {"kvlistValue": {"values": [
            {"key": "inner", "value": {"doubleValue": 1.5}}
        ]}}},
    ])
}

/// Two resources, the first with two scopes, each holding `items`.
fn export(resources_key: &str, scopes_key: &str, items_key: &str, items: impl Fn(usize) -> Value) -> Value {
    let scope = |name: &str, version: &str, at: usize| {
        json!({
            "scope": {"name": name, "version": version, "attributes": attributes("scope")},
            items_key: items(at),
        })
    };
    json!({resources_key: [
        {"resource": {"attributes": attributes("service")}, scopes_key: [scope("io.a", "1.2.0", 0), scope("io.b", "", 1)]},
        {"resource": {"attributes": [{"key": "host.name", "value": {"stringValue": "web-2"}}]},
         scopes_key: [{"scope": {}, items_key: items(2)}]},
    ]})
}

const TRACE: &str = "5b8efff798038103d269b633813fc60c";
const SPAN: &str = "eee19b7ec3c1b174";

#[test]
fn log_records_arrive_as_they_were_received() {
    let logs = export("resourceLogs", "scopeLogs", "logRecords", |at| {
        json!([
            {"timeUnixNano": "1700000000123456000", "observedTimeUnixNano": "1700000000223456000",
             "severityNumber": 9, "severityText": "INFO", "body": {"stringValue": format!("paid {at}")},
             "attributes": attributes("log"), "traceId": TRACE, "spanId": SPAN, "flags": 1,
             "eventName": "payment", "droppedAttributesCount": 2},
            {"body": {"kvlistValue": {"values": [{"key": "n", "value": {"intValue": "7"}}]}}},
        ])
    });
    for compression in [Compression::None, Compression::Gzip] {
        let (received, arrived) = round_trip(Signal::Logs, &logs, 6, compression);
        assert_eq!(arrived, received, "{compression:?}");
    }
}

#[test]
fn spans_arrive_as_they_were_received() {
    let traces = export("resourceSpans", "scopeSpans", "spans", |at| {
        json!([
            {"traceId": TRACE, "spanId": SPAN, "parentSpanId": "0102030405060708", "traceState": "a=b",
             "flags": 257, "name": format!("checkout {at}"), "kind": 2,
             "startTimeUnixNano": "1700000000000001000", "endTimeUnixNano": "1700000000500000000",
             "attributes": attributes("span"), "droppedAttributesCount": 1,
             "events": [{"timeUnixNano": "1700000000100000000", "name": "retry", "attributes": attributes("event"),
                         "droppedAttributesCount": 3}],
             "droppedEventsCount": 4,
             "links": [{"traceId": "0af7651916cd43dd8448eb211c80319c", "spanId": "b7ad6b7169203331",
                        "traceState": "c=d", "attributes": attributes("link"), "flags": 1}],
             "droppedLinksCount": 5, "status": {"code": 2, "message": "card declined"}},
            {"traceId": TRACE, "spanId": "0000000000000001"},
        ])
    });
    let (received, arrived) = round_trip(Signal::Traces, &traces, 6, Compression::None);
    assert_eq!(arrived, received);
}

#[test]
fn metric_points_arrive_as_they_were_received() {
    let exemplar = json!({"timeUnixNano": "1700000000000002000", "asDouble": 0.5, "traceId": TRACE, "spanId": SPAN,
                          "filteredAttributes": attributes("exemplar")});
    let metrics = export("resourceMetrics", "scopeMetrics", "metrics", |at| {
        let point = |extra: Value| {
            let mut p = json!({"startTimeUnixNano": "1700000000000000000", "timeUnixNano": "1700000060000000000",
                               "attributes": attributes("point"), "flags": 1});
            p.as_object_mut().unwrap().extend(extra.as_object().unwrap().clone());
            p
        };
        json!([
            {"name": format!("queue.depth.{at}"), "unit": "1", "description": "items waiting",
             "metadata": [{"key": "source", "value": {"stringValue": "sdk"}}],
             "gauge": {"dataPoints": [point(json!({"asInt": "12", "exemplars": [exemplar]})),
                                      point(json!({"asDouble": 12.0}))]}},
            {"name": "requests", "sum": {"aggregationTemporality": 2, "isMonotonic": true,
             "dataPoints": [point(json!({"asInt": "1000"}))]}},
            {"name": "latency", "unit": "ms", "histogram": {"aggregationTemporality": 1, "dataPoints": [point(json!({
                "count": "6", "sum": 31.5, "min": 0.5, "max": 20.0, "bucketCounts": ["1", "2", "3"],
                "explicitBounds": [1.0, 10.0], "exemplars": [exemplar]}))]}},
            {"name": "size", "exponentialHistogram": {"aggregationTemporality": 2, "dataPoints": [point(json!({
                "count": "7", "sum": 70.0, "scale": 3, "zeroCount": "1", "zeroThreshold": 0.001,
                "positive": {"offset": -2, "bucketCounts": ["1", "0", "5"]},
                "negative": {"offset": 1, "bucketCounts": ["0"]}}))]}},
            {"name": "rtt", "summary": {"dataPoints": [point(json!({"count": "4", "sum": 10.0,
                "quantileValues": [{"quantile": 0.5, "value": 2.0}, {"quantile": 0.99, "value": 4.5}]}))]}},
        ])
    });
    let (received, arrived) = round_trip(Signal::Metrics, &metrics, 18, Compression::Gzip);
    assert_eq!(arrived, received);
}

/// A row without the fields it came under is sent under an empty resource and scope, and one OTLP has no place for
/// is refused naming why, while the rest land.
#[test]
fn a_row_of_no_resource_lands_and_a_row_otlp_cannot_take_is_refused() {
    let to = free_port();
    let mut second = receiver(to, Signal::Traces);
    let config = destination(to, Signal::Traces, Compression::None);
    let agent = agent_config(None, &HeldSecrets::default()).unwrap();
    let transport = http_post::transport_as(&config.http, Encoding::Protobuf.content_type(), &[], &agent);
    let row = |json: &str| OwnedRecord {
        value: json.as_bytes().to_vec(),
        payload: Payload::Record,
        ..OwnedRecord::default()
    };
    let rows = vec![
        row(&format!(
            r#"{{"traceId":"{TRACE}","spanId":"{SPAN}","name":"bare","other":1}}"#
        )),
        row(r#"{"name":"no ids"}"#),
        row(&format!(r#"{{"traceId":"{TRACE}","spanId":"short"}}"#)),
    ];
    let delivered = std::thread::spawn(move || {
        let mut d = HttpDestination::new(transport, Otlp::new(&config));
        d.deliver(0, Cow::Owned(rows)).map(|(_, refused)| refused)
    });
    let arrived = read(Signal::Traces, &take(&mut second, 1));
    let refused = delivered.join().unwrap().expect("delivers");
    assert_eq!(
        arrived,
        vec![json!({"record": {"traceId": TRACE, "spanId": SPAN, "name": "bare"}, "filed": {}})]
    );
    let reasons: Vec<&str> = refused.iter().map(|r| r.reason.as_str()).collect();
    assert_eq!(reasons.len(), 2, "{reasons:?}");
    assert!(reasons[0].contains("traceId and a spanId"), "{reasons:?}");
    assert!(reasons[1].contains("`spanId`"), "{reasons:?}");
}

/// A destination of each signal is loaded from a bundle as its settings say.
#[test]
fn an_otlp_destination_loads_from_a_bundle() {
    let bundle = json!({
        "version": 1,
        "sources": [{"id": "in", "type": "otlpReceiver", "port": 4318, "signal": "traces",
                     "tokens": {"ci": {"secret": "hook", "version": 1}}}],
        "pipelines": [{"id": "p", "name": "p", "steps": [{"id": "carry", "type": "code", "body": CARRY}]}],
        "destinations": [{"id": "out", "type": "otlp", "url": "https://collector.example.com:4318", "signal": "traces",
                          "compression": "gzip", "headers": {"X-Tenant": "a"}}],
        "routes": [{"id": "r", "source": "in", "pipeline": "p", "deliver": [{"to": ["out"]}]}],
    });
    let system = smeltr::config_file::load_str(&bundle.to_string()).unwrap();
    let DestinationKind::Otlp(c) = &system.topology.destinations[0].kind else {
        panic!("loads as {:?}", system.topology.destinations[0].kind);
    };
    assert_eq!(c.signal, Signal::Traces);
    assert_eq!(c.http.compression, Compression::Gzip);
    assert_eq!(c.http.url, "https://collector.example.com:4318/");
    assert_eq!(c.http.headers, vec![("X-Tenant".to_string(), "a".to_string())]);
}

/// The destination adds its signal's path, so a URL already holding one, as an exporter's per-signal endpoint does,
/// is refused rather than sent to twice the path.
#[test]
fn a_url_naming_a_signals_path_is_refused() {
    for url in [
        "https://collector.example.com:4318/v1/logs",
        "https://c.example/otlp/v1/traces/",
    ] {
        let bundle = json!({
            "version": 1,
            "sources": [{"id": "s", "type": "kafka", "brokers": "b:9092", "topics": ["t"], "groupId": "g"}],
            "pipelines": [{"id": "p", "name": "p", "steps": []}],
            "destinations": [{"id": "out", "type": "otlp", "url": url}],
            "routes": [{"id": "r", "source": "s", "pipeline": "p", "deliver": [{"to": ["out"]}]}],
        });
        let file = serde_json::from_value(bundle).unwrap();
        let errors = smeltr::config_file::validation_errors(file);
        assert!(
            errors
                .iter()
                .any(|e| e.to_string().contains("which the destination adds itself")),
            "{url}: {errors:?}"
        );
    }
}
