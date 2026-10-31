//! The Prometheus destination's protocol (`io::remote_write`) over the HTTP core: what goes on the wire, what each
//! answer means, and a store taking what it sends.

use std::borrow::Cow;
use std::collections::VecDeque;
use std::io::{BufRead, BufReader, Read, Write};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use prost::Message;
use smeltr::io::http::{HttpDestination, Response, Transport, agent_config};
use smeltr::io::remote_write::{RemoteWrite, transport};
use smeltr::io::{Destination, Dispatched, IoError, OwnedRecord, Rejection};
use smeltr::plan::config::Payload;
use smeltr::plan::topology::{HeldSecrets, PrometheusDestinationConfig, RemoteWriteVersion};

use crate::live;

mod wire {
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct WriteRequest {
        #[prost(message, repeated, tag = "1")]
        pub timeseries: Vec<TimeSeries>,
        #[prost(message, repeated, tag = "3")]
        pub metadata: Vec<MetricMetadata>,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct MetricMetadata {
        #[prost(enumeration = "MetricType", tag = "1")]
        pub r#type: i32,
        #[prost(string, tag = "2")]
        pub metric_family_name: String,
        #[prost(string, tag = "4")]
        pub help: String,
        #[prost(string, tag = "5")]
        pub unit: String,
    }
    #[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
    pub enum MetricType {
        Unknown = 0,
        Counter = 1,
        Gauge = 2,
        Histogram = 3,
        GaugeHistogram = 4,
        Summary = 5,
        Info = 6,
        Stateset = 7,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct TimeSeries {
        #[prost(message, repeated, tag = "1")]
        pub labels: Vec<Label>,
        #[prost(message, repeated, tag = "2")]
        pub samples: Vec<Sample>,
        #[prost(message, repeated, tag = "3")]
        pub exemplars: Vec<Exemplar>,
        #[prost(message, repeated, tag = "4")]
        pub histograms: Vec<Histogram>,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Exemplar {
        #[prost(message, repeated, tag = "1")]
        pub labels: Vec<Label>,
        #[prost(double, tag = "2")]
        pub value: f64,
        #[prost(int64, tag = "3")]
        pub timestamp: i64,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Label {
        #[prost(string, tag = "1")]
        pub name: String,
        #[prost(string, tag = "2")]
        pub value: String,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Sample {
        #[prost(double, tag = "1")]
        pub value: f64,
        #[prost(int64, tag = "2")]
        pub timestamp: i64,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Histogram {
        #[prost(oneof = "Count", tags = "1, 2")]
        pub count: Option<Count>,
        #[prost(double, tag = "3")]
        pub sum: f64,
        #[prost(sint32, tag = "4")]
        pub schema: i32,
        #[prost(double, tag = "5")]
        pub zero_threshold: f64,
        #[prost(oneof = "ZeroCount", tags = "6, 7")]
        pub zero_count: Option<ZeroCount>,
        #[prost(message, repeated, tag = "8")]
        pub negative_spans: Vec<BucketSpan>,
        #[prost(sint64, repeated, tag = "9")]
        pub negative_deltas: Vec<i64>,
        #[prost(double, repeated, tag = "10")]
        pub negative_counts: Vec<f64>,
        #[prost(message, repeated, tag = "11")]
        pub positive_spans: Vec<BucketSpan>,
        #[prost(sint64, repeated, tag = "12")]
        pub positive_deltas: Vec<i64>,
        #[prost(double, repeated, tag = "13")]
        pub positive_counts: Vec<f64>,
        #[prost(enumeration = "ResetHint", tag = "14")]
        pub reset_hint: i32,
        #[prost(int64, tag = "15")]
        pub timestamp: i64,
        #[prost(double, repeated, tag = "16")]
        pub custom_values: Vec<f64>,
    }
    #[derive(Clone, PartialEq, prost::Oneof)]
    pub enum Count {
        #[prost(uint64, tag = "1")]
        Int(u64),
        #[prost(double, tag = "2")]
        Float(f64),
    }
    #[derive(Clone, PartialEq, prost::Oneof)]
    pub enum ZeroCount {
        #[prost(uint64, tag = "6")]
        Int(u64),
        #[prost(double, tag = "7")]
        Float(f64),
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct BucketSpan {
        #[prost(sint32, tag = "1")]
        pub offset: i32,
        #[prost(uint32, tag = "2")]
        pub length: u32,
    }
    #[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
    pub enum ResetHint {
        Unknown = 0,
        Yes = 1,
        No = 2,
        Gauge = 3,
    }
}

fn config(url: &str) -> PrometheusDestinationConfig {
    PrometheusDestinationConfig {
        url: url.to_string(),
        version: RemoteWriteVersion::V1,
        headers: Vec::new(),
        credentials: Vec::new(),
        tls: None,
        max_request_bytes: 1024 * 1024,
        buffer_bytes: 1024 * 1024,
        stall_after: Duration::from_secs(5),
    }
}

/// A sample of `name` with `job` as its one attribute, at `time`, an ISO 8601 datetime as a row carries one.
fn sample(name: &str, job: &str, value: f64, time: &str) -> OwnedRecord {
    let json = format!(r#"{{"time":"{time}","name":"{name}","value":{value:?},"attributes":{{"job":"{job}"}}}}"#);
    OwnedRecord {
        value: json.into_bytes(),
        payload: Payload::Record,
        ..OwnedRecord::default()
    }
}

type Sent = Arc<Mutex<Vec<Vec<u8>>>>;

/// Answers each request from the script, in order, and records its body.
struct ScriptedTransport {
    answers: VecDeque<u16>,
    sent: Sent,
}

impl Transport for ScriptedTransport {
    fn post(&mut self, _path: &str, body: &[u8], _budget: Duration) -> Result<Response, IoError> {
        self.sent.lock().unwrap().push(body.to_vec());
        let status = self.answers.pop_front().expect("the script ran out");
        Ok(Response {
            status,
            headers: Vec::new(),
            body: format!("status {status}").into_bytes(),
        })
    }
}

fn scripted(statuses: &[u16]) -> (HttpDestination<RemoteWrite, ScriptedTransport>, Sent) {
    let sent = Arc::new(Mutex::new(Vec::new()));
    let transport = ScriptedTransport {
        answers: statuses.iter().copied().collect(),
        sent: Arc::clone(&sent),
    };
    let d = HttpDestination::new(transport, RemoteWrite::new(&config("http://unused")))
        .with_stall_after(Duration::from_secs(5));
    (d, sent)
}

/// Hands `rows` over as one batch, answered before `deliver` returns.
fn deliver<T: Transport>(
    d: &mut HttpDestination<RemoteWrite, T>,
    rows: Vec<OwnedRecord>,
) -> Result<Vec<Rejection>, IoError> {
    let (dispatched, refused) = d.deliver(0, Cow::Owned(rows))?;
    assert_eq!(dispatched, Dispatched::Complete);
    Ok(refused)
}

fn decoded(body: &[u8]) -> wire::WriteRequest {
    let raw = snap::raw::Decoder::new().decompress_vec(body).unwrap();
    wire::WriteRequest::decode(raw.as_slice()).unwrap()
}

/// A store refuses a request without naming the sample at fault, so the halves find it, and it alone is refused.
#[test]
fn a_refused_request_is_split_to_the_sample_at_fault() {
    for status in [400, 413] {
        // [a,b,c,d] refused; [a,b] lands; [c,d] refused; [c] refused alone; [d] lands.
        let (mut d, sent) = scripted(&[status, 204, status, status, 204]);
        let rows = (0..4)
            .map(|n| sample("x", &n.to_string(), 1.0, "2026-10-11T18:00:00.000000Z"))
            .collect();
        let refused = deliver(&mut d, rows).unwrap();
        assert_eq!(refused.len(), 1, "{status}");
        assert_eq!(refused[0].doc, 2, "{status}");
        assert!(
            refused[0].reason.contains(&format!("HTTP {status}")),
            "{}",
            refused[0].reason
        );
        assert_eq!(sent.lock().unwrap().len(), 5, "{status}");
    }
}

/// The waits it asked for count as throttled time.
#[test]
fn a_store_asking_to_wait_gets_the_request_again() {
    let registry = Arc::new(smeltr::metrics::Metrics::new());
    let (d, sent) = scripted(&[503, 429, 204]);
    let mut d = d.with_metrics(smeltr::metrics::MetricsHandle::new(registry.clone(), "egress-4"), 4);
    let rows = vec![sample("x", "a", 1.0, "2026-10-11T18:00:00.000000Z")];
    assert!(deliver(&mut d, rows).unwrap().is_empty());
    let sent = sent.lock().unwrap();
    assert_eq!(sent.len(), 3);
    assert_eq!(sent[0], sent[2]);
    let throttled = registry
        .snapshot()
        .into_iter()
        .find(|s| s.key.name == "smeltr_destination_throttled_seconds_total")
        .expect("throttled time counted");
    assert!(throttled.key.labels.contains(&("destination", "4".to_string())));
    // Two waits, the first two backoffs.
    assert!(
        matches!(throttled.value, smeltr::metrics::MetricValue::FloatCounter(s) if s >= 0.6),
        "{throttled:?}"
    );
}

/// A batch spanning several requests goes oldest first across them, so no request carries a sample older than one
/// already sent, and a refusal still names its own row.
#[test]
fn a_batch_spanning_requests_goes_oldest_first() {
    let (d, sent) = scripted(&[204, 400, 204]);
    let mut d = d.with_max_request_bytes(1);
    let rows = vec![
        sample("x", "a", 3.0, "2026-10-11T18:00:03.000000Z"),
        sample("x", "a", 2.0, "2026-10-11T18:00:02.000000Z"),
        sample("x", "a", 1.0, "2026-10-11T18:00:01.000000Z"),
    ];
    let refused = deliver(&mut d, rows).unwrap();
    let values: Vec<f64> = (sent.lock().unwrap().iter())
        .flat_map(|body| decoded(body).timeseries)
        .flat_map(|series| series.samples)
        .map(|sample| sample.value)
        .collect();
    assert_eq!(values, [1.0, 2.0, 3.0]);
    assert_eq!(refused.len(), 1);
    assert_eq!(refused[0].doc, 1);
}

/// A row that is no sample is refused at the destination, and the rest are sent.
#[test]
fn a_row_that_is_no_sample_is_refused_and_the_rest_are_sent() {
    let (mut d, sent) = scripted(&[204]);
    let no_value = OwnedRecord {
        value: br#"{"time":"2026-10-11T18:00:00.000000Z","name":"x"}"#.to_vec(),
        payload: Payload::Record,
        ..OwnedRecord::default()
    };
    let refused = deliver(
        &mut d,
        vec![no_value, sample("x", "a", 2.0, "2026-10-11T18:00:00.000000Z")],
    )
    .unwrap();
    assert_eq!(refused.len(), 1);
    assert_eq!(refused[0].doc, 0);
    assert!(refused[0].reason.contains("value"), "{}", refused[0].reason);
    let request = decoded(&sent.lock().unwrap()[0]);
    assert_eq!(request.timeseries.len(), 1);
    assert_eq!(request.timeseries[0].samples[0].value, 2.0);
}

fn row(json: &str) -> OwnedRecord {
    OwnedRecord {
        value: json.as_bytes().to_vec(),
        payload: Payload::Record,
        ..OwnedRecord::default()
    }
}

fn span(offset: i32, length: u32) -> wire::BucketSpan {
    wire::BucketSpan { offset, length }
}

/// Exponential histograms, from a Prometheus receiver's record or an OTLP point's, go as native histograms, and
/// histograms of explicit buckets as classic `_bucket`, `_sum` and `_count` series.
#[test]
fn a_histogram_row_is_sent_as_a_native_or_a_classic_histogram() {
    let (mut d, sent) = scripted(&[204]);
    let t = r#""time":"2026-10-11T18:00:00Z""#;
    let rows = vec![
        // Integers, with a gap of three empty buckets and a negative side.
        row(&format!(
            r#"{{{t},"name":"a","type":"exponentialHistogram","aggregationTemporality":2,"count":10,"sum":20.5,
            "scale":0,"zeroCount":1,"zeroThreshold":0.001,"positive":{{"offset":0,"bucketCounts":[2,0,0,0,3,1]}},
            "negative":{{"offset":-2,"bucketCounts":[0,3]}}}}"#
        )),
        // Floats, a gauge histogram, downscaled from scale 10, as an OTLP point with fields beyond the record.
        row(&format!(
            r#"{{{t},"name":"b","description":"d","unit":"s","startTime":"2026-10-11T17:00:00Z","flags":0,"min":1,
            "max":2,"exemplars":[],"type":"exponentialHistogram","count":2.5,"sum":3,"scale":10,
            "positive":{{"offset":4,"bucketCounts":[0.5,2]}}}}"#
        )),
        row(&format!(
            r#"{{{t},"name":"c","type":"histogram","aggregationTemporality":2,"count":3,"sum":4.5,
            "explicitBounds":[0.1,1,10],"bucketCounts":[0,2,0,1]}}"#
        )),
        row(r#"{"time":"2026-10-11T18:00:01Z","name":"c_count","value":7}"#),
        // An OTLP point of explicit buckets, a gauge histogram.
        row(&format!(
            r#"{{{t},"name":"d","description":"d","unit":"s","startTime":"2026-10-11T17:00:00Z","flags":0,"min":1,
            "max":2,"exemplars":[],"type":"histogram","aggregationTemporality":null,"count":2,"sum":3,
            "explicitBounds":[1.5],"bucketCounts":[1,1],"attributes":{{"job":"x"}}}}"#
        )),
    ];
    assert!(deliver(&mut d, rows).unwrap().is_empty());
    let request = decoded(&sent.lock().unwrap()[0]);
    assert_eq!(request.timeseries.len(), 2 + 6 + 4);
    let a = &request.timeseries[0].histograms[0];
    assert_eq!(
        (a.count.clone(), a.zero_count.clone(), a.schema, a.reset_hint),
        (
            Some(wire::Count::Int(10)),
            Some(wire::ZeroCount::Int(1)),
            0,
            wire::ResetHint::Unknown as i32
        )
    );
    assert_eq!((a.sum, a.zero_threshold, a.timestamp), (20.5, 0.001, 1_791_741_600_000));
    assert_eq!(a.positive_spans, [span(1, 1), span(3, 2)]);
    assert_eq!(a.positive_deltas, [2, 1, -2]);
    assert_eq!(
        (a.negative_spans.clone(), a.negative_deltas.clone()),
        (vec![span(0, 1)], vec![3])
    );
    let b = &request.timeseries[1].histograms[0];
    assert_eq!(
        (b.count.clone(), b.schema, b.reset_hint),
        (Some(wire::Count::Float(2.5)), 8, wire::ResetHint::Gauge as i32)
    );
    // OTLP's buckets 4 and 5 at scale 10 are both 1 at scale 8, Prometheus's 2.
    assert_eq!(
        (b.positive_spans.clone(), b.positive_counts.clone()),
        (vec![span(2, 1)], vec![2.5])
    );
    let t0 = 1_791_741_600_000;
    let classic: Vec<_> = (request.timeseries[2..].iter())
        .map(|s| {
            assert!(s.histograms.is_empty());
            (
                s.labels.iter().map(|l| (l.name.as_str(), l.value.as_str())).collect(),
                s.samples.iter().map(|s| (s.value, s.timestamp)).collect(),
            )
        })
        .collect();
    assert_eq!(
        classic,
        [
            (vec![("__name__", "c_bucket"), ("le", "0.1")], vec![(0.0, t0)]),
            (vec![("__name__", "c_bucket"), ("le", "1")], vec![(2.0, t0)]),
            (vec![("__name__", "c_bucket"), ("le", "10")], vec![(2.0, t0)]),
            (vec![("__name__", "c_bucket"), ("le", "+Inf")], vec![(3.0, t0)]),
            (vec![("__name__", "c_sum")], vec![(4.5, t0)]),
            // The histogram's count and the later sample of its series are one series, oldest first.
            (vec![("__name__", "c_count")], vec![(3.0, t0), (7.0, t0 + 1000)]),
            (
                vec![("__name__", "d_bucket"), ("job", "x"), ("le", "1.5")],
                vec![(1.0, t0)]
            ),
            (
                vec![("__name__", "d_bucket"), ("job", "x"), ("le", "+Inf")],
                vec![(2.0, t0)]
            ),
            (vec![("__name__", "d_sum"), ("job", "x")], vec![(3.0, t0)]),
            (vec![("__name__", "d_count"), ("job", "x")], vec![(2.0, t0)]),
        ]
    );
}

/// A series' points go oldest first whatever their kind: each run of samples or of histograms is its own entry, as
/// Prometheus appends an entry's samples before its histograms.
#[test]
fn a_series_mixing_samples_and_histograms_goes_oldest_first() {
    let (mut d, sent) = scripted(&[204]);
    let h = |time: &str| {
        row(&format!(
            r#"{{"time":"{time}","name":"x","type":"exponentialHistogram","aggregationTemporality":2,"count":1,
            "sum":1,"scale":0,"zeroCount":1,"resetHint":"no"}}"#
        ))
    };
    let s = |time: &str| row(&format!(r#"{{"time":"{time}","name":"x","value":1}}"#));
    let rows = vec![
        s("2026-10-11T18:00:02Z"),
        h("2026-10-11T18:00:00Z"),
        h("2026-10-11T18:00:03Z"),
        s("2026-10-11T18:00:01Z"),
    ];
    assert!(deliver(&mut d, rows).unwrap().is_empty());
    let request = decoded(&sent.lock().unwrap()[0]);
    let t0 = 1_791_741_600_000;
    let entries: Vec<(Vec<i64>, Vec<i64>)> = (request.timeseries.iter())
        .map(|s| {
            (
                s.samples.iter().map(|s| s.timestamp).collect(),
                s.histograms.iter().map(|h| h.timestamp).collect(),
            )
        })
        .collect();
    assert_eq!(
        entries,
        [
            (vec![], vec![t0]),
            (vec![t0 + 1000, t0 + 2000], vec![]),
            (vec![], vec![t0 + 3000]),
        ]
    );
    assert_eq!(
        request.timeseries[0].histograms[0].reset_hint,
        wire::ResetHint::No as i32
    );
}

/// A row Prometheus takes no histogram from is refused naming why, and the rest are sent.
#[test]
fn a_histogram_prometheus_does_not_store_is_refused() {
    let (mut d, sent) = scripted(&[204]);
    let t = r#""time":"2026-10-11T18:00:00Z""#;
    let e = |rest: &str| {
        row(&format!(
            r#"{{{t},"name":"x","type":"exponentialHistogram","count":1{rest}}}"#
        ))
    };
    let rows = vec![
        row(&format!(r#"{{{t},"name":"x","type":"summary","count":1,"sum":1}}"#)),
        e(r#","aggregationTemporality":1,"sum":1,"scale":0"#),
        e(r#","aggregationTemporality":2,"sum":1,"scale":-5"#),
        e(r#","aggregationTemporality":2,"sum":null,"scale":0"#),
        e(r#","sum":1,"scale":0,"resetHint":"yes""#),
        e(r#","aggregationTemporality":2,"sum":1,"scale":0,"resetHint":"maybe""#),
        row(&format!(
            r#"{{{t},"name":"x","type":"histogram","count":1,"sum":1,"explicitBounds":[1],"bucketCounts":[0,1],
            "attributes":{{"le":"2"}}}}"#
        )),
        e(r#","aggregationTemporality":2,"sum":1,"scale":0,"resetHint":"yes""#),
    ];
    let refused = deliver(&mut d, rows).unwrap();
    let mut reasons: Vec<(usize, &str)> = refused.iter().map(|r| (r.doc, r.reason.as_str())).collect();
    reasons.sort();
    assert_eq!(reasons.len(), 7, "{reasons:?}");
    for ((doc, reason), (at, why)) in reasons.iter().zip([
        (0, "summary"),
        (1, "delta"),
        (2, "-5"),
        (3, "NaN"),
        (4, "gauge histogram"),
        (5, "resetHint"),
        (6, "attribute le"),
    ]) {
        assert_eq!(*doc, at, "{reason}");
        assert!(reason.contains(why), "{reason}");
    }
    let request = decoded(&sent.lock().unwrap()[0]);
    assert_eq!(request.timeseries[0].histograms.len(), 1);
    assert_eq!(
        request.timeseries[0].histograms[0].reset_hint,
        wire::ResetHint::Yes as i32
    );
}

fn exemplar_labels(e: &wire::Exemplar) -> Vec<(&str, &str)> {
    e.labels.iter().map(|l| (l.name.as_str(), l.value.as_str())).collect()
}

/// A row's exemplars go as its series' exemplars: beside a sample, beside a histogram, and alone, as a Prometheus
/// receiver gives a series with exemplars and no sample.
#[test]
fn a_row_s_exemplars_are_sent_as_its_series_exemplars() {
    let (mut d, sent) = scripted(&[204]);
    let t = r#""time":"2026-10-11T18:00:00Z""#;
    let rows = vec![
        // As a Prometheus receiver gives it.
        row(&format!(
            r#"{{{t},"name":"a","value":1,"exemplars":[{{"time":"2026-10-11T17:59:59Z","value":0.5,
            "traceId":"t1","spanId":"s1","filteredAttributes":{{"user":"u"}}}}]}}"#
        )),
        // As an OTLP point gives it: an integer value, hex ids, attributes of any scalar, and no time.
        row(&format!(
            r#"{{{t},"name":"b","type":"exponentialHistogram","aggregationTemporality":2,"count":1,"sum":3,"scale":0,
            "positive":{{"offset":1,"bucketCounts":[1]}},"exemplars":[{{"value":3,
            "traceId":"5b8efff798038103d269b633813fc60c","spanId":"eee19b7ec3c1b174",
            "filteredAttributes":{{"n":2,"ok":false}}}}]}}"#
        )),
        row(&format!(
            r#"{{{t},"name":"c","value":null,"exemplars":[{{"time":"2026-10-11T18:00:01Z","value":9}}]}}"#
        )),
    ];
    assert!(deliver(&mut d, rows).unwrap().is_empty());
    let request = decoded(&sent.lock().unwrap()[0]);
    assert_eq!(request.timeseries.len(), 3);
    let a = &request.timeseries[0];
    assert_eq!(a.samples.len(), 1);
    assert_eq!(
        exemplar_labels(&a.exemplars[0]),
        [("span_id", "s1"), ("trace_id", "t1"), ("user", "u")]
    );
    assert_eq!(
        (a.exemplars[0].value, a.exemplars[0].timestamp),
        (0.5, 1_791_741_599_000)
    );
    let b = &request.timeseries[1];
    assert_eq!(b.histograms.len(), 1);
    assert_eq!(
        exemplar_labels(&b.exemplars[0]),
        [
            ("n", "2"),
            ("ok", "false"),
            ("span_id", "eee19b7ec3c1b174"),
            ("trace_id", "5b8efff798038103d269b633813fc60c")
        ]
    );
    assert_eq!(
        (b.exemplars[0].value, b.exemplars[0].timestamp),
        (3.0, 1_791_741_600_000)
    );
    let c = &request.timeseries[2];
    assert!(c.samples.is_empty() && c.histograms.is_empty());
    assert_eq!(
        (c.exemplars[0].value, c.exemplars[0].timestamp),
        (9.0, 1_791_741_601_000)
    );
}

/// An exemplar Prometheus would not store is left out, as Prometheus leaves it out, and its row is sent.
#[test]
fn an_exemplar_prometheus_would_not_store_is_left_out() {
    let (mut d, sent) = scripted(&[204]);
    let e = |exemplars: &str| {
        row(&format!(
            r#"{{"time":"2026-10-11T18:00:00Z","name":"x","value":1,"exemplars":{exemplars}}}"#
        ))
    };
    let rows = vec![
        e(r#"{"value":1}"#),
        e("[1]"),
        e(r#"[{"value":null}]"#),
        e(r#"[{"value":1,"traceId":7,"filteredAttributes":{"tags":[1]}}]"#),
    ];
    assert!(deliver(&mut d, rows).unwrap().is_empty());
    let request = decoded(&sent.lock().unwrap()[0]);
    let series = &request.timeseries[0];
    assert_eq!(series.samples.len(), 4);
    assert_eq!(series.exemplars.len(), 1);
    assert!(series.exemplars[0].labels.is_empty());
}

/// A family's metadata goes once a request: from its own record, as a Prometheus receiver gives it, or from a point's
/// description and unit, as an OTLP point carries them. A row of unknown type is refused, and the rest are sent.
#[test]
fn each_family_s_metadata_is_sent_once() {
    let (mut d, sent) = scripted(&[204]);
    let t = r#""time":"2026-10-11T18:00:00Z""#;
    let rows = vec![
        row(&format!(
            r#"{{{t},"name":"requests","value":3,"description":"from the point"}}"#
        )),
        row(
            r#"{"time":null,"value":null,"name":"requests","description":"Requests served.","unit":"requests",
            "type":"sum","isMonotonic":true,"aggregationTemporality":2}"#,
        ),
        row(&format!(
            r#"{{{t},"name":"latency","unit":"seconds","type":"histogram","count":1,"sum":1,"explicitBounds":[],
            "bucketCounts":[1]}}"#
        )),
        row(r#"{"name":"tables","type":"table"}"#),
    ];
    let refused = deliver(&mut d, rows).unwrap();
    assert_eq!(refused.len(), 1);
    assert_eq!(refused[0].doc, 3);
    assert!(refused[0].reason.contains("\"table\""), "{}", refused[0].reason);
    let request = decoded(&sent.lock().unwrap()[0]);
    // The metadata row goes first in the batch, so its metadata is the family's.
    assert_eq!(
        request.metadata,
        [
            wire::MetricMetadata {
                r#type: wire::MetricType::Counter as i32,
                metric_family_name: "requests".into(),
                help: "Requests served.".into(),
                unit: "requests".into(),
            },
            wire::MetricMetadata {
                r#type: wire::MetricType::GaugeHistogram as i32,
                metric_family_name: "latency".into(),
                help: String::new(),
                unit: "seconds".into(),
            },
        ]
    );
    assert_eq!(request.timeseries.len(), 1 + 3);
}

/// A store on a socket that answers one request `status` and hands back its head, lowercased, and body.
fn store(status: &'static str) -> (String, std::thread::JoinHandle<(String, Vec<u8>)>) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}/api/v1/write", listener.local_addr().unwrap());
    let served = std::thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        let mut reader = BufReader::new(stream);
        let mut head = String::new();
        loop {
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            if line == "\r\n" {
                break;
            }
            head.push_str(&line.to_ascii_lowercase());
        }
        let length: usize = head
            .lines()
            .find_map(|l| l.strip_prefix("content-length: "))
            .map_or(0, |v| v.trim().parse().unwrap());
        let mut body = vec![0; length];
        reader.read_exact(&mut body).unwrap();
        write!(
            reader.get_mut(),
            "HTTP/1.1 {status}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
        )
        .unwrap();
        (head, body)
    });
    (url, served)
}

/// A request is remote_write 1.0 as a store reads it: its headers, and a snappy-compressed `WriteRequest`.
#[test]
fn every_request_is_remote_write_with_its_headers() {
    let (url, served) = store("204 No Content");
    let c = PrometheusDestinationConfig {
        headers: vec![("X-Scope-OrgID".to_string(), "tenant-1".to_string())],
        ..config(&url)
    };
    let agent = agent_config(None, &HeldSecrets::default()).unwrap();
    let credentials = vec![("Authorization".to_string(), "Bearer t-123".to_string())];
    let mut d = HttpDestination::new(transport(&c, &credentials, &agent), RemoteWrite::new(&c));
    let rows = vec![sample("up", "api", 1.0, "2026-10-11T18:00:00.250000Z")];
    assert!(deliver(&mut d, rows).unwrap().is_empty());
    let (head, body) = served.join().unwrap();
    assert!(head.starts_with("post /api/v1/write "), "{head}");
    for header in [
        "content-type: application/x-protobuf",
        "content-encoding: snappy",
        "x-prometheus-remote-write-version: 0.1.0",
        "x-scope-orgid: tenant-1",
        "authorization: bearer t-123",
    ] {
        assert!(head.contains(header), "{header} in {head}");
    }
    let request = decoded(&body);
    let labels: Vec<(&str, &str)> = (request.timeseries[0].labels.iter())
        .map(|l| (l.name.as_str(), l.value.as_str()))
        .collect();
    assert_eq!(labels, [("__name__", "up"), ("job", "api")]);
    let sample = &request.timeseries[0].samples[0];
    assert_eq!((sample.value, sample.timestamp), (1.0, 1_791_741_600_250));
}

/// A remote_write 2.0 request as a store reads it, each string an index into `symbols`.
mod wire_v2 {
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Request {
        #[prost(string, repeated, tag = "4")]
        pub symbols: Vec<String>,
        #[prost(message, repeated, tag = "5")]
        pub timeseries: Vec<TimeSeries>,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct TimeSeries {
        #[prost(uint32, repeated, tag = "1")]
        pub labels_refs: Vec<u32>,
        #[prost(message, repeated, tag = "2")]
        pub samples: Vec<Sample>,
        #[prost(message, optional, tag = "5")]
        pub metadata: Option<Metadata>,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Sample {
        #[prost(double, tag = "1")]
        pub value: f64,
        #[prost(int64, tag = "2")]
        pub timestamp: i64,
        #[prost(int64, tag = "3")]
        pub start_timestamp: i64,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Metadata {
        #[prost(int32, tag = "1")]
        pub r#type: i32,
        #[prost(uint32, tag = "3")]
        pub help_ref: u32,
        #[prost(uint32, tag = "4")]
        pub unit_ref: u32,
    }
}

/// A destination set to 2.0 sends 2.0's `Content-Type` and version, its labels from the request's table, a row's
/// start time, and a family's metadata on its series.
#[test]
fn a_2_0_destination_sends_2_0_with_metadata_on_each_series() {
    let (url, served) = store("204 No Content\r\nX-Prometheus-Remote-Write-Samples-Written: 1");
    let c = PrometheusDestinationConfig {
        version: RemoteWriteVersion::V2,
        ..config(&url)
    };
    let agent = agent_config(None, &HeldSecrets::default()).unwrap();
    let mut d = HttpDestination::new(transport(&c, &[], &agent), RemoteWrite::new(&c));
    let rows = vec![
        row(r#"{"name":"requests_total","type":"sum","isMonotonic":true,"description":"Requests served."}"#),
        row(
            r#"{"time":"1970-01-01T00:00:02Z","startTime":"1970-01-01T00:00:01Z","name":"requests_total",
            "type":"sum","value":3,"attributes":{"job":"api"}}"#,
        ),
    ];
    assert!(deliver(&mut d, rows).unwrap().is_empty());
    let (head, body) = served.join().unwrap();
    for header in [
        "content-type: application/x-protobuf;proto=io.prometheus.write.v2.request",
        "content-encoding: snappy",
        "x-prometheus-remote-write-version: 2.0.0",
    ] {
        assert!(head.contains(header), "{header} in {head}");
    }
    let raw = snap::raw::Decoder::new().decompress_vec(&body).unwrap();
    let request = wire_v2::Request::decode(raw.as_slice()).unwrap();
    let s = |at: u32| request.symbols[at as usize].as_str();
    assert_eq!(s(0), "");
    let [series] = &request.timeseries[..] else {
        panic!("one series: {request:?}")
    };
    let labels: Vec<_> = series.labels_refs.chunks(2).map(|p| (s(p[0]), s(p[1]))).collect();
    assert_eq!(labels, [("__name__", "requests_total"), ("job", "api")]);
    let sample = &series.samples[0];
    assert_eq!(
        (sample.value, sample.timestamp, sample.start_timestamp),
        (3.0, 2_000, 1_000)
    );
    let metadata = series.metadata.as_ref().expect("its family's metadata");
    assert_eq!(
        (metadata.r#type, s(metadata.help_ref), s(metadata.unit_ref)),
        (1, "Requests served.", "")
    );
}

struct Container(String);

impl Drop for Container {
    fn drop(&mut self) {
        let _ = std::process::Command::new("docker")
            .args(["rm", "-f", &self.0])
            .output();
    }
}

/// Prometheus, taking remote_write, stores what this destination sends, and a query reads it back.
#[test]
fn prometheus_stores_what_the_destination_sends() {
    let Some(image) = live::prometheus("prometheus_stores_what_the_destination_sends") else {
        return;
    };
    let web = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();
    let name = format!("smeltr-prometheus-store-{}", std::process::id());
    let started = std::process::Command::new("docker")
        .args(["run", "-d", "--rm", "--network", "host", "--name", &name])
        .arg(image.as_str())
        .arg("--config.file=/etc/prometheus/prometheus.yml")
        .arg(format!("--web.listen-address={web}"))
        .arg("--web.enable-remote-write-receiver")
        .arg("--storage.tsdb.path=/tmp/prometheus")
        .output()
        .expect("docker runs");
    assert!(started.status.success(), "{}", String::from_utf8_lossy(&started.stderr));
    let _prometheus = Container(name);
    let base = format!("http://{web}");
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    while ureq::get(&format!("{base}/-/ready")).call().is_err() {
        assert!(std::time::Instant::now() < deadline, "Prometheus never became ready");
        std::thread::sleep(Duration::from_millis(200));
    }

    let c = config(&format!("{base}/api/v1/write"));
    let agent = agent_config(None, &HeldSecrets::default()).unwrap();
    let mut d = HttpDestination::new(transport(&c, &[], &agent), RemoteWrite::new(&c));
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap();
    let at = |back: u64| {
        let micros = (now - Duration::from_secs(back)).as_micros() as i64;
        let mut out = Vec::new();
        smeltr::ops::datetime::write_iso8601_micros_bare(micros, &mut out);
        String::from_utf8(out).unwrap()
    };
    // Out of order in the batch: the destination sends a series' samples oldest first, as Prometheus takes them.
    let rows = vec![
        sample("smeltr_test_total", "smeltr", 7.0, &at(1)),
        sample("smeltr_test_total", "smeltr", 5.0, &at(10)),
    ];
    assert!(deliver(&mut d, rows).unwrap().is_empty());

    let query = format!("{base}/api/v1/query?query=smeltr_test_total%7Bjob%3D%22smeltr%22%7D");
    let answer: serde_json::Value = ureq::get(&query).call().unwrap().body_mut().read_json().unwrap();
    let result = &answer["data"]["result"][0];
    assert_eq!(result["metric"]["__name__"], "smeltr_test_total", "{answer}");
    assert_eq!(result["value"][1], "7", "{answer}");
}
