//! Adversarial remote_write against the Prometheus receiver. Its port takes whatever any host that reaches it sends,
//! so a panic in its decoder drops the connection of every sender in flight, and an answer outside the ones it
//! documents makes Prometheus drop samples it should send again, or send again what it should drop.
//!
//! Two layers: the decoder alone, fed valid remote_write 1.0 and 2.0 requests corrupted at every level, and the
//! receiver over a socket, opened under random settings and sent random requests. A deterministic PRNG over a seed
//! range, from `harness::fuzz`: a failure names its seed, and each fuzzer checks that its seeds reached every
//! option it knows.

use std::io::{Read, Write};
use std::net::{Ipv4Addr, SocketAddr, TcpStream};
use std::sync::Arc;
use std::time::{Duration, Instant};

use prost::Message;
use smeltr::io::Source;
use smeltr::io::prometheus::{self, Samples, Version, Written};
use smeltr::io::receiver::INBOX_BYTES;
use smeltr::plan::topology::Receiver;

use crate::harness::docker::{Container, Hook, share};
use crate::harness::fuzz::{self, Coverage, Rng};
use crate::harness::receiver::{self, Answer, Pki, Presents, Request, Tls, commit};

/// The messages of remote_write 1.0 and 2.0, with prometheus/prompb's field numbers. A histogram's `oneof`s are the
/// optional fields they are on the wire, so a request may set both halves of one.
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
        #[prost(int32, tag = "1")]
        pub r#type: i32,
        #[prost(string, tag = "2")]
        pub metric_family_name: String,
        #[prost(string, tag = "4")]
        pub help: String,
        #[prost(string, tag = "5")]
        pub unit: String,
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
    /// A sample of either version; 1.0 sends no start time.
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Sample {
        #[prost(double, tag = "1")]
        pub value: f64,
        #[prost(int64, tag = "2")]
        pub timestamp: i64,
        #[prost(int64, tag = "3")]
        pub start_timestamp: i64,
    }
    /// A native histogram of either version; 1.0 sends no start time.
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Histogram {
        #[prost(uint64, optional, tag = "1")]
        pub count_int: Option<u64>,
        #[prost(double, optional, tag = "2")]
        pub count_float: Option<f64>,
        #[prost(double, tag = "3")]
        pub sum: f64,
        #[prost(sint32, tag = "4")]
        pub schema: i32,
        #[prost(double, tag = "5")]
        pub zero_threshold: f64,
        #[prost(uint64, optional, tag = "6")]
        pub zero_count_int: Option<u64>,
        #[prost(double, optional, tag = "7")]
        pub zero_count_float: Option<f64>,
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
        #[prost(int32, tag = "14")]
        pub reset_hint: i32,
        #[prost(int64, tag = "15")]
        pub timestamp: i64,
        #[prost(double, repeated, tag = "16")]
        pub custom_values: Vec<f64>,
        #[prost(int64, tag = "17")]
        pub start_timestamp: i64,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct BucketSpan {
        #[prost(sint32, tag = "1")]
        pub offset: i32,
        #[prost(uint32, tag = "2")]
        pub length: u32,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Request {
        #[prost(string, repeated, tag = "4")]
        pub symbols: Vec<String>,
        #[prost(message, repeated, tag = "5")]
        pub timeseries: Vec<TimeSeriesV2>,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct TimeSeriesV2 {
        #[prost(uint32, repeated, tag = "1")]
        pub labels_refs: Vec<u32>,
        #[prost(message, repeated, tag = "2")]
        pub samples: Vec<Sample>,
        #[prost(message, repeated, tag = "3")]
        pub histograms: Vec<Histogram>,
        #[prost(message, repeated, tag = "4")]
        pub exemplars: Vec<ExemplarV2>,
        #[prost(message, optional, tag = "5")]
        pub metadata: Option<MetadataV2>,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct ExemplarV2 {
        #[prost(uint32, repeated, tag = "1")]
        pub labels_refs: Vec<u32>,
        #[prost(double, tag = "2")]
        pub value: f64,
        #[prost(int64, tag = "3")]
        pub timestamp: i64,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct MetadataV2 {
        #[prost(int32, tag = "1")]
        pub r#type: i32,
        #[prost(uint32, tag = "3")]
        pub help_ref: u32,
        #[prost(uint32, tag = "4")]
        pub unit_ref: u32,
    }
}

use wire::{BucketSpan, Histogram};

/// The float Prometheus writes for a series that went stale: a NaN of its own bits, which must not read as any other.
const STALE: u64 = 0x7ff0_0000_0000_0002;

/// The times of the requests built here, in milliseconds since the epoch.
const T: i64 = 1_700_000_000_000;

fn span(offset: i32, length: u32) -> BucketSpan {
    BucketSpan { offset, length }
}

fn label(name: &str, value: &str) -> wire::Label {
    wire::Label {
        name: name.into(),
        value: value.into(),
    }
}

fn labels(pairs: &[(&str, &str)]) -> Vec<wire::Label> {
    pairs.iter().map(|(n, v)| label(n, v)).collect()
}

fn floats(values: &[f64], start: i64) -> Vec<wire::Sample> {
    (values.iter().zip(0..))
        .map(|(&value, i)| wire::Sample {
            value,
            timestamp: T + i * 15_000,
            start_timestamp: start,
        })
        .collect()
}

/// Every float a sample must carry through: NaN, the staleness marker, the infinities, a negative zero, a subnormal.
fn edge_floats() -> Vec<f64> {
    vec![
        1.5,
        f64::NAN,
        f64::from_bits(STALE),
        f64::INFINITY,
        f64::NEG_INFINITY,
        -0.0,
        f64::MIN_POSITIVE / 2.0,
    ]
}

/// One histogram of each kind remote_write sends, each adding up, with its start time where `start` is not 0: an
/// integer and a float exponential one with buckets either side of zero and a zero bucket, one of custom buckets, a
/// gauge histogram, and one whose sum is NaN, counting an observation no bucket holds.
fn histograms(start: i64) -> Vec<Histogram> {
    let at = |i: i64| T + i * 15_000;
    vec![
        // Zero bucket 2, positive 1+2+2, negative 1+1+2.
        Histogram {
            count_int: Some(11),
            sum: 33.5,
            schema: 3,
            zero_threshold: 1e-128,
            zero_count_int: Some(2),
            positive_spans: vec![span(-2, 2), span(1, 1)],
            positive_deltas: vec![1, 1, 0],
            negative_spans: vec![span(0, 1), span(2, 2)],
            negative_deltas: vec![1, 0, 1],
            reset_hint: 1,
            timestamp: at(0),
            start_timestamp: start,
            ..Default::default()
        },
        Histogram {
            count_float: Some(7.5),
            sum: -2.25,
            schema: -4,
            zero_threshold: 0.001,
            zero_count_float: Some(0.5),
            positive_spans: vec![span(1, 2)],
            positive_counts: vec![3.0, 1.5],
            negative_spans: vec![span(-1, 1)],
            negative_counts: vec![2.5],
            reset_hint: 2,
            timestamp: at(1),
            start_timestamp: start,
            ..Default::default()
        },
        // Buckets 0, 1 and 3 of four, ending at 0.1, 0.5, 1 and +Inf.
        Histogram {
            count_int: Some(6),
            sum: 2.0,
            schema: -53,
            positive_spans: vec![span(0, 2), span(1, 1)],
            positive_deltas: vec![1, 2, -1],
            custom_values: vec![0.1, 0.5, 1.0],
            timestamp: at(2),
            start_timestamp: start,
            ..Default::default()
        },
        Histogram {
            count_float: Some(4.0),
            sum: 8.0,
            schema: 0,
            positive_spans: vec![span(0, 1)],
            positive_counts: vec![4.0],
            reset_hint: 3,
            timestamp: at(3),
            start_timestamp: start,
            ..Default::default()
        },
        Histogram {
            count_int: Some(3),
            sum: f64::NAN,
            schema: 8,
            positive_spans: vec![span(5, 1)],
            positive_deltas: vec![2],
            timestamp: at(4),
            start_timestamp: start,
            ..Default::default()
        },
    ]
}

/// A remote_write 1.0 request holding every kind of record it reads: float samples of every edge, histograms of every
/// kind, exemplars on samples and alone, a series of no name, and metadata of every type, an unknown one and a family
/// of no name among them.
fn v1_request() -> wire::WriteRequest {
    let exemplar = |pairs: &[(&str, &str)], value, timestamp| wire::Exemplar {
        labels: labels(pairs),
        value,
        timestamp,
    };
    let series = |pairs: &[(&str, &str)], samples| wire::TimeSeries {
        labels: labels(pairs),
        samples,
        ..Default::default()
    };
    let mut up = series(
        &[("__name__", "up"), ("job", "node"), ("instance", "a:9100")],
        floats(&edge_floats(), 0),
    );
    up.exemplars = vec![
        exemplar(&[("trace_id", "abc"), ("span_id", "def"), ("user", "u1")], 0.5, T),
        // A label of no value is left out, and an exemplar naming a label twice is dropped.
        exemplar(&[("trace_id", "abc"), ("pod", "")], f64::NAN, 0),
        exemplar(&[("pod", "a"), ("pod", "b")], 1.0, T),
    ];
    let mut latency = series(&[("__name__", "latency_seconds"), ("route", "/")], Vec::new());
    latency.histograms = histograms(0);
    latency.exemplars = vec![exemplar(&[("trace_id", "0af7651916cd43dd8448eb211c80319c")], 0.25, T)];
    let mut alone = series(&[("__name__", "requests_total"), ("path", "/")], Vec::new());
    alone.exemplars = vec![exemplar(&[("span_id", "b7ad6b7169203331")], 3.0, T + 1)];
    let metadata = |r#type, name: &str| wire::MetricMetadata {
        r#type,
        metric_family_name: name.into(),
        help: format!("{name} help"),
        unit: if r#type == 3 { "seconds".into() } else { String::new() },
    };
    wire::WriteRequest {
        timeseries: vec![
            up,
            series(&[("__name__", "temp_celsius")], floats(&[21.5], 0)),
            latency,
            alone,
            series(&[("job", "nameless")], floats(&[2.0], 0)),
        ],
        metadata: vec![
            metadata(0, "untyped"),
            metadata(1, "requests_total"),
            metadata(2, "temp_celsius"),
            metadata(3, "latency_seconds"),
            metadata(4, "queue_seconds"),
            metadata(5, "rpc_seconds"),
            metadata(6, "build_info"),
            metadata(7, "feature_state"),
            metadata(42, "from_the_future"),
            metadata(1, ""),
        ],
    }
}

/// The table of strings [`v2_request`] names.
const SYMBOLS: &[&str] = &[
    "",                    // 0
    "__name__",            // 1
    "http_requests_total", // 2
    "code",                // 3
    "200",                 // 4
    "500",                 // 5
    "Requests served.",    // 6
    "trace_id",            // 7
    "abc",                 // 8
    "span_id",             // 9
    "def",                 // 10
    "pod",                 // 11
    "p-1",                 // 12
    "latency_seconds",     // 13
    "seconds",             // 14
    "rpc_bucket",          // 15
    "le",                  // 16
    "0.5",                 // 17
    "rpc_count",           // 18
    "queue_depth",         // 19
    "queue_seconds",       // 20
    "build_info",          // 21
    "feature_state",       // 22
    "requests_exemplars",  // 23
];

/// A remote_write 2.0 request holding every kind of record it reads, each series with its family's metadata: samples
/// with and without start times, histograms of every kind, exemplars on samples and alone, a family two series share,
/// a classic histogram's and a summary's series, and metadata of every type.
fn v2_request() -> wire::Request {
    use wire::{ExemplarV2, MetadataV2, TimeSeriesV2};
    let metadata = |r#type, help_ref, unit_ref| {
        Some(MetadataV2 {
            r#type,
            help_ref,
            unit_ref,
        })
    };
    let series = |labels_refs: &[u32], samples, r#type, help, unit| TimeSeriesV2 {
        labels_refs: labels_refs.to_vec(),
        samples,
        metadata: metadata(r#type, help, unit),
        ..Default::default()
    };
    let exemplar = |labels_refs: &[u32], value, timestamp| ExemplarV2 {
        labels_refs: labels_refs.to_vec(),
        value,
        timestamp,
    };
    let mut ok = series(&[1, 2, 3, 4], floats(&edge_floats(), T - 60_000), 1, 6, 0);
    ok.exemplars = vec![exemplar(&[7, 8, 9, 10, 11, 12], 1.0, T), exemplar(&[11, 0], 2.0, 0)];
    let mut latency = series(&[1, 13], Vec::new(), 3, 0, 14);
    latency.histograms = histograms(T - 3_600_000);
    latency.exemplars = vec![exemplar(&[7, 8], 0.2, T)];
    let mut gauge = series(&[1, 20, 11, 12], Vec::new(), 4, 0, 14);
    gauge.histograms = histograms(0)[3..4].to_vec();
    let mut alone = series(&[1, 23], Vec::new(), 0, 0, 0);
    alone.exemplars = vec![exemplar(&[9, 10], 4.0, T)];
    wire::Request {
        symbols: SYMBOLS.iter().map(|s| s.to_string()).collect(),
        timeseries: vec![
            ok,
            series(&[1, 2, 3, 5], floats(&[1.0], 0), 1, 6, 0),
            latency,
            series(&[1, 15, 16, 17], floats(&[3.0], T - 1), 3, 0, 14),
            series(&[1, 18], floats(&[9.0], 0), 5, 0, 14),
            series(&[1, 19], floats(&[7.0], 0), 2, 0, 0),
            gauge,
            series(&[1, 21], floats(&[1.0], 0), 6, 0, 0),
            series(&[1, 22], floats(&[1.0], 0), 7, 0, 0),
            series(&[1, 2, 3, 4, 11, 12], floats(&[5.0], 0), 42, 0, 0),
            alone,
        ],
    }
}

fn snappy(proto: &[u8]) -> Vec<u8> {
    snap::raw::Encoder::new().compress_vec(proto).unwrap()
}

/// A valid request of either version, holding a random share of [`v1_request`]'s or [`v2_request`]'s records, and
/// which version it is.
fn valid(rng: &mut Rng) -> (Vec<u8>, Version) {
    if rng.chance(2) {
        let mut request = v1_request();
        request.timeseries.retain(|_| rng.below(3) != 0);
        request.metadata.retain(|_| rng.below(3) != 0);
        (request.encode_to_vec(), Version::V1)
    } else {
        let mut request = v2_request();
        request.timeseries.retain(|_| rng.below(3) != 0);
        (request.encode_to_vec(), Version::V2)
    }
}

/// A value most likely to break whoever reads it as a count, a time or an index.
fn edge_int(rng: &mut Rng) -> i64 {
    [
        0,
        1,
        -1,
        i64::MAX,
        i64::MIN,
        i32::MAX as i64,
        i32::MIN as i64,
        u32::MAX as i64,
        rng.next_u64() as i64,
    ][rng.below(9)]
}

fn edge_float(rng: &mut Rng) -> f64 {
    [
        0.0,
        -1.0,
        f64::NAN,
        f64::INFINITY,
        f64::NEG_INFINITY,
        1e308,
        -0.0,
        f64::from_bits(rng.next_u64()),
    ][rng.below(8)]
}

const HISTOGRAM_SPOILS: &[&str] = &[
    "histogram schema",
    "histogram integer count",
    "histogram float count",
    "histogram zero count",
    "histogram zero threshold",
    "histogram positive span",
    "histogram negative span",
    "histogram positive delta",
    "histogram negative count",
    "histogram positive count",
    "histogram custom bound",
    "histogram reset hint",
    "histogram time",
    "histogram spans far apart",
];

/// `h` made wrong in one way a sender could: a schema past any, counts that do not add up, spans that overlap, go back
/// or reach past any bucket, counts below zero or NaN, bounds that do not increase, a zero bucket on custom buckets, or
/// both halves of a `oneof`.
fn spoil_histogram(rng: &mut Rng, h: &mut Histogram, cov: &mut Coverage) {
    let i = |rng: &mut Rng, n: usize| rng.below(n.max(1));
    let spoil = rng.below(HISTOGRAM_SPOILS.len());
    cov.reach(HISTOGRAM_SPOILS[spoil]);
    match spoil {
        0 => h.schema = edge_int(rng) as i32,
        1 => h.count_int = Some(edge_int(rng) as u64),
        2 => h.count_float = Some(edge_float(rng)),
        3 => h.zero_count_int = Some(edge_int(rng) as u64),
        4 => h.zero_threshold = edge_float(rng),
        5 => {
            let at = i(rng, h.positive_spans.len());
            let spoiled = span(edge_int(rng) as i32, edge_int(rng) as u32);
            match h.positive_spans.get_mut(at) {
                Some(s) => *s = spoiled,
                None => h.positive_spans.push(spoiled),
            }
        }
        6 => h
            .negative_spans
            .push(span(edge_int(rng) as i32, 1 + rng.below(3) as u32)),
        7 => h.positive_deltas.push(edge_int(rng)),
        8 => h.negative_counts.push(edge_float(rng)),
        9 => h.positive_counts.push(edge_float(rng)),
        10 => h
            .custom_values
            .insert(i(rng, h.custom_values.len() + 1), edge_float(rng)),
        11 => h.reset_hint = edge_int(rng) as i32,
        12 => h.timestamp = edge_int(rng),
        _ => (h.positive_spans, h.positive_deltas) = (vec![span(0, 1), span(i32::MAX, 1)], vec![1, 0]),
    }
}

const V1_SPOILS: &[&str] = &[
    "1.0 label of no name",
    "1.0 label of no value",
    "1.0 label twice",
    "1.0 series of no labels",
    "1.0 edge sample",
    "1.0 exemplar of no label name",
    "1.0 histogram",
];

/// `request` made wrong in one way a sender could, each a message that still encodes.
fn spoil_v1(rng: &mut Rng, request: &mut wire::WriteRequest, cov: &mut Coverage) {
    if rng.chance(5)
        && let Some(m) = request.metadata.first_mut()
    {
        cov.reach("1.0 metadata");
        (m.r#type, m.metric_family_name) = (edge_int(rng) as i32, "\u{0}\u{ffff}".repeat(rng.below(3)));
        return;
    }
    let n = request.timeseries.len();
    let Some(series) = request.timeseries.get_mut(rng.below(n.max(1))) else {
        return;
    };
    let spoil = rng.below(V1_SPOILS.len());
    cov.reach(V1_SPOILS[spoil]);
    match spoil {
        0 => series.labels.push(label("", "x")),
        1 => series.labels.push(label("job", "")),
        2 => series
            .labels
            .push(series.labels.first().cloned().unwrap_or_else(|| label("a", "b"))),
        3 => series.labels.clear(),
        4 => series.samples.push(wire::Sample {
            value: edge_float(rng),
            timestamp: edge_int(rng),
            start_timestamp: edge_int(rng),
        }),
        5 => series.exemplars.push(wire::Exemplar {
            labels: vec![label("", "v"), label("trace_id", "\u{fffd}")],
            value: edge_float(rng),
            timestamp: edge_int(rng),
        }),
        _ => {
            if series.histograms.is_empty() {
                series.histograms = histograms(0);
            }
            let at = rng.below(series.histograms.len());
            spoil_histogram(rng, &mut series.histograms[at], cov);
        }
    }
}

const TABLE_SPOILS: &[&str] = &["2.0 first string not empty", "2.0 no table", "2.0 string of NULs"];

const V2_SPOILS: &[&str] = &[
    "2.0 odd label references",
    "2.0 label past the table",
    "2.0 metadata past the table",
    "2.0 exemplar past the table",
    "2.0 samples and histograms",
    "2.0 label twice",
    "2.0 histogram",
];

/// `request` made wrong in one way a 2.0 sender could: a reference past the table or an odd count of them, a table
/// whose first string is not empty, a series of samples and histograms both, or a histogram that is not one.
fn spoil_v2(rng: &mut Rng, request: &mut wire::Request, cov: &mut Coverage) {
    if rng.chance(6) {
        let spoil = rng.below(TABLE_SPOILS.len());
        cov.reach(TABLE_SPOILS[spoil]);
        match spoil {
            0 => {
                if let Some(first) = request.symbols.first_mut() {
                    *first = "x".into();
                }
            }
            1 => request.symbols.clear(),
            _ => request.symbols.push("\u{0}".repeat(rng.below(40))),
        }
        return;
    }
    let n = request.timeseries.len();
    let Some(series) = request.timeseries.get_mut(rng.below(n.max(1))) else {
        return;
    };
    let past = SYMBOLS.len() as u32 + rng.below(3) as u32;
    let spoil = rng.below(V2_SPOILS.len());
    cov.reach(V2_SPOILS[spoil]);
    match spoil {
        0 => series.labels_refs.push(past),
        1 => series.labels_refs.extend([past, edge_int(rng) as u32]),
        2 => {
            if let Some(m) = &mut series.metadata {
                (m.help_ref, m.unit_ref, m.r#type) = (past, edge_int(rng) as u32, edge_int(rng) as i32);
            }
        }
        3 => series.exemplars.push(wire::ExemplarV2 {
            labels_refs: vec![past, 1, 2],
            value: edge_float(rng),
            timestamp: edge_int(rng),
        }),
        4 => (series.samples, series.histograms) = (floats(&[1.0], 0), histograms(0)),
        5 => series.labels_refs.extend([1, 2]),
        _ => {
            if series.histograms.is_empty() {
                (series.samples, series.histograms) = (Vec::new(), histograms(T));
            }
            let at = rng.below(series.histograms.len());
            spoil_histogram(rng, &mut series.histograms[at], cov);
        }
    }
}

/// The bytes of a varint no field can hold: ten bytes, the most a varint takes, of all ones.
const HUGE_VARINT: &[u8] = &[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01];

const PROTO_CORRUPTIONS: &[&str] = &[
    "protobuf bytes overwritten",
    "protobuf cut",
    "protobuf huge varint",
    "protobuf huge length",
    "protobuf unsent wire type",
    "protobuf run repeated",
    "protobuf sent twice",
];

/// `proto` corrupted at the byte level: bytes overwritten, a cut, a varint or a length prefix too large for anything,
/// a field of a wire type remote_write never sends, a run of it repeated, or the whole message again, which protobuf
/// merges as every field sent twice.
fn corrupt_proto(rng: &mut Rng, proto: &mut Vec<u8>, cov: &mut Coverage) {
    for _ in 0..1 + rng.below(3) {
        let at = rng.below(proto.len() + 1);
        let corruption = rng.below(PROTO_CORRUPTIONS.len());
        cov.reach(PROTO_CORRUPTIONS[corruption]);
        match corruption {
            0 => {
                for _ in 0..1 + rng.below(4) {
                    if !proto.is_empty() {
                        let at = rng.below(proto.len());
                        proto[at] = rng.next_u64() as u8;
                    }
                }
            }
            1 => proto.truncate(at),
            2 => {
                proto.splice(at..at, HUGE_VARINT.iter().copied());
            }
            3 => {
                // A length-delimited field of a number the reader looks at, claiming more than any body holds.
                let key = ((1 + rng.below(17)) << 3 | 2) as u8;
                let len: &[u8] = [HUGE_VARINT, &[0xff, 0xff, 0xff, 0xff, 0x0f], &[0x80, 0x01]][rng.below(3)];
                proto.splice(at..at, std::iter::once(key).chain(len.iter().copied()));
            }
            4 => {
                let key = ((1 + rng.below(17)) << 3 | (3 + rng.below(5))) as u8;
                proto.splice(at..at, [key, rng.next_u64() as u8]);
            }
            5 => {
                let from = rng.below(proto.len() + 1);
                let run = proto[from..(from + rng.below(64)).min(proto.len())].to_vec();
                proto.splice(at..at, run);
            }
            _ => proto.extend_from_slice(&proto.clone()),
        }
    }
}

const SNAPPY_CORRUPTIONS: &[&str] = &["snappy bytes overwritten", "snappy cut", "snappy header lies"];

/// `body`, snappy, corrupted after compression: bytes overwritten, a cut, or a header claiming a length other than
/// what it holds, up to more than any receiver holds.
fn corrupt_snappy(rng: &mut Rng, body: &mut Vec<u8>, cov: &mut Coverage) {
    let corruption = rng.below(SNAPPY_CORRUPTIONS.len());
    cov.reach(SNAPPY_CORRUPTIONS[corruption]);
    match corruption {
        0 => {
            for _ in 0..1 + rng.below(3) {
                if !body.is_empty() {
                    let at = rng.below(body.len());
                    body[at] = rng.next_u64() as u8;
                }
            }
        }
        1 => body.truncate(rng.below(body.len() + 1)),
        _ => {
            let header = body.iter().position(|b| b & 0x80 == 0).map_or(0, |end| end + 1);
            let claimed: &[u8] = [
                &[0x00][..],
                &[0x7f],
                &[0xff, 0xff, 0xff, 0xff, 0x0f],
                &[0x80, 0x80, 0x80, 0x40],
            ][rng.below(4)];
            body.splice(..header.min(body.len()), claimed.iter().copied());
        }
    }
}

/// A corrupted request: a valid one spoiled as a message, corrupted as bytes, or both, then compressed and maybe
/// corrupted again. Which version it was built as.
fn corrupted(rng: &mut Rng, cov: &mut Coverage) -> (Vec<u8>, Version) {
    let (mut proto, version) = match rng.below(2) {
        0 => {
            let mut request = v1_request();
            for _ in 0..1 + rng.below(3) {
                spoil_v1(rng, &mut request, cov);
            }
            (request.encode_to_vec(), Version::V1)
        }
        _ => {
            let mut request = v2_request();
            for _ in 0..1 + rng.below(3) {
                spoil_v2(rng, &mut request, cov);
            }
            (request.encode_to_vec(), Version::V2)
        }
    };
    if rng.chance(2) {
        corrupt_proto(rng, &mut proto, cov);
    }
    let mut body = snappy(&proto);
    if rng.chance(4) {
        corrupt_snappy(rng, &mut body, cov);
    }
    (body, version)
}

/// What a receiver holding at most `limit` makes of `body` read as `version`, in the order it reads it: refused for a
/// header claiming more than it holds, refused unread past `limit`, checked whole, then every record taken. The
/// records and what the request wrote, or `Err` for a refusal and whether it is for size (413) or not (400).
fn decode(body: &[u8], version: Version, limit: usize) -> Result<(usize, Written), bool> {
    let len = prometheus::decompressed_len(body).map_err(|_| false)?;
    if body.len().saturating_add(len) > limit {
        return Err(true);
    }
    let mut samples = Samples::snappy(body, version, limit).map_err(|_| false)?;
    if samples.size() > limit {
        return Err(true);
    }
    let written = samples.check().map_err(|_| false)?;
    Ok((std::iter::from_fn(|| samples.next_record()).count(), written))
}

/// A remote_write request is whatever reaches the receiver's port. Valid 1.0 and 2.0 requests of every record kind,
/// spoiled as messages, corrupted as protobuf and as snappy, read as the other version, and read within limits from a
/// few bytes to what a receiver holds, must each be refused or read whole, and never panic.
#[test]
fn decoding_never_panics_on_corrupted_requests() {
    // Each sample and histogram a record, exemplars alone a record, and each family's metadata once.
    let v1 = Written {
        samples: 9,
        histograms: 5,
        exemplars: 4,
    };
    let v2 = Written {
        samples: 14,
        histograms: 6,
        exemplars: 4,
    };
    for (proto, version, records, written) in [
        (v1_request().encode_to_vec(), Version::V1, 24, v1),
        (v2_request().encode_to_vec(), Version::V2, 28, v2),
    ] {
        let read = decode(&snappy(&proto), version, INBOX_BYTES).expect("the request reads");
        assert_eq!(read, (records, written), "{version:?} before it is corrupted");
    }

    const LIMITS: &[usize] = &[0, 16, 300, 4096, 65_536, 1 << 20, INBOX_BYTES];
    const OUTCOMES: &[&str] = &["read whole", "refused", "refused for its size"];
    let mut cov = Coverage::new(
        [
            HISTOGRAM_SPOILS,
            V1_SPOILS,
            V2_SPOILS,
            TABLE_SPOILS,
            PROTO_CORRUPTIONS,
            SNAPPY_CORRUPTIONS,
            OUTCOMES,
        ]
        .concat()
        .into_iter()
        .map(String::from)
        .chain(["1.0 metadata".into(), "read as the other version".into()])
        .chain(LIMITS.iter().map(|limit| format!("limit {limit}"))),
    );
    fuzz::run(1..20_000, |seed| {
        let mut rng = Rng::seeded(seed);
        let (body, built) = corrupted(&mut rng, &mut cov);
        // A reader of the other version sees fields of numbers it does not expect, or expects as something else.
        let version = match rng.chance(4) {
            true => *rng.pick(&[Version::V1, Version::V2]),
            false => built,
        };
        if version != built {
            cov.reach("read as the other version");
        }
        let limit = *rng.pick(LIMITS);
        cov.reach(format!("limit {limit}"));
        let outcome = match decode(&body, version, limit) {
            Ok(_) => 0,
            Err(false) => 1,
            Err(true) => 2,
        };
        cov.reach(OUTCOMES[outcome]);
        Ok(())
    });
    cov.assert_complete();
}

/// One receiver's settings, as an operator could save them.
struct Settings {
    listen: SocketAddr,
    /// Where a sender reaches it.
    connect: SocketAddr,
    tokens: Vec<String>,
    tls: Tls,
    max_request_bytes: usize,
}

/// The limits a receiver is given, by the bound each is below.
const RECEIVER_LIMITS: &[(usize, &str)] = &[
    (1 << 10, "limit under 1 KiB"),
    (64 << 10, "limit under 64 KiB"),
    (usize::MAX, "limit to 1 MiB"),
];

fn random_settings(rng: &mut Rng, ipv6: bool) -> Settings {
    let (listen, connect) = receiver::random_addresses(rng, ipv6);
    let port = receiver::free_port(connect);
    let tls = receiver::random_tls(rng);
    let tokens = receiver::random_tokens(rng, tls, |rng, i| format!("tok{i}-{:x}", rng.next_u64()));
    Settings {
        listen: SocketAddr::new(listen, port),
        connect: SocketAddr::new(connect, port),
        tokens,
        tls,
        max_request_bytes: (32 << rng.below(16)) + rng.below(1024),
    }
}

fn open(settings: &Settings, pki: &Pki) -> Box<dyn Source> {
    let Settings {
        listen,
        tokens,
        tls,
        max_request_bytes,
        ..
    } = settings;
    receiver::build(
        Receiver::Prometheus,
        *listen,
        *tls,
        tokens,
        "sender",
        *max_request_bytes,
        pki,
    )
}

/// One request a sender makes, and what the receiver answers it.
struct Sent {
    method: &'static str,
    path: &'static str,
    /// Header lines, each ending in CRLF.
    headers: Vec<u8>,
    body: Vec<u8>,
    presents: Presents,
    expected: Expected,
}

#[derive(Debug, PartialEq)]
struct Expected {
    /// 0 for a connection refused before any HTTP is spoken.
    status: u16,
    records: usize,
    /// What a 2.0 sender is told its request wrote.
    written: Option<Written>,
}

/// `Content-Type`s a sender may send, and the version each names, or `None` for one that is not remote_write's.
const CONTENT_TYPES: &[(Option<&str>, Option<Version>)] = &[
    (Some("application/x-protobuf"), Some(Version::V1)),
    (
        Some("application/x-protobuf;proto=prometheus.WriteRequest"),
        Some(Version::V1),
    ),
    (Some("application/x-protobuf; charset=utf-8"), Some(Version::V1)),
    (
        Some("application/x-protobuf;proto=io.prometheus.write.v2.Request"),
        Some(Version::V2),
    ),
    (
        Some("APPLICATION/X-PROTOBUF; proto=\"io.prometheus.write.v2.Request\""),
        Some(Version::V2),
    ),
    (
        Some("application/x-protobuf;proto=io.prometheus.write.v3.Request"),
        None,
    ),
    (Some("application/x-protobufs"), None),
    (Some("application/json"), None),
    (Some(""), None),
    (None, None),
];

/// `Content-Encoding`s a sender may send, and whether each is snappy.
const ENCODINGS: &[(Option<&str>, bool)] = &[
    (Some("snappy"), true),
    (Some("SNAPPY"), true),
    (Some("gzip"), false),
    (Some("identity"), false),
    (Some("snappy, gzip"), false),
    (Some("zstd"), false),
    (None, false),
];

/// Where a request is sent, and how: to remote_write's path by any method, to the health check, or to a path near
/// remote_write's.
const ROUTES: &[(&str, &str)] = &[
    ("POST", prometheus::WRITE_PATH),
    ("GET", prometheus::WRITE_PATH),
    ("PUT", prometheus::WRITE_PATH),
    ("DELETE", prometheus::WRITE_PATH),
    ("HEAD", prometheus::WRITE_PATH),
    ("GET", "/health"),
    ("HEAD", "/health"),
    ("POST", "/health"),
    ("POST", "/"),
    ("POST", "/api/v1/write/"),
    ("POST", "/API/V1/WRITE"),
    ("POST", "/api/v1/push"),
    ("POST", "/api/v1/write%00"),
];

const AUTHORIZATIONS: &[&str] = &[
    "no authorization",
    "a token it does not hold",
    "a token as Basic",
    "a scheme and no token",
    "a token and no scheme",
    "a token not ASCII",
    "a token after a scheme of any case and two spaces",
    "a token it holds",
];

const BODIES: &[&str] = &[
    "a valid body",
    "a corrupted body",
    "an empty body",
    "bytes",
    "a body past the limit",
];

/// A random request to `settings`'s receiver, and what it must answer: the route, then the token, then the request's
/// framing, then its size, then what its body reads as, as the receiver takes them in that order.
fn random_request(rng: &mut Rng, settings: &Settings, cov: &mut Coverage) -> Sent {
    // remote_write's path, by POST, most of the time, so most requests reach the receiver's own checks.
    let (method, path) = match rng.chance(4) {
        true => *rng.pick(ROUTES),
        false => ROUTES[0],
    };
    cov.reach(format!("{method} {path}"));
    let presents = match settings.tls {
        Tls::None => Presents::Nothing,
        _ => *rng.pick(&[
            Presents::Trusted,
            Presents::Trusted,
            Presents::Trusted,
            Presents::Nothing,
            Presents::Stranger,
        ]),
    };
    cov.reach(format!("presents {presents:?} under {:?}", settings.tls));
    // Most requests are framed as a sender of remote_write frames them, so most of those reach the decoder.
    let framed = !rng.chance(4);
    let max = settings.max_request_bytes;
    let kind = match framed && rng.chance(2) {
        true => 0,
        false => [0, 0, 0, 1, 1, 2, 3, 4][rng.below(8)],
    };
    cov.reach(BODIES[kind]);
    let (body, built) = match kind {
        0 => valid(rng),
        1 => corrupted(rng, cov),
        2 => (Vec::new(), Version::V1),
        3 => (
            (0..rng.below(2048)).map(|_| rng.next_u64() as u8).collect(),
            Version::V1,
        ),
        _ => (
            (0..max + 1 + rng.below(4096)).map(|_| rng.next_u64() as u8).collect(),
            Version::V1,
        ),
    };
    let body = match rng.chance(10) {
        true => {
            cov.reach("a body compressed twice");
            snappy(&body)
        }
        false => body,
    };
    let right = (settings.tokens.get(rng.below(settings.tokens.len().max(1))).cloned()).unwrap_or_default();
    let authorization = match framed {
        true => AUTHORIZATIONS.len() - 1,
        false => rng.below(AUTHORIZATIONS.len()),
    };
    cov.reach(AUTHORIZATIONS[authorization]);
    // An `Authorization` value, and whether it presents a token the receiver holds.
    let (authorization, admitted): (Option<Vec<u8>>, bool) = match authorization {
        0 => (None, false),
        1 => (Some(format!("Bearer {right}x").into()), false),
        2 => (Some(format!("Basic {right}").into()), false),
        3 => (Some(b"Bearer".to_vec()), false),
        4 => (Some(right.clone().into()), false),
        5 => (Some(b"Bearer \xe9\xff".to_vec()), false),
        6 => (Some(format!("bEaReR  {right}").into()), !right.is_empty()),
        _ => (Some(format!("Bearer {right}").into()), !right.is_empty()),
    };
    let admitted = admitted || settings.tokens.is_empty();
    let (content_type, version) = match (framed, built) {
        (true, Version::V1) => CONTENT_TYPES[rng.below(3)],
        (true, Version::V2) => CONTENT_TYPES[3 + rng.below(2)],
        (false, _) => *rng.pick(CONTENT_TYPES),
    };
    cov.reach(format!("Content-Type {content_type:?}"));
    let (encoding, is_snappy) = match framed {
        true => ENCODINGS[rng.below(2)],
        false => *rng.pick(ENCODINGS),
    };
    cov.reach(format!("Content-Encoding {encoding:?}"));

    let mut headers = Vec::new();
    for (name, value) in [("Content-Type", content_type), ("Content-Encoding", encoding)] {
        if let Some(value) = value {
            headers.extend(format!("{name}: {value}\r\n").bytes());
        }
    }
    if let Some(authorization) = authorization {
        headers.extend(b"Authorization: ");
        headers.extend(authorization);
        headers.extend(b"\r\n");
    }

    let refused = |status| Expected {
        status,
        records: 0,
        written: None,
    };
    let expected = if settings.tls == Tls::Vouched && presents != Presents::Trusted {
        refused(0)
    } else if path == "/health" {
        refused(if method == "POST" { 405 } else { 200 })
    } else if path != prometheus::WRITE_PATH {
        refused(404)
    } else if method != "POST" {
        refused(405)
    } else if !admitted {
        refused(401)
    } else if version.is_none() || !is_snappy {
        refused(415)
    } else if body.len() > max {
        refused(413)
    } else {
        let version = version.unwrap();
        let v2 = |written| (version == Version::V2).then_some(written);
        match decode(&body, version, INBOX_BYTES) {
            Ok((records, written)) => Expected {
                status: 204,
                records,
                written: v2(written),
            },
            Err(true) => refused(413),
            Err(false) => Expected {
                status: 400,
                records: 0,
                written: v2(Written::default()),
            },
        }
    };
    Sent {
        method,
        path,
        headers,
        body,
        presents,
        expected,
    }
}

/// What a 2.0 answer says its request wrote, where it says.
fn answered_written(answer: &Answer) -> Option<Written> {
    let header = |name: &str| {
        let line = answer
            .head
            .lines()
            .find_map(|l| l.strip_prefix(name)?.strip_prefix(':'))?;
        line.trim().parse::<usize>().ok()
    };
    Some(Written {
        samples: header("x-prometheus-remote-write-samples-written")?,
        histograms: header("x-prometheus-remote-write-histograms-written")?,
        exemplars: header("x-prometheus-remote-write-exemplars-written")?,
    })
}

/// `sent` over a new connection to `at`, under TLS where `tls` says.
fn exchange(at: SocketAddr, tls: Tls, pki: &Pki, sent: &Sent) -> Answer {
    let request = Request {
        method: sent.method,
        path: sent.path,
        headers: &sent.headers,
        body: &sent.body,
        chunked: false,
        presents: sent.presents,
    };
    receiver::exchange(at, tls, pki, &request)
}

/// What a Prometheus receiver documents it answers at `path` to `method`. 0 is no answer: a connection its TLS refused.
fn documented(method: &str, path: &str) -> &'static [u16] {
    match (method, path) {
        ("POST", prometheus::WRITE_PATH) => &[0, 204, 400, 401, 413, 415, 503],
        (_, prometheus::WRITE_PATH) => &[0, 405],
        ("GET" | "HEAD", "/health") => &[0, 200, 503],
        (_, "/health") => &[0, 405],
        _ => &[0, 404],
    }
}

/// A receiver under any settings an operator can save — any address, tokens or none, TLS with or without client
/// certificates, any request limit — answers whatever a sender sends with a status it documents for that path, takes a
/// valid request it admits within its limit whole, and answers within the sockets' timeouts.
#[test]
fn every_setting_answers_random_requests_within_the_documented_set() {
    crate::certs::install_provider();
    let pki = Arc::new(Pki::new());
    let ipv6 = receiver::ipv6();
    let statuses = [0, 200, 204, 400, 401, 404, 405, 413, 415];
    let mut cov = Coverage::new(
        receiver::listen_names(ipv6)
            .map(|ip| format!("listen {ip}"))
            .chain(["None", "Served", "Vouched"].map(|tls| format!("tls {tls}")))
            .chain((0..=3).map(|n| format!("tokens {n}")))
            .chain(RECEIVER_LIMITS.iter().map(|(_, name)| name.to_string()))
            .chain(ROUTES.iter().map(|(method, path)| format!("{method} {path}")))
            .chain(["presents Nothing under None".to_string()])
            .chain(["Served", "Vouched"].iter().flat_map(|tls| {
                ["Nothing", "Trusted", "Stranger"].map(|presents| format!("presents {presents} under {tls}"))
            }))
            .chain(BODIES.iter().chain(AUTHORIZATIONS).map(|s| s.to_string()))
            .chain([
                "a body compressed twice".to_string(),
                "records of a request answered 204".to_string(),
            ])
            .chain(CONTENT_TYPES.iter().map(|(t, _)| format!("Content-Type {t:?}")))
            .chain(ENCODINGS.iter().map(|(e, _)| format!("Content-Encoding {e:?}")))
            .chain(statuses.iter().map(|status| format!("answered {status}"))),
    );
    fuzz::run(1..61, |seed| {
        let mut rng = Rng::seeded(seed);
        let settings = random_settings(&mut rng, ipv6);
        cov.reach(format!("listen {}", settings.listen.ip()));
        cov.reach(format!("tls {:?}", settings.tls));
        cov.reach(format!("tokens {}", settings.tokens.len()));
        let limit = RECEIVER_LIMITS
            .iter()
            .find(|(below, _)| settings.max_request_bytes < *below)
            .unwrap();
        cov.reach(limit.1);
        let mut source = open(&settings, &pki);
        let sent: Vec<Sent> = (0..10).map(|_| random_request(&mut rng, &settings, &mut cov)).collect();
        let expected_records: usize = sent.iter().map(|s| s.expected.records).sum();
        let sending = Arc::new(sent);
        let (connect, tls, pki) = (settings.connect, settings.tls, pki.clone());
        let requests = sending.clone();
        let sender = std::thread::spawn(move || {
            (requests.iter())
                .map(|sent| exchange(connect, tls, &pki, sent))
                .collect::<Vec<Answer>>()
        });
        // A valid request is answered once its records are committed, so they are taken as they arrive.
        let deadline = Instant::now() + Duration::from_secs(60);
        let mut records = 0;
        while !sender.is_finished() {
            if Instant::now() > deadline {
                return Err("the sender is still waiting".into());
            }
            let batch = source.poll_batch(10_000, 1 << 26, Duration::from_millis(10)).unwrap();
            records += batch.len();
            commit(&mut source, &batch);
        }
        let answers = sender.join().unwrap();
        records += source
            .poll_batch(10_000, 1 << 26, Duration::from_millis(10))
            .unwrap()
            .len();

        for (i, (sent, answer)) in sending.iter().zip(&answers).enumerate() {
            let what = format!(
                "request {i} ({} {} under {:?}, presenting {:?}, {} bytes, limit {}, headers {:?})",
                sent.method,
                sent.path,
                settings.tls,
                sent.presents,
                sent.body.len(),
                settings.max_request_bytes,
                String::from_utf8_lossy(&sent.headers),
            );
            if !documented(sent.method, sent.path).contains(&answer.status) {
                return Err(format!(
                    "{what}: answered {}, which it does not document",
                    answer.status
                ));
            }
            if answer.status != sent.expected.status {
                return Err(format!(
                    "{what}: answered {}, not {}: {}",
                    answer.status, sent.expected.status, answer.head
                ));
            }
            if let Some(written) = sent.expected.written
                && answered_written(answer) != Some(written)
            {
                return Err(format!(
                    "{what}: wrote {:?}, not {written:?}: {}",
                    answered_written(answer),
                    answer.head
                ));
            }
            cov.reach(format!("answered {}", answer.status));
            if answer.status == 204 && sent.expected.records > 0 {
                cov.reach("records of a request answered 204");
            }
        }
        match records == expected_records {
            true => Ok(()),
            false => Err(format!(
                "{records} records arrived of the {expected_records} of the requests answered 204"
            )),
        }
    });
    cov.assert_complete();
}

/// What the static endpoint a live Prometheus scrapes serves, as OpenMetrics: a counter and a classic histogram with
/// exemplars, a gauge of NaN, an info, a stateset and a summary, each with its metadata.
const EXPOSITION: &str = r#"# TYPE fuzz_requests counter
# HELP fuzz_requests Requests served.
fuzz_requests_total{code="200"} 7 # {trace_id="4bf92f3577b34da6a3ce929d0e0e4736",span_id="00f067aa0ba902b7"} 1.0
# TYPE fuzz_latency_seconds histogram
# UNIT fuzz_latency_seconds seconds
# HELP fuzz_latency_seconds Time taken.
fuzz_latency_seconds_bucket{le="0.1"} 1 # {trace_id="0af7651916cd43dd8448eb211c80319c",user="u1"} 0.05
fuzz_latency_seconds_bucket{le="+Inf"} 3
fuzz_latency_seconds_sum 1.5
fuzz_latency_seconds_count 3
# TYPE fuzz_queue gauge
fuzz_queue NaN
# TYPE fuzz_build info
fuzz_build_info{version="1"} 1
# TYPE fuzz_feature stateset
fuzz_feature{fuzz_feature="a"} 1
fuzz_feature{fuzz_feature="b"} 0
# TYPE fuzz_rpc_seconds summary
fuzz_rpc_seconds{quantile="0.5"} 0.2
fuzz_rpc_seconds_sum 1
fuzz_rpc_seconds_count 4
# EOF
"#;

/// Serves [`EXPOSITION`] to every request on a port of its own until dropped: what Prometheus scrapes for the exemplars
/// and the metadata types its own metrics lack.
struct Endpoint {
    at: SocketAddr,
    stop: Arc<std::sync::atomic::AtomicBool>,
}

impl Endpoint {
    fn serve() -> Endpoint {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let at = listener.local_addr().unwrap();
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stopped = stop.clone();
        std::thread::spawn(move || {
            while !stopped.load(std::sync::atomic::Ordering::Relaxed) {
                let Ok((mut stream, _)) = listener.accept() else {
                    std::thread::sleep(Duration::from_millis(20));
                    continue;
                };
                let _ = stream.set_nonblocking(false);
                let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
                let mut request = Vec::new();
                let mut byte = [0];
                while !request.ends_with(b"\r\n\r\n") && stream.read_exact(&mut byte).is_ok() {
                    request.push(byte[0]);
                }
                let _ = write!(
                    stream,
                    "HTTP/1.1 200 OK\r\nContent-Type: application/openmetrics-text; version=1.0.0; charset=utf-8\r\n\
                     Content-Length: {}\r\nConnection: close\r\n\r\n{EXPOSITION}",
                    EXPOSITION.len()
                );
            }
        });
        Endpoint { at, stop }
    }
}

impl Drop for Endpoint {
    fn drop(&mut self) {
        self.stop.store(true, std::sync::atomic::Ordering::Relaxed);
    }
}

/// One plain HTTP/1.1 exchange with a local server: its status and body.
fn plain(at: SocketAddr, method: &str, path: &str, wait: Duration) -> (u16, String) {
    let Ok(mut stream) = TcpStream::connect(at) else {
        return (0, String::new());
    };
    stream.set_read_timeout(Some(wait)).unwrap();
    let _ = write!(
        stream,
        "{method} {path} HTTP/1.1\r\nHost: prometheus\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
    );
    let mut answer = String::new();
    let _ = stream.read_to_string(&mut answer);
    let status = answer.get(9..12).and_then(|s| s.parse().ok()).unwrap_or(0);
    (
        status,
        answer
            .split_once("\r\n\r\n")
            .map(|(_, b)| b.to_string())
            .unwrap_or_default(),
    )
}

/// What Prometheus at `web` counts as failed to send to `url`: samples, histograms, exemplars and metadata it dropped
/// on an answer it does not retry.
fn failed(web: SocketAddr, url: &str) -> f64 {
    let (_, metrics) = plain(web, "GET", "/metrics", Duration::from_secs(5));
    let url = format!("url=\"{url}\"");
    (metrics.lines())
        .filter(|l| l.starts_with("prometheus_remote_storage_") && l.contains("_failed_total{") && l.contains(&url))
        .filter_map(|l| l.rsplit(' ').next()?.parse::<f64>().ok())
        .sum()
}

/// Why `record`, as a pipeline reads it, is not one a Prometheus receiver gives, or `Ok`.
fn well_formed(record: &serde_json::Map<String, serde_json::Value>) -> Result<(), String> {
    let has = |field: &str| record.get(field).is_some_and(|v| !v.is_null());
    if !record
        .get("name")
        .and_then(|n| n.as_str())
        .is_some_and(|n| !n.is_empty())
        && !has("attributes")
    {
        return Err("a record of no name nor labels".into());
    }
    if (has("value") || has("count")) && !has("time") {
        return Err("a sample of no time".into());
    }
    match record.get("type").and_then(|t| t.as_str()) {
        Some("exponentialHistogram") if !(has("scale") && has("count")) => {
            return Err("an exponential histogram of no scale or count".into());
        }
        Some("histogram") if has("time") && !(has("bucketCounts") && has("explicitBounds")) => {
            return Err("a custom-bucket histogram of no buckets".into());
        }
        _ => {}
    }
    if let Some(exemplars) = record.get("exemplars") {
        let ok = exemplars
            .as_array()
            .is_some_and(|all| !all.is_empty() && all.iter().all(|e| e.get("value").is_some()));
        if !ok {
            return Err(format!("exemplars {exemplars}"));
        }
    }
    Ok(())
}

/// A real Prometheus, scraping itself and an endpoint of exemplars and every metadata type, remote_writes to receivers
/// of random settings under random remote_write configurations: 1.0 or 2.0, native histograms and exemplars sent or
/// not, metadata sent or not, queues small to large, and timeouts short to long. The receiver never panics, its records
/// keep arriving well formed, and Prometheus fails to send nothing, but where the receiver's limit is set to refuse
/// every request.
#[test]
fn live_prometheus_remote_writes_under_random_configurations() {
    const TEST: &str = "live_prometheus_remote_writes_under_random_configurations";
    let Some(image) = crate::live::prometheus(TEST) else {
        return;
    };
    // A panic on any thread but the test's own is the receiver's: its handlers run on the runtime's threads, where a
    // panic drops a connection and fails no test by itself.
    let panicked = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let seen = panicked.clone();
    let test_thread = std::thread::current().id();
    let _hook = Hook::set(Box::new(move |info| {
        if std::thread::current().id() != test_thread {
            seen.store(true, std::sync::atomic::Ordering::Relaxed);
        }
        eprintln!("{info}");
    }));

    let endpoint = Endpoint::serve();
    let web = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();
    let dir = std::env::temp_dir().join(format!("smeltr-prometheus-fuzz-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let config_file = dir.join("prometheus.yml");
    let mut prometheus: Option<Container> = None;
    // The receiver of the seed before, until Prometheus has reloaded away from it.
    let mut last: Option<Box<dyn Source>> = None;
    let meta = smeltr::plan::topology::SourceMeta::prometheus_receiver();
    let pki = Pki::new();
    let ipv6 = receiver::ipv6();
    let toggles = ["2.0", "native histograms", "exemplars", "metadata"];
    let mut cov = Coverage::new(
        (toggles.iter().flat_map(|t| [format!("{t} on"), format!("{t} off")])).chain(["a refused request".to_string()]),
    );

    // The toggles are the seed's low bits, so eight seeds send every combination of them.
    fuzz::run(1..9, |seed| {
        let mut rng = Rng::seeded(seed);
        let [v2, histograms, exemplars, metadata] = [0, 1, 2, 3].map(|bit| seed >> bit & 1 == 1);
        for (toggle, on) in toggles.iter().zip([v2, histograms, exemplars, metadata]) {
            cov.reach(format!("{toggle} {}", if on { "on" } else { "off" }));
        }
        let forced = seed == 5;
        let mut settings = random_settings(&mut rng, ipv6);
        // Prometheus reaches the receiver over IPv4, on the host's network.
        settings.connect = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), settings.connect.port());
        if !settings.listen.ip().is_unspecified() {
            settings.listen = settings.connect;
        }
        settings.tls = Tls::None;
        if settings.tokens.is_empty() {
            settings.tokens.push(format!("tok-{:x}", rng.next_u64()));
        }
        // 64 bytes is less than any request; a megabyte or more takes any this queue sends.
        settings.max_request_bytes = match forced {
            true => 64,
            false => *rng.pick(&[1 << 20, 8 << 20]),
        };
        let per_send = *rng.pick(&[5, 50, 500, 2000]);
        let token = rng.pick(&settings.tokens).clone();
        let url = format!("http://{}{}", settings.connect, prometheus::WRITE_PATH);
        let what = format!(
            "{per_send} per send, limit {}, listening on {}",
            settings.max_request_bytes, settings.listen
        );
        let mut source = open(&settings, &pki);
        let config = format!(
            r#"global:
  scrape_interval: 1s
scrape_configs:
  - job_name: self
    static_configs: [{{targets: ["{web}"]}}]
  - job_name: static
    scrape_protocols: [OpenMetricsText1.0.0]
    static_configs: [{{targets: ["{endpoint}"]}}]
remote_write:
  - url: {url}
    protobuf_message: {message}
    authorization: {{credentials: "{token}"}}
    remote_timeout: {timeout}
    send_exemplars: {exemplars}
    send_native_histograms: {histograms}
    metadata_config: {{send: {metadata}, send_interval: 1s}}
    queue_config: {{capacity: {capacity}, max_samples_per_send: {per_send}, batch_send_deadline: 1s, max_shards: {shards}, min_backoff: 100ms, max_backoff: 1s}}
"#,
            endpoint = endpoint.at,
            message = if v2 {
                "io.prometheus.write.v2.Request"
            } else {
                "prometheus.WriteRequest"
            },
            timeout = rng.pick(&["2s", "10s", "30s"]),
            capacity = per_send * rng.pick(&[1, 2, 5]),
            shards = rng.pick(&[1, 4]),
        );
        std::fs::write(&config_file, &config).unwrap();
        share(&dir);
        match &prometheus {
            None => {
                let name = format!("smeltr-prometheus-fuzz-{}", std::process::id());
                let started = std::process::Command::new("docker")
                    .args(["run", "-d", "--rm", "--network", "host", "--name", &name, "-v"])
                    .arg(format!("{}:/smeltr:ro", dir.display()))
                    .args([image.as_str(), "--config.file=/smeltr/prometheus.yml"])
                    .arg(format!("--web.listen-address={web}"))
                    .args([
                        "--storage.tsdb.path=/tmp/prometheus",
                        "--web.enable-lifecycle",
                        "--enable-feature=native-histograms,exemplar-storage,metadata-wal-records",
                    ])
                    .output()
                    .expect("docker runs");
                if !started.status.success() {
                    return Err(format!("docker run: {}", String::from_utf8_lossy(&started.stderr)));
                }
                prometheus = Some(Container(name));
            }
            Some(container) => {
                // A reload stops the old queue once its requests in flight are answered, and a request is answered once
                // its records are committed, so the old receiver's records are taken until the reload is done.
                let draining = last.take().map(|mut source| {
                    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
                    let stopping = stop.clone();
                    let drain = std::thread::spawn(move || {
                        while !stopping.load(std::sync::atomic::Ordering::Relaxed) {
                            let batch = source.poll_batch(10_000, 1 << 26, Duration::from_millis(100)).unwrap();
                            commit(&mut source, &batch);
                        }
                    });
                    (stop, drain)
                });
                let (status, answer) = plain(web, "POST", "/-/reload", Duration::from_secs(60));
                if let Some((stop, drain)) = draining {
                    stop.store(true, std::sync::atomic::Ordering::Relaxed);
                    let _ = drain.join();
                }
                if status != 200 {
                    let logs = std::process::Command::new("docker")
                        .args(["logs", "--tail", "20", &container.0])
                        .output();
                    let logs = logs
                        .map(|l| String::from_utf8_lossy(&l.stderr).to_string())
                        .unwrap_or_default();
                    return Err(format!(
                        "{what}: Prometheus does not reload its configuration, answering {status}: {answer}\n{logs}"
                    ));
                }
            }
        }

        // Each kind of record this configuration sends, until every one has arrived and a few sends have passed.
        let (mut up, mut native, mut exemplar, mut family, mut records) = (false, false, false, false, 0);
        let started = Instant::now();
        let deadline = started + Duration::from_secs(60);
        loop {
            let batch = source.poll_batch(10_000, 1 << 26, Duration::from_millis(200)).unwrap();
            for record in &batch {
                let row = meta.record_object(record, "");
                well_formed(&row).map_err(|why| format!("{what}: {why}: {row:?}"))?;
                up |= row.get("name").is_some_and(|n| n == "up") && row.get("value").is_some_and(|v| v == 1.0);
                native |= row.get("type").is_some_and(|t| t == "exponentialHistogram");
                exemplar |= row.get("exemplars").is_some();
                family |= row.get("type").is_some() && row.get("time").is_none();
            }
            records += batch.len();
            commit(&mut source, &batch);
            if panicked.load(std::sync::atomic::Ordering::Relaxed) {
                return Err(format!("{what}: the receiver panicked"));
            }
            let settled = started.elapsed() > Duration::from_secs(4);
            let done = match forced {
                true => failed(web, &url) > 0.0,
                false => up && (native || !histograms) && (exemplar || !exemplars) && (family || !metadata || v2),
            };
            if settled && done {
                break;
            }
            if Instant::now() > deadline {
                return Err(format!(
                    "{what}: after {records} records, up {up}, native histograms {native}, exemplars {exemplar}, \
                     metadata {family}; failed {}",
                    failed(web, &url)
                ));
            }
        }
        if forced {
            cov.reach("a refused request");
        } else if failed(web, &url) != 0.0 {
            return Err(format!("{what}: Prometheus failed to send {}", failed(web, &url)));
        }
        last = Some(source);
        Ok(())
    });
    drop(prometheus);
    let _ = std::fs::remove_dir_all(dir);
    cov.assert_complete();
}
