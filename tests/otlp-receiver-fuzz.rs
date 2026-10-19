//! Random OTLP exports against OTLP receivers of random settings. An exporter drops an export answered with a status it
//! does not retry, and sends again one answered busy, so each answer must be the one OTLP gives, and each export
//! answered as delivered must arrive as its records, each typed and filed as OTLP says.
//!
//! Each seed opens receivers of one, two or three signals on one port, under settings an operator could save, and
//! sends them random exports of every signal: resources, scopes and items with every field and every kind of value,
//! written as protobuf, as OTLP/JSON under either name of each field, or as one gRPC message, plain or gzip, under
//! every way of presenting a token, within each receiver's limit or past it, whole or spoiled. A model built beside
//! each export says what the receiver must answer and the record each item, or each metric's data point, must be. A
//! deterministic PRNG over a seed range, from `harness::fuzz`: a failure names its seed, and the fuzzer checks that
//! its seeds reached every option it knows.

use std::io::Write;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{Map, Value, json};
use smeltr::core::column::{Column, DataType};
use smeltr::core::variant;
use smeltr::io::otlp::Encoding;
use smeltr::io::{Source, SourceRecord};
use smeltr::plan::topology::{Receiver, Signal, SourceMeta};

use crate::harness::docker::{Container, Hook, Scratch, container_logs, share};
use crate::harness::fuzz::{self, Coverage, Rng};
use crate::harness::receiver::{self, Pki, Presents, Tls, commit};

const SIGNALS: [Signal; 3] = [Signal::Logs, Signal::Metrics, Signal::Traces];

/// One port's settings, as an operator could save them: the port's, and each receiver's on it.
struct Settings {
    listen: SocketAddr,
    /// Where an exporter reaches it.
    connect: SocketAddr,
    tls: Tls,
    receivers: Vec<Taking>,
}

/// One receiver on the port: the signal it takes, its tokens and its limit.
struct Taking {
    signal: Signal,
    tokens: Vec<String>,
    max_request_bytes: usize,
}

impl Settings {
    fn taking(&self, signal: Signal) -> Option<&Taking> {
        self.receivers.iter().find(|r| r.signal == signal)
    }
}

/// The limits a receiver is given, by the bound each is below.
const RECEIVER_LIMITS: &[(usize, &str)] = &[(2 << 10, "limit under 2 KiB"), (usize::MAX, "limit past 2 KiB")];

fn random_settings(rng: &mut Rng, ipv6: bool) -> Settings {
    let (listen, connect) = receiver::random_addresses(rng, ipv6);
    let port = receiver::free_port(connect);
    let tls = receiver::random_tls(rng);
    let mut signals: Vec<Signal> = SIGNALS.into_iter().filter(|_| rng.chance(2)).collect();
    if signals.is_empty() {
        signals.push(*rng.pick(&SIGNALS));
    }
    // The receiver that opens the port first serves it.
    for i in (1..signals.len()).rev() {
        signals.swap(i, rng.below(i + 1));
    }
    let receivers = (signals.into_iter())
        .map(|signal| Taking {
            signal,
            tokens: receiver::random_tokens(rng, tls, |rng, i| format!("tok{i}-{:x}", rng.next_u64())),
            max_request_bytes: match rng.below(3) {
                0 => 512 + rng.below(1536),
                _ => (4 << 10 << rng.below(5)) + rng.below(1024),
            },
        })
        .collect();
    Settings {
        listen: SocketAddr::new(listen, port),
        connect: SocketAddr::new(connect, port),
        tls,
        receivers,
    }
}

/// The receivers `settings` names, each opened on the port in turn.
fn open(settings: &Settings, pki: &Pki) -> Vec<(Signal, Box<dyn Source>)> {
    (settings.receivers.iter())
        .map(|taking| {
            let otlp = Receiver::Otlp(taking.signal);
            let source = receiver::build(
                otlp,
                settings.listen,
                settings.tls,
                &taking.tokens,
                "exporter",
                taking.max_request_bytes,
                pki,
            );
            (taking.signal, source)
        })
        .collect()
}

// What a record holds, typed as its columns type it.

/// A double, equal to another of the same bits, or to any NaN where it is one.
#[derive(Clone, Copy, Debug)]
struct F(f64);

impl PartialEq for F {
    fn eq(&self, other: &F) -> bool {
        self.0.to_bits() == other.0.to_bits() || (self.0.is_nan() && other.0.is_nan())
    }
}

/// One value of a record, of the type it was read as. A time is microseconds since the epoch.
#[derive(Clone, Debug, PartialEq)]
enum T {
    Null,
    Bool(bool),
    Int(i64),
    Float(F),
    Text(String),
    Bytes(Vec<u8>),
    Time(i64),
    List(Vec<T>),
    Object(Vec<(String, T)>),
}

impl T {
    fn float(f: f64) -> T {
        T::Float(F(f))
    }

    fn text(text: &str) -> T {
        T::Text(text.to_string())
    }
}

/// A variant as `T`.
fn variant_t(encoded: &[u8]) -> T {
    use variant::Value as V;
    match variant::read(encoded) {
        V::Null => T::Null,
        V::Bool(b) => T::Bool(b),
        V::I64(i) => T::Int(i),
        V::F64(f) => T::float(f),
        V::Str(s) => T::text(s),
        V::Bytes(b) => T::Bytes(b.to_vec()),
        V::DateTime(micros) => T::Time(micros),
        V::Duration(_) => panic!("an OTLP record holds no duration"),
        V::Array(items) => T::List(items.map(variant_t).collect()),
        V::Object(entries) => T::Object(entries.map(|(k, v)| (k.to_string(), variant_t(v))).collect()),
    }
}

/// Row `row` of `column`, as `T`.
fn column_t(column: &Column, row: usize) -> T {
    if let Some(constant) = column.constant_value() {
        use smeltr::core::column::Scalar as S;
        return match constant {
            S::Bool(b) => T::Bool(*b),
            S::I64(i) => T::Int(*i),
            S::F64(f) => T::float(*f),
            S::DateTime(micros) => T::Time(*micros),
            S::Str(s) => T::Text(s.clone()),
            S::Bytes(b) => T::Bytes(b.clone()),
            S::Variant(v) => variant_t(v),
            S::Duration(_) => panic!("an OTLP record holds no duration"),
        };
    }
    match column.data_type() {
        // A struct column holds no nulls of its own: a row the record left unset is null in every field.
        DataType::Struct(fields, _) => {
            let read: Vec<(String, T)> = (fields.iter())
                .map(|(name, _)| (name.clone(), column_t(column.field(name).unwrap(), row)))
                .collect();
            match read.iter().all(|(_, t)| *t == T::Null) {
                true => T::Null,
                false => T::Object(read),
            }
        }
        _ if !column.is_valid(row) => T::Null,
        DataType::Bool => {
            let bits = column.bool().unwrap();
            T::Bool(bits.values[row / 64] >> (row % 64) & 1 == 1)
        }
        DataType::I64 => T::Int(column.i64().unwrap().values[row]),
        DataType::F64 => T::float(column.f64().unwrap().values[row]),
        DataType::DateTime => T::Time(column.datetime().unwrap().values[row]),
        DataType::Str => T::Text(String::from_utf8(column.var().unwrap().get(row).to_vec()).unwrap()),
        DataType::Variant => variant_t(column.variant().unwrap().get(row)),
        DataType::Array(_) => {
            let list = column.list().unwrap();
            let (from, to) = (list.offsets[row] as usize, list.offsets[row + 1] as usize);
            T::List((from..to).map(|i| column_t(list.child, i)).collect())
        }
        other => panic!("an OTLP record holds no {other:?}"),
    }
}

/// What a record holds: each field of its signal's record by name, what it is filed under, and who sent it.
#[derive(Clone, Debug, PartialEq)]
struct Got {
    fields: Vec<(String, T)>,
    filed: Filed,
    remote: String,
}

/// What a record is filed under: its resource's attributes, and its scope's name, version and attributes.
#[derive(Clone, Debug, Default, PartialEq)]
struct Filed {
    resource: Option<T>,
    scope_name: Option<String>,
    scope_version: Option<String>,
    scope_attributes: Option<T>,
}

fn read_back(signal: Signal, records: &[SourceRecord]) -> Vec<Got> {
    let meta = SourceMeta::otlp(signal);
    (records.iter())
        .map(|record| {
            let fields = (meta.record().iter().enumerate())
                .map(|(i, (name, _))| (name.to_string(), column_t(record.chunk.column(i), record.row as usize)))
                .collect();
            let mut filed = Filed::default();
            for (key, value) in record.chunk.filed().into_iter().flatten() {
                let text = || String::from_utf8(value.clone()).unwrap();
                match key.as_str() {
                    "resource" => filed.resource = Some(variant_t(value)),
                    "scope.name" => filed.scope_name = Some(text()),
                    "scope.version" => filed.scope_version = Some(text()),
                    "scope.attributes" => filed.scope_attributes = Some(variant_t(value)),
                    other => panic!("a record filed under {other}"),
                }
            }
            Got {
                fields,
                filed,
                remote: record.topic.to_string(),
            }
        })
        .collect()
}

// An export, as the model holds it: what an exporter means to send, before it is written in any encoding.

/// What drawing one request takes from the seed, and the options it reached.
struct Gen {
    rng: Rng,
    labels: Vec<String>,
    /// Draws only what an OpenTelemetry Collector passes on as it was given.
    collected: bool,
}

impl Gen {
    fn below(&mut self, n: usize) -> usize {
        self.rng.below(n)
    }

    fn chance(&mut self, one_in: usize) -> bool {
        self.rng.chance(one_in)
    }

    fn pick<'a, X>(&mut self, xs: &'a [X]) -> &'a X {
        self.rng.pick(xs)
    }

    fn label(&mut self, label: impl Into<String>) {
        self.labels.push(label.into());
    }
}

/// An `AnyValue`'s value; `None` is an `AnyValue` that holds none.
#[derive(Clone, Debug)]
enum Any {
    Str(String),
    Bool(bool),
    Int(i64),
    Double(f64),
    Bytes(Vec<u8>),
    Array(Vec<Option<Any>>),
    Kvlist(Vec<Kv>),
}

/// A `KeyValue`. Its value is `None` where it carries no `AnyValue` at all.
#[derive(Clone, Debug)]
struct Kv {
    key: String,
    value: Option<Option<Any>>,
}

#[derive(Clone, Debug, Default)]
struct Resource {
    attributes: Vec<Kv>,
    dropped: u32,
}

#[derive(Clone, Debug, Default)]
struct Scope {
    name: String,
    version: String,
    attributes: Vec<Kv>,
    dropped: u32,
}

/// An `Export*ServiceRequest` of one signal.
#[derive(Clone, Debug)]
struct Export {
    signal: Signal,
    resources: Vec<Resourced>,
}

#[derive(Clone, Debug)]
struct Resourced {
    resource: Option<Resource>,
    scopes: Vec<Scoped>,
}

#[derive(Clone, Debug)]
struct Scoped {
    scope: Option<Scope>,
    items: Vec<Item>,
}

/// One item of a scope, of its export's signal.
#[derive(Clone, Debug)]
enum Item {
    Log(Log),
    Metric(Metric),
    Span(Span),
}

#[derive(Clone, Debug, Default)]
struct Log {
    time: u64,
    observed: u64,
    severity_number: i32,
    severity_text: String,
    /// `None` for no body; `Some(None)` for a body that holds no value.
    body: Option<Option<Any>>,
    attributes: Vec<Kv>,
    dropped: u32,
    flags: u32,
    trace_id: Vec<u8>,
    span_id: Vec<u8>,
    event_name: String,
}

#[derive(Clone, Debug, Default)]
struct Span {
    trace_id: Vec<u8>,
    span_id: Vec<u8>,
    trace_state: String,
    parent_span_id: Vec<u8>,
    flags: u32,
    name: String,
    kind: i32,
    start: u64,
    end: u64,
    attributes: Vec<Kv>,
    dropped_attributes: u32,
    events: Vec<Event>,
    dropped_events: u32,
    links: Vec<Link>,
    dropped_links: u32,
    status: Option<Status>,
}

#[derive(Clone, Debug, Default)]
struct Event {
    time: u64,
    name: String,
    attributes: Vec<Kv>,
    dropped: u32,
}

#[derive(Clone, Debug, Default)]
struct Link {
    trace_id: Vec<u8>,
    span_id: Vec<u8>,
    trace_state: String,
    attributes: Vec<Kv>,
    dropped: u32,
    flags: u32,
}

#[derive(Clone, Debug, Default)]
struct Status {
    message: String,
    code: i32,
}

#[derive(Clone, Debug, Default)]
struct Metric {
    name: String,
    description: String,
    unit: String,
    data: Option<Data>,
    metadata: Vec<Kv>,
}

#[derive(Clone, Debug)]
enum Data {
    Gauge(Vec<NumberPoint>),
    Sum {
        points: Vec<NumberPoint>,
        temporality: i32,
        monotonic: bool,
    },
    Histogram {
        points: Vec<HistogramPoint>,
        temporality: i32,
    },
    Exponential {
        points: Vec<ExponentialPoint>,
        temporality: i32,
    },
    Summary(Vec<SummaryPoint>),
}

/// What every data point carries.
#[derive(Clone, Debug, Default)]
struct Base {
    attributes: Vec<Kv>,
    start: u64,
    time: u64,
    flags: u32,
}

/// A number data point's or an exemplar's value.
#[derive(Clone, Copy, Debug)]
enum Num {
    Double(f64),
    Int(i64),
}

#[derive(Clone, Debug, Default)]
struct NumberPoint {
    base: Base,
    value: Option<Num>,
    exemplars: Vec<Exemplar>,
}

#[derive(Clone, Debug, Default)]
struct HistogramPoint {
    base: Base,
    count: u64,
    sum: Option<f64>,
    min: Option<f64>,
    max: Option<f64>,
    bucket_counts: Vec<u64>,
    bounds: Vec<f64>,
    exemplars: Vec<Exemplar>,
}

#[derive(Clone, Debug, Default)]
struct ExponentialPoint {
    base: Base,
    count: u64,
    sum: Option<f64>,
    min: Option<f64>,
    max: Option<f64>,
    scale: i32,
    zero_count: u64,
    zero_threshold: f64,
    positive: Option<Buckets>,
    negative: Option<Buckets>,
    exemplars: Vec<Exemplar>,
}

#[derive(Clone, Debug, Default)]
struct Buckets {
    offset: i32,
    counts: Vec<u64>,
}

#[derive(Clone, Debug, Default)]
struct SummaryPoint {
    base: Base,
    count: u64,
    sum: f64,
    quantiles: Vec<(f64, f64)>,
}

#[derive(Clone, Debug, Default)]
struct Exemplar {
    attributes: Vec<Kv>,
    time: u64,
    value: Option<Num>,
    span_id: Vec<u8>,
    trace_id: Vec<u8>,
}

// Drawing an export: every field, often at its zero value, which OTLP sends for one an item does not carry.

const TEXTS: &[&str] = &[
    "",
    "a",
    "checkout",
    "ünïcödé ✓",
    "say \"hi\" \\ there",
    "line\nbreak\ttab",
    "nul\u{0}inside",
    "🦀",
];

/// Few, so an object often gives one twice.
const KEYS: &[&str] = &["k", "service.name", "dup", "", "ключ"];

fn text(g: &mut Gen) -> String {
    match g.chance(4) {
        true => format!("t{:x}", g.rng.next_u64() >> 40),
        false => g.pick(TEXTS).to_string(),
    }
}

fn int64(g: &mut Gen) -> i64 {
    match g.below(6) {
        0 => 0,
        1 => -1,
        2 => i64::MIN,
        3 => i64::MAX,
        _ => (g.rng.next_u64() as i64) >> g.below(63),
    }
}

fn int32(g: &mut Gen) -> i32 {
    match g.below(6) {
        0 => 0,
        1 => -1,
        2 => i32::MIN,
        3 => i32::MAX,
        _ => g.below(7) as i32,
    }
}

fn uint32(g: &mut Gen) -> u32 {
    match g.below(4) {
        0 => 0,
        1 => u32::MAX,
        _ => g.below(300) as u32,
    }
}

fn uint64(g: &mut Gen) -> u64 {
    match g.below(5) {
        0 => 0,
        1 => u64::MAX,
        2 => 1 << 53,
        _ => g.rng.next_u64() >> g.below(64),
    }
}

/// A time in nanoseconds. One under a microsecond is none, as a datetime holds microseconds.
fn nanos(g: &mut Gen) -> u64 {
    match g.below(6) {
        0 => 0,
        1 => {
            g.label("a time under a microsecond");
            999
        }
        2 => 1_700_000_000_123_456_789,
        3 => u64::MAX,
        _ => g.rng.next_u64(),
    }
}

fn double(g: &mut Gen) -> f64 {
    const DOUBLES: &[f64] = &[
        0.0,
        -0.0,
        1.5,
        -2.25,
        0.1,
        f64::NAN,
        f64::INFINITY,
        f64::NEG_INFINITY,
        1e300,
        5e-324,
        f64::MAX,
        3.0,
    ];
    let d = match g.chance(4) {
        true => loop {
            let random = f64::from_bits(g.rng.next_u64());
            if random.is_finite() {
                break random;
            }
        },
        false => *g.pick(DOUBLES),
    };
    // The collector writes a -0 it was given as no value, which reads back as 0.
    match g.collected && d == 0.0 {
        true => 0.0,
        false => d,
    }
}

/// A trace or span ID of `len` bytes, or one that is not: empty, all zero, or of another length.
fn id(g: &mut Gen, len: usize) -> Vec<u8> {
    match g.below(6) {
        0 => Vec::new(),
        1 => {
            g.label("an all-zero ID");
            vec![0; len]
        }
        // The collector refuses an ID of another length.
        2 if !g.collected => {
            g.label("an ID of the wrong length");
            (0..*g.pick(&[len / 2, len + 1, 3])).map(|i| i as u8 + 1).collect()
        }
        // Its first byte is odd, so a drawn ID is never all zero.
        _ => (0..len).map(|i| (g.rng.next_u64() as u8) | (i == 0) as u8).collect(),
    }
}

fn any(g: &mut Gen, depth: usize) -> Option<Any> {
    let kinds = if depth < 2 { 9 } else { 7 };
    let (label, value) = match g.below(kinds) {
        0 => ("no value", None),
        1 | 6 => ("a string", Some(Any::Str(text(g)))),
        2 => ("a bool", Some(Any::Bool(g.chance(2)))),
        3 => ("an int", Some(Any::Int(int64(g)))),
        4 => ("a double", Some(Any::Double(double(g)))),
        5 => {
            let bytes = (0..g.below(6)).map(|_| g.rng.next_u64() as u8).collect();
            ("bytes", Some(Any::Bytes(bytes)))
        }
        7 => {
            let items = (0..g.below(4)).map(|_| any(g, depth + 1)).collect();
            ("an array", Some(Any::Array(items)))
        }
        _ => ("a kvlist", Some(Any::Kvlist(kvs(g, depth + 1)))),
    };
    g.label(format!("value {label}"));
    value
}

fn kvs(g: &mut Gen, depth: usize) -> Vec<Kv> {
    let kvs: Vec<Kv> = (0..g.below(5))
        .map(|_| Kv {
            key: g.pick(KEYS).to_string(),
            value: match g.chance(8) {
                true => {
                    g.label("an attribute that carries no AnyValue");
                    None
                }
                false => Some(any(g, depth)),
            },
        })
        .collect();
    if (kvs.iter().enumerate()).any(|(i, kv)| kvs[..i].iter().any(|before| before.key == kv.key)) {
        g.label("a key given twice");
    }
    kvs
}

fn attributes(g: &mut Gen) -> Vec<Kv> {
    kvs(g, 0)
}

fn random_export(g: &mut Gen, signal: Signal) -> Export {
    let resources = (0..g.below(4))
        .map(|_| Resourced {
            resource: (!g.chance(4)).then(|| Resource {
                attributes: attributes(g),
                dropped: uint32(g),
            }),
            scopes: (0..g.below(4))
                .map(|_| Scoped {
                    scope: (!g.chance(4)).then(|| Scope {
                        name: text(g),
                        version: text(g),
                        attributes: attributes(g),
                        dropped: uint32(g),
                    }),
                    items: (0..g.below(4)).map(|_| item(g, signal)).collect(),
                })
                .collect(),
        })
        .collect();
    Export { signal, resources }
}

fn item(g: &mut Gen, signal: Signal) -> Item {
    match signal {
        Signal::Logs => Item::Log(Log {
            time: nanos(g),
            observed: nanos(g),
            severity_number: int32(g),
            severity_text: text(g),
            body: match g.below(4) {
                0 => None,
                _ => Some(any(g, 0)),
            },
            attributes: attributes(g),
            dropped: uint32(g),
            flags: uint32(g),
            trace_id: id(g, 16),
            span_id: id(g, 8),
            event_name: text(g),
        }),
        Signal::Traces => Item::Span(Span {
            trace_id: id(g, 16),
            span_id: id(g, 8),
            trace_state: text(g),
            parent_span_id: id(g, 8),
            flags: uint32(g),
            name: text(g),
            kind: int32(g),
            start: nanos(g),
            end: nanos(g),
            attributes: attributes(g),
            dropped_attributes: uint32(g),
            events: (0..g.below(3))
                .map(|_| Event {
                    time: nanos(g),
                    name: text(g),
                    attributes: attributes(g),
                    dropped: uint32(g),
                })
                .collect(),
            dropped_events: uint32(g),
            links: (0..g.below(3))
                .map(|_| Link {
                    trace_id: id(g, 16),
                    span_id: id(g, 8),
                    trace_state: text(g),
                    attributes: attributes(g),
                    dropped: uint32(g),
                    flags: uint32(g),
                })
                .collect(),
            dropped_links: uint32(g),
            status: (!g.chance(3)).then(|| Status {
                message: text(g),
                code: int32(g),
            }),
        }),
        Signal::Metrics => Item::Metric(metric(g)),
    }
}

fn metric(g: &mut Gen) -> Metric {
    let temporality = |g: &mut Gen| *g.pick(&[0, 1, 2, 2, 7, -1]);
    let points = |g: &mut Gen| g.below(4);
    let data = match g.below(6) {
        0 => {
            g.label("a metric with no data");
            None
        }
        1 => Some(Data::Gauge((0..points(g)).map(|_| number_point(g)).collect())),
        2 => Some(Data::Sum {
            points: (0..points(g)).map(|_| number_point(g)).collect(),
            temporality: temporality(g),
            monotonic: g.chance(2),
        }),
        3 => Some(Data::Histogram {
            points: (0..points(g)).map(|_| histogram_point(g)).collect(),
            temporality: temporality(g),
        }),
        4 => Some(Data::Exponential {
            points: (0..points(g)).map(|_| exponential_point(g)).collect(),
            temporality: temporality(g),
        }),
        _ => Some(Data::Summary(
            (0..points(g))
                .map(|_| SummaryPoint {
                    base: base(g),
                    count: uint64(g),
                    sum: double(g),
                    quantiles: (0..g.below(4)).map(|_| (double(g), double(g))).collect(),
                })
                .collect(),
        )),
    };
    Metric {
        name: text(g),
        description: text(g),
        unit: text(g),
        data,
        metadata: attributes(g),
    }
}

fn base(g: &mut Gen) -> Base {
    Base {
        attributes: attributes(g),
        start: nanos(g),
        time: nanos(g),
        flags: uint32(g),
    }
}

fn num(g: &mut Gen) -> Option<Num> {
    match g.below(3) {
        0 => None,
        1 => Some(Num::Double(double(g))),
        _ => Some(Num::Int(int64(g))),
    }
}

fn exemplars(g: &mut Gen) -> Vec<Exemplar> {
    (0..g.below(3))
        .map(|_| Exemplar {
            attributes: attributes(g),
            time: nanos(g),
            value: num(g),
            span_id: id(g, 8),
            trace_id: id(g, 16),
        })
        .collect()
}

fn optional_double(g: &mut Gen) -> Option<f64> {
    (!g.chance(3)).then(|| double(g))
}

fn counts(g: &mut Gen) -> Vec<u64> {
    (0..g.below(5)).map(|_| uint64(g)).collect()
}

fn number_point(g: &mut Gen) -> NumberPoint {
    NumberPoint {
        base: base(g),
        value: num(g),
        exemplars: exemplars(g),
    }
}

fn histogram_point(g: &mut Gen) -> HistogramPoint {
    HistogramPoint {
        base: base(g),
        count: uint64(g),
        sum: optional_double(g),
        min: optional_double(g),
        max: optional_double(g),
        bucket_counts: counts(g),
        bounds: (0..g.below(4)).map(|_| double(g)).collect(),
        exemplars: exemplars(g),
    }
}

fn exponential_point(g: &mut Gen) -> ExponentialPoint {
    // The collector writes a point's buckets whether it was given them or not.
    let buckets = |g: &mut Gen| {
        (g.collected || !g.chance(3)).then(|| Buckets {
            offset: int32(g),
            counts: counts(g),
        })
    };
    ExponentialPoint {
        base: base(g),
        count: uint64(g),
        sum: optional_double(g),
        min: optional_double(g),
        max: optional_double(g),
        scale: int32(g),
        zero_count: uint64(g),
        zero_threshold: double(g),
        positive: buckets(g),
        negative: buckets(g),
        exemplars: exemplars(g),
    }
}

// What the receiver must make of an export: a record for each item, or for each data point of a metric, its fields
// null at their zero values.

fn time_t(nanos: u64) -> T {
    match (nanos / 1000) as i64 {
        0 => T::Null,
        micros => T::Time(micros),
    }
}

fn int_t(n: impl Into<i64>) -> T {
    match n.into() {
        0 => T::Null,
        n => T::Int(n),
    }
}

fn text_t(text: &str) -> T {
    match text.is_empty() {
        true => T::Null,
        false => T::text(text),
    }
}

fn id_t(id: &[u8], len: usize) -> T {
    match id.len() == len && id.iter().any(|b| *b != 0) {
        true => T::Text(id.iter().map(|b| format!("{b:02x}")).collect()),
        false => T::Null,
    }
}

fn any_t(value: &Option<Any>) -> T {
    match value {
        None => T::Null,
        Some(Any::Str(s)) => T::text(s),
        Some(Any::Bool(b)) => T::Bool(*b),
        Some(Any::Int(i)) => T::Int(*i),
        Some(Any::Double(d)) => T::float(*d),
        Some(Any::Bytes(b)) => T::Bytes(b.clone()),
        Some(Any::Array(items)) => T::List(items.iter().map(any_t).collect()),
        Some(Any::Kvlist(kvs)) => object_t(kvs),
    }
}

/// Pairs as an object: one that carries no value is no entry, and of a key given twice the last value stands, in
/// the first one's place.
fn object_t(kvs: &[Kv]) -> T {
    let mut entries: Vec<(String, T)> = Vec::new();
    for kv in kvs {
        let Some(value @ Some(_)) = &kv.value else { continue };
        match entries.iter_mut().find(|(key, _)| *key == kv.key) {
            Some(entry) => entry.1 = any_t(value),
            None => entries.push((kv.key.clone(), any_t(value))),
        }
    }
    T::Object(entries)
}

/// Pairs as a field: null for none.
fn pairs_t(kvs: &[Kv]) -> T {
    match kvs.is_empty() {
        true => T::Null,
        false => object_t(kvs),
    }
}

fn list_t<X>(items: &[X], each: impl Fn(&X) -> T) -> T {
    match items.is_empty() {
        true => T::Null,
        false => T::List(items.iter().map(each).collect()),
    }
}

/// An object of `entries`, each left out where it is null.
fn entries_t(entries: Vec<(&str, T)>) -> T {
    T::Object(
        (entries.into_iter())
            .filter(|(_, t)| *t != T::Null)
            .map(|(key, t)| (key.to_string(), t))
            .collect(),
    )
}

fn num_t(value: Option<Num>) -> T {
    match value {
        None => T::Null,
        Some(Num::Double(d)) => T::float(d),
        Some(Num::Int(i)) => T::Int(i),
    }
}

fn count_t(n: u64) -> T {
    T::float(n as f64)
}

fn optional_t(d: Option<f64>) -> T {
    d.map_or(T::Null, T::float)
}

fn filed_of(resource: &Option<Resource>, scope: &Option<Scope>) -> Filed {
    let resource = resource.as_ref().filter(|r| !r.attributes.is_empty());
    let scope = scope.clone().unwrap_or_default();
    let text = |text: &str| (!text.is_empty()).then(|| text.to_string());
    Filed {
        resource: resource.map(|r| object_t(&r.attributes)),
        scope_name: text(&scope.name),
        scope_version: text(&scope.version),
        scope_attributes: (!scope.attributes.is_empty()).then(|| object_t(&scope.attributes)),
    }
}

/// The records `export` must arrive as, each from `remote`.
fn records_of(export: &Export, remote: &str) -> Vec<Got> {
    let mut records = Vec::new();
    for resourced in &export.resources {
        for scoped in &resourced.scopes {
            let filed = filed_of(&resourced.resource, &scoped.scope);
            for item in &scoped.items {
                for fields in item_fields(item) {
                    records.push(Got {
                        fields: fields.into_iter().map(|(name, t)| (name.to_string(), t)).collect(),
                        filed: filed.clone(),
                        remote: remote.to_string(),
                    });
                }
            }
        }
    }
    records
}

/// The fields of each record `item` must be, in its signal's record's order.
fn item_fields(item: &Item) -> Vec<Vec<(&'static str, T)>> {
    match item {
        Item::Log(log) => vec![vec![
            ("time", time_t(log.time)),
            ("observedTime", time_t(log.observed)),
            ("severityNumber", int_t(log.severity_number)),
            ("severityText", text_t(&log.severity_text)),
            ("body", log.body.as_ref().map_or(T::Null, any_t)),
            ("attributes", pairs_t(&log.attributes)),
            ("traceId", id_t(&log.trace_id, 16)),
            ("spanId", id_t(&log.span_id, 8)),
            ("flags", int_t(log.flags)),
            ("eventName", text_t(&log.event_name)),
            ("droppedAttributesCount", int_t(log.dropped)),
        ]],
        Item::Span(span) => {
            let status = span.status.clone().unwrap_or_default();
            let event = |e: &Event| {
                entries_t(vec![
                    ("time", time_t(e.time)),
                    ("name", text_t(&e.name)),
                    ("attributes", pairs_t(&e.attributes)),
                    ("droppedAttributesCount", int_t(e.dropped)),
                ])
            };
            let link = |l: &Link| {
                entries_t(vec![
                    ("traceId", id_t(&l.trace_id, 16)),
                    ("spanId", id_t(&l.span_id, 8)),
                    ("traceState", text_t(&l.trace_state)),
                    ("attributes", pairs_t(&l.attributes)),
                    ("droppedAttributesCount", int_t(l.dropped)),
                    ("flags", int_t(l.flags)),
                ])
            };
            vec![vec![
                ("traceId", id_t(&span.trace_id, 16)),
                ("spanId", id_t(&span.span_id, 8)),
                ("parentSpanId", id_t(&span.parent_span_id, 8)),
                ("traceState", text_t(&span.trace_state)),
                ("flags", int_t(span.flags)),
                ("name", text_t(&span.name)),
                ("kind", int_t(span.kind)),
                ("startTime", time_t(span.start)),
                ("endTime", time_t(span.end)),
                ("attributes", pairs_t(&span.attributes)),
                ("droppedAttributesCount", int_t(span.dropped_attributes)),
                ("events", list_t(&span.events, event)),
                ("droppedEventsCount", int_t(span.dropped_events)),
                ("links", list_t(&span.links, link)),
                ("droppedLinksCount", int_t(span.dropped_links)),
                ("statusCode", int_t(status.code)),
                ("statusMessage", text_t(&status.message)),
            ]]
        }
        Item::Metric(metric) => metric_fields(metric),
    }
}

/// A point's own fields, each null unless its type gives it.
#[derive(Default)]
struct PointT {
    count: Option<T>,
    sum: Option<T>,
    min: Option<T>,
    max: Option<T>,
    bucket_counts: Option<T>,
    explicit_bounds: Option<T>,
    scale: Option<T>,
    zero_count: Option<T>,
    zero_threshold: Option<T>,
    positive: Option<T>,
    negative: Option<T>,
    quantile_values: Option<T>,
}

fn metric_fields(metric: &Metric) -> Vec<Vec<(&'static str, T)>> {
    let Some(data) = &metric.data else {
        return Vec::new();
    };
    let (kind, temporality, monotonic) = match data {
        Data::Gauge(_) => ("gauge", T::Null, T::Null),
        Data::Sum {
            temporality, monotonic, ..
        } => ("sum", int_t(*temporality), T::Bool(*monotonic)),
        Data::Histogram { temporality, .. } => ("histogram", int_t(*temporality), T::Null),
        Data::Exponential { temporality, .. } => ("exponentialHistogram", int_t(*temporality), T::Null),
        Data::Summary(_) => ("summary", T::Null, T::Null),
    };
    let exemplar = |e: &Exemplar| {
        entries_t(vec![
            ("time", time_t(e.time)),
            ("value", num_t(e.value)),
            ("traceId", id_t(&e.trace_id, 16)),
            ("spanId", id_t(&e.span_id, 8)),
            ("filteredAttributes", pairs_t(&e.attributes)),
        ])
    };
    let floats = |xs: &[f64]| list_t(xs, |x| T::float(*x));
    let counts = |xs: &[u64]| list_t(xs, |n| count_t(*n));
    let buckets = |b: &Option<Buckets>| {
        b.as_ref().map_or(T::Null, |b| {
            T::Object(vec![
                ("offset".into(), T::Int(b.offset.into())),
                (
                    "bucketCounts".into(),
                    T::List(b.counts.iter().map(|n| count_t(*n)).collect()),
                ),
            ])
        })
    };
    // Each point's base, value, exemplars and own fields.
    let points: Vec<(&Base, T, T, PointT)> = match data {
        Data::Gauge(points) | Data::Sum { points, .. } => (points.iter())
            .map(|p| {
                (
                    &p.base,
                    num_t(p.value),
                    list_t(&p.exemplars, exemplar),
                    PointT::default(),
                )
            })
            .collect(),
        Data::Histogram { points, .. } => (points.iter())
            .map(|p| {
                let own = PointT {
                    count: Some(count_t(p.count)),
                    sum: Some(optional_t(p.sum)),
                    min: Some(optional_t(p.min)),
                    max: Some(optional_t(p.max)),
                    bucket_counts: Some(counts(&p.bucket_counts)),
                    explicit_bounds: Some(floats(&p.bounds)),
                    ..PointT::default()
                };
                (&p.base, T::Null, list_t(&p.exemplars, exemplar), own)
            })
            .collect(),
        Data::Exponential { points, .. } => (points.iter())
            .map(|p| {
                let own = PointT {
                    count: Some(count_t(p.count)),
                    sum: Some(optional_t(p.sum)),
                    min: Some(optional_t(p.min)),
                    max: Some(optional_t(p.max)),
                    scale: Some(T::Int(p.scale.into())),
                    zero_count: Some(count_t(p.zero_count)),
                    zero_threshold: (p.zero_threshold != 0.0).then_some(T::float(p.zero_threshold)),
                    positive: Some(buckets(&p.positive)),
                    negative: Some(buckets(&p.negative)),
                    ..PointT::default()
                };
                (&p.base, T::Null, list_t(&p.exemplars, exemplar), own)
            })
            .collect(),
        Data::Summary(points) => (points.iter())
            .map(|p| {
                let quantile = |(q, v): &(f64, f64)| {
                    T::Object(vec![("quantile".into(), T::float(*q)), ("value".into(), T::float(*v))])
                };
                let own = PointT {
                    count: Some(count_t(p.count)),
                    sum: Some(T::float(p.sum)),
                    quantile_values: Some(list_t(&p.quantiles, quantile)),
                    ..PointT::default()
                };
                (&p.base, T::Null, T::Null, own)
            })
            .collect(),
    };
    let null = |t: Option<T>| t.unwrap_or(T::Null);
    (points.into_iter())
        .map(|(base, value, exemplars, own)| {
            vec![
                ("name", text_t(&metric.name)),
                ("description", text_t(&metric.description)),
                ("unit", text_t(&metric.unit)),
                ("type", T::text(kind)),
                ("metadata", pairs_t(&metric.metadata)),
                ("aggregationTemporality", temporality.clone()),
                ("isMonotonic", monotonic.clone()),
                ("startTime", time_t(base.start)),
                ("time", time_t(base.time)),
                ("attributes", pairs_t(&base.attributes)),
                ("flags", int_t(base.flags)),
                ("value", value),
                ("count", null(own.count)),
                ("sum", null(own.sum)),
                ("min", null(own.min)),
                ("max", null(own.max)),
                ("bucketCounts", null(own.bucket_counts)),
                ("explicitBounds", null(own.explicit_bounds)),
                ("scale", null(own.scale)),
                ("zeroCount", null(own.zero_count)),
                ("zeroThreshold", null(own.zero_threshold)),
                ("positive", null(own.positive)),
                ("negative", null(own.negative)),
                ("quantileValues", null(own.quantile_values)),
                ("exemplars", exemplars),
            ]
        })
        .collect()
}

// An export as protobuf, written as any writer may: fields in any order, among fields the receiver does not know, a
// scalar given more than once, a zero written or left out, repeated numbers packed or not.

const VARINT: u8 = 0;
const I64: u8 = 1;
const LEN: u8 = 2;
const I32: u8 = 5;

fn varint(out: &mut Vec<u8>, mut n: u64) {
    while n >= 0x80 {
        out.push(n as u8 | 0x80);
        n >>= 7;
    }
    out.push(n as u8);
}

fn key(tag: u32, wire: u8) -> Vec<u8> {
    let mut out = Vec::new();
    varint(&mut out, u64::from(tag) << 3 | u64::from(wire));
    out
}

fn delimited(bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    varint(&mut out, bytes.len() as u64);
    out.extend_from_slice(bytes);
    out
}

/// A message being written: each field's bytes, by the group whose order among themselves they keep. A field's group
/// is its tag; a oneof's members share one, so the member that stands is written last.
#[derive(Default)]
struct Msg(Vec<(u32, Vec<u8>)>);

/// The group of the oneof whose first member is `tag`.
fn oneof(tag: u32) -> u32 {
    10_000 + tag
}

impl Msg {
    fn push(&mut self, group: u32, tag: u32, wire: u8, value: &[u8]) {
        let mut field = key(tag, wire);
        field.extend_from_slice(value);
        self.0.push((group, field));
    }

    fn message(&mut self, tag: u32, message: &[u8]) {
        self.push(tag, tag, LEN, &delimited(message));
    }

    /// A singular scalar: left out at its zero value or written, and now and then given first as `decoy`, which the
    /// last value given replaces.
    fn scalar(&mut self, g: &mut Gen, tag: u32, wire: u8, value: &[u8], zero: bool, decoy: &[u8]) {
        if zero && g.chance(2) {
            return;
        }
        if zero {
            g.label("protobuf: a zero written");
        }
        // The collector refuses an ID of another length even where a later one replaces it.
        if !g.collected && g.chance(10) {
            g.label("protobuf: a scalar given twice, the last standing");
            self.push(tag, tag, wire, decoy);
        }
        self.push(tag, tag, wire, value);
    }

    fn uint(&mut self, g: &mut Gen, tag: u32, n: u64) {
        let mut value = Vec::new();
        varint(&mut value, n);
        self.scalar(g, tag, VARINT, &value, n == 0, &[7]);
    }

    fn int32(&mut self, g: &mut Gen, tag: u32, n: i32) {
        self.uint(g, tag, n as i64 as u64);
    }

    fn sint32(&mut self, g: &mut Gen, tag: u32, n: i32) {
        self.uint(g, tag, ((n << 1) ^ (n >> 31)) as u32 as u64);
    }

    fn fixed64(&mut self, g: &mut Gen, tag: u32, n: u64) {
        self.scalar(g, tag, I64, &n.to_le_bytes(), n == 0, &7u64.to_le_bytes());
    }

    fn fixed32(&mut self, g: &mut Gen, tag: u32, n: u32) {
        self.scalar(g, tag, I32, &n.to_le_bytes(), n == 0, &7u32.to_le_bytes());
    }

    fn double(&mut self, g: &mut Gen, tag: u32, d: f64) {
        self.scalar(g, tag, I64, &d.to_le_bytes(), d.to_bits() == 0, &7f64.to_le_bytes());
    }

    fn bytes(&mut self, g: &mut Gen, tag: u32, bytes: &[u8]) {
        self.scalar(g, tag, LEN, &delimited(bytes), bytes.is_empty(), &delimited(b"decoy"));
    }

    /// A field with presence: written whenever it is set.
    fn optional_double(&mut self, tag: u32, d: Option<f64>) {
        if let Some(d) = d {
            self.push(tag, tag, I64, &d.to_le_bytes());
        }
    }

    /// Repeated numbers, each already written as `wire` writes it: packed in one field, each in its own, or in runs
    /// of either.
    fn numbers(&mut self, g: &mut Gen, tag: u32, wire: u8, numbers: &[Vec<u8>]) {
        if numbers.is_empty() {
            if g.chance(6) {
                g.label("protobuf: an empty packed run");
                self.push(tag, tag, LEN, &delimited(&[]));
            }
            return;
        }
        let mut rest = numbers;
        while !rest.is_empty() {
            // The collector keeps only the last of several runs.
            let run = match g.collected || g.chance(3) {
                true => rest.len(),
                false => 1 + g.below(rest.len()),
            };
            let (now, later) = rest.split_at(run);
            if g.chance(2) {
                g.label("protobuf: numbers packed");
                self.push(tag, tag, LEN, &delimited(&now.concat()));
            } else {
                g.label("protobuf: numbers unpacked");
                for n in now {
                    self.push(tag, tag, wire, n);
                }
            }
            rest = later;
        }
    }

    /// The message's bytes: its groups in order or shuffled, each keeping its own order, with fields no reader
    /// knows among them now and then.
    fn finish(self, g: &mut Gen) -> Vec<u8> {
        let mut groups: Vec<(u32, std::collections::VecDeque<Vec<u8>>)> = Vec::new();
        for (group, field) in self.0 {
            match groups.iter_mut().find(|(g, _)| *g == group) {
                Some((_, fields)) => fields.push_back(field),
                None => groups.push((group, [field].into())),
            }
        }
        if g.chance(8) {
            g.label("protobuf: a field no reader knows");
            for i in 0..1 + g.below(3) {
                let tag = 100 + g.below(1000) as u32;
                let mut field = Vec::new();
                match g.below(4) {
                    0 => {
                        field = key(tag, VARINT);
                        varint(&mut field, g.rng.next_u64());
                    }
                    1 => {
                        field = key(tag, I64);
                        field.extend_from_slice(&g.rng.next_u64().to_le_bytes());
                    }
                    2 => {
                        field = key(tag, I32);
                        field.extend_from_slice(&(g.rng.next_u64() as u32).to_le_bytes());
                    }
                    _ => {
                        field.extend(key(tag, LEN));
                        field.extend(delimited(&[0x0a, 0x01, 0x78]));
                    }
                }
                let at = g.below(groups.len() + 1);
                groups.insert(at, (u32::MAX - i as u32, [field].into()));
            }
        }
        let shuffled = g.chance(2);
        let mut out = Vec::new();
        while !groups.is_empty() {
            let at = if shuffled { g.below(groups.len()) } else { 0 };
            out.extend(groups[at].1.pop_front().unwrap());
            if groups[at].1.is_empty() {
                groups.remove(at);
            }
        }
        out
    }
}

fn pb_any(g: &mut Gen, value: &Option<Any>) -> Vec<u8> {
    let mut m = Msg::default();
    if let Some(value) = value {
        let group = oneof(1);
        if g.chance(10) {
            g.label("protobuf: a oneof given twice, the last standing");
            match value {
                Any::Str(_) => m.push(group, 2, VARINT, &[1]),
                _ => m.push(group, 1, LEN, &delimited(b"decoy")),
            }
        }
        match value {
            Any::Str(s) => m.push(group, 1, LEN, &delimited(s.as_bytes())),
            Any::Bool(b) => m.push(group, 2, VARINT, &[u8::from(*b)]),
            Any::Int(i) => {
                let mut v = Vec::new();
                varint(&mut v, *i as u64);
                m.push(group, 3, VARINT, &v);
            }
            Any::Double(d) => m.push(group, 4, I64, &d.to_le_bytes()),
            Any::Array(items) => {
                let mut array = Msg::default();
                for item in items {
                    array.message(1, &pb_any(g, item));
                }
                m.push(group, 5, LEN, &delimited(&array.finish(g)));
            }
            Any::Kvlist(kvs) => {
                let mut list = Msg::default();
                pb_kvs(g, &mut list, 1, kvs);
                m.push(group, 6, LEN, &delimited(&list.finish(g)));
            }
            Any::Bytes(b) => m.push(group, 7, LEN, &delimited(b)),
        }
    }
    m.finish(g)
}

fn pb_kvs(g: &mut Gen, m: &mut Msg, tag: u32, kvs: &[Kv]) {
    for kv in kvs {
        let mut pair = Msg::default();
        pair.bytes(g, 1, kv.key.as_bytes());
        if let Some(value) = &kv.value {
            pair.message(2, &pb_any(g, value));
        }
        m.message(tag, &pair.finish(g));
    }
}

/// `export` as protobuf.
fn pb_export(g: &mut Gen, export: &Export) -> Vec<u8> {
    let mut request = Msg::default();
    for resourced in &export.resources {
        let mut r = Msg::default();
        if let Some(resource) = &resourced.resource {
            let mut m = Msg::default();
            pb_kvs(g, &mut m, 1, &resource.attributes);
            m.uint(g, 2, resource.dropped.into());
            r.message(1, &m.finish(g));
        }
        for scoped in &resourced.scopes {
            let mut s = Msg::default();
            if let Some(scope) = &scoped.scope {
                let mut m = Msg::default();
                m.bytes(g, 1, scope.name.as_bytes());
                m.bytes(g, 2, scope.version.as_bytes());
                pb_kvs(g, &mut m, 3, &scope.attributes);
                m.uint(g, 4, scope.dropped.into());
                s.message(1, &m.finish(g));
            }
            for item in &scoped.items {
                s.message(2, &pb_item(g, item));
            }
            r.message(2, &s.finish(g));
        }
        request.message(1, &r.finish(g));
    }
    request.finish(g)
}

fn pb_item(g: &mut Gen, item: &Item) -> Vec<u8> {
    let mut m = Msg::default();
    match item {
        Item::Log(log) => {
            m.fixed64(g, 1, log.time);
            m.fixed64(g, 11, log.observed);
            m.int32(g, 2, log.severity_number);
            m.bytes(g, 3, log.severity_text.as_bytes());
            if let Some(body) = &log.body {
                m.message(5, &pb_any(g, body));
            }
            pb_kvs(g, &mut m, 6, &log.attributes);
            m.uint(g, 7, log.dropped.into());
            m.fixed32(g, 8, log.flags);
            m.bytes(g, 9, &log.trace_id);
            m.bytes(g, 10, &log.span_id);
            m.bytes(g, 12, log.event_name.as_bytes());
        }
        Item::Span(span) => {
            m.bytes(g, 1, &span.trace_id);
            m.bytes(g, 2, &span.span_id);
            m.bytes(g, 3, span.trace_state.as_bytes());
            m.bytes(g, 4, &span.parent_span_id);
            m.fixed32(g, 16, span.flags);
            m.bytes(g, 5, span.name.as_bytes());
            m.int32(g, 6, span.kind);
            m.fixed64(g, 7, span.start);
            m.fixed64(g, 8, span.end);
            pb_kvs(g, &mut m, 9, &span.attributes);
            m.uint(g, 10, span.dropped_attributes.into());
            for event in &span.events {
                let mut e = Msg::default();
                e.fixed64(g, 1, event.time);
                e.bytes(g, 2, event.name.as_bytes());
                pb_kvs(g, &mut e, 3, &event.attributes);
                e.uint(g, 4, event.dropped.into());
                m.message(11, &e.finish(g));
            }
            m.uint(g, 12, span.dropped_events.into());
            for link in &span.links {
                let mut l = Msg::default();
                l.bytes(g, 1, &link.trace_id);
                l.bytes(g, 2, &link.span_id);
                l.bytes(g, 3, link.trace_state.as_bytes());
                pb_kvs(g, &mut l, 4, &link.attributes);
                l.uint(g, 5, link.dropped.into());
                l.fixed32(g, 6, link.flags);
                m.message(13, &l.finish(g));
            }
            m.uint(g, 14, span.dropped_links.into());
            if let Some(status) = &span.status {
                let mut s = Msg::default();
                s.bytes(g, 2, status.message.as_bytes());
                s.int32(g, 3, status.code);
                m.message(15, &s.finish(g));
            }
        }
        Item::Metric(metric) => {
            m.bytes(g, 1, metric.name.as_bytes());
            m.bytes(g, 2, metric.description.as_bytes());
            m.bytes(g, 3, metric.unit.as_bytes());
            if let Some(data) = &metric.data {
                let (tag, data) = pb_data(g, data);
                let group = oneof(5);
                if g.chance(10) {
                    g.label("protobuf: a metric's data given twice, the last standing");
                    let decoy = if tag == 5 { 11 } else { 5 };
                    m.push(group, decoy, LEN, &delimited(&[]));
                }
                m.push(group, tag, LEN, &delimited(&data));
            }
            pb_kvs(g, &mut m, 12, &metric.metadata);
        }
    }
    m.finish(g)
}

/// A metric's data, and the tag of its member of the oneof.
fn pb_data(g: &mut Gen, data: &Data) -> (u32, Vec<u8>) {
    let mut m = Msg::default();
    let tag = match data {
        Data::Gauge(points) => {
            points.iter().for_each(|p| m.message(1, &pb_number_point(g, p)));
            5
        }
        Data::Sum {
            points,
            temporality,
            monotonic,
        } => {
            points.iter().for_each(|p| m.message(1, &pb_number_point(g, p)));
            m.int32(g, 2, *temporality);
            m.uint(g, 3, u64::from(*monotonic));
            7
        }
        Data::Histogram { points, temporality } => {
            for p in points {
                let mut d = Msg::default();
                pb_kvs(g, &mut d, 9, &p.base.attributes);
                d.fixed64(g, 2, p.base.start);
                d.fixed64(g, 3, p.base.time);
                d.fixed64(g, 4, p.count);
                d.optional_double(5, p.sum);
                let counts: Vec<Vec<u8>> = p.bucket_counts.iter().map(|n| n.to_le_bytes().to_vec()).collect();
                d.numbers(g, 6, I64, &counts);
                let bounds: Vec<Vec<u8>> = p.bounds.iter().map(|b| b.to_le_bytes().to_vec()).collect();
                d.numbers(g, 7, I64, &bounds);
                p.exemplars.iter().for_each(|e| d.message(8, &pb_exemplar(g, e)));
                d.uint(g, 10, p.base.flags.into());
                d.optional_double(11, p.min);
                d.optional_double(12, p.max);
                m.message(1, &d.finish(g));
            }
            m.int32(g, 2, *temporality);
            9
        }
        Data::Exponential { points, temporality } => {
            for p in points {
                let mut d = Msg::default();
                pb_kvs(g, &mut d, 1, &p.base.attributes);
                d.fixed64(g, 2, p.base.start);
                d.fixed64(g, 3, p.base.time);
                d.fixed64(g, 4, p.count);
                d.optional_double(5, p.sum);
                d.sint32(g, 6, p.scale);
                d.fixed64(g, 7, p.zero_count);
                for (tag, buckets) in [(8, &p.positive), (9, &p.negative)] {
                    if let Some(b) = buckets {
                        let mut bm = Msg::default();
                        bm.sint32(g, 1, b.offset);
                        let counts: Vec<Vec<u8>> = (b.counts.iter())
                            .map(|n| {
                                let mut v = Vec::new();
                                varint(&mut v, *n);
                                v
                            })
                            .collect();
                        bm.numbers(g, 2, VARINT, &counts);
                        d.message(tag, &bm.finish(g));
                    }
                }
                d.uint(g, 10, p.base.flags.into());
                p.exemplars.iter().for_each(|e| d.message(11, &pb_exemplar(g, e)));
                d.optional_double(12, p.min);
                d.optional_double(13, p.max);
                d.double(g, 14, p.zero_threshold);
                m.message(1, &d.finish(g));
            }
            m.int32(g, 2, *temporality);
            10
        }
        Data::Summary(points) => {
            for p in points {
                let mut d = Msg::default();
                pb_kvs(g, &mut d, 7, &p.base.attributes);
                d.fixed64(g, 2, p.base.start);
                d.fixed64(g, 3, p.base.time);
                d.fixed64(g, 4, p.count);
                d.double(g, 5, p.sum);
                for (quantile, value) in &p.quantiles {
                    let mut q = Msg::default();
                    q.double(g, 1, *quantile);
                    q.double(g, 2, *value);
                    d.message(6, &q.finish(g));
                }
                d.uint(g, 8, p.base.flags.into());
                m.message(1, &d.finish(g));
            }
            11
        }
    };
    (tag, m.finish(g))
}

/// A number's member of a oneof whose double is at `double` and int at `int`.
fn pb_num(g: &mut Gen, m: &mut Msg, double: u32, int: u32, value: Option<Num>) {
    let group = oneof(double);
    let Some(value) = value else { return };
    if g.chance(10) {
        g.label("protobuf: a number given as both kinds, the last standing");
        match value {
            Num::Double(_) => m.push(group, int, I64, &7i64.to_le_bytes()),
            Num::Int(_) => m.push(group, double, I64, &7f64.to_le_bytes()),
        }
    }
    match value {
        Num::Double(d) => m.push(group, double, I64, &d.to_le_bytes()),
        Num::Int(i) => m.push(group, int, I64, &i.to_le_bytes()),
    }
}

fn pb_number_point(g: &mut Gen, p: &NumberPoint) -> Vec<u8> {
    let mut d = Msg::default();
    pb_kvs(g, &mut d, 7, &p.base.attributes);
    d.fixed64(g, 2, p.base.start);
    d.fixed64(g, 3, p.base.time);
    pb_num(g, &mut d, 4, 6, p.value);
    p.exemplars.iter().for_each(|e| d.message(5, &pb_exemplar(g, e)));
    d.uint(g, 8, p.base.flags.into());
    d.finish(g)
}

fn pb_exemplar(g: &mut Gen, e: &Exemplar) -> Vec<u8> {
    let mut m = Msg::default();
    pb_kvs(g, &mut m, 7, &e.attributes);
    m.fixed64(g, 2, e.time);
    pb_num(g, &mut m, 3, 6, e.value);
    m.bytes(g, 4, &e.span_id);
    m.bytes(g, 5, &e.trace_id);
    m.finish(g)
}

// An export as OTLP/JSON, written as any writer may: each field under either of its names, a zero written, left out
// or null, integers as numbers or text, enums as numbers or names, IDs in either case, bytes in either base64
// alphabet, padded or not, and keys no reader knows.

/// A JSON object being written.
#[derive(Default)]
struct Jo(Map<String, Value>);

impl Jo {
    fn name<'a>(g: &mut Gen, camel: &'a str, snake: &'a str) -> &'a str {
        match camel != snake && g.chance(3) {
            true => {
                g.label("json: a field under its protobuf name");
                snake
            }
            false => camel,
        }
    }

    /// A field with presence, written whenever it is set.
    fn set(&mut self, g: &mut Gen, [camel, snake]: [&str; 2], value: Value) {
        self.0.insert(Jo::name(g, camel, snake).to_string(), value);
    }

    /// A field written as `value`, or at its zero value left out or written as null now and then.
    fn put(&mut self, g: &mut Gen, names: [&str; 2], value: Value, zero: bool) {
        if zero {
            match g.below(3) {
                0 => return,
                1 => {
                    g.label("json: a zero given as null");
                    return self.set(g, names, Value::Null);
                }
                _ => g.label("json: a zero written"),
            }
        }
        self.set(g, names, value);
    }

    fn text(&mut self, g: &mut Gen, names: [&str; 2], text: &str) {
        self.put(g, names, text.into(), text.is_empty());
    }

    fn list(&mut self, g: &mut Gen, names: [&str; 2], items: Vec<Value>) {
        let empty = items.is_empty();
        self.put(g, names, Value::Array(items), empty);
    }

    fn finish(mut self, g: &mut Gen) -> Value {
        if g.chance(10) {
            g.label("json: a key no reader knows");
            let unknown = json!({"x": [1, "two", {"three": null}]});
            self.0.insert(format!("unknownField{}", g.below(3)), unknown);
        }
        Value::Object(self.0)
    }
}

fn j_uint(g: &mut Gen, n: u64) -> Value {
    match g.chance(2) {
        true => {
            g.label("json: an integer as text");
            n.to_string().into()
        }
        false => n.into(),
    }
}

fn j_int(g: &mut Gen, n: i64) -> Value {
    match g.chance(2) {
        true => {
            g.label("json: an integer as text");
            n.to_string().into()
        }
        false => n.into(),
    }
}

fn j_double(g: &mut Gen, d: f64) -> Value {
    if d.is_nan() {
        return "NaN".into();
    }
    if d.is_infinite() {
        g.label("json: an infinite double");
        return if d > 0.0 { "Infinity" } else { "-Infinity" }.into();
    }
    match g.below(4) {
        0 => {
            g.label("json: a double as text");
            format!("{d:?}").into()
        }
        1 if d.fract() == 0.0 && d.abs() < 1e15 && d.is_sign_positive() => {
            g.label("json: a whole double as an integer");
            (d as i64).into()
        }
        _ => d.into(),
    }
}

/// An enum: its number, or now and then its name where it has one.
fn j_enum(g: &mut Gen, n: i32, names: &[&str]) -> Value {
    match usize::try_from(n).ok().and_then(|i| names.get(i)) {
        Some(name) if g.chance(2) => {
            g.label("json: an enum by name");
            (*name).into()
        }
        _ => j_int(g, n.into()),
    }
}

const SPAN_KINDS: &[&str] = &[
    "SPAN_KIND_UNSPECIFIED",
    "SPAN_KIND_INTERNAL",
    "SPAN_KIND_SERVER",
    "SPAN_KIND_CLIENT",
    "SPAN_KIND_PRODUCER",
    "SPAN_KIND_CONSUMER",
];
const STATUS_CODES: &[&str] = &["STATUS_CODE_UNSET", "STATUS_CODE_OK", "STATUS_CODE_ERROR"];
const TEMPORALITIES: &[&str] = &[
    "AGGREGATION_TEMPORALITY_UNSPECIFIED",
    "AGGREGATION_TEMPORALITY_DELTA",
    "AGGREGATION_TEMPORALITY_CUMULATIVE",
];

fn j_id(g: &mut Gen, id: &[u8]) -> Value {
    let hex: String = id.iter().map(|b| format!("{b:02x}")).collect();
    match g.chance(4) {
        true => {
            g.label("json: an ID in upper-case hex");
            hex.to_uppercase().into()
        }
        false => hex.into(),
    }
}

fn j_bytes(g: &mut Gen, bytes: &[u8]) -> Value {
    use base64::Engine as _;
    use base64::engine::general_purpose::{STANDARD, STANDARD_NO_PAD, URL_SAFE, URL_SAFE_NO_PAD};
    let (label, engine) = *g.pick(&[
        ("standard", &STANDARD),
        ("standard unpadded", &STANDARD_NO_PAD),
        ("URL-safe", &URL_SAFE),
        ("URL-safe unpadded", &URL_SAFE_NO_PAD),
    ]);
    g.label(format!("json: bytes in {label} base64"));
    engine.encode(bytes).into()
}

fn j_any(g: &mut Gen, value: &Option<Any>) -> Value {
    let mut o = Jo::default();
    match value {
        None => {}
        Some(Any::Str(s)) => o.set(g, ["stringValue", "string_value"], s.as_str().into()),
        Some(Any::Bool(b)) => o.set(g, ["boolValue", "bool_value"], (*b).into()),
        Some(Any::Int(i)) => {
            let i = j_int(g, *i);
            o.set(g, ["intValue", "int_value"], i);
        }
        Some(Any::Double(d)) => {
            let d = j_double(g, *d);
            o.set(g, ["doubleValue", "double_value"], d);
        }
        Some(Any::Bytes(b)) => {
            let b = j_bytes(g, b);
            o.set(g, ["bytesValue", "bytes_value"], b);
        }
        Some(Any::Array(items)) => {
            let mut array = Jo::default();
            let items = items.iter().map(|item| j_any(g, item)).collect();
            array.list(g, ["values", "values"], items);
            let array = array.finish(g);
            o.set(g, ["arrayValue", "array_value"], array);
        }
        Some(Any::Kvlist(kvs)) => {
            let mut list = Jo::default();
            let pairs = j_kvs(g, kvs);
            list.list(g, ["values", "values"], pairs);
            let list = list.finish(g);
            o.set(g, ["kvlistValue", "kvlist_value"], list);
        }
    }
    o.finish(g)
}

fn j_kvs(g: &mut Gen, kvs: &[Kv]) -> Vec<Value> {
    (kvs.iter())
        .map(|kv| {
            let mut o = Jo::default();
            o.text(g, ["key", "key"], &kv.key);
            match &kv.value {
                Some(value) => {
                    let value = j_any(g, value);
                    o.set(g, ["value", "value"], value);
                }
                None if g.chance(2) => o.set(g, ["value", "value"], Value::Null),
                None => {}
            }
            o.finish(g)
        })
        .collect()
}

fn j_attributes(g: &mut Gen, o: &mut Jo, names: [&str; 2], kvs: &[Kv]) {
    let pairs = j_kvs(g, kvs);
    o.list(g, names, pairs);
}

fn j_u32(g: &mut Gen, o: &mut Jo, names: [&str; 2], n: u32) {
    let value = j_uint(g, n.into());
    o.put(g, names, value, n == 0);
}

fn j_i32(g: &mut Gen, o: &mut Jo, names: [&str; 2], n: i32) {
    let value = j_int(g, n.into());
    o.put(g, names, value, n == 0);
}

fn j_u64(g: &mut Gen, o: &mut Jo, names: [&str; 2], n: u64) {
    let value = j_uint(g, n);
    o.put(g, names, value, n == 0);
}

fn j_f64(g: &mut Gen, o: &mut Jo, names: [&str; 2], d: f64) {
    let value = j_double(g, d);
    o.put(g, names, value, d.to_bits() == 0);
}

fn j_optional_f64(g: &mut Gen, o: &mut Jo, names: [&str; 2], d: Option<f64>) {
    match d {
        Some(d) => {
            let value = j_double(g, d);
            o.set(g, names, value);
        }
        None if g.chance(3) => o.set(g, names, Value::Null),
        None => {}
    }
}

fn j_enum_field(g: &mut Gen, o: &mut Jo, names: [&str; 2], n: i32, values: &[&str]) {
    let value = j_enum(g, n, values);
    o.put(g, names, value, n == 0);
}

fn j_id_field(g: &mut Gen, o: &mut Jo, names: [&str; 2], id: &[u8]) {
    let value = j_id(g, id);
    o.put(g, names, value, id.is_empty());
}

/// The names of a signal's list of resources in its request.
fn resources_names(signal: Signal) -> [&'static str; 2] {
    match signal {
        Signal::Logs => ["resourceLogs", "resource_logs"],
        Signal::Metrics => ["resourceMetrics", "resource_metrics"],
        Signal::Traces => ["resourceSpans", "resource_spans"],
    }
}

/// `export` as OTLP/JSON.
fn json_export(g: &mut Gen, export: &Export) -> Value {
    let (scopes_names, items_names) = match export.signal {
        Signal::Logs => (["scopeLogs", "scope_logs"], ["logRecords", "log_records"]),
        Signal::Metrics => (["scopeMetrics", "scope_metrics"], ["metrics", "metrics"]),
        Signal::Traces => (["scopeSpans", "scope_spans"], ["spans", "spans"]),
    };
    let mut resources = Vec::new();
    for resourced in &export.resources {
        let mut r = Jo::default();
        if let Some(resource) = &resourced.resource {
            let mut m = Jo::default();
            j_attributes(g, &mut m, ["attributes", "attributes"], &resource.attributes);
            j_u32(
                g,
                &mut m,
                ["droppedAttributesCount", "dropped_attributes_count"],
                resource.dropped,
            );
            let m = m.finish(g);
            r.set(g, ["resource", "resource"], m);
        }
        let mut scopes = Vec::new();
        for scoped in &resourced.scopes {
            let mut s = Jo::default();
            if let Some(scope) = &scoped.scope {
                let mut m = Jo::default();
                m.text(g, ["name", "name"], &scope.name);
                m.text(g, ["version", "version"], &scope.version);
                j_attributes(g, &mut m, ["attributes", "attributes"], &scope.attributes);
                j_u32(
                    g,
                    &mut m,
                    ["droppedAttributesCount", "dropped_attributes_count"],
                    scope.dropped,
                );
                let m = m.finish(g);
                s.set(g, ["scope", "scope"], m);
            }
            let items = scoped.items.iter().map(|item| json_item(g, item)).collect();
            s.list(g, items_names, items);
            scopes.push(s.finish(g));
        }
        r.list(g, scopes_names, scopes);
        resources.push(r.finish(g));
    }
    let mut request = Jo::default();
    request.list(g, resources_names(export.signal), resources);
    request.finish(g)
}

fn json_item(g: &mut Gen, item: &Item) -> Value {
    let mut o = Jo::default();
    match item {
        Item::Log(log) => {
            j_u64(g, &mut o, ["timeUnixNano", "time_unix_nano"], log.time);
            j_u64(
                g,
                &mut o,
                ["observedTimeUnixNano", "observed_time_unix_nano"],
                log.observed,
            );
            j_i32(g, &mut o, ["severityNumber", "severity_number"], log.severity_number);
            o.text(g, ["severityText", "severity_text"], &log.severity_text);
            match &log.body {
                Some(body) => {
                    let body = j_any(g, body);
                    o.set(g, ["body", "body"], body);
                }
                None if g.chance(3) => o.set(g, ["body", "body"], Value::Null),
                None => {}
            }
            j_attributes(g, &mut o, ["attributes", "attributes"], &log.attributes);
            j_u32(
                g,
                &mut o,
                ["droppedAttributesCount", "dropped_attributes_count"],
                log.dropped,
            );
            j_u32(g, &mut o, ["flags", "flags"], log.flags);
            j_id_field(g, &mut o, ["traceId", "trace_id"], &log.trace_id);
            j_id_field(g, &mut o, ["spanId", "span_id"], &log.span_id);
            o.text(g, ["eventName", "event_name"], &log.event_name);
        }
        Item::Span(span) => {
            j_id_field(g, &mut o, ["traceId", "trace_id"], &span.trace_id);
            j_id_field(g, &mut o, ["spanId", "span_id"], &span.span_id);
            o.text(g, ["traceState", "trace_state"], &span.trace_state);
            j_id_field(g, &mut o, ["parentSpanId", "parent_span_id"], &span.parent_span_id);
            j_u32(g, &mut o, ["flags", "flags"], span.flags);
            o.text(g, ["name", "name"], &span.name);
            j_enum_field(g, &mut o, ["kind", "kind"], span.kind, SPAN_KINDS);
            j_u64(g, &mut o, ["startTimeUnixNano", "start_time_unix_nano"], span.start);
            j_u64(g, &mut o, ["endTimeUnixNano", "end_time_unix_nano"], span.end);
            j_attributes(g, &mut o, ["attributes", "attributes"], &span.attributes);
            let dropped = ["droppedAttributesCount", "dropped_attributes_count"];
            j_u32(g, &mut o, dropped, span.dropped_attributes);
            let events = (span.events.iter())
                .map(|e| {
                    let mut m = Jo::default();
                    j_u64(g, &mut m, ["timeUnixNano", "time_unix_nano"], e.time);
                    m.text(g, ["name", "name"], &e.name);
                    j_attributes(g, &mut m, ["attributes", "attributes"], &e.attributes);
                    j_u32(g, &mut m, dropped, e.dropped);
                    m.finish(g)
                })
                .collect();
            o.list(g, ["events", "events"], events);
            j_u32(
                g,
                &mut o,
                ["droppedEventsCount", "dropped_events_count"],
                span.dropped_events,
            );
            let links = (span.links.iter())
                .map(|l| {
                    let mut m = Jo::default();
                    j_id_field(g, &mut m, ["traceId", "trace_id"], &l.trace_id);
                    j_id_field(g, &mut m, ["spanId", "span_id"], &l.span_id);
                    m.text(g, ["traceState", "trace_state"], &l.trace_state);
                    j_attributes(g, &mut m, ["attributes", "attributes"], &l.attributes);
                    j_u32(g, &mut m, dropped, l.dropped);
                    j_u32(g, &mut m, ["flags", "flags"], l.flags);
                    m.finish(g)
                })
                .collect();
            o.list(g, ["links", "links"], links);
            j_u32(
                g,
                &mut o,
                ["droppedLinksCount", "dropped_links_count"],
                span.dropped_links,
            );
            if let Some(status) = &span.status {
                let mut m = Jo::default();
                m.text(g, ["message", "message"], &status.message);
                j_enum_field(g, &mut m, ["code", "code"], status.code, STATUS_CODES);
                let m = m.finish(g);
                o.set(g, ["status", "status"], m);
            }
        }
        Item::Metric(metric) => {
            o.text(g, ["name", "name"], &metric.name);
            o.text(g, ["description", "description"], &metric.description);
            o.text(g, ["unit", "unit"], &metric.unit);
            if let Some(data) = &metric.data {
                let (names, data) = json_data(g, data);
                o.set(g, names, data);
            }
            j_attributes(g, &mut o, ["metadata", "metadata"], &metric.metadata);
        }
    }
    o.finish(g)
}

/// A metric's data, under the names of its member of the oneof.
fn json_data(g: &mut Gen, data: &Data) -> ([&'static str; 2], Value) {
    const POINTS: [&str; 2] = ["dataPoints", "data_points"];
    const TEMPORALITY: [&str; 2] = ["aggregationTemporality", "aggregation_temporality"];
    let mut m = Jo::default();
    let names = match data {
        Data::Gauge(points) => {
            let points = points.iter().map(|p| json_number_point(g, p)).collect();
            m.list(g, POINTS, points);
            ["gauge", "gauge"]
        }
        Data::Sum {
            points,
            temporality,
            monotonic,
        } => {
            let points = points.iter().map(|p| json_number_point(g, p)).collect();
            m.list(g, POINTS, points);
            j_enum_field(g, &mut m, TEMPORALITY, *temporality, TEMPORALITIES);
            m.put(g, ["isMonotonic", "is_monotonic"], (*monotonic).into(), !monotonic);
            ["sum", "sum"]
        }
        Data::Histogram { points, temporality } => {
            let points = (points.iter())
                .map(|p| {
                    let mut d = Jo::default();
                    json_base(g, &mut d, &p.base);
                    j_u64(g, &mut d, ["count", "count"], p.count);
                    j_optional_f64(g, &mut d, ["sum", "sum"], p.sum);
                    let counts = p.bucket_counts.iter().map(|n| j_uint(g, *n)).collect();
                    d.list(g, ["bucketCounts", "bucket_counts"], counts);
                    let bounds = p.bounds.iter().map(|b| j_double(g, *b)).collect();
                    d.list(g, ["explicitBounds", "explicit_bounds"], bounds);
                    let exemplars = p.exemplars.iter().map(|e| json_exemplar(g, e)).collect();
                    d.list(g, ["exemplars", "exemplars"], exemplars);
                    j_optional_f64(g, &mut d, ["min", "min"], p.min);
                    j_optional_f64(g, &mut d, ["max", "max"], p.max);
                    d.finish(g)
                })
                .collect();
            m.list(g, POINTS, points);
            j_enum_field(g, &mut m, TEMPORALITY, *temporality, TEMPORALITIES);
            ["histogram", "histogram"]
        }
        Data::Exponential { points, temporality } => {
            let points = (points.iter())
                .map(|p| {
                    let mut d = Jo::default();
                    json_base(g, &mut d, &p.base);
                    j_u64(g, &mut d, ["count", "count"], p.count);
                    j_optional_f64(g, &mut d, ["sum", "sum"], p.sum);
                    j_i32(g, &mut d, ["scale", "scale"], p.scale);
                    j_u64(g, &mut d, ["zeroCount", "zero_count"], p.zero_count);
                    for (names, buckets) in [
                        (["positive", "positive"], &p.positive),
                        (["negative", "negative"], &p.negative),
                    ] {
                        if let Some(b) = buckets {
                            let mut bm = Jo::default();
                            j_i32(g, &mut bm, ["offset", "offset"], b.offset);
                            let counts = b.counts.iter().map(|n| j_uint(g, *n)).collect();
                            bm.list(g, ["bucketCounts", "bucket_counts"], counts);
                            let bm = bm.finish(g);
                            d.set(g, names, bm);
                        }
                    }
                    let exemplars = p.exemplars.iter().map(|e| json_exemplar(g, e)).collect();
                    d.list(g, ["exemplars", "exemplars"], exemplars);
                    j_optional_f64(g, &mut d, ["min", "min"], p.min);
                    j_optional_f64(g, &mut d, ["max", "max"], p.max);
                    j_f64(g, &mut d, ["zeroThreshold", "zero_threshold"], p.zero_threshold);
                    d.finish(g)
                })
                .collect();
            m.list(g, POINTS, points);
            j_enum_field(g, &mut m, TEMPORALITY, *temporality, TEMPORALITIES);
            ["exponentialHistogram", "exponential_histogram"]
        }
        Data::Summary(points) => {
            let points = (points.iter())
                .map(|p| {
                    let mut d = Jo::default();
                    json_base(g, &mut d, &p.base);
                    j_u64(g, &mut d, ["count", "count"], p.count);
                    j_f64(g, &mut d, ["sum", "sum"], p.sum);
                    let quantiles = (p.quantiles.iter())
                        .map(|(quantile, value)| {
                            let mut q = Jo::default();
                            j_f64(g, &mut q, ["quantile", "quantile"], *quantile);
                            j_f64(g, &mut q, ["value", "value"], *value);
                            q.finish(g)
                        })
                        .collect();
                    d.list(g, ["quantileValues", "quantile_values"], quantiles);
                    d.finish(g)
                })
                .collect();
            m.list(g, POINTS, points);
            ["summary", "summary"]
        }
    };
    (names, m.finish(g))
}

fn json_base(g: &mut Gen, o: &mut Jo, base: &Base) {
    j_attributes(g, o, ["attributes", "attributes"], &base.attributes);
    j_u64(g, o, ["startTimeUnixNano", "start_time_unix_nano"], base.start);
    j_u64(g, o, ["timeUnixNano", "time_unix_nano"], base.time);
    j_u32(g, o, ["flags", "flags"], base.flags);
}

fn json_num(g: &mut Gen, o: &mut Jo, value: Option<Num>) {
    match value {
        Some(Num::Double(d)) => {
            let d = j_double(g, d);
            o.set(g, ["asDouble", "as_double"], d);
        }
        Some(Num::Int(i)) => {
            let i = j_int(g, i);
            o.set(g, ["asInt", "as_int"], i);
        }
        None => {}
    }
}

fn json_number_point(g: &mut Gen, p: &NumberPoint) -> Value {
    let mut d = Jo::default();
    json_base(g, &mut d, &p.base);
    json_num(g, &mut d, p.value);
    let exemplars = p.exemplars.iter().map(|e| json_exemplar(g, e)).collect();
    d.list(g, ["exemplars", "exemplars"], exemplars);
    d.finish(g)
}

fn json_exemplar(g: &mut Gen, e: &Exemplar) -> Value {
    let mut m = Jo::default();
    j_attributes(g, &mut m, ["filteredAttributes", "filtered_attributes"], &e.attributes);
    j_u64(g, &mut m, ["timeUnixNano", "time_unix_nano"], e.time);
    json_num(g, &mut m, e.value);
    j_id_field(g, &mut m, ["spanId", "span_id"], &e.span_id);
    j_id_field(g, &mut m, ["traceId", "trace_id"], &e.trace_id);
    m.finish(g)
}

// Requests: an export over OTLP/HTTP or gRPC, the port's health, or what it does not serve, each with what it must
// be answered.

/// What a request must be answered.
#[derive(Clone, Debug, PartialEq)]
enum Expected {
    /// Nothing: TLS refused the connection.
    Nothing,
    /// OTLP/HTTP's answer: this status, its body in this encoding.
    Otlp(u16, Encoding),
    /// gRPC's: HTTP 200 and this `grpc-status`.
    Grpc(i32),
    /// HTTP's own status alone.
    Plain(u16),
}

struct Sent {
    get: bool,
    path: String,
    h2: bool,
    presents: Presents,
    headers: Vec<(&'static str, String)>,
    body: Vec<u8>,
    expected: Expected,
    /// The signal and records of an export answered as delivered.
    records: Option<(Signal, Vec<Got>)>,
    /// The options the receiver reached before it answered.
    labels: Vec<String>,
}

/// Where a request is in the order the receiver reads it: what it has reached, and the answer once one is decided.
#[derive(Default)]
struct Order {
    labels: Vec<String>,
    decided: Option<Expected>,
}

impl Order {
    fn reach(&mut self, label: impl Into<String>) {
        if self.decided.is_none() {
            self.labels.push(label.into());
        }
    }

    fn decide(&mut self, when: bool, expected: Expected) {
        if when && self.decided.is_none() {
            self.decided = Some(expected);
        }
    }
}

const PROTOBUF_TYPES: &[&str] = &[
    "application/x-protobuf",
    "Application/X-Protobuf",
    "application/x-protobuf; v=1",
];
const JSON_TYPES: &[&str] = &["application/json", "application/JSON; charset=utf-8"];
const UNSUPPORTED_TYPES: &[Option<&str>] = &[None, Some("text/plain"), Some("application/protobuf")];
const GRPC_TYPES: &[&str] = &[
    "application/grpc",
    "application/grpc+proto",
    "Application/GRPC",
    "application/grpc; v=1",
];
const NOT_GRPC_TYPES: &[Option<&str>] = &[None, Some("application/x-protobuf"), Some("application/grpc+json")];

/// Each `Content-Encoding`, and whether it is gzip, or `None` where OTLP/HTTP does not take it.
const CONTENT_ENCODINGS: &[(Option<&str>, Option<bool>)] = &[
    (None, Some(false)),
    (Some("identity"), Some(false)),
    (Some("gzip"), Some(true)),
    (Some("GZIP"), Some(true)),
    (Some("br"), None),
];

/// Each `grpc-encoding`, as `CONTENT_ENCODINGS`.
const GRPC_ENCODINGS: &[(Option<&str>, Option<bool>)] = &[
    (None, Some(false)),
    (Some("identity"), Some(false)),
    (Some("gzip"), Some(true)),
    (Some("snappy"), None),
];

const AUTHORIZATIONS: &[&str] = &[
    "no Authorization",
    "Bearer and its token",
    "bearer in lower case and its token",
    "Bearer and its token among spaces",
    "Bearer and a token it does not hold",
    "Bearer and another receiver's token",
    "Basic and its token",
    "its token alone",
];

const PROTOBUF_SPOILS: &[&str] = &[
    "a field cut short",
    "a varint that never ends",
    "a group",
    "text that is not UTF-8",
    "a known field of the wrong wire type",
    "JSON sent as protobuf",
];

const JSON_SPOILS: &[&str] = &[
    "JSON cut short",
    "a list that is a number",
    "a request that is not an object",
    "protobuf sent as JSON",
];

const FRAMINGS: &[&str] = &[
    "compressed with no grpc-encoding",
    "a prefix of 2",
    "a length past its message",
    "a length short of its message",
    "a body shorter than its prefix",
];

/// An `Authorization` header, and whether it admits a sender to `taking`.
fn authorization(g: &mut Gen, settings: &Settings, taking: Option<&Taking>) -> (Option<String>, bool, &'static str) {
    // Most present the token, so most requests reach their body.
    let label = match g.chance(2) {
        true => AUTHORIZATIONS[1],
        false => *g.pick(AUTHORIZATIONS),
    };
    let others: Vec<&String> = (settings.receivers.iter())
        .filter(|r| taking.is_none_or(|t| t.signal != r.signal))
        .flat_map(|r| &r.tokens)
        .collect();
    let label = match label {
        "Bearer and another receiver's token" if others.is_empty() => "Bearer and a token it does not hold",
        _ => label,
    };
    let its = taking.and_then(|t| (!t.tokens.is_empty()).then(|| g.pick(&t.tokens).clone()));
    let its = its.unwrap_or_else(|| "tok-none".into());
    let (header, right) = match label {
        "no Authorization" => (None, false),
        "Bearer and its token" => (Some(format!("Bearer {its}")), true),
        "bearer in lower case and its token" => (Some(format!("bearer {its}")), true),
        "Bearer and its token among spaces" => (Some(format!("Bearer   {its} ")), true),
        "Bearer and a token it does not hold" => (Some("Bearer not-a-token".into()), false),
        "Bearer and another receiver's token" => (Some(format!("Bearer {}", others[0])), false),
        "Basic and its token" => (Some(format!("Basic {its}")), false),
        _ => (Some(its), false),
    };
    // A receiver that holds no token admits every sender, whatever it presents.
    let label = match taking.is_some_and(|t| t.tokens.is_empty()) {
        true => "a receiver that holds no token",
        false => label,
    };
    (header, taking.is_some_and(|t| t.tokens.is_empty() || right), label)
}

/// `body` gzipped, and whether in two members.
fn gzip(g: &mut Gen, body: &[u8]) -> (Vec<u8>, bool) {
    let member = |part: &[u8]| {
        let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        gz.write_all(part).unwrap();
        gz.finish().unwrap()
    };
    match g.chance(4) {
        true => {
            let (first, second) = body.split_at(g.below(body.len() + 1));
            ([member(first), member(second)].concat(), true)
        }
        false => (member(body), false),
    }
}

/// A random export of `signal`, written in `encoding`, and the options drawing and writing it reached.
fn random_body(g: &mut Gen, signal: Signal, encoding: Encoding) -> (Export, Vec<u8>, Vec<String>) {
    let at = g.labels.len();
    let export = random_export(g, signal);
    let body = written(g, &export, encoding);
    (export, body, g.labels.drain(at..).collect())
}

fn written(g: &mut Gen, export: &Export, encoding: Encoding) -> Vec<u8> {
    match encoding {
        Encoding::Protobuf => pb_export(g, export),
        Encoding::Json => {
            let value = json_export(g, export);
            match g.chance(3) {
                true => serde_json::to_vec_pretty(&value).unwrap(),
                false => serde_json::to_vec(&value).unwrap(),
            }
        }
    }
}

/// `body`, a protobuf export, spoiled as `spoil` names, so that no receiver reads it.
fn spoil_protobuf(g: &mut Gen, spoil: &str, body: &mut Vec<u8>, export: &Export) {
    let at = g.labels.len();
    match spoil {
        "a field cut short" => body.extend([0x0a, 0x05, 0x01]),
        "a varint that never ends" => body.extend([0x08].into_iter().chain([0xff; 11])),
        "a group" => body.extend([0x0b, 0x0c]),
        "text that is not UTF-8" => {
            // A resource of one scope named in bytes that are no text.
            let scope = [0x0a, 0x02, 0xff, 0xfe];
            let scoped = [&[0x0a, scope.len() as u8][..], &scope].concat();
            let resourced = [&[0x12, scoped.len() as u8][..], &scoped].concat();
            body.extend([&[0x0a, resourced.len() as u8][..], &resourced].concat());
        }
        "a known field of the wrong wire type" => body.extend([0x09].into_iter().chain([0; 8])),
        _ => *body = written(g, export, Encoding::Json),
    }
    // The receiver reads none of what the spoiled body was written with.
    g.labels.truncate(at);
}

fn spoil_json(g: &mut Gen, spoil: &str, body: &mut Vec<u8>, export: &Export) {
    let at = g.labels.len();
    match spoil {
        "JSON cut short" => body.truncate(g.below(body.len())),
        "a list that is a number" => {
            *body = format!("{{\"{}\": 5}}", resources_names(export.signal)[0]).into_bytes();
        }
        "a request that is not an object" => *body = b"[]".to_vec(),
        _ => *body = written(g, export, Encoding::Protobuf),
    }
    g.labels.truncate(at);
}

fn random_request(g: &mut Gen, settings: &Settings) -> Sent {
    let mut order = Order::default();
    let presents = match settings.tls {
        Tls::None => Presents::Nothing,
        _ => *g.pick(&[Presents::Nothing, Presents::Trusted, Presents::Stranger]),
    };
    order.reach(format!("presents {presents:?} under {:?}", settings.tls));
    order.decide(
        settings.tls == Tls::Vouched && presents != Presents::Trusted,
        Expected::Nothing,
    );
    let mut sent = Sent {
        get: false,
        path: String::new(),
        h2: g.chance(2),
        presents,
        headers: Vec::new(),
        body: Vec::new(),
        expected: Expected::Nothing,
        records: None,
        labels: Vec::new(),
    };
    let remote = settings.connect.ip().to_string();
    // gRPC is HTTP/2's alone.
    let route = g.below(12);
    sent.h2 |= (2..=4).contains(&route);
    order.reach(format!("over HTTP/{}", if sent.h2 { 2 } else { 1 }));
    match route {
        0 => {
            order.reach("GET /health");
            order.decide(true, Expected::Plain(200));
            (sent.get, sent.path) = (true, "/health".into());
        }
        1 => {
            let signal = *g.pick(&SIGNALS);
            match g.chance(2) {
                true => {
                    order.reach("GET on an export's path");
                    order.decide(true, Expected::Plain(405));
                    (sent.get, sent.path) = (true, smeltr::io::otlp::http_path(signal).into());
                }
                false => {
                    order.reach("a path nothing serves");
                    order.decide(true, Expected::Plain(404));
                    sent.path = "/v1/profiles".into();
                }
            }
        }
        2..=4 => grpc_request(g, settings, &mut order, &mut sent, &remote),
        _ => http_request(g, settings, &mut order, &mut sent, &remote),
    }
    sent.expected = order.decided.expect("every request is answered");
    sent.labels = order.labels;
    if !matches!(sent.expected, Expected::Otlp(200, _) | Expected::Grpc(0)) {
        sent.records = None;
    }
    sent
}

/// A signal, most often one a receiver on the port takes.
fn random_signal(g: &mut Gen, settings: &Settings) -> Signal {
    match g.chance(4) {
        true => *g.pick(&SIGNALS),
        false => g.pick(&settings.receivers).signal,
    }
}

fn http_request(g: &mut Gen, settings: &Settings, order: &mut Order, sent: &mut Sent, remote: &str) {
    let signal = random_signal(g, settings);
    let taking = settings.taking(signal);
    sent.path = smeltr::io::otlp::http_path(signal).into();
    let encoding = *g.pick(&[Encoding::Protobuf, Encoding::Json]);
    let (export, mut body, content) = random_body(g, signal, encoding);
    let spoil = g.chance(5).then(|| match encoding {
        Encoding::Protobuf => *g.pick(PROTOBUF_SPOILS),
        Encoding::Json => *g.pick(JSON_SPOILS),
    });
    if let Some(spoil) = spoil {
        match encoding {
            Encoding::Protobuf => spoil_protobuf(g, spoil, &mut body, &export),
            Encoding::Json => spoil_json(g, spoil, &mut body, &export),
        }
    }
    let (content_type, named) = match g.chance(10) {
        true => (*g.pick(UNSUPPORTED_TYPES), None),
        false => match encoding {
            Encoding::Protobuf => (Some(*g.pick(PROTOBUF_TYPES)), Some(encoding)),
            Encoding::Json => (Some(*g.pick(JSON_TYPES)), Some(encoding)),
        },
    };
    let answered_in = named.unwrap_or(Encoding::Protobuf);
    let (content_encoding, gzipped) = *g.pick(CONTENT_ENCODINGS);
    let inflated = body.len();
    let not_gzip = gzipped == Some(true) && g.chance(10);
    let mut members = false;
    if gzipped == Some(true) && !not_gzip {
        (body, members) = gzip(g, &body);
    }
    let (authorization, admitted, auth_label) = authorization(g, settings, taking);
    let limit = taking.map_or(usize::MAX, |t| t.max_request_bytes);

    order.reach(format!("{} over HTTP", signal.name()));
    order.reach(match taking {
        Some(_) => "an export of a signal the port takes",
        None => "an export of a signal no receiver takes",
    });
    order.decide(taking.is_none(), Expected::Otlp(404, answered_in));
    order.reach(auth_label);
    order.decide(!admitted, Expected::Otlp(401, answered_in));
    order.reach(format!("Content-Type {content_type:?}"));
    order.decide(named.is_none(), Expected::Otlp(415, Encoding::Protobuf));
    order.reach(format!("{encoding:?} over HTTP"));
    if body.len() > limit {
        order.reach("a body past its limit");
        order.decide(true, Expected::Otlp(413, answered_in));
    }
    order.reach(format!("Content-Encoding {content_encoding:?}"));
    order.decide(gzipped.is_none(), Expected::Otlp(415, answered_in));
    if not_gzip {
        order.reach("a body that is not the gzip it says");
        order.decide(true, Expected::Otlp(400, answered_in));
    }
    if members {
        order.reach("gzip in two members");
    }
    if gzipped == Some(true) && inflated > limit {
        order.reach("a body that inflates past its limit");
        order.decide(true, Expected::Otlp(413, answered_in));
    }
    if let Some(spoil) = spoil {
        order.reach(spoil);
        order.decide(true, Expected::Otlp(400, answered_in));
    }
    // What the export holds counts only where its records are read back.
    content.into_iter().for_each(|label| order.reach(label));
    order.decide(true, Expected::Otlp(200, answered_in));
    sent.records = Some((signal, records_of(&export, remote)));
    sent.headers
        .extend(content_type.map(|t| ("content-type", t.to_string())));
    sent.headers
        .extend(content_encoding.map(|e| ("content-encoding", e.to_string())));
    sent.headers.extend(authorization.map(|a| ("authorization", a)));
    sent.body = body;
}

fn grpc_request(g: &mut Gen, settings: &Settings, order: &mut Order, sent: &mut Sent, remote: &str) {
    let signal = random_signal(g, settings);
    let taking = settings.taking(signal);
    sent.path = smeltr::io::otlp::grpc_path(signal).into();
    let (export, mut message, content) = random_body(g, signal, Encoding::Protobuf);
    let spoil = g.chance(5).then(|| *g.pick(PROTOBUF_SPOILS));
    if let Some(spoil) = spoil {
        spoil_protobuf(g, spoil, &mut message, &export);
    }
    let content_type = match g.chance(10) {
        true => *g.pick(NOT_GRPC_TYPES),
        false => Some(*g.pick(GRPC_TYPES)),
    };
    let grpc = content_type.is_some_and(|t| GRPC_TYPES.contains(&t));
    let (grpc_encoding, gzip_named) = *g.pick(GRPC_ENCODINGS);
    let inflated = message.len();
    let framing = (g.chance(5).then(|| *g.pick(FRAMINGS)))
        .filter(|f| *f != "compressed with no grpc-encoding" || gzip_named != Some(true));
    let compressed = match framing {
        Some("compressed with no grpc-encoding") => true,
        _ => gzip_named == Some(true) && !g.chance(3),
    };
    let not_gzip = compressed && g.chance(10);
    let mut members = false;
    if compressed && !not_gzip {
        (message, members) = gzip(g, &message);
    }
    let length = match framing {
        Some("a length past its message") => match g.chance(2) {
            true => message.len() + 1 + g.below(10),
            false => 1 << 20,
        },
        Some("a length short of its message") if !message.is_empty() => message.len() - 1 - g.below(message.len()),
        Some("a length short of its message") => {
            message.push(0);
            0
        }
        _ => message.len(),
    };
    let prefix = match framing {
        Some("a prefix of 2") => 2,
        _ => u8::from(compressed),
    };
    let mut body = vec![prefix];
    body.extend((length as u32).to_be_bytes());
    body.extend(&message);
    if framing == Some("a body shorter than its prefix") {
        body.truncate(3);
    }
    let (authorization, admitted, auth_label) = authorization(g, settings, taking);
    let limit = taking.map_or(usize::MAX, |t| t.max_request_bytes);

    order.reach(format!("{} over gRPC", signal.name()));
    order.reach(format!("gRPC Content-Type {content_type:?}"));
    order.decide(!grpc, Expected::Plain(415));
    order.reach(match taking {
        Some(_) => "an export of a signal the port takes",
        None => "an export of a signal no receiver takes",
    });
    order.decide(taking.is_none(), Expected::Grpc(12));
    order.reach(auth_label);
    order.decide(!admitted, Expected::Grpc(16));
    if body.len() > limit {
        order.reach("a body past its limit");
        order.decide(true, Expected::Grpc(3));
    }
    order.reach(format!("grpc-encoding {grpc_encoding:?}"));
    order.decide(gzip_named.is_none(), Expected::Grpc(12));
    // Its prefix is read whole first, then its length held to the limit, then its message read to that length.
    let lengths = ["a length past its message", "a length short of its message"];
    if let Some(framing) = framing.filter(|f| !lengths.contains(f)) {
        order.reach(framing);
        order.decide(true, Expected::Grpc(3));
    }
    if gzip_named == Some(true) && !compressed {
        order.reach("a plain message under grpc-encoding gzip");
    }
    if length > limit {
        order.reach("a message past its limit");
        order.decide(true, Expected::Grpc(3));
    }
    if let Some(framing) = framing.filter(|f| lengths.contains(f)) {
        order.reach(framing);
        order.decide(true, Expected::Grpc(3));
    }
    if not_gzip {
        order.reach("a body that is not the gzip it says");
        order.decide(true, Expected::Grpc(3));
    }
    if members {
        order.reach("gzip in two members");
    }
    if compressed && inflated > limit {
        order.reach("a body that inflates past its limit");
        order.decide(true, Expected::Grpc(3));
    }
    if let Some(spoil) = spoil {
        order.reach(spoil);
        order.decide(true, Expected::Grpc(3));
    }
    // What the export holds counts only where its records are read back.
    content.into_iter().for_each(|label| order.reach(label));
    order.decide(true, Expected::Grpc(0));
    sent.records = Some((signal, records_of(&export, remote)));
    sent.headers
        .extend(content_type.map(|t| ("content-type", t.to_string())));
    sent.headers
        .extend(grpc_encoding.map(|e| ("grpc-encoding", e.to_string())));
    sent.headers.extend(authorization.map(|a| ("authorization", a)));
    sent.headers.push(("te", "trailers".into()));
    sent.body = body;
}

/// What a request was answered: its HTTP status, 0 for no answer, its headers, its body and its trailers, or why
/// there was no answer.
struct Answer {
    status: u16,
    headers: reqwest::header::HeaderMap,
    body: Vec<u8>,
    trailers: Option<reqwest::header::HeaderMap>,
    error: String,
}

async fn exchange(settings: &Settings, pki: &Pki, sent: &Sent) -> Answer {
    use http_body::Body as _;

    let mut builder = (reqwest::Client::builder())
        .no_proxy()
        .timeout(Duration::from_secs(30))
        .resolve("localhost", settings.connect);
    builder = match sent.h2 {
        true => builder.http2_prior_knowledge(),
        false => builder.http1_only(),
    };
    let scheme = match settings.tls {
        Tls::None => "http",
        _ => {
            let alpn: &[u8] = if sent.h2 { b"h2" } else { b"http/1.1" };
            builder = builder.use_preconfigured_tls(pki.client_speaking(sent.presents, alpn));
            "https"
        }
    };
    let client = builder.build().unwrap();
    let url = format!("{scheme}://localhost:{}{}", settings.connect.port(), sent.path);
    let mut request = match sent.get {
        true => client.get(url),
        false => client.post(url).body(sent.body.clone()),
    };
    for (name, value) in &sent.headers {
        request = request.header(*name, value);
    }
    let failed = |error: String| Answer {
        status: 0,
        headers: Default::default(),
        body: Vec::new(),
        trailers: None,
        error,
    };
    let answer = match request.send().await {
        Ok(answer) => http::Response::<reqwest::Body>::from(answer),
        Err(e) => return failed(format!("{e:?}")),
    };
    let (parts, mut body) = answer.into_parts();
    let (mut data, mut trailers) = (Vec::new(), None);
    while let Some(frame) = std::future::poll_fn(|cx| std::pin::Pin::new(&mut body).poll_frame(cx)).await {
        match frame {
            Ok(frame) => match frame.into_data() {
                Ok(bytes) => data.extend_from_slice(&bytes),
                Err(frame) => trailers = frame.into_trailers().ok(),
            },
            Err(e) => return failed(format!("the answer broke off: {e:?}")),
        }
    }
    Answer {
        status: parts.status.as_u16(),
        headers: parts.headers,
        body: data,
        trailers,
        error: String::new(),
    }
}

/// `google.rpc.Status`, less its details.
#[derive(Clone, PartialEq, prost::Message)]
struct RpcStatus {
    #[prost(int32, tag = "1")]
    code: i32,
    #[prost(string, tag = "2")]
    message: String,
}

/// Whether `answer` is the one `sent` must have, or why not.
fn check_answer(sent: &Sent, answer: &Answer) -> Result<(), String> {
    let header = |name: &str| answer.headers.get(name).map(|v| v.to_str().unwrap_or("?").to_string());
    let status = |expected: u16| match answer.status == expected {
        true => Ok(()),
        false => Err(format!(
            "answered {}, not {expected}: {:?} {}",
            answer.status,
            String::from_utf8_lossy(&answer.body),
            answer.error
        )),
    };
    match &sent.expected {
        Expected::Nothing => status(0),
        Expected::Plain(expected) => {
            status(*expected)?;
            match header("grpc-status") {
                Some(code) => Err(format!("an answer that is not gRPC's carries grpc-status {code}")),
                None => Ok(()),
            }
        }
        Expected::Otlp(expected, encoding) => {
            status(*expected)?;
            if header("content-type").as_deref() != Some(encoding.content_type()) {
                return Err(format!("answered as {:?}, not {encoding:?}", header("content-type")));
            }
            if *expected == 200 {
                let delivered: &[u8] = match encoding {
                    Encoding::Protobuf => b"",
                    Encoding::Json => b"{}",
                };
                return match answer.body == delivered {
                    true => Ok(()),
                    false => Err(format!("delivered with {:?}", String::from_utf8_lossy(&answer.body))),
                };
            }
            let read = match encoding {
                Encoding::Protobuf => {
                    use prost::Message as _;
                    RpcStatus::decode(answer.body.as_slice()).map_err(|e| e.to_string())
                }
                Encoding::Json => serde_json::from_slice::<Value>(&answer.body)
                    .map_err(|e| e.to_string())
                    .map(|v| RpcStatus {
                        code: v["code"].as_i64().unwrap_or(-1) as i32,
                        message: v["message"].as_str().unwrap_or_default().to_string(),
                    }),
            };
            let read = read.map_err(|e| format!("a refusal that is not a Status: {e}"))?;
            if read.code != smeltr::io::otlp::code(*expected) || read.message.is_empty() {
                return Err(format!("refused with {read:?}"));
            }
            if *expected == 401 && header("www-authenticate").as_deref() != Some("Bearer") {
                return Err("a 401 that names no Bearer challenge".into());
            }
            Ok(())
        }
        Expected::Grpc(code) => {
            status(200)?;
            if header("content-type").as_deref() != Some("application/grpc") {
                return Err(format!("answered gRPC as {:?}", header("content-type")));
            }
            let heard = match code {
                // Delivered: an empty response message, then trailers saying so.
                0 => {
                    if answer.body != [0, 0, 0, 0, 0] {
                        return Err(format!("delivered with {:?}", answer.body));
                    }
                    (answer.trailers.as_ref()).and_then(|t| t.get("grpc-status")?.to_str().ok().map(String::from))
                }
                // Refused: the status in the headers alone.
                _ => {
                    if !answer.body.is_empty() {
                        return Err(format!("refused with a message {:?}", answer.body));
                    }
                    header("grpc-status")
                }
            };
            match heard == Some(code.to_string()) {
                true => Ok(()),
                false => Err(format!(
                    "grpc-status {heard:?}, not {code}: {:?}",
                    header("grpc-message")
                )),
            }
        }
    }
}

#[test]
fn every_setting_answers_random_otlp_exports_as_otlp_does() {
    crate::certs::install_provider();
    let pki = Arc::new(Pki::new());
    let ipv6 = receiver::ipv6();
    let signals = SIGNALS.map(|s| s.name());
    let mut cov = Coverage::new(
        receiver::listen_names(ipv6)
            .map(|ip| format!("listen {ip}"))
            .chain(["None", "Served", "Vouched"].map(|tls| format!("tls {tls}")))
            .chain((1..=3).map(|n| format!("{n} receivers on a port")))
            .chain(signals.iter().map(|s| format!("a receiver of {s}")))
            .chain((0..=3).map(|n| format!("a receiver of {n} tokens")))
            .chain(RECEIVER_LIMITS.iter().map(|(_, name)| name.to_string()))
            .chain(["presents Nothing under None".to_string()])
            .chain(["Served", "Vouched"].iter().flat_map(|tls| {
                ["Nothing", "Trusted", "Stranger"].map(|presents| format!("presents {presents} under {tls}"))
            }))
            .chain(["GET /health", "GET on an export's path", "a path nothing serves"].map(String::from))
            .chain(["over HTTP/1", "over HTTP/2"].map(String::from))
            .chain(
                signals
                    .iter()
                    .flat_map(|s| [format!("{s} over HTTP"), format!("{s} over gRPC")]),
            )
            .chain(
                [
                    "an export of a signal the port takes",
                    "an export of a signal no receiver takes",
                ]
                .map(String::from),
            )
            .chain(AUTHORIZATIONS.iter().map(|s| s.to_string()))
            .chain(["a receiver that holds no token".to_string()])
            .chain(
                (PROTOBUF_TYPES.iter().chain(JSON_TYPES).map(|t| Some(*t)))
                    .chain(UNSUPPORTED_TYPES.iter().copied())
                    .map(|t| format!("Content-Type {t:?}")),
            )
            .chain(
                (GRPC_TYPES.iter().map(|t| Some(*t)))
                    .chain(NOT_GRPC_TYPES.iter().copied())
                    .map(|t| format!("gRPC Content-Type {t:?}")),
            )
            .chain(["Protobuf over HTTP", "Json over HTTP"].map(String::from))
            .chain(CONTENT_ENCODINGS.iter().map(|(e, _)| format!("Content-Encoding {e:?}")))
            .chain(GRPC_ENCODINGS.iter().map(|(e, _)| format!("grpc-encoding {e:?}")))
            .chain(
                PROTOBUF_SPOILS
                    .iter()
                    .chain(JSON_SPOILS)
                    .chain(FRAMINGS)
                    .map(|s| s.to_string()),
            )
            .chain(
                [
                    "a body past its limit",
                    "a message past its limit",
                    "a body that inflates past its limit",
                    "a body that is not the gzip it says",
                    "a plain message under grpc-encoding gzip",
                    "gzip in two members",
                ]
                .map(String::from),
            )
            .chain(
                [
                    "no value", "a string", "a bool", "an int", "a double", "bytes", "an array", "a kvlist",
                ]
                .map(|kind| format!("value {kind}")),
            )
            .chain(
                [
                    "an attribute that carries no AnyValue",
                    "a time under a microsecond",
                    "an all-zero ID",
                    "an ID of the wrong length",
                    "a metric with no data",
                    "protobuf: a zero written",
                    "protobuf: a scalar given twice, the last standing",
                    "protobuf: an empty packed run",
                    "protobuf: numbers packed",
                    "protobuf: numbers unpacked",
                    "protobuf: a field no reader knows",
                    "protobuf: a oneof given twice, the last standing",
                    "protobuf: a metric's data given twice, the last standing",
                    "protobuf: a number given as both kinds, the last standing",
                    "json: a field under its protobuf name",
                    "json: a zero given as null",
                    "json: a zero written",
                    "json: a key no reader knows",
                    "json: an integer as text",
                    "json: an infinite double",
                    "json: a double as text",
                    "json: a whole double as an integer",
                    "json: an enum by name",
                    "json: an ID in upper-case hex",
                    "json: bytes in standard base64",
                    "json: bytes in standard unpadded base64",
                    "json: bytes in URL-safe base64",
                    "json: bytes in URL-safe unpadded base64",
                    "a key given twice",
                    "records of an export answered as delivered",
                ]
                .map(String::from),
            )
            .chain([0, 200, 400, 401, 404, 405, 413, 415].map(|status| format!("answered {status}")))
            .chain([0, 3, 12, 16].map(|code| format!("grpc-status {code}"))),
    );
    fuzz::run(1..151, |seed| {
        let mut g = Gen {
            rng: Rng::seeded(seed),
            labels: Vec::new(),
            collected: false,
        };
        let settings = random_settings(&mut g.rng, ipv6);
        cov.reach(format!("listen {}", settings.listen.ip()));
        cov.reach(format!("tls {:?}", settings.tls));
        cov.reach(format!("{} receivers on a port", settings.receivers.len()));
        for taking in &settings.receivers {
            cov.reach(format!("a receiver of {}", taking.signal.name()));
            cov.reach(format!("a receiver of {} tokens", taking.tokens.len()));
            let limit = (RECEIVER_LIMITS.iter())
                .find(|(below, _)| taking.max_request_bytes < *below)
                .unwrap();
            cov.reach(limit.1);
        }
        let mut sources = open(&settings, &pki);
        let sent: Vec<Sent> = (0..10).map(|_| random_request(&mut g, &settings)).collect();
        let sending = Arc::new((settings, sent));
        let (senders_pki, requests) = (pki.clone(), sending.clone());
        // One at a time, so the records of each signal arrive in the order they were sent.
        let sender = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let (settings, sent) = &*requests;
            (sent.iter())
                .map(|sent| runtime.block_on(exchange(settings, &senders_pki, sent)))
                .collect::<Vec<Answer>>()
        });
        // An export is answered once its records are committed, so they are taken as they arrive.
        let deadline = Instant::now() + Duration::from_secs(120);
        let mut took: Vec<(Signal, Vec<Got>)> = sources.iter().map(|(signal, _)| (*signal, Vec::new())).collect();
        let mut poll = |sources: &mut Vec<(Signal, Box<dyn Source>)>, wait: u64| {
            for ((signal, source), (_, took)) in sources.iter_mut().zip(&mut took) {
                let batch = source.poll_batch(10_000, 1 << 26, Duration::from_millis(wait)).unwrap();
                commit(source, &batch);
                took.extend(read_back(*signal, &batch));
            }
        };
        while !sender.is_finished() {
            if Instant::now() > deadline {
                return Err("the sender is still waiting".into());
            }
            poll(&mut sources, 5);
        }
        let answers = sender.join().unwrap();
        poll(&mut sources, 50);

        let (settings, sent) = &*sending;
        let mut expected_records: Vec<(Signal, Vec<Got>)> =
            sources.iter().map(|(signal, _)| (*signal, Vec::new())).collect();
        for (i, (sent, answer)) in sent.iter().zip(&answers).enumerate() {
            let what = format!(
                "request {i} ({} {} over HTTP/{} under {:?}, presenting {:?}, {} bytes, headers {:?}, limits {:?})",
                if sent.get { "GET" } else { "POST" },
                sent.path,
                if sent.h2 { 2 } else { 1 },
                settings.tls,
                sent.presents,
                sent.body.len(),
                sent.headers,
                (settings.receivers.iter())
                    .map(|r| (r.signal.name(), r.max_request_bytes))
                    .collect::<Vec<_>>(),
            );
            check_answer(sent, answer).map_err(|why| format!("{what}: expected {:?}, {why}", sent.expected))?;
            cov.reach(format!("answered {}", answer.status));
            if let Expected::Grpc(code) = sent.expected {
                cov.reach(format!("grpc-status {code}"));
            }
            for label in &sent.labels {
                cov.reach(label.clone());
            }
            if let Some((signal, records)) = &sent.records {
                if !records.is_empty() {
                    cov.reach("records of an export answered as delivered");
                }
                let into = expected_records.iter_mut().find(|(s, _)| s == signal).unwrap();
                into.1.extend(records.iter().cloned());
            }
        }
        for ((signal, took), (_, expected)) in took.iter().zip(&expected_records) {
            if took != expected {
                let at = (took.iter().zip(expected)).position(|(t, e)| t != e);
                let shown = |records: &[Got]| match at {
                    Some(at) => format!("{:?}", records.get(at)),
                    None => format!("{} records", records.len()),
                };
                return Err(format!(
                    "the receiver of {} took {} records, not the {} of the exports answered as delivered; first \
                     apart at {at:?}:\ntook     {}\nexpected {}",
                    signal.name(),
                    took.len(),
                    expected.len(),
                    shown(took),
                    shown(expected),
                ));
            }
        }
        Ok(())
    });
    cov.assert_complete();
}

/// How the collector exports one signal.
#[derive(Debug)]
struct Exporter {
    signal: Signal,
    /// `otlp`, over gRPC, or `otlphttp`.
    grpc: bool,
    json: bool,
    gzip: bool,
    token: Option<String>,
}

/// The collector's configuration: take OTLP/HTTP at `listening`, and export each signal to `at` as `exporters` say, over
/// TLS when `tls` is not None.
fn collector_config(listening: SocketAddr, at: SocketAddr, tls: Tls, exporters: &[Exporter]) -> String {
    let mut config =
        format!("receivers:\n  otlp:\n    protocols:\n      http:\n        endpoint: {listening}\nexporters:\n");
    let scheme = if tls == Tls::None { "http" } else { "https" };
    let tls = match tls {
        Tls::None => "{insecure: true}",
        Tls::Served => "{ca_file: /smeltr/ca.pem, server_name_override: localhost}",
        Tls::Vouched => {
            "{ca_file: /smeltr/ca.pem, server_name_override: localhost, cert_file: /smeltr/exporter.pem, key_file: \
             /smeltr/exporter-key.pem}"
        }
    };
    for e in exporters {
        let (kind, endpoint) = match e.grpc {
            true => ("otlp", at.to_string()),
            false => ("otlphttp", format!("{scheme}://{at}")),
        };
        config.push_str(&format!(
            "  {kind}/{signal}:\n    endpoint: {endpoint}\n    tls: {tls}\n    compression: {}\n    sending_queue: \
             {{enabled: false}}\n    retry_on_failure: {{enabled: false}}\n",
            if e.gzip { "gzip" } else { "none" },
            signal = e.signal.name(),
        ));
        if !e.grpc {
            config.push_str(&format!("    encoding: {}\n", if e.json { "json" } else { "proto" }));
        }
        if let Some(token) = &e.token {
            config.push_str(&format!("    headers: {{authorization: \"Bearer {token}\"}}\n"));
        }
    }
    config.push_str("service:\n  telemetry:\n    metrics: {level: none}\n  pipelines:\n");
    for e in exporters {
        let kind = if e.grpc { "otlp" } else { "otlphttp" };
        config.push_str(&format!(
            "    {signal}:\n      receivers: [otlp]\n      exporters: [{kind}/{signal}]\n",
            signal = e.signal.name()
        ));
    }
    config
}

/// A real OpenTelemetry Collector takes random exports of every signal and passes each on to receivers of random
/// settings, under random configurations of its own: over gRPC or HTTP, as protobuf or JSON, gzip or not, plain, over
/// TLS, or over TLS with a client certificate. The collector hears each export answered as delivered, and every item
/// arrives once, as the record OTLP says, in the order it was sent.
#[test]
fn live_collectors_deliver_random_exports_under_random_configurations() {
    const TEST: &str = "live_collectors_deliver_random_exports_under_random_configurations";
    let Some(collector) = crate::live::otel_collector(TEST) else {
        return;
    };
    // The receiver's handlers run on its runtime's threads, where a panic drops a connection and fails no test by
    // itself.
    let panicked = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let seen = panicked.clone();
    let _hook = Hook::set(Box::new(move |info| {
        if std::thread::current().name() == Some("smeltr-receiver") {
            seen.store(true, std::sync::atomic::Ordering::Relaxed);
        }
        eprintln!("{info}");
    }));
    crate::certs::install_provider();
    let pki = Pki::new();
    let mut cov = Coverage::new(
        ["None", "Served", "Vouched"]
            .map(|tls| format!("tls {tls}"))
            .into_iter()
            .chain(SIGNALS.iter().flat_map(|s| {
                ["gRPC", "HTTP as protobuf", "HTTP as JSON"].map(|wire| format!("{} over {wire}", s.name()))
            }))
            .chain(["gzip", "no compression", "a token", "no token"].map(String::from))
            .chain(["records of an export"].map(String::from)),
    );

    fuzz::run(1..25, |seed| {
        panicked.store(false, std::sync::atomic::Ordering::Relaxed);
        let mut g = Gen {
            rng: Rng::seeded(seed),
            labels: Vec::new(),
            collected: true,
        };
        // Without IPv6, the collector reaches the receivers over IPv4 loopback, on the host's network.
        let mut settings = random_settings(&mut g.rng, false);
        for taking in &mut settings.receivers {
            taking.max_request_bytes = *g.pick(&[1 << 20, 8 << 20]);
            taking.tokens.truncate(1);
        }
        cov.reach(format!("tls {:?}", settings.tls));
        let exporters: Vec<Exporter> = (settings.receivers.iter())
            .map(|taking| {
                let (grpc, json) = match g.below(3) {
                    0 => (true, false),
                    1 => (false, false),
                    _ => (false, true),
                };
                Exporter {
                    signal: taking.signal,
                    grpc,
                    json,
                    gzip: g.chance(2),
                    token: taking.tokens.first().cloned(),
                }
            })
            .collect();
        for e in &exporters {
            let wire = match (e.grpc, e.json) {
                (true, _) => "gRPC",
                (false, false) => "HTTP as protobuf",
                (false, true) => "HTTP as JSON",
            };
            cov.reach(format!("{} over {wire}", e.signal.name()));
            cov.reach(if e.gzip { "gzip" } else { "no compression" });
            cov.reach(if e.token.is_some() { "a token" } else { "no token" });
        }
        let what = format!(
            "under {:?}, listening on {}, exporting {exporters:?}",
            settings.tls, settings.listen
        );
        let mut sources = open(&settings, &pki);

        let scratch = Scratch(std::env::temp_dir().join(format!("smeltr-otlp-fuzz-{}-{seed}", std::process::id())));
        let dir = &scratch.0;
        std::fs::create_dir_all(dir).unwrap();
        let collector_at = SocketAddr::new(
            Ipv4Addr::LOCALHOST.into(),
            std::net::TcpListener::bind("127.0.0.1:0")
                .unwrap()
                .local_addr()
                .unwrap()
                .port(),
        );
        let config = collector_config(collector_at, settings.connect, settings.tls, &exporters);
        let trusted = &pki.sender;
        for (file, contents) in [
            ("config.yaml", config.as_str()),
            ("ca.pem", &pki.authority.pem),
            ("exporter.pem", &trusted.certificate),
            ("exporter-key.pem", &trusted.key),
        ] {
            std::fs::write(dir.join(file), contents).unwrap();
        }
        // The collector runs as a user of its own, which reads what this test wrote.
        share(dir);
        let name = format!("smeltr-otlp-fuzz-{}-{seed}", std::process::id());
        let started = std::process::Command::new("docker")
            .args(["run", "-d", "--network", "host", "--name", &name, "-v"])
            .arg(format!("{}:/smeltr:ro", dir.display()))
            .args([collector.as_str(), "--config", "/smeltr/config.yaml"])
            .output()
            .expect("docker runs");
        // A run that fails may still have created its container.
        let container = Container(name);
        if !started.status.success() {
            return Err(format!("docker run: {}", String::from_utf8_lossy(&started.stderr)));
        }

        let exports: Vec<Export> = (0..1 + g.below(6))
            .map(|_| {
                let signal = g.pick(&settings.receivers).signal;
                random_export(&mut g, signal)
            })
            .collect();
        let bodies: Vec<(Signal, Vec<u8>)> = exports.iter().map(|e| (e.signal, pb_export(&mut g, e))).collect();
        // One at a time, each answered once the collector hears it delivered, so each signal's arrive in order.
        let sender = std::thread::Builder::new()
            .name("exports".into())
            .spawn(move || -> Result<(), String> {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                let _within = runtime.enter();
                let client = reqwest::Client::builder().no_proxy().build().unwrap();
                let up = Instant::now() + Duration::from_secs(30);
                for (signal, body) in bodies {
                    let url = format!("http://{collector_at}{}", smeltr::io::otlp::http_path(signal));
                    let answer = loop {
                        let sent = client
                            .post(&url)
                            .header("content-type", "application/x-protobuf")
                            .body(body.clone())
                            .timeout(Duration::from_secs(30))
                            .send();
                        match runtime.block_on(sent) {
                            // The collector takes exports once it has started.
                            Err(e) if e.is_connect() && Instant::now() < up => {
                                std::thread::sleep(Duration::from_millis(100))
                            }
                            Err(e) => return Err(format!("sending {} to the collector: {e:?}", signal.name())),
                            Ok(answer) => break answer,
                        }
                    };
                    let status = answer.status();
                    if !status.is_success() {
                        let body = runtime.block_on(answer.bytes()).unwrap_or_default();
                        return Err(format!(
                            "the collector answered {} {status}: {:?}",
                            signal.name(),
                            String::from_utf8_lossy(&body)
                        ));
                    }
                }
                Ok(())
            })
            .unwrap();

        let mut took: Vec<(Signal, Vec<Got>)> = sources.iter().map(|(signal, _)| (*signal, Vec::new())).collect();
        let mut poll = |sources: &mut Vec<(Signal, Box<dyn Source>)>| {
            for ((signal, source), (_, took)) in sources.iter_mut().zip(&mut took) {
                let batch = source.poll_batch(10_000, 1 << 26, Duration::from_millis(20)).unwrap();
                commit(source, &batch);
                took.extend(read_back(*signal, &batch));
            }
        };
        let deadline = Instant::now() + Duration::from_secs(90);
        while !sender.is_finished() {
            if Instant::now() > deadline {
                return Err(format!(
                    "{what}: the collector is still sending\n{}",
                    container_logs(&container.0)
                ));
            }
            poll(&mut sources);
        }
        let sent = sender.join().unwrap();
        // A little longer, so a record taken twice shows.
        let settled = Instant::now() + Duration::from_secs(2);
        while Instant::now() < settled {
            poll(&mut sources);
        }
        sent.map_err(|why| format!("{what}: {why}\n{}", container_logs(&container.0)))?;
        if panicked.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(format!("{what}: the receiver panicked"));
        }

        let remote = settings.connect.ip().to_string();
        for (signal, took) in &took {
            let expected: Vec<Got> = (exports.iter())
                .filter(|e| e.signal == *signal)
                .flat_map(|e| records_of(e, &remote))
                .collect();
            if !expected.is_empty() {
                cov.reach("records of an export");
            }
            if *took != expected {
                let at = (took.iter().zip(&expected)).position(|(t, e)| t != e);
                let shown = |records: &[Got]| match at {
                    Some(at) => format!("{:?}", records.get(at)),
                    None => format!("{} records", records.len()),
                };
                return Err(format!(
                    "{what}: the receiver of {} took {} records, not the {} of the exports; first apart at \
                     {at:?}:\ntook     {}\nexpected {}",
                    signal.name(),
                    took.len(),
                    expected.len(),
                    shown(took),
                    shown(&expected),
                ));
            }
        }
        let logs = container_logs(&container.0);
        if ["Exporting failed", "\terror\t"].iter().any(|f| logs.contains(f)) {
            return Err(format!("{what}: the collector logged a failure:\n{logs}"));
        }
        Ok(())
    });
    cov.assert_complete();
}
