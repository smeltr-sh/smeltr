//! What an OTLP receiver reads: OpenTelemetry's exports of one signal (logs, metrics or traces), over HTTP as
//! protobuf or JSON, and over gRPC.
//!
//! A request holds resources, each with its instrumentation scopes, each with its items: log records, metrics or
//! spans. Each item is one record, or a metric one record per data point, its fields typed columns and its values
//! variants of their own types. Its resource and scope ride with it as its `__source` fields. A body is read whole
//! before any record is taken, so a body that does not read is refused whole.
//!
//! ```text
//!   Export*ServiceRequest ─► Resource* ─► Scope* ─► LogRecord* | Metric* ─► data point* | Span*
//!                               └──────────┴─► Filed: __source.resource, __source.scope.*
//! ```

use std::sync::Arc;

use prost::Message as _;

use super::hec::Filed;
use crate::core::variant;
use crate::plan::topology::Signal;

mod json;
mod logs;
mod metrics;
mod traces;

/// The keys of [`Filed`] this module fills, each by the `__source` field it is read as, and each left out when empty.
/// `resource` is the resource's attributes, and `scope.attributes` the scope's, each an object in `core::variant`'s
/// encoding; `scope.name` and `scope.version` are text.
pub const RESOURCE: &str = "resource";
pub const SCOPE_NAME: &str = "scope.name";
pub const SCOPE_VERSION: &str = "scope.version";
pub const SCOPE_ATTRIBUTES: &str = "scope.attributes";

/// Where an OTLP/HTTP exporter sends `signal`.
pub fn http_path(signal: Signal) -> &'static str {
    match signal {
        Signal::Logs => "/v1/logs",
        Signal::Metrics => "/v1/metrics",
        Signal::Traces => "/v1/traces",
    }
}

/// Where an OTLP/gRPC exporter sends `signal`: the `Export` method of its service, as gRPC names a method.
pub fn grpc_path(signal: Signal) -> &'static str {
    match signal {
        Signal::Logs => "/opentelemetry.proto.collector.logs.v1.LogsService/Export",
        Signal::Metrics => "/opentelemetry.proto.collector.metrics.v1.MetricsService/Export",
        Signal::Traces => "/opentelemetry.proto.collector.trace.v1.TraceService/Export",
    }
}

/// The `Content-Type` of a gRPC request and its answer.
pub const GRPC_CONTENT_TYPE: &str = "application/grpc";

/// Whether a request's `Content-Type` is gRPC's: `application/grpc`, or that with a `+` naming its message encoding,
/// which for OTLP is `+proto`.
pub fn is_grpc(content_type: Option<&str>) -> bool {
    content_type.is_some_and(|named| {
        let named = named.split(';').next().unwrap_or_default().trim().to_ascii_lowercase();
        named == GRPC_CONTENT_TYPE || named == "application/grpc+proto"
    })
}

/// The bytes before each gRPC message: whether it is compressed, then its length, big-endian.
pub const GRPC_PREFIX: usize = 5;

/// `message` as gRPC sends it, uncompressed.
pub fn grpc_framed(message: &[u8]) -> Vec<u8> {
    let length = u32::try_from(message.len()).expect("an answer is far below 4 GiB");
    let mut framed = Vec::with_capacity(GRPC_PREFIX + message.len());
    framed.push(0);
    framed.extend_from_slice(&length.to_be_bytes());
    framed.extend_from_slice(message);
    framed
}

/// How a request's body, and so its answer's, is encoded.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub enum Encoding {
    #[default]
    Protobuf,
    Json,
}

impl Encoding {
    /// What a request's `Content-Type` names: `application/x-protobuf` or `application/json`, in any case and with any
    /// parameters. `None` for anything else, or none given.
    pub fn of(content_type: Option<&str>) -> Option<Encoding> {
        let named = content_type?.split(';').next()?.trim();
        [Encoding::Protobuf, Encoding::Json]
            .into_iter()
            .find(|e| named.eq_ignore_ascii_case(e.content_type()))
    }

    pub fn content_type(self) -> &'static str {
        match self {
            Encoding::Protobuf => "application/x-protobuf",
            Encoding::Json => "application/json",
        }
    }
}

/// The body of a 200: an `Export*ServiceResponse` with no `partialSuccess`, which OTLP leaves unset for a request it
/// took whole. In protobuf, a message with nothing set is no bytes, whatever the signal.
pub fn delivered(encoding: Encoding) -> Vec<u8> {
    match encoding {
        Encoding::Protobuf => Vec::new(),
        Encoding::Json => b"{}".to_vec(),
    }
}

/// The gRPC status code nearest HTTP status `http`. An exporter sends again on `UNAVAILABLE`, and drops an export on
/// the others.
pub fn code(http: u16) -> i32 {
    match http {
        // Not RESOURCE_EXHAUSTED for 413: some exporters send that again, and an export too large is too large again.
        400 | 413 | 415 => 3, // INVALID_ARGUMENT
        401 => 16,            // UNAUTHENTICATED
        404 => 12,            // UNIMPLEMENTED
        408 => 4,             // DEADLINE_EXCEEDED
        503 => 14,            // UNAVAILABLE
        _ => 2,               // UNKNOWN
    }
}

/// The body of a refusal answered with HTTP status `http`: a `google.rpc.Status` with `message` and the gRPC code
/// nearest `http`, as OTLP/HTTP answers a failure.
pub fn status(http: u16, message: &str, encoding: Encoding) -> Vec<u8> {
    let code = code(http);
    match encoding {
        Encoding::Protobuf => Status {
            code,
            message: message.to_string(),
        }
        .encode_to_vec(),
        Encoding::Json => {
            let mut out = format!(r#"{{"code":{code},"message":"#).into_bytes();
            crate::format::json::write_json_string(message.as_bytes(), &mut out);
            out.push(b'}');
            out
        }
    }
}

/// One field of a record. A time is an `Int` of microseconds since the epoch, as a datetime column holds it.
#[derive(Clone, Debug, PartialEq)]
pub enum Cell {
    Int(i64),
    Float(f64),
    Bool(bool),
    Text(String),
    /// A value in `core::variant`'s encoding.
    Variant(Vec<u8>),
}

/// One record: its fields in the order of its signal's record (`SourceMeta::otlp`), `None` where it is null.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Row(pub Vec<Option<Cell>>);

impl Row {
    /// Bytes it holds, for the inbox's accounting.
    pub fn size(&self) -> usize {
        (self.0.iter().flatten())
            .map(|cell| match cell {
                Cell::Int(_) | Cell::Float(_) => size_of::<i64>(),
                Cell::Bool(_) => 1,
                Cell::Text(text) => text.len(),
                Cell::Variant(bytes) => bytes.len(),
            })
            .sum()
    }
}

/// What a protobuf export of `signal` will hold once decoded, in bytes, read from its wire form without decoding it,
/// or why it is not one. A message in a repeated field costs its struct twice over, for the room a vector grows into,
/// and the first in each field four times, the least room a vector takes; one in a singular field is inside its
/// parent's; and each string or bytes value costs its length.
pub fn decoded_size(signal: Signal, body: &[u8]) -> Result<usize, String> {
    let mut size = body.len();
    let walked = match signal {
        Signal::Logs => walk(body, Kind::Signal(logs::Kind::Request), 0, &mut size),
        Signal::Metrics => walk(body, Kind::Signal(metrics::Kind::Request), 0, &mut size),
        Signal::Traces => walk(body, Kind::Signal(traces::Kind::Request), 0, &mut size),
    };
    walked.map_err(|e| format!("not an OTLP {} request: {e}", signal.name()))?;
    Ok(size)
}

/// What reading an OTLP JSON body of `signal` holds at once, at most: the body, the text it holds copied out of it,
/// and the messages it is read into. A string with an escape is read into a buffer that grows by doubling, so text
/// copied out takes twice the body at most. Every message is an object, so each `{` makes one at most. A vector of n elements
/// takes room for 2n + 2 at most, the least it takes being four, so each element takes twice its struct and each
/// vector, which opens at a `[`, twice its largest struct again. A repeated number is eight bytes, and follows a `[`
/// or a `,`.
pub fn json_peak(signal: Signal, body: &[u8]) -> usize {
    let largest = match signal {
        Signal::Logs => largest::<logs::Kind>(),
        Signal::Metrics => largest::<metrics::Kind>(),
        Signal::Traces => largest::<traces::Kind>(),
    };
    let (mut objects, mut vectors, mut items) = (0usize, 0usize, 0usize);
    // A string's bytes make no message, whatever they are.
    let (mut quoted, mut escaped) = (false, false);
    for b in body {
        match b {
            _ if escaped => escaped = false,
            b'\\' if quoted => escaped = true,
            b'"' => quoted = !quoted,
            _ if quoted => {}
            b'{' => objects += 1,
            b'[' => (vectors, items) = (vectors + 1, items + 1),
            b',' => items += 1,
            _ => {}
        }
    }
    (body.len().saturating_mul(3))
        .saturating_add((objects.saturating_add(vectors)).saturating_mul(2 * largest))
        .saturating_add(items.saturating_mul(2 * size_of::<u64>()))
}

/// The largest message a request of `S` holds.
fn largest<S: Shape>() -> usize {
    let shared = [
        Kind::Resource,
        Kind::Scope,
        Kind::KeyValue,
        Kind::AnyValue,
        Kind::ArrayValue,
        Kind::KeyValueList,
    ];
    (S::ALL.iter().map(|s| s.size()))
        .chain(shared.into_iter().map(Kind::<S>::size))
        .max()
        .unwrap_or_default()
}

/// A decoded export of one signal, its records rendered one at a time as they are taken.
#[derive(Debug)]
pub enum Export {
    Logs(logs::Logs),
    /// Boxed, as the metric being read holds its own fields beside its points.
    Metrics(Box<metrics::Metrics>),
    Traces(traces::Traces),
}

impl Export {
    /// A protobuf `Export*ServiceRequest` of `signal`, or why it does not decode.
    pub fn protobuf(signal: Signal, body: &[u8]) -> Result<Export, String> {
        let not = |e: prost::DecodeError| format!("not an OTLP {} request: {e}", signal.name());
        Ok(match signal {
            Signal::Logs => Export::Logs(logs::Logs::protobuf(body).map_err(not)?),
            Signal::Metrics => Export::Metrics(Box::new(metrics::Metrics::protobuf(body).map_err(not)?)),
            Signal::Traces => Export::Traces(traces::Traces::protobuf(body).map_err(not)?),
        })
    }

    /// The same request in OTLP's JSON encoding, or why it does not read. Fields are read by their camelCase name or
    /// their protobuf name, and fields this module does not know are skipped, as OTLP asks of a receiver.
    pub fn json(signal: Signal, body: &[u8]) -> Result<Export, String> {
        json::request(body, |r| {
            Ok(match signal {
                Signal::Logs => Export::Logs(logs::Logs::json(r)?),
                Signal::Metrics => Export::Metrics(Box::new(metrics::Metrics::json(r)?)),
                Signal::Traces => Export::Traces(traces::Traces::json(r)?),
            })
        })
    }

    /// What this request holds decoded, as [`decoded_size`] reads it from its wire form.
    pub fn decoded_size(&self) -> usize {
        let (signal, body) = match self {
            Export::Logs(logs) => (Signal::Logs, logs.encoded()),
            Export::Metrics(metrics) => (Signal::Metrics, metrics.encoded()),
            Export::Traces(traces) => (Signal::Traces, traces.encoded()),
        };
        decoded_size(signal, &body).expect("a request this module encoded reads back")
    }

    /// The next record as a row, and what it is filed under, which the records of one scope share. `None` when every
    /// record is taken.
    pub fn next_record(&mut self) -> Option<(Row, Arc<Filed>)> {
        match self {
            Export::Logs(logs) => logs.next_record(),
            Export::Metrics(metrics) => metrics.next_record(),
            Export::Traces(traces) => traces.next_record(),
        }
    }
}

// The messages of opentelemetry-proto every signal shares, by their field numbers there. Fields this module does not
// read are left out: protobuf skips them.

/// `google.rpc.Status`, less its `details`.
#[derive(Clone, PartialEq, prost::Message)]
struct Status {
    #[prost(int32, tag = "1")]
    code: i32,
    #[prost(string, tag = "2")]
    message: String,
}

#[derive(Clone, PartialEq, prost::Message)]
struct Resource {
    #[prost(message, repeated, tag = "1")]
    attributes: Vec<KeyValue>,
    #[prost(uint32, tag = "2")]
    dropped_attributes_count: u32,
}

#[derive(Clone, PartialEq, prost::Message)]
struct InstrumentationScope {
    #[prost(string, tag = "1")]
    name: String,
    #[prost(string, tag = "2")]
    version: String,
    #[prost(message, repeated, tag = "3")]
    attributes: Vec<KeyValue>,
    #[prost(uint32, tag = "4")]
    dropped_attributes_count: u32,
}

#[derive(Clone, PartialEq, prost::Message)]
struct AnyValue {
    #[prost(oneof = "Any", tags = "1, 2, 3, 4, 5, 6, 7")]
    value: Option<Any>,
}

#[derive(Clone, PartialEq, prost::Oneof)]
enum Any {
    #[prost(string, tag = "1")]
    String(String),
    #[prost(bool, tag = "2")]
    Bool(bool),
    #[prost(int64, tag = "3")]
    Int(i64),
    #[prost(double, tag = "4")]
    Double(f64),
    #[prost(message, tag = "5")]
    Array(ArrayValue),
    #[prost(message, tag = "6")]
    Kvlist(KeyValueList),
    #[prost(bytes = "vec", tag = "7")]
    Bytes(Vec<u8>),
}

#[derive(Clone, PartialEq, prost::Message)]
struct ArrayValue {
    #[prost(message, repeated, tag = "1")]
    values: Vec<AnyValue>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct KeyValueList {
    #[prost(message, repeated, tag = "1")]
    values: Vec<KeyValue>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct KeyValue {
    #[prost(string, tag = "1")]
    key: String,
    #[prost(message, optional, tag = "2")]
    value: Option<AnyValue>,
}

/// The messages `walk` reads, by what each field holds: those every signal shares, and its own.
#[derive(Clone, Copy)]
enum Kind<S> {
    Signal(S),
    Resource,
    Scope,
    KeyValue,
    AnyValue,
    ArrayValue,
    KeyValueList,
}

/// A signal's own messages, as `walk` reads them.
trait Shape: Copy + Sized + 'static {
    /// Every message of the signal.
    const ALL: &'static [Self];
    fn size(self) -> usize;
    /// The message field `tag` holds, and whether the field repeats, or `None` for a scalar, a string, or a field
    /// this module does not read.
    fn field(self, tag: u32) -> Option<(Kind<Self>, bool)>;
    /// How field `tag` writes a repeated number, each eight bytes decoded, or `None` where it holds none. A packed
    /// varint can be one byte on the wire, so the body's length does not bound it.
    fn numbers(self, _tag: u32) -> Option<Number> {
        None
    }
}

/// How a repeated number is written on the wire.
#[derive(Clone, Copy)]
enum Number {
    Varint,
    Fixed64,
}

impl<S: Shape> Kind<S> {
    fn size(self) -> usize {
        match self {
            Kind::Signal(s) => s.size(),
            Kind::Resource => size_of::<Resource>(),
            Kind::Scope => size_of::<InstrumentationScope>(),
            Kind::KeyValue => size_of::<KeyValue>(),
            Kind::AnyValue => size_of::<AnyValue>(),
            Kind::ArrayValue => size_of::<ArrayValue>(),
            Kind::KeyValueList => size_of::<KeyValueList>(),
        }
    }

    fn field(self, tag: u32) -> Option<(Kind<S>, bool)> {
        Some(match (self, tag) {
            (Kind::Signal(s), _) => return s.field(tag),
            (Kind::Resource, 1) | (Kind::Scope, 3) | (Kind::KeyValueList, 1) => (Kind::KeyValue, true),
            (Kind::KeyValue, 2) => (Kind::AnyValue, false),
            (Kind::ArrayValue, 1) => (Kind::AnyValue, true),
            (Kind::AnyValue, 5) => (Kind::ArrayValue, false),
            (Kind::AnyValue, 6) => (Kind::KeyValueList, false),
            _ => return None,
        })
    }
}

/// Prost's own limit, past which it refuses a body.
const MAX_DEPTH: usize = 100;

/// Adds what `message`, a `kind`, will hold once decoded to `size`.
fn walk<S: Shape>(mut message: &[u8], kind: Kind<S>, depth: usize, size: &mut usize) -> Result<(), String> {
    use prost::encoding::{WireType, decode_key, decode_varint};

    if depth > MAX_DEPTH {
        return Err("recursion limit reached".into());
    }
    // The repeated fields met so far, by tag: a vector's first element takes room for four, the least a vector takes.
    let mut met = 0u64;
    while !message.is_empty() {
        let (tag, wire) = decode_key(&mut message).map_err(|e| e.to_string())?;
        // A repeated number's vector takes eight bytes for each, and as much again of spare room at most.
        let number = match kind {
            Kind::Signal(s) => s.numbers(tag),
            _ => None,
        };
        let numbers = |n: usize| 16 * n * usize::from(number.is_some());
        let skip = match wire {
            WireType::Varint => {
                decode_varint(&mut message).map_err(|e| e.to_string())?;
                *size = size.saturating_add(numbers(1));
                0
            }
            WireType::SixtyFourBit => {
                *size = size.saturating_add(numbers(1));
                8
            }
            WireType::ThirtyTwoBit => 4,
            WireType::LengthDelimited => {
                let len = decode_varint(&mut message).map_err(|e| e.to_string())?;
                let len = usize::try_from(len)
                    .ok()
                    .filter(|n| *n <= message.len())
                    .ok_or("buffer underflow")?;
                let (field, rest) = message.split_at(len);
                if let Some((inner, repeated)) = kind.field(tag) {
                    if repeated {
                        let first = met & (1 << tag.min(63)) == 0;
                        met |= 1 << tag.min(63);
                        let slots = if first { 4 } else { 2 };
                        *size = size.saturating_add(slots * inner.size());
                    }
                    walk(field, inner, depth + 1, size)?;
                } else {
                    // Packed: a varint ends at each byte with its high bit clear.
                    let count = match number {
                        Some(Number::Varint) => field.iter().filter(|b| **b < 0x80).count(),
                        Some(Number::Fixed64) => field.len() / 8,
                        None => 0,
                    };
                    *size = size.saturating_add(numbers(count));
                }
                message = rest;
                0
            }
            WireType::StartGroup | WireType::EndGroup => return Err("groups are not OTLP".into()),
        };
        if skip > message.len() {
            return Err("buffer underflow".into());
        }
        message = &message[skip..];
    }
    Ok(())
}

/// Where the next item of a request is: its resource, its scope within that, and its place within that; and what
/// the items of the scope being read are filed under, made at its first item and shared by them all.
#[derive(Debug, Default)]
struct Cursor {
    resource: usize,
    scope: usize,
    item: usize,
    filed: Option<Arc<Filed>>,
}

impl Cursor {
    /// The next item of `request`, taken from it, with what it is filed under. `scopes` gives a resource's
    /// resource and scopes, and `items` a scope's scope and items.
    fn next<R, S, T: Default>(
        &mut self,
        request: &mut [R],
        scopes: impl Fn(&mut R) -> (Option<&Resource>, &mut Vec<S>),
        items: impl Fn(&mut S) -> (Option<&InstrumentationScope>, &mut Vec<T>),
    ) -> Option<(T, Arc<Filed>)> {
        loop {
            let (resource, scoped) = scopes(request.get_mut(self.resource)?);
            let Some(scope) = scoped.get_mut(self.scope) else {
                (self.resource, self.scope) = (self.resource + 1, 0);
                continue;
            };
            let (scope, listed) = items(scope);
            let Some(item) = listed.get_mut(self.item) else {
                (self.scope, self.item, self.filed) = (self.scope + 1, 0, None);
                continue;
            };
            // Taken, so what an item held is let go as it is rendered.
            let item = std::mem::take(item);
            self.item += 1;
            let filed = (self.filed).get_or_insert_with(|| Arc::new(filed(resource, scope)));
            return Some((item, filed.clone()));
        }
    }
}

/// What the records of `resource` and `scope` are filed under.
fn filed(resource: Option<&Resource>, scope: Option<&InstrumentationScope>) -> Filed {
    let mut filed = Filed::new();
    if let Some(resource) = resource.filter(|r| !r.attributes.is_empty()) {
        filed.push((
            RESOURCE.to_string(),
            variant_of(|out| put_pairs(out, &resource.attributes)),
        ));
    }
    if let Some(scope) = scope {
        for (key, text) in [(SCOPE_NAME, &scope.name), (SCOPE_VERSION, &scope.version)] {
            if !text.is_empty() {
                filed.push((key.to_string(), text.clone().into_bytes()));
            }
        }
        if !scope.attributes.is_empty() {
            filed.push((
                SCOPE_ATTRIBUTES.to_string(),
                variant_of(|out| put_pairs(out, &scope.attributes)),
            ));
        }
    }
    filed
}

// What an item's fields become. A field at its zero value, which OTLP sends for one an item does not carry, is null.

fn nonzero(n: i64) -> Option<i64> {
    (n != 0).then_some(n)
}

fn int(n: impl Into<i64>) -> Option<Cell> {
    nonzero(n.into()).map(Cell::Int)
}

/// A time in nanoseconds as microseconds, the finest a datetime holds.
fn micros(nanos: u64) -> Option<Cell> {
    int((nanos / 1000) as i64)
}

fn text(text: String) -> Option<Cell> {
    (!text.is_empty()).then_some(Cell::Text(text))
}

/// `pairs` as an object, or null for none.
fn pairs(pairs: &[KeyValue]) -> Option<Cell> {
    (!pairs.is_empty()).then(|| Cell::Variant(variant_of(|out| put_pairs(out, pairs))))
}

pub(super) fn variant_of(put: impl FnOnce(&mut Vec<u8>)) -> Vec<u8> {
    let mut out = Vec::new();
    put(&mut out);
    out
}

/// `value` in `core::variant`'s encoding, each kind as the variant of its type, and no value as null. Every value a
/// record carries is written here, so it is the one place to change for OTLP's typed form.
fn put_any(out: &mut Vec<u8>, value: &AnyValue) {
    match &value.value {
        None => variant::put_null(out),
        Some(Any::String(text)) => variant::put_str(out, text),
        Some(Any::Bool(b)) => variant::put_bool(out, *b),
        Some(Any::Int(i)) => variant::put_i64(out, *i),
        Some(Any::Double(d)) => variant::put_f64(out, *d),
        Some(Any::Bytes(bytes)) => variant::put_bytes(out, bytes),
        Some(Any::Array(array)) => {
            let open = variant::begin_array(out);
            for item in &array.values {
                put_any(out, item);
            }
            variant::end(out, open, array.values.len());
        }
        Some(Any::Kvlist(list)) => put_pairs(out, &list.values),
    }
}

/// `pairs` as an object. A pair with no value is no entry, as a null under a key is none in a variant; of a key
/// given twice, the last value stands, in the first one's place.
fn put_pairs(out: &mut Vec<u8>, pairs: &[KeyValue]) {
    let open = variant::begin_object(out);
    let mut count = 0;
    for (key, value) in pairs
        .iter()
        .filter_map(|p| Some((&p.key, p.value.as_ref().filter(|v| v.value.is_some())?)))
    {
        variant::put_key(out, key);
        put_any(out, value);
        count += 1;
    }
    if count > 1 {
        count = variant::dedupe(out, open);
    }
    variant::end(out, open, count);
}

/// A trace or span ID as hex, where it is one: `len` bytes, not all zero. Any other value names no trace or span.
fn id(bytes: &[u8], len: usize) -> Option<String> {
    (bytes.len() == len && bytes.iter().any(|b| *b != 0)).then(|| hex(bytes))
}

fn hex(bytes: &[u8]) -> String {
    let mut out = Vec::with_capacity(bytes.len() * 2);
    crate::ops::hashops::to_hex(bytes, &mut out);
    String::from_utf8(out).expect("hex digits are ASCII")
}

/// What the tests of each signal read records with.
#[cfg(test)]
mod testing {
    use serde_json::{Map, Value};

    use super::*;

    /// A variant as JSON that keeps its type: an i64 an integer, an f64 a float, bytes `"bytes:<hex>"` and a datetime
    /// `"datetime:<micros>"`.
    pub fn shown(encoded: &[u8]) -> Value {
        use variant::Value as V;
        match variant::read(encoded) {
            V::Null => Value::Null,
            V::Bool(b) => b.into(),
            V::I64(i) => i.into(),
            V::F64(f) => match serde_json::Number::from_f64(f) {
                Some(n) => Value::Number(n),
                None => format!("f64:{f}").into(),
            },
            V::Str(s) => s.into(),
            V::Bytes(b) => format!("bytes:{}", hex(b)).into(),
            V::DateTime(micros) => format!("datetime:{micros}").into(),
            V::Array(items) => Value::Array(items.map(shown).collect()),
            V::Object(entries) => Value::Object(entries.map(|(k, v)| (k.to_string(), shown(v))).collect()),
            other => panic!("not written here: {other:?}"),
        }
    }

    /// A row as JSON by its signal's field names, leaving out its nulls: a time an integer of microseconds, and a
    /// variant as [`shown`].
    pub fn row_json(signal: Signal, row: &Row) -> Value {
        let meta = crate::plan::topology::SourceMeta::otlp(signal);
        assert_eq!(
            row.0.len(),
            meta.record().len(),
            "a {} row has its record's fields",
            signal.name()
        );
        let fields = (meta.record().iter().zip(&row.0)).filter_map(|((name, _), cell)| {
            let value = match cell.as_ref()? {
                Cell::Int(i) => (*i).into(),
                Cell::Float(f) => match serde_json::Number::from_f64(*f) {
                    Some(n) => Value::Number(n),
                    None => format!("f64:{f}").into(),
                },
                Cell::Bool(b) => (*b).into(),
                Cell::Text(t) => t.clone().into(),
                Cell::Variant(v) => shown(v),
            };
            Some((name.to_string(), value))
        });
        Value::Object(fields.collect())
    }

    pub fn filed_as(filed: &Filed, key: &str) -> Option<Value> {
        filed.iter().find(|(k, _)| k == key).map(|(_, v)| shown(v))
    }

    /// The scope a record is filed under, as one object of what it names.
    pub fn scope_of(filed: &Filed) -> Option<Value> {
        let mut scope = Map::new();
        for (key, value) in filed {
            match key.as_str() {
                SCOPE_NAME | SCOPE_VERSION => scope.insert(
                    key["scope.".len()..].into(),
                    String::from_utf8(value.clone()).unwrap().into(),
                ),
                SCOPE_ATTRIBUTES => scope.insert("attributes".into(), shown(value)),
                _ => None,
            };
        }
        (!scope.is_empty()).then_some(Value::Object(scope))
    }

    pub fn kv(key: &str, value: Any) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(AnyValue { value: Some(value) }),
        }
    }

    pub fn text(s: &str) -> Any {
        Any::String(s.to_string())
    }

    /// Every record of `export`.
    pub fn all(mut export: Export) -> Vec<(Row, Arc<Filed>)> {
        std::iter::from_fn(|| export.next_record()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_content_type_names_its_encoding() {
        assert_eq!(Encoding::of(Some("application/x-protobuf")), Some(Encoding::Protobuf));
        assert_eq!(
            Encoding::of(Some("Application/JSON; charset=utf-8")),
            Some(Encoding::Json)
        );
        assert_eq!(Encoding::of(Some(" application/json ")), Some(Encoding::Json));
        assert_eq!(Encoding::of(Some("text/plain")), None);
        assert_eq!(Encoding::of(Some("")), None);
        assert_eq!(Encoding::of(None), None);
        for e in [Encoding::Protobuf, Encoding::Json] {
            assert_eq!(Encoding::of(Some(e.content_type())), Some(e));
        }
    }

    #[test]
    fn a_delivered_request_is_answered_with_no_partial_success() {
        assert!(delivered(Encoding::Protobuf).is_empty());
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&delivered(Encoding::Json)).unwrap(),
            serde_json::json!({})
        );
    }

    #[test]
    fn a_refusal_names_its_grpc_code_and_why() {
        let read = |http| Status::decode(&status(http, "why", Encoding::Protobuf)[..]).unwrap();
        for (http, code) in [
            (400, 3),
            (413, 3),
            (415, 3),
            (401, 16),
            (404, 12),
            (408, 4),
            (503, 14),
            (500, 2),
            (429, 2),
        ] {
            assert_eq!(
                read(http),
                Status {
                    code,
                    message: "why".to_string()
                },
                "HTTP {http}"
            );
        }
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&status(503, "busy \"now\"", Encoding::Json)).unwrap(),
            serde_json::json!({"code": 14, "message": "busy \"now\""})
        );
    }

    /// OTLP's trace ID is 16 bytes and its span ID 8, neither all zero; anything else names none.
    #[test]
    fn an_id_that_is_not_one_is_null() {
        assert_eq!(id(&[0; 16], 16), None);
        assert_eq!(id(&[1; 15], 16), None);
        assert_eq!(id(&[1; 9], 8), None);
        assert_eq!(id(&[1; 16], 16), Some("01".repeat(16)));
        assert_eq!(id(&[2; 8], 8), Some("02".repeat(8)));
    }

    /// What reading a JSON body holds is bounded beforehand by at least what its messages hold, for records that are
    /// all fields and for records that are none.
    #[test]
    fn a_json_peak_bounds_the_messages_read() {
        let attr = r#"{"key":"service.name","value":{"stringValue":"checkout"}}"#;
        let full = format!(
            r#"{{"timeUnixNano":"1700000000000000000","severityText":"INFO","body":{{"stringValue":"GET /orders"}},
            "attributes":[{attr},{attr}],"traceId":"0102030405060708090a0b0c0d0e0f10","spanId":"0102030405060708"}}"#
        );
        for record in [full.as_str(), "{}"] {
            for n in [1, 3, 1000] {
                let records = vec![record; n].join(",");
                let body = format!(
                    r#"{{"resourceLogs":[{{"resource":{{"attributes":[{attr}]}},"scopeLogs":[{{"logRecords":[{records}]}}]}}]}}"#
                );
                let export = Export::json(Signal::Logs, body.as_bytes()).unwrap();
                let peak = json_peak(Signal::Logs, body.as_bytes());
                assert!(peak >= body.len() + export.decoded_size(), "{n} of {record}");
            }
        }
        let with = |text: &str| {
            format!(
                r#"{{"resourceLogs":[{{"scopeLogs":[{{"logRecords":[{{"body":{{"stringValue":"{text}"}}}}]}}]}}]}}"#
            )
        };
        let (braces, plain) = (with(&r#"{[,\""#.repeat(1000)), with(&"abcde".repeat(1000)));
        assert_eq!(braces.len(), plain.len());
        assert_eq!(
            json_peak(Signal::Logs, braces.as_bytes()),
            json_peak(Signal::Logs, plain.as_bytes()),
            "what a string holds makes no message"
        );
    }

    #[test]
    fn a_row_holds_what_its_cells_do() {
        let row = Row(vec![
            Some(Cell::Int(1)),
            None,
            Some(Cell::Float(1.5)),
            Some(Cell::Bool(true)),
            Some(Cell::Text("abc".into())),
            Some(Cell::Variant(vec![0; 7])),
        ]);
        assert_eq!(row.size(), 8 + 8 + 1 + 3 + 7);
        assert_eq!(Row::default().size(), 0);
    }
}
