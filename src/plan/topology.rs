//! Sources, destinations, and routes — the layer above a single compiled `Plan` that decides *which* pipeline a batch
//! of raw records runs through, and where each pipeline's output goes. See the design doc's "Sources, destinations,
//! routes" section for the full reasoning; this module is that section's config schema plus route compiling/evaluation.
//!
//! `SourceKind`/`DestinationKind` are config *shape* only in this pass — `io::kafka`'s actual runtime implementation
//! (behind the `Source`/ `Destination` traits) is a deliberately separate, later step. Nothing here depends on it, and
//! nothing here will need to change once it lands.

use std::collections::{HashMap, HashSet};

use crate::core::batch::{Batch, SLOT_LIVE};
use crate::core::column::{Column, DataType, Scalar};
use crate::core::ctx::Ctx;
use crate::core::error::ExceptionKind;
use crate::core::id::{DestId, Id, SourceId};
use crate::core::variant::EncodedBuf;
use crate::expr::ast::Span;
use crate::expr::eval::eval;
use crate::expr::ir::{Ir, IrKind, flatten};
use crate::format::encoding::Encoding;
use crate::metrics::MetricsSourceConfig;
use crate::ops::BinOp;
use crate::plan::compile::parse;
use crate::plan::compile::{Plan, compile, ingest_fields};
use crate::plan::config::{Answer, CompileError, OnException, Payload, PipelineConfig};
use crate::plan::resolve::{ResourceBuilder, SymbolTable, resolve};

// -- what a record carries ---------------------------------------------------

/// The bucket and key an SQS/S3 record's topic, `bucket/key`, names.
fn object_of(record: &crate::io::SourceRecord) -> (&[u8], &[u8]) {
    let topic = record.topic.as_bytes();
    match topic.iter().position(|&b| b == b'/') {
        Some(at) => (&topic[..at], &topic[at + 1..]),
        None => (topic, &[]),
    }
}

/// The part of an object's key that names its stream: its directories up to the first one with a digit in it, since
/// a date, an hour or a sequence number is where one stream's keys stop agreeing. `logs/okta/2026/10/14/a.json.gz`
/// and `logs/okta/2026/10/15/b.json.gz` are one stream, `logs/okta/`. The object's own name is never part of it.
fn object_prefix(key: &[u8]) -> std::borrow::Cow<'_, str> {
    let dirs = key.iter().rposition(|&b| b == b'/').map_or(&key[..0], |at| &key[..=at]);
    let mut kept = 0;
    for segment in dirs.split_inclusive(|&b| b == b'/') {
        if segment.iter().any(u8::is_ascii_digit) {
            break;
        }
        kept += segment.len();
    }
    String::from_utf8_lossy(&dirs[..kept])
}

impl SourceMeta {
    /// A source of `kind` whose payloads are text, in the text encoding it detects.
    const fn new(kind: &'static str, record: RecordFields, source: &'static [(&'static str, DataType)]) -> SourceMeta {
        SourceMeta {
            kind,
            record,
            source,
            encoding: Encoding::Auto,
            format: Format::Text,
        }
    }

    /// What its kind files `record` under, for the leader to tell one stream reaching a source from another
    /// (`heartbeat::UnmatchedStreams`): a HEC or S2S sender's sourcetype, an Elasticsearch sender's index, an
    /// object's bucket and directory prefix (`object_prefix`), and otherwise the record's topic, which a receiver
    /// fills with the sender's address and Kafka with the topic read. `None` where a sender named nothing.
    pub(crate) fn stream_key<'r>(&self, record: &'r crate::io::SourceRecord) -> Option<std::borrow::Cow<'r, str>> {
        match self.kind {
            "hecReceiver" | "s2sReceiver" => filed(record, "sourcetype").map(String::from_utf8_lossy),
            "elasticReceiver" => filed(record, "index").map(String::from_utf8_lossy),
            _ if self.source.iter().any(|(f, _)| *f == "__source.key") => {
                let (bucket, key) = object_of(record);
                Some(std::borrow::Cow::Owned(format!(
                    "{}/{}",
                    String::from_utf8_lossy(bucket),
                    object_prefix(key)
                )))
            }
            _ => Some(std::borrow::Cow::Borrowed(&record.topic)),
        }
    }

    /// The `__source` field that names what [`Self::stream_key`] files a record under, where a sender is counted
    /// under it (`streams::SenderTally`). `None` where the kind declares none.
    pub(crate) fn stream_field(&self) -> Option<&'static str> {
        let declares = |field: &str| self.source.iter().any(|(f, _)| *f == field);
        match self.kind {
            "hecReceiver" | "s2sReceiver" => Some("__source.sourcetype"),
            "elasticReceiver" => Some("__source.index"),
            _ if declares("__source.key") => Some("__source.key"),
            _ => TOPIC_FIELDS.into_iter().find(|f| declares(f)),
        }
    }

    /// A route condition that takes the records this kind files under `key` ([`Self::stream_key`]). An object's
    /// condition takes its bucket and every key under its prefix, so it also takes a stream filed under a longer
    /// prefix, which is the most a condition can say without the prefix rule itself. `None` where this kind files
    /// no field a condition could read.
    pub(crate) fn stream_condition(&self, key: &str) -> Option<String> {
        let literal = |s: &str| serde_json::to_string(s).expect("a string serializes");
        // A sender that names nothing is keyed as the empty key, and its field reads null.
        let filed_as = |field: &str, key: &str| match key {
            "" => format!("({field} ?? \"\") == \"\""),
            key => format!("{field} == {}", literal(key)),
        };
        let declares = |field: &str| self.source.iter().any(|(f, _)| *f == field);
        match self.kind {
            "hecReceiver" | "s2sReceiver" => Some(filed_as("__source.sourcetype", key)),
            "elasticReceiver" => Some(filed_as("__source.index", key)),
            _ if declares("__source.key") => {
                let (bucket, prefix) = key.split_once('/').unwrap_or((key, ""));
                let bucket = format!("__source.bucket == {}", literal(bucket));
                Some(match prefix {
                    "" => bucket,
                    prefix => format!("{bucket} && startsWith(__source.key, {})", literal(prefix)),
                })
            }
            _ => TOPIC_FIELDS
                .into_iter()
                .find(|f| declares(f))
                .map(|f| format!("{f} == {}", literal(key))),
        }
    }
}

/// What a source's records carry, as fields a pipeline or a route condition may read: the record itself, and the
/// metadata the source attaches to it.
///
/// Every source declares its record's fields, each with its type (`RecordFields`). A source whose client hands over
/// payloads declares one, `_raw`, and lists the payload as read among its metadata, `__source.bytes`
/// (`SourceMeta::reads_bytes`); a source whose data arrives already in typed columns declares one field per column.
/// The metadata is the kind's `__source` fields, so
/// nothing outside a `SourceMeta` assumes a source has a topic. Their names are `__`-prefixed, so
/// `format::record::RecordView` keeps them out of the output by the one rule it already has.
///
/// The record's fields occupy the slots immediately after `core::batch`'s prefix, in declaration order, and the
/// `__source` fields the slots after the record (`SourceMeta::source_slot`). Hashed by content, so pipelines compile
/// once per distinct meta whatever source declares it.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct SourceMeta {
    /// What `__source.type` reads.
    kind: &'static str,
    record: RecordFields,
    source: &'static [(&'static str, DataType)],
    /// How a byte source's payload becomes `_raw`'s text (`format::encoding`).
    encoding: Encoding,
    /// How its payloads are decoded into its record.
    format: Format,
}

/// How a source's payloads are laid out, and so how transform decodes them into its record.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Format {
    /// Text, read in the source's text encoding into `_raw`.
    Text,
    /// Confluent-framed Avro, read with the writer's schema each names (`io::avro::decode`).
    Avro,
}

/// The entries of a source's record, each with its type, in slot order. A source's own kinds declare theirs in a
/// `const`; a source whose record is configured declares it when it is built.
type RecordFields = std::borrow::Cow<'static, [(Entry, DataType)]>;

/// One entry of a record: a field, by its name, or the unclaimed fields of an open object, by the object's name. The
/// root's name is `""`.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum Entry {
    Field(std::borrow::Cow<'static, str>),
    Unclaimed(std::borrow::Cow<'static, str>),
}

/// The entry as a place in the name tree: a field's name, or `object.*` for an object's unclaimed fields.
impl std::fmt::Display for Entry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.place().fmt(f)
    }
}

impl Entry {
    /// The field's name, or `None` for an object's unclaimed fields.
    pub fn field(&self) -> Option<&str> {
        match self {
            Entry::Field(name) => Some(name),
            Entry::Unclaimed(_) => None,
        }
    }

    /// The keys of the field, or of the object whose unclaimed fields it is.
    pub(crate) fn keys(&self) -> Vec<String> {
        let (Entry::Field(name) | Entry::Unclaimed(name)) = self;
        crate::plan::tree::object_keys(name)
            .map(std::borrow::Cow::into_owned)
            .collect()
    }

    /// Its place in the compiler's name tree.
    pub(crate) fn place(&self) -> crate::plan::tree::Place {
        match self {
            Entry::Field(name) => crate::plan::tree::Place::Leaf(name.to_string()),
            Entry::Unclaimed(object) => crate::plan::tree::Place::Unclaimed(object.to_string()),
        }
    }

    /// The name a capture gives its column (`tree::unclaimed_entry` for an object's unclaimed fields).
    pub fn column(&self) -> String {
        match self {
            Entry::Field(name) => name.to_string(),
            Entry::Unclaimed(object) => crate::plan::tree::unclaimed_entry(object),
        }
    }
}

/// The `__source` field holding each record's payload as the source read it, before any decoding.
pub const SOURCE_BYTES: &str = "__source.bytes";

/// The `__destination` field a pipeline sets to give a row its document ID.
pub(crate) const DOC_ID: &str = "__destination.id";

/// The `__source` fields that read a record's topic, each under the name its kind gives it.
const TOPIC_FIELDS: [&str; 4] = ["__source.topic", "__source.url", "__source.remote", "__source.database"];

/// The record of a source whose client hands over payloads: the payload, decoded with the source's encoding.
const PAYLOAD_RECORD: RecordFields =
    std::borrow::Cow::Borrowed(&[(Entry::Field(std::borrow::Cow::Borrowed("_raw")), DataType::Str)]);

/// Every source names itself and its kind. `MetricsSource` also stamps a fixed `__source.topic` (`metrics::METRICS_TOPIC`) so a route can select metrics rows like any
/// other source's. Its partition and offset are a constant and a counter, carried for the commit tracker and
/// meaningless to a pipeline, so it declares neither.
const METRICS_FIELDS: &[(&str, DataType)] = &[
    ("__source.name", DataType::Str),
    ("__source.type", DataType::Str),
    ("__source.topic", DataType::Str),
    (SOURCE_BYTES, DataType::Bytes),
];

/// `LogsSource` stamps a fixed `__source.topic` (`io::logs::LOGS_TOPIC`) for the same reason `MetricsSource` does, and
/// carries neither partition nor offset for the same reason either.
const LOGS_FIELDS: &[(&str, DataType)] = METRICS_FIELDS;

/// A record's headers are an object of each key to its value as text. A key a record repeats holds its last value, and
/// a header with no value is no entry. Its time is the one Kafka stamped it with, its producer's or its broker's as
/// the topic keeps, so a pipeline can measure how long a record took to reach it.
const KAFKA_FIELDS: &[(&str, DataType)] = &[
    ("__source.name", DataType::Str),
    ("__source.type", DataType::Str),
    ("__source.topic", DataType::Str),
    ("__source.partition", DataType::I64),
    ("__source.offset", DataType::I64),
    ("__source.time", DataType::DateTime),
    ("__source.headers", DataType::Variant),
    (SOURCE_BYTES, DataType::Bytes),
];

/// An object source's record names the object it was read from and its place there. Its record's topic is
/// `bucket/key`, which splits at the first `/` because neither a bucket's name nor an Azure container's has one.
const OBJECT_FIELDS: &[(&str, DataType)] = &[
    ("__source.name", DataType::Str),
    ("__source.type", DataType::Str),
    ("__source.bucket", DataType::Str),
    ("__source.key", DataType::Str),
    ("__source.offset", DataType::I64),
    (SOURCE_BYTES, DataType::Bytes),
];

/// A receiver's record names the address of the sender that delivered it, which is its record's topic.
const RECEIVER_FIELDS: &[(&str, DataType)] = &[
    ("__source.name", DataType::Str),
    ("__source.type", DataType::Str),
    ("__source.remote", DataType::Str),
    (SOURCE_BYTES, DataType::Bytes),
];

/// A HEC or S2S receiver's record names its sender, and what the sender filed its event under: its host, source,
/// sourcetype and index, its time, and its indexed fields. Each is null where the sender named none.
const HEC_RECEIVER_FIELDS: &[(&str, DataType)] = &[
    ("__source.name", DataType::Str),
    ("__source.type", DataType::Str),
    ("__source.remote", DataType::Str),
    ("__source.host", DataType::Str),
    ("__source.source", DataType::Str),
    ("__source.sourcetype", DataType::Str),
    ("__source.index", DataType::Str),
    ("__source.time", DataType::DateTime),
    ("__source.fields", DataType::Variant),
    (SOURCE_BYTES, DataType::Bytes),
];

/// An Elasticsearch receiver's record names its sender, and what the sender's bulk action named for its document: the
/// index, the id and the ingest pipeline. Each is null where the sender named none.
const ELASTIC_RECEIVER_FIELDS: &[(&str, DataType)] = &[
    ("__source.name", DataType::Str),
    ("__source.type", DataType::Str),
    ("__source.remote", DataType::Str),
    ("__source.index", DataType::Str),
    ("__source.id", DataType::Str),
    ("__source.pipeline", DataType::Str),
    (SOURCE_BYTES, DataType::Bytes),
];

/// An OTLP log record, each field typed as OTLP types it. `body` and `attributes` keep each value's own type: an int,
/// a double, bytes, a list or a map. Its times are microseconds, the finest a datetime holds. Each field is null where
/// the log record leaves it unset.
const OTLP_LOG: RecordFields = std::borrow::Cow::Borrowed(&[
    (Entry::Field(std::borrow::Cow::Borrowed("time")), DataType::DateTime),
    (
        Entry::Field(std::borrow::Cow::Borrowed("observedTime")),
        DataType::DateTime,
    ),
    (
        Entry::Field(std::borrow::Cow::Borrowed("severityNumber")),
        DataType::I64,
    ),
    (Entry::Field(std::borrow::Cow::Borrowed("severityText")), DataType::Str),
    (Entry::Field(std::borrow::Cow::Borrowed("body")), DataType::Variant),
    (
        Entry::Field(std::borrow::Cow::Borrowed("attributes")),
        DataType::Variant,
    ),
    (Entry::Field(std::borrow::Cow::Borrowed("traceId")), DataType::Str),
    (Entry::Field(std::borrow::Cow::Borrowed("spanId")), DataType::Str),
    (Entry::Field(std::borrow::Cow::Borrowed("flags")), DataType::I64),
    (Entry::Field(std::borrow::Cow::Borrowed("eventName")), DataType::Str),
    (
        Entry::Field(std::borrow::Cow::Borrowed("droppedAttributesCount")),
        DataType::I64,
    ),
]);

/// An OTLP span, each field typed as OTLP types it: `kind` and `statusCode` its enums' numbers, `attributes` and the
/// `events` and `links` lists variants that keep each value's own type. Each field is null where the span leaves it
/// unset.
const OTLP_SPAN: RecordFields = std::borrow::Cow::Borrowed(&[
    (Entry::Field(std::borrow::Cow::Borrowed("traceId")), DataType::Str),
    (Entry::Field(std::borrow::Cow::Borrowed("spanId")), DataType::Str),
    (Entry::Field(std::borrow::Cow::Borrowed("parentSpanId")), DataType::Str),
    (Entry::Field(std::borrow::Cow::Borrowed("traceState")), DataType::Str),
    (Entry::Field(std::borrow::Cow::Borrowed("flags")), DataType::I64),
    (Entry::Field(std::borrow::Cow::Borrowed("name")), DataType::Str),
    (Entry::Field(std::borrow::Cow::Borrowed("kind")), DataType::I64),
    (
        Entry::Field(std::borrow::Cow::Borrowed("startTime")),
        DataType::DateTime,
    ),
    (Entry::Field(std::borrow::Cow::Borrowed("endTime")), DataType::DateTime),
    (
        Entry::Field(std::borrow::Cow::Borrowed("attributes")),
        DataType::Variant,
    ),
    (
        Entry::Field(std::borrow::Cow::Borrowed("droppedAttributesCount")),
        DataType::I64,
    ),
    (Entry::Field(std::borrow::Cow::Borrowed("events")), DataType::Variant),
    (
        Entry::Field(std::borrow::Cow::Borrowed("droppedEventsCount")),
        DataType::I64,
    ),
    (Entry::Field(std::borrow::Cow::Borrowed("links")), DataType::Variant),
    (
        Entry::Field(std::borrow::Cow::Borrowed("droppedLinksCount")),
        DataType::I64,
    ),
    (Entry::Field(std::borrow::Cow::Borrowed("statusCode")), DataType::I64),
    (Entry::Field(std::borrow::Cow::Borrowed("statusMessage")), DataType::Str),
]);

fn array(ty: DataType) -> DataType {
    DataType::Array(Box::new(ty))
}

fn closed(fields: &[(&str, DataType)]) -> DataType {
    DataType::Struct(
        fields.iter().map(|(name, ty)| (name.to_string(), ty.clone())).collect(),
        false,
    )
}

/// The fields `name: ty` declares, onto `fields`: itself, or for an object each leaf under it, then the entry of its
/// unclaimed fields where it is open. Err for an object that declares nothing.
pub(crate) fn leaves(name: &str, ty: &DataType, fields: &mut Vec<(Entry, DataType)>) -> Result<(), String> {
    let DataType::Struct(members, open) = ty else {
        fields.push((Entry::Field(name.to_string().into()), ty.clone()));
        return Ok(());
    };
    if members.is_empty() && !open {
        return Err(format!(
            "`{name}` is declared as `{{}}`, which holds no field. Name its fields, or declare it `{{...}}` to keep \
             whatever keys it has."
        ));
    }
    for (key, member) in members {
        leaves(&crate::plan::tree::child(name, key), member, fields)?;
    }
    if *open {
        fields.push((Entry::Unclaimed(name.to_string().into()), DataType::Variant));
    }
    Ok(())
}

/// One data point of an OTLP metric, with its metric's name, description, unit and `type`: `gauge`, `sum`,
/// `histogram`, `exponentialHistogram` or `summary`. `value` is a gauge's or sum's number, an int or a double as sent;
/// `count`, `sum`, `min` and `max` a distribution's; the buckets and quantiles typed; and the exemplars a variant. Each
/// field is null where its point's type has none or leaves it unset. A distribution's counts are doubles, as a
/// Prometheus histogram's may be, so a pipeline reads a histogram from either alike; a double holds every whole count
/// to 2^53 exactly.
fn otlp_metric_point() -> RecordFields {
    use std::borrow::Cow::Borrowed;
    std::borrow::Cow::Owned(vec![
        (Entry::Field(Borrowed("name")), DataType::Str),
        (Entry::Field(Borrowed("description")), DataType::Str),
        (Entry::Field(Borrowed("unit")), DataType::Str),
        (Entry::Field(Borrowed("type")), DataType::Str),
        (Entry::Field(Borrowed("metadata")), DataType::Variant),
        (Entry::Field(Borrowed("aggregationTemporality")), DataType::I64),
        (Entry::Field(Borrowed("isMonotonic")), DataType::Bool),
        (Entry::Field(Borrowed("startTime")), DataType::DateTime),
        (Entry::Field(Borrowed("time")), DataType::DateTime),
        (Entry::Field(Borrowed("attributes")), DataType::Variant),
        (Entry::Field(Borrowed("flags")), DataType::I64),
        (Entry::Field(Borrowed("value")), DataType::Variant),
        (Entry::Field(Borrowed("count")), DataType::F64),
        (Entry::Field(Borrowed("sum")), DataType::F64),
        (Entry::Field(Borrowed("min")), DataType::F64),
        (Entry::Field(Borrowed("max")), DataType::F64),
        (Entry::Field(Borrowed("bucketCounts")), array(DataType::F64)),
        (Entry::Field(Borrowed("explicitBounds")), array(DataType::F64)),
        (Entry::Field(Borrowed("scale")), DataType::I64),
        (Entry::Field(Borrowed("zeroCount")), DataType::F64),
        (Entry::Field(Borrowed("zeroThreshold")), DataType::F64),
        (Entry::Field(Borrowed("positive.offset")), DataType::I64),
        (Entry::Field(Borrowed("positive.bucketCounts")), array(DataType::F64)),
        (Entry::Field(Borrowed("negative.offset")), DataType::I64),
        (Entry::Field(Borrowed("negative.bucketCounts")), array(DataType::F64)),
        (
            Entry::Field(Borrowed("quantileValues")),
            array(closed(&[("quantile", DataType::F64), ("value", DataType::F64)])),
        ),
        (Entry::Field(Borrowed("exemplars")), DataType::Variant),
    ])
}

/// An OTLP receiver's record names its sender, and the resource and instrumentation scope its record came under:
/// the resource's attributes, which each sender picks, and the scope's name, version and attributes. Each is null
/// where the export named none.
/// Its record is its signal's columns, so there is no payload for `__source.bytes` to read.
const OTLP_RECEIVER_FIELDS: &[(&str, DataType)] = &[
    ("__source.name", DataType::Str),
    ("__source.type", DataType::Str),
    ("__source.remote", DataType::Str),
    ("__source.resource", DataType::Variant),
    ("__source.scope.name", DataType::Str),
    ("__source.scope.version", DataType::Str),
    ("__source.scope.attributes", DataType::Variant),
];

/// A Prometheus sample or native histogram: its series' metric name, its time, and the series' other labels as an
/// object of strings. Its time is milliseconds as sent, in microseconds. Each field is an OTLP metric point's of the
/// same name and type, so a pipeline reads a point from either alike. A sample's `value` is a variant holding a
/// double. A native histogram has no `value`: its `type` is `exponentialHistogram`, or `histogram` where it has
/// custom buckets, with its `count`, `sum` and buckets. `aggregationTemporality` is 2 (cumulative) for a counter's
/// histogram and null for a gauge histogram. `resetHint` is Prometheus's own, which OTLP has none of: `yes` where the
/// counter reset since the histogram before, `no` where it did not, null where the sender did not say. `exemplars`
/// are its series' exemplars, on its last record of the request; a series sent with exemplars alone is one record of
/// them, its `value` null. `startTime` is when its series started counting, which remote_write 2.0 may say and 1.0
/// never does, in microseconds; null where unsaid.
///
/// A family's metadata is a record of its own, its `time` and `value` null, since metadata has none: `name` the
/// family's, `description` its help, `unit`, and `type` as OTLP names it, a counter a `sum` with `isMonotonic` true
/// and a gauge histogram a `histogram` with no `aggregationTemporality`. Prometheus's `info` and `stateset`, which
/// OTLP has none of, keep their own names; an unknown type is null.
fn prometheus_record() -> RecordFields {
    use std::borrow::Cow::Borrowed;
    std::borrow::Cow::Owned(vec![
        (Entry::Field(Borrowed("time")), DataType::DateTime),
        (Entry::Field(Borrowed("name")), DataType::Str),
        (Entry::Field(Borrowed("value")), DataType::Variant),
        (Entry::Field(Borrowed("attributes")), DataType::Variant),
        (Entry::Field(Borrowed("type")), DataType::Str),
        (Entry::Field(Borrowed("aggregationTemporality")), DataType::I64),
        (Entry::Field(Borrowed("count")), DataType::F64),
        (Entry::Field(Borrowed("sum")), DataType::F64),
        (Entry::Field(Borrowed("bucketCounts")), array(DataType::F64)),
        (Entry::Field(Borrowed("explicitBounds")), array(DataType::F64)),
        (Entry::Field(Borrowed("scale")), DataType::I64),
        (Entry::Field(Borrowed("zeroCount")), DataType::F64),
        (Entry::Field(Borrowed("zeroThreshold")), DataType::F64),
        (Entry::Field(Borrowed("positive.offset")), DataType::I64),
        (Entry::Field(Borrowed("positive.bucketCounts")), array(DataType::F64)),
        (Entry::Field(Borrowed("negative.offset")), DataType::I64),
        (Entry::Field(Borrowed("negative.bucketCounts")), array(DataType::F64)),
        (Entry::Field(Borrowed("resetHint")), DataType::Str),
        (Entry::Field(Borrowed("exemplars")), DataType::Variant),
        (Entry::Field(Borrowed("description")), DataType::Str),
        (Entry::Field(Borrowed("unit")), DataType::Str),
        (Entry::Field(Borrowed("isMonotonic")), DataType::Bool),
        (Entry::Field(Borrowed("startTime")), DataType::DateTime),
    ])
}

/// A Prometheus receiver's record names its sender. Its record is [`prometheus_record`]'s columns, so there is no
/// payload for `__source.bytes` to read.
const PROMETHEUS_RECEIVER_FIELDS: &[(&str, DataType)] = &[
    ("__source.name", DataType::Str),
    ("__source.type", DataType::Str),
    ("__source.remote", DataType::Str),
];

/// An API source's record names the page it was read from: the address asked, which is its record's topic.
const API_FIELDS: &[(&str, DataType)] = &[
    ("__source.name", DataType::Str),
    ("__source.type", DataType::Str),
    ("__source.url", DataType::Str),
    (SOURCE_BYTES, DataType::Bytes),
];

/// An SQL source's record names the database it was read from, `host:port/database`, which is its record's topic.
/// Its record is the fields its source declares, so it has no payload for `__source.bytes`.
const SQL_FIELDS: &[(&str, DataType)] = &[
    ("__source.name", DataType::Str),
    ("__source.type", DataType::Str),
    ("__source.database", DataType::Str),
];

impl SourceMeta {
    /// What a Kafka record carries. Also what a pipeline no route reaches is checked against, so an unrouted pipeline
    /// is never rejected for a field the source a future route names would have supplied.
    pub const KAFKA: SourceMeta = SourceMeta::new("kafka", PAYLOAD_RECORD, KAFKA_FIELDS);

    /// What an Event Hubs record carries: a Kafka record's fields, with the event hub as its topic.
    pub const EVENT_HUBS: SourceMeta = SourceMeta::new("eventHubs", PAYLOAD_RECORD, KAFKA_FIELDS);

    /// What a metrics record carries: its source's name and kind, and the topic it is stamped with.
    pub const METRICS: SourceMeta = SourceMeta::new("metrics", PAYLOAD_RECORD, METRICS_FIELDS);

    /// What a logs record carries, which is the same.
    pub const LOGS: SourceMeta = SourceMeta::new("logs", PAYLOAD_RECORD, LOGS_FIELDS);

    /// What an SQS/S3 record carries: the object it was read from, and its record number there.
    pub const S3_SQS: SourceMeta = SourceMeta::new("s3Sqs", PAYLOAD_RECORD, OBJECT_FIELDS);

    /// What a Listing/S3 record carries: the same as an SQS/S3 record.
    pub const S3_LIST: SourceMeta = SourceMeta::new("s3List", PAYLOAD_RECORD, OBJECT_FIELDS);

    /// What a Listing/GCS record carries: the same as an SQS/S3 record.
    pub const GCS_LIST: SourceMeta = SourceMeta::new("gcsList", PAYLOAD_RECORD, OBJECT_FIELDS);

    /// What a Listing/Azure Blob record carries: the same, with the container as its bucket.
    pub const AZURE_BLOB_LIST: SourceMeta = SourceMeta::new("azureBlobList", PAYLOAD_RECORD, OBJECT_FIELDS);

    /// What an SFTP record carries: the same, with the directory as its bucket and the path under it as its key.
    pub const SFTP: SourceMeta = SourceMeta::new("sftp", PAYLOAD_RECORD, OBJECT_FIELDS);

    /// What a Queue/Azure Blob record carries: the same, with the container as its bucket.
    pub const AZURE_BLOB_QUEUE: SourceMeta = SourceMeta::new("azureBlobQueue", PAYLOAD_RECORD, OBJECT_FIELDS);

    /// What a Pub/Sub/GCS record carries: the same.
    pub const GCS_PUB_SUB: SourceMeta = SourceMeta::new("gcsPubSub", PAYLOAD_RECORD, OBJECT_FIELDS);

    /// What an HTTP API record carries: the address of the page it was read from.
    pub const HTTP_API: SourceMeta = SourceMeta::new("httpApi", PAYLOAD_RECORD, API_FIELDS);

    /// What an Okta System Log record carries: the same.
    pub const OKTA_LOGS: SourceMeta = SourceMeta::new("oktaLogs", PAYLOAD_RECORD, API_FIELDS);

    /// What a PostgreSQL source's record carries before it declares its fields, which every one does.
    pub const POSTGRES: SourceMeta = SourceMeta::new("postgres", PAYLOAD_RECORD, SQL_FIELDS);

    /// What a MySQL or MariaDB source's record carries before it declares its fields, which every one does.
    pub(crate) const MYSQL: SourceMeta = SourceMeta::new("mysql", PAYLOAD_RECORD, SQL_FIELDS);

    /// What an HTTP receiver's record carries: the address of the sender that delivered it.
    pub const HTTP_RECEIVER: SourceMeta = SourceMeta::new("httpReceiver", PAYLOAD_RECORD, RECEIVER_FIELDS);

    /// What a HEC receiver's record carries: its sender's address, and what the sender filed its event under.
    pub const HEC_RECEIVER: SourceMeta = SourceMeta::new("hecReceiver", PAYLOAD_RECORD, HEC_RECEIVER_FIELDS);

    /// What an S2S receiver's record carries: its forwarder's address, and what the forwarder filed its event under.
    pub const S2S_RECEIVER: SourceMeta = SourceMeta::new("s2sReceiver", PAYLOAD_RECORD, HEC_RECEIVER_FIELDS);

    /// What an Elasticsearch receiver's record carries: a document, its sender's address, and what its action named.
    pub const ELASTIC_RECEIVER: SourceMeta =
        SourceMeta::new("elasticReceiver", PAYLOAD_RECORD, ELASTIC_RECEIVER_FIELDS);

    /// What a socket receiver carries: a message's bytes, and its sender's address.
    pub const SOCKET_RECEIVER: SourceMeta = SourceMeta::new("socketReceiver", PAYLOAD_RECORD, RECEIVER_FIELDS);

    /// What an OTLP receiver of logs carries: a log record, its sender's address, and its resource and scope.
    pub const OTLP_LOGS: SourceMeta = SourceMeta::new("otlpReceiver", OTLP_LOG, OTLP_RECEIVER_FIELDS);

    /// What an OTLP receiver of traces carries: a span, its sender's address, and its resource and scope.
    pub const OTLP_TRACES: SourceMeta = SourceMeta::new("otlpReceiver", OTLP_SPAN, OTLP_RECEIVER_FIELDS);

    /// What an OTLP receiver of `signal` carries: a log record, a metric's data point or a span, its sender's address,
    /// and its resource and scope.
    pub fn otlp(signal: Signal) -> SourceMeta {
        match signal {
            Signal::Logs => SourceMeta::OTLP_LOGS,
            // A data point's record holds arrays and structs, which no `const` can build.
            Signal::Metrics => SourceMeta::new("otlpReceiver", otlp_metric_point(), OTLP_RECEIVER_FIELDS),
            Signal::Traces => SourceMeta::OTLP_TRACES,
        }
    }

    /// What a Prometheus receiver's record carries: one sample or native histogram, and its sender's address.
    // Its record holds arrays and structs, which no `const` can build.
    pub fn prometheus_receiver() -> SourceMeta {
        SourceMeta::new("prometheusReceiver", prometheus_record(), PROMETHEUS_RECEIVER_FIELDS)
    }

    /// The kind `__source.type` names, or `None` for a kind no source has.
    pub fn of_kind(kind: &str) -> Option<SourceMeta> {
        [
            SourceMeta::KAFKA,
            SourceMeta::EVENT_HUBS,
            SourceMeta::METRICS,
            SourceMeta::LOGS,
            SourceMeta::S3_SQS,
            SourceMeta::S3_LIST,
            SourceMeta::GCS_LIST,
            SourceMeta::AZURE_BLOB_LIST,
            SourceMeta::SFTP,
            SourceMeta::AZURE_BLOB_QUEUE,
            SourceMeta::GCS_PUB_SUB,
            SourceMeta::HTTP_API,
            SourceMeta::OKTA_LOGS,
            SourceMeta::POSTGRES,
            SourceMeta::MYSQL,
            SourceMeta::HTTP_RECEIVER,
            SourceMeta::HEC_RECEIVER,
            SourceMeta::S2S_RECEIVER,
            SourceMeta::OTLP_LOGS,
            SourceMeta::prometheus_receiver(),
            SourceMeta::SOCKET_RECEIVER,
            SourceMeta::ELASTIC_RECEIVER,
        ]
        .into_iter()
        .find(|m| m.kind == kind)
    }

    /// A source whose records carry nothing beyond their payload.
    pub const NONE: SourceMeta = SourceMeta::new("", PAYLOAD_RECORD, &[(SOURCE_BYTES, DataType::Bytes)]);

    /// This meta with `record` as its record's fields in place of `_raw`, and without `__source.bytes`. A field's name is a path, as a pipeline
    /// writes one (`user.name`, `labels["a.b"]`). Err for no fields, a name the engine keeps for itself (`__live`,
    /// `__source`), or two names where one is the other or holds it: each would take a slot another reads.
    pub fn declaring(self, record: Vec<(String, DataType)>) -> Result<SourceMeta, String> {
        if record.is_empty() {
            return Err("a source that declares its record declares at least one field".into());
        }
        self.declare(record, |_| false)
    }

    /// This meta reading each payload as Avro into `record`, open (`opened`). It may declare no field, and keeps
    /// every top-level field then. Its batches are payloads, so it keeps `__source.bytes`.
    pub(crate) fn decoding_avro(self, record: Vec<(String, DataType)>) -> Result<SourceMeta, String> {
        let source = self.source;
        Ok(SourceMeta {
            source,
            format: Format::Avro,
            ..self.declare(record, |_| false)?.opened()
        })
    }

    /// This meta with its record open: every top-level field of a payload no declared field claims is kept, as the
    /// root's unclaimed fields.
    pub(crate) fn opened(mut self) -> SourceMeta {
        let mut record = self.record.into_owned();
        record.push((Entry::Unclaimed("".into()), DataType::Variant));
        self.record = record.into();
        self
    }

    /// Whether this meta's record keeps the fields it does not declare (`opened`).
    pub fn is_open(&self) -> bool {
        self.record.contains(&(Entry::Unclaimed("".into()), DataType::Variant))
    }

    /// `declaring`, allowing a field under an internal key where `internal` says so. A field declared as an object is
    /// its leaves, each a field of the record, and the object's unclaimed fields where it is open (Language).
    fn declare(self, record: Vec<(String, DataType)>, internal: fn(&str) -> bool) -> Result<SourceMeta, String> {
        use crate::plan::tree;
        let mut fields: Vec<(Entry, DataType)> = Vec::with_capacity(record.len());
        // Each field's place, by its keys, and the name it was declared under.
        let mut places: Vec<(Vec<String>, bool, &str)> = Vec::with_capacity(record.len());
        for (name, ty) in &record {
            let unfit = |key: &std::borrow::Cow<str>| key.is_empty() || tree::is_internal(key) && !internal(key);
            if tree::unclaimed_object(name).is_some() || tree::segments(name).next().is_none_or(|key| unfit(&key)) {
                return Err(format!("`{name}` is not a name a record field can take"));
            }
            let from = fields.len();
            leaves(name, ty, &mut fields)?;
            for (leaf, _) in &fields[from..] {
                let (keys, object) = (leaf.keys(), leaf.field().is_none());
                // A leaf holds no other field, and an object's unclaimed fields sit under no leaf.
                let clash = |(other, others, _): &&(Vec<String>, bool, &str)| match (object, *others) {
                    (false, false) => other.starts_with(&keys) || keys.starts_with(other),
                    (true, false) => keys.starts_with(other),
                    (false, true) => other.starts_with(&keys),
                    (true, true) => *other == keys,
                };
                if let Some((_, _, other)) = places.iter().find(clash) {
                    return Err(format!("`{other}` and `{name}` are one field, or one holds the other"));
                }
                places.push((keys, object, name));
            }
        }
        // Its chunks are the record's columns, so there is no payload for `__source.bytes` to read.
        let source = match self.source.split_last() {
            Some(((SOURCE_BYTES, _), rest)) => rest,
            _ => self.source,
        };
        Ok(SourceMeta {
            record: fields.into(),
            source,
            ..self
        })
    }

    /// The meta of a sample captured under `columns`: each column a field of its record, read from the record object by
    /// its path and typed as spelled, `__source` and `__destination` columns as much as any other. A sample that names
    /// no field, a paste, is `_raw` text; the instant it was taken at is the preview's, not a field of its records.
    pub fn of_columns(columns: &[crate::heartbeat::CaptureColumn]) -> Result<SourceMeta, String> {
        let taken_at = |c: &crate::heartbeat::CaptureColumn| c.name == crate::heartbeat::CaptureBatch::TAKEN_AT;
        if columns
            .iter()
            .all(|c| c.name == crate::plan::tree::UNCLAIMED || taken_at(c))
        {
            return Ok(SourceMeta::NONE);
        }
        let mut open = false;
        let mut record = Vec::with_capacity(columns.len());
        for c in columns {
            match crate::plan::tree::unclaimed_object(&c.name) {
                Some("") => open = true,
                // A column of an object's unclaimed fields declares that object open.
                Some(object) => record.push((object.to_string(), DataType::Struct(Vec::new(), true))),
                None => {
                    let ty = crate::expr::parse::parse_type(&c.ty)
                        .map_err(|e| format!("column `{}` has type {:?}, which does not read: {e}", c.name, c.ty))?;
                    record.push((c.name.clone(), ty));
                }
            }
        }
        // `__live` is the one name a pipeline cannot take a value of from its input: the engine primes it.
        let meta = SourceMeta::NONE.declare(record, |key| key != "__live")?;
        Ok(if open { meta.opened() } else { meta })
    }

    /// The record's entries, in slot order.
    pub fn record(&self) -> &RecordFields {
        &self.record
    }

    /// Each of the record's fields, with its type, in the record's order.
    pub fn fields(&self) -> impl Iterator<Item = (&str, &DataType)> {
        self.record.iter().filter_map(|(entry, ty)| Some((entry.field()?, ty)))
    }

    pub(crate) fn record_len(&self) -> usize {
        self.record.len()
    }

    /// This meta for a source that declares `encoding`. A source that reads bytes and declares `none` decodes
    /// nothing, so its record has no `_raw`. A source that reads no bytes has nothing to decode, so its meta keeps
    /// none.
    pub fn with_encoding(self, encoding: Encoding) -> SourceMeta {
        match encoding {
            _ if !self.reads_text() => self,
            Encoding::None => SourceMeta {
                record: std::borrow::Cow::Borrowed(&[]),
                encoding,
                ..self
            },
            _ => SourceMeta { encoding, ..self },
        }
    }

    /// How this source's payloads are decoded.
    pub fn format(&self) -> Format {
        self.format
    }

    /// Whether this source's record is `_raw`: each payload read as text in the source's text encoding.
    pub fn raw_record(&self) -> bool {
        self.reads_text() && !self.record.is_empty()
    }

    /// Whether this source's payloads are text.
    fn reads_text(&self) -> bool {
        self.reads_bytes() && self.format == Format::Text
    }

    /// Whether a stored record of this source -- a sample's -- is read as its fields. An Avro payload names a writer's
    /// schema only the worker that read it holds, so its sample keeps the fields it was shown with.
    pub(crate) fn stored_as_fields(&self) -> bool {
        !self.reads_bytes() || self.format == Format::Avro
    }

    /// Whether this source hands over each record's payload as it read it, as `__source.bytes`: its batches are
    /// payloads, which transform decodes into its record by its format. A source that does not hands over its
    /// record's columns.
    pub fn reads_bytes(&self) -> bool {
        self.source.iter().any(|(name, _)| *name == SOURCE_BYTES)
    }

    /// The type of `field`, a field of the record or a `__source` field, or `None` where this source declares none.
    pub fn field_type(&self, field: &str) -> Option<DataType> {
        self.fields()
            .chain(self.source.iter().map(|(n, t)| (*n, t)))
            .find(|(n, _)| *n == field)
            .map(|(_, t)| t.clone())
    }

    /// The `__source` fields, in slot order.
    pub fn source_fields(&self) -> &'static [(&'static str, DataType)] {
        self.source
    }

    /// The slot `__source` field `field` occupies.
    pub fn source_slot(&self, field: usize) -> crate::core::id::SlotId {
        crate::core::id::SlotId::at(crate::core::batch::PREFIX_SLOTS + self.record_len() + field)
    }

    /// The `__source` field at `slot`, if one is there.
    fn source_field_at(&self, slot: crate::core::id::SlotId) -> Option<usize> {
        slot.index()
            .checked_sub(self.source_slot(0).index())
            .filter(|i| *i < self.source.len())
    }

    /// Field `field` as a column over `records`, in record order, for records `source` read: its name in the
    /// deployment, `None` where there is no deployment to name it. A property of the kind, not of a live `Source`: the
    /// values ride on `SourceRecord`, so transform builds these columns without touching anything ingest owns. Keyed
    /// by the field's name rather than its position, so an entry can move within a source's list without its
    /// extractor staying behind at the old index.
    ///
    /// Each row whose value had bytes that did not decode, replaced with U+FFFD, is pushed to `repaired`.
    fn column(
        &self,
        field: usize,
        records: &[crate::io::SourceRecord],
        source: Option<&str>,
        pool: &mut crate::core::buffer::BufferPool,
        repaired: &mut Vec<u32>,
    ) -> Column {
        match self.source[field].0 {
            "__source.name" => str_column(pool, records.iter().map(|_| source.map(str::as_bytes))),
            "__source.type" => str_column(pool, records.iter().map(|_| Some(self.kind.as_bytes()))),
            f if TOPIC_FIELDS.contains(&f) => str_column(pool, records.iter().map(|r| Some(r.topic.as_bytes()))),
            "__source.bucket" => str_column(pool, records.iter().map(|r| Some(object_of(r).0))),
            "__source.key" => str_column(pool, records.iter().map(|r| Some(object_of(r).1))),
            "__source.partition" => i64_column(pool, records.iter().map(|r| r.partition as i64)),
            "__source.offset" => i64_column(pool, records.iter().map(|r| r.offset)),
            "__source.headers" => headers_column(pool, records, repaired),
            "__source.host"
            | "__source.source"
            | "__source.sourcetype"
            | "__source.index"
            | "__source.id"
            | "__source.pipeline"
            | "__source.scope.name"
            | "__source.scope.version" => {
                let key = &self.source[field].0["__source.".len()..];
                if let Some(shared) = shared_filed(records) {
                    let rows = records.len();
                    let Some(value) = filed_at(shared, key) else {
                        return Column::empty(DataType::Str, rows);
                    };
                    let text = String::from_utf8_lossy(value);
                    if matches!(text, std::borrow::Cow::Owned(_)) {
                        repaired.extend(0..rows as u32);
                    }
                    return Column::constant(Scalar::Str(text.into_owned()), rows);
                }
                let values: Vec<Option<&[u8]>> = records.iter().map(|r| filed(r, key)).collect();
                repaired.extend(
                    (values.iter().enumerate())
                        .filter(|(_, v)| v.is_some_and(|v| std::str::from_utf8(v).is_err()))
                        .map(|(row, _)| row as u32),
                );
                let text: Vec<Option<std::borrow::Cow<'_, str>>> =
                    values.iter().map(|v| v.map(String::from_utf8_lossy)).collect();
                str_column(pool, text.iter().map(|t| t.as_deref().map(str::as_bytes)))
            }
            "__source.time" => {
                let mut col = Column::alloc(pool, &DataType::DateTime, records.len());
                let out = col.datetime_mut().expect("a datetime column");
                for (row, r) in records.iter().enumerate() {
                    let micros = r.chunk.time(r.row as usize).or_else(|| {
                        filed(r, crate::io::hec::TIME).and_then(|v| std::str::from_utf8(v).ok()?.parse::<i64>().ok())
                    });
                    match micros {
                        Some(micros) => out.values[row] = micros,
                        None => out.validity.set_valid(row, false),
                    }
                }
                col
            }
            "__source.fields" | "__source.resource" | "__source.scope.attributes" => {
                let key = &self.source[field].0["__source.".len()..];
                if let Some(shared) = shared_filed(records) {
                    return match filed_at(shared, key) {
                        Some(value) => {
                            Column::constant(Scalar::Variant(EncodedBuf::check(value.to_vec())), records.len())
                        }
                        None => Column::empty(DataType::Variant, records.len()),
                    };
                }
                var_column(
                    pool,
                    crate::core::column::VarType::Variant,
                    records.iter().map(|r| filed(r, key)),
                )
            }
            SOURCE_BYTES => {
                let values: Vec<Option<&[u8]>> = self.read_bytes_each(records).collect();
                var_column(pool, crate::core::column::VarType::Bytes, values.into_iter())
            }
            other => unreachable!("no extractor for declared field {other}"),
        }
    }
}

impl SourceMeta {
    /// Put this meta's columns over `records` into `batch`: every field of the record, and every `__source` field: a
    /// route condition may test any of them before a pipeline is chosen. `text` is each record's payload decoded, read where the record is `_raw`;
    /// `writers` holds the schemas an Avro source's payloads name; `source` is what `__source.name` reads.
    ///
    /// Returns each value ingest could not take as it came, as its row, its slot, how, and the value as read: a
    /// `__source` value repaired to valid UTF-8, a payload that does not decode, or a record value that is not its
    /// field's type and is null. With them, the writer's schema an Avro source's last payload was read with.
    pub fn ingest<T: AsRef<str>>(
        &self,
        batch: &mut Batch,
        records: &[crate::io::SourceRecord],
        text: &[T],
        writers: Option<&crate::io::avro::Writers>,
        source: Option<&str>,
        pool: &mut crate::core::buffer::BufferPool,
    ) -> (Vec<Noted>, Option<std::sync::Arc<crate::io::avro::Parsed>>) {
        let mut noted = Vec::new();
        let mut writer = None;
        if self.raw_record() {
            let values = text.iter().map(|t| Some(t.as_ref().as_bytes()));
            batch.put(crate::core::batch::SLOT_RAW, str_column(pool, values), pool);
        } else if let (Format::Avro, Some(writers)) = (self.format, writers) {
            let payloads: Vec<&[u8]> = self.read_bytes_each(records).map(Option::unwrap_or_default).collect();
            let read = crate::io::avro::decode(&self.record, writers, payloads.iter().copied(), pool);
            for (i, column) in read.columns.into_iter().enumerate() {
                batch.put(record_slot(i), column, pool);
            }
            let (mut unread, mut failed) = (read.unread.iter().peekable(), read.failed.iter().peekable());
            for row in 0..records.len() as u32 {
                let why = unread.next_if(|(r, _)| *r == row).map(|(_, why)| why.as_str());
                let fields = std::iter::from_fn(|| failed.next_if(|(r, _, _)| *r == row).map(|(_, f, v)| (*f, v)));
                self.note_read(&mut noted, row, why, fields);
            }
            writer = read.newest;
        } else if self.stored_as_fields() {
            let (chunk, rows) = chunk_rows(records);
            for (i, (_, ty)) in self.record.iter().enumerate() {
                let column = match &chunk {
                    Some(chunk) => chunk.column(i).gather(pool, &rows),
                    None => Column::empty(ty.clone(), 0),
                };
                batch.put(record_slot(i), column, pool);
            }
            if let Some(chunk) = &chunk {
                for (at, &row) in rows.iter().enumerate() {
                    let from = chunk.failed().partition_point(|(r, _, _)| *r < row);
                    let failed = chunk.failed()[from..].iter().take_while(|(r, _, _)| *r == row);
                    self.note_read(&mut noted, at as u32, None, failed.map(|(_, f, v)| (*f, v)));
                }
            }
        }
        let mut repaired = Vec::new();
        for i in 0..self.source.len() {
            let slot = self.source_slot(i);
            batch.put(slot, self.column(i, records, source, pool, &mut repaired), pool);
            noted.extend(
                repaired
                    .drain(..)
                    .map(|row| (row, slot, ExceptionKind::InvalidUtf8, None)),
            );
        }
        (noted, writer)
    }

    /// Notes row `row`'s payload that did not read, and why, or each value of its that is not its field's type, by
    /// the field's index, as the payload spelled it.
    fn note_read<'v>(
        &self,
        noted: &mut Vec<Noted>,
        row: u32,
        unread: Option<&str>,
        failed: impl Iterator<Item = (usize, &'v String)>,
    ) {
        // A payload that did not read has no field of its own to blame, so its record's first holds it.
        if let Some(why) = unread {
            noted.push((row, record_slot(0), ExceptionKind::Unparsable, Some(why.to_string())));
            return;
        }
        for (field, value) in failed {
            let kind = ExceptionKind::UnparsableField {
                to: self.record[field].1.clone(),
            };
            noted.push((row, record_slot(field), kind, Some(value.clone())));
        }
    }
}

/// The slot of the record's field `field`.
fn record_slot(field: usize) -> crate::core::id::SlotId {
    crate::core::id::SlotId::at(crate::core::batch::PREFIX_SLOTS + field)
}

/// What decoding could not take as it came: the row, the slot, the kind of exception, and the value as read.
pub type Noted = (u32, crate::core::id::SlotId, ExceptionKind, Option<String>);

/// A batch's records as columns, decoded once. Routing reads them, each route's batch takes its own, and whatever
/// shows a record as it arrived reads it here.
pub struct DecodedBatch {
    /// The prefix column `__live`, primed all-true, a column for each field of the record, and every `__source` field
    /// the source declares.
    pub batch: Batch,
    /// What `SourceMeta::decode` noted while building them.
    pub noted: Vec<Noted>,
    /// What decoding a payload's text repaired or guessed, by row.
    pub repairs: Vec<(u32, ExceptionKind)>,
    /// The writers' schemas an Avro source's payloads were decoded with.
    pub writers: Option<std::sync::Arc<crate::io::avro::Writers>>,
    /// Of them, the one its last payload that read was read with.
    pub writer: Option<std::sync::Arc<crate::io::avro::Parsed>>,
}

impl SourceMeta {
    /// `records` as columns. Each payload is decoded by the source's format: as text in its text encoding into
    /// `_raw`, unless `text` already holds that, as a sample's does; or as Avro with the schemas in `writers`.
    /// `source` is what `__source.name` reads.
    pub fn decode<T: AsRef<str>>(
        &self,
        records: &[crate::io::SourceRecord],
        text: Option<&[T]>,
        writers: Option<&std::sync::Arc<crate::io::avro::Writers>>,
        source: Option<&str>,
        pool: &mut crate::core::buffer::BufferPool,
    ) -> DecodedBatch {
        let (writers, of) = (writers.cloned(), writers.map(|w| &**w));
        let rows = records.len();
        let layout: Vec<DataType> = ingest_fields(self).into_iter().map(|(_, t)| t).collect();
        let mut batch = Batch::new(&layout, rows, 0);
        let mut live = Column::alloc(pool, &DataType::Bool, rows);
        crate::core::bits::fill_valid(live.bool_mut().expect("a Bool column").values, rows);
        batch.put(SLOT_LIVE, live, pool);

        let mut repairs = Vec::new();
        let (noted, writer) = match text {
            Some(text) => self.ingest(&mut batch, records, text, of, source, pool),
            None => {
                let mut text = Vec::with_capacity(rows);
                if self.raw_record() {
                    for (row, read) in self.read_bytes_each(records).enumerate() {
                        let decoded = self.encoding.decode(read.unwrap_or_default());
                        if decoded.replaced {
                            repairs.push((row as u32, ExceptionKind::InvalidUtf8));
                        }
                        if let Some(guess) = decoded.guessed {
                            repairs.push((row as u32, ExceptionKind::GuessedEncoding { encoding: guess.name() }));
                        }
                        text.push(decoded.text);
                    }
                }
                self.ingest(&mut batch, records, &text, of, source, pool)
            }
        };
        DecodedBatch {
            batch,
            noted,
            repairs,
            writers,
            writer,
        }
    }
}

impl DecodedBatch {
    /// Puts the record's fields and each `__source` field `wanted` names into `batch`, a route's. They are copied, or
    /// moved when `last` says nothing reads them here again. Returns what was noted against the slots it put.
    pub fn give(
        &mut self,
        batch: &mut Batch,
        meta: &SourceMeta,
        wanted: impl Fn(usize) -> bool,
        last: bool,
        pool: &mut crate::core::buffer::BufferPool,
    ) -> Vec<Noted> {
        let record =
            (0..meta.record_len()).map(|field| crate::core::id::SlotId::at(crate::core::batch::PREFIX_SLOTS + field));
        let source = (0..meta.source.len())
            .filter(|field| wanted(*field))
            .map(|field| meta.source_slot(field));
        let slots: Vec<crate::core::id::SlotId> = record.chain(source).collect();
        for (at, &slot) in slots.iter().enumerate() {
            // A bytes field of the record stays, for a destination that writes it as the source read it.
            let stays = at < meta.record_len() && self.batch.peek(slot).data_type() == DataType::Bytes;
            let column = if last && !stays {
                self.batch.take_dead(slot)
            } else {
                self.batch.peek(slot).deep_copy(pool)
            };
            batch.put(slot, column, pool);
        }
        let put = |note: &&Noted| slots.contains(&note.1);
        self.noted.iter().filter(put).cloned().collect()
    }

    /// The record's fields, as columns under the names `meta` declares for them.
    fn record<'a>(&'a self, meta: &'a SourceMeta) -> crate::format::record::RecordView<'a> {
        crate::format::record::RecordView::from_places(meta.record.iter().enumerate().map(|(field, (entry, _))| {
            let slot = crate::core::id::SlotId::at(crate::core::batch::PREFIX_SLOTS + field);
            (entry.place(), self.batch.peek(slot))
        }))
    }

    /// Row `row` as the document a pipeline with no steps would write for it.
    pub fn record_json(&self, meta: &SourceMeta, row: usize) -> Vec<u8> {
        self.record(meta).row(row)
    }

    /// Row `row` as its record object. A null field is left out, as a document leaves it out.
    pub fn record_object(&self, meta: &SourceMeta, row: usize) -> crate::io::RecordObject {
        serde_json::from_slice(&self.record_json(meta, row)).expect("a serialized row is a JSON object")
    }

    /// The text of row `row`'s field `field`, as a destination writing one field's text takes it. `None` for a
    /// field the record does not have, or a null.
    pub fn record_text(&self, meta: &SourceMeta, row: usize, field: &str) -> Option<Vec<u8>> {
        self.record(meta).text(field, row).map(String::into_bytes)
    }

    /// The bytes of row `row`'s field `field`, a `bytes` field of its record, as a destination writing that field's
    /// bytes takes them. `None` for a null, or for a field the record does not have (loading refuses a destination
    /// that names one, `check_bytes_fields`).
    pub fn record_bytes(&self, meta: &SourceMeta, row: usize, field: &str) -> Option<Vec<u8>> {
        let bytes = |(entry, t): &(Entry, DataType)| entry.field() == Some(field) && *t == DataType::Bytes;
        let slot = crate::core::id::SlotId::at(crate::core::batch::PREFIX_SLOTS + meta.record.iter().position(bytes)?);
        let column = self.batch.peek(slot);
        (column.is_valid(row)).then(|| column.varlen().expect("a bytes column").get(row).to_vec())
    }

    /// Each record's `_raw`, or no text for a record that has none.
    pub fn raw(&self, meta: &SourceMeta) -> Vec<&str> {
        let rows = self.batch.rows();
        if !meta.raw_record() {
            return vec![""; rows];
        }
        let text = self
            .batch
            .peek(crate::core::batch::SLOT_RAW)
            .str()
            .expect("`_raw` is a Str column");
        (0..rows).map(|row| text.get(row)).collect()
    }
}

impl SourceMeta {
    /// `record`'s payload as the source read it, before any decoding: its `__source.bytes`. `None` for a source
    /// that declares none.
    pub(crate) fn read_bytes<'r>(&self, record: &'r crate::io::SourceRecord) -> Option<&'r [u8]> {
        if self.reads_bytes() { record.bytes() } else { None }
    }

    /// `read_bytes` of each of `records`, looking up each chunk's bytes once rather than once a record.
    pub(crate) fn read_bytes_each<'r>(
        &self,
        records: &'r [crate::io::SourceRecord],
    ) -> impl Iterator<Item = Option<&'r [u8]>> + 'r {
        let reads = self.reads_bytes();
        let mut at: Option<(&'r crate::io::RecordChunk, Option<crate::core::column::VarLen<'r>>)> = None;
        records.iter().map(move |record| {
            if !reads {
                return None;
            }
            let chunk: &'r crate::io::RecordChunk = &record.chunk;
            if !at.as_ref().is_some_and(|(c, _)| std::ptr::eq(*c, chunk)) {
                at = Some((chunk, chunk.read()));
            }
            at.as_ref()
                .and_then(|(_, v)| v.as_ref())
                .map(|v| v.get(record.row as usize))
        })
    }
}

/// The chunk every one of `records` points into, and each record's row of it. A source hands one poll's rows in one
/// chunk (`io::SourceRecord::chunk`), and parking keeps a batch's rows in one, so a batch has one.
fn chunk_rows(records: &[crate::io::SourceRecord]) -> (Option<std::sync::Arc<crate::io::RecordChunk>>, Vec<u32>) {
    let mut chunk: Option<&std::sync::Arc<crate::io::RecordChunk>> = None;
    let rows = records
        .iter()
        .map(|r| {
            let first = chunk.get_or_insert(&r.chunk);
            assert!(
                std::sync::Arc::ptr_eq(first, &r.chunk),
                "the records of one batch point into one chunk"
            );
            r.row
        })
        .collect();
    (chunk.cloned(), rows)
}

fn str_column<'a>(
    pool: &mut crate::core::buffer::BufferPool,
    values: impl ExactSizeIterator<Item = Option<&'a [u8]>> + Clone,
) -> Column {
    var_column(pool, crate::core::column::VarType::Str, values)
}

fn var_column<'a>(
    pool: &mut crate::core::buffer::BufferPool,
    ty: crate::core::column::VarType,
    values: impl ExactSizeIterator<Item = Option<&'a [u8]>> + Clone,
) -> Column {
    let mut b = crate::core::column::StrBuilder::new(pool, ty, values.len());
    b.reserve(pool, values.clone().flatten().map(<[u8]>::len).sum());
    for v in values {
        match v {
            Some(v) => b.push(v),
            None => b.push_null(),
        }
    }
    b.finish()
}

/// What `record` is filed under at `key` (`io::hec::Filed`), which its source carries as a header, or its chunk for
/// every record of it.
fn filed<'a>(record: &'a crate::io::SourceRecord, key: &str) -> Option<&'a [u8]> {
    filed_at(
        record
            .headers
            .as_deref()
            .or_else(|| record.chunk.filed().map(Vec::as_slice))?,
        key,
    )
}

fn filed_at<'a>(filed: &'a [(String, Vec<u8>)], key: &str) -> Option<&'a [u8]> {
    filed.iter().find(|(k, _)| k == key).map(|(_, v)| v.as_slice())
}

/// What every record of `records` is filed under, where they share one chunk that carries it for them all. Each
/// value of it is then one column's every row.
fn shared_filed(records: &[crate::io::SourceRecord]) -> Option<&[(String, Vec<u8>)]> {
    let first = records.first()?;
    let shared = (records.iter()).all(|r| r.headers.is_none() && std::sync::Arc::ptr_eq(&r.chunk, &first.chunk));
    shared.then(|| first.chunk.filed().map(Vec::as_slice)).flatten()
}

/// Each record's headers as one variant object. A record with none is an empty object, so a read of a header it
/// lacks is null rather than a read of a missing object.
fn headers_column(
    pool: &mut crate::core::buffer::BufferPool,
    records: &[crate::io::SourceRecord],
    repaired: &mut Vec<u32>,
) -> Column {
    use crate::core::variant;
    let mut b = crate::core::column::StrBuilder::new(pool, crate::core::column::VarType::Variant, records.len());
    let mut out = Vec::new();
    for (row, r) in records.iter().enumerate() {
        out.clear();
        let open = variant::begin_object(&mut out);
        let mut lossy = false;
        let headers = r.headers.as_deref().unwrap_or_default();
        for (key, value) in headers {
            let text = String::from_utf8_lossy(value);
            lossy |= matches!(text, std::borrow::Cow::Owned(_));
            variant::put_key(&mut out, key);
            variant::put_str(&mut out, &text);
        }
        let count = match headers.len() {
            0 | 1 => headers.len(),
            _ => variant::dedupe(&mut out, open),
        };
        variant::end(&mut out, open, count);
        if lossy {
            repaired.push(row as u32);
        }
        b.push_grow(pool, &out);
    }
    b.finish()
}

fn i64_column(pool: &mut crate::core::buffer::BufferPool, values: impl ExactSizeIterator<Item = i64>) -> Column {
    let rows = values.len();
    let mut col = Column::alloc(pool, &DataType::I64, rows);
    for (slot, v) in col.i64_mut().unwrap().values.iter_mut().zip(values) {
        *slot = v;
    }
    col
}

// -- config schema ----------------------------------------------------------

#[derive(Clone, Debug)]
pub struct SourceConfig {
    pub id: SourceId,
    pub name: String,
    pub kind: SourceKind,
}

#[derive(Clone, Debug)]
pub enum SourceKind {
    Kafka(KafkaSourceConfig),
    /// The built-in self-observability source (design doc, Observability) — no
    /// broker, no credentials, just a drain interval.
    Metrics(MetricsSourceConfig),
    /// The other half of the same idea: what this process says about itself in sentences, as records. No broker
    /// and no credentials either -- what it reads has already happened here.
    Logs(crate::io::logs::LogsSourceConfig),
    /// The objects a queue announces: S3's on SQS, Azure Blob Storage's on a Storage queue, or Cloud Storage's on a
    /// Pub/Sub subscription.
    Objects(ObjectSourceConfig),
    /// What an API answers when it is asked, on a timer.
    Api(ApiSourceConfig),
    /// What senders deliver to an address every worker of the group listens on.
    Receiver(ReceiverConfig),
    /// The rows a query returns from a database, on a timer.
    Sql(SqlConfig),
}

impl SourceKind {
    /// The TLS this source speaks, where it speaks any of its own.
    pub fn tls(&self) -> Option<&Tls> {
        match self {
            SourceKind::Kafka(k) => k.tls.as_ref(),
            SourceKind::Objects(o) => o.tls.as_ref(),
            SourceKind::Api(a) => a.tls.as_ref(),
            SourceKind::Receiver(r) => r.tls.as_ref(),
            SourceKind::Sql(c) => c.database.tls(),
            SourceKind::Metrics(_) | SourceKind::Logs(_) => None,
        }
    }

    /// Sets a Kafka source's `fetchBytes`. Other kinds have none.
    pub(crate) fn set_fetch_bytes(&mut self, bytes: usize) {
        if let SourceKind::Kafka(k) = self {
            k.fetch_bytes = bytes;
        }
    }

    /// What a record from this kind carries, beyond its bytes.
    pub fn meta(&self) -> SourceMeta {
        self.declared_meta().with_encoding(self.encoding())
    }

    fn declared_meta(&self) -> SourceMeta {
        match self {
            SourceKind::Kafka(KafkaSourceConfig { avro: Some(a), .. })
            | SourceKind::Objects(ObjectSourceConfig { avro: Some(a), .. }) => a.meta.clone(),
            SourceKind::Kafka(k) => match k.service {
                KafkaService::Kafka => SourceMeta::KAFKA,
                KafkaService::EventHubs => SourceMeta::EVENT_HUBS,
            },
            SourceKind::Metrics(_) => SourceMeta::METRICS,
            SourceKind::Logs(_) => SourceMeta::LOGS,
            SourceKind::Objects(o) => match o.queue {
                ObjectQueue::Sqs { .. } => SourceMeta::S3_SQS,
                ObjectQueue::List { ref place, .. } => match place {
                    ListPlace::S3 { .. } => SourceMeta::S3_LIST,
                    ListPlace::Gcs(_) => SourceMeta::GCS_LIST,
                    ListPlace::Azure(_) => SourceMeta::AZURE_BLOB_LIST,
                    ListPlace::Sftp(_) => SourceMeta::SFTP,
                },
                ObjectQueue::StorageQueue { .. } => SourceMeta::AZURE_BLOB_QUEUE,
                ObjectQueue::PubSub { .. } => SourceMeta::GCS_PUB_SUB,
            },
            SourceKind::Api(a) => match a.api {
                Api::HttpApi(_) => SourceMeta::HTTP_API,
                Api::OktaLogs(_) => SourceMeta::OKTA_LOGS,
            },
            SourceKind::Sql(s) => s.meta.clone(),
            SourceKind::Receiver(r) => match r.receiver {
                Receiver::Http(_) => SourceMeta::HTTP_RECEIVER,
                Receiver::Hec(_) => SourceMeta::HEC_RECEIVER,
                Receiver::S2s(_) => SourceMeta::S2S_RECEIVER,
                Receiver::Otlp(signal) => SourceMeta::otlp(signal),
                Receiver::Prometheus => SourceMeta::prometheus_receiver(),
                Receiver::Socket(_) => SourceMeta::SOCKET_RECEIVER,
                Receiver::Elastic => SourceMeta::ELASTIC_RECEIVER,
            },
        }
    }

    /// How this source's records become text. The built-in sources write their own JSON, so they declare UTF-8
    /// rather than paying for detection or ever guessing.
    pub fn encoding(&self) -> Encoding {
        match self {
            SourceKind::Kafka(k) => k.encoding,
            SourceKind::Objects(o) => o.encoding,
            SourceKind::Receiver(ReceiverConfig {
                receiver: Receiver::Socket(s),
                ..
            }) => s.encoding,
            // An API answers in JSON, which is UTF-8, and so does what is sent to any other receiver.
            SourceKind::Metrics(_)
            | SourceKind::Logs(_)
            | SourceKind::Api(_)
            | SourceKind::Receiver(_)
            | SourceKind::Sql(_) => Encoding::Fixed(encoding_rs::UTF_8),
        }
    }

    /// The place this kind reads, when it is single-worker: its input has no protocol that divides it across a
    /// group, so one worker of the group reads it at a time, while the leader holds it for that worker, and resumes
    /// from what every destination acknowledged. The place keys that progress, so a source pointed somewhere else
    /// starts afresh. `None` is a kind whose transport assigns its work.
    pub fn single_worker(&self) -> Option<String> {
        match self {
            SourceKind::Objects(ObjectSourceConfig {
                queue: ObjectQueue::List { place, prefix, .. },
                ..
            }) => Some(match place {
                ListPlace::S3 { bucket, .. } => format!("s3://{bucket}/{prefix}"),
                ListPlace::Gcs(g) => format!("gs://{}/{prefix}", g.bucket),
                ListPlace::Azure(a) => format!("azure://{}/{}/{prefix}", a.access.account, a.container),
                ListPlace::Sftp(f) => format!("{}/{prefix}", f.address()),
            }),
            SourceKind::Api(a) => Some(match &a.api {
                Api::HttpApi(h) => h.url.clone(),
                // Its own, so progress another kind kept at the same address is never read as Okta's.
                Api::OktaLogs(o) => format!("okta:{}", o.logs_url()),
            }),
            SourceKind::Sql(s) => Some(s.place.clone()),
            SourceKind::Kafka(_)
            | SourceKind::Metrics(_)
            | SourceKind::Logs(_)
            | SourceKind::Objects(_)
            | SourceKind::Receiver(_) => None,
        }
    }

    /// The writers' schemas its payloads name, where it reads them as Avro: shared by the source and transform.
    pub fn avro_writers(&self) -> Option<std::sync::Arc<crate::io::avro::Writers>> {
        let avro = match self {
            SourceKind::Kafka(k) => k.avro.as_ref(),
            SourceKind::Objects(o) => o.avro.as_ref(),
            _ => None,
        };
        avro.map(|a| a.writers.clone())
    }

    /// The address a receiver listens on: every worker of the group binds it, and its supervisor holds it across a
    /// deploy. `None` for a kind that reaches out.
    pub fn listens(&self) -> Option<Listen> {
        match self {
            SourceKind::Receiver(r) => Some(r.listens()),
            _ => None,
        }
    }
}

/// What each source declares, for the compiler. Complete for every source a route names:
/// `LoadedSystem::compile_topology` builds it from the sources themselves, and a route naming a source the config does
/// not define is a load error.
pub type SourceMetas = HashMap<SourceId, SourceMeta>;

/// An unlisted source declares nothing. A default kind would compile clean against a source that is not that kind;
/// declaring nothing can only refuse a name, never mislay one.
fn meta_of(metas: &SourceMetas, source: SourceId) -> SourceMeta {
    metas.get(&source).cloned().unwrap_or(SourceMeta::NONE)
}

/// One credential a bundle names. The value is never here: a bundle is committed and read in review, and once the
/// repository may be an enterprise one it is not even smeltr's to keep a secret in.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SecretRef {
    /// An environment variable on the worker's own host, read when the client is built. The leader never holds this
    /// value, which is how an operator declines to hand it one.
    WorkerLocal(String),
    /// A value the leader holds, sealed (`leader::secrets`). `version` names which one, exactly: a deployment runs
    /// the version its bundle quotes or it does not run, so storing a value changes nothing until a commit says so.
    /// Bumping the number is a content change, which is what makes rotating a reviewed commit like any other.
    LeaderStored { name: String, version: i64 },
}

impl SecretRef {
    /// How to name it in a message an operator reads.
    pub fn describe(&self) -> String {
        match self {
            SecretRef::WorkerLocal(var) => format!("the environment variable {var}"),
            SecretRef::LeaderStored { name, version } => format!("the stored secret {name:?} at version {version}"),
        }
    }
}

/// The TLS a connector speaks: the certificate it presents, an entry of the leader's certificate list by name, and
/// the authorities it verifies the far end by.
///
/// A far end that asks for no certificate needs only the authorities, and one served from a public root needs only
/// the certificate, so either may be absent -- but not both, which `config_file::load` refuses. `authorities` absent
/// means the platform's own roots. The worker is sent the entry's newest chain and private key together, and the
/// client is built from them.
#[derive(Clone, Debug, PartialEq)]
pub struct Tls {
    /// The connector it belongs to, for a message an operator reads: `source "in"`.
    pub of: String,
    pub certificate: Option<String>,
    /// PEM, one or more roots.
    pub authorities: Option<String>,
    /// Whether a client under it also trusts the public roots (`io::trust`). A receiver never does.
    pub trust_public: bool,
    /// Which side presents `certificate`, which is what its leaf has to permit: a receiver to its senders, every
    /// other connector connecting out.
    pub side: crate::io::trust::Side,
    /// A receiver's: whom it admits by a sender's certificate besides `authorities`. A client's is empty.
    pub senders: Senders,
}

/// Whom a receiver admits by a sender's certificate (`io::senders`, RFC 5425 5): any whose certificate is one of
/// `fingerprints`, and, of those chaining to its authorities, only one issued to a host of `names` when it names any.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Senders {
    pub fingerprints: Vec<crate::io::senders::Fingerprint>,
    /// Host names, each maybe `*.` and a domain, which stands for any one label before it.
    pub names: Vec<String>,
}

/// A Confluent Schema Registry: where it is, and who signs in to it. Reached under its connector's TLS.
#[derive(Clone, Debug, PartialEq)]
pub struct SchemaRegistry {
    pub(crate) url: String,
    /// Basic auth: the user, and where the password lives. `None` signs in as no one.
    pub(crate) basic: Option<(String, SecretRef)>,
}

/// How a source reads its payloads as Avro, into a record of its own (`io::avro`).
#[derive(Clone, Debug)]
pub struct AvroRead {
    /// Where a stream's payloads are Confluent-framed: the registry holding each writer's schema. `None` for an
    /// object, a container file holding its own.
    pub(crate) registry: Option<SchemaRegistry>,
    /// What a record carries: the record each payload is read into, and its source's `__source` fields.
    pub meta: SourceMeta,
    /// The writers' schemas the source has met, which transform decodes its payloads with.
    pub writers: std::sync::Arc<crate::io::avro::Writers>,
}

/// Operator-supplied librdkafka properties, already split by whether the value is a credential. `credentials` maps a
/// property name to where its value lives; `io::kafka` resolves those when it builds the client, so one the worker
/// cannot obtain is a startup error naming it rather than a connection that silently fails to authenticate -- the
/// same treatment `ElasticDestinationConfig::auth` gets.
///
/// Ordered pairs rather than a map, because the only thing done with them is iterate and `.set()`, and load-time
/// validation has already rejected duplicates and reserved keys.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct KafkaProps {
    pub literal: Vec<(String, String)>,
    pub credentials: Vec<(String, SecretRef)>,
}

impl KafkaProps {
    pub fn is_empty(&self) -> bool {
        self.literal.is_empty() && self.credentials.is_empty()
    }
}

#[derive(Clone, Debug)]
pub struct KafkaSourceConfig {
    /// Bytes its client may hold fetched-but-unprocessed (`queued.max.messages.kbytes`) -- the largest term in a
    /// reader's memory footprint. See `config_file::dto::FetchBytes`.
    pub fetch_bytes: usize,
    pub brokers: String,
    pub topics: Vec<String>,
    pub group_id: String,
    /// The TLS this consumer speaks. `None` is no certificate and the platform's own roots.
    pub tls: Option<Tls>,
    /// Where it reads from. A committed offset is read on from, unless it descends from another `start` and a
    /// deployed commit older than this one's: then the partition moves here once (`io::kafka::client::resolve_starts`).
    pub start: StartPosition,
    /// Where each partition ends, before the record there. `None` (the default) never ends. "Everything between 4
    /// and 8 AM yesterday" is `start: Timestamp(4am)`, `stop: Some(Timestamp(8am))`.
    pub stop: Option<StopPosition>,
    /// The deployed commit's committer time, in unix seconds: a `start` set under a newer commit moves a partition
    /// placed under an older one. Set from the commit the worker runs, never from the config file; 0 elsewhere.
    pub deployed_at: i64,
    /// This worker's id, under which the consumer joins its group as a static member (`group.instance.id`, KIP-345):
    /// a redeploy that rejoins within the session timeout takes back its partitions without a rebalance. Set from the
    /// worker running the deployment, never from the config file; `None` joins as a dynamic member.
    pub member: Option<String>,
    /// The availability zone this worker runs in (`client.rack`), so a broker can serve it from a replica in that
    /// zone. Set from the worker running the deployment, never from the config file.
    pub rack: Option<String>,
    /// Extra librdkafka properties for this consumer (TLS, SASL, fetch tuning). See [`KafkaProps`].
    pub props: KafkaProps,
    /// How this source's bytes become `_raw`'s text. `Auto` detects per record; see `format::encoding`.
    pub encoding: Encoding,
    /// What the brokers are, which is what the source is called. Reading is the same either way.
    pub service: KafkaService,
    /// Each payload read as Avro into a record of its own, in place of `_raw`. `None` reads payloads as text.
    pub avro: Option<AvroRead>,
}

/// The service a Kafka source reads from or a Kafka destination writes to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KafkaService {
    Kafka,
    /// Azure Event Hubs, through its Kafka endpoint.
    EventHubs,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StartPosition {
    Earliest,
    Latest,
    /// Milliseconds since the Unix epoch — resolved to a concrete per-partition offset via Kafka's own
    /// offsets-for-times lookup (`io::kafka`, not this config-only module).
    Timestamp(i64),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StopPosition {
    /// Milliseconds since the Unix epoch, exclusive — a record whose own timestamp is `>=` this stops that partition.
    /// Pairs with `start: Timestamp` to express a half-open `[start, stop)` time window, the natural reading of
    /// "between 4 and 8 AM."
    Timestamp(i64),
}

#[derive(Clone, Debug)]
pub struct DestinationConfig {
    pub id: DestId,
    pub name: String,
    pub kind: DestinationKind,
    /// The answer to a record *this* destination refuses: the group's answer to `Refused`, or the destination's own.
    /// Never ignore or tag: no destination is left to carry the row.
    pub on_reject: Answer,
}

/// The `__destination` field that says where in a destination a row goes, named in that destination's own terms. Each
/// destination reads one; a row may set several when its route delivers to several kinds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Placement {
    Topic,
    Index,
    Prefix,
}

impl Placement {
    pub const ALL: [Placement; 3] = [Placement::Topic, Placement::Index, Placement::Prefix];

    /// Its name under `__destination`.
    pub(crate) fn name(self) -> &'static str {
        match self {
            Placement::Topic => "topic",
            Placement::Index => "index",
            Placement::Prefix => "prefix",
        }
    }

    /// The field a pipeline binds.
    pub fn field(self) -> &'static str {
        match self {
            Placement::Topic => "__destination.topic",
            Placement::Index => "__destination.index",
            Placement::Prefix => "__destination.prefix",
        }
    }
}

/// Every DLQ a refusal at `dest` can reach, given each destination's own list: its DLQs, theirs, and so on, each once,
/// nearest first. Load refuses a chain that leads back to where it started; this ends regardless.
pub(crate) fn dlq_chain(named: &HashMap<DestId, Vec<DestId>>, dest: DestId) -> Vec<DestId> {
    let mut chain: Vec<DestId> = Vec::new();
    let mut next = 0;
    let mut at = dest;
    loop {
        for &dlq in named.get(&at).map(Vec::as_slice).unwrap_or_default() {
            if dlq != dest && !chain.contains(&dlq) {
                chain.push(dlq);
            }
        }
        let Some(&d) = chain.get(next) else {
            return chain;
        };
        at = d;
        next += 1;
    }
}

#[derive(Clone, Debug)]
pub enum DestinationKind {
    Kafka(KafkaDestinationConfig),
    Elasticsearch(ElasticDestinationConfig),
    SplunkHec(SplunkHecDestinationConfig),
    Objects(ObjectsDestinationConfig),
    Http(HttpDestinationConfig),
    Prometheus(PrometheusDestinationConfig),
    LogAnalytics(LogAnalyticsDestinationConfig),
    Otlp(OtlpDestinationConfig),
    Iceberg(IcebergDestinationConfig),
    Delta(DeltaDestinationConfig),
}

/// Sets `url` to `zone`'s address in `zone_urls` where it names one, and empties `zone_urls`, as
/// [`DestinationKind::take_zone`] does for a destination's config.
pub fn take_zone(url: &mut String, zone_urls: &mut std::collections::BTreeMap<String, String>, zone: Option<&str>) {
    // The worker's zone is trimmed (`startup_config::worker_zone`); a bundle written by hand may not be.
    if let Some(own) = zone.and_then(|z| zone_urls.iter().find(|(name, _)| name.trim() == z)) {
        *url = own.1.clone();
    }
    zone_urls.clear();
}

impl DestinationKind {
    /// The TLS this destination speaks, where it speaks any of its own.
    pub(crate) fn tls(&self) -> Option<&Tls> {
        match self {
            DestinationKind::Kafka(k) => k.tls.as_ref(),
            DestinationKind::Elasticsearch(e) => e.tls.as_ref(),
            DestinationKind::SplunkHec(h) => h.tls.as_ref(),
            DestinationKind::Objects(o) => o.tls.as_ref(),
            DestinationKind::Http(h) => h.tls.as_ref(),
            DestinationKind::Prometheus(p) => p.tls.as_ref(),
            DestinationKind::LogAnalytics(LogAnalyticsDestinationConfig { http, .. })
            | DestinationKind::Otlp(OtlpDestinationConfig { http, .. }) => http.tls.as_ref(),
            DestinationKind::Iceberg(i) => i.tls.as_ref(),
            DestinationKind::Delta(d) => d.tls.as_ref(),
        }
    }

    /// Sends to `zone`'s own address where the destination names one, in place of its `url`. `None` (a worker that
    /// names no zone) keeps `url`. Either way the other zones' addresses are dropped: this worker uses none of them.
    pub fn take_zone(&mut self, zone: Option<&str>) {
        let (url, zone_urls) = match self {
            DestinationKind::Elasticsearch(c) => {
                if let Some(sniff) = &mut c.sniff {
                    sniff.zone = zone.map(str::to_string);
                }
                (&mut c.url, &mut c.zone_urls)
            }
            DestinationKind::SplunkHec(c) => (&mut c.url, &mut c.zone_urls),
            DestinationKind::Http(c) => (&mut c.url, &mut c.zone_urls),
            _ => return,
        };
        take_zone(url, zone_urls, zone);
    }

    /// What this destination writes of each row.
    pub fn payload(&self) -> Payload {
        match self {
            DestinationKind::Kafka(c) => c.payload.clone(),
            DestinationKind::Elasticsearch(_) => Payload::Record,
            DestinationKind::SplunkHec(c) => c.payload.clone(),
            DestinationKind::Http(c) => c.payload.clone(),
            DestinationKind::Objects(c) => match &c.format {
                ObjectFormat::Ndjson
                | ObjectFormat::Json
                | ObjectFormat::Parquet { .. }
                | ObjectFormat::Avro
                | ObjectFormat::Csv => Payload::Record,
                ObjectFormat::Text { field } => Payload::Text { field: field.clone() },
                ObjectFormat::Bytes { field, .. } => Payload::Bytes { field: field.clone() },
            },
            DestinationKind::Prometheus(_)
            | DestinationKind::LogAnalytics(_)
            | DestinationKind::Otlp(_)
            | DestinationKind::Iceberg(_)
            | DestinationKind::Delta(_) => Payload::Record,
        }
    }

    /// The `__destination` field this kind reads to place a row. `None` for one that sends every row to one place.
    pub fn placement(&self) -> Option<Placement> {
        match self {
            DestinationKind::Kafka(_) => Some(Placement::Topic),
            DestinationKind::Elasticsearch(_) | DestinationKind::SplunkHec(_) => Some(Placement::Index),
            DestinationKind::Objects(_) => Some(Placement::Prefix),
            DestinationKind::Http(_)
            | DestinationKind::Prometheus(_)
            | DestinationKind::LogAnalytics(_)
            | DestinationKind::Otlp(_)
            | DestinationKind::Iceberg(_)
            | DestinationKind::Delta(_) => None,
        }
    }

    /// How long one batch may take to deliver before it is a stall.
    pub(crate) fn stall_after(&self) -> std::time::Duration {
        match self {
            DestinationKind::Kafka(k) => k.stall_after,
            DestinationKind::Elasticsearch(e) => e.stall_after,
            DestinationKind::SplunkHec(s) => s.stall_after,
            DestinationKind::Objects(o) => o.stall_after,
            DestinationKind::Http(h) => h.stall_after,
            DestinationKind::Prometheus(p) => p.stall_after,
            DestinationKind::LogAnalytics(LogAnalyticsDestinationConfig { http, .. })
            | DestinationKind::Otlp(OtlpDestinationConfig { http, .. }) => http.stall_after,
            DestinationKind::Iceberg(i) => i.stall_after,
            DestinationKind::Delta(d) => d.stall_after,
        }
    }

    /// Bytes egress may hold for this destination before a reader pushing to it waits: the egress queue's budget,
    /// which a batch holds from acceptance until the destination acknowledges it. So it also bounds what is sent and
    /// unacknowledged, and the reader depth in front of it (`worker::reader_depth`).
    ///
    /// What the egress queue in front of this destination may hold. For an HTTP destination that is the whole of the
    /// destination's `bufferBytes`: the senders hold the same bytes the queue counts. A Kafka destination holds a
    /// batch twice, once in egress and once in librdkafka's own queue, so its `bufferBytes` is split between the two
    /// and this is egress's half ([`KafkaDestinationConfig::queue_bytes`]).
    pub fn queue_bytes(&self) -> usize {
        match self {
            DestinationKind::Kafka(k) => k.queue_bytes(),
            DestinationKind::Elasticsearch(e) => e.buffer_bytes,
            DestinationKind::SplunkHec(s) => s.buffer_bytes,
            DestinationKind::Objects(o) => o.buffer_bytes,
            DestinationKind::Http(h) => h.buffer_bytes,
            DestinationKind::Prometheus(p) => p.buffer_bytes,
            DestinationKind::LogAnalytics(LogAnalyticsDestinationConfig { http, .. })
            | DestinationKind::Otlp(OtlpDestinationConfig { http, .. }) => http.buffer_bytes,
            DestinationKind::Iceberg(i) => i.buffer_bytes,
            DestinationKind::Delta(d) => d.buffer_bytes,
        }
    }

    /// Whether the destination writes files, and so acknowledges a batch only once the file holding it is
    /// committed: when it holds most of its buffer, or its interval passes.
    pub(crate) fn writes_files(&self) -> bool {
        matches!(
            self,
            DestinationKind::Objects(_) | DestinationKind::Iceberg(_) | DestinationKind::Delta(_)
        )
    }

    /// The destination's `bufferBytes`: all it holds of what it has not yet delivered, egress's queue and, for Kafka,
    /// librdkafka's own queue together.
    pub(crate) fn buffer_bytes(&self) -> usize {
        match self {
            DestinationKind::Kafka(k) => k.buffer_bytes,
            kind => kind.queue_bytes(),
        }
    }

    /// Sets the destination's `bufferBytes`.
    pub(crate) fn set_buffer_bytes(&mut self, bytes: usize) {
        match self {
            DestinationKind::Kafka(k) => k.buffer_bytes = bytes,
            DestinationKind::Elasticsearch(e) => e.buffer_bytes = bytes,
            DestinationKind::SplunkHec(s) => s.buffer_bytes = bytes,
            DestinationKind::Objects(o) => o.buffer_bytes = bytes,
            DestinationKind::Http(h) => h.buffer_bytes = bytes,
            DestinationKind::Prometheus(p) => p.buffer_bytes = bytes,
            DestinationKind::LogAnalytics(LogAnalyticsDestinationConfig { http, .. })
            | DestinationKind::Otlp(OtlpDestinationConfig { http, .. }) => http.buffer_bytes = bytes,
            DestinationKind::Iceberg(i) => i.buffer_bytes = bytes,
            DestinationKind::Delta(d) => d.buffer_bytes = bytes,
        }
    }

    /// Whether this destination may hold a variant as Parquet's variant, and so reads its bytes (`parquet::Schema`).
    pub(crate) fn holds_native_variants(&self) -> bool {
        match self {
            DestinationKind::Iceberg(_) | DestinationKind::Delta(_) => true,
            DestinationKind::Objects(o) => o.format == ObjectFormat::Parquet { native_variants: true },
            _ => false,
        }
    }

    /// Which field of the row this destination reads the document identity from, if it does. `None` means it reads
    /// the pipeline's own `__destination.id` (held apart from the row's fields), or has no document identity at all.
    pub fn id_field(&self) -> Option<&str> {
        match self {
            DestinationKind::Kafka(_) => None,
            DestinationKind::Elasticsearch(e) => e.id_field.as_deref().filter(|f| *f != DOC_ID),
            DestinationKind::SplunkHec(_)
            | DestinationKind::Objects(_)
            | DestinationKind::Http(_)
            | DestinationKind::Prometheus(_)
            | DestinationKind::LogAnalytics(_)
            | DestinationKind::Otlp(_)
            | DestinationKind::Iceberg(_)
            | DestinationKind::Delta(_) => None,
        }
    }
}

/// Where an Elasticsearch destination ships to, and how it authenticates.
///
/// The credential is deliberately *not* here: a config bundle is git-committed and reviewed in a pull request (design
/// doc, Secrets boundary), so `auth` names where the credential lives and the value is resolved at
/// construction from what the worker holds (`HeldSecrets`) -- its own environment, or a value the leader sent it.
#[derive(Clone, Debug)]
pub struct ElasticDestinationConfig {
    /// The cluster's base address, e.g. `https://es.internal:9200`.
    pub url: String,
    /// Zone -> the address a worker in that zone sends to instead of `url`. Emptied at deploy, once the worker has
    /// taken its own zone's (`DestinationKind::take_zone`).
    pub zone_urls: std::collections::BTreeMap<String, String>,
    /// Used when a row has no live `__destination.index`.
    pub default_index: String,
    /// The credential holding the API key, or a complete `Authorization` header value (`io::elastic::authorization`).
    /// `None` for a cluster with security disabled, or one signed in to as `basic`.
    pub auth: Option<SecretRef>,
    /// Basic auth: the user, and where the password lives. Never beside `auth`.
    pub basic: Option<(String, SecretRef)>,
    /// The TLS this client speaks. `None` is no certificate and the platform's own roots -- which an
    /// internally-issued server certificate will not be under.
    pub tls: Option<Tls>,
    /// What happens to a document whose `_id` is already in the index. See `BulkAction`.
    pub action: BulkAction,
    /// How many bulk requests this destination starts with outstanding, each on its own connection.
    ///
    /// Where it begins, not where it stays: see `max_in_flight` for where it stops, and `egress::pacing` for what
    /// moves it.
    ///
    /// Concurrency is what buys throughput here, because a bulk round trip costs about the same whatever the batch
    /// carries -- so a worker with one request outstanding cannot go faster than `1/round-trip` no matter what else is
    /// tuned. The ceiling is the cluster's own write capacity, past which it answers 429 and everything queues anyway,
    /// which is the signal pacing reads rather than a figure anyone has to guess.
    pub in_flight: usize,
    /// Bytes of documents one bulk request may carry before the destination sends early.
    pub max_bulk_bytes: usize,
    /// Bytes egress may hold for this destination -- queued, and sent but unacknowledged. Divided by `max_bulk_bytes`
    /// it is also the most requests that can be outstanding at once, since a batch holds its budget until the cluster
    /// answers for it.
    pub buffer_bytes: usize,
    /// How long one batch may take to deliver before it is a stall.
    pub stall_after: std::time::Duration,
    /// Which of the row's own fields holds the document's `_id`. `None` (the default) reads the pipeline's
    /// `__destination.id`. A row where that field is null has Elasticsearch generate the `_id`.
    ///
    /// This is the ordinary case and the reason it is configured here rather than written into every pipeline: a
    /// document that has a stable identity usually already carries it in a field, and pointing at that field is
    /// configuration, not logic. It is also the answer for a tiered deployment, where identity has to survive a hop and
    /// so has to live in a real serialized field anyway (design doc, Runtime stages: a document identity and a
    /// partition key are separate on purpose).
    ///
    /// A pipeline that needs to *compute* an identity sets `__destination.id`, which is the field read when this names
    /// none. Where it names another, that field wins over the pipeline's `__destination.id` for this destination.
    pub id_field: Option<String>,
    /// Present when the worker learns the cluster's nodes from `url` and spreads its requests across them.
    pub sniff: Option<Sniff>,
}

/// How an Elasticsearch destination picks among the nodes it learned (`io::elastic_nodes`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Sniff {
    /// The node attribute naming a node's zone.
    pub zone_attribute: String,
    /// The worker's own zone, whose nodes it prefers. Set at deploy (`DestinationKind::take_zone`).
    pub zone: Option<String>,
}

impl Sniff {
    /// The zone attribute a destination that names none reads.
    pub const DEFAULT_ZONE_ATTRIBUTE: &str = "zone";
}

/// Where a Splunk HTTP Event Collector destination sends events, and what it files them under. Each row is the `event`
/// of one HEC envelope.
#[derive(Clone, Debug)]
pub struct SplunkHecDestinationConfig {
    /// The collector's base address, e.g. `https://splunk.internal:8088`.
    pub url: String,
    /// Zone -> the address a worker in that zone sends to instead of `url`. Emptied at deploy, once the worker has
    /// taken its own zone's (`DestinationKind::take_zone`).
    pub zone_urls: std::collections::BTreeMap<String, String>,
    /// The credential holding the HEC token, never the token itself.
    pub token: SecretRef,
    /// Used when a row has no live `__destination.index`. `None` leaves the token's own default index.
    pub index: Option<String>,
    /// Used when the document has no top-level `sourcetype`, `source` or `host` of its own.
    pub sourcetype: Option<String>,
    pub source: Option<String>,
    pub host: Option<String>,
    pub endpoint: Endpoint,
    /// What each event is: the record, one field's text, or, on the raw endpoint, the source's bytes.
    pub payload: Payload,
    pub acknowledgement: Acknowledgement,
    /// The TLS this client speaks. `None` is no certificate and the platform's own roots.
    pub tls: Option<Tls>,
    /// How many requests this destination starts with outstanding. See `ElasticDestinationConfig::in_flight`.
    pub in_flight: usize,
    /// Bytes of events one request may carry before the destination sends early.
    pub max_request_bytes: usize,
    /// Bytes egress may hold for this destination -- queued, and sent but unacknowledged.
    pub buffer_bytes: usize,
    /// How long one batch may take to deliver, its acknowledgement included, before it is a stall.
    pub stall_after: std::time::Duration,
}

impl SplunkHecDestinationConfig {
    /// The most requests this destination may have outstanding at once. See `ElasticDestinationConfig::max_in_flight`.
    pub(crate) fn max_in_flight(&self) -> usize {
        (self.buffer_bytes / self.max_request_bytes.max(1)).clamp(1, MAX_SENDERS)
    }
}

/// An Iceberg table in a REST catalog. The route's record is the table's schema; a missing table is created.
#[derive(Clone, Debug)]
pub struct IcebergDestinationConfig {
    /// The catalog's base address, before `/v1`.
    pub catalog: String,
    /// The warehouse the catalog serves this table from. `None` is the catalog's default.
    pub warehouse: Option<String>,
    pub sign_in: CatalogSignIn,
    /// The table's namespace, level by level.
    pub namespace: Vec<String>,
    pub table: String,
    /// The partition spec a created table takes, each a transform of a column: `day(ts)`, `bucket(16, id)`,
    /// `host`. An existing table's own spec is followed.
    pub partition_by: Vec<String>,
    /// Storage credentials of its own. `None` uses what the catalog vends.
    pub storage: Option<S3Access>,
    pub compression: Compression,
    /// The TLS this client speaks, to the catalog and to storage.
    pub tls: Option<Tls>,
    /// Parts of one file uploading at once.
    pub in_flight: usize,
    /// The largest data file, compressed.
    pub max_object_bytes: usize,
    /// Bytes egress may hold for this destination -- queued, and sent but not yet committed.
    pub buffer_bytes: usize,
    /// How long one store or catalog call may take, and one commit, before it is a stall.
    pub stall_after: std::time::Duration,
    /// How long files stay open before they are committed and their batches acknowledged.
    pub file_interval: std::time::Duration,
}

/// A Delta table at a path in S3, committed by adding the next entry to its `_delta_log` only where none is. The
/// route's record is the table's schema; a missing table is created.
#[derive(Clone, Debug)]
pub struct DeltaDestinationConfig {
    /// The table's root, `s3://bucket/path`. Empty when the table is reached through `unity_catalog`, which gives its
    /// root.
    pub location: String,
    /// The Unity Catalog table this writes, whose root and storage credentials the catalog gives. `None` writes the
    /// table at `location`.
    pub unity_catalog: Option<UnityCatalogTable>,
    /// The columns a created table is partitioned by, each a top-level column of the record. An existing table's own
    /// are followed.
    pub partition_by: Vec<String>,
    /// How the bucket is reached and signed for.
    pub storage: S3Access,
    pub compression: Compression,
    /// The TLS this client speaks.
    pub tls: Option<Tls>,
    /// Parts of one file uploading at once.
    pub in_flight: usize,
    /// The largest data file, compressed.
    pub max_object_bytes: usize,
    /// Bytes egress may hold for this destination -- queued, and sent but not yet committed.
    pub buffer_bytes: usize,
    /// How long one store call may take, and one commit, before it is a stall.
    pub stall_after: std::time::Duration,
    /// How long files stay open before they are committed and their batches acknowledged.
    pub file_interval: std::time::Duration,
}

/// A Databricks Unity Catalog external table, signed in to as a service principal with OAuth client credentials.
#[derive(Clone, Debug)]
pub struct UnityCatalogTable {
    /// The workspace's address, as `https://dbc-1234.cloud.databricks.com`.
    pub workspace: String,
    /// The table's three-part name, `catalog.schema.table`.
    pub table: String,
    pub client_id: String,
    pub client_secret: SecretRef,
}

/// How an Iceberg destination signs in to its catalog.
#[derive(Clone, Debug)]
pub enum CatalogSignIn {
    None,
    Bearer(SecretRef),
    /// OAuth 2.0 client credentials. `token_url` `None` is the catalog's own `/v1/oauth/tokens`.
    ClientCredentials {
        token_url: Option<String>,
        client_id: String,
        client_secret: SecretRef,
        scope: Option<String>,
    },
    /// Every call signed with AWS Signature Version 4 under the destination's AWS credentials (`storage`, else the
    /// host's): Glue's endpoint takes `glue`, S3 Tables `s3tables`.
    SigV4 {
        signing_name: String,
    },
}

/// Where an HTTP destination POSTs, and how its rows make a request's body. Every row goes to the one URL.
#[derive(Clone, Debug)]
pub struct HttpDestinationConfig {
    /// The whole address each request is POSTed to.
    pub url: String,
    /// Zone -> the address a worker in that zone sends to instead of `url`. Emptied at deploy, once the worker has
    /// taken its own zone's (`DestinationKind::take_zone`).
    pub zone_urls: std::collections::BTreeMap<String, String>,
    pub body: RequestBody,
    /// What each row is: the record, one field's text, or the source's bytes.
    pub payload: Payload,
    /// `None` or `Gzip`; load refuses the others.
    pub compression: Compression,
    /// Headers sent on every request, by name, with their committed values.
    pub headers: Vec<(String, String)>,
    /// Headers whose values are credentials: the header's name, and where its value lives.
    pub credentials: Vec<(String, SecretRef)>,
    /// The TLS this client speaks. `None` is no certificate and the platform's own roots.
    pub tls: Option<Tls>,
    /// How many requests this destination starts with outstanding. See `ElasticDestinationConfig::in_flight`.
    pub in_flight: usize,
    /// Bytes of rows one request may carry, before compression, before the destination sends early.
    pub max_request_bytes: usize,
    /// Bytes egress may hold for this destination -- queued, and sent but unacknowledged.
    pub buffer_bytes: usize,
    /// How long one batch may take to deliver before it is a stall.
    pub stall_after: std::time::Duration,
}

impl HttpDestinationConfig {
    /// The most requests this destination may have outstanding at once. See `ElasticDestinationConfig::max_in_flight`.
    pub(crate) fn max_in_flight(&self) -> usize {
        (self.buffer_bytes / self.max_request_bytes.max(1)).clamp(1, MAX_SENDERS)
    }
}

/// Where a Log Analytics destination sends rows: a stream of a data collection rule, through Azure Monitor's Logs
/// Ingestion API, which lands them in the rule's Log Analytics table. Rows go as gzipped JSON arrays, the HTTP
/// destination's, signed in with Entra.
#[derive(Clone, Debug)]
pub struct LogAnalyticsDestinationConfig {
    /// The rule's stream, as an HTTP destination: its `url` the stream's whole address.
    pub http: HttpDestinationConfig,
    pub sign_in: EntraSignIn,
}

/// Where an OTLP destination sends one signal: OTLP/HTTP protobuf exports, as the HTTP destination's requests, to the
/// signal's path under its `url`.
#[derive(Clone, Debug)]
pub struct OtlpDestinationConfig {
    /// The endpoint, as an HTTP destination: its `url` the address the signal's path goes under.
    pub http: HttpDestinationConfig,
    pub signal: Signal,
}

/// How a client signs in to an Azure service with Entra ID.
#[derive(Clone, Debug)]
pub enum EntraSignIn {
    /// A service principal, its secret traded at `authority`, else where the service's cloud signs in.
    ServicePrincipal {
        authority: Option<String>,
        tenant_id: String,
        client_id: String,
        secret: SecretRef,
    },
    /// One of the host's managed identities: the one `client_id` names, else the host's own, which in an Azure
    /// Kubernetes Service pod with a workload identity is that identity.
    ManagedIdentity { client_id: Option<String> },
}

/// Where a Prometheus destination sends remote_write. Every row goes to the one URL, with requests side by side: smeltr
/// orders nothing across requests, so a store that refuses a series' sample older than the last it stored needs its
/// out-of-order window to take them all.
#[derive(Clone, Debug)]
pub struct PrometheusDestinationConfig {
    /// The whole address each request is POSTed to: a store's remote_write endpoint.
    pub url: String,
    /// The version of remote_write each request is sent in.
    pub version: RemoteWriteVersion,
    /// Headers sent on every request, by name, with their committed values.
    pub headers: Vec<(String, String)>,
    /// Headers whose values are credentials: the header's name, and where its value lives.
    pub credentials: Vec<(String, SecretRef)>,
    /// The TLS this client speaks. `None` is no certificate and the platform's own roots.
    pub tls: Option<Tls>,
    /// How many requests this destination starts with outstanding. See `ElasticDestinationConfig::in_flight`.
    pub in_flight: usize,
    /// Bytes of samples one request may carry, before compression, before the destination sends early: each sample
    /// counted with its labels, which a request sends once per series, so a request is smaller.
    pub max_request_bytes: usize,
    /// Bytes egress may hold for this destination -- queued, and sent but unacknowledged.
    pub buffer_bytes: usize,
    /// How long one batch may take to deliver before it is a stall.
    pub stall_after: std::time::Duration,
}

impl PrometheusDestinationConfig {
    /// The most requests this destination may have outstanding at once. See `ElasticDestinationConfig::max_in_flight`.
    pub fn max_in_flight(&self) -> usize {
        (self.buffer_bytes / self.max_request_bytes.max(1)).clamp(1, MAX_SENDERS)
    }
}

/// The version of remote_write a Prometheus destination sends. Every store takes 1.0; 2.0 also carries each series'
/// metadata and start time, where its store takes it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum RemoteWriteVersion {
    #[default]
    V1,
    V2,
}

/// How an HTTP destination puts rows into one request's body.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum RequestBody {
    /// One row per line.
    #[default]
    Lines,
    /// A JSON array, one element per row: the record, or the text as a JSON string.
    JsonArray,
    /// One row per request, the body the row alone.
    Single,
}

/// The record each route writes to one destination (`plan::dictionary::record_type`), by its index in
/// `CompiledTopology::routes`. `None` is the record no pipeline ran on, `{_raw: str}`.
pub type Records = HashMap<Option<u32>, crate::core::column::DataType>;

/// Where an object store destination writes, and how. A file stays open across batches, one per prefix (and per
/// record, for Parquet), named for a random id, until `file_interval` or `max_object_bytes`.
#[derive(Clone, Debug)]
pub struct ObjectsDestinationConfig {
    /// The cloud, its bucket and its credentials.
    pub cloud: Cloud,
    /// What every object key starts with, for a row with no `__destination.prefix`. `%Y %m %d %H %M` are the write
    /// time, UTC.
    pub prefix: String,
    pub format: ObjectFormat,
    pub compression: Compression,
    /// One of the cloud's own classes. `None` leaves the bucket's default.
    pub storage_class: Option<String>,
    /// The TLS this client speaks. `None` is no certificate and the platform's own roots.
    pub tls: Option<Tls>,
    /// Parts of one file uploading at once.
    pub in_flight: usize,
    /// The largest file this destination puts in its bucket, compressed; one is finished there, even within one batch.
    pub max_object_bytes: usize,
    /// Bytes egress may hold for this destination -- queued, and sent but unacknowledged.
    pub buffer_bytes: usize,
    /// How long one store call may take, and one finishing of the files, before it is a stall.
    pub stall_after: std::time::Duration,
    /// How long a file stays open, taking batch after batch, before it is finished and its batches acknowledged.
    pub file_interval: std::time::Duration,
}

/// The store an object destination writes to. Every cloud credential `None` takes the host's.
#[derive(Clone, Debug)]
pub enum Cloud {
    S3(S3Store),
    Gcs(GcsStore),
    Azure(AzureStore),
    Sftp(SftpStore),
}

/// An S3 bucket, and how requests to it are signed.
#[derive(Clone, Debug)]
pub struct S3Store {
    pub bucket: String,
    /// The KMS key objects are encrypted under. `None` leaves the bucket's default encryption.
    pub kms_key_id: Option<String>,
    pub access: S3Access,
}

/// Where AWS is reached and how its requests are signed: what an S3 destination and an SQS/S3 source share.
#[derive(Clone, Debug, Default)]
pub struct S3Access {
    /// `None` takes the host's (`AWS_REGION`).
    pub region: Option<String>,
    /// An S3-compatible service's address. `None` is AWS.
    pub endpoint: Option<String>,
    /// Where credentials are obtained: STS, for assuming a role. `None` is the region's own.
    pub auth_endpoint: Option<String>,
    /// `None` takes the host's credentials: its environment, a web identity, the container's or the instance's.
    pub access_key: Option<AccessKey>,
    /// A role to assume with the credentials above, for a bucket in another account.
    pub role: Option<AssumedRole>,
}

/// A receiver: a source the sender connects to. Every worker of the group listens on `listen`, and the sender, or a
/// balancer in front of the group, picks the worker for each connection. A request is answered once every
/// destination has acknowledged its records, so a sender that hears nothing sends it again.
#[derive(Clone, Debug)]
pub struct ReceiverConfig {
    pub receiver: Receiver,
    pub listen: std::net::SocketAddr,
    /// The tokens a sender may present, each by its name. Empty is a receiver that admits whoever its authorities
    /// vouch for.
    pub tokens: Vec<(String, SecretRef)>,
    /// What it serves: its certificate and key, and, with `authorities`, the roots a sender's own certificate must
    /// chain to. `None` serves plain TCP.
    pub tls: Option<Tls>,
    /// The largest request body it reads. A larger one is refused whole.
    pub max_request_bytes: usize,
    /// A request is answered once every destination has its records, rather than once it is in the inbox. A worker
    /// that stops between the two loses what it answered for (design doc, delivery).
    pub answer_after_delivery: bool,
    /// How many connections it holds open, for a receiver whose senders stream over a connection held open.
    pub connections: ConnectionLimits,
}

/// How many connections a receiver that streams holds at once, and how long one may sit idle. The defaults hold every
/// honest sender: a quiet one that keeps one connection for days, and every sender behind one balancer's address.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ConnectionLimits {
    /// The most it holds. At the cap a new connection takes the place of the one idle longest, if that one has been
    /// idle for `io::connections::EVICTABLE_AFTER`, and is closed otherwise. `None` holds as many as the worker's
    /// budget allows (`io::connections::Budget`).
    pub max: Option<usize>,
    /// The most it holds from one address, the same way. `None` holds as many as `max` from one.
    pub per_sender: Option<usize>,
    /// A connection that reads nothing this long is closed. `None` holds an idle one for as long as there is room.
    pub idle: Option<std::time::Duration>,
}

impl ConnectionLimits {
    /// The most a receiver, or a worker's receivers together, may hold.
    pub(crate) const MOST: usize = 1_000_000;
}

/// Which protocol a receiver speaks.
#[derive(Clone, Debug)]
pub enum Receiver {
    /// A POST of JSON or lines of text to one path.
    Http(HttpReceiverConfig),
    /// Splunk's HTTP Event Collector: events or raw lines, at the paths Splunk serves them.
    Hec(HecReceiverConfig),
    /// OpenTelemetry's OTLP, over HTTP as protobuf or JSON and over gRPC: the exports of one signal. The receivers of
    /// a group's other signals may share its port.
    Otlp(Signal),
    /// Prometheus remote_write 1.0: snappy-compressed protobuf samples, at `/api/v1/write`.
    Prometheus,
    /// Splunk-to-Splunk, as a Splunk forwarder sends to an indexer over TCP.
    S2s(S2sReceiverConfig),
    /// Bytes over TCP, cut into records by its framing, as `Framing::Syslog` cuts syslog's messages; or over UDP, a
    /// datagram per record.
    Socket(SocketReceiverConfig),
    /// Elasticsearch's `_bulk` API, as Beats, Fluent Bit and other Elasticsearch clients send it: a document per
    /// `index` or `create` action.
    Elastic,
}

impl ReceiverConfig {
    /// What it listens on. Every receiver but a socket receiver given UDP takes connections over TCP.
    pub fn listens(&self) -> Listen {
        let transport = match &self.receiver {
            Receiver::Socket(socket) => socket.transport,
            Receiver::Http(_)
            | Receiver::Hec(_)
            | Receiver::Otlp(_)
            | Receiver::Prometheus
            | Receiver::S2s(_)
            | Receiver::Elastic => Transport::Tcp,
        };
        Listen {
            transport,
            address: self.listen,
        }
    }
}

/// An address a receiver listens on, and over which transport. TCP and UDP on one port are two of them.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Listen {
    pub transport: Transport,
    pub address: std::net::SocketAddr,
}

/// What carries what a sender sends to a receiver.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Transport {
    #[default]
    Tcp,
    /// Datagrams, which a receiver cannot slow: one that arrives while it has no room is dropped.
    Udp,
}

impl Transport {
    pub(crate) fn is_tcp(&self) -> bool {
        *self == Transport::Tcp
    }
}

/// Who a syslog or socket receiver admits, and how it reads what they send.
#[derive(Clone, Debug, Default)]
pub struct SocketReceiverConfig {
    /// The networks a sender may connect from. Empty admits whoever TLS admitted.
    pub allow_from: Vec<ipnetwork::IpNetwork>,
    pub encoding: Encoding,
    pub transport: Transport,
    /// Where a record over TCP ends. Over UDP a datagram is one.
    pub framing: Framing,
}

/// Which of OpenTelemetry's signals an OTLP receiver takes. Each is records of its own fields.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Signal {
    #[default]
    Logs,
    Metrics,
    Traces,
}

impl Signal {
    /// Its name, as a receiver's `signal` setting names it.
    pub fn name(self) -> &'static str {
        match self {
            Signal::Logs => "logs",
            Signal::Metrics => "metrics",
            Signal::Traces => "traces",
        }
    }
}

/// How an S2S receiver breaks what its forwarders send into events.
#[derive(Clone, Debug, Default)]
pub struct S2sReceiverConfig {
    /// By sourcetype, where its stream breaks into events. A sourcetype not named here is an event per line.
    pub sourcetypes: std::collections::BTreeMap<String, crate::io::line_breaker::LineBreaker>,
}

/// What a HEC receiver answers each sender with, by the token it presents.
#[derive(Clone, Debug, Default)]
pub struct HecReceiverConfig {
    /// The tokens, by name, whose senders use indexer acknowledgement: each request names a channel and is answered
    /// at once with an `ackId` its sender asks after. A sender presenting any other token is held until delivered.
    pub acknowledging: std::collections::BTreeSet<String>,
    /// By sourcetype, where a raw request's body breaks into events. A sourcetype not named here is an event per line.
    pub sourcetypes: std::collections::BTreeMap<String, crate::io::line_breaker::LineBreaker>,
}

/// A POST to `path`. Its body is one record, a record per line, or a record per element of a JSON array.
#[derive(Clone, Debug)]
pub struct HttpReceiverConfig {
    pub path: String,
    pub records: ApiRecords,
}

/// An SQL source: a query run against one database every poll, its rows read as the record its fields declare.
/// Single-worker: one database answers one query, which nothing divides across a group.
#[derive(Clone, Debug)]
pub struct SqlConfig {
    pub database: SqlDatabase,
    pub source: crate::io::sql_source::SqlSourceConfig,
    /// The record `source.fields` declares.
    pub(crate) meta: SourceMeta,
    /// The database, the mode and a digest of the query and what the mode remembers by: what its progress is kept
    /// under, so a source pointed at another database, or changed in what it reads, starts afresh.
    pub(crate) place: String,
}

/// Which database an SQL source reads, and how it connects.
#[derive(Clone, Debug)]
pub enum SqlDatabase {
    Postgres(crate::io::postgres::PostgresConnection),
    MySql(crate::io::mysql::MySqlConnection),
}

impl SqlDatabase {
    /// The credential holding the user's password, if the server asks for one.
    pub fn password(&self) -> Option<&SecretRef> {
        match self {
            SqlDatabase::Postgres(p) => p.password.as_ref(),
            SqlDatabase::MySql(m) => m.password.as_ref(),
        }
    }

    /// The TLS it speaks, where it speaks any of its own.
    pub fn tls(&self) -> Option<&Tls> {
        match self {
            SqlDatabase::Postgres(p) => p.tls.as_ref(),
            SqlDatabase::MySql(m) => m.tls.as_ref(),
        }
    }

    /// `host:port/database`: where it reads, as a place names it.
    pub(crate) fn address(&self) -> String {
        let (host, port, database) = match self {
            SqlDatabase::Postgres(p) => (&p.host, p.port, &p.database),
            SqlDatabase::MySql(m) => (&m.host, m.port, &m.database),
        };
        format!("{host}:{port}/{database}")
    }
}

/// An API source: what one API answers, asked every `poll_interval`. Single-worker: nothing divides an API's answers
/// across a group.
#[derive(Clone, Debug)]
pub struct ApiSourceConfig {
    pub api: Api,
    /// The TLS this client speaks. `None` is no certificate and the platform's own roots.
    pub tls: Option<Tls>,
    /// How long after a poll starts the next one does.
    pub poll_interval: std::time::Duration,
}

/// Which API a source asks, and what it asks.
#[derive(Clone, Debug)]
pub enum Api {
    /// Any address that answers a GET with JSON or lines of text.
    HttpApi(HttpApiConfig),
    /// An Okta org's System Log.
    OktaLogs(OktaLogsConfig),
}

/// An Okta org's System Log, read in the order Okta stored it.
#[derive(Clone, Debug)]
pub struct OktaLogsConfig {
    /// The org's address, as `https://acme.okta.com`, with no path.
    pub org: String,
    pub sign_in: OktaSignIn,
    /// What the first poll asks for: what Okta lists with no start, or what happens from then on.
    pub start: ListStart,
}

/// How an Okta source signs in to its org.
#[derive(Clone, Debug)]
pub enum OktaSignIn {
    /// An admin's API token, sent as `Authorization: SSWS <token>`.
    Token(SecretRef),
    /// A service app, signing in as itself with a key assertion for a token scoped to `okta.logs.read`, DPoP-bound when Okta binds it.
    ServiceApp {
        client_id: String,
        /// The app's private key: an RSA key as PEM PKCS#8.
        private_key: SecretRef,
        /// Which of the app's keys `private_key` is, sent as `kid` when the app has several.
        key_id: Option<String>,
    },
}

impl OktaLogsConfig {
    /// The System Log's address in this org.
    pub(crate) fn logs_url(&self) -> String {
        format!("{}/api/v1/logs", self.org)
    }

    /// Where a service app signs in: the org authorization server, the one that grants Okta's own API scopes.
    pub(crate) fn token_url(&self) -> String {
        format!("{}/oauth2/v1/token", self.org)
    }
}

/// A GET of one address. Its answer is one record, a record per line, or a record per element of a JSON array.
#[derive(Clone, Debug)]
pub struct HttpApiConfig {
    /// The whole address asked, with any fixed query parameters.
    pub url: String,
    pub method: ApiMethod,
    /// The JSON every POST carries, as written. `None` is a POST of nothing.
    pub body: Option<String>,
    /// Headers sent on every request, by name, with their committed values.
    pub headers: Vec<(String, String)>,
    /// Headers whose values are credentials: the header's name, and where its value lives.
    pub credentials: Vec<(String, SecretRef)>,
    pub records: ApiRecords,
    /// Whether a poll follows the answer's `Link: rel="next"` to the next page.
    pub follow_links: bool,
    /// The query parameter that asks for what is new. `None` reads everything every poll.
    pub since: Option<Since>,
    /// How the source signs in as an app for the token every request carries. `None` sends only its headers.
    pub sign_in: Option<AppSignIn>,
}

/// An app signing in with OAuth 2.0 client credentials, for a bearer token it sends on every request until shortly
/// before the token expires.
#[derive(Clone, Debug)]
pub struct AppSignIn {
    pub client_id: String,
    pub token_at: TokenAt,
    /// Sent only when given.
    pub scope: Option<String>,
    pub proof: AppProof,
}

/// Where an app trades its credentials for a token.
#[derive(Clone, Debug)]
pub enum TokenAt {
    /// The token endpoint itself.
    Url(String),
    /// An OpenID provider, whose discovery document names its token endpoint.
    Issuer(String),
}

/// How an app proves itself to the token endpoint.
#[derive(Clone, Debug)]
pub enum AppProof {
    /// A client secret.
    Secret(SecretRef),
    /// An assertion signed by its private key (`private_key_jwt`, RFC 7523): an RSA key as PEM PKCS#8.
    Key {
        private_key: SecretRef,
        /// Which of the app's keys `private_key` is, sent as `kid` when the app has several.
        key_id: Option<String>,
    },
}

/// How an API source asks.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ApiMethod {
    #[default]
    Get,
    /// A POST of the source's body, for an API that is asked by what it is sent.
    Post,
}

/// Where an answer's records are.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ApiRecords {
    /// The whole answer is one record.
    Body,
    /// One record per line.
    Lines,
    /// One record per element of the array at this JSON pointer. Empty is an answer that is the array.
    Array(String),
}

/// A query parameter that asks for what happened since a time.
#[derive(Clone, Debug)]
pub struct Since {
    pub param: String,
    /// How far before the last poll's start each poll asks from, for what the API records late.
    pub overlap: std::time::Duration,
    /// What the first poll asks for.
    pub start: ListStart,
}

/// An object source: the objects a queue announces, each read whole and split into records.
#[derive(Clone, Debug)]
pub struct ObjectSourceConfig {
    pub queue: ObjectQueue,
    /// The TLS this client speaks. `None` is no certificate and the platform's own roots.
    pub tls: Option<Tls>,
    /// How an object's bytes, decompressed, become records.
    pub framing: Framing,
    /// How a record's bytes become text.
    pub encoding: Encoding,
    /// Each object read as an Avro container file into a record of its own, in place of `framing`'s records.
    pub avro: Option<AvroRead>,
    /// How long a received message stays hidden from every other worker. Extended while its records are in flight.
    pub visibility: std::time::Duration,
}

/// The queue an object source receives from, and how it and the objects it announces are reached and signed.
#[derive(Clone, Debug)]
pub enum ObjectQueue {
    /// S3's notifications on an SQS queue, from whichever bucket each names.
    Sqs {
        /// The queue's own URL, which names its region and account.
        queue_url: String,
        access: S3Access,
    },
    /// The objects under a prefix of one bucket or container, found by listing it. Single-worker: nothing divides
    /// a listing across a group.
    List {
        place: ListPlace,
        /// Only keys that start with it. Empty is the whole bucket.
        prefix: String,
        /// How long after a listing starts the next one does.
        poll_interval: std::time::Duration,
        start: ListStart,
        after_commit: AfterCommit,
    },
    /// Event Grid's `BlobCreated` events on an Azure Storage queue, from whichever container of `access`'s account
    /// each names.
    StorageQueue {
        /// The queue's own URL.
        queue_url: String,
        access: AzureAccess,
        /// How many times a message is received before it is moved to the poison queue: a Storage queue has no
        /// dead-letter queue of its own.
        max_deliveries: u32,
    },
    /// Cloud Storage's `OBJECT_FINALIZE` notifications on a Pub/Sub subscription, from whichever bucket each names.
    PubSub {
        /// `projects/<project>/subscriptions/<subscription>`.
        subscription: String,
        /// Another Pub/Sub address, such as an emulator's. `None` is Google's.
        endpoint: Option<String>,
        access: GcsAccess,
    },
}

/// The bucket or container a listing source lists, and how it is reached and signed.
#[derive(Clone, Debug)]
pub enum ListPlace {
    S3 { bucket: String, access: S3Access },
    Gcs(GcsStore),
    Azure(AzureStore),
    Sftp(SftpStore),
}

/// What a listing source reads first.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ListStart {
    /// Every object its first listing finds.
    #[default]
    Everything,
    /// Only objects that arrive after its first listing, which is recorded as read.
    New,
}

/// What a listing source does with an object once every record of it is committed.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum AfterCommit {
    /// Leaves it, and records it in the source's ledger. Needs only read access.
    #[default]
    Keep,
    /// Deletes it: the bucket is its own queue, and no ledger is kept.
    Delete,
    /// Copies it under `prefix`, in place of the source's own prefix, then deletes it. As `Delete`, no ledger.
    Move { prefix: String },
}

pub use crate::io::framing::Framing;

/// A Google Cloud Storage bucket. Objects take the bucket's default encryption.
#[derive(Clone, Debug)]
pub struct GcsStore {
    pub bucket: String,
    pub access: GcsAccess,
}

/// Where Google Cloud is reached and how its requests are signed: what a GCS destination and a Pub/Sub/GCS source
/// share.
#[derive(Clone, Debug, Default)]
pub struct GcsAccess {
    /// Another address, such as an emulator's. `None` is Google's.
    pub endpoint: Option<String>,
    /// A service account's JSON key. `None` takes the host's Application Default Credentials.
    pub service_account_key: Option<SecretRef>,
}

/// An Azure Blob Storage container. Objects take the account's default encryption.
#[derive(Clone, Debug)]
pub struct AzureStore {
    pub container: String,
    pub access: AzureAccess,
}

/// Where an Azure storage account is reached and how its requests are signed: what an Azure Blob destination and a
/// Queue/Azure Blob source share.
#[derive(Clone, Debug)]
pub struct AzureAccess {
    pub account: String,
    /// Another address, such as Azurite's or a sovereign cloud's. `None` is the account's in Azure's public cloud.
    pub endpoint: Option<String>,
    /// Where a client secret is exchanged for a token. `None` is the one in the cloud `endpoint` is in.
    pub auth_endpoint: Option<String>,
    /// `None` takes the host's own: a managed or workload identity.
    pub credential: Option<AzureCredential>,
}

/// A directory on an SFTP server.
#[derive(Clone, Debug)]
pub struct SftpStore {
    /// Absolute, or under the user's home directory. Empty is the home directory.
    pub directory: String,
    pub access: SftpAccess,
}

impl SftpStore {
    /// The directory as a URL without a trailing slash. A directory under the home directory is named from `~`, so it
    /// never reads as an absolute one.
    pub(crate) fn address(&self) -> String {
        let a = &self.access;
        let directory = match self.directory.trim_end_matches('/') {
            "" if self.directory.starts_with('/') => String::new(),
            "" => "/~".to_string(),
            d if d.starts_with('/') => d.to_string(),
            d => format!("/~/{d}"),
        };
        format!("sftp://{}@{}:{}{directory}", a.username, a.host, a.port)
    }
}

/// Where an SFTP server is reached, which server it must be, and how this side signs in.
#[derive(Clone, Debug)]
pub struct SftpAccess {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub credential: SftpCredential,
    /// The server's public key, as OpenSSH writes it (`ssh-ed25519 AAAA…`). A server that offers any other is
    /// refused.
    pub host_key: String,
}

/// How this side signs in to an SFTP server.
#[derive(Clone, Debug)]
pub enum SftpCredential {
    Password(SecretRef),
    /// An OpenSSH private key, unencrypted.
    PrivateKey(SecretRef),
}

/// How an Azure request is signed.
#[derive(Clone, Debug)]
pub enum AzureCredential {
    /// The account's shared key.
    AccessKey(SecretRef),
    /// A shared access signature's query string.
    SasToken(SecretRef),
    /// A service principal.
    ClientSecret {
        tenant_id: String,
        client_id: String,
        secret: SecretRef,
    },
    /// One of the host's managed identities, by its client id. The host's own is no credential.
    ManagedIdentity { client_id: String },
}

/// An AWS access key, both halves held as credentials.
#[derive(Clone, Debug)]
pub struct AccessKey {
    pub id: SecretRef,
    pub secret: SecretRef,
}

/// A role in another account, assumed through STS.
#[derive(Clone, Debug)]
pub struct AssumedRole {
    pub arn: String,
    /// What the role's trust policy asks the caller to quote.
    pub external_id: Option<String>,
}

/// What an object holds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ObjectFormat {
    /// One row per line.
    Ndjson,
    /// One JSON array of the rows.
    Json,
    /// One field's text per line.
    Text { field: std::sync::Arc<str> },
    /// One Parquet file, its columns the record of the route whose rows it holds (`CompiledTopology::records`). With
    /// `native_variants`, a variant is Parquet's variant rather than JSON text (`parquet::Schema::with_native_variants`).
    Parquet { native_variants: bool },
    /// One Avro container file, its schema the record of the route whose rows it holds, as `Parquet`'s is.
    Avro,
    /// One CSV file, a column per leaf of the record of the route whose rows it holds, as `Parquet`'s is (`io::csv`).
    Csv,
    /// One `bytes` field of each source record, one per line (`Payload::Bytes`).
    Bytes {
        field: std::sync::Arc<str>,
        newlines: Newlines,
    },
}

impl ObjectFormat {
    /// The name of the format that writes the route's record as columns (`CompiledTopology::records`), `None` for the
    /// line formats, which write each row as egress made it.
    pub(crate) fn column_format(&self) -> Option<&'static str> {
        match self {
            ObjectFormat::Parquet { .. } => Some("Parquet"),
            ObjectFormat::Avro => Some("Avro"),
            ObjectFormat::Csv => Some("CSV"),
            ObjectFormat::Ndjson | ObjectFormat::Json | ObjectFormat::Text { .. } | ObjectFormat::Bytes { .. } => None,
        }
    }

    /// Whether this format writes the route's record as columns.
    pub fn writes_columns(&self) -> bool {
        self.column_format().is_some()
    }
}

/// What a `Bytes` object does with a row whose bytes hold a line break.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Newlines {
    /// `\` becomes `\\`, a newline `\n` and a carriage return `\r`. `Framing::EscapedLines` reverses it.
    #[default]
    Escape,
    /// The row is refused.
    Refuse,
}

/// A destination's codec, with its level where it takes one. What is compressed with it: a Kafka batch, an object, or
/// each column chunk of a Parquet file. Only librdkafka's lz4 reads a level; lz4 elsewhere has none.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Compression {
    None,
    Gzip(u8),
    Snappy,
    Lz4(u8),
    Zstd(u8),
}

impl Compression {
    /// librdkafka's `compression.type` value.
    pub fn as_property(self) -> &'static str {
        match self {
            Compression::None => "none",
            Compression::Gzip(_) => "gzip",
            Compression::Snappy => "snappy",
            Compression::Lz4(_) => "lz4",
            Compression::Zstd(_) => "zstd",
        }
    }

    /// Every codec, each at its default level.
    pub const ALL: [Compression; 5] = [
        Compression::None,
        Compression::Gzip(6),
        Compression::Snappy,
        Compression::Lz4(0),
        Compression::Zstd(3),
    ];

    /// The codec's level, where it takes one.
    pub fn level(self) -> Option<u8> {
        match self {
            Compression::Gzip(l) | Compression::Lz4(l) | Compression::Zstd(l) => Some(l),
            Compression::None | Compression::Snappy => None,
        }
    }

    /// The codec at `level`, where it takes one.
    pub fn at(self, level: u8) -> Compression {
        match self {
            Compression::Gzip(_) => Compression::Gzip(level),
            Compression::Lz4(_) => Compression::Lz4(level),
            Compression::Zstd(_) => Compression::Zstd(level),
            Compression::None | Compression::Snappy => self,
        }
    }
}

/// How many brokers hold a Kafka batch before it counts as delivered.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Acks {
    /// Delivered once sent.
    None,
    /// Once the partition's leader has written it.
    One,
    /// Once every in-sync replica has written it.
    All,
}

impl Acks {
    /// librdkafka's `acks` value.
    pub(crate) fn as_property(self) -> &'static str {
        match self {
            Acks::None => "0",
            Acks::One => "1",
            Acks::All => "all",
        }
    }
}

/// Which HEC endpoint a destination sends to.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Endpoint {
    /// `/services/collector/event`: each document in an envelope, its time, host, source and sourcetype read from the
    /// document itself.
    #[default]
    Event,
    /// `/services/collector/raw`: each document as one line of text, which Splunk breaks and timestamps by its
    /// sourcetype. Index, host, source and sourcetype are the destination's, the same for every line.
    Raw,
}

/// When a HEC destination counts a batch delivered.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Acknowledgement {
    /// Once the indexer acknowledges the events, polled on the request's channel. The token must have indexer
    /// acknowledgement on. At-least-once.
    #[default]
    Indexer,
    /// On the 200. Events queued at an indexer that then fails are lost.
    Receipt,
}

/// The most senders one destination may own.
///
/// Concurrency is bounded by the buffer, and a buffer divided by a small enough bulk size runs to thousands. Each
/// sender is a parked thread holding its own client, so the process needs a bound of its own; the cluster's is its
/// own 429s, which is what the loop reads.
pub const MAX_SENDERS: usize = 64;

impl ElasticDestinationConfig {
    /// The most requests this destination may have outstanding at once: whole bulk requests its buffer affords, and
    /// never more senders than one destination may own.
    ///
    /// `in_flight` is where the loop starts; this is where it stops.
    pub fn max_in_flight(&self) -> usize {
        (self.buffer_bytes / self.max_bulk_bytes.max(1)).clamp(1, MAX_SENDERS)
    }
}

/// The bulk action an Elasticsearch destination writes each document with, named as Elasticsearch names it. A
/// document goes with the `_id` its row's id field holds, and with none where that is null, so Elasticsearch generates
/// one and a re-send after a crash is a second document.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum BulkAction {
    /// Writes a document only where its `_id` is new: a re-send is refused as a `version_conflict_engine_exception`,
    /// which counts as success, so the first copy is kept. The default, and the only action a data stream accepts.
    #[default]
    Create,
    /// Writes a document whether or not its `_id` is already there: a re-send replaces it, so the last copy is kept, and a pipeline can
    /// reprocess a record to correct what was indexed the first time.
    ///
    /// "Last" is not the last smeltr saw. A worker keeps several batches in flight (`runtime::Reader::max_in_flight`),
    /// so two versions of one `_id` in different batches can reach the cluster in either order. Accepted rather than
    /// special-cased: holding `Index` destinations to one batch in flight would buy determinism for the uncommon case
    /// at the cost of a rule every future destination has to know about.
    Index,
}

#[derive(Clone, Debug)]
pub struct KafkaDestinationConfig {
    /// What each message's value is.
    pub payload: Payload,
    pub compression: Compression,
    pub acks: Acks,
    /// How long one batch may take to deliver before it is a stall.
    pub stall_after: std::time::Duration,
    pub brokers: String,
    /// Used when a row has no live `__destination.topic` (see the design doc's "Pipeline-set config variables") — the
    /// destination's own default, not a per-row override.
    pub default_topic: String,
    /// The TLS this producer speaks. `None` is no certificate and the platform's own roots.
    pub tls: Option<Tls>,
    /// The most one worker may hold for this destination, in all: half in the egress queue and half in the
    /// producer's own queue, since each keeps its copy of a batch until the broker acknowledges it. An upper bound
    /// rather than the exact figure -- see `config_file::dto::BufferBytes`.
    pub buffer_bytes: usize,
    /// Extra librdkafka properties for this producer (TLS, SASL, `compression.type`). See [`KafkaProps`].
    pub props: KafkaProps,
    /// What the brokers are, which is what the destination is called, and which a refusal it explains names.
    pub service: KafkaService,
    /// Each row written as Avro, its route's record registered here. `None` writes `payload`.
    pub avro: Option<SchemaRegistry>,
}

impl KafkaDestinationConfig {
    /// The egress queue's share of `buffer_bytes`.
    fn queue_bytes(&self) -> usize {
        self.buffer_bytes / 2
    }

    /// The producer's share of `buffer_bytes`: its queue's bound (`queue.buffering.max.kbytes`), and what its
    /// batches are sized from at deployment.
    pub(crate) fn producer_bytes(&self) -> usize {
        self.buffer_bytes - self.queue_bytes()
    }
}

/// One entry in a source's ordered route list — design doc, Scope: "Multiple routes per source."
#[derive(Clone, Debug)]
pub struct RouteConfig {
    /// The author's id (`RouteDto::id`). A route has no positional id the way a source or destination does; this is
    /// what names its edge on the map and what a diff tells an edit from a delete-and-insert by.
    pub id: String,
    /// How every screen and message names the route (`config_file::route_name`).
    pub name: String,
    pub source: SourceId,
    /// `None` matches every row of its source.
    pub cond: Option<String>,
    /// `PipelineConfig::id` of the pipeline this route dispatches to.
    pub pipeline: usize,
    /// Where this route's output goes — design doc, Runtime stages: "Delivery belongs to the route". Rules are
    /// **additive, not first-match**: a row matching two rules is delivered to the union of their destinations, once each, which is
    /// what makes rule order carry no hidden meaning.
    ///
    /// Empty is legal and means the route delivers nowhere: it runs its pipeline and discards the result. Unhelpful,
    /// but a real configuration rather than an error.
    pub deliver: Vec<DeliverRule>,
    /// The answer to each exception its pipeline raises: the group's, with the route's own laid over it.
    pub on_exception: OnException,
}

/// A `DeliverRule` with its `when` compiled.
///
/// Compiled in `compile_topology` rather than `compile_route`, because a `when` resolves against the target
/// *pipeline's* output slots — which is the whole point of it, and the thing `RouteConfig::cond` cannot do (that runs
/// on the ingest batch and only ever sees the record as it arrived). `compile_route` has no pipeline in scope; `compile_topology` has
/// both.
#[derive(Clone, Debug)]
pub(crate) struct CompiledDeliverRule {
    /// `None` matches every surviving row — the catch-all.
    pub(crate) when: Option<Ir>,
    pub(crate) to: Vec<DestId>,
    /// Regex literals in `when`, compiled once. Evaluation points `Ctx::patterns` at this, exactly as `route_masks`
    /// does for `cond`.
    pub(crate) compiled_patterns: crate::core::id::ById<crate::core::id::PatternId, crate::core::ctx::CompiledPattern>,
}

/// One delivery rule: an optional condition over the pipeline's *output*, and the destinations a matching row goes to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeliverRule {
    /// Evaluated after the pipeline runs, so it can see computed fields — unlike `RouteConfig::cond`, which runs on the
    /// ingest batch and only ever sees the record as it arrived. `None` matches every surviving row: the catch-all, conventionally last.
    ///
    /// It also sees the route's tag field (`Plan::tag_slot`), so a tagged row is placed by a rule like any other
    /// (`compile_deliver_rule`).
    pub when: Option<String>,
    pub to: Vec<DestId>,
}

/// The sources whose `fetchBytes` and the destinations whose `bufferBytes` the bundle leaves out. Each holds its default
/// here, and a worker with a memory figure sizes it to that memory (`worker::memory::fit`).
#[derive(Clone, Debug, Default)]
pub(crate) struct Fitted {
    pub(crate) sources: std::collections::BTreeSet<SourceId>,
    pub(crate) destinations: std::collections::BTreeSet<DestId>,
}

#[derive(Clone, Debug, Default)]
pub struct TopologyConfig {
    pub sources: Vec<SourceConfig>,
    pub destinations: Vec<DestinationConfig>,
    /// What the bundle leaves for the worker to size. See [`Fitted`].
    pub(crate) fitted: Fitted,
    pub routes: Vec<RouteConfig>,
    /// The group's answer to every exception kind. See `CompiledTopology::on_exception`.
    pub on_exception: OnException,
    /// Every credential this bundle names (design doc, Secrets boundary: a bundle holds the *name* of the place a
    /// secret lives, never the value), read from the bundle at load. This is what makes "a named secret that is
    /// missing is a startup failure" checkable before a deployment is torn down: the answer depends only on the
    /// bundle, so a worker can ask it of a new commit while still running the old one.
    pub named_secrets: Vec<NamedSecret>,
    /// Every certificate this bundle names, read from the bundle at load as `named_secrets` is.
    pub named_certificates: Vec<NamedCertificate>,
}

impl TopologyConfig {
    /// Whether a row entering `source` can be `Unmatched` (design doc, Exception taxonomy): no route on it matches
    /// unconditionally.
    ///
    /// A route with no `cond` takes every row, so one makes the source's routing total. Anything narrower answers
    /// `true`, a source with no routes at all included: whether a set of conditions is exhaustive is a question about
    /// the expression language, and a wrong `false` hides a path rows are really taking.
    pub(crate) fn falls_through(&self, source: SourceId) -> bool {
        !self.routes.iter().any(|r| r.source == source && r.cond.is_none())
    }
}

/// One credential a bundle names but does not carry: where its value lives, and what needs it.
#[derive(Clone, Debug)]
pub struct NamedSecret {
    pub reference: SecretRef,
    /// What to say when it is missing, e.g. `source "in": credentials`.
    pub what: String,
}

/// One certificate a bundle names but does not carry: the entry of the leader's list, and the connector presenting
/// it. The worker runs the entry's newest version.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamedCertificate {
    pub name: String,
    /// What to say when it is missing, e.g. `source "in"`.
    pub(crate) what: String,
}

/// A variable set to the empty string counts as unset, matching `startup_config`: that is what an unset
/// docker-compose variable expands to, and an empty credential authenticates as nobody.
fn usable(read: Option<String>) -> Option<String> {
    read.filter(|v| !v.is_empty())
}

/// The credentials a worker can obtain: its own environment, plus what the leader has sent it. Secrets are keyed by
/// name *and* version, because a rotation in flight means holding the version that is running alongside the one a
/// bundle is about to quote. Certificates are keyed by name: a worker runs the newest version the leader has sent.
#[derive(Clone, Default)]
pub struct HeldSecrets {
    stored: std::collections::BTreeMap<(String, i64), String>,
    certificates: std::collections::BTreeMap<String, HeldCertificate>,
}

/// The newest version of a certificate entry the leader has sent: its chain, PEM, and its private key.
#[derive(Clone)]
struct HeldCertificate {
    version: i64,
    chain: String,
    key: String,
}

/// Names and versions, never values. This holds every credential the deployment uses, so one `{:?}` reaching a
/// panic message or a log line would be all of them at once -- the reason `leader::secrets::EncryptionKey` derives
/// nothing at all.
impl std::fmt::Debug for HeldSecrets {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HeldSecrets").field("holds", &self.versions()).finish()
    }
}

impl HeldSecrets {
    /// Replaces what the leader has sent with plain values: each entry is a name, a version, and that version's value.
    pub fn set_stored(&mut self, values: impl IntoIterator<Item = (String, i64, String)>) {
        self.stored = values.into_iter().map(|(n, v, value)| ((n, v), value)).collect();
    }

    /// Every credential in `wanted` and every certificate in `presented` that this worker holds, for handing to the
    /// process that will use them. The one place a value is read back out: it crosses to a runtime on a private
    /// pipe and nowhere else, and a runtime is given what its own bundle quotes rather than everything this worker
    /// has ever been sent.
    pub(crate) fn deliveries_for(
        &self,
        wanted: &[NamedSecret],
        presented: &[NamedCertificate],
    ) -> Vec<crate::heartbeat::SecretDelivery> {
        let secrets = self
            .stored
            .iter()
            .filter(|((name, version), _)| {
                wanted.iter().any(|s| {
                    matches!(&s.reference, SecretRef::LeaderStored { name: n, version: v } if n == name && v == version)
                })
            })
            .map(|((name, version), value)| crate::heartbeat::SecretDelivery {
                name: name.clone(),
                version: *version,
                value: value.clone(),
                chain: None,
            });
        let certificates = self
            .certificates
            .iter()
            .filter(|(name, _)| presented.iter().any(|c| &c.name == *name))
            .map(|(name, held)| crate::heartbeat::SecretDelivery {
                name: name.clone(),
                version: held.version,
                value: held.key.clone(),
                chain: Some(held.chain.clone()),
            });
        secrets.chain(certificates).collect()
    }

    /// Adds what the leader has just sent, keeping everything else. The leader answers with what this worker lacks,
    /// so an empty delivery means nothing changed rather than that nothing is held. A delivery with a chain is a
    /// certificate, kept by name at its newest version; the names whose version this replaced are answered, since
    /// what presents them has to be built again.
    pub fn take(&mut self, delivered: impl IntoIterator<Item = crate::heartbeat::SecretDelivery>) -> Vec<String> {
        let mut replaced = Vec::new();
        for d in delivered {
            let Some(chain) = d.chain else {
                self.stored.insert((d.name, d.version), d.value);
                continue;
            };
            let version = self.certificates.get(&d.name).map(|c| c.version);
            if version.is_some_and(|v| v >= d.version) {
                continue;
            }
            if version.is_some() {
                replaced.push(d.name.clone());
            }
            self.certificates.insert(
                d.name,
                HeldCertificate {
                    version: d.version,
                    chain,
                    key: d.value,
                },
            );
        }
        replaced
    }

    /// Drops everything `wanted` and `presented` do not name -- what a worker does once a deployment has loaded, so
    /// the versions it carries are the ones something runs on rather than every one it has ever been sent.
    pub(crate) fn keep_only(&mut self, wanted: &[NamedSecret], presented: &[NamedCertificate]) {
        self.stored.retain(|(name, version), _| {
            wanted.iter().any(|s| {
                matches!(&s.reference, SecretRef::LeaderStored { name: n, version: v } if n == name && v == version)
            })
        });
        self.certificates
            .retain(|name, _| presented.iter().any(|c| &c.name == name));
    }

    /// Every `(name, version)` held, certificates included, which is what a worker tells the leader so it is sent
    /// only what it lacks.
    pub(crate) fn versions(&self) -> Vec<(String, i64)> {
        let certificates = self.certificates.iter().map(|(n, c)| (n.clone(), c.version));
        self.stored.keys().cloned().chain(certificates).collect()
    }

    /// The value, or `None` when this worker cannot obtain it. A leader-stored reference matches one version and no
    /// other: a bundle quoting version 7 runs on version 7, never on whatever arrived since.
    pub fn value(&self, reference: &SecretRef) -> Option<String> {
        match reference {
            SecretRef::WorkerLocal(var) => usable(std::env::var(var).ok()),
            SecretRef::LeaderStored { name, version } => self.stored.get(&(name.clone(), *version)).cloned(),
        }
    }

    /// The version of the certificate `name` this holder has, which is its newest.
    pub(crate) fn certificate_version(&self, name: &str) -> Option<i64> {
        self.certificates.get(name).map(|held| held.version)
    }

    /// The chain and private key of the certificate `name`, PEM both, at the newest version this worker holds.
    pub(crate) fn certificate(&self, name: &str) -> Option<(String, String)> {
        let held = self.certificates.get(name)?;
        Some((held.chain.clone(), held.key.clone()))
    }
}

/// Why `tls`'s certificate cannot be presented from the side its bundle presents it, once the worker holds it.
/// `None` when there is nothing to check yet: no certificate named, or one this holder does not have at all, which
/// is [`TopologyConfig::missing_secrets`]' concern.
///
/// A free function rather than a method, so [`crate::worker::capture`]'s cold path -- which builds one source and
/// never has a whole [`TopologyConfig`] to ask -- can run the same check on the one it has.
pub(crate) fn certificate_error(tls: &Tls, held: &HeldSecrets) -> Option<String> {
    let (chain, _) = held.certificate(tls.certificate.as_ref()?)?;
    crate::io::trust::refuse_unless_usable(&chain, tls.side)
        .err()
        .map(|why| {
            let verb = if tls.side == crate::io::trust::Side::Server {
                "serves"
            } else {
                "presents"
            };
            format!("{} {verb} a certificate {why}", tls.of)
        })
}

impl TopologyConfig {
    /// The TLS every connector of this bundle speaks, where it speaks any of its own.
    fn tls(&self) -> impl Iterator<Item = &Tls> {
        let sources = self.sources.iter().filter_map(|s| s.kind.tls());
        let destinations = self.destinations.iter().filter_map(|d| d.kind.tls());
        sources.chain(destinations)
    }

    /// The named credentials and certificates this worker cannot obtain, as ready-to-print lines. Empty means the
    /// bundle can be deployed here.
    pub(crate) fn missing_secrets(&self, held: &HeldSecrets) -> Vec<String> {
        let secrets = self
            .named_secrets
            .iter()
            .filter(|s| held.value(&s.reference).is_none())
            .map(|s| {
                format!(
                    "{} names {}, which this worker does not have",
                    s.what,
                    s.reference.describe()
                )
            });
        let certificates = self
            .named_certificates
            .iter()
            .filter(|c| held.certificate(&c.name).is_none())
            .map(|c| {
                format!(
                    "{} names the certificate {:?}, which this worker does not have",
                    c.what, c.name
                )
            });
        secrets.chain(certificates).collect()
    }

    /// Every connector whose certificate cannot be presented the way it presents it, among the certificates this
    /// worker holds. Checked wherever a commit is about to run, so a certificate a far end would refuse is refused
    /// before anything is torn down rather than discovered at the handshake, after it already has been.
    pub(crate) fn certificate_errors(&self, held: &HeldSecrets) -> Vec<String> {
        self.tls().filter_map(|tls| certificate_error(tls, held)).collect()
    }
}

// -- compiling ----------------------------------------------------------

/// A `RouteConfig` with its condition (if any) resolved to `Ir` — compiled against a minimal, ingest-fields-only
/// `SymbolTable` (`plan::ingest_fields()` gives exactly `__live`/`__source.offset`/`_raw`), not any target pipeline's
/// declared fields. Routing has to decide which pipeline (and therefore which parser, which declared fields)
/// applies before any of that exists.
#[derive(Clone, Debug)]
pub struct CompiledRoute {
    /// `RouteConfig::id`, carried through compiling so a caller holding only the compiled form can still name the
    /// route -- the map, and route preview's own report of which route a row matched.
    pub(crate) id: String,
    /// `RouteConfig::name`.
    pub(crate) name: String,
    pub(crate) source: SourceId,
    pub(crate) cond: Option<Ir>,
    pub(crate) pipeline: usize,
    /// This route's own delivery rules, destinations resolved to `DestId`s and each `when` compiled against the target
    /// pipeline's *output*.
    pub(crate) deliver: Vec<CompiledDeliverRule>,
    /// Every regex literal `cond` interned, compiled once here — this route's own counterpart to
    /// `Plan::compiled_patterns`. A route condition resolves against its own disjoint `ResourceBuilder` (see this
    /// struct's doc), so it cannot share a target pipeline's `compiled_patterns`; `route_masks` points `Ctx::patterns`
    /// at this before evaluating `cond`, the same way `plan::exec::run` points it at `Plan::compiled_patterns`.
    pub(crate) compiled_patterns: crate::core::id::ById<crate::core::id::PatternId, crate::core::ctx::CompiledPattern>,
    /// `RouteConfig::on_exception`.
    pub(crate) on_exception: OnException,
}

impl CompiledRoute {
    /// Whether this route sends rows to `dest`: a `deliver` rule's or an exception answer's target.
    pub(crate) fn delivers_to(&self, dest: DestId) -> bool {
        self.deliver.iter().any(|rule| rule.to.contains(&dest))
            || self.on_exception.answers.iter().any(|a| a.targets().contains(&dest))
    }
}

impl CompiledTopology {
    /// Every destination a row entering through `source` could reach: the union of that source's routes' `deliver`
    /// targets, plus the DLQ each of those salvages its refusals to.
    ///
    /// A worker thread serves exactly one source, so this is what it actually needs to connect to. Building a producer
    /// per destination *in the bundle* instead costs a client and a background thread for every destination the thread
    /// can never write to, times every thread.
    pub fn destinations_reachable_from(&self, source: SourceId) -> Vec<DestId> {
        // The group's answers to `Unmatched` and `Unprocessable`, which no route names and a source with no routes
        // at all still reaches.
        let delivery = [ExceptionKind::Unmatched, ExceptionKind::Unprocessable].map(|k| self.on_exception.answer(&k));
        let mut out: Vec<DestId> = delivery.iter().flat_map(|a| a.targets()).copied().collect();
        out = self.with_dlqs(out);
        for r in self.routes.iter().filter(|r| r.source == source) {
            out.extend(self.written_by(r));
        }
        out.sort_unstable();
        out.dedup();
        out
    }

    /// Every destination `route` writes a row to: those it delivers to, those its exceptions' answers deliver to, and
    /// down each one's DLQ chain, since a DLQ answers for what it refuses with its own policy.
    pub(crate) fn written_by(&self, route: &CompiledRoute) -> Vec<DestId> {
        let delivered = route.deliver.iter().flat_map(|rule| rule.to.iter().copied());
        let answered = route.on_exception.answers.iter().flat_map(|a| a.targets()).copied();
        self.with_dlqs(delivered.chain(answered).collect())
    }

    /// `dests` and every DLQ down their chains, each once. `dlq_of` already holds each whole chain.
    pub(crate) fn with_dlqs(&self, mut dests: Vec<DestId>) -> Vec<DestId> {
        let salvage: Vec<DestId> = dests
            .iter()
            .filter_map(|d| self.dlq_of.get(d))
            .flatten()
            .copied()
            .collect();
        dests.extend(salvage);
        dests.sort_unstable();
        dests.dedup();
        dests
    }

    /// The destinations `source`'s routes deliver to: what the flow goes to, and none of the places a row goes
    /// when it fails, matches nothing, or cannot be processed. Those are in `destinations_reachable_from` and carry a
    /// trickle beside this.
    pub(crate) fn delivery_destinations_from(&self, source: SourceId) -> Vec<DestId> {
        let mut out: Vec<DestId> = self
            .routes
            .iter()
            .filter(|r| r.source == source)
            .flat_map(|r| r.deliver.iter().flat_map(|rule| rule.to.iter().copied()))
            .collect();
        out.sort_unstable();
        out.dedup();
        out
    }
}

fn compile_routes(routes: &[RouteConfig], metas: &SourceMetas) -> Result<Vec<CompiledRoute>, Vec<CompileError>> {
    let mut out = Vec::with_capacity(routes.len());
    let mut errors = Vec::new();
    for (i, r) in routes.iter().enumerate() {
        match compile_route(r, &meta_of(metas, r.source)) {
            Ok(c) => out.push(c),
            Err(e) => errors.push(CompileError { route: Some(i), ..e }),
        }
    }
    if errors.is_empty() { Ok(out) } else { Err(errors) }
}

/// `ir` as a condition: `bool`, or null, which reads false and is wrapped so the column the runtime reads is one.
pub(crate) fn condition(ir: Ir, src: &str, what: &str) -> Result<Ir, CompileError> {
    if !crate::plan::resolve::is_condition(&ir.ty) {
        return Err(CompileError::new(
            None,
            format!("{what} `{src}` must be bool, resolved to {}", ir.ty.spell()),
        ));
    }
    Ok(if ir.ty == DataType::Null {
        flatten(crate::plan::compile::is_true(ir))
    } else {
        ir
    })
}

fn compile_route(r: &RouteConfig, meta: &SourceMeta) -> Result<CompiledRoute, CompileError> {
    let mut res = ResourceBuilder::default();
    let cond = match &r.cond {
        None => None,
        Some(src) => {
            let ast = parse(src, None)?;
            let mut syms = SymbolTable::new(&ingest_fields(meta));
            let ir = flatten(resolve(&ast, &mut syms, &mut res)?);
            Some(condition(ir, src, "route condition")?)
        }
    };
    let compiled_patterns = crate::plan::compile::compile_patterns(&res.patterns)?;
    Ok(CompiledRoute {
        id: r.id.clone(),
        name: r.name.clone(),
        source: r.source,
        cond,
        // Filled in by `compile_topology`, which has the pipeline a `when` must resolve against.
        deliver: Vec::new(),
        pipeline: r.pipeline,
        compiled_patterns,
        on_exception: r.on_exception.clone(),
    })
}

/// Every route, compiled, plus every pipeline it can reference, compiled — what `runtime::Worker` needs to actually run
/// a topology. Validates that every route's `pipeline` id resolves to a real compiled pipeline, so `runtime`'s driver
/// loop can assume that rather than handle it as a per-batch runtime error.
pub struct CompiledTopology {
    pub routes: Vec<CompiledRoute>,
    /// Keyed by the source whose metadata the plan was compiled against, and the pipeline id. Keyed by the metadata a
    /// plan was compiled against, so sources that declare the same fields share one. Reach it with `plan_for`.
    pub(crate) pipelines: HashMap<(SourceMeta, usize), Plan>,
    /// What each source declared when these plans were compiled. The runtime reads it from here rather than being told
    /// separately, so a plan's `reads_meta` and the fields the batch is built from cannot disagree.
    pub(crate) metas: SourceMetas,
    /// The group's answer to every exception kind (design doc, Exception taxonomy). The runtime reads `Unmatched` and
    /// `Unprocessable` here: neither reaches a route that could answer for it.
    pub on_exception: OnException,
    /// Each destination that delivers what it refuses, and every DLQ a refusal there can reach: the ones it names, theirs,
    /// and so on down each chain (`dlq_chain`). A destination's own setting, not the compiler's, so
    /// `config_file::LoadedSystem::compile_topology` fills it; `destinations_reachable_from` reads it so a DLQ is
    /// connected wherever a refusal can send a row to it.
    pub dlq_of: HashMap<DestId, Vec<DestId>>,
    /// What each Parquet destination writes, route by route. Filled with `dlq_of`, since a DLQ takes the same rows.
    pub records: HashMap<DestId, Records>,
    /// Each destination that may hold a variant as Parquet's variant (`DestinationKind::holds_native_variants`).
    /// Filled with `dlq_of`.
    pub(crate) native_variants: HashSet<DestId>,
    /// What each destination writes of a row (`DestinationKind::payload`). Filled with `dlq_of`; one missing writes the
    /// record.
    pub payloads: HashMap<DestId, Payload>,
    /// Each destination that names its documents by a field of the row, and that field
    /// (`DestinationKind::id_field`). Filled with `dlq_of`.
    pub id_fields: Vec<(DestId, String)>,
}

impl std::fmt::Debug for CompiledTopology {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompiledTopology")
            .field("routes", &self.routes.len())
            .finish_non_exhaustive()
    }
}

impl CompiledTopology {
    /// What `dest` writes of a row. One no destination config names, a worker's built by hand, writes the record.
    pub(crate) fn payload(&self, dest: DestId) -> Payload {
        self.payloads.get(&dest).cloned().unwrap_or(Payload::Record)
    }

    /// The forms a row bound for `dest` keeps for `dlqs`, the DLQs down its chain: each one's, but `dest`'s own.
    pub(crate) fn kept<'d>(&self, dest: DestId, dlqs: impl IntoIterator<Item = &'d DestId>) -> Vec<Payload> {
        let own = self.payload(dest);
        let mut kept = Vec::new();
        for &dlq in dlqs {
            let p = self.payload(dlq);
            if p != own && !kept.contains(&p) {
                kept.push(p);
            }
        }
        kept
    }

    /// Every field a destination writes as text, each once, sorted.
    pub(crate) fn text_fields(&self) -> Vec<String> {
        let mut fields: Vec<String> = Vec::new();
        for p in self.payloads.values() {
            if let Payload::Text { field } = p
                && !fields.iter().any(|f| **f == **field)
            {
                fields.push(field.to_string());
            }
        }
        fields.sort();
        fields
    }

    /// What `source`'s records carry, as its plans were compiled to expect.
    pub fn meta_for(&self, source: SourceId) -> SourceMeta {
        meta_of(&self.metas, source)
    }

    /// The plan a row entering through `source` runs under `pipeline`.
    pub fn plan_for(&self, source: SourceId, pipeline: usize) -> Option<&Plan> {
        self.pipelines.get(&(self.meta_for(source), pipeline))
    }

    /// Each route that sends rows to `dest`, with its index.
    pub(crate) fn delivering(&self, dest: DestId) -> impl Iterator<Item = (usize, &CompiledRoute)> {
        self.routes.iter().enumerate().filter(move |(_, r)| r.delivers_to(dest))
    }

    /// A route's finished record, as a condition over it reads it.
    pub(crate) fn output_of(&self, route: &CompiledRoute) -> SymbolTable {
        let meta = self.meta_for(route.source);
        let plan = (self.plan_for(route.source, route.pipeline))
            .expect("compile_topology validated every route's pipeline id");
        output_symbols(plan, &route.on_exception.tag_field, &meta)
    }

    /// A capture filter at `dest`, compiled against the output of each route delivering there, by route index. A
    /// filter runs on each of them it compiles on, since they may give `dest` rows of different shapes.
    pub(crate) fn capture_filters(
        &self,
        dest: DestId,
        src: &str,
    ) -> Vec<(usize, Result<OutputCondition, CompileError>)> {
        self.delivering(dest)
            .map(|(idx, r)| {
                let plan = (self.plan_for(r.source, r.pipeline))
                    .expect("compile_topology validated every route's pipeline id");
                (
                    idx,
                    output_condition(
                        src,
                        "a capture filter",
                        plan,
                        &r.on_exception.tag_field,
                        &self.meta_for(r.source),
                    ),
                )
            })
            .collect()
    }

    /// Whether anything `source`'s records reach reads its field `field`: a route's condition, its pipeline or its
    /// delivery rules.
    pub fn reads(&self, source: SourceId, field: &str) -> bool {
        let Some(i) = self
            .meta_for(source)
            .source_fields()
            .iter()
            .position(|(name, _)| *name == field)
        else {
            return false;
        };
        self.routes
            .iter()
            .filter(|r| r.source == source)
            .any(|r| self.plan_for(source, r.pipeline).is_some_and(|p| p.reads_meta[i]))
    }
}

/// `compile_topology_with` over sources that all declare Kafka's fields. The assumption is stated here, at a call site
/// that has no sources to ask, rather than inside the compiler where every caller would inherit it.
pub fn compile_topology(
    routes: &[RouteConfig],
    pipelines: &[PipelineConfig],
) -> Result<CompiledTopology, Vec<CompileError>> {
    let metas: SourceMetas = routes.iter().map(|r| (r.source, SourceMeta::KAFKA)).collect();
    compile_topology_with(routes, pipelines, OnException::default(), &metas)
}

/// The compiler's entry point: every production path reaches it through `LoadedSystem::compile_topology`, with the
/// group's answers and the metadata each source declares. `compile_topology` is the two-argument form for a
/// caller with neither to pass.
pub fn compile_topology_with(
    routes: &[RouteConfig],
    pipelines: &[PipelineConfig],
    on_exception: OnException,
    metas: &SourceMetas,
) -> Result<CompiledTopology, Vec<CompileError>> {
    let mut errors = Vec::new();

    let by_id: HashMap<usize, &PipelineConfig> = pipelines.iter().map(|p| (p.id, p)).collect();

    // What a plan depends on is the names in scope, which is the source's metadata -- not the source. Two Kafka sources
    // routed to one pipeline would compile it to the same instructions, the same slots and the same loaded lookup
    // tables, so they share one plan. The metadata carries the encoding, so two that differ only there compile apart,
    // to the same plan twice. Route order, deduped, so the errors a config produces stay in the same order from run
    // to run.
    let mut seen = std::collections::HashSet::new();
    let pairs: Vec<(SourceMeta, usize)> = routes
        .iter()
        .map(|r| (meta_of(metas, r.source), r.pipeline))
        .filter(|pair| seen.insert(pair.clone()))
        .collect();

    let mut compiled_pipelines: HashMap<(SourceMeta, usize), Plan> = HashMap::new();
    for (meta, pipeline) in pairs {
        let Some(p) = by_id.get(&pipeline) else {
            continue; // a route naming no pipeline is reported below
        };
        match compile(p, &meta) {
            Ok(plan) => {
                compiled_pipelines.insert((meta, pipeline), plan);
            }
            // A save compiles every pipeline in one call, so a bare `function` id needs `p.id` alongside it to
            // say which pipeline it came from.
            Err(e) => errors.extend(e.into_iter().map(|err| err.in_pipeline(p.id))),
        }
    }

    // An unrouted pipeline is checked against `SourceMeta::KAFKA` -- see its doc for why.
    for p in pipelines {
        if !routes.iter().any(|r| r.pipeline == p.id)
            && let Err(e) = compile(p, &SourceMeta::KAFKA)
        {
            errors.extend(e.into_iter().map(|err| err.in_pipeline(p.id)));
        }
    }

    let mut compiled_routes = match compile_routes(routes, metas) {
        Ok(r) => r,
        Err(mut e) => {
            errors.append(&mut e);
            Vec::new()
        }
    };

    // A `when` resolves against its target pipeline's *output*, so it can only be compiled once that pipeline has been
    // -- which is why this happens here rather than in `compile_route`.
    //
    // It also reads the batch the pipeline wrote, so a metadata field named only by a `when` still has to be built.
    // So does one named only by the route's condition: the route chose the row by it, so a repair it made must reach
    // the row's exceptions. `Plan::reads_meta` is derived from the pipeline's own instructions, which cannot see these
    // rules, so their reads are collected here and folded in below.
    let mut rule_reads: HashMap<(SourceMeta, usize), Vec<bool>> = HashMap::new();
    let mut mark = |key: (SourceMeta, usize), ir: &Ir| {
        let meta = key.0.clone();
        let reads = rule_reads
            .entry(key)
            .or_insert_with(|| vec![false; meta.source_fields().len()]);
        crate::plan::compile::walk_slots(ir, &mut |slot| {
            if let Some(i) = meta.source_field_at(slot) {
                reads[i] = true;
            }
        });
    };
    for (r, config) in routes.iter().enumerate() {
        let Some(compiled) = compiled_routes.get_mut(r) else {
            continue;
        };
        let meta = meta_of(metas, config.source);
        let Some(plan) = compiled_pipelines.get(&(meta.clone(), config.pipeline)) else {
            continue; // reported just below
        };
        if let Some(cond) = &compiled.cond {
            mark((meta.clone(), config.pipeline), cond);
        }
        for (i, rule) in config.deliver.iter().enumerate() {
            match compile_deliver_rule(rule, plan, &config.on_exception.tag_field, &meta) {
                Ok(c) => {
                    if let Some(ir) = &c.when {
                        mark((meta.clone(), config.pipeline), ir);
                    }
                    compiled.deliver.push(c);
                }
                Err(mut e) => {
                    e.route = Some(r);
                    e.rule = Some(i as u32);
                    errors.push(e);
                }
            }
        }
    }

    for (key, reads) in rule_reads {
        if let Some(plan) = compiled_pipelines.get_mut(&key) {
            for (built, wanted) in plan.reads_meta.iter_mut().zip(reads) {
                *built |= wanted;
            }
        }
    }

    for (r, route) in compiled_routes.iter().enumerate() {
        if !by_id.contains_key(&route.pipeline) {
            errors.push(CompileError::new(
                None,
                format!(
                    "route {r} (source {}) references pipeline id {}, which doesn't exist",
                    route.source, route.pipeline
                ),
            ));
        }
    }

    if errors.is_empty() {
        Ok(CompiledTopology {
            routes: compiled_routes,
            pipelines: compiled_pipelines,
            metas: metas.clone(),
            on_exception,
            dlq_of: HashMap::new(),
            records: HashMap::new(),
            native_variants: HashSet::new(),
            payloads: HashMap::new(),
            id_fields: Vec::new(),
        })
    } else {
        Err(errors)
    }
}

// -- evaluating -----------------------------------------------------------

/// Compile one `deliver` rule against its target pipeline's output.
///
/// The condition sees exactly what the finished batch holds: every field the pipeline left live, the ingest names,
/// and the route's tag field (`Plan::tag_slot`), written after the pipeline's last step. That is the capability
/// `RouteConfig::cond` lacks -- it runs on the ingest batch, before the pipeline, so it can only test `_raw`.
///
/// It reads the row as it will be sent: the tag field is the tag on a tagged row, and what the row holds there on any
/// other.
fn compile_deliver_rule(
    rule: &DeliverRule,
    plan: &Plan,
    tag_field: &str,
    meta: &SourceMeta,
) -> Result<CompiledDeliverRule, CompileError> {
    let Some(src) = &rule.when else {
        return Ok(CompiledDeliverRule {
            when: None,
            to: rule.to.clone(),
            compiled_patterns: Default::default(),
        });
    };
    let (ir, compiled_patterns) = output_condition(src, "condition", plan, tag_field, meta)?;
    Ok(CompiledDeliverRule {
        when: Some(ir),
        to: rule.to.clone(),
        compiled_patterns,
    })
}

/// The names an expression over `plan`'s finished batch reads, for a row from a source of `meta`: the pipeline's
/// output, the source's own fields, and the route's tag field.
pub(crate) fn output_symbols(plan: &Plan, tag_field: &str, meta: &SourceMeta) -> SymbolTable {
    SymbolTable::over_plan_output(
        &plan.delivery_layout(),
        &plan.tree,
        &ingest_fields(meta),
        &[(tag_field, plan.tag_slot())],
    )
}

/// A condition over a finished batch, and the patterns `Ctx::patterns` is pointed at to evaluate it.
type OutputCondition = (
    Ir,
    crate::core::id::ById<crate::core::id::PatternId, crate::core::ctx::CompiledPattern>,
);

/// Compile `src`, called `what` in an error, as a condition over `plan`'s finished batch: what a `deliver` rule reads,
/// and what a filter at a destination reads. Evaluated with `Ctx::patterns` pointed at the patterns it returns.
pub(crate) fn output_condition(
    src: &str,
    what: &str,
    plan: &Plan,
    tag_field: &str,
    meta: &SourceMeta,
) -> Result<OutputCondition, CompileError> {
    let mut res = ResourceBuilder::default();
    let ast = parse(src, None)?;
    let ir = flatten(resolve(&ast, &mut output_symbols(plan, tag_field, meta), &mut res)?);
    let ir = condition(ir, src, what)?;
    Ok((ir, crate::plan::compile::compile_patterns(&res.patterns)?))
}

/// Evaluate every route belonging to `source` against `batch` — which must have the source's `ingest_fields` layout,
/// its record filled (and `__source.offset`, if the source has one) and `__live` primed all-true. Routes are unordered:
/// each sees the whole batch, so a row matching two routes runs both pipelines, and `__live` is left as it came.
///
/// Returns, for each route belonging to `source`, its index into `routes` and the Bool mask of rows dispatched to it.
/// A route that matches nothing still gets an all-false mask, since a batch's routing shape can vary row to row.
pub fn route_masks<'a>(
    routes: &'a [CompiledRoute],
    source: SourceId,
    batch: &mut Batch,
    ctx: &mut Ctx<'a>,
) -> Vec<(usize, Column)> {
    let sp = Span::default();
    let live = || Ir::new(DataType::Bool, sp, IrKind::Field(SLOT_LIVE));

    let mut out = Vec::new();
    for (i, r) in routes.iter().enumerate() {
        if r.source != source {
            continue;
        }

        ctx.patterns = &r.compiled_patterns;

        let matched_ir = match &r.cond {
            None => live(),
            Some(cond) => Ir::new(
                DataType::Bool,
                sp,
                IrKind::Binary {
                    op: BinOp::And,
                    lhs: Box::new(live()),
                    rhs: Box::new(cond.clone()),
                },
            ),
        };
        let matched = eval(&matched_ir, batch, ctx);
        out.push((i, matched));
    }
    out
}

mod stored;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::bits;
    use crate::core::buffer::BufferPool;
    use crate::core::column::{StrBuilder, VarType};
    use crate::core::error::ErrorSink;
    use crate::heartbeat::pasted_from;

    #[test]
    fn an_objects_stream_is_its_directories_up_to_the_first_dated_one() {
        assert_eq!(object_prefix(b"logs/okta/2026/10/14/a.json.gz"), "logs/okta/");
        assert_eq!(object_prefix(b"logs/okta/2026/10/15/b.json.gz"), "logs/okta/");
        assert_eq!(
            object_prefix(b"okta-logs/a.json"),
            "okta-logs/",
            "the object's name is not the stream's"
        );
        assert_eq!(object_prefix(b"a.json"), "");
        assert_eq!(
            object_prefix(b"dt=2026-10-14/part-0.parquet"),
            "",
            "dated from the root"
        );
    }

    #[test]
    fn a_stream_key_is_what_the_kind_files_a_record_under() {
        let record = |topic: &str, filed: Option<Vec<(String, Vec<u8>)>>| crate::io::SourceRecord {
            chunk: crate::io::RecordChunk::of_bytes(b"{}"),
            row: 0,
            offset: 0,
            partition: 0,
            topic: std::sync::Arc::from(topic),
            headers: filed,
        };
        let filed = |key: &str, value: &str| Some(vec![(key.to_string(), value.as_bytes().to_vec())]);
        let st = record("10.0.0.9", filed("sourcetype", "okta:system"));
        assert_eq!(SourceMeta::HEC_RECEIVER.stream_key(&st).as_deref(), Some("okta:system"));
        assert_eq!(
            SourceMeta::HEC_RECEIVER.stream_key(&record("10.0.0.9", None)),
            None,
            "a sender that named no sourcetype"
        );
        let index = record("10.0.0.9", filed("index", "logs-okta"));
        assert_eq!(
            SourceMeta::ELASTIC_RECEIVER.stream_key(&index).as_deref(),
            Some("logs-okta")
        );
        let object = record("landing/logs/okta/2026/10/14/a.json.gz", None);
        assert_eq!(
            SourceMeta::S3_SQS.stream_key(&object).as_deref(),
            Some("landing/logs/okta/")
        );
        let topic = record("orders", None);
        assert_eq!(SourceMeta::KAFKA.stream_key(&topic).as_deref(), Some("orders"));
        let remote = record("10.0.0.9", None);
        assert_eq!(
            SourceMeta::HTTP_RECEIVER.stream_key(&remote).as_deref(),
            Some("10.0.0.9")
        );
    }

    /// Which of `records` the condition `meta` gives `key` takes, run as a route on `meta`'s source.
    fn taken_by_stream(meta: SourceMeta, key: &str, records: &[crate::io::SourceRecord]) -> Vec<bool> {
        let cond = meta.stream_condition(key).expect("a condition");
        let routes = compile_routes(
            &[route(SourceId::new(0), Some(&cond), 0)],
            &SourceMetas::from([(SourceId::new(0), meta.clone())]),
        )
        .unwrap_or_else(|e| panic!("`{cond}` compiles: {e:?}"));
        let layout: Vec<DataType> = crate::plan::compile::ingest_fields(&meta)
            .into_iter()
            .map(|(_, t)| t)
            .collect();
        let mut pool = BufferPool::new();
        let mut errors = ErrorSink::new();
        let mut batch = Batch::new(&layout, records.len(), 0);
        let texts = vec![""; records.len()];
        meta.ingest(&mut batch, records, &texts, None, None, &mut pool);
        let mut live = Column::alloc(&mut pool, &DataType::Bool, records.len());
        bits::fill_valid(live.bool_mut().unwrap().values, records.len());
        batch.put(SLOT_LIVE, live, &mut pool);
        let mut ctx = Ctx::new(&mut pool, &mut errors);
        let masks = route_masks(&routes, SourceId::new(0), &mut batch, &mut ctx);
        let mask = masks[0].1.bool().unwrap().values;
        let taken = (0..records.len()).map(|r| bits::get_row(mask, r)).collect();
        batch.reclaim(&mut pool);
        masks.into_iter().for_each(|(_, c)| c.reclaim(&mut pool));
        taken
    }

    #[test]
    fn a_streams_condition_takes_the_records_filed_under_its_key() {
        let record = |topic: &str, filed: Option<Vec<(String, Vec<u8>)>>| crate::io::SourceRecord {
            chunk: crate::io::RecordChunk::of_bytes(b"{}"),
            row: 0,
            offset: 0,
            partition: 0,
            topic: std::sync::Arc::from(topic),
            headers: filed,
        };
        let filed = |key: &str, value: &str| Some(vec![(key.to_string(), value.as_bytes().to_vec())]);
        let cases = [
            (
                SourceMeta::HEC_RECEIVER,
                [
                    record("10.0.0.9", filed("sourcetype", "okta:system")),
                    record("10.0.0.9", filed("sourcetype", "other")),
                ],
            ),
            (
                SourceMeta::HEC_RECEIVER,
                [
                    record("10.0.0.9", None),
                    record("10.0.0.9", filed("sourcetype", "other")),
                ],
            ),
            (
                SourceMeta::ELASTIC_RECEIVER,
                [
                    record("10.0.0.9", filed("index", "logs-\"okta\"")),
                    record("10.0.0.9", filed("index", "logs")),
                ],
            ),
            (
                SourceMeta::S3_SQS,
                [
                    record("landing/logs/okta/2026/10/14/a.json.gz", None),
                    record("landing/logs/duo/a.json", None),
                ],
            ),
            (
                SourceMeta::S3_SQS,
                [record("landing/2026/a.json", None), record("other/a.json", None)],
            ),
            (SourceMeta::KAFKA, [record("orders", None), record("returns", None)]),
            (
                SourceMeta::HTTP_RECEIVER,
                [record("10.0.0.9", None), record("10.0.0.10", None)],
            ),
        ];
        for (meta, records) in cases {
            // As `Streams::saw_unmatched` keys a record its kind files under nothing.
            let key = meta.stream_key(&records[0]).unwrap_or_default().into_owned();
            assert_eq!(
                taken_by_stream(meta.clone(), &key, &records),
                [true, false],
                "{key} on {}",
                meta.kind
            );
        }
    }

    fn field(meta: &SourceMeta, name: &str) -> usize {
        meta.source_fields()
            .iter()
            .position(|(n, _)| *n == name)
            .expect("declared")
    }

    /// The payload a source read is its `__source.bytes`, byte for byte, invalid UTF-8 included. A typed source
    /// declares none: a lone `bytes` field is a field of its record, written by naming it, and never decoded into `_raw`.
    #[test]
    fn only_a_source_that_reads_payloads_has_source_bytes() {
        let read = b"\xffline\x00".to_vec();
        let record = crate::io::SourceRecord {
            chunk: crate::io::RecordChunk::of_bytes(&read),
            row: 0,
            offset: 0,
            partition: 0,
            topic: std::sync::Arc::from("t"),
            headers: None,
        };
        let mut pool = crate::core::buffer::BufferPool::new();
        let one = std::slice::from_ref(&record);
        let decoded = SourceMeta::KAFKA.decode(one, None::<&[&str]>, None, None, &mut pool);
        assert_eq!(SourceMeta::KAFKA.read_bytes(&record), Some(&read[..]));
        assert_eq!(decoded.record_bytes(&SourceMeta::KAFKA, 0, "_raw"), None);
        decoded.batch.reclaim(&mut pool);

        let typed = SourceMeta::KAFKA
            .declaring(vec![("payload".into(), DataType::Bytes)])
            .unwrap();
        assert_eq!(typed.read_bytes(&record), None);
        assert_eq!(typed.field_type(SOURCE_BYTES), None);
        let decoded = typed.decode(one, None::<&[&str]>, None, None, &mut pool);
        assert_eq!(decoded.record_bytes(&typed, 0, "payload"), Some(read));
        decoded.batch.reclaim(&mut pool);
    }

    /// A pipeline reads `__source.bytes` as a `bytes` column of each payload as read, invalid UTF-8 included, beside
    /// `_raw`, the same payload decoded.
    #[test]
    fn a_pipeline_reads_the_payload_as_read() {
        let read = b"caf\xe9".to_vec();
        let records = [crate::io::SourceRecord {
            chunk: crate::io::RecordChunk::of_bytes(&read),
            row: 0,
            offset: 0,
            partition: 0,
            topic: std::sync::Arc::from("t"),
            headers: None,
        }];
        let meta = SourceMeta::KAFKA;
        let layout: Vec<DataType> = crate::plan::compile::ingest_fields(&meta)
            .into_iter()
            .map(|(_, t)| t)
            .collect();
        let mut batch = Batch::new(&layout, 1, 0);
        let mut pool = crate::core::buffer::BufferPool::new();
        let at = meta
            .source_fields()
            .iter()
            .position(|(n, _)| *n == SOURCE_BYTES)
            .unwrap();
        let (noted, _) = meta.ingest(&mut batch, &records, &["caf\u{fffd}"], None, None, &mut pool);

        assert!(noted.is_empty(), "{noted:?}");
        let column = batch.get(meta.source_slot(at));
        assert_eq!(column.data_type(), DataType::Bytes);
        assert_eq!(column.varlen().unwrap().get(0), &read[..]);
        assert_eq!(
            batch.get(crate::core::batch::SLOT_RAW).varlen().unwrap().get(0),
            "caf\u{fffd}".as_bytes()
        );
        batch.reclaim(&mut pool);
    }

    /// A source that declares `encoding: none` has no `_raw`: ingest writes none, a record with no pipeline is `{}`,
    /// and its payload is `__source.bytes` alone.
    #[test]
    fn a_source_that_decodes_none_has_no_raw() {
        let read = b"caf\xe9".to_vec();
        let records = [crate::io::SourceRecord {
            chunk: crate::io::RecordChunk::of_bytes(&read),
            row: 0,
            offset: 0,
            partition: 0,
            topic: std::sync::Arc::from("t"),
            headers: None,
        }];
        let meta = SourceMeta::KAFKA.with_encoding(Encoding::None);
        assert!(meta.record.is_empty());
        assert!(meta.reads_bytes() && !meta.raw_record());
        assert_eq!(meta.field_type("_raw"), None);
        assert_eq!(meta.field_type(SOURCE_BYTES), Some(DataType::Bytes));
        assert_eq!(SourceMeta::KAFKA.with_encoding(Encoding::Auto), SourceMeta::KAFKA);
        // A source that reads no bytes keeps no encoding, so the meta it declares and the meta it runs with are one.
        assert_eq!(
            SourceMeta::MYSQL.with_encoding(Encoding::Fixed(encoding_rs::UTF_8)),
            SourceMeta::MYSQL
        );

        let layout: Vec<DataType> = crate::plan::compile::ingest_fields(&meta)
            .into_iter()
            .map(|(_, t)| t)
            .collect();
        let mut batch = Batch::new(&layout, 1, 0);
        let mut pool = crate::core::buffer::BufferPool::new();
        let at = meta
            .source_fields()
            .iter()
            .position(|(n, _)| *n == SOURCE_BYTES)
            .unwrap();
        let (noted, _) = meta.ingest(&mut batch, &records, &[""], None, None, &mut pool);
        assert!(noted.is_empty(), "{noted:?}");
        assert_eq!(batch.get(meta.source_slot(at)).varlen().unwrap().get(0), &read[..]);
        batch.reclaim(&mut pool);

        assert_eq!(meta.record_json(&records[0]), b"{}");
        let decoded = meta.decode(&records, None::<&[&str]>, None, None, &mut pool);
        assert_eq!(decoded.record_text(&meta, 0, "_raw"), None);
        assert_eq!(meta.read_bytes(&records[0]), Some(&read[..]));
        decoded.batch.reclaim(&mut pool);
    }

    /// The GUI reads a table as records by the same rule (`gui-frontend/src/capture.ts`): both read these cases.
    #[test]
    fn a_table_reads_as_the_records_the_gui_shows() {
        #[derive(serde::Deserialize)]
        struct Case {
            name: String,
            #[serde(flatten)]
            table: crate::heartbeat::CaptureBatch,
            records: Vec<crate::io::RecordObject>,
        }
        let cases: Vec<Case> = serde_json::from_str(include_str!("../../tests/fixtures/table-records.json")).unwrap();
        for case in cases {
            let meta = SourceMeta::of_columns(&case.table.columns).unwrap();
            let read = crate::leader::preview::read_sample(&meta, &case.table).unwrap();
            let mut pool = crate::core::buffer::BufferPool::new();
            let decoded = meta.decode(&read.records, Some(&read.texts), None, None, &mut pool);
            let records: Vec<_> = (0..read.records.len())
                .map(|row| decoded.record_object(&meta, row))
                .collect();
            decoded.batch.reclaim(&mut pool);
            assert_eq!(records, case.records, "{}", case.name);
        }
    }

    /// A stored `__source` object keeps the payload as base64 `bytes`, so a record reads back byte for byte, whether
    /// its source decodes it or not.
    #[test]
    fn a_stored_payload_reads_back_as_it_was_read() {
        let read = b"caf\xe9".to_vec();
        let records = [crate::io::SourceRecord {
            chunk: crate::io::RecordChunk::of_bytes(&read),
            row: 0,
            offset: 0,
            partition: 0,
            topic: std::sync::Arc::from("t"),
            headers: None,
        }];
        for meta in [SourceMeta::KAFKA, SourceMeta::KAFKA.with_encoding(Encoding::None)] {
            let fields = meta.objects(&records, None);
            assert_eq!(fields[0]["bytes"], serde_json::json!("Y2Fm6Q=="));
            let record = meta.record_object(&records[0]);
            let back = meta.read_table(&pasted_from(&[(&record, &fields[0])])).unwrap();
            assert_eq!(meta.read_bytes(&back[0]), Some(&read[..]));
        }
        let none = SourceMeta::KAFKA.with_encoding(Encoding::None);
        let err = none
            .read_table(&pasted_from(&[(
                &crate::io::RecordObject::new(),
                &crate::io::SourceFields::new(),
            )]))
            .unwrap_err();
        assert!(err.contains("`__source.bytes`"), "{err}");
    }

    /// An Avro source's stored record is read as the fields it was shown with: the writer's schema its payload names
    /// is only the worker's.
    #[test]
    fn an_avro_sources_stored_record_reads_as_its_fields() {
        let meta = SourceMeta::KAFKA
            .decoding_avro(vec![("id".into(), DataType::I64)])
            .unwrap();
        let shown = serde_json::json!({"id": 1}).as_object().unwrap().clone();
        let records = meta
            .read_table(&pasted_from(&[(&shown, &crate::io::SourceFields::new())]))
            .unwrap();
        assert_eq!(meta.record_object(&records[0]), shown);
    }

    /// A stored record is read as its source reads: a payload source's must be one `_raw` text, and a typed source's may
    /// hold only the fields it declares, each value that is not its type null and counted failed.
    #[test]
    fn a_stored_record_reads_as_its_source_reads() {
        let object = |v: serde_json::Value| v.as_object().unwrap().clone();
        let fields = crate::io::SourceFields::new();

        let raw = object(serde_json::json!({"_raw": "line"}));
        let records = SourceMeta::KAFKA.read_table(&pasted_from(&[(&raw, &fields)])).unwrap();
        assert_eq!(records[0].bytes(), Some(&b"line"[..]));
        let typed = object(serde_json::json!({"id": 1}));
        let err = SourceMeta::KAFKA
            .read_table(&pasted_from(&[(&typed, &fields)]))
            .unwrap_err();
        assert!(err.contains("record 1"), "{err}");

        let meta = SourceMeta::KAFKA
            .declaring(vec![("id".into(), DataType::I64), ("at".into(), DataType::DateTime)])
            .unwrap();
        let good = object(serde_json::json!({"id": 1, "at": "2026-10-10T00:00:00Z"}));
        let bad = object(serde_json::json!({"id": true}));
        let records = meta
            .read_table(&pasted_from(&[(&good, &fields), (&bad, &fields)]))
            .unwrap();
        let chunk = &records[1].chunk;
        assert_eq!(chunk.failed(), &[(1, 0, "true".to_string())]);
        assert_eq!(
            meta.record_object(&records[0]),
            object(serde_json::json!({"id": 1, "at": "2026-10-10T00:00:00.000000Z"}))
        );
        let records = meta.read_table(&pasted_from(&[(&raw, &fields)])).unwrap();
        assert!(
            meta.record_object(&records[0]).is_empty(),
            "a key its record does not declare is left out"
        );

        // A cell its row lists as failed fails again, as it was spelled, though its type would take it; a bytes
        // field's spelling is not read as base64.
        let listed = SourceMeta::KAFKA
            .declaring(vec![("name".into(), DataType::Str), ("blob".into(), DataType::Bytes)])
            .unwrap();
        let column = |name: &str, ty: &str| crate::heartbeat::CaptureColumn {
            name: name.to_string(),
            ty: ty.to_string(),
        };
        let marked = crate::heartbeat::CaptureBatch {
            columns: vec![column("name", "str"), column("blob", "bytes")],
            rows: vec![crate::heartbeat::CapturedRow {
                values: vec![r#"{"a":1}"#.into(), "true".into()],
                failed: vec![0, 1],
                ..Default::default()
            }],
        };
        let records = listed.read_table(&marked).unwrap();
        assert_eq!(
            records[0].chunk.failed(),
            &[(0, 0, r#"{"a":1}"#.to_string()), (0, 1, "true".to_string())]
        );

        let nested = SourceMeta::KAFKA
            .declaring(vec![
                ("user.name".into(), DataType::Str),
                (r#"labels["a.b"]"#.into(), DataType::I64),
            ])
            .unwrap();
        let stored = object(serde_json::json!({"user": {"name": "ann"}, "labels": {"a.b": 2}}));
        let records = nested.read_table(&pasted_from(&[(&stored, &fields)])).unwrap();
        assert_eq!(
            nested.record_object(&records[0]),
            stored,
            "a path's value is where its path names"
        );
        let extra = object(serde_json::json!({"user": {"name": "ann", "age": 3}}));
        let records = nested.read_table(&pasted_from(&[(&extra, &fields)])).unwrap();
        assert_eq!(
            nested.record_object(&records[0]),
            object(serde_json::json!({"user": {"name": "ann"}})),
            "a key its record does not declare is left out"
        );
        let flat = object(serde_json::json!({"user": "ann", "labels": {"a.b": 2}}));
        let records = nested.read_table(&pasted_from(&[(&flat, &fields)])).unwrap();
        assert_eq!(
            records[0].chunk.failed(),
            &[(0, 0, r#""ann""#.to_string())],
            "a field under a value that is no object fails with that value"
        );
    }

    /// A stored array or object reads as declared all the way down: an array of its elements' type, an object a
    /// field per member. A value that cannot be its type is failed with its JSON under its own field, and an
    /// undeclared field's type is refused.
    #[test]
    fn a_stored_array_or_object_reads_as_declared() {
        let object = |v: serde_json::Value| v.as_object().unwrap().clone();
        let fields = crate::io::SourceFields::new();
        let meta = SourceMeta::KAFKA
            .declaring(vec![
                ("tags".into(), DataType::Array(Box::new(DataType::I64))),
                (
                    "who".into(),
                    DataType::Struct(
                        vec![
                            ("name".into(), DataType::Str),
                            ("at".into(), DataType::DateTime),
                            ("took".into(), DataType::Duration),
                        ],
                        false,
                    ),
                ),
            ])
            .unwrap();
        let good = object(
            serde_json::json!({"tags": [1, 2], "who": {"name": "ann", "at": "2026-10-10T00:00:00Z", "took": 1500}}),
        );
        let bad = object(serde_json::json!({"tags": [1, "x"], "who": {"name": "bo", "at": "never"}}));
        let records = meta
            .read_table(&pasted_from(&[(&good, &fields), (&bad, &fields)]))
            .unwrap();
        assert_eq!(
            meta.record_object(&records[0]),
            object(
                serde_json::json!({"tags": [1, 2], "who": {"name": "ann", "at": "2026-10-10T00:00:00.000000Z", "took": 1500}})
            )
        );
        assert_eq!(
            records[1].chunk.failed(),
            &[(1, 0, r#"[1,"x"]"#.to_string()), (1, 2, r#""never""#.to_string())]
        );
        assert_eq!(
            meta.record_object(&records[1]),
            object(serde_json::json!({"who": {"name": "bo"}})),
            "a refused element nulls its array, and a refused member only itself"
        );

        let bytes = SourceMeta::KAFKA
            .declaring(vec![("blobs".into(), DataType::Array(Box::new(DataType::Bytes)))])
            .unwrap();
        let blobs = object(serde_json::json!({"blobs": ["3q0="]}));
        let read = bytes.read_table(&pasted_from(&[(&blobs, &fields)])).unwrap();
        assert_eq!(
            bytes.record_object(&read[0]),
            blobs,
            "stored bytes are base64, and read back as bytes"
        );
    }

    /// Declaring an object declares its leaves: each field under it by its own name, and where the object is open, its
    /// other keys. An object that declares no field and holds no other key is refused.
    #[test]
    fn a_declared_object_is_its_leaves() {
        let object = |fields: &[(&str, DataType)], open| {
            DataType::Struct(fields.iter().map(|(n, t)| (n.to_string(), t.clone())).collect(), open)
        };
        let geo = object(&[("city", DataType::Str)], true);
        let destination = object(&[("ip", DataType::Str), ("port", DataType::I64), ("geo", geo)], false);
        let meta = SourceMeta::KAFKA
            .declaring(vec![
                ("destination".into(), destination),
                ("labels".into(), object(&[], true)),
                ("tags".into(), DataType::Array(Box::new(object(&[], false)))),
            ])
            .unwrap();
        let declared: Vec<_> = (meta.record().iter())
            .map(|(name, ty)| format!("{name}: {}", ty.spell()))
            .collect();
        assert_eq!(
            declared,
            [
                "destination.ip: str",
                "destination.port: i64",
                "destination.geo.city: str",
                "destination.geo.*: variant",
                "labels.*: variant",
                "tags: array<{}>",
            ]
        );

        let object_of = |v: serde_json::Value| v.as_object().unwrap().clone();
        let fields = crate::io::SourceFields::new();
        let stored = object_of(serde_json::json!({
            "destination": {"ip": "8.8.8.8", "port": 443, "geo": {"city": "Ashburn", "zip": 20147}},
            "labels": {"env": "prod"},
        }));
        let read = meta.read_table(&pasted_from(&[(&stored, &fields)])).unwrap();
        assert_eq!(read[0].chunk.failed(), []);
        assert_eq!(
            meta.record_object(&read[0]),
            stored,
            "each leaf and each other key reads back"
        );
        let null = object_of(serde_json::json!({"destination": null, "labels": null}));
        let read = meta.read_table(&pasted_from(&[(&null, &fields)])).unwrap();
        assert!(meta.record_object(&read[0]).is_empty(), "a null object holds no field");
        let extra = object_of(serde_json::json!({"destination": {"ip": "8.8.8.8", "mac": "aa"}}));
        let read = meta.read_table(&pasted_from(&[(&extra, &fields)])).unwrap();
        assert_eq!(
            meta.record_object(&read[0]),
            object_of(serde_json::json!({"destination": {"ip": "8.8.8.8"}})),
            "a closed object holds only its fields"
        );

        for (ty, names) in [
            (object(&[], false), "`meta`"),
            (object(&[("inner", object(&[], false))], true), "`meta.inner`"),
        ] {
            let err = SourceMeta::KAFKA.declaring(vec![("meta".into(), ty)]).unwrap_err();
            assert!(err.contains(names) && err.contains("{}"), "{err}");
        }
        for clash in [
            vec![
                ("user".to_string(), object(&[("name", DataType::Str)], true)),
                ("user.name".to_string(), DataType::Str),
            ],
            vec![("...".to_string(), DataType::Variant)],
            vec![("user....".to_string(), DataType::Variant)],
        ] {
            assert!(SourceMeta::KAFKA.declaring(clash.clone()).is_err(), "{clash:?}");
        }
    }

    /// A declared record cannot take the engine's own names, and no field of it is another or holds another.
    #[test]
    fn a_declared_record_takes_no_engine_name_and_no_field_twice() {
        let declare = |names: &[&str]| {
            SourceMeta::KAFKA.declaring(names.iter().map(|n| (n.to_string(), DataType::Str)).collect())
        };
        assert!(declare(&["a", "user.name", r#"labels["a.b"]"#]).is_ok());
        for refused in [
            &[][..],
            &["__live"],
            &["__source.offset"],
            &["a", "a"],
            &["user", "user.name"],
            &["user.name", "user"],
            &[""],
        ] {
            assert!(declare(refused).is_err(), "{refused:?}");
        }
    }

    /// A record's `__source` object, stored and read back, is the record it was made from.
    #[test]
    fn a_records_source_object_reads_back_as_the_record() {
        let record = crate::io::SourceRecord {
            chunk: crate::io::RecordChunk::of_bytes(Vec::new()),
            row: 0,
            offset: 7,
            partition: 3,
            topic: "t".into(),
            headers: Some(vec![("trace".to_string(), b"t1".to_vec())]),
        };
        let objects = SourceMeta::KAFKA.objects(std::slice::from_ref(&record), Some("orders"));
        assert_eq!(objects[0]["name"], "orders");
        let back = crate::io::SourceRecord::read(crate::io::RecordChunk::of_bytes(Vec::new()), 0, &objects[0]);
        assert_eq!(
            (back.offset, back.partition, &*back.topic, back.headers),
            (7, 3, "t", record.headers)
        );
    }

    #[test]
    fn a_source_column_names_its_source_and_kind_and_notes_a_repaired_header() {
        let mut pool = BufferPool::new();
        let mut record = crate::io::SourceRecord {
            chunk: crate::io::RecordChunk::of_bytes(Vec::new()),
            row: 0,
            offset: 0,
            partition: 0,
            topic: "t".into(),
            headers: None,
        };
        let plain = record.clone();
        record.headers = Some(vec![("k".to_string(), vec![0xff])]);
        let records = [plain, record];
        let meta = SourceMeta::KAFKA;
        let mut repaired = Vec::new();

        let name = meta.column(
            field(&meta, "__source.name"),
            &records,
            Some("orders"),
            &mut pool,
            &mut repaired,
        );
        assert_eq!(name.var().unwrap().get(1), b"orders");
        let unnamed = meta.column(field(&meta, "__source.name"), &records, None, &mut pool, &mut repaired);
        assert!(!unnamed.var().unwrap().validity.is_valid(0), "no deployment names it");
        let kind = meta.column(field(&meta, "__source.type"), &records, None, &mut pool, &mut repaired);
        assert_eq!(kind.var().unwrap().get(0), b"kafka");
        assert!(repaired.is_empty());

        let headers = meta.column(
            field(&meta, "__source.headers"),
            &records,
            None,
            &mut pool,
            &mut repaired,
        );
        assert_eq!(repaired, vec![1], "only the row whose value did not decode");
        let empty = headers.variant().unwrap().get(0);
        assert!(crate::core::variant::is_object(empty) && crate::core::variant::member(empty, "k").is_none());
        for c in [name, unnamed, kind, headers] {
            c.reclaim(&mut pool);
        }
    }

    /// A Kafka record's time is the one its source stamped it with, and null where it stamped none. It stays with its
    /// row when a record is kept apart from its poll, and through a sample.
    #[test]
    fn a_kafka_records_time_is_its_stamp_and_survives_a_sample() {
        let mut pool = BufferPool::new();
        let mut b = crate::io::RecordChunkBuilder::new();
        b.push_stamped(b"a", 0, 0, "t".into(), None, None);
        b.push_stamped(b"b", 1, 0, "t".into(), None, Some(1_700_000_000_250_000));
        let mut records = b.finish();
        let meta = SourceMeta::KAFKA;
        let at = field(&meta, "__source.time");
        let mut repaired = Vec::new();
        let time = meta.column(at, &records, None, &mut pool, &mut repaired);
        let read = time.datetime().unwrap();
        assert!(!read.validity.is_valid(0), "no stamp reads null");
        assert_eq!(read.values[1], 1_700_000_000_250_000);
        time.reclaim(&mut pool);

        crate::io::own_rows(&mut records[1..]);
        assert_eq!(
            records[1].chunk.time(records[1].row as usize),
            Some(1_700_000_000_250_000)
        );

        let objects = meta.objects(&records[1..], None);
        let back = meta
            .read_table(&pasted_from(&[(&crate::io::RecordObject::new(), &objects[0])]))
            .unwrap();
        assert_eq!(back[0].chunk.time(0), Some(1_700_000_000_250_000));
        let unstamped = meta.objects(&records[..1], None);
        let back = meta
            .read_table(&pasted_from(&[(&crate::io::RecordObject::new(), &unstamped[0])]))
            .unwrap();
        assert_eq!(back[0].chunk.time(0), None);

        let typed = serde_json::json!({"time": "2023-11-14T22:13:20.25Z", "bytes": "b25l"});
        let back = meta
            .read_table(&pasted_from(&[(
                &crate::io::RecordObject::new(),
                typed.as_object().unwrap(),
            )]))
            .unwrap();
        assert_eq!(back[0].chunk.time(0), Some(1_700_000_000_250_000));
        assert_eq!(back[0].headers, Some(Vec::new()), "a pasted time is no header");
    }

    /// Records whose chunk files them all under one value read it as one constant per field, which no row copies.
    #[test]
    fn what_a_chunk_files_its_every_record_under_is_one_constant() {
        use crate::core::variant;
        let mut pool = BufferPool::new();
        let mut resource = Vec::new();
        let open = variant::begin_object(&mut resource);
        variant::put_key(&mut resource, "service.name");
        variant::put_str(&mut resource, "api");
        variant::end(&mut resource, open, 1);
        let filed = vec![
            ("resource".to_string(), resource.clone()),
            ("scope.name".to_string(), b"lib".to_vec()),
        ];
        let chunk = std::sync::Arc::new(
            crate::io::RecordChunk::new(Vec::new(), Vec::new()).filed_under(std::sync::Arc::new(filed)),
        );
        let record = |row| crate::io::SourceRecord {
            chunk: chunk.clone(),
            row,
            offset: row as i64,
            partition: 0,
            topic: "t".into(),
            headers: None,
        };
        let records = [record(0), record(1)];
        let meta = SourceMeta::OTLP_LOGS;
        let mut repaired = Vec::new();
        let mut column = |name| meta.column(field(&meta, name), &records, None, &mut pool, &mut repaired);

        assert_eq!(
            records[0].size(),
            resource.len() + "resource".len() + "lib".len() + "scope.name".len()
        );
        let shared = column("__source.resource");
        assert_eq!(shared.len(), 2);
        assert_eq!(
            shared.constant_value(),
            Some(&Scalar::Variant(EncodedBuf::check(resource)))
        );
        let name = column("__source.scope.name");
        assert_eq!(name.constant_value(), Some(&Scalar::Str("lib".into())));
        let version = column("__source.scope.version");
        assert_eq!(version.data_type(), DataType::Str);
        assert!(
            version.is_unbound(),
            "a value it is not filed under is null on every row"
        );
        assert!(repaired.is_empty());
        for c in [shared, name, version] {
            c.reclaim(&mut pool);
        }
    }

    fn local(var: &str) -> SecretRef {
        SecretRef::WorkerLocal(var.to_string())
    }

    fn stored(name: &str, version: i64) -> SecretRef {
        SecretRef::LeaderStored {
            name: name.to_string(),
            version,
        }
    }

    fn held(values: &[(&str, i64, &str)]) -> HeldSecrets {
        let mut h = HeldSecrets::default();
        h.set_stored(
            values
                .iter()
                .map(|(n, v, value)| (n.to_string(), *v, (*value).to_string())),
        );
        h
    }

    /// A bundle states which credentials it needs; every place one can be named has to be among them, or a missing
    /// secret goes unnoticed until it is too late to report (`worker::deploy`).
    /// A chain is every DLQ a refusal can reach, nearest first, each once -- and it ends even where load would have
    /// refused a cycle.
    #[test]
    fn a_dlq_chain_follows_every_dlq_once() {
        let named = std::collections::HashMap::from([
            (DestId::new(0), vec![DestId::new(1), DestId::new(2)]),
            (DestId::new(1), vec![DestId::new(3), DestId::new(2)]),
            (DestId::new(3), vec![DestId::new(0)]),
        ]);
        assert_eq!(
            dlq_chain(&named, DestId::new(0)),
            vec![DestId::new(1), DestId::new(2), DestId::new(3)]
        );
        assert_eq!(dlq_chain(&named, DestId::new(2)), Vec::<DestId>::new());
    }

    /// A bundle naming `reference` for `what`.
    fn naming(what: &str, reference: SecretRef) -> TopologyConfig {
        TopologyConfig {
            named_secrets: vec![NamedSecret {
                reference,
                what: what.to_string(),
            }],
            ..Default::default()
        }
    }

    /// The message has to name the credential *and* what wanted it. An operator reading it is being asked to supply
    /// something, and "a secret is missing" does not say which secret or on whose behalf.
    #[test]
    fn a_missing_secret_is_reported_with_its_credential_and_owner() {
        let t = naming(
            r#"destination "prod-es": auth"#,
            local("SMELTR_TEST_DEFINITELY_UNSET_AUTH"),
        );
        let missing = t.missing_secrets(&HeldSecrets::default());
        assert_eq!(missing.len(), 1, "{missing:?}");
        assert!(missing[0].contains("prod-es"), "{missing:?}");
        assert!(missing[0].contains("auth"), "{missing:?}");
        assert!(missing[0].contains("SMELTR_TEST_DEFINITELY_UNSET_AUTH"), "{missing:?}");
    }

    /// A certificate and its key, its Extended Key Usage naming `purposes` where any are given.
    fn certificate_and_key(purposes: Vec<rcgen::ExtendedKeyUsagePurpose>) -> (String, String) {
        let key = rcgen::KeyPair::generate().unwrap();
        let mut params = rcgen::CertificateParams::new(vec!["smeltr-test".to_string()]).unwrap();
        params.extended_key_usages = purposes;
        (params.self_signed(&key).unwrap().pem(), key.serialize_pem())
    }

    /// A connector presenting `acme`'s certificate from `side`.
    fn presenting(side: crate::io::trust::Side) -> Tls {
        Tls {
            of: "source \"in\"".to_string(),
            certificate: Some("acme".to_string()),
            authorities: None,
            trust_public: true,
            side,
            senders: Default::default(),
        }
    }

    /// What a worker holds once the leader has sent it the certificate `acme` at version 1.
    fn holding_certificate(chain: String, key: String) -> HeldSecrets {
        let mut h = HeldSecrets::default();
        h.take([crate::heartbeat::SecretDelivery {
            name: "acme".to_string(),
            version: 1,
            value: key,
            chain: Some(chain),
        }]);
        h
    }

    /// A certificate whose Extended Key Usage leaves out the side it is presented from is refused before anything
    /// is torn down, rather than at the handshake.
    #[test]
    fn a_certificate_is_held_to_the_side_its_bundle_presents_it_from() {
        use crate::io::trust::Side;
        let (chain, key) = certificate_and_key(vec![rcgen::ExtendedKeyUsagePurpose::ServerAuth]);
        let held = holding_certificate(chain, key);
        let error =
            certificate_error(&presenting(Side::Client), &held).expect("a server certificate presented by a client");
        assert!(
            error.contains("source \"in\"") && error.contains("client authentication"),
            "{error}"
        );
        assert!(certificate_error(&presenting(Side::Server), &held).is_none());
    }

    #[test]
    fn a_certificate_with_no_extended_key_usage_is_presented_from_either_side() {
        use crate::io::trust::Side;
        let (chain, key) = certificate_and_key(Vec::new());
        let held = holding_certificate(chain, key);
        assert!(certificate_error(&presenting(Side::Client), &held).is_none());
        assert!(certificate_error(&presenting(Side::Server), &held).is_none());
    }

    /// A certificate this worker does not have yet is `missing_secrets`' concern.
    #[test]
    fn a_certificate_this_worker_lacks_is_not_this_checks_concern() {
        assert!(certificate_error(&presenting(crate::io::trust::Side::Client), &HeldSecrets::default()).is_none());
        let t = TopologyConfig {
            named_certificates: vec![NamedCertificate {
                name: "acme".to_string(),
                what: "source \"in\"".to_string(),
            }],
            ..Default::default()
        };
        let missing = t.missing_secrets(&HeldSecrets::default());
        assert_eq!(missing.len(), 1, "{missing:?}");
        assert!(missing[0].contains("certificate \"acme\""), "{missing:?}");
        assert!(
            t.missing_secrets(&holding_certificate("chain".into(), "key".into()))
                .is_empty()
        );
    }

    /// The chain travels with its key, and both reach the runtime; the newest version is the one held, and a
    /// version replacing an older one is said, since what presents it has to be built again.
    #[test]
    fn a_certificate_is_held_at_its_newest_version_with_its_chain() {
        let mut held = holding_certificate("chain".into(), "key".into());
        assert_eq!(held.certificate("acme"), Some(("chain".to_string(), "key".to_string())));
        let presented = [NamedCertificate {
            name: "acme".to_string(),
            what: "source \"in\"".to_string(),
        }];
        let handed = held.deliveries_for(&[], &presented);
        assert_eq!(handed.len(), 1);
        assert_eq!(handed[0].chain.as_deref(), Some("chain"));
        assert_eq!(handed[0].version, 1);

        let newer = |version: i64| crate::heartbeat::SecretDelivery {
            name: "acme".to_string(),
            version,
            value: format!("key{version}"),
            chain: Some(format!("chain{version}")),
        };
        assert_eq!(held.take([newer(2)]), vec!["acme".to_string()]);
        assert_eq!(
            held.certificate("acme"),
            Some(("chain2".to_string(), "key2".to_string()))
        );
        assert!(
            held.take([newer(1)]).is_empty(),
            "an older version does not replace the newest"
        );
        assert_eq!(held.versions(), vec![("acme".to_string(), 2)]);
        held.keep_only(&[], &[]);
        assert_eq!(held.certificate("acme"), None);
    }

    /// A deployment runs the version its bundle quotes or it does not run. Neither an older value nor a newer one
    /// stands in: storing a value is staging it, and the commit quoting the number is what deploys it.
    #[test]
    fn a_bundle_runs_the_version_it_quotes_and_no_other() {
        let t = naming(r#"destination "prod-es": auth"#, stored("es-auth", 7));
        assert!(t.missing_secrets(&held(&[("es-auth", 7, "ApiKey abc")])).is_empty());
        assert_eq!(
            t.missing_secrets(&held(&[("es-auth", 8, "ApiKey abc")])).len(),
            1,
            "a newer value is not what this bundle was built for"
        );
        assert_eq!(t.missing_secrets(&held(&[("es-auth", 6, "ApiKey abc")])).len(), 1);
        assert_eq!(t.missing_secrets(&held(&[("other", 7, "x")])).len(), 1);
        assert_eq!(t.missing_secrets(&HeldSecrets::default()).len(), 1);
    }

    /// Holding both is the ordinary state mid-rotation: the version that is running, and the one the next commit
    /// will quote.
    #[test]
    fn two_versions_of_one_name_are_held_apart() {
        let h = held(&[("es-auth", 7, "old"), ("es-auth", 8, "new")]);
        assert_eq!(h.value(&stored("es-auth", 7)).as_deref(), Some("old"));
        assert_eq!(h.value(&stored("es-auth", 8)).as_deref(), Some("new"));
        assert_eq!(h.value(&stored("es-auth", 9)), None);
    }

    /// What a deployment does not name is dropped, so a worker carries the credentials something runs on rather
    /// than every one it has ever been sent.
    #[test]
    fn keeping_only_what_a_deployment_names_drops_the_rest() {
        let mut h = held(&[("es-auth", 7, "old"), ("es-auth", 8, "new"), ("gone", 1, "x")]);
        let t = naming(r#"destination "prod-es": auth"#, stored("es-auth", 8));
        h.keep_only(&t.named_secrets, &[]);
        assert_eq!(h.versions(), vec![("es-auth".to_string(), 8)]);
    }

    #[test]
    fn a_variable_set_to_nothing_is_a_credential_this_worker_does_not_have() {
        assert_eq!(usable(Some("hunter2".to_string())), Some("hunter2".to_string()));
        assert_eq!(usable(Some(String::new())), None);
        assert_eq!(usable(None), None);
    }

    /// The leader answers with what this worker lacks, so an empty delivery has to mean "nothing changed" rather
    /// than "hold nothing".
    #[test]
    fn taking_a_delivery_keeps_what_it_does_not_mention() {
        use crate::heartbeat::SecretDelivery;

        let mut h = held(&[("a", 1, "one"), ("b", 2, "two")]);
        h.take([SecretDelivery {
            name: "b".to_string(),
            version: 3,
            value: "three".to_string(),
            chain: None,
        }]);
        assert_eq!(h.value(&stored("a", 1)).as_deref(), Some("one"));
        assert_eq!(
            h.value(&stored("b", 2)).as_deref(),
            Some("two"),
            "the running one stays"
        );
        assert_eq!(h.value(&stored("b", 3)).as_deref(), Some("three"));

        h.take([]);
        assert_eq!(
            h.versions(),
            vec![("a".to_string(), 1), ("b".to_string(), 2), ("b".to_string(), 3)],
            "a delivery adds; forgetting is what a deploy does"
        );
    }

    /// The type holds every credential a deployment uses, so its own `Debug` must not be the way they escape.
    #[test]
    fn debug_shows_what_is_held_and_never_a_value() {
        let shown = format!("{:?}", held(&[("es-auth", 7, "ApiKey-SECRET")]));
        assert!(shown.contains("es-auth"), "{shown}");
        assert!(shown.contains('7'), "{shown}");
        assert!(!shown.contains("ApiKey-SECRET"), "{shown}");
    }

    fn route(source: SourceId, cond: Option<&str>, pipeline: usize) -> RouteConfig {
        RouteConfig {
            id: format!("route-{source}-{pipeline}"),
            name: format!("route-{source}-{pipeline}"),
            source,
            cond: cond.map(|s| s.to_string()),
            pipeline,
            deliver: Vec::new(),
            on_exception: Default::default(),
        }
    }

    /// A plan depends on the names in scope, so two sources declaring the same fields share one rather than compiling
    /// it twice. Compiling twice would also load the pipeline's lookup tables twice.
    #[test]
    fn sources_declaring_the_same_fields_share_one_plan() {
        let pipelines = vec![crate::plan::config::PipelineConfig {
            id: 7,
            name: "p".into(),
            body: Vec::new(),
            step_ids: Default::default(),
            lookup_tables: Default::default(),
            grok_patterns: Default::default(),
            produces: Vec::new(),
        }];
        let metas = SourceMetas::from([
            (SourceId::new(0), SourceMeta::KAFKA),
            (SourceId::new(1), SourceMeta::KAFKA),
        ]);
        let compiled = compile_topology_with(
            &[route(SourceId::new(0), None, 7), route(SourceId::new(1), None, 7)],
            &pipelines,
            OnException::default(),
            &metas,
        )
        .unwrap();

        assert_eq!(compiled.pipelines.len(), 1, "one plan, not one per source");
        // Both sources still reach it.
        assert!(compiled.plan_for(SourceId::new(0), 7).is_some());
        assert!(compiled.plan_for(SourceId::new(1), 7).is_some());

        // Declaring different fields is what forces a second plan.
        let mixed = SourceMetas::from([
            (SourceId::new(0), SourceMeta::KAFKA),
            (SourceId::new(1), SourceMeta::METRICS),
        ]);
        let compiled = compile_topology_with(
            &[route(SourceId::new(0), None, 7), route(SourceId::new(1), None, 7)],
            &pipelines,
            OnException::default(),
            &mixed,
        )
        .unwrap();
        assert_eq!(compiled.pipelines.len(), 2, "different names in scope");
    }

    /// A worker thread serves one source and should connect only to what that source's own routes can reach. Building
    /// every destination in the bundle instead cost a Kafka client *and* a background thread per destination per worker
    /// thread -- the largest single multiplier in the fan-out, and invisible until you count producers rather than
    /// consumers.
    #[test]
    fn a_source_reaches_only_the_destinations_its_own_routes_name() {
        // Built as `CompiledRoute`s directly rather than through `compile_routes`: that function deliberately leaves
        // `deliver` empty, since a rule's `when` compiles against its target pipeline's output and only
        // `compile_topology` has the plan to do that. The worker calls `compile_topology`, so its routes are fully
        // populated -- but a test that used `compile_routes` would be asserting against rules that were never filled
        // in.
        let mk = |source: SourceId, to: Vec<DestId>| CompiledRoute {
            id: format!("route-{source}"),
            name: format!("route-{source}"),
            source,
            cond: None,
            pipeline: 0,
            deliver: vec![CompiledDeliverRule {
                when: None,
                to,
                compiled_patterns: Default::default(),
            }],
            compiled_patterns: Default::default(),
            on_exception: Default::default(),
        };

        let compiled = CompiledTopology {
            routes: vec![
                mk(SourceId::new(0), vec![DestId::new(2)]),
                mk(SourceId::new(1), vec![DestId::new(7)]),
            ],
            pipelines: std::collections::HashMap::new(),
            metas: SourceMetas::new(),
            on_exception: {
                let mut group = OnException::default();
                for (kind, to) in [
                    (ExceptionKind::Unmatched, DestId::new(4)),
                    (ExceptionKind::Unprocessable, DestId::new(8)),
                ] {
                    group.answers[kind.index()] = Answer::Deliver(vec![to]);
                }
                group
            },
            // Destination 2 salvages its refusals to 5, and 8 to 3. `dlq_of` holds whole chains, so reachability reads
            // one entry per destination.
            dlq_of: std::collections::HashMap::from([
                (DestId::new(2), vec![DestId::new(5)]),
                (DestId::new(8), vec![DestId::new(3)]),
            ]),
            records: HashMap::new(),
            native_variants: Default::default(),
            payloads: HashMap::new(),
            id_fields: Vec::new(),
        };

        // Source 0: its own delivery target plus the DLQ that target salvages to -- a source can write to a
        // destination no route names.
        assert_eq!(
            compiled.destinations_reachable_from(SourceId::new(0)),
            vec![
                DestId::new(2),
                DestId::new(3),
                DestId::new(4),
                DestId::new(5),
                DestId::new(8)
            ]
        );
        // Source 1 reaches neither of those.
        assert_eq!(
            compiled.destinations_reachable_from(SourceId::new(1)),
            vec![DestId::new(3), DestId::new(4), DestId::new(7), DestId::new(8)]
        );
        // A source with no routes reaches only where its unmatched and unprocessable rows go, and the chain behind
        // them.
        assert_eq!(
            compiled.destinations_reachable_from(SourceId::new(9)),
            vec![DestId::new(3), DestId::new(4), DestId::new(8)]
        );

        // Where the flow goes: the delivery targets alone, no DLQ of any kind.
        assert_eq!(
            compiled.delivery_destinations_from(SourceId::new(0)),
            vec![DestId::new(2)]
        );
        assert_eq!(
            compiled.delivery_destinations_from(SourceId::new(1)),
            vec![DestId::new(7)]
        );
        assert_eq!(
            compiled.delivery_destinations_from(SourceId::new(9)),
            Vec::<DestId>::new()
        );
    }

    /// The metadata `seed_batch`'s layout assumes, for a route condition over `__source.offset`.
    fn kafka_metas(source: SourceId) -> SourceMetas {
        SourceMetas::from([(source, SourceMeta::KAFKA)])
    }

    /// `rows` raw byte strings, in a batch laid out for a Kafka source, with `__live` primed all-true and `__source.offset`
    /// set to the row index.
    fn seed_batch(pool: &mut BufferPool, docs: &[&str]) -> Batch {
        use crate::core::batch::{RAW_SLOTS, SLOT_RAW, raw_layout};

        let offset_field = SourceMeta::KAFKA
            .source_fields()
            .iter()
            .position(|f| f.0 == "__source.offset")
            .unwrap();
        let slot_offset = crate::core::id::SlotId::at(RAW_SLOTS + offset_field);

        let rows = docs.len();
        let mut layout = raw_layout();
        layout.extend(SourceMeta::KAFKA.source_fields().iter().map(|f| f.1.clone()));
        let mut batch = Batch::new(&layout, rows, 0);

        let mut live = Column::alloc(pool, &DataType::Bool, rows);
        bits::fill_valid(live.bool_mut().unwrap().values, rows);
        batch.put(SLOT_LIVE, live, pool);

        // Row index, not a real Kafka offset -- just something per-row and distinct for route conditions in these tests
        // to key on, since `_raw` equality/comparison support isn't exercised here.
        let mut offset = Column::alloc(pool, &DataType::I64, rows);
        for (i, v) in offset.i64_mut().unwrap().values.iter_mut().enumerate() {
            *v = i as i64;
        }
        batch.put(slot_offset, offset, pool);

        let mut b = StrBuilder::new(pool, VarType::Str, rows);
        b.reserve(pool, docs.iter().map(|d| d.len()).sum());
        for d in docs {
            b.push(d.as_bytes());
        }
        batch.put(SLOT_RAW, b.finish(), pool);

        batch
    }

    #[test]
    fn a_single_route_with_no_condition_catches_everything() {
        let routes = compile_routes(&[route(SourceId::new(0), None, 7)], &SourceMetas::new()).unwrap();
        let mut pool = BufferPool::new();
        let mut errors = ErrorSink::new();
        let mut batch = seed_batch(&mut pool, &["a", "b", "c"]);
        let mut ctx = Ctx::new(&mut pool, &mut errors);

        let masks = route_masks(&routes, SourceId::new(0), &mut batch, &mut ctx);
        assert_eq!(masks.len(), 1);
        let rows: Vec<bool> = (0..3)
            .map(|r| bits::get_row(masks[0].1.bool().unwrap().values, r))
            .collect();
        assert_eq!(rows, vec![true, true, true]);
        batch.reclaim(&mut pool);
        masks.into_iter().for_each(|(_, c)| c.reclaim(&mut pool));
    }

    #[test]
    fn routes_for_a_different_source_are_skipped() {
        let routes = compile_routes(&[route(SourceId::new(1), None, 7)], &SourceMetas::new()).unwrap();
        let mut pool = BufferPool::new();
        let mut errors = ErrorSink::new();
        let mut batch = seed_batch(&mut pool, &["a"]);
        let mut ctx = Ctx::new(&mut pool, &mut errors);

        let masks = route_masks(&routes, SourceId::new(0), &mut batch, &mut ctx);
        assert!(masks.is_empty(), "route belongs to source 1, not 0");
        batch.reclaim(&mut pool);
    }

    #[test]
    fn every_route_sees_the_whole_batch() {
        // Route on __source.offset (here just the row index -- see seed_batch) rather than a parsed field, since routes only
        // see ingest fields. Row 0 matches both routes, so it runs both pipelines.
        let routes = compile_routes(
            &[
                route(SourceId::new(0), Some("__source.offset == 0"), 1),
                route(SourceId::new(0), None, 2),
            ],
            &kafka_metas(SourceId::new(0)),
        )
        .unwrap();
        let mut pool = BufferPool::new();
        let mut errors = ErrorSink::new();
        let mut batch = seed_batch(&mut pool, &["x", "yy"]);
        let mut ctx = Ctx::new(&mut pool, &mut errors);

        let masks = route_masks(&routes, SourceId::new(0), &mut batch, &mut ctx);
        let rows = |m: &Column| {
            (0..2)
                .map(|r| bits::get_row(m.bool().unwrap().values, r))
                .collect::<Vec<_>>()
        };
        assert_eq!(masks.iter().map(|(i, _)| *i).collect::<Vec<_>>(), vec![0, 1]);
        assert_eq!(
            rows(&masks[0].1),
            vec![true, false],
            "route 0 (__source.offset==0) matches only row 0"
        );
        assert_eq!(
            rows(&masks[1].1),
            vec![true, true],
            "the route with no condition takes every row"
        );
        assert_eq!(
            rows(batch.get(SLOT_LIVE)),
            vec![true, true],
            "routing leaves __live as it came"
        );
        batch.reclaim(&mut pool);
        masks.into_iter().for_each(|(_, c)| c.reclaim(&mut pool));
    }

    #[test]
    fn a_non_bool_route_condition_is_a_compile_error() {
        let err = compile_routes(&[route(SourceId::new(0), Some("_raw"), 1)], &SourceMetas::new());
        assert!(err.is_err());
    }

    /// Regression test: a route condition using a regex literal (`match`/ `replace`) must actually match at runtime.
    /// `compile_route` resolves `cond` against its own disjoint `ResourceBuilder`, so its interned patterns live in
    /// `CompiledRoute::compiled_patterns`, not any target pipeline's `Plan::compiled_patterns` — `route_masks` has to
    /// point `Ctx::patterns` there before evaluating `cond`, or every `match`/ `replace` in a route condition silently
    /// resolves against an empty pattern table and never matches.
    #[test]
    fn a_route_condition_using_a_regex_literal_actually_matches() {
        let routes = compile_routes(
            &[
                route(SourceId::new(0), Some("test(_raw, /^x/)"), 1),
                route(SourceId::new(0), None, 2), // catch-all
            ],
            &SourceMetas::new(),
        )
        .unwrap();
        let mut pool = BufferPool::new();
        let mut errors = ErrorSink::new();
        let mut batch = seed_batch(&mut pool, &["x1", "y2"]);
        let mut ctx = Ctx::new(&mut pool, &mut errors);

        let masks = route_masks(&routes, SourceId::new(0), &mut batch, &mut ctx);
        let (_, m0) = &masks[0];
        assert_eq!(
            (0..2)
                .map(|r| bits::get_row(m0.bool().unwrap().values, r))
                .collect::<Vec<_>>(),
            vec![true, false],
            "route 0 (test(_raw, /^x/)) should match only row 0"
        );
        batch.reclaim(&mut pool);
        masks.into_iter().for_each(|(_, c)| c.reclaim(&mut pool));
    }

    /// A Kafka destination holds a batch twice: in the egress queue and in librdkafka's own. Its `bufferBytes` is split
    /// between the two, so one worker holds no more than the admin set.
    #[test]
    fn a_kafka_destinations_two_queues_add_up_to_its_buffer_bytes() {
        // Odd, so two shares each rounded down fall one byte short.
        let k = KafkaDestinationConfig {
            payload: crate::plan::config::Payload::Record,
            compression: Compression::None,
            acks: Acks::All,
            stall_after: std::time::Duration::from_secs(30),
            brokers: "k:9092".into(),
            default_topic: "t".into(),
            service: crate::plan::topology::KafkaService::Kafka,
            tls: None,
            buffer_bytes: 200_000_001,
            props: KafkaProps {
                literal: Vec::new(),
                credentials: Vec::new(),
            },
            avro: None,
        };
        let (queue, producer) = (k.queue_bytes(), k.producer_bytes());
        assert_eq!(queue + producer, 200_000_001);
        assert!(queue > 0 && producer > 0, "queue {queue}, producer {producer}");
        assert_eq!(
            DestinationKind::Kafka(k).queue_bytes(),
            queue,
            "egress sizes its queue from its own share"
        );
    }
}
