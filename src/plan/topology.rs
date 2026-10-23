//! Sources, destinations, and routes — the layer above a single compiled `Plan` that decides *which* pipeline a batch
//! of raw records runs through, and where each pipeline's output goes. See the design doc's "Sources, destinations,
//! routes" section for the full reasoning; this module is that section's config schema plus route compiling/evaluation.
//!
//! `SourceKind`/`DestinationKind` are config *shape* only in this pass — `io::kafka`'s actual runtime implementation
//! (behind the `Source`/ `Destination` traits) is a deliberately separate, later step. Nothing here depends on it, and
//! nothing here will need to change once it lands.

use std::collections::HashMap;

use crate::core::batch::{Batch, SLOT_LIVE};
use crate::core::bits;
use crate::core::column::{Column, DataType, Scalar};
use crate::core::ctx::Ctx;
use crate::core::error::ExceptionKind;
use crate::core::id::{DestId, SourceId};
use crate::expr::ast::Span;
use crate::expr::eval::eval;
use crate::expr::ir::{Ir, IrKind, flatten};
use crate::format::encoding::Encoding;
use crate::metrics::{MetricsHandle, MetricsSourceConfig};
use crate::ops::BinOp;
use crate::plan::config::{Answer, CompileError, OnException, Payload, PipelineConfig};
use crate::plan::plan::parse;
use crate::plan::plan::{Plan, compile, ingest_fields};
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
    /// What its kind files `record` under, for the leader to tell one stream reaching a source from another
    /// (`heartbeat::UnmatchedStreams`): a HEC or S2S sender's sourcetype, an Elasticsearch sender's index, an
    /// object's bucket and directory prefix (`object_prefix`), and otherwise the record's topic, which a receiver
    /// fills with the sender's address and Kafka with the topic read. `None` where a sender named nothing.
    pub fn stream_key<'r>(&self, record: &'r crate::io::SourceRecord) -> Option<std::borrow::Cow<'r, str>> {
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

    /// A route condition that takes the records this kind files under `key` ([`Self::stream_key`]). An object's
    /// condition takes its bucket and every key under its prefix, so it also takes a stream filed under a longer
    /// prefix, which is the most a condition can say without the prefix rule itself. `None` where this kind files
    /// no field a condition could read.
    pub fn stream_condition(&self, key: &str) -> Option<String> {
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
}

/// The fields of a source's record, each with its type, in slot order. A source's own kinds declare theirs in a
/// `const`; a source whose record is configured declares it when it is built.
pub type RecordFields = std::borrow::Cow<'static, [(std::borrow::Cow<'static, str>, DataType)]>;

/// The `__source` field holding each record's payload as the source read it, before any decoding.
pub const SOURCE_BYTES: &str = "__source.bytes";

/// The `__source` fields that read a record's topic, each under the name its kind gives it.
const TOPIC_FIELDS: [&str; 4] = ["__source.topic", "__source.url", "__source.remote", "__source.database"];

/// The record of a source whose client hands over payloads: the payload, decoded with the source's encoding.
const PAYLOAD_RECORD: RecordFields = std::borrow::Cow::Borrowed(&[(std::borrow::Cow::Borrowed("_raw"), DataType::Str)]);

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
    (std::borrow::Cow::Borrowed("time"), DataType::DateTime),
    (std::borrow::Cow::Borrowed("observedTime"), DataType::DateTime),
    (std::borrow::Cow::Borrowed("severityNumber"), DataType::I64),
    (std::borrow::Cow::Borrowed("severityText"), DataType::Str),
    (std::borrow::Cow::Borrowed("body"), DataType::Variant),
    (std::borrow::Cow::Borrowed("attributes"), DataType::Variant),
    (std::borrow::Cow::Borrowed("traceId"), DataType::Str),
    (std::borrow::Cow::Borrowed("spanId"), DataType::Str),
    (std::borrow::Cow::Borrowed("flags"), DataType::I64),
    (std::borrow::Cow::Borrowed("eventName"), DataType::Str),
    (std::borrow::Cow::Borrowed("droppedAttributesCount"), DataType::I64),
]);

/// An OTLP span, each field typed as OTLP types it: `kind` and `statusCode` its enums' numbers, `attributes` and the
/// `events` and `links` lists variants that keep each value's own type. Each field is null where the span leaves it
/// unset.
const OTLP_SPAN: RecordFields = std::borrow::Cow::Borrowed(&[
    (std::borrow::Cow::Borrowed("traceId"), DataType::Str),
    (std::borrow::Cow::Borrowed("spanId"), DataType::Str),
    (std::borrow::Cow::Borrowed("parentSpanId"), DataType::Str),
    (std::borrow::Cow::Borrowed("traceState"), DataType::Str),
    (std::borrow::Cow::Borrowed("flags"), DataType::I64),
    (std::borrow::Cow::Borrowed("name"), DataType::Str),
    (std::borrow::Cow::Borrowed("kind"), DataType::I64),
    (std::borrow::Cow::Borrowed("startTime"), DataType::DateTime),
    (std::borrow::Cow::Borrowed("endTime"), DataType::DateTime),
    (std::borrow::Cow::Borrowed("attributes"), DataType::Variant),
    (std::borrow::Cow::Borrowed("droppedAttributesCount"), DataType::I64),
    (std::borrow::Cow::Borrowed("events"), DataType::Variant),
    (std::borrow::Cow::Borrowed("droppedEventsCount"), DataType::I64),
    (std::borrow::Cow::Borrowed("links"), DataType::Variant),
    (std::borrow::Cow::Borrowed("droppedLinksCount"), DataType::I64),
    (std::borrow::Cow::Borrowed("statusCode"), DataType::I64),
    (std::borrow::Cow::Borrowed("statusMessage"), DataType::Str),
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

/// An exponential histogram's buckets on one side of zero: the index of the first, and each one's count.
fn exponential_buckets() -> DataType {
    closed(&[("offset", DataType::I64), ("bucketCounts", array(DataType::F64))])
}

/// One data point of an OTLP metric, with its metric's name, description, unit and `type`: `gauge`, `sum`,
/// `histogram`, `exponentialHistogram` or `summary`. `value` is a gauge's or sum's number, an int or a double as sent;
/// `count`, `sum`, `min` and `max` a distribution's; the buckets and quantiles typed; and the exemplars a variant. Each
/// field is null where its point's type has none or leaves it unset. A distribution's counts are doubles, as a
/// Prometheus histogram's may be, so a pipeline reads a histogram from either alike; a double holds every whole count
/// to 2^53 exactly.
fn otlp_metric_point() -> RecordFields {
    use std::borrow::Cow::Borrowed;
    let buckets = exponential_buckets();
    std::borrow::Cow::Owned(vec![
        (Borrowed("name"), DataType::Str),
        (Borrowed("description"), DataType::Str),
        (Borrowed("unit"), DataType::Str),
        (Borrowed("type"), DataType::Str),
        (Borrowed("metadata"), DataType::Variant),
        (Borrowed("aggregationTemporality"), DataType::I64),
        (Borrowed("isMonotonic"), DataType::Bool),
        (Borrowed("startTime"), DataType::DateTime),
        (Borrowed("time"), DataType::DateTime),
        (Borrowed("attributes"), DataType::Variant),
        (Borrowed("flags"), DataType::I64),
        (Borrowed("value"), DataType::Variant),
        (Borrowed("count"), DataType::F64),
        (Borrowed("sum"), DataType::F64),
        (Borrowed("min"), DataType::F64),
        (Borrowed("max"), DataType::F64),
        (Borrowed("bucketCounts"), array(DataType::F64)),
        (Borrowed("explicitBounds"), array(DataType::F64)),
        (Borrowed("scale"), DataType::I64),
        (Borrowed("zeroCount"), DataType::F64),
        (Borrowed("zeroThreshold"), DataType::F64),
        (Borrowed("positive"), buckets.clone()),
        (Borrowed("negative"), buckets),
        (
            Borrowed("quantileValues"),
            array(closed(&[("quantile", DataType::F64), ("value", DataType::F64)])),
        ),
        (Borrowed("exemplars"), DataType::Variant),
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
    let buckets = exponential_buckets();
    std::borrow::Cow::Owned(vec![
        (Borrowed("time"), DataType::DateTime),
        (Borrowed("name"), DataType::Str),
        (Borrowed("value"), DataType::Variant),
        (Borrowed("attributes"), DataType::Variant),
        (Borrowed("type"), DataType::Str),
        (Borrowed("aggregationTemporality"), DataType::I64),
        (Borrowed("count"), DataType::F64),
        (Borrowed("sum"), DataType::F64),
        (Borrowed("bucketCounts"), array(DataType::F64)),
        (Borrowed("explicitBounds"), array(DataType::F64)),
        (Borrowed("scale"), DataType::I64),
        (Borrowed("zeroCount"), DataType::F64),
        (Borrowed("zeroThreshold"), DataType::F64),
        (Borrowed("positive"), buckets.clone()),
        (Borrowed("negative"), buckets),
        (Borrowed("resetHint"), DataType::Str),
        (Borrowed("exemplars"), DataType::Variant),
        (Borrowed("description"), DataType::Str),
        (Borrowed("unit"), DataType::Str),
        (Borrowed("isMonotonic"), DataType::Bool),
        (Borrowed("startTime"), DataType::DateTime),
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
    pub const KAFKA: SourceMeta = SourceMeta {
        kind: "kafka",
        record: PAYLOAD_RECORD,
        source: KAFKA_FIELDS,
    };

    /// What an Event Hubs record carries: a Kafka record's fields, with the event hub as its topic.
    pub const EVENT_HUBS: SourceMeta = SourceMeta {
        kind: "eventHubs",
        record: PAYLOAD_RECORD,
        source: KAFKA_FIELDS,
    };

    /// What a metrics record carries: its source's name and kind, and the topic it is stamped with.
    pub const METRICS: SourceMeta = SourceMeta {
        kind: "metrics",
        record: PAYLOAD_RECORD,
        source: METRICS_FIELDS,
    };

    /// What a logs record carries, which is the same.
    pub const LOGS: SourceMeta = SourceMeta {
        kind: "logs",
        record: PAYLOAD_RECORD,
        source: LOGS_FIELDS,
    };

    /// What an SQS/S3 record carries: the object it was read from, and its record number there.
    pub const S3_SQS: SourceMeta = SourceMeta {
        kind: "s3Sqs",
        record: PAYLOAD_RECORD,
        source: OBJECT_FIELDS,
    };

    /// What a Listing/S3 record carries: the same as an SQS/S3 record.
    pub const S3_LIST: SourceMeta = SourceMeta {
        kind: "s3List",
        record: PAYLOAD_RECORD,
        source: OBJECT_FIELDS,
    };

    /// What a Listing/GCS record carries: the same as an SQS/S3 record.
    pub const GCS_LIST: SourceMeta = SourceMeta {
        kind: "gcsList",
        record: PAYLOAD_RECORD,
        source: OBJECT_FIELDS,
    };

    /// What a Listing/Azure Blob record carries: the same, with the container as its bucket.
    pub const AZURE_BLOB_LIST: SourceMeta = SourceMeta {
        kind: "azureBlobList",
        record: PAYLOAD_RECORD,
        source: OBJECT_FIELDS,
    };

    /// What an SFTP record carries: the same, with the directory as its bucket and the path under it as its key.
    pub const SFTP: SourceMeta = SourceMeta {
        kind: "sftp",
        record: PAYLOAD_RECORD,
        source: OBJECT_FIELDS,
    };

    /// What a Queue/Azure Blob record carries: the same, with the container as its bucket.
    pub const AZURE_BLOB_QUEUE: SourceMeta = SourceMeta {
        kind: "azureBlobQueue",
        record: PAYLOAD_RECORD,
        source: OBJECT_FIELDS,
    };

    /// What a Pub/Sub/GCS record carries: the same.
    pub const GCS_PUB_SUB: SourceMeta = SourceMeta {
        kind: "gcsPubSub",
        record: PAYLOAD_RECORD,
        source: OBJECT_FIELDS,
    };

    /// What an HTTP API record carries: the address of the page it was read from.
    pub const HTTP_API: SourceMeta = SourceMeta {
        kind: "httpApi",
        record: PAYLOAD_RECORD,
        source: API_FIELDS,
    };

    /// What an Okta System Log record carries: the same.
    pub const OKTA_LOGS: SourceMeta = SourceMeta {
        kind: "oktaLogs",
        record: PAYLOAD_RECORD,
        source: API_FIELDS,
    };

    /// What a PostgreSQL source's record carries before it declares its fields, which every one does.
    pub const POSTGRES: SourceMeta = SourceMeta {
        kind: "postgres",
        record: PAYLOAD_RECORD,
        source: SQL_FIELDS,
    };

    /// What a MySQL or MariaDB source's record carries before it declares its fields, which every one does.
    pub const MYSQL: SourceMeta = SourceMeta {
        kind: "mysql",
        record: PAYLOAD_RECORD,
        source: SQL_FIELDS,
    };

    /// What an HTTP receiver's record carries: the address of the sender that delivered it.
    pub const HTTP_RECEIVER: SourceMeta = SourceMeta {
        kind: "httpReceiver",
        record: PAYLOAD_RECORD,
        source: RECEIVER_FIELDS,
    };

    /// What a HEC receiver's record carries: its sender's address, and what the sender filed its event under.
    pub const HEC_RECEIVER: SourceMeta = SourceMeta {
        kind: "hecReceiver",
        record: PAYLOAD_RECORD,
        source: HEC_RECEIVER_FIELDS,
    };

    /// What an S2S receiver's record carries: its forwarder's address, and what the forwarder filed its event under.
    pub const S2S_RECEIVER: SourceMeta = SourceMeta {
        kind: "s2sReceiver",
        record: PAYLOAD_RECORD,
        source: HEC_RECEIVER_FIELDS,
    };

    /// What an Elasticsearch receiver's record carries: a document, its sender's address, and what its action named.
    pub const ELASTIC_RECEIVER: SourceMeta = SourceMeta {
        kind: "elasticReceiver",
        record: PAYLOAD_RECORD,
        source: ELASTIC_RECEIVER_FIELDS,
    };

    /// What a syslog receiver's record carries: a message, and its sender's address.
    pub const SYSLOG_RECEIVER: SourceMeta = SourceMeta {
        kind: "syslogReceiver",
        record: PAYLOAD_RECORD,
        source: RECEIVER_FIELDS,
    };

    /// What an OTLP receiver of logs carries: a log record, its sender's address, and its resource and scope.
    pub const OTLP_LOGS: SourceMeta = SourceMeta {
        kind: "otlpReceiver",
        record: OTLP_LOG,
        source: OTLP_RECEIVER_FIELDS,
    };

    /// What an OTLP receiver of traces carries: a span, its sender's address, and its resource and scope.
    pub const OTLP_TRACES: SourceMeta = SourceMeta {
        kind: "otlpReceiver",
        record: OTLP_SPAN,
        source: OTLP_RECEIVER_FIELDS,
    };

    /// What an OTLP receiver of `signal` carries: a log record, a metric's data point or a span, its sender's address,
    /// and its resource and scope.
    pub fn otlp(signal: Signal) -> SourceMeta {
        match signal {
            Signal::Logs => SourceMeta::OTLP_LOGS,
            // A data point's record holds arrays and structs, which no `const` can build.
            Signal::Metrics => SourceMeta {
                kind: "otlpReceiver",
                record: otlp_metric_point(),
                source: OTLP_RECEIVER_FIELDS,
            },
            Signal::Traces => SourceMeta::OTLP_TRACES,
        }
    }

    /// What a Prometheus receiver's record carries: one sample or native histogram, and its sender's address.
    // Its record holds arrays and structs, which no `const` can build.
    pub fn prometheus_receiver() -> SourceMeta {
        SourceMeta {
            kind: "prometheusReceiver",
            record: prometheus_record(),
            source: PROMETHEUS_RECEIVER_FIELDS,
        }
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
            SourceMeta::SYSLOG_RECEIVER,
            SourceMeta::ELASTIC_RECEIVER,
        ]
        .into_iter()
        .find(|m| m.kind == kind)
    }

    /// A source whose records carry nothing beyond their payload.
    pub const NONE: SourceMeta = SourceMeta {
        kind: "",
        record: PAYLOAD_RECORD,
        source: &[(SOURCE_BYTES, DataType::Bytes)],
    };

    /// This meta with `record` as its record's fields in place of `_raw`, and without `__source.bytes`. A field's name is a path, as a pipeline
    /// writes one (`user.name`, `labels["a.b"]`). Err for no fields, a name the engine keeps for itself (`__live`,
    /// `__source`), or two names where one is the other or holds it: each would take a slot another reads.
    pub fn declaring(self, record: Vec<(String, DataType)>) -> Result<SourceMeta, String> {
        if record.is_empty() {
            return Err("a source that declares its record declares at least one field".into());
        }
        self.declare(record, |_| false)
    }

    /// `declaring`, the record open (`opened`): an Avro record's. It may declare no field, and keeps every top-level
    /// field then.
    pub fn declaring_open(self, record: Vec<(String, DataType)>) -> Result<SourceMeta, String> {
        Ok(self.declare(record, |_| false)?.opened())
    }

    /// This meta with its record open: every top-level field of a payload no declared field claims is kept, as the
    /// root's unclaimed fields (`tree::UNCLAIMED`).
    pub fn opened(mut self) -> SourceMeta {
        let mut record = self.record.into_owned();
        record.push((crate::plan::tree::UNCLAIMED.into(), DataType::Variant));
        self.record = record.into();
        self
    }

    /// Whether this meta's record keeps the fields it does not declare (`opened`).
    pub fn is_open(&self) -> bool {
        self.record.iter().any(|(n, _)| n == crate::plan::tree::UNCLAIMED)
    }

    /// `declaring`, taking a field under an internal key `internal` allows.
    fn declare(self, record: Vec<(String, DataType)>, internal: fn(&str) -> bool) -> Result<SourceMeta, String> {
        let paths: Vec<Vec<String>> = record
            .iter()
            .map(|(name, _)| {
                crate::plan::tree::segments(name)
                    .map(std::borrow::Cow::into_owned)
                    .collect()
            })
            .collect();
        for (i, (name, _)) in record.iter().enumerate() {
            if paths[i]
                .first()
                .is_none_or(|key| key.is_empty() || crate::plan::tree::is_internal(key) && !internal(key))
            {
                return Err(format!("`{name}` is not a name a record field can take"));
            }
            if let Some(j) = (0..i).find(|&j| paths[j].starts_with(&paths[i]) || paths[i].starts_with(&paths[j])) {
                return Err(format!(
                    "`{}` and `{name}` are one field, or one holds the other",
                    record[j].0
                ));
            }
        }
        // Its chunks are the record's columns, so there is no payload for `__source.bytes` to read.
        let source = match self.source.split_last() {
            Some(((SOURCE_BYTES, _), rest)) => rest,
            _ => self.source,
        };
        Ok(SourceMeta {
            record: record.into_iter().map(|(n, t)| (n.into(), t)).collect(),
            source,
            ..self
        })
    }

    /// The meta of a sample captured under `columns`: each column a field of its record, read from the record object by
    /// its path and typed as spelled, `__source` and `__destination` columns as much as any other. A sample of no
    /// columns, a paste, is `_raw` text.
    pub fn of_columns(columns: &[crate::heartbeat::CaptureColumn]) -> Result<SourceMeta, String> {
        if columns.is_empty() {
            return Ok(SourceMeta::NONE);
        }
        let open = columns.iter().any(|c| c.name == crate::plan::tree::UNCLAIMED);
        let record = columns
            .iter()
            .filter(|c| c.name != crate::plan::tree::UNCLAIMED)
            .map(|c| {
                crate::expr::parse::parse_type(&c.ty)
                    .map(|ty| (c.name.clone(), ty))
                    .map_err(|e| format!("column `{}` has type {:?}, which does not read: {e}", c.name, c.ty))
            })
            .collect::<Result<Vec<_>, String>>()?;
        // `__live` is the one name a pipeline cannot take a value of from its input: the engine primes it.
        let meta = SourceMeta::NONE.declare(record, |key| key != "__live")?;
        Ok(if open { meta.opened() } else { meta })
    }

    /// The record's fields, in slot order.
    pub fn record(&self) -> &RecordFields {
        &self.record
    }

    /// Each of the record's fields, with its type, in slot order.
    pub fn record_fields(&self) -> Vec<(&str, DataType)> {
        self.record.iter().map(|(n, t)| (n.as_ref(), t.clone())).collect()
    }

    pub fn record_len(&self) -> usize {
        self.record.len()
    }

    /// This meta for a source that declares `encoding`. A source that reads bytes and declares `none` decodes
    /// nothing, so its record has no `_raw`.
    pub fn with_encoding(self, encoding: Encoding) -> SourceMeta {
        match encoding {
            Encoding::None if self.reads_bytes() => SourceMeta {
                record: std::borrow::Cow::Borrowed(&[]),
                ..self
            },
            _ => self,
        }
    }

    /// Whether this source's record is its payload decoded, `_raw`: it reads bytes and decodes them.
    pub fn decodes(&self) -> bool {
        self.reads_bytes() && !self.record.is_empty()
    }

    /// Whether this source hands over each record's payload as it read it, as `__source.bytes`. Its chunk is then
    /// one `bytes` column, the payloads, and its record is `_raw`, each payload decoded with the source's encoding.
    pub fn reads_bytes(&self) -> bool {
        self.source.iter().any(|(name, _)| *name == SOURCE_BYTES)
    }

    /// The type of `field`, a field of the record or a `__source` field, or `None` where this source declares none.
    pub fn field_type(&self, field: &str) -> Option<DataType> {
        self.record
            .iter()
            .map(|(n, t)| (n.as_ref(), t))
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
        crate::core::batch::PREFIX_SLOTS + self.record_len() + field
    }

    /// The `__source` field at `slot`, if one is there.
    pub fn source_field_at(&self, slot: crate::core::id::SlotId) -> Option<usize> {
        slot.checked_sub(self.source_slot(0)).filter(|i| *i < self.source.len())
    }

    /// Field `field` as a column over `records`, in record order, for records `source` read: its name in the
    /// deployment, `None` where there is no deployment to name it. A property of the kind, not of a live `Source`: the
    /// values ride on `SourceRecord`, so transform builds these columns without touching anything ingest owns. Keyed
    /// by the field's name rather than its position, so an entry can move within a source's list without its
    /// extractor staying behind at the old index.
    ///
    /// Each row whose value had bytes that did not decode, replaced with U+FFFD, is pushed to `repaired`.
    pub fn column(
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
                        Some(value) => Column::constant(Scalar::Variant(value.to_vec()), records.len()),
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
    /// Put this meta's columns over `records` into `batch`: every field of the record, and each `__source` field
    /// `wanted` names by its index. `text` is each record's payload decoded, read where the source `reads_bytes`;
    /// `source` is what `__source.name` reads.
    ///
    /// Returns each value ingest could not take as it came, as its row, its slot, how, and the value as read: a
    /// `__source` value repaired to valid UTF-8, or a record value that is not its field's type and is null.
    pub fn ingest<T: AsRef<str>>(
        &self,
        batch: &mut Batch,
        records: &[crate::io::SourceRecord],
        text: &[T],
        source: Option<&str>,
        wanted: impl Fn(usize) -> bool,
        pool: &mut crate::core::buffer::BufferPool,
    ) -> Vec<(u32, crate::core::id::SlotId, ExceptionKind, Option<String>)> {
        let mut noted = Vec::new();
        if self.decodes() {
            let values = text.iter().map(|t| Some(t.as_ref().as_bytes()));
            batch.put(crate::core::batch::SLOT_RAW, str_column(pool, values), pool);
        } else if !self.reads_bytes() {
            let fields = &self.record;
            let (chunk, rows) = chunk_rows(records);
            for (i, (_, ty)) in fields.iter().enumerate() {
                let slot = crate::core::batch::PREFIX_SLOTS + i;
                let column = match &chunk {
                    Some(chunk) => chunk.column(i).gather(pool, &rows),
                    None => Column::empty(ty.clone(), 0),
                };
                batch.put(slot, column, pool);
            }
            if let Some(chunk) = &chunk {
                for (at, &row) in rows.iter().enumerate() {
                    // A payload that did not read has no field of its own to blame, so its record's first holds it.
                    if let Some(why) = chunk.unread(row) {
                        let kind = ExceptionKind::Unparsable;
                        noted.push((at as u32, crate::core::batch::PREFIX_SLOTS, kind, Some(why.to_string())));
                        continue;
                    }
                    let from = chunk.failed().partition_point(|(r, _, _)| *r < row);
                    for (_, field, value) in chunk.failed()[from..].iter().take_while(|(r, _, _)| *r == row) {
                        let kind = ExceptionKind::UnparsableField {
                            to: fields[*field].1.clone(),
                        };
                        let slot = crate::core::batch::PREFIX_SLOTS + field;
                        noted.push((at as u32, slot, kind, Some(value.clone())));
                    }
                }
            }
        }
        let mut repaired = Vec::new();
        for i in (0..self.source.len()).filter(|i| wanted(*i)) {
            let slot = self.source_slot(i);
            batch.put(slot, self.column(i, records, source, pool, &mut repaired), pool);
            noted.extend(
                repaired
                    .drain(..)
                    .map(|row| (row, slot, ExceptionKind::InvalidUtf8, None)),
            );
        }
        noted
    }
}

impl SourceMeta {
    /// `record`'s payload as the source read it, before any decoding: its `__source.bytes`. `None` for a source
    /// that declares none.
    pub fn read_bytes<'r>(&self, record: &'r crate::io::SourceRecord) -> Option<&'r [u8]> {
        if self.reads_bytes() { record.bytes() } else { None }
    }

    /// `read_bytes` of each of `records`, looking up each chunk's bytes once rather than once a record.
    pub fn read_bytes_each<'r>(
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

    /// What a destination writing `field`'s bytes takes of `record`: its `__source.bytes`, or a `bytes` field of its
    /// record. `None` for a null, or a field it does not have, which loading refuses (`check_bytes_fields`).
    pub fn field_bytes(&self, record: &crate::io::SourceRecord, field: &str) -> Option<Vec<u8>> {
        if field == SOURCE_BYTES {
            return self.read_bytes(record).map(<[u8]>::to_vec);
        }
        if self.reads_bytes() {
            return None;
        }
        let at = self
            .record
            .iter()
            .position(|(n, t)| n == field && *t == DataType::Bytes)?;
        let column = record.chunk.column(at);
        let row = record.row as usize;
        (column.is_valid(row)).then(|| column.varlen().expect("a bytes column").get(row).to_vec())
    }

    /// `record` as the document a pipeline with no steps would write for it: its decoded `text` as `_raw`, nothing
    /// for a source that decodes none, or a row's fields.
    pub fn record_json(&self, record: &crate::io::SourceRecord, text: &str) -> Vec<u8> {
        if self.decodes() {
            let mut json = br#"{"_raw":"#.to_vec();
            crate::format::json::write_json_string(text.as_bytes(), &mut json);
            json.push(b'}');
            json
        } else if self.reads_bytes() {
            b"{}".to_vec()
        } else {
            self.row_view(&record.chunk).row(record.row as usize)
        }
    }

    /// The text of `record`'s field `field`, as a destination writing one field's text takes it: its decoded `text`
    /// for `_raw`, or a row's field. `None` for a field the record does not have, or a null.
    pub fn record_text(&self, record: &crate::io::SourceRecord, text: &str, field: &str) -> Option<Vec<u8>> {
        if self.reads_bytes() {
            (self.decodes() && field == "_raw").then(|| text.as_bytes().to_vec())
        } else {
            self.row_view(&record.chunk)
                .text(field, record.row as usize)
                .map(String::into_bytes)
        }
    }

    /// A view of `chunk`'s columns under the names this meta declares for them.
    fn row_view<'a>(&'a self, chunk: &'a crate::io::RecordChunk) -> crate::format::record::RecordView<'a> {
        assert!(!self.reads_bytes(), "a row's columns are its record's fields");
        crate::format::record::RecordView::from_named(
            self.record
                .iter()
                .enumerate()
                .map(|(i, (name, _))| (name.as_ref(), chunk.column(i))),
        )
    }
}

/// The name of the first value in `object`, an object at `path` in a record, that is no declared field of `paths`
/// and in none: a declared field's value is its own, whatever it holds.
fn undeclared(
    object: &crate::io::RecordObject,
    path: &mut Vec<String>,
    name: &str,
    paths: &[Vec<String>],
) -> Option<String> {
    object.iter().find_map(|(key, value)| {
        path.push(key.clone());
        let found = if paths.iter().any(|p| p == path) {
            None
        } else {
            match value.as_object() {
                Some(inner) if paths.iter().any(|p| p.starts_with(path)) => {
                    undeclared(inner, path, &crate::plan::tree::child(name, key), paths)
                }
                _ => Some(crate::plan::tree::child(name, key)),
            }
        };
        path.pop();
        found
    })
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
    /// Sets a Kafka source's `fetchBytes`. Other kinds have none.
    pub fn set_fetch_bytes(&mut self, bytes: usize) {
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
                Receiver::Syslog(_) => SourceMeta::SYSLOG_RECEIVER,
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
                receiver: Receiver::Syslog(s),
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

/// One identity a worker connects out under: the certificate it shows a source or a destination, the credential
/// holding that certificate's private key, and the authorities it verifies that far end against.
///
/// A far end that asks for no certificate needs only the authorities, and one served from a public root needs only
/// the certificate, so either half may be absent -- but not both, which `config_file::load` refuses. `authorities`
/// absent means the platform's own roots.
///
/// A certificate is public and travels in the bundle; its key is named, like every other credential, and resolved
/// when the client is built.
///
/// Held behind an `Arc` wherever a connector names one: a deployment usually speaks one organization's PKI, so the
/// same certificate and the same authorities are named by many connectors and copied into none of them.
#[derive(Clone, Debug, PartialEq)]
pub struct Identity {
    /// What the bundle calls it, for a message an operator reads.
    pub id: String,
    /// PEM, leaf first, with any intermediates after it.
    pub certificate: Option<String>,
    pub key: Option<SecretRef>,
    /// PEM, one or more roots.
    pub authorities: Option<String>,
    /// Whether a client under it also trusts the public roots (`io::trust`). A receiver never does.
    pub trust_public: bool,
}

/// A Confluent Schema Registry: where it is, and who signs in to it. Reached under its connector's identity.
#[derive(Clone, Debug, PartialEq)]
pub struct SchemaRegistry {
    pub url: String,
    /// Basic auth: the user, and where the password lives. `None` signs in as no one.
    pub basic: Option<(String, SecretRef)>,
}

/// How a source reads its payloads as Avro, into a record of its own (`io::avro`).
#[derive(Clone, Debug)]
pub struct AvroRead {
    /// Where a stream's payloads are Confluent-framed: the registry holding each writer's schema. `None` for an
    /// object, a container file holding its own.
    pub registry: Option<SchemaRegistry>,
    /// What a record carries: the record each payload is read into, and its source's `__source` fields but its
    /// bytes.
    pub meta: SourceMeta,
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
    /// What this consumer connects under. `None` is no certificate and the platform's own roots.
    pub identity: Option<std::sync::Arc<Identity>>,
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
    pub fn name(self) -> &'static str {
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
pub fn dlq_chain(named: &HashMap<DestId, Vec<DestId>>, dest: DestId) -> Vec<DestId> {
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
                | ObjectFormat::Parquet
                | ObjectFormat::Avro
                | ObjectFormat::Csv => Payload::Record,
                ObjectFormat::Text { field } => Payload::Text { field: field.clone() },
                ObjectFormat::Bytes { field, .. } => Payload::Bytes { field: field.clone() },
            },
            DestinationKind::Prometheus(_)
            | DestinationKind::LogAnalytics(_)
            | DestinationKind::Iceberg(_)
            | DestinationKind::Delta(_) => Payload::Record,
        }
    }

    /// Whether this destination keys documents on the row's own `__destination.id`. Kafka never does -- it has no document
    /// identity, and its own `key` is a partition key, a different thing entirely (`io::OutgoingRecord`).
    pub fn wants_doc_id(&self) -> bool {
        match self {
            DestinationKind::Kafka(_) => false,
            DestinationKind::Elasticsearch(e) => e.doc_id.wants_doc_id(),
            DestinationKind::SplunkHec(_)
            | DestinationKind::Objects(_)
            | DestinationKind::Http(_)
            | DestinationKind::Prometheus(_)
            | DestinationKind::LogAnalytics(_)
            | DestinationKind::Iceberg(_)
            | DestinationKind::Delta(_) => false,
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
            | DestinationKind::Iceberg(_)
            | DestinationKind::Delta(_) => None,
        }
    }

    /// How long one batch may take to deliver before it is a stall.
    pub fn stall_after(&self) -> std::time::Duration {
        match self {
            DestinationKind::Kafka(k) => k.stall_after,
            DestinationKind::Elasticsearch(e) => e.stall_after,
            DestinationKind::SplunkHec(s) => s.stall_after,
            DestinationKind::Objects(o) => o.stall_after,
            DestinationKind::Http(h) => h.stall_after,
            DestinationKind::Prometheus(p) => p.stall_after,
            DestinationKind::LogAnalytics(s) => s.http.stall_after,
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
            DestinationKind::LogAnalytics(s) => s.http.buffer_bytes,
            DestinationKind::Iceberg(i) => i.buffer_bytes,
            DestinationKind::Delta(d) => d.buffer_bytes,
        }
    }

    /// The destination's `bufferBytes`: all it holds of what it has not yet delivered, egress's queue and, for Kafka,
    /// librdkafka's own queue together.
    pub fn buffer_bytes(&self) -> usize {
        match self {
            DestinationKind::Kafka(k) => k.buffer_bytes,
            kind => kind.queue_bytes(),
        }
    }

    /// Sets the destination's `bufferBytes`.
    pub fn set_buffer_bytes(&mut self, bytes: usize) {
        match self {
            DestinationKind::Kafka(k) => k.buffer_bytes = bytes,
            DestinationKind::Elasticsearch(e) => e.buffer_bytes = bytes,
            DestinationKind::SplunkHec(s) => s.buffer_bytes = bytes,
            DestinationKind::Objects(o) => o.buffer_bytes = bytes,
            DestinationKind::Http(h) => h.buffer_bytes = bytes,
            DestinationKind::Prometheus(p) => p.buffer_bytes = bytes,
            DestinationKind::LogAnalytics(s) => s.http.buffer_bytes = bytes,
            DestinationKind::Iceberg(i) => i.buffer_bytes = bytes,
            DestinationKind::Delta(d) => d.buffer_bytes = bytes,
        }
    }

    /// The least buffer the destination runs with: the requests it starts with outstanding, each whole. 0 for one that
    /// sends no request of a size of its own.
    pub fn least_buffer_bytes(&self) -> usize {
        match self {
            DestinationKind::Elasticsearch(e) => e.max_bulk_bytes * e.in_flight,
            DestinationKind::SplunkHec(s) => s.max_request_bytes * s.in_flight,
            DestinationKind::Http(h) => h.max_request_bytes * h.in_flight,
            DestinationKind::Prometheus(p) => p.max_request_bytes,
            DestinationKind::LogAnalytics(s) => s.http.max_request_bytes * s.http.in_flight,
            DestinationKind::Kafka(_)
            | DestinationKind::Objects(_)
            | DestinationKind::Iceberg(_)
            | DestinationKind::Delta(_) => 0,
        }
    }

    /// Which field of the row this destination reads the document identity from, if it does. `None` means it relies on
    /// the pipeline's own `__destination.id`, or wants no identity at all.
    pub fn id_field(&self) -> Option<&str> {
        match self {
            DestinationKind::Kafka(_) => None,
            DestinationKind::Elasticsearch(e) if e.doc_id.wants_doc_id() => e.id_field.as_deref(),
            DestinationKind::Elasticsearch(_)
            | DestinationKind::SplunkHec(_)
            | DestinationKind::Objects(_)
            | DestinationKind::Http(_)
            | DestinationKind::Prometheus(_)
            | DestinationKind::LogAnalytics(_)
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
    /// What this client connects under. `None` is no certificate and the platform's own roots -- which an
    /// internally-issued server certificate will not be under.
    pub identity: Option<std::sync::Arc<Identity>>,
    /// Where each document's `_id` comes from, and therefore what a re-send after a crash does. See `DocIdPolicy`.
    pub doc_id: DocIdPolicy,
    /// How many bulk requests this destination starts with outstanding, each on its own connection.
    ///
    /// Where the loop begins, not where it stays: see `max_in_flight` for where it stops, and
    /// `io::pipelined::Pool::note_outcome` for what moves it.
    ///
    /// Concurrency is what buys throughput here, because a bulk round trip costs about the same whatever the batch
    /// carries -- so a worker with one request outstanding cannot go faster than `1/round-trip` no matter what else is
    /// tuned. The ceiling is the cluster's own write capacity, past which it answers 429 and everything queues anyway,
    /// which is the signal the loop reads rather than a figure anyone has to guess.
    pub in_flight: usize,
    /// Bytes of documents one bulk request may carry before the destination sends early.
    pub max_bulk_bytes: usize,
    /// Bytes egress may hold for this destination -- queued, and sent but unacknowledged. Divided by `max_bulk_bytes`
    /// it is also the most requests that can be outstanding at once, since a batch holds its budget until the cluster
    /// answers for it.
    pub buffer_bytes: usize,
    /// How long one batch may take to deliver before it is a stall.
    pub stall_after: std::time::Duration,
    /// Which of the row's own fields holds the document's identity, for a `doc_id` policy that wants one. `None` (the
    /// default) means the row must set `__destination.id` itself.
    ///
    /// This is the ordinary case and the reason it is configured here rather than written into every pipeline: a
    /// document that has a stable identity usually already carries it in a field, and pointing at that field is
    /// configuration, not logic. It is also the answer for a tiered deployment, where identity has to survive a hop and
    /// so has to live in a real serialized field anyway (design doc, Runtime stages: a document identity and a
    /// partition key are separate on purpose).
    ///
    /// A pipeline that needs to *compute* an identity still sets `__destination.id`, which takes precedence per row — the same
    /// relationship `__destination.index` has to `default_index`.
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
    /// What this client connects under. `None` is no certificate and the platform's own roots.
    pub identity: Option<std::sync::Arc<Identity>>,
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
    pub fn max_in_flight(&self) -> usize {
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
    /// What this client connects under, to the catalog and to storage.
    pub identity: Option<std::sync::Arc<Identity>>,
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
    /// What this client connects under.
    pub identity: Option<std::sync::Arc<Identity>>,
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
    /// What this client connects under. `None` is no certificate and the platform's own roots.
    pub identity: Option<std::sync::Arc<Identity>>,
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
    pub fn max_in_flight(&self) -> usize {
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

/// Where a Prometheus destination sends remote_write. Every row goes to the one URL, one request at a time: a store
/// refuses a series' sample older than the last it stored, so two requests in flight could land a series out of order.
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
    /// What this client connects under. `None` is no certificate and the platform's own roots.
    pub identity: Option<std::sync::Arc<Identity>>,
    /// Bytes of samples one request may carry, before compression, before the destination sends early: each sample
    /// counted with its labels, which a request sends once per series, so a request is smaller.
    pub max_request_bytes: usize,
    /// Bytes egress may hold for this destination -- queued, and sent but unacknowledged.
    pub buffer_bytes: usize,
    /// How long one batch may take to deliver before it is a stall.
    pub stall_after: std::time::Duration,
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
    /// What this client connects under. `None` is no certificate and the platform's own roots.
    pub identity: Option<std::sync::Arc<Identity>>,
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
    /// The tokens a sender may present, each by its name. Empty is a receiver that admits whoever its identity's
    /// authorities vouch for.
    pub tokens: Vec<(String, SecretRef)>,
    /// What it serves TLS under: its certificate and key, and, with `authorities`, the roots a sender's own
    /// certificate must chain to. `None` serves plain TCP.
    pub identity: Option<std::sync::Arc<Identity>>,
    /// The largest request body it reads. A larger one is refused whole.
    pub max_request_bytes: usize,
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
    /// Syslog: over TCP a message per record, framed by octet count or ended by a newline; over UDP a datagram per
    /// record.
    Syslog(SyslogReceiverConfig),
    /// Elasticsearch's `_bulk` API, as Beats, Fluent Bit and other Elasticsearch clients send it: a document per
    /// `index` or `create` action.
    Elastic,
}

impl ReceiverConfig {
    /// What it listens on. Every receiver but a syslog receiver given UDP takes connections over TCP.
    pub fn listens(&self) -> Listen {
        let transport = match &self.receiver {
            Receiver::Syslog(syslog) => syslog.transport,
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
    pub fn is_tcp(&self) -> bool {
        *self == Transport::Tcp
    }
}

/// Who a syslog receiver admits.
#[derive(Clone, Debug, Default)]
pub struct SyslogReceiverConfig {
    /// The networks a sender may connect from. Empty admits whoever TLS admitted.
    pub allow_from: Vec<ipnetwork::IpNetwork>,
    pub encoding: Encoding,
    pub transport: Transport,
    /// Where a message over TCP ends, when not at a newline. Octet counting still frames a message that starts with
    /// its length.
    pub line_breaker: Option<crate::io::line_breaker::LineBreaker>,
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
    pub meta: SourceMeta,
    /// The database, the mode and a digest of the query and what the mode remembers by: what its progress is kept
    /// under, so a source pointed at another database, or changed in what it reads, starts afresh.
    pub place: String,
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

    /// The identity it connects under, if it names one.
    pub fn identity(&self) -> Option<&std::sync::Arc<Identity>> {
        match self {
            SqlDatabase::Postgres(p) => p.identity.as_ref(),
            SqlDatabase::MySql(m) => m.identity.as_ref(),
        }
    }

    /// `host:port/database`: where it reads, as a place names it.
    pub fn address(&self) -> String {
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
    /// What this client connects under. `None` is no certificate and the platform's own roots.
    pub identity: Option<std::sync::Arc<Identity>>,
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
    pub fn logs_url(&self) -> String {
        format!("{}/api/v1/logs", self.org)
    }

    /// Where a service app signs in: the org authorization server, the one that grants Okta's own API scopes.
    pub fn token_url(&self) -> String {
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
    /// What this client connects under. `None` is no certificate and the platform's own roots.
    pub identity: Option<std::sync::Arc<Identity>>,
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

/// How an object's bytes become records.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum Framing {
    /// One record per line; a line ends at `\n`, and a `\r` before it is dropped.
    #[default]
    Lines,
    /// The whole object is one record.
    Object,
    /// One record per line, as `Lines`, each unescaped as a `bytes` object with `Newlines::Escape` wrote it.
    EscapedLines,
    /// One record per event, cut where the line breaker breaks the object's bytes.
    LineBreaker(crate::io::line_breaker::LineBreaker),
}

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
    pub fn address(&self) -> String {
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
    /// One Parquet file, its columns the record of the route whose rows it holds (`CompiledTopology::records`).
    Parquet,
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
    pub fn column_format(&self) -> Option<&'static str> {
        match self {
            ObjectFormat::Parquet => Some("Parquet"),
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

/// A destination's codec: a Kafka batch, an object, or a Parquet file's each column chunk is compressed with it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Compression {
    None,
    Gzip,
    Snappy,
    Lz4,
    Zstd,
}

impl Compression {
    /// librdkafka's `compression.type` value.
    pub fn as_property(self) -> &'static str {
        match self {
            Compression::None => "none",
            Compression::Gzip => "gzip",
            Compression::Snappy => "snappy",
            Compression::Lz4 => "lz4",
            Compression::Zstd => "zstd",
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

/// Where an Elasticsearch document's `_id` comes from.
///
/// At-least-once means a batch can be re-sent after a crash between flush and commit. Elasticsearch is one of the few
/// destinations where that need not produce a duplicate document — but only if smeltr supplies a *stable* id, and only
/// at a cost, so which of the three this is has to be asked for by name rather than inferred.
///
/// A named list rather than a pair of booleans (`use an id?` × `create or index?`): the question an operator is answering — what should happen when the same document arrives twice — has three
/// sensible answers with different outcomes, and two independent booleans would spell four combinations of which one
/// (an id with no conflict policy) is meaningless.
///
/// The id itself comes from the row's own `__destination.id` field, set by the pipeline exactly the way `__destination.index` is (design
/// doc, Runtime stages). A row that leaves it null falls back to `Auto` for that row alone.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum DocIdPolicy {
    /// Elasticsearch generates the `_id`. The fastest option and the default: nothing has to be checked against what is
    /// already indexed. Plain at-least-once — a re-send after a crash indexes a second copy of the document.
    #[default]
    Auto,
    /// The row's `__destination.id` as `_id`, written with a `create` action: a re-send is refused as a
    /// `version_conflict_engine_exception`, which counts as success, so the **first** copy is kept and the duplicate is
    /// dropped. Effectively-once, at the cost of real indexing throughput — Elasticsearch must check for an existing
    /// document rather than blindly appending.
    Dedupe,
    /// The row's `__destination.id` as `_id`, written with an `index` action: a re-send **replaces** the document, so the last
    /// copy is kept. Also removes duplicates, and additionally lets a pipeline reprocess a record to correct what was
    /// indexed the first time.
    ///
    /// Not usable against a data stream, which accepts only `create`.
    ///
    /// **"Last" is not guaranteed to be the last one smeltr saw.** A worker keeps several batches in flight at once
    /// (`runtime::Reader::max_in_flight`), so two versions of the same `_id` in different batches can reach the cluster in
    /// either order and the loser is whichever arrives second. `Auto`/`Dedupe` are unaffected: they write with
    /// `create`, so a duplicate is refused rather than overwritten, which is the semantic they are asking for either
    /// way.
    ///
    /// Accepted deliberately rather than special-cased -- holding `Overwrite` destinations to one batch in flight would
    /// buy determinism for the uncommon case at the cost of a rule every future destination has to know about. Revisit
    /// if a deployment actually depends on it.
    Overwrite,
}

impl DocIdPolicy {
    /// Whether this policy wants a `__destination.id` from the row at all.
    pub fn wants_doc_id(self) -> bool {
        !matches!(self, DocIdPolicy::Auto)
    }
}

#[derive(Clone, Debug)]
pub struct KafkaDestinationConfig {
    /// What each message's value is.
    pub payload: Payload,
    pub compression: Compression,
    /// How long one batch may take to deliver before it is a stall.
    pub stall_after: std::time::Duration,
    pub brokers: String,
    /// Used when a row has no live `__destination.topic` (see the design doc's "Pipeline-set config variables") — the
    /// destination's own default, not a per-row override.
    pub default_topic: String,
    /// What this producer connects under. `None` is no certificate and the platform's own roots.
    pub identity: Option<std::sync::Arc<Identity>>,
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
    pub fn queue_bytes(&self) -> usize {
        self.buffer_bytes / 2
    }

    /// The producer's share of `buffer_bytes`: its queue's bound (`queue.buffering.max.kbytes`), and what its
    /// batches are sized from at deployment.
    pub fn producer_bytes(&self) -> usize {
        self.buffer_bytes - self.queue_bytes()
    }
}

/// One entry in a source's ordered route list — design doc, Scope: "Multiple routes per source."
#[derive(Clone, Debug)]
pub struct RouteConfig {
    /// The author's id (`RouteDto::id`). A route has no positional id the way a source or destination does; this is
    /// what names its edge on the map and what a diff tells an edit from a delete-and-insert by.
    pub id: String,
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
pub struct CompiledDeliverRule {
    /// `None` matches every surviving row — the catch-all.
    pub when: Option<Ir>,
    pub to: Vec<DestId>,
    /// Regex literals in `when`, compiled once. Evaluation points `Ctx::patterns` at this, exactly as `route_masks`
    /// does for `cond`.
    pub compiled_patterns: Vec<crate::core::ctx::CompiledPattern>,
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
pub struct Fitted {
    pub sources: std::collections::BTreeSet<SourceId>,
    pub destinations: std::collections::BTreeSet<DestId>,
}

#[derive(Clone, Debug, Default)]
pub struct TopologyConfig {
    pub sources: Vec<SourceConfig>,
    pub destinations: Vec<DestinationConfig>,
    /// What the bundle leaves for the worker to size. See [`Fitted`].
    pub fitted: Fitted,
    pub routes: Vec<RouteConfig>,
    /// The group's answer to every exception kind. See `CompiledTopology::on_exception`.
    pub on_exception: OnException,
    /// Every credential this bundle names (design doc, Secrets boundary: a bundle holds the *name* of the place a
    /// secret lives, never the value), read from the bundle at load. This is what makes "a named secret that is
    /// missing is a startup failure" checkable before a deployment is torn down: the answer depends only on the
    /// bundle, so a worker can ask it of a new commit while still running the old one.
    pub named_secrets: Vec<NamedSecret>,
    /// Every identity a connector of this bundle connects under, once each and in the order they are first named:
    /// the ones whose certificate and key a connection depends on.
    pub identities: Vec<std::sync::Arc<Identity>>,
}

impl TopologyConfig {
    /// Whether a row entering `source` can be `Unmatched` (design doc, Exception taxonomy): no route on it matches
    /// unconditionally.
    ///
    /// A route with no `cond` takes every row, so one makes the source's routing total. Anything narrower answers
    /// `true`, a source with no routes at all included: whether a set of conditions is exhaustive is a question about
    /// the expression language, and a wrong `false` hides a path rows are really taking.
    pub fn falls_through(&self, source: SourceId) -> bool {
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

/// A variable set to the empty string counts as unset, matching `startup_config`: that is what an unset
/// docker-compose variable expands to, and an empty credential authenticates as nobody.
fn usable(read: Option<String>) -> Option<String> {
    read.filter(|v| !v.is_empty())
}

/// The credentials a worker can obtain: its own environment, plus what the leader has sent it. Keyed by name *and*
/// version, because a rotation in flight means holding the version that is running alongside the one a bundle is
/// about to quote.
#[derive(Clone, Default)]
pub struct HeldSecrets {
    stored: std::collections::BTreeMap<(String, i64), String>,
}

/// Names and versions, never values. This holds every credential the deployment uses, so one `{:?}` reaching a
/// panic message or a log line would be all of them at once -- the reason `leader::secrets::SealingKey` derives
/// nothing at all.
impl std::fmt::Debug for HeldSecrets {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HeldSecrets").field("holds", &self.versions()).finish()
    }
}

impl HeldSecrets {
    /// Replaces what the leader has sent. Each entry is a name, a version, and that version's value.
    pub fn set_stored(&mut self, values: impl IntoIterator<Item = (String, i64, String)>) {
        self.stored = values.into_iter().map(|(n, v, value)| ((n, v), value)).collect();
    }

    /// The name, version and value of every credential in `wanted` that this worker holds, for handing to the
    /// process that will use them. The one place a value is read back out: it crosses to a runtime on a private
    /// pipe and nowhere else, and a runtime is given what its own bundle quotes rather than everything this
    /// worker has ever been sent.
    pub fn deliveries_for(&self, wanted: &[NamedSecret]) -> Vec<(String, i64, String)> {
        self.stored
            .iter()
            .filter(|((name, version), _)| {
                wanted.iter().any(|s| {
                    matches!(&s.reference, SecretRef::LeaderStored { name: n, version: v } if n == name && v == version)
                })
            })
            .map(|((name, version), value)| (name.clone(), *version, value.clone()))
            .collect()
    }

    /// Adds what the leader has just sent, keeping everything else. The leader answers with what this worker lacks,
    /// so an empty delivery means nothing changed rather than that nothing is held.
    pub fn take(&mut self, delivered: impl IntoIterator<Item = crate::heartbeat::SecretDelivery>) {
        for d in delivered {
            self.stored.insert((d.name, d.version), d.value);
        }
    }

    /// Drops everything `wanted` does not name -- what a worker does once a deployment has loaded, so the versions
    /// it carries are the ones something runs on rather than every one it has ever been sent.
    pub fn keep_only(&mut self, wanted: &[NamedSecret]) {
        self.stored.retain(|(name, version), _| {
            wanted.iter().any(|s| {
                matches!(&s.reference, SecretRef::LeaderStored { name: n, version: v } if n == name && v == version)
            })
        });
    }

    /// Every `(name, version)` held, which is what a worker tells the leader so it is sent only what it lacks.
    pub fn versions(&self) -> Vec<(String, i64)> {
        self.stored.keys().cloned().collect()
    }

    /// The value, or `None` when this worker cannot obtain it. A leader-stored reference matches one version and no
    /// other: a bundle quoting version 7 runs on version 7, never on whatever arrived since.
    pub fn value(&self, reference: &SecretRef) -> Option<String> {
        match reference {
            SecretRef::WorkerLocal(var) => usable(std::env::var(var).ok()),
            SecretRef::LeaderStored { name, version } => self.stored.get(&(name.clone(), *version)).cloned(),
        }
    }
}

/// Whether `identity`'s certificate and key belong together, when both are named and the key is one `held` has.
/// `None` when there is nothing to check yet: an identity presenting nothing pairs with nothing, and a key this
/// holder does not have at all is [`TopologyConfig::missing_secrets`]' concern, not this one's.
///
/// A free function rather than a method, so [`crate::worker::capture`]'s cold path -- which builds one identity's
/// source and never has a whole [`TopologyConfig`] to ask -- can run the same check on the one identity it has.
pub fn identity_pairing_error(identity: &Identity, held: &HeldSecrets) -> Option<String> {
    let (certificate, key_ref) = (identity.certificate.as_ref()?, identity.key.as_ref()?);
    let key = held.value(key_ref)?;
    crate::leader::tls::is_pair(certificate.as_bytes(), key.as_bytes())
        .err()
        .map(|why| {
            format!(
                "identity {:?} names a key that is not its certificate's: {why}",
                identity.id
            )
        })
}

/// The key an identity names, as one entry of [`TopologyConfig::named_secrets`].
pub(crate) fn identity_secret(identity: Option<&Identity>, what: &str) -> Option<NamedSecret> {
    let identity = identity?;
    Some(NamedSecret {
        reference: identity.key.clone()?,
        what: format!("{what}: the identity {:?}", identity.id),
    })
}

impl TopologyConfig {
    /// The named credentials this worker cannot obtain, as ready-to-print lines. Empty means the bundle can be
    /// deployed here.
    pub fn missing_secrets(&self, held: &HeldSecrets) -> Vec<String> {
        self.named_secrets
            .iter()
            .filter(|s| held.value(&s.reference).is_none())
            .map(|s| {
                format!(
                    "{} names {}, which this worker does not have",
                    s.what,
                    s.reference.describe()
                )
            })
            .collect()
    }

    /// Every identity naming both halves whose certificate and key do not belong together, among the keys this
    /// worker holds. `put_identities` proves the pair at save time, but a secret's name and version are reusable --
    /// deleted and recreated under the same reference, holding different content -- so that proof does not survive
    /// to whenever this commit actually runs. Checked here instead, wherever a commit is about to, so a mismatch is
    /// refused before anything is torn down rather than discovered at the handshake, after it already has been.
    pub fn identity_pairing_errors(&self, held: &HeldSecrets) -> Vec<String> {
        self.identities
            .iter()
            .filter_map(|identity| identity_pairing_error(identity, held))
            .collect()
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
    pub id: String,
    pub source: SourceId,
    pub cond: Option<Ir>,
    pub pipeline: usize,
    /// This route's own delivery rules, destinations resolved to `DestId`s and each `when` compiled against the target
    /// pipeline's *output*.
    pub deliver: Vec<CompiledDeliverRule>,
    /// Every regex literal `cond` interned, compiled once here — this route's own counterpart to
    /// `Plan::compiled_patterns`. A route condition resolves against its own disjoint `ResourceBuilder` (see this
    /// struct's doc), so it cannot share a target pipeline's `compiled_patterns`; `route_masks` points `Ctx::patterns`
    /// at this before evaluating `cond`, the same way `plan::exec::run` points it at `Plan::compiled_patterns`.
    pub compiled_patterns: Vec<crate::core::ctx::CompiledPattern>,
    /// `RouteConfig::on_exception`.
    pub on_exception: OnException,
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
    pub fn written_by(&self, route: &CompiledRoute) -> Vec<DestId> {
        let delivered = route.deliver.iter().flat_map(|rule| rule.to.iter().copied());
        let answered = route.on_exception.answers.iter().flat_map(|a| a.targets()).copied();
        self.with_dlqs(delivered.chain(answered).collect())
    }

    /// `dests` and every DLQ down their chains, each once. `dlq_of` already holds each whole chain.
    pub fn with_dlqs(&self, mut dests: Vec<DestId>) -> Vec<DestId> {
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
    pub fn delivery_destinations_from(&self, source: SourceId) -> Vec<DestId> {
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

pub fn compile_routes(routes: &[RouteConfig], metas: &SourceMetas) -> Result<Vec<CompiledRoute>, Vec<CompileError>> {
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
        flatten(crate::plan::plan::is_true(ir))
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
    let compiled_patterns = crate::plan::plan::compile_patterns(&res.patterns)?;
    Ok(CompiledRoute {
        id: r.id.clone(),
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
    pub pipelines: HashMap<(SourceMeta, usize), Plan>,
    /// What each source declared when these plans were compiled. The runtime reads it from here rather than being told
    /// separately, so a plan's `reads_meta` and the fields the batch is built from cannot disagree.
    pub metas: SourceMetas,
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
    /// What each destination writes of a row (`DestinationKind::payload`). Filled with `dlq_of`; one missing writes the
    /// record.
    pub payloads: HashMap<DestId, Payload>,
}

impl CompiledTopology {
    /// What `dest` writes of a row. One no destination config names, a worker's built by hand, writes the record.
    pub fn payload(&self, dest: DestId) -> Payload {
        self.payloads.get(&dest).cloned().unwrap_or(Payload::Record)
    }

    /// The forms a row bound for `dest` keeps for `dlqs`, the DLQs down its chain: each one's, but `dest`'s own.
    pub fn kept<'d>(&self, dest: DestId, dlqs: impl IntoIterator<Item = &'d DestId>) -> Vec<Payload> {
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
    pub fn text_fields(&self) -> Vec<String> {
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
    // tables, so they share one plan. Route order, deduped, so the errors a config produces stay in the same order from
    // run to run.
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
        crate::plan::plan::walk_slots(ir, &mut |slot| {
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
                    e.message = format!("route {r}'s deliver rule: {}", e.message);
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
            payloads: HashMap::new(),
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
            compiled_patterns: Vec::new(),
        });
    };
    let (ir, compiled_patterns) = output_condition(src, "condition", plan, tag_field, meta)?;
    Ok(CompiledDeliverRule {
        when: Some(ir),
        to: rule.to.clone(),
        compiled_patterns,
    })
}

/// Compile `src`, called `what` in an error, as a condition over `plan`'s finished batch: what a `deliver` rule reads,
/// and what a filter at a destination reads. Evaluated with `Ctx::patterns` pointed at the patterns it returns.
pub(crate) fn output_condition(
    src: &str,
    what: &str,
    plan: &Plan,
    tag_field: &str,
    meta: &SourceMeta,
) -> Result<(Ir, Vec<crate::core::ctx::CompiledPattern>), CompileError> {
    let mut res = ResourceBuilder::default();
    let ast = parse(src, None)?;
    let mut syms = SymbolTable::over_plan_output(
        &plan.delivery_layout(),
        &plan.tree,
        &ingest_fields(meta),
        &[(tag_field, plan.tag_slot())],
    );
    let ir = flatten(resolve(&ast, &mut syms, &mut res)?);
    let ir = condition(ir, src, what)?;
    Ok((ir, crate::plan::plan::compile_patterns(&res.patterns)?))
}

/// Evaluate every route belonging to `source` against `batch` — which must have the source's `ingest_fields` layout,
/// its record filled (and `__source.offset`, if the source has one) and `__live` primed all-true. Routes are unordered:
/// each sees the whole batch, so a row matching two routes runs both pipelines, and `__live` is left as it came.
///
/// Returns, for each route belonging to `source`, its index into `routes` and the Bool mask of rows dispatched to it.
/// A route that matches nothing still gets an all-false mask, since a batch's routing shape can vary row to row.
///
/// `metrics`, when present, records `smeltr_route_matched_total{source,route}` — the popcount of each route's own returned
/// mask — once per route per call, batch granularity (design doc, Observability). `None` for every caller
/// that isn't the real runtime driver loop (tests, benches): the metric is additive instrumentation, never load-bearing
/// for routing itself.
///
/// The `route` label is the route's own id, not its position: a series outlives the list's order, and the whole point
/// of a route id is that reordering or deleting a neighbor does not re-attribute this one's history
/// (`RouteConfig::id`). `source` is positional here and resolved at the wire, because routing works in `SourceId`
/// (`worker::resolve_subject_labels`).
pub fn route_masks<'a>(
    routes: &'a [CompiledRoute],
    source: SourceId,
    batch: &mut Batch,
    ctx: &mut Ctx<'a>,
    metrics: Option<&MetricsHandle>,
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

        if let Some(m) = metrics {
            let count = bits::popcount(matched.bool().unwrap().values) as u64;
            m.incr(
                "smeltr_route_matched_total",
                &[("source", &source.to_string()), ("route", r.id.as_str())],
                count,
            );
        }

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
        let routes = compile_routes(&[route(0, Some(&cond), 0)], &SourceMetas::from([(0, meta.clone())]))
            .unwrap_or_else(|e| panic!("`{cond}` compiles: {e:?}"));
        let layout: Vec<DataType> = crate::plan::plan::ingest_fields(&meta)
            .into_iter()
            .map(|(_, t)| t)
            .collect();
        let mut pool = BufferPool::new();
        let mut errors = ErrorSink::new();
        let mut batch = Batch::new(&layout, records.len(), 0);
        let texts = vec![""; records.len()];
        meta.ingest(&mut batch, records, &texts, None, |_| true, &mut pool);
        let mut live = Column::alloc(&mut pool, &DataType::Bool, records.len());
        bits::fill_valid(live.bool_mut().unwrap().values, records.len());
        batch.put(SLOT_LIVE, live, &mut pool);
        let mut ctx = Ctx::new(&mut pool, &mut errors);
        let masks = route_masks(&routes, 0, &mut batch, &mut ctx, None);
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
        assert_eq!(SourceMeta::KAFKA.read_bytes(&record), Some(&read[..]));
        assert_eq!(SourceMeta::KAFKA.field_bytes(&record, SOURCE_BYTES), Some(read.clone()));
        assert_eq!(SourceMeta::KAFKA.field_bytes(&record, "_raw"), None);

        let typed = SourceMeta::KAFKA
            .declaring(vec![("payload".into(), DataType::Bytes)])
            .unwrap();
        assert_eq!(typed.read_bytes(&record), None);
        assert_eq!(typed.field_type(SOURCE_BYTES), None);
        assert_eq!(typed.field_bytes(&record, SOURCE_BYTES), None);
        assert_eq!(typed.field_bytes(&record, "payload"), Some(read));
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
        let layout: Vec<DataType> = crate::plan::plan::ingest_fields(&meta)
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
        let noted = meta.ingest(&mut batch, &records, &["caf\u{fffd}"], None, |i| i == at, &mut pool);

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
        assert!(meta.reads_bytes() && !meta.decodes());
        assert_eq!(meta.field_type("_raw"), None);
        assert_eq!(meta.field_type(SOURCE_BYTES), Some(DataType::Bytes));
        assert_eq!(SourceMeta::KAFKA.with_encoding(Encoding::Auto), SourceMeta::KAFKA);

        let layout: Vec<DataType> = crate::plan::plan::ingest_fields(&meta)
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
        let noted = meta.ingest(&mut batch, &records, &[""], None, |i| i == at, &mut pool);
        assert!(noted.is_empty(), "{noted:?}");
        assert_eq!(batch.get(meta.source_slot(at)).varlen().unwrap().get(0), &read[..]);
        batch.reclaim(&mut pool);

        assert_eq!(meta.record_json(&records[0], ""), b"{}");
        assert_eq!(meta.record_text(&records[0], "", "_raw"), None);
        assert_eq!(meta.field_bytes(&records[0], SOURCE_BYTES), Some(read));
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
            let record = meta.record_object(&records[0], "caf\u{fffd}");
            let back = meta.read_stored(&[(&record, &fields[0])]).unwrap();
            assert_eq!(meta.read_bytes(&back[0]), Some(&read[..]));
        }
        let none = SourceMeta::KAFKA.with_encoding(Encoding::None);
        let err = none
            .read_stored(&[(&crate::io::RecordObject::new(), &crate::io::SourceFields::new())])
            .unwrap_err();
        assert!(err.contains("`__source.bytes`"), "{err}");
    }

    /// A stored record is read as its source reads: a payload source's must be one `_raw` text, and a typed source's may
    /// hold only the fields it declares, each value that is not its type null and counted failed.
    #[test]
    fn a_stored_record_reads_as_its_source_reads() {
        let object = |v: serde_json::Value| v.as_object().unwrap().clone();
        let fields = crate::io::SourceFields::new();

        let raw = object(serde_json::json!({"_raw": "line"}));
        let records = SourceMeta::KAFKA.read_stored(&[(&raw, &fields)]).unwrap();
        assert_eq!(records[0].bytes(), Some(&b"line"[..]));
        let typed = object(serde_json::json!({"id": 1}));
        let err = SourceMeta::KAFKA.read_stored(&[(&typed, &fields)]).unwrap_err();
        assert!(err.contains("record 1"), "{err}");

        let meta = SourceMeta::KAFKA
            .declaring(vec![("id".into(), DataType::I64), ("at".into(), DataType::DateTime)])
            .unwrap();
        let good = object(serde_json::json!({"id": 1, "at": "2026-10-10T00:00:00Z"}));
        let bad = object(serde_json::json!({"id": true}));
        let records = meta.read_stored(&[(&good, &fields), (&bad, &fields)]).unwrap();
        let chunk = &records[1].chunk;
        assert_eq!(chunk.failed(), &[(1, 0, "true".to_string())]);
        assert_eq!(
            meta.record_object(&records[0], ""),
            object(serde_json::json!({"id": 1, "at": "2026-10-10T00:00:00.000000Z"}))
        );
        let err = meta.read_stored(&[(&raw, &fields)]).unwrap_err();
        assert!(err.contains("`_raw`"), "{err}");

        // A value its record lists as failed fails again, as it was spelled, though its type would take it; a bytes
        // field's spelling is not read as base64.
        let listed = SourceMeta::KAFKA
            .declaring(vec![("name".into(), DataType::Str), ("blob".into(), DataType::Bytes)])
            .unwrap();
        let marked = object(serde_json::json!({"name": "{\"a\":1}", "blob": "true",
            "smeltr": {"mistyped": ["name", "blob"]}}));
        let records = listed.read_stored(&[(&marked, &fields)]).unwrap();
        assert_eq!(
            records[0].chunk.failed(),
            &[(0, 0, r#"{"a":1}"#.to_string()), (0, 1, "true".to_string())]
        );
        let tagged = object(serde_json::json!({"name": "a", "smeltr": {"mistyped": "a message"}}));
        let err = listed.read_stored(&[(&tagged, &fields)]).unwrap_err();
        assert!(
            err.contains("`smeltr"),
            "a string there is not a list of failed fields: {err}"
        );

        let nested = SourceMeta::KAFKA
            .declaring(vec![
                ("user.name".into(), DataType::Str),
                (r#"labels["a.b"]"#.into(), DataType::I64),
            ])
            .unwrap();
        let stored = object(serde_json::json!({"user": {"name": "ann"}, "labels": {"a.b": 2}}));
        let records = nested.read_stored(&[(&stored, &fields)]).unwrap();
        assert_eq!(
            nested.record_object(&records[0], ""),
            stored,
            "a path's value is where its path names"
        );
        let extra = object(serde_json::json!({"user": {"name": "ann", "age": 3}}));
        let err = nested.read_stored(&[(&extra, &fields)]).unwrap_err();
        assert!(err.contains("`user.age`"), "{err}");
    }

    /// A stored array or object reads as declared all the way down: an array of its elements' type, an object a
    /// field per member. A value that cannot be its type is failed with its JSON, and an undeclared field's type is
    /// refused.
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
        let records = meta.read_stored(&[(&good, &fields), (&bad, &fields)]).unwrap();
        assert_eq!(
            meta.record_object(&records[0], ""),
            object(
                serde_json::json!({"tags": [1, 2], "who": {"name": "ann", "at": "2026-10-10T00:00:00.000000Z", "took": 1500}})
            )
        );
        assert_eq!(
            records[1].chunk.failed(),
            &[
                (1, 0, r#"[1,"x"]"#.to_string()),
                (1, 1, r#"{"at":"never","name":"bo"}"#.to_string())
            ]
        );
        assert_eq!(
            meta.record_object(&records[1], ""),
            object(serde_json::json!({"who": {"name": "bo"}})),
            "a refused element nulls its array, and a refused member only itself"
        );

        let bytes = SourceMeta::KAFKA
            .declaring(vec![("blobs".into(), DataType::Array(Box::new(DataType::Bytes)))])
            .unwrap();
        let blobs = object(serde_json::json!({"blobs": ["3q0="]}));
        let read = bytes.read_stored(&[(&blobs, &fields)]).unwrap();
        assert_eq!(
            bytes.record_object(&read[0], ""),
            blobs,
            "stored bytes are base64, and read back as bytes"
        );
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
            .read_stored(&[(&crate::io::RecordObject::new(), &objects[0])])
            .unwrap();
        assert_eq!(back[0].chunk.time(0), Some(1_700_000_000_250_000));
        let unstamped = meta.objects(&records[..1], None);
        let back = meta
            .read_stored(&[(&crate::io::RecordObject::new(), &unstamped[0])])
            .unwrap();
        assert_eq!(back[0].chunk.time(0), None);

        let pasted = serde_json::json!({"time": "2023-11-14T22:13:20.25Z", "bytes": "b25l"});
        let back = meta
            .read_stored(&[(&crate::io::RecordObject::new(), pasted.as_object().unwrap())])
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
        assert_eq!(shared.constant_value(), Some(&Scalar::Variant(resource)));
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
        let named = std::collections::HashMap::from([(0, vec![1, 2]), (1, vec![3, 2]), (3, vec![0])]);
        assert_eq!(dlq_chain(&named, 0), vec![1, 2, 3]);
        assert_eq!(dlq_chain(&named, 2), Vec::<DestId>::new());
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

    /// A certificate, and a key that either issued it or belongs to someone else's.
    fn certificate_and_key(matching: bool) -> (String, String) {
        let issuer_key = rcgen::KeyPair::generate().unwrap();
        let params = rcgen::CertificateParams::new(vec!["smeltr-test".to_string()]).unwrap();
        let certificate = params.self_signed(&issuer_key).unwrap().pem();
        let key = if matching {
            issuer_key
        } else {
            rcgen::KeyPair::generate().unwrap()
        };
        (certificate, key.serialize_pem())
    }

    /// A bundle with a connector that connects under `identity`.
    fn with_identity(identity: Identity) -> TopologyConfig {
        TopologyConfig {
            identities: vec![std::sync::Arc::new(identity)],
            ..Default::default()
        }
    }

    /// The proof `put_identities` ran at save time does not survive a key's name and version being deleted and
    /// recreated under different content -- so it is run again here, against whatever this worker actually holds.
    #[test]
    fn a_key_that_does_not_belong_to_its_certificate_is_refused() {
        let (certificate, key) = certificate_and_key(false);
        let t = with_identity(Identity {
            id: "acme".to_string(),
            certificate: Some(certificate),
            key: Some(stored("acme-key", 1)),
            authorities: None,
            trust_public: true,
        });
        let errors = t.identity_pairing_errors(&held(&[("acme-key", 1, &key)]));
        assert_eq!(errors.len(), 1, "{errors:?}");
        assert!(errors[0].contains("acme"), "{errors:?}");
    }

    #[test]
    fn a_key_that_belongs_to_its_certificate_is_not_refused() {
        let (certificate, key) = certificate_and_key(true);
        let t = with_identity(Identity {
            id: "acme".to_string(),
            certificate: Some(certificate),
            key: Some(stored("acme-key", 1)),
            authorities: None,
            trust_public: true,
        });
        assert!(t.identity_pairing_errors(&held(&[("acme-key", 1, &key)])).is_empty());
    }

    /// A key this worker does not have yet is `missing_secrets`' concern -- there is nothing here to pair against.
    #[test]
    fn an_identity_whose_key_this_worker_lacks_is_not_this_checks_concern() {
        let (certificate, _) = certificate_and_key(true);
        let t = with_identity(Identity {
            id: "acme".to_string(),
            certificate: Some(certificate),
            key: Some(stored("acme-key", 1)),
            authorities: None,
            trust_public: true,
        });
        assert!(t.identity_pairing_errors(&HeldSecrets::default()).is_empty());
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
        h.keep_only(&t.named_secrets);
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
            step_ids: Vec::new(),
            lookup_tables: Default::default(),
            grok_patterns: Default::default(),
        }];
        let metas = SourceMetas::from([(0, SourceMeta::KAFKA), (1, SourceMeta::KAFKA)]);
        let compiled = compile_topology_with(
            &[route(0, None, 7), route(1, None, 7)],
            &pipelines,
            OnException::default(),
            &metas,
        )
        .unwrap();

        assert_eq!(compiled.pipelines.len(), 1, "one plan, not one per source");
        // Both sources still reach it.
        assert!(compiled.plan_for(0, 7).is_some());
        assert!(compiled.plan_for(1, 7).is_some());

        // Declaring different fields is what forces a second plan.
        let mixed = SourceMetas::from([(0, SourceMeta::KAFKA), (1, SourceMeta::METRICS)]);
        let compiled = compile_topology_with(
            &[route(0, None, 7), route(1, None, 7)],
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
            source,
            cond: None,
            pipeline: 0,
            deliver: vec![CompiledDeliverRule {
                when: None,
                to,
                compiled_patterns: Vec::new(),
            }],
            compiled_patterns: Vec::new(),
            on_exception: Default::default(),
        };

        let compiled = CompiledTopology {
            routes: vec![mk(0, vec![2]), mk(1, vec![7])],
            pipelines: std::collections::HashMap::new(),
            metas: SourceMetas::new(),
            on_exception: {
                let mut group = OnException::default();
                for (kind, to) in [(ExceptionKind::Unmatched, 4), (ExceptionKind::Unprocessable, 8)] {
                    group.answers[kind.index()] = Answer::Deliver(vec![to]);
                }
                group
            },
            // Destination 2 salvages its refusals to 5, and 8 to 3. `dlq_of` holds whole chains, so reachability reads
            // one entry per destination.
            dlq_of: std::collections::HashMap::from([(2, vec![5]), (8, vec![3])]),
            records: HashMap::new(),
            payloads: HashMap::new(),
        };

        // Source 0: its own delivery target plus the DLQ that target salvages to -- a source can write to a
        // destination no route names.
        assert_eq!(compiled.destinations_reachable_from(0), vec![2, 3, 4, 5, 8]);
        // Source 1 reaches neither of those.
        assert_eq!(compiled.destinations_reachable_from(1), vec![3, 4, 7, 8]);
        // A source with no routes reaches only where its unmatched and unprocessable rows go, and the chain behind
        // them.
        assert_eq!(compiled.destinations_reachable_from(9), vec![3, 4, 8]);

        // Where the flow goes: the delivery targets alone, no DLQ of any kind.
        assert_eq!(compiled.delivery_destinations_from(0), vec![2]);
        assert_eq!(compiled.delivery_destinations_from(1), vec![7]);
        assert_eq!(compiled.delivery_destinations_from(9), Vec::<DestId>::new());
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
        let slot_offset = RAW_SLOTS + offset_field;

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
        let routes = compile_routes(&[route(0, None, 7)], &SourceMetas::new()).unwrap();
        let mut pool = BufferPool::new();
        let mut errors = ErrorSink::new();
        let mut batch = seed_batch(&mut pool, &["a", "b", "c"]);
        let mut ctx = Ctx::new(&mut pool, &mut errors);

        let masks = route_masks(&routes, 0, &mut batch, &mut ctx, None);
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
        let routes = compile_routes(&[route(1, None, 7)], &SourceMetas::new()).unwrap();
        let mut pool = BufferPool::new();
        let mut errors = ErrorSink::new();
        let mut batch = seed_batch(&mut pool, &["a"]);
        let mut ctx = Ctx::new(&mut pool, &mut errors);

        let masks = route_masks(&routes, 0, &mut batch, &mut ctx, None);
        assert!(masks.is_empty(), "route belongs to source 1, not 0");
        batch.reclaim(&mut pool);
    }

    #[test]
    fn every_route_sees_the_whole_batch() {
        // Route on __source.offset (here just the row index -- see seed_batch) rather than a parsed field, since routes only
        // see ingest fields. Row 0 matches both routes, so it runs both pipelines.
        let routes = compile_routes(
            &[route(0, Some("__source.offset == 0"), 1), route(0, None, 2)],
            &kafka_metas(0),
        )
        .unwrap();
        let mut pool = BufferPool::new();
        let mut errors = ErrorSink::new();
        let mut batch = seed_batch(&mut pool, &["x", "yy"]);
        let mut ctx = Ctx::new(&mut pool, &mut errors);

        let masks = route_masks(&routes, 0, &mut batch, &mut ctx, None);
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
        let err = compile_routes(&[route(0, Some("_raw"), 1)], &SourceMetas::new());
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
                route(0, Some("test(_raw, /^x/)"), 1),
                route(0, None, 2), // catch-all
            ],
            &SourceMetas::new(),
        )
        .unwrap();
        let mut pool = BufferPool::new();
        let mut errors = ErrorSink::new();
        let mut batch = seed_batch(&mut pool, &["x1", "y2"]);
        let mut ctx = Ctx::new(&mut pool, &mut errors);

        let masks = route_masks(&routes, 0, &mut batch, &mut ctx, None);
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
            stall_after: std::time::Duration::from_secs(30),
            brokers: "k:9092".into(),
            default_topic: "t".into(),
            service: crate::plan::topology::KafkaService::Kafka,
            identity: None,
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
