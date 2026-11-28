use crate::core::column::DataType;
use crate::core::id::Id;
use crate::format::encoding::Encoding;

use super::*;

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
    pub(super) kind: &'static str,
    pub(super) record: RecordFields,
    pub(super) source: &'static [(&'static str, DataType)],
    /// How a byte source's payload becomes `_raw`'s text (`format::encoding`).
    encoding: Encoding,
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
pub(super) const TOPIC_FIELDS: [&str; 4] = ["__source.topic", "__source.url", "__source.remote", "__source.database"];

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
    pub const KAFKA: SourceMeta = SourceMeta {
        kind: "kafka",
        record: PAYLOAD_RECORD,
        source: KAFKA_FIELDS,
        encoding: Encoding::Auto,
    };

    /// What an Event Hubs record carries: a Kafka record's fields, with the event hub as its topic.
    pub const EVENT_HUBS: SourceMeta = SourceMeta {
        kind: "eventHubs",
        record: PAYLOAD_RECORD,
        source: KAFKA_FIELDS,
        encoding: Encoding::Auto,
    };

    /// What a metrics record carries: its source's name and kind, and the topic it is stamped with.
    pub const METRICS: SourceMeta = SourceMeta {
        kind: "metrics",
        record: PAYLOAD_RECORD,
        source: METRICS_FIELDS,
        encoding: Encoding::Auto,
    };

    /// What a logs record carries, which is the same.
    pub const LOGS: SourceMeta = SourceMeta {
        kind: "logs",
        record: PAYLOAD_RECORD,
        source: LOGS_FIELDS,
        encoding: Encoding::Auto,
    };

    /// What an SQS/S3 record carries: the object it was read from, and its record number there.
    pub const S3_SQS: SourceMeta = SourceMeta {
        kind: "s3Sqs",
        record: PAYLOAD_RECORD,
        source: OBJECT_FIELDS,
        encoding: Encoding::Auto,
    };

    /// What a Listing/S3 record carries: the same as an SQS/S3 record.
    pub const S3_LIST: SourceMeta = SourceMeta {
        kind: "s3List",
        record: PAYLOAD_RECORD,
        source: OBJECT_FIELDS,
        encoding: Encoding::Auto,
    };

    /// What a Listing/GCS record carries: the same as an SQS/S3 record.
    pub const GCS_LIST: SourceMeta = SourceMeta {
        kind: "gcsList",
        record: PAYLOAD_RECORD,
        source: OBJECT_FIELDS,
        encoding: Encoding::Auto,
    };

    /// What a Listing/Azure Blob record carries: the same, with the container as its bucket.
    pub const AZURE_BLOB_LIST: SourceMeta = SourceMeta {
        kind: "azureBlobList",
        record: PAYLOAD_RECORD,
        source: OBJECT_FIELDS,
        encoding: Encoding::Auto,
    };

    /// What an SFTP record carries: the same, with the directory as its bucket and the path under it as its key.
    pub const SFTP: SourceMeta = SourceMeta {
        kind: "sftp",
        record: PAYLOAD_RECORD,
        source: OBJECT_FIELDS,
        encoding: Encoding::Auto,
    };

    /// What a Queue/Azure Blob record carries: the same, with the container as its bucket.
    pub const AZURE_BLOB_QUEUE: SourceMeta = SourceMeta {
        kind: "azureBlobQueue",
        record: PAYLOAD_RECORD,
        source: OBJECT_FIELDS,
        encoding: Encoding::Auto,
    };

    /// What a Pub/Sub/GCS record carries: the same.
    pub const GCS_PUB_SUB: SourceMeta = SourceMeta {
        kind: "gcsPubSub",
        record: PAYLOAD_RECORD,
        source: OBJECT_FIELDS,
        encoding: Encoding::Auto,
    };

    /// What an HTTP API record carries: the address of the page it was read from.
    pub const HTTP_API: SourceMeta = SourceMeta {
        kind: "httpApi",
        record: PAYLOAD_RECORD,
        source: API_FIELDS,
        encoding: Encoding::Auto,
    };

    /// What an Okta System Log record carries: the same.
    pub const OKTA_LOGS: SourceMeta = SourceMeta {
        kind: "oktaLogs",
        record: PAYLOAD_RECORD,
        source: API_FIELDS,
        encoding: Encoding::Auto,
    };

    /// What a PostgreSQL source's record carries before it declares its fields, which every one does.
    pub const POSTGRES: SourceMeta = SourceMeta {
        kind: "postgres",
        record: PAYLOAD_RECORD,
        source: SQL_FIELDS,
        encoding: Encoding::Auto,
    };

    /// What a MySQL or MariaDB source's record carries before it declares its fields, which every one does.
    pub(crate) const MYSQL: SourceMeta = SourceMeta {
        kind: "mysql",
        record: PAYLOAD_RECORD,
        source: SQL_FIELDS,
        encoding: Encoding::Auto,
    };

    /// What an HTTP receiver's record carries: the address of the sender that delivered it.
    pub const HTTP_RECEIVER: SourceMeta = SourceMeta {
        kind: "httpReceiver",
        record: PAYLOAD_RECORD,
        source: RECEIVER_FIELDS,
        encoding: Encoding::Auto,
    };

    /// What a HEC receiver's record carries: its sender's address, and what the sender filed its event under.
    pub const HEC_RECEIVER: SourceMeta = SourceMeta {
        kind: "hecReceiver",
        record: PAYLOAD_RECORD,
        source: HEC_RECEIVER_FIELDS,
        encoding: Encoding::Auto,
    };

    /// What an S2S receiver's record carries: its forwarder's address, and what the forwarder filed its event under.
    pub const S2S_RECEIVER: SourceMeta = SourceMeta {
        kind: "s2sReceiver",
        record: PAYLOAD_RECORD,
        source: HEC_RECEIVER_FIELDS,
        encoding: Encoding::Auto,
    };

    /// What an Elasticsearch receiver's record carries: a document, its sender's address, and what its action named.
    pub const ELASTIC_RECEIVER: SourceMeta = SourceMeta {
        kind: "elasticReceiver",
        record: PAYLOAD_RECORD,
        source: ELASTIC_RECEIVER_FIELDS,
        encoding: Encoding::Auto,
    };

    /// What a socket receiver carries: a message's bytes, and its sender's address.
    pub const SOCKET_RECEIVER: SourceMeta = SourceMeta {
        kind: "socketReceiver",
        record: PAYLOAD_RECORD,
        source: RECEIVER_FIELDS,
        encoding: Encoding::Auto,
    };

    /// What an OTLP receiver of logs carries: a log record, its sender's address, and its resource and scope.
    pub const OTLP_LOGS: SourceMeta = SourceMeta {
        kind: "otlpReceiver",
        record: OTLP_LOG,
        source: OTLP_RECEIVER_FIELDS,
        encoding: Encoding::Auto,
    };

    /// What an OTLP receiver of traces carries: a span, its sender's address, and its resource and scope.
    pub const OTLP_TRACES: SourceMeta = SourceMeta {
        kind: "otlpReceiver",
        record: OTLP_SPAN,
        source: OTLP_RECEIVER_FIELDS,
        encoding: Encoding::Auto,
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
                encoding: Encoding::Auto,
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
            encoding: Encoding::Auto,
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
            SourceMeta::SOCKET_RECEIVER,
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
        encoding: Encoding::Auto,
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
    pub(crate) fn declaring_open(self, record: Vec<(String, DataType)>) -> Result<SourceMeta, String> {
        Ok(self.declare(record, |_| false)?.opened())
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
    pub(super) fn declare(
        self,
        record: Vec<(String, DataType)>,
        internal: fn(&str) -> bool,
    ) -> Result<SourceMeta, String> {
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
            _ if !self.reads_bytes() => self,
            Encoding::None => SourceMeta {
                record: std::borrow::Cow::Borrowed(&[]),
                encoding,
                ..self
            },
            _ => SourceMeta { encoding, ..self },
        }
    }

    /// How this source's payload becomes `_raw`'s text.
    pub(crate) fn encoding(&self) -> Encoding {
        self.encoding
    }

    /// Whether this source's record is its payload decoded, `_raw`: it reads bytes and decodes them.
    pub(crate) fn decodes(&self) -> bool {
        self.reads_bytes() && !self.record.is_empty()
    }

    /// Whether this source hands over each record's payload as it read it, as `__source.bytes`. Its chunk is then
    /// one `bytes` column, the payloads, and its record is `_raw`, each payload decoded with the source's encoding.
    pub(super) fn reads_bytes(&self) -> bool {
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
    pub(super) fn source_field_at(&self, slot: crate::core::id::SlotId) -> Option<usize> {
        slot.index()
            .checked_sub(self.source_slot(0).index())
            .filter(|i| *i < self.source.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::heartbeat::pasted_from;

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
            meta.record_object(&read[0], ""),
            stored,
            "each leaf and each other key reads back"
        );
        let null = object_of(serde_json::json!({"destination": null, "labels": null}));
        let read = meta.read_table(&pasted_from(&[(&null, &fields)])).unwrap();
        assert!(
            meta.record_object(&read[0], "").is_empty(),
            "a null object holds no field"
        );
        let extra = object_of(serde_json::json!({"destination": {"ip": "8.8.8.8", "mac": "aa"}}));
        let read = meta.read_table(&pasted_from(&[(&extra, &fields)])).unwrap();
        assert_eq!(
            meta.record_object(&read[0], ""),
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
}
