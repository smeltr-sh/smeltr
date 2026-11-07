//! The JSON shape: inert data with serde derives. `deny_unknown_fields` throughout, so a field this loader does not
//! know fails loudly rather than being dropped on the next save. Structs with a `#[serde(flatten)]` field cannot carry
//! it (serde breaks on the combination); their flattened enum still has it.

pub use super::load::destination::{
    AzureBlobDestinationDto, DeltaDestinationDto, ElasticsearchDestinationDto, EventHubsDestinationDto,
    GcsDestinationDto, HttpDestinationDto, IcebergDestinationDto, KafkaDestinationDto, LogAnalyticsDestinationDto,
    OtlpDestinationDto, PrometheusDestinationDto, S3DestinationDto, SftpDestinationDto, SplunkHecDestinationDto,
};
pub use super::load::source::{
    AzureBlobListSourceDto, AzureBlobQueueSourceDto, ElasticReceiverSourceDto, EventHubsSourceDto, GcsListSourceDto,
    GcsPubSubSourceDto, HecReceiverSourceDto, HttpApiSourceDto, HttpReceiverSourceDto, KafkaSourceDto, LogsSourceDto,
    MetricsSourceDto, OktaLogsSourceDto, OtlpReceiverSourceDto, PrometheusReceiverSourceDto, S2sReceiverSourceDto,
    S3ListSourceDto, S3SqsSourceDto, SftpSourceDto, SocketReceiverSourceDto, SyslogReceiverSourceDto,
};
use std::collections::BTreeMap;

use crate::plan::topology::Signal;
use serde::{Deserialize, Serialize};

/// A credential a bundle names, never carries. A bare string names an environment variable on the worker's own host;
/// an object names a value the leader holds, at the version the deployment is built for.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum SecretRefDto {
    WorkerLocal(String),
    LeaderStored(LeaderSecretDto),
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct LeaderSecretDto {
    pub secret: String,
    pub version: i64,
}

impl SecretRefDto {
    /// The variable or leader secret this names. Blank names nothing a worker could look up.
    pub fn name(&self) -> &str {
        match self {
            SecretRefDto::WorkerLocal(var) => var,
            SecretRefDto::LeaderStored(l) => &l.secret,
        }
    }

    /// Moves this reference onto `version` when it names `name`. `true` when it changed.
    ///
    /// A worker-local reference is never moved: it names a variable on a host, and a version means nothing there.
    pub fn point_at(&mut self, name: &str, version: i64) -> bool {
        match self {
            SecretRefDto::LeaderStored(l) if l.secret == name && l.version != version => {
                l.version = version;
                true
            }
            _ => false,
        }
    }
}

/// The TLS a source or destination connects out under, as its own fields: the certificate it presents, an entry of the
/// leader's certificate list by name, and the authorities it verifies the far end by. A worker presents the entry's
/// newest version.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct TlsDto {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub certificate: Option<String>,
    /// PEM. A trust store holds more than one root, so several may follow one another.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trusted_authorities: Option<String>,
    /// Absent is true: a client also trusts the public roots.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trust_public: Option<bool>,
}

impl TlsDto {
    /// Whether the connector says anything about its TLS.
    pub fn is_set(&self) -> bool {
        self.certificate.is_some() || self.trusted_authorities.is_some() || self.trust_public.is_some()
    }
}

/// The TLS a receiver serves: the certificate it serves to every sender, and whom it admits by theirs. It serves TLS
/// when it names a certificate. Naming authorities or sender fingerprints, it admits only a sender whose certificate
/// is one of the fingerprints', or chains to one of the authorities and, when it names sender names, is issued to one
/// of them (`io::senders`). A receiver trusts no public root, so it has no say over them.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ServedTlsDto {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub certificate: Option<String>,
    /// PEM, one or more certificates.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trusted_authorities: Option<String>,
    /// As RFC 5425 (4.2.1) writes a fingerprint: `sha-256:AB:CD:…`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sender_fingerprints: Vec<String>,
    /// Host names a certificate chaining to the authorities must be issued to, `*.` standing for any one label.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sender_names: Vec<String>,
}

impl ServedTlsDto {
    /// Whether the receiver says anything about its TLS.
    pub fn is_set(&self) -> bool {
        self.certificate.is_some()
            || self.trusted_authorities.is_some()
            || !self.sender_fingerprints.is_empty()
            || !self.sender_names.is_empty()
    }

    /// The same settings as any connector's.
    pub fn tls(&self) -> TlsDto {
        TlsDto {
            certificate: self.certificate.clone(),
            trusted_authorities: self.trusted_authorities.clone(),
            trust_public: None,
        }
    }
}

/// Extra librdkafka properties, split by whether the value is a credential. A bundle is committed and read in review,
/// so `properties` holds values that belong in a repository and `credentials` names where each secret one lives.
/// `config_file::load` refuses properties smeltr sets itself and credentials in the literal map.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct KafkaPropsDto {
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub properties: BTreeMap<String, String>,
    /// Property name -> the credential holding its value. One a worker cannot obtain is a deploy that is refused,
    /// naming it.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub credentials: BTreeMap<String, SecretRefDto>,
}

impl KafkaPropsDto {
    pub fn is_empty(&self) -> bool {
        self.properties.is_empty() && self.credentials.is_empty()
    }

    /// Every credential these properties name, by the field naming them.
    pub fn secrets(&self) -> Vec<(&'static str, &SecretRefDto)> {
        self.credentials.values().map(|r| ("credentials", r)).collect()
    }

    /// `secrets`, for the leader to move one onto a new version.
    pub fn secrets_mut(&mut self) -> Vec<&mut SecretRefDto> {
        self.credentials.values_mut().collect()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileConfig {
    pub version: u32,
    #[serde(default)]
    pub sources: Vec<SourceDto>,
    #[serde(default)]
    pub destinations: Vec<DestinationDto>,
    #[serde(default)]
    pub pipelines: Vec<PipelineDto>,
    #[serde(default)]
    pub routes: Vec<RouteDto>,
    #[serde(default, rename = "lookupTables")]
    pub lookup_tables: Vec<LookupTableDto>,
    #[serde(default, rename = "grokPatterns")]
    pub grok_patterns: Vec<GrokPatternDto>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub schemas: Vec<SchemaDto>,
    /// The group's answer to every exception kind. Absent means encoding is ignored, delivery blocks, and every other
    /// category is tagged.
    #[serde(default, rename = "exceptions", skip_serializing_if = "Option::is_none")]
    pub on_exception: Option<OnExceptionDto>,
    /// A group's display name. Absent means show the id instead -- a rendering choice, not a default made up here.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

/// `plan::config::Answer`: `"ignore"`, `"tag"`, `"drop"`, `"block"`, or
/// `{"deliver": ["<destination>", ...]}`. Written by hand rather than derived: a
/// derived shape would nest the object one level deeper, and an untagged one answers a typo with "did not match any
/// variant".
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AnswerDto {
    Ignore,
    Tag,
    Drop,
    /// One or more destination ids.
    Deliver {
        to: Vec<String>,
    },
    Block,
}

const ANSWER_LABELS: [&str; 4] = ["ignore", "tag", "drop", "block"];

impl Serialize for AnswerDto {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        match self {
            AnswerDto::Ignore => s.serialize_str("ignore"),
            AnswerDto::Tag => s.serialize_str("tag"),
            AnswerDto::Drop => s.serialize_str("drop"),
            AnswerDto::Block => s.serialize_str("block"),
            AnswerDto::Deliver { to } => {
                let mut m = s.serialize_map(Some(1))?;
                m.serialize_entry("deliver", to)?;
                m.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for AnswerDto {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct Shape;
        impl<'de> serde::de::Visitor<'de> for Shape {
            type Value = AnswerDto;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str(r#""ignore", "tag", "drop", "block", or {"deliver": [destination ids]}"#)
            }

            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<AnswerDto, E> {
                match v {
                    "ignore" => Ok(AnswerDto::Ignore),
                    "tag" => Ok(AnswerDto::Tag),
                    "drop" => Ok(AnswerDto::Drop),
                    "block" => Ok(AnswerDto::Block),
                    other => Err(E::unknown_variant(other, &ANSWER_LABELS)),
                }
            }

            fn visit_map<A: serde::de::MapAccess<'de>>(self, mut map: A) -> Result<AnswerDto, A::Error> {
                use serde::de::Error;
                let mut to = None;
                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "deliver" if to.is_none() => to = Some(map.next_value::<Vec<String>>()?),
                        "deliver" => return Err(A::Error::custom(format!("duplicate field `{key}`"))),
                        other => return Err(A::Error::unknown_field(other, &["deliver"])),
                    }
                }
                let to = to.ok_or_else(|| A::Error::missing_field("deliver"))?;
                Ok(AnswerDto::Deliver { to })
            }
        }
        d.deserialize_any(Shape)
    }
}

// -- sources / destinations --------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SourceDto {
    pub id: String,
    #[serde(flatten)]
    pub kind: SourceKindDto,
}

/// The keys of a source or destination that none of its fields name, refused as the bundle is read.
/// `deny_unknown_fields` cannot refuse them on a type that flattens another into it, so this takes what the
/// flattening leaves over. Each struct holds it as its last flattened field: serde fills flattened fields in order,
/// and a flattened field after it would find its keys already taken. A flattened block must not flatten another:
/// serde reads a flattened block inside a flattened block without marking its keys taken, so `NoUnknownKeys` would
/// refuse them as unknown.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct NoUnknownKeys;

impl<'de> Deserialize<'de> for NoUnknownKeys {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let keys = BTreeMap::<String, serde::de::IgnoredAny>::deserialize(deserializer)?;
        match keys.keys().next() {
            None => Ok(NoUnknownKeys),
            Some(key) => Err(serde::de::Error::custom(format!("unknown field `{key}`"))),
        }
    }
}

impl Serialize for NoUnknownKeys {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serde::ser::SerializeMap::end(serializer.serialize_map(Some(0))?)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase", deny_unknown_fields)]
pub enum SourceKindDto {
    Kafka(KafkaSourceDto),
    /// Azure Event Hubs, read through its Kafka endpoint: loading makes it a Kafka source signed in as `sign_in`
    /// says.
    EventHubs(EventHubsSourceDto),
    /// The smeltr metrics source: this process's own observability counters (`metrics::MetricsSource`).
    Metrics(MetricsSourceDto),
    /// The smeltr logs source: this process's own logs (`io::logs::LogsSource`).
    Logs(LogsSourceDto),
    /// The objects S3 announces on an SQS queue, from whichever bucket a notification names.
    S3Sqs(S3SqsSourceDto),
    /// The objects under a prefix of one S3 bucket, found by listing it every `pollIntervalSecs`. One worker reads
    /// it.
    ///
    /// Use `s3Sqs` when the bucket can notify. Otherwise delete or move each object after commit, so the bucket is
    /// its own queue. Keep with a ledger is for a bucket the reader may not change, and costs a full listing per
    /// poll.
    S3List(S3ListSourceDto),
    /// The objects under a prefix of one Cloud Storage bucket, found by listing it, as `s3List` reads S3. Use
    /// `gcsPubSub` when the bucket can notify.
    GcsList(GcsListSourceDto),
    /// The blobs under a prefix of one Azure container, found by listing it, as `s3List` reads S3. A blob that
    /// changes after it is read is read again whole. Use `azureBlobQueue` when the account can notify.
    AzureBlobList(AzureBlobListSourceDto),
    /// The files under a prefix of one directory on an SFTP server, found by listing it, as `s3List` reads S3. A file
    /// that changes after it is read is read again whole.
    Sftp(SftpSourceDto),
    /// The blobs Event Grid announces on an Azure Storage queue, from whichever container of the account a
    /// notification names.
    AzureBlobQueue(AzureBlobQueueSourceDto),
    /// The objects Cloud Storage announces on a Pub/Sub subscription, from whichever bucket a notification names.
    GcsPubSub(GcsPubSubSourceDto),
    /// What one address answers a GET, or a POST of a fixed body, with, asked every `pollIntervalSecs`. One worker
    /// reads it.
    ///
    /// For an API that answers with JSON or lines of text. An API with its own kind is read through that kind.
    HttpApi(HttpApiSourceDto),
    /// An Okta org's System Log, every event once, in the order Okta stored them. One worker reads it.
    OktaLogs(OktaLogsSourceDto),
    /// The rows a query returns from a PostgreSQL database, each a record whose fields are the result's columns. One
    /// worker reads it, and the leader keeps how far it has read.
    #[serde(deserialize_with = "SqlSourceDto::postgres")]
    Postgres(SqlSourceDto),
    /// The rows a query returns from a MySQL or MariaDB database, read as a `postgres` source reads its own.
    #[serde(rename = "mysql", deserialize_with = "SqlSourceDto::mysql")]
    MySql(SqlSourceDto),
    /// What senders POST to `path` on `port`. Every worker of the group listens; a request is answered as
    /// `answerAfterDelivery` says.
    HttpReceiver(HttpReceiverSourceDto),
    /// What Splunk HTTP Event Collector clients send to `port`, at the paths Splunk serves. Every worker of the group
    /// listens; a request is answered as `answerAfterDelivery` says.
    HecReceiver(HecReceiverSourceDto),
    /// What Splunk forwarders send to `port` over Splunk-to-Splunk, as to an indexer. Every worker of the group
    /// listens, and each connection is one forwarder's. `maxRequestBytes` bounds one frame and one record.
    S2sReceiver(S2sReceiverSourceDto),

    /// What OpenTelemetry exporters send to `port` over OTLP, over HTTP as protobuf or JSON and over gRPC: the
    /// exports of one `signal`, one record per log record, data point or span. The receivers of a group's other
    /// signals may share its port, as an exporter sends every signal to one. Every worker of the group listens; an
    /// export is answered as `answerAfterDelivery` says.
    OtlpReceiver(OtlpReceiverSourceDto),
    /// What Prometheus and the agents speaking its remote_write protocol send to `port`, at `/api/v1/write`, one
    /// record per sample. Every worker of the group listens; a request is answered as
    /// `answerAfterDelivery` says.
    PrometheusReceiver(PrometheusReceiverSourceDto),
    /// What Elasticsearch clients and shippers send to `port` through the `_bulk` API, at `/_bulk` and
    /// `/{index}/_bulk`, one record per document. Every worker of the group listens; a request is answered as
    /// `answerAfterDelivery` says.
    ElasticReceiver(ElasticReceiverSourceDto),
    /// What syslog senders send to `port`, as RFC 5424 or RFC 3164 messages: over TCP framed by octet count or ended
    /// by a newline (RFC 6587), under TLS (RFC 5425) when it names a certificate; over UDP one to a datagram (RFC 5426).
    /// Every worker of the group listens, and each message is one record. Syslog has no acknowledgement, so delivery
    /// is best-effort. A full receiver stops reading TCP, so the sender's window fills, and drops UDP datagrams.
    SyslogReceiver(SyslogReceiverSourceDto),
    /// What senders send to `port` in a protocol smeltr has no receiver for: over TCP cut into records by its
    /// framing, under TLS when it names a certificate; over UDP a datagram to a record. Every worker of the group
    /// listens. Nothing is acknowledged, so delivery is best-effort. A full receiver stops reading TCP, so the
    /// sender's window fills, and drops UDP datagrams.
    SocketReceiver(SocketReceiverSourceDto),
}

/// What every receiver takes: where it listens and how large a request it reads. Flattened into each receiver's own
/// fields, beside the TLS it serves.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ReceiverSettingsDto {
    /// The port it listens on, at the address its worker's startup settings name.
    pub port: u16,
    #[serde(default, skip_serializing_if = "MaxRequestBytes::is_default")]
    pub max_request_bytes: MaxRequestBytes,
}

/// How many connections a receiver holds open over TCP, and how long it keeps an idle one. Flattened into every
/// receiver's fields.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ConnectionLimitsDto {
    /// The most connections it holds at once, on each worker. Absent, as many as the worker's budget for every
    /// receiver's connections.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_connections: Option<u64>,
    /// The most it holds from one address. Absent, as many as `maxConnections`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_connections_per_sender: Option<u64>,
    /// How long a connection the receiver waits on may send nothing before it is closed. Absent, an idle one is held
    /// while there is room.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idle_timeout_secs: Option<u64>,
}

/// A token a HEC receiver admits.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HecTokenDto {
    /// The credential holding the token.
    pub token: SecretRefDto,
    /// Whether its senders use indexer acknowledgement, as Splunk's `useACK`: each request names a channel and is
    /// answered at once with an `ackId`. Absent, a request is answered as the receiver's `answerAfterDelivery` says. Two names holding the same token
    /// value acknowledge if either does, since only the value is presented.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub acknowledgement: bool,
}

/// How an S2S receiver breaks one sourcetype's stream into events, and a HEC receiver one sourcetype's raw body.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SourcetypeDto {
    /// A pattern whose first capture group is where an event ends, the group dropped, as Splunk's `LINE_BREAKER`.
    /// Absent, or `PER_LINE`, an event per line.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub line_breaker: Option<String>,
}

/// `$`. serde's default is a function, and Rust cannot name one `$`.
fn root() -> String {
    crate::plan::tree::ROOT.to_string()
}

/// The largest request body a receiver reads.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
pub struct MaxRequestBytes(pub u64);

impl MaxRequestBytes {
    pub const DEFAULT: u64 = 10 * 1024 * 1024;
    /// RFC 5425 (4.3.1) has a syslog receiver take messages of 2048 octets.
    pub const MIN: u64 = 2048;
    /// What one request may hold of a receiver's inbox (`io::receiver::INBOX_BYTES`).
    pub const MAX: u64 = 64 * 1024 * 1024;

    pub fn is_default(&self) -> bool {
        self.0 == Self::DEFAULT
    }
}

impl Default for MaxRequestBytes {
    fn default() -> Self {
        MaxRequestBytes(Self::DEFAULT)
    }
}

/// How an API source asks.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
pub enum ApiMethodDto {
    #[default]
    Get,
    Post,
}

impl ApiMethodDto {
    pub fn is_default(&self) -> bool {
        *self == ApiMethodDto::Get
    }
}

/// Where an API's answer holds its records.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum ApiRecordsDto {
    /// The whole answer is one record.
    #[default]
    Body,
    /// One record per line.
    Lines,
    /// One record per element of a JSON array.
    Array,
}

impl ApiRecordsDto {
    pub fn is_default(&self) -> bool {
        *self == ApiRecordsDto::Body
    }
}

impl SourceKindDto {
    /// Whether this is an SQL source that declares no fields yet. Run reads it all the same: the worker declares
    /// every column the query returns, so its record is known only once the worker has described the query.
    pub fn undeclared(&self) -> bool {
        self.sql().is_some_and(|(_, sql)| sql.fields.is_empty())
    }

    /// Whether a capture of this source reads it itself even where a deployment reads it. An SQL source's capture is
    /// Run, which reads the query as its author has it now, and reports what the deployment's rows cannot: its
    /// columns and plan.
    pub fn runs_its_own_query(&self) -> bool {
        self.sql().is_some()
    }

    /// An SQL source's settings, with which SQL source it is. `None` for a kind that is not one.
    pub fn sql(&self) -> Option<(SqlKind, &SqlSourceDto)> {
        match self {
            SourceKindDto::Postgres(s) => Some((SqlKind::Postgres, s)),
            SourceKindDto::MySql(s) => Some((SqlKind::MySql, s)),
            _ => None,
        }
    }

    /// `sql`, to change.
    pub fn sql_mut(&mut self) -> Option<(SqlKind, &mut SqlSourceDto)> {
        match self {
            SourceKindDto::Postgres(s) => Some((SqlKind::Postgres, s)),
            SourceKindDto::MySql(s) => Some((SqlKind::MySql, s)),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum StartPositionDto {
    #[default]
    Earliest,
    Latest,
    /// Milliseconds since the Unix epoch.
    Timestamp {
        millis: i64,
    },
}

impl StartPositionDto {
    pub fn is_default(&self) -> bool {
        *self == StartPositionDto::Earliest
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum StopPositionDto {
    /// Milliseconds since the Unix epoch, exclusive.
    Timestamp { millis: i64 },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DestinationDto {
    pub id: String,
    /// The destination's answer to `Refused`, over the group's. It names no other kind.
    #[serde(default, rename = "exceptions", skip_serializing_if = "Option::is_none")]
    pub on_exception: Option<OnExceptionDto>,
    /// The schemas, by id, a pipeline that sends here produces one of. Empty accepts any.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub accepts: Vec<String>,
    #[serde(flatten)]
    pub kind: DestinationKindDto,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase", deny_unknown_fields)]
pub enum DestinationKindDto {
    Kafka(KafkaDestinationDto),
    /// Azure Event Hubs, written through its Kafka endpoint: loading makes it a Kafka destination signed in as
    /// `sign_in` says.
    EventHubs(EventHubsDestinationDto),
    Elasticsearch(ElasticsearchDestinationDto),
    /// Events sent to a Splunk HTTP Event Collector.
    SplunkHec(SplunkHecDestinationDto),
    /// Objects written to an S3 bucket, one open file per prefix.
    S3(S3DestinationDto),
    /// Objects written to a Google Cloud Storage bucket, one open file per prefix.
    Gcs(GcsDestinationDto),
    /// Blobs written to an Azure Blob Storage container, one open file per prefix.
    AzureBlob(AzureBlobDestinationDto),
    /// Files written to one directory of an SFTP server, one open file per prefix, each renamed into place whole.
    Sftp(SftpDestinationDto),
    /// Rows POSTed to one URL, as lines, a JSON array or one per request.
    Http(HttpDestinationDto),
    /// Samples sent as Prometheus remote_write to a store's endpoint.
    Prometheus(PrometheusDestinationDto),
    /// Rows sent as OTLP/HTTP exports of one signal to an OpenTelemetry endpoint.
    Otlp(OtlpDestinationDto),
    /// Rows sent to a stream of a data collection rule through Azure Monitor's Logs Ingestion API, which lands
    /// them in the rule's Log Analytics table, where Microsoft Sentinel reads them.
    LogAnalytics(LogAnalyticsDestinationDto),
    /// Rows written as Parquet data files into an Iceberg table, committed through a REST catalog.
    Iceberg(IcebergDestinationDto),
    /// Rows written as Parquet data files into a Delta table at a path in S3, committed through its `_delta_log`.
    Delta(DeltaDestinationDto),
}

impl SourceKindDto {
    /// Whether this is a Kafka source whose `fetchBytes` the bundle leaves out, which the worker sizes to its memory.
    pub fn leaves_fetch(&self) -> bool {
        matches!(
            self,
            SourceKindDto::Kafka(KafkaSourceDto { fetch_bytes: None, .. })
                | SourceKindDto::EventHubs(EventHubsSourceDto { fetch_bytes: None, .. })
        )
    }
}

/// A Databricks Unity Catalog external table, signed in to as a service principal with OAuth client credentials.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct UnityCatalogDto {
    /// The workspace's address, as `https://dbc-1234.cloud.databricks.com`.
    pub workspace: String,
    /// The table's three-part name, `catalog.schema.table`.
    pub table: String,
    /// The service principal's OAuth client id.
    pub client_id: String,
    pub client_secret: SecretRefDto,
}

/// An app signing in with OAuth 2.0 client credentials, for a bearer token sent on every request. Flattened into the
/// HTTP API source. `clientId` with one of `clientSecret` and `privateKey`, and one of `tokenUrl` and `issuer`; none of
/// its fields set signs nothing in.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct AppSignInDto {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
    /// The credential holding the app's client secret.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_secret: Option<SecretRefDto>,
    /// The credential holding the app's private key, an RSA key as PEM PKCS#8, that signs its assertion.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub private_key: Option<SecretRefDto>,
    /// Which of the app's keys `privateKey` is, for an app with several.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_id: Option<String>,
    /// The token endpoint.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_url: Option<String>,
    /// An OpenID provider, whose discovery document names the token endpoint.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub issuer: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
}

impl AppSignInDto {
    /// Whether it names nothing.
    pub fn is_empty(&self) -> bool {
        let AppSignInDto {
            client_id,
            client_secret,
            private_key,
            key_id,
            token_url,
            issuer,
            scope,
        } = self;
        [client_id, key_id, token_url, issuer, scope]
            .iter()
            .all(|v| v.is_none())
            && client_secret.is_none()
            && private_key.is_none()
    }

    pub fn secrets(&self) -> Vec<(&'static str, &SecretRefDto)> {
        let secret = self.client_secret.iter().map(|r| ("clientSecret", r));
        secret
            .chain(self.private_key.iter().map(|r| ("privateKey", r)))
            .collect()
    }

    /// `secrets`, for the leader to move one onto a new version.
    pub fn secrets_mut(&mut self) -> Vec<&mut SecretRefDto> {
        [&mut self.client_secret, &mut self.private_key]
            .into_iter()
            .flatten()
            .collect()
    }

    /// Where it sends a credential, by the field naming each.
    pub fn urls(&self) -> Vec<(&'static str, &str)> {
        let token_url = self.token_url.as_deref().map(|u| ("tokenUrl", u));
        token_url
            .into_iter()
            .chain(self.issuer.as_deref().map(|u| ("issuer", u)))
            .collect()
    }
}

/// Where AWS is reached and how its requests are signed. Flattened into the S3 destination, the SQS/S3 source and
/// the Listing/S3 source.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct S3AccessDto {
    /// Absent takes the host's.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    /// An S3-compatible service's address. Absent is AWS.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
    /// Where credentials are obtained: STS, for assuming a role. Absent is the region's own.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth_endpoint: Option<String>,
    /// Both or neither. Neither takes the host's credentials.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub access_key_id: Option<SecretRefDto>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret_access_key: Option<SecretRefDto>,
    /// A role to assume, for a bucket in another account.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role_arn: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub external_id: Option<String>,
}

impl S3AccessDto {
    /// Whether it names nothing.
    pub fn is_empty(&self) -> bool {
        let S3AccessDto {
            region,
            endpoint,
            auth_endpoint,
            access_key_id,
            secret_access_key,
            role_arn,
            external_id,
        } = self;
        [region, endpoint, auth_endpoint, role_arn, external_id]
            .iter()
            .all(|v| v.is_none())
            && access_key_id.is_none()
            && secret_access_key.is_none()
    }

    pub fn secrets(&self) -> Vec<(&'static str, &SecretRefDto)> {
        let id = self.access_key_id.iter().map(|r| ("accessKeyId", r));
        id.chain(self.secret_access_key.iter().map(|r| ("secretAccessKey", r)))
            .collect()
    }

    /// `secrets`, for the leader to move one onto a new version.
    pub fn secrets_mut(&mut self) -> Vec<&mut SecretRefDto> {
        [&mut self.access_key_id, &mut self.secret_access_key]
            .into_iter()
            .flatten()
            .collect()
    }

    /// The storage service, and where credentials are obtained when that is configured apart, by the field naming
    /// each.
    pub fn urls(&self) -> Vec<(&'static str, &str)> {
        std::iter::once(("endpoint", self.endpoint.as_deref().unwrap_or(S3_ENDPOINT)))
            .chain(self.auth_endpoint.as_deref().map(|u| ("authEndpoint", u)))
            .collect()
    }
}

/// Where Google Cloud is reached and how its requests are signed. Flattened into the GCS destination and the
/// Pub/Sub/GCS source.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GcsAccessDto {
    /// Another Cloud Storage address, such as an emulator's. Absent is Google's.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
    /// A service account's JSON key. Absent takes the host's Application Default Credentials.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_account_key: Option<SecretRefDto>,
}

impl GcsAccessDto {
    pub fn secrets(&self) -> Vec<(&'static str, &SecretRefDto)> {
        self.service_account_key
            .iter()
            .map(|r| ("serviceAccountKey", r))
            .collect()
    }

    /// `secrets`, for the leader to move one onto a new version.
    pub fn secrets_mut(&mut self) -> Vec<&mut SecretRefDto> {
        self.service_account_key.iter_mut().collect()
    }

    /// The storage service, by the field naming it.
    pub fn urls(&self) -> Vec<(&'static str, &str)> {
        vec![("endpoint", self.endpoint.as_deref().unwrap_or(GCS_ENDPOINT))]
    }
}

/// Where an Azure storage account is reached and how its requests are signed. Flattened into the Azure Blob
/// destination and the Queue/Azure Blob source.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct AzureAccessDto {
    pub account: String,
    /// Another address, such as Azurite's or a sovereign cloud's. Absent is the account's in Azure's public cloud.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
    /// Where a client secret is exchanged for a token. Absent is the one in the cloud the account's `endpoint` is in.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth_endpoint: Option<String>,
    /// At most one of `accessKey`, `sasToken`, `clientSecret` and `managedIdentityClientId`. None takes the host's
    /// managed or workload identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub access_key: Option<SecretRefDto>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sas_token: Option<SecretRefDto>,
    /// A service principal: all three or none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_secret: Option<SecretRefDto>,
    /// Which of the host's managed identities signs in: a user-assigned one's client id. Absent is the host's own.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub managed_identity_client_id: Option<String>,
}

impl AzureAccessDto {
    /// Its Entra sign-in, whatever its access key or SAS token says.
    pub fn entra(&self) -> EntraSignInDto {
        EntraSignInDto {
            tenant_id: self.tenant_id.clone(),
            client_id: self.client_id.clone(),
            client_secret: self.client_secret.clone(),
            auth_endpoint: self.auth_endpoint.clone(),
            managed_identity_client_id: self.managed_identity_client_id.clone(),
        }
    }

    pub fn secrets(&self) -> Vec<(&'static str, &SecretRefDto)> {
        [
            ("accessKey", &self.access_key),
            ("sasToken", &self.sas_token),
            ("clientSecret", &self.client_secret),
        ]
        .into_iter()
        .filter_map(|(field, r)| Some((field, r.as_ref()?)))
        .collect()
    }

    /// `secrets`, for the leader to move one onto a new version.
    pub fn secrets_mut(&mut self) -> Vec<&mut SecretRefDto> {
        [&mut self.access_key, &mut self.sas_token, &mut self.client_secret]
            .into_iter()
            .flatten()
            .collect()
    }

    /// The storage service, and where credentials are obtained when that is configured apart, by the field naming
    /// each.
    pub fn urls(&self) -> Vec<(&'static str, &str)> {
        std::iter::once(("endpoint", self.endpoint.as_deref().unwrap_or(AZURE_ENDPOINT)))
            .chain(self.auth_endpoint.as_deref().map(|u| ("authEndpoint", u)))
            .collect()
    }
}

/// How a client signs in to an Azure service with Entra ID. Flattened into each kind that signs in so.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct EntraSignInDto {
    /// A service principal: all three or none. At most one of it and `managedIdentityClientId`; neither signs in as
    /// the host's managed identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_secret: Option<SecretRefDto>,
    /// Where a client secret is exchanged for a token. Absent is the one in the cloud the service is in.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth_endpoint: Option<String>,
    /// Which of the host's managed identities signs in: a user-assigned one's client id. Absent is the host's own.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub managed_identity_client_id: Option<String>,
}

impl EntraSignInDto {
    pub fn secrets(&self) -> Vec<(&'static str, &SecretRefDto)> {
        self.client_secret.iter().map(|r| ("clientSecret", r)).collect()
    }

    /// `secrets`, for the leader to move one onto a new version.
    pub fn secrets_mut(&mut self) -> Vec<&mut SecretRefDto> {
        self.client_secret.iter_mut().collect()
    }

    /// Where a token is obtained when that is configured apart, by the field naming it.
    pub fn urls(&self) -> Vec<(&'static str, &str)> {
        self.auth_endpoint
            .as_deref()
            .map(|u| ("authEndpoint", u))
            .into_iter()
            .collect()
    }
}

/// Where an SFTP server is reached, which server it must be, and how this side signs in. Flattened into the SFTP
/// source and destination.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SftpAccessDto {
    pub host: String,
    #[serde(
        default = "SftpAccessDto::default_port",
        skip_serializing_if = "SftpAccessDto::is_default_port"
    )]
    /// Wider than a port, so a number out of range reaches load, which says which field it is.
    pub port: u32,
    pub username: String,
    /// Exactly one of `password` and `privateKey`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub password: Option<SecretRefDto>,
    /// An OpenSSH private key, unencrypted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub private_key: Option<SecretRefDto>,
    /// The server's public key as OpenSSH writes it, `ssh-ed25519 AAAA…`: the one server this connects to.
    pub host_key: String,
}

impl SftpAccessDto {
    pub const DEFAULT_PORT: u32 = 22;

    fn default_port() -> u32 {
        Self::DEFAULT_PORT
    }

    fn is_default_port(port: &u32) -> bool {
        *port == Self::DEFAULT_PORT
    }

    pub fn secrets(&self) -> Vec<(&'static str, &SecretRefDto)> {
        [("password", &self.password), ("privateKey", &self.private_key)]
            .into_iter()
            .filter_map(|(field, r)| Some((field, r.as_ref()?)))
            .collect()
    }

    /// `secrets`, for the leader to move one onto a new version.
    pub fn secrets_mut(&mut self) -> Vec<&mut SecretRefDto> {
        [&mut self.password, &mut self.private_key]
            .into_iter()
            .flatten()
            .collect()
    }
}

/// Which objects a listing source reads, how often it looks, where it starts and what it does with an object it
/// has read. Flattened into each listing source.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListingDto {
    /// Only the objects whose name, under the bucket, container or directory, starts with it. Empty is all of them.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub prefix: String,
    #[serde(default, skip_serializing_if = "PollIntervalSecs::is_default")]
    pub poll_interval_secs: PollIntervalSecs,
    #[serde(default, skip_serializing_if = "ListStartDto::is_default")]
    pub start: ListStartDto,
    #[serde(default, skip_serializing_if = "AfterCommitDto::is_default")]
    pub after_commit: AfterCommitDto,
    /// Where `"move"` puts an object, in place of `prefix`. Named with `"move"` and only then.
    /// Ends in `/` when `prefix` does or is empty, and only then, so a moved key keeps one `/` between them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub move_prefix: Option<String>,
}

/// How a stream's bytes become records: an object source's objects, or what a socket receiver's senders send over
/// TCP. Flattened into each.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct FramingSettingsDto {
    #[serde(default, skip_serializing_if = "FramingDto::is_default")]
    pub framing: FramingDto,
    /// `"lines"` only: a pattern whose first capture group is where an event ends, the group dropped, as Splunk's
    /// `LINE_BREAKER`. Without one, an event ends at a line end.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub line_breaker: Option<String>,
    /// `"lengthPrefix"` only, and required there: where each message's or record's length is and what it counts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub length_prefix: Option<LengthPrefixDto>,
    /// `"fixed"` only, and required there: how many bytes every message or record is.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fixed_bytes: Option<u64>,
}

/// Where a message's length is and what it counts. It counts the whole message or record, its header and length
/// included, or only the bytes after the length.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct LengthPrefixDto {
    /// How many bytes of the message come before its length. Absent is none.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub offset: u64,
    /// How many bytes the length is, 1 to 8.
    pub bytes: u64,
    #[serde(default, skip_serializing_if = "ByteOrderDto::is_default")]
    pub order: ByteOrderDto,
    #[serde(default, skip_serializing_if = "CountsDto::is_default")]
    pub counts: CountsDto,
}

/// Which byte of a number comes first.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum ByteOrderDto {
    /// The most significant first, as a network protocol writes it.
    #[default]
    Big,
    Little,
}

impl ByteOrderDto {
    pub fn is_default(&self) -> bool {
        *self == ByteOrderDto::Big
    }
}

/// What a message's length counts.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum CountsDto {
    /// The bytes that follow the length.
    #[default]
    Rest,
    /// The whole message, its header and length among them.
    Message,
}

impl CountsDto {
    pub fn is_default(&self) -> bool {
        *self == CountsDto::Rest
    }
}

impl FramingSettingsDto {
    /// Whether it says anything but the default, a line per record.
    pub fn is_set(&self) -> bool {
        !self.framing.is_default()
            || self.line_breaker.is_some()
            || self.length_prefix.is_some()
            || self.fixed_bytes.is_some()
    }
}

/// What an object's bytes become.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum FramingDto {
    /// One record per line, or per event where `lineBreaker` breaks the object.
    #[default]
    Lines,
    /// The whole object is one record, for a JSON document such as CloudTrail's that a pipeline unrolls.
    Object,
    /// One record per line, unescaped as a `bytes` object destination wrote it.
    EscapedLines,
    /// One record per message, each saying how long it is where `lengthPrefix` says.
    LengthPrefix,
    /// One record per `fixedBytes` bytes.
    Fixed,
}

impl FramingDto {
    pub fn is_default(&self) -> bool {
        *self == FramingDto::Lines
    }
}

/// Seconds a received SQS message stays hidden from every other worker.
///
/// The source extends a held message's visibility before it lapses, so this is not a bound on how long an object may
/// take. What it bounds is how long a crashed worker's messages stay hidden before another worker gets them: a minute
/// recovers quickly, and is still long enough that extending at half of it costs one call per message per half
/// minute.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
pub struct VisibilitySecs(pub u64);

impl VisibilitySecs {
    pub const DEFAULT: u64 = 60;
    /// SQS's own bounds on a visibility timeout: a second to twelve hours. A Storage queue takes these too.
    pub const MIN: u64 = 1;
    pub const MAX: u64 = 43_200;

    pub fn is_default(&self) -> bool {
        self.0 == Self::DEFAULT
    }
}

impl Default for VisibilitySecs {
    fn default() -> Self {
        VisibilitySecs(Self::DEFAULT)
    }
}

/// Seconds from the start of one listing to the start of the next.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
pub struct PollIntervalSecs(pub u64);

impl PollIntervalSecs {
    pub const DEFAULT: u64 = 60;

    pub fn is_default(&self) -> bool {
        self.0 == Self::DEFAULT
    }
}

impl Default for PollIntervalSecs {
    fn default() -> Self {
        PollIntervalSecs(Self::DEFAULT)
    }
}

/// What an SQL source names, whichever database it reads: where it connects, as whom, and what it reads.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SqlSourceDto {
    /// Which database this source reads, `postgres` or `mysql`.
    #[serde(skip)]
    pub kind: SqlKind,
    pub host: String,
    /// Absent is the kind's own port (`SqlKind::port`). A source naming that port reads as one naming none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
    pub database: String,
    pub user: String,
    /// The credential holding the user's password. Absent is a server that asks for none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub password: Option<SecretRefDto>,
    /// `"require"` (default) encrypts and verifies the server's certificate and name; `"disable"` sends
    /// everything, the password too, in the clear.
    // The file's `tls` is this mode; `tls` in Rust is the TLS settings below, as on every other connector.
    #[serde(default, rename = "tls", skip_serializing_if = "SqlTlsDto::is_default")]
    pub encrypted: SqlTlsDto,
    #[serde(flatten)]
    pub tls: TlsDto,
    /// A `SELECT` or a `WITH`, with no `ORDER BY` or `LIMIT` of its own. smeltr runs it inside a statement of its
    /// own, which adds the position test, the order and the batch limit.
    pub query: String,
    /// How smeltr knows a row is due.
    pub mode: SqlModeDto,
    /// Each field of the record, read from the result column of the same name.
    pub fields: Vec<SqlFieldDto>,
    #[serde(default, skip_serializing_if = "PollIntervalSecs::is_default")]
    pub poll_interval_secs: PollIntervalSecs,
    /// The most rows one poll reads by a position, and every poll sends.
    #[serde(default, skip_serializing_if = "SqlBatch::is_default")]
    pub batch: SqlBatch,
}

impl SqlSourceDto {
    fn postgres<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<SqlSourceDto, D::Error> {
        SqlSourceDto::deserialize(deserializer).map(|s| s.read_as(SqlKind::Postgres))
    }

    fn mysql<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<SqlSourceDto, D::Error> {
        SqlSourceDto::deserialize(deserializer).map(|s| s.read_as(SqlKind::MySql))
    }

    fn read_as(mut self, kind: SqlKind) -> SqlSourceDto {
        self.kind = kind;
        if self.port == Some(kind.port()) {
            self.port = None;
        }
        self
    }

    /// The record its fields declare, read as `kind`. Validation refuses fields that do not declare a record, so the
    /// fallback, what the kind carries before it declares any, is never what a deployment runs.
    pub fn meta(&self, kind: SqlKind) -> crate::plan::topology::SourceMeta {
        SqlFieldDto::meta(kind.meta(), &self.fields).unwrap_or(kind.meta())
    }

    /// The port it connects to, the kind's own where it names none.
    pub fn port(&self, kind: SqlKind) -> u16 {
        self.port.unwrap_or(kind.port())
    }
}

/// Which SQL source a source is, and what that decides about it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SqlKind {
    #[default]
    Postgres,
    MySql,
}

impl SqlKind {
    /// The SQL its database speaks.
    pub fn dialect(self) -> crate::io::sql::Dialect {
        match self {
            SqlKind::Postgres => crate::io::sql::Dialect::Postgres,
            SqlKind::MySql => crate::io::sql::Dialect::MySql,
        }
    }

    /// What its record carries before it declares its fields.
    pub fn meta(self) -> crate::plan::topology::SourceMeta {
        match self {
            SqlKind::Postgres => crate::plan::topology::SourceMeta::POSTGRES,
            SqlKind::MySql => crate::plan::topology::SourceMeta::MYSQL,
        }
    }

    /// The port it connects to when it names none: its server's own.
    pub fn port(self) -> u16 {
        match self {
            SqlKind::Postgres => 5432,
            SqlKind::MySql => 3306,
        }
    }

    /// The scheme of its place, the kind's own name.
    pub fn scheme(self) -> &'static str {
        match self {
            SqlKind::Postgres => "postgres",
            SqlKind::MySql => "mysql",
        }
    }
}

/// Whether an SQL source's connection is encrypted.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum SqlTlsDto {
    #[default]
    Require,
    Disable,
}

impl SqlTlsDto {
    pub fn is_default(&self) -> bool {
        *self == SqlTlsDto::Require
    }
}

/// How an SQL source knows a row is due, cheapest first (`io::sql_source::Mode`).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "camelCase", deny_unknown_fields)]
pub enum SqlModeDto {
    /// Rows past the last one sent, by a column that only grows as rows are added.
    #[serde(rename_all = "camelCase")]
    Incrementing {
        /// The column that only grows, then the columns that tell apart rows with the same value in it.
        position: Vec<String>,
        /// How long to wait for late commits, for a position whose first column is a time.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        settle_secs: Option<u32>,
        /// Where a source with no progress begins: `"now"` (default), `"beginning"`, or `"from"` `startFrom`.
        #[serde(default, skip_serializing_if = "SqlStartDto::is_default")]
        start: SqlStartDto,
        /// Where `"from"` begins: an integer for an i64 position, an ISO 8601 time with its offset for a datetime one.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        start_from: Option<serde_json::Value>,
    },
    /// Rows past the last one sent, by a time every insert and update sets.
    #[serde(rename_all = "camelCase")]
    UpdatedAt {
        /// The time column, then the columns that tell apart rows with the same value in it.
        position: Vec<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        settle_secs: Option<u32>,
        /// Where a source with no progress begins: `"now"` (default), `"beginning"`, or `"from"` `startFrom`.
        #[serde(default, skip_serializing_if = "SqlStartDto::is_default")]
        start: SqlStartDto,
        /// Where `"from"` begins: an integer for an i64 position, an ISO 8601 time with its offset for a datetime one.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        start_from: Option<serde_json::Value>,
    },
    /// Every row each poll, sent when its key is new or its values changed.
    Comparing { key: Vec<String> },
    /// Every row each poll, sent whole.
    EveryPoll,
}

/// Where an SQL source's position mode begins when it has no progress.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum SqlStartDto {
    /// At the largest value the query returns on the first poll.
    #[default]
    Now,
    /// Every row the query returns.
    Beginning,
    /// At `startFrom`.
    From,
}

impl SqlStartDto {
    pub fn is_default(&self) -> bool {
        *self == SqlStartDto::Now
    }
}

/// A Confluent Schema Registry, reached under its connector's TLS.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SchemaRegistryDto {
    /// Its address, as `https://registry.example.com`.
    pub url: String,
    /// Basic auth: `username` and `password` together. Neither signs in as no one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub password: Option<SecretRefDto>,
}

impl SchemaRegistryDto {
    /// Its password, by the field naming it.
    pub fn secrets(&self) -> Vec<(&'static str, &SecretRefDto)> {
        self.password.iter().map(|r| ("schemaRegistry.password", r)).collect()
    }

    pub fn secrets_mut(&mut self) -> Vec<&mut SecretRefDto> {
        self.password.iter_mut().collect()
    }

    /// Its address, by the field naming it.
    pub fn url(&self) -> (&'static str, &str) {
        ("schemaRegistry.url", &self.url)
    }
}

/// How a source's payloads become its record. Absent `format` decodes each as text into `_raw`, with the source's
/// encoding.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ReadFormatDto {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub format: Option<SourceFormatDto>,
    /// The record each payload is read into, each field by its name in the writer's record.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub fields: Vec<SqlFieldDto>,
}

/// A payload format a source reads into declared fields.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum SourceFormatDto {
    Avro,
}

impl ReadFormatDto {
    /// `kind`'s meta, its record the declared fields where it reads Avro. Validation refuses fields that do not
    /// declare a record, so the fallback is never what a deployment runs.
    pub fn meta(&self, kind: crate::plan::topology::SourceMeta) -> crate::plan::topology::SourceMeta {
        match self.format {
            Some(SourceFormatDto::Avro) => SqlFieldDto::open_meta(kind.clone(), &self.fields).unwrap_or(kind),
            None => kind,
        }
    }
}

/// One field a source declares of its record: what it is read from, an SQL result's column or an Avro record's field,
/// and its type as a pipeline spells one (`i64`, `datetime`, `array<str>`).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SqlFieldDto {
    pub name: String,
    #[serde(rename = "type")]
    pub ty: String,
}

impl SqlFieldDto {
    /// `kind`'s meta, declaring `fields` as its record. Err names the first field that does not.
    pub fn meta(
        kind: crate::plan::topology::SourceMeta,
        fields: &[SqlFieldDto],
    ) -> Result<crate::plan::topology::SourceMeta, String> {
        kind.declaring(SqlFieldDto::record(fields)?)
    }

    /// `kind`'s meta, declaring `fields` as its open record (`SourceMeta::declaring_open`): an Avro read's.
    pub fn open_meta(
        kind: crate::plan::topology::SourceMeta,
        fields: &[SqlFieldDto],
    ) -> Result<crate::plan::topology::SourceMeta, String> {
        kind.declaring_open(SqlFieldDto::record(fields)?)
    }

    fn record(fields: &[SqlFieldDto]) -> Result<Vec<(String, crate::core::column::DataType)>, String> {
        fields
            .iter()
            .map(|f| {
                crate::expr::parse::parse_type(&f.ty)
                    .map(|ty| (f.name.clone(), ty))
                    .map_err(|e| {
                        format!(
                            "field `{}` has the type {:?}, which is not a type: {}",
                            f.name, f.ty, e.message
                        )
                    })
            })
            .collect()
    }
}

/// The most rows one poll of an SQL source reads.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
pub struct SqlBatch(pub u32);

impl SqlBatch {
    pub const DEFAULT: u32 = 5_000;
    pub const MAX: u32 = 100_000;

    pub fn is_default(&self) -> bool {
        self.0 == Self::DEFAULT
    }
}

impl Default for SqlBatch {
    fn default() -> Self {
        SqlBatch(Self::DEFAULT)
    }
}

/// What a listing source reads first.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum ListStartDto {
    /// Every object its first listing finds.
    #[default]
    Everything,
    /// Only objects that arrive after its first listing. Needs `afterCommit` `"keep"`: the ledger is what records
    /// the first listing as read.
    New,
}

impl ListStartDto {
    pub fn is_default(&self) -> bool {
        *self == ListStartDto::Everything
    }
}

/// What a listing source does with an object once every record of it is committed.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum AfterCommitDto {
    /// Leaves it, and records it in the source's ledger. Needs only read access.
    #[default]
    Keep,
    /// Deletes it.
    Delete,
    /// Copies it under `movePrefix`, then deletes it.
    Move,
}

impl AfterCommitDto {
    pub fn is_default(&self) -> bool {
        *self == AfterCommitDto::Keep
    }
}

/// What every object store destination writes, and how much at once. Flattened into each cloud's own fields.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ObjectSettingsDto {
    /// What every object key starts with, for a row with no `__destination.prefix`. `%Y %m %d %H %M` are the
    /// write time, UTC.
    #[serde(default, skip_serializing_if = "ObjectPrefix::is_default")]
    pub prefix: ObjectPrefix,
    /// `"ndjson"` (default), `"json"`, `"parquet"`, `"avro"`, `"csv"`, `"text"` or `"bytes"`.
    #[serde(default, skip_serializing_if = "ObjectFormatDto::is_default")]
    pub format: ObjectFormatDto,
    /// The field `"text"` or `"bytes"` writes. Absent is `_raw` for `"text"`, `__source.bytes` for `"bytes"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
    /// What `"bytes"` does with a row that holds a line break. Absent is `"escape"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub newlines: Option<NewlinesDto>,
    /// How `"parquet"` writes a variant. Absent is `"json"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub variant: Option<VariantDto>,
    /// Absent is gzip.
    #[serde(default = "CompressionDto::gzip", skip_serializing_if = "CompressionDto::is_gzip")]
    pub compression: CompressionDto,
    /// Parts of one file uploading at once.
    #[serde(default, skip_serializing_if = "InFlight::is_default")]
    pub in_flight: InFlight,
    #[serde(default, skip_serializing_if = "ObjectBytes::is_default")]
    pub max_object_bytes: ObjectBytes,
    /// Memory for records not yet in the bucket. Unset is `BufferBytes::OBJECT_DEFAULT`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub buffer_bytes: Option<BufferBytes>,
    /// Seconds one store call, or one finishing of the files, may take before it is a stall.
    #[serde(default, skip_serializing_if = "StallAfter::is_default")]
    pub stall_after_secs: StallAfter,
    /// Seconds a file stays open, taking batch after batch, before it is finished and its batches
    /// acknowledged.
    #[serde(default, skip_serializing_if = "FileInterval::is_default")]
    pub file_interval_secs: FileInterval,
}

/// Seconds an object or table destination's files stay open before they are finished or committed. A source waits
/// this long for its batches' acknowledgement, which no source bounds: a Kafka reader keeps its membership while it
/// waits, and a queue source extends its messages' visibility. What it costs is delay, and replay: after a crash, an
/// interval's rows are read again, though no more than `maxObjectBytes` or `bufferBytes` holds. Five minutes is the
/// least Amazon Security Lake takes, and long enough that a quiet stream makes files a query engine reads well.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
pub struct FileInterval(pub u64);

impl FileInterval {
    pub const DEFAULT: u64 = 300;
    pub const MIN: u64 = 5;
    pub const MAX: u64 = 3600;

    pub fn is_default(&self) -> bool {
        self.0 == Self::DEFAULT
    }
}

impl Default for FileInterval {
    fn default() -> Self {
        FileInterval(Self::DEFAULT)
    }
}

/// What an object store destination reached over HTTPS takes besides its [`ObjectSettingsDto`]. Flattened into the
/// S3, GCS and Azure Blob destinations.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct CloudSettingsDto {
    /// One of the cloud's own storage classes. Absent is the bucket's default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_class: Option<String>,
}

/// Where an S3 destination with no `endpoint` is reached.
pub const S3_ENDPOINT: &str = "https://s3.amazonaws.com";

/// How many times a Queue/Azure Blob source receives a message before it moves it to the poison queue.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
pub struct MaxDeliveries(pub u32);

impl MaxDeliveries {
    pub const DEFAULT: u32 = 5;
    pub const MIN: u32 = 1;
    pub const MAX: u32 = 1_000;

    pub fn is_default(&self) -> bool {
        self.0 == Self::DEFAULT
    }
}

impl Default for MaxDeliveries {
    fn default() -> Self {
        MaxDeliveries(Self::DEFAULT)
    }
}

/// Where a Google Cloud Storage destination with no `endpoint` is reached.
pub const GCS_ENDPOINT: &str = "https://storage.googleapis.com";

/// Where a Pub/Sub/GCS source with no `pubsubEndpoint` is reached.
pub const PUBSUB_ENDPOINT: &str = "https://pubsub.googleapis.com";

/// Where an Azure Blob Storage destination with no `endpoint` is reached, under its account's own name.
pub const AZURE_ENDPOINT: &str = "https://blob.core.windows.net";

/// An S3 destination's key prefix.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
pub struct ObjectPrefix(pub String);

impl ObjectPrefix {
    pub const DEFAULT: &str = "%Y/%m/%d/%H/";

    pub fn is_default(&self) -> bool {
        self.0 == Self::DEFAULT
    }
}

impl Default for ObjectPrefix {
    fn default() -> Self {
        ObjectPrefix(Self::DEFAULT.to_string())
    }
}

/// What a Kafka, Splunk or HTTP destination writes of each row: the record as JSON, one field's text, or one field's
/// bytes.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum FormatDto {
    #[default]
    Json,
    Text,
    #[serde(alias = "sourceBytes")]
    Bytes,
    /// Each row's route's record as Avro, framed with its schema's id in a registry.
    Avro,
}

impl FormatDto {
    pub fn is_default(&self) -> bool {
        *self == FormatDto::Json
    }
}

/// Wire form of `plan::topology::RequestBody`.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum RequestBodyDto {
    #[default]
    Lines,
    JsonArray,
    Single,
}

impl RequestBodyDto {
    pub fn is_default(&self) -> bool {
        *self == RequestBodyDto::Lines
    }
}

/// Wire form of `plan::topology::RemoteWriteVersion`.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub enum RemoteWriteVersionDto {
    #[default]
    #[serde(rename = "1.0")]
    V1,
    #[serde(rename = "2.0")]
    V2,
}

impl RemoteWriteVersionDto {
    pub fn is_default(&self) -> bool {
        *self == RemoteWriteVersionDto::V1
    }
}

/// Wire form of `plan::topology::ObjectFormat`, less the text field.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum ObjectFormatDto {
    #[default]
    Ndjson,
    Json,
    Parquet,
    Text,
    #[serde(alias = "sourceBytes")]
    Bytes,
    /// An Avro container file, its schema the route's record.
    Avro,
    /// A CSV file, a column per leaf of the route's record.
    Csv,
}

impl ObjectFormatDto {
    pub fn is_default(&self) -> bool {
        *self == ObjectFormatDto::Ndjson
    }
}

/// Wire form of `plan::topology::Newlines`.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum NewlinesDto {
    Escape,
    Refuse,
}

/// How a Parquet object holds a variant: `json`, a string of its JSON text, or `native`, Parquet's variant type.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum VariantDto {
    Json,
    Native,
}

/// The largest file an object destination puts in its bucket, compressed.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
pub struct ObjectBytes(#[serde(with = "super::bytes")] pub usize);

impl ObjectBytes {
    pub const DEFAULT: usize = 64 * 1024 * 1024;
    pub const MIN: usize = 64 * 1024;
    /// Under S3's largest multipart upload, 10,000 parts of `io::files`' 8 MiB.
    pub const MAX: usize = 64 * 1024 * 1024 * 1024;

    pub fn is_default(&self) -> bool {
        self.0 == Self::DEFAULT
    }
}

impl Default for ObjectBytes {
    fn default() -> Self {
        ObjectBytes(Self::DEFAULT)
    }
}

/// Wire form of `plan::topology::Endpoint`.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum EndpointDto {
    #[default]
    Event,
    Raw,
}

impl EndpointDto {
    pub fn is_default(&self) -> bool {
        *self == EndpointDto::Event
    }
}

/// Wire form of `plan::topology::Acknowledgement`.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum AcknowledgementDto {
    #[default]
    Indexer,
    Receipt,
}

impl AcknowledgementDto {
    pub fn is_default(&self) -> bool {
        *self == AcknowledgementDto::Indexer
    }
}

/// Bytes of events one request may carry before the destination sends early, `DEFAULT` when unset. Stated as a size
/// or a number of bytes, like every byte figure in a bundle.
///
/// The default is the HEC content limit Splunk Cloud's service details give
/// (<https://docs.splunk.com/Documentation/SplunkCloud/latest/Service/SplunkCloudservice>).
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
pub struct RequestBytes<const DEFAULT: usize = { 1024 * 1024 }>(#[serde(with = "super::bytes")] pub usize);

/// An HTTP destination's request size: one whole batch by default, so a batch is one request. A literal, so a change
/// to the batch size does not change what a stored destination means.
pub type HttpRequestBytes = RequestBytes<{ 4 * 1024 * 1024 }>;
const _: () = assert!(
    HttpRequestBytes::DEFAULT >= crate::worker::TICK_MAX_BYTES,
    "a batch is one request"
);

impl RequestBytes {
    pub const MIN: usize = 4 * 1024;
    pub const MAX: usize = 100 * 1024 * 1024;
}

impl<const DEFAULT: usize> RequestBytes<DEFAULT> {
    pub const DEFAULT: usize = DEFAULT;

    pub fn is_default(&self) -> bool {
        self.0 == DEFAULT
    }
}

impl<const DEFAULT: usize> Default for RequestBytes<DEFAULT> {
    fn default() -> Self {
        RequestBytes(DEFAULT)
    }
}

/// How many brokers hold a Kafka batch before it counts as delivered. Kafka's `acks` in `props` is reserved, so this is
/// the one place it is set.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum AcksDto {
    None,
    One,
    #[default]
    All,
}

impl AcksDto {
    pub fn is_default(&self) -> bool {
        *self == AcksDto::All
    }

    pub fn to_plan(self) -> crate::plan::topology::Acks {
        use crate::plan::topology::Acks;
        match self {
            AcksDto::None => Acks::None,
            AcksDto::One => Acks::One,
            AcksDto::All => Acks::All,
        }
    }
}

/// A destination's codec. A closed set, so the GUI cannot offer a value the save would reject; Kafka's
/// `compression.type` in `props` is reserved for the same reason.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum CompressionDto {
    #[default]
    None,
    Gzip,
    Snappy,
    Lz4,
    Zstd,
}

impl CompressionDto {
    pub fn is_default(&self) -> bool {
        *self == CompressionDto::None
    }

    /// An object destination's default.
    pub fn gzip() -> Self {
        CompressionDto::Gzip
    }

    pub fn is_gzip(&self) -> bool {
        *self == CompressionDto::Gzip
    }

    /// A table destination's default.
    pub fn zstd() -> Self {
        CompressionDto::Zstd
    }

    pub fn is_zstd(&self) -> bool {
        *self == CompressionDto::Zstd
    }

    pub fn to_plan(self) -> crate::plan::topology::Compression {
        use crate::plan::topology::Compression;
        match self {
            CompressionDto::None => Compression::None,
            CompressionDto::Gzip => Compression::Gzip,
            CompressionDto::Snappy => Compression::Snappy,
            CompressionDto::Lz4 => Compression::Lz4,
            CompressionDto::Zstd => Compression::Zstd,
        }
    }
}

/// Bytes of documents one bulk request may carry before the destination sends early. Stated as a size (`"5 MiB"`) or
/// a number of bytes, like every byte figure in a bundle (`config_file::bytes`).
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
pub struct BulkBytes(#[serde(with = "super::bytes")] pub usize);

impl BulkBytes {
    pub const DEFAULT: usize = 5 * 1024 * 1024;
    pub const MIN: usize = 64 * 1024;
    /// Elasticsearch's default `http.max_content_length`.
    pub const MAX: usize = 100 * 1024 * 1024;

    pub fn is_default(&self) -> bool {
        self.0 == Self::DEFAULT
    }
}

impl Default for BulkBytes {
    fn default() -> Self {
        BulkBytes(Self::DEFAULT)
    }
}

/// Bytes one destination may hold undelivered, by whichever mechanism holds them. A stalled destination fills its
/// buffer and pauses the sources feeding it rather than growing the process.
///
/// It is the egress queue's budget, which holds a batch from acceptance until the destination acknowledges it -- so it
/// is also what decides how much can be sent and unacknowledged at once, and how far the reader runs ahead. Kafka
/// splits it, half to the egress queue and half to librdkafka (`queue.buffering.max.kbytes`), so a Kafka destination
/// holds this figure in all; `queue.buffering.max.messages` binds first there for small records.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
pub struct BufferBytes(#[serde(with = "super::bytes")] pub usize);

impl BufferBytes {
    pub const DEFAULT: usize = 64 * 1024 * 1024;
    /// One tick's worth of source bytes (`worker::TICK_MAX_BYTES`); below it an ordinary batch reports a stall.
    pub const MIN: usize = 4 * 1024 * 1024;
    /// librdkafka's own default.
    pub const MAX: usize = 1024 * 1024 * 1024;
    /// An object store destination's: memory for records not yet in the bucket. Its files are finished early when it
    /// fills (`io::files`), so it bounds them too.
    pub const OBJECT_DEFAULT: usize = 256 * 1024 * 1024;
    /// An object store destination's: the memory of the largest memory-optimized cloud machine, 32 TiB (AWS
    /// u7in-32tb).
    pub const OBJECT_MAX: usize = 32 * 1024 * 1024 * 1024 * 1024;

    pub fn is_default(&self) -> bool {
        self.0 == Self::DEFAULT
    }
}

impl Default for BufferBytes {
    fn default() -> Self {
        BufferBytes(Self::DEFAULT)
    }
}

/// Bytes a Kafka source's client may hold fetched-but-unprocessed before it stops asking: the largest term in a
/// reader's footprint. One queue per client, whatever the partition count or the sources sharing it. One fetch
/// carries half of it, and a fetch already asked for still lands, so the client may hold this plus half of it per
/// broker; each source's inbox pauses its partitions past one fetch, and with the record that crosses it holds up to
/// two.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
pub struct FetchBytes(#[serde(with = "super::bytes")] pub usize);

impl FetchBytes {
    /// librdkafka's own default.
    pub const DEFAULT: usize = 64 * 1024 * 1024;
    /// One tick's batch.
    pub const MIN: usize = 4 * 1024 * 1024;
    pub const MAX: usize = 512 * 1024 * 1024;

    pub fn is_default(&self) -> bool {
        self.0 == Self::DEFAULT
    }
}

impl Default for FetchBytes {
    fn default() -> Self {
        FetchBytes(Self::DEFAULT)
    }
}

/// A source's text encoding: `"auto"` or a WHATWG label (`"utf-8"`, `"utf-16le"`, `"shift_jis"`). A newtype so an
/// absent field means `auto` rather than an empty name, and so an unchanged file saves without the field.
///
/// A string, not a closed enum: the set is encoding_rs', and load-time validation refuses a name it does not know
/// (`format::encoding::Encoding::for_label`).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
pub struct EncodingLabel(pub String);

impl EncodingLabel {
    /// Detect per record. See `format::encoding`.
    pub const DEFAULT: &str = "auto";

    pub fn is_default(&self) -> bool {
        self.0 == Self::DEFAULT
    }
}

impl Default for EncodingLabel {
    fn default() -> Self {
        EncodingLabel(Self::DEFAULT.to_string())
    }
}

/// A severity written down: one of `logs::LEVEL_LABELS`.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
pub struct LevelLabel(pub String);

impl LevelLabel {
    /// The startup level unless one is set, so a source that names nothing carries what a process keeps until an
    /// operator sets levels.
    pub const DEFAULT: &str = "info";

    pub fn is_default(&self) -> bool {
        self.0 == Self::DEFAULT
    }
}

impl Default for LevelLabel {
    fn default() -> Self {
        LevelLabel(Self::DEFAULT.to_string())
    }
}

/// Seconds a destination may spend delivering one batch before it gives up and reports a stall.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
pub struct StallAfter(pub u64);

impl StallAfter {
    pub const DEFAULT: u64 = 30;
    pub const MIN: u64 = 1;
    /// A fraction of the consumer's `max.poll.interval.ms` (300 s), which a flush must not outlast
    /// (`io::kafka::MAX_POLL_INTERVAL`).
    pub const MAX: u64 = 120;

    pub fn is_default(&self) -> bool {
        self.0 == Self::DEFAULT
    }
}

impl Default for StallAfter {
    fn default() -> Self {
        StallAfter(Self::DEFAULT)
    }
}

/// Concurrent bulk requests to a destination. A newtype so an absent field means the default, not zero.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
pub struct InFlight(pub usize);

impl InFlight {
    /// Where the loop starts, not where it stops: a destination climbs from here to what its buffer affords
    /// (`plan::topology::ElasticDestinationConfig::max_in_flight`).
    pub const DEFAULT: usize = 4;

    pub fn is_default(&self) -> bool {
        self.0 == Self::DEFAULT
    }

    /// Where a destination whose buffer affords `affords` requests starts. The default starts no higher than that, so
    /// raising the request size alone never leaves the start out of reach; any other start above it is refused. A
    /// bundle cannot tell a stated default from an absent one, so a stated default is lowered too.
    pub fn start(&self, affords: usize) -> usize {
        if self.is_default() {
            self.0.min(affords).max(1)
        } else {
            self.0
        }
    }
}

impl Default for InFlight {
    fn default() -> Self {
        InFlight(Self::DEFAULT)
    }
}

/// Wire form of `plan::topology::DocIdPolicy`.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum DocIdPolicyDto {
    #[default]
    Auto,
    Dedupe,
    Overwrite,
}

impl DocIdPolicyDto {
    pub(super) fn is_default(&self) -> bool {
        *self == DocIdPolicyDto::default()
    }
}

// -- routes -------------------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RouteDto {
    /// The author's id, carried onto `RouteConfig`. Unique, so a diff can tell an edit from a delete-and-insert.
    pub id: String,
    pub source: String,
    #[serde(default)]
    pub condition: Option<String>,
    pub pipeline: String,
    /// Rules are additive: a row matching two goes to the union of their destinations, once each. Empty delivers
    /// nowhere.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub deliver: Vec<DeliverRuleDto>,
    /// The route's answer to its pipeline's exceptions, over the group's. Absent means the group's. It names no
    /// delivery kind.
    #[serde(default, rename = "exceptions", skip_serializing_if = "Option::is_none")]
    pub on_exception: Option<OnExceptionDto>,
}

/// A condition over the pipeline's output, and the destinations a matching row goes to.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeliverRuleDto {
    /// Omitted matches every surviving row.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub when: Option<String>,
    /// `DestinationDto.id`s.
    pub to: Vec<String>,
}

// -- pipelines ------------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PipelineDto {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub steps: Vec<StepDto>,
    /// The schemas it produces, by id: each row keeps one of them.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub produces: Vec<String>,
}

/// `plan::config::OnException`, over the answers of the layer below: the built-in ones, then the group's. Each key
/// optional: `categories` answers each category it names, then `kinds` each kind it names. `tagField` names the field
/// that holds a tagged or delivered row's failures; the group's is `smeltr.exception` unless it names another, and a
/// route may name its own. A dotted name nests.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct OnExceptionDto {
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub categories: BTreeMap<String, AnswerDto>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub kinds: BTreeMap<String, AnswerDto>,
    #[serde(default, rename = "tagField", skip_serializing_if = "Option::is_none")]
    pub tag_field: Option<String>,
}

/// `plan::config::Step` as one discriminated union. Every step carries an `id`. No two share one within a pipeline,
/// counting every call's id and the steps its calls inline. Every `"type"` but `"if"` and `"comment"` also carries
/// `filter` and `disabled`.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase", deny_unknown_fields)]
pub enum StepDto {
    If {
        id: String,
        condition: String,
        #[serde(default)]
        then: Vec<StepDto>,
        #[serde(default)]
        r#else: Vec<StepDto>,
    },
    Comment {
        id: String,
        text: String,
    },
    Eval {
        id: String,
        #[serde(default)]
        filter: Option<String>,
        #[serde(default)]
        disabled: bool,
        assignments: Vec<AssignmentDto>,
    },
    Code {
        id: String,
        #[serde(default)]
        filter: Option<String>,
        #[serde(default)]
        disabled: bool,
        body: String,
    },
    Drop {
        id: String,
        #[serde(default)]
        filter: Option<String>,
        #[serde(default)]
        disabled: bool,
        condition: String,
    },
    Rename {
        id: String,
        #[serde(default)]
        filter: Option<String>,
        #[serde(default)]
        disabled: bool,
        pairs: Vec<RenamePairDto>,
    },
    Remove {
        id: String,
        #[serde(default)]
        filter: Option<String>,
        #[serde(default)]
        disabled: bool,
        fields: Vec<String>,
    },
    Parser {
        id: String,
        #[serde(default)]
        filter: Option<String>,
        #[serde(default)]
        disabled: bool,
        input: String,
        /// The object the parse writes into. A step written before it reads `$`, the record.
        #[serde(default = "root")]
        into: String,
        kind: ParserKindDto,
        #[serde(default)]
        declared: Vec<FieldDto>,
    },
    #[serde(rename_all = "camelCase")]
    Lookup {
        id: String,
        #[serde(default)]
        filter: Option<String>,
        #[serde(default)]
        disabled: bool,
        input: String,
        /// A `LookupTableDto.id`. Everything else here is this step's own.
        table: String,
        match_kind: MatchKindDto,
        #[serde(default)]
        multiplicity: MultiplicityDto,
        /// Unused for an MMDB table, which has no key column.
        #[serde(default)]
        key_column: String,
        outputs: Vec<LookupOutputDto>,
    },
    // The enum's `rename_all` renames variants, not fields.
    #[serde(rename_all = "camelCase")]
    Unroll {
        id: String,
        #[serde(default)]
        filter: Option<String>,
        #[serde(default)]
        disabled: bool,
        field: String,
        /// See `plan::config::FunctionKind::Unroll`.
        #[serde(default)]
        keep_parent_fields: bool,
    },
    /// Inline another pipeline's steps here. `config_file::load` expands every `Call` before the compiler sees the
    /// pipeline.
    Call {
        id: String,
        #[serde(default)]
        filter: Option<String>,
        #[serde(default)]
        disabled: bool,
        /// A `PipelineDto.id`.
        pipeline: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AssignmentDto {
    pub target: String,
    pub expr: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RenamePairDto {
    pub from: String,
    pub to: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase", deny_unknown_fields)]
pub enum ParserKindDto {
    #[serde(rename_all = "camelCase")]
    Json {
        /// Whether a key is split at its dots into a path; absent is the parser's default.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        split_dots: Option<bool>,
    },
    /// The columns are the step's own `declared` list, in order -- see `StepDto::Parser`.
    #[serde(rename_all = "camelCase")]
    Csv {
        /// One ASCII character, as a 1-character string.
        delimiter: String,
        /// Whether a key is split at its dots into a path; absent is the parser's default.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        split_dots: Option<bool>,
        /// Whether the parse rebuilds its input, each skipped column left empty.
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        rebuild: bool,
    },
    #[serde(rename_all = "camelCase")]
    KeyValue {
        pair_sep: String,
        kv_sep: String,
        #[serde(default)]
        quote: Option<String>,
        /// Whether an unquoted value runs to the separator before the next key; absent is the parser's default.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        next_key: Option<bool>,
        /// Whether a key is split at its dots into a path; absent is the parser's default.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        split_dots: Option<bool>,
        /// Whether the parse rebuilds its input, each skipped pair left out.
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        rebuild: bool,
    },
    #[serde(rename_all = "camelCase")]
    Grok {
        patterns: Vec<String>,
        #[serde(default)]
        custom_patterns: Vec<CustomPatternDto>,
        /// Whether a key is split at its dots into a path; absent is the parser's default.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        split_dots: Option<bool>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CustomPatternDto {
    pub name: String,
    pub pattern: String,
}

/// A schema the bundle declares (`schema`): a table of fields, named by id from the pipelines that produce it and
/// the destinations that accept it.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SchemaDto {
    pub id: String,
    /// Shown for it. The id where empty.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub title: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub description: String,
    #[serde(default)]
    pub fields: Vec<SchemaFieldDto>,
}

/// One field of a schema: a row of its table.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SchemaFieldDto {
    /// Its keys from the record's root, dotted.
    pub field: String,
    /// What it must hold, spelled as a declaration spells a type: `str`, `array<i64>`, `{...}` for an object.
    #[serde(rename = "type")]
    pub ty: String,
    /// Whether a row must hold it, should, or may.
    #[serde(default, skip_serializing_if = "Requirement::is_optional")]
    pub requirement: Requirement,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub description: String,
    /// A value it may hold, as text: what a standard or a spreadsheet gives as its example.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub example: String,
    /// How it maps in an Elasticsearch index: a field's mapping, laid over the one its type has (`io::elastic_mapping`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub elasticsearch: Option<serde_json::Map<String, serde_json::Value>>,
}

/// How much a schema asks of a row for one field. A field sits in the object its path names, so it is asked of a row
/// only where that object is there.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "lowercase")]
pub enum Requirement {
    /// A row that lacks it breaks the schema.
    Required,
    /// A row should hold it; one that lacks it is told so, and still keeps the schema.
    Recommended,
    #[default]
    Optional,
}

impl Requirement {
    pub fn is_optional(&self) -> bool {
        *self == Requirement::Optional
    }

    /// The requirement a standard or a spreadsheet names: `required`, `recommended` or `optional`, without regard
    /// to case.
    pub fn named(name: &str) -> Option<Requirement> {
        match name.trim().to_ascii_lowercase().as_str() {
            "required" => Some(Requirement::Required),
            "recommended" => Some(Requirement::Recommended),
            "optional" => Some(Requirement::Optional),
            _ => None,
        }
    }
}

/// A named grok pattern, declared once for the bundle and named as `%{id}` from any grok in it.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GrokPatternDto {
    pub id: String,
    pub pattern: String,
}

/// A shared lookup table, referenced by id from any number of `StepDto::Lookup` steps.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LookupTableDto {
    pub id: String,
    #[serde(flatten)]
    pub source: LookupSourceDto,
}

/// `plan::config::LookupSource`'s DTO.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase", deny_unknown_fields)]
pub enum LookupSourceDto {
    Csv {
        path: String,
        /// One ASCII character, as a 1-character string.
        delimiter: String,
        /// The file's column names, a GUI hint. The compiler reads the file.
        #[serde(default)]
        headers: Vec<String>,
    },
    Inline {
        headers: Vec<String>,
        rows: Vec<Vec<String>>,
    },
    Mmdb {
        path: String,
    },
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum MatchKindDto {
    Exact,
    Cidr,
    Regex,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum MultiplicityDto {
    #[default]
    Best,
    First,
    All,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LookupOutputDto {
    pub source: String,
    pub field: String,
    /// The miss default. An empty string loads as absent.
    #[serde(default)]
    pub default: Option<String>,
    /// The field's type, in a declared field's spelling: a scalar name or the `datetime` object. Absent is `str`.
    #[serde(default, rename = "type", skip_serializing_if = "Option::is_none")]
    pub ty: Option<DataTypeDto>,
}

// -- shared: declared fields / DataType --------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FieldDto {
    pub name: String,
    #[serde(rename = "type")]
    pub ty: DataTypeDto,
}

/// A scalar is a bare lowercase string (`"i64"`); everything else is a tagged object.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum DataTypeDto {
    Scalar(String),
    Compound(CompoundTypeDto),
}

/// A type written as a tagged object rather than a bare scalar string: `Array`, `Struct`, or a `datetime` with the
/// `parseTime` format its text arrives in (`{"type":"datetime","format":"%Y/%m/%d %H:%M:%S"}`). A bare `"datetime"`
/// string is the auto-detected form. `plan::config::TimeSource` is the compiled form of either.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase", deny_unknown_fields)]
pub enum CompoundTypeDto {
    Array {
        of: Box<DataTypeDto>,
    },
    /// An object: its named fields, and whether it is open: a read reaches its unclaimed fields, each value a
    /// variant. A closed object carries no `open`.
    Struct {
        fields: Vec<FieldDto>,
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        open: bool,
    },
    /// `rename` pins the wire spelling: `rename_all` alone would camelCase this variant's name to `"dateTime"`,
    /// lowercasing only the leading `D` and leaving the inner `T` capitalized.
    #[serde(rename = "datetime")]
    DateTime {
        format: String,
    },
}

pub(super) fn is_zero(n: &u64) -> bool {
    *n == 0
}

pub(super) fn is_logs(signal: &Signal) -> bool {
    *signal == Signal::Logs
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A postgres source as an earlier build stored it: every field it has, in the order it writes them.
    const STORED_POSTGRES: &str = r#"{"id":"pg","type":"postgres","host":"db.internal",PORT"database":"auth","user":"reader","password":{"secret":"pg","version":2},"tls":"disable","query":"SELECT id FROM events","mode":{"type":"everyPoll"},"fields":[{"name":"id","type":"i64"}],"pollIntervalSecs":120,"batch":100}"#;

    /// A stored postgres source reads and writes back byte for byte, with or without a port.
    #[test]
    fn a_stored_postgres_source_writes_back_as_it_was_stored() {
        for port in ["", r#""port":6432,"#] {
            let json = STORED_POSTGRES.replace("PORT", port);
            let dto: SourceDto = serde_json::from_str(&json).unwrap();
            assert!(matches!(dto.kind, SourceKindDto::Postgres(_)));
            assert_eq!(serde_json::to_string(&dto).unwrap(), json);
        }
    }

    /// A source naming its kind's own port reads as one naming none, and is written without it, as an earlier build
    /// wrote a postgres source naming 5432.
    #[test]
    fn a_source_naming_its_own_port_is_written_without_it() {
        for (kind, own, other) in [("postgres", 5432, 3306), ("mysql", 3306, 5432)] {
            let named = |port: u16| {
                STORED_POSTGRES
                    .replace(r#""type":"postgres""#, &format!(r#""type":"{kind}""#))
                    .replace("PORT", &format!(r#""port":{port},"#))
            };
            let dto: SourceDto = serde_json::from_str(&named(own)).unwrap();
            let (sql_kind, sql) = dto.kind.sql().unwrap();
            assert_eq!(sql.port, None, "{kind}");
            assert_eq!(sql.port(sql_kind), own);
            assert_eq!(
                serde_json::to_string(&dto).unwrap(),
                STORED_POSTGRES
                    .replace(r#""type":"postgres""#, &format!(r#""type":"{kind}""#))
                    .replace("PORT", "")
            );
            let dto: SourceDto = serde_json::from_str(&named(other)).unwrap();
            assert_eq!(dto.kind.sql().unwrap().1.port(sql_kind), other, "{kind}");
            assert_eq!(serde_json::to_string(&dto).unwrap(), named(other));
        }
    }

    /// A mysql source is tagged `"mysql"`, and refuses a field it does not know as a postgres source does.
    #[test]
    fn a_mysql_source_reads_as_mysql_and_refuses_unknown_fields() {
        let json = STORED_POSTGRES
            .replace(r#""type":"postgres""#, r#""type":"mysql""#)
            .replace("PORT", "");
        let dto: SourceDto = serde_json::from_str(&json).unwrap();
        assert!(matches!(dto.kind, SourceKindDto::MySql(_)));
        assert!(dto.kind.single_worker() && dto.kind.runs_its_own_query());
        for kind in ["postgres", "mysql"] {
            let unknown = STORED_POSTGRES
                .replace(r#""type":"postgres""#, &format!(r#""type":"{kind}""#))
                .replace("PORT", r#""sslmode":"verify-full","#);
            assert!(serde_json::from_str::<SourceDto>(&unknown).is_err(), "{kind}");
        }
    }

    /// Pins the exact wire tag against drift: `rename_all = "camelCase"` alone would camelCase the Rust variant
    /// name `DateTime` to `"dateTime"`, and the GUI editor sends the lowercase `"datetime"`.
    #[test]
    fn a_datetime_field_serializes_with_a_lowercase_tag() {
        let dto = DataTypeDto::Compound(CompoundTypeDto::DateTime {
            format: "%Y/%m/%d".into(),
        });
        let json = serde_json::to_string(&dto).unwrap();
        assert!(json.contains(r#""type":"datetime""#), "{json}");
        assert!(json.contains(r#""format":"%Y/%m/%d""#), "{json}");

        let back: DataTypeDto = serde_json::from_str(&json).unwrap();
        assert!(matches!(
            back,
            DataTypeDto::Compound(CompoundTypeDto::DateTime { format }) if format == "%Y/%m/%d"
        ));
    }

    /// Pins the wire shape the GUI writes: a closed object carries no `open` key, and an open one says so.
    #[test]
    fn an_object_writes_open_only_when_open() {
        for json in [
            r#"{"type":"struct","fields":[{"name":"a","type":"i64"}]}"#,
            r#"{"type":"struct","fields":[{"name":"a","type":"i64"}],"open":true}"#,
            r#"{"type":"struct","fields":[],"open":true}"#,
        ] {
            let dto: DataTypeDto = serde_json::from_str(json).unwrap();
            assert_eq!(serde_json::to_string(&dto).unwrap(), json);
        }
    }

    /// Pins the wire shape the GUI writes: four bare words and one object.
    #[test]
    fn an_answer_is_a_word_or_a_deliver_object() {
        for (json, dto) in [
            (r#""ignore""#, AnswerDto::Ignore),
            (r#""tag""#, AnswerDto::Tag),
            (r#""block""#, AnswerDto::Block),
            (r#""drop""#, AnswerDto::Drop),
            (
                r#"{"deliver":["a","b"]}"#,
                AnswerDto::Deliver {
                    to: vec!["a".into(), "b".into()],
                },
            ),
        ] {
            assert_eq!(serde_json::to_string(&dto).unwrap(), json);
            assert_eq!(serde_json::from_str::<AnswerDto>(json).unwrap(), dto);
        }
    }

    /// A shape it does not know is an error naming what was wrong, not a silent default.
    #[test]
    fn an_answer_it_does_not_know_says_what_was_wrong() {
        for (json, says) in [
            (r#""deliver""#, "unknown variant `deliver`"),
            (r#"{"deliver":"a"}"#, "expected a sequence"),
            (r#"{}"#, "missing field `deliver`"),
            (r#"{"deliver":["a"],"payload":"original"}"#, "unknown field `payload`"),
        ] {
            let err = serde_json::from_str::<AnswerDto>(json).unwrap_err().to_string();
            assert!(err.contains(says), "{json}: {err}");
        }
    }

    /// A `datetime` object carries a format and nothing else: the bare string already says auto.
    #[test]
    fn a_datetime_object_without_a_format_is_a_deserialize_error() {
        assert!(serde_json::from_str::<DataTypeDto>(r#"{"type":"datetime"}"#).is_err());
        assert!(serde_json::from_str::<DataTypeDto>(r#"{"type":"datetime","from":"rfc3339"}"#).is_err());
    }

    /// A source that says nothing about where to start or stop stores nothing about
    /// it, the way every other optional field on the same variant already behaves.
    ///
    /// The GUI's dirty check compares what it sent against what the file holds, so a
    /// default written back is a difference the author never made: the editor then
    /// reports that the file moved under an edit nobody else touched.
    #[test]
    fn a_kafka_source_at_its_defaults_stores_no_start_or_stop() {
        let sent = r#"{"id":"kafka-1","type":"kafka","brokers":"b:9092","topics":["orders"],"groupId":"etl"}"#;
        let dto: SourceDto = serde_json::from_str(sent).unwrap();
        let json = serde_json::to_string(&dto).unwrap();
        assert!(!json.contains("start"), "{json}");
        assert!(!json.contains("stop"), "{json}");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(sent).unwrap(),
            serde_json::from_str::<serde_json::Value>(&json).unwrap()
        );
    }

    /// Same rule, same reason, on the other page an author saves from: a
    /// destination that says nothing about refusals stores nothing about them.
    #[test]
    fn a_destination_answering_no_refusal_stores_no_answer() {
        let sent = r#"{"id":"kafka-1","type":"kafka","brokers":"b:9092","defaultTopic":"out"}"#;
        let dto: DestinationDto = serde_json::from_str(sent).unwrap();
        let json = serde_json::to_string(&dto).unwrap();
        assert!(!json.contains("exceptions"), "{json}");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(sent).unwrap(),
            serde_json::from_str::<serde_json::Value>(&json).unwrap()
        );
    }

    #[test]
    fn an_answer_to_refusals_the_author_chose_is_stored() {
        let sent = concat!(
            r#"{"id":"kafka-1","exceptions":{"kinds":{"Refused":"drop"}},"#,
            r#""type":"kafka","brokers":"b:9092","defaultTopic":"out"}"#
        );
        let dto: DestinationDto = serde_json::from_str(sent).unwrap();
        let json = serde_json::to_string(&dto).unwrap();
        assert!(json.contains(r#""exceptions":{"kinds":{"Refused":"drop"}}"#), "{json}");
    }

    /// A position the author did choose is still written, and reads back as itself.
    #[test]
    fn a_start_and_stop_the_author_chose_are_stored() {
        let sent = concat!(
            r#"{"id":"kafka-1","type":"kafka","brokers":"b:9092","topics":["orders"],"#,
            r#""groupId":"etl","start":{"type":"latest"},"stop":{"type":"timestamp","millis":42}}"#
        );
        let dto: SourceDto = serde_json::from_str(sent).unwrap();
        let json = serde_json::to_string(&dto).unwrap();
        assert!(json.contains(r#""start":{"type":"latest"}"#), "{json}");
        assert!(json.contains(r#""stop":{"type":"timestamp","millis":42}"#), "{json}");
    }
}
