use std::collections::HashMap;

use crate::core::id::{DestId, SourceId};
use crate::format::encoding::Encoding;
use crate::metrics::MetricsSourceConfig;
use crate::plan::config::{Answer, Payload};

use super::*;

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
pub(super) fn meta_of(metas: &SourceMetas, source: SourceId) -> SourceMeta {
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

#[cfg(test)]
mod tests {
    use super::*;

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
