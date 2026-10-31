//! The JSON shape: inert data with serde derives. `deny_unknown_fields` throughout, so a field this loader does not
//! know fails loudly rather than being dropped on the next save. Structs with a `#[serde(flatten)]` field cannot carry
//! it (serde breaks on the combination); their flattened enum still has it.

use std::collections::BTreeMap;

use crate::plan::topology::{Signal, Transport};
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

/// The TLS a receiver serves: the certificate it serves to every sender, and the authorities a sender's certificate
/// must chain to. It serves TLS when it names a certificate, and admits only senders whose certificate chains to its
/// authorities when it names any. A receiver trusts no public root, so it has no say over them.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ServedTlsDto {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub certificate: Option<String>,
    /// PEM, one or more certificates.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trusted_authorities: Option<String>,
}

impl ServedTlsDto {
    /// Whether the receiver says anything about its TLS.
    pub fn is_set(&self) -> bool {
        self.certificate.is_some() || self.trusted_authorities.is_some()
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

impl SourceDto {
    /// Every credential this source names, by the field naming it. Exhaustive on purpose: a kind added here must
    /// answer, or its credentials would go unnamed, unchecked at deploy and left behind on a version move.
    pub fn secrets(&self) -> Vec<(&'static str, &SecretRefDto)> {
        match &self.kind {
            SourceKindDto::Kafka {
                props, schema_registry, ..
            } => (props.secrets().into_iter())
                .chain(schema_registry.iter().flat_map(SchemaRegistryDto::secrets))
                .collect(),
            SourceKindDto::EventHubs {
                connection_string,
                sign_in,
                schema_registry,
                ..
            } => (connection_string.iter().map(|r| ("connectionString", r)))
                .chain(sign_in.secrets())
                .chain(schema_registry.iter().flat_map(SchemaRegistryDto::secrets))
                .collect(),
            SourceKindDto::S3Sqs { access, .. } | SourceKindDto::S3List { access, .. } => access.secrets(),
            SourceKindDto::AzureBlobQueue { access, .. } | SourceKindDto::AzureBlobList { access, .. } => {
                access.secrets()
            }
            SourceKindDto::GcsPubSub { access, .. } | SourceKindDto::GcsList { access, .. } => access.secrets(),
            SourceKindDto::Sftp { access, .. } => access.secrets(),
            SourceKindDto::HttpApi {
                credentials, sign_in, ..
            } => (credentials.values().map(|r| ("credentials", r)))
                .chain(sign_in.secrets())
                .collect(),
            SourceKindDto::OktaLogs { token, private_key, .. } => (token.iter().map(|r| ("token", r)))
                .chain(private_key.iter().map(|r| ("privateKey", r)))
                .collect(),
            SourceKindDto::HttpReceiver { tokens, .. }
            | SourceKindDto::OtlpReceiver { tokens, .. }
            | SourceKindDto::PrometheusReceiver { tokens, .. }
            | SourceKindDto::ElasticReceiver { tokens, .. } => tokens.values().map(|r| ("tokens", r)).collect(),
            SourceKindDto::HecReceiver { tokens, .. } => tokens.values().map(|t| ("tokens", &t.token)).collect(),
            SourceKindDto::S2sReceiver { tokens, .. } => tokens.values().map(|r| ("tokens", r)).collect(),
            SourceKindDto::Postgres(s) | SourceKindDto::MySql(s) => {
                s.password.iter().map(|r| ("password", r)).collect()
            }
            SourceKindDto::Metrics { .. } | SourceKindDto::Logs { .. } | SourceKindDto::SyslogReceiver { .. } => {
                Vec::new()
            }
        }
    }

    /// `secrets`, for the leader to move one onto a new version. Exhaustive as `secrets` is.
    pub fn secrets_mut(&mut self) -> Vec<&mut SecretRefDto> {
        match &mut self.kind {
            SourceKindDto::Kafka {
                props, schema_registry, ..
            } => (props.secrets_mut().into_iter())
                .chain(schema_registry.iter_mut().flat_map(SchemaRegistryDto::secrets_mut))
                .collect(),
            SourceKindDto::EventHubs {
                connection_string,
                sign_in,
                schema_registry,
                ..
            } => (connection_string.iter_mut().chain(sign_in.secrets_mut()))
                .chain(schema_registry.iter_mut().flat_map(SchemaRegistryDto::secrets_mut))
                .collect(),
            SourceKindDto::S3Sqs { access, .. } | SourceKindDto::S3List { access, .. } => access.secrets_mut(),
            SourceKindDto::AzureBlobQueue { access, .. } | SourceKindDto::AzureBlobList { access, .. } => {
                access.secrets_mut()
            }
            SourceKindDto::GcsPubSub { access, .. } | SourceKindDto::GcsList { access, .. } => access.secrets_mut(),
            SourceKindDto::Sftp { access, .. } => access.secrets_mut(),
            SourceKindDto::HttpApi {
                credentials, sign_in, ..
            } => credentials.values_mut().chain(sign_in.secrets_mut()).collect(),
            SourceKindDto::OktaLogs { token, private_key, .. } => token.iter_mut().chain(private_key).collect(),
            SourceKindDto::HttpReceiver { tokens, .. }
            | SourceKindDto::OtlpReceiver { tokens, .. }
            | SourceKindDto::PrometheusReceiver { tokens, .. }
            | SourceKindDto::ElasticReceiver { tokens, .. } => tokens.values_mut().collect(),
            SourceKindDto::HecReceiver { tokens, .. } => tokens.values_mut().map(|t| &mut t.token).collect(),
            SourceKindDto::S2sReceiver { tokens, .. } => tokens.values_mut().collect(),
            SourceKindDto::Postgres(s) | SourceKindDto::MySql(s) => s.password.iter_mut().collect(),
            SourceKindDto::Metrics { .. } | SourceKindDto::Logs { .. } | SourceKindDto::SyslogReceiver { .. } => {
                Vec::new()
            }
        }
    }
}

impl SourceKindDto {
    /// The registry a stream's Avro payloads name their schemas in.
    pub fn schema_registry(&self) -> Option<&SchemaRegistryDto> {
        match self {
            SourceKindDto::Kafka { schema_registry, .. } | SourceKindDto::EventHubs { schema_registry, .. } => {
                schema_registry.as_ref()
            }
            _ => None,
        }
    }

    /// How its payloads become its record, where it reads payloads.
    pub fn read(&self) -> Option<&ReadFormatDto> {
        match self {
            SourceKindDto::Kafka { read, .. }
            | SourceKindDto::EventHubs { read, .. }
            | SourceKindDto::S3Sqs { read, .. }
            | SourceKindDto::S3List { read, .. }
            | SourceKindDto::GcsList { read, .. }
            | SourceKindDto::AzureBlobList { read, .. }
            | SourceKindDto::Sftp { read, .. }
            | SourceKindDto::AzureBlobQueue { read, .. }
            | SourceKindDto::GcsPubSub { read, .. } => Some(read),
            _ => None,
        }
    }
}

impl SourceDto {
    /// Every address this source reaches over HTTP, by the field naming it. Empty from a kind that is not reached
    /// over HTTP.
    pub fn urls(&self) -> Vec<(&'static str, &str)> {
        match &self.kind {
            SourceKindDto::S3Sqs { queue_url, access, .. } => std::iter::once(("queueUrl", queue_url.as_str()))
                .chain(access.urls())
                .collect(),
            SourceKindDto::S3List { access, .. } => access.urls(),
            SourceKindDto::GcsList { access, .. } => access.urls(),
            SourceKindDto::AzureBlobList { access, .. } => access.urls(),
            SourceKindDto::AzureBlobQueue { queue_url, access, .. } => {
                std::iter::once(("queueUrl", queue_url.as_str()))
                    .chain(access.urls())
                    .collect()
            }
            SourceKindDto::GcsPubSub {
                pubsub_endpoint,
                access,
                ..
            } => std::iter::once(("pubsubEndpoint", pubsub_endpoint.as_deref().unwrap_or(PUBSUB_ENDPOINT)))
                .chain(access.urls())
                .collect(),
            SourceKindDto::HttpApi { url, sign_in, .. } => {
                std::iter::once(("url", url.as_str())).chain(sign_in.urls()).collect()
            }
            SourceKindDto::OktaLogs { org, .. } => vec![("org", org)],
            SourceKindDto::EventHubs {
                sign_in,
                schema_registry,
                ..
            } => (sign_in.urls().into_iter())
                .chain(schema_registry.iter().map(SchemaRegistryDto::url))
                .collect(),
            SourceKindDto::Kafka { schema_registry, .. } => {
                schema_registry.iter().map(SchemaRegistryDto::url).collect()
            }
            SourceKindDto::Sftp { .. }
            | SourceKindDto::Postgres(_)
            | SourceKindDto::MySql(_)
            | SourceKindDto::Metrics { .. }
            | SourceKindDto::Logs { .. }
            | SourceKindDto::HttpReceiver { .. }
            | SourceKindDto::HecReceiver { .. }
            | SourceKindDto::S2sReceiver { .. }
            | SourceKindDto::OtlpReceiver { .. }
            | SourceKindDto::PrometheusReceiver { .. }
            | SourceKindDto::ElasticReceiver { .. }
            | SourceKindDto::SyslogReceiver { .. } => Vec::new(),
        }
    }

    /// The TLS this source connects out or serves under. `None` from a kind that speaks none.
    pub fn tls(&self) -> Option<TlsDto> {
        match &self.kind {
            SourceKindDto::Kafka { tls, .. }
            | SourceKindDto::S3Sqs { tls, .. }
            | SourceKindDto::S3List { tls, .. }
            | SourceKindDto::GcsList { tls, .. }
            | SourceKindDto::AzureBlobList { tls, .. }
            | SourceKindDto::AzureBlobQueue { tls, .. }
            | SourceKindDto::GcsPubSub { tls, .. }
            | SourceKindDto::HttpApi { tls, .. }
            | SourceKindDto::OktaLogs { tls, .. } => Some(tls.clone()),
            SourceKindDto::Postgres(s) | SourceKindDto::MySql(s) => Some(s.tls.clone()),
            SourceKindDto::HttpReceiver { tls, .. }
            | SourceKindDto::HecReceiver { tls, .. }
            | SourceKindDto::S2sReceiver { tls, .. }
            | SourceKindDto::OtlpReceiver { tls, .. }
            | SourceKindDto::PrometheusReceiver { tls, .. }
            | SourceKindDto::ElasticReceiver { tls, .. }
            | SourceKindDto::SyslogReceiver { tls, .. } => Some(tls.tls()),
            SourceKindDto::EventHubs { .. }
            | SourceKindDto::Sftp { .. }
            | SourceKindDto::Metrics { .. }
            | SourceKindDto::Logs { .. } => None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase", deny_unknown_fields)]
pub enum SourceKindDto {
    #[serde(rename_all = "camelCase")]
    Kafka {
        brokers: String,
        topics: Vec<String>,
        /// The consumer group this source reads as. Absent is its worker group's own
        /// (`load::fill_default_group_ids`).
        #[serde(default, skip_serializing_if = "String::is_empty")]
        group_id: String,
        #[serde(flatten)]
        tls: TlsDto,
        #[serde(default, skip_serializing_if = "StartPositionDto::is_default")]
        start: StartPositionDto,
        /// `None` is an unbounded streaming source.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        stop: Option<StopPositionDto>,
        /// Unset means the worker sizes it to its memory (`worker::memory::fit`), `FetchBytes::DEFAULT` without a memory
        /// figure.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        fetch_bytes: Option<FetchBytes>,
        /// How this source's record bytes become text. See [`EncodingLabel`].
        #[serde(default, skip_serializing_if = "EncodingLabel::is_default")]
        encoding: EncodingLabel,
        /// Where its payloads are Avro: the registry holding each writer's schema.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        schema_registry: Option<SchemaRegistryDto>,
        #[serde(default, flatten)]
        read: ReadFormatDto,
        #[serde(default, flatten, skip_serializing_if = "KafkaPropsDto::is_empty")]
        props: KafkaPropsDto,
    },
    /// Azure Event Hubs, read through its Kafka endpoint: loading makes it a Kafka source signed in as `sign_in`
    /// says.
    #[serde(rename_all = "camelCase")]
    EventHubs {
        /// The namespace: its name in Azure's public cloud, its host name in another cloud, or `host:port`.
        namespace: String,
        /// The event hubs it reads, each a Kafka topic.
        event_hubs: Vec<String>,
        consumer_group: String,
        /// The namespace's or an event hub's connection string. At most one of it, `clientSecret` and
        /// `managedIdentityClientId`; none signs in as the host's managed identity.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        connection_string: Option<SecretRefDto>,
        #[serde(flatten)]
        sign_in: EntraSignInDto,
        #[serde(default, skip_serializing_if = "StartPositionDto::is_default")]
        start: StartPositionDto,
        /// `None` is an unbounded streaming source.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        stop: Option<StopPositionDto>,
        /// Unset means the worker sizes it to its memory (`worker::memory::fit`), `FetchBytes::DEFAULT` without a memory
        /// figure.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        fetch_bytes: Option<FetchBytes>,
        /// How this source's record bytes become text. See [`EncodingLabel`].
        #[serde(default, skip_serializing_if = "EncodingLabel::is_default")]
        encoding: EncodingLabel,
        /// Where its payloads are Avro: the registry holding each writer's schema.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        schema_registry: Option<SchemaRegistryDto>,
        #[serde(default, flatten)]
        read: ReadFormatDto,
    },
    /// The smeltr metrics source: this process's own observability counters (`metrics::MetricsSource`).
    #[serde(rename_all = "camelCase")]
    Metrics { bucket_seconds: u64 },
    /// The smeltr logs source: this process's own logs (`io::logs::LogsSource`).
    #[serde(rename_all = "camelCase")]
    Logs {
        /// The lowest severity it carries. See [`LevelLabel`].
        #[serde(default, skip_serializing_if = "LevelLabel::is_default")]
        min_level: LevelLabel,
    },
    /// The objects S3 announces on an SQS queue, from whichever bucket a notification names.
    #[serde(rename_all = "camelCase")]
    S3Sqs {
        /// The queue's own URL, which names its region and account.
        queue_url: String,
        #[serde(flatten)]
        access: S3AccessDto,
        #[serde(flatten)]
        tls: TlsDto,
        #[serde(flatten)]
        framing: ObjectFramingDto,
        #[serde(default, flatten)]
        read: ReadFormatDto,
        /// How this source's record bytes become text. See [`EncodingLabel`].
        #[serde(default, skip_serializing_if = "EncodingLabel::is_default")]
        encoding: EncodingLabel,
        #[serde(default, skip_serializing_if = "VisibilitySecs::is_default")]
        visibility_secs: VisibilitySecs,
    },
    /// The objects under a prefix of one S3 bucket, found by listing it every `pollIntervalSecs`. One worker reads
    /// it.
    ///
    /// Use `s3Sqs` when the bucket can notify. Otherwise delete or move each object after commit, so the bucket is
    /// its own queue. Keep with a ledger is for a bucket the reader may not change, and costs a full listing per
    /// poll.
    #[serde(rename_all = "camelCase")]
    S3List {
        bucket: String,
        /// Only keys that start with it. Empty is the whole bucket.
        #[serde(default, skip_serializing_if = "String::is_empty")]
        prefix: String,
        #[serde(flatten)]
        access: S3AccessDto,
        #[serde(flatten)]
        tls: TlsDto,
        #[serde(flatten)]
        framing: ObjectFramingDto,
        #[serde(default, flatten)]
        read: ReadFormatDto,
        /// How this source's record bytes become text. See [`EncodingLabel`].
        #[serde(default, skip_serializing_if = "EncodingLabel::is_default")]
        encoding: EncodingLabel,
        #[serde(default, skip_serializing_if = "PollIntervalSecs::is_default")]
        poll_interval_secs: PollIntervalSecs,
        #[serde(default, skip_serializing_if = "ListStartDto::is_default")]
        start: ListStartDto,
        #[serde(default, skip_serializing_if = "AfterCommitDto::is_default")]
        after_commit: AfterCommitDto,
        /// Where `"move"` puts an object, in place of `prefix`. Named with `"move"` and only then.
        /// Ends in `/` when `prefix` does or is empty, and only then, so a moved key keeps one `/` between them.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        move_prefix: Option<String>,
    },
    /// The objects under a prefix of one Cloud Storage bucket, found by listing it, as `s3List` reads S3. Use
    /// `gcsPubSub` when the bucket can notify.
    #[serde(rename_all = "camelCase")]
    GcsList {
        bucket: String,
        /// Only names that start with it. Empty is the whole bucket.
        #[serde(default, skip_serializing_if = "String::is_empty")]
        prefix: String,
        #[serde(flatten)]
        access: GcsAccessDto,
        #[serde(flatten)]
        tls: TlsDto,
        #[serde(flatten)]
        framing: ObjectFramingDto,
        #[serde(default, flatten)]
        read: ReadFormatDto,
        /// How this source's record bytes become text. See [`EncodingLabel`].
        #[serde(default, skip_serializing_if = "EncodingLabel::is_default")]
        encoding: EncodingLabel,
        #[serde(default, skip_serializing_if = "PollIntervalSecs::is_default")]
        poll_interval_secs: PollIntervalSecs,
        #[serde(default, skip_serializing_if = "ListStartDto::is_default")]
        start: ListStartDto,
        #[serde(default, skip_serializing_if = "AfterCommitDto::is_default")]
        after_commit: AfterCommitDto,
        /// Where `"move"` puts an object, in place of `prefix`, as for `s3List`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        move_prefix: Option<String>,
    },
    /// The blobs under a prefix of one Azure container, found by listing it, as `s3List` reads S3. A blob that
    /// changes after it is read is read again whole. Use `azureBlobQueue` when the account can notify.
    #[serde(rename_all = "camelCase")]
    AzureBlobList {
        container: String,
        /// Only names that start with it. Empty is the whole container.
        #[serde(default, skip_serializing_if = "String::is_empty")]
        prefix: String,
        #[serde(flatten)]
        access: AzureAccessDto,
        #[serde(flatten)]
        tls: TlsDto,
        #[serde(flatten)]
        framing: ObjectFramingDto,
        #[serde(default, flatten)]
        read: ReadFormatDto,
        /// How this source's record bytes become text. See [`EncodingLabel`].
        #[serde(default, skip_serializing_if = "EncodingLabel::is_default")]
        encoding: EncodingLabel,
        #[serde(default, skip_serializing_if = "PollIntervalSecs::is_default")]
        poll_interval_secs: PollIntervalSecs,
        #[serde(default, skip_serializing_if = "ListStartDto::is_default")]
        start: ListStartDto,
        #[serde(default, skip_serializing_if = "AfterCommitDto::is_default")]
        after_commit: AfterCommitDto,
        /// Where `"move"` puts an object, in place of `prefix`, as for `s3List`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        move_prefix: Option<String>,
    },
    /// The files under a prefix of one directory on an SFTP server, found by listing it, as `s3List` reads S3. A file
    /// that changes after it is read is read again whole.
    #[serde(rename_all = "camelCase")]
    Sftp {
        /// Absolute, or under the user's home directory. Empty is the home directory.
        #[serde(default, skip_serializing_if = "String::is_empty")]
        directory: String,
        /// Only paths under `directory` that start with it. Empty is all of it.
        #[serde(default, skip_serializing_if = "String::is_empty")]
        prefix: String,
        #[serde(flatten)]
        access: SftpAccessDto,
        #[serde(flatten)]
        framing: ObjectFramingDto,
        #[serde(default, flatten)]
        read: ReadFormatDto,
        /// How this source's record bytes become text. See [`EncodingLabel`].
        #[serde(default, skip_serializing_if = "EncodingLabel::is_default")]
        encoding: EncodingLabel,
        #[serde(default, skip_serializing_if = "PollIntervalSecs::is_default")]
        poll_interval_secs: PollIntervalSecs,
        #[serde(default, skip_serializing_if = "ListStartDto::is_default")]
        start: ListStartDto,
        #[serde(default, skip_serializing_if = "AfterCommitDto::is_default")]
        after_commit: AfterCommitDto,
        /// Where `"move"` puts a file, in place of `prefix`, as for `s3List`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        move_prefix: Option<String>,
    },
    /// The blobs Event Grid announces on an Azure Storage queue, from whichever container of the account a
    /// notification names.
    #[serde(rename_all = "camelCase")]
    AzureBlobQueue {
        /// The queue's own URL.
        queue_url: String,
        #[serde(flatten)]
        access: AzureAccessDto,
        #[serde(flatten)]
        tls: TlsDto,
        #[serde(flatten)]
        framing: ObjectFramingDto,
        #[serde(default, flatten)]
        read: ReadFormatDto,
        /// How this source's record bytes become text. See [`EncodingLabel`].
        #[serde(default, skip_serializing_if = "EncodingLabel::is_default")]
        encoding: EncodingLabel,
        #[serde(default, skip_serializing_if = "VisibilitySecs::is_default")]
        visibility_secs: VisibilitySecs,
        #[serde(default, skip_serializing_if = "MaxDeliveries::is_default")]
        max_deliveries: MaxDeliveries,
    },
    /// The objects Cloud Storage announces on a Pub/Sub subscription, from whichever bucket a notification names.
    #[serde(rename_all = "camelCase")]
    GcsPubSub {
        /// `projects/<project>/subscriptions/<subscription>`.
        subscription: String,
        /// Another Pub/Sub address, such as an emulator's. Absent is Google's.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pubsub_endpoint: Option<String>,
        #[serde(flatten)]
        access: GcsAccessDto,
        #[serde(flatten)]
        tls: TlsDto,
        #[serde(flatten)]
        framing: ObjectFramingDto,
        #[serde(default, flatten)]
        read: ReadFormatDto,
        /// How this source's record bytes become text. See [`EncodingLabel`].
        #[serde(default, skip_serializing_if = "EncodingLabel::is_default")]
        encoding: EncodingLabel,
        /// At most 600, Pub/Sub's longest ack deadline.
        #[serde(default, skip_serializing_if = "VisibilitySecs::is_default")]
        visibility_secs: VisibilitySecs,
    },
    /// What one address answers a GET, or a POST of a fixed body, with, asked every `pollIntervalSecs`. One worker
    /// reads it.
    ///
    /// For an API that answers with JSON or lines of text. An API with its own kind is read through that kind.
    #[serde(rename_all = "camelCase")]
    HttpApi {
        /// The whole address asked, with any fixed query parameters.
        url: String,
        /// `"GET"` (default) or `"POST"`.
        #[serde(default, skip_serializing_if = "ApiMethodDto::is_default")]
        method: ApiMethodDto,
        /// The JSON every POST carries. Absent is a POST of nothing.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        body: Option<serde_json::Value>,
        /// Header name -> its value, committed to the config repository.
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        headers: BTreeMap<String, String>,
        /// Header name -> the credential holding its value.
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        credentials: BTreeMap<String, SecretRefDto>,
        #[serde(flatten)]
        tls: TlsDto,
        /// `"body"` (default), `"lines"` or `"array"`.
        #[serde(default, skip_serializing_if = "ApiRecordsDto::is_default")]
        records: ApiRecordsDto,
        /// The JSON pointer to the array `"array"` reads, such as `/data`. Absent is an answer that is the array.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        records_at: Option<String>,
        /// Whether a poll follows each answer's `Link: rel="next"` to the next page.
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        follow_links: bool,
        /// The query parameter that asks for what happened since a time, sent the start of the last poll whose
        /// records every destination took. Absent reads everything every poll.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        since_param: Option<String>,
        /// How many seconds before the last poll's start `sinceParam` asks from, for an API that records late.
        #[serde(default, skip_serializing_if = "is_zero")]
        since_overlap_secs: u64,
        #[serde(default, skip_serializing_if = "PollIntervalSecs::is_default")]
        poll_interval_secs: PollIntervalSecs,
        /// What the first poll asks for, with `sinceParam`.
        #[serde(default, skip_serializing_if = "ListStartDto::is_default")]
        start: ListStartDto,
        /// Signing in as an app for the token every request carries.
        #[serde(flatten)]
        sign_in: AppSignInDto,
    },
    /// An Okta org's System Log, every event once, in the order Okta stored them. One worker reads it.
    #[serde(rename_all = "camelCase")]
    OktaLogs {
        /// The org's address, as `https://acme.okta.com`.
        org: String,
        /// The credential holding an API token with read access to the System Log. A source names a token or a
        /// service app, never both.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        token: Option<SecretRefDto>,
        /// The service app's client id, when the source signs in as a service app.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        client_id: Option<String>,
        /// The credential holding the service app's private key, a PEM RSA key.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        private_key: Option<SecretRefDto>,
        /// Which of the service app's keys `privateKey` is, for an app with several.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        key_id: Option<String>,
        #[serde(flatten)]
        tls: TlsDto,
        #[serde(default, skip_serializing_if = "PollIntervalSecs::is_default")]
        poll_interval_secs: PollIntervalSecs,
        /// `"everything"` (default) reads what Okta lists with no start, its last seven days; `"new"` reads what
        /// happens from the first poll on.
        #[serde(default, skip_serializing_if = "ListStartDto::is_default")]
        start: ListStartDto,
    },
    /// The rows a query returns from a PostgreSQL database, each a record whose fields are the result's columns. One
    /// worker reads it, and the leader keeps how far it has read.
    #[serde(deserialize_with = "SqlSourceDto::postgres")]
    Postgres(SqlSourceDto),
    /// The rows a query returns from a MySQL or MariaDB database, read as a `postgres` source reads its own.
    #[serde(rename = "mysql", deserialize_with = "SqlSourceDto::mysql")]
    MySql(SqlSourceDto),
    /// What senders POST to `path` on `port`. Every worker of the group listens; a request is answered once every
    /// destination has its records.
    #[serde(rename_all = "camelCase")]
    HttpReceiver {
        #[serde(flatten)]
        listener: ReceiverSettingsDto,
        #[serde(flatten)]
        tls: ServedTlsDto,
        /// The path senders POST to.
        #[serde(default = "root_path", skip_serializing_if = "is_root_path")]
        path: String,
        /// Token name -> the credential holding a token a sender may present as `Authorization: Bearer`.
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        tokens: BTreeMap<String, SecretRefDto>,
        /// `"body"` (default), `"lines"` or `"array"`.
        #[serde(default, skip_serializing_if = "ApiRecordsDto::is_default")]
        records: ApiRecordsDto,
        /// The JSON pointer to the array `"array"` reads, such as `/events`. Absent is a body that is the array.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        records_at: Option<String>,
    },
    /// What Splunk HTTP Event Collector clients send to `port`, at the paths Splunk serves. Every worker of the group
    /// listens; a request is answered once every destination has its records.
    #[serde(rename_all = "camelCase")]
    HecReceiver {
        #[serde(flatten)]
        listener: ReceiverSettingsDto,
        #[serde(flatten)]
        tls: ServedTlsDto,
        /// Token name -> a token a sender may present as `Authorization: Splunk`, and whether its senders use
        /// indexer acknowledgement.
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        tokens: BTreeMap<String, HecTokenDto>,
        /// Sourcetype -> where a raw request's events end, by the request's `sourcetype`. A sourcetype not named
        /// here is an event per line.
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        sourcetypes: BTreeMap<String, SourcetypeDto>,
    },
    /// What Splunk forwarders send to `port` over Splunk-to-Splunk, as to an indexer. Every worker of the group
    /// listens, and each connection is one forwarder's. `maxRequestBytes` bounds one frame and one record.
    #[serde(rename_all = "camelCase")]
    S2sReceiver {
        #[serde(flatten)]
        listener: ReceiverSettingsDto,
        #[serde(flatten)]
        tls: ServedTlsDto,
        /// Token name -> the credential holding a token a forwarder may present, its outputs.conf `token`. A
        /// forwarder presenting none, or another, is cut off.
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        tokens: BTreeMap<String, SecretRefDto>,
        /// Sourcetype -> where its events end. A sourcetype not named here is an event per line.
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        sourcetypes: BTreeMap<String, SourcetypeDto>,
    },

    /// What OpenTelemetry exporters send to `port` over OTLP, over HTTP as protobuf or JSON and over gRPC: the
    /// exports of one `signal`, one record per log record, data point or span. The receivers of a group's other
    /// signals may share its port, as an exporter sends every signal to one. Every worker of the group listens; an
    /// export is answered once every destination has its records.
    #[serde(rename_all = "camelCase")]
    OtlpReceiver {
        #[serde(flatten)]
        listener: ReceiverSettingsDto,
        #[serde(flatten)]
        tls: ServedTlsDto,
        /// The signal it takes. Absent is logs.
        #[serde(default, skip_serializing_if = "is_logs")]
        signal: Signal,
        /// Token name -> the credential holding a token an exporter may present as `Authorization: Bearer`.
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        tokens: BTreeMap<String, SecretRefDto>,
    },
    /// What Prometheus and the agents speaking its remote_write protocol send to `port`, at `/api/v1/write`, one
    /// record per sample. Every worker of the group listens; a request is answered once every destination has its
    /// records.
    #[serde(rename_all = "camelCase")]
    PrometheusReceiver {
        #[serde(flatten)]
        listener: ReceiverSettingsDto,
        #[serde(flatten)]
        tls: ServedTlsDto,
        /// Token name -> the credential holding a token a sender may present as `Authorization: Bearer`.
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        tokens: BTreeMap<String, SecretRefDto>,
    },
    /// What Elasticsearch clients and shippers send to `port` through the `_bulk` API, at `/_bulk` and
    /// `/{index}/_bulk`, one record per document. Every worker of the group listens; a request is answered once
    /// every destination has its records.
    #[serde(rename_all = "camelCase")]
    ElasticReceiver {
        #[serde(flatten)]
        listener: ReceiverSettingsDto,
        #[serde(flatten)]
        tls: ServedTlsDto,
        /// Token name -> the credential holding a token a sender may present as `Authorization: Bearer`, as
        /// `Authorization: ApiKey` (the key as sent, or the `id:key` it encodes), or as the password of
        /// `Authorization: Basic`, whose user is not read.
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        tokens: BTreeMap<String, SecretRefDto>,
    },
    /// What syslog senders send to `port`, as RFC 5424 or RFC 3164 messages: over TCP framed by octet count or ended
    /// by a newline (RFC 6587), under TLS (RFC 5425) when it names a certificate; over UDP one to a datagram (RFC 5426).
    /// Every worker of the group listens, and each message is one record. Syslog has no acknowledgement, so delivery
    /// is best-effort. A full receiver stops reading TCP, so the sender's window fills, and drops UDP.
    #[serde(rename_all = "camelCase")]
    SyslogReceiver {
        #[serde(flatten)]
        listener: ReceiverSettingsDto,
        #[serde(flatten)]
        tls: ServedTlsDto,
        /// The networks a sender may connect from, as CIDR blocks such as `10.0.0.0/8`. Syslog carries no token, so
        /// this is what admits a sender, unless the receiver names authorities a sender's certificate
        /// must chain to.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        allow_from: Vec<String>,
        /// How a message's bytes become text. Syslog over TCP names no charset, and older senders write their own.
        /// See [`EncodingLabel`].
        #[serde(default, skip_serializing_if = "EncodingLabel::is_default")]
        encoding: EncodingLabel,
        /// What senders send over. Absent is TCP. Over UDP each datagram is a message, served without TLS.
        #[serde(default, skip_serializing_if = "Transport::is_tcp")]
        transport: Transport,
        /// A pattern whose first capture group is where a message over TCP ends, the group dropped, as Splunk's
        /// `LINE_BREAKER`, for senders that end a message with something other than a newline, or with nothing.
        /// Absent, a message ends at a newline. Octet counting still frames a message that starts with its length.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        line_breaker: Option<String>,
    },
}

fn is_logs(signal: &Signal) -> bool {
    *signal == Signal::Logs
}

impl SourceKindDto {
    /// The port a receiver listens on, or `None` for a source that connects out.
    pub fn receiver_port(&self) -> Option<u16> {
        match self {
            SourceKindDto::HttpReceiver { listener, .. }
            | SourceKindDto::HecReceiver { listener, .. }
            | SourceKindDto::S2sReceiver { listener, .. }
            | SourceKindDto::OtlpReceiver { listener, .. }
            | SourceKindDto::PrometheusReceiver { listener, .. }
            | SourceKindDto::ElasticReceiver { listener, .. }
            | SourceKindDto::SyslogReceiver { listener, .. } => Some(listener.port),
            _ => None,
        }
    }

    /// What a receiver's senders send over. TCP for every kind but a syslog receiver given UDP.
    pub fn receiver_transport(&self) -> Transport {
        match self {
            SourceKindDto::SyslogReceiver { transport, .. } => *transport,
            _ => Transport::Tcp,
        }
    }
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

/// A token a HEC receiver admits.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HecTokenDto {
    /// The credential holding the token.
    pub token: SecretRefDto,
    /// Whether its senders use indexer acknowledgement, as Splunk's `useACK`: each request names a channel and is
    /// answered at once with an `ackId`. Absent, a request is held until delivered. Two names holding the same token
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

fn root_path() -> String {
    "/".to_string()
}

fn is_root_path(path: &str) -> bool {
    path == "/"
}

/// The largest request body a receiver reads.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
pub struct MaxRequestBytes(pub u64);

impl MaxRequestBytes {
    pub const DEFAULT: u64 = 10 * 1024 * 1024;
    pub const MIN: u64 = 1024;
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

fn is_zero(n: &u64) -> bool {
    *n == 0
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
    /// What records from this kind carry, the same answer `plan::topology::SourceKind::meta` gives once a source is
    /// loaded. Stated here too because a capture filter compiles against a source the author has only written down:
    /// nothing has loaded it, so there is no `SourceKind` to ask.
    pub fn meta(&self) -> crate::plan::topology::SourceMeta {
        use crate::plan::topology::SourceMeta;
        let declared = match self {
            SourceKindDto::Kafka { .. } => SourceMeta::KAFKA,
            SourceKindDto::EventHubs { .. } => SourceMeta::EVENT_HUBS,
            SourceKindDto::Metrics { .. } => SourceMeta::METRICS,
            SourceKindDto::Logs { .. } => SourceMeta::LOGS,
            SourceKindDto::S3Sqs { .. } => SourceMeta::S3_SQS,
            SourceKindDto::S3List { .. } => SourceMeta::S3_LIST,
            SourceKindDto::GcsList { .. } => SourceMeta::GCS_LIST,
            SourceKindDto::AzureBlobList { .. } => SourceMeta::AZURE_BLOB_LIST,
            SourceKindDto::Sftp { .. } => SourceMeta::SFTP,
            SourceKindDto::AzureBlobQueue { .. } => SourceMeta::AZURE_BLOB_QUEUE,
            SourceKindDto::GcsPubSub { .. } => SourceMeta::GCS_PUB_SUB,
            SourceKindDto::HttpApi { .. } => SourceMeta::HTTP_API,
            SourceKindDto::OktaLogs { .. } => SourceMeta::OKTA_LOGS,
            SourceKindDto::HttpReceiver { .. } => SourceMeta::HTTP_RECEIVER,
            SourceKindDto::HecReceiver { .. } => SourceMeta::HEC_RECEIVER,
            SourceKindDto::S2sReceiver { .. } => SourceMeta::S2S_RECEIVER,
            SourceKindDto::OtlpReceiver { signal, .. } => SourceMeta::otlp(*signal),
            SourceKindDto::PrometheusReceiver { .. } => SourceMeta::prometheus_receiver(),
            SourceKindDto::ElasticReceiver { .. } => SourceMeta::ELASTIC_RECEIVER,
            SourceKindDto::SyslogReceiver { .. } => SourceMeta::SYSLOG_RECEIVER,
            SourceKindDto::Postgres(sql) => sql.meta(SqlKind::Postgres),
            SourceKindDto::MySql(sql) => sql.meta(SqlKind::MySql),
        };
        let declared = match self.read() {
            Some(read) => read.meta(declared),
            None => declared,
        };
        declared.with_encoding(self.encoding())
    }

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

    /// Whether one worker of the group reads this source at a time, the same answer
    /// `plan::topology::SourceKind::single_worker` gives. Asked of a source on its way to a capture, which then goes
    /// to one worker too.
    pub fn single_worker(&self) -> bool {
        match self {
            SourceKindDto::S3List { .. }
            | SourceKindDto::GcsList { .. }
            | SourceKindDto::AzureBlobList { .. }
            | SourceKindDto::Sftp { .. }
            | SourceKindDto::HttpApi { .. }
            | SourceKindDto::OktaLogs { .. }
            | SourceKindDto::Postgres(_)
            | SourceKindDto::MySql(_) => true,
            SourceKindDto::Kafka { .. }
            | SourceKindDto::EventHubs { .. }
            | SourceKindDto::S3Sqs { .. }
            | SourceKindDto::AzureBlobQueue { .. }
            | SourceKindDto::GcsPubSub { .. }
            | SourceKindDto::Metrics { .. }
            | SourceKindDto::Logs { .. }
            | SourceKindDto::HttpReceiver { .. }
            | SourceKindDto::HecReceiver { .. }
            | SourceKindDto::S2sReceiver { .. }
            | SourceKindDto::OtlpReceiver { .. }
            | SourceKindDto::SyslogReceiver { .. }
            | SourceKindDto::PrometheusReceiver { .. }
            | SourceKindDto::ElasticReceiver { .. } => false,
        }
    }

    /// How this source's record bytes become text, the same answer `plan::topology::SourceKind::encoding` gives.
    /// A label validation would have refused falls back to detection rather than failing here: this is asked of a
    /// source on its way to a capture, where refusing would lose records over a spelling.
    pub fn encoding(&self) -> crate::format::encoding::Encoding {
        use crate::format::encoding::Encoding;
        match self {
            SourceKindDto::Kafka { encoding, .. }
            | SourceKindDto::EventHubs { encoding, .. }
            | SourceKindDto::S3Sqs { encoding, .. }
            | SourceKindDto::S3List { encoding, .. }
            | SourceKindDto::GcsList { encoding, .. }
            | SourceKindDto::AzureBlobList { encoding, .. }
            | SourceKindDto::Sftp { encoding, .. }
            | SourceKindDto::AzureBlobQueue { encoding, .. }
            | SourceKindDto::GcsPubSub { encoding, .. }
            | SourceKindDto::SyslogReceiver { encoding, .. } => Encoding::for_label(&encoding.0).unwrap_or_default(),
            SourceKindDto::Metrics { .. }
            | SourceKindDto::Logs { .. }
            | SourceKindDto::HttpApi { .. }
            | SourceKindDto::OktaLogs { .. }
            | SourceKindDto::Postgres(_)
            | SourceKindDto::MySql(_)
            | SourceKindDto::HttpReceiver { .. }
            | SourceKindDto::HecReceiver { .. }
            | SourceKindDto::S2sReceiver { .. }
            | SourceKindDto::OtlpReceiver { .. }
            | SourceKindDto::PrometheusReceiver { .. }
            | SourceKindDto::ElasticReceiver { .. } => Encoding::Fixed(encoding_rs::UTF_8),
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

impl DestinationDto {
    /// Every address this destination reaches over HTTP: the service, and where it obtains credentials when that
    /// is configured apart, by the field naming each. Empty from a kind that is not reached over HTTP.
    pub fn urls(&self) -> Vec<(&'static str, &str)> {
        match &self.kind {
            DestinationKindDto::Kafka { schema_registry, .. } => schema_registry.iter().map(|r| r.url()).collect(),
            DestinationKindDto::EventHubs {
                sign_in,
                schema_registry,
                ..
            } => (sign_in.urls().into_iter())
                .chain(schema_registry.iter().map(|r| r.url()))
                .collect(),
            DestinationKindDto::Elasticsearch { url, zone_urls, .. }
            | DestinationKindDto::SplunkHec { url, zone_urls, .. }
            | DestinationKindDto::Http { url, zone_urls, .. } => std::iter::once(("url", url.as_str()))
                .chain(zone_urls.values().map(|u| ("zoneUrls", u.as_str())))
                .collect(),
            DestinationKindDto::S3 { access, .. } => access.urls(),
            DestinationKindDto::Gcs { access, .. } => access.urls(),
            DestinationKindDto::AzureBlob { access, .. } => access.urls(),
            DestinationKindDto::Sftp { .. } => Vec::new(),
            DestinationKindDto::Prometheus { url, .. } => vec![("url", url)],
            DestinationKindDto::LogAnalytics { endpoint, sign_in, .. } => {
                std::iter::once(("endpoint", endpoint.as_str()))
                    .chain(sign_in.urls())
                    .collect()
            }
            DestinationKindDto::Iceberg {
                catalog,
                token_url,
                access,
                ..
            } => {
                let mut urls = vec![("catalog", catalog.as_str())];
                urls.extend(token_url.as_deref().map(|u| ("tokenUrl", u)));
                if !access.is_empty() {
                    urls.extend(access.urls());
                }
                urls
            }
            // A Unity Catalog workspace is held to more than a URL, a root alone, by its own check.
            DestinationKindDto::Delta { access, .. } => access.urls(),
        }
    }

    /// Every credential this destination names, by the field naming it. Exhaustive on purpose: a kind added here
    /// must answer, or its credentials would go unnamed, unchecked at deploy and left behind on a version move.
    pub fn secrets(&self) -> Vec<(&'static str, &SecretRefDto)> {
        match &self.kind {
            DestinationKindDto::Kafka {
                props, schema_registry, ..
            } => (props.secrets().into_iter())
                .chain(schema_registry.iter().flat_map(|r| r.secrets()))
                .collect(),
            DestinationKindDto::EventHubs {
                connection_string,
                sign_in,
                schema_registry,
                ..
            } => (connection_string.iter().map(|r| ("connectionString", r)))
                .chain(sign_in.secrets())
                .chain(schema_registry.iter().flat_map(|r| r.secrets()))
                .collect(),
            DestinationKindDto::Elasticsearch { auth, password, .. } => (auth.iter().map(|r| ("auth", r)))
                .chain(password.iter().map(|r| ("password", r)))
                .collect(),
            DestinationKindDto::SplunkHec { token, .. } => vec![("token", token)],
            DestinationKindDto::S3 { access, .. } => access.secrets(),
            DestinationKindDto::Gcs { access, .. } => access.secrets(),
            DestinationKindDto::AzureBlob { access, .. } => access.secrets(),
            DestinationKindDto::Sftp { access, .. } => access.secrets(),
            DestinationKindDto::Http { credentials, .. } | DestinationKindDto::Prometheus { credentials, .. } => {
                credentials.values().map(|r| ("credentials", r)).collect()
            }
            DestinationKindDto::LogAnalytics { sign_in, .. } => sign_in.secrets(),
            DestinationKindDto::Iceberg {
                token,
                client_secret,
                access,
                ..
            } => (token.iter().map(|r| ("token", r)))
                .chain(client_secret.iter().map(|r| ("clientSecret", r)))
                .chain(access.secrets())
                .collect(),
            DestinationKindDto::Delta {
                unity_catalog, access, ..
            } => (unity_catalog
                .iter()
                .map(|u| ("unityCatalog.clientSecret", &u.client_secret)))
            .chain(access.secrets())
            .collect(),
        }
    }

    /// `secrets`, for the leader to move one onto a new version. Exhaustive as `secrets` is.
    pub fn secrets_mut(&mut self) -> Vec<&mut SecretRefDto> {
        match &mut self.kind {
            DestinationKindDto::Kafka {
                props, schema_registry, ..
            } => (props.secrets_mut().into_iter())
                .chain(schema_registry.iter_mut().flat_map(|r| r.secrets_mut()))
                .collect(),
            DestinationKindDto::EventHubs {
                connection_string,
                sign_in,
                schema_registry,
                ..
            } => (connection_string.iter_mut().chain(sign_in.secrets_mut()))
                .chain(schema_registry.iter_mut().flat_map(|r| r.secrets_mut()))
                .collect(),
            DestinationKindDto::Elasticsearch { auth, password, .. } => {
                auth.iter_mut().chain(password.iter_mut()).collect()
            }
            DestinationKindDto::SplunkHec { token, .. } => vec![token],
            DestinationKindDto::S3 { access, .. } => access.secrets_mut(),
            DestinationKindDto::Gcs { access, .. } => access.secrets_mut(),
            DestinationKindDto::AzureBlob { access, .. } => access.secrets_mut(),
            DestinationKindDto::Sftp { access, .. } => access.secrets_mut(),
            DestinationKindDto::Http { credentials, .. } | DestinationKindDto::Prometheus { credentials, .. } => {
                credentials.values_mut().collect()
            }
            DestinationKindDto::LogAnalytics { sign_in, .. } => sign_in.secrets_mut(),
            DestinationKindDto::Iceberg {
                token,
                client_secret,
                access,
                ..
            } => (token.iter_mut().chain(client_secret.iter_mut()))
                .chain(access.secrets_mut())
                .collect(),
            DestinationKindDto::Delta {
                unity_catalog, access, ..
            } => (unity_catalog.iter_mut().map(|u| &mut u.client_secret))
                .chain(access.secrets_mut())
                .collect(),
        }
    }

    /// The TLS this destination connects out under. `None` from a kind that speaks none.
    pub fn tls(&self) -> Option<TlsDto> {
        match &self.kind {
            DestinationKindDto::Kafka { tls, .. }
            | DestinationKindDto::Elasticsearch { tls, .. }
            | DestinationKindDto::SplunkHec { tls, .. }
            | DestinationKindDto::S3 { tls, .. }
            | DestinationKindDto::Gcs { tls, .. }
            | DestinationKindDto::AzureBlob { tls, .. }
            | DestinationKindDto::Http { tls, .. }
            | DestinationKindDto::Prometheus { tls, .. }
            | DestinationKindDto::LogAnalytics { tls, .. }
            | DestinationKindDto::Iceberg { tls, .. }
            | DestinationKindDto::Delta { tls, .. } => Some(tls.clone()),
            DestinationKindDto::EventHubs { .. } | DestinationKindDto::Sftp { .. } => None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase", deny_unknown_fields)]
pub enum DestinationKindDto {
    #[serde(rename_all = "camelCase")]
    Kafka {
        brokers: String,
        default_topic: String,
        #[serde(flatten)]
        tls: TlsDto,
        #[serde(default, skip_serializing_if = "FormatDto::is_default")]
        format: FormatDto,
        /// The field `"text"` or `"bytes"` writes. Absent is `_raw` for `"text"`, `__source.bytes` for `"bytes"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        field: Option<String>,
        #[serde(default, skip_serializing_if = "CompressionDto::is_default")]
        compression: CompressionDto,
        #[serde(default, skip_serializing_if = "AcksDto::is_default")]
        acks: AcksDto,
        /// Seconds one batch may take to deliver before it is a stall.
        #[serde(default, skip_serializing_if = "StallAfter::is_default")]
        stall_after_secs: StallAfter,
        /// Unset means the worker sizes it to its memory (`worker::memory::fit`), `BufferBytes::DEFAULT` without a
        /// memory figure.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        buffer_bytes: Option<BufferBytes>,
        /// Where `format` is `"avro"`: the registry each route's record is registered with.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        schema_registry: Option<SchemaRegistryDto>,
        #[serde(default, flatten, skip_serializing_if = "KafkaPropsDto::is_empty")]
        props: KafkaPropsDto,
    },
    /// Azure Event Hubs, written through its Kafka endpoint: loading makes it a Kafka destination signed in as
    /// `sign_in` says.
    #[serde(rename_all = "camelCase")]
    EventHubs {
        /// The namespace: its name in Azure's public cloud, its host name in another cloud, or `host:port`.
        namespace: String,
        /// The event hub a row goes to when it names none, as a Kafka destination's default topic.
        default_event_hub: String,
        /// The namespace's or an event hub's connection string. At most one of it, `clientSecret` and
        /// `managedIdentityClientId`; none signs in as the host's managed identity.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        connection_string: Option<SecretRefDto>,
        #[serde(flatten)]
        sign_in: EntraSignInDto,
        #[serde(default, skip_serializing_if = "FormatDto::is_default")]
        format: FormatDto,
        /// The field `"text"` or `"bytes"` writes. Absent is `_raw` for `"text"`, `__source.bytes` for `"bytes"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        field: Option<String>,
        /// None or gzip: Event Hubs refuses the other codecs.
        #[serde(default, skip_serializing_if = "CompressionDto::is_default")]
        compression: CompressionDto,
        /// Seconds one batch may take to deliver before it is a stall.
        #[serde(default, skip_serializing_if = "StallAfter::is_default")]
        stall_after_secs: StallAfter,
        /// As a Kafka destination's.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        buffer_bytes: Option<BufferBytes>,
        /// As a Kafka destination's.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        schema_registry: Option<SchemaRegistryDto>,
    },
    #[serde(rename_all = "camelCase")]
    Elasticsearch {
        url: String,
        /// Zone -> the address a worker in that zone sends to instead of `url`.
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        zone_urls: BTreeMap<String, String>,
        default_index: String,
        #[serde(flatten)]
        tls: TlsDto,
        /// The credential holding the API key or a whole `Authorization` header value, never the value itself.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        auth: Option<SecretRefDto>,
        /// Basic auth, in place of `auth`: `username` and `password` together.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        username: Option<String>,
        /// The credential holding `username`'s password.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        password: Option<SecretRefDto>,
        /// `"auto"` (default), `"dedupe"`, or `"overwrite"`; see `plan::topology::DocIdPolicy`.
        #[serde(default, skip_serializing_if = "DocIdPolicyDto::is_default")]
        doc_id: DocIdPolicyDto,
        /// The row field holding the document identity. Omitted, the pipeline must set `__destination.id`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        id_field: Option<String>,
        #[serde(default, skip_serializing_if = "InFlight::is_default")]
        in_flight: InFlight,
        #[serde(default, skip_serializing_if = "BulkBytes::is_default")]
        max_bulk_bytes: BulkBytes,
        /// Unset means the default, or one whole bulk request if that is larger: raising `max_bulk_bytes` alone must
        /// never leave a destination unable to hand a request over.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        buffer_bytes: Option<BufferBytes>,
        /// Seconds one batch may take to deliver before it is a stall.
        #[serde(default, skip_serializing_if = "StallAfter::is_default")]
        stall_after_secs: StallAfter,
        /// Whether a worker learns the cluster's nodes from `url` and spreads its requests across them.
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        sniff: bool,
        /// The node attribute naming a node's zone, which sniffing prefers the worker's own of. Absent is `zone`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        zone_attribute: Option<String>,
    },
    /// Events sent to a Splunk HTTP Event Collector.
    #[serde(rename_all = "camelCase")]
    SplunkHec {
        /// The collector's base address; the destination adds the endpoint paths itself.
        url: String,
        /// Zone -> the address a worker in that zone sends to instead of `url`.
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        zone_urls: BTreeMap<String, String>,
        /// The credential holding the HEC token, never the token itself.
        token: SecretRefDto,
        /// Where a row with no `__destination.index` goes. Absent leaves the token's own default index. The raw
        /// endpoint files every row here, so it refuses a row whose `__destination.index` names another.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        index: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        sourcetype: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        source: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        host: Option<String>,
        /// `"event"` (default) or `"raw"`; see `plan::topology::Endpoint`.
        #[serde(default, skip_serializing_if = "EndpointDto::is_default")]
        endpoint: EndpointDto,
        /// `"json"` (default) or `"text"`; the raw endpoint also takes `"bytes"`.
        #[serde(default, skip_serializing_if = "FormatDto::is_default")]
        format: FormatDto,
        /// The field `"text"` or `"bytes"` writes. Absent is `_raw` for `"text"`, `__source.bytes` for `"bytes"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        field: Option<String>,
        /// `"indexer"` (default) or `"receipt"`; see `plan::topology::Acknowledgement`.
        #[serde(default, skip_serializing_if = "AcknowledgementDto::is_default")]
        acknowledgement: AcknowledgementDto,
        #[serde(flatten)]
        tls: TlsDto,
        #[serde(default, skip_serializing_if = "InFlight::is_default")]
        in_flight: InFlight,
        #[serde(default, skip_serializing_if = "RequestBytes::is_default")]
        max_request_bytes: RequestBytes,
        /// Unset means the default, or one whole request if that is larger.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        buffer_bytes: Option<BufferBytes>,
        /// Seconds one batch may take to deliver, its acknowledgement included, before it is a stall.
        #[serde(default, skip_serializing_if = "StallAfter::is_default")]
        stall_after_secs: StallAfter,
    },
    /// Objects written to an S3 bucket, one open file per prefix.
    #[serde(rename_all = "camelCase")]
    S3 {
        bucket: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        kms_key_id: Option<String>,
        #[serde(flatten)]
        access: S3AccessDto,
        #[serde(flatten)]
        objects: ObjectSettingsDto,
        #[serde(flatten)]
        cloud: CloudSettingsDto,
        #[serde(flatten)]
        tls: TlsDto,
    },
    /// Objects written to a Google Cloud Storage bucket, one open file per prefix.
    #[serde(rename_all = "camelCase")]
    Gcs {
        bucket: String,
        #[serde(flatten)]
        access: GcsAccessDto,
        #[serde(flatten)]
        objects: ObjectSettingsDto,
        #[serde(flatten)]
        cloud: CloudSettingsDto,
        #[serde(flatten)]
        tls: TlsDto,
    },
    /// Blobs written to an Azure Blob Storage container, one open file per prefix.
    #[serde(rename_all = "camelCase")]
    AzureBlob {
        container: String,
        #[serde(flatten)]
        access: AzureAccessDto,
        #[serde(flatten)]
        objects: ObjectSettingsDto,
        #[serde(flatten)]
        cloud: CloudSettingsDto,
        #[serde(flatten)]
        tls: TlsDto,
    },
    /// Files written to one directory of an SFTP server, one open file per prefix, each renamed into place whole.
    #[serde(rename_all = "camelCase")]
    Sftp {
        /// Absolute, or under the user's home directory. Empty is the home directory.
        #[serde(default, skip_serializing_if = "String::is_empty")]
        directory: String,
        #[serde(flatten)]
        access: SftpAccessDto,
        #[serde(flatten)]
        objects: ObjectSettingsDto,
    },
    /// Rows POSTed to one URL, as lines, a JSON array or one per request.
    #[serde(rename_all = "camelCase")]
    Http {
        /// The whole address each request is POSTed to.
        url: String,
        /// Zone -> the address a worker in that zone sends to instead of `url`.
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        zone_urls: BTreeMap<String, String>,
        #[serde(default, skip_serializing_if = "RequestBodyDto::is_default")]
        body: RequestBodyDto,
        /// `"json"` (default), `"text"` or `"bytes"`; bytes are not a JSON array's element.
        #[serde(default, skip_serializing_if = "FormatDto::is_default")]
        format: FormatDto,
        /// The field `"text"` or `"bytes"` writes. Absent is `_raw` for `"text"`, `__source.bytes` for `"bytes"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        field: Option<String>,
        /// `"none"` (default) or `"gzip"`.
        #[serde(default, skip_serializing_if = "CompressionDto::is_default")]
        compression: CompressionDto,
        /// Header name -> its value, committed to the config repository.
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        headers: BTreeMap<String, String>,
        /// Header name -> the credential holding its value.
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        credentials: BTreeMap<String, SecretRefDto>,
        #[serde(flatten)]
        tls: TlsDto,
        #[serde(default, skip_serializing_if = "InFlight::is_default")]
        in_flight: InFlight,
        #[serde(default, skip_serializing_if = "HttpRequestBytes::is_default")]
        max_request_bytes: HttpRequestBytes,
        /// Unset means the default, or one whole request if that is larger.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        buffer_bytes: Option<BufferBytes>,
        /// Seconds one batch may take to deliver before it is a stall.
        #[serde(default, skip_serializing_if = "StallAfter::is_default")]
        stall_after_secs: StallAfter,
    },
    /// Samples sent as Prometheus remote_write to a store's endpoint.
    #[serde(rename_all = "camelCase")]
    Prometheus {
        /// The whole address each request is POSTed to.
        url: String,
        /// `"1.0"` (default) or `"2.0"`.
        #[serde(default, skip_serializing_if = "RemoteWriteVersionDto::is_default")]
        version: RemoteWriteVersionDto,
        /// Header name -> its value, committed to the config repository.
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        headers: BTreeMap<String, String>,
        /// Header name -> the credential holding its value.
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        credentials: BTreeMap<String, SecretRefDto>,
        #[serde(flatten)]
        tls: TlsDto,
        #[serde(default, skip_serializing_if = "RequestBytes::is_default")]
        max_request_bytes: RequestBytes,
        /// Unset means the default, or one whole request if that is larger.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        buffer_bytes: Option<BufferBytes>,
        /// Seconds one batch may take to deliver before it is a stall.
        #[serde(default, skip_serializing_if = "StallAfter::is_default")]
        stall_after_secs: StallAfter,
    },
    /// Rows sent to a stream of a data collection rule through Azure Monitor's Logs Ingestion API, which lands
    /// them in the rule's Log Analytics table, where Microsoft Sentinel reads them.
    #[serde(rename_all = "camelCase")]
    LogAnalytics {
        /// The rule's logs ingestion endpoint, or the data collection endpoint it is linked to.
        endpoint: String,
        /// The rule's immutable id, `dcr-` and 32 hex digits.
        rule_id: String,
        /// The rule's stream the rows go to, such as `Custom-Events_CL`.
        stream: String,
        #[serde(flatten)]
        sign_in: EntraSignInDto,
        #[serde(flatten)]
        tls: TlsDto,
        #[serde(default, skip_serializing_if = "InFlight::is_default")]
        in_flight: InFlight,
        #[serde(default, skip_serializing_if = "RequestBytes::is_default")]
        max_request_bytes: RequestBytes,
        /// Unset means the default, or one whole request if that is larger.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        buffer_bytes: Option<BufferBytes>,
        /// Seconds one batch may take to deliver before it is a stall.
        #[serde(default, skip_serializing_if = "StallAfter::is_default")]
        stall_after_secs: StallAfter,
    },
    /// Rows written as Parquet data files into an Iceberg table, committed through a REST catalog.
    #[serde(rename_all = "camelCase")]
    Iceberg {
        /// The catalog's base address, before `/v1`.
        catalog: String,
        /// Absent is the catalog's default warehouse.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        warehouse: Option<String>,
        /// The table's namespace, its levels joined by `.`.
        namespace: String,
        table: String,
        /// The partition spec a created table takes: `host`, `day(ts)`, `bucket(16, id)`, `truncate(4, name)`.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        partition_by: Vec<String>,
        /// A bearer token for the catalog. At most one of `token` and `clientId`; neither signs in as no one.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        token: Option<SecretRefDto>,
        /// OAuth client credentials: `clientId` and `clientSecret` together.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        client_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        client_secret: Option<SecretRefDto>,
        /// Where client credentials are exchanged for a token. Absent is the catalog's own `/v1/oauth/tokens`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        token_url: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        scope: Option<String>,
        /// Signs every catalog call with AWS Signature Version 4 for this service, `glue` or `s3tables`, with the AWS
        /// credentials storage names, else the host's, for the region the catalog's AWS address names, else `region`.
        /// In place of `token` and `clientId`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        signing_name: Option<String>,
        /// Storage credentials of its own. None of its fields set uses what the catalog vends.
        #[serde(flatten)]
        access: S3AccessDto,
        #[serde(flatten)]
        tls: TlsDto,
        /// Absent is zstd.
        #[serde(default = "CompressionDto::zstd", skip_serializing_if = "CompressionDto::is_zstd")]
        compression: CompressionDto,
        /// Parts of one file uploading at once.
        #[serde(default, skip_serializing_if = "InFlight::is_default")]
        in_flight: InFlight,
        #[serde(default, skip_serializing_if = "ObjectBytes::is_default")]
        max_object_bytes: ObjectBytes,
        /// Memory for records not yet committed. Unset is `BufferBytes::OBJECT_DEFAULT`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        buffer_bytes: Option<BufferBytes>,
        /// Seconds one store or catalog call, or one commit, may take before it is a stall.
        #[serde(default, skip_serializing_if = "StallAfter::is_default")]
        stall_after_secs: StallAfter,
        /// Seconds files stay open before they are committed and their batches acknowledged.
        #[serde(default, skip_serializing_if = "FileInterval::is_default")]
        file_interval_secs: FileInterval,
    },
    /// Rows written as Parquet data files into a Delta table at a path in S3, committed through its `_delta_log`.
    #[serde(rename_all = "camelCase")]
    Delta {
        /// The table's root, `s3://bucket/path`. Exactly one of `location` and `unityCatalog`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        location: Option<String>,
        /// A Unity Catalog external table, whose root and storage credentials the catalog gives.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        unity_catalog: Option<UnityCatalogDto>,
        /// The columns a created table is partitioned by, each a top-level column of the record.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        partition_by: Vec<String>,
        /// How the bucket is reached and signed for. None of its fields set takes the host's.
        #[serde(flatten)]
        access: S3AccessDto,
        #[serde(flatten)]
        tls: TlsDto,
        /// Absent is zstd.
        #[serde(default = "CompressionDto::zstd", skip_serializing_if = "CompressionDto::is_zstd")]
        compression: CompressionDto,
        /// Parts of one file uploading at once.
        #[serde(default, skip_serializing_if = "InFlight::is_default")]
        in_flight: InFlight,
        #[serde(default, skip_serializing_if = "ObjectBytes::is_default")]
        max_object_bytes: ObjectBytes,
        /// Memory for records not yet committed. Unset is `BufferBytes::OBJECT_DEFAULT`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        buffer_bytes: Option<BufferBytes>,
        /// Seconds one store call, or one commit, may take before it is a stall.
        #[serde(default, skip_serializing_if = "StallAfter::is_default")]
        stall_after_secs: StallAfter,
        /// Seconds files stay open before they are committed and their batches acknowledged.
        #[serde(default, skip_serializing_if = "FileInterval::is_default")]
        file_interval_secs: FileInterval,
    },
}

impl SourceKindDto {
    /// Whether this is a Kafka source whose `fetchBytes` the bundle leaves out, which the worker sizes to its memory.
    pub fn leaves_fetch(&self) -> bool {
        matches!(
            self,
            SourceKindDto::Kafka { fetch_bytes: None, .. } | SourceKindDto::EventHubs { fetch_bytes: None, .. }
        )
    }
}

impl DestinationKindDto {
    /// Whether the bundle states this destination's `bufferBytes`. One it leaves out the worker sizes to its memory.
    pub fn states_buffer(&self) -> bool {
        match self {
            DestinationKindDto::Kafka { buffer_bytes, .. }
            | DestinationKindDto::EventHubs { buffer_bytes, .. }
            | DestinationKindDto::Elasticsearch { buffer_bytes, .. }
            | DestinationKindDto::SplunkHec { buffer_bytes, .. }
            | DestinationKindDto::Http { buffer_bytes, .. }
            | DestinationKindDto::Prometheus { buffer_bytes, .. }
            | DestinationKindDto::LogAnalytics { buffer_bytes, .. }
            | DestinationKindDto::Iceberg { buffer_bytes, .. }
            | DestinationKindDto::Delta { buffer_bytes, .. } => buffer_bytes.is_some(),
            DestinationKindDto::S3 { objects, .. }
            | DestinationKindDto::Gcs { objects, .. }
            | DestinationKindDto::AzureBlob { objects, .. }
            | DestinationKindDto::Sftp { objects, .. } => objects.buffer_bytes.is_some(),
        }
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

/// How an object source's objects become records. Flattened into each object source.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ObjectFramingDto {
    #[serde(default, skip_serializing_if = "FramingDto::is_default")]
    pub framing: FramingDto,
    /// `"lines"` only: a pattern whose first capture group is where an event ends, the group dropped, as Splunk's
    /// `LINE_BREAKER`. Without one, an event ends at a line end.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub line_breaker: Option<String>,
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
        SqlSourceDto::deserialize(deserializer).map(|s| s.naming_no_default_port(SqlKind::Postgres))
    }

    fn mysql<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<SqlSourceDto, D::Error> {
        SqlSourceDto::deserialize(deserializer).map(|s| s.naming_no_default_port(SqlKind::MySql))
    }

    fn naming_no_default_port(mut self, kind: SqlKind) -> SqlSourceDto {
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
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SqlKind {
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

/// Seconds an object file stays open before it is finished. A source waits this long for its batches'
/// acknowledgement, which no source bounds: a Kafka reader keeps its membership while it waits, and a queue source
/// extends its messages' visibility. What it costs is replay: after a crash, an interval's rows are read again.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
pub struct FileInterval(pub u64);

impl FileInterval {
    pub const DEFAULT: u64 = 60;
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
    /// What a process collects at unless `SMELTR_LOG_LEVEL` says otherwise, so a source that names nothing
    /// carries everything its process gathers.
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
    fn is_default(&self) -> bool {
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
    /// Whether a row must hold it.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub required: bool,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub description: String,
    /// A value it may hold, as text: what a standard or a spreadsheet gives as its example.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub example: String,
    /// How it maps in an Elasticsearch index: a field's mapping, laid over the one its type has (`io::elastic_mapping`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub elasticsearch: Option<serde_json::Map<String, serde_json::Value>>,
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
