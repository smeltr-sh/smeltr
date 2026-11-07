use super::super::*;
use super::SourceSpec;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventHubsSourceDto {
    /// The namespace: its name in Azure's public cloud, its host name in another cloud, or `host:port`.
    pub namespace: String,
    /// The event hubs it reads, each a Kafka topic.
    pub event_hubs: Vec<String>,
    pub consumer_group: String,
    /// The namespace's or an event hub's connection string. At most one of it, `clientSecret` and
    /// `managedIdentityClientId`; none signs in as the host's managed identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connection_string: Option<SecretRefDto>,
    #[serde(flatten)]
    pub sign_in: EntraSignInDto,
    #[serde(default, skip_serializing_if = "StartPositionDto::is_default")]
    pub start: StartPositionDto,
    /// `None` is an unbounded streaming source.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stop: Option<StopPositionDto>,
    /// Unset means the worker sizes it to its memory (`worker::memory::fit`), `FetchBytes::DEFAULT` without a memory
    /// figure.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fetch_bytes: Option<FetchBytes>,
    /// How this source's record bytes become text. See [`EncodingLabel`].
    #[serde(default, skip_serializing_if = "EncodingLabel::is_default")]
    pub encoding: EncodingLabel,
    /// Where its payloads are Avro: the registry holding each writer's schema.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_registry: Option<SchemaRegistryDto>,
    #[serde(default, flatten)]
    pub read: ReadFormatDto,
    #[serde(flatten, default)]
    pub unknown: NoUnknownKeys,
}

impl SourceSpec for EventHubsSourceDto {
    fn check(&self, s: &SourceDto, errors: &mut ItemErrors<'_>) {
        check_encoding(&self.encoding, &s.id, errors);
        check_fetch_bytes(self.fetch_bytes, &s.id, errors);
        check_event_hubs_namespace(&self.namespace, &format!("source {:?}", s.id), errors);
        check_event_hubs_sign_in(
            self.connection_string.as_ref(),
            &self.sign_in,
            &format!("source {:?}", s.id),
            errors,
        );
        if self.event_hubs.iter().all(|h| blank(h)) {
            errors
                .at("eventHubs")
                .push(format!("source {:?} reads no event hubs", s.id));
        }
        for hub in self.event_hubs.iter().filter(|h| h.starts_with('^')) {
            errors.at("eventHubs").push(format!(
                "source {:?} names {hub:?}; an event hub is named in full, not by a pattern",
                s.id
            ));
        }
        if blank(&self.consumer_group) {
            errors.at("consumerGroup").push(format!(
                "source {:?} has no consumer group -- it is the group every event hub above is read \
                     under",
                s.id
            ));
        }
    }

    fn to_kind(&self, _of: &str) -> SourceKind {
        let Self {
            unknown: _,
            namespace,
            event_hubs,
            consumer_group,
            connection_string,
            sign_in,
            start,
            stop,
            fetch_bytes,
            encoding,
            schema_registry: _,
            read: _,
        } = self;
        let brokers = event_hubs_brokers(namespace);
        SourceKind::Kafka(KafkaSourceConfig {
            tls: None,
            fetch_bytes: fetch_bytes.map_or(FetchBytes::DEFAULT, |f| f.0),
            // Validation refused an unknown label before any of this ran, so the fallback is unreachable.
            encoding: Encoding::for_label(&encoding.0).unwrap_or_default(),
            props: event_hubs_props(&brokers, connection_string.as_ref(), sign_in),
            brokers,
            topics: event_hubs.clone(),
            group_id: consumer_group.clone(),
            start: to_start(*start),
            stop: stop.map(to_stop),
            deployed_at: 0,
            member: None,
            rack: None,
            service: KafkaService::EventHubs,
            avro: None,
        })
    }

    fn secrets(&self) -> Vec<(&'static str, &SecretRefDto)> {
        let Self {
            connection_string,
            sign_in,
            schema_registry,
            ..
        } = self;
        (connection_string.iter().map(|r| ("connectionString", r)))
            .chain(sign_in.secrets())
            .chain(schema_registry.iter().flat_map(SchemaRegistryDto::secrets))
            .collect()
    }

    fn secrets_mut(&mut self) -> Vec<&mut SecretRefDto> {
        let Self {
            connection_string,
            sign_in,
            schema_registry,
            ..
        } = self;
        (connection_string.iter_mut().chain(sign_in.secrets_mut()))
            .chain(schema_registry.iter_mut().flat_map(SchemaRegistryDto::secrets_mut))
            .collect()
    }

    fn urls(&self) -> Vec<(&'static str, &str)> {
        let Self {
            sign_in,
            schema_registry,
            ..
        } = self;
        (sign_in.urls().into_iter())
            .chain(schema_registry.iter().map(SchemaRegistryDto::url))
            .collect()
    }

    fn tls(&self) -> Option<TlsDto> {
        None
    }

    fn schema_registry(&self) -> Option<&SchemaRegistryDto> {
        self.schema_registry.as_ref()
    }

    fn read(&self) -> Option<&ReadFormatDto> {
        Some(&self.read)
    }

    fn declared_meta(&self) -> SourceMeta {
        SourceMeta::EVENT_HUBS
    }

    fn encoding(&self) -> Encoding {
        Encoding::for_label(&self.encoding.0).unwrap_or_default()
    }
}
