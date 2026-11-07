use super::super::*;
use super::DestinationSpec;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventHubsDestinationDto {
    /// The namespace: its name in Azure's public cloud, its host name in another cloud, or `host:port`.
    pub namespace: String,
    /// The event hub a row goes to when it names none, as a Kafka destination's default topic.
    pub default_event_hub: String,
    /// The namespace's or an event hub's connection string. At most one of it, `clientSecret` and
    /// `managedIdentityClientId`; none signs in as the host's managed identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connection_string: Option<SecretRefDto>,
    #[serde(flatten)]
    pub sign_in: EntraSignInDto,
    #[serde(default, skip_serializing_if = "FormatDto::is_default")]
    pub format: FormatDto,
    /// The field `"text"` or `"bytes"` writes. Absent is `_raw` for `"text"`, `__source.bytes` for `"bytes"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
    /// None or gzip: Event Hubs refuses the other codecs.
    #[serde(default, skip_serializing_if = "CompressionDto::is_default")]
    pub compression: CompressionDto,
    /// Seconds one batch may take to deliver before it is a stall.
    #[serde(default, skip_serializing_if = "StallAfter::is_default")]
    pub stall_after_secs: StallAfter,
    /// As a Kafka destination's.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub buffer_bytes: Option<BufferBytes>,
    /// As a Kafka destination's.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_registry: Option<SchemaRegistryDto>,
    #[serde(flatten, default)]
    pub unknown: NoUnknownKeys,
}

impl DestinationSpec for EventHubsDestinationDto {
    fn check(&self, d: &DestinationDto, errors: &mut ItemErrors<'_>) {
        let compression = &self.compression;
        check_field(
            &d.id,
            matches!(&self.format, FormatDto::Text | FormatDto::Bytes),
            &self.field,
            errors,
        );
        check_stream_avro(self.format, &self.schema_registry, &d.id, errors);
        check_stall_after(self.stall_after_secs, &d.id, errors);
        check_kafka_buffer(self.buffer_bytes, &d.id, errors);
        check_event_hubs_namespace(&self.namespace, &format!("destination {:?}", d.id), errors);
        check_event_hubs_sign_in(
            self.connection_string.as_ref(),
            &self.sign_in,
            &format!("destination {:?}", d.id),
            errors,
        );
        if blank(&self.default_event_hub) {
            errors
                .at("defaultEventHub")
                .push(format!("destination {:?} has no default event hub", d.id));
        }
        if !matches!(compression, CompressionDto::None | CompressionDto::Gzip) {
            errors.at("compression").push(format!(
                "destination {:?} compresses with {compression:?}; an Event Hubs destination takes only \
                     \"gzip\" or \"none\"",
                d.id
            ));
        }
    }

    fn to_kind(&self, _of: &str) -> DestinationKind {
        let Self {
            unknown: _,
            namespace,
            default_event_hub,
            connection_string,
            sign_in,
            format,
            field,
            compression,
            stall_after_secs,
            buffer_bytes,
            schema_registry,
        } = self;
        let brokers = event_hubs_brokers(namespace);
        DestinationKind::Kafka(KafkaDestinationConfig {
            payload: to_payload(*format, field),
            tls: None,
            props: event_hubs_props(&brokers, connection_string.as_ref(), sign_in),
            brokers,
            default_topic: default_event_hub.clone(),
            compression: compression.to_plan(),
            acks: crate::plan::topology::Acks::All,
            stall_after: std::time::Duration::from_secs(stall_after_secs.0),
            buffer_bytes: buffer_bytes.map_or(BufferBytes::DEFAULT, |b| b.0),
            service: KafkaService::EventHubs,
            avro: avro_write(*format, schema_registry),
        })
    }

    fn urls(&self) -> Vec<(&'static str, &str)> {
        let Self {
            sign_in,
            schema_registry,
            ..
        } = self;
        (sign_in.urls().into_iter())
            .chain(schema_registry.iter().map(|r| r.url()))
            .collect()
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
            .chain(schema_registry.iter().flat_map(|r| r.secrets()))
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
            .chain(schema_registry.iter_mut().flat_map(|r| r.secrets_mut()))
            .collect()
    }

    fn tls(&self) -> Option<TlsDto> {
        None
    }

    fn states_buffer(&self) -> bool {
        self.buffer_bytes.is_some()
    }
}
