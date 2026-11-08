use super::super::*;
use super::DestinationSpec;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KafkaDestinationDto {
    pub brokers: String,
    pub default_topic: String,
    #[serde(flatten)]
    pub tls: TlsDto,
    #[serde(default, skip_serializing_if = "FormatDto::is_default")]
    pub format: FormatDto,
    /// The field `"text"` or `"bytes"` writes. Absent is `_raw` for `"text"`, `__source.bytes` for `"bytes"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
    #[serde(default, skip_serializing_if = "CompressionDto::is_default")]
    pub compression: CompressionDto,
    #[serde(default, skip_serializing_if = "AcksDto::is_default")]
    pub acks: AcksDto,
    /// Seconds one batch may take to deliver before it is a stall.
    #[serde(default, skip_serializing_if = "StallAfter::is_default")]
    pub stall_after_secs: StallAfter,
    /// Unset means the worker sizes it to its memory (`worker::memory::fit`), `BufferBytes::DEFAULT` without a
    /// memory figure.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub buffer_bytes: Option<BufferBytes>,
    /// Where `format` is `"avro"`: the registry each route's record is registered with.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_registry: Option<SchemaRegistryDto>,
    #[serde(default, flatten, skip_serializing_if = "KafkaPropsDto::is_empty")]
    pub props: KafkaPropsDto,
    #[serde(flatten, default)]
    pub unknown: NoUnknownKeys,
}

impl DestinationSpec for KafkaDestinationDto {
    fn check(&self, d: &DestinationDto, errors: &mut ItemErrors<'_>) {
        check_field(
            &d.id,
            matches!(&self.format, FormatDto::Text | FormatDto::Bytes),
            &self.field,
            errors,
        );
        check_stream_avro(self.format, &self.schema_registry, &d.id, errors);
        check_stall_after(self.stall_after_secs, &d.id, errors);
        validate_kafka_props(&self.props, &format!("destination {:?}", d.id), errors);
        check_kafka_tls(&self.tls, &self.props, &format!("destination {:?}", d.id), errors);
        check_kafka_buffer(self.buffer_bytes, &d.id, errors);
        if blank(&self.brokers) {
            errors
                .at("brokers")
                .push(format!("destination {:?} has no brokers", d.id));
        }
        if blank(&self.default_topic) {
            errors
                .at("defaultTopic")
                .push(format!("destination {:?} has no default topic", d.id));
        }
    }

    fn to_kind(&self, of: &str) -> DestinationKind {
        let Self {
            unknown: _,
            brokers,
            default_topic,
            tls,
            props,
            compression,
            acks,
            stall_after_secs,
            buffer_bytes,
            format,
            field,
            schema_registry,
        } = self;
        DestinationKind::Kafka(KafkaDestinationConfig {
            payload: to_payload(*format, field),
            tls: to_tls(tls, of, Side::Client),
            brokers: brokers.clone(),
            props: to_kafka_props(props),
            default_topic: default_topic.clone(),
            compression: compression.to_plan(),
            acks: acks.to_plan(),
            stall_after: std::time::Duration::from_secs(stall_after_secs.0),
            buffer_bytes: buffer_bytes.map_or(BufferBytes::DEFAULT, |b| b.0),
            service: KafkaService::Kafka,
            avro: avro_write(*format, schema_registry),
        })
    }

    fn urls(&self) -> Vec<(&'static str, &str)> {
        self.schema_registry.iter().map(|r| r.url()).collect()
    }

    fn secrets(&self) -> Vec<(&'static str, &SecretRefDto)> {
        let Self {
            props, schema_registry, ..
        } = self;
        (props.secrets().into_iter())
            .chain(schema_registry.iter().flat_map(|r| r.secrets()))
            .collect()
    }

    fn secrets_mut(&mut self) -> Vec<&mut SecretRefDto> {
        let Self {
            props, schema_registry, ..
        } = self;
        (props.secrets_mut().into_iter())
            .chain(schema_registry.iter_mut().flat_map(|r| r.secrets_mut()))
            .collect()
    }

    fn tls(&self) -> Option<TlsDto> {
        Some(self.tls.clone())
    }

    fn states_buffer(&self) -> bool {
        self.buffer_bytes.is_some()
    }
}
