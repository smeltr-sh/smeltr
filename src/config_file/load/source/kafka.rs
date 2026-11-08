use super::super::*;
use super::SourceSpec;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KafkaSourceDto {
    pub brokers: String,
    pub topics: Vec<String>,
    /// The consumer group this source reads as. Absent is its worker group's own
    /// (`load::fill_default_group_ids`).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub group_id: String,
    #[serde(flatten)]
    pub tls: TlsDto,
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
    #[serde(default, flatten, skip_serializing_if = "KafkaPropsDto::is_empty")]
    pub props: KafkaPropsDto,
    #[serde(flatten, default)]
    pub unknown: NoUnknownKeys,
}

impl SourceSpec for KafkaSourceDto {
    fn check(&self, s: &SourceDto, errors: &mut ItemErrors<'_>) {
        check_kafka_tls(&self.tls, &self.props, &format!("source {:?}", s.id), errors);
        check_encoding(&self.encoding, &s.id, errors);
        validate_kafka_props(&self.props, &format!("source {:?}", s.id), errors);
        check_fetch_bytes(self.fetch_bytes, &s.id, errors);
        if blank(&self.brokers) {
            errors.at("brokers").push(format!("source {:?} has no brokers", s.id));
        }
        if self.topics.iter().all(|t| blank(t)) {
            errors
                .at("topics")
                .push(format!("source {:?} subscribes to no topics", s.id));
        }
        for topic in &self.topics {
            if let Some(Err(e)) = crate::io::kafka::topic_pattern(topic) {
                errors.at("topics").push(format!(
                    "source {:?} names the topic pattern {topic:?}, which does not compile: {e}",
                    s.id
                ));
            }
        }
    }

    fn to_kind(&self, of: &str) -> SourceKind {
        let Self {
            unknown: _,
            brokers,
            topics,
            group_id,
            tls,
            start,
            stop,
            fetch_bytes,
            encoding,
            schema_registry: _,
            read: _,
            props,
        } = self;
        SourceKind::Kafka(KafkaSourceConfig {
            tls: to_tls(tls, of, Side::Client),
            fetch_bytes: fetch_bytes.map_or(FetchBytes::DEFAULT, |f| f.0),
            // Validation refused an unknown label before any of this ran, so the fallback is unreachable.
            encoding: Encoding::for_label(&encoding.0).unwrap_or_default(),
            props: to_kafka_props(props),
            brokers: brokers.clone(),
            topics: topics.clone(),
            // A worker group's load fills a blank first, so a blank here is a bundle read outside one.
            group_id: if group_id.trim().is_empty() {
                DEFAULT_GROUP_ID.to_string()
            } else {
                group_id.clone()
            },
            start: to_start(*start),
            stop: stop.map(to_stop),
            deployed_at: 0,
            member: None,
            rack: None,
            service: KafkaService::Kafka,
            avro: None,
        })
    }

    fn secrets(&self) -> Vec<(&'static str, &SecretRefDto)> {
        let Self {
            props, schema_registry, ..
        } = self;
        (props.secrets().into_iter())
            .chain(schema_registry.iter().flat_map(SchemaRegistryDto::secrets))
            .collect()
    }

    fn secrets_mut(&mut self) -> Vec<&mut SecretRefDto> {
        let Self {
            props, schema_registry, ..
        } = self;
        (props.secrets_mut().into_iter())
            .chain(schema_registry.iter_mut().flat_map(SchemaRegistryDto::secrets_mut))
            .collect()
    }

    fn urls(&self) -> Vec<(&'static str, &str)> {
        self.schema_registry.iter().map(SchemaRegistryDto::url).collect()
    }

    fn tls(&self) -> Option<TlsDto> {
        Some(self.tls.clone())
    }

    fn schema_registry(&self) -> Option<&SchemaRegistryDto> {
        self.schema_registry.as_ref()
    }

    fn read(&self) -> Option<&ReadFormatDto> {
        Some(&self.read)
    }

    fn declared_meta(&self) -> SourceMeta {
        SourceMeta::KAFKA
    }

    fn encoding(&self) -> Encoding {
        Encoding::for_label(&self.encoding.0).unwrap_or_default()
    }
}
