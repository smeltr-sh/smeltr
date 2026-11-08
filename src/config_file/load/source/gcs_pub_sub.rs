use super::super::*;
use super::SourceSpec;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GcsPubSubSourceDto {
    /// `projects/<project>/subscriptions/<subscription>`.
    pub subscription: String,
    /// Another Pub/Sub address, such as an emulator's. Absent is Google's.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pubsub_endpoint: Option<String>,
    #[serde(flatten)]
    pub access: GcsAccessDto,
    #[serde(flatten)]
    pub tls: TlsDto,
    #[serde(flatten)]
    pub framing: FramingSettingsDto,
    #[serde(default, flatten)]
    pub read: ReadFormatDto,
    /// How this source's record bytes become text. See [`EncodingLabel`].
    #[serde(default, skip_serializing_if = "EncodingLabel::is_default")]
    pub encoding: EncodingLabel,
    /// At most 600, Pub/Sub's longest ack deadline.
    #[serde(default, skip_serializing_if = "VisibilitySecs::is_default")]
    pub visibility_secs: VisibilitySecs,
    #[serde(flatten, default)]
    pub unknown: NoUnknownKeys,
}

impl SourceSpec for GcsPubSubSourceDto {
    fn check(&self, s: &SourceDto, errors: &mut ItemErrors<'_>) {
        let what = format!("source {:?}", s.id);
        check_tls_reaches(&self.tls, &s.urls(), &what, errors);
        check_encoding(&self.encoding, &s.id, errors);
        check_framing(&self.framing, &s.id, MAX_RECORD_BYTES, "record", errors);
        check_subscription(&self.subscription, &what, errors);
        check_endpoint(self.pubsub_endpoint.as_deref(), "pubsubEndpoint", &what, errors);
        check_endpoint(self.access.endpoint.as_deref(), "endpoint", &what, errors);
        check_visibility(self.visibility_secs, ACK_DEADLINE_MAX, &what, errors);
    }

    fn to_kind(&self, of: &str) -> SourceKind {
        let Self {
            unknown: _,
            subscription,
            pubsub_endpoint,
            access,
            tls,
            framing,
            encoding,
            read: _,
            visibility_secs,
        } = self;
        SourceKind::Objects(ObjectSourceConfig {
            queue: ObjectQueue::PubSub {
                subscription: subscription.trim().to_string(),
                endpoint: pubsub_endpoint.clone(),
                access: to_gcs_access(access),
            },
            tls: to_tls(tls, of, Side::Client),
            framing: to_framing(framing),
            // As above.
            encoding: Encoding::for_label(&encoding.0).unwrap_or_default(),
            avro: None,
            visibility: Duration::from_secs(visibility_secs.0),
        })
    }

    fn secrets(&self) -> Vec<(&'static str, &SecretRefDto)> {
        self.access.secrets()
    }

    fn secrets_mut(&mut self) -> Vec<&mut SecretRefDto> {
        self.access.secrets_mut()
    }

    fn urls(&self) -> Vec<(&'static str, &str)> {
        let Self {
            pubsub_endpoint,
            access,
            ..
        } = self;
        std::iter::once(("pubsubEndpoint", pubsub_endpoint.as_deref().unwrap_or(PUBSUB_ENDPOINT)))
            .chain(access.urls())
            .collect()
    }

    fn tls(&self) -> Option<TlsDto> {
        Some(self.tls.clone())
    }

    fn read(&self) -> Option<&ReadFormatDto> {
        Some(&self.read)
    }

    fn declared_meta(&self) -> SourceMeta {
        SourceMeta::GCS_PUB_SUB
    }

    fn encoding(&self) -> Encoding {
        Encoding::for_label(&self.encoding.0).unwrap_or_default()
    }

    fn framing(&self) -> Option<&FramingSettingsDto> {
        Some(&self.framing)
    }
}
