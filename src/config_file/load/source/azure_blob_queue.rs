use super::super::*;
use super::SourceSpec;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AzureBlobQueueSourceDto {
    /// The queue's own URL.
    pub queue_url: String,
    #[serde(flatten)]
    pub access: AzureAccessDto,
    #[serde(flatten)]
    pub tls: TlsDto,
    #[serde(flatten)]
    pub framing: FramingSettingsDto,
    #[serde(default, flatten)]
    pub read: ReadFormatDto,
    /// How this source's record bytes become text. See [`EncodingLabel`].
    #[serde(default, skip_serializing_if = "EncodingLabel::is_default")]
    pub encoding: EncodingLabel,
    #[serde(default, skip_serializing_if = "VisibilitySecs::is_default")]
    pub visibility_secs: VisibilitySecs,
    #[serde(default, skip_serializing_if = "MaxDeliveries::is_default")]
    pub max_deliveries: MaxDeliveries,
    #[serde(flatten, default)]
    pub unknown: NoUnknownKeys,
}

impl SourceSpec for AzureBlobQueueSourceDto {
    fn check(&self, s: &SourceDto, errors: &mut ItemErrors<'_>) {
        let what = format!("source {:?}", s.id);
        check_tls_reaches(&self.tls, &s.urls(), &what, errors);
        check_azure_access(&self.access, &what, errors);
        check_encoding(&self.encoding, &s.id, errors);
        check_framing(&self.framing, &s.id, MAX_RECORD_BYTES, "record", errors);
        check_queue(&self.queue_url, self.visibility_secs, &what, errors);
        let poison = crate::io::storage_queue::POISON_SUFFIX;
        let name = &self
            .queue_url
            .trim()
            .trim_end_matches('/')
            .rsplit('/')
            .next()
            .unwrap_or_default();
        if name.len() + poison.len() > QUEUE_NAME_MAX {
            errors.at("queueUrl").push(format!(
                "{what} reads the queue {name:?}, whose name leaves no room for its poison queue \
                     {name}{poison}. A queue's name is at most {QUEUE_NAME_MAX} characters, so this one may have \
                     at most {}",
                QUEUE_NAME_MAX - poison.len()
            ));
        }
        if !(MaxDeliveries::MIN..=MaxDeliveries::MAX).contains(&self.max_deliveries.0) {
            errors.at("maxDeliveries").push(format!(
                "{what} moves a message to the poison queue after {} deliveries; maxDeliveries must be \
                     between {} and {}",
                self.max_deliveries.0,
                MaxDeliveries::MIN,
                MaxDeliveries::MAX
            ));
        }
    }

    fn to_kind(&self, of: &str) -> SourceKind {
        let Self {
            unknown: _,
            queue_url,
            access,
            tls,
            framing,
            encoding,
            read: _,
            visibility_secs,
            max_deliveries,
        } = self;
        SourceKind::Objects(ObjectSourceConfig {
            queue: ObjectQueue::StorageQueue {
                queue_url: queue_url.clone(),
                access: to_azure_access(access),
                max_deliveries: max_deliveries.0,
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
        std::iter::once(("queueUrl", self.queue_url.as_str()))
            .chain(self.access.urls())
            .collect()
    }

    fn tls(&self) -> Option<TlsDto> {
        Some(self.tls.clone())
    }

    fn read(&self) -> Option<&ReadFormatDto> {
        Some(&self.read)
    }

    fn declared_meta(&self) -> SourceMeta {
        SourceMeta::AZURE_BLOB_QUEUE
    }

    fn encoding(&self) -> Encoding {
        Encoding::for_label(&self.encoding.0).unwrap_or_default()
    }

    fn framing(&self) -> Option<&FramingSettingsDto> {
        Some(&self.framing)
    }
}
