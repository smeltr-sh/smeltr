use super::super::*;
use super::SourceSpec;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct S3SqsSourceDto {
    /// The queue's own URL, which names its region and account.
    pub queue_url: String,
    #[serde(flatten)]
    pub access: S3AccessDto,
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
    #[serde(flatten, default)]
    pub unknown: NoUnknownKeys,
}

impl SourceSpec for S3SqsSourceDto {
    fn check(&self, s: &SourceDto, errors: &mut ItemErrors<'_>) {
        let what = format!("source {:?}", s.id);
        check_tls_reaches(&self.tls, &s.urls(), &what, errors);
        check_s3_access(&self.access, &what, errors);
        check_encoding(&self.encoding, &s.id, errors);
        check_framing(&self.framing, &s.id, MAX_RECORD_BYTES, "record", errors);
        check_queue(&self.queue_url, self.visibility_secs, &what, errors);
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
        } = self;
        SourceKind::Objects(ObjectSourceConfig {
            queue: ObjectQueue::Sqs {
                queue_url: queue_url.trim().to_string(),
                access: to_s3_access(access),
            },
            tls: to_tls(tls, of, Side::Client),
            framing: to_framing(framing),
            // Validation refused an unknown label before any of this ran, so the fallback is unreachable.
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
        SourceMeta::S3_SQS
    }

    fn encoding(&self) -> Encoding {
        Encoding::for_label(&self.encoding.0).unwrap_or_default()
    }

    fn framing(&self) -> Option<&FramingSettingsDto> {
        Some(&self.framing)
    }
}
