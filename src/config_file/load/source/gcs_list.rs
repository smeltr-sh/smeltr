use super::super::*;
use super::SourceSpec;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GcsListSourceDto {
    pub bucket: String,
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
    #[serde(flatten)]
    pub listing: ListingDto,
    #[serde(flatten, default)]
    pub unknown: NoUnknownKeys,
}

impl SourceSpec for GcsListSourceDto {
    fn check(&self, s: &SourceDto, errors: &mut ItemErrors<'_>) {
        let what = format!("source {:?}", s.id);
        check_tls_reaches(&self.tls, &s.urls(), &what, errors);
        check_endpoint(self.access.endpoint.as_deref(), "endpoint", &what, errors);
        check_encoding(&self.encoding, &s.id, errors);
        check_framing(&self.framing, &s.id, MAX_RECORD_BYTES, "record", errors);
        if self.bucket.trim().is_empty() {
            errors.at("bucket").push(format!("{what} names no bucket"));
        }
        check_listing(&self.listing, &what, errors);
    }

    fn to_kind(&self, of: &str) -> SourceKind {
        let Self {
            unknown: _,
            bucket,
            access,
            tls,
            framing,
            encoding,
            read: _,
            listing,
        } = self;
        listed(
            to_list(
                ListPlace::Gcs(GcsStore {
                    bucket: bucket.trim().to_string(),
                    access: to_gcs_access(access),
                }),
                listing,
            ),
            to_tls(tls, of, Side::Client),
            framing,
            encoding,
        )
    }

    fn secrets(&self) -> Vec<(&'static str, &SecretRefDto)> {
        self.access.secrets()
    }

    fn secrets_mut(&mut self) -> Vec<&mut SecretRefDto> {
        self.access.secrets_mut()
    }

    fn urls(&self) -> Vec<(&'static str, &str)> {
        self.access.urls()
    }

    fn tls(&self) -> Option<TlsDto> {
        Some(self.tls.clone())
    }

    fn read(&self) -> Option<&ReadFormatDto> {
        Some(&self.read)
    }

    fn declared_meta(&self) -> SourceMeta {
        SourceMeta::GCS_LIST
    }

    fn single_worker(&self) -> bool {
        true
    }

    fn encoding(&self) -> Encoding {
        Encoding::for_label(&self.encoding.0).unwrap_or_default()
    }

    fn framing(&self) -> Option<&FramingSettingsDto> {
        Some(&self.framing)
    }
}
