use super::super::*;
use super::SourceSpec;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SftpSourceDto {
    /// Absolute, or under the user's home directory. Empty is the home directory.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub directory: String,
    #[serde(flatten)]
    pub access: SftpAccessDto,
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

impl SourceSpec for SftpSourceDto {
    fn check(&self, s: &SourceDto, errors: &mut ItemErrors<'_>) {
        let what = format!("source {:?}", s.id);
        check_sftp_access(&self.access, &what, errors);
        check_encoding(&self.encoding, &s.id, errors);
        check_framing(&self.framing, &s.id, MAX_RECORD_BYTES, "record", errors);
        check_listing(&self.listing, &what, errors);
    }

    fn to_kind(&self, _of: &str) -> SourceKind {
        let Self {
            unknown: _,
            directory,
            access,
            framing,
            encoding,
            read: _,
            listing,
        } = self;
        listed(
            to_list(
                ListPlace::Sftp(SftpStore {
                    directory: sftp_directory(directory),
                    access: to_sftp_access(access),
                }),
                listing,
            ),
            None,
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
        Vec::new()
    }

    fn tls(&self) -> Option<TlsDto> {
        None
    }

    fn read(&self) -> Option<&ReadFormatDto> {
        Some(&self.read)
    }

    fn declared_meta(&self) -> SourceMeta {
        SourceMeta::SFTP
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
