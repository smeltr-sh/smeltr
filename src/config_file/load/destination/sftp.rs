use super::super::*;
use super::DestinationSpec;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SftpDestinationDto {
    /// Absolute, or under the user's home directory. Empty is the home directory.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub directory: String,
    #[serde(flatten)]
    pub access: SftpAccessDto,
    #[serde(flatten)]
    pub objects: ObjectSettingsDto,
    #[serde(flatten, default)]
    pub unknown: NoUnknownKeys,
}

impl DestinationSpec for SftpDestinationDto {
    fn check(&self, d: &DestinationDto, errors: &mut ItemErrors<'_>) {
        check_objects(&d.id, &self.objects, "sftp", &TlsDto::default(), errors);
        if self.objects.stall_after_secs.0 < SFTP_MIN_STALL_AFTER {
            errors.at("stallAfterSecs").push(format!(
                "destination {:?} sets a stall threshold of {}s; an SFTP destination needs at least \
                     {SFTP_MIN_STALL_AFTER}s, or a slow sign-in or a dead connection outlasts every write",
                d.id, self.objects.stall_after_secs.0
            ));
        }
        check_sftp_access(&self.access, &format!("destination {:?}", d.id), errors);
    }

    fn to_kind(&self, of: &str) -> DestinationKind {
        let Self {
            unknown: _,
            directory,
            access,
            objects,
        } = self;
        to_objects(
            Cloud::Sftp(SftpStore {
                directory: sftp_directory(directory),
                access: to_sftp_access(access),
            }),
            objects,
            // An SFTP server has no storage classes, and is trusted by its host key.
            &CloudSettingsDto::default(),
            &TlsDto::default(),
            of,
        )
    }

    fn urls(&self) -> Vec<(&'static str, &str)> {
        Vec::new()
    }

    fn secrets(&self) -> Vec<(&'static str, &SecretRefDto)> {
        self.access.secrets()
    }

    fn secrets_mut(&mut self) -> Vec<&mut SecretRefDto> {
        self.access.secrets_mut()
    }

    fn tls(&self) -> Option<TlsDto> {
        None
    }

    fn states_buffer(&self) -> bool {
        self.objects.buffer_bytes.is_some()
    }
}
