use super::super::*;
use super::DestinationSpec;
use serde::{Deserialize, Serialize};

/// A destination that writes objects to the store `S`, reached and signed as `A` says and encrypted as `E` says, one
/// open file per prefix.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ObjectDestinationDto<S, A, E> {
    #[serde(flatten)]
    pub store: S,
    #[serde(flatten)]
    pub encryption: E,
    #[serde(flatten)]
    pub access: A,
    #[serde(flatten)]
    pub objects: ObjectSettingsDto,
    #[serde(flatten)]
    pub cloud: CloudSettingsDto,
    #[serde(flatten)]
    pub tls: TlsDto,
    #[serde(flatten, default)]
    pub unknown: NoUnknownKeys,
}

pub type S3DestinationDto = ObjectDestinationDto<BucketDto, S3AccessDto, KmsKeyDto>;
pub type GcsDestinationDto = ObjectDestinationDto<BucketDto, GcsAccessDto, DefaultEncryptionDto>;
pub type AzureBlobDestinationDto = ObjectDestinationDto<ContainerDto, AzureAccessDto, DefaultEncryptionDto>;

impl<A: AccessDto> DestinationSpec for ObjectDestinationDto<A::Store, A, A::Encryption> {
    fn check(&self, d: &DestinationDto, errors: &mut ItemErrors<'_>) {
        let what = format!("destination {:?}", d.id);
        let urls = self.urls();
        check_objects(&d.id, &self.objects, urls[0].1, &self.tls, errors);
        check_storage_class(&d.id, &self.cloud, A::STORAGE_CLASSES, errors);
        check_store_name::<A>(&self.store, &what, errors);
        self.access.check(&what, errors);
        self.encryption.check(&what, errors);
    }

    fn to_kind(&self, of: &str) -> DestinationKind {
        to_objects(
            self.access.cloud(self.store.name(), &self.encryption),
            &self.objects,
            &self.cloud,
            &self.tls,
            of,
        )
    }

    fn urls(&self) -> Vec<(&'static str, &str)> {
        self.access.urls()
    }

    fn secrets(&self) -> Vec<(&'static str, &SecretRefDto)> {
        self.access.secrets()
    }

    fn secrets_mut(&mut self) -> Vec<&mut SecretRefDto> {
        self.access.secrets_mut()
    }

    fn tls(&self) -> Option<TlsDto> {
        Some(self.tls.clone())
    }

    fn states_buffer(&self) -> bool {
        self.objects.buffer_bytes.is_some()
    }
}
