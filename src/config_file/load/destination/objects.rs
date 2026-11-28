use super::super::clouds::{
    AccessDto, BucketDto, ContainerDto, DefaultEncryptionDto, EncryptionDto, KmsKeyDto, StoreNameDto, check_store_name,
};
use super::super::shared::*;
use super::DestinationSpec;
use serde::{Deserialize, Serialize};

/// A destination that writes objects to `Store`, reached and signed as `Access` says and encrypted as `Encryption`
/// says, one open file per prefix.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ObjectDestinationDto<Store, Access, Encryption> {
    #[serde(flatten)]
    store: Store,
    #[serde(flatten)]
    encryption: Encryption,
    #[serde(flatten)]
    access: Access,
    #[serde(flatten)]
    pub(crate) objects: ObjectSettingsDto,
    #[serde(flatten)]
    pub(crate) cloud: CloudSettingsDto,
    #[serde(flatten)]
    tls: TlsDto,
    #[serde(flatten, default)]
    unknown: NoUnknownKeys,
}

pub type S3DestinationDto = ObjectDestinationDto<BucketDto, S3AccessDto, KmsKeyDto>;
pub type GcsDestinationDto = ObjectDestinationDto<BucketDto, GcsAccessDto, DefaultEncryptionDto>;
pub type AzureBlobDestinationDto = ObjectDestinationDto<ContainerDto, AzureAccessDto, DefaultEncryptionDto>;

impl<Access: AccessDto> DestinationSpec for ObjectDestinationDto<Access::Store, Access, Access::Encryption> {
    fn check(&self, d: &DestinationDto, errors: &mut ItemErrors<'_>) {
        let what = format!("destination {:?}", d.id);
        let urls = self.urls();
        check_objects(&d.id, &self.objects, urls[0].1, &self.tls, errors);
        check_storage_class(&d.id, &self.cloud, Access::STORAGE_CLASSES, errors);
        check_store_name::<Access>(&self.store, &what, errors);
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
