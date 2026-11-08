use super::super::*;
use super::DestinationSpec;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AzureBlobDestinationDto {
    pub container: String,
    #[serde(flatten)]
    pub access: AzureAccessDto,
    #[serde(flatten)]
    pub objects: ObjectSettingsDto,
    #[serde(flatten)]
    pub cloud: CloudSettingsDto,
    #[serde(flatten)]
    pub tls: TlsDto,
    #[serde(flatten, default)]
    pub unknown: NoUnknownKeys,
}

impl DestinationSpec for AzureBlobDestinationDto {
    fn check(&self, d: &DestinationDto, errors: &mut ItemErrors<'_>) {
        let Self {
            container,
            access,
            objects,
            cloud,
            tls,
            unknown: _,
        } = self;
        let url = access.endpoint.as_deref().unwrap_or(AZURE_ENDPOINT);
        check_objects(&d.id, objects, url, tls, errors);
        check_storage_class(&d.id, cloud, crate::io::objects::AZURE_ACCESS_TIERS, errors);
        if blank(container) {
            errors
                .at("container")
                .push(format!("destination {:?} names no container", d.id));
        }
        check_azure_access(access, &format!("destination {:?}", d.id), errors);
    }

    fn to_kind(&self, of: &str) -> DestinationKind {
        let Self {
            unknown: _,
            container,
            access,
            objects,
            cloud,
            tls,
        } = self;
        to_objects(
            Cloud::Azure(AzureStore {
                container: container.clone(),
                access: to_azure_access(access),
            }),
            objects,
            cloud,
            tls,
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
