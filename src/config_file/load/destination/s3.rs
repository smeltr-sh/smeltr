use super::super::*;
use super::DestinationSpec;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct S3DestinationDto {
    pub bucket: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kms_key_id: Option<String>,
    #[serde(flatten)]
    pub access: S3AccessDto,
    #[serde(flatten)]
    pub objects: ObjectSettingsDto,
    #[serde(flatten)]
    pub cloud: CloudSettingsDto,
    #[serde(flatten)]
    pub tls: TlsDto,
    #[serde(flatten, default)]
    pub unknown: NoUnknownKeys,
}

impl DestinationSpec for S3DestinationDto {
    fn check(&self, d: &DestinationDto, errors: &mut ItemErrors<'_>) {
        let Self {
            bucket,
            kms_key_id,
            access,
            objects,
            cloud,
            tls,
            unknown: _,
        } = self;
        let url = access.endpoint.as_deref().unwrap_or(S3_ENDPOINT);
        check_objects(&d.id, objects, url, tls, errors);
        check_storage_class(&d.id, cloud, crate::io::objects::S3_STORAGE_CLASSES, errors);
        if blank(bucket) {
            errors
                .at("bucket")
                .push(format!("destination {:?} names no bucket", d.id));
        }
        check_s3_access(access, &format!("destination {:?}", d.id), errors);
        if kms_key_id.as_deref().is_some_and(blank) {
            errors.at("kmsKeyId").push(format!(
                "destination {:?} has an empty kmsKeyId; name one, or leave it out",
                d.id
            ));
        }
    }

    fn to_kind(&self, of: &str) -> DestinationKind {
        let Self {
            unknown: _,
            bucket,
            kms_key_id,
            access,
            objects,
            cloud,
            tls,
        } = self;
        to_objects(
            Cloud::S3(S3Store {
                bucket: bucket.clone(),
                kms_key_id: kms_key_id.clone(),
                access: to_s3_access(access),
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
