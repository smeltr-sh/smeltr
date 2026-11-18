//! S3: how it is reached and signed, how its buckets are named, and its SQS queue.

use super::*;
use serde::{Deserialize, Serialize};

/// The KMS key an S3 destination's objects are encrypted under.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KmsKeyDto {
    /// Absent leaves the bucket's default encryption.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kms_key_id: Option<String>,
}

impl EncryptionDto for KmsKeyDto {
    fn check(&self, what: &str, errors: &mut ItemErrors<'_>) {
        if self.kms_key_id.as_deref().is_some_and(blank) {
            errors
                .at("kmsKeyId")
                .push(format!("{what} has an empty kmsKeyId; name one, or leave it out"));
        }
    }
}

impl AccessDto for S3AccessDto {
    type Store = BucketDto;
    type Encryption = KmsKeyDto;
    const NAME: NameRule = NameRule {
        noun: "bucket",
        scheme: "s3://",
        fits: |name| (3..=63).contains(&name.len()) && plain_name(name, &['.', '-']),
    };
    const STORAGE_CLASSES: &'static [&'static str] = crate::io::objects::S3_STORAGE_CLASSES;
    const LIST_META: SourceMeta = SourceMeta::S3_LIST;

    fn place(&self, name: &str) -> ListPlace {
        ListPlace::S3 {
            bucket: name.to_string(),
            access: to_s3_access(self),
        }
    }

    fn cloud(&self, name: &str, encryption: &KmsKeyDto) -> Cloud {
        Cloud::S3(S3Store {
            bucket: name.to_string(),
            kms_key_id: encryption.kms_key_id.clone(),
            access: to_s3_access(self),
        })
    }

    fn check(&self, what: &str, errors: &mut ItemErrors<'_>) {
        if self.access_key_id.is_some() != self.secret_access_key.is_some() {
            let missing = if self.access_key_id.is_none() {
                "accessKeyId"
            } else {
                "secretAccessKey"
            };
            errors.at(missing).push(format!(
                "{what} names half an access key. Name both accessKeyId and secretAccessKey, or neither to use the \
                 host's credentials"
            ));
        }
        if (self.external_id.is_some() || self.auth_endpoint.is_some()) && self.role_arn.is_none() {
            errors.at("roleArn").push(format!(
                "{what} has an externalId or authEndpoint and no roleArn; both serve assuming a role"
            ));
        }
        for (field, value) in [
            ("region", &self.region),
            ("endpoint", &self.endpoint),
            ("authEndpoint", &self.auth_endpoint),
            ("roleArn", &self.role_arn),
            ("externalId", &self.external_id),
        ] {
            if value.as_deref().is_some_and(|v| v.trim().is_empty()) {
                errors
                    .at(field)
                    .push(format!("{what} has an empty {field}; name one, or leave it out"));
            }
        }
    }

    fn urls(&self) -> Vec<(&'static str, &str)> {
        std::iter::once(("endpoint", self.endpoint.as_deref().unwrap_or(S3_ENDPOINT)))
            .chain(self.auth_endpoint.as_deref().map(|u| ("authEndpoint", u)))
            .collect()
    }

    fn secrets(&self) -> Vec<(&'static str, &SecretRefDto)> {
        let id = self.access_key_id.iter().map(|r| ("accessKeyId", r));
        id.chain(self.secret_access_key.iter().map(|r| ("secretAccessKey", r)))
            .collect()
    }

    fn secrets_mut(&mut self) -> Vec<&mut SecretRefDto> {
        [&mut self.access_key_id, &mut self.secret_access_key]
            .into_iter()
            .flatten()
            .collect()
    }
}

/// S3's notifications on an SQS queue.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SqsDto {
    /// The queue's own URL, which names its region and account.
    pub queue_url: String,
    #[serde(default, skip_serializing_if = "VisibilitySecs::is_default")]
    pub visibility_secs: VisibilitySecs,
}

impl QueueDto<AnnouncedDto, S3AccessDto> for SqsDto {
    fn meta(&self) -> SourceMeta {
        SourceMeta::S3_SQS
    }

    fn check(&self, _: &AnnouncedDto, what: &str, errors: &mut ItemErrors<'_>) {
        check_queue(&self.queue_url, self.visibility_secs, what, errors);
    }

    fn queue(&self, _: &AnnouncedDto, access: &S3AccessDto) -> ObjectQueue {
        ObjectQueue::Sqs {
            queue_url: self.queue_url.trim().to_string(),
            access: to_s3_access(access),
        }
    }

    fn visibility(&self) -> VisibilitySecs {
        self.visibility_secs
    }

    fn url(&self) -> Option<(&'static str, &str)> {
        Some(("queueUrl", &self.queue_url))
    }
}
