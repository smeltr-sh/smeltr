//! Cloud Storage: how it is reached and signed, how its buckets are named, and its Pub/Sub subscription.

use super::*;
use serde::{Deserialize, Serialize};

impl AccessDto for GcsAccessDto {
    type Store = BucketDto;
    type Encryption = DefaultEncryptionDto;
    const NAME: NameRule = NameRule {
        noun: "bucket",
        scheme: "gs://",
        fits: |name| {
            let max = if name.contains('.') { 222 } else { 63 };
            (3..=max).contains(&name.len())
                && plain_name(name, &['.', '-', '_'])
                && name.split('.').all(|part| part.len() <= 63)
        },
    };
    const STORAGE_CLASSES: &'static [&'static str] = crate::io::objects::GCS_STORAGE_CLASSES;
    const LIST_META: SourceMeta = SourceMeta::GCS_LIST;

    fn place(&self, name: &str) -> ListPlace {
        ListPlace::Gcs(GcsStore {
            bucket: name.to_string(),
            access: to_gcs_access(self),
        })
    }

    fn cloud(&self, name: &str, _: &DefaultEncryptionDto) -> Cloud {
        Cloud::Gcs(GcsStore {
            bucket: name.to_string(),
            access: to_gcs_access(self),
        })
    }

    fn check(&self, what: &str, errors: &mut ItemErrors<'_>) {
        check_endpoint(self.endpoint.as_deref(), "endpoint", what, errors);
    }

    fn urls(&self) -> Vec<(&'static str, &str)> {
        vec![("endpoint", self.endpoint.as_deref().unwrap_or(GCS_ENDPOINT))]
    }

    fn secrets(&self) -> Vec<(&'static str, &SecretRefDto)> {
        self.service_account_key
            .iter()
            .map(|r| ("serviceAccountKey", r))
            .collect()
    }

    fn secrets_mut(&mut self) -> Vec<&mut SecretRefDto> {
        self.service_account_key.iter_mut().collect()
    }
}

/// Cloud Storage's notifications on a Pub/Sub subscription.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PubSubDto {
    /// `projects/<project>/subscriptions/<subscription>`.
    pub subscription: String,
    /// Another Pub/Sub address, such as an emulator's. Absent is Google's.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pubsub_endpoint: Option<String>,
    /// At most 600, Pub/Sub's longest ack deadline.
    #[serde(default, skip_serializing_if = "VisibilitySecs::is_default")]
    pub visibility_secs: VisibilitySecs,
}

impl QueueDto<AnnouncedDto, GcsAccessDto> for PubSubDto {
    fn meta(&self) -> SourceMeta {
        SourceMeta::GCS_PUB_SUB
    }

    fn check(&self, _: &AnnouncedDto, what: &str, errors: &mut ItemErrors<'_>) {
        check_subscription(&self.subscription, what, errors);
        check_endpoint(self.pubsub_endpoint.as_deref(), "pubsubEndpoint", what, errors);
        check_visibility(self.visibility_secs, ACK_DEADLINE_MAX, what, errors);
    }

    fn queue(&self, _: &AnnouncedDto, access: &GcsAccessDto) -> ObjectQueue {
        ObjectQueue::PubSub {
            subscription: self.subscription.trim().to_string(),
            endpoint: self.pubsub_endpoint.clone(),
            access: to_gcs_access(access),
        }
    }

    fn visibility(&self) -> VisibilitySecs {
        self.visibility_secs
    }

    fn url(&self) -> Option<(&'static str, &str)> {
        Some((
            "pubsubEndpoint",
            self.pubsub_endpoint.as_deref().unwrap_or(PUBSUB_ENDPOINT),
        ))
    }
}
