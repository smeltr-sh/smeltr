//! Azure Blob Storage: how it is reached and signed, how its containers are named, and its Storage queue.

use super::super::{ItemErrors, QUEUE_NAME_MAX, azure_dials, blank, check_entra_sign_in, check_queue, to_azure_access};
use super::{AccessDto, AnnouncedDto, ContainerDto, DefaultEncryptionDto, NameRule, QueueDto, plain_name};
use crate::config_file::dto::{AZURE_ENDPOINT, AzureAccessDto, MaxDeliveries, SecretRefDto, VisibilitySecs};
use crate::plan::topology::{AzureStore, Cloud, ListPlace, ObjectQueue, SourceMeta};
use serde::{Deserialize, Serialize};

impl AccessDto for AzureAccessDto {
    type Store = ContainerDto;
    type Encryption = DefaultEncryptionDto;
    const NAME: NameRule = NameRule {
        noun: "container",
        scheme: "https://",
        fits: |name| {
            matches!(name, "$logs" | "$web" | "$root")
                || ((3..=63).contains(&name.len()) && plain_name(name, &['-']) && !name.contains("--"))
        },
    };
    const STORAGE_CLASSES: &'static [&'static str] = crate::io::objects::AZURE_ACCESS_TIERS;
    const LIST_META: SourceMeta = SourceMeta::AZURE_BLOB_LIST;

    fn place(&self, name: &str) -> ListPlace {
        ListPlace::Azure(AzureStore {
            container: name.to_string(),
            access: to_azure_access(self),
        })
    }

    fn cloud(&self, name: &str, _: &DefaultEncryptionDto) -> Cloud {
        Cloud::Azure(AzureStore {
            container: name.to_string(),
            access: to_azure_access(self),
        })
    }

    fn check(&self, what: &str, errors: &mut ItemErrors<'_>) {
        if blank(&self.account) {
            errors.at("account").push(format!("{what} names no account"));
        }
        let others: Vec<&'static str> = [
            (self.access_key.is_some(), "accessKey"),
            (self.sas_token.is_some(), "sasToken"),
        ]
        .into_iter()
        .filter_map(|(named, field)| named.then_some(field))
        .collect();
        check_entra_sign_in(&self.entra(), &others, &["accessKey", "sasToken"], what, errors);
        if self.endpoint.as_deref().is_some_and(blank) {
            errors
                .at("endpoint")
                .push(format!("{what} has an empty endpoint; name one, or leave it out"));
        }
    }

    fn urls(&self) -> Vec<(&'static str, &str)> {
        std::iter::once(("endpoint", self.endpoint.as_deref().unwrap_or(AZURE_ENDPOINT)))
            .chain(self.auth_endpoint.as_deref().map(|u| ("authEndpoint", u)))
            .collect()
    }

    fn dials(&self) -> Vec<String> {
        azure_dials(self)
    }

    fn secrets(&self) -> Vec<(&'static str, &SecretRefDto)> {
        [
            ("accessKey", &self.access_key),
            ("sasToken", &self.sas_token),
            ("clientSecret", &self.client_secret),
        ]
        .into_iter()
        .filter_map(|(field, r)| Some((field, r.as_ref()?)))
        .collect()
    }

    fn secrets_mut(&mut self) -> Vec<&mut SecretRefDto> {
        [&mut self.access_key, &mut self.sas_token, &mut self.client_secret]
            .into_iter()
            .flatten()
            .collect()
    }
}

/// Event Grid's notifications on an Azure Storage queue.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StorageQueueDto {
    /// The queue's own URL.
    queue_url: String,
    #[serde(default, skip_serializing_if = "VisibilitySecs::is_default")]
    visibility_secs: VisibilitySecs,
    #[serde(default, skip_serializing_if = "MaxDeliveries::is_default")]
    max_deliveries: MaxDeliveries,
}

impl QueueDto<AnnouncedDto, AzureAccessDto> for StorageQueueDto {
    fn meta(&self) -> SourceMeta {
        SourceMeta::AZURE_BLOB_QUEUE
    }

    fn check(&self, _: &AnnouncedDto, what: &str, errors: &mut ItemErrors<'_>) {
        check_queue(&self.queue_url, self.visibility_secs, what, errors);
        let poison = crate::io::storage_queue::POISON_SUFFIX;
        let name = &self
            .queue_url
            .trim()
            .trim_end_matches('/')
            .rsplit('/')
            .next()
            .unwrap_or_default();
        if name.len() + poison.len() > QUEUE_NAME_MAX {
            errors.at("queueUrl").push(format!(
                "{what} reads the queue {name:?}, whose name leaves no room for its poison queue \
                     {name}{poison}. A queue's name is at most {QUEUE_NAME_MAX} characters, so this one may have \
                     at most {}",
                QUEUE_NAME_MAX - poison.len()
            ));
        }
        if !(MaxDeliveries::MIN..=MaxDeliveries::MAX).contains(&self.max_deliveries.0) {
            errors.at("maxDeliveries").push(format!(
                "{what} moves a message to the poison queue after {} deliveries; maxDeliveries must be \
                     between {} and {}",
                self.max_deliveries.0,
                MaxDeliveries::MIN,
                MaxDeliveries::MAX
            ));
        }
    }

    fn queue(&self, _: &AnnouncedDto, access: &AzureAccessDto) -> ObjectQueue {
        ObjectQueue::StorageQueue {
            queue_url: self.queue_url.trim().to_string(),
            access: to_azure_access(access),
            max_deliveries: self.max_deliveries.0,
        }
    }

    fn visibility(&self) -> VisibilitySecs {
        self.visibility_secs
    }

    fn url(&self) -> Option<(&'static str, &str)> {
        Some(("queueUrl", &self.queue_url))
    }
}
