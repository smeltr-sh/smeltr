//! The bucket or container an object source lists and an object destination writes to, and how each cloud is
//! reached and signed.
//!
//! A source or destination flattens the name and the access side by side, never one inside the other: serde reads a
//! flattened block inside a flattened block without marking its keys taken (see `NoUnknownKeys`).

use super::*;
use serde::{Deserialize, Serialize};

/// Where a cloud is reached and how its requests are signed, as a bundle names it, and what that cloud's stores
/// take.
pub(super) trait AccessDto {
    /// The block that names one of its stores.
    type Store: StoreNameDto;
    /// What an object destination writing to one of its stores encrypts under, beyond the store's default.
    type Encryption: EncryptionDto;
    /// How it allows its stores to be named.
    const NAME: NameRule;
    /// The storage classes an object destination writing to one of its stores may name.
    const STORAGE_CLASSES: &'static [&'static str];
    /// What a record from a listing source of one of its stores carries.
    const LIST_META: SourceMeta;
    /// Refuses an access block that cannot sign a request as written. `what` names the connector.
    fn check(&self, what: &str, errors: &mut ItemErrors<'_>);
    /// The storage service, and where credentials are obtained when that is configured apart, by the field naming
    /// each. The service comes first.
    fn urls(&self) -> Vec<(&'static str, &str)>;
    fn secrets(&self) -> Vec<(&'static str, &SecretRefDto)>;
    /// `secrets`, for the leader to move one onto a new version.
    fn secrets_mut(&mut self) -> Vec<&mut SecretRefDto>;
    /// The store named `name`, for a listing source.
    fn place(&self, name: &str) -> ListPlace;
    /// The store named `name`, for an object destination.
    fn cloud(&self, name: &str, encryption: &Self::Encryption) -> Cloud;
}

/// The block that names a bucket or container.
pub(super) trait StoreNameDto {
    /// The name, without the spaces around it.
    fn name(&self) -> &str;
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BucketDto {
    pub bucket: String,
}

impl StoreNameDto for BucketDto {
    fn name(&self) -> &str {
        self.bucket.trim()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ContainerDto {
    pub container: String,
}

impl StoreNameDto for ContainerDto {
    fn name(&self) -> &str {
        self.container.trim()
    }
}

/// Refuses a store name its cloud does not allow.
pub(super) fn check_store_name<A: AccessDto>(store: &A::Store, what: &str, errors: &mut ItemErrors<'_>) {
    let NameRule {
        noun,
        scheme,
        rule,
        fits,
    } = A::NAME;
    let name = store.name();
    if name.is_empty() {
        errors.at(noun).push(format!("{what} names no {noun}"));
    } else if !fits(name) {
        errors.at(noun).push(format!(
            "{what} has the {noun} {name:?}, which is not a {noun}'s name. A name is {rule}; name the {noun} alone, \
             with no {scheme} or path"
        ));
    }
}

/// How a cloud allows its buckets or containers to be named. The usual mistake it catches is a URL or a path in place
/// of the name.
pub(super) struct NameRule {
    /// What the cloud calls one, which is also the field that names it.
    noun: &'static str,
    /// How a path into one starts, the mistake the message names.
    scheme: &'static str,
    /// `fits`, said for the reader.
    rule: &'static str,
    fits: fn(&str) -> bool,
}

fn lower_alnum(c: char) -> bool {
    c.is_ascii_lowercase() || c.is_ascii_digit()
}

/// Whether `name` starts and ends with a lowercase letter or digit, and every character of it is one or is in
/// `also`.
fn plain_name(name: &str, also: &[char]) -> bool {
    let edge = |c: Option<char>| c.is_some_and(lower_alnum);
    name.chars().all(|c| lower_alnum(c) || also.contains(&c)) && edge(name.chars().next()) && edge(name.chars().last())
}

/// What an object destination encrypts under, beyond its store's default.
pub(super) trait EncryptionDto {
    fn check(&self, what: &str, errors: &mut ItemErrors<'_>);
}

/// A store whose objects take its own default encryption.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct DefaultEncryptionDto {}

impl EncryptionDto for DefaultEncryptionDto {
    fn check(&self, _: &str, _: &mut ItemErrors<'_>) {}
}

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
        rule: "3 to 63 lowercase letters, digits, dots and hyphens, starting and ending with a letter or digit",
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

impl AccessDto for GcsAccessDto {
    type Store = BucketDto;
    type Encryption = DefaultEncryptionDto;
    const NAME: NameRule = NameRule {
        noun: "bucket",
        scheme: "gs://",
        rule: "3 to 63 characters, or up to 222 when it has dots, at most 63 between them, of lowercase letters, \
               digits, dots, hyphens and underscores, starting and ending with a letter or digit",
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

impl AccessDto for AzureAccessDto {
    type Store = ContainerDto;
    type Encryption = DefaultEncryptionDto;
    const NAME: NameRule = NameRule {
        noun: "container",
        scheme: "https://",
        rule: "3 to 63 lowercase letters, digits and hyphens, starting and ending with a letter or digit, with no \
               two hyphens together, or is one of $logs, $web and $root",
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
