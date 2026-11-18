//! The clouds an object source reads from and an object destination writes to: how each is reached and signed, how
//! its buckets or containers are named, and the queue that announces its objects. One file per cloud.
//!
//! A source or destination flattens the name and the access side by side, never one inside the other: serde reads a
//! flattened block inside a flattened block without marking its keys taken (see `NoUnknownKeys`).

use super::*;
use serde::{Deserialize, Serialize};

mod azure;
mod gcs;
mod s3;

pub use self::{
    azure::StorageQueueDto,
    gcs::PubSubDto,
    s3::{KmsKeyDto, SqsDto},
};

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
    let NameRule { noun, scheme, fits } = A::NAME;
    let name = store.name();
    if name.is_empty() {
        errors.at(noun).push(format!("{what} names no {noun}"));
    } else if !fits(name) {
        errors.at(noun).push(format!(
            "{what} has the {noun} {name:?}, which is not a valid {noun} name; name the {noun} alone, with no {scheme} \
             or path"
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

/// No store of the source's own: each message names the bucket or container its object is in.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct AnnouncedDto {}

/// What announces an object source's objects in the store `S`, reached and signed as `A` says.
pub(super) trait QueueDto<S, A> {
    /// Whether the queue is read by one worker of the group at a time.
    const SINGLE_WORKER: bool = false;
    fn meta(&self) -> SourceMeta;
    /// Refuses a queue that cannot be read as written. `what` names the source.
    fn check(&self, store: &S, what: &str, errors: &mut ItemErrors<'_>);
    fn queue(&self, store: &S, access: &A) -> ObjectQueue;
    /// How long a received message stays hidden from every other worker.
    fn visibility(&self) -> VisibilitySecs {
        VisibilitySecs::default()
    }
    /// The queue's own address, when it is reached apart from the store.
    fn url(&self) -> Option<(&'static str, &str)>;
}
