use super::super::clouds::{
    AccessDto, AnnouncedDto, BucketDto, ContainerDto, PubSubDto, QueueDto, SqsDto, StorageQueueDto, StoreNameDto,
    check_store_name,
};
use super::super::*;
use super::SourceSpec;
use serde::{Deserialize, Serialize};

/// A source that reads objects as `Queue` announces them, from `Store` or from each store a message names, reached and
/// signed as `Access` says.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ObjectSourceDto<Store, Access, Queue> {
    #[serde(flatten)]
    pub store: Store,
    #[serde(flatten)]
    pub access: Access,
    #[serde(flatten)]
    pub tls: TlsDto,
    #[serde(flatten)]
    pub framing: FramingSettingsDto,
    #[serde(default, flatten)]
    pub read: ReadFormatDto,
    /// How this source's record bytes become text. See [`EncodingLabel`].
    #[serde(default, skip_serializing_if = "EncodingLabel::is_default")]
    pub encoding: EncodingLabel,
    #[serde(flatten)]
    pub queue: Queue,
    #[serde(flatten, default)]
    pub unknown: NoUnknownKeys,
}

pub type S3ListSourceDto = ObjectSourceDto<BucketDto, S3AccessDto, ListingDto>;
pub type GcsListSourceDto = ObjectSourceDto<BucketDto, GcsAccessDto, ListingDto>;
pub type AzureBlobListSourceDto = ObjectSourceDto<ContainerDto, AzureAccessDto, ListingDto>;
pub type S3SqsSourceDto = ObjectSourceDto<AnnouncedDto, S3AccessDto, SqsDto>;
pub type GcsPubSubSourceDto = ObjectSourceDto<AnnouncedDto, GcsAccessDto, PubSubDto>;
pub type AzureBlobQueueSourceDto = ObjectSourceDto<AnnouncedDto, AzureAccessDto, StorageQueueDto>;

impl<Store, Access: AccessDto, Queue: QueueDto<Store, Access>> SourceSpec for ObjectSourceDto<Store, Access, Queue> {
    fn check(&self, s: &SourceDto, errors: &mut ItemErrors<'_>) {
        let what = format!("source {:?}", s.id);
        check_tls_reaches(&self.tls, &s.urls(), &what, errors);
        self.access.check(&what, errors);
        check_encoding(&self.encoding, &s.id, errors);
        check_framing(&self.framing, &s.id, MAX_RECORD_BYTES, "record", errors);
        self.queue.check(&self.store, &what, errors);
    }

    fn to_kind(&self, of: &str) -> SourceKind {
        SourceKind::Objects(ObjectSourceConfig {
            queue: self.queue.queue(&self.store, &self.access),
            tls: to_tls(&self.tls, of, Side::Client),
            framing: to_framing(&self.framing),
            encoding: self.encoding(),
            avro: None,
            visibility: Duration::from_secs(self.queue.visibility().0),
        })
    }

    fn secrets(&self) -> Vec<(&'static str, &SecretRefDto)> {
        self.access.secrets()
    }

    fn secrets_mut(&mut self) -> Vec<&mut SecretRefDto> {
        self.access.secrets_mut()
    }

    fn urls(&self) -> Vec<(&'static str, &str)> {
        self.queue.url().into_iter().chain(self.access.urls()).collect()
    }

    fn dials(&self) -> Vec<String> {
        (self.queue.url().map(|(_, url)| url.to_string()))
            .into_iter()
            .chain(self.access.dials())
            .collect()
    }

    fn tls(&self) -> Option<TlsDto> {
        Some(self.tls.clone())
    }

    fn read(&self) -> Option<&ReadFormatDto> {
        Some(&self.read)
    }

    fn declared_meta(&self) -> SourceMeta {
        self.queue.meta()
    }

    fn single_worker(&self) -> bool {
        Queue::SINGLE_WORKER
    }

    fn encoding(&self) -> Encoding {
        // Validation refuses an unknown label before a source loads, so the fallback is reached only on the way to a
        // capture.
        Encoding::for_label(&self.encoding.0).unwrap_or_default()
    }

    fn framing(&self) -> Option<&FramingSettingsDto> {
        Some(&self.framing)
    }
}

/// A listing of one store. A listed object is not hidden from other workers, so it takes the visibility timeout the
/// queue kinds default to.
impl<Access: AccessDto> QueueDto<Access::Store, Access> for ListingDto {
    // Nothing divides a listing across a group.
    const SINGLE_WORKER: bool = true;

    fn meta(&self) -> SourceMeta {
        Access::LIST_META
    }

    fn check(&self, store: &Access::Store, what: &str, errors: &mut ItemErrors<'_>) {
        check_store_name::<Access>(store, what, errors);
        check_listing(self, what, errors);
    }

    fn queue(&self, store: &Access::Store, access: &Access) -> ObjectQueue {
        to_list(access.place(store.name()), self)
    }

    fn url(&self) -> Option<(&'static str, &str)> {
        None
    }
}
