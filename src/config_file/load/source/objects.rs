use super::super::*;
use super::SourceSpec;
use serde::{Deserialize, Serialize};

/// A source that reads objects as `Q` announces them, from the store `S` or from each one a message names, reached
/// and signed as `A` says.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ObjectSourceDto<S, A, Q> {
    #[serde(flatten)]
    pub store: S,
    #[serde(flatten)]
    pub access: A,
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
    pub queue: Q,
    #[serde(flatten, default)]
    pub unknown: NoUnknownKeys,
}

pub type S3ListSourceDto = ObjectSourceDto<BucketDto, S3AccessDto, ListingDto>;
pub type GcsListSourceDto = ObjectSourceDto<BucketDto, GcsAccessDto, ListingDto>;
pub type AzureBlobListSourceDto = ObjectSourceDto<ContainerDto, AzureAccessDto, ListingDto>;
pub type S3SqsSourceDto = ObjectSourceDto<AnnouncedDto, S3AccessDto, SqsDto>;
pub type GcsPubSubSourceDto = ObjectSourceDto<AnnouncedDto, GcsAccessDto, PubSubDto>;
pub type AzureBlobQueueSourceDto = ObjectSourceDto<AnnouncedDto, AzureAccessDto, StorageQueueDto>;

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

impl<S, A: AccessDto, Q: QueueDto<S, A>> SourceSpec for ObjectSourceDto<S, A, Q> {
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
        Q::SINGLE_WORKER
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
impl<A: AccessDto> QueueDto<A::Store, A> for ListingDto {
    // Nothing divides a listing across a group.
    const SINGLE_WORKER: bool = true;

    fn meta(&self) -> SourceMeta {
        A::LIST_META
    }

    fn check(&self, store: &A::Store, what: &str, errors: &mut ItemErrors<'_>) {
        check_store_name::<A>(store, what, errors);
        check_listing(self, what, errors);
    }

    fn queue(&self, store: &A::Store, access: &A) -> ObjectQueue {
        to_list(access.place(store.name()), self)
    }

    fn url(&self) -> Option<(&'static str, &str)> {
        None
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

/// Event Grid's notifications on an Azure Storage queue.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StorageQueueDto {
    /// The queue's own URL.
    pub queue_url: String,
    #[serde(default, skip_serializing_if = "VisibilitySecs::is_default")]
    pub visibility_secs: VisibilitySecs,
    #[serde(default, skip_serializing_if = "MaxDeliveries::is_default")]
    pub max_deliveries: MaxDeliveries,
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
