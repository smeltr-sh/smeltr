//! Every kind of source: its struct, and what it answers about itself (`SourceSpec`), one file each. The object
//! sources share one generic struct, in `objects`, and the receivers another, in `receiver`.

use super::shared::*;

mod elastic_receiver;
mod event_hubs;
mod hec_receiver;
mod http_api;
mod http_receiver;
mod kafka;
mod logs;
mod metrics;
mod objects;
mod okta_logs;
mod otlp_receiver;
mod prometheus_receiver;
mod receiver;
mod s2s_receiver;
mod sftp;
mod socket_receiver;
mod sql;

pub use self::{
    elastic_receiver::ElasticReceiverSourceDto,
    event_hubs::EventHubsSourceDto,
    hec_receiver::HecReceiverSourceDto,
    http_api::HttpApiSourceDto,
    http_receiver::HttpReceiverSourceDto,
    kafka::KafkaSourceDto,
    logs::LogsSourceDto,
    metrics::MetricsSourceDto,
    objects::{
        AzureBlobListSourceDto, AzureBlobQueueSourceDto, GcsListSourceDto, GcsPubSubSourceDto, S3ListSourceDto,
        S3SqsSourceDto,
    },
    okta_logs::OktaLogsSourceDto,
    otlp_receiver::{OtlpReceiverDto, OtlpReceiverSourceDto},
    prometheus_receiver::PrometheusReceiverSourceDto,
    s2s_receiver::S2sReceiverSourceDto,
    sftp::SftpSourceDto,
    socket_receiver::SocketReceiverSourceDto,
};

/// What one kind of source answers about itself. Each kind implements it in its own file, beside its struct.
pub(super) trait SourceSpec {
    fn check(&self, s: &SourceDto, errors: &mut ItemErrors<'_>);
    /// What it loads as. `of` names it, as `source "in"`, in the messages the loaded source gives an operator.
    fn to_kind(&self, of: &str) -> SourceKind;

    /// What records from this kind carry, before its read format and encoding are applied (`SourceKindDto::meta`).
    fn declared_meta(&self) -> SourceMeta;
    /// Whether one worker of the group reads this source at a time, the same answer
    /// `plan::topology::SourceKind::single_worker` gives. Asked of a source on its way to a capture, which then goes
    /// to one worker too.
    fn single_worker(&self) -> bool {
        false
    }
    /// How this source's record bytes become text, the same answer `plan::topology::SourceKind::encoding` gives.
    /// A label validation would have refused falls back to detection rather than failing here: this is asked of a
    /// source on its way to a capture, where refusing would lose records over a spelling.
    fn encoding(&self) -> Encoding {
        Encoding::Fixed(encoding_rs::UTF_8)
    }
    /// Every credential this source names, by the field naming it. Exhaustive on purpose: a kind added here must
    /// answer, or its credentials would go unnamed, unchecked at deploy and left behind on a version move.
    fn secrets(&self) -> Vec<(&'static str, &SecretRefDto)>;
    /// `secrets`, for the leader to move one onto a new version. Exhaustive as `secrets` is.
    fn secrets_mut(&mut self) -> Vec<&mut SecretRefDto>;
    /// Every address this source reaches over HTTP, by the field naming it. Empty from a kind that is not reached
    /// over HTTP.
    fn urls(&self) -> Vec<(&'static str, &str)>;
    /// Every address this source connects out to, over HTTP or not. Empty for a kind that only listens.
    fn dials(&self) -> Vec<String> {
        self.urls().into_iter().map(|(_, url)| url.to_string()).collect()
    }
    /// The TLS this source connects out or serves under. `None` from a kind that speaks none.
    fn tls(&self) -> Option<TlsDto>;
    /// The registry a stream's Avro payloads name their schemas in.
    fn schema_registry(&self) -> Option<&SchemaRegistryDto> {
        None
    }
    /// How an object it reads is cut into records, for a source that reads objects.
    fn framing(&self) -> Option<&FramingSettingsDto> {
        None
    }

    /// How a payload becomes a record, for a source that reads payloads.
    fn read(&self) -> Option<&ReadFormatDto> {
        None
    }
    /// The port a receiver listens on, or `None` for a source that connects out.
    fn receiver_port(&self) -> Option<u16> {
        None
    }
    /// The transport a receiver's senders use: TCP, except for a socket receiver given UDP.
    fn receiver_transport(&self) -> Transport {
        Transport::Tcp
    }
}

impl SourceKindDto {
    pub(super) fn spec(&self) -> &dyn SourceSpec {
        match self {
            SourceKindDto::Kafka(k) => k,
            SourceKindDto::EventHubs(k) => k,
            SourceKindDto::Metrics(k) => k,
            SourceKindDto::Logs(k) => k,
            SourceKindDto::S3Sqs(k) => k,
            SourceKindDto::S3List(k) => k,
            SourceKindDto::GcsList(k) => k,
            SourceKindDto::AzureBlobList(k) => k,
            SourceKindDto::Sftp(k) => k,
            SourceKindDto::AzureBlobQueue(k) => k,
            SourceKindDto::GcsPubSub(k) => k,
            SourceKindDto::HttpApi(k) => k,
            SourceKindDto::OktaLogs(k) => k,
            SourceKindDto::HttpReceiver(k) => k,
            SourceKindDto::HecReceiver(k) => k,
            SourceKindDto::S2sReceiver(k) => k,
            SourceKindDto::OtlpReceiver(k) => k,
            SourceKindDto::PrometheusReceiver(k) => k,
            SourceKindDto::ElasticReceiver(k) => k,
            SourceKindDto::SocketReceiver(k) => k,
            SourceKindDto::Postgres(k) | SourceKindDto::MySql(k) => k,
        }
    }

    pub(super) fn spec_mut(&mut self) -> &mut dyn SourceSpec {
        match self {
            SourceKindDto::Kafka(k) => k,
            SourceKindDto::EventHubs(k) => k,
            SourceKindDto::Metrics(k) => k,
            SourceKindDto::Logs(k) => k,
            SourceKindDto::S3Sqs(k) => k,
            SourceKindDto::S3List(k) => k,
            SourceKindDto::GcsList(k) => k,
            SourceKindDto::AzureBlobList(k) => k,
            SourceKindDto::Sftp(k) => k,
            SourceKindDto::AzureBlobQueue(k) => k,
            SourceKindDto::GcsPubSub(k) => k,
            SourceKindDto::HttpApi(k) => k,
            SourceKindDto::OktaLogs(k) => k,
            SourceKindDto::HttpReceiver(k) => k,
            SourceKindDto::HecReceiver(k) => k,
            SourceKindDto::S2sReceiver(k) => k,
            SourceKindDto::OtlpReceiver(k) => k,
            SourceKindDto::PrometheusReceiver(k) => k,
            SourceKindDto::ElasticReceiver(k) => k,
            SourceKindDto::SocketReceiver(k) => k,
            SourceKindDto::Postgres(k) | SourceKindDto::MySql(k) => k,
        }
    }

    /// The registry a stream's Avro payloads name their schemas in.
    pub(crate) fn schema_registry(&self) -> Option<&SchemaRegistryDto> {
        self.spec().schema_registry()
    }

    /// How a payload becomes a record, for a source that reads payloads.
    pub(crate) fn read(&self) -> Option<&ReadFormatDto> {
        self.spec().read()
    }

    /// The port a receiver listens on, or `None` for a source that connects out.
    pub fn receiver_port(&self) -> Option<u16> {
        self.spec().receiver_port()
    }

    /// The transport a receiver's senders use: TCP, except for a socket receiver given UDP.
    pub(crate) fn receiver_transport(&self) -> Transport {
        self.spec().receiver_transport()
    }

    /// What records from this kind carry, the same answer `plan::topology::SourceKind::meta` gives once a source is
    /// loaded. Stated here too because a capture filter compiles against a source the author has only written down:
    /// nothing has loaded it, so there is no `SourceKind` to ask.
    pub fn meta(&self) -> SourceMeta {
        let declared = self.spec().declared_meta();
        let declared = match self.read() {
            Some(read) => read.meta(declared),
            None => declared,
        };
        declared.with_encoding(self.encoding())
    }

    /// How an object it reads is cut into records, for a source that reads objects.
    pub(super) fn framing(&self) -> Option<&FramingSettingsDto> {
        self.spec().framing()
    }

    /// Whether one worker of the group reads this source at a time, the same answer
    /// `plan::topology::SourceKind::single_worker` gives. Asked of a source on its way to a capture, which then goes
    /// to one worker too.
    pub(crate) fn single_worker(&self) -> bool {
        self.spec().single_worker()
    }

    /// How this source's record bytes become text, the same answer `plan::topology::SourceKind::encoding` gives.
    /// A label validation would have refused falls back to detection rather than failing here: this is asked of a
    /// source on its way to a capture, where refusing would lose records over a spelling.
    pub(crate) fn encoding(&self) -> Encoding {
        self.spec().encoding()
    }
}

impl SourceDto {
    /// Every credential this source names, by the field naming it. Exhaustive on purpose: a kind added here must
    /// answer, or its credentials would go unnamed, unchecked at deploy and left behind on a version move.
    pub(crate) fn secrets(&self) -> Vec<(&'static str, &SecretRefDto)> {
        self.kind.spec().secrets()
    }

    /// `secrets`, for the leader to move one onto a new version. Exhaustive as `secrets` is.
    pub(crate) fn secrets_mut(&mut self) -> Vec<&mut SecretRefDto> {
        self.kind.spec_mut().secrets_mut()
    }

    /// Every address this source reaches over HTTP, by the field naming it. Empty from a kind that is not reached
    /// over HTTP.
    pub(crate) fn urls(&self) -> Vec<(&'static str, &str)> {
        self.kind.spec().urls()
    }

    /// Every address this source connects out to, over HTTP or not. Empty for a kind that only listens.
    pub fn dials(&self) -> Vec<String> {
        self.kind.spec().dials()
    }

    /// The TLS this source connects out or serves under. `None` from a kind that speaks none.
    pub fn tls(&self) -> Option<TlsDto> {
        self.kind.spec().tls()
    }
}
