//! Every kind of source: its struct, and what it answers about itself (`SourceSpec`), one file each.

use super::*;

mod azure_blob_list;
mod azure_blob_queue;
mod elastic_receiver;
mod event_hubs;
mod gcs_list;
mod gcs_pub_sub;
mod hec_receiver;
mod http_api;
mod http_receiver;
mod kafka;
mod logs;
mod metrics;
mod okta_logs;
mod otlp_receiver;
mod prometheus_receiver;
mod s2s_receiver;
mod s3_list;
mod s3_sqs;
mod sftp;
mod socket_receiver;
mod sql;
mod syslog_receiver;

pub use self::{
    azure_blob_list::AzureBlobListSourceDto, azure_blob_queue::AzureBlobQueueSourceDto,
    elastic_receiver::ElasticReceiverSourceDto, event_hubs::EventHubsSourceDto, gcs_list::GcsListSourceDto,
    gcs_pub_sub::GcsPubSubSourceDto, hec_receiver::HecReceiverSourceDto, http_api::HttpApiSourceDto,
    http_receiver::HttpReceiverSourceDto, kafka::KafkaSourceDto, logs::LogsSourceDto, metrics::MetricsSourceDto,
    okta_logs::OktaLogsSourceDto, otlp_receiver::OtlpReceiverSourceDto,
    prometheus_receiver::PrometheusReceiverSourceDto, s2s_receiver::S2sReceiverSourceDto, s3_list::S3ListSourceDto,
    s3_sqs::S3SqsSourceDto, sftp::SftpSourceDto, socket_receiver::SocketReceiverSourceDto,
    syslog_receiver::SyslogReceiverSourceDto,
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
    /// What a receiver's senders send over. TCP for every kind but a syslog receiver given UDP.
    fn receiver_transport(&self) -> Transport {
        Transport::Tcp
    }
}

/// The checks the OTLP, Prometheus and Elasticsearch receivers share: where they listen, what they serve and who
/// may send.
fn check_intake(
    s: &SourceDto,
    listener: &ReceiverSettingsDto,
    connections: &ConnectionLimitsDto,
    tls: &ServedTlsDto,
    tokens: &BTreeMap<String, SecretRefDto>,
    errors: &mut ItemErrors<'_>,
) {
    let what = format!("source {:?}", s.id);
    check_receiver(&what, listener, tls, tokens, errors);
    check_connection_limits(&what, connections, true, errors);
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
            SourceKindDto::SyslogReceiver(k) => k,
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
            SourceKindDto::SyslogReceiver(k) => k,
            SourceKindDto::SocketReceiver(k) => k,
            SourceKindDto::Postgres(k) | SourceKindDto::MySql(k) => k,
        }
    }

    /// The registry a stream's Avro payloads name their schemas in.
    pub fn schema_registry(&self) -> Option<&SchemaRegistryDto> {
        self.spec().schema_registry()
    }

    /// How a payload becomes a record, for a source that reads payloads.
    pub fn read(&self) -> Option<&ReadFormatDto> {
        self.spec().read()
    }

    /// The port a receiver listens on, or `None` for a source that connects out.
    pub fn receiver_port(&self) -> Option<u16> {
        self.spec().receiver_port()
    }

    /// What a receiver's senders send over. TCP for every kind but a syslog receiver given UDP.
    pub fn receiver_transport(&self) -> Transport {
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
    pub fn single_worker(&self) -> bool {
        self.spec().single_worker()
    }

    /// How this source's record bytes become text, the same answer `plan::topology::SourceKind::encoding` gives.
    /// A label validation would have refused falls back to detection rather than failing here: this is asked of a
    /// source on its way to a capture, where refusing would lose records over a spelling.
    pub fn encoding(&self) -> Encoding {
        self.spec().encoding()
    }
}

impl SourceDto {
    /// Every credential this source names, by the field naming it. Exhaustive on purpose: a kind added here must
    /// answer, or its credentials would go unnamed, unchecked at deploy and left behind on a version move.
    pub fn secrets(&self) -> Vec<(&'static str, &SecretRefDto)> {
        self.kind.spec().secrets()
    }

    /// `secrets`, for the leader to move one onto a new version. Exhaustive as `secrets` is.
    pub fn secrets_mut(&mut self) -> Vec<&mut SecretRefDto> {
        self.kind.spec_mut().secrets_mut()
    }

    /// Every address this source reaches over HTTP, by the field naming it. Empty from a kind that is not reached
    /// over HTTP.
    pub fn urls(&self) -> Vec<(&'static str, &str)> {
        self.kind.spec().urls()
    }

    /// The TLS this source connects out or serves under. `None` from a kind that speaks none.
    pub fn tls(&self) -> Option<TlsDto> {
        self.kind.spec().tls()
    }
}
