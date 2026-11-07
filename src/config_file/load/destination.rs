//! Every kind of destination: its struct, and what it answers about itself (`DestinationSpec`), one file each.

use super::*;

mod azure_blob;
mod delta;
mod elasticsearch;
mod event_hubs;
mod gcs;
mod http;
mod iceberg;
mod kafka;
mod log_analytics;
mod otlp;
mod prometheus;
mod s3;
mod sftp;
mod splunk_hec;

pub use self::{
    azure_blob::AzureBlobDestinationDto, delta::DeltaDestinationDto, elasticsearch::ElasticsearchDestinationDto,
    event_hubs::EventHubsDestinationDto, gcs::GcsDestinationDto, http::HttpDestinationDto,
    iceberg::IcebergDestinationDto, kafka::KafkaDestinationDto, log_analytics::LogAnalyticsDestinationDto,
    otlp::OtlpDestinationDto, prometheus::PrometheusDestinationDto, s3::S3DestinationDto, sftp::SftpDestinationDto,
    splunk_hec::SplunkHecDestinationDto,
};

/// What one kind of destination answers about itself. Each kind implements it in its own file, beside its struct.
pub(super) trait DestinationSpec {
    fn check(&self, d: &DestinationDto, errors: &mut ItemErrors<'_>);
    /// What it loads as. `of` names it in what the loaded destination says about itself.
    fn to_kind(&self, of: &str) -> DestinationKind;
    /// Every address this destination reaches over HTTP: the service, and where it obtains credentials when that
    /// is configured apart, by the field naming each. Empty from a kind that is not reached over HTTP.
    fn urls(&self) -> Vec<(&'static str, &str)>;
    /// Every credential this destination names, by the field naming it. Exhaustive on purpose: a kind added here
    /// must answer, or its credentials would go unnamed, unchecked at deploy and left behind on a version move.
    fn secrets(&self) -> Vec<(&'static str, &SecretRefDto)>;
    /// `secrets`, for the leader to move one onto a new version. Exhaustive as `secrets` is.
    fn secrets_mut(&mut self) -> Vec<&mut SecretRefDto>;
    /// The TLS this destination connects out under. `None` from a kind that speaks none.
    fn tls(&self) -> Option<TlsDto>;
    /// Whether the bundle states this destination's `bufferBytes`. One it leaves out the worker sizes to its memory.
    fn states_buffer(&self) -> bool;
}

impl DestinationKindDto {
    pub(super) fn spec(&self) -> &dyn DestinationSpec {
        match self {
            DestinationKindDto::Kafka(k) => k,
            DestinationKindDto::EventHubs(k) => k,
            DestinationKindDto::Elasticsearch(k) => k,
            DestinationKindDto::SplunkHec(k) => k,
            DestinationKindDto::S3(k) => k,
            DestinationKindDto::Gcs(k) => k,
            DestinationKindDto::AzureBlob(k) => k,
            DestinationKindDto::Sftp(k) => k,
            DestinationKindDto::Http(k) => k,
            DestinationKindDto::Prometheus(k) => k,
            DestinationKindDto::Otlp(k) => k,
            DestinationKindDto::LogAnalytics(k) => k,
            DestinationKindDto::Iceberg(k) => k,
            DestinationKindDto::Delta(k) => k,
        }
    }

    pub(super) fn spec_mut(&mut self) -> &mut dyn DestinationSpec {
        match self {
            DestinationKindDto::Kafka(k) => k,
            DestinationKindDto::EventHubs(k) => k,
            DestinationKindDto::Elasticsearch(k) => k,
            DestinationKindDto::SplunkHec(k) => k,
            DestinationKindDto::S3(k) => k,
            DestinationKindDto::Gcs(k) => k,
            DestinationKindDto::AzureBlob(k) => k,
            DestinationKindDto::Sftp(k) => k,
            DestinationKindDto::Http(k) => k,
            DestinationKindDto::Prometheus(k) => k,
            DestinationKindDto::Otlp(k) => k,
            DestinationKindDto::LogAnalytics(k) => k,
            DestinationKindDto::Iceberg(k) => k,
            DestinationKindDto::Delta(k) => k,
        }
    }

    /// Whether the bundle states this destination's `bufferBytes`. One it leaves out the worker sizes to its memory.
    pub fn states_buffer(&self) -> bool {
        self.spec().states_buffer()
    }
}

impl DestinationDto {
    /// Every address this destination reaches over HTTP: the service, and where it obtains credentials when that
    /// is configured apart, by the field naming each. Empty from a kind that is not reached over HTTP.
    pub fn urls(&self) -> Vec<(&'static str, &str)> {
        self.kind.spec().urls()
    }

    /// Every credential this destination names, by the field naming it. Exhaustive on purpose: a kind added here
    /// must answer, or its credentials would go unnamed, unchecked at deploy and left behind on a version move.
    pub fn secrets(&self) -> Vec<(&'static str, &SecretRefDto)> {
        self.kind.spec().secrets()
    }

    /// `secrets`, for the leader to move one onto a new version. Exhaustive as `secrets` is.
    pub fn secrets_mut(&mut self) -> Vec<&mut SecretRefDto> {
        self.kind.spec_mut().secrets_mut()
    }

    /// The TLS this destination connects out under. `None` from a kind that speaks none.
    pub fn tls(&self) -> Option<TlsDto> {
        self.kind.spec().tls()
    }
}
