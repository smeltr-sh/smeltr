use super::super::*;
use super::DestinationSpec;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LogAnalyticsDestinationDto {
    /// The rule's logs ingestion endpoint, or the data collection endpoint it is linked to.
    pub endpoint: String,
    /// The rule's immutable id, `dcr-` and 32 hex digits.
    pub rule_id: String,
    /// The rule's stream the rows go to, such as `Custom-Events_CL`.
    pub stream: String,
    #[serde(flatten)]
    pub sign_in: EntraSignInDto,
    #[serde(flatten)]
    pub tls: TlsDto,
    #[serde(default, skip_serializing_if = "InFlight::is_default")]
    pub in_flight: InFlight,
    #[serde(default, skip_serializing_if = "RequestBytes::is_default")]
    pub max_request_bytes: RequestBytes,
    /// Unset means the default, or one whole request if that is larger.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub buffer_bytes: Option<BufferBytes>,
    /// Seconds one batch may take to deliver before it is a stall.
    #[serde(default, skip_serializing_if = "StallAfter::is_default")]
    pub stall_after_secs: StallAfter,
    #[serde(flatten, default)]
    pub unknown: NoUnknownKeys,
}

impl DestinationSpec for LogAnalyticsDestinationDto {
    fn check(&self, d: &DestinationDto, errors: &mut ItemErrors<'_>) {
        let Self {
            endpoint,
            rule_id,
            stream,
            sign_in,
            tls,
            in_flight,
            max_request_bytes,
            buffer_bytes,
            stall_after_secs,
            unknown: _,
        } = self;
        check_http_delivery(
            &HttpDelivery {
                id: &d.id,
                url: endpoint,
                url_field: "endpoint",
                zone_urls: None,
                request_field: "maxRequestBytes",
                tls,
                in_flight: Some(*in_flight),
                request_bytes: max_request_bytes.0,
                request_limits: (RequestBytes::MIN, LOG_ANALYTICS_MAX_REQUEST_BYTES),
                request_what: "a request size",
                limit_why: "the Logs Ingestion API takes at most 1 MiB in one call",
                buffer_bytes: *buffer_bytes,
                stall_after: *stall_after_secs,
            },
            errors,
        );
        check_log_analytics_rule(&d.id, endpoint, rule_id, stream, errors);
        check_entra_sign_in(sign_in, &[], &[], &format!("destination {:?}", d.id), errors);
    }

    fn to_kind(&self, of: &str) -> DestinationKind {
        let Self {
            unknown: _,
            endpoint,
            rule_id,
            stream,
            sign_in,
            tls,
            in_flight,
            max_request_bytes,
            buffer_bytes,
            stall_after_secs,
        } = self;
        DestinationKind::LogAnalytics(LogAnalyticsDestinationConfig {
            http: HttpDestinationConfig {
                body: RequestBody::JsonArray,
                compression: crate::plan::topology::Compression::Gzip,
                ..to_http_core(
                    log_analytics_stream_url(endpoint, rule_id, stream),
                    to_tls(tls, of, Side::Client),
                    in_flight,
                    max_request_bytes.0,
                    *buffer_bytes,
                    stall_after_secs,
                )
            },
            sign_in: to_entra_sign_in(sign_in),
        })
    }

    fn urls(&self) -> Vec<(&'static str, &str)> {
        std::iter::once(("endpoint", self.endpoint.as_str()))
            .chain(self.sign_in.urls())
            .collect()
    }

    fn secrets(&self) -> Vec<(&'static str, &SecretRefDto)> {
        self.sign_in.secrets()
    }

    fn secrets_mut(&mut self) -> Vec<&mut SecretRefDto> {
        self.sign_in.secrets_mut()
    }

    fn tls(&self) -> Option<TlsDto> {
        Some(self.tls.clone())
    }

    fn states_buffer(&self) -> bool {
        self.buffer_bytes.is_some()
    }
}
