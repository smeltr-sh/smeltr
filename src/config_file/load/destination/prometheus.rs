use super::super::*;
use super::DestinationSpec;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PrometheusDestinationDto {
    /// The whole address each request is POSTed to.
    pub url: String,
    /// `"1.0"` (default) or `"2.0"`.
    #[serde(default, skip_serializing_if = "RemoteWriteVersionDto::is_default")]
    pub version: RemoteWriteVersionDto,
    /// Header name -> its value, committed to the config repository.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub headers: BTreeMap<String, String>,
    /// Header name -> the credential holding its value.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub credentials: BTreeMap<String, SecretRefDto>,
    #[serde(flatten)]
    pub tls: TlsDto,
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

impl DestinationSpec for PrometheusDestinationDto {
    fn check(&self, d: &DestinationDto, errors: &mut ItemErrors<'_>) {
        let Self {
            url,
            version: _,
            headers,
            credentials,
            tls,
            max_request_bytes,
            buffer_bytes,
            stall_after_secs,
            unknown: _,
        } = self;
        check_http_delivery(
            &HttpDelivery {
                id: &d.id,
                url,
                url_field: "url",
                zone_urls: None,
                request_field: "maxRequestBytes",
                tls,
                in_flight: None,
                request_bytes: max_request_bytes.0,
                request_limits: (RequestBytes::MIN, RequestBytes::MAX),
                request_what: "a request size",
                limit_why: "a request is held whole in memory until the store answers it",
                buffer_bytes: *buffer_bytes,
                stall_after: *stall_after_secs,
            },
            errors,
        );
        check_http_headers(
            &format!("destination {:?}", d.id),
            headers,
            credentials,
            PROMETHEUS_HEADERS,
            "as remote_write asks",
            errors,
        );
    }

    fn to_kind(&self, of: &str) -> DestinationKind {
        let Self {
            unknown: _,
            url,
            version,
            headers,
            credentials,
            tls,
            max_request_bytes,
            buffer_bytes,
            stall_after_secs,
        } = self;
        DestinationKind::Prometheus(PrometheusDestinationConfig {
            url: url.trim().to_string(),
            version: match version {
                RemoteWriteVersionDto::V1 => RemoteWriteVersion::V1,
                RemoteWriteVersionDto::V2 => RemoteWriteVersion::V2,
            },
            headers: headers.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
            credentials: credentials.iter().map(|(k, r)| (k.clone(), to_secret_ref(r))).collect(),
            tls: to_tls(tls, of, Side::Client),
            max_request_bytes: max_request_bytes.0,
            buffer_bytes: effective_buffer_bytes(*buffer_bytes, max_request_bytes.0),
            stall_after: std::time::Duration::from_secs(stall_after_secs.0),
        })
    }

    fn urls(&self) -> Vec<(&'static str, &str)> {
        vec![("url", &self.url)]
    }

    fn secrets(&self) -> Vec<(&'static str, &SecretRefDto)> {
        self.credentials.values().map(|r| ("credentials", r)).collect()
    }

    fn secrets_mut(&mut self) -> Vec<&mut SecretRefDto> {
        self.credentials.values_mut().collect()
    }

    fn tls(&self) -> Option<TlsDto> {
        Some(self.tls.clone())
    }

    fn states_buffer(&self) -> bool {
        self.buffer_bytes.is_some()
    }
}
