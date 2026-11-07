use super::super::*;
use super::DestinationSpec;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OtlpDestinationDto {
    /// The endpoint, which each request goes to with its signal's path, `/v1/logs`, `/v1/metrics` or
    /// `/v1/traces`, after it.
    pub url: String,
    /// The signal it sends. Absent is logs.
    #[serde(default, skip_serializing_if = "is_logs")]
    pub signal: Signal,
    /// `"none"` (default) or `"gzip"`.
    #[serde(default, skip_serializing_if = "CompressionDto::is_default")]
    pub compression: CompressionDto,
    /// Header name -> its value, committed to the config repository.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub headers: BTreeMap<String, String>,
    /// Header name -> the credential holding its value.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub credentials: BTreeMap<String, SecretRefDto>,
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

impl DestinationSpec for OtlpDestinationDto {
    fn check(&self, d: &DestinationDto, errors: &mut ItemErrors<'_>) {
        let Self {
            url,
            signal: _,
            compression,
            headers,
            credentials,
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
                url,
                url_field: "url",
                zone_urls: None,
                request_field: "maxRequestBytes",
                tls,
                in_flight: Some(*in_flight),
                request_bytes: max_request_bytes.0,
                request_limits: (RequestBytes::MIN, RequestBytes::MAX),
                request_what: "a request size",
                limit_why: "a request is held whole in memory until the service answers it",
                buffer_bytes: *buffer_bytes,
                stall_after: *stall_after_secs,
            },
            errors,
        );
        check_http_compression(&d.id, compression, errors);
        // An exporter's per-signal endpoint, as `OTEL_EXPORTER_OTLP_LOGS_ENDPOINT` holds one, names its path.
        let signals = [Signal::Logs, Signal::Metrics, Signal::Traces].map(crate::io::otlp::http_path);
        if let Some(path) = (signals.into_iter()).find(|path| url.trim().trim_end_matches('/').ends_with(path)) {
            errors.at("url").push(format!(
                "destination {:?} has a url ending in {path}; remove {path}, which the destination adds \
                     itself for its signal",
                d.id
            ));
        }
        check_http_headers(
            &format!("destination {:?}", d.id),
            headers,
            credentials,
            DESTINATION_HEADERS,
            "as OTLP asks",
            errors,
        );
    }

    fn to_kind(&self, of: &str) -> DestinationKind {
        let Self {
            unknown: _,
            url,
            signal,
            compression,
            headers,
            credentials,
            tls,
            in_flight,
            max_request_bytes,
            buffer_bytes,
            stall_after_secs,
        } = self;
        DestinationKind::Otlp(OtlpDestinationConfig {
            http: HttpDestinationConfig {
                compression: compression.to_plan(),
                headers: headers.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
                credentials: credentials.iter().map(|(k, r)| (k.clone(), to_secret_ref(r))).collect(),
                ..to_http_core(
                    as_parsed(url),
                    to_tls(tls, of, Side::Client),
                    in_flight,
                    max_request_bytes.0,
                    *buffer_bytes,
                    stall_after_secs,
                )
            },
            signal: *signal,
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
