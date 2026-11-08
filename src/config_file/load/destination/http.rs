use super::super::*;
use super::DestinationSpec;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HttpDestinationDto {
    /// The whole address each request is POSTed to.
    pub url: String,
    /// Zone -> the address a worker in that zone sends to instead of `url`.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub zone_urls: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "RequestBodyDto::is_default")]
    pub body: RequestBodyDto,
    /// `"json"` (default), `"text"` or `"bytes"`; bytes are not a JSON array's element.
    #[serde(default, skip_serializing_if = "FormatDto::is_default")]
    pub format: FormatDto,
    /// The field `"text"` or `"bytes"` writes. Absent is `_raw` for `"text"`, `__source.bytes` for `"bytes"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
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
    #[serde(default, skip_serializing_if = "HttpRequestBytes::is_default")]
    pub max_request_bytes: HttpRequestBytes,
    /// Unset means the default, or one whole request if that is larger.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub buffer_bytes: Option<BufferBytes>,
    /// Seconds one batch may take to deliver before it is a stall.
    #[serde(default, skip_serializing_if = "StallAfter::is_default")]
    pub stall_after_secs: StallAfter,
    #[serde(flatten, default)]
    pub unknown: NoUnknownKeys,
}

impl DestinationSpec for HttpDestinationDto {
    fn check(&self, d: &DestinationDto, errors: &mut ItemErrors<'_>) {
        let Self {
            url,
            zone_urls,
            body: _,
            format,
            field,
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
                zone_urls: Some(zone_urls),
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
        check_field(
            &d.id,
            matches!(format, FormatDto::Text | FormatDto::Bytes),
            field,
            errors,
        );
        refuse_avro(*format, &d.id, errors);
        check_http_compression(&d.id, compression, errors);
        check_http_headers(
            &format!("destination {:?}", d.id),
            headers,
            credentials,
            DESTINATION_HEADERS,
            "from the body and compression",
            errors,
        );
    }

    fn to_kind(&self, of: &str) -> DestinationKind {
        let Self {
            unknown: _,
            url,
            zone_urls,
            body,
            format,
            field,
            compression,
            headers,
            credentials,
            tls,
            in_flight,
            max_request_bytes,
            buffer_bytes,
            stall_after_secs,
        } = self;
        DestinationKind::Http(HttpDestinationConfig {
            zone_urls: zone_urls
                .iter()
                .map(|(zone, url)| (zone.clone(), as_parsed(url)))
                .collect(),
            body: match body {
                RequestBodyDto::Lines => RequestBody::Lines,
                RequestBodyDto::JsonArray => RequestBody::JsonArray,
                RequestBodyDto::Single => RequestBody::Single,
            },
            payload: to_payload(*format, field),
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
        })
    }

    fn urls(&self) -> Vec<(&'static str, &str)> {
        std::iter::once(("url", self.url.as_str()))
            .chain(self.zone_urls.values().map(|u| ("zoneUrls", u.as_str())))
            .collect()
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
