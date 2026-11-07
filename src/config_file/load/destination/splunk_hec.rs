use super::super::*;
use super::DestinationSpec;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SplunkHecDestinationDto {
    /// The collector's base address; the destination adds the endpoint paths itself.
    pub url: String,
    /// Zone -> the address a worker in that zone sends to instead of `url`.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub zone_urls: BTreeMap<String, String>,
    /// The credential holding the HEC token, never the token itself.
    pub token: SecretRefDto,
    /// Where a row with no `__destination.index` goes. Absent leaves the token's own default index. The raw
    /// endpoint files every row here, so it refuses a row whose `__destination.index` names another.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sourcetype: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    /// `"event"` (default) or `"raw"`; see `plan::topology::Endpoint`.
    #[serde(default, skip_serializing_if = "EndpointDto::is_default")]
    pub endpoint: EndpointDto,
    /// `"json"` (default) or `"text"`; the raw endpoint also takes `"bytes"`.
    #[serde(default, skip_serializing_if = "FormatDto::is_default")]
    pub format: FormatDto,
    /// The field `"text"` or `"bytes"` writes. Absent is `_raw` for `"text"`, `__source.bytes` for `"bytes"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
    /// `"indexer"` (default) or `"receipt"`; see `plan::topology::Acknowledgement`.
    #[serde(default, skip_serializing_if = "AcknowledgementDto::is_default")]
    pub acknowledgement: AcknowledgementDto,
    #[serde(flatten)]
    pub tls: TlsDto,
    #[serde(default, skip_serializing_if = "InFlight::is_default")]
    pub in_flight: InFlight,
    #[serde(default, skip_serializing_if = "RequestBytes::is_default")]
    pub max_request_bytes: RequestBytes,
    /// Unset means the default, or one whole request if that is larger.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub buffer_bytes: Option<BufferBytes>,
    /// Seconds one batch may take to deliver, its acknowledgement included, before it is a stall.
    #[serde(default, skip_serializing_if = "StallAfter::is_default")]
    pub stall_after_secs: StallAfter,
    #[serde(flatten, default)]
    pub unknown: NoUnknownKeys,
}

impl DestinationSpec for SplunkHecDestinationDto {
    fn check(&self, d: &DestinationDto, errors: &mut ItemErrors<'_>) {
        let Self {
            url,
            zone_urls,
            index,
            sourcetype,
            source,
            host,
            endpoint,
            format,
            field,
            tls,
            in_flight,
            max_request_bytes,
            buffer_bytes,
            stall_after_secs,
            ..
        } = self;
        check_field(
            &d.id,
            matches!(format, FormatDto::Text | FormatDto::Bytes),
            field,
            errors,
        );
        refuse_avro(*format, &d.id, errors);
        // An event is a JSON string, so its text is UTF-8; only the raw endpoint takes bytes that may not be.
        if *format == FormatDto::Bytes && *endpoint == EndpointDto::Event {
            errors.at("format").push(format!(
                "destination {:?} writes format \"bytes\", which only the raw endpoint takes",
                d.id
            ));
        }
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
                limit_why: "Splunk refuses a body over its own max_content_length, and Splunk Cloud over \
                                a megabyte",
                buffer_bytes: *buffer_bytes,
                stall_after: *stall_after_secs,
            },
            errors,
        );
        // The event and ack paths are joined to the base address, so a pasted endpoint would get its path
        // twice and every request a 404.
        if let Some(at) = url.find("/services/collector") {
            errors.at("url").push(format!(
                "destination {:?} is reached at {url:?}, which names a collector endpoint. Give the base \
                     address, {:?}: the destination adds the endpoint itself",
                d.id,
                &url[..at]
            ));
        }
        // Unset is the way to say "none"; an empty one is a field left half-filled.
        for (what, value) in [
            ("index", index),
            ("sourcetype", sourcetype),
            ("source", source),
            ("host", host),
        ] {
            if value.as_deref().is_some_and(blank) {
                errors.at(what).push(format!(
                    "destination {:?} has an empty {what}; name one, or leave it out",
                    d.id
                ));
            }
        }
    }

    fn to_kind(&self, of: &str) -> DestinationKind {
        let Self {
            unknown: _,
            url,
            zone_urls,
            token,
            index,
            sourcetype,
            source,
            host,
            endpoint,
            format,
            field,
            acknowledgement,
            tls,
            in_flight,
            max_request_bytes,
            buffer_bytes,
            stall_after_secs,
        } = self;
        DestinationKind::SplunkHec(SplunkHecDestinationConfig {
            payload: to_payload(*format, field),
            url: url.clone(),
            zone_urls: zone_urls.clone(),
            token: to_secret_ref(token),
            index: index.clone(),
            sourcetype: sourcetype.clone(),
            source: source.clone(),
            host: host.clone(),
            endpoint: match endpoint {
                EndpointDto::Event => Endpoint::Event,
                EndpointDto::Raw => Endpoint::Raw,
            },
            acknowledgement: match acknowledgement {
                AcknowledgementDto::Indexer => Acknowledgement::Indexer,
                AcknowledgementDto::Receipt => Acknowledgement::Receipt,
            },
            tls: to_tls(tls, of, Side::Client),
            in_flight: in_flight.start(affords(*buffer_bytes, max_request_bytes.0)),
            max_request_bytes: max_request_bytes.0,
            buffer_bytes: effective_buffer_bytes(*buffer_bytes, max_request_bytes.0),
            stall_after: std::time::Duration::from_secs(stall_after_secs.0),
        })
    }

    fn urls(&self) -> Vec<(&'static str, &str)> {
        std::iter::once(("url", self.url.as_str()))
            .chain(self.zone_urls.values().map(|u| ("zoneUrls", u.as_str())))
            .collect()
    }

    fn secrets(&self) -> Vec<(&'static str, &SecretRefDto)> {
        vec![("token", &self.token)]
    }

    fn secrets_mut(&mut self) -> Vec<&mut SecretRefDto> {
        vec![&mut self.token]
    }

    fn tls(&self) -> Option<TlsDto> {
        Some(self.tls.clone())
    }

    fn states_buffer(&self) -> bool {
        self.buffer_bytes.is_some()
    }
}
