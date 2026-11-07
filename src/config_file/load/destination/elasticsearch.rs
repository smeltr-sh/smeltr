use super::super::*;
use super::DestinationSpec;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ElasticsearchDestinationDto {
    pub url: String,
    /// Zone -> the address a worker in that zone sends to instead of `url`.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub zone_urls: BTreeMap<String, String>,
    pub default_index: String,
    #[serde(flatten)]
    pub tls: TlsDto,
    /// The credential holding the API key or a whole `Authorization` header value, never the value itself.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth: Option<SecretRefDto>,
    /// Basic auth, in place of `auth`: `username` and `password` together.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    /// The credential holding `username`'s password.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub password: Option<SecretRefDto>,
    /// `"auto"` (default), `"dedupe"`, or `"overwrite"`; see `plan::topology::DocIdPolicy`.
    #[serde(default, skip_serializing_if = "DocIdPolicyDto::is_default")]
    pub doc_id: DocIdPolicyDto,
    /// The row field holding the document identity. Omitted, the pipeline must set `__destination.id`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id_field: Option<String>,
    #[serde(default, skip_serializing_if = "InFlight::is_default")]
    pub in_flight: InFlight,
    #[serde(default, skip_serializing_if = "BulkBytes::is_default")]
    pub max_bulk_bytes: BulkBytes,
    /// Unset means the default, or one whole bulk request if that is larger: raising `max_bulk_bytes` alone must
    /// never leave a destination unable to hand a request over.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub buffer_bytes: Option<BufferBytes>,
    /// Seconds one batch may take to deliver before it is a stall.
    #[serde(default, skip_serializing_if = "StallAfter::is_default")]
    pub stall_after_secs: StallAfter,
    /// Whether a worker learns the cluster's nodes from `url` and spreads its requests across them.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub sniff: bool,
    /// The node attribute naming a node's zone, which sniffing prefers the worker's own of. Absent is `zone`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub zone_attribute: Option<String>,
    #[serde(flatten, default)]
    pub unknown: NoUnknownKeys,
}

impl DestinationSpec for ElasticsearchDestinationDto {
    fn check(&self, d: &DestinationDto, errors: &mut ItemErrors<'_>) {
        let Self {
            url,
            zone_urls,
            default_index,
            tls,
            in_flight,
            max_bulk_bytes,
            buffer_bytes,
            stall_after_secs,
            sniff,
            zone_attribute,
            auth,
            username,
            password,
            ..
        } = self;
        check_elastic_sign_in(&d.id, auth, username, password, errors);
        match zone_attribute {
            Some(_) if !sniff => errors.at("zoneAttribute").push(format!(
                "destination {:?} names a zone attribute but does not sniff; turn sniffing on, or take \
                     the zone attribute off",
                d.id
            )),
            Some(a) if blank(a) => errors
                .at("zoneAttribute")
                .push(format!("destination {:?} names a blank zone attribute", d.id)),
            _ => {}
        }
        check_http_delivery(
            &HttpDelivery {
                id: &d.id,
                url,
                url_field: "url",
                zone_urls: Some(zone_urls),
                request_field: "maxBulkBytes",
                tls,
                in_flight: Some(*in_flight),
                request_bytes: max_bulk_bytes.0,
                request_limits: (BulkBytes::MIN, BulkBytes::MAX),
                request_what: "a bulk size",
                limit_why: "Elasticsearch refuses a body over its own http.max_content_length, which \
                                defaults to 100 MB",
                buffer_bytes: *buffer_bytes,
                stall_after: *stall_after_secs,
            },
            errors,
        );
        if blank(default_index) {
            errors
                .at("defaultIndex")
                .push(format!("destination {:?} has no default index", d.id));
        }
    }

    fn to_kind(&self, of: &str) -> DestinationKind {
        let Self {
            unknown: _,
            url,
            zone_urls,
            default_index,
            tls,
            auth,
            username,
            password,
            doc_id,
            id_field,
            in_flight,
            max_bulk_bytes,
            buffer_bytes,
            stall_after_secs,
            sniff,
            zone_attribute,
        } = self;
        DestinationKind::Elasticsearch(crate::plan::topology::ElasticDestinationConfig {
            sniff: sniff.then(|| crate::plan::topology::Sniff {
                zone_attribute: zone_attribute
                    .as_deref()
                    .unwrap_or(crate::plan::topology::Sniff::DEFAULT_ZONE_ATTRIBUTE)
                    .trim()
                    .to_string(),
                zone: None,
            }),
            tls: to_tls(tls, of, Side::Client),
            url: url.clone(),
            zone_urls: zone_urls.clone(),
            default_index: default_index.clone(),
            auth: auth.as_ref().map(to_secret_ref),
            // Validation refused a username without a password, and a password without a username.
            basic: username.clone().zip(password.as_ref().map(to_secret_ref)),
            doc_id: match doc_id {
                crate::config_file::dto::DocIdPolicyDto::Auto => DocIdPolicy::Auto,
                crate::config_file::dto::DocIdPolicyDto::Dedupe => DocIdPolicy::Dedupe,
                crate::config_file::dto::DocIdPolicyDto::Overwrite => DocIdPolicy::Overwrite,
            },
            id_field: id_field.clone(),
            in_flight: in_flight.start(affords(*buffer_bytes, max_bulk_bytes.0)),
            max_bulk_bytes: max_bulk_bytes.0,
            buffer_bytes: effective_buffer_bytes(*buffer_bytes, max_bulk_bytes.0),
            stall_after: std::time::Duration::from_secs(stall_after_secs.0),
        })
    }

    fn urls(&self) -> Vec<(&'static str, &str)> {
        std::iter::once(("url", self.url.as_str()))
            .chain(self.zone_urls.values().map(|u| ("zoneUrls", u.as_str())))
            .collect()
    }

    fn secrets(&self) -> Vec<(&'static str, &SecretRefDto)> {
        let Self { auth, password, .. } = self;
        (auth.iter().map(|r| ("auth", r)))
            .chain(password.iter().map(|r| ("password", r)))
            .collect()
    }

    fn secrets_mut(&mut self) -> Vec<&mut SecretRefDto> {
        self.auth.iter_mut().chain(self.password.iter_mut()).collect()
    }

    fn tls(&self) -> Option<TlsDto> {
        Some(self.tls.clone())
    }

    fn states_buffer(&self) -> bool {
        self.buffer_bytes.is_some()
    }
}
