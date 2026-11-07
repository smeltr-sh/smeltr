use super::super::*;
use super::SourceSpec;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HttpReceiverSourceDto {
    #[serde(flatten)]
    pub listener: ReceiverSettingsDto,
    #[serde(flatten)]
    pub connections: ConnectionLimitsDto,
    #[serde(flatten)]
    pub tls: ServedTlsDto,
    /// Whether a request is answered only once every destination has its records, rather than as soon as it is
    /// received.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub answer_after_delivery: bool,
    /// The path senders POST to.
    #[serde(default = "root_path", skip_serializing_if = "is_root_path")]
    pub path: String,
    /// Token name -> the credential holding a token a sender may present as `Authorization: Bearer`.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub tokens: BTreeMap<String, SecretRefDto>,
    /// `"body"` (default), `"lines"` or `"array"`.
    #[serde(default, skip_serializing_if = "ApiRecordsDto::is_default")]
    pub records: ApiRecordsDto,
    /// The JSON pointer to the array `"array"` reads, such as `/events`. Absent is a body that is the array.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub records_at: Option<String>,
    #[serde(flatten, default)]
    pub unknown: NoUnknownKeys,
}

impl SourceSpec for HttpReceiverSourceDto {
    fn check(&self, s: &SourceDto, errors: &mut ItemErrors<'_>) {
        let Self {
            listener,
            connections,
            tls,
            path,
            tokens,
            records,
            records_at,
            ..
        } = self;
        let what = format!("source {:?}", s.id);
        check_receiver(&what, listener, tls, tokens, errors);
        check_connection_limits(&what, connections, true, errors);
        check_records_at(&what, *records, records_at, errors);
        if !path.starts_with('/') || path.contains(['?', '#', ':', '*']) || path.chars().any(char::is_whitespace) {
            errors.at("path").push(format!(
                "{what} takes requests at {path:?}; a path starts with / and holds no ?, #, :, * or space"
            ));
        } else if path == crate::io::receiver::HEALTH_PATH {
            errors.at("path").push(format!(
                "{what} takes requests at {path:?}, which is where it answers health checks; give it another \
                     path"
            ));
        }
    }

    fn to_kind(&self, of: &str) -> SourceKind {
        let Self {
            unknown: _,
            listener,
            connections,
            tls,
            path,
            tokens,
            records,
            records_at,
            answer_after_delivery,
        } = self;
        receiver_config(
            crate::plan::topology::Receiver::Http(crate::plan::topology::HttpReceiverConfig {
                path: path.clone(),
                records: match records {
                    ApiRecordsDto::Body => ApiRecords::Body,
                    ApiRecordsDto::Lines => ApiRecords::Lines,
                    ApiRecordsDto::Array => ApiRecords::Array(records_at.clone().unwrap_or_default()),
                },
            }),
            *answer_after_delivery,
            listener,
            tls,
            tokens.iter(),
            to_connection_limits(connections),
            of,
        )
    }

    fn secrets(&self) -> Vec<(&'static str, &SecretRefDto)> {
        self.tokens.values().map(|r| ("tokens", r)).collect()
    }

    fn secrets_mut(&mut self) -> Vec<&mut SecretRefDto> {
        self.tokens.values_mut().collect()
    }

    fn urls(&self) -> Vec<(&'static str, &str)> {
        Vec::new()
    }

    fn tls(&self) -> Option<TlsDto> {
        Some(self.tls.tls())
    }

    fn receiver_port(&self) -> Option<u16> {
        Some(self.listener.port)
    }

    fn declared_meta(&self) -> SourceMeta {
        SourceMeta::HTTP_RECEIVER
    }
}

fn root_path() -> String {
    "/".to_string()
}

fn is_root_path(path: &str) -> bool {
    path == "/"
}
