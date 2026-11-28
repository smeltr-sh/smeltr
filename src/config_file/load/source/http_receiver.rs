use super::super::shared::*;
use super::receiver::{NoFramingDto, ReceiverKindDto, ReceiverSourceDto};
use crate::plan::topology::{HttpReceiverConfig, Receiver};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub type HttpReceiverSourceDto = ReceiverSourceDto<HttpReceiverDto, NoFramingDto>;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HttpReceiverDto {
    /// Whether a request is answered only once every destination has its records, rather than as soon as it is
    /// received.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub(crate) answer_after_delivery: bool,
    /// The path senders POST to.
    #[serde(default = "root_path", skip_serializing_if = "is_root_path")]
    pub(crate) path: String,
    /// Token name -> the credential holding a token a sender may present as `Authorization: Bearer`.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub(crate) tokens: BTreeMap<String, SecretRefDto>,
    /// `"body"` (default), `"lines"` or `"array"`.
    #[serde(default, skip_serializing_if = "ApiRecordsDto::is_default")]
    pub(crate) records: ApiRecordsDto,
    /// The JSON pointer to the array `"array"` reads, such as `/events`. Absent is a body that is the array.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) records_at: Option<String>,
}

impl ReceiverKindDto for HttpReceiverDto {
    type Framing = NoFramingDto;

    fn meta(&self) -> SourceMeta {
        SourceMeta::HTTP_RECEIVER
    }

    fn check(&self, _: &SourceDto, what: &str, _: &ReceiverSettingsDto, _: &NoFramingDto, errors: &mut ItemErrors<'_>) {
        let Self {
            path,
            records,
            records_at,
            ..
        } = self;
        check_records_at(what, *records, records_at, errors);
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

    fn receiver(&self, _: &NoFramingDto) -> Receiver {
        Receiver::Http(HttpReceiverConfig {
            path: self.path.clone(),
            records: match self.records {
                ApiRecordsDto::Body => ApiRecords::Body,
                ApiRecordsDto::Lines => ApiRecords::Lines,
                ApiRecordsDto::Array => ApiRecords::Array(self.records_at.clone().unwrap_or_default()),
            },
        })
    }

    fn answer_after_delivery(&self) -> bool {
        self.answer_after_delivery
    }

    fn tokens(&self) -> Vec<(&String, &SecretRefDto)> {
        self.tokens.iter().collect()
    }

    fn tokens_mut(&mut self) -> Vec<&mut SecretRefDto> {
        self.tokens.values_mut().collect()
    }
}

fn root_path() -> String {
    "/".to_string()
}

fn is_root_path(path: &str) -> bool {
    path == "/"
}
