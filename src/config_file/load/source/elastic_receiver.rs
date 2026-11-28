use super::super::shared::*;
use super::receiver::{NoFramingDto, ReceiverKindDto, ReceiverSourceDto};
use crate::plan::topology::Receiver;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub type ElasticReceiverSourceDto = ReceiverSourceDto<ElasticReceiverDto, NoFramingDto>;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ElasticReceiverDto {
    /// Whether a request is answered only once every destination has its records, rather than as soon as it is
    /// received.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub(crate) answer_after_delivery: bool,
    /// Token name -> the credential holding a token a sender may present as `Authorization: Bearer`, as
    /// `Authorization: ApiKey` (the key as sent, or the `id:key` it encodes), or as the password of
    /// `Authorization: Basic`, whose user is not read.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub(crate) tokens: BTreeMap<String, SecretRefDto>,
}

impl ReceiverKindDto for ElasticReceiverDto {
    type Framing = NoFramingDto;

    fn meta(&self) -> SourceMeta {
        SourceMeta::ELASTIC_RECEIVER
    }

    fn receiver(&self, _: &NoFramingDto) -> Receiver {
        Receiver::Elastic
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
