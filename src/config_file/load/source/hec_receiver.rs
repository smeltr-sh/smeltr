use super::super::shared::*;
use super::receiver::{NoFramingDto, ReceiverKindDto, ReceiverSourceDto};
use crate::plan::topology::{HecReceiverConfig, Receiver};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub type HecReceiverSourceDto = ReceiverSourceDto<HecReceiverDto, NoFramingDto>;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HecReceiverDto {
    /// Whether a request is answered only once every destination has its records, rather than as soon as it is
    /// received.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    answer_after_delivery: bool,
    /// Token name -> a token a sender may present as `Authorization: Splunk`, and whether its senders use
    /// indexer acknowledgement.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub(crate) tokens: BTreeMap<String, HecTokenDto>,
    /// Sourcetype -> where a raw request's events end, by the request's `sourcetype`. A sourcetype not named
    /// here is an event per line.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub(crate) sourcetypes: BTreeMap<String, SourcetypeDto>,
}

impl ReceiverKindDto for HecReceiverDto {
    type Framing = NoFramingDto;

    fn meta(&self) -> SourceMeta {
        SourceMeta::HEC_RECEIVER
    }

    fn check(&self, s: &SourceDto, _: &str, _: &ReceiverSettingsDto, _: &NoFramingDto, errors: &mut ItemErrors<'_>) {
        check_sourcetypes(&s.id, &self.sourcetypes, errors);
    }

    fn receiver(&self, _: &NoFramingDto) -> Receiver {
        Receiver::Hec(HecReceiverConfig {
            acknowledging: (self.tokens.iter())
                .filter(|(_, t)| t.acknowledgement)
                .map(|(name, _)| name.clone())
                .collect(),
            sourcetypes: to_line_breakers(&self.sourcetypes),
        })
    }

    fn answer_after_delivery(&self) -> bool {
        self.answer_after_delivery
    }

    fn tokens(&self) -> Vec<(&String, &SecretRefDto)> {
        self.tokens.iter().map(|(name, t)| (name, &t.token)).collect()
    }

    fn tokens_mut(&mut self) -> Vec<&mut SecretRefDto> {
        self.tokens.values_mut().map(|t| &mut t.token).collect()
    }
}
