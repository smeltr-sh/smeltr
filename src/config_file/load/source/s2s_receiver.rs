use super::super::shared::*;
use super::receiver::{NoFramingDto, ReceiverKindDto, ReceiverSourceDto};
use crate::plan::topology::{Receiver, S2sReceiverConfig};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub type S2sReceiverSourceDto = ReceiverSourceDto<S2sReceiverDto, NoFramingDto>;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct S2sReceiverDto {
    /// Token name -> the credential holding a token a forwarder may present, its outputs.conf `token`. A
    /// forwarder presenting none, or another, is cut off.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub tokens: BTreeMap<String, SecretRefDto>,
    /// Sourcetype -> where its events end. A sourcetype not named here is an event per line.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub sourcetypes: BTreeMap<String, SourcetypeDto>,
}

impl ReceiverKindDto for S2sReceiverDto {
    type Framing = NoFramingDto;

    fn meta(&self) -> SourceMeta {
        SourceMeta::S2S_RECEIVER
    }

    fn check(&self, s: &SourceDto, _: &str, _: &ReceiverSettingsDto, _: &NoFramingDto, errors: &mut ItemErrors<'_>) {
        check_sourcetypes(&s.id, &self.sourcetypes, errors);
    }

    fn receiver(&self, _: &NoFramingDto) -> Receiver {
        Receiver::S2s(S2sReceiverConfig {
            sourcetypes: to_line_breakers(&self.sourcetypes),
        })
    }

    fn tokens(&self) -> Vec<(&String, &SecretRefDto)> {
        self.tokens.iter().collect()
    }

    fn tokens_mut(&mut self) -> Vec<&mut SecretRefDto> {
        self.tokens.values_mut().collect()
    }
}
