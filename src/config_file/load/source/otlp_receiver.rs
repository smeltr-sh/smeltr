use super::super::*;
use super::receiver::{NoFramingDto, ReceiverKindDto, ReceiverSourceDto};
use crate::plan::topology::Receiver;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub type OtlpReceiverSourceDto = ReceiverSourceDto<OtlpReceiverDto, NoFramingDto>;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OtlpReceiverDto {
    /// Whether a request is answered only once every destination has its records, rather than as soon as it is
    /// received.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub answer_after_delivery: bool,
    /// The signal it takes. Absent is logs.
    #[serde(default, skip_serializing_if = "is_logs")]
    pub signal: Signal,
    /// Token name -> the credential holding a token an exporter may present as `Authorization: Bearer`.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub tokens: BTreeMap<String, SecretRefDto>,
}

impl ReceiverKindDto for OtlpReceiverDto {
    type Framing = NoFramingDto;

    fn meta(&self) -> SourceMeta {
        SourceMeta::otlp(self.signal)
    }

    fn receiver(&self, _: &NoFramingDto) -> Receiver {
        Receiver::Otlp(self.signal)
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
