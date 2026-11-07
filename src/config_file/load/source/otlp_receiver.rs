use super::super::*;
use super::{SourceSpec, check_intake};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OtlpReceiverSourceDto {
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
    /// The signal it takes. Absent is logs.
    #[serde(default, skip_serializing_if = "is_logs")]
    pub signal: Signal,
    /// Token name -> the credential holding a token an exporter may present as `Authorization: Bearer`.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub tokens: BTreeMap<String, SecretRefDto>,
    #[serde(flatten, default)]
    pub unknown: NoUnknownKeys,
}

impl SourceSpec for OtlpReceiverSourceDto {
    fn check(&self, s: &SourceDto, errors: &mut ItemErrors<'_>) {
        check_intake(s, &self.listener, &self.connections, &self.tls, &self.tokens, errors);
    }

    fn to_kind(&self, of: &str) -> SourceKind {
        let Self {
            unknown: _,
            listener,
            connections,
            tls,
            signal,
            tokens,
            answer_after_delivery,
        } = self;
        receiver_config(
            crate::plan::topology::Receiver::Otlp(*signal),
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
        SourceMeta::otlp(self.signal)
    }
}
