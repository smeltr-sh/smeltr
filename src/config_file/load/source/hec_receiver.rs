use super::super::*;
use super::SourceSpec;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HecReceiverSourceDto {
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
    /// Token name -> a token a sender may present as `Authorization: Splunk`, and whether its senders use
    /// indexer acknowledgement.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub tokens: BTreeMap<String, HecTokenDto>,
    /// Sourcetype -> where a raw request's events end, by the request's `sourcetype`. A sourcetype not named
    /// here is an event per line.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub sourcetypes: BTreeMap<String, SourcetypeDto>,
    #[serde(flatten, default)]
    pub unknown: NoUnknownKeys,
}

impl SourceSpec for HecReceiverSourceDto {
    fn check(&self, s: &SourceDto, errors: &mut ItemErrors<'_>) {
        let what = format!("source {:?}", s.id);
        check_receiver(&what, &self.listener, &self.tls, &self.tokens, errors);
        check_connection_limits(&what, &self.connections, true, errors);
        check_sourcetypes(&s.id, &self.sourcetypes, errors);
    }

    fn to_kind(&self, of: &str) -> SourceKind {
        let Self {
            unknown: _,
            listener,
            connections,
            tls,
            tokens,
            sourcetypes,
            answer_after_delivery,
        } = self;
        receiver_config(
            crate::plan::topology::Receiver::Hec(crate::plan::topology::HecReceiverConfig {
                acknowledging: tokens
                    .iter()
                    .filter(|(_, t)| t.acknowledgement)
                    .map(|(name, _)| name.clone())
                    .collect(),
                sourcetypes: to_line_breakers(sourcetypes),
            }),
            *answer_after_delivery,
            listener,
            tls,
            tokens.iter().map(|(name, t)| (name, &t.token)),
            to_connection_limits(connections),
            of,
        )
    }

    fn secrets(&self) -> Vec<(&'static str, &SecretRefDto)> {
        self.tokens.values().map(|t| ("tokens", &t.token)).collect()
    }

    fn secrets_mut(&mut self) -> Vec<&mut SecretRefDto> {
        self.tokens.values_mut().map(|t| &mut t.token).collect()
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
        SourceMeta::HEC_RECEIVER
    }
}
