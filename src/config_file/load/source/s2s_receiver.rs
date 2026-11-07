use super::super::*;
use super::SourceSpec;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct S2sReceiverSourceDto {
    #[serde(flatten)]
    pub listener: ReceiverSettingsDto,
    #[serde(flatten)]
    pub connections: ConnectionLimitsDto,
    #[serde(flatten)]
    pub tls: ServedTlsDto,
    /// Token name -> the credential holding a token a forwarder may present, its outputs.conf `token`. A
    /// forwarder presenting none, or another, is cut off.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub tokens: BTreeMap<String, SecretRefDto>,
    /// Sourcetype -> where its events end. A sourcetype not named here is an event per line.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub sourcetypes: BTreeMap<String, SourcetypeDto>,
    #[serde(flatten, default)]
    pub unknown: NoUnknownKeys,
}

impl SourceSpec for S2sReceiverSourceDto {
    fn check(&self, s: &SourceDto, errors: &mut ItemErrors<'_>) {
        let Self {
            listener,
            connections,
            tls,
            tokens,
            sourcetypes,
            unknown: _,
        } = self;
        let what = format!("source {:?}", s.id);
        check_receiver(&what, listener, tls, tokens, errors);
        check_connection_limits(&what, connections, true, errors);
        check_sourcetypes(&s.id, sourcetypes, errors);
    }

    fn to_kind(&self, of: &str) -> SourceKind {
        let Self {
            unknown: _,
            listener,
            connections,
            tls,
            tokens,
            sourcetypes,
        } = self;
        receiver_config(
            crate::plan::topology::Receiver::S2s(crate::plan::topology::S2sReceiverConfig {
                sourcetypes: to_line_breakers(sourcetypes),
            }),
            false,
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
        SourceMeta::S2S_RECEIVER
    }
}
