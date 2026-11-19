use super::super::shared::*;
use super::SourceSpec;
use crate::plan::topology::Receiver;
use serde::{Deserialize, Serialize};

/// A source that listens for senders as `Kind` says, on the port, connection limits and TLS every receiver has, and
/// cuts a stream into records as `Framing` says.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReceiverSourceDto<Kind, Framing> {
    #[serde(flatten)]
    pub listener: ReceiverSettingsDto,
    #[serde(flatten)]
    pub connections: ConnectionLimitsDto,
    #[serde(flatten)]
    pub tls: ServedTlsDto,
    #[serde(flatten)]
    pub kind: Kind,
    #[serde(flatten)]
    pub framing: Framing,
    #[serde(flatten, default)]
    pub unknown: NoUnknownKeys,
}

/// The framing of a receiver whose protocol marks where a record ends.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct NoFramingDto {}

/// What one kind of receiver adds to the listener every receiver has.
pub(in super::super) trait ReceiverKindDto {
    type Framing;

    fn meta(&self) -> SourceMeta;
    fn receiver(&self, framing: &Self::Framing) -> Receiver;
    /// Checks only its own settings; whom it admits is checked before, by its tokens or its networks.
    fn check(&self, _: &SourceDto, _: &str, _: &ReceiverSettingsDto, _: &Self::Framing, _: &mut ItemErrors<'_>) {}
    fn answer_after_delivery(&self) -> bool {
        false
    }
    /// Each token's name, with the credential that holds it.
    fn tokens(&self) -> Vec<(&String, &SecretRefDto)> {
        Vec::new()
    }
    fn tokens_mut(&mut self) -> Vec<&mut SecretRefDto> {
        Vec::new()
    }
    /// The networks it admits senders from, for a kind whose senders present no token.
    fn allow_from(&self) -> Option<&[String]> {
        None
    }
    fn transport(&self) -> Transport {
        Transport::Tcp
    }
    fn encoding(&self) -> Encoding {
        Encoding::Fixed(encoding_rs::UTF_8)
    }
}

impl<Kind: ReceiverKindDto> SourceSpec for ReceiverSourceDto<Kind, Kind::Framing> {
    fn check(&self, s: &SourceDto, errors: &mut ItemErrors<'_>) {
        let Self {
            listener,
            connections,
            tls,
            kind,
            framing,
            unknown: _,
        } = self;
        let what = format!("source {:?}", s.id);
        let transport = kind.transport();
        match kind.allow_from() {
            Some(networks) => check_socket_receiver(&what, listener, tls, networks, transport, errors),
            None => {
                let names: Vec<&String> = kind.tokens().into_iter().map(|(name, _)| name).collect();
                check_receiver(&what, listener, tls, &names, errors);
            }
        }
        check_connection_limits(&what, connections, transport == Transport::Tcp, errors);
        kind.check(s, &what, listener, framing, errors);
    }

    fn to_kind(&self, of: &str) -> SourceKind {
        receiver_config(
            self.kind.receiver(&self.framing),
            self.kind.answer_after_delivery(),
            &self.listener,
            &self.tls,
            self.kind.tokens().into_iter(),
            to_connection_limits(&self.connections),
            of,
        )
    }

    fn secrets(&self) -> Vec<(&'static str, &SecretRefDto)> {
        self.kind.tokens().into_iter().map(|(_, r)| ("tokens", r)).collect()
    }

    fn secrets_mut(&mut self) -> Vec<&mut SecretRefDto> {
        self.kind.tokens_mut()
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

    fn receiver_transport(&self) -> Transport {
        self.kind.transport()
    }

    fn declared_meta(&self) -> SourceMeta {
        self.kind.meta()
    }

    fn encoding(&self) -> Encoding {
        self.kind.encoding()
    }
}
