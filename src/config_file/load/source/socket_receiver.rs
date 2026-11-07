use super::super::*;
use super::SourceSpec;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SocketReceiverSourceDto {
    #[serde(flatten)]
    pub listener: ReceiverSettingsDto,
    #[serde(flatten)]
    pub connections: ConnectionLimitsDto,
    #[serde(flatten)]
    pub tls: ServedTlsDto,
    /// The networks a sender may connect from, as CIDR blocks such as `10.0.0.0/8`. Nothing it receives carries a
    /// token, so this is what admits a sender, unless the receiver names authorities a sender's certificate must
    /// chain to.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allow_from: Vec<String>,
    /// How a record's bytes become text. See [`EncodingLabel`].
    #[serde(default, skip_serializing_if = "EncodingLabel::is_default")]
    pub encoding: EncodingLabel,
    /// What senders send over. Absent is TCP. Over UDP each datagram is a record, served without TLS.
    #[serde(default, skip_serializing_if = "Transport::is_tcp")]
    pub transport: Transport,
    /// TCP only: where one record ends and the next starts.
    #[serde(flatten)]
    pub framing: FramingSettingsDto,
    #[serde(flatten, default)]
    pub unknown: NoUnknownKeys,
}

impl SourceSpec for SocketReceiverSourceDto {
    fn check(&self, s: &SourceDto, errors: &mut ItemErrors<'_>) {
        let Self {
            listener,
            connections,
            tls,
            allow_from,
            encoding,
            transport,
            framing,
            unknown: _,
        } = self;
        let what = format!("source {:?}", s.id);
        check_syslog_receiver(&what, listener, tls, allow_from, *transport, errors);
        check_connection_limits(&what, connections, *transport == Transport::Tcp, errors);
        check_encoding(encoding, &s.id, errors);
        match transport {
            Transport::Udp if framing.is_set() => errors.at("framing").push(format!(
                "{what} sets a framing, which only TCP reads: over UDP a datagram is a record"
            )),
            Transport::Udp => {}
            // A receiver keeps a message to its largest request.
            Transport::Tcp => {
                let max = listener.max_request_bytes.0;
                check_framing(framing, &s.id, max, "message", errors);
            }
        }
    }

    fn to_kind(&self, of: &str) -> SourceKind {
        let Self {
            unknown: _,
            listener,
            connections,
            tls,
            allow_from,
            encoding,
            transport,
            framing,
        } = self;
        receiver_config(
            crate::plan::topology::Receiver::Socket(crate::plan::topology::SocketReceiverConfig {
                // Validation refused a block that does not parse, or a label that names no encoding, before any of
                // this ran.
                allow_from: allow_from.iter().filter_map(|b| b.trim().parse().ok()).collect(),
                encoding: Encoding::for_label(&encoding.0).unwrap_or_default(),
                transport: *transport,
                framing: to_framing(framing),
            }),
            false,
            listener,
            tls,
            std::iter::empty(),
            to_connection_limits(connections),
            of,
        )
    }

    fn secrets(&self) -> Vec<(&'static str, &SecretRefDto)> {
        Vec::new()
    }

    fn secrets_mut(&mut self) -> Vec<&mut SecretRefDto> {
        Vec::new()
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
        self.transport
    }

    fn declared_meta(&self) -> SourceMeta {
        SourceMeta::SOCKET_RECEIVER
    }

    fn encoding(&self) -> Encoding {
        Encoding::for_label(&self.encoding.0).unwrap_or_default()
    }
}
