use super::super::*;
use super::SourceSpec;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SyslogReceiverSourceDto {
    #[serde(flatten)]
    pub listener: ReceiverSettingsDto,
    #[serde(flatten)]
    pub connections: ConnectionLimitsDto,
    #[serde(flatten)]
    pub tls: ServedTlsDto,
    /// The networks a sender may connect from, as CIDR blocks such as `10.0.0.0/8`. Syslog carries no token, so
    /// this is what admits a sender, unless the receiver names authorities a sender's certificate
    /// must chain to.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allow_from: Vec<String>,
    /// How a message's bytes become text. Syslog over TCP names no charset, and older senders write their own.
    /// See [`EncodingLabel`].
    #[serde(default, skip_serializing_if = "EncodingLabel::is_default")]
    pub encoding: EncodingLabel,
    /// What senders send over. Absent is TCP. Over UDP each datagram is a message, served without TLS.
    #[serde(default, skip_serializing_if = "Transport::is_tcp")]
    pub transport: Transport,
    /// A pattern whose first capture group is where a message over TCP ends, the group dropped, as Splunk's
    /// `LINE_BREAKER`, for senders that end a message with something other than a newline, or with nothing.
    /// Absent, a message ends at a newline. A message that starts with a digit 1 to 9 is framed by octet counting,
    /// whatever this says.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub line_breaker: Option<String>,
    #[serde(flatten, default)]
    pub unknown: NoUnknownKeys,
}

impl SourceSpec for SyslogReceiverSourceDto {
    fn check(&self, s: &SourceDto, errors: &mut ItemErrors<'_>) {
        let Self {
            listener,
            connections,
            tls,
            allow_from,
            encoding,
            transport,
            line_breaker,
            unknown: _,
        } = self;
        let what = format!("source {:?}", s.id);
        check_syslog_receiver(&what, listener, tls, allow_from, *transport, errors);
        check_connection_limits(&what, connections, *transport == Transport::Tcp, errors);
        check_encoding(encoding, &s.id, errors);
        if let Some(line_breaker) = line_breaker {
            match transport {
                Transport::Udp => errors.at("lineBreaker").push(format!(
                    "{what} sets lineBreaker, which only TCP reads: over UDP a datagram is a message"
                )),
                Transport::Tcp => check_line_breaker(&what, line_breaker, errors.at("lineBreaker")),
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
            line_breaker,
        } = self;
        receiver_config(
            crate::plan::topology::Receiver::Syslog(crate::plan::topology::SocketReceiverConfig {
                // Validation refused a block that does not parse, or a label that names no encoding, before any of
                // this ran.
                allow_from: allow_from.iter().filter_map(|b| b.trim().parse().ok()).collect(),
                encoding: Encoding::for_label(&encoding.0).unwrap_or_default(),
                transport: *transport,
                framing: Framing::Syslog(line_breaker.as_deref().and_then(to_line_breaker)),
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
        SourceMeta::SYSLOG_RECEIVER
    }

    fn encoding(&self) -> Encoding {
        Encoding::for_label(&self.encoding.0).unwrap_or_default()
    }
}
