use super::super::*;
use super::receiver::{NoFramingDto, ReceiverKindDto, ReceiverSourceDto};
use crate::plan::topology::{Receiver, SocketReceiverConfig};
use serde::{Deserialize, Serialize};

pub type SyslogReceiverSourceDto = ReceiverSourceDto<SyslogReceiverDto, NoFramingDto>;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SyslogReceiverDto {
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
}

impl ReceiverKindDto for SyslogReceiverDto {
    type Framing = NoFramingDto;

    fn meta(&self) -> SourceMeta {
        SourceMeta::SYSLOG_RECEIVER
    }

    fn check(&self, s: &SourceDto, what: &str, _: &ReceiverSettingsDto, _: &NoFramingDto, errors: &mut ItemErrors<'_>) {
        check_encoding(&self.encoding, &s.id, errors);
        if let Some(line_breaker) = &self.line_breaker {
            match self.transport {
                Transport::Udp => errors.at("lineBreaker").push(format!(
                    "{what} sets lineBreaker, which only TCP reads: over UDP a datagram is a message"
                )),
                Transport::Tcp => check_line_breaker(what, line_breaker, errors.at("lineBreaker")),
            }
        }
    }

    fn receiver(&self, _: &NoFramingDto) -> Receiver {
        Receiver::Syslog(SocketReceiverConfig {
            // Validation refused a block that does not parse, or a label that names no encoding, before any of this
            // ran.
            allow_from: self.allow_from.iter().filter_map(|b| b.trim().parse().ok()).collect(),
            encoding: self.encoding(),
            transport: self.transport,
            framing: Framing::Syslog(self.line_breaker.as_deref().and_then(to_line_breaker)),
        })
    }

    fn allow_from(&self) -> Option<&[String]> {
        Some(&self.allow_from)
    }

    fn transport(&self) -> Transport {
        self.transport
    }

    fn encoding(&self) -> Encoding {
        Encoding::for_label(&self.encoding.0).unwrap_or_default()
    }
}
