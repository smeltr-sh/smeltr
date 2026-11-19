use super::super::*;
use super::receiver::{ReceiverKindDto, ReceiverSourceDto};
use crate::plan::topology::{Receiver, SocketReceiverConfig};
use serde::{Deserialize, Serialize};

/// Its framing sits beside its own settings rather than among them, as no flattened block may flatten another.
pub type SocketReceiverSourceDto = ReceiverSourceDto<SocketReceiverDto, FramingSettingsDto>;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SocketReceiverDto {
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
}

impl ReceiverKindDto for SocketReceiverDto {
    /// TCP only: where one record ends and the next starts.
    type Framing = FramingSettingsDto;

    fn meta(&self) -> SourceMeta {
        SourceMeta::SOCKET_RECEIVER
    }

    fn check(
        &self,
        s: &SourceDto,
        what: &str,
        listener: &ReceiverSettingsDto,
        framing: &FramingSettingsDto,
        errors: &mut ItemErrors<'_>,
    ) {
        check_encoding(&self.encoding, &s.id, errors);
        match self.transport {
            Transport::Udp if !framing.reads_datagrams() => errors.at("framing").push(format!(
                "{what} sets a framing that only TCP reads: over UDP a datagram is a record"
            )),
            Transport::Udp => {}
            // A receiver keeps a message to its largest request.
            Transport::Tcp => check_framing(framing, &s.id, listener.max_request_bytes.0, "message", errors),
        }
    }

    fn receiver(&self, framing: &FramingSettingsDto) -> Receiver {
        Receiver::Socket(SocketReceiverConfig {
            // Validation refused a block that does not parse, or a label that names no encoding, before any of this
            // ran.
            allow_from: self.allow_from.iter().filter_map(|b| b.trim().parse().ok()).collect(),
            encoding: self.encoding(),
            transport: self.transport,
            framing: to_framing(framing),
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
