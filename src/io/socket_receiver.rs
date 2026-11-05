//! What a syslog or socket receiver makes of a sender's TCP connection, or of its datagrams: messages cut by the
//! receiver's framing (`io::framing`), each a record. A socket receiver's framing is the one it names; a syslog
//! receiver's is syslog's own, framed by octet count or ended by a newline (RFC 6587):
//!
//! ```text
//!   sender ── allowFrom ──► bytes ──► Framer: each frame by its first byte ──► Messages ──► admitted, then read on
//!     1-9:   MSG-LEN SP MSG     (octet counting: exactly MSG-LEN bytes, newlines and all)
//!     other: MSG LF             (non-transparent: one trailing CR dropped, a blank line skipped)
//!            MSG break          (with a line breaker: MSG ends where its pattern breaks, a blank event skipped)
//! ```
//!
//! A message longer than the receiver's largest keeps its first bytes, up to the largest, and the rest of it is
//! discarded as it arrives, as RFC 5424 (6.1) has a receiver truncate it: the connection reads on.
//!
//! A line breaker serves senders that end a message with something other than a newline, such as a NUL, or with
//! nothing at all, the next message's start being the only mark. Octet counting still wins wherever a frame starts
//! with 1-9: a message from a sender that writes one starts with `<`, and its pattern decides the rest.
//!
//! Syslog has no acknowledgement: a message is the sender's to forget once written, so a connection waits for the
//! inbox to take what it read, which is its backpressure, and never for delivery.
//!
//! Over UDP each datagram is one message (RFC 5426), and nothing makes a sender wait: what the inbox has no room for
//! is dropped (`receiver::serve_datagrams`).

use std::future::Future;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};

use super::framing::{Framer, Framing, Malformed, Messages, Truncations};
use super::receiver::{Closed, Connection, Counted, Cutter, Held, STREAM_STEP};
use crate::plan::topology::SocketReceiverConfig;

/// How much one read of the socket takes.
const READ_STEP: usize = 64 * 1024;

/// How long a sender may go quiet before what a line breaker holds is taken as its last messages. Nothing waits behind
/// it on the connection, and a sender writes a message whole, so only a sender stalled this long mid-message has it cut
/// in two.
const QUIET: Duration = Duration::from_secs(30);

/// How long closing a connection waits for the sender to take its `close_notify`.
const CLOSING: Duration = Duration::from_secs(5);

/// Why a connection ended.
#[derive(Debug, PartialEq)]
enum End {
    /// The sender hung up or the connection broke, with the bytes of an octet-counted frame it never finished.
    Gone { partial: usize },
    /// The receiver is stopping.
    Stopping,
    /// A frame this receiver does not read.
    Malformed(Malformed),
}

impl From<Closed> for End {
    fn from(_: Closed) -> End {
        End::Stopping
    }
}

/// What a connection does that a syslog receiver serves.
pub(super) struct Handler {
    /// The largest message.
    max_message_bytes: usize,
    /// Where a message over TCP ends.
    framing: Framing,
    /// How long a sender may go quiet before a line breaker's held bytes are taken as its last messages.
    quiet: Duration,
    /// The datagrams it truncated.
    truncations: std::sync::Mutex<Truncations>,
}

impl Handler {
    pub(super) fn new(config: &SocketReceiverConfig, max_message_bytes: usize) -> Handler {
        Handler {
            max_message_bytes,
            framing: config.framing.clone(),
            quiet: QUIET,
            truncations: std::sync::Mutex::default(),
        }
    }

    /// Serves one sender's connection until it ends, logs why when it was refused, and closes it: under TLS with a
    /// `close_notify`, which answers the sender's own (RFC 5425, 4.4).
    pub(super) async fn serve(self: Arc<Self>, connection: Connection) {
        let Connection { mut stream, inlet } = connection;
        let remote = &*inlet.remote;
        let admit = |messages, bytes| {
            let admitted = inlet.admit(Cutter::Messages(messages), bytes);
            // Syslog has no acknowledgement, so delivery is not waited for.
            async move { admitted.await.map(drop) }
        };
        let ended = self.read(&mut stream, remote, admit, &mut inlet.held()).await;
        // A sender that no longer reads is not waited on.
        let _ = tokio::time::timeout(CLOSING, stream.shutdown()).await;
        match ended {
            End::Gone { partial: 0 } | End::Stopping => {}
            End::Gone { partial } => tracing::trace!(
                name: "partial", target: "sources",
                subject = %remote,
                bytes = partial,
                "a sender hung up partway through a message, and what arrived of it is dropped"
            ),
            End::Malformed(Malformed { at, what }) => match self.framing {
                Framing::Syslog(_) => tracing::warn!(
                    name: "malformed", target: "sources",
                    subject = %remote,
                    offset = at,
                    what = %what,
                    "a sender sent what this receiver does not read as syslog (RFC 6587); its connection is closed"
                ),
                _ => tracing::warn!(
                    name: "malformed", target: "sources",
                    subject = %remote,
                    offset = at,
                    what = %what,
                    "a sender sent what this receiver's framing does not read; its connection is closed"
                ),
            },
        }
    }

    /// The message one datagram holds (RFC 5426), its trailing newline dropped where a sender adds one, and truncated
    /// to the largest message (RFC 5424, 6.1). `None` for an empty one.
    pub(super) fn datagram(&self, datagram: &[u8], peer: IpAddr) -> Option<Cutter> {
        let mut message = match self.framing {
            // A syslog sender may end a datagram's message as it would a line.
            Framing::Syslog(_) => {
                let message = datagram.strip_suffix(b"\n").unwrap_or(datagram);
                message.strip_suffix(b"\r").unwrap_or(message)
            }
            _ => datagram,
        };
        if message.len() > self.max_message_bytes {
            message = &message[..self.max_message_bytes];
            let mut truncations = self
                .truncations
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            truncations.count += 1;
            if let Some(truncated) = truncations.due(false) {
                tracing::warn!(
                    name: "truncated", target: "sources",
                    subject = %peer,
                    limit = self.max_message_bytes,
                    truncated,
                    "a datagram larger than the receiver's maxRequestBytes keeps its first bytes, up to it"
                );
            }
        }
        if message.is_empty() {
            return None;
        }
        let mut messages = Messages::default();
        messages.push(message);
        Some(Cutter::Messages(messages))
    }

    /// Reads `reader`, the connection from `remote`, until it ends: the messages each read completes admitted before
    /// the next read, and what it holds between them charged to `held`.
    async fn read<R, A, F>(&self, mut reader: R, remote: &str, mut admit: A, held: &mut Held) -> End
    where
        R: AsyncRead + Unpin,
        A: FnMut(Messages, Counted) -> F,
        F: Future<Output = Result<(), Closed>>,
    {
        let mut framer = Framer::new(self.framing.clone(), self.max_message_bytes);
        let mut buf = vec![0; READ_STEP];
        loop {
            // Room for what it holds, its buffer as much again as it may grow to, and the read it takes next.
            if let Err(closed) = held.hold(buf.len() + 2 * framer.capacity() + STREAM_STEP).await {
                return closed.into();
            }
            let reading = reader.read(&mut buf);
            let read = match framer.waits() {
                true => tokio::time::timeout(self.quiet, reading).await.ok(),
                false => Some(reading.await),
            };
            let mut messages = Messages::default();
            let cut = match read {
                Some(Ok(0)) => break,
                Some(Ok(n)) => {
                    framer.push(&buf[..n]);
                    framer.cut(&mut messages)
                }
                Some(Err(_)) => return End::Gone { partial: 0 },
                // The sender went quiet: nothing more is coming for what is held.
                None => framer.flush(&mut messages),
            };
            // What came before a refused frame is the sender's as much as any, so it is admitted first.
            if !messages.is_empty() {
                let bytes = held.part(messages.size());
                if let Err(closed) = admit(messages, bytes).await {
                    return closed.into();
                }
            }
            self.log_truncated(&mut framer, remote, false);
            if let Err(malformed) = cut {
                return End::Malformed(malformed);
            }
        }
        let mut messages = Messages::default();
        let ended = match framer.finish(&mut messages) {
            Ok(partial) => End::Gone { partial },
            Err(malformed) => End::Malformed(malformed),
        };
        self.log_truncated(&mut framer, remote, true);
        if !messages.is_empty() {
            let bytes = held.part(messages.size());
            if let Err(closed) = admit(messages, bytes).await {
                return closed.into();
            }
        }
        ended
    }

    /// Logs the messages `framer` truncated, when that is due; `ending` when the connection is.
    fn log_truncated(&self, framer: &mut Framer, remote: &str, ending: bool) {
        if let Some(truncated) = framer.truncations.due(ending) {
            tracing::warn!(
                name: "truncated", target: "sources",
                subject = %remote,
                limit = self.max_message_bytes,
                truncated,
                "a sender sent messages larger than the receiver's maxRequestBytes; each keeps its first bytes, up to it"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::AsyncWriteExt;

    use super::*;

    const MAX: usize = 64;

    fn strings(messages: Messages) -> Vec<String> {
        messages.map(|m| String::from_utf8(m).unwrap()).collect()
    }

    /// `n` bytes of `b`, as a string.
    fn run(b: char, n: usize) -> String {
        b.to_string().repeat(n)
    }

    fn handler() -> Handler {
        let config = SocketReceiverConfig {
            framing: Framing::Syslog(None),
            ..Default::default()
        };
        Handler::new(&config, MAX)
    }

    /// What `serve`'s read loop admits from a connection that sends `sent` and hangs up, one entry per admission,
    /// and how the connection ended.
    async fn read_all(sent: &[u8]) -> (Vec<(Vec<String>, usize)>, End) {
        read_with(&handler(), sent).await
    }

    /// What `handler`'s read loop admits from a connection that sends `sent` and hangs up, as `read_all`.
    async fn read_with(handler: &Handler, sent: &[u8]) -> (Vec<(Vec<String>, usize)>, End) {
        let (mut sender, receiver) = tokio::io::duplex(1024);
        sender.write_all(sent).await.unwrap();
        drop(sender);
        let mut admitted = Vec::new();
        let ended = handler
            .read(
                receiver,
                "127.0.0.1",
                |messages: Messages, bytes: Counted| {
                    admitted.push((strings(messages), bytes.total()));
                    async { Ok(()) }
                },
                &mut Held::alone(),
            )
            .await;
        (admitted, ended)
    }

    /// The message `datagram` holds from a loopback sender, or `None` where the handler refuses it.
    fn datagram(datagram: &[u8]) -> Option<Vec<String>> {
        match handler().datagram(datagram, "127.0.0.1".parse().unwrap())? {
            Cutter::Messages(messages) => Some(strings(messages)),
            _ => panic!("a syslog datagram is cut into messages"),
        }
    }

    #[test]
    fn a_datagram_drops_one_trailing_newline() {
        assert_eq!(datagram(b"<13>a"), Some(vec!["<13>a".into()]));
        assert_eq!(datagram(b"<13>a\n"), Some(vec!["<13>a".into()]));
        assert_eq!(datagram(b"<13>a\r\n"), Some(vec!["<13>a".into()]));
        // Only the one a sender ends its message with: what comes before it is the message's own.
        assert_eq!(datagram(b"<13>a\n\n"), Some(vec!["<13>a\n".into()]));
        assert_eq!(datagram(b"<13>a\nb\r"), Some(vec!["<13>a\nb".into()]));
    }

    #[test]
    fn an_empty_datagram_is_refused() {
        assert_eq!(datagram(b""), None);
        assert_eq!(datagram(b"\n"), None);
        assert_eq!(datagram(b"\r\n"), None);
    }

    #[test]
    fn a_datagram_over_the_largest_message_is_truncated() {
        assert_eq!(datagram(run('a', MAX).as_bytes()), Some(vec![run('a', MAX)]));
        // The largest message keeps its line end, which is not counted, and is not truncated.
        for end in ["\n", "\r\n"] {
            assert_eq!(
                datagram(format!("{}{end}", run('a', MAX)).as_bytes()),
                Some(vec![run('a', MAX)])
            );
        }
        // Its newline was cut off with the rest, so what it keeps is all message.
        let over = format!("{}\n", run('a', MAX - 1));
        assert_eq!(datagram(format!("{over}b").as_bytes()), Some(vec![over]));
    }

    /// A socket receiver's handler, whose connections `framing` cuts.
    fn socket(framing: Framing) -> Handler {
        let config = SocketReceiverConfig {
            framing,
            ..Default::default()
        };
        Handler::new(&config, MAX)
    }

    #[tokio::test]
    async fn a_socket_receiver_cuts_a_connection_by_its_framing_and_drops_a_frame_it_never_finished() {
        let framing = Framing::LengthPrefix(crate::io::framing::LengthPrefix {
            offset: 0,
            width: 1,
            order: crate::io::framing::ByteOrder::Big,
            counts: crate::io::framing::Counts::Rest,
        });
        let (admitted, ended) = read_with(&socket(framing), b"\x02ab\x015\x05de").await;
        let messages: Vec<String> = admitted.into_iter().flat_map(|(m, _)| m).collect();
        assert_eq!(messages, ["\x02ab", "\x015"]);
        assert_eq!(ended, End::Gone { partial: 3 });
    }

    #[test]
    fn a_socket_receiver_keeps_a_datagram_as_it_was_sent() {
        let kept = socket(Framing::Lines).datagram(b"5 a\r\n", "127.0.0.1".parse().unwrap());
        let Some(Cutter::Messages(messages)) = kept else {
            panic!("a datagram is a record")
        };
        assert_eq!(strings(messages), ["5 a\r\n"]);
    }

    #[tokio::test]
    async fn a_connection_admits_each_reads_messages_and_its_last_line() {
        let (admitted, ended) = read_all(b"5 <13>a<13>b\n<13>c").await;
        assert_eq!(ended, End::Gone { partial: 0 });
        let messages: Vec<String> = admitted.iter().flat_map(|(m, _)| m.clone()).collect();
        assert_eq!(messages, ["<13>a", "<13>b", "<13>c"]);
        // Each admission counts its bytes and what holding each message costs.
        let (first, bytes) = &admitted[0];
        assert_eq!(*bytes, first.iter().map(|m| m.len() + size_of::<u32>()).sum::<usize>());
    }

    #[tokio::test]
    async fn a_connection_ends_on_a_malformed_frame_or_a_partial_one() {
        let (admitted, ended) = read_all(b"<13>a\n12a ").await;
        assert_eq!(admitted, [(vec!["<13>a".to_string()], 5 + size_of::<u32>())]);
        assert!(matches!(ended, End::Malformed(Malformed { at: 6, .. })));

        let (_, ended) = read_all(b"10 <13>").await;
        assert_eq!(ended, End::Gone { partial: 7 });
    }

    #[tokio::test]
    async fn a_closed_receiver_ends_the_connection() {
        let (mut sender, receiver) = tokio::io::duplex(1024);
        sender.write_all(b"<13>a\n").await.unwrap();
        let ended = handler()
            .read(receiver, "127.0.0.1", |_, _| async { Err(Closed) }, &mut Held::alone())
            .await;
        assert_eq!(ended, End::Stopping);
    }

    #[tokio::test]
    async fn a_quiet_sender_has_what_its_line_breaker_holds_cut() {
        let (mut sender, receiver) = tokio::io::duplex(1024);
        // The NUL ends the first message as it arrives; nothing but the quiet ends the second.
        sender.write_all(b"<13>a\x00<13>b").await.unwrap();
        let mut handler = handler();
        handler.framing = Framing::Syslog(Some(crate::io::line_breaker::LineBreaker(
            regex::bytes::Regex::new(r"(\x00)").unwrap(),
        )));
        handler.quiet = Duration::from_millis(20);
        let mut admitted = Vec::new();
        let ended = handler
            .read(
                receiver,
                "127.0.0.1",
                |messages: Messages, _| {
                    let messages = strings(messages);
                    let last = messages.contains(&"<13>b".to_string());
                    admitted.push(messages);
                    async move { if last { Err(Closed) } else { Ok(()) } }
                },
                &mut Held::alone(),
            )
            .await;
        // The sender is still open: only going quiet could have cut these.
        assert_eq!(ended, End::Stopping);
        assert_eq!(admitted, vec![vec!["<13>a".to_string()], vec!["<13>b".to_string()]]);
        drop(sender);
    }
}
