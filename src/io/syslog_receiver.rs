//! What a syslog receiver makes of a sender's TCP connection: messages framed by octet count or ended by a newline
//! (RFC 6587), each a record.
//!
//! ```text
//!   sender ── allowFrom ──► bytes ──► Framer: each frame by its first byte ──► Messages ──► admitted, then read on
//!     1-9:   MSG-LEN SP MSG     (octet counting: exactly MSG-LEN bytes, newlines and all)
//!     other: MSG LF             (non-transparent: one trailing CR dropped, a blank line skipped)
//!            MSG break          (with a line breaker: MSG ends where its pattern breaks, a blank event skipped)
//! ```
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

use tokio::io::{AsyncRead, AsyncReadExt};

use super::line_breaker::{BREAK_SLACK, LineBreaker, Searched};
use super::receiver::{Closed, Connection, Counted, Cutter, Held, STREAM_STEP};
use crate::plan::topology::SyslogReceiverConfig;

/// How much one read of the socket takes.
const READ_STEP: usize = 64 * 1024;

/// How long a sender may go quiet before what a line breaker holds is taken as its last messages. Nothing waits behind
/// it on the connection, and a sender writes a message whole, so only a sender stalled this long mid-message has it cut
/// in two.
const QUIET: Duration = Duration::from_secs(30);

/// Messages cut from what a connection read, each a record, filed under nothing. Held in one buffer, each message
/// its bytes and where it ends, so many short messages cost about what was sent.
#[derive(Default)]
pub(super) struct Messages {
    bytes: Vec<u8>,
    /// Where each message ends in `bytes`. What one read completes is at most what the framer held, a message short
    /// of the receiver's `maxRequestBytes` and a read, so an offset fits.
    ends: Vec<u32>,
    taken: usize,
}

impl Messages {
    fn push(&mut self, message: &[u8]) {
        self.bytes.extend_from_slice(message);
        self.ends.push(self.bytes.len() as u32);
    }

    fn is_empty(&self) -> bool {
        self.taken == self.ends.len()
    }

    /// What its messages hold once taken, as the inbox counts a record, and what holding them costs until then.
    fn size(&self) -> usize {
        self.bytes.len() + self.ends.len() * size_of::<u32>()
    }
}

impl Iterator for Messages {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Vec<u8>> {
        let end = *self.ends.get(self.taken)? as usize;
        let start = match self.taken {
            0 => 0,
            i => self.ends[i - 1] as usize,
        };
        self.taken += 1;
        Some(self.bytes[start..end].to_vec())
    }
}

/// Cuts a connection's bytes into messages, deciding each frame's framing by its first byte, as RFC 6587 lets a
/// receiver: a sender may mix them.
struct Framer {
    held: Vec<u8>,
    /// How much of `held` is already cut.
    cut: usize,
    /// The largest message.
    max: usize,
    /// How many bytes the connection sent before `held`, so a refusal names where the stream went wrong.
    offset: u64,
    /// Where a non-transparent message ends, when not at a newline.
    line_breaker: Option<LineBreaker>,
    /// How far the line breaker searched what follows `cut` in vain.
    searched: Searched,
}

/// Why a frame was refused, and where it starts in the connection's stream.
#[derive(Debug, PartialEq)]
struct Malformed {
    at: u64,
    what: String,
}

/// One frame read from the front of what is held.
enum Frame {
    /// A message, its bounds in `held`, and where the next frame starts.
    Message(usize, usize, usize),
    /// A blank non-transparent line, and where the next frame starts.
    Blank(usize),
    /// Not all of it has arrived.
    Partial,
}

impl Framer {
    fn new(max: usize, line_breaker: Option<LineBreaker>) -> Framer {
        Framer {
            held: Vec::new(),
            cut: 0,
            max,
            offset: 0,
            line_breaker,
            searched: Searched::default(),
        }
    }

    fn push(&mut self, bytes: &[u8]) {
        if self.cut > 0 {
            self.offset += self.cut as u64;
            self.held.drain(..self.cut);
            self.cut = 0;
        }
        self.held.extend_from_slice(bytes);
    }

    /// Adds every message held whole to `messages`, in order. What follows the last stays held for the next push.
    /// A refused frame leaves the messages before it in `messages`.
    fn cut(&mut self, messages: &mut Messages) -> Result<(), Malformed> {
        loop {
            let frame = self.frame()?;
            if !self.take(frame, messages) {
                return Ok(());
            }
        }
    }

    /// Adds a whole frame's message to `messages` and moves past it. `false` for a partial frame, which stays held.
    fn take(&mut self, frame: Frame, messages: &mut Messages) -> bool {
        let next = match frame {
            Frame::Message(start, end, next) => {
                messages.push(&self.held[start..end]);
                next
            }
            Frame::Blank(next) => next,
            Frame::Partial => return false,
        };
        self.cut = next;
        self.searched.found();
        true
    }

    /// The connection's end: adds a last non-transparent message, which needs no newline, to `messages`. An
    /// octet-counted frame that never arrived whole is dropped, and `Ok` says how many bytes of it there were.
    fn finish(mut self, messages: &mut Messages) -> Result<usize, Malformed> {
        if let Some(line_breaker) = self.line_breaker.take() {
            return self.end_broken(&line_breaker, messages);
        }
        let rest = &self.held[self.cut..];
        match rest.first() {
            None => {}
            Some(b'1'..=b'9') => return Ok(rest.len()),
            Some(_) => {
                self.held.push(b'\n');
                if let Frame::Message(start, end, _) = self.non_transparent(self.cut)? {
                    messages.push(&self.held[start..end]);
                }
            }
        }
        Ok(0)
    }

    /// Whether what it holds waits on a line breaker's break, which a quiet sender may never send.
    fn waits(&self) -> bool {
        self.line_breaker.is_some() && self.held.get(self.cut).is_some_and(|b| !matches!(b, b'1'..=b'9'))
    }

    /// Adds what a line breaker holds to `messages` as though the stream had ended, its sender having gone quiet, and
    /// reads on. An octet-counted frame not yet whole stays held.
    fn flush(&mut self, messages: &mut Messages) -> Result<(), Malformed> {
        if let Some(line_breaker) = self.line_breaker.take() {
            let flushed = self.end_broken(&line_breaker, messages);
            self.line_breaker = Some(line_breaker);
            self.searched.found();
            flushed?;
        }
        Ok(())
    }

    /// Cuts every message held, nothing more to come, up to an octet-counted frame not yet whole, whose length held
    /// it returns.
    fn end_broken(&mut self, line_breaker: &LineBreaker, messages: &mut Messages) -> Result<usize, Malformed> {
        loop {
            let start = self.cut;
            let rest = &self.held[start..];
            let frame = match rest.first() {
                None => return Ok(0),
                Some(b'1'..=b'9') => match self.octet_counted(start)? {
                    Frame::Partial => return Ok(rest.len()),
                    frame => frame,
                },
                Some(_) => match line_breaker.next_break(rest, true) {
                    Some((end, next)) => self.event(start, start + end, start + next)?,
                    None => {
                        let end = start + LineBreaker::last_event(rest).len();
                        self.event(start, end, self.held.len())?
                    }
                },
            };
            assert!(self.take(frame, messages), "an ended stream's frame is whole");
        }
    }

    fn frame(&mut self) -> Result<Frame, Malformed> {
        let start = self.cut;
        let Some(&first) = self.held.get(start) else {
            return Ok(Frame::Partial);
        };
        if first.is_ascii_digit() && first != b'0' {
            return self.octet_counted(start);
        }
        if let Some(line_breaker) = self.line_breaker.take() {
            let frame = self.broken(&line_breaker, start);
            self.line_breaker = Some(line_breaker);
            return frame;
        }
        let frame = self.non_transparent(start)?;
        // A whole line may hold its message and a CR.
        if matches!(frame, Frame::Partial) && self.held.len() - start > self.max + 1 {
            return Err(self.malformed(start, format!("a line runs past {} bytes without a newline", self.max)));
        }
        Ok(frame)
    }

    /// `MSG-LEN SP MSG`, starting at `start`, which holds a digit 1-9.
    fn octet_counted(&self, start: usize) -> Result<Frame, Malformed> {
        let mut len = 0usize;
        for (i, &b) in self.held[start..].iter().enumerate() {
            match b {
                b'0'..=b'9' => {
                    len = len * 10 + (b - b'0') as usize;
                    // Checked as each digit arrives, so a run of digits is refused before it overflows.
                    if len > self.max {
                        return Err(self.malformed(start, format!("a frame's length is over {} bytes", self.max)));
                    }
                }
                b' ' => {
                    let message = start + i + 1;
                    return Ok(match self.held.len() - message >= len {
                        true => Frame::Message(message, message + len, message + len),
                        false => Frame::Partial,
                    });
                }
                _ => {
                    return Err(self.malformed(start, "a frame's length is not followed by a space".into()));
                }
            }
        }
        Ok(Frame::Partial)
    }

    /// `MSG LF`, starting at `start`. The message drops the CR a sender may put before the LF.
    fn non_transparent(&self, start: usize) -> Result<Frame, Malformed> {
        let Some(lf) = self.held[start..].iter().position(|&b| b == b'\n') else {
            return Ok(Frame::Partial);
        };
        let next = start + lf + 1;
        let mut end = start + lf;
        if end > start && self.held[end - 1] == b'\r' {
            end -= 1;
        }
        if end - start > self.max {
            return Err(self.malformed(start, format!("a line is over {} bytes", self.max)));
        }
        Ok(match self.held[start..end].iter().all(u8::is_ascii_whitespace) {
            true => Frame::Blank(next),
            false => Frame::Message(start, end, next),
        })
    }

    /// `MSG break`, starting at `start`: the message ends where `line_breaker` breaks what follows it.
    fn broken(&mut self, line_breaker: &LineBreaker, start: usize) -> Result<Frame, Malformed> {
        let rest = self.held.len() - start;
        let over = rest > self.max + BREAK_SLACK;
        if !self.searched.due(rest) && !over {
            return Ok(Frame::Partial);
        }
        if let Some((end, next)) = line_breaker.next_break(&self.held[start..], false) {
            return self.event(start, start + end, start + next);
        }
        self.searched.missed(rest);
        if over {
            return Err(self.malformed(
                start,
                format!(
                    "a message runs past {} bytes without its line breaker breaking it",
                    self.max
                ),
            ));
        }
        Ok(Frame::Partial)
    }

    /// The message a line breaker broke at `start..end`, the next frame starting at `next`.
    fn event(&self, start: usize, end: usize, next: usize) -> Result<Frame, Malformed> {
        if end - start > self.max {
            return Err(self.malformed(start, format!("a message is over {} bytes", self.max)));
        }
        Ok(match self.held[start..end].iter().all(u8::is_ascii_whitespace) {
            true => Frame::Blank(next),
            false => Frame::Message(start, end, next),
        })
    }

    fn malformed(&self, at: usize, what: String) -> Malformed {
        Malformed {
            at: self.offset + at as u64,
            what,
        }
    }
}

/// Whether a sender at `peer` may connect, or send a datagram. An empty list admits whoever TLS admitted.
fn admits(allow_from: &[ipnetwork::IpNetwork], peer: IpAddr) -> bool {
    allow_from.is_empty() || allow_from.iter().any(|network| network.contains(peer))
}

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
    /// The networks a sender may connect from. Empty admits whoever TLS admitted.
    allow_from: Vec<ipnetwork::IpNetwork>,
    /// The largest message.
    max_message_bytes: usize,
    /// Where a non-transparent message ends, when not at a newline.
    line_breaker: Option<LineBreaker>,
    /// How long a sender may go quiet before a line breaker's held bytes are taken as its last messages.
    quiet: Duration,
}

impl Handler {
    pub(super) fn new(config: &SyslogReceiverConfig, max_message_bytes: usize) -> Handler {
        Handler {
            allow_from: config.allow_from.clone(),
            max_message_bytes,
            line_breaker: config.line_breaker.clone(),
            quiet: QUIET,
        }
    }

    /// Serves one sender's connection until it ends, and logs why when it was refused.
    pub(super) async fn serve(self: Arc<Self>, connection: Connection) {
        let Connection { stream, peer, inlet } = connection;
        let remote = &*inlet.remote;
        if !admits(&self.allow_from, peer) {
            tracing::debug!(
                target: "worker.receiver_syslog_refused",
                subject = %remote,
                "a connection is closed: its address is in none of the receiver's allowFrom networks"
            );
            return;
        }
        let admit = |messages, bytes| {
            let admitted = inlet.admit(Cutter::Messages(messages), bytes);
            // Syslog has no acknowledgement, so delivery is not waited for.
            async move { admitted.await.map(drop) }
        };
        match self.read(stream, admit, &mut inlet.held()).await {
            End::Gone { partial: 0 } | End::Stopping => {}
            End::Gone { partial } => tracing::debug!(
                target: "worker.receiver_syslog_partial",
                subject = %remote,
                bytes = partial,
                "a sender hung up partway through an octet-counted message, which is dropped"
            ),
            End::Malformed(Malformed { at, what }) => tracing::warn!(
                target: "worker.receiver_syslog_malformed",
                subject = %remote,
                offset = at,
                what = %what,
                "a sender sent what this receiver does not read as syslog (RFC 6587); its connection is closed"
            ),
        }
    }

    /// The message one datagram holds (RFC 5426), its trailing newline dropped where a sender adds one. `None` for a
    /// datagram from a sender this receiver does not admit, one larger than its largest message, or an empty one.
    pub(super) fn datagram(&self, datagram: &[u8], peer: IpAddr) -> Option<Cutter> {
        if !admits(&self.allow_from, peer) {
            tracing::debug!(
                target: "worker.receiver_syslog_refused",
                subject = %peer,
                "a datagram is dropped: its address is in none of the receiver's allowFrom networks"
            );
            return None;
        }
        if datagram.len() > self.max_message_bytes {
            tracing::warn!(
                target: "worker.receiver_syslog_malformed",
                subject = %peer,
                limit = self.max_message_bytes,
                "a datagram larger than the receiver's maxRequestBytes is dropped"
            );
            return None;
        }
        let message = datagram.strip_suffix(b"\n").unwrap_or(datagram);
        let message = message.strip_suffix(b"\r").unwrap_or(message);
        if message.is_empty() {
            return None;
        }
        let mut messages = Messages::default();
        messages.push(message);
        Some(Cutter::Messages(messages))
    }

    /// Reads `reader` until the connection ends: the messages each read completes admitted before the next read, and
    /// what it holds between them charged to `held`.
    async fn read<R, A, F>(&self, mut reader: R, mut admit: A, held: &mut Held) -> End
    where
        R: AsyncRead + Unpin,
        A: FnMut(Messages, Counted) -> F,
        F: Future<Output = Result<(), Closed>>,
    {
        let mut framer = Framer::new(self.max_message_bytes, self.line_breaker.clone());
        let mut buf = vec![0; READ_STEP];
        loop {
            // Room for what it holds, its buffer as much again as it may grow to, and the read it takes next.
            if let Err(closed) = held.hold(buf.len() + 2 * framer.held.capacity() + STREAM_STEP).await {
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
            if let Err(malformed) = cut {
                return End::Malformed(malformed);
            }
        }
        let mut messages = Messages::default();
        let ended = match framer.finish(&mut messages) {
            Ok(partial) => End::Gone { partial },
            Err(malformed) => End::Malformed(malformed),
        };
        if !messages.is_empty() {
            let bytes = held.part(messages.size());
            if let Err(closed) = admit(messages, bytes).await {
                return closed.into();
            }
        }
        ended
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::AsyncWriteExt;

    use super::*;

    const MAX: usize = 64;

    /// What the framer cuts after each of `pushes`, as strings, and the first refusal.
    fn cut_each(pushes: &[&[u8]]) -> Result<Vec<Vec<String>>, Malformed> {
        let mut framer = Framer::new(MAX, None);
        let mut cuts = Vec::new();
        for push in pushes {
            framer.push(push);
            let mut messages = Messages::default();
            framer.cut(&mut messages)?;
            cuts.push(strings(messages));
        }
        Ok(cuts)
    }

    fn strings(messages: Messages) -> Vec<String> {
        messages.map(|m| String::from_utf8(m).unwrap()).collect()
    }

    fn handler(allow_from: &[&str]) -> Handler {
        let config = SyslogReceiverConfig {
            allow_from: allow_from.iter().map(|n| n.parse().unwrap()).collect(),
            ..Default::default()
        };
        Handler::new(&config, MAX)
    }

    /// What `serve`'s read loop admits from a connection that sends `sent` and hangs up, one entry per admission,
    /// and how the connection ended.
    async fn read_all(sent: &[u8]) -> (Vec<(Vec<String>, usize)>, End) {
        let (mut sender, receiver) = tokio::io::duplex(1024);
        sender.write_all(sent).await.unwrap();
        drop(sender);
        let mut admitted = Vec::new();
        let ended = (handler(&[]))
            .read(
                receiver,
                |messages: Messages, bytes: Counted| {
                    admitted.push((strings(messages), bytes.total()));
                    async { Ok(()) }
                },
                &mut Held::alone(),
            )
            .await;
        (admitted, ended)
    }

    #[test]
    fn an_octet_counted_message_may_hold_newlines() {
        let cuts = cut_each(&[b"10 <13>a\nb\r\nc"]).unwrap();
        assert_eq!(cuts, [vec!["<13>a\nb\r\nc"]]);
    }

    #[test]
    fn a_line_drops_its_cr_and_blank_lines_are_skipped() {
        let cuts = cut_each(&[b"<13>a\r\n\n  \r\n\t\n<13>b\n"]).unwrap();
        assert_eq!(cuts, [vec!["<13>a", "<13>b"]]);
    }

    #[test]
    fn framings_mix_in_one_stream() {
        let cuts = cut_each(&[b"<13>a\n5 <13>b3 xyz<13>c\r\n"]).unwrap();
        assert_eq!(cuts, [vec!["<13>a", "<13>b", "xyz", "<13>c"]]);
    }

    #[test]
    fn a_frame_split_across_reads_is_cut_once_whole() {
        let cuts = cut_each(&[b"1", b"0 <13>a", b"bcdef<13>g", b"h\n"]).unwrap();
        let expected: [Vec<&str>; 4] = [vec![], vec![], vec!["<13>abcdef"], vec!["<13>gh"]];
        assert_eq!(cuts, expected);
    }

    #[test]
    fn an_octet_count_over_the_largest_message_is_refused() {
        let refused = cut_each(&[b"<13>a\n65 "]).unwrap_err();
        assert_eq!(refused.at, 6);
        // Refused as the digits arrive, before the space.
        assert!(cut_each(&[b"99999999999999999999999"]).is_err());
        assert_eq!(cut_each(&[b"64 "]).unwrap(), [Vec::<String>::new()]);
    }

    #[test]
    fn a_line_longer_than_the_largest_message_is_refused() {
        assert!(cut_each(&[&[b'a'; MAX], b"\r", b"\n"]).is_ok());
        assert!(cut_each(&[&[b'a'; MAX], b"aa"]).is_err());
        assert!(cut_each(&[&[b'a'; MAX], b"a\n"]).is_err());
    }

    fn line_breaker(pattern: &str) -> LineBreaker {
        LineBreaker(regex::bytes::Regex::new(pattern).unwrap())
    }

    /// A framer whose non-transparent messages `pattern` breaks.
    fn broken(pattern: &str) -> Framer {
        Framer::new(MAX, Some(line_breaker(pattern)))
    }

    /// What `framer` cuts from `pushes` and then from the connection's end, as strings, and the first refusal.
    fn cut_all(mut framer: Framer, pushes: &[&[u8]]) -> Result<(Vec<Vec<String>>, Vec<String>), Malformed> {
        let mut cuts = Vec::new();
        for push in pushes {
            framer.push(push);
            let mut messages = Messages::default();
            framer.cut(&mut messages)?;
            cuts.push(strings(messages));
        }
        let mut last = Messages::default();
        framer.finish(&mut last)?;
        Ok((cuts, strings(last)))
    }

    #[test]
    fn a_line_breaker_ends_a_message_where_it_breaks_newlines_and_all() {
        let (cuts, last) = cut_all(broken(r"(\x00)"), &[b"<13>a\nb\x00<13>c\x00", b"<13>d"]).unwrap();
        // The NUL ending the first push reaches its end, so `<13>c` waits for what comes after it.
        assert_eq!(cuts, [vec!["<13>a\nb"], vec!["<13>c"]]);
        assert_eq!(last, ["<13>d"]);
    }

    #[test]
    fn a_line_breaker_breaks_a_stream_with_nothing_between_messages() {
        let (cuts, last) = cut_all(broken(r"()CEF:0\|"), &[b"CEF:0|a|b|CEF:0|c|d|CE", b"F:0|e|"]).unwrap();
        assert_eq!(cuts, [vec!["CEF:0|a|b|"], vec!["CEF:0|c|d|"]]);
        assert_eq!(last, ["CEF:0|e|"]);
    }

    #[test]
    fn octet_counting_still_frames_beside_a_line_breaker() {
        let (cuts, last) = cut_all(broken(r"(\x00)"), &[b"<13>a\x005 <13>b<13>c\x00\x00 \x00<13>d\x00"]).unwrap();
        // The blank messages between NULs are skipped.
        assert_eq!(cuts, [vec!["<13>a", "<13>b", "<13>c"]]);
        assert_eq!(last, ["<13>d"]);
    }

    #[test]
    fn a_message_a_line_breaker_never_breaks_is_refused_past_the_slack() {
        let message = [b'a'; MAX + 1];
        let refused = cut_all(broken(r"(\x00)"), &[&message, b"\x00b"]).unwrap_err();
        assert_eq!(refused.at, 0);
        let unbroken = vec![b'a'; MAX + BREAK_SLACK + 1];
        assert!(cut_all(broken(r"(\x00)"), &[&unbroken[..MAX + BREAK_SLACK]]).is_err());
        assert!(cut_all(broken(r"(\x00)"), &[&unbroken]).is_err());
    }

    #[test]
    fn a_length_not_followed_by_a_space_is_refused() {
        let refused = cut_each(&[b"12a <13>abcdefgh"]).unwrap_err();
        assert_eq!(refused.at, 0);
    }

    /// What the framer cuts from `sent`, then what it finishes with and how many bytes of a partial frame it drops.
    fn cut_and_finish(sent: &[u8]) -> (Vec<String>, Vec<String>, usize) {
        let mut framer = Framer::new(MAX, None);
        framer.push(sent);
        let (mut cut, mut finished) = (Messages::default(), Messages::default());
        framer.cut(&mut cut).unwrap();
        let partial = framer.finish(&mut finished).unwrap();
        (strings(cut), strings(finished), partial)
    }

    #[test]
    fn a_last_line_needs_no_newline_and_a_partial_frame_is_dropped() {
        let (cut, finished, partial) = cut_and_finish(b"<13>a\n<13>b\r");
        assert_eq!(
            (cut, finished, partial),
            (vec!["<13>a".into()], vec!["<13>b".into()], 0)
        );

        let (cut, finished, partial) = cut_and_finish(b"<13>a\n10 <13>");
        assert_eq!((cut, finished, partial), (vec!["<13>a".into()], vec![], 7));

        let (cut, finished, partial) = cut_and_finish(b"<13>a\n \r");
        assert_eq!((cut, finished, partial), (vec!["<13>a".into()], vec![], 0));
    }

    #[test]
    fn allow_from_admits_only_its_networks() {
        let allow: Vec<ipnetwork::IpNetwork> = vec!["10.0.0.0/8".parse().unwrap(), "fd00::/8".parse().unwrap()];
        assert!(admits(&allow, "10.1.2.3".parse().unwrap()));
        assert!(admits(&allow, "fd00::1".parse().unwrap()));
        assert!(!admits(&allow, "192.168.1.1".parse().unwrap()));
        assert!(!admits(&allow, "::1".parse().unwrap()));
        assert!(admits(&[], "192.168.1.1".parse().unwrap()));
    }

    /// The message `datagram` holds from a loopback sender, or `None` where the handler refuses it.
    fn datagram(allow_from: &[&str], datagram: &[u8]) -> Option<Vec<String>> {
        match handler(allow_from).datagram(datagram, "127.0.0.1".parse().unwrap())? {
            Cutter::Messages(messages) => Some(strings(messages)),
            _ => panic!("a syslog datagram is cut into messages"),
        }
    }

    #[test]
    fn a_datagram_drops_one_trailing_newline() {
        assert_eq!(datagram(&[], b"<13>a"), Some(vec!["<13>a".into()]));
        assert_eq!(datagram(&[], b"<13>a\n"), Some(vec!["<13>a".into()]));
        assert_eq!(datagram(&[], b"<13>a\r\n"), Some(vec!["<13>a".into()]));
        // Only the one a sender ends its message with: what comes before it is the message's own.
        assert_eq!(datagram(&[], b"<13>a\n\n"), Some(vec!["<13>a\n".into()]));
        assert_eq!(datagram(&[], b"<13>a\nb\r"), Some(vec!["<13>a\nb".into()]));
    }

    #[test]
    fn an_empty_datagram_is_refused() {
        assert_eq!(datagram(&[], b""), None);
        assert_eq!(datagram(&[], b"\n"), None);
        assert_eq!(datagram(&[], b"\r\n"), None);
    }

    #[test]
    fn a_datagram_from_elsewhere_or_over_the_largest_message_is_refused() {
        assert_eq!(datagram(&["10.0.0.0/8"], b"<13>a"), None);
        assert!(datagram(&["127.0.0.1"], &[b'a'; MAX]).is_some());
        assert_eq!(datagram(&["127.0.0.1"], &[b'a'; MAX + 1]), None);
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
        let ended = handler(&[])
            .read(receiver, |_, _| async { Err(Closed) }, &mut Held::alone())
            .await;
        assert_eq!(ended, End::Stopping);
    }

    #[tokio::test]
    async fn a_quiet_sender_has_what_its_line_breaker_holds_cut() {
        let (mut sender, receiver) = tokio::io::duplex(1024);
        // The NUL ends the first message as it arrives; nothing but the quiet ends the second.
        sender.write_all(b"<13>a\x00<13>b").await.unwrap();
        let mut handler = handler(&[]);
        handler.line_breaker = Some(line_breaker(r"(\x00)"));
        handler.quiet = Duration::from_millis(20);
        let mut admitted = Vec::new();
        let ended = handler
            .read(
                receiver,
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
