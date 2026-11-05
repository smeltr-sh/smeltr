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

use super::line_breaker::{BREAK_SLACK, LineBreaker, Searched};
use super::receiver::{Closed, Connection, Counted, Cutter, Held, INBOX_BYTES, STREAM_STEP};
use crate::plan::topology::SyslogReceiverConfig;

/// How much one read of the socket takes.
const READ_STEP: usize = 64 * 1024;

/// How long a sender may go quiet before what a line breaker holds is taken as its last messages. Nothing waits behind
/// it on the connection, and a sender writes a message whole, so only a sender stalled this long mid-message has it cut
/// in two.
const QUIET: Duration = Duration::from_secs(30);

/// How long closing a connection waits for the sender to take its `close_notify`.
const CLOSING: Duration = Duration::from_secs(5);

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
    /// What is left of a truncated message, discarded before the next frame.
    discarding: Discarding,
    /// The messages it truncated.
    truncations: Truncations,
}

/// How many messages were truncated, and how many of them a log has told: logged at the first, again every
/// `RELOG_EVERY` while more are, and at a connection's end for any since.
#[derive(Debug, Default)]
struct Truncations {
    count: u64,
    logged: u64,
    repeat: crate::logs::Repeat,
}

impl Truncations {
    /// How many it has truncated, when that is due its log: never while none are new. `ending` makes the new ones due
    /// at once, as nothing follows to log them.
    fn due(&mut self, ending: bool) -> Option<u64> {
        if self.count == self.logged || !ending && !self.repeat.due() {
            return None;
        }
        self.logged = self.count;
        Some(self.count)
    }
}

/// What is left of a truncated message: how it ends decides how much more is discarded.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
enum Discarding {
    #[default]
    Nothing,
    /// The rest of an octet-counted message: this many bytes.
    Octets(u64),
    /// The rest of a line, up to and with its newline.
    Line,
    /// The rest of a message a line breaker ends, up to where it breaks.
    Break,
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
    /// The first bytes of a message too long to read whole, their bounds in `held`, and what follows them that is
    /// still the message's.
    Truncated(usize, usize, Discarding),
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
            discarding: Discarding::Nothing,
            truncations: Truncations::default(),
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
            if !self.discard() {
                return Ok(());
            }
            let frame = self.frame()?;
            if !self.take(frame, messages) {
                return Ok(());
            }
        }
    }

    /// Adds every message held whole to `messages`, as `cut` does, its line breaker searching all it holds however
    /// little arrived since it last searched: nothing more is coming.
    fn cut_whole(&mut self, messages: &mut Messages) -> Result<(), Malformed> {
        self.searched.found();
        self.cut(messages)
    }

    /// Adds a whole frame's message to `messages` and moves past it. `false` for a partial frame, which stays held.
    fn take(&mut self, frame: Frame, messages: &mut Messages) -> bool {
        let next = match frame {
            Frame::Message(start, end, next) => {
                messages.push(&self.held[start..end]);
                next
            }
            Frame::Truncated(start, end, rest) => {
                // What it keeps of a message that holds only whitespace so far is blank, and skipped as a blank
                // message is.
                if !self.held[start..end].iter().all(u8::is_ascii_whitespace) {
                    messages.push(&self.held[start..end]);
                    self.truncations.count += 1;
                }
                self.discarding = rest;
                end
            }
            Frame::Blank(next) => next,
            Frame::Partial => return false,
        };
        self.cut = next;
        self.searched.found();
        true
    }

    /// Moves past what it holds of a truncated message's rest. `false` while all it holds is that rest.
    fn discard(&mut self) -> bool {
        let rest = &self.held[self.cut..];
        let (discarded, done) = match self.discarding {
            Discarding::Nothing => return true,
            Discarding::Octets(left) => {
                let discarded = rest.len().min(usize::try_from(left).unwrap_or(usize::MAX));
                let left = left - discarded as u64;
                self.discarding = Discarding::Octets(left);
                (discarded, left == 0)
            }
            Discarding::Line => match rest.iter().position(|&b| b == b'\n') {
                Some(lf) => (lf + 1, true),
                None => (rest.len(), false),
            },
            Discarding::Break => {
                let line_breaker = self
                    .line_breaker
                    .as_ref()
                    .expect("only a line breaker's message is discarded to a break");
                match line_breaker.next_break(rest, false) {
                    Some((_, next)) => (next, true),
                    // What the pattern needs to see of the break's start stays held.
                    None => (rest.len().saturating_sub(BREAK_SLACK), false),
                }
            }
        };
        self.cut += discarded;
        if done {
            self.discarding = Discarding::Nothing;
            self.searched.found();
        }
        done
    }

    /// The connection's end: adds a last non-transparent message, which needs no newline, to `messages`. An
    /// octet-counted frame that never arrived whole is dropped, and `Ok` says how many bytes of it there were.
    fn finish(&mut self, messages: &mut Messages) -> Result<usize, Malformed> {
        self.end_discarding();
        if !self.discard() {
            return Ok(0);
        }
        if let Some(line_breaker) = self.line_breaker.take() {
            return self.end_broken(&line_breaker, messages);
        }
        let rest = &self.held[self.cut..];
        match rest.first() {
            None => {}
            Some(b'1'..=b'9') => return Ok(rest.len()),
            Some(_) => {
                self.held.push(b'\n');
                let frame = self.non_transparent(self.cut);
                self.take(frame, messages);
            }
        }
        Ok(0)
    }

    /// Whether what it holds waits on a line breaker's break, which a quiet sender may never send.
    fn waits(&self) -> bool {
        let breaking = self.discarding == Discarding::Break
            || self.discarding == Discarding::Nothing
                && self.held.get(self.cut).is_some_and(|b| !matches!(b, b'1'..=b'9'));
        self.line_breaker.is_some() && breaking
    }

    /// Nothing more is coming of a truncated message's rest that ends at a newline or a break: what it holds of it
    /// is all of it.
    fn end_discarding(&mut self) {
        if matches!(self.discarding, Discarding::Line | Discarding::Break) {
            self.cut = self.held.len();
            self.discarding = Discarding::Nothing;
        }
    }

    /// Adds what a line breaker holds to `messages` as though the stream had ended, its sender having gone quiet, and
    /// reads on. An octet-counted frame not yet whole stays held.
    fn flush(&mut self, messages: &mut Messages) -> Result<(), Malformed> {
        if self.discarding == Discarding::Break {
            self.end_discarding();
        }
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
            if !self.discard() {
                return Ok(0);
            }
            let start = self.cut;
            let rest = &self.held[start..];
            let frame = match rest.first() {
                None => return Ok(0),
                Some(b'1'..=b'9') => match self.octet_counted(start)? {
                    Frame::Partial => return Ok(rest.len()),
                    frame => frame,
                },
                Some(_) => match line_breaker.next_break(rest, true) {
                    Some((end, next)) => self.message(start, start + end, start + next),
                    None => {
                        let end = start + LineBreaker::last_event(rest).len();
                        self.message(start, end, self.held.len())
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
        let frame = self.non_transparent(start);
        // A whole line may hold its message and a CR.
        if matches!(frame, Frame::Partial) && self.held.len() - start > self.max + 1 {
            return Ok(Frame::Truncated(start, start + self.max, Discarding::Line));
        }
        Ok(frame)
    }

    /// `MSG-LEN SP MSG`, starting at `start`, which holds a digit 1-9. A message longer than the largest is
    /// truncated to it once that much has arrived.
    fn octet_counted(&self, start: usize) -> Result<Frame, Malformed> {
        let mut len = 0u64;
        for (i, &b) in self.held[start..].iter().enumerate() {
            match b {
                b'0'..=b'9' => {
                    // Checked as each digit arrives, so a run of digits is refused before it overflows. A length
                    // over what any receiver takes is a misframed stream, not a message to truncate.
                    len = len * 10 + u64::from(b - b'0');
                    if len > INBOX_BYTES as u64 {
                        return Err(self.malformed(
                            start,
                            "a message's length is over 64 MiB, the most any receiver takes".into(),
                        ));
                    }
                }
                b' ' => {
                    let message = start + i + 1;
                    let arrived = self.held.len() - message;
                    return Ok(match usize::try_from(len).ok().filter(|&len| len <= self.max) {
                        Some(len) if arrived >= len => Frame::Message(message, message + len, message + len),
                        None if arrived >= self.max => {
                            Frame::Truncated(message, message + self.max, Discarding::Octets(len - self.max as u64))
                        }
                        _ => Frame::Partial,
                    });
                }
                _ => {
                    return Err(self.malformed(start, "a message's length is not followed by a space".into()));
                }
            }
        }
        Ok(Frame::Partial)
    }

    /// `MSG LF`, starting at `start`. The message drops the CR a sender may put before the LF.
    fn non_transparent(&self, start: usize) -> Frame {
        let Some(lf) = self.held[start..].iter().position(|&b| b == b'\n') else {
            return Frame::Partial;
        };
        let next = start + lf + 1;
        let mut end = start + lf;
        if end > start && self.held[end - 1] == b'\r' {
            end -= 1;
        }
        self.message(start, end, next)
    }

    /// `MSG break`, starting at `start`: the message ends where `line_breaker` breaks what follows it.
    fn broken(&mut self, line_breaker: &LineBreaker, start: usize) -> Result<Frame, Malformed> {
        let rest = self.held.len() - start;
        let over = rest > self.max + BREAK_SLACK;
        if !self.searched.due(rest) && !over {
            return Ok(Frame::Partial);
        }
        if let Some((end, next)) = line_breaker.next_break(&self.held[start..], false) {
            return Ok(self.message(start, start + end, start + next));
        }
        self.searched.missed(rest);
        Ok(match over {
            true => Frame::Truncated(start, start + self.max, Discarding::Break),
            false => Frame::Partial,
        })
    }

    /// The message at `start..end`, held whole, the next frame starting at `next`: truncated to the largest, and
    /// blank when all of it is whitespace.
    fn message(&self, start: usize, end: usize, next: usize) -> Frame {
        if self.held[start..end].iter().all(u8::is_ascii_whitespace) {
            return Frame::Blank(next);
        }
        match end - start > self.max {
            true => Frame::Truncated(
                start,
                start + self.max,
                Discarding::Octets((next - start - self.max) as u64),
            ),
            false => Frame::Message(start, end, next),
        }
    }

    fn malformed(&self, at: usize, what: String) -> Malformed {
        Malformed {
            at: self.offset + at as u64,
            what,
        }
    }
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
    /// The largest message.
    max_message_bytes: usize,
    /// Where a non-transparent message ends, when not at a newline.
    line_breaker: Option<LineBreaker>,
    /// How long a sender may go quiet before a line breaker's held bytes are taken as its last messages.
    quiet: Duration,
    /// The datagrams it truncated.
    truncations: std::sync::Mutex<Truncations>,
}

impl Handler {
    pub(super) fn new(config: &SyslogReceiverConfig, max_message_bytes: usize) -> Handler {
        Handler {
            max_message_bytes,
            line_breaker: config.line_breaker.clone(),
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
                "a sender hung up partway through an octet-counted message, which is dropped"
            ),
            End::Malformed(Malformed { at, what }) => tracing::warn!(
                name: "malformed", target: "sources",
                subject = %remote,
                offset = at,
                what = %what,
                "a sender sent what this receiver does not read as syslog (RFC 6587); its connection is closed"
            ),
        }
    }

    /// The message one datagram holds (RFC 5426), its trailing newline dropped where a sender adds one, and truncated
    /// to the largest message (RFC 5424, 6.1). `None` for an empty one.
    pub(super) fn datagram(&self, datagram: &[u8], peer: IpAddr) -> Option<Cutter> {
        let message = datagram.strip_suffix(b"\n").unwrap_or(datagram);
        let mut message = message.strip_suffix(b"\r").unwrap_or(message);
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
            let mut broke = false;
            let cut = match read {
                Some(Ok(0)) => break,
                Some(Ok(n)) => {
                    framer.push(&buf[..n]);
                    framer.cut(&mut messages)
                }
                // The connection broke, as a TLS sender's does that hangs up without a `close_notify`. What is held
                // whole is the sender's, and what is not is cut short.
                Some(Err(_)) => {
                    broke = true;
                    framer.cut_whole(&mut messages)
                }
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
            self.log_truncated(&mut framer, remote, broke);
            if let Err(malformed) = cut {
                return End::Malformed(malformed);
            }
            if broke {
                return End::Gone { partial: 0 };
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

    fn handler() -> Handler {
        Handler::new(&SyslogReceiverConfig::default(), MAX)
    }

    /// What `serve`'s read loop admits from a connection that sends `sent` and hangs up, one entry per admission,
    /// and how the connection ended.
    async fn read_all(sent: &[u8]) -> (Vec<(Vec<String>, usize)>, End) {
        let (mut sender, receiver) = tokio::io::duplex(1024);
        sender.write_all(sent).await.unwrap();
        drop(sender);
        let mut admitted = Vec::new();
        let ended = (handler())
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
    fn an_octet_count_counts_octets_not_characters() {
        // `é` is two octets and `€` three: counted as characters, each length would end its message short.
        let cuts = cut_each(&["7 <13>éa".as_bytes(), "8 <13>€a".as_bytes(), b"<13>b\n"]).unwrap();
        assert_eq!(cuts, [vec!["<13>éa"], vec!["<13>€a"], vec!["<13>b"]]);
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

    /// `n` bytes of `b`, as a string.
    fn run(b: char, n: usize) -> String {
        b.to_string().repeat(n)
    }

    #[test]
    fn an_octet_counted_message_over_the_largest_is_truncated_and_the_rest_discarded() {
        let sent = format!("<13>a\n70 <13>{}{}<13>c\n", run('b', MAX - 4), run('x', 6));
        let cuts = cut_each(&[sent.as_bytes()]).unwrap();
        assert_eq!(
            cuts,
            [vec![
                "<13>a".to_string(),
                format!("<13>{}", run('b', MAX - 4)),
                "<13>c".into()
            ]]
        );
        // A length too long for any message is refused as its digits arrive, before the space.
        assert!(cut_each(&[b"99999999999999999999999"]).is_err());
        assert_eq!(cut_each(&[b"64 "]).unwrap(), [Vec::<String>::new()]);
        // A length over what any receiver takes is a misframed stream, which would otherwise discard all that follows.
        assert!(cut_each(&[format!("{} ", INBOX_BYTES + 1).as_bytes()]).is_err());
        assert_eq!(
            cut_each(&[format!("{INBOX_BYTES} ").as_bytes()]).unwrap(),
            [Vec::<String>::new()]
        );
    }

    #[test]
    fn truncations_are_logged_at_the_first_then_only_when_more_are() {
        let mut truncations = Truncations::default();
        assert_eq!(truncations.due(true), None);
        truncations.count = 1;
        assert_eq!(truncations.due(false), Some(1));
        // None new: due neither later nor at the end.
        assert_eq!(truncations.due(false), None);
        assert_eq!(truncations.due(true), None);
        // New ones wait out the repeat, but not the end.
        truncations.count = 3;
        assert_eq!(truncations.due(false), None);
        assert_eq!(truncations.due(true), Some(3));
    }

    #[test]
    fn a_line_over_the_largest_is_truncated_and_the_rest_discarded() {
        let line = run('a', MAX);
        assert_eq!(
            cut_each(&[line.as_bytes(), b"\r", b"\n"]).unwrap(),
            [vec![], vec![], vec![line.clone()]]
        );
        // Whole, or arriving past the largest before its newline.
        let cuts = cut_each(&[format!("{line}bb\n<13>c\n").as_bytes()]).unwrap();
        assert_eq!(cuts, [vec![line.clone(), "<13>c".into()]]);
        let cuts = cut_each(&[format!("{line}bb").as_bytes(), b"bb\n<13>c\n"]).unwrap();
        assert_eq!(cuts, [vec![line], vec!["<13>c".to_string()]]);
        // A blank line is skipped however long it is.
        let cuts = cut_each(&[format!("{}\n<13>c\n", run(' ', MAX + 2)).as_bytes()]).unwrap();
        assert_eq!(cuts, [vec!["<13>c".to_string()]]);
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
    fn a_message_over_the_largest_a_line_breaker_breaks_is_truncated() {
        let message = run('a', MAX + 1);
        let (cuts, last) = cut_all(broken(r"(\x00)"), &[message.as_bytes(), b"\x00b"]).unwrap();
        // Two new bytes are too few to search again for a break, so the message waits on the stream's end.
        assert_eq!(
            (cuts, last),
            (vec![vec![], vec![]], vec![run('a', MAX), "b".to_string()])
        );
    }

    #[test]
    fn a_message_a_line_breaker_never_breaks_is_truncated_past_the_slack_and_discarded_to_its_break() {
        let unbroken = run('a', MAX + BREAK_SLACK + 1);
        // Held to the slack, the break may yet come; past it the message is truncated.
        let (cuts, last) = cut_all(broken(r"(\x00)"), &[&unbroken.as_bytes()[..MAX + BREAK_SLACK]]).unwrap();
        assert_eq!((cuts, last), (vec![vec![]], vec![run('a', MAX)]));
        let rest = format!("{}\x00<13>c\x00", run('z', 3 * BREAK_SLACK));
        let (cuts, last) = cut_all(broken(r"(\x00)"), &[unbroken.as_bytes(), rest.as_bytes()]).unwrap();
        assert_eq!(
            (cuts, last),
            (vec![vec![run('a', MAX)], vec![]], vec!["<13>c".to_string()])
        );
    }

    /// A stream mixing every framing, and what the framer cuts of it.
    const MIXED: &[u8] = b"<13>a\r\n10 <13>b\nc\r\nd\n\n<13>e\n3 xyz";
    const MIXED_CUT: [&str; 4] = ["<13>a", "<13>b\nc\r\nd", "<13>e", "xyz"];

    /// What the framer cuts from `sent` in the pieces `at` splits it into, then from the connection's end.
    fn cut_split(sent: &[u8], at: &[usize]) -> Result<Vec<String>, Malformed> {
        let mut bounds = vec![0];
        bounds.extend_from_slice(at);
        bounds.push(sent.len());
        let pieces: Vec<&[u8]> = bounds.windows(2).map(|w| &sent[w[0]..w[1]]).collect();
        let (cuts, last) = cut_all(Framer::new(MAX, None), &pieces)?;
        Ok(cuts.into_iter().flatten().chain(last).collect())
    }

    #[test]
    fn a_stream_fragmented_anywhere_cuts_the_same_messages() {
        for split in 1..MIXED.len() {
            assert_eq!(cut_split(MIXED, &[split]).unwrap(), MIXED_CUT, "split at {split}");
        }
        let every_byte: Vec<usize> = (1..MIXED.len()).collect();
        assert_eq!(cut_split(MIXED, &every_byte).unwrap(), MIXED_CUT);
    }

    #[test]
    fn an_oversized_frame_is_truncated_however_it_is_fragmented() {
        let kept = format!("<13>{}", run('b', MAX - 4));
        let counted = |len: usize| format!("{len} {kept}{}", run('x', len - MAX));
        for oversized in [counted(MAX + 1), counted(3 * MAX), format!("{kept}xx\r\n")] {
            let sent = format!("<13>a\n{oversized}<13>c\n");
            let expected = ["<13>a", &kept, "<13>c"];
            for split in 1..sent.len() {
                assert_eq!(
                    cut_split(sent.as_bytes(), &[split]).unwrap(),
                    expected,
                    "split at {split}"
                );
            }
            let every_byte: Vec<usize> = (1..sent.len()).collect();
            assert_eq!(cut_split(sent.as_bytes(), &every_byte).unwrap(), expected);
        }
    }

    #[test]
    fn a_frame_of_the_largest_size_is_read_however_it_is_fragmented() {
        let message = [b"<13>".as_slice(), &[b'a'; MAX - 4]].concat();
        let sent = [format!("{MAX} ").as_bytes(), &message, &message, b"\n"].concat();
        let message = String::from_utf8(message).unwrap();
        let expected = [message.clone(), message];
        for split in 1..sent.len() {
            assert_eq!(cut_split(&sent, &[split]).unwrap(), expected, "split at {split}");
        }
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
        handler.line_breaker = Some(line_breaker(r"(\x00)"));
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

    /// Reads its chunks, one a read, then fails, as a connection that breaks.
    struct Breaks(std::collections::VecDeque<Vec<u8>>);

    impl AsyncRead for Breaks {
        fn poll_read(
            mut self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(match self.0.pop_front() {
                Some(chunk) => {
                    buf.put_slice(&chunk);
                    Ok(())
                }
                None => Err(std::io::ErrorKind::UnexpectedEof.into()),
            })
        }
    }

    #[tokio::test]
    async fn a_broken_connection_keeps_what_its_line_breaker_holds_whole() {
        let mut handler = Handler::new(&SyslogReceiverConfig::default(), 1024);
        handler.line_breaker = Some(line_breaker(r"(\x00)"));
        // The second read is too little beside the first for the line breaker to search again as it arrives.
        let long = format!("<13>a{}", run('x', 900));
        let mut admitted = Vec::new();
        let ended = handler
            .read(
                Breaks([long.clone().into_bytes(), b"\x00<13>b".to_vec()].into()),
                "127.0.0.1",
                |messages: Messages, _| {
                    admitted.extend(strings(messages));
                    async { Ok(()) }
                },
                &mut Held::alone(),
            )
            .await;
        assert_eq!(ended, End::Gone { partial: 0 });
        // What follows the last break was cut short.
        assert_eq!(admitted, [long]);
    }
}
