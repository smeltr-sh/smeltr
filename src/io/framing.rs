//! Cuts a stream of bytes into frames, each a record: what a receiver's connection sends, and what an object holds.
//! A [`Framing`] says where one frame ends and the next starts; a [`Framer`] is pushed the bytes as they arrive and
//! cuts every frame they complete.
//!
//! ```text
//!   lines          MSG LF                 (one CR before the LF dropped)
//!   escapedLines   MSG LF                 (as lines, each unescaped as a bytes object wrote it)
//!   lineBreaker    MSG break              (MSG ends where the pattern breaks; a blank event skipped)
//!   lengthPrefix   HEADER LENGTH REST     (LENGTH, at a fixed offset, counts REST or the whole frame)
//!   fixed          MSG                    (every frame the same size)
//!   object         MSG                    (all of the stream, once it ends)
//!   syslog         MSG-LEN SP MSG         (octet counting, by a first byte of 1-9)
//!                  MSG LF | MSG break     (otherwise; a blank message skipped)
//! ```
//!
//! A frame longer than the largest keeps its first bytes, up to the largest, and the rest of it is discarded as it
//! arrives: the stream reads on. A frame whose length is over [`INBOX_BYTES`], the most any receiver takes, is a
//! misframed stream, and cutting refuses it.

use super::line_breaker::{BREAK_SLACK, LineBreaker, Searched};
use super::lines::unescape_line;
use super::receiver::INBOX_BYTES;

/// Where one frame of a stream ends and the next starts.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum Framing {
    /// One frame per line; a line ends at `\n`, and a `\r` before it is dropped.
    #[default]
    Lines,
    /// One frame per line, as `Lines`, each unescaped as a `bytes` object with `Newlines::Escape` wrote it.
    EscapedLines,
    /// One frame per event, cut where the line breaker breaks the stream.
    LineBreaker(LineBreaker),
    /// Each frame says how long it is, in a field at a fixed place in its header.
    LengthPrefix(LengthPrefix),
    /// Every frame is this many bytes. Validation refused none.
    Fixed(usize),
    /// The whole stream is one frame.
    Object,
    /// Syslog over TCP (RFC 6587): a frame that starts with a digit 1-9 is octet-counted, `MSG-LEN SP MSG`, and any
    /// other ends at a newline, or where its line breaker breaks it. A sender may mix them.
    Syslog(Option<LineBreaker>),
}

/// Where a frame's length is and what it counts. The frame is the whole of it, its header and length among it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LengthPrefix {
    /// How many bytes of the frame come before its length.
    pub offset: usize,
    /// How many bytes the length is, 1 to 8. Validation refused any other.
    pub width: usize,
    pub order: ByteOrder,
    pub counts: Counts,
}

/// Which byte of a number comes first.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ByteOrder {
    /// The most significant first, as a network protocol writes it.
    #[default]
    Big,
    Little,
}

/// What a frame's length counts.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Counts {
    /// The bytes that follow the length.
    #[default]
    Rest,
    /// The whole message, its header and length among them.
    Message,
}

impl LengthPrefix {
    /// How many bytes a frame has before what its length counts as the rest.
    fn header(&self) -> usize {
        self.offset + self.width
    }

    /// The length a frame's header holds, as an unsigned number.
    fn read(&self, header: &[u8]) -> u64 {
        let length = &header[self.offset..self.header()];
        let mut bytes = [0u8; 8];
        match self.order {
            ByteOrder::Big => {
                bytes[8 - self.width..].copy_from_slice(length);
                u64::from_be_bytes(bytes)
            }
            ByteOrder::Little => {
                bytes[..self.width].copy_from_slice(length);
                u64::from_le_bytes(bytes)
            }
        }
    }
}

/// Frames cut from a stream, each a record. Held in one buffer, each frame its bytes and where it ends, so many short
/// frames cost about what was sent.
#[derive(Default)]
pub(super) struct Messages {
    bytes: Vec<u8>,
    /// Where each frame ends in `bytes`. What one push completes is at most what the framer held, a frame short of the
    /// largest and a push, so an offset fits.
    ends: Vec<u32>,
    taken: usize,
}

impl Messages {
    pub(super) fn push(&mut self, message: &[u8]) {
        self.bytes.extend_from_slice(message);
        self.ends.push(self.bytes.len() as u32);
    }

    pub(super) fn is_empty(&self) -> bool {
        self.taken == self.ends.len()
    }

    /// What its frames hold once taken, as the inbox counts a record, and what holding them costs until then.
    pub(super) fn size(&self) -> usize {
        self.bytes.len() + self.ends.len() * size_of::<u32>()
    }
}

impl Messages {
    /// The bytes of its frames not yet taken.
    pub(super) fn untaken(&self) -> usize {
        let taken = match self.taken {
            0 => 0,
            i => self.ends[i - 1] as usize,
        };
        self.bytes.len() - taken
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

/// How many frames were truncated, and how many of them a log has told: logged at the first, again every
/// `RELOG_EVERY` while more are, and at a stream's end for any since.
#[derive(Debug, Default)]
pub(super) struct Truncations {
    pub(super) count: u64,
    logged: u64,
    repeat: crate::logs::Repeat,
}

impl Truncations {
    /// How many it has truncated, when that is due its log: never while none are new. `ending` makes the new ones due
    /// at once, as nothing follows to log them.
    pub(super) fn due(&mut self, ending: bool) -> Option<u64> {
        if self.count == self.logged || !ending && !self.repeat.due() {
            return None;
        }
        self.logged = self.count;
        Some(self.count)
    }
}

/// What is left of a truncated frame: how it ends decides how much more is discarded.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
enum Discarding {
    #[default]
    Nothing,
    /// The rest of a frame whose length is known: this many bytes.
    Octets(u64),
    /// The rest of a line, up to and with its newline.
    Line,
    /// The rest of a frame a line breaker ends, up to where it breaks.
    Break,
    /// The rest of the stream.
    All,
}

/// Why a frame was refused, and where it starts in the stream.
#[derive(Debug, PartialEq)]
pub(super) struct Malformed {
    pub(super) at: u64,
    pub(super) what: String,
}

/// One frame read from the front of what is held.
enum Frame {
    /// A frame, its bounds in `held`, and where the next starts.
    Message(usize, usize, usize),
    /// The first bytes of a frame too long to read whole, their bounds in `held`, and what follows them that is
    /// still the frame's.
    Truncated(usize, usize, Discarding),
    /// A blank frame, skipped, and where the next starts.
    Blank(usize),
    /// Not all of it has arrived.
    Partial,
}

/// The kinds of frame, a line breaker aside: `Framing` without what a frame of each is cut by.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Cuts {
    Lines { escaped: bool },
    Breaks,
    Prefixed(LengthPrefix),
    Fixed(usize),
    Object,
    Syslog,
}

/// Cuts a stream's bytes into frames as they are pushed.
pub(super) struct Framer {
    held: Vec<u8>,
    /// How much of `held` is already cut.
    cut: usize,
    /// The largest frame.
    max: usize,
    /// Whether a frame over the largest keeps its first bytes, rather than being refused.
    truncates: bool,
    /// How many bytes the stream had before `held`, so a refusal names where it went wrong.
    offset: u64,
    cuts: Cuts,
    /// Where a frame ends that a line breaker ends.
    line_breaker: Option<LineBreaker>,
    /// How far the line breaker searched what follows `cut` in vain.
    searched: Searched,
    /// What is left of a truncated frame, discarded before the next.
    discarding: Discarding,
    /// The frames it truncated.
    pub(super) truncations: Truncations,
}

impl Framer {
    /// A framer that truncates a frame over `max` bytes, as a receiver does: the stream reads on.
    pub(super) fn new(framing: Framing, max: usize) -> Framer {
        Framer::truncating(framing, max, true)
    }

    /// A framer that refuses a frame over `max` bytes, as an object source does: the object is unreadable.
    pub(super) fn refusing(framing: Framing, max: usize) -> Framer {
        Framer::truncating(framing, max, false)
    }

    fn truncating(framing: Framing, max: usize, truncates: bool) -> Framer {
        let (cuts, line_breaker) = match framing {
            Framing::Lines => (Cuts::Lines { escaped: false }, None),
            Framing::EscapedLines => (Cuts::Lines { escaped: true }, None),
            Framing::LineBreaker(line_breaker) => (Cuts::Breaks, Some(line_breaker)),
            Framing::LengthPrefix(prefix) => (Cuts::Prefixed(prefix), None),
            Framing::Fixed(size) => {
                // A frame of no bytes would cut forever without moving on.
                assert!(size > 0, "validation refused a fixed size of 0");
                (Cuts::Fixed(size), None)
            }
            Framing::Object => (Cuts::Object, None),
            Framing::Syslog(line_breaker) => (Cuts::Syslog, line_breaker),
        };
        Framer {
            held: Vec::new(),
            cut: 0,
            max,
            truncates,
            offset: 0,
            cuts,
            line_breaker,
            searched: Searched::default(),
            discarding: Discarding::Nothing,
            truncations: Truncations::default(),
        }
    }

    pub(super) fn push(&mut self, bytes: &[u8]) {
        if self.cut > 0 {
            self.offset += self.cut as u64;
            self.held.drain(..self.cut);
            self.cut = 0;
        }
        self.held.extend_from_slice(bytes);
    }

    /// How many bytes its buffer holds room for, which a reader charges against what it may hold.
    pub(super) fn capacity(&self) -> usize {
        self.held.capacity()
    }

    /// The bytes it holds that it has not yet cut.
    pub(super) fn uncut(&self) -> usize {
        self.held.len() - self.cut
    }

    /// Adds every frame held whole to `messages`, in order. What follows the last stays held for the next push. A
    /// refused frame leaves the frames before it in `messages`.
    pub(super) fn cut(&mut self, messages: &mut Messages) -> Result<(), Malformed> {
        loop {
            if !self.discard() {
                return Ok(());
            }
            let frame = self.frame()?;
            if !self.take(frame, messages)? {
                return Ok(());
            }
        }
    }

    /// Adds a whole frame to `messages` and moves past it. `false` for a partial frame, which stays held. A frame over
    /// the largest is refused where the framer does not truncate.
    fn take(&mut self, frame: Frame, messages: &mut Messages) -> Result<bool, Malformed> {
        let next = match frame {
            Frame::Message(start, end, next) => {
                self.put(start, end, messages);
                next
            }
            Frame::Truncated(start, end, rest) => {
                // What it keeps of a frame that skips blanks and holds only whitespace so far is blank, and skipped as
                // a blank frame is.
                if !(self.skips_blanks() && self.held[start..end].iter().all(u8::is_ascii_whitespace)) {
                    if !self.truncates {
                        return Err(self.malformed(start, format!("a record is longer than {} bytes", self.max)));
                    }
                    self.put(start, end, messages);
                    self.truncations.count += 1;
                }
                self.discarding = rest;
                end
            }
            Frame::Blank(next) => next,
            Frame::Partial => return Ok(false),
        };
        self.cut = next;
        self.searched.found();
        Ok(true)
    }

    fn put(&self, start: usize, end: usize, messages: &mut Messages) {
        match self.cuts {
            Cuts::Lines { escaped: true } => messages.push(&unescape_line(&self.held[start..end])),
            _ => messages.push(&self.held[start..end]),
        }
    }

    /// Whether a frame of nothing but whitespace is no frame: a syslog message and a line breaker's event, as Splunk
    /// skips one. A line, a counted frame and an object are what was sent, blank or not.
    fn skips_blanks(&self) -> bool {
        matches!(self.cuts, Cuts::Syslog | Cuts::Breaks)
    }

    /// Moves past what it holds of a truncated frame's rest. `false` while all it holds is that rest.
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
                    .expect("only a line breaker's frame is discarded to a break");
                match line_breaker.next_break(rest, false) {
                    Some((_, next)) => (next, true),
                    // What the pattern needs to see of the break's start stays held.
                    None => (rest.len().saturating_sub(BREAK_SLACK), false),
                }
            }
            Discarding::All => (rest.len(), false),
        };
        self.cut += discarded;
        if done {
            self.discarding = Discarding::Nothing;
            self.searched.found();
        }
        done
    }

    /// The stream's end: adds a last frame that needs no end of its own, a line, an event or an object, to
    /// `messages`. A frame whose length says more was to come is dropped, and `Ok` says how many bytes of it there
    /// were.
    pub(super) fn finish(&mut self, messages: &mut Messages) -> Result<usize, Malformed> {
        self.end_discarding();
        if !self.discard() {
            return Ok(0);
        }
        match self.cuts {
            Cuts::Syslog | Cuts::Breaks if self.line_breaker.is_some() => {
                let line_breaker = self.line_breaker.take().expect("a line breaker");
                self.end_broken(&line_breaker, messages)
            }
            Cuts::Syslog if matches!(self.held.get(self.cut), Some(b'1'..=b'9')) => Ok(self.held.len() - self.cut),
            Cuts::Syslog | Cuts::Lines { .. } | Cuts::Breaks => {
                if self.cut < self.held.len() {
                    self.held.push(b'\n');
                    let frame = self.line(self.cut);
                    self.take(frame, messages)?;
                }
                Ok(0)
            }
            Cuts::Object => {
                let (start, end) = (self.cut, self.held.len());
                if start < end {
                    let frame = self.message(start, end, end);
                    self.take(frame, messages)?;
                }
                Ok(0)
            }
            Cuts::Prefixed(_) | Cuts::Fixed(_) => Ok(self.held.len() - self.cut),
        }
    }

    /// Whether what it holds waits on a line breaker's break, which a quiet sender may never send.
    pub(super) fn waits(&self) -> bool {
        let breaking = self.discarding == Discarding::Break
            || self.discarding == Discarding::Nothing
                && self
                    .held
                    .get(self.cut)
                    .is_some_and(|b| self.cuts != Cuts::Syslog || !matches!(b, b'1'..=b'9'));
        self.line_breaker.is_some() && breaking
    }

    /// Nothing more is coming of a truncated frame's rest that ends at a newline, a break or the stream's end: what it
    /// holds of it is all of it.
    fn end_discarding(&mut self) {
        if matches!(self.discarding, Discarding::Line | Discarding::Break | Discarding::All) {
            self.cut = self.held.len();
            self.discarding = Discarding::Nothing;
        }
    }

    /// Adds what a line breaker holds to `messages` as though the stream had ended, its sender having gone quiet, and
    /// reads on. A counted frame not yet whole stays held.
    pub(super) fn flush(&mut self, messages: &mut Messages) -> Result<(), Malformed> {
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

    /// Cuts every frame held, nothing more to come, up to an octet-counted frame not yet whole, whose length held it
    /// returns.
    fn end_broken(&mut self, line_breaker: &LineBreaker, messages: &mut Messages) -> Result<usize, Malformed> {
        loop {
            if !self.discard() {
                return Ok(0);
            }
            let start = self.cut;
            let rest = &self.held[start..];
            let frame = match rest.first() {
                None => return Ok(0),
                Some(b'1'..=b'9') if self.cuts == Cuts::Syslog => match self.octet_counted(start)? {
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
            assert!(self.take(frame, messages)?, "an ended stream's frame is whole");
        }
    }

    fn frame(&mut self) -> Result<Frame, Malformed> {
        let start = self.cut;
        let Some(&first) = self.held.get(start) else {
            return Ok(Frame::Partial);
        };
        match self.cuts {
            Cuts::Syslog if first.is_ascii_digit() && first != b'0' => self.octet_counted(start),
            Cuts::Syslog | Cuts::Breaks if self.line_breaker.is_some() => {
                let line_breaker = self.line_breaker.take().expect("a line breaker");
                let frame = self.broken(&line_breaker, start);
                self.line_breaker = Some(line_breaker);
                frame
            }
            Cuts::Syslog | Cuts::Lines { .. } | Cuts::Breaks => {
                let frame = self.line(start);
                // A whole line may hold its frame and a CR.
                if matches!(frame, Frame::Partial) && self.held.len() - start > self.max + 1 {
                    return Ok(Frame::Truncated(start, start + self.max, Discarding::Line));
                }
                Ok(frame)
            }
            Cuts::Prefixed(prefix) => self.prefixed(prefix, start),
            Cuts::Fixed(size) => Ok(self.counted(start, size as u64)),
            Cuts::Object => Ok(match self.held.len() - start > self.max {
                true => Frame::Truncated(start, start + self.max, Discarding::All),
                false => Frame::Partial,
            }),
        }
    }

    /// `MSG-LEN SP MSG`, starting at `start`, which holds a digit 1-9.
    fn octet_counted(&self, start: usize) -> Result<Frame, Malformed> {
        let mut len = 0u64;
        for (i, &b) in self.held[start..].iter().enumerate() {
            match b {
                b'0'..=b'9' => {
                    // Checked as each digit arrives, so a run of digits is refused before it overflows.
                    len = len * 10 + u64::from(b - b'0');
                    self.within_inbox(start, len)?;
                }
                b' ' => return Ok(self.counted(start + i + 1, len)),
                _ => {
                    return Err(self.malformed(start, "a message's length is not followed by a space".into()));
                }
            }
        }
        Ok(Frame::Partial)
    }

    /// `HEADER LENGTH REST`, starting at `start`, the whole of it the frame.
    fn prefixed(&self, prefix: LengthPrefix, start: usize) -> Result<Frame, Malformed> {
        let Some(header) = self.held.get(start..start + prefix.header()) else {
            return Ok(Frame::Partial);
        };
        let length = prefix.read(header);
        let len = match prefix.counts {
            Counts::Rest => length.saturating_add(prefix.header() as u64),
            Counts::Message if length < prefix.header() as u64 => {
                return Err(self.malformed(
                    start,
                    format!(
                        "a message's length is {length} bytes, shorter than the {} bytes of its header and length",
                        prefix.header()
                    ),
                ));
            }
            Counts::Message => length,
        };
        self.within_inbox(start, len)?;
        Ok(self.counted(start, len))
    }

    /// Refuses a frame at `start` of `len` bytes, over what any receiver takes: a misframed stream, not a frame to
    /// truncate.
    fn within_inbox(&self, start: usize, len: u64) -> Result<(), Malformed> {
        match len > INBOX_BYTES as u64 {
            true => Err(self.malformed(
                start,
                "a message's length is over 64 MiB, the most any receiver takes".into(),
            )),
            false => Ok(()),
        }
    }

    /// The frame of `len` bytes from `message`. One longer than the largest is truncated to it once that much has
    /// arrived.
    fn counted(&self, message: usize, len: u64) -> Frame {
        let arrived = self.held.len() - message;
        match usize::try_from(len).ok().filter(|&len| len <= self.max) {
            Some(len) if arrived >= len => Frame::Message(message, message + len, message + len),
            None if arrived >= self.max => {
                Frame::Truncated(message, message + self.max, Discarding::Octets(len - self.max as u64))
            }
            _ => Frame::Partial,
        }
    }

    /// `MSG LF`, starting at `start`. The frame drops the CR a sender may put before the LF.
    fn line(&self, start: usize) -> Frame {
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

    /// `MSG break`, starting at `start`: the frame ends where `line_breaker` breaks what follows it.
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

    /// The frame at `start..end`, held whole, the next starting at `next`: truncated to the largest, and blank when
    /// all of it is whitespace and it skips blanks.
    fn message(&self, start: usize, end: usize, next: usize) -> Frame {
        if self.skips_blanks() && self.held[start..end].iter().all(u8::is_ascii_whitespace) {
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

#[cfg(test)]
mod tests {
    use super::*;

    const MAX: usize = 64;

    /// What the framer cuts after each of `pushes`, as strings, and the first refusal.
    fn cut_each(pushes: &[&[u8]]) -> Result<Vec<Vec<String>>, Malformed> {
        let mut framer = Framer::new(Framing::Syslog(None), MAX);
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

    /// A syslog framer whose non-transparent messages `pattern` breaks.
    fn broken(pattern: &str) -> Framer {
        Framer::new(Framing::Syslog(Some(line_breaker(pattern))), MAX)
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
        let (cuts, last) = cut_all(Framer::new(Framing::Syslog(None), MAX), &pieces)?;
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
        let mut framer = Framer::new(Framing::Syslog(None), MAX);
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

    /// What a framer of `framing` cuts from `sent` in the pieces `at` splits it into, then from the stream's end, and
    /// the bytes of a partial frame it dropped.
    fn frames(framing: &Framing, sent: &[u8], at: &[usize]) -> Result<(Vec<Vec<u8>>, usize), Malformed> {
        let mut framer = Framer::new(framing.clone(), MAX);
        let mut bounds = vec![0];
        bounds.extend_from_slice(at);
        bounds.push(sent.len());
        let mut cut = Vec::new();
        for w in bounds.windows(2) {
            framer.push(&sent[w[0]..w[1]]);
            let mut messages = Messages::default();
            framer.cut(&mut messages)?;
            cut.extend(messages);
        }
        let mut last = Messages::default();
        let partial = framer.finish(&mut last)?;
        cut.extend(last);
        Ok((cut, partial))
    }

    /// What `frames` cuts from `sent` whole, after checking every split of it in two and into single bytes cuts the
    /// same.
    fn frames_however_split(framing: &Framing, sent: &[u8]) -> Result<(Vec<Vec<u8>>, usize), Malformed> {
        let whole = frames(framing, sent, &[]);
        for split in 1..sent.len() {
            assert_eq!(frames(framing, sent, &[split]), whole, "split at {split}");
        }
        let every_byte: Vec<usize> = (1..sent.len()).collect();
        assert_eq!(frames(framing, sent, &every_byte), whole, "split at every byte");
        whole
    }

    fn prefix(offset: usize, width: usize, order: ByteOrder, counts: Counts) -> Framing {
        Framing::LengthPrefix(LengthPrefix {
            offset,
            width,
            order,
            counts,
        })
    }

    #[test]
    fn a_length_prefix_frames_the_whole_frame_its_header_and_length_among_it() {
        let big = prefix(0, 2, ByteOrder::Big, Counts::Rest);
        let sent = b"\x00\x03abc\x00\x00\x00\x01d";
        let expected = vec![b"\x00\x03abc".to_vec(), b"\x00\x00".to_vec(), b"\x00\x01d".to_vec()];
        assert_eq!(frames_however_split(&big, sent).unwrap(), (expected, 0));

        // A header before a little-endian length that counts the whole frame.
        let little = prefix(1, 4, ByteOrder::Little, Counts::Message);
        let sent = b"T\x07\x00\x00\x00abU\x05\x00\x00\x00";
        let expected = vec![b"T\x07\x00\x00\x00ab".to_vec(), b"U\x05\x00\x00\x00".to_vec()];
        assert_eq!(frames_however_split(&little, sent).unwrap(), (expected, 0));

        // An odd width.
        let three = prefix(0, 3, ByteOrder::Big, Counts::Rest);
        assert_eq!(
            frames_however_split(&three, b"\x00\x00\x02hi").unwrap(),
            (vec![b"\x00\x00\x02hi".to_vec()], 0)
        );
    }

    #[test]
    fn a_length_prefix_over_the_largest_is_truncated_and_one_shorter_than_its_header_refused() {
        let framing = prefix(0, 1, ByteOrder::Big, Counts::Rest);
        let sent = [&[(MAX + 9) as u8][..], &[b'a'; MAX + 9], b"\x01z"].concat();
        let (cut, partial) = frames_however_split(&framing, &sent).unwrap();
        assert_eq!(cut, [sent[..MAX].to_vec(), b"\x01z".to_vec()]);
        assert_eq!(partial, 0);

        let framing = prefix(0, 2, ByteOrder::Big, Counts::Message);
        let refused = frames(&framing, b"\x00\x02\x00\x01", &[]).unwrap_err();
        assert_eq!(refused.at, 2);

        // A length over what any receiver takes misframes the stream.
        let framing = prefix(0, 8, ByteOrder::Big, Counts::Rest);
        assert!(frames(&framing, &u64::MAX.to_be_bytes(), &[]).is_err());
    }

    #[test]
    fn a_frame_a_counted_framing_never_finishes_is_dropped_at_the_end() {
        let framing = prefix(0, 1, ByteOrder::Big, Counts::Rest);
        assert_eq!(
            frames(&framing, b"\x01a\x05ab", &[]).unwrap(),
            (vec![b"\x01a".to_vec()], 3)
        );
        assert_eq!(
            frames(&Framing::Fixed(3), b"abcde", &[]).unwrap(),
            (vec![b"abc".to_vec()], 2)
        );
    }

    #[test]
    fn a_fixed_size_cuts_every_frame_alike_blank_or_not() {
        let (cut, partial) = frames_however_split(&Framing::Fixed(2), b"ab  \n\ncd").unwrap();
        assert_eq!(
            (cut, partial),
            (
                vec![b"ab".to_vec(), b"  ".to_vec(), b"\n\n".to_vec(), b"cd".to_vec()],
                0
            )
        );
    }

    #[test]
    fn lines_keep_a_blank_line_and_a_last_line_needs_no_newline() {
        let (cut, partial) = frames_however_split(&Framing::Lines, b"a\r\n\n  \nb").unwrap();
        assert_eq!(
            (cut, partial),
            (vec![b"a".to_vec(), b"".to_vec(), b"  ".to_vec(), b"b".to_vec()], 0)
        );
        let (cut, _) = frames_however_split(&Framing::EscapedLines, b"a\\nb\nc\\\\").unwrap();
        assert_eq!(cut, [b"a\nb".to_vec(), b"c\\".to_vec()]);
    }

    #[test]
    fn an_object_is_one_frame_and_one_over_the_largest_is_truncated() {
        let (cut, _) = frames_however_split(&Framing::Object, b"a\nb\n").unwrap();
        assert_eq!(cut, [b"a\nb\n".to_vec()]);
        assert_eq!(frames(&Framing::Object, b"", &[]).unwrap(), (vec![], 0));
        let mut framer = Framer::new(Framing::Object, MAX);
        framer.push(&[b'x'; MAX + 1]);
        let mut messages = Messages::default();
        framer.cut(&mut messages).unwrap();
        assert_eq!(messages.collect::<Vec<_>>(), [vec![b'x'; MAX]]);
        assert_eq!(framer.truncations.count, 1);
    }

    #[test]
    fn a_line_breaker_alone_cuts_what_a_digit_starts_as_any_other_event() {
        let framing = Framing::LineBreaker(line_breaker(r"(\x00)"));
        let (cut, partial) = frames_however_split(&framing, b"5 abcdef\x00 \x00g").unwrap();
        assert_eq!((cut, partial), (vec![b"5 abcdef".to_vec(), b"g".to_vec()], 0));
    }
}
