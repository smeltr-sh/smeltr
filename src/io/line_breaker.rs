//! Breaks a stream of bytes into events where a pattern matches, as Splunk's `LINE_BREAKER`. The pattern's first
//! capture group is the break: the bytes before it end an event, the group is dropped, and the bytes after it start the
//! next.
//!
//! ```text
//!   </event>(\s*)        <event>1</event>\n<event>2</event><event>3</event>
//!                        └──── event ───┘  └──── event ───┘└──── event ───┘
//!
//!   ([\r\n]+)\d{4}-      2026-10-11 ERROR failed\n    at a.b.C.run\n2026-10-11 INFO ok
//!                        └──────────────── event ──────────────┘  └─── event ───┘
//! ```
//!
//! What follows the group is not dropped, so a pattern can name how an event starts as well as how one ends. The
//! bytes after the last break are the stream's last event, its last line end dropped: a file ends with one, and no
//! pattern that breaks before how an event starts sees it.

use std::ops::Range;

use regex::bytes::Regex;

/// Splunk's default `LINE_BREAKER`. A sourcetype given it is an event per line, as one given none: a line breaker holds
/// a match that reaches the end of what came until more comes, and a line end is often the last byte of a chunk.
pub const PER_LINE: &str = r"([\r\n]+)";

/// The first line end in `bytes`, for a stream with no line breaker: where the line ends and where the next starts. A
/// line ends at `\n`, `\r\n` or a lone `\r`, as an indexer's default `LINE_BREAKER` ends it. A `\r` that is the last
/// of `bytes` ends its line; one whose `\n` comes in the next chunk leaves that `\n` to start the next.
pub fn next_line(bytes: &[u8]) -> Option<(usize, usize)> {
    let end = bytes.iter().position(|&b| b == b'\n' || b == b'\r')?;
    let next = match bytes[end] {
        b'\r' if bytes.get(end + 1) == Some(&b'\n') => end + 2,
        _ => end + 1,
    };
    Some((end, next))
}

/// How far past the longest record bytes with no break in them may be held: room for a break that the limit falls in,
/// and for what the pattern must see after it, so a cut at the limit does not split it.
pub const BREAK_SLACK: usize = 64 * 1024;

/// A pattern whose first capture group is where an event ends. Validation refused one without a group, and one that
/// matches no bytes at all, which would break between every byte.
#[derive(Clone, Debug)]
pub struct LineBreaker(pub Regex);

impl PartialEq for LineBreaker {
    fn eq(&self, other: &LineBreaker) -> bool {
        self.0.as_str() == other.0.as_str()
    }
}

impl Eq for LineBreaker {}

impl LineBreaker {
    /// The first break in `bytes`, which start an event: where that event ends and where the next starts. A break
    /// whose match reaches the end of `bytes` is not taken until `ended`, the stream having no more bytes to come,
    /// since more could change where it ends. A match of no bytes, and an empty break at the start of `bytes`, are
    /// passed over, as they would cut nothing.
    pub fn next_break(&self, bytes: &[u8], ended: bool) -> Option<(usize, usize)> {
        let mut at = 0;
        while at <= bytes.len() {
            let found = self.0.captures_at(bytes, at)?;
            let whole = found.get(0).expect("a match has its whole");
            // A match of no bytes, as `\b()` makes between words, breaks nothing.
            if whole.is_empty() {
                at = whole.start() + 1;
                continue;
            }
            if !ended && whole.end() == bytes.len() {
                return None;
            }
            // A group that did not take part in the match breaks at the whole match.
            let group = found.get(1).unwrap_or(whole);
            if group.end() > 0 {
                return Some((group.start(), group.end()));
            }
            at = whole.start() + 1;
        }
        None
    }

    /// The last event of a stream that ended: `rest`, the bytes after its last break, its last line end dropped.
    pub fn last_event(rest: &[u8]) -> &[u8] {
        [&b"\r\n"[..], b"\n", b"\r"]
            .iter()
            .find_map(|end| rest.strip_suffix(*end))
            .unwrap_or(rest)
    }

    /// The events it breaks off `bytes`, which start an event, each a range of `bytes`, in order. An event of nothing
    /// but whitespace is passed over. Once the stream has `ended`, the bytes after the last break are its last event;
    /// until then they are left, from [`Events::rest`] on, for more to come.
    pub fn events<'a>(&'a self, bytes: &'a [u8], ended: bool) -> Events<'a> {
        Events {
            line_breaker: self,
            bytes,
            ended,
            at: 0,
        }
    }
}

/// The events a [`LineBreaker`] breaks off a stream's bytes, from [`LineBreaker::events`].
pub struct Events<'a> {
    line_breaker: &'a LineBreaker,
    bytes: &'a [u8],
    ended: bool,
    at: usize,
}

impl Events<'_> {
    /// Where the bytes not yet taken as events start: the end of `bytes` once an ended stream's last event is taken.
    pub fn rest(&self) -> usize {
        self.at
    }
}

impl Iterator for Events<'_> {
    type Item = Range<usize>;

    fn next(&mut self) -> Option<Range<usize>> {
        while self.at < self.bytes.len() {
            let start = self.at;
            let rest = &self.bytes[start..];
            let end = match self.line_breaker.next_break(rest, self.ended) {
                Some((end, next)) => {
                    self.at += next;
                    start + end
                }
                None if self.ended => {
                    self.at = self.bytes.len();
                    start + LineBreaker::last_event(rest).len()
                }
                None => return None,
            };
            if !self.bytes[start..end].trim_ascii().is_empty() {
                return Some(start..end);
            }
        }
        None
    }
}

/// How far a stream's held bytes have been searched in vain for a break. They are searched again once what came
/// since is at least 1/[`SEARCH_GROWTH`] of all that is held, so a whole message is found as it arrives, and bytes
/// trickling in cost no more than [`SEARCH_GROWTH`] times their number in searching.
#[derive(Clone, Copy, Debug, Default)]
pub struct Searched(usize);

/// What part of the held bytes must be new before they are searched again.
pub const SEARCH_GROWTH: usize = 16;

impl Searched {
    /// Whether `held` bytes, of which the first were searched in vain before, are worth searching again.
    pub fn due(self, held: usize) -> bool {
        held > self.0 && (held - self.0) * SEARCH_GROWTH >= held
    }

    /// The `held` bytes were searched, and no break found in them.
    pub fn missed(&mut self, held: usize) {
        self.0 = held;
    }

    /// A break was found: what follows it is yet to be searched.
    pub fn found(&mut self) {
        self.0 = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The events `bytes` holds, as a stream that ends with them.
    fn events_of(pattern: &str, bytes: &str) -> Vec<String> {
        let line_breaker = LineBreaker(Regex::new(pattern).unwrap());
        line_breaker
            .events(bytes.as_bytes(), true)
            .map(|event| bytes[event].to_string())
            .collect()
    }

    #[test]
    fn a_line_breaker_drops_its_group_and_keeps_what_follows_it() {
        assert_eq!(
            events_of(r"</e>(\s*)", "<e>1</e>\n<e>2</e><e>3</e>"),
            ["<e>1</e>", "<e>2</e>", "<e>3</e>"]
        );
        assert_eq!(
            events_of(
                r"([\r\n]+)\d{4}-",
                "2026-10-11 ERROR failed\n    at a.b.C.run\n2026-10-11 INFO ok"
            ),
            ["2026-10-11 ERROR failed\n    at a.b.C.run", "2026-10-11 INFO ok"]
        );
    }

    #[test]
    fn the_last_event_drops_the_line_end_that_ends_the_stream() {
        assert_eq!(events_of(r"([\r\n]+)\d", "1 a\n  b\n2 c\r\n"), ["1 a\n  b", "2 c"]);
        assert_eq!(events_of(r"([\r\n]+)\d", "1 a\n\n\n"), ["1 a\n\n"]);
    }

    #[test]
    fn an_empty_break_at_the_start_of_an_event_is_passed_over() {
        assert_eq!(events_of(r"()<e>", "<e>1<e>2"), ["<e>1", "<e>2"]);
        assert_eq!(events_of(r"(?-u:\b())", "ab cd"), ["ab cd"]);
        assert_eq!(events_of(r"(\n+)", "\n\na\nb"), ["a", "b"]);
    }

    #[test]
    fn an_event_of_only_whitespace_is_passed_over() {
        assert_eq!(events_of(r"(\x00)", "a\x00 \t\x00\x00b\x00\n"), ["a", "b"]);
    }

    #[test]
    fn a_stream_not_ended_leaves_what_follows_its_last_break() {
        let line_breaker = LineBreaker(Regex::new(r"(\x00)").unwrap());
        let mut events = line_breaker.events(b"a\x00b\x00c", false);
        assert_eq!(events.by_ref().collect::<Vec<_>>(), [0..1, 2..3]);
        assert_eq!(events.rest(), 4);
    }

    #[test]
    fn held_bytes_are_searched_again_once_a_part_of_them_is_new() {
        let mut searched = Searched::default();
        assert!(searched.due(10));
        searched.missed(10);
        assert!(!searched.due(10));
        assert!(searched.due(11));
        searched.missed(1600);
        assert!(!searched.due(1706));
        assert!(searched.due(1707));
        searched.found();
        assert!(searched.due(1));
    }

    #[test]
    fn bytes_arriving_one_at_a_time_cost_a_bounded_multiple_of_themselves_in_searching() {
        let mut searched = Searched::default();
        let mut cost = 0;
        let held = 1 << 20;
        for held in 1..=held {
            if searched.due(held) {
                cost += held;
                searched.missed(held);
            }
        }
        assert!(cost <= SEARCH_GROWTH * held, "searched {cost} bytes for {held}");
    }

    #[test]
    fn a_break_reaching_the_end_waits_for_more_bytes_or_the_end_of_the_stream() {
        let line_breaker = LineBreaker(Regex::new(r"([\r\n]+)").unwrap());
        assert_eq!(line_breaker.next_break(b"a\r", false), None);
        assert_eq!(line_breaker.next_break(b"a\r\nb", false), Some((1, 3)));
        assert_eq!(line_breaker.next_break(b"a\r", true), Some((1, 2)));
        assert_eq!(line_breaker.next_break(b"ab", true), None);
    }
}
