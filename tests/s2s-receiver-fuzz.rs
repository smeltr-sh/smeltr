//! Random forwarders against S2S receivers of random settings. A forwarder holds what it sent until the receiver acks
//! it and sends again what no ack covers, so each record must be what an indexer breaks from the bytes sent, filed
//! under the chunk its first byte came in or, for a sourcetype with a line breaker, an earlier chunk of its stream,
//! and an ack must come only once every record holding the bytes it covers is delivered.
//!
//! Each seed opens a receiver under settings an operator could save and sends it forwarders, one after another. Each
//! opens over TCP or TLS with a signature, capabilities and a token, whole or spoiled, then sends a v4 stream, plain or
//! compressed: channels defined, freed and defined again, chunks cut anywhere in their lines and events, events the
//! forwarder broke itself, heartbeats, and every kind of indexed field, ended cleanly, cut off, or spoiled by a frame
//! the receiver cannot read. A model built beside each stream breaks each channel's bytes whole, as an indexer breaks
//! them, and says which records the receiver must give. A deterministic PRNG over a seed range, from `harness::fuzz`:
//! a failure names its seed, and the fuzzer checks that its seeds reached every option it knows.

use std::collections::{HashMap, VecDeque};
use std::io::{Read, Write};
use std::net::{Ipv4Addr, Shutdown, SocketAddr, TcpStream};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::Value;
use smeltr::io::line_breaker::{self, BREAK_SLACK, LineBreaker};
use smeltr::io::{Source, SourceFields, SourceRecord};
use smeltr::plan::topology::{HecReceiverConfig, Receiver, S2sReceiverConfig, SourceMeta};

use crate::harness::docker::{Container, Hook, Scratch, share};
use crate::harness::fuzz::{self, Coverage, Rng};
use crate::harness::receiver::{self, Pki, Presents, Tls, commit};

/// One receiver's settings, as an operator could save them.
struct Settings {
    listen: SocketAddr,
    /// Where a forwarder reaches it.
    connect: SocketAddr,
    tokens: Vec<String>,
    tls: Tls,
    max_request_bytes: usize,
    /// The sourcetypes it breaks by a pattern of their own. Every other is an event per line.
    sourcetypes: Vec<Breaking>,
}

/// How the bytes a channel's sourcetype names are written, and how a receiver given its pattern breaks them.
#[derive(Clone, Copy, PartialEq, Debug)]
enum Breaking {
    /// A line an event, whatever its sourcetype.
    Lines,
    /// `<e>` elements, whatever lines they span.
    Xml,
    /// Lines starting with a date, each with the lines after it that do not.
    Dated,
}

impl Breaking {
    fn sourcetype(self) -> &'static str {
        match self {
            Breaking::Lines => "access",
            Breaking::Xml => "xml",
            Breaking::Dated => "dated",
        }
    }

    fn line_breaker(self) -> Option<LineBreaker> {
        let pattern = match self {
            Breaking::Lines => return None,
            Breaking::Xml => r"</e>(\s*)",
            Breaking::Dated => r"([\r\n]+)\d{4}-",
        };
        Some(LineBreaker(regex::bytes::Regex::new(pattern).unwrap()))
    }
}

/// The limits a receiver is given, by the bound each is below. Past the line breaker's slack, a frame or an event may
/// be longer than the slack it is held past its limit.
const RECEIVER_LIMITS: &[(usize, &str)] = &[
    (4 << 10, "limit under 4 KiB"),
    (BREAK_SLACK, "limit under the line breaker's slack"),
    (usize::MAX, "limit past the line breaker's slack"),
];

fn random_settings(rng: &mut Rng, ipv6: bool) -> Settings {
    let (listen, connect) = receiver::random_addresses(rng, ipv6);
    let port = receiver::free_port(connect);
    let tls = receiver::random_tls(rng);
    let tokens = receiver::random_tokens(rng, tls, |rng, i| format!("tok{i}-{:x}", rng.next_u64()));
    let sourcetypes = [Breaking::Xml, Breaking::Dated]
        .into_iter()
        .filter(|_| rng.chance(2))
        .collect();
    let max_request_bytes = match rng.below(4) {
        0 => 1024 + rng.below(2048),
        1 => BREAK_SLACK + rng.below(3 * BREAK_SLACK),
        _ => (1024 << rng.below(7)) + rng.below(1024),
    };
    Settings {
        listen: SocketAddr::new(listen, port),
        connect: SocketAddr::new(connect, port),
        tokens,
        tls,
        max_request_bytes,
        sourcetypes,
    }
}

/// An S2S receiver under `settings`, or a HEC receiver under them when `hec`, which a forwarder's `httpout` sends to.
fn open(settings: &Settings, pki: &Pki, hec: bool) -> Box<dyn Source> {
    let sourcetypes = (settings.sourcetypes.iter())
        .map(|b| (b.sourcetype().to_string(), b.line_breaker().unwrap()))
        .collect();
    let receiver = match hec {
        true => Receiver::Hec(HecReceiverConfig {
            acknowledging: (0..settings.tokens.len()).map(|i| format!("forwarder{i}")).collect(),
            sourcetypes,
        }),
        false => Receiver::S2s(S2sReceiverConfig { sourcetypes }),
    };
    let Settings {
        listen,
        tokens,
        tls,
        max_request_bytes,
        ..
    } = settings;
    receiver::build(receiver, *listen, *tls, tokens, "forwarder", *max_request_bytes, pki)
}

/// Bytes as a forwarder writes them: v4's varints and strings.
#[derive(Default)]
struct Out(Vec<u8>);

impl Out {
    fn varint(&mut self, mut n: u64) {
        while n >= 0x80 {
            self.0.push(n as u8 | 0x80);
            n >>= 7;
        }
        self.0.push(n as u8);
    }

    fn string(&mut self, s: &[u8]) {
        self.varint(s.len() as u64);
        self.0.extend_from_slice(s);
    }

    /// A channel's string: its length plus one, and 0 for none.
    fn plus_one(&mut self, s: Option<&[u8]>) {
        match s {
            None => self.varint(0),
            Some(s) => {
                self.varint(s.len() as u64 + 1);
                self.0.extend_from_slice(s);
            }
        }
    }
}

/// A v3 message as a forwarder writes it, in a compressed frame when `compressed`, ending in its usual trailer unless
/// `spoiled`.
fn v3(pairs: &[(&[u8], &[u8])], compressed: bool, spoiled: bool) -> Vec<u8> {
    let int = |n: usize| match compressed {
        true => (n as u32).to_le_bytes(),
        false => (n as u32).to_be_bytes(),
    };
    let string = |text: &[u8], body: &mut Vec<u8>| {
        body.extend(int(text.len() + 1));
        body.extend(text);
        body.push(0);
    };
    let mut body = int(pairs.len()).to_vec();
    for (key, value) in pairs {
        string(key, &mut body);
        string(value, &mut body);
    }
    body.extend(int(0));
    string(if spoiled { b"_rawr" } else { b"_raw" }, &mut body);
    match compressed {
        // Its size, its zlib stream's size with the NUL after it, the stream, the NUL.
        true => {
            let mut zlib = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
            zlib.write_all(&body).unwrap();
            let zlib = zlib.finish().unwrap();
            let zlen = zlib.len() as u32 + 1;
            [&(zlen + 4).to_be_bytes()[..], &zlen.to_be_bytes(), &zlib, &[0]].concat()
        }
        false => [&(body.len() as u32).to_be_bytes()[..], &body].concat(),
    }
}

/// A forwarder's signature: the cooked mode, its server name and its management port, each NUL-padded.
fn signature(server: &str) -> Vec<u8> {
    let mut signature = vec![0; 400];
    signature[..25].copy_from_slice(b"--splunk-cooked-mode-v3--");
    signature[128..128 + server.len()].copy_from_slice(server.as_bytes());
    signature[384..388].copy_from_slice(b"8089");
    signature
}

/// What one record is filed under: its channel's host, source and sourcetype, its index and time as `hec::Filed`
/// carries them, and its indexed fields.
#[derive(Clone, Debug, PartialEq)]
struct Filing {
    filed: Vec<(String, Vec<u8>)>,
    fields: Option<Value>,
}

/// A record the receiver must give, and the event ids whose bytes it holds.
#[derive(Clone, Debug)]
struct Expected {
    raw: Vec<u8>,
    filing: Arc<Filing>,
    /// What it may be filed under in its place: the chunks earlier events of its stream started in.
    earlier: Vec<Arc<Filing>>,
    ids: Vec<u64>,
}

/// Records that arrive in this order. Those from `required` on may not arrive, and those that do are the first of
/// them.
#[derive(Default)]
struct Queue {
    records: VecDeque<Expected>,
    required: usize,
}

/// A chunk's place in its channel's stream.
struct Chunked {
    at: usize,
    len: usize,
    id: Option<u64>,
    filing: Arc<Filing>,
}

/// What a channel sent since its stream last ended.
#[derive(Default)]
struct Segment {
    bytes: Vec<u8>,
    chunks: Vec<Chunked>,
}

/// How a channel's stream ends.
#[derive(Clone, Copy, PartialEq)]
enum Ended {
    /// What it holds is its last events: at `_done`, on its channel defined again, or as a connection without acks
    /// ends.
    Whole,
    /// A connection with acks ended: what no ack covers, the forwarder sends again.
    Held,
}

/// The records `segment` breaks into, as an indexer breaks the whole of it: those that must arrive, those that may,
/// and the ids of bytes no record will hold.
fn segment_records(
    segment: &Segment,
    line_breaker: Option<&LineBreaker>,
    max: usize,
    ended: Ended,
    labels: &mut Vec<String>,
) -> (Vec<Expected>, Vec<Expected>, Vec<u64>) {
    let bytes = &segment.bytes[..];
    let whole = ended == Ended::Whole;
    // Each event's start and end, and where the next starts.
    let mut events = Vec::new();
    let mut start = 0;
    loop {
        let next = match line_breaker {
            None => line_breaker::next_line(&bytes[start..]),
            Some(line_breaker) => line_breaker.next_break(&bytes[start..], whole),
        };
        let Some((end, next)) = next else { break };
        events.push((start, start + end));
        start += next;
    }
    // The bytes after the last break: the last event, when the stream ended whole. Else the stream's held tail, of
    // which the receiver took what ran past its limit as it came, a record's length at a time.
    let mut taken = bytes.len();
    if start < bytes.len() {
        match ended {
            Ended::Whole => events.push((start, start + LineBreaker::last_event(&bytes[start..]).len())),
            Ended::Held => {
                let limit = max + if line_breaker.is_some() { BREAK_SLACK } else { 0 };
                let pieces = (bytes.len() - start).saturating_sub(limit).div_ceil(max);
                taken = start + pieces * max;
                if pieces > 0 {
                    events.push((start, taken));
                }
            }
        }
    }
    let chunk_at = |at: usize| {
        (segment.chunks.iter())
            .find(|c| c.at <= at && at < c.at + c.len)
            .expect("every byte came in a chunk")
    };
    let ids = |from: usize, to: usize| -> Vec<u64> {
        (segment.chunks.iter())
            .filter(|c| c.at < to && from < c.at + c.len)
            .filter_map(|c| c.id)
            .collect()
    };
    let mut must = Vec::new();
    let mut may = Vec::new();
    let mut starts: Vec<Arc<Filing>> = Vec::new();
    for &(from, to) in &events {
        let event = &bytes[from..to];
        if event.trim_ascii().is_empty() {
            labels.push("an event of only whitespace".into());
            continue;
        }
        let first = chunk_at(from);
        let earlier = match line_breaker {
            Some(_) => starts.clone(),
            None => Vec::new(),
        };
        starts.push(first.filing.clone());
        if chunk_at(to - 1).at != first.at {
            labels.push("an event spanning chunks".into());
        }
        if event.len() > max {
            labels.push("an event past the limit, in pieces".into());
        }
        if line_breaker.is_some() && event.len() > max + BREAK_SLACK {
            labels.push("an event held past its limit and the slack".into());
        }
        let records = (event.chunks(max).enumerate()).map(|(n, piece)| {
            let at = from + n * max;
            Expected {
                raw: piece.to_vec(),
                filing: first.filing.clone(),
                earlier: earlier.clone(),
                ids: ids(at, at + piece.len()),
            }
        });
        // Without acks, or broken off, an event is delivered. A line breaker holds what it found until it searches
        // again, so what it held is lost with the connection.
        match (ended, line_breaker) {
            (Ended::Held, Some(_)) => may.extend(records),
            _ => must.extend(records),
        }
    }
    let dropped = ids(taken, bytes.len());
    if taken < bytes.len() {
        labels.push("bytes held as a connection with acks ends".into());
    }
    for (i, pair) in bytes.windows(2).enumerate() {
        if pair == b"\r\n" && segment.chunks.iter().any(|c| c.at == i + 1) {
            labels.push("a line end of \\r\\n split between chunks".into());
        }
    }
    (must, may, dropped)
}

/// A channel defined on the connection, and what the model holds of its stream.
struct Def {
    channel: u64,
    breaking: Breaking,
    /// Its sourcetype's pattern, when the receiver was given one.
    line_breaker: Option<LineBreaker>,
    /// Its source, host and sourcetype, as sent.
    named: [Option<Vec<u8>>; 3],
    names: Vec<Vec<u8>>,
    /// What the forwarder has written of its stream and not yet sent.
    pending: Vec<u8>,
    segment: Segment,
    queue: usize,
    /// The queue of the events the forwarder broke itself, which may arrive before what the segment held.
    broken_queue: usize,
}

const HOSTS: &[Option<&str>] = &[
    None,
    Some(""),
    Some("host::"),
    Some("host::web-1"),
    Some("web-2"),
    Some("host::ünï"),
];
const SOURCES: &[Option<&str>] = &[
    None,
    Some(""),
    Some("source::/var/log/app.log"),
    Some("/var/log/bare.log"),
];
const NAMES: &[&str] = &["env", "_MetaData:Index", "_path", "ünï", "team"];
const WORDS: &[&str] = &[
    "GET /a 200",
    "été ünï",
    "tab\there",
    "quote \"q\"",
    "\u{1F600} grin",
    "a,b,\"c\"",
];

/// The forwarder's fields keyed by text, which it does not file.
const BOOKKEEPING: &[&str] = &["_path", "_tcpoutTime", "_guid", "_ingLatChained", "_ingLatColor"];
const INDEX_KEY: &str = "_MetaData:Index";
/// Splunk's static dictionary entries that key a parsed timestamp, which the receiver passes over.
const TIMESTAMP_KEYS: &[u64] = &[1, 2, 3, 4, 5, 6, 7, 8, 46];

/// What one indexed field does to its event's filing.
enum Does {
    Field(String, Value),
    Index(Value),
    Subsecond(i64),
    Nothing,
}

/// The times an event may carry: seconds since the epoch, and what each is called.
const TIMES: &[(Option<u64>, &str)] = &[
    (None, "no time"),
    (Some(1_700_000_000), "a time"),
    (Some(0), "the epoch"),
    (Some(9_223_372_036_854), "a time at the edge of what microseconds hold"),
    (Some(u64::MAX), "a time past what microseconds hold"),
];

/// The stream-level spoils: frames the receiver cannot read, each closing the connection.
const SPOILS: &[&str] = &[
    "an unknown frame",
    "an event with unknown flags",
    "an event on a channel never defined",
    "an event id no later id can follow",
    "a number longer than 64 bits",
    "a frame past the limit",
    "a channel past the limit",
    "a field of an unknown type",
    "a field value of an unknown kind",
    "a field naming raw bytes past the raw",
    "a field naming a name its channel lacks",
    "a key from no dictionary entry",
    "a value from no dictionary entry",
    "an index that is not a name",
    "a subsecond that is no fraction",
];

/// How a connection's stream ends.
#[derive(Clone, Copy, PartialEq, Debug)]
enum Finish {
    /// Every channel's stream ended with `_done`, and, with acks, every id acked before the forwarder hangs up.
    Clean,
    /// The forwarder hangs up mid-stream.
    Abrupt,
    /// A frame the receiver cannot read, and more after it.
    Spoiled,
}

/// One connection's v4 stream, written beside the model of what the receiver makes of it.
struct Stream<'a> {
    rng: &'a mut Rng,
    max: usize,
    sourcetypes: &'a [Breaking],
    ack: bool,
    /// One channel holds bytes at a time, so its events may be any length.
    single: bool,
    /// An `httpout` body, which reads every id.
    httpout: bool,
    tag: String,
    out: Out,
    defs: Vec<Def>,
    /// The definition the decoder holds for each channel id.
    decoder: HashMap<u64, usize>,
    /// The definition whose bytes the receiver holds for each channel id.
    held: HashMap<u64, usize>,
    /// The channel the forwarder's own events go on.
    forwarder_channel: Option<u64>,
    queues: Vec<Queue>,
    next_id: u64,
    /// The first and last ids sent.
    ids: Option<(u64, u64)>,
    /// Ids whose bytes no record will hold, so no ack may cover them.
    dropped: Vec<u64>,
    events: usize,
    labels: Vec<String>,
}

impl<'a> Stream<'a> {
    fn new(rng: &'a mut Rng, settings: &'a Settings, ack: bool, tag: String) -> Stream<'a> {
        Stream {
            max: settings.max_request_bytes,
            sourcetypes: &settings.sourcetypes,
            ack,
            single: rng.chance(3),
            httpout: false,
            tag,
            out: Out::default(),
            defs: Vec::new(),
            decoder: HashMap::new(),
            held: HashMap::new(),
            forwarder_channel: None,
            queues: Vec::new(),
            next_id: *rng.pick(&[0, 1, 1000, 1 << 32]),
            ids: None,
            dropped: Vec::new(),
            events: 0,
            labels: Vec::new(),
            rng,
        }
    }

    fn label(&mut self, label: impl Into<String>) {
        self.labels.push(label.into());
    }

    /// A channel id no definition the decoder holds has.
    fn free_channel_id(&mut self) -> u64 {
        loop {
            let id = match self.rng.below(4) {
                0 => self.rng.below(4) as u64,
                1 => 127 + self.rng.below(3) as u64,
                2 => 16_000 + self.rng.below(1000) as u64,
                _ => self.rng.next_u64() >> self.rng.below(64),
            };
            if !self.decoder.contains_key(&id) && Some(id) != self.forwarder_channel {
                return id;
            }
        }
    }

    /// Defines a channel on `channel`, over whatever the decoder held there.
    fn define(&mut self, channel: u64) -> usize {
        let breaking = *self.rng.pick(&[Breaking::Lines, Breaking::Xml, Breaking::Dated]);
        let sourcetype = match self.rng.below(5) {
            0 => None,
            1 => Some(Vec::new()),
            2 => Some(breaking.sourcetype().as_bytes().to_vec()),
            _ => Some(format!("sourcetype::{}", breaking.sourcetype()).into_bytes()),
        };
        match &sourcetype {
            Some(s) if s.starts_with(b"sourcetype::") => self.label("a sourcetype prefixed"),
            Some(s) if !s.is_empty() => self.label("a sourcetype bare"),
            _ => self.label("no sourcetype"),
        }
        let configured = sourcetype.as_ref().is_some_and(|s| !s.is_empty()) && self.sourcetypes.contains(&breaking);
        let line_breaker = configured.then(|| breaking.line_breaker()).flatten();
        self.label(match (breaking, line_breaker.is_some()) {
            (Breaking::Lines, _) => "lines".to_string(),
            (b, true) => format!("events broken by {b:?}"),
            (b, false) => format!("{b:?} read a line an event"),
        });
        let host = self.rng.pick(HOSTS).map(|s| s.as_bytes().to_vec());
        let source = match self.rng.below(SOURCES.len() + 1) {
            n if n < SOURCES.len() => SOURCES[n].map(|s| s.as_bytes().to_vec()),
            _ => Some(format!("source::/var/log/{}-{channel}.log", self.tag).into_bytes()),
        };
        let names: Vec<Vec<u8>> = match self.rng.chance(3) {
            true => (0..1 + self.rng.below(3))
                .map(|_| self.rng.pick(NAMES).as_bytes().to_vec())
                .collect(),
            false => Vec::new(),
        };
        if !names.is_empty() {
            self.label("a channel with field names");
        }
        self.out.0.push(0xfe);
        self.out.varint(channel);
        self.out.plus_one(source.as_deref());
        self.out.plus_one(host.as_deref());
        self.out.plus_one(sourcetype.as_deref());
        self.out.plus_one(Some(b"2"));
        self.out.varint(names.len() as u64);
        for name in &names {
            self.out.string(name);
        }
        let queue = self.queues.len();
        self.queues.push(Queue::default());
        self.queues.push(Queue::default());
        self.defs.push(Def {
            channel,
            breaking,
            line_breaker,
            named: [source, host, sourcetype],
            names,
            pending: Vec::new(),
            segment: Segment::default(),
            queue,
            broken_queue: queue + 1,
        });
        let def = self.defs.len() - 1;
        self.decoder.insert(channel, def);
        def
    }

    fn free(&mut self, channel: u64) {
        self.out.0.push(0xfd);
        self.out.varint(channel);
        self.decoder.remove(&channel);
    }

    /// The next event written to `def`'s stream, `long` bytes past its usual length.
    fn written(&mut self, def: usize, long: usize) -> Vec<u8> {
        self.events += 1;
        let tag = format!("{}e{}", self.tag, self.events);
        let word = *self.rng.pick(WORDS);
        let filler = "y".repeat(long);
        let rng = &mut *self.rng;
        let mut text = match self.defs[def].breaking {
            Breaking::Lines => {
                let blank = *rng.pick(&["", "", "", "\n", "   \n", "\r\n"]);
                let end = *rng.pick(&["\n", "\n", "\r\n", "\r"]);
                format!("{blank}{tag} {word}{filler}{end}")
            }
            Breaking::Xml => {
                let inner = *rng.pick(&["", "\n  inner", "\r\n"]);
                let between = *rng.pick(&["", " ", "\n", "\r\n\t "]);
                format!("<e>{tag} {word}{inner}{filler}</e>{between}")
            }
            Breaking::Dated => {
                let continued: String = (0..rng.below(3))
                    .map(|i| format!("\n  at x.Y{i}(Y.java:{i})"))
                    .collect();
                let end = *rng.pick(&["\n", "\r\n", "\n\n"]);
                format!("2026-10-14 {tag} {word}{continued}{filler}{end}")
            }
        }
        .into_bytes();
        if long > 0 && self.rng.chance(2) {
            // A long event cut anywhere ends mid-character too.
            text.insert(text.len() / 2, 0xc3);
        }
        text
    }

    /// The bytes of `def`'s stream a chunk takes: `n` of them, written as they are needed.
    fn take(&mut self, def: usize, n: usize) -> Vec<u8> {
        while self.defs[def].pending.len() < n {
            let long = match (self.single, self.rng.below(8)) {
                (true, 0) if self.defs[def].line_breaker.is_some() => self.max + BREAK_SLACK + self.rng.below(self.max),
                (true, 1) => self.rng.below(3 * self.max),
                _ => 0,
            };
            let text = self.written(def, long);
            self.defs[def].pending.extend(text);
        }
        self.defs[def].pending.drain(..n).collect()
    }

    /// An id for the next event, which a connection with acks gives every one.
    fn id(&mut self) -> Option<u64> {
        if !self.ack && self.rng.chance(3) {
            self.label("an event with no id");
            return None;
        }
        let id = self.next_id;
        self.next_id += 1 + if self.rng.chance(5) {
            self.rng.below(3) as u64
        } else {
            0
        };
        self.ids = Some(self.ids.map_or((id, id), |(first, _)| (first, id)));
        Some(id)
    }

    /// One valid indexed field of an event of `raw` on `def`, as sent, and what it does.
    fn field(&mut self, def: usize, raw: &[u8]) -> (Vec<u8>, Does) {
        let mut out = Out::default();
        // The key: a dictionary entry, text, or one of the channel's names.
        let names = self.defs[def].names.len();
        let key = match self.rng.below(if names > 0 { 7 } else { 6 }) {
            0 => {
                let entry = loop {
                    let entry = 1 + self.rng.below(69) as u64;
                    if !TIMESTAMP_KEYS.contains(&entry) {
                        break entry;
                    }
                };
                self.label("a key from the dictionary");
                Err(entry)
            }
            1 => {
                self.label("a timestamp key, passed over");
                Err(*self.rng.pick(TIMESTAMP_KEYS))
            }
            2 => {
                // `_subsecond`: an integer holding the fraction's digits, a one, and a zero for each leading zero.
                let digits = 1 + self.rng.below(6);
                let fraction: String = (0..digits)
                    .map(|_| char::from(b'0' + self.rng.below(10) as u8))
                    .collect();
                let zeros = fraction.bytes().take_while(|&b| b == b'0').count();
                let value: u64 = fraction.parse().unwrap();
                out.0.push(0x40);
                out.varint(0);
                out.varint(((value << 1) | 1) << zeros);
                self.label("a subsecond");
                return (out.0, Does::Subsecond(value as i64 * 10_i64.pow(6 - digits as u32)));
            }
            3 => {
                self.label("an index field");
                Ok(INDEX_KEY.as_bytes().to_vec())
            }
            4 => {
                self.label("a bookkeeping field, passed over");
                Ok(self.rng.pick(BOOKKEEPING).as_bytes().to_vec())
            }
            6 => {
                let n = self.rng.below(names);
                self.label("a key naming its channel's field name");
                let textual = self.defs[def].names[n] == INDEX_KEY.as_bytes();
                let (kind, value, read) = self.value(raw, textual);
                out.0.push(0x01 | kind << 2);
                out.varint(n as u64);
                out.0.extend(value.0);
                return (out.0, keyed(&self.defs[def].names[n], read));
            }
            _ => {
                self.label("a key of text");
                Ok(self.rng.pick(NAMES).as_bytes().to_vec())
            }
        };
        let textual = key.as_ref().is_ok_and(|key| key == INDEX_KEY.as_bytes());
        let (kind, value, does) = self.value(raw, textual);
        match key {
            Err(entry) => {
                out.0.push(0x40 | *self.rng.pick(&[0, 1, 2, 3]) | kind << 2);
                out.varint(entry);
                out.0.extend(value.0);
                let does = match TIMESTAMP_KEYS.contains(&entry) {
                    true => Does::Nothing,
                    false => keyed(smeltr::io::s2s::dictionary(entry).unwrap().as_bytes(), does),
                };
                (out.0, does)
            }
            Ok(key) => {
                out.0.push(*self.rng.pick(&[0x00, 0x02, 0x03]) | kind << 2);
                out.string(&key);
                out.0.extend(value.0);
                (out.0, keyed(&key, does))
            }
        }
    }

    /// A valid field value: its kind, its bytes, and what it reads as. Only text when `textual`, as an index is.
    fn value(&mut self, raw: &[u8], textual: bool) -> (u8, Out, Value) {
        let mut out = Out::default();
        let text = |bytes: &[u8]| Value::String(String::from_utf8_lossy(bytes).into_owned());
        let kind = match textual {
            true => *self.rng.pick(&[1, 2, 3, 4, 5]),
            false => self.rng.below(6),
        };
        let (kind, value) = match kind {
            0 => {
                let n = *self.rng.pick(&[0, 1, 127, 128, u64::MAX]);
                out.varint(n);
                self.label("a value of an integer");
                // A record's fields hold an integer past what a signed one holds as a float.
                let value = match i64::try_from(n) {
                    Ok(n) => Value::from(n),
                    Err(_) => Value::from(n as f64),
                };
                (0, value)
            }
            1 => {
                let s = *self.rng.pick(&["lab", "", "été", "a\"b", "default"]);
                out.string(s.as_bytes());
                self.label("a value of text");
                (1, text(s.as_bytes()))
            }
            2 => {
                let entry = match self.rng.below(3) {
                    0 if !textual => 60 + self.rng.below(3) as u64,
                    _ => 9 + self.rng.below(51) as u64,
                };
                out.varint(entry);
                self.label(match entry {
                    60 => "a value of null",
                    61 => "a value of false",
                    62 => "a value of true",
                    _ => "a value of a dictionary word",
                });
                let value = match entry {
                    60 => Value::Null,
                    61 => false.into(),
                    62 => true.into(),
                    _ => smeltr::io::s2s::dictionary(entry).unwrap().into(),
                };
                (2, value)
            }
            n => {
                let start = self.rng.below(raw.len() + 1);
                let len = self.rng.below(raw.len() - start + 1).min(64);
                let slice = &raw[start..start + len];
                out.varint(start as u64);
                out.varint(len as u64);
                let (kind, value) = match n {
                    3 => {
                        self.label("a value of raw bytes");
                        (3, text(slice))
                    }
                    4 => {
                        self.label("a value of raw bytes escaped as CSV");
                        (4, String::from_utf8_lossy(slice).replace("\"\"", "\"").into())
                    }
                    _ => {
                        self.label("a value of raw bytes escaped as JSON");
                        let quoted = [&b"\""[..], slice, b"\""].concat();
                        let value = serde_json::from_slice::<String>(&quoted)
                            .map(Value::String)
                            .unwrap_or_else(|_| text(slice));
                        (6, value)
                    }
                };
                (kind, value)
            }
        };
        (kind, out, value)
    }
}

/// What a value under a key of `key`'s text does: the index, nothing for the forwarder's bookkeeping, else a field.
fn keyed(key: &[u8], value: Value) -> Does {
    match key {
        k if k == INDEX_KEY.as_bytes() => Does::Index(value),
        k if BOOKKEEPING.iter().any(|b| b.as_bytes() == k) => Does::Nothing,
        k => Does::Field(String::from_utf8_lossy(k).into_owned(), value),
    }
}

/// What a frame's header and fields may take besides its raw.
const FRAME_HEAD: usize = 80;
const FIELDS_ROOM: usize = 400;

/// The decoder's entry for a channel defined for no stream of the model: the forwarder's own, or one past the limit.
const NO_DEF: usize = usize::MAX;

impl Stream<'_> {
    /// What an event on `def` of `time`, whose fields do `does`, is filed under.
    fn filing(&mut self, def: usize, time: Option<u64>, does: &[Does]) -> Filing {
        let mut filed = Vec::new();
        let [source, host, sourcetype] = &self.defs[def].named;
        for (key, value, prefix) in [
            ("host", host, &b"host::"[..]),
            ("source", source, b"source::"),
            ("sourcetype", sourcetype, b"sourcetype::"),
        ] {
            if let Some(value) = value {
                let value = value.strip_prefix(prefix).unwrap_or(value);
                if !value.is_empty() {
                    filed.push((key.to_string(), value.to_vec()));
                }
            }
        }
        let mut index = None;
        let mut subsecond = 0;
        let mut fields = serde_json::Map::new();
        let mut repeated = false;
        for does in does {
            match does {
                Does::Index(value) => index = Some(value.clone()),
                Does::Subsecond(micros) => subsecond = *micros,
                Does::Field(key, value) => match fields.get_mut(key) {
                    None => {
                        fields.insert(key.clone(), value.clone());
                    }
                    Some(Value::Array(items)) => {
                        repeated = true;
                        items.push(value.clone());
                    }
                    Some(one) => {
                        repeated = true;
                        *one = Value::Array(vec![one.take(), value.clone()]);
                    }
                },
                Does::Nothing => {}
            }
        }
        if repeated {
            self.label("a key repeated");
        }
        match index {
            Some(Value::String(index)) if index != "default" => filed.push(("index".into(), index.into_bytes())),
            Some(Value::String(_)) => self.label("the default index"),
            _ => {}
        }
        let micros = time.and_then(|seconds| {
            i64::try_from(seconds)
                .ok()?
                .checked_mul(1_000_000)?
                .checked_add(subsecond)
        });
        if let Some(micros) = micros {
            filed.push(("time".into(), micros.to_string().into_bytes()));
        }
        // A null under a key is no entry of a record's fields.
        fields.retain(|_, value| !value.is_null());
        Filing {
            filed,
            fields: (!fields.is_empty()).then_some(Value::Object(fields)),
        }
    }

    /// The receiver takes an event on `def`'s channel: a stream it held there under another definition ends.
    fn take_over(&mut self, def: usize) {
        let channel = self.defs[def].channel;
        if let Some(held) = self.held.insert(channel, def)
            && held != def
            && !self.defs[held].segment.bytes.is_empty()
        {
            self.label("a stream ended by its channel defined again");
            self.end_segment(held, Ended::Whole);
        }
    }

    fn end_segment(&mut self, def: usize, ended: Ended) {
        let segment = std::mem::take(&mut self.defs[def].segment);
        if segment.bytes.is_empty() {
            return;
        }
        let (must, may, dropped) = segment_records(
            &segment,
            self.defs[def].line_breaker.as_ref(),
            self.max,
            ended,
            &mut self.labels,
        );
        let queue = &mut self.queues[self.defs[def].queue];
        queue.records.extend(must);
        queue.required = queue.records.len();
        queue.records.extend(may);
        self.dropped.extend(dropped);
    }

    /// Whether the receiver may hold bytes of any stream.
    fn holding(&self) -> bool {
        self.defs.iter().any(|def| !def.segment.bytes.is_empty())
    }

    /// Writes an event of `raw` on `def`'s channel: its flags and what they name, and its fields. Answers its id and
    /// what it is filed under.
    fn event(&mut self, def: usize, raw: &[u8], broken: bool, done: bool) -> (Option<u64>, Arc<Filing>) {
        let id = self.id();
        let (time, time_label) = *self.rng.pick(TIMES);
        self.label(time_label);
        let room = FIELDS_ROOM.min(self.max / 4);
        let (mut fields, mut does, mut size) = (Vec::new(), Vec::new(), 0);
        if self.rng.chance(2) {
            for _ in 0..1 + self.rng.below(5) {
                let (bytes, field) = self.field(def, raw);
                if size + bytes.len() > room {
                    break;
                }
                size += bytes.len();
                fields.push(bytes);
                does.push(field);
            }
        }
        let stream = self.rng.chance(4).then(|| {
            self.label("an event with its place in its source");
            (
                self.rng.below(1000) as u64,
                self.rng.next_u64() >> 20,
                self.rng.below(10) as u64,
            )
        });
        let mut flags = 0;
        if !fields.is_empty() || self.rng.chance(8) {
            flags |= 1;
        }
        if stream.is_some() {
            flags |= 1 << 1;
        }
        if id.is_some() {
            flags |= 1 << 2;
        }
        if time.is_some() {
            flags |= 1 << 3;
        }
        if broken {
            flags |= 1 << 6;
        }
        if done {
            flags |= 1 << 9;
        }
        for bit in [4, 5, 10, 11, 14] {
            if self.rng.chance(4) {
                self.label("a flag of the forwarder's bookkeeping");
                flags |= 1 << bit;
            }
        }
        self.out.0.push(0xfc);
        self.out.varint(self.defs[def].channel);
        self.out.varint(flags);
        if let Some((stream, offset, index)) = stream {
            self.out.varint(stream);
            self.out.varint(offset);
            self.out.varint(index);
        }
        for n in id.into_iter().chain(time) {
            self.out.varint(n);
        }
        if flags & 1 != 0 {
            self.out.varint(fields.len() as u64);
            for field in fields {
                self.out.0.extend(field);
            }
        }
        self.out.string(raw);
        let filing = self.filing(def, time, &does);
        (id, Arc::new(filing))
    }

    /// A chunk of `def`'s stream, which ends it when `done`.
    fn chunk(&mut self, def: usize, done: bool) {
        self.take_over(def);
        let room = self.max - FRAME_HEAD - FIELDS_ROOM.min(self.max / 4);
        let most = if self.single {
            room.min(64 << 10)
        } else {
            room.min(1500)
        };
        let n = match self.rng.below(8) {
            0 => {
                self.label("an empty chunk");
                0
            }
            1 => 1 + self.rng.below(8),
            _ => 1 + self.rng.below(most),
        };
        let raw = self.take(def, n);
        let (id, filing) = self.event(def, &raw, false, done);
        let segment = &mut self.defs[def].segment;
        segment.chunks.push(Chunked {
            at: segment.bytes.len(),
            len: raw.len(),
            id,
            filing,
        });
        segment.bytes.extend(raw);
        if done {
            self.label("a chunk ending its stream");
            self.end_segment(def, Ended::Whole);
        }
    }

    /// An event the forwarder broke itself on `def`, which ends `def`'s stream when `done`.
    fn broken(&mut self, def: usize, done: bool) {
        self.take_over(def);
        let raw = match self.rng.chance(6) {
            true => {
                self.label("a broken event of only whitespace");
                b" \r\n\t".to_vec()
            }
            false => {
                self.label("an event the forwarder broke");
                self.written(def, 0)
            }
        };
        let (id, filing) = self.event(def, &raw, true, done);
        if !raw.trim_ascii().is_empty() {
            let queue = &mut self.queues[self.defs[def].broken_queue];
            queue.records.push_back(Expected {
                raw,
                filing,
                earlier: Vec::new(),
                ids: id.into_iter().collect(),
            });
            queue.required = queue.records.len();
        }
        if done {
            self.label("a broken event ending its stream");
            self.end_segment(def, Ended::Whole);
        }
    }

    /// The forwarder's own heartbeat or ForwarderInfo, on a channel naming nothing, which the receiver passes over.
    fn forwarder_event(&mut self) {
        let channel = match self.forwarder_channel {
            Some(channel) => channel,
            None => {
                let channel = self.free_channel_id();
                self.out.0.extend([0xfe]);
                self.out.varint(channel);
                self.out.0.extend([0, 0, 0, 0, 0]);
                self.decoder.insert(channel, NO_DEF);
                self.forwarder_channel = Some(channel);
                channel
            }
        };
        let (sourcetype, raw) = match self.rng.chance(2) {
            true => {
                self.label("a heartbeat");
                ("sourcetype::fwd-hb", "HB")
            }
            false => {
                self.label("a ForwarderInfo event");
                (
                    "sourcetype::fwdinfo",
                    "ForwarderInfo build=0 version=10.6.0.5 hostname=uf",
                )
            }
        };
        self.out.0.push(0xfc);
        self.out.varint(channel);
        self.out.varint(1 | 1 << 2 | 1 << 3);
        self.out.varint(0);
        self.out.varint(1_700_000_000);
        self.out.varint(1);
        self.out.0.push(0x04);
        self.out.string(b"MetaData:Sourcetype");
        self.out.string(sourcetype.as_bytes());
        self.out.string(raw.as_bytes());
    }

    /// The definitions the decoder holds, in the order they were made.
    fn live(&self) -> Vec<usize> {
        (0..self.defs.len())
            .filter(|&def| self.decoder.get(&self.defs[def].channel) == Some(&def))
            .collect()
    }

    /// Whether the receiver holds no bytes on `channel`. Only then may a connection with acks free it or define it
    /// again: a forwarder frees a channel once its file is done.
    fn idle(&self, channel: u64) -> bool {
        self.held
            .get(&channel)
            .is_none_or(|&def| self.defs[def].segment.bytes.is_empty())
    }

    /// The channel id a new definition takes: one of its own, or the one channel of a stream that holds one.
    fn new_channel(&mut self) -> u64 {
        match (self.single, self.defs.first()) {
            (true, Some(def)) => def.channel,
            _ => self.free_channel_id(),
        }
    }

    /// Writes the stream: a timezone, channels, then `steps` events and frames, then its end.
    fn write(&mut self, steps: usize, finish: Finish) {
        if self.rng.chance(2) {
            self.label("a timezone frame");
            self.out.0.push(0xff);
            self.out.string(self.rng.pick(&[&b"N###"[..], b"", b"UTC"]));
        }
        for _ in 0..if self.single { 1 } else { 1 + self.rng.below(3) } {
            let channel = self.new_channel();
            self.define(channel);
        }
        for _ in 0..steps {
            let live = self.live();
            let Some(&def) = live.get(self.rng.below(live.len().max(1))) else {
                let channel = self.new_channel();
                self.define(channel);
                continue;
            };
            let channel = self.defs[def].channel;
            match self.rng.below(24) {
                0 if !self.ack || self.idle(channel) => {
                    self.label(match self.idle(channel) {
                        true => "a channel freed",
                        false => "a channel freed with bytes held",
                    });
                    self.free(channel);
                    if self.rng.chance(2) {
                        self.label("a channel freed and defined again");
                        self.define(channel);
                    }
                }
                1 if !self.ack || self.idle(channel) => {
                    self.label("a channel defined over");
                    self.define(channel);
                }
                2 if !self.single && live.len() < 4 && self.defs.len() < 8 => {
                    let channel = self.free_channel_id();
                    self.define(channel);
                }
                3 | 4 => {
                    let done = self.rng.chance(6);
                    self.broken(def, done);
                }
                5 if !self.ack || !self.holding() => self.forwarder_event(),
                _ => {
                    let done = self.rng.chance(10);
                    self.chunk(def, done);
                }
            }
        }
        match finish {
            Finish::Clean => {
                for def in self.live() {
                    if !self.idle(self.defs[def].channel) || self.rng.chance(4) {
                        self.chunk(def, true);
                    }
                }
            }
            Finish::Abrupt => {}
            Finish::Spoiled => self.spoil(),
        }
        let ended = if self.ack { Ended::Held } else { Ended::Whole };
        for def in 0..self.defs.len() {
            self.end_segment(def, ended);
        }
    }

    /// A frame the receiver cannot read, then frames it must not read.
    fn spoil(&mut self) {
        let spoil = loop {
            let spoil = *self.rng.pick(SPOILS);
            if !(self.httpout && spoil == "an event id no later id can follow") {
                break spoil;
            }
        };
        self.label(spoil);
        let def = match self.live().first() {
            Some(&def) => def,
            None => {
                let channel = self.new_channel();
                self.define(channel)
            }
        };
        let channel = self.defs[def].channel;
        let mut field = Out::default();
        match spoil {
            "an unknown frame" => self.out.0.push(*self.rng.pick(&[0x00, 0x41, 0xf8, 0xf9, 0xfa, 0xfb])),
            "an event with unknown flags" => {
                self.out.0.push(0xfc);
                self.out.varint(channel);
                self.out.varint(1 << (7 + self.rng.below(2)) | 1 << 12);
                self.out.string(b"flagged\n");
            }
            "an event on a channel never defined" => {
                let never = self.free_channel_id();
                self.out.0.push(0xfc);
                self.out.varint(never);
                self.out.varint(0);
                self.out.string(b"nowhere\n");
            }
            "an event id no later id can follow" => {
                self.out.0.push(0xfc);
                self.out.varint(channel);
                self.out.varint(1 << 2);
                self.out.varint(u64::MAX);
                self.out.string(b"last\n");
            }
            "a number longer than 64 bits" => {
                self.out.0.push(0xfc);
                self.out.0.extend([0xff; 10]);
                self.out.0.push(0x01);
            }
            "a frame past the limit" => {
                self.out.0.push(0xfc);
                self.out.varint(channel);
                self.out.varint(0);
                self.out.varint(self.max as u64 + 1);
                self.out.0.extend(vec![b'x'; 64]);
            }
            "a channel past the limit" => {
                while self.decoder.len() <= smeltr::io::s2s::CHANNEL_LIMIT {
                    let channel = self.free_channel_id();
                    self.out.0.push(0xfe);
                    self.out.varint(channel);
                    self.out.0.extend([0, 0, 0, 0, 0]);
                    self.decoder.insert(channel, NO_DEF);
                }
            }
            "a field of an unknown type" => {
                field.0.push(0x20 | *self.rng.pick(&[0x00, 0x80]));
                field.string(b"k");
                field.varint(1);
            }
            "a field value of an unknown kind" => {
                field.0.push(*self.rng.pick(&[5, 7]) << 2);
                field.string(b"k");
                field.varint(1);
            }
            "a field naming raw bytes past the raw" => {
                field.0.push(3 << 2);
                field.string(b"k");
                field.varint(4);
                field.varint(1);
            }
            "a field naming a name its channel lacks" => {
                field.0.push(0x01);
                field.varint(self.defs[def].names.len() as u64 + self.rng.below(3) as u64);
                field.varint(1);
            }
            "a key from no dictionary entry" => {
                field.0.push(0x40);
                field.varint(70 + self.rng.below(1000) as u64);
                field.varint(1);
            }
            "a value from no dictionary entry" => {
                field.0.push(2 << 2);
                field.string(b"k");
                field.varint(70 + self.rng.below(1000) as u64);
            }
            "an index that is not a name" => match self.rng.chance(2) {
                true => {
                    field.0.push(0);
                    field.string(INDEX_KEY.as_bytes());
                    field.varint(5);
                }
                false => {
                    field.0.push(2 << 2);
                    field.string(INDEX_KEY.as_bytes());
                    field.varint(60 + self.rng.below(3) as u64);
                }
            },
            "a subsecond that is no fraction" => {
                field.0.push(0x40);
                field.varint(0);
                field.varint(*self.rng.pick(&[0, ((1_234_567 << 1) | 1)]));
            }
            other => unreachable!("{other}"),
        }
        if !field.0.is_empty() {
            self.out.0.push(0xfc);
            self.out.varint(channel);
            self.out.varint(1 | 1 << 2);
            self.out.varint(self.next_id);
            self.out.varint(1);
            self.out.0.extend(field.0);
            self.out.string(b"raw");
        }
        // What follows a frame the receiver cannot read is never read.
        self.out.0.push(0xfc);
        self.out.varint(channel);
        self.out.varint(1 << 9);
        self.out.string(b"after the spoil\n");
    }
}

/// How far a connection got, for crediting what it sent.
#[derive(Clone, Copy, PartialEq, PartialOrd, Debug)]
enum Stage {
    Tls,
    Signature,
    Capabilities,
    Info,
    Stream,
}

/// One forwarder's connection: what it sends, and what the receiver must make of it.
struct Plan {
    presents: Presents,
    /// The signature and capabilities.
    opening: Vec<u8>,
    /// Whether the receiver answers the capabilities.
    answered: bool,
    /// ForwarderInfo and the stream.
    rest: Vec<u8>,
    /// Whether the receiver reads the v4 stream, and so acks it when asked.
    streamed: bool,
    ack: bool,
    finish: Finish,
    /// The first id sent, and the last acked before the forwarder hangs up when it waits for its acks.
    ids: Option<(u64, u64)>,
    must_ack: bool,
    queues: Vec<Queue>,
    dropped: Vec<u64>,
    reached: Stage,
    labels: Vec<(Stage, String)>,
}

const TOKENS: &[&str] = &[
    "the token",
    "the token padded",
    "a token the receiver does not take",
    "no token",
];

fn random_connection(rng: &mut Rng, settings: &Settings, n: usize) -> Plan {
    let presents = match settings.tls {
        Tls::None => Presents::Nothing,
        _ => *rng.pick(&[
            Presents::Nothing,
            Presents::Trusted,
            Presents::Trusted,
            Presents::Stranger,
        ]),
    };
    let mut plan = Plan {
        presents,
        opening: Vec::new(),
        answered: false,
        rest: Vec::new(),
        streamed: false,
        ack: false,
        finish: Finish::Abrupt,
        ids: None,
        must_ack: false,
        queues: Vec::new(),
        dropped: Vec::new(),
        reached: Stage::Tls,
        labels: vec![(Stage::Tls, format!("presents {presents:?} under {:?}", settings.tls))],
    };
    let label = |plan: &mut Plan, label: &str| plan.labels.push((plan.reached, label.to_string()));
    if settings.tls == Tls::Vouched && presents != Presents::Trusted {
        return plan;
    }

    plan.reached = Stage::Signature;
    let server = format!("uf-{n}");
    match rng.below(14) {
        0 => {
            label(&mut plan, "an HTTP request on its port");
            plan.opening =
                b"POST /services/collector HTTP/1.1\r\nHost: receiver\r\nContent-Length: 2\r\n\r\n{}".to_vec();
            return plan;
        }
        1 => {
            label(&mut plan, "a signature that strays from the cooked mode");
            plan.opening = signature(&server);
            plan.opening[rng.below(25)] ^= 0x20;
            return plan;
        }
        2 => {
            label(&mut plan, "a connection that ends inside its signature");
            plan.opening = signature(&server)[..rng.below(400)].to_vec();
            return plan;
        }
        _ => plan.opening = signature(&server),
    }

    plan.reached = Stage::Capabilities;
    let ack = rng.chance(2);
    let compression = rng.chance(2);
    label(&mut plan, &format!("ack={}", ack as u8));
    label(&mut plan, &format!("compression={}", compression as u8));
    let mut asks = vec![
        format!("ack={}", ack as u8),
        format!("compression={}", compression as u8),
    ];
    for other in ["v4=1", "channel_limit=300", "pl=6", "", "cap_flush_key=1"] {
        if rng.chance(3) {
            label(&mut plan, "capabilities the receiver passes over");
            asks.insert(rng.below(asks.len() + 1), other.to_string());
        }
    }
    let mut capabilities = asks.join(";");
    // A forwarder frames its capabilities as it asks; the receiver reads either.
    let framed = match rng.chance(8) {
        true => {
            label(&mut plan, "capabilities framed against what they ask");
            !compression
        }
        false => compression,
    };
    let mut key = &b"__s2s_capabilities"[..];
    let mut pad = None;
    let mut trailer_spoiled = false;
    let spoiled = match rng.below(16) {
        0 => {
            key = b"__s2s_hello";
            Some("capabilities without __s2s_capabilities")
        }
        1 => {
            capabilities += rng.pick(&[";ack=yes", ";compression=", ";broken"]);
            Some("an unreadable capability")
        }
        2 => {
            pad = Some(vec![b'x'; settings.max_request_bytes]);
            Some("capabilities past the limit")
        }
        3 => {
            trailer_spoiled = true;
            Some("a v3 message that does not end in its usual trailer")
        }
        _ => None,
    };
    let mut pairs = vec![(key, capabilities.as_bytes())];
    if let Some(pad) = &pad {
        pairs.push((b"pad", pad));
    }
    plan.opening.extend(v3(&pairs, framed, trailer_spoiled));
    if let Some(spoiled) = spoiled {
        label(&mut plan, spoiled);
        return plan;
    }
    plan.answered = true;
    plan.ack = ack;

    plan.reached = Stage::Info;
    let token = match settings.tokens.is_empty() {
        true => *rng.pick(&[TOKENS[2], TOKENS[3]]),
        false => *rng.pick(TOKENS),
    };
    let presented = match token {
        "the token" => Some(rng.pick(&settings.tokens).clone()),
        "the token padded" => Some(format!(" {}\t", rng.pick(&settings.tokens))),
        "a token the receiver does not take" => Some(format!("tok-{:x}", rng.next_u64())),
        _ => None,
    };
    label(
        &mut plan,
        &match settings.tokens.is_empty() {
            true => format!("{token} to a receiver that holds none"),
            false => token.to_string(),
        },
    );
    let admitted = settings.tokens.is_empty() || token.starts_with("the token");
    let mut info: Vec<(&[u8], &[u8])> = vec![
        (b"_raw", b"ForwarderInfo build=0 version=10.6.0.5 hostname=uf"),
        (b"_MetaData:Index", b"_internal"),
        (b"MetaData:Sourcetype", b"sourcetype::fwdinfo"),
    ];
    if let Some(presented) = &presented {
        info.push((b"__s2s_token", presented.as_bytes()));
    }
    info.push((b"__s2s_eventId", b"0"));
    // A forwarder that asked for compression frames ForwarderInfo compressed over TCP, and plain over TLS; the
    // receiver reads either.
    let info_framed = match rng.chance(8) {
        true => {
            label(&mut plan, "ForwarderInfo framed against what was asked");
            !compression
        }
        false if compression && settings.tls != Tls::None => {
            label(&mut plan, "compression asked over TLS, ForwarderInfo plain");
            false
        }
        false => compression,
    };
    plan.rest.extend(v3(&info, info_framed, false));
    if !admitted {
        return plan;
    }

    plan.reached = Stage::Stream;
    if rng.chance(16) {
        label(&mut plan, "a v3 message after ForwarderInfo");
        plan.rest.extend(v3(&[(b"_raw", b"v3 event")], compression, false));
        return plan;
    }
    if compression && rng.chance(16) {
        label(&mut plan, "a compressed stream that does not inflate");
        plan.rest.extend([0xf9, 0xff, 0xff, 0x00, 0x01, 0x02, 0x03]);
        return plan;
    }
    plan.streamed = true;
    plan.finish = *rng.pick(&[
        Finish::Clean,
        Finish::Clean,
        Finish::Abrupt,
        Finish::Spoiled,
        Finish::Spoiled,
    ]);
    let finish = format!("{:?} ending with acks {ack}", plan.finish);
    label(&mut plan, &finish);
    let mut stream = Stream::new(rng, settings, ack, format!("c{n}-"));
    let steps = 1 + stream.rng.below(40);
    stream.write(steps, plan.finish);
    let Stream {
        out,
        queues,
        ids,
        dropped,
        labels,
        single,
        rng,
        ..
    } = stream;
    if single {
        label(&mut plan, "one stream at a time");
    }
    plan.labels.extend(labels.into_iter().map(|l| (Stage::Stream, l)));
    plan.queues = queues;
    plan.ids = ids;
    plan.dropped = dropped;
    plan.must_ack = ack && plan.finish == Finish::Clean && ids.is_some();
    match compression {
        true => plan.rest.extend(compressed(rng, &out.0)),
        false => plan.rest.extend(out.0),
    }
    plan
}

/// `v4` as a forwarder compresses it: `0xf9` and one zlib stream, flushed wherever the forwarder flushes it and never
/// finished.
fn compressed(rng: &mut Rng, v4: &[u8]) -> Vec<u8> {
    let mut zlib = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::fast());
    let mut sent = 0;
    while sent < v4.len() {
        let n = (1 + rng.below(8192)).min(v4.len() - sent);
        zlib.write_all(&v4[sent..sent + n]).unwrap();
        if rng.chance(3) {
            zlib.flush().unwrap();
        }
        sent += n;
    }
    zlib.flush().unwrap();
    [&[0xf9][..], zlib.get_ref()].concat()
}

/// A forwarder's side of its connection, over TCP or TLS.
enum Wire {
    Plain(TcpStream),
    Tls(Box<rustls::StreamOwned<rustls::ClientConnection, TcpStream>>),
}

impl Read for Wire {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Wire::Plain(tcp) => tcp.read(buf),
            Wire::Tls(tls) => tls.read(buf),
        }
    }
}

impl Write for Wire {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Wire::Plain(tcp) => tcp.write(buf),
            Wire::Tls(tls) => tls.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Wire::Plain(tcp) => tcp.flush(),
            Wire::Tls(tls) => tls.flush(),
        }
    }
}

impl Wire {
    /// Hangs up its side, as a forwarder done sending does, and reads on.
    fn close_write(&mut self) {
        match self {
            Wire::Plain(tcp) => {
                let _ = tcp.shutdown(Shutdown::Write);
            }
            Wire::Tls(tls) => {
                tls.conn.send_close_notify();
                let _ = tls.flush();
                let _ = tls.sock.shutdown(Shutdown::Write);
            }
        }
    }
}

/// How many records holding each id's bytes are not yet delivered. An id whose bytes no record will hold counts one
/// forever.
type Undelivered = Mutex<HashMap<u64, usize>>;

/// The acks a forwarder heard.
#[derive(Default)]
struct Heard {
    /// The id the next ack must start at.
    next: Option<u64>,
    through: Option<u64>,
    error: Option<String>,
}

impl Heard {
    /// Takes ack `first..=last`, which must follow the last and cover only ids whose records are all delivered.
    fn take(&mut self, (first, last): (u64, u64), undelivered: &Undelivered) {
        let error = if self.next.is_some_and(|next| next != first) || first > last {
            Some(format!("acked {first}..={last}, after {:?}", self.through))
        } else {
            let undelivered = undelivered.lock().unwrap();
            (first..=last)
                .find(|id| undelivered.get(id).is_some_and(|n| *n > 0))
                .map(|id| format!("acked {first}..={last}, though a record holding {id}'s bytes was not delivered"))
        };
        if let Some(error) = error {
            self.error.get_or_insert(error);
        }
        self.next = Some(last + 1);
        self.through = Some(last);
    }
}

fn read_varint(wire: &mut impl Read) -> Result<u64, String> {
    let (mut n, mut shift) = (0u64, 0);
    loop {
        let mut byte = [0];
        wire.read_exact(&mut byte)
            .map_err(|e| format!("an ack cut short: {e}"))?;
        n |= u64::from(byte[0] & 0x7f) << shift;
        if byte[0] < 0x80 {
            return Ok(n);
        }
        shift += 7;
    }
}

/// The next ack the receiver sends, or `None` once it closed the connection.
fn read_ack(wire: &mut impl Read) -> Result<Option<(u64, u64)>, String> {
    let mut op = [0];
    match wire.read(&mut op) {
        Ok(0) => Ok(None),
        Ok(_) => match op[0] {
            0xfb => read_varint(wire).map(|id| Some((id, id))),
            0xfa => Ok(Some((read_varint(wire)?, read_varint(wire)?))),
            other => Err(format!("sent {other:#04x}, which starts no ack")),
        },
        Err(e) if matches!(e.kind(), std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut) => {
            Err("neither acked nor closed the connection".into())
        }
        // A reset closes it too.
        Err(_) => Ok(None),
    }
}

/// Plays `plan`'s forwarder against the receiver at `at`, until the receiver closes the connection.
fn forward(
    at: SocketAddr,
    tls: Tls,
    pki: &Pki,
    plan: &Plan,
    undelivered: &Arc<Undelivered>,
    seed: u64,
) -> Result<(), String> {
    let mut rng = Rng::seeded(seed);
    let tcp = TcpStream::connect(at).map_err(|e| format!("connecting: {e}"))?;
    tcp.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
    tcp.set_write_timeout(Some(Duration::from_secs(30))).unwrap();
    let mut wire = match tls {
        Tls::None => Wire::Plain(tcp),
        _ => {
            let connection =
                rustls::ClientConnection::new(pki.client(plan.presents), "localhost".try_into().unwrap()).unwrap();
            Wire::Tls(Box::new(rustls::StreamOwned::new(connection, tcp)))
        }
    };
    let _ = wire.write_all(&plan.opening).and_then(|()| wire.flush());
    if plan.answered {
        let mut size = [0; 4];
        wire.read_exact(&mut size)
            .map_err(|e| format!("no answer to its capabilities: {e}"))?;
        let mut answer = vec![0; u32::from_be_bytes(size) as usize];
        wire.read_exact(&mut answer)
            .map_err(|e| format!("an answer to its capabilities cut short: {e}"))?;
        if !String::from_utf8_lossy(&answer).contains("cap_response=success") {
            return Err(format!("answered {:?}", String::from_utf8_lossy(&answer)));
        }
    }
    let heard = Arc::new(Mutex::new(Heard {
        next: plan.ids.map(|(first, _)| first),
        ..Heard::default()
    }));
    let acks = plan.streamed && plan.ack;
    // Over TCP, acks are read as they come, so one sent before its records were delivered is read while they are not.
    let reader = match &wire {
        Wire::Plain(tcp) if acks => {
            let mut tcp = tcp.try_clone().unwrap();
            let (heard, undelivered) = (heard.clone(), undelivered.clone());
            Some(std::thread::spawn(move || {
                loop {
                    match read_ack(&mut tcp) {
                        Ok(Some(range)) => heard.lock().unwrap().take(range, &undelivered),
                        Ok(None) => return,
                        Err(e) => {
                            heard.lock().unwrap().error.get_or_insert(e);
                            return;
                        }
                    }
                }
            }))
        }
        _ => None,
    };
    let mut sent = 0;
    while sent < plan.rest.len() {
        let n = (1 + rng.below(16 << 10)).min(plan.rest.len() - sent);
        if let Err(e) = wire.write_all(&plan.rest[sent..sent + n]) {
            // A receiver that refused what came before closes the connection under what follows.
            if plan.streamed && plan.finish != Finish::Spoiled {
                return Err(format!("sending: {e}"));
            }
            break;
        }
        sent += n;
    }
    let _ = wire.flush();
    if plan.must_ack {
        let (_, last) = plan.ids.expect("ids were sent");
        let deadline = Instant::now() + Duration::from_secs(60);
        while heard.lock().unwrap().through.is_none_or(|through| through < last) {
            if Instant::now() > deadline {
                return Err(format!("acked through {:?} of {last}", heard.lock().unwrap().through));
            }
            match reader {
                Some(_) => std::thread::sleep(Duration::from_millis(5)),
                None => match read_ack(&mut wire)? {
                    Some(range) => heard.lock().unwrap().take(range, undelivered),
                    None => return Err("closed the connection before acking every id".into()),
                },
            }
        }
    }
    wire.close_write();
    match reader {
        Some(reader) => reader.join().unwrap(),
        None if acks => {
            while let Some(range) = read_ack(&mut wire)? {
                heard.lock().unwrap().take(range, undelivered);
            }
        }
        None => {
            let mut more = [0; 64];
            match wire.read(&mut more) {
                Ok(0) => {}
                Ok(n) => return Err(format!("sent {:?}", &more[..n])),
                Err(e) if matches!(e.kind(), std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut) => {
                    return Err("kept the connection open".into());
                }
                Err(_) => {}
            }
        }
    }
    let heard = heard.lock().unwrap();
    match &heard.error {
        Some(error) => Err(error.clone()),
        None => Ok(()),
    }
}

/// A record as it reads in an error.
fn shown(raw: &[u8]) -> String {
    let text = String::from_utf8_lossy(raw);
    match text.char_indices().nth(120) {
        Some((at, _)) => format!("{:?}… ({} bytes)", &text[..at], raw.len()),
        None => format!("{text:?}"),
    }
}

/// Sends `plan`'s connection to the receiver, and checks that every record it gives is the next of a stream's, every
/// record a stream must give arrives, and every ack covers only what was delivered.
fn check_connection(
    source: &mut Box<dyn Source>,
    settings: &Settings,
    pki: &Arc<Pki>,
    plan: &Arc<Plan>,
    seed: u64,
) -> Result<usize, String> {
    let mut undelivered = HashMap::new();
    for queue in &plan.queues {
        for record in &queue.records {
            for id in &record.ids {
                *undelivered.entry(*id).or_insert(0) += 1;
            }
        }
    }
    for id in &plan.dropped {
        *undelivered.entry(*id).or_insert(0) += 1;
    }
    let undelivered = Arc::new(Mutex::new(undelivered));
    let forwarder = {
        let (pki, plan, undelivered) = (pki.clone(), plan.clone(), undelivered.clone());
        let (at, tls) = (settings.connect, settings.tls);
        std::thread::spawn(move || forward(at, tls, &pki, &plan, &undelivered, seed))
    };
    let mut matching = Matching::new(&plan.queues);
    let deadline = Instant::now() + Duration::from_secs(120);
    let quiet = Duration::from_millis(60);
    let drained = receiver::drain(source, Duration::from_millis(20), quiet, deadline, |batch| {
        let finished = forwarder.is_finished();
        let ids = matching.take(batch, SourceMeta::S2S_RECEIVER)?;
        let mut held = undelivered.lock().unwrap();
        for id in ids {
            *held.get_mut(&id).unwrap() -= 1;
        }
        drop(held);
        if !batch.is_empty() {
            // An ack the receiver sends before this commit is read meanwhile.
            std::thread::sleep(Duration::from_millis(20));
        }
        Ok(finished && batch.is_empty())
    })?;
    if !drained {
        return Err("the connection is still open".into());
    }
    forwarder.join().unwrap()?;
    matching.complete()?;
    Ok(matching.taken)
}

/// The records a receiver gives, taken against the queues they must come from.
struct Matching<'a> {
    planned: &'a [Queue],
    queues: Vec<VecDeque<Expected>>,
    taken: usize,
}

impl<'a> Matching<'a> {
    fn new(planned: &'a [Queue]) -> Matching<'a> {
        Matching {
            planned,
            queues: planned.iter().map(|q| q.records.clone()).collect(),
            taken: 0,
        }
    }

    /// Takes `batch`, each record of which must be the next of a queue, and answers the ids whose bytes they hold.
    fn take(&mut self, batch: &[SourceRecord], meta: SourceMeta) -> Result<Vec<u64>, String> {
        let mut ids = Vec::new();
        for (record, object) in batch.iter().zip(meta.objects(batch, Some("s2s"))) {
            let raw = record.bytes().unwrap();
            let filed: Vec<(String, Vec<u8>)> = (record.headers.iter().flatten())
                .filter(|(key, _)| key != "fields")
                .cloned()
                .collect();
            let fields = object.get("fields").filter(|f| !f.is_null()).cloned();
            let Some(queue) = self.queues.iter_mut().find(|queue| {
                queue.front().is_some_and(|next| {
                    next.raw == raw
                        && std::iter::once(&next.filing)
                            .chain(&next.earlier)
                            .any(|filing| filing.filed == filed && filing.fields == fields)
                })
            }) else {
                let nexts: Vec<String> = (self.queues.iter().filter_map(VecDeque::front))
                    .map(|next| {
                        format!(
                            "{} filed {:?} fields {:?}",
                            shown(&next.raw),
                            next.filing.filed,
                            next.filing.fields
                        )
                    })
                    .collect();
                return Err(format!(
                    "gave {} filed {filed:?} fields {fields:?}, which no stream gives next: {nexts:#?}",
                    shown(raw)
                ));
            };
            ids.extend(queue.pop_front().unwrap().ids);
            self.taken += 1;
        }
        Ok(ids)
    }

    /// Whether every record a queue must give was taken.
    fn complete(&self) -> Result<(), String> {
        for (queue, planned) in self.queues.iter().zip(self.planned) {
            let missing = (planned.required + queue.len()).saturating_sub(planned.records.len());
            if missing > 0 {
                return Err(format!(
                    "{missing} records of a stream did not arrive, from {}",
                    shown(&queue[0].raw)
                ));
            }
        }
        Ok(())
    }
}

/// Every label a seed may reach, as the generators name them.
fn every_label(ipv6: bool) -> Vec<String> {
    let mut labels: Vec<String> = receiver::listen_names(ipv6).map(|ip| format!("listen {ip}")).collect();
    labels.extend(["None", "Served", "Vouched"].map(|tls| format!("tls {tls}")));
    labels.extend((0..=3).map(|n| format!("tokens {n}")));
    labels.extend(RECEIVER_LIMITS.iter().map(|(_, name)| name.to_string()));
    labels.extend(["Xml", "Dated"].map(|b| format!("sourcetype {b} configured")));
    labels.push("presents Nothing under None".into());
    for tls in ["Served", "Vouched"] {
        labels.extend(["Nothing", "Trusted", "Stranger"].map(|p| format!("presents {p} under {tls}")));
    }
    for finish in ["Clean", "Abrupt", "Spoiled"] {
        labels.extend([true, false].map(|ack| format!("{finish} ending with acks {ack}")));
    }
    labels.extend(TOKENS.iter().map(|t| t.to_string()));
    labels.extend(TOKENS[2..].iter().map(|t| format!("{t} to a receiver that holds none")));
    labels.extend(TIMES.iter().map(|(_, t)| t.to_string()));
    labels.extend(SPOILS.iter().map(|s| s.to_string()));
    labels.extend(
        [
            "an HTTP request on its port",
            "a signature that strays from the cooked mode",
            "a connection that ends inside its signature",
            "ack=0",
            "ack=1",
            "compression=0",
            "compression=1",
            "capabilities the receiver passes over",
            "capabilities framed against what they ask",
            "capabilities without __s2s_capabilities",
            "an unreadable capability",
            "capabilities past the limit",
            "a v3 message that does not end in its usual trailer",
            "ForwarderInfo framed against what was asked",
            "compression asked over TLS, ForwarderInfo plain",
            "a v3 message after ForwarderInfo",
            "a compressed stream that does not inflate",
            "one stream at a time",
            "a sourcetype prefixed",
            "a sourcetype bare",
            "no sourcetype",
            "lines",
            "events broken by Xml",
            "events broken by Dated",
            "Xml read a line an event",
            "Dated read a line an event",
            "a channel with field names",
            "a key from the dictionary",
            "a timestamp key, passed over",
            "a subsecond",
            "an index field",
            "a bookkeeping field, passed over",
            "a key naming its channel's field name",
            "a key of text",
            "a value of an integer",
            "a value of text",
            "a value of null",
            "a value of false",
            "a value of true",
            "a value of a dictionary word",
            "a value of raw bytes",
            "a value of raw bytes escaped as CSV",
            "a value of raw bytes escaped as JSON",
            "a key repeated",
            "the default index",
            "an event with its place in its source",
            "a flag of the forwarder's bookkeeping",
            "an event with no id",
            "an empty chunk",
            "a chunk ending its stream",
            "a broken event of only whitespace",
            "an event the forwarder broke",
            "a broken event ending its stream",
            "a heartbeat",
            "a ForwarderInfo event",
            "a timezone frame",
            "a channel freed",
            "a channel freed with bytes held",
            "a channel freed and defined again",
            "a channel defined over",
            "a stream ended by its channel defined again",
            "an event of only whitespace",
            "an event spanning chunks",
            "an event past the limit, in pieces",
            "an event held past its limit and the slack",
            "bytes held as a connection with acks ends",
            "a line end of \\r\\n split between chunks",
            "records delivered",
        ]
        .map(String::from),
    );
    labels
}

#[test]
fn every_setting_reads_random_forwarders_as_an_indexer_breaks_them() {
    crate::certs::install_provider();
    let pki = Arc::new(Pki::new());
    let ipv6 = receiver::ipv6();
    let mut cov = Coverage::new(every_label(ipv6));
    fuzz::run(1..201, |seed| {
        let mut rng = Rng::seeded(seed);
        let settings = random_settings(&mut rng, ipv6);
        cov.reach(format!("listen {}", settings.listen.ip()));
        cov.reach(format!("tls {:?}", settings.tls));
        cov.reach(format!("tokens {}", settings.tokens.len()));
        for breaking in &settings.sourcetypes {
            cov.reach(format!("sourcetype {breaking:?} configured"));
        }
        let limit = RECEIVER_LIMITS
            .iter()
            .find(|(below, _)| settings.max_request_bytes < *below)
            .unwrap();
        cov.reach(limit.1);
        let mut source = open(&settings, &pki, false);
        for n in 0..1 + rng.below(4) {
            let plan = Arc::new(random_connection(&mut rng, &settings, n));
            let what = format!(
                "connection {n} under {:?} with limit {}, presenting {:?}, reaching {:?}, acks {}, ending {:?}",
                settings.tls, settings.max_request_bytes, plan.presents, plan.reached, plan.ack, plan.finish
            );
            let taken = check_connection(&mut source, &settings, &pki, &plan, rng.next_u64())
                .map_err(|why| format!("{what}: {why}"))?;
            if taken > 0 {
                cov.reach("records delivered");
            }
            for (stage, label) in &plan.labels {
                if *stage <= plan.reached {
                    cov.reach(label.clone());
                }
            }
        }
        Ok(())
    });
    cov.assert_complete();
}

/// One `httpout` request presenting `token` over `tls`, and its answer's status and body. A body past what fits in
/// the sockets' buffers waits for `100 Continue`, so an answer given before it is read is not lost to a reset.
fn httpout(at: SocketAddr, tls: Tls, pki: &Pki, token: &str, body: &[u8]) -> (u16, String) {
    let tcp = TcpStream::connect(at).unwrap();
    tcp.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
    tcp.set_write_timeout(Some(Duration::from_secs(30))).unwrap();
    let mut wire = match tls {
        Tls::None => Wire::Plain(tcp),
        _ => {
            let connection =
                rustls::ClientConnection::new(pki.client(Presents::Trusted), "localhost".try_into().unwrap()).unwrap();
            Wire::Tls(Box::new(rustls::StreamOwned::new(connection, tcp)))
        }
    };
    let expect = body.len() > 16 * 1024;
    let mut head = format!(
        "POST /services/collector/s2s HTTP/1.1\r\nHost: receiver\r\nAuthorization: Splunk {token}\r\n\
         Content-Type: application/octet-stream\r\nContent-Length: {}\r\nConnection: close\r\n",
        body.len()
    );
    if expect {
        head += "Expect: 100-continue\r\n";
    }
    head += "\r\n";
    let mut answer = Vec::new();
    if wire.write_all(head.as_bytes()).is_ok() {
        if expect {
            let mut byte = [0];
            while !answer.ends_with(b"\r\n\r\n") && wire.read_exact(&mut byte).is_ok() {
                answer.push(byte[0]);
            }
            if answer.starts_with(b"HTTP/1.1 100") {
                answer.clear();
                let _ = wire.write_all(body);
            }
        } else {
            let _ = wire.write_all(body);
        }
        let _ = wire.flush();
        let _ = wire.read_to_end(&mut answer);
    }
    let answer = String::from_utf8_lossy(&answer).to_string();
    let status = answer.get(9..12).and_then(|s| s.parse().ok()).unwrap_or(0);
    (
        status,
        answer.split_once("\r\n\r\n").map_or("", |(_, body)| body).to_string(),
    )
}

/// A forwarder's `httpout` sends the same v4 stream to a HEC receiver, a request at a time: each body compressed,
/// standing alone, and its channels' streams ending with it. Each request is answered Success once its records are
/// delivered, refused whole as unreadable when a frame in it is, and as too large when it inflates past the limit.
#[test]
fn hec_receivers_read_random_httpout_requests_as_an_indexer_breaks_them() {
    crate::certs::install_provider();
    let pki = Arc::new(Pki::new());
    let ipv6 = receiver::ipv6();
    let mut cov = Coverage::new(
        ["None", "Served", "Vouched"]
            .map(|tls| format!("tls {tls}"))
            .into_iter()
            .chain(["Clean", "Abrupt", "Spoiled"].map(|f| format!("a body ending {f}")))
            .chain(
                [
                    "answered 200",
                    "answered 400",
                    "answered 413",
                    "a body past the limit",
                    "a body that inflates past the limit",
                    "records delivered",
                    "one stream at a time",
                    "events broken by Xml",
                    "events broken by Dated",
                    "a stream ended by its channel defined again",
                    "a channel freed with bytes held",
                ]
                .map(String::from),
            ),
    );
    fuzz::run(1..101, |seed| {
        let mut rng = Rng::seeded(seed);
        let mut settings = random_settings(&mut rng, ipv6);
        // A HEC receiver holds a token, whatever vouches for its senders.
        if settings.tokens.is_empty() {
            settings.tokens.push(format!("tok-{:x}", rng.next_u64()));
        }
        cov.reach(format!("tls {:?}", settings.tls));
        let max = settings.max_request_bytes;
        let mut source = open(&settings, &pki, true);
        for n in 0..1 + rng.below(4) {
            let finish = *rng.pick(&[Finish::Clean, Finish::Abrupt, Finish::Spoiled]);
            let mut stream = Stream::new(&mut rng, &settings, false, format!("r{n}-"));
            stream.httpout = true;
            let steps = 1 + stream.rng.below(30);
            stream.write(steps, finish);
            let Stream {
                out,
                queues,
                labels,
                single,
                ..
            } = stream;
            let body = compressed(&mut rng, &out.0);
            let expected = match () {
                () if body.len() > max => {
                    cov.reach("a body past the limit");
                    413
                }
                // A step inflates before it is read, and every limit here is within one.
                () if out.0.len() > max => {
                    cov.reach("a body that inflates past the limit");
                    413
                }
                () if finish == Finish::Spoiled => 400,
                () => 200,
            };
            let what = format!(
                "request {n} under {:?} with limit {max}, ending {finish:?}, of {} bytes inflating to {}",
                settings.tls,
                body.len(),
                out.0.len()
            );
            let token = rng.pick(&settings.tokens).clone();
            let request = {
                let (pki, body) = (pki.clone(), body.clone());
                let (at, tls) = (settings.connect, settings.tls);
                std::thread::spawn(move || httpout(at, tls, &pki, &token, &body))
            };
            let none = Vec::new();
            let planned = match expected {
                200 => &queues,
                _ => &none,
            };
            let mut matching = Matching::new(planned);
            let deadline = Instant::now() + Duration::from_secs(120);
            let quiet = Duration::from_millis(60);
            let drained = receiver::drain(&mut source, Duration::from_millis(20), quiet, deadline, |batch| {
                let finished = request.is_finished();
                matching
                    .take(batch, SourceMeta::HEC_RECEIVER)
                    .map_err(|why| format!("{what}: {why}"))?;
                Ok(finished && batch.is_empty())
            })?;
            if !drained {
                return Err(format!("{what}: not answered"));
            }
            let (status, answer) = request.join().unwrap();
            if status != expected {
                return Err(format!("{what}: answered {status} {answer}, not {expected}"));
            }
            let code = serde_json::from_str::<Value>(&answer).ok().map(|a| a["code"].clone());
            let wanted = match status {
                200 => Some(Value::from(0)),
                400 => Some(Value::from(6)),
                _ => code.clone(),
            };
            if code != wanted {
                return Err(format!("{what}: answered {status} {answer}"));
            }
            matching.complete().map_err(|why| format!("{what}: {why}"))?;
            cov.reach(format!("answered {status}"));
            cov.reach(format!("a body ending {finish:?}"));
            if status == 200 {
                if matching.taken > 0 {
                    cov.reach("records delivered");
                }
                if single {
                    cov.reach("one stream at a time");
                }
                for label in labels {
                    cov.reach(label);
                }
            }
        }
        Ok(())
    });
    cov.assert_complete();
}

/// What a file a live forwarder reads holds: events written as a sourcetype's `Breaking` says, which the receiver
/// breaks, or JSON lines, which the forwarder breaks itself and extracts fields from.
#[derive(Clone, Copy, PartialEq, Debug)]
enum Holds {
    Events(Breaking),
    Json,
}

impl Holds {
    fn sourcetype(self) -> &'static str {
        match self {
            Holds::Events(breaking) => breaking.sourcetype(),
            Holds::Json => "json",
        }
    }
}

/// One file a live forwarder reads, and how its input files it.
struct LiveFile {
    name: String,
    holds: Holds,
    body: Vec<u8>,
    host: Option<String>,
    index: Option<&'static str>,
    meta: Option<String>,
}

/// One seed of the live fuzzer: a receiver's settings, the output group that sends to it and the files routed there.
struct LivePlan {
    settings: Settings,
    group: String,
    files: Vec<LiveFile>,
    labels: Vec<String>,
}

/// Where the forwarder's container finds what the test wrote.
const LIVE_DIR: &str = "/smeltr-fuzz";

/// The forwarder's hostname: a file's host where its input names none.
const LIVE_HOST: &str = "s2s-fuzz";

/// One event of `holds`, ending its line or lines with `end`. The `n`th event of a file, unless it is `long` bytes.
fn live_event(rng: &mut Rng, holds: Holds, n: usize, end: &str, long: Option<usize>) -> String {
    let word = *rng.pick(WORDS);
    let filler = |len: usize| "y".repeat(len);
    match holds {
        Holds::Events(Breaking::Lines) => match long {
            Some(len) => format!("long {n} {}{end}", filler(len)),
            None => format!("{n} {word}{end}"),
        },
        Holds::Events(Breaking::Xml) => match long {
            Some(len) => format!("<e n=\"{n}\">{}</e>{end}", filler(len)),
            None => format!("<e n=\"{n}\">{word}{end}  <f>{word}</f>{end}</e>{end}"),
        },
        Holds::Events(Breaking::Dated) => match long {
            Some(len) => format!("2026-10-14 03:00:00 {n} {}{end}", filler(len)),
            None => format!("2026-10-14 03:00:00 {n} {word}{end}  continued {word}{end}"),
        },
        Holds::Json => serde_json::json!({ "n": n, "word": word }).to_string() + "\n",
    }
}

fn live_plan(seed: u64) -> LivePlan {
    let mut rng = Rng::seeded(seed);
    let mut labels = Vec::new();
    let tls = *rng.pick(&[Tls::None, Tls::Served, Tls::Vouched]);
    // A forwarder presents only a token in a GUID's form, and warns of any other.
    let tokens = (0..rng.below(3) + usize::from(tls != Tls::Vouched))
        .map(|_| {
            let (a, b) = (rng.next_u64(), rng.next_u64());
            format!(
                "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
                a >> 32,
                a >> 16 & 0xffff,
                a & 0xffff,
                b >> 48,
                b & 0xffff_ffff_ffff
            )
        })
        .collect();
    let sourcetypes = [Breaking::Xml, Breaking::Dated]
        .into_iter()
        .filter(|_| rng.chance(2))
        .collect();
    // A forwarder's frames carry up to 64 KiB of a file.
    let max_request_bytes = (128 << 10) + rng.below(896 << 10);
    let port = std::net::TcpListener::bind((Ipv4Addr::UNSPECIFIED, 0))
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let listen = SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), port);
    let settings = Settings {
        listen,
        connect: listen,
        tokens,
        tls,
        max_request_bytes,
        sourcetypes,
    };

    let name = format!("seed-{seed}");
    let mut group = format!("[tcpout:{name}]\nserver = host.docker.internal:{port}\n");
    // Each line of the group's settings, and the label it reaches.
    let mut set: Vec<(String, String)> = Vec::new();
    let mut label = |label: &str| labels.push(label.to_string());
    match settings.tokens.is_empty() {
        true => label("no token"),
        false => set.push((format!("token = {}", rng.pick(&settings.tokens)), "a token".into())),
    }
    match rng.chance(2) {
        true => set.push(("useACK = true".into(), "acks".into())),
        false => label("no acks"),
    }
    if rng.chance(2) {
        set.push(("compressed = true".into(), "compressed".into()));
    }
    if rng.chance(2) {
        // Small enough that a forwarder waits on acks for a file's backlog.
        let size = *rng.pick(&["100KB", "512KB", "7MB"]);
        set.push((format!("maxQueueSize = {size}"), "a queue size".into()));
    }
    match tls {
        Tls::None => label("tls None"),
        _ => {
            set.push(("useSSL = true".into(), format!("tls {tls:?}")));
            let verify = rng.chance(2);
            // A forwarder without a certificate of its own reads no authorities, and so cannot verify the receiver:
            // it sends without TLS instead.
            if tls == Tls::Vouched || verify || rng.chance(2) {
                let line = format!("clientCert = {LIVE_DIR}/certs/forwarder.pem");
                set.push((line, "a client certificate".into()));
            }
            if verify {
                set.push((
                    "sslVerifyServerCert = true".into(),
                    "the receiver's certificate verified".into(),
                ));
                if rng.chance(2) {
                    set.push((
                        "sslVerifyServerName = true".into(),
                        "the receiver's name verified".into(),
                    ));
                }
            }
        }
    }
    for (line, reached) in set {
        group += &line;
        group.push('\n');
        labels.push(reached);
    }

    let mut files = Vec::new();
    for k in 0..1 + rng.below(3) {
        let holds = match rng.below(4) {
            0 => Holds::Json,
            b => Holds::Events([Breaking::Lines, Breaking::Xml, Breaking::Dated][b - 1]),
        };
        let configured = matches!(holds, Holds::Events(b) if settings.sourcetypes.contains(&b));
        labels.push(format!(
            "{holds:?} broken {}",
            if configured { "by its pattern" } else { "as lines" }
        ));
        let end = match holds {
            Holds::Json => "\n",
            _ => *rng.pick(&["\n", "\n", "\r\n"]),
        };
        if end == "\r\n" {
            labels.push("lines ending \\r\\n".into());
        }
        let count = 1 + rng.below(400);
        let long_at = (holds != Holds::Json && rng.chance(3)).then(|| rng.below(count));
        let mut body = String::new();
        for n in 0..count {
            let long = (long_at == Some(n)).then(|| max_request_bytes / 2 + rng.below(2 * max_request_bytes));
            body += &live_event(&mut rng, holds, n, end, long);
        }
        let host = rng.chance(3).then(|| format!("web-{seed}-{k}"));
        let index = rng.chance(3).then_some("fuzz");
        let meta = rng.chance(3).then(|| format!("team::fuzz seed::s{seed}"));
        for (set, label) in [
            (host.is_some(), "a host"),
            (index.is_some(), "an index"),
            (meta.is_some(), "_meta"),
        ] {
            if set {
                labels.push(label.into());
            }
        }
        files.push(LiveFile {
            name: format!("file-{k}.log"),
            holds,
            body: body.into_bytes(),
            host,
            index,
            meta,
        });
    }
    LivePlan {
        settings,
        group,
        files,
        labels,
    }
}

impl LivePlan {
    /// Where the forwarder reads `file`.
    fn source(&self, file: &LiveFile) -> String {
        format!("{LIVE_DIR}/data/{}/{}", self.name(), file.name)
    }

    fn name(&self) -> &str {
        self.group["[tcpout:".len()..].split(']').next().unwrap()
    }

    /// The records the receiver must give for `file`, in order, as an indexer breaks the whole of it.
    fn expected(&self, file: &LiveFile, labels: &mut Vec<String>) -> Vec<Vec<u8>> {
        if file.holds == Holds::Json {
            return (file.body.split(|b| *b == b'\n'))
                .filter(|line| !line.is_empty())
                .map(<[u8]>::to_vec)
                .collect();
        }
        let line_breaker = match file.holds {
            Holds::Events(b) if self.settings.sourcetypes.contains(&b) => b.line_breaker(),
            _ => None,
        };
        let filing = Arc::new(Filing {
            filed: Vec::new(),
            fields: None,
        });
        let segment = Segment {
            bytes: file.body.clone(),
            chunks: vec![Chunked {
                at: 0,
                len: file.body.len(),
                id: None,
                filing,
            }],
        };
        let (must, _, _) = segment_records(
            &segment,
            line_breaker.as_ref(),
            self.settings.max_request_bytes,
            Ended::Whole,
            labels,
        );
        must.into_iter().map(|e| e.raw).collect()
    }
}

/// The forwarder's app: its inputs, outputs and props for every plan.
fn live_app(plans: &[LivePlan]) -> [(&'static str, String); 4] {
    let mut inputs = String::new();
    for plan in plans {
        for file in &plan.files {
            inputs += &format!(
                "[monitor://{}]\nsourcetype = {}\n_TCP_ROUTING = {}\ncrcSalt = <SOURCE>\n",
                plan.source(file),
                file.holds.sourcetype(),
                plan.name()
            );
            for (key, value) in [("host", &file.host), ("_meta", &file.meta)] {
                if let Some(value) = value {
                    inputs += &format!("{key} = {value}\n");
                }
            }
            if let Some(index) = file.index {
                inputs += &format!("index = {index}\n");
            }
            inputs.push('\n');
        }
    }
    // Only what the test writes: the forwarder's own indexes are not the test's to control. A forwarder backs off a
    // receiver it cannot reach for one to ten times autoLBFrequency.
    let mut outputs = format!(
        "[tcpout]\ndefaultGroup = {}\nforwardedindex.3.blacklist = _.*\nautoLBFrequency = 1\n\n",
        plans[0].name()
    );
    for plan in plans {
        outputs += &plan.group;
        outputs.push('\n');
    }
    let props = "[json]\nINDEXED_EXTRACTIONS = json\n".to_string();
    // An output group verifies its receiver against the forwarder's authorities: it reads none of its own.
    let server = format!("[sslConfig]\nsslRootCAPath = {LIVE_DIR}/certs/authority.pem\n");
    [
        ("inputs.conf", inputs),
        ("outputs.conf", outputs),
        ("props.conf", props),
        ("server.conf", server),
    ]
}

impl Container {
    /// What the forwarder warned of, most often first: why it sent nothing.
    fn outputs_log(&self) -> String {
        let log = std::process::Command::new("docker")
            .args(["exec", "-u", "root", &self.0, "sh", "-c"])
            .arg(
                "grep -E ' (WARN|ERROR) ' /opt/splunkforwarder/var/log/splunk/splunkd.log | cut -d' ' -f4- \
                 | sort | uniq -c | sort -rn | head -n 20",
            )
            .output();
        log.map(|l| String::from_utf8_lossy(&l.stdout).into_owned())
            .unwrap_or_default()
    }
}

/// Writes `body` to `path` under `root`, readable by the forwarder's user, as is every directory between them.
fn readable(root: &std::path::Path, path: &std::path::Path, body: &[u8]) {
    let path = root.join(path);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, body).unwrap();
    for dir in path.ancestors().skip(1).take_while(|dir| dir.starts_with(root)) {
        share(dir);
    }
}

/// Warnings the receivers log, such as why a forwarder's connection was closed.
struct Warned(Arc<Mutex<Vec<String>>>);

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Warned {
    fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        struct Fields(String);
        impl tracing::field::Visit for Fields {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                self.0 += &format!(" {}={value:?}", field.name());
            }
        }
        if *event.metadata().level() <= tracing::Level::WARN {
            let mut fields = Fields(String::new());
            event.record(&mut fields);
            let warning = format!(
                "{} {}:{}",
                event.metadata().level(),
                event.metadata().target(),
                fields.0
            );
            eprintln!("{warning}");
            self.0.lock().unwrap().push(warning);
        }
    }
}

/// A real universal forwarder reads files of random sourcetypes, hosts, indexes and `_meta`, and sends each seed's to
/// its own receiver of random settings through an output group of random settings: TLS, verifying the receiver or
/// presenting a certificate, a token, acks, compression and queue size. Each receiver gives each file's records once,
/// in order, as an indexer breaks the whole file, filed as its input says, and warns of nothing.
#[test]
fn live_universal_forwarders_send_random_outputs_as_an_indexer_breaks_them() {
    const TEST: &str = "live_universal_forwarders_send_random_outputs_as_an_indexer_breaks_them";
    let Some(image) = crate::live::universal_forwarder(TEST) else {
        return;
    };
    crate::certs::install_provider();
    let warnings = Arc::new(Mutex::new(Vec::new()));
    // The receivers warn on their runtimes' threads, which only a global subscriber hears.
    let installed = tracing::subscriber::set_global_default(tracing_subscriber::layer::SubscriberExt::with(
        tracing_subscriber::registry(),
        Warned(warnings.clone()),
    ));
    assert!(
        installed.is_ok(),
        "another test set the global subscriber, so the receivers' warnings would go unheard: run this test by itself"
    );
    // A panic on any thread but the test's own is the receiver's: its handlers run on the runtime's threads, where a
    // panic drops a connection and fails no test by itself.
    let panicked = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let seen = panicked.clone();
    let test_thread = std::thread::current().id();
    let _hook = Hook::set(Box::new(move |info| {
        if std::thread::current().id() != test_thread {
            seen.store(true, std::sync::atomic::Ordering::Relaxed);
        }
        eprintln!("{info}");
    }));

    // The forwarder reads every seed's files from its start, so every receiver is open before it starts.
    let seeds = fuzz::seeds(1..25);
    let plans: Vec<LivePlan> = seeds.clone().map(live_plan).collect();
    let pki = Pki::new();
    let mut receivers: Vec<Box<dyn Source>> = plans.iter().map(|p| open(&p.settings, &pki, false)).collect();

    let dir = Scratch(std::env::temp_dir().join(format!("smeltr-s2s-fuzz-{}", std::process::id())));
    let forwarder = pki.authority.issue("forwarder", vec!["forwarder".into()]);
    readable(&dir.0, "certs/authority.pem".as_ref(), pki.authority.pem.as_bytes());
    readable(
        &dir.0,
        "certs/forwarder.pem".as_ref(),
        format!("{}{}", forwarder.certificate, forwarder.key).as_bytes(),
    );
    for (name, body) in live_app(&plans) {
        readable(&dir.0, &std::path::Path::new("app/local").join(name), body.as_bytes());
    }
    for plan in &plans {
        for file in &plan.files {
            readable(
                &dir.0,
                &std::path::Path::new("data").join(plan.name()).join(&file.name),
                &file.body,
            );
        }
    }
    let name = format!("smeltr-s2s-fuzz-{}", std::process::id());
    // The app is copied in, not mounted, since the image's set-up takes ownership of everything under the forwarder's
    // home. Running it accepts Splunk's license and general terms.
    let started = std::process::Command::new("docker")
        .args(["run", "-d", "--name", &name, "--hostname", LIVE_HOST])
        .args(["--add-host", "host.docker.internal:host-gateway"])
        .args(["-e", "SPLUNK_START_ARGS=--accept-license"])
        .args(["-e", "SPLUNK_GENERAL_TERMS=--accept-sgt-current-at-splunk-com"])
        .args(["-e", "SPLUNK_PASSWORD=smeltr-live-1"])
        .arg("-v")
        .arg(format!("{}:{LIVE_DIR}:ro", dir.0.display()))
        .args(["--entrypoint", "/bin/sh", &image, "-c"])
        .arg(format!(
            "sudo cp -r {LIVE_DIR}/app /opt/splunkforwarder-etc/apps/smeltr-fuzz && exec /sbin/entrypoint.sh \"$@\""
        ))
        .args(["splunk-uf", "start-service"])
        .output()
        .expect("docker runs");
    // Removed however it went: a run that failed may have created it.
    let container = Container(name);
    assert!(
        started.status.success(),
        "docker run: {}",
        String::from_utf8_lossy(&started.stderr)
    );

    let mut cov = Coverage::new(
        [
            "tls None",
            "tls Served",
            "tls Vouched",
            "a client certificate",
            "the receiver's certificate verified",
        ]
        .into_iter()
        .chain([
            "the receiver's name verified",
            "a token",
            "no token",
            "acks",
            "no acks",
            "compressed",
        ])
        .chain(["a queue size", "a host", "an index", "_meta", "lines ending \\r\\n"])
        .chain(["Json broken as lines", "an event past the limit, in pieces"])
        .map(String::from)
        .chain(
            [Breaking::Lines, Breaking::Xml, Breaking::Dated]
                .into_iter()
                .flat_map(|b| {
                    let mut by = vec![format!("{:?} broken as lines", Holds::Events(b))];
                    if b != Breaking::Lines {
                        by.push(format!("{:?} broken by its pattern", Holds::Events(b)));
                    }
                    by
                }),
        ),
    );
    // What each seed's receiver gave, and its `__source`. Every receiver is taken from while any seed waits, so no
    // group's unacked window holds back the forwarder's others.
    let mut received: Vec<Vec<(Vec<u8>, SourceFields)>> = plans.iter().map(|_| Vec::new()).collect();
    let mut take_all = |received: &mut Vec<Vec<(Vec<u8>, SourceFields)>>| {
        for (source, received) in receivers.iter_mut().zip(received.iter_mut()) {
            let batch = source.poll_batch(10_000, 1 << 26, Duration::from_millis(10)).unwrap();
            commit(source, &batch);
            let fields = SourceMeta::S2S_RECEIVER.objects(&batch, Some("s2s"));
            received.extend(batch.iter().map(|r| r.bytes().unwrap().to_vec()).zip(fields));
        }
    };
    // The forwarder starts before it sends: the first seed waits for that too.
    let mut deadline = Instant::now() + Duration::from_secs(300);
    fuzz::run(seeds.clone(), |seed| {
        let at = (seed - seeds.start) as usize;
        let plan = &plans[at];
        let mut labels = plan.labels.clone();
        let expected: Vec<(&LiveFile, Vec<Vec<u8>>)> =
            plan.files.iter().map(|f| (f, plan.expected(f, &mut labels))).collect();
        let total: usize = expected.iter().map(|(_, e)| e.len()).sum();
        let what = format!(
            "{} under {:?} with limit {}, sourcetypes {:?}",
            plan.group.trim_end().replace('\n', "; "),
            plan.settings.tls,
            plan.settings.max_request_bytes,
            plan.settings.sourcetypes
        );
        while received[at].len() < total && Instant::now() < deadline {
            take_all(&mut received);
        }
        // Long enough to give a record sent twice.
        let settled = Instant::now() + Duration::from_secs(2);
        while Instant::now() < settled {
            take_all(&mut received);
        }
        deadline = Instant::now() + Duration::from_secs(120);
        if panicked.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(format!("{what}: a receiver panicked"));
        }
        let warned = std::mem::take(&mut *warnings.lock().unwrap());
        if !warned.is_empty() {
            return Err(format!("{what}: the receivers warned: {warned:#?}"));
        }
        for (file, records) in &expected {
            let source = plan.source(file);
            let got: Vec<&(Vec<u8>, SourceFields)> = received[at]
                .iter()
                .filter(|(_, f)| f["source"] == source.as_str())
                .collect();
            let missing = || format!("{what}: {source} gave {} of {} records", got.len(), records.len());
            for (n, ((raw, fields), want)) in got.iter().zip(records).enumerate() {
                if raw != want {
                    return Err(format!(
                        "{}: record {n} is {}, not {}",
                        missing(),
                        shown(raw),
                        shown(want)
                    ));
                }
                let filed = [
                    ("host", Some(file.host.as_deref().unwrap_or(LIVE_HOST))),
                    ("sourcetype", Some(file.holds.sourcetype())),
                    ("index", file.index),
                ];
                for (key, want) in filed {
                    if fields.get(key).and_then(Value::as_str) != want {
                        return Err(format!("{}: record {n}'s {key} is not {want:?}: {fields:?}", missing()));
                    }
                }
                let meta = fields.get("fields").unwrap_or(&Value::Null);
                let want_meta = file.meta.is_some();
                if want_meta != (meta["team"] == "fuzz") {
                    return Err(format!("{}: record {n}'s fields are {meta}", missing()));
                }
                if file.holds == Holds::Json && meta["word"].is_null() {
                    return Err(format!("{}: record {n}'s extracted fields are {meta}", missing()));
                }
            }
            if got.len() != records.len() {
                return Err(format!("{}\n{}", missing(), container.outputs_log()));
            }
        }
        let theirs: Vec<String> = plan.files.iter().map(|f| plan.source(f)).collect();
        if let Some((raw, fields)) = received[at]
            .iter()
            .find(|(_, f)| !theirs.iter().any(|s| f["source"] == s.as_str()))
        {
            return Err(format!(
                "{what}: a record of no file routed here: {} {fields:?}",
                shown(raw)
            ));
        }
        for label in labels {
            cov.reach(label);
        }
        Ok(())
    });
    cov.assert_complete();
}
