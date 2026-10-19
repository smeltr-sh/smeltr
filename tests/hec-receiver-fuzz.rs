//! Random HEC requests against HEC receivers of random settings. Its port takes whatever any host that reaches it
//! sends, and a HEC client acts on the answer's code: it drops a request answered with a code it does not retry, and
//! sends again one answered busy. So each answer must be the one HEC gives, and each request answered Success must
//! arrive whole, filed as its envelopes or its query say.
//!
//! Each seed opens a receiver under settings an operator could save and sends it random requests, to every path it
//! serves and to paths beside them: envelopes and raw events, whole or spoiled, under every way of presenting a token
//! and naming a channel, plain or gzip, within its limit or past it. A model built beside each request says what the
//! receiver must answer and which records it must give. An acknowledging sender's `ackId`s are asked after, and each
//! must read true once its records are committed. A deterministic PRNG over a seed range, from `harness::fuzz`: a
//! failure names its seed, and the fuzzer checks that its seeds reached every option it knows.

use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use smeltr::io::line_breaker::LineBreaker;
use smeltr::io::{Source, SourceRecord};
use smeltr::plan::topology::{HecReceiverConfig, Receiver, SourceMeta};

use crate::harness::docker::{Container, Hook, Scratch, container_logs, share};
use crate::harness::fuzz::{self, Coverage, Rng};
use crate::harness::receiver::{self, Answer, Pki, Presents, Request, Tls, commit};

/// One receiver's settings, as an operator could save them.
struct Settings {
    listen: SocketAddr,
    /// Where a sender reaches it.
    connect: SocketAddr,
    /// Each token, and whether its senders use indexer acknowledgement.
    tokens: Vec<(String, bool)>,
    tls: Tls,
    max_request_bytes: usize,
    /// The sourcetypes it breaks by a pattern of their own.
    sourcetypes: Vec<Breaking>,
}

/// How a raw body breaks into events.
#[derive(Clone, Copy, PartialEq, Debug)]
enum Breaking {
    /// An event per line, as a sourcetype given no pattern.
    Lines,
    /// `<e>` elements, whatever lines they span.
    Xml,
    /// Lines starting with a date, each with the lines after it that do not.
    Dated,
}

impl Breaking {
    /// The sourcetype a receiver breaks this way, and its pattern.
    fn sourcetype(self) -> (&'static str, &'static str) {
        match self {
            Breaking::Lines => ("syslog", smeltr::io::line_breaker::PER_LINE),
            Breaking::Xml => ("xml", r"</e>(\s*)"),
            Breaking::Dated => ("dated", r"([\r\n]+)\d{4}-"),
        }
    }
}

/// The limits a receiver is given, by the bound each is below.
const RECEIVER_LIMITS: &[(usize, &str)] = &[
    (4 << 10, "limit under 4 KiB"),
    (64 << 10, "limit under 64 KiB"),
    (usize::MAX, "limit of 64 KiB or more"),
];

fn random_settings(rng: &mut Rng, ipv6: bool) -> Settings {
    let (listen, connect) = receiver::random_addresses(rng, ipv6);
    let port = receiver::free_port(connect);
    let tls = receiver::random_tls(rng);
    // Each token, and whether its sender wants acknowledgements.
    let tokens = receiver::random_tokens(rng, tls, |rng, i| {
        (format!("tok{i}-{:x}", rng.next_u64()), rng.chance(2))
    });
    let sourcetypes = [Breaking::Xml, Breaking::Dated]
        .into_iter()
        .filter(|_| rng.chance(2))
        .collect();
    Settings {
        listen: SocketAddr::new(listen, port),
        connect: SocketAddr::new(connect, port),
        tokens,
        tls,
        max_request_bytes: receiver::random_limit(rng, 11),
        sourcetypes,
    }
}

fn open(settings: &Settings, pki: &Pki) -> Box<dyn Source> {
    let sourcetypes = (settings.sourcetypes.iter())
        .map(|b| {
            let (name, pattern) = b.sourcetype();
            (
                name.to_string(),
                LineBreaker(regex::bytes::Regex::new(pattern).unwrap()),
            )
        })
        .collect();
    let hec = Receiver::Hec(HecReceiverConfig {
        acknowledging: (settings.tokens.iter().enumerate())
            .filter(|(_, (_, acks))| *acks)
            .map(|(i, _)| format!("sender{i}"))
            .collect(),
        sourcetypes,
    });
    let tokens: Vec<String> = settings.tokens.iter().map(|(token, _)| token.clone()).collect();
    receiver::build(
        hec,
        settings.listen,
        settings.tls,
        &tokens,
        "sender",
        settings.max_request_bytes,
        pki,
    )
}

/// A record a receiver gives: its bytes, and what it is filed under.
#[derive(Clone, Debug, PartialEq)]
struct Taken {
    raw: String,
    /// Its host, source, sourcetype, index and time, as `hec::Filed` carries them.
    filed: Vec<(String, String)>,
    /// Its indexed fields.
    fields: Option<Value>,
}

/// What an envelope's `event` reads as, as JSON text, and the record it is: its text where it is a string, and its
/// JSON as written otherwise.
const EVENTS: &[(&str, Option<&str>)] = &[
    (r#""a line""#, Some("a line")),
    (
        r#""quoted \"word\" and \\ slash""#,
        Some("quoted \"word\" and \\ slash"),
    ),
    (r#""two\nlines\r\n""#, Some("two\nlines\r\n")),
    (r#""été 😀""#, Some("été 😀")),
    (r#""\u0000""#, Some("\0")),
    (r#"{"n":1,"s":"x"}"#, None),
    (r#"{ "nested" : { "a" : [1, 2.5, null] } }"#, None),
    (r#"[1,"two"]"#, None),
    ("42", None),
    ("-0.5e3", None),
    ("true", None),
];

/// What an envelope may file its event under, as JSON text, and the text it is filed as. `null` files nothing.
const FILED: &[(&str, Option<&str>)] = &[
    (r#""web-1""#, Some("web-1")),
    (r#""é ü""#, Some("é ü")),
    ("7", Some("7")),
    ("1.5", Some("1.5")),
    ("null", None),
];

/// An envelope's `time`, as JSON text, in microseconds since the epoch.
const TIMES: &[(&str, i64)] = &[
    ("1700000000", 1_700_000_000_000_000),
    ("1700000000.5", 1_700_000_000_500_000),
    (r#""1700000000.25""#, 1_700_000_000_250_000),
    (r#"" 1700000000 ""#, 1_700_000_000_000_000),
    ("1.7e9", 1_700_000_000_000_000),
    ("0", 0),
    ("-1.000001", -1_000_001),
];

/// An envelope's indexed `fields`, as JSON text. `null` names none.
const FIELDS: &[&str] = &[
    r#"{"env":"prod"}"#,
    r#"{"n":3,"tags":["a","b"],"ok":true}"#,
    "{}",
    "null",
];

/// How an envelope is spoiled, and the code HEC refuses its body with.
const SPOILS: &[(&str, u16)] = &[
    ("no event", 12),
    ("a blank event", 13),
    ("an envelope that is no object", 6),
    ("an envelope cut short", 6),
    ("a key given twice", 6),
    ("a host that is no text", 6),
    ("a time that is no number", 6),
    ("fields that are no object", 15),
];

/// One envelope, as JSON text, and the record it is or the code its body is refused with.
fn envelope(rng: &mut Rng, spoiled: bool, labels: &mut Labels) -> (String, Result<Taken, u16>) {
    let mut keys: Vec<String> = Vec::new();
    let (event, text) = *rng.pick(EVENTS);
    let raw = match text {
        Some(text) => text.to_string(),
        None => event.to_string(),
    };
    keys.push(format!(r#""event":{event}"#));
    let mut filed = Vec::new();
    for key in ["host", "source", "sourcetype", "index"] {
        if rng.chance(3) {
            let (json, text) = *rng.pick(FILED);
            keys.push(format!(r#""{key}": {json}"#));
            if let Some(text) = text {
                filed.push((key.to_string(), text.to_string()));
            }
        }
    }
    if rng.chance(2) {
        let (json, micros) = *rng.pick(TIMES);
        keys.push(format!(r#""time":{json}"#));
        filed.push(("time".to_string(), micros.to_string()));
    }
    let mut fields = None;
    if rng.chance(3) {
        let json = *rng.pick(FIELDS);
        keys.push(format!(r#""fields":{json}"#));
        // An empty object reads back as no fields, as `null` does: it indexes none.
        fields =
            Some(serde_json::from_str::<Value>(json).unwrap()).filter(|f| f.as_object().is_some_and(|f| !f.is_empty()));
    }
    if rng.chance(8) {
        labels.at(Stage::Body, "a key HEC does not name");
        keys.push(r#""extra":{"x":[1]}"#.to_string());
    }
    // The keys in any order: an envelope is read by name.
    for i in (1..keys.len()).rev() {
        keys.swap(i, rng.below(i + 1));
    }
    let whole = |keys: &[String]| format!("{{{}}}", keys.join(","));
    if !spoiled {
        let taken = Taken { raw, filed, fields };
        return (whole(&keys), Ok(taken));
    }
    let (spoil, code) = *rng.pick(SPOILS);
    labels.at(Stage::Body, spoil);
    let at = |keys: &[String], key: &str| keys.iter().position(|k| k.starts_with(&format!(r#""{key}""#)));
    let set = |keys: &mut Vec<String>, key: &str, json: &str| {
        let pair = format!(r#""{key}":{json}"#);
        match at(keys, key) {
            Some(i) => keys[i] = pair,
            None => keys.push(pair),
        }
    };
    let text = match spoil {
        "no event" => {
            keys.retain(|k| !k.starts_with(r#""event""#));
            whole(&keys)
        }
        "a blank event" => {
            set(&mut keys, "event", r#""""#);
            whole(&keys)
        }
        "an envelope that is no object" => rng.pick(&[r#"["event"]"#, r#""event""#, "42", "null"]).to_string(),
        "an envelope cut short" => {
            let text = whole(&keys);
            text[..text.len() - 1].to_string()
        }
        "a key given twice" => {
            keys.push(r#""event":"again""#.to_string());
            whole(&keys)
        }
        "a host that is no text" => {
            set(&mut keys, "host", rng.pick(&[r#"{"a":1}"#, "[]", "true"]));
            whole(&keys)
        }
        "a time that is no number" => {
            set(&mut keys, "time", rng.pick(&[r#""noon""#, "{}", "true", r#""""#]));
            whole(&keys)
        }
        _ => {
            set(&mut keys, "fields", rng.pick(&[r#"["a"]"#, r#""env""#, "1"]));
            whole(&keys)
        }
    };
    (text, Err(code))
}

/// What may stand between two envelopes.
const BETWEEN: &[&str] = &["", "\n", " ", "\r\n\t  "];

/// What a body comes to once read: the records it is, or the status it is refused with, with HEC's code and the
/// envelope it is refused for where HEC names them.
type BodyRead = Result<Vec<Taken>, (u16, Option<u16>, Option<usize>)>;

/// An event endpoint's body of envelopes, and what it comes to. The first spoiled envelope decides a refusal.
fn envelopes(rng: &mut Rng, spoiled: bool, labels: &mut Labels) -> (Vec<u8>, BodyRead) {
    let n = 1 + rng.below(5);
    let bad = spoiled.then(|| rng.below(n));
    let mut body = rng.pick(BETWEEN).to_string();
    let mut taken = Vec::new();
    let mut refused = None;
    for i in 0..n {
        if i > 0 {
            body.push_str(rng.pick(BETWEEN));
        }
        let (text, read) = envelope(rng, bad == Some(i), labels);
        body.push_str(&text);
        match read {
            Ok(t) => taken.push(t),
            Err(code) => refused = refused.or(Some((400, Some(code), Some(i)))),
        }
    }
    body.push_str(rng.pick(BETWEEN));
    (body.into_bytes(), refused.map_or(Ok(taken), Err))
}

/// Lines a raw body's events are made of. None holds a line end, starts with a date, or holds `</e>`.
const LINES: &[&str] = &[
    "GET /index.html 200",
    "  indented",
    "tab\tseparated",
    "été 😀",
    "2026 is a year",
    "{\"json\": true}",
    "trailing space ",
];

/// What ends a line.
const LINE_ENDS: &[&str] = &["\n", "\r\n", "\r"];

/// A raw body broken as `breaking` breaks it, and the events it is.
fn raw_events(rng: &mut Rng, breaking: Breaking) -> (Vec<u8>, Vec<String>) {
    let n = 1 + rng.below(5);
    let mut body = String::new();
    let mut events = Vec::new();
    match breaking {
        Breaking::Lines => {
            for _ in 0..n {
                // A blank line, or one of whitespace, is no event.
                if rng.chance(6) {
                    body.push_str(rng.pick(&["", " ", "\t "]));
                } else {
                    let line = *rng.pick(LINES);
                    body.push_str(line);
                    events.push(line.to_string());
                }
                body.push_str(rng.pick(LINE_ENDS));
            }
            // The last line needs no end.
            if rng.chance(2) {
                body.truncate(body.trim_end_matches(['\r', '\n']).len());
            }
        }
        Breaking::Xml => {
            for _ in 0..n {
                let inner: Vec<&str> = (0..1 + rng.below(3)).map(|_| *rng.pick(LINES)).collect();
                let event = format!("<e>{}</e>", inner.join(rng.pick(LINE_ENDS)));
                body.push_str(&event);
                events.push(event);
                body.push_str(rng.pick(&["", "\n", " \r\n\t"]));
            }
        }
        Breaking::Dated => {
            for i in 0..n {
                let mut event = format!("2026-10-{:02} event {i}", 1 + rng.below(28));
                for _ in 0..rng.below(3) {
                    event.push_str(rng.pick(LINE_ENDS));
                    event.push_str("    at a.b.C.run(C.java:1)");
                }
                body.push_str(&event);
                events.push(event);
                // Between events any run of line ends; after the last, one line end, which is dropped.
                if i + 1 < n {
                    for _ in 0..1 + rng.below(2) {
                        body.push_str(rng.pick(LINE_ENDS));
                    }
                } else if rng.chance(2) {
                    body.push_str(rng.pick(LINE_ENDS));
                }
            }
        }
    }
    (body.into_bytes(), events)
}

/// A channel as a request names it.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Named {
    Nothing,
    Header(&'static str),
    Query(&'static str),
    /// Both, the header's naming it.
    Both(&'static str, &'static str),
}

/// Channels a sender may name, of which only the first three are channels.
const CHANNELS: &[&str] = &[
    "0f8a5c2e-1b7d-4c39-9e61-3a2b4c5d6e7f",
    "c_1",
    " padded ",
    "not/a/channel",
    "",
    "a.b",
];

fn valid_channel(name: &str) -> bool {
    let name = name.trim();
    !name.is_empty()
        && name.len() <= 128
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

impl Named {
    /// The channel a request names, or `Err` where HEC refuses what it names.
    fn channel(self) -> Result<Option<String>, ()> {
        let named = match self {
            Named::Nothing => return Ok(None),
            Named::Header(c) | Named::Query(c) | Named::Both(c, _) => c,
        };
        match valid_channel(named) {
            true => Ok(Some(named.trim().to_string())),
            false => Err(()),
        }
    }
}

/// Where a request is sent, and how: to every path HEC serves, by the method it takes and another, and to paths near
/// them.
const ROUTES: &[(&str, &str)] = &[
    ("POST", "/services/collector/event"),
    ("POST", "/services/collector"),
    ("POST", "/services/collector/event/1.0"),
    ("POST", "/services/collector/raw"),
    ("POST", "/services/collector/raw/1.0"),
    ("POST", "/services/collector/ack"),
    ("POST", "/services/collector/ack/1.0"),
    ("POST", "/services/collector/s2s"),
    ("GET", "/services/collector/health"),
    ("GET", "/services/collector/health/1.0"),
    ("HEAD", "/services/collector/health"),
    ("POST", "/services/collector/health"),
    ("GET", "/services/collector/event"),
    ("PUT", "/services/collector/raw"),
    ("POST", "/services/collector/"),
    ("POST", "/services/collector/events"),
    ("POST", "/Services/Collector/Event"),
    ("POST", "/"),
];

/// What a route serves.
#[derive(Clone, Copy, PartialEq, Debug)]
enum Serves {
    Event,
    Raw,
    Ack,
    Httpout,
    Health,
    /// A path HEC serves, by a method it does not take.
    Unmethod,
    Nothing,
}

fn serves(method: &str, path: &str) -> Serves {
    // Each path but `s2s` is served under `/1.0` too.
    let path = match path.strip_suffix("/1.0") {
        Some(base) if base != "/services/collector/s2s" => base,
        _ => path,
    };
    match (method, path) {
        ("POST", "/services/collector" | "/services/collector/event") => Serves::Event,
        ("POST", "/services/collector/raw") => Serves::Raw,
        ("POST", "/services/collector/ack") => Serves::Ack,
        ("POST", "/services/collector/s2s") => Serves::Httpout,
        ("GET" | "HEAD", "/services/collector/health") => Serves::Health,
        (
            _,
            "/services/collector"
            | "/services/collector/event"
            | "/services/collector/raw"
            | "/services/collector/ack"
            | "/services/collector/s2s"
            | "/services/collector/health",
        ) => Serves::Unmethod,
        _ => Serves::Nothing,
    }
}

const AUTHORIZATIONS: &[&str] = &[
    "no authorization",
    "a token it does not hold",
    "a token as Bearer",
    "a scheme and no token",
    "a token and no scheme",
    "a token not ASCII",
    "a token after a scheme of any case and two spaces",
    "a token it holds",
];

const EVENT_BODIES: &[&str] = &[
    "envelopes",
    "envelopes, one spoiled",
    "an empty body",
    "a body of whitespace",
    "bytes",
    "a body past the limit",
];

const RAW_BODIES: &[&str] = &[
    "events",
    "an empty body",
    "a body of whitespace",
    "a body past the limit",
];

const HTTPOUT_BODIES: &[&str] = &["an httpout body of nothing", "bytes"];

const ENCODINGS: &[(Option<&str>, Option<bool>)] = &[
    (None, Some(false)),
    (Some("identity"), Some(false)),
    (Some("gzip"), Some(true)),
    (Some(" GZIP "), Some(true)),
    (Some("deflate"), None),
    (Some("gzip, identity"), None),
    (Some("br"), None),
];

/// A raw request's query: what it files its lines under, by key, as sent and as read.
const QUERY_VALUES: &[(&str, &str)] = &[
    ("web-1", "web-1"),
    ("web+1", "web 1"),
    ("%C3%A9t%C3%A9", "été"),
    ("a%20b", "a b"),
    ("", ""),
];

const RAW_TIMES: &[(&str, Option<i64>)] = &[
    ("1700000000.5", Some(1_700_000_000_500_000)),
    ("1700000000", Some(1_700_000_000_000_000)),
    ("noon", None),
    ("", None),
];

/// How far a receiver takes a request, in the order it takes it: its route, the token it presents, the channel it
/// names, its size and encoding as it is read, and what its body reads as.
#[derive(Clone, Copy, Debug, PartialEq, PartialOrd)]
enum Stage {
    Route,
    Token,
    Channel,
    Read,
    Body,
}

/// What a request tries, each by the stage the receiver acts on it at: a label is reached only by a request the
/// receiver took that far.
#[derive(Default)]
struct Labels(Vec<(Stage, String)>);

impl Labels {
    fn at(&mut self, stage: Stage, label: impl Into<String>) {
        self.0.push((stage, label.into()));
    }
}

/// A request's answer, as the receiver must give it.
#[derive(Debug)]
enum Expected {
    /// No HTTP at all: TLS refused the sender.
    Nothing,
    /// A status and, where HEC gives one, its code and the envelope it is refused for.
    Answer(u16, Option<u16>, Option<usize>),
    /// Success, these records taken, and an `ackId` given on this channel where the sender acknowledges.
    Success(Vec<Taken>, Option<String>),
    /// Whether each of the `ackId`s given on this channel so far reads true.
    Acks(String),
}

/// One request a sender makes, and what the receiver must answer it.
struct Sent {
    method: &'static str,
    path: String,
    /// Header lines, each ending in CRLF.
    headers: Vec<u8>,
    body: Vec<u8>,
    presents: Presents,
    expected: Expected,
    /// What it tries, and how far the receiver takes it: `None` where TLS refused it.
    labels: Labels,
    reached: Option<Stage>,
}

/// A random request to `settings`'s receiver, and what it must answer: the route, then the token, then the channel,
/// then the request's size and framing, then what its body reads as, as the receiver takes them in that order.
fn random_request(rng: &mut Rng, settings: &Settings, cov: &mut Coverage) -> Sent {
    // The paths HEC serves by POST, most of the time, so most requests reach the receiver's own checks.
    let (method, path) = match rng.chance(5) {
        true => *rng.pick(ROUTES),
        false => *rng.pick(&ROUTES[..8]),
    };
    cov.reach(format!("{method} {path}"));
    let serves = serves(method, path);
    let presents = match settings.tls {
        Tls::None => Presents::Nothing,
        _ => *rng.pick(&[
            Presents::Trusted,
            Presents::Trusted,
            Presents::Trusted,
            Presents::Nothing,
            Presents::Stranger,
        ]),
    };
    cov.reach(format!("presents {presents:?} under {:?}", settings.tls));
    // Most requests present a token the receiver holds, so most reach what follows the token.
    let well_sent = !rng.chance(4);

    let token = (!settings.tokens.is_empty()).then(|| rng.pick(&settings.tokens).clone());
    let right = token.as_ref().map_or(String::new(), |(t, _)| t.clone());
    let authorization = match well_sent {
        true => AUTHORIZATIONS.len() - 1 - rng.below(2),
        false => rng.below(AUTHORIZATIONS.len()),
    };
    let mut labels = Labels::default();
    labels.at(Stage::Token, AUTHORIZATIONS[authorization]);
    // An `Authorization` value, and what it comes to: admitted, or refused with HEC's status and code.
    let (authorization, refused): (Option<Vec<u8>>, Option<(u16, u16)>) = match authorization {
        0 => (None, Some((401, 2))),
        1 => (Some(format!("Splunk {right}x").into()), Some((403, 4))),
        2 => (Some(format!("Bearer {right}").into()), Some((401, 3))),
        3 => (Some(b"Splunk".to_vec()), Some((401, 3))),
        4 => (Some(right.clone().into()), Some((401, 3))),
        5 => (Some(b"Splunk \xe9\xff".to_vec()), Some((401, 3))),
        6 => (Some(format!("sPlUnK  {right} ").into()), None),
        _ => (Some(format!("Splunk {right}").into()), None),
    };
    // A receiver holding no token admits whoever TLS admitted.
    let refused = refused.filter(|_| token.is_some());
    let acknowledges = token.as_ref().is_some_and(|(_, acks)| *acks) && refused.is_none();

    // An acknowledging sender names its channel, and one that does not acknowledge may.
    let named = match (acknowledges && well_sent) || rng.chance(3) {
        true => {
            let pool = match well_sent {
                true => &CHANNELS[..3],
                false => CHANNELS,
            };
            match rng.below(3) {
                0 => Named::Header(rng.pick(pool)),
                1 => Named::Query(rng.pick(pool)),
                _ => Named::Both(rng.pick(pool), rng.pick(CHANNELS)),
            }
        }
        false => Named::Nothing,
    };
    let channel = named.channel();
    labels.at(
        Stage::Channel,
        match (named, &channel) {
            (Named::Nothing, _) => "no channel",
            (_, Err(())) => "a channel HEC refuses",
            (Named::Header(_), Ok(_)) => "a channel in the header",
            (Named::Query(_), Ok(_)) => "a channel in the query",
            (Named::Both(..), Ok(_)) => "a channel in both",
        },
    );

    // An ack query's body is built as it is sent, from the IDs given before it, so it is sent uncompressed.
    let (encoding, gzip) = match (serves, well_sent) {
        (Serves::Ack, _) => ENCODINGS[0],
        (_, true) => ENCODINGS[rng.below(4)],
        (_, false) => *rng.pick(ENCODINGS),
    };
    labels.at(Stage::Read, format!("Content-Encoding {encoding:?}"));
    let max = settings.max_request_bytes;
    let mut query: Vec<String> = Vec::new();

    // The body, and what it comes to once read: refused with a status and code, or taken as these records.
    let (body, read): (Vec<u8>, BodyRead) = match serves {
        Serves::Event => {
            let kind = match well_sent {
                true => rng.below(2),
                false => rng.below(EVENT_BODIES.len()),
            };
            // A body past the limit is refused for its size, before what it reads as.
            let stage = if kind == 5 { Stage::Read } else { Stage::Body };
            labels.at(stage, format!("event {}", EVENT_BODIES[kind]));
            match kind {
                0 | 1 => envelopes(rng, kind == 1, &mut labels),
                2 => (Vec::new(), Err((400, Some(5), None))),
                3 => (b" \r\n\t ".to_vec(), Err((400, Some(5), None))),
                // Bytes no JSON starts with.
                4 => {
                    let bytes = [0xff]
                        .into_iter()
                        .chain((0..rng.below(512)).map(|_| rng.next_u64() as u8));
                    (bytes.collect(), Err((400, Some(6), Some(0))))
                }
                _ => {
                    let event = format!(r#"{{"event":"{}"}}"#, "x".repeat(max + 1));
                    (event.into_bytes(), Err((413, None, None)))
                }
            }
        }
        Serves::Raw => {
            let kind = match well_sent {
                true => 0,
                false => rng.below(RAW_BODIES.len()),
            };
            let stage = if kind == 3 { Stage::Read } else { Stage::Body };
            labels.at(stage, format!("raw {}", RAW_BODIES[kind]));
            // What it names its sourcetype as, which decides how its body breaks.
            let sourcetype = *rng.pick(&[None, Some(Breaking::Lines), Some(Breaking::Xml), Some(Breaking::Dated)]);
            let breaking = sourcetype
                .filter(|b| settings.sourcetypes.contains(b))
                .unwrap_or(Breaking::Lines);
            labels.at(Stage::Body, format!("raw broken {breaking:?}"));
            let mut filed = Vec::new();
            for key in ["host", "source", "sourcetype", "index"] {
                let value = match key {
                    "sourcetype" => sourcetype.map(|b| (b.sourcetype().0, b.sourcetype().0)),
                    _ => rng.chance(3).then(|| *rng.pick(QUERY_VALUES)),
                };
                if let Some((sent, read)) = value {
                    query.push(format!("{key}={sent}"));
                    if !read.is_empty() {
                        filed.push((key.to_string(), read.to_string()));
                    }
                }
            }
            if rng.chance(3) {
                let (sent, micros) = *rng.pick(RAW_TIMES);
                query.push(format!("time={sent}"));
                if let Some(micros) = micros {
                    filed.push(("time".to_string(), micros.to_string()));
                }
            }
            match kind {
                0 => {
                    let (body, events) = raw_events(rng, breaking);
                    let taken = (events.into_iter())
                        .map(|raw| Taken {
                            raw,
                            filed: filed.clone(),
                            fields: None,
                        })
                        .collect();
                    // Lines of nothing but whitespace are no data.
                    match body.trim_ascii().is_empty() {
                        true => (body, Err((400, Some(5), None))),
                        false => (body, Ok(taken)),
                    }
                }
                1 => (Vec::new(), Err((400, Some(5), None))),
                2 => (b"\n \r\n\t".to_vec(), Err((400, Some(5), None))),
                _ => (vec![b'x'; max + 1], Err((413, None, None))),
            }
        }
        Serves::Httpout => {
            let kind = rng.below(HTTPOUT_BODIES.len());
            labels.at(Stage::Body, format!("httpout {}", HTTPOUT_BODIES[kind]));
            match kind {
                // A forwarder's stream that defines no channel and sends no event: answered once delivered.
                0 => {
                    let mut zlib = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
                    zlib.flush().unwrap();
                    ([&[0xf9][..], zlib.get_ref()].concat(), Ok(Vec::new()))
                }
                _ => {
                    let bytes = (0..rng.below(512)).map(|_| rng.next_u64() as u8);
                    (b"G".iter().copied().chain(bytes).collect(), Err((400, Some(6), None)))
                }
            }
        }
        Serves::Ack => {
            labels.at(Stage::Body, "an ack query");
            (Vec::new(), Ok(Vec::new()))
        }
        Serves::Health | Serves::Unmethod | Serves::Nothing => (Vec::new(), Ok(Vec::new())),
    };
    let inflated = body.len();
    let body = match gzip {
        Some(true) => {
            let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            encoder.write_all(&body).unwrap();
            encoder.finish().unwrap()
        }
        _ => body,
    };
    // A body that says it is gzip and is not, which `well_sent` never sends.
    let (body, mislabeled) = match (gzip, well_sent) {
        (Some(true), false) if rng.chance(3) => {
            labels.at(Stage::Read, "a body that is not the gzip it says");
            (b"not gzip".to_vec(), true)
        }
        _ => (body, false),
    };
    if let Named::Query(c) | Named::Both(_, c) = named {
        query.push(format!("channel={}", c.replace(' ', "%20").replace('/', "%2F")));
    }
    let path = match query.is_empty() {
        true => path.to_string(),
        false => format!("{path}?{}", query.join("&")),
    };

    let mut headers = Vec::new();
    if let Some(authorization) = authorization {
        headers.extend(b"Authorization: ");
        headers.extend(authorization);
        headers.extend(b"\r\n");
    }
    if let Named::Header(c) | Named::Both(c, _) = named {
        headers.extend(format!("X-Splunk-Request-Channel: {c}\r\n").bytes());
    }
    if let Some(encoding) = encoding {
        headers.extend(format!("Content-Encoding: {encoding}\r\n").bytes());
    }

    let refusal = |status, code| Expected::Answer(status, code, None);
    let (expected, reached) = if settings.tls == Tls::Vouched && presents != Presents::Trusted {
        (Expected::Nothing, None)
    } else {
        let (expected, reached) = match serves {
            Serves::Nothing => (refusal(404, None), Stage::Route),
            Serves::Unmethod => (refusal(405, None), Stage::Route),
            Serves::Health => (refusal(200, Some(17)), Stage::Route),
            _ if refused.is_some() => {
                let (status, code) = refused.unwrap();
                (refusal(status, Some(code)), Stage::Token)
            }
            Serves::Ack if !acknowledges => (refusal(400, Some(14)), Stage::Token),
            Serves::Ack => match channel {
                Err(()) => (refusal(400, Some(11)), Stage::Channel),
                Ok(None) => (refusal(400, Some(10)), Stage::Channel),
                Ok(Some(channel)) => (Expected::Acks(channel), Stage::Body),
            },
            _ => {
                // `httpout` names no channel, and its answer acks it whatever its token says.
                let channel = match serves {
                    Serves::Httpout => Ok(None),
                    _ => channel,
                };
                let acks = acknowledges && serves != Serves::Httpout;
                match (channel, acks) {
                    (Err(()), _) => (refusal(400, Some(11)), Stage::Channel),
                    (Ok(None), true) => (refusal(400, Some(10)), Stage::Channel),
                    _ if body.len() > max => (refusal(413, None), Stage::Read),
                    _ if gzip.is_none() => (refusal(415, None), Stage::Read),
                    _ if mislabeled => (refusal(400, Some(6)), Stage::Read),
                    _ if inflated > max => (refusal(413, None), Stage::Read),
                    (Ok(channel), acks) => match read {
                        Err((status, code, n)) => (Expected::Answer(status, code, n), Stage::Body),
                        Ok(taken) => (Expected::Success(taken, channel.filter(|_| acks)), Stage::Body),
                    },
                }
            }
        };
        (expected, Some(reached))
    };
    // A receiver holding no token reads none, and `httpout` reads no channel.
    labels.0.retain(|(stage, _)| match stage {
        Stage::Token => token.is_some(),
        Stage::Channel => serves != Serves::Httpout,
        _ => true,
    });
    Sent {
        method,
        path,
        headers,
        body,
        presents,
        expected,
        labels,
        reached,
    }
}

/// `sent` over a new connection to `at`, under TLS where `tls` says, with `body` as its body: an ack query's is built
/// as it is sent.
fn exchange(at: SocketAddr, tls: Tls, pki: &Pki, sent: &Sent, body: &[u8]) -> Answer {
    let request = Request {
        method: sent.method,
        path: &sent.path,
        headers: &sent.headers,
        body,
        chunked: false,
        presents: sent.presents,
    };
    receiver::exchange(at, tls, pki, &request)
}

/// What a HEC receiver documents it answers at `path` to `method`. 0 is no answer: a connection its TLS refused.
fn documented(method: &str, path: &str) -> &'static [u16] {
    match serves(method, path.split('?').next().unwrap()) {
        Serves::Event | Serves::Raw | Serves::Httpout => &[0, 200, 400, 401, 403, 413, 415, 503],
        Serves::Ack => &[0, 200, 400, 401, 403, 413, 415],
        Serves::Health => &[0, 200, 503],
        Serves::Unmethod => &[0, 405],
        Serves::Nothing => &[0, 404],
    }
}

/// The records as [`Taken`]s: their bytes, what their headers file them under, and their indexed fields.
fn read_back(records: &[SourceRecord]) -> Vec<Taken> {
    let objects = SourceMeta::HEC_RECEIVER.objects(records, Some("hec"));
    (records.iter().zip(objects))
        .map(|(r, object)| Taken {
            raw: String::from_utf8_lossy(r.bytes().unwrap()).to_string(),
            filed: (r.headers.iter().flatten())
                .filter(|(key, _)| key != "fields")
                .map(|(key, value)| (key.clone(), String::from_utf8_lossy(value).to_string()))
                .collect(),
            fields: object.get("fields").filter(|f| !f.is_null()).cloned(),
        })
        .collect()
}

/// What an ack query asks after on a channel where `given` were given: each of them, the first twice, and one never
/// given.
fn asked(given: &[u64]) -> Vec<u64> {
    let unknown = given.iter().max().map_or(7, |m| m + 1_000);
    given
        .iter()
        .copied()
        .chain(given.first().copied())
        .chain([unknown])
        .collect()
}

fn ack_body(ids: &[u64]) -> Vec<u8> {
    json!({ "acks": ids }).to_string().into_bytes()
}

/// What one seed's sender heard: each answer, and the `ackId`s it was given and heard true, by channel.
struct Heard {
    answers: Vec<Answer>,
    /// By channel, each `ackId` given there, in order.
    given: BTreeMap<String, Vec<u64>>,
    /// By channel, each `ackId` that read true, and so is forgotten.
    heard_true: BTreeMap<String, BTreeSet<u64>>,
}

/// Sends each of `sent` in order, as one sender does. An ack query asks after what its channel was given before it.
fn send_all(at: SocketAddr, tls: Tls, pki: &Pki, sent: &[Sent]) -> Heard {
    let mut heard = Heard {
        answers: Vec::new(),
        given: BTreeMap::new(),
        heard_true: BTreeMap::new(),
    };
    for s in sent {
        let answer = match &s.expected {
            Expected::Acks(channel) => {
                let given = heard.given.get(channel).cloned().unwrap_or_default();
                exchange(at, tls, pki, s, &ack_body(&asked(&given)))
            }
            _ => exchange(at, tls, pki, s, &s.body),
        };
        let json: Value = serde_json::from_str(&answer.body).unwrap_or(Value::Null);
        if let Expected::Success(_, Some(channel)) = &s.expected
            && let Some(id) = json["ackId"].as_u64()
        {
            heard.given.entry(channel.clone()).or_default().push(id);
        }
        if let (Expected::Acks(channel), Some(acks)) = (&s.expected, json["acks"].as_object()) {
            let trues = acks.iter().filter(|(_, v)| **v == Value::Bool(true));
            let ids = trues.filter_map(|(id, _)| id.parse::<u64>().ok());
            heard.heard_true.entry(channel.clone()).or_default().extend(ids);
        }
        heard.answers.push(answer);
    }
    heard
}

/// Why `answer` is not what `sent` expected of a receiver reading at most `max` bytes, or `Ok`. `given` is each
/// `ackId` given on each channel before it.
fn check_answer(sent: &Sent, answer: &Answer, max: usize, given: &BTreeMap<String, Vec<u64>>) -> Result<(), String> {
    let json: Value = serde_json::from_str(&answer.body).unwrap_or(Value::Null);
    let status = answer.status;
    let expected = match &sent.expected {
        Expected::Acks(channel) => {
            let ids = asked(given.get(channel).map_or(&[][..], Vec::as_slice));
            match ack_body(&ids).len() > max {
                true => &Expected::Answer(413, None, None),
                false => &sent.expected,
            }
        }
        expected => expected,
    };
    match expected {
        Expected::Nothing if status == 0 => Ok(()),
        Expected::Answer(200, Some(17), None) if sent.method == "HEAD" && status == 200 => Ok(()),
        Expected::Answer(want, code, n) if status == *want => {
            let named_ok = n.is_none_or(|n| json["invalid-event-number"] == n);
            match code {
                Some(code) if json["code"] != *code || !json["text"].is_string() || !named_ok => {
                    Err(format!("answered {status} {json}, not code {code} at {n:?}"))
                }
                None if ![404, 405].contains(want) && !json["text"].is_string() => {
                    Err(format!("answered {status} {:?}, which says nothing", answer.body))
                }
                _ => Ok(()),
            }
        }
        Expected::Success(_, ack) if status == 200 => {
            let id = json["ackId"].as_u64();
            match (
                json["code"] == 0 && json["text"] == "Success",
                ack.is_some(),
                id.is_some(),
            ) {
                (true, true, true) | (true, false, false) => Ok(()),
                _ => Err(format!("answered {json}, acknowledging {}", ack.is_some())),
            }
        }
        Expected::Acks(channel) if status == 200 => {
            let ids = asked(given.get(channel).map_or(&[][..], Vec::as_slice));
            let unknown = ids.last().unwrap().to_string();
            let answered = json["acks"].as_object().cloned().unwrap_or_default();
            let keys: BTreeSet<&String> = answered.keys().collect();
            let asked: BTreeSet<String> = ids.iter().map(u64::to_string).collect();
            if keys != asked.iter().collect()
                || !answered.values().all(Value::is_boolean)
                || answered[&unknown] != false
            {
                return Err(format!("asked after {asked:?}, answered {json}"));
            }
            Ok(())
        }
        expected => Err(format!("answered {status} {:?}, not {expected:?}", answer.body)),
    }
}

/// An ack query on `channel`, presenting `token`, as an acknowledging sender sends it.
fn ack_query(token: &str, channel: &str) -> Sent {
    Sent {
        method: "POST",
        path: "/services/collector/ack".to_string(),
        headers: format!("Authorization: Splunk {token}\r\nX-Splunk-Request-Channel: {channel}\r\n").into_bytes(),
        body: Vec::new(),
        presents: Presents::Trusted,
        expected: Expected::Acks(channel.to_string()),
        labels: Labels::default(),
        reached: Some(Stage::Body),
    }
}

/// A receiver under any settings an operator can save, sent random requests to every path, answers each as HEC does,
/// takes exactly the records of the requests it answers Success, filed as they say, and reads each `ackId` it gave true
/// once its records are committed.
#[test]
fn every_setting_answers_random_hec_requests_as_hec_does() {
    crate::certs::install_provider();
    let pki = Arc::new(Pki::new());
    let ipv6 = receiver::ipv6();
    let statuses = [0, 200, 400, 401, 403, 404, 405, 413, 415];
    let mut cov = Coverage::new(
        receiver::listen_names(ipv6)
            .map(|ip| format!("listen {ip}"))
            .chain(["None", "Served", "Vouched"].map(|tls| format!("tls {tls}")))
            .chain((0..=3).map(|n| format!("tokens {n}")))
            .chain(["an acknowledging token", "a token that does not acknowledge"].map(String::from))
            .chain(RECEIVER_LIMITS.iter().map(|(_, name)| name.to_string()))
            .chain(ROUTES.iter().map(|(method, path)| format!("{method} {path}")))
            .chain(["presents Nothing under None".to_string()])
            .chain(["Served", "Vouched"].iter().flat_map(|tls| {
                ["Nothing", "Trusted", "Stranger"].map(|presents| format!("presents {presents} under {tls}"))
            }))
            .chain(AUTHORIZATIONS.iter().map(|s| s.to_string()))
            .chain(EVENT_BODIES.iter().map(|s| format!("event {s}")))
            .chain(RAW_BODIES.iter().map(|s| format!("raw {s}")))
            .chain(HTTPOUT_BODIES.iter().map(|s| format!("httpout {s}")))
            .chain(["Lines", "Xml", "Dated"].map(|b| format!("raw broken {b}")))
            .chain(SPOILS.iter().map(|(s, _)| s.to_string()))
            .chain(ENCODINGS.iter().map(|(e, _)| format!("Content-Encoding {e:?}")))
            .chain(
                [
                    "no channel",
                    "a channel HEC refuses",
                    "a channel in the header",
                    "a channel in the query",
                    "a channel in both",
                    "a key HEC does not name",
                    "a body that is not the gzip it says",
                    "an ack query",
                    "an ackId read true",
                    "records of a request answered Success",
                ]
                .map(String::from),
            )
            .chain(statuses.iter().map(|status| format!("answered {status}"))),
    );
    fuzz::run(1..201, |seed| {
        let mut rng = Rng::seeded(seed);
        let settings = random_settings(&mut rng, ipv6);
        cov.reach(format!("listen {}", settings.listen.ip()));
        cov.reach(format!("tls {:?}", settings.tls));
        cov.reach(format!("tokens {}", settings.tokens.len()));
        for (_, acks) in &settings.tokens {
            cov.reach(if *acks {
                "an acknowledging token"
            } else {
                "a token that does not acknowledge"
            });
        }
        let limit = RECEIVER_LIMITS
            .iter()
            .find(|(below, _)| settings.max_request_bytes < *below)
            .unwrap();
        cov.reach(limit.1);
        let mut source = open(&settings, &pki);
        let sent: Vec<Sent> = (0..12).map(|_| random_request(&mut rng, &settings, &mut cov)).collect();
        let sending = Arc::new(sent);
        let (connect, tls) = (settings.connect, settings.tls);
        let requests = sending.clone();
        let senders_pki = pki.clone();
        let sender = std::thread::spawn(move || send_all(connect, tls, &senders_pki, &requests));
        // A held request is answered once its records are committed, so they are taken as they arrive.
        let deadline = Instant::now() + Duration::from_secs(60);
        let mut records = Vec::new();
        while !sender.is_finished() {
            if Instant::now() > deadline {
                return Err("the sender is still waiting".into());
            }
            let batch = source.poll_batch(10_000, 1 << 26, Duration::from_millis(10)).unwrap();
            commit(&mut source, &batch);
            records.extend(read_back(&batch));
        }
        let heard = sender.join().unwrap();
        let batch = source.poll_batch(10_000, 1 << 26, Duration::from_millis(50)).unwrap();
        commit(&mut source, &batch);
        records.extend(read_back(&batch));

        let mut given: BTreeMap<String, Vec<u64>> = BTreeMap::new();
        let mut expected_records = Vec::new();
        for (i, (sent, answer)) in sending.iter().zip(&heard.answers).enumerate() {
            let what = format!(
                "request {i} ({} {} under {:?}, presenting {:?}, {} bytes, limit {}, headers {:?})",
                sent.method,
                sent.path,
                settings.tls,
                sent.presents,
                sent.body.len(),
                settings.max_request_bytes,
                String::from_utf8_lossy(&sent.headers),
            );
            if !documented(sent.method, &sent.path).contains(&answer.status) {
                return Err(format!(
                    "{what}: answered {}, which it does not document",
                    answer.status
                ));
            }
            check_answer(sent, answer, settings.max_request_bytes, &given).map_err(|why| format!("{what}: {why}"))?;
            cov.reach(format!("answered {}", answer.status));
            // An ack query refused for its size never reached its body.
            let reached = match (&sent.expected, answer.status) {
                (Expected::Acks(_), 413) => Some(Stage::Read),
                _ => sent.reached,
            };
            for (stage, label) in &sent.labels.0 {
                if reached.is_some_and(|reached| *stage <= reached) {
                    cov.reach(label.clone());
                }
            }
            if let Expected::Success(taken, ack) = &sent.expected {
                if !taken.is_empty() {
                    cov.reach("records of a request answered Success");
                }
                expected_records.extend(taken.iter().cloned());
                if let Some(channel) = ack {
                    let id = serde_json::from_str::<Value>(&answer.body).unwrap()["ackId"]
                        .as_u64()
                        .unwrap();
                    given.entry(channel.clone()).or_default().push(id);
                }
            }
        }
        if records != expected_records {
            return Err(format!(
                "took {} records, not the {} of the requests answered Success:\ntook     {records:?}\nexpected \
                 {expected_records:?}",
                records.len(),
                expected_records.len()
            ));
        }

        // Every record is committed, so every ID given reads true once, but those already heard true. Each is asked
        // after alone, so the query is within any limit.
        let acker = settings.tokens.iter().find(|(_, acks)| *acks).map(|(t, _)| t.as_str());
        for (channel, ids) in &heard.given {
            let query = ack_query(acker.expect("an ackId was given"), channel);
            let heard_true = heard.heard_true.get(channel).cloned().unwrap_or_default();
            for id in ids {
                let deadline = Instant::now() + Duration::from_secs(10);
                let read = loop {
                    let answer = exchange(settings.connect, settings.tls, &pki, &query, &ack_body(&[*id]));
                    let json: Value = serde_json::from_str(&answer.body).unwrap_or(Value::Null);
                    let read = json["acks"][id.to_string()].clone();
                    if read == !heard_true.contains(id) || Instant::now() > deadline {
                        break read;
                    }
                    std::thread::sleep(Duration::from_millis(50));
                };
                if read != !heard_true.contains(id) {
                    return Err(format!(
                        "asked after {id} on {channel} once delivered, heard true before: {}; read {read}",
                        heard_true.contains(id)
                    ));
                }
                if read == true {
                    cov.reach("an ackId read true");
                }
            }
        }
        Ok(())
    });
    cov.assert_complete();
}

/// A real sender of HEC.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Sender {
    /// The OpenTelemetry Collector's `splunk_hec` exporter, tailing a file.
    Collector,
    /// Fluent Bit's `splunk` output, tailing a file.
    FluentBit,
}

/// What a live seed told its sender, and so what each record it sends must carry.
#[derive(Debug, Default)]
struct Told {
    /// Raw events rather than envelopes.
    raw: bool,
    gzip: bool,
    host: Option<&'static str>,
    source: Option<&'static str>,
    sourcetype: Option<&'static str>,
    index: Option<&'static str>,
    /// An indexed field it adds to each event, holding the event's line.
    field: Option<&'static str>,
    channel: Option<String>,
    /// The most a request may hold, where the sender splits by size.
    request_bytes: Option<usize>,
}

/// The collector's configuration: tail `app.log`, and export to `at` as `told` says, over TLS when `tls` is not None.
fn collector_config(at: SocketAddr, token: &str, tls: Tls, told: &Told) -> String {
    let scheme = if tls == Tls::None { "http" } else { "https" };
    // The exporter sends to its endpoint as given, so raw events go to the raw endpoint.
    let path = if told.raw {
        "/services/collector/raw"
    } else {
        "/services/collector"
    };
    let mut exporter = format!(
        "    endpoint: {scheme}://localhost:{}{path}\n    token: {token}\n    disable_compression: {}\n    \
         export_raw: {}\n    sending_queue: {{enabled: false}}\n    retry_on_failure: {{enabled: true, initial_interval: \
         100ms, max_interval: 1s}}\n",
        at.port(),
        !told.gzip,
        told.raw
    );
    for (key, value) in [
        ("source", told.source),
        ("sourcetype", told.sourcetype),
        ("index", told.index),
    ] {
        if let Some(value) = value {
            exporter.push_str(&format!("    {key}: {value}\n"));
        }
    }
    if let Some(bytes) = told.request_bytes {
        exporter.push_str(&format!("    max_content_length_logs: {bytes}\n"));
    }
    match tls {
        Tls::None => {}
        Tls::Served => exporter.push_str("    tls: {ca_file: /smeltr/ca.pem}\n"),
        Tls::Vouched => exporter.push_str(
            "    tls: {ca_file: /smeltr/ca.pem, cert_file: /smeltr/sender.pem, key_file: /smeltr/sender-key.pem}\n",
        ),
    }
    format!(
        "receivers:\n  filelog:\n    include: [/smeltr/app.log]\n    start_at: beginning\nexporters:\n  splunk_hec:\n\
         {exporter}service:\n  telemetry:\n    metrics: {{level: none}}\n  pipelines:\n    logs:\n      receivers: \
         [filelog]\n      exporters: [splunk_hec]\n"
    )
}

/// Fluent Bit's configuration: tail `app.log`, read each line as JSON where it sends envelopes raw, and send to `at`
/// as `told` says, over TLS when `tls` is not None.
fn fluent_bit_config(at: SocketAddr, token: &str, tls: Tls, told: &Told) -> String {
    let parser = if told.raw { "    Parser json\n" } else { "" };
    let mut output = format!(
        "[OUTPUT]\n    Name splunk\n    Match *\n    Host localhost\n    Port {}\n    Splunk_Token {token}\n    \
         Splunk_Send_Raw {}\n    Retry_Limit 10\n",
        at.port(),
        if told.raw { "On" } else { "Off" }
    );
    for (key, value) in [
        ("Event_Host", told.host),
        ("Event_Source", told.source),
        ("Event_Sourcetype", told.sourcetype),
        ("Event_Index", told.index),
    ] {
        if let Some(value) = value {
            output.push_str(&format!("    {key} {value}\n"));
        }
    }
    if let Some(name) = told.field {
        output.push_str(&format!("    Event_Field {name} $log\n"));
    }
    if told.gzip {
        output.push_str("    Compress gzip\n");
    }
    if let Some(channel) = &told.channel {
        output.push_str(&format!("    Channel {channel}\n"));
    }
    if tls != Tls::None {
        output.push_str("    TLS On\n    TLS.Verify On\n    TLS.CA_File /smeltr/ca.pem\n    TLS.Vhost localhost\n");
    }
    if tls == Tls::Vouched {
        output.push_str("    TLS.Crt_File /smeltr/sender.pem\n    TLS.Key_File /smeltr/sender-key.pem\n");
    }
    format!(
        "[SERVICE]\n    Flush 1\n    Grace 1\n    Log_Level info\n    Parsers_File /fluent-bit/etc/parsers.conf\n\
         [INPUT]\n    Name tail\n    Path /smeltr/app.log\n    Read_from_Head On\n{parser}{output}"
    )
}

/// A real OpenTelemetry Collector or Fluent Bit tails a file of random lines and sends them to receivers of random
/// settings, under random configurations of its own: envelopes or raw, gzip or not, filed under what it is told or
/// not, plain, over TLS, or over TLS with a client certificate. Every line arrives exactly once, filed as the sender
/// was told, the receiver never panics, and the sender logs no failure to send.
#[test]
fn live_senders_deliver_every_line_once_under_random_configurations() {
    const TEST: &str = "live_senders_deliver_every_line_once_under_random_configurations";
    let (Some(collector), Some(fluent_bit)) = (crate::live::otel_collector(TEST), crate::live::fluent_bit(TEST)) else {
        return;
    };
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
    crate::certs::install_provider();
    let pki = Pki::new();
    let ipv6 = receiver::ipv6();
    let options = [
        "raw",
        "gzip",
        "host",
        "source",
        "sourcetype",
        "index",
        "field",
        "channel",
        "request bytes",
    ];
    let mut cov = Coverage::new(
        [Sender::Collector, Sender::FluentBit]
            .iter()
            .flat_map(|s| ["None", "Served", "Vouched"].map(|tls| format!("{s:?} under {tls}")))
            .chain(options.iter().flat_map(|o| [format!("{o} on"), format!("{o} off")]))
            .chain(["an acknowledging token".to_string()]),
    );

    fuzz::run(1..25, |seed| {
        // A panic is the seed's own receiver's only while one seed runs.
        panicked.store(false, std::sync::atomic::Ordering::Relaxed);
        let mut rng = Rng::seeded(seed);
        let sender = match seed % 2 {
            0 => Sender::Collector,
            _ => Sender::FluentBit,
        };
        let mut settings = random_settings(&mut rng, ipv6);
        // A sender reaches the receiver over IPv4, on the host's network, by the name its certificate is for.
        settings.connect = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), settings.connect.port());
        if !settings.listen.ip().is_unspecified() {
            settings.listen = settings.connect;
        }
        settings.max_request_bytes = *rng.pick(&[1 << 20, 8 << 20]);
        // Only Fluent Bit names a channel, so only its token may acknowledge.
        let acknowledges = sender == Sender::FluentBit && rng.chance(2);
        let token = format!("tok-{:x}", rng.next_u64());
        settings.tokens = vec![(token.clone(), acknowledges)];
        if acknowledges {
            cov.reach("an acknowledging token");
        }
        cov.reach(format!("{sender:?} under {:?}", settings.tls));
        let told_raw = rng.chance(2);
        // The collector sends a raw event bare, so it is told nothing to file it under.
        let files = sender == Sender::FluentBit || !told_raw;
        let told = Told {
            raw: told_raw,
            gzip: rng.chance(2),
            host: (sender == Sender::FluentBit && rng.chance(2)).then_some("web-1"),
            source: (files && rng.chance(2)).then_some("/var/log/app.log"),
            sourcetype: (files && rng.chance(2)).then_some("fuzz:log"),
            index: (files && rng.chance(2)).then_some("main"),
            field: (sender == Sender::FluentBit && !told_raw && rng.chance(2)).then_some("line"),
            channel: acknowledges.then(|| format!("0f8a5c2e-1b7d-4c39-9e61-{:012x}", rng.next_u64() >> 16)),
            request_bytes: (sender == Sender::Collector && rng.chance(2)).then(|| *rng.pick(&[1024, 8192])),
        };
        for (option, on) in options.iter().zip([
            told.raw,
            told.gzip,
            told.host.is_some(),
            told.source.is_some(),
            told.sourcetype.is_some(),
            told.index.is_some(),
            told.field.is_some(),
            told.channel.is_some(),
            told.request_bytes.is_some(),
        ]) {
            cov.reach(format!("{option} {}", if on { "on" } else { "off" }));
        }
        let what = format!(
            "{sender:?} under {:?}, listening on {}, told {told:?}",
            settings.tls, settings.listen
        );
        let mut source = open(&settings, &pki);

        let scratch = Scratch(std::env::temp_dir().join(format!("smeltr-hec-fuzz-{}-{seed}", std::process::id())));
        let dir = &scratch.0;
        std::fs::create_dir_all(dir).unwrap();
        let n = 1 + rng.below(300);
        // Each line names itself; Fluent Bit reads a raw line as the envelope it sends.
        let lines: Vec<String> = (0..n)
            .map(|i| format!("line {seed}-{i} été-{}", "x".repeat(rng.below(200))))
            .collect();
        // Fluent Bit sends a raw record as the envelope it is, filed under what the envelope names.
        let envelope = |line: &String| {
            let mut envelope = json!({ "event": line });
            for (key, value) in [
                ("host", told.host),
                ("source", told.source),
                ("sourcetype", told.sourcetype),
                ("index", told.index),
            ] {
                if let Some(value) = value {
                    envelope[key] = value.into();
                }
            }
            envelope
        };
        let file: String = match (sender, told.raw) {
            (Sender::FluentBit, true) => lines.iter().map(|l| format!("{}\n", envelope(l))).collect(),
            _ => lines.iter().map(|l| format!("{l}\n")).collect(),
        };
        std::fs::write(dir.join("app.log"), file).unwrap();
        std::fs::write(dir.join("ca.pem"), &pki.authority.pem).unwrap();
        std::fs::write(dir.join("sender.pem"), &pki.sender.certificate).unwrap();
        std::fs::write(dir.join("sender-key.pem"), &pki.sender.key).unwrap();
        let (image, config, args): (&str, _, Vec<&str>) = match sender {
            Sender::Collector => (
                &collector,
                (
                    "config.yaml",
                    collector_config(settings.connect, &token, settings.tls, &told),
                ),
                vec!["--config", "/smeltr/config.yaml"],
            ),
            Sender::FluentBit => (
                &fluent_bit,
                (
                    "fluent-bit.conf",
                    fluent_bit_config(settings.connect, &token, settings.tls, &told),
                ),
                vec!["/fluent-bit/bin/fluent-bit", "-c", "/smeltr/fluent-bit.conf"],
            ),
        };
        std::fs::write(dir.join(config.0), &config.1).unwrap();
        share(dir);
        let name = format!("smeltr-hec-fuzz-{}-{seed}", std::process::id());
        let started = std::process::Command::new("docker")
            .args(["run", "-d", "--rm", "--network", "host", "--name", &name, "-v"])
            .arg(format!("{}:/smeltr:ro", dir.display()))
            .arg(image)
            .args(&args)
            .output()
            .expect("docker runs");
        if !started.status.success() {
            return Err(format!("docker run: {}", String::from_utf8_lossy(&started.stderr)));
        }
        let container = Container(name);

        let meta = SourceMeta::HEC_RECEIVER;
        let mut arrived: BTreeMap<String, usize> = BTreeMap::new();
        let deadline = Instant::now() + Duration::from_secs(90);
        let take = |batch: &[SourceRecord]| {
            for (record, object) in batch.iter().zip(meta.objects(batch, Some("hec"))) {
                if std::env::var("SMELTR_HEC_FUZZ_SHOW").is_ok() {
                    eprintln!("{:?} {object:?}", String::from_utf8_lossy(record.bytes().unwrap()));
                }
                let line = check_live_record(sender, &told, record, &object, &lines)
                    .map_err(|why| format!("{what}: {why}: {object:?}"))?;
                *arrived.entry(line).or_default() += 1;
            }
            if panicked.load(std::sync::atomic::Ordering::Relaxed) {
                return Err(format!("{what}: the receiver panicked"));
            }
            Ok(arrived.len() == n)
        };
        // Once every line is in, a little longer, for any sent again.
        if !receiver::drain(
            &mut source,
            Duration::from_millis(200),
            Duration::from_secs(3),
            deadline,
            take,
        )? {
            return Err(format!(
                "{what}: {} of {n} lines arrived\n{}",
                arrived.len(),
                container_logs(&container.0)
            ));
        }
        if let Some((line, times)) = arrived.iter().find(|(_, times)| **times > 1) {
            return Err(format!("{what}: {line:?} arrived {times} times"));
        }
        let logs = container_logs(&container.0);
        let failed = ["Exporting failed", "\terror\t", "[error]", "[ warn]"];
        if failed.iter().any(|f| logs.contains(f)) {
            return Err(format!("{what}: the sender logged a failure:\n{logs}"));
        }
        Ok(())
    });
    cov.assert_complete();
}

/// The line `record` carries, of those the sender tailed, or why it is not one filed as the sender was told.
fn check_live_record(
    sender: Sender,
    told: &Told,
    record: &SourceRecord,
    object: &serde_json::Map<String, Value>,
    lines: &[String],
) -> Result<String, String> {
    let raw = String::from_utf8_lossy(record.bytes().unwrap()).to_string();
    // Fluent Bit sends a tailed line as a record holding it under `log`; a raw line, read as JSON, is the envelope.
    let line = match (sender, told.raw) {
        (Sender::FluentBit, false) => {
            let event: Value = serde_json::from_str(&raw).map_err(|e| format!("an event that is not JSON: {e}"))?;
            event["log"].as_str().ok_or("an event holding no log")?.to_string()
        }
        _ => raw,
    };
    if !lines.contains(&line) {
        return Err(format!("{line:?} is no line the sender tailed"));
    }
    let filed = |key: &str| object.get(key).and_then(Value::as_str);
    for (key, told) in [
        ("host", told.host),
        ("source", told.source),
        ("sourcetype", told.sourcetype),
        ("index", told.index),
    ] {
        if told.is_some() && filed(key) != told {
            return Err(format!("filed under {key} {:?}, not {told:?}", filed(key)));
        }
    }
    if let Some(name) = told.field
        && object.get("fields").and_then(|f| f.get(name)).and_then(Value::as_str) != Some(&line)
    {
        return Err(format!("its fields lack {name}"));
    }
    Ok(line)
}
