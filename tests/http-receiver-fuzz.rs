//! Random requests against HTTP receivers of random settings. A sender drops what a receiver answers with a 4xx and
//! sends again what it answers busy, so each answer must say what became of the request, and the records of each
//! request answered 200 must arrive once each, cut from its body as the receiver is set to cut them: the body whole,
//! each line, or each element of an array.
//!
//! Each seed opens a receiver under settings an operator could save and sends it random requests, to its path and to
//! paths beside it: bodies of every shape each way of cutting reads, whole or spoiled, under every way of presenting a
//! token, plain or gzip, within its limit or past it. A model built beside each request says what the receiver must
//! answer and which records it must give. A deterministic PRNG over a seed range, from `harness::fuzz`: a failure names
//! its seed, and the fuzzer checks that its seeds reached every option it knows.

use std::collections::BTreeMap;
use std::io::Write;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::Value;
use smeltr::io::{Source, SourceRecord};
use smeltr::plan::topology::{ApiRecords, HttpReceiverConfig, Receiver, SourceMeta};

use crate::harness::docker::{Container, Hook, Scratch, container_logs, share};
use crate::harness::fuzz::{self, Coverage, Rng};
use crate::harness::receiver::{self, Answer, Pki, Presents, Request, Tls, commit};

/// One receiver's settings, as an operator could save them.
struct Settings {
    listen: SocketAddr,
    /// Where a sender reaches it.
    connect: SocketAddr,
    tokens: Vec<String>,
    tls: Tls,
    max_request_bytes: usize,
    /// The path it takes requests at, as saved and as a sender writes it in a request.
    path: (&'static str, &'static str),
    records: ApiRecords,
}

/// The limits a receiver is given, by the bound each is below.
const RECEIVER_LIMITS: &[(usize, &str)] = &[
    (4 << 10, "limit under 4 KiB"),
    (64 << 10, "limit under 64 KiB"),
    (usize::MAX, "limit of 64 KiB or more"),
];

/// Paths a receiver may be saved with, and each as a sender writes it in a request.
const PATHS: &[(&str, &str)] = &[
    ("/", "/"),
    ("/ingest", "/ingest"),
    ("/v1/logs", "/v1/logs"),
    ("/a/b/", "/a/b/"),
    ("/health/", "/health/"),
    ("/x.y~z_-", "/x.y~z_-"),
    ("/logs-été", "/logs-%C3%A9t%C3%A9"),
    ("/logs-été", "/logs-%c3%a9t%c3%a9"),
];

/// Where an array's records are, as a JSON pointer.
const POINTERS: &[&str] = &["", "/data", "/a~1b", "/d~0x", "/items/1"];

fn random_settings(rng: &mut Rng, ipv6: bool) -> Settings {
    let (listen, connect) = receiver::random_addresses(rng, ipv6);
    let port = receiver::free_port(connect);
    let tls = receiver::random_tls(rng);
    let tokens = receiver::random_tokens(rng, tls, |rng, i| format!("tok{i}-{:x}", rng.next_u64()));
    let records = match rng.below(3) {
        0 => ApiRecords::Body,
        1 => ApiRecords::Lines,
        _ => ApiRecords::Array(rng.pick(POINTERS).to_string()),
    };
    Settings {
        listen: SocketAddr::new(listen, port),
        connect: SocketAddr::new(connect, port),
        tokens,
        tls,
        max_request_bytes: receiver::random_limit(rng, 11),
        path: *rng.pick(PATHS),
        records,
    }
}

fn open(settings: &Settings, pki: &Pki) -> Box<dyn Source> {
    let http = Receiver::Http(HttpReceiverConfig {
        path: settings.path.0.to_string(),
        records: settings.records.clone(),
    });
    let Settings {
        listen,
        tokens,
        tls,
        max_request_bytes,
        ..
    } = settings;
    receiver::build(http, *listen, *tls, tokens, "sender", *max_request_bytes, pki)
}

/// Where a request is sent, beside the path the receiver takes requests at.
const ROUTES: &[(&str, &str)] = &[
    ("POST", "its path"),
    ("POST", "its path with a query"),
    ("GET", "its path"),
    ("PUT", "its path"),
    ("GET", "/health"),
    ("POST", "/health"),
    ("POST", "its path and a slash"),
    ("POST", "its path with a slash encoded"),
    ("POST", "elsewhere"),
];

/// What a route serves.
#[derive(Clone, Copy, PartialEq, Debug)]
enum Serves {
    Records,
    Health,
    /// A path the receiver serves, by a method it does not take.
    Unmethod,
    Nothing,
}

const AUTHORIZATIONS: &[&str] = &[
    "no authorization",
    "a token it does not hold",
    "a scheme a receiver does not take",
    "a scheme and no token",
    "a token not ASCII",
    "a token and no scheme",
    "a token as Bearer",
    "a token after a scheme of any case and two spaces",
];

/// The first of `AUTHORIZATIONS` a receiver admits.
const ADMITTED: usize = 6;

const ENCODINGS: &[(Option<&str>, Option<bool>)] = &[
    (None, Some(false)),
    (Some("identity"), Some(false)),
    (Some("gzip"), Some(true)),
    (Some("GZip"), Some(true)),
    (Some("deflate"), None),
    (Some("gzip, identity"), None),
    (Some("br"), None),
];

/// Bodies of a receiver that takes a body whole.
const WHOLE_BODIES: &[&str] = &[
    "a body of text",
    "a body of bytes",
    "a body of JSON",
    "a body of whitespace",
    "an empty body",
    "a body at the limit",
    "a body past the limit",
];

/// Bodies of a receiver that takes each line.
const LINE_BODIES: &[&str] = &[
    "lines",
    "lines ending CRLF",
    "lines and blank lines",
    "a last line with no line end",
    "a line holding a carriage return",
    "lines of bytes",
    "a line ending in a carriage return before CRLF",
    "a body of whitespace",
    "an empty body",
    "a body at the limit",
    "a body past the limit",
];

/// Bodies of a receiver that takes each element of an array.
const ARRAY_BODIES: &[&str] = &[
    "an array",
    "an empty array",
    "an array of nested values",
    "an array spaced and broken over lines",
    "a body that is not JSON",
    "JSON cut short",
    "JSON with bytes not UTF-8",
    "no value at the pointer",
    "null at the pointer",
    "a value at the pointer that is not an array",
    "a pointer naming a member of an array",
    "a pointer stepping through a scalar",
    "an empty body",
    "a body at the limit",
    "a body past the limit",
];

/// Whether the body of `ARRAY_BODIES` at `kind` can be made for a receiver taking the array at `pointer`: the top of a
/// document is always a value, and only a pointer of two steps has a step partway.
fn makes(kind: usize, pointer: &str) -> bool {
    match ARRAY_BODIES[kind] {
        "no value at the pointer" | "a pointer naming a member of an array" => !pointer.is_empty(),
        "a pointer stepping through a scalar" => pointer.matches('/').count() > 1,
        _ => true,
    }
}

/// Ways of sending a gzip body: whole, or spoiled.
const GZIPS: &[&str] = &[
    "a body of one gzip member",
    "a body of several gzip members",
    "a body that is not the gzip it says",
    "gzip cut short",
    "gzip followed by bytes that are not",
];

/// How far a receiver takes a request, in the order it takes it: its route, the token it presents, its size and
/// encoding as it is read, and how its body is cut.
#[derive(Clone, Copy, Debug, PartialEq, PartialOrd)]
enum Stage {
    /// Sent, whether or not TLS admitted it.
    Sent,
    Route,
    Token,
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
    /// A status, saying why where the receiver refused the request itself.
    Status(u16),
    /// 200, and these records taken.
    Taken(Vec<Vec<u8>>),
}

/// One request a sender makes, and what the receiver must answer it.
struct Sent {
    method: &'static str,
    path: String,
    /// Header lines, each ending in CRLF.
    headers: Vec<u8>,
    body: Vec<u8>,
    /// Sent in chunks, its length undeclared.
    chunked: bool,
    presents: Presents,
    expected: Expected,
    /// What it tries, and how far the receiver takes it.
    labels: Labels,
    reached: Stage,
}

/// Text a line or a body may hold, none of it a line end.
const TEXTS: &[&str] = &[
    "a line",
    "été 🎉",
    "say \"hi\"",
    "tab\there",
    "  spaced  ",
    "{\"a\":1}",
    "x",
];

/// A random JSON value, `depth` deep, as written: on one line unless `broken`.
fn value(rng: &mut Rng, depth: usize, broken: bool) -> String {
    let s = |rng: &mut Rng| match broken {
        true => *rng.pick(&["", " ", "\n", "\r\n\t", "  \n  "]),
        false => *rng.pick(&["", " ", "\t"]),
    };
    let kinds = if depth < 2 { 8 } else { 6 };
    match rng.below(kinds) {
        0 => serde_json::to_string(rng.pick(TEXTS)).unwrap(),
        1 => r#""été \" ] , [""#.to_string(),
        2 => (rng.next_u64() as i64 >> rng.below(64)).to_string(),
        3 => rng.pick(&["2.50", "-1e3", "0.1", "1E-7", "-0"]).to_string(),
        4 => rng.pick(&["true", "false", "null"]).to_string(),
        5 => r#""a, b ] c""#.to_string(),
        6 => {
            let items: Vec<String> = (0..rng.below(4)).map(|_| value(rng, depth + 1, broken)).collect();
            let (a, b) = (s(rng), s(rng));
            format!("[{a}{}{b}]", items.join(&format!("{b},{a}")))
        }
        _ => {
            let members: Vec<String> = ["k", "n", "été"][..rng.below(4)]
                .iter()
                .map(|key| format!("\"{key}\"{}:{}{}", s(rng), s(rng), value(rng, depth + 1, broken)))
                .collect();
            format!("{{{}}}", members.join(","))
        }
    }
}

/// Bytes of no text, a newline among them only where `lines` allows.
fn bytes(rng: &mut Rng, lines: bool) -> Vec<u8> {
    let mut bytes: Vec<u8> = (0..1 + rng.below(256)).map(|_| rng.next_u64() as u8).collect();
    if !lines {
        bytes.retain(|b| *b != b'\n');
    }
    // Never whitespace alone, which a receiver takes as no record.
    bytes.push(0xff);
    bytes
}

/// A JSON document holding `array` where `pointer` points, as written, with members beside it.
fn holding(rng: &mut Rng, pointer: &str, array: &str) -> String {
    if pointer.is_empty() {
        return array.to_string();
    }
    let mut inner = array.to_string();
    let steps: Vec<&str> = pointer[1..].split('/').collect();
    for step in steps.iter().rev() {
        inner = match step.parse::<usize>() {
            Ok(i) => {
                let before: Vec<String> = (0..i).map(|_| value(rng, 2, false)).collect();
                let mut items = before;
                items.push(inner);
                format!("[{}]", items.join(","))
            }
            Err(_) => {
                let key = serde_json::to_string(&step.replace("~1", "/").replace("~0", "~")).unwrap();
                match rng.chance(2) {
                    true => format!("{{\"other\":[1,2],{key}:{inner}}}"),
                    false => format!("{{{key} : {inner}, \"after\":{{\"x\":[]}}}}"),
                }
            }
        };
    }
    inner
}

/// `body` led by whitespace to `max` bytes.
fn filled(body: &[u8], max: usize) -> Vec<u8> {
    [vec![b' '; max - body.len()], body.to_vec()].concat()
}

/// What a body reads as: the records taken from it, or the status it is refused with.
type BodyRead = Result<Vec<Vec<u8>>, u16>;

/// A random body for a receiver that cuts records as `records` says and reads at most `max` bytes, and what it reads
/// as. Its label goes in `labels`, at the stage the receiver acts on it.
fn random_body(
    rng: &mut Rng,
    records: &ApiRecords,
    max: usize,
    well_sent: bool,
    labels: &mut Labels,
) -> (Vec<u8>, BodyRead) {
    let past = vec![b'x'; max + 1];
    match records {
        ApiRecords::Body => {
            let kind = if well_sent && !rng.chance(3) {
                rng.below(3)
            } else {
                rng.below(WHOLE_BODIES.len())
            };
            let past_limit = kind == WHOLE_BODIES.len() - 1;
            labels.at(if past_limit { Stage::Read } else { Stage::Body }, WHOLE_BODIES[kind]);
            let body = match kind {
                0 => (0..1 + rng.below(4))
                    .map(|_| *rng.pick(TEXTS))
                    .collect::<Vec<_>>()
                    .join("\n")
                    .into_bytes(),
                1 => bytes(rng, true),
                2 => value(rng, 0, true).into_bytes(),
                3 => b" \r\n\t ".to_vec(),
                4 => Vec::new(),
                5 => filled(b"x", max),
                _ => past,
            };
            let read = match body.trim_ascii().is_empty() {
                true => Ok(Vec::new()),
                false => Ok(vec![body.clone()]),
            };
            (body, read)
        }
        ApiRecords::Lines => {
            let kind = if well_sent && !rng.chance(3) {
                rng.below(7)
            } else {
                rng.below(LINE_BODIES.len())
            };
            let past_limit = kind == LINE_BODIES.len() - 1;
            labels.at(if past_limit { Stage::Read } else { Stage::Body }, LINE_BODIES[kind]);
            match kind {
                7 => return (b" \n\r\n\t\n".to_vec(), Ok(Vec::new())),
                8 => return (Vec::new(), Ok(Vec::new())),
                9 => {
                    let line = [b"x".as_slice(), &vec![b' '; max - 2], b"x"].concat();
                    return (line.clone(), Ok(vec![line]));
                }
                10 => return (past, Ok(Vec::new())),
                _ => {}
            }
            let (mut body, mut taken) = (Vec::new(), Vec::new());
            for _ in 0..1 + rng.below(8) {
                if kind == 2 && rng.chance(2) {
                    body.extend(*rng.pick(&[&b""[..], b"  ", b"\t", b"\r"]));
                    body.extend(if rng.chance(2) { &b"\r\n"[..] } else { b"\n" });
                }
                let line = match kind {
                    4 => format!("{}\r{}", rng.pick(TEXTS), rng.pick(TEXTS)).into_bytes(),
                    5 => bytes(rng, false),
                    // Only the CR before the newline is the line end's.
                    6 => format!("{}\r", rng.pick(TEXTS)).into_bytes(),
                    _ => rng.pick(TEXTS).as_bytes().to_vec(),
                };
                body.extend(&line);
                body.extend(if kind == 1 || kind == 6 || (kind != 0 && rng.chance(3)) {
                    &b"\r\n"[..]
                } else {
                    b"\n"
                });
                taken.push(line);
            }
            if kind == 3 {
                let end = if body.ends_with(b"\r\n") { 2 } else { 1 };
                body.truncate(body.len() - end);
            }
            (body, Ok(taken))
        }
        ApiRecords::Array(pointer) => {
            // Only a pointer of two steps can step through a scalar, and few receivers hold one, so one that does draws
            // it often.
            let kind = loop {
                let kind = if makes(11, pointer) && rng.chance(4) {
                    11
                } else if well_sent && !rng.chance(3) {
                    rng.below(4)
                } else {
                    rng.below(ARRAY_BODIES.len())
                };
                if makes(kind, pointer) {
                    break kind;
                }
            };
            let past_limit = kind == ARRAY_BODIES.len() - 1;
            labels.at(if past_limit { Stage::Read } else { Stage::Body }, ARRAY_BODIES[kind]);
            let elements: Vec<String> = match kind {
                1 => Vec::new(),
                2 => (0..1 + rng.below(5))
                    .map(|_| match rng.chance(2) {
                        true => format!("[{}]", value(rng, 1, false)),
                        false => format!("{{\"k\":{}}}", value(rng, 1, false)),
                    })
                    .collect(),
                _ => (0..1 + rng.below(5)).map(|_| value(rng, 1, kind == 3)).collect(),
            };
            let sep = match kind {
                3 => *rng.pick(&[" , ", ",\n", "\r\n,\t"]),
                _ => *rng.pick(&[",", ", "]),
            };
            let around = if kind == 3 { "\n  " } else { "" };
            let array = format!("[{around}{}{around}]", elements.join(sep));
            let taken: Vec<Vec<u8>> = elements.iter().map(|e| e.trim_ascii().as_bytes().to_vec()).collect();
            match kind {
                0..=3 => (holding(rng, pointer, &array).into_bytes(), Ok(taken)),
                4 => (b"not json".to_vec(), Err(400)),
                5 => {
                    let whole = holding(rng, pointer, &array);
                    (whole.as_bytes()[..whole.len() - 1].to_vec(), Err(400))
                }
                6 => {
                    let whole = holding(rng, pointer, "[\"\u{e9}\"]").into_bytes();
                    let at = whole.iter().position(|b| *b == 0xc3).unwrap();
                    let mut spoiled = whole.clone();
                    spoiled[at] = 0xff;
                    (spoiled, Err(400))
                }
                7 => (b"{\"elsewhere\":[1],\"items\":[]}".to_vec(), Err(400)),
                8 => (holding(rng, pointer, "null").into_bytes(), Err(400)),
                9 => {
                    let other = *rng.pick(&["{}", "\"[1]\"", "7"]);
                    (holding(rng, pointer, other).into_bytes(), Err(400))
                }
                // Every pointer here starts with a name.
                10 => (holding(rng, "", &array).into_bytes(), Err(400)),
                11 => {
                    let first = &pointer[..pointer[1..].find('/').unwrap() + 1];
                    let scalar = *rng.pick(&["7", "null", "\"[1]\"", "true"]);
                    (holding(rng, first, scalar).into_bytes(), Err(400))
                }
                12 => (Vec::new(), Err(400)),
                13 => (filled(holding(rng, pointer, &array).as_bytes(), max), Ok(taken)),
                _ => (past, Ok(Vec::new())),
            }
        }
    }
}

/// A random request to `settings`'s receiver, and what it must answer: the route, then the token, then the request's
/// size and framing, then what its body reads as, as the receiver takes them in that order.
fn random_request(rng: &mut Rng, settings: &Settings) -> Sent {
    let (method, route) = match rng.chance(5) {
        true => *rng.pick(ROUTES),
        false => *rng.pick(&ROUTES[..2]),
    };
    let mut labels = Labels::default();
    labels.at(Stage::Route, format!("{method} {route}"));
    let own = settings.path.1;
    let (path, serves) = match (method, route) {
        ("POST", "its path") => (own.to_string(), Serves::Records),
        ("POST", "its path with a query") => (format!("{own}?pipeline=x&a=%20b"), Serves::Records),
        (_, "its path") => (own.to_string(), Serves::Unmethod),
        ("GET", "/health") => ("/health".to_string(), Serves::Health),
        (_, "/health") => ("/health".to_string(), Serves::Unmethod),
        (_, "its path and a slash") => (format!("{own}/"), Serves::Nothing),
        // An encoded `/` is not a separator, so this names another path even where it decodes to the receiver's own.
        (_, "its path with a slash encoded") => match own[1..].replace('/', "%2F") {
            encoded if encoded.contains("%2F") => (format!("/{encoded}"), Serves::Nothing),
            encoded => (format!("/{encoded}%2F"), Serves::Nothing),
        },
        _ => ("/elsewhere/x".to_string(), Serves::Nothing),
    };
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
    // Under Served no sender is asked for a certificate, so what it presents tries nothing.
    if settings.tls == Tls::Vouched {
        labels.at(Stage::Sent, format!("presents {presents:?} under Vouched"));
    }
    // Most requests present a token the receiver holds, so most reach what follows the token.
    let well_sent = !rng.chance(4);
    let right = match settings.tokens.is_empty() {
        true => String::new(),
        false => rng.pick(&settings.tokens).clone(),
    };
    let authorization = match well_sent {
        true => ADMITTED + rng.below(AUTHORIZATIONS.len() - ADMITTED),
        false => rng.below(AUTHORIZATIONS.len()),
    };
    if !settings.tokens.is_empty() {
        labels.at(Stage::Token, AUTHORIZATIONS[authorization]);
    }
    // A receiver holding no token admits whoever TLS admitted.
    let refused = authorization < ADMITTED && !settings.tokens.is_empty();
    let authorization: Option<Vec<u8>> = match authorization {
        0 => None,
        1 => Some(format!("Bearer {right}x").into()),
        2 => Some(format!("Splunk {right}").into()),
        3 => Some(b"Bearer".to_vec()),
        4 => Some(b"Bearer \xe9\xff".to_vec()),
        5 => Some(right.clone().into()),
        6 => Some(format!("Bearer {right}").into()),
        _ => Some(format!("bEaReR  {right}").into()),
    };

    let max = settings.max_request_bytes;
    let mut headers = Vec::new();
    let mut body = Vec::new();
    let mut taken = Ok(Vec::new());
    let mut refused_reading = None;
    let chunked = serves == Serves::Records && rng.chance(4);
    if serves == Serves::Records {
        let read;
        (body, read) = random_body(rng, &settings.records, max, well_sent, &mut labels);
        taken = read;
        let (encoding, gzip) = match well_sent {
            true => ENCODINGS[rng.below(4)],
            false => *rng.pick(ENCODINGS),
        };
        let inflated = body.len();
        let gzipped = |body: &[u8]| {
            let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            encoder.write_all(body).unwrap();
            encoder.finish().unwrap()
        };
        let mut spoiled = false;
        if gzip == Some(true) {
            // A gzip body spoiled, which `well_sent` never sends; one that inflates past the limit is refused for that
            // first.
            let way = match !well_sent && inflated <= max && rng.chance(2) {
                true => rng.below(GZIPS.len()),
                false => rng.below(2),
            };
            body = match way {
                0 => gzipped(&body),
                1 => {
                    let at = rng.below(body.len() + 1);
                    [gzipped(&body[..at]), gzipped(&body[at..])].concat()
                }
                2 => b"not gzip".to_vec(),
                3 => {
                    let whole = gzipped(&body);
                    whole[..1 + rng.below(whole.len() - 1)].to_vec()
                }
                _ => [gzipped(&body), b"\x00junk".to_vec()].concat(),
            };
            spoiled = way >= 2;
            if body.len() <= max {
                labels.at(Stage::Read, GZIPS[way]);
            }
        }
        if chunked {
            labels.at(Stage::Read, "a body sent in chunks");
        }
        // A body declared past the limit is refused before its encoding is read; one sent in chunks, once it is.
        if body.len() <= max || chunked {
            labels.at(Stage::Read, format!("Content-Encoding {encoding:?}"));
        }
        if body.len() > max && chunked && gzip.is_some() {
            labels.at(Stage::Read, "a body past the limit sent in chunks");
        }
        refused_reading = match () {
            _ if body.len() > max && !chunked => Some(413),
            _ if gzip.is_none() => Some(415),
            _ if body.len() > max => Some(413),
            _ if spoiled => Some(400),
            _ if inflated > max => Some(413),
            _ => None,
        };
        if let Some(encoding) = encoding {
            headers.extend(format!("Content-Encoding: {encoding}\r\n").bytes());
        }
        if rng.chance(2) {
            headers.extend(b"Content-Type: application/json\r\n");
        }
    }
    if let Some(authorization) = authorization {
        headers.extend(b"Authorization: ");
        headers.extend(authorization);
        headers.extend(b"\r\n");
    }

    let (expected, reached) = if settings.tls == Tls::Vouched && presents != Presents::Trusted {
        (Expected::Nothing, Stage::Sent)
    } else {
        let (expected, reached) = match serves {
            Serves::Nothing => (Expected::Status(404), Stage::Route),
            Serves::Unmethod => (Expected::Status(405), Stage::Route),
            Serves::Health => (Expected::Status(200), Stage::Route),
            Serves::Records if refused => (Expected::Status(401), Stage::Token),
            Serves::Records => match (refused_reading, taken) {
                (Some(status), _) => (Expected::Status(status), Stage::Read),
                (None, Ok(taken)) => (Expected::Taken(taken), Stage::Body),
                (None, Err(status)) => (Expected::Status(status), Stage::Body),
            },
        };
        (expected, reached)
    };
    Sent {
        method,
        path,
        headers,
        body,
        chunked,
        presents,
        expected,
        labels,
        reached,
    }
}

/// `sent` over a new connection to `at`, under TLS where `tls` says.
fn exchange(at: SocketAddr, tls: Tls, pki: &Pki, sent: &Sent) -> Answer {
    let request = Request {
        method: sent.method,
        path: &sent.path,
        headers: &sent.headers,
        body: &sent.body,
        chunked: sent.chunked,
        presents: sent.presents,
    };
    receiver::exchange(at, tls, pki, &request)
}

/// The records' bytes, or why one is not filed as from `remote`, by an HTTP receiver.
fn read_back(records: &[SourceRecord], remote: &str) -> Result<Vec<Vec<u8>>, String> {
    let objects = SourceMeta::HTTP_RECEIVER.objects(records, Some("http"));
    (records.iter().zip(objects))
        .map(|(r, object)| {
            let text = |key: &str| object.get(key).and_then(Value::as_str).map(str::to_string);
            if text("remote").as_deref() != Some(remote) || text("type").as_deref() != Some("httpReceiver") {
                return Err(format!("a record filed as from another: {object:?}"));
            }
            Ok(r.bytes().unwrap().to_vec())
        })
        .collect()
}

/// Why `answer` is not what `sent` expected of a receiver under `settings`, or `Ok`.
fn check_answer(sent: &Sent, answer: &Answer, settings: &Settings) -> Result<(), String> {
    let wanted = match &sent.expected {
        Expected::Nothing => 0,
        Expected::Status(status) => *status,
        Expected::Taken(_) => 200,
    };
    if answer.status != wanted {
        return Err(format!(
            "answered {} {:?}, not {:?}",
            answer.status, answer.body, sent.expected
        ));
    }
    match answer.status {
        401 if !answer.head.contains("\r\nwww-authenticate: bearer") => {
            Err(format!("refused without naming its scheme: {:?}", answer.head))
        }
        // A sender logs why its request was refused.
        400 | 401 | 413 | 415 if !answer.body.ends_with('\n') || answer.body.trim().is_empty() => {
            Err(format!("refused {} saying nothing: {:?}", answer.status, answer.body))
        }
        // The sender sent a body; what it was sent is the receiver's own business.
        400 if answer.body.contains("the answer") => Err(format!("refused naming an answer: {:?}", answer.body)),
        413 if !answer.body.contains(&format!(" {} bytes", settings.max_request_bytes)) => {
            Err(format!("refused as too large without its limit: {:?}", answer.body))
        }
        _ => Ok(()),
    }
}

/// A receiver under any settings an operator can save, sent random requests to its path and beside it, answers each
/// as it documents, and takes exactly the records of the requests it answers 200, each cut from its body as it was
/// written.
#[test]
fn every_setting_answers_random_requests_and_cuts_their_records() {
    crate::certs::install_provider();
    let pki = Arc::new(Pki::new());
    let ipv6 = receiver::ipv6();
    let statuses = [0, 200, 400, 401, 404, 405, 413, 415];
    let mut cov = Coverage::new(
        receiver::listen_names(ipv6)
            .map(|ip| format!("listen {ip}"))
            .chain(["None", "Served", "Vouched"].map(|tls| format!("tls {tls}")))
            .chain((0..=3).map(|n| format!("tokens {n}")))
            .chain(RECEIVER_LIMITS.iter().map(|(_, name)| name.to_string()))
            .chain(PATHS.iter().map(|(_, sent)| format!("path {sent}")))
            .chain(["records Body", "records Lines"].map(String::from))
            .chain(POINTERS.iter().map(|p| format!("records at {p:?}")))
            .chain(ROUTES.iter().map(|(method, route)| format!("{method} {route}")))
            .chain(["Nothing", "Trusted", "Stranger"].map(|presents| format!("presents {presents} under Vouched")))
            .chain(AUTHORIZATIONS.iter().map(|s| s.to_string()))
            .chain(ENCODINGS.iter().map(|(e, _)| format!("Content-Encoding {e:?}")))
            .chain(
                WHOLE_BODIES
                    .iter()
                    .chain(LINE_BODIES)
                    .chain(ARRAY_BODIES)
                    .map(|s| s.to_string()),
            )
            .chain(
                [
                    "a body sent in chunks",
                    "a body past the limit sent in chunks",
                    "records of a request answered 200",
                ]
                .map(String::from)
                .into_iter()
                .chain(GZIPS.iter().map(|s| s.to_string())),
            )
            .chain(statuses.iter().map(|status| format!("answered {status}"))),
    );
    fuzz::run(1..201, |seed| {
        let mut rng = Rng::seeded(seed);
        let settings = random_settings(&mut rng, ipv6);
        cov.reach(format!("listen {}", settings.listen.ip()));
        cov.reach(format!("tls {:?}", settings.tls));
        cov.reach(format!("tokens {}", settings.tokens.len()));
        cov.reach(format!("path {}", settings.path.1));
        cov.reach(match &settings.records {
            ApiRecords::Array(pointer) => format!("records at {pointer:?}"),
            records => format!("records {records:?}"),
        });
        let limit = RECEIVER_LIMITS
            .iter()
            .find(|(below, _)| settings.max_request_bytes < *below)
            .unwrap();
        cov.reach(limit.1);
        let mut source = open(&settings, &pki);
        let sent: Vec<Sent> = (0..12).map(|_| random_request(&mut rng, &settings)).collect();
        let sending = Arc::new(sent);
        let (connect, tls) = (settings.connect, settings.tls);
        let requests = sending.clone();
        let senders_pki = pki.clone();
        let sender = std::thread::spawn(move || {
            (requests.iter())
                .map(|sent| exchange(connect, tls, &senders_pki, sent))
                .collect::<Vec<_>>()
        });
        // A request is answered once its records are committed, so they are taken as they arrive.
        let remote = settings.connect.ip().to_string();
        let deadline = Instant::now() + Duration::from_secs(60);
        let mut records = Vec::new();
        while !sender.is_finished() {
            if Instant::now() > deadline {
                return Err("the sender is still waiting".into());
            }
            let batch = source.poll_batch(10_000, 1 << 26, Duration::from_millis(10)).unwrap();
            commit(&mut source, &batch);
            records.extend(read_back(&batch, &remote)?);
        }
        let answers = sender.join().unwrap();
        let batch = source.poll_batch(10_000, 1 << 26, Duration::from_millis(50)).unwrap();
        commit(&mut source, &batch);
        records.extend(read_back(&batch, &remote)?);

        let mut expected_records = Vec::new();
        for (i, (sent, answer)) in sending.iter().zip(&answers).enumerate() {
            let what = format!(
                "request {i} ({} {} to a receiver at {:?} cutting {:?}, under {:?}, presenting {:?}, {} bytes, limit \
                 {}, headers {:?}, body {:?})",
                sent.method,
                sent.path,
                settings.path.0,
                settings.records,
                settings.tls,
                sent.presents,
                sent.body.len(),
                settings.max_request_bytes,
                String::from_utf8_lossy(&sent.headers),
                String::from_utf8_lossy(&sent.body[..sent.body.len().min(2048)]),
            );
            check_answer(sent, answer, &settings).map_err(|why| format!("{what}: {why}"))?;
            cov.reach(format!("answered {}", answer.status));
            for (stage, label) in &sent.labels.0 {
                if *stage <= sent.reached {
                    cov.reach(label.clone());
                }
            }
            if let Expected::Taken(taken) = &sent.expected {
                if !taken.is_empty() {
                    cov.reach("records of a request answered 200");
                }
                expected_records.extend(taken.iter().cloned());
            }
        }
        if records != expected_records {
            let show = |records: &[Vec<u8>]| -> Vec<String> {
                records.iter().map(|r| String::from_utf8_lossy(r).to_string()).collect()
            };
            return Err(format!(
                "took {} records, not the {} of the requests answered 200:\ntook     {:?}\nexpected {:?}",
                records.len(),
                expected_records.len(),
                show(&records),
                show(&expected_records)
            ));
        }
        Ok(())
    });
    cov.assert_complete();
}

/// What a live seed told Fluent Bit.
#[derive(Debug)]
struct Told {
    /// Its `http` output's `Format`.
    format: &'static str,
    gzip: bool,
}

/// Fluent Bit's configuration: tail `app.log`, and send to `at`'s `path` as `told` says, presenting `token` where it
/// has one, over TLS when `tls` is not None.
fn fluent_bit_config(at: SocketAddr, path: &str, token: Option<&str>, tls: Tls, told: &Told) -> String {
    let mut output = format!(
        "[OUTPUT]\n    Name http\n    Match *\n    Host localhost\n    Port {}\n    URI {path}\n    Format {}\n    \
         Retry_Limit 10\n",
        at.port(),
        told.format
    );
    if let Some(token) = token {
        output.push_str(&format!("    Header Authorization Bearer {token}\n"));
    }
    if told.gzip {
        output.push_str("    Compress gzip\n");
    }
    if tls != Tls::None {
        output.push_str("    TLS On\n    TLS.Verify On\n    TLS.CA_File /smeltr/ca.pem\n    TLS.Vhost localhost\n");
    }
    if tls == Tls::Vouched {
        output.push_str("    TLS.Crt_File /smeltr/sender.pem\n    TLS.Key_File /smeltr/sender-key.pem\n");
    }
    format!(
        "[SERVICE]\n    Flush 1\n    Grace 1\n    Log_Level info\n[INPUT]\n    Name tail\n    Path /smeltr/app.log\n    \
         Read_from_Head On\n{output}"
    )
}

/// The lines a record carries: each JSON value in it, an array's elements each, holds one under `log`.
fn lines_of(record: &[u8]) -> Result<Vec<String>, String> {
    let mut lines = Vec::new();
    for value in serde_json::Deserializer::from_slice(record).into_iter::<Value>() {
        let value = value.map_err(|e| format!("a record that is not JSON: {e}"))?;
        let events = match value {
            Value::Array(events) => events,
            event => vec![event],
        };
        for event in events {
            let line = event["log"]
                .as_str()
                .ok_or_else(|| format!("an event holding no line: {event}"))?;
            lines.push(line.to_string());
        }
    }
    Ok(lines)
}

/// A real Fluent Bit tails a file of random lines and sends them with its `http` output to receivers of random
/// settings, as JSON, JSON lines or a JSON stream, gzip or not, plain, over TLS, or over TLS with a client
/// certificate. However the receiver cuts its records, every line arrives exactly once, the receiver never panics, and
/// Fluent Bit logs no failure to send.
#[test]
fn live_fluent_bit_delivers_every_line_once_under_random_configurations() {
    const TEST: &str = "live_fluent_bit_delivers_every_line_once_under_random_configurations";
    let Some(fluent_bit) = crate::live::fluent_bit(TEST) else {
        return;
    };
    // A panic on the receiver's threads drops a connection and fails no test by itself.
    let panicked = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let seen = panicked.clone();
    let _hook = Hook::set(Box::new(move |info| {
        if std::thread::current().name() == Some("smeltr-receiver") {
            seen.store(true, std::sync::atomic::Ordering::Relaxed);
        }
        eprintln!("{info}");
    }));
    crate::certs::install_provider();
    let pki = Pki::new();
    let ipv6 = receiver::ipv6();
    // Each format, and each way of cutting records that reads it.
    let pairs = [
        ("json", "Array"),
        ("json", "Body"),
        ("json", "Lines"),
        ("json_lines", "Body"),
        ("json_lines", "Lines"),
        ("json_stream", "Body"),
        ("json_stream", "Lines"),
    ];
    let mut cov = Coverage::new(
        (pairs
            .iter()
            .map(|(format, records)| format!("{format} cut as {records}")))
        .chain(["None", "Served", "Vouched"].map(|tls| format!("under {tls}")))
        .chain(["gzip on", "gzip off", "no token"].map(String::from)),
    );

    fuzz::run(1..22, |seed| {
        // A panic is the seed's own receiver's only while one seed runs.
        panicked.store(false, std::sync::atomic::Ordering::Relaxed);
        let mut rng = Rng::seeded(seed);
        let mut settings = random_settings(&mut rng, ipv6);
        // A sender reaches the receiver over IPv4, on the host's network, by the name its certificate is for.
        settings.connect = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), settings.connect.port());
        if !settings.listen.ip().is_unspecified() {
            settings.listen = settings.connect;
        }
        settings.max_request_bytes = *rng.pick(&[1 << 20, 8 << 20]);
        // Each pair, under each way of serving TLS, within twenty-one seeds.
        let (format, records) = pairs[seed as usize % pairs.len()];
        settings.tls = [Tls::None, Tls::Served, Tls::Vouched][(seed as usize / pairs.len()) % 3];
        settings.records = match records {
            "Array" => ApiRecords::Array(String::new()),
            "Body" => ApiRecords::Body,
            _ => ApiRecords::Lines,
        };
        // Authorities alone admit a sender that presents no token.
        if settings.tls == Tls::Vouched && rng.chance(2) {
            settings.tokens.clear();
            cov.reach("no token");
        }
        let token = settings.tokens.first().cloned();
        let told = Told {
            format,
            gzip: rng.chance(2),
        };
        cov.reach(format!("{format} cut as {records}"));
        cov.reach(format!("under {:?}", settings.tls));
        cov.reach(if told.gzip { "gzip on" } else { "gzip off" });
        let what = format!(
            "Fluent Bit under {:?}, listening on {} at {:?} cutting {:?}, told {told:?}",
            settings.tls, settings.listen, settings.path.0, settings.records
        );
        let mut source = open(&settings, &pki);

        let scratch = Scratch(std::env::temp_dir().join(format!("smeltr-http-fuzz-{}-{seed}", std::process::id())));
        let dir = &scratch.0;
        std::fs::create_dir_all(dir).unwrap();
        let n = 1 + rng.below(300);
        // Each line names itself.
        let lines: Vec<String> = (0..n)
            .map(|i| format!("line {seed}-{i} {} été \"q\" \\", "x".repeat(rng.below(200))))
            .collect();
        let file: String = lines.iter().map(|l| format!("{l}\n")).collect();
        std::fs::write(dir.join("app.log"), file).unwrap();
        std::fs::write(dir.join("ca.pem"), &pki.authority.pem).unwrap();
        std::fs::write(dir.join("sender.pem"), &pki.sender.certificate).unwrap();
        std::fs::write(dir.join("sender-key.pem"), &pki.sender.key).unwrap();
        let config = fluent_bit_config(settings.connect, settings.path.1, token.as_deref(), settings.tls, &told);
        std::fs::write(dir.join("fluent-bit.conf"), config).unwrap();
        share(dir);
        let name = format!("smeltr-http-fuzz-{}-{seed}", std::process::id());
        let started = std::process::Command::new("docker")
            .args(["run", "-d", "--network", "host", "--name", &name, "-v"])
            .arg(format!("{}:/smeltr:ro", dir.display()))
            .arg(&fluent_bit)
            .args(["/fluent-bit/bin/fluent-bit", "-c", "/smeltr/fluent-bit.conf"])
            .output()
            .expect("docker runs");
        // A run that fails may still have created its container.
        let container = Container(name);
        if !started.status.success() {
            return Err(format!("docker run: {}", String::from_utf8_lossy(&started.stderr)));
        }

        let remote = settings.connect.ip().to_string();
        let mut arrived: BTreeMap<String, usize> = BTreeMap::new();
        let deadline = Instant::now() + Duration::from_secs(90);
        let take = |batch: &[SourceRecord]| {
            for record in read_back(batch, &remote).map_err(|why| format!("{what}: {why}"))? {
                for line in lines_of(&record).map_err(|why| format!("{what}: {why}"))? {
                    if !lines.contains(&line) {
                        return Err(format!("{what}: {line:?} is no line Fluent Bit tailed"));
                    }
                    *arrived.entry(line).or_default() += 1;
                }
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
        if logs.contains("[error]") || logs.contains("[ warn]") {
            return Err(format!("{what}: Fluent Bit logged a failure:\n{logs}"));
        }
        Ok(())
    });
    cov.assert_complete();
}
