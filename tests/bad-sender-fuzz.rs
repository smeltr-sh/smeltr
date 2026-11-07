//! Senders that misbehave at the transport, against every receiver: messages written in pieces down to a byte, many
//! in one write, ended partway; connections that send nothing, stall, hang up without close_notify, half-close or
//! reset; plain TCP to TLS, no certificate or a stranger's; senders outside a receiver's networks; storms of
//! connections; connections held idle or stalled while others send; a receiver left full. What a protocol makes of a
//! malformed message is left to each receiver's own fuzzer, so each kind here sends one well-formed message, carrying
//! a marker.
//!
//! After every seed: every record carries a marker some admitted sender sent, and none arrives twice; every message of
//! a connection that ended cleanly arrives, or, over HTTP, every request answered 200; nothing arrives from a sender
//! refused; the receiver holds to its connection limits, serves others while some connections idle or stall, never
//! panics, and serves a fresh connection afterwards.

use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use smeltr::io::SourceRecord;
use smeltr::io::framing::Framing;
use smeltr::plan::topology::{
    ApiRecords, ConnectionLimits, HecReceiverConfig, HttpReceiverConfig, Receiver, S2sReceiverConfig, Signal,
    SocketReceiverConfig,
};

use crate::harness::docker::Hook;
use crate::harness::fuzz::{self, Coverage, Rng};
use crate::harness::receiver::{self, Pki, Presents, Tls};
use crate::harness::sender::{self, Connection, End, Holds, Speaks};

#[derive(Clone, Copy, Debug, PartialEq)]
enum Kind {
    Http,
    Hec,
    Otlp,
    Prometheus,
    Elastic,
    S2s,
    Syslog,
    Socket,
}

const KINDS: [Kind; 8] = [
    Kind::Http,
    Kind::Hec,
    Kind::Otlp,
    Kind::Prometheus,
    Kind::Elastic,
    Kind::S2s,
    Kind::Syslog,
    Kind::Socket,
];

impl Kind {
    fn receiver(self) -> Receiver {
        match self {
            Kind::Http => Receiver::Http(HttpReceiverConfig {
                path: "/ingest".into(),
                records: ApiRecords::Lines,
            }),
            Kind::Hec => Receiver::Hec(HecReceiverConfig::default()),
            Kind::Otlp => Receiver::Otlp(Signal::Logs),
            Kind::Prometheus => Receiver::Prometheus,
            Kind::Elastic => Receiver::Elastic,
            Kind::S2s => Receiver::S2s(S2sReceiverConfig::default()),
            Kind::Syslog => Receiver::Syslog(SocketReceiverConfig {
                framing: Framing::Syslog(None),
                ..Default::default()
            }),
            Kind::Socket => Receiver::Socket(SocketReceiverConfig {
                framing: Framing::Lines,
                ..Default::default()
            }),
        }
    }

    /// Whether it speaks HTTP, answering each request.
    fn http(self) -> bool {
        !matches!(self, Kind::S2s | Kind::Syslog | Kind::Socket)
    }

    /// Whether it admits senders by token.
    fn tokens(self) -> bool {
        !matches!(self, Kind::Syslog | Kind::Socket)
    }

    /// Whether it admits senders by network.
    fn networks(self) -> bool {
        matches!(self, Kind::Syslog | Kind::Socket)
    }

    /// What a connection sends before its messages.
    fn opening(self, token: Option<&str>) -> Vec<u8> {
        match self {
            Kind::S2s => s2s_opening(token),
            _ => Vec::new(),
        }
    }

    /// One message carrying `marker`, as a sender presenting `token` writes it.
    fn message(self, marker: &str, token: Option<&str>) -> Vec<u8> {
        let bearer = |scheme: &str| {
            token
                .map(|t| format!("Authorization: {scheme} {t}\r\n"))
                .unwrap_or_default()
        };
        match self {
            Kind::Http => request("/ingest", &bearer("Bearer"), format!("{marker}\n").as_bytes()),
            Kind::Hec => request(
                "/services/collector/event",
                &bearer("Splunk"),
                format!(r#"{{"event":"{marker}"}}"#).as_bytes(),
            ),
            Kind::Otlp => request(
                "/v1/logs",
                &format!("{}Content-Type: application/json\r\n", bearer("Bearer")),
                format!(
                    r#"{{"resourceLogs":[{{"scopeLogs":[{{"logRecords":[{{"severityText":"{marker}","body":{{"stringValue":"{marker}"}}}}]}}]}}]}}"#
                )
                .as_bytes(),
            ),
            Kind::Prometheus => request(
                "/api/v1/write",
                &format!(
                    "{}Content-Type: application/x-protobuf\r\nContent-Encoding: snappy\r\n",
                    bearer("Bearer")
                ),
                &write_request(marker),
            ),
            Kind::Elastic => request(
                "/_bulk",
                &format!("{}Content-Type: application/x-ndjson\r\n", bearer("ApiKey")),
                format!("{{\"create\":{{\"_index\":\"logs\"}}}}\n{{\"message\":\"{marker}\"}}\n").as_bytes(),
            ),
            Kind::S2s => s2s_event(marker),
            Kind::Syslog => format!("<13>1 2026-11-06T12:00:00Z host app - - - {marker}\n").into_bytes(),
            Kind::Socket => format!("{marker}\n").into_bytes(),
        }
    }

    /// The text of `record` a marker is found in.
    fn text(self, record: &SourceRecord) -> Vec<u8> {
        // Each kind's field in its order: OTLP's `severityText`, Prometheus's `name`.
        let column = match self {
            Kind::Otlp => 3,
            Kind::Prometheus => 1,
            _ => return record.bytes().unwrap().to_vec(),
        };
        let column = record.chunk.column(column);
        match column.constant_value() {
            Some(smeltr::core::column::Scalar::Str(s)) => s.as_bytes().to_vec(),
            _ if !column.is_valid(record.row as usize) => Vec::new(),
            _ => column.var().unwrap().get(record.row as usize).to_vec(),
        }
    }
}

/// A POST to `path` with `headers`, each ending in CRLF, and `body`, on a connection kept open for the next.
fn request(path: &str, headers: &str, body: &[u8]) -> Vec<u8> {
    let head = format!(
        "POST {path} HTTP/1.1\r\nHost: receiver\r\n{headers}Content-Length: {}\r\n\r\n",
        body.len()
    );
    [head.as_bytes(), body].concat()
}

fn varint(out: &mut Vec<u8>, mut n: u64) {
    while n >= 0x80 {
        out.push(n as u8 | 0x80);
        n >>= 7;
    }
    out.push(n as u8);
}

/// A protobuf field of length-delimited `bytes`.
fn field(out: &mut Vec<u8>, number: u64, bytes: &[u8]) {
    varint(out, number << 3 | 2);
    varint(out, bytes.len() as u64);
    out.extend(bytes);
}

/// A remote_write 1.0 `WriteRequest` of one sample of the series named `marker`, snappy-compressed.
fn write_request(marker: &str) -> Vec<u8> {
    let mut label = Vec::new();
    field(&mut label, 1, b"__name__");
    field(&mut label, 2, marker.as_bytes());
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let mut sample = vec![0x09];
    sample.extend(1.0f64.to_le_bytes());
    sample.push(0x10);
    varint(&mut sample, now);
    let mut series = Vec::new();
    field(&mut series, 1, &label);
    field(&mut series, 2, &sample);
    let mut write = Vec::new();
    field(&mut write, 1, &series);
    snap::raw::Encoder::new().compress_vec(&write).unwrap()
}

/// An uncompressed S2S v3 message of `pairs`.
fn v3(pairs: &[(&[u8], &[u8])]) -> Vec<u8> {
    let string = |text: &[u8], body: &mut Vec<u8>| {
        body.extend((text.len() as u32 + 1).to_be_bytes());
        body.extend(text);
        body.push(0);
    };
    let mut body = (pairs.len() as u32).to_be_bytes().to_vec();
    for (key, value) in pairs {
        string(key, &mut body);
        string(value, &mut body);
    }
    body.extend(0u32.to_be_bytes());
    string(b"_raw", &mut body);
    [&(body.len() as u32).to_be_bytes()[..], &body].concat()
}

/// The channel every S2S event here is sent on.
const CHANNEL: u64 = 1;

/// A forwarder's signature, its capabilities without acknowledgement or compression, its ForwarderInfo presenting
/// `token`, and the channel its events are sent on.
fn s2s_opening(token: Option<&str>) -> Vec<u8> {
    let mut signature = vec![0; 400];
    signature[..25].copy_from_slice(b"--splunk-cooked-mode-v3--");
    signature[128..130].copy_from_slice(b"uf");
    signature[384..388].copy_from_slice(b"8089");
    let mut info: Vec<(&[u8], &[u8])> = vec![
        (b"_raw", b"ForwarderInfo build=0 version=10.6.0.5 hostname=uf"),
        (b"_MetaData:Index", b"_internal"),
        (b"MetaData:Sourcetype", b"sourcetype::fwdinfo"),
    ];
    if let Some(token) = token {
        info.push((b"__s2s_token", token.as_bytes()));
    }
    info.push((b"__s2s_eventId", b"0"));
    let mut out = [
        signature,
        v3(&[(b"__s2s_capabilities", b"ack=0;compression=0")]),
        v3(&info),
    ]
    .concat();
    out.push(0xfe);
    varint(&mut out, CHANNEL);
    for s in [&b"source::/var/log/bad.log"[..], b"host::bad", b"sourcetype::bad", b"2"] {
        varint(&mut out, s.len() as u64 + 1);
        out.extend(s);
    }
    varint(&mut out, 0);
    out
}

/// One whole event on [`CHANNEL`], already broken.
fn s2s_event(marker: &str) -> Vec<u8> {
    let mut out = vec![0xfc];
    varint(&mut out, CHANNEL);
    varint(&mut out, 1 << 6 | 1 << 9);
    varint(&mut out, marker.len() as u64);
    out.extend(marker.as_bytes());
    out
}

// -- the markers --------------------------------------------------------------------------------------------------

const MARKER: usize = 3 + 16;

fn marker(rng: &mut Rng) -> String {
    format!("mk_{:016x}", rng.next_u64())
}

/// Every marker in `text`.
fn markers(text: &[u8]) -> Vec<String> {
    (0..text.len().saturating_sub(MARKER - 1))
        .filter(|&i| text[i..].starts_with(b"mk_") && text[i + 3..i + MARKER].iter().all(u8::is_ascii_hexdigit))
        .map(|i| String::from_utf8_lossy(&text[i..i + MARKER]).to_string())
        .collect()
}

/// What a marker's message is to the receiver.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Fate {
    /// It must arrive once.
    Must,
    /// It may arrive once.
    May,
    /// Its sender was refused, so it must not arrive.
    Refused(Refused),
}

/// Why a connection is refused.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Refused {
    /// From an address in none of its networks.
    Address,
    /// Under TLS, presenting no certificate or one it does not admit.
    Certificate,
    /// Plain TCP to a TLS receiver.
    Plain,
    /// At its maxConnections, none of the connections it holds idle long enough to close.
    Full,
    /// At its maxConnectionsPerSender from the sender, the same way.
    FullFromSender,
}

/// Takes the markers of `batch` into `seen`, or says why not: a record no sender sent, one from a refused sender, or
/// one taken twice. A record with no marker may be the start of an `unfinished` message.
fn taken(
    kind: Kind,
    batch: &[SourceRecord],
    fates: &HashMap<String, Fate>,
    unfinished: &[Vec<u8>],
    seen: &mut HashSet<String>,
) -> Result<(), String> {
    for record in batch {
        let text = kind.text(record);
        let found = markers(&text);
        match found.as_slice() {
            [] if !text.is_empty() && unfinished.iter().any(|m| m.starts_with(&text)) => {}
            [] => return Err(format!("a record no sender sent: {:?}", String::from_utf8_lossy(&text))),
            [m] => match fates.get(m) {
                None => return Err(format!("a record with a marker no sender sent: {m}")),
                Some(Fate::Refused(r)) => return Err(format!("a record from a sender refused by {r:?}: {m}")),
                Some(_) if !seen.insert(m.clone()) => return Err(format!("a record taken twice: {m}")),
                Some(_) => {}
            },
            _ => return Err(format!("messages run together into one record: {found:?}")),
        }
    }
    Ok(())
}

// -- a seed -------------------------------------------------------------------------------------------------------

/// The idle timeout a seed that draws one sets: longer than any stall.
const IDLE_TIMEOUT: Duration = Duration::from_secs(2);

/// How long a connection stalled partway pauses.
const STALL: Duration = Duration::from_millis(800);

/// The address senders connect from, another the receiver's networks admit, and one they do not.
const FROM: Ipv4Addr = Ipv4Addr::new(127, 0, 0, 1);
const OTHER: Ipv4Addr = Ipv4Addr::new(127, 0, 0, 3);
const OUTSIDE: Ipv4Addr = Ipv4Addr::new(127, 0, 0, 2);

/// One connection a seed sends, and what it is to the receiver.
struct Planned {
    connection: Connection,
    /// Each message's marker and where it ends in the connection's bytes.
    messages: Vec<(String, usize)>,
    /// How many of its bytes it writes before it ends.
    written: usize,
}

impl Planned {
    /// How many of its messages it writes whole.
    fn whole(&self) -> usize {
        self.messages.iter().filter(|(_, end)| *end <= self.written).count()
    }
}

/// How a connection's bytes are written: a piece at a time and the pause after each. Labels what it reached.
fn pieces(rng: &mut Rng, len: usize, labels: &mut Vec<String>) -> Vec<(usize, Duration)> {
    let mut sizes = match rng.below(4) {
        0 if len > 1 && len <= 2000 => {
            labels.push("a connection written a byte at a time".into());
            vec![1; len]
        }
        1 if len > 1 => {
            labels.push("a connection written in pieces".into());
            let mut sizes = Vec::new();
            let mut left = len;
            while left > 0 {
                let n = (1 + rng.below(64)).min(left);
                sizes.push(n);
                left -= n;
            }
            sizes
        }
        _ => vec![len],
    };
    sizes.retain(|&n| n > 0);
    let mut pieces: Vec<(usize, Duration)> = sizes.into_iter().map(|n| (n, Duration::ZERO)).collect();
    for piece in &mut pieces {
        if rng.chance(64) {
            piece.1 = Duration::from_millis(rng.below(20) as u64);
        }
    }
    if pieces.len() > 1 && rng.chance(4) {
        let at = rng.below(pieces.len() - 1);
        pieces[at].1 = STALL;
        labels.push("a connection stalled partway".into());
    }
    pieces
}

/// Every label the seed range must reach.
fn options() -> impl Iterator<Item = String> {
    (KINDS.iter().map(|k| format!("kind {k:?}")))
        .chain(Tls::ALL.iter().map(|t| format!("tls {t:?}")))
        .chain(["HangUp", "Unsaid", "Reset", "HalfClose"].map(|e| format!("end {e}")))
        .chain(["Address", "Certificate", "Plain", "Full", "FullFromSender"].map(|r| format!("refused by {r}")))
        .chain(
            [
                "a connection written a byte at a time",
                "a connection written in pieces",
                "messages in one write",
                "an unfinished message",
                "a connection that sends nothing",
                "a connection stalled partway",
                "a storm of connections",
                "an idle connection held",
                "a connection stalled in its handshake",
                "a connection stalled partway through a message",
                "an idle connection closed by its timeout",
                "a receiver left full",
                "another sender served while one is full",
                "a request answered 200",
                "a fresh connection served afterwards",
            ]
            .map(String::from),
        )
}

/// Each receiver, under random settings, takes from misbehaving senders every message it must, none it must not,
/// none twice and nothing invented, holds to its connection limits, never panics, and serves a fresh connection
/// afterwards.
#[test]
fn misbehaving_senders_never_get_a_record_invented_lost_or_taken_twice() {
    let panicked = std::sync::Arc::new(AtomicBool::new(false));
    let seen = panicked.clone();
    let _hook = Hook::set(Box::new(move |info| {
        if std::thread::current().name() == Some("smeltr-receiver") {
            seen.store(true, Ordering::Relaxed);
        }
        eprintln!("{info}");
    }));
    crate::certs::install_provider();
    let pki = Pki::new();
    let ipv6 = receiver::ipv6();
    let mut cov = Coverage::new(options());
    fuzz::run(1..81, |seed| {
        let mut rng = Rng::seeded(seed);
        let mut labels: Vec<String> = Vec::new();
        let kind = KINDS[seed as usize % KINDS.len()];
        let (listen, connect) = receiver::random_addresses(&mut rng, ipv6);
        let port = receiver::free_port(connect);
        let (listen, at) = (SocketAddr::new(listen, port), SocketAddr::new(connect, port));
        let tls = receiver::random_check(seed, receiver::random_tls(&mut rng));
        // Senders at other addresses of the host reach it over IPv4 alone.
        let v4 = connect.is_ipv4();
        let from: IpAddr = match v4 {
            true => FROM.into(),
            false => connect,
        };
        let mut limits = ConnectionLimits::default();
        let mut full = None;
        match rng.below(4) {
            0 => {
                let holders = 1 + rng.below(3);
                match v4 && rng.chance(2) {
                    true => {
                        limits.per_sender = Some(holders);
                        full = Some((holders, Refused::FullFromSender));
                    }
                    false => {
                        limits.max = Some(holders);
                        full = Some((holders, Refused::Full));
                    }
                }
                labels.push("a receiver left full".into());
            }
            1 => limits.idle = Some(IDLE_TIMEOUT),
            _ => {}
        }
        let networks = kind.networks() && v4 && rng.chance(2);
        let token = kind.tokens().then(|| format!("tok{:x}", rng.next_u64()));
        let token = token.as_deref();
        let mut receiver_settings = kind.receiver();
        if let (true, Receiver::Syslog(socket) | Receiver::Socket(socket)) = (networks, &mut receiver_settings) {
            socket.allow_from = vec![
                format!("{FROM}/32").parse().unwrap(),
                format!("{OTHER}/32").parse().unwrap(),
            ];
        }
        let tokens: Vec<String> = token.into_iter().map(String::from).collect();
        let (mut config, secrets) =
            receiver::configured(receiver_settings, listen, tls, &tokens, "sender", 1 << 20, &pki);
        config.connections = limits;
        let mut source = receiver::opened(config, &secrets);
        let opening = kind.opening(token);
        let trusted = match tls {
            Tls::None => Speaks::Plain,
            _ => Speaks::Tls(Presents::Trusted),
        };

        // The connections held open first: those filling the receiver, else idle, stalled in a handshake or partway
        // through a message.
        let mut fates: HashMap<String, Fate> = HashMap::new();
        let mut unfinished: Vec<Vec<u8>> = Vec::new();
        let mut held = Vec::new();
        // Whether each held connection must have been closed by the end, where that is known.
        let mut closes: Vec<Option<bool>> = Vec::new();
        let mut what = Vec::new();
        let mut hold_for = Duration::ZERO;
        let mut hold = |holds: &Holds, close: Option<bool>, held: &mut Vec<_>| {
            if let Some(h) = sender::hold(at, from, trusted, holds, &pki) {
                held.push(h);
                closes.push(close);
            }
        };
        if let Some((n, _)) = full {
            for _ in 0..n {
                // Under TLS a held connection that never says hello may be closed by the handshake's timeout.
                hold(&Holds::Idle, (tls == Tls::None).then_some(false), &mut held);
            }
            what.push(format!("{n} held to fill it"));
        } else {
            if rng.chance(3) || limits.idle.is_some() {
                hold(&Holds::Idle, Some(limits.idle.is_some()), &mut held);
                labels.push(
                    match limits.idle {
                        Some(_) => "an idle connection closed by its timeout",
                        None => "an idle connection held",
                    }
                    .into(),
                );
                what.push("one held idle".into());
                if limits.idle.is_some() {
                    hold_for = IDLE_TIMEOUT * 2;
                }
            }
            if tls != Tls::None && rng.chance(3) {
                hold(&Holds::Handshake, None, &mut held);
                labels.push("a connection stalled in its handshake".into());
                what.push("one stalled in its handshake".into());
            }
            if rng.chance(3) && tls.admits(Presents::Trusted) {
                let m = marker(&mut rng);
                let message = kind.message(&m, token);
                let partway = [&opening[..], &message[..message.len() / 2]].concat();
                fates.insert(m, Fate::May);
                unfinished.push(message);
                hold(&Holds::Partway(partway), None, &mut held);
                labels.push("a connection stalled partway through a message".into());
                what.push("one stalled partway through a message".into());
            }
        }
        // An HTTP receiver admits the connections it accepts in any order, so the held ones are let in first.
        if !held.is_empty() {
            std::thread::sleep(Duration::from_millis(300));
        }
        let opened_at = Instant::now();

        // The connections that send.
        let mut planned: Vec<Planned> = Vec::new();
        let mut others = 0;
        for n in 0..1 + rng.below(8) {
            let mut reached = Vec::new();
            let mut refused = None;
            let from: IpAddr = match full {
                _ if networks && rng.chance(6) => {
                    refused = Some(Refused::Address);
                    OUTSIDE.into()
                }
                // Another sender is held to the same cap, so it sends fewer at once.
                Some((cap, Refused::FullFromSender)) if others < cap && rng.chance(2) => {
                    others += 1;
                    reached.push("another sender served while one is full".into());
                    OTHER.into()
                }
                _ => from,
            };
            let speaks = match tls {
                Tls::None => Speaks::Plain,
                _ => match rng.below(8) {
                    0 => Speaks::Plain,
                    1 => Speaks::Tls(Presents::Nothing),
                    2 => Speaks::Tls(Presents::Stranger),
                    _ => Speaks::Tls(Presents::Trusted),
                },
            };
            refused = refused.or(match speaks {
                Speaks::Plain if tls != Tls::None => Some(Refused::Plain),
                Speaks::Tls(presents) if !tls.admits(presents) => Some(Refused::Certificate),
                _ => None,
            });
            if let (None, Some((_, by))) = (refused, full)
                && (by == Refused::Full || from != IpAddr::from(OTHER))
            {
                refused = Some(by);
            }
            let count = match rng.below(6) {
                0 => 0,
                1 => 2 + rng.below(40),
                _ => 1 + rng.below(4),
            };
            let mut bytes = match count {
                0 if rng.chance(2) => Vec::new(),
                _ => opening.clone(),
            };
            let mut messages = Vec::new();
            for _ in 0..count {
                let m = marker(&mut rng);
                bytes.extend(kind.message(&m, token));
                messages.push((m, bytes.len()));
            }
            let mut end = match (speaks, rng.below(4)) {
                (_, 0) => End::Reset,
                (Speaks::Plain, 1) => End::HalfClose,
                (Speaks::Tls(_), 1) => End::Unsaid,
                _ => End::HangUp,
            };
            if bytes.is_empty() {
                reached.push("a connection that sends nothing".into());
            }
            let written = match !bytes.is_empty() && rng.chance(4) {
                true => rng.below(bytes.len()),
                false => bytes.len(),
            };
            let pieces = pieces(&mut rng, written, &mut reached);
            if pieces.len() == 1 && messages.iter().filter(|(_, end)| *end <= written).count() > 1 {
                reached.push("messages in one write".into());
            }
            // A connection that sends nothing and resets is a storm's.
            if bytes.is_empty() && end == End::Reset {
                end = End::HangUp;
            }
            let plan = Planned {
                connection: Connection {
                    from,
                    speaks,
                    bytes,
                    pieces,
                    end,
                    answers: kind.http().then_some(0),
                },
                messages,
                written,
            };
            let whole = plan.whole();
            // The message the connection ends partway through, if it does: it may arrive, or under a framing that
            // takes what a hang-up leaves, as much of it as was written.
            if let Some((_, stop)) = plan.messages.get(whole) {
                let start = match whole {
                    0 => opening.len(),
                    _ => plan.messages[whole - 1].1,
                };
                if start < written && refused.is_none() {
                    reached.push("an unfinished message".into());
                    unfinished.push(plan.connection.bytes[start..*stop].to_vec());
                }
            }
            what.push(format!(
                "connection {n} from {from} speaking {speaks:?}, {whole} of {count} messages whole in {} pieces, ending {end:?}{}",
                plan.connection.pieces.len(),
                refused.map(|r| format!(", refused by {r:?}")).unwrap_or_default()
            ));
            for (m, stop) in &plan.messages {
                let fate = match refused {
                    Some(r) => Fate::Refused(r),
                    None if *stop > written => Fate::May,
                    // Over HTTP, only a request answered 200 must arrive; that is known once it is.
                    None if kind.http() || end == End::Reset => Fate::May,
                    None => Fate::Must,
                };
                fates.insert(m.clone(), fate);
            }
            match refused {
                Some(r) => cov.reach(format!("refused by {r:?}")),
                None => {
                    labels.extend(reached);
                    labels.push(format!("end {end:?}"));
                }
            }
            planned.push(plan);
        }
        for plan in &mut planned {
            plan.connection.answers = kind.http().then(|| plan.whole());
        }
        let storm = match rng.chance(4) {
            true => {
                labels.push("a storm of connections".into());
                50 + rng.below(250)
            }
            false => 0,
        };
        let what = format!(
            "seed {seed}: {kind:?} receiver on {listen} under {tls:?}, {limits:?}{}, a storm of {storm}: {}",
            if networks { ", allowing two addresses" } else { "" },
            what.join("; ")
        );

        // The senders send while the receiver is drained, since an HTTP receiver answers once a request is delivered.
        let sent: Vec<Mutex<Option<sender::Sent>>> = planned.iter().map(|_| Mutex::new(None)).collect();
        let mut seen: HashSet<String> = HashSet::new();
        let mut musts: Option<Vec<String>> = None;
        let check = |batch: &[SourceRecord], fates: &HashMap<String, Fate>, seen: &mut HashSet<String>| {
            taken(kind, batch, fates, &unfinished, seen)
                .map_err(|why| format!("{what}\n{why}"))
                .and_then(|()| match panicked.load(Ordering::Relaxed) {
                    true => Err(format!("{what}\nthe receiver panicked")),
                    false => Ok(()),
                })
        };
        let drained = std::thread::scope(|scope| {
            let senders: Vec<_> = (planned.iter().zip(&sent))
                .map(|(plan, slot)| {
                    let pki = &pki;
                    scope.spawn(move || *slot.lock().unwrap() = Some(sender::send(at, pki, &plan.connection)))
                })
                .collect();
            sender::storm(at, from, storm);
            receiver::drain(
                &mut source,
                Duration::from_millis(20),
                Duration::from_millis(300),
                Instant::now() + Duration::from_secs(60),
                |batch| {
                    check(batch, &fates, &mut seen)?;
                    if !senders.iter().all(|s| s.is_finished()) || opened_at.elapsed() < hold_for {
                        return Ok(false);
                    }
                    let musts = musts.get_or_insert_with(|| {
                        (planned.iter().zip(&sent))
                            .flat_map(|(plan, slot)| {
                                let sent = slot.lock().unwrap();
                                let statuses = sent.as_ref().map(|s| s.statuses.clone()).unwrap_or_default();
                                (plan.messages.iter().enumerate())
                                    .filter(|&(i, _)| match fates[&plan.messages[i].0] {
                                        Fate::Must => true,
                                        Fate::May => kind.http() && statuses.get(i) == Some(&200),
                                        Fate::Refused(_) => false,
                                    })
                                    .map(|(_, (m, _))| m.clone())
                                    .collect::<Vec<_>>()
                            })
                            .collect()
                    });
                    Ok(musts.iter().all(|m| seen.contains(m)))
                },
            )
        })?;
        let sent: Vec<sender::Sent> = sent.into_iter().map(|s| s.into_inner().unwrap().unwrap()).collect();
        let musts = musts.unwrap_or_default();
        if let Some(m) = musts.iter().find(|m| !seen.contains(*m)) {
            return Err(format!(
                "{what}\na message that must arrive did not{}: {m}\nsent {sent:?}",
                if drained { "" } else { " by the deadline" }
            ));
        }
        if kind.http() && !musts.is_empty() {
            labels.push("a request answered 200".into());
        }
        for (n, s) in sent.iter().enumerate() {
            if let Some(why) = &s.left_open {
                return Err(format!("{what}\nconnection {n}: {why}"));
            }
        }
        for (n, (h, close)) in held.iter_mut().zip(&closes).enumerate() {
            if let Some(close) = close
                && h.closed(Duration::from_millis(300)) != *close
            {
                return Err(format!("{what}\nheld connection {n} must be closed: {close}"));
            }
        }

        // Once what it held is let go, a fresh connection is served.
        for h in held {
            h.let_go();
        }
        if tls.admits(Presents::Trusted) {
            let (mut served, mut last) = (false, String::new());
            for _ in 0..5 {
                let m = marker(&mut rng);
                fates.insert(m.clone(), Fate::May);
                let bytes = [&opening[..], &kind.message(&m, token)].concat();
                let fresh = Connection {
                    from,
                    speaks: trusted,
                    pieces: vec![(bytes.len(), Duration::ZERO)],
                    bytes,
                    end: End::HangUp,
                    answers: kind.http().then_some(1),
                };
                std::thread::sleep(Duration::from_millis(100));
                let sent = std::thread::scope(|scope| {
                    let sending = scope.spawn(|| sender::send(at, &pki, &fresh));
                    receiver::drain(
                        &mut source,
                        Duration::from_millis(20),
                        Duration::from_millis(300),
                        Instant::now() + Duration::from_secs(10),
                        |batch| {
                            check(batch, &fates, &mut seen)?;
                            Ok(sending.is_finished() && seen.contains(&m))
                        },
                    )?;
                    Ok::<_, String>(sending.join().unwrap())
                })?;
                if seen.contains(&m) {
                    served = true;
                    break;
                }
                last = format!("{sent:?}");
            }
            if !served {
                return Err(format!("{what}\na fresh connection was not served afterwards: {last}"));
            }
            labels.push("a fresh connection served afterwards".into());
        }
        cov.reach(format!("kind {kind:?}"));
        cov.reach(format!("tls {tls:?}"));
        for label in labels {
            cov.reach(label);
        }
        Ok(())
    });
    cov.assert_complete();
}
