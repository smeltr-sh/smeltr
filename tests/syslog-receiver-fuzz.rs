//! Random syslog senders against syslog receivers of random settings. Syslog has no acknowledgement: a sender forgets
//! a message once written. So every message a receiver admits must arrive once, as sent, and none it refuses: a
//! malformed frame closes the connection, keeping what came before it.
//!
//! Each seed opens a receiver under settings an operator could save, over TCP or UDP, and sends it connections or
//! datagrams from senders it admits and senders it does not: messages octet-counted or ended by a newline, or by the
//! receiver's line breaker, mixed, written in random pieces, ending cleanly, cut short or spoiled. A model built beside each sender says which records
//! must arrive. A deterministic PRNG over a seed range, from `harness::fuzz`: a failure names its seed, and the fuzzer
//! checks that its seeds reached every option it knows.

use std::collections::BTreeMap;
use std::io::{self, Read, Write};
use std::net::{Ipv4Addr, Shutdown, SocketAddr, TcpStream, UdpSocket};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::Value;
use smeltr::io::{Source, SourceRecord};
use smeltr::plan::topology::{Receiver, SourceMeta, SyslogReceiverConfig, Transport};

use crate::harness::docker::{Container, Hook, Scratch, container_logs, share};
use crate::harness::fuzz::{self, Coverage, Rng};
use crate::harness::receiver::{self, Pki, Presents, Tls};

/// One receiver's settings, as an operator could save them.
struct Settings {
    listen: SocketAddr,
    /// Where a sender reaches it.
    connect: SocketAddr,
    transport: Transport,
    /// The networks a sender may send from.
    allow_from: Vec<&'static str>,
    tls: Tls,
    max_request_bytes: usize,
    /// Where a message over TCP ends, when not at a newline.
    line_breaker: Option<Breaker>,
}

/// A line breaker a receiver is given.
#[derive(Clone, Copy, PartialEq, Debug)]
enum Breaker {
    /// A NUL ends each message.
    Nul,
    /// Nothing ends a message: the next one's start breaks it.
    Start,
}

impl Breaker {
    fn pattern(self) -> &'static str {
        match self {
            Breaker::Nul => r"(\x00)",
            Breaker::Start => r"()<13>c",
        }
    }
}

/// The limits a receiver is given, by the bound each is below.
const RECEIVER_LIMITS: &[(usize, &str)] = &[
    (4 << 10, "limit under 4 KiB"),
    (64 << 10, "limit under 64 KiB"),
    (usize::MAX, "limit of 64 KiB or more"),
];

/// Networks a receiver may admit senders from, and whether a sender on each loopback address is in them.
const NETWORKS: &[(&str, bool, bool)] = &[
    ("127.0.0.0/8", true, false),
    ("127.0.0.1/32", true, false),
    ("10.0.0.0/8", false, false),
    ("0.0.0.0/0", true, false),
    ("::1/128", false, true),
    ("::/0", false, true),
    ("fd00::/8", false, false),
];

fn random_settings(rng: &mut Rng, ipv6: bool) -> Settings {
    let (listen, connect) = receiver::random_addresses(rng, ipv6);
    let transport = *rng.pick(&[Transport::Tcp, Transport::Udp]);
    let port = loop {
        let port = std::net::TcpListener::bind((connect, 0))
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        if transport == Transport::Tcp || UdpSocket::bind((connect, port)).is_ok() {
            break port;
        }
    };
    // Datagrams have no TLS.
    let tls = match transport {
        Transport::Tcp => receiver::random_tls(rng),
        Transport::Udp => Tls::None,
    };
    // Authorities admit a sender alone; without them, a receiver names the networks it admits.
    let allow_from = receiver::random_tokens(rng, tls, |rng, _| rng.pick(NETWORKS).0);
    let line_breaker = match transport {
        Transport::Tcp => *rng.pick(&[None, None, Some(Breaker::Nul), Some(Breaker::Start)]),
        Transport::Udp => None,
    };
    Settings {
        line_breaker,
        listen: SocketAddr::new(listen, port),
        connect: SocketAddr::new(connect, port),
        transport,
        allow_from,
        tls,
        max_request_bytes: receiver::random_limit(rng, 8),
    }
}

/// Whether a receiver under `settings` admits a sender on `connect`'s address.
fn admits(settings: &Settings) -> bool {
    let v6 = settings.connect.is_ipv6();
    settings.allow_from.is_empty()
        || (settings.allow_from.iter()).any(|network| {
            NETWORKS
                .iter()
                .any(|(n, v4_in, v6_in)| n == network && if v6 { *v6_in } else { *v4_in })
        })
}

fn open(settings: &Settings, pki: &Pki) -> Box<dyn Source> {
    let syslog = Receiver::Syslog(SyslogReceiverConfig {
        allow_from: settings.allow_from.iter().map(|n| n.parse().unwrap()).collect(),
        transport: settings.transport,
        line_breaker: settings
            .line_breaker
            .map(|b| smeltr::io::line_breaker::LineBreaker(regex::bytes::Regex::new(b.pattern()).unwrap())),
        ..Default::default()
    });
    receiver::build(
        syslog,
        settings.listen,
        settings.tls,
        &[],
        "sender",
        settings.max_request_bytes,
        pki,
    )
}

/// Frames a TCP sender writes, each by what it holds.
const FRAMES: &[&str] = &[
    "an octet-counted message",
    "an octet-counted message holding line ends",
    "an octet-counted message at the limit",
    "a line",
    "a line ending CRLF",
    "a line ending two CRs",
    "a line holding a carriage return",
    "a line not UTF-8",
    "a line at the limit",
    "a line at the limit ending CRLF",
    "a line starting with 0",
    "a blank line",
];

/// How a TCP connection ends.
const ENDINGS: &[&str] = &[
    "a connection that ends after a whole frame",
    "a last line with no newline",
    "a last blank line with no newline",
    "a last octet-counted message cut short",
    "a last length with no space",
    "a last line ending CR",
    "a length over the limit",
    "a length not followed by a space",
    "a line over the limit",
    "a last line over the limit",
    "a line over the limit, the connection left open",
];

/// The first of `ENDINGS` the receiver takes as malformed: it closes the connection, keeping the messages before.
const MALFORMED: usize = 6;

/// The endings whose last line the receiver takes only once the connection ends.
const ENDED_BY_HANGING_UP: &[usize] = &[1, 5];

/// Datagrams a UDP sender sends, each by what it holds.
const DATAGRAMS: &[&str] = &[
    "a datagram",
    "a datagram ending LF",
    "a datagram ending CRLF",
    "a datagram ending CR",
    "a datagram holding line ends",
    "a datagram not UTF-8",
    "a datagram of whitespace",
    "a datagram ending two LFs",
    "a datagram ending CR CRLF",
    "a datagram at the limit",
    "a datagram of the largest a host sends",
    "an empty datagram",
    "a datagram of an LF",
    "a datagram of a CR",
    "a datagram of a CRLF",
    "a datagram over the limit",
];

/// The largest datagram a host sends over IPv4, and so over either family.
const LARGEST_DATAGRAM: usize = 65_507;

/// Text of no line end, starting with none of the digits an octet count starts with.
fn text(rng: &mut Rng) -> Vec<u8> {
    let parts = ["app[12]:", " a line", " été 🎉", " say \"hi\"", "\ttab", " 0 1 2", " x"];
    (0..1 + rng.below(5)).flat_map(|_| rng.pick(&parts).bytes()).collect()
}

/// `prefix`, then bytes of no line end up to `len` bytes in all, ending in none of the whitespace a line drops.
fn padded(prefix: &[u8], len: usize) -> Vec<u8> {
    let mut line = prefix.to_vec();
    line.resize(len.max(prefix.len()), b'x');
    line
}

/// One TCP sender's connection: what it writes, how it is presented, and the records it must give.
struct Connection {
    bytes: Vec<u8>,
    /// How many bytes it writes at a time.
    pieces: Vec<usize>,
    presents: Presents,
    /// Whether it hangs up over TLS without saying so first, as a TLS sender may.
    abrupt: bool,
    /// Whether it leaves its side open once written, for the receiver to close.
    left_open: bool,
    taken: Vec<Vec<u8>>,
    /// Whether its last record is one the receiver takes only once the connection ends.
    ended_by_hanging_up: bool,
}

/// A random connection, the `n`th, to a receiver reading messages of at most `max` bytes, and the labels it reaches
/// where the receiver reads it.
fn connection(rng: &mut Rng, n: usize, max: usize, labels: &mut Vec<String>) -> Connection {
    let (mut bytes, mut taken) = (Vec::new(), Vec::new());
    for i in 0..rng.below(8) {
        let kind = rng.below(FRAMES.len());
        labels.push(FRAMES[kind].to_string());
        // Each message names itself, and starts with no digit, so a line never reads as an octet count.
        let named = format!("<13>c{n}m{i} ").into_bytes();
        let message = |rng: &mut Rng| [named.clone(), text(rng)].concat();
        let (frame, record): (Vec<u8>, Option<Vec<u8>>) = match kind {
            0..=2 => {
                let message = match kind {
                    0 => message(rng),
                    1 => [message(rng), b"\nsecond line\r\n\n".to_vec(), text(rng)].concat(),
                    _ => padded(&named, max),
                };
                (
                    [format!("{} ", message.len()).into_bytes(), message.clone()].concat(),
                    Some(message),
                )
            }
            3 => {
                let line = message(rng);
                ([line.clone(), b"\n".to_vec()].concat(), Some(line))
            }
            4 => {
                let line = message(rng);
                ([line.clone(), b"\r\n".to_vec()].concat(), Some(line))
            }
            // Only the CR before the newline is the line end's.
            5 => {
                let line = message(rng);
                (
                    [line.clone(), b"\r\r\n".to_vec()].concat(),
                    Some([line, b"\r".to_vec()].concat()),
                )
            }
            6 => {
                let line = [message(rng), b"\rafter".to_vec()].concat();
                ([line.clone(), b"\n".to_vec()].concat(), Some(line))
            }
            7 => {
                let line = [named.clone(), b"\xff\xfe\x00bytes\x80".to_vec()].concat();
                ([line.clone(), b"\n".to_vec()].concat(), Some(line))
            }
            8 | 9 => {
                let line = padded(&named, max);
                let end: &[u8] = if kind == 8 { b"\n" } else { b"\r\n" };
                ([line.clone(), end.to_vec()].concat(), Some(line))
            }
            // Only 1-9 starts an octet count.
            10 => {
                let line = [b"0 ".to_vec(), message(rng)].concat();
                ([line.clone(), b"\n".to_vec()].concat(), Some(line))
            }
            _ => (rng.pick(&[&b"\n"[..], b"  \n", b"\t\r\n", b"\r\n"]).to_vec(), None),
        };
        bytes.extend(frame);
        taken.extend(record);
    }
    let ending = rng.below(ENDINGS.len());
    labels.push(ENDINGS[ending].to_string());
    let named = format!("<13>c{n}end ").into_bytes();
    match ending {
        0 => {}
        1 => {
            let line = [named, text(&mut *rng)].concat();
            bytes.extend(&line);
            taken.push(line);
        }
        2 => bytes.extend(b"  \t"),
        // Dropped: the sender hung up before it was whole.
        3 => {
            let message = [named, text(&mut *rng)].concat();
            bytes.extend(format!("{} ", message.len() + 1 + rng.below(100)).bytes());
            bytes.extend(message);
        }
        4 => bytes.extend(format!("{}", 1 + rng.below(999)).bytes()),
        5 => {
            let line = [named, text(&mut *rng)].concat();
            bytes.extend(&line);
            bytes.extend(b"\r");
            taken.push(line);
        }
        // Whole, so a receiver that took the count would take it.
        6 => {
            let len = max + 1 + rng.below(1000);
            bytes.extend(format!("{len} ").bytes());
            bytes.extend(padded(&named, len));
        }
        7 => bytes.extend(b"12x message"),
        8 => {
            bytes.extend(padded(&named, max + 1));
            bytes.extend(b"\n");
        }
        9 => bytes.extend(padded(&named, max + 1)),
        _ => bytes.extend(padded(&named, max + 2 + rng.below(100))),
    }
    // A malformed connection's frames after the malformed one are never read.
    if (MALFORMED..9).contains(&ending) {
        bytes.extend(format!("<13>c{n}never read\n").bytes());
    }
    let pieces = pieces(rng, bytes.len(), labels);
    Connection {
        bytes,
        pieces,
        presents: Presents::Nothing,
        abrupt: false,
        left_open: ending == ENDINGS.len() - 1,
        taken,
        ended_by_hanging_up: ENDED_BY_HANGING_UP.contains(&ending),
    }
}

/// Frames a TCP sender writes to a receiver with a line breaker, each by what it holds.
const BROKEN_FRAMES: &[&str] = &[
    "a broken message",
    "a broken message holding line ends",
    "a broken message at the limit",
    "a blank broken message",
    "an octet-counted message beside a line breaker",
];

/// How a TCP connection to a receiver with a line breaker ends.
const BROKEN_ENDINGS: &[&str] = &[
    "a connection that ends after a broken message",
    "a last broken message with no break",
    "a last octet-counted message cut short beside a line breaker",
    "a broken message over the limit",
    "a last broken message over the limit",
    "an unbroken run past the slack, the connection left open",
];

/// A random connection, the `n`th, to a receiver whose messages `breaker` breaks, of at most `max` bytes, and the
/// labels it reaches where the receiver reads it.
fn broken_connection(rng: &mut Rng, n: usize, max: usize, breaker: Breaker, labels: &mut Vec<String>) -> Connection {
    // A message's start breaks the one before it, so with `Start` nothing may come between messages: no blank one,
    // and no octet count, which the message before would take.
    let only_messages = breaker == Breaker::Start;
    let end: &[u8] = match breaker {
        Breaker::Nul => b"\x00",
        Breaker::Start => b"",
    };
    let (mut bytes, mut taken) = (Vec::new(), Vec::new());
    // Whether the last frame is a message that only the connection's end breaks.
    let mut waits = false;
    for i in 0..rng.below(8) {
        let kind = rng.below(if only_messages { 3 } else { BROKEN_FRAMES.len() });
        labels.push(BROKEN_FRAMES[kind].to_string());
        let named = format!("<13>c{n}m{i} ").into_bytes();
        let (frame, record): (Vec<u8>, Option<Vec<u8>>) = match kind {
            0..=2 => {
                let message = match kind {
                    0 => [named, text(rng)].concat(),
                    1 => [named, text(rng), b"\nsecond line\r\n".to_vec(), text(rng)].concat(),
                    _ => padded(&named, max),
                };
                ([message.clone(), end.to_vec()].concat(), Some(message))
            }
            3 => (rng.pick(&[&b"\x00"[..], b"  \x00", b"\r\n\x00"]).to_vec(), None),
            _ => {
                let message = [named, text(rng)].concat();
                (
                    [format!("{} ", message.len()).into_bytes(), message.clone()].concat(),
                    Some(message),
                )
            }
        };
        // A blank's break may reach the end, but it holds nothing to drop, and its bytes break the message before.
        waits = kind <= 2;
        bytes.extend(frame);
        taken.extend(record);
    }
    let ending = match only_messages {
        true => *rng.pick(&[0, 1, 3, 4, 5]),
        false => rng.below(BROKEN_ENDINGS.len()),
    };
    labels.push(BROKEN_ENDINGS[ending].to_string());
    let named = format!("<13>c{n}end ").into_bytes();
    match ending {
        0 => {}
        1 => {
            let message = [named, text(rng)].concat();
            bytes.extend(&message);
            taken.push(message);
            waits = true;
        }
        // Dropped: the sender hung up before it was whole.
        2 => {
            let message = [named, text(rng)].concat();
            bytes.extend(format!("{} ", message.len() + 1 + rng.below(100)).bytes());
            bytes.extend(message);
            waits = false;
        }
        // Malformed, so the connection closes there, keeping the messages before.
        3 => {
            bytes.extend(padded(&named, max + 1 + rng.below(100)));
            bytes.extend(end);
            bytes.extend(format!("<13>c{n}never read").bytes());
            bytes.extend(end);
        }
        4 => bytes.extend(padded(&named, max + 1 + rng.below(100))),
        _ => bytes.extend(padded(
            &named,
            max + smeltr::io::line_breaker::BREAK_SLACK + 1 + rng.below(100),
        )),
    }
    // A malformed ending's message is the last, and is never taken: what waited before it is taken when it breaks.
    if ending >= 3 {
        waits = false;
    }
    let pieces = pieces(rng, bytes.len(), labels);
    Connection {
        bytes,
        pieces,
        presents: Presents::Nothing,
        abrupt: false,
        left_open: ending == BROKEN_ENDINGS.len() - 1,
        taken,
        // A break that reaches the last byte held waits for what comes after it, so the last message waits for the
        // connection's end.
        ended_by_hanging_up: waits,
    }
}

/// How many bytes a connection of `len` writes at a time, and the label it reaches when that is more than once.
fn pieces(rng: &mut Rng, len: usize, labels: &mut Vec<String>) -> Vec<usize> {
    let mut pieces = Vec::new();
    let mut left = len;
    while left > 0 {
        let piece = match rng.below(3) {
            0 => 1 + rng.below(8),
            1 => 1 + rng.below(512),
            _ => left,
        }
        .min(left);
        pieces.push(piece);
        left -= piece;
    }
    if pieces.len() > 1 {
        labels.push("a connection written in pieces".into());
    }
    pieces
}

/// One datagram, and the record it must give, if any.
fn datagram(rng: &mut Rng, n: usize, max: usize, label: &mut String) -> (Vec<u8>, Option<Vec<u8>>) {
    // A datagram larger than a host sends tests nothing of the receiver.
    let kinds = if max < LARGEST_DATAGRAM {
        DATAGRAMS.len()
    } else {
        DATAGRAMS.len() - 1
    };
    let kind = rng.below(kinds);
    *label = DATAGRAMS[kind].to_string();
    // Each message names itself.
    let named = format!("<13>d{n} ").into_bytes();
    let message = [named.clone(), text(rng)].concat();
    let largest = padded(&named, max.min(LARGEST_DATAGRAM));
    match kind {
        9 if max >= LARGEST_DATAGRAM => *label = DATAGRAMS[10].to_string(),
        10 if max < LARGEST_DATAGRAM => *label = DATAGRAMS[9].to_string(),
        _ => {}
    }
    match kind {
        0 => (message.clone(), Some(message)),
        1 => ([message.clone(), b"\n".to_vec()].concat(), Some(message)),
        2 => ([message.clone(), b"\r\n".to_vec()].concat(), Some(message)),
        3 => ([message.clone(), b"\r".to_vec()].concat(), Some(message)),
        4 => {
            let message = [message, b"\nnext\r\nlast".to_vec()].concat();
            (message.clone(), Some(message))
        }
        5 => {
            let message = [named, b"\xff\x00\x80".to_vec()].concat();
            (message.clone(), Some(message))
        }
        // A datagram is the sender's message, whatever it holds.
        6 => (b" \t ".to_vec(), Some(b" \t ".to_vec())),
        // One LF, then one CR, is the datagram's end.
        7 => (
            [message.clone(), b"\n\n".to_vec()].concat(),
            Some([message, b"\n".to_vec()].concat()),
        ),
        8 => (
            [message.clone(), b"\r\r\n".to_vec()].concat(),
            Some([message, b"\r".to_vec()].concat()),
        ),
        9 | 10 => (largest.clone(), Some(largest)),
        11 => (Vec::new(), None),
        12 => (b"\n".to_vec(), None),
        13 => (b"\r".to_vec(), None),
        14 => (b"\r\n".to_vec(), None),
        // Larger than the receiver reads, so it reads only part of it.
        _ => (padded(&named, (max + 2 + rng.below(100)).min(LARGEST_DATAGRAM)), None),
    }
}

/// The records' bytes, or why one is not filed as from `remote`, by a syslog receiver.
fn read_back(records: &[SourceRecord], remote: &str) -> Result<Vec<Vec<u8>>, String> {
    let objects = SourceMeta::SYSLOG_RECEIVER.objects(records, Some("syslog"));
    (records.iter().zip(objects))
        .map(|(r, object)| {
            let text = |key: &str| object.get(key).and_then(Value::as_str).map(str::to_string);
            if text("remote").as_deref() != Some(remote) || text("type").as_deref() != Some("syslogReceiver") {
                return Err(format!("a record filed as from another: {object:?}"));
            }
            Ok(r.bytes().unwrap().to_vec())
        })
        .collect()
}

/// Writes `connection` to `at` over a new connection, under TLS where `tls` says, in its pieces, then hangs up, as the
/// connection says, and waits for the receiver to hang up too. A receiver that closes the connection leaves the rest
/// unwritten. Why the receiver did not hang up, or `Ok`.
fn send(at: SocketAddr, tls: Tls, pki: &Pki, connection: &Connection) -> Result<(), String> {
    let Ok(tcp) = TcpStream::connect(at) else {
        return Ok(());
    };
    tcp.set_write_timeout(Some(Duration::from_secs(10))).unwrap();
    tcp.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    // Each piece its own segment, so the receiver more often reads a frame in parts.
    tcp.set_nodelay(true).unwrap();
    // Closing a socket with unread data resets the connection, and a reset can drop what the receiver has not yet
    // read, as a sender that hangs up properly never sees. So the sender reads until the receiver is done.
    let hang_up = |stream: &mut dyn Read, tcp: &TcpStream| {
        if !connection.left_open {
            let _ = tcp.shutdown(Shutdown::Write);
        }
        match io::copy(stream, &mut io::sink()) {
            Err(e) if matches!(e.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut) => {
                Err("the receiver left the connection open".to_string())
            }
            _ => Ok(()),
        }
    };
    let write = |stream: &mut dyn Write| {
        let mut at = 0;
        for piece in &connection.pieces {
            if stream
                .write_all(&connection.bytes[at..at + piece])
                .and_then(|()| stream.flush())
                .is_err()
            {
                return;
            }
            at += piece;
        }
    };
    match tls {
        Tls::None => {
            let mut tcp = tcp;
            write(&mut tcp);
            hang_up(&mut tcp.try_clone().unwrap(), &tcp)
        }
        _ => {
            let config = pki.client(connection.presents);
            let tls = rustls::ClientConnection::new(config, "localhost".try_into().unwrap()).unwrap();
            let mut stream = rustls::StreamOwned::new(tls, tcp);
            write(&mut stream);
            if !connection.abrupt && !connection.left_open {
                stream.conn.send_close_notify();
                let _ = stream.flush();
            }
            let tcp = stream.sock.try_clone().unwrap();
            hang_up(&mut stream, &tcp)
        }
    }
}

/// A receiver under any settings an operator can save takes every message an admitted sender writes whole, once and as
/// written, however it is framed and written, and nothing from a sender it does not admit or after a malformed frame.
#[test]
fn every_setting_takes_each_message_once_however_it_is_framed() {
    crate::certs::install_provider();
    let pki = Pki::new();
    let ipv6 = receiver::ipv6();
    let mut cov = Coverage::new(
        // Without IPv6, `[::]` is `0.0.0.0`.
        (["0.0.0.0", "127.0.0.1"]
            .iter()
            .chain(ipv6.then_some(&"::"))
            .chain(ipv6.then_some(&"::1")))
        .map(|ip| format!("listen {ip}"))
        .chain(["Tcp", "Udp"].map(|t| format!("transport {t}")))
        .chain(["None", "Served", "Vouched"].map(|tls| format!("tls {tls}")))
        .chain(["Nothing", "Trusted", "Stranger"].map(|p| format!("presents {p} under Vouched")))
        .chain(
            [
                "no networks",
                "a sender in its networks",
                "a sender in none of its networks",
            ]
            .map(String::from),
        )
        .chain(FRAMES.iter().chain(ENDINGS).chain(DATAGRAMS).map(|s| s.to_string()))
        .chain(BROKEN_FRAMES.iter().chain(BROKEN_ENDINGS).map(|s| s.to_string()))
        .chain(["none", "Nul", "Start"].map(|b| format!("line breaker {b}")))
        .chain(
            [
                "a connection written in pieces",
                "a TLS sender hanging up without saying so",
            ]
            .map(String::from),
        )
        .chain(RECEIVER_LIMITS.iter().map(|(_, label)| label.to_string())),
    );
    fuzz::run(1..201, |seed| {
        let mut rng = Rng::seeded(seed);
        let settings = random_settings(&mut rng, ipv6);
        let max = settings.max_request_bytes;
        let allowed = admits(&settings);
        // What the receiver is set to is tried only by a sender it allows.
        if allowed && (ipv6 || settings.listen.is_ipv4()) {
            cov.reach(format!("listen {}", settings.listen.ip()));
        }
        if allowed {
            cov.reach(format!("transport {:?}", settings.transport));
            cov.reach(RECEIVER_LIMITS.iter().find(|(below, _)| max < *below).unwrap().1);
            cov.reach(format!("tls {:?}", settings.tls));
            if settings.transport == Transport::Tcp {
                cov.reach(match settings.line_breaker {
                    None => "line breaker none".to_string(),
                    Some(b) => format!("line breaker {b:?}"),
                });
            }
        }
        cov.reach(match (settings.allow_from.is_empty(), allowed) {
            (true, _) => "no networks",
            (false, true) => "a sender in its networks",
            (false, false) => "a sender in none of its networks",
        });
        let mut source = open(&settings, &pki);
        let remote = settings.connect.ip().to_string();
        // What each sender sends must arrive, in the order it was sent: each sender waits for the receiver to hang up
        // before the next.
        let mut expected: Vec<Vec<u8>> = Vec::new();
        let mut labels: Vec<String> = Vec::new();
        let mut what = Vec::new();
        match settings.transport {
            Transport::Tcp => {
                for n in 0..1 + rng.below(4) {
                    let mut reached = Vec::new();
                    let mut connection = match settings.line_breaker {
                        None => connection(&mut rng, n, max, &mut reached),
                        Some(breaker) => broken_connection(&mut rng, n, max, breaker, &mut reached),
                    };
                    if settings.tls != Tls::None {
                        connection.presents = *rng.pick(&[
                            Presents::Trusted,
                            Presents::Trusted,
                            Presents::Nothing,
                            Presents::Stranger,
                        ]);
                    }
                    let mut dropped = false;
                    if settings.tls != Tls::None && rng.chance(4) {
                        connection.abrupt = true;
                        // Hanging up unsaid reads as cut short, so what waited on the hang-up is not taken.
                        if connection.ended_by_hanging_up {
                            connection.taken.pop();
                            reached.retain(|label| {
                                !ENDINGS.contains(&label.as_str()) && !BROKEN_ENDINGS.contains(&label.as_str())
                            });
                            dropped = true;
                        }
                    }
                    if settings.tls == Tls::Vouched && allowed {
                        cov.reach(format!("presents {:?} under Vouched", connection.presents));
                    }
                    let tls_admits = settings.tls != Tls::Vouched || connection.presents == Presents::Trusted;
                    what.push(format!(
                        "connection {n}, presenting {:?}: {:?}",
                        connection.presents,
                        String::from_utf8_lossy(&connection.bytes[..connection.bytes.len().min(4096)])
                    ));
                    if allowed && tls_admits {
                        labels.extend(reached);
                        if dropped {
                            labels.push("a TLS sender hanging up without saying so".into());
                        }
                        expected.extend(connection.taken.iter().cloned());
                    }
                    send(settings.connect, settings.tls, &pki, &connection)
                        .map_err(|why| format!("{}: {why}", what.last().unwrap()))?;
                }
            }
            Transport::Udp => {
                let local: SocketAddr = match settings.connect {
                    SocketAddr::V4(_) => "127.0.0.1:0".parse().unwrap(),
                    SocketAddr::V6(_) => "[::1]:0".parse().unwrap(),
                };
                let sender = UdpSocket::bind(local).unwrap();
                for n in 0..1 + rng.below(12) {
                    let mut label = String::new();
                    let (datagram, record) = datagram(&mut rng, n, max, &mut label);
                    what.push(format!(
                        "datagram {n}: {:?}",
                        String::from_utf8_lossy(&datagram[..datagram.len().min(256)])
                    ));
                    if allowed {
                        labels.push(label);
                        expected.extend(record);
                    }
                    sender.send_to(&datagram, settings.connect).unwrap();
                }
            }
        }
        let what = format!(
            "{:?} receiver on {} allowing {:?} under {:?}, limit {max}: {}",
            settings.transport,
            settings.listen,
            settings.allow_from,
            settings.tls,
            what.join("; ")
        );

        // Nothing says when a receiver is done, so it is given a moment once all that must arrive has.
        let mut records = Vec::new();
        let deadline = Instant::now() + Duration::from_secs(20);
        receiver::drain(
            &mut source,
            Duration::from_millis(20),
            Duration::from_millis(200),
            deadline,
            |batch| {
                records.extend(read_back(batch, &remote).map_err(|why| format!("{what}: {why}"))?);
                Ok(records.len() >= expected.len())
            },
        )?;
        if records != expected {
            let show = |records: &[Vec<u8>]| -> Vec<String> {
                records
                    .iter()
                    .map(|r| String::from_utf8_lossy(&r[..r.len().min(200)]).to_string())
                    .collect()
            };
            return Err(format!(
                "{what}\ntook {} records, not {}:\ntook     {:?}\nexpected {:?}",
                records.len(),
                expected.len(),
                show(&records),
                show(&expected)
            ));
        }
        for label in labels {
            cov.reach(label);
        }
        Ok(())
    });
    cov.assert_complete();
}

/// A real syslog sender, and how a live seed told it to send.
#[derive(Debug)]
enum Sender {
    /// util-linux `logger`, sending each line of a file. It speaks no TLS.
    Logger { rfc5424: bool, octet_count: bool },
    /// Fluent Bit's `syslog` output, tailing a file.
    FluentBit { rfc5424: bool },
    /// The OpenTelemetry Collector's `syslog` exporter, tailing a file.
    Collector { rfc5424: bool, octet_count: bool },
}

/// The image each sender runs in.
struct Images {
    logger: String,
    fluent_bit: String,
    collector: String,
}

fn format(rfc5424: bool) -> &'static str {
    if rfc5424 { "rfc5424" } else { "rfc3164" }
}

fn network(transport: Transport) -> &'static str {
    match transport {
        Transport::Tcp => "tcp",
        Transport::Udp => "udp",
    }
}

impl Sender {
    /// What `docker run` takes after its options: the image and its command, sending to the receiver at `at`.
    fn command(&self, at: SocketAddr, transport: Transport, images: &Images) -> Vec<String> {
        match self {
            Sender::Logger { rfc5424, octet_count } => {
                let port = at.port().to_string();
                let mut command = vec![
                    images.logger.clone(),
                    "logger".into(),
                    format!("--{}", network(transport)),
                    format!("--{}", format(*rfc5424)),
                ];
                command.extend(["--server", "127.0.0.1", "--port", &port, "--tag", "smeltr-live"].map(String::from));
                if *octet_count {
                    command.push("--octet-count".into());
                }
                command.extend(["-f", "/smeltr/app.log"].map(String::from));
                command
            }
            Sender::FluentBit { .. } => vec![
                images.fluent_bit.clone(),
                "/fluent-bit/bin/fluent-bit".into(),
                "-c".into(),
                "/smeltr/fluent-bit.conf".into(),
            ],
            Sender::Collector { .. } => vec![
                images.collector.clone(),
                "--config".into(),
                "/smeltr/collector.yaml".into(),
            ],
        }
    }

    /// The file the sender reads its configuration from, and what it holds: tail `app.log`, and send to the receiver
    /// at `at` over `transport`, under TLS where `tls` says. `None` for `logger`, told all on its command line.
    fn config(&self, at: SocketAddr, transport: Transport, tls: Tls) -> Option<(&'static str, String)> {
        let port = at.port();
        match self {
            Sender::Logger { .. } => None,
            Sender::FluentBit { rfc5424 } => {
                let mode = if tls == Tls::None { network(transport) } else { "tls" };
                let mut output = format!(
                    "[OUTPUT]\n    Name syslog\n    Match *\n    Host 127.0.0.1\n    Port {port}\n    Mode {mode}\n    \
                     Syslog_Format {}\n    Syslog_Message_Key log\n    Retry_Limit 10\n",
                    format(*rfc5424)
                );
                if tls != Tls::None {
                    output.push_str(
                        "    tls On\n    tls.verify On\n    tls.ca_file /smeltr/ca.pem\n    tls.vhost localhost\n",
                    );
                }
                if tls == Tls::Vouched {
                    output.push_str("    tls.crt_file /smeltr/sender.pem\n    tls.key_file /smeltr/sender-key.pem\n");
                }
                Some((
                    "fluent-bit.conf",
                    format!(
                        "[SERVICE]\n    Flush 1\n    Grace 1\n    Log_Level info\n[INPUT]\n    Name tail\n    \
                         Path /smeltr/app.log\n    Read_from_Head On\n{output}"
                    ),
                ))
            }
            Sender::Collector { rfc5424, octet_count } => {
                let tls = match tls {
                    Tls::None => "{insecure: true}".to_string(),
                    Tls::Served => "{ca_file: /smeltr/ca.pem, server_name_override: localhost}".to_string(),
                    Tls::Vouched => "{ca_file: /smeltr/ca.pem, server_name_override: localhost, cert_file: \
                                     /smeltr/sender.pem, key_file: /smeltr/sender-key.pem}"
                        .to_string(),
                };
                // The exporter sends a log's `message` attribute, so the line read moves there.
                Some((
                    "collector.yaml",
                    format!(
                        "receivers:\n  filelog:\n    include: [/smeltr/app.log]\n    start_at: beginning\n    \
                         operators:\n      - {{type: move, from: body, to: attributes.message}}\nexporters:\n  \
                         syslog:\n    endpoint: 127.0.0.1\n    port: {port}\n    network: {}\n    protocol: {}\n    \
                         enable_octet_counting: {octet_count}\n    tls: {tls}\n    sending_queue: {{enabled: false}}\n    \
                         retry_on_failure: {{enabled: true, initial_interval: 100ms, max_interval: 1s}}\nservice:\n  \
                         telemetry:\n    metrics: {{level: none}}\n  pipelines:\n    logs:\n      receivers: [filelog]\n      \
                         exporters: [syslog]\n",
                        network(transport),
                        format(*rfc5424)
                    ),
                ))
            }
        }
    }

    /// Whether the sender's own log, or for `logger` its exit, in container `name` says it failed to send.
    fn failed(&self, name: &str) -> Option<String> {
        let logs = container_logs(name);
        let failed = match self {
            Sender::Logger { .. } => {
                let inspect = std::process::Command::new("docker")
                    .args(["inspect", "-f", "{{.State.ExitCode}}", name])
                    .output()
                    .unwrap();
                String::from_utf8_lossy(&inspect.stdout).trim() != "0"
            }
            Sender::FluentBit { .. } => logs.contains("[error]") || logs.contains("[ warn]"),
            Sender::Collector { .. } => logs.contains("\terror\t") || logs.contains("\twarn\t"),
        };
        failed.then_some(logs)
    }

    /// Whether the messages it sends keep a newline at their end: the collector's octet-counted ones count it in.
    fn keeps_newline(&self) -> bool {
        matches!(self, Sender::Collector { octet_count: true, .. })
    }
}

/// The line a record carries: what follows the last `line ` in it, but for a newline at its end where `newline`. Why
/// the record is not a syslog message holding one, where it is not.
fn line_of(record: &[u8], newline: bool) -> Result<String, String> {
    let text = String::from_utf8(record.to_vec()).map_err(|_| format!("a record not UTF-8: {record:?}"))?;
    let text = match newline {
        true => text
            .strip_suffix('\n')
            .ok_or_else(|| format!("a record with no newline: {text:?}"))?,
        false => &text,
    };
    if !text.starts_with('<') {
        return Err(format!("a record not starting with its priority: {text:?}"));
    }
    let at = text
        .rfind("line ")
        .ok_or_else(|| format!("a record holding no line: {text:?}"))?;
    Ok(text[at..].to_string())
}

/// Real syslog senders (`logger`, Fluent Bit and the OpenTelemetry Collector) read a file of random lines and send
/// them to receivers of random settings, in RFC 3164 or RFC 5424, octet-counted or not where the sender can say, over
/// TCP or UDP, and over TLS with or without a client certificate where the sender speaks it. Every line arrives exactly
/// once, the receiver never panics, and the sender logs no failure to send.
#[test]
fn live_senders_deliver_every_line_once_under_random_configurations() {
    const TEST: &str = "live_senders_deliver_every_line_once_under_random_configurations";
    let (Some(logger), Some(fluent_bit), Some(collector)) = (
        crate::live::syslog_sender(TEST),
        crate::live::fluent_bit(TEST),
        crate::live::otel_collector(TEST),
    ) else {
        return;
    };
    let images = Images {
        logger,
        fluent_bit,
        collector,
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
    // Each sender over each transport, and under each TLS it speaks.
    let ways = [
        ("logger", Transport::Tcp, Tls::None),
        ("logger", Transport::Udp, Tls::None),
        ("Fluent Bit", Transport::Tcp, Tls::None),
        ("Fluent Bit", Transport::Tcp, Tls::Served),
        ("Fluent Bit", Transport::Tcp, Tls::Vouched),
        ("Fluent Bit", Transport::Udp, Tls::None),
        ("the collector", Transport::Tcp, Tls::None),
        ("the collector", Transport::Tcp, Tls::Served),
        ("the collector", Transport::Tcp, Tls::Vouched),
        ("the collector", Transport::Udp, Tls::None),
    ];
    let mut cov = Coverage::new(
        (ways
            .iter()
            .map(|(sender, transport, tls)| format!("{sender} over {transport:?} under {tls:?}")))
        .chain(
            ["logger", "Fluent Bit", "the collector"]
                .into_iter()
                .flat_map(|sender| [format!("{sender} in rfc3164"), format!("{sender} in rfc5424")]),
        )
        .chain(["logger", "the collector"].into_iter().flat_map(|sender| {
            [
                format!("{sender} octet-counting in rfc3164"),
                format!("{sender} octet-counting in rfc5424"),
            ]
        }))
        .filter(|label| label != "the collector octet-counting in rfc3164"),
    );

    fuzz::run(1..41, |seed| {
        // Receivers share one runtime, so a panic is this seed's while the test runs alone, as `just check-live` runs it.
        panicked.store(false, std::sync::atomic::Ordering::Relaxed);
        let mut rng = Rng::seeded(seed);
        let (name, transport, tls) = ways[(seed as usize - 1) % ways.len()];
        let mut settings = random_settings(&mut rng, ipv6);
        // A sender reaches the receiver over IPv4, on the host's network, by the name its certificate is for, from a
        // network the receiver admits.
        settings.connect = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), settings.connect.port());
        if !settings.listen.ip().is_unspecified() {
            settings.listen = settings.connect;
        }
        settings.transport = transport;
        settings.tls = tls;
        settings.allow_from = match tls {
            Tls::Vouched if rng.chance(2) => Vec::new(),
            _ => vec![*rng.pick(&["127.0.0.0/8", "127.0.0.1/32", "0.0.0.0/0"])],
        };
        settings.max_request_bytes = 1 << 16;
        // Each way, in each format, counting octets and not, within forty seeds. The collector counts octets only in
        // RFC 5424; Fluent Bit never does, nor does anyone over UDP.
        let (rfc5424, counts) =
            [(false, false), (true, false), (true, true), (false, true)][(seed as usize - 1) / ways.len()];
        let octet_count = counts && transport == Transport::Tcp && (rfc5424 || name == "logger");
        let sender = match name {
            "logger" => Sender::Logger { rfc5424, octet_count },
            "Fluent Bit" => Sender::FluentBit { rfc5424 },
            _ => Sender::Collector { rfc5424, octet_count },
        };
        cov.reach(format!("{name} over {transport:?} under {tls:?}"));
        cov.reach(format!("{name} in {}", format(rfc5424)));
        if octet_count {
            cov.reach(format!("{name} octet-counting in {}", format(rfc5424)));
        }
        let what = format!(
            "{sender:?} over {transport:?} under {tls:?}, to a receiver on {} allowing {:?}, limit {}",
            settings.listen, settings.allow_from, settings.max_request_bytes
        );
        let mut source = open(&settings, &pki);

        let scratch = Scratch(std::env::temp_dir().join(format!("smeltr-syslog-fuzz-{}-{seed}", std::process::id())));
        let dir = &scratch.0;
        std::fs::create_dir_all(dir).unwrap();
        // Datagrams the receiver has not read yet wait in a buffer that a burst could fill.
        let n = 1 + rng.below(if transport == Transport::Udp { 100 } else { 300 });
        // Each line names itself.
        let lines: Vec<String> = (0..n)
            .map(|i| format!("line {seed}-{i} {} été \"q\" \\", "x".repeat(rng.below(200))))
            .collect();
        let file: String = lines.iter().map(|l| format!("{l}\n")).collect();
        std::fs::write(dir.join("app.log"), file).unwrap();
        std::fs::write(dir.join("ca.pem"), &pki.authority.pem).unwrap();
        std::fs::write(dir.join("sender.pem"), &pki.sender.certificate).unwrap();
        std::fs::write(dir.join("sender-key.pem"), &pki.sender.key).unwrap();
        if let Some((file, config)) = sender.config(settings.connect, transport, tls) {
            std::fs::write(dir.join(file), config).unwrap();
        }
        share(dir);
        let container = format!("smeltr-syslog-fuzz-{}-{seed}", std::process::id());
        let started = std::process::Command::new("docker")
            .args(["run", "-d", "--network", "host", "--name", &container, "-v"])
            .arg(format!("{}:/smeltr:ro", dir.display()))
            .args(sender.command(settings.connect, transport, &images))
            .output()
            .expect("docker runs");
        // A run that fails may still have created its container.
        let container = Container(container);
        if !started.status.success() {
            return Err(format!("docker run: {}", String::from_utf8_lossy(&started.stderr)));
        }

        let remote = settings.connect.ip().to_string();
        let mut arrived: BTreeMap<String, usize> = BTreeMap::new();
        let deadline = Instant::now() + Duration::from_secs(90);
        let take = |batch: &[SourceRecord]| {
            for record in read_back(batch, &remote).map_err(|why| format!("{what}: {why}"))? {
                let line = line_of(&record, sender.keeps_newline()).map_err(|why| format!("{what}: {why}"))?;
                if !lines.contains(&line) {
                    return Err(format!("{what}: {line:?} is no line the sender read"));
                }
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
        if let Some(logs) = sender.failed(&container.0) {
            return Err(format!("{what}: the sender failed\n{logs}"));
        }
        Ok(())
    });
    cov.assert_complete();
}
