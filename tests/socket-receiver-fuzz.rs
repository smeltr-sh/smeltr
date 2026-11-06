//! Random senders against socket receivers of random settings and framings. Nothing is acknowledged: a sender forgets
//! a message once written. So every message a receiver admits must arrive once, as sent, and none it refuses: a
//! misframed stream closes the connection, keeping what came before it, a message over the limit arrives truncated to
//! it, the connection reading on, and a message the connection ended partway through is dropped.
//!
//! Each seed opens a receiver under settings an operator could save, over TCP or UDP, with a framing drawn from all
//! there are, and sends it connections or datagrams from senders it admits and senders it does not, written in random
//! pieces, ending cleanly or cut short. A model built beside each sender says which records must arrive. A
//! deterministic PRNG over a seed range, from `harness::fuzz`: a failure names its seed, and the fuzzer checks that
//! its seeds reached every option it knows.

use std::io::{self, Read, Write};
use std::net::{Ipv4Addr, Shutdown, SocketAddr, TcpStream, UdpSocket};
use std::time::{Duration, Instant};

use serde_json::Value;
use smeltr::format::encoding::Encoding;
use smeltr::io::framing::{ByteOrder, Counts, Framing, LengthPrefix};
use smeltr::io::line_breaker::{BREAK_SLACK, LineBreaker};
use smeltr::io::{Source, SourceRecord};
use smeltr::plan::topology::{ConnectionLimits, Receiver, SocketReceiverConfig, SourceMeta, Transport};

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
    framing: Framing,
    encoding: Encoding,
    connections: ConnectionLimits,
}

/// Networks a receiver may admit senders from, and whether a sender on each loopback address is in them.
const NETWORKS: &[(&str, bool, bool)] = &[
    ("127.0.0.0/8", true, false),
    ("127.0.0.1/32", true, false),
    ("10.0.0.0/8", false, false),
    ("::1/128", false, true),
    ("::/0", false, true),
];

/// The framings a receiver is given, by name.
const FRAMINGS: &[&str] = &[
    "lines",
    "escapedLines",
    "lineBreaker",
    "lengthPrefix",
    "fixed",
    "object",
];

/// A line breaker that ends each event with a NUL.
const NUL: &str = r"(\x00)";

fn random_framing(rng: &mut Rng, max: usize, labels: &mut Vec<String>) -> Framing {
    let name = *rng.pick(FRAMINGS);
    labels.push(format!("framing {name}"));
    match name {
        "lines" => Framing::Lines,
        "escapedLines" => Framing::EscapedLines,
        "lineBreaker" => Framing::LineBreaker(LineBreaker(regex::bytes::Regex::new(NUL).unwrap())),
        "lengthPrefix" => {
            let width = *rng.pick(&[1, 2, 3, 4, 8]);
            labels.push(format!("length of {width} bytes"));
            let offset = match rng.chance(2) {
                true => {
                    labels.push("a header before the length".into());
                    1 + rng.below(4)
                }
                false => 0,
            };
            let order = *rng.pick(&[ByteOrder::Big, ByteOrder::Little]);
            labels.push(format!("order {order:?}"));
            let counts = *rng.pick(&[Counts::Rest, Counts::Message]);
            labels.push(format!("counts {counts:?}"));
            Framing::LengthPrefix(LengthPrefix {
                offset,
                width,
                order,
                counts,
            })
        }
        "fixed" => Framing::Fixed(1 + rng.below(max.min(256))),
        _ => Framing::Object,
    }
}

fn random_settings(rng: &mut Rng, ipv6: bool, labels: &mut Vec<String>) -> Settings {
    let (listen, connect) = receiver::random_addresses(rng, ipv6);
    let transport = *rng.pick(&[Transport::Tcp, Transport::Udp]);
    let port = loop {
        let port = receiver::free_port(connect);
        if transport == Transport::Tcp || UdpSocket::bind((connect, port)).is_ok() {
            break port;
        }
    };
    // Datagrams have no TLS, and no framing: a datagram is a record.
    let tls = match transport {
        Transport::Tcp => receiver::random_tls(rng),
        Transport::Udp => Tls::None,
    };
    let allow_from = receiver::random_tokens(rng, tls, |rng, _| rng.pick(NETWORKS).0);
    let max_request_bytes = receiver::random_limit(rng, 6);
    let framing = match transport {
        Transport::Tcp => random_framing(rng, max_request_bytes, labels),
        Transport::Udp => Framing::Lines,
    };
    Settings {
        listen: SocketAddr::new(listen, port),
        connect: SocketAddr::new(connect, port),
        transport,
        allow_from,
        tls,
        max_request_bytes,
        framing,
        encoding: Encoding::Auto,
        connections: ConnectionLimits::default(),
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
    let socket = Receiver::Socket(SocketReceiverConfig {
        allow_from: settings.allow_from.iter().map(|n| n.parse().unwrap()).collect(),
        transport: settings.transport,
        framing: settings.framing.clone(),
        encoding: settings.encoding,
    });
    let (mut config, secrets) = receiver::configured(
        socket,
        settings.listen,
        settings.tls,
        &[],
        "sender",
        settings.max_request_bytes,
        pki,
    );
    config.connections = settings.connections;
    receiver::opened(config, &secrets)
}

/// What one connection writes, and the records the receiver must take of it.
struct Connection {
    bytes: Vec<u8>,
    /// How many bytes it writes at a time.
    pieces: Vec<usize>,
    presents: Presents,
    /// Whether it hangs up over TLS without saying so first, as a TLS sender may.
    abrupt: bool,
    taken: Vec<Vec<u8>>,
    /// Whether its last record is one the receiver takes only once the connection ends.
    ended_by_hanging_up: bool,
}

/// Text a record may hold: never a line end, a NUL or a backslash, and never only whitespace.
fn text(rng: &mut Rng, n: usize, i: usize) -> Vec<u8> {
    let mut text = format!("c{n}m{i}").into_bytes();
    for _ in 0..rng.below(40) {
        text.push(*rng.pick(b"abcxyz019 \t|{}\xc3\xa9\xff"));
    }
    text
}

/// Any bytes at all.
fn binary(rng: &mut Rng, len: usize) -> Vec<u8> {
    (0..len).map(|_| rng.below(256) as u8).collect()
}

/// `record` as a `bytes` object destination escapes a line.
fn escaped(record: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    for &b in record {
        match b {
            b'\\' => out.extend_from_slice(b"\\\\"),
            b'\n' => out.extend_from_slice(b"\\n"),
            b'\r' => out.extend_from_slice(b"\\r"),
            b => out.push(b),
        }
    }
    out
}

/// A message over `max`, of text that is not whitespace, and what the receiver keeps of it. It stays within the bytes a
/// line breaker holds past the limit while it looks for a break (`BREAK_SLACK`), so under a line breaker the receiver
/// still waits for the message's break, as for any other.
fn oversized(rng: &mut Rng, max: usize, labels: &mut Vec<String>) -> (Vec<u8>, Vec<u8>) {
    labels.push("a frame over the limit".into());
    let record = vec![b'o'; max + 1 + rng.below((3 * max).min(BREAK_SLACK / 2))];
    let kept = record[..max].to_vec();
    (record, kept)
}

/// A random connection, the `n`th, to a receiver of `framing` reading messages of at most `max` bytes, and the
/// labels it reaches where the receiver reads it.
fn connection(rng: &mut Rng, n: usize, framing: &Framing, max: usize, labels: &mut Vec<String>) -> Connection {
    let (mut bytes, mut taken) = (Vec::new(), Vec::new());
    let mut ended_by_hanging_up = false;
    let records = rng.below(8);
    match framing {
        Framing::Lines | Framing::LineBreaker(_) => {
            let lines = *framing == Framing::Lines;
            for i in 0..records {
                let last = i + 1 == records;
                let (record, kept) = match rng.below(8) {
                    0 => oversized(rng, max, labels),
                    // A line breaker skips a blank event; a line is kept, blank or not.
                    1 if lines => {
                        labels.push("a blank line".into());
                        (Vec::new(), Vec::new())
                    }
                    _ => {
                        let record = text(rng, n, i);
                        (record.clone(), record)
                    }
                };
                bytes.extend(&record);
                // A line over the limit is taken as soon as the limit arrives, never on the hang-up.
                if last && !record.is_empty() && record == kept && rng.chance(2) {
                    labels.push("an unterminated last frame".into());
                    ended_by_hanging_up = true;
                } else if lines {
                    bytes.extend_from_slice(rng.pick(&[&b"\n"[..], b"\r\n"]));
                } else {
                    bytes.push(0);
                    // A break at the very end of what the receiver has read could still grow as more bytes come, so
                    // the receiver holds the last message until the sender hangs up.
                    ended_by_hanging_up |= last;
                }
                taken.push(kept);
            }
        }
        Framing::EscapedLines => {
            for _ in 0..records {
                // Short enough that escaping it keeps it under the limit.
                let len = rng.below(max.min(512) / 2);
                let record = binary(rng, len);
                bytes.extend(escaped(&record));
                bytes.push(b'\n');
                taken.push(record);
            }
            if records > 0 {
                labels.push("escaped lines".into());
            }
        }
        Framing::LengthPrefix(prefix) => {
            let header = prefix.offset + prefix.width;
            let most = match prefix.width {
                8 => u64::MAX,
                width => (1u64 << (8 * width)) - 1,
            };
            for i in 0..records {
                let last = i + 1 == records;
                let mut rest_len = rng.below(64);
                if rng.chance(8) {
                    labels.push("a frame over the limit".into());
                    rest_len = max + rng.below(max);
                }
                let frame_len = header + rest_len;
                let length = match prefix.counts {
                    Counts::Rest => rest_len as u64,
                    Counts::Message => frame_len as u64,
                };
                if length > most {
                    continue;
                }
                let misframed = prefix.counts == Counts::Message && last && rng.chance(4);
                let length = match misframed {
                    true => rng.below(header) as u64,
                    false => length,
                };
                let mut frame = binary(rng, prefix.offset);
                match prefix.order {
                    ByteOrder::Big => frame.extend_from_slice(&length.to_be_bytes()[8 - prefix.width..]),
                    ByteOrder::Little => frame.extend_from_slice(&length.to_le_bytes()[..prefix.width]),
                }
                frame.extend(binary(rng, rest_len));
                if misframed {
                    // Shorter than its own header: the stream is misframed from here, and the connection closed.
                    labels.push("a length shorter than its header".into());
                    bytes.extend(frame);
                    break;
                }
                // Cut short under the limit, so nothing of it was taken.
                if last && frame.len() <= max && rng.chance(3) {
                    labels.push("a frame cut short".into());
                    let cut = rng.below(frame.len());
                    bytes.extend(&frame[..cut]);
                    break;
                }
                bytes.extend(&frame);
                frame.truncate(max);
                taken.push(frame);
            }
        }
        Framing::Fixed(size) => {
            for i in 0..records {
                let frame = binary(rng, *size);
                if i + 1 == records && rng.chance(3) {
                    labels.push("a frame cut short".into());
                    bytes.extend(&frame[..rng.below(*size)]);
                    break;
                }
                bytes.extend(&frame);
                taken.push(frame);
            }
        }
        Framing::Object => {
            if records > 0 {
                let len = rng.below(2 * max);
                let record = binary(rng, len);
                if record.len() > max {
                    labels.push("a frame over the limit".into());
                }
                bytes.extend(&record);
                if !record.is_empty() {
                    taken.push(record[..record.len().min(max)].to_vec());
                    // One over the limit is taken as soon as the limit arrives.
                    ended_by_hanging_up = record.len() <= max;
                }
            }
        }
        Framing::Syslog(_) => unreachable!("a socket receiver draws no syslog framing"),
    }
    let pieces = pieces(rng, bytes.len(), labels);
    Connection {
        bytes,
        pieces,
        presents: Presents::Nothing,
        abrupt: false,
        taken,
        ended_by_hanging_up,
    }
}

/// Random lengths that sum to `len`.
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

/// The kinds of datagram a sender sends.
const DATAGRAMS: &[&str] = &[
    "a datagram",
    "a datagram with a line end",
    "an empty datagram",
    "a datagram over the limit",
];

/// One datagram, and the record it must give, if any: what was sent, a line end and all, up to the limit.
fn datagram(rng: &mut Rng, n: usize, max: usize, label: &mut String) -> (Vec<u8>, Option<Vec<u8>>) {
    let kind = rng.below(DATAGRAMS.len());
    *label = DATAGRAMS[kind].to_string();
    let named = format!("d{n} ").into_bytes();
    let datagram = match kind {
        0 => {
            let len = rng.below(64);
            [named, binary(rng, len)].concat()
        }
        1 => [named, b"\r\n".to_vec()].concat(),
        2 => Vec::new(),
        _ => [named, vec![b'o'; max.min(60_000) + 1]].concat(),
    };
    let record = (!datagram.is_empty()).then(|| datagram[..datagram.len().min(max)].to_vec());
    (datagram, record)
}

/// The bytes of the records a socket receiver recorded as sent from `remote`, or an error naming the first record
/// recorded otherwise.
fn read_back(records: &[SourceRecord], remote: &str) -> Result<Vec<Vec<u8>>, String> {
    let objects = SourceMeta::SOCKET_RECEIVER.objects(records, Some("raw"));
    (records.iter().zip(objects))
        .map(|(r, object)| {
            let text = |key: &str| object.get(key).and_then(Value::as_str).map(str::to_string);
            if text("remote").as_deref() != Some(remote) || text("type").as_deref() != Some("socketReceiver") {
                return Err(format!("a record filed as from another: {object:?}"));
            }
            Ok(r.bytes().unwrap().to_vec())
        })
        .collect()
}

/// Writes `connection` to `at` over a new connection, under TLS where `tls` says, in its pieces, then hangs up, and
/// waits for the receiver to hang up too. A receiver that closes the connection leaves the rest unwritten. Why the
/// receiver did not hang up, or `Ok`.
fn send(at: SocketAddr, tls: Tls, pki: &Pki, connection: &Connection) -> Result<(), String> {
    let Ok(tcp) = TcpStream::connect(at) else {
        return Ok(());
    };
    tcp.set_write_timeout(Some(Duration::from_secs(10))).unwrap();
    tcp.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    // Each piece its own segment, so the receiver more often reads a message in parts.
    tcp.set_nodelay(true).unwrap();
    // Closing a socket with unread data resets the connection, and a reset can drop what the receiver has not yet
    // read. So the sender reads until the receiver is done.
    let hang_up = |stream: &mut dyn Read, tcp: &TcpStream| {
        let _ = tcp.shutdown(Shutdown::Write);
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
            if !connection.abrupt {
                stream.conn.send_close_notify();
                let _ = stream.flush();
            }
            let tcp = stream.sock.try_clone().unwrap();
            hang_up(&mut stream, &tcp)
        }
    }
}

/// Every label a seed range must reach.
fn options(ipv6: bool) -> impl Iterator<Item = String> {
    (["0.0.0.0", "127.0.0.1"]
        .iter()
        .chain(ipv6.then_some(&"::"))
        .chain(ipv6.then_some(&"::1")))
    .map(|ip| format!("listen {ip}"))
    .chain(["Tcp", "Udp"].map(|t| format!("transport {t}")))
    .chain(["None", "Served", "Vouched"].map(|tls| format!("tls {tls}")))
    .chain(["Nothing", "Trusted", "Stranger"].map(|p| format!("presents {p} under Vouched")))
    .chain(FRAMINGS.iter().map(|f| format!("framing {f}")))
    .chain([1, 2, 3, 4, 8].map(|w| format!("length of {w} bytes")))
    .chain(["Big", "Little"].map(|o| format!("order {o}")))
    .chain(["Rest", "Message"].map(|c| format!("counts {c}")))
    .chain(DATAGRAMS.iter().map(|d| d.to_string()))
    .chain(
        [
            "no networks",
            "a sender in its networks",
            "a sender in none of its networks",
            "a header before the length",
            "a blank line",
            "escaped lines",
            "an unterminated last frame",
            "a frame over the limit",
            "a length shorter than its header",
            "a frame cut short",
            "a connection written in pieces",
            "a TLS sender hanging up without saying so",
        ]
        .map(String::from),
    )
}

/// A receiver under any settings an operator can save takes every message an admitted sender writes whole, once and as
/// written, however it is framed and written, and nothing from a sender it does not admit or after a misframed one.
#[test]
fn every_framing_takes_each_frame_once_however_it_is_written() {
    crate::certs::install_provider();
    let pki = Pki::new();
    let ipv6 = receiver::ipv6();
    let mut cov = Coverage::new(options(ipv6));
    fuzz::run(1..301, |seed| {
        let mut rng = Rng::seeded(seed);
        let mut setting_labels = Vec::new();
        let settings = random_settings(&mut rng, ipv6, &mut setting_labels);
        let max = settings.max_request_bytes;
        let allowed = admits(&settings);
        cov.reach(match (settings.allow_from.is_empty(), allowed) {
            (true, _) => "no networks",
            (false, true) => "a sender in its networks",
            (false, false) => "a sender in none of its networks",
        });
        let mut source = open(&settings, &pki);
        let remote = settings.connect.ip().to_string();
        let mut expected: Vec<Vec<u8>> = Vec::new();
        let mut labels: Vec<String> = Vec::new();
        let mut what = Vec::new();
        match settings.transport {
            Transport::Tcp => {
                for n in 0..1 + rng.below(4) {
                    let mut reached = Vec::new();
                    let mut connection = connection(&mut rng, n, &settings.framing, max, &mut reached);
                    if settings.tls != Tls::None {
                        connection.presents = *rng.pick(&[
                            Presents::Trusted,
                            Presents::Trusted,
                            Presents::Nothing,
                            Presents::Stranger,
                        ]);
                    }
                    // A TLS sender may hang up without a close_notify, which the receiver takes as a clean hang-up.
                    connection.abrupt = settings.tls != Tls::None && rng.chance(4);
                    if connection.abrupt {
                        reached.push("a TLS sender hanging up without saying so".into());
                    }
                    if settings.tls == Tls::Vouched && allowed {
                        cov.reach(format!("presents {:?} under Vouched", connection.presents));
                    }
                    let tls_admits = settings.tls != Tls::Vouched || connection.presents == Presents::Trusted;
                    what.push(format!(
                        "connection {n}, presenting {:?}: {:?}",
                        connection.presents,
                        String::from_utf8_lossy(&connection.bytes[..connection.bytes.len().min(512)])
                    ));
                    if allowed && tls_admits {
                        labels.extend(reached);
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
                    what.push(format!("datagram {n} of {} bytes", datagram.len()));
                    if allowed {
                        labels.push(label);
                        expected.extend(record);
                    }
                    sender.send_to(&datagram, settings.connect).unwrap();
                }
            }
        }
        let what = format!(
            "{:?} receiver on {} allowing {:?} under {:?}, limit {max}, framing {:?}: {}",
            settings.transport,
            settings.listen,
            settings.allow_from,
            settings.tls,
            settings.framing,
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
                    .map(|r| String::from_utf8_lossy(&r[..r.len().min(80)]).to_string())
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
        // What the receiver is set to is tried only by a sender it allows.
        if allowed {
            if ipv6 || settings.listen.is_ipv4() {
                cov.reach(format!("listen {}", settings.listen.ip()));
            }
            cov.reach(format!("transport {:?}", settings.transport));
            cov.reach(format!("tls {:?}", settings.tls));
            for label in setting_labels.into_iter().chain(labels) {
                cov.reach(label);
            }
        }
        Ok(())
    });
    cov.assert_complete();
}

// -- live: our own sender, misbehaving ----------------------------------------------------------------------------

/// How a live connection ends.
#[derive(Clone, Copy, Debug, PartialEq)]
enum End {
    /// A hang-up; under TLS, one said first with close_notify.
    HangUp,
    /// Under TLS, a hang-up without a close_notify.
    Unsaid,
    /// A reset.
    Reset,
    /// It shuts down writing and holds the connection open a while.
    HalfClose,
}

/// What one live connection is to the receiver.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Admitted {
    Yes,
    /// From an address in none of its networks.
    Address,
    /// Under TLS, presenting no certificate or a stranger's where one is required.
    Certificate,
    /// Plain TCP to a TLS receiver.
    Plain,
    /// At its maxConnections, none of the connections it holds has been idle long enough to close.
    Full,
    /// At its maxConnectionsPerSender from the sender, the same way.
    FullFromSender,
}

/// The idle timeout a seed that draws one sets: longer than any pause a connection makes partway.
const IDLE_TIMEOUT: Duration = Duration::from_secs(3);

/// The records a live sender's connection must give, and those it may.
struct Expect {
    must: Vec<Vec<u8>>,
    may: Vec<Vec<u8>>,
}

/// Many well-formed messages in `framing`, enough that a receiver not drained fills and stops reading.
fn bulk(framing: &Framing, max: usize) -> (Vec<u8>, Vec<Vec<u8>>) {
    let (mut bytes, mut taken) = (Vec::new(), Vec::new());
    for i in 0..40_000 {
        let message = format!("bulk {i} {}", "b".repeat(i % 64)).into_bytes();
        match framing {
            Framing::Lines | Framing::EscapedLines => {
                bytes.extend(&message);
                bytes.push(b'\n');
            }
            Framing::LineBreaker(_) => {
                bytes.extend(&message);
                bytes.push(0);
            }
            Framing::LengthPrefix(prefix) => {
                let len = match prefix.counts {
                    Counts::Rest => message.len(),
                    Counts::Message => prefix.offset + prefix.width + message.len(),
                } as u64;
                if prefix.width < 8 && len >= 1 << (8 * prefix.width) {
                    continue;
                }
                bytes.extend(vec![b'h'; prefix.offset]);
                match prefix.order {
                    ByteOrder::Big => bytes.extend_from_slice(&len.to_be_bytes()[8 - prefix.width..]),
                    ByteOrder::Little => bytes.extend_from_slice(&len.to_le_bytes()[..prefix.width]),
                }
                bytes.extend(&message);
                let mut frame = vec![b'h'; prefix.offset];
                frame
                    .extend_from_slice(&bytes[bytes.len() - message.len() - prefix.width..bytes.len() - message.len()]);
                frame.extend(&message);
                frame.truncate(max);
                taken.push(frame);
                continue;
            }
            Framing::Fixed(size) => {
                let mut frame = message.clone();
                frame.resize(*size, b'.');
                bytes.extend(&frame);
                taken.push(frame);
                continue;
            }
            Framing::Object | Framing::Syslog(_) => unreachable!("no bulk for one record"),
        }
        let mut kept = message;
        kept.truncate(max);
        taken.push(kept);
    }
    (bytes, taken)
}

fn b64(bytes: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

/// Every label the live seed range must reach.
fn live_options() -> impl Iterator<Item = String> {
    ["Tcp", "Udp"]
        .map(|t| format!("transport {t}"))
        .into_iter()
        .chain(["None", "Served", "Vouched"].map(|tls| format!("tls {tls}")))
        .chain(FRAMINGS.iter().map(|f| format!("framing {f}")))
        .chain(["HangUp", "Unsaid", "Reset", "HalfClose"].map(|e| format!("end {e}")))
        .chain(["Address", "Certificate", "Plain", "Full", "FullFromSender"].map(|a| format!("refused by {a}")))
        .chain(DATAGRAMS.iter().map(|d| d.to_string()))
        .chain(
            [
                "encoding none",
                "a frame over the limit",
                "a frame cut short",
                "a length shorter than its header",
                "an unterminated last frame",
                "a connection written in pieces",
                "a connection stalled partway",
                "connections at once",
                "an idle connection held",
                "an idle connection closed by its timeout",
                "a handshake stalled",
                "a storm of connections",
                "a receiver left full",
                "a datagram from outside its networks",
                "a flood of datagrams",
            ]
            .map(String::from),
        )
}

/// Our own sender, in a container on the host's network, misbehaves at receivers of random settings in every way
/// a bad sender may: messages in pieces down to a byte, over the limit, misframed, cut short; connections
/// at once, stalled, held idle, half-closed, reset, refused by address, by certificate and for speaking plain TCP to
/// TLS; handshakes stalled; storms of connections; a receiver left full; empty, oversized, foreign and flooding
/// datagrams. Every message an admitted connection completes arrives once and as sent, nothing arrives that was
/// not sent, a reset connection gives none of what it cut short, and the receiver never panics.
#[test]
fn live_misbehaving_senders_never_get_a_record_invented_or_lost() {
    const TEST: &str = "live_misbehaving_senders_never_get_a_record_invented_or_lost";
    let Some(image) = crate::live::socket_sender(TEST) else {
        return;
    };
    let panicked = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let seen = panicked.clone();
    let _hook = Hook::set(Box::new(move |info| {
        if std::thread::current().name() == Some("smeltr-receiver") {
            seen.store(true, std::sync::atomic::Ordering::Relaxed);
        }
        eprintln!("{info}");
    }));
    crate::certs::install_provider();
    let pki = Pki::new();
    let script = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/live/socket-sender/send.py");
    let mut cov = Coverage::new(live_options());
    fuzz::run(1..61, |seed| {
        let mut rng = Rng::seeded(seed);
        let mut labels: Vec<String> = Vec::new();
        let transport = match seed % 3 {
            0 => Transport::Udp,
            _ => Transport::Tcp,
        };
        let connect: SocketAddr = (Ipv4Addr::LOCALHOST, 0).into();
        let port = loop {
            let port = receiver::free_port(connect.ip());
            if transport == Transport::Tcp || UdpSocket::bind((connect.ip(), port)).is_ok() {
                break port;
            }
        };
        let tls = match transport {
            Transport::Tcp => *rng.pick(&[Tls::None, Tls::Served, Tls::Vouched]),
            Transport::Udp => Tls::None,
        };
        // 127.0.0.2 is a loopback address outside these networks, from which the sender sends as a stranger.
        let allow_from = match tls == Tls::Vouched && rng.chance(2) {
            true => Vec::new(),
            false => vec!["127.0.0.1/32"],
        };
        let max = receiver::random_limit(&mut rng, 6);
        let framing = match transport {
            Transport::Tcp => random_framing(&mut rng, max, &mut labels),
            Transport::Udp => Framing::Lines,
        };
        let encoding = match rng.chance(2) {
            true => {
                labels.push("encoding none".into());
                Encoding::None
            }
            false => Encoding::Auto,
        };
        let listen = SocketAddr::new(
            std::net::IpAddr::V4(*rng.pick(&[Ipv4Addr::LOCALHOST, Ipv4Addr::UNSPECIFIED])),
            port,
        );
        // A receiver at its cap, filled by idle connections it will not close for a new one before they have idled a
        // minute; or one that closes idle connections.
        let mut connections = ConnectionLimits::default();
        let mut full = None;
        if transport == Transport::Tcp && rng.chance(2) {
            let holders = 1 + rng.below(3);
            match rng.below(3) {
                0 => {
                    connections.max = Some(holders);
                    full = Some((holders, Admitted::Full));
                }
                1 => {
                    connections.max = Some(holders + 1 + rng.below(4));
                    connections.per_sender = Some(holders);
                    full = Some((holders, Admitted::FullFromSender));
                }
                _ => connections.idle = Some(IDLE_TIMEOUT),
            }
        }
        let settings = Settings {
            listen,
            connect: SocketAddr::new(connect.ip(), port),
            transport,
            allow_from,
            tls,
            max_request_bytes: max,
            framing: framing.clone(),
            encoding,
            connections,
        };
        labels.push(format!("transport {transport:?}"));
        labels.push(format!("tls {tls:?}"));
        let mut source = open(&settings, &pki);

        let scratch = Scratch(std::env::temp_dir().join(format!("smeltr-socket-fuzz-{}-{seed}", std::process::id())));
        let dir = &scratch.0;
        std::fs::create_dir_all(dir).unwrap();
        std::fs::copy(&script, dir.join("send.py")).unwrap();
        for (name, pem) in [
            ("ca.pem", &pki.authority.pem),
            ("sender.pem", &pki.sender.certificate),
            ("sender-key.pem", &pki.sender.key),
            ("stranger.pem", &pki.stranger.certificate),
            ("stranger-key.pem", &pki.stranger.key),
        ] {
            std::fs::write(dir.join(name), pem).unwrap();
        }

        let outside = !settings.allow_from.is_empty();
        let mut expects: Vec<Expect> = Vec::new();
        let mut plan_connections = Vec::new();
        let mut holders = Vec::new();
        let (mut plan_datagrams, mut flood) = (Vec::new(), Value::Null);
        let mut what = Vec::new();
        let mut leave_full = false;
        let mut hold_ms = 0;
        // Whether each holder the receiver must have closed by the end, where that is known.
        let mut closes: Vec<Option<bool>> = Vec::new();
        match transport {
            Transport::Tcp => {
                if let Some((n, _)) = full {
                    for _ in 0..n {
                        holders.push(serde_json::json!({"kind": "idle", "from": "127.0.0.1"}));
                        // Under TLS its handshake times out, which frees its place.
                        closes.push((tls == Tls::None).then_some(false));
                    }
                }
                if connections.idle.is_some() {
                    holders.push(serde_json::json!({"kind": "idle", "from": "127.0.0.1"}));
                    closes.push(Some(true));
                    hold_ms = 2 * IDLE_TIMEOUT.as_millis() as u64;
                    labels.push("an idle connection closed by its timeout".into());
                }
                let count = 1 + rng.below(12);
                if count > 1 {
                    labels.push("connections at once".into());
                }
                for n in 0..count {
                    let mut reached = Vec::new();
                    // A receiver left full stops reading the bulk's connection, and it would stay full past an idle
                    // timeout, so a seed with an idle timeout sends no bulk.
                    let fill = connections.idle.is_none() && n == 0 && framing != Framing::Object;
                    let mut c = match fill && rng.chance(3) {
                        true => {
                            reached.push("a receiver left full".into());
                            leave_full = true;
                            let (bytes, taken) = bulk(&framing, max);
                            let pieces = vec![bytes.len()];
                            Connection {
                                bytes,
                                pieces,
                                presents: Presents::Nothing,
                                abrupt: false,
                                taken,
                                ended_by_hanging_up: matches!(framing, Framing::LineBreaker(_)),
                            }
                        }
                        false => connection(&mut rng, n, &framing, max, &mut reached),
                    };
                    let mut admitted = Admitted::Yes;
                    let from = match outside && rng.chance(8) {
                        true => {
                            admitted = Admitted::Address;
                            "127.0.0.2"
                        }
                        false => "127.0.0.1",
                    };
                    let presents = match tls {
                        Tls::None => "nothing",
                        _ => match rng.below(8) {
                            0 => "plain",
                            1 => "nothing",
                            2 => "stranger",
                            _ => "trusted",
                        },
                    };
                    if admitted == Admitted::Yes {
                        admitted = match (tls, presents) {
                            (Tls::None, _) => Admitted::Yes,
                            (_, "plain") => Admitted::Plain,
                            (Tls::Vouched, "nothing" | "stranger") => Admitted::Certificate,
                            _ => Admitted::Yes,
                        };
                    }
                    if let (Admitted::Yes, Some((_, refused))) = (admitted, full) {
                        admitted = refused;
                    }
                    let end = match (tls, rng.below(4)) {
                        (_, 0) => End::Reset,
                        (Tls::None, 1) => End::HalfClose,
                        (Tls::None, _) => End::HangUp,
                        (_, 1) => End::Unsaid,
                        _ => End::HangUp,
                    };
                    // A hang-up ends the stream, under TLS with or without a close_notify. A reset cuts it short, so
                    // what waits on the end of the stream is not taken, and what the receiver had not read yet may be
                    // lost.
                    let mut pauses = vec![0u64; c.pieces.len()];
                    if c.pieces.len() > 1 && rng.chance(4) {
                        let at = rng.below(c.pieces.len() - 1);
                        pauses[at] = 1500;
                        reached.push("a connection stalled partway".into());
                    }
                    for pause in &mut pauses {
                        if rng.chance(16) {
                            *pause = rng.below(30) as u64;
                        }
                    }
                    let mut at = 0;
                    let pieces: Vec<String> = (c.pieces.iter())
                        .map(|&piece| {
                            at += piece;
                            b64(&c.bytes[at - piece..at])
                        })
                        .collect();
                    what.push(format!(
                        "connection {n} from {from} presenting {presents}, ending {end:?}, {} bytes in {} pieces: {:?}",
                        c.bytes.len(),
                        c.pieces.len(),
                        String::from_utf8_lossy(&c.bytes[..c.bytes.len().min(200)])
                    ));
                    plan_connections.push(serde_json::json!({
                        "id": n,
                        "from": from,
                        "tls": presents,
                        "pieces": pieces,
                        "pauses_ms": pauses,
                        "end": match end {
                            End::HangUp if tls != Tls::None => "notify",
                            End::HangUp | End::Unsaid => "fin",
                            End::Reset => "rst",
                            End::HalfClose => "half_close",
                        },
                    }));
                    if admitted != Admitted::Yes {
                        cov.reach(format!("refused by {admitted:?}"));
                        expects.push(Expect {
                            must: Vec::new(),
                            may: Vec::new(),
                        });
                        continue;
                    }
                    cov.reach(format!("end {end:?}"));
                    if end == End::Reset && c.ended_by_hanging_up {
                        c.taken.pop();
                        reached.retain(|label| label != "an unterminated last frame");
                    }
                    labels.extend(reached);
                    expects.push(match end {
                        End::Reset => Expect {
                            must: Vec::new(),
                            may: c.taken,
                        },
                        _ => Expect {
                            must: c.taken,
                            may: Vec::new(),
                        },
                    });
                }
                if full.is_none() && rng.chance(3) {
                    holders.push(serde_json::json!({"kind": "idle", "from": "127.0.0.1"}));
                    // Under TLS a stalled handshake times out; without TLS a receiver with no idle timeout keeps the
                    // connection.
                    closes.push((tls == Tls::None && connections.idle.is_none()).then_some(false));
                    labels.push("an idle connection held".into());
                }
                if tls != Tls::None && rng.chance(3) {
                    holders.push(serde_json::json!({"kind": "handshake", "from": "127.0.0.1"}));
                    closes.push(None);
                    labels.push("a handshake stalled".into());
                }
            }
            Transport::Udp => {
                for n in 0..1 + rng.below(12) {
                    let mut label = String::new();
                    let (datagram, record) = datagram(&mut rng, n, max, &mut label);
                    let from = match outside && rng.chance(4) {
                        true => {
                            labels.push("a datagram from outside its networks".into());
                            "127.0.0.2"
                        }
                        false => {
                            labels.push(label);
                            expects.push(Expect {
                                must: record.into_iter().collect(),
                                may: Vec::new(),
                            });
                            "127.0.0.1"
                        }
                    };
                    what.push(format!("datagram {n} of {} bytes from {from}", datagram.len()));
                    plan_datagrams.push(serde_json::json!({"from": from, "bytes": b64(&datagram)}));
                }
                if rng.chance(2) {
                    labels.push("a flood of datagrams".into());
                    flood = serde_json::json!({"from": "127.0.0.1", "count": 2000 + rng.below(20_000), "size": rng.below(1200)});
                }
            }
        }
        let storm = match transport == Transport::Tcp && rng.chance(3) {
            true => {
                labels.push("a storm of connections".into());
                200 + rng.below(800)
            }
            false => 0,
        };
        let plan = serde_json::json!({
            "host": "127.0.0.1",
            "port": port,
            "transport": match transport { Transport::Tcp => "tcp", Transport::Udp => "udp" },
            "tls": match tls {
                Tls::None => Value::Null,
                _ => serde_json::json!({
                    "ca": "/smeltr/ca.pem",
                    "cert": "/smeltr/sender.pem",
                    "key": "/smeltr/sender-key.pem",
                    "stranger_cert": "/smeltr/stranger.pem",
                    "stranger_key": "/smeltr/stranger-key.pem",
                }),
            },
            "holders": holders,
            "hold_ms": hold_ms,
            "storm": storm,
            "storm_from": "127.0.0.1",
            "connections": plan_connections,
            "datagrams": plan_datagrams,
            "flood": flood,
        });
        std::fs::write(dir.join("plan.json"), plan.to_string()).unwrap();
        share(dir);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o777)).unwrap();
        }
        let what = format!(
            "seed {seed}: {transport:?} receiver on {listen} allowing {:?} under {tls:?}, limit {max}, framing {framing:?}, {encoding:?}, {connections:?}: {}",
            settings.allow_from,
            what.join("; ")
        );
        let container = format!("smeltr-socket-fuzz-{}-{seed}", std::process::id());
        let started = std::process::Command::new("docker")
            .args(["run", "-d", "--network", "host", "--name", &container, "-v"])
            .arg(format!("{}:/smeltr", dir.display()))
            .args([
                image.as_str(),
                "python",
                "/smeltr/send.py",
                "/smeltr/plan.json",
                "/smeltr/report.json",
            ])
            .output()
            .expect("docker runs");
        let container = Container(container);
        if !started.status.success() {
            return Err(format!("docker run: {}", String::from_utf8_lossy(&started.stderr)));
        }
        // Left undrained a while, a receiver given a bulk fills and stops reading, and its sender waits.
        if leave_full {
            std::thread::sleep(Duration::from_secs(3));
        }

        let must: Vec<Vec<u8>> = expects.iter().flat_map(|e| e.must.iter().cloned()).collect();
        let remote = settings.connect.ip().to_string();
        let mut records: Vec<Vec<u8>> = Vec::new();
        let report_path = dir.join("report.json");
        let deadline = Instant::now() + Duration::from_secs(120);
        receiver::drain(
            &mut source,
            Duration::from_millis(50),
            Duration::from_millis(1500),
            deadline,
            |batch| {
                records.extend(read_back(batch, &remote).map_err(|why| format!("{what}: {why}"))?);
                if panicked.load(std::sync::atomic::Ordering::Relaxed) {
                    return Err(format!("{what}: the receiver panicked"));
                }
                Ok(records.len() >= must.len() && report_path.exists())
            },
        )?;
        let report: Value = match std::fs::read_to_string(&report_path) {
            Ok(text) => serde_json::from_str(&text).map_err(|e| format!("{what}: the report: {e}"))?,
            Err(_) => {
                return Err(format!(
                    "{what}: the sender wrote no report\n{}",
                    container_logs(&container.0)
                ));
            }
        };
        let show = |r: &[u8]| String::from_utf8_lossy(&r[..r.len().min(80)]).to_string();

        // Every record is a whole message some connection sent, taken at most as often as it was sent. Each
        // datagram of a flood carries its own number and arrives whole, or truncated to the limit.
        let mut left: Vec<Vec<u8>> = records.clone();
        for e in &expects {
            for m in &e.must {
                match left.iter().position(|r| r == m) {
                    Some(i) => {
                        left.remove(i);
                    }
                    None => {
                        return Err(format!(
                            "{what}\na message that must arrive did not: {:?}\ntook {} records, of which {:?}\nreport {report}",
                            show(m),
                            records.len(),
                            records.iter().take(20).map(|r| show(r)).collect::<Vec<_>>()
                        ));
                    }
                }
            }
        }
        for e in &expects {
            for m in &e.may {
                if let Some(i) = left.iter().position(|r| r == m) {
                    left.remove(i);
                }
            }
        }
        if let Some(size) = flood.get("size").and_then(Value::as_u64) {
            let filler = "f".repeat(size as usize);
            let mut named = std::collections::BTreeSet::new();
            let mut extra = Vec::new();
            for r in left.drain(..) {
                let text = String::from_utf8_lossy(&r).to_string();
                let name = text.split(' ').next().unwrap_or("").to_string();
                let whole = format!("{name} {filler}");
                let kept = &whole.as_bytes()[..whole.len().min(max)];
                match name.starts_with('f') && r == kept && named.insert(name) {
                    true => {}
                    false => extra.push(r),
                }
            }
            left = extra;
        }
        if let Some(r) = left.first() {
            return Err(format!(
                "{what}\na record no sender sent whole, or sent fewer times: {:?} (of {} left)\nreport {report}",
                show(r),
                left.len()
            ));
        }
        // Each connection's whole messages in the order sent.
        for e in &expects {
            let mut at = 0;
            for m in &e.must {
                match records[at..].iter().position(|r| r == m) {
                    Some(i) => at += i + 1,
                    None => return Err(format!("{what}\nmessages of one connection arrived out of order")),
                }
            }
        }
        // A holder at the cap kept its place, and one idle past the timeout lost it.
        for (n, (h, closes)) in report["holders"]
            .as_array()
            .into_iter()
            .flatten()
            .zip(&closes)
            .enumerate()
        {
            if let Some(closes) = closes
                && h["closed"].as_bool() != Some(*closes)
            {
                return Err(format!(
                    "{what}\nholder {n} must be closed: {closes}, is: {}\nreport {report}",
                    h["closed"]
                ));
            }
        }
        // A connection the receiver had to read whole was not refused partway.
        for c in report["connections"].as_array().into_iter().flatten() {
            if let Some(open) = c["open"].as_str() {
                return Err(format!("{what}\nconnection {}: {open}", c["id"]));
            }
        }
        if let Some(logs) = Some(container_logs(&container.0)).filter(|l| l.contains("Traceback")) {
            return Err(format!("{what}\nthe sender failed\n{logs}"));
        }
        for label in labels {
            cov.reach(label);
        }
        Ok(())
    });
    cov.assert_complete();
}
