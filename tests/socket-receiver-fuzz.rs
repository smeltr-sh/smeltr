//! Random senders against socket receivers of random settings and framings. Nothing is acknowledged: a sender forgets
//! a frame once written. So every frame a receiver admits must arrive once, as sent, and none it refuses: a misframed
//! stream closes the connection, keeping what came before it, a frame over the limit arrives truncated to it, the
//! connection reading on, and a frame the connection ended partway through is dropped.
//!
//! Each seed opens a receiver under settings an operator could save, over TCP or UDP, with a framing drawn from all
//! there are, and sends it connections or datagrams from senders it admits and senders it does not, written in random
//! pieces, ending cleanly or cut short. A model built beside each sender says which records must arrive. A
//! deterministic PRNG over a seed range, from `harness::fuzz`: a failure names its seed, and the fuzzer checks that
//! its seeds reached every option it knows.

use std::io::{self, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpStream, UdpSocket};
use std::time::{Duration, Instant};

use serde_json::Value;
use smeltr::io::framing::{ByteOrder, Counts, Framing, LengthPrefix};
use smeltr::io::line_breaker::{BREAK_SLACK, LineBreaker};
use smeltr::io::{Source, SourceRecord};
use smeltr::plan::topology::{Receiver, SocketReceiverConfig, SourceMeta, Transport};

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
const FRAMINGS: &[&str] = &["lines", "escapedLines", "lineBreaker", "lengthPrefix", "fixed", "object"];

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
            let counts = *rng.pick(&[Counts::Rest, Counts::Frame]);
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
        ..Default::default()
    });
    receiver::build(
        socket,
        settings.listen,
        settings.tls,
        &[],
        "sender",
        settings.max_request_bytes,
        pki,
    )
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

/// A record over `max`, of text that is not whitespace, and what the receiver keeps of it. Short of what a line breaker
/// holds past the limit for its break, so the receiver waits for the break as for any event.
fn oversized(rng: &mut Rng, max: usize, labels: &mut Vec<String>) -> (Vec<u8>, Vec<u8>) {
    labels.push("a frame over the limit".into());
    let record = vec![b'o'; max + 1 + rng.below((3 * max).min(BREAK_SLACK / 2))];
    let kept = record[..max].to_vec();
    (record, kept)
}

/// A random connection, the `n`th, to a receiver of `framing` reading frames of at most `max` bytes, and the labels it
/// reaches where the receiver reads it.
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
                    // A break that reaches the end of what came may yet go on, so the last event waits on the hang-up.
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
                    Counts::Frame => frame_len as u64,
                };
                if length > most {
                    continue;
                }
                let misframed = prefix.counts == Counts::Frame && last && rng.chance(4);
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
const DATAGRAMS: &[&str] = &["a datagram", "a datagram with a line end", "an empty datagram", "a datagram over the limit"];

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

/// The records' bytes, or why one is not filed as from `remote`, by a socket receiver.
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
    // Each piece its own segment, so the receiver more often reads a frame in parts.
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
    (["0.0.0.0", "127.0.0.1"].iter().chain(ipv6.then_some(&"::")).chain(ipv6.then_some(&"::1")))
        .map(|ip| format!("listen {ip}"))
        .chain(["Tcp", "Udp"].map(|t| format!("transport {t}")))
        .chain(["None", "Served", "Vouched"].map(|tls| format!("tls {tls}")))
        .chain(["Nothing", "Trusted", "Stranger"].map(|p| format!("presents {p} under Vouched")))
        .chain(FRAMINGS.iter().map(|f| format!("framing {f}")))
        .chain([1, 2, 3, 4, 8].map(|w| format!("length of {w} bytes")))
        .chain(["Big", "Little"].map(|o| format!("order {o}")))
        .chain(["Rest", "Frame"].map(|c| format!("counts {c}")))
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

/// A receiver under any settings an operator can save takes every frame an admitted sender writes whole, once and as
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
                        connection.presents =
                            *rng.pick(&[Presents::Trusted, Presents::Trusted, Presents::Nothing, Presents::Stranger]);
                    }
                    if settings.tls != Tls::None && rng.chance(4) {
                        connection.abrupt = true;
                        // Hanging up unsaid reads as cut short, so what waited on the hang-up is not taken.
                        if connection.ended_by_hanging_up {
                            connection.taken.pop();
                            reached.retain(|label| label != "an unterminated last frame");
                            reached.push("a TLS sender hanging up without saying so".into());
                        }
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
