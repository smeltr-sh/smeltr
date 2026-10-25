//! The syslog receiver over a socket: messages framed by octet count or ended by a newline become one record each,
//! from senders on the networks it admits, plain or under TLS; or a datagram per record over UDP. Against a real
//! sender, `logger` from util-linux, under `just check-live`.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream, UdpSocket};
use std::sync::Arc;
use std::time::{Duration, Instant};

use smeltr::io::{CommitMap, Source, SourceRecord, TopicPartition, build_source};
use smeltr::plan::topology::{
    HeldSecrets, Identity, Receiver, ReceiverConfig, SecretRef, SourceKind, SyslogReceiverConfig, Transport,
};

use crate::live;

fn free_port() -> SocketAddr {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}

/// A syslog receiver on `listen` admitting senders from `allow_from`, reading messages of up to `max` bytes.
fn receiver(
    listen: SocketAddr,
    allow_from: &[&str],
    identity: Option<Identity>,
    max: usize,
    secrets: &HeldSecrets,
) -> Box<dyn Source> {
    open(Transport::Tcp, listen, allow_from, identity, max, secrets)
}

/// A syslog receiver taking datagrams on `listen` from senders in `allow_from`, of up to `max` bytes.
fn udp_receiver(listen: SocketAddr, allow_from: &[&str], max: usize) -> Box<dyn Source> {
    open(Transport::Udp, listen, allow_from, None, max, &HeldSecrets::default())
}

fn open(
    transport: Transport,
    listen: SocketAddr,
    allow_from: &[&str],
    identity: Option<Identity>,
    max: usize,
    secrets: &HeldSecrets,
) -> Box<dyn Source> {
    let config = ReceiverConfig {
        receiver: Receiver::Syslog(SyslogReceiverConfig {
            allow_from: allow_from.iter().map(|n| n.parse().unwrap()).collect(),
            transport,
            ..Default::default()
        }),
        listen,
        tokens: Vec::new(),
        identity: identity.map(Arc::new),
        max_request_bytes: max,
    };
    let registry = Arc::new(smeltr::metrics::Metrics::new());
    let subjects = Arc::new(smeltr::metrics::Subjects::default());
    build_source(&SourceKind::Receiver(config), &registry, &subjects, secrets, None).expect("opens")
}

/// A port free for both TCP and UDP, as a receiver of each binds its own.
fn free_port_for_both() -> SocketAddr {
    loop {
        let at = free_port();
        if UdpSocket::bind(at).is_ok() {
            return at;
        }
    }
}

/// A socket sending datagrams to `to` from the loopback address.
fn datagram_sender(to: SocketAddr) -> UdpSocket {
    let sender = UdpSocket::bind("127.0.0.1:0").unwrap();
    sender.connect(to).unwrap();
    sender
}

fn poll_until(source: &mut Box<dyn Source>, n: usize) -> Vec<SourceRecord> {
    let mut records = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    while records.len() < n {
        assert!(Instant::now() < deadline, "only {} records arrived", records.len());
        records.extend(source.poll_batch(100, 1 << 20, Duration::from_millis(100)).unwrap());
    }
    commit(source, &records);
    records
}

fn nothing_arrives(source: &mut Box<dyn Source>) {
    let polled = source.poll_batch(100, 1 << 20, Duration::from_millis(300)).unwrap();
    assert!(polled.is_empty(), "{} records arrived", polled.len());
}

fn commit(source: &mut Box<dyn Source>, records: &[SourceRecord]) {
    let mut done = CommitMap::new();
    for r in records {
        let at = TopicPartition::new(r.topic.clone(), r.partition);
        let o = done.entry(at).or_insert(r.offset);
        *o = (*o).max(r.offset);
    }
    source.commit(&done).unwrap();
}

fn text(records: &[SourceRecord]) -> Vec<String> {
    records
        .iter()
        .map(|r| String::from_utf8(r.bytes().unwrap().to_vec()).unwrap())
        .collect()
}

/// Whether the receiver closed `stream`: a read sees its end, or the connection reset.
fn closed(stream: &mut impl Read) -> bool {
    let mut byte = [0; 1];
    matches!(stream.read(&mut byte), Ok(0) | Err(_))
}

/// A sender may frame each message either way, and change between messages; each message is a record of its own,
/// named by the sender's address.
#[test]
fn each_message_is_a_record_however_it_is_framed() {
    let at = free_port();
    let mut source = receiver(at, &["127.0.0.0/8"], None, 1 << 16, &HeldSecrets::default());
    let mut sender = TcpStream::connect(at).unwrap();
    let counted = "<34>1 2026-10-11T22:14:15.003Z web-1 su - ID47 - 'su root' failed\non /dev/pts/8";
    sender
        .write_all(
            format!(
                "{} {counted}<13>Oct 11 22:14:15 web-1 app: started\r\n\n",
                counted.len()
            )
            .as_bytes(),
        )
        .unwrap();
    // A frame split across writes is read whole.
    sender.write_all(b"<13>Oct 11 22:14:16 web-1 app: ").unwrap();
    sender.flush().unwrap();
    std::thread::sleep(Duration::from_millis(100));
    sender.write_all(b"ready\n").unwrap();
    let records = poll_until(&mut source, 3);
    assert_eq!(
        text(&records),
        [
            counted,
            "<13>Oct 11 22:14:15 web-1 app: started",
            "<13>Oct 11 22:14:16 web-1 app: ready"
        ]
    );
    assert!(records.iter().all(|r| &*r.topic == "127.0.0.1"), "named by the sender");
    // What a sender wrote before it hung up mid-line is a message.
    sender.write_all(b"<13>Oct 11 22:14:17 web-1 app: bye").unwrap();
    drop(sender);
    assert_eq!(
        text(&poll_until(&mut source, 1)),
        ["<13>Oct 11 22:14:17 web-1 app: bye"]
    );
}

/// Receivers share one runtime, so stopping one ends only what is its own: its connections close and its port is
/// soon free to bind again, while another receiver keeps taking messages. The stop is asked of the runtime, which
/// closes the socket on its own thread, so a connection the receiver ended itself may close first.
#[test]
fn stopping_a_receiver_ends_its_connections_and_leaves_the_others_serving() {
    let (a, b) = (free_port(), free_port());
    let secrets = HeldSecrets::default();
    let mut stopping = receiver(a, &["127.0.0.0/8"], None, 1 << 16, &secrets);
    let mut staying = receiver(b, &["127.0.0.0/8"], None, 1 << 16, &secrets);
    let mut held = TcpStream::connect(a).unwrap();
    held.write_all(b"<13>Oct 11 22:14:15 web-1 app: started\n").unwrap();
    poll_until(&mut stopping, 1);

    drop(stopping);
    held.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    assert!(closed(&mut held), "a stopped receiver's connection is still open");
    let deadline = Instant::now() + Duration::from_secs(5);
    while std::net::TcpListener::bind(a).is_err() {
        assert!(Instant::now() < deadline, "a stopped receiver still holds its port");
        std::thread::sleep(Duration::from_millis(10));
    }
    let mut again = receiver(a, &["127.0.0.0/8"], None, 1 << 16, &secrets);
    for (at, source) in [(a, &mut again), (b, &mut staying)] {
        let mut sender = TcpStream::connect(at).unwrap();
        sender.write_all(b"<13>Oct 11 22:14:16 web-1 app: ready\n").unwrap();
        assert_eq!(text(&poll_until(source, 1)), ["<13>Oct 11 22:14:16 web-1 app: ready"]);
    }
}

/// A worker's receiver threads stay the same however many receivers it runs.
#[cfg(target_os = "linux")]
#[test]
fn receivers_share_their_threads() {
    let cores = std::thread::available_parallelism().unwrap().get();
    let _receivers: Vec<_> = (0..=cores)
        .map(|_| receiver(free_port(), &["127.0.0.0/8"], None, 1 << 16, &HeldSecrets::default()))
        .collect();
    let threads = std::fs::read_dir("/proc/self/task")
        .unwrap()
        .filter(|task| {
            let comm = task.as_ref().unwrap().path().join("comm");
            std::fs::read_to_string(comm).is_ok_and(|name| name.trim() == "smeltr-receiver")
        })
        .count();
    assert!(
        threads <= cores,
        "{} receivers run {threads} threads on {cores} cores",
        cores + 1
    );
}

/// A sender from a network the receiver does not admit is closed before anything it sends is admitted.
#[test]
fn a_sender_from_elsewhere_is_closed() {
    let at = free_port();
    let mut source = receiver(at, &["10.0.0.0/8"], None, 1 << 16, &HeldSecrets::default());
    let mut sender = TcpStream::connect(at).unwrap();
    sender.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let _ = sender.write_all(b"<13>Oct 11 22:14:15 web-1 app: started\n");
    assert!(closed(&mut sender));
    nothing_arrives(&mut source);
}

/// A message larger than the receiver reads closes the connection; what came before it is still delivered.
#[test]
fn a_message_too_large_closes_the_connection() {
    let at = free_port();
    let mut source = receiver(at, &["127.0.0.1"], None, 1024, &HeldSecrets::default());
    let mut sender = TcpStream::connect(at).unwrap();
    sender.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    sender.write_all(b"<13>Oct 11 22:14:15 web-1 app: small\n").unwrap();
    assert_eq!(
        text(&poll_until(&mut source, 1)),
        ["<13>Oct 11 22:14:15 web-1 app: small"]
    );
    let _ = sender.write_all(format!("4096 {}", "x".repeat(4096)).as_bytes());
    assert!(closed(&mut sender));
    nothing_arrives(&mut source);
}

/// Under an identity the receiver serves TLS, as RFC 5425 has it.
#[test]
fn a_sender_is_served_over_tls() {
    crate::certs::install_provider();
    let authority = crate::certs::Authority::new("syslog tests");
    let served = authority.issue("receiver", vec!["localhost".to_string()]);
    let mut secrets = HeldSecrets::default();
    secrets.take([smeltr::heartbeat::SecretDelivery {
        name: "receiver".into(),
        version: 1,
        value: served.key.clone(),
        chain: Some(served.certificate.clone()),
    }]);
    let identity = Identity {
        id: "receiver".into(),
        certificate: Some(SecretRef::LeaderStored {
            name: "receiver".into(),
            version: 1,
        }),
        authorities: None,
        trust_public: true,
        presented: smeltr::plan::topology::PresentedAs {
            client: false,
            server: true,
        },
    };
    let at = free_port();
    let mut source = receiver(at, &["127.0.0.1"], Some(identity), 1 << 16, &secrets);

    let mut roots = rustls::RootCertStore::empty();
    for root in crate::certs::certificates(&authority.pem) {
        roots.add(root).unwrap();
    }
    let config = rustls::ClientConfig::builder_with_provider(smeltr::crypto::in_use())
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connection = rustls::ClientConnection::new(Arc::new(config), "localhost".try_into().unwrap()).unwrap();
    let mut sender = rustls::StreamOwned::new(connection, TcpStream::connect(at).unwrap());
    let message = "<34>1 2026-10-11T22:14:15Z web-1 su - ID47 - over tls";
    sender
        .write_all(format!("{} {message}", message.len()).as_bytes())
        .unwrap();
    sender.flush().unwrap();
    assert_eq!(text(&poll_until(&mut source, 1)), [message]);
}

/// util-linux `logger`, as a host's own syslog client sends over TCP: RFC 5424 with octet counting, and RFC 3164
/// ended by a newline.
#[test]
fn a_real_syslog_client_sends_over_tcp() {
    let Some(image) = live::syslog_sender("a_real_syslog_client_sends_over_tcp") else {
        return;
    };
    let at: SocketAddr = format!("127.0.0.1:{}", free_port().port()).parse().unwrap();
    let mut source = receiver(at, &["127.0.0.1"], None, 1 << 16, &HeldSecrets::default());
    let send = |framing: &[&str], message: &str| {
        let status = std::process::Command::new("docker")
            .args([
                "run",
                "--rm",
                "--network",
                "host",
                &image,
                "logger",
                "--tcp",
                "--server",
                "127.0.0.1",
            ])
            .args(["--port", &at.port().to_string(), "--tag", "smeltr-live"])
            .args(framing)
            .arg(message)
            .status()
            .expect("docker runs");
        assert!(status.success(), "logger {framing:?} failed");
    };
    send(&["--rfc5424", "--octet-count"], "counted");
    send(&["--rfc3164"], "lined");
    let records = text(&poll_until(&mut source, 2));
    assert!(
        records[0].starts_with('<') && records[0].contains(" smeltr-live ") && records[0].ends_with("counted"),
        "{records:?}"
    );
    assert!(
        records[1].starts_with('<') && records[1].contains("smeltr-live") && records[1].ends_with("lined"),
        "{records:?}"
    );
}

/// Over UDP each datagram is one record, its trailing newline dropped where a sender adds one, named by the sender's
/// address.
#[test]
fn each_datagram_is_a_record() {
    let at = free_port_for_both();
    let mut source = udp_receiver(at, &["127.0.0.0/8"], 1 << 16);
    let sender = datagram_sender(at);
    for datagram in [
        "<34>1 2026-10-11T22:14:15.003Z web-1 su - ID47 - 'su root' failed\non /dev/pts/8",
        "<13>Oct 11 22:14:15 web-1 app: started\n",
        "<13>Oct 11 22:14:16 web-1 app: ready\r\n",
    ] {
        sender.send(datagram.as_bytes()).unwrap();
    }
    let records = poll_until(&mut source, 3);
    assert_eq!(
        text(&records),
        [
            "<34>1 2026-10-11T22:14:15.003Z web-1 su - ID47 - 'su root' failed\non /dev/pts/8",
            "<13>Oct 11 22:14:15 web-1 app: started",
            "<13>Oct 11 22:14:16 web-1 app: ready"
        ]
    );
    assert!(records.iter().all(|r| &*r.topic == "127.0.0.1"), "named by the sender");
}

/// A datagram from a network the receiver does not admit becomes nothing.
#[test]
fn a_datagram_from_elsewhere_is_dropped() {
    let at = free_port_for_both();
    let mut source = udp_receiver(at, &["10.0.0.0/8"], 1 << 16);
    datagram_sender(at)
        .send(b"<13>Oct 11 22:14:15 web-1 app: started")
        .unwrap();
    nothing_arrives(&mut source);
}

/// A datagram larger than the receiver reads is dropped alone; the sender's next is still read.
#[test]
fn a_datagram_too_large_is_dropped() {
    let at = free_port_for_both();
    let mut source = udp_receiver(at, &["127.0.0.1"], 1024);
    let sender = datagram_sender(at);
    sender.send("x".repeat(4096).as_bytes()).unwrap();
    sender.send(b"<13>Oct 11 22:14:15 web-1 app: small").unwrap();
    assert_eq!(
        text(&poll_until(&mut source, 1)),
        ["<13>Oct 11 22:14:15 web-1 app: small"]
    );
    nothing_arrives(&mut source);
}

/// Nothing makes a sender of datagrams wait, so one that arrives while the source is blocked is dropped and counted
/// once; once released, datagrams arrive again.
#[test]
fn a_datagram_while_blocked_is_dropped_and_counted() {
    let at = free_port_for_both();
    let mut source = udp_receiver(at, &["127.0.0.1"], 1 << 16);
    let sender = datagram_sender(at);
    source.block().unwrap();
    sender.send(b"<13>Oct 11 22:14:15 web-1 app: lost").unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut dropped = 0;
    while dropped == 0 {
        assert!(Instant::now() < deadline, "no datagram was counted dropped");
        std::thread::sleep(Duration::from_millis(20));
        dropped = source.take_dropped();
    }
    assert_eq!(dropped, 1);
    assert_eq!(source.take_dropped(), 0, "a drop is reported once");
    source.release().unwrap();
    sender.send(b"<13>Oct 11 22:14:16 web-1 app: kept").unwrap();
    assert_eq!(
        text(&poll_until(&mut source, 1)),
        ["<13>Oct 11 22:14:16 web-1 app: kept"]
    );
    assert_eq!(source.take_dropped(), 0);
}

/// A TCP port and a UDP port are two ports, whatever their number: a receiver of each serves one at once.
#[test]
fn tcp_and_udp_receivers_share_a_port_number() {
    let at = free_port_for_both();
    let mut over_tcp = receiver(at, &["127.0.0.1"], None, 1 << 16, &HeldSecrets::default());
    let mut over_udp = udp_receiver(at, &["127.0.0.1"], 1 << 16);
    TcpStream::connect(at)
        .unwrap()
        .write_all(b"<13>Oct 11 22:14:15 web-1 app: over tcp\n")
        .unwrap();
    datagram_sender(at)
        .send(b"<13>Oct 11 22:14:15 web-1 app: over udp")
        .unwrap();
    assert_eq!(
        text(&poll_until(&mut over_tcp, 1)),
        ["<13>Oct 11 22:14:15 web-1 app: over tcp"]
    );
    assert_eq!(
        text(&poll_until(&mut over_udp, 1)),
        ["<13>Oct 11 22:14:15 web-1 app: over udp"]
    );
}

/// util-linux `logger`, as a host's own syslog client sends over UDP: a datagram of RFC 5424, then one of RFC 3164.
#[test]
fn a_real_syslog_client_sends_over_udp() {
    let Some(image) = live::syslog_sender("a_real_syslog_client_sends_over_udp") else {
        return;
    };
    let at: SocketAddr = format!("127.0.0.1:{}", free_port_for_both().port()).parse().unwrap();
    let mut source = udp_receiver(at, &["127.0.0.1"], 1 << 16);
    let send = |format: &str, message: &str| {
        let status = std::process::Command::new("docker")
            .args([
                "run",
                "--rm",
                "--network",
                "host",
                &image,
                "logger",
                "--udp",
                "--server",
                "127.0.0.1",
            ])
            .args(["--port", &at.port().to_string(), "--tag", "smeltr-live", format])
            .arg(message)
            .status()
            .expect("docker runs");
        assert!(status.success(), "logger {format} failed");
    };
    send("--rfc5424", "first");
    send("--rfc3164", "second");
    let records = text(&poll_until(&mut source, 2));
    assert!(
        records[0].starts_with('<') && records[0].contains(" smeltr-live ") && records[0].ends_with("first"),
        "{records:?}"
    );
    assert!(
        records[1].starts_with('<') && records[1].contains("smeltr-live") && records[1].ends_with("second"),
        "{records:?}"
    );
}
