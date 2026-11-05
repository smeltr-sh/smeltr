//! What a receiver is sent, as sent: each read of a sender's connection, after TLS, and each datagram, offered to the
//! receiver's tap before it frames any of it; and a listener that a capture opens on a receiver's address, which
//! answers nothing.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream, UdpSocket};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use smeltr::io::raw::{RawRead, RawTap};
use smeltr::io::{Source, build_source};
use smeltr::plan::topology::{
    ApiRecords, HeldSecrets, HttpReceiverConfig, Receiver, ReceiverConfig, SourceKind, SyslogReceiverConfig, Tls,
    Transport,
};

use crate::certs::Authority;

/// One thing a tap was offered, owned.
#[derive(Clone, Debug)]
struct Offered {
    remote: String,
    transport: Transport,
    tls: bool,
    bytes: Vec<u8>,
    closed: Option<String>,
}

/// A tap that is always watching and keeps everything it is offered.
#[derive(Default)]
struct Kept(Mutex<Vec<Offered>>);

impl RawTap for Kept {
    fn watching(&self) -> bool {
        true
    }

    fn offer(&self, read: RawRead<'_>) {
        self.0.lock().unwrap().push(Offered {
            remote: read.remote.to_string(),
            transport: read.transport,
            tls: read.tls,
            bytes: read.bytes.to_vec(),
            closed: read.closed.map(str::to_string),
        });
    }
}

impl Kept {
    /// What it was offered once `done` holds of it, waiting up to ten seconds.
    fn until(&self, done: impl Fn(&[Offered]) -> bool) -> Vec<Offered> {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let offered = self.0.lock().unwrap().clone();
            if done(&offered) {
                return offered;
            }
            assert!(Instant::now() < deadline, "never offered what was awaited: {offered:?}");
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    /// Every byte it was offered, in order.
    fn bytes(offered: &[Offered]) -> Vec<u8> {
        offered.iter().flat_map(|o| o.bytes.clone()).collect()
    }
}

fn free_port() -> SocketAddr {
    loop {
        let at = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap();
        if UdpSocket::bind(at).is_ok() {
            return at;
        }
    }
}

fn syslog(listen: SocketAddr, transport: Transport, allow_from: &[&str], tls: Option<Tls>) -> ReceiverConfig {
    ReceiverConfig {
        receiver: Receiver::Syslog(SyslogReceiverConfig {
            allow_from: allow_from.iter().map(|n| n.parse().unwrap()).collect(),
            transport,
            ..Default::default()
        }),
        listen,
        tokens: Vec::new(),
        tls,
        max_request_bytes: 1 << 16,
        answer_after_delivery: false,
    }
}

fn http(listen: SocketAddr) -> ReceiverConfig {
    ReceiverConfig {
        receiver: Receiver::Http(HttpReceiverConfig {
            path: "/ingest".into(),
            records: ApiRecords::Lines,
        }),
        listen,
        tokens: Vec::new(),
        tls: None,
        max_request_bytes: 1 << 16,
        answer_after_delivery: false,
    }
}

/// `config` opened as a deployment opens it, tapped by what is returned beside it.
fn tapped(config: ReceiverConfig, secrets: &HeldSecrets) -> (Box<dyn Source>, Arc<Kept>) {
    let registry = Arc::new(smeltr::metrics::Metrics::new());
    let subjects = Arc::new(smeltr::metrics::Subjects::default());
    let mut source = build_source(&SourceKind::Receiver(config), &registry, &subjects, secrets, None).expect("opens");
    let kept = Arc::new(Kept::default());
    source.tap_raw(kept.clone());
    (source, kept)
}

/// A TLS certificate for `localhost` under `authority`, held as the secret a [`Tls`] named "receiver" reads.
fn served_by(authority: &Authority) -> (Tls, HeldSecrets) {
    crate::certs::install_provider();
    let served = authority.issue("receiver", vec!["localhost".to_string()]);
    let mut secrets = HeldSecrets::default();
    secrets.take([smeltr::heartbeat::SecretDelivery {
        name: "receiver".into(),
        version: 1,
        value: served.key,
        chain: Some(served.certificate),
    }]);
    let tls = Tls {
        of: "source \"receiver\"".into(),
        certificate: Some("receiver".into()),
        authorities: None,
        trust_public: false,
        side: smeltr::io::trust::Side::Server,
    };
    (tls, secrets)
}

/// What a sender writes is offered as it was written, however little of it frames as syslog.
#[test]
fn a_receiver_offers_what_a_sender_writes_before_it_frames_any_of_it() {
    let at = free_port();
    let (_source, kept) = tapped(syslog(at, Transport::Tcp, &[], None), &HeldSecrets::default());
    let sent = b"no frame here\0\xff\xfe binary too";
    let mut sender = TcpStream::connect(at).unwrap();
    sender.write_all(sent).unwrap();
    let offered = kept.until(|o| Kept::bytes(o).len() >= sent.len());
    assert_eq!(Kept::bytes(&offered), sent);
    let first = &offered[0];
    assert_eq!(
        (
            first.remote.as_str(),
            first.transport,
            first.tls,
            first.closed.as_deref()
        ),
        ("127.0.0.1", Transport::Tcp, false, None)
    );
}

#[test]
fn a_datagram_is_offered_whole() {
    let at = free_port();
    let (_source, kept) = tapped(syslog(at, Transport::Udp, &[], None), &HeldSecrets::default());
    let sender = UdpSocket::bind("127.0.0.1:0").unwrap();
    sender.send_to(b"<34>one datagram", at).unwrap();
    let offered = kept.until(|o| !o.is_empty());
    assert_eq!(offered[0].bytes, b"<34>one datagram");
    assert_eq!(offered[0].transport, Transport::Udp);
}

/// A sender its allowFrom refuses is closed unread, and offered as closed with why.
#[test]
fn a_refused_sender_is_offered_as_closed() {
    let at = free_port();
    let (_source, kept) = tapped(
        syslog(at, Transport::Tcp, &["10.0.0.0/8"], None),
        &HeldSecrets::default(),
    );
    let _sender = TcpStream::connect(at).unwrap();
    let offered = kept.until(|o| !o.is_empty());
    assert!(offered[0].bytes.is_empty());
    let why = offered[0].closed.as_deref().unwrap_or_default();
    assert!(why.contains("allowFrom"), "{why}");
}

/// A sender that does not speak TLS to a receiver that does fails its handshake, and that is offered with why.
#[test]
fn a_failed_handshake_is_offered_as_closed() {
    let at = free_port();
    let (tls, secrets) = served_by(&Authority::new("raw capture tests"));
    let (_source, kept) = tapped(syslog(at, Transport::Tcp, &[], Some(tls)), &secrets);
    let mut sender = TcpStream::connect(at).unwrap();
    sender.write_all(b"<34>plain text where TLS was wanted\n").unwrap();
    let offered = kept.until(|o| !o.is_empty());
    assert!(offered[0].tls);
    let why = offered[0].closed.as_deref().unwrap_or_default();
    assert!(why.starts_with("TLS handshake failed"), "{why}");
}

/// An HTTP receiver offers the request as the sender wrote it: its line, its headers and its body.
#[test]
fn an_http_receiver_offers_the_request_as_written() {
    let at = free_port();
    let (_source, kept) = tapped(http(at), &HeldSecrets::default());
    let request = b"POST /ingest HTTP/1.1\r\nHost: receiver\r\nContent-Length: 5\r\nConnection: close\r\n\r\nhello";
    let mut sender = TcpStream::connect(at).unwrap();
    sender.write_all(request).unwrap();
    let offered = kept.until(|o| Kept::bytes(o).len() >= request.len());
    assert_eq!(Kept::bytes(&offered), request);
}

/// The listener a capture opens reads what a sender writes and answers nothing, so the sender keeps it.
#[test]
fn a_capture_listener_offers_what_arrives_and_answers_nothing() {
    let at = free_port();
    let mut source = smeltr::io::receiver::open_raw(&http(at), &HeldSecrets::default()).expect("listens");
    let kept = Arc::new(Kept::default());
    source.tap_raw(kept.clone());
    let request = b"POST /ingest HTTP/1.1\r\nHost: receiver\r\nContent-Length: 2\r\n\r\nhi";
    let mut sender = TcpStream::connect(at).unwrap();
    sender.write_all(request).unwrap();
    let offered = kept.until(|o| Kept::bytes(o).len() >= request.len());
    assert_eq!(Kept::bytes(&offered), request);
    sender.set_read_timeout(Some(Duration::from_millis(300))).unwrap();
    let answered = sender.read(&mut [0; 1]);
    assert!(
        matches!(&answered, Err(e) if e.kind() == std::io::ErrorKind::WouldBlock || e.kind() == std::io::ErrorKind::TimedOut),
        "the sender was answered: {answered:?}"
    );
    assert!(
        source
            .poll_batch(100, 1 << 20, Duration::from_millis(50))
            .unwrap()
            .is_empty(),
        "it made records"
    );
}

/// A capture's listener never shares the port a receiver serves, over UDP as over TCP.
#[test]
fn a_capture_listener_is_refused_a_port_a_receiver_holds() {
    for transport in [Transport::Tcp, Transport::Udp] {
        let at = free_port();
        let config = syslog(at, transport, &[], None);
        let (_source, _kept) = tapped(config.clone(), &HeldSecrets::default());
        let refused = smeltr::io::receiver::open_raw(&config, &HeldSecrets::default());
        assert!(refused.is_err(), "{transport:?} was shared");
    }
}
