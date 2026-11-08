//! The syslog receiver over a socket: messages framed by octet count or ended by a newline become one record each,
//! from senders on the networks it admits, plain or under TLS; or a datagram per record over UDP. Against a real
//! sender, `logger` from util-linux, under `just check-live`.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream, UdpSocket};
use std::sync::Arc;
use std::time::{Duration, Instant};

use smeltr::io::framing::Framing;
use smeltr::io::senders::{Fingerprint, Hash};
use smeltr::io::{CommitMap, Source, SourceRecord, TopicPartition, build_source};
use smeltr::plan::topology::{
    HeldSecrets, Receiver, ReceiverConfig, Senders, SocketReceiverConfig, SourceKind, Tls, Transport,
};

use crate::certs::{Authority, Issued};
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
    tls: Option<Tls>,
    max: usize,
    secrets: &HeldSecrets,
) -> Box<dyn Source> {
    open(Transport::Tcp, listen, allow_from, tls, max, secrets)
}

/// A syslog receiver taking datagrams on `listen` from senders in `allow_from`, of up to `max` bytes.
fn udp_receiver(listen: SocketAddr, allow_from: &[&str], max: usize) -> Box<dyn Source> {
    open(Transport::Udp, listen, allow_from, None, max, &HeldSecrets::default())
}

fn open(
    transport: Transport,
    listen: SocketAddr,
    allow_from: &[&str],
    tls: Option<Tls>,
    max: usize,
    secrets: &HeldSecrets,
) -> Box<dyn Source> {
    let config = ReceiverConfig {
        receiver: Receiver::Syslog(SocketReceiverConfig {
            allow_from: allow_from.iter().map(|n| n.parse().unwrap()).collect(),
            transport,
            framing: Framing::Syslog(None),
            ..Default::default()
        }),
        listen,
        tokens: Vec::new(),
        tls,
        max_request_bytes: max,
        answer_after_delivery: false,
        connections: Default::default(),
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

/// A receiver does not fail on control characters, nor on bytes that are not UTF-8 or are UTF-8 in a form longer
/// than the shortest (RFC 5424, 6.3.3 and 6.4): an octet-counted message arrives byte for byte, for decoding to judge.
#[test]
fn a_message_arrives_byte_for_byte_whatever_it_holds() {
    let at = free_port();
    let mut source = receiver(at, &["127.0.0.1"], None, 1 << 16, &HeldSecrets::default());
    let message: &[u8] =
        b"<34>1 2026-10-11T22:14:15Z web-1 su - ID47 [x@32473 v=\"a\x00\x07\x1b\"] \xEF\xBB\xBF\xC0\xAF\xFF tail\n";
    let mut frame = format!("{} ", message.len()).into_bytes();
    frame.extend_from_slice(message);
    TcpStream::connect(at).unwrap().write_all(&frame).unwrap();
    let records = poll_until(&mut source, 1);
    assert_eq!(records[0].bytes().unwrap(), message);
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

/// A message larger than the receiver reads keeps its first bytes, up to the largest, and the rest of it is
/// discarded (RFC 5424, 6.1); the connection reads on.
#[test]
fn a_message_too_large_is_truncated_and_the_connection_reads_on() {
    let at = free_port();
    let mut source = receiver(at, &["127.0.0.1"], None, 1024, &HeldSecrets::default());
    let mut sender = TcpStream::connect(at).unwrap();
    let oversized = format!("<13>{}", "x".repeat(4096));
    sender
        .write_all(
            format!(
                "<13>Oct 11 22:14:15 web-1 app: small\n{} {oversized}{oversized}\n<13>Oct 11 22:14:16 web-1 app: after\n",
                oversized.len()
            )
            .as_bytes(),
        )
        .unwrap();
    assert_eq!(
        text(&poll_until(&mut source, 4)),
        [
            "<13>Oct 11 22:14:15 web-1 app: small",
            &oversized[..1024],
            &oversized[..1024],
            "<13>Oct 11 22:14:16 web-1 app: after"
        ]
    );
}

/// A syslog receiver on `at`, admitting senders from `allow_from`, serving TLS with a certificate for `localhost` that
/// `authority` issued over `key`, and, given `trusted`, requiring each sender's certificate to chain to it.
fn tls_receiver(
    at: SocketAddr,
    allow_from: &[&str],
    authority: &Authority,
    key: rcgen::KeyPair,
    trusted: Option<&Authority>,
    max: usize,
) -> Box<dyn Source> {
    admitting(at, authority, key, trusted, Senders::default(), allow_from, max)
}

/// [`tls_receiver`], admitting `senders` by their certificates besides those `trusted` vouches for.
fn admitting(
    at: SocketAddr,
    authority: &Authority,
    key: rcgen::KeyPair,
    trusted: Option<&Authority>,
    senders: Senders,
    allow_from: &[&str],
    max: usize,
) -> Box<dyn Source> {
    crate::certs::install_provider();
    let served = authority.issue_under("receiver", vec!["localhost".to_string()], key);
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
        authorities: trusted.map(|trusted| trusted.pem.clone()),
        trust_public: false,
        side: smeltr::io::trust::Side::Server,
        senders,
    };
    receiver(at, allow_from, Some(tls), max, &secrets)
}

/// How a test's sender speaks TLS: the versions and suites it offers, and the certificate it presents.
struct Sending<'a> {
    versions: &'a [&'static rustls::SupportedProtocolVersion],
    /// The one suite it offers, or every one the process offers.
    suite: Option<rustls::CipherSuite>,
    certificate: Option<&'a Issued>,
}

impl Default for Sending<'_> {
    fn default() -> Self {
        Sending {
            versions: rustls::DEFAULT_VERSIONS,
            suite: None,
            certificate: None,
        }
    }
}

type TlsSender = rustls::StreamOwned<rustls::ClientConnection, TcpStream>;

/// How a sender speaks TLS, trusting `authority` for the receiver's certificate. It offers early data wherever it
/// resumes a session.
fn client_config(authority: &Authority, sending: Sending) -> Arc<rustls::ClientConfig> {
    use rustls::pki_types::pem::PemObject;

    let mut provider = (*smeltr::crypto::in_use()).clone();
    if let Some(suite) = sending.suite {
        provider.cipher_suites.retain(|offered| offered.suite() == suite);
    }
    let mut roots = rustls::RootCertStore::empty();
    for root in crate::certs::certificates(&authority.pem) {
        roots.add(root).unwrap();
    }
    let builder = rustls::ClientConfig::builder_with_provider(Arc::new(provider))
        .with_protocol_versions(sending.versions)
        .unwrap()
        .with_root_certificates(roots);
    let mut config = match sending.certificate {
        None => builder.with_no_client_auth(),
        Some(issued) => builder
            .with_client_auth_cert(
                crate::certs::certificates(&issued.certificate),
                rustls::pki_types::PrivateKeyDer::from_pem_slice(issued.key.as_bytes()).unwrap(),
            )
            .unwrap(),
    };
    config.enable_early_data = true;
    Arc::new(config)
}

/// A sender connected to `at` as `config` has it, its handshake done.
fn connect(at: SocketAddr, config: &Arc<rustls::ClientConfig>) -> std::io::Result<TlsSender> {
    let connection = rustls::ClientConnection::new(config.clone(), "localhost".try_into().unwrap()).unwrap();
    handshake(at, connection)
}

/// `connection` over a new TCP connection to `at`, its handshake done.
fn handshake(at: SocketAddr, mut connection: rustls::ClientConnection) -> std::io::Result<TlsSender> {
    let mut tcp = TcpStream::connect(at).unwrap();
    tcp.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    while connection.is_handshaking() {
        connection.complete_io(&mut tcp)?;
    }
    Ok(rustls::StreamOwned::new(connection, tcp))
}

/// A sender connected to `at` over TLS, trusting `authority` for the receiver's certificate, its handshake done.
fn tls_sender(at: SocketAddr, authority: &Authority, sending: Sending) -> std::io::Result<TlsSender> {
    connect(at, &client_config(authority, sending))
}

/// Whether `result` failed on a TLS alert from the receiver.
fn alerted<T>(result: &std::io::Result<T>) -> bool {
    let Err(e) = result else {
        return false;
    };
    matches!(
        e.get_ref().and_then(|inner| inner.downcast_ref::<rustls::Error>()),
        Some(rustls::Error::AlertReceived(_))
    )
}

/// `message` framed by its octet count (RFC 5425, 4.3).
fn counted(message: &str) -> String {
    format!("{} {message}", message.len())
}

/// Under a certificate the receiver serves TLS, as RFC 5425 has it: TLS 1.3 to a sender offering both, as RFC 9662
/// has it preferred, and TLS 1.2 with the suite RFC 9662 has every implementation offer,
/// `TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256`, which needs an RSA key.
#[test]
fn a_sender_is_served_tls_1_3_first_and_tls_1_2() {
    let authority = Authority::new("syslog tests");
    let rsa = rcgen::KeyPair::generate_rsa_for(&rcgen::PKCS_RSA_SHA256, rcgen::RsaKeySize::_2048).unwrap();
    let at = free_port();
    let mut source = tls_receiver(at, &["127.0.0.1"], &authority, rsa, None, 1 << 16);

    let tls13 = Sending {
        versions: &[&rustls::version::TLS13],
        ..Default::default()
    };
    let tls12 = Sending {
        versions: &[&rustls::version::TLS12],
        suite: Some(rustls::CipherSuite::TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256),
        ..Default::default()
    };
    for (sending, version) in [
        (Sending::default(), rustls::ProtocolVersion::TLSv1_3),
        (tls13, rustls::ProtocolVersion::TLSv1_3),
        (tls12, rustls::ProtocolVersion::TLSv1_2),
    ] {
        let mut sender = tls_sender(at, &authority, sending).expect("the handshake completes");
        assert_eq!(sender.conn.protocol_version(), Some(version));
        let message = format!("<34>1 2026-10-11T22:14:15Z web-1 su - ID47 - over {version:?}");
        sender.write_all(counted(&message).as_bytes()).unwrap();
        sender.flush().unwrap();
        assert_eq!(text(&poll_until(&mut source, 1)), [message]);
    }
}

/// Under TLS a receiver reads octet-counted frames whatever the TLS records cut them into: many to a record, one
/// across records, newlines within one; and messages of 2048 octets, which every receiver must take, and of 8192,
/// which it should (RFC 5425, 4.3.1).
#[test]
fn octet_counted_frames_are_read_across_tls_records() {
    let authority = Authority::new("syslog tests");
    let at = free_port();
    let mut source = tls_receiver(
        at,
        &["127.0.0.1"],
        &authority,
        rcgen::KeyPair::generate().unwrap(),
        None,
        1 << 16,
    );
    let mut sender = tls_sender(at, &authority, Sending::default()).unwrap();

    let header = "<165>1 2026-10-11T22:14:15.003Z fw01 sshd 4123 AUTH - ";
    let sized = |octets: usize| format!("{header}{}", "x".repeat(octets - header.len()));
    let messages = [
        "<34>1 2026-10-11T22:14:15Z web-1 su - ID47 - first".to_string(),
        "<34>1 2026-10-11T22:14:15Z web-1 su - ID47 - holds\na newline\r\nand more".to_string(),
        sized(2048),
        sized(8192),
        "<34>1 2026-10-11T22:14:16Z web-1 su - ID47 - last".to_string(),
    ];
    let stream: String = messages.iter().map(|m| counted(m)).collect();
    // The first two frames go in one record; the rest is split mid-length and mid-message.
    let first_two = counted(&messages[0]).len() + counted(&messages[1]).len();
    for piece in [
        &stream[..first_two],
        &stream[first_two..first_two + 2],
        &stream[first_two + 2..first_two + 1500],
        &stream[first_two + 1500..],
    ] {
        sender.write_all(piece.as_bytes()).unwrap();
        sender.flush().unwrap();
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(text(&poll_until(&mut source, messages.len())), messages);
}

/// With trusted authorities, a receiver admits only a sender whose certificate chains to one of them (RFC 5425,
/// 4.2.1), and refuses any other with a TLS alert (RFC 5425, 5): one with no certificate, or one under another
/// authority. Nothing a refused sender sent is read.
#[test]
fn a_receiver_with_trusted_authorities_admits_only_their_senders() {
    let authority = Authority::new("syslog tests");
    let senders = Authority::new("syslog senders");
    let strangers = Authority::new("syslog strangers");
    let at = free_port();
    let mut source = tls_receiver(
        at,
        &["127.0.0.1"],
        &authority,
        rcgen::KeyPair::generate().unwrap(),
        Some(&senders),
        1 << 16,
    );
    let message = counted("<34>1 2026-10-11T22:14:15Z web-1 su - ID47 - refused");

    let stranger = strangers.issue("stranger", vec!["stranger".to_string()]);
    for certificate in [None, Some(&stranger)] {
        let sending = Sending {
            certificate,
            ..Default::default()
        };
        // Under TLS 1.3 the sender's handshake ends before the receiver refuses its certificate, and the alert comes
        // after it.
        let refused = tls_sender(at, &authority, sending).and_then(|mut sender| {
            sender.write_all(message.as_bytes())?;
            sender.flush()?;
            sender.read(&mut [0; 1])
        });
        assert!(
            alerted(&refused),
            "a sender with a certificate ({}) was not refused with an alert: {refused:?}",
            certificate.is_some()
        );
    }
    nothing_arrives(&mut source);

    let vouched = senders.issue("sender", vec!["sender".to_string()]);
    let mut sender = tls_sender(
        at,
        &authority,
        Sending {
            certificate: Some(&vouched),
            ..Default::default()
        },
    )
    .unwrap();
    let message = "<34>1 2026-10-11T22:14:15Z web-1 su - ID47 - vouched for";
    sender.write_all(counted(message).as_bytes()).unwrap();
    sender.flush().unwrap();
    assert_eq!(text(&poll_until(&mut source, 1)), [message]);
}

/// A certificate signed by its own key, as a sender with no authority of its own presents.
fn self_signed(common_name: &str) -> Issued {
    let certified = rcgen::generate_simple_self_signed(vec![common_name.to_string()]).unwrap();
    Issued {
        certificate: certified.cert.pem(),
        key: certified.signing_key.serialize_pem(),
    }
}

/// `issued`'s fingerprint, taken with `hash`.
fn fingerprint(hash: Hash, issued: &Issued) -> Fingerprint {
    Fingerprint::of(hash, &crate::certs::certificates(&issued.certificate)[0])
}

/// Whether a sender presenting `certificate` is refused with an alert: under TLS 1.3 its handshake ends before the
/// receiver refuses its certificate, and the alert comes after it.
fn refused(at: SocketAddr, authority: &Authority, certificate: Option<&Issued>) -> bool {
    let sending = Sending {
        certificate,
        ..Default::default()
    };
    let message = counted("<34>1 2026-10-11T22:14:15Z web-1 su - ID47 - refused");
    alerted(&tls_sender(at, authority, sending).and_then(|mut sender| {
        sender.write_all(message.as_bytes())?;
        sender.flush()?;
        sender.read(&mut [0; 1])
    }))
}

/// Whether a sender presenting `certificate` is read.
fn read(at: SocketAddr, authority: &Authority, certificate: &Issued, source: &mut Box<dyn Source>) -> bool {
    let sending = Sending {
        certificate: Some(certificate),
        ..Default::default()
    };
    let message = "<34>1 2026-10-11T22:14:15Z web-1 su - ID47 - admitted";
    let Ok(mut sender) = tls_sender(at, authority, sending) else {
        return false;
    };
    sender.write_all(counted(message).as_bytes()).unwrap();
    sender.flush().unwrap();
    text(&poll_until(source, 1)) == [message]
}

/// A receiver admits a sender named by its certificate's fingerprint, whoever issued it, by either hash RFC 5425
/// (4.2.1, 5.1) has it take, and refuses one it does not name in the handshake.
#[test]
fn a_receiver_admits_a_sender_by_its_certificates_fingerprint() {
    let authority = Authority::new("syslog tests");
    let own = self_signed("sender");
    let issued = Authority::new("syslog senders").issue("sender", vec!["sender".to_string()]);
    let stranger = self_signed("stranger");
    let at = free_port();
    let senders = Senders {
        fingerprints: vec![fingerprint(Hash::Sha256, &own), fingerprint(Hash::Sha1, &issued)],
        names: Vec::new(),
    };
    let key = rcgen::KeyPair::generate().unwrap();
    let mut source = admitting(at, &authority, key, None, senders, &["127.0.0.1"], 1 << 16);

    for certificate in [None, Some(&stranger)] {
        assert!(refused(at, &authority, certificate), "{}", certificate.is_some());
    }
    nothing_arrives(&mut source);
    assert!(read(at, &authority, &own, &mut source));
    assert!(read(at, &authority, &issued, &mut source));
}

/// A receiver that names hosts admits, of the senders its authorities vouch for, only one issued to a host it names:
/// by the certificate's dNSName, or its common name where it has none, a `*` standing for one left-most label (RFC
/// 5425, 5.2).
#[test]
fn a_receiver_naming_hosts_admits_only_senders_issued_to_one() {
    let authority = Authority::new("syslog tests");
    let vouching = Authority::new("syslog senders");
    let at = free_port();
    let senders = Senders {
        fingerprints: Vec::new(),
        names: vec!["*.example.com".into(), "Exact.Example.org".into()],
    };
    let key = rcgen::KeyPair::generate().unwrap();
    let mut source = admitting(at, &authority, key, Some(&vouching), senders, &["127.0.0.1"], 1 << 16);

    let unnamed = [
        vouching.issue("a.example.com", vec!["example.com".into(), "a.b.example.com".into()]),
        vouching.issue("x", vec!["a.example.org".into()]),
        // Named, but not by an authority it trusts.
        Authority::new("syslog strangers").issue("x", vec!["a.example.com".into()]),
    ];
    for certificate in &unnamed {
        assert!(refused(at, &authority, Some(certificate)));
    }
    nothing_arrives(&mut source);
    for named in [
        vouching.issue("x", vec!["other.org".into(), "a.example.com".into()]),
        vouching.issue("x", vec!["exact.example.org".into()]),
        vouching.issue("b.example.com", Vec::new()),
    ] {
        assert!(read(at, &authority, &named, &mut source));
    }
}

/// Syslog has no answer, so a receiver sends a sender nothing; and it answers a sender's `close_notify` with its own
/// (RFC 5425, 4.4), so the sender sees a clean end and not a truncation. What came before it is read.
#[test]
fn a_receiver_answers_close_notify_with_its_own() {
    let authority = Authority::new("syslog tests");
    let at = free_port();
    let mut source = tls_receiver(
        at,
        &["127.0.0.1"],
        &authority,
        rcgen::KeyPair::generate().unwrap(),
        None,
        1 << 16,
    );
    let mut sender = tls_sender(at, &authority, Sending::default()).unwrap();
    let message = "<34>1 2026-10-11T22:14:15Z web-1 su - ID47 - before closing";
    sender.write_all(counted(message).as_bytes()).unwrap();
    sender.flush().unwrap();
    assert_eq!(text(&poll_until(&mut source, 1)), [message]);

    sender.sock.set_read_timeout(Some(Duration::from_millis(300))).unwrap();
    let mut byte = [0; 1];
    let quiet = sender.read(&mut byte);
    assert!(
        matches!(&quiet, Err(e) if matches!(e.kind(), std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut)),
        "the receiver sent a sender something: {quiet:?}"
    );

    sender.conn.send_close_notify();
    sender.flush().unwrap();
    sender.sock.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let ended = sender.read(&mut byte);
    assert!(
        matches!(ended, Ok(0)),
        "the receiver ended without close_notify: {ended:?}"
    );
}

/// A receiver that closes a connection itself, here on a frame it does not read, closes it with `close_notify`
/// (RFC 5425, 4.4).
#[test]
fn a_receiver_closing_a_connection_sends_close_notify() {
    let authority = Authority::new("syslog tests");
    let at = free_port();
    let mut source = tls_receiver(
        at,
        &["127.0.0.1"],
        &authority,
        rcgen::KeyPair::generate().unwrap(),
        None,
        2048,
    );
    let mut sender = tls_sender(at, &authority, Sending::default()).unwrap();
    let _ = sender.write_all(b"12a <13>abcdefgh").and_then(|()| sender.flush());
    let mut byte = [0; 1];
    let ended = sender.read(&mut byte);
    assert!(
        matches!(ended, Ok(0)),
        "the receiver ended without close_notify: {ended:?}"
    );
    nothing_arrives(&mut source);
}

/// A receiver takes no early data (RFC 9662, 6): a sender resuming a session may not send messages before its
/// handshake ends.
#[test]
fn a_receiver_takes_no_early_data() {
    let authority = Authority::new("syslog tests");
    let at = free_port();
    let mut source = tls_receiver(
        at,
        &["127.0.0.1"],
        &authority,
        rcgen::KeyPair::generate().unwrap(),
        None,
        1 << 16,
    );
    let config = client_config(&authority, Sending::default());
    let mut first = connect(at, &config).unwrap();
    let message = "<34>1 2026-10-11T22:14:15Z web-1 su - ID47 - first";
    first.write_all(counted(message).as_bytes()).unwrap();
    first.flush().unwrap();
    assert_eq!(text(&poll_until(&mut source, 1)), [message]);
    // Takes the tickets the receiver issued after the handshake.
    first.sock.set_read_timeout(Some(Duration::from_millis(300))).unwrap();
    let _ = first.read(&mut [0; 1]);

    let mut connection = rustls::ClientConnection::new(config.clone(), "localhost".try_into().unwrap()).unwrap();
    let early = counted("<34>1 2026-10-11T22:14:16Z web-1 su - ID47 - early");
    let offered = connection
        .early_data()
        .map(|mut data| data.write(early.as_bytes()).unwrap());
    let resumed = handshake(at, connection).unwrap();
    assert_eq!(resumed.conn.handshake_kind(), Some(rustls::HandshakeKind::Resumed));
    assert!(
        !resumed.conn.is_early_data_accepted(),
        "the receiver accepted {offered:?} bytes of early data"
    );
    nothing_arrives(&mut source);
}

/// A sender from a network the receiver does not admit is closed before its TLS handshake ends, so it is shown
/// nothing, not even the receiver's certificate.
#[test]
fn a_sender_from_elsewhere_is_refused_before_tls() {
    let authority = Authority::new("syslog tests");
    let at = free_port();
    let mut source = tls_receiver(
        at,
        &["10.0.0.0/8"],
        &authority,
        rcgen::KeyPair::generate().unwrap(),
        None,
        1 << 16,
    );
    let refused = tls_sender(at, &authority, Sending::default());
    assert!(refused.is_err(), "a sender from elsewhere completed its handshake");
    nothing_arrives(&mut source);
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

/// A receiver reads a datagram of every size RFC 5426 (3.2) asks of it: 480 octets over IPv4 and 1180 over IPv6,
/// which it must, and 2048 over either, which it should; and the largest a datagram can hold over IPv4.
#[test]
fn datagrams_of_every_size_rfc_5426_asks_for_are_read() {
    let header = "<165>1 2026-10-11T22:14:15.003Z fw01 sshd 4123 AUTH - ";
    let sized = |octets: usize| format!("{header}{}", "x".repeat(octets - header.len()));
    let families = [("127.0.0.1", &[480, 2048, 65507][..]), ("::1", &[1180, 2048][..])];
    // A host without IPv6 loopback checks IPv4 alone.
    let families = &families[..if crate::harness::receiver::ipv6() { 2 } else { 1 }];
    for &(loopback, sizes) in families {
        let at = SocketAddr::new(loopback.parse().unwrap(), free_port_for_both().port());
        let mut source = udp_receiver(at, &[loopback], 1 << 16);
        let sender = UdpSocket::bind(SocketAddr::new(loopback.parse().unwrap(), 0)).unwrap();
        sender.connect(at).unwrap();
        let messages: Vec<String> = sizes.iter().map(|&octets| sized(octets)).collect();
        for message in &messages {
            sender.send(message.as_bytes()).unwrap();
        }
        assert_eq!(
            text(&poll_until(&mut source, messages.len())),
            messages,
            "over {loopback}"
        );
    }
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

/// A datagram larger than the receiver reads keeps its first bytes, up to the largest (RFC 5424, 6.1).
#[test]
fn a_datagram_too_large_is_truncated() {
    let at = free_port_for_both();
    let mut source = udp_receiver(at, &["127.0.0.1"], 1024);
    let sender = datagram_sender(at);
    let oversized = format!("<13>{}", "x".repeat(4096));
    sender.send(oversized.as_bytes()).unwrap();
    sender.send(b"<13>Oct 11 22:14:15 web-1 app: small").unwrap();
    assert_eq!(
        text(&poll_until(&mut source, 2)),
        [&oversized[..1024], "<13>Oct 11 22:14:15 web-1 app: small"]
    );
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
