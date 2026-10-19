//! The HTTP receiver over a socket: a request is answered once its records are committed, and refused before any of
//! its records are taken when it cannot be admitted.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use std::time::Duration;

use crate::live;

use smeltr::io::line_breaker::LineBreaker;
use smeltr::io::{CommitMap, Source, SourceRecord, TopicPartition, build_source};
use smeltr::plan::topology::{
    ApiRecords, HecReceiverConfig, HeldSecrets, HttpReceiverConfig, Identity, Receiver, ReceiverConfig, SecretRef,
    Signal, SourceKind,
};

fn free_port() -> SocketAddr {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}

fn token() -> SecretRef {
    SecretRef::LeaderStored {
        name: "hook".into(),
        version: 1,
    }
}

fn open(config: ReceiverConfig, secrets: &HeldSecrets) -> Box<dyn Source> {
    let registry = Arc::new(smeltr::metrics::Metrics::new());
    let subjects = Arc::new(smeltr::metrics::Subjects::default());
    build_source(&SourceKind::Receiver(config), &registry, &subjects, secrets, None).expect("opens")
}

fn receiver(listen: SocketAddr, records: ApiRecords) -> Box<dyn Source> {
    let mut secrets = HeldSecrets::default();
    secrets.set_stored([("hook".to_string(), 1, "s3cret".to_string())]);
    open(
        ReceiverConfig {
            receiver: Receiver::Http(HttpReceiverConfig {
                path: "/ingest".into(),
                records,
            }),
            listen,
            tokens: vec![("ci".into(), token())],
            identity: None,
            max_request_bytes: 1024,
        },
        &secrets,
    )
}

/// One HTTP/1.1 exchange over `stream`, presenting `token` as a bearer token: its status code and body.
fn exchange(stream: impl Read + Write, method: &str, path: &str, token: Option<&str>, body: &[u8]) -> (u16, String) {
    let auth = token
        .map(|t| format!("Authorization: Bearer {t}\r\n"))
        .unwrap_or_default();
    request(stream, method, path, &auth, body)
}

/// One HTTP/1.1 exchange over `stream`, with `headers` as whole header lines, each ending in CRLF: its status code and
/// body. A receiver that refuses the sender may close before the request is written; then the status is 0 and the
/// body is the write's error.
fn request(mut stream: impl Read + Write, method: &str, path: &str, headers: &str, body: &[u8]) -> (u16, String) {
    let written = write!(
        stream,
        "{method} {path} HTTP/1.1\r\nHost: receiver\r\n{headers}Content-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )
    .and_then(|()| stream.write_all(body));
    let mut answer = Vec::new();
    let _ = stream.read_to_end(&mut answer);
    if let (Err(e), true) = (written, answer.is_empty()) {
        return (0, e.to_string());
    }
    let answer = String::from_utf8_lossy(&answer).to_string();
    let status = answer.get(9..12).and_then(|s| s.parse().ok()).unwrap_or(0);
    let body = answer
        .split_once("\r\n\r\n")
        .map(|(_, b)| b.to_string())
        .unwrap_or_default();
    (status, body)
}

fn send(at: SocketAddr, path: &str, token: Option<&str>, body: &[u8]) -> (u16, String) {
    exchange(TcpStream::connect(at).unwrap(), "POST", path, token, body)
}

fn poll_until(source: &mut Box<dyn Source>, n: usize) -> Vec<SourceRecord> {
    let mut records = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while records.len() < n {
        assert!(
            std::time::Instant::now() < deadline,
            "only {} records arrived",
            records.len()
        );
        records.extend(source.poll_batch(100, 1 << 20, Duration::from_millis(100)).unwrap());
    }
    records
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

/// The sender hears nothing until every record of its request is committed, and then 200.
#[test]
fn a_request_is_answered_once_its_records_are_committed() {
    let at = free_port();
    let mut source = receiver(at, ApiRecords::Lines);
    let sent = std::thread::spawn(move || send(at, "/ingest", Some("s3cret"), b"{\"n\":1}\n{\"n\":2}\n"));

    let records = poll_until(&mut source, 2);
    let raw: Vec<&[u8]> = records.iter().map(|r| r.bytes().unwrap()).collect();
    assert_eq!(raw, [br#"{"n":1}"#.as_slice(), br#"{"n":2}"#]);
    assert_eq!(&*records[0].topic, "127.0.0.1", "a record carries its sender's address");

    std::thread::sleep(Duration::from_millis(300));
    assert!(!sent.is_finished(), "answered before its records were delivered");
    commit(&mut source, &records);
    assert_eq!(sent.join().unwrap().0, 200);
}

/// A receiver given no address listens on `[::]` dual-stack: an IPv4 sender reaches it too, named by its IPv4 address
/// rather than as `::ffff:127.0.0.1`. On a host without IPv6 it listens on `0.0.0.0`, which IPv4 senders still reach.
#[test]
fn a_receiver_on_every_address_takes_ipv4_and_ipv6_senders() {
    let port = free_port().port();
    let mut source = receiver(
        SocketAddr::new(std::net::Ipv6Addr::UNSPECIFIED.into(), port),
        ApiRecords::Lines,
    );
    let has_ipv6 = std::net::TcpListener::bind("[::1]:0").is_ok();
    let mut senders = vec![SocketAddr::from(([127, 0, 0, 1], port))];
    if has_ipv6 {
        senders.push(SocketAddr::new(std::net::Ipv6Addr::LOCALHOST.into(), port));
    }
    for at in senders {
        let sent = std::thread::spawn(move || send(at, "/ingest", Some("s3cret"), b"one\n"));
        let records = poll_until(&mut source, 1);
        assert_eq!(
            &*records[0].topic,
            at.ip().to_string(),
            "a record carries its sender's address"
        );
        commit(&mut source, &records);
        assert_eq!(sent.join().unwrap().0, 200);
    }
}

/// Each refusal is counted, for the reader to report.
#[test]
fn a_request_without_a_listed_token_is_refused_and_nothing_is_taken() {
    let at = free_port();
    let mut source = receiver(at, ApiRecords::Body);
    assert_eq!(send(at, "/ingest", None, b"{}").0, 401);
    assert_eq!(send(at, "/ingest", Some("wrong"), b"{}").0, 401);
    assert!(
        source
            .poll_batch(100, 1 << 20, Duration::from_millis(100))
            .unwrap()
            .is_empty()
    );
    assert_eq!(source.take_refused(), vec![("token", 2)]);
    assert!(source.take_refused().is_empty(), "a count is taken once");
}

/// A sender's token is checked before its body is read, so one without a token cannot make a receiver hold anything.
#[test]
fn a_request_without_a_token_is_refused_before_its_body_is_read() {
    let at = free_port();
    let _source = receiver(at, ApiRecords::Body);
    let mut stream = TcpStream::connect(at).unwrap();
    stream.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    write!(
        stream,
        "POST /ingest HTTP/1.1\r\nHost: receiver\r\nContent-Length: 1024\r\nConnection: close\r\n\r\n"
    )
    .unwrap();
    let mut answer = [0u8; 12];
    stream.read_exact(&mut answer).expect("answered with the body unsent");
    assert_eq!(&answer[9..12], b"401");
}

#[test]
fn a_body_it_cannot_read_is_refused_whole() {
    let at = free_port();
    let mut source = receiver(at, ApiRecords::Array("/events".into()));
    let (status, why) = send(at, "/ingest", Some("s3cret"), b"{\"events\":[1,");
    assert_eq!(status, 400, "{why}");
    assert_eq!(
        send(at, "/ingest", Some("s3cret"), &[b'x'; 2000]).0,
        413,
        "past maxRequestBytes"
    );
    assert!(
        source
            .poll_batch(100, 1 << 20, Duration::from_millis(100))
            .unwrap()
            .is_empty()
    );
}

#[test]
fn health_answers_while_it_takes_requests_and_not_while_blocked() {
    let at = free_port();
    let mut source = receiver(at, ApiRecords::Body);
    let health = || exchange(TcpStream::connect(at).unwrap(), "GET", "/health", None, b"").0;
    assert_eq!(health(), 200);
    source.block().unwrap();
    assert_eq!(health(), 503);
    assert_eq!(
        send(at, "/ingest", Some("s3cret"), b"{}").0,
        503,
        "a blocked receiver takes nothing"
    );
    source.release().unwrap();
    assert_eq!(health(), 200);
}

/// A receiver that stops with a request held tells its sender to send it again: its records were never delivered.
#[test]
fn a_request_held_when_the_receiver_stops_is_answered_busy() {
    let at = free_port();
    let mut source = receiver(at, ApiRecords::Body);
    let sent = std::thread::spawn(move || send(at, "/ingest", Some("s3cret"), b"{}"));
    poll_until(&mut source, 1);
    drop(source);
    let (status, _) = sent.join().unwrap();
    assert!(status == 503 || status == 0, "a held request was answered {status}");
}

/// A CA, a certificate it signs for this host, and one it signs for a sender.
struct Pki {
    ca: String,
    server: (String, String),
    client: (
        rustls::pki_types::CertificateDer<'static>,
        rustls::pki_types::PrivateKeyDer<'static>,
    ),
    ca_der: rustls::pki_types::CertificateDer<'static>,
}

fn pki() -> Pki {
    let ca_key = rcgen::KeyPair::generate().unwrap();
    let mut ca = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
    ca.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let ca_cert = ca.clone().self_signed(&ca_key).unwrap();
    let issuer = rcgen::Issuer::new(ca, ca_key);

    let server_key = rcgen::KeyPair::generate().unwrap();
    let server = rcgen::CertificateParams::new(vec!["localhost".to_string()])
        .unwrap()
        .signed_by(&server_key, &issuer)
        .unwrap();
    let client_key = rcgen::KeyPair::generate().unwrap();
    let client = rcgen::CertificateParams::new(vec!["sender".to_string()])
        .unwrap()
        .signed_by(&client_key, &issuer)
        .unwrap();
    Pki {
        ca: ca_cert.pem(),
        ca_der: ca_cert.der().clone(),
        server: (server.pem(), server_key.serialize_pem()),
        client: (
            client.der().clone(),
            rustls::pki_types::PrivateKeyDer::try_from(client_key.serialize_der()).unwrap(),
        ),
    }
}

fn tls_send(at: SocketAddr, pki: &Pki, with_certificate: bool) -> (u16, String) {
    let mut roots = rustls::RootCertStore::empty();
    roots.add(pki.ca_der.clone()).unwrap();
    let builder = rustls::ClientConfig::builder_with_provider(smeltr::crypto::in_use())
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots);
    let config = match with_certificate {
        true => builder
            .with_client_auth_cert(vec![pki.client.0.clone()], pki.client.1.clone_key())
            .unwrap(),
        false => builder.with_no_client_auth(),
    };
    let connection = rustls::ClientConnection::new(Arc::new(config), "localhost".try_into().unwrap()).unwrap();
    let stream = rustls::StreamOwned::new(connection, TcpStream::connect(at).unwrap());
    exchange(stream, "POST", "/ingest", None, b"{}")
}

/// Served under an identity with authorities, a receiver takes only a sender whose certificate chains to them, and
/// needs no token.
#[test]
fn a_receiver_with_authorities_admits_only_a_sender_they_vouch_for() {
    let pki = pki();
    let mut secrets = HeldSecrets::default();
    secrets.set_stored([("receiver-key".to_string(), 1, pki.server.1.clone())]);
    let at = free_port();
    let mut source = open(
        ReceiverConfig {
            receiver: Receiver::Http(HttpReceiverConfig {
                path: "/ingest".into(),
                records: ApiRecords::Body,
            }),
            listen: at,
            tokens: Vec::new(),
            identity: Some(Arc::new(Identity {
                id: "receiver".into(),
                certificate: Some(pki.server.0.clone()),
                key: Some(SecretRef::LeaderStored {
                    name: "receiver-key".into(),
                    version: 1,
                }),
                authorities: Some(pki.ca.clone()),
                trust_public: true,
            })),
            max_request_bytes: 1024,
        },
        &secrets,
    );

    assert_ne!(tls_send(at, &pki, false).0, 200, "a sender with no certificate");

    let pki = Arc::new(pki);
    let sending = pki.clone();
    let sent = std::thread::spawn(move || tls_send(at, &sending, true));
    let records = poll_until(&mut source, 1);
    commit(&mut source, &records);
    assert_eq!(sent.join().unwrap().0, 200);
}

/// A HEC receiver admitting `s3cret`, whose senders are held, and `ack3d`, whose senders use indexer acknowledgement.
fn hec_receiver(listen: SocketAddr) -> Box<dyn Source> {
    let mut secrets = HeldSecrets::default();
    secrets.set_stored([
        ("hook".to_string(), 1, "s3cret".to_string()),
        ("acker".to_string(), 1, "ack3d".to_string()),
    ]);
    let acker = SecretRef::LeaderStored {
        name: "acker".into(),
        version: 1,
    };
    open(
        ReceiverConfig {
            receiver: Receiver::Hec(HecReceiverConfig {
                acknowledging: ["vector".to_string()].into(),
                sourcetypes: [(
                    "xml".to_string(),
                    LineBreaker(regex::bytes::Regex::new(r"</e>(\s*)").unwrap()),
                )]
                .into(),
            }),
            listen,
            tokens: vec![("forwarder".into(), token()), ("vector".into(), acker)],
            identity: None,
            max_request_bytes: 4096,
        },
        &secrets,
    )
}

/// One HEC exchange: its status and its JSON answer.
fn hec(at: SocketAddr, method: &str, path: &str, headers: &str, body: &[u8]) -> (u16, serde_json::Value) {
    let (status, answer) = request(TcpStream::connect(at).unwrap(), method, path, headers, body);
    (status, serde_json::from_str(&answer).unwrap_or_default())
}

const SPLUNK: &str = "Authorization: Splunk s3cret\r\n";
const ACKER: &str = "Authorization: Splunk ack3d\r\n";

/// A HEC sender hears Success once every event of its request is committed; each event is a record, filed under what
/// its envelope names.
#[test]
fn a_hec_request_is_answered_success_once_its_events_are_committed() {
    let at = free_port();
    let mut source = hec_receiver(at);
    let body = br#"{"event":"one","host":"web-1","sourcetype":"access","time":1700000000.5,"fields":{"env":"prod"}}
        {"event":{"n":2},"index":"main"}"#;
    let sent = std::thread::spawn(move || hec(at, "POST", "/services/collector/event", SPLUNK, body));

    let records = poll_until(&mut source, 2);
    let raw: Vec<&[u8]> = records.iter().map(|r| r.bytes().unwrap()).collect();
    assert_eq!(raw, [b"one".as_slice(), br#"{"n":2}"#]);
    let objects = smeltr::plan::topology::SourceMeta::HEC_RECEIVER.objects(&records, Some("hec"));
    assert_eq!(objects[0]["host"], "web-1");
    assert_eq!(objects[0]["sourcetype"], "access");
    assert_eq!(objects[0]["fields"]["env"], "prod");
    assert!(
        objects[0]["time"]
            .as_str()
            .unwrap()
            .starts_with("2023-11-14T22:13:20.5")
    );
    assert!(objects[0].get("index").is_none(), "an event names no index of its own");
    assert_eq!(objects[1]["index"], "main");
    assert_eq!(objects[1]["remote"], "127.0.0.1");
    let none = smeltr::io::RecordObject::new();
    let stored: Vec<_> = objects.iter().map(|o| (&none, o)).collect();
    let back = smeltr::plan::topology::SourceMeta::HEC_RECEIVER
        .read_stored(&stored)
        .unwrap();
    assert_eq!(
        smeltr::plan::topology::SourceMeta::HEC_RECEIVER.objects(&back, Some("hec")),
        objects,
        "a stored record reads back as it was received"
    );

    std::thread::sleep(Duration::from_millis(200));
    assert!(!sent.is_finished(), "nothing is answered before it is delivered");
    commit(&mut source, &records);
    let (status, answer) = sent.join().unwrap();
    assert_eq!(
        (status, answer),
        (200, serde_json::json!({"text": "Success", "code": 0}))
    );
}

/// The raw endpoint takes a record per line, each filed under the request's query, and a gzip body as its lines. A
/// sender whose token does not acknowledge is held though it names a channel.
#[test]
fn a_hec_raw_request_is_its_lines_filed_under_its_query() {
    let at = free_port();
    let mut source = hec_receiver(at);
    let mut gzip = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    gzip.write_all(b"first line\nsecond line\n").unwrap();
    let body = gzip.finish().unwrap();
    let headers = format!("{SPLUNK}Content-Encoding: gzip\r\n");
    let path = "/services/collector/raw?sourcetype=syslog&channel=abc";
    let sent = std::thread::spawn(move || hec(at, "POST", path, &headers, &body));

    let records = poll_until(&mut source, 2);
    let raw: Vec<&[u8]> = records.iter().map(|r| r.bytes().unwrap()).collect();
    assert_eq!(raw, [b"first line".as_slice(), b"second line"]);
    let objects = smeltr::plan::topology::SourceMeta::HEC_RECEIVER.objects(&records, Some("hec"));
    assert_eq!(objects[1]["sourcetype"], "syslog");
    std::thread::sleep(Duration::from_millis(200));
    assert!(!sent.is_finished(), "nothing is answered before it is delivered");
    commit(&mut source, &records);
    assert_eq!(
        sent.join().unwrap(),
        (200, serde_json::json!({"text": "Success", "code": 0}))
    );
}

/// A raw request of a sourcetype with a line breaker is an event where it breaks, not a record per line.
#[test]
fn a_hec_raw_request_of_a_sourcetype_with_a_line_breaker_is_its_events() {
    let at = free_port();
    let mut source = hec_receiver(at);
    let body = b"<e>1\n</e><e>2</e>\n<e>3</e>\n".to_vec();
    let path = "/services/collector/raw?sourcetype=xml";
    let sent = std::thread::spawn(move || hec(at, "POST", path, SPLUNK, &body));

    let records = poll_until(&mut source, 3);
    let raw: Vec<&[u8]> = records.iter().map(|r| r.bytes().unwrap()).collect();
    assert_eq!(raw, [b"<e>1\n</e>".as_slice(), b"<e>2</e>", b"<e>3</e>"]);
    commit(&mut source, &records);
    assert_eq!(sent.join().unwrap().0, 200);
}

/// What a HEC client is refused, it is refused in HEC's codes, and before anything reaches the inbox.
#[test]
fn a_hec_receiver_refuses_in_hecs_codes() {
    let at = free_port();
    let mut source = hec_receiver(at);
    let event = br#"{"event":"x"}"#;
    let path = "/services/collector/event";
    assert_eq!(hec(at, "POST", path, "", event).1["code"], 2);
    assert_eq!(
        hec(at, "POST", path, "Authorization: Splunk wrong\r\n", event),
        (403, serde_json::json!({"text": "Invalid token", "code": 4}))
    );
    assert_eq!(
        hec(at, "POST", path, "Authorization: Bearer s3cret\r\n", event).1["code"],
        3
    );
    let (status, answer) = hec(at, "POST", path, SPLUNK, br#"{"event":"a"}{"host":"h"}"#);
    assert_eq!(
        (status, &answer["code"], &answer["invalid-event-number"]),
        (400, &12.into(), &1.into())
    );
    assert_eq!(hec(at, "POST", path, SPLUNK, b"").1["code"], 5);
    assert_eq!(
        hec(at, "POST", "/services/collector/ack", SPLUNK, br#"{"acks":[0]}"#),
        (400, serde_json::json!({"text": "ACK is disabled", "code": 14})),
        "a held sender has no ackId to ask after"
    );
    assert_eq!(
        hec(at, "POST", "/services/collector/ack/1.0", ACKER, br#"{"acks":[0]}"#).1["code"],
        10
    );
    assert_eq!(
        hec(at, "POST", path, ACKER, event).1["code"],
        10,
        "an acknowledging sender names a channel"
    );
    assert_eq!(
        hec(
            at,
            "POST",
            path,
            "Authorization: Splunk s3cret\r\nX-Splunk-Request-Channel: not/a/channel\r\n",
            event
        )
        .1["code"],
        11
    );
    assert_eq!(
        hec(at, "GET", "/services/collector/health", "", b""),
        (200, serde_json::json!({"text": "HEC is healthy", "code": 17}))
    );
    assert!(
        source
            .poll_batch(10, 1 << 20, Duration::from_millis(100))
            .unwrap()
            .is_empty(),
        "no refused request reached the inbox"
    );
}

/// A request on a channel is given an ackId at once; asking about it answers true once its events are committed, and
/// the ID is forgotten once that has been heard.
#[test]
fn a_hec_ack_id_is_true_once_its_events_are_committed() {
    const CHANNEL: &str = "0f8a5c2e-1b7d-4c39-9e61-3a2b4c5d6e7f";
    let at = free_port();
    let mut source = hec_receiver(at);
    let on_channel = format!("{ACKER}X-Splunk-Request-Channel: {CHANNEL}\r\n");
    let ask = |path: &str, headers: &str, id: u64| {
        let (status, answer) = hec(at, "POST", path, headers, format!(r#"{{"acks":[{id}]}}"#).as_bytes());
        (status, answer["acks"][id.to_string()].clone())
    };
    let ack = "/services/collector/ack";

    let (status, given) = hec(
        at,
        "POST",
        "/services/collector/event",
        &on_channel,
        br#"{"event":"one"}"#,
    );
    assert_eq!(
        (status, &given["text"], &given["code"]),
        (200, &"Success".into(), &0.into())
    );
    let id = given["ackId"].as_u64().unwrap();
    assert_eq!(
        ask(ack, &on_channel, id),
        (200, false.into()),
        "nothing is true before it is delivered"
    );

    let records = poll_until(&mut source, 1);
    commit(&mut source, &records);
    assert_eq!(
        ask(&format!("{ack}?channel={CHANNEL}"), ACKER, id),
        (200, true.into()),
        "the channel may be named by the query"
    );
    assert_eq!(
        ask(ack, &on_channel, id),
        (200, false.into()),
        "an ID heard true is forgotten"
    );
    assert_eq!(hec(at, "POST", ack, &on_channel, b"{}").1["code"], 6);
    let large = format!(r#"{{"acks":[{}]}}"#, vec!["1"; 4096].join(","));
    assert_eq!(
        hec(at, "POST", ack, &on_channel, large.as_bytes()).0,
        413,
        "an ack query is held to maxRequestBytes as any request is"
    );
}

/// An OTLP receiver admitting `s3cret` as a bearer token.
fn otlp_receiver(listen: SocketAddr) -> Box<dyn Source> {
    otlp_receiver_reading(listen, 4096)
}

fn otlp_receiver_reading(listen: SocketAddr, max_request_bytes: usize) -> Box<dyn Source> {
    otlp_receiver_of(listen, Signal::Logs, max_request_bytes)
}

fn otlp_receiver_of(listen: SocketAddr, signal: Signal, max_request_bytes: usize) -> Box<dyn Source> {
    let mut secrets = HeldSecrets::default();
    secrets.set_stored([("hook".to_string(), 1, "s3cret".to_string())]);
    open(
        ReceiverConfig {
            receiver: Receiver::Otlp(signal),
            listen,
            tokens: vec![("collector".into(), token())],
            identity: None,
            max_request_bytes,
        },
        &secrets,
    )
}

const OTLP_JSON: &str = "Authorization: Bearer s3cret\r\nContent-Type: application/json\r\n";
const OTLP_PROTOBUF: &str = "Authorization: Bearer s3cret\r\nContent-Type: application/x-protobuf\r\n";

/// The messages of an export the tests send as protobuf, with OTLP's field numbers.
mod otlp_messages {
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct ExportLogsServiceRequest {
        #[prost(message, repeated, tag = "1")]
        pub resource_logs: Vec<ResourceLogs>,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct ResourceLogs {
        #[prost(message, optional, tag = "1")]
        pub resource: Option<Resource>,
        #[prost(message, repeated, tag = "2")]
        pub scope_logs: Vec<ScopeLogs>,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Resource {
        #[prost(message, repeated, tag = "1")]
        pub attributes: Vec<KeyValue>,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct ScopeLogs {
        #[prost(message, repeated, tag = "2")]
        pub log_records: Vec<LogRecord>,
    }

    /// An export of `records` under one resource whose one attribute is `resource` bytes of text.
    pub fn export(resource: usize, records: Vec<LogRecord>) -> ExportLogsServiceRequest {
        ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![KeyValue {
                        key: "k".into(),
                        value: Some(AnyValue {
                            string_value: "x".repeat(resource),
                        }),
                    }],
                }),
                scope_logs: vec![ScopeLogs { log_records: records }],
            }],
        }
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct LogRecord {
        #[prost(fixed64, tag = "1")]
        pub time_unix_nano: u64,
        #[prost(string, tag = "3")]
        pub severity_text: String,
        #[prost(message, optional, tag = "5")]
        pub body: Option<AnyValue>,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct KeyValue {
        #[prost(string, tag = "1")]
        pub key: String,
        #[prost(message, optional, tag = "2")]
        pub value: Option<AnyValue>,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct AnyValue {
        #[prost(string, tag = "1")]
        pub string_value: String,
    }
}

/// An exporter hears an empty partial success once every log record of its export is committed. Each log record is
/// a record, filed under its resource and scope.
#[test]
fn an_otlp_export_is_answered_once_its_log_records_are_committed() {
    let at = free_port();
    let mut source = otlp_receiver(at);
    let body = br#"{"resourceLogs":[{
        "resource":{"attributes":[{"key":"service.name","value":{"stringValue":"checkout"}}]},
        "scopeLogs":[{"scope":{"name":"app.log","version":"1.2"},"logRecords":[
            {"timeUnixNano":"1700000000500000000","severityNumber":9,"severityText":"INFO",
             "body":{"stringValue":"paid"},"attributes":[{"key":"order","value":{"intValue":"42"}}]},
            {"severityText":"ERROR","body":{"stringValue":"declined"}}]}]}]}"#;
    let sent =
        std::thread::spawn(move || request(TcpStream::connect(at).unwrap(), "POST", "/v1/logs", OTLP_JSON, body));

    let records = poll_until(&mut source, 2);
    let meta = smeltr::plan::topology::SourceMeta::OTLP_LOGS;
    let rows: Vec<_> = records.iter().map(|r| meta.record_object(r, "")).collect();
    assert_eq!(rows[0]["body"], "paid");
    assert_eq!(rows[0]["severityNumber"], 9);
    assert_eq!(rows[0]["severityText"], "INFO");
    assert_eq!(rows[0]["attributes"]["order"], 42, "an int attribute stays an int");
    assert!(
        rows[0]["time"].as_str().unwrap().starts_with("2023-11-14T22:13:20.5"),
        "{}",
        rows[0]["time"]
    );
    assert_eq!(rows[1]["severityText"], "ERROR");
    assert!(rows[1].get("time").is_none(), "a log record with no time has none");
    let objects = meta.objects(&records, Some("otlp"));
    assert_eq!(objects[1]["resource"]["service.name"], "checkout");
    assert_eq!(objects[1]["scope"]["name"], "app.log");
    assert_eq!(objects[1]["scope"]["version"], "1.2");
    assert_eq!(objects[1]["remote"], "127.0.0.1");
    let stored: Vec<_> = rows.iter().zip(&objects).collect();
    let back = meta.read_stored(&stored).unwrap();
    assert_eq!(
        (
            back.iter().map(|r| meta.record_object(r, "")).collect::<Vec<_>>(),
            meta.objects(&back, Some("otlp"))
        ),
        (rows, objects),
        "a stored record reads back as it was received"
    );

    std::thread::sleep(Duration::from_millis(200));
    assert!(!sent.is_finished(), "nothing is answered before it is delivered");
    commit(&mut source, &records);
    let (status, answer) = sent.join().unwrap();
    assert_eq!(status, 200);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&answer).unwrap(),
        serde_json::json!({})
    );
}

/// A protobuf export, gzipped as exporters send it by default, is read as its JSON twin is.
#[test]
fn an_otlp_protobuf_export_is_read_as_its_log_records() {
    use otlp_messages::*;
    use prost::Message;

    let at = free_port();
    let mut source = otlp_receiver(at);
    let text = |s: &str| Some(AnyValue { string_value: s.into() });
    let export = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".into(),
                    value: text("checkout"),
                }],
            }),
            scope_logs: vec![ScopeLogs {
                log_records: vec![LogRecord {
                    time_unix_nano: 1_700_000_000_000_000_000,
                    severity_text: "WARN".into(),
                    body: text("slow"),
                }],
            }],
        }],
    };
    let mut gzip = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    gzip.write_all(&export.encode_to_vec()).unwrap();
    let body = gzip.finish().unwrap();
    let headers = format!("{OTLP_PROTOBUF}Content-Encoding: gzip\r\n");
    let sent = std::thread::spawn(move || {
        let mut stream = TcpStream::connect(at).unwrap();
        write!(
            stream,
            "POST /v1/logs HTTP/1.1\r\nHost: receiver\r\n{headers}Content-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        )
        .unwrap();
        stream.write_all(&body).unwrap();
        let mut answer = Vec::new();
        let _ = stream.read_to_end(&mut answer);
        answer
    });

    let records = poll_until(&mut source, 1);
    let meta = smeltr::plan::topology::SourceMeta::OTLP_LOGS;
    let record = meta.record_object(&records[0], "");
    assert_eq!(record["body"], "slow");
    assert_eq!(record["severityText"], "WARN");
    assert!(
        record["time"].as_str().unwrap().starts_with("2023-11-14T22:13:20"),
        "{}",
        record["time"]
    );
    let objects = meta.objects(&records, Some("otlp"));
    assert_eq!(objects[0]["resource"]["service.name"], "checkout");
    assert!(
        objects[0].get("scope").is_none(),
        "an export that names no scope files none"
    );
    commit(&mut source, &records);
    let answer = sent.join().unwrap();
    assert!(
        answer.starts_with(b"HTTP/1.1 200"),
        "{}",
        String::from_utf8_lossy(&answer)
    );
    assert!(
        String::from_utf8_lossy(&answer)
            .to_ascii_lowercase()
            .contains("content-type: application/x-protobuf"),
        "an export is answered in the encoding it was sent in"
    );
}

/// What an exporter is refused, it is refused before anything reaches the inbox: 401 without the token, 415 in an
/// encoding OTLP does not have, 400 for a body that does not read, which an exporter drops rather than sends again.
#[test]
fn an_otlp_receiver_refuses_before_anything_is_taken() {
    let at = free_port();
    let mut source = otlp_receiver(at);
    let export = br#"{"resourceLogs":[{"scopeLogs":[{"logRecords":[{"body":{"stringValue":"x"}}]}]}]}"#;
    let send = |headers: &str, body: &[u8]| request(TcpStream::connect(at).unwrap(), "POST", "/v1/logs", headers, body);

    assert_eq!(send("Content-Type: application/json\r\n", export).0, 401);
    let (status, answer) = send("Authorization: Bearer s3cret\r\nContent-Type: text/plain\r\n", export);
    assert_eq!(status, 415, "{answer}");
    let (status, answer) = send(OTLP_JSON, b"{\"resourceLogs\":");
    assert_eq!(status, 400);
    let answer: serde_json::Value = serde_json::from_str(&answer).unwrap();
    assert_eq!(answer["code"], 3, "a JSON export is refused with a JSON Status");
    assert_eq!(send(OTLP_PROTOBUF, b"\xff\xff\xff").0, 400);
    assert!(
        source
            .poll_batch(100, 1 << 20, Duration::from_millis(200))
            .unwrap()
            .is_empty()
    );

    let (status, answer) = request(TcpStream::connect(at).unwrap(), "GET", "/health", "", b"");
    assert_eq!((status, answer.as_str()), (200, ""));
}

/// One protobuf export of `body` to an OTLP receiver at `at`: its status and its answer's bytes.
fn otlp_protobuf(at: SocketAddr, body: &[u8]) -> (u16, Vec<u8>) {
    let mut stream = TcpStream::connect(at).unwrap();
    write!(
        stream,
        "POST /v1/logs HTTP/1.1\r\nHost: receiver\r\n{OTLP_PROTOBUF}Content-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )
    .unwrap();
    stream.write_all(body).unwrap();
    let mut answer = Vec::new();
    let _ = stream.read_to_end(&mut answer);
    let status = std::str::from_utf8(&answer[9..12]).unwrap().parse().unwrap();
    (status, answer)
}

/// An export is held to what it holds once decoded, not to its body: two bytes on the wire are an empty log record of
/// a hundred and more once decoded, and a resource is copied to each record it files. Past what an inbox holds, it is
/// refused before it is decoded, and nothing reaches the inbox.
#[test]
fn an_otlp_export_is_held_to_what_it_holds_decoded() {
    use otlp_messages::*;
    use prost::Message;

    let at = free_port();
    let mut source = otlp_receiver_reading(at, 64 << 20);
    let empty = export(0, vec![LogRecord::default(); 1_000_000]).encode_to_vec();
    assert!(empty.len() < 3 << 20, "{} bytes on the wire", empty.len());
    let (status, answer) = otlp_protobuf(at, &empty);
    assert_eq!(status, 413, "{}", String::from_utf8_lossy(&answer));
    assert!(String::from_utf8_lossy(&answer).contains("once decoded"));

    assert!(
        source
            .poll_batch(100, 1 << 20, Duration::from_millis(200))
            .unwrap()
            .is_empty()
    );

    // Its records share a large resource rather than each holding a copy of it.
    let fits = export(1 << 20, vec![LogRecord::default(); 1000]).encode_to_vec();
    let sent = std::thread::spawn(move || otlp_protobuf(at, &fits).0);
    let records = poll_until(&mut source, 1000);
    commit(&mut source, &records);
    assert_eq!(sent.join().unwrap(), 200);
}

/// What a gRPC call is answered: its HTTP status and headers, its data, and its trailers.
struct GrpcAnswer {
    status: u16,
    headers: reqwest::header::HeaderMap,
    data: Vec<u8>,
    trailers: Option<reqwest::header::HeaderMap>,
}

/// One OTLP/gRPC export call of logs in `body`, framed or not as the test needs, over HTTP/2 to a receiver at `at`.
fn grpc(at: SocketAddr, headers: &[(&'static str, &str)], body: Vec<u8>) -> GrpcAnswer {
    grpc_to(at, "logs.v1.LogsService", headers, body)
}

/// One gRPC call of `service`'s `Export`, as `grpc` makes one.
fn grpc_to(at: SocketAddr, service: &str, headers: &[(&'static str, &str)], body: Vec<u8>) -> GrpcAnswer {
    use http_body::Body;

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let client = reqwest::Client::builder()
            .http2_prior_knowledge()
            .no_proxy()
            .build()
            .unwrap();
        let mut call = client
            .post(format!("http://{at}/opentelemetry.proto.collector.{service}/Export"))
            .header("te", "trailers")
            .body(body);
        for (name, value) in headers {
            call = call.header(*name, *value);
        }
        let answer = http::Response::<reqwest::Body>::from(call.send().await.unwrap());
        let (parts, mut body) = answer.into_parts();
        let (mut data, mut trailers) = (Vec::new(), None);
        while let Some(frame) = std::future::poll_fn(|cx| std::pin::Pin::new(&mut body).poll_frame(cx)).await {
            match frame.unwrap().into_data() {
                Ok(bytes) => data.extend_from_slice(&bytes),
                Err(frame) => trailers = frame.into_trailers().ok(),
            }
        }
        GrpcAnswer {
            status: parts.status.as_u16(),
            headers: parts.headers,
            data,
            trailers,
        }
    })
}

/// `message` behind gRPC's prefix: whether it is compressed, then its length.
fn grpc_frame(compressed: bool, message: &[u8]) -> Vec<u8> {
    let mut frame = vec![u8::from(compressed)];
    frame.extend_from_slice(&u32::try_from(message.len()).unwrap().to_be_bytes());
    frame.extend_from_slice(message);
    frame
}

const GRPC: [(&str, &str); 2] = [("authorization", "Bearer s3cret"), ("content-type", "application/grpc")];

/// A gRPC export, gzipped as exporters send it, is read as its HTTP twin is, and answered once its log records are
/// committed: an empty response, then trailers saying it is done.
#[test]
fn a_grpc_export_is_answered_once_its_log_records_are_committed() {
    use otlp_messages::*;
    use prost::Message;

    let at = free_port();
    let mut source = otlp_receiver(at);
    let export = export(
        0,
        vec![LogRecord {
            severity_text: "WARN".into(),
            body: Some(AnyValue {
                string_value: "slow".into(),
            }),
            ..LogRecord::default()
        }],
    );
    let mut gzip = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    gzip.write_all(&export.encode_to_vec()).unwrap();
    let body = grpc_frame(true, &gzip.finish().unwrap());
    let sent = std::thread::spawn(move || grpc(at, &[GRPC[0], GRPC[1], ("grpc-encoding", "gzip")], body));

    let records = poll_until(&mut source, 1);
    let meta = smeltr::plan::topology::SourceMeta::OTLP_LOGS;
    let record = meta.record_object(&records[0], "");
    assert_eq!(record["body"], "slow");
    assert_eq!(record["severityText"], "WARN");
    std::thread::sleep(Duration::from_millis(200));
    assert!(!sent.is_finished(), "nothing is answered before it is delivered");
    commit(&mut source, &records);
    let answer = sent.join().unwrap();
    assert_eq!(answer.status, 200);
    assert_eq!(answer.headers["content-type"], "application/grpc");
    assert_eq!(
        answer.data,
        grpc_frame(false, b""),
        "an empty ExportLogsServiceResponse"
    );
    assert_eq!(answer.trailers.expect("trailers")["grpc-status"], "0");
}

/// What an exporter is refused over gRPC, it is refused in gRPC's codes, in the answer's headers, before anything
/// reaches the inbox. A request that is not gRPC is answered in HTTP's own status, since it reads no trailers.
#[test]
fn a_grpc_receiver_refuses_in_grpcs_codes() {
    use otlp_messages::*;
    use prost::Message;

    let at = free_port();
    let mut source = otlp_receiver(at);
    let message = export(0, vec![LogRecord::default()]).encode_to_vec();
    let code = |headers: &[(&'static str, &str)], body: Vec<u8>| {
        let answer = grpc(at, headers, body);
        assert_eq!(answer.status, 200);
        assert!(answer.data.is_empty(), "a refusal carries no message");
        let message = answer
            .headers
            .get("grpc-message")
            .map(|m| m.to_str().unwrap().to_string());
        (answer.headers["grpc-status"].to_str().unwrap().to_string(), message)
    };

    let (status, _) = code(&[GRPC[1]], grpc_frame(false, &message));
    assert_eq!(status, "16", "no token is unauthenticated");
    let (status, why) = code(&GRPC, grpc_frame(true, &message));
    assert_eq!(
        status, "3",
        "a compressed message without grpc-encoding is invalid: {why:?}"
    );
    let (status, _) = code(
        &[GRPC[0], GRPC[1], ("grpc-encoding", "snappy")],
        grpc_frame(false, &message),
    );
    assert_eq!(status, "12", "an encoding it does not read is unimplemented");
    let (status, why) = code(&GRPC, grpc_frame(false, &message)[..message.len()].to_vec());
    assert_eq!(status, "3", "a message that breaks off is invalid: {why:?}");
    let two = [grpc_frame(false, &message), grpc_frame(false, &message)].concat();
    let (status, why) = code(&GRPC, two);
    assert_eq!(status, "3", "one call carries one message: {why:?}");
    let (status, why) = code(&GRPC, grpc_frame(false, &vec![0; 5000]));
    assert_eq!(
        status, "3",
        "a message past what a request may carry is invalid: {why:?}"
    );
    let (status, why) = code(&GRPC, grpc_frame(false, b"\xff\xff\xff"));
    assert_eq!(status, "3", "a message that does not decode is invalid: {why:?}");

    let answer = grpc(at, &[GRPC[0], ("content-type", "application/x-protobuf")], message);
    assert_eq!(answer.status, 415);
    assert!(
        source
            .poll_batch(100, 1 << 20, Duration::from_millis(200))
            .unwrap()
            .is_empty()
    );
}

/// An exporter sends every signal to one port. The receiver of each signal there takes its own exports, as its own
/// records; a signal no receiver takes is refused as an OpenTelemetry Collector refuses it, and a receiver that stops
/// leaves the others serving.
#[test]
fn otlp_receivers_of_each_signal_share_a_port() {
    let at = free_port();
    let mut logs = otlp_receiver(at);
    let mut traces = otlp_receiver_of(at, Signal::Traces, 4096);
    let post = move |path: &str, body: &[u8]| request(TcpStream::connect(at).unwrap(), "POST", path, OTLP_JSON, body);

    let span = br#"{"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"5b8efff798038103d269b633813fc60c",
        "spanId":"eee19b7ec3c1b174","name":"checkout","kind":2,"startTimeUnixNano":"1700000000000000000"}]}]}]}"#;
    let sent = std::thread::spawn(move || post("/v1/traces", span));
    let records = poll_until(&mut traces, 1);
    let span = smeltr::plan::topology::SourceMeta::OTLP_TRACES.record_object(&records[0], "");
    assert_eq!(
        (span["name"].as_str(), span["kind"].as_i64()),
        (Some("checkout"), Some(2))
    );
    commit(&mut traces, &records);
    assert_eq!(sent.join().unwrap().0, 200);

    let log = br#"{"resourceLogs":[{"scopeLogs":[{"logRecords":[{"body":{"stringValue":"paid"}}]}]}]}"#;
    let sent = std::thread::spawn(move || post("/v1/logs", log));
    let records = poll_until(&mut logs, 1);
    commit(&mut logs, &records);
    assert_eq!(sent.join().unwrap().0, 200);
    assert!(
        traces
            .poll_batch(100, 1 << 20, Duration::from_millis(200))
            .unwrap()
            .is_empty(),
        "a log record reaches only the receiver of logs"
    );

    let (status, answer) = post("/v1/metrics", b"{}");
    assert_eq!(status, 404, "{answer}");
    assert!(answer.contains("no receiver on this port takes metrics"), "{answer}");
    let answer = grpc_to(at, "metrics.v1.MetricsService", &GRPC, grpc_frame(false, b""));
    assert_eq!(answer.headers["grpc-status"], "12", "UNIMPLEMENTED over gRPC");
    let (status, _) = request(TcpStream::connect(at).unwrap(), "GET", "/health", "", b"");
    assert_eq!(status, 200);

    drop(traces);
    assert_eq!(
        post("/v1/traces", b"{}").0,
        404,
        "a stopped receiver's signal is no longer taken"
    );
    let sent = std::thread::spawn(move || post("/v1/logs", log));
    let records = poll_until(&mut logs, 1);
    commit(&mut logs, &records);
    assert_eq!(sent.join().unwrap().0, 200, "the others still serve");

    drop(logs);
    std::thread::sleep(Duration::from_millis(100));
    assert!(
        TcpStream::connect(at).is_err(),
        "the port closes with its last receiver"
    );
    let _again = otlp_receiver_of(at, Signal::Metrics, 4096);
    assert_eq!(post("/v1/metrics", b"{}").0, 200, "and opens again with the next");
}

/// A container this test started, removed when the test ends however it ends.
struct Container(String);

impl Drop for Container {
    fn drop(&mut self) {
        let _ = std::process::Command::new("docker")
            .args(["rm", "-f", &self.0])
            .output();
    }
}

/// The OpenTelemetry Collector, the sender most exports come from, delivers a log line it tails over gRPC and over
/// HTTP, gzipped as it sends by default, and hears each answered: neither is sent again.
#[test]
fn an_opentelemetry_collector_exports_over_grpc_and_http() {
    let Some(image) = live::otel_collector("an_opentelemetry_collector_exports_over_grpc_and_http") else {
        return;
    };
    let (grpc_at, http_at) = (free_port(), free_port());
    let mut over_grpc = otlp_receiver(grpc_at);
    let mut over_http = otlp_receiver(http_at);

    let dir = std::env::temp_dir().join(format!("smeltr-otel-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let config = format!(
        r#"receivers:
  filelog:
    include: [/smeltr/app.log]
    start_at: beginning
exporters:
  otlp:
    endpoint: {grpc_at}
    tls: {{insecure: true}}
    headers: {{authorization: Bearer s3cret}}
  otlphttp:
    endpoint: http://{http_at}
    headers: {{authorization: Bearer s3cret}}
service:
  telemetry:
    metrics: {{level: none}}
  pipelines:
    logs:
      receivers: [filelog]
      exporters: [otlp, otlphttp]
"#
    );
    std::fs::write(dir.join("config.yaml"), config).unwrap();
    std::fs::write(dir.join("app.log"), "paid over the collector\n").unwrap();
    // The collector runs as a user of its own, which reads what this test wrote.
    crate::harness::docker::share(&dir);
    let name = format!("smeltr-otel-{}", std::process::id());
    let started = std::process::Command::new("docker")
        .args(["run", "-d", "--rm", "--network", "host", "--name", &name, "-v"])
        .arg(format!("{}:/smeltr:ro", dir.display()))
        .args([image.as_str(), "--config", "/smeltr/config.yaml"])
        .output()
        .expect("docker runs");
    assert!(started.status.success(), "{}", String::from_utf8_lossy(&started.stderr));
    let collector = Container(name);

    let meta = smeltr::plan::topology::SourceMeta::OTLP_LOGS;
    for (wire, source) in [("gRPC", &mut over_grpc), ("HTTP", &mut over_http)] {
        let mut records = Vec::new();
        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        while records.is_empty() {
            assert!(std::time::Instant::now() < deadline, "nothing arrived over {wire}");
            records = source.poll_batch(100, 1 << 20, Duration::from_millis(200)).unwrap();
        }
        assert_eq!(records.len(), 1, "over {wire}");
        assert_eq!(
            meta.record_object(&records[0], "")["body"],
            "paid over the collector",
            "over {wire}"
        );
        commit(source, &records);
    }
    // An export the collector heard refused, or never heard answered, it sends again after five seconds.
    std::thread::sleep(Duration::from_secs(8));
    for (wire, source) in [("gRPC", &mut over_grpc), ("HTTP", &mut over_http)] {
        let again = source.poll_batch(100, 1 << 20, Duration::from_millis(200)).unwrap();
        assert!(again.is_empty(), "the collector sent its export over {wire} again");
    }
    let logs = std::process::Command::new("docker")
        .args(["logs", &collector.0])
        .output()
        .unwrap();
    let logs = String::from_utf8_lossy(&logs.stderr);
    assert!(
        !logs.contains("Exporting failed") && !logs.contains("\terror\t"),
        "{logs}"
    );
    drop(collector);
    let _ = std::fs::remove_dir_all(dir);
}

/// OpenTelemetry's own SDK exporters, as `telemetrygen` runs them, deliver each signal to one port, over gRPC and
/// over HTTP, and hear each export answered.
#[test]
fn an_opentelemetry_sdk_exports_every_signal_over_grpc_and_http() {
    let Some(image) = live::telemetrygen("an_opentelemetry_sdk_exports_every_signal_over_grpc_and_http") else {
        return;
    };
    for http in [false, true] {
        let wire = if http { "HTTP" } else { "gRPC" };
        let at = free_port();
        let signals = [Signal::Logs, Signal::Metrics, Signal::Traces];
        let mut receivers: Vec<_> = signals.map(|s| otlp_receiver_of(at, s, 4096)).into();
        for (signal, source) in signals.into_iter().zip(&mut receivers) {
            let what = format!("{} over {wire}", signal.name());
            let mut run = std::process::Command::new("docker");
            run.args(["run", "--rm", "--network", "host", &image, signal.name(), "--rate", "0"])
                .args([format!("--{}", signal.name()), "3".to_string()])
                .args(["--otlp-endpoint", &at.to_string(), "--otlp-insecure"])
                .args(["--otlp-header", "authorization=\"Bearer s3cret\""]);
            if http {
                run.arg("--otlp-http");
            }
            let mut sent = run
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::piped())
                .spawn()
                .expect("docker runs");
            // It sends its next export once it hears the last one answered, so each is committed as it arrives.
            let mut records = Vec::new();
            let deadline = std::time::Instant::now() + Duration::from_secs(60);
            let exited = loop {
                assert!(
                    std::time::Instant::now() < deadline,
                    "{what}: {} records, then nothing",
                    records.len()
                );
                let taken = source.poll_batch(100, 1 << 20, Duration::from_millis(200)).unwrap();
                commit(source, &taken);
                records.extend(taken);
                if let Some(exited) = sent.try_wait().unwrap() {
                    break exited;
                }
            };
            let mut logs = String::new();
            sent.stderr.take().unwrap().read_to_string(&mut logs).unwrap();
            assert!(exited.success(), "{what}: {logs}");
            assert!(
                !logs.contains("\terror\t") && !logs.contains("failed"),
                "{what}: {logs}"
            );
            assert!(records.len() >= 3, "{what}: {} records", records.len());
            let meta = smeltr::plan::topology::SourceMeta::otlp(signal);
            let first = meta.record_object(&records[0], "");
            let named = match signal {
                Signal::Logs => first["body"] == "the message",
                Signal::Metrics => first["name"] == "gen" && first["type"] == "gauge",
                Signal::Traces => first["name"].is_string() && first["traceId"].is_string(),
            };
            assert!(named, "{what}: {first:?}");
            let objects = meta.objects(&records[..1], Some("otlp"));
            assert_eq!(objects[0]["resource"]["service.name"], "telemetrygen", "{what}");
        }
    }
}

/// A Prometheus receiver admitting `s3cret` as a bearer token.
fn prometheus_receiver(listen: SocketAddr) -> Box<dyn Source> {
    prometheus_receiver_reading(listen, 4096)
}

fn prometheus_receiver_reading(listen: SocketAddr, max_request_bytes: usize) -> Box<dyn Source> {
    let mut secrets = HeldSecrets::default();
    secrets.set_stored([("hook".to_string(), 1, "s3cret".to_string())]);
    open(
        ReceiverConfig {
            receiver: Receiver::Prometheus,
            listen,
            tokens: vec![("prometheus".into(), token())],
            identity: None,
            max_request_bytes,
        },
        &secrets,
    )
}

/// The messages of a remote_write request, with prometheus/prompb's field numbers.
mod remote_write {
    use prost::Message;

    #[derive(Clone, PartialEq, prost::Message)]
    pub struct WriteRequest {
        #[prost(message, repeated, tag = "1")]
        pub timeseries: Vec<TimeSeries>,
        #[prost(message, repeated, tag = "3")]
        pub metadata: Vec<MetricMetadata>,
    }
    /// What a sender says of a metric family. Its type is 1 for a counter, 2 a gauge, 3 a histogram, 4 a gauge
    /// histogram, 5 a summary, 6 an info and 7 a stateset.
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct MetricMetadata {
        #[prost(int32, tag = "1")]
        pub r#type: i32,
        #[prost(string, tag = "2")]
        pub metric_family_name: String,
        #[prost(string, tag = "4")]
        pub help: String,
        #[prost(string, tag = "5")]
        pub unit: String,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct TimeSeries {
        #[prost(message, repeated, tag = "1")]
        pub labels: Vec<Label>,
        #[prost(message, repeated, tag = "2")]
        pub samples: Vec<Sample>,
        #[prost(message, repeated, tag = "3")]
        pub exemplars: Vec<Exemplar>,
        #[prost(message, repeated, tag = "4")]
        pub histograms: Vec<Histogram>,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Exemplar {
        #[prost(message, repeated, tag = "1")]
        pub labels: Vec<Label>,
        #[prost(double, tag = "2")]
        pub value: f64,
        #[prost(int64, tag = "3")]
        pub timestamp: i64,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Label {
        #[prost(string, tag = "1")]
        pub name: String,
        #[prost(string, tag = "2")]
        pub value: String,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Sample {
        #[prost(double, tag = "1")]
        pub value: f64,
        #[prost(int64, tag = "2")]
        pub timestamp: i64,
    }
    /// prompb's `Histogram`, its `oneof`s as the optional fields they are on the wire.
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Histogram {
        #[prost(uint64, optional, tag = "1")]
        pub count_int: Option<u64>,
        #[prost(double, optional, tag = "2")]
        pub count_float: Option<f64>,
        #[prost(double, tag = "3")]
        pub sum: f64,
        #[prost(sint32, tag = "4")]
        pub schema: i32,
        #[prost(double, tag = "5")]
        pub zero_threshold: f64,
        #[prost(uint64, optional, tag = "6")]
        pub zero_count_int: Option<u64>,
        #[prost(double, optional, tag = "7")]
        pub zero_count_float: Option<f64>,
        #[prost(message, repeated, tag = "8")]
        pub negative_spans: Vec<BucketSpan>,
        #[prost(sint64, repeated, tag = "9")]
        pub negative_deltas: Vec<i64>,
        #[prost(message, repeated, tag = "11")]
        pub positive_spans: Vec<BucketSpan>,
        #[prost(sint64, repeated, tag = "12")]
        pub positive_deltas: Vec<i64>,
        #[prost(double, repeated, tag = "13")]
        pub positive_counts: Vec<f64>,
        /// 1 says the counter reset, 2 says it did not, 3 is a gauge histogram's.
        #[prost(int32, tag = "14")]
        pub reset_hint: i32,
        #[prost(int64, tag = "15")]
        pub timestamp: i64,
        #[prost(double, repeated, tag = "16")]
        pub custom_values: Vec<f64>,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct BucketSpan {
        #[prost(sint32, tag = "1")]
        pub offset: i32,
        #[prost(uint32, tag = "2")]
        pub length: u32,
    }

    pub fn span(offset: i32, length: u32) -> BucketSpan {
        BucketSpan { offset, length }
    }

    fn labels(labels: &[(&str, &str)]) -> Vec<Label> {
        (labels.iter())
            .map(|(name, value)| Label {
                name: name.to_string(),
                value: value.to_string(),
            })
            .collect()
    }

    /// One series of `labels` holding `samples`, each a value and its time in milliseconds.
    pub fn series(labels: &[(&str, &str)], samples: &[(f64, i64)]) -> TimeSeries {
        TimeSeries {
            labels: self::labels(labels),
            samples: (samples.iter())
                .map(|&(value, timestamp)| Sample { value, timestamp })
                .collect(),
            exemplars: Vec::new(),
            histograms: Vec::new(),
        }
    }

    /// An exemplar of `labels`, `value`, and its time in milliseconds.
    pub fn exemplar(labels: &[(&str, &str)], value: f64, timestamp: i64) -> Exemplar {
        Exemplar {
            labels: self::labels(labels),
            value,
            timestamp,
        }
    }

    /// The metadata of the family `name`, of `type`, `help` and `unit`.
    pub fn metadata(r#type: i32, name: &str, help: &str, unit: &str) -> MetricMetadata {
        MetricMetadata {
            r#type,
            metric_family_name: name.to_string(),
            help: help.to_string(),
            unit: unit.to_string(),
        }
    }

    /// `series` as a request body, snappy-compressed as remote_write sends it.
    pub fn body(series: Vec<TimeSeries>) -> Vec<u8> {
        body_with(series, Vec::new())
    }

    /// `series` and `metadata` as a request body.
    pub fn body_with(series: Vec<TimeSeries>, metadata: Vec<MetricMetadata>) -> Vec<u8> {
        let request = WriteRequest {
            timeseries: series,
            metadata,
        };
        snap::raw::Encoder::new()
            .compress_vec(&request.encode_to_vec())
            .unwrap()
    }
}

/// One remote_write request of `body`, with `headers` as whole header lines: its status and body.
fn remote_write_request(at: SocketAddr, headers: &str, body: &[u8]) -> (u16, String) {
    request(TcpStream::connect(at).unwrap(), "POST", "/api/v1/write", headers, body)
}

const REMOTE_WRITE: &str =
    "Authorization: Bearer s3cret\r\nContent-Type: application/x-protobuf\r\nContent-Encoding: snappy\r\n";

/// A sender hears 204 once every sample of its request is committed, and not before. Each sample is a record: its
/// series' name, its value and time, and its other labels.
#[test]
fn a_remote_write_request_is_answered_once_its_samples_are_committed() {
    use remote_write::series;

    let at = free_port();
    let mut source = prometheus_receiver(at);
    let body = remote_write::body(vec![
        series(
            &[("__name__", "up"), ("job", "node"), ("instance", "a:9100")],
            &[(1.0, 1_700_000_000_500), (0.0, 1_700_000_015_500)],
        ),
        series(&[("__name__", "temp_celsius")], &[(21.5, 1_700_000_000_000)]),
    ]);
    let sent = std::thread::spawn(move || remote_write_request(at, REMOTE_WRITE, &body));

    let records = poll_until(&mut source, 3);
    let meta = smeltr::plan::topology::SourceMeta::prometheus_receiver();
    let rows: Vec<_> = records.iter().map(|r| meta.record_object(r, "")).collect();
    assert_eq!(rows[0]["name"], "up");
    assert_eq!(rows[0]["value"], 1.0);
    assert_eq!(
        rows[0]["attributes"],
        serde_json::json!({"job": "node", "instance": "a:9100"})
    );
    assert!(
        rows[0]["time"].as_str().unwrap().starts_with("2023-11-14T22:13:20.5"),
        "{}",
        rows[0]["time"]
    );
    assert_eq!(rows[1]["value"], 0.0);
    assert_eq!(rows[2]["name"], "temp_celsius");
    assert!(
        rows[2].get("attributes").is_none(),
        "a series with only its name has no attributes"
    );
    let objects = meta.objects(&records, Some("prom"));
    assert_eq!(objects[0]["remote"], "127.0.0.1");
    let stored: Vec<_> = rows.iter().zip(&objects).collect();
    let back = meta.read_stored(&stored).unwrap();
    assert_eq!(
        back.iter().map(|r| meta.record_object(r, "")).collect::<Vec<_>>(),
        rows,
        "a stored sample reads back as it was received"
    );

    std::thread::sleep(Duration::from_millis(200));
    assert!(!sent.is_finished(), "nothing is answered before it is delivered");
    commit(&mut source, &records);
    assert_eq!(sent.join().unwrap().0, 204);
}

/// What a sender is refused, it is refused before anything reaches the inbox: 401 without the token, 415 for a
/// request that is not remote_write 1.0 or 2.0 or not snappy, 400 for a body that does not read or a histogram that is not
/// one. A sender drops each.
#[test]
fn a_prometheus_receiver_refuses_before_anything_is_taken() {
    use remote_write::series;

    let at = free_port();
    let mut source = prometheus_receiver(at);
    let body = remote_write::body(vec![series(&[("__name__", "up")], &[(1.0, 1)])]);
    let send = |headers: &str, body: &[u8]| remote_write_request(at, headers, body);

    let unsigned = "Content-Type: application/x-protobuf\r\nContent-Encoding: snappy\r\n";
    assert_eq!(send(unsigned, &body).0, 401);
    let v3 = "Authorization: Bearer s3cret\r\nContent-Type: application/x-protobuf;proto=io.prometheus.write.v3.Request\r\n\
              Content-Encoding: snappy\r\n";
    let (status, answer) = send(v3, &body);
    assert_eq!(status, 415, "{answer}");
    let gzip = "Authorization: Bearer s3cret\r\nContent-Type: application/x-protobuf\r\nContent-Encoding: gzip\r\n";
    assert_eq!(send(gzip, &body).0, 415);
    assert_eq!(send(REMOTE_WRITE, b"\xff\xff\xff\xff").0, 400);
    let mut histogram = series(&[("__name__", "latency")], &[]);
    histogram.histograms.push(remote_write::Histogram {
        schema: 9,
        ..Default::default()
    });
    let (status, answer) = send(REMOTE_WRITE, &remote_write::body(vec![histogram]));
    assert_eq!(status, 400);
    assert!(answer.contains("of schema 9"), "{answer}");
    assert!(
        source
            .poll_batch(100, 1 << 20, Duration::from_millis(200))
            .unwrap()
            .is_empty()
    );

    let (status, answer) = request(TcpStream::connect(at).unwrap(), "GET", "/health", "", b"");
    assert_eq!((status, answer.as_str()), (200, ""));
}

/// The messages of a remote_write 2.0 request, with prometheus/prompb's `write/v2` field numbers. Each string is an
/// index into `symbols`.
mod remote_write_v2 {
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Request {
        #[prost(string, repeated, tag = "4")]
        pub symbols: Vec<String>,
        #[prost(message, repeated, tag = "5")]
        pub timeseries: Vec<TimeSeries>,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct TimeSeries {
        #[prost(uint32, repeated, tag = "1")]
        pub labels_refs: Vec<u32>,
        #[prost(message, repeated, tag = "2")]
        pub samples: Vec<Sample>,
        #[prost(message, repeated, tag = "4")]
        pub exemplars: Vec<Exemplar>,
        #[prost(message, optional, tag = "5")]
        pub metadata: Option<Metadata>,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Sample {
        #[prost(double, tag = "1")]
        pub value: f64,
        #[prost(int64, tag = "2")]
        pub timestamp: i64,
        #[prost(int64, tag = "3")]
        pub start_timestamp: i64,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Exemplar {
        #[prost(uint32, repeated, tag = "1")]
        pub labels_refs: Vec<u32>,
        #[prost(double, tag = "2")]
        pub value: f64,
        #[prost(int64, tag = "3")]
        pub timestamp: i64,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Metadata {
        #[prost(int32, tag = "1")]
        pub r#type: i32,
        #[prost(uint32, tag = "3")]
        pub help_ref: u32,
        #[prost(uint32, tag = "4")]
        pub unit_ref: u32,
    }
}

const REMOTE_WRITE_V2: &str = "Authorization: Bearer s3cret\r\n\
    Content-Type: application/x-protobuf;proto=io.prometheus.write.v2.Request\r\nContent-Encoding: snappy\r\n\
    X-Prometheus-Remote-Write-Version: 2.0.0\r\n";

/// One remote_write 2.0 request of `body`: the whole answer, its headers lowercased.
fn remote_write_v2(at: SocketAddr, body: &[u8]) -> String {
    let mut stream = TcpStream::connect(at).unwrap();
    write!(
        stream,
        "POST /api/v1/write HTTP/1.1\r\nHost: receiver\r\n{REMOTE_WRITE_V2}Content-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )
    .and_then(|()| stream.write_all(body))
    .unwrap();
    let mut answer = String::new();
    let _ = stream.read_to_string(&mut answer);
    answer.to_ascii_lowercase()
}

/// A remote_write 2.0 request reads to the records 1.0's does, its labels from its table of strings, a sample's start
/// time its `startTime`, and its family's metadata one record ahead of its first series. Its sender hears 204 once
/// they are committed, with what it wrote.
#[test]
fn a_remote_write_2_request_is_read_as_1_0_is_and_answered_with_what_it_wrote() {
    use prost::Message;
    use remote_write_v2::*;

    let at = free_port();
    let mut source = prometheus_receiver(at);
    let symbols = [
        "",
        "__name__",
        "http_requests_total",
        "code",
        "200",
        "500",
        "Requests served.",
        "trace_id",
        "abc",
    ];
    let metadata = Some(Metadata {
        r#type: 1,
        help_ref: 6,
        unit_ref: 0,
    });
    let sample = |value, start_timestamp| Sample {
        value,
        timestamp: 2_000,
        start_timestamp,
    };
    let request = Request {
        symbols: symbols.iter().map(|s| s.to_string()).collect(),
        timeseries: vec![
            TimeSeries {
                labels_refs: vec![1, 2, 3, 4],
                samples: vec![sample(3.0, 1_000)],
                exemplars: vec![Exemplar {
                    labels_refs: vec![7, 8],
                    value: 1.0,
                    timestamp: 1_500,
                }],
                metadata: metadata.clone(),
            },
            TimeSeries {
                labels_refs: vec![1, 2, 3, 5],
                samples: vec![sample(1.0, 0)],
                exemplars: Vec::new(),
                metadata,
            },
        ],
    };
    let body = snap::raw::Encoder::new()
        .compress_vec(&request.encode_to_vec())
        .unwrap();
    let sent = std::thread::spawn(move || remote_write_v2(at, &body));

    let records = poll_until(&mut source, 3);
    let meta = smeltr::plan::topology::SourceMeta::prometheus_receiver();
    let rows: Vec<_> = records.iter().map(|r| meta.record_object(r, "")).collect();
    assert_eq!(
        serde_json::Value::Object(rows[0].clone()),
        serde_json::json!({
            "name": "http_requests_total",
            "type": "sum",
            "aggregationTemporality": 2,
            "isMonotonic": true,
            "description": "Requests served.",
        }),
        "a family's metadata is one record, ahead of its first series"
    );
    assert_eq!(rows[1]["value"], 3.0);
    assert_eq!(rows[1]["attributes"], serde_json::json!({"code": "200"}));
    assert!(
        rows[1]["startTime"]
            .as_str()
            .unwrap()
            .starts_with("1970-01-01T00:00:01"),
        "{}",
        rows[1]["startTime"]
    );
    assert_eq!(rows[1]["exemplars"][0]["traceId"], "abc");
    assert_eq!(rows[2]["attributes"], serde_json::json!({"code": "500"}));
    assert!(rows[2].get("startTime").is_none(), "a start time of 0 is unknown");
    let objects = meta.objects(&records, Some("prom"));
    let stored: Vec<_> = rows.iter().zip(&objects).collect();
    let back = meta.read_stored(&stored).unwrap();
    assert_eq!(
        back.iter().map(|r| meta.record_object(r, "")).collect::<Vec<_>>(),
        rows,
        "a stored start time reads back as it was received"
    );

    commit(&mut source, &records);
    let answer = sent.join().unwrap();
    assert!(answer.starts_with("http/1.1 204"), "{answer}");
    for written in [
        "x-prometheus-remote-write-samples-written: 2",
        "x-prometheus-remote-write-histograms-written: 0",
        "x-prometheus-remote-write-exemplars-written: 1",
    ] {
        assert!(answer.contains(written), "{answer}");
    }
}

/// A remote_write 2.0 request that breaks the protocol is refused whole, with 400 and nothing written: a label naming
/// a string past its table, here.
#[test]
fn a_remote_write_2_request_naming_a_string_past_its_table_is_refused() {
    use prost::Message;
    use remote_write_v2::*;

    let at = free_port();
    let mut source = prometheus_receiver(at);
    let request = Request {
        symbols: vec![String::new(), "__name__".into()],
        timeseries: vec![TimeSeries {
            labels_refs: vec![1, 9],
            samples: vec![Sample {
                value: 1.0,
                timestamp: 1,
                start_timestamp: 0,
            }],
            exemplars: Vec::new(),
            metadata: None,
        }],
    };
    let body = snap::raw::Encoder::new()
        .compress_vec(&request.encode_to_vec())
        .unwrap();
    let answer = remote_write_v2(at, &body);
    assert!(answer.starts_with("http/1.1 400"), "{answer}");
    assert!(answer.contains("past the request's 2 strings"), "{answer}");
    for written in ["samples", "histograms", "exemplars"] {
        assert!(
            answer.contains(&format!("x-prometheus-remote-write-{written}-written: 0")),
            "{answer}"
        );
    }
    assert!(
        source
            .poll_batch(100, 1 << 20, Duration::from_millis(200))
            .unwrap()
            .is_empty()
    );
}

/// A native histogram is a record typed as an OTLP histogram point: an exponential one's buckets as `positive` and
/// `negative`, each its OTLP offset and its absolute counts with a gap between spans as zeros; a custom one's as
/// `bucketCounts` ending at `explicitBounds`. A series gives its float samples, then its histograms.
#[test]
fn a_remote_write_histogram_is_a_record_typed_as_an_otlp_histogram_point() {
    use remote_write::{Histogram, series, span};

    let at = free_port();
    let mut source = prometheus_receiver(at);
    let mut latency = series(&[("__name__", "latency"), ("job", "api")], &[(0.5, 1_700_000_000_000)]);
    latency.histograms = vec![
        Histogram {
            count_int: Some(20),
            sum: 12.5,
            schema: 3,
            zero_threshold: 0.001,
            zero_count_int: Some(6),
            // Buckets 2, 3 and 5.
            positive_spans: vec![span(2, 2), span(1, 1)],
            positive_deltas: vec![3, -1, 2],
            negative_spans: vec![span(-1, 1)],
            negative_deltas: vec![5],
            reset_hint: 2,
            timestamp: 1_700_000_000_000,
            ..Default::default()
        },
        Histogram {
            count_float: Some(7.5),
            sum: 3.25,
            schema: 0,
            positive_spans: vec![span(0, 2)],
            positive_counts: vec![1.5, 6.0],
            reset_hint: 3,
            timestamp: 1_700_000_000_000,
            ..Default::default()
        },
        Histogram {
            count_int: Some(5),
            sum: 1.75,
            schema: -53,
            custom_values: vec![0.1, 0.5, 1.0],
            positive_spans: vec![span(0, 1), span(1, 2)],
            positive_deltas: vec![2, 1, -3],
            reset_hint: 1,
            timestamp: 1_700_000_000_000,
            ..Default::default()
        },
        Histogram {
            schema: 0,
            timestamp: 1_700_000_000_000,
            ..Default::default()
        },
    ];
    let body = remote_write::body(vec![latency]);
    let sent = std::thread::spawn(move || remote_write_request(at, REMOTE_WRITE, &body));

    let records = poll_until(&mut source, 5);
    let meta = smeltr::plan::topology::SourceMeta::prometheus_receiver();
    let rows: Vec<_> = records.iter().map(|r| meta.record_object(r, "")).collect();
    assert_eq!(rows[0]["value"], 0.5);
    assert!(rows[0].get("type").is_none(), "a float sample has no type");
    let (exponential, float, custom, unhinted) = (&rows[1], &rows[2], &rows[3], &rows[4]);
    assert_eq!(exponential["resetHint"], "no");
    assert_eq!(custom["resetHint"], "yes");
    for (row, what) in [
        (&rows[0], "a float sample"),
        (float, "a gauge histogram"),
        (unhinted, "a histogram whose sender does not say"),
    ] {
        assert!(row.get("resetHint").is_none(), "{what} has no reset hint");
    }
    assert_eq!(exponential["name"], "latency");
    assert_eq!(exponential["attributes"], serde_json::json!({"job": "api"}));
    assert!(exponential.get("value").is_none(), "a histogram has no value");
    assert_eq!(exponential["type"], "exponentialHistogram");
    assert_eq!(exponential["aggregationTemporality"], 2);
    assert_eq!(exponential["count"], 20.0);
    assert_eq!(exponential["sum"], 12.5);
    assert_eq!(exponential["scale"], 3);
    assert_eq!(exponential["zeroCount"], 6.0);
    assert_eq!(exponential["zeroThreshold"], 0.001);
    assert_eq!(
        exponential["positive"],
        serde_json::json!({"offset": 1, "bucketCounts": [3.0, 2.0, 0.0, 4.0]})
    );
    assert_eq!(
        exponential["negative"],
        serde_json::json!({"offset": -2, "bucketCounts": [5.0]})
    );
    assert!(
        float.get("aggregationTemporality").is_none(),
        "a gauge histogram has no temporality"
    );
    assert_eq!(float["count"], 7.5);
    assert_eq!(
        float["positive"],
        serde_json::json!({"offset": -1, "bucketCounts": [1.5, 6.0]})
    );
    assert!(float.get("negative").is_none() && float.get("zeroThreshold").is_none());
    assert_eq!(custom["type"], "histogram");
    assert_eq!(custom["bucketCounts"], serde_json::json!([2.0, 0.0, 3.0, 0.0]));
    assert_eq!(custom["explicitBounds"], serde_json::json!([0.1, 0.5, 1.0]));
    assert!(custom.get("scale").is_none() && custom.get("positive").is_none());

    let objects = meta.objects(&records, Some("prom"));
    let stored: Vec<_> = rows.iter().zip(&objects).collect();
    let back = meta.read_stored(&stored).unwrap();
    assert_eq!(
        back.iter().map(|r| meta.record_object(r, "")).collect::<Vec<_>>(),
        rows,
        "a stored histogram reads back as it was received"
    );
    commit(&mut source, &records);
    assert_eq!(sent.join().unwrap().0, 204);
}

/// A series' exemplars ride on its last record of the request, its last histogram where it has any, each shaped as an
/// OTLP point's exemplar is; a series of exemplars alone is one record of them at its latest exemplar's time. An
/// exemplar with a label of no name, or a name twice, is left out and the request taken.
#[test]
fn remote_write_exemplars_are_shaped_as_otlp_exemplars_on_a_series_last_record() {
    use remote_write::{Histogram, exemplar, series};

    let at = free_port();
    let mut source = prometheus_receiver(at);
    let mut requests = series(
        &[("__name__", "requests"), ("job", "api")],
        &[(1.0, 1_000), (2.0, 2_000)],
    );
    requests.exemplars = vec![exemplar(
        &[
            ("trace_id", "5b8efff798038103d269b633813fc60c"),
            ("span_id", "eee19b7ec3c1b174"),
            ("user", "u1"),
            ("none", ""),
        ],
        0.25,
        1_700_000_000_000,
    )];
    let mut latency = series(&[("__name__", "latency")], &[(0.5, 1_000)]);
    latency.histograms = vec![Histogram::default(), Histogram::default()];
    latency.exemplars = vec![exemplar(&[], 0.75, 0)];
    let mut alone = series(&[("__name__", "requests"), ("job", "api")], &[]);
    alone.exemplars = vec![
        exemplar(&[("trace_id", "a")], 1.0, 3_000),
        exemplar(&[("trace_id", "b")], 2.0, 5_000),
    ];
    let body = remote_write::body(vec![requests, latency, alone]);
    let sent = std::thread::spawn(move || remote_write_request(at, REMOTE_WRITE, &body));

    let records = poll_until(&mut source, 6);
    let meta = smeltr::plan::topology::SourceMeta::prometheus_receiver();
    let rows: Vec<_> = records.iter().map(|r| meta.record_object(r, "")).collect();
    let with: Vec<_> = rows.iter().map(|r| r.get("exemplars").is_some()).collect();
    assert_eq!(with, [false, true, false, false, true, true]);
    assert_eq!(
        rows[1]["exemplars"],
        serde_json::json!([{
            "time": "2023-11-14T22:13:20.000000Z",
            "value": 0.25,
            "traceId": "5b8efff798038103d269b633813fc60c",
            "spanId": "eee19b7ec3c1b174",
            "filteredAttributes": {"user": "u1"},
        }])
    );
    assert_eq!(rows[4]["type"], "exponentialHistogram");
    assert_eq!(rows[4]["exemplars"], serde_json::json!([{"value": 0.75}]));
    let only = &rows[5];
    assert_eq!(only["name"], "requests");
    assert_eq!(only["attributes"], serde_json::json!({"job": "api"}));
    assert!(only.get("value").is_none(), "a record of exemplars alone has no value");
    assert!(only.get("type").is_none());
    assert!(
        only["time"].as_str().unwrap().starts_with("1970-01-01T00:00:05"),
        "{}",
        only["time"]
    );
    assert_eq!(only["exemplars"].as_array().unwrap().len(), 2);

    let objects = meta.objects(&records, Some("prom"));
    let stored: Vec<_> = rows.iter().zip(&objects).collect();
    let back = meta.read_stored(&stored).unwrap();
    assert_eq!(
        back.iter().map(|r| meta.record_object(r, "")).collect::<Vec<_>>(),
        rows,
        "a stored exemplar reads back as it was received"
    );
    commit(&mut source, &records);
    assert_eq!(sent.join().unwrap().0, 204);

    // Prometheus drops an exemplar it cannot store and keeps the request; so does the receiver.
    let mut s = series(&[("__name__", "x")], &[(1.0, 1)]);
    s.exemplars = vec![
        exemplar(&[("", "x")], 1.0, 1),
        exemplar(&[("trace_id", "a"), ("trace_id", "b")], 2.0, 1),
    ];
    let body = remote_write::body(vec![s]);
    let sent = std::thread::spawn(move || remote_write_request(at, REMOTE_WRITE, &body));
    let records = poll_until(&mut source, 1);
    assert!(meta.record_object(&records[0], "").get("exemplars").is_none());
    commit(&mut source, &records);
    assert_eq!(sent.join().unwrap().0, 204);
    assert!(
        source
            .poll_batch(100, 1 << 20, Duration::from_millis(200))
            .unwrap()
            .is_empty()
    );
}

/// Each metric family's metadata is a record of its own, after the series before it in the request: the family its
/// `name`, its help and unit its `description` and `unit`, and its type typed as an OTLP metric is. It has no time,
/// value or attributes. Metadata of no family name is left out and the request taken.
#[test]
fn remote_write_metadata_is_a_record_of_each_metric_family() {
    use remote_write::{metadata, series};

    let at = free_port();
    let mut source = prometheus_receiver(at);
    let body = remote_write::body_with(
        vec![series(&[("__name__", "http_requests_total")], &[(3.0, 1_000)])],
        vec![
            metadata(1, "http_requests_total", "Requests served.", ""),
            metadata(4, "queue_depth", "", "items"),
            metadata(2, "", "Nameless.", ""),
        ],
    );
    let sent = std::thread::spawn(move || remote_write_request(at, REMOTE_WRITE, &body));

    let records = poll_until(&mut source, 3);
    let meta = smeltr::plan::topology::SourceMeta::prometheus_receiver();
    let rows: Vec<_> = records.iter().map(|r| meta.record_object(r, "")).collect();
    assert_eq!(rows[0]["value"], 3.0);
    assert!(rows[0].get("isMonotonic").is_none(), "a sample has no metadata");
    assert_eq!(
        serde_json::Value::Object(rows[1].clone()),
        serde_json::json!({
            "name": "http_requests_total",
            "type": "sum",
            "aggregationTemporality": 2,
            "isMonotonic": true,
            "description": "Requests served.",
        })
    );
    assert_eq!(
        serde_json::Value::Object(rows[2].clone()),
        serde_json::json!({"name": "queue_depth", "type": "histogram", "unit": "items"})
    );

    let objects = meta.objects(&records, Some("prom"));
    let stored: Vec<_> = rows.iter().zip(&objects).collect();
    let back = meta.read_stored(&stored).unwrap();
    assert_eq!(
        back.iter().map(|r| meta.record_object(r, "")).collect::<Vec<_>>(),
        rows,
        "stored metadata reads back as it was received"
    );
    commit(&mut source, &records);
    assert_eq!(sent.join().unwrap().0, 204);
    assert!(
        source
            .poll_batch(100, 1 << 20, Duration::from_millis(200))
            .unwrap()
            .is_empty()
    );
}

/// A histogram whose buckets do not add up refuses its request whole with why, and one whose spans lie far apart is
/// held at what its buckets hold dense, so a few bytes sent are refused as too large rather than made.
#[test]
fn a_histogram_that_does_not_add_up_or_does_not_fit_refuses_the_request() {
    use remote_write::{Histogram, series, span};

    let at = free_port();
    let mut source = prometheus_receiver(at);
    let send = |h: Histogram| {
        let mut s = series(&[("__name__", "latency")], &[(1.0, 1)]);
        s.histograms.push(h);
        remote_write_request(at, REMOTE_WRITE, &remote_write::body(vec![s]))
    };
    let custom = || Histogram {
        schema: -53,
        custom_values: vec![1.0, 2.0],
        ..Default::default()
    };
    for (h, why) in [
        (
            Histogram {
                positive_spans: vec![span(0, 2)],
                positive_deltas: vec![1],
                ..Default::default()
            },
            "spans name 2 buckets, and it holds 1 counts",
        ),
        (
            Histogram {
                positive_spans: vec![span(0, 2)],
                positive_deltas: vec![1, -2],
                ..Default::default()
            },
            "bucket counts go below zero",
        ),
        (
            Histogram {
                schema: -5,
                ..Default::default()
            },
            "of schema -5",
        ),
        (
            Histogram {
                negative_spans: vec![span(0, 1)],
                negative_deltas: vec![1],
                ..custom()
            },
            "custom buckets holds negative buckets",
        ),
        (
            Histogram {
                custom_values: vec![2.0, 1.0],
                ..custom()
            },
            "not finite and strictly increasing",
        ),
        (
            Histogram {
                positive_spans: vec![span(2, 2)],
                positive_deltas: vec![1, 0],
                ..custom()
            },
            "a span past its buckets",
        ),
        (
            Histogram {
                count_int: Some(2),
                sum: 1.0,
                positive_spans: vec![span(0, 1)],
                positive_deltas: vec![1],
                ..Default::default()
            },
            "counts 2 observations, and its buckets hold 1, where only a histogram whose sum is NaN",
        ),
        (
            Histogram {
                count_int: Some(1),
                sum: f64::NAN,
                positive_spans: vec![span(0, 1)],
                positive_deltas: vec![2],
                ..Default::default()
            },
            "counts 1 observations, and its buckets hold 2, more than it counts",
        ),
        (
            Histogram {
                count_float: Some(0.7),
                positive_spans: vec![span(0, 1)],
                positive_counts: vec![0.6],
                ..Default::default()
            },
            "counts 0.7 observations, and its buckets hold 0.6",
        ),
        (
            Histogram {
                count_int: Some(1),
                zero_count_int: Some(1),
                ..custom()
            },
            "custom buckets have no zero bucket",
        ),
        (
            Histogram {
                count_float: Some(0.5),
                zero_count_float: Some(0.5),
                ..custom()
            },
            "custom buckets have no zero bucket",
        ),
        (
            Histogram {
                zero_threshold: 0.001,
                ..custom()
            },
            "custom buckets have no zero bucket",
        ),
    ] {
        let (status, answer) = send(h);
        assert_eq!(status, 400, "{answer}");
        assert!(answer.contains(why), "{answer}, not {why}");
    }
    let far_apart = Histogram {
        count_int: Some(2),
        positive_spans: vec![span(0, 1), span(i32::MAX, 1)],
        positive_deltas: vec![1, 0],
        ..Default::default()
    };
    let (status, answer) = send(far_apart);
    assert_eq!(status, 413, "{answer}");
    assert!(
        source
            .poll_batch(100, 1 << 20, Duration::from_millis(200))
            .unwrap()
            .is_empty()
    );
}

/// A histogram counting observations of NaN, which no bucket holds, is taken where its sum is NaN, as is a staleness
/// marker; float counts are what their buckets hold within rounding.
#[test]
fn a_histogram_counts_what_its_buckets_hold() {
    use remote_write::{Histogram, series, span};

    let at = free_port();
    let mut source = prometheus_receiver(at);
    let mut latency = series(&[("__name__", "latency")], &[]);
    latency.histograms = vec![
        Histogram {
            count_int: Some(3),
            sum: f64::NAN,
            positive_spans: vec![span(0, 1)],
            positive_deltas: vec![2],
            ..Default::default()
        },
        Histogram {
            count_int: Some(0),
            sum: f64::from_bits(0x7ff0_0000_0000_0002),
            ..Default::default()
        },
        // 0.1 + 0.2 is not 0.3 in doubles.
        Histogram {
            count_float: Some(0.3),
            sum: 1.0,
            positive_spans: vec![span(0, 2)],
            positive_counts: vec![0.1, 0.2],
            ..Default::default()
        },
    ];
    let body = remote_write::body(vec![latency]);
    let sent = std::thread::spawn(move || remote_write_request(at, REMOTE_WRITE, &body));

    let records = poll_until(&mut source, 3);
    let meta = smeltr::plan::topology::SourceMeta::prometheus_receiver();
    let rows: Vec<_> = records.iter().map(|r| meta.record_object(r, "")).collect();
    assert_eq!(rows[0]["count"], 3.0);
    assert_eq!(rows[1]["count"], 0.0);
    assert_eq!(rows[2]["count"], 0.3);
    commit(&mut source, &records);
    let (status, answer) = sent.join().unwrap();
    assert_eq!(status, 204, "{answer}");
}

/// Prometheus itself, scraping its own metrics, delivers them over remote_write and hears each request answered.
#[test]
fn prometheus_remote_writes_what_it_scrapes() {
    let Some(image) = live::prometheus("prometheus_remote_writes_what_it_scrapes") else {
        return;
    };
    let at = free_port();
    // A scrape of Prometheus's own metrics is hundreds of series, more than the small receiver of the tests above reads.
    let mut source = prometheus_receiver_reading(at, 1 << 20);
    let web = free_port();

    let dir = std::env::temp_dir().join(format!("smeltr-prometheus-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let config = format!(
        r#"global:
  scrape_interval: 1s
scrape_configs:
  - job_name: self
    static_configs: [{{targets: ["{web}"]}}]
remote_write:
  - url: http://{at}/api/v1/write
    authorization: {{credentials: s3cret}}
    queue_config: {{batch_send_deadline: 1s, max_samples_per_send: 100}}
"#
    );
    std::fs::write(dir.join("prometheus.yml"), config).unwrap();
    crate::harness::docker::share(&dir);
    let name = format!("smeltr-prometheus-{}", std::process::id());
    let started = std::process::Command::new("docker")
        .args(["run", "-d", "--rm", "--network", "host", "--name", &name, "-v"])
        .arg(format!("{}:/smeltr:ro", dir.display()))
        .args([image.as_str(), "--config.file=/smeltr/prometheus.yml"])
        .arg(format!("--web.listen-address={web}"))
        .arg("--storage.tsdb.path=/tmp/prometheus")
        .output()
        .expect("docker runs");
    assert!(started.status.success(), "{}", String::from_utf8_lossy(&started.stderr));
    let prometheus = Container(name);

    let meta = smeltr::plan::topology::SourceMeta::prometheus_receiver();
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    let up = loop {
        assert!(std::time::Instant::now() < deadline, "no `up` sample arrived");
        let records = source.poll_batch(1000, 16 << 20, Duration::from_millis(200)).unwrap();
        let rows: Vec<_> = records.iter().map(|r| meta.record_object(r, "")).collect();
        commit(&mut source, &records);
        if let Some(up) = rows.into_iter().find(|r| r["name"] == "up") {
            break up;
        }
    };
    assert_eq!(up["value"], 1.0);
    assert_eq!(up["attributes"]["job"], "self");
    assert_eq!(up["attributes"]["instance"], web.to_string());
    // Answered requests leave nothing for Prometheus to retry or drop. Only its remote_write's words are read: a
    // warning of its own about anything else is not this receiver's.
    std::thread::sleep(Duration::from_secs(3));
    let logs = std::process::Command::new("docker")
        .args(["logs", &prometheus.0])
        .output()
        .unwrap();
    let logs = String::from_utf8_lossy(&logs.stderr);
    assert!(
        !logs.contains("non-recoverable") && !logs.contains("Failed to send batch"),
        "{logs}"
    );
    drop(prometheus);
    let _ = std::fs::remove_dir_all(dir);
}

/// An Elasticsearch receiver admitting `s3cret` however it is presented, and `beats:k3y` as an API key's `id:key`.
fn elastic_receiver(listen: SocketAddr) -> Box<dyn Source> {
    elastic_receiver_reading(listen, 4096)
}

fn elastic_receiver_reading(listen: SocketAddr, max_request_bytes: usize) -> Box<dyn Source> {
    let mut secrets = HeldSecrets::default();
    secrets.set_stored([
        ("hook".to_string(), 1, "s3cret".to_string()),
        ("keyed".to_string(), 1, "beats:k3y".to_string()),
    ]);
    let keyed = SecretRef::LeaderStored {
        name: "keyed".into(),
        version: 1,
    };
    open(
        ReceiverConfig {
            receiver: Receiver::Elastic,
            listen,
            tokens: vec![("shippers".into(), token()), ("beats".into(), keyed)],
            identity: None,
            max_request_bytes,
        },
        &secrets,
    )
}

/// One exchange with an Elasticsearch receiver: its status, whether it named itself Elasticsearch, and its JSON answer.
fn elastic(at: SocketAddr, method: &str, path: &str, headers: &str, body: &[u8]) -> (u16, bool, serde_json::Value) {
    let mut stream = TcpStream::connect(at).unwrap();
    write!(
        stream,
        "{method} {path} HTTP/1.1\r\nHost: receiver\r\n{headers}Content-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )
    .and_then(|()| stream.write_all(body))
    .unwrap();
    let mut answer = Vec::new();
    stream.read_to_end(&mut answer).unwrap();
    let answer = String::from_utf8_lossy(&answer).to_string();
    let (head, body) = answer.split_once("\r\n\r\n").unwrap();
    let status = head[9..12].parse().unwrap();
    let named = head
        .to_ascii_lowercase()
        .contains("\r\nx-elastic-product: elasticsearch");
    (status, named, serde_json::from_str(body).unwrap_or_default())
}

fn base64(text: &str) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(text)
}

const BEARER: &str = "Authorization: Bearer s3cret\r\n";

/// A bulk request is answered once every document it takes is committed, with an item per action in the order sent:
/// each `index` or `create` of a JSON object created, filed under the index, id and pipeline it names or its path and
/// query name, and every other action refused by itself.
#[test]
fn a_bulk_request_is_answered_per_action_once_its_documents_are_committed() {
    let at = free_port();
    let mut source = elastic_receiver(at);
    let body = concat!(
        r#"{"index":{"_index":"logs","_id":"a1","pipeline":"parse"}}"#,
        "\r\n",
        r#"{"message":"one"}"#,
        "\r\n",
        r#"{"create":{}}"#,
        "\n\n",
        r#"{"message":"two","n":2}"#,
        "\n",
        r#"{"delete":{"_id":"a1"}}"#,
        "\n",
        r#"{"index":{}}"#,
        "\n",
        r#""not an object""#,
        "\n",
    );
    let sent = std::thread::spawn(move || elastic(at, "POST", "/web/_bulk?pipeline=tidy", BEARER, body.as_bytes()));

    let records = poll_until(&mut source, 2);
    let raw: Vec<&[u8]> = records.iter().map(|r| r.bytes().unwrap()).collect();
    assert_eq!(raw, [br#"{"message":"one"}"#.as_slice(), br#"{"message":"two","n":2}"#]);
    let meta = smeltr::plan::topology::SourceMeta::ELASTIC_RECEIVER;
    let objects = meta.objects(&records, Some("es"));
    assert_eq!(objects[0]["index"], "logs");
    assert_eq!(objects[0]["id"], "a1");
    assert_eq!(objects[0]["pipeline"], "parse");
    assert_eq!(objects[1]["index"], "web");
    assert!(
        objects[1].get("id").is_none(),
        "an id the receiver made up is not the sender's"
    );
    assert_eq!(objects[1]["pipeline"], "tidy");
    assert_eq!(objects[1]["remote"], "127.0.0.1");
    assert_eq!(objects[1]["type"], "elasticReceiver");
    let none = smeltr::io::RecordObject::new();
    let stored: Vec<_> = objects.iter().map(|o| (&none, o)).collect();
    let back = meta.read_stored(&stored).unwrap();
    assert_eq!(
        meta.objects(&back, Some("es")),
        objects,
        "a stored record reads back as it was received"
    );

    std::thread::sleep(Duration::from_millis(200));
    assert!(!sent.is_finished(), "nothing is answered before it is delivered");
    commit(&mut source, &records);
    let (status, named, answer) = sent.join().unwrap();
    assert_eq!((status, named), (200, true), "{answer}");
    assert_eq!(answer["errors"], true);
    let items = answer["items"].as_array().unwrap();
    let statuses: Vec<_> = items
        .iter()
        .map(|item| {
            let (action, result) = item.as_object().unwrap().iter().next().unwrap();
            (action.as_str(), result["status"].as_u64().unwrap())
        })
        .collect();
    assert_eq!(
        statuses,
        [("index", 201), ("create", 201), ("delete", 400), ("index", 400)]
    );
    assert_eq!(items[0]["index"]["_id"], "a1");
    assert!(
        items[1]["create"]["_id"].is_string(),
        "a created document is given an id"
    );
}

/// A sender is admitted by a bearer token, an API key as sent or as its `id:key`, or a Basic password, and refused
/// otherwise; whatever is refused is refused in Elasticsearch's words and before anything reaches the inbox.
#[test]
fn an_elastic_receiver_refuses_in_elasticsearchs_words() {
    let at = free_port();
    let mut source = elastic_receiver(at);
    for headers in [
        BEARER.to_string(),
        "Authorization: ApiKey s3cret\r\n".to_string(),
        format!("Authorization: ApiKey {}\r\n", base64("beats:k3y")),
        format!("Authorization: Basic {}\r\n", base64("elastic:s3cret")),
    ] {
        let (status, named, cluster) = elastic(at, "GET", "/", &headers, b"");
        assert_eq!((status, named), (200, true), "{headers}");
        assert!(cluster["version"]["number"].is_string(), "{cluster}");
        let (status, _, license) = elastic(at, "GET", "/_license", &headers, b"");
        assert_eq!(
            (status, &license["license"]["status"]),
            (200, &serde_json::json!("active"))
        );
    }
    for headers in [
        String::new(),
        "Authorization: Bearer wrong\r\n".to_string(),
        "Authorization: Splunk s3cret\r\n".to_string(),
        format!("Authorization: Basic {}\r\n", base64("s3cret")),
    ] {
        let (status, named, refused) = elastic(at, "POST", "/_bulk", &headers, b"{\"index\":{}}\n{}\n");
        assert_eq!((status, named), (401, true), "{headers}");
        assert_eq!(refused["error"]["type"], "security_exception", "{headers}");
    }

    let (status, _, refused) = elastic(at, "POST", "/_bulk", BEARER, b"{\"index\":{}}\n{}\n");
    assert_eq!(
        status, 400,
        "an action naming no index, in a request whose path names none"
    );
    assert_eq!(refused["status"], 400);
    let (status, _, refused) = elastic(at, "PUT", "/web/_bulk", BEARER, b"{\"index\":{}}\n{}\n{\"index\":{}}\n");
    assert_eq!(
        status, 400,
        "an action without its document refuses the whole request: {refused}"
    );
    assert_eq!(elastic(at, "POST", "/web/_bulk", BEARER, b"not json\n").0, 400);
    let (status, named, _) = elastic(at, "PUT", "/_index_template/filebeat", BEARER, b"{}");
    assert_eq!((status, named), (404, true));
    let (status, named, _) = elastic(at, "PUT", "/_index_template/filebeat", "", b"{}");
    assert_eq!((status, named), (401, true), "any path asks for a token");
    assert!(
        source
            .poll_batch(100, 1 << 20, Duration::from_millis(200))
            .unwrap()
            .is_empty()
    );

    let (status, answer) = request(TcpStream::connect(at).unwrap(), "GET", "/health", "", b"");
    assert_eq!(status, 200, "{answer}");
}

/// Runs `image` on the host's network with `dir` at `/smeltr`, as `name`, with `args` after the image.
fn shipper(name: &str, image: &str, dir: &std::path::Path, args: &[&str]) -> Container {
    crate::harness::docker::share(dir);
    let started = std::process::Command::new("docker")
        .args([
            "run",
            "-d",
            "--rm",
            "--network",
            "host",
            "--user",
            "root",
            "--name",
            name,
            "-v",
        ])
        .arg(format!("{}:/smeltr:ro", dir.display()))
        .arg(image)
        .args(args)
        .output()
        .expect("docker runs");
    assert!(started.status.success(), "{}", String::from_utf8_lossy(&started.stderr));
    Container(name.to_string())
}

/// The documents `source` takes until one holds `line` at `field`, each committed as it arrives, and that one. `shipper`
/// is the container sending them, whose logs say why none did.
fn shipped(source: &mut Box<dyn Source>, shipper: &Container, field: &str, line: &str) -> serde_json::Value {
    let meta = smeltr::plan::topology::SourceMeta::ELASTIC_RECEIVER;
    let deadline = std::time::Instant::now() + Duration::from_secs(90);
    loop {
        if std::time::Instant::now() > deadline {
            panic!("no document holding {line:?} arrived: {}", logs(&shipper.0));
        }
        let records = source.poll_batch(100, 1 << 20, Duration::from_millis(200)).unwrap();
        commit(source, &records);
        let found = records
            .iter()
            .zip(meta.objects(&records, Some("es")))
            .find_map(|(r, filed)| {
                let document: serde_json::Value = serde_json::from_slice(r.bytes().unwrap()).unwrap();
                (document[field] == line).then_some(serde_json::json!({"document": document, "filed": filed}))
            });
        if let Some(found) = found {
            return found;
        }
    }
}

/// The logs of container `name`, both streams.
fn logs(name: &str) -> String {
    let logs = std::process::Command::new("docker")
        .args(["logs", name])
        .output()
        .unwrap();
    format!(
        "{}{}",
        String::from_utf8_lossy(&logs.stdout),
        String::from_utf8_lossy(&logs.stderr)
    )
}

/// Filebeat, told not to set up templates or lifecycle policies, ships a line it tails with an API key and hears each
/// bulk request answered.
#[test]
fn elastic_filebeat_ships_a_line_it_tails() {
    let Some(image) = live::filebeat("elastic_filebeat_ships_a_line_it_tails") else {
        return;
    };
    let at = free_port();
    let mut source = elastic_receiver(at);
    let dir = std::env::temp_dir().join(format!("smeltr-filebeat-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("app.log"), "shipped by filebeat\n").unwrap();
    let config = format!(
        r#"filebeat.inputs:
  - type: filestream
    id: app
    paths: [/smeltr/app.log]
output.elasticsearch:
  hosts: ["http://{at}"]
  api_key: "beats:k3y"
setup.template.enabled: false
setup.ilm.enabled: false
"#
    );
    std::fs::write(dir.join("filebeat.yml"), config).unwrap();
    let name = format!("smeltr-filebeat-{}", std::process::id());
    let filebeat = shipper(
        &name,
        &image,
        &dir,
        &["-e", "--strict.perms=false", "-c", "/smeltr/filebeat.yml"],
    );

    let found = shipped(&mut source, &filebeat, "message", "shipped by filebeat");
    assert!(
        found["filed"]["index"].as_str().unwrap().starts_with("filebeat-"),
        "{found}"
    );
    // Answered requests leave nothing for Filebeat to retry or drop.
    std::thread::sleep(Duration::from_secs(3));
    let logs = logs(&filebeat.0);
    assert!(!logs.contains(r#""log.level":"error""#), "{logs}");
    drop(filebeat);
    let _ = std::fs::remove_dir_all(dir);
}

/// Fluent Bit ships a line it tails with a password and hears each bulk request answered.
#[test]
fn elastic_fluent_bit_ships_a_line_it_tails() {
    let Some(image) = live::fluent_bit("elastic_fluent_bit_ships_a_line_it_tails") else {
        return;
    };
    let at = free_port();
    let mut source = elastic_receiver(at);
    let dir = std::env::temp_dir().join(format!("smeltr-fluent-bit-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("app.log"), "shipped by fluent bit\n").unwrap();
    let config = format!(
        r#"[SERVICE]
    Flush 1
[INPUT]
    Name tail
    Path /smeltr/app.log
    Read_from_Head On
[OUTPUT]
    Name es
    Match *
    Host {host}
    Port {port}
    Index apps
    HTTP_User fluent
    HTTP_Passwd s3cret
    Suppress_Type_Name On
"#,
        host = at.ip(),
        port = at.port()
    );
    std::fs::write(dir.join("fluent-bit.conf"), config).unwrap();
    let name = format!("smeltr-fluent-bit-{}", std::process::id());
    let fluent_bit = shipper(
        &name,
        &image,
        &dir,
        &["/fluent-bit/bin/fluent-bit", "-c", "/smeltr/fluent-bit.conf"],
    );

    let found = shipped(&mut source, &fluent_bit, "log", "shipped by fluent bit");
    assert_eq!(found["filed"]["index"], "apps", "{found}");
    std::thread::sleep(Duration::from_secs(3));
    let logs = logs(&fluent_bit.0);
    assert!(!logs.contains("[error]") && !logs.contains("[ warn]"), "{logs}");
    drop(fluent_bit);
    let _ = std::fs::remove_dir_all(dir);
}

/// A request of many small actions is held to what answering them holds, not to its body: one too large to answer is
/// refused `413` before it is read.
#[test]
fn a_bulk_request_too_large_to_answer_is_refused() {
    let at = free_port();
    let mut source = elastic_receiver_reading(at, 8 << 20);
    let deletes = "{\"delete\":{}}\n".repeat(300_000);
    let (status, named, refused) = elastic(at, "POST", "/logs/_bulk", BEARER, deletes.as_bytes());
    assert_eq!((status, named), (413, true), "{refused}");
    assert!(
        refused["error"]["reason"].as_str().unwrap().contains("fewer actions"),
        "{refused}"
    );
    assert!(
        source
            .poll_batch(100, 1 << 20, Duration::from_millis(200))
            .unwrap()
            .is_empty()
    );
}
