//! The S2S receiver over a socket, against a fake forwarder: what a forwarder sends becomes records filed under its
//! channel, and a forwarder that asks for acks hears each event id acked once every record holding its bytes is
//! committed. A connection it cannot read, or whose token it does not take, is closed before anything more is
//! admitted. A forwarder's `httpout` sends the same stream to a HEC receiver, one request at a time.

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use std::time::Duration;

use smeltr::io::line_breaker::LineBreaker;
use smeltr::io::{CommitMap, Source, SourceRecord, TopicPartition, build_source};
use smeltr::plan::topology::{
    HecReceiverConfig, HeldSecrets, Identity, Receiver, ReceiverConfig, S2sReceiverConfig, SecretRef, SourceKind,
    SourceMeta,
};

const TOKEN: &str = "0d2a5f6c-4b1e-4c3a-9f7d-2e8b6a1c5d40";

fn free_port() -> SocketAddr {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}

/// An S2S receiver on `listen` that breaks sourcetype `java` before lines starting `ERROR`, taking `token` when given.
fn receiver(
    listen: SocketAddr,
    token: Option<&str>,
    identity: Option<Identity>,
    secrets: &mut HeldSecrets,
) -> Box<dyn Source> {
    let java = LineBreaker(regex::bytes::Regex::new(r"([\r\n]+)ERROR").unwrap());
    let mut tokens = Vec::new();
    if let Some(token) = token {
        secrets.set_stored([("forwarders".to_string(), 1, token.to_string())]);
        tokens.push((
            "forwarders".to_string(),
            SecretRef::LeaderStored {
                name: "forwarders".into(),
                version: 1,
            },
        ));
    }
    let config = ReceiverConfig {
        receiver: Receiver::S2s(S2sReceiverConfig {
            sourcetypes: BTreeMap::from([("java".to_string(), java)]),
        }),
        listen,
        tokens,
        identity: identity.map(Arc::new),
        max_request_bytes: 1 << 20,
    };
    let registry = Arc::new(smeltr::metrics::Metrics::new());
    let subjects = Arc::new(smeltr::metrics::Subjects::default());
    build_source(&SourceKind::Receiver(config), &registry, &subjects, secrets, None).expect("opens")
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

/// A v3 message as a forwarder writes it: big-endian integers, or the little-endian ones it compresses.
fn v3(pairs: &[(&str, &str)], little: bool) -> Vec<u8> {
    let int = |n: usize| match little {
        true => (n as u32).to_le_bytes(),
        false => (n as u32).to_be_bytes(),
    };
    let string = |text: &str, body: &mut Vec<u8>| {
        body.extend(int(text.len() + 1));
        body.extend(text.as_bytes());
        body.push(0);
    };
    let mut body = int(pairs.len()).to_vec();
    for (key, value) in pairs {
        string(key, &mut body);
        string(value, &mut body);
    }
    body.extend(int(0));
    string("_raw", &mut body);
    match little {
        // A compressed frame: its size, its zlib stream's size with the NUL after it, the stream, the NUL.
        true => {
            let mut zlib = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
            zlib.write_all(&body).unwrap();
            let zlib = zlib.finish().unwrap();
            let zlen = zlib.len() as u32 + 1;
            [&(zlen + 4).to_be_bytes()[..], &zlen.to_be_bytes(), &zlib, &[0]].concat()
        }
        false => [&(body.len() as u32).to_be_bytes()[..], &body].concat(),
    }
}

/// A v4 stream, as a forwarder encodes it.
#[derive(Default)]
struct V4(Vec<u8>);

impl V4 {
    fn varint(&mut self, mut n: u64) -> &mut V4 {
        while n >= 0x80 {
            self.0.push(n as u8 | 0x80);
            n >>= 7;
        }
        self.0.push(n as u8);
        self
    }

    fn string(&mut self, s: &[u8]) -> &mut V4 {
        self.varint(s.len() as u64);
        self.0.extend_from_slice(s);
        self
    }

    fn timezone(&mut self) -> &mut V4 {
        self.0.push(0xff);
        self.string(b"N###")
    }

    fn channel(&mut self, id: u64, source: &str, host: &str, sourcetype: &str) -> &mut V4 {
        self.0.push(0xfe);
        self.varint(id);
        for s in [source, host, sourcetype, "2"] {
            self.varint(s.len() as u64 + 1);
            self.0.extend_from_slice(s.as_bytes());
        }
        self.varint(0)
    }

    /// A chunk with an event id, time, `_meta` field `env::lab` and index `main`; `done` when it ends its stream.
    fn chunk(&mut self, channel: u64, id: u64, raw: &[u8], done: bool) -> &mut V4 {
        self.0.push(0xfc);
        self.varint(channel);
        self.varint(1 | 1 << 2 | 1 << 3 | 1 << 4 | 1 << 5 | (done as u64) << 9);
        self.varint(id);
        self.varint(1_700_000_000);
        self.varint(3);
        self.0.extend([0x07, 3]);
        self.0.extend(b"env");
        self.string(b"lab");
        self.0.extend([0x04, 15]);
        self.0.extend(b"_MetaData:Index");
        self.string(b"main");
        self.0.extend([0x04, 5]);
        self.0.extend(b"_path");
        self.string(b"/var/log/app.log");
        self.string(raw)
    }

    /// The heartbeat a forwarder sends while it waits for acks.
    fn heartbeat(&mut self, channel: u64) -> &mut V4 {
        self.0.push(0xfc);
        self.varint(channel);
        self.varint(1 | 1 << 2 | 1 << 3);
        self.varint(0);
        self.varint(1_700_000_000);
        self.varint(1);
        self.0.extend([0x04, 19]);
        self.0.extend(b"MetaData:Sourcetype");
        self.string(b"sourcetype::fwd-hb");
        self.string(b"HB")
    }
}

/// A fake forwarder: the opening a universal forwarder sends, over plain TCP or TLS.
struct Forwarder<S: Read + Write> {
    stream: S,
}

impl Forwarder<TcpStream> {
    fn connect(at: SocketAddr) -> Forwarder<TcpStream> {
        let stream = TcpStream::connect(at).unwrap();
        stream.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        Forwarder { stream }
    }
}

impl<S: Read + Write> Forwarder<S> {
    /// The signature and the capabilities message, and the receiver's answer read.
    fn open(&mut self, ack: bool, compression: bool) -> String {
        self.open_framed(ack, compression, compression)
    }

    /// [`Forwarder::open`], its capabilities message in a compressed frame when `framed`.
    fn open_framed(&mut self, ack: bool, compression: bool, framed: bool) -> String {
        let mut signature = vec![0; 400];
        signature[..25].copy_from_slice(b"--splunk-cooked-mode-v3--");
        signature[128..136].copy_from_slice(b"uf-tests");
        signature[384..388].copy_from_slice(b"8089");
        let capabilities = format!("ack={};compression={}", ack as u8, compression as u8);
        let asks = v3(&[("__s2s_capabilities", &capabilities)], framed);
        self.stream.write_all(&[signature, asks].concat()).unwrap();
        let mut size = [0; 4];
        self.stream.read_exact(&mut size).unwrap();
        let mut answer = vec![0; u32::from_be_bytes(size) as usize];
        self.stream.read_exact(&mut answer).unwrap();
        String::from_utf8_lossy(&answer).into_owned()
    }

    /// The ForwarderInfo message, presenting `token` when given.
    fn info(&mut self, token: Option<&str>, compression: bool) {
        let mut pairs = vec![
            ("_raw", "ForwarderInfo build=0 version=10.6.0.5 hostname=uf-tests"),
            ("_MetaData:Index", "_internal"),
            ("MetaData:Sourcetype", "sourcetype::fwdinfo"),
        ];
        if let Some(token) = token {
            pairs.push(("__s2s_token", token));
        }
        pairs.push(("__s2s_eventId", "0"));
        self.send(&v3(&pairs, compression));
    }

    /// Sends `bytes`, as a forwarder does without waiting to hear whether the receiver took what came before.
    fn send(&mut self, bytes: &[u8]) {
        // A receiver that refused the connection may have closed it already, which the test reads for itself.
        let _ = self.stream.write_all(bytes);
    }

    /// The next ack frame: its first and last id.
    fn ack(&mut self) -> (u64, u64) {
        let op = self.byte().expect("an ack");
        match op {
            0xfb => {
                let id = self.varint();
                (id, id)
            }
            0xfa => (self.varint(), self.varint()),
            other => panic!("not an ack frame: {other:#04x}"),
        }
    }

    fn byte(&mut self) -> Option<u8> {
        let mut byte = [0];
        self.stream.read_exact(&mut byte).ok()?;
        Some(byte[0])
    }

    fn varint(&mut self) -> u64 {
        let (mut n, mut shift) = (0, 0);
        loop {
            let b = self.byte().expect("a varint");
            n |= u64::from(b & 0x7f) << shift;
            if b < 0x80 {
                return n;
            }
            shift += 7;
        }
    }

    /// Whether the receiver closed the connection: it reads its end, or a reset.
    fn closed(&mut self) -> bool {
        let mut rest = [0; 64];
        loop {
            match self.stream.read(&mut rest) {
                Ok(0) => return true,
                Ok(_) => continue,
                Err(e) => {
                    return !matches!(e.kind(), std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut);
                }
            }
        }
    }
}

impl Forwarder<TcpStream> {
    /// Two heartbeats as a forwarder waiting on acks sends them, 200 ms apart.
    fn waiting(&mut self, channel: u64) {
        self.send(&V4::default().heartbeat(channel).0);
        std::thread::sleep(Duration::from_millis(200));
        self.send(&V4::default().heartbeat(channel).0);
    }

    /// Nothing more from the receiver within a moment.
    fn silent(&mut self) -> bool {
        self.stream.set_read_timeout(Some(Duration::from_millis(300))).unwrap();
        let mut byte = [0];
        let silent = match self.stream.read(&mut byte) {
            Err(e) => matches!(e.kind(), std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut),
            Ok(_) => false,
        };
        self.stream.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        silent
    }
}

/// A file's lines and a stack trace each arrive in chunks cut mid-line; each becomes its own record, filed under its
/// channel, the index the forwarder names, its time and its indexed fields.
#[test]
fn chunks_are_broken_into_lines_and_events_by_sourcetype() {
    let at = free_port();
    let mut source = receiver(at, None, None, &mut HeldSecrets::default());
    let mut forwarder = Forwarder::connect(at);
    let answer = forwarder.open(false, false);
    assert!(answer.contains("cap_response=success"), "{answer}");
    assert!(answer.contains("v4=true"), "{answer}");
    forwarder.info(None, false);
    let mut v4 = V4::default();
    v4.timezone()
        .channel(1, "source::/var/log/access.log", "host::web-1", "sourcetype::access")
        .channel(2, "source::/var/log/app.log", "host::web-1", "sourcetype::java")
        .chunk(1, 2, b"GET /a\r\nGET /", false)
        .chunk(2, 3, b"ERROR first\n  at a.B(B.java:1)\n  at a", false)
        .chunk(1, 4, b"b\n", false)
        .chunk(2, 5, b".C(C.java:2)\nERROR second\n", false)
        .chunk(1, 6, b"", true)
        .chunk(2, 7, b"", true);
    forwarder.send(&v4.0);

    let records = poll_until(&mut source, 4);
    assert_eq!(
        text(&records),
        [
            "GET /a",
            "GET /b",
            "ERROR first\n  at a.B(B.java:1)\n  at a.C(C.java:2)",
            "ERROR second",
        ]
    );
    let objects = SourceMeta::S2S_RECEIVER.objects(&records, Some("s2s"));
    assert_eq!(objects[0]["host"], "web-1");
    assert_eq!(objects[0]["source"], "/var/log/access.log");
    assert_eq!(objects[0]["sourcetype"], "access");
    assert_eq!(objects[2]["sourcetype"], "java");
    assert_eq!(objects[0]["index"], "main");
    assert_eq!(objects[0]["fields"], serde_json::json!({"env": "lab"}));
    assert!(
        objects[0]["time"].as_str().unwrap().starts_with("2023-11-14T22:13:20"),
        "{}",
        objects[0]["time"]
    );
    assert_eq!(objects[0]["remote"], "127.0.0.1");
    assert_eq!(objects[0]["type"], "s2sReceiver");
    assert!(forwarder.silent(), "a forwarder that did not ask for acks hears none");
}

/// A forwarder without acks never sends again what a lost connection held, so what its channels hold arrives when the
/// connection ends. With acks, the forwarder sends it again, and it arrives once.
#[test]
fn a_connection_without_acks_that_ends_delivers_what_its_channels_hold() {
    let at = free_port();
    let mut source = receiver(at, None, None, &mut HeldSecrets::default());
    for ack in [false, true] {
        let mut forwarder = Forwarder::connect(at);
        forwarder.open(ack, false);
        forwarder.info(None, false);
        let mut v4 = V4::default();
        v4.timezone()
            .channel(1, "source::/var/log/access.log", "host::web-1", "sourcetype::access")
            .chunk(1, 2, b"GET /a\nGET /par", false);
        forwarder.send(&v4.0);
        let whole = poll_until(&mut source, 1);
        commit(&mut source, &whole);
        drop(forwarder);
        match ack {
            false => assert_eq!(text(&poll_until(&mut source, 1)), ["GET /par"]),
            true => nothing_arrives(&mut source),
        }
    }
}

/// An id is acked only once every record holding its bytes is committed: one whose bytes sit in an event a later id
/// closes is acked with that one.
#[test]
fn an_id_is_acked_once_every_record_holding_its_bytes_is_committed() {
    let at = free_port();
    let mut source = receiver(at, None, None, &mut HeldSecrets::default());
    let mut forwarder = Forwarder::connect(at);
    forwarder.open(true, false);
    forwarder.info(None, false);
    let mut v4 = V4::default();
    v4.timezone()
        .channel(1, "source::/var/log/app.log", "host::web-1", "sourcetype::java")
        .chunk(1, 2, b"ERROR first\n  at a\n", false)
        // At least as long as the bytes held, so they are searched again and the first event breaks off at once.
        .chunk(1, 3, b"ERROR second, at length\n", false)
        .chunk(1, 4, b"", true);
    forwarder.send(&v4.0);

    let records = poll_until(&mut source, 2);
    assert_eq!(text(&records), ["ERROR first\n  at a", "ERROR second, at length"]);
    assert!(forwarder.silent(), "nothing is acked before it is committed");
    commit(&mut source, &records[..1]);
    assert_eq!(
        forwarder.ack(),
        (2, 2),
        "id 2's bytes are all in the first record; id 3's are in the second"
    );
    assert!(forwarder.silent());
    commit(&mut source, &records);
    assert_eq!(forwarder.ack(), (3, 4));
}

/// A forwarder whose window holds only a line no ack covers until it ends sends heartbeats close together, waiting on
/// the receiver alone. Once nothing admitted waits on delivery, such a heartbeat acks that line, which arrives whole once the forwarder
/// sends the rest, and which the connection ending delivers as it is.
#[test]
fn a_line_a_waiting_forwarder_is_stuck_on_is_acked_early_and_arrives_whole() {
    let at = free_port();
    let mut source = receiver(at, None, None, &mut HeldSecrets::default());
    let mut forwarder = Forwarder::connect(at);
    forwarder.open(true, false);
    forwarder.info(None, false);
    let mut v4 = V4::default();
    v4.timezone()
        .channel(1, "source::/var/log/access.log", "host::web-1", "sourcetype::access")
        .chunk(1, 2, b"GET /a\nGET /b", false);
    forwarder.send(&v4.0);
    forwarder.waiting(1);
    let records = poll_until(&mut source, 1);
    assert_eq!(text(&records), ["GET /a"]);
    assert!(
        forwarder.silent(),
        "a request waits on delivery, so the forwarder may not be waiting on its line"
    );
    commit(&mut source, &records);
    assert!(forwarder.silent(), "id 2's bytes are held open in `GET /b`");

    std::thread::sleep(Duration::from_millis(1100));
    forwarder.send(&V4::default().heartbeat(1).heartbeat(1).0);
    assert!(
        forwarder.silent(),
        "neither a heartbeat a second after the last nor two read together are a forwarder waiting"
    );
    forwarder.waiting(1);
    assert_eq!(forwarder.ack(), (2, 2), "acked while its line is held");
    forwarder.send(&V4::default().chunk(1, 3, b" HTTP/1.1\nGET /c", false).0);
    let records = poll_until(&mut source, 1);
    assert_eq!(text(&records), ["GET /b HTTP/1.1"], "the line arrives whole");
    commit(&mut source, &records);
    assert!(
        forwarder.silent(),
        "id 3's bytes are held open in `GET /c`, which no heartbeat acked"
    );

    forwarder.waiting(1);
    assert_eq!(forwarder.ack(), (3, 3));
    drop(forwarder);
    assert_eq!(
        text(&poll_until(&mut source, 1)),
        ["GET /c"],
        "a line acked early is delivered as it is when the connection ends"
    );
}

/// A connection that ends on a malformed frame still delivers a line acked early: the forwarder does not send it again.
#[test]
fn a_line_acked_early_is_delivered_when_a_malformed_frame_ends_the_connection() {
    let at = free_port();
    let mut source = receiver(at, None, None, &mut HeldSecrets::default());
    let mut forwarder = Forwarder::connect(at);
    forwarder.open(true, false);
    forwarder.info(None, false);
    let mut v4 = V4::default();
    v4.timezone()
        .channel(1, "source::/var/log/access.log", "host::web-1", "sourcetype::access")
        .chunk(1, 2, b"GET /a", false);
    forwarder.send(&v4.0);
    forwarder.waiting(1);
    assert_eq!(forwarder.ack(), (2, 2), "acked while its line is held");
    forwarder.send(&[0xf0, 1, 2, 3]);
    assert!(forwarder.closed());
    assert_eq!(text(&poll_until(&mut source, 1)), ["GET /a"]);
}

/// A receiver that takes a token closes a connection presenting none, or another, before admitting anything it sent.
#[test]
fn a_forwarder_without_the_token_is_closed_before_anything_is_admitted() {
    let at = free_port();
    let mut source = receiver(at, Some(TOKEN), None, &mut HeldSecrets::default());
    let mut v4 = V4::default();
    v4.timezone()
        .channel(1, "source::/var/log/access.log", "host::web-1", "sourcetype::access")
        .chunk(1, 2, b"GET /a\n", true);
    for presented in [None, Some("0d2a5f6c-0000-4c3a-9f7d-2e8b6a1c5d40")] {
        let mut forwarder = Forwarder::connect(at);
        forwarder.open(false, false);
        forwarder.info(presented, false);
        forwarder.send(&v4.0);
        assert!(forwarder.closed(), "presenting {presented:?}");
        nothing_arrives(&mut source);
    }
    let mut forwarder = Forwarder::connect(at);
    forwarder.open(false, false);
    forwarder.info(Some(TOKEN), false);
    forwarder.send(&v4.0);
    assert_eq!(text(&poll_until(&mut source, 1)), ["GET /a"]);
}

#[test]
fn a_handshake_message_past_64_kib_is_refused_before_it_is_read() {
    let at = free_port();
    let mut source = receiver(at, Some(TOKEN), None, &mut HeldSecrets::default());
    // Capabilities within the receiver's request limit and past the handshake's: refused once its size and count
    // are held, before any of it is sent, so a sender without the token makes the receiver hold at most a handshake.
    let mut forwarder = Forwarder::connect(at);
    let mut signature = vec![0; 400];
    signature[..25].copy_from_slice(b"--splunk-cooked-mode-v3--");
    let size = ((1 << 20) - 4096_u32).to_be_bytes();
    let count = 1_u32.to_be_bytes();
    forwarder.send(&[signature, size.to_vec(), count.to_vec()].concat());
    assert!(forwarder.closed());
    nothing_arrives(&mut source);
    // One under the handshake's limit is read as before.
    let mut forwarder = Forwarder::connect(at);
    forwarder.open(false, false);
    forwarder.info(Some(TOKEN), false);
    let mut v4 = V4::default();
    v4.timezone()
        .channel(1, "source::/var/log/access.log", "host::web-1", "sourcetype::access")
        .chunk(1, 2, b"GET /a\n", true);
    forwarder.send(&v4.0);
    assert_eq!(text(&poll_until(&mut source, 1)), ["GET /a"]);
}

/// A forwarder that compresses sends its handshake in compressed v3 frames, then `0xf9` and one zlib stream.
#[test]
fn a_compressed_stream_is_read_as_it_inflates() {
    let at = free_port();
    let mut source = receiver(at, Some(TOKEN), None, &mut HeldSecrets::default());
    let mut forwarder = Forwarder::connect(at);
    assert!(forwarder.open(true, true).contains("cap_response=success"));
    forwarder.info(Some(TOKEN), true);
    let mut v4 = V4::default();
    v4.timezone()
        .channel(1, "source::/var/log/access.log", "host::web-1", "sourcetype::access")
        .chunk(1, 2, b"GET /a\nGET /b\n", true);
    let mut zlib = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
    zlib.write_all(&v4.0).unwrap();
    zlib.flush().unwrap();
    forwarder.send(&[&[0xf9][..], zlib.get_ref()].concat());

    let records = poll_until(&mut source, 2);
    assert_eq!(text(&records), ["GET /a", "GET /b"]);
    commit(&mut source, &records);
    assert_eq!(forwarder.ack(), (2, 2));
}

/// A forwarder set to stay on v3 sends its events as v3 messages, which this receiver does not read.
#[test]
fn a_v3_stream_is_refused() {
    let at = free_port();
    let mut source = receiver(at, None, None, &mut HeldSecrets::default());
    let mut forwarder = Forwarder::connect(at);
    forwarder.open(false, false);
    forwarder.info(None, false);
    forwarder.send(&v3(&[("_raw", "GET /a\n"), ("__s2s_eventId", "2")], false));
    assert!(forwarder.closed());
    nothing_arrives(&mut source);
}

/// A frame this receiver does not know closes the connection; what came before it was admitted.
#[test]
fn a_malformed_frame_closes_the_connection() {
    let at = free_port();
    let mut source = receiver(at, None, None, &mut HeldSecrets::default());
    let mut forwarder = Forwarder::connect(at);
    forwarder.open(false, false);
    forwarder.info(None, false);
    let mut v4 = V4::default();
    v4.timezone()
        .channel(1, "source::/var/log/access.log", "host::web-1", "sourcetype::access")
        .chunk(1, 2, b"GET /a\n", false);
    v4.0.extend([0xf0, 1, 2, 3]);
    forwarder.send(&v4.0);
    assert!(forwarder.closed());
    assert_eq!(text(&poll_until(&mut source, 1)), ["GET /a"]);
    nothing_arrives(&mut source);
}

/// The last id there is leaves no next id to ack from, so no forwarder counting up from 2 sends it.
#[test]
fn the_last_event_id_there_is_closes_the_connection() {
    let at = free_port();
    let mut source = receiver(at, None, None, &mut HeldSecrets::default());
    let mut forwarder = Forwarder::connect(at);
    forwarder.open(true, false);
    forwarder.info(None, false);
    let mut v4 = V4::default();
    v4.timezone()
        .channel(1, "source::/var/log/access.log", "host::web-1", "sourcetype::access")
        .chunk(1, u64::MAX, b"GET /a\n", false);
    forwarder.send(&v4.0);
    assert!(forwarder.closed());
    nothing_arrives(&mut source);
}

/// Served under an identity, a receiver speaks S2S inside TLS. A forwarder that compresses over TLS sends its v3
/// messages plain, and only its v4 compressed.
#[test]
fn a_forwarder_is_served_over_tls() {
    crate::certs::install_provider();
    let authority = crate::certs::Authority::new("s2s tests");
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
    let mut source = receiver(at, None, Some(identity), &mut secrets);

    let mut roots = rustls::RootCertStore::empty();
    for root in crate::certs::certificates(&authority.pem) {
        roots.add(root).unwrap();
    }
    let config = Arc::new(
        rustls::ClientConfig::builder_with_provider(smeltr::crypto::in_use())
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    );
    for compression in [false, true] {
        let connection = rustls::ClientConnection::new(config.clone(), "localhost".try_into().unwrap()).unwrap();
        let tcp = TcpStream::connect(at).unwrap();
        tcp.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let mut forwarder = Forwarder {
            stream: rustls::StreamOwned::new(connection, tcp),
        };
        assert!(
            forwarder
                .open_framed(false, compression, false)
                .contains("cap_response=success")
        );
        forwarder.info(None, false);
        let mut v4 = V4::default();
        v4.timezone()
            .channel(1, "source::/var/log/access.log", "host::web-1", "sourcetype::access")
            .chunk(1, 2, b"over tls\n", true);
        match compression {
            true => {
                let mut zlib = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
                zlib.write_all(&v4.0).unwrap();
                zlib.flush().unwrap();
                forwarder.send(&[&[0xf9][..], zlib.get_ref()].concat());
            }
            false => forwarder.send(&v4.0),
        }
        assert_eq!(
            text(&poll_until(&mut source, 1)),
            ["over tls"],
            "compression {compression}"
        );
    }
}

/// A HEC receiver on `listen` taking `TOKEN`, and `ACKER`, a token that acknowledges, with sourcetype `java` broken as
/// [`receiver`] breaks it, and requests of at most `max` bytes.
fn hec_receiver(listen: SocketAddr, max: usize) -> Box<dyn Source> {
    let java = LineBreaker(regex::bytes::Regex::new(r"([\r\n]+)ERROR").unwrap());
    let mut secrets = HeldSecrets::default();
    secrets.set_stored([
        ("forwarders".to_string(), 1, TOKEN.to_string()),
        ("acker".to_string(), 1, ACKER.to_string()),
    ]);
    let stored = |name: &str| SecretRef::LeaderStored {
        name: name.into(),
        version: 1,
    };
    let config = ReceiverConfig {
        receiver: Receiver::Hec(HecReceiverConfig {
            acknowledging: ["acker".to_string()].into(),
            sourcetypes: BTreeMap::from([("java".to_string(), java)]),
        }),
        listen,
        tokens: vec![
            ("forwarders".to_string(), stored("forwarders")),
            ("acker".to_string(), stored("acker")),
        ],
        identity: None,
        max_request_bytes: max,
    };
    let registry = Arc::new(smeltr::metrics::Metrics::new());
    let subjects = Arc::new(smeltr::metrics::Subjects::default());
    build_source(&SourceKind::Receiver(config), &registry, &subjects, &secrets, None).expect("opens")
}

const ACKER: &str = "7c1e9b04-2d6f-4a8e-b3c5-9f0a1d2e3b4c";

/// `v4` as an `httpout` body: `0xf9` and a zlib stream, flushed and not finished.
fn httpout_body(v4: &V4) -> Vec<u8> {
    let mut zlib = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
    zlib.write_all(&v4.0).unwrap();
    zlib.flush().unwrap();
    [&[0xf9][..], zlib.get_ref()].concat()
}

/// One `httpout` request presenting `token`: its status and its answer.
fn httpout(at: SocketAddr, token: &str, body: &[u8]) -> (u16, serde_json::Value) {
    let mut stream = TcpStream::connect(at).unwrap();
    let head = format!(
        "POST /services/collector/s2s HTTP/1.1\r\nHost: x\r\nAuthorization: Splunk {token}\r\n\
         Content-Type: application/octet-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes()).unwrap();
    stream.write_all(body).unwrap();
    let mut answer = String::new();
    stream.read_to_string(&mut answer).unwrap();
    let status = answer[9..12].parse().unwrap();
    let json = answer.split("\r\n\r\n").nth(1).unwrap_or_default();
    (status, serde_json::from_str(json).unwrap_or_default())
}

/// An `httpout` request is its channels' events, broken by sourcetype and filed as over S2S, and each channel's last
/// event ends with the request. It is answered once delivered, whatever its token: `httpout` asks after no `ackId`.
#[test]
fn an_httpout_request_is_its_events_answered_once_delivered() {
    let at = free_port();
    let mut source = hec_receiver(at, 1 << 20);
    let mut v4 = V4::default();
    v4.timezone()
        .channel(1, "source::/var/log/access.log", "host::web-1", "sourcetype::access")
        .channel(2, "source::/var/log/app.log", "host::web-1", "sourcetype::java")
        .chunk(1, 0, b"GET /a\nGET /b", false)
        .chunk(2, 0, b"ERROR first\n  at a.B(B.java:1)\nERROR second\n  at c.D", false);
    let body = httpout_body(&v4);
    for token in [TOKEN, ACKER] {
        let body = body.clone();
        let sent = std::thread::spawn(move || httpout(at, token, &body));

        let records = poll_until(&mut source, 4);
        // The request's end ends each channel's last event, in no order between channels.
        let mut events = text(&records);
        events[2..].sort();
        assert_eq!(
            events,
            [
                "GET /a",
                "ERROR first\n  at a.B(B.java:1)",
                "ERROR second\n  at c.D",
                "GET /b",
            ]
        );
        let objects = SourceMeta::HEC_RECEIVER.objects(&records, Some("hec"));
        assert_eq!(objects[0]["host"], "web-1");
        assert_eq!(objects[0]["source"], "/var/log/access.log");
        assert_eq!(objects[1]["sourcetype"], "java");
        assert_eq!(objects[0]["index"], "main");
        assert_eq!(objects[0]["fields"], serde_json::json!({"env": "lab"}));
        std::thread::sleep(Duration::from_millis(200));
        assert!(!sent.is_finished(), "nothing is answered before it is delivered");
        commit(&mut source, &records);
        assert_eq!(
            sent.join().unwrap(),
            (200, serde_json::json!({"text": "Success", "code": 0}))
        );
    }
}

/// A body that is not a compressed S2S stream, or that inflates past what a request may hold, is refused whole.
#[test]
fn an_httpout_request_it_cannot_read_is_refused_whole() {
    let at = free_port();
    let mut source = hec_receiver(at, 4096);
    let (status, answer) = httpout(at, TOKEN, b"GET /a\n");
    assert_eq!((status, &answer["code"]), (400, &serde_json::json!(6)), "{answer}");

    let mut v4 = V4::default();
    v4.timezone()
        .channel(1, "source::/var/log/access.log", "host::web-1", "sourcetype::access")
        .chunk(1, 0, &[b'x'; 8192], false);
    assert_eq!(httpout(at, TOKEN, &httpout_body(&v4)).0, 413);

    let mut v4 = V4::default();
    v4.timezone()
        .channel(1, "source::/var/log/access.log", "host::web-1", "sourcetype::access")
        .chunk(1, 0, b"GET /a\n", false);
    v4.0.truncate(v4.0.len() - 3);
    assert_eq!(
        httpout(at, TOKEN, &httpout_body(&v4)).0,
        400,
        "a stream that ends partway through a frame"
    );

    let mut v4 = V4::default();
    v4.timezone()
        .channel(1, "source::/var/log/access.log", "host::web-1", "sourcetype::access")
        .chunk(1, 0, b"GET /a\n", false);
    let body = httpout_body(&v4);
    assert_eq!(
        httpout(at, TOKEN, &body[..body.len() - 4]).0,
        400,
        "a body cut short of its sync flush"
    );
    assert_eq!(httpout(at, TOKEN, &[0xf9]).0, 400, "a body of no stream");
    nothing_arrives(&mut source);
}

/// An `httpout` request whose records would hold more than a receiver's inbox is refused as too large, not as busy,
/// so the forwarder does not send it again forever.
#[test]
fn an_httpout_request_past_what_a_receiver_holds_is_too_large() {
    let at = free_port();
    let mut source = hec_receiver(at, 1 << 30);
    let mut v4 = V4::default();
    v4.timezone()
        .channel(1, "source::/var/log/access.log", "host::web-1", "sourcetype::access")
        .chunk(1, 0, &vec![b'x'; 70 << 20], false);
    assert_eq!(httpout(at, TOKEN, &httpout_body(&v4)).0, 413);
    nothing_arrives(&mut source);
}
