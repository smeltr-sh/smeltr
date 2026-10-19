//! What the receiver fuzzers share: how a receiver listens and under what TLS, the authorities and certificates every
//! receiver and sender here is under, the receiver built as a source, one HTTP request to it as a client of each kind,
//! and the commit of what it took, alone or as it drains the source.
//!
//! Each fuzzer keeps its settings beyond these, its requests, its checks and its live half.

use std::io::{Read, Write};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, TcpStream};
use std::sync::Arc;
use std::time::{Duration, Instant};

use smeltr::io::{CommitMap, Source, SourceRecord, TopicPartition, build_source};
use smeltr::plan::topology::{HeldSecrets, Identity, Receiver, ReceiverConfig, SecretRef, SourceKind};

use crate::certs::{self, Authority, Issued};
use crate::harness::fuzz::Rng;

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum Tls {
    None,
    /// Under an identity: TLS, any sender.
    Served,
    /// Under an identity with authorities: a sender's certificate must chain to them.
    Vouched,
}

/// The certificate a sender over TLS presents.
#[derive(Clone, Copy, PartialEq, Debug)]
pub enum Presents {
    Nothing,
    /// One the receiver's authorities issued.
    Trusted,
    /// One another authority issued.
    Stranger,
}

// -- the certificates ---------------------------------------------------------------------------------------------

/// The authorities, and the identities under them, every receiver and sender here shares.
pub struct Pki {
    pub authority: Authority,
    /// The receiver's certificate, for `localhost` and the name a container reaches the host by.
    pub server: Issued,
    /// The certificate a trusted sender presents.
    pub sender: Issued,
    /// The certificate a stranger presents: one another authority issued.
    pub stranger: Issued,
    clients: [Arc<rustls::ClientConfig>; 3],
}

impl Pki {
    pub fn new() -> Pki {
        let authority = Authority::new("fuzz authority");
        let server = authority.issue("localhost", vec!["localhost".into(), "host.docker.internal".into()]);
        let sender = authority.issue("sender", vec!["sender".into()]);
        let stranger = Authority::new("stranger authority").issue("sender", vec!["sender".into()]);
        let clients = [
            Arc::new(client_config(&authority, None)),
            Arc::new(client_config(&authority, Some(&sender))),
            Arc::new(client_config(&authority, Some(&stranger))),
        ];
        Pki {
            authority,
            server,
            sender,
            stranger,
            clients,
        }
    }

    /// A TLS client trusting the authority and presenting `presents`.
    pub fn client(&self, presents: Presents) -> Arc<rustls::ClientConfig> {
        self.clients[presents as usize].clone()
    }

    /// The same, speaking `alpn`.
    pub fn client_speaking(&self, presents: Presents, alpn: &[u8]) -> rustls::ClientConfig {
        let issued = match presents {
            Presents::Nothing => None,
            Presents::Trusted => Some(&self.sender),
            Presents::Stranger => Some(&self.stranger),
        };
        let mut config = client_config(&self.authority, issued);
        config.alpn_protocols = vec![alpn.to_vec()];
        config
    }
}

/// A client trusting `authority`, presenting `issued` where there is one.
fn client_config(authority: &Authority, issued: Option<&Issued>) -> rustls::ClientConfig {
    use rustls::pki_types::PrivateKeyDer;
    use rustls::pki_types::pem::PemObject;

    let mut roots = rustls::RootCertStore::empty();
    roots.add(certs::certificates(&authority.pem).remove(0)).unwrap();
    let builder = rustls::ClientConfig::builder_with_provider(smeltr::crypto::in_use())
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots);
    match issued {
        Some(issued) => builder
            .with_client_auth_cert(
                certs::certificates(&issued.certificate),
                PrivateKeyDer::from_pem_slice(issued.key.as_bytes()).unwrap(),
            )
            .unwrap(),
        None => builder.with_no_client_auth(),
    }
}

// -- how a receiver listens ---------------------------------------------------------------------------------------

/// Whether this host has IPv6 loopback.
pub fn ipv6() -> bool {
    std::net::TcpListener::bind("[::1]:0").is_ok()
}

/// The addresses a receiver may listen on, by name, for a coverage list.
pub fn listen_names(ipv6: bool) -> impl Iterator<Item = &'static str> {
    ["0.0.0.0", "::", "127.0.0.1"].into_iter().chain(ipv6.then_some("::1"))
}

/// Where a receiver listens, and where a sender reaches it: every address of the host, of either family. `[::]` takes
/// IPv4 senders too, or is `0.0.0.0` without IPv6.
pub fn random_addresses(rng: &mut Rng, ipv6: bool) -> (IpAddr, IpAddr) {
    let loopback = |v6: bool| -> IpAddr {
        match v6 {
            true => Ipv6Addr::LOCALHOST.into(),
            false => Ipv4Addr::LOCALHOST.into(),
        }
    };
    match rng.below(4) {
        0 => (Ipv4Addr::UNSPECIFIED.into(), loopback(false)),
        1 => (Ipv6Addr::UNSPECIFIED.into(), loopback(false)),
        2 if ipv6 => (loopback(true), loopback(true)),
        _ => (loopback(false), loopback(false)),
    }
}

/// A port nothing listens on at `connect`.
pub fn free_port(connect: IpAddr) -> u16 {
    std::net::TcpListener::bind((connect, 0))
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// No TLS half the time, else served or vouched.
pub fn random_tls(rng: &mut Rng) -> Tls {
    *rng.pick(&[Tls::None, Tls::None, Tls::Served, Tls::Vouched])
}

/// Up to three of what `token` draws, and at least one unless authorities admit a sender alone.
pub fn random_tokens<T>(rng: &mut Rng, tls: Tls, mut token: impl FnMut(&mut Rng, usize) -> T) -> Vec<T> {
    (0..rng.below(3) + usize::from(tls != Tls::Vouched))
        .map(|i| token(rng, i))
        .collect()
}

/// A request limit from the least a save takes, up to a kibibyte doubled one short of `doublings` times: most limits
/// take most requests, so most reach what the receiver makes of their bodies.
pub fn random_limit(rng: &mut Rng, doublings: usize) -> usize {
    match rng.chance(4) {
        true => 1024 + rng.below(2048),
        false => (1024 << rng.below(doublings)) + rng.below(1024),
    }
}

// -- the receiver -------------------------------------------------------------------------------------------------

/// `receiver` opened on `listen`, under `tls` with `pki`'s identity, holding `tokens` for the senders named
/// `{sender}0`, `{sender}1` and on, and taking requests up to `max_request_bytes`.
pub fn build(
    receiver: Receiver,
    listen: SocketAddr,
    tls: Tls,
    tokens: &[String],
    sender: &str,
    max_request_bytes: usize,
    pki: &Pki,
) -> Box<dyn Source> {
    let mut secrets = HeldSecrets::default();
    let mut stored: Vec<_> = (tokens.iter().enumerate())
        .map(|(i, token)| (format!("token{i}"), 1, token.clone()))
        .collect();
    stored.push(("receiver-key".into(), 1, pki.server.key.clone()));
    secrets.set_stored(stored);
    let identity = (tls != Tls::None).then(|| {
        Arc::new(Identity {
            id: "receiver".into(),
            certificate: Some(pki.server.certificate.clone()),
            key: Some(SecretRef::LeaderStored {
                name: "receiver-key".into(),
                version: 1,
            }),
            authorities: (tls == Tls::Vouched).then(|| pki.authority.pem.clone()),
            trust_public: true,
        })
    });
    let config = ReceiverConfig {
        receiver,
        listen,
        tokens: (0..tokens.len())
            .map(|i| {
                let reference = SecretRef::LeaderStored {
                    name: format!("token{i}"),
                    version: 1,
                };
                (format!("{sender}{i}"), reference)
            })
            .collect(),
        identity,
        max_request_bytes,
    };
    let registry = Arc::new(smeltr::metrics::Metrics::new());
    let subjects = Arc::new(smeltr::metrics::Subjects::default());
    build_source(&SourceKind::Receiver(config), &registry, &subjects, &secrets, None).expect("opens")
}

/// Commits every record of `records`, as far as each partition's last.
pub fn commit(source: &mut Box<dyn Source>, records: &[SourceRecord]) {
    let mut done = CommitMap::new();
    for r in records {
        let at = TopicPartition::new(r.topic.clone(), r.partition);
        let o = done.entry(at).or_insert(r.offset);
        *o = (*o).max(r.offset);
    }
    source.commit(&done).unwrap();
}

/// Polls `source`, waiting up to `wait` each time, and commits each batch once `take` has read it. `take` answers
/// whether all that must arrive has. Once that has held without a break for `settle`, so a record sent twice shows
/// too, it answers `true`; when `deadline` passes first, `false`.
pub fn drain(
    source: &mut Box<dyn Source>,
    wait: Duration,
    settle: Duration,
    deadline: Instant,
    mut take: impl FnMut(&[SourceRecord]) -> Result<bool, String>,
) -> Result<bool, String> {
    let mut since: Option<Instant> = None;
    loop {
        let batch = source.poll_batch(10_000, 1 << 26, wait).unwrap();
        let done = take(&batch)?;
        commit(source, &batch);
        since = match done {
            true => since.or(Some(Instant::now())),
            false => None,
        };
        if since.is_some_and(|at| at.elapsed() >= settle) {
            return Ok(true);
        }
        if Instant::now() > deadline {
            return Ok(false);
        }
    }
}

// -- one request --------------------------------------------------------------------------------------------------

/// One HTTP request as a sender writes it.
pub struct Request<'a> {
    pub method: &'a str,
    pub path: &'a str,
    /// Header lines, each ending in CRLF.
    pub headers: &'a [u8],
    pub body: &'a [u8],
    /// Sent in chunks, its length undeclared.
    pub chunked: bool,
    pub presents: Presents,
}

/// What a receiver answered: its status, or 0 where it closed the connection without answering, its head, lowercase,
/// and its body.
#[derive(Debug, PartialEq)]
pub struct Answer {
    pub status: u16,
    pub head: String,
    pub body: String,
}

/// `request` over a new connection to `at`, under TLS where `tls` says. A body larger than fits in the sockets'
/// buffers is sent only once the receiver asks for it with `100 Continue`, so an answer given before the body is read
/// reaches the sender rather than being lost to the reset of a connection closed with the body unread.
pub fn exchange(at: SocketAddr, tls: Tls, pki: &Pki, request: &Request) -> Answer {
    let tcp = TcpStream::connect(at).unwrap();
    tcp.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    tcp.set_write_timeout(Some(Duration::from_secs(10))).unwrap();
    if tls == Tls::None {
        return http(tcp, request);
    }
    let config = pki.client(request.presents);
    let connection = rustls::ClientConnection::new(config, "localhost".try_into().unwrap()).unwrap();
    http(rustls::StreamOwned::new(connection, tcp), request)
}

/// `request` over `stream`, and what came back.
pub fn http(mut stream: impl Read + Write, request: &Request) -> Answer {
    let expect = request.body.len() > 16 * 1024;
    let mut head = format!("{} {} HTTP/1.1\r\nHost: receiver\r\n", request.method, request.path).into_bytes();
    head.extend(request.headers);
    if expect {
        head.extend(b"Expect: 100-continue\r\n");
    }
    let body = match request.chunked {
        true => {
            head.extend(b"Transfer-Encoding: chunked\r\n");
            let mut chunks = Vec::new();
            for chunk in request.body.chunks(4096) {
                chunks.extend(format!("{:x}\r\n", chunk.len()).bytes());
                chunks.extend(chunk);
                chunks.extend(b"\r\n");
            }
            chunks.extend(b"0\r\n\r\n");
            chunks
        }
        false => {
            head.extend(format!("Content-Length: {}\r\n", request.body.len()).bytes());
            request.body.to_vec()
        }
    };
    head.extend(b"Connection: close\r\n\r\n");
    let mut answer = Vec::new();
    if stream.write_all(&head).is_ok() {
        if expect {
            // The interim answer, or the final one given before the body is read.
            let mut byte = [0];
            while !answer.ends_with(b"\r\n\r\n") && stream.read_exact(&mut byte).is_ok() {
                answer.push(byte[0]);
            }
            if answer.starts_with(b"HTTP/1.1 100") {
                answer.clear();
                let _ = stream.write_all(&body);
            }
        } else {
            let _ = stream.write_all(&body);
        }
        let _ = stream.flush();
        let _ = stream.read_to_end(&mut answer);
    }
    let answer = String::from_utf8_lossy(&answer).to_string();
    let (head, body) = answer.split_once("\r\n\r\n").unwrap_or((&answer, ""));
    Answer {
        status: answer.get(9..12).and_then(|s| s.parse().ok()).unwrap_or(0),
        head: head.to_ascii_lowercase(),
        body: body.to_string(),
    }
}
