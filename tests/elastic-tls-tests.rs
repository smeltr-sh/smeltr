//! A real TLS handshake for the Elasticsearch transport: what an identity makes the client present, and what it
//! makes the client accept.
//!
//! Against a rustls server in this process, not an Elastic cluster. What is under test is smeltr's own TLS configuration --
//! `io::http::agent_config` and the agent it builds -- and a server that demands a client certificate under a
//! private authority proves that whether or not it speaks the bulk API. `elastic-live-tests` covers the protocol
//! against a real Elasticsearch, and cannot cover this: the one `just live-up` starts serves plain HTTP.
//!
//! No service, so this runs in `just check` rather than behind `SMELTR_LIVE_TESTS`. A handshake nothing routinely
//! runs is a handshake nobody finds out about.

use crate::certs;

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use smeltr::io::http::{HttpTransport, Transport, agent_config};
use smeltr::plan::topology::{HeldSecrets, Identity, SecretRef};

/// What the bulk API answers when every document landed. Enough for the transport to call the request a success.
const ACCEPTED: &str = r#"{"took":1,"errors":false,"items":[]}"#;

/// A TLS server on loopback that answers one request and then stops.
///
/// Its own thread and its own ephemeral port, so a suite running in parallel cannot collide with itself.
struct Served {
    url: String,
    /// Whether the server completed a handshake and answered. The negative cases turn on this rather than on what
    /// the client made of the failure: a far end that refuses the connection and a client that gives up on the
    /// refusal are the same event, and which of the two the client reports is a race.
    answered: Arc<AtomicBool>,
    thread: std::thread::JoinHandle<()>,
}

impl Served {
    /// Waits for the server to finish with the one connection it takes.
    fn finish(self) -> bool {
        let _ = self.thread.join();
        self.answered.load(Ordering::SeqCst)
    }
}

fn serve(config: Arc<rustls::ServerConfig>) -> Served {
    let listener = TcpListener::bind("127.0.0.1:0").expect("a loopback port");
    let url = format!("https://localhost:{}", listener.local_addr().unwrap().port());
    let answered = Arc::new(AtomicBool::new(false));
    let reporting = answered.clone();
    let thread = std::thread::spawn(move || {
        // One connection is all a test makes, so the first one decides.
        if let Some(Ok(stream)) = listener.incoming().next()
            && answer(stream, &config).is_ok()
        {
            reporting.store(true, Ordering::SeqCst);
        }
    });
    Served { url, answered, thread }
}

fn answer(mut stream: TcpStream, config: &Arc<rustls::ServerConfig>) -> std::io::Result<()> {
    let mut connection = rustls::ServerConnection::new(config.clone())
        .map_err(|e| std::io::Error::other(format!("server connection: {e}")))?;
    let mut tls = rustls::Stream::new(&mut connection, &mut stream);

    // The whole request before any of the answer. Replying to the headers alone closes the connection under a
    // client still writing its body, which fails the request for a reason that is this server's fault rather than
    // the handshake's.
    let mut seen = Vec::new();
    let mut chunk = [0u8; 1024];
    loop {
        let read = tls.read(&mut chunk)?;
        if read == 0 {
            break;
        }
        seen.extend_from_slice(&chunk[..read]);
        if let Some(head) = seen.windows(4).position(|w| w == b"\r\n\r\n")
            && seen.len() - (head + 4) >= content_length(&seen[..head])
        {
            break;
        }
    }

    write!(
        tls,
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{ACCEPTED}",
        ACCEPTED.len()
    )?;
    tls.flush()
}

/// What the request said its body weighs. Absent means none, which is not a request this transport makes.
fn content_length(head: &[u8]) -> usize {
    String::from_utf8_lossy(head)
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse().ok())?
        })
        .unwrap_or(0)
}

/// The certificate entry an identity's reference resolves to, as a worker would hold it.
fn holding(certificate: &str, key: &str) -> HeldSecrets {
    let mut secrets = HeldSecrets::default();
    secrets.take([smeltr::heartbeat::SecretDelivery {
        name: "client".to_string(),
        version: 1,
        value: key.to_string(),
        chain: Some(certificate.to_string()),
    }]);
    secrets
}

fn identity(presents: bool, authorities: Option<&str>) -> Identity {
    Identity {
        id: "acme".to_string(),
        certificate: presents.then(|| SecretRef::LeaderStored {
            name: "client".to_string(),
            version: 1,
        }),
        authorities: authorities.map(str::to_string),
        trust_public: true,
        presented: smeltr::plan::topology::PresentedAs::CLIENT,
    }
}

fn post(url: &str, identity: Option<&Identity>, secrets: &HeldSecrets) -> Result<Vec<u8>, String> {
    certs::install_provider();
    let config = agent_config(identity, secrets).map_err(|e| e.to_string())?;
    let mut transport = HttpTransport::new(url, "application/x-ndjson", Vec::new(), &config);
    transport
        .post("/_bulk", b"{\"index\":{}}\n{\"a\":1}\n", Duration::from_secs(10))
        .map(|r| r.body)
        .map_err(|e| e.to_string())
}

/// An identity's certificate reaches the far end, and its authority is what the far end is checked against.
/// Neither is true of the default agent this replaced.
#[test]
fn an_identity_completes_a_mutual_tls_handshake() {
    let authority = certs::Authority::new("Acme Root CA");
    let served = authority.issue("es.internal", vec!["localhost".to_string()]);
    let client = authority.issue("smeltr-orders-west", vec!["smeltr".to_string()]);

    let server = serve(certs::server_config(&served, &authority));
    let answered = post(
        &server.url,
        Some(&identity(true, Some(&authority.pem))),
        &holding(&client.certificate, &client.key),
    );
    assert!(server.finish(), "the server never answered a request");

    assert_eq!(
        answered.as_deref().map(String::from_utf8_lossy),
        Ok(std::borrow::Cow::Borrowed(ACCEPTED)),
        "an identity naming a certificate, its key and its authority did not complete the handshake"
    );
}

/// The gap this work closed: an Elastic cluster served under a private authority is unreachable without one, and
/// configuration short of an identity could say so.
#[test]
fn without_the_authority_a_privately_issued_server_is_refused() {
    let authority = certs::Authority::new("Acme Root CA");
    let served = authority.issue("es.internal", vec!["localhost".to_string()]);
    let client = authority.issue("smeltr-orders-west", vec!["smeltr".to_string()]);

    let server = serve(certs::server_config(&served, &authority));
    // Everything but the authority: the client still presents its certificate, and still has nothing to check the
    // server's against but the platform's public roots.
    let refused = post(
        &server.url,
        Some(&identity(true, None)),
        &holding(&client.certificate, &client.key),
    );
    assert!(
        !server.finish(),
        "a request reached a server the client should not have trusted"
    );

    let why = refused.err().unwrap_or_default();
    assert!(
        why.contains("UnknownIssuer"),
        "a server under a private authority should not verify against the public roots, and failed instead \
         with: {why:?}"
    );
}

/// An authority a far end does not chain to is not trust, and is refused as plainly as no authority at all.
#[test]
fn an_identity_trusting_another_authority_is_refused() {
    let authority = certs::Authority::new("Acme Root CA");
    let served = authority.issue("es.internal", vec!["localhost".to_string()]);
    let client = authority.issue("smeltr-orders-west", vec!["smeltr".to_string()]);
    let elsewhere = certs::Authority::new("Some Other Root CA");

    let server = serve(certs::server_config(&served, &authority));
    let refused = post(
        &server.url,
        Some(&identity(true, Some(&elsewhere.pem))),
        &holding(&client.certificate, &client.key),
    );
    assert!(
        !server.finish(),
        "a request reached a server the client should not have trusted"
    );

    let why = refused.err().unwrap_or_default();
    assert!(
        why.contains("UnknownIssuer"),
        "a server should not verify against an authority it does not chain to, and failed instead with: {why:?}"
    );
}

/// The other half. A far end that asks for a certificate gets nothing from an identity that presents none, and the
/// connection does not complete -- which is what makes the positive test above about the certificate.
#[test]
fn trusting_without_presenting_is_refused_where_a_certificate_is_demanded() {
    let authority = certs::Authority::new("Acme Root CA");
    let served = authority.issue("es.internal", vec!["localhost".to_string()]);

    let server = serve(certs::server_config(&served, &authority));
    let refused = post(
        &server.url,
        Some(&identity(false, Some(&authority.pem))),
        &HeldSecrets::default(),
    );
    // The far end's own verdict, not what this client made of it: a server that ends the connection and a
    // client that gives up on the alert are the same event, and which of the two the client reports is a race.
    // That the server never answered is what makes the positive test above about the certificate.
    assert!(
        !server.finish(),
        "a server demanding a client certificate answered a connection presenting none"
    );
    assert!(refused.is_err(), "the client was answered by a server that refused it");
}

/// A key that is not the certificate's is refused where it is read, not where it is used: save-time validation
/// catches the pair, and this is the backstop for a bundle that never went through one.
#[test]
fn a_key_that_is_not_pem_is_refused_when_the_agent_is_built() {
    let authority = certs::Authority::new("Acme Root CA");
    let client = authority.issue("smeltr-orders-west", vec!["smeltr".to_string()]);
    let mangled = client.key.replace('\n', " ");

    let built = agent_config(
        Some(&identity(true, Some(&authority.pem))),
        &holding(&client.certificate, &mangled),
    );
    let message = built.err().map(|e| e.to_string()).unwrap_or_default();
    assert!(
        message.contains("unencrypted PEM private key"),
        "a key stripped of its line breaks was not named as the problem: {message:?}"
    );
}
