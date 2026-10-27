//! The HTTP destination's protocol (`io::http_post`) over the HTTP core: how rows make a body, what each answer means,
//! and what goes on the wire.

use std::borrow::Cow;
use std::collections::VecDeque;
use std::io::{BufRead, BufReader, Read, Write};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use smeltr::io::http::{HttpDestination, Response, Transport, agent_config};
use smeltr::io::http_post::{Post, transport};
use smeltr::io::{Destination, Dispatched, IoError, OwnedRecord, Rejection};
use smeltr::plan::config::Payload;
use smeltr::plan::topology::{
    Compression, DestinationKind, HeldSecrets, HttpDestinationConfig, RequestBody, SecretRef,
};

fn config(url: &str, body: RequestBody) -> HttpDestinationConfig {
    HttpDestinationConfig {
        zone_urls: Default::default(),
        url: url.to_string(),
        body,
        payload: Payload::Record,
        compression: Compression::None,
        headers: Vec::new(),
        credentials: Vec::new(),
        tls: None,
        in_flight: 1,
        max_request_bytes: 1024 * 1024,
        buffer_bytes: 1024 * 1024,
        stall_after: Duration::from_secs(5),
    }
}

/// A request's path, or its head, and its body.
type Request = (String, Vec<u8>);

type Sent = Arc<Mutex<Vec<Request>>>;

/// Answers each request from the script, in order, and records its path and body.
struct ScriptedTransport {
    answers: VecDeque<Response>,
    sent: Sent,
}

impl Transport for ScriptedTransport {
    fn post(&mut self, path: &str, body: &[u8], _budget: Duration) -> Result<Response, IoError> {
        self.sent.lock().unwrap().push((path.to_string(), body.to_vec()));
        Ok(self.answers.pop_front().expect("the script ran out"))
    }
}

fn scripted(statuses: &[u16]) -> (ScriptedTransport, Sent) {
    let sent = Arc::new(Mutex::new(Vec::new()));
    let answers = statuses
        .iter()
        .map(|&status| Response {
            status,
            headers: Vec::new(),
            body: format!("status {status}").into_bytes(),
        })
        .collect();
    (
        ScriptedTransport {
            answers,
            sent: Arc::clone(&sent),
        },
        sent,
    )
}

fn destination(c: &HttpDestinationConfig, statuses: &[u16]) -> (HttpDestination<Post, ScriptedTransport>, Sent) {
    let (t, sent) = scripted(statuses);
    (
        HttpDestination::new(t, Post::new(c)).with_stall_after(Duration::from_secs(5)),
        sent,
    )
}

fn row(value: &[u8]) -> OwnedRecord {
    OwnedRecord {
        value: value.to_vec(),
        payload: Payload::Record,
        ..OwnedRecord::default()
    }
}

fn text(value: &[u8]) -> OwnedRecord {
    OwnedRecord {
        payload: Payload::Text { field: "_raw".into() },
        ..row(value)
    }
}

fn source_bytes(value: &[u8]) -> OwnedRecord {
    OwnedRecord {
        payload: Payload::source_bytes(),
        ..row(value)
    }
}

/// Hands `rows` over as one batch, answered before `deliver` returns.
fn deliver<T: Transport>(d: &mut HttpDestination<Post, T>, rows: Vec<OwnedRecord>) -> Result<Vec<Rejection>, IoError> {
    let (dispatched, refused) = d.deliver(0, Cow::Owned(rows))?;
    assert_eq!(dispatched, Dispatched::Complete);
    Ok(refused)
}

fn bodies(sent: &Sent) -> Vec<String> {
    sent.lock()
        .unwrap()
        .iter()
        .map(|(_, b)| String::from_utf8(b.clone()).unwrap())
        .collect()
}

// --- the body ------------------------------------------------------------

#[test]
fn lines_is_a_line_per_row() {
    let (mut d, sent) = destination(&config("http://unused", RequestBody::Lines), &[200]);
    let refused = deliver(
        &mut d,
        vec![
            row(b" {\"a\":1}\n"),
            text(b"plain words"),
            source_bytes(b"<14>raw\n\xe9"),
        ],
    )
    .unwrap();
    assert!(refused.is_empty());
    // Bytes are base64, so a line break or a byte that is not text stays inside its line.
    assert_eq!(bodies(&sent), vec!["{\"a\":1}\nplain words\nPDE0PnJhdwrp\n"]);
}

/// A line is one row, so a line break inside text is written as `\n`.
#[test]
fn lines_writes_a_line_break_in_text_as_an_escape() {
    let (mut d, sent) = destination(&config("http://unused", RequestBody::Lines), &[200]);
    let refused = deliver(&mut d, vec![text(b"one\ntwo\r\nthree"), text(b"four")]).unwrap();
    assert!(refused.is_empty());
    assert_eq!(bodies(&sent), vec!["one\\ntwo\\r\\nthree\nfour\n"]);
}

#[test]
fn a_json_array_holds_records_as_they_are_text_as_strings_and_bytes_as_base64() {
    let (mut d, sent) = destination(&config("http://unused", RequestBody::JsonArray), &[200]);
    let rows = vec![row(b"{\"a\":1}"), text(b"say \"hi\"\nthere"), source_bytes(b"caf\xe9")];
    let refused = deliver(&mut d, rows).unwrap();
    assert!(refused.is_empty());
    let body: serde_json::Value = serde_json::from_str(&bodies(&sent)[0]).unwrap();
    assert_eq!(body, serde_json::json!([{"a": 1}, "say \"hi\"\nthere", "Y2Fm6Q=="]));
}

#[test]
fn single_sends_each_row_as_its_own_request() {
    let (mut d, sent) = destination(&config("http://unused", RequestBody::Single), &[200, 200, 200]);
    let refused = deliver(&mut d, vec![row(b"{\"n\":0}"), row(b"{\"n\":1}"), text(b"two\nlines")]).unwrap();
    assert!(refused.is_empty());
    assert_eq!(bodies(&sent), vec!["{\"n\":0}", "{\"n\":1}", "two\nlines"]);
}

/// With senders, a batch of single rows goes to the pool whole rather than posting on the caller's thread.
#[test]
fn single_rows_go_to_the_pool_as_one_batch() {
    let c = config("http://unused", RequestBody::Single);
    let (sender, sent) = scripted(&[200, 200, 200]);
    let mut d = HttpDestination::new(scripted(&[]).0, Post::new(&c)).with_senders(vec![sender], 1);
    let rows = vec![row(b"{\"n\":0}"), row(b"{\"n\":1}"), row(b"{\"n\":2}")];
    assert_eq!(d.deliver(7, Cow::Owned(rows)).unwrap().0, Dispatched::InFlight);
    d.flush().unwrap();
    let done = d.poll_completions().unwrap();
    assert_eq!(done.iter().map(|c| c.batch).collect::<Vec<_>>(), vec![7]);
    assert_eq!(bodies(&sent).len(), 3);
}

#[test]
fn gzip_compresses_the_whole_body() {
    let c = HttpDestinationConfig {
        compression: Compression::Gzip,
        ..config("http://unused", RequestBody::Lines)
    };
    let (mut d, sent) = destination(&c, &[200]);
    deliver(&mut d, vec![row(b"{\"a\":1}"), row(b"{\"a\":2}")]).unwrap();
    let gz = sent.lock().unwrap()[0].1.clone();
    let mut plain = String::new();
    flate2::read::GzDecoder::new(&gz[..])
        .read_to_string(&mut plain)
        .unwrap();
    assert_eq!(plain, "{\"a\":1}\n{\"a\":2}\n");
}

/// The transport trims a base address's trailing slashes; the path puts them back, so the URL is the one given.
#[test]
fn the_url_keeps_its_trailing_slash() {
    let (mut d, sent) = destination(&config("https://intake.example/hook/", RequestBody::Lines), &[200]);
    deliver(&mut d, vec![row(b"{}")]).unwrap();
    assert_eq!(sent.lock().unwrap()[0].0, "/");
    let (mut d, sent) = destination(&config("https://intake.example/hook?a=b", RequestBody::Lines), &[200]);
    deliver(&mut d, vec![row(b"{}")]).unwrap();
    assert_eq!(sent.lock().unwrap()[0].0, "");
}

// --- answers -------------------------------------------------------------

#[test]
fn any_2xx_lands_the_request() {
    for status in [200, 201, 202, 204] {
        let (mut d, _) = destination(&config("http://unused", RequestBody::Lines), &[status]);
        assert!(deliver(&mut d, vec![row(b"{}")]).unwrap().is_empty(), "{status}");
    }
}

#[test]
fn a_service_asking_to_wait_gets_the_request_again() {
    for status in [408, 429, 500, 502, 503, 504] {
        let (mut d, sent) = destination(&config("http://unused", RequestBody::Lines), &[status, 200]);
        assert!(deliver(&mut d, vec![row(b"{\"a\":1}")]).unwrap().is_empty(), "{status}");
        let bodies = bodies(&sent);
        assert_eq!(bodies.len(), 2, "{status}");
        assert_eq!(bodies[0], bodies[1], "{status}: the same rows again");
    }
}

#[test]
fn a_service_asking_to_wait_is_waited_its_retry_after() {
    let (mut t, sent) = scripted(&[429, 200]);
    t.answers[0].headers = vec![("retry-after".into(), "1".into())];
    let mut d = HttpDestination::new(t, Post::new(&config("http://unused", RequestBody::Lines)))
        .with_stall_after(Duration::from_secs(5));
    let started = std::time::Instant::now();
    assert!(deliver(&mut d, vec![row(b"{\"a\":1}")]).unwrap().is_empty());
    assert!(started.elapsed() >= Duration::from_secs(1), "{:?}", started.elapsed());
    assert_eq!(bodies(&sent).len(), 2);
}

/// A refusal naming no row is split until the row at fault is alone; only it is refused.
#[test]
fn a_refused_request_is_split_to_the_row_at_fault() {
    for status in [400, 413, 422] {
        // [a,b,c,d] refused; [a,b] lands; [c,d] refused; [c] refused alone; [d] lands.
        let (mut d, sent) = destination(
            &config("http://unused", RequestBody::Lines),
            &[status, 200, status, status, 200],
        );
        let rows = vec![
            row(b"{\"n\":0}"),
            row(b"{\"n\":1}"),
            row(b"{\"bad\":2}"),
            row(b"{\"n\":3}"),
        ];
        let refused = deliver(&mut d, rows).unwrap();
        assert_eq!(refused.len(), 1, "{status}");
        assert_eq!(refused[0].doc, 2, "{status}");
        assert!(
            refused[0].reason.contains(&format!("HTTP {status}")),
            "{}",
            refused[0].reason
        );
        assert!(
            refused[0].record.is_some(),
            "the refused row comes back for the destination's answer"
        );
        assert_eq!(bodies(&sent).len(), 5, "{status}");
    }
}

/// Answers every request of more than `fits` lines as too large to hold, and lands the rest.
struct Oversizing {
    fits: usize,
    sent: Sent,
}

impl Transport for Oversizing {
    fn post(&mut self, path: &str, body: &[u8], _budget: Duration) -> Result<Response, IoError> {
        self.sent.lock().unwrap().push((path.to_string(), body.to_vec()));
        if body.split(|b| *b == b'\n').filter(|l| !l.is_empty()).count() > self.fits {
            return Err(IoError::oversized("the answer was larger than 50 MiB"));
        }
        Ok(Response {
            status: 200,
            headers: Vec::new(),
            body: Vec::new(),
        })
    }
}

/// An answer too large to hold is split like a request too large to take: half the request draws about half the
/// answer. Only a document whose answer alone is past the bound is refused, and it is named.
#[test]
fn an_oversized_answer_is_split_to_the_document_it_answers() {
    let sent: Sent = Arc::new(Mutex::new(Vec::new()));
    let transport = Oversizing {
        fits: 1,
        sent: Arc::clone(&sent),
    };
    let mut d = HttpDestination::new(transport, Post::new(&config("http://unused", RequestBody::Lines)))
        .with_stall_after(Duration::from_secs(5));
    let rows = vec![row(b"{\"n\":0}"), row(b"{\"n\":1}"), row(b"{\"n\":2}")];
    // [0,1,2] too large; [0] lands; [1,2] too large; [1] lands; [2] lands.
    let refused = deliver(&mut d, rows).unwrap();
    assert!(refused.is_empty(), "{refused:?}");
    assert_eq!(bodies(&sent).len(), 5);

    let sent: Sent = Arc::new(Mutex::new(Vec::new()));
    let transport = Oversizing {
        fits: 0,
        sent: Arc::clone(&sent),
    };
    let mut d = HttpDestination::new(transport, Post::new(&config("http://unused", RequestBody::Lines)))
        .with_stall_after(Duration::from_secs(5));
    let refused = deliver(&mut d, vec![row(b"{\"n\":0}"), row(b"{\"n\":1}")]).unwrap();
    assert_eq!(refused.len(), 2);
    for (doc, r) in refused.iter().enumerate() {
        assert_eq!(r.doc, doc);
        assert_eq!(r.reason, "http: the answer to one document was larger than 50 MiB");
        assert!(
            r.record.is_some(),
            "the refused row comes back for the destination's answer"
        );
    }
}

#[test]
fn a_refused_credential_or_an_unknown_answer_stalls_naming_the_status() {
    for (status, says) in [
        (401, "credentials"),
        (403, "credentials"),
        (404, "HTTP 404"),
        (501, "HTTP 501"),
    ] {
        let (mut d, _) = destination(&config("http://unused", RequestBody::Lines), &[status]);
        let err = deliver(&mut d, vec![row(b"{}")]).unwrap_err();
        assert!(err.message.contains(says), "{status}: {}", err.message);
    }
}

// --- the wire ------------------------------------------------------------

/// A service on a socket that answers `n` requests 202 and hands back each one's head, lowercased, and body.
fn service(n: usize) -> (String, std::thread::JoinHandle<Vec<Request>>) {
    answering(n, "202 Accepted\r\n")
}

/// A service on a socket that answers `n` requests with `status`, which ends with its line's CRLF and may carry
/// headers after it.
fn answering(n: usize, status: &'static str) -> (String, std::thread::JoinHandle<Vec<Request>>) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}/v1/input", listener.local_addr().unwrap());
    let served = std::thread::spawn(move || {
        (0..n)
            .map(|_| {
                let (stream, _) = listener.accept().unwrap();
                let mut reader = BufReader::new(stream);
                let mut head = String::new();
                loop {
                    let mut line = String::new();
                    reader.read_line(&mut line).unwrap();
                    if line == "\r\n" {
                        break;
                    }
                    head.push_str(&line.to_ascii_lowercase());
                }
                let length: usize = head
                    .lines()
                    .find_map(|l| l.strip_prefix("content-length: "))
                    .map_or(0, |v| v.trim().parse().unwrap());
                let mut body = vec![0; length];
                reader.read_exact(&mut body).unwrap();
                write!(
                    reader.get_mut(),
                    "HTTP/1.1 {status}Content-Length: 0\r\nConnection: close\r\n\r\n"
                )
                .unwrap();
                (head, body)
            })
            .collect()
    });
    (url, served)
}

/// Committed headers and credentials go on every request, beside the type the body is and its encoding.
#[test]
fn every_request_carries_the_headers_and_says_what_its_body_is() {
    let (url, served) = service(1);
    let c = HttpDestinationConfig {
        compression: Compression::Gzip,
        headers: vec![("X-Source".to_string(), "smeltr".to_string())],
        ..config(&url, RequestBody::JsonArray)
    };
    let agent = agent_config(None, &HeldSecrets::default()).unwrap();
    let credentials = vec![("DD-API-KEY".to_string(), "k-123".to_string())];
    let mut d = HttpDestination::new(transport(&c, &credentials, &agent), Post::new(&c));
    assert!(deliver(&mut d, vec![row(b"{\"a\":1}")]).unwrap().is_empty());
    let (head, body) = served.join().unwrap().remove(0);
    assert!(head.starts_with("post /v1/input "), "{head}");
    for header in [
        "x-source: smeltr",
        "dd-api-key: k-123",
        "content-type: application/json",
        "content-encoding: gzip",
    ] {
        assert!(head.contains(header), "{header} in {head}");
    }
    let mut plain = String::new();
    flate2::read::GzDecoder::new(&body[..])
        .read_to_string(&mut plain)
        .unwrap();
    assert_eq!(plain, "[{\"a\":1}]");
}

/// A redirect is an answer, not a place to go: followed, ureq would send a GET with no body, whose 2xx would count
/// the rows landed, and carry the credentials to wherever it pointed.
#[test]
fn a_redirect_stalls_rather_than_being_followed() {
    let (url, served) = answering(1, "301 Moved Permanently\r\nLocation: /elsewhere\r\n");
    let c = config(&url, RequestBody::Lines);
    let agent = agent_config(None, &HeldSecrets::default()).unwrap();
    let mut d = HttpDestination::new(transport(&c, &[], &agent), Post::new(&c));
    let err = deliver(&mut d, vec![row(b"{}")]).unwrap_err();
    assert!(err.message.contains("HTTP 301"), "{}", err.message);
    assert_eq!(
        served.join().unwrap().len(),
        1,
        "one request, and no second to the redirect's target"
    );
}

/// A secret pasted with its trailing newline is no header value: the destination refuses to start, naming the
/// header and the secret, and never the value.
#[test]
fn a_credential_holding_a_line_break_is_refused_when_built() {
    let c = HttpDestinationConfig {
        credentials: vec![(
            "DD-API-KEY".to_string(),
            SecretRef::LeaderStored {
                name: "dd".to_string(),
                version: 1,
            },
        )],
        ..config("https://intake.example/", RequestBody::Lines)
    };
    let mut secrets = HeldSecrets::default();
    secrets.set_stored([("dd".to_string(), 1, "k-123\n".to_string())]);
    let metrics = smeltr::metrics::MetricsHandle::new(Arc::new(smeltr::metrics::Metrics::new()), "test");
    let Err(e) = smeltr::io::build_destination(&DestinationKind::Http(c), 0, metrics, &secrets, None) else {
        panic!("a header value holding a line break must not build");
    };
    assert!(
        e.message.contains("DD-API-KEY") && e.message.contains("line break"),
        "{}",
        e.message
    );
    assert!(!e.message.contains("k-123"), "{}", e.message);
}
