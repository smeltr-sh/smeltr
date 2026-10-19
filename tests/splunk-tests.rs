//! The Splunk HEC protocol (`io::splunk`) over the HTTP core: the envelope each row goes in, how an answer is read,
//! and indexer acknowledgement, against a transport that answers by path.

use std::borrow::Cow;
use std::collections::VecDeque;
use std::io::{BufRead, BufReader, Read, Write};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use smeltr::io::http::{HttpDestination, Response, Transport, agent_config};
use smeltr::io::splunk::{Collector, Hec, transport, write_epoch_seconds};
use smeltr::io::{Destination, Dispatched, IoError, OwnedRecord, Rejection};
use smeltr::plan::config::Payload;
use smeltr::plan::topology::{Acknowledgement, Endpoint, HeldSecrets, SecretRef, SplunkHecDestinationConfig};

const EVENT: &str = "/services/collector/event";
const ACK: &str = "/services/collector/ack";
const SUCCESS: &str = r#"{"text":"Success","code":0}"#;

fn config(url: &str, acknowledgement: Acknowledgement) -> SplunkHecDestinationConfig {
    SplunkHecDestinationConfig {
        payload: smeltr::plan::config::Payload::Record,
        zone_urls: Default::default(),
        url: url.to_string(),
        token: SecretRef::LeaderStored {
            name: "hec-token".to_string(),
            version: 1,
        },
        index: Some("main".to_string()),
        sourcetype: Some("smeltr:json".to_string()),
        source: None,
        host: None,
        endpoint: Endpoint::Event,
        acknowledgement,
        identity: None,
        in_flight: 1,
        max_request_bytes: 1024 * 1024,
        buffer_bytes: 1024 * 1024,
        stall_after: Duration::from_secs(5),
    }
}

/// One request the transport was asked to send.
#[derive(Debug)]
struct Sent {
    path: String,
    body: String,
}

/// Answers each request from the script for its path, in order, and records it.
struct ScriptedTransport {
    answers: Vec<(&'static str, VecDeque<Response>)>,
    sent: Arc<Mutex<Vec<Sent>>>,
}

impl Transport for ScriptedTransport {
    fn post(&mut self, path: &str, body: &[u8], _budget: Duration) -> Result<Response, IoError> {
        self.sent.lock().unwrap().push(Sent {
            path: path.to_string(),
            body: String::from_utf8_lossy(body).into_owned(),
        });
        let (_, queue) = self
            .answers
            .iter_mut()
            .find(|(p, _)| *p == path)
            .unwrap_or_else(|| panic!("nothing scripted for {path}"));
        Ok(queue
            .pop_front()
            .unwrap_or_else(|| panic!("the script for {path} ran out")))
    }
}

fn answer(status: u16, body: &str) -> Response {
    Response {
        status,
        headers: Vec::new(),
        body: body.as_bytes().to_vec(),
    }
}

/// A transport answering the event endpoint with `events` and the ack endpoint with `acks`.
fn scripted(events: Vec<Response>, acks: Vec<Response>) -> (ScriptedTransport, Arc<Mutex<Vec<Sent>>>) {
    let sent = Arc::new(Mutex::new(Vec::new()));
    let t = ScriptedTransport {
        answers: vec![(EVENT, events.into()), (ACK, acks.into())],
        sent: Arc::clone(&sent),
    };
    (t, sent)
}

fn destination(t: ScriptedTransport, acknowledgement: Acknowledgement) -> HttpDestination<Hec, ScriptedTransport> {
    HttpDestination::new(t, Hec::new(&config("http://unused", acknowledgement)))
        .with_stall_after(Duration::from_secs(5))
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

fn numbered(n: usize) -> Vec<OwnedRecord> {
    (0..n).map(|i| row(format!(r#"{{"n":{i}}}"#).as_bytes())).collect()
}

/// Hands `rows` over as one batch. With no senders the batch is answered before `deliver` returns.
fn deliver<T: Transport>(d: &mut HttpDestination<Hec, T>, rows: Vec<OwnedRecord>) -> Result<Vec<Rejection>, IoError> {
    let (dispatched, refused) = d.deliver(0, Cow::Owned(rows))?;
    assert_eq!(dispatched, Dispatched::Complete);
    Ok(refused)
}

// --- the envelope --------------------------------------------------------

#[test]
fn a_time_is_unix_seconds_with_six_decimals() {
    let text = |micros| {
        let mut out = Vec::new();
        write_epoch_seconds(micros, &mut out);
        String::from_utf8(out).unwrap()
    };
    assert_eq!(text(1_787_659_200_250_000), "1787659200.250000");
    assert_eq!(text(0), "0.000000");
    assert_eq!(text(-1_500_000), "-1.500000", "before the epoch, not -2.5");
}

/// The row is the `event`, as it is. Its `_time`, `host`, `source` and `sourcetype` sit beside it in the envelope,
/// where HEC files it under them, the destination's own standing in for any it lacks; `__destination.index` picks the index.
#[test]
fn each_row_is_the_event_of_one_envelope() {
    let (t, sent) = scripted(vec![answer(200, SUCCESS)], vec![]);
    let mut d = destination(t, Acknowledgement::Receipt);
    let timed = row(br#"{"_time":"2026-08-25T12:00:00.250000Z","host":"fw01","a":1}"#);
    let mut audit = row(br#"{"_time":1787659200.5,"sourcetype":"pan:traffic","source":7}"#);
    audit.placement = Some("audit".to_string());
    let undated = row(br#"{"_time":"not a time","host":{"a":1}}"#);
    let refused = deliver(
        &mut d,
        vec![
            timed,
            audit,
            undated,
            text(b"a raw line, not JSON"),
            text(b"[12:00] ERROR {x}"),
            text(b"{braced, not JSON}"),
        ],
    )
    .unwrap();
    assert!(refused.is_empty());

    let sent = sent.lock().unwrap();
    assert_eq!(sent.len(), 1, "receipt asks nothing more");
    assert_eq!(sent[0].path, EVENT);
    let events: Vec<serde_json::Value> = sent[0].body.lines().map(|l| serde_json::from_str(l).unwrap()).collect();
    assert_eq!(
        events[0],
        serde_json::json!({"time":1787659200.25,"index":"main","sourcetype":"smeltr:json","host":"fw01",
            "event":{"_time":"2026-08-25T12:00:00.250000Z","host":"fw01","a":1}})
    );
    assert_eq!(events[1]["time"], 1787659200.5, "a number is seconds as it stands");
    assert_eq!(events[1]["index"], "audit");
    assert_eq!(events[1]["sourcetype"], "pan:traffic");
    assert_eq!(events[1]["source"], "7");
    assert!(events[2].get("time").is_none(), "no time is Splunk's own receipt time");
    assert!(events[2].get("host").is_none(), "an object names no host");
    for (i, line) in [
        (3, "a raw line, not JSON"),
        (4, "[12:00] ERROR {x}"),
        (5, "{braced, not JSON}"),
    ] {
        assert_eq!(events[i]["event"], line, "text goes as a string event");
    }
}

/// The raw endpoint takes each document as a line, filed under the destination's own index, host, source and
/// sourcetype for the whole request, and asks every request for a channel.
#[test]
fn the_raw_endpoint_sends_lines_under_the_destination_s_fields() {
    let c = SplunkHecDestinationConfig {
        payload: smeltr::plan::config::Payload::Record,
        endpoint: Endpoint::Raw,
        source: Some("smeltr fw".to_string()),
        ..config("http://unused", Acknowledgement::Receipt)
    };
    let path = "/services/collector/raw?index=main&sourcetype=smeltr%3Ajson&source=smeltr%20fw";
    let sent = Arc::new(Mutex::new(Vec::new()));
    let t = ScriptedTransport {
        answers: vec![(path, vec![answer(200, SUCCESS)].into())],
        sent: Arc::clone(&sent),
    };
    let mut d = HttpDestination::new(t, Hec::new(&c));
    // A row naming the destination's own index goes as any other; one naming another is refused alone, since the
    // request's index is the destination's.
    let mut r = row(br#"{"_time":1,"a":1}"#);
    r.placement = Some("main".to_string());
    let mut elsewhere = row(b"elsewhere");
    elsewhere.placement = Some("audit".to_string());
    let refused = deliver(&mut d, vec![r, elsewhere, row(b"[12:00] ERROR")]).unwrap();
    assert_eq!(refused.len(), 1, "{refused:?}");
    assert!(refused[0].reason.contains("\"audit\""), "{refused:?}");
    assert_eq!(sent.lock().unwrap()[0].body, "{\"_time\":1,\"a\":1}\n[12:00] ERROR\n");

    // Receipt asks `/ack` first too, since this channel would collect acknowledgements; acknowledgement off is the
    // answer it wants.
    let (url, served) = collector(1, "");
    let agent = agent_config(None, &HeldSecrets::default()).unwrap();
    let c = SplunkHecDestinationConfig { url, ..c };
    let mut t = transport(&c, "t", &agent);
    let e = t.post(path, b"x", Duration::from_secs(5)).unwrap_err();
    assert!(e.to_string().contains("indexer acknowledgement on"), "{e}");
    assert!(channel_of(&served.join().unwrap()[0]).is_some());
}

/// An index from a pipeline expression is data, never JSON of its own.
#[test]
fn an_index_is_escaped_not_interpolated() {
    let (t, sent) = scripted(vec![answer(200, SUCCESS)], vec![]);
    let mut d = destination(t, Acknowledgement::Receipt);
    let mut r = row(b"{}");
    r.placement = Some(r#"a","event":"forged"#.to_string());
    deliver(&mut d, vec![r]).unwrap();
    let event: serde_json::Value = serde_json::from_str(sent.lock().unwrap()[0].body.trim_end()).unwrap();
    assert_eq!(event["index"], r#"a","event":"forged"#);
    assert_eq!(event["event"], serde_json::json!({}));
}

// --- reading the answer --------------------------------------------------

fn bad_at(n: usize) -> Response {
    answer(
        400,
        &format!(r#"{{"text":"Invalid data format","code":6,"invalid-event-number":{n}}}"#),
    )
}

/// Splunk names the event it could not take. That one is refused. The events before it were read and none was
/// acknowledged, and the events after it were never read, so all of them go again.
#[test]
fn an_invalid_event_is_refused_alone_and_the_rest_go_again() {
    let (t, sent) = scripted(vec![bad_at(1), answer(200, SUCCESS)], vec![]);
    let mut d = destination(t, Acknowledgement::Receipt);
    let refused = deliver(&mut d, numbered(3)).unwrap();
    assert_eq!(refused.len(), 1);
    assert_eq!(refused[0].doc, 1);
    assert!(
        refused[0].reason.contains("Invalid data format (code 6)"),
        "{}",
        refused[0].reason
    );

    let sent = sent.lock().unwrap();
    assert_eq!(sent.len(), 2);
    assert!(sent[1].body.contains(r#"{"n":0}"#) && sent[1].body.contains(r#"{"n":2}"#));
    assert!(!sent[1].body.contains(r#"{"n":1}"#));
}

/// Several bad events in one request cost one round trip each, and each is refused under its own row.
#[test]
fn several_invalid_events_are_each_refused() {
    // Events 1, 3 and 5 are bad. Each request stops at its first bad event, which is index 1, 2, then 3 of what is
    // left.
    let (t, sent) = scripted(vec![bad_at(1), bad_at(2), bad_at(3), answer(200, SUCCESS)], vec![]);
    let mut d = destination(t, Acknowledgement::Receipt);
    let refused = deliver(&mut d, numbered(6)).unwrap();
    let mut docs: Vec<usize> = refused.iter().map(|r| r.doc).collect();
    docs.sort_unstable();
    assert_eq!(docs, [1, 3, 5]);
    let sent = sent.lock().unwrap();
    assert_eq!(sent.len(), 4);
    assert_eq!(sent[3].body.lines().count(), 3, "{:?}", sent[3]);
}

/// A 400 that names no event refuses the request whole; it is split down to the event at fault.
#[test]
fn a_bare_refusal_is_split_down_to_the_event_at_fault() {
    let no_data = || answer(400, r#"{"text":"Event field is required","code":12}"#);
    let (t, sent) = scripted(vec![no_data(), answer(200, SUCCESS), no_data()], vec![]);
    let mut d = destination(t, Acknowledgement::Receipt);
    let refused = deliver(&mut d, vec![row(b"{}"), row(b"{}")]).unwrap();
    assert_eq!(refused.len(), 1);
    assert_eq!(refused[0].doc, 1);
    assert!(
        refused[0].reason.contains("Event field is required (code 12)"),
        "{}",
        refused[0].reason
    );
    assert_eq!(sent.lock().unwrap().len(), 3);
}

/// A request past the collector's `max_content_length` is split, not sent again whole.
#[test]
fn a_request_too_large_is_split() {
    let too_large = answer(413, r#"{"text":"Content too large","code":27}"#);
    let (t, sent) = scripted(vec![too_large, answer(200, SUCCESS), answer(200, SUCCESS)], vec![]);
    let mut d = destination(t, Acknowledgement::Receipt);
    assert!(deliver(&mut d, vec![row(b"{}"), row(b"{}")]).unwrap().is_empty());
    assert_eq!(sent.lock().unwrap().len(), 3);
}

/// A token Splunk refuses gets fixed, so the request fails rather than refusing its rows, and the reason reaches the
/// operator.
#[test]
fn a_refused_token_fails_the_request() {
    let (t, _) = scripted(vec![answer(403, r#"{"text":"Invalid token","code":4}"#)], vec![]);
    let mut d = destination(t, Acknowledgement::Receipt);
    let e = deliver(&mut d, vec![row(b"{}")]).unwrap_err();
    assert!(!e.is_rejection(), "{e}");
    assert!(e.to_string().contains("Invalid token (code 4)"), "{e}");
}

/// A busy collector is throttling: the request goes again, whole.
#[test]
fn a_busy_collector_is_retried() {
    let (t, sent) = scripted(
        vec![
            answer(503, r#"{"text":"Server is busy","code":9}"#),
            answer(200, SUCCESS),
        ],
        vec![],
    );
    let mut d = destination(t, Acknowledgement::Receipt);
    assert!(deliver(&mut d, numbered(2)).unwrap().is_empty());
    let sent = sent.lock().unwrap();
    assert_eq!(sent.len(), 2);
    assert_eq!(sent[0].body, sent[1].body);
}

/// A collector answering with a status HEC gives no meaning to fails the request, quoting the answer.
#[test]
fn an_unexpected_status_fails_the_request_and_quotes_it() {
    let (t, _) = scripted(vec![answer(500, "upstream exploded")], vec![]);
    let mut d = destination(t, Acknowledgement::Receipt);
    let e = deliver(&mut d, vec![row(b"{}")]).unwrap_err();
    assert!(e.to_string().contains("HTTP 500: upstream exploded"), "{e}");
}

/// Only HEC's own success lands a request. A 2xx from something else in front of it, such as a login page, took
/// nothing, so it fails the request and says so.
#[test]
fn a_success_that_is_not_hecs_fails_the_request() {
    let (t, _) = scripted(vec![answer(200, "<html>Sign in</html>")], vec![]);
    let mut d = destination(t, Acknowledgement::Receipt);
    let e = deliver(&mut d, vec![row(b"{}")]).unwrap_err();
    assert!(!e.is_rejection(), "{e}");
    assert!(e.to_string().contains("without HEC's success"), "{e}");
}

/// A token that has acknowledgement on asks for a channel, which receipt does not send. That stops at the first
/// request with what to change.
#[test]
fn receipt_names_a_token_that_wants_a_channel() {
    let (t, _) = scripted(
        vec![answer(400, r#"{"text":"Data channel is missing","code":10}"#)],
        vec![],
    );
    let mut d = destination(t, Acknowledgement::Receipt);
    let e = deliver(&mut d, vec![row(b"{}")]).unwrap_err();
    assert!(!e.is_rejection(), "{e}");
    assert!(e.to_string().contains("indexer acknowledgement on"), "{e}");
}

#[test]
fn a_refused_channel_fails_the_request() {
    let (t, _) = scripted(
        vec![answer(400, r#"{"text":"Invalid data channel","code":11}"#)],
        vec![],
    );
    let mut d = destination(t, Acknowledgement::Indexer);
    let e = deliver(&mut d, vec![row(b"{}")]).unwrap_err();
    assert!(!e.is_rejection(), "{e}");
    assert!(e.to_string().contains("refused the request channel"), "{e}");
}

// --- indexer acknowledgement ---------------------------------------------

/// A 200 is queued, not indexed. The batch is delivered once the ack endpoint says its id is indexed.
#[test]
fn a_batch_is_delivered_once_the_indexer_acknowledges_it() {
    let (t, sent) = scripted(
        vec![answer(200, r#"{"text":"Success","code":0,"ackId":7}"#)],
        vec![
            answer(200, r#"{"acks":{"7":false}}"#),
            answer(200, r#"{"acks":{"7":true}}"#),
        ],
    );
    let mut d = destination(t, Acknowledgement::Indexer);
    assert!(deliver(&mut d, vec![row(b"{}")]).unwrap().is_empty());

    let sent = sent.lock().unwrap();
    assert_eq!(sent.len(), 3, "the events, then the ack asked until it is true");
    assert_eq!(sent[1].path, ACK);
    assert_eq!(sent[1].body, r#"{"acks":[7]}"#);
    assert_eq!(sent[2].path, ACK);
}

/// A busy ack endpoint is throttling: the waits after it count as throttled time, and the one before it, which is
/// indexing, does not.
#[test]
fn a_busy_ack_endpoint_counts_as_throttled() {
    let registry = Arc::new(smeltr::metrics::Metrics::new());
    let (t, _) = scripted(
        vec![answer(200, r#"{"text":"Success","code":0,"ackId":7}"#)],
        vec![
            answer(200, r#"{"acks":{"7":false}}"#),
            answer(503, r#"{"text":"Server is busy","code":9}"#),
            answer(200, r#"{"acks":{"7":true}}"#),
        ],
    );
    let mut d = destination(t, Acknowledgement::Indexer)
        .with_metrics(smeltr::metrics::MetricsHandle::new(registry.clone(), "egress-2"), 2);
    assert!(deliver(&mut d, vec![row(b"{}")]).unwrap().is_empty());
    let throttled = registry
        .snapshot()
        .into_iter()
        .find(|s| s.key.name == "smeltr_destination_throttled_seconds_total")
        .expect("throttled time counted");
    // The second poll's wait only: 100 ms then 200 ms, doubling.
    assert!(
        matches!(throttled.value, smeltr::metrics::MetricValue::FloatCounter(s) if s == 0.2),
        "{throttled:?}"
    );
}

/// An ack that never comes fails the batch inside its budget. It is sent again, never counted as delivered.
#[test]
fn an_ack_that_never_comes_fails_the_batch() {
    let (t, _) = scripted(
        vec![answer(200, r#"{"text":"Success","code":0,"ackId":1}"#)],
        (0..50).map(|_| answer(200, r#"{"acks":{"1":false}}"#)).collect(),
    );
    let mut d = destination(t, Acknowledgement::Indexer).with_stall_after(Duration::from_millis(600));
    let e = deliver(&mut d, vec![row(b"{}")]).unwrap_err();
    assert!(e.to_string().contains("did not acknowledge"), "{e}");
}

/// A token with acknowledgement off answers with no ackId. Under `indexer` that is a failure that says what to
/// change, never a receipt quietly taken as delivery.
#[test]
fn a_token_without_acknowledgement_fails_and_says_why() {
    let (t, _) = scripted(vec![answer(200, SUCCESS)], vec![]);
    let mut d = destination(t, Acknowledgement::Indexer);
    let e = deliver(&mut d, vec![row(b"{}")]).unwrap_err();
    assert!(!e.is_rejection());
    assert!(e.to_string().contains("indexer acknowledgement off"), "{e}");
}

/// Under `indexer`, a token with acknowledgement off is found out before any event is sent, so none is indexed and
/// then sent again. A token that acknowledges is asked once, not before every request.
#[test]
fn a_collector_asks_once_whether_the_token_acknowledges() {
    let disabled = answer(400, r#"{"text":"ACK is disabled","code":14}"#);
    let (t, sent) = scripted(vec![], vec![disabled]);
    let mut t = Collector::new(t, Acknowledgement::Indexer, Endpoint::Event);
    let e = t.post(EVENT, b"{}", Duration::from_secs(5)).unwrap_err();
    assert!(e.to_string().contains("indexer acknowledgement off"), "{e}");
    assert!(sent.lock().unwrap().iter().all(|s| s.path == ACK));

    let (t, sent) = scripted(
        vec![answer(200, SUCCESS), answer(200, SUCCESS)],
        vec![answer(200, r#"{"acks":{"0":false}}"#)],
    );
    let mut t = Collector::new(t, Acknowledgement::Indexer, Endpoint::Event);
    for _ in 0..2 {
        assert_eq!(t.post(EVENT, b"{}", Duration::from_secs(5)).unwrap().status, 200);
    }
    let paths: Vec<String> = sent.lock().unwrap().iter().map(|s| s.path.clone()).collect();
    assert_eq!(paths, [ACK, EVENT, EVENT]);
}

/// On the raw endpoint receipt carries a channel too, so it asks `/ack` once: acknowledgement off is what it wants, and
/// on stops it before acknowledgements nobody polls can pile up.
#[test]
fn raw_receipt_asks_once_that_the_token_does_not_acknowledge() {
    let on = answer(200, r#"{"acks":{"0":false}}"#);
    let (t, sent) = scripted(vec![], vec![on]);
    let e = Collector::new(t, Acknowledgement::Receipt, Endpoint::Raw)
        .post(EVENT, b"{}", Duration::from_secs(5))
        .unwrap_err();
    assert!(e.to_string().contains("indexer acknowledgement on"), "{e}");
    assert_eq!(sent.lock().unwrap().len(), 1, "nothing sent past the ask");

    let off = answer(400, r#"{"text":"ACK is disabled","code":14}"#);
    let (t, sent) = scripted(vec![answer(200, SUCCESS)], vec![off]);
    let mut t = Collector::new(t, Acknowledgement::Receipt, Endpoint::Raw);
    assert_eq!(t.post(EVENT, b"{}", Duration::from_secs(5)).unwrap().status, 200);
    assert_eq!(sent.lock().unwrap().len(), 2);
}

/// A refusal answers nothing, so there is nothing to wait on.
#[test]
fn a_request_refused_whole_polls_no_ack() {
    let (t, sent) = scripted(vec![bad_at(0)], vec![]);
    let mut d = destination(t, Acknowledgement::Indexer);
    assert_eq!(deliver(&mut d, vec![row(b"{}")]).unwrap().len(), 1);
    assert_eq!(sent.lock().unwrap().len(), 1);
}

// --- the transport -------------------------------------------------------

/// Serves `n` requests on loopback, answering each with HEC's success and the header lines `extra`, and returns each
/// request's head.
fn collector(n: usize, extra: &'static str) -> (String, std::thread::JoinHandle<Vec<String>>) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
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
                reader.read_exact(&mut vec![0; length]).unwrap();
                write!(
                    reader.get_mut(),
                    "HTTP/1.1 200 OK\r\n{extra}Content-Length: {}\r\nConnection: close\r\n\r\n{SUCCESS}",
                    SUCCESS.len()
                )
                .unwrap();
                head
            })
            .collect()
    });
    (url, served)
}

fn channel_of(head: &str) -> Option<&str> {
    head.lines()
        .find_map(|l| l.strip_prefix("x-splunk-request-channel: "))
        .map(str::trim)
}

/// The token goes as `Splunk <token>`. Under indexer acknowledgement each transport, so each sender, asks on a
/// channel of its own; under receipt none is sent, since acknowledgements nobody polls would pile up on it.
#[test]
fn each_transport_carries_the_token_and_a_channel_of_its_own() {
    let (url, served) = collector(1, "");
    let agent = agent_config(None, &HeldSecrets::default()).unwrap();
    let c = config(&url, Acknowledgement::Receipt);
    let mut d = HttpDestination::new(transport(&c, "0000-1111", &agent), Hec::new(&c));
    assert!(deliver(&mut d, vec![row(b"{}")]).unwrap().is_empty());
    let heads = served.join().unwrap();
    assert!(heads[0].starts_with(&format!("post {EVENT} ")), "{}", heads[0]);
    assert!(heads[0].contains("authorization: splunk 0000-1111"), "{}", heads[0]);
    assert!(heads[0].contains("content-type: application/json"), "{}", heads[0]);
    assert_eq!(channel_of(&heads[0]), None);

    // Each transport asks `/ack` once, then sends; both on its channel.
    let (url, served) = collector(4, "");
    let c = config(&url, Acknowledgement::Indexer);
    for _ in 0..2 {
        let mut t = transport(&c, "0000-1111", &agent);
        assert_eq!(t.post(EVENT, b"{}", Duration::from_secs(5)).unwrap().status, 200);
    }
    let heads = served.join().unwrap();
    let channels: Vec<&str> = heads.iter().map(|h| channel_of(h).expect("a channel")).collect();
    assert!(channels.iter().all(|c| c.len() == 36), "GUIDs: {channels:?}");
    assert_eq!(channels[0], channels[1]);
    assert_eq!(channels[2], channels[3]);
    assert_ne!(channels[0], channels[2]);
}

/// A load balancer pins a client with a cookie; the next request carries it back.
#[test]
fn a_cookie_the_collector_sets_goes_back_on_the_next_request() {
    let (url, served) = collector(2, "Set-Cookie: lb=one; Path=/; HttpOnly\r\n");
    let agent = agent_config(None, &HeldSecrets::default()).unwrap();
    let mut t = transport(&config(&url, Acknowledgement::Receipt), "t", &agent);
    for _ in 0..2 {
        assert_eq!(t.post(EVENT, b"{}", Duration::from_secs(5)).unwrap().status, 200);
    }
    let heads = served.join().unwrap();
    assert!(!heads[0].contains("cookie:"), "{}", heads[0]);
    assert!(heads[1].contains("cookie: lb=one\r\n"), "{}", heads[1]);
}
