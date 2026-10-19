//! The Splunk HTTP Event Collector destination, as a [`Protocol`] over the HTTP core (`io::http`). On the event
//! endpoint each row is the `event` of one HEC envelope, filed under the `_time`, `host`, `source` and `sourcetype` the
//! document carries; on the raw endpoint each row is one line of text, which Splunk parses by its sourcetype.
//!
//! A HEC 200 means the events are queued, not indexed. Under indexer acknowledgement the request carries a channel,
//! the answer carries an `ackId`, and the batch is delivered once `/services/collector/ack` says that id is indexed:
//!
//! ```text
//!   POST /event  ──► 200 {ackId: 7}
//!   POST /ack {acks:[7]} ──► {acks:{7:false}} ... wait ... {acks:{7:true}}  ──► delivered
//! ```
//!
//! A 400 naming an `invalid-event-number` refuses that event alone. Nothing in a refused request was acknowledged,
//! so the events before it go again, and Splunk keeps what it had queued from each try: an event arrives once more for
//! every refused event after it in its request. At-least-once.

use std::time::{Duration, Instant};

use super::http::{HttpTransport, Outcome, Protocol, Response, Transport, percent_encode, push_json_string};
use super::{IoError, OwnedRecord, Payload};
use crate::format::json_scan::{Next, Reader};
use crate::plan::topology::{Acknowledgement, Endpoint, SplunkHecDestinationConfig};

const ACK_PATH: &str = "/services/collector/ack";

/// The first wait between acknowledgement polls, doubling to [`MAX_ACK_POLL`]. An indexer acknowledges once the
/// events are written, which is commonly a second or two.
const FIRST_ACK_POLL: Duration = Duration::from_millis(100);
const MAX_ACK_POLL: Duration = Duration::from_secs(1);

/// The HEC request and answer.
#[derive(Clone, Debug)]
pub struct Hec {
    /// For a row with no `__destination.index`. `None` leaves the token's own default.
    index: Option<String>,
    /// For a document that names none of its own.
    sourcetype: Option<String>,
    source: Option<String>,
    host: Option<String>,
    endpoint: Endpoint,
    acknowledgement: Acknowledgement,
    /// The endpoint, and on the raw one the destination's index, host, source and sourcetype as its query.
    path: String,
}

impl Hec {
    pub fn new(config: &SplunkHecDestinationConfig) -> Hec {
        let path = match config.endpoint {
            Endpoint::Event => "/services/collector/event".to_string(),
            Endpoint::Raw => {
                let query: Vec<String> = [
                    ("index", &config.index),
                    ("sourcetype", &config.sourcetype),
                    ("source", &config.source),
                    ("host", &config.host),
                ]
                .into_iter()
                .filter_map(|(key, value)| Some(format!("{key}={}", percent_encode(value.as_deref()?))))
                .collect();
                format!("/services/collector/raw?{}", query.join("&"))
            }
        };
        Hec {
            index: config.index.clone(),
            sourcetype: config.sourcetype.clone(),
            source: config.source.clone(),
            host: config.host.clone(),
            endpoint: config.endpoint,
            acknowledgement: config.acknowledgement,
            path,
        }
    }
}

/// A transport to the collector at `config.url`, carrying `token`. Under indexer acknowledgement it also carries a
/// channel of its own: Splunk tracks acknowledgements per channel, so each sender's must be its own. The raw endpoint
/// asks every request for one. Otherwise it carries none, since acknowledgements nobody polls would pile up on it.
pub fn transport(config: &SplunkHecDestinationConfig, token: &str, agent: &ureq::config::Config) -> Collector {
    let mut headers = vec![("Authorization".to_string(), format!("Splunk {token}"))];
    if config.acknowledgement == Acknowledgement::Indexer || config.endpoint == Endpoint::Raw {
        headers.push(("X-Splunk-Request-Channel".to_string(), channel()));
    }
    Collector::new(
        HttpTransport::new(&config.url, "application/json", headers, agent),
        config.acknowledgement,
        config.endpoint,
    )
}

/// A transport that, when it carries a channel, asks `/ack` once before its first request whether the token
/// acknowledges, and stops unless that matches `acknowledgement`. Both mismatches index what they are sent before
/// they show: under `indexer`, a token with acknowledgement off answers no `ackId`, so every batch would go again; under
/// `receipt` on the raw endpoint, which always carries a channel, ids nobody polls pile up until Splunk refuses events.
pub struct Collector<T = HttpTransport> {
    inner: T,
    acknowledgement: Acknowledgement,
    checked: bool,
}

impl<T: Transport> Collector<T> {
    pub fn new(inner: T, acknowledgement: Acknowledgement, endpoint: Endpoint) -> Self {
        Collector {
            inner,
            acknowledgement,
            checked: acknowledgement == Acknowledgement::Receipt && endpoint == Endpoint::Event,
        }
    }
}

impl<T: Transport> Transport for Collector<T> {
    fn post(&mut self, path: &str, body: &[u8], budget: Duration) -> Result<Response, IoError> {
        let started = Instant::now();
        if !self.checked {
            // Any other answer is left to the request itself to report.
            let answer = self.inner.post(ACK_PATH, br#"{"acks":[0]}"#, budget)?;
            let off = answer.status == 400 && HecAnswer::read(&answer.body).code == Some(14);
            match self.acknowledgement {
                Acknowledgement::Indexer if off => {
                    return Err(IoError::transient(
                        "this token has indexer acknowledgement off. Turn it on for the token, or set \
                         acknowledgement to \"receipt\" to count a receipt as delivered",
                    ));
                }
                Acknowledgement::Receipt if answer.status == 200 => {
                    return Err(IoError::transient(
                        "this token has indexer acknowledgement on, and receipt polls no acknowledgement. Set \
                         acknowledgement to \"indexer\", or turn acknowledgement off for the token",
                    ));
                }
                _ => {}
            }
            self.checked = true;
        }
        // The ask came out of this request's budget, not on top of it.
        self.inner.post(path, body, budget.saturating_sub(started.elapsed()))
    }
}

/// A fresh GUID.
fn channel() -> String {
    let hex: String = crate::leader::auth::random_bytes(16)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    format!(
        "{}-{}-{}-{}-{}",
        &hex[..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..]
    )
}

/// `"key":"value",`, the value escaped as data: it can come from a pipeline expression.
fn push_field(out: &mut Vec<u8>, key: &str, value: &str) {
    push_json_string(out, key);
    out.push(b':');
    push_json_string(out, value);
    out.push(b',');
}

/// `micros` since the epoch as Unix seconds with six decimals, the form HEC's `time` takes.
pub fn write_epoch_seconds(micros: i64, out: &mut Vec<u8>) {
    if micros < 0 {
        out.push(b'-');
    }
    let abs = micros.unsigned_abs();
    out.extend_from_slice(format!("{}.{:06}", abs / 1_000_000, abs % 1_000_000).as_bytes());
}

/// What Splunk files an event under, where the document carries it at its top level: each member as the document
/// writes it.
#[derive(Default)]
struct Filed<'d> {
    time: Option<&'d str>,
    host: Option<&'d str>,
    source: Option<&'d str>,
    sourcetype: Option<&'d str>,
}

impl<'d> Filed<'d> {
    /// `None` for a document that is no JSON object, or that names one of these twice.
    fn read(value: &'d [u8]) -> Option<Filed<'d>> {
        let braced = value.trim_ascii();
        if !(braced.starts_with(b"{") && braced.ends_with(b"}")) {
            return None;
        }
        let mut r = Reader::new(braced).ok()?;
        r.value().ok()?;
        let mut filed = Filed::default();
        while let Some(key) = r.key().ok()? {
            let member = match &*key {
                "_time" => &mut filed.time,
                "host" => &mut filed.host,
                "source" => &mut filed.source,
                "sourcetype" => &mut filed.sourcetype,
                _ => {
                    r.skip().ok()?;
                    continue;
                }
            };
            if member.is_some() {
                return None;
            }
            *member = Some(std::str::from_utf8(r.raw().ok()?).ok()?);
        }
        r.end().then_some(filed)
    }
}

/// The value `raw` writes, as [`Next`] reads it.
fn read(raw: &str) -> Option<Next<'_>> {
    Reader::of_str(raw).value().ok()
}

/// A string or a number as its text; anything else names nothing. What HEC files an event under is either. A number
/// is spelled as JSON's own reader spells it: a whole number as written, anything else as the f64 it reads as, and
/// one no f64 holds names nothing.
pub(crate) fn text(value: Next<'_>) -> Option<String> {
    match value {
        Next::Str(s) => Some(s.into_owned()),
        Next::Number(number) => {
            if let Ok(whole) = number.parse::<i64>() {
                return Some(whole.to_string());
            }
            if let Ok(whole) = number.parse::<u64>() {
                return Some(whole.to_string());
            }
            let f: f64 = number.parse().ok()?;
            f.is_finite().then(|| format!("{f:?}"))
        }
        _ => None,
    }
}

/// `_time` as HEC's `time`: a number is Unix seconds as it stands, and text is read as a datetime, the form a
/// pipeline's datetime is written in. Text that is no datetime leaves the event at the time Splunk receives it.
fn write_time(raw: Option<&str>, out: &mut Vec<u8>) {
    let written = match raw.and_then(read) {
        Some(number @ Next::Number(_)) => match text(number) {
            Some(seconds) => {
                out.extend_from_slice(br#""time":"#);
                out.extend_from_slice(seconds.as_bytes());
                true
            }
            None => false,
        },
        Some(Next::Str(s)) => match crate::ops::timeparse::parse_auto(&s) {
            Some(micros) => {
                out.extend_from_slice(br#""time":"#);
                write_epoch_seconds(micros, out);
                true
            }
            None => false,
        },
        _ => false,
    };
    if written {
        out.push(b',');
    }
}

/// A number field of a HEC answer: `Some(None)` for `null`, `None` for anything else that is not one.
fn number<T: std::str::FromStr>(r: &mut Reader) -> Option<Option<T>> {
    match r.value().ok()? {
        Next::Number(n) => n.parse().ok().map(Some),
        Next::Null => Some(None),
        _ => None,
    }
}

/// Whether an answer to an ack poll says `id` is acknowledged: `{"acks":{"<id>":true}}`.
fn acked(body: &[u8], id: u64) -> bool {
    let id = id.to_string();
    let read = || -> Option<bool> {
        let mut r = Reader::new(body).ok()?;
        if r.value().ok()? != Next::Object {
            return None;
        }
        let mut acked = false;
        while let Some(key) = r.key().ok()? {
            if key != "acks" {
                r.skip().ok()?;
                continue;
            }
            acked = false;
            if r.value().ok()? != Next::Object {
                return None;
            }
            while let Some(asked) = r.key().ok()? {
                match asked == id {
                    true => acked = r.value().ok()? == Next::Bool(true),
                    false => r.skip().ok()?,
                }
            }
        }
        r.end().then_some(acked)
    };
    read() == Some(true)
}

/// One HEC answer's body, as far as it is read. Splunk answers JSON on every status it produces; a proxy in front of it
/// may not, and then there is no field to read.
#[derive(Default)]
struct HecAnswer {
    text: Option<String>,
    code: Option<i64>,
    invalid_event: Option<usize>,
    ack_id: Option<u64>,
}

impl HecAnswer {
    /// Nothing where the body is no JSON object, or a field is not of its type.
    fn read(body: &[u8]) -> HecAnswer {
        HecAnswer::fields(body).unwrap_or_default()
    }

    fn fields(body: &[u8]) -> Option<HecAnswer> {
        let mut r = Reader::new(body).ok()?;
        if r.value().ok()? != Next::Object {
            return None;
        }
        let mut answer = HecAnswer::default();
        while let Some(key) = r.key().ok()? {
            match &*key {
                "text" => {
                    answer.text = match r.value().ok()? {
                        Next::Str(text) => Some(text.into_owned()),
                        Next::Null => None,
                        _ => return None,
                    }
                }
                "code" => answer.code = number(&mut r)?,
                "invalid-event-number" => answer.invalid_event = number(&mut r)?,
                "ackId" => answer.ack_id = number(&mut r)?,
                _ => r.skip().ok()?,
            }
        }
        r.end().then_some(answer)
    }

    fn reason(&self, response: &Response) -> String {
        match (&self.text, self.code) {
            (Some(text), Some(code)) => format!("{text} (code {code})"),
            _ => response.describe(),
        }
    }
}

impl Protocol for Hec {
    /// An envelope, or on the raw endpoint a line, without its newline.
    type Item = Vec<u8>;

    fn name(&self) -> &'static str {
        "splunk"
    }

    fn path(&self) -> &str {
        &self.path
    }

    /// On the event endpoint, the envelope; the placement is the index. On the raw endpoint, the document.
    fn admit(&self, row: &OwnedRecord) -> Result<Vec<u8>, String> {
        if self.endpoint == Endpoint::Raw {
            // The raw endpoint's index is the request's, so a row naming another would land where it did not ask.
            if let Some(own) = &row.placement
                && Some(own) != self.index.as_ref()
            {
                let filed = self
                    .index
                    .as_deref()
                    .map_or("the token's default index".to_string(), |i| format!("{i:?}"));
                return Err(format!(
                    "names the index {own:?}, and the raw endpoint files every row under {filed}; send it to the event \
                     endpoint to pick its own"
                ));
            }
            return Ok(row.value.clone());
        }
        // Load gives the event endpoint no source bytes: an event's text is UTF-8.
        let record = row.payload == Payload::Record;
        let filed = record.then(|| Filed::read(&row.value)).flatten().unwrap_or_default();
        let mut out = Vec::with_capacity(row.value.len() + 96);
        out.push(b'{');
        write_time(filed.time, &mut out);
        if let Some(index) = row.placement.as_ref().or(self.index.as_ref()) {
            push_field(&mut out, "index", index);
        }
        let own = |raw: Option<&str>| raw.and_then(read).and_then(text);
        for (key, value, default) in [
            ("sourcetype", own(filed.sourcetype), &self.sourcetype),
            ("source", own(filed.source), &self.source),
            ("host", own(filed.host), &self.host),
        ] {
            if let Some(value) = value.as_ref().or(default.as_ref()) {
                push_field(&mut out, key, value);
            }
        }
        out.extend_from_slice(br#""event":"#);
        match record {
            // An empty record is a blank event, which Splunk refuses alone; nothing after the colon would spoil the request.
            true if row.value.trim_ascii().is_empty() => out.extend_from_slice(b"\"\""),
            true => out.extend_from_slice(row.value.trim_ascii()),
            false => push_json_string(&mut out, &String::from_utf8_lossy(&row.value)),
        }
        out.push(b'}');
        Ok(out)
    }

    /// The item and its newline.
    fn size(&self, event: &Vec<u8>) -> usize {
        event.len() + 1
    }

    fn body(&self, events: &[Vec<u8>]) -> Vec<u8> {
        let mut body = Vec::with_capacity(events.iter().map(|e| e.len() + 1).sum());
        for e in events {
            body.extend_from_slice(e);
            body.push(b'\n');
        }
        body
    }

    fn read(&self, response: &Response, items: &[Self::Item]) -> Result<Outcome, IoError> {
        let count = items.len();
        let answer = HecAnswer::read(&response.body);
        match (response.status, answer.invalid_event, answer.code) {
            // HEC reads a request in order and stops at the event it cannot take. The events before it may be queued,
            // but none is acknowledged, so they go again; those after it were never read. Neither is back-pressure.
            (400, Some(at), _) if at < count => Ok(Outcome {
                retryable: (0..count).filter(|&i| i != at).collect(),
                permanent: vec![(at, answer.reason(response))],
                ..Outcome::default()
            }),
            // A token with indexer acknowledgement on asks every request for a channel; receipt sends none.
            (400, _, Some(10)) if self.acknowledgement == Acknowledgement::Receipt => Err(IoError::transient(format!(
                "splunk asked for a request channel, so this token has indexer acknowledgement on. Set \
                 acknowledgement to \"indexer\", or turn acknowledgement off for the token: {}",
                answer.reason(response)
            ))),
            // Data channel missing or invalid: this destination's own setup, fixed there.
            (400, _, Some(10 | 11)) => Err(IoError::transient(format!(
                "splunk refused the request channel: {}",
                answer.reason(response)
            ))),
            // A refusal naming no event, or a request larger than the collector takes: split to find the events at fault.
            (400 | 413, _, _) => Ok(Outcome {
                split: Some(answer.reason(response)),
                ..Outcome::default()
            }),
            (401 | 403, _, _) => Err(IoError::transient(format!(
                "splunk refused the token: {}",
                answer.reason(response)
            ))),
            // Only HEC's own success lands a request. A 2xx from anything else in front of it -- a login page, a
            // proxy's placeholder, a misrouted path -- took nothing.
            (200..=299, _, Some(0)) => Ok(Outcome::default()),
            (200..=299, _, _) => Err(IoError::transient(format!(
                "splunk answered {} without HEC's success; is the URL the collector's?",
                response.describe()
            ))),
            (429 | 503, _, _) => Ok(Outcome {
                retryable: (0..count).collect(),
                throttled: true,
                ..Outcome::default()
            }),
            _ => Err(IoError::transient(format!(
                "splunk returned {}",
                answer.reason(response)
            ))),
        }
    }

    fn confirm(
        &self,
        response: &Response,
        transport: &mut dyn Transport,
        deadline: Instant,
        waited: &mut Duration,
    ) -> Result<bool, IoError> {
        if self.acknowledgement == Acknowledgement::Receipt {
            return Ok(false);
        }
        let Some(id) = HecAnswer::read(&response.body).ack_id else {
            return Err(IoError::transient(
                "the answer has no ackId: this token has indexer acknowledgement off. Turn it on for the token, or \
                 set acknowledgement to \"receipt\" to count a receipt as delivered",
            ));
        };
        let body = format!(r#"{{"acks":[{id}]}}"#);
        let mut wait = FIRST_ACK_POLL;
        let mut busy = false;
        loop {
            let budget = deadline.saturating_duration_since(Instant::now());
            if budget.is_zero() {
                return Err(IoError::transient(format!(
                    "did not acknowledge ackId {id} in time; the request goes again"
                )));
            }
            let polled = transport.post(ACK_PATH, body.as_bytes(), budget)?;
            match polled.status {
                200 => {
                    if acked(&polled.body, id) {
                        return Ok(busy);
                    }
                }
                // Busy: ask again after the wait.
                429 | 503 => busy = true,
                _ => {
                    return Err(IoError::transient(format!(
                        "would not say whether ackId {id} is indexed: {}",
                        HecAnswer::read(&polled.body).reason(&polled)
                    )));
                }
            }
            let nap = wait.min(deadline.saturating_duration_since(Instant::now()));
            std::thread::sleep(nap);
            // Throttled only once the endpoint has said busy: until then the wait is indexing, not the service
            // holding back.
            if busy {
                *waited += nap;
            }
            wait = (wait * 2).min(MAX_ACK_POLL);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_answer_reads_its_fields_and_nothing_from_what_is_no_answer() {
        let answer = HecAnswer::read(br#"{"text":"Invalid data format","code":6,"invalid-event-number":2,"x":[1]}"#);
        assert_eq!(answer.text.as_deref(), Some("Invalid data format"));
        assert_eq!(
            (answer.code, answer.invalid_event, answer.ack_id),
            (Some(6), Some(2), None)
        );
        assert_eq!(HecAnswer::read(br#"{"ackId":7}"#).ack_id, Some(7));
        assert_eq!(
            HecAnswer::read(br#"{"text":5,"code":6}"#).code,
            None,
            "a field not of its type reads nothing"
        );
        assert_eq!(HecAnswer::read(b"<html>busy</html>").code, None);
    }

    #[test]
    fn an_ack_poll_reads_the_one_asked_after() {
        assert!(acked(br#"{"acks":{"6":false,"7":true}}"#, 7));
        assert!(!acked(br#"{"acks":{"7":false}}"#, 7));
        assert!(!acked(br#"{"acks":{"77":true}}"#, 7));
        assert!(!acked(b"not json", 7));
        assert!(!acked(br#"{"acks":[{"7":true}]}"#, 7));
        assert!(!acked(br#"{"acks":{"7":true}}x"#, 7));
    }

    #[test]
    fn a_document_files_its_event_once_per_member() {
        let filed = Filed::read(br#" {"_time":1.5,"host":7,"x":{"host":"no"}} "#).unwrap();
        assert_eq!((filed.time, filed.host), (Some("1.5"), Some("7")));
        assert!(Filed::read(br#"{"host":"a","host":"b"}"#).is_none());
        assert!(Filed::read(br#"{"host":"a"} {}"#).is_none());
    }
}
