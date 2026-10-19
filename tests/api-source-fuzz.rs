//! The API sources' offline fuzzers: an HTTP API source and an Okta System Log source under random settings, each
//! reading a service on this host that holds events added while it reads, pages them, and now and then refuses,
//! redirects or asks to wait. A sender of what the source hands out commits all, some or none of it, and now and then
//! the source fails over to a new holder that resumes from the progress the old one left.
//!
//! However it goes, every page the service answers is handed out as the source cuts its records, once and in order,
//! and every event the service holds arrives: an HTTP API source asks each poll from where the last began, less its
//! overlap, and an Okta source from the next link of the last page delivered. A failure is shown until a page reads.
//! Each draws from a deterministic PRNG over a seed range, from `harness::fuzz`: a failure names its seed, and each
//! fuzzer checks that its seeds reached every option it knows.

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Write};
use std::net::TcpListener;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use smeltr::io::{CommitMap, Source, SourceRecord, TopicPartition, build_source};
use smeltr::plan::topology::{
    Api, ApiMethod, ApiRecords, ApiSourceConfig, AppProof, AppSignIn, HeldSecrets, HttpApiConfig, ListStart,
    OktaLogsConfig, OktaSignIn, SecretRef, Since, SourceKind, TokenAt,
};

use crate::harness::fuzz::{self, Coverage, Rng};

/// One request a service read.
struct Request {
    method: String,
    /// Its path and query, as sent.
    target: String,
    /// Its headers, names lowercased.
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl Request {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers.iter().find(|(n, _)| n == name).map(|(_, v)| v.as_str())
    }

    /// The value of query parameter `name`, decoded.
    fn param(&self, name: &str) -> Option<String> {
        let url = reqwest::Url::parse(&format!("http://x{}", self.target)).ok()?;
        url.query_pairs().find(|(k, _)| k == name).map(|(_, v)| v.into_owned())
    }
}

/// An answer: its status, its headers, and its body.
type Answer = (u16, Vec<(String, String)>, Vec<u8>);

/// What answers each request.
type Respond = Arc<Mutex<dyn FnMut(&Request) -> Answer + Send>>;

/// A service on this host answering each request with `respond`, one connection each, until dropped.
struct Service {
    url: String,
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Service {
    fn new(respond: Respond) -> Service {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let at = listener.local_addr().unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let stopping = stop.clone();
        let thread = std::thread::spawn(move || {
            for stream in listener.incoming() {
                if stopping.load(Ordering::Relaxed) {
                    return;
                }
                let Ok(stream) = stream else { continue };
                // A half-open connection must not hold up every request after it.
                let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
                let mut reader = BufReader::new(stream);
                let mut line = String::new();
                if reader.read_line(&mut line).is_err() {
                    continue;
                }
                let mut words = line.split(' ');
                let method = words.next().unwrap_or_default().to_string();
                let target = words.next().unwrap_or_default().to_string();
                let mut headers = Vec::new();
                loop {
                    let mut line = String::new();
                    if reader.read_line(&mut line).is_err() || line == "\r\n" || line.is_empty() {
                        break;
                    }
                    if let Some((name, value)) = line.trim_end().split_once(':') {
                        headers.push((name.trim().to_ascii_lowercase(), value.trim().to_string()));
                    }
                }
                // Read whole, so closing the connection cannot reset it under the answer.
                let length = headers
                    .iter()
                    .find(|(n, _)| n == "content-length")
                    .and_then(|(_, v)| v.parse().ok());
                let mut body = vec![0; length.unwrap_or(0)];
                if std::io::Read::read_exact(&mut reader, &mut body).is_err() {
                    continue;
                }
                let request = Request {
                    method,
                    target,
                    headers,
                    body,
                };
                let (status, head, body) = (respond.lock().unwrap())(&request);
                let mut out = format!("HTTP/1.1 {status} X\r\n").into_bytes();
                for (name, value) in head {
                    out.extend(format!("{name}: {value}\r\n").bytes());
                }
                out.extend(format!("Content-Length: {}\r\nConnection: close\r\n\r\n", body.len()).bytes());
                out.extend(body);
                let _ = reader.get_mut().write_all(&out);
            }
        });
        Service {
            url: format!("http://{at}"),
            stop,
            thread: Some(thread),
        }
    }
}

impl Drop for Service {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        let _ = std::net::TcpStream::connect(self.url.trim_start_matches("http://"));
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn millis(at: SystemTime) -> i64 {
    at.duration_since(UNIX_EPOCH).unwrap().as_millis() as i64
}

/// A time as the sources write one, `2025-10-10T13:00:34.567Z`, in milliseconds since the epoch.
fn read_stamp(text: &str) -> Option<i64> {
    let (date, time) = text.strip_suffix('Z')?.split_once('T')?;
    let mut d = date.split('-').map(|p| p.parse::<i32>().ok());
    let (y, m, day) = (d.next()??, d.next()??, d.next()??);
    let (hms, ms) = time.split_once('.')?;
    let mut t = hms.split(':').map(|p| p.parse::<u8>().ok());
    let (h, min, s) = (t.next()??, t.next()??, t.next()??);
    let date = time::Date::from_calendar_date(y, time::Month::try_from(m as u8).ok()?, day as u8).ok()?;
    let at = date.with_hms(h, min, s).ok()?.assume_utc();
    Some(at.unix_timestamp() * 1000 + ms.parse::<i64>().ok()?)
}

/// One event the service holds.
#[derive(Clone, Debug)]
struct Event {
    id: u64,
    /// When it was recorded, in milliseconds since the epoch.
    at: i64,
}

/// Text an event may carry beside its id.
const TEXTS: &[&str] = &["", "plain", "été 🎉", "say \"hi\"", "tab\there", "{\"x\":[1,2]}"];

/// One page the service answered with 200, and what each of its records must be.
#[derive(Debug)]
struct Page {
    /// The source holder that asked it.
    holder: usize,
    /// The address it was asked at, as the source writes it.
    url: String,
    records: Cut,
    /// Whether the source can read it: a page it cannot is asked again.
    readable: bool,
}

/// Each record a page must be cut into, and the events it carries.
type Cut = Vec<(Vec<u8>, Vec<u64>)>;

/// What a sender of the source's records holds of one page: its records by offset.
type Delivered = BTreeMap<(usize, i32), Vec<(i64, Vec<u8>)>>;

/// Polls `source` once, taking what it hands out into `delivered` under `holder`, and commits all, some or none of
/// what has been delivered so far, as `rng` draws, into `committed`, by page.
fn poll(
    rng: &mut Rng,
    source: &mut Box<dyn Source>,
    holder: usize,
    delivered: &mut Delivered,
    committed: &mut BTreeMap<(usize, i32), i64>,
    cov: &mut Coverage,
) -> Result<(), String> {
    let max_rows = 1 + rng.below(40);
    let bound = if rng.chance(2) { 100 } else { 1 << 16 };
    let max_bytes = 1 + rng.below(bound);
    let batch = source
        .poll_batch(max_rows, max_bytes, Duration::from_millis(30))
        .map_err(|e| e.message)?;
    let size: usize = batch.iter().map(|r| r.bytes().unwrap().len()).sum();
    if batch.len() > max_rows || (batch.len() > 1 && size > max_bytes) {
        return Err(format!(
            "a batch of {} records and {size} bytes, past {max_rows} and {max_bytes}",
            batch.len()
        ));
    }
    for record in &batch {
        let at = (holder, record.partition);
        delivered
            .entry(at)
            .or_default()
            .push((record.offset, record.bytes().unwrap().to_vec()));
    }
    let mut offsets = CommitMap::new();
    let commit = |record: &SourceRecord, offsets: &mut CommitMap| {
        let o = offsets
            .entry(TopicPartition::new(record.topic.clone(), record.partition))
            .or_insert(record.offset);
        *o = (*o).max(record.offset);
    };
    match (rng.below(4), rng.index(batch.len())) {
        // A sender that has not delivered them yet.
        (0, _) => cov.reach("a batch not yet committed"),
        // A prefix, as a sender acknowledges in order.
        (1, Some(part)) => {
            cov.reach("a batch committed in part");
            for record in &batch[..part] {
                commit(record, &mut offsets);
            }
        }
        _ => {
            for record in &batch {
                commit(record, &mut offsets);
            }
        }
    }
    for (at, &offset) in &offsets {
        let held = committed.entry((holder, at.partition)).or_insert(-1);
        *held = (*held).max(offset);
    }
    source.commit(&offsets).map_err(|e| e.message)
}

/// The failures seed `seed` meets: for one seed in `every`, `count` of the `kinds`, each kind in turn; none for the
/// rest. Failures cost the source a second's wait each, so only some seeds meet them.
fn failures_in_turn(seed: u64, every: u64, count: usize, kinds: usize) -> Vec<usize> {
    if !seed.is_multiple_of(every) {
        return Vec::new();
    }
    let turn = (seed / every) as usize;
    (0..count).map(|k| (turn + k) % kinds).collect()
}

/// Adds up to 11 events to `events`, stamped now and numbered on from `next_id`.
fn add_events(rng: &mut Rng, events: &mut Vec<Event>, next_id: &mut u64) {
    for _ in 0..rng.below(12) {
        events.push(Event {
            id: *next_id,
            at: millis(SystemTime::now()),
        });
        *next_id += 1;
    }
}

/// The source `kind` builds with `secrets`, resumed from `progress` where one is given.
fn open(
    kind: &SourceKind,
    secrets: &HeldSecrets,
    progress: Option<smeltr::io::Progress>,
) -> Result<Box<dyn Source>, String> {
    let registry = Arc::new(smeltr::metrics::Metrics::new());
    let subjects = Arc::new(smeltr::metrics::Subjects::default());
    let mut source = build_source(kind, &registry, &subjects, secrets, None).map_err(|e| e.message)?;
    if let Some(progress) = progress {
        source.resume(progress).map_err(|e| e.message)?;
    }
    Ok(source)
}

/// Why what `delivered` holds is not what the service answered in `pages`, or `Ok`: each page of each holder, in the
/// order the holder asked, handed out as its records, once each, by offset. A holder's last page may be cut short
/// where it failed over.
fn check_pages(pages: &[Page], delivered: &Delivered, holders: usize) -> Result<(), String> {
    for holder in 0..holders {
        let read: Vec<&Page> = pages.iter().filter(|p| p.holder == holder && p.readable).collect();
        for (partition, page) in read.iter().enumerate() {
            let got = delivered
                .get(&(holder, partition as i32))
                .map(Vec::as_slice)
                .unwrap_or_default();
            let last = partition + 1 == read.len();
            for (i, (offset, bytes)) in got.iter().enumerate() {
                if *offset != i as i64 {
                    return Err(format!(
                        "holder {holder}'s page {partition} handed offset {offset} as its {i}th"
                    ));
                }
                let Some((record, _)) = page.records.get(i) else {
                    return Err(format!(
                        "holder {holder}'s page {partition} at {} handed more than it holds",
                        page.url
                    ));
                };
                if bytes != record {
                    return Err(format!(
                        "holder {holder}'s page {partition} at {} handed {:?}, not {:?}",
                        page.url,
                        String::from_utf8_lossy(bytes),
                        String::from_utf8_lossy(record)
                    ));
                }
            }
            if got.len() < page.records.len() && !last {
                return Err(format!(
                    "holder {holder}'s page {partition} at {} handed {} of its {} records",
                    page.url,
                    got.len(),
                    page.records.len()
                ));
            }
        }
        if let Some(((_, partition), _)) = delivered
            .range((holder, read.len() as i32)..(holder + 1, i32::MIN))
            .next()
        {
            return Err(format!(
                "holder {holder} handed records of page {partition}, which it never read"
            ));
        }
    }
    Ok(())
}

/// The events delivered whole: those of every record handed out.
fn arrived(pages: &[Page], delivered: &Delivered, holders: usize) -> std::collections::BTreeSet<u64> {
    let mut ids = std::collections::BTreeSet::new();
    for holder in 0..holders {
        let read = pages.iter().filter(|p| p.holder == holder && p.readable);
        for (partition, page) in read.enumerate() {
            let got = delivered.get(&(holder, partition as i32)).map(Vec::len).unwrap_or(0);
            for (_, events) in page.records.iter().take(got) {
                ids.extend(events);
            }
        }
    }
    ids
}

/// A way a service fails one request, and the label of what it tries.
const FAILURES: &[&str] = &[
    "asked to wait for seconds",
    "asked to wait until a date",
    "a server error",
    "a redirect",
    "credentials refused",
    "a page it cannot read",
];

/// How long the source must wait after the failure of `kind`: what it asks, or the first backoff.
fn asked_wait(kind: usize) -> i64 {
    if kind <= 1 { 2000 } else { 1000 }
}

/// The one failure of `kind`, answered by a service at `base`.
fn failure(kind: usize, base: &str) -> Answer {
    // A date is read against the service's own `Date`, here a second ahead of this host's clock.
    let ahead = |secs: u64| httpdate(SystemTime::now() + Duration::from_secs(secs));
    match kind {
        0 => (429, vec![("Retry-After".into(), "2".into())], b"slow down".to_vec()),
        1 => (
            503,
            vec![("Retry-After".into(), ahead(3)), ("Date".into(), ahead(1))],
            Vec::new(),
        ),
        2 => (500, Vec::new(), b"it broke".to_vec()),
        3 => (301, vec![("Location".into(), format!("{base}/v1/moved"))], Vec::new()),
        4 => (401, Vec::new(), b"{\"error\":\"no\"}".to_vec()),
        _ => (200, Vec::new(), b"{\"data\": [1, 2".to_vec()),
    }
}

/// `at` as an HTTP date, `Sun, 06 Nov 1994 08:49:37 GMT`.
fn httpdate(at: SystemTime) -> String {
    let at = time::OffsetDateTime::from_unix_timestamp(millis(at) / 1000).unwrap();
    let day = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"][at.weekday().number_days_from_monday() as usize];
    let month = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ][at.month() as usize - 1];
    format!(
        "{day}, {:02} {month} {} {:02}:{:02}:{:02} GMT",
        at.day(),
        at.year(),
        at.hour(),
        at.minute(),
        at.second()
    )
}

/// What an HTTP API service holds and how it answers.
struct GetService {
    events: Vec<Event>,
    records: ApiRecords,
    /// Its `since` parameter, where the source names one.
    since: Option<String>,
    page_size: usize,
    /// Whether the source follows links: a service never asked for a second page answers whole.
    follow_links: bool,
    /// The address the source is given, and the headers and credentials it must send.
    url: String,
    headers: Vec<(String, String)>,
    credentials: Vec<(String, String)>,
    /// What every request must POST, where the source POSTs: the JSON body, or `None` for nothing.
    post: Option<Option<String>>,
    /// The holder the last failure was answered to, when, and how long it must wait after it. A new holder
    /// starts afresh.
    failed: Option<(usize, i64, i64)>,
    /// A failure to answer the next request with.
    fail: Option<usize>,
    /// Whether the next page links back to itself, which ends a poll.
    loop_back: bool,
    pages: Vec<Page>,
    /// Each poll's first request: its holder, when it was read, and its `since`.
    firsts: Vec<(usize, i64, Option<i64>)>,
    /// Whether the address carries a query of its own, which every request must keep.
    query: bool,
    holder: usize,
    wrong: Vec<String>,
    labels: Vec<&'static str>,
    /// How the source's app signs in, where it does.
    sign_in: Option<GetSignIn>,
}

/// How an HTTP API source's app signs in to its service, and the token the service last gave.
struct GetSignIn {
    /// Whether the source names the issuer, so its first sign-in reads the discovery document.
    issuer: bool,
    /// The app key's public half, for an app proving itself with a key; else it sends its secret.
    key: Option<Vec<u8>>,
    /// Whether the discovery document says the token endpoint takes the secret in the header.
    basic: bool,
    scope: Option<String>,
    /// The `expires_in` each token is given. 0 names none.
    life: u64,
    /// The token last given, when, and whether a request has used it.
    token: Option<(String, Instant, bool)>,
    tokens: usize,
    /// Whether the next request with a used token is refused, as a revoked one is.
    refuse_used: bool,
    /// Whether the token the last request carried had been let through before it.
    answered: bool,
}

const SECRET: &str = "s3cr&t";

impl GetService {
    fn answer(&mut self, request: &Request) -> Answer {
        let now = millis(SystemTime::now());
        let base = self.url.split_once("/v1/").unwrap().0.to_string();
        let url = format!("{base}{}", request.target);
        match request.target.as_str() {
            "/idp/.well-known/openid-configuration" => return self.discovery(&base),
            "/idp/token" => return self.token(request, &base),
            _ => {}
        }
        let method = if self.post.is_some() { "POST" } else { "GET" };
        if request.method != method || request.target.split('?').next() != Some("/v1/events") {
            self.wrong.push(format!("asked {} {}", request.method, request.target));
            return (404, Vec::new(), Vec::new());
        }
        let body = self.post.clone().flatten();
        if request.body != body.as_deref().unwrap_or_default().as_bytes() {
            let sent = String::from_utf8_lossy(&request.body);
            self.wrong
                .push(format!("{} sent the body {sent:?}, not {body:?}", request.target));
        }
        if body.is_some() && request.header("content-type") != Some("application/json") {
            self.wrong.push(format!(
                "{} sent its body as {:?}",
                request.target,
                request.header("content-type")
            ));
        }
        if self.query
            && (request.param("limit").as_deref() != Some("50") || request.param("q").as_deref() != Some("a b"))
        {
            self.wrong
                .push(format!("{} lost the address's own query", request.target));
        }
        if let Some((holder, at, wait)) = self.failed.take()
            && holder == self.holder
            && now < at + wait - 50
        {
            self.wrong.push(format!(
                "asked again {} ms after a failure that asked {wait} ms",
                now - at
            ));
        }
        for (name, value) in self.headers.iter().chain(&self.credentials) {
            if request.header(&name.to_ascii_lowercase()) != Some(value) {
                self.wrong
                    .push(format!("{} asked without {name}: {value}", request.target));
            }
        }
        if let Some(refused) = self.authorized(request) {
            return refused;
        }
        if let Some(kind) = self.fail.take() {
            self.labels.push(FAILURES[kind]);
            let answer = failure(kind, &base);
            // A token refused after it was let through once is replaced and the request sent again at once.
            let resent = answer.0 == 401 && self.sign_in.as_ref().is_some_and(|s| s.answered);
            if !resent {
                self.failed = Some((self.holder, millis(SystemTime::now()), asked_wait(kind)));
            }
            if kind == FAILURES.len() - 1 {
                self.pages.push(Page {
                    holder: self.holder,
                    url,
                    records: Vec::new(),
                    readable: false,
                });
            }
            return answer;
        }
        let page_no: usize = request.param("page").and_then(|p| p.parse().ok()).unwrap_or(0);
        if page_no > 0 && !self.follow_links {
            self.wrong
                .push(format!("followed a link to {}, though set not to", request.target));
        }
        let since = self.since.as_ref().and_then(|name| request.param(name));
        if page_no == 0 {
            let since = since.as_deref().map(|s| read_stamp(s).ok_or(s.to_string()));
            let since = match since {
                Some(Err(s)) => {
                    self.wrong.push(format!("asked since {s:?}, which is no time"));
                    None
                }
                Some(Ok(at)) => Some(at),
                None => None,
            };
            self.firsts.push((self.holder, now, since));
        }
        let from = since.as_deref().and_then(read_stamp).unwrap_or(i64::MIN);
        let held: Vec<Event> = self.events.iter().filter(|e| e.at >= from).cloned().collect();
        let size = if self.follow_links { self.page_size } else { usize::MAX };
        let on_page: Vec<&Event> = held.iter().skip(page_no * size).take(size).collect();
        let more = held.len() > (page_no + 1).saturating_mul(size);
        let mut headers = Vec::new();
        // A link back to a page already asked comes only where nothing follows: the source takes it as the end.
        let back = self.loop_back && !more;
        if more || back {
            let next = match back {
                true => {
                    self.labels.push("a link back to a page already asked");
                    self.loop_back = false;
                    request.target.clone()
                }
                false => {
                    let mut next = reqwest::Url::parse(&url).unwrap();
                    let pairs: Vec<(String, String)> = next
                        .query_pairs()
                        .filter(|(k, _)| k != "page")
                        .map(|(k, v)| (k.into(), v.into()))
                        .collect();
                    next.query_pairs_mut()
                        .clear()
                        .extend_pairs(pairs)
                        .append_pair("page", &(page_no + 1).to_string());
                    format!("{}?{}", next.path(), next.query().unwrap_or(""))
                }
            };
            // Absolute or relative, quoted or not, beside another link or alone.
            let target = match next.len() % 3 {
                0 => {
                    self.labels.push("an absolute link");
                    format!("{base}{next}")
                }
                _ => next,
            };
            let link = match (page_no + self.firsts.len()) % 3 {
                0 => format!("<{target}>; rel=\"next\""),
                1 => {
                    self.labels.push("a link beside another");
                    format!("<{base}/first>; rel=\"first\", <{target}>; rel=NEXT")
                }
                _ => {
                    self.labels.push("a link of several relations");
                    format!("<{target}>;rel=\"prev next\"")
                }
            };
            headers.push(("Link".to_string(), link));
        }
        if !self.follow_links {
            // A link the source is set not to follow.
            headers.push(("Link".to_string(), "</v1/events?page=1>; rel=\"next\"".to_string()));
        }
        let (body, records) = self.render(&on_page);
        self.pages.push(Page {
            holder: self.holder,
            url,
            records,
            readable: true,
        });
        (200, headers, body)
    }

    /// `None` when `request` carries the token last given, or the 401 the service answers it instead.
    fn authorized(&mut self, request: &Request) -> Option<Answer> {
        let sign_in = self.sign_in.as_mut()?;
        let authorization = request.header("authorization").unwrap_or_default().to_string();
        let Some((token, given, used)) = sign_in.token.clone() else {
            self.wrong
                .push(format!("asked with {authorization:?} before a token was given"));
            return Some((401, Vec::new(), Vec::new()));
        };
        if authorization != format!("Bearer {token}") {
            self.wrong
                .push(format!("asked with {authorization:?}, not Bearer {token}"));
        }
        // A token is replaced a minute before it expires: one living 61 seconds is used for one second.
        if sign_in.life == 61 && given.elapsed() > Duration::from_millis(1500) {
            self.wrong.push(format!(
                "asked with {token} {:?} after it was given, within a minute of expiring",
                given.elapsed()
            ));
        }
        if sign_in.refuse_used && used {
            sign_in.refuse_used = false;
            sign_in.token = None;
            self.labels.push("a used token refused and replaced");
            let head = vec![("WWW-Authenticate".into(), "Bearer error=\"invalid_token\"".into())];
            return Some((401, head, Vec::new()));
        }
        if let Some(t) = &mut sign_in.token {
            sign_in.answered = t.2;
            t.2 = true;
        }
        None
    }

    /// The issuer's discovery document.
    fn discovery(&mut self, base: &str) -> Answer {
        let Some(sign_in) = &self.sign_in else {
            self.wrong.push("read the discovery document with no sign-in".into());
            return (404, Vec::new(), Vec::new());
        };
        if !sign_in.issuer {
            self.wrong
                .push("read the discovery document given the token endpoint".into());
        }
        self.labels.push("the token endpoint discovered");
        let methods: &[&str] = match sign_in.basic {
            true => &["private_key_jwt", "client_secret_basic", "client_secret_post"],
            false => &["client_secret_post", "private_key_jwt"],
        };
        let body = serde_json::json!({
            "issuer": format!("{base}/idp"),
            "token_endpoint": format!("{base}/idp/token"),
            "token_endpoint_auth_methods_supported": methods,
        });
        (200, Vec::new(), body.to_string().into_bytes())
    }

    /// The token endpoint: checks the app's secret or assertion, and gives a token.
    fn token(&mut self, request: &Request, base: &str) -> Answer {
        let Some(sign_in) = &mut self.sign_in else {
            self.wrong.push("asked for a token with no sign-in".into());
            return (404, Vec::new(), Vec::new());
        };
        let form: BTreeMap<String, String> =
            reqwest::Url::parse(&format!("http://x/?{}", String::from_utf8_lossy(&request.body)))
                .unwrap()
                .query_pairs()
                .into_owned()
                .collect();
        let field = |name: &str| form.get(name).map(String::as_str);
        let mut wrong = Vec::new();
        if request.method != "POST" || field("grant_type") != Some("client_credentials") {
            wrong.push(format!("a sign-in of {} {form:?}", request.method));
        }
        if field("scope") != sign_in.scope.as_deref() {
            wrong.push(format!(
                "a sign-in asking scope {:?}, not {:?}",
                field("scope"),
                sign_in.scope
            ));
        }
        if request.header("dpop").is_some() {
            wrong.push("a sign-in offering a DPoP proof".into());
        }
        let authorization = request.header("authorization");
        match &sign_in.key {
            Some(public) => {
                let (_, claims) = crate::api_source_tests::verified(
                    field("client_assertion").unwrap_or_default(),
                    &aws_lc_rs::signature::RSA_PKCS1_2048_8192_SHA256,
                    public,
                );
                if claims["sub"] != "app" || claims["aud"] != format!("{base}/idp/token").as_str() {
                    wrong.push(format!("an assertion of {claims}"));
                }
            }
            None if sign_in.issuer && sign_in.basic => {
                self.labels.push("a secret in the header");
                let basic = format!(
                    "Basic {}",
                    smeltr::format::bytes_text::BytesForm::Base64.text(b"app:s3cr%26t")
                );
                if authorization != Some(basic.as_str()) || field("client_secret").is_some() {
                    wrong.push(format!("a basic sign-in of {authorization:?} {form:?}"));
                }
            }
            None => {
                self.labels.push("a secret in the form");
                if authorization.is_some()
                    || field("client_id") != Some("app")
                    || field("client_secret") != Some(SECRET)
                {
                    wrong.push(format!("a form sign-in of {authorization:?} {form:?}"));
                }
            }
        }
        sign_in.tokens += 1;
        if sign_in.tokens > 1 {
            self.labels.push("a token asked for again");
        }
        let token = format!("at{}", sign_in.tokens);
        sign_in.token = Some((token.clone(), Instant::now(), false));
        let body = match sign_in.life {
            0 => serde_json::json!({"access_token": token, "token_type": "Bearer"}),
            life => serde_json::json!({"access_token": token, "token_type": "Bearer", "expires_in": life}),
        };
        self.wrong.extend(wrong);
        (
            200,
            vec![("Content-Type".into(), "application/json".into())],
            body.to_string().into_bytes(),
        )
    }

    /// `events` as the source is set to cut them, and the records it must cut.
    fn render(&mut self, events: &[&Event]) -> (Vec<u8>, Cut) {
        let text = |e: &Event| TEXTS[e.id as usize % TEXTS.len()];
        match &self.records {
            ApiRecords::Body => {
                let body: String = events.iter().map(|e| format!("event {} {}\n", e.id, text(e))).collect();
                let body = if body.is_empty() && self.firsts.len().is_multiple_of(2) {
                    " \r\n".to_string()
                } else {
                    body
                };
                let ids = events.iter().map(|e| e.id).collect();
                match body.trim().is_empty() {
                    true => (body.into_bytes(), Vec::new()),
                    false => (body.clone().into_bytes(), vec![(body.into_bytes(), ids)]),
                }
            }
            ApiRecords::Lines => {
                let mut body = Vec::new();
                let mut records = Vec::new();
                for (i, e) in events.iter().enumerate() {
                    let line = format!("event {} {}", e.id, text(e));
                    if i % 4 == 3 {
                        body.extend(b"  \r\n");
                    }
                    body.extend(line.as_bytes());
                    body.extend(if e.id % 2 == 0 { &b"\r\n"[..] } else { b"\n" });
                    records.push((line.into_bytes(), vec![e.id]));
                }
                (body, records)
            }
            ApiRecords::Array(pointer) => {
                let elements: Vec<String> = (events.iter())
                    .map(|e| {
                        format!(
                            "{{\"id\": {}, \"text\": {}}}",
                            e.id,
                            serde_json::to_string(text(e)).unwrap()
                        )
                    })
                    .collect();
                let array = match elements.is_empty() && self.firsts.len().is_multiple_of(2) && !pointer.is_empty() {
                    true => {
                        self.labels.push("null where the records are");
                        "null".to_string()
                    }
                    false => format!("[{}]", elements.join(",\n ")),
                };
                let body = match pointer.as_str() {
                    "" => array,
                    "/data" => format!("{{\"total\": {}, \"data\": {array}}}", events.len()),
                    _ => format!("{{\"a/b\": {array}, \"after\": [9]}}"),
                };
                let records = elements
                    .into_iter()
                    .zip(events)
                    .map(|(e, ev)| (e.into_bytes(), vec![ev.id]))
                    .collect();
                (body.into_bytes(), records)
            }
        }
    }
}

/// An HTTP API source under any settings, reading a service that holds events added while it reads, hands out every
/// page it reads as its records once, in order, and in the end every event the service holds from its start, however
/// its sender commits, its holder fails over and the service fails.
#[test]
fn every_http_api_setting_delivers_each_page_once_and_every_event_in_the_end() {
    let mut cov = Coverage::new(
        [
            "records Body",
            "records Lines",
            "records at \"\"",
            "records at \"/data\"",
            "records at \"/a~1b\"",
        ]
        .into_iter()
        .chain(["no since", "since from everything", "since from new", "an overlap"])
        .chain(["links followed", "links not followed", "an address with a query"])
        .chain([
            "an absolute link",
            "a link beside another",
            "a link of several relations",
        ])
        .chain(["a GET", "a POST of nothing", "a POST of a body"])
        .chain([
            "no header or credential",
            "a header or credential",
            "several headers or credentials",
        ])
        .chain([
            "no sign-in",
            "a secret at a token URL",
            "a secret through an issuer",
            "a key at a token URL",
            "a key through an issuer",
            "a token with no lifetime",
            "a token about to expire",
            "a token of an hour",
            "the token endpoint discovered",
            "a secret in the header",
            "a secret in the form",
            "a token asked for again",
            "a used token refused and replaced",
        ])
        .chain(FAILURES.iter().copied())
        .chain([
            "a link back to a page already asked",
            "null where the records are",
            "a batch not yet committed",
            "a batch committed in part",
            "a failover",
            "a failover with pages not committed",
            "a page of several events",
            "a failure shown and then cleared",
        ])
        .map(String::from),
    );
    fuzz::run(1..101, |seed| {
        let mut rng = Rng::seeded(seed);
        let mut failures = failures_in_turn(seed, 4, 1, FAILURES.len());
        // Only records in an array can be unreadable.
        let unreadable = failures.contains(&(FAILURES.len() - 1));
        let records = match rng.below(5) {
            n @ (0 | 1) if unreadable => ApiRecords::Array(["", "/data"][n].to_string()),
            0 => ApiRecords::Body,
            1 => ApiRecords::Lines,
            n => ApiRecords::Array(["", "/data", "/a~1b"][n - 2].to_string()),
        };
        cov.reach(match &records {
            ApiRecords::Array(p) => format!("records at {p:?}"),
            r => format!("records {r:?}"),
        });
        let since = match rng.below(3) {
            0 => None,
            n => Some(Since {
                param: (*rng.pick(&["since", "from", "start_time"])).to_string(),
                overlap: Duration::from_millis(*rng.pick(&[0, 0, 5, 2000])),
                start: if n == 1 { ListStart::Everything } else { ListStart::New },
            }),
        };
        cov.reach(match &since {
            None => "no since",
            Some(s) if s.start == ListStart::Everything => "since from everything",
            Some(_) => "since from new",
        });
        if since.as_ref().is_some_and(|s| !s.overlap.is_zero()) {
            cov.reach("an overlap");
        }
        let (method, body) = match rng.below(3) {
            0 => (ApiMethod::Get, None),
            1 => (ApiMethod::Post, None),
            _ => (
                ApiMethod::Post,
                Some(format!(r#"{{"batchSize":{},"q":"a b"}}"#, rng.below(1000))),
            ),
        };
        cov.reach(match (method, &body) {
            (ApiMethod::Get, _) => "a GET",
            (_, None) => "a POST of nothing",
            (_, Some(_)) => "a POST of a body",
        });
        let follow_links = !rng.chance(4);
        cov.reach(if follow_links {
            "links followed"
        } else {
            "links not followed"
        });
        let query = rng.chance(2);
        if query {
            cov.reach("an address with a query");
        }

        // None, one or several of each.
        let headers: Vec<(String, String)> = (0..rng.below(3))
            .map(|i| {
                (
                    ["Accept", "X-Tenant"][i].to_string(),
                    format!("v{}-{seed}", rng.below(9)),
                )
            })
            .collect();
        // An app signing in sets Authorization itself.
        let sign_in = match rng.below(3) {
            0 => None,
            _ => {
                let key = rng.chance(3).then(crate::api_source_tests::app_key);
                Some(GetSignIn {
                    issuer: rng.chance(2),
                    basic: rng.chance(2),
                    scope: rng.chance(2).then(|| "read:events".to_string()),
                    life: *rng.pick(&[0, 61, 3600]),
                    key: key.as_ref().map(|(_, public)| public.clone()),
                    token: None,
                    tokens: 0,
                    refuse_used: rng.chance(3),
                    answered: false,
                })
                .map(|s| (s, key.map(|(pem, _)| pem)))
            }
        };
        cov.reach(match &sign_in {
            None => "no sign-in",
            Some((s, _)) => match (s.key.is_some(), s.issuer) {
                (false, false) => "a secret at a token URL",
                (false, true) => "a secret through an issuer",
                (true, false) => "a key at a token URL",
                (true, true) => "a key through an issuer",
            },
        });
        if let Some((s, _)) = &sign_in {
            cov.reach(match s.life {
                0 => "a token with no lifetime",
                61 => "a token about to expire",
                _ => "a token of an hour",
            });
        }
        let credential_names: &[&str] = match sign_in {
            Some(_) => &["X-Api-Key"],
            None => &["X-Api-Key", "Authorization"],
        };
        let credentials: Vec<(String, String)> = (0..rng.below(credential_names.len() + 1))
            .map(|i| (credential_names[i].to_string(), format!("k3y-{i}-{seed}")))
            .collect();
        cov.reach(match (headers.len(), credentials.len()) {
            (0, 0) => "no header or credential",
            (2, _) | (_, 2) => "several headers or credentials",
            _ => "a header or credential",
        });
        let state = Arc::new(Mutex::new(GetService {
            events: Vec::new(),
            records: records.clone(),
            since: since.as_ref().map(|s| s.param.clone()),
            page_size: 1 + rng.below(6),
            follow_links,
            url: String::new(),
            headers: headers.clone(),
            credentials: credentials.clone(),
            post: (method == ApiMethod::Post).then(|| body.clone()),
            failed: None,
            fail: None,
            loop_back: false,
            pages: Vec::new(),
            firsts: Vec::new(),
            query,
            holder: 0,
            wrong: Vec::new(),
            labels: Vec::new(),
            sign_in: None,
        }));
        let (sign_in, pem) = match sign_in {
            Some((s, pem)) => (Some(s), pem),
            None => (None, None),
        };
        let fuzzed_sign_in = sign_in.as_ref().map(|s| (s.issuer, s.scope.clone(), s.key.is_some()));
        state.lock().unwrap().sign_in = sign_in;
        let serving = state.clone();
        let service = Service::new(Arc::new(Mutex::new(move |r: &Request| {
            serving.lock().unwrap().answer(r)
        })));
        let app_sign_in = fuzzed_sign_in.map(|(issuer, scope, key)| AppSignIn {
            client_id: "app".into(),
            token_at: match issuer {
                true => TokenAt::Issuer(format!("{}/idp", service.url)),
                false => TokenAt::Url(format!("{}/idp/token", service.url)),
            },
            scope,
            proof: match key {
                true => AppProof::Key {
                    private_key: SecretRef::LeaderStored {
                        name: "app-key".into(),
                        version: 1,
                    },
                    key_id: None,
                },
                false => AppProof::Secret(SecretRef::LeaderStored {
                    name: "app-secret".into(),
                    version: 1,
                }),
            },
        });
        let url = format!(
            "{}/v1/events{}",
            service.url,
            if query { "?limit=50&q=a%20b" } else { "" }
        );
        state.lock().unwrap().url = url.clone();
        let kind = SourceKind::Api(ApiSourceConfig {
            api: Api::HttpApi(HttpApiConfig {
                url: url.clone(),
                method,
                body,
                headers,
                credentials: (credentials.iter().enumerate())
                    .map(|(i, (name, _))| {
                        let secret = SecretRef::LeaderStored {
                            name: format!("api{i}"),
                            version: 1,
                        };
                        (name.clone(), secret)
                    })
                    .collect(),
                records: records.clone(),
                follow_links,
                since: since.clone(),
                sign_in: app_sign_in,
            }),
            identity: None,
            poll_interval: Duration::from_millis(rng.below(80) as u64),
        });
        let mut secrets = HeldSecrets::default();
        secrets.set_stored(
            (credentials.iter().enumerate())
                .map(|(i, (_, value))| (format!("api{i}"), 1, value.clone()))
                .chain([("app-secret".to_string(), 1, SECRET.to_string())])
                .chain(pem.map(|pem| ("app-key".to_string(), 1, pem))),
        );

        let mut next_id = 0u64;
        let opened = millis(SystemTime::now());
        // When each holder opened, and the cursor it resumed from.
        let mut resumed: Vec<(i64, Option<String>)> = vec![(opened, None)];
        // Some services hold nothing when the source first asks.
        let quiet_start = rng.chance(4);
        if !quiet_start {
            add_events(&mut rng, &mut state.lock().unwrap().events, &mut next_id);
        }
        let mut source = open(&kind, &secrets, None)?;
        let mut holder = 0;
        let mut delivered = Delivered::new();
        let mut committed: BTreeMap<(usize, i32), i64> = BTreeMap::new();
        let mut failed_shown = false;
        // What the leader holds of the source's progress: each poll hands on what it has delivered since the last.
        let mut stored = None;
        for round in 0..4 + rng.below(12) {
            if round > 0 || !quiet_start {
                add_events(&mut rng, &mut state.lock().unwrap().events, &mut next_id);
            }
            if !failures.is_empty() && rng.chance(3) {
                state.lock().unwrap().fail = failures.pop();
            }
            if follow_links && rng.chance(10) {
                state.lock().unwrap().loop_back = true;
            }
            for _ in 0..1 + rng.below(4) {
                poll(&mut rng, &mut source, holder, &mut delivered, &mut committed, &mut cov)?;
                stored = source.take_progress().or(stored);
                let shown = source.connection_error();
                if shown.is_some() {
                    failed_shown = true;
                } else if failed_shown && state.lock().unwrap().pages.last().is_some_and(|p| p.readable) {
                    cov.reach("a failure shown and then cleared");
                    failed_shown = false;
                }
            }
            // A new holder resumes from what the old ones handed on.
            if rng.chance(8) {
                let read = state
                    .lock()
                    .unwrap()
                    .pages
                    .iter()
                    .filter(|p| p.holder == holder && p.readable)
                    .count();
                let whole = (0..read).all(|p| {
                    let handed = delivered.get(&(holder, p as i32)).map(Vec::len).unwrap_or(0);
                    committed
                        .get(&(holder, p as i32))
                        .is_some_and(|c| *c + 1 >= handed as i64)
                });
                cov.reach(if whole {
                    "a failover"
                } else {
                    "a failover with pages not committed"
                });
                drop(source);
                holder += 1;
                state.lock().unwrap().holder = holder;
                resumed.push((
                    millis(SystemTime::now()),
                    stored.as_ref().and_then(|p| p.cursor.clone()),
                ));
                source = open(&kind, &secrets, stored.clone())?;
            }
        }
        // Nothing more is added, and the source reads until every event is in, or long enough that one is lost.
        let deadline = Instant::now() + Duration::from_secs(10);
        let total = next_id;
        loop {
            poll(&mut rng, &mut source, holder, &mut delivered, &mut committed, &mut cov)?;
            let state = state.lock().unwrap();
            let expected = state.events.iter().filter(|e| match &since {
                Some(s) if s.start == ListStart::New => {
                    e.at >= state.firsts.first().and_then(|f| f.2).unwrap_or(opened)
                }
                _ => true,
            });
            let got = arrived(&state.pages, &delivered, holder + 1);
            let missing: Vec<u64> = expected.map(|e| e.id).filter(|id| !got.contains(id)).collect();
            if missing.is_empty() {
                break;
            }
            if Instant::now() > deadline {
                return Err(format!(
                    "{} of {total} events never arrived: {missing:?}",
                    missing.len()
                ));
            }
        }

        let state = state.lock().unwrap();
        if let Some(wrong) = state.wrong.first() {
            return Err(wrong.clone());
        }
        check_pages(&state.pages, &delivered, holder + 1)?;
        if (state.pages.iter()).any(|p| p.readable && p.records.iter().any(|(_, ids)| ids.len() > 1)) {
            cov.reach("a page of several events");
        }
        // Each holder's first poll asks from the cursor it resumed from, less the overlap; without one, from
        // everything, or from a time it fixed between opening and asking.
        if let Some(s) = &since {
            let overlap = s.overlap.as_millis() as i64;
            for (h, (at, cursor)) in resumed.iter().enumerate() {
                let Some((_, read, asked)) = state.firsts.iter().find(|f| f.0 == h) else {
                    continue;
                };
                let fine = match (cursor, s.start) {
                    (Some(cursor), _) => *asked == read_stamp(cursor).map(|c| c - overlap),
                    (None, ListStart::Everything) => asked.is_none(),
                    (None, ListStart::New) => asked.is_some_and(|a| *at <= a && a <= *read),
                };
                if !fine {
                    return Err(format!(
                        "holder {h} opened at {at} from {cursor:?} first asked from {asked:?}"
                    ));
                }
            }
        }
        // Each poll of a holder after its first asks from where the poll before it began, less the overlap: before
        // that poll's first request, and after the first request of the poll before that.
        if let Some(s) = &since {
            let overlap = s.overlap.as_millis() as i64;
            for h in 0..=holder {
                let firsts: Vec<_> = state.firsts.iter().filter(|f| f.0 == h).collect();
                for (k, (_, _, since_k)) in firsts.iter().enumerate().skip(1) {
                    let Some(since_k) = since_k else {
                        return Err(format!("holder {h}'s poll {k} asked without {}", s.param));
                    };
                    let began_by = firsts[k - 1].1;
                    let began_after = k.checked_sub(2).map_or(i64::MIN, |b| firsts[b].1);
                    if since_k + overlap > began_by || since_k + overlap < began_after {
                        return Err(format!(
                            "holder {h}'s poll {k} asked from {since_k} less {overlap}, not between {began_after} \
                             and {began_by}"
                        ));
                    }
                }
            }
        }
        for label in &state.labels {
            cov.reach(*label);
        }
        Ok(())
    });
    cov.assert_complete();
}

/// A way Okta fails one request, and the label of what it tries.
const OKTA_FAILURES: &[&str] = &[
    "rate limited until its reset",
    "refused for want of a role",
    "an Okta server error",
    "an answer that is not an array",
    "a token refused",
];

/// How a source signs in to the Okta org the fuzzer holds.
enum SignIn {
    /// An API token.
    Token(String),
    /// A service app: whether the org binds its tokens with DPoP, the key's id the app names, and how long a token
    /// lasts.
    App { dpop: bool, kid: Option<String>, life: u64 },
}

/// What an Okta org holds and how it answers.
struct OktaService {
    events: Vec<Event>,
    page_size: usize,
    org: String,
    sign_in: SignIn,
    public: Vec<u8>,
    /// A failure to answer the next request with.
    fail: Option<usize>,
    /// Whether the token endpoint asks for a nonce before it gives the next token.
    token_nonce_asked: bool,
    /// The nonce the token endpoint gave and the holder it gave it to, whose every proof must carry it from then on.
    token_nonce: Option<(usize, String)>,
    /// The nonce the API gave last and the holder it gave it to, whose every proof must carry it.
    api_nonce: Option<(usize, String)>,
    /// Whether the API asks for a new nonce on the next request.
    api_nonce_asked: bool,
    /// The token the API takes, and whether a page has been read with it.
    token: Option<(String, bool)>,
    tokens: usize,
    /// Whether the API refuses the next request with a token used before.
    refuse_used: bool,
    pages: Vec<Page>,
    /// Each poll's first request: its holder, when it was read, and its `since` where it names one.
    firsts: Vec<(usize, i64, Option<i64>)>,
    /// The holder the last failure was answered to, and when it may ask again. A new holder starts afresh.
    failed: Option<(usize, i64)>,
    holder: usize,
    wrong: Vec<String>,
    labels: Vec<&'static str>,
}

impl OktaService {
    fn answer(&mut self, request: &Request) -> Answer {
        if request.target.starts_with("/oauth2/v1/token") {
            return self.token(request);
        }
        if request.method != "GET" || !request.target.starts_with("/api/v1/logs?") {
            self.wrong.push(format!("asked {} {}", request.method, request.target));
            return (404, Vec::new(), Vec::new());
        }
        if let Some((holder, not_before)) = self.failed.take()
            && holder == self.holder
            && millis(SystemTime::now()) < not_before
        {
            self.wrong.push(format!(
                "asked again {} ms before its wait ended",
                not_before - millis(SystemTime::now())
            ));
        }
        if let Some(refused) = self.authorized(request) {
            return refused;
        }
        let mut head = Vec::new();
        if let SignIn::App { dpop: true, .. } = self.sign_in
            && self.pages.len() % 3 == 2
        {
            // The API may move its nonce on any answer.
            let nonce = format!("an{}", self.pages.len());
            self.labels.push("a nonce moved on an answer");
            head.push(("DPoP-Nonce".to_string(), nonce.clone()));
            self.api_nonce = Some((self.holder, nonce));
        }
        if let Some(kind) = self.fail.take() {
            self.labels.push(OKTA_FAILURES[kind]);
            let now = millis(SystemTime::now());
            // The reset two seconds on, and a second more after it; any other failure waits the first backoff.
            let reset = now / 1000 + 2;
            let not_before = if kind == 0 {
                reset * 1000 + 1000 - 50
            } else {
                now + 1000 - 50
            };
            self.failed = Some((self.holder, not_before));
            let failed = match kind {
                0 => (
                    429,
                    vec![("x-rate-limit-reset".into(), reset.to_string())],
                    b"{}".to_vec(),
                ),
                1 => (403, Vec::new(), br#"{"errorCode":"E0000006"}"#.to_vec()),
                2 => (500, Vec::new(), b"{}".to_vec()),
                3 => (200, Vec::new(), br#"{"errorCode":"E0000011"}"#.to_vec()),
                _ => (401, Vec::new(), br#"{"errorCode":"E0000011"}"#.to_vec()),
            };
            if kind == 3 {
                self.pages.push(Page {
                    holder: self.holder,
                    url: request.target.clone(),
                    records: Vec::new(),
                    readable: false,
                });
            }
            head.extend(failed.1);
            return (failed.0, head, failed.2);
        }
        if let Some(token) = &mut self.token {
            token.1 = true;
        }
        // The first page of a poll is the source's own; the rest are the links this org wrote.
        for (name, value) in [("sortOrder", "ASCENDING"), ("limit", "1000")] {
            if request.param("after").is_none() && request.param(name).as_deref() != Some(value) {
                self.wrong
                    .push(format!("{} asked without {name}={value}", request.target));
            }
        }
        let after = request.param("after").and_then(|a| a.parse::<u64>().ok());
        let since = request.param("since");
        let held: Vec<&Event> = match (after, &since) {
            (Some(after), _) => self.events.iter().filter(|e| e.id >= after).collect(),
            (None, since) => {
                let since = since.as_deref().map(|s| read_stamp(s).ok_or(s.to_string()));
                let since = match since {
                    Some(Err(s)) => {
                        self.wrong.push(format!("asked since {s:?}, which is no time"));
                        None
                    }
                    Some(Ok(at)) => Some(at),
                    None => None,
                };
                self.firsts.push((self.holder, millis(SystemTime::now()), since));
                self.events
                    .iter()
                    .filter(|e| e.at >= since.unwrap_or(i64::MIN))
                    .collect()
            }
        };
        let on_page: Vec<&Event> = held.into_iter().take(self.page_size).collect();
        // Every page links onward, a page of nothing new to where it was asked from.
        let next = match (on_page.last(), after) {
            (Some(e), _) => e.id + 1,
            (None, Some(after)) => after,
            (None, None) => self.events.len() as u64,
        };
        if on_page.is_empty() {
            self.labels.push("a page of nothing new");
        }
        let link = format!("/api/v1/logs?after={next}&sortOrder=ASCENDING&limit=1000");
        let link = match next % 2 {
            0 => link,
            _ => {
                self.labels.push("an absolute link");
                format!("{}{link}", self.org)
            }
        };
        let own = format!("<{}{}>; rel=\"self\"", self.org, request.target);
        match self.pages.len() % 3 {
            0 => {
                self.labels.push("next and self in one header");
                head.push(("Link".into(), format!("{own}, <{link}>; rel=\"next\"")));
            }
            1 => {
                self.labels.push("next and self apart");
                head.push(("Link".into(), own));
                head.push(("Link".into(), format!("<{link}>; rel=\"next\"")));
            }
            _ => head.push(("link".into(), format!("<{link}>; rel=\"next\""))),
        }
        let records: Vec<(Vec<u8>, Vec<u64>)> = (on_page.iter())
            .map(|e| {
                let text = serde_json::to_string(TEXTS[e.id as usize % TEXTS.len()]).unwrap();
                (
                    format!("{{\"uuid\":\"{}\",\"displayMessage\":{text}}}", e.id).into_bytes(),
                    vec![e.id],
                )
            })
            .collect();
        let body = match records.len() % 2 {
            0 => format!(
                "[{}]",
                records
                    .iter()
                    .map(|r| String::from_utf8_lossy(&r.0))
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            _ => format!(
                "[\n  {}\n]",
                records
                    .iter()
                    .map(|r| String::from_utf8_lossy(&r.0))
                    .collect::<Vec<_>>()
                    .join(",\n  ")
            ),
        };
        self.pages.push(Page {
            holder: self.holder,
            url: request.target.clone(),
            records,
            readable: true,
        });
        (200, head, body.into_bytes())
    }

    /// `None` when `request` carries what the org takes, or what the API answers it instead.
    fn authorized(&mut self, request: &Request) -> Option<Answer> {
        if request.header("accept") != Some("application/json") {
            self.wrong
                .push(format!("{} asked without Accept: application/json", request.target));
        }
        let authorization = request.header("authorization").unwrap_or_default().to_string();
        let (dpop, life) = match &self.sign_in {
            SignIn::Token(token) => {
                if authorization != format!("SSWS {token}") {
                    self.wrong.push(format!("asked with {authorization:?}, not the token"));
                }
                return None;
            }
            SignIn::App { dpop, life, .. } => (*dpop, *life),
        };
        let Some((token, used)) = self.token.clone() else {
            self.wrong
                .push(format!("asked with {authorization:?} before a token was given"));
            return Some((401, Vec::new(), Vec::new()));
        };
        if life <= 60 && used {
            self.wrong
                .push(format!("asked again with {token}, within a minute of expiring"));
        }
        let scheme = if dpop { "DPoP" } else { "Bearer" };
        if authorization != format!("{scheme} {token}") {
            self.wrong
                .push(format!("asked with {authorization:?}, not {scheme} {token}"));
        }
        if dpop {
            let claims = self.proof(request);
            if claims["htm"] != "GET" || claims["htu"] != format!("{}/api/v1/logs", self.org) {
                self.wrong
                    .push(format!("a proof for {} {}", claims["htm"], claims["htu"]));
            }
            let digest = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, token.as_bytes());
            let ath = smeltr::format::bytes_text::BytesForm::Base64.text(digest.as_ref());
            let ath = ath.trim_end_matches('=').replace('+', "-").replace('/', "_");
            if claims["ath"] != ath.as_str() {
                self.wrong
                    .push(format!("a proof for token hash {}, not {ath}", claims["ath"]));
            }
            // A new holder cannot know the nonce an old one was given, so the API asks it for one.
            let theirs = self.api_nonce.as_ref().is_none_or(|n| n.0 == self.holder);
            if claims["nonce"].as_str() != self.api_nonce.as_ref().map(|n| n.1.as_str()) && !theirs {
                self.labels.push("a new holder asked for the nonce");
                self.api_nonce_asked = true;
            } else if claims["nonce"].as_str() != self.api_nonce.as_ref().map(|n| n.1.as_str()) {
                self.wrong.push(format!(
                    "a proof with nonce {}, not {:?}",
                    claims["nonce"], self.api_nonce
                ));
            }
            if self.api_nonce_asked {
                self.api_nonce_asked = false;
                self.labels.push("the API asked for a nonce");
                let nonce = format!("an-asked{}", self.tokens);
                self.api_nonce = Some((self.holder, nonce.clone()));
                let head = vec![
                    ("WWW-Authenticate".into(), r#"DPoP error="use_dpop_nonce""#.into()),
                    ("DPoP-Nonce".into(), nonce),
                ];
                return Some((401, head, Vec::new()));
            }
        }
        if self.refuse_used && used && life > 60 {
            self.refuse_used = false;
            self.labels.push("a used token refused and replaced");
            self.token = None;
            let head = vec![("WWW-Authenticate".into(), format!("{scheme} error=\"invalid_token\""))];
            return Some((401, head, Vec::new()));
        }
        None
    }

    /// The token endpoint: checks the app's assertion and gives a token.
    fn token(&mut self, request: &Request) -> Answer {
        let SignIn::App { dpop, kid, life } = &self.sign_in else {
            self.wrong.push("a token asked for with an API token".into());
            return (404, Vec::new(), Vec::new());
        };
        let (dpop, kid, life) = (*dpop, kid.clone(), *life);
        let url = format!("{}/oauth2/v1/token", self.org);
        let form: BTreeMap<String, String> =
            reqwest::Url::parse(&format!("http://x/?{}", String::from_utf8_lossy(&request.body)))
                .unwrap()
                .query_pairs()
                .into_owned()
                .collect();
        let field = |name: &str| form.get(name).map(String::as_str).unwrap_or_default();
        if request.method != "POST" || field("grant_type") != "client_credentials" || field("scope") != "okta.logs.read"
        {
            self.wrong.push(format!("a sign-in of {} {:?}", request.method, form));
        }
        let (header, claims) = crate::api_source_tests::verified(
            field("client_assertion"),
            &aws_lc_rs::signature::RSA_PKCS1_2048_8192_SHA256,
            &self.public,
        );
        if header["kid"].as_str() != kid.as_deref() {
            self.wrong
                .push(format!("an assertion naming key {}, not {kid:?}", header["kid"]));
        }
        if claims["iss"] != "0oa-app" || claims["sub"] != "0oa-app" || claims["aud"] != url.as_str() {
            self.wrong.push(format!("an assertion of {claims}"));
        }
        if dpop {
            let proof = self.proof(request);
            if proof["htm"] != "POST" || proof["htu"] != url.as_str() {
                self.wrong
                    .push(format!("a sign-in proof for {} {}", proof["htm"], proof["htu"]));
            }
            // A new holder cannot know the nonce an old one was given, so the endpoint asks it for one.
            let theirs = self.token_nonce.as_ref().is_none_or(|n| n.0 == self.holder);
            if proof["nonce"].as_str() != self.token_nonce.as_ref().map(|n| n.1.as_str()) && !theirs {
                self.labels.push("a new holder asked for the nonce");
                self.token_nonce_asked = true;
            } else if proof["nonce"].as_str() != self.token_nonce.as_ref().map(|n| n.1.as_str()) {
                self.wrong.push(format!(
                    "a sign-in proof with nonce {}, not {:?}",
                    proof["nonce"], self.token_nonce
                ));
            }
            if self.token_nonce_asked {
                self.token_nonce_asked = false;
                self.labels.push("a token asked for a nonce");
                let nonce = format!("tn{}", self.tokens);
                self.token_nonce = Some((self.holder, nonce.clone()));
                let body = br#"{"error":"use_dpop_nonce","error_description":"Authorization server requires nonce in DPoP proof."}"#;
                return (400, vec![("DPoP-Nonce".into(), nonce)], body.to_vec());
            }
        }
        self.tokens += 1;
        if self.tokens > 1 {
            self.labels.push("a token asked for again");
        }
        let token = format!("at{}", self.tokens);
        self.token = Some((token.clone(), false));
        let kind = if dpop { "DPoP" } else { "Bearer" };
        let body = format!(
            r#"{{"token_type":"{kind}","expires_in":{life},"access_token":"{token}","scope":"okta.logs.read"}}"#
        );
        (
            200,
            vec![("Content-Type".into(), "application/json".into())],
            body.into_bytes(),
        )
    }

    /// A DPoP proof's claims, unverified: `api_source_tests` checks its signature, and the callers what it binds.
    fn proof(&mut self, request: &Request) -> serde_json::Value {
        let claims = request
            .header("dpop")
            .and_then(|jws| jws.split('.').nth(1))
            .and_then(|c| serde_json::from_slice(&crate::api_source_tests::unb64url(c)).ok());
        claims.unwrap_or_else(|| {
            self.wrong
                .push(format!("{} asked without a DPoP proof", request.target));
            serde_json::Value::Null
        })
    }
}

/// An Okta System Log source under any sign-in and start, reading an org that holds events added while it reads,
/// hands out every page it reads as its records once, in order, and every event from its start in the end: exactly
/// once where its holder never fails over, and never one a holder handed on as delivered.
#[test]
fn every_okta_setting_delivers_each_event_in_order_and_resumes_after_what_it_delivered() {
    let mut cov = Coverage::new(
        [
            "start everything",
            "start new",
            "an API token",
            "a service app with Bearer",
            "a service app with DPoP",
        ]
        .into_iter()
        .chain(["a key named", "no key named", "a token renewed before each request"])
        .chain([
            "a token asked for a nonce",
            "the API asked for a nonce",
            "a nonce moved on an answer",
        ])
        .chain(["a used token refused and replaced", "a token asked for again"])
        .chain(["next and self in one header", "next and self apart", "an absolute link"])
        .chain(OKTA_FAILURES.iter().copied())
        .chain([
            "a page of nothing new",
            "a role named for a 403",
            "a batch not yet committed",
            "a batch committed in part",
            "a holder handing on every page but its last",
            "a holder handing on pages not committed",
            "a new holder asked for the nonce",
            "a run that never failed over",
        ])
        .map(String::from),
    );
    let (pem, public) = crate::api_source_tests::app_key();
    fuzz::run(1..101, |seed| {
        let mut rng = Rng::seeded(seed);
        let start = if rng.chance(2) {
            ListStart::Everything
        } else {
            ListStart::New
        };
        cov.reach(if start == ListStart::Everything {
            "start everything"
        } else {
            "start new"
        });
        let mut failures = failures_in_turn(seed, 3, 2, OKTA_FAILURES.len());
        // An app's refused token is one it replaces, which `refuse_used` tries, so only an API token is refused as
        // a failure.
        let refused = failures.contains(&(OKTA_FAILURES.len() - 1));
        let sign_in = match rng.below(3) {
            n if n == 0 || refused => SignIn::Token(format!("00tok-{seed}")),
            n => SignIn::App {
                dpop: n == 2,
                kid: rng.chance(2).then(|| format!("k{seed}")),
                life: *rng.pick(&[3600, 30]),
            },
        };
        let app = match &sign_in {
            SignIn::Token(_) => {
                cov.reach("an API token");
                None
            }
            SignIn::App { dpop, kid, life } => {
                cov.reach(if *dpop {
                    "a service app with DPoP"
                } else {
                    "a service app with Bearer"
                });
                cov.reach(if kid.is_some() { "a key named" } else { "no key named" });
                if *life <= 60 {
                    // Within a minute of expiring, so replaced before every request.
                    cov.reach("a token renewed before each request");
                }
                Some((*dpop, kid.clone()))
            }
        };

        let state = Arc::new(Mutex::new(OktaService {
            events: Vec::new(),
            page_size: 1 + rng.below(6),
            org: String::new(),
            sign_in,
            public: public.clone(),
            fail: None,
            token_nonce_asked: app.as_ref().is_some_and(|a| a.0) && rng.chance(2),
            token_nonce: None,
            api_nonce: None,
            api_nonce_asked: false,
            token: None,
            tokens: 0,
            refuse_used: false,
            pages: Vec::new(),
            firsts: Vec::new(),
            failed: None,
            holder: 0,
            wrong: Vec::new(),
            labels: Vec::new(),
        }));
        let serving = state.clone();
        let service = Service::new(Arc::new(Mutex::new(move |r: &Request| {
            serving.lock().unwrap().answer(r)
        })));
        state.lock().unwrap().org = service.url.clone();
        let kind = SourceKind::Api(ApiSourceConfig {
            api: Api::OktaLogs(OktaLogsConfig {
                org: service.url.clone(),
                sign_in: match &app {
                    None => OktaSignIn::Token(SecretRef::LeaderStored {
                        name: "okta".into(),
                        version: 1,
                    }),
                    Some((_, kid)) => OktaSignIn::ServiceApp {
                        client_id: "0oa-app".into(),
                        private_key: SecretRef::LeaderStored {
                            name: "okta".into(),
                            version: 1,
                        },
                        key_id: kid.clone(),
                    },
                },
                start,
            }),
            identity: None,
            poll_interval: Duration::from_millis(rng.below(80) as u64),
        });
        let mut secrets = HeldSecrets::default();
        let secret = if app.is_some() {
            pem.clone()
        } else {
            format!("00tok-{seed}")
        };
        secrets.set_stored([("okta".to_string(), 1, secret)]);

        let mut next_id = 0u64;
        let opened = millis(SystemTime::now());
        add_events(&mut rng, &mut state.lock().unwrap().events, &mut next_id);
        let mut source = open(&kind, &secrets, None)?;
        let mut holder = 0;
        let mut delivered = Delivered::new();
        let mut committed: BTreeMap<(usize, i32), i64> = BTreeMap::new();
        let mut stored = None;
        // The highest event each holder handed on as delivered when it failed over.
        let mut handed_on: Vec<Option<u64>> = Vec::new();
        let step = |rng: &mut Rng,
                    source: &mut Box<dyn Source>,
                    holder: usize,
                    delivered: &mut Delivered,
                    committed: &mut BTreeMap<(usize, i32), i64>,
                    stored: &mut Option<smeltr::io::Progress>,
                    cov: &mut Coverage|
         -> Result<(), String> {
            poll(rng, source, holder, delivered, committed, cov)?;
            *stored = source.take_progress().or(stored.take());
            if let Some(shown) = source.connection_error()
                && shown.contains("(403)")
            {
                if !shown.contains("Read-only Administrator") {
                    return Err(format!("a 403 shown without the role it needs: {shown}"));
                }
                cov.reach("a role named for a 403");
            }
            Ok(())
        };
        for _ in 0..4 + rng.below(12) {
            add_events(&mut rng, &mut state.lock().unwrap().events, &mut next_id);
            {
                let mut state = state.lock().unwrap();
                if !failures.is_empty() && rng.chance(3) {
                    state.fail = failures.pop();
                }
                if app.as_ref().is_some_and(|a| a.0) && rng.chance(6) {
                    state.api_nonce_asked = true;
                }
                if app.is_some() && rng.chance(6) {
                    state.refuse_used = true;
                }
            }
            for _ in 0..1 + rng.below(4) {
                step(
                    &mut rng,
                    &mut source,
                    holder,
                    &mut delivered,
                    &mut committed,
                    &mut stored,
                    &mut cov,
                )?;
            }
            if rng.chance(8) {
                // What this holder handed on: its pages before the first not wholly committed, and never its last,
                // which the source may not have finished.
                let held = state.lock().unwrap();
                let read: Vec<&Page> = held.pages.iter().filter(|p| p.holder == holder && p.readable).collect();
                let whole = |(p, page): &(usize, &&Page)| {
                    p + 1 < read.len()
                        && committed.get(&(holder, *p as i32)).map_or(-1, |c| *c) + 1 >= page.records.len() as i64
                };
                let settled = read.iter().enumerate().take_while(|p| whole(p)).count();
                let last = read[..settled]
                    .iter()
                    .flat_map(|p| p.records.iter().flat_map(|r| r.1.iter()))
                    .max();
                handed_on.push(last.copied());
                cov.reach(if settled + 1 >= read.len() {
                    "a holder handing on every page but its last"
                } else {
                    "a holder handing on pages not committed"
                });
                drop(held);
                drop(source);
                holder += 1;
                state.lock().unwrap().holder = holder;
                source = open(&kind, &secrets, stored.clone())?;
            }
        }
        // Nothing more is added, and the source reads until every event is in, or long enough that one is lost.
        let deadline = Instant::now() + Duration::from_secs(10);
        let total = next_id;
        let from = loop {
            step(
                &mut rng,
                &mut source,
                holder,
                &mut delivered,
                &mut committed,
                &mut stored,
                &mut cov,
            )?;
            let state = state.lock().unwrap();
            let from = match start {
                ListStart::New => state.firsts.first().and_then(|f| f.2).unwrap_or(opened),
                ListStart::Everything => i64::MIN,
            };
            let got = arrived(&state.pages, &delivered, holder + 1);
            let missing: Vec<u64> = state
                .events
                .iter()
                .filter(|e| e.at >= from)
                .map(|e| e.id)
                .filter(|id| !got.contains(id))
                .collect();
            if missing.is_empty() {
                break from;
            }
            if Instant::now() > deadline {
                return Err(format!(
                    "{} of {total} events never arrived: {missing:?}",
                    missing.len()
                ));
            }
        };

        let state = state.lock().unwrap();
        if let Some(wrong) = state.wrong.first() {
            return Err(wrong.clone());
        }
        // The first poll asks from everything, or from a time it fixed between opening and asking.
        if let Some((_, read, asked)) = state.firsts.first() {
            let fine = match start {
                ListStart::Everything => asked.is_none(),
                ListStart::New => asked.is_some_and(|a| opened <= a && a <= *read),
            };
            if !fine {
                return Err(format!("opened at {opened}, first asked since {asked:?}"));
            }
        }
        check_pages(&state.pages, &delivered, holder + 1)?;
        // Each holder's events, in the order it handed them out.
        let by_holder: Vec<Vec<u64>> = (0..=holder)
            .map(|h| {
                let read = state.pages.iter().filter(|p| p.holder == h && p.readable);
                (read.enumerate())
                    .flat_map(|(p, page)| {
                        let got = delivered.get(&(h, p as i32)).map(Vec::len).unwrap_or(0);
                        page.records.iter().take(got).flat_map(|r| r.1.clone())
                    })
                    .collect()
            })
            .collect();
        for (h, ids) in by_holder.iter().enumerate() {
            if let Some(w) = ids.windows(2).find(|w| w[1] <= w[0]) {
                return Err(format!("holder {h} handed out event {} after {}", w[1], w[0]));
            }
            if let Some(on) = handed_on[..h].iter().flatten().max()
                && let Some(again) = ids.iter().find(|id| *id <= on)
            {
                return Err(format!(
                    "holder {h} handed out event {again}, though an earlier holder handed on to {on}"
                ));
            }
        }
        if holder == 0 {
            cov.reach("a run that never failed over");
            let expected: Vec<u64> = state.events.iter().filter(|e| e.at >= from).map(|e| e.id).collect();
            if by_holder[0] != expected {
                return Err(format!("handed out {:?}, not each of {expected:?} once", by_holder[0]));
            }
        }
        for label in &state.labels {
            cov.reach(*label);
        }
        Ok(())
    });
    cov.assert_complete();
}
