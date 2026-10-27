//! The API poller core: one [`Api`] kind asks, this core sends, follows pages, backs off and keeps progress.
//!
//! ```text
//!   every poll interval:  Api::first(cursor) ──► ask ──► answer ──► Api::read ──► records, cursor, next page
//!                                                 ▲                                      │
//!                                                 └──────────── next page ◄──────────────┘
//! ```
//!
//! A kind says what to ask and what an answer means; the core owns the wire, the schedule and the commit. Each page
//! is a partition of its own, so a page is done once its last record commits. The cursor a page carries becomes the
//! source's progress once that page and every page before it are done: duplicates on a failover, never lost
//! records.
//!
//! The next page is fetched only once the last one is handed out, so a source holds at most one answer.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use super::line_breaker::{self, LineBreaker};
use super::pacer::Pacer;
use super::reads::Reads;
use super::{CommitMap, IoError, Progress, ReadAt, Source, SourceRecord};
use crate::format::json_scan::Reader;
use crate::plan::topology::{ApiSourceConfig, HeldSecrets};

/// The most one answer may hold. An answer is held whole until its records are handed out.
pub const MAX_ANSWER_BYTES: u64 = 256 * 1024 * 1024;

/// How long one request may take, connecting and reading the answer included. Long enough to read an answer of
/// [`MAX_ANSWER_BYTES`] at about 2 MiB/s.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(120);

/// The longest a `Retry-After` is waited out. An API asking for longer is asked again then.
const MAX_RETRY_AFTER: Duration = Duration::from_secs(3600);

/// How much of a refusing answer an error quotes.
const QUOTED: usize = 200;

/// One API's half of a poll: what to ask, and what each answer says.
pub trait Api: Send {
    /// What a poll begun at `now` asks first, from `cursor`: the last a page carried, or what an earlier holder
    /// delivered.
    fn first(&mut self, cursor: Option<&str>, now: SystemTime) -> Result<First, String>;

    /// What the answer to `url` holds. An error is retried, from the same request.
    fn read(&mut self, url: &str, answer: Answer) -> Result<Page, String>;

    /// How long an answer asking to wait, with no `Retry-After`, asks for at `now`, by the API's own header. `now`
    /// is the answer's `Date` when it has one.
    fn wait(&self, _answer: &Answer, _now: SystemTime) -> Option<Duration> {
        None
    }

    /// What to check when the API refuses the credentials with `status`, 401 or 403, beyond what it says itself.
    fn refused_hint(&self, _status: u16) -> Option<&'static str> {
        None
    }
}

/// The first request of a poll.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct First {
    pub url: String,
    /// Where a source resumes before this poll delivers anything: a start a kind fixes before reading, so a source
    /// that fails over before its first page lands asks from the same start. `None` moves nothing.
    pub cursor: Option<String>,
    /// Where a source resumes once this whole poll is delivered, whichever page ends it. `None` moves nothing.
    pub end: Option<String>,
}

/// One answer, whatever its status.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Answer {
    pub status: u16,
    /// Every header, its name lowercased, in the answer's order.
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl From<super::http::Response> for Answer {
    fn from(r: super::http::Response) -> Answer {
        Answer {
            status: r.status,
            headers: r.headers,
            body: r.body,
        }
    }
}

impl From<Answer> for super::http::Response {
    fn from(a: Answer) -> super::http::Response {
        super::http::Response {
            status: a.status,
            headers: a.headers,
            body: a.body,
        }
    }
}

impl Answer {
    /// The first value of the header `name`, which is lowercase.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.iter().find(|(n, _)| n == name).map(|(_, v)| v.as_str())
    }
}

/// What one page holds.
#[derive(Clone, Debug, PartialEq)]
pub struct Page {
    pub records: Records,
    /// Where a source resumes once this page and every page before it are delivered. `None` moves nothing.
    pub cursor: Option<String>,
    /// The next page of this poll. `None` ends the poll.
    pub next: Option<String>,
}

/// An answer's records, cut from it one at a time as they are handed out: an answer of many small records costs
/// its own bytes and one record at a time, never a copy of each up front.
#[derive(Clone, Debug, PartialEq)]
pub struct Records {
    body: Vec<u8>,
    /// Where the next record starts.
    at: usize,
    cut: Cut,
}

#[derive(Clone, Debug, PartialEq)]
enum Cut {
    Whole,
    Lines,
    /// Each event the line breaker cuts, or each line without one.
    Broken(Option<LineBreaker>),
    /// The elements of the array whose `[` is at `at` when it starts, in the body it holds, which is valid JSON.
    Array(String),
    Done,
}

impl Records {
    /// The bytes of the answer it cuts from.
    pub fn held(&self) -> usize {
        match &self.cut {
            Cut::Array(text) => text.len(),
            _ => self.body.len(),
        }
    }

    pub fn none() -> Records {
        Records {
            body: Vec::new(),
            at: 0,
            cut: Cut::Done,
        }
    }

    /// The whole of `body` as one record, or none when it holds nothing but whitespace.
    pub fn whole(body: Vec<u8>) -> Records {
        match body.trim_ascii().is_empty() {
            true => Records::none(),
            false => Records {
                body,
                at: 0,
                cut: Cut::Whole,
            },
        }
    }

    /// Each line of `body` that holds anything, without its line ending.
    pub fn lines(body: Vec<u8>) -> Records {
        Records {
            body,
            at: 0,
            cut: Cut::Lines,
        }
    }

    /// Each event of `body` that holds more than whitespace, broken as an S2S receiver breaks a stream that ends
    /// with it: where `line_breaker` breaks it, or without one at each line end `line_breaker::next_line` finds.
    pub fn broken(body: Vec<u8>, line_breaker: Option<LineBreaker>) -> Records {
        Records {
            body,
            at: 0,
            cut: Cut::Broken(line_breaker),
        }
    }

    /// Each element of the array whose `[` is at `open` in `body`, as written. `body` must be valid JSON, which
    /// is what lets an element be cut without parsing the rest.
    pub fn array(body: Vec<u8>, open: usize) -> Records {
        match String::from_utf8(body) {
            Ok(text) => Records {
                body: Vec::new(),
                at: open + 1,
                cut: Cut::Array(text),
            },
            Err(_) => Records::none(),
        }
    }

    /// The next record, or `None` once there are no more.
    pub fn next_record(&mut self) -> Option<Vec<u8>> {
        loop {
            match self.cut {
                Cut::Done => return None,
                Cut::Whole => {
                    self.cut = Cut::Done;
                    return Some(std::mem::take(&mut self.body));
                }
                Cut::Lines => {
                    let rest = &self.body[self.at..];
                    if rest.is_empty() {
                        self.cut = Cut::Done;
                        continue;
                    }
                    let end = rest.iter().position(|&b| b == b'\n').unwrap_or(rest.len());
                    let line = &rest[..end];
                    let line = line.strip_suffix(b"\r").unwrap_or(line);
                    let record = (!line.trim_ascii().is_empty()).then(|| line.to_vec());
                    self.at += (end + 1).min(rest.len());
                    if record.is_some() {
                        return record;
                    }
                }
                Cut::Broken(Some(ref line_breaker)) => {
                    let rest = &self.body[self.at..];
                    let mut events = line_breaker.events(rest, true);
                    let record = events.next().map(|event| rest[event].to_vec());
                    self.at += events.rest();
                    if record.is_none() {
                        self.cut = Cut::Done;
                    }
                    return record;
                }
                Cut::Broken(None) => {
                    let rest = &self.body[self.at..];
                    let found = line_breaker::next_line(rest);
                    let (event, next) = match found {
                        Some((end, next)) => (&rest[..end], next),
                        None => (LineBreaker::last_event(rest), rest.len()),
                    };
                    let record = (!event.trim_ascii().is_empty()).then(|| event.to_vec());
                    self.at += next;
                    if found.is_none() {
                        self.cut = Cut::Done;
                    }
                    if record.is_some() {
                        return record;
                    }
                }
                Cut::Array(ref text) => {
                    let rest = text.get(self.at..).unwrap_or_default();
                    let skip = rest
                        .bytes()
                        .position(|b| !matches!(b, b' ' | b'\t' | b'\n' | b'\r' | b','))
                        .unwrap_or(rest.len());
                    // The body was checked as JSON whole, so an element always reads.
                    let element = match rest.as_bytes().get(skip) {
                        None | Some(b']') => None,
                        Some(_) => Reader::of_str(&rest[skip..]).raw().ok(),
                    };
                    let Some(element) = element else {
                        self.cut = Cut::Done;
                        continue;
                    };
                    let record = element.to_vec();
                    self.at += skip + element.len();
                    return Some(record);
                }
            }
        }
    }
}

/// Sends one request: a GET of an address, or a POST of the source's body to it.
pub trait Fetch: Send {
    fn ask(&mut self, url: &str) -> Result<Answer, IoError>;
}

/// How a source signs its requests in: the headers each carries, and what the API's answers say about them.
pub trait Authorize: Send {
    /// The headers that authorize a GET of `url`, signing in first over `agent`, bounded by `timeout`, when no token
    /// is held or the one held is about to expire.
    fn authorize(
        &mut self,
        agent: &ureq::Agent,
        timeout: Duration,
        url: &str,
    ) -> Result<Vec<(String, String)>, IoError>;

    /// Takes what the API's 401 says. True when the request is worth sending again.
    fn refused(&mut self, answer: &Answer) -> bool;

    /// Takes what an answer the API let through says.
    fn used(&mut self, answer: &Answer);
}

/// The real fetch: blocking requests over one agent, with the source's headers on every request, and a token when
/// the source signs in as an app.
pub struct UreqFetch {
    agent: ureq::Agent,
    headers: Vec<(String, String)>,
    /// Whether each request is a POST rather than a GET.
    post: bool,
    /// The JSON every POST carries. `None` is a POST of nothing.
    body: Option<String>,
    sign_in: Option<Box<dyn Authorize>>,
}

impl UreqFetch {
    pub fn new(config: &ureq::config::Config, headers: Vec<(String, String)>) -> Self {
        UreqFetch {
            agent: config.new_agent(),
            headers,
            post: false,
            body: None,
            sign_in: None,
        }
    }

    /// Sends every request as a POST of `body`, JSON, instead of a GET.
    pub fn posting(mut self, body: Option<String>) -> Self {
        self.post = true;
        self.body = body;
        self
    }

    /// Signs every request in with `sign_in`'s token.
    pub fn signed_in(mut self, sign_in: impl Authorize + 'static) -> Self {
        self.sign_in = Some(Box::new(sign_in));
        self
    }

    fn send(&self, url: &str, auth: &[(String, String)]) -> Result<Answer, IoError> {
        let sent = match (self.post, &self.body) {
            (false, _) => self.prepared(self.agent.get(url), auth).call(),
            (true, None) => self.prepared(self.agent.post(url), auth).send_empty(),
            (true, Some(body)) => {
                let mut req = self.agent.post(url);
                if !self.headers.iter().any(|(n, _)| n.eq_ignore_ascii_case("content-type")) {
                    req = req.content_type("application/json");
                }
                self.prepared(req, auth).send(body.as_bytes())
            }
        };
        let mut response = sent.map_err(|e| IoError::transient(format!("request failed: {e}")))?;
        let headers = response
            .headers()
            .iter()
            .filter_map(|(n, v)| Some((n.as_str().to_ascii_lowercase(), v.to_str().ok()?.to_string())))
            .collect();
        let status = response.status().as_u16();
        let body = response
            .body_mut()
            .with_config()
            .limit(MAX_ANSWER_BYTES)
            .read_to_vec()
            .map_err(|e| IoError::transient(format!("could not read the answer: {e}")))?;
        Ok(Answer { status, headers, body })
    }

    /// `req` with this fetch's limits, its headers and `auth`.
    fn prepared<B>(&self, req: ureq::RequestBuilder<B>, auth: &[(String, String)]) -> ureq::RequestBuilder<B> {
        let mut req = req
            .config()
            .timeout_global(Some(REQUEST_TIMEOUT))
            .http_status_as_error(false)
            // A redirect may lead anywhere, and the credentials go with every request.
            .max_redirects(0)
            .build();
        for (name, value) in self.headers.iter().chain(auth) {
            req = req.header(name, value);
        }
        req
    }
}

impl Fetch for UreqFetch {
    fn ask(&mut self, url: &str) -> Result<Answer, IoError> {
        let Some(mut sign_in) = self.sign_in.take() else {
            return self.send(url, &[]);
        };
        let answer = self.signed(&mut sign_in, url);
        self.sign_in = Some(sign_in);
        answer
    }
}

impl UreqFetch {
    /// A request with `sign_in`'s token. A 401 naming a nonce, and one refusing a token used before, are each asked
    /// again: the API's nonces and tokens change while it runs.
    fn signed(&self, sign_in: &mut Box<dyn Authorize>, url: &str) -> Result<Answer, IoError> {
        let mut refusals = 0;
        loop {
            let auth = sign_in.authorize(&self.agent, REQUEST_TIMEOUT, url)?;
            let answer = self.send(url, &auth)?;
            if answer.status != 401 {
                sign_in.used(&answer);
                return Ok(answer);
            }
            refusals += 1;
            if refusals > 2 || !sign_in.refused(&answer) {
                return Ok(answer);
            }
        }
    }
}

/// Opens `config` as a live source.
pub fn open(config: &ApiSourceConfig, secrets: &HeldSecrets) -> Result<Box<dyn Source>, IoError> {
    use crate::plan::topology::Api as Kind;
    let agent = super::http::agent_config(config.tls.as_ref(), secrets)?;
    match &config.api {
        Kind::HttpApi(c) => {
            let mut headers = c.headers.clone();
            headers.extend(super::http::credential_headers(
                "HTTP API source",
                &c.credentials,
                secrets,
            )?);
            let api = super::http_api::HttpApi::new(c).map_err(IoError::transient)?;
            let mut fetch = UreqFetch::new(&agent, headers);
            if c.method == crate::plan::topology::ApiMethod::Post {
                fetch = fetch.posting(c.body.clone());
            }
            if let Some(sign_in) = &c.sign_in {
                fetch = fetch.signed_in(app_sign_in("HTTP API source", sign_in, secrets)?);
            }
            Ok(Box::new(Poller::new(api, fetch, config.poll_interval)))
        }
        Kind::OktaLogs(c) => {
            use crate::plan::topology::OktaSignIn;
            let what = "Okta System Log source";
            let mut headers = vec![("Accept".to_string(), "application/json".to_string())];
            let api = super::okta::OktaLogs::new(c).map_err(IoError::transient)?;
            let fetch = match &c.sign_in {
                OktaSignIn::Token(token) => {
                    let token = super::http::credential_headers(
                        what,
                        &[("Authorization".to_string(), token.clone())],
                        secrets,
                    )?;
                    headers.push(("Authorization".to_string(), format!("SSWS {}", token[0].1)));
                    UreqFetch::new(&agent, headers)
                }
                OktaSignIn::ServiceApp {
                    client_id,
                    private_key,
                    key_id,
                } => {
                    let pem = super::held(secrets, private_key, what, "private key")?;
                    let key = super::oauth::AppKey::from_pem(&pem, key_id.clone())
                        .map_err(|e| IoError::transient(format!("{what}: {e}")))?;
                    let sign_in =
                        super::oauth::SignIn::new(c.token_url(), client_id.clone(), Some(OKTA_SCOPE.to_string()), key)
                            .map_err(|(e, _)| IoError::transient(e))?
                            .offering_dpop();
                    UreqFetch::new(&agent, headers).signed_in(sign_in)
                }
            };
            Ok(Box::new(Poller::new(api, fetch, config.poll_interval)))
        }
    }
}

/// `sign_in` with its secrets read from `secrets`.
fn app_sign_in(
    what: &str,
    sign_in: &crate::plan::topology::AppSignIn,
    secrets: &HeldSecrets,
) -> Result<super::app_sign_in::AppSignIn, IoError> {
    use super::app_sign_in::{AppSignIn, Proof};
    use crate::plan::topology::AppProof;
    let proof = match &sign_in.proof {
        AppProof::Secret(secret) => Proof::Secret(super::held(secrets, secret, what, "client secret")?),
        AppProof::Key { private_key, key_id } => Proof::Key(
            super::oauth::AppKey::from_pem(&super::held(secrets, private_key, what, "private key")?, key_id.clone())
                .map_err(|e| IoError::transient(format!("{what}: {e}")))?,
        ),
    };
    Ok(AppSignIn::new(
        sign_in.client_id.clone(),
        sign_in.token_at.clone(),
        sign_in.scope.clone(),
        proof,
    ))
}

/// The one scope an Okta service app asks for: reading the System Log.
const OKTA_SCOPE: &str = "okta.logs.read";

/// The page whose records are being handed out.
struct Current {
    records: Records,
    /// A record a batch had no room for, handed out first next time.
    held_back: Option<Vec<u8>>,
    next: Option<String>,
}

/// An [`Api`] as a [`Source`].
pub struct Poller<A, F> {
    api: A,
    fetch: F,
    interval: Duration,
    /// When the next poll may start.
    next_poll: Instant,
    /// The next request of the poll under way. `None` between polls.
    request: Option<String>,
    /// Every page the poll under way has asked, so a chain of links that comes back on itself ends it.
    asked: HashSet<String>,
    /// The cursor the poll under way leaves when it ends.
    poll_end: Option<String>,
    /// When the source may ask again after a failure, and the failure to show.
    pacer: Pacer,
    current: Option<Current>,
    /// Reads, one a page, not yet committed whole, each carrying its cursor. The current page is the newest.
    reads: Reads<Option<String>>,
    /// The last cursor any page carried: where the next poll asks from.
    cursor: Option<String>,
    /// The last cursor every destination has acknowledged, not yet taken.
    progress: Option<String>,
    blocked: bool,
}

impl<A: Api, F: Fetch> Poller<A, F> {
    pub fn new(api: A, fetch: F, interval: Duration) -> Self {
        Poller {
            api,
            fetch,
            interval,
            next_poll: Instant::now(),
            request: None,
            asked: HashSet::new(),
            poll_end: None,
            pacer: Pacer::new("API"),
            current: None,
            reads: Reads::default(),
            cursor: None,
            progress: None,
            blocked: false,
        }
    }

    /// Asks for the next page, if one is due, and makes it the current one. Waits until `deadline` at most for one
    /// to come due.
    fn step(&mut self, deadline: Instant) {
        let now = Instant::now();
        if self.pacer.retrying() {
            // A wait can outlast the warning, so the failure is told again while it is waited out.
            self.pacer.relog();
            self.pacer.wait(deadline);
            return;
        }
        if self.request.is_none() {
            if self.next_poll > now {
                std::thread::sleep(self.next_poll.min(deadline).saturating_duration_since(now));
                return;
            }
            match self.api.first(self.cursor.as_deref(), SystemTime::now()) {
                Ok(first) => {
                    if first.cursor.is_some() {
                        // A page of no records, so the cursor moves once every page before it is delivered.
                        self.cursor.clone_from(&first.cursor);
                        self.reads.carry(first.cursor);
                        self.settle();
                    }
                    self.asked.clear();
                    self.poll_end = first.end;
                    self.request = Some(first.url);
                    self.next_poll = now + self.interval;
                }
                Err(why) => return self.pacer.failed(why, None),
            }
        }
        let url = self.request.clone().expect("a request");
        let answer = match self.fetch.ask(&url) {
            Ok(answer) => answer,
            Err(e) => return self.pacer.failed(e.message, None),
        };
        match answer.status {
            200..=299 => {}
            429 | 503 => {
                let wait = self.asked_wait(&answer);
                return self
                    .pacer
                    .failed(format!("{url} answered {}: asked to wait", answer.status), wait);
            }
            300..=399 => {
                let to = answer.header("location").unwrap_or("nowhere");
                return self.pacer.failed(
                    format!("{url} answered {} to {to}; give the address it leads to", answer.status),
                    None,
                );
            }
            401 | 403 => {
                let hint = self
                    .api
                    .refused_hint(answer.status)
                    .map(|h| format!(". {h}"))
                    .unwrap_or_default();
                return self.pacer.failed(
                    format!(
                        "{url} refused the credentials ({}): {}{hint}",
                        answer.status,
                        quoted(&answer.body)
                    ),
                    None,
                );
            }
            status => {
                return self
                    .pacer
                    .failed(format!("{url} answered {status}: {}", quoted(&answer.body)), None);
            }
        }
        let page = match self.api.read(&url, answer) {
            Ok(page) => page,
            Err(why) => return self.pacer.failed(format!("{url}: {why}"), None),
        };
        self.pacer.landed();
        self.request = None;
        self.asked.insert(url.clone());
        if page.cursor.is_some() {
            self.cursor.clone_from(&page.cursor);
        }
        self.reads.open(Arc::from(url.as_str()), page.cursor);
        self.current = Some(Current {
            records: page.records,
            held_back: None,
            next: page.next,
        });
    }

    /// The current page's next record, with its offset, partition and topic, or `None` once it has no more, which ends
    /// the page.
    fn next_record(&mut self) -> Option<(Vec<u8>, ReadAt)> {
        let current = self.current.as_mut()?;
        match current.held_back.take().or_else(|| current.records.next_record()) {
            Some(raw) => {
                let (offset, partition, topic) = self.reads.hand_out();
                Some((raw, (offset, partition, topic, None)))
            }
            None => {
                self.reads.end();
                let current = self.current.take().expect("a current page");
                // A page of nothing new, or a link to a page this poll already asked, ends the poll: an API that
                // always links onward would otherwise be asked without pause.
                self.request = current
                    .next
                    .filter(|next| self.reads.handed_out() > 0 && !self.asked.contains(next));
                if self.request.is_none()
                    && let Some(end) = self.poll_end.take()
                {
                    let cursor = self.reads.carries_mut();
                    cursor.get_or_insert(end);
                    self.cursor.clone_from(cursor);
                }
                self.settle();
                None
            }
        }
    }

    /// How long an answer asking to wait asks for: its `Retry-After`, or the kind's own header.
    fn asked_wait(&self, answer: &Answer) -> Option<Duration> {
        // A date the API names is on its clock, so the wait is measured from its `Date`, not from ours.
        let now = answer
            .header("date")
            .and_then(http_date)
            .unwrap_or_else(SystemTime::now);
        answer
            .header("retry-after")
            .and_then(|v| retry_after(v, now))
            .or_else(|| self.api.wait(answer, now).map(|w| w.min(MAX_RETRY_AFTER)))
    }

    /// Retires the oldest pages whose every record is committed, and takes the cursor the last of them carried.
    fn settle(&mut self) {
        for cursor in self.reads.settle() {
            if cursor.is_some() {
                self.progress = cursor;
            }
        }
    }
}

impl<A: Api, F: Fetch> Source for Poller<A, F> {
    fn poll_batch(
        &mut self,
        max_rows: usize,
        max_bytes: usize,
        timeout: Duration,
    ) -> Result<Vec<SourceRecord>, IoError> {
        if self.blocked {
            std::thread::sleep(timeout);
            return Ok(Vec::new());
        }
        let deadline = Instant::now() + timeout;
        let mut out = crate::io::RecordChunkBuilder::new();
        let mut bytes = 0;
        while out.len() < max_rows {
            if self.current.is_none() {
                if !out.is_empty() || Instant::now() >= deadline {
                    break;
                }
                self.step(deadline);
                continue;
            }
            let Some((raw, (offset, partition, topic, headers))) = self.next_record() else {
                continue;
            };
            if !out.is_empty() && bytes + raw.len() > max_bytes {
                self.reads.hand_back();
                self.current.as_mut().expect("a current page").held_back = Some(raw);
                break;
            }
            bytes += raw.len();
            out.push(&raw, offset, partition, topic, headers);
        }
        Ok(out.finish())
    }

    fn commit(&mut self, offsets: &CommitMap) -> Result<(), IoError> {
        self.reads.commit(offsets);
        self.settle();
        Ok(())
    }

    fn block(&mut self) -> Result<(), IoError> {
        self.blocked = true;
        Ok(())
    }

    fn release(&mut self) -> Result<(), IoError> {
        self.blocked = false;
        Ok(())
    }

    fn connection_error(&self) -> Option<String> {
        self.pacer.error().map(str::to_string)
    }

    /// The page being handed out, which is held whole until its last record goes, and the record it held back.
    fn held_bytes(&self) -> Option<u64> {
        let held = self
            .current
            .as_ref()
            .map_or(0, |c| c.records.held() + c.held_back.as_ref().map_or(0, Vec::len));
        Some(held as u64)
    }

    fn take_progress(&mut self) -> Option<Progress> {
        self.progress.take().map(|cursor| Progress {
            cursor: Some(cursor),
            ..Progress::default()
        })
    }

    fn resume(&mut self, from: Progress) -> Result<(), IoError> {
        self.cursor = from.cursor;
        Ok(())
    }
}

/// How long a `Retry-After` asks to wait, at `now`: seconds, or an HTTP date.
pub(crate) fn retry_after(value: &str, now: SystemTime) -> Option<Duration> {
    let value = value.trim();
    let wait = match value.parse::<u64>() {
        Ok(secs) => Duration::from_secs(secs),
        Err(_) => http_date(value)?.duration_since(now).unwrap_or_default(),
    };
    Some(wait.min(MAX_RETRY_AFTER))
}

/// An HTTP date as RFC 9110 writes it, `Sun, 06 Nov 1994 08:49:37 GMT`. The obsolete forms are not read.
pub(crate) fn http_date(text: &str) -> Option<SystemTime> {
    at_micros(crate::ops::timeparse::parse_with_format(
        text,
        "%a, %d %b %Y %H:%M:%S GMT",
    )?)
}

/// The instant `micros` after the epoch. `None` before it.
pub fn at_micros(micros: i64) -> Option<SystemTime> {
    Some(SystemTime::UNIX_EPOCH + Duration::from_micros(u64::try_from(micros).ok()?))
}

/// The start of `body`, as text, for an error.
fn quoted(body: &[u8]) -> String {
    let text = String::from_utf8_lossy(&body[..body.len().min(QUOTED)]);
    let text = text.trim();
    if body.len() > QUOTED {
        format!("{text}...")
    } else {
        text.to_string()
    }
}

/// The URL of `answer`'s `Link: rel="next"`, resolved against `url`, the address it answered. Refused when it leaves
/// `url`'s scheme, host and port, since the source's credentials go with every request.
pub fn next_link(url: &str, answer: &Answer) -> Result<Option<String>, String> {
    let Some(next) = answer
        .headers
        .iter()
        .filter(|(n, _)| n == "link")
        .flat_map(|(_, v)| links(v))
        .find(|(_, rels)| rels.split_ascii_whitespace().any(|r| r.eq_ignore_ascii_case("next")))
        .map(|(target, _)| target)
    else {
        return Ok(None);
    };
    let base = reqwest::Url::parse(url).map_err(|e| format!("{url} is not a URL: {e}"))?;
    let resolved = base
        .join(&next)
        .map_err(|e| format!("the next page's link {next:?} is not a URL: {e}"))?;
    if resolved.origin() != base.origin() {
        return Err(format!(
            "the next page's link leads to {}, away from {}; the credentials are sent only there",
            resolved.origin().ascii_serialization(),
            base.origin().ascii_serialization()
        ));
    }
    Ok(Some(resolved.to_string()))
}

/// Each link of one `Link` header: its target and its `rel`, unquoted.
fn links(header: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let mut rest = header;
    while let Some(open) = rest.find('<') {
        let Some(close) = rest[open..].find('>') else { break };
        let target = rest[open + 1..open + close].trim().to_string();
        rest = &rest[open + close + 1..];
        // The parameters run to the next link, which starts at a `,` before a `<`.
        let end = rest
            .find(",")
            .filter(|&c| rest[c..].trim_start_matches([',', ' ']).starts_with('<'));
        let end = end.or_else(|| rest.find('<')).unwrap_or(rest.len());
        let rel = rest[..end]
            .split(';')
            .filter_map(|p| p.split_once('='))
            .find(|(k, _)| k.trim().eq_ignore_ascii_case("rel"))
            .map(|(_, v)| v.trim().trim_matches('"').to_string())
            .unwrap_or_default();
        out.push((target, rel));
        rest = &rest[end..];
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::TopicPartition;
    use std::collections::VecDeque;
    use std::sync::Mutex;

    /// Answers each GET from a script, and records the address asked.
    struct Scripted {
        answers: VecDeque<Result<Answer, IoError>>,
        asked: Arc<Mutex<Vec<String>>>,
    }

    impl Fetch for Scripted {
        fn ask(&mut self, url: &str) -> Result<Answer, IoError> {
            self.asked.lock().unwrap().push(url.to_string());
            self.answers.pop_front().unwrap_or_else(|| Ok(ok("")))
        }
    }

    fn ok(body: &str) -> Answer {
        Answer {
            status: 200,
            headers: Vec::new(),
            body: body.as_bytes().to_vec(),
        }
    }

    /// One record per line; the cursor is the page's own address, and a page links to `next=` in its body.
    struct Lines;

    impl Api for Lines {
        fn first(&mut self, cursor: Option<&str>, _now: SystemTime) -> Result<First, String> {
            Ok(First {
                url: format!("http://api/first?from={}", cursor.unwrap_or("start")),
                ..First::default()
            })
        }
        fn read(&mut self, url: &str, answer: Answer) -> Result<Page, String> {
            let body = String::from_utf8(answer.body).unwrap();
            if body == "bad" {
                return Err("unreadable".into());
            }
            let mut records = String::new();
            let mut next = None;
            for line in body.lines() {
                match line.strip_prefix("next=") {
                    Some(n) => next = Some(n.to_string()),
                    None => records += &format!("{line}\n"),
                }
            }
            Ok(Page {
                records: Records::lines(records.into_bytes()),
                cursor: Some(url.to_string()),
                next,
            })
        }
    }

    fn poller(answers: Vec<Result<Answer, IoError>>) -> (Poller<Lines, Scripted>, Arc<Mutex<Vec<String>>>) {
        let asked = Arc::new(Mutex::new(Vec::new()));
        let fetch = Scripted {
            answers: answers.into(),
            asked: asked.clone(),
        };
        (Poller::new(Lines, fetch, Duration::from_secs(3600)), asked)
    }

    /// A poll whose deadline passes before its first step returns empty, so a deadline a busy machine can overrun
    /// in one preemption would make a page arrive a poll late.
    fn poll<A: Api>(p: &mut Poller<A, Scripted>) -> Vec<SourceRecord> {
        p.poll_batch(100, 1 << 20, Duration::from_millis(200)).unwrap()
    }

    fn commit<A: Api>(p: &mut Poller<A, Scripted>, records: &[SourceRecord]) {
        let mut map = CommitMap::new();
        for r in records {
            let at = TopicPartition {
                topic: r.topic.clone(),
                partition: r.partition,
            };
            let o = map.entry(at).or_insert(r.offset);
            *o = (*o).max(r.offset);
        }
        p.commit(&map).unwrap();
    }

    #[test]
    fn a_poll_follows_its_pages_and_progress_waits_for_every_page_before() {
        let (mut p, asked) = poller(vec![Ok(ok("a\nb\nnext=http://api/2")), Ok(ok("c"))]);
        let first = poll(&mut p);
        assert_eq!(
            first.iter().map(|r| r.bytes().unwrap()).collect::<Vec<_>>(),
            [b"a", b"b"]
        );
        let second = poll(&mut p);
        assert_eq!(second[0].bytes().unwrap(), b"c");
        assert_eq!(*asked.lock().unwrap(), ["http://api/first?from=start", "http://api/2"]);

        // The second page commits first: nothing moves until the first does.
        commit(&mut p, &second);
        assert_eq!(p.take_progress(), None);
        commit(&mut p, &first[..1]);
        assert_eq!(p.take_progress(), None);
        commit(&mut p, &first);
        assert_eq!(
            p.take_progress().and_then(|p| p.cursor).as_deref(),
            Some("http://api/2")
        );
        assert_eq!(p.take_progress(), None);
    }

    #[test]
    fn an_empty_page_moves_progress_once_those_before_it_commit() {
        let (mut p, _) = poller(vec![Ok(ok("a\nnext=http://api/2")), Ok(ok(""))]);
        let first = poll(&mut p);
        assert!(poll(&mut p).is_empty());
        assert_eq!(p.take_progress(), None);
        commit(&mut p, &first);
        assert_eq!(
            p.take_progress().and_then(|p| p.cursor).as_deref(),
            Some("http://api/2")
        );
    }

    #[test]
    fn a_poll_asks_from_the_resumed_cursor_and_then_from_the_last_read() {
        let (mut p, asked) = poller(vec![Ok(ok("a"))]);
        p.resume(Progress {
            cursor: Some("C".into()),
            ..Progress::default()
        })
        .unwrap();
        poll(&mut p);
        p.next_poll = Instant::now();
        poll(&mut p);
        assert_eq!(
            *asked.lock().unwrap(),
            [
                "http://api/first?from=C",
                "http://api/first?from=http://api/first?from=C"
            ]
        );
    }

    #[test]
    fn a_refusal_is_shown_and_the_same_request_asked_again_after_a_wait() {
        let refused = Answer {
            status: 429,
            headers: vec![("retry-after".into(), "60".into())],
            body: Vec::new(),
        };
        let unauthorized = Answer {
            status: 401,
            headers: Vec::new(),
            body: b"bad token".to_vec(),
        };
        let (mut p, asked) = poller(vec![
            Ok(ok("a\nnext=http://api/2")),
            Ok(refused),
            Ok(unauthorized),
            Ok(ok("bad")),
            Err(IoError::transient("connection refused")),
            Ok(ok("b")),
        ]);
        assert_eq!(poll(&mut p).len(), 1);
        assert!(poll(&mut p).is_empty());
        assert!(p.connection_error().unwrap().contains("429"));
        p.pacer.retry_now();
        assert!(poll(&mut p).is_empty());
        assert!(
            p.connection_error()
                .unwrap()
                .contains("refused the credentials (401): bad token")
        );
        p.pacer.retry_now();
        assert!(poll(&mut p).is_empty());
        assert!(p.connection_error().unwrap().contains("unreadable"));
        p.pacer.retry_now();
        assert!(poll(&mut p).is_empty());
        assert!(p.connection_error().unwrap().contains("connection refused"));
        p.pacer.retry_now();
        assert_eq!(poll(&mut p)[0].bytes().unwrap(), b"b");
        assert_eq!(p.connection_error(), None);
        assert!(asked.lock().unwrap()[1..].iter().all(|u| u == "http://api/2"));
    }

    /// A kind that always links onward, to `x` appended or to the address its answer names; fixes its start
    /// before reading, and leaves `END` when a poll ends.
    struct Onward;

    impl Api for Onward {
        fn first(&mut self, cursor: Option<&str>, _now: SystemTime) -> Result<First, String> {
            Ok(First {
                url: "http://api/1".into(),
                cursor: cursor.is_none().then(|| "T0".to_string()),
                end: Some("END".into()),
            })
        }
        fn read(&mut self, url: &str, answer: Answer) -> Result<Page, String> {
            let next = match answer.header("link") {
                Some(to) => to.to_string(),
                None => format!("{url}x"),
            };
            Ok(Page {
                records: Records::lines(answer.body),
                cursor: None,
                next: Some(next),
            })
        }
    }

    fn onward(answers: Vec<Answer>) -> (Poller<Onward, Scripted>, Arc<Mutex<Vec<String>>>) {
        let asked = Arc::new(Mutex::new(Vec::new()));
        let fetch = Scripted {
            answers: answers.into_iter().map(Ok).collect(),
            asked: asked.clone(),
        };
        (Poller::new(Onward, fetch, Duration::from_secs(3600)), asked)
    }

    #[test]
    fn an_empty_page_ends_a_poll_that_links_onward() {
        let (mut p, asked) = onward(vec![ok("a"), ok(""), ok("never")]);
        assert_eq!(p.poll_batch(100, 1 << 20, Duration::from_millis(10)).unwrap().len(), 1);
        assert!(
            p.poll_batch(100, 1 << 20, Duration::from_millis(10))
                .unwrap()
                .is_empty()
        );
        assert!(
            p.poll_batch(100, 1 << 20, Duration::from_millis(10))
                .unwrap()
                .is_empty()
        );
        assert_eq!(*asked.lock().unwrap(), ["http://api/1", "http://api/1x"]);
    }

    #[test]
    fn a_start_fixed_before_reading_is_progress_at_once() {
        let (mut p, _) = onward(Vec::new());
        p.fetch.answers.push_back(Err(IoError::transient("down")));
        p.poll_batch(100, 1 << 20, Duration::from_millis(10)).unwrap();
        assert_eq!(p.take_progress().and_then(|p| p.cursor).as_deref(), Some("T0"));
    }

    #[test]
    fn a_link_back_to_a_page_already_asked_ends_the_poll_and_leaves_its_end() {
        let back = |to: &str, body: &str| Answer {
            headers: vec![("link".into(), to.into())],
            ..ok(body)
        };
        let (mut p, asked) = onward(vec![back("http://api/2", "a"), back("http://api/1", "b"), ok("never")]);
        let first = poll(&mut p);
        let second = poll(&mut p);
        assert!(poll(&mut p).is_empty());
        assert_eq!(*asked.lock().unwrap(), ["http://api/1", "http://api/2"]);
        p.take_progress();
        commit(&mut p, &first);
        assert_eq!(p.take_progress(), None, "the poll is not delivered whole");
        commit(&mut p, &second);
        assert_eq!(p.take_progress().and_then(|p| p.cursor).as_deref(), Some("END"));
    }

    #[test]
    fn a_retry_after_is_seconds_or_a_date() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_445_412_480);
        assert_eq!(retry_after("7", now), Some(Duration::from_secs(7)));
        assert_eq!(retry_after("99999", now), Some(MAX_RETRY_AFTER));
        assert_eq!(
            retry_after("Wed, 21 Oct 2015 07:28:30 GMT", now),
            Some(Duration::from_secs(30))
        );
        assert_eq!(retry_after("Wed, 21 Oct 2015 07:27:00 GMT", now), Some(Duration::ZERO));
        assert_eq!(retry_after("Wednesday, 21-Oct-15 07:28:00 GMT", now), None);
        assert_eq!(retry_after("soon", now), None);
    }

    fn all(mut records: Records) -> Vec<String> {
        std::iter::from_fn(|| records.next_record())
            .map(|r| String::from_utf8(r).unwrap())
            .collect()
    }

    #[test]
    fn a_wait_is_measured_from_the_apis_own_clock() {
        let (p, _) = poller(Vec::new());
        let asked = |headers: &[(&str, &str)]| {
            p.asked_wait(&Answer {
                status: 429,
                headers: headers.iter().map(|(n, v)| (n.to_string(), v.to_string())).collect(),
                body: Vec::new(),
            })
        };
        let date = ("date", "Wed, 21 Oct 2015 07:28:00 GMT");
        assert_eq!(
            asked(&[date, ("retry-after", "Wed, 21 Oct 2015 07:28:45 GMT")]),
            Some(Duration::from_secs(45))
        );
        assert_eq!(asked(&[date, ("retry-after", "5")]), Some(Duration::from_secs(5)));
        assert_eq!(asked(&[date]), None);
    }

    #[test]
    fn records_are_cut_one_at_a_time_as_written() {
        assert_eq!(all(Records::whole(b" {} ".to_vec())), [" {} "]);
        assert!(all(Records::whole(b" \n".to_vec())).is_empty());
        assert_eq!(all(Records::lines(b"a\r\n\n \nb".to_vec())), ["a", "b"]);
        let body = br#"{"x": [ {"b":2, "a":[1,"]"]} ,"x", 3 ], "y": []}"#.to_vec();
        let open = body.iter().position(|&b| b == b'[').unwrap();
        assert_eq!(
            all(Records::array(body.clone(), open)),
            [r#"{"b":2, "a":[1,"]"]}"#, r#""x""#, "3"]
        );
        let empty = body.iter().rposition(|&b| b == b'[').unwrap();
        assert!(all(Records::array(body, empty)).is_empty());
        assert!(all(Records::none()).is_empty());
    }

    #[test]
    fn records_hold_their_answer_whatever_they_are_cut_by() {
        let body = br#"{"x": [1, 2]}"#.to_vec();
        let open = body.iter().position(|&b| b == b'[').unwrap();
        assert_eq!(Records::lines(body.clone()).held(), body.len());
        assert_eq!(Records::array(body.clone(), open).held(), body.len());
    }

    #[test]
    fn a_batch_keeps_to_its_limits() {
        let (mut p, _) = poller(vec![Ok(ok("aaaa\nbbbb\ncccc"))]);
        let batch = p.poll_batch(100, 6, Duration::from_millis(10)).unwrap();
        assert_eq!(batch.len(), 1);
        let batch = p.poll_batch(1, 1 << 20, Duration::from_millis(10)).unwrap();
        assert_eq!(batch.len(), 1);
        assert_eq!(poll(&mut p).len(), 1);
    }

    #[test]
    fn a_poller_holds_the_page_until_its_last_record_goes() {
        let (mut p, _) = poller(vec![Ok(ok("aaaa\nbbbb\ncccc"))]);
        assert_eq!(p.held_bytes(), Some(0));
        p.poll_batch(100, 6, Duration::from_millis(10)).unwrap();
        // `Lines` ends every line it keeps.
        let page = "aaaa\nbbbb\ncccc\n".len() as u64;
        assert_eq!(
            p.held_bytes(),
            Some(page + "bbbb".len() as u64),
            "the page, and the record it held back"
        );
        p.poll_batch(100, 1 << 20, Duration::from_millis(10)).unwrap();
        assert_eq!(p.held_bytes(), Some(0));
    }

    #[test]
    fn a_blocked_poller_asks_nothing() {
        let (mut p, asked) = poller(vec![Ok(ok("a"))]);
        p.block().unwrap();
        assert!(poll(&mut p).is_empty());
        assert!(asked.lock().unwrap().is_empty());
        p.release().unwrap();
        assert_eq!(poll(&mut p).len(), 1);
    }

    fn linked(link: &str) -> Answer {
        Answer {
            status: 200,
            headers: vec![("link".into(), link.into())],
            body: Vec::new(),
        }
    }

    #[test]
    fn the_next_link_is_found_among_others_and_resolved() {
        let a = linked(r#"<https://api.x/e?page=1>; rel="prev", <https://api.x/e?page=3>; rel="next""#);
        assert_eq!(
            next_link("https://api.x/e?page=2", &a).unwrap().as_deref(),
            Some("https://api.x/e?page=3")
        );
        let relative = linked(r#"</e?after=abc,def>; rel=next"#);
        assert_eq!(
            next_link("https://api.x/e", &relative).unwrap().as_deref(),
            Some("https://api.x/e?after=abc,def")
        );
        assert_eq!(
            next_link("https://api.x/e", &linked(r#"<https://api.x/self>; rel="self""#)).unwrap(),
            None
        );
        assert_eq!(next_link("https://api.x/e", &ok("")).unwrap(), None);
    }

    #[test]
    fn a_next_link_to_another_host_is_refused() {
        let away = linked(r#"<https://elsewhere.x/e?page=2>; rel="next""#);
        assert!(
            next_link("https://api.x/e", &away)
                .unwrap_err()
                .contains("https://elsewhere.x")
        );
        let downgraded = linked(r#"<http://api.x/e?page=2>; rel="next""#);
        assert!(next_link("https://api.x/e", &downgraded).is_err());
    }
}
