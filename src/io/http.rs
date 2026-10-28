//! The HTTP destination core: rows framed into request bodies by a [`Protocol`], posted over blocking HTTP (`ureq`),
//! pipelined through `io::pipelined::Pool`. Elasticsearch is one protocol over it.
//!
//! An answer may refuse some items and accept the rest, so the answer is the delivery receipt, not the status line.
//! A retryable item is re-sent alone with backoff, or after the answer's `Retry-After` where longer; a permanent one
//! is a rejection, since retrying it would stall the stream.
//!
//! `stall_after` bounds total time -- connect, request, every retry -- because it must stay under the Kafka
//! consumer's `max.poll.interval.ms`.

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use super::pipelined::{self, Pool};
use super::{BatchCompletion, BatchId, Destination, Dispatched, IoError, OwnedRecord, Rejection};
use crate::core::id::DestId;
use crate::metrics::MetricsHandle;
use crate::plan::topology::{HeldSecrets, Tls};

/// Total time one `flush` may spend, retries included.
const DEFAULT_STALL_AFTER: Duration = Duration::from_secs(30);

/// How large a request may get before `deliver` posts it and starts the next.
const DEFAULT_MAX_REQUEST_BYTES: usize = 5 * 1024 * 1024;

const INITIAL_BACKOFF: Duration = Duration::from_millis(200);

/// What one answer says about the items it answered.
#[derive(Debug, Default, PartialEq)]
pub struct Outcome {
    /// Indices into the request's item list to send again.
    pub retryable: Vec<usize>,
    /// Indices that will never succeed, with the reason.
    pub permanent: Vec<(usize, String)>,
    /// The service refused the request as a whole, for this reason, without naming an item. The request is split
    /// until each part lands or is one item, which is then refused for it.
    pub split: Option<String>,
    /// The service asked to be sent less. The retries back off, and the pool's loop hears it.
    pub throttled: bool,
}

impl Outcome {
    pub fn is_clean(&self) -> bool {
        self.retryable.is_empty() && self.permanent.is_empty() && self.split.is_none()
    }
}

/// What a store took and said it did not store as sent: counted in `smeltr_destination_unwritten_total` by the store's own
/// unit, and warned of at most once each [`crate::logs::RELOG_EVERY`], since a store may say it of every document.
/// Clones share the warning's clock, so a destination's senders warn once between them.
#[derive(Clone)]
pub struct Unwritten {
    metrics: MetricsHandle,
    destination: String,
    warned: Arc<Mutex<Option<Instant>>>,
}

impl std::fmt::Debug for Unwritten {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Unwritten")
            .field("destination", &self.destination)
            .finish()
    }
}

impl Unwritten {
    pub fn new(metrics: MetricsHandle, id: DestId) -> Unwritten {
        Unwritten {
            metrics,
            destination: id.to_string(),
            warned: Arc::default(),
        }
    }

    /// Counts `count` of `kind` unwritten, and warns, saying `why`, when no warning of this destination's is recent.
    pub fn note(&self, kind: &str, count: u64, why: &str) {
        if count == 0 {
            return;
        }
        let labels = [("destination", self.destination.as_str()), ("kind", kind)];
        self.metrics.incr("smeltr_destination_unwritten_total", &labels, count);
        let mut warned = self.warned.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if warned.is_some_and(|at| at.elapsed() < crate::logs::RELOG_EVERY) {
            return;
        }
        *warned = Some(Instant::now());
        tracing::warn!(
            target: "egress.destination_unwritten",
            subject = %self.destination,
            kind,
            count,
            "a destination's store took what it was sent but did not store all of it as sent: {why}"
        );
    }
}

/// The most of an answer a transport holds. An answer is held whole until the protocol has read it, and a sender
/// holds one at a time, so this times the senders is what a service can make a destination hold. Past it the answer
/// is [`IoError::oversized`], and `deliver` halves the request, since half the request draws about half the answer.
///
/// Sized for the one answer that grows with the request: a bulk index answers a few hundred bytes per document, so
/// this holds the answer to any bulk an operator is likely to set, and only a misbehaving service draws it in full.
pub const MAX_ANSWER_BYTES: u64 = 50 * 1024 * 1024;

/// An answer, whatever its status: a protocol decides what a status means.
#[derive(Clone, Debug, PartialEq)]
pub struct Response {
    pub status: u16,
    /// Each header's name, lowercase, and value, in the order they came.
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Response {
    /// The value of the first header named `name`, which is lowercase.
    pub fn header(&self, name: &str) -> Option<&str> {
        (self.headers.iter()).find(|(n, _)| n == name).map(|(_, v)| v.as_str())
    }

    /// How long the answer's `Retry-After` asks to wait, a date measured from the answer's own `Date`.
    pub fn asked_wait(&self) -> Option<Duration> {
        let now = (self.header("date"))
            .and_then(super::api::http_date)
            .unwrap_or_else(std::time::SystemTime::now);
        super::api::retry_after(self.header("retry-after")?, now)
    }

    /// The status and the start of the body, for an error an operator reads.
    pub fn describe(&self) -> String {
        let start: String = String::from_utf8_lossy(&self.body).chars().take(400).collect();
        format!("HTTP {}: {start}", self.status)
    }
}

/// Writes `s` as a JSON string. A value that can come from a pipeline expression (`__destination.topic`) is escaped as data.
pub fn push_json_string(out: &mut Vec<u8>, s: &str) {
    crate::format::json::write_json_string(s.as_bytes(), out);
}

/// `value` as a query parameter's or form field's value, or as one path segment: every byte but RFC 3986's
/// unreserved ones percent-encoded.
pub fn percent_encode(value: &str) -> String {
    value
        .bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => (b as char).to_string(),
            _ => format!("%{b:02X}"),
        })
        .collect()
}

/// How two items compare in the order a service must receive them. See [`Protocol::order`].
pub type Order<I> = fn(&I, &I) -> std::cmp::Ordering;

/// What a service's requests and answers look like. `Clone` because every pool sender holds its own.
pub trait Protocol: Clone + Send + 'static {
    /// What one row becomes, kept until the service confirms it: a retry is rebuilt from the items that failed.
    type Item: Clone + Send + 'static;

    /// Names the service in errors and refusals.
    fn name(&self) -> &'static str;

    /// Where a request goes, under the destination's URL.
    fn path(&self) -> &str;

    /// Where `item`'s request goes. Items with different routes never share a request.
    fn route<'a>(&'a self, _item: &'a Self::Item) -> &'a str {
        self.path()
    }

    /// Which body `item` can share: items of different parts never share a request, though they go to one route.
    fn part(&self, _item: &Self::Item) -> usize {
        0
    }

    /// The order a batch's items must reach the service in, when it has one. `Some` holds a batch's items until all
    /// are admitted and sorts each group by it before cutting requests, so no request carries an item that sorts
    /// before one already sent. A pool sends a batch's requests side by side, so a destination whose protocol has an
    /// order has none. `None` cuts a request as soon as it fills.
    fn order(&self) -> Option<Order<Self::Item>> {
        None
    }

    /// The item for `row`, or why the service could never take it.
    fn admit(&self, row: &OwnedRecord) -> Result<Self::Item, String>;

    /// What `item` adds to a request body, counted against the request size limit.
    fn size(&self, item: &Self::Item) -> usize;

    fn body(&self, items: &[Self::Item]) -> Vec<u8>;

    /// Reads the answer to a request carrying `items`. `Err` fails the whole request.
    fn read(&self, response: &Response, items: &[Self::Item]) -> Result<Outcome, IoError>;

    /// Waits until what `response` accepted is durable, asking over `transport` if the service says so separately.
    /// Called when an answer lands an item. `Ok(true)` says the service asked to slow down on the way, as a throttled
    /// answer would, and `waited` gains each wait that asking cost, whether or not this succeeds. `Err` fails the
    /// whole request, which is sent again.
    fn confirm(
        &self,
        _response: &Response,
        _transport: &mut dyn Transport,
        _deadline: Instant,
        _waited: &mut Duration,
    ) -> Result<bool, IoError> {
        Ok(false)
    }
}

/// The HTTP round trip, behind a trait so the retry and accounting loop can be tested without a socket. `'static`
/// because each pool sender owns one on its own thread.
pub trait Transport: Send + 'static {
    /// POSTs one request body to `path` under the destination's URL. `Err` is a transport failure (no answer).
    fn post(&mut self, path: &str, body: &[u8], budget: Duration) -> Result<Response, IoError>;
}

pub struct HttpDestination<P: Protocol, T: Transport> {
    protocol: P,
    transport: T,
    max_request_bytes: usize,
    stall_after: Duration,
    /// Present once the destination has senders; without it `deliver` posts each batch and waits.
    pool: Option<Pool<HttpSender<P, T>>>,
    /// Completions taken out of the pool by a barrier `flush`, held for the next `poll_completions`; dropping them
    /// would leave those batches uncommitted.
    drained: Vec<BatchCompletion>,
    throttle: Option<Throttle>,
}

/// Where a destination counts the time it held its sends back because the service asked it to slow down, as
/// `smeltr_destination_throttled_seconds_total`: the same series Kafka's broker throttle time feeds.
#[derive(Clone)]
struct Throttle {
    metrics: crate::metrics::MetricsHandle,
    destination: String,
}

impl Throttle {
    fn note(throttle: &Option<Throttle>, waited: Duration) {
        if let Some(t) = throttle {
            t.metrics.incr_seconds(
                "smeltr_destination_throttled_seconds_total",
                &[("destination", t.destination.as_str())],
                waited.as_secs_f64(),
            );
        }
    }
}

impl<P: Protocol, T: Transport> HttpDestination<P, T> {
    pub fn new(transport: T, protocol: P) -> Self {
        HttpDestination {
            protocol,
            transport,
            max_request_bytes: DEFAULT_MAX_REQUEST_BYTES,
            stall_after: DEFAULT_STALL_AFTER,
            pool: None,
            drained: Vec::new(),
            throttle: None,
        }
    }

    /// Count throttled time into `metrics` under `destination`. Before [`HttpDestination::with_senders`], which hands
    /// each sender its own copy.
    pub fn with_metrics(
        mut self,
        metrics: crate::metrics::MetricsHandle,
        destination: crate::core::id::DestId,
    ) -> Self {
        self.throttle = Some(Throttle {
            metrics,
            destination: destination.to_string(),
        });
        self
    }

    pub fn with_max_request_bytes(mut self, n: usize) -> Self {
        self.max_request_bytes = n;
        self
    }

    pub fn with_stall_after(mut self, d: Duration) -> Self {
        self.stall_after = d;
        self
    }

    /// [`HttpDestination::with_senders`] with `n` senders, each `make()`'s.
    pub fn with_senders_from(self, make: impl Fn() -> T, n: usize, start: usize) -> Self {
        self.with_senders((0..n).map(|_| make()).collect(), start)
    }

    /// Delivers through `senders` concurrently instead of blocking the caller for a round trip per batch. One
    /// connection per sender, so `senders.len()` is the most that can ever be outstanding; `start` is where the
    /// loop begins. Empty keeps the blocking behavior.
    pub fn with_senders(mut self, senders: Vec<T>, start: usize) -> Self {
        if senders.is_empty() {
            return self;
        }
        debug_assert!(
            self.protocol.order().is_none(),
            "a pool sends a batch's requests out of order"
        );
        self.pool = Some(Pool::new(
            senders
                .into_iter()
                .map(|transport| HttpSender {
                    protocol: self.protocol.clone(),
                    transport,
                    stall_after: self.stall_after,
                    throttle: self.throttle.clone(),
                })
                .collect(),
            start,
        ));
        self
    }

    /// Sorts `group` by `order`, then cuts it into requests within `max_request_bytes`: all but the last go to
    /// `full`, the last stays in `group`.
    fn cut_ordered(&self, group: &mut Group<P::Item>, order: Order<P::Item>, full: &mut Vec<Group<P::Item>>) {
        let mut sorted: Vec<(P::Item, usize)> = std::mem::take(&mut group.pending)
            .into_iter()
            .zip(std::mem::take(&mut group.sent_index))
            .collect();
        sorted.sort_by(|a, b| order(&a.0, &b.0));
        group.bytes = 0;
        for (item, index) in sorted {
            let size = self.protocol.size(&item);
            if self.overflows(group, size) {
                full.push(group.cut());
            }
            group.bytes += size;
            group.pending.push(item);
            group.sent_index.push(index);
        }
    }

    /// Whether `group` must go before an item of `size` joins it: a request stays within `max_request_bytes`, and an
    /// item larger than that goes alone.
    fn overflows(&self, group: &Group<P::Item>, size: usize) -> bool {
        !group.pending.is_empty() && group.bytes + size > self.max_request_bytes
    }

    /// One round on this thread, with retries, against `group`: the refusals, each carrying its row out of `rows`.
    fn post(&mut self, rows: &[OwnedRecord], group: &mut Group<P::Item>) -> Result<Vec<Rejection>, IoError> {
        let mut throttled = false;
        let mut waited = Duration::ZERO;
        group.bytes = 0;
        let delivered = deliver(
            &self.protocol,
            &mut self.transport,
            group,
            self.stall_after,
            Instant::now() + self.stall_after,
            &mut throttled,
            &mut waited,
        );
        Throttle::note(&self.throttle, waited);
        let mut refused = delivered?;
        for r in &mut refused {
            r.record = rows.get(r.doc).cloned();
        }
        Ok(refused)
    }
}

/// `group` delivered as two halves, for a request the service would not take whole or an answer the worker could not
/// hold whole. A group of one item has no halves: `None`, for the caller to refuse that item.
fn in_halves<P: Protocol, T: Transport>(
    protocol: &P,
    transport: &mut T,
    group: &mut Group<P::Item>,
    stall_after: Duration,
    deadline: Instant,
    throttled: &mut bool,
    waited: &mut Duration,
) -> Option<Result<Vec<Rejection>, IoError>> {
    if group.pending.len() < 2 {
        return None;
    }
    let half = group.pending.len() / 2;
    let mut tail = Group {
        route: group.route.clone(),
        part: group.part,
        pending: group.pending.split_off(half),
        sent_index: group.sent_index.split_off(half),
        bytes: 0,
    };
    let mut rejections = match deliver(protocol, transport, group, stall_after, deadline, throttled, waited) {
        Ok(rejections) => rejections,
        Err(e) => return Some(Err(e)),
    };
    match deliver(protocol, transport, &mut tail, stall_after, deadline, throttled, waited) {
        Ok(more) => rejections.extend(more),
        Err(e) => return Some(Err(e)),
    }
    Some(Ok(rejections))
}

/// One round, with retries, against `group`. Free because [`HttpSender`] runs it on a pool thread with its own
/// transport. `deadline` bounds the whole thing, not one attempt; `stall_after` is how it was set, for the error.
/// `waited` gains every backoff a throttled answer cost, whether or not the round lands.
fn deliver<P: Protocol, T: Transport>(
    protocol: &P,
    transport: &mut T,
    group: &mut Group<P::Item>,
    stall_after: Duration,
    deadline: Instant,
    throttled: &mut bool,
    waited: &mut Duration,
) -> Result<Vec<Rejection>, IoError> {
    let mut backoff = INITIAL_BACKOFF;
    let mut rejections = Vec::new();
    loop {
        if group.pending.is_empty() {
            return Ok(rejections);
        }
        let budget = deadline.saturating_duration_since(Instant::now());
        if budget.is_zero() {
            return Err(IoError::transient(format!(
                "{}: {} document(s) still undelivered after {:?}",
                protocol.name(),
                group.pending.len(),
                stall_after
            )));
        }

        // Named for the service here, so no transport has to know it, and no error carries a URL's credentials.
        let named = |mut e: IoError| {
            e.message = format!("{}: {}", protocol.name(), e.message);
            e
        };
        let response = match transport.post(&group.route, &protocol.body(&group.pending), budget) {
            Ok(response) => response,
            Err(e) if e.is_oversized() => {
                if let Some(halves) = in_halves(protocol, transport, group, stall_after, deadline, throttled, waited) {
                    rejections.extend(halves?);
                    return Ok(rejections);
                }
                // One document whose answer alone is past the bound: refused, since asking again reads the same answer.
                rejections.push(Rejection {
                    doc: group.sent_index[0],
                    reason: format!(
                        "{}: the answer to one document was larger than {} MiB",
                        protocol.name(),
                        MAX_ANSWER_BYTES >> 20
                    ),
                    record: None,
                });
                group.pending.clear();
                group.sent_index.clear();
                return Ok(rejections);
            }
            Err(e) => return Err(named(e)),
        };
        let mut outcome = protocol.read(&response, &group.pending)?;
        if let Some(why) = outcome.split.take() {
            if let Some(halves) = in_halves(protocol, transport, group, stall_after, deadline, throttled, waited) {
                rejections.extend(halves?);
                return Ok(rejections);
            }
            outcome.permanent.push((0, why));
        }
        if outcome.permanent.len() + outcome.retryable.len() < group.pending.len() {
            *throttled |= protocol
                .confirm(&response, transport, deadline, waited)
                .map_err(named)?;
        }

        // A document the service refused is not a flush failure: it would fail identically forever, and failing the
        // flush would leave the whole batch uncommitted behind it. It comes back as a rejection, for the destination's
        // answer to `Refused` to decide.
        for (i, why) in &outcome.permanent {
            rejections.push(Rejection {
                doc: group.sent_index[*i],
                reason: format!("{} refused it: {why}", protocol.name()),
                record: None,
            });
        }
        if outcome.retryable.is_empty() {
            group.pending.clear();
            group.sent_index.clear();
            return Ok(rejections);
        }
        // Only the ones that failed; resending the rest would duplicate them.
        let keep: Vec<P::Item> = outcome.retryable.iter().map(|&i| group.pending[i].clone()).collect();
        let keep_index: Vec<usize> = outcome.retryable.iter().map(|&i| group.sent_index[i]).collect();
        group.pending = keep;
        group.sent_index = keep_index;
        // Reported rather than inferred from timing: a slow service and an overloaded one look the same from the
        // outside. A retry the service did not ask to slow down goes at once.
        if !outcome.throttled {
            continue;
        }
        *throttled = true;

        // The service's own `Retry-After`, where longer than the backoff, and never past the deadline.
        let nap = response
            .asked_wait()
            .map_or(backoff, |wait| wait.max(backoff))
            .min(deadline.saturating_duration_since(Instant::now()));
        if nap.is_zero() {
            continue;
        }
        std::thread::sleep(nap);
        *waited += nap;
        backoff = (backoff * 2).min(Duration::from_secs(5));
    }
}

/// Items bound for one route, in one part.
struct Group<I> {
    route: String,
    part: usize,
    pending: Vec<I>,
    /// For each entry of `pending`, its position in the batch's rows: what a `Rejection` reports. Kept apart because
    /// `pending` is compacted on every retry.
    sent_index: Vec<usize>,
    /// What `pending` adds to a request body.
    bytes: usize,
}

impl<I> Group<I> {
    /// What this group holds, as a request of its own; the group is left empty, to fill again.
    fn cut(&mut self) -> Group<I> {
        Group {
            route: self.route.clone(),
            part: self.part,
            pending: std::mem::take(&mut self.pending),
            sent_index: std::mem::take(&mut self.sent_index),
            bytes: std::mem::take(&mut self.bytes),
        }
    }
}

/// A [`pipelined::Sender`] doing what the synchronous path does, on its own thread with its own transport.
struct HttpSender<P: Protocol, T: Transport> {
    protocol: P,
    transport: T,
    stall_after: Duration,
    throttle: Option<Throttle>,
}

impl<P: Protocol, T: Transport> pipelined::Sender for HttpSender<P, T> {
    /// One request.
    type Job = Group<P::Item>;

    fn send_one(&mut self, mut group: Group<P::Item>) -> Result<pipelined::Delivered, IoError> {
        let mut throttled = false;
        let mut waited = Duration::ZERO;
        let delivered = deliver(
            &self.protocol,
            &mut self.transport,
            &mut group,
            self.stall_after,
            Instant::now() + self.stall_after,
            &mut throttled,
            &mut waited,
        );
        Throttle::note(&self.throttle, waited);
        delivered.map(|rejections| pipelined::Delivered { rejections, throttled })
    }
}

impl<P: Protocol, T: Transport> Destination for HttpDestination<P, T> {
    /// Rows are grouped by route and cut into requests within `max_request_bytes`. Every request goes to the pool, each
    /// to a sender of its own, and the batch completes when the last is answered; without a pool, each is posted here.
    fn deliver(
        &mut self,
        batch: BatchId,
        rows: Cow<'_, [OwnedRecord]>,
    ) -> Result<(Dispatched, Vec<Rejection>), IoError> {
        let mut refused = Vec::new();
        let order = self.protocol.order();
        let mut groups: Vec<Group<P::Item>> = Vec::new();
        let mut full: Vec<Group<P::Item>> = Vec::new();
        // Each route's groups, by part, so neither a batch spanning many routes nor a part per row is searched once
        // per row.
        let mut by_route: HashMap<String, HashMap<usize, usize>> = HashMap::new();
        for (i, row) in rows.iter().enumerate() {
            if let Some(why) = &row.unfit {
                refused.push(Rejection::of(&rows, i, why.clone()));
                continue;
            }
            let item = match self.protocol.admit(row) {
                Ok(item) => item,
                Err(why) => {
                    refused.push(Rejection::of(&rows, i, why));
                    continue;
                }
            };
            let route = self.protocol.route(&item);
            let part = self.protocol.part(&item);
            let found = by_route.get(route).and_then(|at| at.get(&part)).copied();
            let at = match found {
                Some(at) => at,
                None => {
                    by_route
                        .entry(route.to_string())
                        .or_default()
                        .insert(part, groups.len());
                    groups.push(Group {
                        route: route.to_string(),
                        part,
                        pending: Vec::new(),
                        sent_index: Vec::new(),
                        bytes: 0,
                    });
                    groups.len() - 1
                }
            };
            let size = self.protocol.size(&item);
            if order.is_none() && self.overflows(&groups[at], size) {
                full.push(groups[at].cut());
            }
            let group = &mut groups[at];
            group.bytes += size;
            group.sent_index.push(i);
            group.pending.push(item);
        }
        if let Some(order) = order {
            for group in &mut groups {
                self.cut_ordered(group, order, &mut full);
            }
        }
        full.extend(groups.into_iter().filter(|g| !g.pending.is_empty()));
        // A batch with nothing to send is done: no answer will come for it.
        let Some(pool) = self.pool.as_mut().filter(|_| !full.is_empty()) else {
            for group in &mut full {
                refused.extend(self.post(&rows, group)?);
            }
            return Ok((Dispatched::Complete, refused));
        };
        pool.submit(batch, full)?;
        Ok((Dispatched::InFlight, refused))
    }

    /// Wait for every batch the pool holds. `flush` cannot return the completions, so they are held for the next
    /// `poll_completions`.
    fn flush(&mut self) -> Result<(), IoError> {
        if let Some(pool) = &mut self.pool {
            let done = pool.drain()?;
            self.drained.extend(done);
        }
        Ok(())
    }

    /// Requests the pool has open. A destination delivering one batch at a time has no pool and no figure to report.
    fn concurrency(&mut self) -> Option<usize> {
        self.pool.as_ref().map(|p| p.outstanding())
    }

    fn lend_senders(&mut self, lend: crate::io::Lend) {
        if let Some(pool) = &mut self.pool {
            pool.lend_senders(lend);
        }
    }

    fn poll_completions(&mut self) -> Result<Vec<BatchCompletion>, IoError> {
        // What a barrier saw land is reported before the pool is asked: a failure there would otherwise take it back.
        let drained = std::mem::take(&mut self.drained);
        match &mut self.pool {
            Some(pool) if drained.is_empty() => pool.poll(),
            _ => Ok(drained),
        }
    }
}

/// The real transport: blocking HTTP POSTs under one base URL.
pub struct HttpTransport {
    agent: ureq::Agent,
    url: String,
    content_type: &'static str,
    headers: Vec<(String, String)>,
    /// What the service set with `Set-Cookie`, by name, sent back on every request. A load balancer keeps a client on
    /// one backend by cookie, and a HEC acknowledgement is only known to the indexer that took the request. One jar per
    /// transport, so each sender keeps its own backend.
    cookies: Vec<(String, String)>,
    /// Whether a 3xx is followed. ureq follows a POST's redirect as a GET with no body, so a service whose answer is
    /// read by its status alone would count that GET's 2xx as the rows landing.
    follow_redirects: bool,
    /// How long a connection may take to open, within a request's budget. Unbounded, a host that never answers
    /// takes the whole budget.
    connect_within: Option<Duration>,
    /// Signs every request in with a token. Shared by every sender of a destination, so they hold one token
    /// between them.
    sign_in: Option<SharedSignIn>,
}

/// One sign-in, shared by the transports that send with its token.
pub type SharedSignIn = Arc<Mutex<Box<dyn super::api::Authorize>>>;

/// The most cookies one transport keeps: a load balancer's affinity needs one or two, and a service that sets a new
/// name on every answer must not grow the jar without end.
const MAX_COOKIES: usize = 16;

impl HttpTransport {
    /// `url` is the service's base address; a request's path is appended to it. `headers` go on every request, an
    /// `Authorization` among them when the service asks for one. `config` is [`agent_config`]'s answer for this
    /// destination. Every transport it owns shares it and takes an agent, and so a connection, of its own.
    pub fn new(
        url: &str,
        content_type: &'static str,
        headers: Vec<(String, String)>,
        config: &ureq::config::Config,
    ) -> Self {
        HttpTransport {
            agent: config.new_agent(),
            url: url.trim_end_matches('/').to_string(),
            content_type,
            headers,
            cookies: Vec::new(),
            follow_redirects: true,
            connect_within: None,
            sign_in: None,
        }
    }

    /// Signs every request in with `sign_in`'s token. A request whose token is refused with a 401 is sent once more
    /// with a new token when `sign_in` says one is worth asking for.
    pub fn signed_in(mut self, sign_in: SharedSignIn) -> Self {
        self.sign_in = Some(sign_in);
        self
    }

    /// Sends later requests under `url`. A new address is another service, so the cookies the old one set go.
    pub fn rebase(&mut self, url: &str) {
        let url = url.trim_end_matches('/');
        if self.url != url {
            self.url = url.to_string();
            self.cookies.clear();
        }
    }

    /// Gives up on a connection that has not opened within `within`, so a request can move on to another address.
    pub fn with_connect_within(mut self, within: Duration) -> Self {
        self.connect_within = Some(within);
        self
    }

    /// Hands a 3xx to the protocol as the answer, rather than following it.
    pub fn without_redirects(mut self) -> Self {
        self.follow_redirects = false;
        self
    }
}

impl Transport for HttpTransport {
    fn post(&mut self, path: &str, body: &[u8], budget: Duration) -> Result<Response, IoError> {
        let Some(sign_in) = self.sign_in.clone() else {
            return self.send(path, body, budget, &[]);
        };
        let deadline = Instant::now() + budget;
        let left = || deadline.saturating_duration_since(Instant::now());
        let url = format!("{}{path}", self.url);
        let mut auth = lock(&sign_in).authorize(&self.agent, super::client_credentials::SIGN_IN_TIMEOUT, &url)?;
        let mut asked_again = false;
        loop {
            let answer = super::api::Answer::from(self.send(path, body, left(), &auth)?);
            let mut sign_in = lock(&sign_in);
            if answer.status != 401 {
                sign_in.used(&answer);
                return Ok(answer.into());
            }
            if asked_again {
                return Ok(answer.into());
            }
            asked_again = true;
            // Another sender may have replaced the token this request carried: that one is not the token refused.
            let now = sign_in.authorize(&self.agent, super::client_credentials::SIGN_IN_TIMEOUT, &url)?;
            if now == auth && !sign_in.refused(&answer) {
                return Ok(answer.into());
            }
            auth = if now == auth {
                sign_in.authorize(&self.agent, super::client_credentials::SIGN_IN_TIMEOUT, &url)?
            } else {
                now
            };
            // A sign-in can spend what the request had left, and a request sent with no time cannot land.
            if left().is_zero() {
                return Ok(answer.into());
            }
        }
    }
}

/// The sign-in, whatever a sender that panicked holding it left: a token is replaced when refused.
fn lock(sign_in: &SharedSignIn) -> std::sync::MutexGuard<'_, Box<dyn super::api::Authorize>> {
    sign_in.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

impl HttpTransport {
    /// POSTs `body` to `path` with the transport's headers and `auth`.
    fn send(
        &mut self,
        path: &str,
        body: &[u8],
        budget: Duration,
        auth: &[(String, String)],
    ) -> Result<Response, IoError> {
        let mut config = self
            .agent
            .post(format!("{}{path}", self.url))
            .header("Content-Type", self.content_type)
            .config()
            .timeout_global(Some(budget))
            .timeout_connect(self.connect_within.map(|within| within.min(budget)))
            // The protocol reads every status; an error status still carries the answer that says why.
            .http_status_as_error(false);
        if !self.follow_redirects {
            config = config.max_redirects(0);
        }
        let mut req = config.build();
        for (name, value) in self.headers.iter().chain(auth) {
            req = req.header(name, value);
        }
        if !self.cookies.is_empty() {
            let jar: Vec<String> = self.cookies.iter().map(|(n, v)| format!("{n}={v}")).collect();
            req = req.header("Cookie", jar.join("; "));
        }
        let mut response = req
            .send(body)
            .map_err(|e| IoError::transient(format!("request failed: {e}")))?;
        let status = response.status().as_u16();
        for set in response.headers().get_all("set-cookie") {
            if let Ok(set) = set.to_str() {
                keep_cookie(&mut self.cookies, set);
            }
        }
        let headers = (response.headers().iter())
            .filter_map(|(name, value)| Some((name.as_str().to_string(), value.to_str().ok()?.to_string())))
            .collect();
        let body = response
            .body_mut()
            .with_config()
            .limit(MAX_ANSWER_BYTES)
            .read_to_vec()
            .map_err(|e| match e {
                // Only an answer to the request's items scales with them. An error page that large is a refusal
                // to retry whole, as it would be at any size, never a batch to halve.
                ureq::Error::BodyExceedsLimit(_) if (200..300).contains(&status) => {
                    IoError::oversized(format!("the answer was larger than {} MiB", MAX_ANSWER_BYTES >> 20))
                }
                e => IoError::transient(format!("HTTP {status}: could not read the answer: {e}")),
            })?;
        Ok(Response { status, headers, body })
    }
}

/// Takes one `Set-Cookie` value into `jar`: its name and value, which is all a client sends back. A cookie set to
/// expire now is removed; past [`MAX_COOKIES`] the oldest goes. Its other attributes scope it to a host and path, and a
/// transport talks to one service.
fn keep_cookie(jar: &mut Vec<(String, String)>, set: &str) {
    let mut parts = set.split(';');
    let Some((name, value)) = parts.next().and_then(|p| p.split_once('=')) else {
        return;
    };
    let (name, value) = (name.trim(), value.trim());
    if name.is_empty() {
        return;
    }
    jar.retain(|(n, _)| n != name);
    if parts.any(|a| a.trim().eq_ignore_ascii_case("max-age=0")) {
        return;
    }
    if jar.len() == MAX_COOKIES {
        jar.remove(0);
    }
    jar.push((name.to_string(), value.to_string()));
}

/// Each of `credentials`, a header's name and where its value lives, with the value this worker holds. `what` names
/// the connector for an operator reading the failure. A credential this worker cannot obtain is an error, never a
/// request sent without it.
pub fn credential_headers(
    what: &str,
    credentials: &[(String, crate::plan::topology::SecretRef)],
    secrets: &HeldSecrets,
) -> Result<Vec<(String, String)>, IoError> {
    credentials
        .iter()
        .map(|(header, reference)| {
            Ok((
                header.clone(),
                credential(what, &format!("{header:?} header"), reference, secrets)?,
            ))
        })
        .collect()
}

/// The value of the credential `what` names `name`, refused as `credential_headers` refuses a header's.
pub fn credential(
    what: &str,
    name: &str,
    reference: &crate::plan::topology::SecretRef,
    secrets: &HeldSecrets,
) -> Result<String, IoError> {
    let value = super::held(secrets, reference, what, name)?;
    // A secret pasted with its trailing newline would fail every request, as no header value.
    if value.chars().any(|c| c.is_control() && c != '\t') {
        return Err(IoError::transient(format!(
            "{what}'s {name} takes {}, whose value holds a line break or control character",
            reference.describe()
        )));
    }
    Ok(value)
}

/// The `Authorization` value signing in as `user` with the password the credential `what` names `password` holds,
/// refused as `credential` refuses one.
pub fn basic(
    what: &str,
    name: &str,
    user: &str,
    password: &crate::plan::topology::SecretRef,
    secrets: &HeldSecrets,
) -> Result<String, IoError> {
    Ok(basic_authorization(user, &credential(what, name, password, secrets)?))
}

/// The `Authorization` value signing in as `user` with `password` (RFC 7617).
pub fn basic_authorization(user: &str, password: &str) -> String {
    format!(
        "Basic {}",
        crate::format::bytes_text::BytesForm::Base64.text(format!("{user}:{password}").as_bytes())
    )
}

/// How a client's transports connect: the certificate they present, and the roots they verify the service against
/// (`io::trust`). Speaking no TLS of its own, no certificate.
///
/// A credential this worker cannot obtain is an error here, never a connection that silently falls back to
/// presenting nothing.
pub fn agent_config(tls: Option<&Tls>, secrets: &HeldSecrets) -> Result<ureq::config::Config, IoError> {
    Ok(ureq::Agent::config_builder()
        .tls_config(tls_config(tls, secrets)?)
        .build())
}

/// `agent_config`'s TLS alone, for a caller that sets the rest of the agent itself.
pub fn tls_config(tls: Option<&Tls>, secrets: &HeldSecrets) -> Result<ureq::tls::TlsConfig, IoError> {
    let config = ureq::tls::TlsConfig::builder().root_certs(super::trust::ureq_roots(tls)?);
    let Some(presented) = tls.map(|t| super::trust::presented(t, secrets)).transpose()?.flatten() else {
        return Ok(config.build());
    };
    let chain: Vec<_> = (presented.chain.iter())
        .map(|der| ureq::tls::Certificate::from_der(der.as_ref()).to_owned())
        .collect();
    let key = ureq::tls::PrivateKey::from_pem(presented.key_pem.as_bytes())
        .map_err(|e| IoError::transient(format!("a private key ureq cannot read: {e}")))?
        .to_owned();
    Ok(config
        .client_cert(Some(ureq::tls::ClientCert::new_with_certs(&chain, key)))
        .build())
}

#[cfg(test)]
mod tests {
    use std::io::{BufRead, BufReader, Read, Write};
    use std::net::TcpListener;

    use super::*;
    use crate::io::api::{Answer, Authorize};

    /// A sign-in handing out `token-1`, `token-2`, ... and saying a refused token is worth replacing when `replace`.
    struct Tokens {
        issued: usize,
        replace: bool,
        refusals: Arc<Mutex<usize>>,
    }

    impl Authorize for Tokens {
        fn authorize(&mut self, _: &ureq::Agent, _: Duration, _: &str) -> Result<Vec<(String, String)>, IoError> {
            if self.issued == 0 {
                self.issued = 1;
            }
            Ok(vec![("Authorization".into(), format!("Bearer token-{}", self.issued))])
        }

        fn refused(&mut self, _: &Answer) -> bool {
            *self.refusals.lock().unwrap() += 1;
            self.issued += 1;
            self.replace
        }

        fn used(&mut self, _: &Answer) {}
    }

    /// A service on 127.0.0.1 answering 401 to every request whose token is not one of `good`, 204 to the rest, and
    /// keeping the token each request carried.
    fn serve(good: &'static [&'static str]) -> (String, Arc<Mutex<Vec<String>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let seen = Arc::new(Mutex::new(Vec::new()));
        let log = seen.clone();
        std::thread::spawn(move || {
            for conn in listener.incoming() {
                let mut reader = BufReader::new(conn.unwrap());
                let (mut length, mut token) = (0, String::new());
                loop {
                    let mut line = String::new();
                    if reader.read_line(&mut line).unwrap_or(0) == 0 {
                        break;
                    }
                    let line = line.trim_end();
                    if line.is_empty() {
                        break;
                    }
                    let lower = line.to_ascii_lowercase();
                    if let Some(v) = lower.strip_prefix("content-length:") {
                        length = v.trim().parse().unwrap();
                    }
                    if lower.starts_with("authorization: bearer ") {
                        token = line["authorization: bearer ".len()..].to_string();
                    }
                }
                let mut body = vec![0; length];
                let _ = reader.read_exact(&mut body);
                let status = if good.contains(&token.as_str()) { 204 } else { 401 };
                log.lock().unwrap().push(token);
                let mut conn = reader.into_inner();
                let _ = write!(
                    conn,
                    "HTTP/1.1 {status} X\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                );
            }
        });
        (url, seen)
    }

    fn transport(url: &str, replace: bool) -> (HttpTransport, Arc<Mutex<usize>>) {
        let refusals = Arc::new(Mutex::new(0));
        let sign_in: SharedSignIn = Arc::new(Mutex::new(Box::new(Tokens {
            issued: 0,
            replace,
            refusals: refusals.clone(),
        })));
        let config = ureq::Agent::config_builder().proxy(None).build();
        (
            HttpTransport::new(url, "application/json", Vec::new(), &config).signed_in(sign_in),
            refusals,
        )
    }

    #[test]
    fn a_refused_token_worth_replacing_is_replaced_and_the_request_sent_once_more() {
        let (url, seen) = serve(&["token-2"]);
        let (mut t, refusals) = transport(&url, true);
        let response = t.post("/x", b"[]", Duration::from_secs(5)).unwrap();
        assert_eq!(response.status, 204);
        assert_eq!(*seen.lock().unwrap(), ["token-1", "token-2"]);
        assert_eq!(*refusals.lock().unwrap(), 1);
    }

    #[test]
    fn a_refused_token_not_worth_replacing_is_the_answer() {
        let (url, seen) = serve(&[]);
        let (mut t, _) = transport(&url, false);
        assert_eq!(t.post("/x", b"[]", Duration::from_secs(5)).unwrap().status, 401);
        assert_eq!(seen.lock().unwrap().len(), 1);
    }

    #[test]
    fn a_request_is_sent_again_at_most_once() {
        let (url, seen) = serve(&[]);
        let (mut t, _) = transport(&url, true);
        assert_eq!(t.post("/x", b"[]", Duration::from_secs(5)).unwrap().status, 401);
        assert_eq!(*seen.lock().unwrap(), ["token-1", "token-2"]);
    }

    #[test]
    fn a_token_another_sender_already_replaced_is_not_refused_again() {
        let (url, seen) = serve(&["token-2"]);
        let (mut a, refusals) = transport(&url, true);
        let mut b = HttpTransport::new(
            &url,
            "application/json",
            Vec::new(),
            &ureq::Agent::config_builder().proxy(None).build(),
        )
        .signed_in(a.sign_in.clone().unwrap());
        // `b` carries token-1 to a service that refuses it, after `a` has already had it replaced.
        assert_eq!(a.post("/x", b"[]", Duration::from_secs(5)).unwrap().status, 204);
        let stale = vec![("Authorization".to_string(), "Bearer token-1".to_string())];
        assert_eq!(b.send("/x", b"[]", Duration::from_secs(5), &stale).unwrap().status, 401);
        assert_eq!(b.post("/x", b"[]", Duration::from_secs(5)).unwrap().status, 204);
        assert_eq!(
            *refusals.lock().unwrap(),
            1,
            "only the first refusal replaced the token"
        );
        assert_eq!(*seen.lock().unwrap(), ["token-1", "token-2", "token-1", "token-2"]);
    }
}
