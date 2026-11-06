//! The connections a receiver holds open, held to its `ConnectionLimits`. Each connection's stream is wrapped so it
//! marks when the receiver began to wait on it for a read. A connection is idle while the receiver
//! waits on it and it sends nothing; one the receiver is busy with, or has stopped reading while it is full, is not.
//! At the cap, a new connection takes the place of the one idle longest, if that one has been idle at least
//! [`EVICTABLE_AFTER`]; otherwise the new one is closed. So a quiet sender keeps its connection as long as there is
//! room, and a sender that opens connections and leaves them idle cannot take the room every other sender needs.
//!
//! A sender that sends a little before each connection has idled [`EVICTABLE_AFTER`] keeps every place it takes. Only
//! `maxConnectionsPerSender` stops it, and it is unset by default, since a balancer or NAT in front of the workers
//! sends every sender through one address.
//!
//! Every receiver of a process also draws on one [`Budget`]: its open-file limit, less what the process keeps for
//! everything else it holds open. When the budget is spent, a new connection takes the place of the connection idle
//! longest on any receiver, by the same rule.

use std::collections::HashMap;
use std::net::IpAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::task::AbortHandle;

use crate::plan::topology::ConnectionLimits;

/// How long a connection must have been idle before a new one at the cap may take its place. Long enough that a
/// sender between two messages keeps its connection. The GUI's help and the refusals the receiver logs say "a minute".
pub(super) const EVICTABLE_AFTER: Duration = Duration::from_secs(60);

/// Why a connection was closed to keep the limits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Closed {
    /// A new connection at `maxConnections` took its place.
    ForNew,
    /// A new connection, with the worker's [`Budget`] spent, took its place.
    ForNewOnWorker,
    /// A new connection from the same sender at `maxConnectionsPerSender` took its place.
    ForNewFromSender,
    /// It read nothing for `idleTimeoutSecs`.
    Idle,
}

/// Why a new connection was refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Refused {
    /// The receiver holds `maxConnections`, none idle long enough to close.
    Full,
    /// It holds `maxConnectionsPerSender` from this sender, none idle long enough to close.
    FullFromSender,
    /// The worker's [`Budget`] is spent, none of its connections idle long enough to close.
    WorkerFull,
}

/// A connection's mark while the receiver is not waiting on it: never idle.
const BUSY: u64 = u64::MAX;

struct Open {
    peer: IpAddr,
    /// Since when the receiver has waited on it for a read, in milliseconds since `Connections::epoch`; or [`BUSY`].
    read: Arc<AtomicU64>,
    /// The requests it is answering, for a connection served over HTTP ([`Counting`]).
    in_flight: Arc<AtomicUsize>,
    shut: Arc<Shut>,
    abort: Option<AbortHandle>,
}

impl Open {
    /// Since when it has been idle, as the stream's `read` mark is kept; or [`BUSY`]. hyper reads a connection while
    /// it answers a request, to see whether the client hung up, so a request in flight keeps it busy too.
    fn idle_since(&self) -> u64 {
        match self.in_flight.load(Ordering::Relaxed) {
            0 => self.read.load(Ordering::Relaxed),
            _ => BUSY,
        }
    }
}

/// How a connection is closed from outside the task that serves it: its stream fails every read and write once
/// `closed`, and `waiting`, the reader that was last left waiting, is woken to find that.
#[derive(Default)]
struct Shut {
    closed: AtomicBool,
    waiting: Mutex<Option<Waker>>,
}

impl Shut {
    fn close(&self) {
        self.closed.store(true, Ordering::Relaxed);
        if let Some(waker) = self.waiting.lock().unwrap_or_else(|e| e.into_inner()).take() {
            waker.wake();
        }
    }
}

/// The connections a receiver holds, by an id of their own.
pub(super) struct Connections {
    limits: ConnectionLimits,
    budget: Arc<Budget>,
    epoch: Instant,
    next: u64,
    open: HashMap<u64, Open>,
}

/// The connections the receivers of a process hold together, and the most they may.
pub(super) struct Budget {
    most: usize,
    held: AtomicUsize,
    receivers: Mutex<Vec<Weak<Mutex<Connections>>>>,
}

/// The open-file limit of this process, and the connections its receivers may hold together within it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OpenFiles {
    /// `None` where the platform has no such limit, or it is unlimited.
    pub limit: Option<u64>,
    pub budget: usize,
}

/// The descriptors a process keeps from its receivers' connections, for its destinations, its files, its pipes and
/// its link to the leader: a quarter of its limit, up to this many.
const RESERVE_MOST: u64 = 8_192;

impl OpenFiles {
    fn within(limit: Option<u64>) -> OpenFiles {
        let budget = match limit {
            Some(limit) => usize::try_from(limit - (limit / 4).min(RESERVE_MOST)).unwrap_or(usize::MAX),
            None => usize::MAX,
        };
        OpenFiles {
            limit,
            budget: budget.min(ConnectionLimits::MOST),
        }
    }
}

/// Raises this process's soft open-file limit to its hard one, as far as the system lets it, and returns the limit
/// it has then. Docker 29 gives a container a soft limit of 1,024, and the soft one is what
/// bounds the connections a worker can hold. Called once, at startup, before any receiver opens: a child
/// process inherits the raised limit.
pub fn raise_open_file_limit() -> std::io::Result<OpenFiles> {
    #[cfg(unix)]
    {
        let now = open_file_limit()?;
        if now.rlim_cur < now.rlim_max {
            let raised = libc::rlimit {
                rlim_cur: now.rlim_max,
                rlim_max: now.rlim_max,
            };
            // SAFETY: `raised` is a valid `rlimit`. A system that refuses it leaves the limit as it was.
            if unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &raised) } != 0 {
                return Err(std::io::Error::last_os_error());
            }
        }
    }
    Ok(open_files())
}

/// This process's soft and hard open-file limits.
#[cfg(unix)]
fn open_file_limit() -> std::io::Result<libc::rlimit> {
    let mut now = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: `now` is a valid `rlimit` for the call to fill.
    match unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut now) } {
        0 => Ok(now),
        _ => Err(std::io::Error::last_os_error()),
    }
}

/// This process's open-file limit, as it is now, and the budget it gives.
pub fn open_files() -> OpenFiles {
    #[cfg(unix)]
    {
        let soft = open_file_limit().ok().map(|now| now.rlim_cur);
        OpenFiles::within(soft.filter(|&soft| soft != libc::RLIM_INFINITY))
    }
    #[cfg(not(unix))]
    {
        OpenFiles::within(None)
    }
}

impl Budget {
    pub(super) fn new(most: usize) -> Arc<Budget> {
        Arc::new(Budget {
            most,
            held: AtomicUsize::new(0),
            receivers: Mutex::new(Vec::new()),
        })
    }

    /// This process's budget, taken from its open-file limit when it is first asked for and the same after that. main
    /// raises the limit before any receiver starts, so this reads the raised one.
    fn process() -> &'static Arc<Budget> {
        static BUDGET: OnceLock<Arc<Budget>> = OnceLock::new();
        BUDGET.get_or_init(|| Budget::new(open_files().budget))
    }

    /// Closes the connection idle longest on any receiver, if it has been idle for [`EVICTABLE_AFTER`]. Returns its
    /// sender. Each receiver is locked in turn, never two at once, so it may be called by any receiver not holding
    /// its own lock.
    fn close_idlest(&self) -> Option<IpAddr> {
        let receivers: Vec<Arc<Mutex<Connections>>> = {
            let mut receivers = self.receivers.lock().unwrap_or_else(|e| e.into_inner());
            receivers.retain(|r| r.strong_count() > 0);
            receivers.iter().filter_map(Weak::upgrade).collect()
        };
        let (receiver, id, _) = (receivers.iter())
            .filter_map(|r| {
                let (id, idle) = r.lock().unwrap_or_else(|e| e.into_inner()).idlest(None)?;
                Some((r, id, idle))
            })
            .max_by_key(|(_, _, idle)| *idle)?;
        let mut receiver = receiver.lock().unwrap_or_else(|e| e.into_inner());
        // It may have read since it was found, so it is checked again before it is closed.
        match receiver.idle_for(id)? >= EVICTABLE_AFTER {
            true => receiver.close(id),
            false => None,
        }
    }
}

/// A connection's place among the receiver's: its id, and the clock its stream marks its waits on.
pub(super) struct Place {
    pub(super) id: u64,
    pub(super) read: Arc<AtomicU64>,
    pub(super) in_flight: Arc<AtomicUsize>,
    shut: Arc<Shut>,
    epoch: Instant,
}

impl Connections {
    /// A receiver's connections, held to `limits` and to this process's [`Budget`].
    pub(super) fn new(limits: ConnectionLimits) -> Arc<Mutex<Connections>> {
        Connections::within(limits, Budget::process())
    }

    /// A receiver's connections, held to `limits` and to `budget`.
    pub(super) fn within(limits: ConnectionLimits, budget: &Arc<Budget>) -> Arc<Mutex<Connections>> {
        let open = Arc::new(Mutex::new(Connections {
            limits,
            budget: budget.clone(),
            epoch: Instant::now(),
            next: 0,
            open: HashMap::new(),
        }));
        let mut receivers = budget.receivers.lock().unwrap_or_else(|e| e.into_inner());
        receivers.retain(|r| r.strong_count() > 0);
        receivers.push(Arc::downgrade(&open));
        open
    }

    fn now(&self) -> u64 {
        self.epoch.elapsed().as_millis() as u64
    }

    pub(super) fn limits(&self) -> ConnectionLimits {
        self.limits
    }

    /// The most connections the worker's receivers hold together.
    pub(super) fn budget(&self) -> usize {
        self.budget.most
    }

    /// Makes room in `open` for a connection from `peer`, closing the connection idle longest where the limits say:
    /// the receiver's own, and then, with the worker's budget spent, any receiver's. Returns the connections it
    /// closed and the new one's place, or why it is refused.
    pub(super) fn admit(
        open: &Arc<Mutex<Connections>>,
        peer: IpAddr,
    ) -> (Vec<(IpAddr, Closed)>, Result<Place, Refused>) {
        let (mut closed, id, budget) = {
            let mut c = open.lock().unwrap_or_else(|e| e.into_inner());
            let (closed, id) = c.make_room(peer);
            let id = match id {
                Ok(id) => id,
                Err(why) => return (closed, Err(why)),
            };
            (closed, id, c.budget.clone())
        };
        // Its own lock is let go first: making room on the worker locks each receiver in turn, this one too.
        if budget.held.load(Ordering::Relaxed) > budget.most {
            match budget.close_idlest() {
                Some(gone) => closed.push((gone, Closed::ForNewOnWorker)),
                None => {
                    open.lock().unwrap_or_else(|e| e.into_inner()).remove(id);
                    return (closed, Err(Refused::WorkerFull));
                }
            }
        }
        let c = open.lock().unwrap_or_else(|e| e.into_inner());
        let Some(admitted) = c.open.get(&id) else {
            return (closed, Err(Refused::WorkerFull));
        };
        admitted.read.store(c.now(), Ordering::Relaxed);
        let place = Place {
            id,
            read: admitted.read.clone(),
            in_flight: admitted.in_flight.clone(),
            shut: admitted.shut.clone(),
            epoch: c.epoch,
        };
        (closed, Ok(place))
    }

    /// Makes room for a connection from `peer` within this receiver's own limits, and holds its place, busy so
    /// nothing closes it before it is admitted on the worker too.
    fn make_room(&mut self, peer: IpAddr) -> (Vec<(IpAddr, Closed)>, Result<u64, Refused>) {
        let mut closed = Vec::new();
        if let Some(cap) = self.limits.per_sender
            && self.open.values().filter(|o| o.peer == peer).count() >= cap
        {
            match self.close_idlest(Some(peer)) {
                Some(gone) => closed.push((gone, Closed::ForNewFromSender)),
                None => return (closed, Err(Refused::FullFromSender)),
            }
        }
        if let Some(max) = self.limits.max
            && self.open.len() >= max
        {
            match self.close_idlest(None) {
                Some(gone) => closed.push((gone, Closed::ForNew)),
                None => return (closed, Err(Refused::Full)),
            }
        }
        let id = self.next;
        self.next += 1;
        self.open.insert(
            id,
            Open {
                peer,
                read: Arc::new(AtomicU64::new(BUSY)),
                in_flight: Arc::new(AtomicUsize::new(0)),
                shut: Arc::new(Shut::default()),
                abort: None,
            },
        );
        self.budget.held.fetch_add(1, Ordering::Relaxed);
        (closed, Ok(id))
    }

    /// Gives up connection `id`'s place, here and in the worker's budget.
    fn remove(&mut self, id: u64) -> Option<Open> {
        let open = self.open.remove(&id)?;
        self.budget.held.fetch_sub(1, Ordering::Relaxed);
        Some(open)
    }

    /// Gives connection `id` the handle that ends its task, once it runs, so it is closed even while its task is not
    /// reading it.
    pub(super) fn started(&mut self, id: u64, abort: AbortHandle) {
        if let Some(open) = self.open.get_mut(&id) {
            open.abort = Some(abort);
        }
    }

    /// Connection `id` has ended.
    pub(super) fn ended(&mut self, id: u64) {
        self.remove(id);
    }

    /// The connection, of `peer`'s where given, that has been idle longest, and how long.
    fn idlest(&self, peer: Option<IpAddr>) -> Option<(u64, Duration)> {
        let (&id, _) = (self.open.iter())
            .filter(|(_, o)| peer.is_none_or(|p| o.peer == p))
            .min_by_key(|(_, o)| o.idle_since())?;
        Some((id, self.idle_for(id)?))
    }

    /// How long connection `id` has been idle.
    fn idle_for(&self, id: u64) -> Option<Duration> {
        let since = self.open.get(&id)?.idle_since();
        Some(Duration::from_millis(self.now().saturating_sub(since)))
    }

    /// Closes the connection, of `peer`'s where given, that has been idle longest, if it has been idle for
    /// [`EVICTABLE_AFTER`]. Returns the sender of the connection it closed.
    fn close_idlest(&mut self, peer: Option<IpAddr>) -> Option<IpAddr> {
        match self.idlest(peer)? {
            (id, idle) if idle >= EVICTABLE_AFTER => self.close(id),
            _ => None,
        }
    }

    /// Closes every connection that has been idle for the idle timeout. Returns the senders of the connections it
    /// closed.
    pub(super) fn close_idle(&mut self) -> Vec<IpAddr> {
        let Some(idle) = self.limits.idle else {
            return Vec::new();
        };
        let now = self.now();
        let idle = u64::try_from(idle.as_millis()).unwrap_or(u64::MAX);
        let idle: Vec<u64> = (self.open.iter())
            .filter(|(_, o)| now.saturating_sub(o.idle_since()) >= idle)
            .map(|(&id, _)| id)
            .collect();
        idle.into_iter().filter_map(|id| self.close(id)).collect()
    }

    fn close(&mut self, id: u64) -> Option<IpAddr> {
        let open = self.remove(id)?;
        open.shut.close();
        if let Some(abort) = open.abort {
            abort.abort();
        }
        Some(open.peer)
    }

    /// How often the idle timeout is checked, if there is one.
    pub(super) fn idle_check(&self) -> Option<Duration> {
        self.limits
            .idle
            .map(|idle| (idle / 4).clamp(Duration::from_millis(100), Duration::from_secs(1)))
    }
}

/// A connection's stream, which marks when its reader began to wait on it and that it is busy once a read returns.
/// It fails once the receiver closes the connection, and dropping it gives up the connection's place.
pub(super) struct Marked<S> {
    stream: S,
    id: u64,
    open: Arc<Mutex<Connections>>,
    read: Arc<AtomicU64>,
    shut: Arc<Shut>,
    epoch: Instant,
    waiting: bool,
}

impl<S> Marked<S> {
    /// `stream`, the connection given `place` among `open`.
    pub(super) fn new(stream: S, place: &Place, open: &Arc<Mutex<Connections>>) -> Marked<S> {
        Marked {
            stream,
            id: place.id,
            open: open.clone(),
            read: place.read.clone(),
            shut: place.shut.clone(),
            epoch: place.epoch,
            waiting: false,
        }
    }

    fn closed(&self) -> Option<std::io::Error> {
        self.shut.closed.load(Ordering::Relaxed).then(|| {
            std::io::Error::new(
                std::io::ErrorKind::ConnectionAborted,
                "the receiver closed the connection to keep its limits",
            )
        })
    }
}

impl<S> Drop for Marked<S> {
    fn drop(&mut self) {
        self.open.lock().unwrap_or_else(|e| e.into_inner()).ended(self.id);
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for Marked<S> {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        if let Some(e) = self.closed() {
            return Poll::Ready(Err(e));
        }
        let polled = Pin::new(&mut self.stream).poll_read(cx, buf);
        match polled {
            Poll::Ready(_) => {
                self.read.store(BUSY, Ordering::Relaxed);
                self.waiting = false;
            }
            Poll::Pending => {
                if !self.waiting {
                    self.read
                        .store(self.epoch.elapsed().as_millis() as u64, Ordering::Relaxed);
                    self.waiting = true;
                }
                let mut waiting = self.shut.waiting.lock().unwrap_or_else(|e| e.into_inner());
                if !waiting.as_ref().is_some_and(|w| w.will_wake(cx.waker())) {
                    *waiting = Some(cx.waker().clone());
                }
                drop(waiting);
                // Re-check: if the connection was closed after the check above, nothing would wake this reader, so
                // fail now.
                if let Some(e) = self.closed() {
                    return Poll::Ready(Err(e));
                }
            }
        }
        polled
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for Marked<S> {
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<std::io::Result<usize>> {
        if let Some(e) = self.closed() {
            return Poll::Ready(Err(e));
        }
        Pin::new(&mut self.stream).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        if let Some(e) = self.closed() {
            return Poll::Ready(Err(e));
        }
        Pin::new(&mut self.stream).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}

/// A connection's HTTP service, `inner`, which counts the requests it is answering in `in_flight`, so the connection
/// is not idle while it answers one.
/// A response's body is written after the answer is given and is not counted, so the connection may look idle while
/// it is written; a receiver's answers are small, so this does not matter.
#[derive(Clone)]
pub(super) struct Counting<S> {
    pub(super) inner: S,
    pub(super) in_flight: Arc<AtomicUsize>,
}

impl<S: tower_service::Service<R>, R> tower_service::Service<R> for Counting<S> {
    type Response = S::Response;
    type Error = S::Error;
    type Future = Answering<S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: R) -> Self::Future {
        self.in_flight.fetch_add(1, Ordering::Relaxed);
        Answering {
            answer: Box::pin(self.inner.call(request)),
            in_flight: self.in_flight.clone(),
        }
    }
}

/// A request's answer, counted in flight until it is given or dropped.
pub(super) struct Answering<F> {
    answer: Pin<Box<F>>,
    in_flight: Arc<AtomicUsize>,
}

impl<F: std::future::Future> std::future::Future for Answering<F> {
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        self.answer.as_mut().poll(cx)
    }
}

impl<F> Drop for Answering<F> {
    fn drop(&mut self) {
        self.in_flight.fetch_sub(1, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits(max: usize, per_sender: Option<usize>) -> ConnectionLimits {
        ConnectionLimits {
            max: Some(max),
            per_sender,
            idle: None,
        }
    }

    fn ip(last: u8) -> IpAddr {
        IpAddr::from([10, 0, 0, last])
    }

    /// A receiver's connections with a budget of their own, so no other test's draw on this process's.
    fn alone(limits: ConnectionLimits) -> Arc<Mutex<Connections>> {
        Connections::within(limits, &Budget::new(1_000))
    }

    fn admit(c: &Arc<Mutex<Connections>>, peer: IpAddr) -> (Vec<(IpAddr, Closed)>, Result<Place, Refused>) {
        Connections::admit(c, peer)
    }

    /// Ages connection `id` by `by`, as though it read nothing that long.
    fn age(c: &Arc<Mutex<Connections>>, id: u64, by: Duration) {
        let mut c = c.lock().unwrap();
        // The clock runs `by` ahead, and every other connection reads at its new now.
        c.epoch -= by;
        for (other, o) in &c.open {
            if *other != id && o.read.load(Ordering::Relaxed) != BUSY {
                o.read.fetch_add(by.as_millis() as u64, Ordering::Relaxed);
            }
        }
    }

    #[test]
    fn under_the_cap_every_connection_is_admitted() {
        let c = alone(limits(3, None));
        for i in 0..3 {
            let (closed, place) = admit(&c, ip(i));
            assert!(closed.is_empty());
            assert!(place.is_ok());
        }
    }

    #[test]
    fn at_the_cap_a_new_connection_is_refused_while_none_has_idled_long_enough() {
        let c = alone(limits(2, None));
        admit(&c, ip(1)).1.unwrap();
        admit(&c, ip(2)).1.unwrap();
        let (closed, place) = admit(&c, ip(3));
        assert!(closed.is_empty());
        assert_eq!(place.err(), Some(Refused::Full));
    }

    #[test]
    fn at_the_cap_a_new_connection_takes_the_place_of_the_one_idle_longest() {
        let c = alone(limits(2, None));
        let first = admit(&c, ip(1)).1.unwrap();
        admit(&c, ip(2)).1.unwrap();
        age(&c, first.id, EVICTABLE_AFTER);
        let (closed, place) = admit(&c, ip(3));
        assert_eq!(closed, vec![(ip(1), Closed::ForNew)]);
        assert!(place.is_ok());
        assert_eq!(c.lock().unwrap().open.len(), 2);
    }

    #[test]
    fn a_sender_at_its_own_cap_gives_up_its_own_idlest_and_no_one_elses() {
        let c = alone(limits(10, Some(2)));
        let other = admit(&c, ip(9)).1.unwrap();
        let first = admit(&c, ip(1)).1.unwrap();
        admit(&c, ip(1)).1.unwrap();
        age(&c, other.id, EVICTABLE_AFTER * 2);
        assert_eq!(admit(&c, ip(1)).1.err(), Some(Refused::FullFromSender));
        age(&c, first.id, EVICTABLE_AFTER);
        let (closed, place) = admit(&c, ip(1));
        assert_eq!(closed, vec![(ip(1), Closed::ForNewFromSender)]);
        assert!(place.is_ok());
        assert!(c.lock().unwrap().open.contains_key(&other.id));
    }

    #[test]
    fn the_idle_timeout_closes_only_what_has_idled_that_long() {
        let c = alone(ConnectionLimits {
            max: Some(10),
            per_sender: None,
            idle: Some(Duration::from_secs(30)),
        });
        let quiet = admit(&c, ip(1)).1.unwrap();
        admit(&c, ip(2)).1.unwrap();
        age(&c, quiet.id, Duration::from_secs(30));
        let mut c = c.lock().unwrap();
        assert_eq!(c.close_idle(), vec![ip(1)]);
        assert_eq!(c.open.len(), 1);
    }

    /// A receiver that stops reading a connection, as it does while it is full, does not make it idle.
    #[test]
    fn a_connection_the_receiver_is_not_waiting_on_never_idles() {
        let c = alone(ConnectionLimits {
            max: Some(1),
            per_sender: None,
            idle: Some(Duration::from_secs(30)),
        });
        let busy = admit(&c, ip(1)).1.unwrap();
        busy.read.store(BUSY, Ordering::Relaxed);
        c.lock().unwrap().epoch -= EVICTABLE_AFTER * 2;
        assert!(c.lock().unwrap().close_idle().is_empty());
        assert_eq!(admit(&c, ip(2)).1.err(), Some(Refused::Full));
    }

    #[test]
    fn without_an_idle_timeout_nothing_idles_out() {
        let c = alone(limits(10, None));
        let quiet = admit(&c, ip(1)).1.unwrap();
        age(&c, quiet.id, Duration::from_secs(86_400));
        let mut c = c.lock().unwrap();
        assert!(c.close_idle().is_empty());
        assert!(c.idle_check().is_none());
    }

    /// With the worker's budget spent, a new connection on one receiver takes the place of the connection idle
    /// longest on any, and is refused while none has idled long enough.
    #[test]
    fn the_receivers_of_a_worker_share_its_budget() {
        let budget = Budget::new(3);
        let (a, b) = (
            Connections::within(ConnectionLimits::default(), &budget),
            Connections::within(ConnectionLimits::default(), &budget),
        );
        let quiet = admit(&a, ip(1)).1.unwrap();
        admit(&a, ip(2)).1.unwrap();
        admit(&b, ip(3)).1.unwrap();
        assert_eq!(admit(&b, ip(4)).1.err(), Some(Refused::WorkerFull));
        assert_eq!(budget.held.load(Ordering::Relaxed), 3);
        age(&a, quiet.id, EVICTABLE_AFTER);
        let (closed, place) = admit(&b, ip(4));
        assert_eq!(closed, vec![(ip(1), Closed::ForNewOnWorker)]);
        assert!(place.is_ok());
        assert_eq!(a.lock().unwrap().open.len(), 1);
        assert_eq!(b.lock().unwrap().open.len(), 2);
        assert_eq!(budget.held.load(Ordering::Relaxed), 3);
    }

    /// A receiver that ends gives up its connections' places in the budget.
    #[test]
    fn a_connection_that_ends_gives_its_place_back() {
        let budget = Budget::new(1);
        let a = Connections::within(ConnectionLimits::default(), &budget);
        let place = admit(&a, ip(1)).1.unwrap();
        assert_eq!(admit(&a, ip(2)).1.err(), Some(Refused::WorkerFull));
        a.lock().unwrap().ended(place.id);
        assert!(admit(&a, ip(2)).1.is_ok());
    }

    #[test]
    fn the_budget_keeps_a_quarter_of_the_limit_up_to_its_reserve() {
        assert_eq!(OpenFiles::within(Some(1_024)).budget, 768);
        assert_eq!(OpenFiles::within(Some(524_288)).budget, 524_288 - 8_192);
        assert_eq!(OpenFiles::within(Some(u64::MAX)).budget, ConnectionLimits::MOST);
        assert_eq!(OpenFiles::within(None).budget, ConnectionLimits::MOST);
    }

    #[cfg(unix)]
    #[test]
    fn raising_the_limit_takes_the_soft_one_to_the_hard_one() {
        let raised = raise_open_file_limit().unwrap();
        let now = open_file_limit().unwrap();
        assert_eq!(now.rlim_cur, now.rlim_max);
        assert_eq!(raised, open_files());
    }
}
