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

use std::collections::HashMap;
use std::net::IpAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
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
    epoch: Instant,
    next: u64,
    open: HashMap<u64, Open>,
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
    pub(super) fn new(limits: ConnectionLimits) -> Arc<Mutex<Connections>> {
        Arc::new(Mutex::new(Connections {
            limits,
            epoch: Instant::now(),
            next: 0,
            open: HashMap::new(),
        }))
    }

    fn now(&self) -> u64 {
        self.epoch.elapsed().as_millis() as u64
    }

    /// Makes room for a connection from `peer`, closing the connection idle longest where the limits say. Returns the
    /// connections it closed and the new one's place, or why it is refused.
    pub(super) fn admit(&mut self, peer: IpAddr) -> (Vec<(IpAddr, Closed)>, Result<Place, Refused>) {
        let mut closed = Vec::new();
        if let Some(cap) = self.limits.per_sender
            && self.open.values().filter(|o| o.peer == peer).count() >= cap
        {
            match self.close_idlest(Some(peer)) {
                Some(gone) => closed.push((gone, Closed::ForNewFromSender)),
                None => return (closed, Err(Refused::FullFromSender)),
            }
        }
        if self.open.len() >= self.limits.max {
            match self.close_idlest(None) {
                Some(gone) => closed.push((gone, Closed::ForNew)),
                None => return (closed, Err(Refused::Full)),
            }
        }
        let id = self.next;
        self.next += 1;
        let read = Arc::new(AtomicU64::new(self.now()));
        let shut = Arc::new(Shut::default());
        let in_flight = Arc::new(AtomicUsize::new(0));
        self.open.insert(
            id,
            Open {
                peer,
                read: read.clone(),
                in_flight: in_flight.clone(),
                shut: shut.clone(),
                abort: None,
            },
        );
        let place = Place {
            id,
            read,
            in_flight,
            shut,
            epoch: self.epoch,
        };
        (closed, Ok(place))
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
        self.open.remove(&id);
    }

    /// Closes the connection, of `peer`'s where given, that has been idle longest, if it has been idle for
    /// [`EVICTABLE_AFTER`]. Returns the sender of the connection it closed.
    fn close_idlest(&mut self, peer: Option<IpAddr>) -> Option<IpAddr> {
        let now = self.now();
        let (&id, open) = (self.open.iter())
            .filter(|(_, o)| peer.is_none_or(|p| o.peer == p))
            .min_by_key(|(_, o)| o.idle_since())?;
        if now.saturating_sub(open.idle_since()) < EVICTABLE_AFTER.as_millis() as u64 {
            return None;
        }
        self.close(id)
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
        let open = self.open.remove(&id)?;
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
            max,
            per_sender,
            idle: None,
        }
    }

    fn ip(last: u8) -> IpAddr {
        IpAddr::from([10, 0, 0, last])
    }

    /// Ages connection `id` by `by`, as though it read nothing that long.
    fn age(c: &mut Connections, id: u64, by: Duration) {
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
        let c = Connections::new(limits(3, None));
        let mut c = c.lock().unwrap();
        for i in 0..3 {
            let (closed, place) = c.admit(ip(i));
            assert!(closed.is_empty());
            assert!(place.is_ok());
        }
    }

    #[test]
    fn at_the_cap_a_new_connection_is_refused_while_none_has_idled_long_enough() {
        let c = Connections::new(limits(2, None));
        let mut c = c.lock().unwrap();
        c.admit(ip(1)).1.unwrap();
        c.admit(ip(2)).1.unwrap();
        let (closed, place) = c.admit(ip(3));
        assert!(closed.is_empty());
        assert_eq!(place.err(), Some(Refused::Full));
    }

    #[test]
    fn at_the_cap_a_new_connection_takes_the_place_of_the_one_idle_longest() {
        let c = Connections::new(limits(2, None));
        let mut c = c.lock().unwrap();
        let first = c.admit(ip(1)).1.unwrap();
        c.admit(ip(2)).1.unwrap();
        age(&mut c, first.id, EVICTABLE_AFTER);
        let (closed, place) = c.admit(ip(3));
        assert_eq!(closed, vec![(ip(1), Closed::ForNew)]);
        assert!(place.is_ok());
        assert_eq!(c.open.len(), 2);
    }

    #[test]
    fn a_sender_at_its_own_cap_gives_up_its_own_idlest_and_no_one_elses() {
        let c = Connections::new(limits(10, Some(2)));
        let mut c = c.lock().unwrap();
        let other = c.admit(ip(9)).1.unwrap();
        let first = c.admit(ip(1)).1.unwrap();
        c.admit(ip(1)).1.unwrap();
        age(&mut c, other.id, EVICTABLE_AFTER * 2);
        assert_eq!(c.admit(ip(1)).1.err(), Some(Refused::FullFromSender));
        age(&mut c, first.id, EVICTABLE_AFTER);
        let (closed, place) = c.admit(ip(1));
        assert_eq!(closed, vec![(ip(1), Closed::ForNewFromSender)]);
        assert!(place.is_ok());
        assert!(c.open.contains_key(&other.id));
    }

    #[test]
    fn the_idle_timeout_closes_only_what_has_idled_that_long() {
        let c = Connections::new(ConnectionLimits {
            max: 10,
            per_sender: None,
            idle: Some(Duration::from_secs(30)),
        });
        let mut c = c.lock().unwrap();
        let quiet = c.admit(ip(1)).1.unwrap();
        c.admit(ip(2)).1.unwrap();
        age(&mut c, quiet.id, Duration::from_secs(30));
        assert_eq!(c.close_idle(), vec![ip(1)]);
        assert_eq!(c.open.len(), 1);
    }

    /// A receiver that stops reading a connection, as it does while it is full, does not make it idle.
    #[test]
    fn a_connection_the_receiver_is_not_waiting_on_never_idles() {
        let c = Connections::new(ConnectionLimits {
            max: 1,
            per_sender: None,
            idle: Some(Duration::from_secs(30)),
        });
        let mut c = c.lock().unwrap();
        let busy = c.admit(ip(1)).1.unwrap();
        busy.read.store(BUSY, Ordering::Relaxed);
        c.epoch -= EVICTABLE_AFTER * 2;
        assert!(c.close_idle().is_empty());
        assert_eq!(c.admit(ip(2)).1.err(), Some(Refused::Full));
    }

    #[test]
    fn without_an_idle_timeout_nothing_idles_out() {
        let c = Connections::new(limits(10, None));
        let mut c = c.lock().unwrap();
        let quiet = c.admit(ip(1)).1.unwrap();
        age(&mut c, quiet.id, Duration::from_secs(86_400));
        assert!(c.close_idle().is_empty());
        assert!(c.idle_check().is_none());
    }
}
