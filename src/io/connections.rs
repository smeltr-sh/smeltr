//! The connections a receiver that streams holds open, held to its `ConnectionLimits`. Each connection's stream is
//! wrapped so every read marks it live. At the cap, a new connection takes the place of the one idle longest, if that
//! one has been idle at least [`EVICTABLE_AFTER`]; otherwise the new one is closed. So a quiet sender keeps its
//! connection as long as there is room, and a sender that opens connections and leaves them idle cannot take the room
//! every other sender needs.

use std::collections::HashMap;
use std::net::IpAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::task::AbortHandle;

use crate::plan::topology::ConnectionLimits;

/// How long a connection must have read nothing before a new one at the cap may take its place. Long enough that a
/// sender between two messages keeps its connection.
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

struct Open {
    peer: IpAddr,
    /// When it last read, in milliseconds since `Connections::epoch`.
    read: Arc<AtomicU64>,
    abort: Option<AbortHandle>,
}

/// The connections a receiver holds, by an id of their own.
pub(super) struct Connections {
    limits: ConnectionLimits,
    epoch: Instant,
    next: u64,
    open: HashMap<u64, Open>,
}

/// A connection's place among the receiver's: its id, and the clock its stream marks reads on.
pub(super) struct Place {
    pub(super) id: u64,
    pub(super) read: Arc<AtomicU64>,
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

    /// Makes room for a connection from `peer`, closing the idlest where the limits say. The connections it closed, and
    /// its place; or why it is refused.
    pub(super) fn admit(&mut self, peer: IpAddr) -> (Vec<(IpAddr, Closed)>, Result<Place, Refused>) {
        let mut closed = Vec::new();
        if let Some(cap) = self.limits.per_sender {
            if self.open.values().filter(|o| o.peer == peer).count() >= cap {
                match self.close_idlest(Some(peer)) {
                    Some(gone) => closed.push((gone, Closed::ForNewFromSender)),
                    None => return (closed, Err(Refused::FullFromSender)),
                }
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
        self.open.insert(
            id,
            Open {
                peer,
                read: read.clone(),
                abort: None,
            },
        );
        let place = Place {
            id,
            read,
            epoch: self.epoch,
        };
        (closed, Ok(place))
    }

    /// Gives connection `id` the handle that ends its task, once it runs.
    pub(super) fn started(&mut self, id: u64, abort: AbortHandle) {
        if let Some(open) = self.open.get_mut(&id) {
            open.abort = Some(abort);
        }
    }

    /// Connection `id` has ended.
    pub(super) fn ended(&mut self, id: u64) {
        self.open.remove(&id);
    }

    /// Closes the connection, of `peer`'s where given, that has read nothing longest, if it has read nothing for
    /// [`EVICTABLE_AFTER`]. Its sender.
    fn close_idlest(&mut self, peer: Option<IpAddr>) -> Option<IpAddr> {
        let now = self.now();
        let (&id, open) = (self.open.iter())
            .filter(|(_, o)| peer.is_none_or(|p| o.peer == p))
            .min_by_key(|(_, o)| o.read.load(Ordering::Relaxed))?;
        if now.saturating_sub(open.read.load(Ordering::Relaxed)) < EVICTABLE_AFTER.as_millis() as u64 {
            return None;
        }
        self.close(id)
    }

    /// Closes every connection that has read nothing for the idle timeout. Their senders.
    pub(super) fn close_idle(&mut self) -> Vec<IpAddr> {
        let Some(idle) = self.limits.idle else {
            return Vec::new();
        };
        let now = self.now();
        let idle: Vec<u64> = (self.open.iter())
            .filter(|(_, o)| now.saturating_sub(o.read.load(Ordering::Relaxed)) >= idle.as_millis() as u64)
            .map(|(&id, _)| id)
            .collect();
        idle.into_iter().filter_map(|id| self.close(id)).collect()
    }

    fn close(&mut self, id: u64) -> Option<IpAddr> {
        let open = self.open.remove(&id)?;
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

/// A stream that marks its place read each time it reads.
pub(super) struct Marked<S> {
    stream: S,
    read: Arc<AtomicU64>,
    epoch: Instant,
}

impl<S> Marked<S> {
    pub(super) fn new(stream: S, place: &Place) -> Marked<S> {
        Marked {
            stream,
            read: place.read.clone(),
            epoch: place.epoch,
        }
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for Marked<S> {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        let before = buf.filled().len();
        let polled = Pin::new(&mut self.stream).poll_read(cx, buf);
        if matches!(polled, Poll::Ready(Ok(()))) && buf.filled().len() > before {
            self.read
                .store(self.epoch.elapsed().as_millis() as u64, Ordering::Relaxed);
        }
        polled
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for Marked<S> {
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.stream).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.stream).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(cx)
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
            if *other != id {
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
