//! What arrives at a receiver before it frames any of it: each read of a sender's connection, after TLS, and each
//! datagram. A capture of a receiver as sent reads this, so an author sees what a sender wrote when the receiver cannot
//! make records of it.
//!
//! ```text
//!   sender ── TCP ──► TLS ──► Tapped: each read ──► framer ──► records
//!                      │         └──► tap
//!                      └ failed ───► tap
//!   sender ── UDP ──► datagram ──► tap, then cut into records
//! ```
//!
//! A receiver carries its tap whether or not anyone is capturing: what it costs a read when nobody is, is one atomic
//! load (`RawTap::watching`).

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use crate::plan::topology::Transport;

/// One thing that arrived at a receiver: a read of a connection or a datagram, before any framing, or a connection the
/// receiver closed before reading it.
#[derive(Clone, Copy)]
pub struct RawRead<'a> {
    /// The sender's address, as a record's `__source.remote` names it.
    pub remote: &'a str,
    pub transport: Transport,
    /// Whether the connection came over TLS. A datagram never does.
    pub tls: bool,
    /// What was read, as decrypted. Empty on what the receiver refused unread.
    pub bytes: &'a [u8],
    /// Why the receiver refused what arrived unread: a failed TLS handshake, or a sender its `allowFrom` refuses.
    pub closed: Option<&'a str>,
}

/// Where a receiver copies what arrives while someone captures it as sent.
pub trait RawTap: Send + Sync {
    /// Whether anyone wants what arrives now. Asked on every read, so it answers without taking a lock.
    fn watching(&self) -> bool;

    fn offer(&self, read: RawRead<'_>);
}

/// A sender's connection, after TLS, whose every read is offered to `tap` as it is read.
pub(super) struct Tapped<S> {
    pub(super) inner: S,
    /// `None` serves the connection untapped.
    pub(super) tap: Option<Arc<dyn RawTap>>,
    pub(super) remote: Arc<str>,
    pub(super) tls: bool,
}

impl<S: AsyncRead + Unpin> AsyncRead for Tapped<S> {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        let before = buf.filled().len();
        let polled = Pin::new(&mut self.inner).poll_read(cx, buf);
        let read = &buf.filled()[before..];
        if let Some(tap) = self.tap.as_ref().filter(|tap| !read.is_empty() && tap.watching()) {
            tap.offer(RawRead {
                remote: &self.remote,
                transport: Transport::Tcp,
                tls: self.tls,
                bytes: read,
                closed: None,
            });
        }
        polled
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for Tapped<S> {
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
}

/// Offers a connection `tap` closed unread, from `remote`, for `why`.
pub(super) fn offer_closed(tap: &dyn RawTap, remote: &str, tls: bool, why: &str) {
    if tap.watching() {
        tap.offer(RawRead {
            remote,
            transport: Transport::Tcp,
            tls,
            bytes: &[],
            closed: Some(why),
        });
    }
}

/// An HTTP server's acceptor, `inner`, whose connections are [`Tapped`] once it has accepted them, and whose failed
/// handshakes are offered to `tap`.
#[derive(Clone)]
pub(super) struct Tapping<A> {
    pub(super) inner: A,
    pub(super) tap: Option<Arc<dyn RawTap>>,
    pub(super) tls: bool,
}

type Accepting<T, S> = Pin<Box<dyn Future<Output = std::io::Result<(T, S)>> + Send>>;

impl<A, S> axum_server::accept::Accept<tokio::net::TcpStream, S> for Tapping<A>
where
    A: axum_server::accept::Accept<tokio::net::TcpStream, S>,
    A::Future: Send + 'static,
    A::Stream: Send,
    A::Service: Send,
{
    type Stream = Tapped<A::Stream>;
    type Service = A::Service;
    type Future = Accepting<Self::Stream, Self::Service>;

    fn accept(&self, stream: tokio::net::TcpStream, service: S) -> Self::Future {
        // Named as `serve_streams` names a sender: a dual-stack socket's IPv4 sender as the IPv4 address it is.
        let remote: Arc<str> = match stream.peer_addr() {
            Ok(peer) => Arc::from(peer.ip().to_canonical().to_string()),
            Err(_) => Arc::from(""),
        };
        let (tap, tls) = (self.tap.clone(), self.tls);
        let accepting = self.inner.accept(stream, service);
        Box::pin(async move {
            match accepting.await {
                Ok((inner, service)) => Ok((
                    Tapped {
                        inner,
                        tap,
                        remote,
                        tls,
                    },
                    service,
                )),
                Err(e) => {
                    if let Some(tap) = &tap {
                        offer_closed(&**tap, &remote, tls, &format!("TLS handshake failed: {e}"));
                    }
                    Err(e)
                }
            }
        })
    }
}
