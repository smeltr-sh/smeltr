//! What bounds a connection to the leader while it waits on its client.
//!
//! A client holds a connection, and with it a task and a descriptor, from its TLS handshake until the server
//! closes it. The handshake itself is bounded by the acceptor. After it, hyper bounds the wait for a request's
//! headers only when it is given a timer, and pings an idle HTTP/2 connection only when told how often; given
//! neither, a client that sends one byte and stops is kept until the leader stops. At the process's descriptor
//! limit, accepts fail: workers' heartbeats are refused and nobody can sign in.

use std::time::Duration;

use hyper_util::rt::{TokioExecutor, TokioTimer};
use hyper_util::server::conn::auto::Builder;

/// How long a client has to send a request's headers: from connecting, and from the end of its previous
/// request on the same connection, so it bounds an idle keep-alive too.
pub const HEADER_READ: Duration = Duration::from_secs(30);

/// How often an HTTP/2 connection with nothing in flight is pinged.
const H2_PING: Duration = Duration::from_secs(30);

/// How long a ping may go unanswered before its connection is closed.
const H2_PING_TIMEOUT: Duration = Duration::from_secs(20);

/// `builder`, the HTTP settings of an `axum_server::Server`, bounded so that no connection waits on its client
/// for longer than `header_read` between requests, over HTTP/1 by that timeout and over HTTP/2 by pings.
pub fn bound(builder: &mut Builder<TokioExecutor>, header_read: Duration) {
    builder
        .http1()
        .timer(TokioTimer::new())
        .header_read_timeout(header_read);
    builder
        .http2()
        .timer(TokioTimer::new())
        .keep_alive_interval(H2_PING)
        .keep_alive_timeout(H2_PING_TIMEOUT);
}
