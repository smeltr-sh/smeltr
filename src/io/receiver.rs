//! A receiver: a source the sender connects to. Every worker of the group listens on the receiver's address, and the
//! sender, or a balancer in front of the group, picks the worker for each connection, so the transport assigns the
//! work (`Source`'s own rule).
//!
//! A sender drops what it sent once it hears success, so a request is answered only once every destination has its
//! records. Until then the sender holds the only other copy, and it sends again when no answer comes: a worker that
//! stops with a request held costs a duplicate, never a loss.
//!
//! ```text
//!   sender ── POST ──► server ── records ──► inbox ──► poll_batch ──► pipelines, destinations
//!     ▲                                                                        │
//!     └──────────── 200 ◄── commit: the watermark passed the request's last record
//! ```
//!
//! The server runs on the process's receiver runtime and only fills the inbox and waits; the reader drains it on its own thread.
//! A full inbox, or a blocked source, answers a new request busy at once, and the bodies being read share a budget the
//! size of the inbox, so what a receiver holds is bounded by twice its inbox and never by how many senders there are.
//! A sender's token is checked before any of its body is read.
//!
//! A receiver whose protocol streams records over a connection it holds open ([`serve_streams`]) has no busy answer:
//! it waits for room before it reads on, and the sender's TCP window fills instead.
//!
//! A runtime is replaced on every deploy, so its supervisor binds each receiver's address and hands the socket to
//! every runtime it starts ([`Sockets`]): across a deploy, a connection waits in the kernel's queue rather than being
//! refused.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::future::Future;
use std::net::{SocketAddr, TcpListener};
use std::sync::{Arc, Condvar, Mutex, OnceLock, Weak};
use std::time::{Duration, Instant};

use aws_lc_rs::constant_time::verify_slices_are_equal;
use axum::body::Bytes;
use axum::extract::{ConnectInfo, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use futures_util::StreamExt;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{Notify, oneshot};

use super::api::Records;
use super::elastic_receiver;
use super::hec;
use super::line_breaker::LineBreaker;
use super::otlp;
use super::prometheus;
use super::s2s_receiver;
use super::syslog_receiver;
use super::{CommitMap, IoError, RecordChunkBuilder, Source, SourceRecord};
use crate::egress::QueuePool;
use crate::format::json::write_json_string;
use crate::format::json_scan::{Next, Reader};
use crate::plan::topology::{
    ApiRecords, HeldSecrets, Identity, Listen, Receiver, ReceiverConfig, Signal, SourceMeta, Transport,
};

/// Where a receiver answers whether it takes requests: 200 while it does, 503 while it is busy, blocked or stopping.
/// What a balancer's health check or a Kubernetes readiness probe asks.
pub const HEALTH_PATH: &str = "/health";

/// What a receiver holds that its reader has not taken. One request may be as large as this
/// (`MaxRequestBytes::MAX`), so a receiver with room takes any request it may be sent.
pub const INBOX_BYTES: usize = 64 * 1024 * 1024;

/// How long a sender has to send a request's whole body. One that sends slower holds read budget other senders need.
const BODY_READ_SECS: u64 = 30;

/// How long a busy answer tells a sender to wait before sending again.
const RETRY_AFTER_SECS: &str = "1";

/// The environment a supervisor names the sockets it hands a runtime in: `address=fd` pairs, comma-separated.
pub const SOCKETS_ENV: &str = "SMELTR_RECEIVER_SOCKETS";

/// What a receiver started from `config` keeps for its turn: the most one request holds, decoded where its protocol
/// decodes it, and for a receiver that streams, one request and the room a connection takes before it reads on.
pub fn reserve(config: &ReceiverConfig) -> usize {
    match config.receiver {
        Receiver::Otlp(_) | Receiver::Prometheus | Receiver::Elastic => INBOX_BYTES,
        Receiver::S2s(_) | Receiver::Syslog(_) => config.max_request_bytes + STREAM_STEP,
        Receiver::Http(_) | Receiver::Hec(_) => config.max_request_bytes,
    }
}

/// What a connection of a receiver that streams takes room for before it reads on, above what it holds: its read and
/// what that read may add to its buffers.
pub(super) const STREAM_STEP: usize = 256 * 1024;

/// Opens a receiver: takes its socket, from this process's supervisor when it handed one over, and serves it. With
/// `pool`, it holds one request of its own and borrows above that from `pool`, which the deployment's receivers share.
pub fn open(
    config: &ReceiverConfig,
    secrets: &HeldSecrets,
    pool: Option<&Arc<QueuePool>>,
) -> Result<Box<dyn Source>, IoError> {
    let shared = || {
        Arc::new(Shared {
            holdings: Holdings::reserving(reserve(config), pool.cloned()),
            ..Shared::default()
        })
    };
    let acknowledging = |name: &String| match &config.receiver {
        Receiver::Hec(hec) => hec.acknowledging.contains(name),
        Receiver::Http(_)
        | Receiver::Otlp(_)
        | Receiver::Prometheus
        | Receiver::S2s(_)
        | Receiver::Syslog(_)
        | Receiver::Elastic => false,
    };
    let tokens = config
        .tokens
        .iter()
        .map(|(name, reference)| {
            let value = super::held(secrets, reference, "the receiver", &format!("token {name:?}"))?;
            // A token pasted with its trailing newline would match no request, since no header value holds one.
            match value.trim() {
                "" => Err(IoError::transient(format!(
                    "receiver token {name:?} is empty, which would admit anyone"
                ))),
                token => Ok(Admits {
                    token: token.as_bytes().to_vec(),
                    acknowledges: acknowledging(name),
                }),
            }
        })
        .collect::<Result<Vec<_>, IoError>>()?;
    if let Receiver::Otlp(signal) = config.receiver {
        let shared = shared();
        let server = Arc::new(Server {
            shared: shared.clone(),
            speaks: Speaks::Otlp,
            tokens,
            max_request_bytes: config.max_request_bytes,
        });
        let joined = OtlpPort::join(config, signal, server, secrets)?;
        return Ok(Box::new(ReceiverSource {
            shared,
            serving: None,
            port: Some(joined),
        }));
    }
    let shared = shared();
    let runtime = receiver_runtime()?;
    if let Receiver::Syslog(syslog) = &config.receiver
        && syslog.transport == Transport::Udp
    {
        let socket = std::net::UdpSocket::from(bound(config.listens())?);
        // `from_std` registers the socket with the runtime it is called in.
        let socket = {
            let _entered = runtime.enter();
            tokio::net::UdpSocket::from_std(socket)
        }
        .map_err(|e| IoError::transient(format!("receiver could not serve {}: {e}", named(config.listens()))))?;
        let handler = syslog_receiver::Handler::new(syslog, config.max_request_bytes);
        let cut = move |datagram: &[u8], peer| handler.datagram(datagram, peer);
        let task = runtime.spawn(serve_datagrams(socket, shared.clone(), config.max_request_bytes, cut));
        return Ok(Box::new(ReceiverSource {
            shared,
            serving: Some(Serving::Task(task)),
            port: None,
        }));
    }
    let listener = TcpListener::from(bound(config.listens())?);
    if let Receiver::S2s(_) | Receiver::Syslog(_) = &config.receiver {
        let tls = match &config.identity {
            Some(identity) => Some(server_tls(identity, secrets)?),
            None => None,
        };
        // `from_std` registers the socket with the runtime it is called in.
        let listener = {
            let _entered = runtime.enter();
            tokio::net::TcpListener::from_std(listener)
        }
        .map_err(|e| IoError::transient(format!("receiver could not serve {}: {e}", config.listen)))?;
        let task = match &config.receiver {
            Receiver::S2s(s2s) => {
                let tokens = tokens.into_iter().map(|admits| admits.token).collect();
                let handler = Arc::new(s2s_receiver::Handler::new(s2s, tokens, config.max_request_bytes));
                let handle = move |connection| handler.clone().serve(connection);
                runtime.spawn(serve_streams(listener, tls, shared.clone(), handle))
            }
            Receiver::Syslog(syslog) => {
                let handler = Arc::new(syslog_receiver::Handler::new(syslog, config.max_request_bytes));
                let handle = move |connection| handler.clone().serve(connection);
                runtime.spawn(serve_streams(listener, tls, shared.clone(), handle))
            }
            _ => unreachable!("only a receiver that streams is served here"),
        };
        return Ok(Box::new(ReceiverSource {
            shared,
            serving: Some(Serving::Task(task)),
            port: None,
        }));
    }
    let speaks = match &config.receiver {
        Receiver::Http(http) => Speaks::Http(http.records.clone()),
        Receiver::Hec(hec) => Speaks::Hec(hec.sourcetypes.clone()),
        Receiver::S2s(_) | Receiver::Otlp(_) | Receiver::Syslog(_) => unreachable!("served above"),
        Receiver::Prometheus => Speaks::Prometheus,
        Receiver::Elastic => Speaks::Elastic,
    };
    let server = Arc::new(Server {
        shared: shared.clone(),
        speaks,
        tokens,
        max_request_bytes: config.max_request_bytes,
    });
    use axum::routing::{get, post};
    let router = match &config.receiver {
        Receiver::Http(http) => {
            // A client percent-encodes what it must of a path, so a path that is not plain ASCII is asked encoded
            // and matched decoded.
            let path = http.path.clone();
            let encoded = move |method: axum::http::Method, uri: axum::http::Uri, s, c, h, b| {
                let decoded = crate::ops::webops::percent_decode(uri.path().as_bytes(), false, true);
                // An encoded `/` is not a separator (RFC 3986), so `/a%2Fb` is not `/a/b`.
                let slash = uri.path().to_ascii_lowercase().contains("%2f");
                let ours = !slash && decoded.as_deref() == Some(path.as_bytes());
                async move {
                    match (ours, method == axum::http::Method::POST) {
                        (true, true) => receive(s, c, h, b).await,
                        (true, false) => ([(header::ALLOW, "POST")], StatusCode::METHOD_NOT_ALLOWED).into_response(),
                        (false, _) => StatusCode::NOT_FOUND.into_response(),
                    }
                }
            };
            axum::Router::new()
                .route(&http.path, post(receive))
                .route(HEALTH_PATH, get(health))
                .fallback(encoded)
        }
        // Each path as Splunk serves it, with and without its version.
        Receiver::Hec(_) => axum::Router::new()
            .route("/services/collector", post(hec_event))
            .route("/services/collector/event", post(hec_event))
            .route("/services/collector/event/1.0", post(hec_event))
            .route("/services/collector/raw", post(hec_raw))
            .route("/services/collector/raw/1.0", post(hec_raw))
            .route("/services/collector/s2s", post(hec_s2s))
            .route("/services/collector/ack", post(hec_ack))
            .route("/services/collector/ack/1.0", post(hec_ack))
            .route(hec::HEALTH_PATH, get(health))
            .route("/services/collector/health/1.0", get(health)),
        Receiver::S2s(_) | Receiver::Otlp(_) | Receiver::Syslog(_) => unreachable!("served above"),
        Receiver::Prometheus => axum::Router::new()
            .route(prometheus::WRITE_PATH, post(prometheus_write))
            .route(HEALTH_PATH, get(health)),
        // A client asks `/` which version it sends to, and a Beat asks `/_license` what it may send under, before
        // either sends; any other path, a template or a lifecycle policy, is one a receiver has none of.
        Receiver::Elastic => axum::Router::new()
            .route("/", get(|s, c, h| elastic_about(s, c, h, elastic_receiver::cluster)))
            .route(
                "/_license",
                get(|s, c, h| elastic_about(s, c, h, elastic_receiver::license)),
            )
            // Elasticsearch takes a path with a slash after it, as Fluent Bit sends one naming a pipeline.
            .route("/_bulk", post(elastic_bulk).put(elastic_bulk))
            .route("/_bulk/", post(elastic_bulk).put(elastic_bulk))
            .route("/:index/_bulk", post(elastic_bulk_into).put(elastic_bulk_into))
            .route("/:index/_bulk/", post(elastic_bulk_into).put(elastic_bulk_into))
            .route(HEALTH_PATH, get(health))
            .fallback(elastic_unknown),
    }
    .with_state(server);
    let server = serve_router(
        runtime,
        listener,
        http_tls(config, secrets)?,
        router,
        config.listen,
        "receiver",
    )?;
    Ok(Box::new(ReceiverSource {
        shared,
        serving: Some(Serving::Server(server)),
        port: None,
    }))
}

/// `listen`'s socket, or why a receiver cannot listen on it.
fn bound(listen: Listen) -> Result<socket2::Socket, IoError> {
    socket(listen).map_err(|e| {
        IoError::transient(format!(
            "receiver could not listen on {}: {e}. Another process may hold the port, or a port below 1024 \
             may need permission this host has not given",
            named(listen)
        ))
    })
}

/// The TLS an HTTP receiver serves under its identity, offering HTTP/2, which gRPC needs, and HTTP/1.1.
fn http_tls(
    config: &ReceiverConfig,
    secrets: &HeldSecrets,
) -> Result<Option<axum_server::tls_rustls::RustlsConfig>, IoError> {
    let Some(identity) = &config.identity else {
        return Ok(None);
    };
    let mut tls = server_tls(identity, secrets)?;
    tls.alpn_protocols = http_alpn();
    Ok(Some(axum_server::tls_rustls::RustlsConfig::from_config(Arc::new(tls))))
}

/// What a server of HTTP offers in its TLS handshake: HTTP/2, which gRPC needs, and HTTP/1.1.
pub(crate) fn http_alpn() -> Vec<Vec<u8>> {
    vec![b"h2".to_vec(), b"http/1.1".to_vec()]
}

/// Serves `router` on `listener` on `runtime`, under TLS when there is some, until the returned handle shuts it down.
/// `serving` names what serves it, for an operator reading the failure.
pub(crate) fn serve_router(
    runtime: &tokio::runtime::Runtime,
    listener: TcpListener,
    tls: Option<axum_server::tls_rustls::RustlsConfig>,
    router: axum::Router,
    address: SocketAddr,
    serving: &str,
) -> Result<axum_server::Handle<SocketAddr>, IoError> {
    // `from_tcp` registers the socket with the runtime it is called in.
    let _entered = runtime.enter();
    let service = router.into_make_service_with_connect_info::<SocketAddr>();
    let handle = axum_server::Handle::new();
    let served = match tls {
        Some(tls) => axum_server::tls_rustls::from_tcp_rustls(listener, tls).map(|server| {
            let server = server.handle(handle.clone());
            runtime.spawn(async move { server.serve(service).await })
        }),
        None => axum_server::from_tcp(listener).map(|server| {
            let server = server.handle(handle.clone());
            runtime.spawn(async move { server.serve(service).await })
        }),
    };
    served
        .map(|_| handle)
        .map_err(|e| IoError::transient(format!("{serving} could not serve {address}: {e}")))
}

/// The runtime every receiver of this process serves on, sized by cores, so a worker's receiver threads stay the same
/// however many receivers it runs. The worker's metrics endpoint serves on it too.
pub(crate) fn receiver_runtime() -> Result<&'static tokio::runtime::Runtime, IoError> {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    if let Some(runtime) = RUNTIME.get() {
        return Ok(runtime);
    }
    let built = tokio::runtime::Builder::new_multi_thread()
        .thread_name("smeltr-receiver")
        // A worker frees what other threads allocated, and they what it did, into pages only its own collect hands
        // back. One gone idle collects nothing, so it collects as it parks.
        .on_thread_park(|| {
            crate::core::buffer::release_if_due();
        })
        .enable_all()
        .build()
        .map_err(|e| IoError::transient(format!("receiver could not start its server: {e}")))?;
    // Two receivers starting at once may both build one; the second is dropped before it serves anything.
    Ok(RUNTIME.get_or_init(|| built))
}

/// What one receiver serves on the receiver runtime. Dropping it ends the receiver's listener and every connection it
/// holds, and leaves the others serving.
enum Serving {
    /// An HTTP server, which ends its connections itself when shut down.
    Server(axum_server::Handle<SocketAddr>),
    /// A task holding its socket and, for a receiver that streams, its connections.
    Task(tokio::task::JoinHandle<()>),
}

impl Drop for Serving {
    fn drop(&mut self) {
        match self {
            Serving::Server(handle) => handle.shutdown(),
            Serving::Task(task) => task.abort(),
        }
    }
}

/// The OTLP receivers of one address: one server on its socket, which hands each export to the receiver of its
/// signal, as an exporter sends every signal to one port. It stops with the last of them.
struct OtlpPort {
    receivers: Arc<OtlpReceivers>,
    /// Dropped with the port, which ends its server.
    _serving: Serving,
}

/// The receivers of a port, by the signal each takes.
#[derive(Default)]
struct OtlpReceivers(Mutex<HashMap<Signal, Arc<Server>>>);

impl OtlpReceivers {
    fn of(&self, signal: Signal) -> Option<Arc<Server>> {
        self.0.lock().unwrap().get(&signal).cloned()
    }
}

/// The ports this process serves OTLP on, by address.
fn otlp_ports() -> &'static Mutex<HashMap<SocketAddr, Weak<OtlpPort>>> {
    static PORTS: OnceLock<Mutex<HashMap<SocketAddr, Weak<OtlpPort>>>> = OnceLock::new();
    PORTS.get_or_init(Default::default)
}

impl OtlpPort {
    /// `server`, a receiver of `signal`, on `config`'s address: on the port another signal's receiver serves there,
    /// or on one it starts. A port is served as the receiver that started it says, since the config plane holds the
    /// receivers of one port to one address and identity.
    fn join(
        config: &ReceiverConfig,
        signal: Signal,
        server: Arc<Server>,
        secrets: &HeldSecrets,
    ) -> Result<Joined, IoError> {
        let mut ports = otlp_ports().lock().unwrap();
        let port = match ports.get(&config.listen).and_then(Weak::upgrade) {
            Some(port) => port,
            None => {
                let port = Arc::new(OtlpPort::serve(config, secrets)?);
                ports.insert(config.listen, Arc::downgrade(&port));
                port
            }
        };
        let mut receivers = port.receivers.0.lock().unwrap();
        if receivers.contains_key(&signal) {
            return Err(IoError::transient(format!(
                "another receiver takes {} on {} still",
                signal.name(),
                config.listen
            )));
        }
        receivers.insert(signal, server.clone());
        drop(receivers);
        Ok(Joined { port, signal, server })
    }

    fn serve(config: &ReceiverConfig, secrets: &HeldSecrets) -> Result<OtlpPort, IoError> {
        use axum::routing::{get, post};

        let listener = TcpListener::from(bound(config.listens())?);
        let tls = http_tls(config, secrets)?;
        let runtime = receiver_runtime()?;
        let receivers = Arc::new(OtlpReceivers::default());
        let mut router = axum::Router::new().route(HEALTH_PATH, get(otlp_health));
        for signal in [Signal::Logs, Signal::Metrics, Signal::Traces] {
            router = router
                .route(
                    otlp::http_path(signal),
                    post(move |State(receivers), peer, headers, body| {
                        otlp_export(signal, receivers, peer, headers, body)
                    }),
                )
                .route(
                    otlp::grpc_path(signal),
                    post(move |State(receivers), peer, headers, body| {
                        otlp_grpc(signal, receivers, peer, headers, body)
                    }),
                );
        }
        let server = serve_router(
            runtime,
            listener,
            tls,
            router.with_state(receivers.clone()),
            config.listen,
            "receiver",
        )?;
        Ok(OtlpPort {
            receivers,
            _serving: Serving::Server(server),
        })
    }
}

/// A receiver's place on its OTLP port, given up when the receiver stops.
struct Joined {
    port: Arc<OtlpPort>,
    signal: Signal,
    server: Arc<Server>,
}

impl Drop for Joined {
    fn drop(&mut self) {
        let mut receivers = self.port.receivers.0.lock().unwrap();
        if receivers
            .get(&self.signal)
            .is_some_and(|s| Arc::ptr_eq(s, &self.server))
        {
            receivers.remove(&self.signal);
        }
    }
}

/// A receiver's half on the reader's thread: the inbox it drains, and the answers its commits release.
pub struct ReceiverSource {
    shared: Arc<Shared>,
    /// Dropped with the source, which ends the server and every connection it holds.
    serving: Option<Serving>,
    /// An OTLP receiver's place on the port it shares with the receivers of other signals.
    port: Option<Joined>,
}

impl Drop for ReceiverSource {
    fn drop(&mut self) {
        self.shared.close();
        self.serving.take();
    }
}

impl Source for ReceiverSource {
    fn poll_batch(
        &mut self,
        max_rows: usize,
        max_bytes: usize,
        timeout: Duration,
    ) -> Result<Vec<SourceRecord>, IoError> {
        let taken = self.shared.take(max_rows, max_bytes, timeout);
        // A receiver speaks one protocol, so a poll's records are all bytes or all rows, of OTLP or of Prometheus.
        if let Some(joined) = &self.port {
            return Ok(rows_chunk(taken, &SourceMeta::otlp(joined.signal)));
        }
        if matches!(taken.first().map(|q| &q.record), Some(Payload::Row(_))) {
            return Ok(rows_chunk(taken, &SourceMeta::prometheus_receiver()));
        }
        let mut out = RecordChunkBuilder::new();
        for q in taken {
            let Payload::Bytes(record) = q.record else {
                unreachable!("a poll's records are all of one protocol")
            };
            out.push(&record, q.offset, 0, q.remote, q.filed.map(Arc::unwrap_or_clone));
        }
        Ok(out.finish())
    }

    fn take_dropped(&mut self) -> u64 {
        self.shared.dropped.swap(0, std::sync::atomic::Ordering::Relaxed)
    }

    fn take_full(&mut self) -> u64 {
        self.shared.holdings.full.swap(0, std::sync::atomic::Ordering::Relaxed)
    }

    fn held_bytes(&self) -> Option<u64> {
        Some(self.shared.holdings.held() as u64)
    }

    fn take_refused(&mut self) -> Vec<(&'static str, u64)> {
        std::mem::take(&mut self.shared.refused.lock().unwrap().unread)
            .into_iter()
            .collect()
    }

    /// Every request whose last record is at or before the watermark is answered. The commit tracker retires a
    /// prefix in poll order, so the highest offset it names covers every record polled before it, whichever sender
    /// it came from.
    fn commit(&mut self, offsets: &CommitMap) -> Result<(), IoError> {
        if let Some(&watermark) = offsets.values().max() {
            self.shared.answer_through(watermark);
        }
        Ok(())
    }

    fn block(&mut self) -> Result<(), IoError> {
        self.shared.state.lock().unwrap().blocked = true;
        Ok(())
    }

    fn release(&mut self) -> Result<(), IoError> {
        self.shared.state.lock().unwrap().blocked = false;
        self.shared.arrived.notify_all();
        self.shared.room.notify_waiters();
        Ok(())
    }
}

/// `taken`, each a typed row, as one chunk of `meta`'s record columns. They are filed under one value
/// (`Shared::take`), which the chunk carries for them all.
fn rows_chunk(taken: Vec<Queued>, meta: &SourceMeta) -> Vec<SourceRecord> {
    use crate::core::column::{Column, DataType, StrBuilder, VarType};
    use otlp::Cell;

    let mut pool = crate::core::buffer::BufferPool::new();
    let rows: Vec<&otlp::Row> = taken
        .iter()
        .map(|q| match &q.record {
            Payload::Row(row) => row,
            Payload::Bytes(_) => unreachable!("a poll's records are all of one protocol"),
        })
        .collect();
    let mismatch = |name: &str, ty: &DataType| -> ! { unreachable!("{name} is a {ty}, and a row gave it otherwise") };
    let columns = (meta.record().iter().enumerate())
        .map(|(at, (name, ty))| {
            let cells = rows.iter().map(|r| r.0[at].as_ref());
            match ty {
                DataType::I64 | DataType::DateTime | DataType::F64 | DataType::Bool => {
                    let mut column = Column::alloc(&mut pool, ty, rows.len());
                    match ty {
                        DataType::Bool => {
                            let out = column.bool_mut().expect("a column of its own type");
                            for (row, cell) in cells.enumerate() {
                                match cell {
                                    Some(Cell::Bool(b)) => _ = crate::core::bits::set_row(out.values, row, *b),
                                    None => out.validity.set_valid(row, false),
                                    Some(_) => mismatch(name, ty),
                                }
                            }
                        }
                        DataType::F64 => {
                            let out = column.f64_mut().expect("a column of its own type");
                            for (row, cell) in cells.enumerate() {
                                match cell {
                                    Some(Cell::Float(f)) => out.values[row] = *f,
                                    None => out.validity.set_valid(row, false),
                                    Some(_) => mismatch(name, ty),
                                }
                            }
                        }
                        _ => {
                            let out = match ty {
                                DataType::DateTime => column.datetime_mut(),
                                _ => column.i64_mut(),
                            }
                            .expect("a column of its own type");
                            for (row, cell) in cells.enumerate() {
                                match cell {
                                    Some(Cell::Int(i)) => out.values[row] = *i,
                                    None => out.validity.set_valid(row, false),
                                    Some(_) => mismatch(name, ty),
                                }
                            }
                        }
                    }
                    column
                }
                DataType::Str | DataType::Variant => {
                    let var = if *ty == DataType::Str {
                        VarType::Str
                    } else {
                        VarType::Variant
                    };
                    fn bytes(cell: Option<&Cell>, var: VarType) -> Result<Option<&[u8]>, ()> {
                        match cell {
                            Some(Cell::Text(t)) if var == VarType::Str => Ok(Some(t.as_bytes())),
                            Some(Cell::Variant(v)) if var == VarType::Variant => Ok(Some(v)),
                            None => Ok(None),
                            Some(_) => Err(()),
                        }
                    }
                    let bytes = |cell| bytes(cell, var).unwrap_or_else(|()| mismatch(name, ty));
                    let mut b = StrBuilder::new(&mut pool, var, rows.len());
                    b.reserve(&mut pool, cells.clone().filter_map(bytes).map(<[u8]>::len).sum());
                    for cell in cells {
                        match bytes(cell) {
                            Some(v) => b.push(v),
                            None => b.push_null(),
                        }
                    }
                    b.finish()
                }
                // An array or struct cell is a variant the decoder wrote in its type's shape.
                DataType::Array(_) | DataType::Struct(..) => {
                    let values = cells.map(|cell| match cell {
                        Some(Cell::Variant(v)) => Some(crate::format::typed::Cell::Variant(v)),
                        None => None,
                        Some(_) => mismatch(name, ty),
                    });
                    let (column, failed) = crate::format::typed::build(values, rows.len(), ty, 0, &mut pool);
                    assert!(failed.is_empty(), "{name}: a row's variant is not a {ty}");
                    column
                }
                _ => unreachable!("a typed record has no {ty} field"),
            }
        })
        .collect();
    let filed = taken.first().and_then(|q| q.filed.clone());
    let chunk = super::RecordChunk::new(columns, Vec::new());
    let chunk = Arc::new(match filed {
        Some(filed) => chunk.filed_under(filed),
        None => chunk,
    });
    taken
        .into_iter()
        .enumerate()
        .map(|(row, q)| SourceRecord {
            chunk: chunk.clone(),
            row: row as u32,
            offset: q.offset,
            partition: 0,
            topic: q.remote,
            headers: None,
        })
        .collect()
}

/// What the server and the reader share.
#[derive(Default)]
struct Shared {
    state: Mutex<Inbox>,
    /// Everything the receiver holds: what its inbox queues, the bodies it reads and what its connections hold.
    holdings: Holdings,
    arrived: Condvar,
    /// Woken when the inbox may take what it could not: bytes taken, the source released, or the receiver closed.
    room: Notify,
    /// Datagrams let go for want of room since the reader last asked.
    dropped: std::sync::atomic::AtomicU64,
    refused: Mutex<Refused>,
}

/// The requests a receiver refused, by reason.
#[derive(Default)]
struct Refused {
    /// Since the reader last asked.
    unread: BTreeMap<&'static str, u64>,
    /// When each reason was last logged, and how many since.
    logged: HashMap<&'static str, (Option<Instant>, u64)>,
}

#[derive(Default)]
struct Inbox {
    /// Requests whose records the reader has not all taken, oldest first.
    queued: VecDeque<Request>,
    /// What `queued` holds in memory, taken from the receiver's holdings.
    queued_bytes: Counted,
    /// The offset the next record takes. One sequence per receiver, so the watermark is one number.
    next_offset: i64,
    /// Each request whose records are all taken, by the offset of its last record.
    answers: BTreeMap<i64, Answer>,
    /// The `ackId`s a HEC receiver has given.
    acks: hec::Acks,
    blocked: bool,
    closed: bool,
}

impl Inbox {
    fn open(&self) -> bool {
        !self.closed && !self.blocked
    }

    fn queue(&mut self, request: Request) {
        self.queued_bytes.add(request.bytes);
        self.queued.push_back(request);
    }
}

/// A request held whole: its records are cut from its body as the reader takes them, so a body of many small records
/// costs what was sent and not a buffer per record.
struct Request {
    records: Cutter,
    /// The record the reader takes next, cut ahead so the last one is known when it is taken.
    next: Cut,
    bytes: Counted,
    remote: Arc<str>,
    answer: Answer,
}

/// Who hears that a request is delivered.
enum Answer {
    /// A sender holding its connection open for the answer.
    Held(oneshot::Sender<()>),
    /// A HEC sender given this `ackId` on its channel, which asks after it.
    Acked(Arc<str>, u64),
}

/// What cuts a request's records from its body.
pub(super) enum Cutter {
    Records(Records),
    /// Lines, each filed under what the request names.
    Filed(Records, Arc<hec::Filed>),
    Events(hec::Events),
    /// Records cut already, each filed under its own.
    Each(s2s_receiver::Cuts),
    /// Records cut already, filed under nothing.
    Messages(syslog_receiver::Messages),
    Otlp(otlp::Export),
    Samples(prometheus::Samples),
    Documents(elastic_receiver::Documents),
}

/// One record cut from a request, and what it is filed under, where its protocol files records. The records a
/// request files under one value share it.
#[derive(Default)]
struct Cut {
    record: Payload,
    filed: Option<Arc<hec::Filed>>,
}

/// What a record holds: the bytes it was sent as, or, where its protocol types them, its fields.
enum Payload {
    Bytes(Vec<u8>),
    Row(otlp::Row),
}

impl Default for Payload {
    fn default() -> Self {
        Payload::Bytes(Vec::new())
    }
}

impl Cut {
    /// What the record holds once taken, with what it is filed under. A row's chunk shares that, but a reader that
    /// needs it as rows copies it to each, so a poll is bounded by what its rows hold so copied.
    fn size(&self) -> usize {
        let record = match &self.record {
            Payload::Bytes(bytes) => bytes.len(),
            Payload::Row(row) => row.size(),
        };
        let filed = self.filed.iter().flat_map(|f| f.iter());
        record + filed.map(|(k, v)| k.len() + v.len()).sum::<usize>()
    }

    /// Whether this record is filed under the very value `queued` is.
    fn files_with(&self, queued: &Queued) -> bool {
        match (&self.filed, &queued.filed) {
            (Some(a), Some(b)) => Arc::ptr_eq(a, b),
            (a, b) => a.is_none() && b.is_none(),
        }
    }
}

impl Cutter {
    fn next_record(&mut self) -> Option<Cut> {
        match self {
            Cutter::Records(records) => records.next_record().map(|record| Cut {
                record: Payload::Bytes(record),
                filed: None,
            }),
            Cutter::Filed(records, filed) => records.next_record().map(|record| Cut {
                record: Payload::Bytes(record),
                filed: Some(filed.clone()),
            }),
            Cutter::Events(events) => events.next_record().map(|(record, filed)| Cut {
                record: Payload::Bytes(record),
                filed: Some(Arc::new(filed)),
            }),
            Cutter::Otlp(export) => export.next_record().map(|(row, filed)| Cut {
                record: Payload::Row(row),
                filed: Some(filed),
            }),
            Cutter::Documents(documents) => documents.next_record().map(|(record, filed)| Cut {
                record: Payload::Bytes(record),
                filed: Some(Arc::new(filed)),
            }),
            Cutter::Samples(samples) => samples.next_record().map(|row| Cut {
                record: Payload::Row(row),
                filed: None,
            }),
            Cutter::Each(records) => records.next().map(|(record, filed)| Cut {
                record: Payload::Bytes(record),
                filed: Some(filed),
            }),
            Cutter::Messages(records) => records.next().map(|record| Cut {
                record: Payload::Bytes(record),
                filed: None,
            }),
        }
    }
}

struct Queued {
    record: Payload,
    filed: Option<Arc<hec::Filed>>,
    offset: i64,
    /// The sender's address, which is the record's `__source.remote`.
    remote: Arc<str>,
}

/// What became of a request offered to the inbox.
enum Admitted {
    /// It held no records, so there is nothing to wait for.
    Now,
    /// Answered once its last record is committed. A sender dropped without a send is a receiver stopping.
    Later(oneshot::Receiver<()>),
    /// Given this `ackId` on its channel, delivered once its last record is committed.
    Acked(u64),
    /// No room, or the source is blocked or stopping. The sender is told to send it again.
    Busy,
}

impl Shared {
    /// Count `answer` if it refuses `peer`'s request, and log it, once per reason every [`crate::logs::RELOG_EVERY`]
    /// with how many were refused since. Busy and late are counted, not logged: they are this receiver falling
    /// behind, which its source's block already reports.
    fn answered(&self, answer: Response, peer: SocketAddr) -> Response {
        let Some(reason) = refusal(&answer) else {
            return answer;
        };
        let mut refused = self.refused.lock().unwrap();
        *refused.unread.entry(reason).or_default() += 1;
        if matches!(reason, "busy" | "late") {
            return answer;
        }
        let (since, count) = refused.logged.entry(reason).or_insert((None, 0));
        *count += 1;
        if since.is_none_or(|at| at.elapsed() >= crate::logs::RELOG_EVERY) {
            tracing::warn!(
                target: "worker.receiver_refused",
                subject = %peer.ip().to_canonical(),
                reason,
                status = answer.status().as_u16(),
                refused = *count,
                "a receiver refused a sender's request; refused counts every sender's since this was last logged"
            );
            *since = Some(Instant::now());
            *count = 0;
        }
        answer
    }

    /// Offers a request whose body, of `bytes`, `records` cuts. One on a `channel` is given an `ackId` there rather
    /// than held. `charge` is what the caller already took of the holdings for it, which passes to the inbox: the rest
    /// is taken here, and what it took beyond `bytes`, or all of it when the request is not queued, is given back.
    fn admit(
        &self,
        mut records: Cutter,
        bytes: usize,
        remote: Arc<str>,
        channel: Option<&Arc<str>>,
        mut charge: Charge<'_>,
    ) -> Admitted {
        let next = records.next_record();
        let mut inbox = self.state.lock().unwrap();
        let Some(next) = next else {
            // Nothing to deliver: an `ackId` asked after is already true.
            let Some(channel) = channel else {
                return Admitted::Now;
            };
            let Some(id) = inbox.acks.give(channel, Instant::now()) else {
                return Admitted::Busy;
            };
            inbox.acks.deliver(channel, id);
            return Admitted::Acked(id);
        };
        if !inbox.open() {
            return Admitted::Busy;
        }
        if !charge.resize(bytes) {
            self.holdings.note_full();
            return Admitted::Busy;
        }
        let (answer, admitted) = match channel {
            None => {
                let (answer, rx) = oneshot::channel();
                (Answer::Held(answer), Admitted::Later(rx))
            }
            Some(channel) => match inbox.acks.give(channel, Instant::now()) {
                Some(id) => (Answer::Acked(channel.clone(), id), Admitted::Acked(id)),
                None => return Admitted::Busy,
            },
        };
        inbox.queue(Request {
            records,
            next,
            bytes: charge.keep(),
            remote,
            answer,
        });
        drop(inbox);
        self.arrived.notify_all();
        admitted
    }

    /// Offers a request held for its answer, whose `bytes` its connection already took, and waits while the source is
    /// blocked. What it returns fires once the request's last record is committed, as [`Admitted::Later`]'s does.
    async fn admit_waiting(
        &self,
        mut records: Cutter,
        bytes: Counted,
        remote: Arc<str>,
    ) -> Result<oneshot::Receiver<()>, Closed> {
        let (answer, delivered) = oneshot::channel();
        let Some(next) = records.next_record() else {
            self.holdings.give(bytes);
            let _ = answer.send(());
            return Ok(delivered);
        };
        let request = Request {
            records,
            next,
            bytes,
            remote,
            answer: Answer::Held(answer),
        };
        loop {
            // Enabled before the inbox is looked at, so room made between the look and the wait still wakes it.
            let mut room = std::pin::pin!(self.room.notified());
            room.as_mut().enable();
            {
                let mut inbox = self.state.lock().unwrap();
                if inbox.closed {
                    drop(inbox);
                    self.holdings.give(bytes);
                    return Err(Closed);
                }
                if inbox.open() {
                    inbox.queue(request);
                    drop(inbox);
                    self.arrived.notify_all();
                    return Ok(delivered);
                }
            }
            room.await;
        }
    }

    /// Up to `max_rows` records and `max_bytes` of them, the first whatever its size, waiting up to `timeout` for
    /// the first to arrive. Nothing while blocked: a blocked source stops supplying.
    fn take(&self, max_rows: usize, max_bytes: usize, timeout: Duration) -> Vec<Queued> {
        let deadline = Instant::now() + timeout;
        let mut inbox = self.state.lock().unwrap();
        while inbox.queued.is_empty() || inbox.blocked {
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() || inbox.closed {
                return Vec::new();
            }
            inbox = self.arrived.wait_timeout(inbox, left).unwrap().0;
        }
        let inbox = &mut *inbox;
        let mut taken = Vec::new();
        let mut bytes = 0usize;
        let mut freed = false;
        while taken.len() < max_rows.max(1) {
            let Some(front) = inbox.queued.front_mut() else { break };
            let size = front.next.size();
            if let Some(first) = taken.first() {
                // A poll's rows are one chunk, filed under one value.
                let filed_apart = matches!(front.next.record, Payload::Row(_)) && !front.next.files_with(first);
                if filed_apart || bytes + size > max_bytes {
                    break;
                }
            }
            bytes += size;
            let Cut { record, filed } = std::mem::take(&mut front.next);
            let offset = inbox.next_offset;
            inbox.next_offset += 1;
            taken.push(Queued {
                record,
                filed,
                offset,
                remote: front.remote.clone(),
            });
            match front.records.next_record() {
                Some(next) => front.next = next,
                None => {
                    let done = inbox.queued.pop_front().expect("the front was there");
                    inbox.queued_bytes.split_off(done.bytes.total());
                    self.holdings.give(done.bytes);
                    inbox.answers.insert(offset, done.answer);
                    freed = true;
                }
            }
        }
        if freed {
            self.room.notify_waiters();
        }
        taken
    }

    fn answer_through(&self, watermark: i64) {
        let mut inbox = self.state.lock().unwrap();
        let later = inbox.answers.split_off(&(watermark.saturating_add(1)));
        let done = std::mem::replace(&mut inbox.answers, later);
        let mut held = Vec::new();
        for (_, answer) in done {
            match answer {
                Answer::Held(answer) => held.push(answer),
                Answer::Acked(channel, id) => inbox.acks.deliver(&channel, id),
            }
        }
        drop(inbox);
        for answer in held {
            // A sender that hung up is not waiting for this.
            let _ = answer.send(());
        }
    }

    /// Every held request is answered busy, and every new one too: its records were never delivered, and the sender
    /// sends them again. An `ackId` given and not delivered is never delivered.
    fn close(&self) {
        let mut inbox = self.state.lock().unwrap();
        inbox.closed = true;
        inbox.queued.clear();
        self.holdings.give(std::mem::take(&mut inbox.queued_bytes));
        inbox.answers.clear();
        inbox.acks = hec::Acks::default();
        drop(inbox);
        self.arrived.notify_all();
        self.room.notify_waiters();
    }

    fn ready(&self) -> bool {
        let inbox = self.state.lock().unwrap();
        inbox.open() && (inbox.queued.is_empty() || self.holdings.has_room())
    }
}

/// Everything one receiver holds in memory: the requests its inbox queues, the bodies it is reading, and what its
/// connections hold between requests. What it takes it shares with the other receivers: up to its `ceiling`, borrowed
/// from the pool the deployment's receivers share when there is one. Its `reserve`, held above that, is its turn's
/// alone: one taker at a time that found no shared room takes from it, so a receiver whose shared room is spent still
/// finishes a request and moves. Nothing takes past the two, which is what `fit` counts for a receiver.
pub(super) struct Holdings {
    account: Mutex<Account>,
    reserve: usize,
    ceiling: usize,
    pool: Option<Arc<QueuePool>>,
    /// Woken when it gives bytes back or its turn is let go.
    freed: Notify,
    /// Times a taker found no room, once for each wait, since the reader last asked.
    full: std::sync::atomic::AtomicU64,
}

#[derive(Default)]
struct Account {
    counted: Counted,
    /// Whether a taker holds the turn.
    turn: bool,
}

/// Bytes taken of a receiver's holdings: those it shares, and those of its reserve.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct Counted {
    shared: usize,
    reserved: usize,
}

impl Counted {
    pub(super) fn total(self) -> usize {
        self.shared + self.reserved
    }

    fn add(&mut self, other: Counted) {
        self.shared += other.shared;
        self.reserved += other.reserved;
    }

    /// `bytes` of it, its reserve first so the turn has room again soonest.
    fn split_off(&mut self, bytes: usize) -> Counted {
        assert!(bytes <= self.total(), "gives back no more than it took");
        let reserved = bytes.min(self.reserved);
        let part = Counted {
            shared: bytes - reserved,
            reserved,
        };
        self.shared -= part.shared;
        self.reserved -= part.reserved;
        part
    }
}

impl Default for Holdings {
    /// A receiver alone: no pool and no reserve, held to its ceiling.
    fn default() -> Holdings {
        Holdings::reserving(0, None)
    }
}

impl Holdings {
    /// Holdings with `reserve` bytes for its turn and up to `INBOX_BYTES` shared, from `pool` when given.
    pub fn reserving(reserve: usize, pool: Option<Arc<QueuePool>>) -> Holdings {
        Holdings {
            account: Mutex::new(Account::default()),
            reserve,
            ceiling: INBOX_BYTES,
            pool,
            freed: Notify::new(),
            full: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Takes `bytes` more for a taker that holds `counted` and, when `turn`, the turn: shared where they fit, else of
    /// the reserve, taking the turn when it is free. Whether it took them.
    fn take(&self, bytes: usize, counted: &mut Counted, turn: &mut bool) -> bool {
        let mut account = self.account.lock().unwrap();
        let shared =
            account.counted.shared + bytes <= self.ceiling && self.pool.as_ref().is_none_or(|p| p.take(bytes, false));
        if shared {
            account.counted.shared += bytes;
            counted.shared += bytes;
            return true;
        }
        if !*turn && !account.turn {
            account.turn = true;
            *turn = true;
        }
        if *turn && account.counted.reserved + bytes <= self.reserve {
            account.counted.reserved += bytes;
            counted.reserved += bytes;
            return true;
        }
        false
    }

    fn give(&self, counted: Counted) {
        if counted.total() == 0 {
            return;
        }
        let mut account = self.account.lock().unwrap();
        account.counted.shared -= counted.shared;
        account.counted.reserved -= counted.reserved;
        if let Some(pool) = &self.pool {
            pool.give(counted.shared);
        }
        drop(account);
        self.freed.notify_waiters();
    }

    /// Lets the turn go, so the next taker short of room may take it.
    fn release(&self) {
        self.account.lock().unwrap().turn = false;
        self.freed.notify_waiters();
    }

    /// Whether it could ever hold `bytes` for one taker: of its reserve, or shared when the rest is let go.
    fn could_hold(&self, bytes: usize) -> bool {
        bytes <= self.reserve || (bytes <= self.ceiling && self.pool.as_ref().is_none_or(|p| bytes <= p.limit()))
    }

    /// What its takers hold now, shared and of its reserve.
    fn held(&self) -> usize {
        self.account.lock().unwrap().counted.total()
    }

    /// Whether a taker without the turn, or one that would take it, takes another byte.
    fn has_room(&self) -> bool {
        let account = self.account.lock().unwrap();
        (account.counted.shared < self.ceiling && self.pool.as_ref().is_none_or(|p| p.fits(1)))
            || (!account.turn && account.counted.reserved < self.reserve)
    }

    /// Counts a taker that found no room.
    fn note_full(&self) {
        self.full.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Waits for `room` or `freed`, enabled before the caller looked. Another receiver giving back to the pool wakes
    /// neither, so a receiver borrowing looks again after a while.
    async fn wait(
        &self,
        room: std::pin::Pin<&mut tokio::sync::futures::Notified<'_>>,
        freed: std::pin::Pin<&mut tokio::sync::futures::Notified<'_>>,
    ) {
        tokio::select! {
            () = room => {}
            () = freed => {}
            () = tokio::time::sleep(POOL_LOOK), if self.pool.is_some() => {}
        }
    }
}

/// How often a receiver waiting on the pool looks again.
const POOL_LOOK: Duration = Duration::from_millis(100);

/// Bytes taken from a receiver's holdings for one request, given back when dropped.
struct Charge<'a> {
    holdings: &'a Holdings,
    counted: Counted,
    turn: bool,
}

impl<'a> Charge<'a> {
    fn new(holdings: &'a Holdings) -> Charge<'a> {
        Charge {
            holdings,
            counted: Counted::default(),
            turn: false,
        }
    }

    fn bytes(&self) -> usize {
        self.counted.total()
    }

    /// Takes `bytes` more, of the reserve with the turn when nothing is shared.
    fn take(&mut self, bytes: usize) -> bool {
        self.holdings.take(bytes, &mut self.counted, &mut self.turn)
    }

    /// Holds `bytes` in all, taking or giving the difference. Whether it does.
    fn resize(&mut self, bytes: usize) -> bool {
        match bytes.checked_sub(self.bytes()) {
            Some(more) => self.take(more),
            None => {
                let less = self.bytes() - bytes;
                self.holdings.give(self.counted.split_off(less));
                true
            }
        }
    }

    /// Its bytes, no longer given back when it is dropped: whoever keeps them gives them back. Its turn is let go.
    fn keep(&mut self) -> Counted {
        if std::mem::take(&mut self.turn) {
            self.holdings.release();
        }
        std::mem::take(&mut self.counted)
    }
}

impl Drop for Charge<'_> {
    fn drop(&mut self) {
        let counted = self.keep();
        self.holdings.give(counted);
    }
}

/// The receiver stopped: a request waiting for room was never admitted, and its sender sends it again.
#[derive(Debug)]
pub(super) struct Closed;

/// A receiver's server: who it admits, how much of its senders' bodies it reads, and the protocol it speaks.
struct Server {
    shared: Arc<Shared>,
    speaks: Speaks,
    /// Each token a sender may present. Empty admits whoever TLS admitted, and holds each request.
    tokens: Vec<Admits>,
    /// The largest body it reads. A request takes each chunk's bytes from the receiver's holdings as it arrives.
    max_request_bytes: usize,
}

/// A token a receiver admits.
struct Admits {
    token: Vec<u8>,
    /// Whether its senders name a channel and are given an `ackId` there, rather than held.
    acknowledges: bool,
}

/// The protocol a receiver speaks: how a sender presents its token, how a body becomes records, and the words of
/// each answer.
enum Speaks {
    /// A POST of one record, a record per line, or a record per element of a JSON array, with a bearer token.
    Http(ApiRecords),
    /// Splunk's HTTP Event Collector, with a `Splunk` token, and where a raw body of each sourcetype breaks.
    Hec(BTreeMap<String, LineBreaker>),
    /// OpenTelemetry's OTLP exports, with a bearer token.
    Otlp,
    /// Prometheus remote_write, with a bearer token.
    Prometheus,
    /// Elasticsearch's `_bulk` API, with a bearer token, an API key, or a password.
    Elastic,
}

/// What a sender's `Authorization` comes to.
#[derive(Debug, PartialEq)]
enum Token {
    Admitted {
        acknowledges: bool,
    },
    Missing,
    /// Not the scheme the protocol takes.
    Malformed,
    Unknown,
}

/// What a request comes to, which each protocol answers in its own words. A body that does not read as the protocol
/// says is refused where it is cut.
enum Outcome {
    Delivered,
    /// Given this `ackId`, which its sender asks after.
    Acked(u64),
    Refused(Token),
    TooLarge,
    Late,
    Unreadable(String),
    Unsupported(String),
    Busy,
}

impl Server {
    fn token(&self, headers: &HeaderMap) -> Token {
        if self.tokens.is_empty() {
            return Token::Admitted { acknowledges: false };
        }
        let Some(value) = headers.get(header::AUTHORIZATION) else {
            return Token::Missing;
        };
        let Some(presented) = value
            .to_str()
            .ok()
            .and_then(|v| v.split_once(' '))
            .and_then(|(scheme, token)| self.presented(scheme, token.trim()))
        else {
            return Token::Malformed;
        };
        // Every token is compared with every way it is presented, so the time taken says nothing about which one
        // nearly matched.
        let (found, acknowledges) = (self.tokens.iter())
            .flat_map(|admits| presented.iter().map(move |p| (admits, p)))
            .fold((false, false), |(found, acknowledges), (admits, presented)| {
                // An unequal length is refused at once, so timing tells whether the presented token is as long as a
                // configured one, and nothing of the configured one's bytes.
                let matches = verify_slices_are_equal(presented, &admits.token).is_ok();
                (found | matches, acknowledges | (matches & admits.acknowledges))
            });
        match found {
            true => Token::Admitted { acknowledges },
            false => Token::Unknown,
        }
    }

    /// What a sender presents as its token under `scheme`, each way it may hold it, or `None` for a scheme its
    /// protocol does not take.
    fn presented(&self, scheme: &str, token: &str) -> Option<Vec<Vec<u8>>> {
        use base64::Engine;
        let is = |name: &str| scheme.eq_ignore_ascii_case(name);
        let decoded = || base64::engine::general_purpose::STANDARD.decode(token).ok();
        match self.speaks {
            Speaks::Http(_) | Speaks::Otlp | Speaks::Prometheus => is("Bearer").then(|| vec![token.into()]),
            Speaks::Hec(_) => is("Splunk").then(|| vec![token.into()]),
            Speaks::Elastic if is("Bearer") => Some(vec![token.into()]),
            // A client given an API key as `id:key` sends it encoded; the token may be either.
            Speaks::Elastic if is("ApiKey") => Some([Some(token.into()), decoded()].into_iter().flatten().collect()),
            // The user is whoever the sender says; the password is the token.
            Speaks::Elastic if is("Basic") => {
                let pair = decoded()?;
                let colon = pair.iter().position(|b| *b == b':')?;
                Some(vec![pair[colon + 1..].to_vec()])
            }
            Speaks::Elastic => None,
        }
    }

    fn answer(&self, outcome: Outcome) -> Response {
        match self.speaks {
            Speaks::Http(_) => match outcome {
                // An HTTP receiver names no channel, so gives no `ackId`.
                Outcome::Delivered | Outcome::Acked(_) => StatusCode::OK.into_response(),
                Outcome::Refused(_) => {
                    bearer((StatusCode::UNAUTHORIZED, "a receiver token is required\n").into_response())
                }
                Outcome::TooLarge => (StatusCode::PAYLOAD_TOO_LARGE, format!("{}\n", self.too_large())).into_response(),
                Outcome::Late => (StatusCode::REQUEST_TIMEOUT, format!("{}\n", late())).into_response(),
                Outcome::Unreadable(why) => (StatusCode::BAD_REQUEST, format!("{why}\n")).into_response(),
                Outcome::Unsupported(why) => (StatusCode::UNSUPPORTED_MEDIA_TYPE, format!("{why}\n")).into_response(),
                Outcome::Busy => {
                    retry_after((StatusCode::SERVICE_UNAVAILABLE, "busy; send it again\n").into_response())
                }
            },
            Speaks::Hec(_) => match outcome {
                Outcome::Delivered => hec_answer(StatusCode::OK, Some(0), "Success"),
                Outcome::Acked(id) => json(
                    StatusCode::OK,
                    format!(r#"{{"ackId":{id},"code":0,"text":"Success"}}"#).into_bytes(),
                ),
                Outcome::Refused(Token::Missing) => hec_answer(StatusCode::UNAUTHORIZED, Some(2), "Token is required"),
                Outcome::Refused(Token::Malformed) => {
                    hec_answer(StatusCode::UNAUTHORIZED, Some(3), "Invalid authorization")
                }
                Outcome::Refused(_) => hec_answer(StatusCode::FORBIDDEN, Some(4), "Invalid token"),
                Outcome::TooLarge => hec_answer(StatusCode::PAYLOAD_TOO_LARGE, None, &self.too_large()),
                Outcome::Late => hec_answer(StatusCode::REQUEST_TIMEOUT, None, &late()),
                Outcome::Unreadable(_) => hec_answer(StatusCode::BAD_REQUEST, Some(6), "Invalid data format"),
                Outcome::Unsupported(why) => hec_answer(StatusCode::UNSUPPORTED_MEDIA_TYPE, None, &why),
                Outcome::Busy => retry_after(hec_answer(StatusCode::SERVICE_UNAVAILABLE, Some(9), "Server is busy")),
            },
            // Protobuf is OTLP/HTTP's default encoding; an export's own handler answers in the encoding it was sent.
            Speaks::Otlp => otlp_answer(self, outcome, Wire::Http(otlp::Encoding::Protobuf)),
            // A sender sends again on 5xx and 429 and drops a request on any other 4xx, so a request sent too slowly,
            // which was never taken, is answered busy.
            Speaks::Prometheus => match outcome {
                Outcome::Delivered | Outcome::Acked(_) => StatusCode::NO_CONTENT.into_response(),
                Outcome::Refused(_) => {
                    bearer((StatusCode::UNAUTHORIZED, "a receiver token is required\n").into_response())
                }
                Outcome::TooLarge => (StatusCode::PAYLOAD_TOO_LARGE, format!("{}\n", self.too_large())).into_response(),
                Outcome::Unreadable(why) => (StatusCode::BAD_REQUEST, format!("{why}\n")).into_response(),
                Outcome::Unsupported(why) => (StatusCode::UNSUPPORTED_MEDIA_TYPE, format!("{why}\n")).into_response(),
                Outcome::Late => {
                    retry_after((StatusCode::SERVICE_UNAVAILABLE, format!("{}\n", late())).into_response())
                }
                Outcome::Busy => {
                    retry_after((StatusCode::SERVICE_UNAVAILABLE, "busy; send it again\n").into_response())
                }
            },
            Speaks::Elastic => match outcome {
                // A bulk request's own handler answers each of its actions.
                Outcome::Delivered | Outcome::Acked(_) => unreachable!("a bulk request is answered by its handler"),
                Outcome::Refused(Token::Missing) => elastic_refused("missing authentication credentials"),
                Outcome::Refused(_) => elastic_refused("unable to authenticate with provided credentials"),
                Outcome::TooLarge => {
                    elastic_error(StatusCode::PAYLOAD_TOO_LARGE, "content_too_long", &self.too_large())
                }
                Outcome::Late => elastic_error(StatusCode::REQUEST_TIMEOUT, "timeout_exception", &late()),
                Outcome::Unreadable(why) => elastic_error(StatusCode::BAD_REQUEST, "parse_exception", &why),
                Outcome::Unsupported(why) => {
                    elastic_error(StatusCode::UNSUPPORTED_MEDIA_TYPE, "illegal_argument_exception", &why)
                }
                // A client sends again on 429.
                Outcome::Busy => retry_after(elastic_error(
                    StatusCode::TOO_MANY_REQUESTS,
                    "es_rejected_execution_exception",
                    "busy; send it again",
                )),
            },
        }
    }

    fn too_large(&self) -> String {
        format!("a request body is read whole up to {} bytes", self.max_request_bytes)
    }
}

fn late() -> String {
    format!("a request body is sent whole within {BODY_READ_SECS}s")
}

/// A HEC answer: its text, and its code where HEC has one for it.
fn hec_answer(status: StatusCode, code: Option<u16>, text: &str) -> Response {
    let mut body = match code {
        Some(code) => format!(r#"{{"code":{code},"text":"#).into_bytes(),
        None => br#"{"text":"#.to_vec(),
    };
    write_json_string(text.as_bytes(), &mut body);
    body.push(b'}');
    json(status, body)
}

/// A refused HEC body, naming the envelope at fault.
fn hec_refused(invalid: hec::Invalid) -> Response {
    let mut body = format!(r#"{{"code":{},"#, invalid.code).into_bytes();
    if let Some(event) = invalid.event {
        body.extend_from_slice(format!(r#""invalid-event-number":{event},"#).as_bytes());
    }
    body.extend_from_slice(br#""text":"#);
    write_json_string(invalid.text.as_bytes(), &mut body);
    body.push(b'}');
    json(StatusCode::BAD_REQUEST, body)
}

/// `body` as JSON.
fn json(status: StatusCode, body: Vec<u8>) -> Response {
    (status, [(header::CONTENT_TYPE, "application/json")], body).into_response()
}

/// `refused`, naming the scheme a token is presented in.
fn bearer(mut refused: Response) -> Response {
    refused
        .headers_mut()
        .insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
    refused
}

fn retry_after(mut busy: Response) -> Response {
    busy.headers_mut()
        .insert(header::RETRY_AFTER, HeaderValue::from_static(RETRY_AFTER_SECS));
    busy
}

async fn receive(
    State(server): State<Arc<Server>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: axum::body::Body,
) -> Response {
    let Speaks::Http(records) = &server.speaks else {
        unreachable!("only an HTTP receiver routes here")
    };
    let cut = |body, _: &mut Holding<'_>| match split(records, body) {
        Ok(records) => Ok(Cutter::Records(records)),
        Err(why) => Err(Box::new((StatusCode::BAD_REQUEST, format!("{why}\n")).into_response())),
    };
    take_in(&server, peer, &headers, Ok(None), Incoming::Http(body), cut, |o| {
        server.answer(o)
    })
    .await
}

/// `body` as JSON, from a server that names itself Elasticsearch.
fn elastic_json(status: StatusCode, body: Vec<u8>) -> Response {
    elastic_named(json(status, body))
}

/// `answer`, from a server that names itself Elasticsearch, which a client checks before it reads any answer.
fn elastic_named(mut answer: Response) -> Response {
    answer
        .headers_mut()
        .insert("x-elastic-product", HeaderValue::from_static("Elasticsearch"));
    answer
}

fn elastic_error(status: StatusCode, kind: &str, reason: &str) -> Response {
    elastic_json(status, elastic_receiver::error(status.as_u16(), kind, reason))
}

/// A sender refused for its credentials, told each scheme it may present them in.
fn elastic_refused(reason: &str) -> Response {
    let mut refused = elastic_error(StatusCode::UNAUTHORIZED, "security_exception", reason);
    refused.headers_mut().insert(
        header::WWW_AUTHENTICATE,
        HeaderValue::from_static("Basic realm=\"security\", ApiKey, Bearer"),
    );
    refused
}

/// What a receiver says of the cluster it names itself as, to a sender with a token.
async fn elastic_about(
    State(server): State<Arc<Server>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    about: fn() -> Vec<u8>,
) -> Response {
    match server.token(&headers) {
        Token::Admitted { .. } => elastic_json(StatusCode::OK, about()),
        refused => server.shared.answered(server.answer(Outcome::Refused(refused)), peer),
    }
}

/// Any other path, to a sender with a token.
async fn elastic_unknown(
    State(server): State<Arc<Server>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    uri: axum::http::Uri,
) -> Response {
    if let refused @ (Token::Missing | Token::Malformed | Token::Unknown) = server.token(&headers) {
        return server.shared.answered(server.answer(Outcome::Refused(refused)), peer);
    }
    let reason = format!("a receiver serves `/_bulk` and `/{{index}}/_bulk`, not {}", uri.path());
    server.shared.answered(
        elastic_error(StatusCode::NOT_FOUND, "resource_not_found_exception", &reason),
        peer,
    )
}

async fn elastic_bulk(
    State(server): State<Arc<Server>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    axum::extract::Query(query): axum::extract::Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: axum::body::Body,
) -> Response {
    elastic_take(&server, peer, None, &query, &headers, body).await
}

async fn elastic_bulk_into(
    State(server): State<Arc<Server>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    axum::extract::Path(index): axum::extract::Path<String>,
    axum::extract::Query(query): axum::extract::Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: axum::body::Body,
) -> Response {
    elastic_take(&server, peer, Some(&index), &query, &headers, body).await
}

/// A bulk request, its documents filed under the index its path names and the pipeline its query names where an
/// action names neither, answered with an item per action once its documents are delivered.
async fn elastic_take(
    server: &Server,
    peer: SocketAddr,
    index: Option<&str>,
    query: &HashMap<String, String>,
    headers: &HeaderMap,
    body: axum::body::Body,
) -> Response {
    let started = Instant::now();
    // What `cut` reads of each action, for the answer once its documents are delivered.
    let items = OnceLock::new();
    let defaults = elastic_receiver::Defaults {
        index,
        pipeline: query.get("pipeline").map(String::as_str),
    };
    // A request of many small actions holds far more answering them than its body: each is held for what answering
    // it holds at most before it is read, and the inbox counts it so.
    let cut = |body: Vec<u8>, holding: &mut Holding<'_>| {
        let holds = elastic_receiver::holds(&body);
        if holds > INBOX_BYTES {
            let why = format!(
                "a request's actions and their answers hold up to {holds} bytes, and a receiver holds at most \
                 {INBOX_BYTES}; send fewer actions to a request"
            );
            let too_large = elastic_error(StatusCode::PAYLOAD_TOO_LARGE, "content_too_long", &why);
            return Err(Box::new(too_large));
        }
        holding
            .hold(holds)
            .map_err(|outcome| Box::new(server.answer(outcome)))?;
        holding.counts(holds);
        match elastic_receiver::read(body, defaults) {
            Ok((documents, answers)) => {
                let _ = items.set(answers);
                Ok(Cutter::Documents(documents))
            }
            Err(invalid) => Err(Box::new(elastic_error(
                StatusCode::BAD_REQUEST,
                invalid.kind,
                &invalid.reason,
            ))),
        }
    };
    let answer = |outcome| match (outcome, items.get()) {
        (Outcome::Delivered, Some(items)) => {
            let mut answer = (StatusCode::OK, items.answer(started.elapsed())).into_response();
            answer
                .headers_mut()
                .insert(header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
            elastic_named(answer)
        }
        (outcome, _) => server.answer(outcome),
    };
    take_in(server, peer, headers, Ok(None), Incoming::Http(body), cut, answer).await
}

async fn hec_event(
    State(server): State<Arc<Server>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    axum::extract::Query(query): axum::extract::Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: axum::body::Body,
) -> Response {
    let cut = |body, _: &mut Holding<'_>| {
        hec::Events::new(body)
            .map(Cutter::Events)
            .map_err(|invalid| Box::new(hec_refused(invalid)))
    };
    let named = hec_channel(&headers, &query);
    take_in(&server, peer, &headers, named, Incoming::Http(body), cut, |o| {
        server.answer(o)
    })
    .await
}

async fn hec_raw(
    State(server): State<Arc<Server>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    axum::extract::Query(query): axum::extract::Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: axum::body::Body,
) -> Response {
    let cut = |body: Vec<u8>, _: &mut Holding<'_>| {
        if body.trim_ascii().is_empty() {
            return Err(Box::new(hec_refused(hec::Invalid::NO_DATA)));
        }
        let line_breaker = match (&server.speaks, query.get("sourcetype")) {
            (Speaks::Hec(sourcetypes), Some(sourcetype)) => sourcetypes.get(sourcetype),
            _ => None,
        };
        let records = Records::broken(body, line_breaker.cloned());
        Ok(Cutter::Filed(records, Arc::new(hec::raw_filed(&query))))
    };
    let named = hec_channel(&headers, &query);
    take_in(&server, peer, &headers, named, Incoming::Http(body), cut, |o| {
        server.answer(o)
    })
    .await
}

/// A forwarder's `httpout` request: S2S, compressed, its channels' streams ending with it. Answered once delivered,
/// as `httpout` asks after no `ackId`.
async fn hec_s2s(
    State(server): State<Arc<Server>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: axum::body::Body,
) -> Response {
    let Speaks::Hec(sourcetypes) = &server.speaks else {
        unreachable!("only a HEC receiver routes here")
    };
    let cut = |body: Vec<u8>, holding: &mut Holding<'_>| {
        let refused = |outcome| Box::new(server.answer(outcome));
        // What it holds past an inbox is never taken, so it is refused as too large rather than as busy.
        let hold = |bytes: usize| match bytes {
            bytes if bytes > INBOX_BYTES => Err(Outcome::TooLarge),
            bytes => holding.hold(bytes),
        };
        let records = match s2s_receiver::httpout(&body, sourcetypes, server.max_request_bytes, hold) {
            Ok(records) => records,
            Err(s2s_receiver::HttpoutRefused::TooLarge) => return Err(refused(Outcome::TooLarge)),
            Err(s2s_receiver::HttpoutRefused::Malformed(why)) => return Err(refused(Outcome::Unreadable(why))),
            Err(s2s_receiver::HttpoutRefused::Held(outcome)) => return Err(refused(outcome)),
        };
        holding.counts(records.size());
        Ok(Cutter::Each(records))
    };
    take_in(&server, peer, &headers, Ok(None), Incoming::Httpout(body), cut, |o| {
        server.answer(o)
    })
    .await
}

/// An OTLP/HTTP export of `signal`, read in the encoding its `Content-Type` names and answered in it.
async fn otlp_export(
    signal: Signal,
    receivers: Arc<OtlpReceivers>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: axum::body::Body,
) -> Response {
    let content_type = headers.get(header::CONTENT_TYPE).and_then(|v| v.to_str().ok());
    let encoding = otlp::Encoding::of(content_type);
    let wire = Wire::Http(encoding.unwrap_or_default());
    let Some(server) = receivers.of(signal) else {
        return not_taken(signal, wire);
    };
    // OTLP names no channel; a body in an encoding it does not have is refused unread.
    let named = match encoding {
        Some(_) => Ok(None),
        None => Err(Box::new(otlp_answer(
            &server,
            Outcome::Unsupported(format!(
                "an export is sent as application/x-protobuf or application/json, not as {}",
                content_type.unwrap_or("nothing")
            )),
            wire,
        ))),
    };
    take_in(
        &server,
        peer,
        &headers,
        named,
        Incoming::Http(body),
        |body, holding| otlp_cut(&server, signal, body, wire, holding),
        |outcome| otlp_answer(&server, outcome, wire),
    )
    .await
}

/// An OTLP/gRPC export of `signal`: one `Export*ServiceRequest`, its status answered in gRPC's trailers.
async fn otlp_grpc(
    signal: Signal,
    receivers: Arc<OtlpReceivers>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: axum::body::Body,
) -> Response {
    let content_type = headers.get(header::CONTENT_TYPE).and_then(|v| v.to_str().ok());
    // gRPC answers a request it did not frame with HTTP's own status, since its sender reads no trailers.
    if !otlp::is_grpc(content_type) {
        let why = format!(
            "a gRPC export is sent as {}, not as {}",
            otlp::GRPC_CONTENT_TYPE,
            content_type.unwrap_or("nothing")
        );
        let refused = (StatusCode::UNSUPPORTED_MEDIA_TYPE, why).into_response();
        return match receivers.of(signal) {
            Some(server) => server.shared.answered(refused, peer),
            None => refused,
        };
    }
    let Some(server) = receivers.of(signal) else {
        return not_taken(signal, Wire::Grpc);
    };
    take_in(
        &server,
        peer,
        &headers,
        Ok(None),
        Incoming::Grpc(body),
        |body, holding| otlp_cut(&server, signal, body, Wire::Grpc, holding),
        |outcome| otlp_answer(&server, outcome, Wire::Grpc),
    )
    .await
}

/// A remote_write request: a snappy-compressed `WriteRequest` of 1.0 or 2.0, refused unread when it is sent as
/// anything else. A 2.0 request read is answered with what it wrote, as 2.0's senders read it.
async fn prometheus_write(
    State(server): State<Arc<Server>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: axum::body::Body,
) -> Response {
    let named = |name| headers.get(name).and_then(|v: &HeaderValue| v.to_str().ok());
    let unsupported = |why| Err(Box::new(server.answer(Outcome::Unsupported(why))));
    let readable = prometheus::readable(named(header::CONTENT_TYPE));
    let version = *readable.as_ref().unwrap_or(&prometheus::Version::V1);
    let named = match readable {
        Err(why) => unsupported(why),
        Ok(_) if !prometheus::is_snappy(named(header::CONTENT_ENCODING)) => unsupported(format!(
            "a remote_write request is compressed as snappy, not as {}",
            named(header::CONTENT_ENCODING).unwrap_or("nothing")
        )),
        Ok(_) => Ok(None),
    };
    let written = std::sync::OnceLock::new();
    let mut response = take_in(
        &server,
        peer,
        &headers,
        named,
        Incoming::Snappy(body),
        |body, holding| {
            let (cutter, wrote) = prometheus_cut(&server, body, holding, version)?;
            let _ = written.set(wrote);
            Ok(cutter)
        },
        |outcome| server.answer(outcome),
    )
    .await;
    // A 2.0 sender reads what its request wrote once it is read, taken or refused: none, refused.
    let wrote = match response.status() {
        status if status.is_success() => written.get().copied(),
        StatusCode::BAD_REQUEST => Some(prometheus::Written::default()),
        _ => None,
    };
    if let (prometheus::Version::V2, Some(wrote)) = (version, wrote) {
        for (name, n) in [
            ("x-prometheus-remote-write-samples-written", wrote.samples),
            ("x-prometheus-remote-write-histograms-written", wrote.histograms),
            ("x-prometheus-remote-write-exemplars-written", wrote.exemplars),
        ] {
            response.headers_mut().insert(name, HeaderValue::from(n));
        }
    }
    response
}

/// A request's samples, decompressed within what an inbox holds. The snappy header says what decompressing holds before
/// it starts, so the read budget takes that first, and the inbox counts the request as its decompressed body.
fn prometheus_cut(
    server: &Server,
    body: Vec<u8>,
    holding: &mut Holding<'_>,
    version: prometheus::Version,
) -> Result<(Cutter, prometheus::Written), Box<Response>> {
    let refused = |outcome| Box::new(server.answer(outcome));
    let len = prometheus::decompressed_len(&body).map_err(|why| refused(Outcome::Unreadable(why)))?;
    if body.len().saturating_add(len) > INBOX_BYTES {
        let why = format!(
            "a request holds {len} bytes decompressed, and a receiver holds at most {INBOX_BYTES}; lower \
             max_samples_per_send in this remote_write's queue_config"
        );
        return Err(Box::new(
            (StatusCode::PAYLOAD_TOO_LARGE, format!("{why}\n")).into_response(),
        ));
    }
    holding.hold(body.len() + len).map_err(refused)?;
    let samples =
        prometheus::Samples::snappy(&body, version, INBOX_BYTES).map_err(|why| refused(Outcome::Unreadable(why)))?;
    drop(body);
    let size = samples.size();
    if size > INBOX_BYTES {
        let why = format!("a request holds {size} bytes decoded, and a receiver holds at most {INBOX_BYTES}");
        return Err(Box::new(
            (StatusCode::PAYLOAD_TOO_LARGE, format!("{why}\n")).into_response(),
        ));
    }
    holding.hold(size).map_err(refused)?;
    holding.counts(size);
    let written = samples.check().map_err(|why| refused(Outcome::Unreadable(why)))?;
    Ok((Cutter::Samples(samples), written))
}

/// An export of a signal no receiver on its port takes: 404, which gRPC calls UNIMPLEMENTED, as an OpenTelemetry
/// Collector answers a signal it has no pipeline for. The exporter drops it.
fn not_taken(signal: Signal, wire: Wire) -> Response {
    let why = format!("no receiver on this port takes {}", signal.name());
    otlp_refused(StatusCode::NOT_FOUND, &why, wire)
}

/// Whether every receiver on an OTLP port takes exports, as [`health`] answers for one.
async fn otlp_health(State(receivers): State<Arc<OtlpReceivers>>) -> Response {
    let servers: Vec<Arc<Server>> = receivers.0.lock().unwrap().values().cloned().collect();
    match servers.iter().find(|s| !s.shared.ready()) {
        Some(busy) => busy.answer(Outcome::Busy),
        None if servers.is_empty() => otlp_refused(
            StatusCode::SERVICE_UNAVAILABLE,
            "stopping",
            Wire::Http(otlp::Encoding::Protobuf),
        ),
        None => StatusCode::OK.into_response(),
    }
}

/// How an OTLP export arrived, and so how it is answered.
#[derive(Clone, Copy)]
enum Wire {
    /// OTLP/HTTP, in this encoding.
    Http(otlp::Encoding),
    /// OTLP/gRPC, whose messages are protobuf.
    Grpc,
}

impl Wire {
    fn encoding(self) -> otlp::Encoding {
        match self {
            Wire::Http(encoding) => encoding,
            Wire::Grpc => otlp::Encoding::Protobuf,
        }
    }
}

/// An export's records, decoded within what an inbox holds. A decoded export holds more than its body, so it takes
/// the read budget for what decoding holds before it decodes, and the inbox counts it as what it holds decoded.
fn otlp_cut(
    server: &Server,
    signal: Signal,
    body: Vec<u8>,
    wire: Wire,
    holding: &mut Holding<'_>,
) -> Result<Cutter, Box<Response>> {
    let too_large = |why: String| {
        let why = format!("{why}, and a receiver holds at most {INBOX_BYTES}; send it in smaller exports");
        Err(Box::new(otlp_refused(StatusCode::PAYLOAD_TOO_LARGE, &why, wire)))
    };
    let decoded = |size: usize| format!("an export holds {size} bytes once decoded");
    let unreadable = |why| Box::new(otlp_answer(server, Outcome::Unreadable(why), wire));
    let busy = |outcome| Box::new(otlp_answer(server, outcome, wire));
    // What decoding holds at once, which the read budget must have room for before it starts.
    let peak = match wire.encoding() {
        otlp::Encoding::Protobuf => otlp::decoded_size(signal, &body).map_err(unreadable)?,
        otlp::Encoding::Json => {
            let peak = otlp::json_peak(signal, &body);
            if peak > INBOX_BYTES {
                let why = format!(
                    "an export holds up to {peak} bytes while its JSON is read, and a receiver holds at most \
                     {INBOX_BYTES}; send it in smaller exports, or as protobuf"
                );
                return Err(Box::new(otlp_refused(StatusCode::PAYLOAD_TOO_LARGE, &why, wire)));
            }
            peak
        }
    };
    if peak > INBOX_BYTES {
        return too_large(decoded(peak));
    }
    holding.hold(peak).map_err(busy)?;
    let export = match wire.encoding() {
        otlp::Encoding::Protobuf => otlp::Export::protobuf(signal, &body),
        otlp::Encoding::Json => otlp::Export::json(signal, &body),
    }
    .map_err(unreadable)?;
    drop(body);
    let size = export.decoded_size();
    if size > INBOX_BYTES {
        return too_large(decoded(size));
    }
    holding.hold(size).map_err(busy)?;
    holding.counts(size);
    Ok(Cutter::Otlp(export))
}

/// An OTLP answer, as `wire` answers: an empty partial success once delivered, and otherwise a `Status` naming why,
/// which the spec has every 4xx and 5xx carry. 503 is one an exporter retries; 400 one it drops.
fn otlp_answer(server: &Server, outcome: Outcome, wire: Wire) -> Response {
    let (status, why) = match outcome {
        Outcome::Delivered | Outcome::Acked(_) => {
            return match wire {
                Wire::Http(encoding) => encoded(StatusCode::OK, encoding, otlp::delivered(encoding)),
                Wire::Grpc => grpc_answer(
                    0,
                    "",
                    Some(otlp::grpc_framed(&otlp::delivered(otlp::Encoding::Protobuf))),
                ),
            };
        }
        Outcome::Refused(_) => (StatusCode::UNAUTHORIZED, "a receiver token is required".to_string()),
        Outcome::TooLarge => (StatusCode::PAYLOAD_TOO_LARGE, server.too_large()),
        // An exporter drops an export answered anything but 429, 502, 503 or 504, and one sent too slowly was never
        // taken.
        Outcome::Late => (StatusCode::SERVICE_UNAVAILABLE, late()),
        Outcome::Unreadable(why) => (StatusCode::BAD_REQUEST, why),
        Outcome::Unsupported(why) => (StatusCode::UNSUPPORTED_MEDIA_TYPE, why),
        Outcome::Busy => (StatusCode::SERVICE_UNAVAILABLE, "busy; send it again".to_string()),
    };
    otlp_refused(status, &why, wire)
}

/// An OTLP refusal with `status`, or over gRPC the code nearest it, naming `why`.
fn otlp_refused(status: StatusCode, why: &str, wire: Wire) -> Response {
    let encoding = match wire {
        Wire::Http(encoding) => encoding,
        // A `grpc-encoding` gRPC does not have is UNIMPLEMENTED, as gRPC names it.
        Wire::Grpc if status == StatusCode::UNSUPPORTED_MEDIA_TYPE => return grpc_answer(12, why, None),
        Wire::Grpc => return grpc_answer(otlp::code(status.as_u16()), why, None),
    };
    let answer = encoded(status, encoding, otlp::status(status.as_u16(), why, encoding));
    match status {
        StatusCode::SERVICE_UNAVAILABLE => retry_after(answer),
        StatusCode::UNAUTHORIZED => bearer(answer),
        _ => answer,
    }
}

fn encoded(status: StatusCode, encoding: otlp::Encoding, body: Vec<u8>) -> Response {
    let content_type = HeaderValue::from_static(encoding.content_type());
    (status, [(header::CONTENT_TYPE, content_type)], body).into_response()
}

/// Why `answer` refuses its request, or `None` when it does not, read from its status alone. A gRPC answer is HTTP 200
/// and names its refusal in `grpc-status`, which folds a body too large or in an encoding not taken into unreadable.
/// OTLP/HTTP and Prometheus answer late `503`, so there it counts as busy; HEC answers a missing channel `400`, so it
/// counts as unreadable.
fn refusal(answer: &Response) -> Option<&'static str> {
    let grpc = (answer.headers().get("grpc-status"))
        .and_then(|v| v.to_str().ok()?.parse::<i32>().ok())
        .filter(|code| *code != 0);
    Some(match (answer.status().as_u16(), grpc) {
        (401 | 403, _) | (_, Some(16)) => "token",
        (400, _) | (_, Some(3)) => "unreadable",
        (413, _) => "too_large",
        (415, _) | (_, Some(12)) => "unsupported",
        (408, _) | (_, Some(4)) => "late",
        (429 | 503, _) | (_, Some(14)) => "busy",
        (status, _) if status >= 400 => "other",
        (_, Some(_)) => "other",
        _ => return None,
    })
}

/// A gRPC answer: HTTP 200, `message` where there is one, and `grpc-status` `code` naming `why`. With no message the
/// status is in the headers, which gRPC calls trailers-only; with one it follows the message as trailers.
fn grpc_answer(code: i32, why: &str, message: Option<Vec<u8>>) -> Response {
    let mut status = HeaderMap::new();
    status.insert("grpc-status", HeaderValue::from(code));
    if !why.is_empty() {
        let why = HeaderValue::from_str(&grpc_message(why)).expect("percent-encoded text is a header value");
        status.insert("grpc-message", why);
    }
    let content_type = HeaderValue::from_static(otlp::GRPC_CONTENT_TYPE);
    let Some(message) = message else {
        let mut answer = (StatusCode::OK, [(header::CONTENT_TYPE, content_type)]).into_response();
        answer.headers_mut().extend(status);
        return answer;
    };
    let body = GrpcBody {
        message: Some(message.into()),
        trailers: Some(status),
    };
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, content_type)],
        axum::body::Body::new(body),
    )
        .into_response()
}

/// `why` as `grpc-message` carries it: each byte outside printable ASCII, and `%`, percent-encoded.
fn grpc_message(why: &str) -> String {
    let mut encoded = String::with_capacity(why.len());
    for byte in why.bytes() {
        match byte {
            b' '..=b'~' if byte != b'%' => encoded.push(char::from(byte)),
            _ => encoded.push_str(&format!("%{byte:02X}")),
        }
    }
    encoded
}

/// A gRPC answer's body: its one message, then the trailers that carry its status.
struct GrpcBody {
    message: Option<axum::body::Bytes>,
    trailers: Option<HeaderMap>,
}

impl axum::body::HttpBody for GrpcBody {
    type Data = axum::body::Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();
        let frame = match this.message.take() {
            Some(message) => Some(http_body::Frame::data(message)),
            None => this.trailers.take().map(http_body::Frame::trailers),
        };
        std::task::Poll::Ready(frame.map(Ok))
    }
}

/// The channel a HEC request names, or its refusal.
fn hec_channel(headers: &HeaderMap, query: &HashMap<String, String>) -> Result<Option<Arc<str>>, Box<Response>> {
    let header = headers.get("x-splunk-request-channel").map(HeaderValue::as_bytes);
    hec::channel(header, query.get("channel").map(String::as_str)).map_err(|invalid| Box::new(hec_refused(invalid)))
}

/// Whether each `ackId` a sender asks after on its channel is delivered. A sender whose token does not acknowledge
/// is told ACK is disabled, as Splunk tells it.
async fn hec_ack(
    State(server): State<Arc<Server>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    query: axum::extract::Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: axum::body::Body,
) -> Response {
    let answered = answer_ack(&server, query, headers, body).await;
    server.shared.answered(answered, peer)
}

/// [`hec_ack`]'s answer, before it is counted.
async fn answer_ack(
    server: &Server,
    axum::extract::Query(query): axum::extract::Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: axum::body::Body,
) -> Response {
    match server.token(&headers) {
        Token::Admitted { acknowledges: true } => {}
        Token::Admitted { acknowledges: false } => {
            return hec_answer(StatusCode::BAD_REQUEST, Some(14), "ACK is disabled");
        }
        refused => return server.answer(Outcome::Refused(refused)),
    }
    let channel = match hec_channel(&headers, &query) {
        Ok(Some(channel)) => channel,
        Ok(None) => return hec_refused(hec::Invalid::NO_CHANNEL),
        Err(refused) => return *refused,
    };
    // Read as an event body is, within `maxRequestBytes` and the budget every request's reading shares.
    let encoding = headers.get(header::CONTENT_ENCODING).and_then(|v| v.to_str().ok());
    let (body, _reading) = match read_body(body, encoding, server).await {
        Ok(read) => read,
        Err(outcome) => return server.answer(outcome),
    };
    let Some(asked) = asked(&body) else {
        return hec_answer(StatusCode::BAD_REQUEST, Some(6), "Invalid data format");
    };
    let answered = server
        .shared
        .state
        .lock()
        .unwrap()
        .acks
        .ask(&channel, &asked, Instant::now());
    // Each id once, in the order of its text, as HEC's answer has them.
    let mut answered: Vec<(String, bool)> = (answered.into_iter())
        .map(|(id, done)| (id.to_string(), done))
        .collect();
    answered.sort_by(|a, b| a.0.cmp(&b.0));
    answered.dedup_by(|later, kept| later.0 == kept.0);
    let mut body = br#"{"acks":{"#.to_vec();
    for (n, (id, done)) in answered.iter().enumerate() {
        let comma = if n > 0 { "," } else { "" };
        body.extend_from_slice(format!(r#"{comma}"{id}":{done}"#).as_bytes());
    }
    body.extend_from_slice(b"}}");
    json(StatusCode::OK, body)
}

/// The ids an ack poll asks after, `{"acks":[<id>, ...]}`, or `None` for a body that is not one.
fn asked(body: &[u8]) -> Option<Vec<u64>> {
    let mut r = Reader::new(body).ok()?;
    if r.value().ok()? != Next::Object {
        return None;
    }
    let mut acks = None;
    while let Some(key) = r.key().ok()? {
        if key != "acks" {
            r.skip().ok()?;
            continue;
        }
        if acks.is_some() || r.value().ok()? != Next::Array {
            return None;
        }
        let mut ids = Vec::new();
        while r.item().ok()? {
            match r.value().ok()? {
                Next::Number(id) => ids.push(id.parse().ok()?),
                _ => return None,
            }
        }
        acks = Some(ids);
    }
    if !r.end() {
        return None;
    }
    acks
}

/// Reads a request whole, offers its records to the inbox as `cut` cuts them, and answers once they are delivered,
/// or, for a sender whose token acknowledges, at once with an `ackId` on the channel its headers name. `named` is that
/// channel, or the refusal of headers its protocol does not take, which an admitted sender hears before its body is
/// read. `cut` refuses a body that is not what its protocol reads, in that protocol's words, and `answer` words each
/// outcome. Every refusal is counted ([`Shared::answered`]).
async fn take_in(
    server: &Server,
    peer: SocketAddr,
    headers: &HeaderMap,
    named: Result<Option<Arc<str>>, Box<Response>>,
    body: Incoming,
    cut: impl FnOnce(Vec<u8>, &mut Holding<'_>) -> Result<Cutter, Box<Response>>,
    answer: impl Fn(Outcome) -> Response,
) -> Response {
    let answered = answer_request(server, peer, headers, named, body, cut, answer).await;
    server.shared.answered(answered, peer)
}

/// [`take_in`]'s answer, before it is counted.
async fn answer_request(
    server: &Server,
    peer: SocketAddr,
    headers: &HeaderMap,
    named: Result<Option<Arc<str>>, Box<Response>>,
    body: Incoming,
    cut: impl FnOnce(Vec<u8>, &mut Holding<'_>) -> Result<Cutter, Box<Response>>,
    answer: impl Fn(Outcome) -> Response,
) -> Response {
    let acknowledges = match server.token(headers) {
        Token::Admitted { acknowledges } => acknowledges,
        refused => return answer(Outcome::Refused(refused)),
    };
    // `httpout` names no channel and asks after no `ackId`: its answer acks it, whatever its token says.
    let acknowledges = acknowledges && !matches!(body, Incoming::Httpout(_));
    let channel = match (named, acknowledges) {
        (Err(refused), _) => return *refused,
        (Ok(Some(channel)), true) => Some(channel),
        // Only a HEC token acknowledges, so only HEC asks for a channel.
        (Ok(None), true) => return hec_refused(hec::Invalid::NO_CHANNEL),
        (Ok(_), false) => None,
    };
    let declared = headers
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok()?.parse::<usize>().ok());
    if declared.is_some_and(|n| n > server.max_request_bytes) {
        return answer(Outcome::TooLarge);
    }
    // A receiver that would refuse the records refuses them before they are sent; `admit` decides the race.
    if !server.shared.ready() {
        return answer(Outcome::Busy);
    }
    let read = match body {
        Incoming::Http(body) | Incoming::Httpout(body) => {
            let encoding = headers.get(header::CONTENT_ENCODING).and_then(|v| v.to_str().ok());
            read_body(body, encoding, server).await
        }
        Incoming::Grpc(body) => read_grpc(body, headers, server).await,
        // Its own protocol decompresses it, from what its header says it holds.
        Incoming::Snappy(body) => read_body(body, None, server).await,
    };
    let (body, reading) = match read {
        Ok(read) => read,
        Err(outcome) => return answer(outcome),
    };
    let mut holding = Holding {
        reading,
        taken: body.len(),
        bytes: body.len(),
    };
    let records = match cut(body, &mut holding) {
        Ok(records) => records,
        Err(refused) => return *refused,
    };
    // A dual-stack socket sees an IPv4 sender as `::ffff:a.b.c.d`; it is named as the IPv4 address it is.
    let remote = Arc::from(peer.ip().to_canonical().to_string());
    // What the body took passes to the inbox with its records, so it is never counted twice.
    let Holding { reading, bytes, .. } = holding;
    let admitted = server.shared.admit(records, bytes, remote, channel.as_ref(), reading);
    answer(match admitted {
        Admitted::Now => Outcome::Delivered,
        Admitted::Later(answer) => match answer.await {
            Ok(()) => Outcome::Delivered,
            Err(_) => Outcome::Busy,
        },
        Admitted::Acked(id) => Outcome::Acked(id),
        Admitted::Busy => Outcome::Busy,
    })
}

/// A request's body, and how it is framed.
enum Incoming {
    /// As HTTP sends it, gzip where its `Content-Encoding` says.
    Http(axum::body::Body),
    /// One gRPC message.
    Grpc(axum::body::Body),
    /// As remote_write sends it: snappy, read as sent.
    Snappy(axum::body::Body),
    /// As a forwarder's `httpout` sends it: read as HTTP sends it, and answered once delivered.
    Httpout(axum::body::Body),
}

/// What a request holds while it is cut: the read budget it has taken, and what the inbox counts it as once
/// admitted. Both are its body, or more where its protocol decodes it into more.
struct Holding<'a> {
    reading: Charge<'a>,
    taken: usize,
    bytes: usize,
}

impl Holding<'_> {
    /// Takes the read budget for holding `bytes` in all, or busy when the budget has no room for them. At most
    /// `INBOX_BYTES`.
    fn hold(&mut self, bytes: usize) -> Result<(), Outcome> {
        if let Some(more) = bytes.checked_sub(self.taken).filter(|n| *n > 0) {
            budget(&mut self.reading, more)?;
            self.taken = bytes;
        }
        Ok(())
    }

    /// Has the inbox count it as `bytes`.
    fn counts(&mut self, bytes: usize) {
        self.bytes = bytes;
    }
}

/// `body` whole and decoded as its `encoding` says, with the read budget its bytes took. Refused past
/// `max_request_bytes` read or decoded, past `BODY_READ_SECS`, or when the budget has no room for a chunk.
async fn read_body<'a>(
    body: axum::body::Body,
    encoding: Option<&str>,
    server: &'a Server,
) -> Result<(Vec<u8>, Charge<'a>), Outcome> {
    let gzip = match encoding.map(str::trim) {
        None | Some("") => false,
        Some(e) if e.eq_ignore_ascii_case("identity") => false,
        Some(e) if e.eq_ignore_ascii_case("gzip") => true,
        Some(other) => {
            return Err(Outcome::Unsupported(format!(
                "a request body is sent as it is or as gzip, not as {other}"
            )));
        }
    };
    let deadline = tokio::time::Instant::now() + Duration::from_secs(BODY_READ_SECS);
    read_into(body.into_data_stream(), Bytes::new(), gzip, None, deadline, server).await
}

/// A gRPC request's one message, inflated where its prefix marks it compressed and its `grpc-encoding` is gzip, and
/// held as [`read_body`] holds a body.
async fn read_grpc<'a>(
    body: axum::body::Body,
    headers: &HeaderMap,
    server: &'a Server,
) -> Result<(Vec<u8>, Charge<'a>), Outcome> {
    let named = headers
        .get("grpc-encoding")
        .and_then(|v| v.to_str().ok())
        .map(str::trim);
    let gzip_named = match named {
        None | Some("") => false,
        Some(e) if e.eq_ignore_ascii_case("identity") => false,
        Some(e) if e.eq_ignore_ascii_case("gzip") => true,
        Some(other) => {
            return Err(Outcome::Unsupported(format!(
                "a gRPC message is sent as it is or as gzip, not as {other}"
            )));
        }
    };
    let deadline = tokio::time::Instant::now() + Duration::from_secs(BODY_READ_SECS);
    let mut chunks = body.into_data_stream();
    let mut prefix = Vec::with_capacity(otlp::GRPC_PREFIX);
    let mut rest = Bytes::new();
    while prefix.len() < otlp::GRPC_PREFIX {
        let Some(mut chunk) = next_chunk(&mut chunks, deadline).await? else {
            return Err(Outcome::Unreadable(
                "a gRPC request carries one whole message".to_string(),
            ));
        };
        let wanted = (otlp::GRPC_PREFIX - prefix.len()).min(chunk.len());
        prefix.extend_from_slice(&chunk.split_to(wanted));
        rest = chunk;
    }
    let gzip = match prefix[0] {
        0 => false,
        1 if gzip_named => true,
        1 => {
            let why = "a gRPC message marked compressed names no grpc-encoding".to_string();
            return Err(Outcome::Unreadable(why));
        }
        other => {
            let why = format!("a gRPC message's prefix starts with {other}, which is neither 0 nor 1");
            return Err(Outcome::Unreadable(why));
        }
    };
    let length = u32::from_be_bytes([prefix[1], prefix[2], prefix[3], prefix[4]]) as usize;
    if length > server.max_request_bytes {
        return Err(Outcome::TooLarge);
    }
    read_into(chunks, rest, gzip, Some(length), deadline, server).await
}

/// The next chunk of a body, `None` at its end, read by `deadline`.
async fn next_chunk(
    chunks: &mut axum::body::BodyDataStream,
    deadline: tokio::time::Instant,
) -> Result<Option<Bytes>, Outcome> {
    match tokio::time::timeout_at(deadline, chunks.next()).await {
        Err(_) => Err(Outcome::Late),
        Ok(None) => Ok(None),
        Ok(Some(Err(e))) => Err(Outcome::Unreadable(format!("the body broke off: {e}"))),
        Ok(Some(Ok(chunk))) => Ok(Some(chunk)),
    }
}

/// `first`, then the rest of `chunks`, held as [`Taken`] holds them and inflated where `gzip`. With `exactly`, they
/// are one gRPC message of that many bytes.
async fn read_into<'a>(
    mut chunks: axum::body::BodyDataStream,
    first: Bytes,
    gzip: bool,
    exactly: Option<usize>,
    deadline: tokio::time::Instant,
    server: &'a Server,
) -> Result<(Vec<u8>, Charge<'a>), Outcome> {
    let taken = Taken {
        body: Vec::new(),
        server,
        reading: Charge::new(&server.shared.holdings),
        refused: None,
    };
    let mut into = match gzip {
        true => Into::Gzip(Box::new(flate2::write::MultiGzDecoder::new(taken))),
        false => Into::Plain(taken),
    };
    let mut sent = 0usize;
    let mut chunk = Some(first);
    while let Some(read) = chunk {
        sent += read.len();
        if sent > server.max_request_bytes {
            return Err(Outcome::TooLarge);
        }
        if exactly.is_some_and(|length| sent > length) {
            return Err(Outcome::Unreadable("a gRPC request carries one message".to_string()));
        }
        into.write(&read)?;
        chunk = next_chunk(&mut chunks, deadline).await?;
    }
    if exactly.is_some_and(|length| sent < length) {
        return Err(Outcome::Unreadable("the gRPC message broke off".to_string()));
    }
    let taken = into.finish()?;
    Ok((taken.body, taken.reading))
}

/// Where a body's bytes go as they are read: grown only as budgeted bytes arrive, never to what a sender declares, and
/// refused past `max_request_bytes`. A gzip body's are what it inflates to, so its compressed bytes are never held
/// whole and a small body that inflates without bound holds no more than a large one.
struct Taken<'a> {
    body: Vec<u8>,
    server: &'a Server,
    reading: Charge<'a>,
    /// Why a write was refused, which the writer's error cannot carry.
    refused: Option<Outcome>,
}

impl std::io::Write for Taken<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let refused = match self.body.len() + bytes.len() > self.server.max_request_bytes {
            true => Some(Outcome::TooLarge),
            false => budget(&mut self.reading, bytes.len()).err(),
        };
        if let Some(refused) = refused {
            self.refused = Some(refused);
            return Err(std::io::Error::other("refused"));
        }
        self.body.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// A body as it is sent, or inflated as it arrives.
enum Into<'a> {
    Plain(Taken<'a>),
    Gzip(Box<flate2::write::MultiGzDecoder<Taken<'a>>>),
}

impl<'a> Into<'a> {
    fn write(&mut self, chunk: &[u8]) -> Result<(), Outcome> {
        use std::io::Write;
        let wrote = match self {
            Into::Plain(taken) => taken.write_all(chunk),
            // A small chunk can inflate to a whole request, which takes long enough to stall every other connection
            // this runtime serves.
            Into::Gzip(gzip) => tokio::task::block_in_place(|| gzip.write_all(chunk)),
        };
        wrote.map_err(|e| self.refused(e))
    }

    fn finish(mut self) -> Result<Taken<'a>, Outcome> {
        if let Into::Gzip(gzip) = &mut self {
            gzip.try_finish().map_err(|e| self.refused(e))?;
        }
        match self {
            Into::Plain(taken) => Ok(taken),
            Into::Gzip(gzip) => (*gzip)
                .finish()
                .map_err(|e| Outcome::Unreadable(format!("the body is not gzip: {e}"))),
        }
    }

    /// Why a write failed: what `Taken` refused, or else a body that is not gzip.
    fn refused(&mut self, e: std::io::Error) -> Outcome {
        let taken = match self {
            Into::Plain(taken) => taken,
            Into::Gzip(gzip) => gzip.get_mut(),
        };
        (taken.refused.take()).unwrap_or_else(|| Outcome::Unreadable(format!("the body is not gzip: {e}")))
    }
}

/// Takes `bytes` of the receiver's holdings into `reading`, or answers busy when it has no room for them.
fn budget(reading: &mut Charge<'_>, bytes: usize) -> Result<(), Outcome> {
    if !reading.holdings.could_hold(reading.bytes() + bytes) {
        return Err(Outcome::TooLarge);
    }
    match reading.take(bytes) {
        true => Ok(()),
        false => {
            reading.holdings.note_full();
            Err(Outcome::Busy)
        }
    }
}

async fn health(State(server): State<Arc<Server>>) -> Response {
    let ready = server.shared.ready();
    match (&server.speaks, ready) {
        (Speaks::Http(_), true) => StatusCode::OK.into_response(),
        (Speaks::Hec(_), true) => hec_answer(StatusCode::OK, Some(17), "HEC is healthy"),
        (Speaks::Hec(_), false) => retry_after(hec_answer(
            StatusCode::SERVICE_UNAVAILABLE,
            Some(18),
            "HEC is unhealthy, queues are full",
        )),
        (Speaks::Http(_) | Speaks::Otlp | Speaks::Prometheus | Speaks::Elastic, false) => server.answer(Outcome::Busy),
        (Speaks::Otlp | Speaks::Prometheus | Speaks::Elastic, true) => StatusCode::OK.into_response(),
    }
}

/// What cuts a request body's records, each as it was written. A body that is not what `records` reads is refused
/// whole, before any of it reaches the inbox.
fn split(records: &ApiRecords, body: Vec<u8>) -> Result<Records, String> {
    Ok(match records {
        ApiRecords::Body => Records::whole(body),
        ApiRecords::Lines => Records::lines(body),
        ApiRecords::Array(pointer) => {
            match super::http_api::array_at(&body, pointer).map_err(|e| e.replace("the answer", "the body"))? {
                Some(open) => Records::array(body, open),
                None if pointer.is_empty() => return Err("the body is not a JSON array".into()),
                None => return Err(format!("the body holds no array at {pointer}")),
            }
        }
    })
}

/// What a receiver serves TLS under: its identity's certificate and key, and, when the identity names authorities,
/// a sender's certificate required to chain to one of them. It offers no application protocol: the caller adds what
/// it speaks.
fn server_tls(identity: &Identity, secrets: &HeldSecrets) -> Result<rustls::ServerConfig, IoError> {
    let named = |what: &str| format!("the identity {:?}: {what}", identity.id);
    // Save-time validation refuses a receiver whose identity holds no certificate.
    let super::trust::Presented { chain, key, .. } = super::trust::presented(identity, secrets)?
        .ok_or_else(|| IoError::transient(named("it holds no certificate for a receiver to serve")))?;
    let authorities = match identity.authorities.as_deref() {
        None => None,
        Some(pem) => Some(super::trust::certificates(identity, pem, "what it trusts")?),
    };
    serving_tls(chain, key, authorities).map_err(|refused| {
        IoError::transient(match refused {
            (Some(what), e) => named(&format!("{what}: {e}")),
            (None, e) => format!("receiver: {e}"),
        })
    })
}

/// TLS that presents `chain` signed by `key` and, given `authorities`, requires a client certificate chaining to one
/// of them. A refusal names the setting that was wrong, `what it trusts` or `its certificate and key`, beside why, and
/// names none when the TLS library itself refused.
pub(crate) fn serving_tls(
    chain: Vec<rustls::pki_types::CertificateDer<'static>>,
    key: rustls::pki_types::PrivateKeyDer<'static>,
    authorities: Option<Vec<rustls::pki_types::CertificateDer<'static>>>,
) -> Result<rustls::ServerConfig, (Option<&'static str>, String)> {
    let provider = crate::crypto::in_use();
    let builder = rustls::ServerConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .map_err(|e| (None, e.to_string()))?;
    let builder = match authorities {
        None => builder.with_no_client_auth(),
        Some(authorities) => {
            let mut roots = rustls::RootCertStore::empty();
            for root in authorities {
                roots.add(root).map_err(|e| (Some("what it trusts"), e.to_string()))?;
            }
            let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider)
                .build()
                .map_err(|e| (Some("what it trusts"), e.to_string()))?;
            builder.with_client_cert_verifier(verifier)
        }
    };
    builder
        .with_single_cert(chain, key)
        .map_err(|e| (Some("its certificate and key"), e.to_string()))
}

/// A connection's stream, over TLS or not.
pub(super) trait Stream: AsyncRead + AsyncWrite + Unpin + Send {}

impl<T: AsyncRead + AsyncWrite + Unpin + Send> Stream for T {}

/// A sender's connection to a receiver that streams, as its handler gets it.
pub(super) struct Connection {
    pub(super) stream: Box<dyn Stream>,
    /// The sender's address.
    pub(super) peer: std::net::IpAddr,
    /// What admits the requests read from `stream`. Apart from it, so a handler may split the stream to read and
    /// write at once.
    pub(super) inlet: Inlet,
}

/// What admits a connection's requests to its receiver's inbox.
#[derive(Clone)]
pub(super) struct Inlet {
    /// The sender's address, which is each record's `__source.remote`.
    pub(super) remote: Arc<str>,
    shared: Arc<Shared>,
}

impl Inlet {
    /// Offers a request this connection read, whose `bytes` it took of what the connection held (`Held::part`),
    /// waiting while the source is blocked. `Closed` ends the connection.
    pub(super) fn admit(
        &self,
        records: Cutter,
        bytes: Counted,
    ) -> impl Future<Output = Result<oneshot::Receiver<()>, Closed>> + Send + 'static {
        // Holds no borrow, so a handler awaits it while its stream is split and written elsewhere.
        let (shared, remote) = (self.shared.clone(), self.remote.clone());
        async move { shared.admit_waiting(records, bytes, remote).await }
    }

    /// What the connection holds between requests, charged to the receiver's holdings.
    pub(super) fn held(&self) -> Held {
        Held {
            shared: self.shared.clone(),
            counted: Counted::default(),
            turn: false,
        }
    }
}

/// What one connection of a receiver that streams holds: its partial frames and events, its buffers, and room for what
/// its next step adds. Taken from the receiver's holdings before the connection reads, and given back when dropped.
pub(super) struct Held {
    shared: Arc<Shared>,
    counted: Counted,
    turn: bool,
}

impl Held {
    /// Holds `bytes` in all: what the connection holds and what its next step may add, so it reads only into room it
    /// took. Waits while the receiver has no room for them, so a full receiver stops reading and the sender's window
    /// fills. A connection short of room takes the receiver's turn when it is free, so one connection always moves.
    pub(super) async fn hold(&mut self, bytes: usize) -> Result<(), Closed> {
        let holdings = &self.shared.holdings;
        let Some(more) = bytes.checked_sub(self.counted.total()) else {
            holdings.give(self.counted.split_off(self.counted.total() - bytes));
            self.release_unused();
            return Ok(());
        };
        let mut waited = false;
        loop {
            // Enabled before it looks, so room made between the look and the wait still wakes it.
            let mut room = std::pin::pin!(self.shared.room.notified());
            room.as_mut().enable();
            let mut freed = std::pin::pin!(holdings.freed.notified());
            freed.as_mut().enable();
            if self.shared.state.lock().unwrap().closed {
                return Err(Closed);
            }
            if holdings.take(more, &mut self.counted, &mut self.turn) {
                return Ok(());
            }
            if !waited {
                holdings.note_full();
                waited = true;
            }
            holdings.wait(room, freed).await;
        }
    }

    /// `bytes` of what it holds, which pass to the records it admits.
    pub(super) fn part(&mut self, bytes: usize) -> Counted {
        let part = self.counted.split_off(bytes.min(self.counted.total()));
        self.release_unused();
        part
    }

    /// Lets the turn go once nothing it holds is of the reserve.
    fn release_unused(&mut self) {
        if self.turn && self.counted.reserved == 0 {
            self.turn = false;
            self.shared.holdings.release();
        }
    }
}

#[cfg(test)]
impl Held {
    /// Held for a receiver of its own, which nothing else fills.
    pub(super) fn alone() -> Held {
        Held {
            shared: Arc::new(Shared::default()),
            counted: Counted::default(),
            turn: false,
        }
    }
}

impl Drop for Held {
    fn drop(&mut self) {
        self.shared.holdings.give(std::mem::take(&mut self.counted));
        if self.turn {
            self.shared.holdings.release();
        }
    }
}

/// The largest datagram UDP carries.
const MAX_DATAGRAM: usize = u16::MAX as usize;

/// Serves `socket` for a receiver that takes datagrams: `cut` makes a datagram from a sender the records it holds, or
/// `None` for one it refuses. Nothing slows a sender of datagrams, so one that arrives while the inbox has no room, or
/// the source is blocked, is dropped and counted. A datagram larger than `max` reaches `cut` as `max + 1` bytes.
/// Runs until dropped.
async fn serve_datagrams<C>(socket: tokio::net::UdpSocket, shared: Arc<Shared>, max: usize, cut: C)
where
    C: Fn(&[u8], std::net::IpAddr) -> Option<Cutter> + Send + 'static,
{
    let mut buffer = vec![0; max.min(MAX_DATAGRAM) + 1];
    loop {
        let (length, peer) = match socket.recv_from(&mut buffer).await {
            Ok(received) => received,
            Err(e) => {
                tracing::warn!(
                    target: "worker.receiver_datagram_failed",
                    error = %e,
                    "a receiver could not read a datagram"
                );
                continue;
            }
        };
        let peer = peer.ip().to_canonical();
        let Some(records) = cut(&buffer[..length], peer) else {
            continue;
        };
        shared.admit_datagram(records, length, peer);
    }
}

/// What a datagram held in the inbox costs beyond its body: its request, its buffers, its address and its answer. A
/// sender of datagrams waits on nothing, so without it a flood of small ones holds far more than the inbox allows.
const DATAGRAM_COST: usize = 512;

impl Shared {
    /// Offers a datagram of `length` bytes from `peer`, or drops and counts it when the inbox has no room.
    fn admit_datagram(&self, records: Cutter, length: usize, peer: std::net::IpAddr) {
        // Nothing waits on delivery: there is no one to tell.
        let charge = Charge::new(&self.holdings);
        let remote = Arc::from(peer.to_string());
        if let Admitted::Busy = self.admit(records, length + DATAGRAM_COST, remote, None, charge) {
            self.dropped.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }
}

/// Serves `listener` for a receiver whose protocol streams over a connection held open: each connection, under `tls`
/// when given, is a task of its own that `handle` runs. A connection that fails to arrive or to complete its
/// handshake ends alone. Runs until dropped, which ends every connection it holds.
async fn serve_streams<H, F>(
    listener: tokio::net::TcpListener,
    tls: Option<rustls::ServerConfig>,
    shared: Arc<Shared>,
    handle: H,
) where
    H: Fn(Connection) -> F + Send + Sync + 'static,
    F: Future<Output = ()> + Send + 'static,
{
    use axum_server::accept::Accept;

    let tls = tls.map(|config| {
        axum_server::tls_rustls::RustlsAcceptor::new(axum_server::tls_rustls::RustlsConfig::from_config(Arc::new(
            config,
        )))
    });
    let handle = Arc::new(handle);
    let mut connections = tokio::task::JoinSet::new();
    loop {
        let accepted = tokio::select! {
            accepted = listener.accept() => accepted,
            Some(_) = connections.join_next() => continue,
        };
        let (tcp, peer) = match accepted {
            Ok(accepted) => accepted,
            Err(e) => {
                tracing::warn!(
                    target: "worker.receiver_accept_failed",
                    error = %e,
                    "a receiver could not take a connection"
                );
                // Out of descriptors, accepting again at once fails again at once.
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };
        let (tls, shared, handle) = (tls.clone(), shared.clone(), handle.clone());
        connections.spawn(async move {
            let stream: Box<dyn Stream> = match tls {
                None => Box::new(tcp),
                Some(tls) => match tls.accept(tcp, ()).await {
                    Ok((stream, ())) => Box::new(stream),
                    Err(e) => {
                        tracing::debug!(
                            target: "worker.receiver_handshake_failed",
                            subject = %peer,
                            error = %e,
                            "a sender's TLS handshake failed"
                        );
                        return;
                    }
                },
            };
            // A dual-stack socket sees an IPv4 sender as `::ffff:a.b.c.d`; it is named as the IPv4 address it is.
            let peer = peer.ip().to_canonical();
            let remote = Arc::from(peer.to_string());
            handle(Connection {
                stream,
                peer,
                inlet: Inlet { remote, shared },
            })
            .await;
        });
    }
}

/// The sockets this process's supervisor handed it, by what each listens on.
fn inherited() -> &'static Mutex<HashMap<Listen, socket2::Socket>> {
    static INHERITED: OnceLock<Mutex<HashMap<Listen, socket2::Socket>>> = OnceLock::new();
    INHERITED.get_or_init(Default::default)
}

/// A UDP socket's name in [`SOCKETS_ENV`] is its address after this. A TCP socket's is its address alone.
const UDP_PREFIX: &str = "udp:";

/// `listen` as [`SOCKETS_ENV`] names it.
fn named(listen: Listen) -> String {
    match listen.transport {
        Transport::Tcp => listen.address.to_string(),
        Transport::Udp => format!("{UDP_PREFIX}{}", listen.address),
    }
}

/// What [`named`] names.
#[cfg(unix)]
fn unnamed(name: &str) -> Option<Listen> {
    let (transport, address) = match name.strip_prefix(UDP_PREFIX) {
        Some(address) => (Transport::Udp, address),
        None => (Transport::Tcp, name),
    };
    Some(Listen {
        transport,
        address: address.parse().ok()?,
    })
}

/// Take the sockets this process's supervisor handed it ([`SOCKETS_ENV`]). A runtime calls this first, before it
/// starts any process of its own, which would otherwise inherit them too. A pair that does not name a listening
/// socket on its address is left alone, and that receiver binds its own.
pub fn adopt_inherited() {
    #[cfg(unix)]
    {
        use std::os::fd::FromRawFd;

        let Ok(named) = std::env::var(SOCKETS_ENV) else { return };
        let mut held = inherited().lock().unwrap();
        for pair in named.split(',').filter(|p| !p.is_empty()) {
            let Some((name, fd)) = pair.rsplit_once('=') else {
                continue;
            };
            let (Some(listen), Ok(fd)) = (unnamed(name), fd.parse::<i32>()) else {
                continue;
            };
            // SAFETY: `fcntl` on any integer is defined; -1 answers a number that is no open descriptor.
            if unsafe { libc::fcntl(fd, libc::F_GETFD) } == -1 {
                continue;
            }
            // SAFETY: the descriptor is open (above), and this process owns it from here: the supervisor handed it
            // over for this, and nothing else in this process knows its number. A descriptor that is not this
            // receiver's socket is handed back below, unclosed.
            let socket = std::mem::ManuallyDrop::new(unsafe { socket2::Socket::from_raw_fd(fd) });
            let bound = socket.local_addr().ok().and_then(|a| a.as_socket());
            if !bound.is_some_and(|bound| serves(bound, listen.address))
                || !socket.r#type().is_ok_and(|t| t == socket_type(listen.transport))
            {
                continue;
            }
            // SAFETY: as above. Closed on exec again, so nothing this process starts holds the port.
            unsafe { libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) };
            held.insert(listen, std::mem::ManuallyDrop::into_inner(socket));
        }
    }
}

/// Whether a socket bound to `bound` serves `address`: the same address, or `0.0.0.0` standing in for `[::]` on a
/// host without IPv6 ([`bind`]). It vets a descriptor handed over by number, which could name any socket, before a
/// receiver serves it. Unix only, as the handover is ([`Sockets`]).
#[cfg(unix)]
fn serves(bound: SocketAddr, address: SocketAddr) -> bool {
    bound == address
        || (address.ip() == std::net::Ipv6Addr::UNSPECIFIED
            && bound == SocketAddr::new(std::net::Ipv4Addr::UNSPECIFIED.into(), address.port()))
}

/// A socket on `listen`, non-blocking: a copy of the one the supervisor handed over, or one of its own.
fn socket(listen: Listen) -> std::io::Result<socket2::Socket> {
    let socket = match inherited().lock().unwrap().get(&listen) {
        Some(handed) => handed.try_clone()?,
        None => bind(listen)?,
    };
    socket.set_nonblocking(true)?;
    Ok(socket)
}

/// Binds `listen` with `SO_REUSEADDR` on unix, so a port a stopped process held a moment ago binds again.
/// `[::]` is every address of the host, so it is bound dual-stack, taking IPv4 senders as well; on a host without
/// IPv6, it binds `0.0.0.0` instead.
pub(crate) fn bind(listen: Listen) -> std::io::Result<socket2::Socket> {
    let address = listen.address;
    let every_v6 = address.ip() == std::net::Ipv6Addr::UNSPECIFIED;
    match bind_one(listen.transport, address, every_v6) {
        Err(e) if every_v6 && no_ipv6(&e) => bind_one(
            listen.transport,
            SocketAddr::new(std::net::Ipv4Addr::UNSPECIFIED.into(), address.port()),
            false,
        ),
        bound => bound,
    }
}

fn bind_one(transport: Transport, address: SocketAddr, dual_stack: bool) -> std::io::Result<socket2::Socket> {
    let protocol = match transport {
        Transport::Tcp => socket2::Protocol::TCP,
        Transport::Udp => socket2::Protocol::UDP,
    };
    let socket = socket2::Socket::new(
        socket2::Domain::for_address(address),
        socket_type(transport),
        Some(protocol),
    )?;
    #[cfg(unix)]
    socket.set_reuse_address(true)?;
    if dual_stack {
        socket.set_only_v6(false)?;
    }
    socket.bind(&address.into())?;
    if transport == Transport::Tcp {
        socket.listen(1024)?;
    }
    Ok(socket)
}

fn socket_type(transport: Transport) -> socket2::Type {
    match transport {
        Transport::Tcp => socket2::Type::STREAM,
        Transport::Udp => socket2::Type::DGRAM,
    }
}

/// Whether `e` says this host has no IPv6: the kernel has it turned off, or no address to bind.
fn no_ipv6(e: &std::io::Error) -> bool {
    #[cfg(unix)]
    if e.raw_os_error() == Some(libc::EAFNOSUPPORT) {
        return true;
    }
    e.kind() == std::io::ErrorKind::AddrNotAvailable
}

/// The sockets a supervisor holds for its receivers, kept across the runtimes it starts. While one runtime stops and
/// the next starts, a sender's connection, or datagram, waits in the kernel's queue on the socket held here, until
/// that queue is full. The supervisor's own restart closes them, and senders are refused until it binds again.
///
/// Unix only: a runtime inherits a held socket as an open descriptor, kept open across exec in `pre_exec` and named
/// in [`SOCKETS_ENV`]. On Windows `std` offers no way to hand a child process a socket, so there each runtime binds
/// its own, and a sender that connects while one runtime is replaced is refused and sends again.
#[derive(Default)]
pub struct Sockets {
    held: HashMap<Listen, socket2::Socket>,
}

impl Sockets {
    /// Bind everything in `wanted` not already held. One that will not bind is left to the runtime, whose own bind
    /// says why.
    pub fn hold(&mut self, wanted: &[Listen]) {
        if !cfg!(unix) {
            return;
        }
        for &listen in wanted {
            if self.held.contains_key(&listen) {
                continue;
            }
            match bind(listen) {
                Ok(socket) => {
                    self.held.insert(listen, socket);
                }
                Err(e) => tracing::warn!(
                    target: "worker.receiver_not_bound",
                    subject = %named(listen),
                    error = %e,
                    "the worker could not hold a receiver's address across deploys; the runtime will try it itself"
                ),
            }
        }
    }

    /// Close every socket `wanted` no longer names.
    pub fn keep_only(&mut self, wanted: &[Listen]) {
        self.held.retain(|listen, _| wanted.contains(listen));
    }

    /// Each held socket, as [`SOCKETS_ENV`] names it, and its descriptor, for a runtime about to be started.
    #[cfg(unix)]
    pub fn handed(&self, listens: &[Listen]) -> (String, Vec<i32>) {
        use std::os::fd::AsRawFd;
        let pairs: Vec<(Listen, i32)> = (self.held.iter())
            .filter(|(l, _)| listens.contains(l))
            .map(|(l, s)| (*l, s.as_raw_fd()))
            .collect();
        let named = pairs
            .iter()
            .map(|(l, fd)| format!("{}={fd}", named(*l)))
            .collect::<Vec<_>>()
            .join(",");
        (named, pairs.into_iter().map(|(_, fd)| fd).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    impl Queued {
        /// The bytes of a record sent as bytes.
        fn bytes(&self) -> &[u8] {
            match &self.record {
                Payload::Bytes(bytes) => bytes,
                Payload::Row(_) => panic!("a row, not bytes"),
            }
        }
    }

    /// `n` records of two bytes each, and the size of the body they are cut from.
    fn records(n: usize) -> (Records, usize) {
        let body: String = (0..n).map(|i| format!("r{i}\n")).collect();
        let bytes = body.len();
        (Records::lines(body.into_bytes()), bytes)
    }

    fn admit(shared: &Shared, (records, bytes): (Records, usize)) -> Admitted {
        shared.admit(
            Cutter::Records(records),
            bytes,
            remote(),
            None,
            Charge::new(&shared.holdings),
        )
    }

    fn all(mut records: Records) -> Vec<Vec<u8>> {
        std::iter::from_fn(|| records.next_record()).collect()
    }

    /// `bytes` taken of `shared`'s holdings, as a connection takes them before it admits a request.
    fn taken(shared: &Shared, bytes: usize) -> Counted {
        let (mut counted, mut turn) = (Counted::default(), false);
        assert!(shared.holdings.take(bytes, &mut counted, &mut turn));
        if turn {
            shared.holdings.release();
        }
        counted
    }

    fn remote() -> Arc<str> {
        Arc::from("10.0.0.1")
    }

    fn answered(rx: &mut oneshot::Receiver<()>) -> bool {
        rx.try_recv().is_ok()
    }

    #[test]
    fn a_request_is_answered_once_the_watermark_passes_its_last_record() {
        let shared = Shared::default();
        let Admitted::Later(mut first) = admit(&shared, records(2)) else {
            panic!()
        };
        let Admitted::Later(mut second) = admit(&shared, records(1)) else {
            panic!()
        };
        let taken = shared.take(10, 1 << 20, Duration::ZERO);
        assert_eq!(taken.iter().map(|q| q.offset).collect::<Vec<_>>(), [0, 1, 2]);

        shared.answer_through(0);
        assert!(!answered(&mut first), "its second record is not yet delivered");
        shared.answer_through(1);
        assert!(answered(&mut first));
        assert!(!answered(&mut second));
        shared.answer_through(2);
        assert!(answered(&mut second));
    }

    /// A request is held whole and its records cut as they are taken, so one taken over several polls is answered
    /// only once its last record is delivered, and its body is counted until then.
    #[test]
    fn a_request_taken_over_several_polls_is_answered_after_its_last_record() {
        let shared = Shared::default();
        let Admitted::Later(mut held) = admit(&shared, records(3)) else {
            panic!()
        };
        assert_eq!(shared.take(2, 1 << 20, Duration::ZERO).len(), 2);
        assert_eq!(
            shared.state.lock().unwrap().queued_bytes.total(),
            9,
            "its body is held until its last record is taken"
        );
        shared.answer_through(1);
        assert!(!answered(&mut held));
        let last = shared.take(10, 1 << 20, Duration::ZERO);
        assert_eq!(last.iter().map(|q| q.offset).collect::<Vec<_>>(), [2]);
        assert_eq!(shared.state.lock().unwrap().queued_bytes.total(), 0);
        shared.answer_through(2);
        assert!(answered(&mut held));
    }

    /// Each raw line carries its own copy of what the query files it under, so a poll counts the copies.
    #[test]
    fn a_poll_counts_what_each_record_is_filed_under() {
        let shared = Shared::default();
        let filed = vec![("host".to_string(), vec![b'h'; 1000])];
        let lines = Records::lines(b"a\nb\nc\n".to_vec());
        assert!(matches!(
            shared.admit(
                Cutter::Filed(lines, Arc::new(filed)),
                6,
                remote(),
                None,
                Charge::new(&shared.holdings)
            ),
            Admitted::Later(_)
        ));
        assert_eq!(shared.take(10, 2000, Duration::ZERO).len(), 1);
    }

    /// A poll's rows are one chunk, which carries what they are filed under for them all.
    #[test]
    fn a_poll_of_rows_stops_where_what_they_are_filed_under_changes() {
        let shared = Shared::default();
        let resource = |r| {
            format!(
                r#"{{"resource": {{"attributes": [{{"key": "r", "value": {{"stringValue": "{r}"}}}}]}},
                    "scopeLogs": [{{"scope": {{"name": "s"}}, "logRecords": [{{}}, {{}}]}}]}}"#
            )
        };
        let body = format!(r#"{{"resourceLogs": [{}, {}]}}"#, resource("r1"), resource("r2"));
        let logs = otlp::Export::json(Signal::Logs, body.as_bytes()).unwrap();
        assert!(matches!(
            shared.admit(
                Cutter::Otlp(logs),
                body.len(),
                remote(),
                None,
                Charge::new(&shared.holdings)
            ),
            Admitted::Later(_)
        ));
        for _ in ["r1", "r2"] {
            let records = rows_chunk(shared.take(10, 1 << 20, Duration::ZERO), &SourceMeta::OTLP_LOGS);
            assert_eq!(records.len(), 2);
            assert!(
                records
                    .iter()
                    .all(|r| r.headers.is_none() && Arc::ptr_eq(&r.chunk, &records[0].chunk))
            );
            let filed = records[0].chunk.filed().expect("the chunk carries it");
            assert!(filed.iter().any(|(k, _)| k == otlp::RESOURCE));
        }
    }

    /// A data point's arrays and structs are columns of their declared types, holding what the decoder wrote.
    #[test]
    fn a_metric_point_s_arrays_and_structs_are_typed_columns() {
        use crate::core::column::DataType;
        let body = r#"{"resourceMetrics": [{"scopeMetrics": [{"metrics": [
            {"name": "h", "histogram": {"dataPoints": [{"bucketCounts": ["1", "2"], "explicitBounds": [1.5]}]}},
            {"name": "e", "exponentialHistogram": {"dataPoints": [{"positive": {"offset": 1, "bucketCounts": ["3"]}}]}},
            {"name": "s", "summary": {"dataPoints": [{"quantileValues": [{"quantile": 0.5, "value": 2}]}]}}
        ]}]}]}"#;
        let meta = SourceMeta::otlp(Signal::Metrics);
        let mut export = otlp::Export::json(Signal::Metrics, body.as_bytes()).unwrap();
        let rows: Vec<otlp::Row> = std::iter::from_fn(|| export.next_record())
            .map(|(row, _)| row)
            .collect();
        let shared = Shared::default();
        let export = otlp::Export::json(Signal::Metrics, body.as_bytes()).unwrap();
        assert!(matches!(
            shared.admit(
                Cutter::Otlp(export),
                body.len(),
                remote(),
                None,
                Charge::new(&shared.holdings)
            ),
            Admitted::Later(_)
        ));
        let records = rows_chunk(shared.take(10, 1 << 20, Duration::ZERO), &meta);
        assert_eq!(records.len(), 3);
        let mut pool = crate::core::buffer::BufferPool::new();
        let mut typed = 0;
        for (at, (name, ty)) in meta.record().iter().enumerate() {
            let column = records[0].chunk.column(at);
            assert_eq!(&column.data_type(), ty, "{name}");
            if !matches!(ty, DataType::Array(_) | DataType::Struct(..)) {
                continue;
            }
            let as_variant = crate::format::tag::to_variant(column, &mut pool);
            let shown = as_variant.variant().unwrap();
            for (row, cells) in rows.iter().enumerate() {
                match &cells.0[at] {
                    Some(otlp::Cell::Variant(wrote)) => {
                        typed += 1;
                        assert_eq!(shown.get(row), wrote.as_slice(), "{name} at row {row}");
                    }
                    None => assert!(!shown.validity.is_valid(row), "{name} at row {row}"),
                    Some(other) => panic!("{name} is {other:?}"),
                }
            }
            as_variant.reclaim(&mut pool);
        }
        assert_eq!(typed, 4, "bucket counts, bounds, positive and quantiles");
    }

    #[test]
    fn an_empty_request_is_answered_at_once() {
        assert!(matches!(admit(&Shared::default(), records(0)), Admitted::Now));
    }

    #[test]
    fn a_full_inbox_answers_busy_and_takes_one_again_once_drained() {
        let shared = Shared::default();
        let big = Records::whole(vec![b'x'; INBOX_BYTES]);
        assert!(matches!(admit(&shared, (big, INBOX_BYTES)), Admitted::Later(_)));
        assert!(matches!(admit(&shared, records(1)), Admitted::Busy));
        assert!(!shared.ready());
        assert_eq!(
            shared.take(10, 1, Duration::ZERO).len(),
            1,
            "one record comes back whatever its size"
        );
        assert!(matches!(admit(&shared, records(1)), Admitted::Later(_)));
    }

    /// A connection takes room before it reads on, and waits while the receiver has none. One short of room takes the
    /// receiver's turn and its reserve, so one connection reads on whatever the others hold.
    #[tokio::test]
    async fn a_connection_reads_on_into_shared_room_or_with_the_turn() {
        let shared = Arc::new(Shared {
            holdings: Holdings {
                ceiling: 100,
                ..Holdings::reserving(50, None)
            },
            ..Shared::default()
        });
        let inlet = Inlet {
            remote: remote(),
            shared: shared.clone(),
        };
        let mut first = inlet.held();
        first.hold(80).await.unwrap();
        let mut second = inlet.held();
        second.hold(40).await.unwrap();
        assert!(second.turn, "short of shared room, it takes the turn");
        assert_eq!(shared.holdings.held(), 120);

        let mut third = inlet.held();
        let waiting = tokio::time::timeout(Duration::from_millis(50), third.hold(30)).await;
        assert!(waiting.is_err(), "no shared room, and the turn is taken");
        assert_eq!(shared.holdings.full.load(std::sync::atomic::Ordering::Relaxed), 1);

        let records = second.part(40);
        assert!(
            !second.turn,
            "nothing it holds is of the reserve, so the turn is let go"
        );
        shared.holdings.give(records);
        drop(first);
        third.hold(30).await.unwrap();
        assert!(!third.turn, "the shared room was let go");
        drop(second);
        drop(third);
        assert_eq!(shared.holdings.held(), 0);
    }

    /// A receiver shares from the pool, and its turn takes its reserve once the pool is spent.
    #[test]
    fn a_receiver_shares_the_pool_and_its_turn_takes_its_reserve() {
        let pool = QueuePool::new(10);
        let shared = Shared {
            holdings: Holdings::reserving(6, Some(pool.clone())),
            ..Shared::default()
        };
        let mut first = Charge::new(&shared.holdings);
        assert!(first.take(4));
        let mut second = Charge::new(&shared.holdings);
        assert!(second.take(6));
        assert_eq!(pool.lent(), 10);
        let mut third = Charge::new(&shared.holdings);
        assert!(third.take(5), "the pool is spent, so it takes the turn");
        assert_eq!(pool.lent(), 10, "the reserve is not borrowed");
        let mut fourth = Charge::new(&shared.holdings);
        assert!(!fourth.take(1), "the turn is taken");
        drop(third);
        assert!(fourth.take(1), "the turn was let go");
        drop(first);
        drop(second);
        drop(fourth);
        assert_eq!(pool.lent(), 0);
        assert_eq!(shared.holdings.held(), 0);
        assert!(
            !shared.holdings.could_hold(11),
            "more than its reserve and the whole pool"
        );
    }

    /// A datagram is charged what it holds, not only its body, so a flood of one-byte datagrams fills the inbox.
    #[test]
    fn small_datagrams_fill_the_inbox_by_what_each_holds() {
        let shared = Shared::default();
        let peer = "10.0.0.1".parse().unwrap();
        let held = INBOX_BYTES / (1 + DATAGRAM_COST);
        for _ in 0..held + 10 {
            shared.admit_datagram(Cutter::Records(Records::whole(b"x".to_vec())), 1, peer);
        }
        assert_eq!(shared.state.lock().unwrap().queued.len(), held);
        assert_eq!(shared.dropped.load(std::sync::atomic::Ordering::Relaxed), 10);
    }

    #[test]
    fn a_blocked_receiver_supplies_nothing_and_takes_nothing() {
        let shared = Shared::default();
        assert!(matches!(admit(&shared, records(1)), Admitted::Later(_)));
        shared.state.lock().unwrap().blocked = true;
        assert!(shared.take(10, 1 << 20, Duration::ZERO).is_empty());
        assert!(matches!(admit(&shared, records(1)), Admitted::Busy));
        shared.state.lock().unwrap().blocked = false;
        assert_eq!(shared.take(10, 1 << 20, Duration::ZERO).len(), 1);
    }

    #[test]
    fn closing_answers_every_held_request_busy() {
        let shared = Shared::default();
        let Admitted::Later(mut held) = admit(&shared, records(1)) else {
            panic!()
        };
        shared.close();
        assert!(matches!(held.try_recv(), Err(oneshot::error::TryRecvError::Closed)));
        assert!(matches!(admit(&shared, records(1)), Admitted::Busy));
    }

    #[test]
    fn a_poll_stops_at_its_row_and_byte_bounds() {
        let shared = Shared::default();
        assert!(matches!(admit(&shared, records(5)), Admitted::Later(_)));
        assert_eq!(shared.take(2, 1 << 20, Duration::ZERO).len(), 2);
        assert_eq!(
            shared.take(10, 4, Duration::ZERO).len(),
            2,
            "two records of two bytes each"
        );
        assert_eq!(shared.take(10, 1 << 20, Duration::ZERO).len(), 1);
        assert_eq!(shared.state.lock().unwrap().queued_bytes.total(), 0);
    }

    #[test]
    fn a_body_splits_as_its_receiver_reads_it() {
        let split = |records: &ApiRecords, body: Vec<u8>| split(records, body).map(all);
        let lines = split(&ApiRecords::Lines, b"a\r\nb\n\nc".to_vec()).unwrap();
        assert_eq!(lines, [b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
        let whole = split(&ApiRecords::Body, b"{\"a\":1}".to_vec()).unwrap();
        assert_eq!(whole, [b"{\"a\":1}".to_vec()]);
        let array = split(
            &ApiRecords::Array("/events".into()),
            br#"{"events":[{"a":1}, 2]}"#.to_vec(),
        )
        .unwrap();
        assert_eq!(array, [br#"{"a":1}"#.to_vec(), b"2".to_vec()]);
        assert!(split(&ApiRecords::Array(String::new()), b"not json".to_vec()).is_err());
        assert!(split(&ApiRecords::Array("/events".into()), b"{}".to_vec()).is_err());
    }

    /// A body takes from the receiver's holdings what it sent, not what it may send, and keeps it until it is let go.
    #[tokio::test]
    async fn a_body_takes_the_budget_its_bytes_need() {
        let server = server(Speaks::Http(ApiRecords::Body), &[], 10);
        let body = |n: usize| axum::body::Body::from(vec![b'x'; n]);

        let (first, held) = read_body(body(8), None, &server).await.ok().unwrap();
        assert_eq!(first.len(), 8);
        assert_eq!(server.shared.holdings.held(), 8);
        let busy = read_body(body(8), None, &server).await.err().unwrap();
        assert!(matches!(busy, Outcome::Busy));
        drop(held);
        assert_eq!(server.shared.holdings.held(), 0);

        let large = read_body(body(1025), None, &server).await.err().unwrap();
        assert!(matches!(large, Outcome::TooLarge));
        assert_eq!(
            server.shared.holdings.held(),
            0,
            "a refused body gives back what it took"
        );
    }

    /// A gzip body is inflated whole, and what it inflates to is held to the request's limit and takes its budget.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_gzip_body_is_inflated_within_its_limits() {
        use std::io::Write;
        let gzip = |bytes: &[u8]| {
            let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            encoder.write_all(bytes).unwrap();
            axum::body::Body::from(encoder.finish().unwrap())
        };
        let server = server(Speaks::Http(ApiRecords::Body), &[], INBOX_BYTES);
        let (inflated, held) = read_body(gzip(b"a\nb\n"), Some("gzip"), &server).await.ok().unwrap();
        assert_eq!(inflated, b"a\nb\n");
        assert!(server.shared.holdings.held() > 0);
        drop(held);

        let bomb = read_body(gzip(&[0; 2048]), Some("gzip"), &server).await.err().unwrap();
        assert!(matches!(bomb, Outcome::TooLarge), "it inflates past maxRequestBytes");
        let garbage = axum::body::Body::from(b"not gzip".to_vec());
        assert!(matches!(
            read_body(garbage, Some("gzip"), &server).await.err().unwrap(),
            Outcome::Unreadable(_)
        ));
        let br = axum::body::Body::from(b"x".to_vec());
        assert!(matches!(
            read_body(br, Some("br"), &server).await.err().unwrap(),
            Outcome::Unsupported(_)
        ));
        assert_eq!(server.shared.holdings.held(), 0);

        let whole = {
            let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            encoder.write_all(b"one\ntwo\nthree\n").unwrap();
            encoder.finish().unwrap()
        };
        let pieces: Vec<Result<Vec<u8>, std::io::Error>> = whole.chunks(3).map(|c| Ok(c.to_vec())).collect();
        let chunked = axum::body::Body::from_stream(futures_util::stream::iter(pieces));
        let (inflated, held) = read_body(chunked, Some("gzip"), &server).await.ok().unwrap();
        assert_eq!(inflated, b"one\ntwo\nthree\n", "it inflates as its chunks arrive");
        drop(held);
        let cut = axum::body::Body::from(whole[..whole.len() / 2].to_vec());
        assert!(matches!(
            read_body(cut, Some("gzip"), &server).await.err().unwrap(),
            Outcome::Unreadable(_)
        ));

        let tight = super::tests::server(Speaks::Http(ApiRecords::Body), &[], 1000);
        let (inflated, held) = read_body(gzip(&[0; 1000]), Some("gzip"), &tight).await.ok().unwrap();
        assert_eq!(inflated.len(), 1000, "what it inflates to is all the budget it needs");
        assert_eq!(tight.shared.holdings.held(), 1000);
        drop(held);
    }

    /// A server whose receiver holds at most `ceiling`.
    fn server(speaks: Speaks, tokens: &[&str], ceiling: usize) -> Server {
        let shared = Shared {
            holdings: Holdings {
                ceiling,
                ..Holdings::default()
            },
            ..Shared::default()
        };
        Server {
            shared: Arc::new(shared),
            speaks,
            tokens: tokens
                .iter()
                .map(|t| Admits {
                    token: t.as_bytes().to_vec(),
                    acknowledges: false,
                })
                .collect(),
            max_request_bytes: 1024,
        }
    }

    fn with(value: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(header::AUTHORIZATION, HeaderValue::from_str(value).unwrap());
        headers
    }

    #[test]
    fn only_a_listed_token_is_admitted() {
        let http = server(Speaks::Http(ApiRecords::Body), &["one", "two"], INBOX_BYTES);
        let admitted = Token::Admitted { acknowledges: false };
        assert_eq!(http.token(&with("Bearer two")), admitted);
        assert_eq!(
            http.token(&with("bearer two")),
            admitted,
            "a scheme is matched regardless of case"
        );
        assert_eq!(http.token(&with("Bearer three")), Token::Unknown);
        assert_eq!(http.token(&with("Basic two")), Token::Malformed);
        assert_eq!(http.token(&HeaderMap::new()), Token::Missing);
    }

    #[test]
    fn a_hec_receiver_takes_a_splunk_token() {
        let hec = server(Speaks::Hec(BTreeMap::new()), &["one"], INBOX_BYTES);
        assert_eq!(hec.token(&with("Splunk one")), Token::Admitted { acknowledges: false });
        assert_eq!(hec.token(&with("Bearer one")), Token::Malformed);
        assert_eq!(hec.token(&with("Splunk two")), Token::Unknown);
    }

    /// HEC answers in its own codes, which its clients read to decide whether to send again.
    #[tokio::test]
    async fn a_hec_receiver_answers_in_hecs_codes() {
        let hec = server(Speaks::Hec(BTreeMap::new()), &["one"], INBOX_BYTES);
        let read = |r: Response| async move {
            let status = r.status();
            let body = axum::body::to_bytes(r.into_body(), 1024).await.unwrap();
            let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
            (status.as_u16(), json["code"].as_i64())
        };
        assert_eq!(read(hec.answer(Outcome::Delivered)).await, (200, Some(0)));
        assert_eq!(read(hec.answer(Outcome::Refused(Token::Missing))).await, (401, Some(2)));
        assert_eq!(
            read(hec.answer(Outcome::Refused(Token::Malformed))).await,
            (401, Some(3))
        );
        assert_eq!(read(hec.answer(Outcome::Refused(Token::Unknown))).await, (403, Some(4)));
        assert_eq!(read(hec.answer(Outcome::Busy)).await, (503, Some(9)));
        assert_eq!(read(hec_refused(hec::Invalid::NO_DATA)).await, (400, Some(5)));
    }

    /// A refusal is read from the answer every protocol gives, gRPC's status included; a delivery is no refusal.
    #[test]
    fn a_refusal_is_named_from_its_answer() {
        let hec = server(Speaks::Hec(BTreeMap::new()), &["one"], INBOX_BYTES);
        assert_eq!(refusal(&hec.answer(Outcome::Delivered)), None);
        assert_eq!(refusal(&hec.answer(Outcome::Refused(Token::Unknown))), Some("token"));
        assert_eq!(refusal(&hec.answer(Outcome::TooLarge)), Some("too_large"));
        assert_eq!(refusal(&hec.answer(Outcome::Busy)), Some("busy"));
        assert_eq!(refusal(&hec_refused(hec::Invalid::NO_DATA)), Some("unreadable"));
        assert_eq!(refusal(&grpc_answer(0, "", Some(Vec::new()))), None);
        assert_eq!(refusal(&grpc_answer(16, "no token", None)), Some("token"));
        assert_eq!(refusal(&grpc_answer(14, "busy", None)), Some("busy"));
    }

    /// What `shared` holds once it holds `n` records, taken on a thread of its own since a take blocks.
    async fn take_all(shared: &Arc<Shared>, n: usize) -> Vec<Queued> {
        let shared = shared.clone();
        tokio::task::spawn_blocking(move || {
            let mut taken = Vec::new();
            let deadline = Instant::now() + Duration::from_secs(10);
            while taken.len() < n && Instant::now() < deadline {
                taken.extend(shared.take(n - taken.len(), 1 << 20, Duration::from_millis(50)));
            }
            taken
        })
        .await
        .unwrap()
    }

    /// Serves a receiver that streams whose handler admits each read as lines and writes `ok` once they are
    /// delivered.
    fn serve_lines(tls: Option<rustls::ServerConfig>) -> (SocketAddr, Arc<Shared>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let at = listener.local_addr().unwrap();
        let shared = Arc::new(Shared::default());
        tokio::spawn(serve_streams(
            tokio::net::TcpListener::from_std(listener).unwrap(),
            tls,
            shared.clone(),
            |mut c: Connection| async move {
                let mut read = vec![0; 1024];
                let mut held = c.inlet.held();
                while let Ok(n @ 1..) = c.stream.read(&mut read).await {
                    let lines = Records::lines(read[..n].to_vec());
                    if held.hold(n).await.is_err() {
                        return;
                    }
                    let Ok(delivered) = c.inlet.admit(Cutter::Records(lines), held.part(n)).await else {
                        return;
                    };
                    if delivered.await.is_err() || c.stream.write_all(b"ok").await.is_err() {
                        return;
                    }
                }
            },
        ));
        (at, shared)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_streamed_request_is_admitted_and_answered_once_committed() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let (at, shared) = serve_lines(None);
        let mut sender = tokio::net::TcpStream::connect(at).await.unwrap();
        sender.write_all(b"a\nb\n").await.unwrap();
        let taken = take_all(&shared, 2).await;
        assert_eq!(
            taken.iter().map(|q| q.bytes().to_vec()).collect::<Vec<_>>(),
            [b"a".to_vec(), b"b".to_vec()]
        );
        assert_eq!(&*taken[0].remote, "127.0.0.1");

        let mut answer = [0; 2];
        let early = tokio::time::timeout(Duration::from_millis(100), sender.read_exact(&mut answer)).await;
        assert!(early.is_err(), "nothing is answered before it is committed");
        shared.answer_through(1);
        sender.read_exact(&mut answer).await.unwrap();
        assert_eq!(&answer, b"ok");
    }

    /// A stream cannot be told busy, so its request, whose bytes its connection already took, waits while the source
    /// is blocked.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_streamed_request_waits_while_the_source_is_blocked() {
        let shared = Arc::new(Shared::default());
        let mut source = ReceiverSource {
            shared: shared.clone(),
            serving: None,
            port: None,
        };
        source.block().unwrap();
        let waiting = tokio::spawn({
            let shared = shared.clone();
            async move {
                let lines = Cutter::Records(Records::lines(b"b\n".to_vec()));
                shared.admit_waiting(lines, taken(&shared, 2), remote()).await
            }
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!waiting.is_finished(), "the source is blocked");
        source.release().unwrap();
        assert!(waiting.await.unwrap().is_ok());
        assert_eq!(take_all(&shared, 1).await[0].bytes(), b"b");
        assert_eq!(shared.holdings.held(), 0);
    }

    /// What the source says it holds is what its takers hold: a charge's bytes while it is taken, none once given
    /// back.
    #[test]
    fn a_receiver_holds_what_its_charges_take() {
        let shared = Arc::new(Shared {
            holdings: Holdings::reserving(1024, None),
            ..Shared::default()
        });
        let source = ReceiverSource {
            shared: shared.clone(),
            serving: None,
            port: None,
        };
        assert_eq!(source.held_bytes(), Some(0));
        let mut charge = Charge::new(&shared.holdings);
        assert!(charge.take(300));
        assert_eq!(source.held_bytes(), Some(300));
        assert!(charge.resize(100));
        assert_eq!(source.held_bytes(), Some(100));
        drop(charge);
        assert_eq!(source.held_bytes(), Some(0));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_streamed_request_waiting_is_refused_once_the_receiver_closes() {
        let shared = Arc::new(Shared::default());
        shared.state.lock().unwrap().blocked = true;
        let waiting = tokio::spawn({
            let shared = shared.clone();
            async move {
                let lines = Cutter::Records(Records::lines(b"a\n".to_vec()));
                shared.admit_waiting(lines, taken(&shared, 2), remote()).await
            }
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        shared.close();
        assert!(matches!(waiting.await.unwrap(), Err(Closed)));
        let empty = Cutter::Records(Records::lines(Vec::new()));
        let mut now = shared.admit_waiting(empty, taken(&shared, 1), remote()).await.unwrap();
        assert!(answered(&mut now), "a request of no records is answered at once");
        assert_eq!(shared.holdings.held(), 0, "what each took is given back");
    }

    /// One connection's failed handshake ends that connection, and the next sender is served.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_failed_handshake_ends_only_its_own_connection() {
        use crate::plan::topology::SecretRef;
        use std::io::{Read, Write};

        let key = rcgen::KeyPair::generate().unwrap();
        let certificate = rcgen::CertificateParams::new(vec!["localhost".to_string()])
            .unwrap()
            .self_signed(&key)
            .unwrap();
        let mut secrets = HeldSecrets::default();
        secrets.take([crate::heartbeat::SecretDelivery {
            name: "receiver".into(),
            version: 1,
            value: key.serialize_pem(),
            chain: Some(certificate.pem()),
        }]);
        let identity = Identity {
            id: "receiver".into(),
            certificate: Some(SecretRef::LeaderStored {
                name: "receiver".into(),
                version: 1,
            }),
            authorities: None,
            trust_public: true,
            presented: crate::plan::topology::PresentedAs {
                client: false,
                server: true,
            },
        };
        let tls = server_tls(&identity, &secrets).unwrap();
        assert!(tls.alpn_protocols.is_empty(), "a stream offers no application protocol");
        let (at, shared) = serve_lines(Some(tls));

        let mut plain = tokio::net::TcpStream::connect(at).await.unwrap();
        tokio::io::AsyncWriteExt::write_all(&mut plain, b"not a handshake\n")
            .await
            .unwrap();

        let der = certificate.der().clone();
        let sent = std::thread::spawn(move || {
            let mut roots = rustls::RootCertStore::empty();
            roots.add(der).unwrap();
            let config = rustls::ClientConfig::builder_with_provider(crate::crypto::in_use())
                .with_safe_default_protocol_versions()
                .unwrap()
                .with_root_certificates(roots)
                .with_no_client_auth();
            let connection = rustls::ClientConnection::new(Arc::new(config), "localhost".try_into().unwrap()).unwrap();
            let mut stream = rustls::StreamOwned::new(connection, std::net::TcpStream::connect(at).unwrap());
            stream.write_all(b"over tls\n").unwrap();
            let mut answer = [0; 2];
            stream.read_exact(&mut answer).unwrap();
            answer
        });
        let taken = take_all(&shared, 1).await;
        assert_eq!(taken[0].bytes(), b"over tls");
        shared.answer_through(0);
        assert_eq!(
            &tokio::task::spawn_blocking(move || sent.join().unwrap()).await.unwrap(),
            b"ok"
        );
    }

    #[test]
    fn a_held_socket_is_kept_until_its_address_is_dropped() {
        let mut sockets = Sockets::default();
        let address = TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap();
        let tcp = Listen {
            transport: Transport::Tcp,
            address,
        };
        let udp = Listen {
            transport: Transport::Udp,
            address,
        };
        sockets.hold(&[tcp, udp]);
        if cfg!(unix) {
            assert!(sockets.held.contains_key(&tcp) && sockets.held.contains_key(&udp));
            assert!(TcpListener::bind(address).is_err(), "the supervisor holds it");
            assert!(
                std::net::UdpSocket::bind(address).is_err(),
                "the supervisor holds UDP on it too"
            );
        }
        #[cfg(unix)]
        {
            assert_eq!(sockets.handed(&[tcp, udp]).1.len(), 2);
            assert_eq!(
                sockets.handed(&[udp]).0,
                format!("udp:{address}={}", sockets.handed(&[udp]).1[0]),
                "a UDP socket is named apart from TCP on the same port"
            );
            assert!(
                sockets.handed(&[]).1.is_empty(),
                "a runtime is handed only what its target serves"
            );
        }
        sockets.keep_only(&[udp]);
        assert!(!sockets.held.contains_key(&tcp));
        sockets.keep_only(&[]);
        assert!(sockets.held.is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn a_handed_socket_is_named_by_its_transport_and_address() {
        for name in ["[::]:514", "udp:[::]:514", "udp:10.0.0.5:6514"] {
            assert_eq!(named(unnamed(name).unwrap()), name);
        }
        assert_eq!(unnamed("udp:[::]:514").unwrap().transport, Transport::Udp);
        assert_eq!(unnamed("[::]:514").unwrap().transport, Transport::Tcp);
        assert!(unnamed("tcp:[::]:514").is_none(), "only UDP is prefixed");
    }

    #[cfg(unix)]
    #[test]
    fn a_socket_that_fell_back_to_ipv4_still_serves_every_address() {
        let every: SocketAddr = "[::]:8080".parse().unwrap();
        assert!(serves(every, every));
        assert!(serves("0.0.0.0:8080".parse().unwrap(), every));
        assert!(!serves("0.0.0.0:8081".parse().unwrap(), every));
        assert!(!serves("0.0.0.0:8080".parse().unwrap(), "[::1]:8080".parse().unwrap()));
        assert!(!serves("127.0.0.1:8080".parse().unwrap(), every));
    }
}
