//! A sender that misbehaves at the transport, on this host, as `tests/live/socket-sender/send.py` does from a
//! container: it writes a connection's bytes in pieces with pauses between them, speaks plain TCP to TLS or presents
//! a certificate of any kind, ends by a hang-up, a reset or a half-close, holds connections idle or stalled, and
//! storms. It knows nothing of what it sends, but where told to it reads HTTP answers, one for each request.

use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{IpAddr, Shutdown, SocketAddr, TcpStream};
use std::time::Duration;

use crate::harness::receiver::{Pki, Presents};

/// How long a sender waits on the receiver for a read, an answer or a hang-up.
const PATIENCE: Duration = Duration::from_secs(15);

/// How a connection speaks to the receiver.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Speaks {
    /// Plain TCP, to a receiver with or without TLS.
    Plain,
    /// TLS, presenting a certificate or none.
    Tls(Presents),
}

/// How a connection ends.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum End {
    /// A hang-up; under TLS, said first with close_notify.
    HangUp,
    /// Under TLS, a hang-up without close_notify.
    Unsaid,
    /// A reset.
    Reset,
    /// It shuts down writing, waits, and then reads what the receiver answers.
    HalfClose,
}

/// One connection, as a sender writes it.
#[derive(Clone, Debug)]
pub struct Connection {
    /// The address it connects from, on this host.
    pub from: IpAddr,
    pub speaks: Speaks,
    pub bytes: Vec<u8>,
    /// How many of its bytes it writes at a time, and how long it pauses after each.
    pub pieces: Vec<(usize, Duration)>,
    pub end: End,
    /// How many HTTP answers it reads before it ends, or `None` where the receiver answers with nothing to read, as a
    /// stream's does.
    pub answers: Option<usize>,
}

/// What became of a connection.
#[derive(Debug, Default)]
pub struct Sent {
    pub connected: bool,
    /// How many of its bytes the kernel took before a write failed, if one did.
    pub written: usize,
    /// The status of each answer it read, in order.
    pub statuses: Vec<u16>,
    /// Why the receiver did not hang up once the sender had, where it did not.
    pub left_open: Option<String>,
}

/// A connection over TCP or TLS.
pub enum Stream {
    Plain(TcpStream),
    Tls(Box<rustls::StreamOwned<rustls::ClientConnection, TcpStream>>),
}

impl Stream {
    fn tcp(&self) -> &TcpStream {
        match self {
            Stream::Plain(tcp) => tcp,
            Stream::Tls(tls) => &tls.sock,
        }
    }
}

impl Read for Stream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Stream::Plain(tcp) => tcp.read(buf),
            Stream::Tls(tls) => tls.read(buf),
        }
    }
}

impl Write for Stream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Stream::Plain(tcp) => tcp.write(buf),
            Stream::Tls(tls) => tls.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Stream::Plain(tcp) => tcp.flush(),
            Stream::Tls(tls) => tls.flush(),
        }
    }
}

/// A TCP connection to `at` from `from`, each write its own segment so the receiver more often reads a message in
/// parts.
pub fn connect(at: SocketAddr, from: IpAddr) -> io::Result<TcpStream> {
    let socket = socket2::Socket::new(socket2::Domain::for_address(at), socket2::Type::STREAM, None)?;
    socket.bind(&SocketAddr::new(from, 0).into())?;
    socket.connect_timeout(&at.into(), PATIENCE)?;
    let tcp: TcpStream = socket.into();
    tcp.set_nodelay(true)?;
    tcp.set_read_timeout(Some(PATIENCE))?;
    tcp.set_write_timeout(Some(PATIENCE))?;
    Ok(tcp)
}

/// `tcp`, under TLS where `speaks` says. The handshake is made with the first write or read.
fn speaking(tcp: TcpStream, speaks: Speaks, pki: &Pki) -> Stream {
    match speaks {
        Speaks::Plain => Stream::Plain(tcp),
        Speaks::Tls(presents) => {
            let tls = rustls::ClientConnection::new(pki.client(presents), "localhost".try_into().unwrap()).unwrap();
            Stream::Tls(Box::new(rustls::StreamOwned::new(tls, tcp)))
        }
    }
}

/// Closes `tcp` with a reset.
fn reset(tcp: &TcpStream) {
    let _ = socket2::SockRef::from(tcp).set_linger(Some(Duration::ZERO));
}

/// Reads until the receiver hangs up, so a close leaves no unread bytes to reset the connection with. Returns why the
/// receiver did not hang up, where it did not.
fn drained(stream: &mut Stream) -> Option<String> {
    match io::copy(stream, &mut io::sink()) {
        Err(e) if matches!(e.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut) => {
            Some("the receiver left the connection open".into())
        }
        _ => None,
    }
}

/// One HTTP answer's status, its head and body read; `None` where the connection ended first.
fn answer(reader: &mut impl BufRead) -> Option<u16> {
    let mut line = String::new();
    let mut status = None;
    let (mut length, mut chunked) = (0, false);
    loop {
        line.clear();
        if reader.read_line(&mut line).ok()? == 0 {
            return None;
        }
        let trimmed = line.trim_end();
        if trimmed.is_empty() {
            break;
        }
        let lower = trimmed.to_ascii_lowercase();
        match status {
            None => status = Some(lower.get(9..12)?.parse().ok()?),
            Some(_) => match lower.split_once(':') {
                Some(("content-length", n)) => length = n.trim().parse().ok()?,
                Some(("transfer-encoding", e)) => chunked = e.contains("chunked"),
                _ => {}
            },
        }
    }
    if chunked {
        loop {
            line.clear();
            reader.read_line(&mut line).ok()?;
            let size = usize::from_str_radix(line.trim().split(';').next()?, 16).ok()?;
            io::copy(&mut reader.take(size as u64 + 2), &mut io::sink()).ok()?;
            if size == 0 {
                break;
            }
        }
    } else {
        io::copy(&mut reader.take(length), &mut io::sink()).ok()?;
    }
    status
}

/// Reads up to `n` answers from `stream`.
fn answers(stream: &mut Stream, n: usize, sent: &mut Sent) {
    let mut reader = BufReader::new(stream);
    for _ in 0..n {
        match answer(&mut reader) {
            Some(status) => sent.statuses.push(status),
            None => return,
        }
    }
}

/// Writes `connection` to the receiver at `at` and ends it as it says.
pub fn send(at: SocketAddr, pki: &Pki, connection: &Connection) -> Sent {
    let mut sent = Sent::default();
    let Ok(tcp) = connect(at, connection.from) else {
        return sent;
    };
    sent.connected = true;
    let mut stream = speaking(tcp, connection.speaks, pki);
    for &(piece, pause) in &connection.pieces {
        let bytes = &connection.bytes[sent.written..sent.written + piece];
        if stream.write_all(bytes).and_then(|()| stream.flush()).is_err() {
            break;
        }
        sent.written += piece;
        std::thread::sleep(pause);
    }
    match connection.end {
        End::Reset => {
            reset(stream.tcp());
            return sent;
        }
        End::HalfClose => {
            let _ = stream.tcp().shutdown(Shutdown::Write);
            std::thread::sleep(Duration::from_millis(300));
            answers(&mut stream, connection.answers.unwrap_or(0), &mut sent);
        }
        End::HangUp | End::Unsaid => {
            answers(&mut stream, connection.answers.unwrap_or(0), &mut sent);
            if let (Stream::Tls(tls), End::HangUp) = (&mut stream, connection.end) {
                tls.conn.send_close_notify();
                let _ = tls.flush();
            }
            let _ = stream.tcp().shutdown(Shutdown::Write);
        }
    }
    sent.left_open = drained(&mut stream);
    sent
}

/// What a held connection does until it is let go.
#[derive(Clone, Debug, PartialEq)]
pub enum Holds {
    /// Sends nothing.
    Idle,
    /// Sends a TLS record header promising a ClientHello that never comes.
    Handshake,
    /// Writes these bytes, part of a message, and no more.
    Partway(Vec<u8>),
}

/// A connection held open until it is let go.
pub struct Held {
    stream: Stream,
}

/// Opens a connection to `at` from `from` and holds it as `holds` says, speaking as `speaks`; `None` where it could
/// not connect.
pub fn hold(at: SocketAddr, from: IpAddr, speaks: Speaks, holds: &Holds, pki: &Pki) -> Option<Held> {
    let tcp = connect(at, from).ok()?;
    let mut stream = match holds {
        Holds::Handshake => Stream::Plain(tcp),
        _ => speaking(tcp, speaks, pki),
    };
    let _ = match holds {
        Holds::Idle => Ok(()),
        Holds::Handshake => stream.write_all(b"\x16\x03\x01\x02\x00\x01"),
        Holds::Partway(bytes) => stream.write_all(bytes).and_then(|()| stream.flush()),
    };
    Some(Held { stream })
}

impl Held {
    /// Whether the receiver has closed it, waiting up to `within` to see. Anything the receiver sends first, such as a
    /// TLS handshake's answer, is read and ignored.
    pub fn closed(&mut self, within: Duration) -> bool {
        let mut tcp = self.stream.tcp();
        tcp.set_read_timeout(Some(within)).unwrap();
        let mut buf = [0; 16 * 1024];
        loop {
            match tcp.read(&mut buf) {
                Ok(0) => return true,
                Ok(_) => continue,
                Err(e) if matches!(e.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut) => return false,
                Err(_) => return true,
            }
        }
    }

    /// Hangs up.
    pub fn let_go(self) {
        let _ = self.stream.tcp().shutdown(Shutdown::Both);
    }
}

/// Opens `n` connections to `at` from `from` and ends each at once, every other one by a reset. How many connected.
pub fn storm(at: SocketAddr, from: IpAddr, n: usize) -> usize {
    (0..n)
        .filter(|i| match connect(at, from) {
            Ok(tcp) => {
                if i % 2 == 1 {
                    reset(&tcp);
                }
                true
            }
            Err(_) => false,
        })
        .count()
}
