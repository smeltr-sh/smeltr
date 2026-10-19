//! A service on this host for the fuzzers of the destinations on the HTTP destination core (`io::http`), and the
//! checks they share of what it read.

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::value::RawValue;
use smeltr::io::{OwnedRecord, Rejection};

/// A row, by its batch and its place in the batch.
pub type Id = (usize, usize);

/// A row and what becomes of it.
pub struct Sent {
    pub row: OwnedRecord,
    /// The row as one item of a request carries it.
    pub item: Vec<u8>,
    /// The service refuses any request carrying it.
    pub poison: bool,
}

/// One request as the service read it.
#[derive(Clone, Debug)]
pub struct Seen {
    pub method: String,
    pub target: String,
    /// Each header's name, lowercase, and its value, trimmed, in the order they came.
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Seen {
    /// Every value of the header `name`, lowercase, so a caller can tell one from several.
    pub fn header(&self, name: &str) -> Vec<&str> {
        (self.headers.iter())
            .filter(|(n, _)| n == name)
            .map(|(_, v)| v.as_str())
            .collect()
    }
}

/// What the service answers a request with. A `Connection` header among `headers` closes the connection after it.
pub struct Answer {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: String,
}

/// How a fuzzer's service answers each request, read on the connection numbered `conn`.
pub trait Answers: Send + 'static {
    fn answer(&mut self, seen: &Seen, conn: usize) -> Answer;
}

/// FNV-1a: a hash that is the same in every build, so an answer drawn from it replays.
pub fn fnv(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf29ce484222325, |h, b| {
        (h ^ u64::from(*b)).wrapping_mul(0x100000001b3)
    })
}

/// A service on this host, over TLS when it has a configuration to present, answering by its script. Dropped, it
/// stops taking connections.
pub struct Service {
    addr: SocketAddr,
    stop: Arc<AtomicBool>,
}

impl Service {
    /// Answers on `listener`, bound before the destination was drawn so its address could name the port.
    pub fn start<A: Answers>(
        listener: TcpListener,
        tls: Option<Arc<rustls::ServerConfig>>,
        script: Arc<Mutex<A>>,
    ) -> Service {
        let addr = listener.local_addr().unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let stopped = stop.clone();
        std::thread::spawn(move || {
            for (conn, stream) in listener.incoming().enumerate() {
                if stopped.load(Ordering::SeqCst) {
                    return;
                }
                let Ok(stream) = stream else { continue };
                let script = script.clone();
                let tls = tls.clone();
                std::thread::spawn(move || {
                    let _ = stream.set_read_timeout(Some(Duration::from_secs(30)));
                    match tls {
                        Some(config) => {
                            let Ok(connection) = rustls::ServerConnection::new(config) else {
                                return;
                            };
                            serve(rustls::StreamOwned::new(connection, stream), &script, conn);
                        }
                        None => serve(stream, &script, conn),
                    }
                });
            }
        });
        Service { addr, stop }
    }
}

impl Drop for Service {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        // Wakes the accept the listener is blocked in.
        let _ = TcpStream::connect(self.addr);
    }
}

/// Reads requests off connection `conn` until the client closes it or an answer does.
fn serve<S: Read + Write, A: Answers>(stream: S, script: &Mutex<A>, conn: usize) {
    let mut reader = BufReader::new(stream);
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).unwrap_or(0) == 0 {
            return;
        }
        let mut parts = line.trim_end().splitn(3, ' ');
        let method = parts.next().unwrap_or_default().to_string();
        let target = parts.next().unwrap_or_default().to_string();
        let mut headers = Vec::new();
        loop {
            let mut h = Vec::new();
            if reader.read_until(b'\n', &mut h).unwrap_or(0) == 0 {
                return;
            }
            if h == b"\r\n" {
                break;
            }
            let h = String::from_utf8_lossy(&h).trim_end_matches(['\r', '\n']).to_string();
            let (name, value) = h.split_once(':').unwrap_or((&h, ""));
            headers.push((name.to_ascii_lowercase(), value.trim().to_string()));
        }
        let length: usize = (headers.iter())
            .find(|(n, _)| n == "content-length")
            .map_or(0, |(_, v)| v.parse().unwrap_or(0));
        let mut body = vec![0; length];
        if reader.read_exact(&mut body).is_err() {
            return;
        }
        let seen = Seen {
            method,
            target,
            headers,
            body,
        };
        let answer = script.lock().unwrap().answer(&seen, conn);
        let close = answer.headers.iter().any(|(n, _)| n == "Connection");
        let mut head = format!(
            "HTTP/1.1 {} Fuzz\r\nContent-Length: {}\r\n",
            answer.status,
            answer.body.len()
        );
        for (name, value) in &answer.headers {
            head.push_str(&format!("{name}: {value}\r\n"));
        }
        head.push_str("\r\n");
        head.push_str(&answer.body);
        let out = reader.get_mut();
        if out.write_all(head.as_bytes()).and_then(|()| out.flush()).is_err() || close {
            return;
        }
    }
}

/// `body`, gunzipped.
pub fn gunzip(body: &[u8]) -> Result<Vec<u8>, String> {
    let mut plain = Vec::new();
    flate2::read::GzDecoder::new(body)
        .read_to_end(&mut plain)
        .map_err(|e| format!("not gzip: {e}"))?;
    Ok(plain)
}

/// The rows `body` carries, in order, when it is exactly their items as one JSON array. `find` names the row an item
/// is.
pub fn json_array_rows(body: &[u8], find: impl Fn(&[u8]) -> Result<Id, String>) -> Result<Vec<Id>, String> {
    let elements: Vec<&RawValue> = serde_json::from_slice(body).map_err(|e| format!("not a JSON array: {e}"))?;
    let ids = (elements.iter())
        .map(|e| find(e.get().as_bytes()))
        .collect::<Result<Vec<Id>, String>>()?;
    let items: Vec<&[u8]> = elements.iter().map(|e| e.get().as_bytes()).collect();
    let exact = [b"[".as_slice(), &items.join(&b","[..]), b"]"].concat();
    if exact != body {
        return Err(format!("not exactly its items: {:?}", String::from_utf8_lossy(body)));
    }
    Ok(ids)
}

/// What is wrong with a request carrying `ids` of `sent`, under a request limit of `max` bytes: rows of several
/// batches, out of order, none, or more than the limit holds.
pub fn wrong_rows(ids: &[Id], sent: &[Vec<Sent>], max: usize) -> Vec<String> {
    let mut wrong = Vec::new();
    if ids.is_empty() {
        wrong.push("no rows".to_string());
    }
    if ids.windows(2).any(|w| w[0].0 != w[1].0 || w[0].1 >= w[1].1) {
        wrong.push(format!("rows of several batches, or out of order: {ids:?}"));
    }
    // A request stays within the limit, unless it is one row larger than that.
    let held: usize = ids.iter().map(|(b, r)| sent[*b][*r].item.len() + 1).sum();
    if ids.len() > 1 && held > max {
        wrong.push(format!("{held} bytes in {} rows, past the limit of {max}", ids.len()));
    }
    wrong
}

/// How many requests the fit rows of `rows` make, batched under a request limit of `max` bytes, when every one
/// lands: one each time the next row would pass the limit, and one for the rest.
pub fn requests_for(max: usize, rows: &[Sent]) -> usize {
    let (mut count, mut held) = (0, 0);
    for s in rows.iter().filter(|s| s.row.unfit.is_none()) {
        let size = s.item.len() + 1;
        if held > 0 && held + size > max {
            count += 1;
            held = 0;
        }
        held += size;
    }
    count + usize::from(held > 0)
}

/// Checks that every row of `sent` landed once, by `landed`, or was refused as it had to be: an unfit row for why it
/// is unfit, and a poisoned one for the status `split` the service refused it with.
pub fn check_landed(
    sent: &[Vec<Sent>],
    refused: &[Vec<Rejection>],
    landed: &BTreeMap<Id, usize>,
    split: u16,
) -> Result<(), String> {
    super::destination::check_refused(sent, refused, |s| s.poison || s.row.unfit.is_some())?;
    for (batch, rows) in sent.iter().enumerate() {
        for Rejection {
            doc: i, reason: why, ..
        } in &refused[batch]
        {
            let because = match &rows[*i].row.unfit {
                Some(unfit) => unfit.clone(),
                None => format!("HTTP {split}"),
            };
            if !why.contains(&because) {
                return Err(format!("batch {batch} row {i} refused for {why:?}, not {because:?}"));
            }
        }
        for (i, s) in rows.iter().enumerate() {
            let times = landed.get(&(batch, i)).copied().unwrap_or(0);
            let once = usize::from(!s.poison && s.row.unfit.is_none());
            if times != once {
                return Err(format!("batch {batch} row {i} landed {times} times, not {once}"));
            }
        }
    }
    Ok(())
}
