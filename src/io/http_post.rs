//! The HTTP destination, as a [`Protocol`] over the HTTP core (`io::http`): each batch POSTed to one URL, its rows as
//! lines, as a JSON array, or one row per request. Bytes go as they are one per request, and as base64 in a batch. Any 2xx lands every row the request carried.
//!
//! What an answer means is fixed rather than configured, since nearly every intake means the same by it:
//!
//! | status | meaning |
//! |---|---|
//! | 2xx | landed |
//! | 408, 429, 500, 502, 503, 504 | retried, backing off |
//! | 400, 413, 422 | split until the row at fault is alone, which is refused |
//! | anything else | the destination stalls, naming the status |

use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::http::{HttpDestination, HttpTransport, Outcome, Protocol, Response, SharedSignIn, push_json_string};
use super::{IoError, OwnedRecord, Payload};
use crate::format::bytes_text::BytesForm;
use crate::plan::topology::{Compression, HttpDestinationConfig, RequestBody};

/// One row as its request carries it.
#[derive(Clone, Debug)]
pub struct Item {
    /// Under [`RequestBody::Single`] each row's own, so no two share a request; otherwise 0.
    part: usize,
    bytes: Vec<u8>,
}

/// The request and the answer.
#[derive(Clone, Debug)]
pub struct Post {
    /// How rows make a request's body.
    shape: RequestBody,
    gzip: bool,
    /// The URL's trailing slashes, which the transport trims off its base address and this puts back.
    path: String,
    /// Where the next row's part is taken from under [`RequestBody::Single`]. Shared by every clone, though only the
    /// egress thread admits.
    next_part: Arc<AtomicUsize>,
    /// Names the service in errors and refusals.
    name: &'static str,
}

impl Post {
    pub fn new(config: &HttpDestinationConfig) -> Post {
        let url = config.url.as_str();
        Post {
            name: "http",
            shape: config.body,
            gzip: config.compression == Compression::Gzip,
            path: url[url.trim_end_matches('/').len()..].to_string(),
            next_part: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Names the service `name` in errors and refusals, for a destination of its own built on this one.
    pub fn named(mut self, name: &'static str) -> Post {
        self.name = name;
        self
    }
}

/// A transport to `config.url`, carrying its committed headers and `credentials`, each a header's name and its
/// value as this worker resolved it.
/// The destination posting to `config`'s URL: as `name` in what it says of itself, with `credentials` on every
/// request, and signed in with `sign_in` when there is one. `agent` is [`super::http::agent_config`]'s answer for it.
pub fn destination(
    config: &HttpDestinationConfig,
    credentials: &[(String, String)],
    sign_in: Option<SharedSignIn>,
    name: &'static str,
    agent: &ureq::config::Config,
    (metrics, id): (crate::metrics::MetricsHandle, crate::core::id::DestId),
    subject: &str,
) -> HttpDestination<Post, HttpTransport> {
    let make = || {
        let transport = transport(config, credentials, agent);
        match &sign_in {
            Some(sign_in) => transport.signed_in(sign_in.clone()),
            None => transport,
        }
    };
    HttpDestination::new(make(), Post::new(config).named(name))
        .with_subject(subject)
        .with_metrics(metrics, id)
        .with_max_request_bytes(config.max_request_bytes)
        .with_stall_after(config.stall_after)
        .with_senders_from(make, config.max_in_flight(), config.in_flight)
}

pub fn transport(
    config: &HttpDestinationConfig,
    credentials: &[(String, String)],
    agent: &ureq::config::Config,
) -> HttpTransport {
    let mut headers: Vec<(String, String)> = config.headers.iter().chain(credentials).cloned().collect();
    if config.compression == Compression::Gzip {
        headers.push(("Content-Encoding".to_string(), "gzip".to_string()));
    }
    // A redirect would land nothing: see `HttpTransport::without_redirects`. It would also carry the credential
    // headers to wherever it points.
    HttpTransport::new(&config.url, content_type(config), headers, agent).without_redirects()
}

fn content_type(config: &HttpDestinationConfig) -> &'static str {
    match (config.body, &config.payload) {
        (RequestBody::JsonArray, _) | (RequestBody::Single, Payload::Record) => "application/json",
        (RequestBody::Lines, Payload::Record) => "application/x-ndjson",
        (_, Payload::Text { .. }) => "text/plain; charset=utf-8",
        (RequestBody::Single, Payload::Bytes { .. }) => "application/octet-stream",
        // Base64 lines.
        (RequestBody::Lines, Payload::Bytes { .. }) => "text/plain",
    }
}

/// `text` with each line break written as the two characters `\n` or `\r`.
fn on_one_line(text: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(text.len());
    for &b in text {
        match b {
            b'\n' => out.extend_from_slice(b"\\n"),
            b'\r' => out.extend_from_slice(b"\\r"),
            _ => out.push(b),
        }
    }
    out
}

impl Protocol for Post {
    type Item = Item;

    fn name(&self) -> &'static str {
        self.name
    }

    fn path(&self) -> &str {
        &self.path
    }

    fn part(&self, item: &Item) -> usize {
        item.part
    }

    /// The record as JSON, or the text or bytes as they are. Under `lines` each line is one row, so a line break
    /// in text is written as the two characters `\n` (or `\r`). In a JSON array text is a JSON string. Bytes that
    /// share a request with other rows are base64, as JSON holds bytes: a JSON string in an array, a line of their
    /// own in `lines`.
    fn admit(&self, row: &OwnedRecord) -> Result<Item, String> {
        let bytes = match (&row.payload, self.shape) {
            (Payload::Record, _) => row.value.trim_ascii().to_vec(),
            (Payload::Bytes { .. }, RequestBody::JsonArray) => {
                let mut out = Vec::with_capacity(row.value.len() * 4 / 3 + 6);
                out.push(b'"');
                BytesForm::Base64.write(&row.value, &mut out);
                out.push(b'"');
                out
            }
            (Payload::Bytes { .. }, RequestBody::Lines) => BytesForm::Base64.text(&row.value).into_bytes(),
            (Payload::Text { .. }, RequestBody::JsonArray) => {
                let mut out = Vec::with_capacity(row.value.len() + 2);
                push_json_string(&mut out, &String::from_utf8_lossy(&row.value));
                out
            }
            (Payload::Text { .. }, RequestBody::Lines) => on_one_line(&row.value),
            (Payload::Text { .. } | Payload::Bytes { .. }, RequestBody::Single) => row.value.clone(),
        };
        let part = match self.shape {
            RequestBody::Single => self.next_part.fetch_add(1, Ordering::Relaxed),
            RequestBody::Lines | RequestBody::JsonArray => 0,
        };
        Ok(Item { part, bytes })
    }

    /// The row and its separator.
    fn size(&self, item: &Item) -> usize {
        item.bytes.len() + 1
    }

    fn body(&self, items: &[Item]) -> Vec<u8> {
        let mut body = Vec::with_capacity(items.iter().map(|i| i.bytes.len() + 1).sum::<usize>() + 2);
        match self.shape {
            RequestBody::Lines => {
                for item in items {
                    body.extend_from_slice(&item.bytes);
                    body.push(b'\n');
                }
            }
            RequestBody::JsonArray => {
                body.push(b'[');
                for (i, item) in items.iter().enumerate() {
                    if i > 0 {
                        body.push(b',');
                    }
                    body.extend_from_slice(&item.bytes);
                }
                body.push(b']');
            }
            // One item: a part of its own never shares a request.
            RequestBody::Single => {
                for item in items {
                    body.extend_from_slice(&item.bytes);
                }
            }
        }
        if !self.gzip {
            return body;
        }
        let mut out = flate2::write::GzEncoder::new(Vec::with_capacity(body.len() / 4), flate2::Compression::fast());
        // Writing to a Vec cannot fail.
        out.write_all(&body).expect("gzip into memory");
        out.finish().expect("gzip into memory")
    }

    fn read(&self, response: &Response, items: &[Self::Item]) -> Result<Outcome, IoError> {
        let count = items.len();
        match response.status {
            200..=299 => Ok(Outcome::default()),
            // Every one of these says "not now", and a retry that does not back off would only say it louder.
            408 | 429 | 500 | 502 | 503 | 504 => Ok(Outcome {
                retryable: (0..count).collect(),
                throttled: true,
                ..Outcome::default()
            }),
            // The service took none of it and named no row: the halves find the one it will not take.
            400 | 413 | 422 => Ok(Outcome {
                split: Some(response.describe()),
                ..Outcome::default()
            }),
            401 | 403 => Err(IoError::transient(format!(
                "the service refused this destination's credentials: {}",
                response.describe()
            ))),
            _ => Err(IoError::transient(format!(
                "the service answered {}",
                response.describe()
            ))),
        }
    }
}
