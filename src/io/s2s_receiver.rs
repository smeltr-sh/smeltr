//! What an S2S receiver makes of a Splunk forwarder's connection, from its first byte to records in the inbox and
//! acks back.
//!
//! ```text
//!   forwarder ── signature, capabilities ──► answer ──► ForwarderInfo: its token checked
//!             ── v4, or 0xf9 and v4 in one zlib stream ──► Decoder ──► events
//!   event ──► its channel's bytes ──► events broken by the channel's sourcetype, or at line ends ──► records
//!   records closed by one forwarder event ──► one request, admitted before the connection reads on
//!   request delivered ──► `fa first last`: every id whose bytes no channel still holds open   (ack=1 only)
//! ```
//!
//! A universal forwarder sends a file in chunks cut wherever it cut them, so a channel's chunks are one stream of
//! bytes, broken into events here as an indexer breaks them. An event may take bytes of several forwarder events, so
//! a forwarder event is acked only once every record holding any of its bytes is delivered: the ids a channel still
//! holds bytes of are not acked until the event holding them is broken off.
//!
//! Every byte is understood or the connection is closed. What the connection had admitted stays admitted; what the
//! forwarder sent and no ack covers, it sends again.

use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use aws_lc_rs::constant_time::verify_slices_are_equal;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;

use super::hec::{self, Filed};
use super::line_breaker::{self, BREAK_SLACK, LineBreaker, Searched};
use super::receiver::{Closed, Connection, Cutter, Held, Inlet, STREAM_STEP};
use super::s2s::{self, Decoder, Escapes, Event, Format, Inflate, Key, Message, Value};
use crate::core::variant;
use crate::plan::topology::S2sReceiverConfig;

/// How long a channel of a connection with acks may go without a chunk before the bytes it holds are its last event:
/// a file that stopped mid-line. A forwarder whose window a held line fills sends heartbeats instead, which ack the
/// line early. It outlasts a forwarder's stall, as when its other output group is down, so a stall does not cut an
/// event in two. A connection without acks holds nothing back, and cuts only when it ends.
const QUIET: Duration = Duration::from_secs(30);

/// How far apart two heartbeats come from a forwarder waiting on acks. One waiting sends them every 200 ms; one that is
/// not sends them a whole number of seconds apart, 30 by default. Heartbeats read together after the receiver stalled
/// arrive closer than either.
const WAITING_BEATS: std::ops::Range<Duration> = Duration::from_millis(100)..Duration::from_millis(500);

/// How long a forwarder has from connecting to its first v4 byte. A forwarder sends its handshake at once; one that
/// connects and stays silent would otherwise hold its task until the receiver stops.
const HANDSHAKE: Duration = Duration::from_secs(30);

/// How long a handshake message may be. Capabilities and ForwarderInfo are a few short pairs; the token is in the
/// second, so until it is read the connection is anyone's, and what it may make the receiver hold is this, not
/// `max_request_bytes`.
const HANDSHAKE_BYTES: usize = 64 * 1024;

/// How much of a compressed stream is inflated before the frames in it are read: what the zlib stream may add to
/// what the decoder holds.
const INFLATE_STEP: usize = 64 * 1024;

/// How much one read of the socket takes.
const READ_STEP: usize = 64 * 1024;

/// How many answers a connection's acks may wait behind. A forwarder that reads no acks stops being read once they
/// fill, as a full inbox stops it, rather than holding the receiver's memory.
const ANSWERS: usize = 1024;

/// The forwarder's own keys among an event's fields, which say nothing about the event: the file it read, when it
/// sent it, and, on `ForwarderInfo`, who it is. `_MetaData:Index` is read as the index.
const BOOKKEEPING: [&[u8]; 5] = [b"_path", b"_tcpoutTime", b"_guid", b"_ingLatChained", b"_ingLatColor"];

/// Splunk's static dictionary entries a field is keyed by when the forwarder parsed the event's timestamp:
/// `date_second` to `date_zone`, and `timestamp`. The event's time says when it happened.
const TIMESTAMP_KEYS: [u64; 9] = [1, 2, 3, 4, 5, 6, 7, 8, 46];

/// The static dictionary entry a field is keyed by that carries the fraction of the event's second.
const SUBSECOND_KEY: u64 = 0;

/// The field that names where the forwarder filed an event: absent for the index a receiver chooses.
const INDEX_KEY: &[u8] = b"_MetaData:Index";
const DEFAULT_INDEX: &[u8] = b"default";

/// Records cut from a connection's stream and not yet admitted, held in one buffer so a frame of many short lines
/// costs about what was sent: each record its bytes and where it ends, and the records cut from one chunk one filed
/// set between them.
#[derive(Default)]
pub(super) struct Cuts {
    bytes: Vec<u8>,
    /// Where each record ends in `bytes`. What a connection cuts at once is at most a frame and what its channels
    /// hold, each no more than `maxRequestBytes`, so an offset fits.
    ends: Vec<u32>,
    /// The index of the first record each filed set is for, in order; it is for every record up to the next's.
    filed: Vec<(usize, Arc<Filed>)>,
    /// The records already taken, and the filed sets passed.
    taken: usize,
    filed_taken: usize,
}

impl Cuts {
    fn push(&mut self, record: &[u8], filed: &Arc<Filed>) {
        let index = self.ends.len();
        if !self.filed.last().is_some_and(|(_, last)| Arc::ptr_eq(last, filed)) {
            self.filed.push((index, filed.clone()));
        }
        self.bytes.extend_from_slice(record);
        self.ends.push(self.bytes.len() as u32);
    }

    fn is_empty(&self) -> bool {
        self.taken == self.ends.len()
    }

    fn append(&mut self, other: Cuts) {
        for (record, filed) in other {
            self.push(&record, &filed);
        }
    }

    /// What its records hold once taken, each counted with what it is filed under, as the inbox counts a record, and
    /// what holding them costs until then.
    pub(super) fn size(&self) -> usize {
        let mut size = self.bytes.len() + self.ends.len() * size_of::<u32>();
        for (i, (first, filed)) in self.filed.iter().enumerate() {
            let next = self.filed.get(i + 1).map_or(self.ends.len(), |(next, _)| *next);
            size += (next - first) * filed.iter().map(|(k, v)| k.len() + v.len()).sum::<usize>();
        }
        size
    }
}

impl Iterator for Cuts {
    type Item = (Vec<u8>, Arc<Filed>);

    fn next(&mut self) -> Option<(Vec<u8>, Arc<Filed>)> {
        let end = *self.ends.get(self.taken)? as usize;
        let start = match self.taken {
            0 => 0,
            i => self.ends[i - 1] as usize,
        };
        while self
            .filed
            .get(self.filed_taken + 1)
            .is_some_and(|(first, _)| *first <= self.taken)
        {
            self.filed_taken += 1;
        }
        self.taken += 1;
        let filed = self.filed[self.filed_taken].1.clone();
        Some((self.bytes[start..end].to_vec(), filed))
    }
}

/// What a connection does that an S2S receiver serves.
pub(super) struct Handler {
    /// Where an event ends, by sourcetype.
    sourcetypes: BTreeMap<String, LineBreaker>,
    /// The tokens a forwarder may present. Empty admits whoever TLS admitted.
    tokens: Vec<Vec<u8>>,
    /// The largest frame, v3 message or record.
    max_request_bytes: usize,
}

/// Why a connection ended.
enum End {
    /// The forwarder hung up, or the connection broke.
    Gone,
    /// The receiver is stopping.
    Stopping,
    /// Bytes this receiver does not understand, at `at` of `within`.
    Malformed {
        at: u64,
        within: &'static str,
        what: String,
    },
    /// Its token is missing or not one this receiver takes.
    Token(&'static str),
    /// It is set to stay on v3 (`enableOldS2SProtocol`), which this receiver does not read.
    OldProtocol,
    /// It did not finish its handshake within [`HANDSHAKE`].
    Silent,
}

impl From<Closed> for End {
    fn from(_: Closed) -> End {
        End::Stopping
    }
}

/// One forwarder event's answer, and the ids to ack once it fires.
type Answer = (oneshot::Receiver<()>, Option<(u64, u64)>);

/// Where a connection's answers go to have their acks written in order, and how many are not yet written.
struct Answering {
    answers: mpsc::Sender<Answer>,
    unwritten: Arc<AtomicUsize>,
}

impl Answering {
    async fn send(&self, answer: Answer) {
        self.unwritten.fetch_add(1, Ordering::SeqCst);
        // A writer that stopped stopped on a broken connection, which the reader hears of itself.
        let _ = self.answers.send(answer).await;
    }

    /// Whether every answer sent has fired and had its acks written: nothing admitted waits on delivery.
    fn all_written(&self) -> bool {
        self.unwritten.load(Ordering::SeqCst) == 0
    }
}

impl Handler {
    pub(super) fn new(config: &S2sReceiverConfig, tokens: Vec<Vec<u8>>, max_request_bytes: usize) -> Handler {
        Handler {
            sourcetypes: config.sourcetypes.clone(),
            tokens,
            max_request_bytes,
        }
    }

    /// Serves one forwarder's connection until it ends, and logs why when it was refused.
    pub(super) async fn serve(self: Arc<Self>, connection: Connection) {
        let Connection { stream, inlet, .. } = connection;
        let (reader, writer) = tokio::io::split(stream);
        let mut wire = Wire {
            reader,
            held: Vec::new(),
            taken: 0,
        };
        let mut forwarder = None;
        let mut acking = Acking(None);
        let ended = self
            .converse(&mut wire, writer, &inlet, &mut forwarder, &mut acking.0)
            .await;
        drop(acking);
        let remote = &*inlet.remote;
        let forwarder = forwarder.as_deref().unwrap_or("");
        match ended {
            End::Gone | End::Stopping => {}
            End::Malformed { at, within, what } => tracing::warn!(
                target: "worker.receiver_s2s_malformed",
                subject = %remote,
                forwarder,
                offset = at,
                within,
                what = %what,
                "a forwarder sent what this receiver does not read; its connection is closed and it sends again what \
                 was not acknowledged"
            ),
            End::Token(why) => tracing::warn!(
                target: "worker.receiver_s2s_token_refused",
                subject = %remote,
                forwarder,
                why,
                "a forwarder's connection is closed: {why}"
            ),
            End::Silent => tracing::debug!(
                target: "worker.receiver_s2s_silent",
                subject = %remote,
                forwarder,
                "a connection is closed: it did not finish its S2S handshake in time"
            ),
            End::OldProtocol => tracing::warn!(
                target: "worker.receiver_s2s_old_protocol",
                subject = %remote,
                forwarder,
                "a forwarder's connection is closed: it is set to send S2S v3 (enableOldS2SProtocol), and this \
                 receiver reads v4"
            ),
        }
    }

    /// The connection's whole conversation. `forwarder` is named once its signature is read, and `acking` holds the
    /// task writing acks once there is one.
    async fn converse<R: AsyncRead + Unpin, W: AsyncWrite + Unpin + Send + 'static>(
        &self,
        wire: &mut Wire<R>,
        mut writer: W,
        inlet: &Inlet,
        forwarder: &mut Option<String>,
        acking: &mut Option<tokio::task::JoinHandle<()>>,
    ) -> End {
        let handshake = self.handshake(wire, &mut writer, forwarder);
        let (capabilities, first) = match tokio::time::timeout(HANDSHAKE, handshake).await {
            Ok(Ok(done)) => done,
            Ok(Err(end)) => return end,
            Err(_) => return End::Silent,
        };
        let mut v4 = match s2s::format(first) {
            Format::V3 => return End::OldProtocol,
            Format::V4 => V4::new(self.max_request_bytes, None, wire.taken),
            Format::CompressedV4 => {
                wire.take(1);
                V4::new(self.max_request_bytes, Some(Inflate::default()), wire.taken)
            }
        };
        v4.push(&std::mem::take(&mut wire.held));
        let answers = capabilities.ack.then(|| {
            let (answers, answered) = mpsc::channel(ANSWERS);
            let unwritten = Arc::new(AtomicUsize::new(0));
            *acking = Some(tokio::spawn(write_acks(writer, answered, unwritten.clone())));
            Answering { answers, unwritten }
        });
        let forwarder = forwarder.as_deref().unwrap_or("");
        match self.stream(wire, v4, inlet, forwarder, answers).await {
            Ok(never) => match never {},
            Err(end) => end,
        }
    }

    /// The forwarder's signature, its capabilities answered, and its ForwarderInfo and token: what it asked for, and
    /// the first byte after.
    async fn handshake<R: AsyncRead + Unpin, W: AsyncWrite + Unpin>(
        &self,
        wire: &mut Wire<R>,
        writer: &mut W,
        forwarder: &mut Option<String>,
    ) -> Result<(s2s::Capabilities, u8), End> {
        let max = self.max_request_bytes.min(HANDSHAKE_BYTES);
        let signature = (wire.until(|b| Ok(s2s::signature(b)?.map(|s| (s, s2s::SIGNATURE_LEN))))).await?;
        *forwarder = Some(signature.server);
        // A forwarder that compresses over TCP sends its v3 messages compressed, even its capabilities before it
        // hears whether the receiver takes compression. Over TLS it sends them plain, and compresses only its v4.
        let v3 = |b: &[u8]| match compressed_frame(b) {
            None => Ok(None),
            Some(true) => s2s::read_compressed(b, max),
            Some(false) => s2s::read_message(b, max),
        };
        let at = wire.taken;
        let capabilities = wire.until(v3).await.and_then(|message| {
            s2s::capabilities(&message).map_err(|e| End::Malformed {
                at,
                within: "connection",
                what: e.what,
            })
        })?;
        let mut answer = Vec::new();
        s2s::write_capabilities_answer(&mut answer);
        if writer.write_all(&answer).await.is_err() || writer.flush().await.is_err() {
            return Err(End::Gone);
        }
        let info = wire.until(v3).await?;
        self.admits(&info).map_err(End::Token)?;
        let first = wire.until(|b| Ok(b.first().map(|&first| (first, 0)))).await?;
        Ok((capabilities, first))
    }

    /// Whether ForwarderInfo presents a token this receiver takes, when it takes any.
    fn admits(&self, info: &Message) -> Result<(), &'static str> {
        if self.tokens.is_empty() {
            return Ok(());
        }
        let Some(presented) = info.get(b"__s2s_token") else {
            return Err("it presented no token");
        };
        // Every token is compared, so the time taken says nothing about which one nearly matched.
        let found = (self.tokens.iter()).fold(false, |found, token| {
            found | verify_slices_are_equal(presented.trim_ascii(), token).is_ok()
        });
        match found {
            true => Ok(()),
            false => Err("its token is not one this receiver takes"),
        }
    }

    /// Reads the v4 stream until the connection ends: each event's records admitted before the next is read.
    async fn stream<R: AsyncRead + Unpin>(
        &self,
        wire: &mut Wire<R>,
        mut v4: V4,
        inlet: &Inlet,
        forwarder: &str,
        answers: Option<Answering>,
    ) -> Result<std::convert::Infallible, End> {
        let mut channels = Channels::new(&self.sourcetypes, self.max_request_bytes);
        let mut acks = Acks::default();
        let mut held = inlet.held();
        // The events, until the stream ends. A block, so that a `?` in it ends the events rather than the connection,
        // and the channels are flushed below however they end.
        let events: Result<std::convert::Infallible, End> = async {
            // Borrowed, not moved: the flush below reads the channels, the acks and what is held once the events end.
            let (v4, channels, acks, held) = (&mut v4, &mut channels, &mut acks, &mut held);
            let mut read = vec![0; READ_STEP];
            let mut last_beat: Option<Instant> = None;
            loop {
                while let Some(event) = v4.decoder.next_event().map_err(|e| v4.malformed(e.at, e.what))? {
                    if forwarder_info(&event) {
                        // A forwarder sends heartbeats close together while its ack window is full. With every answer
                        // written, what holds the window is a line no ack covers until it ends: acking it now lets the
                        // forwarder send the rest, so it arrives whole rather than cut at the window.
                        let waiting = heartbeat(&event) && {
                            let now = Instant::now();
                            last_beat
                                .replace(now)
                                .is_some_and(|last| WAITING_BEATS.contains(&(now - last)))
                        };
                        if waiting
                            && let Some(answering) = answers.as_ref().filter(|a| a.all_written())
                            && let Some(range) = acks.early(acks.open(channels))
                        {
                            let (fired, answer) = oneshot::channel();
                            let _ = fired.send(());
                            answering.send((answer, Some(range))).await;
                        }
                        continue;
                    }
                    let id = event.id;
                    if id == Some(u64::MAX) {
                        return Err(v4.malformed(v4.decoder.offset(), "an event id no later id can follow".into()));
                    }
                    let filed = filed(&event).map_err(|what| v4.malformed(v4.decoder.offset(), what))?;
                    let records = channels.take(event, filed, Instant::now());
                    let range = acks.after(id, acks.open(channels));
                    admit(inlet, held, records, range, answers.as_ref()).await?;
                }
                // Room for what it holds, each buffer as much again as it may grow to, and the step it takes next.
                let buffers = v4.memory() + channels.memory();
                held.hold(read.len() + 2 * buffers + STREAM_STEP).await?;
                if v4.inflate_step()? {
                    // A read can inflate a thousandfold; between steps the other connections on this thread are served.
                    tokio::task::yield_now().await;
                    continue;
                }
                let quiet = answers.as_ref().and(channels.next_quiet());
                let n = tokio::select! {
                    n = wire.reader.read(&mut read) => n,
                    () = sleep_until(quiet) => {
                        let records = channels.close_quiet(Instant::now());
                        if !records.is_empty() {
                            tracing::warn!(
                                target: "worker.receiver_s2s_line_cut",
                                subject = %inlet.remote,
                                forwarder,
                                "a forwarder sent no more of a line for 30 s, and it is cut where it stopped: its file \
                                 stopped mid-line"
                            );
                        }
                        let range = acks.after(None, acks.open(channels));
                        admit(inlet, held, records, range, answers.as_ref()).await?;
                        continue;
                    }
                };
                match n {
                    Ok(0) | Err(_) => return Err(End::Gone),
                    Ok(n) => {
                        wire.taken += n as u64;
                        v4.push(&read[..n]);
                    }
                }
            }
        }
        .await;
        let Err(end) = events;
        // However the connection ended, a forwarder sends again only what it waits on acks for: without them, or past
        // them, what the channels hold is all of it there will be.
        let ended = match (&answers, acks.next) {
            (None, _) => channels.end(),
            (Some(_), Some(next)) => channels.end_before(next),
            (Some(_), None) => Cuts::default(),
        };
        admit(inlet, &mut held, ended, None, None).await?;
        Err(end)
    }
}

async fn sleep_until(at: Option<Instant>) {
    match at {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}

/// Offers one forwarder event's records, and hands the writer its answer when there is one, to ack `range` once it
/// fires. Neither records nor ids to ack is nothing to do.
async fn admit(
    inlet: &Inlet,
    held: &mut Held,
    records: Cuts,
    range: Option<(u64, u64)>,
    answers: Option<&Answering>,
) -> Result<(), End> {
    let range = range.filter(|_| answers.is_some());
    if records.is_empty() && range.is_none() {
        return Ok(());
    }
    let bytes = held.part(records.size());
    let answer = inlet.admit(Cutter::Each(records), bytes).await?;
    if let Some(answers) = answers {
        answers.send((answer, range)).await;
    }
    Ok(())
}

/// Why an `httpout` request is refused.
pub(super) enum HttpoutRefused<E> {
    /// It inflates past what a request may hold.
    TooLarge,
    Malformed(String),
    /// What it holds could not be held.
    Held(E),
}

/// How much of an `httpout` stream is inflated at a time, held before it is inflated.
const HTTPOUT_STEP: usize = 1024 * 1024;

/// The end of a zlib sync flush, which ends every `httpout` body.
const SYNC_FLUSH: [u8; 4] = [0, 0, 0xff, 0xff];

/// The records of one `httpout` request. A body is [`s2s::COMPRESSED`] and a zlib stream of v4 that stands alone: it
/// defines every channel it uses, ends on a sync flush, and acks nothing, its answer acking it. Each channel's stream
/// ends with the body, since the forwarder may send the next request to another worker.
///
/// It inflates and decodes a step at a time, and before each step calls `hold` with what the request will then hold:
/// its body twice, as sent and as the inflater's input, its records, what its channels hold, and the step, inflated
/// and pushed to the decoder.
/// So `hold` can refuse it before it is allocated.
pub(super) fn httpout<E>(
    body: &[u8],
    sourcetypes: &BTreeMap<String, LineBreaker>,
    max: usize,
    mut hold: impl FnMut(usize) -> Result<(), E>,
) -> Result<Cuts, HttpoutRefused<E>> {
    let malformed = |what: String| HttpoutRefused::Malformed(what);
    let Some((&s2s::COMPRESSED, zlib)) = body.split_first() else {
        return Err(malformed(format!(
            "an httpout body starts with {:#x}, the start of a compressed stream",
            s2s::COMPRESSED
        )));
    };
    if !zlib.ends_with(&SYNC_FLUSH) {
        return Err(malformed(
            "an httpout body ends on a zlib sync flush, and this one is cut short".to_string(),
        ));
    }
    let mut inflate = Inflate::default();
    inflate.push(zlib);
    let mut decoder = Decoder::new(max);
    let mut channels = Channels::new(sourcetypes, max);
    let mut records = Cuts::default();
    let mut step = Vec::with_capacity(HTTPOUT_STEP);
    let mut inflated = 0;
    let now = Instant::now();
    loop {
        hold(2 * body.len() + records.size() + channels.held() + decoder.held() + 2 * HTTPOUT_STEP)
            .map_err(HttpoutRefused::Held)?;
        step.clear();
        let added = inflate
            .inflate(&mut step, HTTPOUT_STEP)
            .map_err(|e| malformed(format!("at {} of the compressed stream: {}", e.at, e.what)))?;
        if added == 0 {
            break;
        }
        inflated += added;
        if inflated > max {
            return Err(HttpoutRefused::TooLarge);
        }
        decoder.push(&step);
        while let Some(event) = decoder
            .next_event()
            .map_err(|e| malformed(format!("at {} of the inflated stream: {}", e.at, e.what)))?
        {
            if forwarder_info(&event) {
                continue;
            }
            let filed = filed(&event).map_err(malformed)?;
            records.append(channels.take(event, filed, now));
        }
    }
    if decoder.held() > 0 {
        return Err(malformed(format!(
            "the inflated stream ends {} bytes into a frame",
            decoder.held()
        )));
    }
    records.append(channels.end());
    hold(body.len() + records.size()).map_err(HttpoutRefused::Held)?;
    Ok(records)
}

/// The task writing a connection's acks, ended with the connection, however the connection ends: a receiver that
/// stops drops its connections mid-conversation.
struct Acking(Option<tokio::task::JoinHandle<()>>);

impl Drop for Acking {
    fn drop(&mut self) {
        if let Some(task) = &self.0 {
            task.abort();
        }
    }
}

/// Writes each answer's acks once it fires, in the order the requests were admitted, which is the order their answers
/// fire in. Ends when the connection or the receiver does.
async fn write_acks<W: AsyncWrite + Unpin>(
    mut writer: W,
    mut answers: mpsc::Receiver<Answer>,
    unwritten: Arc<AtomicUsize>,
) {
    let mut frame = Vec::new();
    while let Some((answer, range)) = answers.recv().await {
        // Dropped unanswered: the receiver stopped, and its records were never delivered.
        if answer.await.is_err() {
            return;
        }
        if let Some((first, last)) = range {
            frame.clear();
            s2s::ack(first, last, &mut frame);
            if writer.write_all(&frame).await.is_err() || writer.flush().await.is_err() {
                return;
            }
        }
        unwritten.fetch_sub(1, Ordering::SeqCst);
    }
}

/// The handshake's side of the socket: the bytes read and not yet taken.
struct Wire<R> {
    reader: R,
    held: Vec<u8>,
    /// The connection's offset of `held[0]`.
    taken: u64,
}

impl<R: AsyncRead + Unpin> Wire<R> {
    /// What `parse` reads from the bytes held, reading more until it has all it needs. `parse` answers what it read
    /// and how many bytes it took.
    async fn until<T>(
        &mut self,
        mut parse: impl FnMut(&[u8]) -> Result<Option<(T, usize)>, s2s::Malformed>,
    ) -> Result<T, End> {
        loop {
            match parse(&self.held) {
                Ok(Some((read, took))) => {
                    self.take(took);
                    return Ok(read);
                }
                Ok(None) => {}
                Err(e) => return Err(self.malformed(e)),
            }
            let mut read = vec![0; READ_STEP];
            match self.reader.read(&mut read).await {
                Ok(0) | Err(_) => return Err(End::Gone),
                Ok(n) => self.held.extend_from_slice(&read[..n]),
            }
        }
    }

    fn take(&mut self, n: usize) {
        self.held.drain(..n);
        self.taken += n as u64;
    }

    /// A handshake message refused, placed where it starts in the connection.
    fn malformed(&self, e: s2s::Malformed) -> End {
        End::Malformed {
            at: self.taken + e.at,
            within: "connection",
            what: e.what,
        }
    }
}

/// Whether the bytes held open a compressed v3 frame, once its first two integers are held: a compressed frame's
/// second is its first less four, which no v3 message's count can be, its pairs being longer than their count.
fn compressed_frame(held: &[u8]) -> Option<bool> {
    let int = |at: usize| u32::from_be_bytes(held[at..at + 4].try_into().expect("four bytes"));
    (held.len() >= 8).then(|| int(0).checked_sub(4) == Some(int(4)))
}

/// The v4 stream, as sent or inflated.
struct V4 {
    decoder: Decoder,
    inflate: Option<Inflate>,
    inflated: Vec<u8>,
    /// The connection's offset of the stream's first byte, or of the zlib stream's.
    start: u64,
}

impl V4 {
    fn new(max_frame: usize, inflate: Option<Inflate>, start: u64) -> V4 {
        V4 {
            decoder: Decoder::new(max_frame),
            inflate,
            inflated: Vec::new(),
            start,
        }
    }

    /// The memory it holds between reads: frames not yet whole, and what is not yet inflated.
    fn memory(&self) -> usize {
        self.decoder.memory() + self.inflated.capacity() + self.inflate.as_ref().map_or(0, Inflate::memory)
    }

    fn push(&mut self, bytes: &[u8]) {
        match &mut self.inflate {
            Some(inflate) => inflate.push(bytes),
            None => self.decoder.push(bytes),
        }
    }

    /// Inflates the next step of a compressed stream into the decoder. False once what was pushed is all inflated,
    /// and for a stream sent as it is.
    fn inflate_step(&mut self) -> Result<bool, End> {
        let Some(inflate) = &mut self.inflate else {
            return Ok(false);
        };
        self.inflated.clear();
        let n = inflate
            .inflate(&mut self.inflated, INFLATE_STEP)
            .map_err(|e| End::Malformed {
                at: self.start + e.at,
                within: "connection",
                what: e.what,
            })?;
        self.decoder.push(&self.inflated);
        Ok(n > 0)
    }

    /// Bytes refused at `at` of the decoded stream: the connection's offset when it is sent as it is, else the
    /// inflated stream's.
    fn malformed(&self, at: u64, what: String) -> End {
        match self.inflate {
            None => End::Malformed {
                at: self.start + at,
                within: "connection",
                what,
            },
            Some(_) => End::Malformed {
                at,
                within: "inflated stream",
                what,
            },
        }
    }
}

/// The forwarder's own `ForwarderInfo` event, which it sends again in v4 from time to time, and its heartbeat, `HB`,
/// which it sends while it waits for acks: each on a channel naming nothing and with id 0, which an indexer does not
/// ack.
fn forwarder_info(event: &Event) -> bool {
    event.fields.iter().any(|field| {
        matches!((&field.key, &field.value), (Key::Text(key), Value::Text(value))
            if &**key == b"MetaData:Sourcetype" && (value == b"sourcetype::fwdinfo" || value == b"sourcetype::fwd-hb"))
    })
}

/// The forwarder's heartbeat: every 200 ms while it waits for acks, every 30 s by default otherwise.
fn heartbeat(event: &Event) -> bool {
    event.fields.iter().any(|field| {
        matches!((&field.key, &field.value), (Key::Text(key), Value::Text(value))
            if &**key == b"MetaData:Sourcetype" && value == b"sourcetype::fwd-hb")
    })
}

/// What `event` is filed under: its channel's host, source and sourcetype, the index the forwarder names, its time,
/// and its indexed fields. A field this receiver cannot name or read refuses it.
fn filed(event: &Event) -> Result<Filed, String> {
    let mut filed = Filed::new();
    let channel = &event.channel;
    let named = [
        (&channel.host, &b"host::"[..]),
        (&channel.source, b"source::"),
        (&channel.sourcetype, b"sourcetype::"),
    ];
    for (key, (value, prefix)) in hec::NAMED.into_iter().zip(named) {
        if let Some(value) = value.as_deref().map(|v| v.strip_prefix(prefix).unwrap_or(v))
            && !value.is_empty()
        {
            filed.push((key.to_string(), value.to_vec()));
        }
    }
    let mut index = None;
    let mut subsecond = 0;
    // By name, as a JSON object holds its members.
    let mut fields: BTreeMap<String, Vec<Scalar>> = BTreeMap::new();
    for field in &event.fields {
        let key = match &field.key {
            Key::Dictionary(SUBSECOND_KEY) => {
                subsecond = match field.value {
                    Value::Int(value) => subsecond_micros(value),
                    _ => None,
                }
                .ok_or_else(|| format!("a subsecond of {:?}, which is no fraction", field.value))?;
                continue;
            }
            Key::Dictionary(key) if TIMESTAMP_KEYS.contains(key) => continue,
            // `_event_status` among them: resent or resent_acked on what the forwarder sends again after a
            // connection was lost, which a pipeline may want to know.
            Key::Dictionary(key) => match s2s::dictionary(*key) {
                Some(name) => name.to_string(),
                None => return Err(format!("a field keyed by dictionary entry {key}, which is not known")),
            },
            Key::Text(key) if &**key == INDEX_KEY => {
                index = Some(scalar(event, &field.value)?);
                continue;
            }
            Key::Text(key) if BOOKKEEPING.contains(&&**key) => continue,
            Key::Text(key) => String::from_utf8_lossy(key).into_owned(),
        };
        // A JSON array arrives as a field per element, under one key.
        fields.entry(key).or_default().push(scalar(event, &field.value)?);
    }
    match index {
        Some(Scalar::Text(index)) if index.as_bytes() != DEFAULT_INDEX => {
            filed.push(("index".to_string(), index.into_bytes()));
        }
        Some(Scalar::Text(_)) | None => {}
        Some(other) => return Err(format!("an index of {}, which is not a name", other.json())),
    }
    let micros = event.time.and_then(|seconds| {
        i64::try_from(seconds)
            .ok()?
            .checked_mul(1_000_000)?
            .checked_add(subsecond)
    });
    if let Some(micros) = micros {
        filed.push((hec::TIME.to_string(), micros.to_string().into_bytes()));
    }
    if !fields.is_empty() {
        let mut encoded = Vec::new();
        let open = variant::begin_object(&mut encoded);
        for (key, values) in &fields {
            variant::put_key(&mut encoded, key);
            match values.as_slice() {
                [one] => one.put(&mut encoded),
                many => {
                    let items = variant::begin_array(&mut encoded);
                    for value in many {
                        value.put(&mut encoded);
                    }
                    variant::end(&mut encoded, items, many.len());
                }
            }
        }
        variant::end(&mut encoded, open, fields.len());
        filed.push((hec::FIELDS.to_string(), encoded));
    }
    Ok(filed)
}

/// The microseconds a `_subsecond` value adds to its event's second. The value holds the fraction's digits as
/// written, at most six: as an integer, then a one, then a zero for each leading zero. `.5` arrives as 11, `.07` as
/// 30 and `.000123` as 1976. `None` for a value that holds no such fraction.
fn subsecond_micros(value: u64) -> Option<i64> {
    let zeros = value.trailing_zeros();
    let digits = value.checked_shr(zeros + 1)?;
    let width = zeros + digits.checked_ilog10().map_or(0, |n| n + 1);
    let scale = 10_i64.checked_pow(6_u32.checked_sub(width)?)?;
    i64::try_from(digits).ok()?.checked_mul(scale)
}

/// A field's value, as JSON holds it.
enum Scalar {
    Null,
    Bool(bool),
    Int(u64),
    Text(String),
}

impl Scalar {
    fn put(&self, out: &mut Vec<u8>) {
        match self {
            Scalar::Null => variant::put_null(out),
            Scalar::Bool(b) => variant::put_bool(out, *b),
            // One past an i64 is held as the nearest double, as a JSON number is.
            Scalar::Int(n) => match i64::try_from(*n) {
                Ok(n) => variant::put_i64(out, n),
                Err(_) => variant::put_f64(out, *n as f64),
            },
            Scalar::Text(text) => variant::put_str(out, text),
        }
    }

    /// The value as JSON's text, for a refusal to name it.
    fn json(&self) -> String {
        match self {
            Scalar::Null => "null".to_string(),
            Scalar::Bool(b) => b.to_string(),
            Scalar::Int(n) => n.to_string(),
            Scalar::Text(text) => {
                let mut out = Vec::new();
                crate::format::json::write_json_string(text.as_bytes(), &mut out);
                String::from_utf8_lossy(&out).into_owned()
            }
        }
    }
}

/// A field's value: an integer as a number, `null`, `false` and `true` as themselves, anything else as text, with
/// the escapes in a raw slice resolved.
fn scalar(event: &Event, value: &Value) -> Result<Scalar, String> {
    let text = |bytes: &[u8]| Scalar::Text(String::from_utf8_lossy(bytes).into_owned());
    Ok(match *value {
        Value::Int(n) => Scalar::Int(n),
        Value::Text(ref bytes) => text(bytes),
        Value::Dictionary(60) => Scalar::Null,
        Value::Dictionary(61) => Scalar::Bool(false),
        Value::Dictionary(62) => Scalar::Bool(true),
        Value::Dictionary(entry) => match s2s::dictionary(entry) {
            Some(word) => Scalar::Text(word.to_string()),
            None => {
                return Err(format!(
                    "a field valued by dictionary entry {entry}, which is not known"
                ));
            }
        },
        Value::Raw {
            start,
            len,
            escapes: Escapes::None,
        } => text(event.slice(start, len)),
        Value::Raw {
            start,
            len,
            escapes: Escapes::Csv,
        } => Scalar::Text(String::from_utf8_lossy(event.slice(start, len)).replace("\"\"", "\"")),
        Value::Raw {
            start,
            len,
            escapes: Escapes::Json,
        } => {
            let raw = event.slice(start, len);
            json_unescaped(raw).map_or_else(|| text(raw), Scalar::Text)
        }
    })
}

/// `raw`, the inside of a JSON string, with its escapes resolved: `None` for anything JSON does not allow in a string,
/// a raw control byte, a bad escape or a lone surrogate among them, so a value that is not JSON is kept as written.
fn json_unescaped(raw: &[u8]) -> Option<String> {
    let hex = |at: usize| -> Option<u32> {
        let digits = raw.get(at..at + 4)?;
        digits
            .iter()
            .all(u8::is_ascii_hexdigit)
            .then(|| (digits.iter()).fold(0, |n, d| n * 16 + char::from(*d).to_digit(16).expect("a hex digit")))
    };
    let mut out = Vec::with_capacity(raw.len());
    let mut i = 0;
    while i < raw.len() {
        match raw[i] {
            b'\\' => {
                let escaped = match *raw.get(i + 1)? {
                    b'"' => b'"',
                    b'\\' => b'\\',
                    b'/' => b'/',
                    b'b' => 0x08,
                    b'f' => 0x0C,
                    b'n' => b'\n',
                    b'r' => b'\r',
                    b't' => b'\t',
                    b'u' => {
                        let first = hex(i + 2)?;
                        i += 6;
                        let c = match first {
                            0xD800..=0xDBFF => {
                                let second = hex(i + 2).filter(|_| raw.get(i..i + 2) == Some(b"\\u"))?;
                                if !(0xDC00..=0xDFFF).contains(&second) {
                                    return None;
                                }
                                i += 6;
                                char::from_u32(0x10000 + ((first - 0xD800) << 10) + (second - 0xDC00))?
                            }
                            // A low surrogate alone is no character.
                            unit => char::from_u32(unit)?,
                        };
                        out.extend_from_slice(c.encode_utf8(&mut [0; 4]).as_bytes());
                        continue;
                    }
                    _ => return None,
                };
                out.push(escaped);
                i += 2;
            }
            b'"' | 0x00..=0x1F => return None,
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8(out).ok()
}

/// Which ids a connection has acked, and which it may ack next.
#[derive(Default)]
struct Acks {
    /// The first id no ack covers yet: the first the connection sent, until one is acked.
    next: Option<u64>,
    /// The highest id the connection sent.
    seen: Option<u64>,
}

impl Acks {
    /// The oldest id not yet acked whose bytes a channel holds open. One holding bytes of an id acked already, as a
    /// line acked early, holds back no ack.
    fn open(&self, channels: &Channels) -> Option<u64> {
        channels.holding_from(self.next.unwrap_or(0))
    }

    /// The ids to ack once the request for forwarder event `id` is delivered, `None` for a request of channels gone
    /// quiet: every id not acked up to the one before `oldest_open`, the oldest not acked a channel holds bytes of
    /// ([`Acks::open`]), or up to the last sent when none holds any. The ids up to it are delivered once that request is: every request before it was
    /// admitted, and so answered, before it.
    fn after(&mut self, id: Option<u64>, oldest_open: Option<u64>) -> Option<(u64, u64)> {
        if let Some(id) = id {
            self.seen = Some(self.seen.map_or(id, |seen| seen.max(id)));
            self.next.get_or_insert(id);
        }
        let seen = self.seen?;
        let first = self.next?;
        let through = match oldest_open {
            Some(open) => open.checked_sub(1)?.min(seen),
            None => seen,
        };
        if through < first {
            return None;
        }
        self.next = Some(through + 1);
        Some((first, through))
    }

    /// Every id not acked, through the last sent, when a channel holds bytes of one, `oldest_open`, that keeps them
    /// from being acked: the ids of a line acked before it ends. Its channel holds it until it does, and holds back
    /// no ack after.
    fn early(&mut self, oldest_open: Option<u64>) -> Option<(u64, u64)> {
        oldest_open?;
        let first = self.next?;
        let seen = self.seen.filter(|seen| *seen >= first)?;
        self.next = Some(seen + 1);
        Some((first, seen))
    }
}

/// The chunk an event's bytes are held from: the first id they hold back, and what records cut from it are filed
/// under. Bytes the line breaker found no break in are held from the first chunk they came in, and it searches them
/// again only once they double, so an event it breaks off them is filed under that chunk, which may be earlier than
/// the one its first byte came in.
#[derive(Clone)]
struct Origin {
    /// The chunk's own id. A chunk without one holds back the id after the last the connection sent: the bytes of
    /// any later chunk can join what it left open.
    id: u64,
    filed: Arc<Filed>,
}

/// A connection's channels, each one stream of bytes broken into events. A channel is held only while it holds
/// bytes no record has taken.
struct Channels<'a> {
    sourcetypes: &'a BTreeMap<String, LineBreaker>,
    /// The most bytes a record may hold, and the channels between them.
    max: usize,
    held: HashMap<u64, ChannelBytes>,
    /// The id after the last the connection sent.
    next_id: u64,
}

/// What one channel holds of its stream.
struct ChannelBytes {
    /// The definition its bytes came under: one freed and defined again under the same id is another stream.
    channel: Arc<s2s::Channel>,
    /// Where the sourcetype breaks its events, or `None` for an event per line.
    line_breaker: Option<LineBreaker>,
    /// The bytes after its last whole event, and where the first of them came.
    tail: Vec<u8>,
    tail_from: Option<Origin>,
    /// How far the line breaker searched `tail` in vain.
    searched: Searched,
    /// Whether its last chunk ended on `\r`, which a `\n` starting the next ends a line with. Only without a line
    /// breaker.
    after_cr: bool,
    /// When its last chunk arrived.
    heard: Instant,
}

impl<'a> Channels<'a> {
    fn new(sourcetypes: &'a BTreeMap<String, LineBreaker>, max: usize) -> Channels<'a> {
        Channels {
            sourcetypes,
            max,
            held: HashMap::new(),
            next_id: 0,
        }
    }

    /// Takes one forwarder event on its channel, and returns the records it closed.
    fn take(&mut self, event: Event, filed: Filed, now: Instant) -> Cuts {
        let mut records = Cuts::default();
        let id = event.channel.id;
        let held = match self.held.entry(id) {
            Entry::Occupied(held) if Arc::ptr_eq(&held.get().channel, &event.channel) => held.into_mut(),
            // Freed and defined again: what the old stream holds is all of it there will be.
            Entry::Occupied(mut held) => {
                held.get_mut().end(self.max, &mut records);
                *held.get_mut() = ChannelBytes::new(&event.channel, self.sourcetypes, now);
                held.into_mut()
            }
            Entry::Vacant(new) => new.insert(ChannelBytes::new(&event.channel, self.sourcetypes, now)),
        };
        held.heard = now;
        let from = Origin {
            id: event.id.unwrap_or(self.next_id),
            filed: Arc::new(filed),
        };
        if let Some(id) = event.id {
            self.next_id = self.next_id.max(id.saturating_add(1));
        }
        match event.broken {
            true if event.raw.trim_ascii().is_empty() => {}
            true => records.push(&event.raw, &from.filed),
            false => held.chunk(&event.raw, &from, self.max, &mut records),
        }
        if event.done {
            held.end(self.max, &mut records);
        }
        if held.is_empty() {
            self.held.remove(&id);
        }
        self.bound(&mut records);
        records
    }

    /// Holds the connection's channels to `max` bytes between them, and a line breaker's slack: past it, the channel
    /// holding most ends its stream, what it holds its last event, until they fit. Its next chunk starts another.
    fn bound(&mut self, records: &mut Cuts) {
        let mut held = self.held();
        while held > self.max + BREAK_SLACK {
            let (&id, _) = self
                .held
                .iter()
                .max_by_key(|(_, held)| held.held())
                .expect("bytes are held");
            let mut most = self.held.remove(&id).expect("it is held");
            held -= most.held();
            most.end(self.max, records);
        }
    }

    /// The memory its channels hold for events not yet whole.
    fn memory(&self) -> usize {
        self.held.values().map(|held| held.tail.capacity()).sum()
    }

    /// The bytes the channels hold between them.
    fn held(&self) -> usize {
        self.held.values().map(ChannelBytes::held).sum()
    }

    /// The oldest id from `from` on whose bytes a channel holds open.
    fn holding_from(&self, from: u64) -> Option<u64> {
        self.held
            .values()
            .filter_map(ChannelBytes::oldest)
            .filter(|id| *id >= from)
            .min()
    }

    /// When the first channel holding bytes goes quiet.
    fn next_quiet(&self) -> Option<Instant> {
        self.held.values().map(|held| held.heard + QUIET).min()
    }

    /// Ends every channel's stream, what each holds its last event.
    fn end(&mut self) -> Cuts {
        let mut records = Cuts::default();
        for (_, mut held) in self.held.drain() {
            held.end(self.max, &mut records);
        }
        records
    }

    /// Ends the stream of each channel holding bytes of an id before `from`, acked already, what it holds its last
    /// event.
    fn end_before(&mut self, from: u64) -> Cuts {
        let mut records = Cuts::default();
        let max = self.max;
        self.held.retain(|_, held| {
            let acked = held.oldest().is_some_and(|id| id < from);
            if acked {
                held.end(max, &mut records);
            }
            !acked
        });
        records
    }

    /// Ends the stream of each channel quiet by `now`, what it holds its last event.
    fn close_quiet(&mut self, now: Instant) -> Cuts {
        let mut records = Cuts::default();
        let max = self.max;
        self.held.retain(|_, held| {
            let quiet = held.heard + QUIET <= now;
            if quiet {
                held.end(max, &mut records);
            }
            !quiet
        });
        records
    }
}

impl ChannelBytes {
    fn new(channel: &Arc<s2s::Channel>, sourcetypes: &BTreeMap<String, LineBreaker>, now: Instant) -> ChannelBytes {
        let sourcetype = channel.sourcetype.as_deref().unwrap_or_default();
        let sourcetype = sourcetype.strip_prefix(b"sourcetype::").unwrap_or(sourcetype);
        let line_breaker = std::str::from_utf8(sourcetype).ok().and_then(|s| sourcetypes.get(s));
        ChannelBytes {
            channel: channel.clone(),
            line_breaker: line_breaker.cloned(),
            tail: Vec::new(),
            tail_from: None,
            searched: Searched::default(),
            after_cr: false,
            heard: now,
        }
    }

    /// Takes a chunk: each whole event it ends, and its partial last event held for the next chunk. An event ends where
    /// the sourcetype's line breaker breaks it, or without one at `\n`, `\r\n` or a lone `\r`, as an indexer's
    /// default LINE_BREAKER ends it.
    fn chunk(&mut self, mut bytes: &[u8], from: &Origin, max: usize, records: &mut Cuts) {
        let limit = if self.line_breaker.is_some() {
            let before = self.tail.len();
            self.hold(bytes, from);
            if self.searched.due(self.tail.len()) {
                self.break_tail(before, from, false, max, records);
            }
            max + BREAK_SLACK
        } else {
            // A `\r` the last chunk ended on and the `\n` this one starts with are one line end.
            if self.after_cr && !bytes.is_empty() {
                self.after_cr = false;
                bytes = bytes.strip_prefix(b"\n").unwrap_or(bytes);
            }
            while let Some((end, next)) = line_breaker::next_line(bytes) {
                self.after_cr = next == end + 1 && bytes[end] == b'\r' && next == bytes.len();
                match self.tail_from.take() {
                    None => cut(&bytes[..end], from, max, records),
                    Some(tail_from) => {
                        let mut line = std::mem::take(&mut self.tail);
                        line.extend_from_slice(&bytes[..end]);
                        cut(&line, &tail_from, max, records);
                    }
                }
                bytes = &bytes[next..];
            }
            self.hold(bytes, from);
            max
        };
        // An event longer than a record may be is cut where it reaches the limit; none of it is dropped. A line breaker
        // holds the slack past it first, so a break the limit falls in is taken whole.
        while self.tail.len() > limit {
            let piece: Vec<u8> = self.tail.drain(..max).collect();
            let tail_from = self.tail_from.as_ref().expect("held bytes came from somewhere");
            records.push(&piece, &tail_from.filed);
            self.searched.found();
        }
    }

    /// Adds `bytes`, which came at `from`, to the partial last event.
    fn hold(&mut self, bytes: &[u8], from: &Origin) {
        if bytes.is_empty() {
            return;
        }
        if self.tail.is_empty() {
            self.tail_from = Some(from.clone());
        }
        self.tail.extend_from_slice(bytes);
    }

    /// Takes each whole event the line breaker breaks off the held bytes, of which the first `before` came before
    /// `from`. The bytes after the last break stay held.
    fn break_tail(&mut self, before: usize, from: &Origin, ended: bool, max: usize, records: &mut Cuts) {
        let Some(line_breaker) = &self.line_breaker else {
            return;
        };
        let came = |at: usize| match at < before {
            true => self.tail_from.as_ref().expect("held bytes came from somewhere"),
            false => from,
        };
        let mut events = line_breaker.events(&self.tail, ended);
        for event in events.by_ref() {
            cut(&self.tail[event.clone()], came(event.start), max, records);
        }
        let start = events.rest();
        let rest_from = (start < self.tail.len()).then(|| came(start).clone());
        self.tail.drain(..start);
        self.tail_from = rest_from;
        self.searched.missed(self.tail.len());
    }

    /// Ends the stream, at `_done`, once it went quiet or past the bound: the bytes it holds are its last events.
    fn end(&mut self, max: usize, records: &mut Cuts) {
        let Some(from) = self.tail_from.clone() else {
            return;
        };
        match self.line_breaker {
            Some(_) => self.break_tail(self.tail.len(), &from, true, max, records),
            None => {
                self.tail_from = None;
                let rest = std::mem::take(&mut self.tail);
                cut(LineBreaker::last_event(&rest), &from, max, records);
            }
        }
    }

    /// The oldest id this channel holds bytes of.
    fn oldest(&self) -> Option<u64> {
        self.tail_from.as_ref().map(|from| from.id)
    }

    fn is_empty(&self) -> bool {
        self.tail.is_empty()
    }

    /// The bytes it holds: its partial last event.
    fn held(&self) -> usize {
        self.tail.len()
    }
}

/// Takes a whole event, which came at `from`, cut into records no longer than `max`. An event of nothing but
/// whitespace is no record.
fn cut(event: &[u8], from: &Origin, max: usize, records: &mut Cuts) {
    if event.trim_ascii().is_empty() {
        return;
    }
    for piece in event.chunks(max) {
        records.push(piece, &from.filed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A JSON-escaped value resolves as serde_json resolves a string, and is `None` wherever serde_json refuses one.
    #[test]
    fn a_json_escaped_value_unescapes_as_serde_json_reads_it() {
        let parts: [&[u8]; 16] = [
            b"a",
            b"\\\\",
            b"\\\"",
            b"\\/",
            b"\\n",
            b"\\t",
            b"\\u00e9",
            b"\\uD83D\\uDE00",
            b"\\ud800",
            b"\\udc00",
            b"\\U",
            b"\\x41",
            b"\t",
            b"\"",
            "\u{e9}".as_bytes(),
            b"\xff",
        ];
        let mut x: u64 = 0x9e37_79b9_7f4a_7c15;
        for _ in 0..50_000 {
            let mut raw = Vec::new();
            for _ in 0..x % 5 {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                raw.extend_from_slice(parts[(x % parts.len() as u64) as usize]);
            }
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let quoted = [&b"\""[..], &raw, b"\""].concat();
            let serde = serde_json::from_slice::<String>(&quoted).ok();
            assert_eq!(json_unescaped(&raw), serde, "{}", String::from_utf8_lossy(&raw));
        }
        assert_eq!(
            json_unescaped(br"C:\Users\temp"),
            None,
            "a bad escape keeps the value as written"
        );
    }

    /// A v4 stream, encoded as a forwarder encodes it.
    #[derive(Default)]
    struct V4Out(Vec<u8>);

    impl V4Out {
        fn varint(&mut self, mut n: u64) {
            while n >= 0x80 {
                self.0.push(n as u8 | 0x80);
                n >>= 7;
            }
            self.0.push(n as u8);
        }

        fn string(&mut self, s: &[u8]) {
            self.varint(s.len() as u64);
            self.0.extend_from_slice(s);
        }

        fn channel(&mut self, id: u64, sourcetype: &str) {
            self.0.push(0xfe);
            self.varint(id);
            let sourcetype = format!("sourcetype::{sourcetype}");
            for s in ["source::/var/log/app.log", "host::web-1", &sourcetype, "2"] {
                self.varint(s.len() as u64 + 1);
                self.0.extend_from_slice(s.as_bytes());
            }
            self.varint(0);
        }

        fn free(&mut self, id: u64) {
            self.0.push(0xfd);
            self.varint(id);
        }

        /// An event: a chunk, or one the forwarder `broken` itself, `done` when it ends its stream.
        fn event(&mut self, channel: u64, id: u64, raw: &[u8], broken: bool, done: bool) {
            self.0.push(0xfc);
            self.varint(channel);
            self.varint(1 << 2 | 1 << 3 | (broken as u64) << 6 | (done as u64) << 9);
            self.varint(id);
            self.varint(1_700_000_000);
            self.string(raw);
        }
    }

    /// Sourcetype `java` broken by `line_breaker`.
    fn rule(line_breaker: &str) -> BTreeMap<String, LineBreaker> {
        let line_breaker = LineBreaker(regex::bytes::Regex::new(line_breaker).unwrap());
        BTreeMap::from([("java".to_string(), line_breaker)])
    }

    /// Breaks an event before each line that starts `ERROR`.
    const AT_ERROR: &str = r"([\r\n]+)ERROR";

    /// The records each event of `stream` closes, as text, and the oldest id held open after it.
    fn records_of(
        rules: &BTreeMap<String, LineBreaker>,
        max: usize,
        stream: &V4Out,
    ) -> Vec<(Vec<String>, Option<u64>)> {
        let mut decoder = Decoder::new(1 << 20);
        decoder.push(&stream.0);
        let mut channels = Channels::new(rules, max);
        let mut out = Vec::new();
        while let Some(event) = decoder.next_event().unwrap() {
            let filed = filed(&event).unwrap();
            let records = channels.take(event, filed, Instant::now());
            let text = records.map(|(r, _)| String::from_utf8(r).unwrap()).collect();
            out.push((text, channels.holding_from(0)));
        }
        out
    }

    #[test]
    fn a_line_ends_at_a_lone_cr_and_at_crlf_split_across_chunks() {
        let mut s = V4Out::default();
        s.channel(1, "access");
        s.event(1, 2, b"a\rb\r", false, false);
        s.event(1, 3, b"\nc\r\n", false, false);
        let records: Vec<Vec<String>> = records_of(&BTreeMap::new(), 1 << 20, &s)
            .into_iter()
            .map(|(r, _)| r)
            .collect();
        assert_eq!(records, [vec!["a", "b"], vec!["c"]]);
    }

    #[test]
    fn an_event_of_nothing_but_whitespace_is_no_record_with_or_without_a_line_breaker() {
        let mut s = V4Out::default();
        s.channel(1, "access");
        s.event(1, 2, b"a\n  \n\t\nb\n", false, false);
        s.channel(2, "java");
        s.event(2, 3, b"ERROR one\n \nERROR two\n", false, true);
        let records: Vec<String> = records_of(&rule(r"([\r\n]+)(?:ERROR| )"), 1 << 20, &s)
            .into_iter()
            .flat_map(|(r, _)| r)
            .collect();
        assert_eq!(records, ["a", "b", "ERROR one", "ERROR two"]);
    }

    /// A raw HEC body is broken as a stream of its sourcetype that ends with it, so the same bytes make the same events.
    #[test]
    fn a_raw_hec_body_makes_the_events_a_channel_of_its_sourcetype_makes_of_the_same_bytes() {
        let rules = rule(AT_ERROR);
        let body = b"ERROR one\r  at a\rERROR two\r\n  \nERROR three\n\n";
        for sourcetype in ["java", "access"] {
            let mut s = V4Out::default();
            s.channel(1, sourcetype);
            s.event(1, 2, body, false, true);
            let streamed: Vec<String> = records_of(&rules, 1 << 20, &s)
                .into_iter()
                .flat_map(|(r, _)| r)
                .collect();
            let mut raw = crate::io::api::Records::broken(body.to_vec(), rules.get(sourcetype).cloned());
            let raw: Vec<String> = std::iter::from_fn(|| raw.next_record())
                .map(|r| String::from_utf8(r).unwrap())
                .collect();
            assert_eq!(raw, streamed, "{sourcetype}");
        }
    }

    #[test]
    fn a_line_begun_in_a_chunk_without_an_id_holds_the_next_id_open() {
        let mut s = V4Out::default();
        s.channel(1, "access");
        s.event(1, 2, b"one\n", false, false);
        // A chunk without an id: flags for its time alone.
        s.0.push(0xfc);
        s.varint(1);
        s.varint(1 << 3);
        s.varint(1_700_000_000);
        s.string(b"ab");
        s.event(1, 5, b"c", false, false);
        s.event(1, 6, b"\n", false, false);
        assert_eq!(
            records_of(&BTreeMap::new(), 1 << 20, &s),
            [
                (vec!["one".to_string()], None),
                (vec![], Some(3)),
                (vec![], Some(3)),
                (vec!["abc".to_string()], None),
            ]
        );
    }

    #[test]
    fn a_line_split_across_chunks_is_one_record_and_holds_its_first_chunk_open() {
        let mut s = V4Out::default();
        s.channel(1, "access");
        s.event(1, 2, b"one\r\ntw", false, false);
        s.event(1, 3, b"o\nthr", false, false);
        s.event(1, 4, b"ee\n\n", false, false);
        assert_eq!(
            records_of(&BTreeMap::new(), 1 << 20, &s),
            [
                (vec!["one".to_string()], Some(2)),
                (vec!["two".to_string()], Some(3)),
                (vec!["three".to_string()], None),
            ]
        );
    }

    #[test]
    fn an_event_of_several_lines_spans_chunks_and_closes_at_done() {
        let mut s = V4Out::default();
        s.channel(1, "java");
        s.event(1, 2, b"ERROR one\n  at a\n", false, false);
        s.event(1, 3, b"  at b\nERROR two\n", false, false);
        s.event(1, 4, b"", false, true);
        assert_eq!(
            records_of(&rule(AT_ERROR), 1 << 20, &s),
            [
                (vec![], Some(2)),
                (vec!["ERROR one\n  at a\n  at b".to_string()], Some(3)),
                (vec!["ERROR two".to_string()], None),
            ]
        );
    }

    #[test]
    fn done_ends_the_last_event_with_what_is_held() {
        // A file that stops without a newline: the forwarder sends what it holds with `_done`, as an indexer reads it.
        let mut s = V4Out::default();
        s.channel(1, "java");
        s.event(1, 2, b"ERROR x\n  at a\n  at b", false, true);
        s.channel(2, "access");
        s.event(2, 3, b"a\nb", false, true);
        assert_eq!(
            records_of(&rule(AT_ERROR), 1 << 20, &s),
            [
                (vec!["ERROR x\n  at a\n  at b".to_string()], None),
                (vec!["a".to_string(), "b".to_string()], None),
            ]
        );
    }

    #[test]
    fn a_line_breaker_breaks_a_stream_without_line_ends_once_the_next_chunk_shows_where_the_break_ends() {
        let mut s = V4Out::default();
        s.channel(1, "java");
        s.event(1, 2, b"<e>1</e", false, false);
        s.event(1, 3, b">\n<e>2</e>", false, false);
        s.event(1, 4, b"<e>3333333", false, false);
        s.event(1, 5, b"</e>", false, true);
        assert_eq!(
            records_of(&rule(r"</e>(\s*)"), 1 << 20, &s),
            [
                (vec![], Some(2)),
                (vec!["<e>1</e>".to_string()], Some(3)),
                (vec!["<e>2</e>".to_string()], Some(4)),
                (vec!["<e>3333333</e>".to_string()], None),
            ],
            "a break reaching the end of the bytes held waits for the next chunk, or for `_done`"
        );
    }

    #[test]
    fn an_event_broken_across_chunks_holds_the_id_of_its_first_byte_open() {
        let mut s = V4Out::default();
        s.channel(1, "java");
        s.event(1, 2, b"<e>1</e><e>2", false, false);
        s.event(1, 3, b"</e><e>", false, false);
        s.event(1, 4, b"3</e><e>4</e>x", false, false);
        assert_eq!(
            records_of(&rule(r"</e>()"), 1 << 20, &s),
            [
                (vec!["<e>1</e>".to_string()], Some(2)),
                (vec!["<e>2</e>".to_string()], Some(3)),
                (vec!["<e>3</e>".to_string(), "<e>4</e>".to_string()], Some(4)),
            ]
        );
    }

    #[test]
    fn an_event_longer_than_a_record_may_be_is_cut_and_none_is_dropped() {
        let mut s = V4Out::default();
        s.channel(1, "access");
        s.event(1, 2, b"abcdefg", false, false);
        s.event(1, 3, b"hij\nklmnopqrstu\n", false, false);
        let pieces: Vec<Vec<String>> = records_of(&BTreeMap::new(), 4, &s)
            .into_iter()
            .map(|(r, _)| r)
            .collect();
        assert_eq!(pieces, [vec!["abcd"], vec!["efgh", "ij", "klmn", "opqr", "stu"]]);
        let mut s = V4Out::default();
        s.channel(1, "java");
        s.event(1, 2, b"ERROR 1\nab\nabc\nERROR 2", false, true);
        assert_eq!(
            records_of(&rule(AT_ERROR), 10, &s)[0].0,
            ["ERROR 1\nab", "\nabc", "ERROR 2"],
            "an event broken off is cut at the record limit"
        );
    }

    #[test]
    fn an_event_the_forwarder_broke_is_one_record_as_it_came() {
        let mut s = V4Out::default();
        s.channel(1, "_json");
        s.event(1, 2, b"{\"a\":1}\n{\"b\":2}", true, true);
        s.event(1, 3, b"", true, true);
        assert_eq!(
            records_of(&BTreeMap::new(), 1 << 20, &s),
            [(vec!["{\"a\":1}\n{\"b\":2}".to_string()], None), (vec![], None)]
        );
    }

    #[test]
    fn a_channel_freed_and_defined_again_ends_its_old_stream_first() {
        let mut s = V4Out::default();
        s.channel(1, "java");
        s.event(1, 2, b"ERROR one\n  at a\npartial", false, false);
        s.free(1);
        s.channel(1, "access");
        s.event(1, 3, b"x\n", false, false);
        assert_eq!(
            records_of(&rule(AT_ERROR), 1 << 20, &s),
            [
                (vec![], Some(2)),
                (vec!["ERROR one\n  at a\npartial".to_string(), "x".to_string()], None),
            ]
        );
    }

    #[test]
    fn a_quiet_channel_closes_what_it_holds() {
        let rules = rule(AT_ERROR);
        let mut decoder = Decoder::new(1 << 20);
        let mut s = V4Out::default();
        s.channel(1, "java");
        s.event(1, 2, b"ERROR one\n  at a\npart", false, false);
        decoder.push(&s.0);
        let mut channels = Channels::new(&rules, 1 << 20);
        let now = Instant::now();
        let event = decoder.next_event().unwrap().unwrap();
        let filed = filed(&event).unwrap();
        assert!(channels.take(event, filed, now).is_empty());
        assert_eq!(channels.next_quiet(), Some(now + QUIET));
        assert!(channels.close_quiet(now + QUIET / 2).is_empty());
        let closed: Vec<Vec<u8>> = channels.close_quiet(now + QUIET).map(|(r, _)| r).collect();
        assert_eq!(
            closed,
            [b"ERROR one\n  at a\npart".to_vec()],
            "what it holds is its last event"
        );
        assert_eq!((channels.holding_from(0), channels.next_quiet()), (None, None));
    }

    #[test]
    fn the_channels_of_a_connection_hold_no_more_than_one_event_and_the_slack_may_between_them() {
        let mut java = b"ERROR 1\n".to_vec();
        java.resize(BREAK_SLACK + 8, b'x');
        let mut s = V4Out::default();
        s.channel(1, "access");
        s.channel(2, "java");
        s.event(1, 2, b"abcdef", false, false);
        s.event(2, 3, &java, false, false);
        s.event(1, 4, b"g", false, false);
        let records = records_of(&rule(AT_ERROR), 10, &s);
        assert_eq!((records[0].0.len(), records[0].1), (0, Some(2)));
        assert_eq!(
            (records[1].0.concat().into_bytes(), records[1].1),
            (java, Some(2)),
            "past the bound, the channel holding most ends its stream"
        );
        assert_eq!((records[2].0.len(), records[2].1), (0, Some(2)));
    }

    #[test]
    fn a_line_breaker_holds_the_slack_past_the_limit_so_a_break_there_is_taken_whole() {
        // The break `\n2026-` starts below the limit and ends in the next chunk.
        let mut first = vec![b'x'; 1023];
        first.extend_from_slice(b"\n2");
        let mut s = V4Out::default();
        s.channel(1, "java");
        s.event(1, 2, &first, false, false);
        s.event(1, 3, b"026-10-11 next", false, true);
        let records = records_of(&rule(r"(\n)2026-"), 1024, &s);
        assert_eq!(
            records[1].0,
            ["x".repeat(1023), "2026-10-11 next".to_string()],
            "the break is not cut into the record before it"
        );
    }

    #[test]
    fn records_cut_from_one_chunk_cost_their_bytes_and_one_filed_set() {
        let filed = Arc::new(vec![("host".to_string(), b"web-1".to_vec())]);
        let other = Arc::new(vec![("host".to_string(), b"web-2".to_vec())]);
        let mut cuts = Cuts::default();
        for record in [&b"a"[..], b"bc", b"d"] {
            cuts.push(record, &filed);
        }
        cuts.push(b"e", &other);
        assert_eq!(cuts.filed.len(), 2, "one filed set for each chunk");
        assert_eq!(
            cuts.size(),
            5 + 4 * 4 + 4 * 9,
            "each record its bytes, its end and its copy of the filed set"
        );
        let taken: Vec<(Vec<u8>, Arc<Filed>)> = cuts.by_ref().collect();
        assert!(cuts.is_empty());
        let records: Vec<&[u8]> = taken.iter().map(|(r, _)| &r[..]).collect();
        assert_eq!(records, [&b"a"[..], b"bc", b"d", b"e"]);
        let shared = taken.iter().map(|(_, f)| Arc::ptr_eq(f, &filed));
        assert_eq!(
            shared.collect::<Vec<_>>(),
            [true, true, true, false],
            "each shares its chunk's filed set"
        );
    }

    #[test]
    fn ids_are_acked_through_the_one_before_the_oldest_held_open() {
        let mut acks = Acks::default();
        assert_eq!(acks.after(Some(2), None), Some((2, 2)));
        assert_eq!(acks.after(Some(3), Some(3)), None, "id 3's bytes are held open");
        assert_eq!(acks.after(Some(4), Some(3)), None);
        assert_eq!(
            acks.after(Some(5), Some(5)),
            Some((3, 4)),
            "3 and 4 closed with 5's records"
        );
        assert_eq!(acks.after(None, None), Some((5, 5)), "a quiet channel closed");
        assert_eq!(acks.after(None, None), None, "nothing new to ack");
        assert_eq!(acks.after(Some(6), None), Some((6, 6)));
        assert_eq!(Acks::default().after(None, None), None, "no id seen");
        let mut early = Acks::default();
        assert_eq!(early.after(Some(9), Some(9)), None);
        assert_eq!(early.early(None), None, "no channel holds an ack back");
        assert_eq!(early.early(Some(9)), Some((9, 9)), "a held line acked before it ends");
        assert_eq!(early.after(Some(10), None), Some((10, 10)));
        let mut late = Acks::default();
        assert_eq!(late.after(Some(7), Some(7)), None);
        assert_eq!(
            late.after(Some(8), None),
            Some((7, 8)),
            "the first range starts at the first id sent"
        );
    }

    /// An event of `fields` and `raw` on a channel naming `index`, as the decoder reads it.
    fn decoded(fields: &[u8], count: u64, raw: &[u8]) -> Event {
        let mut s = V4Out::default();
        s.channel(1, "access");
        s.0.push(0xfc);
        s.varint(1);
        s.varint(1 | 1 << 2 | 1 << 3);
        s.varint(9);
        s.varint(1_700_000_000);
        s.varint(count);
        s.0.extend_from_slice(fields);
        s.string(raw);
        let mut decoder = Decoder::new(1 << 20);
        decoder.push(&s.0);
        decoder.next_event().unwrap().unwrap()
    }

    fn text_field(key: &str, value: &str) -> Vec<u8> {
        let mut out = vec![0x04, key.len() as u8];
        out.extend_from_slice(key.as_bytes());
        out.push(value.len() as u8);
        out.extend_from_slice(value.as_bytes());
        out
    }

    #[test]
    fn a_csv_field_resolves_its_doubled_quotes() {
        // `INDEXED_EXTRACTIONS = csv` names the quoted field's bytes inside the raw's quotes, escapes kept.
        let raw = br#"r9,1,"a,""b"" c""#;
        let event = decoded(&[0x13, 6, b'q', b'u', b'o', b't', b'e', b'd', 6, 9], 1, raw);
        let got = filed(&event).unwrap();
        let encoded = &got.iter().find(|(k, _)| k == hec::FIELDS).unwrap().1;
        let member = crate::core::variant::member(encoded, "quoted").unwrap();
        assert_eq!(
            format!("{:?}", crate::core::variant::read(member)),
            r#"Str("a,\"b\" c")"#
        );
    }

    #[test]
    fn an_event_is_filed_under_its_channel_its_index_its_time_and_its_indexed_fields() {
        let raw = br#"{"s":"a \"q\"","n":-7}"#;
        let fields = [
            text_field("_path", "/var/log/app.log"),
            text_field("_MetaData:Index", "main"),
            text_field("_tcpoutTime", "1700000000.1"),
            vec![0x07, 3, b'e', b'n', b'v', 3, b'l', b'a', b'b'],
            // `s`, a raw slice holding JSON escapes; `n`, a raw slice; `o.b{}` twice, an array.
            vec![0x1b, 1, b's', 6, 7],
            vec![0x0f, 1, b'n', 19, 2],
            vec![0x03, 5, b'o', b'.', b'b', b'{', b'}', 1],
            vec![0x03, 5, b'o', b'.', b'b', b'{', b'}', 2],
            vec![0x0b, 1, b'z', 60],
            // `.123` of a second; `date_hour`, and `timestamp` valued `none`.
            vec![0x42, 0, 0xf7, 0x01],
            vec![0x42, 3, 2],
            vec![0x4a, 46, 48],
            // `_event_status`: the forwarder sends this event again.
            [&[0x44, 69, 6][..], b"resent"].concat(),
        ];
        let event = decoded(&fields.concat(), fields.len() as u64, raw);
        let got = filed(&event).unwrap();
        let text = |key: &str| {
            let value = &got.iter().find(|(k, _)| k == key)?.1;
            Some(String::from_utf8(value.clone()).unwrap())
        };
        assert_eq!(text("host").as_deref(), Some("web-1"));
        assert_eq!(text("source").as_deref(), Some("/var/log/app.log"));
        assert_eq!(text("sourcetype").as_deref(), Some("access"));
        assert_eq!(text("index").as_deref(), Some("main"));
        assert_eq!(text("time").as_deref(), Some("1700000000123000"));
        let encoded = &got.iter().find(|(k, _)| k == hec::FIELDS).unwrap().1;
        let read = |key: &str| {
            let member = crate::core::variant::member(encoded, key)?;
            Some(format!("{:?}", crate::core::variant::read(member)))
        };
        assert_eq!(read("env").as_deref(), Some(r#"Str("lab")"#));
        assert_eq!(read("s").as_deref(), Some(r#"Str("a \"q\"")"#));
        assert_eq!(read("n").as_deref(), Some(r#"Str("-7")"#));
        assert!(
            read("o.b{}").is_some_and(|v| v.starts_with("Array")),
            "{:?}",
            read("o.b{}")
        );
        assert_eq!(read("z").as_deref(), Some("Null"));
        assert_eq!(read("_event_status").as_deref(), Some(r#"Str("resent")"#));
        for dropped in ["_path", "_tcpoutTime", "_MetaData:Index", "date_hour", "timestamp"] {
            assert_eq!(read(dropped), None, "{dropped} is the forwarder's bookkeeping");
        }

        let default = decoded(&text_field("_MetaData:Index", "default"), 1, b"x");
        assert!(filed(&default).unwrap().iter().all(|(k, _)| k != "index"));
        let unknown = decoded(&[0x0b, 1, b'k', 70], 1, b"x");
        assert!(filed(&unknown).unwrap_err().contains("dictionary entry 70"));
        let unknown_key = decoded(&[0x42, 70, 1], 1, b"x");
        assert!(filed(&unknown_key).unwrap_err().contains("dictionary entry 70"));
        let no_fraction = decoded(&[0x42, 0, 0], 1, b"x");
        assert!(filed(&no_fraction).unwrap_err().contains("no fraction"));
    }

    #[test]
    fn a_subsecond_is_the_fraction_as_written_to_the_microsecond() {
        // As a forwarder sent each fraction, parsing `ts` of an indexed JSON extraction.
        let sent = [
            (".1", 3, 100_000),
            (".12", 25, 120_000),
            (".123", 247, 123_000),
            (".1234", 2469, 123_400),
            (".123456", 246_913, 123_456),
            (".1234567", 246_913, 123_456),
            (".5", 11, 500_000),
            (".5000", 10_001, 500_000),
            (".100", 201, 100_000),
            (".999999", 1_999_999, 999_999),
            (".0", 2, 0),
            (".07", 30, 70_000),
            (".001", 12, 1_000),
            (".007", 60, 7_000),
            (".000123", 1_976, 123),
            (".000001", 96, 1),
            (".000000001", 64, 0),
        ];
        for (fraction, value, micros) in sent {
            assert_eq!(subsecond_micros(value), Some(micros), "{fraction}");
        }
        assert_eq!(subsecond_micros(0), None, "no one marks where the digits start");
        assert_eq!(subsecond_micros(2 * 1_234_567 + 1), None, "seven digits");
        assert_eq!(subsecond_micros(1 << 7), None, "seven zeros");
    }

    #[test]
    fn forwarder_info_is_told_by_its_sourcetype_field() {
        let info = decoded(
            &text_field("MetaData:Sourcetype", "sourcetype::fwdinfo"),
            1,
            b"ForwarderInfo",
        );
        assert!(forwarder_info(&info));
        let heartbeat = decoded(&text_field("MetaData:Sourcetype", "sourcetype::fwd-hb"), 1, b"HB");
        assert!(forwarder_info(&heartbeat), "a heartbeat, which holds no file's bytes");
        assert!(!forwarder_info(&decoded(&text_field("_path", "x"), 1, b"x")));
    }

    #[test]
    fn a_compressed_frame_is_told_from_a_v3_message_by_its_first_two_integers() {
        let mut plain = Vec::new();
        s2s::write_message(&[(b"__s2s_capabilities", b"ack=0;compression=0")], &mut plain);
        assert_eq!(compressed_frame(&plain), Some(false));
        assert_eq!(compressed_frame(&plain[..7]), None);
        let frame = [&70u32.to_be_bytes()[..], &66u32.to_be_bytes(), &[0x78, 0x9c]].concat();
        assert_eq!(compressed_frame(&frame), Some(true));
    }
}
