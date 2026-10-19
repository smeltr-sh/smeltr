//! What a Splunk forwarder sends over an S2S connection, and what a receiver sends back.
//!
//! A forwarder opens with a fixed signature and its capabilities, in version 3 of Splunk-to-Splunk, and waits for the
//! receiver's answer, also v3. It then sends its `ForwarderInfo` event in v3, and switches to version 4 for the rest
//! of the connection, unless it is set to stay on v3. A forwarder that asked for compression sends its v4 as one zlib
//! stream after [`COMPRESSED`], and over TCP its v3 messages each in a zlib frame. Over TLS its v3 messages are plain.
//!
//! ```text
//!   forwarder: signature  v3 capabilities             v3 ForwarderInfo  v4 ...
//!   receiver:                            v3 answer                      v4 acks ...
//! ```
//!
//! A v3 message is a map of strings, framed by lengths ([`read_message`]).
//!
//! A v4 stream is frames, each led by one byte. Integers are LEB128 varints and a string is a varint length and its
//! bytes. A channel names what its events are filed under, once; each event then names its channel.
//!
//! ```text
//!   ff  timezone table                    (string)
//!   fe  channel id, source, host, sourcetype, v3 number  (each string length + 1, 0 absent)
//!       field names: count, strings        (a CSV header, which fields name by index)
//!   fd  free channel id                    (the id is defined again before it is used again)
//!   fc  event: channel, flags, what the flags name, fields, raw
//!   fb  ack of one event id                (receiver to forwarder)
//!   fa  ack of event ids first..=last      (receiver to forwarder)
//! ```
//!
//! Nothing around a frame or a field says how long it is, so a byte this decoder does not know cannot be stepped
//! over: it is refused, naming the frame it stands in, rather than guessed past.

use std::collections::HashMap;
use std::sync::Arc;

/// The byte a forwarder sends before a compressed stream: what follows it is one zlib stream, sync-flushed, for the
/// rest of the connection, or the rest of an `httpout` request's body.
pub const COMPRESSED: u8 = 0xf9;

/// The most channels a connection may hold at once: the `channel_limit` a receiver tells the forwarder, which frees
/// one (`fd`) before it defines another past it.
pub const CHANNEL_LIMIT: usize = 300;

const TIMEZONE: u8 = 0xff;
const CHANNEL: u8 = 0xfe;
const FREE: u8 = 0xfd;
const EVENT: u8 = 0xfc;
const ACK: u8 = 0xfb;
const ACK_RANGE: u8 = 0xfa;

/// What an event's flags name, in the order the event carries them: bit 1, then 2, then 3, then 0.
const STREAM: u64 = 1 << 1;
const ID: u64 = 1 << 2;
const TIME: u64 = 1 << 3;
const FIELDS: u64 = 1;
/// Flags that carry no bytes. `_hpn` (bit 4), `_charSet` (bit 5) and `_nfd` (bit 14) are forwarder bookkeeping, and so
/// are bit 10, which a forwarder sets on some of its empty `_done` events, and bit 11, which it sets on what it sends
/// again after a connection was lost. `BROKEN` is the forwarder's `_linebreaker`.
const BROKEN: u64 = 1 << 6;
const DONE: u64 = 1 << 9;
const KNOWN: u64 = STREAM | ID | TIME | FIELDS | 1 << 4 | 1 << 5 | BROKEN | DONE | 1 << 10 | 1 << 11 | 1 << 14;

/// A frame this decoder does not know, or one that contradicts itself. The connection it came on cannot be read
/// further: the next frame's start is unknown.
#[derive(Debug, Clone, PartialEq)]
pub struct Malformed {
    /// The offset of the frame at fault in the decoded stream; for [`Inflate`], the offset in the zlib stream; for
    /// the signature and a v3 message, 0, its own start, which its caller places in the stream.
    pub at: u64,
    pub what: String,
}

impl std::fmt::Display for Malformed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "S2S frame at byte {}: {}", self.at, self.what)
    }
}

/// What a channel's events are filed under, as the forwarder names them: `source::…`, `host::…`, `sourcetype::…`.
#[derive(Debug, Default, PartialEq)]
pub struct Channel {
    /// The id the forwarder names it by. A channel freed and defined again under the same id is another `Channel`.
    pub id: u64,
    pub source: Option<Vec<u8>>,
    pub host: Option<Vec<u8>>,
    pub sourcetype: Option<Vec<u8>>,
    /// The field names a field may name by index.
    names: Vec<Arc<[u8]>>,
}

/// One event: a chunk of a file, or an event the forwarder broke itself.
#[derive(Debug)]
pub struct Event {
    pub channel: Arc<Channel>,
    pub stream: Option<Stream>,
    /// What the receiver acks it by. `httpout` sends none: it is acked by its request's answer.
    pub id: Option<u64>,
    /// Unix seconds.
    pub time: Option<u64>,
    pub fields: Vec<Field>,
    pub raw: Vec<u8>,
    /// The forwarder broke this event itself, so it is whole: structured input, or its own line breaking.
    pub broken: bool,
    /// The forwarder has sent all its source holds for now: no more of this stream follows until the source grows.
    pub done: bool,
}

/// Where an event's bytes sit in the source the forwarder reads.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Stream {
    pub id: u64,
    /// Where in the source the event's bytes start.
    pub offset: u64,
    /// The event's place among those broken from one chunk.
    pub index: u64,
}

/// An indexed field: from inputs.conf `_meta`, from INDEXED_EXTRACTIONS, or the forwarder's own.
#[derive(Debug, Clone, PartialEq)]
pub struct Field {
    pub key: Key,
    pub value: Value,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Key {
    /// Shared, as a channel's field name is by every field naming it.
    Text(Arc<[u8]>),
    /// An entry of Splunk's static dictionary ([`dictionary`]).
    Dictionary(u64),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Int(u64),
    Text(Vec<u8>),
    /// An entry of Splunk's static dictionary ([`dictionary`]).
    Dictionary(u64),
    /// Bytes of the event's raw, as they stood in it.
    Raw {
        start: usize,
        len: usize,
        escapes: Escapes,
    },
}

/// The escapes a raw slice holds: a field taken from structured input keeps its source's.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Escapes {
    None,
    /// JSON's backslash escapes.
    Json,
    /// CSV's doubled quote.
    Csv,
}

impl Event {
    /// The bytes a `Value::Raw` names. The decoder checked that every one lies inside the raw.
    pub fn slice(&self, start: usize, len: usize) -> &[u8] {
        &self.raw[start..start + len]
    }
}

/// Splunk's static dictionary, by index: field names, and words a value may be. Entry 0 is the fraction of the event's
/// second, as `subsecond_micros` in the receiver reads it.
const DICTIONARY: [&str; 70] = [
    "_subsecond",
    "date_second",
    "date_minute",
    "date_hour",
    "date_year",
    "date_month",
    "date_mday",
    "date_wday",
    "date_zone",
    "sunday",
    "monday",
    "tuesday",
    "wednesday",
    "thursday",
    "friday",
    "saturday",
    "january",
    "february",
    "march",
    "april",
    "may",
    "june",
    "july",
    "august",
    "september",
    "october",
    "november",
    "december",
    "local",
    "punct",
    "timestartpos",
    "timeendpos",
    "_indextime",
    "-60",
    "-120",
    "-180",
    "-240",
    "-300",
    "-360",
    "-420",
    "-480",
    "-540",
    "-600",
    "-660",
    "meta",
    "truncated",
    "timestamp",
    "invalid",
    "none",
    "eet",
    "elt",
    "ert",
    "iet",
    "ilt",
    "usz",
    "nev",
    "nh",
    "ns",
    "nst",
    "nstr",
    "null",
    "false",
    "true",
    "metric_name",
    "_value",
    "time",
    "linecount",
    "metric_type",
    "metric_timestamp",
    "_event_status",
];

/// Entry `index` of Splunk's static dictionary.
pub fn dictionary(index: u64) -> Option<&'static str> {
    DICTIONARY.get(usize::try_from(index).ok()?).copied()
}

/// Reads a forwarder's v4 stream as it arrives, in whatever pieces the transport hands it.
pub struct Decoder {
    /// Bytes pushed and not yet a whole frame.
    pending: Vec<u8>,
    /// How much of `pending` was decoded.
    at: usize,
    /// The stream offset of `pending[0]`.
    base: u64,
    channels: HashMap<u64, Arc<Channel>>,
    max_frame: usize,
    /// How many bytes the frame cut short needs held before it can be read further.
    need: usize,
    /// How far the frame cut short was read, so a frame arriving in many pieces is read once, not once a piece.
    resume: Resume,
}

impl Decoder {
    /// The memory it holds for frames not yet whole.
    pub fn memory(&self) -> usize {
        self.pending.capacity()
    }

    /// A decoder that refuses a frame longer than `max_frame`, so a stream cannot make it hold more than that while
    /// it waits for the rest of a frame.
    pub fn new(max_frame: usize) -> Decoder {
        Decoder {
            pending: Vec::new(),
            at: 0,
            base: 0,
            channels: HashMap::new(),
            max_frame,
            need: 0,
            resume: Resume::default(),
        }
    }

    pub fn push(&mut self, bytes: &[u8]) {
        if self.at > 0 && self.at * 2 >= self.pending.len() {
            self.pending.drain(..self.at);
            self.base += self.at as u64;
            self.at = 0;
        }
        self.pending.extend_from_slice(bytes);
    }

    /// The bytes held that are not yet a whole frame.
    pub fn held(&self) -> usize {
        self.pending.len() - self.at
    }

    /// How far the stream is decoded: the end of the last frame read, which is where the last event returned ends.
    pub fn offset(&self) -> u64 {
        self.base + self.at as u64
    }

    /// The next event, or `None` until more bytes are pushed. Timezone and channel frames are taken on the way. After
    /// `None`, what it holds is within its frame limit.
    pub fn next_event(&mut self) -> Result<Option<Event>, Malformed> {
        loop {
            if self.held() < self.need {
                return Ok(None);
            }
            let mut cursor = Cursor {
                bytes: &self.pending[self.at..],
                i: 0,
                need: 0,
                max: self.max_frame,
            };
            let frame = frame(&mut self.channels, &mut self.resume, &mut cursor);
            let at = self.base + self.at as u64;
            match frame {
                Ok(event) => {
                    self.at += cursor.i;
                    self.need = 0;
                    self.resume = Resume::default();
                    if event.is_some() {
                        return Ok(event);
                    }
                }
                Err(Cut::Short) => {
                    self.need = cursor.need;
                    return Ok(None);
                }
                Err(Cut::Bad(what)) => return Err(Malformed { at, what }),
            }
        }
    }
}

/// One frame. A whole timezone or channel frame is `None`; a frame cut short is `Cut::Short`.
fn frame(channels: &mut HashMap<u64, Arc<Channel>>, resume: &mut Resume, c: &mut Cursor) -> Result<Option<Event>, Cut> {
    match c.byte()? {
        TIMEZONE => {
            c.string()?;
            Ok(None)
        }
        CHANNEL => {
            let id = c.varint()?;
            if !channels.contains_key(&id) && channels.len() >= CHANNEL_LIMIT {
                return Err(Cut::Bad(format!(
                    "channel {id} past the {CHANNEL_LIMIT} a connection may hold"
                )));
            }
            let source = c.plus_one()?;
            let host = c.plus_one()?;
            let sourcetype = c.plus_one()?;
            c.plus_one()?;
            let names = items(c, &mut resume.names, |c| Ok(Arc::from(c.string()?)))?;
            let channel = Channel {
                id,
                source,
                host,
                sourcetype,
                names,
            };
            channels.insert(id, Arc::new(channel));
            Ok(None)
        }
        FREE => {
            channels.remove(&c.varint()?);
            Ok(None)
        }
        EVENT => event(channels, &mut resume.fields, c).map(Some),
        other => Err(Cut::Bad(format!("unknown frame {other:#04x}"))),
    }
}

fn event(
    channels: &HashMap<u64, Arc<Channel>>,
    resume: &mut Option<Items<Field>>,
    c: &mut Cursor,
) -> Result<Event, Cut> {
    let id = c.varint()?;
    let channel = channels
        .get(&id)
        .ok_or_else(|| Cut::Bad(format!("an event on channel {id}, which was never defined")))?
        .clone();
    let flags = c.varint()?;
    if flags & !KNOWN != 0 {
        return Err(Cut::Bad(format!("an event with unknown flags {:#x}", flags & !KNOWN)));
    }
    let stream = match flags & STREAM {
        0 => None,
        _ => Some(Stream {
            id: c.varint()?,
            offset: c.varint()?,
            index: c.varint()?,
        }),
    };
    let id = (flags & ID != 0).then(|| c.varint()).transpose()?;
    let time = (flags & TIME != 0).then(|| c.varint()).transpose()?;
    let fields = match flags & FIELDS {
        0 => Vec::new(),
        _ => items(c, resume, |c| field(c, &channel))?,
    };
    let raw = c.string()?.to_vec();
    for field in &fields {
        if let Value::Raw { start, len, .. } = field.value
            && start.checked_add(len).is_none_or(|end| end > raw.len())
        {
            return Err(Cut::Bad(format!(
                "a field names raw bytes {start}..+{len} of a raw of {}",
                raw.len()
            )));
        }
    }
    Ok(Event {
        channel,
        stream,
        id,
        time,
        fields,
        raw,
        broken: flags & BROKEN != 0,
        done: flags & DONE != 0,
    })
}

/// A field: a type byte, a key, a value. The type's bit 6 takes the key from the static dictionary; else bit 1, or
/// a field with bit 0 clear, carries it as text; else it is an index into the channel's field names. Bits 2 to 4
/// say what the value is.
fn field(c: &mut Cursor, channel: &Channel) -> Result<Field, Cut> {
    let kind = c.byte()?;
    if kind & !0x5f != 0 {
        return Err(Cut::Bad(format!("unknown field type {kind:#04x}")));
    }
    let key = if kind & 0x40 != 0 {
        Key::Dictionary(c.varint()?)
    } else if kind & 0x02 != 0 || kind & 0x01 == 0 {
        Key::Text(Arc::from(c.string()?))
    } else {
        let n = c.varint()?;
        let name = usize::try_from(n)
            .ok()
            .and_then(|n| channel.names.get(n))
            .ok_or_else(|| {
                Cut::Bad(format!(
                    "a field names field {n} of a channel with {}",
                    channel.names.len()
                ))
            })?;
        Key::Text(name.clone())
    };
    let value = match kind >> 2 & 7 {
        0 => Value::Int(c.varint()?),
        1 => Value::Text(c.string()?.to_vec()),
        2 => Value::Dictionary(c.varint()?),
        raw @ (3 | 4 | 6) => Value::Raw {
            start: c.length()?,
            len: c.length()?,
            escapes: match raw {
                3 => Escapes::None,
                4 => Escapes::Csv,
                _ => Escapes::Json,
            },
        },
        other => {
            return Err(Cut::Bad(format!(
                "field type {kind:#04x} has unknown value kind {other}"
            )));
        }
    };
    Ok(Field { key, value })
}

/// The frame acking event ids `first..=last`, appended to `out`.
pub fn ack(first: u64, last: u64, out: &mut Vec<u8>) {
    match first == last {
        true => out.push(ACK),
        false => {
            out.push(ACK_RANGE);
            put_varint(first, out);
        }
    }
    put_varint(last, out);
}

fn put_varint(mut n: u64, out: &mut Vec<u8>) {
    while n >= 0x80 {
        out.push(n as u8 | 0x80);
        n >>= 7;
    }
    out.push(n as u8);
}

/// The zlib stream that follows [`COMPRESSED`], inflated as it arrives and never faster than its reader takes it.
pub struct Inflate {
    zlib: flate2::Decompress,
    /// Compressed bytes pushed, inflated up to `read`.
    input: Vec<u8>,
    read: usize,
    ended: bool,
}

impl Default for Inflate {
    fn default() -> Inflate {
        Inflate {
            zlib: flate2::Decompress::new(true),
            input: Vec::new(),
            read: 0,
            ended: false,
        }
    }
}

impl Inflate {
    /// The memory it holds for compressed bytes not yet inflated.
    pub fn memory(&self) -> usize {
        self.input.capacity()
    }

    pub fn push(&mut self, compressed: &[u8]) {
        // Kept until half is inflated, so a body drained in small pieces is not shifted once a piece.
        if self.read > 0 && self.read * 2 >= self.input.len() {
            self.input.drain(..self.read);
            self.read = 0;
        }
        self.input.extend_from_slice(compressed);
    }

    /// Inflates into `out` until it holds `limit` more bytes or what was pushed is all inflated, and says how many
    /// it added. A small input can inflate to far more than its size, so `limit` is what bounds the memory it costs.
    pub fn inflate(&mut self, out: &mut Vec<u8>, limit: usize) -> Result<usize, Malformed> {
        let start = out.len();
        let mut used = self.read;
        while out.len() - start < limit {
            if self.ended {
                if used < self.input.len() {
                    return Err(Malformed {
                        at: self.zlib.total_in(),
                        what: "bytes after the end of the compressed stream".to_string(),
                    });
                }
                break;
            }
            let room = (limit - (out.len() - start)).min(64 * 1024);
            let (before_in, before_out) = (self.zlib.total_in(), self.zlib.total_out());
            // Called with nothing left to read too: zlib may hold output it had no room for.
            let status = decompress_into(&mut self.zlib, &self.input[used..], out, room).map_err(|e| Malformed {
                at: self.zlib.total_in(),
                what: format!("the compressed stream does not inflate: {e}"),
            })?;
            used += (self.zlib.total_in() - before_in) as usize;
            self.ended = status == flate2::Status::StreamEnd;
            if self.zlib.total_in() == before_in && self.zlib.total_out() == before_out {
                break;
            }
        }
        self.read = used;
        Ok(out.len() - start)
    }
}

/// Inflates `input` onto the end of `out`, at most `room` bytes of it.
fn decompress_into(
    zlib: &mut flate2::Decompress,
    input: &[u8],
    out: &mut Vec<u8>,
    room: usize,
) -> Result<flate2::Status, flate2::DecompressError> {
    let len = out.len();
    out.resize(len + room, 0);
    let before = zlib.total_out();
    let status = zlib.decompress(input, &mut out[len..], flate2::FlushDecompress::Sync);
    out.truncate(len + (zlib.total_out() - before) as usize);
    status
}

/// The signature's first field, NUL-padded to 128 bytes.
const COOKED: &[u8] = b"--splunk-cooked-mode-v3--";

/// How long a forwarder's signature is: [`COOKED`] in 128 bytes, its server name in 256, its management port in 16,
/// each NUL-padded.
pub const SIGNATURE_LEN: usize = 400;

/// Who opened the connection, in its own words, for naming it in logs.
#[derive(Debug, Clone, PartialEq)]
pub struct Signature {
    /// The forwarder's server name, its hostname by default.
    pub server: String,
    /// Its management port, as the text it sent.
    pub port: String,
}

/// The signature a connection opens with, once [`SIGNATURE_LEN`] bytes are held, and `None` before. A first field
/// other than [`COOKED`] is refused as soon as its first wrong byte is held, so an HTTP client on the port is refused
/// at once.
pub fn signature(bytes: &[u8]) -> Result<Option<Signature>, Malformed> {
    let held = &bytes[..bytes.len().min(128)];
    let cooked = COOKED.iter().chain(std::iter::repeat(&0)).take(held.len());
    if !held.iter().eq(cooked) {
        return Err(Malformed {
            at: 0,
            what: "a connection that does not open with the S2S signature".to_string(),
        });
    }
    if bytes.len() < SIGNATURE_LEN {
        return Ok(None);
    }
    let text = |field: &[u8]| {
        let end = field.iter().position(|&b| b == 0).unwrap_or(field.len());
        String::from_utf8_lossy(&field[..end]).into_owned()
    };
    Ok(Some(Signature {
        server: text(&bytes[128..384]),
        port: text(&bytes[384..SIGNATURE_LEN]),
    }))
}

/// A v3 message: its keys and values, in the order sent, each without the NUL that ends it on the wire. They are
/// bytes because `_raw` is a chunk of a file cut wherever the forwarder cut it, inside a character too.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Message {
    pub pairs: Vec<(Vec<u8>, Vec<u8>)>,
}

impl Message {
    /// The first value under `key`.
    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        self.pairs.iter().find(|(k, _)| k == key).map(|(_, v)| &v[..])
    }
}

/// What follows a v3 message's pairs, the same on every message seen in both directions: a `u32` 0, then `_raw` as a
/// string. What it means is not known, so a message ending otherwise is refused.
const TRAILER_KEY: &[u8] = b"_raw";

/// The v3 message at the start of `bytes` and how many bytes it took, or `None` until all of it is held:
///
/// ```text
///   u32 size                          the bytes after this field
///   u32 count
///   count × (u32 len, key, u32 len, value)   each string ends in a NUL its length counts
///   u32 0, u32 5, "_raw\0"
/// ```
///
/// Every integer is big-endian. A message longer than `max`, its size field included, is refused as soon as its
/// size is held, so waiting for one holds no more than `max`.
pub fn read_message(bytes: &[u8], max: usize) -> Result<Option<(Message, usize)>, Malformed> {
    let Some(len) = framed(bytes, max)? else {
        return Ok(None);
    };
    let message = pairs(&bytes[4..len], u32::from_be_bytes).map_err(refused)?;
    Ok(Some((message, len)))
}

/// The compressed v3 frame at the start of `bytes` and how many bytes it took, or `None` until all of it is held. A
/// forwarder that asked for compression over TCP sends each v3 message so:
///
/// ```text
///   u32 total                         the bytes after this field
///   u32 zlen                          total - 4
///   zlen bytes: one zlib stream of the message without its size field, every integer little-endian, then a NUL
/// ```
///
/// The two leading integers are big-endian. A frame longer than `max` is refused as soon as its size is held, and one
/// whose message, counted as [`read_message`] counts it, is longer than `max` once that much has inflated.
pub fn read_compressed(bytes: &[u8], max: usize) -> Result<Option<(Message, usize)>, Malformed> {
    let Some(len) = framed(bytes, max)? else {
        return Ok(None);
    };
    // The zlib stream is followed by one NUL, which `zlen` counts.
    if len < 9
        || u32::from_be_bytes(bytes[4..8].try_into().expect("four bytes")) as usize != len - 8
        || bytes[len - 1] != 0
    {
        return Err(refused(format!(
            "a compressed frame of {} bytes whose zlib stream and NUL are not the rest of it",
            len - 4
        )));
    }
    let body_max = max.saturating_sub(4);
    let mut inflate = Inflate::default();
    inflate.push(&bytes[8..len - 1]);
    let mut body = Vec::new();
    inflate
        .inflate(&mut body, body_max + 1)
        .map_err(|e| refused(format!("its zlib stream at byte {}: {}", e.at, e.what)))?;
    if body.len() > body_max {
        return Err(refused(format!(
            "a compressed message that inflates past the {max} bytes this receiver takes"
        )));
    }
    if !inflate.ended {
        return Err(refused("a zlib stream that does not end with its frame".to_string()));
    }
    let message = pairs(&body, u32::from_le_bytes).map_err(refused)?;
    Ok(Some((message, len)))
}

/// The length of the frame at the start of `bytes`, its leading big-endian size field included, once all of it is
/// held. Refuses one longer than `max`.
fn framed(bytes: &[u8], max: usize) -> Result<Option<usize>, Malformed> {
    let Some(size) = bytes.get(..4) else {
        return Ok(None);
    };
    let len = (u32::from_be_bytes(size.try_into().expect("four bytes")) as usize).saturating_add(4);
    if len > max {
        return Err(refused(format!(
            "a v3 message of {len} bytes, over the {max} this receiver takes"
        )));
    }
    Ok((bytes.len() >= len).then_some(len))
}

/// A v3 message's count, pairs and trailer, which must be all of `body`.
fn pairs(body: &[u8], u32_from: fn([u8; 4]) -> u32) -> Result<Message, String> {
    let mut c = V3 { body, at: 0, u32_from };
    let count = c.int()?;
    let mut message = Message::default();
    for _ in 0..count {
        let key = c.string()?;
        message.pairs.push((key, c.string()?));
    }
    if (c.int()?, c.string()?) != (0, TRAILER_KEY.to_vec()) || c.at != body.len() {
        return Err("a v3 message that does not end in its usual trailer".to_string());
    }
    Ok(message)
}

struct V3<'a> {
    body: &'a [u8],
    at: usize,
    u32_from: fn([u8; 4]) -> u32,
}

impl<'a> V3<'a> {
    fn int(&mut self) -> Result<usize, String> {
        let bytes = self.take(4)?;
        Ok((self.u32_from)(bytes.try_into().expect("four bytes")) as usize)
    }

    /// A string without the NUL that ends it.
    fn string(&mut self) -> Result<Vec<u8>, String> {
        let n = self.int()?;
        match self.take(n)?.split_last() {
            Some((0, text)) => Ok(text.to_vec()),
            _ => Err("a v3 string that does not end in a NUL".to_string()),
        }
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], String> {
        let body = self.body;
        let bytes = self
            .at
            .checked_add(n)
            .and_then(|end| body.get(self.at..end))
            .ok_or("a v3 message whose contents run past its size")?;
        self.at += n;
        Ok(bytes)
    }
}

fn refused(what: String) -> Malformed {
    Malformed { at: 0, what }
}

/// Appends the v3 message holding `pairs` to `out`, uncompressed, as a receiver sends whatever the forwarder sends.
pub fn write_message(pairs: &[(&[u8], &[u8])], out: &mut Vec<u8>) {
    let start = out.len();
    out.extend([0; 4]);
    let put = |n: usize, out: &mut Vec<u8>| {
        out.extend(u32::try_from(n).expect("a v3 message under 4 GiB").to_be_bytes());
    };
    let string = |text: &[u8], out: &mut Vec<u8>| {
        put(text.len() + 1, out);
        out.extend_from_slice(text);
        out.push(0);
    };
    put(pairs.len(), out);
    for (key, value) in pairs {
        string(key, out);
        string(value, out);
    }
    put(0, out);
    string(TRAILER_KEY, out);
    let size = out.len() - start - 4;
    out[start..start + 4].copy_from_slice(&(size as u32).to_be_bytes());
}

/// Appends the answer to a forwarder's capabilities: the same answer to every forwarder, whatever it asked, with
/// this receiver's [`CHANNEL_LIMIT`]. `v4=true;pl=7` offers the v4 level 9.4 and 10.6 forwarders speak.
pub fn write_capabilities_answer(out: &mut Vec<u8>) {
    let answer = format!(
        "cap_response=success;cap_flush_key=true;idx_can_send_hb=true;idx_can_recv_token=true;\
         request_certificate=true;v4=true;channel_limit={CHANNEL_LIMIT};pl=7"
    );
    write_message(&[(b"__s2s_control_msg", answer.as_bytes())], out);
}

/// What a forwarder asks of the receiver, in its first message.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct Capabilities {
    /// It holds what it sent until the receiver acks it (`useACK`).
    pub ack: bool,
    /// It sends compressed: v4 after [`COMPRESSED`], and over TCP v3 in [`read_compressed`] frames.
    pub compression: bool,
}

/// Reads a message's `__s2s_capabilities`: `key=value` pairs separated by `;`. `ack` and `compression` are 0 or 1,
/// and 0 when absent. Other keys say what the forwarder can do, which asks nothing of the receiver.
pub fn capabilities(message: &Message) -> Result<Capabilities, Malformed> {
    let text = message
        .get(b"__s2s_capabilities")
        .ok_or_else(|| refused("a forwarder's first message without __s2s_capabilities".to_string()))?;
    let mut asks = Capabilities::default();
    for pair in text.split(|&b| b == b';').filter(|pair| !pair.is_empty()) {
        let bad = || refused(format!("an unreadable capability {:?}", String::from_utf8_lossy(pair)));
        let at = pair.iter().position(|&b| b == b'=').ok_or_else(bad)?;
        let (key, value) = (&pair[..at], &pair[at + 1..]);
        let flag = match key {
            b"ack" => &mut asks.ack,
            b"compression" => &mut asks.compression,
            _ => continue,
        };
        *flag = match value {
            b"0" => false,
            b"1" => true,
            _ => return Err(bad()),
        };
    }
    Ok(asks)
}

/// What a forwarder's next bytes are, once the receiver has answered.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Format {
    /// A v3 message, plain or in a compressed frame: the forwarder is set to stay on v3.
    V3,
    /// The v4 stream, for the rest of the connection.
    V4,
    /// [`COMPRESSED`], then the v4 stream inflated from one zlib stream, for the rest of the connection.
    CompressedV4,
}

/// Tells which [`Format`] the bytes starting with `first` are. A v3 message, or a compressed v3 frame, opens with
/// the high byte of its big-endian size, which a limit under `0xf9 << 24` bytes keeps below [`COMPRESSED`]. v4
/// opens with a frame's opcode, the timezone's `0xff` first, or with [`COMPRESSED`].
pub fn format(first: u8) -> Format {
    match first {
        COMPRESSED => Format::CompressedV4,
        f if f > COMPRESSED => Format::V4,
        _ => Format::V3,
    }
}

/// The lists read so far of a frame cut short: its own header is read again, its lists are not.
#[derive(Default)]
struct Resume {
    names: Option<Items<Arc<[u8]>>>,
    fields: Option<Items<Field>>,
}

/// A list cut short: the items read, how many are still to come, and where the next starts.
struct Items<T> {
    read: Vec<T>,
    left: u64,
    at: usize,
}

/// A count and that many items, carrying on from `resume` when the frame was cut short inside them before.
fn items<T>(
    c: &mut Cursor,
    resume: &mut Option<Items<T>>,
    mut item: impl FnMut(&mut Cursor) -> Result<T, Cut>,
) -> Result<Vec<T>, Cut> {
    let mut list = match resume.take() {
        Some(list) => {
            c.i = list.at;
            list
        }
        None => Items {
            read: Vec::new(),
            left: c.varint()?,
            at: c.i,
        },
    };
    while list.left > 0 {
        list.at = c.i;
        match item(c) {
            Ok(one) => list.read.push(one),
            Err(Cut::Short) => {
                *resume = Some(list);
                return Err(Cut::Short);
            }
            Err(bad) => return Err(bad),
        }
        list.left -= 1;
    }
    Ok(list.read)
}

/// Why a frame could not be read: the bytes pushed end inside it, or it is not a frame this decoder knows.
enum Cut {
    Short,
    Bad(String),
}

struct Cursor<'a> {
    bytes: &'a [u8],
    i: usize,
    /// When cut short, how many bytes would have let it read on.
    need: usize,
    max: usize,
}

impl<'a> Cursor<'a> {
    fn byte(&mut self) -> Result<u8, Cut> {
        Ok(self.bytes(1)?[0])
    }

    fn varint(&mut self) -> Result<u64, Cut> {
        let mut n = 0u64;
        for shift in (0..64).step_by(7) {
            let b = self.byte()?;
            if shift == 63 && b > 1 {
                break;
            }
            n |= u64::from(b & 0x7f) << shift;
            if b < 0x80 {
                return Ok(n);
            }
        }
        Err(Cut::Bad("a number longer than 64 bits".to_string()))
    }

    /// A length or offset within a frame, which no frame larger than `max` may hold.
    fn length(&mut self) -> Result<usize, Cut> {
        let n = self.varint()?;
        match usize::try_from(n) {
            Ok(n) if n <= self.max => Ok(n),
            _ => Err(Cut::Bad(format!(
                "a length of {n}, over the {} this receiver takes",
                self.max
            ))),
        }
    }

    /// Refuses a frame as soon as it runs past `max`, so a long frame costs no more than `max` to read.
    fn bytes(&mut self, n: usize) -> Result<&'a [u8], Cut> {
        if self.i + n > self.max {
            return Err(Cut::Bad(format!(
                "a frame longer than the {} this receiver takes",
                self.max
            )));
        }
        let Some(bytes) = self.bytes.get(self.i..self.i + n) else {
            self.need = self.i + n;
            return Err(Cut::Short);
        };
        self.i += n;
        Ok(bytes)
    }

    fn string(&mut self) -> Result<&'a [u8], Cut> {
        let n = self.length()?;
        self.bytes(n)
    }

    /// A channel's string: its length plus one, and 0 for none.
    fn plus_one(&mut self) -> Result<Option<Vec<u8>>, Cut> {
        match self.length()? {
            0 => Ok(None),
            n => Ok(Some(self.bytes(n - 1)?.to_vec())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn all(bytes: &[u8]) -> Vec<Event> {
        let mut decoder = Decoder::new(1 << 20);
        decoder.push(bytes);
        let events: Vec<Event> = std::iter::from_fn(|| decoder.next_event().unwrap()).collect();
        assert_eq!(decoder.held(), 0, "the stream ends on a frame's end");
        events
    }

    #[test]
    fn a_frame_of_many_fields_decodes_the_same_a_byte_at_a_time() {
        let fields = 2000;
        let mut frame = vec![CHANNEL, 1, 1, 1, 1, 1, 0, EVENT, 1, 0x01];
        put_varint(fields, &mut frame);
        for i in 0..fields {
            frame.extend([0x00, 1, b'k', 0x80 | (i as u8 & 0x7f), 0x01]);
        }
        frame.extend([3, b'r', b'a', b'w']);
        let whole = all(&frame).remove(0);
        let mut decoder = Decoder::new(1 << 20);
        let mut events = Vec::new();
        for byte in frame.chunks(1) {
            decoder.push(byte);
            events.extend(decoder.next_event().unwrap());
        }
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].fields, whole.fields);
        assert_eq!(events[0].fields.len(), fields as usize);
        assert_eq!(events[0].raw, b"raw");
    }

    #[test]
    fn inflating_stops_at_its_limit_however_well_the_input_compresses() {
        let mut zlib = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::best());
        std::io::Write::write_all(&mut zlib, &vec![b'a'; 1 << 20]).unwrap();
        let mut inflate = Inflate::default();
        inflate.push(&zlib.finish().unwrap());
        let mut out = Vec::new();
        assert_eq!(inflate.inflate(&mut out, 4096).unwrap(), 4096);
        assert_eq!(out.len(), 4096);
        let mut total = out.len();
        loop {
            out.clear();
            match inflate.inflate(&mut out, 4096).unwrap() {
                0 => break,
                n => total += n,
            }
        }
        assert_eq!(total, 1 << 20);
    }

    #[test]
    fn bytes_after_a_compressed_stream_ends_are_refused() {
        let mut zlib = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
        std::io::Write::write_all(&mut zlib, b"abc").unwrap();
        let mut stream = zlib.finish().unwrap();
        stream.push(0);
        let mut inflate = Inflate::default();
        inflate.push(&stream);
        let mut out = Vec::new();
        let refused = loop {
            match inflate.inflate(&mut out, 100) {
                Ok(0) => panic!("the byte after the end was taken"),
                Ok(_) => continue,
                Err(e) => break e,
            }
        };
        assert_eq!(out, b"abc");
        assert!(refused.what.contains("after"), "{refused}");
    }

    #[test]
    fn an_empty_done_with_bit_10_set_is_read() {
        // Flags 0x600, as a forwarder sent them closing its own audit log's stream.
        let mut decoder = Decoder::new(1 << 10);
        decoder.push(&[CHANNEL, 1, 1, 1, 1, 1, 0, EVENT, 1, 0x80, 0x0c, 0]);
        let event = decoder.next_event().unwrap().unwrap();
        assert!(event.done && event.raw.is_empty());
    }

    #[test]
    fn an_event_sent_again_with_bit_11_set_is_read() {
        // Flags 0x804: an id, and bit 11, which a forwarder sets beside _skipAcks on what it sends again.
        let mut decoder = Decoder::new(1 << 10);
        decoder.push(&[CHANNEL, 1, 1, 1, 1, 1, 0, EVENT, 1, 0x84, 0x10, 1, 1, b'x']);
        let event = decoder.next_event().unwrap().unwrap();
        assert_eq!((event.id, &event.raw[..]), (Some(1), &b"x"[..]));
    }

    #[test]
    fn what_this_decoder_does_not_know_is_refused_where_it_stands() {
        let refused = |bytes: &[u8]| {
            let mut decoder = Decoder::new(1 << 10);
            decoder.push(bytes);
            loop {
                match decoder.next_event() {
                    Ok(Some(_)) => continue,
                    Ok(None) => panic!("{bytes:02x?} was not refused"),
                    Err(e) => return e,
                }
            }
        };
        let channel = [CHANNEL, 1, 1, 1, 1, 1, 0];
        assert_eq!(refused(&[0x00]).at, 0);
        let unknown_flag = [&channel[..], &[EVENT, 1, 0x80, 0x01, 0]].concat();
        assert!(refused(&unknown_flag).what.contains("unknown flags 0x80"));
        assert_eq!(refused(&unknown_flag).at, 7);
        let unknown_field = [&channel[..], &[EVENT, 1, 0x01, 1, 0x20, 0]].concat();
        assert!(refused(&unknown_field).what.contains("unknown field type 0x20"));
        assert_eq!(refused(&unknown_field).at, 7);
        let undefined = [EVENT, 9, 0, 0];
        assert!(refused(&undefined).what.contains("never defined"));
        assert_eq!(refused(&[&channel[..], &undefined].concat()).at, 7);
        let past_raw = [
            &channel[..],
            &[EVENT, 1, 0x01, 1, 0x0f, 1, b'k', 2, 2, 3, b'a', b'b', b'c'],
        ]
        .concat();
        assert!(refused(&past_raw).what.contains("raw bytes 2..+2"));
        let too_long = [&channel[..], &[EVENT, 1, 0, 0xff, 0xff, 0x03]].concat();
        assert!(refused(&too_long).what.contains("over the 1024"));
        let many_fields: Vec<u8> = [&channel[..], &[EVENT, 1, 0x01, 0xff, 0x7f]]
            .concat()
            .into_iter()
            .chain(std::iter::repeat_n([0x04, 1, b'k', 1, b'v'], 300).flatten())
            .collect();
        assert!(refused(&many_fields).what.contains("a frame longer than the 1024"));
        let whole = [
            &many_fields[..channel.len() + 3],
            &[0xac, 0x02],
            &many_fields[channel.len() + 5..],
            &[0],
        ]
        .concat();
        assert!(refused(&whole).what.contains("a frame longer than the 1024"));
        let mut past_limit = Vec::new();
        for id in 1..=CHANNEL_LIMIT as u64 + 1 {
            past_limit.push(CHANNEL);
            put_varint(id, &mut past_limit);
            past_limit.extend([1, 1, 1, 1, 0]);
        }
        assert!(refused(&past_limit).what.contains("channel 301 past the 300"));
        assert!(
            refused(&[CHANNEL, 0x81, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x7e])
                .what
                .contains("64 bits")
        );
    }

    /// A v3 message's body, everything after its size field, built from the format: big-endian unless `little`.
    fn v3_body(pairs: &[(&str, &str)], little: bool) -> Vec<u8> {
        let int = |n: usize| match little {
            true => (n as u32).to_le_bytes(),
            false => (n as u32).to_be_bytes(),
        };
        let string = |text: &str, body: &mut Vec<u8>| {
            body.extend(int(text.len() + 1));
            body.extend(text.as_bytes());
            body.push(0);
        };
        let mut body = int(pairs.len()).to_vec();
        for (key, value) in pairs {
            string(key, &mut body);
            string(value, &mut body);
        }
        body.extend(int(0));
        string("_raw", &mut body);
        body
    }

    fn v3(pairs: &[(&str, &str)]) -> Vec<u8> {
        let body = v3_body(pairs, false);
        [&(body.len() as u32).to_be_bytes()[..], &body].concat()
    }

    fn compressed(body: &[u8]) -> Vec<u8> {
        let mut zlib = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
        std::io::Write::write_all(&mut zlib, body).unwrap();
        let mut zlib = zlib.finish().unwrap();
        zlib.push(0);
        let mut frame = (zlib.len() as u32 + 4).to_be_bytes().to_vec();
        frame.extend((zlib.len() as u32).to_be_bytes());
        frame.extend(zlib);
        frame
    }

    fn pairs_of(message: &Message) -> Vec<(String, String)> {
        let text = |b: &[u8]| String::from_utf8(b.to_vec()).unwrap();
        message.pairs.iter().map(|(k, v)| (text(k), text(v))).collect()
    }

    fn owned(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    #[test]
    fn a_signature_names_the_forwarder_and_any_other_opening_is_refused_at_its_first_wrong_byte() {
        let mut opening = vec![0; SIGNATURE_LEN];
        opening[..COOKED.len()].copy_from_slice(COOKED);
        opening[128..136].copy_from_slice(b"uf-plain");
        opening[384..388].copy_from_slice(b"8089");
        for held in [0, 1, 25, 128, 399] {
            assert_eq!(signature(&opening[..held]), Ok(None), "{held} bytes held");
        }
        opening.extend(v3(&[("__s2s_capabilities", "ack=0;compression=0")]));
        let signed = Signature {
            server: "uf-plain".to_string(),
            port: "8089".to_string(),
        };
        assert_eq!(signature(&opening), Ok(Some(signed)));
        assert!(signature(b"POST /services/collector").is_err());
        assert!(
            signature(b"-").is_ok(),
            "a first byte that may be the signature's waits for more"
        );
        assert!(signature(b"--splunk-cooked-mode-v2--").is_err());
        let mut longer = opening[..SIGNATURE_LEN].to_vec();
        longer[COOKED.len()] = b'x';
        assert!(
            signature(&longer).is_err(),
            "the first field is the cooked-mode name alone"
        );
    }

    #[test]
    fn a_v3_message_decodes_the_same_whole_and_a_byte_at_a_time() {
        let sent = [
            ("_raw", "line one\nline two\n"),
            ("MetaData:Host", "host::web01"),
            ("_done", "_done"),
            ("__s2s_eventId", "0"),
        ];
        let mut stream = v3(&sent);
        let len = stream.len();
        stream.extend([0xff, 0]);
        let (message, took) = read_message(&stream, 1 << 20).unwrap().unwrap();
        assert_eq!((pairs_of(&message), took), (owned(&sent), len));
        assert_eq!(message.get(b"_done"), Some(&b"_done"[..]));
        for held in 0..len {
            assert_eq!(read_message(&stream[..held], 1 << 20), Ok(None), "{held} bytes held");
        }
        let mut buffer = Vec::new();
        let mut read = None;
        for byte in &stream {
            buffer.push(*byte);
            if let Some(whole) = read_message(&buffer, 1 << 20).unwrap() {
                read = Some(whole);
                break;
            }
        }
        assert_eq!(read, Some((message, len)));
    }

    #[test]
    fn a_v3_message_past_its_limit_is_refused_once_its_size_is_held() {
        let message = v3(&[("_raw", "a chunk of a file")]);
        assert!(read_message(&message, message.len()).unwrap().is_some());
        let refused = read_message(&message[..4], message.len() - 1).unwrap_err();
        assert!(refused.what.contains("over the"), "{refused}");
        assert!(read_message(&[0xff, 0xff, 0xff, 0xff], 1 << 20).is_err());
    }

    #[test]
    fn a_v3_message_that_breaks_its_format_is_refused() {
        let refused = |body: &[u8]| {
            let message = [&(body.len() as u32).to_be_bytes()[..], body].concat();
            read_message(&message, 1 << 20).unwrap_err().what
        };
        let body = v3_body(&[("k", "v")], false);
        assert!(refused(&body[..body.len() - 1]).contains("past its size"));
        let mut other_trailer = body.clone();
        let at = other_trailer.len() - 2;
        other_trailer[at] = b'x';
        assert!(refused(&other_trailer).contains("trailer"));
        assert!(refused(&[&body[..], &[0]].concat()).contains("trailer"));
        let mut no_nul = body.clone();
        no_nul[9] = b'x';
        assert!(refused(&no_nul).contains("NUL"), "{}", refused(&no_nul));
        let mut past = body.clone();
        past[4..8].copy_from_slice(&u32::MAX.to_be_bytes());
        assert!(refused(&past).contains("past its size"));
        let mut many = body.clone();
        many[..4].copy_from_slice(&u32::MAX.to_be_bytes());
        // The pairs it claims run into the trailer, and then past the message.
        refused(&many);
    }

    #[test]
    fn a_compressed_v3_frame_inflates_to_its_little_endian_message() {
        let sent = [("__s2s_capabilities", "ack=1;compression=1")];
        let mut stream = compressed(&v3_body(&sent, true));
        let len = stream.len();
        stream.push(COMPRESSED);
        let (message, took) = read_compressed(&stream, 1 << 20).unwrap().unwrap();
        assert_eq!((pairs_of(&message), took), (owned(&sent), len));
        for held in 0..len {
            assert_eq!(read_compressed(&stream[..held], 1 << 20), Ok(None), "{held} bytes held");
        }
        let big_endian = compressed(&v3_body(&sent, false));
        assert!(read_compressed(&big_endian, 1 << 20).is_err());
        let mut wrong_zlen = stream[..len].to_vec();
        wrong_zlen[7] -= 1;
        assert!(read_compressed(&wrong_zlen, 1 << 20).is_err());
        for total in 0..4u32 {
            let short = [&total.to_be_bytes()[..], &[0; 4]].concat();
            assert!(read_compressed(&short, 1 << 20).is_err(), "a frame of total {total}");
        }
        let mut after_end = stream[..len].to_vec();
        after_end.push(0);
        after_end[3] += 1;
        after_end[7] += 1;
        assert!(read_compressed(&after_end, 1 << 20).is_err());
    }

    #[test]
    fn a_compressed_frame_that_inflates_past_its_limit_is_refused() {
        let bomb = v3_body(&[("_raw", &"a".repeat(1 << 20))], true);
        let frame = compressed(&bomb);
        assert!(frame.len() < 4096, "{} bytes compressed", frame.len());
        assert!(read_compressed(&frame, bomb.len() + 4).unwrap().is_some());
        let refused = read_compressed(&frame, 4096).unwrap_err();
        assert!(refused.what.contains("inflates past"), "{refused}");
    }

    #[test]
    fn the_capabilities_answer_is_what_an_indexer_answers() {
        let mut out = Vec::new();
        write_capabilities_answer(&mut out);
        let answer = "cap_response=success;cap_flush_key=true;idx_can_send_hb=true;idx_can_recv_token=true;\
                      request_certificate=true;v4=true;channel_limit=300;pl=7";
        assert_eq!(out, v3(&[("__s2s_control_msg", answer)]));
        let (message, took) = read_message(&out, 1 << 20).unwrap().unwrap();
        assert_eq!(took, out.len());
        assert_eq!(pairs_of(&message), owned(&[("__s2s_control_msg", answer)]));
    }

    #[test]
    fn a_written_message_reads_back_equal() {
        let sent = Message {
            pairs: vec![
                (b"__s2s_eventId".to_vec(), b"3-3".to_vec()),
                (b"_raw".to_vec(), vec![0xe2, 0x82]),
                (Vec::new(), Vec::new()),
            ],
        };
        let pairs: Vec<(&[u8], &[u8])> = sent.pairs.iter().map(|(k, v)| (&k[..], &v[..])).collect();
        let mut out = vec![0xaa];
        write_message(&pairs, &mut out);
        assert_eq!(read_message(&out[1..], 1 << 20), Ok(Some((sent, out.len() - 1))));
    }

    #[test]
    fn capabilities_say_whether_the_forwarder_asks_for_acks_and_compression() {
        let asks = |text: &str| {
            let message = Message {
                pairs: vec![(b"__s2s_capabilities".to_vec(), text.as_bytes().to_vec())],
            };
            capabilities(&message)
        };
        let both = Capabilities {
            ack: true,
            compression: true,
        };
        assert_eq!(asks("ack=0;compression=0"), Ok(Capabilities::default()));
        assert_eq!(asks("ack=1;compression=1"), Ok(both));
        assert_eq!(
            asks("cli_can_rcv_hb=1;compression=1;pl=7;request_certificate=1;v4=1"),
            Ok(Capabilities {
                ack: false,
                compression: true
            })
        );
        assert!(asks("ack=2;compression=0").is_err());
        assert!(asks("ack=0;compression=yes").is_err());
        assert!(asks("ack").is_err());
        assert!(capabilities(&Message::default()).is_err());
    }

    #[test]
    fn v4_starts_at_a_byte_no_v3_message_opens_with() {
        assert_eq!(format(0x00), Format::V3);
        assert_eq!(format(0xf8), Format::V3);
        assert_eq!(format(COMPRESSED), Format::CompressedV4);
        assert_eq!(format(TIMEZONE), Format::V4);
        assert_eq!(v3(&[("_raw", "x")])[0], 0);
    }

    #[test]
    fn an_ack_names_one_id_or_a_range() {
        let mut out = Vec::new();
        ack(5, 5, &mut out);
        ack(10, 31, &mut out);
        ack(200, 300, &mut out);
        assert_eq!(out, [0xfb, 5, 0xfa, 10, 31, 0xfa, 0xc8, 0x01, 0xac, 0x02]);
    }
}
