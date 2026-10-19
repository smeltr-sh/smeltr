//! What a HEC receiver reads: Splunk's HTTP Event Collector format, as its clients send it.
//!
//! The event endpoint takes a body of envelopes one after another, `{"event": ..., "time": ..., "host": ...}`, with
//! or without whitespace between them. Each envelope is one record: its `event`, a string as its text and anything
//! else as its JSON. What the envelope files the event under rides with the record as its `__source` fields. The raw
//! endpoint takes a record per line, each filed under what the request's query names.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::format::json_scan::{Malformed, Next, Reader};

/// Where a HEC receiver answers whether it takes requests, as Splunk does.
pub const HEALTH_PATH: &str = "/services/collector/health";

/// What an event is filed under, each by the `__source` field it is read as: `SourceRecord::headers` carries them.
/// `host`, `source`, `sourcetype` and `index` are text; `time` is microseconds since the epoch as decimal text; and
/// `fields` is in `core::variant`'s encoding.
pub type Filed = Vec<(String, Vec<u8>)>;

/// The keys of [`Filed`] that are text, as the envelope and the raw endpoint's query name them.
pub const NAMED: [&str; 4] = ["host", "source", "sourcetype", "index"];
pub const TIME: &str = "time";
pub const FIELDS: &str = "fields";

/// Why HEC refuses a body: its code, its text, and the envelope it refuses, counted from 0.
#[derive(Debug, PartialEq)]
pub struct Invalid {
    pub code: u16,
    pub text: &'static str,
    pub event: Option<usize>,
}

impl Invalid {
    pub const NO_DATA: Invalid = Invalid {
        code: 5,
        text: "No data",
        event: None,
    };

    pub const NO_CHANNEL: Invalid = Invalid {
        code: 10,
        text: "Data channel is missing",
        event: None,
    };

    fn at(code: u16, text: &'static str, event: usize) -> Invalid {
        Invalid {
            code,
            text,
            event: Some(event),
        }
    }

    fn format(event: usize) -> Invalid {
        Invalid::at(6, "Invalid data format", event)
    }
}

/// The envelopes of an event endpoint's body, cut one at a time as they are taken.
#[derive(Debug)]
pub struct Events {
    body: String,
    /// Where the next envelope starts.
    at: usize,
    /// The number of the next envelope.
    n: usize,
}

impl Events {
    /// `body`'s envelopes, or why HEC refuses it. Every envelope is read here, so none of a refused body is taken.
    pub fn new(body: Vec<u8>) -> Result<Events, Invalid> {
        let body = match String::from_utf8(body) {
            Ok(body) => body,
            // The envelopes before the first byte that is no UTF-8 read; the one holding it does not.
            Err(e) => {
                let valid = e.utf8_error().valid_up_to();
                let text = std::str::from_utf8(&e.as_bytes()[..valid]).expect("UTF-8 up to here");
                return Err(Invalid::format(read_all(text)?));
            }
        };
        match read_all(&body)? {
            0 => Err(Invalid::NO_DATA),
            _ => Ok(Events { body, at: 0, n: 0 }),
        }
    }

    /// The next envelope's record and what it is filed under.
    pub fn next_record(&mut self) -> Option<(Vec<u8>, Filed)> {
        let mut r = Reader::of_str(&self.body[self.at..]);
        let next = cut(&mut r, self.n)?;
        self.at += r.at();
        self.n += 1;
        Some(next.expect("every envelope was read when the request was"))
    }
}

/// How many envelopes `text` holds, each read, or why HEC refuses the first it cannot take.
fn read_all(text: &str) -> Result<usize, Invalid> {
    let mut r = Reader::of_str(text);
    let mut n = 0;
    while let Some(envelope) = cut(&mut r, n) {
        envelope?;
        n += 1;
    }
    Ok(n)
}

/// The envelope `r` is at, numbered `n`. `None` once only whitespace is left.
fn cut(r: &mut Reader, n: usize) -> Option<Result<(Vec<u8>, Filed), Invalid>> {
    (!r.end()).then(|| envelope(r, n))
}

/// An envelope's members, each as the document writes it. The whole envelope is read before any member is judged,
/// so a body that is no JSON is refused as such whatever it names.
fn envelope(r: &mut Reader, n: usize) -> Result<(Vec<u8>, Filed), Invalid> {
    let bad = |_: Malformed| Invalid::format(n);
    if r.value().map_err(bad)? != Next::Object {
        return Err(Invalid::format(n));
    }
    let (mut event, mut time, mut fields) = (None, None, None);
    let mut named: [Option<&str>; 4] = [None; 4];
    let mut seen = 0u8;
    while let Some(key) = r.key().map_err(bad)? {
        let member = match &*key {
            "event" => 0,
            "time" => 1,
            "fields" => 2,
            name => match NAMED.iter().position(|n| *n == name) {
                Some(i) => 3 + i,
                None => {
                    r.skip().map_err(bad)?;
                    continue;
                }
            },
        };
        // HEC takes each member once.
        if seen & 1 << member != 0 {
            return Err(Invalid::format(n));
        }
        seen |= 1 << member;
        match member {
            0 => event = Some(raw(r).map_err(bad)?),
            1 => time = Some(raw(r).map_err(bad)?),
            2 => {
                let mut encoded = Vec::new();
                r.variant(&mut encoded).map_err(bad)?;
                fields = Some(encoded);
            }
            i => named[i - 3] = Some(raw(r).map_err(bad)?),
        }
    }
    let event = event
        .filter(|e| *e != "null")
        .ok_or_else(|| Invalid::at(12, "Event field is required", n))?;
    let record = match Reader::of_str(event).value() {
        Ok(Next::Str(text)) => text.into_owned().into_bytes(),
        _ => event.as_bytes().to_vec(),
    };
    if record.is_empty() {
        return Err(Invalid::at(13, "Event field cannot be blank", n));
    }
    let mut filed = Filed::new();
    for (key, value) in NAMED.into_iter().zip(named) {
        match value.map(|v| Reader::of_str(v).value()) {
            None | Some(Ok(Next::Null)) => {}
            Some(Ok(value @ (Next::Str(_) | Next::Number(_)))) => {
                let text = super::splunk::text(value).ok_or_else(|| Invalid::format(n))?;
                filed.push((key.to_string(), text.into_bytes()));
            }
            Some(_) => return Err(Invalid::format(n)),
        }
    }
    if let Some(time) = time.filter(|t| *t != "null") {
        let micros = epoch_micros(time).ok_or_else(|| Invalid::format(n))?;
        filed.push((TIME.to_string(), micros.to_string().into_bytes()));
    }
    match fields {
        Some(fields) if crate::core::variant::is_object(&fields) => filed.push((FIELDS.to_string(), fields)),
        Some(fields) if fields[0] != crate::core::variant::NULL => {
            return Err(Invalid::at(15, "Error in handling indexed fields", n));
        }
        _ => {}
    }
    Ok((record, filed))
}

/// The value `r` is at, as the document writes it.
fn raw<'d>(r: &mut Reader<'d>) -> Result<&'d str, Malformed> {
    Ok(std::str::from_utf8(r.raw()?).expect("a value of a document read as text"))
}

/// HEC's `time`, Unix seconds as a number or as text, in microseconds.
fn epoch_micros(time: &str) -> Option<i64> {
    let seconds: f64 = time.trim_matches('"').trim().parse().ok()?;
    let micros = (seconds * 1e6).round();
    (micros.is_finite() && micros.abs() < i64::MAX as f64).then_some(micros as i64)
}

/// What the raw endpoint's query files every line of a request under. A `time` that is not Unix seconds files none.
pub fn raw_filed(query: &HashMap<String, String>) -> Filed {
    let mut filed: Filed = NAMED
        .into_iter()
        .filter_map(|key| Some((key.to_string(), query.get(key)?.clone().into_bytes())))
        .filter(|(_, value)| !value.is_empty())
        .collect();
    if let Some(micros) = query.get(TIME).and_then(|t| epoch_micros(t)) {
        filed.push((TIME.to_string(), micros.to_string().into_bytes()));
    }
    filed
}

/// The most channels a receiver keeps `ackId`s for. A request on a new channel past it first forgets the channels idle
/// past [`CHANNEL_IDLE`], and is answered busy if that leaves no room.
pub const MAX_CHANNELS: usize = 10_000;

/// How long a channel nobody sends on or asks after is kept.
pub const CHANNEL_IDLE: Duration = Duration::from_secs(10 * 60);

/// How often a receiver with no room for a new channel looks for idle ones to forget. Looking reads every channel,
/// under the lock the reader takes too.
const SWEEP_EVERY: Duration = Duration::from_secs(1);

/// The most delivered `ackId`s a receiver keeps, across its channels, for senders to ask after. Past it the channel
/// holding the most forgets its oldest, so a sender that never asks costs only itself, and a sender asking after one
/// forgotten hears `false` and sends its request again: a duplicate, never a loss.
pub const MAX_DELIVERED: usize = 1_000_000;

/// The `ackId`s a receiver has given, by channel. Each is pending until its request is delivered, then delivered until
/// its sender asks after it. One no channel here knows is never delivered: it was given by another worker, or by this
/// one before it restarted, and its sender sends it again. A channel's IDs start at a random number, so an ID another
/// worker, or an earlier life of this channel, gave is not mistaken for one of this channel's.
#[derive(Default)]
pub struct Acks {
    channels: HashMap<Arc<str>, Channel>,
    /// Delivered IDs, across every channel.
    delivered: usize,
    /// The channel holding the most delivered IDs, or one that did when it was last looked for.
    fullest: Option<Arc<str>>,
    swept: Option<Instant>,
}

struct Channel {
    next: u64,
    delivered: BTreeSet<u64>,
    used: Instant,
}

impl Acks {
    /// A new `ackId` on `channel`, or `None` when this receiver keeps as many channels as it may, or cannot draw a
    /// channel's first ID.
    pub fn give(&mut self, channel: &Arc<str>, now: Instant) -> Option<u64> {
        if !self.channels.contains_key(channel) && self.channels.len() >= MAX_CHANNELS {
            if self.swept.is_some_and(|at| now.duration_since(at) < SWEEP_EVERY) {
                return None;
            }
            self.swept = Some(now);
            let mut forgotten = 0;
            self.channels.retain(|_, c| {
                let idle = now.duration_since(c.used) >= CHANNEL_IDLE;
                if idle {
                    forgotten += c.delivered.len();
                }
                !idle
            });
            self.delivered -= forgotten;
            if self.channels.len() >= MAX_CHANNELS {
                return None;
            }
        }
        let open = match self.channels.entry(channel.clone()) {
            std::collections::hash_map::Entry::Occupied(open) => open.into_mut(),
            std::collections::hash_map::Entry::Vacant(new) => new.insert(Channel {
                next: first_id()?,
                delivered: BTreeSet::new(),
                used: now,
            }),
        };
        open.used = now;
        open.next += 1;
        Some(open.next - 1)
    }

    pub fn deliver(&mut self, channel: &str, id: u64) {
        let Some((name, _)) = self.channels.get_key_value(channel) else {
            return;
        };
        let name = name.clone();
        if self.delivered >= MAX_DELIVERED && self.forget_oldest_of_fullest() {
            self.delivered -= 1;
        }
        let Some(open) = self.channels.get_mut(&name) else {
            return;
        };
        if open.delivered.insert(id) {
            self.delivered += 1;
        }
        let held = open.delivered.len();
        let fuller = (self.fullest.as_ref())
            .and_then(|f| self.channels.get(f))
            .is_none_or(|f| f.delivered.len() < held);
        if fuller {
            self.fullest = Some(name);
        }
    }

    /// Forgets the oldest delivered ID of the channel holding the most. Every channel is looked through only when the
    /// one last known fullest has none left, so a delivery at the cap is not a walk of every channel.
    fn forget_oldest_of_fullest(&mut self) -> bool {
        let known = (self.fullest.as_ref())
            .and_then(|f| self.channels.get(f))
            .is_some_and(|f| !f.delivered.is_empty());
        if !known {
            self.fullest = (self.channels.iter())
                .max_by_key(|(_, c)| c.delivered.len())
                .map(|(name, _)| name.clone());
        }
        (self.fullest.as_ref())
            .and_then(|f| self.channels.get_mut(f))
            .is_some_and(|f| f.delivered.pop_first().is_some())
    }

    /// Whether each of `ids` on `channel` is delivered. One that is, is forgotten once asked after.
    pub fn ask(&mut self, channel: &str, ids: &[u64], now: Instant) -> Vec<(u64, bool)> {
        let mut open = self.channels.get_mut(channel);
        if let Some(open) = open.as_deref_mut() {
            open.used = now;
        }
        // An ID asked after twice in one ask is answered once, so its second asking cannot turn its `true` false.
        let asked: Vec<(u64, bool)> = (ids.iter().copied().collect::<BTreeSet<u64>>())
            .into_iter()
            .map(|id| (id, open.as_deref_mut().is_some_and(|c| c.delivered.remove(&id))))
            .collect();
        self.delivered -= asked.iter().filter(|(_, done)| *done).count();
        asked
    }
}

/// A random first `ackId` below 2^52, so a channel's IDs stay exact in a sender that reads JSON numbers as doubles.
/// `None` when the system's randomness fails: starting at a known number could mistake another life's IDs for this
/// one's.
fn first_id() -> Option<u64> {
    use aws_lc_rs::rand::SecureRandom;
    let mut bytes = [0u8; 8];
    aws_lc_rs::rand::SystemRandom::new().fill(&mut bytes).ok()?;
    Some(u64::from_le_bytes(bytes) >> 12)
}

/// The channel a request names in `X-Splunk-Request-Channel` or its `channel` query, or why HEC refuses it.
pub fn channel(header: Option<&[u8]>, query: Option<&str>) -> Result<Option<Arc<str>>, Invalid> {
    match header.or(query.map(str::as_bytes)).map(<[u8]>::trim_ascii) {
        None => Ok(None),
        Some(named)
            if !named.is_empty()
                && named.len() <= 128
                && named
                    .iter()
                    .all(|b| b.is_ascii_alphanumeric() || *b == b'-' || *b == b'_') =>
        {
            Ok(Some(Arc::from(String::from_utf8_lossy(named))))
        }
        Some(_) => Err(Invalid {
            code: 11,
            text: "Invalid data channel",
            event: None,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn all(body: &str) -> Result<Vec<(String, Filed)>, Invalid> {
        let mut events = Events::new(body.as_bytes().to_vec())?;
        Ok(std::iter::from_fn(|| events.next_record())
            .map(|(record, filed)| (String::from_utf8(record).unwrap(), filed))
            .collect())
    }

    fn text(filed: &Filed, key: &str) -> Option<String> {
        filed
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| String::from_utf8(v.clone()).unwrap())
    }

    #[test]
    fn an_envelope_is_its_event_filed_under_what_it_names() {
        let read = all(
            r#"{"event":"one","host":"h","sourcetype":"st","index":"main","time":1700000000.25}
            {"event":{"a":1},"source":"s","time":"1700000001"}{"event":2}"#,
        )
        .unwrap();
        assert_eq!(
            read.iter().map(|(r, _)| r.as_str()).collect::<Vec<_>>(),
            ["one", r#"{"a":1}"#, "2"]
        );
        let (_, first) = &read[0];
        assert_eq!(text(first, "host").as_deref(), Some("h"));
        assert_eq!(text(first, "sourcetype").as_deref(), Some("st"));
        assert_eq!(text(first, "index").as_deref(), Some("main"));
        assert_eq!(text(first, TIME).as_deref(), Some("1700000000250000"));
        assert_eq!(text(&read[1].1, TIME).as_deref(), Some("1700000001000000"));
        assert_eq!(text(&read[1].1, "source").as_deref(), Some("s"));
        assert!(read[2].1.is_empty());
    }

    #[test]
    fn indexed_fields_are_carried_as_a_variant() {
        let read = all(r#"{"event":"e","fields":{"env":"prod","tags":["a","b"]}}"#).unwrap();
        let (_, encoded) = read[0].1.iter().find(|(k, _)| k == FIELDS).unwrap();
        let env = crate::core::variant::member(encoded, "env").unwrap();
        assert!(matches!(
            crate::core::variant::read(env),
            crate::core::variant::Value::Str("prod")
        ));
    }

    #[test]
    fn a_body_is_refused_whole_naming_the_envelope_at_fault() {
        let refused = |body: &str| all(body).unwrap_err();
        assert_eq!(refused(""), Invalid::NO_DATA);
        assert_eq!(refused("  \n"), Invalid::NO_DATA);
        assert_eq!(refused(r#"{"event":"a"}{"host":"h"}"#).code, 12);
        assert_eq!(refused(r#"{"event":"a"}{"host":"h"}"#).event, Some(1));
        assert_eq!(refused(r#"{"event":null}"#).code, 12);
        assert_eq!(refused(r#"{"event":""}"#).code, 13);
        assert_eq!(refused(r#"{"event":"a"} not json"#), Invalid::format(1));
        assert_eq!(refused(r#"[{"event":"a"}]"#).code, 6);
        assert_eq!(refused(r#"{"event":"a","time":"noon"}"#).code, 6);
        assert_eq!(refused(r#"{"event":"a","host":{"x":1}}"#).code, 6);
        assert_eq!(refused(r#"{"event":"a","fields":[1]}"#).code, 15);
        assert_eq!(
            refused(r#"{"event":"a","event":"b"}"#),
            Invalid::format(0),
            "each member once"
        );
        assert_eq!(
            refused(r#"{"event":"a","fields":[1}"#),
            Invalid::format(0),
            "no JSON before what it names"
        );
        let mut body = br#"{"event":"a"} {"event":""#.to_vec();
        body.extend_from_slice(b"\xff\"}");
        assert_eq!(
            Events::new(body).unwrap_err(),
            Invalid::format(1),
            "the envelope holding what is no UTF-8"
        );
    }

    #[test]
    fn an_envelope_reads_its_event_and_names_as_json_does() {
        let got = all(r#"{"event":"a\"b","host":7,"source":1.5,"x":[1,{"y":2}]} {"event":{"k":[1, 2]}}"#).unwrap();
        assert_eq!(got[0].0, r#"a"b"#);
        assert_eq!(text(&got[0].1, "host").as_deref(), Some("7"));
        assert_eq!(text(&got[0].1, "source").as_deref(), Some("1.5"));
        assert_eq!(
            got[1].0, r#"{"k":[1, 2]}"#,
            "an event that is no string is its JSON as sent"
        );
    }

    #[test]
    fn an_ack_is_true_once_delivered_and_then_forgotten() {
        let (mut acks, now) = (Acks::default(), Instant::now());
        let c: Arc<str> = Arc::from("c1");
        let first = acks.give(&c, now).unwrap();
        assert_eq!(acks.give(&c, now), Some(first + 1));
        acks.deliver("c1", first + 1);
        assert_eq!(
            acks.ask("c1", &[first, first + 1], now),
            [(first, false), (first + 1, true)]
        );
        assert_eq!(
            acks.ask("c1", &[first + 1], now),
            [(first + 1, false)],
            "it is forgotten once asked after"
        );
        assert_eq!(
            acks.ask("other", &[first], now),
            [(first, false)],
            "another worker's channel is never delivered"
        );
        assert_ne!(
            Acks::default().give(&c, now),
            Some(first),
            "a channel's next life starts its IDs elsewhere"
        );
    }

    #[test]
    fn past_the_cap_the_channel_holding_most_forgets_its_oldest() {
        let (mut acks, now) = (Acks::default(), Instant::now());
        let (silent, asking): (Arc<str>, Arc<str>) = (Arc::from("silent"), Arc::from("asking"));
        let first = acks.give(&silent, now).unwrap();
        for id in first..first + MAX_DELIVERED as u64 {
            acks.deliver("silent", id);
        }
        let mine = acks.give(&asking, now).unwrap();
        acks.deliver("asking", mine);
        assert_eq!(acks.delivered, MAX_DELIVERED);
        assert_eq!(
            acks.ask("asking", &[mine], now),
            [(mine, true)],
            "a sender that never asks does not crowd out one that does"
        );
        assert_eq!(
            acks.ask("silent", &[first, first + 1], now),
            [(first, false), (first + 1, true)]
        );
        assert_eq!(acks.delivered, MAX_DELIVERED - 2);
    }

    #[test]
    fn an_id_asked_after_twice_in_one_ask_reads_true_once() {
        let (mut acks, now) = (Acks::default(), Instant::now());
        let c: Arc<str> = Arc::from("c");
        let id = acks.give(&c, now).unwrap();
        acks.deliver("c", id);
        assert_eq!(acks.ask("c", &[id, id], now), [(id, true)]);
        assert_eq!(acks.delivered, 0);
    }

    #[test]
    fn a_full_table_looks_for_idle_channels_at_most_once_a_second() {
        let (mut acks, now) = (Acks::default(), Instant::now());
        for i in 0..MAX_CHANNELS {
            acks.give(&Arc::from(i.to_string()), now).unwrap();
        }
        let late: Arc<str> = Arc::from("late");
        let looked = now + CHANNEL_IDLE - Duration::from_millis(500);
        assert_eq!(acks.give(&late, looked), None, "none is idle yet");
        assert_eq!(acks.give(&late, now + CHANNEL_IDLE), None, "too soon to look again");
        assert!(acks.give(&late, looked + SWEEP_EVERY).is_some());
    }

    #[test]
    fn a_channel_past_the_last_is_refused_until_one_idles_out() {
        let (mut acks, now) = (Acks::default(), Instant::now());
        for i in 0..MAX_CHANNELS {
            assert!(acks.give(&Arc::from(i.to_string()), now).is_some());
        }
        let late: Arc<str> = Arc::from("late");
        assert_eq!(acks.give(&late, now), None);
        assert!(acks.give(&late, now + CHANNEL_IDLE).is_some());
    }

    #[test]
    fn a_channel_is_named_by_its_header_or_query() {
        assert_eq!(channel(None, None), Ok(None));
        assert_eq!(channel(Some(b"abc-1"), Some("x")), Ok(Some(Arc::from("abc-1"))));
        assert_eq!(channel(None, Some("q")), Ok(Some(Arc::from("q"))));
        assert_eq!(channel(Some(b"no spaces"), None).unwrap_err().code, 11);
        assert_eq!(channel(Some(b""), None).unwrap_err().code, 11);
        assert_eq!(channel(Some("caf\u{e9}".as_bytes()), None).unwrap_err().code, 11);
    }

    #[test]
    fn the_raw_query_files_every_line() {
        let query = HashMap::from([
            ("sourcetype".to_string(), "syslog".to_string()),
            ("channel".to_string(), "c".to_string()),
            ("host".to_string(), String::new()),
            ("time".to_string(), "1700000000".to_string()),
        ]);
        assert_eq!(
            raw_filed(&query),
            vec![
                ("sourcetype".to_string(), b"syslog".to_vec()),
                ("time".to_string(), b"1700000000000000".to_vec())
            ]
        );
    }
}
