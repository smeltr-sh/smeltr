//! The object source's core (`io::object_source`) over `object_store`'s in-memory store and a queue held in memory:
//! how objects become records, and when a message is extended and deleted.

use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use smeltr::io::line_breaker::LineBreaker;
use smeltr::io::object_source::{Message, ObjectRef, ObjectSource, Queue, Unread};
use smeltr::io::{CommitMap, IoError, PartitionLag, Source, SourceRecord, TopicPartition};
use smeltr::plan::topology::Framing;

#[derive(Default)]
struct Calls {
    inbox: Vec<Message>,
    receives: usize,
    /// How long a receive takes to answer.
    delay: Duration,
    extended: Vec<String>,
    /// Extended by zero: shown again at once.
    shown: Vec<String>,
    deleted: Vec<String>,
    /// What a depth call answers, or none for a queue that cannot count.
    depth: Option<Result<i64, String>>,
}

/// Hands out what is in its inbox once, and remembers every extend and delete.
#[derive(Clone, Default)]
struct FakeQueue(Arc<Mutex<Calls>>);

impl Queue for FakeQueue {
    fn name(&self) -> &str {
        "fake"
    }

    fn receive(&self, max: usize, _: Duration, _: Duration) -> Result<Vec<Message>, IoError> {
        let delay = self.0.lock().unwrap().delay;
        std::thread::sleep(delay);
        let mut calls = self.0.lock().unwrap();
        calls.receives += 1;
        let n = calls.inbox.len().min(max);
        Ok(calls.inbox.drain(..n).collect())
    }

    fn extend(&self, handles: &[String], visibility: Duration) -> Vec<(String, String)> {
        let mut calls = self.0.lock().unwrap();
        let to = if visibility.is_zero() {
            &mut calls.shown
        } else {
            &mut calls.extended
        };
        to.extend_from_slice(handles);
        Vec::new()
    }

    fn delete(&self, handles: &[String]) -> Vec<(String, String)> {
        self.0.lock().unwrap().deleted.extend_from_slice(handles);
        Vec::new()
    }

    fn depth(&self) -> Option<Result<PartitionLag, Unread>> {
        let depth = self.0.lock().unwrap().depth.clone()?;
        Some(depth.map_err(Unread::Failed).map(|lag| PartitionLag {
            partition: TopicPartition::new(Arc::from("orders"), 0),
            lag,
        }))
    }
}

struct Setup {
    queue: FakeQueue,
    store: Arc<InMemory>,
    runtime: Arc<tokio::runtime::Runtime>,
}

impl Setup {
    fn new() -> Setup {
        Setup {
            queue: FakeQueue::default(),
            store: Arc::new(InMemory::new()),
            runtime: smeltr::io::objects::runtime().unwrap(),
        }
    }

    fn put(&self, key: &str, body: &[u8]) {
        let path = Path::parse(key).unwrap();
        let put = self.store.put(&path, PutPayload::from(body.to_vec()));
        self.runtime.block_on(put).unwrap();
    }

    /// A message `handle` announcing `keys` in bucket `logs`.
    fn announce(&self, handle: &str, keys: &[&str]) {
        let objects = keys
            .iter()
            .map(|k| ObjectRef {
                bucket: "logs".to_string(),
                key: k.to_string(),
                version: None,
            })
            .collect();
        self.queue.0.lock().unwrap().inbox.push(Message {
            handle: handle.to_string(),
            objects: Ok(objects),
        });
    }

    fn source(&self, framing: Framing, visibility: Duration) -> ObjectSource {
        self.source_up_to(framing, visibility, smeltr::io::object_source::MAX_RECORD_BYTES)
    }

    fn source_up_to(&self, framing: Framing, visibility: Duration, max_record: u64) -> ObjectSource {
        let store: Arc<dyn ObjectStore> = self.store.clone();
        ObjectSource::new(
            Arc::new(self.queue.clone()),
            Box::new(move |_: &str| Ok(store.clone())),
            self.runtime.clone(),
            framing,
            visibility,
            max_record,
        )
        .unwrap()
    }

    fn deleted(&self) -> Vec<String> {
        self.queue.0.lock().unwrap().deleted.clone()
    }
}

const HOUR: Duration = Duration::from_secs(3600);
const POLL: Duration = Duration::from_millis(50);
/// Long enough for a loaded machine. A wait for something that comes ends when it comes.
const WAIT: Duration = Duration::from_secs(10);

/// What is announced, read. Waits for the receive, and ends once everything received is read.
fn poll(source: &mut ObjectSource) -> Vec<SourceRecord> {
    source.poll_batch(1000, 1 << 20, WAIT).unwrap()
}

/// One short poll, for a source that should hand out nothing: with nothing to read it waits out its whole timeout.
fn poll_briefly(source: &mut ObjectSource) -> Vec<SourceRecord> {
    source.poll_batch(1000, 1 << 20, POLL).unwrap()
}

/// What the queue was asked to extend, once it was asked: the source extends on a thread of its own, on its own clock.
fn extended(s: &Setup) -> Vec<String> {
    let deadline = Instant::now() + WAIT;
    loop {
        let extended = s.queue.0.lock().unwrap().extended.clone();
        if !extended.is_empty() || Instant::now() > deadline {
            return extended;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn raw(records: &[SourceRecord]) -> Vec<&str> {
    records
        .iter()
        .map(|r| std::str::from_utf8(r.bytes().unwrap()).unwrap())
        .collect()
}

/// The watermark the reader would commit for `records`.
fn commit(source: &mut ObjectSource, records: &[SourceRecord]) {
    let mut offsets = CommitMap::new();
    for r in records {
        let at = offsets
            .entry(TopicPartition::new(r.topic.clone(), r.partition))
            .or_insert(r.offset);
        *at = (*at).max(r.offset);
    }
    source.commit(&offsets).unwrap();
}

fn gzip(parts: &[&[u8]]) -> Vec<u8> {
    let mut out = Vec::new();
    for part in parts {
        let mut member = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        member.write_all(part).unwrap();
        out.extend(member.finish().unwrap());
    }
    out
}

#[test]
fn lines_become_records_whatever_their_compression() {
    let s = Setup::new();
    s.put("plain.log", b"one\r\ntwo\n\nlast");
    s.put("multi.gz", &gzip(&[b"one\r\ntwo\n", b"\nlast"]));
    s.put("frame.zst", &zstd::bulk::compress(b"one\r\ntwo\n\nlast", 3).unwrap());
    let mut lz4 = lz4_flex::frame::FrameEncoder::new(Vec::new());
    lz4.write_all(b"one\r\ntwo\n\nlast").unwrap();
    s.put("frame.lz4", &lz4.finish().unwrap());
    let mut snappy = snap::write::FrameEncoder::new(Vec::new());
    snappy.write_all(b"one\r\ntwo\n\nlast").unwrap();
    s.put("frame.sz", &snappy.into_inner().unwrap());
    s.put("empty.log", b"");
    for key in ["plain.log", "multi.gz", "frame.zst", "frame.lz4", "frame.sz"] {
        s.announce(key, &[key]);
        let records = poll(&mut s.source(Framing::Lines, HOUR));
        assert_eq!(raw(&records), ["one", "two", "", "last"], "{key}");
        assert!(
            records
                .iter()
                .all(|r| *r.topic == format!("logs/{key}") && r.headers.is_none())
        );
        assert_eq!(records.iter().map(|r| r.offset).collect::<Vec<_>>(), [0, 1, 2, 3]);
    }
    s.announce("empty", &["empty.log"]);
    let mut source = s.source(Framing::Lines, HOUR);
    // Read in one poll, and deleted by a later one.
    let deadline = Instant::now() + WAIT;
    while s.deleted().is_empty() && Instant::now() < deadline {
        assert!(poll_briefly(&mut source).is_empty());
    }
    assert_eq!(s.deleted(), ["empty"], "an empty object is done once read");
}

#[test]
fn an_object_framed_whole_is_one_record() {
    let s = Setup::new();
    s.put("trail.json.gz", &gzip(&[b"{\"Records\":[\n{},\n{}]}\n"]));
    s.announce("m", &["trail.json.gz"]);
    let mut source = s.source(Framing::Object, HOUR);
    let records = poll(&mut source);
    assert_eq!(raw(&records), ["{\"Records\":[\n{},\n{}]}\n"]);
    commit(&mut source, &records);
    assert_eq!(s.deleted(), ["m"]);
}

/// An escapedLines object's lines come back as the bytes a bytes destination escaped; a `\\` before any other byte stays.
#[test]
fn escaped_lines_are_unescaped() {
    let s = Setup::new();
    s.put("archive.raw", b"two\\nlines\r\nc:\\\\dir\\x\n");
    s.announce("m", &["archive.raw"]);
    let records = poll(&mut s.source(Framing::EscapedLines, HOUR));
    assert_eq!(raw(&records), ["two\nlines", "c:\\dir\\x"]);
}

#[test]
fn a_line_breaker_keeps_a_stack_trace_in_one_record() {
    let s = Setup::new();
    s.put(
        "app.log.gz",
        &gzip(&[
            b"2026-10-11 ERROR failed\n    at a.C.run(C.java:10)\r\n",
            b"    at a.D.main(D.java:3)\n2026-10-11 INFO ok",
        ]),
    );
    s.announce("m", &["app.log.gz"]);
    let mut source = s.source(broken(r"([\r\n]+)\d{4}-"), HOUR);
    let records = poll(&mut source);
    assert_eq!(
        raw(&records),
        [
            "2026-10-11 ERROR failed\n    at a.C.run(C.java:10)\r\n    at a.D.main(D.java:3)",
            "2026-10-11 INFO ok"
        ]
    );
    commit(&mut source, &records);
    assert_eq!(s.deleted(), ["m"], "the object's last event is its last record");
}

fn broken(line_breaker: &str) -> Framing {
    Framing::LineBreaker(LineBreaker(regex::bytes::Regex::new(line_breaker).unwrap()))
}

#[test]
fn a_line_breaker_cuts_an_object_where_its_group_matches() {
    let s = Setup::new();
    s.put("events.xml", b"<e>1</e><e>2</e>\n<e>3</e>");
    s.announce("m", &["events.xml"]);
    let mut source = s.source(broken(r"</e>(\s*)"), HOUR);
    let records = poll(&mut source);
    assert_eq!(raw(&records), ["<e>1</e>", "<e>2</e>", "<e>3</e>"]);
    commit(&mut source, &records);
    assert_eq!(s.deleted(), ["m"]);
}

#[test]
fn a_line_breaker_passes_over_an_event_of_only_whitespace() {
    let s = Setup::new();
    s.put("events.raw", b"a\x00 \t\x00\x00b\x00\n");
    s.announce("m", &["events.raw"]);
    let mut source = s.source(broken(r"(\x00)"), HOUR);
    let records = poll(&mut source);
    assert_eq!(raw(&records), ["a", "b"]);
    commit(&mut source, &records);
    assert_eq!(s.deleted(), ["m"]);
}

/// More bytes than one read and one search take, so breaks fall across both.
#[test]
fn a_line_breaker_cuts_events_across_reads() {
    let s = Setup::new();
    let mut body = String::new();
    let mut want = Vec::new();
    for i in 0..15_000 {
        body.push_str(&format!("<e>{i}</e>"));
        body.push_str(if i % 7 == 0 { "\r\n" } else { "" });
        want.push(format!("<e>{i}</e>"));
    }
    s.put("events.xml.gz", &gzip(&[body.as_bytes()]));
    s.announce("m", &["events.xml.gz"]);
    let mut source = s.source(broken(r"</e>(\s*)"), HOUR);
    let records = source.poll_batch(20_000, 1 << 20, Duration::from_secs(10)).unwrap();
    assert_eq!(raw(&records), want);
    commit(&mut source, &records);
    assert_eq!(s.deleted(), ["m"]);
}

#[test]
fn a_line_breaker_finding_no_break_holds_no_more_than_the_longest() {
    let s = Setup::new();
    s.put("unbroken", &[b'a'; 200_000]);
    s.put("fits", b"<e>1</e>");
    s.announce("unbroken", &["unbroken"]);
    s.announce("fits", &["fits"]);
    let records = poll(&mut s.source_up_to(broken(r"</e>(\s*)"), HOUR, 1024));
    assert_eq!(raw(&records), ["<e>1</e>"]);
}

#[test]
fn an_object_streams_across_polls_under_a_byte_cap() {
    let s = Setup::new();
    let lines: Vec<String> = (0..50).map(|i| format!("line {i:03}")).collect();
    s.put("big.gz", &gzip(&[lines.join("\n").as_bytes()]));
    s.announce("m", &["big.gz"]);
    let mut source = s.source(Framing::Lines, HOUR);
    // A record over the cap still comes back, alone.
    let mut all = source.poll_batch(1000, 1, WAIT).unwrap();
    assert_eq!(all.len(), 1);
    while all.len() < lines.len() {
        let records = source.poll_batch(1000, 20, WAIT).unwrap();
        assert!(!records.is_empty(), "the object ended early");
        assert!(records.len() <= 2, "{} records over 20 bytes", records.len());
        commit(&mut source, &records);
        all.extend(records);
    }
    assert_eq!(raw(&all), lines.iter().map(String::as_str).collect::<Vec<_>>());
    assert!(all.iter().enumerate().all(|(i, r)| r.offset == i as i64));
    assert_eq!(s.deleted(), ["m"]);
}

#[test]
fn a_source_holds_the_record_a_batch_had_no_room_for() {
    let s = Setup::new();
    s.put("small", b"a\nbbbb\n");
    s.announce("m", &["small"]);
    let mut source = s.source(Framing::Lines, HOUR);
    assert_eq!(source.held_bytes(), Some(0));
    assert_eq!(raw(&source.poll_batch(1000, 1, WAIT).unwrap()), ["a"]);
    assert_eq!(source.held_bytes(), Some(4));
    assert_eq!(raw(&poll(&mut source)), ["bbbb"]);
    assert_eq!(source.held_bytes(), Some(0));
}

#[test]
fn a_message_is_deleted_once_every_record_of_every_object_is_committed() {
    let s = Setup::new();
    s.put("a", b"1\n2\n");
    s.put("b", b"3\n4\n");
    s.announce("m", &["a", "b", "a"]);
    let mut source = s.source(Framing::Lines, HOUR);
    let records = poll(&mut source);
    assert_eq!(raw(&records), ["1", "2", "3", "4", "1", "2"]);
    let partitions: std::collections::HashSet<i32> = records.iter().map(|r| r.partition).collect();
    assert_eq!(partitions.len(), 3, "each read of an object is its own partition");
    commit(&mut source, &records[..4]);
    commit(&mut source, &records[4..5]);
    assert!(
        s.deleted().is_empty(),
        "the second read of a is not committed to its end"
    );
    commit(&mut source, &records[5..]);
    assert_eq!(s.deleted(), ["m"]);
}

#[test]
fn an_unreadable_object_leaves_its_message_on_the_queue() {
    let s = Setup::new();
    s.put("good", b"x\n");
    s.put("corrupt.gz", &[0x1f, 0x8b, 8, 0, 0, 0, 0, 0, 0, 0xff, 1, 2, 3]);
    s.announce("missing", &["good", "nowhere"]);
    s.announce("corrupt", &["corrupt.gz"]);
    s.announce("fine", &["good"]);
    s.queue.0.lock().unwrap().inbox.push(Message {
        handle: "stranger".to_string(),
        objects: Err("no notification".to_string()),
    });
    s.queue.0.lock().unwrap().inbox.push(Message {
        handle: "test-event".to_string(),
        objects: Ok(Vec::new()),
    });
    let visibility = Duration::from_millis(400);
    let mut source = s.source(Framing::Lines, visibility);
    let records = poll(&mut source);
    assert_eq!(raw(&records), ["x", "x"]);
    let extended = extended(&s);
    assert!(
        !extended.is_empty() && extended.iter().all(|h| h == "fine"),
        "{extended:?}"
    );
    commit(&mut source, &records);
    assert_eq!(s.deleted(), ["test-event", "fine"]);
}

#[test]
fn a_record_over_the_longest_makes_its_object_unreadable() {
    let s = Setup::new();
    s.put("bomb.gz", &gzip(&[&[b'a'; 4096], b"\n"]));
    s.put("lines.gz", &gzip(&[b"short\n", &[b'b'; 4096]]));
    s.put("fits", &[b'c'; 1024]);
    s.announce("bomb", &["bomb.gz"]);
    s.announce("lines", &["lines.gz"]);
    s.announce("fits", &["fits"]);
    let records = poll(&mut s.source_up_to(Framing::Lines, HOUR, 1024));
    assert_eq!(records.len(), 2, "{:?}", raw(&records));
    assert_eq!(records[0].bytes().unwrap(), b"short");
    assert_eq!(records[1].bytes().unwrap(), [b'c'; 1024]);
    s.announce("whole", &["bomb.gz"]);
    assert!(poll_briefly(&mut s.source_up_to(Framing::Object, HOUR, 1024)).is_empty());
}

/// A line's end is not part of its record, so a line of the longest fits whichever end it has.
#[test]
fn a_line_of_the_longest_fits_whatever_its_line_end() {
    let s = Setup::new();
    let mut body = [b'a'; 1024].to_vec();
    body.extend(b"\r\n");
    body.extend([b'b'; 1024]);
    body.extend(b"\n");
    s.put("crlf", &body);
    s.put("over", &[&[b'c'; 1025][..], b"\r\n"].concat());
    s.announce("crlf", &["crlf"]);
    s.announce("over", &["over"]);
    let records = poll(&mut s.source_up_to(Framing::Lines, HOUR, 1024));
    assert_eq!(raw(&records), ["a".repeat(1024), "b".repeat(1024)]);
}

#[test]
fn a_dropped_source_shows_what_it_holds_again_once_its_receive_answers() {
    let s = Setup::new();
    s.put("a", b"1\n");
    s.announce("m", &["a"]);
    let mut source = s.source(Framing::Lines, HOUR);
    assert_eq!(raw(&poll(&mut source)), ["1"]);
    s.queue.0.lock().unwrap().delay = POLL * 4;
    s.announce("late", &["a"]);
    assert!(poll_briefly(&mut source).is_empty(), "the receive is still under way");
    drop(source);
    let calls = s.queue.0.lock().unwrap();
    assert_eq!(calls.shown, ["late", "m"]);
    assert!(calls.extended.is_empty() && calls.deleted.is_empty());
}

#[test]
fn a_held_message_is_extended_past_half_its_time_and_while_blocked() {
    let s = Setup::new();
    s.put("a", b"1\n");
    s.announce("m", &["a"]);
    let visibility = Duration::from_millis(400);
    let mut source = s.source(Framing::Lines, visibility);
    let records = poll(&mut source);
    assert_eq!(records.len(), 1);
    source.keep_membership(POLL).unwrap();
    assert!(
        s.queue.0.lock().unwrap().extended.is_empty(),
        "more than half its time is left"
    );

    source.block().unwrap();
    s.announce("later", &["a"]);
    // Extended with no read under way: a read blocked on the store does not hold it back.
    assert_eq!(extended(&s), ["m"]);
    assert!(poll_briefly(&mut source).is_empty());
    assert_eq!(
        s.queue.0.lock().unwrap().receives,
        1,
        "a blocked source receives nothing"
    );

    source.release().unwrap();
    assert_eq!(raw(&poll(&mut source)), ["1"]);
}

/// A bucket's listing as the queue: each object is read once, and once committed is in the progress a later holder
/// resumes from.
#[test]
fn a_listed_object_is_read_and_its_progress_taken_once_committed() {
    use smeltr::io::listing::Listing;
    use smeltr::plan::topology::{AfterCommit, ListStart};

    let s = Setup::new();
    s.put("in/a.log", b"one\ntwo\n");
    s.put("in/b.log", b"three\n");
    s.put("out/c.log", b"elsewhere\n");
    let listing = |s: &Setup| {
        Listing::new(
            s.store.clone(),
            "logs",
            "in/",
            Duration::ZERO,
            ListStart::Everything,
            AfterCommit::Keep,
            s.runtime.clone(),
        )
        .unwrap()
    };
    let open = |s: &Setup| {
        let store: Arc<dyn ObjectStore> = s.store.clone();
        ObjectSource::new(
            Arc::new(listing(s)),
            Box::new(move |_: &str| Ok(store.clone())),
            s.runtime.clone(),
            Framing::Lines,
            HOUR,
            smeltr::io::object_source::MAX_RECORD_BYTES,
        )
        .unwrap()
    };

    let mut source = open(&s);
    source.resume(smeltr::io::Progress::default()).unwrap();
    let mut records = Vec::new();
    for _ in 0..20 {
        records.extend(poll(&mut source));
        if records.len() >= 3 {
            break;
        }
    }
    let mut lines = raw(&records);
    lines.sort();
    assert_eq!(lines, ["one", "three", "two"]);
    assert_eq!(
        source.take_progress().map(|p| p.added),
        Some(Vec::new()),
        "nothing committed yet"
    );
    commit(&mut source, &records);
    let progress = source.take_progress().expect("progress once committed");
    let mut items: Vec<&str> = progress.added.iter().map(|e| e.item.as_str()).collect();
    items.sort();
    assert_eq!(items, ["in/a.log", "in/b.log"]);
    assert!(progress.removed.is_empty());
    drop(source);

    let mut next = open(&s);
    next.resume(progress).unwrap();
    for _ in 0..3 {
        assert!(poll_briefly(&mut next).is_empty(), "the ledger holds everything listed");
    }
}

/// What the source reports as lag, once its sampler has asked: the sampler asks on a thread of its own.
fn lag(source: &mut ObjectSource) -> Vec<PartitionLag> {
    let deadline = Instant::now() + WAIT;
    loop {
        let lag = source.lag();
        if !lag.is_empty() || Instant::now() > deadline {
            return lag;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn a_queues_depth_is_its_lag_and_each_deleted_message_is_finished() {
    let s = Setup::new();
    s.queue.0.lock().unwrap().depth = Some(Ok(7));
    s.put("a.log", b"one\n");
    s.announce("m1", &["a.log"]);
    let mut source = s.source(Framing::Lines, HOUR);
    assert_eq!(
        lag(&mut source),
        [PartitionLag {
            partition: TopicPartition::new(Arc::from("orders"), 0),
            lag: 7
        }]
    );
    assert!(source.lag().is_empty(), "a sample is taken once");

    let records = poll(&mut source);
    commit(&mut source, &records);
    assert_eq!(s.deleted(), ["m1"]);
    assert_eq!(source.take_finished(), 1);
    assert_eq!(source.take_finished(), 0, "counted once");
}

#[test]
fn a_queue_that_cannot_count_or_did_not_reports_no_lag() {
    for depth in [None, Some(Err("refused".to_string()))] {
        let s = Setup::new();
        s.queue.0.lock().unwrap().depth = depth.clone();
        let mut source = s.source(Framing::Lines, HOUR);
        std::thread::sleep(POLL);
        assert!(source.lag().is_empty(), "{depth:?}");
    }
}
