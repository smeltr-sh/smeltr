//! Builds a worker over a reference bundle, with a generator and a counter
//! in place of its declared ends, and times it. This is the half of the
//! harness that reaches into the engine, so the benchmark log builds each
//! side with its own copy of this file.

use std::borrow::Cow;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use smeltr::config_file::{BundleAt, DeclaredAt};
use smeltr::core::buffer::MemoryBudget;
use smeltr::io::{
    BatchCompletion, BatchId, CommitMap, Destination, Dispatched, IoError, OwnedRecord, Rejection, Source, SourceRecord,
};
use smeltr::metrics::{MetricValue, Metrics, MetricsHandle};
use smeltr::runtime::{DestinationTarget, Worker};

use super::{CORPUS, GB, bundle_dir};

const MAX_BYTES: usize = 64_000_000;

/// One drive of `rows` records through a pipeline.
pub struct Run {
    pub rows: u64,
    pub bytes_in: u64,
    pub bytes_out: u64,
    pub wall: Duration,
    /// `smeltr_tick_process_seconds_total`: decode, route, pipeline, serialize.
    pub process: u64,
    pub docs: u64,
}

impl Run {
    pub fn rows_per_sec(&self) -> f64 {
        self.rows as f64 / self.wall.as_secs_f64()
    }

    /// Ingest volume one core sustains, at this thread count.
    pub fn gb_day_in(&self, threads: usize) -> f64 {
        self.bytes_in as f64 / self.wall.as_secs_f64() * 86_400.0 / GB / threads as f64
    }

    /// The same, counting bytes on both sides -- how the sizing guidance
    /// every vendor publishes is quoted.
    pub fn gb_day_both(&self, threads: usize) -> f64 {
        (self.bytes_in + self.bytes_out) as f64 / self.wall.as_secs_f64() * 86_400.0 / GB / threads as f64
    }
}

fn counter(samples: &[smeltr::metrics::MetricSample], name: &str) -> u64 {
    samples
        .iter()
        .filter(|s| s.key.name == name)
        .map(|s| match s.value {
            MetricValue::Counter(c) => c,
            MetricValue::FloatCounter(g) | MetricValue::Gauge(g) => g as u64,
        })
        .sum()
}

/// Build a worker over `bundle` with the generator and counter in place of
/// the declared Kafka ends, drive `rows` records of `line` through it, and
/// return what it cost.
pub fn run(bundle: &str, line: fn(u64) -> String, rows: u64, max_rows: usize, keep: usize) -> (Run, Counter) {
    let tables: serde_json::Value = serde_json::from_str(bundle).expect("bundle is JSON");
    let declared_at: DeclaredAt = tables["lookupTables"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|t| (t["id"].as_str().unwrap().to_string(), BundleAt::Dir(bundle_dir())))
        .collect();
    let system = smeltr::config_file::load_str_at(bundle, &declared_at).expect("bundle loads");
    let topology = system.compile_topology().unwrap_or_else(|e| {
        panic!(
            "the workload bundle must compile: {}",
            e.iter().map(|x| x.to_string()).collect::<Vec<_>>().join("; ")
        )
    });
    // The tables are read; the files are not needed again.
    let _ = std::fs::remove_dir_all(bundle_dir());

    let registry = Arc::new(Metrics::new());
    let source = Generator::new(rows, line);
    let sink = Counter::new(keep);
    let destinations = vec![DestinationTarget {
        id: system.topology.destinations[0].id,
        name: system.topology.destinations[0].name.clone(),
        destination: Box::new(sink.clone()),
        on_reject: system.topology.destinations[0].on_reject.clone(),
        wants_doc_id: false,
        placement: Some(smeltr::plan::topology::Placement::Topic),
    }];

    let mut worker = Worker::new(
        system.topology.sources[0].id,
        Box::new(source.clone()),
        topology,
        destinations,
        MemoryBudget::unlimited(),
        MetricsHandle::new(registry.clone(), "reference-0"),
    );

    // One batch before the clock starts: the first tick pays for the
    // lookup tables' first touch and every lazily-taken buffer.
    worker.tick(max_rows, MAX_BYTES, Duration::ZERO).expect("warm-up tick");
    let base = registry.snapshot();
    let (b_process, b_rows) = (counter(&base, "smeltr_tick_process_seconds_total"), source.totals().0);
    let b_bytes = source.totals().1;
    let (b_docs, b_out) = sink.totals();

    let wall = Instant::now();
    loop {
        match worker.tick(max_rows, MAX_BYTES, Duration::ZERO) {
            Ok(0) => break,
            Ok(_) => {}
            Err(e) => panic!("tick failed: {e}"),
        }
    }
    let wall = wall.elapsed();

    let s = registry.snapshot();
    let (generated, bytes_in) = source.totals();
    let (docs, bytes_out) = sink.totals();
    let r = Run {
        rows: generated - b_rows,
        bytes_in: bytes_in - b_bytes,
        bytes_out: bytes_out - b_out,
        wall,
        process: counter(&s, "smeltr_tick_process_seconds_total") - b_process,
        docs: docs - b_docs,
    };
    assert!(r.rows > 0, "the generator produced nothing");
    (r, sink)
}

/// The corpus stops short of `CORPUS` once it holds this many bytes. Either
/// bound is past any L3, so replay is not a cache-resident read; this one
/// keeps a pipeline of large records from measuring its own corpus.
const CORPUS_BYTES: usize = 32_000_000;

/// Replays a corpus built before the clock starts, so record synthesis is
/// not inside the measurement. What `poll_batch` costs is the copy a real
/// source also pays taking a payload out of its client.
///
/// `Arc`-backed so the caller keeps a handle after boxing it into a worker.
#[derive(Clone)]
pub struct Generator(Arc<Mutex<GenState>>);

struct GenState {
    corpus: Vec<Vec<u8>>,
    next: u64,
    limit: u64,
    bytes: u64,
    topic: Arc<str>,
}

impl Generator {
    /// `limit` records, cycling through the first `CORPUS` of `line`, or
    /// fewer once they hold `CORPUS_BYTES`.
    pub fn new(limit: u64, line: fn(u64) -> String) -> Generator {
        let mut corpus: Vec<Vec<u8>> = Vec::new();
        let mut bytes = 0;
        for i in 0..limit.min(CORPUS) {
            if bytes >= CORPUS_BYTES {
                break;
            }
            let record = line(i).into_bytes();
            bytes += record.len();
            corpus.push(record);
        }
        Generator(Arc::new(Mutex::new(GenState {
            corpus,
            next: 0,
            limit,
            bytes: 0,
            topic: Arc::from("records"),
        })))
    }

    /// Records handed out, and the raw bytes they carried.
    pub fn totals(&self) -> (u64, u64) {
        let g = self.0.lock().unwrap();
        (g.next, g.bytes)
    }
}

impl Source for Generator {
    fn poll_batch(&mut self, max_rows: usize, max_bytes: usize, _t: Duration) -> Result<Vec<SourceRecord>, IoError> {
        let mut g = self.0.lock().unwrap();
        let mut out = smeltr::io::RecordChunkBuilder::new();
        let mut bytes = 0usize;
        while out.len() < max_rows && g.next < g.limit {
            let at = (g.next % g.corpus.len() as u64) as usize;
            let len = g.corpus[at].len();
            if !out.is_empty() && bytes + len > max_bytes {
                break;
            }
            bytes += len;
            let (offset, topic) = (g.next as i64, g.topic.clone());
            out.push(&g.corpus[at], offset, 0, topic, None);
            g.next += 1;
        }
        g.bytes += bytes as u64;
        Ok(out.finish())
    }

    fn commit(&mut self, _offsets: &CommitMap) -> Result<(), IoError> {
        Ok(())
    }

    fn is_exhausted(&self) -> bool {
        let g = self.0.lock().unwrap();
        g.next >= g.limit
    }
}

#[derive(Default)]
struct Counts {
    records: u64,
    bytes: u64,
    kept: Vec<Vec<u8>>,
    keep: usize,
}

/// Counts what the pipeline produced and discards it. A destination that
/// accumulated rows would put its own allocator in the measurement.
#[derive(Clone, Default)]
pub struct Counter(Arc<Mutex<Counts>>);

impl Counter {
    /// Keeps the first `keep` documents so a test can check the mapping.
    pub fn new(keep: usize) -> Counter {
        Counter(Arc::new(Mutex::new(Counts {
            keep,
            ..Default::default()
        })))
    }

    /// Records delivered, and the bytes they serialized to.
    pub fn totals(&self) -> (u64, u64) {
        let c = self.0.lock().unwrap();
        (c.records, c.bytes)
    }

    pub fn kept(&self) -> Vec<Vec<u8>> {
        self.0.lock().unwrap().kept.clone()
    }
}

impl Destination for Counter {
    fn deliver(
        &mut self,
        _batch: BatchId,
        rows: Cow<'_, [OwnedRecord]>,
    ) -> Result<(Dispatched, Vec<Rejection>), IoError> {
        let mut c = self.0.lock().unwrap();
        for row in rows.iter() {
            c.records += 1;
            c.bytes += row.value.len() as u64;
            if c.kept.len() < c.keep {
                c.kept.push(row.value.clone());
            }
        }
        Ok((Dispatched::Complete, Vec::new()))
    }

    fn flush(&mut self) -> Result<(), IoError> {
        Ok(())
    }

    fn poll_completions(&mut self) -> Result<Vec<BatchCompletion>, IoError> {
        Ok(Vec::new())
    }
}
