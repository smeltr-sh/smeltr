//! What a typed record costs against the same data arriving as `_raw` JSON: one source's rows, once as
//! a chunk handing `id`, `service`, `message`, `total` and `status` as typed columns, and once as the JSON
//! text a parse step reads those fields out of. The same eval runs after either, and both make the same documents.
//! `cargo test --profile dist --test record-cost -- --ignored --nocapture`.

mod panos;
mod reference;

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use smeltr::core::buffer::MemoryBudget;
use smeltr::core::column::DataType;
use smeltr::io::{CommitMap, IoError, RecordChunk, RecordObject, Source, SourceRecord};
use smeltr::metrics::{Metrics, MetricsHandle};
use smeltr::plan::topology::{SourceMeta, SourceMetas, compile_topology_with};
use smeltr::runtime::{DestinationTarget, Worker};

const ROWS: u64 = 400_000;
const BATCH: usize = 5_000;
const RUNS: usize = 7;

fn fields() -> Vec<(String, DataType)> {
    [
        ("id", DataType::I64),
        ("service", DataType::Str),
        ("message", DataType::Str),
        ("total", DataType::F64),
        ("status", DataType::I64),
    ]
    .into_iter()
    .map(|(n, t)| (n.to_string(), t))
    .collect()
}

fn object(i: u64) -> RecordObject {
    const SERVICES: &[&str] = &["checkout", "cart", "inventory.reserve", "auth.session", "search.query"];
    const MESSAGES: &[&str] = &[
        "order placed",
        "item added to cart",
        "stock reserved",
        "session refreshed",
    ];
    const STATUSES: &[i64] = &[200, 201, 404, 500];
    let v = serde_json::json!({
        "id": i,
        "service": SERVICES[(i % 5) as usize],
        "message": MESSAGES[(i % 4) as usize],
        "total": (i % 10_000) as f64 / 100.0,
        "status": STATUSES[(i % 4) as usize],
    });
    v.as_object().unwrap().clone()
}

/// The steps after the record is in hand: one eval over two of its fields.
fn eval() -> serde_json::Value {
    serde_json::json!({
        "id": "ok", "type": "eval",
        "assignments": [{"target": "ok", "expr": "status < 400 && total > 1.0"}],
    })
}

/// Replays chunks built before the clock starts, one per poll, as a typed source hands one per query.
#[derive(Clone)]
struct Rows(Arc<Mutex<(Vec<Arc<RecordChunk>>, u64)>>);

impl Source for Rows {
    fn poll_batch(&mut self, max_rows: usize, _max_bytes: usize, _t: Duration) -> Result<Vec<SourceRecord>, IoError> {
        let mut g = self.0.lock().unwrap();
        let (chunks, next) = &mut *g;
        if *next >= ROWS {
            return Ok(Vec::new());
        }
        let chunk = chunks[(*next / BATCH as u64) as usize % chunks.len()].clone();
        let take = max_rows.min(BATCH).min((ROWS - *next) as usize);
        let out = (0..take as u32)
            .map(|row| SourceRecord {
                chunk: chunk.clone(),
                row,
                offset: (*next + row as u64) as i64,
                partition: 0,
                topic: Arc::from("records"),
                headers: None,
            })
            .collect();
        *next += take as u64;
        Ok(out)
    }

    fn commit(&mut self, _offsets: &CommitMap) -> Result<(), IoError> {
        Ok(())
    }

    fn is_exhausted(&self) -> bool {
        self.0.lock().unwrap().1 >= ROWS
    }
}

/// docs/s for `bundle`'s pipeline over `source`, with the source's record declared as `meta` says.
fn time(bundle: &str, source: Box<dyn Source>, meta: SourceMeta) -> (f64, reference::Counter) {
    let system = smeltr::config_file::load_str(bundle).expect("bundle loads");
    let metas = SourceMetas::from([(system.topology.sources[0].id, meta)]);
    let topology = compile_topology_with(
        &system.topology.routes,
        &system.pipelines,
        system.topology.on_exception.clone(),
        &metas,
    )
    .unwrap_or_else(|e| panic!("compile: {e:?}"));
    let sink = reference::Counter::new(1);
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
        source,
        topology,
        destinations,
        MemoryBudget::unlimited(),
        MetricsHandle::new(Arc::new(Metrics::new()), "record-cost-0"),
    );
    worker.tick(BATCH, usize::MAX, Duration::ZERO).expect("warm-up tick");
    let before = sink.totals().0;
    let wall = Instant::now();
    while worker.tick(BATCH, usize::MAX, Duration::ZERO).expect("tick") > 0 {}
    let docs = sink.totals().0 - before;
    (docs as f64 / wall.elapsed().as_secs_f64(), sink)
}

#[test]
#[ignore = "a benchmark: run with --ignored --nocapture"]
fn typed_against_raw() {
    let corpus: Vec<RecordObject> = (0..50_000).map(object).collect();
    let chunks: Vec<Arc<RecordChunk>> = corpus
        .chunks(BATCH)
        .map(|c| Arc::new(RecordChunk::from_objects(&fields(), &c.iter().collect::<Vec<_>>(), &[]).unwrap()))
        .collect();
    let lines: fn(u64) -> String = |i| serde_json::Value::Object(object(i)).to_string();

    let parse = serde_json::json!({
        "id": "body", "type": "parser", "input": "_raw", "kind": {"type": "json"},
        "declared": [
            {"name": "id", "type": "i64"}, {"name": "service", "type": "str"}, {"name": "message", "type": "str"},
            {"name": "total", "type": "f64"}, {"name": "status", "type": "i64"},
        ],
    });
    // The record is its fields, as the typed one is: the text they were read from does not go out.
    let unraw = serde_json::json!({
        "id": "unraw", "type": "eval", "assignments": [{"target": "_raw", "expr": "null"}],
    });
    let raw_bundle = reference::bundle(vec![parse, unraw, eval()], vec![]);
    let typed_bundle = reference::bundle(vec![eval()], vec![]);

    let (mut raw, mut typed) = (Vec::new(), Vec::new());
    let (mut raw_doc, mut typed_doc) = (Vec::new(), Vec::new());
    for _ in 0..RUNS {
        let (r, sink) = time(
            &raw_bundle,
            Box::new(reference::Generator::new(ROWS, lines)),
            SourceMeta::KAFKA,
        );
        raw.push(r);
        raw_doc = sink.kept();
        let rows = Rows(Arc::new(Mutex::new((chunks.clone(), 0))));
        let (t, sink) = time(
            &typed_bundle,
            Box::new(rows),
            SourceMeta::KAFKA.declaring(fields()).unwrap(),
        );
        typed.push(t);
        typed_doc = sink.kept();
    }
    let median = |v: &mut Vec<f64>| {
        v.sort_by(f64::total_cmp);
        v[v.len() / 2]
    };
    let doc = |d: &[Vec<u8>]| serde_json::from_slice::<serde_json::Value>(&d[0]).unwrap();
    assert_eq!(doc(&raw_doc), doc(&typed_doc), "both make the same document");
    let (r, t) = (median(&mut raw), median(&mut typed));
    println!("\nRECORD raw={:.0} typed={:.0} docs/s, typed/raw={:.2}", r, t, t / r);
}
