//! The reader and transform on their own threads: the halves progress
//! independently, and the handoff and reports carry everything they need.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use smeltr::core::buffer::MemoryBudget;
use smeltr::core::id::{DestId, FunctionId};
use smeltr::egress::Egress;
use smeltr::format::encoding::Encoding;
use smeltr::handoff::{Handed, Handoff, Ready, Report, reports};
use smeltr::io::fake::{AsyncDestination, FakeSource};
use smeltr::io::{SourceRecord, TopicPartition};
use smeltr::metrics::{Metrics, MetricsHandle};
use smeltr::plan::config::{Answer, FunctionConfig, FunctionKind, PipelineConfig, Step};
use smeltr::plan::topology::{DeliverRule, RouteConfig, compile_topology};
use smeltr::runtime::{DestinationTarget, Lane, Polled, Transform, Worker};

fn record(offset: i64) -> SourceRecord {
    SourceRecord {
        chunk: smeltr::io::RecordChunk::of_bytes(br#"{"a":1}"#),
        row: 0,
        offset,
        partition: 0,
        topic: "t".into(),
        headers: None,
    }
}

fn deliver_to(destinations: &[DestId]) -> Vec<DeliverRule> {
    vec![DeliverRule {
        when: None,
        to: destinations.to_vec(),
    }]
}

fn code(id: FunctionId, body: &str) -> Step {
    Step::Function(FunctionConfig::new(id, FunctionKind::Code { body: body.into() }))
}

fn worker(source: &FakeSource, dest: &AsyncDestination) -> Worker {
    let routes = vec![RouteConfig {
        id: "r".to_string(),
        source: 0,
        cond: None,
        pipeline: 0,
        deliver: deliver_to(&[0]),
        on_exception: Default::default(),
    }];
    let pipelines = vec![PipelineConfig {
        id: 0,
        name: "pipeline-0".into(),
        body: vec![code(0, "Object.assign($, json(_raw));")],
        step_ids: Vec::new(),
        lookup_tables: Default::default(),
        grok_patterns: Default::default(),
    }];
    let topology = compile_topology(&routes, &pipelines).unwrap();
    let mut w = Worker::new(
        0,
        Box::new(source.clone()),
        Encoding::Auto,
        topology,
        Vec::new(),
        MemoryBudget::unlimited(),
        MetricsHandle::new(Arc::new(Metrics::new()), "t0"),
    );
    w.add_destination(DestinationTarget {
        id: 0,
        name: "0".to_string(),
        destination: Box::new(dest.clone()),
        on_reject: Answer::Drop,
        wants_doc_id: false,
        placement: Some(smeltr::plan::topology::Placement::Topic),
        id_field: None,
    });
    w
}

/// Two sources, each delivering to every destination in `dests`, drained by one transform thread. Returns the
/// thread, the lanes, and each lane's reader-side `Acks`.
fn two_lanes(dests: Vec<DestinationTarget>) -> (Transform, Vec<Arc<Lane>>, Vec<smeltr::handoff::Acks>) {
    let dest_ids: Vec<DestId> = dests.iter().map(|d| d.id).collect();
    two_lanes_to(dests, [dest_ids.clone(), dest_ids])
}

/// Two sources, source `i` delivering to `to[i]`, drained by one transform thread.
fn two_lanes_to(
    dests: Vec<DestinationTarget>,
    to: [Vec<DestId>; 2],
) -> (Transform, Vec<Arc<Lane>>, Vec<smeltr::handoff::Acks>) {
    let routes: Vec<RouteConfig> = [0u32, 1]
        .into_iter()
        .map(|source| RouteConfig {
            id: format!("route-{source}"),
            source,
            cond: None,
            pipeline: 0,
            deliver: deliver_to(&to[source as usize]),
            on_exception: Default::default(),
        })
        .collect();
    let pipelines = vec![PipelineConfig {
        id: 0,
        name: "pipeline-0".into(),
        body: vec![code(0, "Object.assign($, json(_raw));")],
        step_ids: Vec::new(),
        lookup_tables: Default::default(),
        grok_patterns: Default::default(),
    }];
    let topology = Arc::new(compile_topology(&routes, &pipelines).unwrap());
    let ready = Arc::new(Ready::new());

    let mut lanes = Vec::new();
    let mut acks = Vec::new();
    for source_id in [0u32, 1] {
        let (tx, rx) = reports();
        lanes.push(Arc::new(Lane::new(
            source_id,
            Some(format!("source-{source_id}")),
            smeltr::runtime::Reading {
                meta: topology.meta_for(source_id),
                encoding: Encoding::Auto,
            },
            Handoff::watched(ready.clone(), 8_000_000),
            to[source_id as usize].clone(),
            tx,
        )));
        acks.push(rx);
    }
    let transform = Transform::new(
        lanes.clone(),
        ready,
        0,
        topology,
        dests,
        MemoryBudget::unlimited(),
        MetricsHandle::new(Arc::new(Metrics::new()), "transform-0"),
    );
    (transform, lanes, acks)
}

fn async_target(id: DestId, dest: &AsyncDestination) -> DestinationTarget {
    DestinationTarget {
        id,
        name: id.to_string(),
        destination: Box::new(dest.clone()),
        on_reject: Answer::Drop,
        wants_doc_id: false,
        placement: Some(smeltr::plan::topology::Placement::Topic),
        id_field: None,
    }
}

fn push(lane: &Lane, batch: u64, value: &[u8]) {
    lane.batches
        .push(
            Handed {
                batch,
                records: vec![SourceRecord {
                    chunk: smeltr::io::RecordChunk::of_bytes(value),
                    row: 0,
                    offset: 0,
                    partition: 0,
                    topic: "t".into(),
                    headers: None,
                }],
            },
            Duration::from_secs(1),
        )
        .unwrap();
}

/// Two sources drained by one pool: a batch's outcome goes to the reader
/// that polled it, never to the other. Batch ids are unique across the
/// worker, and that is the only thing a completion carries.
#[test]
fn a_pool_reports_each_lane_s_batch_to_that_lane_s_reader() {
    let dest = AsyncDestination::new();
    let (mut transform, lanes, acks) = two_lanes(vec![async_target(0, &dest)]);

    // One batch per lane, with ids that would have collided under the old
    // per-reader numbering.
    push(&lanes[0], 100, br#"{"a":1}"#);
    push(&lanes[1], 200, br#"{"a":1}"#);

    // Run until both batches have been handed to the destination and
    // acknowledged back.
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut seen: Vec<Vec<u64>> = vec![Vec::new(), Vec::new()];
    while Instant::now() < deadline && !(seen[0].contains(&100) && seen[1].contains(&200)) {
        let _ = transform.step(Duration::from_millis(5));
        for batch in dest.outstanding() {
            dest.complete(batch);
        }
        for (i, rx) in acks.iter().enumerate() {
            for r in rx.drain() {
                if let Report::Acked { batch, .. } = r {
                    seen[i].push(batch);
                }
            }
        }
    }

    assert_eq!(seen[0], vec![100], "lane 0's reader heard only its own batch");
    assert_eq!(seen[1], vec![200], "lane 1's reader heard only its own batch");
}

/// A handle buffers rows until `dispatch` seals them. When one destination refuses lane A's batch, the destinations
/// after it still hold A's rows -- and this thread's next seal may be lane B's batch. Those rows must be dropped: A is
/// parked and re-runs whole.
#[test]
fn rows_of_a_failed_batch_do_not_ride_out_in_another_lane_s() {
    let (first, second) = (AsyncDestination::new(), AsyncDestination::new());
    let (mut transform, lanes, _acks) = two_lanes(vec![async_target(0, &first), async_target(1, &second)]);

    // Lane A's batch: destination 0 refuses the seal, so `hand_off` never reaches destination 1, which is left
    // holding A's rows.
    first.fail_next_deliver("queue stayed full");
    push(&lanes[0], 100, br#"{"lane":"A"}"#);
    assert!(transform.step(Duration::ZERO).is_err());
    assert!(
        second.units().is_empty(),
        "nothing was sealed at the second destination"
    );

    // Lane B's batch is the next thing this thread seals. Lane A's parked batch is taken first -- it is older -- so
    // both go out; what matters is which rows each unit carries.
    push(&lanes[1], 200, br#"{"lane":"B"}"#);
    for _ in 0..4 {
        let _ = transform.step(Duration::ZERO);
    }

    let by_batch = |units: Vec<(u64, Vec<Vec<u8>>)>| -> Vec<(u64, Vec<String>)> {
        units
            .into_iter()
            .map(|(b, rows)| (b, rows.into_iter().map(|r| String::from_utf8(r).unwrap()).collect()))
            .collect()
    };
    let second_units = by_batch(second.units());
    let b = second_units
        .iter()
        .find(|(id, _)| *id == 200)
        .expect("lane B's batch sealed");
    assert_eq!(
        b.1.len(),
        1,
        "lane B's unit carries lane B's row and nothing else: {second_units:?}"
    );
    assert!(b.1[0].contains(r#""lane":"B""#), "{second_units:?}");
    let a = second_units
        .iter()
        .find(|(id, _)| *id == 100)
        .expect("lane A's re-run sealed");
    assert_eq!(
        a.1.len(),
        1,
        "the re-run carries lane A's row exactly once: {second_units:?}"
    );
    assert!(a.1[0].contains(r#""lane":"A""#), "{second_units:?}");
}

/// A destination whose queue is full stops the lanes feeding it and no other. The pool never waits on the queue: a
/// step over the blocked lane returns at once, the other lane keeps flowing, the reader hears of a block only once the
/// wait has outlasted the destination's stall threshold, and the parked batch goes out once room returns.
#[test]
fn a_lane_waiting_on_a_full_destination_leaves_the_pool_to_the_others() {
    let slow = AsyncDestination::new();
    let fast = AsyncDestination::new();
    let stall_after = Duration::from_millis(200);
    let mut egress = Egress::start(
        0,
        "d",
        Box::new(slow.clone()),
        stall_after,
        1,
        MetricsHandle::new(Arc::new(Metrics::new()), "egress-0"),
    );
    let slow_handle = DestinationTarget {
        id: 0,
        name: "0".to_string(),
        destination: Box::new(egress.handle()),
        on_reject: Answer::Drop,
        wants_doc_id: false,
        placement: Some(smeltr::plan::topology::Placement::Topic),
        id_field: None,
    };
    let (mut transform, lanes, acks) = two_lanes_to(vec![slow_handle, async_target(1, &fast)], [vec![0], vec![1]]);

    // Lane 0's first batch fills the slow destination's whole budget and is never acknowledged; its second finds no
    // room and is parked.
    push(&lanes[0], 100, br#"{"lane":"A"}"#);
    push(&lanes[0], 101, br#"{"lane":"A"}"#);
    let started = Instant::now();
    for _ in 0..4 {
        let _ = transform.step(Duration::ZERO);
    }
    assert!(
        started.elapsed() < Duration::from_millis(100),
        "a full queue is not waited on: {:?}",
        started.elapsed()
    );
    for _ in 0..200 {
        if slow.outstanding().len() == 1 {
            break;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(slow.outstanding().len(), 1, "the first batch went out alone");
    assert!(
        !acks[0].drain().iter().any(|r| matches!(r, Report::Block(..))),
        "a wait is not a block until it has lasted"
    );

    // Lane 1 keeps flowing through the same pool.
    push(&lanes[1], 200, br#"{"lane":"B"}"#);
    push(&lanes[1], 201, br#"{"lane":"B"}"#);
    for _ in 0..6 {
        let _ = transform.step(Duration::ZERO);
    }
    let mut delivered: Vec<u64> = fast.outstanding();
    delivered.sort_unstable();
    assert_eq!(delivered, vec![200, 201], "the other lane is served while lane 0 waits");
    assert_eq!(slow.outstanding().len(), 1, "nothing was re-run against the full queue");

    // Past the threshold, the wait is a stall and the reader is told, once.
    std::thread::sleep(stall_after);
    for _ in 0..4 {
        let _ = transform.step(Duration::ZERO);
    }
    let blocks = acks[0]
        .drain()
        .iter()
        .filter(|r| matches!(r, Report::Block(..)))
        .count();
    assert_eq!(blocks, 1, "blocked once the wait outlasted the stall threshold");
    assert!(
        !acks[1].drain().iter().any(|r| matches!(r, Report::Block(..))),
        "lane 1 was never blocked"
    );

    // Room returns: the parked batch goes out, and the reader is released.
    for b in slow.outstanding() {
        slow.complete(b);
    }
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && slow.outstanding().is_empty() {
        let _ = transform.step(Duration::from_millis(5));
    }
    // Egress numbers batches in its own id space, so count the units and read their rows.
    let units = slow.units();
    assert_eq!(
        units.len(),
        2,
        "the parked batch went out once there was room: {units:?}"
    );
    assert!(
        units
            .iter()
            .all(|(_, rows)| rows.len() == 1 && rows[0].windows(10).any(|w| w == br#""lane":"A""#)),
        "each unit carries lane A's one row: {units:?}"
    );
    assert!(
        acks[0].drain().iter().any(|r| matches!(r, Report::Release)),
        "delivery releases the block"
    );
    drop(egress);
}

/// Both halves, each on its own thread, stopped by one flag.
struct Running {
    stop: Arc<AtomicBool>,
    threads: Vec<std::thread::JoinHandle<()>>,
}

impl Running {
    fn stop(self) {
        self.stop.store(true, Ordering::Relaxed);
        for t in self.threads {
            t.join().unwrap();
        }
    }
}

fn spawn(w: Worker) -> Running {
    let lane = w.lane().clone();
    let Worker {
        mut reader,
        mut transform,
        acks,
    } = w;
    let stop = Arc::new(AtomicBool::new(false));

    let r = {
        let (lane, stop) = (lane.clone(), stop.clone());
        std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                let _ = reader.step(&lane.batches, &acks, 100, 1 << 20, Duration::from_millis(5));
            }
            let deadline = Instant::now() + Duration::from_secs(5);
            while Instant::now() < deadline && !reader.commits.is_empty() {
                reader.apply(acks.drain());
                if reader.retire_and_commit().is_err() {
                    break;
                }
                std::thread::sleep(Duration::from_millis(2));
            }
        })
    };
    let t = {
        let stop = stop.clone();
        std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                let _ = transform.step(Duration::from_millis(5));
            }
            // As `worker::run_transform_loop`: finish the queue, then
            // report what the destinations said.
            let deadline = Instant::now() + Duration::from_secs(5);
            while Instant::now() < deadline && !lane.batches.is_empty() {
                if transform.step(Duration::from_millis(5)).is_err() {
                    break;
                }
            }
            let _ = transform.flush_destinations();
            let _ = transform.collect();
        })
    };
    Running {
        stop,
        threads: vec![r, t],
    }
}

fn until(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if done() {
            return;
        }
        std::thread::sleep(Duration::from_millis(2));
    }
    panic!("timed out waiting for {what}");
}

#[test]
fn offsets_commit_only_after_the_destination_acknowledges() {
    let src = FakeSource::new((0..4).map(record));
    let dest = AsyncDestination::new();
    let running = spawn(worker(&src, &dest));

    // Rows are sent before the batch is dispatched, so wait for the
    // batch itself, not the row count.
    until("the batch to reach the destination", || {
        dest.sent() == 4 && !dest.outstanding().is_empty()
    });
    assert!(
        src.committed().is_empty(),
        "nothing may commit while the destination has not answered"
    );

    for batch in dest.outstanding() {
        dest.complete(batch);
    }
    until("the acknowledgment to reach the reader", || !src.committed().is_empty());

    let committed = src.committed();
    let last = committed.last().unwrap();
    assert_eq!(
        last[&TopicPartition::new("t".into(), 0)],
        3,
        "the watermark reaches the newest record of the run"
    );
    running.stop();
}

/// The reader outlives its own polling to commit what transform lands.
#[test]
fn a_batch_in_flight_at_shutdown_is_still_committed() {
    let src = FakeSource::new((0..4).map(record));
    let dest = AsyncDestination::new();
    let running = spawn(worker(&src, &dest));

    until("the batch to reach the destination", || {
        dest.sent() == 4 && !dest.outstanding().is_empty()
    });
    assert!(src.committed().is_empty());

    // Stop while the destination still owes an acknowledgment.
    let batches = dest.outstanding();
    let stop = running.stop.clone();
    let finisher = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(20));
        for b in batches {
            dest.complete(b);
        }
    });
    stop.store(true, Ordering::Relaxed);
    for t in running.threads {
        t.join().unwrap();
    }
    finisher.join().unwrap();

    assert!(
        !src.committed().is_empty(),
        "a batch acknowledged during shutdown still has to be committed"
    );
}

/// A record big enough that two do not fit in the handoff together.
fn big_record(offset: i64) -> SourceRecord {
    let mut raw = br#"{"a":""#.to_vec();
    raw.extend(std::iter::repeat_n(b'x', smeltr::handoff::HANDOFF_BYTES * 3 / 4));
    raw.extend_from_slice(br#""}"#);
    SourceRecord {
        chunk: smeltr::io::RecordChunk::of_bytes(raw),
        row: 0,
        offset,
        partition: 0,
        topic: "t".into(),
        headers: None,
    }
}

/// A tracked batch the queue refuses stays with the reader and is
/// offered again: it is delivered, and the commit waits for it.
#[test]
fn a_batch_the_queue_refuses_is_kept_and_still_commits() {
    let source = FakeSource::new([big_record(1), big_record(2), big_record(3)]);
    let dest = AsyncDestination::new();
    let mut w = worker(&source, &dest);
    let short = Duration::from_millis(10);
    let one = 1;

    assert!(matches!(
        w.reader
            .step(&w.transform.lanes[0].batches, &w.acks, one, usize::MAX, short)
            .unwrap(),
        Polled::Handed
    ));
    assert!(matches!(
        w.reader
            .step(&w.transform.lanes[0].batches, &w.acks, one, usize::MAX, short)
            .unwrap(),
        Polled::Full
    ));

    w.transform.step(Duration::ZERO).unwrap();
    assert!(matches!(
        w.reader
            .step(&w.transform.lanes[0].batches, &w.acks, one, usize::MAX, short)
            .unwrap(),
        Polled::Handed
    ));
    w.transform.step(Duration::ZERO).unwrap();
    w.reader
        .step(&w.transform.lanes[0].batches, &w.acks, one, usize::MAX, short)
        .unwrap();
    w.transform.step(Duration::ZERO).unwrap();

    // In dispatch order: the batches of records 1, 2 and 3. Record 2's is the one the queue refused.
    let out = dest.outstanding();
    assert_eq!(out.len(), 3, "the refused batch was delivered, not dropped");
    let tp = TopicPartition::new("t".into(), 0);
    let settle = |w: &mut Worker| {
        w.transform.step(Duration::ZERO).unwrap();
        w.reader.apply(w.acks.drain());
        w.reader.retire_and_commit().unwrap();
        source.committed().last().map(|m| m[&tp])
    };

    dest.complete(out[0]);
    dest.complete(out[2]);
    assert_eq!(settle(&mut w), Some(1), "the commit waits for the refused batch");

    dest.complete(out[1]);
    assert_eq!(settle(&mut w), Some(3), "everything delivered was committed");
    assert!(w.reader.commits.is_empty(), "nothing is left outstanding");
}

/// A source that never polls empty still commits on every step.
#[test]
fn a_busy_source_still_commits_and_never_sticks_at_the_depth_limit() {
    let source = FakeSource::new((1..=12).map(record));
    let dest = AsyncDestination::new();
    let mut w = worker(&source, &dest);
    let one = 1;
    let short = Duration::from_millis(10);

    for _ in 0..12 {
        let polled = w
            .reader
            .step(&w.transform.lanes[0].batches, &w.acks, one, usize::MAX, short)
            .unwrap();
        assert!(
            matches!(polled, Polled::Handed),
            "the reader stopped taking work while everything before was acknowledged"
        );
        w.transform.step(Duration::ZERO).unwrap();
        for b in dest.outstanding() {
            dest.complete(b);
        }
        w.transform.step(Duration::ZERO).unwrap();
    }
    w.reader
        .step(&w.transform.lanes[0].batches, &w.acks, one, usize::MAX, short)
        .unwrap();

    let tp = TopicPartition::new("t".into(), 0);
    let commits = source.committed();
    assert!(
        commits.len() >= 8,
        "commits should have happened all along, not once at the end: {}",
        commits.len()
    );
    assert_eq!(commits.last().map(|m| m[&tp]), Some(12));
    assert!(w.reader.commits.is_empty());
}

/// Past the depth limit, a step waits for an acknowledgment rather than
/// returning at once, so the reader loop needs no sleep of its own.
#[test]
fn a_full_reader_waits_for_an_acknowledgment() {
    let source = FakeSource::new((0..8).map(record));
    let dest = AsyncDestination::new();
    let mut w = worker(&source, &dest);
    let short = Duration::from_millis(10);

    // Hand every batch over and run them, acknowledging nothing.
    for _ in 0..=smeltr::runtime::MAX_IN_FLIGHT {
        assert!(matches!(
            w.reader
                .step(&w.transform.lanes[0].batches, &w.acks, 1, usize::MAX, short)
                .unwrap(),
            Polled::Handed
        ));
        w.transform.step(Duration::ZERO).unwrap();
    }
    let started = Instant::now();
    assert!(matches!(
        w.reader
            .step(
                &w.transform.lanes[0].batches,
                &w.acks,
                1,
                usize::MAX,
                Duration::from_millis(100)
            )
            .unwrap(),
        Polled::Full
    ));
    assert!(
        started.elapsed() >= Duration::from_millis(100),
        "a full reader waits its timeout for an acknowledgment"
    );

    // An acknowledgment wakes it before the timeout.
    let acked = dest.outstanding()[0];
    dest.complete(acked);
    w.transform.step(Duration::ZERO).unwrap();
    let started = Instant::now();
    assert!(matches!(
        w.reader
            .step(
                &w.transform.lanes[0].batches,
                &w.acks,
                1,
                usize::MAX,
                Duration::from_secs(5)
            )
            .unwrap(),
        Polled::Handed
    ));
    assert!(started.elapsed() < Duration::from_secs(5));
}

/// One source whose pipeline declares `n` and delivers to destination 0, drained by `transform` with `captures`.
fn capturing(dest: &AsyncDestination, captures: &Arc<smeltr::worker::capture::Captures>) -> (Transform, Arc<Lane>) {
    capturing_route(
        dest,
        captures,
        RouteConfig {
            id: "orders".to_string(),
            source: 0,
            cond: None,
            pipeline: 0,
            deliver: deliver_to(&[0]),
            on_exception: Default::default(),
        },
    )
}

/// `capturing`, over `route`.
fn capturing_route(
    dest: &AsyncDestination,
    captures: &Arc<smeltr::worker::capture::Captures>,
    route: RouteConfig,
) -> (Transform, Arc<Lane>) {
    capturing_routes(
        dest,
        captures,
        vec![route],
        vec![pipeline(0, "n: i64; Object.assign($, json(_raw)); doubled = n * 2;")],
    )
}

/// A pipeline `id` of one code step, `body`.
fn pipeline(id: usize, body: &str) -> PipelineConfig {
    PipelineConfig {
        id,
        name: format!("pipeline-{id}"),
        body: vec![code(0, body)],
        step_ids: Vec::new(),
        lookup_tables: Default::default(),
        grok_patterns: Default::default(),
    }
}

/// `capturing`, over `routes` from source 0 and their `pipelines`.
fn capturing_routes(
    dest: &AsyncDestination,
    captures: &Arc<smeltr::worker::capture::Captures>,
    routes: Vec<RouteConfig>,
    pipelines: Vec<PipelineConfig>,
) -> (Transform, Arc<Lane>) {
    let topology = Arc::new(compile_topology(&routes, &pipelines).unwrap());
    let ready = Arc::new(Ready::new());
    let (tx, _rx) = reports();
    let lane = Arc::new(Lane::new(
        0,
        Some("source-0".to_string()),
        smeltr::runtime::Reading {
            meta: topology.meta_for(0),
            encoding: Encoding::Auto,
        },
        Handoff::watched(ready.clone(), 8_000_000),
        vec![0],
        tx,
    ));
    let transform = Transform::new(
        vec![lane.clone()],
        ready,
        0,
        topology,
        vec![async_target(0, dest)],
        MemoryBudget::unlimited(),
        MetricsHandle::new(Arc::new(Metrics::new()), "transform-0"),
    )
    .with_captures(captures.clone());
    (transform, lane)
}

fn at_destination(filter: Option<&str>) -> smeltr::heartbeat::CaptureInvitation {
    smeltr::heartbeat::CaptureInvitation {
        session: "s1".to_string(),
        group: "g".to_string(),
        source: None,
        destination: Some("0".to_string()),
        filter: filter.map(str::to_string),
        start: None,
        max_matches: 10,
        max_bytes: 1_000_000,
        deadline_secs: 60,
        secrets: Vec::new(),
    }
}

/// A capture at a destination keeps the rows its filter matches, each as its pipeline finished it: the fields the
/// pipeline writes, typed as declared, then where the destination files it and the route that made it.
#[test]
fn a_destination_capture_keeps_the_finished_rows_its_filter_matches() {
    let dest = AsyncDestination::new();
    let captures = Arc::new(smeltr::worker::capture::Captures::default());
    captures.admit(&[at_destination(Some("doubled > 2"))]);
    let (mut transform, lane) = capturing(&dest, &captures);

    push(&lane, 1, br#"{"n":1}"#);
    push(&lane, 2, br#"{"n":2}"#);
    for _ in 0..4 {
        let _ = transform.step(Duration::ZERO);
    }

    let report = captures.progress("w1").remove(0);
    let [batch] = &report.batches[..] else {
        panic!("one route, one batch: {report:?}")
    };
    assert_eq!(batch.rows.len(), 1, "the filter read the pipeline's output: {report:?}");
    let column = |name: &str| batch.column(name).unwrap_or_else(|| panic!("no {name}: {report:?}"));
    assert_eq!(
        batch.columns[column("n")].ty,
        "i64",
        "typed as the pipeline declares it"
    );
    assert_eq!(batch.rows[0].values[column("n")], serde_json::json!(2));
    assert_eq!(batch.rows[0].values[column("doubled")], serde_json::json!(4));
    assert_eq!(
        batch.rows[0].values[column("__destination.route")],
        serde_json::json!("orders")
    );
}

/// A filter naming what a route's pipeline does not write ends the capture, saying which route.
#[test]
fn a_destination_filter_a_route_cannot_read_ends_the_capture() {
    let dest = AsyncDestination::new();
    let captures = Arc::new(smeltr::worker::capture::Captures::default());
    captures.admit(&[at_destination(Some("nothing_here > 1"))]);
    let (mut transform, lane) = capturing(&dest, &captures);

    push(&lane, 1, br#"{"n":1}"#);
    let _ = transform.step(Duration::ZERO);

    let report = captures.finished("w1").remove(0);
    assert!(
        matches!(&report.finished, Some(smeltr::heartbeat::CaptureEnd::Failed(why)) if why.contains("\"orders\"")),
        "{report:?}"
    );
}

/// Where routes of different shapes deliver to one destination, a filter keeps what it matches on the routes it
/// compiles on and passes over the others.
#[test]
fn a_destination_filter_reads_the_routes_it_compiles_on() {
    let dest = AsyncDestination::new();
    let captures = Arc::new(smeltr::worker::capture::Captures::default());
    captures.admit(&[at_destination(Some("doubled > 2"))]);
    let route = |id: &str, pipeline| RouteConfig {
        id: id.to_string(),
        source: 0,
        cond: None,
        pipeline,
        deliver: deliver_to(&[0]),
        on_exception: Default::default(),
    };
    let (mut transform, lane) = capturing_routes(
        &dest,
        &captures,
        vec![route("orders", 0), route("audit", 1)],
        vec![
            pipeline(0, "n: i64; Object.assign($, json(_raw)); doubled = n * 2;"),
            pipeline(1, "seen = true;"),
        ],
    );

    push(&lane, 1, br#"{"n":2}"#);
    for _ in 0..4 {
        let _ = transform.step(Duration::ZERO);
    }

    let report = captures.progress("w1").remove(0);
    assert_eq!(report.finished, None, "{report:?}");
    let rows: Vec<_> = report.batches.iter().flat_map(|b| &b.rows).collect();
    assert_eq!(rows.len(), 1, "{report:?}");
    assert_eq!(
        held_column(&report, "__destination.route"),
        vec![serde_json::json!("orders")]
    );
}

/// A filter may read what the source declares, though the pipeline reads none of it.
#[test]
fn a_destination_filter_reads_the_source_s_fields() {
    let dest = AsyncDestination::new();
    let captures = Arc::new(smeltr::worker::capture::Captures::default());
    captures.admit(&[at_destination(Some(r#"__source.topic == "t""#))]);
    let (mut transform, lane) = capturing(&dest, &captures);

    push(&lane, 1, br#"{"n":1}"#);
    let _ = transform.step(Duration::ZERO);

    let report = captures.progress("w1").remove(0);
    assert_eq!(report.batches[0].rows.len(), 1, "{report:?}");
}

/// A capture at a destination shows every `__` field the row holds, though no filter or pipeline reads them.
#[test]
fn a_destination_capture_carries_the_row_s_internal_fields() {
    let dest = AsyncDestination::new();
    let captures = Arc::new(smeltr::worker::capture::Captures::default());
    captures.admit(&[at_destination(None)]);
    let (mut transform, lane) = capturing(&dest, &captures);

    push(&lane, 1, br#"{"n":1}"#);
    let _ = transform.step(Duration::ZERO);

    let report = captures.progress("w1").remove(0);
    let batch = &report.batches[0];
    let column = |name: &str| batch.column(name).unwrap_or_else(|| panic!("no {name}: {report:?}"));
    assert_eq!(batch.rows[0].values[column("__source.topic")], serde_json::json!("t"));
    assert_eq!(batch.column("__live"), None, "true on every row sent: {report:?}");
}

/// A transform thread copies no more than a capture's bytes still hold, whatever its count allows.
#[test]
fn a_destination_capture_copies_no_more_than_its_bytes_allow() {
    let dest = AsyncDestination::new();
    let captures = Arc::new(smeltr::worker::capture::Captures::default());
    captures.admit(&[smeltr::heartbeat::CaptureInvitation {
        max_bytes: 1,
        ..at_destination(None)
    }]);
    let (mut transform, lane) = capturing(&dest, &captures);
    let room = captures.at_destinations()[0].room;
    assert_eq!(room, 1);

    push(&lane, 1, br#"{"n":1}"#);
    let _ = transform.step(Duration::ZERO);

    let report = captures.finished("w1").remove(0);
    assert_eq!(report.finished, Some(smeltr::heartbeat::CaptureEnd::Bytes));
    assert!(report.batches.iter().all(|b| b.rows.is_empty()), "{report:?}");
}

/// A capture of what `source-0` reads, keeping every record.
fn at_source() -> smeltr::heartbeat::CaptureInvitation {
    smeltr::heartbeat::CaptureInvitation {
        source: Some(smeltr::config_file::SourceDto {
            id: "source-0".to_string(),
            kind: smeltr::config_file::SourceKindDto::Kafka {
                schema_registry: None,
                read: Default::default(),
                brokers: "b1:9092".to_string(),
                topics: vec!["t".to_string()],
                group_id: "production-group".to_string(),
                tls: Default::default(),
                start: Default::default(),
                stop: None,
                fetch_bytes: Default::default(),
                encoding: Default::default(),
                props: Default::default(),
            },
        }),
        destination: None,
        ..at_destination(None)
    }
}

/// One batch of `values`, each at its own offset.
fn push_each(lane: &Lane, batch: u64, values: &[&[u8]]) {
    let records = values
        .iter()
        .enumerate()
        .map(|(offset, value)| SourceRecord {
            chunk: smeltr::io::RecordChunk::of_bytes(value),
            row: 0,
            offset: offset as i64,
            partition: 0,
            topic: "t".into(),
            headers: None,
        })
        .collect();
    lane.batches
        .push(Handed { batch, records }, Duration::from_secs(1))
        .unwrap();
}

/// What `name` holds on each row of the batch that has a column `name`.
fn held_column(report: &smeltr::heartbeat::CaptureReport, name: &str) -> Vec<serde_json::Value> {
    report
        .batches
        .iter()
        .filter_map(|b| b.column(name).map(|at| (b, at)))
        .flat_map(|(b, at)| b.rows.iter().map(move |r| r.values[at].clone()))
        .collect()
}

/// A capture opened on a source a block holds is shown the records that blocked, and only those, once, each with
/// why it blocked.
#[test]
fn a_capture_of_a_blocked_source_is_shown_the_records_that_blocked() {
    let dest = AsyncDestination::new();
    let captures = Arc::new(smeltr::worker::capture::Captures::default());
    let route = RouteConfig {
        id: "orders".to_string(),
        source: 0,
        cond: None,
        pipeline: 0,
        deliver: deliver_to(&[0]),
        on_exception: smeltr::plan::config::OnException::every(Answer::Block),
    };
    let (mut transform, lane) = capturing_route(&dest, &captures, route);

    push_each(&lane, 1, &[br#"{"n":1}"#, b"oops", br#"{"n":3}"#]);
    transform.step(Duration::ZERO).expect_err("the batch blocks");

    captures.admit(&[at_source()]);
    for _ in 0..3 {
        transform.step(Duration::ZERO).expect("a held batch is not run again");
    }

    let report = captures.progress("w1").remove(0);
    let why = held_column(&report, smeltr::worker::capture::EXCEPTION_MESSAGE);
    assert_eq!(why.len(), 1, "the one record that blocked, once: {report:?}");
    assert!(why[0].as_str().unwrap().contains("json"), "{why:?}");
    assert_eq!(held_column(&report, "_raw"), [serde_json::json!("oops")]);
    assert_eq!(held_column(&report, "__source.offset"), [serde_json::json!(1)]);
}

/// A record no route took, under block, is shown saying so.
#[test]
fn a_capture_of_a_source_blocked_on_unmatched_records_is_shown_them() {
    let dest = AsyncDestination::new();
    let captures = Arc::new(smeltr::worker::capture::Captures::default());
    let route = RouteConfig {
        id: "orders".to_string(),
        source: 0,
        cond: Some("__source.offset == 0".to_string()),
        pipeline: 0,
        deliver: deliver_to(&[0]),
        on_exception: Default::default(),
    };
    let (mut transform, lane) = capturing_route(&dest, &captures, route);

    push_each(&lane, 1, &[br#"{"n":1}"#, br#"{"n":2}"#]);
    transform.step(Duration::ZERO).expect_err("the batch blocks");
    captures.admit(&[at_source()]);
    transform.step(Duration::ZERO).expect("a held batch is not run again");

    let report = captures.progress("w1").remove(0);
    assert_eq!(
        held_column(&report, smeltr::worker::capture::EXCEPTION_MESSAGE),
        [serde_json::json!("matched no route")]
    );
    assert_eq!(held_column(&report, "__source.offset"), [serde_json::json!(1)]);
}
