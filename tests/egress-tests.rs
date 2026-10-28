//! The egress stage: one thread per destination, shared by every reader.

use std::time::Duration;

use smeltr::egress::Egress;
use smeltr::handoff::Ready;
use smeltr::io::fake::{AsyncDestination, FakeDestination};
use smeltr::io::{Destination, Dispatched, OutgoingRecord, OwnedRecord, SalvageTarget};
use smeltr::metrics::{Metrics, MetricsHandle};
use std::borrow::Cow;
use std::sync::Arc;

/// The queue budget these tests run under: what a destination's `bufferBytes` would state.
const BUDGET: usize = 16_000_000;

fn metrics() -> MetricsHandle {
    MetricsHandle::new(Arc::new(Metrics::new()), "test")
}

fn timeout() -> Duration {
    Duration::from_secs(5)
}

/// Hand `rows` to `h` as `batch`, in one call, as a transform thread does.
fn hand(h: &mut dyn Destination, batch: u64, rows: &[OutgoingRecord<'_>]) -> Dispatched {
    let rows: Vec<OwnedRecord> = rows.iter().map(OwnedRecord::from).collect();
    let (dispatched, refused) = h.deliver(batch, Cow::Owned(rows)).unwrap();
    assert!(refused.is_empty(), "a handle refuses nothing itself");
    dispatched
}

/// One client, however many readers.
#[test]
fn every_reader_shares_one_destination_client() {
    let dest = FakeDestination::new();
    let mut egress = Egress::start(0, "d", Box::new(dest.clone()), timeout(), BUDGET, metrics());

    let mut readers: Vec<_> = (0..3).map(|_| egress.handle()).collect();
    for (i, h) in readers.iter_mut().enumerate() {
        hand(h, i as u64, &[OutgoingRecord::new(format!("row-{i}").as_bytes())]);
        h.flush().unwrap();
    }

    let mut sent: Vec<String> = dest
        .sent()
        .iter()
        .map(|r| String::from_utf8(r.value.clone()).unwrap())
        .collect();
    sent.sort();
    assert_eq!(sent, vec!["row-0", "row-1", "row-2"]);
}

/// Batch ids are per reader and collide; a completion reaches the reader
/// that dispatched it.
#[test]
fn completions_go_back_to_the_reader_that_dispatched_them() {
    let dest = AsyncDestination::new();
    let mut egress = Egress::start(0, "d", Box::new(dest.clone()), timeout(), BUDGET, metrics());

    let mut first = egress.handle();
    let mut second = egress.handle();

    // Both readers call their batch 7.
    hand(&mut first, 7, &[OutgoingRecord::new(b"from-first")]);
    hand(&mut second, 7, &[OutgoingRecord::new(b"from-second")]);

    // Wait for both to reach the destination, then acknowledge them.
    let mut outstanding = Vec::new();
    for _ in 0..200 {
        outstanding = dest.outstanding();
        if outstanding.len() == 2 {
            break;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(outstanding.len(), 2, "both batches reached the destination");
    for b in outstanding {
        dest.complete(b);
    }

    first.flush().unwrap();
    second.flush().unwrap();

    // Each reader is told about its own batch 7, exactly once.
    for (label, h) in [("first", &mut first), ("second", &mut second)] {
        let done = h.poll_completions().unwrap();
        assert!(
            done.iter().all(|c| c.batch == 7),
            "{label} got a completion for someone else's batch: {:?}",
            done.iter().map(|c| c.batch).collect::<Vec<_>>()
        );
    }
}

/// A batch that sent nothing here never touches the queue.
#[test]
fn a_batch_with_no_rows_for_this_destination_stays_off_the_queue() {
    let dest = FakeDestination::new();
    let mut egress = Egress::start(0, "d", Box::new(dest.clone()), timeout(), BUDGET, metrics());
    let mut h = egress.handle();

    assert!(matches!(hand(&mut h, 1, &[]), smeltr::io::Dispatched::Complete));
    h.flush().unwrap();
    assert!(dest.sent().is_empty());
    assert_eq!(dest.flushes(), 0, "the destination was never asked");
}

/// A destination that will not take records fails the reader rather than
/// growing the queue.
#[test]
fn a_destination_that_never_drains_fails_the_reader() {
    let dest = FakeDestination::always_failing();
    let mut egress = Egress::start(0, "d", Box::new(dest), Duration::from_millis(200), BUDGET, metrics());
    let mut h = egress.handle();

    hand(&mut h, 1, &[OutgoingRecord::new(b"row")]);

    let err = h
        .flush()
        .expect_err("a destination refusing every record must reach the reader");
    assert!(!err.to_string().is_empty());
}

/// A delivery failure is re-sent from here, not re-run upstream.
#[test]
fn a_transient_failure_is_re_sent_from_here_not_re_run_upstream() {
    let dest = FakeDestination::new();
    // The first two attempts at this row fail; the third works.
    dest.fail_next_sends(2);
    let mut egress = Egress::start(
        0,
        "d",
        Box::new(dest.clone()),
        Duration::from_secs(5),
        BUDGET,
        metrics(),
    );
    let mut h = egress.handle();

    hand(&mut h, 1, &[OutgoingRecord::new(b"row")]);

    h.flush()
        .expect("egress re-sends until it lands, without telling the reader to re-run");
    assert_eq!(
        dest.sent().len(),
        1,
        "delivered exactly once, from the rows egress kept"
    );
    let done = h.poll_completions().unwrap();
    assert_eq!(done.iter().map(|c| c.batch).collect::<Vec<_>>(), vec![1]);
}

/// The budget is the destination's own figure, not one constant for every destination: a destination configured
/// smaller refuses its readers sooner, on the same rows.
#[test]
fn each_destination_holds_its_own_budget() {
    let budget = 1_000_000;
    let big = vec![b'x'; budget / 2];
    let dest = AsyncDestination::new();
    let mut egress = Egress::start(
        0,
        "d",
        Box::new(dest.clone()),
        Duration::from_millis(300),
        budget,
        metrics(),
    );
    let mut h = egress.handle();

    for batch in 0..2 {
        hand(&mut h, batch, &[OutgoingRecord::new(&big)]);
    }
    for _ in 0..200 {
        if dest.outstanding().len() == 2 {
            break;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(dest.outstanding().len(), 2, "both were sent");

    let started = std::time::Instant::now();
    let answer = hand(&mut h, 2, &[OutgoingRecord::new(&big)]);
    let Dispatched::NoRoom { bytes, .. } = answer else {
        panic!("this destination's own budget is full: {answer:?}");
    };
    assert!(
        started.elapsed() < Duration::from_millis(100),
        "a full queue answers at once; it does not hold the thread"
    );
    assert_eq!(bytes, big.len(), "the size the batch needs, for asking again");
    assert!(!h.has_room(bytes), "still full: nothing was acknowledged");
}

/// A queue that frees room wakes the transform pool, so a lane parked on it is run again without polling.
#[test]
fn freed_room_wakes_the_pool() {
    let dest = AsyncDestination::new();
    let pool = Arc::new(Ready::new());
    let mut egress = Egress::start(0, "d", Box::new(dest.clone()), timeout(), 1, metrics()).with_pool(pool.clone());
    let mut h = egress.handle();

    hand(&mut h, 1, &[OutgoingRecord::new(b"fills the budget alone")]);
    for _ in 0..200 {
        if dest.outstanding().len() == 1 {
            break;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    let Dispatched::NoRoom { bytes, .. } = hand(&mut h, 2, &[OutgoingRecord::new(b"row")]) else {
        panic!("the one unit fills the budget");
    };
    assert!(!h.has_room(bytes));

    let seen = pool.seen();
    for b in dest.outstanding() {
        dest.complete(b);
    }
    let started = std::time::Instant::now();
    pool.wait_past(seen, Duration::from_secs(5));
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "woken by the release, not the timeout"
    );
    assert!(h.has_room(bytes), "room again once the rows are acknowledged");
}

/// Sent-but-unacknowledged rows stay charged against the budget.
#[test]
fn rows_sent_but_not_acknowledged_still_count_against_the_budget() {
    // Half the budget each, from the constant itself: this is a test about the budget.
    let big = vec![b'x'; BUDGET / 2];
    let dest = AsyncDestination::new();
    let mut egress = Egress::start(
        0,
        "d",
        Box::new(dest.clone()),
        Duration::from_millis(300),
        BUDGET,
        metrics(),
    );
    let mut h = egress.handle();

    // Two units fill the budget. Neither is ever acknowledged.
    for batch in 0..2 {
        hand(&mut h, batch, &[OutgoingRecord::new(&big)]);
    }
    for _ in 0..200 {
        if dest.outstanding().len() == 2 {
            break;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(dest.outstanding().len(), 2, "both were sent");

    let Dispatched::NoRoom { bytes, .. } = hand(&mut h, 2, &[OutgoingRecord::new(&big)]) else {
        panic!("the budget is full of unacknowledged rows");
    };
    assert!(!h.has_room(bytes));

    for b in dest.outstanding() {
        dest.complete(b);
    }
    for _ in 0..200 {
        if h.poll_completions().map(|c| !c.is_empty()).unwrap_or(false) {
            break;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(h.has_room(big.len()), "room again once the rows are acknowledged");
    assert_eq!(hand(&mut h, 3, &[OutgoingRecord::new(&big)]), Dispatched::InFlight);
}

/// Shutting the stage down releases a reader waiting on it.
#[test]
fn dropping_the_stage_releases_a_waiting_reader() {
    let dest = AsyncDestination::new();
    let mut egress = Egress::start(0, "d", Box::new(dest), Duration::from_secs(30), BUDGET, metrics());
    let mut h = egress.handle();

    hand(&mut h, 1, &[OutgoingRecord::new(b"row")]);

    drop(egress);

    let waiter = std::thread::spawn(move || h.flush());
    let start = std::time::Instant::now();
    let outcome = waiter.join().expect("the flush thread must not panic");
    assert!(
        outcome.is_err(),
        "a batch nothing will ever acknowledge has to fail, not wait"
    );
    assert!(
        start.elapsed() < Duration::from_secs(20),
        "it waited out the stall threshold instead of noticing the shutdown"
    );
}

/// An idle destination still gets polled for completions.
#[test]
fn a_batch_acknowledged_while_the_queue_is_idle_still_comes_back() {
    let dest = AsyncDestination::new();
    let mut egress = Egress::start(0, "d", Box::new(dest.clone()), timeout(), BUDGET, metrics());
    let mut h = egress.handle();

    hand(&mut h, 3, &[OutgoingRecord::new(b"row")]);

    for _ in 0..200 {
        if !dest.outstanding().is_empty() {
            break;
        }
        std::thread::sleep(Duration::from_millis(5));
    }

    for b in dest.outstanding() {
        dest.complete(b);
    }

    h.flush()
        .expect("the acknowledgment has to arrive with no further traffic");
    let done = h.poll_completions().unwrap();
    assert_eq!(
        done.iter().map(|c| c.batch).collect::<Vec<_>>(),
        vec![3],
        "a completion `flush` consumed still has to reach the commit tracker"
    );
}

/// A refused record is a rejection against its row, not a failed batch.
#[test]
fn a_refused_record_comes_back_as_a_rejection_not_a_failed_batch() {
    let dest = FakeDestination::new();
    dest.reject_next_sends(1);
    let mut egress = Egress::start(
        0,
        "d",
        Box::new(dest.clone()),
        Duration::from_millis(500),
        BUDGET,
        metrics(),
    );
    let mut h = egress.handle();

    hand(
        &mut h,
        1,
        &[OutgoingRecord::new(b"refused"), OutgoingRecord::new(b"fine")],
    );

    h.flush().expect("a refused record must not fail the whole batch");
    let done = h.poll_completions().unwrap();
    let all: Vec<_> = done.into_iter().flat_map(|c| c.rejections).collect();
    assert_eq!(all.len(), 1, "the refusal is reported, once");
    assert_eq!(all[0].doc, 0, "against the row it refused");

    assert_eq!(dest.sent().len(), 1, "the rest of the batch still went out");
}

/// The requests a destination has open are visible, and a destination that opens none of its own publishes no
/// series: a zero there would read as a stalled destination rather than an absent question.
#[test]
fn the_requests_a_destination_has_open_are_reported_and_only_when_it_opens_its_own() {
    /// Reports whatever it is told to, so this test is about egress reading the figure, not about what sets it.
    #[derive(Clone)]
    struct Reporting(Arc<std::sync::Mutex<Option<usize>>>);

    impl Destination for Reporting {
        fn deliver(
            &mut self,
            _b: u64,
            _rows: Cow<'_, [OwnedRecord]>,
        ) -> Result<(Dispatched, Vec<smeltr::io::Rejection>), smeltr::io::IoError> {
            Ok((Dispatched::Complete, Vec::new()))
        }
        fn flush(&mut self) -> Result<(), smeltr::io::IoError> {
            Ok(())
        }
        fn concurrency(&mut self) -> Option<usize> {
            *self.0.lock().unwrap()
        }
    }

    let gauge = |registry: &Arc<Metrics>| {
        registry.snapshot().into_iter().find_map(|s| match s.value {
            smeltr::metrics::MetricValue::Gauge(v) if s.key.name == "smeltr_destination_in_flight" => Some(v),
            _ => None,
        })
    };

    let level = Arc::new(std::sync::Mutex::new(Some(7)));
    let registry = Arc::new(Metrics::new());
    let mut egress = Egress::start(
        0,
        "d",
        Box::new(Reporting(level.clone())),
        timeout(),
        BUDGET,
        MetricsHandle::new(registry.clone(), "egress-0"),
    );
    let mut h = egress.handle();
    hand(&mut h, 1, &[OutgoingRecord::new(b"row")]);
    h.flush().unwrap();
    for _ in 0..200 {
        if gauge(&registry).is_some() {
            break;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(gauge(&registry), Some(7.0), "the level it allows");

    // A destination with no loop of its own leaves the series absent.
    let quiet = Arc::new(Metrics::new());
    let mut egress = Egress::start(
        1,
        "d",
        Box::new(Reporting(Arc::new(std::sync::Mutex::new(None)))),
        timeout(),
        BUDGET,
        MetricsHandle::new(quiet.clone(), "egress-1"),
    );
    let mut h = egress.handle();
    hand(&mut h, 1, &[OutgoingRecord::new(b"row")]);
    h.flush().unwrap();
    std::thread::sleep(Duration::from_millis(50));
    assert_eq!(gauge(&quiet), None, "no loop, no series");
}

/// The egress thread owns the client, so it is the one writer of the
/// destination's health gauge: unhealthy while it will not take rows,
/// healthy again once a send lands.
#[test]
fn a_stall_and_its_recovery_are_reported_on_the_destination_gauge() {
    let registry = Arc::new(Metrics::new());
    let dest = FakeDestination::new();
    dest.fail_next_sends(2);
    let mut egress = Egress::start(
        0,
        "d",
        Box::new(dest.clone()),
        Duration::from_secs(5),
        BUDGET,
        MetricsHandle::new(registry.clone(), "egress-0"),
    );
    let mut h = egress.handle();

    hand(&mut h, 1, &[OutgoingRecord::new(b"row")]);
    h.flush().unwrap();

    let gauge = |name: &str| {
        registry.snapshot().into_iter().find_map(|s| match s.value {
            smeltr::metrics::MetricValue::Gauge(v) if s.key.name == name => Some(v),
            _ => None,
        })
    };
    let counter = |name: &str| {
        registry.snapshot().into_iter().find_map(|s| match s.value {
            smeltr::metrics::MetricValue::Counter(v) if s.key.name == name => Some(v),
            _ => None,
        })
    };
    assert_eq!(counter("smeltr_destination_stalls_total"), Some(2));
    assert_eq!(
        gauge("smeltr_destination_healthy"),
        Some(1.0),
        "healthy again once the re-send landed"
    );
}

/// A client that says it reaches no broker marks its destination down at once, without waiting for an in-flight batch
/// to time out, and up again once it reaches one. The batch stays with the client throughout: it is still sending it.
#[test]
fn a_client_that_reaches_no_broker_marks_its_destination_down_until_it_does() {
    let registry = Arc::new(Metrics::new());
    let dest = AsyncDestination::new();
    let mut egress = Egress::start(
        0,
        "d",
        Box::new(dest.clone()),
        timeout(),
        BUDGET,
        MetricsHandle::new(registry.clone(), "egress-0"),
    );
    let mut h = egress.handle();
    let gauge = || {
        registry.snapshot().into_iter().find_map(|s| match s.value {
            smeltr::metrics::MetricValue::Gauge(v) if s.key.name == "smeltr_destination_healthy" => Some(v),
            _ => None,
        })
    };
    let wait_for = |want: Option<f64>| {
        for _ in 0..200 {
            if gauge() == want {
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(gauge(), want);
    };

    hand(&mut h, 1, &[OutgoingRecord::new(b"row")]);
    dest.set_connection_error(Some("1/1 brokers are down"));
    wait_for(Some(0.0));
    assert_eq!(dest.units().len(), 1, "nothing was taken back to send again");

    dest.set_connection_error(None);
    wait_for(Some(1.0));
}

/// The client's report and a failed send each keep the destination down on their own: a landed send does not mark it up
/// while the client still reaches nothing, and the client reaching a broker again does not mark it up while a failed
/// send's re-send has not landed.
#[test]
fn neither_a_landed_send_nor_a_broker_coming_back_clears_the_other_cause() {
    let registry = Arc::new(Metrics::new());
    let dest = AsyncDestination::new();
    let mut egress = Egress::start(
        0,
        "d",
        Box::new(dest.clone()),
        timeout(),
        BUDGET,
        MetricsHandle::new(registry.clone(), "egress-0"),
    );
    let mut h = egress.handle();
    let gauge = || {
        registry.snapshot().into_iter().find_map(|s| match s.value {
            smeltr::metrics::MetricValue::Gauge(v) if s.key.name == "smeltr_destination_healthy" => Some(v),
            _ => None,
        })
    };
    let wait_until = |done: &dyn Fn() -> bool| {
        for _ in 0..200 {
            if done() {
                return;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        panic!("timed out");
    };

    hand(&mut h, 1, &[OutgoingRecord::new(b"row")]);
    wait_until(&|| !dest.outstanding().is_empty());
    dest.set_connection_error(Some("1/1 brokers are down"));
    wait_until(&|| gauge() == Some(0.0));

    // A batch lands while the client still reaches nothing: down again by the next pass.
    let batch = dest.outstanding()[0];
    dest.complete(batch);
    std::thread::sleep(Duration::from_millis(50));
    assert_eq!(gauge(), Some(0.0), "the client still reaches no broker");

    // A send fails, then the broker comes back: still down until the re-send lands.
    hand(&mut h, 2, &[OutgoingRecord::new(b"row")]);
    wait_until(&|| !dest.outstanding().is_empty());
    dest.fail_next_poll("timed out");
    wait_until(&|| dest.outstanding().len() == 1 && dest.units().len() == 3);
    dest.set_connection_error(None);
    std::thread::sleep(Duration::from_millis(50));
    assert_eq!(gauge(), Some(0.0), "the failed send has not been re-sent and landed");

    let batch = dest.outstanding()[0];
    dest.complete(batch);
    wait_until(&|| gauge() == Some(1.0));
}

/// `Dispatched::InFlight` means only that a batch was handed to the transport, not that anything landed --
/// `AsyncDestination` never completes on its own, so a destination this test never told to fail is still not
/// healthy on that alone: reporting so would call an outage recovered on nothing more than a retry attempt.
#[test]
fn an_in_flight_dispatch_alone_never_reports_recovered() {
    let registry = Arc::new(Metrics::new());
    let dest = AsyncDestination::new();
    let mut egress = Egress::start(
        0,
        "d",
        Box::new(dest.clone()),
        timeout(),
        BUDGET,
        MetricsHandle::new(registry.clone(), "egress-0"),
    );
    let mut h = egress.handle();

    let gauge = |name: &str| {
        registry.snapshot().into_iter().find_map(|s| match s.value {
            smeltr::metrics::MetricValue::Gauge(v) if s.key.name == name => Some(v),
            _ => None,
        })
    };

    hand(&mut h, 1, &[OutgoingRecord::new(b"row")]);
    for _ in 0..200 {
        if !dest.outstanding().is_empty() {
            break;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(!dest.outstanding().is_empty(), "the batch reached the destination");
    assert_eq!(
        gauge("smeltr_destination_healthy"),
        None,
        "handed to the transport is not delivered; nothing has confirmed it either way yet"
    );

    // The delivery report never comes -- a real timeout, or here, the test saying so directly.
    dest.fail_next_poll("boom");
    for _ in 0..200 {
        if gauge("smeltr_destination_healthy") == Some(0.0) {
            break;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(gauge("smeltr_destination_healthy"), Some(0.0));

    // The retry reaches the transport again -- still not itself a delivery.
    for _ in 0..200 {
        if !dest.outstanding().is_empty() {
            break;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(
        gauge("smeltr_destination_healthy"),
        Some(0.0),
        "the retry landing on the transport is not the retry landing on the broker"
    );

    // Only a real completion clears it.
    for b in dest.outstanding() {
        dest.complete(b);
    }
    for _ in 0..200 {
        if gauge("smeltr_destination_healthy") == Some(1.0) {
            break;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(gauge("smeltr_destination_healthy"), Some(1.0));
}

/// A completion collected alongside a shutdown failure still reaches the
/// caller: the batch is off egress's books and would never commit
/// otherwise.
#[test]
fn a_completion_that_arrived_before_the_stage_stopped_is_not_lost() {
    let dest = AsyncDestination::new();
    let mut egress = Egress::start(0, "d", Box::new(dest.clone()), timeout(), BUDGET, metrics());
    let mut h = egress.handle();

    hand(&mut h, 4, &[OutgoingRecord::new(b"row")]);
    for _ in 0..200 {
        if !dest.outstanding().is_empty() {
            break;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    for b in dest.outstanding() {
        dest.complete(b);
    }
    h.flush().unwrap();
    drop(egress);

    let mut seen = Vec::new();
    for _ in 0..3 {
        if let Ok(done) = h.poll_completions() {
            seen.extend(done.into_iter().map(|c| c.batch));
        }
    }
    assert_eq!(seen, vec![4], "the completion survived the shutdown report");
}

/// The queue's fill is a level, and the budget beside it is what makes the level mean something: 6 MB is most of a
/// Kafka queue and nothing against a destination that states a larger one. Both are stated whether or not anything
/// has been sent, so a destination sitting idle is reported as empty rather than as absent.
#[test]
fn a_destination_reports_what_its_queue_holds_and_what_it_may_hold() {
    let registry = Arc::new(Metrics::new());
    let dest = AsyncDestination::new();
    let _egress = Egress::start(
        0,
        "d",
        Box::new(dest.clone()),
        timeout(),
        BUDGET,
        MetricsHandle::new(registry.clone(), "egress-0"),
    );
    let gauge = |name: &str| {
        registry.snapshot().into_iter().find_map(|s| match s.value {
            smeltr::metrics::MetricValue::Gauge(v) if s.key.name == name => Some(v),
            _ => None,
        })
    };

    assert_eq!(
        gauge("smeltr_destination_queue_budget_bytes"),
        Some(BUDGET as f64),
        "the bound is stated up front, before anything is offered"
    );

    for _ in 0..200 {
        if gauge("smeltr_destination_queue_bytes").is_some() {
            break;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(
        gauge("smeltr_destination_queue_bytes"),
        Some(0.0),
        "an idle destination holds nothing, which is a reading rather than an absence"
    );
}

/// What the queue holds is what it has not yet had acknowledged. `AsyncDestination` takes a batch and never
/// completes it, so the bytes stay held: taking a unit off the queue is not what frees it, acknowledgment is.
#[test]
fn a_queue_holding_an_unacknowledged_batch_reports_the_bytes() {
    let registry = Arc::new(Metrics::new());
    let dest = AsyncDestination::new();
    let mut egress = Egress::start(
        0,
        "d",
        Box::new(dest.clone()),
        timeout(),
        BUDGET,
        MetricsHandle::new(registry.clone(), "egress-0"),
    );
    let held = || {
        registry.snapshot().into_iter().find_map(|s| match s.value {
            smeltr::metrics::MetricValue::Gauge(v) if s.key.name == "smeltr_destination_queue_bytes" => Some(v),
            _ => None,
        })
    };

    let mut h = egress.handle();
    hand(&mut h, 1, &[OutgoingRecord::new(b"a row worth some bytes")]);

    for _ in 0..200 {
        if held().unwrap_or(0.0) > 0.0 {
            break;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    let bytes = held().expect("the queue reports what it holds");
    assert!(bytes > 0.0, "a batch nobody acknowledged is still held: {bytes}");
    assert!(
        bytes <= BUDGET as f64,
        "never past the budget it is measured against: {bytes}"
    );
}

/// A row's salvage plan is held as long as the row is: one identity for every DLQ down the chain. It counts against
/// the budget, or a long chain would hold far more than the budget says.
#[test]
fn a_rows_salvage_plan_counts_against_the_budget() {
    let registry = Arc::new(Metrics::new());
    let dest = AsyncDestination::new();
    let mut egress = Egress::start(
        0,
        "d",
        Box::new(dest.clone()),
        timeout(),
        BUDGET,
        MetricsHandle::new(registry.clone(), "egress-0"),
    );
    let held = || {
        registry.snapshot().into_iter().find_map(|s| match s.value {
            smeltr::metrics::MetricValue::Gauge(v) if s.key.name == "smeltr_destination_queue_bytes" => Some(v),
            _ => None,
        })
    };
    let plan: Vec<SalvageTarget> = (1..=3)
        .map(|dlq| SalvageTarget {
            dlq,
            doc_id: Some("k".repeat(1000)),
            reached: false,
        })
        .collect();

    let mut h = egress.handle();
    hand(&mut h, 1, &[OutgoingRecord::new(b"x").with_salvage_to(Some(&plan))]);

    for _ in 0..200 {
        if held().unwrap_or(0.0) > 0.0 {
            break;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    let bytes = held().expect("the queue reports what it holds");
    assert!(bytes >= 3001.0, "the plan's three identities are held: {bytes}");
}

/// Wait for `handle`'s completions, up to `within`, and answer what came back.
fn completions_within(handle: &mut smeltr::egress::EgressHandle, within: Duration) -> Vec<smeltr::io::BatchCompletion> {
    let deadline = std::time::Instant::now() + within;
    loop {
        let got = handle.poll_completions().unwrap();
        if !got.is_empty() || std::time::Instant::now() >= deadline {
            return got;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// A refusal comes back with the document it refused, so a reader whose destination sends refusals to a DLQ has what
/// to send.
#[test]
fn a_refusal_comes_back_with_its_document() {
    let dest = FakeDestination::new();
    dest.reject_next_sends(1);
    let mut egress = Egress::start(0, "d", Box::new(dest.clone()), timeout(), BUDGET, metrics());
    let mut h = egress.handle();
    hand(
        &mut h,
        3,
        &[
            OutgoingRecord::new(b"refused").with_doc_id(Some("id-1")),
            OutgoingRecord::new(b"taken"),
        ],
    );

    let done = completions_within(&mut h, timeout());
    assert_eq!(done.len(), 1);
    let [r] = &done[0].rejections[..] else {
        panic!("one refusal, got {:?}", done[0].rejections)
    };
    let record = r.record.as_ref().expect("the document comes back");
    assert_eq!(
        (record.value.as_slice(), record.doc_id.as_deref()),
        (&b"refused"[..], Some("id-1"))
    );
    assert!(h.take_held().is_empty(), "not set to block: nothing is held");
}

/// A destination set to block keeps the rows it refused and sends them again after a wait. The batch is reported held,
/// not done, and completes only once they land -- with nothing refused, since they were taken in the end.
#[test]
fn a_blocking_destination_holds_its_refusals_and_sends_them_again() {
    let dest = FakeDestination::new();
    dest.reject_next_sends(1);
    let mut egress = Egress::start(0, "d", Box::new(dest.clone()), timeout(), BUDGET, metrics()).holding_refusals();
    let mut h = egress.handle();
    hand(
        &mut h,
        3,
        &[OutgoingRecord::new(b"refused"), OutgoingRecord::new(b"taken")],
    );

    let deadline = std::time::Instant::now() + timeout();
    let held = loop {
        assert!(h.poll_completions().unwrap().is_empty(), "held, not done");
        let held = h.take_held();
        if !held.is_empty() || std::time::Instant::now() >= deadline {
            break held;
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    assert_eq!(held.len(), 1);
    assert_eq!((held[0].batch, held[0].rejections.len()), (3, 1));

    let done = completions_within(&mut h, timeout());
    assert_eq!(done.len(), 1, "done once the refused row landed");
    assert_eq!(done[0].batch, 3);
    assert!(done[0].rejections.is_empty());
    let sent: Vec<Vec<u8>> = dest.sent().into_iter().map(|r| r.value).collect();
    assert_eq!(
        sent,
        vec![b"taken".to_vec(), b"refused".to_vec()],
        "only the refused row went again"
    );
}

/// A DLQ answers for what it refuses with its own policy, what it was sent in salvage included: set to block, it
/// holds a salvaged row like any other.
#[test]
fn a_blocking_dlq_holds_a_salvaged_row_it_refuses() {
    let dest = FakeDestination::new();
    dest.reject_next_sends(1);
    let mut egress = Egress::start(0, "d", Box::new(dest.clone()), timeout(), BUDGET, metrics()).holding_refusals();
    let mut h = egress.handle();
    hand(&mut h, 4, &[OutgoingRecord::new(b"salvaged").salvaged()]);

    let deadline = std::time::Instant::now() + timeout();
    while h.take_held().is_empty() {
        assert!(std::time::Instant::now() < deadline, "held");
        assert!(h.poll_completions().unwrap().is_empty(), "not done");
        std::thread::sleep(Duration::from_millis(10));
    }
    let done = completions_within(&mut h, timeout());
    assert_eq!(done.len(), 1, "done once it landed");
    assert!(done[0].rejections.is_empty());
}

/// Ending the deployment ends a capture at its destination with what it holds, rather than leaving it to wait out
/// its deadline.
#[test]
fn dropping_an_egress_ends_the_capture_at_its_destination() {
    use smeltr::heartbeat::CaptureInvitation;
    use smeltr::worker::capture::Captures;

    let captures = Arc::new(Captures::default());
    let egress = Egress::start(0, "es", Box::new(FakeDestination::new()), timeout(), BUDGET, metrics())
        .with_captures(captures.clone());
    assert!(captures.sends_to("es"));
    captures.admit(&[CaptureInvitation {
        session: "s1".to_string(),
        group: "g".to_string(),
        source: None,
        destination: Some("es".to_string()),
        filter: None,
        start: None,
        max_matches: 10,
        max_bytes: 1_000_000,
        deadline_secs: 60,
        secrets: Vec::new(),
    }]);

    drop(egress);
    assert!(!captures.sends_to("es"));
    let report = captures.finished("w1").remove(0);
    assert!(
        matches!(report.finished, Some(smeltr::heartbeat::CaptureEnd::Failed(_))),
        "{report:?}"
    );
}

/// A capture at a destination set to block sees the rows it refused and keeps, once each, with why it refused them
/// first. They wait between sends rather than passing through a transform thread, so the egress thread shows them, and
/// says how many it keeps, for the leader to know a capture there has them to wait for.
#[test]
fn a_capture_at_a_blocking_destination_sees_its_held_rows_once() {
    use smeltr::heartbeat::CaptureInvitation;
    use smeltr::worker::capture::{Captures, EXCEPTION_MESSAGE};

    let dest = FakeDestination::new();
    // Refused twice, so the row waits twice; it is shown on the first wait only.
    dest.reject_next_sends(2);
    let captures = Arc::new(Captures::default());
    let registry = Arc::new(Metrics::new());
    let kept = || {
        registry.snapshot().into_iter().find_map(|s| match s.value {
            smeltr::metrics::MetricValue::Gauge(v) if s.key.name == "smeltr_destination_held_rows" => Some(v),
            _ => None,
        })
    };
    let mut egress = Egress::start(
        0,
        "es",
        Box::new(dest.clone()),
        timeout(),
        BUDGET,
        MetricsHandle::new(registry.clone(), "egress-0"),
    )
    .holding_refusals()
    .with_captures(captures.clone());
    captures.admit(&[CaptureInvitation {
        session: "s1".to_string(),
        group: "g".to_string(),
        source: None,
        destination: Some("es".to_string()),
        filter: None,
        start: None,
        max_matches: 10,
        max_bytes: 1_000_000,
        deadline_secs: 60,
        secrets: Vec::new(),
    }]);
    let mut h = egress.handle();
    hand(
        &mut h,
        5,
        &[OutgoingRecord::new(br#"{"a":1}"#).with_doc_id(Some("id-1"))],
    );

    let deadline = std::time::Instant::now() + timeout();
    while kept() != Some(1.0) {
        assert!(std::time::Instant::now() < deadline, "the refused row is kept");
        std::thread::sleep(Duration::from_millis(5));
    }
    let done = completions_within(&mut h, timeout());
    assert_eq!(done.len(), 1, "done once the refused row landed");
    // Written later in the pass that reported the batch done.
    while kept() != Some(0.0) {
        assert!(std::time::Instant::now() < deadline, "nothing kept once it landed");
        std::thread::sleep(Duration::from_millis(5));
    }
    let report = captures.progress("w1").remove(0);
    let [batch] = &report.batches[..] else {
        panic!("one batch, got {:?}", report.batches)
    };
    let columns: Vec<&str> = batch.columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(
        columns,
        [
            EXCEPTION_MESSAGE,
            "a",
            "__destination.placement",
            "__destination.id",
            "__destination.route"
        ]
    );
    let [row] = &batch.rows[..] else {
        panic!("shown once, got {:?}", batch.rows)
    };
    assert_eq!(
        row.values,
        [
            serde_json::json!("fake destination: record refused"),
            serde_json::json!(1),
            serde_json::Value::Null,
            serde_json::json!("id-1"),
            serde_json::Value::Null
        ]
    );
}

/// How far away [`Link`] is.
const LINK_RTT: Duration = Duration::from_millis(20);

/// A link [`LINK_RTT`] away, over which a destination answers each batch once it has been open that long. Answering
/// on a tick instead would answer a batch sent just before it at once, and the model's least round trip, which keeps
/// such a reading, would shrink what it holds.
#[derive(Default)]
struct Link(std::collections::HashMap<u64, std::time::Instant>);

impl Link {
    /// Acknowledge what `dest` has held for a round trip; how many batches that was.
    fn answer(&mut self, dest: &AsyncDestination) -> usize {
        // Read after `outstanding`, so a batch is never taken as open before it was.
        let open = dest.outstanding();
        let now = std::time::Instant::now();
        let mut answered = 0;
        for b in open {
            let at = *self.0.entry(b).or_insert(now);
            if now.duration_since(at) >= LINK_RTT {
                self.0.remove(&b);
                dest.complete(b);
                answered += 1;
            }
        }
        answered
    }
}

/// A paced queue starts at one batch and, while its source presses and the destination keeps up, grows its budget
/// toward its share of the pool; it never passes that share.
#[test]
fn a_paced_queue_grows_from_one_batch_while_its_source_presses() {
    let registry = Arc::new(Metrics::new());
    let row = vec![b'x'; 64 * 1024];
    let pool = smeltr::egress::QueuePool::new(64 * 1024 * 1024);
    let dest = AsyncDestination::new();
    let mut egress = Egress::start(
        0,
        "d",
        Box::new(dest.clone()),
        timeout(),
        pool.limit(),
        MetricsHandle::new(registry.clone(), "egress-0"),
    )
    .borrowing(pool.clone(), row.len(), 1)
    .paced(row.len());
    let budget = || {
        registry.snapshot().into_iter().find_map(|s| match s.value {
            smeltr::metrics::MetricValue::Gauge(v) if s.key.name == "smeltr_destination_queue_budget_bytes" => {
                Some(v as usize)
            }
            _ => None,
        })
    };
    let mut h = egress.handle();

    // A source that always has more, over a link a few milliseconds away.
    let mut link = Link::default();
    let started = std::time::Instant::now();
    let mut batch = 0;
    let mut most = 0;
    while started.elapsed() < Duration::from_secs(2) {
        while let Dispatched::InFlight = hand(&mut h, batch, &[OutgoingRecord::new(&row)]) {
            batch += 1;
        }
        std::thread::sleep(Duration::from_millis(1));
        link.answer(&dest);
        most = most.max(budget().unwrap_or(0));
    }
    assert!(most >= 8 * row.len(), "the budget grew from one batch: {most}");
    assert!(most <= row.len() + pool.limit(), "never past the share: {most}");
}

/// Two paced queues on one pool smaller than either would hold alone. The first, alone, grows over most of it; once
/// the second presses too, each settles near an equal share. Once the first stalls, holding what it has, the second
/// still moves.
#[test]
fn paced_queues_on_one_pool_settle_near_an_equal_share_and_a_stall_starves_neither() {
    let registry = Arc::new(Metrics::new());
    let row = vec![b'x'; 64 * 1024];
    let pool = smeltr::egress::QueuePool::new(8 * 64 * 1024);
    let batches = pool.limit() / row.len();
    let dests = [AsyncDestination::new(), AsyncDestination::new()];
    let mut egresses: Vec<Egress> = dests
        .iter()
        .enumerate()
        .map(|(i, d)| {
            let metrics = MetricsHandle::new(registry.clone(), format!("egress-{i}"));
            Egress::start(i as u32, "d", Box::new(d.clone()), timeout(), pool.limit(), metrics)
                .borrowing(pool.clone(), row.len(), 1)
                .paced(row.len())
        })
        .collect();
    let mut handles: Vec<_> = egresses.iter_mut().map(Egress::handle).collect();
    let mut next = [0u64; 2];
    // Each queue's budget, in batches.
    let budget = |i: usize| {
        let label = i.to_string();
        registry.snapshot().into_iter().find_map(|s| match s.value {
            smeltr::metrics::MetricValue::Gauge(v)
                if s.key.name == "smeltr_destination_queue_budget_bytes"
                    && s.key.labels.iter().any(|(k, v)| *k == "destination" && *v == label) =>
            {
                Some(v as usize / row.len())
            }
            _ => None,
        })
    };
    let mut links: [Link; 2] = Default::default();
    // For `secs`, each of `pressing` sends while it has room and each of `answering` answers over its link.
    // Answers each queue's budget in batches, on average over the last second, and how many batches each delivered.
    let mut run = |secs: u64, pressing: &[usize], answering: &[usize]| {
        let started = std::time::Instant::now();
        let (mut held, mut samples, mut delivered) = ([0usize; 2], 0, [0usize; 2]);
        while started.elapsed() < Duration::from_secs(secs) {
            for &i in pressing {
                while let Dispatched::InFlight = hand(&mut handles[i], next[i], &[OutgoingRecord::new(&row)]) {
                    next[i] += 1;
                }
            }
            std::thread::sleep(Duration::from_millis(1));
            let last_second = started.elapsed() + Duration::from_secs(1) >= Duration::from_secs(secs);
            for (i, d) in dests.iter().enumerate() {
                if answering.contains(&i) {
                    delivered[i] += links[i].answer(d);
                }
                if last_second {
                    held[i] += budget(i).unwrap_or(0);
                }
            }
            samples += usize::from(last_second);
        }
        (held.map(|h| h / samples.max(1)), delivered)
    };

    let (alone, _) = run(2, &[0], &[0]);
    assert!(
        alone[0] > batches / 2,
        "alone, the first grows over most of the pool: {alone:?} of {batches}"
    );

    let (both, delivered) = run(3, &[0, 1], &[0, 1]);
    assert!(
        both[1] >= batches / 4,
        "the second gets near its share: {both:?} of {batches}"
    );
    assert!(
        both[0] <= 3 * batches / 4,
        "the first gives back toward its share: {both:?} of {batches}"
    );
    assert!(
        2 * delivered[1] >= delivered[0] && 2 * delivered[0] >= delivered[1],
        "each carries about as much: {delivered:?}"
    );

    let (stalled, delivered_stalled) = run(1, &[0, 1], &[1]);
    assert!(
        stalled[1] + 1 >= both[1],
        "the first stalled, the second keeps its share: {stalled:?} after {both:?}"
    );
    assert!(
        6 * delivered_stalled[1] >= delivered[1],
        "and carries at least half as much a second: {delivered_stalled:?} in one, {delivered:?} in three"
    );
}
