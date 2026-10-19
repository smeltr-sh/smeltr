# Self-observability

What smeltr knows about itself, where that knowledge lives, and who may
ask for it. This is the shape of the design; the mechanisms move into
`smeltr-design.md` once it is settled.

## The shape

```text
worker process

  call sites --> buffer --> store, on disk, bounded by bytes
                   |          (coalesces repeats)
                   +--> console, unconditional
                   |
                   +--> SystemLogs source --> any destination
                   |
                   +--> rollup --> heartbeat --> leader

  A request for detail rides the heartbeat response; the worker answers
  on its next beat. Nothing originates at the leader.
```

## Logs are a source

Metrics already travel as a `Source`, routed and delivered like customer
data. Logs join them, so self-observability has one delivery mechanism
and smeltr can put its own account of itself anywhere it can put data.

The route is shared with metrics. The buffer is not, because four
properties differ:

- A log record carries the timestamp of its write. A metric sample
  carries the time its bucket drained.
- Log delivery is at-least-once. Metrics trade a window of counts for
  simplicity, which is the wrong trade for the record of a failure.
- The buffer outlives any deployment. The metrics registry does not.
- `poll_batch` honors its timeout, so an idle source costs nothing.

## The buffer belongs to the process

Twenty-seven of smeltr's forty-eight log sites emit where no
deployment exists — during startup, config validation, enrollment, and
the eleven paths that exit. A process-scoped buffer carries all of them.

The metrics registry moves to the same scope, which is where the design
doc's Observability already describes it. Counters then survive a redeploy.

Console output is not a configured destination. It is what still works
when configuration is what broke, and it is the only sink a process
about to exit can reach.

## A worker keeps its own logs

A worker holds its logs and its per-thread metric detail in SQLite under
its state directory, bounded by a byte budget. Logs and metrics get
separate budgets, so a log storm cannot evict the history the fleet view
reads.

The store is disposable. Nothing reads it expecting it to be there; one
that cannot be opened is deleted and started again, and the loss is
counted. There is no repair path and no migration path.

Bytes are the bound because bytes are the constraint: a year of
logs that fits is fine, and a day that does not is an outage.

This is worker-internal state. Data-plane statelessness — no
aggregation, no checkpointing, no rebalance state, uncommitted work
redelivered on restart — is unchanged.

## Volume is controlled at the buffer

Identical events coalesce as they buffer: the first is emitted
immediately, and a second of the same kind inside the window starts a
count. A coalesced record keeps the count, its first and last
timestamps, and one exemplar.

The key is the call site and a chosen subset of its fields, never the
rendered text — a message naming a row number renders differently for
every row and is one event. Distinct keys per window are capped, with a
catch-all beyond the cap.

Occupancy is then proportional to the number of distinct events rather
than to throughput, which is what bounds the buffer under a fault that
would otherwise emit per row.

## The leader holds a rollup and asks for detail

Every worker reports a small per-object summary on each heartbeat,
labeled with the stable source and destination ids it holds at deploy
time. This is what the fleet view reads, and its cost does not grow with
the fleet.

Full detail stays on the worker. The leader's request rides the
heartbeat response, so every exchange remains worker-initiated and a
host behind NAT is unchanged. Coverage varies per worker under a byte
budget, so a partial answer is the normal case and is reported as
coverage.

## Consequences

**Three storage points, not one.** A worker's bounded store, the
leader's rollup, and whatever a deployment routes its telemetry to. They
differ in fidelity and lifetime.

**Retention on the worker becomes a subject.** A data-plane host gains a
file that grows to a ceiling and stays there, beside a git clone that is
already unbounded and that nothing reclaims.

**A second pipeline that can fail to compile.** A worker stops what it
is running before it discovers a new commit does not compile, so a bad
pipeline leaves it running nothing. The existing defect gates this work.

**Detail dies with its host.** A worker that is gone cannot be asked
what happened to it; the pushed rollup is what remains.

## What changes in the existing documents

- The design doc's Observability, "one `Arc<Metrics>` per process",
  becomes true.
- The design doc's Observability durability posture is stated per source
  rather than per section: at-least-once for logs, a dropped bucket for
  metrics.
- `review-control-plane.md`'s "the leader owns all new state" carries a
  named exception for worker-internal logs.
- "One retention problem rather than two" is replaced.

## Sequence

The process-scoped buffer, console output, and the metrics registry's
scope land first — one correction, and it converts a class of message
from lost to local.

The compile check before teardown is a gate rather than a step. It
closes an existing defect and must precede a second self-observability
pipeline.

Bounded storage and coalescing land together. A store without
coalescing is drowned by the first per-row fault; coalescing without a
store has nowhere to put what it summarized.

`SystemLogs` follows, after the leader's in-process preview worker is
retired — it emits the same records from a process with no deployment.

The rollup and detail-on-request come last. They are what the GUI reads,
and nothing underneath them exists until the rest has landed.

## Out of scope

Who may read any of this. Warnings — what raises one and how it clears
— which reads these facts and is its own design. What the GUI does with
them. Retention for the worker's git clone, which this makes urgent
without addressing.
